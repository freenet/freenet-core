//! Ring protocol logic and supporting types.
//!
//! # Routing
//! The routing mechanism consist in a greedy routing algorithm which just targets
//! the closest location to the target destination iteratively in each hop, until it reaches
//! the destination.
//!
//! Path is limited to local knowledge, at any given point only 3 data points are known:
//! - previous node
//! - next node
//! - final location

use std::{
    borrow::Borrow,
    collections::{BTreeMap, HashSet},
    convert::TryFrom,
    fmt::Display,
    hash::Hasher,
};

use dashmap::DashSet;
use parking_lot::RwLock;

use crate::{
    conn_manager::{self, PeerKeyLocation},
    contract::ContractKey,
};

/// Thread safe and friendly data structure to keep track of the local knowledge
/// of the state of the ring.
#[derive(Debug)]
pub(crate) struct Ring {
    pub rnd_if_htl_above: usize,
    pub max_hops_to_live: usize,
    pub connections_by_location: RwLock<BTreeMap<Location, PeerKeyLocation>>,
    /// contracts in the ring cached by this node
    cached_contracts: DashSet<ContractKey>,
    // TODO: optimize this for an AtomicU64
    own_location: RwLock<Option<Location>>,
}

impl Ring {
    const MIN_CONNECTIONS: usize = 10;
    const MAX_CONNECTIONS: usize = 20;

    /// Above this number of remaining hops,
    /// randomize which of node a message which be forwarded to.
    pub const RAND_WALK_ABOVE_HTL: usize = 7;

    pub const MAX_HOPS_TO_LIVE: usize = 10;

    pub fn new() -> Self {
        Ring {
            rnd_if_htl_above: Self::RAND_WALK_ABOVE_HTL,
            max_hops_to_live: Self::MAX_HOPS_TO_LIVE,
            connections_by_location: RwLock::new(BTreeMap::new()),
            cached_contracts: DashSet::new(),
            own_location: RwLock::new(None),
        }
    }

    pub fn with_rnd_walk_above(&mut self, rnd_if_htl_above: usize) -> &mut Self {
        self.rnd_if_htl_above = rnd_if_htl_above;
        self
    }

    pub fn with_max_hops(&mut self, max_hops_to_live: usize) -> &mut Self {
        self.max_hops_to_live = max_hops_to_live;
        self
    }

    pub fn within_caching_distance(&self, loc: &Location) -> bool {
        // FIXME: add logic here
        true
    }

    pub fn has_contract(&self, key: &ContractKey) -> bool {
        self.cached_contracts.contains(key)
    }

    pub fn update_location(&self, loc: Location) {
        let old_loc = &mut *self.own_location.write();
        *old_loc = Some(loc);
    }

    /// Returns this node location in the ring, if any (must have join the ring already).
    pub fn own_location(&self) -> Option<Location> {
        *self.own_location.read()
    }

    pub fn should_accept(&self, my_location: &Location, location: &Location) -> bool {
        let cbl = &*self.connections_by_location.read();
        if location == my_location || cbl.contains_key(location) {
            false
        } else if cbl.len() < Self::MIN_CONNECTIONS {
            true
        } else if cbl.len() >= Self::MAX_CONNECTIONS {
            false
        } else {
            my_location.distance(location) < self.median_distance_to(my_location)
        }
    }

    pub fn median_distance_to(&self, location: &Location) -> Distance {
        let connections = self.connections_by_location.read();
        let mut conn_by_dist: Vec<_> = connections
            .keys()
            .map(|key| key.distance(location))
            .collect();
        conn_by_dist.sort_unstable();
        let idx = self.connections_by_location.read().len() / 2;
        conn_by_dist[idx]
    }

    pub fn routing(&self, target: &Location) -> Option<(Location, PeerKeyLocation)> {
        let connections = self.connections_by_location.read();
        let mut conn_by_dist: Vec<_> = connections
            .iter()
            .map(|(loc, peer)| (loc.distance(target), (loc, peer)))
            .collect();
        conn_by_dist.sort_by_key(|&(dist, _)| dist);
        conn_by_dist.first().map(|(_, v)| (*v.0, *v.1))
    }

    pub fn random_peer<F>(&self, filter_fn: F) -> Option<PeerKeyLocation>
    where
        F: FnMut(&&PeerKeyLocation) -> bool,
    {
        // FIXME: should be optimized and avoid copying
        self.connections_by_location
            .read()
            .values()
            .find(filter_fn)
            .copied()
    }
}

/// An abstract location on the 1D ring, represented by a real number on the interal [0, 1]
#[derive(Debug, serde::Serialize, serde::Deserialize, Clone, Copy)]
pub struct Location(f64);

pub(crate) type Distance = Location;

impl Location {
    /// Returns a new random location.
    pub fn random() -> Self {
        use rand::prelude::*;
        let mut rng = rand::thread_rng();
        Location(rng.gen_range(0.0..=1.0))
    }

    /// Compute the distance between two locations.
    pub fn distance(&self, other: &Location) -> Distance {
        let d = (self.0 - other.0).abs();
        if d < 0.5 {
            Location(d)
        } else {
            Location(1.0 - d)
        }
    }
}

/// Ensure at compile time locations can only be constructed from well formed contract keys
/// (which have been hashed with a strong, cryptographically safe, hash function first).
impl<T> From<T> for Location
where
    T: Borrow<ContractKey>,
{
    fn from(key: T) -> Self {
        let mut value = 0.0;
        let mut divisor = 256.0;
        // assert!((1..=7).contains(&precision));
        for byte in key.borrow().bytes().iter().take(7) {
            value += *byte as f64 / divisor;
            divisor *= 256.0;
        }
        Location::try_from(value).expect("value should be between 0 and 1")
    }
}

impl Display for Location {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.0.to_string().as_str())?;
        Ok(())
    }
}

impl PartialEq for Location {
    fn eq(&self, other: &Self) -> bool {
        self.0 == other.0
    }
}

/// Since we don't allow NaN values in the construction of Location
/// we can safely assume that an equivalence relation holds.  
impl Eq for Location {}

impl Ord for Location {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.partial_cmp(other)
            .expect("always should return a cmp value")
    }
}

impl PartialOrd for Location {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        self.0.partial_cmp(&other.0)
    }
}

impl std::hash::Hash for Location {
    fn hash<H: Hasher>(&self, state: &mut H) {
        let bits = self.0.to_bits();
        state.write_u64(bits);
        state.finish();
    }
}

impl TryFrom<f64> for Location {
    type Error = ();

    fn try_from(value: f64) -> Result<Self, Self::Error> {
        if !(0.0..=1.0).contains(&value) {
            Err(())
        } else {
            Ok(Location(value))
        }
    }
}

#[derive(thiserror::Error, Debug)]
pub(crate) enum RingError {
    #[error("failed while attempting to join a ring")]
    Join,
    #[error(transparent)]
    ConnError(#[from] Box<conn_manager::ConnError>),
    #[error("no ring connections found")]
    EmptyRing,
    #[error("no location assigned to this node")]
    NoLocationAssigned,
}

#[cfg(test)]
mod test {
    use libp2p::PeerId;

    use super::*;
    use crate::conn_manager::PeerKey;

    #[test]
    fn location_dist() {
        let l0 = Location(0.);
        let l1 = Location(0.25);
        assert!(l0.distance(&l1) == Location(0.25));

        let l0 = Location(0.75);
        let l1 = Location(0.50);
        assert!(l0.distance(&l1) == Location(0.25));
    }

    #[test]
    fn find_closest() {
        let ring = Ring::new();
        {
            let conns = &mut *ring.connections_by_location.write();
            let def_peer = PeerKeyLocation {
                peer: PeerKey(PeerId::random()),
                location: None,
            };
            conns.insert(Location(0.3), def_peer);
            conns.insert(Location(0.5), def_peer);
            conns.insert(Location(0.0), def_peer);
        }

        assert_eq!(Location(0.0), ring.routing(&Location(0.9)).unwrap().0);
        assert_eq!(Location(0.0), ring.routing(&Location(0.1)).unwrap().0);
        assert_eq!(Location(0.5), ring.routing(&Location(0.41)).unwrap().0);
        assert_eq!(Location(0.3), ring.routing(&Location(0.39)).unwrap().0);
    }
}
