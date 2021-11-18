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
    collections::BTreeMap,
    convert::TryFrom,
    fmt::Display,
    hash::Hasher,
    sync::atomic::{AtomicU64, Ordering::SeqCst},
    time::Instant,
};

use anyhow::bail;
use dashmap::{mapref::one::Ref as DmRef, DashMap, DashSet};
use parking_lot::RwLock;
use serde::{Deserialize, Serialize};

use crate::{
    conn_manager::{self, PeerKey},
    contract::ContractKey,
};

#[derive(Debug, Serialize, Deserialize, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
/// The location of a peer in the ring. This location allows routing towards the peer.
pub(crate) struct PeerKeyLocation {
    pub peer: PeerKey,
    /// An unspecified location means that the peer hasn't been asigned a location, yet.
    pub location: Option<Location>,
}

/// Thread safe and friendly data structure to keep track of the local knowledge
/// of the state of the ring.
#[derive(Debug)]
pub(crate) struct Ring {
    pub rnd_if_htl_above: usize,
    pub max_hops_to_live: usize,
    max_connections: usize,
    pub peer_key: PeerKey,
    connections_by_location: RwLock<BTreeMap<Location, PeerKeyLocation>>,
    /// contracts in the ring cached by this node
    cached_contracts: DashSet<ContractKey>,
    own_location: AtomicU64,
    /// The container for subscriber is a vec instead of something like a hashset
    /// that would allow for blind inserts of duplicate peers subscribing because
    /// of data locality, since we are likely to end up iterating over the whole sequence
    /// of subscribers more often than inserting, and anyways is a relatively short sequence
    /// then is more optimal to just use a vector for it's compact memory layout.
    subscribers: DashMap<ContractKey, Vec<PeerKeyLocation>>,
    subscriptions: RwLock<Vec<ContractKey>>,
    /// A peer which has been blacklisted to perform actions regarding a given contract.
    contract_blacklist: DashMap<ContractKey, Vec<Blacklisted>>,
}

/// A data type that represents the fact that a peer has been blacklisted
/// for some action. Has to be coupled with that action
#[derive(Debug)]
struct Blacklisted {
    since: Instant,
    peer: PeerKey,
}

impl Ring {
    const MIN_CONNECTIONS: usize = 10;

    const MAX_CONNECTIONS: usize = 20;

    /// Max number of subscribers for a contract.
    const MAX_SUBSCRIBERS: usize = 10;

    /// Above this number of remaining hops,
    /// randomize which of node a message which be forwarded to.
    const RAND_WALK_ABOVE_HTL: usize = 7;

    /// Max hops to be performed for certain operations (e.g. propagating
    /// connection of a peer in the network).
    const MAX_HOPS_TO_LIVE: usize = 10;

    pub fn new(key: PeerKey) -> Self {
        // for location here consider -1 == None
        let own_location = AtomicU64::new(u64::from_le_bytes((-1f64).to_le_bytes()));
        Ring {
            rnd_if_htl_above: Self::RAND_WALK_ABOVE_HTL,
            max_hops_to_live: Self::MAX_HOPS_TO_LIVE,
            max_connections: Self::MAX_CONNECTIONS,
            connections_by_location: RwLock::new(BTreeMap::new()),
            cached_contracts: DashSet::new(),
            own_location,
            peer_key: key,
            subscribers: DashMap::new(),
            subscriptions: RwLock::new(Vec::new()),
            contract_blacklist: DashMap::new(),
        }
    }

    /// Set the threashold of HPL for which a random walk will be performed.
    pub fn with_rnd_walk_above(&mut self, rnd_if_htl_above: usize) -> &mut Self {
        self.rnd_if_htl_above = rnd_if_htl_above;
        self
    }

    /// Set the maximum HTL for transactions.
    pub fn with_max_hops(&mut self, max_hops_to_live: usize) -> &mut Self {
        self.max_hops_to_live = max_hops_to_live;
        self
    }

    pub fn with_max_connections(&mut self, connections: usize) -> &mut Self {
        self.max_connections = connections;
        self
    }

    #[inline(always)]
    /// Return if a location is within appropiate caching distance.
    pub fn within_caching_distance(&self, _loc: &Location) -> bool {
        // This always returns true as of current version since LRU cache will make sure
        // to remove contracts when capacity is fully utilized.
        // So all nodes along the path will be caching all the contracts.
        // This will be changed in the future as the caching logic gets more complicated.
        true
    }

    /// Whether this node already has this contract cached or not.
    #[inline]
    pub fn contract_exists(&self, key: &ContractKey) -> bool {
        self.cached_contracts.contains(key)
    }

    /// Update this node location.
    pub fn update_location(&self, loc: PeerKeyLocation) {
        if let Some(loc) = loc.location {
            self.own_location
                .store(u64::from_le_bytes(loc.0.to_le_bytes()), SeqCst)
        } else {
            self.own_location
                .store(u64::from_le_bytes((-1f64).to_le_bytes()), SeqCst)
        }
    }

    /// Returns this node location in the ring, if any (must have join the ring already).
    pub fn own_location(&self) -> PeerKeyLocation {
        let location = f64::from_le_bytes(self.own_location.load(SeqCst).to_le_bytes());
        let location = if (location - -1f64).abs() < f64::EPSILON {
            None
        } else {
            Some(Location(location))
        };
        PeerKeyLocation {
            peer: self.peer_key,
            location,
        }
    }

    /// Whether a node should accept a new node connection or not based
    /// on the relative location and other conditions.
    pub fn should_accept(&self, my_location: &Location, location: &Location) -> bool {
        let cbl = &*self.connections_by_location.read();
        if location == my_location || cbl.contains_key(location) {
            false
        } else if cbl.len() < Self::MIN_CONNECTIONS {
            true
        } else if cbl.len() >= self.max_connections {
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

    /// Return the closest peers to a contract location which are caching it,
    /// excluding whichever peers in the skip list.
    #[inline]
    pub fn closest_caching(
        &self,
        contract: &ContractKey,
        n: usize,
        skip_list: &[PeerKey],
    ) -> Vec<PeerKeyLocation> {
        // Right now we return just the closest known peers to that location.
        // In the future this may change to the ones closest which are actually already caching it.
        self.routing(&contract.location(), n, skip_list)
    }

    /// Find the closest number of peers to a given location. Result is returned sorted by proximity.
    pub fn routing(
        &self,
        target: &Location,
        n: usize,
        skip_list: &[PeerKey],
    ) -> Vec<PeerKeyLocation> {
        let connections = self.connections_by_location.read();
        let mut conn_by_dist: Vec<_> = connections
            .iter()
            .filter(|(_, pkloc)| !skip_list.contains(&pkloc.peer))
            .map(|(loc, peer)| (loc.distance(target), (loc, peer)))
            .collect();
        conn_by_dist.sort_by_key(|&(dist, _)| dist);
        let iter = conn_by_dist.into_iter().map(|(_, v)| *v.1).take(n);
        iter.collect()
    }

    /// Get a random peer from the known ring connections.
    pub fn random_peer<F>(&self, filter_fn: F) -> Option<PeerKeyLocation>
    where
        F: FnMut(&&PeerKeyLocation) -> bool,
    {
        self.connections_by_location
            .read()
            .values()
            .find(filter_fn)
            .copied()
    }

    /// Will return an error in case the max number of subscribers has been added.
    pub fn add_subscriber(
        &self,
        contract: ContractKey,
        subscriber: PeerKeyLocation,
    ) -> Result<(), ()> {
        let mut subs = self
            .subscribers
            .entry(contract)
            .or_insert(Vec::with_capacity(Self::MAX_SUBSCRIBERS));
        if let Err(next_idx) = subs.value_mut().binary_search(&subscriber) {
            let subs = subs.value_mut();
            if subs.len() == Self::MAX_SUBSCRIBERS {
                return Err(());
            } else {
                subs.insert(next_idx, subscriber);
            }
        }
        Ok(())
    }

    /// Add a new subscription for this peer.
    pub fn add_subscription(&self, contract: ContractKey) {
        self.subscriptions.write().push(contract);
    }

    pub fn subscribers_of(
        &self,
        contract: &ContractKey,
    ) -> Option<DmRef<ContractKey, Vec<PeerKeyLocation>>> {
        self.subscribers.get(contract)
    }

    pub fn num_connections(&self) -> usize {
        self.connections_by_location.read().len()
    }
}

/// An abstract location on the 1D ring, represented by a real number on the interal [0, 1]
#[derive(Debug, serde::Serialize, serde::Deserialize, Clone, Copy)]
pub struct Location(pub(crate) f64);

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
    type Error = anyhow::Error;

    fn try_from(value: f64) -> Result<Self, Self::Error> {
        if !(0.0..=1.0).contains(&value) {
            bail!("expected a value between 0.0 and 1.0, received {}", value)
        } else {
            Ok(Location(value))
        }
    }
}

#[derive(thiserror::Error, Debug)]
pub(crate) enum RingError {
    #[error(transparent)]
    ConnError(#[from] Box<conn_manager::ConnError>),
    #[error("no ring connections found")]
    EmptyRing,
    #[error("ran out of, or haven't found any, caching peers for contract {0}")]
    NoCachingPeers(ContractKey),
}

#[cfg(test)]
mod test {
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
        let ring = Ring::new(PeerKey::random());

        fn build_pk(loc: Location) -> PeerKeyLocation {
            PeerKeyLocation {
                peer: PeerKey::random(),
                location: Some(loc),
            }
        }

        {
            let conns = &mut *ring.connections_by_location.write();
            conns.insert(Location(0.3), build_pk(Location(0.3)));
            conns.insert(Location(0.5), build_pk(Location(0.5)));
            conns.insert(Location(0.0), build_pk(Location(0.0)));
        }

        assert_eq!(
            Location(0.0),
            ring.routing(&Location(0.9), 1, &[])
                .first()
                .unwrap()
                .location
                .unwrap()
        );
        assert_eq!(
            Location(0.0),
            ring.routing(&Location(0.1), 1, &[])
                .first()
                .unwrap()
                .location
                .unwrap()
        );
        assert_eq!(
            Location(0.5),
            ring.routing(&Location(0.41), 1, &[])
                .first()
                .unwrap()
                .location
                .unwrap()
        );
        assert_eq!(
            Location(0.3),
            ring.routing(&Location(0.39), 1, &[])
                .first()
                .unwrap()
                .location
                .unwrap()
        );
    }
}
