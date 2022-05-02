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
    sync::{
        atomic::{AtomicU64, AtomicUsize, Ordering::SeqCst},
        Arc,
    },
};

use anyhow::bail;
use dashmap::{mapref::one::Ref as DmRef, DashMap, DashSet};
use locutus_runtime::prelude::ContractKey;
use parking_lot::RwLock;
use serde::{Deserialize, Serialize};

use crate::{
    node::{self, PeerKey},
    NodeConfig,
};

#[derive(Debug, Serialize, Deserialize, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
/// The location of a peer in the ring. This location allows routing towards the peer.
pub(crate) struct PeerKeyLocation {
    pub peer: PeerKey,
    /// An unspecified location means that the peer hasn't been asigned a location, yet.
    pub location: Option<Location>,
}

impl From<PeerKey> for PeerKeyLocation {
    fn from(peer: PeerKey) -> Self {
        PeerKeyLocation {
            peer,
            location: None,
        }
    }
}

/// Thread safe and friendly data structure to keep track of the local knowledge
/// of the state of the ring.
#[derive(Debug, Clone)]
pub(crate) struct Ring {
    pub rnd_if_htl_above: usize,
    pub max_hops_to_live: usize,
    pub peer_key: PeerKey,
    max_connections: usize,
    min_connections: usize,
    connections_by_location: Arc<RwLock<BTreeMap<Location, PeerKeyLocation>>>,
    location_for_peer: Arc<RwLock<BTreeMap<PeerKey, Location>>>,
    /// contracts in the ring cached by this node
    cached_contracts: DashSet<ContractKey>,
    own_location: Arc<AtomicU64>,
    /// The container for subscriber is a vec instead of something like a hashset
    /// that would allow for blind inserts of duplicate peers subscribing because
    /// of data locality, since we are likely to end up iterating over the whole sequence
    /// of subscribers more often than inserting, and anyways is a relatively short sequence
    /// then is more optimal to just use a vector for it's compact memory layout.
    subscribers: Arc<DashMap<ContractKey, Vec<PeerKeyLocation>>>,
    subscriptions: Arc<RwLock<Vec<ContractKey>>>,

    // A peer which has been blacklisted to perform actions regarding a given contract.
    // todo: add blacklist
    // contract_blacklist: Arc<DashMap<ContractKey, Vec<Blacklisted>>>,
    /// Interim connections ongoing haandshake or successfully open connections
    /// Is important to keep track of this so no more connections are accepted prematurely.
    open_connections: Arc<AtomicUsize>,
}

// /// A data type that represents the fact that a peer has been blacklisted
// /// for some action. Has to be coupled with that action
// #[derive(Debug)]
// struct Blacklisted {
//     since: Instant,
//     peer: PeerKey,
// }

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

    pub fn new<const CLIENTS: usize>(
        config: &NodeConfig<CLIENTS>,
        gateways: &[PeerKeyLocation],
    ) -> Result<Self, anyhow::Error> {
        let peer_key = PeerKey::from(config.local_key.public());

        // for location here consider -1 == None
        let own_location = Arc::new(AtomicU64::new(u64::from_le_bytes((-1f64).to_le_bytes())));

        let max_hops_to_live = if let Some(v) = config.max_hops_to_live {
            v
        } else {
            Self::MAX_HOPS_TO_LIVE
        };

        let rnd_if_htl_above = if let Some(v) = config.rnd_if_htl_above {
            v
        } else {
            Self::RAND_WALK_ABOVE_HTL
        };

        let min_connections = if let Some(v) = config.min_number_conn {
            v
        } else {
            Self::MIN_CONNECTIONS
        };

        let max_connections = if let Some(v) = config.max_number_conn {
            v
        } else {
            Self::MAX_CONNECTIONS
        };

        let ring = Ring {
            rnd_if_htl_above,
            max_hops_to_live,
            max_connections,
            min_connections,
            connections_by_location: Arc::new(RwLock::new(BTreeMap::new())),
            location_for_peer: Arc::new(RwLock::new(BTreeMap::new())),
            cached_contracts: DashSet::new(),
            own_location,
            peer_key,
            subscribers: Arc::new(DashMap::new()),
            subscriptions: Arc::new(RwLock::new(Vec::new())),
            // contract_blacklist: Arc::new(DashMap::new()),
            open_connections: Arc::new(AtomicUsize::new(0)),
        };

        if let Some(loc) = config.location {
            if config.local_ip.is_none() || config.local_port.is_none() {
                return Err(anyhow::anyhow!("IP and port are required for gateways"));
            }
            ring.update_location(Some(loc));
            for PeerKeyLocation { peer, location } in gateways {
                // all gateways are aware of each other
                ring.add_connection((*location).unwrap(), *peer);
            }
        }

        Ok(ring)
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
    pub fn is_contract_cached(&self, key: &ContractKey) -> bool {
        self.cached_contracts.contains(key)
    }

    #[inline]
    pub fn contract_cached(&self, key: ContractKey) {
        self.cached_contracts.insert(key);
    }

    /// Update this node location.
    pub fn update_location(&self, loc: Option<Location>) {
        if let Some(loc) = loc {
            self.own_location
                .store(u64::from_le_bytes(loc.0.to_le_bytes()), SeqCst)
        } else {
            self.own_location
                .store(u64::from_le_bytes((-1f64).to_le_bytes()), SeqCst)
        }
    }

    /// Returns this node location in the ring, if any (must have join the ring already).
    pub fn own_location(&self) -> PeerKeyLocation {
        log::debug!("Getting loc for peer {}", self.peer_key);
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
    ///
    /// # Panic
    /// Will panic if the node checking for this condition has no location assigned.
    pub fn should_accept(&self, location: &Location) -> bool {
        let open_conn = self.open_connections.fetch_add(1, SeqCst) + 1;
        let my_location = &self
            .own_location()
            .location
            .expect("this node has no location assigned!");
        let cbl = &*self.connections_by_location.read();
        let accepted = if location == my_location || cbl.contains_key(location) {
            false
        } else if open_conn < self.min_connections {
            true
        } else if open_conn >= self.max_connections {
            false
        } else {
            my_location.distance(location)
                < self
                    .median_distance_to(my_location)
                    .unwrap_or_else(|| Distance::try_from(0.5).unwrap())
        };
        if !accepted {
            self.open_connections.fetch_sub(1, SeqCst);
        }
        accepted
    }

    pub fn add_connection(&self, loc: Location, peer: PeerKey) {
        let mut cbl = self.connections_by_location.write();
        self.location_for_peer.write().insert(peer, loc);
        cbl.insert(
            loc,
            PeerKeyLocation {
                peer,
                location: Some(loc),
            },
        );
    }

    /// Returns the median distance to other peers for the node. None if there are
    /// no other active connections.
    pub fn median_distance_to(&self, location: &Location) -> Option<Distance> {
        let connections = self.connections_by_location.read();
        if connections.is_empty() {
            return None;
        }
        let mut conn_by_dist: Vec<_> = connections
            .keys()
            .map(|key| key.distance(location))
            .collect();
        conn_by_dist.sort_unstable();
        let idx = self.connections_by_location.read().len() / 2;
        Some(conn_by_dist[idx])
    }

    /// Return the closest peers to a contract location which are caching it,
    /// excluding whichever peers in the skip list.
    #[inline]
    pub fn closest_caching(
        &self,
        contract_key: &ContractKey,
        n: usize,
        skip_list: &[PeerKey],
    ) -> Vec<PeerKeyLocation> {
        // Right now we return just the closest known peers to that location.
        // In the future this may change to the ones closest which are actually already caching it.
        self.routing(&Location::from(contract_key), None, n, skip_list)
    }

    /// Find the closest number of peers to a given location. Result is returned sorted by proximity.
    pub fn routing(
        &self,
        target: &Location,
        requesting: Option<&PeerKey>,
        n: usize,
        skip_list: &[PeerKey],
    ) -> Vec<PeerKeyLocation> {
        let connections = self.connections_by_location.read();
        let mut conn_by_dist: Vec<_> = connections
            .iter()
            .filter(|(_, pkloc)| {
                if let Some(requester) = requesting {
                    if requester == &pkloc.peer {
                        return false;
                    }
                }
                !skip_list.contains(&pkloc.peer)
            })
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
        if subs.len() >= Self::MAX_SUBSCRIBERS {
            return Err(());
        }
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

    pub fn prune_connection(&self, peer: PeerKey) {
        let loc = self.location_for_peer.write().remove(&peer).unwrap();
        {
            let conns = &mut *self.connections_by_location.write();
            conns.remove(&loc);
        }
        {
            self.subscribers.alter_all(|_, mut subs| {
                if let Some(pos) = subs.iter().position(|l| l.location == Some(loc)) {
                    subs.swap_remove(pos);
                }
                subs
            });
        }
        self.open_connections.fetch_sub(1, SeqCst);
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
impl From<&ContractKey> for Location {
    fn from(key: &ContractKey) -> Self {
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
        (self.0 - other.0).abs() < f64::EPSILON
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
    ConnError(#[from] Box<node::ConnectionError>),
    #[error("No ring connections found")]
    EmptyRing,
    #[error("Ran out of, or haven't found any, caching peers for contract {0}")]
    NoCachingPeers(ContractKey),
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::client_events::test::MemoryEventsGen;
    use tokio::sync::watch::channel;

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
        let peer_key: PeerKey = PeerKey::random();

        let (_, receiver) = channel((0, peer_key));
        let user_events = MemoryEventsGen::new(receiver, peer_key);
        let config = NodeConfig::new([Box::new(user_events)]);
        let ring = Ring::new(&config, &[]).unwrap();

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
            ring.routing(&Location(0.9), None, 1, &[])
                .first()
                .unwrap()
                .location
                .unwrap()
        );
        assert_eq!(
            Location(0.0),
            ring.routing(&Location(0.1), None, 1, &[])
                .first()
                .unwrap()
                .location
                .unwrap()
        );
        assert_eq!(
            Location(0.5),
            ring.routing(&Location(0.41), None, 1, &[])
                .first()
                .unwrap()
                .location
                .unwrap()
        );
        assert_eq!(
            Location(0.3),
            ring.routing(&Location(0.39), None, 1, &[])
                .first()
                .unwrap()
                .location
                .unwrap()
        );
    }
}
