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
    collections::BTreeMap,
    convert::TryFrom,
    fmt::Display,
    hash::Hasher,
    sync::{
        atomic::{AtomicU64, AtomicUsize, Ordering::SeqCst},
        Arc,
    },
    time::Duration, ops::Add,
};

use anyhow::bail;
use dashmap::{mapref::one::Ref as DmRef, DashMap, DashSet};
use freenet_stdlib::prelude::ContractKey;
use parking_lot::RwLock;
use serde::{Deserialize, Serialize};

use crate::{
    config::GlobalExecutor,
    node::{self, EventLogRegister, EventRegister, NodeBuilder, PeerKey},
    router::Router,
};

#[derive(Debug, Serialize, Deserialize, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
/// The location of a peer in the ring. This location allows routing towards the peer.
pub(crate) struct PeerKeyLocation {
    pub peer: PeerKey,
    /// An unspecified location means that the peer hasn't been asigned a location, yet.
    pub location: Option<Location>,
}

impl PeerKeyLocation {
    #[cfg(test)]
    pub fn random() -> Self {
        PeerKeyLocation {
            peer: PeerKey::random(),
            location: Some(Location::random()),
        }
    }
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
#[derive(Debug)]
pub(crate) struct Ring {
    pub rnd_if_htl_above: usize,
    pub max_hops_to_live: usize,
    pub peer_key: PeerKey,
    max_connections: usize,
    min_connections: usize,
    router: Arc<RwLock<Router>>,
    connections_by_location: RwLock<BTreeMap<Location, PeerKeyLocation>>,
    location_for_peer: RwLock<BTreeMap<PeerKey, Location>>,
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

    // A peer which has been blacklisted to perform actions regarding a given contract.
    // todo: add blacklist
    // contract_blacklist: Arc<DashMap<ContractKey, Vec<Blacklisted>>>,
    /// Interim connections ongoing haandshake or successfully open connections
    /// Is important to keep track of this so no more connections are accepted prematurely.
    open_connections: AtomicUsize,
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

    pub fn new<const CLIENTS: usize, EL: EventLogRegister>(
        config: &NodeBuilder<CLIENTS>,
        gateways: &[PeerKeyLocation],
    ) -> Result<Self, anyhow::Error> {
        let peer_key = PeerKey::from(config.local_key.public());

        // for location here consider -1 == None
        let own_location = AtomicU64::new(u64::from_le_bytes((-1f64).to_le_bytes()));

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

        let router = Arc::new(RwLock::new(Router::new(&[])));
        GlobalExecutor::spawn(Self::refresh_router::<EL>(router.clone()));

        let ring = Ring {
            rnd_if_htl_above,
            max_hops_to_live,
            max_connections,
            min_connections,
            router,
            connections_by_location: RwLock::new(BTreeMap::new()),
            location_for_peer: RwLock::new(BTreeMap::new()),
            cached_contracts: DashSet::new(),
            own_location,
            peer_key,
            subscribers: DashMap::new(),
            subscriptions: RwLock::new(Vec::new()),
            // contract_blacklist: Arc::new(DashMap::new()),
            open_connections: AtomicUsize::new(0),
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

    async fn refresh_router<EL: EventLogRegister>(router: Arc<RwLock<Router>>) {
        let mut interval = tokio::time::interval(Duration::from_secs(60 * 5));
        interval.tick().await;
        loop {
            interval.tick().await;
            let history = if std::any::type_name::<EL>() == std::any::type_name::<EventRegister>() {
                EventRegister::get_router_events(10_000).await.unwrap()
            } else {
                vec![]
            };
            let router_ref = &mut *router.write();
            *router_ref = Router::new(&history);
        }
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
    pub fn contract_cached(&self, key: &ContractKey) {
        self.cached_contracts.insert(key.clone());
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
        let cbl = &*self.connections_by_location.read();
        let open_conn = self.open_connections.fetch_add(1, SeqCst) + 1;
        let my_location = &self
            .own_location()
            .location
            .expect("this node has no location assigned!");
        let accepted = if location == my_location || cbl.contains_key(location) {
            false
        } else if open_conn < self.min_connections {
            true
        } else if open_conn >= self.max_connections {
            tracing::debug!(peer = %self.peer_key, "max connections reached");
            false
        } else {
            // todo: in the future maybe use the `small worldness` metric to decide
            let median_distance = self
                .median_distance_to(my_location)
                .unwrap_or(Distance(0.5));
            let dist_to_loc = my_location.distance(location);
            let is_lower_than_median = dist_to_loc < median_distance;
            tracing::debug!("dist to connection loc: {dist_to_loc}, median dist: {median_distance}, accepting: {is_lower_than_median}");
            is_lower_than_median
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

    /// Return the most optimal peer caching a given contract.
    #[inline]
    pub fn closest_caching(
        &self,
        contract_key: &ContractKey,
        skip_list: &[PeerKey],
    ) -> Option<PeerKeyLocation> {
        self.routing(&Location::from(contract_key), None, skip_list)
    }

    /// Route an op to the most optimal target.
    pub fn routing(
        &self,
        target: &Location,
        requesting: Option<&PeerKey>,
        skip_list: &[PeerKey],
    ) -> Option<PeerKeyLocation> {
        let connections = self.connections_by_location.read();
        let peers = connections
            .iter()
            .filter(|(_, pkloc)| {
                if let Some(requester) = requesting {
                    if requester == &pkloc.peer {
                        return false;
                    }
                }
                !skip_list.contains(&pkloc.peer)
            })
            .map(|(_, peer)| peer);
        let router = &*self.router.read();
        router.select_peer(peers, target).cloned()
    }

    pub fn routing_finished(&self, event: crate::router::RouteEvent) {
        self.router.write().add_event(event);
    }

    /// Get a random peer from the known ring connections.
    pub fn random_peer<F>(&self, filter_fn: F) -> Option<PeerKeyLocation>
    where
        F: Fn(&PeerKey) -> bool,
    {
        use rand::Rng;
        let peers = &*self.location_for_peer.read();
        let amount = peers.len();
        if amount == 0 {
            return None;
        }
        let mut rng = rand::thread_rng();
        let mut attempts = 0;
        loop {
            if attempts >= amount {
                return None;
            }
            let selected = rng.gen_range(0..amount);
            let (peer, loc) = peers.iter().nth(selected).expect("infallible");
            if !filter_fn(peer) {
                attempts += 1;
                continue;
            } else {
                return Some(PeerKeyLocation {
                    peer: *peer,
                    location: Some(*loc),
                });
            }
        }
    }

    /// Will return an error in case the max number of subscribers has been added.
    pub fn add_subscriber(
        &self,
        contract: &ContractKey,
        subscriber: PeerKeyLocation,
    ) -> Result<(), ()> {
        let mut subs = self
            .subscribers
            .entry(contract.clone())
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
pub struct Location(f64);

impl Location {
    pub fn new(location: f64) -> Self {
        debug_assert!(
            (0.0..=1.0).contains(&location),
            "Location must be in the range [0, 1]"
        );
        Location(location)
    }

    /// Returns a new location rounded to ensure it is between 0.0 and 1.0
    pub fn new_rounded(location: f64) -> Self {
        Self::new(location.rem_euclid(1.0))
    }

    /// Returns a new random location.
    pub fn random() -> Self {
        use rand::prelude::*;
        let mut rng = rand::thread_rng();
        Location(rng.gen_range(0.0..=1.0))
    }

    /// Compute the distance between two locations.
    pub fn distance(&self, other: impl std::borrow::Borrow<Location>) -> Distance {
        let d = (self.0 - other.borrow().0).abs();
        if d < 0.5f64 {
            Distance::new(d)
        } else {
            Distance::new(1.0f64 - d)
        }
    }

    pub fn as_f64(&self) -> f64 {
        self.0
    }
}

/// Ensure at compile time locations can only be constructed from well formed contract keys
/// (which have been hashed with a strong, cryptographically safe, hash function first).
impl From<&ContractKey> for Location {
    fn from(key: &ContractKey) -> Self {
        let mut value = 0.0;
        let mut divisor = 256.0;
        for byte in key.bytes().iter().take(7) {
            value += *byte as f64 / divisor;
            divisor *= 256.0;
        }
        Location::try_from(value).expect("value should be between 0 and 1")
    }
}

impl Display for Location {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
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
        self.0
            .partial_cmp(&other.0)
            .expect("always should return a cmp value")
    }
}

#[allow(clippy::incorrect_partial_ord_impl_on_ord_type)]
impl PartialOrd for Location {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
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

#[derive(Debug, Copy, Clone)]
pub struct Distance(f64);

impl Distance {
    pub fn new(value: f64) -> Self {
        debug_assert!(!value.is_nan(), "Distance cannot be NaN");
        debug_assert!(
            (0.0..=1.0).contains(&value),
            "Distance must be in the range [0, 1.0]"
        );
        if value <= 0.5 {
            Distance(value)
        } else {
            Distance(1.0 - value)
        }
    }

    pub fn as_f64(&self) -> f64 {
        self.0
    }
}

impl Add for Distance {
    type Output = Self;

    fn add(self, rhs: Self) -> Self::Output {
        let d = self.0 + rhs.0;
        if d > 0.5 {
            Distance::new(1.0 - d)
        } else {
            Distance::new(d)
        }
    }
}

impl PartialEq for Distance {
    fn eq(&self, other: &Self) -> bool {
        (self.0 - other.0).abs() < f64::EPSILON
    }
}

#[allow(clippy::incorrect_partial_ord_impl_on_ord_type)]
impl PartialOrd for Distance {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Eq for Distance {}

impl Ord for Distance {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.0
            .partial_cmp(&other.0)
            .expect("always should return a cmp value")
    }
}

impl Display for Distance {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
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

    #[test]
    fn location_dist() {
        let l0 = Location(0.);
        let l1 = Location(0.25);
        assert!(l0.distance(l1) == Distance(0.25));

        let l0 = Location(0.75);
        let l1 = Location(0.50);
        assert!(l0.distance(l1) == Distance(0.25));
    }
}
