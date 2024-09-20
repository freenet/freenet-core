//! Ring protocol logic and supporting types.
//!
//! Mainly maintains a healthy and optimal pool of connections to other peers in the network
//! and routes requests to the optimal peers.

use std::collections::VecDeque;
use std::hash::Hash;
use std::net::SocketAddr;
use std::{
    cmp::Reverse,
    collections::BTreeMap,
    fmt::Display,
    hash::Hasher,
    ops::Add,
    sync::{
        atomic::{AtomicU64, AtomicUsize},
        Arc,
    },
    time::{Duration, Instant},
};

use anyhow::bail;
use dashmap::{mapref::one::Ref as DmRef, DashMap};
use either::Either;
use freenet_stdlib::prelude::{ContractInstanceId, ContractKey};
use itertools::Itertools;
use parking_lot::RwLock;
use rand::Rng;
use serde::{Deserialize, Serialize};
use tokio::sync;
use tracing::Instrument;

use crate::message::TransactionType;
use crate::topology::rate::Rate;
use crate::topology::TopologyAdjustment;
use crate::tracing::{NetEventLog, NetEventRegister};
use crate::transport::TransportPublicKey;
use crate::util::Contains;
use crate::{
    config::GlobalExecutor,
    message::Transaction,
    node::{self, EventLoopNotificationsSender, NodeConfig, PeerId},
    operations::connect,
    router::Router,
};

mod connection_manager;
pub(crate) use connection_manager::ConnectionManager;

#[derive(Serialize, Deserialize, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
#[cfg_attr(test, derive(arbitrary::Arbitrary))]
/// The location of a peer in the ring. This location allows routing towards the peer.
pub struct PeerKeyLocation {
    pub peer: PeerId,
    /// An unspecified location means that the peer hasn't been asigned a location, yet.
    pub location: Option<Location>,
}

impl PeerKeyLocation {
    #[cfg(test)]
    pub fn random() -> Self {
        PeerKeyLocation {
            peer: PeerId::random(),
            location: Some(Location::random()),
        }
    }
}

impl From<PeerId> for PeerKeyLocation {
    fn from(peer: PeerId) -> Self {
        PeerKeyLocation {
            peer,
            location: None,
        }
    }
}

impl std::fmt::Debug for PeerKeyLocation {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        <Self as Display>::fmt(self, f)
    }
}

impl Display for PeerKeyLocation {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self.location {
            Some(loc) => write!(f, "{} (@ {loc})", self.peer),
            None => write!(f, "{}", self.peer),
        }
    }
}

#[derive(Clone, Debug)]
pub(crate) struct Connection {
    location: PeerKeyLocation,
    open_at: Instant,
}

#[cfg(test)]
impl Connection {
    pub fn new(peer: PeerId, location: Location) -> Self {
        Connection {
            location: PeerKeyLocation {
                peer,
                location: Some(location),
            },
            open_at: Instant::now(),
        }
    }
}

#[derive(Clone)]
pub(crate) struct LiveTransactionTracker {
    tx_per_peer: Arc<DashMap<PeerId, Vec<Transaction>>>,
    missing_candidate_sender: sync::mpsc::Sender<PeerId>,
}

impl LiveTransactionTracker {
    /// The given peer does not have (good) candidates for acquiring new connections.
    pub async fn missing_candidate_peers(&self, peer: PeerId) {
        let _ = self
            .missing_candidate_sender
            .send(peer)
            .await
            .map_err(|error| {
                tracing::debug!(%error, "live transaction tracker channel closed");
                error
            });
    }

    pub fn add_transaction(&self, peer: PeerId, tx: Transaction) {
        self.tx_per_peer.entry(peer).or_default().push(tx);
    }

    pub fn remove_finished_transaction(&self, tx: Transaction) {
        let keys_to_remove: Vec<PeerId> = self
            .tx_per_peer
            .iter()
            .filter(|entry| entry.value().iter().any(|otx| otx == &tx))
            .map(|entry| entry.key().clone())
            .collect();

        for k in keys_to_remove {
            self.tx_per_peer.remove_if_mut(&k, |_, v| {
                v.retain(|otx| otx != &tx);
                v.is_empty()
            });
        }
    }

    fn new() -> (Self, sync::mpsc::Receiver<PeerId>) {
        let (missing_peer, rx) = sync::mpsc::channel(10);
        (
            Self {
                tx_per_peer: Arc::new(DashMap::default()),
                missing_candidate_sender: missing_peer,
            },
            rx,
        )
    }

    fn prune_transactions_from_peer(&self, peer: &PeerId) {
        self.tx_per_peer.remove(peer);
    }

    fn has_live_connection(&self, peer: &PeerId) -> bool {
        self.tx_per_peer.contains_key(peer)
    }

    fn still_alive(&self, tx: &Transaction) -> bool {
        self.tx_per_peer.iter().any(|e| e.value().contains(tx))
    }
}

#[derive(PartialEq, Clone, Copy)]
struct Score(f64);

impl PartialOrd for Score {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for Score {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.partial_cmp(other).unwrap_or(std::cmp::Ordering::Equal)
    }
}

impl Eq for Score {}

/// Thread safe and friendly data structure to keep track of the local knowledge
/// of the state of the ring.
///
// Note: For now internally we wrap some of the types internally with locks and/or use
// multithreaded maps. In the future if performance requires it some of this can be moved
// towards a more lock-free multithreading model if necessary.
pub(crate) struct Ring {
    pub max_hops_to_live: usize,
    pub connection_manager: ConnectionManager,
    pub router: Arc<RwLock<Router>>,
    pub live_tx_tracker: LiveTransactionTracker,
    peer_pub_key: TransportPublicKey,
    /// The container for subscriber is a vec instead of something like a hashset
    /// that would allow for blind inserts of duplicate peers subscribing because
    /// of data locality, since we are likely to end up iterating over the whole sequence
    /// of subscribers more often than inserting, and anyways is a relatively short sequence
    /// then is more optimal to just use a vector for it's compact memory layout.
    subscribers: DashMap<ContractKey, Vec<PeerKeyLocation>>,
    /// Contracts this peer is seeding.
    seeding_contract: DashMap<ContractKey, Score>,
    // A peer which has been blacklisted to perform actions regarding a given contract.
    // todo: add blacklist
    // contract_blacklist: Arc<DashMap<ContractKey, Vec<Blacklisted>>>,
    event_register: Box<dyn NetEventRegister>,
    /// Whether this peer is a gateway or not. This will affect behavior of the node when acquiring
    /// and dropping connections.
    #[allow(unused)]
    is_gateway: bool,
}

// /// A data type that represents the fact that a peer has been blacklisted
// /// for some action. Has to be coupled with that action
// #[derive(Debug)]
// struct Blacklisted {
//     since: Instant,
//     peer: PeerKey,
// }

impl Ring {
    const DEFAULT_MIN_CONNECTIONS: usize = 5;

    const DEFAULT_MAX_CONNECTIONS: usize = 200;

    const DEFAULT_MAX_UPSTREAM_BANDWIDTH: Rate = Rate::new_per_second(1_000_000.0);

    const DEFAULT_MAX_DOWNSTREAM_BANDWIDTH: Rate = Rate::new_per_second(1_000_000.0);

    /// Max number of subscribers for a contract.
    const MAX_SUBSCRIBERS: usize = 10;

    /// All subscribers, including the upstream subscriber.
    const TOTAL_MAX_SUBSCRIPTIONS: usize = Self::MAX_SUBSCRIBERS + 1;

    /// Above this number of remaining hops, randomize which node a message which be forwarded to.
    const DEFAULT_RAND_WALK_ABOVE_HTL: usize = 7;

    /// Max hops to be performed for certain operations (e.g. propagating connection of a peer in the network).
    pub const DEFAULT_MAX_HOPS_TO_LIVE: usize = 10;

    /// Max number of seeding contracts.
    const MAX_SEEDING_CONTRACTS: usize = 100;

    /// Min number of seeding contracts.
    const MIN_SEEDING_CONTRACTS: usize = Self::MAX_SEEDING_CONTRACTS / 4;

    pub fn new<ER: NetEventRegister + Clone>(
        config: &NodeConfig,
        event_loop_notifier: EventLoopNotificationsSender,
        event_register: ER,
        is_gateway: bool,
        connection_manager: ConnectionManager,
    ) -> anyhow::Result<Arc<Self>> {
        let (live_tx_tracker, missing_candidate_rx) = LiveTransactionTracker::new();

        let peer_pub_key = config.key_pair.public().clone();

        let max_hops_to_live = if let Some(v) = config.max_hops_to_live {
            v
        } else {
            Self::DEFAULT_MAX_HOPS_TO_LIVE
        };

        let router = Arc::new(RwLock::new(Router::new(&[])));
        GlobalExecutor::spawn(Self::refresh_router(router.clone(), event_register.clone()));

        // Just initialize with a fake location, this will be later updated when the peer has an actual location assigned.
        let ring = Ring {
            max_hops_to_live,
            router,
            connection_manager,
            peer_pub_key,
            subscribers: DashMap::new(),
            seeding_contract: DashMap::new(),
            live_tx_tracker: live_tx_tracker.clone(),
            event_register: Box::new(event_register),
            is_gateway,
        };

        if let Some(loc) = config.location {
            if config.peer_id.is_none() {
                return Err(anyhow::anyhow!("PeerId is required for gateways"));
            }
            ring.connection_manager.update_location(Some(loc));
        }

        let ring = Arc::new(ring);
        let current_span = tracing::Span::current();
        let span = if current_span.is_none() {
            tracing::info_span!("connection_maintenance")
        } else {
            tracing::info_span!(parent: current_span, "connection_maintenance")
        };

        GlobalExecutor::spawn(
            ring.clone()
                .connection_maintenance(event_loop_notifier, live_tx_tracker, missing_candidate_rx)
                .instrument(span),
        );

        Ok(ring)
    }

    pub fn get_peer_pub_key(&self) -> TransportPublicKey {
        self.peer_pub_key.clone()
    }

    pub fn is_gateway(&self) -> bool {
        self.is_gateway
    }

    pub fn open_connections(&self) -> usize {
        self.connection_manager
            .open_connections
            .load(std::sync::atomic::Ordering::SeqCst)
    }

    async fn refresh_router<ER: NetEventRegister>(router: Arc<RwLock<Router>>, register: ER) {
        let mut interval = tokio::time::interval(Duration::from_secs(60 * 5));
        interval.tick().await;
        loop {
            interval.tick().await;
            let history = register
                .get_router_events(10_000)
                .await
                .map_err(|error| {
                    tracing::error!(%error, "shutting down refresh router task");
                    error
                })
                .expect("todo: propagate this to main thread");
            if !history.is_empty() {
                let router_ref = &mut *router.write();
                *router_ref = Router::new(&history);
            }
        }
    }

    /// Return if a contract is within appropiate seeding distance.
    pub fn should_seed(&self, key: &ContractKey) -> bool {
        const CACHING_DISTANCE: f64 = 0.05;
        let caching_distance = Distance::new(CACHING_DISTANCE);
        if self.seeding_contract.len() < Self::MIN_SEEDING_CONTRACTS {
            return true;
        }
        let key_loc = Location::from(key);
        let own_loc = self
            .connection_manager
            .own_location()
            .location
            .expect("should be set");
        if self.seeding_contract.len() < Self::MAX_SEEDING_CONTRACTS {
            return own_loc.distance(key_loc) <= caching_distance;
        }

        let contract_score = self.calculate_seed_score(key);
        let r = self
            .seeding_contract
            .iter()
            .min_by_key(|v| *v.value())
            .unwrap();
        let min_score = *r.value();
        contract_score > min_score
    }

    /// Add a new subscription for this peer.
    pub fn seed_contract(&self, key: ContractKey) -> (Option<ContractKey>, Vec<PeerKeyLocation>) {
        let seed_score = self.calculate_seed_score(&key);
        let mut old_subscribers = vec![];
        let mut contract_to_drop = None;
        if self.seeding_contract.len() < Self::MAX_SEEDING_CONTRACTS {
            let dropped_contract = *self
                .seeding_contract
                .iter()
                .min_by_key(|v| *v.value())
                .unwrap()
                .key();
            self.seeding_contract.remove(&dropped_contract);
            if let Some((_, mut subscribers_of_contract)) =
                self.subscribers.remove(&dropped_contract)
            {
                std::mem::swap(&mut subscribers_of_contract, &mut old_subscribers);
            }
            contract_to_drop = Some(dropped_contract);
        }
        self.seeding_contract.insert(key, seed_score);
        (contract_to_drop, old_subscribers)
    }

    fn calculate_seed_score(&self, key: &ContractKey) -> Score {
        let location = self
            .connection_manager
            .own_location()
            .location
            .expect("should be set");
        let key_loc = Location::from(key);
        let distance = key_loc.distance(location);
        let score = 0.5 - distance.as_f64();
        Score(score)
    }

    /// Whether this node already is seeding to this contract or not.
    #[inline]
    pub fn is_seeding_contract(&self, key: &ContractKey) -> bool {
        self.seeding_contract.contains_key(key)
    }

    pub fn record_request(
        &self,
        recipient: PeerKeyLocation,
        target: Location,
        request_type: TransactionType,
    ) {
        self.connection_manager
            .topology_manager
            .write()
            .record_request(recipient, target, request_type);
    }

    pub async fn add_connection(&self, loc: Location, peer: PeerId, was_reserved: bool) {
        tracing::info!(%peer, this = ?self.connection_manager.get_peer_key(), %was_reserved, "Adding connection to peer");
        debug_assert!(
            self.connection_manager
                .get_peer_key()
                .expect("should be set")
                != peer
        );
        if was_reserved {
            let old = self
                .connection_manager
                .reserved_connections
                .fetch_sub(1, std::sync::atomic::Ordering::SeqCst);
            #[cfg(debug_assertions)]
            {
                tracing::debug!(old, "Decremented reserved connections");
                if old == 0 {
                    panic!("Underflow of reserved connections");
                }
            }
            let _ = old;
        }
        self.connection_manager
            .open_connections
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        self.event_register
            .register_events(Either::Left(NetEventLog::connected(
                self,
                peer.clone(),
                loc,
            )))
            .await;
        let mut cbl = self.connection_manager.connections_by_location.write();
        cbl.entry(loc).or_default().push(Connection {
            location: PeerKeyLocation {
                peer: peer.clone(),
                location: Some(loc),
            },
            open_at: Instant::now(),
        });
        self.connection_manager
            .location_for_peer
            .write()
            .insert(peer.clone(), loc);
        std::mem::drop(cbl);
        self.refresh_density_request_cache()
    }

    fn refresh_density_request_cache(&self) {
        let cbl = self.connection_manager.connections_by_location.read();
        let topology_manager = &mut self.connection_manager.topology_manager.write();
        let _ = topology_manager.refresh_cache(&cbl);
    }

    /// Returns a filtered iterator for peers that are not connected to this node already.
    pub fn is_not_connected<'a>(
        &self,
        peers: impl Iterator<Item = &'a PeerKeyLocation>,
    ) -> impl Iterator<Item = &'a PeerKeyLocation> + Send {
        let locs = &*self.connection_manager.location_for_peer.read();
        let mut filtered = Vec::new();
        for peer in peers {
            if !locs.contains_key(&peer.peer) {
                filtered.push(peer);
            }
        }
        filtered.into_iter()
    }

    /// Return the most optimal peer caching a given contract.
    #[inline]
    pub fn closest_potentially_caching(
        &self,
        contract_key: &ContractKey,
        skip_list: impl Contains<PeerId>,
    ) -> Option<PeerKeyLocation> {
        let router = self.router.read();
        self.connection_manager
            .routing(Location::from(contract_key), None, skip_list, &router)
    }

    pub fn routing_finished(&self, event: crate::router::RouteEvent) {
        self.connection_manager
            .topology_manager
            .write()
            .report_outbound_request(event.peer.clone(), event.contract_location);
        self.router.write().add_event(event);
    }

    pub fn register_subscription(&self, contract: &ContractKey, subscriber: PeerKeyLocation) {
        self.subscribers
            .entry(*contract)
            .or_insert(Vec::with_capacity(Self::TOTAL_MAX_SUBSCRIPTIONS))
            .value_mut()
            .push(subscriber);
    }

    /// Will return an error in case the max number of subscribers has been added.
    pub fn add_subscriber(
        &self,
        contract: &ContractKey,
        subscriber: PeerKeyLocation,
    ) -> Result<(), ()> {
        let mut subs = self
            .subscribers
            .entry(*contract)
            .or_insert(Vec::with_capacity(Self::TOTAL_MAX_SUBSCRIPTIONS));
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

    pub fn subscribers_of(
        &self,
        contract: &ContractKey,
    ) -> Option<DmRef<ContractKey, Vec<PeerKeyLocation>>> {
        self.subscribers.get(contract)
    }

    pub async fn prune_connection(&self, peer: PeerId) {
        tracing::debug!(%peer, "Removing connection");
        self.live_tx_tracker.prune_transactions_from_peer(&peer);
        // This case would be when a connection is being open, so peer location hasn't been recorded yet and we can ignore everything below
        let Some(loc) = self.connection_manager.prune_alive_connection(&peer) else {
            return;
        };
        {
            self.subscribers.alter_all(|_, mut subs| {
                if let Some(pos) = subs.iter().position(|l| l.location == Some(loc)) {
                    subs.swap_remove(pos);
                }
                subs
            });
        }
        self.event_register
            .register_events(Either::Left(NetEventLog::disconnected(self, &peer)))
            .await;
    }

    pub fn closest_to_location(
        &self,
        location: Location,
        skip_list: &[PeerId],
    ) -> Option<PeerKeyLocation> {
        use rand::seq::SliceRandom;
        self.connection_manager
            .connections_by_location
            .read()
            .iter()
            .sorted_by(|(loc_a, _), (loc_b, _)| {
                loc_a.distance(location).cmp(&loc_b.distance(location))
            })
            .find_map(|(_, conns)| {
                for _ in 0..conns.len() {
                    let conn = conns.choose(&mut rand::thread_rng()).unwrap();
                    let selected =
                        (!skip_list.contains(&conn.location.peer)).then_some(conn.location.clone());
                    if selected.is_some() {
                        return selected;
                    }
                }
                None
            })
    }

    async fn connection_maintenance(
        self: Arc<Self>,
        notifier: EventLoopNotificationsSender,
        live_tx_tracker: LiveTransactionTracker,
        mut missing_candidates: sync::mpsc::Receiver<PeerId>,
    ) -> anyhow::Result<()> {
        #[cfg(not(test))]
        const CONNECTION_AGE_THRESOLD: Duration = Duration::from_secs(60 * 5);
        #[cfg(test)]
        const CONNECTION_AGE_THRESOLD: Duration = Duration::from_secs(5);
        const CHECK_TICK_DURATION: Duration = Duration::from_secs(10);
        const REGENERATE_DENSITY_MAP_INTERVAL: Duration = Duration::from_secs(60);

        let mut check_interval = tokio::time::interval(CHECK_TICK_DURATION);
        check_interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        let mut refresh_density_map = tokio::time::interval(REGENERATE_DENSITY_MAP_INTERVAL);
        refresh_density_map.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

        let mut missing = BTreeMap::new();

        #[cfg(not(test))]
        let retry_peers_missing_candidates_interval = Duration::from_secs(60 * 5) * 2;
        #[cfg(test)]
        let retry_peers_missing_candidates_interval = Duration::from_secs(5);

        // if the peer is just starting wait a bit before
        // we even attempt acquiring more connections
        tokio::time::sleep(Duration::from_secs(2)).await;

        let mut live_tx = None;
        let mut pending_conn_adds = VecDeque::new();
        loop {
            if self.connection_manager.get_peer_key().is_none() {
                tokio::time::sleep(Duration::from_secs(1)).await;
                continue;
            }
            loop {
                match missing_candidates.try_recv() {
                    Ok(missing_candidate) => {
                        missing.insert(Reverse(Instant::now()), missing_candidate);
                    }
                    Err(sync::mpsc::error::TryRecvError::Empty) => break,
                    Err(sync::mpsc::error::TryRecvError::Disconnected) => {
                        tracing::debug!("Shutting down connection maintenance");
                        anyhow::bail!("finished");
                    }
                }
            }

            // eventually peers which failed to return candidates should be retried when enough time has passed
            let retry_missing_candidates_until =
                Instant::now() - retry_peers_missing_candidates_interval;

            // remove all missing candidates which have been retried
            missing.split_off(&Reverse(retry_missing_candidates_until));

            if let Some(ideal_location) = pending_conn_adds.pop_front() {
                live_tx = self
                    .acquire_new(
                        ideal_location,
                        &missing.values().collect::<Vec<_>>(),
                        &notifier,
                    )
                    .await
                    .map_err(|error| {
                        tracing::debug!(?error, "Shutting down connection maintenance task");
                        error
                    })?;
            }

            // if there are no open connections, we need to acquire more
            if let Some(tx) = &live_tx {
                if !live_tx_tracker.still_alive(tx) {
                    let _ = live_tx.take();
                }
            }

            let neighbor_locations = {
                let peers = self.connection_manager.connections_by_location.read();
                peers
                    .iter()
                    .map(|(loc, conns)| {
                        let conns: Vec<_> = conns
                            .iter()
                            .filter(|conn| {
                                conn.open_at.elapsed() > CONNECTION_AGE_THRESOLD
                                    && !live_tx_tracker.has_live_connection(&conn.location.peer)
                            })
                            .cloned()
                            .collect();
                        (*loc, conns)
                    })
                    .filter(|(_, conns)| !conns.is_empty())
                    .collect()
            };

            let adjustment = self
                .connection_manager
                .topology_manager
                .write()
                .adjust_topology(
                    &neighbor_locations,
                    &self.connection_manager.own_location().location,
                    Instant::now(),
                );
            match adjustment {
                TopologyAdjustment::AddConnections(target_locs) => {
                    pending_conn_adds.extend(target_locs);
                }
                TopologyAdjustment::RemoveConnections(mut should_disconnect_peers) => {
                    for peer in should_disconnect_peers.drain(..) {
                        notifier
                            .send(Either::Right(crate::message::NodeEvent::DropConnection(
                                peer.peer,
                            )))
                            .await
                            .map_err(|error| {
                                tracing::debug!(
                                    ?error,
                                    "Shutting down connection maintenance task"
                                );
                                error
                            })?;
                    }
                }
                TopologyAdjustment::NoChange => {}
            }

            tokio::select! {
              _ = refresh_density_map.tick() => {
                self.refresh_density_request_cache();
              }
              _ = check_interval.tick() => {}
            }
        }
    }

    #[tracing::instrument(level = "debug", skip(self, notifier), fields(peer = %self.connection_manager.pub_key))]
    async fn acquire_new(
        &self,
        ideal_location: Location,
        skip_list: &[&PeerId],
        notifier: &EventLoopNotificationsSender,
    ) -> anyhow::Result<Option<Transaction>> {
        use crate::message::InnerMessage;
        let query_target = {
            let router = self.router.read();
            if let Some(t) =
                self.connection_manager
                    .routing(ideal_location, None, skip_list, &router)
            {
                t
            } else {
                return Ok(None);
            }
        };
        let joiner = self.connection_manager.own_location();
        tracing::debug!(
            this_peer = %joiner,
            %query_target,
            %ideal_location,
            "Adding new connections"
        );
        let missing_connections = self.connection_manager.max_connections - self.open_connections();
        let connected = self.connection_manager.connected_peers();
        let msg = connect::ConnectMsg::Request {
            id: Transaction::new::<connect::ConnectMsg>(),
            target: query_target.clone(),
            msg: connect::ConnectRequest::FindOptimalPeer {
                query_target,
                ideal_location,
                joiner,
                max_hops_to_live: missing_connections,
                skip_list: skip_list
                    .iter()
                    .map(|p| (*p).clone())
                    .chain(connected)
                    .collect(),
            },
        };
        let id = *msg.id();
        notifier.send(Either::Left(msg.into())).await?;
        Ok(Some(id))
    }
}

/// An abstract location on the 1D ring, represented by a real number on the interal [0, 1]
#[derive(Debug, serde::Serialize, serde::Deserialize, Clone, Copy)]
#[cfg_attr(test, derive(arbitrary::Arbitrary))]
pub struct Location(f64);

impl Location {
    #[cfg(not(feature = "local-simulation"))]
    pub fn from_address(addr: &SocketAddr) -> Self {
        match addr.ip() {
            std::net::IpAddr::V4(ipv4) => {
                let octets = ipv4.octets();
                let combined_octets = (u32::from(octets[0]) << 16)
                    | (u32::from(octets[1]) << 8)
                    | u32::from(octets[2]);
                Location(combined_octets as f64 / (u32::MAX as f64))
            }
            std::net::IpAddr::V6(ipv6) => {
                let segments = ipv6.segments();
                let combined_segments = (u64::from(segments[0]) << 32)
                    | (u64::from(segments[1]) << 16)
                    | u64::from(segments[2]);
                Location(combined_segments as f64 / (u64::MAX as f64))
            }
        }
    }

    #[cfg(feature = "local-simulation")]
    pub fn from_address(_addr: &SocketAddr) -> Self {
        let random_component: f64 = rand::random();
        Location(random_component)
    }

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

    fn from_contract_key(bytes: &[u8]) -> Self {
        let mut value = 0.0;
        let mut divisor = 256.0;
        for byte in bytes {
            value += *byte as f64 / divisor;
            divisor *= 256.0;
        }
        Location::try_from(value).expect("value should be between 0 and 1")
    }
}

impl std::ops::Add<Distance> for Location {
    type Output = (Location, Location);

    /// Returns the positive and directive locations on the ring  at the given distance.
    fn add(self, distance: Distance) -> Self::Output {
        let neg_loc = self.0 - distance.0;
        let pos_loc = self.0 + distance.0;
        (Location(neg_loc), Location(pos_loc))
    }
}

/// Ensure at compile time locations can only be constructed from well formed contract keys
/// (which have been hashed with a strong, cryptographically safe, hash function first).
impl From<&ContractKey> for Location {
    fn from(key: &ContractKey) -> Self {
        Self::from_contract_key(key.id().as_bytes())
    }
}

impl From<&ContractInstanceId> for Location {
    fn from(key: &ContractInstanceId) -> Self {
        Self::from_contract_key(key.as_bytes())
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
    #[error("No location assigned to this peer")]
    NoLocation,
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
