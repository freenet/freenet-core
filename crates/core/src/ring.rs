//! Ring protocol logic and supporting types.
//!
//! Mainly maintains a healthy and optimal pool of connections to other peers in the network
//! and routes requests to the optimal peers.

use std::collections::VecDeque;
use std::hash::Hash;
use std::sync::atomic::AtomicBool;
use std::{
    cmp::Reverse,
    collections::BTreeMap,
    convert::TryFrom,
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
use parking_lot::RwLock;
use rand::seq::SliceRandom;
use rand::Rng;
use serde::{Deserialize, Serialize};
use tokio::sync;
use tracing::Instrument;

use crate::message::TransactionType;
use crate::topology::meter::ResourceType;
use crate::topology::rate::Rate;
use crate::topology::{AcquisitionStrategy, Limits, TopologyAdjustment, TopologyManager};
use crate::tracing::{NetEventLog, NetEventRegister};
use crate::util::Contains;
use crate::{
    config::GlobalExecutor,
    message::Transaction,
    node::{self, EventLoopNotificationsSender, NodeConfig, PeerId},
    operations::connect,
    router::Router,
    DynError,
};

#[derive(Serialize, Deserialize, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
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

#[derive(Clone, Copy)]
pub(crate) struct Connection {
    location: PeerKeyLocation,
    open_at: Instant,
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
            .map(|entry| *entry.key())
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

/// Thread safe and friendly data structure to keep track of the local knowledge
/// of the state of the ring.
///
// Note: For now internally we wrap some of the types internally with locks and/or use
// multithreaded maps. In the future if performance requires it some of this can be moved
// towards a more lock-free multithreading model if necessary.
pub(crate) struct Ring {
    pub rnd_if_htl_above: usize,
    pub max_hops_to_live: usize,
    pub peer_key: PeerId,
    pub max_connections: usize,
    pub min_connections: usize,
    router: Arc<RwLock<Router>>,
    topology_manager: RwLock<TopologyManager>,
    /// Fast is for when there are less than our target number of connections so we want to acquire new connections quickly.
    /// Slow is for when there are enough connections so we need to drop a connection in order to replace it.
    fast_acquisition: AtomicBool,
    connections_by_location: RwLock<BTreeMap<Location, Vec<Connection>>>,
    location_for_peer: RwLock<BTreeMap<PeerId, Location>>,
    own_location: AtomicU64,
    /// The container for subscriber is a vec instead of something like a hashset
    /// that would allow for blind inserts of duplicate peers subscribing because
    /// of data locality, since we are likely to end up iterating over the whole sequence
    /// of subscribers more often than inserting, and anyways is a relatively short sequence
    /// then is more optimal to just use a vector for it's compact memory layout.
    subscribers: DashMap<ContractKey, Vec<PeerKeyLocation>>,
    /// Contracts this peer is subscribed to.
    subscriptions: DashMap<ContractKey, PeerKeyLocation>,
    /// Interim connections ongoing handshake or successfully open connections
    /// Is important to keep track of this so no more connections are accepted prematurely.
    open_connections: AtomicUsize,
    pub live_tx_tracker: LiveTransactionTracker,
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

    /// Above this number of remaining hops, randomize which node a message which be forwarded to.
    const DEFAULT_RAND_WALK_ABOVE_HTL: usize = 7;

    /// Max hops to be performed for certain operations (e.g. propagating connection of a peer in the network).
    const DEFAULT_MAX_HOPS_TO_LIVE: usize = 10;

    pub fn new<ER: NetEventRegister + Clone>(
        config: &NodeConfig,
        event_loop_notifier: EventLoopNotificationsSender,
        event_register: ER,
        is_gateway: bool,
    ) -> Result<Arc<Self>, anyhow::Error> {
        let (live_tx_tracker, missing_candidate_rx) = LiveTransactionTracker::new();

        let peer_key = config.peer_id;

        // for location here consider -1 == None
        let own_location = AtomicU64::new(u64::from_le_bytes((-1f64).to_le_bytes()));

        let max_hops_to_live = if let Some(v) = config.max_hops_to_live {
            v
        } else {
            Self::DEFAULT_MAX_HOPS_TO_LIVE
        };

        let rnd_if_htl_above = if let Some(v) = config.rnd_if_htl_above {
            v
        } else {
            Self::DEFAULT_RAND_WALK_ABOVE_HTL
        };

        let min_connections = if let Some(v) = config.min_number_conn {
            v
        } else {
            Self::DEFAULT_MIN_CONNECTIONS
        };

        let max_connections = if let Some(v) = config.max_number_conn {
            v
        } else {
            Self::DEFAULT_MAX_CONNECTIONS
        };

        let max_upstream_bandwidth = if let Some(v) = config.max_upstream_bandwidth {
            v
        } else {
            Self::DEFAULT_MAX_UPSTREAM_BANDWIDTH
        };

        let max_downstream_bandwidth = if let Some(v) = config.max_downstream_bandwidth {
            v
        } else {
            Self::DEFAULT_MAX_DOWNSTREAM_BANDWIDTH
        };

        let topology_manager = RwLock::new(TopologyManager::new(Limits {
            max_upstream_bandwidth,
            max_downstream_bandwidth,
            min_connections,
            max_connections,
        }));

        let router = Arc::new(RwLock::new(Router::new(&[])));
        GlobalExecutor::spawn(Self::refresh_router(router.clone(), event_register.clone()));

        // Just initialize with a fake location, this will be later updated when the peer has an actual location assigned.
        let ring = Ring {
            rnd_if_htl_above,
            max_hops_to_live,
            max_connections,
            min_connections,
            router,
            topology_manager,
            fast_acquisition: AtomicBool::new(true),
            connections_by_location: RwLock::new(BTreeMap::new()),
            location_for_peer: RwLock::new(BTreeMap::new()),
            own_location,
            peer_key,
            subscribers: DashMap::new(),
            subscriptions: DashMap::new(),
            open_connections: AtomicUsize::new(0),
            live_tx_tracker: live_tx_tracker.clone(),
            event_register: Box::new(event_register),
            is_gateway,
        };

        if let Some(loc) = config.location {
            if config.local_ip.is_none() || config.local_port.is_none() {
                return Err(anyhow::anyhow!("IP and port are required for gateways"));
            }
            ring.update_location(Some(loc));
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

    pub fn open_connections(&self) -> usize {
        self.open_connections
            .load(std::sync::atomic::Ordering::Acquire)
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
                    tracing::error!("shutting down refresh router task");
                    error
                })
                .expect("todo: propagate this to main thread");
            if !history.is_empty() {
                let router_ref = &mut *router.write();
                *router_ref = Router::new(&history);
            }
        }
    }

    /// Return if a location is within appropiate subscription distance.
    pub fn within_subscribing_distance(&self, loc: &Location) -> bool {
        const CACHING_DISTANCE: f64 = 0.05;
        const MAX_CACHED: usize = 100;
        const MIN_CACHED: usize = MAX_CACHED / 4;
        let caching_distance = Distance::new(CACHING_DISTANCE);
        if self.subscriptions.len() < MIN_CACHED {
            return true;
        }
        self.subscriptions.len() < MAX_CACHED
            && self
                .own_location()
                .location
                .map(|own_loc| own_loc.distance(loc) <= caching_distance)
                .unwrap_or(false)
    }

    /// Whether this node already is subscribed to this contract or not.
    #[inline]
    pub fn is_subscribed_to_contract(&self, key: &ContractKey) -> bool {
        self.subscriptions.contains_key(key)
    }

    #[inline]
    pub fn subscribed_to_contract(&self, key: &ContractKey) -> Option<PeerKeyLocation> {
        self.subscriptions.get(key).map(|v| *v.value())
    }

    /// Update this node location.
    pub fn update_location(&self, loc: Option<Location>) {
        if let Some(loc) = loc {
            self.own_location.store(
                u64::from_le_bytes(loc.0.to_le_bytes()),
                std::sync::atomic::Ordering::Release,
            );
        } else {
            self.own_location.store(
                u64::from_le_bytes((-1f64).to_le_bytes()),
                std::sync::atomic::Ordering::Release,
            )
        }
    }

    /// Returns this node location in the ring, if any (must have join the ring already).
    pub fn own_location(&self) -> PeerKeyLocation {
        let location = f64::from_le_bytes(
            self.own_location
                .load(std::sync::atomic::Ordering::Acquire)
                .to_le_bytes(),
        );
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
    pub fn should_accept(&self, location: Location, peer: &PeerId) -> bool {
        let open_conn = self
            .open_connections
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        if self.location_for_peer.read().get(peer).is_some() {
            // avoid connecting mroe than once to the same peer
            self.open_connections
                .fetch_sub(1, std::sync::atomic::Ordering::SeqCst);
            return false;
        }
        let my_location = self
            .own_location()
            .location
            .expect("this node has no location assigned!");
        let accepted = if location == my_location
            || self.connections_by_location.read().contains_key(&location)
        {
            false
        } else if open_conn < self.min_connections {
            true
        } else if open_conn >= self.max_connections {
            false
        } else {
            let strategy = if self
                .fast_acquisition
                .load(std::sync::atomic::Ordering::Acquire)
            {
                AcquisitionStrategy::Fast
            } else {
                AcquisitionStrategy::Slow
            };
            self.topology_manager
                .write()
                .evaluate_new_connection(location, strategy, Instant::now())
                .unwrap_or(false)
        };
        if !accepted {
            self.open_connections
                .fetch_sub(1, std::sync::atomic::Ordering::SeqCst);
        }
        accepted
    }

    pub fn record_request(&self, requested_location: Location, request_type: TransactionType) {
        self.topology_manager
            .write()
            .record_request(requested_location, request_type);
    }

    pub fn add_connection(&self, loc: Location, peer: PeerId) {
        let mut cbl = self.connections_by_location.write();
        self.event_register
            .register_events(Either::Left(NetEventLog::connected(self, peer, loc)));
        cbl.entry(loc).or_default().push(Connection {
            location: PeerKeyLocation {
                peer,
                location: Some(loc),
            },
            open_at: Instant::now(),
        });
        self.location_for_peer.write().insert(peer, loc);
        std::mem::drop(cbl);
        self.refresh_density_request_cache()
    }

    fn refresh_density_request_cache(&self) {
        let cbl = self.connections_by_location.read();
        let topology_manager = &mut self.topology_manager.write();
        let _ = topology_manager.refresh_cache(&cbl);
    }

    /// Return the most optimal peer caching a given contract.
    #[inline]
    pub fn closest_potentially_caching(
        &self,
        contract_key: &ContractKey,
        skip_list: impl Contains<PeerId>,
    ) -> Option<PeerKeyLocation> {
        self.routing(Location::from(contract_key), None, skip_list)
    }

    /// Route an op to the most optimal target.
    pub fn routing(
        &self,
        target: Location,
        requesting: Option<&PeerId>,
        skip_list: impl Contains<PeerId>,
    ) -> Option<PeerKeyLocation> {
        let connections = self.connections_by_location.read();
        let peers = connections.values().filter_map(|conns| {
            let conn = conns.choose(&mut rand::thread_rng()).unwrap();
            if let Some(requester) = requesting {
                if requester == &conn.location.peer {
                    return None;
                }
            }
            (!skip_list.has_element(&conn.location.peer)).then_some(&conn.location)
        });
        let router = &*self.router.read();
        router.select_peer(peers, target).cloned()
    }

    pub fn routing_finished(&self, event: crate::router::RouteEvent) {
        self.topology_manager
            .write()
            .report_outbound_request(event.peer, event.contract_location);
        self.router.write().add_event(event);
    }

    /// Get a random peer from the known ring connections.
    pub fn random_peer<F>(&self, filter_fn: F) -> Option<PeerKeyLocation>
    where
        F: Fn(&PeerId) -> bool,
    {
        let peers = &*self.location_for_peer.read();
        let amount = peers.len();
        if amount == 0 {
            return None;
        }
        let mut rng = rand::thread_rng();
        let mut attempts = 0;
        loop {
            if attempts >= amount * 2 {
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
    pub fn add_subscription(&self, contract: ContractKey, peer: PeerKeyLocation) {
        self.subscriptions.insert(contract, peer);
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

    pub fn prune_connection(&self, peer: PeerId) {
        #[cfg(debug_assertions)]
        {
            tracing::info!(%peer, "Removing connection");
        }
        self.live_tx_tracker.prune_transactions_from_peer(&peer);
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
        self.event_register
            .register_events(Either::Left(NetEventLog::disconnected(self, &peer)));
        self.open_connections
            .fetch_sub(1, std::sync::atomic::Ordering::SeqCst);
    }

    pub fn closest_to_location(
        &self,
        location: Location,
        skip_list: &[PeerId],
    ) -> Option<PeerKeyLocation> {
        self.connections_by_location
            .read()
            .range(..location)
            .filter_map(|(_, conns)| {
                let conn = conns.choose(&mut rand::thread_rng()).unwrap();
                (!skip_list.contains(&conn.location.peer)).then_some(conn.location)
            })
            .next_back()
    }

    async fn connection_maintenance(
        self: Arc<Self>,
        notifier: EventLoopNotificationsSender,
        live_tx_tracker: LiveTransactionTracker,
        mut missing_candidates: sync::mpsc::Receiver<PeerId>,
    ) -> Result<(), DynError> {
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
        'outer: loop {
            //
            loop {
                match missing_candidates.try_recv() {
                    Ok(missing_candidate) => {
                        missing.insert(Reverse(Instant::now()), missing_candidate);
                    }
                    Err(sync::mpsc::error::TryRecvError::Empty) => break,
                    Err(sync::mpsc::error::TryRecvError::Disconnected) => {
                        tracing::debug!("Shutting down connection maintenance");
                        break 'outer Err("finished".into());
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
                let peers = self.connections_by_location.read();
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

            // todo: when we are actually trackign the resources, pass the resource type
            // which is most constraint at the moment when calling this function
            let adjustment = self.topology_manager.write().adjust_topology(
                ResourceType::InboundBandwidthBytes,
                &neighbor_locations,
                Instant::now(),
            );
            match adjustment {
                TopologyAdjustment::AddConnections(target_locs) => {
                    pending_conn_adds.extend(target_locs);
                    continue;
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

    #[tracing::instrument(level = "debug", skip(self, notifier))]
    async fn acquire_new(
        &self,
        ideal_location: Location,
        skip_list: &[&PeerId],
        notifier: &EventLoopNotificationsSender,
    ) -> Result<Option<Transaction>, DynError> {
        use crate::message::InnerMessage;
        let Some(query_target) = self.routing(ideal_location, None, skip_list) else {
            return Ok(None);
        };
        let joiner = self.own_location();
        tracing::debug!(
            this_peer = %joiner,
            %query_target,
            %ideal_location,
            "Adding new connections"
        );
        let missing_connections = self.max_connections - self.open_connections();
        let msg = connect::ConnectMsg::Request {
            id: Transaction::new::<connect::ConnectMsg>(),
            msg: connect::ConnectRequest::FindOptimalPeer {
                query_target,
                ideal_location,
                joiner,
                max_hops_to_live: missing_connections,
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
