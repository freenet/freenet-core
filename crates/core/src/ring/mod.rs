//! Ring protocol logic and supporting types.
//!
//! Mainly maintains a healthy and optimal pool of connections to other peers in the network
//! and routes requests to the optimal peers.

use std::collections::{BTreeMap, BTreeSet, HashSet};
use std::net::SocketAddr;
use std::{
    sync::{atomic::AtomicU64, Arc, Weak},
    time::{Duration, Instant},
};
use tracing::Instrument;

use dashmap::mapref::one::Ref as DmRef;
use either::Either;
use freenet_stdlib::prelude::ContractKey;
use parking_lot::RwLock;

use crate::message::TransactionType;
use crate::topology::rate::Rate;
use crate::topology::TopologyAdjustment;
use crate::tracing::{NetEventLog, NetEventRegister};

use crate::transport::{ObservedAddr, TransportPublicKey};
use crate::util::Contains;
use crate::{
    config::GlobalExecutor,
    message::{NetMessage, NetMessageV1, Transaction},
    node::{self, EventLoopNotificationsSender, NodeConfig, OpManager, PeerId},
    operations::{connect::ConnectOp, OpEnum},
    router::Router,
};

mod connection_manager;
pub(crate) use connection_manager::ConnectionManager;
mod connection;
mod live_tx;
mod location;
mod peer_key_location;
mod seeding;
mod seeding_cache;

pub use self::live_tx::LiveTransactionTracker;
pub use connection::Connection;
pub use location::{Distance, Location};
#[allow(unused_imports)] // PeerAddr will be used as refactoring progresses
pub use peer_key_location::{PeerAddr, PeerKeyLocation};

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
    seeding_manager: seeding::SeedingManager,
    event_register: Box<dyn NetEventRegister>,
    op_manager: RwLock<Option<Weak<OpManager>>>,
    /// Whether this peer is a gateway or not. This will affect behavior of the node when acquiring
    /// and dropping connections.
    pub(crate) is_gateway: bool,
}

// /// A data type that represents the fact that a peer has been blacklisted
// /// for some action. Has to be coupled with that action
// #[derive(Debug)]
// struct Blacklisted {
//     since: Instant,
//     peer: PeerKey,
// }

impl Ring {
    const DEFAULT_MIN_CONNECTIONS: usize = 25;

    const DEFAULT_MAX_CONNECTIONS: usize = 200;

    const DEFAULT_MAX_UPSTREAM_BANDWIDTH: Rate = Rate::new_per_second(1_000_000.0);

    const DEFAULT_MAX_DOWNSTREAM_BANDWIDTH: Rate = Rate::new_per_second(1_000_000.0);

    /// Above this number of remaining hops, randomize which node a message which be forwarded to.
    const DEFAULT_RAND_WALK_ABOVE_HTL: usize = 7;

    /// Max hops to be performed for certain operations (e.g. propagating connection of a peer in the network).
    pub const DEFAULT_MAX_HOPS_TO_LIVE: usize = 10;

    pub fn new<ER: NetEventRegister + Clone>(
        config: &NodeConfig,
        event_loop_notifier: EventLoopNotificationsSender,
        event_register: ER,
        is_gateway: bool,
        connection_manager: ConnectionManager,
    ) -> anyhow::Result<Arc<Self>> {
        let live_tx_tracker = LiveTransactionTracker::new();

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
            seeding_manager: seeding::SeedingManager::new(),
            live_tx_tracker: live_tx_tracker.clone(),
            event_register: Box::new(event_register),
            op_manager: RwLock::new(None),
            is_gateway,
        };

        if let Some(loc) = config.location {
            if config.own_addr.is_none() && is_gateway {
                return Err(anyhow::anyhow!("own_addr is required for gateways"));
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
                .connection_maintenance(event_loop_notifier, live_tx_tracker)
                .instrument(span),
        );
        Ok(ring)
    }

    pub fn attach_op_manager(&self, op_manager: &Arc<OpManager>) {
        self.op_manager.write().replace(Arc::downgrade(op_manager));
    }

    fn upgrade_op_manager(&self) -> Option<Arc<OpManager>> {
        self.op_manager
            .read()
            .as_ref()
            .and_then(|weak| weak.clone().upgrade())
    }

    pub fn is_gateway(&self) -> bool {
        self.is_gateway
    }

    pub fn open_connections(&self) -> usize {
        self.connection_manager.connection_count()
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
                    tracing::error!(error = %error, "Shutting down refresh router task");
                    error
                })
                .expect("todo: propagate this to main thread");
            if !history.is_empty() {
                let router_ref = &mut *router.write();
                *router_ref = Router::new(&history);
            }
        }
    }

    /// Record an access to a contract (GET, PUT, or SUBSCRIBE).
    ///
    /// This adds the contract to the seeding cache if not present, or refreshes
    /// its LRU position if already cached. Returns the list of evicted contracts
    /// that need cleanup (unsubscription, state removal, etc.).
    ///
    /// The `size_bytes` should be the size of the contract state.
    pub fn seed_contract(&self, key: ContractKey, size_bytes: u64) -> Vec<ContractKey> {
        use seeding_cache::AccessType;
        self.seeding_manager
            .record_contract_access(key, size_bytes, AccessType::Put)
    }

    /// Record a GET access to a contract.
    pub fn record_get_access(&self, key: ContractKey, size_bytes: u64) -> Vec<ContractKey> {
        use seeding_cache::AccessType;
        self.seeding_manager
            .record_contract_access(key, size_bytes, AccessType::Get)
    }

    /// Record a subscribe access for a contract (for future use when subscribe
    /// operations directly record access rather than delegating to GET).
    #[allow(dead_code)]
    pub fn record_subscribe_access(&self, key: ContractKey, size_bytes: u64) -> Vec<ContractKey> {
        use seeding_cache::AccessType;
        self.seeding_manager
            .record_contract_access(key, size_bytes, AccessType::Subscribe)
    }

    /// Whether this node already is seeding to this contract or not.
    #[inline]
    pub fn is_seeding_contract(&self, key: &ContractKey) -> bool {
        self.seeding_manager.is_seeding_contract(key)
    }

    /// Remove a contract from the seeding cache (for future use in cleanup paths).
    #[allow(dead_code)]
    pub fn remove_seeded_contract(&self, key: &ContractKey) -> bool {
        self.seeding_manager.remove_seeded_contract(key)
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
        tracing::info!(
            peer = %peer,
            peer_location = %loc,
            this = ?self.connection_manager.get_own_addr(),
            was_reserved = %was_reserved,
            "Adding connection to peer"
        );
        let addr = peer.addr;
        let pub_key = peer.pub_key.clone();
        self.connection_manager
            .add_connection(loc, addr, pub_key, was_reserved);
        self.event_register
            .register_events(Either::Left(NetEventLog::connected(self, peer, loc)))
            .await;
        self.refresh_density_request_cache()
    }

    pub fn update_connection_identity(&self, old_peer: &PeerId, new_peer: PeerId) {
        if self.connection_manager.update_peer_identity(
            old_peer.addr,
            new_peer.addr,
            new_peer.pub_key,
        ) {
            self.refresh_density_request_cache();
        }
    }

    fn refresh_density_request_cache(&self) {
        let cbl = self.connection_manager.get_connections_by_location();
        let topology_manager = &mut self.connection_manager.topology_manager.write();
        let _ = topology_manager.refresh_cache(&cbl);
    }

    /// Returns a filtered iterator for peers that are not connected to this node already.
    pub fn is_not_connected<'a>(
        &self,
        peers: impl Iterator<Item = &'a PeerKeyLocation>,
    ) -> impl Iterator<Item = &'a PeerKeyLocation> + Send {
        let mut filtered = Vec::new();
        for peer in peers {
            if let Some(addr) = peer.socket_addr() {
                if !self.connection_manager.has_connection_or_pending(addr) {
                    filtered.push(peer);
                }
            } else {
                // If address is unknown, include the peer
                filtered.push(peer);
            }
        }
        filtered.into_iter()
    }

    /// Return the most optimal peer for caching a given contract.
    ///
    /// This function only considers connected peers, not the node itself.
    #[inline]
    pub fn closest_potentially_caching(
        &self,
        contract_key: &ContractKey,
        skip_list: impl Contains<std::net::SocketAddr>,
    ) -> Option<PeerKeyLocation> {
        let router = self.router.read();
        self.connection_manager
            .routing(Location::from(contract_key), None, skip_list, &router)
    }

    /// Get k best peers for caching a contract, ranked by routing predictions
    pub fn k_closest_potentially_caching(
        &self,
        contract_key: &ContractKey,
        skip_list: impl Contains<std::net::SocketAddr> + Clone,
        k: usize,
    ) -> Vec<PeerKeyLocation> {
        let router = self.router.read();
        let target_location = Location::from(contract_key);

        let mut seen = HashSet::new();
        let mut candidates: Vec<PeerKeyLocation> = Vec::new();

        let connections = self.connection_manager.get_connections_by_location();
        for conns in connections.values() {
            for conn in conns {
                if let Some(addr) = conn.location.socket_addr() {
                    if skip_list.has_element(addr) || !seen.insert(addr) {
                        continue;
                    }
                }
                candidates.push(conn.location.clone());
            }
        }

        // Note: We intentionally do NOT fall back to known_locations here.
        // known_locations may contain peers we're not currently connected to,
        // and attempting to route to them would require establishing a new connection
        // which may fail (especially in NAT scenarios without coordination).
        // It's better to return fewer candidates than unreachable ones.

        router
            .select_k_best_peers(candidates.iter(), target_location, k)
            .into_iter()
            .cloned()
            .collect()
    }

    pub fn routing_finished(&self, event: crate::router::RouteEvent) {
        self.connection_manager
            .topology_manager
            .write()
            .report_outbound_request(event.peer.clone(), event.contract_location);
        self.router.write().add_event(event);
    }

    /// Will return an error in case the max number of subscribers has been added.
    ///
    /// The `upstream_addr` parameter is the transport-level address from which the subscribe
    /// message was received. This is used instead of the address embedded in `subscriber`
    /// because NAT peers may embed incorrect (e.g., loopback) addresses in their messages.
    /// The transport address is the only reliable way to route back to them.
    pub fn add_subscriber(
        &self,
        contract: &ContractKey,
        subscriber: PeerKeyLocation,
        upstream_addr: Option<ObservedAddr>,
    ) -> Result<(), ()> {
        self.seeding_manager
            .add_subscriber(contract, subscriber, upstream_addr)
    }

    /// Remove a subscriber by peer ID from a specific contract
    pub fn remove_subscriber(&self, contract: &ContractKey, peer: &PeerId) {
        self.seeding_manager
            .remove_subscriber_by_peer(contract, peer)
    }

    pub fn subscribers_of(
        &self,
        contract: &ContractKey,
    ) -> Option<DmRef<'_, ContractKey, Vec<PeerKeyLocation>>> {
        self.seeding_manager.subscribers_of(contract)
    }

    /// Get all network subscriptions across all contracts
    pub fn all_network_subscriptions(&self) -> Vec<(ContractKey, Vec<PeerKeyLocation>)> {
        self.seeding_manager.all_subscriptions()
    }

    pub async fn prune_connection(&self, peer: PeerId) {
        tracing::debug!(peer = %peer, "Removing connection");
        self.live_tx_tracker.prune_transactions_from_peer(peer.addr);
        // This case would be when a connection is being open, so peer location hasn't been recorded yet and we can ignore everything below
        let Some(_loc) = self.connection_manager.prune_alive_connection(peer.addr) else {
            return;
        };
        // Use address-based pruning for O(1) lookup via reverse index
        self.seeding_manager.prune_subscriber_by_addr(peer.addr);
        self.event_register
            .register_events(Either::Left(NetEventLog::disconnected(self, &peer)))
            .await;
    }

    async fn connection_maintenance(
        self: Arc<Self>,
        notifier: EventLoopNotificationsSender,
        live_tx_tracker: LiveTransactionTracker,
    ) -> anyhow::Result<()> {
        let is_gateway = self.is_gateway;
        tracing::info!(is_gateway, "Connection maintenance task starting");
        #[cfg(not(test))]
        const CHECK_TICK_DURATION: Duration = Duration::from_secs(60);
        #[cfg(test)]
        const CHECK_TICK_DURATION: Duration = Duration::from_secs(2);

        const REGENERATE_DENSITY_MAP_INTERVAL: Duration = Duration::from_secs(60);

        /// Maximum number of concurrent connection acquisition attempts.
        /// Allows parallel connection attempts to speed up network formation
        /// instead of serial blocking on a single connection at a time.
        const MAX_CONCURRENT_CONNECTIONS: usize = 3;

        let mut check_interval = tokio::time::interval(CHECK_TICK_DURATION);
        check_interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        let mut refresh_density_map = tokio::time::interval(REGENERATE_DENSITY_MAP_INTERVAL);
        refresh_density_map.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

        // if the peer is just starting wait a bit before
        // we even attempt acquiring more connections
        tokio::time::sleep(Duration::from_secs(2)).await;

        let mut pending_conn_adds = BTreeSet::new();
        let mut this_peer = None;
        loop {
            let op_manager = match self.upgrade_op_manager() {
                Some(op_manager) => op_manager,
                None => {
                    tokio::time::sleep(Duration::from_millis(100)).await;
                    continue;
                }
            };
            let Some(this_addr) = &this_peer else {
                let Some(addr) = self.connection_manager.get_own_addr() else {
                    tokio::time::sleep(Duration::from_secs(1)).await;
                    continue;
                };
                this_peer = Some(addr);
                continue;
            };
            // avoid connecting to the same peer multiple times
            let mut skip_list = HashSet::new();
            skip_list.insert(*this_addr);

            // Acquire new connections up to MAX_CONCURRENT_CONNECTIONS limit
            // Only count Connect transactions, not all operations (Get/Put/Subscribe/Update)
            let active_count = live_tx_tracker.active_connect_transaction_count();
            if let Some(ideal_location) = pending_conn_adds.pop_first() {
                if active_count < MAX_CONCURRENT_CONNECTIONS {
                    tracing::debug!(
                        active_connections = active_count,
                        max_concurrent = MAX_CONCURRENT_CONNECTIONS,
                        target_location = %ideal_location,
                        "Attempting to acquire new connection"
                    );
                    let tx = self
                        .acquire_new(
                            ideal_location,
                            &skip_list,
                            &notifier,
                            &live_tx_tracker,
                            &op_manager,
                        )
                        .await
                        .map_err(|error| {
                            tracing::error!(
                                error = ?error,
                                "FATAL: Connection maintenance task failed - shutting down"
                            );
                            error
                        })?;
                    if tx.is_none() {
                        let conns = self.connection_manager.connection_count();
                        tracing::warn!(
                            connections = conns,
                            target_location = %ideal_location,
                            "acquire_new returned None - likely no peers to query through"
                        );
                    } else {
                        tracing::debug!(
                            active_connections = active_count + 1,
                            "Successfully initiated connection acquisition"
                        );
                    }
                } else {
                    tracing::debug!(
                        active_connections = active_count,
                        max_concurrent = MAX_CONCURRENT_CONNECTIONS,
                        target_location = %ideal_location,
                        "At max concurrent connections, re-queuing location"
                    );
                    pending_conn_adds.insert(ideal_location);
                }
            }

            let current_connections = self.connection_manager.connection_count();
            let pending_connection_targets = pending_conn_adds.len();
            let peers = self.connection_manager.get_connections_by_location();
            let connections_considered: usize = peers.values().map(|c| c.len()).sum();

            let mut neighbor_locations: BTreeMap<_, Vec<_>> = peers
                .iter()
                .map(|(loc, conns)| {
                    let conns: Vec<_> = conns
                        .iter()
                        .filter(|conn| {
                            conn.location
                                .socket_addr()
                                .map(|addr| !live_tx_tracker.has_live_connection(addr))
                                .unwrap_or(true)
                        })
                        .cloned()
                        .collect();
                    (*loc, conns)
                })
                .filter(|(_, conns)| !conns.is_empty())
                .collect();

            if neighbor_locations.is_empty() && connections_considered > 0 {
                tracing::warn!(
                    current_connections,
                    connections_considered,
                    live_tx_peers = live_tx_tracker.len(),
                    "Neighbor filtering removed all candidates; using all connections"
                );

                neighbor_locations = peers
                    .iter()
                    .map(|(loc, conns)| (*loc, conns.clone()))
                    .filter(|(_, conns)| !conns.is_empty())
                    .collect();
            }

            if current_connections > self.connection_manager.max_connections {
                // When over capacity, consider all connections for removal regardless of live_tx filter.
                neighbor_locations = peers.clone();
            }

            tracing::debug!(
                current_connections,
                candidates = peers.len(),
                live_tx_peers = live_tx_tracker.len(),
                "Evaluating topology maintenance"
            );

            let adjustment = self
                .connection_manager
                .topology_manager
                .write()
                .adjust_topology(
                    &neighbor_locations,
                    &self.connection_manager.own_location().location(),
                    Instant::now(),
                    current_connections,
                );

            tracing::debug!(
                adjustment = ?adjustment,
                current_connections,
                is_gateway,
                pending_adds = pending_connection_targets,
                "Topology adjustment result"
            );

            match adjustment {
                TopologyAdjustment::AddConnections(target_locs) => {
                    let allowed = calculate_allowed_connection_additions(
                        current_connections,
                        pending_connection_targets,
                        self.connection_manager.min_connections,
                        self.connection_manager.max_connections,
                        target_locs.len(),
                    );

                    if allowed == 0 {
                        tracing::debug!(
                            requested = target_locs.len(),
                            current_connections,
                            pending = pending_connection_targets,
                            min_connections = self.connection_manager.min_connections,
                            max_connections = self.connection_manager.max_connections,
                            "Skipping queuing new connection targets – backlog already satisfies capacity constraints"
                        );
                    } else {
                        let total_pending_after = pending_connection_targets + allowed;
                        tracing::debug!(
                            requested = target_locs.len(),
                            allowed,
                            total_pending_after,
                            "Queuing additional connection targets"
                        );
                        pending_conn_adds.extend(target_locs.into_iter().take(allowed));
                    }
                }
                TopologyAdjustment::RemoveConnections(mut should_disconnect_peers) => {
                    for peer in should_disconnect_peers.drain(..) {
                        if let Some(addr) = peer.socket_addr() {
                            notifier
                                .notifications_sender
                                .send(Either::Right(crate::message::NodeEvent::DropConnection(
                                    addr,
                                )))
                                .await
                                .map_err(|error| {
                                    tracing::debug!(
                                        error = ?error,
                                        "Shutting down connection maintenance task"
                                    );
                                    error
                                })?;
                        }
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

    #[tracing::instrument(level = "debug", skip(self, notifier, live_tx_tracker, op_manager), fields(peer = %self.connection_manager.pub_key))]
    async fn acquire_new(
        &self,
        ideal_location: Location,
        skip_list: &HashSet<SocketAddr>,
        notifier: &EventLoopNotificationsSender,
        live_tx_tracker: &LiveTransactionTracker,
        op_manager: &Arc<OpManager>,
    ) -> anyhow::Result<Option<Transaction>> {
        let current_connections = self.connection_manager.connection_count();
        let is_gateway = self.is_gateway;

        tracing::debug!(
            current_connections,
            is_gateway,
            target_location = %ideal_location,
            "acquire_new: attempting to find peer to query"
        );

        let query_target = {
            let router = self.router.read();
            let num_connections = self.connection_manager.num_connections();
            tracing::debug!(
                target_location = %ideal_location,
                num_connections,
                skip_list_size = skip_list.len(),
                self_addr = ?self.connection_manager.get_own_addr(),
                "Looking for peer to route through"
            );
            if let Some(target) =
                self.connection_manager
                    .routing(ideal_location, None, skip_list, &router)
            {
                tracing::debug!(
                    query_target = %target,
                    target_location = %ideal_location,
                    "connection_maintenance selected routing target"
                );
                target
            } else {
                tracing::warn!(
                    current_connections,
                    is_gateway,
                    target_location = %ideal_location,
                    "acquire_new: routing() returned None - cannot find peer to query"
                );
                return Ok(None);
            }
        };

        let joiner = self.connection_manager.own_location();
        tracing::debug!(
            this_peer = %joiner,
            query_target_peer = %query_target,
            target_location = %ideal_location,
            "Sending connect request via connection_maintenance"
        );
        let ttl = self.max_hops_to_live.max(1).min(u8::MAX as usize) as u8;
        let target_connections = self.connection_manager.min_connections;

        let (tx, op, msg) = ConnectOp::initiate_join_request(
            joiner,
            query_target.clone(),
            ideal_location,
            ttl,
            target_connections,
            op_manager.connect_forward_estimator.clone(),
        );

        if let Some(addr) = query_target.socket_addr() {
            live_tx_tracker.add_transaction(addr, tx);
        }
        op_manager
            .push(tx, OpEnum::Connect(Box::new(op)))
            .await
            .map_err(|err| anyhow::anyhow!(err))?;
        notifier
            .notifications_sender
            .send(Either::Left(NetMessage::V1(NetMessageV1::Connect(msg))))
            .await?;
        tracing::debug!(tx = %tx, "Connect request sent");
        Ok(Some(tx))
    }
}

fn calculate_allowed_connection_additions(
    current_connections: usize,
    pending_connections: usize,
    min_connections: usize,
    max_connections: usize,
    requested: usize,
) -> usize {
    if requested == 0 {
        return 0;
    }

    let effective_connections = current_connections.saturating_add(pending_connections);
    if effective_connections >= max_connections {
        return 0;
    }

    let mut available_capacity = max_connections - effective_connections;

    if current_connections < min_connections {
        let deficit_to_min = min_connections.saturating_sub(effective_connections);
        available_capacity = available_capacity.min(deficit_to_min);
    }

    available_capacity.min(requested)
}

#[cfg(test)]
mod pending_additions_tests {
    use super::calculate_allowed_connection_additions;

    #[test]
    fn respects_minimum_when_backlog_exists() {
        let allowed = calculate_allowed_connection_additions(1, 24, 25, 200, 24);
        assert_eq!(allowed, 0, "Backlog should satisfy minimum deficit");
    }

    #[test]
    fn permits_requests_until_minimum_is_met() {
        let allowed = calculate_allowed_connection_additions(1, 0, 25, 200, 24);
        assert_eq!(allowed, 24);
    }

    #[test]
    fn caps_additions_at_available_capacity() {
        let allowed = calculate_allowed_connection_additions(190, 5, 25, 200, 10);
        assert_eq!(allowed, 5);
    }

    #[test]
    fn respects_requested_when_capacity_allows() {
        let allowed = calculate_allowed_connection_additions(50, 0, 25, 200, 3);
        assert_eq!(allowed, 3);
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
