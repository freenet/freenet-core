//! Ring protocol logic and supporting types.
//!
//! Mainly maintains a healthy and optimal pool of connections to other peers in the network
//! and routes requests to the optimal peers.

use std::collections::{BTreeMap, BTreeSet, HashSet};
use std::net::SocketAddr;
use std::sync::{atomic::AtomicU64, Arc, Weak};
use std::time::Duration;
use tokio::time::Instant;

use tracing::Instrument;

use either::Either;
use freenet_stdlib::prelude::{ContractInstanceId, ContractKey};
use parking_lot::{Mutex, RwLock};

pub use hosting::{AddClientSubscriptionResult, ClientDisconnectResult, SubscribeResult};

use crate::message::TransactionType;
use crate::topology::rate::Rate;
use crate::topology::TopologyAdjustment;
use crate::tracing::{NetEventLog, NetEventRegister};

use crate::transport::TransportPublicKey;
use crate::util::Contains;
use crate::{
    config::{GlobalExecutor, GlobalRng},
    message::{NetMessage, NetMessageV1, Transaction},
    node::{self, EventLoopNotificationsSender, NodeConfig, OpManager, PeerId},
    operations::{connect::ConnectOp, OpEnum},
    router::Router,
};

mod connection_backoff;
mod connection_manager;
pub(crate) use connection_manager::ConnectionManager;
mod connection;
mod hosting;
mod hosting_cache;
pub use hosting_cache::AccessType;
pub mod interest;
mod live_tx;
mod location;
mod peer_connection_backoff;
mod peer_key_location;
pub mod topology_registry;

/// Whether to auto-subscribe to contracts on GET.
/// When true, GET operations will automatically subscribe to the contract
/// to receive updates. This is controlled by hosting cache eviction.
pub const AUTO_SUBSCRIBE_ON_GET: bool = true;

use connection_backoff::ConnectionBackoff;
pub use connection_backoff::ConnectionFailureReason;
pub(crate) use peer_connection_backoff::PeerConnectionBackoff;

pub use self::live_tx::LiveTransactionTracker;
pub use connection::Connection;
pub use interest::PeerKey;
pub use location::{Distance, Location};
pub use peer_key_location::{KnownPeerKeyLocation, PeerAddr, PeerKeyLocation};

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
    hosting_manager: hosting::HostingManager,
    event_register: Box<dyn NetEventRegister>,
    op_manager: RwLock<Option<Weak<OpManager>>>,
    /// Whether this peer is a gateway or not. This will affect behavior of the node when acquiring
    /// and dropping connections.
    pub(crate) is_gateway: bool,
    /// Shared connection backoff tracker for all connection failure types.
    connection_backoff: Arc<parking_lot::Mutex<ConnectionBackoff>>,
}

// /// A data type that represents the fact that a peer has been blacklisted
// /// for some action. Has to be coupled with that action
// #[derive(Debug)]
// struct Blacklisted {
//     since: Instant,
//     peer: PeerKey,
// }

/// Guard that ensures `complete_subscription_request` is called even if the
/// subscription task panics. This prevents contracts from being stuck in
/// `pending_subscription_requests` forever.
pub(crate) struct SubscriptionRecoveryGuard {
    op_manager: Arc<OpManager>,
    contract_key: ContractKey,
    completed: bool,
}

impl SubscriptionRecoveryGuard {
    pub(crate) fn new(op_manager: Arc<OpManager>, contract_key: ContractKey) -> Self {
        Self {
            op_manager,
            contract_key,
            completed: false,
        }
    }

    pub(crate) fn complete(mut self, success: bool) {
        self.op_manager
            .ring
            .complete_subscription_request(&self.contract_key, success);
        self.completed = true;
    }
}

impl Drop for SubscriptionRecoveryGuard {
    fn drop(&mut self) {
        if !self.completed {
            // Task panicked or was cancelled before completion - treat as failure
            tracing::warn!(
                contract = %self.contract_key,
                "Subscription recovery task terminated unexpectedly, marking as failed"
            );
            self.op_manager
                .ring
                .complete_subscription_request(&self.contract_key, false);
        }
    }
}

/// Result of pruning a connection.
#[derive(Debug, Default)]
pub struct PruneConnectionResult {
    /// Orphaned transactions that need to be retried or failed.
    pub orphaned_transactions: Vec<Transaction>,
}

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

        // Interval for periodic subscription state telemetry snapshots (1 minute)
        const SUBSCRIPTION_STATE_INTERVAL: Duration = Duration::from_secs(60);

        // Interval for periodic subscription recovery attempts (30 seconds)
        // This recovers "orphaned seeders" - peers that have contracts in cache
        // but failed to establish subscription (no upstream in subscription tree)
        const SUBSCRIPTION_RECOVERY_INTERVAL: Duration = Duration::from_secs(30);

        // Interval for GET subscription cache sweep (60 seconds)
        // Cleans up expired GET-triggered subscriptions and sends Unsubscribed messages
        //
        // Interval for topology snapshot registration (1 second in test mode)
        // Registers subscription topology with the global registry for validation
        #[cfg(any(test, feature = "testing"))]
        const TOPOLOGY_SNAPSHOT_INTERVAL: Duration = Duration::from_secs(1);
        const GET_SUBSCRIPTION_SWEEP_INTERVAL: Duration = Duration::from_secs(60);

        // Just initialize with a fake location, this will be later updated when the peer has an actual location assigned.
        let ring = Ring {
            max_hops_to_live,
            router,
            connection_manager,
            hosting_manager: hosting::HostingManager::new(),
            live_tx_tracker: live_tx_tracker.clone(),
            event_register: Box::new(event_register),
            op_manager: RwLock::new(None),
            is_gateway,
            connection_backoff: Arc::new(Mutex::new(ConnectionBackoff::new())),
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

        // Spawn periodic subscription state telemetry task
        GlobalExecutor::spawn(Self::emit_subscription_state_telemetry(
            ring.clone(),
            SUBSCRIPTION_STATE_INTERVAL,
        ));

        // Spawn periodic subscription recovery task to fix "orphaned seeders"
        // (peers that have contracts cached but aren't in the subscription tree)
        GlobalExecutor::spawn(Self::recover_orphaned_subscriptions(
            ring.clone(),
            SUBSCRIPTION_RECOVERY_INTERVAL,
        ));

        // Spawn periodic GET subscription cache sweep task
        // Cleans up expired GET-triggered subscriptions to maintain bounded memory
        GlobalExecutor::spawn(Self::sweep_get_subscription_cache(
            ring.clone(),
            GET_SUBSCRIPTION_SWEEP_INTERVAL,
        ));

        // Spawn periodic topology snapshot registration task (test mode only)
        // This allows SimNetwork to validate subscription topology during tests
        #[cfg(any(test, feature = "testing"))]
        GlobalExecutor::spawn(Self::register_topology_snapshots_periodically(
            ring.clone(),
            TOPOLOGY_SNAPSHOT_INTERVAL,
        ));

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

    /// Record a connection failure to the backoff tracker.
    pub fn record_connection_failure(&self, target: Location, reason: ConnectionFailureReason) {
        let mut backoff = self.connection_backoff.lock();
        backoff.record_failure_with_reason(target, reason);
    }

    /// Record a successful connection to clear backoff.
    pub fn record_connection_success(&self, target: Location) {
        let mut backoff = self.connection_backoff.lock();
        backoff.record_success(target);
    }

    /// Check if a target is currently in backoff.
    pub fn is_in_connection_backoff(&self, target: Location) -> bool {
        self.connection_backoff.lock().is_in_backoff(target)
    }

    /// Periodic cleanup of expired backoff entries.
    pub fn cleanup_connection_backoff(&self) {
        self.connection_backoff.lock().cleanup_expired();
    }

    /// Register events with the event system.
    /// This is used by operations to emit failure and other events.
    pub async fn register_events<'a>(
        &self,
        events: either::Either<
            crate::tracing::NetEventLog<'a>,
            Vec<crate::tracing::NetEventLog<'a>>,
        >,
    ) {
        self.event_register.register_events(events).await;
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

    /// Periodically emit subscription_state telemetry events for all active subscriptions.
    ///
    /// This enables the telemetry dashboard to reconstruct historical subscription trees
    /// and show accurate subscription state at any point in time.
    async fn emit_subscription_state_telemetry(ring: Arc<Self>, interval_duration: Duration) {
        let mut interval = tokio::time::interval(interval_duration);
        // Skip the first immediate tick
        interval.tick().await;

        loop {
            interval.tick().await;

            // Get subscription states from the new lease-based model
            let subscription_states = ring.get_subscription_states();

            if subscription_states.is_empty() {
                continue;
            }

            tracing::debug!(
                subscription_count = subscription_states.len(),
                "Emitting periodic subscription state telemetry"
            );

            // Log subscription states (simplified - no upstream/downstream in new model)
            for (key, has_client, is_active, _expires_at) in subscription_states {
                tracing::trace!(
                    %key,
                    has_client_subscription = has_client,
                    is_active_subscription = is_active,
                    "Subscription state"
                );
            }
        }
    }

    /// Maximum number of subscription recovery attempts per interval.
    /// This prevents spawning too many concurrent tasks if there are many orphaned contracts.
    const MAX_RECOVERY_ATTEMPTS_PER_INTERVAL: usize = 20;

    /// Periodically attempt to recover "orphaned seeders" - contracts we're seeding
    /// but don't have an upstream subscription for.
    ///
    /// This can happen when:
    /// - The initial subscription after GET/PUT failed (network issues, timeout)
    /// - Our upstream peer disconnected and we haven't found a new one
    /// - A race condition left us seeding without subscription
    ///
    /// The task respects existing backoff mechanisms to avoid subscription spam.
    async fn recover_orphaned_subscriptions(ring: Arc<Self>, interval_duration: Duration) {
        // Add random initial delay (30-60 seconds) to prevent synchronized recovery
        // across all peers. This avoids "thundering herd" problems and prevents the
        // recovery task from firing at exactly the same time as test stabilization
        // periods or other network operations that happen at fixed intervals.
        let initial_delay = Duration::from_secs(GlobalRng::random_range(30u64..=60u64));
        tokio::time::sleep(initial_delay).await;

        let mut interval = tokio::time::interval(interval_duration);
        // Skip the first immediate tick (we already waited above)
        interval.tick().await;

        loop {
            interval.tick().await;

            // First, expire any stale subscriptions
            let expired = ring.expire_stale_subscriptions();
            if !expired.is_empty() {
                tracing::debug!(
                    expired_count = expired.len(),
                    "Expired {} stale subscriptions",
                    expired.len()
                );
            }

            // Get contracts that need subscription renewal (have client subscriptions)
            let mut contracts_needing_renewal = ring.contracts_needing_renewal();

            if contracts_needing_renewal.is_empty() {
                continue;
            }

            // Shuffle to prevent starvation: without this, the same failing contracts
            // (first N in iteration order) would always be tried first, blocking later
            // contracts from ever being attempted when they hit the batch limit.
            GlobalRng::shuffle(&mut contracts_needing_renewal);

            // Get op_manager to spawn subscription requests
            let Some(op_manager) = ring.upgrade_op_manager() else {
                tracing::debug!("OpManager not available for subscription renewal");
                continue;
            };

            let mut attempted = 0;
            let mut skipped = 0;

            for contract in contracts_needing_renewal {
                // Limit concurrent renewal attempts to avoid overwhelming the network
                if attempted >= Self::MAX_RECOVERY_ATTEMPTS_PER_INTERVAL {
                    tracing::debug!(
                        limit = Self::MAX_RECOVERY_ATTEMPTS_PER_INTERVAL,
                        "Reached max renewal attempts for this interval, remaining will be tried next cycle"
                    );
                    break;
                }

                // Check spam prevention (respects exponential backoff and pending checks)
                if !ring.can_request_subscription(&contract) {
                    skipped += 1;
                    continue;
                }

                // Mark as pending and spawn subscription request
                if ring.mark_subscription_pending(contract) {
                    attempted += 1;
                    let op_manager_clone = op_manager.clone();
                    let contract_key = contract;

                    GlobalExecutor::spawn(async move {
                        // Guard ensures complete_subscription_request is called even on panic
                        let guard =
                            SubscriptionRecoveryGuard::new(op_manager_clone.clone(), contract_key);

                        let instance_id = *contract_key.id();
                        let sub_op = crate::operations::subscribe::start_op(instance_id);
                        let result = crate::operations::subscribe::request_subscribe(
                            &op_manager_clone,
                            sub_op,
                        )
                        .await;

                        let success = result.is_ok();
                        if success {
                            tracing::info!(
                                %contract_key,
                                "Subscription renewal succeeded"
                            );
                        } else if let Err(ref e) = result {
                            tracing::debug!(
                                %contract_key,
                                error = %e,
                                "Subscription renewal failed (will retry with backoff)"
                            );
                        }

                        // Mark as completed so guard doesn't treat it as failure
                        guard.complete(success);
                    });
                }
            }

            if attempted > 0 || skipped > 0 {
                tracing::info!(
                    attempted,
                    skipped_rate_limited = skipped,
                    "Subscription renewal: attempted {} renewals",
                    attempted
                );
            }
        }
    }

    /// Background task to sweep expired entries from the GET subscription cache.
    ///
    /// When contracts are evicted (past max entries and beyond TTL), this task
    /// cleans up the local subscription state. The upstream peer will eventually
    /// prune us when updates fail to deliver.
    async fn sweep_get_subscription_cache(ring: Arc<Self>, interval_duration: Duration) {
        // Add random initial delay to prevent synchronized sweeps across peers
        let initial_delay = Duration::from_secs(GlobalRng::random_range(10u64..=30u64));
        tokio::time::sleep(initial_delay).await;

        let mut interval = tokio::time::interval(interval_duration);
        interval.tick().await; // Skip first immediate tick

        loop {
            interval.tick().await;

            // Sweep expired entries from GET subscription cache
            let expired = ring.sweep_expired_get_subscriptions();

            if expired.is_empty() {
                continue;
            }

            tracing::debug!(
                expired_count = expired.len(),
                "GET subscription cache sweep found expired entries"
            );

            // Clean up local subscription state for each expired contract
            for key in expired {
                // Skip if there's still a client subscription - don't unsubscribe.
                // We don't re-add to GET cache because the client subscription
                // already protects this contract from cleanup.
                if ring.hosting_manager.has_client_subscriptions(key.id()) {
                    tracing::debug!(
                        %key,
                        "Skipping cleanup for expired GET subscription - has client subscription"
                    );
                    continue;
                }

                // In the lease-based model, just unsubscribe from the contract.
                // The lease will naturally expire if not renewed.
                ring.unsubscribe(&key);

                tracing::info!(
                    %key,
                    "Cleaned up expired GET subscription from local state"
                );
            }
        }
    }

    /// Periodically register topology snapshots for simulation testing.
    ///
    /// This task only runs when `CURRENT_NETWORK_NAME` is set (i.e., during SimNetwork tests).
    /// It allows SimNetwork to validate subscription topology by querying the global registry.
    #[cfg(any(test, feature = "testing"))]
    async fn register_topology_snapshots_periodically(
        ring: Arc<Self>,
        interval_duration: Duration,
    ) {
        use topology_registry::{get_current_network_name, register_topology_snapshot};

        tracing::info!("Topology snapshot registration task started");

        // Add small initial delay to let network stabilize (use short delay in tests)
        tokio::time::sleep(Duration::from_millis(100)).await;

        let mut interval = tokio::time::interval(interval_duration);
        interval.tick().await; // Skip first immediate tick

        loop {
            interval.tick().await;

            // Only register if we're in a simulation context
            let Some(network_name) = get_current_network_name() else {
                tracing::debug!("Topology snapshot: no network name set, skipping");
                continue;
            };

            let Some(peer_addr) = ring.connection_manager.get_own_addr() else {
                tracing::debug!("Topology snapshot: no peer address yet, skipping");
                continue;
            };

            // Use get_stored_location() for consistency with set_upstream distance check.
            // This ensures topology validation uses the same location as the tie-breaker.
            let location = ring
                .connection_manager
                .get_stored_location()
                .map(|l| l.as_f64())
                .unwrap_or(0.0);

            let snapshot = ring
                .hosting_manager
                .generate_topology_snapshot(peer_addr, location);
            let contract_count = snapshot.contracts.len();
            register_topology_snapshot(&network_name, snapshot);

            tracing::info!(
                %peer_addr,
                location,
                network = %network_name,
                contract_count,
                "Registered topology snapshot"
            );
        }
    }

    /// Record a PUT access to a contract in the hosting cache.
    /// Alias for host_contract with Put access type.
    pub fn seed_contract(&self, key: ContractKey, size_bytes: u64) -> Vec<ContractKey> {
        self.host_contract(key, size_bytes, AccessType::Put)
    }

    /// Record an access to a contract in the hosting cache.
    ///
    /// This adds or refreshes the contract in the unified hosting cache.
    /// ALL contracts in the hosting cache get subscription renewal.
    pub fn host_contract(
        &self,
        key: ContractKey,
        size_bytes: u64,
        access_type: AccessType,
    ) -> Vec<ContractKey> {
        self.hosting_manager
            .record_contract_access(key, size_bytes, access_type)
    }

    /// Record a GET access to a contract in the hosting cache.
    pub fn record_get_access(&self, key: ContractKey, size_bytes: u64) -> Vec<ContractKey> {
        self.host_contract(key, size_bytes, AccessType::Get)
    }

    /// Whether this node is hosting this contract (has it in cache).
    #[inline]
    pub fn is_hosting_contract(&self, key: &ContractKey) -> bool {
        self.hosting_manager.is_hosting_contract(key)
    }

    /// Alias for backwards compatibility - use is_hosting_contract instead.
    #[inline]
    pub fn is_seeding_contract(&self, key: &ContractKey) -> bool {
        self.is_hosting_contract(key)
    }

    /// Set the storage reference for hosting metadata persistence.
    ///
    /// Must be called after executor creation. This enables automatic
    /// cleanup of persisted metadata when contracts are evicted.
    pub fn set_hosting_storage(&self, storage: crate::contract::storages::Storage) {
        self.hosting_manager.set_storage(storage);
    }

    /// Load hosting cache from persisted storage.
    ///
    /// Call this during startup after storage is available to restore
    /// the hosting cache from the previous run. Also migrates legacy contracts
    /// that have state but no hosting metadata.
    ///
    /// # Arguments
    /// * `storage` - The storage backend
    /// * `code_hash_lookup` - Function to look up CodeHash from ContractInstanceId.
    ///   Uses ContractStore which has the id->code_hash mapping.
    #[cfg(feature = "redb")]
    pub fn load_hosting_cache<F>(
        &self,
        storage: &crate::contract::storages::Storage,
        code_hash_lookup: F,
    ) -> Result<usize, redb::Error>
    where
        F: Fn(
            &freenet_stdlib::prelude::ContractInstanceId,
        ) -> Option<freenet_stdlib::prelude::CodeHash>,
    {
        self.hosting_manager
            .load_from_storage(storage, code_hash_lookup)
    }

    /// Load hosting cache from persisted storage (sqlite version).
    ///
    /// Also migrates legacy contracts that have state but no hosting metadata.
    #[cfg(all(feature = "sqlite", not(feature = "redb")))]
    pub async fn load_hosting_cache<F>(
        &self,
        storage: &crate::contract::storages::Storage,
        code_hash_lookup: F,
    ) -> Result<usize, crate::contract::storages::sqlite::SqlDbError>
    where
        F: Fn(
            &freenet_stdlib::prelude::ContractInstanceId,
        ) -> Option<freenet_stdlib::prelude::CodeHash>,
    {
        self.hosting_manager
            .load_from_storage(storage, code_hash_lookup)
            .await
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
        if let Some(event) = NetEventLog::connected(self, peer, loc) {
            self.event_register
                .register_events(Either::Left(event))
                .await;
        }
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

    /// Get k best peers for caching a contract, ranked by routing predictions.
    /// Accepts either &ContractKey or &ContractInstanceId (both implement From<&T> for Location).
    pub fn k_closest_potentially_caching<K>(
        &self,
        contract_id: &K,
        skip_list: impl Contains<std::net::SocketAddr> + Clone,
        k: usize,
    ) -> Vec<PeerKeyLocation>
    where
        for<'a> Location: From<&'a K>,
    {
        let router = self.router.read();
        let target_location = Location::from(contract_id);

        let mut seen = HashSet::new();
        let mut candidates: Vec<PeerKeyLocation> = Vec::new();

        let connections = self.connection_manager.get_connections_by_location();
        // Sort keys for deterministic iteration order (HashMap iteration is non-deterministic)
        // This ensures the `seen.insert()` check behaves consistently across runs
        let mut sorted_keys: Vec<_> = connections.keys().collect();
        sorted_keys.sort();
        for loc in sorted_keys {
            let conns = connections.get(loc).expect("key exists");
            // Sort connections for deterministic iteration order
            let mut sorted_conns: Vec<_> = conns.iter().collect();
            sorted_conns.sort_by_key(|c| c.location.clone());
            for conn in sorted_conns {
                if let Some(addr) = conn.location.socket_addr() {
                    if skip_list.has_element(addr) || !seen.insert(addr) {
                        continue;
                    }
                }
                candidates.push(conn.location.clone());
            }
        }

        // Sort candidates for deterministic input to select_k_best_peers
        candidates.sort();

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

    // ==================== Subscription Management (Lease-Based) ====================

    /// Subscribe to a contract with a lease.
    ///
    /// Creates a new subscription or renews an existing one. The subscription
    /// will expire after `SUBSCRIPTION_LEASE_DURATION` unless renewed.
    pub fn subscribe(&self, contract: ContractKey) -> SubscribeResult {
        self.hosting_manager.subscribe(contract)
    }

    /// Unsubscribe from a contract.
    ///
    /// Removes the active subscription. The contract may still be hosted
    /// (in the hosting cache) until evicted by LRU.
    pub fn unsubscribe(&self, contract: &ContractKey) {
        self.hosting_manager.unsubscribe(contract)
    }

    /// Check if we have an active (non-expired) subscription to a contract.
    pub fn is_subscribed(&self, contract: &ContractKey) -> bool {
        self.hosting_manager.is_subscribed(contract)
    }

    /// Get all contracts with active subscriptions.
    pub fn get_subscribed_contracts(&self) -> Vec<ContractKey> {
        self.hosting_manager.get_subscribed_contracts()
    }

    /// Expire stale subscriptions and return the contracts that were expired.
    ///
    /// Should be called periodically by a background task.
    pub fn expire_stale_subscriptions(&self) -> Vec<ContractKey> {
        self.hosting_manager.expire_stale_subscriptions()
    }

    /// Check if we should continue hosting a contract.
    ///
    /// Returns true if we have an active subscription, client subscriptions,
    /// or the contract is in our hosting cache.
    pub fn should_host(&self, contract: &ContractKey) -> bool {
        self.hosting_manager.should_host(contract)
    }

    /// Alias for backwards compatibility - use should_host instead.
    pub fn should_seed(&self, contract: &ContractKey) -> bool {
        self.should_host(contract)
    }

    /// Get contracts that need subscription renewal.
    ///
    /// Returns contracts where:
    /// - We have an active subscription that will expire soon, OR
    /// - We have client subscriptions but no active network subscription, OR
    /// - We have hosted contracts without active subscriptions (THE FIX)
    pub fn contracts_needing_renewal(&self) -> Vec<ContractKey> {
        self.hosting_manager.contracts_needing_renewal()
    }

    // ==================== Client Subscription Management ====================

    /// Register a client subscription for a contract (WebSocket client subscribed).
    ///
    /// Returns information about the operation for telemetry.
    pub fn add_client_subscription(
        &self,
        instance_id: &ContractInstanceId,
        client_id: crate::client_events::ClientId,
    ) -> AddClientSubscriptionResult {
        self.hosting_manager
            .add_client_subscription(instance_id, client_id)
    }

    /// Remove a client from all its subscriptions (used when client disconnects).
    ///
    /// Returns a [`ClientDisconnectResult`] with:
    /// - `affected_contracts`: all contracts where the client was subscribed (for cleanup)
    pub fn remove_client_from_all_subscriptions(
        &self,
        client_id: crate::client_events::ClientId,
    ) -> ClientDisconnectResult {
        self.hosting_manager
            .remove_client_from_all_subscriptions(client_id)
    }

    /// Get the number of contracts in the hosting cache.
    /// This is the actual count of contracts this node is caching/hosting.
    pub fn hosting_contracts_count(&self) -> usize {
        self.hosting_manager.hosting_contracts_count()
    }

    /// Alias for backwards compatibility - use hosting_contracts_count instead.
    pub fn seeding_contracts_count(&self) -> usize {
        self.hosting_contracts_count()
    }

    /// Get subscription state for all contracts (for telemetry).
    ///
    /// Returns: (contract, has_client_subscription, is_active_subscription, expires_at)
    pub fn get_subscription_states(&self) -> Vec<(ContractKey, bool, bool, Option<Instant>)> {
        self.hosting_manager.get_subscription_states()
    }

    // ==================== Subscription Retry Spam Prevention ====================

    /// Check if a subscription request can be made for a contract.
    /// Returns false if request is already pending or in backoff period.
    pub fn can_request_subscription(&self, contract: &ContractKey) -> bool {
        self.hosting_manager.can_request_subscription(contract)
    }

    /// Mark a subscription request as in-flight.
    /// Returns false if already pending.
    pub fn mark_subscription_pending(&self, contract: ContractKey) -> bool {
        self.hosting_manager.mark_subscription_pending(contract)
    }

    /// Mark a subscription request as completed.
    /// If success is false, applies exponential backoff.
    pub fn complete_subscription_request(&self, contract: &ContractKey, success: bool) {
        self.hosting_manager
            .complete_subscription_request(contract, success)
    }

    // ==================== Hosting Cache Management ====================

    /// Touch a contract in the hosting cache (refresh TTL without adding).
    ///
    /// Called when UPDATE is received for a hosted contract.
    pub fn touch_hosting(&self, key: &ContractKey) {
        self.hosting_manager.touch_hosting(key)
    }

    /// Sweep for expired entries in the hosting cache.
    ///
    /// Returns contracts evicted from this cache. Callers should check
    /// `has_client_subscriptions()` before removing subscription state.
    pub fn sweep_expired_hosting(&self) -> Vec<ContractKey> {
        self.hosting_manager.sweep_expired_hosting()
    }

    // ==================== Legacy GET Auto-Subscription (delegating to hosting cache) ====================
    /// Sweep for expired entries (delegated to hosting cache).
    ///
    /// Returns contracts evicted from the cache. Callers should check
    /// `has_client_subscriptions()` before removing subscription state.
    pub fn sweep_expired_get_subscriptions(&self) -> Vec<ContractKey> {
        // Delegate to hosting cache
        self.sweep_expired_hosting()
    }

    // ==================== Connection Pruning ====================

    /// Prune a peer connection.
    ///
    /// Returns orphaned transactions that need to be retried or failed.
    /// In the new lease-based subscription model, subscriptions are not tied to specific
    /// peers, so no subscription pruning is needed when a peer disconnects.
    pub async fn prune_connection(&self, peer: PeerId) -> PruneConnectionResult {
        use crate::tracing::DisconnectReason;

        tracing::debug!(%peer, "Removing connection");
        let orphaned_transactions = self.live_tx_tracker.prune_transactions_from_peer(peer.addr);

        if !orphaned_transactions.is_empty() {
            tracing::debug!(
                %peer,
                orphaned_count = orphaned_transactions.len(),
                "Connection pruned with orphaned transactions"
            );
        }

        // Capture connection duration before pruning
        let connection_duration_ms = self
            .connection_manager
            .get_connection_duration_ms(peer.addr);

        // This case would be when a connection is being open, so peer location hasn't been recorded yet
        let Some(_loc) = self.connection_manager.prune_alive_connection(peer.addr) else {
            return PruneConnectionResult {
                orphaned_transactions,
            };
        };

        if let Some(event) = NetEventLog::disconnected_with_context(
            self,
            &peer,
            DisconnectReason::Pruned,
            connection_duration_ms,
            None, // bytes_sent not tracked yet
            None, // bytes_received not tracked yet
        ) {
            self.event_register
                .register_events(Either::Left(event))
                .await;
        }

        PruneConnectionResult {
            orphaned_transactions,
        }
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
        let mut last_backoff_cleanup = Instant::now();
        const BACKOFF_CLEANUP_INTERVAL: Duration = Duration::from_secs(60);
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

            // Periodic cleanup of expired backoff entries
            if last_backoff_cleanup.elapsed() > BACKOFF_CLEANUP_INTERVAL {
                self.cleanup_connection_backoff();
                last_backoff_cleanup = Instant::now();
            }

            // Acquire new connections up to MAX_CONCURRENT_CONNECTIONS limit
            // Only count Connect transactions, not all operations (Get/Put/Subscribe/Update)
            let active_count = live_tx_tracker.active_connect_transaction_count();
            if let Some(ideal_location) = pending_conn_adds.pop_first() {
                // Check if this target is in backoff due to previous failures
                if self.is_in_connection_backoff(ideal_location) {
                    tracing::debug!(
                        target_location = %ideal_location,
                        "Skipping connection attempt - target in backoff"
                    );
                    // Intentionally do NOT re-queue here:
                    // - Avoids repeatedly popping and checking the same location while it is
                    //   under backoff, which would waste work in this tight maintenance loop.
                    // - The topology manager will re-request connections to this location
                    //   in the next cycle if we're still below min_connections.
                } else if active_count < MAX_CONCURRENT_CONNECTIONS {
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
                                ?error,
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
                        // Record failure for exponential backoff
                        self.record_connection_failure(
                            ideal_location,
                            ConnectionFailureReason::RoutingFailed,
                        );
                    } else {
                        tracing::info!(
                            active_connections = active_count + 1,
                            "Successfully initiated connection acquisition"
                        );
                        // Note: Backoff is only cleared when the connection actually completes
                        // successfully in ConnectOp::handle_msg when acceptance.satisfied is true.
                        // We don't clear it here at initiation because the connection could still
                        // timeout or be rejected before completing.
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
                tracing::debug!(
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

            crate::deterministic_select! {
              _ = refresh_density_map.tick() => {
                self.refresh_density_request_cache();
              },
              _ = check_interval.tick() => {},
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
            joiner.clone(),
            query_target.clone(),
            ideal_location,
            ttl,
            target_connections,
            op_manager.connect_forward_estimator.clone(),
        );

        // Emit telemetry for initial connect request sent
        if let Some(event) = NetEventLog::connect_request_sent(
            &tx,
            self,
            ideal_location,
            joiner,
            query_target.clone(),
            ttl,
            true, // is_initial
        ) {
            self.register_events(Either::Left(event)).await;
        }

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

    /// Register a topology snapshot for this peer with the global registry.
    ///
    /// This should be called periodically during simulation tests to enable
    /// topology validation. The snapshot captures the current subscription
    /// state for all contracts.
    #[cfg(any(test, feature = "testing"))]
    #[allow(dead_code)] // Used by SimNetwork tests
    pub fn register_topology_snapshot(&self, network_name: &str) {
        let Some(peer_addr) = self.connection_manager.get_own_addr() else {
            return;
        };
        // Use get_stored_location() for consistency with set_upstream distance check.
        let location = self
            .connection_manager
            .get_stored_location()
            .map(|l| l.as_f64())
            .unwrap_or(0.0);

        let snapshot = self
            .hosting_manager
            .generate_topology_snapshot(peer_addr, location);
        topology_registry::register_topology_snapshot(network_name, snapshot);
    }

    /// Get a topology snapshot for this peer without registering it.
    #[cfg(any(test, feature = "testing"))]
    #[allow(dead_code)] // Used by SimNetwork tests
    pub fn get_topology_snapshot(&self) -> Option<topology_registry::TopologySnapshot> {
        let peer_addr = self.connection_manager.get_own_addr()?;
        // Use get_stored_location() for consistency with set_upstream distance check.
        let location = self
            .connection_manager
            .get_stored_location()
            .map(|l| l.as_f64())
            .unwrap_or(0.0);

        Some(
            self.hosting_manager
                .generate_topology_snapshot(peer_addr, location),
        )
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
    NoCachingPeers(ContractInstanceId),
    #[error("Peer has not joined the network yet (no ring location established)")]
    PeerNotJoined,
}
