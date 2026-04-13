use super::*;
use super::{
    ContractExecutor, ContractRequest, ContractResponse, ExecutorError, InitCheckResult,
    OpRequestSender, RequestError, Response, SLOW_INIT_THRESHOLD, STALE_INIT_THRESHOLD,
    StateStoreError, now_nanos,
};

/// Maximum number of related contracts a single validation can request.
/// Bounds worst-case first-time cost: N GETs of up to 50MB each.
const MAX_RELATED_CONTRACTS_PER_REQUEST: usize = 10;

/// Timeout for fetching all related contracts during validation.
const RELATED_FETCH_TIMEOUT: Duration = Duration::from_secs(10);
use crate::node::OpManager;
use crate::wasm_runtime::{
    BackendEngine, DEFAULT_MODULE_CACHE_CAPACITY, MAX_STATE_SIZE, RuntimeConfig, SharedModuleCache,
};
use dashmap::DashMap;
use freenet_stdlib::prelude::{MessageOrigin, RelatedContract};
use lru::LruCache;
use std::collections::{HashMap, HashSet};
use std::num::NonZeroUsize;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use tokio::sync::Semaphore;

// Type alias for shared notification storage.
// Uses DashMap for fine-grained per-key locking: concurrent reads to different
// contracts proceed in parallel, and writes only block the affected shard.
type SharedNotifications =
    Arc<DashMap<ContractInstanceId, Vec<(ClientId, tokio::sync::mpsc::Sender<HostResult>)>>>;

// Type alias for shared subscriber summaries.
type SharedSummaries =
    Arc<DashMap<ContractInstanceId, HashMap<ClientId, Option<StateSummary<'static>>>>>;

// Tracks per-client subscription counts for O(1) limit enforcement.
type SharedClientCounts = Arc<DashMap<ClientId, usize>>;

/// Construct a subscriber limit error for a registration that was rejected.
///
/// Uses `Subscribe` variant with a synthetic `ContractKey` (zeroed `CodeHash`)
/// because the registration path only has a `ContractInstanceId`, not the full
/// `ContractKey`. The cause string carries the real rejection reason.
fn subscriber_limit_error(instance_id: ContractInstanceId, cause: &str) -> Box<RequestError> {
    let synthetic_key = ContractKey::from_id_and_code(
        instance_id,
        freenet_stdlib::prelude::CodeHash::new([0u8; 32]),
    );
    Box::new(RequestError::ContractError(StdContractError::Subscribe {
        key: synthetic_key,
        cause: cause.to_string().into(),
    }))
}

/// Send a subscription error to all clients registered for a contract.
/// Takes a snapshot of the channels to avoid holding any lock during sends.
fn send_subscription_error_to_clients(
    channels: &[(ClientId, tokio::sync::mpsc::Sender<HostResult>)],
    key: ContractInstanceId,
    reason: String,
) {
    let error: freenet_stdlib::client_api::ClientError =
        freenet_stdlib::client_api::ErrorKind::OperationError {
            cause: reason.into(),
        }
        .into();
    for (client_id, sender) in channels {
        match sender.try_send(Err(error.clone())) {
            Ok(()) => {}
            Err(mpsc::error::TrySendError::Full(_)) => {
                tracing::warn!(
                    client = %client_id,
                    contract = %key,
                    "Subscriber notification channel full — subscription error dropped"
                );
            }
            Err(mpsc::error::TrySendError::Closed(_)) => {
                tracing::debug!(
                    client = %client_id,
                    contract = %key,
                    "Failed to send subscription error notification (channel closed)"
                );
            }
        }
    }
}

// ============================================================================
// RuntimePool - Pool of executors for concurrent contract execution
// ============================================================================

/// Health status information for the RuntimePool.
#[derive(Debug, Clone)]
#[allow(dead_code)]
pub struct PoolHealthStatus {
    /// Number of executors currently available in the pool
    pub available: usize,
    /// Number of executors currently checked out (in use)
    pub checked_out: usize,
    /// Total pool size (should equal available + checked_out)
    pub total: usize,
    /// Number of executors that were replaced due to failures
    pub replacements: usize,
    /// Number of distinct contracts currently being processed.
    /// Observability-only: the sequential event loop means this is always 0 or 1.
    pub contracts_in_flight: usize,
}

/// A pool of executors that enables concurrent contract execution.
///
/// This pool manages multiple `Executor<Runtime>` instances, allowing multiple
/// contract operations to run in parallel. The pool uses a semaphore to control
/// access to executors and ensures thread-safe executor borrowing and returning.
///
/// # Architecture
///
/// The pool maintains a fixed number of executors (typically CPU count) and uses
/// a semaphore to gate access. When an operation needs an executor:
/// 1. It acquires a semaphore permit (blocking if all executors are busy)
/// 2. Takes an available executor from the pool
/// 3. Executes the operation
/// 4. Returns the executor to the pool and releases the permit
///
/// This design ensures:
/// - Bounded parallelism (no unbounded thread spawning)
/// - Fair access to executors
/// - Proper cleanup on errors
///
/// # Pool Health Tracking
///
/// The pool tracks actual executor count separately from the semaphore to detect
/// capacity degradation. If executors are lost due to panics or other issues,
/// the pool can detect and report this mismatch.
pub struct RuntimePool {
    /// Pool of available executors. `None` slots indicate executors currently in use.
    runtimes: Vec<Option<Executor<Runtime>>>,
    /// Semaphore controlling access to executors (permits = available executors)
    available: Semaphore,
    /// Configuration for creating new executors
    config: Arc<Config>,
    /// Channel to send operation requests to the event loop (cloneable, shared by all executors)
    op_sender: OpRequestSender,
    /// Reference to the operation manager (cloneable, shared by all executors)
    op_manager: Arc<OpManager>,
    /// Total pool size (for health checking)
    pool_size: usize,
    /// Count of executors currently checked out (for health checking)
    checked_out: AtomicUsize,
    /// Count of executors that were lost/replaced (for monitoring)
    replacements_count: AtomicUsize,
    /// Shared StateStore used by all executors (ReDb uses exclusive file locking)
    shared_state_store: StateStore<Storage>,
    /// Shared notification channels for all subscribed clients.
    /// Stored at pool level to avoid race condition where subscriptions registered
    /// while an executor is checked out would be missed by that executor.
    shared_notifications: SharedNotifications,
    /// Shared subscriber summaries for computing deltas.
    shared_summaries: SharedSummaries,
    /// Per-client subscription count for O(1) limit enforcement.
    shared_client_counts: SharedClientCounts,
    /// Shared compiled contract module cache (avoids 16x duplication across pool executors).
    shared_contract_modules: SharedModuleCache<ContractKey>,
    /// Shared compiled delegate module cache.
    shared_delegate_modules: SharedModuleCache<DelegateKey>,
    /// Shared backend engine used by all executors.
    ///
    /// All executors MUST share the same backend engine because compiled modules
    /// store references to the compiling Engine's internal data structures. Using
    /// a Module compiled by one Engine
    /// in a Store backed by a different Engine causes SIGSEGV.
    shared_backend_engine: BackendEngine,
    /// Shared recovery guard for corrupted-state self-healing across all pool executors.
    shared_recovery_guard: super::CorruptedStateRecoveryGuard,
    /// Sender for delegate notifications (cloned into each executor and replacements).
    delegate_notification_tx: super::DelegateNotificationSender,
    /// Receiver for delegate notifications (taken once by `contract_handling()`).
    delegate_notification_rx: Option<super::DelegateNotificationReceiver>,
    /// Per-contract executor usage tracking (for observability).
    /// Not used for enforcement since the event loop is sequential.
    /// Tracked via RAII `InFlightGuard` in: `fetch_contract`, `upsert_contract_state`,
    /// `summarize_contract_state`, `get_contract_state_delta`.
    /// Intentionally NOT tracked in: `execute_delegate_request` (no contract key),
    /// `register_contract_notifier` (synchronous, no executor checkout),
    /// `lookup_key`, `get_subscription_info`, `notify_subscription_error`,
    /// `remove_client` (read-only / no executor checkout).
    in_flight_contracts: HashMap<ContractKey, usize>,
}

impl RuntimePool {
    /// Create a new pool with the specified number of executors.
    ///
    /// # Arguments
    /// * `config` - Configuration for executors
    /// * `op_sender` - Channel to send operation requests to the event loop (cloneable)
    /// * `op_manager` - Reference to the operation manager
    /// * `pool_size` - Number of executors to create (typically CPU count)
    pub async fn new(
        config: Arc<Config>,
        op_sender: OpRequestSender,
        op_manager: Arc<OpManager>,
        pool_size: NonZeroUsize,
    ) -> anyhow::Result<Self> {
        let pool_size_usize: usize = pool_size.into();
        let mut runtimes = Vec::with_capacity(pool_size_usize);

        let (_, _, _, shared_state_store) = Executor::<Runtime>::get_stores(&config).await?;

        // Create shared notification storage BEFORE creating executors
        // so we can pass references to each executor
        let shared_notifications: SharedNotifications = Arc::new(DashMap::new());
        let shared_summaries: SharedSummaries = Arc::new(DashMap::new());
        let shared_client_counts: SharedClientCounts = Arc::new(DashMap::new());

        // Create delegate notification channel for subscription delivery
        let (delegate_notification_tx, delegate_notification_rx) =
            tokio::sync::mpsc::channel(super::DELEGATE_NOTIFICATION_CHANNEL_SIZE);

        // Create shared module caches so all pool executors share one set of compiled WASM modules.
        // Without this, each of the N executors would maintain its own LRU cache, causing
        // the same contracts to be compiled and stored N times (e.g., 16 executors × 92 contracts
        // × ~500KB-1MB = ~1.2 GB of duplicate compiled modules on the nova gateway).
        let cache_capacity =
            NonZeroUsize::new(DEFAULT_MODULE_CACHE_CAPACITY).unwrap_or(NonZeroUsize::MIN);
        let shared_contract_modules: SharedModuleCache<ContractKey> =
            Arc::new(Mutex::new(LruCache::new(cache_capacity)));
        let shared_delegate_modules: SharedModuleCache<DelegateKey> =
            Arc::new(Mutex::new(LruCache::new(cache_capacity)));

        // Create shared recovery guard for corrupted-state self-healing.
        // All pool executors share this so recovery tracking is consistent.
        let shared_recovery_guard: super::CorruptedStateRecoveryGuard =
            Arc::new(std::sync::Mutex::new(HashSet::new()));

        // Create the first executor to obtain a backend engine, then share it
        // with all subsequent executors. All executors MUST share the same backend
        // engine because compiled modules store references tied to the compiling
        // Engine's internal data structures.
        let mut first_executor = Executor::from_config_with_shared_modules(
            config.clone(),
            shared_state_store.clone(),
            Some(op_sender.clone()),
            Some(op_manager.clone()),
            shared_contract_modules.clone(),
            shared_delegate_modules.clone(),
            None, // No shared backend yet — this executor creates the engine
        )
        .await?;
        let shared_backend_engine = first_executor.runtime.clone_backend_engine();
        first_executor.set_shared_notifications(
            shared_notifications.clone(),
            shared_summaries.clone(),
            shared_client_counts.clone(),
        );
        first_executor.set_recovery_guard(shared_recovery_guard.clone());
        first_executor.set_delegate_notification_tx(delegate_notification_tx.clone());
        runtimes.push(Some(first_executor));

        for i in 1..pool_size_usize {
            let mut executor = Executor::from_config_with_shared_modules(
                config.clone(),
                shared_state_store.clone(),
                Some(op_sender.clone()),
                Some(op_manager.clone()),
                shared_contract_modules.clone(),
                shared_delegate_modules.clone(),
                Some(shared_backend_engine.clone()),
            )
            .await?;

            // Set shared notification storage so this executor uses pool-level storage
            executor.set_shared_notifications(
                shared_notifications.clone(),
                shared_summaries.clone(),
                shared_client_counts.clone(),
            );
            executor.set_recovery_guard(shared_recovery_guard.clone());
            executor.set_delegate_notification_tx(delegate_notification_tx.clone());

            runtimes.push(Some(executor));

            // Yield to prevent async starvation during CPU-intensive WASM engine creation
            if i < pool_size_usize - 1 {
                tokio::task::yield_now().await;
            }
        }

        tracing::info!(pool_size = pool_size_usize, "RuntimePool created");

        Ok(Self {
            runtimes,
            available: Semaphore::new(pool_size_usize),
            config,
            op_sender,
            op_manager,
            pool_size: pool_size_usize,
            checked_out: AtomicUsize::new(0),
            replacements_count: AtomicUsize::new(0),
            shared_state_store,
            shared_notifications,
            shared_summaries,
            shared_client_counts,
            shared_contract_modules,
            shared_delegate_modules,
            shared_backend_engine,
            shared_recovery_guard,
            delegate_notification_tx,
            delegate_notification_rx: Some(delegate_notification_rx),
            in_flight_contracts: HashMap::new(),
        })
    }

    /// Get the current health status of the pool.
    ///
    /// Returns a tuple of:
    /// - `available`: Number of executors currently available
    /// - `checked_out`: Number of executors currently in use
    /// - `total`: Total pool size
    /// - `replacements`: Number of executors that were replaced due to failures
    pub fn health_status(&self) -> PoolHealthStatus {
        let available = self.runtimes.iter().filter(|s| s.is_some()).count();
        let checked_out = self.checked_out.load(Ordering::SeqCst);
        let replacements = self.replacements_count.load(Ordering::SeqCst);

        // Check for pool degradation
        let expected_total = available + checked_out;
        if expected_total != self.pool_size {
            tracing::warn!(
                available = available,
                checked_out = checked_out,
                expected = self.pool_size,
                actual = expected_total,
                "Pool capacity mismatch detected - possible semaphore drift"
            );
        }

        PoolHealthStatus {
            available,
            checked_out,
            total: self.pool_size,
            replacements,
            contracts_in_flight: self.in_flight_contracts.len(),
        }
    }

    /// Log the pool health status at debug level.
    /// Called periodically when there have been replacements to help diagnose issues.
    fn log_health_if_degraded(&self) {
        let status = self.health_status();
        if status.replacements > 0 {
            tracing::info!(
                available = status.available,
                checked_out = status.checked_out,
                total = status.total,
                replacements = status.replacements,
                "RuntimePool health status (degraded - {} replacements)",
                status.replacements
            );
        }
    }

    /// Record that an executor has been checked out for the given contract.
    /// Observability-only — the sequential event loop means at most one contract
    /// is ever in flight at a time.
    ///
    /// Panic safety: WASM panics are caught at the wasmer FFI boundary and
    /// converted to errors (not Rust panics), so `track_contract_return` is
    /// guaranteed to run after `track_contract_checkout` in the methods below.
    /// There are no `?` operators between the checkout/return calls.
    fn track_contract_checkout(&mut self, key: &ContractKey) {
        *self.in_flight_contracts.entry(*key).or_insert(0) += 1;
    }

    /// Record that an executor has been returned after processing the given contract.
    fn track_contract_return(&mut self, key: &ContractKey) {
        if let std::collections::hash_map::Entry::Occupied(mut e) =
            self.in_flight_contracts.entry(*key)
        {
            *e.get_mut() = e.get().saturating_sub(1);
            if *e.get() == 0 {
                e.remove();
            }
        }
    }

    /// Pop an executor from the pool, blocking until one is available.
    ///
    /// The caller MUST return the executor via `return_executor` after use.
    async fn pop_executor(&mut self) -> Executor<Runtime> {
        // Wait for an available permit
        let permit = self
            .available
            .acquire()
            .await
            .expect("Semaphore should not be closed");

        // Consume the permit without returning it to the semaphore.
        // The permit will be restored in `return_executor` via `add_permits(1)`.
        permit.forget();

        // Track that we're checking out an executor
        self.checked_out.fetch_add(1, Ordering::SeqCst);

        // Find the first available executor
        for slot in &mut self.runtimes {
            if let Some(executor) = slot.take() {
                return executor;
            }
        }

        // This should never happen because of the semaphore
        // But if it does, we need to restore the checked_out count
        self.checked_out.fetch_sub(1, Ordering::SeqCst);
        unreachable!("No executors available despite semaphore permit")
    }

    /// Return an executor to the pool after use.
    fn return_executor(&mut self, executor: Executor<Runtime>) {
        // Track that we're returning an executor
        self.checked_out.fetch_sub(1, Ordering::SeqCst);

        // Find an empty slot
        if let Some(empty_slot) = self.runtimes.iter_mut().find(|slot| slot.is_none()) {
            *empty_slot = Some(executor);
            self.available.add_permits(1);
        } else {
            // This should never happen, but log it if it does
            tracing::error!(
                pool_size = self.pool_size,
                checked_out = self.checked_out.load(Ordering::SeqCst),
                "No empty slot found when returning executor - pool may be corrupted"
            );
            // Still add the permit back to avoid deadlock
            self.available.add_permits(1);
        }
    }

    /// Check if an executor is healthy and can be reused.
    /// An executor is unhealthy if the WASM engine is in a broken state (e.g., store lost due to panic).
    fn is_executor_healthy(executor: &Executor<Runtime>) -> bool {
        executor.runtime.is_healthy()
    }

    /// Create a new executor to replace a broken one.
    /// Uses the shared StateStore to avoid opening a new database connection.
    async fn create_replacement_executor(&self) -> anyhow::Result<Executor<Runtime>> {
        tracing::warn!("Creating replacement executor due to previous failure");
        let mut executor = Executor::from_config_with_shared_modules(
            self.config.clone(),
            self.shared_state_store.clone(),
            Some(self.op_sender.clone()),
            Some(self.op_manager.clone()),
            self.shared_contract_modules.clone(),
            self.shared_delegate_modules.clone(),
            Some(self.shared_backend_engine.clone()),
        )
        .await?;

        // Set shared notification storage so the replacement executor uses pool-level storage
        executor.set_shared_notifications(
            self.shared_notifications.clone(),
            self.shared_summaries.clone(),
            self.shared_client_counts.clone(),
        );
        executor.set_recovery_guard(self.shared_recovery_guard.clone());
        executor.set_delegate_notification_tx(self.delegate_notification_tx.clone());

        Ok(executor)
    }

    /// Return an executor to the pool, replacing it if unhealthy.
    ///
    /// This is the single point for post-operation health checking. After any
    /// pool operation (contract or delegate), the executor's WASM store is
    /// checked. If the store was lost (e.g., due to a panic during WASM
    /// execution), a fresh executor is created and returned to the pool instead.
    async fn return_checked(&mut self, executor: Executor<Runtime>, operation: &str) {
        if Self::is_executor_healthy(&executor) {
            self.return_executor(executor);
            return;
        }

        let replacement_num = self.replacements_count.fetch_add(1, Ordering::SeqCst) + 1;
        tracing::warn!(
            operation,
            replacement_number = replacement_num,
            "Executor became unhealthy, creating replacement"
        );
        match self.create_replacement_executor().await {
            Ok(new_executor) => {
                self.return_executor(new_executor);
            }
            Err(e) => {
                tracing::error!(error = %e, "Failed to create replacement executor");
                // Return the broken executor anyway — next operation will fail
                // but at least the pool isn't depleted
                self.return_executor(executor);
            }
        }
        self.log_health_if_degraded();
    }

    /// Get a reference to the shared state store.
    /// Used for hosting metadata persistence operations during startup.
    pub fn state_store(&self) -> &StateStore<Storage> {
        &self.shared_state_store
    }

    /// Look up a code hash from an instance ID.
    /// Used for legacy contract migration during startup.
    pub fn code_hash_from_id(&self, instance_id: &ContractInstanceId) -> Option<CodeHash> {
        // Try to find the code hash in any available executor
        self.runtimes.iter().flatten().find_map(|executor| {
            executor
                .runtime
                .contract_store
                .code_hash_from_id(instance_id)
        })
    }
}

impl ContractExecutor for RuntimePool {
    fn lookup_key(&self, instance_id: &ContractInstanceId) -> Option<ContractKey> {
        // Try to find the key in any available executor
        self.runtimes.iter().flatten().find_map(|executor| {
            executor
                .runtime
                .contract_store
                .code_hash_from_id(instance_id)
                .map(|key| ContractKey::from_id_and_code(*instance_id, key))
        })
    }

    async fn fetch_contract(
        &mut self,
        key: ContractKey,
        return_contract_code: bool,
    ) -> Result<(Option<WrappedState>, Option<ContractContainer>), ExecutorError> {
        self.track_contract_checkout(&key);
        let mut executor = self.pop_executor().await;
        let result = executor.fetch_contract(key, return_contract_code).await;
        self.return_checked(executor, "fetch_contract").await;
        self.track_contract_return(&key);
        result
    }

    async fn upsert_contract_state(
        &mut self,
        key: ContractKey,
        update: Either<WrappedState, StateDelta<'static>>,
        related_contracts: RelatedContracts<'static>,
        code: Option<ContractContainer>,
    ) -> Result<UpsertResult, ExecutorError> {
        self.track_contract_checkout(&key);
        let mut executor = self.pop_executor().await;
        let result = executor
            .upsert_contract_state(key, update, related_contracts, code)
            .await;
        self.return_checked(executor, "upsert_contract_state").await;
        self.track_contract_return(&key);
        result
    }

    fn register_contract_notifier(
        &mut self,
        instance_id: ContractInstanceId,
        cli_id: ClientId,
        notification_ch: tokio::sync::mpsc::Sender<HostResult>,
        summary: Option<StateSummary<'_>>,
    ) -> Result<(), Box<RequestError>> {
        // Register in shared storage at pool level.
        // This ensures notifications work regardless of which executor is checked out
        // when the subscription is registered or when updates arrive.
        let owned_summary = summary.map(StateSummary::into_owned);

        // Check if this client is already registered for this contract
        let already_registered = self
            .shared_notifications
            .get(&instance_id)
            .and_then(|channels| {
                channels
                    .binary_search_by_key(&&cli_id, |(p, _)| p)
                    .ok()
                    .map(|i| (i, channels[i].1.same_channel(&notification_ch)))
            });

        if let Some((idx, same_channel)) = already_registered {
            if !same_channel {
                // Client reconnected with new channel, update it.
                if let Some(mut channels) = self.shared_notifications.get_mut(&instance_id) {
                    channels[idx] = (cli_id, notification_ch);
                }
                tracing::debug!(
                    client = %cli_id,
                    contract = %instance_id,
                    "Updated notification channel for existing subscription"
                );
            }
        } else {
            // New subscriber: enforce per-contract limit
            let contract_sub_count = self
                .shared_notifications
                .get(&instance_id)
                .map_or(0, |ch| ch.len());
            if contract_sub_count >= super::MAX_SUBSCRIBERS_PER_CONTRACT {
                tracing::warn!(
                    client = %cli_id,
                    contract = %instance_id,
                    limit = super::MAX_SUBSCRIBERS_PER_CONTRACT,
                    "Subscriber limit reached for contract, rejecting registration"
                );
                return Err(subscriber_limit_error(
                    instance_id,
                    &format!(
                        "subscriber limit ({}) reached for contract",
                        super::MAX_SUBSCRIBERS_PER_CONTRACT
                    ),
                ));
            }

            // Enforce per-client subscription limit using O(1) counter
            let client_sub_count = self.shared_client_counts.get(&cli_id).map_or(0, |c| *c);
            if client_sub_count >= super::MAX_SUBSCRIPTIONS_PER_CLIENT {
                tracing::warn!(
                    client = %cli_id,
                    contract = %instance_id,
                    limit = super::MAX_SUBSCRIPTIONS_PER_CLIENT,
                    current = client_sub_count,
                    "Per-client subscription limit reached, rejecting registration"
                );
                return Err(subscriber_limit_error(
                    instance_id,
                    &format!(
                        "per-client subscription limit ({}) reached",
                        super::MAX_SUBSCRIPTIONS_PER_CLIENT
                    ),
                ));
            }

            // Insert in sorted order for efficient lookup
            let mut channels = self.shared_notifications.entry(instance_id).or_default();
            let insert_pos = channels.partition_point(|(id, _)| id < &cli_id);
            channels.insert(insert_pos, (cli_id, notification_ch));
            let total = channels.len();
            drop(channels); // Release DashMap ref before accessing another entry

            *self.shared_client_counts.entry(cli_id).or_insert(0) += 1;
            tracing::debug!(
                client = %cli_id,
                contract = %instance_id,
                total_subscribers = total,
                "Registered new subscription in shared pool storage"
            );
        }

        // Also register the summary
        self.shared_summaries
            .entry(instance_id)
            .or_default()
            .insert(cli_id, owned_summary);

        Ok(())
    }

    async fn execute_delegate_request(
        &mut self,
        req: DelegateRequest<'_>,
        origin_contract: Option<&ContractInstanceId>,
        caller_delegate: Option<&DelegateKey>,
    ) -> Response {
        let mut executor = self.pop_executor().await;
        let result = executor.delegate_request(req, origin_contract, caller_delegate);
        self.return_checked(executor, "execute_delegate_request")
            .await;
        result
    }

    fn get_subscription_info(&self) -> Vec<crate::message::SubscriptionInfo> {
        // Read subscription info from shared storage at pool level
        self.shared_notifications
            .iter()
            .flat_map(|entry| {
                let instance_id = *entry.key();
                entry
                    .value()
                    .iter()
                    .map(move |(client_id, _)| crate::message::SubscriptionInfo {
                        instance_id,
                        client_id: *client_id,
                        last_update: None, // Pool doesn't track last update time
                    })
                    .collect::<Vec<_>>()
            })
            .collect()
    }

    fn notify_subscription_error(&self, key: ContractInstanceId, reason: String) {
        let channels = self
            .shared_notifications
            .get(&key)
            .map(|e| e.value().clone());
        if let Some(channels) = channels {
            send_subscription_error_to_clients(&channels, key, reason);
        }
    }

    /// Remove all subscriptions for a disconnected client.
    ///
    /// Cleans up both notification channels and stored summaries across all contracts.
    /// Without this, disconnected clients leak entries in shared_summaries and
    /// shared_notifications indefinitely.
    fn remove_client(&self, client_id: ClientId) {
        let mut removed_notifications = 0usize;
        let mut removed_summaries = 0usize;

        // Clean shared_notifications (DashMap::retain gives &mut V per entry)
        self.shared_notifications.retain(|_contract, channels| {
            if let Ok(i) = channels.binary_search_by_key(&&client_id, |(id, _)| id) {
                channels.remove(i);
                removed_notifications += 1;
            }
            !channels.is_empty()
        });

        // Update per-client subscription counter
        if removed_notifications > 0 {
            self.shared_client_counts.remove(&client_id);
        }

        // Clean shared_summaries
        self.shared_summaries.retain(|_contract, client_summaries| {
            if client_summaries.remove(&client_id).is_some() {
                removed_summaries += 1;
            }
            !client_summaries.is_empty()
        });

        if removed_notifications > 0 || removed_summaries > 0 {
            tracing::info!(
                client = %client_id,
                removed_notifications,
                removed_summaries,
                "Cleaned up subscriptions for disconnected client"
            );
        }
    }

    async fn summarize_contract_state(
        &mut self,
        key: ContractKey,
    ) -> Result<StateSummary<'static>, ExecutorError> {
        self.track_contract_checkout(&key);
        let mut executor = self.pop_executor().await;
        let result = executor.summarize_contract_state(key).await;
        self.return_checked(executor, "summarize_contract_state")
            .await;
        self.track_contract_return(&key);
        result
    }

    async fn get_contract_state_delta(
        &mut self,
        key: ContractKey,
        their_summary: StateSummary<'static>,
    ) -> Result<StateDelta<'static>, ExecutorError> {
        self.track_contract_checkout(&key);
        let mut executor = self.pop_executor().await;
        let result = executor.get_contract_state_delta(key, their_summary).await;
        self.return_checked(executor, "get_contract_state_delta")
            .await;
        self.track_contract_return(&key);
        result
    }

    fn take_delegate_notification_rx(&mut self) -> Option<super::DelegateNotificationReceiver> {
        self.delegate_notification_rx.take()
    }
}

// ============================================================================
// Single Executor Implementation
// ============================================================================

/// Result of computing a state update without committing.
/// Used in network mode to separate state computation from commit,
/// allowing the network operation to handle the commit and properly
/// detect changes for broadcasting.
enum ComputedStateUpdate {
    /// State changed - contains the new state to commit
    Changed(WrappedState),
    /// No change detected - contains the current state
    NoChange(WrappedState),
    /// Missing related contracts - contains the list of required contracts
    MissingRelated(Vec<RelatedContract>),
}

// ============================================================================
// Bridged methods - shared production logic for Runtime and MockWasmRuntime
// ============================================================================

#[allow(private_bounds)]
impl<R, S> Executor<R, S>
where
    R: crate::wasm_runtime::ContractRuntimeBridge,
    S: crate::wasm_runtime::StateStorage + Send + Sync + 'static,
    <S as crate::wasm_runtime::StateStorage>::Error: Into<anyhow::Error>,
{
    pub(super) fn bridged_lookup_key(
        &self,
        instance_id: &ContractInstanceId,
    ) -> Option<ContractKey> {
        let code_hash = self.runtime.code_hash_from_id(instance_id)?;
        Some(ContractKey::from_id_and_code(*instance_id, code_hash))
    }

    pub(super) async fn bridged_fetch_contract(
        &mut self,
        key: ContractKey,
        return_contract_code: bool,
    ) -> Result<(Option<WrappedState>, Option<ContractContainer>), ExecutorError> {
        tracing::debug!(
            contract = %key,
            return_code = return_contract_code,
            "fetching contract"
        );
        let result = self.perform_contract_get(return_contract_code, key).await;
        if let Ok((Some(ref state), ref code)) = result {
            let hash = blake3::hash(state.as_ref());
            tracing::debug!(
                contract = %key,
                state_size = state.as_ref().len(),
                state_hash = %hash,
                has_code = code.is_some(),
                "fetched contract state"
            );
        }
        match result {
            Ok((state, code)) => Ok((state, code)),
            Err(err) => Err(err),
        }
    }

    #[allow(clippy::too_many_lines)]
    pub(super) async fn bridged_upsert_contract_state(
        &mut self,
        key: ContractKey,
        update: Either<WrappedState, StateDelta<'static>>,
        related_contracts: RelatedContracts<'static>,
        code: Option<ContractContainer>,
    ) -> Result<UpsertResult, ExecutorError> {
        // CRITICAL: When a ContractContainer is provided, use its key instead of the passed-in key.
        let key = if let Some(ref container) = code {
            let container_key = container.key();
            if key.id() != container_key.id() {
                tracing::error!(
                    passed_key = %key,
                    container_key = %container_key,
                    "CRITICAL: Contract key instance ID mismatch - passed key doesn't match container"
                );
                return Err(ExecutorError::other(anyhow::anyhow!(
                    "contract key instance ID mismatch"
                )));
            }
            container_key
        } else {
            key
        };

        // Opportunistically clean up any stale initializations to prevent resource leaks
        let now = now_nanos();
        let stale = self
            .init_tracker
            .cleanup_stale_initializations(STALE_INIT_THRESHOLD, now);
        for info in stale {
            tracing::warn!(
                contract = %info.key,
                age_secs = info.age.as_secs(),
                dropped_ops = info.dropped_ops,
                "Cleaned up stale contract initialization (possible bug or timeout)"
            );
        }

        // Check if this contract is currently being initialized
        match self.init_tracker.check_and_maybe_queue(
            &key,
            code.is_some(),
            update.clone(),
            related_contracts.clone(),
            now,
        ) {
            InitCheckResult::NotInitializing => {
                // Continue with normal processing below
            }
            InitCheckResult::PutDuringInit => {
                return Err(ExecutorError::request(StdContractError::Put {
                    key,
                    cause: "contract is already being initialized".into(),
                }));
            }
            InitCheckResult::QueueFull => {
                tracing::warn!(
                    contract = %key,
                    limit = MAX_QUEUED_OPS_PER_CONTRACT,
                    "Contract initialization queue full, rejecting operation"
                );
                return Err(ExecutorError::request(StdContractError::Update {
                    key,
                    cause: "contract initialization queue is full, try again later".into(),
                }));
            }
            InitCheckResult::Queued { queue_size } => {
                tracing::info!(
                    contract = %key,
                    queue_size,
                    "Operation queued during contract initialization"
                );
                return Ok(UpsertResult::NoChange);
            }
        }
        if let Either::Left(ref state) = update {
            let hash = blake3::hash(state.as_ref());
            tracing::debug!(
                contract = %key,
                state_size = state.as_ref().len(),
                state_hash = %hash,
                phase = "upsert_start",
                "Upserting contract state"
            );
        }
        let params = if let Some(code) = &code {
            let p = code.params();
            // Ensure params are persisted to state_store so they survive restarts.
            // The code path (PUT via GET) always provides params in the container,
            // but state_store.store() is only called for new contracts. If the contract
            // already exists (merge path), commit_state_update() calls state_store.update()
            // which doesn't write params. Persisting here covers all cases.
            if let Err(e) = self.state_store.ensure_params(key, p.clone()).await {
                tracing::warn!(
                    contract = %key,
                    error = %e,
                    "Failed to persist contract parameters to state_store"
                );
            }
            p
        } else {
            self.state_store
                .get_params(&key)
                .await
                .map_err(ExecutorError::other)?
                .ok_or_else(|| {
                    tracing::warn!(
                        contract = %key,
                        is_delta = matches!(update, Either::Right(_)),
                        "Contract parameters not found in state_store"
                    );
                    ExecutorError::request(StdContractError::Put {
                        key,
                        cause: "missing contract parameters".into(),
                    })
                })?
        };

        // Track if we stored a new contract
        let (remove_if_fail, contract_was_provided) =
            if self.runtime.fetch_contract_code(&key, &params).is_none() {
                if let Some(ref contract_code) = code {
                    tracing::debug!(
                        contract = %key,
                        phase = "store_contract",
                        "Storing new contract"
                    );

                    self.runtime
                        .store_contract(contract_code.clone())
                        .map_err(ExecutorError::other)?;
                    (true, true)
                } else {
                    // Bug #2306: This should never happen for PUT operations because they
                    // always provide the contract code. If we hit this path during a PUT,
                    // it indicates the contract code was lost somewhere in the flow.
                    tracing::error!(
                        contract = %key,
                        key_code_hash = ?key.code_hash(),
                        code_provided = code.is_some(),
                        is_delta = matches!(update, Either::Right(_)),
                        phase = "missing_contract_error",
                        "Contract not in store and no code provided"
                    );
                    return Err(ExecutorError::request(StdContractError::MissingContract {
                        key: key.into(),
                    }));
                }
            } else {
                // Contract already in store. ensure_key_indexed handles the case of contracts
                // that reuse the same WASM code with different parameters (e.g., different
                // River rooms). Without this, lookup_key() fails for the new instance_id.
                // See issue #2380.
                //
                // We only index when code was provided in this request (code.is_some()).
                // When code is None, this is a state-only update to an existing contract
                // that should already be indexed.
                if code.is_some() {
                    self.runtime
                        .ensure_key_indexed(&key)
                        .map_err(ExecutorError::other)?;
                }
                (false, code.is_some())
            };

        let is_new_contract = self.state_store.get(&key).await.is_err();

        // Save the incoming full state (if any) for potential corrupted-state recovery.
        // When the stored state is corrupted, WASM merge fails. If we have a validated
        // incoming full state, we can replace the corrupted state with it.
        let incoming_full_state = match &update {
            Either::Left(state) => Some(state.clone()),
            Either::Right(_) => None,
        };

        // If this is a new contract being stored, mark it as initializing
        if remove_if_fail && is_new_contract && contract_was_provided {
            tracing::debug!(
                contract = %key,
                "Starting contract initialization - queueing subsequent operations"
            );
            if let Err(e) = self.init_tracker.start_initialization(key, now_nanos()) {
                tracing::warn!(
                    contract = %key,
                    error = %e,
                    limit = MAX_CONCURRENT_INITIALIZATIONS,
                    "Too many concurrent initializations, rejecting PUT"
                );
                if let Err(re) = self.runtime.remove_contract(&key) {
                    tracing::warn!(
                        contract = %key,
                        error = %re,
                        "Failed to remove contract after init tracker rejection"
                    );
                }
                return Err(ExecutorError::request(StdContractError::Put {
                    key,
                    cause: "node is too busy: too many contracts initializing simultaneously, try again later".into(),
                }));
            }
        }

        let mut updates = match update {
            Either::Left(incoming_state) => {
                // Fast-reject oversized state before expensive WASM validation.
                // A malicious contract could return `Valid` for any state, so this check must
                // happen at the node level before any WASM execution.
                let incoming_size = incoming_state.as_ref().len();
                if incoming_size > MAX_STATE_SIZE {
                    tracing::warn!(
                        contract = %key,
                        size_bytes = incoming_size,
                        limit_bytes = MAX_STATE_SIZE,
                        "Rejecting oversized state before WASM validation"
                    );
                    if remove_if_fail {
                        if let Err(e) = self.runtime.remove_contract(&key) {
                            tracing::warn!(
                                contract = %key,
                                error = %e,
                                "failed to remove contract after size rejection"
                            );
                        }
                    }
                    return Err(ExecutorError::request(StdContractError::Put {
                        key,
                        cause: format!(
                            "state size {incoming_size} bytes exceeds maximum allowed {MAX_STATE_SIZE} bytes"
                        )
                        .into(),
                    }));
                }

                let result = self
                    .fetch_related_for_validation(
                        &key,
                        &params,
                        &incoming_state,
                        &related_contracts,
                    )
                    .await
                    .inspect_err(|_| {
                        if remove_if_fail {
                            if let Err(e) = self.runtime.remove_contract(&key) {
                                tracing::warn!(contract = %key, error = %e, "failed to remove contract after validation failure");
                            }
                        }
                        // Clean up init_tracker so queued operations aren't left dangling
                        if let Some(dropped_count) = self.init_tracker.fail_initialization(&key) {
                            tracing::warn!(
                                contract = %key,
                                dropped_operations = dropped_count,
                                "Related contract validation failed, dropping queued operations"
                            );
                        }
                    })?;
                match result {
                    ValidateResult::Valid => {
                        tracing::debug!(
                            contract = %key,
                            phase = "validation_complete",
                            "Incoming state is valid"
                        );

                        if is_new_contract {
                            tracing::debug!(
                                contract = %key,
                                phase = "store_initial_state",
                                "Contract is new, storing initial state"
                            );
                            let state_to_store = incoming_state.clone();
                            self.state_store
                                .store(key, state_to_store, params.clone())
                                .await
                                .map_err(ExecutorError::other)?;

                            let completion_now = now_nanos();
                            if let Some(completion_info) = self
                                .init_tracker
                                .complete_initialization(&key, completion_now)
                            {
                                let init_duration = completion_info.init_duration;
                                if init_duration > SLOW_INIT_THRESHOLD {
                                    tracing::warn!(
                                        contract = %key,
                                        queued_operations = completion_info.queued_ops.len(),
                                        init_duration_ms = init_duration.as_millis(),
                                        threshold_ms = SLOW_INIT_THRESHOLD.as_millis(),
                                        "Contract initialization took longer than expected"
                                    );
                                } else {
                                    tracing::info!(
                                        contract = %key,
                                        queued_operations = completion_info.queued_ops.len(),
                                        init_duration_ms = init_duration.as_millis(),
                                        "Contract initialization complete"
                                    );
                                }

                                // Replay queued operations that arrived during initialization.
                                // These were UPDATE operations that couldn't proceed while the
                                // contract was being initialized. Now that initialization is
                                // complete, we apply them in order to the stored state.
                                let mut current = incoming_state.clone();
                                for op in completion_info.queued_ops {
                                    let queue_time = ContractInitTracker::queue_wait_duration(
                                        &op,
                                        completion_now,
                                    );
                                    tracing::info!(
                                        contract = %key,
                                        queue_time_ms = queue_time.as_millis(),
                                        is_delta = matches!(op.update, Either::Right(_)),
                                        has_related = op.related_contracts.states().next().is_some(),
                                        "Replaying queued operation after initialization"
                                    );

                                    let replay_updates = match op.update {
                                        Either::Left(state) => {
                                            vec![UpdateData::State(state.into())]
                                        }
                                        Either::Right(delta) => {
                                            vec![UpdateData::Delta(delta)]
                                        }
                                    };

                                    match self
                                        .attempt_state_update(
                                            &params,
                                            &current,
                                            &key,
                                            &replay_updates,
                                        )
                                        .await
                                    {
                                        Ok(Either::Left(new_state)) => {
                                            // Validate before accepting
                                            let valid = self
                                                .runtime
                                                .validate_state(
                                                    &key,
                                                    &params,
                                                    &new_state,
                                                    &op.related_contracts,
                                                )
                                                .map(|r| r == ValidateResult::Valid)
                                                .unwrap_or(false);

                                            if valid && new_state.as_ref() != current.as_ref() {
                                                if let Err(e) = self
                                                    .commit_state_update(&key, &params, &new_state)
                                                    .await
                                                {
                                                    tracing::warn!(
                                                        contract = %key,
                                                        error = %e,
                                                        "Failed to commit replayed queued operation"
                                                    );
                                                } else {
                                                    current = new_state;
                                                }
                                            } else if !valid {
                                                tracing::warn!(
                                                    contract = %key,
                                                    "Queued operation produced invalid state, skipping"
                                                );
                                            }
                                        }
                                        Ok(Either::Right(_missing)) => {
                                            tracing::warn!(
                                                contract = %key,
                                                "Queued operation needs related contracts, skipping"
                                            );
                                        }
                                        Err(e) => {
                                            tracing::warn!(
                                                contract = %key,
                                                error = %e,
                                                "Failed to replay queued operation, skipping"
                                            );
                                        }
                                    }
                                }
                            }

                            self.broadcast_state_change(key, incoming_state.clone())
                                .await;
                            return Ok(UpsertResult::Updated(incoming_state));
                        }
                    }
                    ValidateResult::Invalid => {
                        if let Some(dropped_count) = self.init_tracker.fail_initialization(&key) {
                            tracing::warn!(
                                contract = %key,
                                dropped_operations = dropped_count,
                                "Contract validation failed, dropping queued operations"
                            );
                        }
                        return Err(ExecutorError::request(StdContractError::invalid_put(key)));
                    }
                    ValidateResult::RequestRelated(_) => {
                        // fetch_related_for_validation resolves RequestRelated internally.
                        // If this is reached, it indicates a logic error in the helper.
                        if let Some(dropped_count) = self.init_tracker.fail_initialization(&key) {
                            tracing::warn!(
                                contract = %key,
                                dropped_operations = dropped_count,
                                "Unexpected RequestRelated after fetch, dropping queued operations"
                            );
                        }
                        return Err(ExecutorError::request(StdContractError::Put {
                            key,
                            cause: "unexpected RequestRelated after related contract resolution"
                                .into(),
                        }));
                    }
                }

                vec![UpdateData::State(incoming_state.clone().into())]
            }
            Either::Right(delta) => {
                vec![UpdateData::Delta(delta)]
            }
        };

        let current_state = match self.state_store.get(&key).await {
            Ok(s) => s,
            Err(StateStoreError::MissingContract(_)) => {
                tracing::warn!(
                    contract = %key,
                    phase = "upsert_failed",
                    "Missing contract for upsert"
                );
                return Err(ExecutorError::request(StdContractError::MissingContract {
                    key: key.into(),
                }));
            }
            Err(StateStoreError::Any(err)) => return Err(ExecutorError::other(err)),
            Err(err @ StateStoreError::StateTooLarge { .. }) => {
                return Err(ExecutorError::other(err));
            }
        };

        for (id, state) in related_contracts
            .states()
            .filter_map(|(id, c)| c.as_ref().map(|c| (id, c)))
        {
            updates.push(UpdateData::RelatedState {
                related_to: *id,
                state: state.clone(),
            });
        }

        let mut recovery_performed = false;
        let updated_state = match self
            .attempt_state_update(&params, &current_state, &key, &updates)
            .await
        {
            Ok(Either::Left(s)) => s,
            Ok(Either::Right(mut r)) => {
                let Some(c) = r.pop() else {
                    return Err(ExecutorError::internal_error());
                };
                return Err(ExecutorError::request(StdContractError::MissingRelated {
                    key: c.contract_instance_id,
                }));
            }
            Err(merge_err) => {
                // Merge failed. If we have a validated full incoming state, try to recover
                // by replacing the (likely corrupted) local state. The incoming state was
                // already validated at entry to this function.
                let Some(ref valid_incoming) = incoming_full_state else {
                    // Delta update failed and we don't have a full state to recover with.
                    // Propagate the error (the caller may send a ResyncRequest).
                    return Err(merge_err);
                };

                // Before assuming local state is corrupted, validate it. If the local
                // state is valid, the merge failure is legitimate (e.g., the incoming
                // state is older and the contract's merge function correctly rejected
                // it). Only trigger recovery when the local state itself fails
                // validation. See issue #3109.
                let local_valid = self
                    .runtime
                    .validate_state(&key, &params, &current_state, &related_contracts)
                    .map(|r| r == ValidateResult::Valid)
                    .unwrap_or(false);

                // Local state is valid — the merge failure is legitimate, not corruption.
                if local_valid {
                    tracing::info!(
                        contract = %key,
                        error = %merge_err,
                        local_state_size = current_state.size(),
                        incoming_state_size = valid_incoming.size(),
                        event = "merge_rejected_valid_local",
                        "Merge rejected incoming state but local state is valid - \
                         not replacing (incoming state may be stale)"
                    );
                    return Err(merge_err);
                }

                // Local state failed validation — it's likely corrupted.

                // Check and mark recovery in a single lock acquisition.
                let already_recovered = {
                    let mut guard = self
                        .recovery_guard
                        .lock()
                        .unwrap_or_else(|e| e.into_inner());
                    if guard.contains(&key) {
                        true
                    } else {
                        guard.insert(key);
                        false
                    }
                };

                if already_recovered {
                    tracing::error!(
                        contract = %key,
                        error = %merge_err,
                        event = "corrupted_state_recovery_exhausted",
                        "State recovery already attempted, contract is broken - not retrying"
                    );
                    return Err(merge_err);
                }

                tracing::warn!(
                    contract = %key,
                    error = %merge_err,
                    incoming_state_size = valid_incoming.size(),
                    event = "corrupted_state_recovery",
                    "Merge failed with validated incoming state and local state is invalid - \
                     replacing corrupted local state with incoming state"
                );

                recovery_performed = true;
                valid_incoming.clone()
            }
        };

        // Clear the recovery guard for this contract on a successful merge
        // (NOT on the same call that performed recovery — the guard must persist
        // so a subsequent failure is detected as a broken contract).
        if incoming_full_state.is_some() && !recovery_performed {
            self.recovery_guard
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .remove(&key);
        }

        let result = self
            .fetch_related_for_validation(&key, &params, &updated_state, &related_contracts)
            .await?;

        if result != ValidateResult::Valid {
            return Err(Self::validation_error(key, result));
        }
        if updated_state.as_ref() == current_state.as_ref() {
            Ok(UpsertResult::NoChange)
        } else {
            self.commit_state_update(&key, &params, &updated_state)
                .await?;
            Ok(UpsertResult::Updated(updated_state))
        }
    }

    pub(super) fn bridged_register_contract_notifier(
        &mut self,
        instance_id: ContractInstanceId,
        cli_id: ClientId,
        notification_ch: tokio::sync::mpsc::Sender<HostResult>,
        summary: Option<StateSummary<'_>>,
    ) -> Result<(), Box<RequestError>> {
        // Check if already registered (immutable borrow)
        let already_registered = self
            .update_notifications
            .get(&instance_id)
            .and_then(|channels| {
                channels
                    .binary_search_by_key(&&cli_id, |(p, _)| p)
                    .ok()
                    .map(|i| (i, channels[i].1.same_channel(&notification_ch)))
            });

        if let Some((idx, same_channel)) = already_registered {
            if !same_channel {
                tracing::info!(
                    contract = %instance_id,
                    client = %cli_id,
                    "Client already subscribed, updating notification channel"
                );
                // Safety: `already_registered` was derived from `self.update_notifications.get(&instance_id)`
                // succeeding, so the entry is guaranteed to exist.
                if let Some(channels) = self.update_notifications.get_mut(&instance_id) {
                    channels[idx] = (cli_id, notification_ch);
                }
            }
        } else {
            // New subscriber: enforce per-contract limit
            let contract_sub_count = self
                .update_notifications
                .get(&instance_id)
                .map_or(0, |ch| ch.len());
            if contract_sub_count >= super::MAX_SUBSCRIBERS_PER_CONTRACT {
                tracing::warn!(
                    client = %cli_id,
                    contract = %instance_id,
                    limit = super::MAX_SUBSCRIBERS_PER_CONTRACT,
                    "Subscriber limit reached for contract, rejecting registration"
                );
                return Err(subscriber_limit_error(
                    instance_id,
                    &format!(
                        "subscriber limit ({}) reached for contract",
                        super::MAX_SUBSCRIBERS_PER_CONTRACT
                    ),
                ));
            }

            // Enforce per-client subscription limit using O(1) counter
            let client_sub_count = self
                .client_subscription_counts
                .get(&cli_id)
                .copied()
                .unwrap_or(0);
            if client_sub_count >= super::MAX_SUBSCRIPTIONS_PER_CLIENT {
                tracing::warn!(
                    client = %cli_id,
                    contract = %instance_id,
                    limit = super::MAX_SUBSCRIPTIONS_PER_CLIENT,
                    current = client_sub_count,
                    "Per-client subscription limit reached, rejecting registration"
                );
                return Err(subscriber_limit_error(
                    instance_id,
                    &format!(
                        "per-client subscription limit ({}) reached",
                        super::MAX_SUBSCRIPTIONS_PER_CLIENT
                    ),
                ));
            }

            // Insert in sorted order for efficient lookup (matches pool path)
            let channels = self.update_notifications.entry(instance_id).or_default();
            let insert_pos = channels.partition_point(|(id, _)| id < &cli_id);
            channels.insert(insert_pos, (cli_id, notification_ch));
            *self.client_subscription_counts.entry(cli_id).or_insert(0) += 1;
        }

        if self
            .subscriber_summaries
            .entry(instance_id)
            .or_default()
            .insert(cli_id, summary.map(StateSummary::into_owned))
            .is_some()
        {
            tracing::debug!(
                contract = %instance_id,
                client = %cli_id,
                "Contract already registered for client, replaced summary"
            );
        }
        Ok(())
    }

    pub(super) fn bridged_notify_subscription_error(
        &self,
        key: ContractInstanceId,
        reason: String,
    ) {
        if let Some(channels) = self.update_notifications.get(&key) {
            send_subscription_error_to_clients(channels, key, reason);
        }
    }

    pub(super) async fn bridged_summarize_contract_state(
        &mut self,
        key: ContractKey,
    ) -> Result<StateSummary<'static>, ExecutorError> {
        let (state, _) = self.bridged_fetch_contract(key, false).await?;

        let state = state.ok_or_else(|| {
            ExecutorError::request(StdContractError::Get {
                key,
                cause: "contract state not found".into(),
            })
        })?;

        // Check summary cache: if state hash matches, return cached summary
        let state_hash = {
            use std::hash::{Hash, Hasher};
            let mut hasher = std::collections::hash_map::DefaultHasher::new();
            state.as_ref().hash(&mut hasher);
            hasher.finish()
        };

        if let Some((cached_hash, cached_summary)) = self.summary_cache.get(&key) {
            if *cached_hash == state_hash {
                return Ok(cached_summary.clone());
            }
        }

        let params = self
            .state_store
            .get_params(&key)
            .await
            .map_err(ExecutorError::other)?
            .ok_or_else(|| {
                ExecutorError::request(StdContractError::Get {
                    key,
                    cause: "contract parameters not found".into(),
                })
            })?;

        let summary = self
            .runtime
            .summarize_state(&key, &params, &state)
            .map_err(|e| ExecutorError::execution(e, None))?;

        self.summary_cache.put(key, (state_hash, summary.clone()));
        Ok(summary)
    }

    pub(super) async fn bridged_get_contract_state_delta(
        &mut self,
        key: ContractKey,
        their_summary: StateSummary<'static>,
    ) -> Result<StateDelta<'static>, ExecutorError> {
        let (state, _) = self.bridged_fetch_contract(key, false).await?;

        let state = state.ok_or_else(|| {
            ExecutorError::request(StdContractError::Get {
                key,
                cause: "contract state not found".into(),
            })
        })?;

        // Check delta cache: if (state_hash, their_summary_hash) matches, return cached delta
        let (state_hash, summary_hash) = {
            use std::hash::{Hash, Hasher};
            let mut h1 = std::collections::hash_map::DefaultHasher::new();
            state.as_ref().hash(&mut h1);
            let mut h2 = std::collections::hash_map::DefaultHasher::new();
            their_summary.as_ref().hash(&mut h2);
            (h1.finish(), h2.finish())
        };

        let cache_key = (key, state_hash, summary_hash);
        if let Some(cached_delta) = self.delta_cache.get(&cache_key) {
            return Ok(cached_delta.clone());
        }

        let params = self
            .state_store
            .get_params(&key)
            .await
            .map_err(ExecutorError::other)?
            .ok_or_else(|| {
                ExecutorError::request(StdContractError::Get {
                    key,
                    cause: "contract parameters not found".into(),
                })
            })?;

        let delta = self
            .runtime
            .get_state_delta(&key, &params, &state, &their_summary)
            .map_err(|e| ExecutorError::execution(e, None))?;

        self.delta_cache.put(cache_key, delta.clone());
        Ok(delta)
    }

    // --- Helper methods ---

    async fn perform_contract_get(
        &mut self,
        return_contract_code: bool,
        key: ContractKey,
    ) -> Result<(Option<WrappedState>, Option<ContractContainer>), ExecutorError> {
        tracing::debug!(
            contract = %key,
            return_code = return_contract_code,
            "Getting contract"
        );
        let mut got_contract: Option<ContractContainer> = None;

        if return_contract_code {
            if let Some(contract) = self.get_contract_locally(&key).await? {
                got_contract = Some(contract);
            }
        }

        let state_result = self.state_store.get(&key).await;
        tracing::debug!(
            contract = %key,
            state_found = state_result.is_ok(),
            has_contract = got_contract.is_some(),
            "Contract get result"
        );
        match state_result {
            Ok(state) => Ok((Some(state), got_contract)),
            Err(StateStoreError::MissingContract(_)) => {
                tracing::warn!(contract = %key, "Contract state not found in store");
                Ok((None, got_contract))
            }
            Err(err) => {
                tracing::error!(contract = %key, error = %err, "Failed to get contract state");
                Err(ExecutorError::request(RequestError::from(
                    StdContractError::Get {
                        key,
                        cause: format!("{err}").into(),
                    },
                )))
            }
        }
    }

    async fn get_contract_locally(
        &self,
        key: &ContractKey,
    ) -> Result<Option<ContractContainer>, ExecutorError> {
        let Some(parameters) = self
            .state_store
            .get_params(key)
            .await
            .map_err(ExecutorError::other)?
        else {
            tracing::debug!(
                contract = %key,
                "Contract parameters not in state_store, cannot fetch contract"
            );
            return Ok(None);
        };

        let Some(contract) = self.runtime.fetch_contract_code(key, &parameters) else {
            return Ok(None);
        };
        Ok(Some(contract))
    }

    async fn attempt_state_update(
        &mut self,
        parameters: &Parameters<'_>,
        current_state: &WrappedState,
        key: &ContractKey,
        updates: &[UpdateData<'_>],
    ) -> Result<Either<WrappedState, Vec<RelatedContract>>, ExecutorError> {
        let update_modification =
            match self
                .runtime
                .update_state(key, parameters, current_state, updates)
            {
                Ok(result) => result,
                Err(err) => {
                    return Err(ExecutorError::execution(
                        err,
                        Some(InnerOpError::Upsert(*key)),
                    ));
                }
            };
        let UpdateModification {
            new_state, related, ..
        } = update_modification;
        let Some(new_state) = new_state else {
            if related.is_empty() {
                return Ok(Either::Left(current_state.clone()));
            } else {
                return Ok(Either::Right(related));
            }
        };
        let new_state = WrappedState::new(new_state.into_bytes());

        if new_state.as_ref() == current_state.as_ref() {
            tracing::debug!(
                contract = %key,
                phase = "update_skipped",
                "No changes in state, avoiding update"
            );
            return Ok(Either::Left(current_state.clone()));
        }

        Ok(Either::Left(new_state))
    }

    async fn commit_state_update(
        &mut self,
        key: &ContractKey,
        parameters: &Parameters<'_>,
        new_state: &WrappedState,
    ) -> Result<(), ExecutorError> {
        let state_size = new_state.as_ref().len();
        if state_size > MAX_STATE_SIZE {
            tracing::warn!(
                contract = %key,
                size_bytes = state_size,
                limit_bytes = MAX_STATE_SIZE,
                "Rejecting oversized contract state at executor layer"
            );
            return Err(ExecutorError::request(StdContractError::Update {
                key: *key,
                cause: format!(
                    "state size {state_size} bytes exceeds maximum allowed {MAX_STATE_SIZE} bytes"
                )
                .into(),
            }));
        }

        self.state_store
            .update(key, new_state.clone())
            .await
            .map_err(ExecutorError::other)?;

        tracing::info!(
            contract = %key,
            new_size_bytes = new_state.as_ref().len(),
            phase = "update_complete",
            "Contract state updated"
        );

        // Record update timestamp for dashboard display
        crate::node::network_status::record_contract_updated(&format!("{key}"));

        if let Err(err) = self
            .send_update_notification(key, parameters, new_state)
            .await
        {
            tracing::error!(
                contract = %key,
                error = %err,
                phase = "notification_failed",
                "Failed to send update notification"
            );
        }

        // Notify subscribed delegates about the state change
        self.send_delegate_contract_notifications(key, new_state);

        if let Some(op_manager) = &self.op_manager {
            if let Err(err) = op_manager
                .notify_node_event(crate::message::NodeEvent::BroadcastStateChange {
                    key: *key,
                    new_state: new_state.clone(),
                })
                .await
            {
                tracing::warn!(
                    contract = %key,
                    error = %err,
                    "Failed to broadcast state change to network peers"
                );
            }
        }

        Ok(())
    }

    /// Send notifications to delegates subscribed to a contract's state changes.
    ///
    /// Checks the global `DELEGATE_SUBSCRIPTIONS` registry and sends a
    /// `DelegateNotification` for each subscribed delegate through the channel.
    ///
    /// This is a **best-effort, lossy** notification path: if the bounded channel
    /// is full, notifications are dropped rather than blocking the commit path.
    /// Delegates that require guaranteed delivery should poll contract state
    /// periodically as a fallback.
    fn send_delegate_contract_notifications(&self, key: &ContractKey, new_state: &WrappedState) {
        let tx = match &self.delegate_notification_tx {
            Some(tx) => tx,
            None => return,
        };

        let instance_id = *key.id();
        // Snapshot subscribers and release the DashMap read-lock before sending
        let subscribers: Vec<DelegateKey> = {
            let entry = crate::wasm_runtime::DELEGATE_SUBSCRIPTIONS.get(&instance_id);
            match entry {
                Some(ref s) if !s.is_empty() => s.iter().cloned().collect(),
                _ => return,
            }
        };

        tracing::debug!(
            contract = %key,
            subscriber_count = subscribers.len(),
            "Sending delegate contract notifications"
        );

        // Share one Arc allocation across all subscribers
        let shared_state = Arc::new(new_state.clone());

        for delegate_key in subscribers {
            match tx.try_send(super::DelegateNotification {
                delegate_key: delegate_key.clone(),
                contract_id: instance_id,
                new_state: Arc::clone(&shared_state),
            }) {
                Ok(()) => {}
                Err(mpsc::error::TrySendError::Full(_)) => {
                    static DROPPED: AtomicUsize = AtomicUsize::new(0);
                    let total = DROPPED.fetch_add(1, Ordering::Relaxed) + 1;
                    tracing::warn!(
                        contract = %key,
                        delegate = %delegate_key,
                        total_dropped = total,
                        "Delegate notification channel full — notification dropped"
                    );
                }
                Err(mpsc::error::TrySendError::Closed(_)) => {
                    tracing::warn!(
                        contract = %key,
                        "Delegate notification channel closed — removing stale subscriptions"
                    );
                    // Receiver is gone; clean up all subscriptions for this contract
                    // to prevent repeated failed sends on future state updates.
                    crate::wasm_runtime::DELEGATE_SUBSCRIPTIONS.remove(&instance_id);
                    return;
                }
            }
        }
    }

    /// Validate a contract's state, automatically fetching related contracts if requested.
    ///
    /// Depth=1 only: the original contract may request related contract states via
    /// `RequestRelated`. Those related contracts are fetched locally and
    /// `validate_state` is called exactly once more. If the second call also returns
    /// `RequestRelated`, that's an error — contracts must declare all dependencies in
    /// one round.
    ///
    /// # Return value
    /// On success, returns only `Valid` or `Invalid`, never `RequestRelated`.
    /// `RequestRelated` is resolved internally or converted to an error.
    ///
    /// # Safety limits
    /// - At most `MAX_RELATED_CONTRACTS_PER_REQUEST` (10) related contracts
    /// - Self-reference (requesting own ID) is rejected
    /// - Empty `RequestRelated` is rejected
    /// - Overall timeout of `RELATED_FETCH_TIMEOUT` (10s)
    async fn fetch_related_for_validation(
        &mut self,
        key: &ContractKey,
        params: &Parameters<'_>,
        state: &WrappedState,
        initial_related: &RelatedContracts<'_>,
    ) -> Result<ValidateResult, ExecutorError> {
        let result = self
            .runtime
            .validate_state(key, params, state, initial_related)
            .map_err(|e| ExecutorError::execution(e, None))?;

        let requested_ids = match result {
            ValidateResult::Valid | ValidateResult::Invalid => return Ok(result),
            ValidateResult::RequestRelated(ids) => ids,
        };

        // Reject empty requests
        if requested_ids.is_empty() {
            tracing::warn!(
                contract = %key,
                "Contract returned RequestRelated with empty list"
            );
            return Err(ExecutorError::request(StdContractError::Put {
                key: *key,
                cause: "contract requested related contracts but provided empty list".into(),
            }));
        }

        // Reject self-reference
        let self_id = key.id();
        if requested_ids.iter().any(|id| id == self_id) {
            tracing::warn!(
                contract = %key,
                "Contract requested its own state as a related contract"
            );
            return Err(ExecutorError::request(StdContractError::Put {
                key: *key,
                cause: "contract cannot request itself as a related contract".into(),
            }));
        }

        // Dedup
        let unique_ids: HashSet<ContractInstanceId> = requested_ids.into_iter().collect();

        // Reject too many
        if unique_ids.len() > MAX_RELATED_CONTRACTS_PER_REQUEST {
            tracing::warn!(
                contract = %key,
                requested = unique_ids.len(),
                limit = MAX_RELATED_CONTRACTS_PER_REQUEST,
                "Contract requested too many related contracts"
            );
            return Err(ExecutorError::request(StdContractError::Put {
                key: *key,
                cause: format!(
                    "contract requested {} related contracts, limit is {}",
                    unique_ids.len(),
                    MAX_RELATED_CONTRACTS_PER_REQUEST
                )
                .into(),
            }));
        }

        tracing::debug!(
            contract = %key,
            related_count = unique_ids.len(),
            "Fetching related contracts for validation"
        );

        // Fetch each related contract locally with overall timeout.
        // Local lookup via lookup_key + state_store.get() is available on all executor
        // types (Runtime, MockWasmRuntime). Network fetch is only available on
        // Executor<Runtime> and happens automatically via GET auto-subscribe when
        // the contract isn't found locally.
        let mut related_map: HashMap<ContractInstanceId, Option<State<'static>>> =
            HashMap::with_capacity(unique_ids.len());

        let fetch_all = async {
            for id in &unique_ids {
                let full_key = self.bridged_lookup_key(id).ok_or_else(|| {
                    ExecutorError::request(StdContractError::MissingRelated { key: *id })
                })?;
                let state = self.state_store.get(&full_key).await.map_err(|_| {
                    ExecutorError::request(StdContractError::MissingRelated { key: *id })
                })?;
                related_map.insert(*id, Some(State::from(state.as_ref().to_vec())));
            }
            Ok::<(), ExecutorError>(())
        };

        match tokio::time::timeout(RELATED_FETCH_TIMEOUT, fetch_all).await {
            Ok(Ok(())) => {}
            Ok(Err(e)) => {
                tracing::warn!(
                    contract = %key,
                    error = %e,
                    "Failed to fetch related contracts"
                );
                return Err(e);
            }
            Err(_elapsed) => {
                tracing::warn!(
                    contract = %key,
                    timeout_secs = RELATED_FETCH_TIMEOUT.as_secs(),
                    fetched = related_map.len(),
                    total = unique_ids.len(),
                    "Timed out fetching related contracts"
                );
                return Err(ExecutorError::request(StdContractError::Put {
                    key: *key,
                    cause: "timed out fetching related contracts".into(),
                }));
            }
        }

        // Merge initial_related (caller-provided) with newly fetched states.
        // The contract's first call saw initial_related; the second call should see
        // both the original entries and the newly fetched ones.
        let initial_owned = initial_related.clone().into_owned();
        for (id, state) in initial_owned.states() {
            if let Some(s) = state {
                related_map
                    .entry(*id)
                    .or_insert_with(|| Some(s.clone().into_owned()));
            }
        }
        let populated_related = RelatedContracts::from(related_map);
        let retry_result = self
            .runtime
            .validate_state(key, params, state, &populated_related)
            .map_err(|e| ExecutorError::execution(e, None))?;

        // If the contract requests more related contracts, that's depth>1 — reject
        if let ValidateResult::RequestRelated(_) = &retry_result {
            tracing::warn!(
                contract = %key,
                "Contract returned RequestRelated after related contracts were provided (depth>1 not supported)"
            );
            return Err(ExecutorError::request(StdContractError::Put {
                key: *key,
                cause: "contract requested additional related contracts after first round (depth=1 limit exceeded)".into(),
            }));
        }

        Ok(retry_result)
    }

    /// Build an Update error for a non-Valid validation result.
    ///
    /// Used by UPDATE code paths. For PUT paths, use `validation_error_put`.
    fn validation_error(key: ContractKey, result: ValidateResult) -> ExecutorError {
        match result {
            ValidateResult::Invalid => {
                ExecutorError::request(freenet_stdlib::client_api::ContractError::Update {
                    key,
                    cause: "invalid outcome state".into(),
                })
            }
            ValidateResult::RequestRelated(_) => {
                tracing::error!(
                    contract = %key,
                    "validation_error called with RequestRelated — expected only Invalid"
                );
                ExecutorError::request(freenet_stdlib::client_api::ContractError::Update {
                    key,
                    cause: "missing related contracts for validation".into(),
                })
            }
            ValidateResult::Valid => {
                tracing::error!(
                    contract = %key,
                    "validation_error called with Valid result — this is a bug"
                );
                ExecutorError::internal_error()
            }
        }
    }

    /// Build a Put error for a non-Valid validation result.
    ///
    /// Used by PUT code paths to preserve correct error semantics for callers.
    fn validation_error_put(key: ContractKey, result: ValidateResult) -> ExecutorError {
        match result {
            ValidateResult::Invalid => ExecutorError::request(StdContractError::Put {
                key,
                cause: "invalid outcome state after merge".into(),
            }),
            ValidateResult::RequestRelated(_) => {
                tracing::error!(
                    contract = %key,
                    "validation_error_put called with RequestRelated — expected only Invalid"
                );
                ExecutorError::request(StdContractError::Put {
                    key,
                    cause: "missing related contracts for validation".into(),
                })
            }
            ValidateResult::Valid => {
                tracing::error!(
                    contract = %key,
                    "validation_error_put called with Valid result — this is a bug"
                );
                ExecutorError::internal_error()
            }
        }
    }

    async fn broadcast_state_change(&self, key: ContractKey, new_state: WrappedState) {
        if let Some(op_manager) = &self.op_manager {
            if let Err(err) = op_manager
                .notify_node_event(crate::message::NodeEvent::BroadcastStateChange {
                    key,
                    new_state,
                })
                .await
            {
                tracing::warn!(
                    contract = %key,
                    error = %err,
                    "Failed to broadcast state change to network peers"
                );
            }
        }
    }

    async fn send_update_notification(
        &mut self,
        key: &ContractKey,
        params: &Parameters<'_>,
        new_state: &WrappedState,
    ) -> Result<(), ExecutorError> {
        tracing::debug!(contract = %key, "notify of contract update");
        let key = *key;
        let instance_id = *key.id();

        if let (Some(shared_notifications), Some(shared_summaries)) = (
            self.shared_notifications.as_ref(),
            self.shared_summaries.as_ref(),
        ) {
            // Snapshot subscribers and release the DashMap read-lock before sending
            let notifiers_snapshot: Vec<(ClientId, mpsc::Sender<HostResult>)> =
                match shared_notifications.get(&instance_id) {
                    Some(notifiers) => notifiers.value().clone(),
                    None => return Ok(()),
                };

            let summaries_snapshot: HashMap<ClientId, Option<StateSummary<'static>>> =
                shared_summaries
                    .get(&instance_id)
                    .map_or_else(HashMap::new, |s| s.value().clone());

            if notifiers_snapshot.len() > super::FANOUT_WARNING_THRESHOLD {
                tracing::warn!(
                    contract = %key,
                    subscriber_count = notifiers_snapshot.len(),
                    "High subscriber count for notification fan-out"
                );
            }

            let mut failures = Vec::with_capacity(32);
            let mut delta_computations = 0usize;
            // Pre-allocate full state once for subscribers that don't get deltas
            let full_state = State::from(new_state.as_ref()).into_owned();

            for (peer_key, notifier) in &notifiers_snapshot {
                let peer_summary = summaries_snapshot.get(peer_key).and_then(|s| s.as_ref());

                let update = match peer_summary {
                    Some(summary)
                        if delta_computations < super::MAX_DELTA_COMPUTATIONS_PER_FANOUT =>
                    {
                        delta_computations += 1;
                        self.runtime
                            .get_state_delta(&key, params, new_state, summary)
                            .map_err(|err| {
                                tracing::error!("{err}");
                                ExecutorError::execution(err, Some(InnerOpError::Upsert(key)))
                            })?
                            .to_owned()
                            .into()
                    }
                    Some(_) => {
                        // Delta computation cap reached: send full state instead of
                        // running another WASM get_state_delta() call
                        tracing::debug!(
                            client = %peer_key,
                            contract = %key,
                            "Delta computation cap reached, sending full state"
                        );
                        UpdateData::State(full_state.clone())
                    }
                    None => UpdateData::State(full_state.clone()),
                };

                match notifier.try_send(Ok(
                    ContractResponse::UpdateNotification { key, update }.into()
                )) {
                    Ok(()) => {
                        tracing::debug!(
                            client = %peer_key,
                            contract = %key,
                            phase = "notification_sent_shared",
                            "Sent update notification to client (shared storage)"
                        );
                    }
                    Err(mpsc::error::TrySendError::Full(_)) => {
                        tracing::warn!(
                            client = %peer_key,
                            contract = %key,
                            "Subscriber notification channel full — notification dropped"
                        );
                    }
                    Err(mpsc::error::TrySendError::Closed(_)) => {
                        failures.push(*peer_key);
                        tracing::error!(
                            client = %peer_key,
                            contract = %key,
                            phase = "notification_send_failed_shared",
                            "Failed to send update notification to client (channel closed)"
                        );
                    }
                }
            }

            if !failures.is_empty() {
                if let Some(mut notifiers) = shared_notifications.get_mut(&instance_id) {
                    notifiers.retain(|(c, _)| !failures.contains(c));
                }

                if let Some(mut contract_summaries) = shared_summaries.get_mut(&instance_id) {
                    for failed_client in &failures {
                        contract_summaries.remove(failed_client);
                    }
                }

                // Decrement per-client subscription counters for failed clients
                if let Some(shared_client_counts) = &self.shared_client_counts {
                    for failed_client in &failures {
                        let remove = shared_client_counts
                            .get_mut(failed_client)
                            .map(|mut count| {
                                *count = count.saturating_sub(1);
                                *count == 0
                            })
                            .unwrap_or(false);
                        if remove {
                            shared_client_counts.remove(failed_client);
                        }
                    }
                }
            }
        } else if let Some(notifiers) = self.update_notifications.get_mut(&instance_id) {
            let summaries = self.subscriber_summaries.get_mut(&instance_id).unwrap();

            if notifiers.len() > super::FANOUT_WARNING_THRESHOLD {
                tracing::warn!(
                    contract = %key,
                    subscriber_count = notifiers.len(),
                    "High subscriber count for notification fan-out"
                );
            }

            let mut failures = Vec::with_capacity(32);
            let mut delta_computations = 0usize;
            // Pre-allocate full state once for subscribers that don't get deltas
            let full_state = State::from(new_state.as_ref()).into_owned();

            for (peer_key, notifier) in notifiers.iter() {
                let peer_summary = summaries.get_mut(peer_key).unwrap();
                let update = match peer_summary {
                    Some(summary)
                        if delta_computations < super::MAX_DELTA_COMPUTATIONS_PER_FANOUT =>
                    {
                        delta_computations += 1;
                        self.runtime
                            .get_state_delta(&key, params, new_state, &*summary)
                            .map_err(|err| {
                                tracing::error!("{err}");
                                ExecutorError::execution(err, Some(InnerOpError::Upsert(key)))
                            })?
                            .to_owned()
                            .into()
                    }
                    Some(_) => {
                        tracing::debug!(
                            client = %peer_key,
                            contract = %key,
                            "Delta computation cap reached, sending full state"
                        );
                        UpdateData::State(full_state.clone())
                    }
                    None => UpdateData::State(full_state.clone()),
                };

                match notifier.try_send(Ok(
                    ContractResponse::UpdateNotification { key, update }.into()
                )) {
                    Ok(()) => {
                        tracing::debug!(
                            client = %peer_key,
                            contract = %key,
                            phase = "notification_sent",
                            "Sent update notification to client"
                        );
                    }
                    Err(mpsc::error::TrySendError::Full(_)) => {
                        tracing::warn!(
                            client = %peer_key,
                            contract = %key,
                            "Subscriber notification channel full — notification dropped"
                        );
                    }
                    Err(mpsc::error::TrySendError::Closed(_)) => {
                        failures.push(*peer_key);
                        tracing::error!(
                            client = %peer_key,
                            contract = %key,
                            phase = "notification_send_failed",
                            "Failed to send update notification to client (channel closed)"
                        );
                    }
                }
            }

            if !failures.is_empty() {
                notifiers.retain(|(c, _)| !failures.contains(c));
                // Decrement per-client subscription counters for failed clients
                for failed_client in &failures {
                    if let Some(count) = self.client_subscription_counts.get_mut(failed_client) {
                        *count = count.saturating_sub(1);
                        if *count == 0 {
                            self.client_subscription_counts.remove(failed_client);
                        }
                    }
                }
            }
        }
        Ok(())
    }
}

// ============================================================================
// ContractExecutor for Executor<Runtime> - delegates to bridged methods
// ============================================================================

impl ContractExecutor for Executor<Runtime> {
    fn lookup_key(&self, instance_id: &ContractInstanceId) -> Option<ContractKey> {
        self.bridged_lookup_key(instance_id)
    }

    async fn fetch_contract(
        &mut self,
        key: ContractKey,
        return_contract_code: bool,
    ) -> Result<(Option<WrappedState>, Option<ContractContainer>), ExecutorError> {
        self.bridged_fetch_contract(key, return_contract_code).await
    }

    async fn upsert_contract_state(
        &mut self,
        key: ContractKey,
        update: Either<WrappedState, StateDelta<'static>>,
        related_contracts: RelatedContracts<'static>,
        code: Option<ContractContainer>,
    ) -> Result<UpsertResult, ExecutorError> {
        self.bridged_upsert_contract_state(key, update, related_contracts, code)
            .await
    }

    fn register_contract_notifier(
        &mut self,
        instance_id: ContractInstanceId,
        cli_id: ClientId,
        notification_ch: tokio::sync::mpsc::Sender<HostResult>,
        summary: Option<StateSummary<'_>>,
    ) -> Result<(), Box<RequestError>> {
        self.bridged_register_contract_notifier(instance_id, cli_id, notification_ch, summary)
    }

    async fn execute_delegate_request(
        &mut self,
        req: DelegateRequest<'_>,
        origin_contract: Option<&ContractInstanceId>,
        caller_delegate: Option<&DelegateKey>,
    ) -> Response {
        self.delegate_request(req, origin_contract, caller_delegate)
    }

    fn get_subscription_info(&self) -> Vec<crate::message::SubscriptionInfo> {
        self.get_subscription_info()
    }

    fn notify_subscription_error(&self, key: ContractInstanceId, reason: String) {
        self.bridged_notify_subscription_error(key, reason)
    }

    async fn summarize_contract_state(
        &mut self,
        key: ContractKey,
    ) -> Result<StateSummary<'static>, ExecutorError> {
        self.bridged_summarize_contract_state(key).await
    }

    async fn get_contract_state_delta(
        &mut self,
        key: ContractKey,
        their_summary: StateSummary<'static>,
    ) -> Result<StateDelta<'static>, ExecutorError> {
        self.bridged_get_contract_state_delta(key, their_summary)
            .await
    }
}

impl Executor<Runtime> {
    /// Create an Executor for local-only mode (no network operations).
    /// Use this from the binary for local mode execution.
    pub async fn from_config_local(config: Arc<Config>) -> anyhow::Result<Self> {
        Self::from_config(config, None, None).await
    }

    /// Create an Executor with optional network operation support.
    /// This is `pub(crate)` because the parameters involve crate-internal types.
    pub(crate) async fn from_config(
        config: Arc<Config>,
        op_sender: Option<OpRequestSender>,
        op_manager: Option<Arc<OpManager>>,
    ) -> anyhow::Result<Self> {
        let (contract_store, delegate_store, secret_store, state_store) =
            Self::get_stores(&config).await?;
        let mut rt = Runtime::build(contract_store, delegate_store, secret_store, false).unwrap();
        // Enable V2 delegate contract access by providing the state store DB
        rt.set_state_store_db(state_store.storage());
        Executor::new(
            state_store,
            move || {
                if let Err(error) = crate::util::set_cleanup_on_exit(config.paths().clone()) {
                    tracing::error!("Failed to set cleanup on exit: {error}");
                }
                Ok(())
            },
            OperationMode::Local,
            rt,
            op_sender,
            op_manager,
        )
        .await
    }

    /// Create an executor that shares compiled module caches and backend engine
    /// with other pool executors.
    ///
    /// If `shared_backend` is `None`, a new backend engine is created (used for
    /// the first executor in a pool). If `Some`, the provided engine is shared
    /// (used for subsequent executors and replacements).
    pub(crate) async fn from_config_with_shared_modules(
        config: Arc<Config>,
        shared_state_store: StateStore<Storage>,
        op_sender: Option<OpRequestSender>,
        op_manager: Option<Arc<OpManager>>,
        contract_modules: SharedModuleCache<ContractKey>,
        delegate_modules: SharedModuleCache<DelegateKey>,
        shared_backend: Option<BackendEngine>,
    ) -> anyhow::Result<Self> {
        let db = shared_state_store.storage();
        let (contract_store, delegate_store, secret_store) =
            Self::get_runtime_stores(&config, db.clone())?;
        let mut rt = Runtime::build_with_shared_module_caches(
            contract_store,
            delegate_store,
            secret_store,
            false,
            contract_modules,
            delegate_modules,
            shared_backend.unwrap_or_else(|| {
                // First executor — create a fresh backend engine; RuntimePool
                // will extract and share it with subsequent executors.
                crate::wasm_runtime::engine::Engine::create_backend_engine(
                        &RuntimeConfig::default(),
                    )
                    .expect("Failed to create WASM backend engine")
            }),
        )
        .unwrap();
        rt.set_state_store_db(db);
        Executor::new(
            shared_state_store,
            || Ok(()),
            OperationMode::Local,
            rt,
            op_sender,
            op_manager,
        )
        .await
    }

    pub async fn preload(
        &mut self,
        cli_id: ClientId,
        contract: ContractContainer,
        state: WrappedState,
        related_contracts: RelatedContracts<'static>,
    ) {
        if let Err(err) = self
            .contract_requests(
                ContractRequest::Put {
                    contract,
                    state,
                    related_contracts,
                    subscribe: false,
                    blocking_subscribe: false,
                },
                cli_id,
                None,
            )
            .await
        {
            match err.inner {
                Either::Left(err) => tracing::error!("req error: {err}"),
                Either::Right(err) => tracing::error!("other error: {err}"),
            }
        }
    }

    pub async fn handle_request(
        &mut self,
        id: ClientId,
        req: ClientRequest<'_>,
        updates: Option<mpsc::Sender<Result<HostResponse, WsClientError>>>,
    ) -> Response {
        match req {
            ClientRequest::ContractOp(op) => self.contract_requests(op, id, updates).await,
            ClientRequest::DelegateOp(op) => self.delegate_request(op, None, None),
            ClientRequest::Disconnect { cause } => {
                if let Some(cause) = cause {
                    tracing::info!("disconnecting cause: {cause}");
                }
                Err(RequestError::Disconnect.into())
            }
            other @ (ClientRequest::Authenticate { .. }
            | ClientRequest::NodeQueries(_)
            | ClientRequest::Close
            | _) => {
                tracing::warn!(
                    client = %id,
                    request = ?other,
                    "unsupported client request"
                );
                Err(ExecutorError::other(anyhow::anyhow!("not supported")))
            }
        }
    }

    /// Respond to requests made through any API's from client applications in local mode.
    pub async fn contract_requests(
        &mut self,
        req: ContractRequest<'_>,
        cli_id: ClientId,
        updates: Option<mpsc::Sender<Result<HostResponse, WsClientError>>>,
    ) -> Response {
        tracing::debug!(
            client = %cli_id,
            "received contract request"
        );
        let result = match req {
            ContractRequest::Put {
                contract,
                state,
                related_contracts,
                ..
            } => {
                tracing::debug!(
                    client = %cli_id,
                    contract = %contract.key(),
                    state_size = state.as_ref().len(),
                    "putting contract"
                );
                self.perform_contract_put(contract, state, related_contracts)
                    .await
            }
            ContractRequest::Update { key, data } => self.perform_contract_update(key, data).await,
            // Handle Get requests by returning the contract state and optionally the contract code
            ContractRequest::Get {
                key: instance_id,
                return_contract_code,
                ..
            } => {
                // Look up the full key from the instance_id
                let full_key = self.lookup_key(&instance_id).ok_or_else(|| {
                    tracing::debug!(
                        contract = %instance_id,
                        phase = "key_lookup_failed",
                        "Contract not found during get request"
                    );
                    ExecutorError::request(StdContractError::MissingContract { key: instance_id })
                })?;

                match self
                    .perform_contract_get(return_contract_code, full_key)
                    .await
                {
                    Ok((state, contract)) => Ok(ContractResponse::GetResponse {
                        key: full_key,
                        state: state.ok_or_else(|| {
                            tracing::debug!(
                                contract = %full_key,
                                phase = "get_failed",
                                "Contract state not found during get request"
                            );
                            ExecutorError::request(StdContractError::Get {
                                key: full_key,
                                cause: "contract state not found".into(),
                            })
                        })?,
                        contract,
                    }
                    .into()),
                    Err(err) => Err(err),
                }
            }
            ContractRequest::Subscribe {
                key: instance_id,
                summary,
            } => {
                tracing::debug!(
                    client = %cli_id,
                    contract = %instance_id,
                    has_summary = summary.is_some(),
                    "subscribing to contract"
                );
                let updates = updates.ok_or_else(|| {
                    ExecutorError::other(anyhow::anyhow!("missing update channel"))
                })?;
                self.register_contract_notifier(instance_id, cli_id, updates, summary)?;

                // Look up the full key for storage operations
                let full_key = self.lookup_key(&instance_id).ok_or_else(|| {
                    tracing::debug!(
                        contract = %instance_id,
                        phase = "key_lookup_failed",
                        "Contract not found during subscribe request"
                    );
                    ExecutorError::request(StdContractError::MissingContract { key: instance_id })
                })?;

                // by default a subscribe op has an implicit get
                let _res = self.perform_contract_get(false, full_key).await?;
                self.subscribe(full_key).await?;
                Ok(ContractResponse::SubscribeResponse {
                    key: full_key,
                    subscribed: true,
                }
                .into())
            }
            other => {
                tracing::warn!(
                    client = %cli_id,
                    request = ?other,
                    "unsupported contract request"
                );
                Err(ExecutorError::other(anyhow::anyhow!("not supported")))
            }
        };

        if let Err(ref e) = result {
            tracing::error!(
                client = %cli_id,
                error = %e,
                phase = "request_failed",
                "Contract request failed"
            );
        }

        result
    }

    pub fn delegate_request(
        &mut self,
        req: DelegateRequest<'_>,
        origin_contract: Option<&ContractInstanceId>,
        caller_delegate: Option<&DelegateKey>,
    ) -> Response {
        // Mutual exclusion invariant: a single inbound delegate request is
        // either dispatched on behalf of a contract-backed web app
        // (`origin_contract = Some`) or on behalf of another delegate
        // (`caller_delegate = Some`), never both. The doc comment on
        // `ContractExecutor::execute_delegate_request` states this. The
        // `debug_assert!` turns the convention into a tripwire so a future
        // call site that violates it fails loudly in debug/test builds; in
        // release builds the precedence below silently picks `caller_delegate`
        // (fail-safe in the direction of "least surprising attestation").
        debug_assert!(
            !(origin_contract.is_some() && caller_delegate.is_some()),
            "execute_delegate_request: at most one of origin_contract and \
             caller_delegate may be Some (got both)"
        );
        tracing::debug!(
            origin_contract = ?origin_contract,
            caller_delegate = ?caller_delegate.map(|k| k.to_string()),
            "received delegate request"
        );
        match req {
            DelegateRequest::RegisterDelegate {
                delegate,
                cipher,
                nonce,
            } => {
                use chacha20poly1305::{KeyInit, XChaCha20Poly1305};
                let key = delegate.key().clone();
                let arr = (&cipher).into();
                let cipher = XChaCha20Poly1305::new(arr);
                let nonce = nonce.into();
                if let Some(contract) = origin_contract {
                    self.delegate_origin_ids
                        .entry(key.clone())
                        .or_default()
                        .push(*contract);
                }
                match self.runtime.register_delegate(delegate, cipher, nonce) {
                    Ok(_) => Ok(DelegateResponse {
                        key,
                        values: Vec::new(),
                    }),
                    Err(err) => {
                        tracing::warn!(
                            delegate_key = %key,
                            error = %err,
                            phase = "register_failed",
                            "Failed to register delegate"
                        );
                        Err(ExecutorError::other(StdDelegateError::RegisterError(key)))
                    }
                }
            }
            DelegateRequest::UnregisterDelegate(key) => {
                self.delegate_origin_ids.remove(&key);

                // Remove delegate from all contract subscription entries
                crate::wasm_runtime::DELEGATE_SUBSCRIPTIONS.retain(|_, subscribers| {
                    subscribers.remove(&key);
                    !subscribers.is_empty()
                });

                // Clean up delegate creation tracking to prevent unbounded growth
                crate::wasm_runtime::DELEGATE_INHERITED_ORIGINS.remove(&key);

                // Decrement the global created-delegates counter so the slot can be reused.
                // Only decrement if count > 0 to avoid underflow for delegates not created
                // via the host function (e.g., registered directly by apps).
                {
                    use std::sync::atomic::Ordering;
                    let count = &crate::wasm_runtime::CREATED_DELEGATES_COUNT;
                    let prev = count.load(Ordering::Relaxed);
                    if prev > 0 {
                        count.fetch_sub(1, Ordering::Relaxed);
                    }
                }

                match self.runtime.unregister_delegate(&key) {
                    Ok(_) => Ok(HostResponse::Ok),
                    Err(err) => {
                        tracing::warn!(
                            delegate_key = %key,
                            error = %err,
                            phase = "unregister_failed",
                            "Failed to unregister delegate"
                        );
                        Ok(HostResponse::Ok)
                    }
                }
            }
            DelegateRequest::ApplicationMessages {
                key,
                inbound,
                params,
            } => {
                let origin = resolve_message_origin(caller_delegate, origin_contract, &key);
                match self.runtime.inbound_app_message(
                    &key,
                    &params,
                    origin.as_ref(),
                    inbound
                        .into_iter()
                        .map(InboundDelegateMsg::into_owned)
                        .collect(),
                ) {
                    Ok(values) => Ok(DelegateResponse { key, values }),
                    Err(err) => {
                        let key_display = key.to_string();
                        let exec_err =
                            ExecutorError::execution(err, Some(InnerOpError::Delegate(key)));
                        // Downgrade "not found" to warn — expected during legacy
                        // migration probes when old delegate WASM isn't on this node
                        if exec_err.is_missing_delegate() {
                            tracing::warn!(
                                delegate_key = %key_display,
                                "Delegate not found in store (expected for migration probes)"
                            );
                        } else {
                            tracing::error!(
                                delegate_key = %key_display,
                                error = %exec_err,
                                phase = "execution_failed",
                                "Failed executing delegate"
                            );
                        }
                        Err(exec_err)
                    }
                }
            }
            _ => Err(ExecutorError::other(anyhow::anyhow!("not supported"))),
        }
    }

    async fn perform_contract_put(
        &mut self,
        contract: ContractContainer,
        state: WrappedState,
        related_contracts: RelatedContracts<'_>,
    ) -> Response {
        let key = contract.key();
        let params = contract.params();

        if self.get_local_contract(key.id()).await.is_ok() {
            // Contract already exists — merge states locally and broadcast async.
            //
            // We intentionally do NOT delegate to perform_contract_update here because
            // its network mode path uses op_request() which blocks waiting for the
            // network operation to complete (120s timeout). For client-initiated puts
            // (e.g. fdev publish), the client needs a timely response. The network
            // broadcast is fire-and-forget — if it fails, subscribers will still get
            // the update via their next sync.
            //
            // NOTE: This simplified path does not handle contracts that require related
            // contracts for update_state or validate_state. If update_state returns
            // MissingRelated (new_state=None with non-empty related), it is treated as
            // "no change". This is acceptable because no current contracts use related
            // contracts (see issue #2870 for completing that mechanism).
            let current_state = self
                .state_store
                .get(&key)
                .await
                .map_err(ExecutorError::other)?
                .clone();

            let update = UpdateData::State(state.into());
            let update_result = self
                .runtime
                .update_state(&key, &params, &current_state, &[update])
                .map_err(|err| ExecutorError::execution(err, Some(InnerOpError::Upsert(key))))?;

            // If update_state produced no new state, or the merged state is identical
            // to current, return early with the current summary (no work to persist).
            let new_state = match update_result.new_state {
                Some(s) if s.as_ref() != current_state.as_ref() => {
                    WrappedState::new(s.into_bytes())
                }
                _ => {
                    let summary = self
                        .runtime
                        .summarize_state(&key, &params, &current_state)
                        .map_err(|e| ExecutorError::execution(e, None))?;
                    return Ok(ContractResponse::UpdateResponse { key, summary }.into());
                }
            };

            // Validate before persisting (fetch related contracts from network if needed)
            let validate_result = self
                .fetch_related_for_validation_network(&key, &params, &new_state, &related_contracts)
                .await?;
            if validate_result != ValidateResult::Valid {
                return Err(Self::validation_error_put(key, validate_result));
            }

            // Commit locally
            self.state_store
                .update(&key, new_state.clone())
                .await
                .map_err(ExecutorError::other)?;

            self.send_update_notification(&key, &params, &new_state)
                .await
                .map_err(|_| {
                    ExecutorError::request(StdContractError::Put {
                        key,
                        cause: "failed while sending notifications".into(),
                    })
                })?;

            self.broadcast_state_change(key, new_state.clone()).await;

            let summary = self
                .runtime
                .summarize_state(&key, &params, &new_state)
                .map_err(|e| ExecutorError::execution(e, None))?;
            return Ok(ContractResponse::UpdateResponse { key, summary }.into());
        }

        self.verify_and_store_contract(state.clone(), contract, related_contracts)
            .await?;

        self.send_update_notification(&key, &params, &state)
            .await
            .map_err(|_| {
                ExecutorError::request(StdContractError::Put {
                    key,
                    cause: "failed while sending notifications".into(),
                })
            })?;

        self.broadcast_state_change(key, state.clone()).await;

        Ok(ContractResponse::PutResponse { key }.into())
    }

    async fn perform_contract_update(
        &mut self,
        key: ContractKey,
        update: UpdateData<'_>,
    ) -> Response {
        let parameters = {
            self.state_store
                .get_params(&key)
                .await
                .map_err(ExecutorError::other)?
                .ok_or_else(|| {
                    RequestError::ContractError(StdContractError::Update {
                        cause: "missing contract parameters".into(),
                        key,
                    })
                })?
        };

        let current_state = self
            .state_store
            .get(&key)
            .await
            .map_err(ExecutorError::other)?
            .clone();

        let updates = vec![update];

        // In local mode, we handle the full update locally (compute, commit, notify)
        if self.mode == OperationMode::Local {
            let new_state = self
                .get_updated_state(&parameters, current_state, key, updates)
                .await?;
            let summary = self
                .runtime
                .summarize_state(&key, &parameters, &new_state)
                .map_err(|e| ExecutorError::execution(e, None))?;
            return Ok(ContractResponse::UpdateResponse { key, summary }.into());
        }

        // In network mode, compute the state WITHOUT committing.
        // The network operation will handle the commit and broadcast.
        // This fixes issue #2301: previously we committed here, causing the network
        // operation to see no change and skip broadcasting.
        //
        // If related contracts are needed, we fetch them and retry compute_state_update
        // in a loop (similar to get_updated_state, but without committing).
        let mut updates = updates;
        let start = Instant::now();
        let new_state = loop {
            let computed = self
                .compute_state_update(&parameters, &current_state, &key, &updates)
                .await?;

            match computed {
                ComputedStateUpdate::NoChange(state) => {
                    // No change detected, return early without starting network operation
                    tracing::debug!(
                        contract = %key,
                        phase = "update_no_change",
                        "Update resulted in no change, skipping network operation"
                    );
                    let summary = self
                        .runtime
                        .summarize_state(&key, &parameters, &state)
                        .map_err(|e| ExecutorError::execution(e, None))?;
                    return Ok(ContractResponse::UpdateResponse { key, summary }.into());
                }
                ComputedStateUpdate::MissingRelated(missing) => {
                    // Fetch missing related contracts WITHOUT committing.
                    // This mirrors the logic in get_updated_state but avoids the commit
                    // that would cause the network operation to see NoChange.
                    tracing::debug!(
                        contract = %key,
                        missing_count = missing.len(),
                        phase = "update_fetching_related",
                        "Fetching missing related contracts"
                    );

                    let required_contracts = missing.len() + 1;
                    for RelatedContract {
                        contract_instance_id: id,
                        mode,
                    } in missing
                    {
                        // Try to look up the full key; if not found, treat as missing
                        let local_state = if let Some(related_key) = self.lookup_key(&id) {
                            self.state_store.get(&related_key).await.ok()
                        } else {
                            None
                        };

                        match local_state {
                            Some(state) => {
                                // Already have this contract locally
                                updates.push(UpdateData::RelatedState {
                                    related_to: id,
                                    state: state.into(),
                                });
                            }
                            None => {
                                // Fetch from network
                                let state =
                                    match self.local_state_or_from_network(&id, false).await? {
                                        Either::Left(state) => state,
                                        Either::Right(GetResult {
                                            state, contract, ..
                                        }) => {
                                            let Some(contract) = contract else {
                                                return Err(ExecutorError::request(
                                                    RequestError::ContractError(
                                                        StdContractError::Get {
                                                            key,
                                                            cause: "Missing contract".into(),
                                                        },
                                                    ),
                                                ));
                                            };
                                            // Store the related contract (this is necessary for future lookups)
                                            // but does NOT commit the main contract's state update
                                            self.verify_and_store_contract(
                                                state.clone(),
                                                contract.clone(),
                                                RelatedContracts::default(),
                                            )
                                            .await?;
                                            state
                                        }
                                    };
                                updates.push(UpdateData::State(state.into()));
                                match mode {
                                    RelatedMode::StateOnce => {}
                                    RelatedMode::StateThenSubscribe => {
                                        // After storing, we should be able to look up the key
                                        if let Some(related_key) = self.lookup_key(&id) {
                                            self.subscribe(related_key).await?;
                                        }
                                    }
                                }
                            }
                        }
                    }

                    // Check if we have enough related contracts to retry
                    if updates.len() + 1 >= required_contracts {
                        // Retry with related contracts
                        continue;
                    } else if start.elapsed() > Duration::from_secs(10) {
                        tracing::error!(
                            contract = %key,
                            elapsed_secs = start.elapsed().as_secs(),
                            phase = "update_timeout",
                            "Timeout fetching related contracts for update"
                        );
                        return Err(ExecutorError::request(RequestError::Timeout));
                    }
                }
                ComputedStateUpdate::Changed(new_state) => {
                    break new_state;
                }
            }
        };

        // State changed - start network operation which will commit and broadcast.
        // We pass the computed new_state so the network operation can detect the change.
        //
        // Notification flow in this path:
        // 1. compute_state_update does NOT send notifications (by design)
        // 2. Network operation calls update_contract -> UpdateQuery -> upsert_contract_state
        // 3. upsert_contract_state validates, then commit_state_update sends the notification
        tracing::debug!(
            contract = %key,
            new_size_bytes = new_state.as_ref().len(),
            phase = "update_starting_network_op",
            "State changed, starting network operation for commit and broadcast"
        );
        let summary = self
            .runtime
            .summarize_state(&key, &parameters, &new_state)
            .map_err(|e| ExecutorError::execution(e, None))?;
        let request = UpdateContract { key, new_state };
        let _op: operations::update::UpdateResult = self.op_request(request).await?;
        Ok(ContractResponse::UpdateResponse { key, summary }.into())
    }

    /// Computes the updated state WITHOUT committing it to storage.
    ///
    /// This is used in network mode to prepare the state for the network operation
    /// which will handle the commit. This separation fixes issue #2301 where
    /// committing before the network operation caused change detection to fail.
    async fn compute_state_update(
        &mut self,
        parameters: &Parameters<'_>,
        current_state: &WrappedState,
        key: &ContractKey,
        updates: &[UpdateData<'_>],
    ) -> Result<ComputedStateUpdate, ExecutorError> {
        let update_modification =
            match self
                .runtime
                .update_state(key, parameters, current_state, updates)
            {
                Ok(result) => result,
                Err(err) => {
                    return Err(ExecutorError::execution(
                        err,
                        Some(InnerOpError::Upsert(*key)),
                    ));
                }
            };

        let UpdateModification {
            new_state, related, ..
        } = update_modification;

        let Some(new_state) = new_state else {
            if related.is_empty() {
                // No updates were made, return current state
                return Ok(ComputedStateUpdate::NoChange(current_state.clone()));
            } else {
                // Missing related contracts
                return Ok(ComputedStateUpdate::MissingRelated(related));
            }
        };

        let new_state = WrappedState::new(new_state.into_bytes());

        // Compare bytes to determine if state actually changed.
        // Note: Even though runtime.update_state returned Some(new_state), we still check bytes
        // because the contract may have "processed" the update but produced identical output
        // (e.g., idempotent merge operations in CRDTs). In such cases, we should NOT broadcast
        // since nothing actually changed from the subscribers' perspective.
        let changed = new_state.as_ref() != current_state.as_ref();

        if changed {
            Ok(ComputedStateUpdate::Changed(new_state))
        } else {
            Ok(ComputedStateUpdate::NoChange(current_state.clone()))
        }
    }

    /// Given a contract and a series of delta updates, it will try to perform an update
    /// to the contract state and return the new state. If it fails to update the state,
    /// it will return an error.
    ///
    /// If there are missing updates for related contracts, it will try to fetch them from the network.
    async fn get_updated_state(
        &mut self,
        parameters: &Parameters<'_>,
        current_state: WrappedState,
        key: ContractKey,
        mut updates: Vec<UpdateData<'_>>,
    ) -> Result<WrappedState, ExecutorError> {
        let new_state = {
            let start = Instant::now();
            loop {
                let state_update_res = self
                    .attempt_state_update(parameters, &current_state, &key, &updates)
                    .await?;
                let missing = match state_update_res {
                    Either::Left(new_state) => {
                        break new_state;
                    }
                    Either::Right(missing) => missing,
                };
                // some required contracts are missing
                let required_contracts = missing.len() + 1;
                for RelatedContract {
                    contract_instance_id: id,
                    mode,
                } in missing
                {
                    // Try to look up the full key; if not found, treat as missing
                    let local_state = if let Some(related_key) = self.lookup_key(&id) {
                        self.state_store.get(&related_key).await.ok()
                    } else {
                        None
                    };

                    match local_state {
                        Some(state) => {
                            // in this case we are already subscribed to and are updating this contract,
                            // we can try first with the existing value
                            updates.push(UpdateData::RelatedState {
                                related_to: id,
                                state: state.into(),
                            });
                        }
                        None => {
                            let state = match self.local_state_or_from_network(&id, false).await? {
                                Either::Left(state) => state,
                                Either::Right(GetResult {
                                    state, contract, ..
                                }) => {
                                    let Some(contract) = contract else {
                                        return Err(ExecutorError::request(
                                            RequestError::ContractError(StdContractError::Get {
                                                key,
                                                cause: "Missing contract".into(),
                                            }),
                                        ));
                                    };
                                    self.verify_and_store_contract(
                                        state.clone(),
                                        contract.clone(),
                                        RelatedContracts::default(),
                                    )
                                    .await?;
                                    state
                                }
                            };
                            updates.push(UpdateData::State(state.into()));
                            match mode {
                                RelatedMode::StateOnce => {}
                                RelatedMode::StateThenSubscribe => {
                                    // After storing, we should be able to look up the key
                                    if let Some(related_key) = self.lookup_key(&id) {
                                        self.subscribe(related_key).await?;
                                    }
                                }
                            }
                        }
                    }
                }
                if updates.len() + 1 /* includes the original contract being updated update */ >= required_contracts
                {
                    // try running again with all the related contracts retrieved
                    continue;
                } else if start.elapsed() > Duration::from_secs(10) {
                    /* make this timeout configurable, and anyway should be controlled globally*/
                    return Err(RequestError::Timeout.into());
                }
            }
        };

        // Validate before persisting or broadcasting (fetch related contracts from network if needed).
        let result = self
            .fetch_related_for_validation_network(
                &key,
                parameters,
                &new_state,
                &RelatedContracts::default(),
            )
            .await?;

        if result != ValidateResult::Valid {
            return Err(Self::validation_error(key, result));
        }
        if new_state.as_ref() != current_state.as_ref() {
            self.commit_state_update(&key, parameters, &new_state)
                .await?;
        }
        Ok(new_state)
    }

    async fn get_local_contract(
        &self,
        id: &ContractInstanceId,
    ) -> Result<State<'static>, Either<Box<RequestError>, anyhow::Error>> {
        let Some(full_key) = self.lookup_key(id) else {
            return Err(Either::Right(
                StdContractError::MissingRelated { key: *id }.into(),
            ));
        };
        let Ok(contract) = self.state_store.get(&full_key).await else {
            return Err(Either::Right(
                StdContractError::MissingRelated { key: *id }.into(),
            ));
        };
        // SAFETY: `contract` is alive for the remainder of this function,
        // and `state` does not escape this scope, so the reborrowed slice
        // remains valid for its entire use.
        let state: &[u8] = unsafe { std::mem::transmute::<&[u8], &'_ [u8]>(contract.as_ref()) };
        Ok(State::from(state))
    }

    /// Verify and store a contract with depth=1 related contract resolution.
    ///
    /// 1. Store the contract code in the runtime store
    /// 2. Validate state (fetching related contracts if requested, one round only)
    /// 3. If valid, persist to state_store
    async fn verify_and_store_contract(
        &mut self,
        state: WrappedState,
        contract: ContractContainer,
        related_contracts: RelatedContracts<'_>,
    ) -> Result<(), ExecutorError> {
        let key = contract.key();
        let params = contract.params();
        let state_hash = blake3::hash(state.as_ref());

        tracing::debug!(
            contract = %key,
            state_size = state.as_ref().len(),
            state_hash = %state_hash,
            params_size = params.as_ref().len(),
            "starting contract verification and storage"
        );

        // Store contract code in runtime store
        self.runtime
            .contract_store
            .store_contract(contract)
            .map_err(|e| {
                tracing::error!(
                    contract = %key,
                    error = %e,
                    "failed to store contract in runtime"
                );
                ExecutorError::other(e)
            })?;

        // Validate with depth=1 related contract resolution.
        //
        // DEPTH PROTECTION: fetch_related_for_validation enforces depth=1 —
        // if validate_state returns RequestRelated(ids), we fetch those contracts
        // and retry exactly once. A second RequestRelated is rejected as an error.
        // This prevents:
        //   - Recursive depth (related contracts requesting their own related contracts)
        //   - Amplification attacks (contract requesting new contracts on every retry)
        //   - Self-reference (contract requesting its own state)
        //   - Excessive fan-out (max 10 related contracts per request)
        // See MAX_RELATED_CONTRACTS_PER_REQUEST and RELATED_FETCH_TIMEOUT constants.
        let result = self
            .fetch_related_for_validation_network(&key, &params, &state, &related_contracts)
            .await
            .inspect_err(|_| {
                if let Err(e) = self.runtime.contract_store.remove_contract(&key) {
                    tracing::warn!(contract = %key, error = %e, "failed to remove contract after validation failure");
                }
            })?;

        // fetch_related_for_validation resolves RequestRelated internally,
        // so only Valid or Invalid are possible here.
        if result != ValidateResult::Valid {
            if let Err(e) = self.runtime.contract_store.remove_contract(&key) {
                tracing::warn!(contract = %key, error = %e, "failed to remove contract after invalid validation");
            }
            return Err(ExecutorError::request(StdContractError::Put {
                key,
                cause: "not valid".into(),
            }));
        }

        tracing::debug!(
            contract = %key,
            state_size = state.as_ref().len(),
            "storing contract state"
        );
        self.state_store
            .store(key, state, params)
            .await
            .map_err(|e| {
                tracing::error!(
                    contract = %key,
                    error = %e,
                    "failed to store contract state"
                );
                ExecutorError::other(e)
            })?;

        Ok(())
    }
}

impl Executor<Runtime> {
    /// Network-aware version of `fetch_related_for_validation`.
    ///
    /// Uses `local_state_or_from_network` to fetch related contracts, allowing
    /// first-time publishes that depend on contracts not yet stored locally.
    /// The base `fetch_related_for_validation` on the bridged impl only does
    /// local lookups.
    async fn fetch_related_for_validation_network(
        &mut self,
        key: &ContractKey,
        params: &Parameters<'_>,
        state: &WrappedState,
        initial_related: &RelatedContracts<'_>,
    ) -> Result<ValidateResult, ExecutorError> {
        let result = self
            .runtime
            .validate_state(key, params, state, initial_related)
            .map_err(|e| ExecutorError::execution(e, None))?;

        let requested_ids = match result {
            ValidateResult::Valid | ValidateResult::Invalid => return Ok(result),
            ValidateResult::RequestRelated(ids) => ids,
        };

        // Apply the same safety checks as the base helper
        if requested_ids.is_empty() {
            return Err(ExecutorError::request(StdContractError::Put {
                key: *key,
                cause: "contract requested related contracts but provided empty list".into(),
            }));
        }
        let self_id = key.id();
        if requested_ids.iter().any(|id| id == self_id) {
            return Err(ExecutorError::request(StdContractError::Put {
                key: *key,
                cause: "contract cannot request itself as a related contract".into(),
            }));
        }
        let unique_ids: HashSet<ContractInstanceId> = requested_ids.into_iter().collect();
        if unique_ids.len() > MAX_RELATED_CONTRACTS_PER_REQUEST {
            return Err(ExecutorError::request(StdContractError::Put {
                key: *key,
                cause: format!(
                    "contract requested {} related contracts, limit is {}",
                    unique_ids.len(),
                    MAX_RELATED_CONTRACTS_PER_REQUEST
                )
                .into(),
            }));
        }

        tracing::debug!(
            contract = %key,
            related_count = unique_ids.len(),
            "Fetching related contracts (with network fallback) for validation"
        );

        let mut related_map: HashMap<ContractInstanceId, Option<State<'static>>> =
            HashMap::with_capacity(unique_ids.len());

        let fetch_all = async {
            for id in &unique_ids {
                match self.local_state_or_from_network(id, false).await? {
                    Either::Left(state) => {
                        related_map.insert(*id, Some(State::from(state.as_ref().to_vec())));
                    }
                    Either::Right(get_result) => {
                        related_map
                            .insert(*id, Some(State::from(get_result.state.as_ref().to_vec())));
                    }
                }
            }
            Ok::<(), ExecutorError>(())
        };

        match tokio::time::timeout(RELATED_FETCH_TIMEOUT, fetch_all).await {
            Ok(Ok(())) => {}
            Ok(Err(e)) => return Err(e),
            Err(_elapsed) => {
                return Err(ExecutorError::request(StdContractError::Put {
                    key: *key,
                    cause: "timed out fetching related contracts".into(),
                }));
            }
        }

        // Merge initial_related with newly fetched states
        let initial_owned = initial_related.clone().into_owned();
        for (id, state) in initial_owned.states() {
            if let Some(s) = state {
                related_map
                    .entry(*id)
                    .or_insert_with(|| Some(s.clone().into_owned()));
            }
        }

        let populated_related = RelatedContracts::from(related_map);
        let retry_result = self
            .runtime
            .validate_state(key, params, state, &populated_related)
            .map_err(|e| ExecutorError::execution(e, None))?;

        if let ValidateResult::RequestRelated(_) = &retry_result {
            return Err(ExecutorError::request(StdContractError::Put {
                key: *key,
                cause: "contract requested additional related contracts after first round (depth=1 limit exceeded)".into(),
            }));
        }

        Ok(retry_result)
    }

    async fn subscribe(&mut self, key: ContractKey) -> Result<(), ExecutorError> {
        if self.mode == OperationMode::Local {
            return Ok(());
        }
        let request = SubscribeContract {
            instance_id: *key.id(),
        };
        let _sub: operations::subscribe::SubscribeResult = self.op_request(request).await?;
        Ok(())
    }

    #[inline]
    async fn local_state_or_from_network(
        &mut self,
        id: &ContractInstanceId,
        return_contract_code: bool,
    ) -> Result<Either<WrappedState, operations::get::GetResult>, ExecutorError> {
        // Try to get locally if we have the full key
        if let Some(full_key) = self.lookup_key(id) {
            if let Ok(state) = self.state_store.get(&full_key).await {
                return Ok(Either::Left(state));
            }
        }
        // Fetch from network
        let request: GetContract = GetContract {
            instance_id: *id,
            return_contract_code,
        };
        let get_result: operations::get::GetResult = self.op_request(request).await?;
        Ok(Either::Right(get_result))
    }
}

/// Resolve a [`MessageOrigin`] for a delegate `ApplicationMessages` request,
/// in priority order:
///
/// 1. `caller_delegate` — set when another delegate dispatched this request
///    via `OutboundDelegateMsg::SendDelegateMessage` (issue #3860). The
///    runtime attests the caller's identity, so the receiver can authorize
///    on it. This wins unconditionally — an inter-delegate message
///    deliberately replaces (not composes with) any inherited WebApp origin.
/// 2. `origin_contract` — set when a contract-backed web app dispatched
///    this request via the WebSocket API.
/// 3. `DELEGATE_INHERITED_ORIGINS[delegate_key]` — set when a parent
///    delegate created this delegate via `create_delegate`, inheriting its
///    WebApp attestation.
///
/// Extracted as a free function so the precedence rules can be unit-tested
/// directly without standing up a full `Executor`.
fn resolve_message_origin(
    caller_delegate: Option<&DelegateKey>,
    origin_contract: Option<&ContractInstanceId>,
    delegate_key: &DelegateKey,
) -> Option<MessageOrigin> {
    if let Some(caller) = caller_delegate {
        Some(MessageOrigin::Delegate(caller.clone()))
    } else if let Some(contract_id) = origin_contract {
        Some(MessageOrigin::WebApp(*contract_id))
    } else {
        crate::wasm_runtime::DELEGATE_INHERITED_ORIGINS
            .get(delegate_key)
            .and_then(|ids| ids.first().map(|c| MessageOrigin::WebApp(*c)))
    }
}

#[cfg(test)]
mod resolve_message_origin_tests {
    use super::*;
    use freenet_stdlib::prelude::CodeHash;

    fn dkey(seed: u8) -> DelegateKey {
        DelegateKey::new([seed; 32], CodeHash::new([seed; 32]))
    }

    /// Caller delegate identity wins over a concurrently-supplied WebApp
    /// contract (regression for issue #3860 precedence rule).
    #[test]
    fn caller_delegate_takes_precedence_over_origin_contract() {
        let caller = dkey(0xA1);
        let recipient = dkey(0xB2);
        let app_contract = ContractInstanceId::new([0xC3; 32]);

        let origin = resolve_message_origin(Some(&caller), Some(&app_contract), &recipient);

        match origin {
            Some(MessageOrigin::Delegate(k)) => assert_eq!(k, caller),
            other => panic!("Expected Delegate(caller), got {other:?}"),
        }
    }

    /// With only `origin_contract` set, the receiver sees `WebApp(..)` —
    /// the historical behavior for web-app-driven dispatch must be
    /// preserved.
    #[test]
    fn origin_contract_alone_yields_webapp() {
        let recipient = dkey(0xB2);
        let app_contract = ContractInstanceId::new([0xC3; 32]);

        let origin = resolve_message_origin(None, Some(&app_contract), &recipient);

        match origin {
            Some(MessageOrigin::WebApp(id)) => assert_eq!(id, app_contract),
            other => panic!("Expected WebApp(app_contract), got {other:?}"),
        }
    }

    /// With neither argument set and no inherited origin in the static
    /// map, the receiver sees `None` (matches pre-#3860 behavior for
    /// orphaned dispatches and the fall-through case for unrelated
    /// recipients in tests).
    #[test]
    fn no_arguments_and_no_inherited_yields_none() {
        // Pick a recipient key with no entry in DELEGATE_INHERITED_ORIGINS.
        // Using a randomized seed avoids collision with anything another
        // test populated in the same process.
        let recipient = dkey(0xEE);
        crate::wasm_runtime::DELEGATE_INHERITED_ORIGINS.remove(&recipient);

        let origin = resolve_message_origin(None, None, &recipient);
        assert!(origin.is_none(), "Expected None, got {origin:?}");
    }

    /// Caller delegate identity also wins over an inherited WebApp origin
    /// in `DELEGATE_INHERITED_ORIGINS`. This documents the deliberate
    /// "inter-delegate calls revoke inherited contract access" semantics
    /// from the `MessageOrigin::Delegate` rustdoc.
    #[test]
    fn caller_delegate_overrides_inherited_origin() {
        let caller = dkey(0xA1);
        let recipient = dkey(0xB3);
        let inherited_contract = ContractInstanceId::new([0xDD; 32]);

        // Plant an inherited WebApp origin for the recipient so the
        // fallback branch would have something to return.
        crate::wasm_runtime::DELEGATE_INHERITED_ORIGINS
            .entry(recipient.clone())
            .or_default()
            .push(inherited_contract);

        let origin = resolve_message_origin(Some(&caller), None, &recipient);

        // Cleanup before assertions so a panic doesn't leak state into
        // sibling tests sharing the same process.
        crate::wasm_runtime::DELEGATE_INHERITED_ORIGINS.remove(&recipient);

        match origin {
            Some(MessageOrigin::Delegate(k)) => assert_eq!(k, caller),
            other => panic!("Expected Delegate(caller), got {other:?}"),
        }
    }
}
