use super::*;
use super::{
    ContractExecutor, ContractRequest, ContractResponse, ExecutorError, InitCheckResult,
    RequestError, Response, SLOW_INIT_THRESHOLD, STALE_INIT_THRESHOLD, StateStoreError, now_nanos,
};

/// Maximum number of related contracts a single validation can request.
/// Bounds worst-case first-time cost: N GETs of up to 50MB each.
const MAX_RELATED_CONTRACTS_PER_REQUEST: usize = 10;

/// Timeout for fetching all related contracts during validation.
const RELATED_FETCH_TIMEOUT: Duration = Duration::from_secs(10);

/// Probability that a given state-changing merge is checked for
/// `update_state` idempotency. One re-invocation of WASM per sample, so
/// the per-merge cost is ~`p * average_update_state_us`. At 1/32 ≈ 3%
/// the overhead is negligible on healthy contracts and detection is
/// effectively certain within a few seconds on a contract that's
/// firing dozens of merges/sec.
///
/// Sample selection uses `GlobalRng` (deterministic under a fixed seed
/// for simulation tests). See `Executor::maybe_probe_idempotency`.
const IDEMPOTENCY_PROBE_PROBABILITY: f64 = 1.0 / 32.0;

/// Returns true if `a` and `b` contain the same multiset of bytes — i.e. one
/// is a reordering of the other — and false if their byte content differs.
///
/// This is the discriminator the idempotency probe uses to tell benign
/// serialization nondeterminism apart from a genuine non-idempotent merge:
///
/// - **Benign flutter** (the #4295 false-positive case): a correct contract
///   with non-canonical serialization (`HashMap`/`HashSet` iteration order)
///   re-serializes the SAME logical state in a different byte ORDER. Reordering
///   permutes the serialized bytes but preserves their multiset, so this
///   returns `true` → not flagged.
/// - **Genuine non-idempotency** (the #4251/#4279 case the gate must catch):
///   re-applying the update changes the state's CONTENT — a counter that churns
///   in place, an embedded timestamp/signature regenerated each merge, an
///   added/removed entry. Any content change alters the byte multiset (e.g. a
///   464→465 counter flips digit bytes), so this returns `false` → flagged.
///   Crucially this catches the *fixed-size* byte-different violator (the real
///   #4251 incident was a constant-size ~464-byte state) that a size-only
///   check would miss.
///
/// Residual false-negative: a content change that coincidentally preserves the
/// exact byte multiset (e.g. swapping two equal bytes) evades detection. This
/// is far narrower than the size-only heuristic's blind spot and far safer than
/// byte-equality's false positives (which permanently suppressed propagation in
/// #4295). O(n) in the state size; only runs on the sampled, byte-different
/// probe path.
fn byte_multiset_eq(a: &[u8], b: &[u8]) -> bool {
    if a.len() != b.len() {
        return false;
    }
    let mut hist = [0i64; 256];
    for &x in a {
        hist[x as usize] += 1;
    }
    for &x in b {
        hist[x as usize] -= 1;
    }
    hist.iter().all(|&c| c == 0)
}

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
    /// Shared per-delegate `ctx.write()` cache (see `DelegateContextCache`).
    shared_delegate_contexts: crate::wasm_runtime::DelegateContextCache,
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
    /// `lookup_key`, `get_subscription_info`, `remove_client`
    /// (read-only / no executor checkout).
    in_flight_contracts: HashMap<ContractKey, usize>,
}

impl RuntimePool {
    /// Create a new pool with the specified number of executors.
    ///
    /// # Arguments
    /// * `config` - Configuration for executors
    /// * `op_manager` - Reference to the operation manager
    /// * `pool_size` - Number of executors to create (typically CPU count)
    pub async fn new(
        config: Arc<Config>,
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
        // Shared delegate-context cache so a prompt round-trip routed to a
        // different pool executor still finds its `ctx.write()` blob.
        let shared_delegate_contexts = crate::wasm_runtime::new_delegate_context_cache();

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
            Some(op_manager.clone()),
            shared_contract_modules.clone(),
            shared_delegate_modules.clone(),
            shared_delegate_contexts.clone(),
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
                Some(op_manager.clone()),
                shared_contract_modules.clone(),
                shared_delegate_modules.clone(),
                shared_delegate_contexts.clone(),
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
            shared_delegate_contexts,
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
            Some(self.op_manager.clone()),
            self.shared_contract_modules.clone(),
            self.shared_delegate_modules.clone(),
            self.shared_delegate_contexts.clone(),
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
    /// Forward the pending-reclamation registration to the ring's retry
    /// queue. See `Ring::pending_reclamation_add` and the
    /// `pending_reclamation` field docs on `HostingManager`. Called by
    /// the `contract_handling` event loop when a fair-queue rejection
    /// drops an `EvictContract` event before it can complete.
    fn track_pending_reclamation(&self, key: ContractKey, expected_generation: u64) {
        self.op_manager
            .ring
            .pending_reclamation_add(key, expected_generation);
    }

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

    async fn remove_contract(
        &mut self,
        key: &ContractKey,
        expected_generation: u64,
    ) -> Result<(), ExecutorError> {
        // Re-check at deletion time. `EvictContract` is fire-and-forget and
        // fair-queued, so an arbitrary amount of time can pass between the
        // hosting cache evicting the contract and this event being processed.
        // In that window a GET/PUT can re-store the contract (re-hosting it) or
        // a subscription can re-register interest. Reclaiming the on-disk
        // storage then would either leave the contract in-cache-but-not-on-disk
        // or delete the state of a contract that is once again wanted. Bailing
        // out here closes that re-host / re-subscribe TOCTOU window — the
        // hosting cache will issue a fresh `EvictContract` if the contract is
        // genuinely evicted again later.
        //
        // Three guards, in order:
        // 1. `is_hosting_contract` — the hosting cache itself re-added the
        //    contract (a GET refreshed it).
        // 2. `contract_in_use` — client / downstream / network subscription
        //    re-registered interest in this contract.
        // 3. `state_generation != expected_generation` — a state write
        //    occurred between eviction and now. This is the load-bearing
        //    check: `EvictContract{X}` and `PutQuery{X}` are serialized in
        //    the per-key fair queue, but the driver-side `host_contract(X)`
        //    that re-marks X as hosted runs on the driver task AFTER
        //    `PutQuery{X}.await` returns — so the `is_hosting_contract`
        //    re-check above can still see a freshly-PUT contract as "not
        //    hosting" and delete its state. The state-write generation
        //    is bumped under the executor in the contract-handler call
        //    path (i.e. before the PUT response returns), so a write that
        //    raced ahead of this handler will have already advanced it
        //    past `expected_generation`.
        if self.op_manager.ring.is_hosting_contract(key) {
            // The hosting cache itself re-added the contract — a later
            // genuine eviction will emit a fresh `EvictContract`, so we
            // do NOT need a pending-reclamation entry. Adding one here
            // would race the cache and risk a spurious retry against a
            // contract that is still hosted.
            //
            // If a pending entry already exists (e.g. an earlier
            // `contract_in_use` skip queued the key, then a later write
            // re-hosted it via the cache), clear it: the cache is now
            // responsible for emitting a fresh `EvictContract` if and
            // when the contract is evicted again. Leaving the stale
            // pending entry behind would let the sweep keep emitting
            // `EvictContract` events that hit `is_hosting_contract` and
            // bail without progress.
            self.op_manager.ring.pending_reclamation_remove(key);
            tracing::debug!(
                contract = %key,
                "Skipping eviction reclamation — contract was re-hosted \
                 (hosting cache contains it) since it was evicted"
            );
            return Ok(());
        }
        if self.op_manager.ring.contract_in_use(key) {
            // A subscriber (client or downstream peer) appeared in the
            // window between eviction and this handler running. The
            // hosting-cache entry is already gone, so when that
            // subscriber later expires/disconnects no cache entry will
            // remain to emit another `EvictContract`. Stash the key in
            // the pending-reclamation queue so the periodic sweep
            // retries once `contract_in_use` becomes false.
            //
            // Disk-leak edge case #2 in PR #4212 review round 7 — see
            // `HostingManager::pending_reclamation` docs.
            self.op_manager
                .ring
                .pending_reclamation_add(*key, expected_generation);
            tracing::debug!(
                contract = %key,
                "Skipping eviction reclamation — contract is in use \
                 (client subscription or downstream subscriber); queued \
                 for retry by the periodic sweep"
            );
            return Ok(());
        }
        let current_generation = self.op_manager.ring.state_generation(key);
        if current_generation != expected_generation {
            // A state write (PUT/UPDATE) occurred between eviction and
            // now. Two sub-cases matter for whether we keep the
            // pending-reclamation entry:
            //
            // (a) The contract IS in the hosting cache: PUT's write
            //     path re-hosts via `host_contract`, so the cache
            //     itself now owns subsequent eviction. A later genuine
            //     eviction will emit a fresh `EvictContract` with the
            //     up-to-date generation. Clear the pending entry —
            //     leaving it would let the sweep keep emitting
            //     `EvictContract` events that all bail at the
            //     `is_hosting_contract` check above.
            //
            // (b) The contract is NOT in the hosting cache: UPDATE
            //     bumps `state_generation` without calling
            //     `host_contract`, so a subscriber-only contract that
            //     was evicted and then UPDATEd reaches this branch
            //     with an advanced generation but no cache entry.
            //     Clearing pending here would permanently leak the
            //     on-disk storage once the subscriber later expires
            //     (there is no cache entry left to emit another
            //     `EvictContract`). Upsert the pending entry with the
            //     current generation so the periodic sweep retries
            //     with a fresh `EvictContract{key, current_generation}`;
            //     if no further writes happen the next pass will reach
            //     the reclaim step, and if more writes happen this
            //     upsert repeats until the generation stabilises.
            //     See PR #4212 review round 8.
            if self.op_manager.ring.is_hosting_contract(key) {
                self.op_manager.ring.pending_reclamation_remove(key);
            } else {
                self.op_manager
                    .ring
                    .pending_reclamation_add(*key, current_generation);
            }
            tracing::debug!(
                contract = %key,
                expected_generation,
                current_generation,
                "Skipping eviction reclamation — contract was written since eviction"
            );
            return Ok(());
        }

        // Mirror the pop-an-executor pattern from `fetch_contract`: the
        // checked-out executor owns the `ContractStore` whose on-disk `.wasm`
        // blob must be removed, while the `StateStore` is shared across the
        // pool. Delegating to `Executor::reclaim_contract_storage` keeps the
        // best-effort, idempotent reclaim logic in one place.
        self.track_contract_checkout(key);
        let mut executor = self.pop_executor().await;
        let result = executor.reclaim_contract_storage(key).await;
        self.return_checked(executor, "remove_contract").await;
        self.track_contract_return(key);

        // Translate the `ReclaimOutcome` into pending-reclamation management:
        //   - Full   → forget state_generation + clear pending (existing behavior).
        //   - Partial → keep pending and keep state_generation; the next sweep
        //               retries the half that failed. Avoids leaking the
        //               unreclaimed half forever when a transient DB/FS error
        //               struck only one of the two delete steps. See PR #4212
        //               review round 8.
        //   - Err    → both halves failed; log and keep pending for retry.
        match &result {
            Ok(ReclaimOutcome::Full) => {
                self.op_manager.ring.forget_state_generation(key);
                // A successful retry from the pending-reclamation queue
                // must clear the queue entry too. No-op when the key was
                // not previously pending (the common case — most evictions
                // succeed on the first attempt).
                self.op_manager.ring.pending_reclamation_remove(key);
            }
            Ok(ReclaimOutcome::Partial) => {
                // Upsert pending-reclamation so the periodic sweep retries
                // the unreclaimed half. On the first EvictContract attempt
                // there is NO prior pending entry, so "retaining" alone
                // would leave the failed half permanently leaked — we must
                // affirmatively insert. The current state_generation is
                // captured so the deletion-time guard matches on retry
                // unless new writes have happened in the meantime.
                let current_gen = self.op_manager.ring.state_generation(key);
                self.op_manager
                    .ring
                    .pending_reclamation_add(*key, current_gen);
                tracing::debug!(
                    contract = %key,
                    "partial reclaim — queued for retry via pending_reclamation"
                );
            }
            Err(_) => {
                // Both halves failed. Same logic as Partial: on a first
                // attempt there is no prior pending entry, so we must
                // affirmatively insert one so the periodic sweep retries
                // both halves. (The error itself is logged inside
                // `reclaim_contract_storage`.)
                let current_gen = self.op_manager.ring.state_generation(key);
                self.op_manager
                    .ring
                    .pending_reclamation_add(*key, current_gen);
            }
        }
        // Drop the outcome detail at the trait boundary: callers expect
        // `Result<(), ExecutorError>` and cannot act on Full vs Partial.
        result.map(|_| ())
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

// ============================================================================
// Bridged methods - shared production logic for Runtime and MockWasmRuntime
// ============================================================================

#[allow(private_bounds)]
impl<R, S> Executor<R, S>
where
    R: crate::wasm_runtime::ContractRuntimeBridge + Send + Sync,
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
                // Use the typed `ContractQueueFull` marker so the same
                // amplification suppression and DEBUG log severity that
                // the per-contract fair queue gets via
                // `send_queue_full_response` also applies here. Without
                // the typed marker, an init-queue-full looks like a
                // generic StdContractError::Update to callers and
                // re-enters the `try_auto_fetch_contract` /
                // `ResyncRequest` paths. Issue #4251.
                return Err(ExecutorError::other(super::ContractQueueFull));
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
                            let written_bytes = state_to_store.as_ref().len();
                            self.state_store
                                .store(key, state_to_store, params.clone())
                                .await
                                .map_err(ExecutorError::other)?;
                            // State-write chokepoint: delegate the three
                            // mandatory side effects (bump generation,
                            // refresh hosting-cache snapshot, report
                            // StateBytesWritten) to `Ring::commit_state_write`.
                            // See its rustdoc and `RuntimePool::remove_contract`
                            // for the EvictContract re-host race this closes.
                            if let Some(op_manager) = &self.op_manager {
                                op_manager.ring.commit_state_write(&key, written_bytes);
                            }

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

                            // Notify locally-subscribed WS clients of the
                            // new state. Without this, the very first state
                            // install for a contract on this node never
                            // reaches `register_contract_notifier` consumers
                            // — only the merge path at the end of this
                            // function calls `commit_state_update`, which
                            // is the only other site that fans out to the
                            // local notifier map. ResyncResponse-driven
                            // applies hit this branch when the state_store
                            // entry is missing, so subscribers would miss
                            // every cross-node delivery that recovers via
                            // resync.
                            tracing::info!(
                                contract = %key,
                                new_size_bytes = incoming_state.as_ref().len(),
                                phase = "update_complete",
                                event = "initial_state_installed",
                                "Contract initial state installed"
                            );
                            // Dashboard "last updated" telemetry; no-op if
                            // we're not subscribed to this contract.
                            if let Some(op_manager) = &self.op_manager {
                                op_manager.ring.record_contract_update(&key);
                            }
                            if let Err(err) = self
                                .send_update_notification(&key, &params, &incoming_state)
                                .await
                            {
                                tracing::error!(
                                    contract = %key,
                                    error = %err,
                                    phase = "notification_failed",
                                    "Failed to send initial-state notification"
                                );
                            }

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

        // Short-circuit: if the incoming state is byte-identical to the stored
        // state, there is nothing to merge and no WASM call is needed.  This
        // is the dominant case for idempotent re-broadcasts (a peer re-pushes
        // the state it already received) and avoids the spurious
        // `merge_rejected_valid_local` INFO log that was firing every time the
        // dedup cache missed an already-current state.  See issue #4151.
        if let Some(ref full_incoming) = incoming_full_state {
            if full_incoming.as_ref() == current_state.as_ref() {
                tracing::debug!(
                    contract = %key,
                    state_size = current_state.size(),
                    event = "merge_skipped_identical",
                    "Incoming state is byte-identical to stored state — skipping WASM update_state"
                );
                return Ok(UpsertResult::NoChange);
            }
        }

        let mut recovery_performed = false;
        let updated_state = match self
            .attempt_state_update(&params, &current_state, &key, &updates)
            .await
        {
            Ok(Either::Left(s)) => s,
            Ok(Either::Right(missing_related)) => {
                // Contract's `update_state` returned `UpdateModification::requires(...)`,
                // listing related contracts it needs to apply the delta. Try to
                // fetch each one (local first, then network when op_manager is
                // wired) and re-attempt the update with the fetched states
                // surfaced as `UpdateData::RelatedState` entries — the same
                // pattern the validate-side `fetch_related_for_validation`
                // helper uses. Without this, every cross-node UPDATE that
                // references a related contract not yet cached locally would
                // fail with `MissingRelated` even though we could resolve it.
                //
                // Apply the same abuse-prevention guards the validate-side
                // path enforces (see `contracts.md` Abuse prevention):
                //   * Reject empty list (a contract MUST signal Valid via
                //     a populated state, not an empty `requires`).
                //   * Reject self-reference (no contract may ask for its
                //     own state through this path).
                //   * Cap at MAX_RELATED_CONTRACTS_PER_REQUEST so a
                //     misbehaving contract can't fan out 50 network GETs.
                //   * Dedup IDs so repeated declarations don't multiply
                //     the fetch count.
                if missing_related.is_empty() {
                    tracing::warn!(
                        contract = %key,
                        "update_state returned requires() with empty list"
                    );
                    return Err(ExecutorError::request(StdContractError::Update {
                        key,
                        cause: "contract requested related contracts but provided empty list"
                            .into(),
                    }));
                }
                let self_id = key.id();
                if missing_related
                    .iter()
                    .any(|c| &c.contract_instance_id == self_id)
                {
                    tracing::warn!(
                        contract = %key,
                        "update_state requires() included self-reference"
                    );
                    return Err(ExecutorError::request(StdContractError::Update {
                        key,
                        cause: "contract cannot request itself as a related contract".into(),
                    }));
                }
                let unique_ids: HashSet<ContractInstanceId> = missing_related
                    .iter()
                    .map(|c| c.contract_instance_id)
                    .collect();
                if unique_ids.len() > MAX_RELATED_CONTRACTS_PER_REQUEST {
                    tracing::warn!(
                        contract = %key,
                        requested = unique_ids.len(),
                        limit = MAX_RELATED_CONTRACTS_PER_REQUEST,
                        "update_state requires() exceeded MAX_RELATED_CONTRACTS_PER_REQUEST"
                    );
                    return Err(ExecutorError::request(StdContractError::Update {
                        key,
                        cause: format!(
                            "contract requested {} related contracts, limit is {}",
                            unique_ids.len(),
                            MAX_RELATED_CONTRACTS_PER_REQUEST
                        )
                        .into(),
                    }));
                }

                // Parallel fetch: each related contract goes through its
                // own GET sub-op concurrently. Previously this loop ran
                // serially under a single 10s wall-clock budget, so a
                // contract requesting N>1 related ids could time out at
                // ~10s/N effective per fetch. Fan-out via `join_all`
                // turns the budget back into 10s _per id_ in the common
                // case (network bandwidth, not CPU, is the constraint).
                // See freenet/freenet-core#4077.
                let mut fetched_updates = updates.clone();
                let fetch_results: Vec<(
                    ContractInstanceId,
                    Result<State<'static>, ExecutorError>,
                )> = {
                    // Reborrow as `&Self` so the per-id futures all share
                    // an immutable borrow; this releases the outer
                    // `&mut self` only for the duration of `fetch_all`,
                    // which is fully awaited before the next `&mut self`
                    // call (`attempt_state_update` below).
                    let this: &Self = &*self;
                    futures::future::join_all(unique_ids.iter().map(|id| {
                        let id = *id;
                        async move {
                            if let Some(full_key) = this.bridged_lookup_key(&id) {
                                if let Ok(state) = this.state_store.get(&full_key).await {
                                    return (id, Ok(State::from(state.as_ref().to_vec())));
                                }
                            }
                            let outcome = fetch_related_via_network(this.op_manager.as_ref(), &id)
                                .await
                                .map(|state| State::from(state.as_ref().to_vec()));
                            (id, outcome)
                        }
                    }))
                    .await
                };
                let mut failed_id: Option<ContractInstanceId> = None;
                for (id, res) in fetch_results {
                    match res {
                        Ok(state) => fetched_updates.push(UpdateData::RelatedState {
                            related_to: id,
                            state,
                        }),
                        Err(err) => {
                            tracing::warn!(
                                contract = %key,
                                related_id = %id,
                                error = %err,
                                "Failed to fetch related contract for update_state requires()"
                            );
                            failed_id.get_or_insert(id);
                        }
                    }
                }
                if let Some(id) = failed_id {
                    return Err(ExecutorError::request(StdContractError::MissingRelated {
                        key: id,
                    }));
                }
                match self
                    .attempt_state_update(&params, &current_state, &key, &fetched_updates)
                    .await
                {
                    Ok(Either::Left(s)) => s,
                    Ok(Either::Right(mut r)) => {
                        // Contract still demanding more after one round → reject
                        // (depth limit, matching the validate-side behavior).
                        let Some(c) = r.pop() else {
                            return Err(ExecutorError::internal_error());
                        };
                        tracing::warn!(
                            contract = %key,
                            related_id = %c.contract_instance_id,
                            "update_state still requires() after first fetch round (depth>1 not supported)"
                        );
                        return Err(ExecutorError::request(StdContractError::MissingRelated {
                            key: c.contract_instance_id,
                        }));
                    }
                    Err(retry_err) => return Err(retry_err),
                }
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
                    // Downgrade to DEBUG for idempotent re-pushes where the contract's
                    // merge function correctly rejected the incoming state because its
                    // version is not newer (e.g. "New state version X must be higher
                    // than current version X"). These fire on every re-broadcast that
                    // misses the dedup cache and are not operator-actionable. Any other
                    // merge failure (OOG, WASM trap, etc.) keeps the INFO level because
                    // it may indicate a real problem. See issue #4151.
                    if merge_err.is_invalid_update_rejection() {
                        tracing::debug!(
                            contract = %key,
                            error = %merge_err,
                            local_state_size = current_state.size(),
                            incoming_state_size = valid_incoming.size(),
                            event = "merge_rejected_valid_local",
                            "Merge rejected incoming state (idempotent re-push, \
                             incoming version not newer) - not replacing"
                        );
                    } else {
                        tracing::info!(
                            contract = %key,
                            error = %merge_err,
                            local_state_size = current_state.size(),
                            incoming_state_size = valid_incoming.size(),
                            event = "merge_rejected_valid_local",
                            "Merge rejected incoming state but local state is valid - \
                             not replacing (incoming state may be stale)"
                        );
                    }
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
            // CRDT-invariant idempotency probe. With low probability, re-run
            // `update_state` with `updated_state` as the current state and
            // the same `updates`. A correct CRDT must satisfy
            // `update_state(update_state(S, U), U) == update_state(S, U)` —
            // a violation indicates a contract bug (timestamp/RNG/position-
            // dependent signing/etc.) that produces an infinite broadcast
            // storm in the network. See `crate::ring::broken_invariants`.
            //
            // `RelatedState` inputs are skipped: re-applying a cross-
            // contract state hint isn't required to be a no-op by the
            // contract ABI, so probing it can produce false positives.
            self.maybe_probe_idempotency(&key, &params, &updated_state, &updates)
                .await;

            if self
                .op_manager
                .as_ref()
                .map(|m| m.ring.is_contract_broken(&key))
                .unwrap_or(false)
            {
                // Probe just flagged this contract (or it was already
                // flagged). Skip the commit so we don't extend the
                // problematic state, and surface the suppression to
                // callers via NoChange — there is no state change the
                // network should observe.
                tracing::debug!(
                    contract = %key,
                    event = "merge_suppressed_broken_contract",
                    "Skipping commit_state_update for contract flagged as broken"
                );
                return Ok(UpsertResult::NoChange);
            }

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

    /// Probe-sampled idempotency check. Re-runs `update_state` with the just-
    /// produced state as the current state and the same updates; flags the
    /// contract as broken if the re-applied state's byte MULTISET differs from
    /// the original (a genuine content change), but NOT if it is merely a
    /// reordering of the same bytes (benign serialization nondeterminism such
    /// as `HashMap` key order — the #4295 false-positive case). See
    /// [`byte_multiset_eq`].
    ///
    /// Costs one extra WASM invocation per sampled merge. With the
    /// configured probability [`IDEMPOTENCY_PROBE_PROBABILITY`] (currently
    /// 1/32) this is a few percent overhead on active contracts and
    /// effectively zero on quiet ones. Sample selection uses `GlobalRng`
    /// so simulation builds remain deterministic under a fixed seed.
    ///
    /// Only probes update batches whose every entry is
    /// `UpdateData::State(_)`. `Delta`/`StateAndDelta` are exempted
    /// because operation-based CRDTs (counters, append-logs) legitimately
    /// violate the byte-equality property on re-apply; `RelatedState` is
    /// exempted because it's a cross-contract hint, not a CRDT op over
    /// this contract's state. Probe traps (timeout, OOG, panic) are
    /// logged but not treated as positive signals — distinguishing
    /// "buggy on re-apply" from "buggy in some other way" requires a
    /// separate detector. See `crate::ring::broken_invariants`.
    async fn maybe_probe_idempotency(
        &mut self,
        key: &ContractKey,
        parameters: &Parameters<'_>,
        post_merge_state: &WrappedState,
        updates: &[UpdateData<'_>],
    ) {
        // Cheap precheck: if already flagged, no value in probing again.
        if let Some(op_manager) = &self.op_manager {
            if op_manager.ring.is_contract_broken(key) {
                return;
            }
        }

        // Probe ONLY when every input is `UpdateData::State(...)`. Three
        // reasons for this conservative gating:
        //
        // 1. `Delta` inputs are NOT contractually required to be
        //    idempotent. An "increment by 1" delta produces S+1 then S+2
        //    on re-apply — that's a CmRDT-shaped contract, perfectly
        //    valid, but `update_state(update_state(S, U), U) != update_state(S, U)`
        //    in the exact byte sense the probe checks. Probing Delta
        //    inputs would mass-flag legitimate counter / append-log
        //    contracts. Skip them entirely.
        //
        // 2. `StateAndDelta` carries a delta too, with the same risk.
        //
        // 3. `RelatedState` is a cross-contract hint, not a CRDT op
        //    over this contract's state — re-applying isn't required
        //    to be a no-op even for a correct contract.
        //
        // State-only batches are the unambiguous case: receiving the
        // same full state twice MUST produce the same merged result by
        // CvRDT lattice-join semantics. That's the invariant the probe
        // tests.
        let all_state =
            !updates.is_empty() && updates.iter().all(|u| matches!(u, UpdateData::State(_)));
        if !all_state {
            return;
        }

        // Sampling. `GlobalRng` honors a fixed seed in simulation tests
        // (see `crate::config::GlobalRng`), so determinism is preserved
        // under the existing test harness — but the call ordering would
        // shift if existing tests don't expect a new RNG consumer on the
        // merge path. The probe runs only on pure-State batches (above),
        // which the simulation harness drives explicitly and rarely, so
        // the RNG stream perturbation is bounded.
        if !crate::config::GlobalRng::random_bool(IDEMPOTENCY_PROBE_PROBABILITY) {
            return;
        }

        let probe_result = self
            .runtime
            .update_state(key, parameters, post_merge_state, updates);
        let probe_outcome = match probe_result {
            Ok(modification) => modification,
            Err(err) => {
                // The probe failing (timeout, trap, etc.) is not a positive
                // signal — the contract is exercising some other failure
                // mode that doesn't necessarily imply non-idempotency.
                // Log at DEBUG and bail without flagging.
                tracing::debug!(
                    contract = %key,
                    error = %err,
                    event = "idempotency_probe_error",
                    "Idempotency probe failed to execute; skipping detection"
                );
                return;
            }
        };
        let UpdateModification {
            new_state: probe_state,
            ..
        } = probe_outcome;
        let Some(probe_state) = probe_state else {
            // No state output from probe (e.g. contract returned only
            // `requires(...)`). Inconclusive — bail.
            return;
        };
        let probe_state = WrappedState::new(probe_state.into_bytes());

        // Byte-identical re-application: definitively idempotent. Fast path
        // for contracts with canonical serialization.
        if probe_state.as_ref() == post_merge_state.as_ref() {
            return;
        }

        // Bytes differ — but byte inequality alone does NOT prove a CRDT
        // violation. A correct, logically-idempotent merge can still emit
        // byte-different output for the SAME logical state when the contract's
        // serialization is non-canonical (HashMap/HashSet iteration order):
        // the re-serialized state is a REORDERING of the same bytes. Flagging
        // on that byte flutter false-positives correct contracts and — because
        // the broken-invariant flag gates ALL propagation — silently bricks
        // them. That was the root cause of #4295: the ping contract's
        // `HashMap`-backed state re-serialized in non-deterministic key order.
        //
        // Distinguish a benign reordering from a genuine content change by
        // comparing the byte MULTISET (not the bytes, not just the size).
        // Reordering preserves the multiset; a real non-idempotent merge
        // changes content (a counter, timestamp, signature, added/removed
        // entry), which changes the multiset — INCLUDING the fixed-size
        // byte-churn shape of the #4251/#4279 production violator that a
        // size-only check would miss.
        if byte_multiset_eq(post_merge_state.as_ref(), probe_state.as_ref()) {
            tracing::debug!(
                contract = %key,
                size = post_merge_state.size(),
                event = "idempotency_probe_byte_flutter_ignored",
                "Idempotency probe saw byte-different but same-multiset re-application \
                 (serialization reordering); treating as benign, not a violation"
            );
            return;
        }

        if let Some(op_manager) = &self.op_manager {
            tracing::warn!(
                contract = %key,
                post_merge_size = post_merge_state.size(),
                probe_size = probe_state.size(),
                event = "non_idempotent_merge_detected",
                "Contract violates update_state idempotency: re-application changes \
                 state content (different byte multiset, not a reordering). \
                 Flagging contract; outbound BroadcastStateChange will be suppressed."
            );
            op_manager
                .ring
                .record_broken_invariant(*key, crate::ring::BrokenInvariant::NonIdempotent);
        }
    }

    /// Persist an updated contract state via `state_store.update`.
    ///
    /// This is the canonical chokepoint for UPDATE-shaped writes: every
    /// in-place state update funnels through here. Bumping the per-contract
    /// state-write generation immediately after the store succeeds is what
    /// closes the EvictContract re-host race for UPDATE — see
    /// `RuntimePool::remove_contract`.
    async fn commit_state_update(
        &mut self,
        key: &ContractKey,
        parameters: &Parameters<'_>,
        new_state: &WrappedState,
    ) -> Result<(), ExecutorError> {
        // Blanket gate: a contract flagged as violating a CRDT invariant
        // (e.g. non-idempotent merge) must not have its state extended
        // OR broadcast from this node. The merge in
        // `bridged_upsert_contract_state` already short-circuits before
        // reaching here on the probe-positive path, but
        // `commit_state_update` has other call sites (related-contract
        // retry, validation re-attempt) that don't run the probe first.
        // Gating here covers all of them with one check. See
        // `crate::ring::broken_invariants` for the tracker and #4279
        // for the storm shape this defends against.
        if let Some(op_manager) = &self.op_manager {
            if op_manager.ring.is_contract_broken(key) {
                tracing::debug!(
                    contract = %key,
                    event = "commit_suppressed_broken_contract",
                    "Skipping commit_state_update for contract flagged as broken"
                );
                return Ok(());
            }
        }

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
        // State-write chokepoint (UPDATE): delegate the bump + refresh +
        // report side effects to `Ring::commit_state_write`. See its
        // rustdoc and `RuntimePool::remove_contract` for the EvictContract
        // re-host race this closes; the report leg feeds the governance
        // scoring layer (`docs/design/contract-hardening.md` — Phase 3).
        if let Some(op_manager) = &self.op_manager {
            op_manager.ring.commit_state_write(key, state_size);
        }

        tracing::debug!(
            contract = %key,
            new_size_bytes = new_state.as_ref().len(),
            phase = "update_complete",
            "Contract state updated"
        );

        // Record update timestamp for dashboard display. No-op if we're
        // not subscribed (e.g., a relay forwarding an UPDATE for a
        // contract this peer doesn't track).
        if let Some(op_manager) = &self.op_manager {
            op_manager.ring.record_contract_update(key);
        }

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
            // Skip the broadcast entirely if this contract has been flagged
            // as violating a CRDT invariant (e.g. non-idempotent
            // `update_state`). The idempotency probe in
            // `bridged_upsert_contract_state` sets this flag when it
            // catches `update_state(update_state(S, U), U) != update_state(S, U)`.
            // Once flagged, propagating this contract's state changes
            // re-engages the broadcast storm we are trying to suppress.
            // See `crate::ring::broken_invariants`.
            if op_manager.ring.is_contract_broken(key) {
                tracing::debug!(
                    contract = %key,
                    event = "broadcast_suppressed_broken_contract",
                    "Skipping BroadcastStateChange for contract flagged as broken"
                );
            } else if let Err(err) =
                op_manager.try_notify_node_event(crate::message::NodeEvent::BroadcastStateChange {
                    key: *key,
                    new_state: new_state.clone(),
                    is_retry: false,
                })
            {
                // Non-blocking emit: a 30-second `notify_node_event(...).await`
                // on this commit path was the primary back-pressure source
                // that wedged both gateways on 2026-05-24 (#4145). Missed
                // broadcasts heal via the next UPDATE or via summary-mismatch
                // SyncStateToPeer rounds — the executor must not stall here.
                //
                // Best-effort by design (see comment block above and
                // #4145): a missed broadcast heals via the next UPDATE
                // or summary-mismatch SyncStateToPeer round. Per-
                // occurrence WARN here flooded gateways under fan-out
                // at the same rate as the helper-internal log it
                // mirrored (#4238). The rate-limited `notify_node_event:
                // Notification channel full for too long` ERROR in
                // op_state_manager.rs is the sustained-back-pressure
                // alert operators should grep for.
                tracing::debug!(
                    contract = %key,
                    error = %err,
                    "Failed to broadcast state change to network peers (best-effort)"
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

        // Fetch each related contract: try local state_store first, escalate
        // to network GET when the executor has an `op_manager` attached. The
        // previous version was local-only, which silently failed cross-node
        // UPDATE flows where the validating node was a fresh receiver that
        // hadn't yet cached the related contract (see freenet/mail#80 — the
        // recipient's inbox UPDATE always carried `RequestRelated` for the
        // sender's AFT record, which the receiver hadn't seen before).
        let mut related_map: HashMap<ContractInstanceId, Option<State<'static>>> =
            HashMap::with_capacity(unique_ids.len());

        // Parallel fetch via `join_all`: previously serial under a single
        // 10s wall-clock budget, so N related ids each got ~10s/N
        // effective. Each id now races its own sub-op GET, so the budget
        // is per-id in the common case. See freenet/freenet-core#4077.
        //
        // Reborrow as `&Self` so the per-id futures share an immutable
        // borrow; the outer `&mut self` is reclaimed once `fetch_all`
        // is awaited.
        let this: &Self = &*self;
        let fetch_all = async {
            let results: Vec<(ContractInstanceId, Result<State<'static>, ExecutorError>)> =
                futures::future::join_all(unique_ids.iter().map(|id| {
                    let id = *id;
                    async move {
                        if let Some(full_key) = this.bridged_lookup_key(&id) {
                            if let Ok(state) = this.state_store.get(&full_key).await {
                                return (id, Ok(State::from(state.as_ref().to_vec())));
                            }
                        }
                        // Local lookup miss → escalate via the
                        // network-fallback helper (factored out so the
                        // per-id branch logic is testable with a stubbed
                        // fetcher). Mock executors that lack an
                        // `op_manager` get the legacy MissingRelated.
                        let outcome = fetch_related_via_network(this.op_manager.as_ref(), &id)
                            .await
                            .map(|state| State::from(state.as_ref().to_vec()));
                        (id, outcome)
                    }
                }))
                .await;
            for (id, res) in results {
                related_map.insert(id, Some(res?));
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
            // Mirror the broken-invariant gate in `commit_state_update`
            // above. Same rationale: a contract flagged as non-idempotent
            // must not be propagated.
            if op_manager.ring.is_contract_broken(&key) {
                tracing::debug!(
                    contract = %key,
                    event = "broadcast_suppressed_broken_contract",
                    "Skipping BroadcastStateChange for contract flagged as broken"
                );
                return;
            }
            // Non-blocking emit — see comment in the update path above
            // and #4145 for the wedge this prevents.
            if let Err(err) =
                op_manager.try_notify_node_event(crate::message::NodeEvent::BroadcastStateChange {
                    key,
                    new_state,
                    is_retry: false,
                })
            {
                // Best-effort by design — see #4145 and the sibling
                // commit path above. Per-occurrence WARN here re-
                // introduced the #4238 spam at the caller layer even
                // after the helper-internal downgrade.
                tracing::debug!(
                    contract = %key,
                    error = %err,
                    "Failed to broadcast state change to network peers (best-effort)"
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

    async fn remove_contract(
        &mut self,
        key: &ContractKey,
        _expected_generation: u64,
    ) -> Result<(), ExecutorError> {
        // The inner Executor does not own a Ring (and so cannot consult
        // the state-write generation directly). Race detection and
        // partial-failure retry both live at the
        // `RuntimePool::remove_contract` layer; the inner impl just
        // performs the disk reclamation. Trait-level callers that go
        // through this method (i.e. not via `RuntimePool`) cannot make
        // a Full/Partial distinction anyway, so collapse to
        // `Result<(), _>` — `Partial` is reported as `Ok` here.
        self.reclaim_contract_storage(key).await.map(|_| ())
    }
}

impl Executor<Runtime> {
    /// Create an Executor for local-only mode (no network operations).
    /// Use this from the binary for local mode execution.
    pub async fn from_config_local(config: Arc<Config>) -> anyhow::Result<Self> {
        Self::from_config(config, None).await
    }

    /// Create an Executor with optional network operation support.
    /// This is `pub(crate)` because the parameters involve crate-internal types.
    pub(crate) async fn from_config(
        config: Arc<Config>,
        op_manager: Option<Arc<OpManager>>,
    ) -> anyhow::Result<Self> {
        let (contract_store, delegate_store, secret_store, state_store) =
            Self::get_stores(&config).await?;
        let mut rt = Runtime::build(contract_store, delegate_store, secret_store, false).unwrap();
        // Enable V2 delegate contract access by providing the state store DB
        rt.set_state_store_db(state_store.storage());
        // V2 delegate state writes bypass the executor's
        // `state_store.{store,update}` chokepoints, so install a callback
        // that mirrors the bump+refresh+report side effects via
        // `Ring::commit_state_write`. Without this, V2 delegate PUT/UPDATE
        // would leave the EvictContract re-host race open AND undercount
        // StateBytesWritten in the governance meter.
        if let Some(op_manager_ref) = &op_manager {
            let op_manager_clone = op_manager_ref.clone();
            rt.set_state_write_callback(Arc::new(move |key: &ContractKey, state_size: usize| {
                op_manager_clone.ring.commit_state_write(key, state_size);
            }));
        }
        Executor::new(
            state_store,
            move || {
                if let Err(error) = crate::util::set_cleanup_on_exit(config.paths()) {
                    tracing::error!("Failed to set cleanup on exit: {error}");
                }
                Ok(())
            },
            OperationMode::Local,
            rt,
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
    // Each parameter is a distinct shared resource the pool wires through
    // explicitly; bundling them into a struct just to satisfy the lint
    // would obscure which executor sees which cache.
    #[allow(clippy::too_many_arguments)]
    pub(crate) async fn from_config_with_shared_modules(
        config: Arc<Config>,
        shared_state_store: StateStore<Storage>,
        op_manager: Option<Arc<OpManager>>,
        contract_modules: SharedModuleCache<ContractKey>,
        delegate_modules: SharedModuleCache<DelegateKey>,
        delegate_contexts: crate::wasm_runtime::DelegateContextCache,
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
            delegate_contexts,
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
        // V2 delegate state writes bypass the executor chokepoints — install
        // the commit_state_write callback so the bump+refresh+report side
        // effects are still applied. See `from_config` and
        // `Runtime::set_state_write_callback`.
        if let Some(op_manager_ref) = &op_manager {
            let op_manager_clone = op_manager_ref.clone();
            rt.set_state_write_callback(Arc::new(move |key: &ContractKey, state_size: usize| {
                op_manager_clone.ring.commit_state_write(key, state_size);
            }));
        }
        Executor::new(
            shared_state_store,
            || Ok(()),
            OperationMode::Local,
            rt,
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
            let written_bytes = new_state.as_ref().len();
            self.state_store
                .update(&key, new_state.clone())
                .await
                .map_err(ExecutorError::other)?;
            // State-write chokepoint (re-PUT): delegate to
            // `Ring::commit_state_write` for bump + refresh + report. See
            // its rustdoc and `RuntimePool::remove_contract` for the
            // EvictContract re-host race this closes.
            if let Some(op_manager) = &self.op_manager {
                op_manager.ring.commit_state_write(&key, written_bytes);
            }

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
            .map_err(ExecutorError::other)?;

        let updates = vec![update];

        // `Executor::contract_requests` is only invoked from `run_local_node`
        // (HTTP/WS local-only entry points). Network-mode UPDATEs from
        // clients arrive through `client_event_handling` →
        // `start_client_update` and never reach this function.
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

        Err(ExecutorError::other(anyhow::anyhow!(
            "network UPDATE must dispatch via `start_client_update` (client_events.rs); \
             `perform_contract_update` is reachable only in local mode"
        )))
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
        let written_bytes = state.as_ref().len();
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
        // State-write chokepoint (verify_and_store PUT): delegate to
        // `Ring::commit_state_write` for bump + refresh + report. See
        // its rustdoc and `RuntimePool::remove_contract` for the
        // EvictContract re-host race this closes.
        if let Some(op_manager) = &self.op_manager {
            op_manager.ring.commit_state_write(&key, written_bytes);
        }

        Ok(())
    }

    /// Reclaim a contract's on-disk storage after it was evicted from the
    /// hosting cache.
    ///
    /// Deletes (1) the persisted state and parameters from the `StateStore`
    /// and (2) the WASM code blob from the `ContractStore`. The contract-store
    /// removal is code-hash refcount-safe: the shared `.wasm` blob is only
    /// deleted once no other contract instance references the same code.
    ///
    /// Both steps are best-effort and independent: if one fails, the other is
    /// still attempted so a partial reclaim is achieved rather than none. The
    /// method is idempotent — both `StateStore::delete` and
    /// `ContractStore::remove_contract` tolerate already-missing entries — so a
    /// double eviction is harmless.
    ///
    /// Return value:
    ///   - `Ok(ReclaimOutcome::Full)` — both halves are absent at end (either
    ///     both deleted in this call, or one was already missing and the other
    ///     was deleted, or both were already missing).
    ///   - `Ok(ReclaimOutcome::Partial)` — exactly one half failed with a real
    ///     error while the other succeeded. The caller MUST retain the
    ///     pending-reclamation entry so a future sweep retries the remaining
    ///     work. Closes the disk-leak edge case where a transient DB/FS
    ///     error in one half leaves the other half permanently leaked. See
    ///     PR #4212 review round 8.
    ///   - `Err` — BOTH halves failed; surfaced so the caller can log/retry.
    ///
    /// This is the inherent implementation; the `ContractExecutor::remove_contract`
    /// trait method delegates to it and translates the outcome into
    /// pending-reclamation management.
    async fn reclaim_contract_storage(
        &mut self,
        key: &ContractKey,
    ) -> Result<ReclaimOutcome, ExecutorError> {
        let state_result = match self.state_store.delete(key).await {
            Ok(()) => Ok(()),
            Err(e) => {
                tracing::warn!(
                    contract = %key,
                    error = %e,
                    "failed to delete persisted state while reclaiming evicted contract"
                );
                Err(())
            }
        };
        let code_result = match self.runtime.contract_store.remove_contract(key) {
            Ok(()) => Ok(()),
            Err(e) => {
                tracing::warn!(
                    contract = %key,
                    error = %e,
                    "failed to delete WASM code while reclaiming evicted contract"
                );
                Err(())
            }
        };

        let state_ok = state_result.is_ok();
        let code_ok = code_result.is_ok();
        if !state_ok && !code_ok {
            return Err(ExecutorError::other(anyhow::anyhow!(
                "failed to reclaim any on-disk storage for contract {key}"
            )));
        }

        let outcome = if state_ok && code_ok {
            ReclaimOutcome::Full
        } else {
            ReclaimOutcome::Partial
        };
        tracing::info!(
            contract = %key,
            state_deleted = state_ok,
            code_deleted = code_ok,
            ?outcome,
            "reclaimed on-disk storage for evicted contract"
        );
        Ok(outcome)
    }
}

/// Outcome of [`Executor::reclaim_contract_storage`].
///
/// The split exists so the caller (`RuntimePool::remove_contract`) can decide
/// whether to clear the pending-reclamation entry (on `Full`) or leave it for
/// a future retry (on `Partial`). See PR #4212 review round 8.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ReclaimOutcome {
    /// Both state and code are absent at end of the reclaim call.
    Full,
    /// Exactly one half failed with a real error; the other succeeded (or was
    /// already absent). The pending-reclamation entry should be retained so a
    /// future sweep retries the remaining work.
    Partial,
}

impl Executor<Runtime> {
    /// Network-aware variant retained for the PUT path.
    ///
    /// The base `fetch_related_for_validation` on the bridged impl now also
    /// escalates to network GET via `op_manager` when the local state_store
    /// lookup misses, so the two implementations are functionally equivalent
    /// for `Executor<Runtime>`. This variant is kept because it exposes the
    /// `local_state_or_from_network` helper directly and several PUT call
    /// sites already wire through it; collapsing the two would touch more
    /// surface area than the bug fix needs.
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

        // Parallel fetch — see fetch_related_for_validation for rationale
        // (freenet/freenet-core#4077). The serial loop here had the same
        // 10s/N effective per fetch problem.
        //
        // We can't reuse the `&mut self` `local_state_or_from_network`
        // helper across multiple concurrent futures, so the per-id body
        // is inlined: try the local state_store first, escalate to
        // `fetch_related_via_network` (which only borrows
        // `&Option<Arc<OpManager>>`). Reborrow as `&Self` so the
        // per-id futures share an immutable borrow; the outer
        // `&mut self` is reclaimed once `fetch_all` is awaited.
        let this: &Self = &*self;
        let fetch_all = async {
            let results: Vec<(ContractInstanceId, Result<State<'static>, ExecutorError>)> =
                futures::future::join_all(unique_ids.iter().map(|id| {
                    let id = *id;
                    async move {
                        if let Some(full_key) = this.lookup_key(&id) {
                            if let Ok(state) = this.state_store.get(&full_key).await {
                                return (id, Ok(State::from(state.as_ref().to_vec())));
                            }
                        }
                        let outcome = fetch_related_via_network(this.op_manager.as_ref(), &id)
                            .await
                            .map(|state| State::from(state.as_ref().to_vec()));
                        (id, outcome)
                    }
                }))
                .await;
            for (id, res) in results {
                related_map.insert(id, Some(res?));
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
        let op_manager = self
            .op_manager
            .as_ref()
            .ok_or_else(|| ExecutorError::other(anyhow::anyhow!("missing op_manager")))?;
        let executor_tx = crate::message::Transaction::new::<operations::subscribe::SubscribeMsg>();
        // Caps total task lifetime — the inner driver's per-attempt
        // `OPERATION_TTL = 60 s` would otherwise allow multi-attempt
        // waits to compound. Any change here should be checked against
        // the per-attempt budget so `MAX_RETRIES` attempts can complete
        // within the deadline.
        const SUBSCRIBE_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(120);
        match tokio::time::timeout(
            SUBSCRIBE_TIMEOUT,
            operations::subscribe::run_executor_subscribe(
                op_manager.clone(),
                *key.id(),
                executor_tx,
            ),
        )
        .await
        {
            Ok(Ok(())) => Ok(()),
            Ok(Err(err)) => Err(ExecutorError::other(anyhow::anyhow!("{err}"))),
            Err(_) => Err(ExecutorError::other(anyhow::anyhow!(
                "executor subscribe timed out after {}s",
                SUBSCRIBE_TIMEOUT.as_secs()
            ))),
        }
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
        // Fetch from network via the sub-op GET driver. The driver
        // delivers the resolved `GetResult` directly through a oneshot.
        let op_manager = self
            .op_manager
            .as_ref()
            .ok_or_else(|| ExecutorError::other(anyhow::anyhow!("missing op_manager")))?;
        let (_tx, rx) =
            operations::get::op_ctx_task::start_sub_op_get(op_manager, *id, return_contract_code);
        // Outer callers may wrap this with a tighter budget (e.g.,
        // `fetch_related_for_validation_network` uses
        // `RELATED_FETCH_TIMEOUT = 10s`); when that fires first the
        // receiver is dropped silently and the spawned sub-op task
        // continues until OPERATION_TTL exhausts the retry loop. No
        // leak (oneshot send-after-drop is graceful) — just a
        // longer-lived background task.
        const SUB_OP_FETCH_TIMEOUT: Duration = Duration::from_secs(120);
        let outcome = tokio::time::timeout(SUB_OP_FETCH_TIMEOUT, rx)
            .await
            .map_err(|_| {
                tracing::warn!(
                    contract = %id,
                    "sub-op GET timed out at executor"
                );
                ExecutorError::other(anyhow::anyhow!("sub-op GET timed out"))
            })?
            .map_err(|_| ExecutorError::other(anyhow::anyhow!("sub-op GET task dropped")))?;
        match outcome {
            operations::get::op_ctx_task::SubOpGetOutcome::Found(get_result) => {
                Ok(Either::Right(get_result))
            }
            operations::get::op_ctx_task::SubOpGetOutcome::NotFound(cause) => {
                Err(ExecutorError::other(anyhow::anyhow!(cause)))
            }
            operations::get::op_ctx_task::SubOpGetOutcome::Infra(err) => {
                Err(ExecutorError::other(err))
            }
        }
    }
}

/// Network-escalation half of the bridged `fetch_related_for_validation`
/// loop, factored out so the dispatch logic is unit-testable with a
/// stubbed fetcher. Production callers pass the executor's own
/// `op_manager`; tests in this module override [`TEST_NETWORK_FETCH_OVERRIDE`]
/// (a thread-local) to redirect the network call to a stub instead of
/// driving a real network sub-op.
///
/// Behavior:
/// - `op_manager.is_none()` → return `MissingRelated`. This preserves the
///   legacy local-only outcome for mock executors and unit tests that
///   never wire up a real op_manager.
/// - `op_manager.is_some()` → drive a sub-op GET via
///   `start_sub_op_get`. `Found` resolves to the fetched state;
///   `NotFound`/`Infra` map back to `MissingRelated`.
async fn fetch_related_via_network(
    op_manager: Option<&Arc<crate::node::OpManager>>,
    id: &ContractInstanceId,
) -> Result<WrappedState, ExecutorError> {
    #[cfg(test)]
    {
        if let Some(stub) = TEST_NETWORK_FETCH_OVERRIDE.with(|cell| cell.borrow().clone()) {
            return stub(*id);
        }
    }
    let Some(op_manager) = op_manager else {
        return Err(ExecutorError::request(StdContractError::MissingRelated {
            key: *id,
        }));
    };
    // `_tx` is named for clarity; not a drop guard. `Transaction` is
    // `Copy` so the binding has no lifetime effect today.
    let (_tx, rx) = crate::operations::get::op_ctx_task::start_sub_op_get(op_manager, *id, false);
    let outcome = rx
        .await
        .map_err(|_| ExecutorError::other(anyhow::anyhow!("sub-op GET task dropped")))?;
    match outcome {
        crate::operations::get::op_ctx_task::SubOpGetOutcome::Found(get_result) => {
            Ok(WrappedState::from(get_result.state.as_ref().to_vec()))
        }
        crate::operations::get::op_ctx_task::SubOpGetOutcome::NotFound(_)
        | crate::operations::get::op_ctx_task::SubOpGetOutcome::Infra(_) => {
            Err(ExecutorError::request(StdContractError::MissingRelated {
                key: *id,
            }))
        }
    }
}

#[cfg(test)]
pub(crate) type NetworkFetchStub =
    std::rc::Rc<dyn Fn(ContractInstanceId) -> Result<WrappedState, ExecutorError>>;

#[cfg(test)]
thread_local! {
    /// Test hook used by `fetch_related_via_network` to bypass the real
    /// network sub-op driver. Set with [`set_test_network_fetch_override`]
    /// inside a `#[tokio::test(flavor = "current_thread")]` so the
    /// thread-local lookup hits the same task that ran the test setup.
    static TEST_NETWORK_FETCH_OVERRIDE: std::cell::RefCell<Option<NetworkFetchStub>> =
        const { std::cell::RefCell::new(None) };
}

#[cfg(test)]
pub(crate) fn set_test_network_fetch_override(stub: Option<NetworkFetchStub>) {
    TEST_NETWORK_FETCH_OVERRIDE.with(|cell| *cell.borrow_mut() = stub);
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

#[cfg(test)]
mod executor_pin_tests {
    /// Pin: `local_state_or_from_network` MUST use the sub-op GET
    /// driver.
    #[test]
    fn local_state_or_from_network_uses_sub_op_driver() {
        let src = include_str!("runtime.rs");
        let body = src
            .split("async fn local_state_or_from_network(")
            .nth(1)
            .expect("local_state_or_from_network must exist")
            .split(
                "
    }",
            )
            .next()
            .expect("closing brace");
        assert!(
            body.contains("start_sub_op_get"),
            "local_state_or_from_network must call start_sub_op_get"
        );
        // Compose the needles at runtime so the assertion source itself
        // doesn't trip the pin.
        let get_contract_needle = ["Get", "Contract", " {"].concat();
        assert!(
            !body.contains(&get_contract_needle),
            "local_state_or_from_network must NOT construct GetContract"
        );
        let op_request_needle = ["self.", "op_request"].concat();
        assert!(
            !body.contains(&op_request_needle),
            "local_state_or_from_network must NOT call self.op_request"
        );
    }

    /// Pin: `executor::subscribe` MUST use `run_executor_subscribe`.
    #[test]
    fn executor_subscribe_uses_run_executor_subscribe() {
        let src = include_str!("runtime.rs");
        let body = src
            .split("async fn subscribe(&mut self, key: ContractKey)")
            .nth(1)
            .expect("executor::subscribe must exist")
            .split(
                "
    }",
            )
            .next()
            .expect("closing brace");
        assert!(
            body.contains("run_executor_subscribe"),
            "executor::subscribe must call run_executor_subscribe"
        );
        // Compose the needle at runtime so the assertion source itself
        // doesn't trip the pin.
        let sub_contract_needle = ["Subscribe", "Contract", " {"].concat();
        assert!(
            !body.contains(&sub_contract_needle),
            "executor::subscribe must NOT construct SubscribeContract"
        );
        let op_request_needle = ["self.", "op_request"].concat();
        assert!(
            !body.contains(&op_request_needle),
            "executor::subscribe must NOT call self.op_request"
        );
    }

    /// Pin: `perform_contract_update` MUST NOT route the network branch
    /// through `UpdateContract` / `self.op_request` / `request_update`.
    /// Network-mode UPDATEs flow through `start_client_update`.
    #[test]
    fn perform_contract_update_does_not_use_network_op_request() {
        let src = include_str!("runtime.rs");
        let body = src
            .split("async fn perform_contract_update(")
            .nth(1)
            .expect("perform_contract_update must exist")
            .split(
                "
    }",
            )
            .next()
            .expect("closing brace");
        let update_contract_needle = ["Update", "Contract", " {"].concat();
        assert!(
            !body.contains(&update_contract_needle),
            "perform_contract_update must NOT construct UpdateContract"
        );
        let op_request_needle = ["self.", "op_request"].concat();
        assert!(
            !body.contains(&op_request_needle),
            "perform_contract_update must NOT call self.op_request; \
             network-mode UPDATEs flow through start_client_update"
        );
        let request_update_needle = ["request_", "update("].concat();
        assert!(
            !body.contains(&request_update_needle),
            "perform_contract_update must NOT call request_update"
        );
    }

    /// Pin: the three sites that resolve a contract's `requires(...)`
    /// related-list MUST fan out via `join_all`, not iterate serially
    /// inside a `for` loop. Regression: the previous serial loops shared
    /// a single 10s wall-clock budget (`RELATED_FETCH_TIMEOUT`), so for
    /// N>1 ids the per-fetch budget was ~10s/N. On real networks where
    /// AFT-style related contracts are far in keyspace, this pinned
    /// receivers' inboxes at empty state forever — see
    /// `freenet/freenet-core#4077` and `freenet/mail#198 / mail#202`
    /// for the production trace and the app-side workaround.
    ///
    /// We can't unit-test the parallelism timing directly: the
    /// `TEST_NETWORK_FETCH_OVERRIDE` stub is sync (`Rc<dyn Fn>`), so a
    /// per-id artificial delay would block the executor thread rather
    /// than yield. A source-string pin ensures none of the three sites
    /// silently regresses to a `for id in &unique_ids { ... await ... }`
    /// loop, which is the exact failure mode #4077 documents.
    #[test]
    fn related_contract_fetch_sites_use_join_all() {
        let src = include_str!("runtime.rs");

        // Each entry: (function name, exact split needle for body extraction).
        let sites: &[(&str, &str)] = &[
            (
                "fetch_related_for_validation",
                "async fn fetch_related_for_validation(",
            ),
            (
                "fetch_related_for_validation_network",
                "async fn fetch_related_for_validation_network(",
            ),
        ];

        for (name, needle) in sites {
            let body = src
                .split(needle)
                .nth(1)
                .unwrap_or_else(|| panic!("{name} must exist in runtime.rs"))
                .split("\n    }\n")
                .next()
                .unwrap_or_else(|| panic!("{name} closing brace not found"));
            assert!(
                body.contains("join_all"),
                "{name} must call futures::future::join_all — serial fetch \
                 regressed in freenet/freenet-core#4077; do not revert"
            );
            // Spot-check the loop construct doesn't reappear: a plain
            // `for id in &unique_ids` inside the function body almost
            // certainly means the parallel fan-out is gone.
            let serial_needle = ["for ", "id in &unique_ids"].concat();
            assert!(
                !body.contains(&serial_needle),
                "{name} must not iterate serially over &unique_ids — \
                 regressed to pre-#4077 behavior"
            );
        }

        // The third site is inline inside `upsert_contract_state`, not
        // its own function. Pin it by confirming the comment marker
        // that frames the parallel-fetch block is present and that the
        // block contains `join_all`. Brittle by design: a refactor that
        // moves the comment also has to move the assertion.
        let inline_marker = "Parallel fetch: each related contract goes through its";
        let after_marker = src.split(inline_marker).nth(1).expect(
            "inline UPDATE-side parallel-fetch marker missing — \
                 #4077 regressed",
        );
        assert!(
            after_marker[..2_000].contains("join_all"),
            "UPDATE-side inline related fetch must call \
             futures::future::join_all (#4077)"
        );
    }

    /// Pin: the `"Contract state updated"` notice fires on every successful
    /// state write — at INFO it contributed ~44% of the post-#4252
    /// log-volume regression on River-subscribed peers (see #4251 follow-up
    /// PR). Re-promoting it would silently restore the disk-fill issue.
    ///
    /// Anchored on the *closest* preceding `tracing::` macro via `rfind` so
    /// the assertion can't false-pass if an unrelated nearby site is at
    /// DEBUG. An additional guard rejects matches inside string literals or
    /// comments by requiring the match to start a code line (whitespace-only
    /// prefix on its line).
    #[test]
    fn contract_state_updated_logs_at_debug_pin_test() {
        let src = include_str!("runtime.rs");
        let needle = "\"Contract state updated\"";
        let idx = src
            .find(needle)
            .expect("Contract state updated log message must still exist in source");
        let preceding = &src[..idx];
        let macro_idx = preceding
            .rfind("tracing::")
            .expect("a tracing macro must precede the Contract-state-updated log site");
        let line_start = preceding[..macro_idx].rfind('\n').map_or(0, |n| n + 1);
        let line_prefix = &preceding[line_start..macro_idx];
        assert!(
            line_prefix.chars().all(char::is_whitespace),
            "rfind matched `tracing::` inside a string literal or comment, \
             not a macro invocation. Prefix on its line: {line_prefix:?}"
        );
        let after_macro = &preceding[macro_idx + "tracing::".len()..];
        let macro_name = after_macro.split('!').next().unwrap_or("");
        let tail = &preceding[preceding.len().saturating_sub(200)..];
        assert_eq!(
            macro_name, "debug",
            "Contract-state-updated log site must be at DEBUG \
             (closest preceding macro is `tracing::{macro_name}!`). \
             Re-promotion to INFO/WARN restores the #4251 / #4272 log-volume regression.\n\
             Preceding source (last 200 bytes):\n{tail}"
        );
    }
}

#[cfg(test)]
mod remove_contract_tests {
    //! Tests for `Executor::reclaim_contract_storage` — the disk-reclamation
    //! path wired to hosting-cache eviction. The core proof here is that
    //! evicting a contract actually frees its on-disk state and WASM code,
    //! so the hosting budget is a real disk bound.
    //!
    //! Note: the `RuntimePool::remove_contract` re-host / re-subscribe /
    //! generation-mismatch TOCTOU guards (which consult `op_manager.ring`)
    //! are not unit-tested here because constructing a `RuntimePool`
    //! requires a fully-built `OpManager` (config, `NetEventRegister`,
    //! ring, etc.), which is too heavy for a focused unit test. The
    //! `Ring::is_hosting_contract` / `Ring::contract_in_use` /
    //! `Ring::state_generation` predicates the guards rely on are covered
    //! directly in `ring/hosting.rs`. End-to-end coverage of the guarded
    //! eviction path is a deferred `#[freenet_test]` follow-up.

    use std::sync::Arc;

    use freenet_stdlib::prelude::{
        ContractCode, ContractContainer, ContractKey, ContractWasmAPIVersion, Parameters,
        WrappedContract, WrappedState,
    };

    use super::ReclaimOutcome;
    use crate::contract::executor::Executor;
    use crate::contract::storages::Storage;
    use crate::wasm_runtime::{
        ContractStore, DelegateStore, Runtime, SecretsStore, StateStore, StateStoreError,
    };

    /// Build a disk-backed `Executor<Runtime>` and return it alongside the
    /// `contracts_dir` (so the test can probe the `.wasm` file directly) and
    /// the `TempDir` (kept alive for the test's duration).
    async fn build_disk_executor(
        seed: &str,
    ) -> (Executor<Runtime>, std::path::PathBuf, tempfile::TempDir) {
        let temp_dir = crate::util::tests::get_temp_dir();
        let db = Storage::new(temp_dir.path())
            .await
            .expect("create storage db");
        let contracts_dir = temp_dir.path().join(format!("contracts-{seed}"));
        let contract_store = ContractStore::new(contracts_dir.clone(), 10_000, db.clone())
            .expect("create contract store");
        let delegate_store =
            DelegateStore::new(temp_dir.path().join("delegate"), 10_000, db.clone())
                .expect("create delegate store");
        let secrets_store = SecretsStore::new(
            temp_dir.path().join("secrets"),
            Default::default(),
            db.clone(),
        )
        .expect("create secrets store");
        let state_store = StateStore::new(db, 10_000_000).expect("create state store");
        let runtime = Runtime::build(contract_store, delegate_store, secrets_store, false)
            .expect("build runtime");
        let executor = Executor::new(
            state_store,
            || Ok(()),
            crate::contract::executor::OperationMode::Local,
            runtime,
            None,
        )
        .await
        .expect("create executor");
        (executor, contracts_dir, temp_dir)
    }

    /// Construct a synthetic contract container. The bytes are never executed
    /// (`reclaim_contract_storage` only deletes files / DB rows), so a fake
    /// blob is sufficient and far faster than compiling real WASM.
    fn make_contract(code_seed: u8, param_seed: u8) -> (ContractContainer, ContractKey) {
        let code = ContractCode::from(vec![code_seed; 64]);
        let params = Parameters::from(vec![param_seed; 8]);
        let key = ContractKey::from_params_and_code(&params, &code);
        let wrapped = WrappedContract::new(Arc::new(code), params);
        let container = ContractContainer::Wasm(ContractWasmAPIVersion::V1(wrapped));
        (container, key)
    }

    fn wasm_path(contracts_dir: &std::path::Path, key: &ContractKey) -> std::path::PathBuf {
        contracts_dir
            .join(key.code_hash().encode())
            .with_extension("wasm")
    }

    /// Core regression test: storing a contract makes its state retrievable
    /// and its `.wasm` blob present on disk; `remove_contract` reclaims both.
    #[tokio::test(flavor = "multi_thread")]
    async fn remove_contract_reclaims_state_and_wasm_from_disk() {
        let (mut executor, contracts_dir, _temp) = build_disk_executor("reclaim").await;
        let (container, key) = make_contract(0x11, 0x22);
        let params = container.params();
        let state = WrappedState::new(b"hosted state payload".to_vec());

        // Store the WASM blob and the persisted state.
        executor
            .runtime
            .contract_store
            .store_contract(container)
            .expect("store contract code");
        executor
            .state_store
            .store(key, state.clone(), params)
            .await
            .expect("store contract state");

        // Pre-conditions: state retrievable and the .wasm file exists.
        let fetched = executor
            .state_store
            .get(&key)
            .await
            .expect("state retrievable before eviction");
        assert_eq!(fetched, state, "stored state must round-trip");
        let blob = wasm_path(&contracts_dir, &key);
        assert!(
            blob.exists(),
            "WASM blob must exist on disk before eviction: {blob:?}"
        );

        // Evict.
        let outcome = executor
            .reclaim_contract_storage(&key)
            .await
            .expect("reclaim must succeed");
        assert_eq!(
            outcome,
            ReclaimOutcome::Full,
            "fresh-evict path with both halves present must be Full"
        );

        // Post-conditions: state gone, .wasm gone.
        match executor.state_store.get(&key).await {
            Err(StateStoreError::MissingContract(missing)) => assert_eq!(missing, key),
            other => panic!("expected MissingContract after eviction, got {other:?}"),
        }
        assert!(
            !blob.exists(),
            "WASM blob must be deleted from disk after eviction: {blob:?}"
        );
    }

    /// Double eviction is idempotent: a second `remove_contract` on an
    /// already-reclaimed contract is a harmless no-op, not an error.
    #[tokio::test(flavor = "multi_thread")]
    async fn remove_contract_is_idempotent_on_double_eviction() {
        let (mut executor, contracts_dir, _temp) = build_disk_executor("idempotent").await;
        let (container, key) = make_contract(0x33, 0x44);
        let params = container.params();
        let state = WrappedState::new(b"payload".to_vec());

        executor
            .runtime
            .contract_store
            .store_contract(container)
            .expect("store contract code");
        executor
            .state_store
            .store(key, state, params)
            .await
            .expect("store contract state");

        let first = executor
            .reclaim_contract_storage(&key)
            .await
            .expect("first reclaim must succeed");
        assert_eq!(
            first,
            ReclaimOutcome::Full,
            "first reclaim with both halves present must be Full"
        );
        // Second reclaim: state and .wasm are already gone — both
        // backends treat missing entries as a successful no-op, so the
        // outcome stays Full (not Partial). This pins down the
        // "idempotent double-evict" invariant after the Full/Partial
        // refactor.
        let second = executor
            .reclaim_contract_storage(&key)
            .await
            .expect("second reclaim must be a no-op, not an error");
        assert_eq!(
            second,
            ReclaimOutcome::Full,
            "double-evict must report Full (both backends treat missing as ok)"
        );
        assert!(
            !wasm_path(&contracts_dir, &key).exists(),
            "WASM blob must remain absent after double eviction"
        );
    }

    /// Reclaiming a never-stored contract is also a harmless no-op: both the
    /// state-store delete and the contract-store removal tolerate a fully
    /// absent contract.
    #[tokio::test(flavor = "multi_thread")]
    async fn remove_contract_unknown_contract_is_noop() {
        let (mut executor, _contracts_dir, _temp) = build_disk_executor("unknown").await;
        let (_container, key) = make_contract(0x55, 0x66);
        let outcome = executor
            .reclaim_contract_storage(&key)
            .await
            .expect("reclaiming an unknown contract must be Ok");
        assert_eq!(
            outcome,
            ReclaimOutcome::Full,
            "unknown-contract path is treated as already-clean, hence Full"
        );
    }

    /// `ReclaimOutcome` discrimination compiles and the `Full` vs `Partial`
    /// shape works in trivial cases.
    ///
    /// Full coverage:
    ///   - state present + code present → Full (covered above in
    ///     `remove_contract_reclaims_state_and_wasm_from_disk`).
    ///   - both absent → Full (covered above in
    ///     `remove_contract_is_idempotent_on_double_eviction` and
    ///     `remove_contract_unknown_contract_is_noop`).
    ///   - state present + code already gone → still Full (because
    ///     `ContractStore::remove_contract` treats a missing blob as
    ///     `Ok(())`, and the state half deletes cleanly).
    ///
    /// Partial coverage: a real `Partial` outcome would require fault
    /// injection at the `StateStore::delete` or
    /// `ContractStore::remove_contract` level (e.g. a poisoned redb
    /// transaction or a permissions error on the contracts dir). The
    /// current backends do not surface a "failed but not for missing"
    /// error mode that's safe to provoke from a unit test without
    /// reaching into private state — so genuine `Partial` is exercised
    /// only via the manager-layer logic (`RuntimePool::remove_contract`
    /// retains the pending entry on `Partial` and forgets it on
    /// `Full`). A `#[freenet_test]` follow-up could simulate a backend
    /// fault, but that's out of scope here.
    #[tokio::test(flavor = "multi_thread")]
    async fn reclaim_outcome_state_present_code_absent_is_full() {
        let (mut executor, _contracts_dir, _temp) = build_disk_executor("partial-state-only").await;
        let (container, key) = make_contract(0x77, 0x88);
        let params = container.params();
        let state = WrappedState::new(b"state without code".to_vec());

        // Skip storing the contract code; only persist state. The
        // contract store's `remove_contract` for an absent key is
        // `Ok(())`, so the outcome should still be Full.
        executor
            .state_store
            .store(key, state, params)
            .await
            .expect("store contract state");

        let outcome = executor
            .reclaim_contract_storage(&key)
            .await
            .expect("reclaim must succeed even when code half is already absent");
        assert_eq!(
            outcome,
            ReclaimOutcome::Full,
            "state-only present + code-already-gone counts as Full because \
             both halves are absent at end"
        );
    }
}

#[cfg(test)]
mod state_write_attribution_pin_tests {
    //! Source-grep pin tests for the StateBytesWritten reporter. The
    //! `Ring::commit_state_write` helper bundles three side effects
    //! (bump generation, refresh hosting-cache snapshot, report bytes
    //! to the governance meter). The "Manually-mirrored telemetry
    //! counters" row in `.claude/rules/bug-prevention-patterns.md`
    //! says: a future refactor that hand-inlines one of those three
    //! steps WITHOUT the report leg silently undercounts every state
    //! write on that path. To make that failure mode trip CI instead
    //! of going unnoticed for months, this module asserts at the
    //! source level that:
    //!
    //!   1. There is exactly ONE place that calls `bump_state_generation`
    //!      directly: the `commit_state_write` helper itself in ring.rs.
    //!      Every other state-write site goes through the helper.
    //!   2. The number of `commit_state_write` call sites in runtime.rs
    //!      matches the number of state-write chokepoints we currently
    //!      have (6 — see the comment at the top of the helper). If
    //!      a new chokepoint is added without wiring it, this test
    //!      will fail loudly until either the chokepoint is wired or
    //!      this expected count is updated *with* a comment explaining
    //!      why.
    //!
    //! These tests read their own source code (a common Rust idiom for
    //! enforcing structural invariants — see `cargo` and `rustc`'s own
    //! test suites for similar patterns).

    const RUNTIME_SRC: &str = include_str!("runtime.rs");
    const RING_SRC: &str = include_str!("../../ring.rs");
    const NATIVE_API_SRC: &str = include_str!("../../wasm_runtime/native_api.rs");

    /// Count lines containing the needle that are NOT comments, docstrings,
    /// or string literals. A line counts only when the needle appears as
    /// real code — the heuristic is: the line is not a comment AND the
    /// needle does not appear inside a double-quoted string on that line.
    fn count_call_sites(src: &str, needle: &str) -> usize {
        src.lines()
            .filter(|line| {
                let trimmed = line.trim_start();
                if trimmed.starts_with("//") {
                    return false;
                }
                // Strip everything between matched double quotes so we
                // don't count needle occurrences inside string literals
                // (the test's own assertion messages contain the needles).
                let stripped = strip_string_literals(line);
                stripped.contains(needle)
            })
            .count()
    }

    /// Replace the contents of every `"..."` on the line with empty
    /// quotes so substring searches on the result skip string literals.
    /// Handles escaped quotes pragmatically (rare in this codebase).
    fn strip_string_literals(line: &str) -> String {
        let mut out = String::with_capacity(line.len());
        let mut in_string = false;
        let mut prev_was_backslash = false;
        for c in line.chars() {
            if in_string {
                if c == '"' && !prev_was_backslash {
                    in_string = false;
                    out.push('"');
                }
                // drop characters inside the string
            } else if c == '"' {
                in_string = true;
                out.push('"');
            } else {
                out.push(c);
            }
            prev_was_backslash = c == '\\' && !prev_was_backslash;
        }
        out
    }

    #[test]
    fn bump_state_generation_has_exactly_one_caller_outside_hosting_manager() {
        // The only NON-comment call to `.bump_state_generation(` in ring.rs
        // should be the one inside `commit_state_write`. Every other
        // state-write site goes through `commit_state_write` rather than
        // calling the primitive directly.
        let count = count_call_sites(RING_SRC, ".bump_state_generation(");
        assert_eq!(
            count, 1,
            "expected exactly 1 .bump_state_generation( call in ring.rs \
             (inside commit_state_write); found {count}. New direct \
             callers should go through Ring::commit_state_write instead, \
             or this assertion needs updating with a comment explaining \
             why the new direct caller is correct."
        );

        // And runtime.rs MUST NOT call .bump_state_generation directly —
        // every state-write site should go through commit_state_write.
        let runtime_calls = count_call_sites(RUNTIME_SRC, ".bump_state_generation(");
        assert_eq!(
            runtime_calls, 0,
            "runtime.rs must not call .bump_state_generation directly; \
             use Ring::commit_state_write instead (which bundles the \
             bump + refresh + report side effects). See \
             `.claude/rules/bug-prevention-patterns.md` row \
             'Manually-mirrored telemetry counters'."
        );
    }

    #[test]
    fn every_runtime_state_write_chokepoint_goes_through_commit_state_write() {
        // 4 executor-internal chokepoints (PUT-new, UPDATE, re-PUT,
        // verify_and_store PUT) + 2 V2 delegate callback installers
        // = 6 total commit_state_write call sites in runtime.rs.
        const EXPECTED: usize = 6;
        let count = count_call_sites(RUNTIME_SRC, ".commit_state_write(");
        assert_eq!(
            count, EXPECTED,
            "expected exactly {EXPECTED} .commit_state_write( call sites \
             in runtime.rs; found {count}. If you added a new state-write \
             chokepoint, wire it through `Ring::commit_state_write` and \
             bump this expectation. If you removed one, ensure the \
             chokepoint is genuinely gone (not just relocated) before \
             lowering this expectation."
        );
    }

    #[test]
    fn v2_delegate_state_write_paths_invoke_callback_with_state_size() {
        // The V2 delegate PUT and UPDATE paths in native_api.rs MUST
        // capture state.len() BEFORE the move into store_state_sync /
        // update_state_sync, and pass it to the callback. Otherwise the
        // governance scoring undercounts every V2 delegate write by the
        // full state size of that write.
        let calls = count_call_sites(NATIVE_API_SRC, "cb(&contract_key,");
        assert_eq!(
            calls, 2,
            "expected exactly 2 callback invocations passing state_size \
             in native_api.rs (one for PUT, one for UPDATE); found {calls}"
        );
        // And state.len() MUST be captured before the move.
        let captures = count_call_sites(NATIVE_API_SRC, "let state_size = state.len();");
        assert_eq!(
            captures, 2,
            "expected exactly 2 `let state_size = state.len();` captures \
             in native_api.rs (one before each state-store move); found \
             {captures}. The order matters — capturing AFTER the move \
             into store_state_sync would not compile, but a refactor \
             that moves state into an intermediate first could regress \
             this silently."
        );
    }
}

// NOTE: this module is placed at the END of the file on purpose. The
// `production_gate_sites_consult_is_contract_broken` pin test in
// `pool_tests/non_idempotent_detector_tests.rs` greps the production slice of
// this file (everything before the FIRST `#[cfg(test)]`) for
// `is_contract_broken`; a `#[cfg(test)]` placed above the gate sites would
// truncate that slice and break the pin. Keep new test modules below all
// production code.
#[cfg(test)]
mod idempotency_probe_convergence_tests {
    use super::byte_multiset_eq;

    /// Regression for #4295: the ping contract's `HashMap` state re-serialized
    /// in a different key ORDER on re-merge — same bytes, permuted. That MUST
    /// be treated as benign (same multiset), not flagged.
    #[test]
    fn reordered_bytes_are_benign_flutter() {
        assert!(
            byte_multiset_eq(b"{\"a\":1,\"b\":2}", b"{\"b\":2,\"a\":1}"),
            "a key-order permutation has the same byte multiset and must not be flagged"
        );
        // Identical bytes are trivially benign.
        assert!(byte_multiset_eq(b"same", b"same"));
    }

    /// Regression for the review finding: the #4251 production violator was a
    /// FIXED-SIZE, byte-different non-idempotent merge (a ~464-byte state whose
    /// counter prefix churns in place). A size-only check missed it; the
    /// multiset check MUST flag it (different content => different multiset).
    #[test]
    fn fixed_size_content_change_is_flagged() {
        // Same length, one byte of content differs (e.g. a counter 464 -> 465).
        assert!(
            !byte_multiset_eq(b"counter=464;payload", b"counter=465;payload"),
            "a fixed-size content change must be detected as non-idempotent"
        );
        // Simulate the 464-byte fixed-size shape: equal length, differing bytes.
        let mut s1 = vec![b'x'; 464];
        let mut s2 = vec![b'x'; 464];
        s1[0] = 0;
        s2[0] = 1; // counter prefix churn at constant size
        assert!(
            !byte_multiset_eq(&s1, &s2),
            "fixed-size (464-byte) counter churn must be flagged (the #4251 shape)"
        );
    }

    /// A growing (accumulating) merge changes the length (and would also change
    /// content); the length guard alone flags it. Non-convergent.
    #[test]
    fn growing_state_is_flagged() {
        assert!(
            !byte_multiset_eq(b"abc", b"abcd"),
            "a state that grows on re-application is non-convergent"
        );
    }
}
