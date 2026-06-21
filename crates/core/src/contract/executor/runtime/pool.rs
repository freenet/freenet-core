use super::*;

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
        // Without this, each of the N executors would maintain its own cache, causing
        // the same contracts to be compiled and stored N times (e.g., 16 executors × 92 contracts
        // × ~500KB-1MB = ~1.2 GB of duplicate compiled modules on the nova gateway).
        //
        // The caches are bounded by total compiled BYTES, not entry count, so a
        // gateway hosting thousands of contracts no longer thrashes the old
        // 1024-entry count cap (issue #4441). The contract budget is
        // operator-tunable via `Config::module_cache_budget_bytes`
        // (FREENET_MODULE_CACHE_BUDGET_BYTES / `module-cache-budget-bytes`);
        // when unset it scales with system RAM (see
        // `wasm_runtime::default_module_cache_budget_bytes`). The DELEGATE cache
        // gets only a fraction of the contract budget
        // (`DELEGATE_MODULE_CACHE_BUDGET_DIVISOR`) — delegates are far fewer and
        // smaller, and this keeps the COMBINED ceiling safe on a small VPS so
        // the #4441 OOM fix doesn't itself OOM a small box.
        let contract_cache_budget = config.module_cache_budget_bytes;
        let delegate_cache_budget = (contract_cache_budget
            / crate::wasm_runtime::DELEGATE_MODULE_CACHE_BUDGET_DIVISOR)
            .max(1);
        let shared_contract_modules: SharedModuleCache<ContractKey> = Arc::new(Mutex::new(
            ModuleCache::with_label(contract_cache_budget, "contract"),
        ));
        let shared_delegate_modules: SharedModuleCache<DelegateKey> = Arc::new(Mutex::new(
            ModuleCache::with_label(delegate_cache_budget, "delegate"),
        ));
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
    /// Panic safety: WASM traps are caught at the wasmtime boundary and
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

    fn op_manager_handle(&self) -> Option<Arc<crate::node::OpManager>> {
        Some(self.op_manager.clone())
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

    async fn upsert_contract_state_deferrable(
        &mut self,
        key: ContractKey,
        update: Either<WrappedState, StateDelta<'static>>,
        related_contracts: RelatedContracts<'static>,
        code: Option<ContractContainer>,
    ) -> Result<UpsertOutcome, ExecutorError> {
        self.track_contract_checkout(&key);
        let mut executor = self.pop_executor().await;
        let result = bridged_upsert_outcome(
            executor
                .bridged_upsert_contract_state_inner(key, update, related_contracts, code, true)
                .await,
        );
        self.return_checked(executor, "upsert_contract_state_deferrable")
            .await;
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
        user_context: Option<&UserSecretContext>,
    ) -> Response {
        let mut executor = self.pop_executor().await;
        let result = executor.delegate_request(req, origin_contract, caller_delegate, user_context);
        self.return_checked(executor, "execute_delegate_request")
            .await;
        result
    }

    async fn export_user_secrets(
        &mut self,
        user_context: &UserSecretContext,
        token: &[u8],
    ) -> Result<Vec<u8>, ExecutorError> {
        // Run on a pooled executor so the on-disk secrets redb is touched only
        // by its single writer (all pool executors point at the same
        // `secrets_dir`, but `pop_executor` serializes the checkout).
        let executor = self.pop_executor().await;
        let result = executor.export_user_secrets(user_context, token);
        self.return_checked(executor, "export_user_secrets").await;
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
