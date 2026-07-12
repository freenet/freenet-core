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
    /// Shared contract instance index (`ContractInstanceId -> CodeHash`) so a
    /// contract stored / indexed / removed via any executor is visible to all
    /// the others (#4218). Cloned into every executor's `ContractStore` at
    /// construction and into replacements.
    shared_contract_index: SharedContractIndex,
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
    /// Bounds how many hosted-mode secret exports may run OFF-LOOP concurrently
    /// (#4381 P5). Each admitted export holds one permit (in its `ExportJob`)
    /// AND one pool executor for its duration. Sized to
    /// [`effective_export_permits`] (`min(MAX_CONCURRENT_EXPORTS, pool_size-1)`)
    /// so >=1 executor is ALWAYS reserved for normal contract ops — this is the
    /// enforced anti-deadlock invariant (see `effective_export_permits`), not a
    /// prose hope. `Arc` so an admitted `ExportJob` can hold an owned permit
    /// across the spawned background task without borrowing the pool.
    ///
    /// NOTE: this is EXPORT-only. The live secret IMPORT (#4592) deliberately
    /// runs ON the contract loop (serialized with delegate `store_secret`), not
    /// off-loop, so it takes no permit here — see the `import_secrets` impl and
    /// the `ImportSecrets` arm in `contract.rs` for why (the store write path
    /// assumes node-wide write serialization).
    export_semaphore: Arc<Semaphore>,
    /// Inactive-user reclaim sweep task (#4561, P5 of #4381). `Some` ONLY when
    /// hosted mode is on AND `per-user-inactive-ttl > 0`. The pool owns the
    /// abort guard so the sweep — which holds a clone of the shared ReDb
    /// `Storage` — is aborted when the pool is dropped on node shutdown, matching
    /// the #4401 teardown discipline for every other `Storage`-holding task.
    /// `None` on a Local single-user node, so that node spawns nothing and the
    /// reclaim machinery is entirely inert.
    _inactive_user_sweep: Option<crate::util::AbortOnDrop>,
}

/// Max hosted-mode secret exports allowed to run off-loop at once (#4381 P5),
/// BEFORE the pool-size clamp in [`effective_export_permits`].
///
/// Small and fixed: exports are an occasional self-host-migration action, not a
/// hot path, and each one holds a pool executor for its duration. The real cap
/// is `min(this, pool_size - 1)` so >=1 executor is ALWAYS free for normal ops.
/// A request that arrives while that many exports are in flight (or that can't
/// immediately reserve an executor) is rejected with a typed busy error (HTTP
/// 503), never queued on the contract loop.
pub(crate) const MAX_CONCURRENT_EXPORTS: usize = 2;

/// Effective off-loop export concurrency for a pool of `pool_size` executors.
///
/// `min(MAX_CONCURRENT_EXPORTS, pool_size - 1)` — this is the load-bearing
/// anti-deadlock invariant, ENFORCED in code (not prose): an admitted export
/// holds one pool executor off-loop, and the only executor-return path
/// (`finish_export`) runs ON the contract loop. If exports could hold EVERY
/// executor, a normal contract op would park the loop waiting for an executor
/// that only the (now-parked) loop can return — a permanent self-deadlock.
/// Reserving at least one executor for normal ops makes that impossible.
///
/// `pool_size == 1` → 0: exports are DISABLED on a single-executor node (they
/// return the typed Busy → 503). That is correct, not a regression — a tiny
/// 1-executor node has no spare executor to lend a deferred export anyway, and
/// the alternative (running it on the sole executor) is exactly the inline
/// blocking this whole change removes.
pub(crate) fn effective_export_permits(pool_size: usize) -> usize {
    MAX_CONCURRENT_EXPORTS.min(pool_size.saturating_sub(1))
}

/// Result of [`RuntimePool::try_begin_export`]: an admitted off-loop export job,
/// or a rejection because too many exports are already in flight, or because
/// this executor kind does not support export (mock executors).
pub(crate) enum ExportAdmission {
    /// Admitted. The contract-handling loop moves this `ExportJob` into a
    /// background task and calls [`ExportJob::run`] there; the resulting
    /// [`ExportDone`] comes back to the loop, which calls
    /// [`RuntimePool::finish_export`] to return/replace the executor. Boxed: an
    /// `ExportJob` owns a whole `Executor<Runtime>`, so keeping it inline would
    /// make every `ExportAdmission` (incl. the zero-size `Busy`/`Unsupported`)
    /// that large.
    Admitted(Box<ExportJob>),
    /// `MAX_CONCURRENT_EXPORTS` exports are already running. The caller answers
    /// the client with a typed busy error (HTTP 503); the export is NOT queued.
    Busy,
    /// This executor kind keeps no on-disk secrets and cannot export (mock /
    /// test executors). The caller answers with the not-supported error.
    Unsupported,
}

/// An admitted hosted-mode export, owning everything it needs to run OFF the
/// contract loop: an exclusively-checked-out pool executor, the export-
/// concurrency permit (released when the job is consumed by `run`), and the
/// owned inputs. Opaque to the loop, which just moves it into a background task.
pub(crate) struct ExportJob {
    executor: Executor<Runtime>,
    /// Held for the lifetime of the job so the export-concurrency slot is
    /// occupied until the work finishes; dropped at the end of `run`.
    _permit: tokio::sync::OwnedSemaphorePermit,
    user_context: UserSecretContext,
    /// Zeroized when the job drops, so the high-value token isn't left in memory.
    token: zeroize::Zeroizing<Vec<u8>>,
}

impl ExportJob {
    /// Run the export off the contract loop and produce an [`ExportDone`] to hand
    /// back to the loop. The CPU/IO work (enumerate + decrypt + AEAD-seal) runs
    /// via [`run_blocking_offloaded`], so on the production multi-thread runtime
    /// it lands on a blocking thread; a panic there is caught and surfaced as a
    /// lost executor (`ExportDone { executor: None, .. }`) rather than crashing.
    ///
    /// This is intended to be called from a spawned background task, NOT on the
    /// contract loop — that is the whole point of the deferral (#4531/#4381 P5).
    pub(crate) async fn run(self) -> ExportDone {
        let ExportJob {
            executor,
            _permit,
            user_context,
            token,
        } = self;
        match super::run_blocking_offloaded(executor, move |executor| {
            let result = executor.export_user_secrets(&user_context, token.as_slice());
            (executor, result)
        })
        .await
        {
            super::OffloadOutcome::Completed(executor, result) => ExportDone {
                executor: Some(executor),
                result,
            },
            super::OffloadOutcome::Panicked => ExportDone {
                executor: None,
                result: Err(ExecutorError::other(anyhow::anyhow!(
                    "secret export task panicked"
                ))),
            },
        }
        // `_permit` drops here, freeing the export-concurrency slot for the next
        // request. The executor (when `Some`) is returned to the pool by the loop
        // via `finish_export`.
    }
}

/// The completed export, handed back to the contract loop. Carries the result
/// for the client and the executor to return to the pool (`None` if the export
/// task panicked, in which case the loop replaces the lost executor).
pub(crate) struct ExportDone {
    executor: Option<Executor<Runtime>>,
    pub(crate) result: Result<Vec<u8>, ExecutorError>,
}

impl ExportDone {
    /// An `ExportDone` for the case where the off-loop export task was dropped /
    /// cancelled before producing a result (e.g. runtime teardown). Carries no
    /// executor — the job owned it and is gone — so the loop's `finish_export`
    /// treats it like the panic path (replace the lost slot) and answers the
    /// client with an error. Delivered by `ExportGuard::drop`.
    pub(crate) fn lost_to_drop() -> Self {
        Self {
            executor: None,
            result: Err(ExecutorError::other(anyhow::anyhow!(
                "secret export task was dropped before completion"
            ))),
        }
    }

    /// Consume the `ExportDone`, dropping any carried executor and returning just
    /// the result. Used by the trait default `finish_export` (no pool to return
    /// the executor to); the real `RuntimePool::finish_export` returns the
    /// executor to the pool instead of dropping it.
    pub(crate) fn into_result(self) -> Result<Vec<u8>, ExecutorError> {
        self.result
    }
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
        // Per-node telemetry sink shared with the `Ring` snapshot task (#4440 /
        // #4488). The caches publish occupancy/eviction into it; the snapshot
        // task reads the same `Arc` via `Ring`. Threading it (rather than a
        // process-global) keeps the gauges per-node. Both caches share one sink;
        // they're routed apart by their `"contract"` / `"delegate"` label.
        let module_cache_metrics = op_manager.ring.module_cache_metrics();
        // Interest predicate for the CONTRACT cache only: a contract is "of
        // interest" while `Ring::contract_in_use` holds (a live local client
        // subscription OR a downstream peer subscriber — deliberately NOT an
        // upstream-only subscription, which would be unbounded). This drives the
        // interest-weighted (two-tier) eviction policy AND the always-on shadow
        // metrics. The delegate cache has no interest concept, so it gets none
        // and stays pure byte-LRU. Capturing a clone of the `Arc<Ring>` keeps
        // `wasm_runtime` free of any `ring` dependency. See #4441 / #4534.
        let ring_for_interest = op_manager.ring.clone();
        let contract_interest: crate::wasm_runtime::InterestPredicate<ContractKey> =
            Arc::new(move |key: &ContractKey| ring_for_interest.contract_in_use(key));
        let shared_contract_modules: SharedModuleCache<ContractKey> =
            Arc::new(Mutex::new(ModuleCache::with_label_and_interest(
                contract_cache_budget,
                "contract",
                Some(module_cache_metrics.clone()),
                Some(contract_interest),
            )));
        let shared_delegate_modules: SharedModuleCache<DelegateKey> =
            Arc::new(Mutex::new(ModuleCache::with_label(
                delegate_cache_budget,
                "delegate",
                Some(module_cache_metrics.clone()),
            )));
        // Install the on-demand interest-shadow refresher so the `router_snapshot`
        // emitter can recompute the contract cache's interest split right before
        // it reads, keeping every emitted snapshot fresh even on an idle cache
        // (the throttled get/insert/remove refresh is unbounded on a quiet node).
        // The closure forces an un-throttled recompute on the SAME shared contract
        // cache the executors use. Cheap O(entries) scan, once per 5-min snapshot.
        //
        // It captures a WEAK handle (and upgrades at call time), NOT a strong
        // Arc: `ModuleCacheMetrics` is owned by `Ring`, each `ModuleCache` holds a
        // strong `Arc<ModuleCacheMetrics>`, and the cache's interest predicate
        // holds an `Arc<Ring>` — a strong clone here would close a reference cycle
        // (metrics → refresher → cache → metrics → … → ring) and leak the whole
        // runtime/ring on pool/node drop (simulations, tests, restarts). The Weak
        // breaks the cycle; when the cache is gone the refresh is a no-op.
        // (Codex review.)
        let refresher_cache = Arc::downgrade(&shared_contract_modules);
        module_cache_metrics.set_interest_shadow_refresher(Arc::new(move || {
            if let Some(cache) = refresher_cache.upgrade() {
                if let Ok(mut cache) = cache.lock() {
                    cache.force_refresh_interest_shadow();
                }
            }
        }));
        // Companion GAUGES-ONLY refresher used by the migration-admission gate:
        // recomputes the interest split for a fresh hot-occupancy read per inbound
        // SubscribeHint WITHOUT bumping the throttle-sampled would-reclassify
        // counter or resetting the throttle (Codex review). Same Weak-handle
        // cycle-break rationale as the snapshot refresher above.
        let gauges_refresher_cache = Arc::downgrade(&shared_contract_modules);
        module_cache_metrics.set_interest_gauges_refresher(Arc::new(move || {
            if let Some(cache) = gauges_refresher_cache.upgrade() {
                if let Ok(cache) = cache.lock() {
                    cache.force_refresh_interest_gauges_only();
                }
            }
        }));
        // Shared delegate-context cache so a prompt round-trip routed to a
        // different pool executor still finds its `ctx.write()` blob.
        let shared_delegate_contexts = crate::wasm_runtime::new_delegate_context_cache();

        // Create shared recovery guard for corrupted-state self-healing.
        // All pool executors share this so recovery tracking is consistent.
        let shared_recovery_guard: super::CorruptedStateRecoveryGuard =
            Arc::new(std::sync::Mutex::new(HashSet::new()));

        // Pool-owned contract instance index shared by every executor's
        // `ContractStore` (#4218). The first executor loads it from ReDb; the
        // rest inherit the same live `Arc`.
        let shared_contract_index: SharedContractIndex = Arc::new(DashMap::new());

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
            shared_contract_index.clone(),
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
                shared_contract_index.clone(),
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

        // Inactive-user reclaim sweep (#4561, P5 of #4381). Spawned ONLY in
        // hosted mode with a non-zero TTL — a Local single-user node spawns
        // nothing, never enumerates the `users/` tree, and so can never reclaim
        // its own (Local-scope) data. The sweep holds a clone of the SAME shared
        // ReDb `Storage` the executors use (single-writer per process), so we
        // pass `shared_state_store.storage()` rather than opening a second
        // database. Its abort guard is owned by the pool so node shutdown tears
        // it down (#4401 discipline).
        let inactive_user_sweep = if crate::wasm_runtime::should_spawn_inactive_user_sweep(
            config.ws_api.hosted_mode,
            config.per_user_inactive_ttl_secs,
        ) {
            crate::wasm_runtime::spawn_inactive_user_sweep(
                config.secrets_dir(),
                shared_state_store.storage(),
                config.per_user_inactive_ttl_secs,
                config.inactive_user_sweep_interval_secs,
            )
            .map(|handle| {
                let mut guard = crate::util::AbortOnDrop::new();
                guard.push(handle.abort_handle());
                guard
            })
        } else {
            None
        };

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
            shared_contract_index,
            shared_delegate_modules,
            shared_delegate_contexts,
            shared_backend_engine,
            shared_recovery_guard,
            delegate_notification_tx,
            delegate_notification_rx: Some(delegate_notification_rx),
            in_flight_contracts: HashMap::new(),
            export_semaphore: Arc::new(Semaphore::new(effective_export_permits(pool_size_usize))),
            _inactive_user_sweep: inactive_user_sweep,
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

    /// Non-blocking executor checkout: returns `Some(executor)` only if one is
    /// immediately available, `None` otherwise (NEVER awaits/parks the caller).
    ///
    /// Used by the off-loop export admission path (`try_begin_export`), which
    /// runs ON the contract loop and therefore must not block: if no executor is
    /// free right now, the export is rejected (Busy → 503) rather than parking
    /// the loop. Mirrors `pop_executor`'s permit/slot accounting exactly, just
    /// with a non-blocking `try_acquire`.
    fn try_pop_executor(&mut self) -> Option<Executor<Runtime>> {
        let permit = self.available.try_acquire().ok()?;
        // Take the slot BEFORE forgetting the permit: if (impossibly, given the
        // permit==Some-slot-count invariant) no slot is found, dropping `permit`
        // here restores it, so we never leak a permit on the unreachable path.
        let executor = self
            .runtimes
            .iter_mut()
            .find_map(|slot| slot.take())
            .or_else(|| {
                tracing::error!(
                    "try_pop_executor acquired a permit but found no executor slot \
                     (pool invariant violated)"
                );
                None
            })?;
        // Found a slot: now consume the permit (restored in `return_executor`).
        permit.forget();
        self.checked_out.fetch_add(1, Ordering::SeqCst);
        Some(executor)
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
            self.shared_contract_index.clone(),
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

        // Retract our hosting advertisement once ANY required on-disk half (state
        // or WASM code) is gone — i.e. on `Full` (both gone) OR `Partial` (one gone,
        // the other queued for retry): in either case we can no longer reliably
        // serve `key`, so we must stop advertising it (Fix 1, #4642 spec step 1;
        // invariant 1: advertise iff a fresh in-mesh host). `Err` (BOTH deletes
        // failed → nothing removed → still fully present) keeps the advertisement
        // and retries. Wired HERE on the confirmed-delete path, NOT at the eviction
        // decision, so the three guards above that DELIBERATELY skip the delete
        // (contract re-hosted / re-subscribed / written to a newer generation in
        // the eviction→reclaim window) also skip the retraction — a contract kept
        // alive by that race retains BOTH its state AND its advertisement.
        // Best-effort (`try_notify_node_event`), healed by the periodic re-request
        // on drop; idempotent, so a later Full retry after a Partial (or any repeat)
        // re-retracts harmlessly.
        if matches!(&result, Ok(ReclaimOutcome::Full | ReclaimOutcome::Partial)) {
            crate::operations::announce_contract_unhosted(self.op_manager.as_ref(), key);
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

    fn try_begin_export(
        &mut self,
        user_context: &UserSecretContext,
        token: &[u8],
    ) -> ExportAdmission {
        // FULLY NON-BLOCKING admission — this runs ON the contract loop, so it
        // must never await/park. Two non-blocking gates, in order:
        //
        // 1. Export-concurrency permit. Sized to `min(MAX_CONCURRENT_EXPORTS,
        //    pool_size-1)` so admitted exports can never hold every executor. On
        //    a 1-executor pool this is 0, so exports are disabled here. Over the
        //    cap → Busy (HTTP 503), never queued.
        let Ok(permit) = self.export_semaphore.clone().try_acquire_owned() else {
            return ExportAdmission::Busy;
        };
        // 2. A pool executor, taken NON-BLOCKING. If every executor is momentarily
        //    busy with normal ops, we do NOT await (that would park the loop, the
        //    whole #4531 deadlock) — we reject with Busy and the permit drops
        //    here (restoring the export slot). The pool-size-1 invariant means
        //    this only happens under genuine concurrent normal-op load, where a
        //    "retry shortly" 503 is the correct answer.
        let Some(executor) = self.try_pop_executor() else {
            return ExportAdmission::Busy;
        };

        // Own the inputs so the spawned task's closure is `'static`. The token is
        // copied into a Zeroizing buffer wiped when the job drops it, so the
        // high-value credential is not left in an un-zeroized Vec.
        ExportAdmission::Admitted(Box::new(ExportJob {
            executor,
            _permit: permit,
            user_context: user_context.clone(),
            token: zeroize::Zeroizing::new(token.to_vec()),
        }))
    }

    async fn finish_export(&mut self, done: ExportDone) -> Result<Vec<u8>, ExecutorError> {
        let ExportDone { executor, result } = done;
        match executor {
            // Normal path: the export ran (success or a clean export error) and
            // handed the executor back. Return it through the health-checked path
            // (it stays healthy — a read-only walk runs no WASM — but this keeps
            // the accounting identical to every other op).
            Some(executor) => {
                self.return_checked(executor, "export_user_secrets").await;
            }
            // The offloaded export task PANICKED: the executor was owned by the
            // unwinding blocking thread and is gone. Build a replacement and
            // return THAT so the pool's permit is restored and capacity is not
            // permanently leaked — exactly the "health check replaces a broken
            // executor" semantic, here for a lost-to-panic executor.
            None => {
                tracing::error!("export offload task panicked; replacing the lost pool executor");
                match self.create_replacement_executor().await {
                    // `return_executor` fills the slot the lost executor vacated
                    // and restores the pool permit (sub checked_out, add_permits).
                    Ok(replacement) => self.return_executor(replacement),
                    Err(e) => {
                        // Could not build a replacement (a serious node problem on
                        // its own). Do NOT restore the permit: leaving the slot
                        // empty WITHOUT a matching permit keeps the pool invariant
                        // `available permits == count of Some(executor) slots`, so
                        // `pop_executor` can never acquire a permit and then find
                        // no executor (its `unreachable!`). The pool is one
                        // executor smaller until restart; that's the safe choice
                        // versus a latent slot/permit mismatch.
                        tracing::error!(
                            error = %e,
                            "failed to replace export-panicked executor; pool capacity reduced by one"
                        );
                        self.checked_out.fetch_sub(1, Ordering::SeqCst);
                    }
                }
            }
        }
        result
    }

    /// Run a live secret import ON the contract loop (#4592). DELIBERATELY
    /// on-loop, NOT off-loop like the export: the import WRITES to the
    /// `SecretsStore`, whose write path (fixed sibling `.tmp` + check-then-write)
    /// assumes node-wide write serialization. Checking out a pooled executor and
    /// running the import here — awaited by the single-threaded contract loop —
    /// keeps every secret write in one serialization domain (with delegate
    /// `store_secret`), so an import can never race another writer on the same
    /// secret file (Codex P1). The heavy work is acceptable on the loop because
    /// the endpoint is loopback + dashboard-gated (operator's one-shot migration),
    /// not the authenticated-remote DoS surface that justified off-loop export.
    ///
    /// `pop_executor().await` always succeeds promptly: an in-flight export holds
    /// at most `pool_size-1` executors, so >=1 is always free for the loop.
    async fn import_secrets(
        &mut self,
        target_scope: crate::contract::handler::ImportTargetScope,
        bundle: &[u8],
        key: &[u8],
        key_kind: crate::contract::handler::BundleKeyKind,
        overwrite: bool,
    ) -> Result<crate::wasm_runtime::secret_export::ImportReport, ExecutorError> {
        use crate::contract::handler::{BundleKeyKind, ImportTargetScope};
        use crate::wasm_runtime::secret_export::{BundleKeyMaterial, TargetScope};
        let mut executor = self.pop_executor().await;
        let scope = match target_scope {
            ImportTargetScope::Local => TargetScope::Local,
        };
        let material = match key_kind {
            BundleKeyKind::Token => BundleKeyMaterial::Token(key),
            BundleKeyKind::Passphrase => BundleKeyMaterial::Passphrase(key),
        };
        // Synchronous decrypt + per-secret write. No `.await` between checkout and
        // return, so the executor is held for exactly this op.
        let result = executor.import_secrets(&scope, bundle, &material, overwrite);
        // The import runs no WASM, so the executor stays healthy; route it back
        // through the health-checked return for accounting parity with every op.
        self.return_checked(executor, "import_secrets").await;
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{ConfigArgs, GlobalTestMetrics};
    use freenet_stdlib::prelude::{
        ContractCode, ContractContainer, ContractKey, ContractWasmAPIVersion, Parameters,
        WrappedContract, WrappedState,
    };
    use std::num::NonZeroUsize;
    use std::sync::Arc;

    /// Synthetic contract container. The bytes are never executed
    /// (`remove_contract` only deletes files / DB rows), so a fake blob is
    /// sufficient — mirrors `runtime.rs`'s `make_contract` helper.
    fn make_contract(code_seed: u8, param_seed: u8) -> (ContractContainer, ContractKey) {
        let code = ContractCode::from(vec![code_seed; 64]);
        let params = Parameters::from(vec![param_seed; 8]);
        let key = ContractKey::from_params_and_code(&params, &code);
        let wrapped = WrappedContract::new(Arc::new(code), params);
        let container = ContractContainer::Wasm(ContractWasmAPIVersion::V1(wrapped));
        (container, key)
    }

    /// End-to-end proof that evicting an advertised contract through the real
    /// `RuntimePool::remove_contract` path retracts the hosting advertisement
    /// and records `GlobalTestMetrics::neighbor_hosting_retractions()` — giving
    /// that (otherwise unread) counter a real reader.
    ///
    /// The path exercised: `RuntimePool::remove_contract` → reclaim on-disk
    /// storage (`ReclaimOutcome::Full`) → `announce_contract_unhosted` →
    /// `NeighborHostingManager::on_contract_unhosted` →
    /// `record_neighbor_hosting_retraction()`.
    ///
    /// Must run on a `current_thread` runtime (the `#[tokio::test]` default):
    /// the retraction counter is a `thread_local!`, and `remove_contract` (with
    /// its synchronous `announce_contract_unhosted` call) is polled on the same
    /// thread the assertion reads from. A `multi_thread` runtime could poll the
    /// future on a worker thread whose thread-local the assertion would not see.
    ///
    /// This CANNOT be exercised via `SimNetwork`: the simulation contract
    /// handlers use the direct inner `Executor` (no `op_manager`, no
    /// retraction); only the real `RuntimePool` used by `NetworkContractHandler`
    /// wires the retraction. So the test builds a real `OpManager` + `RuntimePool`
    /// directly.
    #[tokio::test]
    async fn test_eviction_emits_hosting_retraction() {
        // Reset thread-local counters at the start of the (current_thread) test.
        GlobalTestMetrics::reset();

        // --- Build a real OpManager backed by a temp-dir Config -----------
        // Mirrors the wiring in `node::testing_impl::in_memory::Builder`, minus
        // the node event loop. `id: Some(..)` isolates the on-disk state in a
        // fresh temp dir (cleaned on each run — see `ConfigPathsArgs::default_dirs`).
        let config_args = ConfigArgs {
            id: Some("evict-retraction-step1".to_string()),
            mode: Some(OperationMode::Local),
            ..Default::default()
        };
        let node_config =
            crate::node::NodeConfig::new(config_args.build().await.expect("build Config"))
                .await
                .expect("build NodeConfig");
        let config = node_config.config.clone();

        // Keep the receivers / task monitor alive for the whole test (the
        // `_`-prefixed bindings live to end of scope) so the OpManager's
        // channels and background sweeps aren't torn down mid-run.
        let (_notification_rx, notification_tx) = crate::node::event_loop_notification_channel();
        let (ops_ch_channel, _ch_channel, _wait_for_event) =
            crate::contract::contract_handler_channel();
        let connection_manager = crate::ring::ConnectionManager::new(&node_config);
        let (result_router_tx, _result_router_rx) = tokio::sync::mpsc::channel(100);
        let task_monitor = crate::node::background_task_monitor::BackgroundTaskMonitor::new();

        let op_manager = Arc::new(
            OpManager::new(
                notification_tx,
                ops_ch_channel,
                &node_config,
                // Empty no-op event register — the eviction path emits no
                // NetEventLog we need to capture here.
                crate::tracing::DynamicRegister::new(vec![]),
                connection_manager,
                result_router_tx,
                &task_monitor,
            )
            .expect("build OpManager"),
        );
        op_manager.ring.attach_op_manager(&op_manager);

        // --- Build a real RuntimePool over that OpManager -----------------
        let mut pool = RuntimePool::new(
            config.clone(),
            op_manager.clone(),
            NonZeroUsize::new(1).expect("nonzero pool size"),
        )
        .await
        .expect("build RuntimePool");

        // --- Store a contract (state + wasm) on disk through the pool -----
        let (container, key) = make_contract(0x11, 0x22);
        let params = container.params();
        let state = WrappedState::new(b"hosted state payload".to_vec());

        // WASM blob: stored via a pool executor's ContractStore. All executors
        // share the pool's single ReDb handle and on-disk contracts dir, so the
        // executor reclaimed by `remove_contract` sees it.
        pool.runtimes[0]
            .as_mut()
            .expect("pool executor present")
            .runtime
            .contract_store
            .store_contract(container)
            .expect("store contract code");
        // State: stored via the pool's shared StateStore — the very store the
        // checked-out executor reclaims from (clones share the ReDb + caches).
        pool.shared_state_store
            .store(key, state.clone(), params)
            .await
            .expect("store contract state");

        assert_eq!(
            pool.shared_state_store
                .get(&key)
                .await
                .expect("state present before eviction"),
            state,
            "stored state must round-trip before eviction",
        );

        // Register the contract as an advertised host — the retraction (and its
        // counter) only fire when the contract is in `neighbor_hosting.my_contracts`.
        op_manager.neighbor_hosting.on_contract_hosted(&key);
        assert!(
            op_manager.neighbor_hosting.is_hosted_locally(&key),
            "contract advertised as hosted before eviction",
        );

        // The three `remove_contract` guards must NOT bail:
        //   1. is_hosting_contract(key) — false; we never touched the ring hosting cache.
        //   2. contract_in_use(key)     — false; no subscription/downstream interest.
        //   3. state_generation(key)    — pass it as expected_generation so it matches.
        assert!(
            !op_manager.ring.is_hosting_contract(&key),
            "guard precondition: contract must not be in the ring hosting cache",
        );
        assert!(
            !op_manager.ring.contract_in_use(&key),
            "guard precondition: contract must not be in use",
        );
        let expected_generation = op_manager.ring.state_generation(&key);

        // --- Evict via the real RuntimePool::remove_contract path ---------
        let before = GlobalTestMetrics::neighbor_hosting_retractions();
        pool.remove_contract(&key, expected_generation)
            .await
            .expect("remove_contract reclaim must succeed");
        let after = GlobalTestMetrics::neighbor_hosting_retractions();

        // (i) On-disk state is gone.
        match pool.shared_state_store.get(&key).await {
            Err(StateStoreError::MissingContract(missing)) => assert_eq!(missing, key),
            other => panic!("expected MissingContract after eviction, got {other:?}"),
        }
        // ...and the advertisement was retracted from the local hosted set.
        assert!(
            !op_manager.neighbor_hosting.is_hosted_locally(&key),
            "eviction must retract the hosting advertisement",
        );

        // (ii) The retraction counter advanced by exactly one — the real reader
        // this test exists to provide.
        assert_eq!(
            after,
            before + 1,
            "eviction of an advertised contract must emit exactly one hosting retraction",
        );

        // Explicitly hold the guards to end of scope.
        drop(task_monitor);
    }
}
