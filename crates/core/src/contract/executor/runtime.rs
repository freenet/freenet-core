mod contract_ops;
mod delegates;
mod executor_impl;
mod pool;
mod subscriptions;

use super::*;
use super::{
    ContractExecutor, ContractRequest, ContractResponse, ExecutorError, InitCheckResult,
    RequestError, Response, SLOW_INIT_THRESHOLD, STALE_INIT_THRESHOLD, StateStoreError, now_nanos,
};
use crate::wasm_runtime::default_wasmtime_cache_size_bytes_for_dir;
pub(crate) use contract_ops::ReclaimOutcome;
pub use pool::RuntimePool;
pub(crate) use pool::{ExportAdmission, ExportDone, MAX_CONCURRENT_EXPORTS};

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

/// Maximum successive re-applies the deterministic identical-input probe
/// (`Executor::probe_identical_input_idempotency`) runs while looking for a
/// fixpoint. The contract is flagged only if EVERY one of these applies
/// changes the byte multiset again (never stabilizes): one re-apply
/// suffices for a healthy contract (immediate fixpoint), a legitimate
/// canonicalizing contract stabilizes on the second, so requiring three
/// successive content changes leaves generous room for one-shot
/// normalization while still deterministically catching a contract that
/// mutates on every apply. Cost is bounded by the per-contract
/// `IDENTITY_PROBE_COOLDOWN` claim, and only a genuinely churning contract
/// ever pays all three.
const IDENTITY_PROBE_MAX_APPLIES: usize = 3;

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
    BackendEngine, MAX_STATE_SIZE, ModuleCache, RuntimeConfig, SharedModuleCache, SharedStores,
    UserSecretContext,
};

use dashmap::DashMap;
use freenet_stdlib::prelude::{MessageOrigin, RelatedContract};
use std::collections::{HashMap, HashSet};

/// Whether the production `RuntimePool` requests offloading cache-miss WASM
/// compiles to a blocking thread.
///
/// Always `true`: the production pool *opts in* to offload so a cold-contract
/// Cranelift compile doesn't stall the current worker's other tasks (issue
/// #4441's whole-node HANG). Whether the offload actually happens is decided
/// at compile time from the LIVE runtime flavor inside
/// `wasmtime_engine::compile_offloaded`: it offloads only on a multi-thread
/// runtime and compiles INLINE under a current_thread runtime (the sim runner
/// and `current_thread` integration tests) or with no runtime at all.
///
/// This is why correctness does NOT rest on `cfg!(test)`. The previous
/// `!cfg!(test)` gate was wrong: in an integration-test crate the freenet lib
/// is compiled *without* `cfg(test)`, so the gate evaluated to `true` and
/// turned offload on under the `current_thread` runtime of
/// `error_notification::test_connection_drop_error_notification`, panicking at
/// `block_in_place`. The runtime-flavor check in the engine is the real safety
/// net; this flag is just the explicit production opt-in.
fn production_offload_compilation() -> bool {
    true
}

/// Outcome of [`run_blocking_offloaded`].
///
/// Distinguishes the normal case (work completed, the moved-in value `T` came
/// back) from a panic inside the offloaded closure. On panic the `spawn_blocking`
/// thread unwound while owning `value`, so it is GONE and cannot be returned —
/// the caller must reconcile a lost resource (for the export path: replace the
/// lost pool executor so the permit is restored, and fail just that one export).
pub(crate) enum OffloadOutcome<T, R> {
    /// The closure ran to completion; `value` is returned alongside the result.
    Completed(T, R),
    /// The offloaded closure PANICKED. `value` was owned by the unwinding
    /// blocking thread and is unrecoverable.
    Panicked,
}

/// Run a synchronous, potentially-long CPU/IO closure OFF the contract-handling
/// loop when we're on a multi-threaded runtime, or INLINE otherwise.
///
/// The contract-handling loop is single-threaded and processes one event at a
/// time, so any synchronous work done inside an event handler blocks every
/// other contract op (GET/PUT/UPDATE/delegate) for its full duration. The
/// hosted-mode secret export (`export_user_secrets`) enumerates, decrypts, and
/// re-encrypts every secret in a user's scope synchronously, so an authenticated
/// token-holder with a large secret set could otherwise wedge the loop (the
/// #4381 P5 DoS). Moving that work onto a blocking thread keeps the loop free.
///
/// `f` takes ownership of `value` (the checked-out executor) and returns it
/// alongside the result, so the caller can return the executor to the pool
/// afterwards. `value` is exclusively checked out for the whole call.
///
/// PANIC SAFETY: mirrors `wasmtime_engine::compile_offloaded` (#4441) — a panic
/// inside the offloaded closure is CAUGHT (`JoinError::is_panic()`) and reported
/// as [`OffloadOutcome::Panicked`], NOT re-raised. Re-raising would propagate the
/// panic onto the *caller's* task; for the export caller (the contract-handling
/// loop / a loop-spawned task) that would abort the loop and, via the node's
/// top-level `select!`, shut down the WHOLE node — and leak the executor's pool
/// slot. Catching it lets the caller fail just that one export and reconcile the
/// lost slot.
///
/// Runtime-flavor gate: offload (multi-thread) vs inline (`current_thread` / no
/// runtime, where `spawn_blocking` + a blocking `await` is unnecessary and the
/// sim/test runners want a deterministic inline run). On the inline path a panic
/// propagates normally (there is no thread boundary to catch it at), exactly as
/// it would have without this helper.
async fn run_blocking_offloaded<T, R>(
    value: T,
    f: impl FnOnce(T) -> (T, R) + Send + 'static,
) -> OffloadOutcome<T, R>
where
    T: Send + 'static,
    R: Send + 'static,
{
    use tokio::runtime::RuntimeFlavor;
    match tokio::runtime::Handle::try_current() {
        Ok(handle) if handle.runtime_flavor() == RuntimeFlavor::MultiThread => {
            match tokio::task::spawn_blocking(move || f(value)).await {
                Ok((value, result)) => OffloadOutcome::Completed(value, result),
                Err(e) if e.is_panic() => {
                    // The blocking thread unwound while owning `value`; it is
                    // lost. Report Panicked so the caller replaces the slot and
                    // fails this one operation, rather than crashing the node.
                    tracing::error!("offloaded export task panicked");
                    OffloadOutcome::Panicked
                }
                // Cancellation: the runtime is shutting down. Treat like a panic
                // (value gone), but this only happens at teardown.
                Err(e) => {
                    tracing::error!(error = %e, "offloaded export task failed (cancelled)");
                    OffloadOutcome::Panicked
                }
            }
        }
        // current_thread runtime (sim / current_thread integration tests) or no
        // tokio runtime: run inline. There is no thread boundary, so a panic
        // here propagates exactly as it would without the offload.
        _ => {
            let (value, result) = f(value);
            OffloadOutcome::Completed(value, result)
        }
    }
}
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
/// Callers resolve the real `ContractKey` (via `lookup_key` /
/// `bridged_lookup_key`) before calling this, so the client can tell which
/// contract was refused. The cause string carries the real rejection reason.
fn subscriber_limit_error(key: ContractKey, cause: &str) -> Box<RequestError> {
    Box::new(RequestError::ContractError(StdContractError::Subscribe {
        key,
        cause: cause.to_string().into(),
    }))
}

/// Fallback key for `subscriber_limit_error` when the real `ContractKey`
/// can't be resolved from a `ContractInstanceId` (the code hash isn't
/// registered — shouldn't happen for a contract a client is actively
/// subscribing to, but the registration path only has the instance id to
/// begin with, so this is the honest degradation rather than a panic).
/// Zeroed `CodeHash` is the documented sentinel used elsewhere for the same
/// situation (see `operations::get::op_ctx_task::synthetic_key`).
fn synthetic_key(instance_id: ContractInstanceId) -> ContractKey {
    ContractKey::from_id_and_code(
        instance_id,
        freenet_stdlib::prelude::CodeHash::new([0u8; 32]),
    )
}

// ============================================================================
// ContractExecutor for Executor<Runtime> - delegates to bridged methods
// ============================================================================

impl ContractExecutor for Executor<Runtime> {
    fn lookup_key(&self, instance_id: &ContractInstanceId) -> Option<ContractKey> {
        self.bridged_lookup_key(instance_id)
    }

    fn op_manager_handle(&self) -> Option<Arc<crate::node::OpManager>> {
        self.op_manager.clone()
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

    async fn upsert_contract_state_deferrable(
        &mut self,
        key: ContractKey,
        update: Either<WrappedState, StateDelta<'static>>,
        related_contracts: RelatedContracts<'static>,
        code: Option<ContractContainer>,
    ) -> Result<UpsertOutcome, ExecutorError> {
        bridged_upsert_outcome(
            self.bridged_upsert_contract_state_inner(key, update, related_contracts, code, true)
                .await,
        )
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
        connection_scope: crate::client_events::ConnectionScope,
        user_context: Option<&UserSecretContext>,
    ) -> Response {
        self.delegate_request(
            req,
            origin_contract,
            caller_delegate,
            connection_scope,
            user_context,
        )
    }

    // NOTE: `ContractExecutor::try_begin_export` / `finish_export` are NOT
    // overridden for a bare `Executor<Runtime>` — only the pooled `RuntimePool`
    // can admit a deferred export (it owns the executor pool + the export
    // concurrency semaphore needed to check one out and return it). A bare
    // executor (tests / direct use, never the production hosted path) falls
    // through to the trait default (`ExportAdmission::Unsupported`). The inherent
    // `Executor::export_user_secrets` (delegates.rs) remains as the work
    // function the pool's `ExportJob::run` calls.

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

/// The production demand-registration callback for V2 delegate
/// `subscribe_contract()` (#4669 part 1 / #5467 Phase 1).
///
/// **There is one of these on purpose**, shared by every `Executor<Runtime>`
/// constructor, rather than an inline closure per constructor. The V2 host
/// function runs synchronously inside the WASM call and records only a
/// notification hook in `DELEGATE_SUBSCRIPTIONS`, which nothing in `ring/`
/// reads — so a constructor that omits this callback produces an executor on
/// which a V2 delegate subscribe silently registers no demand, while the V1
/// path (`contract.rs`, the `SubscribeContractRequest` arm) still does. One
/// shared value means the two constructors cannot drift from each other, and
/// both converge with V1 on `delegate_demand::register_subscription`.
///
/// Returns `None` when there is no `OpManager` (local-only and mock executors),
/// which have no demand machinery to register with; the notification half still
/// works there, so the absence degrades to exactly the pre-#4669 behaviour.
///
/// Shaped deliberately like `v2_delegate_state_write_callback` (#5479/#5490),
/// which collapses the same per-constructor duplication for the state-write
/// callback. Keeping the two in the same shape is what stops this file
/// re-accumulating one bespoke inline closure per callback.
pub(super) fn v2_delegate_subscribe_callback(
    op_manager: Option<Arc<crate::node::OpManager>>,
) -> Option<crate::wasm_runtime::DelegateSubscribeCallback> {
    let op_manager = op_manager?;
    Some(Arc::new(
        move |delegate: &freenet_stdlib::prelude::DelegateKey, key: &ContractKey| {
            crate::contract::delegate_demand::register_subscription(&op_manager, delegate, key);
        },
    ))
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
        // V2 delegate state writes (put/update_contract_state_sync) write
        // directly through the raw `Storage`, bypassing the executor's
        // `state_store.{store,update}` chokepoints. The callback restores the
        // side effects those chokepoints perform on every write:
        //   1. Drop StateStore's cached view of the contract — ALWAYS. The
        //      bypass write doesn't touch the moka state-bytes cache OR the
        //      change-detector, so without this a later read would serve the OLD
        //      bytes from moka and the summarize/delta fast path could serve a
        //      STALE summary/delta → state divergence (Codex review).
        //   2. Bump+refresh+report via `Ring::commit_state_write` — only when an
        //      op_manager is present (it owns the ring/governance state).
        //      Without this, V2 PUT/UPDATE would leave the EvictContract re-host
        //      race open AND undercount StateBytesWritten in the meter.
        let cache_invalidator = state_store.cache_invalidator();
        let op_manager_for_cb = op_manager.clone();
        rt.set_state_write_callback(Arc::new(move |key: &ContractKey, state_size: usize| {
            cache_invalidator.invalidate(key);
            if let Some(op_manager) = &op_manager_for_cb {
                op_manager.ring.commit_state_write(key, state_size);
            }
        }));
        // Disk-budget admission gate for the V2 delegate write path (#4683,
        // PR 3): V2 PUT/UPDATE bypass the executor's `state_store` chokepoints
        // (and hence the gate installed there), so install the same pre-write
        // gate here. Returns Err(cause) → the native-API method aborts without
        // writing. No-op admit until the disk tracker is seeded.
        let op_manager_for_admit = op_manager.clone();
        if let Some(op_manager) = op_manager_for_admit {
            rt.set_state_admit_callback(Arc::new(
                move |key: &ContractKey, state_size: usize, is_update: bool| {
                    // V2 PUT → hard gate; V2 UPDATE → growth-only gate (#4683).
                    // A shrinking/holding V2 UPDATE must never block convergence.
                    let result = if is_update {
                        op_manager.ring.admit_state_update(key, state_size)
                    } else {
                        op_manager.ring.admit_state_write(key, state_size)
                    };
                    result.map_err(|over| over.to_string())
                },
            ));
        }
        // Demand registration for V2 delegate `subscribe_contract()` (#4669
        // part 1 / #5467 Phase 1). See `v2_delegate_subscribe_callback`.
        if let Some(callback) = v2_delegate_subscribe_callback(op_manager.clone()) {
            rt.set_delegate_subscribe_callback(callback);
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
        contract_modules: SharedModuleCache<CodeHash>,
        delegate_modules: SharedModuleCache<CodeHash>,
        delegate_contexts: crate::wasm_runtime::DelegateContextCache,
        created_delegates_count: crate::wasm_runtime::SharedDelegateCounter,
        inherited_origins: crate::wasm_runtime::SharedInheritedOrigins,
        shared_backend: Option<BackendEngine>,
        shared_stores: SharedStores,
    ) -> anyhow::Result<Self> {
        let db = shared_state_store.storage();
        // Pool executors all share ONE contract instance index (#4218), so a
        // contract stored / indexed / removed via any executor is visible to
        // every other executor's `ContractStore`.
        let (contract_store, delegate_store, secret_store) =
            Self::get_runtime_stores(&config, db.clone(), Some(shared_stores))?;
        // Production RuntimeConfig: opt in to compile offload so a cold-contract
        // Cranelift compile can run on a blocking thread instead of stalling the
        // current worker's other tasks (issue #4441). Whether the offload
        // actually happens is decided from the live runtime flavor inside
        // `wasmtime_engine::compile_offloaded` — it offloads only on a
        // multi-thread runtime and compiles inline under a current_thread / no
        // runtime, so this stays deterministic in the sim runner and never
        // panics in `current_thread` integration tests. The byte budget here
        // also threads into the backend (though the *shared* cache size comes
        // from the caches passed in by RuntimePool::new).
        // Only probe disk / size the compile-cache soft limit when we're about
        // to build a NEW backend engine (`shared_backend.is_none()`) — that is
        // the ONLY branch that reads `wasmtime_cache_dir`/`wasmtime_cache_size_bytes`
        // (inside `create_backend_engine`, see below). Computing it
        // unconditionally would run `default_wasmtime_cache_size_bytes_for_dir`'s
        // startup reconciliation (#5014) for every pool worker AND on every
        // mid-life `create_replacement_executor` call (panic recovery) — the
        // latter passes `shared_backend: Some(..)`, so it would run
        // `reconcile_existing_cache_dir`'s directory walk / possible
        // `remove_dir_all` against a directory the LIVE, already-in-use shared
        // engine is actively reading/writing, exactly when the cache is most
        // likely to be genuinely populated. Gating on `is_none()` makes this
        // run exactly once per node, only for the executor that actually
        // builds the engine, matching the doc comment on
        // `default_wasmtime_cache_size_bytes_for_dir` (#5328 review).
        let wasmtime_cache_dir = config.wasmtime_cache_dir();
        let wasmtime_cache_size_bytes = shared_backend.is_none().then(|| {
            default_wasmtime_cache_size_bytes_for_dir(
                &wasmtime_cache_dir,
                config.hosting_disk_pct,
                config.max_hosting_disk,
            )
        });
        let runtime_config = RuntimeConfig {
            offload_compilation: production_offload_compilation(),
            module_cache_budget_bytes: config.module_cache_budget_bytes,
            // Relocate the wasmtime compile cache onto the data-dir mount and
            // pin its soft-size limit (#4683) so it lives on the mount whose
            // free space sizes the disk budget and is measurable as freenet's
            // own on-disk usage. `with_directory` requires an absolute path;
            // the data dir is absolute. The soft limit is bounded by BOTH the
            // memory the node may use AND the disk actually free on that mount
            // (#5014), so a small/containerized node no longer gets a compile
            // cache larger than the contract state it accelerates, AND a
            // disk-tight-but-RAM-rich host no longer gets a cache the disk
            // budget can't actually afford — see
            // `default_wasmtime_cache_size_bytes_for_dir`.
            wasmtime_cache_dir: Some(wasmtime_cache_dir),
            wasmtime_cache_size_bytes,
            ..RuntimeConfig::default()
        };
        let mut rt = Runtime::build_with_shared_module_caches(
            contract_store,
            delegate_store,
            secret_store,
            false,
            contract_modules,
            delegate_modules,
            delegate_contexts,
            created_delegates_count,
            inherited_origins,
            shared_backend.unwrap_or_else(|| {
                // First executor — create a fresh backend engine; RuntimePool
                // will extract and share it with subsequent executors.
                crate::wasm_runtime::engine::Engine::create_backend_engine(&runtime_config)
                    .expect("Failed to create WASM backend engine")
            }),
            &runtime_config,
        )
        .unwrap();
        rt.set_state_store_db(db);
        // V2 delegate state writes bypass the executor chokepoints — install the
        // callback that (1) ALWAYS drops StateStore's cached view of the contract
        // (both the moka state-bytes cache and the change-detector; a stale
        // cached state or detector hash after a V2 write would serve a stale
        // summary/delta → divergence; Codex review) and (2) mirrors the
        // bump+refresh+report side effects via `Ring::commit_state_write` when an
        // op_manager is present. See `from_config` and
        // `Runtime::set_state_write_callback`.
        let cache_invalidator = shared_state_store.cache_invalidator();
        let op_manager_for_cb = op_manager.clone();
        rt.set_state_write_callback(Arc::new(move |key: &ContractKey, state_size: usize| {
            cache_invalidator.invalidate(key);
            if let Some(op_manager) = &op_manager_for_cb {
                op_manager.ring.commit_state_write(key, state_size);
            }
        }));
        // Disk-budget admission gate for the V2 delegate write path (#4683,
        // PR 3) — see `from_config` for the rationale. Same gate the executor
        // chokepoints apply, restored for the V2 bypass.
        let op_manager_for_admit = op_manager.clone();
        if let Some(op_manager) = op_manager_for_admit {
            rt.set_state_admit_callback(Arc::new(
                move |key: &ContractKey, state_size: usize, is_update: bool| {
                    // V2 PUT → hard gate; V2 UPDATE → growth-only gate (#4683).
                    // A shrinking/holding V2 UPDATE must never block convergence.
                    let result = if is_update {
                        op_manager.ring.admit_state_update(key, state_size)
                    } else {
                        op_manager.ring.admit_state_write(key, state_size)
                    };
                    result.map_err(|over| over.to_string())
                },
            ));
        }
        // Demand registration for V2 delegate `subscribe_contract()` (#4669
        // part 1 / #5467 Phase 1). See `v2_delegate_subscribe_callback`.
        if let Some(callback) = v2_delegate_subscribe_callback(op_manager.clone()) {
            rt.set_delegate_subscribe_callback(callback);
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
            // Local-node path (no hosted-mode connection): always single-user
            // (`user_context = None`), so secrets stay `SecretScope::Local`.
            // `origin_contract` is `None` here, so the connection scope changes
            // nothing about what this path can attest — pass `Local` so the
            // behavior is byte-for-byte what it was.
            ClientRequest::DelegateOp(op) => self.delegate_request(
                op,
                None,
                None,
                crate::client_events::ConnectionScope::Local,
                None,
            ),
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
                // Reject debug-compiled contracts (#2257). The network
                // client path guards this in `process_open_request`, but
                // local-node mode (`run_local_node`, the local server
                // loop, `preload`, `handle_request`) PUTs straight through
                // here — and local development is the most likely place to
                // hand the node a debug build. Debug WASM carries DWARF
                // `.debug_*` sections and is 10-100x larger than release,
                // so without this guard it surfaces as an opaque
                // "Message too long" error instead of an actionable one.
                let key = contract.key();
                if crate::contract::contains_debug_sections(contract.data()) {
                    let sections = crate::contract::debug_sections(contract.data()).join(", ");
                    return Err(ExecutorError::request(StdContractError::Put {
                        key,
                        cause: format!(
                            "contract appears to be compiled in debug mode \
                             (contains {sections} section(s)). Debug WASM is \
                             typically 10-100x larger than release builds and \
                             may exceed message-size limits. Recompile the \
                             contract with `--release` before publishing."
                        )
                        .into(),
                    }));
                }
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

/// Map the result of a deferrable `bridged_upsert_contract_state_inner` call
/// into an [`UpsertOutcome`].
///
/// A clean completion becomes [`UpsertOutcome::Completed`]; the typed
/// [`ExecutorError::defer_related_fetch`] signal becomes
/// [`UpsertOutcome::DeferRelated`]; any other error propagates unchanged.
pub(super) fn bridged_upsert_outcome(
    result: Result<UpsertResult, ExecutorError>,
) -> Result<UpsertOutcome, ExecutorError> {
    match result {
        Ok(res) => Ok(UpsertOutcome::Completed(res)),
        Err(err) => match err.into_defer_related_fetch() {
            Ok(missing) => Ok(UpsertOutcome::DeferRelated(missing)),
            Err(other) => Err(other),
        },
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

/// Test hook for the OTHER network-fetch leg: `local_state_or_from_network`.
///
/// Distinct from [`NetworkFetchStub`] because the two legs return different
/// things and only this one can install a contract. `fetch_related_via_network`
/// resolves a bare state; `local_state_or_from_network` resolves a whole
/// `GetResult`, and it is the `GetResult::contract` half that lets
/// `get_updated_state` install a SECOND contract mid-UPDATE — the site whose
/// post-store fan-out #5481 is about. Returning `None` stands for
/// `SubOpGetOutcome::NotFound`.
#[cfg(test)]
pub(crate) type SubOpGetStub =
    std::rc::Rc<dyn Fn(ContractInstanceId) -> Option<crate::operations::get::GetResult>>;

#[cfg(test)]
thread_local! {
    /// Test hook used by `local_state_or_from_network` to bypass the real
    /// network sub-op driver. Set with [`set_test_sub_op_get_override`]
    /// inside a `#[tokio::test(flavor = "current_thread")]` so the
    /// thread-local lookup hits the same task that ran the test setup.
    static TEST_SUB_OP_GET_OVERRIDE: std::cell::RefCell<Option<SubOpGetStub>> =
        const { std::cell::RefCell::new(None) };
}

#[cfg(test)]
pub(crate) fn set_test_sub_op_get_override(stub: Option<SubOpGetStub>) {
    TEST_SUB_OP_GET_OVERRIDE.with(|cell| *cell.borrow_mut() = stub);
}

#[cfg(test)]
mod executor_pin_tests {
    /// Pin: `local_state_or_from_network` MUST use the sub-op GET
    /// driver.
    #[test]
    fn local_state_or_from_network_uses_sub_op_driver() {
        // `local_state_or_from_network` lives in runtime/subscriptions.rs after the split.
        let src = include_str!("runtime/subscriptions.rs");
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
        // `executor::subscribe` lives in runtime/subscriptions.rs after the split.
        let src = include_str!("runtime/subscriptions.rs");
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

    /// Source pin for freenet/freenet-core#4978: BOTH UPDATE apply paths must
    /// resolve the code hash from the instance id BEFORE they touch the state
    /// store.
    ///
    /// This is a source scrape rather than a behavioural test because the two
    /// sites are not equally reachable. The network site
    /// (`bridged_upsert_contract_state_inner`) is covered behaviourally by
    /// `update_by_instance_id_tests`, but `perform_contract_update` lives in
    /// `impl Executor<Runtime>` and only reaches the durable write after a real
    /// WASM `update_state` succeeds — so exercising it needs a compiled test
    /// contract, and nothing else in the tree calls it at all. Without this pin
    /// the local-mode resolution can be deleted with the whole suite green,
    /// which is exactly how it got missed the first time.
    ///
    /// Ordering matters, not just presence: the harm is what the UNRESOLVED key
    /// writes durably (`StateStorage::store` persists `key.code_hash()` into the
    /// hosting-metadata row), so a resolution placed after the first state-store
    /// access would pin nothing.
    #[test]
    fn update_apply_paths_resolve_code_hash_before_touching_the_state_store() {
        // Anchored on the API surface (`bridged_lookup_key(`), not on the `let
        // key =` binding, so a rename of the local does not silently unpin it.
        const RESOLVE: &str = "bridged_lookup_key(";

        // Scan CODE only. Both sites carry long `//` rationale blocks that name
        // `state_store` and `code_blob_stored` in prose, and an offset comparison
        // against prose measures the comments rather than the code — this pin
        // failed on exactly that before the strip was added.
        fn code_only(src: &str) -> String {
            src.lines()
                .map(|line| match line.find("//") {
                    Some(i) => &line[..i],
                    None => line,
                })
                .collect::<Vec<_>>()
                .join("\n")
        }

        // --- local mode ---
        let contract_ops = code_only(include_str!("runtime/contract_ops.rs"));
        let body = contract_ops
            .split("async fn perform_contract_update(")
            .nth(1)
            .expect("perform_contract_update must exist");
        let resolve_at = body.find(RESOLVE).unwrap_or_else(|| {
            panic!(
                "perform_contract_update must resolve the code hash from the \
                 instance id (#4978): local mode has no code_blob_stored gate, so \
                 an unresolved key reaches the durable hosting-metadata write"
            )
        });
        let store_at = body
            .find("state_store")
            .expect("perform_contract_update must touch the state store");
        assert!(
            resolve_at < store_at,
            "perform_contract_update must resolve BEFORE its first state_store \
             access (#4978) — the persisted code hash is the thing being fixed"
        );

        // --- network path ---
        let executor_impl = code_only(include_str!("runtime/executor_impl.rs"));
        let inner = executor_impl
            .split("async fn bridged_upsert_contract_state_inner(")
            .nth(1)
            .expect("bridged_upsert_contract_state_inner must exist");
        let resolve_at = inner.find(RESOLVE).unwrap_or_else(|| {
            panic!(
                "bridged_upsert_contract_state_inner must resolve the code hash \
                 from the instance id when no container is supplied (#4978)"
            )
        });
        let gate_at = inner
            .find("code_blob_stored(")
            .expect("the disk gate must still exist");
        assert!(
            resolve_at < gate_at,
            "the resolution must precede the code_blob_stored gate (#4978), or \
             an instance-id-addressed UPDATE is still refused"
        );
    }

    /// Pin: `perform_contract_update` MUST NOT route the network branch
    /// through `UpdateContract` / `self.op_request` / `request_update`.
    /// Network-mode UPDATEs flow through `start_client_update`.
    #[test]
    fn perform_contract_update_does_not_use_network_op_request() {
        // `perform_contract_update` lives in runtime/contract_ops.rs after the split.
        let src = include_str!("runtime/contract_ops.rs");
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
        // After the file split, `fetch_related_for_validation` lives in
        // runtime/executor_impl.rs and `fetch_related_for_validation_network`
        // lives in runtime/contract_ops.rs. Search each function in its own
        // source file so that the needle string used in the search does not
        // appear as a string literal inside this test (which would make nth(1)
        // extract the test code instead of the production function body).
        const CONTRACT_OPS_SRC: &str = include_str!("runtime/contract_ops.rs");
        const EXECUTOR_IMPL_SRC: &str = include_str!("runtime/executor_impl.rs");

        // Each entry: (function name, file source to search, split needle).
        // Using per-file sources prevents accidental matches against the
        // needle appearing as a Rust string literal inside this test module.
        let sites: &[(&str, &str, &str)] = &[
            (
                "fetch_related_for_validation",
                EXECUTOR_IMPL_SRC,
                "async fn fetch_related_for_validation(",
            ),
            (
                "fetch_related_for_validation_network",
                CONTRACT_OPS_SRC,
                "async fn fetch_related_for_validation_network(",
            ),
        ];

        for (name, file_src, needle) in sites {
            let body = file_src
                .split(needle)
                .nth(1)
                .unwrap_or_else(|| panic!("{name} must exist in its source file"))
                .split("\n    }\n")
                .next()
                .unwrap_or_else(|| panic!("{name} closing brace not found"));
            assert!(
                body.contains("join_all"),
                "{name} must call futures::future::join_all — serial fetch \
                 regressed in freenet/freenet-core#4077; do not revert"
            );
            // Spot-check the NETWORK fetch doesn't regress to a serial
            // `for id in &unique_ids { ... fetch_related_via_network ... await }`
            // loop (the exact pre-#4077 shape). NOTE: the deferrable-mode block
            // (#4391) legitimately iterates `&unique_ids` to do a LOCAL-ONLY
            // presence check that contains NO `fetch_related_via_network`, so
            // the bare `for id in &unique_ids` form is no longer a reliable
            // signal.
            //
            // The check: within the `body` string for each site, split on
            // "for id in &unique_ids" and look for "fetch_related_via_network"
            // within a 1 500-char window after each such split point. The
            // deferrable-mode loop is ~700 chars; 1 500 chars comfortably
            // covers the immediate loop body without accidentally capturing the
            // non-deferrable join_all block that appears ~2 600 chars later.
            let serial_network_fetch = body.split("for id in &unique_ids").skip(1).any(|seg| {
                // Bound the look-ahead to the immediate loop body so that
                // `fetch_related_via_network` in a later branch of the same
                // function is not falsely attributed to this `for` loop.
                // Back the end index up to a UTF-8 char boundary so a
                // multi-byte character straddling byte 1 500 doesn't panic.
                let mut end = seg.len().min(1_500);
                while end > 0 && !seg.is_char_boundary(end) {
                    end -= 1;
                }
                let window = &seg[..end];
                window.contains("fetch_related_via_network")
            });
            assert!(
                !serial_network_fetch,
                "{name} must not iterate serially over &unique_ids to call \
                 fetch_related_via_network — regressed to pre-#4077 behavior"
            );
        }

        // The third site is inline inside `bridged_upsert_contract_state`,
        // not its own function. After the file split it lives in
        // runtime/executor_impl.rs. Pin it by confirming the comment marker
        // that frames the parallel-fetch block is present and that the
        // block contains `join_all`. Brittle by design: a refactor that
        // moves the comment also has to move the assertion.
        // Search EXECUTOR_IMPL_SRC directly so the marker is not confused
        // with any identical substring that may appear as a string literal
        // inside this test file.
        let inline_marker = "NON-deferrable mode: parallel fetch";
        let after_marker = EXECUTOR_IMPL_SRC.split(inline_marker).nth(1).expect(
            "inline UPDATE-side parallel-fetch marker missing from \
             executor_impl.rs — #4077 regressed",
        );
        // Clamp to a UTF-8 char boundary (and to the string length) so a
        // multi-byte character near byte 2 000 doesn't panic the slice.
        let mut window_end = after_marker.len().min(2_000);
        while window_end > 0 && !after_marker.is_char_boundary(window_end) {
            window_end -= 1;
        }
        assert!(
            after_marker[..window_end].contains("join_all"),
            "UPDATE-side inline related fetch in bridged_upsert_contract_state \
             must call futures::future::join_all (#4077)"
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
        // "Contract state updated" log lives in runtime/executor_impl.rs after the split.
        // Search only executor_impl.rs to avoid matching the same string in the doc
        // comment of this test function (runtime.rs line ~1852: `"Contract state updated"`).
        let src = include_str!("runtime/executor_impl.rs");
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

    /// Gate (issue #4441): the production pool OPTS IN to compile offload
    /// (`production_offload_compilation()` is always `true`). Safety/determinism
    /// no longer rests on this flag — it rests on the runtime-flavor check in
    /// `wasmtime_engine::compile_offloaded`, which compiles inline under a
    /// current_thread / no runtime and offloads only on a multi-thread runtime.
    /// This is the fix for the old `!cfg!(test)` gate that wrongly turned
    /// offload on in integration-test crates (compiled without `cfg(test)`) and
    /// panicked at `block_in_place` on their current_thread runtimes.
    #[test]
    fn production_offload_is_opt_in() {
        assert!(
            super::production_offload_compilation(),
            "production pool must opt in to offload; the runtime-flavor check in \
             compile_offloaded keeps it safe/deterministic everywhere else"
        );
    }

    /// `run_blocking_offloaded` under a MULTI-THREAD runtime must run the
    /// closure on a DIFFERENT thread (the offload actually happens, so a long
    /// export does not occupy the calling/loop thread) and round-trip both the
    /// moved-in value and the closure's result.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn run_blocking_offloaded_runs_off_thread_under_multithread() {
        let caller_thread = std::thread::current().id();
        // Move a value in, return it alongside the result.
        let outcome =
            super::run_blocking_offloaded(41u64, |v| (v + 1, (v * 2, std::thread::current().id())))
                .await;
        let super::OffloadOutcome::Completed(value, (result, ran_on)) = outcome else {
            panic!("a non-panicking closure must complete");
        };
        assert_eq!(
            value, 42,
            "moved-in value is returned (mutated by the closure)"
        );
        assert_eq!(result, 82, "closure result is returned");
        assert_ne!(
            ran_on, caller_thread,
            "multi-thread runtime must offload the closure to a blocking thread"
        );
    }

    /// `run_blocking_offloaded` under a CURRENT-THREAD runtime must run INLINE
    /// (same thread). `spawn_blocking` real threads break the simulation
    /// runner's paused-time determinism (and the current_thread integration
    /// tests), so the helper runs the closure inline there instead. This pins
    /// the runtime-flavor fallback.
    #[tokio::test(flavor = "current_thread")]
    async fn run_blocking_offloaded_runs_inline_under_current_thread() {
        let caller_thread = std::thread::current().id();
        let outcome =
            super::run_blocking_offloaded(7u64, |v| (v, std::thread::current().id())).await;
        let super::OffloadOutcome::Completed(value, ran_on) = outcome else {
            panic!("inline run must complete");
        };
        assert_eq!(value, 7);
        assert_eq!(
            ran_on, caller_thread,
            "current_thread runtime must run the closure inline (no offload)"
        );
    }

    /// PANIC SAFETY (#4531): a panic inside the offloaded closure on a
    /// multi-thread runtime must be CAUGHT and reported as
    /// `OffloadOutcome::Panicked` — NOT re-raised onto the caller's task (which
    /// for the export path is the contract loop, whose abort would shut the node
    /// down). The moved-in value is lost (the blocking thread owned it).
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn run_blocking_offloaded_catches_panic_under_multithread() {
        let outcome = super::run_blocking_offloaded(1u64, |_v| -> (u64, ()) {
            panic!("boom inside the offloaded closure");
        })
        .await;
        assert!(
            matches!(outcome, super::OffloadOutcome::Panicked),
            "a panicking offload must surface as Panicked, not unwind the caller"
        );
    }

    /// Pin (#4531 / #4381 P5): the off-loop export MUST route the synchronous
    /// enumerate+decrypt+seal through `run_blocking_offloaded` (so the CPU work
    /// lands on a blocking thread, not the contract loop). A future refactor that
    /// drops the offload re-introduces the head-of-line-blocking DoS and must
    /// fail CI here. Anchored on `ExportJob::run`, the off-loop entry point.
    #[test]
    fn export_job_run_offloads_blocking_work() {
        let src = include_str!("runtime/pool.rs");
        let body = src
            .split("pub(crate) async fn run(self) -> ExportDone {")
            .nth(1)
            .expect("ExportJob::run must exist")
            .split("\n    }\n")
            .next()
            .expect("end of ExportJob::run");
        assert!(
            body.contains("run_blocking_offloaded("),
            "ExportJob::run must offload the synchronous export off the contract \
             loop via run_blocking_offloaded (#4531 / #4381 P5)"
        );
    }

    /// The anti-deadlock invariant (#4531): off-loop export concurrency is
    /// `min(MAX_CONCURRENT_EXPORTS, pool_size - 1)`, so at least one executor is
    /// ALWAYS reserved for normal contract ops — exports can never hold every
    /// executor and wedge the loop. On a 1-executor pool this is 0 (exports
    /// disabled → Busy/503). Pure-function guard for the math.
    #[test]
    fn effective_export_permits_always_reserves_one_executor() {
        use super::pool::{MAX_CONCURRENT_EXPORTS, effective_export_permits};
        // 1-executor pool: exports DISABLED (no spare executor to lend).
        assert_eq!(effective_export_permits(1), 0);
        // 2-executor pool: exactly one export, one executor reserved for ops.
        assert_eq!(effective_export_permits(2), 1);
        // Below the MAX_CONCURRENT_EXPORTS ceiling, scales with pool_size - 1.
        assert_eq!(effective_export_permits(3), MAX_CONCURRENT_EXPORTS.min(2));
        // Large pool: clamped to MAX_CONCURRENT_EXPORTS, never more.
        assert_eq!(effective_export_permits(64), MAX_CONCURRENT_EXPORTS);
        // The invariant: for any pool_size >= 1, permits <= pool_size - 1, so a
        // spare executor always remains for normal ops.
        for n in 1..=64usize {
            assert!(
                effective_export_permits(n) <= n.saturating_sub(1),
                "must always reserve >=1 executor for normal ops (pool_size={n})"
            );
        }
        // Degenerate pool_size == 0 must not panic (saturating).
        assert_eq!(effective_export_permits(0), 0);
    }

    /// Pin (#4531): when the offloaded export task PANICS, the executor is lost
    /// with the unwinding thread, so `RuntimePool::finish_export` MUST reconcile
    /// the missing pool slot — build a replacement (restoring the permit) rather
    /// than leaving the pool one short or, worse, leaking the permit. A refactor
    /// that drops the replacement re-introduces a slow capacity leak (and risks
    /// the `pop_executor` `unreachable!` from a permit/slot mismatch).
    #[test]
    fn finish_export_replaces_panicked_executor() {
        let src = include_str!("runtime/pool.rs");
        let body = src
            .split("async fn finish_export(&mut self, done: ExportDone)")
            .nth(1)
            .expect("RuntimePool::finish_export must exist")
            .split("\n    fn get_subscription_info(")
            .next()
            .expect("end of finish_export");
        // The None (panicked) arm must build a replacement executor.
        assert!(
            body.contains("create_replacement_executor("),
            "finish_export must replace a panic-lost executor (create_replacement_executor)"
        );
        // ...and on a successful export, return the executor to the pool.
        assert!(
            body.contains("return_checked("),
            "finish_export must return a healthy executor to the pool"
        );
    }

    /// Pin (#4531 / #4381 P5): the `ExportUserSecrets` dispatch arm MUST defer
    /// the export onto a spawned background task (so the loop returns
    /// immediately) rather than awaiting it inline. Anchored on the arm spawning
    /// the job via `GlobalExecutor::spawn` and routing through `try_begin_export`
    /// — awaiting the export inline (the previous design) re-introduces #4531.
    #[test]
    fn export_dispatch_arm_defers_off_loop() {
        let src = include_str!("../../contract.rs");
        let arm = src
            .split("ContractHandlerEvent::ExportUserSecrets {\n            user_context,\n            token,\n        } => {")
            .nth(1)
            .expect("ExportUserSecrets dispatch arm must exist")
            .split("ContractHandlerEvent::RegisterSubscriberListener")
            .next()
            .expect("end of ExportUserSecrets arm");
        assert!(
            arm.contains("try_begin_export("),
            "the export arm must admit via try_begin_export (off-loop deferral)"
        );
        assert!(
            arm.contains("GlobalExecutor::spawn("),
            "the export arm must run the export on a spawned background task, \
             not await it inline on the contract loop (#4531)"
        );
        assert!(
            !arm.contains(".export_user_secrets("),
            "the export arm must NOT call export_user_secrets inline on the loop"
        );
    }

    /// Pin: `from_config_with_shared_modules` MUST build the engine with the
    /// offload gate (`production_offload_compilation()`) and thread the byte
    /// budget through, rather than the old hardcoded `RuntimeConfig::default()`
    /// that left `offload_compilation` dead on the production pool path.
    #[test]
    fn from_config_with_shared_modules_wires_offload_and_budget() {
        let src = include_str!("runtime.rs");
        let body = src
            .split("pub(crate) async fn from_config_with_shared_modules(")
            .nth(1)
            .expect("from_config_with_shared_modules must exist")
            .split("\n    pub async fn preload(")
            .next()
            .expect("end of from_config_with_shared_modules");
        assert!(
            body.contains("offload_compilation: production_offload_compilation()"),
            "must set offload_compilation from the production gate"
        );
        assert!(
            body.contains("module_cache_budget_bytes: config.module_cache_budget_bytes"),
            "must thread the operator-configured byte budget into the runtime config"
        );
        // The hardcoded default that previously dropped offload must be gone:
        // the backend engine is now built from `runtime_config`, not a fresh
        // `RuntimeConfig::default()`.
        assert!(
            body.contains("Engine::create_backend_engine(&runtime_config)"),
            "backend engine must be built from the threaded runtime_config"
        );
        // The wasmtime ON-DISK compile cache's soft limit must be derived from
        // BOTH the memory the node may use AND the disk actually free on the
        // cache's mount, never re-hardcoded to a flat constant or RAM alone: a
        // fixed 512 MiB let a 2 GiB-cgroup node keep a compile cache larger than
        // its entire 256 MiB contract-state budget (#4683), and a RAM-only figure
        // left a disk-tight-but-RAM-rich host's admission gate wedged shut by its
        // own oversized compile cache (#5014). Whitespace is collapsed so the pin
        // survives a rustfmt line-wrap of the field.
        let collapsed = body.split_whitespace().collect::<Vec<_>>().join(" ");
        assert!(
            collapsed.contains("default_wasmtime_cache_size_bytes_for_dir( &wasmtime_cache_dir,"),
            "the wasmtime on-disk compile-cache soft limit must come from \
             default_wasmtime_cache_size_bytes_for_dir() (RAM- AND disk-relative), \
             not a constant or a RAM-only figure"
        );
        // #5328 review: the operator's configured disk-budget knobs
        // (`--hosting-disk-pct` / `--max-hosting-disk`) must feed the sizing
        // call too — a raw-physical-disk-only bound leaves an operator who
        // shrinks `--max-hosting-disk` below physical capacity permanently
        // wedged, since the compile cache would still size itself off the
        // larger physical disk. This is the case the ORIGINAL issue (#5014)
        // suggested addressing via `disk_budget_for_clamped`.
        assert!(
            collapsed.contains("config.hosting_disk_pct, config.max_hosting_disk,"),
            "the sizing call must be fed the operator's configured \
             hosting-disk-pct/max-hosting-disk, not just raw physical disk \
             availability — otherwise an operator-shrunk disk budget below \
             physical capacity stays permanently wedged"
        );
        // #5328 review: that sizing call does real filesystem work — a statvfs
        // read and, via reconciliation, a directory walk and possibly a
        // remove_dir_all — so it MUST be gated on `shared_backend.is_none()`
        // (the one branch that actually builds a fresh engine/Cache). Without
        // this gate, every pool worker AND every mid-life
        // `create_replacement_executor` panic-recovery call would re-run it
        // against a directory the shared, already-in-use engine is actively
        // reading/writing.
        assert!(
            collapsed.contains("shared_backend.is_none().then(|| {"),
            "the disk-aware compile-cache sizing (and its reconciliation side \
             effect) must be gated on shared_backend.is_none(), so it runs only \
             for the executor that actually builds a new backend engine — never \
             for a pool worker reusing the shared engine or a mid-life \
             executor replacement"
        );
    }

    /// Pin: `RuntimePool::new` MUST size the shared module caches by the
    /// operator-configured byte budget, not a hardcoded count constant.
    #[test]
    fn runtime_pool_sizes_caches_by_byte_budget() {
        // RuntimePool::new lives in runtime/pool.rs after the split.
        let src = include_str!("runtime/pool.rs");
        let body = src
            .split("pub async fn new(")
            .nth(1)
            .expect("RuntimePool::new must exist")
            // Take the first chunk of the function body. Whitespace is collapsed
            // so the assertions below survive line-wrapping / reformatting of the
            // (now multi-line, metrics-threaded, interest-predicate-threaded)
            // cache construction.
            .split("\n    ")
            .take(110)
            .collect::<String>()
            .split_whitespace()
            .collect::<Vec<_>>()
            .join(" ");
        assert!(
            body.contains("config.module_cache_budget_bytes"),
            "RuntimePool::new must size caches from config.module_cache_budget_bytes"
        );
        // The contract cache is built from `contract_cache_budget` with the
        // "contract" label (now via `with_label_and_interest`, which threads the
        // `Ring::contract_in_use` interest predicate for two-tier eviction —
        // #4441/#4534); the delegate cache from `delegate_cache_budget` with the
        // "delegate" label. We assert each piece independently (rather than a
        // single literal call string) so threading the metrics `Arc` / interest
        // predicate doesn't make this pin brittle.
        assert!(
            body.contains(
                "ModuleCache::with_label_and_interest( contract_cache_budget, \"contract\""
            ) || body.contains(
                "ModuleCache::with_label_and_interest(contract_cache_budget, \"contract\""
            ),
            "RuntimePool::new must build the contract cache from the contract byte budget"
        );
        // The contract cache must thread the Ring interest predicate so two-tier
        // eviction (and the shadow metrics) actually see contract_in_use.
        assert!(
            body.contains("contract_in_use"),
            "RuntimePool::new must wire the Ring::contract_in_use interest predicate \
             into the contract module cache (#4441/#4534)"
        );
        assert!(
            body.contains("ModuleCache::with_label( delegate_cache_budget, \"delegate\"")
                || body.contains("ModuleCache::with_label(delegate_cache_budget, \"delegate\""),
            "RuntimePool::new must build the delegate cache from its own (smaller) budget"
        );
        assert!(
            body.contains("DELEGATE_MODULE_CACHE_BUDGET_DIVISOR"),
            "the delegate cache must be a fraction of the contract budget so the \
             COMBINED default ceiling stays safe on a small box (issue #4441 fix-up)"
        );
        // The old count-cap constant must be gone from this path.
        assert!(
            !body.contains("DEFAULT_MODULE_CACHE_CAPACITY"),
            "RuntimePool::new must no longer reference the removed count cap"
        );
    }

    /// Pin (#2257): the `ContractRequest::Put` arm of `contract_requests`
    /// MUST reject debug-compiled WASM (`contains_debug_sections`) BEFORE
    /// delegating to `perform_contract_put`. This is the only debug-WASM
    /// guard on the local-node PUT path (`run_local_node`, the local
    /// server loop, `preload`, `handle_request`), which never touches
    /// `process_open_request`. A migration that drops this call would
    /// silently restore the opaque "Message too long" symptom for local
    /// development — exactly the case #2257 targets. Source-scrape pin per
    /// `.claude/rules/bug-prevention-patterns.md` (cheaper and more robust
    /// than standing up a full `Executor<Runtime>` fixture for a one-line
    /// guard).
    #[test]
    fn contract_requests_put_rejects_debug_wasm_before_perform_put() {
        let src = include_str!("runtime.rs");
        // Isolate the `contract_requests` function body.
        let body = src
            .split("pub async fn contract_requests(")
            .nth(1)
            .expect("contract_requests must exist");
        let guard_pos = body
            .find("contains_debug_sections")
            .expect("contract_requests Put arm must call contains_debug_sections");
        let put_pos = body
            .find("self.perform_contract_put(")
            .expect("contract_requests must call perform_contract_put");
        assert!(
            guard_pos < put_pos,
            "the debug-WASM guard (contains_debug_sections) must run BEFORE \
             perform_contract_put, so a debug build is rejected before any \
             local storage/validation work"
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

    /// The executor's "code already stored" branch must refuse a container whose
    /// instance id is not derived from its own code and parameters — tested at the
    /// executor entry point, not at the store.
    ///
    /// This test exists because of WHERE the original defect lived. Every test for
    /// the identity check was scoped to `contract_store.rs`, and the bypass was one
    /// layer up: `bridged_upsert_contract_state_inner` reached the durable
    /// instance→code index through a bare index-write helper instead of through
    /// `store_contract`, so the store's own tests could not see it and a four-lens
    /// review did not catch it. A pin on the source text is not the same thing as
    /// exercising the entry point, so this drives the real
    /// `Executor<Runtime>` — real `ContractStore`, real `StateStore`, no network.
    ///
    /// # Why garbage WASM bytes are sufficient
    ///
    /// The refusal happens in the store/index branch, BEFORE
    /// `fetch_related_for_validation` (and therefore before any WASM call), so the
    /// forged container is rejected without the module ever being compiled. The
    /// control below relies on the same ordering from the other side: an HONEST
    /// second instance gets past the identity check and is indexed, then fails
    /// later in WASM validation because `vec![3u8; 64]` is not a real module. That
    /// asymmetry — indexed-and-failed-late versus refused-and-not-indexed — is
    /// what makes this discriminating rather than a blanket "PUT fails" assertion.
    #[tokio::test]
    async fn already_stored_branch_refuses_an_underived_instance() {
        use crate::contract::executor::ContractExecutor;
        use crate::wasm_runtime::ContractStoreBridge;
        use either::Either;
        use freenet_stdlib::prelude::RelatedContracts;

        let (mut executor, contracts_dir, _temp) = build_disk_executor("identity-gate").await;

        // Precondition: the code blob is on disk, so `code_blob_stored` reports
        // true and a PUT of another instance of that code takes the already-stored
        // branch. Established through the store directly because this is setup,
        // not the behaviour under test — and because routing it through the
        // executor would fail in WASM validation and then remove the blob again.
        let (honest, honest_key) = make_contract(3, 3);
        executor
            .runtime
            .store_contract(honest)
            .expect("seeding the code blob must succeed");
        assert!(
            wasm_path(&contracts_dir, &honest_key).exists(),
            "fixture must leave the code blob on disk"
        );

        // CONTROL first: an honest second instance of the same code. Its identity
        // is derived, so the branch indexes it; it then dies in WASM validation.
        let (honest_b, honest_b_key) = make_contract(3, 9);
        assert_eq!(
            honest_b_key.code_hash(),
            honest_key.code_hash(),
            "fixture must share the code blob"
        );
        assert_ne!(honest_b_key.id(), honest_key.id(), "must be a NEW instance");
        let control = executor
            .upsert_contract_state(
                honest_b_key,
                Either::Left(WrappedState::new(vec![7u8; 8])),
                RelatedContracts::default(),
                Some(honest_b),
            )
            .await;
        assert!(
            !format!("{control:?}").contains("identity does not match its code"),
            "an honestly-derived instance must pass the identity check (it may fail \
             later in WASM validation): {control:?}"
        );
        assert_eq!(
            executor.runtime.code_hash_from_id(honest_b_key.id()),
            Some(*honest_key.code_hash()),
            "the honest new instance must have been indexed by the guarded ingress"
        );

        // Now the forgery: same code, so the blob is present and the branch is
        // taken, but an instance id derived from DIFFERENT parameters.
        let code = ContractCode::from(vec![3u8; 64]);
        let real_params = Parameters::from(vec![3u8; 8]);
        let unrelated_instance =
            *ContractKey::from_params_and_code(Parameters::from(vec![200u8; 8]), &code).id();
        let mut forged = WrappedContract::new(Arc::new(code.clone()), real_params);
        forged.key = ContractKey::from_id_and_code(unrelated_instance, *code.hash());
        let forged_key = forged.key;

        let err = executor
            .upsert_contract_state(
                forged_key,
                Either::Left(WrappedState::new(vec![7u8; 8])),
                RelatedContracts::default(),
                Some(ContractContainer::Wasm(ContractWasmAPIVersion::V1(forged))),
            )
            .await
            .expect_err("an underived instance must be refused at the executor entry point");
        assert!(
            format!("{err:?}").contains("identity does not match its code"),
            "expected the store's identity refusal to surface here, got: {err:?}"
        );
        assert!(
            executor
                .runtime
                .code_hash_from_id(&unrelated_instance)
                .is_none(),
            "a refused container must not leave an instance→code index row"
        );
    }

    /// GHSA-824h-7x5x-wfmf regression: a NON-LOCAL registration must not be able
    /// to write the durable first-registration origin record.
    ///
    /// The gate on `resolve_message_origin` only controls what a delegate is
    /// TOLD about its caller. `origin_contract` has a second, far more damaging
    /// consumer: `register_delegate_and_record_origin` writes it into a record
    /// that is FIRST-WRITER-WINS and IMMUTABLE. An off-host caller could mint a
    /// token for any contract id (the node issues one on request, for any id,
    /// existing or not) and permanently freeze a delegate's recorded provenance
    /// to a value of their choosing — unrepairable short of wiping the database.
    ///
    /// Delegate WASM and params are public, so the key is derivable and the
    /// record can be poisoned BEFORE the legitimate app ever registers.
    #[tokio::test(flavor = "multi_thread")]
    async fn remote_registration_cannot_write_the_durable_origin_record() {
        use crate::contract::storages::Storage;
        use freenet_stdlib::client_api::DelegateRequest;
        use freenet_stdlib::prelude::{
            ContractInstanceId, Delegate, DelegateContainer, DelegateWasmAPIVersion,
        };

        let temp_dir = crate::util::tests::get_temp_dir();
        let db = Storage::new(temp_dir.path()).await.expect("create db");
        let contract_store =
            ContractStore::new(temp_dir.path().join("contracts"), 10_000, db.clone())
                .expect("contract store");
        let delegate_store =
            DelegateStore::new(temp_dir.path().join("delegate"), 10_000, db.clone())
                .expect("delegate store");
        let secrets_store = SecretsStore::new(
            temp_dir.path().join("secrets"),
            Default::default(),
            db.clone(),
        )
        .expect("secrets store");
        let state_store = StateStore::new(db.clone(), 10_000_000).expect("state store");
        let runtime = Runtime::build(contract_store, delegate_store, secrets_store, false)
            .expect("build runtime");
        let mut executor = Executor::new(
            state_store,
            || Ok(()),
            crate::contract::executor::OperationMode::Local,
            runtime,
            None,
        )
        .await
        .expect("create executor");

        let victim = Delegate::from((&vec![0u8].into(), &vec![0xD4u8].into()));
        let attacker_claim = ContractInstanceId::new([0x99u8; 32]);

        // An off-host caller registers the delegate, presenting a token bound to
        // a contract id it does not own.
        executor
            .delegate_request(
                DelegateRequest::RegisterDelegate {
                    delegate: DelegateContainer::Wasm(DelegateWasmAPIVersion::V1(victim.clone())),
                    cipher: [7u8; 32],
                    nonce: [9u8; 24],
                },
                Some(&attacker_claim),
                None,
                crate::client_events::ConnectionScope::Remote,
                None,
            )
            .expect("registration itself still succeeds; only the attestation is withheld");

        let (_has_admin_none, origins) = db
            .get_delegate_origins(victim.key())
            .expect("record must be readable")
            .expect("registration must have written a record");
        assert!(
            origins.is_empty(),
            "a non-local registration must record NO contract id (the record is \
             immutable, so a poisoned value can never be corrected); got {origins:?}"
        );

        // Control: the SAME request from a local connection DOES record the id,
        // so the assertion above is about the scope gate and not about the
        // record being write-only.
        let legit = Delegate::from((&vec![0u8].into(), &vec![0xD5u8].into()));
        let app = ContractInstanceId::new([0x11u8; 32]);
        executor
            .delegate_request(
                DelegateRequest::RegisterDelegate {
                    delegate: DelegateContainer::Wasm(DelegateWasmAPIVersion::V1(legit.clone())),
                    cipher: [7u8; 32],
                    nonce: [9u8; 24],
                },
                Some(&app),
                None,
                crate::client_events::ConnectionScope::Local,
                None,
            )
            .expect("local registration must succeed");

        let (_, origins) = db
            .get_delegate_origins(legit.key())
            .expect("record must be readable")
            .expect("registration must have written a record");
        assert_eq!(
            origins.len(),
            1,
            "a local registration must still record its attested contract id"
        );
        assert_eq!(origins[0], *app.as_bytes(), "wrong contract id recorded");
    }

    /// GHSA-824h-7x5x-wfmf regression: an UNATTESTED delegate must still be
    /// able to send delegate-to-delegate messages.
    ///
    /// A revision of that fix refused to dispatch from any delegate whose
    /// first-registration record held no contract id. That record is `None` both
    /// for "never registered here" AND for the tokenless local CLI shape, so it
    /// silently broke every delegate installed by riverctl / atlasctl / fdev —
    /// silently because the caller swallows the error into a warning and the
    /// client still receives `Ok` with the second delegate's reply simply
    /// missing. Re-adding any such gate must fail here.
    ///
    /// The escalation that gate was aimed at is closed by connection scope
    /// instead (`resolve_message_origin` returns `None` for a non-local
    /// connection, and the scope is propagated into the hop), which the
    /// `remote_connection_gets_no_caller_delegate_origin` unit test pins.
    #[tokio::test(flavor = "multi_thread")]
    async fn unattested_delegate_can_still_dispatch_to_another_delegate() {
        use freenet_stdlib::client_api::DelegateRequest;
        use freenet_stdlib::prelude::{
            Delegate, DelegateContainer, DelegateWasmAPIVersion, Parameters,
        };

        let (mut executor, _contracts_dir, _temp) =
            build_disk_executor("unattested-inter-delegate").await;

        // Registered with NO attested origin — the tokenless CLI shape.
        let cli_delegate = Delegate::from((&vec![0u8].into(), &vec![0xA1u8].into()));
        let victim = Delegate::from((&vec![0u8].into(), &vec![0xC3u8].into()));

        for delegate in [&cli_delegate, &victim] {
            executor
                .delegate_request(
                    DelegateRequest::RegisterDelegate {
                        delegate: DelegateContainer::Wasm(DelegateWasmAPIVersion::V1(
                            delegate.clone(),
                        )),
                        cipher: [7u8; 32],
                        nonce: [9u8; 24],
                    },
                    None,
                    None,
                    crate::client_events::ConnectionScope::Local,
                    None,
                )
                .expect("registration must succeed");
        }

        let result = executor.delegate_request(
            DelegateRequest::ApplicationMessages {
                key: victim.key().clone(),
                params: Parameters::from(Vec::new()),
                inbound: vec![],
            },
            None,
            Some(cli_delegate.key()),
            crate::client_events::ConnectionScope::Local,
            None,
        );

        // The fixture delegates are not real WASM, so the call still fails — but
        // it must fail on EXECUTION, never on a provenance/attestation refusal.
        // Asserting on the absence of that refusal is the whole point: a
        // re-introduced gate would short-circuit before execution.
        if let Err(e) = result {
            let msg = e.to_string();
            assert!(
                !msg.contains("attested registration origin") && !msg.contains("dispatch refused"),
                "an unattested delegate must not be refused dispatch; got: {msg}"
            );
        }
    }

    /// Handler-level (GHSA-824h-7x5x-wfmf): `RegisterDelegateWithPredecessors`
    /// NEVER copies a predecessor's secrets, even when the registering request's
    /// `origin_contract` exactly matches the predecessor's recorded
    /// first-registration origin (the one case the H1 same-origin gate in
    /// `SecretsStore::migrate_secrets` would otherwise allow). The copy-forward
    /// call is disabled at the handler level because `origin_contract` itself is
    /// forgeable by any HTTP client (see GHSA-824h-7x5x-wfmf) — so even a "matching" origin
    /// proves nothing. Registration still succeeds, exactly as plain
    /// `RegisterDelegate` would. This replaces the pre-advisory test asserting the
    /// copy DID happen on a matching origin.
    #[tokio::test(flavor = "multi_thread")]
    async fn register_delegate_with_predecessors_never_copies_secrets() {
        use crate::wasm_runtime::SecretScope;
        use freenet_stdlib::client_api::{DelegateRequest, HostResponse};
        use freenet_stdlib::prelude::{
            ContractInstanceId, Delegate, DelegateContainer, DelegateWasmAPIVersion, SecretsId,
        };
        use zeroize::Zeroizing;

        const ORIGIN: [u8; 32] = [0x11u8; 32];

        let temp_dir = crate::util::tests::get_temp_dir();
        let db = Storage::new(temp_dir.path()).await.expect("create db");
        let contract_store =
            ContractStore::new(temp_dir.path().join("contracts"), 10_000, db.clone())
                .expect("create contract store");
        let delegate_store =
            DelegateStore::new(temp_dir.path().join("delegate"), 10_000, db.clone())
                .expect("create delegate store");
        let secrets_dir = temp_dir.path().join("secrets");
        let mut secrets_store =
            SecretsStore::new(secrets_dir.clone(), Default::default(), db.clone())
                .expect("create secrets store");

        // Same params, different code == an ABI bump that mints a new key.
        let pred = Delegate::from((&vec![0u8].into(), &vec![1u8].into()));
        let succ = Delegate::from((&vec![0u8].into(), &vec![2u8].into()));

        // Seed a predecessor Local secret under its DERIVED DEK (the at-rest
        // path) BEFORE moving the secrets store into the runtime.
        let secret_id = SecretsId::new(b"room:alice".to_vec());
        secrets_store
            .store_secret(
                pred.key(),
                &secret_id,
                SecretScope::Local,
                Zeroizing::new(b"profile".to_vec()),
            )
            .expect("seed predecessor secret");

        // Record the predecessor's FIRST-registration origin (H1 same-origin
        // gate): the migrating registration below must present this SAME origin
        // for the copy to be allowed.
        secrets_store
            .record_delegate_registration_origin(pred.key(), Some(ORIGIN))
            .unwrap();

        let successor_secret_path = secrets_dir
            .join(succ.key().encode())
            .join(secret_id.encode());
        assert!(
            !successor_secret_path.exists(),
            "successor secret must not exist before migration"
        );

        let state_store = StateStore::new(db, 10_000_000).expect("create state store");
        let runtime = Runtime::build(contract_store, delegate_store, secrets_store, false)
            .expect("build runtime");
        let mut executor = Executor::new(
            state_store,
            || Ok(()),
            crate::contract::executor::OperationMode::Local,
            runtime,
            None,
        )
        .await
        .expect("create executor");

        let origin_contract = ContractInstanceId::new(ORIGIN);

        let req = DelegateRequest::RegisterDelegateWithPredecessors {
            delegate: DelegateContainer::Wasm(DelegateWasmAPIVersion::V1(succ.clone())),
            cipher: [7u8; 32],
            nonce: [9u8; 24],
            predecessors: vec![pred.key().clone()],
        };
        let resp = executor
            .delegate_request(
                req,
                Some(&origin_contract),
                None,
                crate::client_events::ConnectionScope::Local,
                None,
            )
            .expect("register-with-predecessors must succeed");
        let HostResponse::DelegateResponse { key, .. } = &resp else {
            panic!("expected DelegateResponse, got {resp:?}");
        };
        assert_eq!(key, succ.key(), "response carries the successor key");

        // The predecessor's Local secret must NOT be copied — the copy-forward
        // is unconditionally disabled (GHSA-824h-7x5x-wfmf), even though the supplied
        // `origin_contract` matches the predecessor's recorded origin exactly
        // (the one case the underlying H1 gate would otherwise have allowed).
        assert!(
            !successor_secret_path.exists(),
            "successor secret file must NOT exist: copy-forward is disabled (GHSA-824h-7x5x-wfmf) \
             regardless of origin_contract"
        );

        // The predecessor's own secret is untouched (registration never mutates
        // or deletes a predecessor's data, disabled copy-forward or not).
        let predecessor_secret_path = secrets_dir
            .join(pred.key().encode())
            .join(secret_id.encode());
        assert!(
            predecessor_secret_path.exists(),
            "predecessor secret must remain untouched"
        );
    }

    /// Regression test for GHSA-824h-7x5x-wfmf, directory-level: a
    /// predecessor holding MULTIPLE Local secrets, named in a
    /// `RegisterDelegateWithPredecessors` request with a matching
    /// `origin_contract`, must leave the successor's on-disk secrets
    /// directory completely absent (or empty) — not merely missing one
    /// known secret ID. This is stronger than
    /// `register_delegate_with_predecessors_never_copies_secrets`, which
    /// only checks a single secret path; a copy-forward bug that mis-copies
    /// a DIFFERENT secret ID than the one under test would slip past a
    /// single-path check but not a whole-directory scan.
    #[tokio::test(flavor = "multi_thread")]
    async fn register_delegate_with_predecessors_successor_dir_stays_empty() {
        use crate::wasm_runtime::SecretScope;
        use freenet_stdlib::client_api::{DelegateRequest, HostResponse};
        use freenet_stdlib::prelude::{
            ContractInstanceId, Delegate, DelegateContainer, DelegateWasmAPIVersion, SecretsId,
        };
        use zeroize::Zeroizing;

        const ORIGIN: [u8; 32] = [0x22u8; 32];

        let temp_dir = crate::util::tests::get_temp_dir();
        let db = Storage::new(temp_dir.path()).await.expect("create db");
        let contract_store =
            ContractStore::new(temp_dir.path().join("contracts"), 10_000, db.clone())
                .expect("create contract store");
        let delegate_store =
            DelegateStore::new(temp_dir.path().join("delegate"), 10_000, db.clone())
                .expect("create delegate store");
        let secrets_dir = temp_dir.path().join("secrets");
        let mut secrets_store =
            SecretsStore::new(secrets_dir.clone(), Default::default(), db.clone())
                .expect("create secrets store");

        let pred = Delegate::from((&vec![9u8].into(), &vec![1u8].into()));
        let succ = Delegate::from((&vec![9u8].into(), &vec![2u8].into()));

        // Seed THREE Local secrets under the predecessor.
        for i in 0u8..3 {
            secrets_store
                .store_secret(
                    pred.key(),
                    &SecretsId::new(format!("secret-{i}").into_bytes()),
                    SecretScope::Local,
                    Zeroizing::new(format!("value-{i}").into_bytes()),
                )
                .expect("seed predecessor secret");
        }
        secrets_store
            .record_delegate_registration_origin(pred.key(), Some(ORIGIN))
            .unwrap();

        let successor_secrets_dir = secrets_dir.join(succ.key().encode());
        assert!(
            !successor_secrets_dir.exists(),
            "successor secrets directory must not exist before registration"
        );

        let state_store = StateStore::new(db, 10_000_000).expect("create state store");
        let runtime = Runtime::build(contract_store, delegate_store, secrets_store, false)
            .expect("build runtime");
        let mut executor = Executor::new(
            state_store,
            || Ok(()),
            crate::contract::executor::OperationMode::Local,
            runtime,
            None,
        )
        .await
        .expect("create executor");

        let origin_contract = ContractInstanceId::new(ORIGIN);
        let req = DelegateRequest::RegisterDelegateWithPredecessors {
            delegate: DelegateContainer::Wasm(DelegateWasmAPIVersion::V1(succ.clone())),
            cipher: [7u8; 32],
            nonce: [9u8; 24],
            predecessors: vec![pred.key().clone()],
        };
        let resp = executor
            .delegate_request(
                req,
                Some(&origin_contract),
                None,
                crate::client_events::ConnectionScope::Local,
                None,
            )
            .expect("register-with-predecessors must succeed");
        assert!(matches!(resp, HostResponse::DelegateResponse { .. }));

        // Whole-directory check: NOTHING was copied into the successor's
        // namespace, whether the directory was never created or was created
        // empty.
        let successor_has_any_secret = successor_secrets_dir
            .read_dir()
            .map(|mut entries| entries.next().is_some())
            .unwrap_or(false);
        assert!(
            !successor_has_any_secret,
            "successor secrets directory must be absent or empty: copy-forward is \
             disabled (GHSA-824h-7x5x-wfmf) regardless of origin_contract or predecessor secret count"
        );
    }

    /// Handler-level (#4117 H1, persistence-succeeds-before-usable): if the
    /// first-writer origin record cannot be DURABLY persisted, the WHOLE
    /// registration is aborted — for BOTH the plain `RegisterDelegate` and the
    /// `RegisterDelegateWithPredecessors` variants. The delegate is NOT
    /// registered (no `.reg` file) and no predecessor secret is copied, so a
    /// registered-but-recordless delegate (a claimable first-writer slot an
    /// attacker could later name as its own) can never exist. Once the disk
    /// recovers, the app's retry registers and records normally (copy-forward
    /// itself is unconditionally disabled, GHSA-824h-7x5x-wfmf, so it never copies — see
    /// the final assertion). Uses the fault-injecting redb backend to fail the
    /// origin-record write on demand.
    #[cfg(feature = "redb")]
    #[tokio::test(flavor = "multi_thread")]
    // Shares the process-global `POISON_RECOVERY_TRIGGERED` counter with the
    // poison-recovery tests in `contract::storages::redb`, because driving the
    // fault injector trips it through `route_txn_error`/`route_redb_error`/
    // `commit_guarded` whether or not this test looks at it. The key is
    // cross-module by design: `serial_test` serializes on the key, not the
    // module. Pinned by `every_test_using_the_failure_injector_is_serialized`.
    #[serial_test::serial(redb_poison_recovery)]
    async fn register_aborts_when_origin_record_fails_then_recovers() {
        use crate::contract::storages::redb::{FailingBackend, open_redb_with_backend};
        use crate::wasm_runtime::SecretScope;
        use freenet_stdlib::client_api::DelegateRequest;
        use freenet_stdlib::prelude::{
            ContractInstanceId, Delegate, DelegateContainer, DelegateWasmAPIVersion, SecretsId,
        };
        use zeroize::Zeroizing;

        const ORIGIN: [u8; 32] = [0x11u8; 32];

        let temp_dir = crate::util::tests::get_temp_dir();
        let delegate_dir = temp_dir.path().join("delegate");
        let secrets_dir = temp_dir.path().join("secrets");

        // A secrets-store DB whose backend I/O can be flipped to fail on demand.
        let backend = FailingBackend::new();
        let db = open_redb_with_backend(backend.clone());

        let contract_store =
            ContractStore::new(temp_dir.path().join("contracts"), 10_000, db.clone())
                .expect("create contract store");
        let delegate_store = DelegateStore::new(delegate_dir.clone(), 10_000, db.clone())
            .expect("create delegate store");
        let mut secrets_store =
            SecretsStore::new(secrets_dir.clone(), Default::default(), db.clone())
                .expect("create secrets store");

        let pred = Delegate::from((&vec![0u8].into(), &vec![1u8].into()));
        let succ = Delegate::from((&vec![0u8].into(), &vec![2u8].into()));

        // Seed a predecessor Local secret + its first-registration origin WHILE the
        // backend is healthy (both must exist for the copy to be allowed later).
        let secret_id = SecretsId::new(b"room:alice".to_vec());
        secrets_store
            .store_secret(
                pred.key(),
                &secret_id,
                SecretScope::Local,
                Zeroizing::new(b"profile".to_vec()),
            )
            .expect("seed predecessor secret");
        secrets_store
            .record_delegate_registration_origin(pred.key(), Some(ORIGIN))
            .expect("record predecessor origin");

        let state_store = StateStore::new(db.clone(), 10_000_000).expect("create state store");
        let runtime = Runtime::build(contract_store, delegate_store, secrets_store, false)
            .expect("build runtime");
        let mut executor = Executor::new(
            state_store,
            || Ok(()),
            crate::contract::executor::OperationMode::Local,
            runtime,
            None,
        )
        .await
        .expect("create executor");

        let origin_contract = ContractInstanceId::new(ORIGIN);
        let succ_reg_path = delegate_dir.join(succ.key().encode()).with_extension("reg");
        let succ_secret_path = secrets_dir
            .join(succ.key().encode())
            .join(secret_id.encode());
        let pred_secret_path = secrets_dir
            .join(pred.key().encode())
            .join(secret_id.encode());

        // ---- The disk now fails: the successor's origin-record write cannot persist.
        backend.start_failing();

        // (a) RegisterDelegateWithPredecessors MUST abort: no register, no copy.
        let req = DelegateRequest::RegisterDelegateWithPredecessors {
            delegate: DelegateContainer::Wasm(DelegateWasmAPIVersion::V1(succ.clone())),
            cipher: [7u8; 32],
            nonce: [9u8; 24],
            predecessors: vec![pred.key().clone()],
        };
        assert!(
            executor
                .delegate_request(
                    req,
                    Some(&origin_contract),
                    None,
                    crate::client_events::ConnectionScope::Local,
                    None
                )
                .is_err(),
            "registration must FAIL when the first-writer origin record cannot persist"
        );
        assert!(
            !succ_reg_path.exists(),
            "an aborted registration must register NOTHING (no .reg file on disk)"
        );
        assert!(
            !succ_secret_path.exists(),
            "an aborted registration must copy NOTHING"
        );

        // (b) Plain RegisterDelegate MUST abort under the SAME failure (both variants).
        let plain = Delegate::from((&vec![0u8].into(), &vec![3u8].into()));
        let plain_reg_path = delegate_dir
            .join(plain.key().encode())
            .with_extension("reg");
        let req_plain = DelegateRequest::RegisterDelegate {
            delegate: DelegateContainer::Wasm(DelegateWasmAPIVersion::V1(plain.clone())),
            cipher: [7u8; 32],
            nonce: [9u8; 24],
        };
        assert!(
            executor
                .delegate_request(
                    req_plain,
                    Some(&origin_contract),
                    None,
                    crate::client_events::ConnectionScope::Local,
                    None
                )
                .is_err(),
            "plain RegisterDelegate must ALSO fail when the origin record cannot persist"
        );
        assert!(
            !plain_reg_path.exists(),
            "an aborted RegisterDelegate must register NOTHING"
        );

        // The predecessor's own secret is untouched throughout.
        assert!(
            pred_secret_path.exists(),
            "the predecessor's own secret must survive the failed migrating registrations"
        );

        // ---- Recovery: the disk heals (a fresh handle over a healthy backend);
        // the app retries and the registration now completes end-to-end.
        drop(executor); // release the poisoned db handle
        let db2 = open_redb_with_backend(FailingBackend::new()); // healthy
        let contract_store2 =
            ContractStore::new(temp_dir.path().join("contracts"), 10_000, db2.clone())
                .expect("create contract store 2");
        let delegate_store2 = DelegateStore::new(delegate_dir.clone(), 10_000, db2.clone())
            .expect("create delegate store 2");
        let secrets_store2 =
            SecretsStore::new(secrets_dir.clone(), Default::default(), db2.clone())
                .expect("create secrets store 2");
        // The origin table lived in the failed DB; on a real node it persists
        // across the restart, but here the fresh handle starts empty, so
        // re-establish the predecessor's origin as the app's retry would.
        secrets_store2
            .record_delegate_registration_origin(pred.key(), Some(ORIGIN))
            .expect("re-record predecessor origin after recovery");
        let state_store2 = StateStore::new(db2, 10_000_000).expect("create state store 2");
        let runtime2 = Runtime::build(contract_store2, delegate_store2, secrets_store2, false)
            .expect("build runtime 2");
        let mut executor2 = Executor::new(
            state_store2,
            || Ok(()),
            crate::contract::executor::OperationMode::Local,
            runtime2,
            None,
        )
        .await
        .expect("create executor 2");

        let req2 = DelegateRequest::RegisterDelegateWithPredecessors {
            delegate: DelegateContainer::Wasm(DelegateWasmAPIVersion::V1(succ.clone())),
            cipher: [7u8; 32],
            nonce: [9u8; 24],
            predecessors: vec![pred.key().clone()],
        };
        executor2
            .delegate_request(
                req2,
                Some(&origin_contract),
                None,
                crate::client_events::ConnectionScope::Local,
                None,
            )
            .expect("retry after recovery must succeed");
        assert!(
            succ_reg_path.exists(),
            "after recovery the successor IS registered (.reg present)"
        );
        // Copy-forward is unconditionally disabled (GHSA-824h-7x5x-wfmf): the retry succeeds
        // (registration itself was only ever blocked by the origin-record
        // write failure, now healed), but no secret is ever copied.
        assert!(
            !succ_secret_path.exists(),
            "the predecessor secret must NOT be copied forward: copy-forward is disabled (GHSA-824h-7x5x-wfmf)"
        );
    }

    /// #4117 P2b/M1: the predecessor-list bound is TWO-tiered and enforced
    /// through the real `Executor::delegate_request` path (not just the pure
    /// dedupe fn). The cap is on the UNIQUE count, matching the docstrings:
    ///   - 65 DISTINCT predecessors (> the deduped cap of 64) → request REJECTED,
    ///     nothing registered (silent truncation would strand older generations);
    ///   - a duplicate-heavy list whose UNIQUE count is within the cap → ACCEPTED
    ///     (duplicates dropped, not counted);
    ///   - a raw list past the DoS sanity bound → REJECTED up front regardless of
    ///     unique count.
    #[tokio::test(flavor = "multi_thread")]
    async fn register_with_predecessors_cap_is_on_unique_count_end_to_end() {
        use super::delegates::{MAX_MIGRATION_PREDECESSORS, MAX_MIGRATION_PREDECESSORS_RAW};
        use freenet_stdlib::client_api::DelegateRequest;
        use freenet_stdlib::prelude::{Delegate, DelegateContainer, DelegateWasmAPIVersion};

        let (mut executor, _contracts_dir, temp_dir) = build_disk_executor("pred-cap").await;
        let delegate_dir = temp_dir.path().join("delegate");

        let make_pred = |i: u8| {
            Delegate::from((&vec![i].into(), &vec![0u8].into()))
                .key()
                .clone()
        };
        let reg_path =
            |succ: &Delegate| delegate_dir.join(succ.key().encode()).with_extension("reg");

        // (1) 65 UNIQUE predecessors > the deduped cap of 64 → REJECTED.
        let succ_over = Delegate::from((&vec![0u8].into(), &vec![0xA1u8].into()));
        let over: Vec<_> = (0u8..=64).map(make_pred).collect(); // 65 distinct
        assert_eq!(over.len(), MAX_MIGRATION_PREDECESSORS + 1);
        let req_over = DelegateRequest::RegisterDelegateWithPredecessors {
            delegate: DelegateContainer::Wasm(DelegateWasmAPIVersion::V1(succ_over.clone())),
            cipher: [7u8; 32],
            nonce: [9u8; 24],
            predecessors: over,
        };
        assert!(
            executor
                .delegate_request(
                    req_over,
                    None,
                    None,
                    crate::client_events::ConnectionScope::Local,
                    None
                )
                .is_err(),
            "an over-cap UNIQUE predecessor list must be rejected"
        );
        assert!(
            !reg_path(&succ_over).exists(),
            "a rejected over-cap request must register NOTHING"
        );

        // (2) A duplicate-heavy list whose UNIQUE count (3) is within the cap →
        //     ACCEPTED (duplicates dropped, not counted).
        let succ_ok = Delegate::from((&vec![0u8].into(), &vec![0xB2u8].into()));
        let mut dupes: Vec<_> = Vec::new();
        for _ in 0..40 {
            dupes.push(make_pred(1));
            dupes.push(make_pred(2));
            dupes.push(make_pred(3));
        } // 120 raw, 3 unique
        assert!(
            dupes.len() > MAX_MIGRATION_PREDECESSORS
                && dupes.len() <= MAX_MIGRATION_PREDECESSORS_RAW,
            "the duplicate-heavy list must exceed the deduped cap in RAW length \
             yet stay under the raw sanity bound, to isolate the dedupe semantics"
        );
        let req_ok = DelegateRequest::RegisterDelegateWithPredecessors {
            delegate: DelegateContainer::Wasm(DelegateWasmAPIVersion::V1(succ_ok.clone())),
            cipher: [7u8; 32],
            nonce: [9u8; 24],
            predecessors: dupes,
        };
        executor
            .delegate_request(
                req_ok,
                None,
                None,
                crate::client_events::ConnectionScope::Local,
                None,
            )
            .expect("a duplicate-heavy but under-cap-UNIQUE list must be accepted");
        assert!(
            reg_path(&succ_ok).exists(),
            "an accepted request must register the successor"
        );

        // (3) A raw list past the DoS sanity bound → REJECTED up front regardless
        //     of unique count (all identical here: unique = 1, raw > the bound).
        let succ_raw = Delegate::from((&vec![0u8].into(), &vec![0xC3u8].into()));
        let raw_huge = vec![make_pred(7); MAX_MIGRATION_PREDECESSORS_RAW + 1];
        let req_raw = DelegateRequest::RegisterDelegateWithPredecessors {
            delegate: DelegateContainer::Wasm(DelegateWasmAPIVersion::V1(succ_raw.clone())),
            cipher: [7u8; 32],
            nonce: [9u8; 24],
            predecessors: raw_huge,
        };
        assert!(
            executor
                .delegate_request(
                    req_raw,
                    None,
                    None,
                    crate::client_events::ConnectionScope::Local,
                    None
                )
                .is_err(),
            "a raw list past the DoS sanity bound must be rejected even when unique count is small"
        );
        assert!(
            !reg_path(&succ_raw).exists(),
            "a rejected raw-oversize request must register NOTHING"
        );

        // (4) EXACTLY 64 UNIQUE predecessors (the at-cap boundary) → ACCEPTED.
        let succ_at_cap = Delegate::from((&vec![0u8].into(), &vec![0xD4u8].into()));
        let at_cap: Vec<_> = (0u8..64).map(make_pred).collect(); // 64 distinct
        assert_eq!(at_cap.len(), MAX_MIGRATION_PREDECESSORS);
        let req_at_cap = DelegateRequest::RegisterDelegateWithPredecessors {
            delegate: DelegateContainer::Wasm(DelegateWasmAPIVersion::V1(succ_at_cap.clone())),
            cipher: [7u8; 32],
            nonce: [9u8; 24],
            predecessors: at_cap,
        };
        executor
            .delegate_request(
                req_at_cap,
                None,
                None,
                crate::client_events::ConnectionScope::Local,
                None,
            )
            .expect("an exactly-at-cap UNIQUE count (64) must be accepted");
        assert!(
            reg_path(&succ_at_cap).exists(),
            "the at-cap boundary (64 unique) must register the successor"
        );

        // (5) EMPTY predecessor list → ACCEPTED, behaving like a plain
        //     RegisterDelegate (successor registered, nothing to copy). Pins the
        //     intended zero-predecessor semantics (the code does NOT reject empty).
        let succ_empty = Delegate::from((&vec![0u8].into(), &vec![0xE5u8].into()));
        let req_empty = DelegateRequest::RegisterDelegateWithPredecessors {
            delegate: DelegateContainer::Wasm(DelegateWasmAPIVersion::V1(succ_empty.clone())),
            cipher: [7u8; 32],
            nonce: [9u8; 24],
            predecessors: Vec::new(),
        };
        executor
            .delegate_request(
                req_empty,
                None,
                None,
                crate::client_events::ConnectionScope::Local,
                None,
            )
            .expect("an empty predecessor list must be accepted (plain-register equivalent)");
        assert!(
            reg_path(&succ_empty).exists(),
            "an empty-predecessor request must register the successor"
        );

        // (6) A raw list at EXACTLY the sanity bound (1024) with a within-cap
        //     UNIQUE count (64) → ACCEPTED (the raw-bound boundary: only > the
        //     bound is rejected).
        let succ_raw_boundary = Delegate::from((&vec![0u8].into(), &vec![0xF6u8].into()));
        let mut raw_at_bound: Vec<_> = Vec::new();
        for _ in 0..(MAX_MIGRATION_PREDECESSORS_RAW / MAX_MIGRATION_PREDECESSORS) {
            for i in 0..MAX_MIGRATION_PREDECESSORS as u8 {
                raw_at_bound.push(make_pred(i));
            }
        } // 16 * 64 = 1024 raw, 64 unique
        assert_eq!(raw_at_bound.len(), MAX_MIGRATION_PREDECESSORS_RAW);
        let req_raw_boundary = DelegateRequest::RegisterDelegateWithPredecessors {
            delegate: DelegateContainer::Wasm(DelegateWasmAPIVersion::V1(
                succ_raw_boundary.clone(),
            )),
            cipher: [7u8; 32],
            nonce: [9u8; 24],
            predecessors: raw_at_bound,
        };
        executor
            .delegate_request(
                req_raw_boundary,
                None,
                None,
                crate::client_events::ConnectionScope::Local,
                None,
            )
            .expect("a raw list at exactly the sanity bound (unique within cap) must be accepted");
        assert!(
            reg_path(&succ_raw_boundary).exists(),
            "the raw-bound boundary (raw == 1024, 64 unique) must register the successor"
        );
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

    // After the split, commit_state_write call sites live in runtime.rs (the V2
    // delegate callback installers), runtime/executor_impl.rs (the generic
    // bridged impl), and runtime/contract_ops.rs (the concrete PUT/UPDATE
    // chokepoints). Concatenate all three so the count covers every chokepoint.
    const RUNTIME_SRC: &str = concat!(
        include_str!("runtime.rs"),
        include_str!("runtime/executor_impl.rs"),
        include_str!("runtime/contract_ops.rs")
    );
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

    #[test]
    fn executor_update_state_call_sites_report_exec_cpu_micros() {
        // Cost-aware eviction (#4861): both executor `update_state` WASM
        // invocations — the upsert/apply chokepoint (`attempt_state_update`)
        // AND the sampled idempotency probe (`maybe_probe_idempotency`, which
        // fires precisely on the storm-relevant non-idempotent class) — MUST
        // attribute their elapsed on the `ExecCpuMicros` meter axis, the
        // signal the hosting sweep's cost-pressure trigger reads. A refactor
        // that drops a report silently re-opens the cost-blind-eviction gap
        // (a zero-demand contract burning update CPU is never an eviction
        // candidate) while every behavioral test stays green — the exact
        // failure mode of the "Manually-mirrored telemetry counters" row in
        // `.claude/rules/bug-prevention-patterns.md`. (The third ExecCpuMicros
        // reporter — the per-target send-time summarize/delta — lives in
        // broadcast_queue.rs, pinned there by
        // `broadcast_to_single_peer_reports_send_wasm_cost_pin`.)
        //
        // Split needle so this test's own source cannot self-count.
        let needle = concat!("ResourceType::", "Exec", "CpuMicros");
        let count = count_call_sites(RUNTIME_SRC, needle);
        assert_eq!(
            count, 2,
            "expected exactly 2 ExecCpuMicros report sites in the executor \
             runtime sources (attempt_state_update + the idempotency probe); \
             found {count}. If you added a WASM-execution chokepoint that \
             burns attributable CPU, report it on the same axis and bump \
             this expectation with a comment."
        );
    }

    #[test]
    fn v2_delegate_callback_installers_invalidate_state_caches() {
        // V2 delegate state writes (put/update_contract_state_sync) bypass
        // `StateStore::{store,update}`, so both `state_write_callback` installers
        // (in `from_config` and `from_config_with_shared_modules`) MUST drop
        // StateStore's cached view of the contract (moka state cache + change
        // detector). Dropping this would let a V2 write leave a stale cached
        // state/detector hash and the summarize/delta fast path serve a STALE
        // summary/delta → state divergence (Codex review).
        let count = count_call_sites(RUNTIME_SRC, "cache_invalidator.invalidate(");
        assert_eq!(
            count, 2,
            "expected exactly 2 `cache_invalidator.invalidate(` calls in \
             runtime.rs (one per V2 state_write_callback installer); found \
             {count}. If a callback installer stopped invalidating StateStore's \
             caches, a V2 delegate state write would leave stale cached state \
             and the summarize/delta fast path could serve a stale result."
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

#[cfg(test)]
mod update_by_instance_id_tests {
    //! Regression tests for freenet/freenet-core#4978.
    //!
    //! GET and SUBSCRIBE carry only an instance id on the wire, so the executor
    //! resolves the real code hash for them via `lookup_key`. UPDATE carries a
    //! full `ContractKey`, so whatever code hash the client put in it was taken
    //! at face value and fed straight to the
    //! `code_blob_stored(key.code_hash())` gate in
    //! `bridged_upsert_contract_state_inner`. Clients that address a contract by
    //! its instance id alone — `fdev update`, which fills in an all-zero
    //! `CodeHash` placeholder — therefore failed that gate with
    //! `MissingContract` on a node that was holding the contract all along.
    //!
    //! Scope note: the TypeScript SDK's `fromInstanceId()` emits a
    //! present-but-EMPTY code vector, which stdlib 0.8.5's
    //! `ContractKey::try_decode_fbs` refuses at the wire boundary — so that
    //! client is not fixed here and cannot be tested here. Only a well-formed
    //! but wrong 32-byte hash reaches this gate.
    //!
    //! These drive a real `Executor<Runtime>` (real `ContractStore`, real
    //! `StateStore`, no network) because the gate under test exists only there:
    //! `MockRuntime`'s executor keys everything by instance id and has no
    //! code-hash probe at all, so a `MockRuntime`-backed test would pass with
    //! the fix reverted. (`MockWasmRuntime` does wrap a real `ContractStore` and
    //! would hit the gate; a real `Runtime` is used anyway so the test exercises
    //! the production type.)

    use std::sync::Arc;

    use either::Either;
    use freenet_stdlib::prelude::{
        CodeHash, ContractCode, ContractContainer, ContractInstanceId, ContractKey,
        ContractWasmAPIVersion, Parameters, RelatedContracts, WrappedContract, WrappedState,
    };

    use crate::contract::executor::{ContractExecutor, Executor};
    use crate::contract::storages::Storage;
    use crate::wasm_runtime::{
        ContractStore, ContractStoreBridge, DelegateStore, Runtime, SecretsStore, StateStore,
    };

    /// Build a disk-backed `Executor<Runtime>`, holding the `TempDir` alive for
    /// the duration of the test.
    async fn build_executor(name: &str) -> (Executor<Runtime>, tempfile::TempDir) {
        let temp_dir = crate::util::tests::get_temp_dir();
        let db = Storage::new(temp_dir.path())
            .await
            .expect("create storage db");
        let contract_store = ContractStore::new(
            temp_dir.path().join(format!("contracts-{name}")),
            10_000,
            db.clone(),
        )
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
        (executor, temp_dir)
    }

    /// A synthetic contract container. The WASM bytes are deliberately garbage:
    /// the gate under test runs BEFORE any module is compiled, so an UPDATE that
    /// gets past it dies later in WASM validation instead. That asymmetry —
    /// refused-at-the-gate versus reached-the-engine — is what makes these
    /// assertions discriminating rather than a blanket "UPDATE fails".
    fn make_contract(code_seed: u8, param_seed: u8) -> (ContractContainer, ContractKey) {
        let code = ContractCode::from(vec![code_seed; 64]);
        let params = Parameters::from(vec![param_seed; 8]);
        let key = ContractKey::from_params_and_code(&params, &code);
        let wrapped = WrappedContract::new(Arc::new(code), params);
        (
            ContractContainer::Wasm(ContractWasmAPIVersion::V1(wrapped)),
            key,
        )
    }

    /// Seed the code blob, its instance→code index row, and the contract's
    /// params + state, so the executor is a node that genuinely holds the
    /// contract. Done through the store/state-store directly because this is
    /// setup, not the behaviour under test — routing it through a PUT would die
    /// in WASM validation on the synthetic bytes and remove the blob again.
    async fn seed_held_contract(
        executor: &mut Executor<Runtime>,
        container: ContractContainer,
        key: ContractKey,
        state: WrappedState,
    ) {
        let params = container.params().into_owned();
        executor
            .runtime
            .store_contract(container)
            .expect("seeding the code blob must succeed");
        executor
            .state_store
            .store(key, state, params)
            .await
            .expect("seeding params + state must succeed");
    }

    /// True when `err` is the `code_blob_stored` gate's refusal.
    ///
    /// Two near-misses this deliberately excludes, because the negative control
    /// below would silently accept either and report a broken fix as working:
    ///
    /// - the Display text "missing contract parameters" — a DIFFERENT failure
    ///   (`state_store.get_params` missing), raised BEFORE the gate, so a
    ///   lowercased "missing contract" substring would conflate the two;
    /// - `StateStoreError::MissingContract`, a different error type that shares
    ///   the variant NAME. (It reaches `ExecutorError` through
    ///   `ExecutorError::other(anyhow)`, whose `Debug` prints the Display chain
    ///   rather than the derived variant, so it would not in fact match the
    ///   needle below — but the name collision is exactly the kind of thing a
    ///   looser match would start catching, so the needle is anchored anyway.)
    ///
    /// So match the gate's own shape, `RequestError::ContractError` wrapping
    /// `StdContractError::MissingContract`.
    ///
    /// This predicate is only honest while something asserts it POSITIVELY: two
    /// of the three tests below assert `!missing_contract(..)`, which a needle
    /// that stopped matching would satisfy vacuously. That job belongs to
    /// `update_for_an_unheld_contract_still_reports_missing_contract` — do not
    /// `#[ignore]` or weaken it.
    fn missing_contract(err: &crate::contract::ExecutorError) -> bool {
        let rendered = format!("{err:?}");
        rendered.contains("ContractError(MissingContract")
    }

    /// An UPDATE addressed by instance id — the code hash a client that only
    /// knows the base58 instance id can supply — must reach the same place a
    /// fully-specified UPDATE reaches, on a node that holds the contract.
    ///
    /// Before the fix the placeholder-keyed call returned `MissingContract` from
    /// the `code_blob_stored` gate while the correctly-keyed call sailed past
    /// it, so this test fails on the first assertion with the fix reverted.
    #[tokio::test]
    async fn update_with_placeholder_code_hash_resolves_from_instance_id() {
        let (mut executor, _temp) = build_executor("update-by-instance-id").await;

        let (container, key) = make_contract(7, 7);
        seed_held_contract(
            &mut executor,
            container,
            key,
            WrappedState::new(vec![1u8; 8]),
        )
        .await;

        // Exactly what `fdev update` and the TS SDK's `fromInstanceId()` produce:
        // the right instance id, a code hash that names no blob.
        let placeholder = ContractKey::from_id_and_code(*key.id(), CodeHash::new([0u8; 32]));
        assert_ne!(
            placeholder.code_hash(),
            key.code_hash(),
            "the fixture must actually carry a wrong code hash"
        );

        let by_instance_id = executor
            .upsert_contract_state(
                placeholder,
                Either::Left(WrappedState::new(vec![2u8; 8])),
                RelatedContracts::default(),
                None,
            )
            .await;
        let err = by_instance_id
            .expect_err("synthetic WASM cannot validate, so this must fail somewhere");
        assert!(
            !missing_contract(&err),
            "an UPDATE addressed by instance id must resolve the code hash from \
             the store instead of failing the code_blob_stored gate (#4978), \
             got: {err:?}"
        );

        // Control: the fully-specified key must land in the same place, which is
        // what "addressing by instance id now works like GET/SUBSCRIBE" means.
        let by_full_key = executor
            .upsert_contract_state(
                key,
                Either::Left(WrappedState::new(vec![2u8; 8])),
                RelatedContracts::default(),
                None,
            )
            .await;
        let full_key_err =
            by_full_key.expect_err("synthetic WASM cannot validate, so this must fail too");
        assert!(
            !missing_contract(&full_key_err),
            "the fully-specified control must not hit the gate either: {full_key_err:?}"
        );
        // `ExecutorError` is a struct, so there is no variant to compare, and
        // the observable is the rendered error. Compare only the CONTRACT KEY
        // each error carries, not the whole rendering: the full text includes
        // the WASM engine's message, so equality on it would turn any engine
        // bump — or any future per-attempt annotation — into an intermittent
        // red with no useful diagnostic. The key is the part that says
        // instance-id addressing resolved to the same contract full-key
        // addressing did.
        let key_witness = format!("{:?}", key.code_hash());
        assert!(
            format!("{err:?}").contains(&key_witness),
            "the instance-id-addressed error must witness the resolved key: {err:?}"
        );
        assert!(
            format!("{full_key_err:?}").contains(&key_witness),
            "the full-key control must witness the same key: {full_key_err:?}"
        );
    }

    /// The resolution must not paper over a genuinely absent contract: with no
    /// instance→code row to resolve, the caller's key stands and the existing
    /// `MissingContract` refusal is unchanged. Without this the fix could look
    /// like it works simply because it stopped rejecting anything.
    #[tokio::test]
    async fn update_for_an_unheld_contract_still_reports_missing_contract() {
        let (mut executor, _temp) = build_executor("update-unheld-contract").await;

        // A different contract is held, so the store is populated but has no row
        // for the instance under test.
        let (container, held_key) = make_contract(7, 7);
        seed_held_contract(
            &mut executor,
            container,
            held_key,
            WrappedState::new(vec![1u8; 8]),
        )
        .await;

        let (_absent_container, absent_key) = make_contract(9, 9);
        let absent_instance: ContractInstanceId = *absent_key.id();
        assert!(
            executor.lookup_key(&absent_instance).is_none(),
            "fixture must leave the instance unresolvable"
        );

        // Params are seeded so the failure is the code gate rather than the
        // earlier "missing contract parameters" refusal.
        executor
            .state_store
            .store(
                absent_key,
                WrappedState::new(vec![1u8; 8]),
                Parameters::from(vec![9u8; 8]),
            )
            .await
            .expect("seeding params + state must succeed");

        let placeholder = ContractKey::from_id_and_code(absent_instance, CodeHash::new([0u8; 32]));
        let err = executor
            .upsert_contract_state(
                placeholder,
                Either::Left(WrappedState::new(vec![2u8; 8])),
                RelatedContracts::default(),
                None,
            )
            .await
            .expect_err("an UPDATE for a contract this node does not hold must fail");
        assert!(
            missing_contract(&err),
            "an unresolvable instance must still be refused with MissingContract, \
             got: {err:?}"
        );
    }

    /// The other direction, and the one this change actually NARROWS: a code
    /// hash naming a DIFFERENT contract's stored blob.
    ///
    /// Before the fix that hash passed `code_blob_stored` (the blob is on disk,
    /// it just belongs to another contract) and was carried onward as the key —
    /// into the state store, whose backends persist `key.code_hash()` into the
    /// hosting-metadata row they rebuild the `ContractKey` from on restart. So
    /// the pre-fix gate accepted the one wrong hash it should have caught and
    /// rejected the harmless one. After the fix the store's instance->code row
    /// overrides it and the key is the contract's own.
    ///
    /// The all-zero fixtures cannot see this: they exercise a hash that names no
    /// blob at all. Without this test, "a wrong hash is corrected rather than
    /// trusted" is an unverified claim.
    #[tokio::test]
    async fn update_with_another_contracts_code_hash_resolves_to_its_own() {
        let (mut executor, _temp) = build_executor("update-foreign-code-hash").await;

        let (container_a, key_a) = make_contract(7, 7);
        seed_held_contract(
            &mut executor,
            container_a,
            key_a,
            WrappedState::new(vec![1u8; 8]),
        )
        .await;

        // A second, genuinely-held contract with DIFFERENT code, so its blob is
        // on disk and `code_blob_stored` says yes for its hash.
        let (container_b, key_b) = make_contract(11, 11);
        seed_held_contract(
            &mut executor,
            container_b,
            key_b,
            WrappedState::new(vec![1u8; 8]),
        )
        .await;
        assert_ne!(
            key_a.code_hash(),
            key_b.code_hash(),
            "the fixtures must have distinct code blobs"
        );
        assert!(
            executor.runtime.code_blob_stored(key_b.code_hash()),
            "B's blob must really be on disk, or this fixture proves nothing"
        );

        // A's instance, B's code hash: the shape that used to sail through.
        let mismatched = ContractKey::from_id_and_code(*key_a.id(), *key_b.code_hash());

        // The resolution is what is under test, so assert it directly rather
        // than inferring it from an error string.
        assert_eq!(
            executor
                .lookup_key(mismatched.id())
                .expect("A is held, so its instance must resolve")
                .code_hash(),
            key_a.code_hash(),
            "the store's instance->code row must win over the caller's hash"
        );

        let err = executor
            .upsert_contract_state(
                mismatched,
                Either::Left(WrappedState::new(vec![2u8; 8])),
                RelatedContracts::default(),
                None,
            )
            .await
            .expect_err("synthetic WASM cannot validate, so this must fail");
        assert!(
            !missing_contract(&err),
            "a held contract must not be refused by the gate: {err:?}"
        );
        // The error carries the key the executor settled on, so it witnesses
        // WHICH contract the update was attributed to — B's hash here would mean
        // the foreign hash had been carried into the state store.
        assert!(
            format!("{err:?}").contains(&format!("{:?}", key_a.code_hash())),
            "the update must be attributed to A's own code hash, got: {err:?}"
        );
        assert!(
            !format!("{err:?}").contains(&format!("{:?}", key_b.code_hash())),
            "the foreign code hash must not survive resolution, got: {err:?}"
        );
    }
}
