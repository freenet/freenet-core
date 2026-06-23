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
    BackendEngine, MAX_STATE_SIZE, ModuleCache, RuntimeConfig, SharedModuleCache, UserSecretContext,
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
        user_context: Option<&UserSecretContext>,
    ) -> Response {
        self.delegate_request(req, origin_contract, caller_delegate, user_context)
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
        let runtime_config = RuntimeConfig {
            offload_compilation: production_offload_compilation(),
            module_cache_budget_bytes: config.module_cache_budget_bytes,
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
            // Local-node path (no hosted-mode connection): always single-user
            // (`user_context = None`), so secrets stay `SecretScope::Local`.
            ClientRequest::DelegateOp(op) => self.delegate_request(op, None, None, None),
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
            // (now multi-line, metrics-threaded) cache construction.
            .split("\n    ")
            .take(80)
            .collect::<String>()
            .split_whitespace()
            .collect::<Vec<_>>()
            .join(" ");
        assert!(
            body.contains("config.module_cache_budget_bytes"),
            "RuntimePool::new must size caches from config.module_cache_budget_bytes"
        );
        // The contract cache is built from `contract_cache_budget` with the
        // "contract" label; the delegate cache from `delegate_cache_budget` with
        // the "delegate" label. We assert each piece independently (rather than a
        // single literal call string) so threading the metrics `Arc` through
        // `with_label` (#4488) doesn't make this pin brittle.
        assert!(
            body.contains("ModuleCache::with_label( contract_cache_budget, \"contract\"")
                || body.contains("ModuleCache::with_label(contract_cache_budget, \"contract\""),
            "RuntimePool::new must build the contract cache from the contract byte budget"
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
