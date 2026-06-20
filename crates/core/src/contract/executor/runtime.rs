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

mod executor_impl;
mod pool;
pub use pool::RuntimePool;

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

    pub fn delegate_request(
        &mut self,
        req: DelegateRequest<'_>,
        origin_contract: Option<&ContractInstanceId>,
        caller_delegate: Option<&DelegateKey>,
        user_context: Option<&UserSecretContext>,
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
                    // The per-user secret scope, present only in hosted mode and
                    // derived solely from the connection token. It is delivered
                    // here on a SEPARATE channel from `origin`/the request body,
                    // so neither WASM nor any delegate-message content can set or
                    // change which user's namespace a secret op touches.
                    user_context,
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
        // Plain read, no timestamp update. The "last used" time is refreshed in
        // inbound_app_message instead, so a child that only ever gets messages
        // from other delegates (those don't reach this branch) still counts as
        // active and isn't dropped.
        crate::wasm_runtime::DELEGATE_INHERITED_ORIGINS
            .get(delegate_key)
            .and_then(|entry| entry.origins.first().copied().map(MessageOrigin::WebApp))
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
        crate::wasm_runtime::DELEGATE_INHERITED_ORIGINS.insert(
            recipient.clone(),
            crate::wasm_runtime::InheritedOriginsEntry::new(vec![inherited_contract]),
        );

        let origin = resolve_message_origin(Some(&caller), None, &recipient);

        // Cleanup before assertions so a panic doesn't leak state into
        // sibling tests sharing the same process.
        crate::wasm_runtime::DELEGATE_INHERITED_ORIGINS.remove(&recipient);

        match origin {
            Some(MessageOrigin::Delegate(k)) => assert_eq!(k, caller),
            other => panic!("Expected Delegate(caller), got {other:?}"),
        }
    }

    /// Fallback branch (no live caller/origin) yields the child's inherited
    /// WebApp origin via a pure read — it does not refresh `last_access`
    /// (liveness lives in `inbound_app_message`). Pairs with
    /// `no_arguments_and_no_inherited_yields_none`.
    #[test]
    fn inherited_origin_fallback_yields_webapp() {
        use crate::wasm_runtime::{DELEGATE_INHERITED_ORIGINS, InheritedOriginsEntry};

        let recipient = dkey(0xC5);
        let contract = ContractInstanceId::new([0xC6; 32]);
        DELEGATE_INHERITED_ORIGINS.insert(
            recipient.clone(),
            InheritedOriginsEntry::new(vec![contract]),
        );

        let origin = resolve_message_origin(None, None, &recipient);
        DELEGATE_INHERITED_ORIGINS.remove(&recipient);

        assert!(
            matches!(origin, Some(MessageOrigin::WebApp(c)) if c == contract),
            "fallback must yield the inherited WebApp origin, got {origin:?}"
        );
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
        // After the file split, `fetch_related_for_validation` lives in
        // runtime/executor_impl.rs and `fetch_related_for_validation_network`
        // lives in runtime.rs. Search each function in its own source file so
        // that the needle string used in the search does not appear as a
        // string literal inside this test (which would make nth(1) extract the
        // test code instead of the production function body).
        const RUNTIME_SRC: &str = include_str!("runtime.rs");
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
                RUNTIME_SRC,
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
                let window = &seg[..seg.len().min(1_500)];
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
        assert!(
            after_marker[..2_000].contains("join_all"),
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
            .split("\n    ")
            .take(60)
            .collect::<String>();
        assert!(
            body.contains("config.module_cache_budget_bytes"),
            "RuntimePool::new must size caches from config.module_cache_budget_bytes"
        );
        assert!(
            body.contains("ModuleCache::with_label(contract_cache_budget, \"contract\")"),
            "RuntimePool::new must build the contract cache from the contract byte budget"
        );
        assert!(
            body.contains("ModuleCache::with_label(delegate_cache_budget, \"delegate\")"),
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

    // After the split, commit_state_write call sites live in both runtime.rs and
    // runtime/executor_impl.rs (the generic bridged impl). Concatenate both so the
    // count covers all chokepoints.
    const RUNTIME_SRC: &str = concat!(
        include_str!("runtime.rs"),
        include_str!("runtime/executor_impl.rs")
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
