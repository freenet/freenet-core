use super::*;
use crate::wasm_runtime::{
    ContractRuntimeInterface, ContractStoreBridge, InMemoryContractStore, MockStateStorage,
    UserSecretContext,
};
use std::collections::HashMap;

/// Configurable validation behavior for testing related contracts.
#[derive(Clone, Debug)]
#[allow(dead_code)] // Variants constructed in test code only
pub(crate) enum ValidateOverride {
    /// Return `RequestRelated(ids)` on first call (when related map is empty),
    /// then `Valid` on second call (when related contracts are populated).
    RequestRelated(Vec<ContractInstanceId>),
    /// Always return `RequestRelated(ids)` regardless of provided related contracts.
    /// Used to test depth>1 rejection / repeated request rejection.
    AlwaysRequestRelated(Vec<ContractInstanceId>),
    /// Always return `Invalid`.
    Invalid,
    /// Return `RequestRelated` with an empty vec (malformed request).
    EmptyRequestRelated,
}

/// Configurable `update_state` behavior for testing related contract flows.
#[derive(Clone, Debug)]
#[allow(dead_code)] // Variants constructed in test code only
pub(crate) enum UpdateOverride {
    /// Return `UpdateModification::requires(...)` on first call (no
    /// `RelatedState` entry present in the updates yet) and accept the
    /// merge on second call (RelatedState entries populated by the
    /// bridged-upsert retry path). Mirrors `ValidateOverride::RequestRelated`
    /// but at the update-side of the fetch loop.
    RequiresRelated(Vec<ContractInstanceId>),
    /// Always return `UpdateModification::requires(...)` regardless of
    /// whether RelatedState entries are already populated. Drives the
    /// depth-limit branch in `bridged_upsert_contract_state`.
    AlwaysRequiresRelated(Vec<ContractInstanceId>),
    /// Always return `ContractError::InvalidUpdateWithInfo` with the given
    /// reason string. Used to test same-version / idempotent-push rejection
    /// paths (issue #4151): the returned error must be classified as
    /// `is_invalid_update_rejection()` and logged at DEBUG, not INFO.
    RejectInvalidUpdate { reason: String },
    /// Models a non-idempotent contract: every call to `update_state`
    /// returns a state that is byte-different from the previous one even
    /// when the input update is the same. The mock prepends an internal
    /// monotonically-increasing counter to the state bytes, mimicking the
    /// shape of a real contract that embeds a timestamp / position-
    /// dependent signature / re-signed payload — the smoking-gun shape
    /// the in-peer detector is built to catch (see #4251 and the
    /// `bdtchyck…wasm` analysis in `~/.claude/jobs/.../wasm-analysis.md`).
    /// Used by tests to verify the idempotency probe fires on a real-
    /// world-shaped failure mode.
    NonIdempotent(std::sync::Arc<std::sync::atomic::AtomicU64>),
    /// Models the #4295 false-positive shape: a correct, logically-
    /// idempotent contract whose serialization is non-canonical
    /// (HashMap/HashSet iteration order). Every call returns the logical
    /// input state's bytes ROTATED left by one — byte-different from the
    /// input but the same byte MULTISET — so the detectors must classify
    /// it as benign flutter, NOT a violation.
    ReorderBytes,
    /// Models a correct CANONICALIZING contract (the F3 false-positive
    /// shape for the identical-input probe): `update_state` normalizes a
    /// raw state ONCE — here, by stripping leading `0xFF` marker bytes, a
    /// genuine content (multiset) change — and is then STABLE: an input
    /// already in canonical form (no leading `0xFF`) is returned
    /// unchanged. The realistic scenario is a fresh PUT whose raw client
    /// bytes were installed without running `update_state`; the first
    /// merge canonicalizes, every later one is a fixpoint. The
    /// identical-input probe must NOT flag this shape.
    CanonicalizeOnce,
}

/// A lightweight mock runtime at the `ContractRuntimeInterface` level that lets
/// simulation tests exercise the **production** `ContractExecutor` code path
/// without requiring real WASM binaries.
///
/// Unlike `MockRuntime` (which has its own `ContractExecutor` impl with hash-based
/// merge), `MockWasmRuntime` delegates to the same `bridged_*` methods that
/// `Executor<Runtime>` uses, exercising init_tracker, validation, subscriber
/// notification pipeline, corrupted state recovery, and contract key indexing.
pub(crate) struct MockWasmRuntime {
    pub(crate) contract_store: InMemoryContractStore,
    /// Per-contract validation overrides for testing related contract flows.
    pub(crate) validate_overrides: HashMap<ContractInstanceId, ValidateOverride>,
    /// Per-contract update_state overrides for testing the
    /// `requires(missing)` fetch-and-retry path.
    pub(crate) update_overrides: HashMap<ContractInstanceId, UpdateOverride>,
    /// Scripted delegate runs (#5544). Each `execute_delegate_request` pops the
    /// next entry; an exhausted script returns no messages, which the caller
    /// reads as "the delegate is done".
    pub(crate) delegate_script: DelegateScript,
    /// One entry appended per `execute_delegate_request`, so a test can assert
    /// HOW MANY times the delegate's `process()` was entered and in what order
    /// relative to a park. This is what makes the per-delegate exclusion
    /// falsifiable: the invariant is "no second entry while parked".
    pub(crate) delegate_calls: DelegateCallLog,
    /// Model of the runtime's real `DelegateContextCache` (#5544 S7).
    ///
    /// Keyed by `DelegateKey`, last-write-wins — deliberately the same shape as
    /// the real thing, INCLUDING the property that makes it hazardous. Without
    /// this the mock could only show that an extra `process()` happened; with
    /// it, a test can show that the extra run CORRUPTED a parked continuation,
    /// which is the thing that actually matters.
    ///
    /// Two exclusion bypasses (the notification path and the inter-delegate
    /// hop) shipped in this PR's first round and were caught by review rather
    /// than by a test, precisely because the mock modelled call counts and not
    /// context continuity. Both are now caught by construction.
    pub(crate) delegate_contexts: DelegateContexts,
    /// What each invocation OBSERVED on entry, in order. The discriminator for
    /// an interleaving bug: a resumed run must observe the context ITS OWN
    /// pre-park run wrote, not one a foreign run left behind.
    pub(crate) delegate_observations: DelegateObservations,
}

/// One scripted delegate invocation.
#[derive(Default, Clone)]
pub(crate) struct ScriptedRun {
    /// Messages this invocation emits.
    pub outbound: Vec<OutboundDelegateMsg>,
    /// Context bytes this invocation writes on exit (`None` leaves it alone),
    /// modelling the delegate's `ctx.write()`.
    pub writes_context: Option<Vec<u8>>,
}

impl From<Vec<OutboundDelegateMsg>> for ScriptedRun {
    fn from(outbound: Vec<OutboundDelegateMsg>) -> Self {
        Self {
            outbound,
            writes_context: None,
        }
    }
}

/// What one invocation saw when it entered.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct DelegateObservation {
    pub delegate_key: DelegateKey,
    /// Inbound message kinds, so a test can tell WHICH logical run this was —
    /// a `UserResponse` is a resume, a `ContractNotification` is the
    /// notification path, and so on. Identifying the run by script position
    /// would not work: the whole point of an interleaving bug is that the runs
    /// arrive in a different order than intended.
    pub inbound_kinds: Vec<&'static str>,
    /// The context this invocation read on entry.
    pub observed_context: Option<Vec<u8>>,
}

/// Shared handle to a mock delegate's scripted runs.
pub(crate) type DelegateScript =
    std::sync::Arc<std::sync::Mutex<std::collections::VecDeque<ScriptedRun>>>;

/// Shared handle to the log of delegate invocations.
pub(crate) type DelegateCallLog = std::sync::Arc<std::sync::Mutex<Vec<DelegateKey>>>;

/// Shared handle to the modelled per-delegate context store.
pub(crate) type DelegateContexts =
    std::sync::Arc<std::sync::Mutex<std::collections::HashMap<DelegateKey, Vec<u8>>>>;

/// Shared handle to the per-invocation observation log.
pub(crate) type DelegateObservations = std::sync::Arc<std::sync::Mutex<Vec<DelegateObservation>>>;

/// Name the inbound variants so an observation can identify its logical run.
fn inbound_kind(msg: &InboundDelegateMsg<'_>) -> &'static str {
    match msg {
        InboundDelegateMsg::ApplicationMessage(_) => "ApplicationMessage",
        InboundDelegateMsg::UserResponse(_) => "UserResponse",
        InboundDelegateMsg::GetContractResponse(_) => "GetContractResponse",
        InboundDelegateMsg::PutContractResponse(_) => "PutContractResponse",
        InboundDelegateMsg::UpdateContractResponse(_) => "UpdateContractResponse",
        InboundDelegateMsg::SubscribeContractResponse(_) => "SubscribeContractResponse",
        InboundDelegateMsg::ContractNotification(_) => "ContractNotification",
        InboundDelegateMsg::DelegateMessage(_) => "DelegateMessage",
        _ => "Other",
    }
}

impl ContractRuntimeInterface for MockWasmRuntime {
    fn validate_state(
        &mut self,
        key: &ContractKey,
        _parameters: &Parameters<'_>,
        _state: &WrappedState,
        related: &RelatedContracts<'_>,
    ) -> crate::wasm_runtime::RuntimeResult<ValidateResult> {
        let instance_id = key.id();
        if let Some(override_behavior) = self.validate_overrides.get(instance_id).cloned() {
            return Ok(match override_behavior {
                ValidateOverride::RequestRelated(ids) => {
                    // First call (related empty) → RequestRelated.
                    // Second call (related populated after fetch) → Valid.
                    let has_populated = related
                        .clone()
                        .into_owned()
                        .states()
                        .any(|(_, s)| s.is_some());
                    if has_populated {
                        ValidateResult::Valid
                    } else {
                        ValidateResult::RequestRelated(ids)
                    }
                }
                ValidateOverride::AlwaysRequestRelated(ids) => ValidateResult::RequestRelated(ids),
                ValidateOverride::Invalid => ValidateResult::Invalid,
                ValidateOverride::EmptyRequestRelated => ValidateResult::RequestRelated(vec![]),
            });
        }
        Ok(ValidateResult::Valid)
    }

    fn update_state(
        &mut self,
        _key: &ContractKey,
        _parameters: &Parameters<'_>,
        _state: &WrappedState,
        update_data: &[UpdateData<'_>],
    ) -> crate::wasm_runtime::RuntimeResult<UpdateModification<'static>> {
        // If a per-contract override is wired, replay the production
        // require-then-merge flow: first call (no RelatedState in the
        // update_data slice) returns `requires`; once the bridged path
        // re-attempts with `RelatedState` entries appended, fall through
        // to the default merge.
        if let Some(override_) = self.update_overrides.get(_key.id()).cloned() {
            match override_ {
                UpdateOverride::RequiresRelated(ids) => {
                    let has_related = update_data
                        .iter()
                        .any(|u| matches!(u, UpdateData::RelatedState { .. }));
                    if !has_related {
                        let related: Vec<RelatedContract> = ids
                            .iter()
                            .map(|id| RelatedContract {
                                contract_instance_id: *id,
                                mode: RelatedMode::StateOnce,
                            })
                            .collect();
                        return Ok(UpdateModification::requires(related)
                            .map_err(|e| anyhow::anyhow!("{e}"))?);
                    }
                }
                UpdateOverride::AlwaysRequiresRelated(ids) => {
                    let related: Vec<RelatedContract> = ids
                        .iter()
                        .map(|id| RelatedContract {
                            contract_instance_id: *id,
                            mode: RelatedMode::StateOnce,
                        })
                        .collect();
                    return Ok(UpdateModification::requires(related)
                        .map_err(|e| anyhow::anyhow!("{e}"))?);
                }
                UpdateOverride::RejectInvalidUpdate { reason } => {
                    // Simulate the WASM contract returning InvalidUpdateWithInfo
                    // (e.g., "New state version X must be higher than current version X").
                    // This produces an error that `ExecutorError::is_invalid_update_rejection()`
                    // must classify as a benign idempotent-push rejection.
                    use crate::wasm_runtime::ContractExecError;
                    use freenet_stdlib::prelude::ContractError as StdlibContractError;
                    let inner_err = StdlibContractError::InvalidUpdateWithInfo { reason };
                    return Err(crate::wasm_runtime::ContractError::from(
                        ContractExecError::ContractError(inner_err),
                    ));
                }
                UpdateOverride::NonIdempotent(counter) => {
                    // Pick the "logical" input state — same precedence as
                    // the default branch — then prepend a monotonically-
                    // increasing 8-byte counter. Re-running with the
                    // produced state as input still bumps the counter,
                    // so the result is byte-different every call.
                    let logical = update_data
                        .iter()
                        .find_map(|u| match u {
                            UpdateData::State(s) => Some(s.as_ref().to_vec()),
                            UpdateData::Delta(d) => Some(d.as_ref().to_vec()),
                            UpdateData::StateAndDelta { state, .. } => {
                                Some(state.as_ref().to_vec())
                            }
                            UpdateData::RelatedState { .. }
                            | UpdateData::RelatedDelta { .. }
                            | UpdateData::RelatedStateAndDelta { .. } => None,
                            // `UpdateData` is `#[non_exhaustive]`; allow future
                            // variants to fall through to the `_state` fallback.
                            _ => None,
                        })
                        .unwrap_or_else(|| _state.as_ref().to_vec());
                    let n = counter.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                    let mut out = Vec::with_capacity(8 + logical.len().saturating_sub(8));
                    out.extend_from_slice(&n.to_le_bytes());
                    // Strip any prior counter prefix so the state stays a
                    // fixed size — matching the 464-byte shape we saw in
                    // production rather than growing unboundedly.
                    let tail_start = logical.len().min(8);
                    out.extend_from_slice(&logical[tail_start..]);
                    return Ok(UpdateModification::valid(out.into()));
                }
                UpdateOverride::ReorderBytes => {
                    // Same logical-input precedence as the other overrides,
                    // then rotate: byte-different output, identical byte
                    // multiset (the #4295 serialization-flutter shape).
                    let mut out = update_data
                        .iter()
                        .find_map(|u| match u {
                            UpdateData::State(s) => Some(s.as_ref().to_vec()),
                            UpdateData::Delta(d) => Some(d.as_ref().to_vec()),
                            UpdateData::StateAndDelta { state, .. } => {
                                Some(state.as_ref().to_vec())
                            }
                            UpdateData::RelatedState { .. }
                            | UpdateData::RelatedDelta { .. }
                            | UpdateData::RelatedStateAndDelta { .. } => None,
                            // `UpdateData` is `#[non_exhaustive]`; fall
                            // through to the `_state` fallback.
                            _ => None,
                        })
                        .unwrap_or_else(|| _state.as_ref().to_vec());
                    if !out.is_empty() {
                        out.rotate_left(1);
                    }
                    return Ok(UpdateModification::valid(out.into()));
                }
                UpdateOverride::CanonicalizeOnce => {
                    // Canonical form: leading 0xFF marker bytes stripped.
                    // Non-canonical input → normalized output (a genuine
                    // multiset change). Canonical input → returned
                    // unchanged (fixpoint).
                    let logical = update_data
                        .iter()
                        .find_map(|u| match u {
                            UpdateData::State(s) => Some(s.as_ref().to_vec()),
                            UpdateData::Delta(d) => Some(d.as_ref().to_vec()),
                            UpdateData::StateAndDelta { state, .. } => {
                                Some(state.as_ref().to_vec())
                            }
                            UpdateData::RelatedState { .. }
                            | UpdateData::RelatedDelta { .. }
                            | UpdateData::RelatedStateAndDelta { .. } => None,
                            // `UpdateData` is `#[non_exhaustive]`; fall
                            // through to the `_state` fallback.
                            _ => None,
                        })
                        .unwrap_or_else(|| _state.as_ref().to_vec());
                    let canonical_start = logical
                        .iter()
                        .position(|b| *b != 0xFF)
                        .unwrap_or(logical.len());
                    let out = logical[canonical_start..].to_vec();
                    return Ok(UpdateModification::valid(out.into()));
                }
            }
        }
        // Accept the last full state or delta from update_data as the new state
        let mut new_state = None;
        for ud in update_data {
            match ud {
                UpdateData::State(state) => {
                    new_state = Some(state.clone().into_owned());
                }
                UpdateData::Delta(delta) => {
                    new_state = Some(State::from(delta.as_ref().to_vec()));
                }
                UpdateData::StateAndDelta { state, .. } => {
                    new_state = Some(state.clone().into_owned());
                }
                UpdateData::RelatedState { .. }
                | UpdateData::RelatedDelta { .. }
                | UpdateData::RelatedStateAndDelta { .. } => {
                    // Ignore related data for the merge
                }
                // `UpdateData` is `#[non_exhaustive]` since stdlib 0.6.0.
                // Mock-only path: ignore future variants for the merge.
                _ => {}
            }
        }
        match new_state {
            Some(state) => Ok(UpdateModification::valid(state)),
            None => Ok(UpdateModification::valid(_state.as_ref().to_vec().into())),
        }
    }

    fn summarize_state(
        &mut self,
        _key: &ContractKey,
        _parameters: &Parameters<'_>,
        state: &WrappedState,
    ) -> crate::wasm_runtime::RuntimeResult<StateSummary<'static>> {
        Ok(StateSummary::from(
            blake3::hash(state.as_ref()).as_bytes().to_vec(),
        ))
    }

    fn get_state_delta(
        &mut self,
        _key: &ContractKey,
        _parameters: &Parameters<'_>,
        state: &WrappedState,
        _summary: &StateSummary<'_>,
    ) -> crate::wasm_runtime::RuntimeResult<StateDelta<'static>> {
        // Pessimistic: always return the full state as the delta
        Ok(StateDelta::from(state.as_ref().to_vec()))
    }
}

impl ContractStoreBridge for MockWasmRuntime {
    fn code_hash_from_id(&self, id: &ContractInstanceId) -> Option<CodeHash> {
        self.contract_store.code_hash_from_id(id)
    }

    fn fetch_contract_code(
        &self,
        key: &ContractKey,
        params: &Parameters<'_>,
    ) -> Option<ContractContainer> {
        self.contract_store.fetch_contract(key, params)
    }

    fn code_blob_stored(&self, code_hash: &CodeHash) -> bool {
        self.contract_store.code_blob_stored(code_hash)
    }

    fn store_contract(&mut self, contract: ContractContainer) -> Result<(), anyhow::Error> {
        self.contract_store.store_contract(contract)
    }

    fn remove_contract(&mut self, key: &ContractKey) -> Result<(), anyhow::Error> {
        self.contract_store.remove_contract(key)
    }
}

impl crate::wasm_runtime::ContractRuntimeBridge for MockWasmRuntime {}

impl ContractExecutor for Executor<MockWasmRuntime, MockStateStorage> {
    fn lookup_key(&self, instance_id: &ContractInstanceId) -> Option<ContractKey> {
        self.bridged_lookup_key(instance_id)
    }

    fn op_manager_handle(&self) -> Option<std::sync::Arc<crate::node::OpManager>> {
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
        super::runtime::bridged_upsert_outcome(
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
        _origin_contract: Option<&ContractInstanceId>,
        _caller_delegate: Option<&DelegateKey>,
        _connection_scope: crate::client_events::ConnectionScope,
        _user_context: Option<&UserSecretContext>,
    ) -> Response {
        // Scripted, for the #5544 park/resume tests. Without a script this
        // stays the "not supported" error it has always been, so no existing
        // test changes behaviour.
        let key = req.key().clone();

        // Model the real `DelegateContextCache` read-modify-write around every
        // invocation (#5544 S7): read on entry, record what was seen, write on
        // exit. Same key, same last-write-wins semantics as the runtime's.
        let observed_context = self
            .runtime
            .delegate_contexts
            .lock()
            .unwrap()
            .get(&key)
            .cloned();
        // Only `ApplicationMessages` carries inbound to classify; the
        // registration variants carry none. The wildcard is required by
        // `#[non_exhaustive]` and is listed alongside the known variants so a
        // future one is not silently swallowed.
        let inbound_kinds: Vec<&'static str> = match &req {
            DelegateRequest::ApplicationMessages { inbound, .. } => {
                inbound.iter().map(inbound_kind).collect()
            }
            DelegateRequest::RegisterDelegate { .. }
            | DelegateRequest::UnregisterDelegate(_)
            | DelegateRequest::RegisterDelegateWithPredecessors { .. }
            | _ => Vec::new(),
        };

        let script = self.runtime.delegate_script.lock().unwrap().pop_front();
        let record = |rt: &MockWasmRuntime, writes: Option<Vec<u8>>| {
            rt.delegate_calls.lock().unwrap().push(key.clone());
            rt.delegate_observations
                .lock()
                .unwrap()
                .push(DelegateObservation {
                    delegate_key: key.clone(),
                    inbound_kinds: inbound_kinds.clone(),
                    observed_context: observed_context.clone(),
                });
            if let Some(bytes) = writes {
                rt.delegate_contexts
                    .lock()
                    .unwrap()
                    .insert(key.clone(), bytes);
            }
        };

        let Some(run) = script else {
            if self.runtime.delegate_calls.lock().unwrap().is_empty() {
                return Err(ExecutorError::other(anyhow::anyhow!(
                    "delegates not supported in MockWasmRuntime"
                )));
            }
            // Script exhausted after a real scripted run: the delegate has
            // nothing more to say. Record the entry so exclusion assertions
            // still see it.
            record(&self.runtime, None);
            return Ok(freenet_stdlib::client_api::HostResponse::DelegateResponse {
                key,
                values: Vec::new(),
            });
        };
        record(&self.runtime, run.writes_context);
        Ok(freenet_stdlib::client_api::HostResponse::DelegateResponse {
            key,
            values: run.outbound,
        })
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
}

impl Executor<MockWasmRuntime, MockStateStorage> {
    pub async fn new_mock_wasm(
        _identifier: &str,
        shared_storage: MockStateStorage,
        contract_store: Option<InMemoryContractStore>,
        op_manager: Option<std::sync::Arc<crate::node::OpManager>>,
    ) -> anyhow::Result<Self> {
        let state_store =
            crate::wasm_runtime::StateStore::new(shared_storage.clone(), 10_000_000).unwrap();

        let runtime = MockWasmRuntime {
            contract_store: contract_store.unwrap_or_default(),
            validate_overrides: HashMap::new(),
            update_overrides: HashMap::new(),
            delegate_script: DelegateScript::default(),
            delegate_calls: DelegateCallLog::default(),
            delegate_contexts: DelegateContexts::default(),
            delegate_observations: DelegateObservations::default(),
        };

        Executor::new(
            state_store,
            || Ok(()),
            OperationMode::Local,
            runtime,
            op_manager,
        )
        .await
    }

    /// Like [`new_mock_wasm`](Self::new_mock_wasm) but with an UNCACHED
    /// `StateStore`, so every state load hits the backing storage (and is
    /// therefore observable via `MockStateStorage::get_count`). Used by the
    /// summarize/delta change-detector tests to prove the fast path skips the
    /// state load+hash entirely on an unchanged contract — with the moka state
    /// cache on, a load would be served from memory and the `get_count` probe
    /// could not distinguish fast path from slow path.
    #[cfg(test)]
    pub async fn new_mock_wasm_uncached(
        _identifier: &str,
        shared_storage: MockStateStorage,
    ) -> anyhow::Result<Self> {
        let state_store = crate::wasm_runtime::StateStore::new_uncached(shared_storage);

        let runtime = MockWasmRuntime {
            contract_store: InMemoryContractStore::default(),
            validate_overrides: HashMap::new(),
            update_overrides: HashMap::new(),
            delegate_script: DelegateScript::default(),
            delegate_calls: DelegateCallLog::default(),
            delegate_contexts: DelegateContexts::default(),
            delegate_observations: DelegateObservations::default(),
        };

        Executor::new(state_store, || Ok(()), OperationMode::Local, runtime, None).await
    }

    /// Like [`new_mock_wasm_uncached`](Self::new_mock_wasm_uncached) but also wires
    /// a real `OpManager`, so `Ring::hosting_contracts_count()` reflects contracts
    /// hosted via `op_manager.ring.host_contract(..)`. Used by the summary-cache
    /// sizing test to prove `ensure_cache_covers_hosted_set` grows the caches to
    /// the live hosted count (observable via `MockStateStorage::get_count`).
    #[cfg(test)]
    pub async fn new_mock_wasm_uncached_with_op_manager(
        _identifier: &str,
        shared_storage: MockStateStorage,
        op_manager: std::sync::Arc<crate::node::OpManager>,
    ) -> anyhow::Result<Self> {
        let state_store = crate::wasm_runtime::StateStore::new_uncached(shared_storage);

        let runtime = MockWasmRuntime {
            contract_store: InMemoryContractStore::default(),
            validate_overrides: HashMap::new(),
            update_overrides: HashMap::new(),
            delegate_script: DelegateScript::default(),
            delegate_calls: DelegateCallLog::default(),
            delegate_contexts: DelegateContexts::default(),
            delegate_observations: DelegateObservations::default(),
        };

        Executor::new(
            state_store,
            || Ok(()),
            OperationMode::Local,
            runtime,
            Some(op_manager),
        )
        .await
    }

    /// Mutable access to the inner `MockWasmRuntime` so tests can install
    /// per-contract `validate_overrides` / `update_overrides` after the
    /// executor is wrapped in a handler. The `runtime` field is private to
    /// the `executor` module, so this accessor exists to surface it.
    #[cfg(test)]
    pub(crate) fn mock_runtime_mut(&mut self) -> &mut MockWasmRuntime {
        &mut self.runtime
    }
}
