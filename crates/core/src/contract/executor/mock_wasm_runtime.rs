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

    fn store_contract(&mut self, contract: ContractContainer) -> Result<(), anyhow::Error> {
        self.contract_store.store_contract(contract)
    }

    fn remove_contract(&mut self, key: &ContractKey) -> Result<(), anyhow::Error> {
        self.contract_store.remove_contract(key)
    }

    fn ensure_key_indexed(&mut self, key: &ContractKey) -> Result<(), anyhow::Error> {
        self.contract_store.ensure_key_indexed(key)
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
        _req: DelegateRequest<'_>,
        _origin_contract: Option<&ContractInstanceId>,
        _caller_delegate: Option<&DelegateKey>,
        _user_context: Option<&UserSecretContext>,
    ) -> Response {
        Err(ExecutorError::other(anyhow::anyhow!(
            "delegates not supported in MockWasmRuntime"
        )))
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

    /// Mutable access to the inner `MockWasmRuntime` so tests can install
    /// per-contract `validate_overrides` / `update_overrides` after the
    /// executor is wrapped in a handler. The `runtime` field is private to
    /// the `executor` module, so this accessor exists to surface it.
    #[cfg(test)]
    pub(crate) fn mock_runtime_mut(&mut self) -> &mut MockWasmRuntime {
        &mut self.runtime
    }
}
