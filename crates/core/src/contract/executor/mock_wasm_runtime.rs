use super::*;
use crate::wasm_runtime::{
    ContractRuntimeInterface, ContractStoreBridge, InMemoryContractStore, MockStateStorage,
};

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
}

impl ContractRuntimeInterface for MockWasmRuntime {
    fn validate_state(
        &mut self,
        _key: &ContractKey,
        _parameters: &Parameters<'_>,
        _state: &WrappedState,
        _related: &RelatedContracts<'_>,
    ) -> crate::wasm_runtime::RuntimeResult<ValidateResult> {
        Ok(ValidateResult::Valid)
    }

    fn update_state(
        &mut self,
        _key: &ContractKey,
        _parameters: &Parameters<'_>,
        _state: &WrappedState,
        update_data: &[UpdateData<'_>],
    ) -> crate::wasm_runtime::RuntimeResult<UpdateModification<'static>> {
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
        notification_ch: tokio::sync::mpsc::UnboundedSender<HostResult>,
        summary: Option<StateSummary<'_>>,
    ) -> Result<(), Box<RequestError>> {
        self.bridged_register_contract_notifier(instance_id, cli_id, notification_ch, summary)
    }

    async fn execute_delegate_request(
        &mut self,
        _req: DelegateRequest<'_>,
        _attested_contract: Option<&ContractInstanceId>,
    ) -> Response {
        Err(ExecutorError::other(anyhow::anyhow!(
            "delegates not supported in MockWasmRuntime"
        )))
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

impl Executor<MockWasmRuntime, MockStateStorage> {
    pub async fn new_mock_wasm(
        _identifier: &str,
        shared_storage: MockStateStorage,
        op_sender: Option<OpRequestSender>,
        op_manager: Option<std::sync::Arc<crate::node::OpManager>>,
    ) -> anyhow::Result<Self> {
        let state_store =
            crate::wasm_runtime::StateStore::new(shared_storage.clone(), 10_000_000).unwrap();

        let runtime = MockWasmRuntime {
            contract_store: InMemoryContractStore::new(),
        };

        Executor::new(
            state_store,
            || Ok(()),
            OperationMode::Local,
            runtime,
            op_sender,
            op_manager,
        )
        .await
    }
}
