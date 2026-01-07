use super::*;
use crate::node::OpManager;
use crate::wasm_runtime::{InMemoryContractStore, MockStateStorage, StateStorage};
use std::sync::Arc;
use tokio::sync::mpsc::UnboundedSender;

/// Mock runtime for testing that uses fully in-memory storage.
///
/// Unlike the production runtime which uses disk-based storage with background
/// threads (file watchers, compaction), this runtime keeps everything in memory
/// for deterministic simulation testing.
pub(crate) struct MockRuntime {
    pub contract_store: InMemoryContractStore,
}

/// Executor with MockRuntime using disk-based state storage (for backward compatibility)
impl Executor<MockRuntime, Storage> {
    /// Create a mock executor with disk-based state storage.
    ///
    /// Contract code is stored in memory (InMemoryContractStore) for determinism,
    /// but state is stored on disk (SQLite). For fully in-memory storage, use
    /// `new_mock_in_memory`.
    pub async fn new_mock(
        identifier: &str,
        op_sender: Option<OpRequestSender>,
        op_manager: Option<Arc<OpManager>>,
    ) -> anyhow::Result<Self> {
        let data_dir = Self::test_data_dir(identifier);

        // Use in-memory contract store for deterministic behavior
        let contract_store = InMemoryContractStore::new();

        tracing::debug!("creating state store at path: {data_dir:?}");
        std::fs::create_dir_all(&data_dir).expect("directory created");
        let state_store = StateStore::new(Storage::new(&data_dir).await?, u16::MAX as u32).unwrap();
        tracing::debug!("state store created");

        let executor = Executor::new(
            state_store,
            || Ok(()),
            OperationMode::Local,
            MockRuntime { contract_store },
            op_sender,
            op_manager,
        )
        .await?;
        Ok(executor)
    }
}

/// Executor with MockRuntime using fully in-memory storage (for deterministic simulation)
impl Executor<MockRuntime, MockStateStorage> {
    /// Create a mock executor with fully in-memory storage.
    ///
    /// This is designed for deterministic simulation testing where:
    /// - Both contract code and state are stored in memory
    /// - No disk I/O or background threads (file watchers, compaction)
    /// - State persists across node crash/restart (same MockStateStorage instance)
    /// - State can be inspected/verified through MockStateStorage methods
    ///
    /// # Arguments
    /// * `_identifier` - Unused (kept for API compatibility)
    /// * `shared_storage` - A MockStateStorage instance (clone it to share across restarts)
    /// * `op_sender` - Optional channel for network operations
    /// * `op_manager` - Optional reference to the operation manager
    pub async fn new_mock_in_memory(
        _identifier: &str,
        shared_storage: MockStateStorage,
        op_sender: Option<OpRequestSender>,
        op_manager: Option<Arc<OpManager>>,
    ) -> anyhow::Result<Self> {
        // Use fully in-memory storage - no disk I/O, no background threads
        let contract_store = InMemoryContractStore::new();
        let state_store = StateStore::new(shared_storage, u16::MAX as u32).unwrap();
        tracing::debug!("created fully in-memory executor for deterministic simulation");

        let executor = Executor::new(
            state_store,
            || Ok(()),
            OperationMode::Local,
            MockRuntime { contract_store },
            op_sender,
            op_manager,
        )
        .await?;
        Ok(executor)
    }
}

/// Common methods for MockRuntime executors (works with any storage type)
impl<S> Executor<MockRuntime, S>
where
    S: StateStorage + Send + Sync + 'static,
    <S as StateStorage>::Error: Into<anyhow::Error>,
{
    pub async fn handle_request(
        &mut self,
        _id: ClientId,
        _req: ClientRequest<'_>,
        _updates: Option<mpsc::UnboundedSender<Result<HostResponse, WsClientError>>>,
    ) -> Response {
        unreachable!("MockRuntime does not handle client requests directly")
    }
}

/// ContractExecutor implementation for MockRuntime with any storage type
impl<S> ContractExecutor for Executor<MockRuntime, S>
where
    S: StateStorage + Send + Sync + 'static,
    <S as StateStorage>::Error: Into<anyhow::Error>,
{
    fn lookup_key(&self, instance_id: &ContractInstanceId) -> Option<ContractKey> {
        let code_hash = self.runtime.contract_store.code_hash_from_id(instance_id)?;
        Some(ContractKey::from_id_and_code(*instance_id, code_hash))
    }

    async fn fetch_contract(
        &mut self,
        key: ContractKey,
        return_contract_code: bool,
    ) -> Result<(Option<WrappedState>, Option<ContractContainer>), ExecutorError> {
        let Some(parameters) = self
            .state_store
            .get_params(&key)
            .await
            .map_err(ExecutorError::other)?
        else {
            return Err(ExecutorError::other(anyhow::anyhow!(
                "missing state and/or parameters for contract {key}"
            )));
        };
        let contract = if return_contract_code {
            self.runtime
                .contract_store
                .fetch_contract(&key, &parameters)
        } else {
            None
        };
        let Ok(state) = self.state_store.get(&key).await else {
            return Err(ExecutorError::other(anyhow::anyhow!(
                "missing state for contract {key}"
            )));
        };
        Ok((Some(state), contract))
    }

    async fn upsert_contract_state(
        &mut self,
        key: ContractKey,
        state: Either<WrappedState, StateDelta<'static>>,
        _related_contracts: RelatedContracts<'static>,
        code: Option<ContractContainer>,
    ) -> Result<UpsertResult, ExecutorError> {
        // todo: instead allow to perform mutations per contract based on incoming value so we can track
        // state values over the network
        match (state, code) {
            (Either::Left(incoming_state), Some(contract)) => {
                self.runtime
                    .contract_store
                    .store_contract(contract.clone())
                    .map_err(ExecutorError::other)?;
                self.state_store
                    .store(key, incoming_state.clone(), contract.params().into_owned())
                    .await
                    .map_err(ExecutorError::other)?;
                Ok(UpsertResult::Updated(incoming_state))
            }
            (Either::Left(incoming_state), None) => {
                // update case
                self.state_store
                    .update(&key, incoming_state.clone())
                    .await
                    .map_err(ExecutorError::other)?;
                Ok(UpsertResult::Updated(incoming_state))
            }
            (update, contract) => unreachable!("Invalid combination of state/delta and contract presence: {update:?}, {contract:?}"),
        }
    }

    fn register_contract_notifier(
        &mut self,
        _key: ContractInstanceId,
        _cli_id: ClientId,
        _notification_ch: UnboundedSender<HostResult>,
        _summary: Option<StateSummary<'_>>,
    ) -> Result<(), Box<RequestError>> {
        Ok(())
    }

    fn execute_delegate_request(
        &mut self,
        _req: DelegateRequest<'_>,
        _attested_contract: Option<&ContractInstanceId>,
    ) -> Response {
        Err(ExecutorError::other(anyhow::anyhow!(
            "not supported in mock runtime"
        )))
    }

    fn get_subscription_info(&self) -> Vec<crate::message::SubscriptionInfo> {
        vec![] // Mock implementation returns empty list
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[tokio::test(flavor = "multi_thread")]
    async fn local_node_handle() -> Result<(), Box<dyn std::error::Error>> {
        const MAX_MEM_CACHE: u32 = 10_000_000;
        let tmp_dir = tempfile::tempdir()?;
        let state_store_path = tmp_dir.path().join("state_store");
        std::fs::create_dir_all(&state_store_path)?;
        // Use in-memory contract store for deterministic behavior
        let contract_store = InMemoryContractStore::new();
        let state_store =
            StateStore::new(Storage::new(&state_store_path).await?, MAX_MEM_CACHE).unwrap();
        let mut counter = 0;
        Executor::new(
            state_store,
            || {
                counter += 1;
                Ok(())
            },
            OperationMode::Local,
            MockRuntime { contract_store },
            None,
            None,
        )
        .await
        .expect("local node with handle");

        assert_eq!(counter, 1);
        Ok(())
    }
}
