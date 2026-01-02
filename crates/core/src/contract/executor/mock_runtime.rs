use super::*;
use crate::node::OpManager;
use crate::wasm_runtime::{MockStateStorage, StateStorage};
use std::sync::Arc;
use tokio::sync::mpsc::UnboundedSender;

pub(crate) struct MockRuntime {
    pub contract_store: ContractStore,
}

/// Executor with MockRuntime using disk-based storage (default for backward compatibility)
impl Executor<MockRuntime, Storage> {
    /// Create a mock executor with disk-based storage.
    ///
    /// This is the original implementation that stores state in SQLite on disk.
    /// For simulation testing with in-memory storage, use `new_mock_in_memory`.
    pub async fn new_mock(
        identifier: &str,
        op_sender: Option<OpRequestSender>,
        op_manager: Option<Arc<OpManager>>,
    ) -> anyhow::Result<Self> {
        let data_dir = Self::test_data_dir(identifier);

        let contracts_data_dir = data_dir.join("contracts");
        std::fs::create_dir_all(&contracts_data_dir).expect("directory created");
        let contract_store = ContractStore::new(contracts_data_dir, u16::MAX as i64)?;

        // FIXME: if is sqlite it should be a dir, named <data_dir>/db
        // let db_path = data_dir.join("db");
        // let state_store = StateStore::new(Storage::new(&db_path).await?, u16::MAX as u32).unwrap();
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

/// Executor with MockRuntime using in-memory storage (for deterministic simulation)
impl Executor<MockRuntime, MockStateStorage> {
    /// Create a mock executor with shared in-memory storage.
    ///
    /// This is designed for deterministic simulation testing where:
    /// - State persists across node crash/restart (same MockStateStorage instance)
    /// - No disk I/O timing variance
    /// - State can be inspected/verified through MockStateStorage methods
    ///
    /// # Arguments
    /// * `identifier` - Unique identifier for this executor (used for contract store path)
    /// * `shared_storage` - A MockStateStorage instance (clone it to share across restarts)
    /// * `op_sender` - Optional channel for network operations
    /// * `op_manager` - Optional reference to the operation manager
    pub async fn new_mock_in_memory(
        identifier: &str,
        shared_storage: MockStateStorage,
        op_sender: Option<OpRequestSender>,
        op_manager: Option<Arc<OpManager>>,
    ) -> anyhow::Result<Self> {
        let data_dir = Self::test_data_dir(identifier);

        let contracts_data_dir = data_dir.join("contracts");
        std::fs::create_dir_all(&contracts_data_dir).expect("directory created");
        let contract_store = ContractStore::new(contracts_data_dir, u16::MAX as i64)?;

        // Use the shared in-memory storage instead of disk
        let state_store = StateStore::new(shared_storage, u16::MAX as u32).unwrap();
        tracing::debug!("created in-memory state store for simulation");

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
        const MAX_SIZE: i64 = 10 * 1024 * 1024;
        const MAX_MEM_CACHE: u32 = 10_000_000;
        let tmp_dir = tempfile::tempdir()?;
        let state_store_path = tmp_dir.path().join("state_store");
        std::fs::create_dir_all(&state_store_path)?;
        let contract_store = ContractStore::new(tmp_dir.path().join("executor-test"), MAX_SIZE)?;
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
