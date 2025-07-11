use super::*;
use tokio::sync::mpsc::UnboundedSender;

pub(crate) struct MockRuntime {
    pub contract_store: ContractStore,
}

impl Executor<MockRuntime> {
    pub async fn new_mock(
        identifier: &str,
        event_loop_channel: ExecutorToEventLoopChannel<ExecutorHalve>,
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
            Some(event_loop_channel),
        )
        .await?;
        Ok(executor)
    }

    pub async fn handle_request(
        &mut self,
        _id: ClientId,
        _req: ClientRequest<'_>,
        _updates: Option<mpsc::UnboundedSender<Result<HostResponse, WsClientError>>>,
    ) -> Response {
        unreachable!("MockRuntime does not handle client requests directly")
    }
}

impl ContractExecutor for Executor<MockRuntime> {
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
        _key: ContractKey,
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
        )
        .await
        .expect("local node with handle");

        assert_eq!(counter, 1);
        Ok(())
    }
}
