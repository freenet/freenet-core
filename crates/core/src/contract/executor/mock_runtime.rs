use super::*;

pub(crate) struct MockRuntime {
    pub contract_store: ContractStore,
}

impl Executor<MockRuntime> {
    pub async fn new_mock(
        identifier: &str,
        event_loop_channel: ExecutorToEventLoopChannel<ExecutorHalve>,
    ) -> Result<Self, DynError> {
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

    pub async fn handle_request<'a>(
        &mut self,
        _id: ClientId,
        _req: ClientRequest<'a>,
        _updates: Option<mpsc::UnboundedSender<Result<HostResponse, WsClientError>>>,
    ) -> Response {
        unreachable!()
    }
}

impl ContractExecutor for Executor<MockRuntime> {
    async fn fetch_contract(
        &mut self,
        key: ContractKey,
        fetch_contract: bool,
    ) -> Result<(WrappedState, Option<ContractContainer>), ExecutorError> {
        let Some(parameters) = self
            .state_store
            .get_params(&key)
            .await
            .map_err(ExecutorError::other)?
        else {
            return Err(ExecutorError::other(format!(
                "missing state and/or parameters for contract {key}"
            )));
        };
        let contract = if fetch_contract {
            self.runtime
                .contract_store
                .fetch_contract(&key, &parameters)
        } else {
            None
        };
        let Ok(state) = self.state_store.get(&key).await else {
            return Err(ExecutorError::other(format!(
                "missing state for contract {key}"
            )));
        };
        Ok((state, contract))
    }

    async fn store_contract(&mut self, contract: ContractContainer) -> Result<(), ExecutorError> {
        self.runtime
            .contract_store
            .store_contract(contract)
            .map_err(ExecutorError::other)?;
        Ok(())
    }

    async fn upsert_contract_state(
        &mut self,
        key: ContractKey,
        state: Either<WrappedState, StateDelta<'static>>,
        _related_contracts: RelatedContracts<'static>,
        code: Option<ContractContainer>,
    ) -> Result<WrappedState, ExecutorError> {
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
                Ok(incoming_state)
            }
            (Either::Left(incoming_state), None) => {
                // update case

                self.state_store
                    .update(&key, incoming_state.clone())
                    .await
                    .map_err(ExecutorError::other)?;
                Ok(incoming_state)
            }
            (update, contract) => unreachable!("{update:?}, {contract:?}"),
        }
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
