use super::{runtime::RuntimePool, *};
use std::sync::Arc;
use tokio::sync::mpsc::{self, UnboundedSender};
use tokio::sync::Semaphore;

pub(crate) struct MockRuntime {
    pub contract_store: ContractStore,
}

impl Executor<MockRuntime> {
    pub async fn handle_request(
        &mut self,
        _id: ClientId,
        _req: ClientRequest<'_>,
        _updates: Option<mpsc::UnboundedSender<Result<HostResponse, WsClientError>>>,
    ) -> Response {
        unreachable!("MockRuntime does not handle client requests directly")
    }

    async fn perform_contract_get(
        &mut self,
        return_contract_code: bool,
        key: ContractKey,
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

        match self.state_store.get(&key).await {
            Ok(state) => Ok((Some(state), contract)),
            Err(_) => Err(ExecutorError::other(anyhow::anyhow!(
                "missing state for contract {key}"
            ))),
        }
    }
}

impl RuntimePool<String, MockRuntime> {
    pub async fn new_mock(
        identifier: &str,
        to_process_tx: mpsc::Sender<(
            Transaction,
            tokio::sync::oneshot::Sender<Result<OpEnum, CallbackError>>,
        )>,
        op_manager: Arc<OpManager>,
        num_executors: std::num::NonZeroUsize,
    ) -> anyhow::Result<Self> {
        let data_dir = Executor::<MockRuntime>::test_data_dir(identifier);

        let contracts_data_dir = data_dir.join("contracts");
        std::fs::create_dir_all(&contracts_data_dir).expect("directory created");
        let contract_store = ContractStore::new(contracts_data_dir, u16::MAX as i64)?;

        tracing::debug!("creating state store at path: {data_dir:?}");
        std::fs::create_dir_all(&data_dir).expect("directory created");

        let mut runtimes = Vec::with_capacity(1);

        let exec_dir = data_dir.join(format!("executor-{identifier}"));
        std::fs::create_dir_all(&exec_dir).expect("directory created");

        let state_store = StateStore::new(Storage::new(&exec_dir).await?, u16::MAX as u32).unwrap();

        let executor = Executor::new(
            state_store,
            OperationMode::Local,
            MockRuntime { contract_store },
            Some(to_process_tx.clone()),
            Some(op_manager.clone()),
        )
        .await?;

        runtimes.push(Some(executor));

        Ok(RuntimePool {
            runtimes,
            available: Semaphore::new(num_executors.get()),
            op_sender: to_process_tx,
            op_manager,
            config: identifier.to_string(),
        })
    }

    // Pop an executor from the pool - blocks until one is available
    async fn pop_executor(&mut self) -> Executor<MockRuntime> {
        // Wait for an available permit
        let _ = self.available.acquire().await.expect("Semaphore is closed");

        // Find the first available executor
        for slot in &mut self.runtimes {
            if let Some(executor) = slot.take() {
                return executor;
            }
        }

        // This should never happen because of the semaphore
        unreachable!("No executors available despite semaphore permit")
    }
}

impl ContractExecutor for RuntimePool<String, MockRuntime> {
    type InnerExecutor = Executor<MockRuntime>;

    fn return_executor(&mut self, executor: Self::InnerExecutor) {
        // Find an empty slot and return the executor
        if let Some(empty_slot) = self.runtimes.iter_mut().find(|slot| slot.is_none()) {
            *empty_slot = Some(executor);
            self.available.add_permits(1);
        } else {
            unreachable!("No empty slot found in the pool");
        }
    }

    async fn create_new_executor(&mut self) -> Self::InnerExecutor {
        // Create a new executor with a unique directory
        let identifier = self.config.clone();
        let data_dir = Executor::<MockRuntime>::test_data_dir(&identifier);
        let exec_dir = data_dir.join(format!(
            "executor-new-{}",
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_millis()
        ));
        std::fs::create_dir_all(&exec_dir).expect("directory created");

        let contracts_data_dir = data_dir.join("contracts");
        std::fs::create_dir_all(&contracts_data_dir).expect("directory created");
        let contract_store = ContractStore::new(contracts_data_dir, u16::MAX as i64)
            .expect("Failed to create contract store");

        let state_store =
            StateStore::new(Storage::new(&exec_dir).await.unwrap(), u16::MAX as u32).unwrap();

        Executor::new(
            state_store,
            OperationMode::Local,
            MockRuntime { contract_store },
            Some(self.op_sender.clone()),
            Some(self.op_manager.clone()),
        )
        .await
        .expect("Failed to create new executor")
    }

    async fn fetch_contract(
        &mut self,
        key: ContractKey,
        return_contract_code: bool,
    ) -> impl Future<
        Output = (
            Self::InnerExecutor,
            Result<(Option<WrappedState>, Option<ContractContainer>), ExecutorError>,
        ),
    > + Send
           + 'static {
        let mut executor = self.pop_executor().await;

        async move {
            let result = executor
                .perform_contract_get(return_contract_code, key)
                .await;
            (executor, result)
        }
    }

    async fn upsert_contract_state(
        &mut self,
        key: ContractKey,
        state: Either<WrappedState, StateDelta<'static>>,
        _related_contracts: RelatedContracts<'static>,
        code: Option<ContractContainer>,
    ) -> impl Future<Output = (Self::InnerExecutor, Result<UpsertResult, ExecutorError>)> + Send + 'static
    {
        let mut executor = self.pop_executor().await;
        // todo: instead allow to perform mutations per contract based on incoming value so we can track
        // state values over the network
        async move {
            let r = async {
                match (state, code) {
                    (Either::Left(incoming_state), Some(contract)) => {
                        executor
                            .runtime
                            .contract_store
                            .store_contract(contract.clone())
                            .map_err(ExecutorError::other)?;
                        executor
                            .state_store
                            .store(key, incoming_state.clone(), contract.params().into_owned())
                            .await
                            .map_err(ExecutorError::other)?;
                        Ok(UpsertResult::Updated(incoming_state))
                    }
                    (Either::Left(incoming_state), None) => {
                        // update case
                        executor
                            .state_store
                            .update(&key, incoming_state.clone())
                            .await
                            .map_err(ExecutorError::other)?;
                        Ok(UpsertResult::Updated(incoming_state))
                    }
                    (update, contract) => unreachable!("{update:?}, {contract:?}"),
                }
            };
            let r = r.await;
            (executor, r)
        }
    }

    fn register_contract_notifier(
        &mut self,
        _key: ContractKey,
        _cli_id: ClientId,
        _notification_ch: UnboundedSender<HostResult>,
        _summary: Option<StateSummary<'_>>,
    ) -> Result<(), Box<RequestError>> {
        // For mock, we just acknowledge the registration without actually implementing notification
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
