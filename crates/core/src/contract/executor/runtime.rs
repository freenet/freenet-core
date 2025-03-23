use super::*;
use super::{
    ContractExecutor, ContractRequest, ContractResponse, ExecutorError, RequestError, Response,
    StateStoreError,
};
use std::num::NonZeroUsize;
use std::sync::Arc;
use tokio::sync::Semaphore;

pub(crate) struct RuntimePool<C = Arc<Config>, R = Runtime> {
    // Keeping track of available executors
    pub runtimes: Vec<Option<Executor<R>>>,
    // Semaphore to control access to executors
    pub available: Semaphore,
    pub config: C,
    pub op_sender: mpsc::Sender<(
        Transaction,
        tokio::sync::oneshot::Sender<Result<OpEnum, CallbackError>>,
    )>,
    pub op_manager: Arc<OpManager>,
}

impl RuntimePool<Arc<Config>, Runtime> {
    /// Create a new pool with the given number of runtime executors
    pub async fn new(
        config: Arc<Config>,
        op_sender: mpsc::Sender<(
            Transaction,
            tokio::sync::oneshot::Sender<Result<OpEnum, CallbackError>>,
        )>,
        op_manager: Arc<OpManager>,
        pool_size: NonZeroUsize,
    ) -> anyhow::Result<Self> {
        let mut runtimes = Vec::with_capacity(pool_size.into());

        for _ in 0..pool_size.into() {
            let executor = Executor::from_config(
                config.clone(),
                Some(op_sender.clone()),
                Some(op_manager.clone()),
            )
            .await?;
            runtimes.push(Some(executor));
        }

        Ok(Self {
            runtimes,
            available: Semaphore::new(pool_size.into()),
            config,
            op_sender,
            op_manager,
        })
    }

    // Pop an executor from the pool - blocks until one is available
    async fn pop_executor(&mut self) -> Executor<Runtime> {
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

impl ContractExecutor for RuntimePool<Arc<Config>, Runtime> {
    type InnerExecutor = Executor<Runtime>;

    async fn fetch_contract(
        &mut self,
        key: ContractKey,
        return_contract_code: bool,
    ) -> impl Future<Output = (Self::InnerExecutor, FetchContractR)> + Send + 'static {
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
        update: Either<WrappedState, StateDelta<'static>>,
        related_contracts: RelatedContracts<'static>,
        code: Option<ContractContainer>,
    ) -> impl Future<Output = (Self::InnerExecutor, UpsertContractR)> + Send + 'static {
        let mut executor = self.pop_executor().await;

        async move {
            match update {
                Either::Left(state) => {
                    // For state updates with simplified implementation
                    let params = if let Some(code) = &code {
                        code.params()
                    } else {
                        match executor.state_store.get_params(&key).await {
                            Ok(Some(params)) => params,
                            Ok(None) => {
                                return (
                                    executor,
                                    Err(ExecutorError::request(StdContractError::Put {
                                        key,
                                        cause: "missing contract parameters".into(),
                                    })),
                                )
                            }
                            Err(err) => return (executor, Err(ExecutorError::other(err))),
                        }
                    };

                    let remove_if_fail = if executor
                        .runtime
                        .contract_store
                        .fetch_contract(&key, &params)
                        .is_none()
                    {
                        if let Some(code) = code {
                            if let Err(err) =
                                executor.runtime.contract_store.store_contract(code.clone())
                            {
                                return (executor, Err(ExecutorError::other(err)));
                            }
                            true
                        } else {
                            return (
                                executor,
                                Err(ExecutorError::request(StdContractError::MissingContract {
                                    key: key.into(),
                                })),
                            );
                        }
                    } else {
                        false
                    };

                    // Validate the state
                    match executor
                        .runtime
                        .validate_state(&key, &params, &state, &related_contracts)
                    {
                        Ok(validate_result) => {
                            match validate_result {
                                ValidateResult::Valid => {
                                    // Store and send notifications
                                    if let Err(err) = executor
                                        .state_store
                                        .store(key, state.clone(), params.clone())
                                        .await
                                    {
                                        return (executor, Err(ExecutorError::other(err)));
                                    }

                                    // Attempt to send notifications
                                    if let Err(err) = executor
                                        .send_update_notification(&key, &params, &state)
                                        .await
                                    {
                                        tracing::error!("Failed to send notifications: {}", err);
                                    }

                                    (executor, Ok(UpsertResult::Updated(state)))
                                }
                                ValidateResult::Invalid => (
                                    executor,
                                    Err(ExecutorError::request(StdContractError::invalid_put(key))),
                                ),
                                ValidateResult::RequestRelated(mut related) => {
                                    if let Some(key) = related.pop() {
                                        (
                                            executor,
                                            Err(ExecutorError::request(
                                                StdContractError::MissingRelated { key },
                                            )),
                                        )
                                    } else {
                                        (executor, Err(ExecutorError::internal_error()))
                                    }
                                }
                            }
                        }
                        Err(err) => {
                            if remove_if_fail {
                                let _ = executor.runtime.contract_store.remove_contract(&key);
                            }
                            (executor, Err(ExecutorError::execution(err, None)))
                        }
                    }
                }
                Either::Right(delta) => {
                    // For delta updates, use the full implementation
                    match executor.state_store.get(&key).await {
                        Ok(current_state) => match executor.state_store.get_params(&key).await {
                            Ok(Some(parameters)) => {
                                let updates = vec![UpdateData::Delta(delta)];
                                match executor
                                    .get_updated_state(&parameters, current_state, key, updates)
                                    .await
                                {
                                    Ok(new_state) => {
                                        (executor, Ok(UpsertResult::Updated(new_state)))
                                    }
                                    Err(err) => (executor, Err(err)),
                                }
                            }
                            Ok(None) => (
                                executor,
                                Err(ExecutorError::request(StdContractError::Update {
                                    key,
                                    cause: "missing contract parameters".into(),
                                })),
                            ),
                            Err(err) => (executor, Err(ExecutorError::other(err))),
                        },
                        Err(err) => (executor, Err(ExecutorError::other(err))),
                    }
                }
            }
        }
    }

    fn register_contract_notifier(
        &mut self,
        key: ContractKey,
        cli_id: ClientId,
        notification_ch: tokio::sync::mpsc::UnboundedSender<HostResult>,
        summary: Option<StateSummary<'_>>,
    ) -> Result<(), Box<RequestError>> {
        // We need to register with all executors
        let mut last_error = None;

        // Temporarily collect all executors
        let mut executors = Vec::new();
        for slot in &mut self.runtimes {
            if let Some(executor) = slot.take() {
                executors.push(executor);
            }
        }

        // Register with each executor
        // FIXME: potentially missing registers
        for executor in &mut executors {
            if let Err(err) = executor.register_contract_notifier(
                key,
                cli_id,
                notification_ch.clone(),
                summary.clone(),
            ) {
                last_error = Some(err);
            }
        }

        // Return executors to pool
        for executor in executors {
            if let Some(empty_slot) = self.runtimes.iter_mut().find(|slot| slot.is_none()) {
                *empty_slot = Some(executor);
                self.available.add_permits(1);
            }
        }

        last_error.map_or(Ok(()), Err)
    }

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
        Executor::from_config(
            self.config.clone(),
            Some(self.op_sender.clone()),
            Some(self.op_manager.clone()),
        )
        .await
        .expect("Failed to create new executor")
    }
}

impl Executor<Runtime> {
    pub async fn local(config: Arc<Config>) -> anyhow::Result<Self> {
        let (contract_store, delegate_store, secret_store, state_store) =
            Self::get_stores(&config).await?;
        let rt = Runtime::build(contract_store, delegate_store, secret_store, false).unwrap();
        assert!(config.mode == OperationMode::Local);
        Executor::new(state_store, OperationMode::Local, rt, None, None).await
    }

    async fn from_config(
        config: Arc<Config>,
        op_sender: Option<OpResult>,
        op_manager: Option<Arc<OpManager>>,
    ) -> anyhow::Result<Self> {
        let (contract_store, delegate_store, secret_store, state_store) =
            Self::get_stores(&config).await?;
        let rt = Runtime::build(contract_store, delegate_store, secret_store, false).unwrap();
        Executor::new(state_store, config.mode, rt, op_sender, op_manager).await
    }

    pub fn register_contract_notifier(
        &mut self,
        key: ContractKey,
        cli_id: ClientId,
        notification_ch: tokio::sync::mpsc::UnboundedSender<HostResult>,
        summary: Option<StateSummary<'_>>,
    ) -> Result<(), Box<RequestError>> {
        let channels = self.update_notifications.entry(key).or_default();
        if let Ok(i) = channels.binary_search_by_key(&&cli_id, |(p, _)| p) {
            let (_, existing_ch) = &channels[i];
            if !existing_ch.same_channel(&notification_ch) {
                return Err(RequestError::from(StdContractError::Subscribe {
                    key,
                    cause: format!("Peer {cli_id} already subscribed").into(),
                })
                .into());
            }
        } else {
            channels.push((cli_id, notification_ch));
        }

        if self
            .subscriber_summaries
            .entry(key)
            .or_default()
            .insert(cli_id, summary.map(StateSummary::into_owned))
            .is_some()
        {
            tracing::warn!(
                "contract {key} already was registered for peer {cli_id}; replaced summary"
            );
        }
        Ok(())
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
        updates: Option<mpsc::UnboundedSender<Result<HostResponse, WsClientError>>>,
    ) -> Response {
        match req {
            ClientRequest::ContractOp(op) => self.contract_requests(op, id, updates).await,
            ClientRequest::DelegateOp(op) => self.delegate_request(op, None),
            ClientRequest::Disconnect { cause } => {
                if let Some(cause) = cause {
                    tracing::info!("disconnecting cause: {cause}");
                }
                Err(RequestError::Disconnect.into())
            }
            _ => Err(ExecutorError::other(anyhow::anyhow!("not supported"))),
        }
    }

    /// Responde to requests made through any API's from client applications in local mode.
    pub async fn contract_requests(
        &mut self,
        req: ContractRequest<'_>,
        cli_id: ClientId,
        updates: Option<mpsc::UnboundedSender<Result<HostResponse, WsClientError>>>,
    ) -> Response {
        match req {
            ContractRequest::Put {
                contract,
                state,
                related_contracts,
            } => {
                self.perform_contract_put(contract, state, related_contracts)
                    .await
            }
            ContractRequest::Update { key, data } => self.perform_contract_update(key, data).await,
            // FIXME
            // Handle Get requests by returning the contract state and optionally the contract code
            ContractRequest::Get {
                key,
                return_contract_code,
            } => match self.perform_contract_get(return_contract_code, key).await {
                Ok((state, contract)) => Ok(ContractResponse::GetResponse {
                    key,
                    state: state.ok_or_else(|| {
                        ExecutorError::request(StdContractError::Get {
                            key,
                            cause: "contract state not found".into(),
                        })
                    })?,
                    contract,
                }
                .into()),
                Err(err) => Err(err),
            },
            ContractRequest::Subscribe { key, summary } => {
                tracing::debug!("subscribing to contract {key}");
                let updates = updates.ok_or_else(|| {
                    ExecutorError::other(anyhow::anyhow!("missing update channel"))
                })?;
                self.register_contract_notifier(key, cli_id, updates, summary)?;

                // by default a subscribe op has an implicit get
                let _res = self.perform_contract_get(false, key).await?;
                self.subscribe(key).await?;
                Ok(ContractResponse::SubscribeResponse {
                    key,
                    subscribed: true,
                }
                .into())
            }
            _ => Err(ExecutorError::other(anyhow::anyhow!("not supported"))),
        }
    }

    pub fn delegate_request(
        &mut self,
        req: DelegateRequest<'_>,
        attestaded_contract: Option<&ContractInstanceId>,
    ) -> Response {
        match req {
            DelegateRequest::RegisterDelegate {
                delegate,
                cipher,
                nonce,
            } => {
                use chacha20poly1305::{KeyInit, XChaCha20Poly1305};
                let key = delegate.key().clone();
                let arr = GenericArray::from_slice(&cipher);
                let cipher = XChaCha20Poly1305::new(arr);
                let nonce = GenericArray::from_slice(&nonce).to_owned();
                tracing::debug!("registering delegate `{key}");
                if let Some(contract) = attestaded_contract {
                    self.delegate_attested_ids
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
                        tracing::error!("failed registering delegate `{key}`: {err}");
                        Err(ExecutorError::other(StdDelegateError::RegisterError(key)))
                    }
                }
            }
            DelegateRequest::UnregisterDelegate(key) => {
                self.delegate_attested_ids.remove(&key);
                match self.runtime.unregister_delegate(&key) {
                    Ok(_) => Ok(HostResponse::Ok),
                    Err(err) => {
                        tracing::error!("failed unregistering delegate `{key}`: {err}");
                        Ok(HostResponse::Ok)
                    }
                }
            }
            DelegateRequest::GetSecretRequest {
                key,
                params,
                get_request,
            } => {
                let attested = attestaded_contract.and_then(|contract| {
                    self.delegate_attested_ids
                        .get(&key)
                        .and_then(|contracts| contracts.iter().find(|c| *c == contract))
                });
                match self.runtime.inbound_app_message(
                    &key,
                    &params,
                    attested.map(|c| c.as_bytes()),
                    vec![InboundDelegateMsg::GetSecretRequest(get_request)],
                ) {
                    Ok(values) => Ok(HostResponse::DelegateResponse { key, values }),
                    Err(err) => Err(ExecutorError::execution(
                        err,
                        Some(InnerOpError::Delegate(key.clone())),
                    )),
                }
            }
            DelegateRequest::ApplicationMessages {
                key,
                inbound,
                params,
            } => {
                let attested = attestaded_contract.and_then(|contract| {
                    self.delegate_attested_ids
                        .get(&key)
                        .and_then(|contracts| contracts.iter().find(|c| *c == contract))
                });
                match self.runtime.inbound_app_message(
                    &key,
                    &params,
                    attested.map(|c| c.as_bytes()),
                    inbound
                        .into_iter()
                        .map(InboundDelegateMsg::into_owned)
                        .collect(),
                ) {
                    Ok(values) => Ok(HostResponse::DelegateResponse { key, values }),
                    Err(err) => {
                        tracing::error!("failed executing delegate `{key}`: {err}");
                        Err(ExecutorError::execution(
                            err,
                            Some(InnerOpError::Delegate(key)),
                        ))
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
            // already existing contract, just try to merge states
            return self
                .perform_contract_update(key, UpdateData::State(state.into()))
                .await;
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
            .map_err(ExecutorError::other)?
            .clone();

        let updates = vec![update];
        let new_state = self
            .get_updated_state(&parameters, current_state, key, updates)
            .await?;

        // in the network impl this would be sent over the network
        let summary = self
            .runtime
            .summarize_state(&key, &parameters, &new_state)
            .map_err(|e| ExecutorError::execution(e, None))?;
        self.send_update_notification(&key, &parameters, &new_state)
            .await?;

        if self.mode == OperationMode::Local {
            return Ok(ContractResponse::UpdateResponse { key, summary }.into());
        }
        // notify peers with deltas from summary in network
        let request = UpdateContract { key, new_state };
        let _op: operations::update::UpdateResult = self.op_request(request).await?;

        Ok(ContractResponse::UpdateResponse { key, summary }.into())
    }

    /// Attempts to update the state with the provided updates.
    /// If there were no updates, it will return the current state.
    async fn attempt_state_update(
        &mut self,
        parameters: &Parameters<'_>,
        current_state: &WrappedState,
        key: &ContractKey,
        updates: &[UpdateData<'_>],
    ) -> Result<Either<WrappedState, Vec<RelatedContract>>, ExecutorError> {
        let update_modification =
            match self
                .runtime
                .update_state(key, parameters, current_state, updates)
            {
                Ok(result) => result,
                Err(err) => {
                    return Err(ExecutorError::execution(
                        err,
                        Some(InnerOpError::Upsert(*key)),
                    ))
                }
            };
        let UpdateModification {
            new_state, related, ..
        } = update_modification;
        let Some(new_state) = new_state else {
            if related.is_empty() {
                // no updates were made, just return old state
                return Ok(Either::Left(current_state.clone()));
            } else {
                return Ok(Either::Right(related));
            }
        };
        let new_state = WrappedState::new(new_state.into_bytes());

        if new_state.as_ref() == current_state.as_ref() {
            tracing::debug!("No changes in state for contract {key}, avoiding update");
            return Ok(Either::Left(current_state.clone()));
        }

        self.state_store
            .update(key, new_state.clone())
            .await
            .map_err(ExecutorError::other)?;

        if let Err(err) = self
            .send_update_notification(key, parameters, &new_state)
            .await
        {
            tracing::error!(
                "Failed while sending notifications for contract {}: {}",
                key,
                err
            );
        }
        Ok(Either::Left(new_state))
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
                        self.state_store
                            .update(&key, new_state.clone())
                            .await
                            .map_err(ExecutorError::other)?;
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
                    match self.state_store.get(&id.into()).await {
                        Ok(state) => {
                            // in this case we are already subscribed to and are updating this contract,
                            // we can try first with the existing value
                            updates.push(UpdateData::RelatedState {
                                related_to: id,
                                state: state.into(),
                            });
                        }

                        Err(StateStoreError::MissingContract(_)) => {
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
                                        contract,
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
                                    self.subscribe(id.into()).await?;
                                }
                            }
                        }
                        Err(other_err) => {
                            let _ = mode;
                            return Err(ExecutorError::other(other_err));
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
        Ok(new_state)
    }

    async fn perform_contract_get(
        &mut self,
        return_contract_code: bool,
        key: ContractKey,
    ) -> Result<(Option<WrappedState>, Option<ContractContainer>), ExecutorError> {
        let mut got_contract: Option<ContractContainer> = None;

        if return_contract_code {
            if let Some(contract) = self.get_contract_locally(&key).await? {
                got_contract = Some(contract);
            }
        }

        match self.state_store.get(&key).await {
            Ok(state) => Ok((Some(state), got_contract)),
            Err(StateStoreError::MissingContract(_)) => Ok((None, got_contract)),
            Err(err) => Err(ExecutorError::request(RequestError::from(
                StdContractError::Get {
                    key,
                    cause: format!("{err}").into(),
                },
            ))),
        }
    }

    async fn get_local_contract(
        &self,
        id: &ContractInstanceId,
    ) -> Result<State<'static>, Either<Box<RequestError>, anyhow::Error>> {
        let Ok(contract) = self.state_store.get(&(*id).into()).await else {
            return Err(Either::Right(
                StdContractError::MissingRelated { key: *id }.into(),
            ));
        };
        let state: &[u8] = unsafe {
            // Safety: this is fine since this will never scape this scope
            std::mem::transmute::<&[u8], &'_ [u8]>(contract.as_ref())
        };
        Ok(State::from(state))
    }

    async fn verify_and_store_contract(
        &mut self,
        state: WrappedState,
        trying_container: ContractContainer,
        mut related_contracts: RelatedContracts<'_>,
    ) -> Result<(), ExecutorError> {
        let key = trying_container.key();
        let params = trying_container.params();

        const DEPENDENCY_CYCLE_LIMIT_GUARD: usize = 100;
        let mut iterations = 0;

        let original_key = key;
        let original_state = state.clone();
        let original_params = params.clone();
        let mut trying_key = key;
        let mut trying_state = state;
        let mut trying_params = params;
        let mut trying_contract = Some(trying_container);

        while iterations < DEPENDENCY_CYCLE_LIMIT_GUARD {
            if let Some(contract) = trying_contract.take() {
                self.runtime
                    .contract_store
                    .store_contract(contract)
                    .map_err(ExecutorError::other)?;
            }

            let result = self
                .runtime
                .validate_state(
                    &trying_key,
                    &trying_params,
                    &trying_state,
                    &related_contracts,
                )
                .map_err(|err| {
                    let _ = self.runtime.contract_store.remove_contract(&trying_key);
                    ExecutorError::execution(err, None)
                })?;

            let is_valid = match result {
                ValidateResult::Valid => true,
                ValidateResult::Invalid => false,
                ValidateResult::RequestRelated(related) => {
                    iterations += 1;
                    related_contracts.missing(related);
                    for (id, related) in related_contracts.update() {
                        if related.is_none() {
                            match self.local_state_or_from_network(id, false).await? {
                                Either::Left(state) => {
                                    *related = Some(state.into());
                                }
                                Either::Right(result) => {
                                    let Some(contract) = result.contract else {
                                        return Err(ExecutorError::request(
                                            RequestError::ContractError(StdContractError::Get {
                                                key: (*id).into(),
                                                cause: "missing-contract".into(),
                                            }),
                                        ));
                                    };
                                    trying_key = (*id).into();
                                    trying_params = contract.params();
                                    trying_state = result.state;
                                    trying_contract = Some(contract);
                                    continue;
                                }
                            }
                        }
                    }
                    continue;
                }
            };

            if !is_valid {
                return Err(ExecutorError::request(StdContractError::Put {
                    key: trying_key,
                    cause: "not valid".into(),
                }));
            }

            self.state_store
                .store(trying_key, trying_state.clone(), trying_params.clone())
                .await
                .map_err(ExecutorError::other)?;
            if trying_key != original_key {
                trying_key = original_key;
                trying_params = original_params.clone();
                trying_state = original_state.clone();
                continue;
            }
            break;
        }
        if iterations == DEPENDENCY_CYCLE_LIMIT_GUARD {
            return Err(ExecutorError::request(StdContractError::MissingRelated {
                key: *original_key.id(),
            }));
        }
        Ok(())
    }

    async fn send_update_notification(
        &mut self,
        key: &ContractKey,
        params: &Parameters<'_>,
        new_state: &WrappedState,
    ) -> Result<(), ExecutorError> {
        tracing::debug!(contract = %key, "notify of contract update");
        let key = *key;
        if let Some(notifiers) = self.update_notifications.get_mut(&key) {
            let summaries = self.subscriber_summaries.get_mut(&key).unwrap();
            // in general there should be less than 32 failures
            let mut failures = Vec::with_capacity(32);
            for (peer_key, notifier) in notifiers.iter() {
                let peer_summary = summaries.get_mut(peer_key).unwrap();
                let update = match peer_summary {
                    Some(summary) => self
                        .runtime
                        .get_state_delta(&key, params, new_state, &*summary)
                        .map_err(|err| {
                            tracing::error!("{err}");
                            ExecutorError::execution(err, Some(InnerOpError::Upsert(key)))
                        })?
                        .to_owned()
                        .into(),
                    None => UpdateData::State(State::from(new_state.as_ref()).into_owned()),
                };
                if let Err(err) =
                    notifier.send(Ok(
                        ContractResponse::UpdateNotification { key, update }.into()
                    ))
                {
                    failures.push(*peer_key);
                    tracing::error!(cli_id = %peer_key, "{err}");
                } else {
                    tracing::debug!(cli_id = %peer_key, contract = %key, "notified of update");
                }
            }
            if !failures.is_empty() {
                notifiers.retain(|(c, _)| !failures.contains(c));
            }
        }
        Ok(())
    }

    async fn get_contract_locally(
        &self,
        key: &ContractKey,
    ) -> Result<Option<ContractContainer>, ExecutorError> {
        let Some(parameters) = self
            .state_store
            .get_params(key)
            .await
            .map_err(ExecutorError::other)?
        else {
            return Ok(None);
        };
        let Some(contract) = self.runtime.contract_store.fetch_contract(key, &parameters) else {
            return Ok(None);
        };
        Ok(Some(contract))
    }
}

impl Executor<Runtime> {
    async fn subscribe(&mut self, key: ContractKey) -> Result<(), ExecutorError> {
        if self.mode == OperationMode::Local {
            return Ok(());
        }
        let request = SubscribeContract { key };
        let _sub: operations::subscribe::SubscribeResult = self.op_request(request).await?;
        Ok(())
    }

    #[inline]
    async fn local_state_or_from_network(
        &mut self,
        id: &ContractInstanceId,
        return_contract_code: bool,
    ) -> Result<Either<WrappedState, operations::get::GetResult>, ExecutorError> {
        if let Ok(contract) = self.state_store.get(&(*id).into()).await {
            return Ok(Either::Left(contract));
        };
        let request: GetContract = GetContract {
            key: (*id).into(),
            return_contract_code,
        };
        let get_result: operations::get::GetResult = self.op_request(request).await?;
        Ok(Either::Right(get_result))
    }
}
