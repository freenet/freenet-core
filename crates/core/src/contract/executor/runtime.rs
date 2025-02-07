use super::*;

impl ContractExecutor for Executor<Runtime> {
    async fn fetch_contract(
        &mut self,
        key: ContractKey,
        return_contract_code: bool,
    ) -> Result<(Option<WrappedState>, Option<ContractContainer>), ExecutorError> {
        match self.perform_contract_get(return_contract_code, key).await {
            Ok((state, code)) => Ok((state, code)),
            Err(err) => Err(err),
        }
    }

    async fn upsert_contract_state(
        &mut self,
        key: ContractKey,
        update: Either<WrappedState, StateDelta<'static>>,
        related_contracts: RelatedContracts<'static>,
        code: Option<ContractContainer>,
    ) -> Result<UpsertResult, ExecutorError> {
        let params = if let Some(code) = &code {
            code.params()
        } else {
            self.state_store
                .get_params(&key)
                .await
                .map_err(ExecutorError::other)?
                .ok_or_else(|| {
                    ExecutorError::request(StdContractError::Put {
                        key,
                        cause: "missing contract parameters".into(),
                    })
                })?
        };

        let remove_if_fail = if self
            .runtime
            .contract_store
            .fetch_contract(&key, &params)
            .is_none()
        {
            let code = code.ok_or_else(|| {
                ExecutorError::request(StdContractError::MissingContract { key: key.into() })
            })?;
            self.runtime
                .contract_store
                .store_contract(code.clone())
                .map_err(ExecutorError::other)?;
            true
        } else {
            false
        };

        let mut updates = match update {
            Either::Left(incoming_state) => {
                let result = self
                    .runtime
                    .validate_state(&key, &params, &incoming_state, &related_contracts)
                    .map_err(|err| {
                        if remove_if_fail {
                            let _ = self.runtime.contract_store.remove_contract(&key);
                        }
                        ExecutorError::execution(err, None)
                    })?;
                match result {
                    ValidateResult::Valid => {
                        self.state_store
                            .store(key, incoming_state.clone(), params.clone())
                            .await
                            .map_err(ExecutorError::other)?;
                    }
                    ValidateResult::Invalid => {
                        return Err(ExecutorError::request(StdContractError::invalid_put(key)));
                    }
                    ValidateResult::RequestRelated(mut related) => {
                        if let Some(key) = related.pop() {
                            return Err(ExecutorError::request(StdContractError::MissingRelated {
                                key,
                            }));
                        } else {
                            return Err(ExecutorError::internal_error());
                        }
                    }
                }

                vec![UpdateData::State(incoming_state.clone().into())]
            }
            Either::Right(delta) => {
                // todo: forward delta like we are doing with puts
                tracing::warn!("Delta updates are not yet supported");
                vec![UpdateData::Delta(delta)]
            }
        };

        let current_state = match self.state_store.get(&key).await {
            Ok(s) => s,
            Err(StateStoreError::MissingContract(_)) => {
                tracing::warn!("Missing contract {key} for upsert");
                return Err(ExecutorError::request(StdContractError::MissingContract {
                    key: key.into(),
                }));
            }
            Err(StateStoreError::Any(err)) => return Err(ExecutorError::other(err)),
        };

        for (id, state) in related_contracts
            .states()
            .filter_map(|(id, c)| c.as_ref().map(|c| (id, c)))
        {
            updates.push(UpdateData::RelatedState {
                related_to: *id,
                state: state.clone(),
            });
        }

        let updated_state = match self
            .attempt_state_update(&params, &current_state, &key, &updates)
            .await?
        {
            Either::Left(s) => s,
            Either::Right(mut r) => {
                let Some(c) = r.pop() else {
                    // this branch should be unreachable since attempt_state_update should only
                    return Err(ExecutorError::internal_error());
                };
                return Err(ExecutorError::request(StdContractError::MissingRelated {
                    key: c.contract_instance_id,
                }));
            }
        };
        match self
            .runtime
            .validate_state(&key, &params, &updated_state, &related_contracts)
            .map_err(|e| ExecutorError::execution(e, None))?
        {
            ValidateResult::Valid => {
                if updated_state.as_ref() == current_state.as_ref() {
                    Ok(UpsertResult::NoChange)
                } else {
                    Ok(UpsertResult::Updated(updated_state))
                }
            }
            ValidateResult::Invalid => Err(ExecutorError::request(
                freenet_stdlib::client_api::ContractError::Update {
                    key,
                    cause: "invalid outcome state".into(),
                },
            )),
            ValidateResult::RequestRelated(_) => todo!(),
        }
    }

    fn register_contract_notifier(
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
}

impl Executor<Runtime> {
    pub async fn from_config(
        config: Arc<Config>,
        event_loop_channel: Option<ExecutorToEventLoopChannel<ExecutorHalve>>,
    ) -> anyhow::Result<Self> {
        let (contract_store, delegate_store, secret_store, state_store) =
            Self::get_stores(&config).await?;
        let rt = Runtime::build(contract_store, delegate_store, secret_store, false).unwrap();
        Executor::new(
            state_store,
            move || {
                crate::util::set_cleanup_on_exit(config.paths().clone())?;
                Ok(())
            },
            OperationMode::Local,
            rt,
            event_loop_channel,
        )
        .await
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
                let _res = self.perform_contract_get(true, key).await?;
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

        // if new_state.as_ref() == current_state.as_ref() {
        //     tracing::debug!("No changes in state for contract {key}, avoiding update");
        //     return Ok(Either::Left(current_state.clone()));
        // }

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
