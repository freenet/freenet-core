use super::*;
use super::{
    ContractExecutor, ContractRequest, ContractResponse, ExecutorError, ExecutorHalve,
    ExecutorToEventLoopChannel, RequestError, Response, StateStoreError,
};

impl ContractExecutor for Executor<Runtime> {
    #[tracing::instrument(
        level = "debug",
        name = "fetch_contract",
        skip(self),
        fields(contract = %key, return_code = return_contract_code)
    )]
    async fn fetch_contract(
        &mut self,
        key: ContractKey,
        return_contract_code: bool,
    ) -> Result<(Option<WrappedState>, Option<ContractContainer>), ExecutorError> {
        tracing::trace!("Starting contract fetch operation");
        let result = self.perform_contract_get(return_contract_code, key).await;
        
        match &result {
            Ok((Some(state), code)) => {
                let hash = blake3::hash(state.as_ref());
                tracing::debug!(
                    state_size = state.as_ref().len(),
                    state_hash = %hash,
                    has_code = code.is_some(),
                    "Successfully fetched contract state"
                );
            }
            Ok((None, _)) => {
                tracing::debug!("Contract state not found");
            }
            Err(err) => {
                tracing::error!(error = %err, "Failed to fetch contract");
            }
        }
        
        result
    }

    #[tracing::instrument(
        level = "debug",
        name = "upsert_contract_state",
        skip(self, update, related_contracts, code),
        fields(contract = %key, update_type = ?update.is_left())
    )]
    async fn upsert_contract_state(
        &mut self,
        key: ContractKey,
        update: Either<WrappedState, StateDelta<'static>>,
        related_contracts: RelatedContracts<'static>,
        code: Option<ContractContainer>,
    ) -> Result<UpsertResult, ExecutorError> {
        if let Either::Left(ref state) = update {
            let hash = blake3::hash(state.as_ref());
            tracing::debug!(
                state_size = state.as_ref().len(),
                state_hash = %hash,
                "Processing full state update"
            );
        } else {
            tracing::debug!("Processing delta update");
        }
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

    #[tracing::instrument(
        level = "debug",
        name = "register_contract_notifier",
        skip(self, notification_ch, summary),
        fields(contract = %key, client_id = %cli_id, has_summary = summary.is_some())
    )]
    fn register_contract_notifier(
        &mut self,
        key: ContractKey,
        cli_id: ClientId,
        notification_ch: tokio::sync::mpsc::UnboundedSender<HostResult>,
        summary: Option<StateSummary<'_>>,
    ) -> Result<(), Box<RequestError>> {
        tracing::trace!("Registering contract notification channel");
        
        let channels = self.update_notifications.entry(key).or_default();
        if let Ok(i) = channels.binary_search_by_key(&&cli_id, |(p, _)| p) {
            let (_, existing_ch) = &channels[i];
            if !existing_ch.same_channel(&notification_ch) {
                tracing::warn!("Client already has a different notification channel registered");
                return Err(RequestError::from(StdContractError::Subscribe {
                    key,
                    cause: format!("Peer {cli_id} already subscribed").into(),
                })
                .into());
            }
            tracing::debug!("Client already registered with same channel");
        } else {
            tracing::debug!("Adding new notification channel for client");
            channels.push((cli_id, notification_ch));
        }

        let replaced = self
            .subscriber_summaries
            .entry(key)
            .or_default()
            .insert(cli_id, summary.map(StateSummary::into_owned))
            .is_some();
            
        if replaced {
            tracing::warn!("Replaced existing summary for client");
        } else {
            tracing::debug!("Added new summary for client");
        }
        
        tracing::debug!(
            subscribers = channels.len(),
            "Contract notification registration complete"
        );
        Ok(())
    }
}

impl Executor<Runtime> {
    // Private implementation methods
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
                let _ =
                    crate::util::set_cleanup_on_exit(config.paths().clone()).inspect_err(|error| {
                        tracing::error!("Failed to set cleanup on exit: {error}");
                    });
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

    #[tracing::instrument(
        level = "debug",
        name = "preload_contract",
        skip(self, contract, state, related_contracts),
        fields(client_id = %cli_id, contract_key = %contract.key())
    )]
    pub async fn preload(
        &mut self,
        cli_id: ClientId,
        contract: ContractContainer,
        state: WrappedState,
        related_contracts: RelatedContracts<'static>,
    ) {
        tracing::debug!(
            state_size = state.as_ref().len(),
            state_hash = %blake3::hash(state.as_ref()),
            "Preloading contract"
        );
        
        let result = self
            .contract_requests(
                ContractRequest::Put {
                    contract,
                    state,
                    related_contracts,
                    subscribe: false,
                },
                cli_id,
                None,
            )
            .await;
            
        if let Err(err) = result {
            match err.inner {
                Either::Left(err) => tracing::error!(error = %err, "Request error during preload"),
                Either::Right(err) => tracing::error!(error = %err, "Other error during preload"),
            }
        } else {
            tracing::info!("Contract successfully preloaded");
        }
    }

    #[tracing::instrument(
        level = "debug",
        name = "handle_client_request",
        skip(self, req, updates),
        fields(client_id = %id)
    )]
    pub async fn handle_request(
        &mut self,
        id: ClientId,
        req: ClientRequest<'_>,
        updates: Option<mpsc::UnboundedSender<Result<HostResponse, WsClientError>>>,
    ) -> Response {
        match &req {
            ClientRequest::ContractOp(op) => {
                tracing::debug!("Processing contract operation");
                self.contract_requests(op.clone(), id, updates).await
            },
            ClientRequest::DelegateOp(op) => {
                tracing::debug!("Processing delegate operation");
                self.delegate_request(op.clone(), None)
            },
            ClientRequest::Disconnect { cause } => {
                if let Some(cause) = cause {
                    tracing::info!(cause = %cause, "Client disconnecting with cause");
                } else {
                    tracing::info!("Client disconnecting");
                }
                Err(RequestError::Disconnect.into())
            }
            other => {
                tracing::warn!(request_type = ?std::any::type_name_of_val(&other), "Unsupported client request");
                Err(ExecutorError::other(anyhow::anyhow!("Request type not supported")))
            }
        }
    }

    /// Respond to requests made through any API's from client applications in local mode.
    #[tracing::instrument(
        level = "debug",
        name = "process_contract_request",
        skip(self, req, updates),
        fields(client_id = %cli_id)
    )]
    pub async fn contract_requests(
        &mut self,
        req: ContractRequest<'_>,
        cli_id: ClientId,
        updates: Option<mpsc::UnboundedSender<Result<HostResponse, WsClientError>>>,
    ) -> Response {
        let result = match req {
            ContractRequest::Put {
                contract,
                state,
                related_contracts,
                ..
            } => {
                let key = contract.key();
                tracing::info!(
                    contract = %key,
                    state_size = state.as_ref().len(),
                    state_hash = %blake3::hash(state.as_ref()),
                    "Processing contract put request"
                );
                self.perform_contract_put(contract, state, related_contracts)
                    .await
            }
            ContractRequest::Update { key, data } => {
                tracing::info!(
                    contract = %key,
                    update_type = ?std::any::type_name_of_val(&data),
                    "Processing contract update request"
                );
                self.perform_contract_update(key, data).await
            }
            ContractRequest::Get {
                key,
                return_contract_code,
                ..
            } => {
                tracing::info!(
                    contract = %key,
                    return_code = return_contract_code,
                    "Processing contract get request"
                );
                
                match self.perform_contract_get(return_contract_code, key).await {
                    Ok((state, contract)) => {
                        if let Some(ref state) = state {
                            tracing::debug!(
                                state_size = state.as_ref().len(),
                                state_hash = %blake3::hash(state.as_ref()),
                                has_contract = contract.is_some(),
                                "Contract state retrieved successfully"
                            );
                        } else {
                            tracing::debug!("Contract state not found");
                            return Err(ExecutorError::request(StdContractError::Get {
                                key,
                                cause: "contract state not found".into(),
                            }));
                        }
                        
                        Ok(ContractResponse::GetResponse {
                            key,
                            state: state.unwrap(),
                            contract,
                        }
                        .into())
                    }
                    Err(err) => Err(err),
                }
            }
            ContractRequest::Subscribe { key, summary } => {
                tracing::info!(
                    contract = %key,
                    has_summary = summary.is_some(),
                    "Processing contract subscribe request"
                );
                
                let updates = match updates {
                    Some(ch) => ch,
                    None => {
                        tracing::error!("Missing update channel for subscription");
                        return Err(ExecutorError::other(anyhow::anyhow!("Missing update channel")));
                    }
                };
                
                if let Err(e) = self.register_contract_notifier(key, cli_id, updates, summary) {
                    tracing::error!(error = %e, "Failed to register contract notifier");
                    return Err(e.into());
                }

                // by default a subscribe op has an implicit get
                tracing::debug!("Performing implicit get for subscription");
                if let Err(e) = self.perform_contract_get(false, key).await {
                    tracing::error!(error = %e, "Failed implicit get during subscription");
                    return Err(e);
                }
                
                if let Err(e) = self.subscribe(key).await {
                    tracing::error!(error = %e, "Failed to subscribe to contract");
                    return Err(e);
                }
                
                tracing::info!("Successfully subscribed to contract");
                Ok(ContractResponse::SubscribeResponse {
                    key,
                    subscribed: true,
                }
                .into())
            }
            other => {
                tracing::warn!(request_type = ?std::any::type_name_of_val(&other), "Unsupported contract request");
                Err(ExecutorError::other(anyhow::anyhow!("Request type not supported")))
            }
        };

        match &result {
            Ok(_) => tracing::debug!("Contract request completed successfully"),
            Err(e) => tracing::error!(error = %e, "Contract request failed"),
        }

        result
    }

    #[tracing::instrument(
        level = "debug",
        name = "process_delegate_request",
        skip(self, req),
        fields(has_attested_contract = attestaded_contract.is_some())
    )]
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
                
                tracing::info!(
                    delegate_key = %key,
                    "Registering delegate"
                );
                
                let arr = GenericArray::from_slice(&cipher);
                let cipher = XChaCha20Poly1305::new(arr);
                let nonce = GenericArray::from_slice(&nonce).to_owned();
                
                if let Some(contract) = attestaded_contract {
                    tracing::debug!(
                        delegate_key = %key,
                        contract = %contract,
                        "Registering attested contract for delegate"
                    );
                    self.delegate_attested_ids
                        .entry(key.clone())
                        .or_default()
                        .push(*contract);
                }
                
                match self.runtime.register_delegate(delegate, cipher, nonce) {
                    Ok(_) => {
                        tracing::info!(delegate_key = %key, "Delegate registered successfully");
                        Ok(DelegateResponse {
                            key,
                            values: Vec::new(),
                        })
                    },
                    Err(err) => {
                        tracing::error!(
                            delegate_key = %key,
                            error = %err,
                            "Failed to register delegate"
                        );
                        Err(ExecutorError::other(StdDelegateError::RegisterError(key)))
                    }
                }
            }
            DelegateRequest::UnregisterDelegate(key) => {
                tracing::info!(delegate_key = %key, "Unregistering delegate");
                
                self.delegate_attested_ids.remove(&key);
                match self.runtime.unregister_delegate(&key) {
                    Ok(_) => {
                        tracing::info!(delegate_key = %key, "Delegate unregistered successfully");
                        Ok(HostResponse::Ok)
                    },
                    Err(err) => {
                        tracing::warn!(
                            delegate_key = %key,
                            error = %err,
                            "Error during delegate unregistration, continuing anyway"
                        );
                        Ok(HostResponse::Ok)
                    }
                }
            }
            DelegateRequest::GetSecretRequest {
                key,
                params,
                get_request,
            } => {
                tracing::info!(
                    delegate_key = %key,
                    "Processing get secret request"
                );
                
                let attested = attestaded_contract.and_then(|contract| {
                    self.delegate_attested_ids
                        .get(&key)
                        .and_then(|contracts| contracts.iter().find(|c| *c == contract))
                });
                
                if let Some(contract) = attested {
                    tracing::debug!(
                        delegate_key = %key,
                        contract = %contract,
                        "Request has attested contract"
                    );
                }
                
                match self.runtime.inbound_app_message(
                    &key,
                    &params,
                    attested.map(|c| c.as_bytes()),
                    vec![InboundDelegateMsg::GetSecretRequest(get_request)],
                ) {
                    Ok(values) => {
                        tracing::debug!(
                            delegate_key = %key,
                            values_count = values.len(),
                            "Secret request processed successfully"
                        );
                        Ok(HostResponse::DelegateResponse { key, values })
                    },
                    Err(err) => {
                        tracing::error!(
                            delegate_key = %key,
                            error = %err,
                            "Failed to process secret request"
                        );
                        Err(ExecutorError::execution(
                            err,
                            Some(InnerOpError::Delegate(key.clone())),
                        ))
                    }
                }
            }
            DelegateRequest::ApplicationMessages {
                key,
                inbound,
                params,
            } => {
                tracing::info!(
                    delegate_key = %key,
                    messages_count = inbound.len(),
                    "Processing application messages"
                );
                
                let attested = attestaded_contract.and_then(|contract| {
                    self.delegate_attested_ids
                        .get(&key)
                        .and_then(|contracts| contracts.iter().find(|c| *c == contract))
                });
                
                if let Some(contract) = attested {
                    tracing::debug!(
                        delegate_key = %key,
                        contract = %contract,
                        "Request has attested contract"
                    );
                }
                
                match self.runtime.inbound_app_message(
                    &key,
                    &params,
                    attested.map(|c| c.as_bytes()),
                    inbound
                        .into_iter()
                        .map(InboundDelegateMsg::into_owned)
                        .collect(),
                ) {
                    Ok(values) => {
                        tracing::debug!(
                            delegate_key = %key,
                            values_count = values.len(),
                            "Application messages processed successfully"
                        );
                        Ok(HostResponse::DelegateResponse { key, values })
                    },
                    Err(err) => {
                        tracing::error!(
                            delegate_key = %key,
                            error = %err,
                            "Failed to process application messages"
                        );
                        Err(ExecutorError::execution(
                            err,
                            Some(InnerOpError::Delegate(key)),
                        ))
                    }
                }
            }
            _ => {
                tracing::warn!("Unsupported delegate request type");
                Err(ExecutorError::other(anyhow::anyhow!("Request type not supported")))
            }
        }
    }

    #[tracing::instrument(
        level = "debug",
        name = "perform_contract_put",
        skip(self, contract, state, related_contracts),
        fields(contract_key = %contract.key(), state_size = state.as_ref().len())
    )]
    async fn perform_contract_put(
        &mut self,
        contract: ContractContainer,
        state: WrappedState,
        related_contracts: RelatedContracts<'_>,
    ) -> Response {
        let key = contract.key();
        let params = contract.params();
        let state_hash = blake3::hash(state.as_ref());
        
        tracing::debug!(
            state_hash = %state_hash,
            params_size = params.as_ref().len(),
            "Starting contract put operation"
        );

        // Check if contract already exists
        let contract_exists = self.get_local_contract(key.id()).await.is_ok();
        if contract_exists {
            tracing::info!(
                "Contract already exists, performing update instead of put"
            );
            return self
                .perform_contract_update(key, UpdateData::State(state.into()))
                .await;
        }

        tracing::debug!("Verifying and storing new contract");
        if let Err(e) = self.verify_and_store_contract(state.clone(), contract, related_contracts).await {
            tracing::error!(
                error = %e,
                "Failed to verify and store contract"
            );
            return Err(e);
        }

        tracing::debug!("Sending update notifications");
        if let Err(e) = self.send_update_notification(&key, &params, &state).await {
            tracing::error!(
                error = %e,
                "Failed to send update notifications"
            );
            return Err(ExecutorError::request(StdContractError::Put {
                key,
                cause: "failed while sending notifications".into(),
            }));
        }
        
        tracing::info!("Contract put operation completed successfully");
        Ok(ContractResponse::PutResponse { key }.into())
    }

    #[tracing::instrument(
        level = "debug",
        name = "perform_contract_update",
        skip(self, update),
        fields(contract_key = %key, update_type = ?update.is_left())
    )]
    async fn perform_contract_update(
        &mut self,
        key: ContractKey,
        update: UpdateData<'_>,
    ) -> Response {
        tracing::debug!("Retrieving contract parameters");
        let parameters = match self.state_store.get_params(&key).await {
            Ok(Some(params)) => params,
            Ok(None) => {
                tracing::error!("Missing contract parameters");
                return Err(ExecutorError::request(StdContractError::Update {
                    cause: "missing contract parameters".into(),
                    key,
                }));
            }
            Err(e) => {
                tracing::error!(error = %e, "Failed to retrieve contract parameters");
                return Err(ExecutorError::other(e));
            }
        };

        tracing::debug!("Retrieving current contract state");
        let current_state = match self.state_store.get(&key).await {
            Ok(state) => state.clone(),
            Err(e) => {
                tracing::error!(error = %e, "Failed to retrieve current contract state");
                return Err(ExecutorError::other(e));
            }
        };

        let updates = vec![update];
        tracing::debug!("Calculating updated state");
        let new_state = match self.get_updated_state(&parameters, current_state, key, updates).await {
            Ok(state) => state,
            Err(e) => {
                tracing::error!(error = %e, "Failed to calculate updated state");
                return Err(e);
            }
        };

        // Generate state summary
        tracing::debug!("Generating state summary");
        let summary = match self.runtime.summarize_state(&key, &parameters, &new_state) {
            Ok(summary) => summary,
            Err(e) => {
                tracing::error!(error = %e, "Failed to generate state summary");
                return Err(ExecutorError::execution(e, None));
            }
        };
        
        // Send update notifications
        tracing::debug!("Sending update notifications");
        if let Err(e) = self.send_update_notification(&key, &parameters, &new_state).await {
            tracing::error!(error = %e, "Failed to send update notifications");
            return Err(e);
        }

        // Handle local vs network mode
        if self.mode == OperationMode::Local {
            tracing::info!("Update completed in local mode");
            return Ok(ContractResponse::UpdateResponse { key, summary }.into());
        }
        
        // In network mode, notify peers with deltas
        tracing::debug!("Sending update to network peers");
        let request = UpdateContract { key, new_state };
        match self.op_request(request).await {
            Ok(_) => {
                tracing::info!("Network update completed successfully");
                Ok(ContractResponse::UpdateResponse { key, summary }.into())
            },
            Err(e) => {
                tracing::error!(error = %e, "Failed to send update to network");
                Err(e)
            }
        }
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

    #[tracing::instrument(
        level = "debug",
        name = "perform_contract_get",
        skip(self),
        fields(contract_key = %key, return_code = return_contract_code)
    )]
    async fn perform_contract_get(
        &mut self,
        return_contract_code: bool,
        key: ContractKey,
    ) -> Result<(Option<WrappedState>, Option<ContractContainer>), ExecutorError> {
        let mut got_contract: Option<ContractContainer> = None;

        // Fetch contract code if requested
        if return_contract_code {
            tracing::debug!("Attempting to fetch contract code");
            match self.get_contract_locally(&key).await {
                Ok(Some(contract)) => {
                    tracing::debug!("Contract code retrieved successfully");
                    got_contract = Some(contract);
                }
                Ok(None) => {
                    tracing::debug!("Contract code not found locally");
                }
                Err(e) => {
                    tracing::error!(error = %e, "Error retrieving contract code");
                    return Err(e);
                }
            }
        }

        // Fetch contract state
        tracing::debug!("Retrieving contract state");
        let state_result = self.state_store.get(&key).await;
        
        match state_result {
            Ok(state) => {
                let state_size = state.as_ref().len();
                let state_hash = blake3::hash(state.as_ref());
                
                tracing::info!(
                    state_size = state_size,
                    state_hash = %state_hash,
                    has_contract_code = got_contract.is_some(),
                    "Contract state retrieved successfully"
                );
                
                Ok((Some(state), got_contract))
            }
            Err(StateStoreError::MissingContract(_)) => {
                tracing::warn!("Contract state not found in store");
                Ok((None, got_contract))
            }
            Err(err) => {
                tracing::error!(error = %err, "Failed to get contract state");
                Err(ExecutorError::request(RequestError::from(
                    StdContractError::Get {
                        key,
                        cause: format!("{err}").into(),
                    },
                )))
            }
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
        let state_hash = blake3::hash(state.as_ref());

        tracing::debug!(
            contract = %key,
            state_size = state.as_ref().len(),
            state_hash = %state_hash,
            params_size = params.as_ref().len(),
            "starting contract verification and storage"
        );

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
                tracing::debug!(
                    contract = %trying_key,
                    "storing contract in runtime store"
                );
                self.runtime
                    .contract_store
                    .store_contract(contract)
                    .map_err(|e| {
                        tracing::error!(
                            contract = %trying_key,
                            error = %e,
                            "failed to store contract in runtime"
                        );
                        ExecutorError::other(e)
                    })?;
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

            tracing::debug!(
                contract = %trying_key,
                state_size = trying_state.as_ref().len(),
                "storing contract state"
            );
            self.state_store
                .store(trying_key, trying_state.clone(), trying_params.clone())
                .await
                .map_err(|e| {
                    tracing::error!(
                        contract = %trying_key,
                        error = %e,
                        "failed to store contract state"
                    );
                    ExecutorError::other(e)
                })?;
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

    #[tracing::instrument(
        level = "debug",
        name = "send_update_notifications",
        skip(self, params, new_state),
        fields(contract_key = %key)
    )]
    async fn send_update_notification(
        &mut self,
        key: &ContractKey,
        params: &Parameters<'_>,
        new_state: &WrappedState,
    ) -> Result<(), ExecutorError> {
        let key = *key;
        
        // Check if there are any subscribers for this contract
        if let Some(notifiers) = self.update_notifications.get_mut(&key) {
            let subscriber_count = notifiers.len();
            if subscriber_count == 0 {
                tracing::debug!("No subscribers for this contract");
                return Ok(());
            }
            
            tracing::info!(subscribers = subscriber_count, "Sending update notifications");
            
            let summaries = match self.subscriber_summaries.get_mut(&key) {
                Some(s) => s,
                None => {
                    tracing::error!("Subscriber summaries missing for contract");
                    return Err(ExecutorError::other(anyhow::anyhow!(
                        "Subscriber summaries missing for contract"
                    )));
                }
            };
            
            // Track notification failures
            let mut failures = Vec::with_capacity(32);
            
            // Send notifications to each subscriber
            for (peer_key, notifier) in notifiers.iter() {
                tracing::debug!(client_id = %peer_key, "Preparing notification for client");
                
                let peer_summary = match summaries.get_mut(peer_key) {
                    Some(s) => s,
                    None => {
                        tracing::error!(client_id = %peer_key, "Missing summary for client");
                        failures.push(*peer_key);
                        continue;
                    }
                };
                
                // Generate update data based on client's summary
                let update = match peer_summary {
                    Some(summary) => {
                        tracing::debug!(client_id = %peer_key, "Generating delta update");
                        match self.runtime.get_state_delta(&key, params, new_state, &*summary) {
                            Ok(delta) => delta.to_owned().into(),
                            Err(err) => {
                                tracing::error!(
                                    client_id = %peer_key,
                                    error = %err,
                                    "Failed to generate delta update"
                                );
                                failures.push(*peer_key);
                                continue;
                            }
                        }
                    },
                    None => {
                        tracing::debug!(client_id = %peer_key, "Sending full state update");
                        UpdateData::State(State::from(new_state.as_ref()).into_owned())
                    },
                };
                
                // Send the notification
                let notification = ContractResponse::UpdateNotification { key, update };
                match notifier.send(Ok(notification.into())) {
                    Ok(_) => {
                        tracing::debug!(client_id = %peer_key, "Notification sent successfully");
                    },
                    Err(err) => {
                        tracing::error!(
                            client_id = %peer_key,
                            error = %err,
                            "Failed to send notification"
                        );
                        failures.push(*peer_key);
                    }
                }
            }
            
            // Clean up failed notifiers
            if !failures.is_empty() {
                tracing::warn!(
                    failure_count = failures.len(),
                    "Removing failed notification channels"
                );
                notifiers.retain(|(c, _)| !failures.contains(c));
            }
        } else {
            tracing::debug!("No notification channels registered for this contract");
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
