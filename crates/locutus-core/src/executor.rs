//! Contract executor.

use std::collections::HashMap;

use blake2::digest::generic_array::GenericArray;
use locutus_runtime::prelude::*;
use locutus_stdlib::client_api::{
    ClientError, ClientRequest, ContractError as CoreContractError, ContractRequest,
    ContractResponse, DelegateError as CoreDelegateError, DelegateRequest, HostResponse,
    RequestError,
};
use tokio::sync::mpsc::UnboundedSender;

use crate::{either::Either, ClientId, DynError, HostResult, Storage};

type Response = Result<HostResponse, Either<RequestError, DynError>>;

#[derive(clap::ValueEnum, Clone, Copy, Debug, PartialEq, Eq)]
pub enum OperationMode {
    /// Run the node in local-only mode. Useful for development purposes.
    Local,
    /// Standard operation mode.
    Network,
}

/// A WASM executor which will run any contracts, delegates, etc. registered.
///
/// This executor will monitor the store directories and databases to detect state changes.
/// Consumers of the executor are required to poll for new changes in order to be notified
/// of changes or can alternatively use the notification channel.
pub struct Executor {
    mode: OperationMode,
    runtime: Runtime,
    contract_state: StateStore<Storage>,
    update_notifications: HashMap<ContractKey, Vec<(ClientId, UnboundedSender<HostResult>)>>,
    subscriber_summaries: HashMap<ContractKey, HashMap<ClientId, Option<StateSummary<'static>>>>,
}

impl Executor {
    pub async fn new(
        contract_store: ContractStore,
        delegate_store: DelegateStore,
        secret_store: SecretsStore,
        contract_state: StateStore<Storage>,
        ctrl_handler: impl FnOnce(),
        mode: OperationMode,
    ) -> Result<Self, DynError> {
        ctrl_handler();

        Ok(Self {
            mode,
            runtime: Runtime::build(contract_store, delegate_store, secret_store, false).unwrap(),
            contract_state,
            update_notifications: HashMap::default(),
            subscriber_summaries: HashMap::default(),
        })
    }

    pub fn register_contract_notifier(
        &mut self,
        key: ContractKey,
        cli_id: ClientId,
        notification_ch: tokio::sync::mpsc::UnboundedSender<HostResult>,
        summary: Option<StateSummary<'_>>,
    ) -> Result<(), RequestError> {
        let channels = self.update_notifications.entry(key.clone()).or_default();
        if let Ok(i) = channels.binary_search_by_key(&&cli_id, |(p, _)| p) {
            let (_, existing_ch) = &channels[i];
            if !existing_ch.same_channel(&notification_ch) {
                return Err(RequestError::from(CoreContractError::Subscribe {
                    key,
                    cause: format!("Peer {cli_id} already subscribed"),
                }));
            }
        } else {
            channels.push((cli_id, notification_ch));
        }

        if self
            .subscriber_summaries
            .entry(key.clone())
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
            .handle_request(
                cli_id,
                ContractRequest::Put {
                    contract,
                    state,
                    related_contracts,
                }
                .into(),
                None,
            )
            .await
        {
            match err {
                Either::Left(err) => tracing::error!("req error: {err}"),
                Either::Right(err) => tracing::error!("other error: {err}"),
            }
        }
    }

    pub async fn handle_request(
        &mut self,
        id: ClientId,
        req: ClientRequest<'static>,
        updates: Option<UnboundedSender<Result<HostResponse, ClientError>>>,
    ) -> Response {
        match req {
            ClientRequest::ContractOp(op) => self.contract_op(op, id, updates).await,
            ClientRequest::DelegateOp(op) => self.delegate_op(op),
            ClientRequest::Disconnect { cause } => {
                if let Some(cause) = cause {
                    tracing::info!("disconnecting cause: {cause}");
                }
                Err(Either::Left(RequestError::Disconnect))
            }
            ClientRequest::GenerateRandData { bytes } => {
                let mut output = vec![0; bytes];
                locutus_runtime::util::generate_random_bytes(&mut output);
                Ok(HostResponse::GenerateRandData(output))
            }
        }
    }

    async fn contract_op(
        &mut self,
        req: ContractRequest<'_>,
        id: ClientId,
        updates: Option<UnboundedSender<Result<HostResponse, ClientError>>>,
    ) -> Response {
        match req {
            ContractRequest::Put {
                contract,
                state,
                mut related_contracts,
            } => {
                // FIXME: in net node, we don't allow puts for existing contract states
                //        if it hits a node which already has it it will get rejected
                //        while we wait for confirmation for the state,
                //        we don't respond with the interim state
                //
                //        if there is a conflict, resolve the conflict to see which
                //        is the outdated state:
                //          1. through the arbitraur mechanism
                //          2. a new func which compared two summaries and gives the most fresh
                //        you can request to several nodes and determine which node has a fresher ver
                let key = contract.key();
                let params = contract.params();
                self.runtime
                    .contract_store
                    .store_contract(contract.clone())
                    .map_err(Into::into)
                    .map_err(Either::Right)?;

                tracing::debug!("executing with params: {:?}", params);

                if self.mode == OperationMode::Local {
                    for (id, related) in related_contracts.update() {
                        let Ok(contract) = self.contract_state.get(&(*id).into()).await else {
                            return Err(Either::Right(CoreContractError::MissingRelated { key: *id}.into()));
                        };
                        let state: &[u8] = unsafe {
                            // Safety: this is fine since this will never scape this scope
                            std::mem::transmute::<&[u8], &'_ [u8]>(contract.as_ref())
                        };
                        *related = Some(State::from(state));
                    }
                }

                let result = self
                    .runtime
                    .validate_state(&key, &params, &state, related_contracts)
                    .map_err(Into::into)
                    .map_err(Either::Right)?;
                let is_valid = match result {
                    ValidateResult::Valid => true,
                    ValidateResult::Invalid => false,
                    ValidateResult::RequestRelated(_related) => {
                        if self.mode == OperationMode::Network {
                            // FIXME: should deal with additional related contracts requested
                            todo!()
                        } else {
                            unreachable!()
                        }
                    }
                };
                let res = is_valid
                    .then(|| ContractResponse::PutResponse { key: key.clone() }.into())
                    .ok_or_else(|| {
                        Either::Left(
                            CoreContractError::Put {
                                key: key.clone(),
                                cause: "not valid".to_owned(),
                            }
                            .into(),
                        )
                    })?;

                self.contract_state
                    .store(key.clone(), state.clone(), Some(params.clone()))
                    .await
                    .map_err(Into::into)
                    .map_err(Either::Right)?;
                self.send_update_notification(&key, &params, &state)
                    .await
                    .map_err(|_| {
                        Either::Left(
                            CoreContractError::Put {
                                key: key.clone(),
                                cause: "failed while sending notifications".to_owned(),
                            }
                            .into(),
                        )
                    })?;
                Ok(res)
            }
            ContractRequest::Update { key, data } => {
                let parameters = {
                    self.contract_state
                        .get_params(&key)
                        .await
                        .map_err(|err| Either::Right(err.into()))?
                };
                let new_state = {
                    let state = self
                        .contract_state
                        .get(&key)
                        .await
                        .map_err(Into::into)
                        .map_err(Either::Right)?
                        .clone();
                    let mut retrieved_contracts = Vec::new();
                    retrieved_contracts.push(data);
                    loop {
                        let update_modification = self
                            .runtime
                            .update_state(&key, &parameters, &state, &retrieved_contracts)
                            .map_err(|err| match err {
                                err if err.is_contract_exec_error() => Either::Left(
                                    CoreContractError::Update {
                                        key: key.clone(),
                                        cause: format!("{err}"),
                                    }
                                    .into(),
                                ),
                                other => Either::Right(other.into()),
                            })?;
                        let UpdateModification {
                            new_state, related, ..
                        } = update_modification;
                        if let Some(new_state) = new_state {
                            let new_state = WrappedState::new(new_state.into_bytes());
                            self.contract_state
                                .store(key.clone(), new_state.clone(), None)
                                .await
                                .map_err(|err| Either::Right(err.into()))?;
                            break new_state;
                        } else if !related.is_empty() {
                            // some required contracts are missing
                            let required_contracts = related.len() + 1;
                            for RelatedContract {
                                contract_instance_id: id,
                                mode,
                            } in related
                            {
                                match self.contract_state.get(&id.into()).await {
                                    Ok(state) => {
                                        // in this case we are already subscribed to and are updating this contract,
                                        // we can try first with the existing value
                                        retrieved_contracts.push(UpdateData::RelatedState {
                                            related_to: id,
                                            state: state.into(),
                                        });
                                    }
                                    Err(StateStoreError::MissingContract(_))
                                        if self.mode == OperationMode::Network =>
                                    {
                                        // retrieve the contract from the network first in the mode the consumer contract informed the node
                                        todo!("related mode updates subscription not implemented for type {mode:?}")
                                    }
                                    Err(other_err) => return Err(Either::Right(other_err.into())),
                                }
                            }
                            if retrieved_contracts.len() == required_contracts {
                                // try running again with all the related contracts retrieved
                                continue;
                            } else {
                                todo!("keep waiting/trying until timeout?")
                            }
                        } else {
                            // state wasn't updated
                            break state;
                        }
                    }
                };
                // in the network impl this would be sent over the network
                let summary = self
                    .runtime
                    .summarize_state(&key, &parameters, &new_state)
                    .map_err(Into::into)
                    .map_err(Either::Right)?;
                self.send_update_notification(&key, &parameters, &new_state)
                    .await?;
                // TODO: in network mode, wait at least for one confirmation
                //       when a node receives a delta from updates, run the update themselves
                //       and send back confirmation
                Ok(ContractResponse::UpdateResponse { key, summary }.into())
            }
            ContractRequest::Get {
                key,
                fetch_contract: contract,
            } => self.perform_get(contract, key).await.map_err(Either::Left),
            ContractRequest::Subscribe { key, summary } => {
                let updates =
                    updates.ok_or_else(|| Either::Right("missing update channel".into()))?;
                self.register_contract_notifier(key.clone(), id, updates, summary)
                    .map_err(Either::Left)?;
                // by default a subscribe op has an implicit get
                self.perform_get(false, key).await.map_err(Either::Left)
                // todo: in network mode, also send a subscribe to keep up to date
            }
        }
    }

    fn delegate_op(&mut self, req: DelegateRequest<'_>) -> Response {
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
                match self.runtime.register_delegate(delegate, cipher, nonce) {
                    Ok(_) => Ok(HostResponse::Ok),
                    Err(err) => {
                        tracing::error!("failed registering delegate `{key}`: {err}");
                        Err(Either::Left(CoreDelegateError::RegisterError(key).into()))
                    }
                }
            }
            DelegateRequest::UnregisterDelegate(key) => {
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
            } => match self.runtime.inbound_app_message(
                &key,
                &params,
                vec![InboundDelegateMsg::GetSecretRequest(get_request)],
            ) {
                Ok(values) => Ok(HostResponse::DelegateResponse { key, values }),
                Err(err) => Err(Either::Right(
                    format!("uncontrolled error while getting secret for `{key}`: {err}").into(),
                )),
            },
            DelegateRequest::ApplicationMessages {
                key,
                inbound,
                params,
            } => {
                match self.runtime.inbound_app_message(
                    &key,
                    &params,
                    inbound
                        .into_iter()
                        .map(InboundDelegateMsg::into_owned)
                        .collect(),
                ) {
                    Ok(values) => Ok(HostResponse::DelegateResponse { key, values }),
                    Err(err) if err.is_delegate_exec_error() | err.is_execution_error() => {
                        tracing::error!("failed processing messages for delegate `{key}`: {err}");
                        Err(Either::Left(
                            CoreDelegateError::ExecutionError(format!("{err}")).into(),
                        ))
                    }
                    Err(err) if err.delegate_is_missing() => {
                        tracing::error!("delegate not found `{key}`");
                        Err(Either::Left(CoreDelegateError::Missing(key).into()))
                    }
                    Err(err) if err.secret_is_missing() => {
                        tracing::error!("secret not found `{key}`");
                        Err(Either::Left(
                            CoreDelegateError::MissingSecret {
                                key,
                                secret: err.get_secret_id(),
                            }
                            .into(),
                        ))
                    }
                    Err(err) => {
                        tracing::error!("failed executing delegate `{key}`: {err}");
                        Err(Either::Right(
                            format!("uncontrolled error while executing `{key}`").into(),
                        ))
                    }
                }
            }
        }
    }

    async fn send_update_notification<'a>(
        &mut self,
        key: &ContractKey,
        params: &Parameters<'a>,
        new_state: &WrappedState,
    ) -> Result<(), Either<RequestError, DynError>> {
        tracing::debug!(contract = %key, "notify of contract update");
        if let Some(notifiers) = self.update_notifications.get_mut(key) {
            let summaries = self.subscriber_summaries.get_mut(key).unwrap();
            // in general there should be less than 32 failures
            let mut failures = arrayvec::ArrayVec::<_, 32>::new();
            for (peer_key, notifier) in notifiers.iter() {
                let peer_summary = summaries.get_mut(peer_key).unwrap();
                let update = match peer_summary {
                    Some(summary) => self
                        .runtime
                        .get_state_delta(key, params, new_state, &*summary)
                        .map_err(|err| {
                            tracing::error!("{err}");
                            match err {
                                err if err.is_contract_exec_error() => Either::Left(
                                    CoreContractError::Put {
                                        key: key.clone(),
                                        cause: format!("{err}"),
                                    }
                                    .into(),
                                ),
                                other => Either::Right(other.into()),
                            }
                        })?
                        .to_owned()
                        .into(),
                    None => UpdateData::State(State::from(new_state.as_ref()).into_owned()),
                };
                if let Err(err) = notifier.send(Ok(ContractResponse::UpdateNotification {
                    key: key.clone(),
                    update,
                }
                .into()))
                {
                    let _ = failures.try_push(*peer_key);
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

    async fn perform_get(
        &mut self,
        contract: bool,
        key: ContractKey,
    ) -> Result<HostResponse, RequestError> {
        let mut got_contract = None;
        if contract {
            let parameters = self.contract_state.get_params(&key).await.map_err(|e| {
                tracing::error!("{e}");
                RequestError::from(CoreContractError::Get {
                    key: key.clone(),
                    cause: "missing contract".to_owned(),
                })
            })?;
            let contract = self
                .runtime
                .contract_store
                .fetch_contract(&key, &parameters)
                .ok_or_else(|| {
                    RequestError::from(CoreContractError::Get {
                        key: key.clone(),
                        cause: "Missing contract".into(),
                    })
                })?;
            got_contract = Some(contract);
        }
        match self.contract_state.get(&key).await {
            Ok(state) => Ok(ContractResponse::GetResponse {
                key,
                contract: got_contract,
                state,
            }
            .into()),
            Err(StateStoreError::MissingContract(_)) => Err(CoreContractError::Get {
                key,
                cause: "missing contract state".into(),
            }
            .into()),
            Err(err) => Err(CoreContractError::Get {
                key,
                cause: format!("{err}"),
            }
            .into()),
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use locutus_runtime::{ContractStore, StateStore};

    #[tokio::test(flavor = "multi_thread")]
    async fn local_node_handle() -> Result<(), Box<dyn std::error::Error>> {
        const MAX_SIZE: i64 = 10 * 1024 * 1024;
        const MAX_MEM_CACHE: u32 = 10_000_000;
        let tmp_path = std::env::temp_dir().join("locutus-test");
        let contract_store = ContractStore::new(tmp_path.join("executor-test"), MAX_SIZE)?;
        let state_store = StateStore::new(Storage::new().await?, MAX_MEM_CACHE).unwrap();
        let mut counter = 0;
        Executor::new(
            contract_store,
            DelegateStore::default(),
            SecretsStore::default(),
            state_store,
            || {
                counter += 1;
            },
            OperationMode::Local,
        )
        .await
        .expect("local node with handle");

        assert_eq!(counter, 1);
        Ok(())
    }
}
