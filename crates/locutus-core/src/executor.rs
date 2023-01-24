//! Contract executor.

use std::collections::HashMap;

use blake2::digest::generic_array::GenericArray;
use locutus_runtime::prelude::*;
use locutus_stdlib::client_api::{
    ClientError, ClientRequest, ComponentRequest, ContractRequest, ContractResponse, HostResponse,
};
use tokio::sync::mpsc::UnboundedSender;

use crate::{
    client_events::{ComponentError as CoreComponentError, ContractError as CoreContractError},
    either::Either,
    ClientId, DynError, HostResult, RequestError, SqlitePool,
};

type Response = Result<HostResponse, Either<RequestError, DynError>>;

/// A WASM executor which will run any contracts, components, etc. registered.
///
/// This executor will monitor the store directories and databases to detect state changes.
/// Consumers of the executor are required to poll for new changes in order to be notified
/// of changes or can alternatively use the notification channel.
pub struct Executor {
    runtime: Runtime,
    contract_state: StateStore<SqlitePool>,
    update_notifications: HashMap<ContractKey, Vec<(ClientId, UnboundedSender<HostResult>)>>,
    subscriber_summaries: HashMap<ContractKey, HashMap<ClientId, StateSummary<'static>>>,
}

impl Executor {
    pub async fn new(
        store: ContractStore,
        contract_state: StateStore<SqlitePool>,
        ctrl_handler: impl FnOnce(),
    ) -> Result<Self, DynError> {
        ctrl_handler();

        Ok(Self {
            runtime: Runtime::build(
                store,
                ComponentStore::default(),
                SecretsStore::default(),
                false,
            )
            .unwrap(),
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
        summary: StateSummary<'static>,
    ) -> Result<(), DynError> {
        let channels = self.update_notifications.entry(key.clone()).or_default();
        if let Ok(i) = channels.binary_search_by_key(&&cli_id, |(p, _)| p) {
            let (_, existing_ch) = &channels[i];
            if !existing_ch.same_channel(&notification_ch) {
                return Err(format!("peer {cli_id} has multiple notification channels").into());
            }
        } else {
            channels.push((cli_id, notification_ch));
        }

        if self
            .subscriber_summaries
            .entry(key.clone())
            .or_default()
            .insert(cli_id, summary)
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
            ClientRequest::ComponentOp(op) => self.component_op(op),
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
                related_contracts,
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
                let is_valid = self
                    .runtime
                    .validate_state(&key, &params, &state, related_contracts)
                    .map_err(Into::into)
                    .map_err(Either::Right)?
                    == ValidateResult::Valid;
                // FIXME: should deal with additional related contracts requested
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
                    let update_modification = self
                        .runtime
                        .update_state(&key, &parameters, &state, &[data])
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
                    if let Some(new_state) = update_modification.new_state {
                        let new_state = WrappedState::new(new_state.into_bytes());
                        self.contract_state
                            .store(key.clone(), new_state.clone(), None)
                            .await
                            .map_err(|err| Either::Right(err.into()))?;
                        new_state
                    } else {
                        todo!()
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
            ContractRequest::Subscribe { key } => {
                let updates =
                    updates.ok_or_else(|| Either::Right("missing update channel".into()))?;
                self.register_contract_notifier(key.clone(), id, updates, [].as_ref().into())
                    .unwrap();
                tracing::info!("getting contract: {}", key.encoded_contract_id());
                // by default a subscribe op has an implicit get
                self.perform_get(true, key).await.map_err(Either::Left)
                // todo: in network mode, also send a subscribe to keep up to date
            }
        }
    }

    fn component_op(&mut self, req: ComponentRequest<'_>) -> Response {
        match req {
            ComponentRequest::RegisterComponent {
                component,
                cipher,
                nonce,
            } => {
                use chacha20poly1305::{KeyInit, XChaCha20Poly1305};
                let key = component.key().clone();

                let arr = GenericArray::from_slice(&cipher);
                let cipher = XChaCha20Poly1305::new(arr);
                let nonce = GenericArray::from_slice(&nonce).to_owned();

                match self.runtime.register_component(component, cipher, nonce) {
                    Ok(_) => Ok(HostResponse::Ok),
                    Err(err) => {
                        tracing::error!("failed registering component `{key}`: {err}");
                        Err(Either::Left(CoreComponentError::RegisterError(key).into()))
                    }
                }
            }
            ComponentRequest::UnregisterComponent(key) => {
                match self.runtime.unregister_component(&key) {
                    Ok(_) => Ok(HostResponse::Ok),
                    Err(err) => {
                        tracing::error!("failed unregistering component `{key}`: {err}");
                        Ok(HostResponse::Ok)
                    }
                }
            }
            ComponentRequest::ApplicationMessages { key, inbound } => {
                match self.runtime.inbound_app_message(
                    &key,
                    inbound
                        .into_iter()
                        .map(InboundComponentMsg::into_owned)
                        .collect(),
                ) {
                    Ok(values) => Ok(HostResponse::ComponentResponse { key, values }),
                    Err(err) if err.is_component_exec_error() => {
                        tracing::error!("failed processing messages for component `{key}`: {err}");
                        Err(Either::Left(
                            CoreComponentError::ExecutionError(format!("{err}")).into(),
                        ))
                    }
                    Err(err) => {
                        tracing::error!("failed executing component `{key}`: {err}");
                        Ok(HostResponse::Ok)
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
        if let Some(notifiers) = self.update_notifications.get(key) {
            let summaries = self.subscriber_summaries.get_mut(key).unwrap();
            for (peer_key, notifier) in notifiers {
                let peer_summary = summaries.get_mut(peer_key).unwrap();
                // FIXME: here we are always sending an state_delta,
                //        but what we send back depends on the kind of subscription
                let update = self
                    .runtime
                    .get_state_delta(key, params, new_state, &*peer_summary)
                    .map_err(|err| match err {
                        err if err.is_contract_exec_error() => Either::Left(
                            CoreContractError::Put {
                                key: key.clone(),
                                cause: format!("{err}"),
                            }
                            .into(),
                        ),
                        other => Either::Right(other.into()),
                    })?;
                notifier
                    .send(Ok(ContractResponse::UpdateNotification {
                        key: key.clone(),
                        update: update.to_owned().into(),
                    }
                    .into()))
                    .unwrap();
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
                contract: got_contract,
                state,
            }
            .into()),
            Err(StateStoreError::MissingContract) => Err(CoreContractError::Get {
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
        let state_store = StateStore::new(SqlitePool::new().await?, MAX_MEM_CACHE).unwrap();
        let mut counter = 0;
        Executor::new(contract_store, state_store, || {
            counter += 1;
        })
        .await
        .expect("local node with handle");

        assert_eq!(counter, 1);
        Ok(())
    }
}
