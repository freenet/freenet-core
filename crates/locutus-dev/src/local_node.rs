use std::{collections::HashMap, sync::Arc};

use locutus_node::{
    either::Either, ClientError, ClientId, ClientRequest, HostResponse, HostResult, RequestError,
    SqlitePool,
};
use locutus_runtime::{prelude::*, ContractRuntimeError};
use tokio::sync::mpsc::UnboundedSender;

use crate::DynError;

type Response = Result<HostResponse, Either<RequestError, DynError>>;

/// A node which only functions on the local host without any network.
/// Use for testing pourpouses.
///
/// This node will monitor the store directories and databases to detect state changes.
/// Consumers of the node are required to poll for new changes in order to be notified
/// of changes or can alternatively use the notification channel.
#[derive(Clone)]
pub struct LocalNode {
    contract_params: HashMap<ContractKey, Parameters<'static>>,
    contract_data: HashMap<String, Arc<ContractCode<'static>>>,
    pub runtime: Runtime,
    update_notifications: HashMap<ContractKey, Vec<(ClientId, UnboundedSender<HostResult>)>>,
    subscriber_summaries: HashMap<ContractKey, HashMap<ClientId, StateSummary<'static>>>,
    pub contract_state: StateStore<SqlitePool>,
}

impl LocalNode {
    pub async fn new(
        store: ContractStore,
        contract_state: StateStore<SqlitePool>,
        ctrl_handler: impl FnOnce(),
    ) -> Result<Self, DynError> {
        ctrl_handler();

        Ok(Self {
            contract_params: HashMap::default(),
            contract_data: HashMap::default(),
            contract_state,
            runtime: Runtime::build(store, false).unwrap(),
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
        let channels = self.update_notifications.entry(key).or_default();
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
            .entry(key)
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
        contract: WrappedContract<'static>,
        state: WrappedState,
    ) {
        tracing::warn!("preload: {}", contract.key());
        if let Err(err) = self
            .handle_request(
                cli_id,
                locutus_node::ClientRequest::Put { contract, state },
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
        req: ClientRequest,
        updates: Option<UnboundedSender<Result<HostResponse, ClientError>>>,
    ) -> Response {
        match req {
            ClientRequest::Put { contract, state } => {
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
                self.runtime
                    .contracts
                    .store_contract(contract.clone())
                    .map_err(Into::into)
                    .map_err(Either::Right)?;

                let key = contract.key();
                tracing::debug!("executing with params: {:?}", contract.params());
                let is_valid = self
                    .runtime
                    .validate_state(key, contract.params(), &state)
                    .map_err(Into::into)
                    .map_err(Either::Right)?;
                let res = is_valid
                    .then(|| HostResponse::PutResponse { key: *key })
                    .ok_or_else(|| {
                        Either::Left(RequestError::Put {
                            key: *key,
                            cause: "not valid".to_owned(),
                        })
                    })?;

                self.contract_state
                    .store(*key, state.clone(), Some(contract.params().clone()))
                    .await
                    .map_err(Into::into)
                    .map_err(Either::Right)?;
                self.contract_params.insert(*key, contract.params().clone());
                tracing::warn!("inserting: {}", contract.key());
                self.contract_data.insert(
                    key.contract_part_encoded().unwrap(),
                    contract.code().clone(),
                );
                self.send_update_notification(key, contract.params(), &state)
                    .await
                    .map_err(|_| {
                        Either::Left(RequestError::Put {
                            key: *key,
                            cause: "failed while sending notifications".to_owned(),
                        })
                    })?;
                Ok(res)
            }
            ClientRequest::Update { key, delta } => {
                let parameters = {
                    tracing::warn!("updating: {key}");
                    match self
                        .contract_params
                        .get(&key)
                        .ok_or_else(|| Either::Right("contract not found".into()))
                    {
                        Ok(state) => state.clone(),
                        Err(err) => self
                            .contract_state
                            .get_params(&key)
                            .await
                            .map_err(|_| err)?,
                    }
                };

                let new_state = {
                    let state = self
                        .contract_state
                        .get(&key)
                        .await
                        .map_err(Into::into)
                        .map_err(Either::Right)?
                        .clone();

                    let new_state = self
                        .runtime
                        .update_state(&key, &parameters, &state, &delta)
                        .map_err(|err| match err {
                            ContractRuntimeError::ExecError(ExecError::InvalidPutValue) => {
                                Either::Left(RequestError::Update {
                                    key,
                                    cause: "invalid update value".to_owned(),
                                })
                            }
                            other => Either::Right(other.into()),
                        })?;
                    self.contract_state
                        .store(key, new_state.clone(), None)
                        .await
                        .map_err(|err| Either::Right(err.into()))?;
                    new_state
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
                Ok(HostResponse::UpdateResponse { key, summary })
            }
            ClientRequest::Get {
                key,
                fetch_contract: contract,
            } => self.perform_get(contract, key).await.map_err(Either::Left),
            ClientRequest::Subscribe { key } => {
                let updates =
                    updates.ok_or_else(|| Either::Right("missing update channel".into()))?;
                self.register_contract_notifier(key, id, updates, [].as_ref().into())
                    .unwrap();
                tracing::info!("getting contract: {}", key.encode());
                // by default a subscribe op has an implicit get
                self.perform_get(true, key).await.map_err(Either::Left)
                // todo: in network mode, also send a subscribe to keep up to date
            }
            ClientRequest::Disconnect { cause } => {
                if let Some(cause) = cause {
                    tracing::info!("disconnecting cause: {cause}");
                }
                Err(Either::Left(RequestError::Disconnect))
            }
        }
    }

    async fn send_update_notification<'a>(
        &'a mut self,
        key: &ContractKey,
        params: &Parameters<'a>,
        new_state: &WrappedState,
    ) -> Result<(), Either<RequestError, DynError>> {
        if let Some(notifiers) = self.update_notifications.get(key) {
            let summaries = self.subscriber_summaries.get_mut(key).unwrap();
            for (peer_key, notifier) in notifiers {
                let peer_summary = summaries.get_mut(peer_key).unwrap();
                let update = self
                    .runtime
                    .get_state_delta(key, params, new_state, &*peer_summary)
                    .map_err(|err| match err {
                        ContractRuntimeError::ExecError(ExecError::InvalidPutValue) => {
                            Either::Left(RequestError::Put {
                                key: *key,
                                cause: "invalid put value".to_owned(),
                            })
                        }
                        other => Either::Right(other.into()),
                    })?;
                notifier
                    .send(Ok(HostResponse::UpdateNotification { key: *key, update }))
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
        let got_contract = contract
            .then(|| {
                let parameters =
                    self.contract_params
                        .get(&key)
                        .ok_or_else(|| RequestError::Get {
                            key,
                            cause: "missing contract".to_owned(),
                        })?;
                let data_key = key
                    .contract_part_encoded()
                    .or_else(|| {
                        Some(ContractCode::encode_key(
                            &self.runtime.contracts.code_hash_from_key(&key).unwrap(),
                        ))
                    })
                    .unwrap();
                let data = self.contract_data.get(&data_key).unwrap();
                Ok(WrappedContract::new(data.clone(), parameters.clone()))
            })
            .transpose()?;
        match self.contract_state.get(&key).await {
            Ok(state) => Ok(HostResponse::GetResponse {
                contract: got_contract,
                state,
            }),
            Err(StateStoreError::MissingContract) => Err(RequestError::Get {
                key,
                cause: "missing contract state".into(),
            }),
            Err(err) => Err(RequestError::Get {
                key,
                cause: format!("{err}"),
            }),
        }
    }
}

#[cfg(test)]
mod test {
    use crate::LocalNode;
    use locutus_node::SqlitePool;
    use locutus_runtime::{ContractStore, StateStore};

    #[tokio::test(flavor = "multi_thread")]
    async fn local_node_handle() -> Result<(), Box<dyn std::error::Error>> {
        const MAX_SIZE: i64 = 10 * 1024 * 1024;
        const MAX_MEM_CACHE: u32 = 10_000_000;
        let tmp_path = std::env::temp_dir().join("locutus");
        let contract_store = ContractStore::new(tmp_path.join("contracts"), MAX_SIZE);
        let state_store = StateStore::new(SqlitePool::new().await?, MAX_MEM_CACHE).unwrap();
        let mut counter = 0;
        LocalNode::new(contract_store.clone(), state_store.clone(), || {
            counter += 1;
        })
        .await
        .expect("local node with handle");

        assert_eq!(counter, 1);

        Ok(())
    }
}
