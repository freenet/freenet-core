use std::{collections::HashMap, sync::Arc};

use crossbeam::channel::Sender;
use locutus_node::{
    either::Either, ClientRequest, HostResponse, PeerKey, RequestError, SqlitePool,
};
use locutus_runtime::{prelude::*, ContractRuntimeError};

use crate::DynError;

type Response = Result<HostResponse, Either<RequestError, DynError>>;

/// A node which only functions on the local host without any network.
/// Use for testing pourpouses.
///
/// This node will monitor the store directories and databases to detect state changes.
/// Consumers of the node are required to poll for new changes in order to be notified
/// of changes or can alternatively use the notification channel.
pub struct LocalNode {
    contract_params: HashMap<ContractKey, Parameters<'static>>,
    contract_data: HashMap<String, Arc<ContractCode<'static>>>,
    runtime: Runtime,
    update_notifications: HashMap<ContractKey, Vec<(PeerKey, Sender<HostResponse>)>>,
    subscriber_summaries: HashMap<ContractKey, HashMap<PeerKey, StateSummary<'static>>>,
    contract_state: StateStore<SqlitePool>,
}

impl LocalNode {
    pub async fn new(
        store: ContractStore,
        contract_state: StateStore<SqlitePool>,
    ) -> Result<Self, DynError> {
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
        peer_key: PeerKey,
        notification_ch: Sender<HostResponse>,
        summary: StateSummary<'static>,
    ) -> Result<(), DynError> {
        let channels = self.update_notifications.entry(key).or_default();
        if let Ok(i) = channels.binary_search_by_key(&&peer_key, |(p, _)| p) {
            let (_, existing_ch) = &channels[i];
            if !existing_ch.same_channel(&notification_ch) {
                return Err(format!("peer {peer_key} has multiple notification channels").into());
            }
        } else {
            channels.push((peer_key, notification_ch));
        }

        if self
            .subscriber_summaries
            .entry(key)
            .or_default()
            .insert(peer_key, summary)
            .is_some()
        {
            log::warn!(
                "contract {key} already was registered for peer {peer_key}; replaced summary"
            );
        }
        Ok(())
    }

    pub async fn preload(&mut self, contract: WrappedContract<'static>, state: WrappedState) {
        todo!()
    }

    pub async fn handle_request(&mut self, req: ClientRequest) -> Response {
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
                let key = contract.key();
                let is_valid = self
                    .runtime
                    .validate_state(key, contract.params(), &state)
                    .map_err(Into::into)
                    .map_err(Either::Right)?;
                self.contract_state
                    .store(*key, state.clone())
                    .await
                    .map_err(|e| Either::Right(e.into()))?;
                self.contract_params.insert(*key, contract.params().clone());
                self.contract_data
                    .insert(key.contract_part_as_str(), contract.code().clone());
                let res = is_valid
                    .then(|| HostResponse::PutResponse(*key))
                    .ok_or(Either::Left(RequestError::Put(*key)));
                self.send_update_notification(key, contract.params(), &state)?;
                res
            }
            ClientRequest::Update { key, delta } => {
                let parameters = self.contract_params.get(&key).unwrap().clone();
                let new_state = {
                    let state = self.contract_state.get(&key).await.unwrap().clone();
                    let new_state = self
                        .runtime
                        .update_state(&key, &parameters, &state, &delta)
                        .map_err(|err| match err {
                            ContractRuntimeError::ExecError(ExecError::InvalidPutValue) => {
                                Either::Left(RequestError::Put(key))
                            }
                            other => Either::Right(other.into()),
                        })?;
                    self.contract_state
                        .store(key, new_state.clone())
                        .await
                        .unwrap();
                    new_state
                };
                // in the network impl this would be sent over the network
                let summary = self
                    .runtime
                    .summarize_state(&key, &parameters, &new_state)
                    .map_err(Into::into)
                    .map_err(Either::Right)?;
                self.send_update_notification(&key, &parameters, &new_state)?;
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
                // by default a subscribe op has an implicit get
                self.perform_get(true, key).await.map_err(Either::Left)
                // todo: in network mode, also send a subscribe to keep up to date
            }
            ClientRequest::Disconnect { cause } => {
                if let Some(cause) = cause {
                    log::info!("disconnecting cause: {cause}");
                }
                Err(Either::Right("disconnected".into()))
            }
        }
    }

    fn send_update_notification<'a>(
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
                            Either::Left(RequestError::Put(*key))
                        }
                        other => Either::Right(other.into()),
                    })?;
                notifier
                    .send(HostResponse::UpdateNotification { key: *key, update })
                    .map_err(|_| Either::Right("disconnected".into()))?;
            }
        }
        Ok(())
    }

    async fn perform_get(
        &mut self,
        contract: bool,
        key: ContractKey,
    ) -> Result<HostResponse, RequestError> {
        let contract = contract.then(|| {
            let parameters = self.contract_params.get(&key).unwrap();
            let data = self.contract_data.get(&key.contract_part_as_str()).unwrap();
            WrappedContract::new(data.clone(), parameters.clone())
        });
        match self.contract_state.get(&key).await {
            Ok(state) => Ok(HostResponse::GetResponse { contract, state }),
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
