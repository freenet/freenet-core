use std::{collections::HashMap, sync::Arc};

use crossbeam::channel::Sender;
use locutus_node::{either::Either, ClientRequest, HostResponse, RequestError};
use locutus_runtime::{prelude::*, ContractRuntimeError};
use locutus_stdlib::prelude::*;

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
    contract_data: HashMap<String, Arc<ContractData<'static>>>,
    pub(crate) contract_state: HashMap<ContractKey, WrappedState>,
    runtime: Runtime,
    update_notifications: HashMap<ContractKey, Vec<Sender<HostResponse>>>,
}

impl LocalNode {
    pub fn new(store: ContractStore) -> Self {
        Self {
            contract_params: HashMap::default(),
            contract_data: HashMap::default(),
            contract_state: HashMap::default(),
            runtime: Runtime::build(store, false).unwrap(),
            update_notifications: HashMap::default(),
        }
    }

    pub fn register_contract_notifier(
        &mut self,
        key: ContractKey,
        notification_ch: Sender<HostResponse>,
    ) {
        self.update_notifications
            .entry(key)
            .or_default()
            .push(notification_ch);
    }

    pub fn handle_request(&mut self, req: ClientRequest) -> Response {
        match req {
            ClientRequest::Put {
                contract,
                state,
                parameters,
            } => {
                let key = contract.key();
                let is_valid = self
                    .runtime
                    .validate_state(key, parameters.clone(), state.clone())
                    .map_err(Into::into)
                    .map_err(Either::Right)?;
                self.contract_state.insert(*key, state.clone());
                self.contract_params.insert(*key, parameters);
                self.contract_data
                    .insert(key.contract_part_as_str(), contract.data().clone());
                let res = is_valid
                    .then(|| HostResponse::PutResponse(*key))
                    .ok_or(Either::Left(RequestError::Put(*key)));
                if let Some(notifiers) = self.update_notifications.get(key) {
                    // todo: keep track of summaries from consumers and return deltas
                    for notifier in notifiers {
                        notifier
                            .send(HostResponse::UpdateNotification {
                                key: *key,
                                update: Either::Right(state.clone()),
                            })
                            .map_err(|_| Either::Right("disconnected".into()))?;
                    }
                }
                res
            }
            ClientRequest::Update { key, delta } => {
                let parameters = self.contract_params.get(&key).unwrap();
                let new_state = match delta {
                    Either::Left(delta) => {
                        let state = self.contract_state.get(&key).unwrap().clone();
                        let new_state = self
                            .runtime
                            .update_state(&key, parameters.clone(), state, delta)
                            .map_err(|err| match err {
                                ContractRuntimeError::ExecError(ExecError::InvalidPutValue) => {
                                    Either::Left(RequestError::Put(key))
                                }
                                other => Either::Right(other.into()),
                            })?;
                        self.contract_state.insert(key, new_state.clone());
                        new_state
                    }
                    Either::Right(state) => {
                        if self
                            .runtime
                            .validate_state(&key, parameters.clone(), state.clone())
                            .map_err(Into::into)
                            .map_err(Either::Right)?
                        {
                            self.contract_state.insert(key, state.clone());
                            state
                        } else {
                            log::error!("Invalid state for `{key}`: {state}");
                            todo!()
                        }
                    }
                };
                // in the network impl this would be sent over the network
                let summary = self
                    .runtime
                    .summarize_state(&key, parameters.clone(), new_state.clone())
                    .map_err(Into::into)
                    .map_err(Either::Right)?;
                if let Some(notifiers) = self.update_notifications.get(&key) {
                    // todo: keep track of summaries from consumers and return deltas
                    for notifier in notifiers {
                        notifier
                            .send(HostResponse::UpdateNotification {
                                key,
                                update: Either::Right(new_state.clone()),
                            })
                            .map_err(|_| Either::Right("disconnected".into()))?;
                    }
                }
                Ok(HostResponse::UpdateResponse { key, summary })
            }
            ClientRequest::Get { key, contract } => {
                self.perform_get(contract, key).map_err(Either::Left)
            }
            ClientRequest::Subscribe { key } => self.perform_get(true, key).map_err(Either::Left),
            ClientRequest::Disconnect { cause } => {
                if let Some(cause) = cause {
                    log::info!("disconnecting cause: {cause}");
                }
                Err(Either::Right("disconnected".into()))
            }
        }
    }

    fn perform_get(
        &mut self,
        contract: bool,
        key: ContractKey,
    ) -> Result<HostResponse, RequestError> {
        let contract = contract.then(|| {
            let parameters = self.contract_params.get(&key).unwrap();
            let data = self.contract_data.get(&key.contract_part_as_str()).unwrap();
            WrappedContract::new(data.clone(), parameters.clone())
        });
        self.contract_state
            .get(&key)
            .cloned()
            .map(|state| HostResponse::GetResponse { contract, state })
            .ok_or_else(|| RequestError::Get {
                key,
                cause: "missing contract state".into(),
            })
    }
}
