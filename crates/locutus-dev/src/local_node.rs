use std::{collections::HashMap, path::PathBuf, sync::Arc};

use locutus_node::{either::Either, ClientRequest, HostResponse, RequestError};
use locutus_runtime::{prelude::*, ContractRuntimeError};
use locutus_stdlib::prelude::*;

use crate::DynError;

pub struct LocalNode {
    contract_params: HashMap<ContractKey, Parameters<'static>>,
    contract_data: HashMap<String, Arc<ContractData<'static>>>,
    pub(crate) contract_state: HashMap<ContractKey, WrappedState>,
    runtime: Runtime,
}

impl LocalNode {
    pub fn new() -> Self {
        let store = ContractStore::new(test_dir(), 10_000_000);
        Self {
            contract_params: HashMap::default(),
            contract_data: HashMap::default(),
            contract_state: HashMap::default(),
            runtime: Runtime::build(store, false).unwrap(),
        }
    }

    pub fn handle_request(
        &mut self,
        req: ClientRequest,
    ) -> Result<HostResponse, Either<RequestError, DynError>> {
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
                self.contract_state.insert(*key, state);
                self.contract_params.insert(*key, parameters);
                self.contract_data
                    .insert(key.contract_part_as_str(), contract.data().clone());
                // todo: should simulate sending "update notifications"
                is_valid
                    .then(|| HostResponse::PutResponse(*key))
                    .ok_or(Either::Left(RequestError::PutError(*key)))
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
                                    Either::Left(RequestError::PutError(key))
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
                    .summarize_state(&key, parameters.clone(), new_state)
                    .map_err(Into::into)
                    .map_err(Either::Right)?;
                // todo: should simulate sending "update notifications"
                Ok(HostResponse::UpdateResponse { key, summary })
            }
            ClientRequest::Get { key, contract } => {
                let contract = contract.then(|| {
                    let parameters = self.contract_params.get(&key).unwrap();
                    let data = self.contract_data.get(&key.contract_part_as_str()).unwrap();
                    WrappedContract::new(data.clone(), parameters.clone())
                });
                self.contract_state
                    .get(&key)
                    .cloned()
                    .map(|state| HostResponse::GetResponse { contract, state })
                    .ok_or_else(|| {
                        Either::Left(RequestError::GetError {
                            key,
                            cause: "missing contract state".into(),
                        })
                    })
            }
            ClientRequest::Disconnect { cause } => {
                if let Some(cause) = cause {
                    log::info!("disconnecting cause: {cause}");
                }
                Err(Either::Right("disconnected".into()))
            }
            _ => unimplemented!(),
        }
    }
}

impl Default for LocalNode {
    fn default() -> Self {
        Self::new()
    }
}

fn test_dir() -> PathBuf {
    let test_dir = std::env::temp_dir().join("locutus").join("contracts");
    if !test_dir.exists() {
        std::fs::create_dir_all(&test_dir).unwrap();
    }
    test_dir
}
