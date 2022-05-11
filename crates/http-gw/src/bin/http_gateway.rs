use std::net::{Ipv4Addr, SocketAddr};

use http_gw::HttpGateway;
use locutus_node::{ClientEventsCombinator, ClientEventsProxy, WebSocketProxy};
use locutus_stdlib::prelude::Contract;
use tracing::metadata::LevelFilter;
use tracing_subscriber::util::SubscriberInitExt;

pub(crate) type DynError = Box<dyn std::error::Error + Send + Sync + 'static>;

fn main() -> Result<(), DynError> {
    let sub = tracing_subscriber::fmt()
        .with_max_level(LevelFilter::DEBUG)
        .with_level(true)
        .finish();
    sub.init();
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(4)
        .enable_all()
        .build()
        .unwrap();

    rt.block_on(async move {
        let sample = Contract::new(vec![1, 2, 3, 4].into(), vec![].into());
        tracing::info!("available contract: {}", sample.key().encode());
        // 8xzpWrEm4bvnYBrneF3fTbNFN2JYpRKAZ2M7QgsmmBLS

        let socket: SocketAddr = (Ipv4Addr::LOCALHOST, 50509).into();
        let (http_handle, filter) = HttpGateway::as_filter();
        let ws_handle = WebSocketProxy::as_upgrade(socket, filter).await?;
        let mut all_clients =
            ClientEventsCombinator::new([Box::new(ws_handle), Box::new(http_handle)]);
        loop {
            let (id, req) = all_clients.recv().await?;
            tracing::info!("client {id}, req -> {req}");
        }
    })
}

#[cfg(feature = "local")]
mod local_dev {
    use std::collections::HashMap;

    use locutus_node::{either::Either, ClientRequest};
    use locutus_runtime::prelude::*;
    use locutus_stdlib::prelude::*;

    use crate::DynError;

    struct LocalNode {
        contract_params: HashMap<ContractKey, Parameters<'static>>,
        contract_data: HashMap<String, ContractData<'static>>,
        contract_state: HashMap<ContractKey, WrappedState>,
        runtime: Runtime,
    }

    impl LocalNode {
        pub(crate) fn new() -> Self {
            todo!()
        }

        pub(crate) fn handle_request(&mut self, req: ClientRequest) -> Result<(), DynError> {
            match req {
                ClientRequest::Put {
                    contract,
                    state,
                    parameters,
                } => {
                    let key = contract.key();
                    self.runtime.validate_state(key, parameters, state)?;
                }
                ClientRequest::Update { key, delta } => {
                    let parameters = self.contract_params.get(&key).unwrap();
                    match delta {
                        Either::Left(delta) => {
                            let state = self.contract_state.get(&key).unwrap().clone();
                            let new_state = self.runtime.update_state(
                                &key,
                                parameters.clone(),
                                state.clone(),
                                delta,
                            )?;
                            self.contract_state.insert(key, new_state);
                            // in the network impl this would be sent over the network
                            let _summary =
                                self.runtime
                                    .summarize_state(&key, parameters.clone(), state)?;
                            // todo: print summary
                        }
                        Either::Right(state) => {
                            if self.runtime.validate_state(
                                &key,
                                parameters.clone(),
                                state.clone(),
                            )? {
                                self.contract_state.insert(key, state);
                            } else {
                                todo!("log this")
                            }
                        }
                    }
                    // self.runtime.update_state_from_summary(key, parameters, current_state, current_summary);
                }
                ClientRequest::Get { key, contract } => {
                    if contract {
                        let parameters = self.contract_params.get(&key).unwrap();
                        let data = self.contract_data.get(&key.contract_part_as_str()).unwrap();
                        let _contract = Contract::new(data.clone(), parameters.clone());
                    }
                    let _state = self.contract_state.get(&key).unwrap();
                }
                ClientRequest::Subscribe { .. } => {}
                ClientRequest::Disconnect { cause } => {
                    if let Some(cause) = cause {
                        todo!("log cause: {cause}");
                    }
                }
            }
            Ok(())
        }
    }
}
