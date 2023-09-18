use locutus_runtime::{prelude::ContractKey, ContractError as ContractRtError};

mod executor;
mod handler;
#[cfg(test)]
mod in_memory;
pub mod storages;

pub(crate) use handler::{
    contract_handler_channel, CHSenderHalve, ContractHandler, ContractHandlerChannel,
    ContractHandlerEvent, NetworkContractHandler, StoreResponse,
};
#[cfg(test)]
pub(crate) use in_memory::{MemoryContractHandler, MockRuntime};

use executor::ContractExecutor;
pub use executor::{Executor, ExecutorError, OperationMode};

pub(crate) async fn contract_handling<'a, CH>(mut contract_handler: CH) -> Result<(), ContractError>
where
    CH: ContractHandler + Send + 'static,
{
    loop {
        let res = contract_handler.channel().recv_from_listener().await?;
        match res {
            (
                _id,
                ContractHandlerEvent::GetQuery {
                    key,
                    fetch_contract,
                },
            ) => {
                match contract_handler
                    .executor()
                    .fetch_contract(key.clone(), fetch_contract)
                    .await
                {
                    Ok((state, contract)) => {
                        contract_handler
                            .channel()
                            .send_to_listener(
                                _id,
                                ContractHandlerEvent::GetResponse {
                                    key,
                                    response: Ok(StoreResponse {
                                        state: Some(state),
                                        contract,
                                    }),
                                },
                            )
                            .await?;
                    }
                    Err(err) => {
                        tracing::warn!("error while executing get contract query: {err}");
                        contract_handler
                            .channel()
                            .send_to_listener(
                                _id,
                                ContractHandlerEvent::GetResponse {
                                    key,
                                    response: Err(err.into()),
                                },
                            )
                            .await?;
                    }
                }
            }
            (id, ContractHandlerEvent::Cache(contract)) => {
                match contract_handler.executor().store_contract(contract).await {
                    Ok(_) => {
                        contract_handler
                            .channel()
                            .send_to_listener(id, ContractHandlerEvent::CacheResult(Ok(())))
                            .await?;
                    }
                    Err(err) => {
                        let err = ContractError::ContractRuntimeError(err);
                        contract_handler
                            .channel()
                            .send_to_listener(id, ContractHandlerEvent::CacheResult(Err(err)))
                            .await?;
                    }
                }
            }
            (
                _id,
                ContractHandlerEvent::PutQuery {
                    key: _key,
                    state: _state,
                },
            ) => {
                // let _put_result = contract_handler
                //     .handle_request(ClientRequest::Put {
                //         contract: todo!(),
                //         state: _state,
                //     }.into())
                //     .await
                //     .map(|r| {
                //         let _r = r.unwrap_put();
                //         unimplemented!();
                //     });
                // contract_handler
                //     .channel()
                //     .send_to_listener(
                //         _id,
                //         ContractHandlerEvent::PushResponse {
                //             new_value: put_result,
                //         },
                //     )
                //     .await?;
                todo!("perform put request");
            }
            _ => unreachable!(),
        }
    }
}

#[derive(Debug, thiserror::Error)]
pub(crate) enum ContractError {
    #[error("handler channel dropped")]
    ChannelDropped(Box<ContractHandlerEvent>),
    #[error("contract {0} not found in storage")]
    ContractNotFound(ContractKey),
    #[error("")]
    ContractRuntimeError(ContractRtError),
    #[error(transparent)]
    IOError(#[from] std::io::Error),
    #[error("no response received from handler")]
    NoEvHandlerResponse,
}
