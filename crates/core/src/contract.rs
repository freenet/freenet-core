use crate::runtime::ContractError as ContractRtError;
use either::Either;
use freenet_stdlib::prelude::*;

mod executor;
mod handler;
#[cfg(test)]
mod in_memory;
pub mod storages;

pub(crate) use executor::{
    executor_channel, ExecutorToEventLoopChannel, NetworkEventListenerHalve,
};
pub(crate) use handler::{
    contract_handler_channel, ClientResponses, ClientResponsesSender, ContractHandler,
    ContractHandlerEvent, ContractHandlerToEventLoopChannel, EventId, NetEventListenerHalve,
    NetworkContractHandler, StoreResponse,
};
#[cfg(test)]
pub(crate) use in_memory::{MemoryContractHandler, MockRuntime};

pub use executor::{Executor, ExecutorError, OperationMode};

use executor::ContractExecutor;

#[tracing::instrument(skip_all)]
pub(crate) async fn contract_handling<'a, CH>(mut contract_handler: CH) -> Result<(), ContractError>
where
    CH: ContractHandler + Send + 'static,
{
    loop {
        let (id, event) = contract_handler.channel().recv_from_event_loop().await?;
        tracing::debug!(%event, "Got contract handling event");
        match event {
            ContractHandlerEvent::GetQuery {
                key,
                fetch_contract,
            } => {
                match contract_handler
                    .executor()
                    .fetch_contract(key.clone(), fetch_contract)
                    .await
                {
                    Ok((state, contract)) => {
                        tracing::debug!("Fetched contract {key}");
                        contract_handler
                            .channel()
                            .send_to_event_loop(
                                id,
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
                        tracing::warn!("Error while executing get contract query: {err}");
                        contract_handler
                            .channel()
                            .send_to_event_loop(
                                id,
                                ContractHandlerEvent::GetResponse {
                                    key,
                                    response: Err(err.into()),
                                },
                            )
                            .await?;
                    }
                }
            }
            ContractHandlerEvent::Cache(contract) => {
                match contract_handler.executor().store_contract(contract).await {
                    Ok(_) => {
                        contract_handler
                            .channel()
                            .send_to_event_loop(id, ContractHandlerEvent::CacheResult(Ok(())))
                            .await?;
                    }
                    Err(err) => {
                        tracing::error!("Error while caching: {err}");
                        let err = ContractError::ContractRuntimeError(err);
                        contract_handler
                            .channel()
                            .send_to_event_loop(id, ContractHandlerEvent::CacheResult(Err(err)))
                            .await?;
                    }
                }
            }
            ContractHandlerEvent::PutQuery {
                key,
                state,
                related_contracts,
                parameters,
            } => {
                let put_result = contract_handler
                    .executor()
                    .upsert_contract_state(key, Either::Left(state), related_contracts, parameters)
                    .await
                    .map_err(Into::into);
                contract_handler
                    .channel()
                    .send_to_event_loop(
                        id,
                        ContractHandlerEvent::PutResponse {
                            new_value: put_result,
                        },
                    )
                    .await?;
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
    #[error(transparent)]
    ContractRuntimeError(ContractRtError),
    #[error("{0}")]
    IOError(#[from] std::io::Error),
    #[error("no response received from handler")]
    NoEvHandlerResponse,
}
