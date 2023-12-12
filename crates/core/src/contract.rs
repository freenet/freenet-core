use either::Either;
use freenet_stdlib::prelude::*;

mod executor;
mod handler;
mod in_memory;
pub mod storages;

pub(crate) use executor::{
    executor_channel, Callback, ExecutorToEventLoopChannel, NetworkEventListenerHalve,
};
pub(crate) use handler::{
    contract_handler_channel, ClientResponses, ClientResponsesSender, ContractHandler,
    ContractHandlerChannel, ContractHandlerEvent, EventId, NetworkContractHandler, SenderHalve,
    StoreResponse,
};
pub(crate) use in_memory::{MemoryContractHandler, MockRuntime};

pub use executor::{Executor, ExecutorError, OperationMode};

use executor::ContractExecutor;
use tracing::Instrument;

pub(crate) async fn contract_handling<'a, CH>(mut contract_handler: CH) -> Result<(), ContractError>
where
    CH: ContractHandler + Send + 'static,
{
    loop {
        let (id, event) = contract_handler.channel().recv_from_sender().await?;
        tracing::debug!(%event, "Got contract handling event");
        match event {
            ContractHandlerEvent::GetQuery {
                key,
                fetch_contract,
            } => {
                match contract_handler
                    .executor()
                    .fetch_contract(key.clone(), fetch_contract)
                    .instrument(tracing::info_span!("fetch_contract", %key))
                    .await
                {
                    Ok((state, contract)) => {
                        tracing::debug!("Fetched contract {key}");
                        contract_handler
                            .channel()
                            .send_to_sender(
                                id,
                                ContractHandlerEvent::GetResponse {
                                    key,
                                    response: Ok(StoreResponse {
                                        state: Some(state),
                                        contract,
                                    }),
                                },
                            )
                            .await
                            .map_err(|error| {
                                tracing::debug!(%error, "shutting down contract handler");
                                error
                            })?;
                    }
                    Err(err) => {
                        tracing::warn!("Error while executing get contract query: {err}");
                        contract_handler
                            .channel()
                            .send_to_sender(
                                id,
                                ContractHandlerEvent::GetResponse {
                                    key,
                                    response: Err(err.into()),
                                },
                            )
                            .await
                            .map_err(|error| {
                                tracing::debug!(%error, "shutting down contract handler");
                                error
                            })?;
                    }
                }
            }
            ContractHandlerEvent::PutQuery {
                key,
                state,
                related_contracts,
                contract,
            } => {
                let put_result = contract_handler
                    .executor()
                    .upsert_contract_state(
                        key.clone(),
                        Either::Left(state),
                        related_contracts,
                        contract,
                    )
                    .instrument(tracing::info_span!("upsert_contract_state", %key))
                    .await;
                contract_handler
                    .channel()
                    .send_to_sender(
                        id,
                        ContractHandlerEvent::PutResponse {
                            new_value: put_result.map_err(Into::into),
                        },
                    )
                    .await
                    .map_err(|error| {
                        tracing::debug!(%error, "shutting down contract handler");
                        error
                    })?;
            }
            ContractHandlerEvent::Cache(contract) => {
                let key = contract.key();
                match contract_handler
                    .executor()
                    .store_contract(contract)
                    .instrument(tracing::info_span!("store_contract", %key))
                    .await
                {
                    Ok(_) => {
                        contract_handler
                            .channel()
                            .send_to_sender(id, ContractHandlerEvent::CacheResult(Ok(())))
                            .await
                            .map_err(|error| {
                                tracing::debug!(%error, "shutting down contract handler");
                                error
                            })?;
                    }
                    Err(err) => {
                        tracing::error!("Error while caching: {err}");
                        contract_handler
                            .channel()
                            .send_to_sender(id, ContractHandlerEvent::CacheResult(Err(err)))
                            .await
                            .map_err(|error| {
                                tracing::debug!(%error, "shutting down contract handler");
                                error
                            })?;
                    }
                }
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
    #[error("{0}")]
    IOError(#[from] std::io::Error),
    #[error("no response received from handler")]
    NoEvHandlerResponse,
}
