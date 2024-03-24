//! Handling of contracts and delegates, including storage, execution, caching, etc.
//!
//! Internally uses the wasm_runtime module to execute contract and/or delegate instructions.

use either::Either;
use freenet_stdlib::prelude::*;

mod executor;
mod handler;
pub mod storages;

pub(crate) use executor::{
    executor_channel, mock_runtime::MockRuntime, Callback, ExecutorToEventLoopChannel,
    NetworkEventListenerHalve,
};
pub(crate) use handler::{
    client_responses_channel, contract_handler_channel, in_memory::MemoryContractHandler,
    ClientResponsesReceiver, ClientResponsesSender, ContractHandler, ContractHandlerChannel,
    ContractHandlerEvent, NetworkContractHandler, SenderHalve, StoreResponse, WaitingResolution,
};

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
                    .instrument(tracing::info_span!("fetch_contract", %key, %fetch_contract))
                    .await
                {
                    Ok((state, contract)) => {
                        tracing::debug!(with_contract = %fetch_contract, has_contract = %contract.is_some(), "Fetched contract {key}");
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
                                    response: Err(err),
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
            ContractHandlerEvent::UpdateQuery {
                key,
                state,
                related_contracts,
            } => {
                let update_result = contract_handler
                    .executor()
                    .upsert_contract_state(
                        key.clone(),
                        Either::Left(state.clone()),
                        related_contracts,
                        None,
                    )
                    .instrument(tracing::info_span!("upsert_contract_state", %key))
                    .await;

                contract_handler
                    .channel()
                    .send_to_sender(
                        id,
                        ContractHandlerEvent::UpdateResponse {
                            new_value: update_result.map_err(Into::into),
                        },
                    )
                    .await
                    .map_err(|error| {
                        tracing::debug!(%error, "shutting down contract handler");
                        error
                    })?;
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
