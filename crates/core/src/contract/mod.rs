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
    NetworkEventListenerHalve, UpsertResult,
};
pub(crate) use handler::{
    client_responses_channel, contract_handler_channel, in_memory::MemoryContractHandler,
    ClientResponsesReceiver, ClientResponsesSender, ContractHandler, ContractHandlerChannel,
    ContractHandlerEvent, NetworkContractHandler, SenderHalve, StoreResponse, WaitingResolution,
    WaitingTransaction,
};

pub use executor::{Executor, ExecutorError, OperationMode};

use executor::ContractExecutor;
use tracing::Instrument;

pub(crate) async fn contract_handling<CH>(mut contract_handler: CH) -> Result<(), ContractError>
where
    CH: ContractHandler + Send + 'static,
{
    loop {
        let (id, event) = contract_handler.channel().recv_from_sender().await?;
        tracing::debug!(%event, "Got contract handling event");
        match event {
            ContractHandlerEvent::GetQuery {
                key,
                return_contract_code,
            } => {
                let start = std::time::Instant::now();
                tracing::info!(%key, %return_contract_code, "Starting contract GET execution");

                match contract_handler
                    .executor()
                    .fetch_contract(key, return_contract_code)
                    .instrument(tracing::info_span!("fetch_contract", %key, %return_contract_code))
                    .await
                {
                    Ok((state, contract)) => {
                        let elapsed = start.elapsed();
                        if elapsed > std::time::Duration::from_millis(10) {
                            tracing::warn!(%key, elapsed_ms = elapsed.as_millis(), "SLOW contract GET execution blocked message pipeline!");
                        } else {
                            tracing::info!(%key, elapsed_ms = elapsed.as_millis(), "Contract GET execution completed");
                        }

                        tracing::debug!(with_contract_code = %return_contract_code, has_contract = %contract.is_some(), "Fetched contract {key}");
                        contract_handler
                            .channel()
                            .send_to_sender(
                                id,
                                ContractHandlerEvent::GetResponse {
                                    key,
                                    response: Ok(StoreResponse { state, contract }),
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
                        if err.is_fatal() {
                            todo!("Handle fatal error; reset executor");
                        }
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
                let start = std::time::Instant::now();
                tracing::info!(%key, "Starting contract PUT execution");

                let put_result = contract_handler
                    .executor()
                    .upsert_contract_state(
                        key,
                        Either::Left(state.clone()),
                        related_contracts,
                        contract,
                    )
                    .instrument(tracing::info_span!("upsert_contract_state", %key))
                    .await;

                let elapsed = start.elapsed();
                if elapsed > std::time::Duration::from_millis(10) {
                    tracing::warn!(%key, elapsed_ms = elapsed.as_millis(), "SLOW contract PUT execution blocked message pipeline!");
                } else {
                    tracing::info!(%key, elapsed_ms = elapsed.as_millis(), "Contract PUT execution completed");
                }

                let event_result = match put_result {
                    Ok(UpsertResult::NoChange) => ContractHandlerEvent::PutResponse {
                        new_value: Ok(state),
                    },
                    Ok(UpsertResult::Updated(state)) => ContractHandlerEvent::PutResponse {
                        new_value: Ok(state),
                    },
                    Err(err) => {
                        if err.is_fatal() {
                            todo!("Handle fatal error; reset executor");
                        }
                        ContractHandlerEvent::PutResponse {
                            new_value: Err(err),
                        }
                    } // UpsertResult::NotAvailable is not used in this path
                };

                contract_handler
                    .channel()
                    .send_to_sender(id, event_result)
                    .await
                    .map_err(|error| {
                        tracing::debug!(%error, "shutting down contract handler");
                        error
                    })?;
            }
            ContractHandlerEvent::UpdateQuery {
                key,
                data,
                related_contracts,
            } => {
                let update_value: Either<WrappedState, StateDelta<'static>> = match data {
                    freenet_stdlib::prelude::UpdateData::State(state) => {
                        Either::Left(WrappedState::from(state.into_bytes()))
                    }
                    freenet_stdlib::prelude::UpdateData::Delta(delta) => Either::Right(delta),
                    _ => unreachable!(),
                };
                let update_result = contract_handler
                    .executor()
                    .upsert_contract_state(key, update_value, related_contracts, None)
                    .instrument(tracing::info_span!("upsert_contract_state", %key))
                    .await;

                let event_result = match update_result {
                    Ok(UpsertResult::NoChange) => ContractHandlerEvent::UpdateNoChange { key },
                    Ok(UpsertResult::Updated(state)) => ContractHandlerEvent::UpdateResponse {
                        new_value: Ok(state),
                    },
                    Err(err) => {
                        if err.is_fatal() {
                            todo!("Handle fatal error; reset executor");
                        }
                        ContractHandlerEvent::UpdateResponse {
                            new_value: Err(err),
                        }
                    }
                };

                contract_handler
                    .channel()
                    .send_to_sender(id, event_result)
                    .await
                    .map_err(|error| {
                        tracing::debug!(%error, "shutting down contract handler");
                        error
                    })?;
            }
            ContractHandlerEvent::DelegateRequest {
                req,
                attested_contract,
            } => {
                let delegate_key = req.key().clone();
                tracing::debug!(
                    delegate_key = %delegate_key,
                    ?attested_contract,
                    "Processing delegate request"
                );

                let response = match contract_handler
                    .executor()
                    .execute_delegate_request(req, attested_contract.as_ref())
                {
                    Ok(freenet_stdlib::client_api::HostResponse::DelegateResponse {
                        key: _,
                        values,
                    }) => values,
                    Ok(freenet_stdlib::client_api::HostResponse::Ok) => Vec::new(),
                    Ok(_other) => {
                        tracing::error!("unexpected response type from delegate request");
                        return Err(ContractError::NoEvHandlerResponse);
                    }
                    Err(err) => {
                        tracing::error!("failed executing delegate request: {}", err);
                        return Err(ContractError::NoEvHandlerResponse);
                    }
                };

                contract_handler
                    .channel()
                    .send_to_sender(id, ContractHandlerEvent::DelegateResponse(response))
                    .await
                    .map_err(|error| {
                        tracing::debug!(%error, "shutting down contract handler");
                        error
                    })?;
            }
            ContractHandlerEvent::RegisterSubscriberListener {
                key,
                client_id,
                summary,
                subscriber_listener,
            } => {
                let _ = contract_handler
                    .executor()
                    .register_contract_notifier(key, client_id, subscriber_listener, summary)
                    .inspect_err(|err| {
                        tracing::warn!("Error while registering subscriber listener: {err}");
                    });

                // FIXME: if there is an error senc actually an error back
                contract_handler
                    .channel()
                    .send_to_sender(id, ContractHandlerEvent::RegisterSubscriberListenerResponse)
                    .await
                    .inspect_err(|error| {
                        tracing::debug!(%error, "shutting down contract handler");
                    })?;
            }
            ContractHandlerEvent::QuerySubscriptions { callback } => {
                // Get subscription information from the executor and send it through the callback
                let subscriptions = contract_handler.executor().get_subscription_info();
                let connections = vec![]; // For now, we'll populate this from the calling context
                let network_debug = crate::message::NetworkDebugInfo {
                    application_subscriptions: subscriptions,
                    network_subscriptions: vec![], // Contract handler only tracks application subscriptions
                    connected_peers: connections,
                };
                let _ = callback
                    .send(crate::message::QueryResult::NetworkDebug(network_debug))
                    .await;

                contract_handler
                    .channel()
                    .send_to_sender(id, ContractHandlerEvent::QuerySubscriptionsResponse)
                    .await
                    .inspect_err(|error| {
                        tracing::debug!(%error, "shutting down contract handler");
                    })?;
            }
            _ => unreachable!("ContractHandlerEvent enum should be exhaustive here"),
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
