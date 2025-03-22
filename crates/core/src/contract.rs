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

pub(crate) async fn contract_handling<CH>(mut contract_handler: CH) -> Result<(), ContractError>
where
    CH: ContractHandler + Send + 'static,
{
    use tokio::task::JoinSet;

    let mut pending_tasks = JoinSet::new();

    loop {
        // Check for completed tasks and send their responses via contract_handler.channel()
        while let Some(result) = pending_tasks.try_join_next() {
            match result {
                Ok((id, event, executor)) => {
                    // Return the executor back to the pool
                    contract_handler.executor().return_executor(executor);
                    
                    // Send the result using the contract_handler's channel
                    if let Err(error) = contract_handler.channel().send_to_sender(id, event).await {
                        tracing::debug!(%error, "shutting down contract handler");
                    }
                }
                Err(e) => {
                    tracing::error!("Task error: {:?}", e);
                    // Create a new executor to replace the one that failed
                    let new_executor = contract_handler.executor().create_new_executor().await;
                    contract_handler.executor().return_executor(new_executor);
                    tracing::info!("Created replacement executor after task failure");
                }
            }
        }

        // Wait for next event with a timeout to allow checking pending tasks
        let recv_result = tokio::time::timeout(
            std::time::Duration::from_millis(100),
            contract_handler.channel().recv_from_sender(),
        )
        .await;

        let (id, event) = match recv_result {
            Ok(Ok(result)) => result,
            Ok(Err(e)) => return Err(e),
            Err(_) => continue, // Timeout, continue to check pending tasks
        };

        tracing::debug!(%event, "Got contract handling event");

        match event {
            ContractHandlerEvent::GetQuery {
                key,
                return_contract_code,
            } => {
                // Clone needed values for the task
                let fetch_contract = contract_handler.executor().fetch_contract(key, return_contract_code).await;
                let id_clone = id;

                pending_tasks.spawn(async move {
                    let span = tracing::info_span!("fetch_contract", %key, %return_contract_code);
                    let _guard = span.enter();

                    let (executor, result) = fetch_contract.await;

                    let response_event = match result {
                        Ok((state, contract)) => {
                            tracing::debug!(with_contract_code = %return_contract_code, 
                                           has_contract = %contract.is_some(), 
                                           "Fetched contract {key}");

                            ContractHandlerEvent::GetResponse {
                                key,
                                response: Ok(StoreResponse { state, contract }),
                            }
                        }
                        Err(err) => {
                            tracing::warn!("Error while executing get contract query: {err}");

                            if err.is_fatal() {
                                tracing::error!("Fatal error encountered in executor");
                            }

                            ContractHandlerEvent::GetResponse {
                                key,
                                response: Err(err),
                            }
                        }
                    };

                    (id_clone, response_event, executor)
                });
            }
            ContractHandlerEvent::PutQuery {
                key,
                state,
                related_contracts,
                contract,
            } => {
                // Clone needed values for the task
                let put_future = contract_handler.executor().upsert_contract_state(key, Either::Left(state.clone()), related_contracts, contract).await;

                pending_tasks.spawn(async move {
                    let span = tracing::info_span!("upsert_contract_state", %key);
                    let _guard = span.enter();

                    let (executor, result) = put_future.await;

                    let event_result = match result {
                        Ok(UpsertResult::NoChange) => ContractHandlerEvent::PutResponse {
                            new_value: Ok(state),
                        },
                        Ok(UpsertResult::Updated(state)) => ContractHandlerEvent::PutResponse {
                            new_value: Ok(state),
                        },
                        Err(err) => {
                            if err.is_fatal() {
                                tracing::error!("Fatal error in executor during put");
                            }
                            ContractHandlerEvent::PutResponse {
                                new_value: Err(err),
                            }
                        }
                    };

                    (id, event_result, executor)
                });
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
                let update_future = contract_handler.executor().upsert_contract_state(key, update_value, related_contracts, None).await;

                pending_tasks.spawn(async move {
                    let span = tracing::info_span!("upsert_contract_state", %key);
                    let _guard = span.enter();

                    let (executor, result) = update_future.await;

                    let event_result = match result {
                        Ok(UpsertResult::NoChange) => ContractHandlerEvent::UpdateNoChange { key },
                        Ok(UpsertResult::Updated(state)) => ContractHandlerEvent::UpdateResponse {
                            new_value: Ok(state),
                        },
                        Err(err) => {
                            if err.is_fatal() {
                                tracing::error!("Fatal error in executor during update");
                            }
                            ContractHandlerEvent::UpdateResponse {
                                new_value: Err(err),
                            }
                        }
                    };

                    (id, event_result, executor)
                });
            }

            ContractHandlerEvent::RegisterSubscriberListener {
                key,
                client_id,
                summary,
                subscriber_listener,
            } => {
                let result = contract_handler.executor().register_contract_notifier(
                    key,
                    client_id,
                    subscriber_listener,
                    summary,
                );

                if let Err(err) = &result {
                    tracing::warn!("Error while registering subscriber listener: {err}");
                }

                if let Err(error) = contract_handler
                    .channel()
                    .send_to_sender(id, ContractHandlerEvent::RegisterSubscriberListenerResponse)
                    .await
                {
                    tracing::debug!(%error, "shutting down contract handler");
                    return Err(ContractError::ChannelDropped(Box::new(
                        ContractHandlerEvent::RegisterSubscriberListenerResponse,
                    )));
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
