//! Handling of contracts and delegates, including storage, execution, caching, etc.
//!
//! Internally uses the wasm_runtime module to execute contract and/or delegate instructions.

use either::Either;
use freenet_stdlib::prelude::*;

mod executor;
mod handler;
pub mod storages;

pub(crate) use executor::{
    mediator_channels, mock_runtime::MockRuntime, op_request_channel, run_op_request_mediator,
    Callback, ContractExecutor, ExecutorToEventLoopChannel, NetworkEventListenerHalve,
    UpsertResult,
};

// Re-export CRDT emulation functions for testing
pub use executor::mock_runtime::{clear_crdt_contracts, is_crdt_contract, register_crdt_contract};
pub(crate) use handler::{
    client_responses_channel, contract_handler_channel,
    in_memory::{MemoryContractHandler, SimulationContractHandler, SimulationHandlerBuilder},
    ClientResponsesReceiver, ClientResponsesSender, ContractHandler, ContractHandlerChannel,
    ContractHandlerEvent, NetworkContractHandler, SenderHalve, SessionMessage, StoreResponse,
    WaitingResolution, WaitingTransaction,
};

pub use executor::{Executor, ExecutorError, OperationMode};
pub use handler::reset_event_id_counter;

use freenet_stdlib::client_api::DelegateRequest;
use tracing::Instrument;

/// Maximum iterations when handling GetContractRequest to prevent infinite loops
const MAX_CONTRACT_REQUEST_ITERATIONS: usize = 100;

/// Handle a delegate request, including any GetContractRequest messages in the response.
///
/// When a delegate emits GetContractRequest, this function:
/// 1. Fetches the contract state from the local state store
/// 2. Creates a GetContractResponse and sends it back to the delegate
/// 3. Repeats until no more GetContractRequest messages
///
/// Returns the final response with GetContractRequest messages filtered out.
async fn handle_delegate_with_contract_requests<CH>(
    contract_handler: &mut CH,
    initial_req: DelegateRequest<'static>,
    attested_contract: Option<&ContractInstanceId>,
    delegate_key: &DelegateKey,
) -> Vec<OutboundDelegateMsg>
where
    CH: ContractHandler + Send + 'static,
{
    // Extract initial params from the request (only ApplicationMessages has params we need)
    let initial_params = match &initial_req {
        DelegateRequest::ApplicationMessages { params, .. } => params.clone(),
        DelegateRequest::GetSecretRequest { params, .. } => params.clone(),
        _ => Parameters::from(Vec::new()),
    };

    let mut current_req = initial_req;
    let current_params = initial_params;
    let mut iterations = 0;
    // Accumulate non-contract-request messages across iterations
    let mut accumulated_messages: Vec<OutboundDelegateMsg> = Vec::new();

    loop {
        iterations += 1;
        if iterations > MAX_CONTRACT_REQUEST_ITERATIONS {
            tracing::error!(
                delegate_key = %delegate_key,
                iterations = iterations,
                "Exceeded maximum GetContractRequest iterations, possible infinite loop"
            );
            // Return whatever we accumulated so far
            return accumulated_messages;
        }

        // Execute the delegate request
        let values = match contract_handler
            .executor()
            .execute_delegate_request(current_req, attested_contract)
            .await
        {
            Ok(freenet_stdlib::client_api::HostResponse::DelegateResponse { key: _, values }) => {
                values
            }
            Ok(freenet_stdlib::client_api::HostResponse::Ok) => Vec::new(),
            Ok(_other) => {
                tracing::error!(
                    delegate_key = %delegate_key,
                    phase = "unexpected_response",
                    "Unexpected response type from delegate request"
                );
                // Return whatever we accumulated so far
                return accumulated_messages;
            }
            Err(err) => {
                tracing::error!(
                    delegate_key = %delegate_key,
                    error = %err,
                    phase = "execution_failed",
                    "Failed executing delegate request"
                );
                // Return whatever we accumulated so far
                return accumulated_messages;
            }
        };

        // Check for GetContractRequest messages
        let mut contract_requests: Vec<GetContractRequest> = Vec::new();

        for msg in values {
            match msg {
                OutboundDelegateMsg::GetContractRequest(req) => {
                    contract_requests.push(req);
                }
                other => {
                    // Accumulate non-contract-request messages
                    accumulated_messages.push(other);
                }
            }
        }

        // If no contract requests, we're done - return all accumulated messages
        if contract_requests.is_empty() {
            return accumulated_messages;
        }

        tracing::debug!(
            delegate_key = %delegate_key,
            count = contract_requests.len(),
            "Processing GetContractRequest messages from delegate"
        );

        // Process each contract request and build responses
        let mut inbound_responses: Vec<InboundDelegateMsg<'static>> = Vec::new();
        for req in contract_requests {
            let contract_id = req.contract_id;
            let context = req.context;

            // Look up the full key from the instance id
            let state = match contract_handler.executor().lookup_key(&contract_id) {
                Some(full_key) => {
                    // Fetch the contract state
                    match contract_handler
                        .executor()
                        .fetch_contract(full_key, false)
                        .await
                    {
                        Ok((state, _)) => state,
                        Err(err) => {
                            tracing::warn!(
                                contract = %contract_id,
                                error = %err,
                                "Failed to fetch contract for delegate GetContractRequest"
                            );
                            None
                        }
                    }
                }
                None => {
                    tracing::debug!(
                        contract = %contract_id,
                        "Contract not found locally for delegate GetContractRequest"
                    );
                    None
                }
            };

            inbound_responses.push(InboundDelegateMsg::GetContractResponse(
                GetContractResponse {
                    contract_id,
                    state,
                    context,
                },
            ));
        }

        // Create a new request to send the responses back to the delegate
        current_req = DelegateRequest::ApplicationMessages {
            key: delegate_key.clone(),
            inbound: inbound_responses,
            params: current_params.clone(),
        };
    }
}

pub(crate) async fn contract_handling<CH>(mut contract_handler: CH) -> Result<(), ContractError>
where
    CH: ContractHandler + Send + 'static,
{
    loop {
        let (id, event) = contract_handler.channel().recv_from_sender().await?;
        tracing::debug!(
            event = %event,
            "Received contract handling event"
        );
        match event {
            ContractHandlerEvent::GetQuery {
                instance_id,
                return_contract_code,
            } => {
                // Look up the full key from the instance_id
                let key = contract_handler.executor().lookup_key(&instance_id);
                match key {
                    Some(key) => {
                        match contract_handler
                            .executor()
                            .fetch_contract(key, return_contract_code)
                            .instrument(
                                tracing::info_span!("fetch_contract", %key, %return_contract_code),
                            )
                            .await
                        {
                            Ok((state, contract)) => {
                                tracing::debug!(
                                    contract = %key,
                                    with_contract_code = return_contract_code,
                                    has_contract = contract.is_some(),
                                    has_state = state.is_some(),
                                    phase = "get_complete",
                                    "Fetched contract"
                                );
                                // Send response back to caller. If the caller disconnected, just log and continue.
                                if let Err(error) = contract_handler
                                    .channel()
                                    .send_to_sender(
                                        id,
                                        ContractHandlerEvent::GetResponse {
                                            key: Some(key),
                                            response: Ok(StoreResponse { state, contract }),
                                        },
                                    )
                                    .await
                                {
                                    tracing::debug!(
                                        error = %error,
                                        contract = %key,
                                        "Failed to send GET response (client may have disconnected)"
                                    );
                                }
                            }
                            Err(err) => {
                                tracing::warn!(
                                    contract = %key,
                                    error = %err,
                                    phase = "get_failed",
                                    "Error executing get contract query"
                                );
                                if err.is_fatal() {
                                    tracing::error!(
                                        contract = %key,
                                        error = %err,
                                        phase = "fatal_error",
                                        "Fatal executor error during get query"
                                    );
                                    return Err(ContractError::FatalExecutorError {
                                        key,
                                        error: err,
                                    });
                                }
                                // Send error response back to caller. If the caller disconnected, just log and continue.
                                if let Err(error) = contract_handler
                                    .channel()
                                    .send_to_sender(
                                        id,
                                        ContractHandlerEvent::GetResponse {
                                            key: Some(key),
                                            response: Err(err),
                                        },
                                    )
                                    .await
                                {
                                    tracing::debug!(
                                        error = %error,
                                        contract = %key,
                                        "Failed to send GET error response (client may have disconnected)"
                                    );
                                }
                            }
                        }
                    }
                    None => {
                        // Contract not found locally - return None for key
                        tracing::debug!(
                            instance_id = %instance_id,
                            phase = "not_found",
                            "Contract not found in local store"
                        );
                        if let Err(error) = contract_handler
                            .channel()
                            .send_to_sender(
                                id,
                                ContractHandlerEvent::GetResponse {
                                    key: None,
                                    response: Ok(StoreResponse {
                                        state: None,
                                        contract: None,
                                    }),
                                },
                            )
                            .await
                        {
                            tracing::debug!(
                                error = %error,
                                instance_id = %instance_id,
                                "Failed to send GET not-found response (client may have disconnected)"
                            );
                        }
                    }
                }
            }
            ContractHandlerEvent::PutQuery {
                key,
                state,
                related_contracts,
                contract,
            } => {
                // DEBUG: Log contract details in PutQuery handler
                if let Some(ref contract_container) = contract {
                    tracing::debug!(
                        contract = %key,
                        key_code_hash = ?key.code_hash(),
                        container_key = %contract_container.key(),
                        container_code_hash = ?contract_container.key().code_hash(),
                        data_len = contract_container.data().len(),
                        phase = "put_query_debug",
                        "DEBUG PUT: In PutQuery handler with contract"
                    );
                } else {
                    tracing::debug!(
                        contract = %key,
                        phase = "put_query_debug",
                        "DEBUG PUT: In PutQuery handler - contract is None"
                    );
                }

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

                let event_result = match put_result {
                    Ok(UpsertResult::NoChange) => ContractHandlerEvent::PutResponse {
                        new_value: Ok(state),
                        state_changed: false,
                    },
                    Ok(UpsertResult::Updated(new_state)) => ContractHandlerEvent::PutResponse {
                        new_value: Ok(new_state),
                        state_changed: true,
                    },
                    Ok(UpsertResult::CurrentWon(current_state)) => {
                        // Merge resulted in no change (incoming state was old/already incorporated).
                        ContractHandlerEvent::PutResponse {
                            new_value: Ok(current_state),
                            state_changed: false,
                        }
                    }
                    Err(err) => {
                        if err.is_fatal() {
                            tracing::error!(
                                contract = %key,
                                error = %err,
                                phase = "fatal_error",
                                "Fatal executor error during put query"
                            );
                            return Err(ContractError::FatalExecutorError { key, error: err });
                        }
                        ContractHandlerEvent::PutResponse {
                            new_value: Err(err),
                            state_changed: false,
                        }
                    }
                };

                // Send response back to caller. If the caller disconnected (e.g., WebSocket closed),
                // the response channel may be dropped. This is not fatal - the contract has already
                // been stored, so we just log and continue processing other events.
                if let Err(error) = contract_handler
                    .channel()
                    .send_to_sender(id, event_result)
                    .await
                {
                    tracing::debug!(
                        error = %error,
                        contract = %key,
                        "Failed to send PUT response (client may have disconnected)"
                    );
                }
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
                    Ok(UpsertResult::NoChange) => {
                        tracing::info!(
                            contract = %key,
                            phase = "update_no_change",
                            "UPDATE resulted in NoChange, fetching current state to return UpdateResponse"
                        );
                        // When there's no change, we still need to return the current state
                        // so the client gets a proper response
                        match contract_handler.executor().fetch_contract(key, false).await {
                            Ok((Some(current_state), _)) => {
                                tracing::info!(
                                    contract = %key,
                                    phase = "fetch_complete",
                                    "Successfully fetched current state for NoChange update"
                                );
                                ContractHandlerEvent::UpdateResponse {
                                    new_value: Ok(current_state),
                                    state_changed: false,
                                }
                            }
                            Ok((None, _)) => {
                                tracing::warn!(
                                    contract = %key,
                                    phase = "fetch_failed",
                                    "No state found when fetching for NoChange update"
                                );
                                // Fallback to the old behavior if we can't fetch the state
                                ContractHandlerEvent::UpdateNoChange { key }
                            }
                            Err(err) => {
                                tracing::error!(
                                    contract = %key,
                                    error = %err,
                                    phase = "fetch_error",
                                    "Error fetching state for NoChange update"
                                );
                                // Fallback to the old behavior if we can't fetch the state
                                ContractHandlerEvent::UpdateNoChange { key }
                            }
                        }
                    }
                    Ok(UpsertResult::Updated(state)) => ContractHandlerEvent::UpdateResponse {
                        new_value: Ok(state),
                        state_changed: true,
                    },
                    Ok(UpsertResult::CurrentWon(current_state)) => {
                        // Merge resulted in no change (incoming state was old/already incorporated).
                        // Return current state with state_changed=false.
                        ContractHandlerEvent::UpdateResponse {
                            new_value: Ok(current_state),
                            state_changed: false,
                        }
                    }
                    Err(err) => {
                        if err.is_fatal() {
                            tracing::error!(
                                contract = %key,
                                error = %err,
                                phase = "fatal_error",
                                "Fatal executor error during update query"
                            );
                            return Err(ContractError::FatalExecutorError { key, error: err });
                        }
                        ContractHandlerEvent::UpdateResponse {
                            new_value: Err(err),
                            state_changed: false,
                        }
                    }
                };

                // Send response back to caller. If the caller disconnected, the response channel
                // may be dropped. This is not fatal - the update has already been applied.
                if let Err(error) = contract_handler
                    .channel()
                    .send_to_sender(id, event_result)
                    .await
                {
                    tracing::debug!(
                        error = %error,
                        contract = %key,
                        "Failed to send UPDATE response (client may have disconnected)"
                    );
                }
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

                // Execute the delegate and handle any GetContractRequest messages
                let response = handle_delegate_with_contract_requests(
                    &mut contract_handler,
                    req,
                    attested_contract.as_ref(),
                    &delegate_key,
                )
                .await;

                // Send response back to caller. If the caller disconnected, the response channel
                // may be dropped. This is not fatal - the delegate has already been processed.
                if let Err(error) = contract_handler
                    .channel()
                    .send_to_sender(id, ContractHandlerEvent::DelegateResponse(response))
                    .await
                {
                    tracing::debug!(
                        error = %error,
                        delegate_key = %delegate_key,
                        "Failed to send DELEGATE response (client may have disconnected)"
                    );
                }
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
                        tracing::warn!(
                            contract = %key,
                            client = %client_id,
                            error = %err,
                            phase = "registration_failed",
                            "Error registering subscriber listener"
                        );
                    });

                // FIXME: if there is an error send actually an error back
                // If the caller disconnected, just log and continue.
                if let Err(error) = contract_handler
                    .channel()
                    .send_to_sender(id, ContractHandlerEvent::RegisterSubscriberListenerResponse)
                    .await
                {
                    tracing::debug!(
                        error = %error,
                        contract = %key,
                        "Failed to send RegisterSubscriberListener response (client may have disconnected)"
                    );
                }
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

                // If the caller disconnected, just log and continue.
                if let Err(error) = contract_handler
                    .channel()
                    .send_to_sender(id, ContractHandlerEvent::QuerySubscriptionsResponse)
                    .await
                {
                    tracing::debug!(
                        error = %error,
                        "Failed to send QuerySubscriptions response (client may have disconnected)"
                    );
                }
            }
            ContractHandlerEvent::GetSummaryQuery { key } => {
                let summary = contract_handler
                    .executor()
                    .summarize_contract_state(key)
                    .instrument(tracing::info_span!("summarize_contract_state", %key))
                    .await;

                if let Err(error) = contract_handler
                    .channel()
                    .send_to_sender(
                        id,
                        ContractHandlerEvent::GetSummaryResponse { key, summary },
                    )
                    .await
                {
                    tracing::debug!(
                        error = %error,
                        contract = %key,
                        "Failed to send GetSummary response (client may have disconnected)"
                    );
                }
            }
            ContractHandlerEvent::GetDeltaQuery { key, their_summary } => {
                let delta = contract_handler
                    .executor()
                    .get_contract_state_delta(key, their_summary)
                    .instrument(tracing::info_span!("get_contract_state_delta", %key))
                    .await;

                if let Err(error) = contract_handler
                    .channel()
                    .send_to_sender(id, ContractHandlerEvent::GetDeltaResponse { key, delta })
                    .await
                {
                    tracing::debug!(
                        error = %error,
                        contract = %key,
                        "Failed to send GetDelta response (client may have disconnected)"
                    );
                }
            }
            _ => unreachable!("ContractHandlerEvent enum should be exhaustive here"),
        }
    }
}

#[derive(Debug, thiserror::Error)]
pub(crate) enum ContractError {
    #[error("handler channel dropped")]
    ChannelDropped(Box<ContractHandlerEvent>),
    #[error("{0}")]
    IOError(#[from] std::io::Error),
    #[error("no response received from handler")]
    NoEvHandlerResponse,
    #[error("fatal executor error for contract {key}: {error}")]
    FatalExecutorError {
        key: ContractKey,
        error: ExecutorError,
    },
}
