//! Handling of contracts and delegates, including storage, execution, caching, etc.
//!
//! Internally uses the wasm_runtime module to execute contract and/or delegate instructions.

use either::Either;
use freenet_stdlib::client_api::DelegateRequest;
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

use tracing::Instrument;

/// Handle delegate response values that may contain contract requests.
/// This function loops to process any GetContractRequest or PutContractRequest messages,
/// performing the async contract operations and sending responses back to the delegate.
async fn handle_delegate_with_contract_requests<CH>(
    contract_handler: &mut CH,
    delegate_key: DelegateKey,
    params: Parameters<'static>,
    attested_contract: Option<ContractInstanceId>,
    mut response_values: Vec<OutboundDelegateMsg>,
) -> Vec<OutboundDelegateMsg>
where
    CH: ContractHandler + Send + 'static,
{
    const MAX_ITERATIONS: usize = 100;
    let mut iteration = 0;
    let mut final_results = Vec::new();

    loop {
        if iteration >= MAX_ITERATIONS {
            tracing::error!(
                delegate_key = %delegate_key,
                "Max iterations reached handling delegate contract requests"
            );
            break;
        }
        iteration += 1;

        // Separate pending contract requests from other messages
        let mut pending_get_requests = Vec::new();
        let mut pending_put_requests = Vec::new();
        let mut other_messages = Vec::new();

        for msg in response_values.drain(..) {
            match msg {
                OutboundDelegateMsg::GetContractRequest(req) if !req.processed => {
                    pending_get_requests.push(req);
                }
                OutboundDelegateMsg::PutContractRequest(req) if !req.processed => {
                    pending_put_requests.push(req);
                }
                other => other_messages.push(other),
            }
        }

        // Add non-contract-request messages to final results
        final_results.extend(other_messages);

        // If no pending contract requests, we're done
        if pending_get_requests.is_empty() && pending_put_requests.is_empty() {
            break;
        }

        // Process GET contract requests
        for get_req in pending_get_requests {
            let contract_id = get_req.contract_id.clone();
            let context = get_req.context.clone();

            tracing::debug!(
                delegate_key = %delegate_key,
                contract_id = %contract_id,
                "Processing GetContractRequest from delegate"
            );

            // Fetch the contract state
            let key = contract_handler.executor().lookup_key(&contract_id);
            let state = match key {
                Some(key) => {
                    match contract_handler
                        .executor()
                        .fetch_contract(key, false) // Only fetch state, not contract code
                        .await
                    {
                        Ok((state, _contract)) => state,
                        Err(err) => {
                            tracing::warn!(
                                delegate_key = %delegate_key,
                                contract_id = %contract_id,
                                error = %err,
                                "Failed to fetch contract for delegate GetContractRequest"
                            );
                            None
                        }
                    }
                }
                None => {
                    tracing::debug!(
                        delegate_key = %delegate_key,
                        contract_id = %contract_id,
                        "Contract not found for delegate GetContractRequest"
                    );
                    None
                }
            };

            // Create response and send back to delegate
            let response = GetContractResponse {
                contract_id: contract_id.clone(),
                state,
                context,
            };

            let inbound = vec![InboundDelegateMsg::GetContractResponse(response)];

            match contract_handler.executor().execute_delegate_request(
                DelegateRequest::ApplicationMessages {
                    key: delegate_key.clone(),
                    inbound: inbound.iter().map(|m| m.clone().into_owned()).collect(),
                    params: params.clone(),
                },
                attested_contract.as_ref(),
            ) {
                Ok(freenet_stdlib::client_api::HostResponse::DelegateResponse {
                    values, ..
                }) => {
                    response_values = values;
                }
                Ok(_) => {
                    tracing::warn!(
                        delegate_key = %delegate_key,
                        "Unexpected response after GetContractResponse"
                    );
                }
                Err(err) => {
                    tracing::error!(
                        delegate_key = %delegate_key,
                        error = %err,
                        "Failed to send GetContractResponse to delegate"
                    );
                }
            }
        }

        // Process PUT contract requests
        for put_req in pending_put_requests {
            let contract = put_req.contract.clone();
            let state = put_req.state.clone();
            let related_contracts = put_req.related_contracts.clone();
            let context = put_req.context.clone();
            let contract_key = contract.key();
            let contract_id = ContractInstanceId::from(contract_key);

            tracing::debug!(
                delegate_key = %delegate_key,
                contract_key = %contract_key,
                "Processing PutContractRequest from delegate"
            );

            // Perform the PUT operation
            let result = match contract_handler
                .executor()
                .upsert_contract_state(
                    contract_key,
                    Either::Left(state.clone()),
                    related_contracts.into_owned(),
                    Some(contract),
                )
                .await
            {
                Ok(_) => Ok(()),
                Err(err) => {
                    tracing::warn!(
                        delegate_key = %delegate_key,
                        contract_key = %contract_key,
                        error = %err,
                        "Failed to PUT contract for delegate PutContractRequest"
                    );
                    Err(err.to_string())
                }
            };

            // Create response and send back to delegate
            let response = PutContractResponse {
                contract_id: contract_id.clone(),
                result,
                context,
            };

            let inbound = vec![InboundDelegateMsg::PutContractResponse(response)];

            match contract_handler.executor().execute_delegate_request(
                DelegateRequest::ApplicationMessages {
                    key: delegate_key.clone(),
                    inbound: inbound.iter().map(|m| m.clone().into_owned()).collect(),
                    params: params.clone(),
                },
                attested_contract.as_ref(),
            ) {
                Ok(freenet_stdlib::client_api::HostResponse::DelegateResponse {
                    values, ..
                }) => {
                    response_values.extend(values);
                }
                Ok(_) => {
                    tracing::warn!(
                        delegate_key = %delegate_key,
                        "Unexpected response after PutContractResponse"
                    );
                }
                Err(err) => {
                    tracing::error!(
                        delegate_key = %delegate_key,
                        error = %err,
                        "Failed to send PutContractResponse to delegate"
                    );
                }
            }
        }
    }

    final_results
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
                // Extract params for contract request handling (only available for ApplicationMessages)
                let params_for_contract_requests = match &req {
                    DelegateRequest::ApplicationMessages { params, .. } => {
                        Some(params.clone().into_owned())
                    }
                    _ => None,
                };
                tracing::debug!(
                    delegate_key = %delegate_key,
                    ?attested_contract,
                    "Processing delegate request"
                );

                let initial_response = match contract_handler
                    .executor()
                    .execute_delegate_request(req, attested_contract.as_ref())
                {
                    Ok(freenet_stdlib::client_api::HostResponse::DelegateResponse {
                        key: _,
                        values,
                    }) => values,
                    Ok(freenet_stdlib::client_api::HostResponse::Ok) => Vec::new(),
                    Ok(_other) => {
                        tracing::error!(
                            delegate_key = %delegate_key,
                            phase = "unexpected_response",
                            "Unexpected response type from delegate request"
                        );
                        // Don't crash the node - log and continue with empty response
                        Vec::new()
                    }
                    Err(err) => {
                        tracing::error!(
                            delegate_key = %delegate_key,
                            error = %err,
                            phase = "execution_failed",
                            "Failed executing delegate request"
                        );
                        // Don't crash the node - log and continue with empty response.
                        // This can happen during startup if stale delegate state references
                        // data that is no longer valid (e.g., missing attested contract).
                        Vec::new()
                    }
                };

                // Handle any contract requests (GET/PUT) from the delegate
                let response = if let Some(params) = params_for_contract_requests {
                    handle_delegate_with_contract_requests(
                        &mut contract_handler,
                        delegate_key.clone(),
                        params,
                        attested_contract.clone(),
                        initial_response,
                    )
                    .await
                } else {
                    // For RegisterDelegate/UnregisterDelegate, just return the initial response
                    initial_response
                };

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
