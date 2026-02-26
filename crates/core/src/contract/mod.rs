//! Handling of contracts and delegates, including storage, execution, caching, etc.
//!
//! Internally uses the wasm_runtime module to execute contract and/or delegate instructions.

use std::sync::Arc;

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
    in_memory::{
        MemoryContractHandler, MockWasmContractHandler, MockWasmHandlerBuilder,
        SimulationContractHandler, SimulationHandlerBuilder,
    },
    ClientResponsesReceiver, ClientResponsesSender, ContractHandler, ContractHandlerChannel,
    ContractHandlerEvent, NetworkContractHandler, SenderHalve, SessionMessage, StoreResponse,
    WaitingResolution, WaitingTransaction,
};

pub use executor::{Executor, ExecutorError, OperationMode};
pub use handler::reset_event_id_counter;

use freenet_stdlib::client_api::DelegateRequest;
use tracing::Instrument;

use self::executor::DelegateNotificationReceiver;

/// Maximum iterations when handling contract requests to prevent infinite loops
const MAX_CONTRACT_REQUEST_ITERATIONS: usize = 100;

/// Handle a delegate request, including any contract request messages in the response.
///
/// When a delegate emits contract request messages, this function:
/// 1. For GET: Fetches the contract state and sends GetContractResponse back
/// 2. For PUT: Upserts the contract state via `upsert_contract_state` (which automatically
///    propagates to the network via `BroadcastStateChange`), sends PutContractResponse back
/// 3. For UPDATE: Applies a state update via `upsert_contract_state` (same propagation),
///    sends UpdateContractResponse back
/// 4. For SUBSCRIBE: Registers in subscription registry, sends SubscribeContractResponse back
/// 5. Repeats until no more contract request messages
///
/// Returns the final response with contract request messages filtered out.
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
        DelegateRequest::RegisterDelegate { .. } | DelegateRequest::UnregisterDelegate(_) | _ => {
            Parameters::from(Vec::new())
        }
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
                "Exceeded maximum contract request iterations, possible infinite loop"
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

        // Check for contract request messages (GET, PUT, UPDATE, SUBSCRIBE) and delegate messages
        let mut get_requests: Vec<GetContractRequest> = Vec::new();
        let mut put_requests: Vec<PutContractRequest> = Vec::new();
        let mut update_requests: Vec<UpdateContractRequest> = Vec::new();
        let mut subscribe_requests: Vec<SubscribeContractRequest> = Vec::new();
        let mut delegate_messages: Vec<DelegateMessage> = Vec::new();

        for msg in values {
            match msg {
                OutboundDelegateMsg::GetContractRequest(req) => {
                    get_requests.push(req);
                }
                OutboundDelegateMsg::PutContractRequest(req) => {
                    put_requests.push(req);
                }
                OutboundDelegateMsg::UpdateContractRequest(req) => {
                    update_requests.push(req);
                }
                OutboundDelegateMsg::SubscribeContractRequest(req) => {
                    subscribe_requests.push(req);
                }
                OutboundDelegateMsg::SendDelegateMessage(msg) => {
                    delegate_messages.push(msg);
                }
                other @ OutboundDelegateMsg::ApplicationMessage(_)
                | other @ OutboundDelegateMsg::RequestUserInput(_)
                | other @ OutboundDelegateMsg::ContextUpdated(_) => {
                    // Accumulate non-contract-request messages
                    accumulated_messages.push(other);
                }
            }
        }

        // If no contract requests and no delegate messages, we're done
        if get_requests.is_empty()
            && put_requests.is_empty()
            && update_requests.is_empty()
            && subscribe_requests.is_empty()
            && delegate_messages.is_empty()
        {
            return accumulated_messages;
        }

        let mut inbound_responses: Vec<InboundDelegateMsg<'static>> = Vec::new();

        // Process PUT requests (fire-and-forget: upsert state, send result back).
        // This calls upsert_contract_state which stores locally AND automatically
        // propagates to the network via BroadcastStateChange — the same mechanism
        // used by normal client PUT operations (ContractHandlerEvent::PutQuery).
        if !put_requests.is_empty() {
            tracing::debug!(
                delegate_key = %delegate_key,
                count = put_requests.len(),
                "Processing PutContractRequest messages from delegate"
            );

            for req in put_requests {
                let contract_key = req.contract.key();
                let context = req.context;

                let result = contract_handler
                    .executor()
                    .upsert_contract_state(
                        contract_key,
                        Either::Left(req.state),
                        req.related_contracts,
                        Some(req.contract),
                    )
                    .await;

                let put_result = match result {
                    Ok(_) => Ok(()),
                    Err(err) => {
                        tracing::warn!(
                            contract = %contract_key,
                            error = %err,
                            "Failed to upsert contract for delegate PutContractRequest"
                        );
                        Err(format!("{err}"))
                    }
                };

                inbound_responses.push(InboundDelegateMsg::PutContractResponse(
                    PutContractResponse {
                        contract_id: *contract_key.id(),
                        result: put_result,
                        context,
                    },
                ));
            }
        }

        // Process GET requests
        if !get_requests.is_empty() {
            tracing::debug!(
                delegate_key = %delegate_key,
                count = get_requests.len(),
                "Processing GetContractRequest messages from delegate"
            );

            for req in get_requests {
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
        }

        // Process UPDATE requests (fire-and-forget: apply update, send result back).
        // Like PUT, this calls upsert_contract_state which propagates to the network
        // automatically via BroadcastStateChange when the state actually changes.
        // Only UpdateData::State and UpdateData::Delta are supported; compound variants
        // (StateAndDelta, Related*) are rejected because the delegate API doesn't
        // currently provide the related contract information needed to resolve them.
        if !update_requests.is_empty() {
            tracing::debug!(
                delegate_key = %delegate_key,
                count = update_requests.len(),
                "Processing UpdateContractRequest messages from delegate"
            );

            for req in update_requests {
                let contract_id = req.contract_id;
                let context = req.context;

                // Look up the full key from the instance id
                let result = match contract_handler.executor().lookup_key(&contract_id) {
                    Some(full_key) => {
                        let update_value: Either<WrappedState, StateDelta<'static>> = match req
                            .update
                        {
                            freenet_stdlib::prelude::UpdateData::State(state) => {
                                Either::Left(WrappedState::from(state.into_bytes()))
                            }
                            freenet_stdlib::prelude::UpdateData::Delta(delta) => {
                                Either::Right(delta)
                            }
                            // StateAndDelta, RelatedState, RelatedDelta, RelatedStateAndDelta
                            // are not supported because the delegate API doesn't provide the
                            // related contract context needed to resolve them.
                            other @ freenet_stdlib::prelude::UpdateData::StateAndDelta {
                                ..
                            }
                            | other @ freenet_stdlib::prelude::UpdateData::RelatedState { .. }
                            | other @ freenet_stdlib::prelude::UpdateData::RelatedDelta { .. }
                            | other @ freenet_stdlib::prelude::UpdateData::RelatedStateAndDelta {
                                ..
                            } => {
                                tracing::warn!(
                                    contract = %contract_id,
                                    variant = ?std::mem::discriminant(&other),
                                    "Unsupported UpdateData variant in delegate UpdateContractRequest \
                                     (only State and Delta are supported)"
                                );
                                inbound_responses.push(InboundDelegateMsg::UpdateContractResponse(
                                    UpdateContractResponse {
                                        contract_id,
                                        result: Err("Unsupported UpdateData variant".to_string()),
                                        context,
                                    },
                                ));
                                continue;
                            }
                        };

                        contract_handler
                            .executor()
                            .upsert_contract_state(
                                full_key,
                                update_value,
                                RelatedContracts::default(),
                                None,
                            )
                            .await
                    }
                    None => {
                        tracing::debug!(
                            contract = %contract_id,
                            "Contract not found locally for delegate UpdateContractRequest"
                        );
                        inbound_responses.push(InboundDelegateMsg::UpdateContractResponse(
                            UpdateContractResponse {
                                contract_id,
                                result: Err("Contract not found".to_string()),
                                context,
                            },
                        ));
                        continue;
                    }
                };

                let update_result = match result {
                    Ok(_) => Ok(()),
                    Err(err) => {
                        tracing::warn!(
                            contract = %contract_id,
                            error = %err,
                            "Failed to update contract for delegate UpdateContractRequest"
                        );
                        Err(format!("{err}"))
                    }
                };

                inbound_responses.push(InboundDelegateMsg::UpdateContractResponse(
                    UpdateContractResponse {
                        contract_id,
                        result: update_result,
                        context,
                    },
                ));
            }
        }

        // Process SUBSCRIBE requests
        // There are two registration paths that converge on DELEGATE_SUBSCRIPTIONS:
        // 1. V2 delegates: subscribe_contract() host function (native_api.rs) registers
        //    during WASM execution and returns success/error synchronously.
        // 2. V1 delegates: emit SubscribeContractRequest in process() outbound, handled here.
        // Both paths are idempotent — inserting the same (contract_id, delegate_key) twice
        // is a no-op on the HashSet. After registration, the delegate receives
        // ContractNotification messages when the subscribed contract's state changes.
        //
        // TODO(#2830): UnsubscribeContractRequest is not yet handled. Delegates can
        // only unsubscribe implicitly via UnregisterDelegate cleanup.
        if !subscribe_requests.is_empty() {
            tracing::debug!(
                delegate_key = %delegate_key,
                count = subscribe_requests.len(),
                "Processing SubscribeContractRequest messages from delegate"
            );

            for req in subscribe_requests {
                let contract_id = req.contract_id;
                let context = req.context;

                // Validate contract existence before registering (matches V2 host function behavior)
                let result = if contract_handler
                    .executor()
                    .lookup_key(&contract_id)
                    .is_some()
                {
                    // Register subscription in the global registry
                    crate::wasm_runtime::DELEGATE_SUBSCRIPTIONS
                        .entry(contract_id)
                        .or_default()
                        .insert(delegate_key.clone());
                    Ok(())
                } else {
                    tracing::debug!(
                        contract = %contract_id,
                        "Contract not found locally for delegate SubscribeContractRequest"
                    );
                    Err("Contract not found".to_string())
                };

                inbound_responses.push(InboundDelegateMsg::SubscribeContractResponse(
                    SubscribeContractResponse {
                        contract_id,
                        result,
                        context,
                    },
                ));
            }
        }

        // Deliver delegate-to-delegate messages (fire-and-forget).
        // Each message is delivered to the target delegate as an InboundDelegateMsg::DelegateMessage.
        // Any outbound messages from the target are accumulated alongside the sender's.
        if !delegate_messages.is_empty() {
            tracing::debug!(
                delegate_key = %delegate_key,
                count = delegate_messages.len(),
                "Delivering delegate-to-delegate messages"
            );

            for msg in delegate_messages {
                let target_key = msg.target.clone();
                let inbound = vec![InboundDelegateMsg::DelegateMessage(msg)];
                let target_req = DelegateRequest::ApplicationMessages {
                    key: target_key.clone(),
                    params: Parameters::from(Vec::new()),
                    inbound,
                };
                match contract_handler
                    .executor()
                    .execute_delegate_request(target_req, None)
                    .await
                {
                    Ok(freenet_stdlib::client_api::HostResponse::DelegateResponse {
                        values,
                        ..
                    }) => {
                        for value in values {
                            accumulated_messages.push(value);
                        }
                    }
                    Ok(_) => {}
                    Err(err) => {
                        tracing::warn!(
                            target_delegate = %target_key,
                            error = %err,
                            "Failed to deliver delegate message"
                        );
                    }
                }
            }
        }

        // Create a new request to send the responses back to the delegate
        current_req = DelegateRequest::ApplicationMessages {
            key: delegate_key.clone(),
            inbound: inbound_responses,
            params: current_params.clone(),
        };
    }
}

/// Receive a delegate notification from the optional channel.
/// Returns `std::future::pending()` if the channel is `None` or closed,
/// so the `tokio::select!` branch is effectively disabled.
async fn recv_delegate_notification(
    rx: &mut Option<DelegateNotificationReceiver>,
) -> executor::DelegateNotification {
    match rx {
        Some(rx) => match rx.recv().await {
            Some(n) => n,
            None => std::future::pending().await,
        },
        None => std::future::pending().await,
    }
}

pub(crate) async fn contract_handling<CH>(mut contract_handler: CH) -> Result<(), ContractError>
where
    CH: ContractHandler + Send + 'static,
{
    let mut delegate_rx = contract_handler.executor().take_delegate_notification_rx();

    loop {
        tokio::select! {
            result = contract_handler.channel().recv_from_sender() => {
                let (id, event) = result?;
                handle_contract_event(&mut contract_handler, id, event).await?;
            }
            notification = recv_delegate_notification(&mut delegate_rx) => {
                handle_delegate_notification(&mut contract_handler, notification).await;
            }
        }
    }
}

async fn handle_delegate_notification<CH>(
    contract_handler: &mut CH,
    notification: executor::DelegateNotification,
) where
    CH: ContractHandler + Send + 'static,
{
    let executor::DelegateNotification {
        delegate_key,
        contract_id,
        new_state,
    } = notification;

    tracing::debug!(
        delegate = %delegate_key,
        contract = %contract_id,
        "Delivering contract notification to delegate"
    );

    // Unwrap the Arc — if this is the last subscriber the state moves without cloning,
    // otherwise a clone is made (unavoidable since ContractNotification owns the state).
    let owned_state = Arc::try_unwrap(new_state).unwrap_or_else(|arc| (*arc).clone());

    let inbound = vec![InboundDelegateMsg::ContractNotification(
        ContractNotification {
            contract_id,
            new_state: owned_state,
            context: DelegateContext::default(),
        },
    )];

    let req = DelegateRequest::ApplicationMessages {
        key: delegate_key.clone(),
        params: Parameters::from(vec![]),
        inbound,
    };

    let outbound =
        handle_delegate_with_contract_requests(contract_handler, req, None, &delegate_key).await;

    // TODO-MUST-FIX: Route outbound ApplicationMessages to subscribed apps #3275
    // handle_delegate_with_contract_requests already processes contract requests
    // (GET/PUT/UPDATE/SUBSCRIBE) internally. The remaining outbound messages are
    // ApplicationMessages meant for connected apps, but notification-driven
    // invocations have no originating client connection to route them to.
    // When delegate-to-app notification routing is implemented, these should be
    // forwarded to all apps registered with this delegate.
    for msg in &outbound {
        match msg {
            OutboundDelegateMsg::ApplicationMessage(app_msg) => {
                tracing::warn!(
                    delegate = %delegate_key,
                    app = %app_msg.app,
                    payload_len = app_msg.payload.len(),
                    "Delegate produced ApplicationMessage from contract notification \
                     but no client routing is available yet — message dropped"
                );
            }
            OutboundDelegateMsg::RequestUserInput(_)
            | OutboundDelegateMsg::ContextUpdated(_)
            | OutboundDelegateMsg::GetContractRequest(_)
            | OutboundDelegateMsg::PutContractRequest(_)
            | OutboundDelegateMsg::UpdateContractRequest(_)
            | OutboundDelegateMsg::SubscribeContractRequest(_)
            | OutboundDelegateMsg::SendDelegateMessage(_) => {
                tracing::warn!(
                    delegate = %delegate_key,
                    msg_type = ?std::mem::discriminant(msg),
                    "Delegate produced unexpected outbound message from contract notification — dropped"
                );
            }
        }
    }
}

async fn handle_contract_event<CH>(
    contract_handler: &mut CH,
    id: handler::EventId,
    event: ContractHandlerEvent,
) -> Result<(), ContractError>
where
    CH: ContractHandler + Send + 'static,
{
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
                                return Err(ContractError::FatalExecutorError { key, error: err });
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
                freenet_stdlib::prelude::UpdateData::StateAndDelta { .. }
                | freenet_stdlib::prelude::UpdateData::RelatedState { .. }
                | freenet_stdlib::prelude::UpdateData::RelatedDelta { .. }
                | freenet_stdlib::prelude::UpdateData::RelatedStateAndDelta { .. } => {
                    unreachable!()
                }
            };
            let update_result = contract_handler
                .executor()
                .upsert_contract_state(key, update_value, related_contracts, None)
                .instrument(tracing::info_span!("upsert_contract_state", %key))
                .await;

            let event_result = match update_result {
                Ok(UpsertResult::NoChange) => {
                    tracing::debug!(
                        contract = %key,
                        phase = "update_no_change",
                        "UPDATE resulted in NoChange, fetching current state to return UpdateResponse"
                    );
                    // When there's no change, we still need to return the current state
                    // so the client gets a proper response
                    match contract_handler.executor().fetch_contract(key, false).await {
                        Ok((Some(current_state), _)) => {
                            tracing::debug!(
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
                contract_handler,
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
            if let Err(err) = contract_handler.executor().register_contract_notifier(
                key,
                client_id,
                subscriber_listener,
                summary,
            ) {
                tracing::warn!(
                    contract = %key,
                    client = %client_id,
                    error = %err,
                    phase = "registration_failed",
                    "Error registering subscriber listener"
                );
            }

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
        ContractHandlerEvent::NotifySubscriptionError { key, reason } => {
            contract_handler
                .executor()
                .notify_subscription_error(key, reason);
            if let Err(error) = contract_handler
                .channel()
                .send_to_sender(id, ContractHandlerEvent::NotifySubscriptionErrorResponse)
                .await
            {
                tracing::debug!(
                    error = %error,
                    contract = %key,
                    "Failed to send NotifySubscriptionError response"
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
            if let Err(e) = callback
                .send(crate::message::QueryResult::NetworkDebug(network_debug))
                .await
            {
                tracing::debug!(error = %e, "failed to send network debug info via callback");
            }

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
        ContractHandlerEvent::ClientDisconnect { client_id } => {
            contract_handler.executor().remove_client(client_id);
            contract_handler.channel().drop_waiting_response(id);
        }
        ContractHandlerEvent::DelegateResponse(_)
        | ContractHandlerEvent::PutResponse { .. }
        | ContractHandlerEvent::GetResponse { .. }
        | ContractHandlerEvent::UpdateResponse { .. }
        | ContractHandlerEvent::UpdateNoChange { .. }
        | ContractHandlerEvent::RegisterSubscriberListenerResponse
        | ContractHandlerEvent::QuerySubscriptionsResponse
        | ContractHandlerEvent::GetSummaryResponse { .. }
        | ContractHandlerEvent::GetDeltaResponse { .. }
        | ContractHandlerEvent::NotifySubscriptionErrorResponse => {
            unreachable!("response events should not be received by the handler")
        }
    }
    Ok(())
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
