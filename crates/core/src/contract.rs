//! Handling of contracts and delegates, including storage, execution, caching, etc.
//!
//! Internally uses the wasm_runtime module to execute contract and/or delegate instructions.

use std::sync::Arc;

use either::Either;
use freenet_stdlib::prelude::*;

mod executor;
mod fair_queue;
pub(crate) mod governance;
mod handler;
pub mod storages;
pub(crate) mod user_input;

pub(crate) use executor::{
    ContractExecutor, ExecutorTransactionStream, MAX_CREATED_DELEGATES_PER_NODE,
    MAX_DELEGATE_CREATION_DEPTH, MAX_DELEGATE_CREATIONS_PER_CALL,
    SUBSCRIBER_NOTIFICATION_CHANNEL_SIZE, UpsertOutcome, UpsertResult, mock_runtime::MockRuntime,
};

// Re-export CRDT emulation functions for testing
pub use executor::mock_runtime::{clear_crdt_contracts, is_crdt_contract, register_crdt_contract};
pub(crate) use handler::{
    ClientResponsesReceiver, ClientResponsesSender, ContractHandler, ContractHandlerChannel,
    ContractHandlerEvent, NetworkContractHandler, SenderHalve, SessionMessage, StashedResponder,
    StoreResponse, WaitingResolution, WaitingTransaction, client_responses_channel,
    contract_handler_channel,
    in_memory::{
        MemoryContractHandler, MockWasmContractHandler, MockWasmHandlerBuilder,
        SimulationContractHandler, SimulationHandlerBuilder,
    },
};

pub use executor::{ContractQueueFull, Executor, ExecutorError, OperationMode};
pub use handler::reset_event_id_counter;

use freenet_stdlib::client_api::DelegateRequest;
use tracing::Instrument;

use self::executor::DelegateNotificationReceiver;
use self::user_input::{CallerIdentity, UserInputPrompter};
use crate::config::GlobalExecutor;

/// Maximum iterations when handling contract requests to prevent infinite loops
const MAX_CONTRACT_REQUEST_ITERATIONS: usize = 100;

/// Maximum delegate notifications to drain per iteration.
/// Matches the same bounded-drain pattern as MAX_DRAIN_BATCH for contract events,
/// preventing delegate notification channel growth under sustained contract load.
const MAX_DELEGATE_DRAIN_BATCH: usize = 16;

/// Unified timeout for the off-loop related-contract fetch that backs a
/// deferred PUT/UPDATE (#4391).
///
/// This reconciles the two legacy inline timeouts (`RELATED_FETCH_TIMEOUT`=10s
/// for validate-side fetches, `SUB_OP_FETCH_TIMEOUT`=120s in
/// `local_state_or_from_network`): the 120s value only existed because the
/// wait sat on the serial loop and we did not want to fail a slow cross-node
/// fetch prematurely. Now that the wait is OFF the loop, the loop keeps
/// draining other events regardless, so we apply the tighter
/// `RELATED_FETCH_TIMEOUT` consistently — a deferred op that cannot resolve
/// its related contracts within 10s surfaces `MissingRelated` to the client,
/// exactly as the inline validate path did.
const DEFERRED_RELATED_FETCH_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(10);

/// Maximum number of PUT/UPDATE operations that may have an in-flight
/// off-loop related-contract fetch at once.
///
/// Bounds the number of concurrently spawned waiter tasks so a flood of
/// related-needing PUTs cannot spawn unbounded background work. When the cap
/// is reached, a new related-needing op falls back to surfacing
/// `MissingRelated` to the client rather than deferring — backpressure, not
/// unbounded growth (see code-style.md "per-key/per-client caps").
const MAX_INFLIGHT_DEFERRALS: usize = 256;

/// A PUT/UPDATE that deferred its related-contract fetch off the serial loop,
/// carrying everything needed to re-run the upsert once the fetch resolves.
///
/// Built by the off-loop waiter task and sent back to the `contract_handling`
/// loop via the resume channel; the loop re-runs the upsert ON the loop (so
/// WASM stays serial) with `fetched` supplied, then answers the stashed client
/// responder. See issue #4391.
struct DeferredResume {
    /// Key into the loop's stashed-responder map (and the inflight set).
    deferral_id: u64,
    /// The contract being PUT/UPDATEd.
    key: ContractKey,
    /// The original update payload (full state for PUT, delta/state for UPDATE).
    update: Either<WrappedState, StateDelta<'static>>,
    /// Caller-supplied related contracts from the original event.
    related_contracts: RelatedContracts<'static>,
    /// Contract code (Some for PUT, None for UPDATE).
    code: Option<ContractContainer>,
    /// `true` for PUT (build `PutResponse`), `false` for UPDATE
    /// (build `UpdateResponse`).
    is_put: bool,
    /// The states fetched off-loop, or `Err` if the fetch failed/timed out
    /// (→ resume surfaces `MissingRelated`).
    fetched: Result<Vec<(ContractInstanceId, WrappedState)>, ExecutorError>,
}

/// Loop-side state for off-loading deferred related-contract fetches (#4391).
///
/// Owned by `contract_handling`; passed by `&mut` into `handle_contract_event`
/// so the PUT/UPDATE arms can defer. When this context is absent (e.g. the
/// delegate-driven PUT path, or direct unit-test calls to
/// `handle_contract_event`), the arms keep the legacy inline-fetch behavior.
struct DeferralCtx {
    /// Off-loop waiters send completed [`DeferredResume`]s back to the loop here.
    resume_tx: tokio::sync::mpsc::UnboundedSender<DeferredResume>,
    /// Client responders parked while their op's related fetch runs off-loop,
    /// keyed by `deferral_id`. Drained by the loop when the resume arrives.
    stashed: std::collections::HashMap<u64, StashedResponder>,
    /// Monotonic id generator for `deferral_id`.
    next_deferral_id: u64,
}

impl DeferralCtx {
    fn new(resume_tx: tokio::sync::mpsc::UnboundedSender<DeferredResume>) -> Self {
        Self {
            resume_tx,
            stashed: std::collections::HashMap::new(),
            next_deferral_id: 0,
        }
    }

    /// `true` when the in-flight deferral cap is reached and a new op must NOT
    /// defer (it falls back to surfacing the related-fetch failure inline).
    fn at_capacity(&self) -> bool {
        self.stashed.len() >= MAX_INFLIGHT_DEFERRALS
    }
}

#[cfg(test)]
pub(crate) type OffLoopFetchStub = Arc<
    dyn Fn(
            Vec<ContractInstanceId>,
        ) -> std::pin::Pin<
            Box<
                dyn std::future::Future<
                        Output = Result<Vec<(ContractInstanceId, WrappedState)>, ExecutorError>,
                    > + Send,
            >,
        > + Send
        + Sync,
>;

#[cfg(test)]
static OFF_LOOP_FETCH_OVERRIDE: std::sync::Mutex<Option<OffLoopFetchStub>> =
    std::sync::Mutex::new(None);

/// Install (or clear) the test override for [`fetch_related_off_loop`].
///
/// Unlike the executor's `TEST_NETWORK_FETCH_OVERRIDE` (a thread-local, sync
/// stub), this override is `Arc`-based and async so a test running on a
/// multi-thread runtime can make the off-loop fetch *hang* until the test
/// releases it — the deterministic way to reproduce the head-of-line stall
/// the fix removes (#4391).
#[cfg(test)]
pub(crate) fn set_off_loop_fetch_override(stub: Option<OffLoopFetchStub>) {
    *OFF_LOOP_FETCH_OVERRIDE.lock().unwrap() = stub;
}

/// Drive the related-contract GET(s) for a deferred upsert OFF the serial
/// `contract_handling` loop.
///
/// Each missing id races its own background sub-op GET (`start_sub_op_get`
/// already spawns the GET on its own task); we await the oneshots under a
/// single [`DEFERRED_RELATED_FETCH_TIMEOUT`] budget. Any miss/timeout/infra
/// error fails the whole fetch with `MissingRelated`, mirroring the inline
/// validate path's all-or-nothing semantics. Runs in a `GlobalExecutor::spawn`
/// task; nothing here touches the executor or the serial loop.
async fn fetch_related_off_loop(
    op_manager: Option<Arc<crate::node::OpManager>>,
    missing: Vec<ContractInstanceId>,
) -> Result<Vec<(ContractInstanceId, WrappedState)>, ExecutorError> {
    #[cfg(test)]
    {
        let stub = OFF_LOOP_FETCH_OVERRIDE.lock().unwrap().clone();
        if let Some(stub) = stub {
            return stub(missing).await;
        }
    }

    let Some(op_manager) = op_manager else {
        // No network available (local-only executor): surface the first
        // missing id as MissingRelated, matching the legacy local-only path.
        let id = missing
            .first()
            .copied()
            .unwrap_or_else(|| ContractInstanceId::new([0u8; 32]));
        return Err(ExecutorError::missing_related(id));
    };

    let fetch_all = async {
        let results: Vec<(ContractInstanceId, Result<WrappedState, ExecutorError>)> =
            futures::future::join_all(missing.iter().map(|id| {
                let id = *id;
                let op_manager = op_manager.clone();
                async move {
                    let (_tx, rx) = crate::operations::get::op_ctx_task::start_sub_op_get(
                        &op_manager,
                        id,
                        false,
                    );
                    let outcome = match rx.await {
                        Ok(outcome) => outcome,
                        Err(_) => return (id, Err(ExecutorError::missing_related(id))),
                    };
                    let mapped = match outcome {
                        crate::operations::get::op_ctx_task::SubOpGetOutcome::Found(get_result) => {
                            Ok(WrappedState::from(get_result.state.as_ref().to_vec()))
                        }
                        crate::operations::get::op_ctx_task::SubOpGetOutcome::NotFound(_)
                        | crate::operations::get::op_ctx_task::SubOpGetOutcome::Infra(_) => {
                            Err(ExecutorError::missing_related(id))
                        }
                    };
                    (id, mapped)
                }
            }))
            .await;
        let mut fetched = Vec::with_capacity(results.len());
        for (id, res) in results {
            fetched.push((id, res?));
        }
        Ok::<_, ExecutorError>(fetched)
    };

    match tokio::time::timeout(DEFERRED_RELATED_FETCH_TIMEOUT, fetch_all).await {
        Ok(result) => result,
        Err(_) => {
            let id = missing
                .first()
                .copied()
                .unwrap_or_else(|| ContractInstanceId::new([0u8; 32]));
            Err(ExecutorError::missing_related(id))
        }
    }
}

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
async fn handle_delegate_with_contract_requests<CH, P>(
    contract_handler: &mut CH,
    initial_req: DelegateRequest<'static>,
    origin_contract: Option<&ContractInstanceId>,
    delegate_key: &DelegateKey,
    prompter: &P,
) -> Vec<OutboundDelegateMsg>
where
    CH: ContractHandler + Send + 'static,
    P: UserInputPrompter,
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
            .execute_delegate_request(current_req, origin_contract, None)
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
                // Downgrade "not found" to warn — expected during legacy
                // migration probes when old delegate WASM isn't on this node
                if err.is_missing_delegate() {
                    tracing::warn!(
                        delegate_key = %delegate_key,
                        "Delegate not found in store (expected for migration probes)"
                    );
                } else {
                    tracing::error!(
                        delegate_key = %delegate_key,
                        error = %err,
                        phase = "execution_failed",
                        "Failed executing delegate request"
                    );
                }
                // Return whatever we accumulated so far
                return accumulated_messages;
            }
        };

        // Check for contract request messages (GET, PUT, UPDATE, SUBSCRIBE), delegate messages,
        // and user input requests
        let mut get_requests: Vec<GetContractRequest> = Vec::new();
        let mut put_requests: Vec<PutContractRequest> = Vec::new();
        let mut update_requests: Vec<UpdateContractRequest> = Vec::new();
        let mut subscribe_requests: Vec<SubscribeContractRequest> = Vec::new();
        let mut delegate_messages: Vec<DelegateMessage> = Vec::new();
        let mut user_input_requests: Vec<UserInputRequest<'static>> = Vec::new();

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
                OutboundDelegateMsg::RequestUserInput(req) => {
                    user_input_requests.push(req);
                }
                other @ OutboundDelegateMsg::ApplicationMessage(_)
                | other @ OutboundDelegateMsg::ContextUpdated(_) => {
                    // Accumulate non-request messages
                    accumulated_messages.push(other);
                }
            }
        }

        // If no contract requests, delegate messages, or user input requests, we're done
        if get_requests.is_empty()
            && put_requests.is_empty()
            && update_requests.is_empty()
            && subscribe_requests.is_empty()
            && delegate_messages.is_empty()
            && user_input_requests.is_empty()
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
                            // related contract context needed to resolve them. Future
                            // `UpdateData` variants (the enum is `#[non_exhaustive]` since
                            // stdlib 0.6.0) fall through the same "unsupported" branch and
                            // are similarly rejected with a warn-level log.
                            other @ freenet_stdlib::prelude::UpdateData::StateAndDelta {
                                ..
                            }
                            | other @ freenet_stdlib::prelude::UpdateData::RelatedState { .. }
                            | other @ freenet_stdlib::prelude::UpdateData::RelatedDelta { .. }
                            | other @ freenet_stdlib::prelude::UpdateData::RelatedStateAndDelta {
                                ..
                            }
                            | other => {
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

        // Deliver delegate-to-delegate messages (fire-and-forget, single-hop).
        //
        // Design notes:
        // - Delivery is intentionally single-hop: if target delegate B emits its own
        //   SendDelegateMessage in response, it is filtered out (not delivered). This
        //   prevents infinite recursion between delegates. We use execute_delegate_request
        //   (not the full delivery loop) for the same reason.
        // - Target delegate receives empty params because params are not stored in the
        //   delegate registry — they are passed per-request at the API layer. If a target
        //   delegate's process() relies on params, the caller must use ApplicationMessages
        //   directly. This is a known v1 limitation.
        // - The caller's delegate key is passed as `caller_delegate` so the receiver
        //   sees `MessageOrigin::Delegate(caller_key)` and can authorize on it
        //   (issue #3860). The runtime attests this identity — the calling delegate
        //   cannot forge it.
        // - Inter-delegate messaging only works via ApplicationMessages path; messages
        //   from handle_delegate_notification (contract state change callbacks) do not
        //   trigger delegate-to-delegate delivery.
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
                    .execute_delegate_request(target_req, None, Some(delegate_key))
                    .await
                {
                    Ok(freenet_stdlib::client_api::HostResponse::DelegateResponse {
                        values,
                        ..
                    }) => {
                        // Filter out SendDelegateMessage from target's output to enforce
                        // single-hop delivery. Only accumulate client-visible messages.
                        for value in values {
                            if !matches!(value, OutboundDelegateMsg::SendDelegateMessage(_)) {
                                accumulated_messages.push(value);
                            }
                        }
                    }
                    Ok(_) => {}
                    Err(err) => {
                        tracing::warn!(
                            target_delegate = %target_key,
                            error = %err,
                            "Failed to deliver delegate message (fire-and-forget)"
                        );
                    }
                }
            }
        }

        // Process UserInput requests: prompt the user and send responses back
        if !user_input_requests.is_empty() {
            tracing::debug!(
                delegate_key = %delegate_key,
                count = user_input_requests.len(),
                "Processing UserInputRequest messages from delegate"
            );

            // The caller identity passed to the prompter is built from the
            // executor's runtime context, NOT from anything the delegate could
            // influence — so a malicious delegate cannot spoof another app's
            // or delegate's identity in the structured fields the prompt UI
            // renders. Today the only attested non-None caller is a web app
            // (via `MessageOrigin::WebApp`); delegate-to-delegate attestation
            // is tracked by #3860 and will appear here as a new variant.
            //
            // The actual mapping lives in `caller_identity_from_origin` so it
            // can be unit-tested independently of the executor plumbing.
            let caller = caller_identity_from_origin(origin_contract);
            let delegate_key_str = delegate_key.to_string();

            for req in user_input_requests {
                let request_id = req.request_id;
                let response = match prompter
                    .prompt(&req, &delegate_key_str, caller.clone())
                    .await
                {
                    Some((_, response)) => response,
                    None => {
                        tracing::warn!(
                            request_id,
                            delegate = %delegate_key,
                            "User input request timed out or was denied"
                        );
                        // Send an empty response so the delegate knows the request
                        // was denied/timed out, rather than leaving it waiting forever.
                        ClientResponse::new(Vec::new())
                    }
                };
                inbound_responses.push(InboundDelegateMsg::UserResponse(UserInputResponse {
                    request_id,
                    response,
                    // UserInputRequest has no context field, so we use default.
                    // The delegate's actual context is maintained separately in
                    // the process_outbound loop in delegate.rs.
                    context: DelegateContext::default(),
                }));
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

/// Try to receive a delegate notification without blocking.
/// Returns `None` if the channel is `None`, closed, or has no pending notifications.
fn try_recv_delegate_notification(
    rx: &mut Option<DelegateNotificationReceiver>,
) -> Option<executor::DelegateNotification> {
    match rx {
        Some(rx) => rx.try_recv().ok(),
        None => None,
    }
}

/// Build a `CallerIdentity` for the permission prompt UI from the executor's
/// runtime origin context (see #3857).
///
/// The mapping is intentionally narrow: today the only attested non-`None`
/// caller is a web app via `MessageOrigin::WebApp(..)`. Every other path
/// (delegate-to-delegate, local client, missing attestation) collapses to
/// `CallerIdentity::None` and renders as `"No app caller"` in the UI.
/// Issue #3860 tracks adding a `MessageOrigin::Delegate(DelegateKey)` variant
/// and the corresponding `CallerIdentity::Delegate` row; until that lands,
/// inter-delegate calls are not distinguishable here from "no caller at all".
///
/// Extracted as a free function (rather than inlined at the call site) so
/// the mapping can be unit-tested in isolation. Without this, swapping the
/// `Some` and `None` arms — or accidentally wiring a delegate-controlled
/// value into the prompter — would not fail any test.
fn caller_identity_from_origin(origin: Option<&ContractInstanceId>) -> CallerIdentity {
    match origin {
        Some(id) => CallerIdentity::WebApp(id.to_string()),
        None => CallerIdentity::None,
    }
}

/// Inject a related contract's full state into `related_contracts` for
/// downstream propagation to the contract WASM as a `RelatedState` entry.
///
/// `RelatedContracts` exposes no public `insert`, so we use the documented
/// `missing()` + `update()` pattern: `missing()` registers the id with a
/// `None` slot, and `update()` returns a mutable iterator we can use to set
/// the slot. The slot is guaranteed to exist after `missing()`; if a caller
/// previously supplied a state for the same id via the bare
/// `related_contracts` argument on `UpdateQuery`, we overwrite it — the
/// inline `RelatedStateAndDelta` payload is the atomic package the sender
/// committed to and is preferred over any out-of-band hint.
///
/// Extracted as a free function so the conversion can be unit-tested in
/// isolation. Without that, regressions in `RelatedContracts::missing()` or
/// `update()` semantics — for instance if `missing()` ever stopped
/// guaranteeing the slot exists — would silently drop the inline state and
/// surface only as missing-message bugs in higher-level integration tests.
fn inject_related_state(
    related_contracts: &mut RelatedContracts<'static>,
    related_to: ContractInstanceId,
    state: freenet_stdlib::prelude::State<'static>,
) {
    related_contracts.missing(vec![related_to]);
    let mut state_holder = Some(state);
    for (id, slot) in related_contracts.update() {
        if id == &related_to {
            *slot = state_holder.take();
            break;
        }
    }
    debug_assert!(
        state_holder.is_none(),
        "RelatedContracts::missing() must register the slot it just declared, \
         so update() should always have a slot for the requested id"
    );
}

pub(crate) async fn contract_handling<CH, P>(
    mut contract_handler: CH,
    prompter: P,
) -> Result<(), ContractError>
where
    CH: ContractHandler + Send + 'static,
    P: UserInputPrompter,
{
    let mut delegate_rx = contract_handler.executor().take_delegate_notification_rx();
    let mut fair_queue = fair_queue::FairEventQueue::new();

    // Resume channel for PUT/UPDATE operations whose related-contract fetch was
    // off-loaded from this serial loop (#4391). Off-loop waiter tasks send the
    // fetched states back here; the loop re-runs the upsert ON the loop so WASM
    // stays serial. Unbounded is acceptable per channel-safety.md's carve-out:
    // the producer count is bounded by MAX_INFLIGHT_DEFERRALS, the receiver is
    // this loop (which drains every iteration), and there is no cycle (the
    // waiter never reads what the loop produces). The waiter sends with a
    // non-blocking `unbounded_send` (no `.await`), so it can never back-pressure
    // anything.
    let (resume_tx, mut resume_rx) = tokio::sync::mpsc::unbounded_channel::<DeferredResume>();
    let mut deferral_ctx = DeferralCtx::new(resume_tx);

    loop {
        // Drain resumed (deferred) upserts FIRST so a completed off-loop fetch
        // is applied promptly and its client answered, ahead of new queued work.
        // Bounded by MAX_INFLIGHT_DEFERRALS (no resume can enqueue another).
        while let Ok(resume) = resume_rx.try_recv() {
            handle_deferred_resume(&mut contract_handler, &mut deferral_ctx, resume).await?;
        }

        // Drain pending events from the channel into the fair queue (bounded batch).
        // Capped at MAX_DRAIN_BATCH (256) to bound the synchronous work per iteration
        // before yielding back to the Tokio scheduler. Each iteration of this loop is
        // a non-blocking try_recv + HashMap insert, so 256 iterations complete in
        // single-digit microseconds even at peak load.
        for _ in 0..fair_queue::MAX_DRAIN_BATCH {
            match contract_handler.channel().try_recv_from_sender()? {
                Some((id, event)) => {
                    // Process ClientDisconnect inline — it's a lightweight cleanup
                    // operation that should never be delayed or compete with
                    // DelegateRequest events for the default queue's limited capacity.
                    if let ContractHandlerEvent::ClientDisconnect { client_id } = &event {
                        let client_id = *client_id;
                        contract_handler.executor().remove_client(client_id);
                        contract_handler.channel().drop_waiting_response(id);
                        continue;
                    }
                    if let Err(rejected) = fair_queue.try_push(id, event) {
                        track_pending_reclamation_if_evict(&mut contract_handler, &rejected);
                        send_queue_full_response(contract_handler.channel(), rejected).await;
                    }
                }
                None => break,
            }
        }

        // Drain delegate notifications (non-blocking, bounded) even when the fair queue
        // has work. This prevents delegate starvation under sustained contract load.
        // We drain up to MAX_DELEGATE_DRAIN_BATCH to handle bursts (e.g., a contract
        // update triggering many delegate notifications simultaneously).
        for _ in 0..MAX_DELEGATE_DRAIN_BATCH {
            match try_recv_delegate_notification(&mut delegate_rx) {
                Some(notification) => {
                    handle_delegate_notification(&mut contract_handler, notification, &prompter)
                        .await;
                }
                None => break,
            }
        }

        // Process one event from the fair queue (round-robin across contracts)
        if let Some((id, event)) = fair_queue.pop() {
            handle_contract_event(
                &mut contract_handler,
                id,
                event,
                &prompter,
                Some(&mut deferral_ctx),
            )
            .await?;
            continue;
        }

        // Fair queue is empty — block-wait for a new event, a resumed deferral,
        // or a delegate notification.
        tokio::select! {
            result = contract_handler.channel().recv_from_sender() => {
                let (id, event) = result?;
                if let Err(rejected) = fair_queue.try_push(id, event) {
                    track_pending_reclamation_if_evict(&mut contract_handler, &rejected);
                    send_queue_full_response(contract_handler.channel(), rejected).await;
                }
            }
            Some(resume) = resume_rx.recv() => {
                handle_deferred_resume(&mut contract_handler, &mut deferral_ctx, resume).await?;
            }
            notification = recv_delegate_notification(&mut delegate_rx) => {
                handle_delegate_notification(&mut contract_handler, notification, &prompter).await;
            }
        }
    }
}

/// Re-run a deferred PUT/UPDATE upsert ON the serial loop once its related
/// contracts have been fetched off-loop, then answer the original client.
///
/// The fetched related states are injected into `related_contracts` so the
/// resumed upsert finds them locally and does NOT re-fetch. The upsert runs
/// via the plain (non-deferrable) `upsert_contract_state`, so if the contract
/// *still* requests a different related contract it cannot defer again — it
/// surfaces `MissingRelated` to the client (the depth=1 / one-deferral cap,
/// constraint of #4391).
async fn handle_deferred_resume<CH>(
    contract_handler: &mut CH,
    deferral_ctx: &mut DeferralCtx,
    resume: DeferredResume,
) -> Result<(), ContractError>
where
    CH: ContractHandler + Send + 'static,
{
    let DeferredResume {
        deferral_id,
        key,
        update,
        mut related_contracts,
        code,
        is_put,
        fetched,
    } = resume;

    // Reclaim the parked client responder. If it's gone the loop is shutting
    // down or the entry was already consumed — nothing to answer.
    let Some(responder) = deferral_ctx.stashed.remove(&deferral_id) else {
        tracing::debug!(contract = %key, "Deferred resume has no stashed responder; dropping");
        return Ok(());
    };

    let event_result = match fetched {
        Ok(states) => {
            // Inject the off-loop-fetched related states so the resumed upsert
            // resolves them locally (no second network round trip).
            for (id, state) in states {
                inject_related_state(
                    &mut related_contracts,
                    id,
                    freenet_stdlib::prelude::State::from(state.as_ref().to_vec()),
                );
            }
            // Preserve the original incoming full state (PUT only) for the
            // NoChange response, since `update` is consumed by the upsert.
            let incoming_state = match &update {
                Either::Left(state) => Some(state.clone()),
                Either::Right(_) => None,
            };
            // Re-run the upsert ON the loop in DEFERRABLE mode again. With the
            // related states supplied, validation/merge resolves locally and
            // returns `Completed`. The one-deferral cap (#4391): if the contract
            // now requests a DIFFERENT related contract not held locally, the
            // deferrable path returns `DeferRelated` again — we convert that to
            // MissingRelated rather than spawning a second off-loop fetch, so a
            // misbehaving contract cannot defer indefinitely and the network
            // wait never re-enters the loop.
            let result = match contract_handler
                .executor()
                .upsert_contract_state_deferrable(key, update, related_contracts, code)
                .instrument(tracing::info_span!("upsert_contract_state_resumed", %key))
                .await
            {
                Ok(UpsertOutcome::Completed(result)) => Ok(result),
                Ok(UpsertOutcome::DeferRelated(missing)) => {
                    tracing::debug!(
                        contract = %key,
                        missing = missing.len(),
                        "Resumed upsert still needs a related contract — surfacing \
                         MissingRelated (one-deferral cap, #4391)"
                    );
                    Err(ExecutorError::missing_related(
                        missing.first().copied().unwrap_or_else(|| *key.id()),
                    ))
                }
                Err(err) => Err(err),
            };
            if is_put {
                let incoming_state =
                    incoming_state.unwrap_or_else(|| WrappedState::new(Vec::new()));
                put_response_from_result(contract_handler, key, incoming_state, result).await?
            } else {
                update_response_from_result(contract_handler, key, result).await?
            }
        }
        Err(err) => {
            // Off-loop fetch failed/timed out: surface the related-fetch error
            // to the client (do NOT defer again).
            tracing::debug!(
                contract = %key,
                error = %err,
                "Deferred related-contract fetch failed; surfacing to client"
            );
            if is_put {
                ContractHandlerEvent::PutResponse {
                    new_value: Err(err),
                    state_changed: false,
                }
            } else {
                ContractHandlerEvent::UpdateResponse {
                    new_value: Err(err),
                    state_changed: false,
                }
            }
        }
    };

    if let Err(error) = responder.respond(event_result) {
        tracing::debug!(
            error = %error,
            contract = %key,
            "Failed to deliver deferred upsert response (client may have disconnected)"
        );
    }
    Ok(())
}

/// Result of attempting a deferrable upsert in the PUT/UPDATE arms.
enum DeferStep {
    /// The upsert ran to completion (no related fetch needed, or it was
    /// resolvable locally). Build the response from this result.
    Completed(Result<UpsertResult, ExecutorError>),
    /// The upsert deferred its related-contract fetch off-loop. The client
    /// responder has been parked; the arm must return immediately and let the
    /// resume path answer the client later.
    Deferred,
}

/// Run an upsert in deferrable mode when a [`DeferralCtx`] is available,
/// off-loading the related-contract network fetch from the serial loop (#4391).
///
/// When `deferral` is `None` (delegate-driven PUTs, direct unit tests) or the
/// in-flight deferral cap is reached, this falls back to the legacy inline
/// `upsert_contract_state` and always returns `Completed`.
#[allow(clippy::too_many_arguments)]
async fn maybe_defer_upsert<CH>(
    contract_handler: &mut CH,
    deferral: Option<&mut DeferralCtx>,
    id: &handler::EventId,
    key: ContractKey,
    update: Either<WrappedState, StateDelta<'static>>,
    related_contracts: RelatedContracts<'static>,
    code: Option<ContractContainer>,
    is_put: bool,
) -> DeferStep
where
    CH: ContractHandler + Send + 'static,
{
    // No deferral context (or at capacity) → behave exactly as the legacy
    // inline path: await the network related-fetch on this loop. This keeps
    // delegate-driven PUTs and direct unit-test calls unchanged, and provides
    // backpressure (rather than unbounded waiter tasks) under a deferral flood.
    let Some(deferral) = deferral else {
        return DeferStep::Completed(
            contract_handler
                .executor()
                .upsert_contract_state(key, update, related_contracts, code)
                .instrument(tracing::info_span!("upsert_contract_state", %key))
                .await,
        );
    };
    if deferral.at_capacity() {
        tracing::warn!(
            contract = %key,
            inflight = deferral.stashed.len(),
            limit = MAX_INFLIGHT_DEFERRALS,
            "Deferral capacity reached — falling back to inline related fetch"
        );
        return DeferStep::Completed(
            contract_handler
                .executor()
                .upsert_contract_state(key, update, related_contracts, code)
                .instrument(tracing::info_span!("upsert_contract_state", %key))
                .await,
        );
    }

    let outcome = contract_handler
        .executor()
        .upsert_contract_state_deferrable(
            key,
            update.clone(),
            related_contracts.clone(),
            code.clone(),
        )
        .instrument(tracing::info_span!("upsert_contract_state_deferrable", %key))
        .await;

    let missing = match outcome {
        Ok(UpsertOutcome::Completed(result)) => return DeferStep::Completed(Ok(result)),
        Ok(UpsertOutcome::DeferRelated(missing)) => missing,
        Err(err) => return DeferStep::Completed(Err(err)),
    };

    // Park the client responder so it survives the off-loop wait.
    let Some(responder) = contract_handler.channel().take_waiting_response(id) else {
        // Fire-and-forget event (no responder): nothing to answer, and there's
        // no client awaiting the resume. Fall back to the inline path so the
        // upsert's side effects (store/broadcast) still happen.
        return DeferStep::Completed(
            contract_handler
                .executor()
                .upsert_contract_state(key, update, related_contracts, code)
                .instrument(tracing::info_span!("upsert_contract_state", %key))
                .await,
        );
    };

    let deferral_id = deferral.next_deferral_id;
    deferral.next_deferral_id = deferral.next_deferral_id.wrapping_add(1);
    deferral.stashed.insert(deferral_id, responder);

    let op_manager = contract_handler.executor().op_manager_handle();
    let resume_tx = deferral.resume_tx.clone();

    tracing::debug!(
        contract = %key,
        missing = missing.len(),
        deferral_id,
        "Off-loading related-contract fetch from the contract-handling loop (#4391)"
    );

    // Spawn the off-loop waiter. It drives the related GET(s), then sends a
    // DeferredResume back to the loop. Fire-and-forget is acceptable: the task
    // is short-lived and bounded by MAX_INFLIGHT_DEFERRALS, and a dropped task
    // simply lets the client time out (same as the legacy timeout path).
    GlobalExecutor::spawn(async move {
        let fetched = fetch_related_off_loop(op_manager, missing).await;
        // Non-blocking send on an unbounded channel; if it fails the loop has
        // shut down and the client will time out (same as the legacy path).
        if resume_tx
            .send(DeferredResume {
                deferral_id,
                key,
                update,
                related_contracts,
                code,
                is_put,
                fetched,
            })
            .is_err()
        {
            tracing::debug!(
                contract = %key,
                "Deferred resume channel closed; contract-handling loop gone"
            );
        }
    });

    DeferStep::Deferred
}

/// Build the `PutResponse` (or fatal error) for a PUT upsert result. Shared by
/// the inline PUT arm and the deferred-resume path so both produce identical
/// responses.
///
/// `incoming_state` is the full state the PUT carried; it is returned on the
/// `NoChange` outcome (the incoming state was identical to / older than the
/// stored state), matching the legacy inline behavior exactly.
async fn put_response_from_result<CH>(
    contract_handler: &mut CH,
    key: ContractKey,
    incoming_state: WrappedState,
    put_result: Result<UpsertResult, ExecutorError>,
) -> Result<ContractHandlerEvent, ContractError>
where
    CH: ContractHandler + Send + 'static,
{
    let _ = contract_handler;
    Ok(match put_result {
        Ok(UpsertResult::NoChange) => ContractHandlerEvent::PutResponse {
            new_value: Ok(incoming_state),
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
    })
}

/// Build the `UpdateResponse`/`UpdateNoChange` (or fatal error) for an UPDATE
/// upsert result. Shared by the inline UPDATE arm and the deferred-resume path.
async fn update_response_from_result<CH>(
    contract_handler: &mut CH,
    key: ContractKey,
    update_result: Result<UpsertResult, ExecutorError>,
) -> Result<ContractHandlerEvent, ContractError>
where
    CH: ContractHandler + Send + 'static,
{
    Ok(match update_result {
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
    })
}

/// When the fair queue rejects an `EvictContract` event (queue-full),
/// the hosting-cache entry is already gone — no later sweep would
/// re-emit on its own. Record the key in the pending-reclamation retry
/// queue so the periodic sweep retries via `reclaim_evicted_contract`.
///
/// This closes disk-leak edge case #1 in PR #4212 review round 7
/// (other variants are no-ops here — they have their own error paths).
fn track_pending_reclamation_if_evict<CH>(
    contract_handler: &mut CH,
    rejected: &fair_queue::RejectedEvent,
) where
    CH: ContractHandler + Send + 'static,
{
    if let ContractHandlerEvent::EvictContract {
        key,
        expected_generation,
    } = &rejected.event
    {
        contract_handler
            .executor()
            .track_pending_reclamation(*key, *expected_generation);
    }
}

/// Send an error response to the event sender when the per-contract queue is full.
///
/// Logs at DEBUG (per-event backpressure is not user-actionable; see #4251)
/// and sends the appropriate error response variant for the rejected event.
/// For fire-and-forget events (delegates, disconnects), no response is sent.
async fn send_queue_full_response(
    channel: &mut handler::ContractHandlerChannel<handler::ContractHandlerHalve>,
    rejected: Box<fair_queue::RejectedEvent>,
) {
    // Per-event backpressure is normal under sustained load on a hot contract
    // and is not user-actionable. Logging at WARN once per rejection drowns
    // node logs (millions of lines/day on a saturated contract). Aggregate
    // backpressure visibility belongs in metrics, not per-event logs.
    // See issue #4251 for the underlying queue-saturation tracking.
    tracing::debug!(
        event = %rejected.event,
        "Rejected event due to per-contract queue capacity limit"
    );
    // Typed `ContractQueueFull` marker powers `is_contract_queue_full` at
    // every caller; the UPDATE relay drivers gate amplification on it. #4251.
    let make_err = || ExecutorError::other(ContractQueueFull);
    let response = match &rejected.event {
        ContractHandlerEvent::PutQuery { .. } => ContractHandlerEvent::PutResponse {
            new_value: Err(make_err()),
            state_changed: false,
        },
        ContractHandlerEvent::UpdateQuery { .. } => ContractHandlerEvent::UpdateResponse {
            new_value: Err(make_err()),
            state_changed: false,
        },
        ContractHandlerEvent::GetQuery { .. } => ContractHandlerEvent::GetResponse {
            key: None,
            response: Err(make_err()),
        },
        ContractHandlerEvent::GetSummaryQuery { key, .. } => {
            ContractHandlerEvent::GetSummaryResponse {
                key: *key,
                summary: Err(make_err()),
            }
        }
        ContractHandlerEvent::GetDeltaQuery { key, .. } => ContractHandlerEvent::GetDeltaResponse {
            key: *key,
            delta: Err(make_err()),
        },
        // Events without error response variants: drop the oneshot sender to
        // unblock the caller. The caller's receiver will get a RecvError, which
        // maps to ContractError::NoEvHandlerResponse. This prevents leaking
        // entries in the waiting_response map.
        ContractHandlerEvent::DelegateRequest { .. }
        | ContractHandlerEvent::DelegateResponse(_)
        | ContractHandlerEvent::PutResponse { .. }
        | ContractHandlerEvent::GetResponse { .. }
        | ContractHandlerEvent::UpdateResponse { .. }
        | ContractHandlerEvent::UpdateNoChange { .. }
        | ContractHandlerEvent::RegisterSubscriberListener { .. }
        | ContractHandlerEvent::RegisterSubscriberListenerResponse
        | ContractHandlerEvent::QuerySubscriptions { .. }
        | ContractHandlerEvent::QuerySubscriptionsResponse
        | ContractHandlerEvent::GetSummaryResponse { .. }
        | ContractHandlerEvent::GetDeltaResponse { .. }
        | ContractHandlerEvent::ClientDisconnect { .. }
        | ContractHandlerEvent::EvictContract { .. } => {
            channel.drop_waiting_response(rejected.id);
            return;
        }
    };
    if let Err(error) = channel.send_to_sender(rejected.id, response).await {
        tracing::warn!(
            error = %error,
            "Failed to send queue-full response (client may have disconnected)"
        );
    }
}

async fn handle_delegate_notification<CH, P>(
    contract_handler: &mut CH,
    notification: executor::DelegateNotification,
    prompter: &P,
) where
    CH: ContractHandler + Send + 'static,
    P: UserInputPrompter,
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

    let outbound = handle_delegate_with_contract_requests(
        contract_handler,
        req,
        None,
        &delegate_key,
        prompter,
    )
    .await;

    // TODO: Route outbound ApplicationMessages to subscribed apps #3275
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

async fn handle_contract_event<CH, P>(
    contract_handler: &mut CH,
    id: handler::EventId,
    event: ContractHandlerEvent,
    prompter: &P,
    deferral: Option<&mut DeferralCtx>,
) -> Result<(), ContractError>
where
    CH: ContractHandler + Send + 'static,
    P: UserInputPrompter,
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

            // Run the upsert in deferrable mode when a deferral context is
            // present (the production loop). If the contract needs a related
            // contract that isn't held locally, the executor returns
            // `DeferRelated` instead of awaiting the network GET inline; we
            // off-load that fetch so the serial loop keeps draining other
            // events (e.g. cached GETs) — issue #4391. Without a deferral
            // context (delegate-driven PUTs, direct unit tests), this falls
            // back to the inline-fetch `upsert_contract_state`.
            let put_result = match maybe_defer_upsert(
                contract_handler,
                deferral,
                &id,
                key,
                Either::Left(state.clone()),
                related_contracts,
                contract,
                true,
            )
            .await
            {
                DeferStep::Deferred => return Ok(()),
                DeferStep::Completed(result) => result,
            };

            let event_result =
                put_response_from_result(contract_handler, key, state, put_result).await?;

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
            mut related_contracts,
        } => {
            let update_value: Either<WrappedState, StateDelta<'static>> = match data {
                freenet_stdlib::prelude::UpdateData::State(state) => {
                    Either::Left(WrappedState::from(state.into_bytes()))
                }
                freenet_stdlib::prelude::UpdateData::Delta(delta) => Either::Right(delta),
                freenet_stdlib::prelude::UpdateData::StateAndDelta { state, .. } => {
                    // Prefer the full state — it's authoritative and avoids
                    // an extra delta-merge round trip in the executor. The
                    // delta is dropped because `upsert_contract_state` only
                    // accepts one of the two via the `Either` argument; the
                    // full state is a strict superset of the delta's effect,
                    // so callers cannot observe a stale result. If a caller
                    // ever needs strict delta-only semantics (e.g. a write
                    // that intentionally relies on merge-time validation
                    // rejecting a full-state replacement), they should send
                    // `Delta` directly.
                    Either::Left(WrappedState::from(state.into_bytes()))
                }
                freenet_stdlib::prelude::UpdateData::RelatedStateAndDelta {
                    related_to,
                    state,
                    delta,
                } => {
                    // Bundle the related contract's state into
                    // `related_contracts` so the executor's
                    // related-state-injection bridge surfaces it to the
                    // contract WASM as a `UpdateData::RelatedState` entry.
                    // The delta on this variant targets `key` itself.
                    // `RelatedContracts` exposes no public `insert`, so use
                    // `update()` to mutate the inner slot. If the caller
                    // also supplied a state for the same id via the bare
                    // `related_contracts` field, prefer the inline one —
                    // it arrived with the delta as an atomic package.
                    inject_related_state(&mut related_contracts, related_to, state);
                    Either::Right(delta)
                }
                freenet_stdlib::prelude::UpdateData::RelatedState { .. }
                | freenet_stdlib::prelude::UpdateData::RelatedDelta { .. } => {
                    // These variants carry a payload only for a related
                    // contract, with no main-contract state or delta. The
                    // runtime's `RequestRelated → GET → re-update` flow
                    // populates `related_contracts` itself rather than
                    // routing these variants through `UpdateQuery`, so
                    // hitting this branch indicates a misuse from a
                    // client-facing surface. Reject explicitly instead of
                    // panicking — see freenet/freenet-core#4003.
                    let err = ExecutorError::other(anyhow::anyhow!(
                        "RelatedState / RelatedDelta UpdateData variants are not \
                         accepted directly via ContractRequest::Update — they are \
                         reserved for the runtime's request-related orchestration"
                    ));
                    if let Err(send_err) = contract_handler
                        .channel()
                        .send_to_sender(
                            id,
                            ContractHandlerEvent::UpdateResponse {
                                new_value: Err(err),
                                state_changed: false,
                            },
                        )
                        .await
                    {
                        tracing::debug!(error = %send_err, contract = %key, "Failed to send rejection");
                    }
                    return Ok(());
                }
                // `UpdateData` is `#[non_exhaustive]` since stdlib 0.6.0.
                // Reject unknown future variants explicitly instead of
                // panicking the executor task; the producer at the edge
                // should validate variants before routing them here.
                _ => {
                    let err = ExecutorError::other(anyhow::anyhow!(
                        "Unknown UpdateData variant reached \
                         ContractHandlerEvent::UpdateQuery; add explicit \
                         handling before landing the new variant"
                    ));
                    if let Err(send_err) = contract_handler
                        .channel()
                        .send_to_sender(
                            id,
                            ContractHandlerEvent::UpdateResponse {
                                new_value: Err(err),
                                state_changed: false,
                            },
                        )
                        .await
                    {
                        tracing::debug!(error = %send_err, contract = %key, "Failed to send rejection");
                    }
                    return Ok(());
                }
            };
            // Deferrable upsert: off-load the related-contract fetch from the
            // serial loop when the contract needs a related contract not held
            // locally (#4391). See the PUT arm above for the rationale.
            let update_result = match maybe_defer_upsert(
                contract_handler,
                deferral,
                &id,
                key,
                update_value,
                related_contracts,
                None,
                false,
            )
            .await
            {
                DeferStep::Deferred => return Ok(()),
                DeferStep::Completed(result) => result,
            };

            let event_result =
                update_response_from_result(contract_handler, key, update_result).await?;

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
            origin_contract,
        } => {
            let delegate_key = req.key().clone();
            tracing::debug!(
                delegate_key = %delegate_key,
                ?origin_contract,
                "Processing delegate request"
            );

            // Execute the delegate and handle any GetContractRequest messages
            let response = handle_delegate_with_contract_requests(
                contract_handler,
                req,
                origin_contract.as_ref(),
                &delegate_key,
                prompter,
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
        ContractHandlerEvent::EvictContract {
            key,
            expected_generation,
        } => {
            // Reclaim the contract's on-disk storage after it was evicted
            // from the hosting cache. Routed here (not inline-fast-pathed in
            // the drain loop) so it goes through the fair queue and is
            // serialized per-contract with any other in-flight ops on the
            // same key. Fire-and-forget: no response is sent.
            //
            // `expected_generation` is consulted by
            // `RuntimePool::remove_contract` to skip deletion if the
            // contract's state was rewritten since eviction (re-host race).
            match contract_handler
                .executor()
                .remove_contract(&key, expected_generation)
                .await
            {
                Ok(()) => {
                    tracing::info!(contract = %key, "Reclaimed on-disk storage for evicted contract");
                }
                Err(error) => {
                    tracing::warn!(
                        contract = %key,
                        error = %error,
                        "Failed to reclaim on-disk storage for evicted contract"
                    );
                }
            }
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
        | ContractHandlerEvent::GetDeltaResponse { .. } => {
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

#[cfg(test)]
#[allow(clippy::wildcard_enum_match_arm)]
mod tests {
    use super::*;
    use crate::config::GlobalExecutor;
    use std::time::Duration;

    /// Pin the rejection log site to DEBUG.
    ///
    /// Demoting `Rejected event due to per-contract queue capacity limit`
    /// from WARN to DEBUG was the user-visible point of the #4251 fix —
    /// at WARN it produced millions of lines/day on saturated contracts.
    /// A future refactor that re-promotes it would silently restore that
    /// regression. This is a source-level pin (per
    /// `.claude/rules/bug-prevention-patterns.md`'s precedent for
    /// regression-guard pins) rather than a tracing-subscriber capture,
    /// because the level is the *only* thing being asserted and a string
    /// scan over our own source is more robust than runtime-subscriber
    /// state-machine plumbing.
    #[test]
    fn send_queue_full_response_logs_at_debug_not_warn_pin_test() {
        // file!() returns a workspace-rooted path but tests run from
        // CARGO_MANIFEST_DIR. Anchor explicitly.
        let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("src/contract.rs");
        let source = std::fs::read_to_string(&path)
            .unwrap_or_else(|e| panic!("must read own source at {}: {e}", path.display()));
        let needle = "Rejected event due to per-contract queue capacity limit";
        let idx = source
            .find(needle)
            .expect("rejection log message must still exist in source");
        // Anchor on the closest preceding `tracing::` macro (rfind) rather
        // than a fixed byte window, so the assertion is immune to refactors
        // that move this site relative to other nearby tracing macros.
        // Adopted from the #4272 pin tests; see
        // `operations/update.rs::no_targets_propagation_logs_at_debug_pin_test`.
        let preceding = &source[..idx];
        let macro_idx = preceding
            .rfind("tracing::")
            .expect("a tracing macro must precede the rejection log site");
        let line_start = preceding[..macro_idx].rfind('\n').map_or(0, |n| n + 1);
        let line_prefix = &preceding[line_start..macro_idx];
        assert!(
            line_prefix.chars().all(char::is_whitespace),
            "rfind matched `tracing::` inside a string literal or comment, \
             not a macro invocation. Prefix on its line: {line_prefix:?}"
        );
        let after_macro = &preceding[macro_idx + "tracing::".len()..];
        let macro_name = after_macro.split('!').next().unwrap_or("");
        // Char-boundary-safe last-200-bytes window: a raw byte slice could
        // start mid-UTF-8-char and panic while building the failure message.
        let tail_start = preceding
            .char_indices()
            .map(|(i, _)| i)
            .find(|&i| preceding.len() - i <= 200)
            .unwrap_or(0);
        let context = &preceding[tail_start..];
        // Equality against "debug" rejects WARN/INFO (and any other level)
        // implicitly: re-promotion is exactly the issue #4251 regression.
        assert_eq!(
            macro_name, "debug",
            "Rejected-event log site must be at DEBUG, not WARN/INFO \
             (closest preceding macro is `tracing::{macro_name}!`). \
             Re-promotion restores the issue #4251 log-volume regression.\n\
             Preceding source (last 200 bytes):\n{context}"
        );
    }

    fn make_contract_key() -> ContractKey {
        let code = ContractCode::from(vec![42u8; 32]);
        let params = Parameters::from(vec![7u8; 8]);
        ContractKey::from_params_and_code(&params, &code)
    }

    // Regression test for issue #3857: the executor's origin context must be
    // mapped onto the prompter's CallerIdentity correctly. Without this test,
    // accidentally swapping the Some/None arms — or wiring the wrong
    // variable into the prompter call — would not fail any other test in
    // the suite (the prompter unit tests pass `CallerIdentity` in directly).
    #[test]
    fn test_caller_identity_from_origin() {
        // None origin (delegate-to-delegate, local client, missing
        // attestation) collapses to CallerIdentity::None.
        assert_eq!(caller_identity_from_origin(None), CallerIdentity::None);

        // Some(ContractInstanceId) becomes CallerIdentity::WebApp(<id>),
        // with the id stringified via its Display impl.
        let key = make_contract_key();
        let id = *key.id();
        let mapped = caller_identity_from_origin(Some(&id));
        assert_eq!(mapped, CallerIdentity::WebApp(id.to_string()));

        // Sanity check: the produced string must not be empty (would
        // render as "Freenet app " with a dangling space in the UI).
        match mapped {
            CallerIdentity::WebApp(s) => assert!(!s.is_empty()),
            other => panic!("expected WebApp, got {other:?}"),
        }
    }

    // Regression tests for the `UpdateQuery` arm rewrite that landed in
    // PR #4004: previously every variant other than `State` and `Delta`
    // hit `unreachable!()` and panicked the executor task. The conversion
    // now decomposes `RelatedStateAndDelta` and `StateAndDelta` instead;
    // these tests exercise the `inject_related_state` helper directly so
    // a regression in the conversion (silent state drop, wrong slot, lost
    // related id) fails fast without needing the full runtime stack.

    #[test]
    fn inject_related_state_writes_inline_state_to_slot() {
        // When `RelatedStateAndDelta` arrives with an inline `state` for
        // `related_to`, the inline state must end up in the
        // `RelatedContracts` slot for that id so the executor's
        // related-state-injection bridge surfaces it as a
        // `UpdateData::RelatedState` entry to the contract WASM.
        let related_id = *make_contract_key().id();
        let inline_state = freenet_stdlib::prelude::State::from(vec![1, 2, 3, 4]);

        let mut related = RelatedContracts::default();
        inject_related_state(&mut related, related_id, inline_state.clone());

        let states: Vec<_> = related.states().collect();
        assert_eq!(states.len(), 1, "expected exactly one related entry");
        let (id, slot) = states[0];
        assert_eq!(id, &related_id);
        let stored = slot.as_ref().expect("slot must hold the inline state");
        assert_eq!(stored.as_ref(), inline_state.as_ref());
    }

    #[test]
    fn inject_related_state_overrides_existing_slot() {
        // If the caller already populated `related_contracts` with a state
        // for the same id (e.g. via a separate `related_contracts` arg on
        // `UpdateQuery`), the inline state from `RelatedStateAndDelta`
        // wins. The inline payload arrived alongside the delta as an
        // atomic package and is the sender's authoritative intent.
        let related_id = *make_contract_key().id();

        let prior = freenet_stdlib::prelude::State::from(vec![9, 9]);
        let inline = freenet_stdlib::prelude::State::from(vec![1, 2, 3]);

        let mut map = std::collections::HashMap::new();
        map.insert(related_id, Some(prior));
        let mut related = RelatedContracts::from(map);

        inject_related_state(&mut related, related_id, inline.clone());

        let states: Vec<_> = related.states().collect();
        assert_eq!(states.len(), 1);
        let stored = states[0].1.as_ref().expect("slot must be Some");
        assert_eq!(
            stored.as_ref(),
            inline.as_ref(),
            "inline state must override pre-existing slot"
        );
    }

    /// Helper: send an event through the sender halve and receive it on the handler side,
    /// then wrap it as a RejectedEvent. This simulates the normal drain path where
    /// try_recv_from_sender inserts into waiting_response, followed by queue rejection.
    async fn setup_rejected_event(
        send_halve: handler::ContractHandlerChannel<handler::SenderHalve>,
        rcv_halve: &mut handler::ContractHandlerChannel<handler::ContractHandlerHalve>,
        event: ContractHandlerEvent,
    ) -> (
        Box<fair_queue::RejectedEvent>,
        tokio::task::JoinHandle<Result<ContractHandlerEvent, anyhow::Error>>,
    ) {
        // Spawn the sender — it blocks until the response arrives
        let handle = GlobalExecutor::spawn(async move {
            send_halve
                .send_to_handler(event)
                .await
                .map_err(|e| anyhow::anyhow!("{e}"))
        });

        // Receive on the handler side (populates waiting_response)
        let (id, received_event) =
            tokio::time::timeout(Duration::from_millis(200), rcv_halve.recv_from_sender())
                .await
                .expect("timeout waiting for event")
                .expect("channel should be open");

        let rejected = Box::new(fair_queue::RejectedEvent {
            id,
            event: received_event,
        });

        (rejected, handle)
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn send_queue_full_response_put_query() {
        let (send_halve, mut rcv_halve, _) = handler::contract_handler_channel();
        let key = make_contract_key();

        let (rejected, handle) = setup_rejected_event(
            send_halve,
            &mut rcv_halve,
            ContractHandlerEvent::PutQuery {
                key,
                state: WrappedState::new(vec![1, 2, 3]),
                related_contracts: RelatedContracts::default(),
                contract: None,
            },
        )
        .await;

        send_queue_full_response(&mut rcv_halve, rejected).await;

        let response = tokio::time::timeout(Duration::from_millis(200), handle)
            .await
            .expect("timeout")
            .expect("task should complete")
            .expect("should get response");

        match response {
            ContractHandlerEvent::PutResponse {
                new_value,
                state_changed,
            } => {
                assert!(new_value.is_err(), "should be an error response");
                assert!(!state_changed);
            }
            other => panic!("expected PutResponse, got {other}"),
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn send_queue_full_response_get_query() {
        let (send_halve, mut rcv_halve, _) = handler::contract_handler_channel();
        let key = make_contract_key();

        let (rejected, handle) = setup_rejected_event(
            send_halve,
            &mut rcv_halve,
            ContractHandlerEvent::GetQuery {
                instance_id: *key.id(),
                return_contract_code: false,
            },
        )
        .await;

        send_queue_full_response(&mut rcv_halve, rejected).await;

        let response = tokio::time::timeout(Duration::from_millis(200), handle)
            .await
            .expect("timeout")
            .expect("task should complete")
            .expect("should get response");

        match response {
            ContractHandlerEvent::GetResponse { response, .. } => {
                assert!(response.is_err(), "should be an error response");
            }
            other => panic!("expected GetResponse, got {other}"),
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn send_queue_full_response_update_query() {
        let (send_halve, mut rcv_halve, _) = handler::contract_handler_channel();
        let key = make_contract_key();

        let (rejected, handle) = setup_rejected_event(
            send_halve,
            &mut rcv_halve,
            ContractHandlerEvent::UpdateQuery {
                key,
                data: UpdateData::Delta(StateDelta::from(vec![1])),
                related_contracts: RelatedContracts::default(),
            },
        )
        .await;

        send_queue_full_response(&mut rcv_halve, rejected).await;

        let response = tokio::time::timeout(Duration::from_millis(200), handle)
            .await
            .expect("timeout")
            .expect("task should complete")
            .expect("should get response");

        match response {
            ContractHandlerEvent::UpdateResponse {
                new_value,
                state_changed,
            } => {
                assert!(new_value.is_err(), "should be an error response");
                assert!(!state_changed);
            }
            other => panic!("expected UpdateResponse, got {other}"),
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn send_queue_full_response_get_summary_query() {
        let (send_halve, mut rcv_halve, _) = handler::contract_handler_channel();
        let key = make_contract_key();

        let (rejected, handle) = setup_rejected_event(
            send_halve,
            &mut rcv_halve,
            ContractHandlerEvent::GetSummaryQuery { key },
        )
        .await;

        send_queue_full_response(&mut rcv_halve, rejected).await;

        let response = tokio::time::timeout(Duration::from_millis(200), handle)
            .await
            .expect("timeout")
            .expect("task should complete")
            .expect("should get response");

        match response {
            ContractHandlerEvent::GetSummaryResponse {
                key: resp_key,
                summary,
            } => {
                assert_eq!(resp_key, key);
                assert!(summary.is_err(), "should be an error response");
            }
            other => panic!("expected GetSummaryResponse, got {other}"),
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn send_queue_full_response_get_delta_query() {
        let (send_halve, mut rcv_halve, _) = handler::contract_handler_channel();
        let key = make_contract_key();

        let (rejected, handle) = setup_rejected_event(
            send_halve,
            &mut rcv_halve,
            ContractHandlerEvent::GetDeltaQuery {
                key,
                their_summary: StateSummary::from(vec![1, 2]),
            },
        )
        .await;

        send_queue_full_response(&mut rcv_halve, rejected).await;

        let response = tokio::time::timeout(Duration::from_millis(200), handle)
            .await
            .expect("timeout")
            .expect("task should complete")
            .expect("should get response");

        match response {
            ContractHandlerEvent::GetDeltaResponse {
                key: resp_key,
                delta,
            } => {
                assert_eq!(resp_key, key);
                assert!(delta.is_err(), "should be an error response");
            }
            other => panic!("expected GetDeltaResponse, got {other}"),
        }
    }

    /// Issue #4251: the queue-full response MUST carry the typed
    /// `ContractQueueFull` marker, not just an opaque anyhow error, so
    /// callers can suppress amplification side effects (auto-fetch,
    /// ResyncRequest, ERROR logs) via
    /// `ExecutorError::is_contract_queue_full`. A regression here would
    /// silently bring back the storm-on-saturation behavior.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn send_queue_full_response_carries_typed_queue_full_marker() {
        let (send_halve, mut rcv_halve, _) = handler::contract_handler_channel();
        let key = make_contract_key();

        let (rejected, handle) = setup_rejected_event(
            send_halve,
            &mut rcv_halve,
            ContractHandlerEvent::UpdateQuery {
                key,
                data: UpdateData::Delta(StateDelta::from(vec![1])),
                related_contracts: RelatedContracts::default(),
            },
        )
        .await;

        send_queue_full_response(&mut rcv_halve, rejected).await;

        let response = tokio::time::timeout(Duration::from_millis(200), handle)
            .await
            .expect("timeout")
            .expect("task should complete")
            .expect("should get response");

        match response {
            ContractHandlerEvent::UpdateResponse { new_value, .. } => {
                let err = new_value.expect_err("UpdateResponse should carry an error");
                assert!(
                    err.is_contract_queue_full(),
                    "queue-full response MUST be classified by is_contract_queue_full; \
                     got err = {err:?}. If you removed this classification, you have also \
                     re-enabled the auto-fetch / ResyncRequest amplification storm — see \
                     issue #4251."
                );
                // Disjointness from other predicates is covered by
                // `test_contract_queue_full_disjoint_from_other_predicates`
                // in executor.rs.
            }
            other => panic!("expected UpdateResponse, got {other}"),
        }
    }

    /// Verify that fire-and-forget events (RegisterSubscriberListener, delegates, etc.)
    /// have their waiting_response entry cleaned up via drop_waiting_response when rejected.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn send_queue_full_response_fire_and_forget_cleans_waiting_response() {
        let (send_halve, mut rcv_halve, _) = handler::contract_handler_channel();
        let key = make_contract_key();

        let (rejected, handle) = setup_rejected_event(
            send_halve,
            &mut rcv_halve,
            ContractHandlerEvent::RegisterSubscriberListener {
                key: *key.id(),
                client_id: crate::client_events::ClientId::next(),
                summary: None,
                subscriber_listener: tokio::sync::mpsc::channel(64).0,
            },
        )
        .await;

        // Verify waiting_response was populated
        assert!(
            rcv_halve.has_waiting_response(&rejected.id),
            "waiting_response should contain the event"
        );

        let rejected_id = handler::EventId { id: rejected.id.id };
        send_queue_full_response(&mut rcv_halve, rejected).await;

        // Verify waiting_response was cleaned up
        assert!(
            !rcv_halve.has_waiting_response(&rejected_id),
            "waiting_response should be cleaned up after rejection"
        );

        // The sender should get a RecvError (NoEvHandlerResponse) since we dropped the oneshot
        let result = tokio::time::timeout(Duration::from_millis(200), handle)
            .await
            .expect("timeout")
            .expect("task should complete");
        assert!(
            result.is_err(),
            "fire-and-forget rejection should produce an error"
        );
    }

    // ======================================================================
    // End-to-end UpdateQuery dispatch tests (issue #4005)
    // ======================================================================
    //
    // These drive an `UpdateData` variant through the *full* dispatch arm in
    // `handle_contract_event` via the `ContractHandlerChannel`, then assert on
    // the `UpdateResponse` that flows back to the sender. Unlike the
    // `inject_related_state_*` unit tests (which call the helper in isolation)
    // and `send_queue_full_response_update_query` (which only exercises the
    // rejection path), these tests would catch a regression that reintroduced
    // `unreachable!()` in the dispatch arm itself — the exact failure mode
    // PR #4004 fixed. A revert of the `StateAndDelta` or `RelatedStateAndDelta`
    // conversion arms now fails CI here.

    /// Drive a single `UpdateQuery` event end-to-end through
    /// `handle_contract_event` over a real `ContractHandlerChannel` backed by a
    /// `MemoryContractHandler` (mock executor), returning the response the
    /// sender observes.
    ///
    /// Mirrors the production driver loop (`contract_handling`): the sender's
    /// `send_to_handler` blocks awaiting the response; the handler side
    /// `recv_from_sender`s to register the waiting-response slot and obtain the
    /// `EventId`, then `handle_contract_event` runs the dispatch arm and
    /// fulfils the sender's oneshot via `send_to_sender`.
    async fn dispatch_update_query_e2e(
        handler: &mut MemoryContractHandler,
        send_halve: &handler::ContractHandlerChannel<handler::SenderHalve>,
        key: ContractKey,
        data: UpdateData<'static>,
        related_contracts: RelatedContracts<'static>,
    ) -> ContractHandlerEvent {
        let event = ContractHandlerEvent::UpdateQuery {
            key,
            data,
            related_contracts,
        };
        // Run the sender concurrently with the handler-side dispatch via
        // `tokio::join!` (the handler is borrowed, so it cannot move into a
        // spawned task). `send_to_handler` awaits the oneshot until the
        // dispatch arm replies via `send_to_sender`; the dispatch never blocks,
        // so the two futures make progress cooperatively to completion.
        let send_fut = send_halve.send_to_handler(event);
        let recv_fut = async {
            let (id, received) = handler
                .channel()
                .recv_from_sender()
                .await
                .expect("handler channel should be open");
            handle_contract_event(
                handler,
                id,
                received,
                &user_input::AutoApprovePrompter,
                None,
            )
            .await
            .expect("dispatch must not error for a well-formed UpdateQuery");
        };
        let (send_res, ()) = tokio::join!(send_fut, recv_fut);
        send_res.expect("sender must receive a response")
    }

    /// Seed an initial state for `contract` via a `PutQuery` so subsequent
    /// `UpdateQuery`s have an existing state to merge against (the mock
    /// executor rejects an update with no prior state via `MissingContract`).
    async fn seed_contract_state(
        handler: &mut MemoryContractHandler,
        send_halve: &handler::ContractHandlerChannel<handler::SenderHalve>,
        contract: ContractContainer,
        initial: WrappedState,
    ) {
        let key = contract.key();
        let event = ContractHandlerEvent::PutQuery {
            key,
            state: initial,
            related_contracts: RelatedContracts::default(),
            contract: Some(contract),
        };
        let send_fut = send_halve.send_to_handler(event);
        let recv_fut = async {
            let (id, received) = handler
                .channel()
                .recv_from_sender()
                .await
                .expect("handler channel should be open");
            handle_contract_event(
                handler,
                id,
                received,
                &user_input::AutoApprovePrompter,
                None,
            )
            .await
            .expect("seed PutQuery must not error");
        };
        let (send_res, ()) = tokio::join!(send_fut, recv_fut);
        match send_res.expect("seed PutQuery must respond") {
            ContractHandlerEvent::PutResponse { new_value, .. } => {
                new_value.expect("seed PutQuery must store state");
            }
            other => panic!("expected PutResponse from seed, got {other}"),
        }
    }

    /// `StateAndDelta` must survive dispatch (not panic) AND the conversion
    /// must prefer the full `state` over the `delta`. We seed a contract, then
    /// send `StateAndDelta { state, delta }` where `state` is chosen to win the
    /// mock's hash-comparison merge and `delta` is distinct, non-empty bytes.
    /// If the dispatch arm wrongly routed the delta into the executor, the
    /// stored result would be `seed ++ delta` (the mock's delta-concat merge);
    /// asserting the response equals the full `state` bytes proves the
    /// full-state path was taken.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn update_query_state_and_delta_dispatch_prefers_full_state_e2e() {
        let (send_halve, rcv_halve, _) = handler::contract_handler_channel();
        let mut handler = MemoryContractHandler::new(
            rcv_halve,
            None,
            "update_query_state_and_delta_dispatch_prefers_full_state",
        )
        .await;

        let contract = executor::mock_runtime::test::create_test_contract(b"sad_e2e");
        let key = contract.key();

        // Seed with bytes whose hash is the smaller of the two, so the update's
        // full state deterministically wins the mock's "larger hash wins"
        // merge and we observe `Updated(full_state)`.
        let candidate_a = WrappedState::new(vec![0xAA; 16]);
        let candidate_b = WrappedState::new(vec![0xBB; 16]);
        let (seed_state, winning_full_state) = if blake3::hash(candidate_a.as_ref()).as_bytes()
            < blake3::hash(candidate_b.as_ref()).as_bytes()
        {
            (candidate_a, candidate_b)
        } else {
            (candidate_b, candidate_a)
        };

        seed_contract_state(&mut handler, &send_halve, contract, seed_state.clone()).await;

        // A delta distinct from the full state. If the dispatch arm used this
        // delta instead of the state, the mock would store `seed ++ delta`,
        // which is neither `winning_full_state` nor equal in length to it.
        let distinct_delta = StateDelta::from(vec![0xCC; 8]);
        let data = UpdateData::StateAndDelta {
            state: freenet_stdlib::prelude::State::from(winning_full_state.as_ref().to_vec()),
            delta: distinct_delta,
        };

        let response = dispatch_update_query_e2e(
            &mut handler,
            &send_halve,
            key,
            data,
            RelatedContracts::default(),
        )
        .await;

        match response {
            ContractHandlerEvent::UpdateResponse {
                new_value,
                state_changed,
            } => {
                let stored = new_value.expect("StateAndDelta update must succeed");
                assert_eq!(
                    stored.as_ref(),
                    winning_full_state.as_ref(),
                    "dispatch must store the full state, not seed++delta — \
                     a regression here means the conversion arm routed the \
                     delta instead of the authoritative full state"
                );
                assert!(
                    state_changed,
                    "full state differs from seed, so the merge changed state"
                );
            }
            other => panic!("expected UpdateResponse, got {other}"),
        }
    }

    /// `RelatedStateAndDelta` must survive dispatch (not panic) and route
    /// through `inject_related_state` + the delta path. We seed a contract,
    /// then send `RelatedStateAndDelta { related_to, state, delta }`. The
    /// dispatch arm injects the related state into `related_contracts` and
    /// feeds the `delta` to the executor for `key`. The mock applies the delta
    /// (seed ++ delta), so the response is a successful `UpdateResponse`
    /// reflecting the delta application — proving the arm reaches the executor
    /// rather than panicking. (The injection itself is unit-tested in
    /// `inject_related_state_*`; this test covers the wiring.)
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn update_query_related_state_and_delta_dispatch_routes_through_helper_e2e() {
        let (send_halve, rcv_halve, _) = handler::contract_handler_channel();
        let mut handler = MemoryContractHandler::new(
            rcv_halve,
            None,
            "update_query_related_state_and_delta_dispatch_routes_through_helper",
        )
        .await;

        let contract = executor::mock_runtime::test::create_test_contract(b"rsad_e2e");
        let key = contract.key();
        let related_contract = executor::mock_runtime::test::create_test_contract(b"rsad_related");
        let related_id = *related_contract.key().id();

        let seed_state = WrappedState::new(vec![1, 2, 3]);
        seed_contract_state(&mut handler, &send_halve, contract, seed_state.clone()).await;

        // The mock applies a delta by concatenating `seed ++ delta` and only
        // stores the result if its hash beats the current state's hash
        // (otherwise `NoChange`). Pick a delta whose concatenation
        // deterministically wins, so the test is independent of incidental
        // hash ordering and reliably observes `Updated(seed ++ delta)`.
        let seed_hash = *blake3::hash(seed_state.as_ref()).as_bytes();
        let delta_bytes = (0u8..=255)
            .map(|n| vec![n; 4])
            .find(|candidate| {
                let mut merged = seed_state.as_ref().to_vec();
                merged.extend_from_slice(candidate);
                *blake3::hash(&merged).as_bytes() > seed_hash
            })
            .expect("some 4-byte delta must produce a winning concatenation hash");
        let data = UpdateData::RelatedStateAndDelta {
            related_to: related_id,
            state: freenet_stdlib::prelude::State::from(vec![9, 9, 9]),
            delta: StateDelta::from(delta_bytes.clone()),
        };

        let response = dispatch_update_query_e2e(
            &mut handler,
            &send_halve,
            key,
            data,
            RelatedContracts::default(),
        )
        .await;

        match response {
            ContractHandlerEvent::UpdateResponse { new_value, .. } => {
                let stored = new_value.expect(
                    "RelatedStateAndDelta must reach the executor and apply the \
                     delta — an Err here (or a panic) means the dispatch arm \
                     regressed to rejecting/unreachable!() the variant",
                );
                // The mock concatenates the delta onto the seed; the result must
                // therefore contain the delta bytes as a suffix, confirming the
                // delta (not the related state) drove the `key` update.
                assert!(
                    stored.as_ref().ends_with(&delta_bytes),
                    "stored state {:?} must end with the delta bytes {:?}, \
                     proving the delta arm of RelatedStateAndDelta was used",
                    stored.as_ref(),
                    delta_bytes
                );
            }
            other => panic!("expected UpdateResponse, got {other}"),
        }
    }

    /// The `RelatedState` and `RelatedDelta` variants carry a payload only for
    /// a *related* contract, with no main-contract state/delta. They used to
    /// hit `unreachable!()`; the fix returns a structured
    /// `UpdateResponse { new_value: Err, state_changed: false }`. This test
    /// drives both through dispatch end-to-end and asserts the structured
    /// rejection (no panic, error returned via the channel, no state change).
    ///
    /// The unknown-`_` arm cannot be exercised from test code: `UpdateData` is
    /// `#[non_exhaustive]` *outside* its defining crate, but within freenet-core
    /// every existing variant is matched explicitly, so there is no constructible
    /// value that reaches the `_` arm. It remains as defensive handling for a
    /// future stdlib variant; the two reachable rejection variants are covered
    /// here.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn update_query_rejection_arms_return_structured_error_e2e() {
        let (send_halve, rcv_halve, _) = handler::contract_handler_channel();
        let mut handler = MemoryContractHandler::new(
            rcv_halve,
            None,
            "update_query_rejection_arms_return_structured_error",
        )
        .await;

        let contract = executor::mock_runtime::test::create_test_contract(b"reject_e2e");
        let key = contract.key();
        let related_id = *executor::mock_runtime::test::create_test_contract(b"reject_related")
            .key()
            .id();

        // Seed so a *correct* main-contract update would succeed — this rules
        // out "rejected because the contract was missing" and isolates the
        // rejection to the variant itself.
        seed_contract_state(
            &mut handler,
            &send_halve,
            contract,
            WrappedState::new(vec![7, 7, 7]),
        )
        .await;

        let rejection_variants = [
            UpdateData::RelatedState {
                related_to: related_id,
                state: freenet_stdlib::prelude::State::from(vec![1, 1]),
            },
            UpdateData::RelatedDelta {
                related_to: related_id,
                delta: StateDelta::from(vec![2, 2]),
            },
        ];

        for data in rejection_variants {
            let label = format!("{data:?}");
            let response = dispatch_update_query_e2e(
                &mut handler,
                &send_halve,
                key,
                data,
                RelatedContracts::default(),
            )
            .await;

            match response {
                ContractHandlerEvent::UpdateResponse {
                    new_value,
                    state_changed,
                } => {
                    assert!(
                        new_value.is_err(),
                        "{label} must be rejected with a structured error, \
                         not applied as an update"
                    );
                    assert!(
                        !state_changed,
                        "{label} rejection must report state_changed = false"
                    );
                }
                other => panic!("expected UpdateResponse for {label}, got {other}"),
            }
        }
    }
}

// ============================================================================
// Head-of-line blocking regression tests (issue #4391)
// ============================================================================
//
// The serial `contract_handling` loop used to await a PUT/UPDATE's related-
// contract NETWORK fetch INLINE, pinning the loop for up to
// RELATED_FETCH_TIMEOUT and blocking every queued event behind it — including
// local-store-hit GETs that need no network at all. These tests drive the REAL
// `contract_handling` loop and assert the fix off-loads that wait so unrelated
// work keeps draining.
#[cfg(test)]
#[allow(clippy::wildcard_enum_match_arm)]
mod hol_4391_tests {
    use super::*;
    use crate::config::GlobalExecutor;
    use crate::contract::executor::mock_wasm_runtime::ValidateOverride;
    use std::sync::Arc;
    use std::time::Duration;

    /// Serializes these tests: `OFF_LOOP_FETCH_OVERRIDE` is a process-global
    /// hook, so two of these tests running concurrently would clobber each
    /// other's stub. Each test holds this guard for its duration. An
    /// async-aware mutex so the guard can be held across the tests' `.await`s.
    static TEST_GUARD: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

    fn make_contract(code_bytes: &[u8]) -> ContractContainer {
        let code = ContractCode::from(code_bytes.to_vec());
        let params = Parameters::from(vec![]);
        ContractContainer::Wasm(ContractWasmAPIVersion::V1(WrappedContract::new(
            Arc::new(code),
            params,
        )))
    }

    /// Build a handler over a `MockWasmRuntime` executor (no `op_manager`), with
    /// the given per-contract validate overrides installed, plus the sender
    /// halve used to drive it.
    async fn build_handler(
        overrides: Vec<(ContractInstanceId, ValidateOverride)>,
    ) -> (
        MockWasmContractHandler,
        handler::ContractHandlerChannel<handler::SenderHalve>,
    ) {
        let (send_halve, rcv_halve, _) = handler::contract_handler_channel();
        let mut handler = MockWasmContractHandler::new_test(rcv_halve, None, "hol_4391").await;
        for (id, ov) in overrides {
            handler.runtime_mut().validate_overrides.insert(id, ov);
        }
        (handler, send_halve)
    }

    /// PUT a contract with no related dependencies (a plain local store).
    async fn put_local(
        send: &handler::ContractHandlerChannel<handler::SenderHalve>,
        contract: ContractContainer,
        state: WrappedState,
    ) -> ContractHandlerEvent {
        send.send_to_handler(ContractHandlerEvent::PutQuery {
            key: contract.key(),
            state,
            related_contracts: RelatedContracts::default(),
            contract: Some(contract),
        })
        .await
        .expect("PUT must respond")
    }

    /// The core regression: a PUT whose related fetch hangs must NOT block a
    /// subsequent local-store-hit GET on the serial loop. Before the fix the
    /// GET would wait the full related-fetch timeout; after the fix the fetch
    /// is off-loaded and the GET returns promptly.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn deferred_related_fetch_does_not_block_local_get() {
        let _guard = TEST_GUARD.lock().await;
        // Contract A requests related contract B (which is never local). The
        // off-loop fetch hangs until we release `gate`, simulating a slow /
        // unavailable network GET.
        let contract_a = make_contract(b"hol_a_blocking");
        let key_a = contract_a.key();
        let id_b = *make_contract(b"hol_b_missing").key().id();

        let (handler, send) = build_handler(vec![(
            *key_a.id(),
            ValidateOverride::RequestRelated(vec![id_b]),
        )])
        .await;

        // Off-loop fetch hangs on this gate, then reports B as missing so A
        // ultimately fails with MissingRelated (we only care that it does NOT
        // block the loop while it hangs).
        let gate = Arc::new(tokio::sync::Notify::new());
        let gate_for_stub = gate.clone();
        set_off_loop_fetch_override(Some(Arc::new(move |missing| {
            let gate = gate_for_stub.clone();
            Box::pin(async move {
                gate.notified().await;
                Err(ExecutorError::missing_related(missing[0]))
            })
        })));

        // Seed a locally-stored contract C (no related, plain local store).
        let contract_c = make_contract(b"hol_c_cached");
        let key_c = contract_c.key();

        // Share the sender across the test task and the background PUT task
        // (the channel itself is not `Clone`).
        let send = Arc::new(send);

        let handle = GlobalExecutor::spawn(contract_handling(
            handler,
            crate::contract::user_input::AutoApprovePrompter,
        ));

        // Store C so a later GET for it is a pure local-store hit.
        let put_c = put_local(
            send.as_ref(),
            contract_c,
            WrappedState::new(b"c_state".to_vec()),
        )
        .await;
        assert!(
            matches!(
                put_c,
                ContractHandlerEvent::PutResponse {
                    new_value: Ok(_),
                    ..
                }
            ),
            "seed PUT for C must succeed, got {put_c}"
        );

        // Fire the related-needing PUT for A on a background task: it will defer
        // (off-loop fetch hangs on the gate) and not respond until we release it.
        let send_a = send.clone();
        let a_state = WrappedState::new(b"a_state".to_vec());
        let a_task: tokio::task::JoinHandle<Result<ContractHandlerEvent, ContractError>> =
            GlobalExecutor::spawn(async move {
                send_a
                    .send_to_handler(ContractHandlerEvent::PutQuery {
                        key: key_a,
                        state: a_state,
                        related_contracts: RelatedContracts::default(),
                        contract: Some(contract_a),
                    })
                    .await
            });

        // Give the loop a beat to pick up A's PUT and enter the deferral path.
        tokio::time::sleep(Duration::from_millis(100)).await;

        // The crux: a local-store-hit GET for C must complete PROMPTLY while
        // A's fetch is still hanging. Before the fix this would block on A's
        // inline network wait. Use a tight timeout — well under the deferred
        // fetch timeout — so a regression (inline blocking) fails the test.
        let get_c = tokio::time::timeout(
            Duration::from_secs(2),
            send.send_to_handler(ContractHandlerEvent::GetQuery {
                instance_id: *key_c.id(),
                return_contract_code: false,
            }),
        )
        .await
        .expect("GET for cached C must NOT block behind A's deferred network fetch (#4391)")
        .expect("GET for C must respond");

        match get_c {
            ContractHandlerEvent::GetResponse { response, .. } => {
                let store = response.expect("GET for C must succeed");
                assert_eq!(
                    store.state.expect("C has state").as_ref(),
                    b"c_state",
                    "GET must return C's stored state"
                );
            }
            other => panic!("expected GetResponse for C, got {other}"),
        }

        // Release the gate: A's deferred fetch reports B missing, so A's PUT
        // resolves (with a MissingRelated error) and its client is answered.
        gate.notify_one();
        let a_resp = tokio::time::timeout(Duration::from_secs(5), a_task)
            .await
            .expect("A's PUT must eventually resolve after the fetch completes")
            .expect("A task join")
            .expect("A's PUT must respond");
        match a_resp {
            ContractHandlerEvent::PutResponse { new_value, .. } => {
                assert!(
                    new_value.is_err(),
                    "A's PUT must surface MissingRelated once the off-loop fetch fails"
                );
            }
            other => panic!("expected PutResponse for A, got {other}"),
        }

        set_off_loop_fetch_override(None);
        handle.abort();
    }

    /// Happy path: a deferred PUT whose off-loop fetch SUCCEEDS resumes and
    /// stores the contract, answering the original client.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn deferred_related_fetch_success_resumes_and_completes() {
        let _guard = TEST_GUARD.lock().await;
        let contract_a = make_contract(b"hol_resume_a");
        let key_a = contract_a.key();
        let id_b = *make_contract(b"hol_resume_b").key().id();

        let (handler, send) = build_handler(vec![(
            *key_a.id(),
            ValidateOverride::RequestRelated(vec![id_b]),
        )])
        .await;

        // Off-loop fetch returns a synthetic state for B immediately.
        set_off_loop_fetch_override(Some(Arc::new(move |missing| {
            Box::pin(async move {
                Ok(missing
                    .into_iter()
                    .map(|id| (id, WrappedState::new(b"fetched_b".to_vec())))
                    .collect())
            })
        })));

        let handle = GlobalExecutor::spawn(contract_handling(
            handler,
            crate::contract::user_input::AutoApprovePrompter,
        ));

        let resp = tokio::time::timeout(
            Duration::from_secs(5),
            send.send_to_handler(ContractHandlerEvent::PutQuery {
                key: key_a,
                state: WrappedState::new(b"a_state".to_vec()),
                related_contracts: RelatedContracts::default(),
                contract: Some(contract_a),
            }),
        )
        .await
        .expect("deferred PUT must resume and respond")
        .expect("PUT must respond");

        match resp {
            ContractHandlerEvent::PutResponse { new_value, .. } => {
                new_value.expect("deferred PUT must succeed once B is supplied off-loop");
            }
            other => panic!("expected PutResponse, got {other}"),
        }

        set_off_loop_fetch_override(None);
        handle.abort();
    }

    /// Failure path: a deferred PUT whose off-loop fetch FAILS surfaces
    /// MissingRelated to the client (and does not hang or defer again).
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn deferred_related_fetch_failure_surfaces_missing_related() {
        let _guard = TEST_GUARD.lock().await;
        let contract_a = make_contract(b"hol_fail_a");
        let key_a = contract_a.key();
        let id_b = *make_contract(b"hol_fail_b").key().id();

        let (handler, send) = build_handler(vec![(
            *key_a.id(),
            ValidateOverride::RequestRelated(vec![id_b]),
        )])
        .await;

        set_off_loop_fetch_override(Some(Arc::new(move |missing| {
            Box::pin(async move { Err(ExecutorError::missing_related(missing[0])) })
        })));

        let handle = GlobalExecutor::spawn(contract_handling(
            handler,
            crate::contract::user_input::AutoApprovePrompter,
        ));

        let resp = tokio::time::timeout(
            Duration::from_secs(5),
            send.send_to_handler(ContractHandlerEvent::PutQuery {
                key: key_a,
                state: WrappedState::new(b"a_state".to_vec()),
                related_contracts: RelatedContracts::default(),
                contract: Some(contract_a),
            }),
        )
        .await
        .expect("deferred PUT must resolve (not hang) when the fetch fails")
        .expect("PUT must respond");

        match resp {
            ContractHandlerEvent::PutResponse { new_value, .. } => {
                assert!(
                    new_value.is_err(),
                    "a failed off-loop fetch must surface an error to the client"
                );
            }
            other => panic!("expected PutResponse, got {other}"),
        }

        set_off_loop_fetch_override(None);
        handle.abort();
    }

    /// A PUT whose related contract IS already local must NOT defer — it
    /// behaves exactly as before (no off-loop fetch invoked). We prove this by
    /// installing an override that PANICS if called, then PUTting a contract
    /// whose only related dependency was stored locally first.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn all_related_local_does_not_defer() {
        let _guard = TEST_GUARD.lock().await;
        // B is stored locally first, then A requests B — validation must
        // resolve B from the local store and never reach the off-loop fetch.
        let contract_b = make_contract(b"hol_local_b");
        let key_b = contract_b.key();
        let contract_a = make_contract(b"hol_local_a");
        let key_a = contract_a.key();

        let (handler, send) = build_handler(vec![(
            *key_a.id(),
            ValidateOverride::RequestRelated(vec![*key_b.id()]),
        )])
        .await;

        // Override panics if the off-loop fetch is ever invoked — proving the
        // all-local path never defers.
        set_off_loop_fetch_override(Some(Arc::new(|_missing| {
            Box::pin(async move {
                panic!("off-loop fetch must NOT be invoked when all related contracts are local");
            })
        })));

        let handle = GlobalExecutor::spawn(contract_handling(
            handler,
            crate::contract::user_input::AutoApprovePrompter,
        ));

        // Store B locally so A's RequestRelated([B]) resolves without network.
        let put_b = put_local(&send, contract_b, WrappedState::new(b"b_state".to_vec())).await;
        assert!(
            matches!(
                put_b,
                ContractHandlerEvent::PutResponse {
                    new_value: Ok(_),
                    ..
                }
            ),
            "seed PUT for B must succeed, got {put_b}"
        );

        let resp = tokio::time::timeout(
            Duration::from_secs(5),
            send.send_to_handler(ContractHandlerEvent::PutQuery {
                key: key_a,
                state: WrappedState::new(b"a_state".to_vec()),
                related_contracts: RelatedContracts::default(),
                contract: Some(contract_a),
            }),
        )
        .await
        .expect("local-resolvable PUT must complete inline")
        .expect("PUT must respond");

        match resp {
            ContractHandlerEvent::PutResponse { new_value, .. } => {
                new_value.expect("A must store successfully when B is already local");
            }
            other => panic!("expected PutResponse, got {other}"),
        }

        set_off_loop_fetch_override(None);
        handle.abort();
    }

    /// One-deferral cap: a contract that requests a related contract on EVERY
    /// validate call (even after the first round's states are supplied) must
    /// surface MissingRelated after a single deferral — it must NOT spawn a
    /// second off-loop fetch or loop forever. We prove the off-loop fetch runs
    /// at most once by counting stub invocations.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn second_related_request_after_resume_does_not_defer_again() {
        let _guard = TEST_GUARD.lock().await;
        let contract_a = make_contract(b"hol_cap_a");
        let key_a = contract_a.key();
        let id_b = *make_contract(b"hol_cap_b").key().id();

        // AlwaysRequestRelated: returns RequestRelated([B]) on EVERY call, so
        // even after B is supplied on resume the contract asks again — the
        // depth>1 / one-deferral cap must kick in.
        let (handler, send) = build_handler(vec![(
            *key_a.id(),
            ValidateOverride::AlwaysRequestRelated(vec![id_b]),
        )])
        .await;

        // Count how many times the off-loop fetch is invoked. The cap requires
        // it be invoked AT MOST once (the first/only deferral); the resume must
        // not spawn a second fetch.
        let calls = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let calls_stub = calls.clone();
        set_off_loop_fetch_override(Some(Arc::new(move |missing| {
            calls_stub.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            Box::pin(async move {
                Ok(missing
                    .into_iter()
                    .map(|id| (id, WrappedState::new(b"fetched".to_vec())))
                    .collect())
            })
        })));

        let handle = GlobalExecutor::spawn(contract_handling(
            handler,
            crate::contract::user_input::AutoApprovePrompter,
        ));

        let resp = tokio::time::timeout(
            Duration::from_secs(5),
            send.send_to_handler(ContractHandlerEvent::PutQuery {
                key: key_a,
                state: WrappedState::new(b"a_state".to_vec()),
                related_contracts: RelatedContracts::default(),
                contract: Some(contract_a),
            }),
        )
        .await
        .expect("deferred-then-resumed PUT must resolve, not loop forever")
        .expect("PUT must respond");

        match resp {
            ContractHandlerEvent::PutResponse { new_value, .. } => {
                assert!(
                    new_value.is_err(),
                    "a contract that keeps requesting related contracts must be \
                     rejected after one deferral (depth=1 / one-deferral cap)"
                );
            }
            other => panic!("expected PutResponse, got {other}"),
        }

        assert_eq!(
            calls.load(std::sync::atomic::Ordering::SeqCst),
            1,
            "the off-loop fetch must run exactly once — the resume must NOT \
             defer again (#4391 one-deferral cap)"
        );

        set_off_loop_fetch_override(None);
        handle.abort();
    }
}
