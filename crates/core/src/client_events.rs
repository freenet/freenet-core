//! Clients events related logic and type definitions. For example, receival of client events from applications throught the HTTP API.

// pub(crate) mod admin_endpoints; // TODO: Add axum dependencies
pub(crate) mod combinator;
pub(crate) mod error;
#[cfg(test)]
mod integration_verification;
pub(crate) mod proxy;
pub(crate) mod result_router;
pub(crate) mod session_actor;
pub(crate) mod test;
#[cfg(test)]
mod test_correlation;
pub(crate) mod types;
/// Per-user operation/export rate limiting for hosted mode (#4561, P5 of #4381).
/// Not gated on the `websocket` feature: it has no axum dependency (just
/// `DashMap` + `tokio::time` + `UserId`) and its `DEFAULT_*` constants are the
/// single source of truth for the operator-config defaults in `config.rs`,
/// which compiles with or without the feature.
pub(crate) mod user_op_rate_limit;
#[cfg(feature = "websocket")]
pub(crate) mod websocket;

pub(crate) use error::{Error, ensure_peer_ready};
pub(crate) use proxy::BoxedClient;
pub use proxy::ClientEventsProxy;
pub(crate) use types::HostIncomingMsg;
pub use types::{AuthToken, ClientId, HostResult, OpenRequest, RequestId};

use either::Either;
use freenet_stdlib::{
    client_api::{
        ClientError, ClientRequest, ContractRequest, ContractResponse, ErrorKind, HostResponse,
        QueryResponse,
    },
    prelude::*,
};
use futures::stream::FuturesUnordered;
use futures::{FutureExt, StreamExt, future::BoxFuture};
use std::convert::Infallible;
use tracing::Instrument;

use crate::contract::{ClientResponsesReceiver, ContractHandlerEvent};
use crate::message::{NodeEvent, QueryResult};
use crate::node::OpManager;
use crate::operations::{OpError, get, put, update};
use crate::ring::KnownPeerKeyLocation;
use crate::tracing::NetEventLog;
use crate::{config::GlobalExecutor, contract::StoreResponse};

use crate::contract::{contains_debug_sections, debug_sections};

/// Helper function to register a subscription listener for GET/PUT operations with auto-subscribe
async fn register_subscription_listener(
    op_manager: &OpManager,
    instance_id: ContractInstanceId,
    client_id: ClientId,
    subscription_listener: mpsc::Sender<HostResult>,
    operation_type: &str,
) -> Result<(), Error> {
    tracing::debug!(
        client_id = %client_id,
        contract = %instance_id,
        operation = operation_type,
        "Registering subscription listener"
    );
    let register_listener = op_manager
        .notify_contract_handler_prioritized(
            ContractHandlerEvent::RegisterSubscriberListener {
                key: instance_id,
                client_id,
                summary: None, // No summary for GET/PUT-based subscriptions
                subscriber_listener: subscription_listener,
            },
            crate::contract::Priority::ClientLocal,
        )
        .await
        .inspect_err(|err| {
            tracing::error!(
                client_id = %client_id,
                contract = %instance_id,
                operation = operation_type,
                error = %err,
                "Register subscriber listener failed"
            );
        });
    match register_listener {
        Ok(ContractHandlerEvent::RegisterSubscriberListenerResponse { result: Ok(()) }) => {
            tracing::debug!(
                client_id = %client_id,
                contract = %instance_id,
                operation = operation_type,
                "Subscriber listener registered successfully"
            );
            // Register client subscription to prevent upstream unsubscription while this client is active
            let result = op_manager
                .ring
                .add_client_subscription(&instance_id, client_id);
            // Emit telemetry if this was the first client (hosting started)
            if result.is_first_client {
                if let Some(event) = NetEventLog::hosting_started(&op_manager.ring, instance_id) {
                    op_manager.ring.register_events(Either::Left(event)).await;
                }
            }
            Ok(())
        }
        // #4681: registration was rejected (e.g. subscriber-limit reached).
        // Forward the underlying RequestError so the client sees the real
        // reason instead of a generic "unexpected op state".
        Ok(ContractHandlerEvent::RegisterSubscriberListenerResponse { result: Err(e) }) => {
            tracing::error!(
                client_id = %client_id,
                contract = %instance_id,
                operation = operation_type,
                error = %e,
                phase = "registration_failed",
                "Subscriber listener registration rejected"
            );
            Err(Error::Registration(e))
        }
        _ => {
            tracing::error!(
                client_id = %client_id,
                contract = %instance_id,
                operation = operation_type,
                phase = "registration_failed",
                "Subscriber listener registration failed"
            );
            Err(Error::Op(OpError::UnexpectedOpState))
        }
    }
}

/// serve-DURING gate (#4642 R3 piece C — `hosting-invariants.md` invariant 1).
///
/// Decides whether a client GET is answered from this node's LOCAL copy
/// immediately, or routed through the network. The originator serves its best
/// local copy the instant it holds a FRESH one — it never blocks a read on a
/// network round-trip to confirm freshness (serve-DURING).
///
/// Serve locally when we have valid local state (`local_satisfies_request`) AND
/// any of:
/// 1. **No connections** — an isolated node can only answer from local state.
/// 2. **Actively receiving updates** (`is_subscribed`) — a network/client
///    subscription keeps the copy fresh.
/// 3. **Local interest** (`has_local_interest`) — this node is a fresh in-mesh
///    HOST of the contract. `has_local_interest` is true for a demandless
///    every-hop hosting copy (it registers `hosting = true` and joins the
///    InterestSync anti-entropy heartbeat — #4744 piece B), a local-client-
///    subscribed copy, or a copy with a downstream subscriber. This is the SAME
///    predicate the TERMINUS serve gate uses (`check_local_with_interest_gate`
///    → `InterestManager::has_local_interest`), so the originator no longer
///    routes a whole GET for a copy it already holds fresh.
///
/// Term 3 REPLACES the pre-serve-DURING `is_hosting_contract &&
/// has_local_client_access` gate, which served only copies the LOCAL user had
/// touched and forced a demandless hosted copy to route the whole GET — the
/// exact "goes dark on a read it could answer" gap serve-DURING closes.
/// Freshness is NOT confirmed here; it rides on the anti-entropy mesh
/// (invariant 1), and an isolated holder serves best-effort at the ~5-9%
/// near-miss floor. `local_satisfies_request` already guarantees valid state is
/// present, so a phantom (interested-but-stateless, #4440) copy is excluded even
/// though it can register interest via a downstream subscriber.
///
/// Do NOT re-add `is_hosting_contract` (the in-memory hosting-cache membership)
/// as a serve term — see .claude/rules/hosting-invariants.md (invariant 1). A
/// hosting-cache copy is NOT necessarily in the update mesh: on restart the
/// cache is restored but the InterestManager is not, so `is_hosting_contract`
/// can be true while the copy has no freshening path. Serving on it would
/// re-introduce the #3698 stale-serve bug. The correct fix for restored copies
/// is to rehydrate `has_local_interest` at startup
/// (`OpManager::rehydrate_local_hosting_interest`, #4780), which registers the
/// copy into anti-entropy so `has_local_interest` (term 3) is a true
/// fresh-in-mesh signal — NOT to widen this gate.
fn should_serve_local_copy(
    local_satisfies_request: bool,
    connection_count: usize,
    is_subscribed: bool,
    has_local_interest: bool,
) -> bool {
    local_satisfies_request && (connection_count == 0 || is_subscribed || has_local_interest)
}

/// Report an operation init failure to the client via the result router.
async fn report_op_init_error(
    op_manager: &OpManager,
    tx: crate::message::Transaction,
    contract: &(impl std::fmt::Display + Sync),
    op_name: &str,
    err: &OpError,
    client_id: ClientId,
    request_id: RequestId,
) {
    tracing::error!(
        client_id = %client_id,
        request_id = %request_id,
        tx = %tx,
        contract = %contract,
        error = %err,
        phase = "error",
        "{op_name} request failed"
    );

    // Convert ring errors to type-safe ErrorKind variants so that downstream
    // consumers (e.g. the HTTP handler's SERVICE_UNAVAILABLE page) can match on
    // them instead of relying on error message string contents.
    let error_kind = match err {
        OpError::RingError(crate::ring::RingError::EmptyRing) => ErrorKind::EmptyRing,
        OpError::RingError(crate::ring::RingError::PeerNotJoined) => ErrorKind::PeerNotJoined,
        // Admission-gate rejection during graceful shutdown — surface
        // as the existing `Shutdown` kind so clients see the same
        // typed reason as a mid-flight cancellation. Without an
        // explicit arm here the match is non-exhaustive (CI fails),
        // so this also serves as the source-of-truth for the
        // user-visible shape of `OpError::NodeShuttingDown`.
        OpError::NodeShuttingDown => ErrorKind::Shutdown,
        // Phase 7 egress self-block (#4300): a local client originated a
        // request for a contract this node has banned. Surface as a
        // typed `OperationError` (no dedicated wire variant exists in
        // stdlib, and adding one would be a wire-format change requiring
        // a separate stdlib-first release) so the client sees a clear,
        // contract-named reason instead of a silent timeout.
        OpError::ContractBanned { .. }
        | OpError::RingError(crate::ring::RingError::ConnError(_))
        | OpError::RingError(crate::ring::RingError::NoHostingPeers(_))
        | OpError::ConnError(_)
        | OpError::ContractError(_)
        | OpError::ExecutorError(_)
        | OpError::UnexpectedOpState
        | OpError::InvalidStateTransition { .. }
        | OpError::NotificationError
        | OpError::PeerDisconnected { .. }
        | OpError::NotificationChannelError(_)
        | OpError::IncorrectTxType(..)
        | OpError::OpNotPresent(_)
        | OpError::StreamCancelled
        | OpError::OrphanStreamClaimFailed => ErrorKind::OperationError {
            cause: format!("{op_name} operation failed: {err}").into(),
        },
    };

    let error_response = Err(error_kind.into());

    if let Err(e) = op_manager.result_router_tx.try_send((tx, error_response)) {
        tracing::error!(
            tx = %tx,
            error = %e,
            "Failed to send {op_name} error to result router \
             (channel full or closed)"
        );
    }

    // Clean up request router so subsequent requests for the same resource
    // create a fresh operation instead of reusing this failed transaction.
    // Without this, the resource→transaction mapping persists and new clients
    // get stale cached errors indefinitely (see diagnostic report 8TSMXY).
    op_manager.completed(tx);
}

/// Process client events.
///
/// # Architecture: Dual-Mode Client Handling
///
/// This function operates in one of two modes based on `op_manager.actor_clients`:
///
/// - Uses ResultRouter → SessionActor for centralized client communication
/// - Uses RequestRouter for operation deduplication (multiple clients share one operation)
/// - More scalable and efficient for concurrent clients
pub async fn client_event_handling<ClientEv>(
    op_manager: Arc<OpManager>,
    mut client_events: ClientEv,
    mut client_responses: ClientResponsesReceiver,
    node_controller: tokio::sync::mpsc::Sender<NodeEvent>,
) -> anyhow::Result<Infallible>
where
    ClientEv: ClientEventsProxy + Send + 'static,
{
    let request_router = std::sync::Arc::new(crate::node::RequestRouter::new());
    // Register the router with op_manager so completed operations clean up stale entries.
    // Without this, subsequent requests for the same resource would hang forever.
    op_manager.set_request_router(request_router.clone());
    let request_router = Some(request_router);
    let mut results = FuturesUnordered::new();
    loop {
        // Uses deterministic_select! for DST - guards are evaluated BEFORE futures are created
        crate::deterministic_select! {
            client_request = client_events.recv() => {
                let req = match client_request {
                    Ok(request) => {
                        tracing::debug!(
                            client_id = %request.client_id,
                            request_id = %request.request_id,
                            request_type = ?request.request,
                            "Received client request"
                        );
                        request
                    }
                    Err(error) if matches!(error.kind(), ErrorKind::Shutdown) => {
                        node_controller.send(NodeEvent::Disconnect { cause: None }).await.ok();
                        anyhow::bail!("shutdown event");
                    }
                    Err(error) if matches!(error.kind(), ErrorKind::TransportProtocolDisconnect) => {
                        // A single client disconnecting is not fatal — continue serving
                        // other clients. The combinator already cleaned up the dead slot.
                        tracing::debug!(error = %error, "Client transport disconnected");
                        continue;
                    }
                    Err(error) => {
                        tracing::debug!(error = %error, "Client error");
                        continue;
                    }
                };
                let cli_id = req.client_id;
                let res = process_open_request(req, op_manager.clone(), request_router.clone()).await;
                results.push(async move {
                    match res.await {
                        Ok(Some(Either::Left(res))) => (cli_id, Ok(Some(res))),
                        Ok(Some(Either::Right(mut cb))) => {
                            match cb.recv().await {
                                Some(res) => (cli_id, Ok(Some(res))),
                                None => (cli_id, Err(ClientError::from(ErrorKind::ChannelClosed))),
                            }
                        }
                        Ok(None) => (cli_id, Ok(None)),
                        Err(Error::Disconnected) => {
                            tracing::debug!(client_id = %cli_id, "Client disconnected");
                            (cli_id, Err(ClientError::from(ErrorKind::Disconnect)))
                        }
                        Err(Error::PeerNotJoined) => {
                            tracing::warn!(
                                client_id = %cli_id,
                                "Operation rejected: peer has not joined network yet - client should retry after join"
                            );
                            (cli_id, Err(ErrorKind::PeerNotJoined.into()))
                        }
                        Err(Error::EmptyRing) => {
                            tracing::warn!(
                                client_id = %cli_id,
                                "Operation rejected: no ring connections found - client should retry after connections are established"
                            );
                            (cli_id, Err(ErrorKind::EmptyRing.into()))
                        }
                        Err(err) => {
                            tracing::error!(
                                client_id = %cli_id,
                                error = %err,
                                "Operation error"
                            );
                            (cli_id, Err(ErrorKind::OperationError { cause: format!("{err}").into() }.into()))
                        }
                    }
                });
            },
            res = client_responses.recv() => {
                if let Some((cli_id, request_id, res)) = res {
                    if let Ok(result) = &res {
                        tracing::debug!(
                            client_id = %cli_id,
                            request_id = %request_id,
                            response = %result,
                            "Sending client response"
                        );
                    }
                    if let Err(err) = client_events.send(cli_id, res).await {
                        tracing::debug!(
                            client_id = %cli_id,
                            error = %err,
                            "Client channel closed, response dropped"
                        );
                    }
                }
            },
            res = results.next(), if !results.is_empty() => {
                let Some(f_res) = res else {
                    unreachable!("results.next() should only return None if results is empty, which is guarded against");
                };
                match f_res {
                    (cli_id, Ok(Some(res))) => {
                        let res = match res {
                            QueryResult::Connections(conns) => {
                                // Connected peers must have known addresses - use type-safe conversion
                                Ok(HostResponse::QueryResponse(QueryResponse::ConnectedPeers {
                                    peers: conns.into_iter().filter_map(|p| {
                                        KnownPeerKeyLocation::try_from(&p).ok().map(|known| {
                                            (p.pub_key.to_string(), known.socket_addr())
                                        })
                                    }).collect() }
                                ))
                            }
                            QueryResult::GetResult { key, state, contract } => {
                                Ok(HostResponse::ContractResponse(ContractResponse::GetResponse {
                                    key,
                                    state,
                                    contract,
                                }))
                            }
                            QueryResult::DelegateResult { response, .. } => {
                                response
                            }
                            QueryResult::NetworkDebug(debug_info) => {
                                // Convert internal types to stdlib types
                                let subscriptions = debug_info.application_subscriptions.into_iter().map(|sub| {
                                    freenet_stdlib::client_api::SubscriptionInfo {
                                        contract_key: sub.instance_id,
                                        client_id: sub.client_id.into(),
                                    }
                                }).collect();

                                // Connected peers must have known addresses - use type-safe conversion
                                let connected_peers = debug_info.connected_peers.into_iter().filter_map(|peer| {
                                    KnownPeerKeyLocation::try_from(&peer).ok().map(|known| {
                                        (peer.to_string(), known.socket_addr())
                                    })
                                }).collect();

                                Ok(HostResponse::QueryResponse(QueryResponse::NetworkDebug(
                                    freenet_stdlib::client_api::NetworkDebugInfo {
                                        subscriptions,
                                        connected_peers,
                                    }
                                )))
                            }
                            QueryResult::NodeDiagnostics(response) => {
                                Ok(HostResponse::QueryResponse(QueryResponse::NodeDiagnostics(response)))
                            }
                        };
                        if let Ok(result) = &res {
                            tracing::debug!(
                                client_id = %cli_id,
                                response = %result,
                                "Sending client operation response"
                            );
                        }
                        if let Err(err) = client_events.send(cli_id, res).await {
                            tracing::debug!(
                                client_id = %cli_id,
                                error = %err,
                                "Client channel closed, operation response dropped"
                            );
                        }
                    }
                    (_, Ok(None)) => continue,
                    (cli_id, Err(err)) => {
                        tracing::error!(
                            client_id = %cli_id,
                            error = %err,
                            "Sending error response to client"
                        );
                        if let Err(send_err) = client_events.send(cli_id, Err(err)).await {
                            tracing::debug!(
                                client_id = %cli_id,
                                error = %send_err,
                                "Client channel closed, error response dropped"
                            );
                        }
                    }
                }
            },
        }
    }
}

#[inline]
async fn process_open_request(
    mut request: OpenRequest<'static>,
    op_manager: Arc<OpManager>,
    request_router: Option<Arc<crate::node::RequestRouter>>,
) -> BoxFuture<'static, Result<Option<Either<QueryResult, mpsc::Receiver<QueryResult>>>, Error>> {
    let (callback_tx, callback_rx) = if matches!(
        &*request.request,
        ClientRequest::NodeQueries(_) | ClientRequest::ContractOp(ContractRequest::Get { .. })
    ) {
        let (tx, rx) = mpsc::channel(1);
        (Some(tx), Some(rx))
    } else {
        (None, None)
    };

    // TODO: wait until we have a peer_id to attempt (should be connected)
    // this will indirectly start actions on the local contract executor
    let fut = async move {
        let client_id = request.client_id;
        let request_id = request.request_id;

        let subscription_listener: Option<mpsc::Sender<HostResult>> =
            request.notification_channel.take();

        match *request.request {
            ClientRequest::ContractOp(ops) => {
                match ops {
                    ContractRequest::Put {
                        state,
                        contract,
                        related_contracts,
                        subscribe,
                        blocking_subscribe,
                    } => {
                        let peer_id = ensure_peer_ready(&op_manager)?;

                        tracing::debug!(
                            client_id = %client_id,
                            request_id = %request_id,
                            peer = %peer_id,
                            phase = "request",
                            "Received PUT request from client"
                        );

                        let contract_key = contract.key();

                        // Driver handles both local-only and network
                        // PUTs: calls put_contract locally, finds
                        // peers, sends the request. Each task owns
                        // its own operation lifecycle.
                        let client_tx = crate::message::Transaction::new::<put::PutMsg>();

                        op_manager
                            .ch_outbound
                            .waiting_for_transaction_result(client_tx, client_id, request_id)
                            .await
                            .inspect_err(|err| {
                                tracing::error!(
                                    client_id = %client_id,
                                    request_id = %request_id,
                                    tx = %client_tx,
                                    error = %err,
                                    "Error waiting for transaction result"
                                )
                            })?;

                        // Reject debug-compiled contracts BEFORE routing
                        // (#2257). Debug WASM carries DWARF `.debug_*`
                        // custom sections and is typically 10-100x larger
                        // than release builds, which can blow past
                        // WebSocket message-size limits and surface as a
                        // confusing "Message too long" transport error.
                        // Failing here gives the client an actionable
                        // "recompile with --release" message instead.
                        if contains_debug_sections(contract.data()) {
                            // Re-scan only on the (rare) rejection path to
                            // name the offending sections in the error.
                            let detected = debug_sections(contract.data());
                            let err = OpError::ContractError(
                                crate::contract::ContractError::DebugWasmRejected {
                                    sections: detected.join(", "),
                                },
                            );
                            report_op_init_error(
                                &op_manager,
                                client_tx,
                                &contract_key,
                                "PUT",
                                &err,
                                client_id,
                                request_id,
                            )
                            .await;
                            return Ok(None);
                        }

                        if subscribe {
                            if let Some(sl) = subscription_listener {
                                register_subscription_listener(
                                    &op_manager,
                                    *contract_key.id(),
                                    client_id,
                                    sl,
                                    "PUT",
                                )
                                .await?;
                            } else {
                                tracing::warn!(
                                    client_id = %client_id,
                                    contract = %contract_key,
                                    "PUT with subscribe=true but no subscription_listener"
                                );
                            }
                        }

                        if let Err(err) = put::op_ctx_task::start_client_put(
                            op_manager.clone(),
                            client_tx,
                            contract,
                            related_contracts,
                            state,
                            op_manager.ring.max_hops_to_live,
                            subscribe,
                            blocking_subscribe,
                        )
                        .await
                        {
                            report_op_init_error(
                                &op_manager,
                                client_tx,
                                &contract_key,
                                "PUT",
                                &err,
                                client_id,
                                request_id,
                            )
                            .await;
                        }
                    }
                    ContractRequest::Update { key, data } => {
                        let peer_id = ensure_peer_ready(&op_manager)?;

                        tracing::debug!(
                            client_id = %client_id,
                            request_id = %request_id,
                            peer = %peer_id,
                            contract = %key,
                            phase = "request",
                            "Received UPDATE request from client"
                        );

                        let related_contracts = RelatedContracts::default();

                        tracing::debug!(
                            client_id = %client_id,
                            request_id = %request_id,
                            peer = %peer_id,
                            contract = %key,
                            data = ?data,
                            phase = "starting",
                            "Starting UPDATE operation - passing delta to network layer"
                        );

                        // Convert UpdateData to 'static lifetime for storage in operation state.
                        // This is safe because we're cloning the underlying bytes.
                        let update_data: UpdateData<'static> = match data {
                            UpdateData::State(s) => UpdateData::State(State::from(s.into_bytes())),
                            UpdateData::Delta(d) => {
                                UpdateData::Delta(StateDelta::from(d.into_bytes()))
                            }
                            UpdateData::StateAndDelta { state, delta } => {
                                UpdateData::StateAndDelta {
                                    state: State::from(state.into_bytes()),
                                    delta: StateDelta::from(delta.into_bytes()),
                                }
                            }
                            UpdateData::RelatedState { related_to, state } => {
                                UpdateData::RelatedState {
                                    related_to,
                                    state: State::from(state.into_bytes()),
                                }
                            }
                            UpdateData::RelatedDelta { related_to, delta } => {
                                UpdateData::RelatedDelta {
                                    related_to,
                                    delta: StateDelta::from(delta.into_bytes()),
                                }
                            }
                            UpdateData::RelatedStateAndDelta {
                                related_to,
                                state,
                                delta,
                            } => UpdateData::RelatedStateAndDelta {
                                related_to,
                                state: State::from(state.into_bytes()),
                                delta: StateDelta::from(delta.into_bytes()),
                            },
                            // `UpdateData` is `#[non_exhaustive]` since
                            // stdlib 0.6.0. Future variants reach this
                            // arm because the compiler requires it; until
                            // they are explicitly handled (each variant
                            // has its own owned-bytes conversion), reject
                            // them here rather than silently dropping the
                            // payload further down the operation pipeline.
                            other => {
                                tracing::error!(
                                    client_id = %client_id,
                                    request_id = %request_id,
                                    contract = %key,
                                    variant = ?std::mem::discriminant(&other),
                                    "Rejecting UPDATE: unknown UpdateData variant — \
                                     freenet-core was built against an older stdlib than \
                                     the client expected; rebuild the host to handle this variant"
                                );
                                return Err(Error::Node(
                                    "UPDATE rejected: unknown UpdateData variant from client; \
                                     rebuild freenet-core against the stdlib version emitting \
                                     this variant"
                                        .to_string(),
                                ));
                            }
                        };

                        tracing::debug!(
                            client_id = %client_id,
                            request_id = %request_id,
                            peer = %peer_id,
                            contract = %key,
                            phase = "sending",
                            "Sending UPDATE operation to network layer"
                        );

                        if let Some(router) = &request_router {
                            tracing::debug!(
                                client_id = %client_id,
                                request_id = %request_id,
                                peer = %peer_id,
                                contract = %key,
                                phase = "routing",
                                "Routing UPDATE request through deduplication layer"
                            );

                            let request = crate::node::DeduplicatedRequest::Update {
                                key,
                                update_data: update_data.clone(),
                                related_contracts: related_contracts.clone(),
                                client_id,
                                request_id,
                            };

                            let (transaction_id, should_start_operation) =
                                router.route_request(request).await.map_err(|e| {
                                    Error::Node(format!("Request routing failed: {}", e))
                                })?;

                            // Always register this client for the result
                            op_manager
                                .ch_outbound
                                .waiting_for_transaction_result(
                                    transaction_id,
                                    client_id,
                                    request_id,
                                )
                                .await
                                .inspect_err(|err| {
                                    tracing::error!(
                                        "Error waiting for transaction result: {}",
                                        err
                                    );
                                })?;

                            // Only start new network operation if this is a new operation
                            if should_start_operation {
                                tracing::debug!(
                                    client_id = %client_id,
                                    request_id = %request_id,
                                    tx = %transaction_id,
                                    peer = %peer_id,
                                    contract = %key,
                                    phase = "new_operation",
                                    "Starting new UPDATE network operation"
                                );

                                tracing::debug!(
                                    request_id = %request_id,
                                    transaction_id = %transaction_id,
                                    operation = "update",
                                    "Request-Transaction correlation"
                                );

                                match update::op_ctx_task::start_client_update(
                                    op_manager.clone(),
                                    transaction_id,
                                    key,
                                    update_data.clone(),
                                    related_contracts,
                                )
                                .await
                                {
                                    Ok(_) => {}
                                    Err(err) => {
                                        report_op_init_error(
                                            &op_manager,
                                            transaction_id,
                                            &key,
                                            "UPDATE",
                                            &err,
                                            client_id,
                                            request_id,
                                        )
                                        .await;
                                    }
                                }
                            } else {
                                tracing::debug!(
                                    client_id = %client_id,
                                    request_id = %request_id,
                                    tx = %transaction_id,
                                    peer = %peer_id,
                                    contract = %key,
                                    phase = "reuse",
                                    "Reusing existing UPDATE operation - client registered for result"
                                );
                            }
                        } else {
                            tracing::debug!(
                                client_id = %client_id,
                                request_id = %request_id,
                                peer = %peer_id,
                                contract = %key,
                                phase = "legacy",
                                "Starting direct UPDATE operation (legacy mode)"
                            );

                            // Legacy mode: direct operation without deduplication
                            let op_id = crate::message::Transaction::new::<update::UpdateMsg>();

                            tracing::debug!(
                                request_id = %request_id,
                                transaction_id = %op_id,
                                operation = "update",
                                "Request-Transaction correlation"
                            );

                            op_manager
                                .ch_outbound
                                .waiting_for_transaction_result(op_id, client_id, request_id)
                                .await
                                .inspect_err(|err| {
                                    tracing::error!(
                                        "Error waiting for transaction result: {}",
                                        err
                                    );
                                })?;

                            if let Err(err) = update::op_ctx_task::start_client_update(
                                op_manager.clone(),
                                op_id,
                                key,
                                update_data,
                                related_contracts,
                            )
                            .await
                            {
                                report_op_init_error(
                                    &op_manager,
                                    op_id,
                                    &key,
                                    "UPDATE",
                                    &err,
                                    client_id,
                                    request_id,
                                )
                                .await;
                            }
                        }
                    }
                    ContractRequest::Get {
                        key,
                        return_contract_code,
                        subscribe,
                        blocking_subscribe,
                    } => {
                        // Try local cache before requiring ring join for non-subscribe GETs.
                        let peer_id = match ensure_peer_ready(&op_manager) {
                            Ok(id) => id,
                            Err(err) if !subscribe => {
                                // Not joined yet — check if we can serve from local cache
                                let local_result = op_manager
                                    .notify_contract_handler_prioritized(
                                        ContractHandlerEvent::GetQuery {
                                            instance_id: key,
                                            return_contract_code,
                                        },
                                        crate::contract::Priority::ClientLocal,
                                    )
                                    .await;
                                if let Ok(ContractHandlerEvent::GetResponse {
                                    key: Some(full_key),
                                    response:
                                        Ok(StoreResponse {
                                            state: Some(state),
                                            contract,
                                        }),
                                }) = local_result
                                {
                                    if !return_contract_code || contract.is_some() {
                                        tracing::info!(
                                            client_id = %client_id,
                                            contract = %full_key,
                                            phase = "local_cache_pre_join",
                                            "Serving locally cached contract state before network join"
                                        );
                                        return Ok(Some(Either::Left(QueryResult::GetResult {
                                            key: full_key,
                                            state,
                                            contract,
                                        })));
                                    }
                                }
                                // No local cache — propagate the original error
                                return Err(err);
                            }
                            Err(err) => return Err(err),
                        };

                        // Query local store first. We use the result in two cases:
                        // 1. Error handling: if local storage has issues, fail fast
                        // 2. No connections: if isolated (no peers), return local cache immediately
                        //
                        // For connected nodes, we use smart cache routing: return local cache
                        // if subscribed (cache is fresh), otherwise fetch from network.
                        // See PR #2388 for why always-local-first was problematic.
                        let (full_key, state, contract) = match op_manager
                            .notify_contract_handler_prioritized(
                                ContractHandlerEvent::GetQuery {
                                    instance_id: key,
                                    return_contract_code,
                                },
                                crate::contract::Priority::ClientLocal,
                            )
                            .await
                        {
                            Ok(ContractHandlerEvent::GetResponse {
                                key: Some(full_key),
                                response: Ok(StoreResponse { state, contract }),
                            }) => (Some(full_key), state, contract),
                            Ok(ContractHandlerEvent::GetResponse {
                                key: None,
                                response:
                                    Ok(StoreResponse {
                                        state: None,
                                        contract: None,
                                    }),
                            }) => (None, None, None), // Contract not found locally
                            Ok(ContractHandlerEvent::GetResponse {
                                response: Err(err), ..
                            }) => {
                                tracing::error!(
                                    client_id = %client_id,
                                    request_id = %request_id,
                                    %key,
                                    error = %err,
                                    phase = "error",
                                    "GET query failed (executor error)"
                                );
                                return Err(Error::Executor(err));
                            }
                            Err(err) => {
                                tracing::error!(
                                    client_id = %client_id,
                                    request_id = %request_id,
                                    %key,
                                    error = %err,
                                    phase = "error",
                                    "GET query failed (contract error)"
                                );
                                return Err(Error::Contract(err));
                            }
                            Ok(_) => {
                                tracing::error!(
                                    client_id = %client_id,
                                    request_id = %request_id,
                                    %key,
                                    phase = "error",
                                    "GET query failed (unexpected state)"
                                );
                                return Err(Error::Op(OpError::UnexpectedOpState));
                            }
                        };

                        // Determine whether to route through network or return local cache.
                        //
                        // If we're actively receiving updates (is_receiving_updates), our
                        // local cache is fresh and we can return it directly. Otherwise,
                        // fetch from network to avoid serving stale state.
                        // See PR #2388 (original fix) and #3340 (LRU staleness fix).
                        let connection_count = op_manager.ring.open_connections();
                        let has_local_state = full_key.is_some() && state.is_some();
                        let local_satisfies_request =
                            has_local_state && (!return_contract_code || contract.is_some());

                        // Only return local cache if we're actively receiving updates
                        // (network subscription or client subscriptions). The hosting
                        // LRU cache alone is insufficient — contracts can outlive their
                        // subscriptions, leaving stale state (see #3340).
                        let is_subscribed = full_key
                            .as_ref()
                            .map(|k| op_manager.ring.is_receiving_updates(k))
                            .unwrap_or(false);

                        // Mark as locally accessed (#3769) and refresh hosting TTL.
                        if let Some(ref fk) = full_key {
                            op_manager.ring.mark_local_client_access(fk);
                            if op_manager.ring.is_hosting_contract(fk) {
                                op_manager.ring.touch_hosting(fk);
                            }
                        }

                        // serve-DURING (#4642 R3 piece C): serve immediately when
                        // this node HAS LOCAL INTEREST in the contract — i.e. it is
                        // a fresh in-mesh host. `has_local_interest` is true for a
                        // demandless every-hop copy (registers `hosting = true`,
                        // joins anti-entropy — #4744 piece B), a locally-subscribed
                        // copy, or one with a downstream subscriber. This is the
                        // SAME predicate the TERMINUS serve gate uses
                        // (`check_local_with_interest_gate`), replacing the old
                        // `has_local_client_access` gate that forced a demandless
                        // hosted copy to route a whole GET through the network.
                        let has_local_interest = full_key
                            .as_ref()
                            .map(|k| op_manager.interest_manager.has_local_interest(k))
                            .unwrap_or(false);

                        // Return local cache if we have valid state AND any of:
                        // 1. No connections (isolated node)
                        // 2. Actively subscribed (cache kept fresh via updates)
                        // 3. We hold a fresh in-mesh copy (serve-DURING)
                        if should_serve_local_copy(
                            local_satisfies_request,
                            connection_count,
                            is_subscribed,
                            has_local_interest,
                        ) {
                            let full_key = full_key.unwrap();
                            let state = state.unwrap();

                            tracing::info!(
                                client_id = %client_id,
                                request_id = %request_id,
                                peer = %peer_id,
                                contract = %full_key,
                                is_subscribed,
                                has_local_interest,
                                connection_count,
                                phase = "local_cache",
                                "Returning locally cached contract state"
                            );

                            // #4642 A3 hit-rate instrumentation: this client GET
                            // was answered from local hosted state (a hit).
                            op_manager.ring.record_get_served_locally();

                            // Handle subscription for locally found contracts
                            if subscribe {
                                if let Some(subscription_listener) = subscription_listener {
                                    register_subscription_listener(
                                        &op_manager,
                                        *full_key.id(),
                                        client_id,
                                        subscription_listener,
                                        "local GET",
                                    )
                                    .await?;
                                } else {
                                    // Expected for HTTP web endpoint which sets subscribe=true
                                    // but has no notification channel. The subscription is
                                    // handled at the node level, not the client level.
                                    tracing::debug!(
                                        client_id = %client_id,
                                        contract = %full_key,
                                        "GET with subscribe=true but no subscription_listener (expected for HTTP clients)"
                                    );
                                }

                                // serve-DURING (#4642 R3 piece C): a subscribe=true GET
                                // served from a LOCAL copy did NOT route through the
                                // network, which is where the subscribe normally rides
                                // (`maybe_subscribe_child`, the sole GET subscribe
                                // path). If we were not already receiving updates (a
                                // demandless every-hop copy), establish the upstream
                                // subscription so the client gets ongoing updates. The
                                // state itself was already served from the LOCAL copy
                                // above (serve-DURING) regardless of the flag — only the
                                // subscribe-registration wait differs here.
                                //
                                // `blocking_subscribe` is the client's real wire-API flag
                                // and is LOAD-BEARING (#4524): when `true`, the served read
                                // is held until the upstream subscription is registered, so
                                // the client cannot see the `GetResponse` before its
                                // downstream-subscriber chain exists (else an UPDATE sent
                                // immediately after the response would race registration and
                                // be missed — the exact bug #4524 fixed). Hardcoding `false`
                                // here would silently downgrade an explicit
                                // `blocking_subscribe=true` GET to fire-and-forget the moment
                                // serve-DURING fires — a regression vs the network path,
                                // which passes the real flag. When `false`, the subscribe is
                                // spawned in the background and the served read returns
                                // immediately.
                                //
                                // A subscribe=FALSE read establishes NOTHING: no durable
                                // demand is created, the demandless copy stays fresh via
                                // anti-entropy and is NEVER re-rooted (re-root is
                                // connection-drop-driven — P0 / #4642 piece F — and its
                                // interest gate refuses a demandless copy), so a burst of
                                // serve-DURING reads fans out ZERO re-links. This kick
                                // fires only on an EXPLICIT client subscribe — the
                                // client-subscribe origination path has no per-contract
                                // dedup, so re-link volume is bounded by explicit client
                                // subscribes plus the `!is_subscribed` gate (which skips a
                                // redundant subscribe when an upstream already exists), not
                                // by internal dedup. It therefore cannot become a read storm.
                                if !is_subscribed {
                                    get::op_ctx_task::maybe_subscribe_child(
                                        &op_manager,
                                        crate::message::Transaction::new::<get::GetMsg>(),
                                        full_key,
                                        true,               // subscribe
                                        blocking_subscribe, // honor the client's #4524 flag
                                    )
                                    .await;
                                }
                            }

                            return Ok(Some(Either::Left(QueryResult::GetResult {
                                key: full_key,
                                state,
                                contract,
                            })));
                        }

                        // #4642 A3 hit-rate instrumentation: this client GET could
                        // not be answered locally and is routed to the network
                        // (a forward/miss).
                        op_manager.ring.record_get_forwarded();

                        // Driver owns its routing state in task
                        // locals and calls notify_contract_handler
                        // locally as needed.
                        tracing::debug!(
                            client_id = %client_id,
                            request_id = %request_id,
                            peer = %peer_id,
                            contract = %key,
                            has_local = has_local_state,
                            is_subscribed,
                            connection_count,
                            phase = "network_routing",
                            "Routing GET request through network"
                        );

                        let client_tx = crate::message::Transaction::new::<get::GetMsg>();

                        op_manager
                            .ch_outbound
                            .waiting_for_transaction_result(client_tx, client_id, request_id)
                            .await
                            .inspect_err(|err| {
                                tracing::error!(
                                    client_id = %client_id,
                                    request_id = %request_id,
                                    tx = %client_tx,
                                    error = %err,
                                    "Error waiting for transaction result"
                                )
                            })?;

                        if subscribe {
                            if let Some(sl) = subscription_listener {
                                register_subscription_listener(
                                    &op_manager,
                                    key,
                                    client_id,
                                    sl,
                                    "GET",
                                )
                                .await?;
                            } else {
                                tracing::warn!(
                                    client_id = %client_id,
                                    contract = %key,
                                    "GET with subscribe=true but no subscription_listener"
                                );
                            }
                        }

                        // `report_op_init_error` takes a &ContractKey, so we
                        // synthesize one from the instance_id for the error
                        // path (the full key isn't known until a Response).
                        let key_for_err = full_key.unwrap_or_else(|| {
                            ContractKey::from_id_and_code(
                                key,
                                freenet_stdlib::prelude::CodeHash::new([0u8; 32]),
                            )
                        });
                        if let Err(err) = get::op_ctx_task::start_client_get(
                            op_manager.clone(),
                            client_tx,
                            key,
                            return_contract_code,
                            subscribe,
                            blocking_subscribe,
                        )
                        .await
                        {
                            report_op_init_error(
                                &op_manager,
                                client_tx,
                                &key_for_err,
                                "GET",
                                &err,
                                client_id,
                                request_id,
                            )
                            .await;
                        }
                    }
                    ContractRequest::Subscribe { key, summary } => {
                        let peer_id = ensure_peer_ready(&op_manager)?;

                        tracing::debug!(
                            client_id = %client_id,
                            request_id = %request_id,
                            peer = %peer_id,
                            contract = %key,
                            phase = "request",
                            "Received SUBSCRIBE request from client"
                        );

                        // Reject Subscribe if the contract WASM isn't cached locally.
                        // Without WASM, the node can't validate or apply updates,
                        // leading to a "subscribed but can't update" state.
                        // Clients must PUT or GET first (any GET will cache WASM
                        // internally regardless of return_contract_code, see #3757).
                        //
                        // Note: This only guards explicit ContractRequest::Subscribe.
                        // GET+subscribe=true and PUT+subscribe=true bypass this check
                        // because those operations inherently fetch/provide the WASM.
                        match op_manager
                            .notify_contract_handler_prioritized(
                                crate::contract::ContractHandlerEvent::GetQuery {
                                    instance_id: key,
                                    return_contract_code: true,
                                },
                                crate::contract::Priority::ClientLocal,
                            )
                            .await
                        {
                            Ok(crate::contract::ContractHandlerEvent::GetResponse {
                                response:
                                    Ok(crate::contract::StoreResponse {
                                        state: Some(_),
                                        contract: Some(_),
                                    }),
                                ..
                            }) => {
                                // Contract WASM and state are cached locally, proceed
                            }
                            Ok(crate::contract::ContractHandlerEvent::GetResponse { .. }) => {
                                tracing::warn!(
                                    client_id = %client_id,
                                    request_id = %request_id,
                                    contract = %key,
                                    "Rejecting SUBSCRIBE: contract WASM not cached locally. \
                                     PUT the contract or GET the contract first."
                                );
                                return Err(Error::Node(format!(
                                    "Cannot subscribe to contract {key}: contract WASM/parameters \
                                     not cached locally. PUT the contract or GET the contract \
                                     before subscribing."
                                )));
                            }
                            Err(err) => {
                                tracing::error!(
                                    client_id = %client_id,
                                    request_id = %request_id,
                                    contract = %key,
                                    error = %err,
                                    "Contract handler error while checking WASM for SUBSCRIBE"
                                );
                                return Err(Error::Node(format!(
                                    "Cannot subscribe to contract {key}: \
                                     failed to query contract store: {err}"
                                )));
                            }
                            Ok(unexpected) => {
                                tracing::error!(
                                    client_id = %client_id,
                                    request_id = %request_id,
                                    contract = %key,
                                    "Unexpected contract handler response for SUBSCRIBE WASM check: {unexpected:?}"
                                );
                                return Err(Error::Node(format!(
                                    "Cannot subscribe to contract {key}: \
                                     unexpected contract handler response"
                                )));
                            }
                        }

                        let Some(subscriber_listener) = subscription_listener else {
                            tracing::error!(
                                client_id = %client_id,
                                request_id = %request_id,
                                contract = %key,
                                "No subscriber listener for SUBSCRIBE request"
                            );
                            return Ok(None);
                        };

                        let register_listener = op_manager
                            .notify_contract_handler_prioritized(
                                ContractHandlerEvent::RegisterSubscriberListener {
                                    key,
                                    client_id,
                                    summary,
                                    subscriber_listener,
                                },
                                crate::contract::Priority::ClientLocal,
                            )
                            .await
                            .inspect_err(|err| {
                                tracing::error!(
                                    client_id = %client_id,
                                    request_id = %request_id,
                                    contract = %key,
                                    error = %err,
                                    "Register subscriber listener error"
                                );
                            });
                        match register_listener {
                            Ok(ContractHandlerEvent::RegisterSubscriberListenerResponse {
                                result: Ok(()),
                            }) => {
                                tracing::debug!(
                                    client_id = %client_id,
                                    request_id = %request_id,
                                    contract = %key,
                                    phase = "listener_registered",
                                    "Subscriber listener registered successfully"
                                );
                                // Register client subscription to enable subscription tree pruning on disconnect
                                let result =
                                    op_manager.ring.add_client_subscription(&key, client_id);
                                // Emit telemetry if this was the first client (hosting started)
                                if result.is_first_client {
                                    if let Some(event) =
                                        NetEventLog::hosting_started(&op_manager.ring, key)
                                    {
                                        op_manager.ring.register_events(Either::Left(event)).await;
                                    }
                                }
                            }
                            // #4681: registration rejected (e.g. subscriber-limit
                            // reached). Forward the underlying RequestError so the
                            // client sees the real reason instead of a generic
                            // "unexpected op state".
                            Ok(ContractHandlerEvent::RegisterSubscriberListenerResponse {
                                result: Err(e),
                            }) => {
                                tracing::error!(
                                    client_id = %client_id,
                                    request_id = %request_id,
                                    contract = %key,
                                    error = %e,
                                    phase = "registration_failed",
                                    "Subscriber listener registration rejected"
                                );
                                return Err(Error::Registration(e));
                            }
                            _ => {
                                tracing::error!(
                                    client_id = %client_id,
                                    request_id = %request_id,
                                    contract = %key,
                                    phase = "registration_failed",
                                    "Subscriber listener registration failed"
                                );
                                return Err(Error::Op(OpError::UnexpectedOpState));
                            }
                        }

                        // Now start the network subscription operation
                        // SUBSCRIBE: Skip router deduplication due to instant-completion race conditions
                        // When contracts are local, Subscribe completes instantly which breaks deduplication:
                        // - Client 1 subscribes → operation completes → result delivered → TX removed
                        // - Client 2 subscribes → tries to reuse TX → but TX already gone
                        // Solution: Each client gets their own Subscribe operation (they're lightweight)
                        if let Some(_router) = &request_router {
                            tracing::debug!(
                                client_id = %client_id,
                                request_id = %request_id,
                                peer = %peer_id,
                                contract = %key,
                                phase = "no_dedup",
                                "Processing SUBSCRIBE without deduplication (instant-completion race avoidance)"
                            );

                            // Create operation with new transaction ID
                            let tx = crate::message::Transaction::new::<
                                crate::operations::subscribe::SubscribeMsg,
                            >();

                            // CRITICAL: Register BEFORE starting operation to avoid race with instant-completion
                            use crate::contract::WaitingTransaction;
                            op_manager
                                .ch_outbound
                                .waiting_for_transaction_result(
                                    WaitingTransaction::Transaction(tx),
                                    client_id,
                                    request_id,
                                )
                                .await
                                .inspect_err(|err| {
                                    tracing::error!(
                                        "Error waiting for transaction result: {}",
                                        err
                                    );
                                })?;

                            // Start dedicated operation for this
                            // client AFTER registration.
                            // `subscribe_with_id` is
                            // client-initiated-only; no `is_renewal`.
                            let _result_tx = crate::node::subscribe_with_id(
                                op_manager.clone(),
                                key,
                                None, // No legacy registration
                                Some(tx),
                            )
                            .await
                            .inspect_err(|err| {
                                tracing::error!(
                                    client_id = %client_id,
                                    request_id = %request_id,
                                    tx = %tx,
                                    contract = %key,
                                    error = %err,
                                    phase = "error",
                                    "SUBSCRIBE operation failed"
                                );
                            })?;

                            tracing::debug!(
                                request_id = %request_id,
                                transaction_id = %tx,
                                operation = "subscribe",
                                "SUBSCRIBE operation started with dedicated transaction for this client"
                            );
                        } else {
                            tracing::debug!(
                                peer_id = %peer_id,
                                key = %key,
                                "Starting direct SUBSCRIBE operation",
                            );

                            // Generate transaction, register first, then run op
                            let tx = crate::message::Transaction::new::<
                                crate::operations::subscribe::SubscribeMsg,
                            >();

                            op_manager
                                .ch_outbound
                                .waiting_for_transaction_result(tx, client_id, request_id)
                                .await
                                .inspect_err(|err| {
                                    tracing::error!(
                                        "Error waiting for transaction result: {}",
                                        err
                                    );
                                })?;

                            // `subscribe_with_id` is client-initiated-only; no `is_renewal` parameter.
                            crate::node::subscribe_with_id(op_manager.clone(), key, None, Some(tx))
                                .await
                                .inspect_err(|err| {
                                    tracing::error!(
                                        client_id = %client_id,
                                        request_id = %request_id,
                                        tx = %tx,
                                        contract = %key,
                                        error = %err,
                                        phase = "error",
                                        "SUBSCRIBE operation failed"
                                    );
                                })?;

                            tracing::debug!(
                                request_id = %request_id,
                                transaction_id = %tx,
                                operation = "subscribe",
                                "Request-Transaction correlation"
                            );
                        }
                    }
                    _ => {
                        tracing::error!(
                            client_id = %client_id,
                            request_id = %request_id,
                            "Unsupported contract operation"
                        );
                    }
                }
            }
            ClientRequest::DelegateOp(req) => {
                tracing::debug!(
                    client_id = %client_id,
                    request_id = %request_id,
                    phase = "request",
                    "Received delegate operation from client"
                );
                let delegate_key = req.key().clone();

                // Register (or refresh) this app's routing path so the delegate
                // can push notification-driven ApplicationMessages back to it
                // (#3275). An app "registers with a delegate" by talking to it
                // over a connection that carries an async notification channel;
                // any ApplicationMessages request with such a channel establishes
                // the path. UnregisterDelegate tears it down. RegisterDelegate
                // (installing the delegate binary) does NOT register an app —
                // it's an admin op, not an app conversation.
                match &req {
                    freenet_stdlib::client_api::DelegateRequest::ApplicationMessages { .. } => {
                        if let Some(sender) = &subscription_listener {
                            if !crate::contract::delegate_app_registry::register_app(
                                &delegate_key,
                                client_id,
                                sender.clone(),
                            ) {
                                tracing::warn!(
                                    %client_id,
                                    delegate = %delegate_key,
                                    "App not registered for delegate notifications (registry at capacity)"
                                );
                            }
                        }
                    }
                    freenet_stdlib::client_api::DelegateRequest::UnregisterDelegate(key) => {
                        crate::contract::delegate_app_registry::remove_delegate(key);
                    }
                    // RegisterDelegate installs a delegate binary (admin op), not
                    // an app conversation, so it registers no routing path. The
                    // wildcard also absorbs future `#[non_exhaustive]` variants;
                    // it exists ONLY to satisfy non_exhaustive (see
                    // git-workflow.md) — new app-facing variants must be handled
                    // explicitly above, not swept here.
                    #[allow(clippy::wildcard_enum_match_arm)]
                    freenet_stdlib::client_api::DelegateRequest::RegisterDelegate { .. } | _ => {}
                }

                // Derive a short discriminant tag for the INFO logs, but only when INFO is
                // enabled — the Vec<&str> collect + format! would otherwise allocate on every
                // delegate dispatch even when the log line is suppressed.
                let msg_type: Option<String> = if tracing::enabled!(tracing::Level::INFO) {
                    Some(match &req {
                        freenet_stdlib::client_api::DelegateRequest::ApplicationMessages {
                            inbound,
                            ..
                        } => {
                            // Include the count of inbound message variants for observability.
                            // Each variant name is a short discriminant so operators can tell
                            // ApplicationMessage (from app) apart from GetContractResponse etc.
                            let tags: Vec<&str> = inbound
                                .iter()
                                .map(|m| match m {
                                    InboundDelegateMsg::ApplicationMessage(_) => {
                                        "ApplicationMessage"
                                    }
                                    InboundDelegateMsg::UserResponse(_) => "UserResponse",
                                    InboundDelegateMsg::GetContractResponse(_) => {
                                        "GetContractResponse"
                                    }
                                    InboundDelegateMsg::PutContractResponse(_) => {
                                        "PutContractResponse"
                                    }
                                    InboundDelegateMsg::UpdateContractResponse(_) => {
                                        "UpdateContractResponse"
                                    }
                                    InboundDelegateMsg::SubscribeContractResponse(_) => {
                                        "SubscribeContractResponse"
                                    }
                                    InboundDelegateMsg::ContractNotification(_) => {
                                        "ContractNotification"
                                    }
                                    InboundDelegateMsg::DelegateMessage(_) => "DelegateMessage",
                                    _ => "Unknown",
                                })
                                .collect();
                            // Format as "ApplicationMessages[ApplicationMessage,DelegateMessage]"
                            // when there are inbound messages, or bare "ApplicationMessages" when empty.
                            if tags.is_empty() {
                                "ApplicationMessages".to_string()
                            } else {
                                format!("ApplicationMessages[{}]", tags.join(","))
                            }
                        }
                        freenet_stdlib::client_api::DelegateRequest::RegisterDelegate {
                            ..
                        } => "RegisterDelegate".to_string(),
                        freenet_stdlib::client_api::DelegateRequest::UnregisterDelegate(_) => {
                            "UnregisterDelegate".to_string()
                        }
                        _ => "Unknown".to_string(),
                    })
                } else {
                    None
                };
                if let Some(ref mt) = msg_type {
                    tracing::info!(
                        delegate = %delegate_key,
                        msg_type = %mt,
                        request_id = %request_id,
                        outcome = "queued",
                        "DelegateRequest dispatch"
                    );
                }
                let origin_contract = request.origin_contract;
                // Per-connection user secret namespace (hosted mode). Taken from
                // the OpenRequest, which received it from the connection layer —
                // NOT from anything inside `req`. Moving it into the event keeps
                // it on a channel the delegate/client cannot reach.
                let user_context = request.user_context;

                let res = match op_manager
                    .notify_contract_handler_prioritized(
                        ContractHandlerEvent::DelegateRequest {
                            req,
                            origin_contract,
                            user_context,
                        },
                        crate::contract::Priority::ClientLocal,
                    )
                    .await
                {
                    Ok(ContractHandlerEvent::DelegateResponse(res)) => {
                        if let Some(ref mt) = msg_type {
                            tracing::info!(
                                delegate = %delegate_key,
                                msg_type = %mt,
                                request_id = %request_id,
                                outcome = "executed",
                                "DelegateRequest dispatch"
                            );
                        }
                        res
                    }
                    Err(err) => {
                        tracing::error!(
                            client_id = %client_id,
                            request_id = %request_id,
                            delegate = %delegate_key,
                            error = %err,
                            phase = "error",
                            "Delegate operation failed (contract error)"
                        );
                        return Err(Error::Contract(err));
                    }
                    Ok(_) => {
                        tracing::error!(
                            client_id = %client_id,
                            request_id = %request_id,
                            delegate = %delegate_key,
                            phase = "error",
                            "Delegate operation failed (unexpected state)"
                        );
                        return Err(Error::Op(OpError::UnexpectedOpState));
                    }
                };

                let host_response = Ok(HostResponse::DelegateResponse {
                    key: delegate_key.clone(),
                    values: res,
                });

                if let Some(ch) = &subscription_listener {
                    match ch.try_send(host_response) {
                        Ok(()) => {}
                        Err(tokio::sync::mpsc::error::TrySendError::Full(_)) => {
                            tracing::warn!(
                                client_id = %client_id,
                                request_id = %request_id,
                                delegate = %delegate_key,
                                "Subscription channel full — delegate response dropped"
                            );
                        }
                        Err(tokio::sync::mpsc::error::TrySendError::Closed(_)) => {
                            tracing::error!(
                                client_id = %client_id,
                                request_id = %request_id,
                                delegate = %delegate_key,
                                "Failed to send delegate response — subscription channel closed"
                            );
                        }
                    }
                    return Ok(None);
                }

                // Return the response to be sent by client_event_handling
                return Ok(Some(Either::Left(QueryResult::DelegateResult {
                    key: delegate_key,
                    response: host_response,
                })));
            }
            ClientRequest::Disconnect { .. } => {
                tracing::debug!(
                    client_id = %client_id,
                    request_id = %request_id,
                    "Received disconnect request from client, triggering subscription cleanup"
                );

                // Drop any delegate-notification routing registrations for this
                // client (#3275).
                crate::contract::delegate_app_registry::remove_client(client_id);

                let result = op_manager
                    .ring
                    .remove_client_from_all_subscriptions(client_id);
                for contract in &result.affected_contracts {
                    op_manager.interest_manager.remove_local_client(contract);
                }
                if !result.affected_contracts.is_empty() {
                    tracing::debug!(
                        %client_id,
                        subscriptions_cleaned = result.affected_contracts.len(),
                        "Cleaned up client subscriptions and interest tracking"
                    );
                }

                // Send Unsubscribe upstream for contracts with no remaining interest.
                // FLIP (keystone P6, #4642): the collapse decision is driven by the
                // reconcile controller's strict-farther interest gate
                // (`reconcile_wants_collapse` = `!contract_in_use`), replacing the
                // legacy ANY-downstream `should_unsubscribe_upstream`. The teardown
                // still targets the STORED upstream (narrow flip keeps the flag).
                for contract in &result.affected_contracts {
                    if op_manager.reconcile_wants_collapse(
                        contract,
                        crate::node::network_status::ReconcileShadowSite::Collapse,
                    ) {
                        let op_mgr = op_manager.clone();
                        let contract = *contract;
                        GlobalExecutor::spawn(async move {
                            op_mgr.send_unsubscribe_upstream(&contract).await;
                        });
                    }
                }

                // Notify contract handler to clean up shared_summaries and
                // shared_notifications for this client (fire-and-forget — no response needed).
                if let Err(err) = op_manager.ch_outbound.send_to_handler_fire_and_forget(
                    ContractHandlerEvent::ClientDisconnect { client_id },
                ) {
                    tracing::warn!(
                        %client_id,
                        error = %err,
                        "Failed to notify contract handler of client disconnect"
                    );
                }
            }
            ClientRequest::NodeQueries(query) => {
                tracing::debug!(
                    client_id = %client_id,
                    request_id = %request_id,
                    query = ?query,
                    "Received node query from client"
                );

                let Some(tx) = callback_tx else {
                    tracing::error!(
                        client_id = %client_id,
                        request_id = %request_id,
                        "callback_tx not available for NodeQueries"
                    );
                    unreachable!(
                        "callback_tx should always be Some for NodeQueries based on initialization logic"
                    );
                };

                let node_event = match query {
                    freenet_stdlib::client_api::NodeQuery::ConnectedPeers => {
                        NodeEvent::QueryConnections { callback: tx }
                    }
                    freenet_stdlib::client_api::NodeQuery::SubscriptionInfo => {
                        NodeEvent::QuerySubscriptions { callback: tx }
                    }
                    freenet_stdlib::client_api::NodeQuery::NodeDiagnostics { config } => {
                        NodeEvent::QueryNodeDiagnostics {
                            config,
                            callback: tx,
                        }
                    }
                    freenet_stdlib::client_api::NodeQuery::NeighborHostingInfo => {
                        // TODO: Implement neighbor hosting info query
                        tracing::warn!(
                            client_id = %client_id,
                            request_id = %request_id,
                            "NeighborHostingInfo query not yet implemented"
                        );
                        return Ok(None);
                    }
                };

                if let Err(err) = op_manager.notify_node_event(node_event).await {
                    tracing::error!(
                        client_id = %client_id,
                        request_id = %request_id,
                        error = %err,
                        "notify_node_event error"
                    );
                    return Err(Error::from(err));
                }

                return Ok(Some(Either::Right(callback_rx.unwrap())));
            }
            ClientRequest::Close => {
                return Err(Error::Disconnected);
            }
            ClientRequest::Authenticate { .. } | _ => {
                tracing::error!(
                    client_id = %client_id,
                    request_id = %request_id,
                    "Unsupported operation"
                );
            }
        }
        Ok(None)
    };

    GlobalExecutor::spawn(fut.instrument(tracing::info_span!(
        parent: tracing::Span::current(),
        "process_client_request"
    )))
    .map(|res| match res {
        Ok(Ok(res)) => Ok(res),
        Ok(Err(err)) => Err(err),
        Err(err) => {
            tracing::error!(
                error = %err,
                "Error processing client request (task panic)"
            );
            Err(Error::from(err))
        }
    })
    .boxed()
}

// Re-export mpsc so submodules that use `super::mpsc` still resolve correctly
use tokio::sync::mpsc;
// Re-export Arc for use in this module
use std::sync::Arc;

#[cfg(test)]
mod serve_during_gate_tests {
    use super::should_serve_local_copy;

    // Ring distinctions used below (from `LocalInterest::is_interested` +
    // `is_receiving_updates`): a demandless every-hop hosting copy has
    // `has_local_interest = true` (hosting flag) but `is_subscribed = false`.

    /// serve-DURING falsifier (#4642 R3 piece C): a DEMANDLESS but interested
    /// hosted copy — `has_local_interest = true`, `is_subscribed = false` — on a
    /// node WITH connections MUST be served locally. Before serve-DURING the
    /// third gate term was `is_hosting_contract && has_local_client_access`,
    /// which is `false` for a demandless copy (no local client ever touched it),
    /// so the whole GET was routed through the network — the "goes dark on a read
    /// it could answer" gap. This asserts the flipped behavior; it fails against
    /// the pre-serve-DURING client-access gate.
    #[test]
    fn demandless_interested_copy_serves_locally_with_connections() {
        assert!(
            should_serve_local_copy(
                /* local_satisfies_request */ true, /* connection_count       */ 8,
                /* is_subscribed           */ false, /* has_local_interest      */ true,
            ),
            "a demandless-but-interested hosted copy must serve locally (serve-DURING), \
             not route through the network"
        );
    }

    /// A copy with NO local interest and NO subscription on a connected node must
    /// still route (we have no fresh in-mesh copy to serve). Guards against
    /// over-broadening the gate to serve any stored bytes regardless of mesh
    /// membership (which would re-introduce stale-serve, invariant 1).
    #[test]
    fn uninterested_copy_forwards_with_connections() {
        assert!(
            !should_serve_local_copy(
                /* local_satisfies_request */ true, /* connection_count       */ 8,
                /* is_subscribed           */ false, /* has_local_interest      */ false,
            ),
            "a stored copy with no local interest and no subscription must route \
             (serve-DURING serves only a fresh in-mesh copy)"
        );
    }

    /// Never serve when local state does not satisfy the request, regardless of
    /// interest/subscription/connectivity.
    #[test]
    fn missing_local_state_never_serves() {
        for is_subscribed in [false, true] {
            for has_local_interest in [false, true] {
                for connection_count in [0usize, 8] {
                    assert!(
                        !should_serve_local_copy(
                            /* local_satisfies_request */ false,
                            connection_count,
                            is_subscribed,
                            has_local_interest,
                        ),
                        "must not serve without valid local state \
                         (is_subscribed={is_subscribed}, \
                         has_local_interest={has_local_interest}, \
                         connection_count={connection_count})"
                    );
                }
            }
        }
    }

    /// The three pre-existing serve reasons are preserved: isolated node
    /// (no connections), active subscription, and the new local-interest term.
    #[test]
    fn each_serve_reason_holds_independently() {
        // Isolated node: serves purely on zero connections.
        assert!(should_serve_local_copy(true, 0, false, false));
        // Actively receiving updates.
        assert!(should_serve_local_copy(true, 8, true, false));
        // Fresh in-mesh host (serve-DURING).
        assert!(should_serve_local_copy(true, 8, false, true));
    }

    /// #4524 regression guard (source-scrape): the serve-DURING subscribe kick
    /// MUST forward the client's real `blocking_subscribe` flag to
    /// `maybe_subscribe_child`, NOT a hardcoded `false`.
    ///
    /// `blocking_subscribe` is load-bearing (#4524): when `true` the served read
    /// must be held until the upstream subscription is registered, so the client
    /// cannot see the `GetResponse` before its downstream-subscriber chain exists
    /// (else an UPDATE sent immediately after the response races registration and
    /// is missed). Hardcoding `false` here silently downgrades an explicit
    /// `blocking_subscribe=true` GET to fire-and-forget the moment serve-DURING
    /// fires — a regression vs the network path, which passes the real flag.
    ///
    /// This deterministically FAILS on the pre-fix code (the fifth positional
    /// arg was `false`) and PASSES once the real flag is forwarded. It mirrors
    /// the codebase's source-scrape pin idiom (e.g.
    /// `maybe_subscribe_child_short_circuits_on_false` in get/op_ctx_task.rs).
    #[test]
    fn serve_during_kick_forwards_real_blocking_subscribe_flag() {
        const SOURCE: &str = include_str!("client_events.rs");
        let call_start = SOURCE
            .find("get::op_ctx_task::maybe_subscribe_child(")
            .expect("serve-DURING kick must call maybe_subscribe_child");
        // Slice the call up to its terminating `.await`.
        let call_tail = &SOURCE[call_start..];
        let call_end = call_tail
            .find(".await")
            .expect("maybe_subscribe_child call must be awaited");
        let call = &call_tail[..call_end];

        // Strip line comments so the pre-fix comment ("// blocking_subscribe:
        // never block the served read") cannot mask the hardcoded `false`.
        let args_only: String = call
            .lines()
            .map(|line| line.split("//").next().unwrap_or(""))
            .collect::<Vec<_>>()
            .join("\n");

        assert!(
            args_only.contains("blocking_subscribe"),
            "serve-DURING kick must forward the client's real `blocking_subscribe` \
             flag to maybe_subscribe_child (#4524), not a hardcoded value. \
             Call args: {args_only:?}"
        );
        assert!(
            !args_only.contains("false"),
            "serve-DURING kick must NOT pass a hardcoded `false` for \
             blocking_subscribe — that silently downgrades an explicit \
             blocking_subscribe=true GET to fire-and-forget (#4524 regression). \
             Call args: {args_only:?}"
        );
    }
}
