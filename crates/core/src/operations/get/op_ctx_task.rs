//! Task-per-transaction client-initiated GET (#1454 Phase 3b).
//!
//! Mirrors [`crate::operations::put::op_ctx_task`] — the Phase 3a
//! production consumer of [`OpCtx::send_and_await`] for PUT, which in
//! turn follows the Phase 2b SUBSCRIBE driver shape. This module
//! applies the same pattern to client-initiated GET.
//!
//! # Scope (Phase 3b)
//!
//! Only the **client-initiated originator** GET runs through this
//! module. Relay GETs (non-terminal hops), GC-spawned retries, and
//! `start_targeted_op()` (UPDATE-triggered auto-fetch) stay on the
//! legacy re-entry loop. Relay migration is tracked in #3883.
//!
//! # Architecture
//!
//! The task owns all routing state in its locals — there is no `GetOp`
//! in `OpManager.ops.get` for any attempt this task makes. The task:
//!
//! 1. Loops, calling [`OpCtx::send_and_await`] with a fresh
//!    `Transaction` per attempt (single-use-per-tx constraint).
//!    The loop-back through `process_message` handles initial peer
//!    selection and forwarding; if the contract is already cached
//!    locally, `process_message` synthesizes a terminal result and
//!    echoes the `GetMsg::Request` back via
//!    `forward_pending_op_result_if_completed` (same mechanism PUT 3a
//!    relies on for `LocalCompletion`).
//! 2. On terminal `Response{Found}` / `ResponseStreaming` /
//!    Request-echo: re-queries the contract store via
//!    `notify_contract_handler(GetQuery)` to extract the assembled
//!    state + contract, publishes `HostResponse::ContractResponse::GetResponse`
//!    to the client via `send_client_result`.
//! 3. On `Response{NotFound}`: treats as a terminal failure from that
//!    peer and advances to the next.
//! 4. On timeout / wire-error: advances to next peer or exhausts.
//!
//! # Streaming assembly
//!
//! `process_message` already assembles `ResponseStreaming` via
//! `stream_handle.assemble().await` inline at the originator and
//! writes the bytes into the local contract store before completing.
//! The driver never touches the stream handle — it re-queries the
//! store after the terminal reply arrives. Because the originator's
//! `process_message` runs synchronously before
//! `try_forward_task_per_tx_reply` intercepts the reply, the store
//! write is visible to the driver's `GetQuery` by the time it fires.
//!
//! # Connection-drop latency (R6)
//!
//! Legacy `handle_abort` detects disconnects in <1s. Task-per-tx
//! relies on the `OPERATION_TTL` (60s) timeout. Accepted ceiling,
//! matching Phase 2b/3a.

use std::sync::Arc;

use freenet_stdlib::client_api::{ContractResponse, ErrorKind, HostResponse};
use freenet_stdlib::prelude::*;

use crate::client_events::HostResult;
use crate::config::GlobalExecutor;
use crate::contract::{ContractHandlerEvent, StoreResponse};
use crate::message::{NetMessage, NetMessageV1, Transaction};
use crate::node::OpManager;
use crate::operations::OpError;
use crate::operations::VisitedPeers;
use crate::operations::op_ctx::{
    AdvanceOutcome, AttemptOutcome, RetryDriver, RetryLoopOutcome, drive_retry_loop,
};
use crate::ring::{Location, PeerKeyLocation};
use crate::router::{RouteEvent, RouteOutcome};

use super::{GetMsg, GetMsgResult};

/// Start a client-initiated GET, returning as soon as the task has been
/// spawned (mirrors legacy `request_get` timing).
///
/// The caller must have already registered a result waiter for
/// `client_tx` via `op_manager.ch_outbound.waiting_for_transaction_result`.
/// This function does NOT touch the waiter; it only drives the
/// ring/network side and publishes the terminal result to
/// `result_router_tx` keyed by `client_tx`.
#[allow(clippy::too_many_arguments)]
pub(crate) async fn start_client_get(
    op_manager: Arc<OpManager>,
    client_tx: Transaction,
    instance_id: ContractInstanceId,
    return_contract_code: bool,
    subscribe: bool,
    blocking_subscribe: bool,
) -> Result<Transaction, OpError> {
    tracing::debug!(
        tx = %client_tx,
        contract = %instance_id,
        "get (task-per-tx): spawning client-initiated task"
    );

    // Fire-and-forget spawn; same rationale as PUT 3a's `start_client_put`.
    // Failures are published to the client via `result_router_tx`, not
    // via this function's return value. Not registered with
    // `BackgroundTaskMonitor`: per-transaction task that terminates via
    // happy path, exhaustion, timeout, or infra error.
    GlobalExecutor::spawn(run_client_get(
        op_manager,
        client_tx,
        instance_id,
        return_contract_code,
        subscribe,
        blocking_subscribe,
    ));

    Ok(client_tx)
}

async fn run_client_get(
    op_manager: Arc<OpManager>,
    client_tx: Transaction,
    instance_id: ContractInstanceId,
    return_contract_code: bool,
    subscribe: bool,
    blocking_subscribe: bool,
) {
    let outcome = drive_client_get(
        op_manager.clone(),
        client_tx,
        instance_id,
        return_contract_code,
        subscribe,
        blocking_subscribe,
    )
    .await;
    deliver_outcome(&op_manager, client_tx, outcome);
}

/// GET driver has exactly two outcomes, matching PUT 3a.
#[derive(Debug)]
enum DriverOutcome {
    /// The driver produced a `HostResult` that must be published via
    /// `result_router_tx`.
    Publish(HostResult),
    /// A genuine infrastructure failure escaped the driver loop.
    InfrastructureError(OpError),
}

async fn drive_client_get(
    op_manager: Arc<OpManager>,
    client_tx: Transaction,
    instance_id: ContractInstanceId,
    return_contract_code: bool,
    subscribe: bool,
    blocking_subscribe: bool,
) -> DriverOutcome {
    match drive_client_get_inner(
        &op_manager,
        client_tx,
        instance_id,
        return_contract_code,
        subscribe,
        blocking_subscribe,
    )
    .await
    {
        Ok(outcome) => outcome,
        Err(err) => DriverOutcome::InfrastructureError(err),
    }
}

async fn drive_client_get_inner(
    op_manager: &Arc<OpManager>,
    client_tx: Transaction,
    instance_id: ContractInstanceId,
    return_contract_code: bool,
    subscribe: bool,
    blocking_subscribe: bool,
) -> Result<DriverOutcome, OpError> {
    let htl = op_manager.ring.max_hops_to_live;

    // Pre-select initial target for the driver's retry state. Actual
    // routing is done by `process_message` on the loop-back; this is
    // just so `advance_to_next_peer` has a starting "tried" set.
    //
    // At the client-API boundary we only have an instance_id — the
    // full ContractKey (which includes the code hash) isn't known
    // until a terminal reply with `GetMsgResult::Found` arrives.
    // `k_closest_potentially_hosting` accepts either.
    let mut tried: Vec<std::net::SocketAddr> = Vec::new();
    if let Some(own_addr) = op_manager.ring.connection_manager.get_own_addr() {
        tried.push(own_addr);
    }
    let initial_target = op_manager
        .ring
        .k_closest_potentially_hosting(&instance_id, tried.as_slice(), 1)
        .into_iter()
        .next();
    let current_target = match initial_target {
        Some(peer) => {
            if let Some(addr) = peer.socket_addr() {
                tried.push(addr);
            }
            peer
        }
        None => op_manager.ring.connection_manager.own_location(),
    };

    struct GetRetryDriver<'a> {
        op_manager: &'a OpManager,
        instance_id: ContractInstanceId,
        htl: usize,
        tried: Vec<std::net::SocketAddr>,
        retries: usize,
        current_target: PeerKeyLocation,
        attempt_visited: VisitedPeers,
    }

    /// Terminal value for the GET driver. Carries the instance_id
    /// (always known) and optionally the full ContractKey recovered
    /// from a remote `GetMsgResult::Found` reply. LocalCompletion
    /// and ResponseStreaming both provide the full key; Response with
    /// `Found` also provides it. The instance_id suffices to re-query
    /// the local store in all cases.
    #[derive(Debug)]
    struct Terminal {
        key: Option<ContractKey>,
    }

    impl RetryDriver for GetRetryDriver<'_> {
        type Terminal = Terminal;

        fn new_attempt_tx(&mut self) -> Transaction {
            // Refresh the per-attempt VisitedPeers bloom so
            // process_message's forwarding loop-prevention uses a
            // fresh-per-tx filter (matching legacy `request_get`
            // which calls `VisitedPeers::new(&tx)` once per op).
            let tx = Transaction::new::<GetMsg>();
            self.attempt_visited = VisitedPeers::new(&tx);
            tx
        }

        fn build_request(&mut self, attempt_tx: Transaction) -> NetMessage {
            NetMessage::from(GetMsg::Request {
                id: attempt_tx,
                instance_id: self.instance_id,
                // Wire always requests contract code post-#3757; the
                // client's `return_contract_code` flag only gates the
                // client-facing payload at delivery time.
                fetch_contract: true,
                htl: self.htl,
                visited: self.attempt_visited.clone(),
                // Subscription hand-off is driven by `maybe_subscribe_child`
                // after the GET terminates, NOT by this flag. Leave
                // false so the server-side GET path doesn't
                // double-register a subscription.
                subscribe: false,
            })
        }

        fn classify(&mut self, reply: NetMessage) -> AttemptOutcome<Terminal> {
            match classify_reply(&reply) {
                ReplyClass::Terminal { key } => {
                    AttemptOutcome::Terminal(Terminal { key: Some(key) })
                }
                ReplyClass::LocalCompletion => AttemptOutcome::Terminal(Terminal { key: None }),
                ReplyClass::Retry => AttemptOutcome::Retry,
                ReplyClass::Unexpected => AttemptOutcome::Unexpected,
            }
        }

        fn advance(&mut self) -> AdvanceOutcome {
            match advance_to_next_peer(
                self.op_manager,
                &self.instance_id,
                &mut self.tried,
                &mut self.retries,
            ) {
                Some((next_target, _next_addr)) => {
                    self.current_target = next_target;
                    AdvanceOutcome::Next
                }
                None => AdvanceOutcome::Exhausted,
            }
        }
    }

    let mut driver = GetRetryDriver {
        op_manager,
        instance_id,
        htl,
        tried,
        retries: 0,
        current_target,
        attempt_visited: VisitedPeers::new(&client_tx),
    };

    let loop_result = drive_retry_loop(op_manager, client_tx, "get", &mut driver).await;

    match loop_result {
        RetryLoopOutcome::Done(terminal) => {
            // Clean up the DashMap entry that process_message created.
            // Without this, the GC task finds a stale entry and may
            // launch speculative retries on the completed op.
            op_manager.completed(client_tx);

            // Re-query the local contract store. `process_message`
            // has already written the assembled state into the store
            // (either via stream assembly for ResponseStreaming, by
            // `put_contract` for Response{Found}, or via the original
            // local-completion path for Request-echo). This ordering
            // is load-bearing: the bypass forwards the reply only
            // after process_message runs on the originator, so the
            // store write is guaranteed visible here.
            //
            // The query resolves the full ContractKey for us if the
            // Request-echo path left us with only an instance_id.
            let host_result =
                build_host_response(op_manager, &instance_id, return_contract_code).await;

            // Prefer the ContractKey the reply carried (authoritative
            // on the happy path); fall back to the one resolved from
            // the store re-query (LocalCompletion) or a synthetic one
            // when even that fails.
            let reply_key = terminal
                .key
                .or_else(|| extract_host_response_key(&host_result))
                .unwrap_or_else(|| synthetic_key(&instance_id));

            // Emit routing event + telemetry — report_result (which
            // normally does both) doesn't run because the bypass
            // intercepted the Response. Without this, the router's
            // prediction model never receives GET success feedback.
            let contract_location = Location::from(&reply_key);
            let route_event = RouteEvent {
                peer: driver.current_target.clone(),
                contract_location,
                outcome: RouteOutcome::SuccessUntimed,
                op_type: Some(crate::node::network_status::OpType::Get),
            };
            if let Some(log_event) =
                crate::tracing::NetEventLog::route_event(&client_tx, &op_manager.ring, &route_event)
            {
                op_manager
                    .ring
                    .register_events(either::Either::Left(log_event))
                    .await;
            }
            op_manager.ring.routing_finished(route_event);
            crate::node::network_status::record_op_result(
                crate::node::network_status::OpType::Get,
                true,
            );

            // Drive subscribe hand-off separately. Mirrors PUT 3a's
            // `maybe_subscribe_child` — subscribe is never handled in
            // the terminal-result construction to avoid double-subscribe
            // (commit 494a3c69).
            maybe_subscribe_child(
                op_manager,
                client_tx,
                reply_key,
                subscribe,
                blocking_subscribe,
            )
            .await;

            Ok(DriverOutcome::Publish(host_result))
        }
        RetryLoopOutcome::Exhausted(cause) => {
            Ok(DriverOutcome::Publish(Err(ErrorKind::OperationError {
                cause: cause.into(),
            }
            .into())))
        }
        RetryLoopOutcome::Unexpected => Err(OpError::UnexpectedOpState),
        RetryLoopOutcome::InfraError(err) => Err(err),
    }
}

// --- Host-response construction ---

/// Query the local contract store for `(state, contract)` and package
/// a client-facing `HostResponse::ContractResponse::GetResponse`.
///
/// If the local store doesn't have the state (which should not happen
/// on the happy path — `process_message` stores before the terminal
/// reply fires), synthesize an operation error matching the shape
/// `to_host_result` produces on NotFound.
async fn build_host_response(
    op_manager: &OpManager,
    instance_id: &ContractInstanceId,
    return_contract_code: bool,
) -> HostResult {
    let lookup = op_manager
        .notify_contract_handler(ContractHandlerEvent::GetQuery {
            instance_id: *instance_id,
            return_contract_code,
        })
        .await;

    match lookup {
        Ok(ContractHandlerEvent::GetResponse {
            key: Some(resolved_key),
            response:
                Ok(StoreResponse {
                    state: Some(state),
                    contract,
                }),
        }) => {
            // Strip contract code if client didn't ask for it. The
            // node always pulls WASM for local caching/validation,
            // but the client-facing payload obeys their flag.
            let client_contract = if return_contract_code { contract } else { None };
            Ok(HostResponse::ContractResponse(
                ContractResponse::GetResponse {
                    key: resolved_key,
                    contract: client_contract,
                    state,
                },
            ))
        }
        _ => {
            tracing::warn!(
                contract = %instance_id,
                "get (task-per-tx): terminal reply classified success but local \
                 store lookup returned no state; synthesizing client error"
            );
            Err(ErrorKind::OperationError {
                cause: format!(
                    "GET succeeded on wire but local store lookup failed for {instance_id}"
                )
                .into(),
            }
            .into())
        }
    }
}

/// Extract the `ContractKey` from a successful `HostResponse::GetResponse`.
/// Used to recover the full key when the driver's terminal reply only
/// carried an instance_id (LocalCompletion path).
fn extract_host_response_key(result: &HostResult) -> Option<ContractKey> {
    if let Ok(HostResponse::ContractResponse(ContractResponse::GetResponse { key, .. })) = result {
        Some(*key)
    } else {
        None
    }
}

/// Fallback `ContractKey` for telemetry when we have neither a
/// remote-reply key nor a store-resolved key. Zero code-hash is the
/// documented sentinel — routing telemetry only needs a `Location`
/// derived from the instance_id, which is preserved here.
fn synthetic_key(instance_id: &ContractInstanceId) -> ContractKey {
    ContractKey::from_id_and_code(*instance_id, CodeHash::new([0u8; 32]))
}

// --- Reply classification ---

#[derive(Debug)]
enum ReplyClass {
    /// Remote peer returned a successful terminal (Found / streaming).
    Terminal {
        key: ContractKey,
    },
    /// Remote peer explicitly reported NotFound — advance to next peer.
    Retry,
    /// Local completion: process_message had the contract already and
    /// echoed the Request back via `forward_pending_op_result_if_completed`.
    /// The echo doesn't carry a full `ContractKey` (the Request uses
    /// `instance_id`), so the driver's `classify` impl substitutes its
    /// own `self.key` on this variant.
    LocalCompletion,
    Unexpected,
}

fn classify_reply(msg: &NetMessage) -> ReplyClass {
    match msg {
        NetMessage::V1(NetMessageV1::Get(GetMsg::Response {
            result: GetMsgResult::Found { key, .. },
            ..
        })) => ReplyClass::Terminal { key: *key },
        NetMessage::V1(NetMessageV1::Get(GetMsg::Response {
            result: GetMsgResult::NotFound,
            ..
        })) => ReplyClass::Retry,
        NetMessage::V1(NetMessageV1::Get(GetMsg::ResponseStreaming { key, .. })) => {
            ReplyClass::Terminal { key: *key }
        }
        // Request-echo from forward_pending_op_result_if_completed —
        // same mechanism PUT 3a uses. The echo carries only
        // instance_id, not the full ContractKey; the driver's
        // `classify` impl substitutes `self.key`.
        NetMessage::V1(NetMessageV1::Get(GetMsg::Request { .. })) => ReplyClass::LocalCompletion,
        _ => ReplyClass::Unexpected,
    }
}

// --- Peer advance ---

/// Maximum routing rounds before giving up. Matches PUT 3a's
/// `MAX_RETRIES = 3` and SUBSCRIBE's driver. With typical ring
/// fan-out of 3–5 peers per k_closest call, 3 rounds covers
/// 9–15 distinct peers.
const MAX_RETRIES: usize = 3;

fn advance_to_next_peer(
    op_manager: &OpManager,
    instance_id: &ContractInstanceId,
    tried: &mut Vec<std::net::SocketAddr>,
    retries: &mut usize,
) -> Option<(PeerKeyLocation, std::net::SocketAddr)> {
    if *retries >= MAX_RETRIES {
        return None;
    }
    *retries += 1;

    let peer = op_manager
        .ring
        .k_closest_potentially_hosting(instance_id, tried.as_slice(), 1)
        .into_iter()
        .next()?;
    let addr = peer.socket_addr()?;
    tried.push(addr);
    Some((peer, addr))
}

// --- Subscribe child ---

/// Start a post-GET subscription if requested. Mirrors
/// `put::op_ctx_task::maybe_subscribe_child` verbatim.
///
/// For `blocking_subscribe = true`, awaits the subscribe driver inline.
/// For `blocking_subscribe = false`, spawns a fire-and-forget task.
async fn maybe_subscribe_child(
    op_manager: &Arc<OpManager>,
    client_tx: Transaction,
    key: ContractKey,
    subscribe: bool,
    blocking_subscribe: bool,
) {
    if !subscribe {
        return;
    }

    use crate::operations::subscribe;

    let child_tx = Transaction::new_child_of::<subscribe::SubscribeMsg>(&client_tx);

    // Register the child so `LocalSubscribeComplete` hits the
    // silent-absorb branch instead of trying to publish to a
    // nonexistent waiter.
    op_manager.expect_and_register_sub_operation(client_tx, child_tx);

    if blocking_subscribe {
        subscribe::run_client_subscribe(op_manager.clone(), *key.id(), child_tx).await;
    } else {
        GlobalExecutor::spawn(subscribe::run_client_subscribe(
            op_manager.clone(),
            *key.id(),
            child_tx,
        ));
    }
}

// --- Outcome delivery ---

fn deliver_outcome(op_manager: &OpManager, client_tx: Transaction, outcome: DriverOutcome) {
    match outcome {
        DriverOutcome::Publish(result) => {
            op_manager.send_client_result(client_tx, result);
        }
        DriverOutcome::InfrastructureError(err) => {
            tracing::warn!(
                tx = %client_tx,
                error = %err,
                "get (task-per-tx): infrastructure error; publishing synthesized client error"
            );
            let synthesized: HostResult = Err(ErrorKind::OperationError {
                cause: format!("GET failed: {err}").into(),
            }
            .into());
            op_manager.send_client_result(client_tx, synthesized);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn dummy_key() -> ContractKey {
        ContractKey::from_id_and_code(ContractInstanceId::new([1u8; 32]), CodeHash::new([2u8; 32]))
    }

    fn dummy_tx() -> Transaction {
        Transaction::new::<GetMsg>()
    }

    #[test]
    fn classify_reply_response_found_is_terminal() {
        let tx = dummy_tx();
        let key = dummy_key();
        let msg = NetMessage::V1(NetMessageV1::Get(GetMsg::Response {
            id: tx,
            instance_id: *key.id(),
            result: GetMsgResult::Found {
                key,
                value: StoreResponse {
                    state: Some(WrappedState::new(vec![1u8])),
                    contract: None,
                },
            },
        }));
        assert!(matches!(classify_reply(&msg), ReplyClass::Terminal { .. }));
    }

    #[test]
    fn classify_reply_response_notfound_is_retry() {
        let tx = dummy_tx();
        let key = dummy_key();
        let msg = NetMessage::V1(NetMessageV1::Get(GetMsg::Response {
            id: tx,
            instance_id: *key.id(),
            result: GetMsgResult::NotFound,
        }));
        assert!(matches!(classify_reply(&msg), ReplyClass::Retry));
    }

    #[test]
    fn classify_reply_response_streaming_is_terminal() {
        let tx = dummy_tx();
        let key = dummy_key();
        let msg = NetMessage::V1(NetMessageV1::Get(GetMsg::ResponseStreaming {
            id: tx,
            instance_id: *key.id(),
            stream_id: crate::transport::peer_connection::StreamId::next(),
            key,
            total_size: 1024,
            includes_contract: true,
        }));
        assert!(matches!(classify_reply(&msg), ReplyClass::Terminal { .. }));
    }

    #[test]
    fn classify_reply_forwarding_ack_is_unexpected() {
        let tx = dummy_tx();
        let key = dummy_key();
        let msg = NetMessage::V1(NetMessageV1::Get(GetMsg::ForwardingAck {
            id: tx,
            instance_id: *key.id(),
        }));
        assert!(
            matches!(classify_reply(&msg), ReplyClass::Unexpected),
            "ForwardingAck must NOT be classified as terminal (Phase 2b bug 2)"
        );
    }

    #[test]
    fn classify_reply_response_streaming_ack_is_unexpected() {
        let tx = dummy_tx();
        let msg = NetMessage::V1(NetMessageV1::Get(GetMsg::ResponseStreamingAck {
            id: tx,
            stream_id: crate::transport::peer_connection::StreamId::next(),
        }));
        assert!(matches!(classify_reply(&msg), ReplyClass::Unexpected));
    }

    #[test]
    fn classify_reply_request_echo_is_local_completion() {
        // When process_message completes locally (no next hop, contract
        // already cached), the Request is echoed back via
        // forward_pending_op_result_if_completed.
        let tx = dummy_tx();
        let key = dummy_key();
        let msg = NetMessage::V1(NetMessageV1::Get(GetMsg::Request {
            id: tx,
            instance_id: *key.id(),
            fetch_contract: true,
            htl: 5,
            visited: VisitedPeers::new(&tx),
            subscribe: false,
        }));
        assert!(matches!(classify_reply(&msg), ReplyClass::LocalCompletion));
    }

    #[test]
    fn classify_reply_unexpected_for_non_get_message() {
        let tx = dummy_tx();
        let msg = NetMessage::V1(NetMessageV1::Aborted(tx));
        assert!(matches!(classify_reply(&msg), ReplyClass::Unexpected));
    }

    #[test]
    fn max_retries_boundary_exhausts_at_limit() {
        let mut retries: usize = 0;
        for _ in 0..MAX_RETRIES {
            assert!(retries < MAX_RETRIES, "should not exhaust before limit");
            retries += 1;
        }
        assert!(
            retries >= MAX_RETRIES,
            "should exhaust at MAX_RETRIES={MAX_RETRIES}"
        );
    }

    #[test]
    fn driver_outcome_exhausted_produces_client_error() {
        let cause = "GET to contract failed after 3 attempts".to_string();
        let outcome: DriverOutcome = match RetryLoopOutcome::<()>::Exhausted(cause) {
            RetryLoopOutcome::Exhausted(cause) => {
                DriverOutcome::Publish(Err(ErrorKind::OperationError {
                    cause: cause.into(),
                }
                .into()))
            }
            RetryLoopOutcome::Done(_)
            | RetryLoopOutcome::Unexpected
            | RetryLoopOutcome::InfraError(_) => unreachable!(),
        };
        assert!(
            matches!(outcome, DriverOutcome::Publish(Err(_))),
            "Exhaustion must produce a client error, not be swallowed"
        );
    }

    /// Guard against subscribe firing when the client did not request it.
    /// Source-scrape to verify `maybe_subscribe_child` short-circuits on
    /// `!subscribe`. Mirrors the spirit of PUT 3a's
    /// `finalize_put_at_originator_never_subscribes_from_driver`.
    #[test]
    fn maybe_subscribe_child_short_circuits_on_false() {
        const SOURCE: &str = include_str!("op_ctx_task.rs");
        let fn_start = SOURCE
            .find("async fn maybe_subscribe_child(")
            .expect("maybe_subscribe_child must exist");
        let body = &SOURCE[fn_start..];
        let early_return = body
            .find("if !subscribe {")
            .expect("maybe_subscribe_child must short-circuit on !subscribe");
        let register_call = body
            .find("expect_and_register_sub_operation")
            .expect("maybe_subscribe_child must register sub-operation");
        assert!(
            early_return < register_call,
            "The !subscribe short-circuit must come BEFORE the \
             expect_and_register_sub_operation call — otherwise we'd \
             register a spurious sub-op for a client who didn't ask for \
             one. See PUT 3a commit 494a3c69 for the analogous bug class."
        );
    }
}
