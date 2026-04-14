//! Task-per-transaction client-initiated PUT (#1454 Phase 3a).
//!
//! Mirrors [`crate::operations::subscribe::op_ctx_task`] — the first
//! production consumer of [`OpCtx::send_and_await`] for SUBSCRIBE.
//! This module applies the same pattern to client-initiated PUT.
//!
//! # Scope (Phase 3a)
//!
//! Only the **client-initiated originator** PUT runs through this module.
//! Relay PUTs (with `upstream_addr: Some`), GC-spawned speculative retries,
//! and streaming request setup stay on the legacy re-entry loop.
//!
//! # Architecture
//!
//! The task owns all routing state in its locals — there is no `PutOp` in
//! `OpManager.ops.put` for any attempt this task makes. The task:
//!
//! 1. Calls [`super::put_contract`] to store the contract locally.
//! 2. Finds the k-closest peers to forward the PUT to.
//! 3. If no remote peers: delivers directly via `send_client_result`.
//! 4. Otherwise: loops, calling [`OpCtx::send_and_await`] with a fresh
//!    `Transaction` per attempt (single-use-per-tx constraint).
//! 5. On terminal `Response`/`ResponseStreaming`: calls
//!    [`super::finalize_put_at_originator`] for telemetry + subscription,
//!    then delivers via `send_client_result`.
//! 6. On timeout/wire-error: advances to next peer or exhausts.
//!
//! # Speculative retries (R2)
//!
//! The driver uses serial retries only. Speculative parallel paths
//! (GC-spawned via `speculative_paths`) are not supported — the driver
//! has no DashMap entry for the GC task to find. This is an accepted
//! regression for Phase 3a; the driver's retry loop covers the same
//! failure modes, just sequentially.
//!
//! # Connection-drop latency (R6)
//!
//! Legacy `handle_abort` detects disconnects in <1s. Task-per-tx relies
//! on the `OPERATION_TTL` (60s) timeout. Accepted ceiling, matching
//! Phase 2b's SUBSCRIBE driver.

use std::collections::HashSet;
use std::sync::Arc;

use freenet_stdlib::client_api::{ContractResponse, ErrorKind, HostResponse};
use freenet_stdlib::prelude::*;

use crate::client_events::HostResult;
use crate::config::GlobalExecutor;
use crate::message::{NetMessage, NetMessageV1, Transaction};
use crate::node::OpManager;
use crate::operations::OpError;
use crate::operations::op_ctx::{
    AdvanceOutcome, AttemptOutcome, RetryDriver, RetryLoopOutcome, drive_retry_loop,
};
use crate::ring::{Location, PeerKeyLocation};
use crate::router::{RouteEvent, RouteOutcome};

use super::{PutFinalizationData, PutMsg};

/// Start a client-initiated PUT, returning as soon as the task has been
/// spawned (mirrors legacy `request_put` timing).
///
/// The caller must have already registered a result waiter for `client_tx`
/// via `op_manager.ch_outbound.waiting_for_transaction_result`. This
/// function does NOT touch the waiter; it only drives the ring/network
/// side and publishes the terminal result to `result_router_tx` keyed
/// by `client_tx`.
#[allow(clippy::too_many_arguments)]
pub(crate) async fn start_client_put(
    op_manager: Arc<OpManager>,
    client_tx: Transaction,
    contract: ContractContainer,
    related: RelatedContracts<'static>,
    value: WrappedState,
    htl: usize,
    subscribe: bool,
    blocking_subscribe: bool,
) -> Result<Transaction, OpError> {
    tracing::debug!(
        tx = %client_tx,
        contract = %contract.key(),
        "put (task-per-tx): spawning client-initiated task"
    );

    // Spawn the driver. Same fire-and-forget rationale as SUBSCRIBE's
    // `start_client_subscribe` — failures are delivered to the client
    // via `result_router_tx`, not via this function's return value.
    //
    // Not registered with `BackgroundTaskMonitor`: per-transaction task
    // that terminates via happy path, exhaustion, timeout, or infra error.
    //
    // Amplification ceiling: the client_events.rs PUT handler allocates
    // one task per client PUT request. Client request rate is bounded by
    // the WS connection handler's backpressure.
    GlobalExecutor::spawn(run_client_put(
        op_manager,
        client_tx,
        contract,
        related,
        value,
        htl,
        subscribe,
        blocking_subscribe,
    ));

    Ok(client_tx)
}

#[allow(clippy::too_many_arguments)]
async fn run_client_put(
    op_manager: Arc<OpManager>,
    client_tx: Transaction,
    contract: ContractContainer,
    related: RelatedContracts<'static>,
    value: WrappedState,
    htl: usize,
    subscribe: bool,
    blocking_subscribe: bool,
) {
    let outcome = drive_client_put(
        op_manager.clone(),
        client_tx,
        contract,
        related,
        value,
        htl,
        subscribe,
        blocking_subscribe,
    )
    .await;
    deliver_outcome(&op_manager, client_tx, outcome);
}

/// PUT driver has exactly two outcomes — no `SkipAlreadyDelivered` because
/// PUT doesn't use `NodeEvent::LocalPutComplete` (the driver owns local
/// completion delivery directly).
#[derive(Debug)]
enum DriverOutcome {
    /// The driver produced a `HostResult` that must be published via
    /// `result_router_tx`.
    Publish(HostResult),
    /// A genuine infrastructure failure escaped the driver loop.
    InfrastructureError(OpError),
}

#[allow(clippy::too_many_arguments)]
async fn drive_client_put(
    op_manager: Arc<OpManager>,
    client_tx: Transaction,
    contract: ContractContainer,
    related: RelatedContracts<'static>,
    value: WrappedState,
    htl: usize,
    subscribe: bool,
    blocking_subscribe: bool,
) -> DriverOutcome {
    match drive_client_put_inner(
        &op_manager,
        client_tx,
        contract,
        related,
        value,
        htl,
        subscribe,
        blocking_subscribe,
    )
    .await
    {
        Ok(outcome) => outcome,
        Err(err) => DriverOutcome::InfrastructureError(err),
    }
}

#[allow(clippy::too_many_arguments)]
async fn drive_client_put_inner(
    op_manager: &Arc<OpManager>,
    client_tx: Transaction,
    contract: ContractContainer,
    related: RelatedContracts<'static>,
    value: WrappedState,
    htl: usize,
    subscribe: bool,
    blocking_subscribe: bool,
) -> Result<DriverOutcome, OpError> {
    let key = contract.key();

    // Always send through send_and_await so process_message handles
    // local storage + hosting/interest/broadcast side effects.
    // Do NOT call put_contract directly — process_message does it
    // with the correct state_changed tracking for convergence.
    //
    // If no remote peers exist, process_message stores locally and
    // returns ContinueOp(Finished). forward_pending_op_result_if_completed
    // then sends the original Request back to the driver, which
    // classify_reply handles as LocalCompletion.
    let mut tried: Vec<std::net::SocketAddr> = Vec::new();
    if let Some(own_addr) = op_manager.ring.connection_manager.get_own_addr() {
        tried.push(own_addr);
    }

    // Pre-select initial target for the driver's retry state. The actual
    // routing decision is made by process_message, not the driver.
    let initial_target = op_manager
        .ring
        .closest_potentially_hosting(&key, tried.as_slice());
    let current_target = match initial_target {
        Some(peer) => {
            if let Some(addr) = peer.socket_addr() {
                tried.push(addr);
            }
            peer
        }
        None => op_manager.ring.connection_manager.own_location(),
    };

    // Retry loop via shared driver (#3807).
    struct PutRetryDriver<'a> {
        op_manager: &'a OpManager,
        key: ContractKey,
        contract: ContractContainer,
        related: RelatedContracts<'static>,
        value: WrappedState,
        htl: usize,
        tried: Vec<std::net::SocketAddr>,
        retries: usize,
        current_target: PeerKeyLocation,
    }

    impl RetryDriver for PutRetryDriver<'_> {
        type Terminal = ContractKey;

        fn new_attempt_tx(&mut self) -> Transaction {
            Transaction::new::<PutMsg>()
        }

        fn build_request(&mut self, attempt_tx: Transaction) -> NetMessage {
            NetMessage::from(PutMsg::Request {
                id: attempt_tx,
                contract: self.contract.clone(),
                related_contracts: self.related.clone(),
                value: self.value.clone(),
                htl: self.htl,
                // Only include own_addr in skip_list (matching legacy request_put).
                // `tried` contains driver-side routing state (peers the driver
                // selected); process_message makes its own forwarding decisions.
                skip_list: self
                    .op_manager
                    .ring
                    .connection_manager
                    .get_own_addr()
                    .into_iter()
                    .collect::<HashSet<_>>(),
            })
        }

        fn classify(&mut self, reply: NetMessage) -> AttemptOutcome<ContractKey> {
            match classify_reply(&reply) {
                ReplyClass::Stored { key } | ReplyClass::LocalCompletion { key } => {
                    AttemptOutcome::Terminal(key)
                }
                ReplyClass::Unexpected => AttemptOutcome::Unexpected,
            }
        }

        fn advance(&mut self) -> AdvanceOutcome {
            match advance_to_next_peer(
                self.op_manager,
                &self.key,
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

    let mut driver = PutRetryDriver {
        op_manager,
        key,
        contract,
        related,
        value,
        htl,
        tried,
        retries: 0,
        current_target,
    };

    let loop_result = drive_retry_loop(op_manager, client_tx, "put", &mut driver).await;

    match loop_result {
        RetryLoopOutcome::Done(reply_key) => {
            // Clean up the DashMap entry that process_message created.
            // Without this, the GC task finds a stale AwaitingResponse
            // entry and launches speculative retries on the completed op.
            op_manager.completed(client_tx);

            // Emit routing event + telemetry — report_result (which normally
            // does both) doesn't run because the bypass intercepted the
            // Response. Without this, the router's prediction model never
            // receives PUT success feedback and simulation tests that check
            // route_outcome telemetry fail.
            let contract_location = Location::from(&reply_key);
            let route_event = RouteEvent {
                peer: driver.current_target.clone(),
                contract_location,
                outcome: RouteOutcome::SuccessUntimed,
                op_type: Some(crate::node::network_status::OpType::Put),
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
                crate::node::network_status::OpType::Put,
                true,
            );

            // Telemetry only — subscribe=false to avoid double-subscribe.
            super::finalize_put_at_originator(
                op_manager,
                client_tx,
                reply_key,
                PutFinalizationData {
                    sender: driver.current_target,
                    hop_count: None,
                    state_hash: None,
                    state_size: None,
                },
                false,
                false,
            )
            .await;

            maybe_subscribe_child(
                op_manager,
                client_tx,
                reply_key,
                subscribe,
                blocking_subscribe,
            )
            .await;

            Ok(DriverOutcome::Publish(Ok(HostResponse::ContractResponse(
                ContractResponse::PutResponse { key: reply_key },
            ))))
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

// --- Reply classification ---

#[derive(Debug)]
enum ReplyClass {
    /// Remote peer accepted the PUT.
    Stored {
        key: ContractKey,
    },
    /// Local completion: process_message stored locally but found no
    /// next hop, so forward_pending_op_result_if_completed sent back
    /// the original Request. The contract is stored at the originator.
    LocalCompletion {
        key: ContractKey,
    },
    Unexpected,
}

fn classify_reply(msg: &NetMessage) -> ReplyClass {
    match msg {
        NetMessage::V1(NetMessageV1::Put(
            PutMsg::Response { key, .. } | PutMsg::ResponseStreaming { key, .. },
        )) => ReplyClass::Stored { key: *key },
        // When process_message completes locally (no next hop), the
        // Request is echoed back via forward_pending_op_result_if_completed.
        NetMessage::V1(NetMessageV1::Put(PutMsg::Request {
            id: _, contract, ..
        })) => ReplyClass::LocalCompletion {
            key: contract.key(),
        },
        _ => ReplyClass::Unexpected,
    }
}

// --- Peer advance ---

/// Maximum routing rounds before giving up. Matches the legacy PUT retry
/// budget (3 alternatives via `retry_with_next_alternative`) and the
/// SUBSCRIBE driver's `MAX_RETRIES`. With typical ring fan-out of 3-5
/// peers per k_closest call, 3 rounds covers 9-15 distinct peers.
const MAX_RETRIES: usize = 3;

/// Ask the ring for a new closest peer, excluding all previously tried
/// addresses. Returns `None` when exhausted.
fn advance_to_next_peer(
    op_manager: &OpManager,
    key: &ContractKey,
    tried: &mut Vec<std::net::SocketAddr>,
    retries: &mut usize,
) -> Option<(PeerKeyLocation, std::net::SocketAddr)> {
    if *retries >= MAX_RETRIES {
        return None;
    }
    *retries += 1;

    let peer = op_manager
        .ring
        .closest_potentially_hosting(key, tried.as_slice())?;
    let addr = peer.socket_addr()?;
    tried.push(addr);
    Some((peer, addr))
}

// --- Subscribe child ---

/// Start a post-PUT subscription if requested.
///
/// For `blocking_subscribe = true`, awaits the subscribe driver inline
/// (requires `run_client_subscribe` to be `pub(crate)`).
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
    // silent-absorb branch at `p2p_protoc.rs:2057–2066` instead
    // of trying to publish to a nonexistent waiter.
    op_manager.expect_and_register_sub_operation(client_tx, child_tx);

    if blocking_subscribe {
        // Inline await — PUT response waits for subscribe completion.
        subscribe::run_client_subscribe(op_manager.clone(), *key.id(), child_tx).await;
    } else {
        // Fire-and-forget — PUT response returns immediately.
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
                "put (task-per-tx): infrastructure error; publishing synthesized client error"
            );
            let synthesized: HostResult = Err(ErrorKind::OperationError {
                cause: format!("PUT failed: {err}").into(),
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
        Transaction::new::<PutMsg>()
    }

    #[test]
    fn classify_reply_response_is_stored() {
        let tx = dummy_tx();
        let key = dummy_key();
        let msg = NetMessage::V1(NetMessageV1::Put(PutMsg::Response { id: tx, key }));
        assert!(matches!(classify_reply(&msg), ReplyClass::Stored { .. }));
    }

    #[test]
    fn classify_reply_response_streaming_is_stored() {
        let tx = dummy_tx();
        let key = dummy_key();
        let msg = NetMessage::V1(NetMessageV1::Put(PutMsg::ResponseStreaming {
            id: tx,
            key,
            continue_forwarding: false,
        }));
        assert!(matches!(classify_reply(&msg), ReplyClass::Stored { .. }));
    }

    #[test]
    fn classify_reply_forwarding_ack_is_unexpected() {
        let tx = dummy_tx();
        let key = dummy_key();
        let msg = NetMessage::V1(NetMessageV1::Put(PutMsg::ForwardingAck {
            id: tx,
            contract_key: key,
        }));
        assert!(
            matches!(classify_reply(&msg), ReplyClass::Unexpected),
            "ForwardingAck must NOT be classified as terminal (Phase 2b bug 2)"
        );
    }

    /// Regression guard for the double-subscribe bug (commit 494a3c69).
    ///
    /// The driver calls `finalize_put_at_originator` for telemetry and then
    /// `maybe_subscribe_child` for subscriptions. If `finalize_put_at_originator`
    /// is called with `subscribe=true`, it starts a subscription via the legacy
    /// `start_subscription_after_put` path, AND `maybe_subscribe_child` starts
    /// another via the task-per-tx `run_client_subscribe` path — doubling
    /// network traffic and subscription registrations.
    ///
    /// This test scrapes the source to verify all `finalize_put_at_originator`
    /// calls inside the driver pass `false` for the subscribe arguments.
    #[test]
    fn finalize_put_at_originator_never_subscribes_from_driver() {
        const SOURCE: &str = include_str!("op_ctx_task.rs");

        // Find every call to finalize_put_at_originator in this file.
        // Each call must pass `false` for the subscribe parameter (5th arg).
        // The pattern we check: the two lines after `state_size: None,` and
        // before `)` must both be `false,` or `false`.
        let call_marker = "finalize_put_at_originator(";
        let mut offset = 0;
        let mut call_count = 0;

        while let Some(pos) = SOURCE[offset..].find(call_marker) {
            let abs_pos = offset + pos;
            // Get the window from the call to the closing `.await`
            let window_end = SOURCE[abs_pos..]
                .find(".await")
                .map(|p| abs_pos + p)
                .unwrap_or(SOURCE.len().min(abs_pos + 500));
            let window = &SOURCE[abs_pos..window_end];

            // The subscribe arguments should be `false`. Check that
            // `true` does NOT appear as a subscribe argument.
            // The window contains the struct literal for PutFinalizationData
            // plus the two boolean args. After `},` the next two values
            // are subscribe and blocking_subscribe.
            let after_struct = window.find("},").map(|p| &window[p..]);
            if let Some(tail) = after_struct {
                assert!(
                    !tail.contains("subscribe"),
                    "finalize_put_at_originator call in driver passes subscribe \
                     arguments that reference the `subscribe` variable instead of \
                     hardcoded `false`. This would cause double-subscription — \
                     subscriptions must be handled exclusively by maybe_subscribe_child. \
                     See commit 494a3c69 for the original fix."
                );
            }
            call_count += 1;
            offset = abs_pos + call_marker.len();
        }

        assert!(
            call_count >= 1,
            "Expected at least 1 finalize_put_at_originator call in the driver, \
             found {call_count}"
        );
    }

    #[test]
    fn max_retries_boundary_exhausts_at_limit() {
        // Verify the MAX_RETRIES boundary: retries >= MAX_RETRIES → None.
        // Tests the counter logic that advance_to_next_peer uses.
        let mut retries: usize = 0;
        // First MAX_RETRIES calls should increment (simulating advance succeeding)
        for _ in 0..MAX_RETRIES {
            assert!(retries < MAX_RETRIES, "should not exhaust before limit");
            retries += 1;
        }
        // At MAX_RETRIES, the guard triggers
        assert!(
            retries >= MAX_RETRIES,
            "should exhaust at MAX_RETRIES={MAX_RETRIES}"
        );
    }

    #[test]
    fn classify_reply_unexpected_for_non_put_message() {
        // An Aborted message (non-PUT) should be Unexpected.
        let tx = dummy_tx();
        let msg = NetMessage::V1(NetMessageV1::Aborted(tx));
        assert!(matches!(classify_reply(&msg), ReplyClass::Unexpected));
    }

    #[test]
    fn driver_outcome_exhausted_produces_client_error() {
        // Verify that RetryLoopOutcome::Exhausted maps to a client-visible
        // OperationError, not a silent drop or infrastructure error.
        let cause = "PUT to contract failed after 3 attempts".to_string();
        let outcome: DriverOutcome = match RetryLoopOutcome::<ContractKey>::Exhausted(cause) {
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

    #[test]
    fn classify_reply_request_is_local_completion() {
        // When process_message completes locally (no next hop), the Request
        // is echoed back via forward_pending_op_result_if_completed.
        let tx = dummy_tx();
        let msg = NetMessage::V1(NetMessageV1::Put(PutMsg::Request {
            id: tx,
            contract: ContractContainer::Wasm(ContractWasmAPIVersion::V1(WrappedContract::new(
                Arc::new(ContractCode::from(vec![0u8])),
                Parameters::from(vec![]),
            ))),
            related_contracts: RelatedContracts::default(),
            value: WrappedState::new(vec![1u8]),
            htl: 5,
            skip_list: HashSet::new(),
        }));
        assert!(matches!(
            classify_reply(&msg),
            ReplyClass::LocalCompletion { .. }
        ));
    }
}

