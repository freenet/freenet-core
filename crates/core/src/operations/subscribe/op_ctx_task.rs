//! Task-per-transaction client-initiated SUBSCRIBE (#1454 Phase 2b).
//!
//! This module hosts the first production consumer of [`OpCtx::send_and_await`][ocx],
//! driving a client-initiated SUBSCRIBE end-to-end inside a single spawned
//! task instead of the legacy re-entry loop through `handle_op_result` +
//! `OpManager.ops.subscribe` DashMap.
//!
//! # Scope (Phase 2b)
//!
//! Only the **client-initiated** SUBSCRIBE entry point (via
//! [`crate::node::subscribe_with_id`]) runs through this module. The other
//! three entry points stay on the legacy path for reasons documented on
//! issue #1454 Phase 2b:
//!
//! - **Renewals** (`ring.rs::connection_maintenance` loop) — their jitter
//!   and spam-prevention in `can_request_subscription()` are load-bearing.
//! - **PUT sub-ops** (`start_subscription_request_internal`) — blocking
//!   semantics with `SubOperationTracker`; migrated in Phase 2.5.
//! - **Intermediate-peer forwarding** (`SubscribeOp::load_or_init` on
//!   incoming `Request`) — server-side response role; migrated in Phase 5.
//!
//! Only the **client-initiated** path goes through `subscribe_with_id`;
//! the three legacy paths never call it, so migrating the body of
//! `subscribe_with_id` wholesale is sufficient to gate the new path.
//!
//! # Architecture
//!
//! The task owns all routing state in its locals — there is no
//! `SubscribeOp` in the `OpManager.ops.subscribe` DashMap for any attempt
//! this task makes. The task:
//!
//! 1. Calls [`super::prepare_initial_request`] to decide target peer vs
//!    local-completion vs give-up.
//! 2. If network target: loops, calling [`OpCtx::send_and_await`][ocx]
//!    with a **fresh `Transaction` per attempt** (required by
//!    `send_and_await`'s single-use-per-tx contract). Each attempt
//!    inserts a capacity-1 reply channel into `p2p_protoc::pending_op_results`
//!    keyed by the attempt tx; the pure-network-message handler routes
//!    replies via the SUBSCRIBE bypass added in Phase 2b (see
//!    `node::try_forward_task_per_tx_reply`).
//! 3. On `Subscribed`: delivers via `result_router_tx` keyed by the
//!    **client-visible** `Transaction` (the one returned to the caller
//!    and registered with `ch_outbound.waiting_for_transaction_result`).
//! 4. On `NotFound`: applies breadth retry → fresh k_closest round →
//!    exhaustion, mirroring the legacy retry logic in
//!    `subscribe::handle_op_response`.
//! 5. On `send_and_await` timeout: treats the attempt as a peer timeout
//!    and applies the same retry logic.
//!
//! # Client-visible tx vs per-attempt tx
//!
//! Legacy SUBSCRIBE reuses one `Transaction` end-to-end. Phase 2b splits
//! this into two tx lifetimes:
//!
//! - **`client_tx`**: allocated once by the WS handler, registered with
//!   `ch_outbound.waiting_for_transaction_result`, delivered to via
//!   `result_router_tx`. Never passed to `send_and_await`.
//! - **`attempt_tx`**: fresh per retry. Used for the wire Request, the
//!   `OpCtx`, and the `pending_op_results` callback slot. Never surfaced
//!   to the client.
//!
//! The split is mandatory: `OpCtx::send_and_await` can only fire once per
//! Transaction (the `completed` / `under_progress` dedup sets in
//! `OpManager` suppress second callbacks). Attempt isolation is the
//! simplest reconciliation with that constraint.
//!
//! [ocx]: crate::operations::OpCtx::send_and_await

use std::collections::HashSet;
use std::sync::Arc;

use freenet_stdlib::client_api::{ContractResponse, ErrorKind, HostResponse};
use freenet_stdlib::prelude::ContractInstanceId;

use crate::client_events::HostResult;
use crate::config::{GlobalExecutor, OPERATION_TTL};
use crate::message::{NetMessage, NetMessageV1, Transaction};
use crate::node::OpManager;
use crate::operations::{OpError, VisitedPeers};
use crate::ring::{PeerKeyLocation, RingError};

use super::{
    InitialRequest, MAX_BREADTH, MAX_RETRIES, SubscribeMsg, SubscribeMsgResult,
    complete_local_subscription, prepare_initial_request,
};

/// Start a client-initiated subscribe, returning as soon as the task has
/// been spawned (mirrors legacy `subscribe_with_id` timing).
///
/// The caller must have already registered a result waiter for `client_tx`
/// via `op_manager.ch_outbound.waiting_for_transaction_result` or
/// `waiting_for_subscription_result`. This function does NOT touch the
/// waiter; it only drives the ring/network side and publishes the terminal
/// result to `result_router_tx` keyed by `client_tx`.
///
/// Returns `Ok(client_tx)` once the task has been spawned. The spawned task
/// owns the rest of the subscribe lifetime.
pub(crate) async fn start_client_subscribe(
    op_manager: Arc<OpManager>,
    instance_id: ContractInstanceId,
    client_tx: Transaction,
) -> Result<Transaction, OpError> {
    tracing::debug!(
        tx = %client_tx,
        contract = %instance_id,
        "subscribe (task-per-tx): spawning client-initiated task"
    );

    // Spawn the driver. The task is fire-and-forget from this function's
    // perspective — failures are delivered to the client via
    // `result_router_tx`, not via the return value of this function, to
    // match legacy `request_subscribe` behaviour (it pushes the op via
    // `notify_op_change` and returns `Ok(())` immediately, letting the
    // event loop drive the rest).
    //
    // Not registered with BackgroundTaskMonitor: like legacy subscribe
    // work, this task is scoped to a single transaction and terminates
    // on its own either on success, error, or the OPERATION_TTL-wrapped
    // timeout path in the retry loop. A leaked task here would cost one
    // tx slot in `pending_op_results` and one spawned future — bounded
    // by the client-side rate-limiting on SUBSCRIBE requests, not
    // unbounded amplification.
    GlobalExecutor::spawn(run_client_subscribe(op_manager, instance_id, client_tx));

    Ok(client_tx)
}

/// Drive a client-initiated subscribe to completion and publish the result
/// to `result_router_tx` keyed by `client_tx`.
///
/// Runs inside a spawned task. Never panics — any error is converted into
/// a `HostResult::Err` and delivered through `result_router_tx`.
async fn run_client_subscribe(
    op_manager: Arc<OpManager>,
    instance_id: ContractInstanceId,
    client_tx: Transaction,
) {
    let outcome = drive_client_subscribe(op_manager.clone(), instance_id, client_tx).await;
    deliver_outcome(&op_manager, client_tx, instance_id, outcome);
}

/// Outcome of the driver, carrying an explicit signal for "local completion
/// already published to the router, no follow-up `result_router_tx` send
/// needed". Using an enum here instead of piggybacking on `OpManager::is_completed`
/// makes the skip condition explicit and unshareable with any other path
/// that might happen to mark the same tx completed (review finding M3).
#[derive(Debug)]
enum DriverOutcome {
    /// The driver produced a `HostResult` that must be published via
    /// `result_router_tx`.
    Publish(HostResult),
    /// Local completion already published via
    /// `NodeEvent::LocalSubscribeComplete` inside
    /// `complete_local_subscription`. The driver must NOT publish a
    /// second result for this tx — doing so would duplicate the
    /// `HostResponse::ContractResponse(SubscribeResponse)` the
    /// `LocalSubscribeComplete` handler already pushed.
    SkipAlreadyDelivered,
    /// A genuine infrastructure failure escaped the driver loop
    /// (e.g., executor channel closed, unexpected reply variant).
    /// `deliver_outcome` converts this into a synthesized client error.
    InfrastructureError(OpError),
}

/// The inner driver: returns a [`DriverOutcome`] describing how the
/// subscribe terminated and whether the delivery side-effect has already
/// been applied.
async fn drive_client_subscribe(
    op_manager: Arc<OpManager>,
    instance_id: ContractInstanceId,
    client_tx: Transaction,
) -> DriverOutcome {
    match drive_client_subscribe_inner(&op_manager, instance_id, client_tx).await {
        Ok(outcome) => outcome,
        Err(err) => DriverOutcome::InfrastructureError(err),
    }
}

async fn drive_client_subscribe_inner(
    op_manager: &Arc<OpManager>,
    instance_id: ContractInstanceId,
    client_tx: Transaction,
) -> Result<DriverOutcome, OpError> {
    // Decide: local-completion, give up, or send to the network.
    // `prepare_initial_request` uses `client_tx` for its visited-peers
    // bloom filter seed and telemetry; this is fine because the bloom
    // filter is per-attempt-first-peer only and telemetry correlates on
    // the client-visible tx (matching legacy behaviour for the first
    // attempt).
    let initial = prepare_initial_request(
        op_manager,
        client_tx,
        instance_id,
        /* is_renewal */ false,
    )
    .await?;

    let (target_peer, target_addr, mut visited, mut alternatives, htl) = match initial {
        InitialRequest::LocallyComplete { key } => {
            // Local completion reuses the existing helper. It publishes
            // via `NodeEvent::LocalSubscribeComplete` → `result_router_tx`,
            // so the driver MUST NOT deliver a second time — return
            // `SkipAlreadyDelivered` to explicitly signal that to
            // `deliver_outcome`.
            complete_local_subscription(op_manager, client_tx, key, /* is_renewal */ false).await?;
            return Ok(DriverOutcome::SkipAlreadyDelivered);
        }
        InitialRequest::NoHostingPeers => {
            return Ok(DriverOutcome::Publish(Err(ErrorKind::OperationError {
                cause: format!("no remote peers available for subscription to {instance_id}")
                    .into(),
            }
            .into())));
        }
        InitialRequest::PeerNotJoined => {
            return Err(RingError::PeerNotJoined.into());
        }
        InitialRequest::NetworkRequest {
            target,
            target_addr,
            visited,
            alternatives,
            htl,
        } => (target, target_addr, visited, alternatives, htl),
    };

    tracing::debug!(
        tx = %client_tx,
        contract = %instance_id,
        target = %target_addr,
        "subscribe (task-per-tx): initial target selected, entering retry loop"
    );

    // Initial state for the retry loop. The first iteration uses
    // `target_peer` (as the current target) from `prepare_initial_request`;
    // subsequent attempts pull from `advance_to_next_peer`.
    //
    // `prepare_initial_request` has already emitted a
    // `NetEventLog::subscribe_request` event keyed on `client_tx` for the
    // first attempt (mirroring legacy behaviour). Subsequent attempts
    // re-emit inside the loop using the per-attempt tx so retries are
    // visible in the event log (review finding L4).
    let mut tried_peers: HashSet<std::net::SocketAddr> = HashSet::new();
    tried_peers.insert(target_addr);
    let mut retries: usize = 0;
    let mut attempts_at_hop: usize = 1;
    let mut current_target: PeerKeyLocation = target_peer;
    let mut current_target_addr: std::net::SocketAddr = target_addr;
    let mut is_first_attempt = true;

    loop {
        // Fresh attempt tx: single-use-per-tx for send_and_await.
        let attempt_tx = Transaction::new::<SubscribeMsg>();

        tracing::debug!(
            tx = %client_tx,
            attempt_tx = %attempt_tx,
            target = %current_target_addr,
            retries,
            attempts_at_hop,
            "subscribe (task-per-tx): sending attempt"
        );

        // Per-attempt telemetry (review finding L4). `prepare_initial_request`
        // emits the first-attempt event on `client_tx`; every retry after
        // that emits on the fresh `attempt_tx` so the event log captures
        // the full retry chain.
        if !is_first_attempt {
            if let Some(event) = crate::tracing::NetEventLog::subscribe_request(
                &attempt_tx,
                &op_manager.ring,
                instance_id,
                current_target.clone(),
                htl,
            ) {
                op_manager
                    .ring
                    .register_events(either::Either::Left(event))
                    .await;
            }
        }
        is_first_attempt = false;

        let request = SubscribeMsg::Request {
            id: attempt_tx,
            instance_id,
            htl,
            visited: visited.clone(),
            is_renewal: false,
        };

        let mut ctx = op_manager.op_ctx(attempt_tx);
        let round_trip =
            tokio::time::timeout(OPERATION_TTL, ctx.send_and_await(NetMessage::from(request)))
                .await;

        // Release the per-attempt `pending_op_results` slot before any
        // retry or return. Without this emission entries would only be
        // reclaimed by the 60 s periodic sweep of closed senders
        // (p2p_protoc.rs:960-987), which lags the real completion by
        // several seconds per attempt and causes the
        // `test_pending_op_results_bounded` regression guard to flag a
        // leak. We emit the event regardless of outcome (success, wire
        // error, timeout) because the inserted callback slot is keyed
        // only on `attempt_tx` — its lifetime matches the attempt, not
        // the reply classification.
        //
        // `release_pending_op_slot` uses a timeout-wrapped `send().await`
        // rather than `try_send` so the cleanup survives transient
        // notification-channel backpressure (review finding M1). We are
        // inside a spawned task, not an event loop, so `send().await` is
        // within the channel-safety rules.
        op_manager.release_pending_op_slot(attempt_tx).await;

        let reply = match round_trip {
            Ok(Ok(reply)) => reply,
            Ok(Err(err)) => {
                tracing::warn!(
                    tx = %client_tx,
                    attempt_tx = %attempt_tx,
                    error = %err,
                    "subscribe (task-per-tx): send_and_await failed; treating as peer timeout"
                );
                // Fall through to the "peer timeout / try next" branch.
                // We don't distinguish infrastructure failure from a
                // wire-level NotFound here — from "should we retry on a
                // different peer?" the answer is the same.
                match advance_to_next_peer(
                    op_manager,
                    &instance_id,
                    &mut visited,
                    &mut tried_peers,
                    &mut alternatives,
                    &mut retries,
                    &mut attempts_at_hop,
                )
                .await
                {
                    Some((next_target, next_addr)) => {
                        current_target = next_target;
                        current_target_addr = next_addr;
                        continue;
                    }
                    None => {
                        return Ok(DriverOutcome::Publish(Err(ErrorKind::OperationError {
                            cause: format!(
                                "subscribe to {instance_id} failed after {} rounds (last peer error: {err})",
                                retries + 1
                            )
                            .into(),
                        }
                        .into())));
                    }
                }
            }
            Err(_) => {
                tracing::warn!(
                    tx = %client_tx,
                    attempt_tx = %attempt_tx,
                    target = %current_target_addr,
                    "subscribe (task-per-tx): attempt timed out after OPERATION_TTL; trying next peer"
                );
                match advance_to_next_peer(
                    op_manager,
                    &instance_id,
                    &mut visited,
                    &mut tried_peers,
                    &mut alternatives,
                    &mut retries,
                    &mut attempts_at_hop,
                )
                .await
                {
                    Some((next_target, next_addr)) => {
                        current_target = next_target;
                        current_target_addr = next_addr;
                        continue;
                    }
                    None => {
                        return Ok(DriverOutcome::Publish(Err(ErrorKind::OperationError {
                            cause: format!(
                                "subscribe to {instance_id} timed out after {} rounds",
                                retries + 1
                            )
                            .into(),
                        }
                        .into())));
                    }
                }
            }
        };

        // Classify the terminal reply.
        match classify_reply(&reply) {
            ReplyClass::Subscribed { key } => {
                tracing::info!(
                    tx = %client_tx,
                    attempt_tx = %attempt_tx,
                    contract = %key,
                    "subscribe (task-per-tx): subscribed"
                );
                return Ok(DriverOutcome::Publish(Ok(HostResponse::ContractResponse(
                    ContractResponse::SubscribeResponse {
                        key,
                        subscribed: true,
                    },
                ))));
            }
            ReplyClass::NotFound => {
                tracing::debug!(
                    tx = %client_tx,
                    attempt_tx = %attempt_tx,
                    "subscribe (task-per-tx): NotFound, trying next peer"
                );
                match advance_to_next_peer(
                    op_manager,
                    &instance_id,
                    &mut visited,
                    &mut tried_peers,
                    &mut alternatives,
                    &mut retries,
                    &mut attempts_at_hop,
                )
                .await
                {
                    Some((next_target, next_addr)) => {
                        current_target = next_target;
                        current_target_addr = next_addr;
                        continue;
                    }
                    None => {
                        return Ok(DriverOutcome::Publish(Err(ErrorKind::OperationError {
                            cause: format!(
                                "contract {instance_id} not found after exhaustive search"
                            )
                            .into(),
                        }
                        .into())));
                    }
                }
            }
            ReplyClass::Unexpected => {
                tracing::warn!(
                    tx = %client_tx,
                    attempt_tx = %attempt_tx,
                    "subscribe (task-per-tx): unexpected terminal reply"
                );
                return Err(OpError::UnexpectedOpState);
            }
        }
    }
}

/// Classification of a terminal reply delivered to `send_and_await`.
#[derive(Debug)]
enum ReplyClass {
    Subscribed {
        key: freenet_stdlib::prelude::ContractKey,
    },
    NotFound,
    /// Any reply that shouldn't be a terminal for the originator
    /// (e.g., a `Request`, `Unsubscribe`, or `ForwardingAck`). These
    /// indicate a classification bug upstream and are surfaced as errors.
    Unexpected,
}

fn classify_reply(msg: &NetMessage) -> ReplyClass {
    match msg {
        NetMessage::V1(NetMessageV1::Subscribe(SubscribeMsg::Response { result, .. })) => {
            match result {
                SubscribeMsgResult::Subscribed { key } => ReplyClass::Subscribed { key: *key },
                SubscribeMsgResult::NotFound => ReplyClass::NotFound,
            }
        }
        _ => ReplyClass::Unexpected,
    }
}

/// Advance the task-local routing state to the next peer to try, mirroring
/// the legacy `subscribe::handle_op_response` NotFound + alternatives-exhausted
/// logic (`subscribe.rs` ~1675–1842 before Phase 2b).
///
/// Priority:
/// 1. If `attempts_at_hop < MAX_BREADTH` and `alternatives` is non-empty,
///    take the next alternative (breadth retry at the same hop).
/// 2. Otherwise, if `retries < MAX_RETRIES`, run a fresh
///    `k_closest_potentially_hosting` round with the accumulated `visited`
///    filter, reset `attempts_at_hop` to 1, and increment `retries`.
/// 3. Otherwise, return `None` (exhausted).
///
/// Mutates all state references on a successful advance.
async fn advance_to_next_peer(
    op_manager: &OpManager,
    instance_id: &ContractInstanceId,
    visited: &mut VisitedPeers,
    tried_peers: &mut HashSet<std::net::SocketAddr>,
    alternatives: &mut Vec<PeerKeyLocation>,
    retries: &mut usize,
    attempts_at_hop: &mut usize,
) -> Option<(PeerKeyLocation, std::net::SocketAddr)> {
    // 1. Breadth retry at the same hop. Use FIFO (`remove(0)`) to match the
    //    legacy `handle_op_response` ordering (`subscribe.rs:949, 1821`).
    //    `alternatives` is built in closest-first order by
    //    `k_closest_potentially_hosting`, so FIFO means we try the best
    //    candidate we have not yet tried. LIFO (`pop()`) would iterate in
    //    reverse-distance order and diverge from legacy routing behaviour.
    if *attempts_at_hop < MAX_BREADTH {
        while !alternatives.is_empty() {
            let candidate = alternatives.remove(0);
            if let Some(addr) = candidate.socket_addr() {
                if tried_peers.contains(&addr) || visited.probably_visited(addr) {
                    continue;
                }
                tried_peers.insert(addr);
                visited.mark_visited(addr);
                *attempts_at_hop += 1;
                tracing::debug!(
                    %instance_id,
                    target = %addr,
                    attempts_at_hop = *attempts_at_hop,
                    "subscribe (task-per-tx): breadth retry with next alternative"
                );
                return Some((candidate, addr));
            }
        }
    }

    // 2. Fresh k_closest round.
    if *retries < MAX_RETRIES {
        *retries += 1;
        *attempts_at_hop = 1;
        let mut fresh =
            op_manager
                .ring
                .k_closest_potentially_hosting(instance_id, &*visited, MAX_BREADTH);
        while let Some(candidate) = (!fresh.is_empty()).then(|| fresh.remove(0)) {
            if let Some(addr) = candidate.socket_addr() {
                if tried_peers.contains(&addr) || visited.probably_visited(addr) {
                    continue;
                }
                tried_peers.insert(addr);
                visited.mark_visited(addr);
                // Rest of `fresh` becomes the new alternatives pool for
                // subsequent breadth retries at this new hop.
                *alternatives = fresh;
                tracing::debug!(
                    %instance_id,
                    target = %addr,
                    retries = *retries,
                    "subscribe (task-per-tx): fresh k_closest round found new target"
                );
                return Some((candidate, addr));
            }
            // Skip candidate without socket addr, try next from fresh.
        }
        tracing::debug!(
            %instance_id,
            retries = *retries,
            "subscribe (task-per-tx): fresh k_closest round returned no usable candidates"
        );
    }

    // 3. Exhausted.
    None
}

/// Publish the driver's outcome to the client, routing on the explicit
/// [`DriverOutcome`] variant rather than inferring delivery state from
/// [`OpManager::is_completed`].
///
/// - [`DriverOutcome::Publish`] routes the contained `HostResult` through
///   [`OpManager::send_client_result`], which both pushes to
///   `result_router_tx` and emits `NodeEvent::TransactionCompleted(client_tx)`
///   so `p2p_protoc`'s `tx_to_client` table is reclaimed.
/// - [`DriverOutcome::SkipAlreadyDelivered`] is a deliberate no-op:
///   `complete_local_subscription` has already delivered the result via
///   `NodeEvent::LocalSubscribeComplete`, and a second send would
///   publish a duplicate `HostResponse::ContractResponse(SubscribeResponse)`
///   to the client.
/// - [`DriverOutcome::InfrastructureError`] is converted into a
///   synthesized client-facing `HostResult::Err` and then published via
///   `send_client_result`. This path is for errors that do not fit the
///   user-visible error shape (e.g., `OpError::NotificationError` from
///   `send_and_await`) — everything else the driver builds an explicit
///   `DriverOutcome::Publish(Err(...))` for.
fn deliver_outcome(
    op_manager: &OpManager,
    client_tx: Transaction,
    instance_id: ContractInstanceId,
    outcome: DriverOutcome,
) {
    match outcome {
        DriverOutcome::Publish(result) => {
            op_manager.send_client_result(client_tx, result);
        }
        DriverOutcome::SkipAlreadyDelivered => {
            tracing::debug!(
                tx = %client_tx,
                "subscribe (task-per-tx): local completion already published; \
                 skipping result_router_tx"
            );
        }
        DriverOutcome::InfrastructureError(err) => {
            tracing::warn!(
                tx = %client_tx,
                contract = %instance_id,
                error = %err,
                "subscribe (task-per-tx): infrastructure error; \
                 publishing synthesized client error"
            );
            let synthesized: HostResult = Err(ErrorKind::OperationError {
                cause: format!("subscribe failed: {err}").into(),
            }
            .into());
            op_manager.send_client_result(client_tx, synthesized);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::message::NetMessageV1;
    use crate::operations::connect::ConnectMsg;

    fn fresh_tx() -> Transaction {
        Transaction::new::<SubscribeMsg>()
    }

    #[test]
    fn classify_reply_subscribed() {
        use freenet_stdlib::prelude::{CodeHash, ContractInstanceId, ContractKey};
        let key = ContractKey::from_id_and_code(
            ContractInstanceId::new([3u8; 32]),
            CodeHash::new([4u8; 32]),
        );
        let tx = fresh_tx();
        let msg = NetMessage::V1(NetMessageV1::Subscribe(SubscribeMsg::Response {
            id: tx,
            instance_id: *key.id(),
            result: SubscribeMsgResult::Subscribed { key },
        }));
        match classify_reply(&msg) {
            ReplyClass::Subscribed { key: got } => assert_eq!(got, key),
            other @ (ReplyClass::NotFound | ReplyClass::Unexpected) => {
                panic!("expected Subscribed, got {other:?}")
            }
        }
    }

    #[test]
    fn classify_reply_not_found() {
        let instance_id = freenet_stdlib::prelude::ContractInstanceId::new([4u8; 32]);
        let tx = fresh_tx();
        let msg = NetMessage::V1(NetMessageV1::Subscribe(SubscribeMsg::Response {
            id: tx,
            instance_id,
            result: SubscribeMsgResult::NotFound,
        }));
        assert!(matches!(classify_reply(&msg), ReplyClass::NotFound));
    }

    #[test]
    fn classify_reply_unexpected_for_request() {
        // A `Request` arriving as a "reply" is structurally wrong — it's
        // what the ORIGINATOR sends, not receives. The classifier must
        // flag it so the caller can surface an error rather than silently
        // retry.
        let instance_id = freenet_stdlib::prelude::ContractInstanceId::new([5u8; 32]);
        let tx = fresh_tx();
        let msg = NetMessage::V1(NetMessageV1::Subscribe(SubscribeMsg::Request {
            id: tx,
            instance_id,
            htl: 5,
            visited: super::VisitedPeers::new(&tx),
            is_renewal: false,
        }));
        assert!(matches!(classify_reply(&msg), ReplyClass::Unexpected));
    }

    #[test]
    fn classify_reply_unexpected_for_non_subscribe_variant() {
        // An arbitrary non-subscribe message in the reply slot indicates
        // a routing bug upstream; classifier must surface it.
        let tx = Transaction::new::<ConnectMsg>();
        let msg = NetMessage::V1(NetMessageV1::Aborted(tx));
        assert!(matches!(classify_reply(&msg), ReplyClass::Unexpected));
    }

    // Note on retry-logic coverage: `advance_to_next_peer` is the core
    // decision helper. Pure unit-testing it requires constructing a
    // minimal `OpManager` with a stubbed `Ring::k_closest_potentially_hosting`
    // — heavier than the existing per-op unit-test harness supports out
    // of the box. The legacy logic it mirrors is already covered by the
    // integration tests in `subscribe/tests.rs`:
    //
    // - `test_subscription_routing_calls_k_closest_with_skip_list`
    // - the alternatives/bloom-filter retry tests
    //
    // Those tests exercise the equivalent legacy code paths. The
    // task-per-tx path is validated end-to-end by the
    // `simulation_integration` subscribe scenarios, which run through
    // `subscribe_with_id` → `start_client_subscribe` after Phase 2b
    // lands. If `advance_to_next_peer` grows more logic in the future
    // (e.g., when Phase 2.5 adds sub-op interactions), a `MockRing`-
    // backed unit test should be added here.
}
