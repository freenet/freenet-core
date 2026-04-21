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
    complete_local_subscription, prepare_initial_request, register_downstream_subscriber,
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
    // Not registered with `BackgroundTaskMonitor`: per the decision tree
    // in `.claude/rules/code-style.md` under "WHEN spawning tasks with
    // `GlobalExecutor::spawn`", the monitor is for tasks that must run
    // for the node's lifetime. This driver is per-transaction and
    // terminates on its own via one of:
    //
    //   1. Happy path: `send_and_await` returns a terminal reply, loop exits.
    //   2. Exhaustion path: `advance_to_next_peer` returns `None`, loop exits.
    //   3. Per-attempt timeout: each `send_and_await` is wrapped in
    //      `tokio::time::timeout(OPERATION_TTL, ...)`; a timed-out attempt
    //      advances to the next peer or falls into the exhaustion path.
    //   4. `OpCtx::send_and_await` infrastructure error (executor channel
    //      closed / receiver dropped): surfaces as `DriverOutcome::InfrastructureError`
    //      and exits via `deliver_outcome`.
    //
    // Amplification ceiling: the WS SUBSCRIBE request path enforces
    // `MAX_SUBSCRIPTIONS_PER_CLIENT = 50` upstream via
    // `notify_contract_handler(RegisterSubscriberListener)` →
    // `runtime.rs:623` (Executor::register_subscription), which rejects
    // the registration BEFORE `subscribe_with_id` is called. A client
    // that tries to open more than 50 in-flight subscribes gets a
    // `SubscriberLimit` error from the contract handler and never
    // reaches this spawn site. In-flight task count is therefore
    // bounded by `num_clients * 50`, not unbounded.
    //
    // Leak detection: if the driver somehow gets stuck without exiting
    // any of the four paths above, `test_pending_op_results_bounded`
    // (which watches `pending_op_inserts - pending_op_removes`) will
    // flag the leak during simulation tests. Because every
    // `send_and_await` attempt both inserts (via `handle_op_execution`)
    // and removes (via `release_pending_op_slot`) a `pending_op_results`
    // slot, a stuck task would show up as a widening insert/remove gap.
    GlobalExecutor::spawn(run_client_subscribe(op_manager, instance_id, client_tx));

    Ok(client_tx)
}

/// Drive a client-initiated subscribe to completion and publish the result
/// to `result_router_tx` keyed by `client_tx`.
///
/// Runs inside a spawned task. Never panics — any error is converted into
/// a `HostResult::Err` and delivered through `result_router_tx`.
///
/// `pub(crate)` so PUT's task-per-tx driver (Phase 3a) can `.await` this
/// inline for blocking-subscribe children without spawning a separate task.
pub(crate) async fn run_client_subscribe(
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

        // Dispatch via `send_to_and_await` so the Request reaches `current_target_addr`
        // on the wire instead of looping back to `process_message` as a local
        // `InboundMessage`. The local short-circuit would synthesize a success
        // reply when the contract is cached locally (e.g., after a prior GET),
        // but the home node would never see the request and never register us
        // as a downstream subscriber — so subsequent UPDATE broadcasts would
        // never reach this peer. See issue #3838 and the doc comment on
        // `OpCtx::send_to_and_await`.
        let mut ctx = op_manager.op_ctx(attempt_tx);
        let round_trip = tokio::time::timeout(
            OPERATION_TTL,
            ctx.send_to_and_await(current_target_addr, NetMessage::from(request)),
        )
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
                // `send_and_await` infrastructure failure (executor
                // channel closed, receiver dropped). Distinct from a
                // wire-level `NotFound` for observability purposes:
                // emit a structured `outcome=wire_error` field so log
                // analytics can tell them apart. Retry behaviour is
                // the same as NotFound — from "should we try another
                // peer?" the answer is yes — so the downstream logic
                // is shared (review finding T-4).
                tracing::warn!(
                    tx = %client_tx,
                    attempt_tx = %attempt_tx,
                    target = %current_target_addr,
                    retries,
                    attempts_at_hop,
                    outcome = "wire_error",
                    error = %err,
                    "subscribe (task-per-tx): send_and_await failed; advancing to next peer"
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
                // OPERATION_TTL elapsed without the peer producing a
                // terminal reply. Distinct from `wire_error` (which is
                // an infrastructure failure on the executor/send side)
                // and from `not_found` (a legitimate wire-level
                // response). `outcome=timeout` (review finding T-4).
                tracing::warn!(
                    tx = %client_tx,
                    attempt_tx = %attempt_tx,
                    target = %current_target_addr,
                    retries,
                    attempts_at_hop,
                    outcome = "timeout",
                    timeout_secs = OPERATION_TTL.as_secs(),
                    "subscribe (task-per-tx): attempt timed out; advancing to next peer"
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
                    target = %current_target_addr,
                    retries,
                    attempts_at_hop,
                    outcome = "subscribed",
                    "subscribe (task-per-tx): subscribed"
                );
                // Mirror the legacy Response-handler side effects from
                // `subscribe.rs`'s `SubscribeMsg::Response` arm. The
                // task-per-tx reply forwarding bypass in `node.rs` skips
                // `handle_op_request` for terminal Responses, so without
                // these calls the local interest manager and ring would
                // never learn about the subscription, breaking
                // ChangeInterests-driven update propagation. Both calls are
                // idempotent for repeat subscribes.
                //
                // Register the responding peer as our upstream. Without
                // this, `send_unsubscribe_upstream` cannot locate the peer
                // on client disconnect and no Unsubscribe is emitted (#3874).
                if let Some(pkl) = op_manager
                    .ring
                    .connection_manager
                    .get_peer_by_addr(current_target_addr)
                {
                    let peer_key = crate::ring::interest::PeerKey::from(pkl.pub_key.clone());
                    op_manager
                        .interest_manager
                        .register_peer_interest(&key, peer_key, None, true);
                }
                op_manager.ring.subscribe(key);
                op_manager.ring.complete_subscription_request(&key, true);
                let became_interested = op_manager.interest_manager.add_local_client(&key);
                if became_interested {
                    crate::operations::broadcast_change_interests(op_manager, vec![key], vec![])
                        .await;
                }
                return Ok(DriverOutcome::Publish(Ok(HostResponse::ContractResponse(
                    ContractResponse::SubscribeResponse {
                        key,
                        subscribed: true,
                    },
                ))));
            }
            ReplyClass::NotFound => {
                // Wire-level NotFound from a legitimate peer response.
                // Distinct from `wire_error` (executor/send failure)
                // and `timeout` (no terminal reply at all), so log
                // analytics can count "contract not found at this
                // peer" separately from infrastructure issues (review
                // finding T-4).
                tracing::debug!(
                    tx = %client_tx,
                    attempt_tx = %attempt_tx,
                    target = %current_target_addr,
                    retries,
                    attempts_at_hop,
                    outcome = "not_found",
                    "subscribe (task-per-tx): NotFound from peer; advancing to next peer"
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
/// Thin wrapper around [`advance_to_next_peer_impl`] that binds the
/// `fresh_candidates` hook to `op_manager.ring.k_closest_potentially_hosting`.
/// Splitting the bind out keeps the retry decision logic unit-testable
/// without needing a full `OpManager` + `Ring` setup (review finding
/// Testing #2).
async fn advance_to_next_peer(
    op_manager: &OpManager,
    instance_id: &ContractInstanceId,
    visited: &mut VisitedPeers,
    tried_peers: &mut HashSet<std::net::SocketAddr>,
    alternatives: &mut Vec<PeerKeyLocation>,
    retries: &mut usize,
    attempts_at_hop: &mut usize,
) -> Option<(PeerKeyLocation, std::net::SocketAddr)> {
    advance_to_next_peer_impl(
        instance_id,
        visited,
        tried_peers,
        alternatives,
        retries,
        attempts_at_hop,
        |instance_id, visited| {
            op_manager
                .ring
                .k_closest_potentially_hosting(instance_id, visited, MAX_BREADTH)
        },
    )
}

/// Core decision logic, parameterized on a `fresh_candidates` hook so
/// tests can drive it without a real `Ring`.
///
/// Priority:
/// 1. If `attempts_at_hop < MAX_BREADTH` and `alternatives` is non-empty,
///    take the next alternative (breadth retry at the same hop, FIFO
///    order to match legacy `handle_op_response`).
/// 2. Otherwise, if `retries < MAX_RETRIES`, call `fresh_candidates` with
///    the accumulated `visited` filter, reset `attempts_at_hop` to 1,
///    and increment `retries`.
/// 3. Otherwise, return `None` (exhausted).
///
/// Mutates all state references on a successful advance. The hook is a
/// synchronous `Fn` rather than `async fn` because the real
/// `k_closest_potentially_hosting` is also synchronous; this keeps the
/// signature simple and `impl`-backed. No `.await` inside the body means
/// the whole decision function can be a plain (non-async) fn.
fn advance_to_next_peer_impl<F>(
    instance_id: &ContractInstanceId,
    visited: &mut VisitedPeers,
    tried_peers: &mut HashSet<std::net::SocketAddr>,
    alternatives: &mut Vec<PeerKeyLocation>,
    retries: &mut usize,
    attempts_at_hop: &mut usize,
    mut fresh_candidates: F,
) -> Option<(PeerKeyLocation, std::net::SocketAddr)>
where
    F: FnMut(&ContractInstanceId, &VisitedPeers) -> Vec<PeerKeyLocation>,
{
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
        let mut fresh = fresh_candidates(instance_id, visited);
        while !fresh.is_empty() {
            let candidate = fresh.remove(0);
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

// ── Relay SUBSCRIBE driver (#1454 phase 5 follow-up slice A) ─────────────────

/// Counter: relay SUBSCRIBE drivers currently in flight. Decremented in
/// the RAII guard on driver exit. Diagnostic for `FREENET_MEMORY_STATS`.
pub static RELAY_SUBSCRIBE_INFLIGHT: std::sync::atomic::AtomicUsize =
    std::sync::atomic::AtomicUsize::new(0);

/// Counter: total relay SUBSCRIBE drivers ever spawned on this node.
pub static RELAY_SUBSCRIBE_SPAWNED_TOTAL: std::sync::atomic::AtomicUsize =
    std::sync::atomic::AtomicUsize::new(0);

/// Counter: total relay SUBSCRIBE drivers that exited (any path).
pub static RELAY_SUBSCRIBE_COMPLETED_TOTAL: std::sync::atomic::AtomicUsize =
    std::sync::atomic::AtomicUsize::new(0);

/// Counter: duplicate inbound `SubscribeMsg::Request` rejected by the
/// per-node dedup gate (`active_relay_subscribe_txs`).
pub static RELAY_SUBSCRIBE_DEDUP_REJECTS: std::sync::atomic::AtomicUsize =
    std::sync::atomic::AtomicUsize::new(0);

/// Counter: number of times `start_relay_subscribe` was invoked. Used by
/// structural pin tests to prove the dispatch gate routes fresh inbound
/// `SubscribeMsg::Request` through the task-per-tx driver rather than
/// legacy `handle_op_request`.
#[cfg(any(test, feature = "testing"))]
pub static RELAY_SUBSCRIBE_DRIVER_CALL_COUNT: std::sync::atomic::AtomicUsize =
    std::sync::atomic::AtomicUsize::new(0);

/// Spawn a relay driver for a fresh inbound `SubscribeMsg::Request`.
///
/// Gated by the dispatch site in `node.rs::handle_pure_network_message_v1`
/// on `source_addr.is_some() && !has_subscribe_op(id)`. The driver owns
/// local-hit response + single downstream forward + upstream bubble-up
/// in its task locals — no `SubscribeOp` is stored in
/// `OpManager.ops.subscribe` for this transaction.
///
/// # Slice A
///
/// Migrated:
/// - `SubscribeMsg::Request` relay arm: has-contract check (including the
///   brief wait-for-in-flight-PUT helper), `register_downstream_subscriber`
///   on local hit, forward-and-await-Response on next hop, bubble
///   `SubscribeMsg::Response` back upstream after registering the
///   requester as a downstream subscriber.
/// - `ForwardingAck` OMITTED deliberately — same reasoning as GET/PUT/
///   UPDATE slice A: the ack shares `incoming_tx` with the reply and
///   would satisfy upstream's capacity-1 `pending_op_results` waiter
///   before the real `Response`.
/// - Cross-peer retries OMITTED at the relay — legacy breadth/retry loop
///   at relay hops is the phase-5 memory-explosion amplifier (see
///   `project_1454_phase5_memory.md`). Originator-side driver (Phase 2b)
///   owns cross-peer retry; relay forwards once.
///
/// NOT migrated (stays on legacy path):
/// - `SubscribeMsg::Response` originator arm (Phase 2b driver handles it
///   via `pending_op_results` bypass).
/// - `SubscribeMsg::Unsubscribe` and `SubscribeMsg::ForwardingAck` — fire
///   and forget fast-path, no op state needed.
/// - Renewals, PUT sub-op subscribes, and intermediate-peer forwarding
///   through renewal/executor entry points — no `source_addr` or
///   pre-existing `SubscribeOp` makes them fall through.
pub(crate) async fn start_relay_subscribe(
    op_manager: Arc<OpManager>,
    incoming_tx: Transaction,
    instance_id: ContractInstanceId,
    htl: usize,
    visited: VisitedPeers,
    is_renewal: bool,
    upstream_addr: std::net::SocketAddr,
) -> Result<(), OpError> {
    #[cfg(any(test, feature = "testing"))]
    RELAY_SUBSCRIBE_DRIVER_CALL_COUNT.fetch_add(1, std::sync::atomic::Ordering::SeqCst);

    if !op_manager.active_relay_subscribe_txs.insert(incoming_tx) {
        RELAY_SUBSCRIBE_DEDUP_REJECTS.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        tracing::debug!(
            tx = %incoming_tx,
            %instance_id,
            %upstream_addr,
            phase = "relay_subscribe_dedup_reject",
            "SUBSCRIBE relay (task-per-tx): duplicate Request for in-flight tx, replying NotFound"
        );
        // Emit an immediate `NotFound` back to the rejected upstream so
        // its `send_to_and_await` returns fast instead of stalling the
        // full `OPERATION_TTL` (60 s). Silently dropping the duplicate
        // loses the upstream's retry budget and produces a spurious
        // "this contract is unreachable" verdict even when the winning
        // driver fulfills the subscription. The reply uses a fresh
        // `OpCtx` scoped to `incoming_tx` — the fire-and-forget send
        // does not touch the winning driver's `pending_op_results`
        // slot (that slot is keyed on the attempt_tx the WINNER
        // forwarded downstream, not on `incoming_tx` at this node).
        let response = NetMessage::from(SubscribeMsg::Response {
            id: incoming_tx,
            instance_id,
            result: SubscribeMsgResult::NotFound,
        });
        let mut ctx = op_manager.op_ctx(incoming_tx);
        if let Err(err) = ctx.send_fire_and_forget(upstream_addr, response).await {
            tracing::debug!(
                tx = %incoming_tx,
                %upstream_addr,
                error = %err,
                "SUBSCRIBE relay (task-per-tx): dedup-reject NotFound send failed"
            );
        }
        return Ok(());
    }

    // Construct the guard IMMEDIATELY after `insert` so a panic in any
    // intervening work (fmt-allocating logs, future OOM) cannot leak
    // the dedup-set entry. The guard's Drop clears the entry; without
    // the guard, there is no TTL and no recovery path. (Review M3,
    // skeptical #3932.)
    RELAY_SUBSCRIBE_INFLIGHT.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    RELAY_SUBSCRIBE_SPAWNED_TOTAL.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    let guard = RelaySubscribeInflightGuard {
        op_manager: op_manager.clone(),
        incoming_tx,
    };

    tracing::debug!(
        tx = %incoming_tx,
        %instance_id,
        htl,
        %upstream_addr,
        is_renewal,
        phase = "relay_subscribe_start",
        "SUBSCRIBE relay (task-per-tx): spawning driver"
    );

    GlobalExecutor::spawn(run_relay_subscribe(
        guard,
        op_manager,
        incoming_tx,
        instance_id,
        htl,
        visited,
        is_renewal,
        upstream_addr,
    ));
    Ok(())
}

/// RAII guard that decrements `RELAY_SUBSCRIBE_INFLIGHT`, bumps
/// `RELAY_SUBSCRIBE_COMPLETED_TOTAL`, and removes the driver's
/// `incoming_tx` from `active_relay_subscribe_txs` on drop.
struct RelaySubscribeInflightGuard {
    op_manager: Arc<OpManager>,
    incoming_tx: Transaction,
}

impl Drop for RelaySubscribeInflightGuard {
    fn drop(&mut self) {
        self.op_manager
            .active_relay_subscribe_txs
            .remove(&self.incoming_tx);
        RELAY_SUBSCRIBE_INFLIGHT.fetch_sub(1, std::sync::atomic::Ordering::Relaxed);
        RELAY_SUBSCRIBE_COMPLETED_TOTAL.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    }
}

#[allow(clippy::too_many_arguments)]
async fn run_relay_subscribe(
    guard: RelaySubscribeInflightGuard,
    op_manager: Arc<OpManager>,
    incoming_tx: Transaction,
    instance_id: ContractInstanceId,
    htl: usize,
    visited: VisitedPeers,
    is_renewal: bool,
    upstream_addr: std::net::SocketAddr,
) {
    let _guard = guard;

    if let Err(err) = drive_relay_subscribe(
        &op_manager,
        incoming_tx,
        instance_id,
        htl,
        visited,
        is_renewal,
        upstream_addr,
    )
    .await
    {
        tracing::warn!(
            tx = %incoming_tx,
            %instance_id,
            error = %err,
            phase = "relay_subscribe_error",
            "SUBSCRIBE relay (task-per-tx): driver returned error"
        );
    }

    // Release the pending_op_results slot at driver exit. Same reasoning
    // as GET/PUT relay — `send_to_and_await` leaves a closed sender in
    // the slot that only the 60s sweep would reclaim without this
    // explicit release.
    tokio::task::yield_now().await;
    op_manager.release_pending_op_slot(incoming_tx).await;
}

/// Drive a fresh inbound relay `SubscribeMsg::Request`: fast-path on
/// local contract hit, otherwise forward to the closest potentially
/// hosting peer and bubble the Response upstream.
///
/// # Intentional signal drop: `PeerHealthTracker` on downstream timeout
///
/// The legacy relay Request arm attached a `SubscribeStats {
/// target_peer, contract_location, request_sent_at }` to the
/// `AwaitingResponse` state so that downstream timeouts (reported via
/// `handle_abort`) fed `PeerHealthTracker` and the failure estimator.
/// The task-per-tx driver DROPS this signal on the relay node: on
/// `send_to_and_await` timeout we bubble `NotFound` upstream and exit
/// without touching `PeerHealthTracker`.
///
/// This is deliberate for slice A. Cross-peer retry at the relay was
/// the phase-5 memory-explosion amplifier (see `project_1454_phase5_memory.md`
/// and PR #3896's fix stack) — the task-per-tx design moves ALL
/// cross-peer retry to the originator's client-init driver. The
/// originator's `send_to_and_await` timeout path there is the correct
/// site for peer-health updates; reporting them at a relay that does
/// not retry adds no actionable signal and makes the symmetry with
/// GET/PUT/UPDATE slice A harder to reason about.
///
/// Follow-up: if peer-health reporting at the relay becomes necessary
/// for other reasons (e.g. targeting a specific slow peer cluster
/// without originating traffic), extend `OpCtx::send_to_and_await`
/// to emit a stat update on `Err(_elapsed)` via a hook rather than
/// reintroducing the signal on each relay driver.
async fn drive_relay_subscribe(
    op_manager: &Arc<OpManager>,
    incoming_tx: Transaction,
    instance_id: ContractInstanceId,
    htl: usize,
    visited: VisitedPeers,
    is_renewal: bool,
    upstream_addr: std::net::SocketAddr,
) -> Result<(), OpError> {
    tracing::info!(
        tx = %incoming_tx,
        %instance_id,
        htl,
        %upstream_addr,
        is_renewal,
        phase = "relay_subscribe_request",
        "SUBSCRIBE relay (task-per-tx): processing Request"
    );

    // ── Step 1: Local-hit fast path (with brief wait-for-in-flight-PUT) ──
    if let Some(key) = super::wait_for_contract_with_timeout(
        op_manager,
        instance_id,
        super::CONTRACT_WAIT_TIMEOUT_MS,
    )
    .await?
    {
        // Register the subscribing peer as a downstream subscriber.
        // The relay task-per-tx path does not know the requester's
        // `TransportPublicKey` (the legacy path had it because the op
        // state carried `requester_pub_key`); `register_downstream_subscriber`
        // falls back to addr-based lookup in that case, which is adequate
        // for relay hops where the upstream is a direct connection.
        register_downstream_subscriber(
            op_manager,
            &key,
            upstream_addr,
            None,
            Some(upstream_addr),
            &incoming_tx,
            " (task-per-tx relay local hit)",
        )
        .await;

        tracing::info!(
            tx = %incoming_tx,
            contract = %key,
            is_renewal,
            phase = "relay_subscribe_local_hit",
            "SUBSCRIBE relay (task-per-tx): fulfilled locally, sending Response"
        );
        return relay_subscribe_send_response(
            op_manager,
            incoming_tx,
            instance_id,
            SubscribeMsgResult::Subscribed { key },
            upstream_addr,
        )
        .await;
    }

    // ── Step 2: HTL / candidate selection ────────────────────────────────
    if htl == 0 {
        tracing::warn!(
            tx = %incoming_tx,
            contract = %instance_id,
            htl = 0,
            phase = "relay_subscribe_not_found",
            "SUBSCRIBE relay (task-per-tx): HTL exhausted"
        );
        if let Some(event) = crate::tracing::NetEventLog::subscribe_not_found(
            &incoming_tx,
            &op_manager.ring,
            instance_id,
            Some(op_manager.ring.max_hops_to_live),
        ) {
            op_manager
                .ring
                .register_events(either::Either::Left(event))
                .await;
        }
        return relay_subscribe_send_response(
            op_manager,
            incoming_tx,
            instance_id,
            SubscribeMsgResult::NotFound,
            upstream_addr,
        )
        .await;
    }

    // Restore hash keys after deserialization + mark ourselves + upstream
    // as visited so we never loop back.
    let own_addr = op_manager.ring.connection_manager.peer_addr()?;
    let mut new_visited = visited.with_transaction(&incoming_tx);
    new_visited.mark_visited(own_addr);
    new_visited.mark_visited(upstream_addr);

    let mut candidates =
        op_manager
            .ring
            .k_closest_potentially_hosting(&instance_id, &new_visited, MAX_BREADTH);

    if candidates.is_empty() {
        tracing::warn!(
            tx = %incoming_tx,
            contract = %instance_id,
            phase = "relay_subscribe_not_found",
            "SUBSCRIBE relay (task-per-tx): no closer peers to forward"
        );
        if let Some(event) = crate::tracing::NetEventLog::subscribe_not_found(
            &incoming_tx,
            &op_manager.ring,
            instance_id,
            None,
        ) {
            op_manager
                .ring
                .register_events(either::Either::Left(event))
                .await;
        }
        return relay_subscribe_send_response(
            op_manager,
            incoming_tx,
            instance_id,
            SubscribeMsgResult::NotFound,
            upstream_addr,
        )
        .await;
    }

    let next_hop = candidates.remove(0);
    let next_addr = match next_hop.socket_addr() {
        Some(addr) => addr,
        None => {
            tracing::error!(
                tx = %incoming_tx,
                %instance_id,
                target_pub_key = %next_hop.pub_key(),
                "SUBSCRIBE relay (task-per-tx): next hop has no socket address"
            );
            return relay_subscribe_send_response(
                op_manager,
                incoming_tx,
                instance_id,
                SubscribeMsgResult::NotFound,
                upstream_addr,
            )
            .await;
        }
    };
    new_visited.mark_visited(next_addr);

    // ── Step 3: Forward downstream, await Response ───────────────────────
    let new_htl = htl.saturating_sub(1);

    if let Some(event) = crate::tracing::NetEventLog::subscribe_request(
        &incoming_tx,
        &op_manager.ring,
        instance_id,
        next_hop.clone(),
        new_htl,
    ) {
        op_manager
            .ring
            .register_events(either::Either::Left(event))
            .await;
    }

    tracing::debug!(
        tx = %incoming_tx,
        %instance_id,
        peer_addr = %next_addr,
        htl = new_htl,
        phase = "relay_subscribe_forward",
        "SUBSCRIBE relay (task-per-tx): forwarding to next hop"
    );

    let forward = NetMessage::from(SubscribeMsg::Request {
        id: incoming_tx,
        instance_id,
        htl: new_htl,
        visited: new_visited,
        is_renewal,
    });

    let mut ctx = op_manager.op_ctx(incoming_tx);
    let round_trip =
        tokio::time::timeout(OPERATION_TTL, ctx.send_to_and_await(next_addr, forward)).await;

    // Release the pending_op_results slot installed by send_to_and_await.
    // Mirrors PUT/GET relay — the upstream fire-and-forget reply below
    // would otherwise leak a slot entry per driver run.
    op_manager.release_pending_op_slot(incoming_tx).await;

    let reply = match round_trip {
        Ok(Ok(reply)) => reply,
        Ok(Err(err)) => {
            tracing::warn!(
                tx = %incoming_tx,
                %instance_id,
                target = %next_addr,
                error = %err,
                "SUBSCRIBE relay (task-per-tx): send_to_and_await failed"
            );
            return relay_subscribe_send_response(
                op_manager,
                incoming_tx,
                instance_id,
                SubscribeMsgResult::NotFound,
                upstream_addr,
            )
            .await;
        }
        Err(_elapsed) => {
            tracing::warn!(
                tx = %incoming_tx,
                %instance_id,
                target = %next_addr,
                timeout_secs = OPERATION_TTL.as_secs(),
                "SUBSCRIBE relay (task-per-tx): downstream timed out"
            );
            return relay_subscribe_send_response(
                op_manager,
                incoming_tx,
                instance_id,
                SubscribeMsgResult::NotFound,
                upstream_addr,
            )
            .await;
        }
    };

    // ── Step 4: Classify reply, register requester if Subscribed, bubble up ──
    let result = match reply {
        NetMessage::V1(NetMessageV1::Subscribe(SubscribeMsg::Response {
            result: SubscribeMsgResult::Subscribed { key },
            ..
        })) => {
            // Relay-side Subscribed registration — mirror legacy
            // subscribe.rs:1690 arm. DO NOT call ring.subscribe /
            // record_subscription / announce_contract_hosted here; a
            // relay is not itself a subscriber. See subscribe.rs:1655–1688
            // for the full reasoning (prevents the #3763 subscription
            // storm feedback loop).
            register_downstream_subscriber(
                op_manager,
                &key,
                upstream_addr,
                None,
                Some(upstream_addr),
                &incoming_tx,
                " (task-per-tx relay registration on Response)",
            )
            .await;

            tracing::info!(
                tx = %incoming_tx,
                contract = %key,
                phase = "relay_subscribe_bubble",
                "SUBSCRIBE relay (task-per-tx): downstream Subscribed; bubbling upstream"
            );
            SubscribeMsgResult::Subscribed { key }
        }
        NetMessage::V1(NetMessageV1::Subscribe(SubscribeMsg::Response {
            result: SubscribeMsgResult::NotFound,
            ..
        })) => {
            tracing::debug!(
                tx = %incoming_tx,
                %instance_id,
                phase = "relay_subscribe_bubble_not_found",
                "SUBSCRIBE relay (task-per-tx): downstream NotFound; bubbling upstream"
            );
            SubscribeMsgResult::NotFound
        }
        other => {
            tracing::warn!(
                tx = %incoming_tx,
                %instance_id,
                reply_variant = ?std::mem::discriminant(&other),
                "SUBSCRIBE relay (task-per-tx): unexpected reply variant; treating as NotFound"
            );
            SubscribeMsgResult::NotFound
        }
    };

    relay_subscribe_send_response(op_manager, incoming_tx, instance_id, result, upstream_addr).await
}

/// Send `SubscribeMsg::Response` upstream (fire-and-forget: upstream
/// relay awaits via its own `send_to_and_await`, no reply expected).
async fn relay_subscribe_send_response(
    op_manager: &OpManager,
    incoming_tx: Transaction,
    instance_id: ContractInstanceId,
    result: SubscribeMsgResult,
    upstream_addr: std::net::SocketAddr,
) -> Result<(), OpError> {
    let response = NetMessage::from(SubscribeMsg::Response {
        id: incoming_tx,
        instance_id,
        result,
    });
    let mut ctx = op_manager.op_ctx(incoming_tx);
    ctx.send_fire_and_forget(upstream_addr, response).await
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

    // ──────────────────────────────────────────────────────────────
    // Retry-logic coverage for `advance_to_next_peer_impl` (review
    // finding Testing #2). The impl is parameterized on a
    // `fresh_candidates` closure so these tests can drive it without
    // building a full `OpManager` + `Ring`. Each test pins one
    // distinct transition in the retry decision tree.
    // ──────────────────────────────────────────────────────────────

    /// Helper: construct a synthetic `PeerKeyLocation` with a
    /// predictable socket address. Uses `PeerKeyLocation::random()` for
    /// the `pub_key` (cached per-thread so it's cheap) and then
    /// overrides the address with one we control so test assertions
    /// can match on it. The actual location is derived from the
    /// address by the ring code, which is irrelevant for these tests —
    /// `advance_to_next_peer_impl` only looks at `socket_addr()`.
    fn peer_at(addr: &str) -> PeerKeyLocation {
        let mut p = PeerKeyLocation::random();
        p.set_addr(addr.parse().expect("valid socket addr"));
        p
    }

    fn contract_id() -> ContractInstanceId {
        ContractInstanceId::new([9u8; 32])
    }

    #[test]
    fn advance_breadth_retry_returns_next_alternative_fifo() {
        // Setup: three alternatives, none tried yet, attempts_at_hop
        // below MAX_BREADTH, retries at 0. The helper should pop the
        // FIRST alternative (FIFO — closest-first) and return it.
        let id = contract_id();
        let tx = fresh_tx();
        let mut visited = VisitedPeers::new(&tx);
        let mut tried_peers: HashSet<std::net::SocketAddr> = HashSet::new();
        let a = peer_at("10.0.0.1:1001");
        let b = peer_at("10.0.0.2:1002");
        let c = peer_at("10.0.0.3:1003");
        let a_addr = a.socket_addr().unwrap();
        let mut alternatives = vec![a.clone(), b.clone(), c.clone()];
        let mut retries = 0usize;
        let mut attempts_at_hop = 1usize;

        let result = advance_to_next_peer_impl(
            &id,
            &mut visited,
            &mut tried_peers,
            &mut alternatives,
            &mut retries,
            &mut attempts_at_hop,
            |_, _| panic!("breadth retry path must not call fresh_candidates"),
        );

        let (picked, picked_addr) = result.expect("breadth retry should return an alternative");
        assert_eq!(picked_addr, a_addr, "must pick FIRST alternative (FIFO)");
        assert_eq!(picked.socket_addr(), Some(a_addr));
        assert_eq!(attempts_at_hop, 2, "attempts_at_hop must increment");
        assert_eq!(retries, 0, "retries must not change on breadth retry");
        assert_eq!(alternatives.len(), 2, "one alternative consumed");
        assert!(tried_peers.contains(&a_addr));
        assert!(visited.probably_visited(a_addr));
    }

    #[test]
    fn advance_breadth_retry_skips_already_visited() {
        // Setup: first alternative is already in visited bloom; helper
        // must skip it and take the next one.
        let id = contract_id();
        let tx = fresh_tx();
        let mut visited = VisitedPeers::new(&tx);
        let a = peer_at("10.0.0.1:1001");
        let b = peer_at("10.0.0.2:1002");
        let a_addr = a.socket_addr().unwrap();
        let b_addr = b.socket_addr().unwrap();
        visited.mark_visited(a_addr); // A was already tried earlier
        let mut tried_peers: HashSet<std::net::SocketAddr> = HashSet::new();
        tried_peers.insert(a_addr);
        let mut alternatives = vec![a, b];
        let mut retries = 0usize;
        let mut attempts_at_hop = 1usize;

        let result = advance_to_next_peer_impl(
            &id,
            &mut visited,
            &mut tried_peers,
            &mut alternatives,
            &mut retries,
            &mut attempts_at_hop,
            |_, _| panic!("should find B before falling through"),
        );

        let (_, picked_addr) = result.expect("should pick B after skipping A");
        assert_eq!(picked_addr, b_addr);
        assert!(alternatives.is_empty(), "both A and B consumed");
    }

    #[test]
    fn advance_fresh_round_triggered_when_alternatives_exhausted() {
        // Setup: alternatives empty, attempts_at_hop below MAX_BREADTH.
        // The impl should bypass the breadth branch (nothing to pop)
        // and call `fresh_candidates`, resetting attempts_at_hop to 1
        // and incrementing retries.
        let id = contract_id();
        let tx = fresh_tx();
        let mut visited = VisitedPeers::new(&tx);
        let mut tried_peers: HashSet<std::net::SocketAddr> = HashSet::new();
        let mut alternatives: Vec<PeerKeyLocation> = Vec::new();
        let mut retries = 0usize;
        let mut attempts_at_hop = 1usize;

        let fresh_peer = peer_at("10.0.0.5:1005");
        let fresh_addr = fresh_peer.socket_addr().unwrap();
        let mut fresh_calls = 0;

        let result = advance_to_next_peer_impl(
            &id,
            &mut visited,
            &mut tried_peers,
            &mut alternatives,
            &mut retries,
            &mut attempts_at_hop,
            |got_id, _got_visited| {
                fresh_calls += 1;
                assert_eq!(got_id, &contract_id(), "passes through instance_id");
                vec![fresh_peer.clone()]
            },
        );

        assert_eq!(
            fresh_calls, 1,
            "fresh_candidates must be called exactly once"
        );
        let (_, picked_addr) = result.expect("fresh round should find a peer");
        assert_eq!(picked_addr, fresh_addr);
        assert_eq!(retries, 1, "retries incremented on fresh round");
        assert_eq!(
            attempts_at_hop, 1,
            "attempts_at_hop reset to 1 on fresh round"
        );
    }

    #[test]
    fn advance_fresh_round_after_max_breadth_hit() {
        // Setup: attempts_at_hop at MAX_BREADTH (the breadth guard
        // rejects further breadth retries even with alternatives
        // available). The impl must immediately fall through to the
        // fresh_candidates branch.
        let id = contract_id();
        let tx = fresh_tx();
        let mut visited = VisitedPeers::new(&tx);
        let mut tried_peers: HashSet<std::net::SocketAddr> = HashSet::new();
        // Alternatives are present, but breadth is already exhausted.
        let unused_alt = peer_at("10.0.0.1:1001");
        let mut alternatives = vec![unused_alt.clone()];
        let mut retries = 0usize;
        let mut attempts_at_hop = MAX_BREADTH;

        let fresh_peer = peer_at("10.0.0.5:1005");
        let fresh_addr = fresh_peer.socket_addr().unwrap();

        let result = advance_to_next_peer_impl(
            &id,
            &mut visited,
            &mut tried_peers,
            &mut alternatives,
            &mut retries,
            &mut attempts_at_hop,
            |_, _| vec![fresh_peer.clone()],
        );

        let (_, picked_addr) = result.expect("fresh round should run");
        assert_eq!(picked_addr, fresh_addr);
        // The unused alt must still be in `alternatives` OR have been
        // replaced by the remainder of `fresh` — check that we did NOT
        // consume it via the breadth branch.
        assert_eq!(retries, 1, "went through fresh round, not breadth");
        assert_eq!(attempts_at_hop, 1, "attempts_at_hop reset by fresh round");
    }

    #[test]
    fn advance_exhausted_after_max_retries() {
        // Setup: retries at MAX_RETRIES, no alternatives. Both guards
        // reject; helper must return None.
        let id = contract_id();
        let tx = fresh_tx();
        let mut visited = VisitedPeers::new(&tx);
        let mut tried_peers: HashSet<std::net::SocketAddr> = HashSet::new();
        let mut alternatives: Vec<PeerKeyLocation> = Vec::new();
        let mut retries = MAX_RETRIES;
        let mut attempts_at_hop = 1usize;

        let result = advance_to_next_peer_impl(
            &id,
            &mut visited,
            &mut tried_peers,
            &mut alternatives,
            &mut retries,
            &mut attempts_at_hop,
            |_, _| panic!("fresh_candidates must not be called when retries == MAX"),
        );

        assert!(result.is_none(), "exhausted case returns None");
        assert_eq!(retries, MAX_RETRIES, "retries unchanged when exhausted");
    }

    #[test]
    fn advance_exhausted_when_fresh_round_returns_empty() {
        // Setup: below MAX_RETRIES, alternatives empty. fresh_candidates
        // returns empty Vec (e.g., the ring has no candidates left after
        // accounting for visited filter). Helper must return None AND
        // have incremented retries.
        let id = contract_id();
        let tx = fresh_tx();
        let mut visited = VisitedPeers::new(&tx);
        let mut tried_peers: HashSet<std::net::SocketAddr> = HashSet::new();
        let mut alternatives: Vec<PeerKeyLocation> = Vec::new();
        let mut retries = 0usize;
        let mut attempts_at_hop = 1usize;

        let result = advance_to_next_peer_impl(
            &id,
            &mut visited,
            &mut tried_peers,
            &mut alternatives,
            &mut retries,
            &mut attempts_at_hop,
            |_, _| Vec::new(),
        );

        assert!(result.is_none());
        assert_eq!(
            retries, 1,
            "retries incremented even though fresh round was empty \
             — the round was 'attempted', it just found nothing"
        );
    }

    #[test]
    fn advance_fresh_round_leftover_becomes_new_alternatives() {
        // Setup: fresh_candidates returns 3 peers; helper picks the
        // first, and the remaining 2 MUST be written back to
        // `alternatives` so subsequent breadth retries can use them.
        let id = contract_id();
        let tx = fresh_tx();
        let mut visited = VisitedPeers::new(&tx);
        let mut tried_peers: HashSet<std::net::SocketAddr> = HashSet::new();
        let mut alternatives: Vec<PeerKeyLocation> = Vec::new();
        let mut retries = 0usize;
        let mut attempts_at_hop = 1usize;

        let p1 = peer_at("10.0.0.1:1001");
        let p2 = peer_at("10.0.0.2:1002");
        let p3 = peer_at("10.0.0.3:1003");
        let p1_addr = p1.socket_addr().unwrap();
        let p2_addr = p2.socket_addr().unwrap();
        let p3_addr = p3.socket_addr().unwrap();

        let result = advance_to_next_peer_impl(
            &id,
            &mut visited,
            &mut tried_peers,
            &mut alternatives,
            &mut retries,
            &mut attempts_at_hop,
            |_, _| vec![p1.clone(), p2.clone(), p3.clone()],
        );

        let (_, picked) = result.expect("fresh round returns first candidate");
        assert_eq!(picked, p1_addr);
        assert_eq!(
            alternatives.len(),
            2,
            "rest of fresh becomes new alternatives"
        );
        let alt_addrs: Vec<_> = alternatives
            .iter()
            .filter_map(|p| p.socket_addr())
            .collect();
        assert!(alt_addrs.contains(&p2_addr));
        assert!(alt_addrs.contains(&p3_addr));
    }

    // ── Relay SUBSCRIBE driver structural pin tests (#1454 phase 5
    // follow-up slice A). Anchor on the `start_relay_subscribe`
    // entry-point fn so module-level docs referencing variant names
    // don't contaminate the scan.

    fn relay_section(src: &str) -> &str {
        let start = src
            .find("pub(crate) async fn start_relay_subscribe(")
            .expect("start_relay_subscribe not found");
        let end = src
            .find("\n#[cfg(test)]")
            .expect("test module marker not found");
        &src[start..end]
    }

    /// Pin: dispatch entry must insert into `active_relay_subscribe_txs`
    /// BEFORE spawning. Same dedup-gate-ordering invariant as GET/PUT/
    /// UPDATE — without this, duplicate inbound `SubscribeMsg::Request`
    /// for an in-flight tx would spawn redundant drivers.
    #[test]
    fn start_relay_subscribe_checks_dedup_gate() {
        let src = include_str!("op_ctx_task.rs");
        let relay = relay_section(src);
        let window_start = relay
            .find("pub(crate) async fn start_relay_subscribe(")
            .expect("entry-point not found");
        let spawn_pos = relay[window_start..]
            .find("GlobalExecutor::spawn(run_relay_subscribe(")
            .expect("spawn site not found")
            + window_start;
        let insert_pos = relay[window_start..]
            .find("active_relay_subscribe_txs.insert(incoming_tx)")
            .expect("dedup insert site not found")
            + window_start;
        assert!(
            insert_pos < spawn_pos,
            "active_relay_subscribe_txs.insert MUST happen before GlobalExecutor::spawn"
        );
    }

    /// Pin: dedup rejection must emit `SubscribeMsgResult::NotFound`
    /// back to the rejected upstream. Silently dropping the duplicate
    /// request stalls the upstream's `send_to_and_await` for the full
    /// `OPERATION_TTL` (60 s), wasting its retry budget.
    #[test]
    fn dedup_rejection_emits_not_found_reply() {
        let src = include_str!("op_ctx_task.rs");
        let relay = relay_section(src);
        let after_insert = relay
            .split("active_relay_subscribe_txs.insert(incoming_tx)")
            .nth(1)
            .expect("dedup insert site not found");
        // Look at the body between the insert and the early return.
        let window_end = after_insert
            .find("return Ok(())")
            .expect("early return after dedup-reject not found");
        let window = &after_insert[..window_end];
        assert!(
            window.contains("SubscribeMsgResult::NotFound"),
            "dedup-reject path must emit SubscribeMsgResult::NotFound to \
             the rejected upstream so its send_to_and_await returns fast"
        );
        assert!(
            window.contains("send_fire_and_forget"),
            "dedup-reject NotFound must be sent via send_fire_and_forget \
             (not send_to_and_await — upstream's own waiter owns its slot)"
        );
    }

    /// Pin: dedup rejection must bump `RELAY_SUBSCRIBE_DEDUP_REJECTS`.
    #[test]
    fn dedup_rejection_increments_counter() {
        let src = include_str!("op_ctx_task.rs");
        let relay = relay_section(src);
        let after_insert = relay
            .split("active_relay_subscribe_txs.insert(incoming_tx)")
            .nth(1)
            .expect("dedup insert site not found");
        let window = &after_insert[..500.min(after_insert.len())];
        assert!(
            window.contains("RELAY_SUBSCRIBE_DEDUP_REJECTS.fetch_add"),
            "dedup gate must increment RELAY_SUBSCRIBE_DEDUP_REJECTS on rejection"
        );
    }

    /// Pin: RAII guard must clear `active_relay_subscribe_txs` + bump
    /// completion counters on drop.
    #[test]
    fn raii_guard_clears_dedup_set_on_drop() {
        let src = include_str!("op_ctx_task.rs");
        let drop_start = src
            .find("impl Drop for RelaySubscribeInflightGuard")
            .expect("RelaySubscribeInflightGuard Drop impl not found");
        let drop_body = &src[drop_start..drop_start + 600];
        assert!(
            drop_body.contains("active_relay_subscribe_txs"),
            "RelaySubscribeInflightGuard::drop must remove from active_relay_subscribe_txs"
        );
        assert!(
            drop_body.contains("RELAY_SUBSCRIBE_INFLIGHT.fetch_sub"),
            "RelaySubscribeInflightGuard::drop must decrement RELAY_SUBSCRIBE_INFLIGHT"
        );
        assert!(
            drop_body.contains("RELAY_SUBSCRIBE_COMPLETED_TOTAL.fetch_add"),
            "RelaySubscribeInflightGuard::drop must increment RELAY_SUBSCRIBE_COMPLETED_TOTAL"
        );
    }

    /// Pin: driver forwards downstream via `send_to_and_await` — SUBSCRIBE
    /// relay IS req/response. Fire-and-forget here would lose the reply.
    #[test]
    fn drive_relay_subscribe_forwards_via_send_to_and_await() {
        let src = include_str!("op_ctx_task.rs");
        let driver_start = src
            .find("async fn drive_relay_subscribe(")
            .expect("drive_relay_subscribe not found");
        let driver_end = src[driver_start..]
            .find("\nasync fn relay_subscribe_send_response(")
            .expect("driver body end not found")
            + driver_start;
        let driver_src = &src[driver_start..driver_end];
        assert!(
            driver_src.contains("ctx.send_to_and_await("),
            "drive_relay_subscribe must forward downstream via send_to_and_await"
        );
    }

    /// Pin: relay forward must reuse `incoming_tx`. Minting a fresh tx
    /// at each hop was the phase-5 GET 3^HTL amplifier.
    #[test]
    fn drive_relay_subscribe_reuses_incoming_tx_on_forward() {
        let src = include_str!("op_ctx_task.rs");
        let driver_start = src
            .find("async fn drive_relay_subscribe(")
            .expect("drive_relay_subscribe not found");
        let driver_end = src[driver_start..]
            .find("\nasync fn relay_subscribe_send_response(")
            .expect("driver body end not found")
            + driver_start;
        let driver_src = &src[driver_start..driver_end];
        let forward_pos = driver_src
            .find("SubscribeMsg::Request {")
            .expect("forward SubscribeMsg::Request not found in driver");
        let forward_window = &driver_src[forward_pos..forward_pos + 400];
        assert!(
            forward_window.contains("id: incoming_tx"),
            "relay forward must reuse incoming_tx"
        );
        assert!(
            !forward_window.contains("Transaction::new::<SubscribeMsg>()"),
            "relay forward must NOT mint a fresh Transaction"
        );
    }

    /// Pin: relay upstream reply MUST use `send_fire_and_forget`.
    /// Upstream's own `send_to_and_await` owns the reply slot.
    #[test]
    fn relay_subscribe_send_response_is_fire_and_forget() {
        let src = include_str!("op_ctx_task.rs");
        let fn_start = src
            .find("async fn relay_subscribe_send_response(")
            .expect("relay_subscribe_send_response not found");
        let fn_end = src[fn_start..]
            .find("\n}\n")
            .expect("function body end not found")
            + fn_start;
        let fn_src = &src[fn_start..fn_end];
        assert!(
            fn_src.contains("send_fire_and_forget"),
            "relay_subscribe_send_response must use send_fire_and_forget for the upstream response"
        );
    }

    /// Pin: relay driver MUST NOT emit `ForwardingAck`. The ack would
    /// share `incoming_tx` with the reply and satisfy upstream's
    /// capacity-1 `pending_op_results` waiter before the real Response.
    #[test]
    fn relay_subscribe_does_not_emit_forwarding_ack() {
        let src = include_str!("op_ctx_task.rs");
        let relay = relay_section(src);
        assert!(
            !relay.contains("SubscribeMsg::ForwardingAck {"),
            "relay SUBSCRIBE driver must NOT construct a ForwardingAck — \
             sharing incoming_tx with the reply collides with upstream's waiter"
        );
    }

    /// Pin: relay driver MUST call `register_downstream_subscriber`
    /// on BOTH the local-hit and downstream-Subscribed paths. Missing
    /// this registration breaks UPDATE propagation to the original
    /// requester (see subscribe.rs:1655–1688 for full reasoning).
    #[test]
    fn relay_subscribe_registers_downstream_on_both_paths() {
        let src = include_str!("op_ctx_task.rs");
        let driver_start = src
            .find("async fn drive_relay_subscribe(")
            .expect("drive_relay_subscribe not found");
        let driver_end = src[driver_start..]
            .find("\nasync fn relay_subscribe_send_response(")
            .expect("driver body end not found")
            + driver_start;
        let driver_src = &src[driver_start..driver_end];
        let hits = driver_src
            .matches("register_downstream_subscriber(")
            .count();
        assert!(
            hits >= 2,
            "driver must call register_downstream_subscriber on BOTH local-hit \
             and downstream-Subscribed paths (found {hits})"
        );
    }

    /// Pin: relay driver MUST NOT call `ring.subscribe`, `record_subscription`,
    /// or `announce_contract_hosted` on behalf of the relayed Subscribed
    /// response. A relay is not itself a subscriber; doing so would
    /// install a lease and trigger #3763 subscription-storm feedback
    /// loops via the renewal cycle.
    #[test]
    fn relay_subscribe_does_not_install_lease_on_relayed_response() {
        let src = include_str!("op_ctx_task.rs");
        let driver_start = src
            .find("async fn drive_relay_subscribe(")
            .expect("drive_relay_subscribe not found");
        let driver_end = src[driver_start..]
            .find("\nasync fn relay_subscribe_send_response(")
            .expect("driver body end not found")
            + driver_start;
        let driver_src = &src[driver_start..driver_end];
        // Strip line comments so doc strings that mention these call-sites
        // as negative constraints do not trip the substring scan.
        let stripped: String = driver_src
            .lines()
            .map(|line| match line.find("//") {
                Some(idx) => &line[..idx],
                None => line,
            })
            .collect::<Vec<_>>()
            .join("\n");
        assert!(
            !stripped.contains("ring.subscribe("),
            "relay driver must NOT call ring.subscribe on relayed response"
        );
        assert!(
            !stripped.contains("complete_subscription_request"),
            "relay driver must NOT call complete_subscription_request on relayed response"
        );
        assert!(
            !stripped.contains("announce_contract_hosted"),
            "relay driver must NOT call announce_contract_hosted on relayed response"
        );
    }
}
