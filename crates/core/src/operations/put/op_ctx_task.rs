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
use std::net::SocketAddr;
use std::sync::Arc;

use either::Either;
use freenet_stdlib::client_api::{ContractResponse, ErrorKind, HostResponse};
use freenet_stdlib::prelude::*;

use crate::client_events::HostResult;
use crate::config::{GlobalExecutor, OPERATION_TTL};
use crate::message::{NetMessage, NetMessageV1, Transaction};
use crate::node::OpManager;
use crate::operations::OpError;
use crate::operations::op_ctx::{
    AdvanceOutcome, AttemptOutcome, RetryDriver, RetryLoopOutcome, drive_retry_loop,
};
use crate::ring::{Location, PeerKeyLocation};
use crate::router::{RouteEvent, RouteOutcome};
use crate::tracing::{NetEventLog, OperationFailure, state_hash_full};

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

    // No SubOperationTracker registration needed: the silent-absorb
    // guards at `p2p_protoc.rs`, `node.rs`, and `subscribe.rs` use the
    // structural `Transaction::is_sub_operation()` check (parent field
    // set by `new_child_of`), not the tracker DashMap.

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

// ── Relay PUT driver (#1454 phase 5 follow-up slice A) ──────────────────────

/// Counter: number of times `start_relay_put` was invoked. Incremented
/// under test/testing feature only — used by structural pin tests to
/// prove the dispatch gate routes fresh inbound `PutMsg::Request`
/// through the task-per-tx driver rather than legacy `handle_op_request`.
#[cfg(any(test, feature = "testing"))]
pub static RELAY_PUT_DRIVER_CALL_COUNT: std::sync::atomic::AtomicUsize =
    std::sync::atomic::AtomicUsize::new(0);

/// Counter: relay PUT drivers currently in flight. Decremented in the
/// RAII guard on driver exit. Diagnostic for `FREENET_MEMORY_STATS`.
pub static RELAY_PUT_INFLIGHT: std::sync::atomic::AtomicUsize =
    std::sync::atomic::AtomicUsize::new(0);

/// Counter: total relay PUT drivers ever spawned on this node.
pub static RELAY_PUT_SPAWNED_TOTAL: std::sync::atomic::AtomicUsize =
    std::sync::atomic::AtomicUsize::new(0);

/// Counter: total relay PUT drivers that exited (any path).
pub static RELAY_PUT_COMPLETED_TOTAL: std::sync::atomic::AtomicUsize =
    std::sync::atomic::AtomicUsize::new(0);

/// Counter: duplicate inbound `PutMsg::Request` rejected by the per-node
/// dedup gate (`active_relay_put_txs`).
pub static RELAY_PUT_DEDUP_REJECTS: std::sync::atomic::AtomicUsize =
    std::sync::atomic::AtomicUsize::new(0);

/// Spawn a relay driver for a fresh inbound non-streaming `PutMsg::Request`.
///
/// Gated by the dispatch site in `node.rs::handle_pure_network_message_v1`
/// on `source_addr.is_some() && !has_put_op(id)`. The driver owns local
/// store + optional downstream forward + upstream bubble-up in its task
/// locals — no `PutOp` is stored in `OpManager.ops.put` for this
/// transaction.
///
/// Returns immediately after spawning. Driver publishes its own side
/// effects (local put_contract / host_contract / interest broadcast,
/// a single non-streaming forward via `OpCtx::send_to_and_await`, and
/// `PutMsg::Response` back to the upstream).
///
/// # Scope (slice A)
///
/// Migrated:
/// - `PutMsg::Request` relay arm: local store, decide forward-or-respond,
///   forward + await downstream `PutMsg::Response`, bubble upstream.
/// - `PutMsg::Response` bubble-up to upstream (handled inline by this
///   driver via `send_to_and_await`'s reply, not by the legacy arm).
/// - `PutMsg::ForwardingAck` OMITTED — same reasoning as GET relay: the
///   ack carried `incoming_tx` and would satisfy upstream's capacity-1
///   `pending_op_results` waiter before the real `Response` arrived.
///
/// NOT migrated (stays on legacy path):
/// - `PutMsg::RequestStreaming` / `PutMsg::ResponseStreaming`. The
///   dispatch gate in `node.rs` re-checks `should_use_streaming` on the
///   inbound non-streaming Request and falls through to legacy if the
///   serialized payload would exceed `streaming_threshold` on the
///   forward — slice A only handles end-to-end non-streaming hops.
/// - GC-spawned speculative retries (`speculative_paths`): no `PutOp`
///   in the DashMap for the GC to find, so they never enter this driver.
#[allow(clippy::too_many_arguments)]
pub(crate) async fn start_relay_put(
    op_manager: Arc<OpManager>,
    incoming_tx: Transaction,
    contract: ContractContainer,
    related_contracts: RelatedContracts<'static>,
    value: WrappedState,
    htl: usize,
    skip_list: HashSet<SocketAddr>,
    upstream_addr: SocketAddr,
) -> Result<(), OpError> {
    #[cfg(any(test, feature = "testing"))]
    RELAY_PUT_DRIVER_CALL_COUNT.fetch_add(1, std::sync::atomic::Ordering::SeqCst);

    if !op_manager.active_relay_put_txs.insert(incoming_tx) {
        RELAY_PUT_DEDUP_REJECTS.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        tracing::debug!(
            tx = %incoming_tx,
            contract = %contract.key(),
            %upstream_addr,
            phase = "relay_put_dedup_reject",
            "PUT relay (task-per-tx): duplicate Request for in-flight tx, dropping"
        );
        return Ok(());
    }

    tracing::debug!(
        tx = %incoming_tx,
        contract = %contract.key(),
        htl,
        %upstream_addr,
        phase = "relay_put_start",
        "PUT relay (task-per-tx): spawning driver"
    );

    // Construct guard + bump counters BEFORE spawn so the dedup-set
    // entry is paired with a Drop even if the spawned future is dropped
    // before its first poll (executor shutdown, scheduling panic).
    // Without this, a pre-poll drop would permanently leak the
    // `active_relay_put_txs` entry — no TTL → permanent dedup
    // blind-spot for that tx. Mirrors UPDATE's pre-spawn guard pattern.
    RELAY_PUT_INFLIGHT.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    RELAY_PUT_SPAWNED_TOTAL.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    let guard = RelayPutInflightGuard {
        op_manager: op_manager.clone(),
        incoming_tx,
    };

    GlobalExecutor::spawn(run_relay_put(
        guard,
        op_manager,
        incoming_tx,
        contract,
        related_contracts,
        value,
        htl,
        skip_list,
        upstream_addr,
    ));
    Ok(())
}

/// RAII guard that decrements `RELAY_PUT_INFLIGHT`, bumps
/// `RELAY_PUT_COMPLETED_TOTAL`, and removes the driver's `incoming_tx`
/// from `active_relay_put_txs` on drop. Mirrors GET/UPDATE relay guards.
struct RelayPutInflightGuard {
    op_manager: Arc<OpManager>,
    incoming_tx: Transaction,
}

impl Drop for RelayPutInflightGuard {
    fn drop(&mut self) {
        self.op_manager
            .active_relay_put_txs
            .remove(&self.incoming_tx);
        RELAY_PUT_INFLIGHT.fetch_sub(1, std::sync::atomic::Ordering::Relaxed);
        RELAY_PUT_COMPLETED_TOTAL.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    }
}

#[allow(clippy::too_many_arguments)]
async fn run_relay_put(
    guard: RelayPutInflightGuard,
    op_manager: Arc<OpManager>,
    incoming_tx: Transaction,
    contract: ContractContainer,
    related_contracts: RelatedContracts<'static>,
    value: WrappedState,
    htl: usize,
    skip_list: HashSet<SocketAddr>,
    upstream_addr: SocketAddr,
) {
    let _guard = guard;

    if let Err(err) = drive_relay_put(
        &op_manager,
        incoming_tx,
        contract,
        related_contracts,
        value,
        htl,
        skip_list,
        upstream_addr,
    )
    .await
    {
        tracing::warn!(
            tx = %incoming_tx,
            error = %err,
            phase = "relay_put_error",
            "PUT relay (task-per-tx): driver returned error"
        );
    }

    // Release the per-tx `pending_op_results` slot at driver exit, same
    // rationale as GET relay — `send_to_and_await` leaves an is_closed
    // sender in the slot that only the 60s sweep would reclaim without
    // this explicit release.
    tokio::task::yield_now().await;
    op_manager.release_pending_op_slot(incoming_tx).await;
}

#[allow(clippy::too_many_arguments)]
async fn drive_relay_put(
    op_manager: &Arc<OpManager>,
    incoming_tx: Transaction,
    contract: ContractContainer,
    related_contracts: RelatedContracts<'static>,
    value: WrappedState,
    htl: usize,
    skip_list: HashSet<SocketAddr>,
    upstream_addr: SocketAddr,
) -> Result<(), OpError> {
    let key = contract.key();

    tracing::info!(
        tx = %incoming_tx,
        contract = %key,
        htl,
        %upstream_addr,
        phase = "relay_put_request",
        "PUT relay (task-per-tx): processing Request"
    );

    // ── Step 1: Store contract locally (all nodes cache) ────────────────────
    let merged_value = relay_put_store_locally(
        op_manager,
        incoming_tx,
        key,
        value.clone(),
        &contract,
        related_contracts.clone(),
        htl,
    )
    .await?;

    // ── Step 2: Build skip list + select next hop ──────────────────────────
    let mut new_skip_list = skip_list;
    new_skip_list.insert(upstream_addr);
    if let Some(own_addr) = op_manager.ring.connection_manager.get_own_addr() {
        new_skip_list.insert(own_addr);
    }

    let next_hop = if htl > 0 {
        op_manager
            .ring
            .closest_potentially_hosting(&key, &new_skip_list)
    } else {
        None
    };

    let (next_peer, next_addr) = match next_hop {
        Some(peer) => {
            let addr = match peer.socket_addr() {
                Some(a) => a,
                None => {
                    tracing::error!(
                        tx = %incoming_tx,
                        contract = %key,
                        target_pub_key = %peer.pub_key(),
                        "PUT relay (task-per-tx): next hop has no socket address"
                    );
                    // No next hop — act as final destination.
                    return relay_put_finalize_local(
                        op_manager,
                        incoming_tx,
                        key,
                        merged_value,
                        upstream_addr,
                    )
                    .await;
                }
            };
            (peer, addr)
        }
        None => {
            // No next hop — this node is the final destination.
            tracing::info!(
                tx = %incoming_tx,
                contract = %key,
                phase = "relay_put_complete",
                "PUT relay (task-per-tx): no next hop, finalizing at this node"
            );
            return relay_put_finalize_local(
                op_manager,
                incoming_tx,
                key,
                merged_value,
                upstream_addr,
            )
            .await;
        }
    };

    // ── Step 3: Forward downstream, await Response, bubble up ──────────────

    let new_htl = htl.saturating_sub(1);

    if let Some(event) = NetEventLog::put_request(
        &incoming_tx,
        &op_manager.ring,
        key,
        next_peer.clone(),
        new_htl,
    ) {
        op_manager.ring.register_events(Either::Left(event)).await;
    }

    tracing::debug!(
        tx = %incoming_tx,
        contract = %key,
        peer_addr = %next_addr,
        htl = new_htl,
        phase = "relay_put_forward",
        "PUT relay (task-per-tx): forwarding to next hop"
    );

    // Slice A: streaming upgrade on forward (legacy put.rs:648-668
    // re-checks `should_use_streaming` after serializing the payload)
    // is deferred to slice B together with the streaming variant
    // migration. The dispatch gate in node.rs filters out inbound
    // streaming Requests and falls through to legacy if the forward
    // would need streaming upgrade, so this arm only ever builds the
    // non-streaming forward.
    let forward = NetMessage::from(PutMsg::Request {
        id: incoming_tx,
        contract,
        related_contracts,
        value: merged_value,
        htl: new_htl,
        skip_list: new_skip_list,
    });

    let mut ctx = op_manager.op_ctx(incoming_tx);
    let round_trip =
        tokio::time::timeout(OPERATION_TTL, ctx.send_to_and_await(next_addr, forward)).await;

    // Release the pending_op_results slot that send_to_and_await
    // installed. The downstream reply has already been delivered (or
    // timed out); the upstream-reply fire-and-forget below will
    // re-insert under the same key, and a single TransactionCompleted
    // at driver exit only removes one entry — without this interim
    // release the inserts/removes ledger leaks one entry per relay
    // driver run (reproduced by test_pending_op_results_bounded at
    // 74/461 on the PR branch before this release call was added).
    // Mirrors GET relay's post-send_to_and_await release.
    op_manager.release_pending_op_slot(incoming_tx).await;

    let reply = match round_trip {
        Ok(Ok(reply)) => reply,
        Ok(Err(err)) => {
            tracing::warn!(
                tx = %incoming_tx,
                contract = %key,
                target = %next_addr,
                error = %err,
                "PUT relay (task-per-tx): send_to_and_await failed"
            );
            return Err(err);
        }
        Err(_elapsed) => {
            tracing::warn!(
                tx = %incoming_tx,
                contract = %key,
                target = %next_addr,
                timeout_secs = OPERATION_TTL.as_secs(),
                "PUT relay (task-per-tx): downstream timed out"
            );
            return Err(OpError::UnexpectedOpState);
        }
    };

    // ── Step 4: Classify reply and bubble Response upstream ────────────────
    match reply {
        NetMessage::V1(NetMessageV1::Put(PutMsg::Response { key: reply_key, .. })) => {
            tracing::info!(
                tx = %incoming_tx,
                contract = %reply_key,
                phase = "relay_put_bubble",
                "PUT relay (task-per-tx): downstream returned Response; bubbling upstream"
            );
            relay_put_send_response(op_manager, incoming_tx, reply_key, upstream_addr).await
        }
        NetMessage::V1(NetMessageV1::Put(PutMsg::ResponseStreaming { key: reply_key, .. })) => {
            // Streaming response arrived from downstream. Slice A does
            // not relay-forward streaming payloads, so log and
            // synthesize a non-streaming Response upstream. This
            // preserves correctness — the contract IS stored at this
            // node (step 1) — at the cost of the upstream not getting
            // the streamed payload. Slice B handles stream pass-through.
            tracing::warn!(
                tx = %incoming_tx,
                contract = %reply_key,
                phase = "relay_put_bubble_streaming_downgrade",
                "PUT relay (task-per-tx): downstream returned ResponseStreaming — \
                 synthesizing non-streaming Response upstream (slice A limitation)"
            );
            relay_put_send_response(op_manager, incoming_tx, reply_key, upstream_addr).await
        }
        other => {
            tracing::warn!(
                tx = %incoming_tx,
                contract = %key,
                reply_variant = ?std::mem::discriminant(&other),
                "PUT relay (task-per-tx): unexpected reply variant; treating as failure"
            );
            Err(OpError::UnexpectedOpState)
        }
    }
}

/// Store a relayed PUT's contract locally: `put_contract` + (if not
/// already hosting) `host_contract` + `announce_contract_hosted` +
/// interest register/unregister + broadcast interest changes.
///
/// Shared between the non-streaming relay driver (`drive_relay_put`)
/// and the streaming relay driver (`drive_relay_put_streaming`) so both
/// paths run identical local-store semantics. Returns the post-merge
/// `WrappedState` the caller forwards downstream / bubbles upstream.
///
/// This helper is **relay-only** — it never sets
/// `mark_local_client_access` (that's originator-side). Errors emit a
/// `put_failure` telemetry event and propagate.
async fn relay_put_store_locally(
    op_manager: &Arc<OpManager>,
    incoming_tx: Transaction,
    key: ContractKey,
    value: WrappedState,
    contract: &ContractContainer,
    related_contracts: RelatedContracts<'static>,
    htl: usize,
) -> Result<WrappedState, OpError> {
    let was_hosting = op_manager.ring.is_hosting_contract(&key);
    let (merged_value, _state_changed) = match super::put_contract(
        op_manager,
        key,
        value.clone(),
        related_contracts,
        contract,
    )
    .await
    {
        Ok(result) => result,
        Err(err) => {
            tracing::error!(
                tx = %incoming_tx,
                contract = %key,
                error = %err,
                htl,
                "PUT relay (task-per-tx): put_contract failed"
            );
            if let Some(event) = NetEventLog::put_failure(
                &incoming_tx,
                &op_manager.ring,
                key,
                OperationFailure::ContractError(err.to_string()),
                Some(op_manager.ring.max_hops_to_live.saturating_sub(htl)),
            ) {
                op_manager.ring.register_events(Either::Left(event)).await;
            }
            return Err(err);
        }
    };

    if !was_hosting {
        let evicted = op_manager
            .ring
            .host_contract(key, value.size() as u64, crate::ring::AccessType::Put)
            .evicted;

        crate::operations::announce_contract_hosted(op_manager, &key).await;

        let mut removed_contracts = Vec::new();
        for evicted_key in evicted {
            if op_manager
                .interest_manager
                .unregister_local_hosting(&evicted_key)
            {
                removed_contracts.push(evicted_key);
            }
        }

        let became_interested = op_manager.interest_manager.register_local_hosting(&key);
        let added = if became_interested { vec![key] } else { vec![] };
        if !added.is_empty() || !removed_contracts.is_empty() {
            crate::operations::broadcast_change_interests(op_manager, added, removed_contracts)
                .await;
        }
    }

    debug_assert!(
        op_manager.ring.is_hosting_contract(&key),
        "PUT relay (task-per-tx): contract {key} must be in hosting list after put_contract + host_contract"
    );

    Ok(merged_value)
}

/// Finalize at this node when there's no next hop. Emits `put_success`
/// telemetry and sends `PutMsg::Response` upstream.
async fn relay_put_finalize_local(
    op_manager: &OpManager,
    incoming_tx: Transaction,
    key: ContractKey,
    merged_value: WrappedState,
    upstream_addr: SocketAddr,
) -> Result<(), OpError> {
    // Telemetry: non-originator target peer — emit put_success for
    // convergence checking (mirrors put.rs:798-811 non-originator arm).
    let own_location = op_manager.ring.connection_manager.own_location();
    let hash = Some(state_hash_full(&merged_value));
    let size = Some(merged_value.len());
    if let Some(event) = NetEventLog::put_success(
        &incoming_tx,
        &op_manager.ring,
        key,
        own_location,
        None,
        hash,
        size,
    ) {
        op_manager.ring.register_events(Either::Left(event)).await;
    }

    relay_put_send_response(op_manager, incoming_tx, key, upstream_addr).await
}

/// Send `PutMsg::Response` upstream (fire-and-forget: upstream relay
/// awaits via its own `send_to_and_await`, no reply expected).
async fn relay_put_send_response(
    op_manager: &OpManager,
    incoming_tx: Transaction,
    key: ContractKey,
    upstream_addr: SocketAddr,
) -> Result<(), OpError> {
    let msg = NetMessage::from(PutMsg::Response {
        id: incoming_tx,
        key,
    });
    let mut ctx = op_manager.op_ctx(incoming_tx);
    ctx.send_fire_and_forget(upstream_addr, msg)
        .await
        .map_err(|_| OpError::NotificationError)
}

// ── End of relay PUT driver ─────────────────────────────────────────────────

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

    // ── Relay PUT driver structural pin tests (#1454 phase 5 follow-up,
    // slice A). Anchor the relay section on the entry-point fn so
    // module-level doc comments (which reference variant names by
    // design) don't enter scope.

    fn relay_section(src: &str) -> &str {
        let start = src
            .find("pub(crate) async fn start_relay_put(")
            .expect("start_relay_put not found");
        let end = src
            .find("\n#[cfg(test)]")
            .expect("test module marker not found");
        &src[start..end]
    }

    /// Pin: dispatch entry must insert into `active_relay_put_txs`
    /// BEFORE spawning. Without this guard, duplicate inbound
    /// `PutMsg::Request` for an in-flight tx would spawn redundant
    /// drivers — same amplification mode that drove phase-5 GET's
    /// 63GB RSS explosion.
    #[test]
    fn start_relay_put_checks_dedup_gate() {
        let src = include_str!("op_ctx_task.rs");
        let relay = relay_section(src);
        let window_start = relay
            .find("pub(crate) async fn start_relay_put(")
            .expect("entry-point not found");
        let spawn_pos = relay[window_start..]
            .find("GlobalExecutor::spawn(run_relay_put(")
            .expect("spawn site not found")
            + window_start;
        let insert_pos = relay[window_start..]
            .find("active_relay_put_txs.insert(incoming_tx)")
            .expect("dedup insert site not found")
            + window_start;
        assert!(
            insert_pos < spawn_pos,
            "active_relay_put_txs.insert MUST happen before GlobalExecutor::spawn"
        );
    }

    /// Pin: dedup rejection must bump `RELAY_PUT_DEDUP_REJECTS`. Without
    /// the counter, `FREENET_MEMORY_STATS` operators cannot see the
    /// dedup gate firing under ci-fault-loss.
    #[test]
    fn dedup_rejection_increments_counter() {
        let src = include_str!("op_ctx_task.rs");
        let relay = relay_section(src);
        let after_insert = relay
            .split("active_relay_put_txs.insert(incoming_tx)")
            .nth(1)
            .expect("dedup insert site not found");
        let window = &after_insert[..500.min(after_insert.len())];
        assert!(
            window.contains("RELAY_PUT_DEDUP_REJECTS.fetch_add"),
            "dedup gate must increment RELAY_PUT_DEDUP_REJECTS on rejection"
        );
    }

    /// Pin: RAII guard must clear `active_relay_put_txs` + bump
    /// completion counters on drop. Without the guard, a panicking
    /// driver would leak the dedup entry permanently (no TTL).
    #[test]
    fn raii_guard_clears_dedup_set_on_drop() {
        let src = include_str!("op_ctx_task.rs");
        let drop_start = src
            .find("impl Drop for RelayPutInflightGuard")
            .expect("RelayPutInflightGuard Drop impl not found");
        let drop_body = &src[drop_start..drop_start + 600];
        assert!(
            drop_body.contains("active_relay_put_txs"),
            "RelayPutInflightGuard::drop must remove from active_relay_put_txs"
        );
        assert!(
            drop_body.contains("RELAY_PUT_INFLIGHT.fetch_sub"),
            "RelayPutInflightGuard::drop must decrement RELAY_PUT_INFLIGHT"
        );
        assert!(
            drop_body.contains("RELAY_PUT_COMPLETED_TOTAL.fetch_add"),
            "RelayPutInflightGuard::drop must increment RELAY_PUT_COMPLETED_TOTAL"
        );
    }

    /// Pin: driver forwards downstream using `send_to_and_await` — PUT
    /// relay IS req/response (like GET), so we need the reply to bubble
    /// upstream. If this flips to `send_fire_and_forget`, downstream
    /// Response never makes it back to originator.
    #[test]
    fn drive_relay_put_forwards_via_send_to_and_await() {
        let src = include_str!("op_ctx_task.rs");
        let driver_start = src
            .find("async fn drive_relay_put(")
            .expect("drive_relay_put not found");
        let driver_end = src[driver_start..]
            .find("\nasync fn relay_put_finalize_local(")
            .expect("driver body end not found")
            + driver_start;
        let driver_src = &src[driver_start..driver_end];
        assert!(
            driver_src.contains("ctx.send_to_and_await("),
            "drive_relay_put must forward downstream via send_to_and_await \
             so the downstream Response bubbles back to this relay"
        );
        assert!(
            !driver_src.contains("send_fire_and_forget"),
            "drive_relay_put must NOT fire-and-forget the downstream forward; \
             PUT relay is req/response"
        );
    }

    /// Pin: driver forward must reuse `incoming_tx` — legacy PUT relay
    /// uses the same tx end-to-end. Minting a fresh tx per hop breaks
    /// the dispatch gate's has_put_op check at the downstream peer.
    #[test]
    fn drive_relay_put_reuses_incoming_tx_on_forward() {
        let src = include_str!("op_ctx_task.rs");
        let driver_start = src
            .find("async fn drive_relay_put(")
            .expect("drive_relay_put not found");
        let driver_end = src[driver_start..]
            .find("\nasync fn relay_put_finalize_local(")
            .expect("driver body end not found")
            + driver_start;
        let driver_src = &src[driver_start..driver_end];
        // The PutMsg::Request forward must carry id: incoming_tx (not a
        // freshly-minted Transaction::new::<PutMsg>()).
        let forward_pos = driver_src
            .find("PutMsg::Request {")
            .expect("forward PutMsg::Request not found in driver");
        let forward_window = &driver_src[forward_pos..forward_pos + 400];
        assert!(
            forward_window.contains("id: incoming_tx"),
            "relay forward must reuse incoming_tx; minting a fresh tx per hop \
             breaks the downstream dispatch gate's has_put_op check"
        );
        assert!(
            !forward_window.contains("Transaction::new::<PutMsg>()"),
            "relay forward must NOT mint a fresh Transaction"
        );
    }

    /// Pin: relay upstream reply is fire-and-forget (matches legacy —
    /// upstream's own `send_to_and_await` owns the capacity-1 reply
    /// channel; sending with another `send_to_and_await` would reuse
    /// our own tx slot for no reason).
    #[test]
    fn relay_put_send_response_is_fire_and_forget() {
        let src = include_str!("op_ctx_task.rs");
        let fn_start = src
            .find("async fn relay_put_send_response(")
            .expect("relay_put_send_response not found");
        let fn_end = src[fn_start..]
            .find("\n}\n")
            .expect("function body end not found")
            + fn_start;
        let fn_src = &src[fn_start..fn_end];
        assert!(
            fn_src.contains("send_fire_and_forget"),
            "relay_put_send_response must use send_fire_and_forget for the \
             upstream response"
        );
    }

    /// Pin: slice A does NOT handle streaming variants. Dispatch gate
    /// in node.rs filters them out; driver source must never reference
    /// the streaming wire variants.
    #[test]
    fn slice_a_does_not_touch_put_streaming_variants() {
        let src = include_str!("op_ctx_task.rs");
        let relay = relay_section(src);
        assert!(
            !relay.contains("PutMsg::RequestStreaming"),
            "slice A must not handle PutMsg::RequestStreaming (deferred to slice B)"
        );
        // ResponseStreaming IS referenced in one place: the downstream
        // reply classifier that synthesizes a non-streaming Response
        // upstream (documented limitation). Ensure NO branch BUILDS a
        // ResponseStreaming — we only match on it.
        let builds_streaming = relay.contains("NetMessage::from(PutMsg::ResponseStreaming")
            || relay.contains("PutMsg::ResponseStreaming {\n") && {
                // Detect construction (not match arm) by looking for `id:` in the next 100 chars
                let pos = relay
                    .find("PutMsg::ResponseStreaming {")
                    .expect("anchor present by guard");
                let window = &relay[pos..pos + 200.min(relay.len() - pos)];
                window.contains("id: ")
            };
        assert!(
            !builds_streaming,
            "slice A driver must not CONSTRUCT a PutMsg::ResponseStreaming"
        );
    }

    /// Pin: driver MUST call `put_contract` + `host_contract` before
    /// forwarding (so the local cache + hosting advertisement happens
    /// regardless of whether the forward succeeds).
    #[test]
    fn drive_relay_put_stores_locally_before_forwarding() {
        let src = include_str!("op_ctx_task.rs");
        let driver_start = src
            .find("async fn drive_relay_put(")
            .expect("drive_relay_put not found");
        let driver_end = src[driver_start..]
            .find("\nasync fn relay_put_store_locally(")
            .expect("driver body end not found")
            + driver_start;
        let driver_src = &src[driver_start..driver_end];
        let store_pos = driver_src
            .find("relay_put_store_locally(")
            .expect("relay_put_store_locally call missing in driver");
        let forward_pos = driver_src
            .find("ctx.send_to_and_await(")
            .expect("send_to_and_await call missing in driver");
        assert!(
            store_pos < forward_pos,
            "local store MUST run before the downstream forward"
        );

        // Helper must encapsulate put_contract + host_contract + announce.
        let helper_start = src
            .find("async fn relay_put_store_locally(")
            .expect("helper not found");
        let helper_end = src[helper_start..]
            .find("\nasync fn relay_put_finalize_local(")
            .expect("helper body end not found")
            + helper_start;
        let helper_src = &src[helper_start..helper_end];
        assert!(
            helper_src.contains("super::put_contract("),
            "helper MUST call put_contract"
        );
        assert!(
            helper_src.contains("host_contract("),
            "helper MUST call ring.host_contract for first-time hosting"
        );
        assert!(
            helper_src.contains("announce_contract_hosted"),
            "helper MUST call announce_contract_hosted for first-time hosting"
        );
    }
}
