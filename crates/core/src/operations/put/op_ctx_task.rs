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
use crate::config::{GlobalExecutor, OPERATION_TTL};
use crate::message::{NetMessage, NetMessageV1, Transaction};
use crate::node::OpManager;
use crate::operations::OpError;
use crate::ring::PeerKeyLocation;

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

    // 1. Local upsert FIRST. put_contract is the authoritative
    //    "my node has it" event.
    let (merged_value, _state_changed) =
        super::put_contract(op_manager, key, value, related.clone(), &contract).await?;

    // 2. Route: local-only, or network fan-out?
    let mut tried: Vec<std::net::SocketAddr> = Vec::new();
    if let Some(own_addr) = op_manager.ring.connection_manager.get_own_addr() {
        tried.push(own_addr);
    }

    let initial_target = op_manager
        .ring
        .closest_potentially_hosting(&key, tried.as_slice());

    let (mut current_target, mut current_addr) = match initial_target {
        Some(peer) => match peer.socket_addr() {
            Some(addr) => {
                tried.push(addr);
                (peer, addr)
            }
            None => {
                return local_only_completion(
                    op_manager,
                    client_tx,
                    key,
                    subscribe,
                    blocking_subscribe,
                )
                .await;
            }
        },
        None => {
            return local_only_completion(
                op_manager,
                client_tx,
                key,
                subscribe,
                blocking_subscribe,
            )
            .await;
        }
    };

    // 3. Retry loop. Fresh attempt_tx per retry (Phase 2b pattern, R4).
    let mut retries: usize = 0;
    let mut is_first_attempt = true;

    loop {
        // Fresh attempt tx: first attempt reuses client_tx (Phase 2b
        // convention keeps first-attempt telemetry on the client-visible
        // tx); retries allocate fresh txs because send_and_await is
        // single-use-per-tx.
        let attempt_tx = if is_first_attempt {
            client_tx
        } else {
            Transaction::new::<PutMsg>()
        };
        is_first_attempt = false;

        tracing::debug!(
            tx = %client_tx,
            attempt_tx = %attempt_tx,
            target = %current_addr,
            retries,
            "put (task-per-tx): sending attempt"
        );

        let request = PutMsg::Request {
            id: attempt_tx,
            contract: contract.clone(),
            related_contracts: related.clone(),
            value: merged_value.clone(),
            htl,
            skip_list: tried.iter().copied().collect::<HashSet<_>>(),
        };

        let mut ctx = op_manager.op_ctx(attempt_tx);
        let round_trip =
            tokio::time::timeout(OPERATION_TTL, ctx.send_and_await(NetMessage::from(request)))
                .await;
        op_manager.release_pending_op_slot(attempt_tx).await;

        let reply = match round_trip {
            Ok(Ok(reply)) => reply,
            Ok(Err(err)) => {
                tracing::warn!(
                    tx = %client_tx,
                    attempt_tx = %attempt_tx,
                    target = %current_addr,
                    retries,
                    outcome = "wire_error",
                    error = %err,
                    "put (task-per-tx): send_and_await failed; advancing to next peer"
                );
                match advance_to_next_peer(op_manager, &key, &mut tried, &mut retries) {
                    Some((next_target, next_addr)) => {
                        current_target = next_target;
                        current_addr = next_addr;
                        continue;
                    }
                    None => {
                        return Ok(DriverOutcome::Publish(Err(ErrorKind::OperationError {
                            cause: format!(
                                "PUT to {key} failed after {} rounds (last peer error: {err})",
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
                    target = %current_addr,
                    retries,
                    outcome = "timeout",
                    timeout_secs = OPERATION_TTL.as_secs(),
                    "put (task-per-tx): attempt timed out; advancing to next peer"
                );
                match advance_to_next_peer(op_manager, &key, &mut tried, &mut retries) {
                    Some((next_target, next_addr)) => {
                        current_target = next_target;
                        current_addr = next_addr;
                        continue;
                    }
                    None => {
                        return Ok(DriverOutcome::Publish(Err(ErrorKind::OperationError {
                            cause: format!("PUT to {key} timed out after {} rounds", retries + 1)
                                .into(),
                        }
                        .into())));
                    }
                }
            }
        };

        match classify_reply(&reply) {
            ReplyClass::Stored { key: reply_key } => {
                tracing::info!(
                    tx = %client_tx,
                    attempt_tx = %attempt_tx,
                    contract = %reply_key,
                    target = %current_addr,
                    retries,
                    outcome = "stored",
                    "put (task-per-tx): PUT accepted"
                );

                // Telemetry only — pass subscribe=false because the driver
                // handles subscriptions via maybe_subscribe_child below.
                // Passing subscribe=true here would double-subscribe: once
                // via start_subscription_after_put (legacy path inside
                // finalize_put_at_originator) and once via
                // maybe_subscribe_child (task-per-tx path).
                super::finalize_put_at_originator(
                    op_manager,
                    client_tx,
                    reply_key,
                    PutFinalizationData {
                        sender: current_target,
                        hop_count: None,
                        state_hash: None,
                        state_size: None,
                    },
                    false, // subscribe handled by maybe_subscribe_child
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

                return Ok(DriverOutcome::Publish(Ok(HostResponse::ContractResponse(
                    ContractResponse::PutResponse { key: reply_key },
                ))));
            }
            ReplyClass::Unexpected => {
                tracing::warn!(
                    tx = %client_tx,
                    attempt_tx = %attempt_tx,
                    "put (task-per-tx): unexpected terminal reply"
                );
                return Err(OpError::UnexpectedOpState);
            }
        }
    }
}

// --- Reply classification ---

#[derive(Debug)]
enum ReplyClass {
    Stored { key: ContractKey },
    Unexpected,
}

fn classify_reply(msg: &NetMessage) -> ReplyClass {
    match msg {
        NetMessage::V1(NetMessageV1::Put(
            PutMsg::Response { key, .. } | PutMsg::ResponseStreaming { key, .. },
        )) => ReplyClass::Stored { key: *key },
        _ => ReplyClass::Unexpected,
    }
}

// --- Peer advance ---

/// Maximum routing rounds before giving up.
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

// --- Local-only completion ---

async fn local_only_completion(
    op_manager: &Arc<OpManager>,
    client_tx: Transaction,
    key: ContractKey,
    subscribe: bool,
    blocking_subscribe: bool,
) -> Result<DriverOutcome, OpError> {
    tracing::info!(
        tx = %client_tx,
        contract = %key,
        phase = "complete",
        "put (task-per-tx): local-only completion (no remote peers)"
    );

    // Telemetry only — pass subscribe=false to avoid double-subscribe.
    // maybe_subscribe_child handles subscriptions via the task-per-tx path.
    let own_location = op_manager.ring.connection_manager.own_location();
    super::finalize_put_at_originator(
        op_manager,
        client_tx,
        key,
        PutFinalizationData {
            sender: own_location,
            hop_count: Some(0),
            state_hash: None,
            state_size: None,
        },
        false,
        false,
    )
    .await;

    maybe_subscribe_child(op_manager, client_tx, key, subscribe, blocking_subscribe).await;

    Ok(DriverOutcome::Publish(Ok(HostResponse::ContractResponse(
        ContractResponse::PutResponse { key },
    ))))
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

    #[test]
    fn classify_reply_request_is_unexpected() {
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
        assert!(matches!(classify_reply(&msg), ReplyClass::Unexpected));
    }
}
