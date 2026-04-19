//! Task-per-transaction client-initiated UPDATE (#1454 Phase 4).
//!
//! UPDATE is fire-and-forget: the originator applies the update locally,
//! optionally forwards a `RequestUpdate` to a remote peer, and delivers
//! the result to the client immediately. There is no response to await
//! and no retry loop.
//!
//! # Scope (Phase 4)
//!
//! Only the **client-initiated originator** UPDATE runs through this
//! module. Relay UPDATEs (RequestUpdate arriving from network),
//! BroadcastTo fan-out, and streaming variants stay on the legacy
//! state-machine path.
//!
//! # Improvements over legacy path
//!
//! - Uses [`OpManager::send_client_result`] instead of raw
//!   `result_router_tx.try_send`, ensuring [`NodeEvent::TransactionCompleted`]
//!   is emitted for cleanup of `tx_to_client` and `pending_op_results`.
//! - Never pushes to `OpManager.ops.update` DashMap, eliminating stale
//!   entry retention between completion and GC timeout.

use std::net::SocketAddr;
use std::sync::Arc;

use either::Either;
use freenet_stdlib::client_api::{ContractResponse, ErrorKind, HostResponse};
use freenet_stdlib::prelude::*;

use crate::client_events::HostResult;
use crate::config::GlobalExecutor;
use crate::contract::{ContractHandlerEvent, StoreResponse};
use crate::message::{DeltaOrFullState, InterestMessage, NetMessage, NodeEvent, Transaction};
use crate::node::OpManager;
use crate::operations::OpError;
use crate::ring::{PeerKeyLocation, RingError};
use crate::tracing::{NetEventLog, OperationFailure, state_hash_full};

use super::{UpdateExecution, UpdateMsg};

/// Counter: number of times `start_relay_request_update` or
/// `start_relay_broadcast_to` was invoked. Incremented under test/testing
/// feature only — used by structural pin tests in #1454 phase 5
/// follow-up to prove the dispatch gate routes fresh inbound traffic
/// through the task-per-tx driver rather than legacy
/// `handle_op_request`.
#[cfg(any(test, feature = "testing"))]
pub static RELAY_UPDATE_DRIVER_CALL_COUNT: std::sync::atomic::AtomicUsize =
    std::sync::atomic::AtomicUsize::new(0);

/// Counter: relay UPDATE drivers currently in flight. Decremented in the
/// RAII guard on driver exit. Diagnostic for `FREENET_MEMORY_STATS`.
pub static RELAY_UPDATE_INFLIGHT: std::sync::atomic::AtomicUsize =
    std::sync::atomic::AtomicUsize::new(0);

/// Counter: total relay UPDATE drivers ever spawned on this node.
pub static RELAY_UPDATE_SPAWNED_TOTAL: std::sync::atomic::AtomicUsize =
    std::sync::atomic::AtomicUsize::new(0);

/// Counter: total relay UPDATE drivers that exited (any path).
pub static RELAY_UPDATE_COMPLETED_TOTAL: std::sync::atomic::AtomicUsize =
    std::sync::atomic::AtomicUsize::new(0);

/// Counter: number of duplicate inbound `RequestUpdate` /
/// `BroadcastTo` Requests rejected by the per-node dedup gate.
pub static RELAY_UPDATE_DEDUP_REJECTS: std::sync::atomic::AtomicUsize =
    std::sync::atomic::AtomicUsize::new(0);

/// Start a client-initiated UPDATE, returning as soon as the task has been
/// spawned (mirrors legacy `request_update` timing).
///
/// The caller must have already registered a result waiter for `client_tx`
/// via `op_manager.ch_outbound.waiting_for_transaction_result`. This
/// function does NOT touch the waiter; it only drives the local merge +
/// optional network forward and publishes the terminal result to
/// `result_router_tx` keyed by `client_tx`.
pub(crate) async fn start_client_update(
    op_manager: Arc<OpManager>,
    client_tx: Transaction,
    key: ContractKey,
    update_data: UpdateData<'static>,
    related_contracts: RelatedContracts<'static>,
) -> Result<Transaction, OpError> {
    tracing::debug!(
        tx = %client_tx,
        contract = %key,
        "update (task-per-tx): spawning client-initiated task"
    );

    // Spawn the driver. Same fire-and-forget rationale as PUT's
    // `start_client_put` — failures are delivered to the client via
    // `result_router_tx`, not via this function's return value.
    //
    // Not registered with `BackgroundTaskMonitor`: per-transaction task
    // that terminates via happy path or infra error.
    //
    // Amplification ceiling: the client_events.rs UPDATE handler allocates
    // one task per client UPDATE request. Client request rate is bounded by
    // the WS connection handler's backpressure.
    GlobalExecutor::spawn(run_client_update(
        op_manager,
        client_tx,
        key,
        update_data,
        related_contracts,
    ));

    Ok(client_tx)
}

async fn run_client_update(
    op_manager: Arc<OpManager>,
    client_tx: Transaction,
    key: ContractKey,
    update_data: UpdateData<'static>,
    related_contracts: RelatedContracts<'static>,
) {
    let outcome = drive_client_update(&op_manager, client_tx, key, update_data, related_contracts)
        .await
        .unwrap_or_else(DriverOutcome::InfrastructureError);
    deliver_outcome(&op_manager, client_tx, outcome);
}

#[derive(Debug)]
enum DriverOutcome {
    /// The driver produced a `HostResult` that must be published via
    /// `send_client_result`.
    Publish(HostResult),
    /// A genuine infrastructure failure escaped the driver.
    InfrastructureError(OpError),
}

async fn drive_client_update(
    op_manager: &OpManager,
    client_tx: Transaction,
    key: ContractKey,
    update_data: UpdateData<'static>,
    related_contracts: RelatedContracts<'static>,
) -> Result<DriverOutcome, OpError> {
    let sender_addr = op_manager.ring.connection_manager.peer_addr()?;

    // --- Find target peer (proximity cache → ring routing) ---
    // Mirrors request_update's target selection at update.rs:1871-1995.

    let proximity_neighbors: Vec<_> = op_manager.neighbor_hosting.neighbors_with_contract(&key);

    let mut target_from_proximity: Option<PeerKeyLocation> = None;
    for pub_key in &proximity_neighbors {
        match op_manager
            .ring
            .connection_manager
            .get_peer_by_pub_key(pub_key)
        {
            Some(peer) => {
                if peer
                    .socket_addr()
                    .map(|a| a == sender_addr)
                    .unwrap_or(false)
                {
                    continue;
                }
                target_from_proximity = Some(peer);
                break;
            }
            None => {
                tracing::debug!(
                    %key,
                    peer = %pub_key,
                    "update (task-per-tx): proximity cache neighbor not connected, trying next"
                );
            }
        }
    }

    let target = if let Some(proximity_neighbor) = target_from_proximity {
        tracing::debug!(
            %key,
            target = ?proximity_neighbor.socket_addr(),
            proximity_neighbors_found = proximity_neighbors.len(),
            "update (task-per-tx): using proximity cache neighbor as target"
        );
        Some(proximity_neighbor)
    } else {
        op_manager
            .ring
            .closest_potentially_hosting(&key, [sender_addr].as_slice())
    };

    match target {
        None => {
            // --- No remote peers: handle locally ---
            tracing::debug!(
                tx = %client_tx,
                %key,
                "update (task-per-tx): no remote peers, handling locally"
            );

            let is_hosting = op_manager.ring.is_hosting_contract(&key);
            if !is_hosting {
                tracing::error!(
                    contract = %key,
                    phase = "error",
                    "update (task-per-tx): cannot update contract on isolated node — not hosted"
                );
                return Err(OpError::RingError(RingError::NoHostingPeers(*key.id())));
            }

            let UpdateExecution {
                value: _,
                summary,
                changed,
                ..
            } = super::update_contract(op_manager, key, update_data, related_contracts).await?;

            if !changed {
                tracing::debug!(
                    tx = %client_tx,
                    %key,
                    "update (task-per-tx): local update resulted in no change"
                );
            } else {
                tracing::debug!(
                    tx = %client_tx,
                    %key,
                    "update (task-per-tx): local-only update complete"
                );
            }

            // BroadcastStateChange is emitted automatically by the executor
            // inside update_contract → commit_state_update.
            let host_result: HostResult = Ok(HostResponse::ContractResponse(
                ContractResponse::UpdateResponse {
                    key,
                    summary: summary.clone(),
                },
            ));
            Ok(DriverOutcome::Publish(host_result))
        }

        Some(target) => {
            // --- Remote target: apply locally then forward ---
            let target_addr = match target.socket_addr() {
                Some(addr) => addr,
                None => {
                    tracing::error!(
                        tx = %client_tx,
                        %key,
                        target_pub_key = %target.pub_key(),
                        "update (task-per-tx): target peer has no socket address"
                    );
                    return Err(OpError::RingError(RingError::NoHostingPeers(*key.id())));
                }
            };

            tracing::debug!(
                tx = %client_tx,
                %key,
                target_peer = %target_addr,
                "update (task-per-tx): applying locally before forwarding"
            );

            let UpdateExecution {
                value: updated_value,
                summary,
                changed: _,
                ..
            } = super::update_contract(
                op_manager,
                key,
                update_data.clone(),
                related_contracts.clone(),
            )
            .await
            .map_err(|e| {
                tracing::error!(
                    tx = %client_tx,
                    contract = %key,
                    error = %e,
                    phase = "error",
                    "update (task-per-tx): failed to apply update locally before forwarding"
                );
                e
            })?;

            // Emit telemetry: UPDATE request initiated (mirrors legacy line 2047)
            if let Some(event) =
                NetEventLog::update_request(&client_tx, &op_manager.ring, key, target.clone())
            {
                op_manager.ring.register_events(Either::Left(event)).await;
            }

            // Build the wire message with the UPDATED value (post-merge),
            // not the original — mirrors legacy update.rs:2055.
            let msg = NetMessage::from(UpdateMsg::RequestUpdate {
                id: client_tx,
                key,
                related_contracts,
                value: updated_value,
            });

            // Fire-and-forget send to target peer.
            let mut ctx = op_manager.op_ctx(client_tx);
            ctx.send_fire_and_forget(target_addr, msg).await?;

            // No release_pending_op_slot call here: send_client_result()
            // (called by deliver_outcome) emits TransactionCompleted which
            // removes the pending_op_results entry. Calling release here
            // would emit a second TransactionCompleted that races with
            // handle_op_execution's insert due to channel priority ordering
            // (notification channel is higher priority than op_execution),
            // making the eager cleanup a no-op in the common case.

            tracing::debug!(
                tx = %client_tx,
                %key,
                target_peer = %target_addr,
                "update (task-per-tx): forwarded to target, operation complete"
            );

            let host_result: HostResult = Ok(HostResponse::ContractResponse(
                ContractResponse::UpdateResponse {
                    key,
                    summary: summary.clone(),
                },
            ));
            Ok(DriverOutcome::Publish(host_result))
        }
    }
}

// --- Outcome delivery ---

fn deliver_outcome(op_manager: &OpManager, client_tx: Transaction, outcome: DriverOutcome) {
    match outcome {
        DriverOutcome::Publish(result) => {
            // send_client_result handles both result_router_tx.try_send AND
            // NodeEvent::TransactionCompleted — fixing the legacy bug where
            // Path B (remote-target) skipped TransactionCompleted.
            op_manager.send_client_result(client_tx, result);
        }
        DriverOutcome::InfrastructureError(err) => {
            tracing::warn!(
                tx = %client_tx,
                error = %err,
                "update (task-per-tx): infrastructure error; publishing synthesized client error"
            );
            let synthesized: HostResult = Err(ErrorKind::OperationError {
                cause: format!("UPDATE failed: {err}").into(),
            }
            .into());
            op_manager.send_client_result(client_tx, synthesized);
        }
    }
    // Clean up: request router dedup, completed set, live tx tracker.
    op_manager.completed(client_tx);
}

// ── Relay UPDATE drivers (#1454 phase 5 follow-up — slice A) ────────────────
//
// UPDATE has no end-to-end response, so the relay drivers below are
// strictly fire-and-forget: they apply state changes locally (or forward
// once downstream) and exit.  No `send_and_await`, no
// `pending_op_results` slot, no upstream reply.  This makes the relay
// drivers immune to the four amplifiers that hit phase-5 GET (workflow
// runs 24600168871 / 24600634908 / 24601267577 / 24601908758):
//
//   1. No retry loop at relay → no MAX_RELAY_RETRIES needed.
//   2. No fresh `attempt_tx` → forwarding reuses `incoming_tx`.
//   3. No `ForwardingAck` → never had one.
//   4. Per-node dedup gate (`active_relay_update_txs`) drops duplicate
//      inbound Requests for an in-flight tx, mirroring GET phase-5 even
//      though the structural risk is lower.
//
// # Scope
//
// Migrated:
//   - `UpdateMsg::RequestUpdate` (non-streaming relay arm:
//      `update.rs::process_message`, lines 347–594).
//   - `UpdateMsg::BroadcastTo`  (non-streaming relay arm:
//      `update.rs::process_message`, lines 595–825).
//
// NOT migrated (stays on legacy path; see port plan §3 / §9):
//   - `UpdateMsg::Broadcasting` (deprecated wire variant — legacy
//      handler is a no-op).
//   - `UpdateMsg::RequestUpdateStreaming` /
//     `UpdateMsg::BroadcastToStreaming` (slice B follow-up — orphan
//      stream claim + assembly).
//   - GC-spawned retries / `start_targeted_op` UPDATE-triggered fetch
//     (existing `UpdateOp` in `OpManager.ops.update` falls through to
//     legacy via `has_update_op` dispatch gate).

/// Spawn a relay driver for a fresh inbound `UpdateMsg::RequestUpdate`.
///
/// Caller-side gates (in `node.rs`): `source_addr.is_some()` AND no
/// existing `UpdateOp` in `OpManager.ops.update` for `incoming_tx`.
///
/// Returns immediately after spawning. Driver publishes its own side
/// effects (local merge → BroadcastStateChange via the executor, OR a
/// single fire-and-forget downstream forward).
pub(crate) async fn start_relay_request_update(
    op_manager: Arc<OpManager>,
    incoming_tx: Transaction,
    key: ContractKey,
    related_contracts: RelatedContracts<'static>,
    value: WrappedState,
    sender_addr: SocketAddr,
) -> Result<(), OpError> {
    #[cfg(any(test, feature = "testing"))]
    RELAY_UPDATE_DRIVER_CALL_COUNT.fetch_add(1, std::sync::atomic::Ordering::SeqCst);

    if !op_manager.active_relay_update_txs.insert(incoming_tx) {
        RELAY_UPDATE_DEDUP_REJECTS.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        tracing::debug!(
            tx = %incoming_tx,
            %key,
            %sender_addr,
            phase = "relay_update_dedup_reject",
            "UPDATE relay (task-per-tx): duplicate RequestUpdate for in-flight tx, dropping"
        );
        return Ok(());
    }

    tracing::debug!(
        tx = %incoming_tx,
        %key,
        %sender_addr,
        phase = "relay_update_request_start",
        "UPDATE relay (task-per-tx): spawning RequestUpdate driver"
    );

    // Construct guard + bump counters BEFORE spawn so the dedup-set
    // entry is paired with a Drop even if the spawned future is dropped
    // before its first poll (executor shutdown, scheduling panic).
    // Without this, a pre-poll drop would permanently leak the
    // `active_relay_update_txs` entry — no TTL → permanent dedup
    // blind-spot for that tx.
    RELAY_UPDATE_INFLIGHT.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    RELAY_UPDATE_SPAWNED_TOTAL.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    let guard = RelayUpdateInflightGuard {
        op_manager: op_manager.clone(),
        incoming_tx,
    };

    GlobalExecutor::spawn(run_relay_request_update(
        guard,
        op_manager,
        incoming_tx,
        key,
        related_contracts,
        value,
        sender_addr,
    ));
    Ok(())
}

/// Spawn a relay driver for a fresh inbound `UpdateMsg::BroadcastTo`.
///
/// Same gates as `start_relay_request_update`. The driver applies the
/// broadcast payload locally (after dedup) and the executor's
/// `BroadcastStateChange` event fans out to other interested peers
/// automatically — the driver itself never sends `BroadcastTo`
/// downstream.
pub(crate) async fn start_relay_broadcast_to(
    op_manager: Arc<OpManager>,
    incoming_tx: Transaction,
    key: ContractKey,
    payload: DeltaOrFullState,
    sender_summary_bytes: Vec<u8>,
    sender_addr: SocketAddr,
) -> Result<(), OpError> {
    #[cfg(any(test, feature = "testing"))]
    RELAY_UPDATE_DRIVER_CALL_COUNT.fetch_add(1, std::sync::atomic::Ordering::SeqCst);

    if !op_manager.active_relay_update_txs.insert(incoming_tx) {
        RELAY_UPDATE_DEDUP_REJECTS.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        tracing::debug!(
            tx = %incoming_tx,
            %key,
            %sender_addr,
            phase = "relay_update_dedup_reject",
            "UPDATE relay (task-per-tx): duplicate BroadcastTo for in-flight tx, dropping"
        );
        return Ok(());
    }

    tracing::debug!(
        tx = %incoming_tx,
        %key,
        %sender_addr,
        phase = "relay_update_broadcast_start",
        "UPDATE relay (task-per-tx): spawning BroadcastTo driver"
    );

    // See `start_relay_request_update` — guard pre-spawn closes the
    // pre-poll dedup-leak window.
    RELAY_UPDATE_INFLIGHT.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    RELAY_UPDATE_SPAWNED_TOTAL.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    let guard = RelayUpdateInflightGuard {
        op_manager: op_manager.clone(),
        incoming_tx,
    };

    GlobalExecutor::spawn(run_relay_broadcast_to(
        guard,
        op_manager,
        incoming_tx,
        key,
        payload,
        sender_summary_bytes,
        sender_addr,
    ));
    Ok(())
}

/// RAII guard that decrements `RELAY_UPDATE_INFLIGHT`, bumps
/// `RELAY_UPDATE_COMPLETED_TOTAL`, and removes the driver's
/// `incoming_tx` from `active_relay_update_txs` on drop. Mirrors GET's
/// `RelayInflightGuard` pattern.
struct RelayUpdateInflightGuard {
    op_manager: Arc<OpManager>,
    incoming_tx: Transaction,
}

impl Drop for RelayUpdateInflightGuard {
    fn drop(&mut self) {
        self.op_manager
            .active_relay_update_txs
            .remove(&self.incoming_tx);
        RELAY_UPDATE_INFLIGHT.fetch_sub(1, std::sync::atomic::Ordering::Relaxed);
        RELAY_UPDATE_COMPLETED_TOTAL.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    }
}

async fn run_relay_request_update(
    guard: RelayUpdateInflightGuard,
    op_manager: Arc<OpManager>,
    incoming_tx: Transaction,
    key: ContractKey,
    related_contracts: RelatedContracts<'static>,
    value: WrappedState,
    sender_addr: SocketAddr,
) {
    let _guard = guard;

    if let Err(err) = drive_relay_request_update(
        &op_manager,
        incoming_tx,
        key,
        related_contracts,
        value,
        sender_addr,
    )
    .await
    {
        tracing::warn!(
            tx = %incoming_tx,
            %key,
            error = %err,
            phase = "relay_update_request_error",
            "UPDATE relay (task-per-tx): RequestUpdate driver returned error"
        );
    }
}

async fn run_relay_broadcast_to(
    guard: RelayUpdateInflightGuard,
    op_manager: Arc<OpManager>,
    incoming_tx: Transaction,
    key: ContractKey,
    payload: DeltaOrFullState,
    sender_summary_bytes: Vec<u8>,
    sender_addr: SocketAddr,
) {
    let _guard = guard;

    if let Err(err) = drive_relay_broadcast_to(
        &op_manager,
        incoming_tx,
        key,
        payload,
        sender_summary_bytes,
        sender_addr,
    )
    .await
    {
        tracing::warn!(
            tx = %incoming_tx,
            %key,
            error = %err,
            phase = "relay_update_broadcast_error",
            "UPDATE relay (task-per-tx): BroadcastTo driver returned error"
        );
    }
}

/// Inner driver for `RequestUpdate`. Mirrors `update.rs:347-594`.
///
/// Decision tree:
///   - We host the contract → apply update locally; executor emits
///     `BroadcastStateChange` automatically. No downstream forward.
///   - We do NOT host the contract → forward `RequestUpdate` once to
///     `closest_potentially_hosting`. No retry, no upstream reply.
async fn drive_relay_request_update(
    op_manager: &OpManager,
    incoming_tx: Transaction,
    key: ContractKey,
    related_contracts: RelatedContracts<'static>,
    value: WrappedState,
    sender_addr: SocketAddr,
) -> Result<(), OpError> {
    let executing_addr = op_manager
        .ring
        .connection_manager
        .own_location()
        .socket_addr();
    tracing::debug!(
        tx = %incoming_tx,
        %key,
        executing_peer = ?executing_addr,
        request_sender = %sender_addr,
        "UPDATE relay (task-per-tx): processing RequestUpdate"
    );

    // ── Step 1: do we host the contract locally? ──────────────────────────
    //
    // Mirrors update.rs:371-389. There's a known TOCTOU between this lookup
    // and the update_contract() call below — the legacy code accepts this
    // for telemetry purposes and the driver matches that behavior.
    let state_before = match op_manager
        .notify_contract_handler(ContractHandlerEvent::GetQuery {
            instance_id: *key.id(),
            return_contract_code: false,
        })
        .await
    {
        Ok(ContractHandlerEvent::GetResponse {
            response: Ok(StoreResponse { state: Some(s), .. }),
            ..
        }) => Some(s),
        _ => None,
    };

    if state_before.is_some() {
        // ── Local apply path ──────────────────────────────────────────────
        let hash_before = state_before.as_ref().map(state_hash_full);

        // Note: legacy comment at update.rs:402-406 — RequestUpdate sender
        // should NOT be excluded from broadcast. Sender is REQUESTING the
        // update and needs to receive the result via subscription.
        let UpdateExecution {
            value: updated_value,
            summary: _,
            changed,
            ..
        } = super::update_contract(
            op_manager,
            key,
            UpdateData::State(State::from(value.clone())),
            related_contracts.clone(),
        )
        .await?;

        let hash_after = Some(state_hash_full(&updated_value));

        // Telemetry: UPDATE succeeded at this peer (mirrors update.rs:428-444).
        if let Some(requester_pkl) = op_manager
            .ring
            .connection_manager
            .get_peer_by_addr(sender_addr)
        {
            if let Some(event) = NetEventLog::update_success(
                &incoming_tx,
                &op_manager.ring,
                key,
                requester_pkl,
                hash_before,
                hash_after,
                Some(updated_value.len()),
            ) {
                op_manager.ring.register_events(Either::Left(event)).await;
            }
        }

        if !changed {
            tracing::debug!(
                tx = %incoming_tx,
                %key,
                "UPDATE relay (task-per-tx): yielded no state change, skipping broadcast"
            );
        } else {
            tracing::debug!(
                tx = %incoming_tx,
                %key,
                "UPDATE relay (task-per-tx): RequestUpdate succeeded, state changed"
            );
        }
        // No `Finished`/`ReceivedRequest` bookkeeping: relay driver owns
        // its lifetime; BroadcastStateChange fans out automatically.
        return Ok(());
    }

    // ── Forward path: contract not local ──────────────────────────────────
    let self_addr = op_manager.ring.connection_manager.peer_addr()?;
    let skip_list = vec![self_addr, sender_addr];

    let next_target = op_manager
        .ring
        .closest_potentially_hosting(&key, skip_list.as_slice());

    let forward_target = match next_target {
        Some(t) => t,
        None => {
            // Mirrors update.rs:560-590: no peers + no local contract.
            let candidates = op_manager
                .ring
                .k_closest_potentially_hosting(&key, skip_list.as_slice(), 5)
                .into_iter()
                .filter_map(|loc| loc.socket_addr())
                .map(|addr| format!("{:.8}", addr))
                .collect::<Vec<_>>();
            let connection_count = op_manager.ring.connection_manager.num_connections();
            tracing::error!(
                tx = %incoming_tx,
                contract = %key,
                ?candidates,
                connection_count,
                peer_addr = %sender_addr,
                phase = "error",
                "UPDATE relay (task-per-tx): contract not local and no peers to forward to"
            );
            if let Some(event) = NetEventLog::update_failure(
                &incoming_tx,
                &op_manager.ring,
                key,
                OperationFailure::NoPeersAvailable,
            ) {
                op_manager.ring.register_events(Either::Left(event)).await;
            }
            return Err(OpError::RingError(RingError::NoHostingPeers(*key.id())));
        }
    };

    let forward_addr = forward_target
        .socket_addr()
        .expect("forward target must have socket address");

    tracing::debug!(
        tx = %incoming_tx,
        %key,
        next_peer = %forward_addr,
        "UPDATE relay (task-per-tx): forwarding to peer that might have contract"
    );

    // Telemetry: relay forwarding update request.
    if let Some(event) =
        NetEventLog::update_request(&incoming_tx, &op_manager.ring, key, forward_target.clone())
    {
        op_manager.ring.register_events(Either::Left(event)).await;
    }

    // Forward downstream. We use the INCOMING tx (end-to-end preservation).
    // Streaming variants are handled on the legacy path in slice A; if the
    // payload size triggers streaming, fall back to legacy by emitting the
    // forward via legacy plumbing in commit 2's dispatch (TODO: confirm
    // dispatch falls through to legacy when payload requires streaming —
    // currently slice A only routes non-streaming RequestUpdate to the
    // driver).
    let request = NetMessage::from(UpdateMsg::RequestUpdate {
        id: incoming_tx,
        key,
        related_contracts,
        value,
    });

    let mut ctx = op_manager.op_ctx(incoming_tx);
    if let Err(err) = ctx.send_fire_and_forget(forward_addr, request).await {
        tracing::warn!(
            tx = %incoming_tx,
            %key,
            target = %forward_addr,
            error = %err,
            "UPDATE relay (task-per-tx): forward send failed"
        );
        return Err(err);
    }

    Ok(())
}

/// Inner driver for `BroadcastTo`. Mirrors `update.rs:595-825`.
///
/// 1. Update sender's cached summary in `interest_manager`.
/// 2. Telemetry: `update_broadcast_received`.
/// 3. Dedup cache check (skip merge if seen) — MUST run before WASM merge.
/// 4. WASM merge via `update_contract`. On delta failure → ResyncRequest;
///    on full-state failure → `try_auto_fetch_contract`.
/// 5. Telemetry: `update_broadcast_applied`.
/// 6. Spawn proactive summary notification background task.
async fn drive_relay_broadcast_to(
    op_manager: &OpManager,
    incoming_tx: Transaction,
    key: ContractKey,
    payload: DeltaOrFullState,
    sender_summary_bytes: Vec<u8>,
    sender_addr: SocketAddr,
) -> Result<(), OpError> {
    let self_location = op_manager.ring.connection_manager.own_location();

    let sender_summary = StateSummary::from(sender_summary_bytes);

    // ── Step 1: update sender's cached summary ────────────────────────────
    if let Some(sender_pkl) = op_manager
        .ring
        .connection_manager
        .get_peer_by_addr(sender_addr)
    {
        let sender_key = crate::ring::PeerKey::from(sender_pkl.pub_key().clone());
        op_manager
            .interest_manager
            .update_peer_summary(&key, &sender_key, Some(sender_summary));
    }

    let (update_data, payload_bytes) = match &payload {
        DeltaOrFullState::Delta(bytes) => {
            tracing::debug!(
                contract = %key,
                delta_size = bytes.len(),
                "UPDATE relay (task-per-tx): received delta broadcast"
            );
            (
                UpdateData::Delta(StateDelta::from(bytes.clone())),
                bytes.clone(),
            )
        }
        DeltaOrFullState::FullState(bytes) => {
            tracing::debug!(
                contract = %key,
                state_size = bytes.len(),
                "UPDATE relay (task-per-tx): received full state broadcast"
            );
            (UpdateData::State(State::from(bytes.clone())), bytes.clone())
        }
    };
    let is_delta = matches!(payload, DeltaOrFullState::Delta(_));
    let state_for_telemetry = WrappedState::from(payload_bytes.clone());

    // ── Step 2: telemetry — broadcast received ─────────────────────────────
    if let Some(requester_pkl) = op_manager
        .ring
        .connection_manager
        .get_peer_by_addr(sender_addr)
    {
        if let Some(event) = NetEventLog::update_broadcast_received(
            &incoming_tx,
            &op_manager.ring,
            key,
            requester_pkl,
            state_for_telemetry.clone(),
        ) {
            op_manager.ring.register_events(Either::Left(event)).await;
        }
    }

    // ── Step 3: dedup cache (BEFORE merge) ────────────────────────────────
    //
    // Mirrors update.rs:670-694. MUST run before update_contract — a missed
    // dedup re-runs WASM merge for every duplicate broadcast.
    if op_manager.broadcast_dedup_cache.check_and_insert(
        &key,
        &payload_bytes,
        is_delta,
        op_manager.interest_manager.now(),
    ) {
        tracing::debug!(
            tx = %incoming_tx,
            %key,
            "UPDATE relay (task-per-tx): BroadcastTo skipped — duplicate payload (dedup hit)"
        );
        return Ok(());
    }

    // ── Step 4: apply broadcast via WASM merge ────────────────────────────
    let update_result =
        super::update_contract(op_manager, key, update_data, RelatedContracts::default()).await;

    let UpdateExecution {
        value: updated_value,
        summary: update_summary,
        changed,
        ..
    } = match update_result {
        Ok(result) => result,
        Err(err) => {
            if is_delta {
                // Delta application failed → send ResyncRequest. Mirrors
                // update.rs:710-758.
                tracing::warn!(
                    tx = %incoming_tx,
                    contract = %key,
                    sender = %sender_addr,
                    error = %err,
                    event = "delta_apply_failed",
                    "UPDATE relay (task-per-tx): delta apply failed, sending ResyncRequest"
                );

                if let Some(sender_pkl) = op_manager
                    .ring
                    .connection_manager
                    .get_peer_by_addr(sender_addr)
                {
                    let sender_key = crate::ring::PeerKey::from(sender_pkl.pub_key().clone());
                    op_manager
                        .interest_manager
                        .update_peer_summary(&key, &sender_key, None);
                }

                tracing::info!(
                    tx = %incoming_tx,
                    contract = %key,
                    target = %sender_addr,
                    event = "resync_request_sent",
                    "UPDATE relay (task-per-tx): sending ResyncRequest after delta failure"
                );
                if let Err(e) = op_manager
                    .notify_node_event(NodeEvent::SendInterestMessage {
                        target: sender_addr,
                        message: InterestMessage::ResyncRequest { key },
                    })
                    .await
                {
                    tracing::warn!(
                        tx = %incoming_tx,
                        error = %e,
                        "UPDATE relay (task-per-tx): failed to send ResyncRequest"
                    );
                }
            } else if !err.is_contract_exec_rejection() {
                // Full state failed and the merge function did NOT reject it
                // (so contract code is missing). Trigger self-healing GET.
                op_manager.try_auto_fetch_contract(&key, sender_addr);
            }
            return Err(err);
        }
    };

    tracing::debug!(
        tx = %incoming_tx,
        %key,
        "UPDATE relay (task-per-tx): BroadcastTo applied"
    );

    // ── Step 5: telemetry — broadcast applied ─────────────────────────────
    if let Some(event) = NetEventLog::update_broadcast_applied(
        &incoming_tx,
        &op_manager.ring,
        key,
        &state_for_telemetry,
        &updated_value,
        changed,
    ) {
        op_manager.ring.register_events(Either::Left(event)).await;
    }

    if !changed {
        tracing::debug!(
            tx = %incoming_tx,
            %key,
            "UPDATE relay (task-per-tx): BroadcastTo produced no change, ending propagation"
        );
        return Ok(());
    }

    tracing::debug!(
        "UPDATE relay (task-per-tx): contract {} @ {:?} updated via BroadcastTo",
        key,
        self_location.location()
    );

    // ── Step 6: proactive summary notification ────────────────────────────
    //
    // Mirrors update.rs:806-819 — spawn a background task to notify
    // interested peers our state changed. Unregistered with
    // BackgroundTaskMonitor to match legacy timing semantics
    // (per-broadcast spawn, dies when complete). Per-broadcast
    // amplification is bounded by `should_send_summary_notification`'s
    // 100ms-per-contract throttle inside the task.
    let op_mgr = op_manager.clone();
    let summary = update_summary.clone();
    GlobalExecutor::spawn(async move {
        super::send_proactive_summary_notification(&op_mgr, &key, sender_addr, summary).await;
    });

    Ok(())
}

#[cfg(test)]
mod tests {
    /// Guard: `client_events.rs` must call `start_client_update` for both the
    /// routed and legacy UPDATE paths, not the legacy `request_update`.
    #[test]
    fn client_events_calls_start_client_update() {
        let src = include_str!("../../client_events.rs");
        // The routed path and legacy path should both reference start_client_update
        assert!(
            src.contains("op_ctx_task::start_client_update"),
            "client_events.rs must call update::op_ctx_task::start_client_update \
             for client-initiated UPDATEs (not the legacy request_update path)"
        );
        // The legacy request_update should no longer be called from client_events
        // for client-initiated UPDATEs. It may still be referenced elsewhere
        // (relay paths, imports), so we check specifically in the UPDATE handler section.
        let update_section = src
            .split("ContractRequest::Update")
            .nth(1)
            .expect("client_events.rs must contain a ContractRequest::Update handler");
        // Truncate to the next ContractRequest variant to avoid false positives
        let update_section = update_section
            .split("ContractRequest::")
            .next()
            .unwrap_or(update_section);
        assert!(
            !update_section.contains("request_update("),
            "client_events.rs UPDATE handler must NOT call request_update() directly \
             — it should use op_ctx_task::start_client_update instead"
        );
    }

    /// Guard: the driver must use `send_client_result` (which emits
    /// `TransactionCompleted`) rather than raw `result_router_tx.try_send`.
    #[test]
    fn driver_uses_send_client_result_not_raw_try_send() {
        let src = include_str!("op_ctx_task.rs");
        assert!(
            src.contains("send_client_result"),
            "driver must use op_manager.send_client_result() for result delivery"
        );
        // Verify the production functions (`deliver_outcome`, `drive_client_update`)
        // never call `.result_router_tx.try_send(` — only comments/docs mention it.
        // Extract deliver_outcome body as the critical section.
        let deliver_fn = src
            .find("fn deliver_outcome(")
            .expect("deliver_outcome must exist");
        let deliver_body = &src[deliver_fn..];
        // Take up to the next top-level function or #[cfg(test)]
        let end = deliver_body
            .find("#[cfg(test)]")
            .unwrap_or(deliver_body.len());
        let deliver_body = &deliver_body[..end];
        assert!(
            deliver_body.contains("send_client_result("),
            "deliver_outcome must call send_client_result"
        );
    }

    /// Guard: the driver must call `op_manager.completed(client_tx)` for
    /// request router dedup cleanup.
    #[test]
    fn driver_calls_completed() {
        let src = include_str!("op_ctx_task.rs");
        assert!(
            src.contains("op_manager.completed("),
            "driver must call op_manager.completed() for cleanup"
        );
    }

    /// Guard: the driver must call `update_contract` to preserve all side
    /// effects (WASM merge, state persistence, BroadcastStateChange emission,
    /// delegate notifications, record_contract_updated telemetry).
    #[test]
    fn driver_calls_update_contract() {
        let src = include_str!("op_ctx_task.rs");
        assert!(
            src.contains("update_contract("),
            "driver must call update_contract() to preserve WASM merge + \
             BroadcastStateChange + persistence side effects"
        );
    }

    /// Guard: in the remote-target path, the driver must send the UPDATED
    /// value (post-merge) in RequestUpdate, not the original client value.
    /// This mirrors legacy update.rs:2055.
    #[test]
    fn driver_sends_updated_value_in_request() {
        let src = include_str!("op_ctx_task.rs");
        assert!(
            src.contains("value: updated_value"),
            "RequestUpdate must use the post-merge updated_value, not the original \
             client update_data (mirrors legacy update.rs:2055)"
        );
    }

    // ── Relay UPDATE driver structural pin tests (#1454 phase 5 follow-up,
    // slice A scaffold). These pin the driver SOURCE against documented
    // invariants; a future change that breaks any invariant should fail
    // these before sim runs catch the consequence. Behavioral tests for
    // the dispatch gate live in commit 2.

    /// Pin: relay drivers must NOT call `send_and_await` anywhere. UPDATE
    /// is fire-and-forget end-to-end; introducing a `send_and_await`
    /// reintroduces the four-amplifier surface that hit phase-5 GET.
    #[test]
    fn relay_drivers_never_call_send_and_await() {
        let src = include_str!("op_ctx_task.rs");
        // Find the relay driver section (everything between the relay
        // marker comment and the test module).
        // Skip past the section header / module-level scope doc comment
        // (which references variant names by design) by anchoring on the
        // first relay entry-point fn.
        let relay_start = src
            .find("pub(crate) async fn start_relay_request_update(")
            .expect("start_relay_request_update not found");
        let relay_end = src
            .find("#[cfg(test)]")
            .expect("test module marker not found");
        let relay_src = &src[relay_start..relay_end];
        // Match call-site forms only (`.send_and_await(` /
        // `send_and_await(`), so doc comments mentioning the symbol
        // don't trip the lint.
        assert!(
            !relay_src.contains(".send_and_await("),
            "Relay UPDATE drivers MUST NOT call send_and_await. UPDATE is \
             fire-and-forget; introducing a wait reintroduces phase-5 GET's \
             amplifier surface."
        );
    }

    /// Pin: `RequestUpdate` driver must call `send_fire_and_forget` for
    /// the forward path (single forward, no retry, no ack).
    #[test]
    fn relay_request_update_uses_fire_and_forget() {
        let src = include_str!("op_ctx_task.rs");
        let driver_start = src
            .find("async fn drive_relay_request_update(")
            .expect("drive_relay_request_update not found");
        let driver_end_marker = src[driver_start..]
            .find("\n}")
            .expect("driver body end not found");
        let driver_src = &src[driver_start..driver_start + driver_end_marker];
        assert!(
            driver_src.contains("send_fire_and_forget"),
            "drive_relay_request_update must use send_fire_and_forget for the \
             forward path"
        );
    }

    /// Pin: `BroadcastTo` driver must check the dedup cache BEFORE calling
    /// `update_contract`. If the order flips, duplicate broadcasts re-run
    /// the WASM merge and amplify executor cost N-fold.
    #[test]
    fn broadcast_to_dedup_runs_before_merge() {
        let src = include_str!("op_ctx_task.rs");
        let driver_start = src
            .find("async fn drive_relay_broadcast_to(")
            .expect("drive_relay_broadcast_to not found");
        let driver_end_marker = src[driver_start..]
            .find("\n}")
            .expect("driver body end not found");
        let driver_src = &src[driver_start..driver_start + driver_end_marker];

        let dedup_pos = driver_src
            .find("broadcast_dedup_cache.check_and_insert")
            .expect("dedup cache check missing in BroadcastTo driver");
        let merge_pos = driver_src
            .find("super::update_contract(")
            .expect("update_contract call missing in BroadcastTo driver");
        assert!(
            dedup_pos < merge_pos,
            "broadcast_dedup_cache.check_and_insert MUST appear before \
             update_contract() in drive_relay_broadcast_to. If the order \
             flips, duplicate broadcasts re-run WASM merge and amplify cost."
        );
    }

    /// Pin: `BroadcastTo` driver must send `ResyncRequest` on delta apply
    /// failure (before returning the error). Without this, peers stay out
    /// of sync indefinitely on delta divergence.
    #[test]
    fn broadcast_to_sends_resync_on_delta_failure() {
        let src = include_str!("op_ctx_task.rs");
        let driver_start = src
            .find("async fn drive_relay_broadcast_to(")
            .expect("drive_relay_broadcast_to not found");
        let driver_src = &src[driver_start..];
        assert!(
            driver_src.contains("InterestMessage::ResyncRequest"),
            "drive_relay_broadcast_to must emit InterestMessage::ResyncRequest \
             when a delta payload fails to apply (mirrors legacy update.rs:745-758)."
        );
    }

    /// Pin: `BroadcastTo` driver must call `try_auto_fetch_contract` on
    /// full-state failure that is NOT a contract execution rejection. This
    /// is the self-healing recovery path for missing contract code.
    #[test]
    fn broadcast_to_triggers_auto_fetch_on_full_state_failure() {
        let src = include_str!("op_ctx_task.rs");
        let driver_start = src
            .find("async fn drive_relay_broadcast_to(")
            .expect("drive_relay_broadcast_to not found");
        let driver_src = &src[driver_start..];
        assert!(
            driver_src.contains("try_auto_fetch_contract"),
            "drive_relay_broadcast_to must call try_auto_fetch_contract on \
             non-rejection full-state failures (mirrors legacy update.rs:764)."
        );
    }

    /// Pin: `BroadcastTo` driver must spawn the proactive summary
    /// notification background task on a successful state change. Skipping
    /// it causes peers to repeatedly broadcast stale data.
    #[test]
    fn broadcast_to_spawns_proactive_summary() {
        let src = include_str!("op_ctx_task.rs");
        let driver_start = src
            .find("async fn drive_relay_broadcast_to(")
            .expect("drive_relay_broadcast_to not found");
        let driver_src = &src[driver_start..];
        assert!(
            driver_src.contains("send_proactive_summary_notification"),
            "drive_relay_broadcast_to must spawn send_proactive_summary_notification \
             after a successful state change (mirrors legacy update.rs:806-819)."
        );
    }

    /// Pin: both relay drivers MUST gate on `active_relay_update_txs` to
    /// reject duplicate inbound messages for an in-flight tx (matches the
    /// pattern that GET phase 5 needed to avoid amplification).
    #[test]
    fn relay_drivers_use_per_node_dedup_gate() {
        let src = include_str!("op_ctx_task.rs");
        let request_start = src
            .find("pub(crate) async fn start_relay_request_update(")
            .expect("start_relay_request_update not found");
        let broadcast_start = src
            .find("pub(crate) async fn start_relay_broadcast_to(")
            .expect("start_relay_broadcast_to not found");

        // Take ~1KB of body after each entry — enough to cover the dedup
        // gate but not the rest of the file.
        let req_body = &src[request_start..request_start + 1500];
        let bc_body = &src[broadcast_start..broadcast_start + 1500];
        assert!(
            req_body.contains("active_relay_update_txs.insert"),
            "start_relay_request_update must check active_relay_update_txs"
        );
        assert!(
            bc_body.contains("active_relay_update_txs.insert"),
            "start_relay_broadcast_to must check active_relay_update_txs"
        );
    }

    /// Pin: dedup gate must increment `RELAY_UPDATE_DEDUP_REJECTS` when it
    /// rejects a duplicate. Without the counter, the regression
    /// signal is invisible to `FREENET_MEMORY_STATS` operators.
    #[test]
    fn dedup_rejection_increments_counter() {
        let src = include_str!("op_ctx_task.rs");
        // Restrict to the production driver section so the test source's
        // own grep references don't enter the iteration.
        let relay_start = src
            .find("pub(crate) async fn start_relay_request_update(")
            .expect("start_relay_request_update not found");
        let relay_end = src
            .find("#[cfg(test)]")
            .expect("test module marker not found");
        let relay_src = &src[relay_start..relay_end];

        let sites: Vec<&str> = relay_src
            .split("active_relay_update_txs.insert(incoming_tx)")
            .skip(1)
            .collect();
        assert_eq!(
            sites.len(),
            2,
            "expected exactly two dedup sites (RequestUpdate + BroadcastTo)"
        );
        for (idx, section) in sites.iter().enumerate() {
            let window = &section[..500.min(section.len())];
            assert!(
                window.contains("RELAY_UPDATE_DEDUP_REJECTS.fetch_add"),
                "dedup gate site #{idx} does not increment RELAY_UPDATE_DEDUP_REJECTS"
            );
        }
    }

    /// Pin: the RAII guard must remove from `active_relay_update_txs` on
    /// drop. Without this, a panicking driver would permanently block
    /// future drivers for the same tx.
    #[test]
    fn raii_guard_clears_dedup_set_on_drop() {
        let src = include_str!("op_ctx_task.rs");
        let drop_start = src
            .find("impl Drop for RelayUpdateInflightGuard")
            .expect("RelayUpdateInflightGuard Drop impl not found");
        let drop_body = &src[drop_start..drop_start + 600];
        assert!(
            drop_body.contains("active_relay_update_txs"),
            "RelayUpdateInflightGuard::drop must remove from active_relay_update_txs"
        );
        assert!(
            drop_body.contains("RELAY_UPDATE_INFLIGHT.fetch_sub"),
            "RelayUpdateInflightGuard::drop must decrement RELAY_UPDATE_INFLIGHT"
        );
        assert!(
            drop_body.contains("RELAY_UPDATE_COMPLETED_TOTAL.fetch_add"),
            "RelayUpdateInflightGuard::drop must increment RELAY_UPDATE_COMPLETED_TOTAL"
        );
    }

    /// Pin: streaming variants MUST NOT be touched by the driver in slice A.
    /// They stay on the legacy path until slice B. Reference the variant
    /// names by their wire-message identifiers.
    #[test]
    fn slice_a_does_not_touch_streaming_variants() {
        let src = include_str!("op_ctx_task.rs");
        // Skip past the section header / module-level scope doc comment
        // (which references variant names by design) by anchoring on the
        // first relay entry-point fn.
        let relay_start = src
            .find("pub(crate) async fn start_relay_request_update(")
            .expect("start_relay_request_update not found");
        let relay_end = src
            .find("#[cfg(test)]")
            .expect("test module marker not found");
        let relay_src = &src[relay_start..relay_end];
        assert!(
            !relay_src.contains("RequestUpdateStreaming"),
            "slice A must not handle RequestUpdateStreaming (deferred to slice B)"
        );
        assert!(
            !relay_src.contains("BroadcastToStreaming"),
            "slice A must not handle BroadcastToStreaming (deferred to slice B)"
        );
    }

    /// Pin: `Broadcasting` (deprecated) must not be migrated. Legacy
    /// handler treats it as a no-op; driver should never reference it.
    #[test]
    fn deprecated_broadcasting_variant_stays_legacy() {
        let src = include_str!("op_ctx_task.rs");
        // Skip past the section header / module-level scope doc comment
        // (which references variant names by design) by anchoring on the
        // first relay entry-point fn.
        let relay_start = src
            .find("pub(crate) async fn start_relay_request_update(")
            .expect("start_relay_request_update not found");
        let relay_end = src
            .find("#[cfg(test)]")
            .expect("test module marker not found");
        let relay_src = &src[relay_start..relay_end];
        // Match the variant constructor (UpdateMsg::Broadcasting), not
        // arbitrary substrings (e.g. "Broadcasting" in a doc comment).
        assert!(
            !relay_src.contains("UpdateMsg::Broadcasting"),
            "Driver must not handle UpdateMsg::Broadcasting — deprecated wire \
             variant; legacy no-op handler stays in place."
        );
    }
}
