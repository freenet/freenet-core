//! Task-per-transaction UPDATE drivers.
//!
//! UPDATE is fire-and-forget end-to-end: the originator applies the
//! update locally, optionally forwards a `RequestUpdate` to a remote
//! peer, and delivers the result to the client immediately. No
//! response is awaited, no retry loop.
//!
//! Drivers use [`OpManager::send_client_result`] (which emits
//! [`NodeEvent::TransactionCompleted`] for `tx_to_client` and
//! `pending_op_results` cleanup) and never push state into a
//! `OpManager.ops.*` DashMap.

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use tokio::time::Instant;

use either::Either;
use freenet_stdlib::client_api::{ContractResponse, ErrorKind, HostResponse};
use freenet_stdlib::prelude::*;

use crate::client_events::HostResult;
use crate::config::{GlobalExecutor, GlobalRng};
use crate::contract::{ContractHandlerEvent, StoreResponse};
use crate::message::{DeltaOrFullState, InterestMessage, NetMessage, NodeEvent, Transaction};
use crate::node::OpManager;
use crate::operations::OpError;
use crate::operations::bootstrap::bootstrap_gateway_target;
use crate::ring::interest::RESYNC_REQUEST_MIN_INTERVAL;
use crate::ring::{PeerKeyLocation, RingError};
use crate::tracing::{NetEventLog, OperationFailure, state_hash_full};

use super::{
    AutoFetchReason, BroadcastStreamingPayload, UpdateExecution, UpdateMsg, UpdateStreamingPayload,
};
use crate::transport::peer_connection::StreamId;

/// Counter: number of times `start_relay_request_update` or
/// `start_relay_broadcast_to` was invoked. Incremented under
/// test/testing feature only; used by structural pin tests to prove
/// the dispatch gate routes fresh inbound traffic through the
/// driver.
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

// ── Streaming relay UPDATE counters ──────────────────────────────────────
//
// Separate from the non-streaming `RELAY_UPDATE_*` counters so
// operators can ramp-compare observability between the two. The tx
// dedup set (`active_relay_update_txs`) and
// `RELAY_UPDATE_DEDUP_REJECTS` are shared.

/// Counter: number of times `start_relay_request_update_streaming` or
/// `start_relay_broadcast_to_streaming` was invoked. Used by dispatch
/// gate pin tests to prove streaming relays route through the
/// driver rather than `handle_op_request`.
#[cfg(any(test, feature = "testing"))]
pub static RELAY_UPDATE_STREAMING_DRIVER_CALL_COUNT: std::sync::atomic::AtomicUsize =
    std::sync::atomic::AtomicUsize::new(0);

/// Counter: streaming relay UPDATE drivers currently in flight.
pub static RELAY_UPDATE_STREAMING_INFLIGHT: std::sync::atomic::AtomicUsize =
    std::sync::atomic::AtomicUsize::new(0);

/// Counter: total streaming relay UPDATE drivers ever spawned on this
/// node.
pub static RELAY_UPDATE_STREAMING_SPAWNED_TOTAL: std::sync::atomic::AtomicUsize =
    std::sync::atomic::AtomicUsize::new(0);

/// Counter: total streaming relay UPDATE drivers that exited.
pub static RELAY_UPDATE_STREAMING_COMPLETED_TOTAL: std::sync::atomic::AtomicUsize =
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
    // Phase 7 egress self-block (#4300): refuse to originate an UPDATE
    // for a contract this node has banned, BEFORE spawning the driver.
    // This covers the client-originated UPDATE path; the delegate-driven
    // broadcast fan-out is gated separately at `handle_broadcast_state_change`
    // in p2p_protoc.rs. Mirrors the receive-side `UpdateMsg::*` drop in
    // node.rs (PR #4299). The client gets a typed `ContractBanned` error.
    crate::operations::reject_if_contract_banned(&op_manager, key.id())?;

    tracing::debug!(
        tx = %client_tx,
        contract = %key,
        "update: spawning client-initiated task"
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
    // Atomic admission gate + counter bump (closes the drain race
    // window). See `OpManager::admit_client_op` for the race
    // analysis. The guard is held by the driver task for its
    // lifetime so `ShutdownHandle::shutdown` waits for in-flight
    // client UPDATEs before tearing down peer connections.
    let inflight_guard = match op_manager.admit_client_op() {
        Some(g) => g,
        None => return Err(OpError::NodeShuttingDown),
    };
    GlobalExecutor::spawn(async move {
        let _inflight_guard = inflight_guard;
        run_client_update(op_manager, client_tx, key, update_data, related_contracts).await;
    });

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

    // Whether this client update is a pure DELTA — the only convergence-proving
    // merge, per the strictly-delta-only backoff-reset rule (#4861). Captured
    // before `update_data` is moved into `update_contract` below; used to clear
    // the merge-failure backoff when a local client's delta recovers a contract
    // (#4864 review P2). A client full-state (State) update just replaces state
    // and proves nothing, so it does NOT reset.
    let is_delta_update = matches!(
        &update_data,
        UpdateData::Delta(_) | UpdateData::RelatedDelta { .. }
    );

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
                    "update: proximity cache neighbor not connected, trying next"
                );
            }
        }
    }

    let target = if let Some(proximity_neighbor) = target_from_proximity {
        tracing::debug!(
            %key,
            target = ?proximity_neighbor.socket_addr(),
            proximity_neighbors_found = proximity_neighbors.len(),
            "update: using proximity cache neighbor as target"
        );
        Some(proximity_neighbor)
    } else {
        op_manager
            .ring
            .closest_potentially_hosting(&key, [sender_addr].as_slice())
            // Bootstrap fallback (#4361 / #4365): with an empty ring there
            // is no routing candidate, so route the UPDATE via a configured
            // gateway instead of handling it locally. In the hosting case
            // the remote-target branch applies the update locally and
            // forwards it to the gateway, so it actually propagates instead
            // of broadcasting to an empty ring. In the not-hosting case the
            // local apply fails with missing params and the existing
            // auto-fetch + retry path primes the store from the gateway —
            // far better than the flat NoHostingPeers a fresh node hits today.
            .or_else(|| {
                bootstrap_gateway_target(op_manager, |addr| addr == sender_addr).map(
                    |(gateway, gateway_addr)| {
                        tracing::info!(
                            %key,
                            gateway = %gateway_addr,
                            "update: ring empty — target falls back to configured gateway"
                        );
                        gateway
                    },
                )
            })
    };

    match target {
        None => {
            // --- No remote peers: handle locally ---
            tracing::debug!(
                tx = %client_tx,
                %key,
                "update: no remote peers, handling locally"
            );

            let is_hosting = op_manager.ring.is_hosting_contract(&key);
            if !is_hosting {
                tracing::error!(
                    contract = %key,
                    phase = "error",
                    "update: cannot update contract on isolated node — not hosted"
                );
                return Err(OpError::RingError(RingError::NoHostingPeers(*key.id())));
            }

            // No remote target → no peer to auto-fetch from. If
            // `is_hosting_contract` reports true but `state_store`
            // doesn't have params, that's an internal inconsistency
            // (the ring's hosting registry is the announce/subscribe
            // layer, not the executor's StateStore — they CAN
            // diverge if a hosting announcement races with state
            // eviction). Surface the missing-params case explicitly
            // so operators can correlate the inconsistency, instead
            // of letting it bubble as an opaque OpError.
            let UpdateExecution { value: _, changed } = match super::update_contract(
                op_manager,
                key,
                update_data,
                related_contracts,
                crate::contract::Priority::ClientLocal,
                crate::node::ApplyOrigin::ClientLocal,
                // #5147: no upstream sender, so nothing is covered. An EMPTY
                // registration is not the same as no registration — it collapses
                // any concurrent relayed coverage for this contract, so a local
                // update is never suppressed against someone else's target list.
                crate::ring::broadcast_coverage::BroadcastOrigin::local(),
            )
            .await
            {
                Ok(execution) => {
                    // A successful client-local DELTA merge proves the contract's
                    // merge works now — clear any merge-failure backoff so inbound
                    // broadcasts resume (#4864 review P2, delta-only).
                    if is_delta_update {
                        // Contract-wide clear only when the state advanced
                        // (execution.changed) — a no-op client delta must not
                        // wipe a live Timeout quarantine (#4864 round-4).
                        op_manager
                            .ring
                            .merge_backoff
                            .record_success_local(key.id(), execution.changed);
                    } else if execution.changed {
                        // #4864 round-9 item 3: a CHANGED client FULL-STATE apply
                        // advances the state, invalidating the failed-payload MEMO's
                        // premise — clear ONLY the memo (mirrors the relay + streaming
                        // + resync paths). NOT a backoff reset (fork-flip no-reset
                        // doctrine holds); gated on execution.changed.
                        op_manager
                            .ring
                            .merge_backoff
                            .invalidate_payload_memo(key.id());
                    }
                    execution
                }
                Err(err) if err.is_missing_contract_parameters() => {
                    tracing::error!(
                        tx = %client_tx,
                        contract = %key,
                        error = %err,
                        phase = "ring_state_store_inconsistency",
                        "update: is_hosting_contract reports true \
                         but state_store has no params for this contract; cannot \
                         auto-fetch (no remote target) — the ring/state-store \
                         divergence must be repaired by a re-PUT or a manual \
                         seeding step (#4066)"
                    );
                    return Ok(DriverOutcome::Publish(Err(ErrorKind::OperationError {
                        cause: format!(
                            "originator hosts {key} per ring but local state_store \
                             is missing contract code/params; no remote peer \
                             available to auto-fetch from"
                        )
                        .into(),
                    }
                    .into())));
                }
                Err(err) => return Err(err),
            };

            if !changed {
                tracing::debug!(
                    tx = %client_tx,
                    %key,
                    "update: local update resulted in no change"
                );
            } else {
                tracing::debug!(
                    tx = %client_tx,
                    %key,
                    "update: local-only update complete"
                );
            }

            // BroadcastStateChange is emitted automatically by the executor
            // inside update_contract → commit_state_update.
            //
            // #4923: fetch the contract's REAL summary for the client
            // response — never the state relabeled as a summary. Fetched
            // AFTER the update committed, so under concurrency it may
            // describe a state slightly NEWER than the one this update
            // produced (another update can land between commit and
            // summarize). Harmless for consumers: the summary is an opaque
            // advisory snapshot of "current state after your update", not a
            // receipt for the exact merged bytes.
            let summary = super::contract_summary_or_empty(
                op_manager,
                key,
                crate::contract::Priority::ClientLocal,
            )
            .await;
            let host_result: HostResult = Ok(HostResponse::ContractResponse(
                ContractResponse::UpdateResponse { key, summary },
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
                        "update: target peer has no socket address"
                    );
                    return Err(OpError::RingError(RingError::NoHostingPeers(*key.id())));
                }
            };

            tracing::debug!(
                tx = %client_tx,
                %key,
                target_peer = %target_addr,
                "update: applying locally before forwarding"
            );

            let local_apply = super::update_contract(
                op_manager,
                key,
                update_data.clone(),
                related_contracts.clone(),
                crate::contract::Priority::ClientLocal,
                crate::node::ApplyOrigin::ClientLocal,
                // #5147: no upstream sender, so nothing is covered. An EMPTY
                // registration is not the same as no registration — it collapses
                // any concurrent relayed coverage for this contract, so a local
                // update is never suppressed against someone else's target list.
                crate::ring::broadcast_coverage::BroadcastOrigin::local(),
            )
            .await;

            let UpdateExecution {
                value: updated_value,
                changed: _,
            } = match local_apply {
                Ok(execution) => {
                    // Successful client-local DELTA merge on the remote-target
                    // path clears the backoff too (#4864 review P2, delta-only).
                    if is_delta_update {
                        // Contract-wide clear only when the state advanced
                        // (execution.changed) — a no-op client delta must not
                        // wipe a live Timeout quarantine (#4864 round-4).
                        op_manager
                            .ring
                            .merge_backoff
                            .record_success_local(key.id(), execution.changed);
                    } else if execution.changed {
                        // #4864 round-9 item 3: a CHANGED client FULL-STATE apply on
                        // the remote-target path invalidates the failed-payload MEMO
                        // premise — clear ONLY the memo (mirrors the other paths).
                        // NOT a backoff reset; gated on execution.changed.
                        op_manager
                            .ring
                            .merge_backoff
                            .invalidate_payload_memo(key.id());
                    }
                    execution
                }
                Err(err) => {
                    // The wire format `UpdateMsg::RequestUpdate.value:
                    // WrappedState` carries a post-merge full state, so this
                    // branch must produce one before forwarding. The narrow
                    // failure mode self-heal addresses is "missing contract
                    // parameters" from `runtime.rs::get_params` (the local
                    // `state_store` has no entry for this contract because
                    // the originator never PUT/GET'd it).
                    //
                    // Self-heal by triggering a background GET from the
                    // target peer chosen by proximity / ring routing for
                    // this contract key. After the auto-fetch lands, the
                    // local store is primed and a client retry succeeds.
                    //
                    // Why `target_addr` and not some other peer: the
                    // auto-fetch is a directed `start_targeted_sub_op_get`
                    // targeting a SocketAddr, and `target_addr` is the
                    // only addr we
                    // have at this site that the routing layer just
                    // confirmed is reachable AND closer to the key than
                    // we are. It may not host the contract directly, but
                    // its own proximity cache / closest_hosting routing
                    // is far more likely to land on a hoster on the next
                    // hop than ours is (we don't host the contract, by
                    // definition of being in this branch).
                    //
                    // Discriminator is the NARROW
                    // `is_missing_contract_parameters` predicate, NOT the
                    // broader `!is_contract_exec_rejection`: an
                    // `is_contract_exec_rejection`-negation also matches
                    // `Deser` / `InvalidState` / `InvalidDelta` / `Other`
                    // / `DoublePut` / `InvalidArrayLength` and storage
                    // errors. Auto-fetching on a malformed-input or
                    // disk-error case is wasted work and an
                    // amplification vector (every malformed delta
                    // triggers a wire-level GET). Skeptical-review
                    // callout on PR #4072 #4 (#4066).
                    if err.is_missing_contract_parameters() {
                        tracing::warn!(
                            tx = %client_tx,
                            contract = %key,
                            error = %err,
                            target = %target_addr,
                            phase = "auto_fetch_originator",
                            "update: originator has no contract \
                             code/params for this contract; triggering \
                             auto-fetch from target and asking client to retry"
                        );
                        // Originator self-heal: a local client is waiting on
                        // the retry, so this is demand-driven and bypasses the
                        // #4473 phantom-interest gate (Codex review on #4489).
                        op_manager.try_auto_fetch_contract(
                            &key,
                            target_addr,
                            AutoFetchReason::Originator,
                        );
                        return Ok(DriverOutcome::Publish(Err(ErrorKind::OperationError {
                            cause: format!(
                                "originator missing contract code/params for {key}; \
                                     auto-fetch triggered from {target_addr}, \
                                     client should retry after the local store \
                                     is primed (typically <5s in healthy \
                                     networks). Note: `try_auto_fetch_contract` \
                                     is rate-limited 5min/contract, so retries \
                                     within that window will surface the same \
                                     error until the in-flight GET completes."
                            )
                            .into(),
                        }
                        .into())));
                    }

                    // Issue #4251: per-contract queue saturation is
                    // transient backpressure, not an operator-actionable
                    // failure. On a hot contract this fires hundreds of
                    // times per second; ERROR-level here drowns real
                    // failures on the originator path. Real WASM faults
                    // (OOG, traps, missing parameters) keep the ERROR
                    // level.
                    if err.is_contract_queue_full() {
                        tracing::debug!(
                            tx = %client_tx,
                            contract = %key,
                            error = %err,
                            event = "queue_full",
                            "update: per-contract queue saturated before forwarding"
                        );
                    } else {
                        tracing::error!(
                            tx = %client_tx,
                            contract = %key,
                            error = %err,
                            phase = "error",
                            "update: failed to apply update locally before forwarding"
                        );
                    }
                    return Err(err);
                }
            };

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
                "update: forwarded to target, operation complete"
            );

            // #4923: same as the local-only arm — fetch the contract's REAL
            // summary post-commit for the client response (may describe a
            // slightly newer state under concurrency; harmless, see the
            // local-only arm's comment).
            let summary = super::contract_summary_or_empty(
                op_manager,
                key,
                crate::contract::Priority::ClientLocal,
            )
            .await;
            let host_result: HostResult = Ok(HostResponse::ContractResponse(
                ContractResponse::UpdateResponse { key, summary },
            ));
            Ok(DriverOutcome::Publish(host_result))
        }
    }
}

// --- Outcome delivery ---

/// Dashboard op_stats success classifier (issue #4010). Extracted so the
/// success/failure mapping per `DriverOutcome` variant has direct unit
/// coverage; `deliver_outcome` itself takes an `OpManager` which is
/// expensive to construct in tests.
fn classify_update_outcome_for_op_stats(outcome: &DriverOutcome) -> bool {
    matches!(outcome, DriverOutcome::Publish(Ok(_)))
}

fn deliver_outcome(op_manager: &OpManager, client_tx: Transaction, outcome: DriverOutcome) {
    // Dashboard op_stats UPDATE counter (issue #4010). The legacy
    // `report_result` recording path is bypassed for driver
    // terminal replies (see `node::record_op_result` rustdoc), so
    // every terminal outcome must be recorded here.
    //
    // Sub-operation gate: defensive. Today UPDATE has no sub-operation
    // entry points (it's fire-and-forget at the originator), so
    // `client_tx.is_sub_operation()` is always false on this path. The
    // explicit gate matches SUBSCRIBE's pattern so a future change that
    // routes a sub-op tx through `run_client_update` doesn't silently
    // start inflating the user-facing dashboard counter.
    if !client_tx.is_sub_operation() {
        let success = classify_update_outcome_for_op_stats(&outcome);
        crate::node::network_status::record_op_result(
            crate::node::network_status::OpType::Update,
            success,
        );
    }

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
                "update: infrastructure error; publishing synthesized client error"
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

// ── Relay UPDATE drivers (non-streaming) ─────────────────────────────────────
//
// UPDATE has no end-to-end response, so the relay drivers are
// strictly fire-and-forget: they apply state changes locally (or
// forward once downstream) and exit. No `send_and_await`, no
// `pending_op_results` slot, no upstream reply.
//
// Amplifier audit (vs GET relay):
//   1. No retry loop at relay → no MAX_RELAY_RETRIES needed.
//   2. No fresh `attempt_tx` → forwarding reuses `incoming_tx`.
//   3. No `ForwardingAck` → never had one.
//   4. Per-node dedup gate (`active_relay_update_txs`) drops
//      duplicate inbound Requests for an in-flight tx.
//
// Migrated wire variants:
//   - `UpdateMsg::RequestUpdate` (non-streaming relay arm).
//   - `UpdateMsg::BroadcastTo`  (non-streaming relay arm:
//      `update.rs::process_message`, lines 595–825).
//
// UPDATE auto-fetch (`OpManager::try_auto_fetch_contract`)
// dispatches `get::op_ctx_task::start_targeted_sub_op_get` (a
// directed GET that pre-seeds the UPDATE sender as the first hop
// and falls back to `k_closest_potentially_hosting` for retries).

/// Spawn a relay driver for a fresh inbound `UpdateMsg::RequestUpdate`.
///
/// Caller-side gates (in `node.rs`): `source_addr.is_some()`. Per-node
/// dedup against concurrent inbound retries is enforced by
/// `OpManager.active_relay_update_txs` inside this driver.
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

    // Note: the Phase 2 per-(sender, contract) rate limit is applied
    // UPSTREAM at the wire dispatch site in `node.rs` so the same
    // gate covers all four UPDATE wire variants (RequestUpdate,
    // BroadcastTo, RequestUpdateStreaming, BroadcastToStreaming)
    // uniformly. Rejected messages never reach this driver.

    if !op_manager.active_relay_update_txs.insert(incoming_tx) {
        RELAY_UPDATE_DEDUP_REJECTS.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        tracing::debug!(
            tx = %incoming_tx,
            %key,
            %sender_addr,
            phase = "relay_update_dedup_reject",
            "UPDATE relay: duplicate RequestUpdate for in-flight tx, dropping"
        );
        return Ok(());
    }

    // Routing/hosting attribution (Group C): count this as a genuine relayed
    // UPDATE (past the dedup gate — a deduped duplicate is not a relay).
    crate::node::network_status::record_relayed_update();

    tracing::debug!(
        tx = %incoming_tx,
        %key,
        %sender_addr,
        phase = "relay_update_request_start",
        "UPDATE relay: spawning RequestUpdate driver"
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

/// Resolve an originator's target list against the peers we could ourselves
/// broadcast this contract to (#5147).
///
/// Resolves against `advertised_cohost_pub_keys` — the exact set
/// `get_broadcast_targets_update` draws from — rather than every connected
/// peer, so the work is proportional to the fan-out (median 16-17) instead of
/// the connection count, and a named peer we would never have targeted costs
/// nothing.
///
/// `tx` is the transaction the list arrived on. The hashes are seeded with it,
/// so resolving under any other transaction yields nothing; that is what stops
/// a list from being replayed onto a different broadcast.
///
/// # Both halves are behind the #5147 version gate, including the sender
///
/// The sender exclusion needs no wire change of its own — it is a decision
/// about our OWN fan-out — so it would ship unconditionally if left ungated.
/// It is gated anyway, and deliberately:
///
/// * It has the same unsafe shape as the target list. Excluding the peer that
///   delivered a payload assumes it holds what we are about to send, and on a
///   merge path our post-merge state can be strictly newer than what it sent
///   us. That is the same assumption the target list makes, so it deserves the
///   same caution rather than inheriting the list's without its gate.
/// * Ungated, it would reach every peer on the floor release with none of the
///   machinery that makes the rest of this reviewable — no sim override, so no
///   simulation could turn it off to get a baseline, and no version floor, so
///   no staged rollout.
///
/// Gating on the SENDER's version is the coherent choice: if the peer that
/// delivered this payload predates the feature, we are in the legacy world
/// with it and behave toward it exactly as today. An unknown version fails
/// closed, which on a gateway link is the common case until #5161 deploys.
/// NOTE: `pub(crate)` rather than private, and deliberately NOT reached through
/// a test-only shim, so a unit test in `node::op_state_manager` drives the real
/// gate.
///
/// A test-gated item must not be introduced above this point in the file. The
/// source-scrape pins below slice production code with `production_source`,
/// which truncates the file at the first test-gate attribute, so an earlier one
/// silently hides every function after it from those pins — they then fail with
/// "could not find ...", or worse, pass vacuously. Note the attribute must not
/// be spelled out in prose here either: `production_source` matches the literal
/// text, so even a comment mentioning it truncates the slice. That is not
/// hypothetical; writing this warning the obvious way broke four pins.
pub(crate) fn resolve_covered_peers(
    op_manager: &OpManager,
    key: &ContractKey,
    tx: &Transaction,
    sender_addr: SocketAddr,
    covered: &crate::ring::broadcast_coverage::CoveredPeers,
) -> crate::ring::broadcast_coverage::BroadcastOrigin {
    // The two halves are gated SEPARATELY, and conflating them is what made the
    // whole feature inert. See `BroadcastOrigin::relayed_list_only`.
    //
    // The covered LIST is self-gating: it rides only on the V2 variants, so a
    // pre-floor sender hands us an empty one and suppresses nothing no matter
    // what we do here. Holding a populated list IS proof the sender supports the
    // feature — its own act, rather than our record of it.
    //
    // The sender EXCLUSION keeps the version gate — but as a ROLLOUT SWITCH,
    // not as wire or semantic safety, and it is worth being exact because the
    // obvious reading is wrong. Not sending a peer a message cannot break its
    // decoding, so there is no wire-compat exposure to fail closed against; and
    // the semantic worry (our post-merge state can be newer than what the
    // sender gave us) is version-INDEPENDENT — a post-floor sender has exactly
    // the same property, and that is the risk the list half accepts and the
    // multi-writer simulation validates. What the gate buys is a staged rollout
    // and a sim toggle, both of which are worth having on a change with this
    // blast radius.
    //
    // Consequence to keep in view: on an unknown-version link we honour the
    // sender's list while still echoing back to the sender itself — suppressing
    // legs that are only probably redundant, while keeping the one that is
    // certainly redundant. At the fleet median fan-out of 16-17 that is roughly
    // 6% of legs left on the table, on the gateway links that matter most,
    // until #5167 propagates.
    let resolved = if covered.is_empty() {
        std::collections::HashSet::new()
    } else {
        let candidates = op_manager.advertised_cohost_pub_keys(key);
        covered.resolve(tx, candidates.iter())
    };

    if op_manager
        .ring
        .connection_manager
        .supports_broadcast_target_list(sender_addr)
    {
        crate::ring::broadcast_coverage::BroadcastOrigin::relayed(sender_addr, resolved)
    } else {
        // Pre-floor or UNKNOWN sender version. Unknown is the common case, not a
        // corner: a node does not learn its gateway's version until #5167
        // propagates, and simulations default that ack gate OFF. Gating the list
        // on this lookup too discarded a claim the message in hand had already
        // proven honourable, which zeroed suppression on the highest-degree
        // links in production and entirely in simulation.
        crate::ring::broadcast_coverage::BroadcastOrigin::relayed_list_only(resolved)
    }
}

/// Spawn a relay driver for a fresh inbound `UpdateMsg::BroadcastTo`.
///
/// Same gates as `start_relay_request_update`. The driver applies the
/// broadcast payload locally (after dedup) and the executor's
/// `BroadcastStateChange` event fans out to other interested peers
/// automatically — the driver itself never sends `BroadcastTo`
/// downstream.
#[allow(clippy::too_many_arguments)]
pub(crate) async fn start_relay_broadcast_to(
    op_manager: Arc<OpManager>,
    incoming_tx: Transaction,
    key: ContractKey,
    payload: DeltaOrFullState,
    sender_summary_bytes: Vec<u8>,
    sender_addr: SocketAddr,
    // Peers the sender says it already delivered to (#5147). Empty for the
    // legacy `BroadcastTo`, which names none.
    covered: crate::ring::broadcast_coverage::CoveredPeers,
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
            "UPDATE relay: duplicate BroadcastTo for in-flight tx, dropping"
        );
        return Ok(());
    }

    // NOT counted in `relayed_updates_total`: BroadcastTo is update-mesh
    // fan-out to interested co-hosts, not a relayed UPDATE *request* toward the
    // key. Counting fan-out here would conflate relay volume with fan-out
    // volume (one update -> N broadcasts). See `record_relayed_update` doc.

    tracing::debug!(
        tx = %incoming_tx,
        %key,
        %sender_addr,
        phase = "relay_update_broadcast_start",
        "UPDATE relay: spawning BroadcastTo driver"
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
        covered,
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
        if err.is_contract_queue_full() {
            tracing::debug!(
                tx = %incoming_tx,
                %key,
                error = %err,
                phase = "relay_update_request_error",
                event = "queue_full",
                "UPDATE relay: RequestUpdate driver returned error"
            );
        } else {
            tracing::warn!(
                tx = %incoming_tx,
                %key,
                error = %err,
                phase = "relay_update_request_error",
                "UPDATE relay: RequestUpdate driver returned error"
            );
        }
    }
}

#[allow(clippy::too_many_arguments)]
async fn run_relay_broadcast_to(
    guard: RelayUpdateInflightGuard,
    op_manager: Arc<OpManager>,
    incoming_tx: Transaction,
    key: ContractKey,
    payload: DeltaOrFullState,
    sender_summary_bytes: Vec<u8>,
    sender_addr: SocketAddr,
    covered: crate::ring::broadcast_coverage::CoveredPeers,
) {
    let _guard = guard;

    if let Err(err) = drive_relay_broadcast_to(
        &op_manager,
        incoming_tx,
        key,
        payload,
        sender_summary_bytes,
        sender_addr,
        covered,
    )
    .await
    {
        if err.is_contract_queue_full() {
            tracing::debug!(
                tx = %incoming_tx,
                %key,
                error = %err,
                phase = "relay_update_broadcast_error",
                event = "queue_full",
                "UPDATE relay: BroadcastTo driver returned error"
            );
        } else {
            tracing::warn!(
                tx = %incoming_tx,
                %key,
                error = %err,
                phase = "relay_update_broadcast_error",
                "UPDATE relay: BroadcastTo driver returned error"
            );
        }
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
        "UPDATE relay: processing RequestUpdate"
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
            changed,
        } = super::update_contract(
            op_manager,
            key,
            UpdateData::State(State::from(value.clone())),
            related_contracts.clone(),
            crate::contract::Priority::NetworkRelay,
            crate::node::ApplyOrigin::NetworkRelay,
            // #5147: a `RequestUpdate` names no delivery targets (it is a
            // routed request, not a fan-out), so this apply attests to no
            // coverage. Registering empty is deliberate — see the two
            // client-local sites.
            crate::ring::broadcast_coverage::BroadcastOrigin::local(),
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
                "UPDATE relay: yielded no state change, skipping broadcast"
            );
        } else {
            tracing::debug!(
                tx = %incoming_tx,
                %key,
                "UPDATE relay: RequestUpdate succeeded, state changed"
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
                "UPDATE relay: contract not local and no peers to forward to"
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

    let forward_addr = match forward_target.socket_addr() {
        Some(addr) => addr,
        None => {
            tracing::error!(
                tx = %incoming_tx,
                %key,
                target_pub_key = %forward_target.pub_key(),
                "UPDATE relay: forward target has no socket address"
            );
            return Err(OpError::RingError(RingError::NoHostingPeers(*key.id())));
        }
    };

    tracing::debug!(
        tx = %incoming_tx,
        %key,
        next_peer = %forward_addr,
        "UPDATE relay: forwarding to peer that might have contract"
    );

    // Telemetry: relay forwarding update request.
    if let Some(event) =
        NetEventLog::update_request(&incoming_tx, &op_manager.ring, key, forward_target.clone())
    {
        op_manager.ring.register_events(Either::Left(event)).await;
    }

    // Forward downstream. We use the INCOMING tx (end-to-end preservation).
    //
    // Slice A scope: this driver only handles the inbound non-streaming
    // wire variant (the dispatch gate in node.rs filters out the
    // streaming sibling). On relay forward we re-emit the same
    // non-streaming variant regardless of payload size — the size-based
    // upgrade to the streaming sibling on forward (legacy
    // update.rs:518-535 checks `should_use_streaming`) is deferred to
    // slice B together with the streaming-variant migration.
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
            "UPDATE relay: forward send failed"
        );
        return Err(err);
    }

    Ok(())
}

/// Why an inbound broadcast was dropped, for
/// [`send_dropped_broadcast_resync_request`]'s telemetry.
///
/// The repair is identical for both — the sender's summary cache is poisoned
/// the same way either way — but the OPERATOR reading `resync_request_sent`
/// needs to tell them apart: a queue-full drop means this node's executor is
/// saturated, a rate-limited drop means the `(sender, contract)` pair exceeded
/// the dispatch budget in `crate::ring::update_rate_limit`. Those call for
/// different responses, so they must not share one label.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum DroppedBroadcastReason {
    /// `ContractQueueFull` from the executor (#4857).
    QueueFull,
    /// Refused by the per-(sender, contract) UPDATE dispatch rate limiter
    /// in `node.rs` before any driver ran (#5510).
    RateLimited,
    /// The trailing-edge coalesced repair (#5510): one or more drops were
    /// swallowed by a held throttle window, and this is the single request
    /// emitted at that window's close to cover all of them. The original cause
    /// of those drops is deliberately not carried — a window can swallow both
    /// kinds, and one full-state response repairs either.
    DroppedDuringThrottleWindow,
}

impl DroppedBroadcastReason {
    /// Stable telemetry label. `"queue_full"` is preserved verbatim from the
    /// pre-#5510 log line so existing operator greps keep working.
    fn as_str(self) -> &'static str {
        match self {
            DroppedBroadcastReason::QueueFull => "queue_full",
            DroppedBroadcastReason::RateLimited => "rate_limited",
            DroppedBroadcastReason::DroppedDuringThrottleWindow => "dropped_during_window",
        }
    }
}

/// On a DROPPED inbound broadcast, emit a RATE-LIMITED `ResyncRequest` to the
/// sender so it clears its poisoned summary cache of us and re-sends the change
/// it believes we already have (#4857, #5510).
///
/// Two distinct drops land here, and the reason they need the SAME repair is
/// the reason they are handled by one helper:
///
/// - `ContractQueueFull` at the executor (#4857) — the delta / full state
///   reached this node but was never applied.
/// - Refusal by the dispatch rate limiter in `node.rs` (#5510) — the payload
///   never even reached a driver. `reason` distinguishes the two in telemetry;
///   see `DroppedBroadcastReason`.
///
/// Both are SILENT to the sender: it cached its own summary as ours on send-Ok
/// (`broadcast_queue.rs::record_delivery_to_interest`), so it believes we are
/// current and will never re-send the dropped change. Since #5464 the payload
/// is a true difference against that belief, so once the belief is wrong the
/// lost entries are excluded from every LATER delta too — the divergence is
/// permanent, not just delayed. Left unhealed, a rarely-changing field
/// (member_info, a ban, a config) diverges until the InterestSync heartbeat
/// happens to correct it, which is 300s at minimum and 40-75 min at field
/// shared-set sizes since #5238/#5346. An inbound `ResyncRequest` makes the
/// sender clear that cached summary (node.rs handler) and re-send full state.
///
/// The `ResyncRequest` this emits, and the `ResyncResponse` it provokes, both
/// ride `NetMessageV1::InterestSync` — NOT `NetMessageV1::Update` — so neither
/// is subject to the UPDATE dispatch rate limiter that caused the #5510 drop.
/// The repair therefore cannot be silenced by the condition it repairs. do NOT
/// move this repair onto an `Update` wire variant.
///
/// Issue #4251 suppressed this entirely because one request per dropped delta
/// amplifies into a full-state storm onto the same saturated queue (~40
/// ResyncRequests/sec on a hot contract). The rate limit
/// (`InterestManager::begin_resync_request`) bounds that to at most one
/// request per (contract, sender) per `RESYNC_REQUEST_MIN_INTERVAL`.
///
/// Shared by both broadcast drivers (`drive_relay_broadcast_to` and
/// `drive_relay_broadcast_to_streaming`) and by the `node.rs` rate-limiter drop
/// arm, so the three drop paths cannot drift. The queue-full callers keep
/// auto-fetch suppressed (we already hold the contract; a GET onto the same
/// saturated queue is pure amplification).
///
/// do NOT re-add an unthrottled emission — see #4251/#4857.
pub(crate) async fn send_dropped_broadcast_resync_request(
    op_manager: &OpManager,
    key: ContractKey,
    sender_addr: SocketAddr,
    incoming_tx: Transaction,
    reason: DroppedBroadcastReason,
) {
    // `note_debt: true` — this IS a genuine new drop, so if gate 1 refuses
    // (a reservation is already held) the debt belongs on that reservation.
    let (reservation_deadline, correlated) =
        match reserve_resync_emit(op_manager, key, sender_addr, incoming_tx, reason, true).await {
            Ok(reserved) => reserved,
            Err(ReserveRefusal::WindowHeld) => {
                // The held reservation's chain now owes this drop's repair and
                // will emit it at the window's close. Nothing more to do.
                return;
            }
            Err(ReserveRefusal::GlobalCap { deadline }) => {
                // Review round 2: this path used to simply return. The global
                // cap `cancel`s the reservation, so NOTHING recorded the debt
                // and NO follow-up existed to retry it — a one-off drop that
                // happened to arrive as a contract's third repair in a minute
                // was lost until anti-entropy, which is the very failure #5510
                // exists to close. Spawn a follow-up that carries the debt and
                // retries once the cap refills. It delivers nothing first
                // (no request was authorized) and anchors on the reservation
                // it actually holds (X1) rather than an approximation of it: the
                // window is what bounds the chain, and waiting on a different
                // instant than the one the flag lives on means either waking
                // early to find nothing or waking late and holding the slot past
                // the window.
                let retry_deadline = deadline;
                spawn_resync_followups(
                    op_manager,
                    key,
                    sender_addr,
                    incoming_tx,
                    FollowupStart::OwedOnly {
                        retry_at: retry_deadline,
                    },
                );
                return;
            }
        };
    // Spawn BEFORE the blocking dispatch below — that ordering is load-bearing;
    // see `spawn_resync_followups` (#4862 P2).
    spawn_resync_followups(
        op_manager,
        key,
        sender_addr,
        incoming_tx,
        FollowupStart::Reserved {
            deadline: reservation_deadline,
            correlated,
        },
    );
    dispatch_resync_request(op_manager, key, sender_addr, incoming_tx).await;
}

/// How a follow-up chain begins, and the reservation state that goes with it.
///
/// The deadline and the correlation flag are carried IN the variant rather than
/// passed alongside it, so `OwedOnly` cannot be handed a correlation flag it has
/// no request to correlate — the illegal pairing is unrepresentable instead of
/// merely unwritten. Same remedy as `.claude/rules/bug-prevention-patterns.md`
/// prescribes for paired `Option` fields that must co-occur.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum FollowupStart {
    /// The caller holds a granted reservation and has dispatched its request.
    /// The chain re-delivers that request, then sweeps at the window's close.
    Reserved {
        /// The granted reservation's close, on the throttle's own clock.
        deadline: Instant,
        /// Whether the correlation record was taken (#4864 round-8). Drives the
        /// re-delivery's #4863 "did the first request already land" gate.
        correlated: bool,
    },
    /// The caller was refused by the global emit cap, so nothing was emitted
    /// and nothing is authorized. The chain owes a repair from the outset: it
    /// delivers nothing, waits out an interval, and then tries to reserve.
    /// There is no reservation to anchor to, hence a fresh interval and no
    /// correlation flag.
    OwedOnly {
        /// When to make the first re-reservation attempt.
        retry_at: Instant,
    },
}

/// Take the two gates and record the correlation entry for ONE `ResyncRequest`,
/// returning the reservation deadline and whether the request is correlated.
///
/// Split out of [`send_dropped_broadcast_resync_request`] so the #5510
/// trailing-edge sweep can emit a NEW request without re-entering that function.
/// Calling it recursively is not an option: `send_...` spawns a task whose
/// future would then have to prove itself `Send` in terms of itself, and rustc
/// rejects that cycle outright. Sharing this function rather than hand-inlining
/// the gates is what stops the two emit paths drifting — the failure shape
/// `.claude/rules/bug-prevention-patterns.md` calls "manually-inlined originator
/// side effects".
///
/// Returns `None` when either gate refused, in which case NOTHING was emitted
/// and no correlation entry exists.
/// Which gate refused a reservation, for callers that must react differently.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ReserveRefusal {
    /// Gate 1: a reservation for this `(contract, sender)` is already held.
    /// For the follow-up chain this means a SIBLING owns the window, so the
    /// chain must stop rather than keep retrying against a reservation it does
    /// not hold (review round 2).
    WindowHeld,
    /// Gate 2: the global per-contract emit cap. The window is KEPT and its
    /// dropped-during-window flag set (review round 3, X1), so the chain that
    /// owns it serves the debt at the close and later drops on the pair are
    /// throttled into that same chain rather than each spawning one. Carries
    /// the held reservation's deadline so the chain anchors on the real window
    /// rather than an approximation of it.
    GlobalCap { deadline: Instant },
}

async fn reserve_resync_emit(
    op_manager: &OpManager,
    key: ContractKey,
    sender_addr: SocketAddr,
    incoming_tx: Transaction,
    reason: DroppedBroadcastReason,
    note_debt: bool,
) -> Result<(Instant, bool), ReserveRefusal> {
    // Gate 1 — per-(contract, sender)/30s throttle (#4857/#4862 + #4864 round-6).
    // RESERVE the window atomically (`begin` checks AND records under the
    // throttle's own lock) and return the reservation deadline on the throttle's
    // OWN clock (the #4862 retry anchors to it). Two concurrent queue-full
    // callbacks for the same (contract, sender) cannot both pass. If the global
    // cap below then rejects, we `cancel` the reservation so the window is
    // released (not burned).
    let crate::ring::interest::ResyncGrant::Granted {
        deadline: reservation_deadline,
    } = op_manager
        .interest_manager
        .begin_resync_request_or_note_drop(&key, sender_addr, note_debt)
    else {
        // #5510: this drop's repair is being refused, and BEFORE this change
        // nothing ever re-sent it — the throttle simply returned. Mark the held
        // reservation so the trailing-edge sweep emits ONE coalesced
        // ResyncRequest when the window closes. One full-state ResyncResponse
        // restores every entry the window swallowed, so one flag covers any
        // number of drops and the #4251 amplification bound is untouched.
        // The debt was recorded INSIDE the call above, under the throttle's own
        // lock, when `note_debt` is set — never as a second lock acquisition
        // here. See `begin_resync_request_or_note_drop` for why the two-step
        // has a TOCTOU hole the generation check does not close.
        //
        // `note_debt` is false for the chain's own coalesced retry (review
        // round 2). Without it the retry records debt against the reservation
        // it is ALSO tracking in `owed`, so one swallowed drop is counted
        // twice and the pair pays an extra full-state response per window.
        // Only a genuine NEW drop may record debt.
        //
        // Per-(contract, sender)/30s throttle (existing #4857 bound).
        tracing::debug!(
            tx = %incoming_tx,
            contract = %key,
            sender = %sender_addr,
            event = "queue_full_resync_throttled",
            reason = reason.as_str(),
            "UPDATE relay: dropped-broadcast ResyncRequest throttled (rate limit) to avoid amplification"
        );
        return Err(ReserveRefusal::WindowHeld);
    };

    // ALSO subject to the GLOBAL per-contract emit cap (#4864 round-4 P1): the
    // #4857 queue-full heal path previously bypassed it, so a saturated contract
    // with many senders still emitted per-(contract, sender)/30s. Routing it
    // through resync_emit_limiter bounds the contract's TOTAL queue-full resync
    // emission. (This is NOT a mirror of the delta-failure path — that path is
    // global-cap-only, with no per-sender throttle. The global suppression records
    // the same `record_resync_request_suppressed` metric node.rs does.) On
    // suppression, KEEP the reservation and mark it as owing a repair.
    //
    // This used to `cancel`, releasing the window so the sender could retry as
    // soon as tokens refilled. That was wrong once a follow-up chain existed
    // (#5510 review round 3, X1): with the window released, EVERY later drop on
    // the pair also reaches the cap, also gets refused, and also spawns its own
    // chain — so one hot pair could take slot after slot, in seconds, all of
    // them waiting on the same empty per-contract bucket. Keeping the window
    // makes those later drops `WindowHeld` instead, so they hand their debt to
    // the one chain already carrying it. One chain per pair per window, the
    // same bound gate 1 already gives every other path.
    //
    // Note this is not a lost repair: the reservation is retained WITH its
    // dropped-during-window flag set, so the chain that owns it serves the debt
    // at the window's close, once tokens have refilled.
    if !op_manager
        .ring
        .resync_emit_limiter
        .check_and_record(*key.id())
    {
        op_manager
            .interest_manager
            .note_resync_debt_on_held_reservation(&key, sender_addr);
        // The COUNTER answers "how many repairs did the cap suppress" — one per
        // drop that wanted one — so a chain's own retries must not inflate it
        // (review round 3). One swallowed drop retried three times is ONE
        // suppressed repair, not three; counting each attempt would make the
        // metric drift with chain length rather than with the thing an operator
        // is asking about, and it would grow precisely when the cap is busiest.
        let is_chain_retry = matches!(reason, DroppedBroadcastReason::DroppedDuringThrottleWindow);
        if !is_chain_retry {
            crate::config::GlobalTestMetrics::record_resync_request_suppressed();
        }
        tracing::debug!(
            tx = %incoming_tx,
            contract = %key,
            sender = %sender_addr,
            // Distinct event for a chain retry, so the log and the counter
            // agree about what each is describing.
            event = if is_chain_retry {
                "coalesced_resync_suppressed_global"
            } else {
                "queue_full_resync_suppressed_global"
            },
            reason = reason.as_str(),
            "UPDATE relay: dropped-broadcast ResyncRequest suppressed by global per-contract cap"
        );
        return Err(ReserveRefusal::GlobalCap {
            deadline: reservation_deadline,
        });
    }

    // Both gates passed → the reservation stands (already recorded by `begin`).
    tracing::info!(
        tx = %incoming_tx,
        contract = %key,
        target = %sender_addr,
        event = "resync_request_sent",
        reason = reason.as_str(),
        "UPDATE relay: sending rate-limited ResyncRequest after a dropped inbound \
         broadcast (#4857/#5510)"
    );
    // #4864 round-8: record the outstanding request so the matching ResyncResponse
    // from this peer is authorized ONCE at the receive arm; an unsolicited or
    // replayed response finds no entry and is dropped without running WASM.
    //
    // #4863: this MUST stay ABOVE the retry spawn below. The retry's conditional
    // gate READS this record to decide whether the first request already landed
    // (the receive arm consumes it on a matching ResyncResponse), so a retry task
    // that could observe a not-yet-written record would conclude "landed" and skip
    // the heal. Recording first makes that race structurally impossible; pinned by
    // `queue_full_resync_retry_is_bounded_and_reservation_scoped`. Recording is a
    // non-blocking map insert, so it does not delay the spawn (#4862 P2).
    // `false` = the strict cap rejected a brand-new key, so the request is
    // UNCORRELATED. The retry's gate must then fall back to unconditional
    // re-dispatch: `is_outstanding` would read `false` forever, and reading that
    // as "landed" would silently disable the retry node-wide while the map is
    // full. A re-dispatch still heals when uncorrelated — the responder clears
    // its cached summary on the ResyncRequest itself (#4857), independently of
    // our correlation record; only the full-state ResyncResponse needs it.
    let correlated = op_manager
        .ring
        .outstanding_resync_requests
        .record(*key.id(), sender_addr);

    Ok((reservation_deadline, correlated))
}

/// Spawn the follow-up task that owns everything this reservation still has to
/// do: the #4862 re-delivery burst, then the #5510 trailing-edge sweep, then —
/// if that sweep found a swallowed drop — the next reservation's burst and
/// sweep, and so on.
///
/// # Why it is ONE looping task
///
/// The obvious shape is for the sweep to call
/// [`send_dropped_broadcast_resync_request`] again. It does not compile, and the
/// reason is worth recording so nobody re-tries it: that function spawns a task,
/// so proving the spawned future `Send` would require already knowing whether it
/// is `Send`, and rustc rejects the cycle rather than resolving it. Looping in
/// one task removes the recursion entirely and is a better fit anyway — the loop
/// exits the first time a window closes with nothing swallowed, so a quiet
/// contract costs one task for one window.
///
/// # Ordering (#4862 P2), unchanged and still load-bearing
///
/// The caller spawns this BEFORE its blocking `notify_node_event`, which can
/// consume up to `NOTIFICATION_SEND_TIMEOUT` (30s) under backpressure. Spawning
/// after it would let the task start with the window already gone and do
/// nothing. The correlation entry is recorded before this too (in
/// `reserve_resync_emit`), so the #4863 gate can never read a not-yet-written
/// record and mistake it for "landed".
///
/// # Bounds
///
/// One node-wide slot per chain ([`crate::ring::interest::
/// MAX_OUTSTANDING_QUEUE_FULL_RESYNC_RETRIES`]), and the chain only continues
/// while drops keep being swallowed. Each iteration emits at most
/// `1 + QUEUE_FULL_RESYNC_MAX_RETRIES` re-deliveries of one authorized request
/// plus at most one NEW request, per (contract, sender) per window — the #4251
/// bound.
///
/// **The slot is held for as long as a repair is OWED, not merely for one
/// window.** When a gate refuses a coalesced emit the chain keeps the debt and
/// waits another interval rather than dropping it, so the slot stays occupied
/// across that wait. [`MAX_COALESCED_RESYNC_WINDOWS`] is what makes that
/// bounded: the debt can survive at most that many windows, after which the
/// chain gives up and the guard drops. Without the window cap, "hold the slot
/// while debt is owed" and "re-arm the debt on every swallowed drop" would
/// together be an unbounded hold — which is the shape
/// `.claude/rules/code-style.md` forbids, and the reason the two were designed
/// together rather than separately.
/// Maximum consecutive reservation windows one follow-up chain will serve
/// (#5510, review finding 1).
///
/// Without a bound the chain re-reserves for as long as each window keeps
/// swallowing a drop, so a pair under sustained drops holds its node-wide slot
/// forever. 256 such pairs — reachable by legitimately busy traffic, not only by
/// an attacker — would then permanently starve every later pair of BOTH the
/// #4862 re-delivery and the trailing repair. That is the refreshable-cap shape
/// `.claude/rules/code-style.md` forbids: an exemption that renews without limit
/// is not a bound.
///
/// Three is generous. A pair that keeps dropping every window is also taking a
/// fresh immediate reservation from each new real drop, so the trailing repair
/// is marginal there; its value is concentrated in the case it exists for, where
/// traffic STOPS right after a swallowed drop.
///
/// # The resulting bound, stated in one place
///
/// **One chain per `(contract, sender)` pair per window**, because gate 1 is per
/// pair and a global-cap refusal now KEEPS the window rather than releasing it
/// (X1) — so later drops on the pair are `WindowHeld` and hand their debt to the
/// chain already carrying it, instead of each spawning one of their own.
///
/// Node-wide, chains are bounded by
/// [`crate::ring::interest::MAX_OUTSTANDING_QUEUE_FULL_RESYNC_RETRIES`] (256)
/// slots, shared with the #4862 re-delivery. Each chain lives at most
/// `MAX_COALESCED_RESYNC_WINDOWS` served windows (3) or
/// [`MAX_COALESCED_RESYNC_ATTEMPTS`] passes (6), whichever comes first, plus the
/// tokio backstop inside the wait.
///
/// There is deliberately no SECOND pool for chains that start owing a repair. An
/// earlier revision added one, to bound a case where a contract with N dropping
/// senders spawned N such chains per window. X1 removed the cause of that rather
/// than capping its effect, so a second pool would now bound something that
/// cannot happen, at the cost of a limit nobody can reason about.
const MAX_COALESCED_RESYNC_WINDOWS: u32 = 3;

/// Maximum loop passes for one follow-up chain, however few are SERVED.
///
/// [`MAX_COALESCED_RESYNC_WINDOWS`] counts windows actually served, so a chain
/// that is only ever refused would never reach it — the counters measure
/// different things on purpose (review round 2: counting attempts as windows
/// dropped the debt at t=90 after two refusals, having served none). This is the
/// separate bound on attempts, so a pair whose contract is permanently over the
/// global emit cap cannot hold its node-wide slot indefinitely.
const MAX_COALESCED_RESYNC_ATTEMPTS: u32 = 6;

/// Poll granularity for the trailing-edge wait (#5510).
///
/// Deliberately coarser than [`QUEUE_FULL_RESYNC_RETRY_POLL_INTERVAL`]: the
/// #4862 retry polls at 100 ms because it aims at jittered sub-second targets
/// inside the window, whereas this wait only has to NOTICE a 30 s deadline. At
/// 100 ms that was ~300 wakeups per repair, and at the 256-slot cap ~2,500
/// wakeups/s on exactly the node already loaded enough to saturate it. One
/// second keeps the worst case ~250/s and still lands the emit within a second
/// of the window closing, which is nothing against a 30 s window.
const COALESCED_RESYNC_POLL_INTERVAL: Duration = Duration::from_secs(1);

/// Throttle slot for the slot-cap log line (#5510, review finding 14).
///
/// Process-global, and stamped with `tokio::time::Instant`, so under
/// `start_paused` it carries whichever test runtime touched it first. That is
/// harmless for a log throttle — the worst case is one suppressed or one extra
/// line — but do NOT write a test that asserts on this line's presence or
/// absence without first moving the slot per-node. A test that did would be
/// order-dependent in exactly the way `.claude/rules/testing.md` describes, and
/// invisible under nextest.
///
/// The condition fires once per repair attempt on a node that is by definition
/// loaded enough to have exhausted 256 slots, so an unthrottled line there is a
/// log storm on the worst possible node. Same reasoning and interval as
/// `update_rate_limit`'s three throttled signals.
static SLOT_CAP_LOG: std::sync::Mutex<Option<tokio::time::Instant>> = std::sync::Mutex::new(None);

/// One minute, matching `update_rate_limit::EVICTION_LOG_INTERVAL`.
const SLOT_CAP_LOG_INTERVAL: Duration = Duration::from_secs(60);

/// Whether the slot-cap line is due, stamping the slot when it is.
fn slot_cap_log_due(now: tokio::time::Instant) -> bool {
    let mut last = match SLOT_CAP_LOG.lock() {
        Ok(guard) => guard,
        // Poisoned only if a previous holder panicked while formatting a log
        // line. Losing the throttle is not worth propagating a panic here.
        Err(poisoned) => poisoned.into_inner(),
    };
    let due = match *last {
        Some(prev) => now.saturating_duration_since(prev) >= SLOT_CAP_LOG_INTERVAL,
        None => true,
    };
    if due {
        *last = Some(now);
    }
    due
}

fn spawn_resync_followups(
    op_manager: &OpManager,
    key: ContractKey,
    sender_addr: SocketAddr,
    incoming_tx: Transaction,
    start: FollowupStart,
) {
    // ONE pool, because X1 removed the reason for two. An earlier round gave
    // owing chains their own smaller pool, on the grounds that a contract with
    // N dropping senders could spawn N of them per window and starve the #4862
    // re-delivery. That explosion was real, but its cause was the cap path
    // CANCELLING the reservation: with the window released, every later drop on
    // the pair was refused afresh and spawned a chain of its own. X1 keeps the
    // window, so those drops are `WindowHeld` and hand their debt to the chain
    // already carrying it — one chain per pair per window, the same bound gate 1
    // gives every other path. A second pool would now cap a symptom that no
    // longer occurs, at the cost of a bound nobody can reason about.
    let Some(slot) = op_manager.interest_manager.try_reserve_resync_retry_slot() else {
        // #5510: at the node-wide cap NOTHING is spawned, so this reservation
        // gets neither the #4862 re-delivery nor the trailing coalesced repair.
        // A drop the window then swallows waits for the next drop after it
        // closes. `info!` rather than `debug!` because `release_max_level_info`
        // compiles `debug!` out — the very reason #5510 was invisible in the
        // field — and because this is the one path where the repair silently
        // does less than the code promises.
        if slot_cap_log_due(tokio::time::Instant::now()) {
            tracing::info!(
                tx = %incoming_tx,
                contract = %key,
                target = %sender_addr,
                cap = crate::ring::interest::MAX_OUTSTANDING_QUEUE_FULL_RESYNC_RETRIES,
                event = "resync_retry_slot_cap_reached",
                "UPDATE relay: at the node-wide resync-retry slot cap; immediate \
                 ResyncRequests still send, but reservations are getting no \
                 re-delivery and no trailing coalesced repair. A repair already \
                 owed on a held window waits for the next drop on that pair or \
                 for anti-entropy. Throttled to one line per minute \
                 (#4862 P1/#5510)"
            );
        }
        return;
    };

    let op_mgr = op_manager.clone();
    GlobalExecutor::spawn(async move {
        let _slot = slot; // frees the node-wide slot on task exit
        let (mut deadline, mut correlated) = match start {
            FollowupStart::Reserved {
                deadline,
                correlated,
            } => (deadline, correlated),
            FollowupStart::OwedOnly { retry_at } => (retry_at, false),
        };
        // The caller dispatches ITS window's request itself, concurrently with
        // this spawn (#4862 P2), so the first pass only re-delivers.
        let mut dispatch_pending = false;
        // Whether this pass should re-deliver at all. False after a gate refused
        // a coalesced emit — there is then no newly-authorized request to
        // re-deliver, and re-delivering the previous window's would be an emit
        // outside any reservation, the #4251 bound — and false from the outset
        // for a chain that starts owing (the caller was capped and emitted
        // nothing).
        let mut deliver = matches!(start, FollowupStart::Reserved { .. });
        // A repair is owed but not yet authorized. Held ACROSS iterations
        // because the flag is consumed at the window's close, before the gates
        // run: a refusal after that point would otherwise drop the repair
        // silently, and if that was the last message on the pair the divergence
        // would persist to anti-entropy (review finding 16).
        let mut owed = matches!(start, FollowupStart::OwedOnly { .. });
        // Windows this chain has SERVED — reservations actually granted and
        // emitted on, including the caller's when it had one. Deliberately NOT
        // a count of attempts (review round 2): incrementing on a refusal meant
        // that with the emit cap refilling once per 60s against a 30s
        // re-reservation, two refused attempts exhausted the bound and the debt
        // was dropped at t=90 having never been served once. Attempts are
        // bounded separately, by `attempts` below and by the tokio backstop
        // inside the wait.
        let mut windows_served: u32 = match start {
            FollowupStart::Reserved { .. } => 1,
            FollowupStart::OwedOnly { .. } => 0,
        };
        // Total loop passes, bounding a chain that only ever gets refused.
        let mut attempts: u32 = 0;

        loop {
            if deliver {
                if dispatch_pending {
                    // CONCURRENT, not sequential. `dispatch_resync_request`
                    // blocks on `notify_node_event`, which is a `send().await`
                    // under NOTIFICATION_SEND_TIMEOUT (30s) — the whole window.
                    // Run sequentially, the re-delivery below would start with
                    // its deadline already gone and retry nothing. This is the
                    // same hazard, and the same fix, as the caller spawning
                    // before its own blocking dispatch (#4862 P2).
                    tokio::join!(
                        dispatch_resync_request(&op_mgr, key, sender_addr, incoming_tx),
                        resend_dropped_broadcast_resync_request(
                            &op_mgr,
                            key,
                            sender_addr,
                            incoming_tx,
                            deadline,
                            correlated,
                        ),
                    );
                } else {
                    resend_dropped_broadcast_resync_request(
                        &op_mgr,
                        key,
                        sender_addr,
                        incoming_tx,
                        deadline,
                        correlated,
                    )
                    .await;
                }
            }

            // #5510. The re-delivery can return EARLY (its #4863 gate stops it
            // once the first response lands); this waits out the rest of the
            // window regardless, because a drop swallowed AFTER that response is
            // exactly the case this exists for.
            if !wait_until_reservation_close(&op_mgr, key, incoming_tx, deadline).await {
                // The backstop fired or the wait was cancelled. If a repair was
                // owed, it dies here — name it (review round 3), because a
                // silent exit makes an unrepaired drop indistinguishable from a
                // chain that simply had nothing to do.
                if owed {
                    tracing::info!(
                        tx = %incoming_tx,
                        contract = %key,
                        target = %sender_addr,
                        event = "coalesced_resync_abandoned_at_backstop",
                        "UPDATE relay: dropped-broadcast repair abandoned with a \
                         debt still owed; that drop now waits for the next drop \
                         on this pair or for anti-entropy (#5510)"
                    );
                }
                return;
            }
            attempts += 1;
            if !owed {
                // `Unknown` (the reservation was evicted from the bounded LRU
                // mid-window) counts as owed — see `ResyncDebt`. The chain
                // cannot distinguish "nothing was swallowed" from "the record
                // of what was swallowed is gone", and only one of those is safe
                // to act on by exiting.
                owed = op_mgr
                    .interest_manager
                    .take_resync_drop_during_window(&key, sender_addr, deadline)
                    .is_owed();
            }
            if !owed {
                // Nothing was swallowed by this window. Usually that means the
                // chain is done — but not if the request it emitted was never
                // ANSWERED (review round 3, X2).
                //
                // The responder runs its own 30s `resync_response_limiter` per
                // (contract, peer). If the initial request was delayed by the
                // blocking `notify_node_event` and landed late, the coalesced
                // request and its short retries can all arrive inside that one
                // responder window and be rejected before the responder clears
                // its cached summary of us. Nothing is swallowed here, because
                // nothing new was dropped — and yet the divergence this chain
                // exists to repair is still there.
                //
                // Our correlation record is consumed when a matching
                // ResyncResponse arrives, so a record still outstanding means no
                // response has landed. Keep going in that case, so the next
                // served window retries after the responder's window has passed.
                // Bounded exactly as before by `windows_served` and `attempts`,
                // so this cannot become a spin.
                let unanswered = correlated
                    && op_mgr
                        .ring
                        .outstanding_resync_requests
                        .is_outstanding(*key.id(), sender_addr);
                if !unanswered {
                    return;
                }
                owed = true;
                tracing::debug!(
                    tx = %incoming_tx,
                    contract = %key,
                    target = %sender_addr,
                    event = "coalesced_resync_unanswered_retry",
                    "UPDATE relay: the emitted repair has had no ResyncResponse, \
                     so the responder's own throttle may have rejected it; \
                     retrying in the next window (#5510)"
                );
            }
            if windows_served >= MAX_COALESCED_RESYNC_WINDOWS
                || attempts >= MAX_COALESCED_RESYNC_ATTEMPTS
            {
                // The debt was CONSUMED into `owed` a few lines above, so
                // returning here would abandon it silently (review round 3, X4):
                // a drop swallowed late in the last served window would vanish
                // at t=90 with nothing left holding the flag. Hand it back to
                // the reservation instead, so the next drop on this pair — or a
                // chain that later owns this window — still serves it.
                //
                // Handing back rather than emitting one more time keeps the
                // bound meaning what it says. Emitting here would make the
                // chain serve MAX+1 windows, and the bound exists to cap slot
                // occupancy, not to cap repairs.
                op_mgr
                    .interest_manager
                    .note_resync_debt_on_held_reservation(&key, sender_addr);
                tracing::info!(
                    tx = %incoming_tx,
                    contract = %key,
                    target = %sender_addr,
                    windows_served,
                    attempts,
                    event = "coalesced_resync_chain_bound",
                    "UPDATE relay: dropped-broadcast repair chain hit its bound \
                     and is releasing its slot with a debt still owed; the debt \
                     is left on the reservation, so a pair still dropping this \
                     steadily repairs from its next reservation (#5510)"
                );
                return;
            }

            // Re-check the hosting gate before EACH coalesced emit (review
            // round 2). A chain can live for tens of seconds, and the contract
            // may have been evicted underneath it; without this the chain would
            // keep asking a peer to full-state us a contract we no longer hold.
            // Cheap: `should_summarize_or_broadcast` is in-memory plus a redb
            // point lookup, and it short-circuits for an unheld key.
            //
            // Applied to EVERY coalesced emit, whatever the origin (review
            // round 3, correcting round 2). The "#4857 has never been
            // hosting-gated" argument is true of the IMMEDIATE queue-full emit
            // and does not extend to this chain, which is NEW code: it can
            // re-emit across up to MAX_COALESCED_RESYNC_WINDOWS served windows,
            // roughly 180s, for a contract that may have been evicted in the
            // meantime. Carrying a pre-existing exemption into new code that
            // spans three minutes is not preserving behaviour, it is inventing
            // a new one. The immediate queue-full emit stays ungated.
            if !op_mgr.ring.should_summarize_or_broadcast(&key) {
                tracing::debug!(
                    tx = %incoming_tx,
                    contract = %key,
                    target = %sender_addr,
                    event = "coalesced_resync_no_longer_held",
                    "UPDATE relay: contract no longer held; abandoning the \
                     trailing coalesced repair (#5510)"
                );
                return;
            }

            match reserve_resync_emit(
                &op_mgr,
                key,
                sender_addr,
                incoming_tx,
                DroppedBroadcastReason::DroppedDuringThrottleWindow,
                // TRUE, and the reason is subtle. This chain holds a debt in
                // `owed`. If its re-reservation is refused because a SIBLING
                // now owns the window, that debt must move to the sibling's
                // reservation or it dies with this chain — and the only
                // TOCTOU-free moment to move it is inside that same locked
                // check. On the granted path nothing is recorded (there is no
                // held window to record against), so this cannot double-count:
                // the flag is set only on the refusal this chain exits on.
                true,
            )
            .await
            {
                Ok((next_deadline, next_correlated)) => {
                    owed = false;
                    deadline = next_deadline;
                    correlated = next_correlated;
                    dispatch_pending = true;
                    deliver = true;
                    windows_served += 1;
                }
                Err(ReserveRefusal::WindowHeld) => {
                    // A SIBLING took the reservation for this pair. Its chain
                    // now owns the window and will serve the debt; continuing
                    // would mean retrying against a reservation this chain does
                    // not hold, burning its slot to do nothing.
                    //
                    // The hand-over happens INSIDE the call above, because this
                    // arm passes `note_debt: true` — that is the whole reason
                    // the chain's retry sets it. An earlier version passed
                    // `false` and re-noted here on a second lock acquisition,
                    // which made `note_debt` unobservable (both values behaved
                    // identically) and reopened the TOCTOU hole the atomic
                    // method exists to close. Do not re-add a note here.
                    tracing::debug!(
                        tx = %incoming_tx,
                        contract = %key,
                        target = %sender_addr,
                        event = "coalesced_resync_handed_to_sibling",
                        "UPDATE relay: another reservation now holds this pair's \
                         window; handing the debt to it and releasing this slot \
                         (#5510)"
                    );
                    return;
                }
                Err(ReserveRefusal::GlobalCap { deadline: held }) => {
                    // The global per-contract cap refused; it refills 1/60s
                    // against a 1/30s re-reservation, so this is the common case
                    // rather than a corner. KEEP `owed` and wait one more
                    // interval — the cap's `cancel` released the window, so the
                    // next attempt can take a fresh one. Do NOT re-deliver in
                    // the meantime (nothing new is authorized), and do NOT count
                    // this as a served window.
                    deadline = held;
                    dispatch_pending = false;
                    deliver = false;
                }
            }
        }
    });
}

/// Wait out the rest of a reservation window (#5510).
///
/// Returns `false` only when the tokio liveness backstop fired, i.e. a decoupled
/// injected clock froze before its deadline and the caller should give up.
///
/// Polls the throttle's OWN clock — the injected `hosting_time_source_override`
/// in sims, the wall clock in prod — so sim pacing stays deterministic, at
/// [`COALESCED_RESYNC_POLL_INTERVAL`] rather than the #4862 retry's finer step
/// (see that constant for the wakeup arithmetic). The backstop is measured from
/// the start of THIS wait, so under a frozen injected clock one chain iteration
/// can burn up to two reservation intervals of tokio time rather than one. That
/// is immaterial — the bound exists only to stop an unbounded spin — and is
/// stated rather than left to be discovered.
///
/// Deliberately does NOT take the swallowed-drop flag. The caller takes it,
/// because it must survive a gate refusal that happens after the window closes
/// (review finding 16); consuming it here would lose the repair in exactly the
/// case the chain exists to cover.
async fn wait_until_reservation_close(
    op_manager: &OpManager,
    key: ContractKey,
    incoming_tx: Transaction,
    reservation_deadline: Instant,
) -> bool {
    let started = tokio::time::Instant::now();
    while op_manager.interest_manager.now() < reservation_deadline {
        if started.elapsed() >= RESYNC_REQUEST_MIN_INTERVAL {
            tracing::debug!(
                tx = %incoming_tx,
                contract = %key,
                event = "coalesced_resync_backstop",
                "UPDATE relay: tokio liveness backstop elapsed (injected clock \
                 stalled), abandoning the trailing coalesced ResyncRequest (#5510)"
            );
            return false;
        }
        tokio::time::sleep(COALESCED_RESYNC_POLL_INTERVAL).await;
    }
    true
}

/// Enqueue one `ResyncRequest` to `sender_addr`.
///
/// Blocking best-effort: the path is lossy (`notify_node_event` -> event loop ->
/// `P2pBridge::try_send`), which is what the #4862 re-delivery exists to cover.
async fn dispatch_resync_request(
    op_manager: &OpManager,
    key: ContractKey,
    sender_addr: SocketAddr,
    incoming_tx: Transaction,
) {
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
            "UPDATE relay: failed to send dropped-broadcast ResyncRequest"
        );
    }
}

/// Maximum ADDITIONAL trailing re-dispatch attempts for a queue-full
/// `ResyncRequest` after the immediate one (#4857 P2). Kept small so the whole
/// retry burst for a single drop stays far below the #4251 storm: one
/// reservation (see [`crate::ring::InterestManager::begin_resync_request`])
/// dispatches at most `1 + QUEUE_FULL_RESYNC_MAX_RETRIES` ResyncRequests per
/// (contract, peer) per `RESYNC_REQUEST_MIN_INTERVAL` (30s).
const QUEUE_FULL_RESYNC_MAX_RETRIES: u32 = 2;

/// Base inter-attempt delay for queue-full `ResyncRequest` retries (#4857 P2).
///
/// Linear backoff: the `attempt`-th retry waits ~`attempt * BASE` (±20%
/// jitter), MEASURED ON THE THROTTLE'S CLOCK (see `resend_dropped_broadcast_resync_request`).
/// The worst-case total span (`sum_{n=1..=MAX} n * BASE * 1.2` = 3 · 2s · 1.2 =
/// 7.2s for the current constants) stays comfortably under
/// `RESYNC_REQUEST_MIN_INTERVAL` (30s); the retry is ALSO hard-stopped at the
/// reservation deadline regardless of span, so it can never spill into the next
/// reservation. Pinned by `resync_retry_span_stays_within_throttle_window`.
const QUEUE_FULL_RESYNC_RETRY_BASE_DELAY: Duration = Duration::from_secs(2);

/// Poll granularity for the injected-clock retry wait (#4857 P2).
///
/// The util `TimeSource` the throttle uses exposes only `now()` — no sleep — so
/// `resend_dropped_broadcast_resync_request` waits by polling `interest_manager.now()`
/// in bounded `tokio` increments until the injected clock reaches the attempt's
/// target (or the reservation deadline). Small enough that a retry fires
/// promptly once the clock reaches its target; large enough that wakeups stay
/// negligible (a ~2s delay ⇒ ~20 polls, and the burst is throttle-bounded to
/// one per (contract, peer) per 30s). Under an injected mock clock the wait
/// advances only as the sim advances that clock, and `tokio` auto-advance
/// interleaves this poll timer with the sim's own timers (bounded wakeups).
const QUEUE_FULL_RESYNC_RETRY_POLL_INTERVAL: Duration = Duration::from_millis(100);

/// Jittered delay before the `attempt`-th queue-full `ResyncRequest` retry
/// (#4857 P2).
///
/// Linear backoff (`attempt * BASE`) with ±20% jitter via [`GlobalRng`]
/// (deterministic under the simulation harness), so a load spike that drops
/// broadcasts to MANY (contract, peer) pairs at once does not produce a
/// synchronised retry wave onto the just-recovering bridge. Pure fn so the
/// bounds are unit-testable without a runtime.
fn jittered_resync_retry_delay(attempt: u32) -> Duration {
    // Sample in [0.8, 1.2): at least the ±20% the retry/backoff rule mandates.
    let factor: f64 = GlobalRng::random_range(0.8..1.2);
    QUEUE_FULL_RESYNC_RETRY_BASE_DELAY.mul_f64(attempt as f64 * factor)
}

/// Trailing best-effort re-dispatch of a dropped-broadcast `ResyncRequest`
/// (#4857 P2).
///
/// Runs as a short-lived fire-and-forget task spawned by
/// [`send_dropped_broadcast_resync_request`] AFTER its immediate ResyncRequest,
/// and ONLY when the throttle reservation was granted. It serves whichever drop
/// caused that emit; the `queue_full_resync_retry_*` log events keep their
/// pre-#5510 names (they are `debug!`, so they never reached a release build
/// and no operator grep depends on them) rather than churning a mechanism this
/// change does not alter. It re-dispatches the same
/// ResyncRequest up to [`QUEUE_FULL_RESYNC_MAX_RETRIES`] times with jittered
/// backoff so a resync dropped by the lossy event-loop → `P2pBridge::try_send`
/// path still heals the divergence before the ~5-min heartbeat.
///
/// Anchored to the reservation window (#4857 P2, review P2-A/P2-B):
/// `reservation_deadline` is the instant the throttle's window closes, on the
/// SAME clock the throttle stamped it with (`interest_manager.now()` — the
/// injected `hosting_time_source_override` in sims, the wall clock in prod).
/// Both the inter-attempt delay and the stop condition are driven by that
/// clock: each attempt waits until the injected clock reaches its jittered
/// target, and the WHOLE task bails the moment the injected clock reaches
/// `reservation_deadline`. So a burst can never cross into a new reservation —
/// even if the caller's blocking `notify_node_event(...).await` consumed most
/// of the window before this task was spawned — and sims that advance the
/// injected clock see deterministic pacing. The util `TimeSource` has no sleep
/// primitive, so the wait polls `now()` in bounded `tokio` increments
/// ([`QUEUE_FULL_RESYNC_RETRY_POLL_INTERVAL`]).
///
/// DST liveness backstop: the injected deadline is PRIMARY, but a decoupled
/// injected clock (`hosting_time_source_override`) that FREEZES before its
/// deadline would leave the injected gates never firing and this poll spinning
/// forever. So the task is ALSO floored at one reservation interval of tokio
/// time (`RESYNC_REQUEST_MIN_INTERVAL` from `started`). In production the two
/// clocks coincide and the injected stop fires first, so the floor is a no-op;
/// it only takes effect when the injected clock stalls.
///
/// Conditional (#4863): each attempt re-dispatches ONLY while the caller's
/// request is still outstanding in the correlation map. The `ResyncResponse`
/// receive arm consumes that entry before applying, so its absence means the
/// response arrived and was AUTHORIZED (not necessarily applied — the consume
/// precedes the apply, which can still fail). Re-dispatching then would make the
/// sender clear its summary and ship FULL STATE again onto a receiver whose
/// queue was just full (up to [`QUEUE_FULL_RESYNC_MAX_RETRIES`] redundant
/// full-state responses per drop). The check is READ-ONLY (`is_outstanding`,
/// never `consume`): consuming the entry here would make the genuine response
/// look unsolicited and be dropped without applying.
///
/// The gate applies ONLY when `correlated` — see the caller. An uncorrelated
/// request (strict-cap rejection) reads `is_outstanding == false` forever, so
/// gating on it unconditionally would disable this retry node-wide whenever the
/// map is full.
///
/// Scale note: against a peer running >= v0.2.102 the responder's own
/// `resync_response_limiter` already caps it at ONE full-state reply per
/// (peer, contract) per 30s, and the whole retry burst lives inside one such
/// window — so there the redundant responses were already suppressed at the
/// source. The amplification this gate removes is reachable against peers older
/// than that, i.e. it shrinks as the fleet upgrades. The gate is still worth
/// having: it also stops the redundant REQUESTS, and it is the only bound that
/// does not depend on the remote peer's version.
///
/// Storm bound (#4251 / #4864 cap): this fn RE-DELIVERS the one already-authorized
/// emit — it deliberately does NOT call `begin_resync_request` (no new
/// reservation), does NOT consume a `resync_emit_limiter` token (no new global
/// emit), and does NOT re-record the correlation entry (the caller's one record
/// authorizes the eventual response once; a redundant response is dropped by
/// #4864 before WASM). All its sends belong to the single reservation already
/// granted by the caller. Combined with the deadline stop, steady-state stays
/// capped at one reservation (≤ `1 + MAX` sends) per (contract, peer) per window.
///
/// Uses the NON-blocking `try_notify_node_event` (bounded-channel rule): a
/// dropped retry is covered by the next attempt or the heartbeat backstop, and
/// blocking would keep the task alive up to `NOTIFICATION_SEND_TIMEOUT` (30s).
///
/// do NOT add a `begin_resync_request`/`resync_emit_limiter` call here, do NOT
/// switch to a blocking `notify_node_event`, and do NOT drive the delay/deadline
/// off a clock other than `interest_manager.now()` — see #4251/#4857/#4864.
async fn resend_dropped_broadcast_resync_request(
    op_manager: &OpManager,
    key: ContractKey,
    sender_addr: SocketAddr,
    incoming_tx: Transaction,
    reservation_deadline: Instant,
    correlated: bool,
) {
    // Tokio-clock liveness backstop (DST safety). The injected `reservation_deadline`
    // below is the PRIMARY stop, but if the injected clock is a DECOUPLED
    // `hosting_time_source_override` that FREEZES before its deadline, the
    // injected gates would never fire and this poll would spin forever (a
    // `start_paused` test hangs; a real sim leaks a task per drop). tokio's clock
    // always advances (or `start_paused` auto-advances), so cap the whole task at
    // one reservation interval of tokio time as a floor. In production the two
    // clocks coincide and the injected deadline — captured at reservation stamp,
    // strictly before `started` — always fires first, so this floor is a no-op.
    let started = tokio::time::Instant::now();
    for attempt in 1..=QUEUE_FULL_RESYNC_MAX_RETRIES {
        // Target wake time for THIS attempt, on the throttle's own clock, CLAMPED
        // into the remaining reservation window (#4862 P2). A fixed
        // `now + jittered_delay` could already be past `reservation_deadline` when
        // the initial enqueue consumed most of the window — then the deadline
        // check below returns with ZERO retries, exactly failing to heal the
        // backpressure case. Clamping the delay to at most half the remaining
        // window keeps the target strictly inside `[now, reservation_deadline)`,
        // so a retry still fires (and the geometric shrink lets later attempts
        // fit too). No-op in the common full-window case (jittered delay wins).
        let now = op_manager.interest_manager.now();
        let remaining = reservation_deadline.saturating_duration_since(now);
        let delay = jittered_resync_retry_delay(attempt).min(remaining / 2);
        let target = now + delay;

        // Wait until the injected clock reaches `target`, bailing the instant it
        // reaches the reservation deadline (P2-A). Interruptible-sleep note:
        // this is an unmonitored fire-and-forget task (mirrors the step-6
        // proactive-summary spawn) holding no locks or channel receivers; each
        // poll sleep is cancel-safe, and the whole task is hard-bounded by BOTH
        // the injected reservation deadline and the tokio backstop, so a runtime
        // shutdown drops it cleanly.
        loop {
            let now = op_manager.interest_manager.now();
            // PRIMARY stop: injected reservation window closed (P2-A/P2-B).
            if now >= reservation_deadline {
                tracing::debug!(
                    tx = %incoming_tx,
                    contract = %key,
                    attempt,
                    event = "queue_full_resync_retry_window_elapsed",
                    "UPDATE relay: reservation window elapsed, stopping queue-full \
                     ResyncRequest retries (#4857 P2)"
                );
                return;
            }
            if now >= target {
                break;
            }
            // FLOOR: never depend solely on the injected clock — if it froze
            // before its deadline, bail after one reservation interval of tokio
            // time so the task can't spin. (No-op in prod; injected stop above
            // fires first when the injected clock advances normally.)
            if started.elapsed() >= RESYNC_REQUEST_MIN_INTERVAL {
                tracing::debug!(
                    tx = %incoming_tx,
                    contract = %key,
                    attempt,
                    event = "queue_full_resync_retry_backstop",
                    "UPDATE relay: tokio liveness backstop elapsed (injected clock \
                     stalled), stopping queue-full ResyncRequest retries (#4857 P2)"
                );
                return;
            }
            tokio::time::sleep(QUEUE_FULL_RESYNC_RETRY_POLL_INTERVAL).await;
        }

        // #4863: CONDITIONAL re-dispatch. Every ResyncRequest makes the sender
        // clear its cached summary of us and re-send FULL contract state, so a
        // BLIND retry lands up to MAX redundant full-state ResyncResponses on a
        // receiver whose queue was full moments ago. Re-dispatch only while the
        // request is NOT observed to have landed.
        //
        // The signal is the #4864 round-8 correlation record: the ResyncResponse
        // receive arm (node.rs) require-and-CONSUMES the (contract, sender) entry
        // before applying, so its absence means the request landed, the sender
        // answered, and we AUTHORIZED the answer. It observes authorization, not
        // application — the receive arm consumes before applying and the apply can
        // still fail — so this is not an end-to-end heal confirmation. It is still
        // the right stop signal: a redundant response would be dropped by the
        // consume-once gate anyway, so re-dispatching cannot repair a failed apply.
        //
        // Gated on `correlated` (#4863 review): when the strict cap rejected the
        // record, `is_outstanding` reads false FOREVER, and treating that as
        // "landed" would silently disable this retry node-wide while the map is
        // full. Uncorrelated requests fall back to unconditional re-dispatch,
        // which still heals — the responder clears its cached summary on the
        // ResyncRequest itself (#4857), with no dependence on our record.
        //
        // Read-only on purpose: the retry must NOT consume the entry — that
        // authorization belongs to the receive arm, and consuming it here would
        // make the real response look unsolicited (dropped without applying).
        //
        // Return, don't continue: once the heal has landed every REMAINING
        // attempt is redundant too. NOTE (#5510): returning no longer frees the
        // node-wide slot — the guard now lives in `spawn_resync_followups`, which
        // holds it until the reservation window closes and, if drops keep being
        // swallowed, until the chain's window bound. This return ends the
        // RE-DELIVERY only; the caller then waits out the window. The residual
        // case this cannot cover is a request that landed but whose response is
        // still in flight — bounded by the same `1 + MAX` cap as before, never
        // worse than the blind retry it replaces.
        if correlated
            && !op_manager
                .ring
                .outstanding_resync_requests
                .is_outstanding(*key.id(), sender_addr)
        {
            // info!, not debug!: `release_max_level_info` compiles debug out of
            // release builds, and this is the ONLY field-visible signal of the
            // gate's decision. Both the benefit (fewer redundant full-state
            // resends) and the main risk (a gate that wrongly skips silently
            // loses the #4857 heal) are unfalsifiable without it. Rate is
            // bounded by the per-(contract, peer) reservation, so this cannot
            // become a log storm.
            tracing::info!(
                tx = %incoming_tx,
                contract = %key,
                target = %sender_addr,
                attempt,
                event = "queue_full_resync_retry_skipped_landed",
                "UPDATE relay: dropped-broadcast ResyncRequest already landed \
                 (its ResyncResponse was consumed), skipping the redundant \
                 full-state re-dispatch (#4863)"
            );
            return;
        }

        match op_manager.try_notify_node_event(NodeEvent::SendInterestMessage {
            target: sender_addr,
            message: InterestMessage::ResyncRequest { key },
        }) {
            Ok(()) => tracing::debug!(
                tx = %incoming_tx,
                contract = %key,
                target = %sender_addr,
                attempt,
                event = "queue_full_resync_retry_sent",
                "UPDATE relay: re-dispatched queue-full ResyncRequest (#4857 P2)"
            ),
            Err(e) => tracing::debug!(
                tx = %incoming_tx,
                contract = %key,
                error = %e,
                attempt,
                event = "queue_full_resync_retry_dropped",
                "UPDATE relay: queue-full ResyncRequest retry dropped (heartbeat backstops)"
            ),
        }
    }
}

/// Seed our cache of a broadcast sender's summary from the wire field.
///
/// # Why an EMPTY wire field is never usable evidence
///
/// `sender_summary_bytes` is a bare `Vec<u8>` on the wire, so it cannot
/// express "absent". Empty bytes arrive from **two** different causes and the
/// receiver cannot tell them apart:
///
/// 1. **The sender could not summarize itself.** Every producer builds the
///    field as `our_summary…unwrap_or_default()` (both arms of
///    `broadcast_queue.rs`, plus the sim-path twins in
///    `p2p_protoc/broadcast.rs`), so a `get_contract_summary` that returned
///    `None` (WASM `summarize_state` error, or a contract-handler timeout at
///    `BROADCAST_CH_TIMEOUT` under load) ships empty rather than omitting the
///    field. This is the `PayloadArm::FullNoOurSummary` arm.
/// 2. **The contract's summary genuinely IS empty.** Real contracts in this
///    tree do this: `tests/test-app-1`'s container and deps return
///    `StateSummary::from(vec![])` unconditionally, and `website-contract` /
///    `freenet-ping` return it for empty state.
///
/// Neither is worth caching, for different reasons that happen to agree:
///
/// - In case 1 the bytes say nothing about what the peer holds, so caching
///   them is simply false. `PeerInterest::summary_missing_reason` reports no
///   reason for a *populated* entry, so the cache would look healthy while
///   being wrong, and it would be invisible to the `tracked_missing_*`
///   telemetry built to find exactly this class of staleness.
/// - In case 2 the summary is real but carries zero information, and the
///   fan-out path compares cached summaries **by bytes**
///   (`plan_fanout_send`'s equal-summaries short circuit). Two peers that
///   both summarize to empty would compare equal and skip the broadcast
///   entirely, which is a wrongful skip: matching zero-information summaries
///   prove nothing about convergence. Declining to cache turns that silent
///   skip into a correct full-state send.
///
/// So the rule is "an empty summary is not usable evidence about the peer",
/// which is right under either cause.
///
/// The sender side already takes this care — `record_delivery_to_interest`
/// gates its own write on `if let Some(summary)` — so this closes the
/// receiving half of the same asymmetry.
///
/// # Why skip rather than clear
///
/// Declining to cache must not destroy what we already hold. This function
/// only ever adds information; it never removes any. That is deliberately
/// different from [`SummaryMissingReason::ClearedByNoneReport`], which belongs
/// to a peer that explicitly reported `None` in an InterestSync `Summaries`
/// message: there the wire type IS `Option<Vec<u8>>`, so the `None` is a real
/// claim about that peer and clearing is the right response.
///
/// For the same reason the empty path still refreshes an existing entry's TTL.
/// A broadcast did arrive from this peer, which is liveness evidence even when
/// the summary is not; without the refresh, an entry kept alive solely by
/// empty-summary broadcasts would age out at `INTEREST_TTL`.
///
/// # Sibling sites
///
/// `node.rs`'s `ResyncResponse` handler also stores a possibly-empty summary,
/// and that one is CORRECT as-is: its producer bails when it cannot summarize,
/// so empty there is unambiguously a genuine empty summary rather than a
/// failure. Do not "fix" it to match this function.
///
/// Returns whether a summary was cached.
fn seed_sender_summary_from_broadcast(
    op_manager: &OpManager,
    key: &ContractKey,
    sender_summary_bytes: &[u8],
    sender_addr: SocketAddr,
) -> bool {
    let Some(sender_pkl) = op_manager
        .ring
        .connection_manager
        .get_peer_by_addr(sender_addr)
    else {
        return false;
    };
    let sender_key = crate::ring::PeerKey::from(sender_pkl.pub_key().clone());

    if sender_summary_bytes.is_empty() {
        // Liveness without information: refresh an EXISTING entry's TTL, but
        // never create one and never overwrite a good cached summary.
        op_manager
            .interest_manager
            .refresh_peer_interest(key, &sender_key);
        tracing::debug!(
            contract = %key,
            peer = %sender_addr,
            "Broadcast carried an empty sender summary; refreshed the interest \
             TTL but left the cached summary untouched"
        );
        return false;
    }

    // #4952: upsert — the sender self-reported this summary, and the fan-out
    // sender population is by definition co-hosts we often don't
    // interest-track. Seeding here lets OUR next broadcast to them be a delta
    // without waiting for a delivered send.
    op_manager.interest_manager.upsert_peer_summary_from(
        key,
        &sender_key,
        StateSummary::from(sender_summary_bytes.to_vec()),
        crate::ring::interest::SummaryPopulationSource::InboundBroadcast,
    ) != crate::ring::interest::SummaryPopulationOutcome::RejectedAtCap
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
#[allow(clippy::too_many_arguments)]
async fn drive_relay_broadcast_to(
    op_manager: &OpManager,
    incoming_tx: Transaction,
    key: ContractKey,
    payload: DeltaOrFullState,
    sender_summary_bytes: Vec<u8>,
    sender_addr: SocketAddr,
    covered: crate::ring::broadcast_coverage::CoveredPeers,
) -> Result<(), OpError> {
    let self_location = op_manager.ring.connection_manager.own_location();

    // ── Step 1: update sender's cached summary ────────────────────────────
    // Empty bytes mean the sender could not summarize itself, NOT that its
    // summary is empty — see `seed_sender_summary_from_broadcast`.
    seed_sender_summary_from_broadcast(op_manager, &key, &sender_summary_bytes, sender_addr);

    let (update_data, payload_bytes) = match &payload {
        DeltaOrFullState::Delta(bytes) => {
            tracing::debug!(
                contract = %key,
                delta_size = bytes.len(),
                "UPDATE relay: received delta broadcast"
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
                "UPDATE relay: received full state broadcast"
            );
            (UpdateData::State(State::from(bytes.clone())), bytes.clone())
        }
    };
    let is_delta = matches!(payload, DeltaOrFullState::Delta(_));
    let mut receiver_terminal = op_manager
        .payload_mix
        .receiver_terminal_guard(is_delta, payload_bytes.len());
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
        receiver_terminal.mark_dedup();
        tracing::debug!(
            tx = %incoming_tx,
            %key,
            "UPDATE relay: BroadcastTo skipped — duplicate payload (dedup hit)"
        );
        return Ok(());
    }

    // ── Step 3b: merge-failure backoff gate (#4861) ───────────────────────
    //
    // Quarantine "poison" contracts whose merges reliably fail/time out. While a
    // contract is in cooldown (or this exact payload is known-failed) skip the
    // WASM merge entirely and — critically — do NOT emit the ResyncRequest
    // amplification or clear the sender's cached summary. Complete exactly like
    // the dedup-skip path above. Cleared the instant a merge succeeds (below).
    //
    // The payload hash is computed EAGERLY even on the common Allow path (#4864
    // review declined lazy-hash suggestion): a single ahash of the payload bytes
    // is negligible next to the WASM merge it gates, and the hash is reused
    // verbatim by `record_failure` in the error arm, so lazy computation would
    // only duplicate the work on the failure path.
    let payload_hash = crate::ring::merge_backoff::merge_payload_hash(is_delta, &payload_bytes);
    match op_manager
        .ring
        .merge_backoff
        .check(key.id(), sender_addr, payload_hash)
    {
        crate::ring::merge_backoff::MergeDecision::Allow => {}
        decision @ (crate::ring::merge_backoff::MergeDecision::InBackoff
        | crate::ring::merge_backoff::MergeDecision::KnownFailedPayload) => {
            receiver_terminal.mark_backoff();
            crate::config::GlobalTestMetrics::record_merge_suppressed_by_backoff();
            tracing::debug!(
                tx = %incoming_tx,
                %key,
                ?decision,
                "UPDATE relay: BroadcastTo merge skipped — contract in merge-failure backoff"
            );
            return Ok(());
        }
    }

    // ── Step 4: apply broadcast via WASM merge ────────────────────────────
    let update_result = super::update_contract(
        op_manager,
        key,
        update_data,
        RelatedContracts::default(),
        crate::contract::Priority::NetworkRelay,
        crate::node::ApplyOrigin::NetworkRelay,
        resolve_covered_peers(op_manager, &key, &incoming_tx, sender_addr, &covered),
    )
    .await;

    let UpdateExecution {
        value: updated_value,
        changed,
    } = match update_result {
        Ok(result) => {
            // Reset the merge-failure backoff ONLY on a successful DELTA merge
            // (#4861): a delta that applies cleanly proves the incoming change
            // was compatible with local state — genuine convergence. A
            // successful FULL-STATE apply just replaces state and "succeeds"
            // regardless of which fork it carries, so it proves nothing about
            // convergence (the semantic fork-oscillation poison class flips
            // forks on every full-state apply). Full-state broadcasts therefore
            // do NOT reset here; the resync-apply path in node.rs does not reset
            // either (see the note there).
            if is_delta {
                // Per-sender Invalid channel clears unconditionally on a clean
                // delta apply (channel-trust); the contract-wide Timeout + memo
                // clear only when the state ADVANCED (result.changed) — a no-op
                // delta must not wipe a live Timeout quarantine (#4864 round-4).
                op_manager.ring.merge_backoff.record_success_from_sender(
                    key.id(),
                    sender_addr,
                    result.changed,
                );
                // Delta-incompat memo self-heal (HQk7 resync loop): a delta
                // that applied cleanly proves the contract takes deltas, so
                // drop the memo and its failure counter. Deliberately NOT
                // gated on `result.changed` — a no-op delta apply still ran
                // the contract's delta path successfully, which is the exact
                // property the memo tracks. See `crate::ring::delta_incompat`.
                op_manager
                    .ring
                    .delta_incompat
                    .record_delta_success(key.id());
            } else if result.changed {
                // #4864 round-9 item 3: a CHANGED FULL-STATE broadcast apply
                // advances the state, which invalidates the failed-payload MEMO's
                // premise (a delta that failed against the OLD state may be valid
                // against the NEW one). Mirror the streaming Ok arm and the
                // resync-apply path in node.rs: clear ONLY the memo. This is NOT a
                // backoff reset — no cooldown channel is touched — so the fork-flip
                // no-reset doctrine still holds (a full-state merge carries the same
                // fork ambiguity). Gated on result.changed so a no-op full-state
                // apply does not needlessly re-admit known-bad payloads. Without
                // this, a payload that failed against old state stayed
                // KnownFailedPayload for up to FAILED_PAYLOAD_TTL (10min) after the
                // full state advanced it — the non-streaming relay's omission.
                op_manager
                    .ring
                    .merge_backoff
                    .invalidate_payload_memo(key.id());
            }
            result
        }
        Err(err) => {
            // Record the merge failure for the per-contract backoff (#4861),
            // but ONLY for genuine contract-exec failures (the merge ran and the
            // contract rejected it or it timed out). `is_contract_exec_rejection`
            // excludes queue-full (transient load) and missing-contract failures
            // (healed by the auto-fetch below) — backing either off would be
            // wrong. A timeout gets the longer Timeout-class cooldown.
            //
            // #4864 round-6: a SCHEDULER timeout (guest never ran, blocking-pool
            // saturation) IS an exec rejection by cause-prefix, but the guest
            // never executed the delta, so it must NOT be quarantined — exclude
            // it here exactly like queue-full.
            if err.is_contract_exec_rejection() && !err.is_scheduler_timeout() {
                let class = if err.is_wasm_timeout() {
                    crate::ring::merge_backoff::MergeFailureClass::Timeout
                } else {
                    crate::ring::merge_backoff::MergeFailureClass::Invalid
                };
                op_manager.ring.merge_backoff.record_failure(
                    key.id(),
                    sender_addr,
                    class,
                    payload_hash,
                );
                // Delta-incompatibility signal (HQk7 resync loop): an
                // `Invalid`-class DELTA rejection is the receiver-side form of
                // "this contract can't take deltas". We fan the same contract
                // out to our own downstreams, so count it toward the
                // sender-side memo that switches our broadcasts to full state.
                // Timeout-class failures are load, not incompatibility — and
                // full-state merges say nothing about deltas — so both are
                // excluded. See `crate::ring::delta_incompat`.
                if is_delta && class == crate::ring::merge_backoff::MergeFailureClass::Invalid {
                    op_manager
                        .ring
                        .delta_incompat
                        .note_delta_apply_failed(*key.id());
                }
            }

            // On benign stale-version rejection (not OOG/traps/validation
            // failures — see `is_invalid_update_rejection`), nudge the sender's
            // peer-summary cache of us toward Some(our_summary). Helper gates
            // internally on summary equality + per-contract throttle. Mirrors
            // the matching sites in legacy `update.rs`.
            if err.is_invalid_update_rejection() {
                let op_mgr = op_manager.clone();
                let contract_key = key;
                let sender_summary = sender_summary_bytes.clone();
                tokio::spawn(async move {
                    super::send_summary_back_on_rejection(
                        &op_mgr,
                        &contract_key,
                        sender_addr,
                        sender_summary,
                    )
                    .await;
                });
            }

            // Issue #4251: on queue-full the merge never ran, so neither
            // amplification branch below is correct — ResyncRequest asks the
            // sender to resend full state onto the same saturated queue, and
            // auto-fetch enqueues a GET right back onto it. Skip both; still
            // surface the error to the caller for telemetry.
            //
            // #4864 round-6: a scheduler timeout (guest never ran, blocking-pool
            // saturation) is the SAME transient/load class — the delta was never
            // applied and we still hold the contract — so it takes the identical
            // heal path (resync, no auto-fetch). Keep the name `queue_full` so
            // the downstream branch logic is untouched.
            let queue_full = err.is_contract_queue_full() || err.is_scheduler_timeout();

            if is_delta && !queue_full {
                // #5510: count THIS cause at the branch that DECIDES it, which
                // is here — above the emit cap, not inside it.
                //
                // `GlobalTestMetrics::resync_requests()` is the total across
                // every cause, and since this PR there are several, so a test
                // asserting "no delta failed" cannot use the total without
                // mistaking a rate-limited broadcast repair for the #2763
                // summary-caching regression. But the counter must describe the
                // FAILURE, not the emission: `simulation_integration.rs` reads
                // it as "no delta failed to apply", and a delta that failed
                // while the global cap happened to be exhausted failed just the
                // same. Counting inside the `if` would have made the assertion
                // quietly weaker exactly when the node is busy enough for the
                // cap to bind — which is when a real regression is most likely.
                // Same rule as `.claude/rules/bug-prevention-patterns.md`: the
                // metric is produced by the code that makes the decision.
                crate::config::GlobalTestMetrics::record_delta_failure_resync();

                // Delta application failed → send ResyncRequest. Mirrors
                // update.rs:710-758. Rate-limited per-contract (#4861): a poison
                // contract that fails deltas from many senders must not drive a
                // full-state resync storm. When the emit is suppressed, skip the
                // sender summary-clear too — it is part of the resync handshake,
                // so clearing it without emitting would just force the sender to
                // full-state us on the next interest cycle.
                if op_manager
                    .ring
                    .resync_emit_limiter
                    .check_and_record(*key.id())
                {
                    tracing::warn!(
                        tx = %incoming_tx,
                        contract = %key,
                        sender = %sender_addr,
                        error = %err,
                        event = "delta_apply_failed",
                        "UPDATE relay: delta apply failed, sending ResyncRequest"
                    );

                    if let Some(sender_pkl) = op_manager
                        .ring
                        .connection_manager
                        .get_peer_by_addr(sender_addr)
                    {
                        let sender_key = crate::ring::PeerKey::from(sender_pkl.pub_key().clone());
                        op_manager.interest_manager.clear_peer_summary(
                            &key,
                            &sender_key,
                            crate::ring::interest::SummaryMissingReason::ClearedByDeltaApplyFailure,
                        );
                    }

                    tracing::info!(
                        tx = %incoming_tx,
                        contract = %key,
                        target = %sender_addr,
                        event = "resync_request_sent",
                        "UPDATE relay: sending ResyncRequest after delta failure"
                    );
                    // #4864 round-8: record the outstanding request so only the
                    // matching ResyncResponse from this peer is authorized (once)
                    // at the receive arm; unsolicited/replayed responses are
                    // dropped without running WASM.
                    //
                    // The correlated/uncorrelated distinction only matters for the
                    // queue-full retry's gate (#4863); this path has no retry, so
                    // a strict-cap rejection just means the eventual response is
                    // dropped as unsolicited and anti-entropy heals instead.
                    let _correlated = op_manager
                        .ring
                        .outstanding_resync_requests
                        .record(*key.id(), sender_addr);
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
                            "UPDATE relay: failed to send ResyncRequest"
                        );
                    }
                } else {
                    crate::config::GlobalTestMetrics::record_resync_request_suppressed();
                    tracing::debug!(
                        tx = %incoming_tx,
                        contract = %key,
                        target = %sender_addr,
                        event = "resync_request_suppressed",
                        "UPDATE relay: ResyncRequest suppressed by per-contract rate limit"
                    );
                }
            } else if !is_delta && !err.is_contract_exec_rejection() && !queue_full {
                // Full state failed and the merge function did NOT reject it
                // (so contract code is missing). Trigger self-healing GET.
                // Inbound relay path → gated on `contract_in_use` (#4473).
                op_manager.try_auto_fetch_contract(
                    &key,
                    sender_addr,
                    AutoFetchReason::InboundRelay,
                );
            } else if queue_full {
                // Issue #4857: a queue-full drop is SILENT — the sender cached
                // our summary on send-Ok and will never re-send the dropped
                // change. Emit a rate-limited ResyncRequest so it re-syncs. The
                // auto-fetch branch stays suppressed on queue-full (we already
                // hold the contract). Shared with the streaming driver.
                send_dropped_broadcast_resync_request(
                    op_manager,
                    key,
                    sender_addr,
                    incoming_tx,
                    DroppedBroadcastReason::QueueFull,
                )
                .await;
            }
            return Err(err);
        }
    };

    tracing::debug!(
        tx = %incoming_tx,
        %key,
        "UPDATE relay: BroadcastTo applied"
    );

    // ── Step 5: telemetry — broadcast applied ─────────────────────────────
    receiver_terminal.mark_applied(changed, updated_value.len());
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
            "UPDATE relay: BroadcastTo produced no change, ending propagation"
        );
        return Ok(());
    }

    // Dashboard `updates_received` counter (issue #4828). Mirrors the
    // streaming twin `drive_relay_broadcast_to_streaming`, and must stay
    // in both: `broadcast_queue.rs` only picks the streaming variant above
    // `streaming_threshold` (default 64 KB), so this non-streaming path
    // carries every ordinary small delta — the normal River case. Before
    // #4828 only the streaming twin recorded, so an actively-receiving
    // subscriber displayed 0 (`cards.rs` renders `updates_received` as the
    // ONLY UPDATE number for subscriber nodes).
    //
    // Placed after the `!changed` guard, matching the streaming twin: a
    // broadcast that mutated nothing is not a received update.
    crate::node::network_status::record_update_received();

    tracing::debug!(
        "UPDATE relay: contract {} @ {:?} updated via BroadcastTo",
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
    // 100ms-per-contract throttle inside the task. The helper fetches the
    // contract's REAL summary itself (#4923) — no caller-supplied value.
    let op_mgr = op_manager.clone();
    GlobalExecutor::spawn(async move {
        super::send_proactive_summary_notification(&op_mgr, &key, sender_addr).await;
    });

    Ok(())
}

// ── Relay UPDATE streaming drivers ───────────────────────────────────────
//
// Streaming UPDATE relays are qualitatively simpler than streaming PUT
// relays:
//   - No upstream reply to bubble (UPDATE is fire-and-forget e2e).
//   - No downstream forward via `pipe_stream` — network propagation is
//     automatic via `BroadcastStateChange` after local apply.
//   - The driver's job = claim inbound stream → assemble → deserialize
//     payload → `update_contract` → emit telemetry + (BroadcastTo only)
//     dedup + proactive summary.
//
// Shared with slice A:
//   - `active_relay_update_txs` dedup set (tx space is unified).
//   - `RelayUpdateInflightGuard` RAII (same tx cleanup semantics).
//   - `RELAY_UPDATE_DEDUP_REJECTS` counter for dedup observability.
//
// NOT shared (slice-C-specific counters):
//   - `RELAY_UPDATE_STREAMING_INFLIGHT` / `_SPAWNED_TOTAL` /
//     `_COMPLETED_TOTAL` / `_DRIVER_CALL_COUNT` — separate streams for
//     ramp comparison, mirroring PUT slice B rationale.
//
// Fragment-level dedup is handled by `orphan_stream_registry` via
// `claim_or_wait`; tx-level dedup is handled by `active_relay_update_txs`.
// These are complementary — fragment metadata may arrive on a new tx
// while tx dedup catches wire-level retries of the metadata message.
//
// # Why tx dedup is held across `claim_or_wait`'s timeout window
//
// The driver inserts into `active_relay_update_txs` BEFORE calling
// `claim_or_wait`. Worst case (attacker sends a bogus
// `RequestUpdateStreaming` with a fabricated `stream_id` whose
// fragments never arrive), the tx entry is held for up to
// `STREAM_CLAIM_TIMEOUT` (60s) before the RAII guard drops and frees
// it. This is deliberate: releasing the tx entry before
// `claim_or_wait` returns would let a duplicate metadata message
// spawn a parallel driver that would also block on `claim_or_wait` —
// amplifying work and producing duplicate telemetry on eventual
// fragment arrival. The 60s hold is bounded, per-tx (not per-peer),
// and the stream-level dedup inside `orphan_stream_registry` still
// short-circuits real duplicates. Operators monitoring
// `RELAY_UPDATE_STREAMING_INFLIGHT` will see anomalous growth if a
// peer floods bogus stream_ids — that's the intended signal.

/// Spawn a relay driver for a fresh inbound
/// `UpdateMsg::RequestUpdateStreaming`.
///
/// Caller-side gates (in `node.rs`): `source_addr.is_some()`. Per-node
/// dedup against concurrent inbound retries is enforced by
/// `OpManager.active_relay_update_txs` inside this driver.
pub(crate) async fn start_relay_request_update_streaming(
    op_manager: Arc<OpManager>,
    incoming_tx: Transaction,
    key: ContractKey,
    stream_id: StreamId,
    total_size: u64,
    sender_addr: SocketAddr,
) -> Result<(), OpError> {
    #[cfg(any(test, feature = "testing"))]
    RELAY_UPDATE_STREAMING_DRIVER_CALL_COUNT.fetch_add(1, std::sync::atomic::Ordering::SeqCst);

    if !op_manager.active_relay_update_txs.insert(incoming_tx) {
        RELAY_UPDATE_DEDUP_REJECTS.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        tracing::debug!(
            tx = %incoming_tx,
            %key,
            %sender_addr,
            phase = "relay_update_streaming_dedup_reject",
            "UPDATE relay (driver streaming): duplicate RequestUpdateStreaming for in-flight tx, dropping"
        );
        return Ok(());
    }

    // Routing/hosting attribution (Group C): count this as a genuine relayed
    // UPDATE, mirroring the non-streaming `start_relay_request_update`.
    // `relayed_updates_total` counts relayed UPDATE *requests* toward the key;
    // `RequestUpdateStreaming` is the large-payload variant of that request, so
    // it is counted. No loopback exclusion: UPDATE relay drivers run only for
    // `source_addr.is_some()` (an internal/loopback UPDATE is dropped in
    // node.rs and never reaches this driver).
    crate::node::network_status::record_relayed_update();

    tracing::debug!(
        tx = %incoming_tx,
        %key,
        %sender_addr,
        %stream_id,
        total_size,
        phase = "relay_update_streaming_request_start",
        "UPDATE relay (driver streaming): spawning RequestUpdateStreaming driver"
    );

    // Guard pre-spawn (see `start_relay_request_update` rationale).
    RELAY_UPDATE_STREAMING_INFLIGHT.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    RELAY_UPDATE_STREAMING_SPAWNED_TOTAL.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    let guard = RelayUpdateStreamingInflightGuard {
        op_manager: op_manager.clone(),
        incoming_tx,
    };

    GlobalExecutor::spawn(run_relay_request_update_streaming(
        guard,
        op_manager,
        incoming_tx,
        key,
        stream_id,
        total_size,
        sender_addr,
    ));
    Ok(())
}

/// Spawn a relay driver for a fresh inbound
/// `UpdateMsg::BroadcastToStreaming`.
#[allow(clippy::too_many_arguments)]
pub(crate) async fn start_relay_broadcast_to_streaming(
    op_manager: Arc<OpManager>,
    incoming_tx: Transaction,
    key: ContractKey,
    stream_id: StreamId,
    total_size: u64,
    sender_addr: SocketAddr,
    // Peers the sender says it already delivered to (#5147). Empty for the
    // legacy `BroadcastToStreaming`.
    covered: crate::ring::broadcast_coverage::CoveredPeers,
) -> Result<(), OpError> {
    #[cfg(any(test, feature = "testing"))]
    RELAY_UPDATE_STREAMING_DRIVER_CALL_COUNT.fetch_add(1, std::sync::atomic::Ordering::SeqCst);

    if !op_manager.active_relay_update_txs.insert(incoming_tx) {
        RELAY_UPDATE_DEDUP_REJECTS.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        tracing::debug!(
            tx = %incoming_tx,
            %key,
            %sender_addr,
            phase = "relay_update_streaming_dedup_reject",
            "UPDATE relay (driver streaming): duplicate BroadcastToStreaming for in-flight tx, dropping"
        );
        return Ok(());
    }

    // NOT counted in `relayed_updates_total` (see `start_relay_broadcast_to`):
    // BroadcastToStreaming is update-mesh fan-out, not a relayed request.

    tracing::debug!(
        tx = %incoming_tx,
        %key,
        %sender_addr,
        %stream_id,
        total_size,
        phase = "relay_update_streaming_broadcast_start",
        "UPDATE relay (driver streaming): spawning BroadcastToStreaming driver"
    );

    RELAY_UPDATE_STREAMING_INFLIGHT.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    RELAY_UPDATE_STREAMING_SPAWNED_TOTAL.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    let guard = RelayUpdateStreamingInflightGuard {
        op_manager: op_manager.clone(),
        incoming_tx,
    };

    GlobalExecutor::spawn(run_relay_broadcast_to_streaming(
        guard,
        op_manager,
        incoming_tx,
        key,
        stream_id,
        total_size,
        sender_addr,
        covered,
    ));
    Ok(())
}

/// RAII guard for streaming relay UPDATE drivers. Mirrors slice A's
/// `RelayUpdateInflightGuard` but uses the streaming-specific counters.
/// Still cleans up `active_relay_update_txs` (unified tx space).
struct RelayUpdateStreamingInflightGuard {
    op_manager: Arc<OpManager>,
    incoming_tx: Transaction,
}

impl Drop for RelayUpdateStreamingInflightGuard {
    fn drop(&mut self) {
        self.op_manager
            .active_relay_update_txs
            .remove(&self.incoming_tx);
        RELAY_UPDATE_STREAMING_INFLIGHT.fetch_sub(1, std::sync::atomic::Ordering::Relaxed);
        RELAY_UPDATE_STREAMING_COMPLETED_TOTAL.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    }
}

async fn run_relay_request_update_streaming(
    guard: RelayUpdateStreamingInflightGuard,
    op_manager: Arc<OpManager>,
    incoming_tx: Transaction,
    key: ContractKey,
    stream_id: StreamId,
    total_size: u64,
    sender_addr: SocketAddr,
) {
    let _guard = guard;

    if let Err(err) = drive_relay_request_update_streaming(
        &op_manager,
        incoming_tx,
        key,
        stream_id,
        total_size,
        sender_addr,
    )
    .await
    {
        if err.is_contract_queue_full() {
            tracing::debug!(
                tx = %incoming_tx,
                %key,
                error = %err,
                phase = "relay_update_streaming_request_error",
                event = "queue_full",
                "UPDATE relay (driver streaming): RequestUpdateStreaming driver returned error"
            );
        } else {
            tracing::warn!(
                tx = %incoming_tx,
                %key,
                error = %err,
                phase = "relay_update_streaming_request_error",
                "UPDATE relay (driver streaming): RequestUpdateStreaming driver returned error"
            );
        }
    }
}

#[allow(clippy::too_many_arguments)]
async fn run_relay_broadcast_to_streaming(
    guard: RelayUpdateStreamingInflightGuard,
    op_manager: Arc<OpManager>,
    incoming_tx: Transaction,
    key: ContractKey,
    stream_id: StreamId,
    total_size: u64,
    sender_addr: SocketAddr,
    covered: crate::ring::broadcast_coverage::CoveredPeers,
) {
    let _guard = guard;

    if let Err(err) = drive_relay_broadcast_to_streaming(
        &op_manager,
        incoming_tx,
        key,
        stream_id,
        total_size,
        sender_addr,
        covered,
    )
    .await
    {
        if err.is_contract_queue_full() {
            tracing::debug!(
                tx = %incoming_tx,
                %key,
                error = %err,
                phase = "relay_update_streaming_broadcast_error",
                event = "queue_full",
                "UPDATE relay (driver streaming): BroadcastToStreaming driver returned error"
            );
        } else {
            tracing::warn!(
                tx = %incoming_tx,
                %key,
                error = %err,
                phase = "relay_update_streaming_broadcast_error",
                "UPDATE relay (driver streaming): BroadcastToStreaming driver returned error"
            );
        }
    }
}

/// Inner driver for `RequestUpdateStreaming`. Mirrors the legacy
/// handler at `update.rs:871-1071`:
///
/// 1. Claim inbound stream via orphan registry (atomic dedup on
///    `stream_id`).
/// 2. Assemble stream data.
/// 3. Deserialize `UpdateStreamingPayload`.
/// 4. `update_contract` (state update — related contracts preserved).
/// 5. Emit `update_success` telemetry.
///
/// Network propagation is automatic via `BroadcastStateChange`.
async fn drive_relay_request_update_streaming(
    op_manager: &OpManager,
    incoming_tx: Transaction,
    key: ContractKey,
    stream_id: StreamId,
    total_size: u64,
    sender_addr: SocketAddr,
) -> Result<(), OpError> {
    use crate::operations::orphan_streams::{OrphanStreamError, STREAM_CLAIM_TIMEOUT};

    tracing::info!(
        tx = %incoming_tx,
        contract = %key,
        %stream_id,
        total_size,
        "UPDATE relay (driver streaming): processing RequestUpdateStreaming"
    );

    // Step 1: claim stream.
    let stream_handle = match op_manager
        .orphan_stream_registry()
        .claim_or_wait(sender_addr, stream_id, STREAM_CLAIM_TIMEOUT)
        .await
    {
        Ok(handle) => handle,
        Err(OrphanStreamError::AlreadyClaimed) => {
            tracing::debug!(
                tx = %incoming_tx,
                %stream_id,
                "UPDATE relay (driver streaming): RequestUpdateStreaming skipped — stream already claimed (dedup)"
            );
            return Ok(());
        }
        Err(e) => {
            tracing::error!(
                tx = %incoming_tx,
                %stream_id,
                error = %e,
                "UPDATE relay (driver streaming): failed to claim stream from orphan registry"
            );
            return Err(OpError::OrphanStreamClaimFailed);
        }
    };

    // Step 2: assemble stream.
    let stream_data = match stream_handle.assemble().await {
        Ok(data) => {
            tracing::debug!(
                tx = %incoming_tx,
                %stream_id,
                assembled_size = data.len(),
                expected_size = total_size,
                "UPDATE relay (driver streaming): stream assembled"
            );
            data
        }
        Err(e) => {
            tracing::error!(
                tx = %incoming_tx,
                %stream_id,
                error = %e,
                "UPDATE relay (driver streaming): failed to assemble stream"
            );
            return Err(OpError::StreamCancelled);
        }
    };

    // Step 3: deserialize payload.
    let payload: UpdateStreamingPayload = match bincode::deserialize(&stream_data) {
        Ok(p) => p,
        Err(e) => {
            tracing::error!(
                tx = %incoming_tx,
                error = %e,
                "UPDATE relay (driver streaming): failed to deserialize UpdateStreamingPayload"
            );
            return Err(OpError::invalid_transition(incoming_tx));
        }
    };

    let UpdateStreamingPayload {
        related_contracts,
        value,
    } = payload;

    // Step 4: apply update. BroadcastStateChange propagates automatically.
    let UpdateExecution {
        value: updated_value,
        changed,
    } = super::update_contract(
        op_manager,
        key,
        UpdateData::State(State::from(value.clone())),
        related_contracts,
        crate::contract::Priority::NetworkRelay,
        crate::node::ApplyOrigin::NetworkRelay,
        // A streaming `RequestUpdateStreaming` names no delivery targets; see
        // the non-streaming twin.
        crate::ring::broadcast_coverage::BroadcastOrigin::local(),
    )
    .await?;

    // Step 5: telemetry.
    let hash_after = Some(state_hash_full(&updated_value));
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
            None, // No before-hash for streaming (legacy match).
            hash_after,
            Some(updated_value.len()),
        ) {
            op_manager.ring.register_events(Either::Left(event)).await;
        }
    }

    if changed {
        tracing::debug!(
            tx = %incoming_tx,
            %key,
            "UPDATE relay (driver streaming): RequestUpdateStreaming succeeded, state changed"
        );
    } else {
        tracing::debug!(
            tx = %incoming_tx,
            %key,
            "UPDATE relay (driver streaming): RequestUpdateStreaming yielded no state change"
        );
    }

    Ok(())
}

/// Inner driver for `BroadcastToStreaming`. Mirrors the legacy
/// handler at `update.rs:1073-1366`:
///
/// 1. Claim inbound stream.
/// 2. Assemble stream.
/// 3. Deserialize `BroadcastStreamingPayload`.
/// 4. Update sender's cached summary.
/// 5. Dedup check against broadcast_dedup_cache (BEFORE WASM merge).
/// 6. Telemetry: `update_broadcast_received`.
/// 7. `update_contract` (state update).
/// 8. On success: telemetry + proactive summary notification.
/// 9. On failure: classify via `log_broadcast_to_streaming_failure`;
///    `try_auto_fetch_contract` on real failure,
///    `send_summary_back_on_rejection` on benign stale-version.
async fn drive_relay_broadcast_to_streaming(
    op_manager: &OpManager,
    incoming_tx: Transaction,
    key: ContractKey,
    stream_id: StreamId,
    total_size: u64,
    sender_addr: SocketAddr,
    covered: crate::ring::broadcast_coverage::CoveredPeers,
) -> Result<(), OpError> {
    use crate::operations::orphan_streams::{OrphanStreamError, STREAM_CLAIM_TIMEOUT};

    tracing::info!(
        tx = %incoming_tx,
        contract = %key,
        %stream_id,
        total_size,
        sender = %sender_addr,
        "UPDATE relay (driver streaming): processing BroadcastToStreaming"
    );

    // Step 1: claim stream.
    let stream_handle = match op_manager
        .orphan_stream_registry()
        .claim_or_wait(sender_addr, stream_id, STREAM_CLAIM_TIMEOUT)
        .await
    {
        Ok(handle) => handle,
        Err(OrphanStreamError::AlreadyClaimed) => {
            tracing::debug!(
                tx = %incoming_tx,
                %stream_id,
                "UPDATE relay (driver streaming): BroadcastToStreaming skipped — stream already claimed (dedup)"
            );
            return Ok(());
        }
        Err(e) => {
            tracing::error!(
                tx = %incoming_tx,
                %stream_id,
                error = %e,
                "UPDATE relay (driver streaming): failed to claim stream from orphan registry (broadcast)"
            );
            return Err(OpError::OrphanStreamClaimFailed);
        }
    };

    // Step 2: assemble stream.
    let stream_data = match stream_handle.assemble().await {
        Ok(data) => {
            tracing::debug!(
                tx = %incoming_tx,
                %stream_id,
                assembled_size = data.len(),
                expected_size = total_size,
                "UPDATE relay (driver streaming): stream assembled (broadcast)"
            );
            data
        }
        Err(e) => {
            tracing::error!(
                tx = %incoming_tx,
                %stream_id,
                error = %e,
                "UPDATE relay (driver streaming): failed to assemble stream (broadcast)"
            );
            return Err(OpError::StreamCancelled);
        }
    };

    // Step 3: deserialize payload.
    let payload: BroadcastStreamingPayload = match bincode::deserialize(&stream_data) {
        Ok(p) => p,
        Err(e) => {
            tracing::error!(
                tx = %incoming_tx,
                error = %e,
                "UPDATE relay (driver streaming): failed to deserialize BroadcastStreamingPayload"
            );
            return Err(OpError::invalid_transition(incoming_tx));
        }
    };

    let BroadcastStreamingPayload {
        state_bytes,
        sender_summary_bytes,
    } = payload;

    apply_streaming_broadcast(
        op_manager,
        incoming_tx,
        key,
        state_bytes,
        sender_summary_bytes,
        sender_addr,
        covered,
    )
    .await
}

/// Apply an assembled streaming full-state broadcast (steps 4-9 of
/// `drive_relay_broadcast_to_streaming`): update the sender's cached summary,
/// dedup, WASM-merge the full state, classify failures, and fan out the
/// proactive summary. Extracted from the driver so the #4857 queue-full heal on
/// the streaming path is unit-testable without the transport stream plumbing
/// (steps 1-3: claim + assemble + deserialize).
#[allow(clippy::too_many_arguments)]
async fn apply_streaming_broadcast(
    op_manager: &OpManager,
    incoming_tx: Transaction,
    key: ContractKey,
    state_bytes: Vec<u8>,
    sender_summary_bytes: Vec<u8>,
    sender_addr: SocketAddr,
    covered: crate::ring::broadcast_coverage::CoveredPeers,
) -> Result<(), OpError> {
    let mut receiver_terminal = op_manager
        .payload_mix
        .receiver_terminal_guard(false, state_bytes.len());
    // Step 4: update sender's cached summary. Same self-reported provenance
    // and the same empty-means-could-not-summarize guard as the non-streaming
    // BroadcastTo arm above.
    seed_sender_summary_from_broadcast(op_manager, &key, &sender_summary_bytes, sender_addr);

    // Step 5: dedup cache BEFORE merge (mirrors non-streaming slice A
    // ordering — skipping WASM merge on duplicate broadcast is the key
    // amplifier mitigation). Streaming broadcasts are always full state
    // (`is_delta = false`).
    if op_manager.broadcast_dedup_cache.check_and_insert(
        &key,
        &state_bytes,
        false,
        op_manager.interest_manager.now(),
    ) {
        receiver_terminal.mark_dedup();
        tracing::debug!(
            tx = %incoming_tx,
            %key,
            "UPDATE relay (driver streaming): BroadcastToStreaming skipped — duplicate payload (dedup hit)"
        );
        return Ok(());
    }

    // Step 5b: merge-failure backoff gate (#4861). Streaming broadcasts are
    // always full state (`is_delta = false`) and never emit a ResyncRequest, so
    // only the merge-skip applies. Cleared the instant a merge succeeds (below).
    let stream_payload_hash = crate::ring::merge_backoff::merge_payload_hash(false, &state_bytes);
    match op_manager
        .ring
        .merge_backoff
        .check(key.id(), sender_addr, stream_payload_hash)
    {
        crate::ring::merge_backoff::MergeDecision::Allow => {}
        decision @ (crate::ring::merge_backoff::MergeDecision::InBackoff
        | crate::ring::merge_backoff::MergeDecision::KnownFailedPayload) => {
            receiver_terminal.mark_backoff();
            crate::config::GlobalTestMetrics::record_merge_suppressed_by_backoff();
            tracing::debug!(
                tx = %incoming_tx,
                %key,
                ?decision,
                "UPDATE relay (driver streaming): merge skipped — contract in merge-failure backoff"
            );
            return Ok(());
        }
    }

    let state_for_telemetry = WrappedState::from(state_bytes.clone());

    // Step 6: telemetry — broadcast received.
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

    // Step 7: WASM merge.
    let update_result = super::update_contract(
        op_manager,
        key,
        UpdateData::State(State::from(state_bytes.clone())),
        RelatedContracts::default(),
        crate::contract::Priority::NetworkRelay,
        crate::node::ApplyOrigin::NetworkRelay,
        resolve_covered_peers(op_manager, &key, &incoming_tx, sender_addr, &covered),
    )
    .await;

    let UpdateExecution {
        value: updated_value,
        changed,
    } = match update_result {
        Ok(exec) => {
            // Strictly delta-only reset (#4861): a streaming full-state broadcast
            // merge is mechanically the SAME operation as a ResyncResponse apply
            // (a WASM merge of an incoming FULL STATE), so the fork-oscillation
            // trap applies identically — a large forked contract would oscillate
            // via streaming full states and reset its own backoff on every flip,
            // so the backoff would never trip. Only a clean DELTA apply proves the
            // receiver shared the sender's base (genuine convergence). Streaming
            // is always full state, so it NEVER resets the backoff here; a
            // recovered contract's entry simply ages out via the reaper if
            // failures stop.
            //
            // BUT a state-advancing full-state apply (exec.changed) invalidates
            // the failed-payload MEMO's premise: a delta that failed against the
            // OLD state may be valid against the new one (#4864 round-4 P2). This
            // clears ONLY the memo — NOT any cooldown channel — so it is NOT a
            // backoff reset (the delta-only no-reset doctrine stands; the
            // streaming pin still forbids resetting the backoff here).
            if exec.changed {
                op_manager
                    .ring
                    .merge_backoff
                    .invalidate_payload_memo(key.id());
            }
            exec
        }
        Err(err) => {
            // Record poison-contract merge failures for the backoff (#4861),
            // same classification as the non-streaming driver: only genuine
            // contract-exec rejections, timeout → longer cooldown. #4864 round-6:
            // exclude a scheduler timeout (guest never ran) just like the
            // non-streaming driver — it must not quarantine the contract.
            if err.is_contract_exec_rejection() && !err.is_scheduler_timeout() {
                let class = if err.is_wasm_timeout() {
                    crate::ring::merge_backoff::MergeFailureClass::Timeout
                } else {
                    crate::ring::merge_backoff::MergeFailureClass::Invalid
                };
                op_manager.ring.merge_backoff.record_failure(
                    key.id(),
                    sender_addr,
                    class,
                    stream_payload_hash,
                );
            }

            // #4864 round-6: a scheduler timeout (guest never ran, blocking-pool
            // saturation) is the SAME transient/load class as queue-full — heal
            // via ResyncRequest, no auto-fetch (we still hold the contract).
            // `log_broadcast_to_streaming_failure` already returns false for it
            // (it is an exec rejection), so the auto-fetch branch stays
            // suppressed; keep the name `queue_full` so the heal arm is untouched.
            let queue_full = err.is_contract_queue_full() || err.is_scheduler_timeout();

            // Classify failure: real failure → self-heal via
            // try_auto_fetch_contract; benign stale-version rejection →
            // send_summary_back_on_rejection. Mirrors legacy at
            // update.rs:1336-1361.
            if super::log_broadcast_to_streaming_failure(&incoming_tx, &key, &err) {
                // Inbound broadcast relay path → gated on `contract_in_use` (#4473).
                op_manager.try_auto_fetch_contract(
                    &key,
                    sender_addr,
                    AutoFetchReason::InboundRelay,
                );
            } else if err.is_invalid_update_rejection() {
                let op_mgr = op_manager.clone();
                let contract_key = key;
                let sender_summary_bytes = sender_summary_bytes.clone();
                GlobalExecutor::spawn(async move {
                    super::send_summary_back_on_rejection(
                        &op_mgr,
                        &contract_key,
                        sender_addr,
                        sender_summary_bytes,
                    )
                    .await;
                });
            } else if queue_full {
                // Issue #4857 on the STREAMING full-state path: identical
                // poisoning to the non-streaming driver. A delivered streaming
                // full-state broadcast caches the peer summary
                // (broadcast_queue.rs → record_streaming_delivery →
                // record_delivery_to_interest), so a queue-full drop here leaves
                // the sender believing we are current while our subsequent CRDT
                // deltas merge onto a diverged base WITHOUT error — the
                // delta-apply-failure heal never fires. Emit the same
                // rate-limited ResyncRequest as the non-streaming path.
                send_dropped_broadcast_resync_request(
                    op_manager,
                    key,
                    sender_addr,
                    incoming_tx,
                    DroppedBroadcastReason::QueueFull,
                )
                .await;
            }
            return Err(err);
        }
    };

    // Step 8: telemetry — broadcast applied + proactive summary.
    receiver_terminal.mark_applied(changed, updated_value.len());
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
            "UPDATE relay (driver streaming): BroadcastToStreaming produced no change"
        );
        return Ok(());
    }

    crate::node::network_status::record_update_received();
    tracing::debug!(
        tx = %incoming_tx,
        %key,
        "UPDATE relay (driver streaming): BroadcastToStreaming applied (state changed)"
    );

    // The helper fetches the contract's REAL summary itself (#4923) — no
    // caller-supplied value.
    let op_mgr = op_manager.clone();
    GlobalExecutor::spawn(async move {
        super::send_proactive_summary_notification(&op_mgr, &key, sender_addr).await;
    });

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    /// `start_client_update` must acquire a `ClientOpGuard` before
    /// the `GlobalExecutor::spawn` and move it into the spawned
    /// future. See the sibling pin in `put/op_ctx_task.rs` for the
    /// full rationale (shutdown drain depends on the per-driver
    /// counter; without it the gateway tears down peer connections
    /// mid-UPDATE on auto-update).
    #[test]
    fn start_client_update_acquires_inflight_guard_before_spawn() {
        let src = include_str!("op_ctx_task.rs");
        let entry = src
            .find("pub(crate) async fn start_client_update(")
            .expect("start_client_update must exist");
        let after_spawn = src[entry..]
            .find("GlobalExecutor::spawn(")
            .expect("start_client_update must spawn a driver task");
        let before_spawn = &src[entry..entry + after_spawn];
        assert!(
            before_spawn.contains("op_manager.admit_client_op()"),
            "start_client_update must call op_manager.admit_client_op() \
             before GlobalExecutor::spawn (atomic admission gate + \
             counter bump; closes the Codex r2 TOCTOU)."
        );
        assert!(
            before_spawn.contains("OpError::NodeShuttingDown"),
            "start_client_update must early-return OpError::NodeShuttingDown \
             on a refused admission."
        );
        let spawned = &src[entry + after_spawn..];
        let block_end = spawned
            .find("\n    Ok(client_tx)")
            .expect("start_client_update must return Ok(client_tx)");
        let spawn_block = &spawned[..block_end];
        assert!(
            spawn_block.contains("let _inflight_guard = inflight_guard;"),
            "the ClientOpGuard must be moved into the spawned future."
        );
    }

    /// Phase 7 egress self-block pin (#4300). `start_client_update` MUST
    /// reject a banned contract BEFORE spawning the driver. Mirrors the
    /// receive-side `update_dispatch_gates_banned_contracts` pin in
    /// `ring/contract_ban_list.rs`. See the sibling pin in
    /// `put/op_ctx_task.rs` for the full rationale. The delegate-driven
    /// broadcast fan-out is gated separately at
    /// `handle_broadcast_state_change` in p2p_protoc.rs (pinned there).
    #[test]
    fn start_client_update_gates_banned_contracts_before_spawn() {
        let src = include_str!("op_ctx_task.rs");
        let entry = src
            .find("pub(crate) async fn start_client_update(")
            .expect("start_client_update must exist");
        let after_spawn = src[entry..]
            .find("GlobalExecutor::spawn(")
            .expect("start_client_update must spawn a driver task");
        let before_spawn = &src[entry..entry + after_spawn];
        assert!(
            before_spawn.contains("reject_if_contract_banned"),
            "start_client_update must call reject_if_contract_banned() \
             before GlobalExecutor::spawn so a banned contract's UPDATE \
             is rejected with a typed error instead of being driven to \
             peers (#4300 egress self-block)."
        );
    }

    /// Phase 7 egress self-block pin (#4300). The typed
    /// `OpError::ContractBanned` raised by the egress gate MUST be
    /// translated to a client-visible error in `report_op_init_error`
    /// (`client_events.rs`). The match there is exhaustive (no wildcard),
    /// so a missing arm fails to compile — but this pin documents the
    /// requirement and fails loudly if someone routes ContractBanned to
    /// an inappropriate handler (e.g. silently swallows it).
    #[test]
    fn report_op_init_error_handles_contract_banned() {
        let src = include_str!("../../client_events.rs");
        assert!(
            src.contains("OpError::ContractBanned"),
            "report_op_init_error in client_events.rs must explicitly \
             handle OpError::ContractBanned so the egress self-block \
             (#4300) surfaces a typed error to the client instead of a \
             silent proceed-then-timeout."
        );
    }

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
    /// delegate notifications, dashboard last-updated telemetry via
    /// `Ring::record_contract_update`).
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

    /// Source-grep pin (#4361 / #4365): the client UPDATE driver must
    /// consult `bootstrap_gateway_target` when neither the proximity
    /// cache nor `closest_potentially_hosting` yields a target, so a
    /// freshly-bootstrapped node (empty ring) routes the UPDATE through a
    /// configured gateway. In the hosting case this propagates the update
    /// instead of applying it locally to no audience; in the not-hosting
    /// case it lets the existing missing-params auto-fetch + retry path
    /// prime the local store from the gateway rather than failing flat.
    #[test]
    fn drive_client_update_uses_bootstrap_gateway_fallback() {
        // Brace-matched `extract_fn_body` over `production_source()` (not a
        // `"\nasync fn "` EOF scan): the scan bounded this pin only by
        // function ordering — if `drive_client_update` ever became the last
        // top-level `async fn`, its "body" would run to EOF and swallow the
        // test module (the defect that neutered the PUT streaming pin, #4635
        // review). Brace-matching bounds it structurally instead.
        const SOURCE: &str = include_str!("op_ctx_task.rs");
        let prod = production_source(SOURCE);
        let body = extract_fn_body(prod, "async fn drive_client_update(");
        assert!(
            body.contains("bootstrap_gateway_target("),
            "drive_client_update must fall back to bootstrap_gateway_target \
             on an empty ring instead of applying locally to no audience or \
             failing with NoHostingPeers (#4361 / #4365)"
        );
        // The exclusion predicate must skip this node's own address, matching
        // the skip list passed to closest_potentially_hosting — not a no-op.
        assert!(
            body.contains("bootstrap_gateway_target(op_manager, |addr| addr == sender_addr)"),
            "drive_client_update must exclude its own address (sender_addr) \
             when selecting the bootstrap gateway (#4361 / #4365)"
        );
    }

    // ── Relay UPDATE driver structural pin tests. These pin the
    // driver SOURCE against documented invariants; a future change
    // that breaks any invariant should fail
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

    /// Pin: on `is_invalid_update_rejection()` (benign stale-version
    /// rejection, NOT OOG/traps/validation failures), the driver MUST spawn
    /// `send_summary_back_on_rejection` so the sender's cached peer
    /// summary of us converges. Without this, the sender keeps
    /// full-state-ing identical content to us every interest cycle; see
    /// PR description for the production evidence and the
    /// `broadcast_queue.rs:352-356` full-state fallback that this
    /// counteracts.
    #[test]
    fn broadcast_to_sends_summary_back_on_rejection() {
        let src = include_str!("op_ctx_task.rs");
        let driver_start = src
            .find("async fn drive_relay_broadcast_to(")
            .expect("drive_relay_broadcast_to not found");
        // Scope to the driver body only.
        let after_start = &src[driver_start + 1..];
        let end_offset = after_start
            .find("\nasync fn ")
            .or_else(|| after_start.find("\n#[cfg(test)]"))
            .unwrap_or(after_start.len());
        let driver_src = &src[driver_start..driver_start + 1 + end_offset];

        assert!(
            driver_src.contains("err.is_invalid_update_rejection()"),
            "drive_relay_broadcast_to must gate summary-back on \
             is_invalid_update_rejection (not is_contract_exec_rejection): \
             the broader predicate matches OOG/traps which are \
             attacker-inducible and must not amplify into summary-back"
        );
        assert!(
            driver_src.contains("send_summary_back_on_rejection"),
            "drive_relay_broadcast_to must spawn send_summary_back_on_rejection \
             when the WASM merge rejects — otherwise the sender's cached view \
             of us stays wrong and it keeps full-state-broadcasting identical \
             content"
        );
    }

    /// Pin (HQk7 resync loop): the BroadcastTo driver must feed the
    /// delta-incompatibility memo on BOTH edges — arm it on an
    /// `Invalid`-class DELTA rejection (receiver-side signal that the
    /// contract can't take deltas) and clear it on a successful delta apply
    /// (self-heal). If either call is dropped, the sender-side full-state
    /// fallback in `broadcast_to_single_peer` either never arms (the doomed
    /// delta → ResyncRequest → full-state loop resumes) or never heals (a
    /// recovered contract is stuck on full-state sends until TTL expiry).
    /// See `crate::ring::delta_incompat`.
    #[test]
    fn broadcast_to_feeds_delta_incompat_memo() {
        let driver_src = broadcast_to_driver_src();

        // Arm: inside the exec-rejection failure classification, gated on
        // is_delta AND the Invalid class (Timeout is load, not
        // incompatibility; full-state merges say nothing about deltas).
        let arm_pos = driver_src
            .find(".note_delta_apply_failed(")
            .expect("drive_relay_broadcast_to must arm the delta-incompat memo on delta failure");
        assert!(
            driver_src.contains(
                "if is_delta && class == crate::ring::merge_backoff::MergeFailureClass::Invalid"
            ),
            "the delta-incompat arm must be gated on is_delta AND the Invalid \
             failure class — arming on Timeout (load) or full-state failures \
             would flip healthy contracts to full-state fan-out"
        );

        // Clear: inside the `if is_delta` success arm, next to the backoff
        // reset (the same delta-only convergence signal).
        let clear_pos = driver_src
            .find(".record_delta_success(")
            .expect("drive_relay_broadcast_to must clear the delta-incompat memo on delta success");
        let is_delta_gate_pos = driver_src
            .find("if is_delta {")
            .expect("is_delta success gate not found");
        assert!(
            is_delta_gate_pos < clear_pos && clear_pos < arm_pos,
            "record_delta_success must sit inside the `if is_delta` success arm \
             (delta-only signal), before the failure classification \
             (order: is_delta gate {is_delta_gate_pos} < clear {clear_pos} < arm {arm_pos})"
        );
    }

    /// Helper: return the source of `drive_relay_broadcast_to`'s body,
    /// bounded to the next top-level `async fn` / test module.
    #[cfg(test)]
    fn broadcast_to_driver_src() -> &'static str {
        let src = include_str!("op_ctx_task.rs");
        let start = src
            .find("async fn drive_relay_broadcast_to(")
            .expect("drive_relay_broadcast_to not found");
        let after = &src[start + 1..];
        let end = after
            .find("\nasync fn ")
            .or_else(|| after.find("\n#[cfg(test)]"))
            .unwrap_or(after.len());
        &src[start..start + 1 + end]
    }

    /// Pin (#4861): the merge-failure backoff gate MUST run AFTER the dedup
    /// check and BEFORE `update_contract`. If it ran after the merge, a poison
    /// contract would still re-run the (failing, expensive) WASM merge on every
    /// inbound broadcast — the exact CPU churn the backoff exists to stop.
    #[test]
    fn broadcast_to_backoff_gate_runs_before_merge() {
        let driver_src = broadcast_to_driver_src();
        let dedup_pos = driver_src
            .find("broadcast_dedup_cache.check_and_insert")
            .expect("dedup check missing");
        let backoff_pos = driver_src
            .find(".check(key.id(), sender_addr, payload_hash)")
            .expect("merge_backoff.check gate missing in BroadcastTo driver");
        let merge_pos = driver_src
            .find("super::update_contract(")
            .expect("update_contract call missing");
        assert!(
            dedup_pos < backoff_pos && backoff_pos < merge_pos,
            "merge_backoff.check MUST appear after the dedup check and before \
             update_contract() (order: dedup {dedup_pos} < backoff {backoff_pos} \
             < merge {merge_pos})"
        );
    }

    /// Pin (#4861): the backoff reset MUST be gated on `is_delta` (only a
    /// successful DELTA merge resets — a full-state apply proves nothing about
    /// convergence, see the fork-oscillation note), and a failure MUST be
    /// recorded — but ONLY for genuine contract-exec rejections. Gating
    /// `record_failure` on `is_contract_exec_rejection()` excludes queue-full
    /// (transient load) and missing-contract failures (healed by auto-fetch);
    /// backing either off would block recovery.
    #[test]
    fn broadcast_to_backoff_records_success_and_gated_failure() {
        let driver_src = broadcast_to_driver_src();
        let success_pos = driver_src
            .find(".record_success_from_sender(")
            .expect("drive_relay_broadcast_to must clear the backoff on a successful merge");
        // The reset must be gated on is_delta (delta-only convergence signal).
        let is_delta_gate_pos = driver_src
            .find("if is_delta {")
            .expect("backoff reset must be gated on `if is_delta` (#4861)");
        assert!(
            is_delta_gate_pos < success_pos,
            "merge_backoff.record_success_from_sender MUST be inside an `if is_delta` \
             gate so a full-state apply does not reset the backoff (#4861 fork oscillation)"
        );
        let record_pos = driver_src
            .find(".record_failure(")
            .expect("merge_backoff.record_failure missing in BroadcastTo driver");
        // The record_failure must be inside an is_contract_exec_rejection guard
        // that ALSO excludes scheduler timeouts (#4864 round-6): a scheduler
        // timeout is an exec rejection by cause-prefix but the guest never ran,
        // so it must not create a backoff entry.
        let guard_pos = driver_src
            .find("if err.is_contract_exec_rejection() && !err.is_scheduler_timeout() {")
            .expect(
                "record_failure must be gated on \
                 is_contract_exec_rejection() && !err.is_scheduler_timeout()",
            );
        assert!(
            guard_pos < record_pos,
            "merge_backoff.record_failure MUST be gated on \
             is_contract_exec_rejection() so queue-full and missing-contract \
             failures do NOT create a backoff entry (#4861)"
        );
        // #4864 round-6: the same guard must exclude a scheduler timeout via
        // !err.is_scheduler_timeout() — the guest-never-ran case takes the
        // transient (queue-full-style) path, never the contract quarantine.
        assert!(
            driver_src.contains("!err.is_scheduler_timeout()"),
            "the record gate MUST exclude scheduler timeouts via \
             !err.is_scheduler_timeout() (#4864 round-6): a queued-never-ran \
             merge must not quarantine the contract"
        );
        assert!(
            driver_src.contains("is_wasm_timeout()"),
            "the failure class must distinguish a WASM timeout (longer cooldown) \
             via is_wasm_timeout()"
        );
    }

    /// Pin (#4861): the `ResyncRequest` emission on delta-apply failure MUST be
    /// gated by the per-contract emit rate limiter, and the sender summary-clear
    /// MUST live inside that gate (it is part of the resync handshake — clearing
    /// it without emitting would force the sender to full-state us next cycle).
    #[test]
    fn broadcast_to_resync_emit_is_rate_limited() {
        let driver_src = broadcast_to_driver_src();
        // Match the field name (fmt-stable) rather than the full method call,
        // which rustfmt may break across lines.
        let gate_pos = driver_src
            .find("resync_emit_limiter")
            .expect("ResyncRequest emit must be gated by resync_emit_limiter");
        let summary_clear_pos = driver_src
            .find("clear_peer_summary(")
            .expect("sender summary-clear missing");
        assert!(
            driver_src.contains("SummaryMissingReason::ClearedByDeltaApplyFailure"),
            "the sender summary-clear must be tagged ClearedByDeltaApplyFailure \
             so #4961 can attribute the full_no_their_summary_tracked arm to \
             this path"
        );
        let resync_pos = driver_src
            .find("InterestMessage::ResyncRequest")
            .expect("ResyncRequest emission missing");
        assert!(
            gate_pos < summary_clear_pos && summary_clear_pos < resync_pos,
            "emit rate-limit gate ({gate_pos}) must wrap BOTH the summary-clear \
             ({summary_clear_pos}) and the ResyncRequest emission ({resync_pos})"
        );
        assert!(
            driver_src.contains("record_resync_request_suppressed()"),
            "the suppressed branch must record the resync-request-suppressed metric"
        );
    }

    /// Pin (#4861): the streaming full-state merge path must carry the same
    /// backoff gate + record wiring (a poison contract must be quarantined on
    /// the streaming path too). The streaming merge logic lives in
    /// `apply_streaming_broadcast` (steps 4-9), which
    /// `drive_relay_broadcast_to_streaming` delegates to after assembling the
    /// stream — scrape that function, not the thin transport wrapper.
    #[test]
    fn broadcast_to_streaming_has_backoff_wiring() {
        let src = include_str!("op_ctx_task.rs");
        let start = src
            .find("async fn apply_streaming_broadcast(")
            .expect("apply_streaming_broadcast (streaming merge path) not found");
        let after = &src[start + 1..];
        let end = after
            .find("\nasync fn ")
            .or_else(|| after.find("\n#[cfg(test)]"))
            .unwrap_or(after.len());
        let driver_src = &src[start..start + 1 + end];

        // Match the args (fmt-stable) rather than `merge_backoff.check(`, which
        // rustfmt breaks across lines here (the longer `stream_payload_hash`
        // pushes the chain over the chain-width limit).
        let backoff_pos = driver_src
            .find(".check(key.id(), sender_addr, stream_payload_hash)")
            .expect("streaming merge path missing merge_backoff.check gate");
        let merge_pos = driver_src
            .find("super::update_contract(")
            .expect("streaming merge path missing update_contract");
        assert!(
            backoff_pos < merge_pos,
            "streaming merge_backoff.check MUST run before update_contract"
        );
        // Strictly delta-only reset (#4861): streaming is always full state, and
        // a full-state merge is the same fork-flippable operation as a resync
        // apply, so the streaming path must NOT reset the backoff. (Assemble
        // the needle so this literal is not a self-referential match.)
        // Match any `record_success*` variant (record_success_from_sender /
        // record_success_local) — the streaming full-state path must call none.
        let record_success = concat!("record_", "success");
        assert!(
            !driver_src.contains(record_success),
            "streaming path must NOT reset the backoff — a full-state merge is \
             not convergence evidence (only a clean DELTA apply is). See #4861."
        );
        assert!(
            driver_src.contains(".record_failure(")
                && driver_src.contains("is_contract_exec_rejection()"),
            "streaming path must record gated failures like the non-streaming driver"
        );
        // #4864 round-7: the streaming record gate MUST also exclude scheduler
        // timeouts (the guest-never-ran, queued-on-a-saturated-pool case), exactly
        // like the relay driver (pinned in
        // broadcast_to_backoff_records_success_and_gated_failure). Without this
        // needle a refactor dropping the exclusion from the streaming driver would
        // silently re-quarantine a healthy contract on transient scheduler
        // overload — the round-6/7 regression this exclusion exists to prevent.
        assert!(
            driver_src.contains("!err.is_scheduler_timeout()"),
            "streaming record gate MUST exclude scheduler timeouts \
             (!err.is_scheduler_timeout(), #4864 round-6/7): a queued-never-ran \
             failure is transient load, not a contract fault — mirror of the relay pin"
        );
    }

    /// Pin (#4864 review P2): a successful client-local DELTA merge in
    /// `drive_client_update` MUST reset the merge-failure backoff (so a contract
    /// a local client recovers stops suppressing inbound broadcasts), gated
    /// delta-only (a client full-state update proves nothing).
    #[test]
    fn client_local_delta_success_resets_backoff() {
        let src = include_str!("op_ctx_task.rs");
        let start = src
            .find("async fn drive_client_update(")
            .expect("drive_client_update not found");
        let after = &src[start + 1..];
        let end = after
            .find("\nasync fn ")
            .or_else(|| after.find("\n#[cfg(test)]"))
            .unwrap_or(after.len());
        let body = &src[start..start + 1 + end];
        let success = concat!(".record_", "success_local(");
        assert!(
            body.contains(success),
            "drive_client_update must reset the backoff on a successful merge via \
             record_success_local (contract-wide only, not per-sender) (#4864 P2)"
        );
        assert!(
            body.contains("is_delta_update"),
            "the client-local backoff reset must be gated delta-only (is_delta_update)"
        );
    }

    /// Pin: both relay drivers MUST gate on
    /// `active_relay_update_txs` to reject duplicate inbound
    /// messages for an in-flight tx (amplification guard).
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
            4,
            "expected four dedup sites (RequestUpdate + BroadcastTo + \
             RequestUpdateStreaming + BroadcastToStreaming)"
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

    /// Pin: the slice A non-streaming drivers (`drive_relay_request_update`
    /// / `drive_relay_broadcast_to`) must NOT reference the streaming
    /// wire variants. Streaming logic lives in the slice C drivers.
    #[test]
    fn slice_a_drivers_do_not_touch_streaming_variants() {
        let src = include_str!("op_ctx_task.rs");
        for driver_name in ["drive_relay_request_update(", "drive_relay_broadcast_to("] {
            let start = src
                .find(&format!("async fn {driver_name}"))
                .unwrap_or_else(|| panic!("{driver_name} not found"));
            // End at the fn's closing brace at column 0 — "\n}\n" — which
            // is the cargo-fmt-canonical shape for a top-level fn body.
            let after = &src[start..];
            let end = after.find("\n}\n").expect("fn body end not found");
            let driver_src = &after[..end];
            assert!(
                !driver_src.contains("RequestUpdateStreaming"),
                "{driver_name} (slice A) must not reference RequestUpdateStreaming"
            );
            assert!(
                !driver_src.contains("BroadcastToStreaming"),
                "{driver_name} (slice A) must not reference BroadcastToStreaming"
            );
        }
    }

    // ── Streaming relay driver structural pin tests ─────────────────────

    /// Pin: streaming drivers must exist and be public at the
    /// crate level.
    #[test]
    fn slice_c_streaming_drivers_exist() {
        let src = include_str!("op_ctx_task.rs");
        assert!(
            src.contains("pub(crate) async fn start_relay_request_update_streaming("),
            "start_relay_request_update_streaming must exist (slice C entry point)"
        );
        assert!(
            src.contains("pub(crate) async fn start_relay_broadcast_to_streaming("),
            "start_relay_broadcast_to_streaming must exist (slice C entry point)"
        );
    }

    /// Pin: streaming drivers must gate on `active_relay_update_txs`
    /// (unified tx dedup set across slices A and C).
    #[test]
    fn slice_c_drivers_use_tx_dedup_gate() {
        let src = include_str!("op_ctx_task.rs");
        for entry in [
            "pub(crate) async fn start_relay_request_update_streaming(",
            "pub(crate) async fn start_relay_broadcast_to_streaming(",
        ] {
            let start = src.find(entry).unwrap_or_else(|| panic!("{entry} missing"));
            let body = &src[start..start + 1500];
            assert!(
                body.contains("active_relay_update_txs.insert"),
                "{entry} must insert into active_relay_update_txs for per-tx dedup"
            );
            assert!(
                body.contains("RELAY_UPDATE_DEDUP_REJECTS.fetch_add"),
                "{entry} must increment RELAY_UPDATE_DEDUP_REJECTS on dedup reject"
            );
        }
    }

    /// Pin: streaming drivers must claim via
    /// `orphan_stream_registry().claim_or_wait` — this is the atomic
    /// stream-level dedup that prevents duplicate assembly runs.
    #[test]
    fn slice_c_drivers_claim_orphan_stream() {
        let src = include_str!("op_ctx_task.rs");
        for driver in [
            "drive_relay_request_update_streaming(",
            "drive_relay_broadcast_to_streaming(",
        ] {
            let start = src
                .find(&format!("async fn {driver}"))
                .unwrap_or_else(|| panic!("{driver} not found"));
            let after = &src[start + 1..];
            let end = after
                .find("\nasync fn ")
                .or_else(|| after.find("\n#[cfg(test)]"))
                .unwrap_or(after.len());
            let driver_src = &src[start..start + 1 + end];
            assert!(
                driver_src.contains("orphan_stream_registry()"),
                "{driver} must claim via orphan_stream_registry() for atomic \
                 stream dedup"
            );
            assert!(
                driver_src.contains("claim_or_wait("),
                "{driver} must use claim_or_wait on the orphan stream registry"
            );
        }
    }

    /// Pin: streaming drivers are fire-and-forget. They MUST NOT call
    /// `send_and_await` (no upstream reply) and MUST NOT call
    /// `pipe_stream` (propagation is via BroadcastStateChange, not
    /// explicit forward).
    #[test]
    fn slice_c_drivers_are_fire_and_forget() {
        let src = include_str!("op_ctx_task.rs");
        for driver in [
            "drive_relay_request_update_streaming(",
            "drive_relay_broadcast_to_streaming(",
        ] {
            let start = src
                .find(&format!("async fn {driver}"))
                .unwrap_or_else(|| panic!("{driver} not found"));
            let after = &src[start + 1..];
            let end = after
                .find("\nasync fn ")
                .or_else(|| after.find("\n#[cfg(test)]"))
                .unwrap_or(after.len());
            let driver_src = &src[start..start + 1 + end];
            assert!(
                !driver_src.contains(".send_and_await("),
                "{driver} must not call send_and_await — UPDATE streaming is \
                 fire-and-forget"
            );
            assert!(
                !driver_src.contains(".pipe_stream("),
                "{driver} must not call pipe_stream — streaming UPDATE relay \
                 propagation is via BroadcastStateChange after local apply, \
                 not explicit downstream piping"
            );
        }
    }

    /// Pin: `BroadcastToStreaming` driver must check dedup cache BEFORE
    /// calling `update_contract`. If the order flips, duplicate
    /// broadcasts re-run WASM merge and amplify cost.
    #[test]
    fn broadcast_to_streaming_dedup_runs_before_merge() {
        let src = include_str!("op_ctx_task.rs");
        let start = src
            .find("async fn apply_streaming_broadcast(")
            .expect("apply_streaming_broadcast not found");
        let after = &src[start + 1..];
        let end = after
            .find("\nasync fn ")
            .or_else(|| after.find("\n#[cfg(test)]"))
            .unwrap_or(after.len());
        let driver_src = &src[start..start + 1 + end];

        let dedup_pos = driver_src
            .find("broadcast_dedup_cache.check_and_insert")
            .expect("dedup check missing in BroadcastToStreaming driver");
        let merge_pos = driver_src
            .find("super::update_contract(")
            .expect("update_contract call missing in BroadcastToStreaming driver");
        assert!(
            dedup_pos < merge_pos,
            "broadcast_dedup_cache.check_and_insert MUST appear before \
             update_contract() in apply_streaming_broadcast"
        );
    }

    /// Issue #4857 / #4251 reconciliation pin (+ #4864 round-4). On a queue-full
    /// broadcast drop, BOTH broadcast drivers must emit a RATE-LIMITED
    /// `ResyncRequest` — neither suppress it (permanent divergence, #4857) nor
    /// emit it unthrottled (full-state storm, #4251). Both route through the
    /// shared `send_dropped_broadcast_resync_request` helper, which gates the
    /// `InterestMessage::ResyncRequest` emit behind BOTH the per-(contract, sender)
    /// `begin_resync_request` throttle AND the global per-contract
    /// `resync_emit_limiter` cap (each a `!gate { return; }` guard before the
    /// emit). Auto-fetch stays suppressed on each driver's queue-full arm.
    #[test]
    fn broadcast_to_sends_throttled_resync_on_queue_full() {
        let src = include_str!("op_ctx_task.rs");

        // Slice a top-level `async fn NAME(` body out of the source (up to the
        // next top-level `async fn` or the test module).
        let fn_body = |name: &str| -> &str {
            let start = src.find(name).unwrap_or_else(|| panic!("{name} not found"));
            let after = &src[start + 1..];
            // Terminate on ANY fn opener, not just `async fn` (review finding
            // 19). Several plain `fn`s sit between the async ones this pin
            // slices: `jittered_resync_retry_delay` between the two resync
            // fns, `seed_sender_summary_from_broadcast` before the broadcast
            // driver, and — added by this PR — `spawn_resync_followups`
            // between `reserve_resync_emit` and the next async fn. With an
            // `async fn`-only terminator a slice ran straight through them,
            // and a gate deleted from the sliced function would still satisfy
            // the pin as long as the same identifier appeared anywhere
            // downstream. Taking the MINIMUM rather than the first match
            // matters: the openers are not in source order.
            let end = [
                "\nasync fn ",
                "\nfn ",
                "\npub async fn ",
                "\npub fn ",
                "\npub(",
                // Item openers, not just fn openers (review round 3, C4).
                // `reserve_resync_emit` is followed by `const
                // MAX_COALESCED_RESYNC_*` and `static SLOT_CAP_LOG` before the
                // next `fn`, so a fn-only terminator ran the slice straight
                // through their DOC COMMENTS — and a pin asserting on a
                // mechanism could then be satisfied by prose describing it
                // rather than by the code.
                "\nconst ",
                "\nstatic ",
                "\npub(crate) const ",
                "\npub(crate) static ",
            ]
            .iter()
            .filter_map(|kw| after.find(kw))
            .chain(after.find("\n#[cfg(test)]"))
            .min()
            .unwrap_or(after.len());
            &src[start..start + 1 + end]
        };
        // Slice the queue-full match arm (from `marker` up to its closing
        // `return Err(err);`) out of a driver body.
        let queue_full_arm = |body: &'static str, marker: &str| -> &'static str {
            let arm_start = body
                .find(marker)
                .unwrap_or_else(|| panic!("queue-full arm `{marker}` missing"));
            let arm_end = body[arm_start..]
                .find("return Err(err);")
                .expect("return after queue-full arm missing");
            &body[arm_start..arm_start + arm_end]
        };

        // Non-streaming driver delegates to the helper and does NOT auto-fetch.
        // #4864 round-6: the `queue_full` local now also folds in a scheduler
        // timeout (transient load, same heal path), so the classification is
        // `is_contract_queue_full() || is_scheduler_timeout()`.
        let ns = fn_body("async fn drive_relay_broadcast_to(");
        assert!(
            ns.contains(
                "let queue_full = err.is_contract_queue_full() || err.is_scheduler_timeout();"
            ),
            "drive_relay_broadcast_to must still classify queue-full and fold in \
             scheduler timeouts (#4251, #4864 round-6)"
        );
        let ns_arm = queue_full_arm(ns, "} else if queue_full {");
        assert!(
            ns_arm.contains("send_dropped_broadcast_resync_request("),
            "drive_relay_broadcast_to queue-full arm MUST delegate to \
             send_dropped_broadcast_resync_request (heals #4857)"
        );
        assert!(
            !ns_arm.contains("try_auto_fetch_contract"),
            "drive_relay_broadcast_to queue-full arm must NOT auto-fetch (#4251)"
        );

        // Streaming path: the queue-full arm lives in `apply_streaming_broadcast`
        // (steps 4-9, extracted from the driver for testability). It delegates to
        // the SAME helper and does NOT auto-fetch (auto-fetch lives in the
        // separate log_broadcast_to_streaming_failure branch). Missing arm ⇒
        // #4857 open on the streaming full-state path.
        assert!(
            fn_body("async fn drive_relay_broadcast_to_streaming(")
                .contains("apply_streaming_broadcast("),
            "drive_relay_broadcast_to_streaming must delegate to \
             apply_streaming_broadcast — otherwise the streaming apply/heal logic \
             is unreachable"
        );
        // #4864 round-6: the streaming heal arm is now gated on the same
        // `queue_full` local (queue-full OR scheduler timeout).
        let st = fn_body("async fn apply_streaming_broadcast(");
        assert!(
            st.contains(
                "let queue_full = err.is_contract_queue_full() || err.is_scheduler_timeout();"
            ),
            "apply_streaming_broadcast must classify queue-full and fold in \
             scheduler timeouts (#4251, #4864 round-6)"
        );
        let st_arm = queue_full_arm(st, "} else if queue_full {");
        assert!(
            st_arm.contains("send_dropped_broadcast_resync_request("),
            "apply_streaming_broadcast queue-full arm MUST delegate to \
             send_dropped_broadcast_resync_request — otherwise #4857 stays OPEN on the \
             streaming full-state path"
        );
        assert!(
            !st_arm.contains("try_auto_fetch_contract"),
            "streaming queue-full arm must NOT auto-fetch (#4251)"
        );

        // The shared helper gates the single emit behind BOTH the per-(contract,
        // sender) throttle AND the global per-contract emit cap (#4251/#4857 +
        // #4864 round-4). Each is a `!gate { return; }` guard that precedes the
        // emit, so an unthrottled emission cannot re-open the #4251 storm. The
        // per-sender throttle is RESERVED atomically (`begin`, records under the
        // lock) then RELEASED (`cancel`) if the global cap rejects (#4864 round-6
        // item 2), so concurrent callbacks can't both pass and a globally
        // suppressed emit does not burn the window.
        // #5510 re-anchor: the gates now live in `reserve_resync_emit`, which
        // `send_dropped_broadcast_resync_request` calls before anything else.
        // The property is unchanged — nothing is emitted until both gates pass —
        // and it is now checked where the gates actually are. The `emit` anchor
        // is the correlation record rather than the wire message, because the
        // enqueue moved to `dispatch_resync_request`; the record is made at the
        // same point in the sequence and is what authorizes the response, so it
        // is the tighter anchor of the two.
        let helper = fn_body("async fn reserve_resync_emit(");
        let per_sender = helper.find(".begin_resync_request_or_note_drop(").expect(
            "helper must RESERVE the per-sender throttle atomically via \
             begin_resync_request_or_note_drop (#4864 round-6 item 2; renamed in \
             #5510 round 3 when the refusal and the debt-note became one \
             critical section)",
        );
        // Anchor on the CALL, not the bare identifier — a comment earlier in the
        // helper mentions `resync_emit_limiter` before the real gate (#4864 round-6
        // item 6c).
        let global_cap = helper.find(".check_and_record(*key.id())").expect(
            "helper must ALSO gate on the global per-contract emit cap \
             (resync_emit_limiter.check_and_record, #4864 round-4)",
        );
        let emit = helper.find("outstanding_resync_requests").expect(
            "helper must record the outstanding request that authorizes the heal (#4857/#4864)",
        );
        // #5510 review round 3 (X1) REVERSED this. The helper used to `cancel`
        // the reservation on a global-cap rejection, releasing the window so the
        // sender could retry once tokens refilled. Correct before a follow-up
        // chain existed; wrong after. With the window released, every later drop
        // on the pair is refused afresh and spawns a chain of its own, so a hot
        // pair drains the node-wide slot pool in seconds. The reservation is now
        // KEPT with its dropped-during-window flag set, which throttles those
        // later drops into the one chain already carrying the debt.
        let keep = helper
            .find(".note_resync_debt_on_held_reservation(")
            .expect(
                "helper must KEEP the reservation on a global-cap rejection and mark \
             it as owing a repair — releasing the window lets every later drop \
             on the pair spawn its own chain (#5510 X1)",
            );
        assert!(
            !helper.contains(".cancel_resync_request("),
            "helper must NOT cancel the reservation on a global-cap rejection; \
             that is the pre-X1 behaviour whose slot-drain this replaced"
        );
        assert!(
            per_sender < emit && global_cap < emit,
            "the authorized emit ({emit}) MUST come AFTER both the per-sender \
             throttle reservation ({per_sender}) and the global emit cap ({global_cap}), \
             so an unthrottled emission cannot re-open the #4251 storm"
        );
        assert!(
            per_sender < global_cap && global_cap < keep && keep < emit,
            "the debt-mark ({keep}) MUST be in the global-cap reject branch — after \
             the reservation ({per_sender}) and the global cap ({global_cap}), before \
             the emit ({emit}) — so a suppressed repair is remembered on the window \
             it is owed against rather than dropped (#4864 round-6, #5510 X1)"
        );
    }

    /// Drain the event-loop notification channel (non-blocking) and return the
    /// targets of every `ResyncRequest(key)` emitted since the last drain.
    /// Make `op_manager` genuinely HOLD `keys`, so gate B does not suppress the
    /// chain's coalesced emit (#5510, review round 3).
    ///
    /// `should_summarize_or_broadcast` is `(is_hosting_contract ||
    /// contract_in_use) && contract_state_present`: a local client subscription
    /// supplies the first, stored state the second. Both are needed, and the
    /// caller asserts the predicate afterwards — a test of the repair that runs
    /// on a node holding nothing passes against the GATE rather than against the
    /// repair, which is a vacuous pass in the direction that looks like success.
    ///
    /// Returns the tempdir guard; keep it alive for the test's duration.
    async fn hold_contracts(op_manager: &OpManager, keys: &[ContractKey]) -> tempfile::TempDir {
        let dir = tempfile::tempdir().expect("tempdir");
        let storage = crate::contract::storages::Storage::new(dir.path())
            .await
            .expect("storage");
        for key in keys {
            storage
                .store_state_sync(key, WrappedState::new(vec![1u8, 2, 3]))
                .expect("store state");
        }
        op_manager.ring.set_hosting_storage(storage);
        for key in keys {
            op_manager
                .ring
                .add_client_subscription(key.id(), crate::client_events::ClientId::FIRST);
            assert!(
                op_manager.ring.should_summarize_or_broadcast(key),
                "precondition: the node must HOLD {key}, or gate B suppresses the \
                 coalesced emit and the test passes for the wrong reason"
            );
        }
        dir
    }

    fn drain_resync_targets(
        rx: &mut crate::node::EventLoopNotificationsReceiver,
        key: ContractKey,
    ) -> Vec<SocketAddr> {
        drain_resync_events(rx)
            .into_iter()
            .filter_map(|(k, target)| (k == key).then_some(target))
            .collect()
    }

    /// Drain every emitted `ResyncRequest`, KEEPING its contract key.
    ///
    /// [`drain_resync_targets`] filters to one key and discards the rest, which
    /// is fine for a single-contract test and a trap for any test watching two:
    /// draining the first key silently eats the second key's events, and the
    /// second assertion then reads an empty vector as "it did not emit". Use
    /// this whenever more than one contract is in play.
    fn drain_resync_events(
        rx: &mut crate::node::EventLoopNotificationsReceiver,
    ) -> Vec<(ContractKey, SocketAddr)> {
        let mut events = Vec::new();
        while let Ok(ev) = rx.notifications_receiver.try_recv() {
            if let Either::Right(NodeEvent::SendInterestMessage {
                target,
                message: InterestMessage::ResyncRequest { key },
            }) = ev
            {
                events.push((key, target));
            }
        }
        events
    }

    /// Build a minimal real OpManager wired to a contract-handler stand-in that
    /// answers every `UpdateQuery` with `ContractQueueFull` (the silent drop a
    /// saturated receiver hits under load) and every `GetQuery` with "no state".
    /// Returns the op_manager, the event-loop notification receiver (to observe
    /// emitted `ResyncRequest`s), and a keep-alive guard holding the handler task
    /// plus the channel halves that must outlive the op_manager. Mirrors
    /// `op_state_manager.rs::build_reroot_test_op_manager`; shared by the #4857
    /// queue-full behavioral tests for the non-streaming and streaming paths.
    async fn build_queue_full_test_node(
        id: &str,
    ) -> (
        Arc<OpManager>,
        crate::node::EventLoopNotificationsReceiver,
        Box<dyn std::any::Any>,
    ) {
        build_queue_full_test_node_with_clock(id, None).await
    }

    /// As [`build_queue_full_test_node`], but injects `override_clock` as the
    /// node's `hosting_time_source_override` — the SAME clock the `InterestManager`
    /// throttle uses (see `op_state_manager.rs`). A `SharedMockTimeSource` here
    /// lets a test DECOUPLE the throttle's clock from tokio's, exercising the
    /// #4857 P2 retry's injected-clock pacing and DST liveness backstop.
    async fn build_queue_full_test_node_with_clock(
        id: &str,
        override_clock: Option<crate::util::time_source::DynTimeSource>,
    ) -> (
        Arc<OpManager>,
        crate::node::EventLoopNotificationsReceiver,
        Box<dyn std::any::Any>,
    ) {
        // Distinct `id` per caller: `ConfigArgs::build` derives a Local-mode data
        // directory from it, so two concurrently-running tests sharing an id race
        // on that directory (flaky "No such file or directory").
        let config_args = crate::config::ConfigArgs {
            id: Some(id.to_string()),
            mode: Some(crate::contract::OperationMode::Local),
            ..Default::default()
        };
        let mut node_config =
            crate::node::NodeConfig::new(config_args.build().await.expect("build Config"))
                .await
                .expect("build NodeConfig");
        // Decouple the throttle/hosting clock from tokio when a test asks for it.
        node_config.hosting_time_source_override = override_clock;
        let (notification_rx, notification_tx) = crate::node::event_loop_notification_channel();
        let (ops_ch_channel, mut ch_channel, wait_for_event) =
            crate::contract::contract_handler_channel();
        let connection_manager = crate::ring::ConnectionManager::new(&node_config);
        let (result_router_tx, result_router_rx) = tokio::sync::mpsc::channel(100);
        let task_monitor = crate::node::background_task_monitor::BackgroundTaskMonitor::new();
        let op_manager = Arc::new(
            OpManager::new(
                notification_tx,
                ops_ch_channel,
                &node_config,
                crate::tracing::DynamicRegister::new(vec![]),
                connection_manager,
                result_router_tx,
                &task_monitor,
            )
            .expect("build OpManager"),
        );
        op_manager.ring.attach_op_manager(&op_manager);
        let self_addr: SocketAddr = "127.0.0.1:12000".parse().unwrap();
        op_manager
            .ring
            .connection_manager
            .set_own_addr_local_for_test(self_addr);

        let handler = tokio::spawn(async move {
            while let Ok((id, ev, _priority)) = ch_channel.recv_from_sender().await {
                #[allow(
                    clippy::wildcard_enum_match_arm,
                    reason = "a stand-in handler that serves exactly the two events this test drives; any other event is an unexpected-input panic, and enumerating all 20+ ContractHandlerEvent variants to reach the same panic would only rot"
                )]
                let response = match ev {
                    ContractHandlerEvent::GetQuery { .. } => ContractHandlerEvent::GetResponse {
                        key: None,
                        response: Ok(StoreResponse {
                            state: None,
                            contract: None,
                        }),
                    },
                    ContractHandlerEvent::UpdateQuery { .. } => {
                        ContractHandlerEvent::UpdateResponse {
                            new_value: Err(crate::contract::ExecutorError::other(
                                crate::contract::ContractQueueFull,
                            )),
                            state_changed: false,
                        }
                    }
                    other => panic!("unexpected handler event in stand-in: {other:?}"),
                };
                if ch_channel.send_to_sender(id, response).await.is_err() {
                    break;
                }
            }
        });

        let guard: Box<dyn std::any::Any> =
            Box::new((handler, result_router_rx, task_monitor, wait_for_event));
        (op_manager, notification_rx, guard)
    }

    /// Issue #4857 behavioral reproduction (non-streaming delta path): when an
    /// inbound broadcast delta is dropped with `ContractQueueFull`,
    /// `drive_relay_broadcast_to` MUST emit a `ResyncRequest` back to the sender
    /// so it clears its poisoned summary cache and re-sends the dropped change —
    /// and that emission MUST be rate-limited so a burst of drops does not
    /// re-open the #4251 storm.
    ///
    /// On `origin/main` this test FAILS: the queue-full arm suppresses the
    /// ResyncRequest entirely, so `first` is empty (the divergence is silent and
    /// permanent). With the fix, the first drop emits exactly one ResyncRequest
    /// and the immediate second drop is throttled.
    ///
    /// The drop is injected at the contract-handler seam (a stand-in handler
    /// answers every `UpdateQuery` with `ContractQueueFull`) rather than by
    /// saturating the real 100-slot fair queue, which is non-deterministic.
    ///
    /// Note (#4857 P2): each drop also spawns a trailing-retry task, but its
    /// first re-dispatch is a jittered ~1.6-2.4s away on the throttle's clock —
    /// far beyond this synchronous, non-`start_paused` test's wall-clock window
    /// — so the immediate/throttled emit counts asserted here are unaffected.
    #[tokio::test]
    async fn queue_full_broadcast_emits_throttled_resync_request() {
        let (op_manager, mut notification_rx, _guard) =
            build_queue_full_test_node("queue-full-resync-4857-delta").await;
        let key = ContractKey::from_id_and_code(
            ContractInstanceId::new([7u8; 32]),
            CodeHash::new([8u8; 32]),
        );
        let sender_addr: SocketAddr = "127.0.0.1:12100".parse().unwrap();

        // First broadcast delta → dropped queue-full → MUST emit a ResyncRequest.
        let r1 = drive_relay_broadcast_to(
            &op_manager,
            Transaction::new::<UpdateMsg>(),
            key,
            DeltaOrFullState::Delta(vec![1, 2, 3]),
            Vec::new(),
            sender_addr,
            crate::ring::broadcast_coverage::CoveredPeers::empty(),
        )
        .await;
        assert!(
            r1.as_ref()
                .err()
                .is_some_and(|e| e.is_contract_queue_full()),
            "expected a queue-full error from the stand-in handler, got {r1:?}"
        );
        assert_eq!(
            drain_resync_targets(&mut notification_rx, key),
            vec![sender_addr],
            "a queue-full drop MUST emit exactly one ResyncRequest to the sender \
             so it clears its poisoned summary and re-sends (heals #4857)"
        );

        // Second broadcast with DIFFERENT delta bytes (so it is not a dedup hit)
        // within the throttle window → still queue-full, but ResyncRequest is
        // throttled (bounds the #4251 amplification).
        let r2 = drive_relay_broadcast_to(
            &op_manager,
            Transaction::new::<UpdateMsg>(),
            key,
            DeltaOrFullState::Delta(vec![4, 5, 6]),
            Vec::new(),
            sender_addr,
            crate::ring::broadcast_coverage::CoveredPeers::empty(),
        )
        .await;
        assert!(
            r2.as_ref()
                .err()
                .is_some_and(|e| e.is_contract_queue_full()),
            "second broadcast should still hit queue-full, got {r2:?}"
        );
        assert!(
            drain_resync_targets(&mut notification_rx, key).is_empty(),
            "a second queue-full drop within the throttle window MUST NOT emit \
             another ResyncRequest (bounds #4251 amplification)"
        );
    }

    /// #5510 review round 3 (S1): a chain that starts OWING a repair — because
    /// the global emit cap refused the immediate one — actually retries and
    /// emits once the cap refills.
    ///
    /// This path had no behavioural test, and the gap was not benign: reverting
    /// the `Err(ReserveRefusal::GlobalCap)` spawn to a bare `return` left the
    /// whole suite green, because the existing pins anchor on
    /// `spawn_resync_followups`'s own match arm, which survives deleting the
    /// CALL SITE. A pin on the callee cannot see a caller that stopped calling.
    ///
    /// Before this path existed, a drop whose repair the cap refused recorded
    /// nothing and retried never: the cap `cancel`s the reservation, so there
    /// was not even a window to hang the debt on. With `EMIT_BURST` 2 against a
    /// 30s re-reservation, that is a routine case, not a corner.
    #[tokio::test(start_paused = true)]
    async fn a_cap_refused_immediate_repair_still_emits_once_the_cap_refills() {
        let (op_manager, mut notification_rx, _guard) =
            build_queue_full_test_node("owed-only-chain-5510").await;
        let key = ContractKey::from_id_and_code(
            ContractInstanceId::new([81u8; 32]),
            CodeHash::new([82u8; 32]),
        );
        let _held = hold_contracts(&op_manager, &[key]).await;
        let target: SocketAddr = "127.0.0.1:13600".parse().unwrap();

        // Drain this contract's emit tokens BEFORE any drop, so the very first
        // repair attempt is refused by the global cap rather than granted.
        for _ in 0..(crate::ring::resync_rate_limit::EMIT_BURST as usize) {
            assert!(
                op_manager
                    .ring
                    .resync_emit_limiter
                    .check_and_record(*key.id()),
                "draining the burst must succeed while tokens remain"
            );
        }
        assert!(
            !op_manager
                .ring
                .resync_emit_limiter
                .check_and_record(*key.id()),
            "precondition: the contract's emit bucket must be empty, or this \
             test exercises the granted path instead of the refused one"
        );

        let before = op_manager.interest_manager.outstanding_resync_retries();
        send_dropped_broadcast_resync_request(
            &op_manager,
            key,
            target,
            Transaction::new::<UpdateMsg>(),
            DroppedBroadcastReason::RateLimited,
        )
        .await;
        tokio::task::yield_now().await;

        assert!(
            drain_resync_targets(&mut notification_rx, key).is_empty(),
            "the cap refused the emit, so nothing may go out now"
        );
        assert!(
            op_manager.interest_manager.outstanding_resync_retries() > before,
            "an owing chain must have been spawned to carry the debt; without it \
             this drop is simply lost, which is the #5510 failure"
        );

        // Advance past the cap's refill. The chain re-reserves and emits ONE
        // coalesced request for the drop it has been carrying.
        let mut emitted = Vec::new();
        for _ in 0..200 {
            tokio::time::advance(Duration::from_millis(500)).await;
            tokio::task::yield_now().await;
            emitted.extend(drain_resync_targets(&mut notification_rx, key));
            if !emitted.is_empty() {
                break;
            }
        }
        assert_eq!(
            emitted,
            vec![target],
            "once the cap refills, the owing chain must emit exactly one repair \
             for the drop it carried"
        );
    }

    /// #5510 review round 3 (T1): when a SIBLING takes the window at the
    /// boundary, the old chain hands its debt over and exits rather than
    /// retrying against a reservation it does not hold.
    ///
    /// The map is keyed by `(contract, target)` alone, so at a window boundary
    /// two chains can briefly both believe they are the owner. Exactly one
    /// repair must result: the sibling's. Without the hand-over the debt dies
    /// with the departing chain and the drop is never repaired — the silent
    /// loss #5510 exists to close, reintroduced one layer up.
    ///
    /// The hand-over happens inside `begin_resync_request_or_note_drop` under
    /// one lock, which is what makes it safe; a second note here would be both
    /// redundant and a TOCTOU hole.
    ///
    /// # What this test does and does not establish
    ///
    /// It covers the OBSERVABLE requirement at a window boundary: exactly one
    /// repair crosses it, and no chain keeps a node-wide slot afterwards. It
    /// does NOT force the A-vs-B race itself. I tried, and measured that it
    /// does not: with `start_paused`, a drop issued before the boundary is
    /// refused (A still owns the window) and one issued after finds A has
    /// already re-reserved, so the `WindowHeld` arm is never entered — I
    /// confirmed that by collapsing that arm into a plain retry, which this
    /// test still passed. Winning the race needs the sibling to reserve in the
    /// instant between A's timer firing and A's task being polled, which the
    /// runtime does not let a test schedule.
    ///
    /// So the arm's own behaviour is pinned by
    /// `the_window_held_arm_exits_rather_than_retrying`, and the generational
    /// half by
    /// `a_stale_chain_cannot_consume_a_newer_reservations_swallowed_drop` in
    /// `ring::interest`. Recording this rather than leaving the mutation gap
    /// silent, because a behavioural test that cannot fail for the reason it
    /// names is worse than an honest pin.
    #[tokio::test(start_paused = true)]
    async fn a_sibling_taking_the_window_inherits_the_debt_and_the_old_chain_exits() {
        let (op_manager, mut notification_rx, _guard) =
            build_queue_full_test_node("sibling-takeover-5510").await;
        let key = ContractKey::from_id_and_code(
            ContractInstanceId::new([85u8; 32]),
            CodeHash::new([86u8; 32]),
        );
        let _held = hold_contracts(&op_manager, &[key]).await;
        let target: SocketAddr = "127.0.0.1:13800".parse().unwrap();

        // Chain A: granted, holds a slot, then a second drop inside its window
        // leaves it owing a repair.
        for _ in 0..2 {
            send_dropped_broadcast_resync_request(
                &op_manager,
                key,
                target,
                Transaction::new::<UpdateMsg>(),
                DroppedBroadcastReason::RateLimited,
            )
            .await;
        }
        assert_eq!(
            drain_resync_targets(&mut notification_rx, key),
            vec![target],
            "only the FIRST drop may emit; the second is inside A's window"
        );
        let slots_with_a = op_manager.interest_manager.outstanding_resync_retries();
        assert!(slots_with_a > 0, "precondition: chain A must hold a slot");

        // Run to just BEFORE A's window closes, discarding the #4862
        // re-deliveries. Those re-send the request A already emitted, so they
        // are not repairs and counting them would drown the signal.
        for _ in 0..58 {
            tokio::time::advance(Duration::from_millis(500)).await;
            tokio::task::yield_now().await;
            let _ = drain_resync_targets(&mut notification_rx, key);
        }

        // A genuine new drop right at the boundary. Whether it is refused (A
        // still owns the window and inherits this drop too) or granted (it
        // becomes sibling B and A must hand its debt over and exit), the
        // observable requirement is the same and is what this asserts.
        send_dropped_broadcast_resync_request(
            &op_manager,
            key,
            target,
            Transaction::new::<UpdateMsg>(),
            DroppedBroadcastReason::RateLimited,
        )
        .await;

        // Cross the boundary. Stop 1s past it: a fresh reservation taken here
        // has its first #4862 retry >= 1.6s out, so it cannot contaminate the
        // count — the same narrow window the deadline test relies on.
        let mut emitted = Vec::new();
        for _ in 0..4 {
            tokio::time::advance(Duration::from_millis(500)).await;
            tokio::task::yield_now().await;
            emitted.extend(drain_resync_targets(&mut notification_rx, key));
        }

        assert_eq!(
            emitted.len(),
            1,
            "exactly ONE repair may cross the window boundary, whichever chain \
             ends up owning the reservation. Two would be the #4251 \
             amplification the throttle exists to bound — the shape a hand-over \
             that ALSO left the old chain emitting would produce. Zero would be \
             the silent loss #5510 exists to close, which is what a hand-over \
             that dropped the debt would produce. Got {emitted:?}"
        );

        // And the chains must drain: nothing may hold a node-wide slot forever
        // because it lost a race for a reservation.
        let mut ticks = 0;
        while op_manager.interest_manager.outstanding_resync_retries() > 0 && ticks < 2000 {
            tokio::time::advance(Duration::from_millis(500)).await;
            tokio::task::yield_now().await;
            let _ = drain_resync_targets(&mut notification_rx, key);
            ticks += 1;
        }
        assert_eq!(
            op_manager.interest_manager.outstanding_resync_retries(),
            0,
            "every chain must eventually release its slot; a chain that lost the \
             window to a sibling must exit rather than retry against a \
             reservation it does not hold"
        );
    }

    /// #5510 review round 3 (T3): the tokio liveness backstop fires when the
    /// injected clock never advances.
    ///
    /// `wait_until_reservation_close` loops on the THROTTLE's clock, which a
    /// node can be configured to drive from an injected `TimeSource`. If that
    /// clock stalls — a frozen mock, a suspended host, a DST discontinuity —
    /// the loop's own condition can never become true, and without a second
    /// clock to bound it the task would poll forever while holding one of the
    /// 256 node-wide slots. The backstop measures `tokio::time::Instant`
    /// instead, which is a different clock on purpose.
    ///
    /// Frozen throttle clock, advancing tokio clock: the wait must give up
    /// within the backstop rather than spin.
    #[tokio::test(start_paused = true)]
    async fn the_wait_gives_up_when_the_injected_clock_never_advances() {
        let (op_manager, _notification_rx, _guard) = build_queue_full_test_node_with_clock(
            "coalesced-backstop-5510",
            Some(
                Arc::new(crate::util::time_source::SharedMockTimeSource::new())
                    as crate::util::time_source::DynTimeSource,
            ),
        )
        .await;
        let key = ContractKey::from_id_and_code(
            ContractInstanceId::new([87u8; 32]),
            CodeHash::new([88u8; 32]),
        );

        // A deadline the frozen clock can never reach.
        let unreachable = op_manager.interest_manager.now() + Duration::from_secs(3600);
        let waiter = tokio::spawn({
            let op_manager = op_manager.clone();
            async move {
                wait_until_reservation_close(
                    &op_manager,
                    key,
                    Transaction::new::<UpdateMsg>(),
                    unreachable,
                )
                .await
            }
        });

        // Advance tokio's clock only. The throttle's clock stays frozen, so the
        // loop condition never clears and only the backstop can end this.
        for _ in 0..80 {
            tokio::time::advance(Duration::from_millis(500)).await;
            tokio::task::yield_now().await;
            if waiter.is_finished() {
                break;
            }
        }

        assert!(
            waiter.is_finished(),
            "the wait must give up on the tokio backstop rather than poll \
             forever against a stalled injected clock — otherwise a frozen \
             clock leaks node-wide slots one chain at a time"
        );
        assert!(
            !waiter.await.expect("waiter must not panic"),
            "giving up must report FALSE, so the caller abandons the coalesced \
             emit instead of emitting against a window it never observed close"
        );
    }

    /// #5510 review round 3 (T1, pin): the `WindowHeld` arm must EXIT, not
    /// retry.
    ///
    /// A chain whose re-reservation is refused because a sibling now owns the
    /// window holds nothing. Retrying would burn one of the 256 node-wide slots
    /// waiting on a reservation it does not own, and the sibling — which does
    /// own it, and has the debt — is the one that will serve the repair. So the
    /// arm must return.
    ///
    /// This is a source pin because the race it describes cannot be scheduled
    /// from a test; see
    /// `a_sibling_taking_the_window_inherits_the_debt_and_the_old_chain_exits`
    /// for the measurement behind that claim.
    #[test]
    fn the_window_held_arm_exits_rather_than_retrying() {
        let src = include_str!("op_ctx_task.rs");
        let start = src
            .find("fn spawn_resync_followups(")
            .expect("spawn_resync_followups not found");
        let rest = &src[start + 1..];
        let end = [
            "\nasync fn ",
            "\nfn ",
            "\npub async fn ",
            "\npub fn ",
            "\npub(",
            "\nconst ",
            "\nstatic ",
            "\npub(crate) const ",
            "\npub(crate) static ",
        ]
        .iter()
        .filter_map(|kw| rest.find(kw))
        .min()
        .unwrap_or(rest.len());
        let body = &src[start..start + 1 + end];

        let arm = body
            .find("Err(ReserveRefusal::WindowHeld) => {")
            .expect("the sibling-held refusal must be its own arm, so it can be handled");
        // No closing paren: the variant carries a field now, so the old exact
        // match silently failed and ran `arm_body` to the end of the function.
        let arm_end = body[arm..]
            .find("Err(ReserveRefusal::GlobalCap")
            .map(|i| arm + i)
            .unwrap_or(body.len());
        let arm_body = &body[arm..arm_end];

        assert!(
            arm_body.contains("return;"),
            "the WindowHeld arm must RETURN: a sibling owns the window and holds \
             the debt, so retrying only burns a node-wide slot waiting on a \
             reservation this chain does not own"
        );
        assert!(
            !arm_body.contains("deliver = false;") && !arm_body.contains("dispatch_pending"),
            "the WindowHeld arm must not fall through into the retry shape used \
             by the GlobalCap arm — those are different situations: the cap \
             releases the window (so retrying can win it back), a sibling holds \
             it (so retrying cannot)"
        );
        // #5510 review round 3 (X4): the bound's return must hand the debt back.
        //
        // The debt is CONSUMED into `owed` before the bound is checked, so a
        // bare return abandons it — a drop swallowed late in the last served
        // window would vanish at t=90 with nothing holding the flag. Handing it
        // back rather than emitting one more time keeps the bound meaning what
        // it says: it caps slot occupancy, not repairs.
        let bound = body
            .find("if windows_served >= MAX_COALESCED_RESYNC_WINDOWS")
            .expect("the chain bound must exist");
        let bound_end = body[bound..]
            .find("\n            }")
            .map(|i| bound + i)
            .unwrap_or(body.len());
        assert!(
            body[bound..bound_end].contains("note_resync_debt_on_held_reservation"),
            "the chain-bound return must hand the consumed debt back to the \
             reservation; it was taken into `owed` before this check, so \
             returning without it drops a real repair on the floor at the exact \
             moment the chain stops looking (#5510 X4)"
        );

        assert!(
            !arm_body.contains("note_resync_drop_during_window"),
            "the hand-over happens inside begin_resync_request_or_note_drop, \
             under the same lock as the refusal. A second note here would be a \
             TOCTOU hole AND would make `note_debt` unobservable, which is how \
             the round-2 version hid this arm from testing entirely"
        );
    }

    /// #5510 review round 3 (X2): a chain whose request went unanswered keeps
    /// trying rather than exiting on "nothing swallowed".
    ///
    /// The responder runs its own 30s `resync_response_limiter` per (contract,
    /// peer). If our request lands late — the enqueue is a blocking
    /// `notify_node_event` — the coalesced request and its short retries can all
    /// fall inside ONE responder window and be rejected before the responder
    /// clears its cached summary of us. From this side nothing new is swallowed,
    /// so the old code exited satisfied while the divergence remained.
    ///
    /// The correlation record is consumed when a matching ResyncResponse
    /// arrives, so a record still outstanding means nothing came back. This
    /// harness never answers, so the record stays outstanding throughout, and
    /// the chain must keep going to its bound rather than stopping at the first
    /// window that swallowed nothing.
    #[tokio::test(start_paused = true)]
    async fn an_unanswered_repair_keeps_retrying_instead_of_exiting_satisfied() {
        let (op_manager, mut notification_rx, _guard) =
            build_queue_full_test_node("unanswered-retry-5510").await;
        let key = ContractKey::from_id_and_code(
            ContractInstanceId::new([91u8; 32]),
            CodeHash::new([92u8; 32]),
        );
        let _held = hold_contracts(&op_manager, &[key]).await;
        let target: SocketAddr = "127.0.0.1:14000".parse().unwrap();

        // One drop only. Nothing further is swallowed, so a chain that judged
        // solely on the flag would exit at the first window close.
        send_dropped_broadcast_resync_request(
            &op_manager,
            key,
            target,
            Transaction::new::<UpdateMsg>(),
            DroppedBroadcastReason::RateLimited,
        )
        .await;
        assert_eq!(
            drain_resync_targets(&mut notification_rx, key),
            vec![target],
            "the drop must emit its immediate request"
        );
        assert!(
            op_manager
                .ring
                .outstanding_resync_requests
                .is_outstanding(*key.id(), target),
            "precondition: the correlation record must be outstanding, or there \
             is no 'unanswered' signal for the chain to read"
        );

        // Past the first window close. The chain must still be alive: no
        // response has arrived, so its repair has not landed.
        for _ in 0..64 {
            tokio::time::advance(Duration::from_millis(500)).await;
            tokio::task::yield_now().await;
            let _ = drain_resync_targets(&mut notification_rx, key);
        }
        assert!(
            op_manager.interest_manager.outstanding_resync_retries() > 0,
            "the chain must NOT have exited at the first window close: its \
             request was never answered, so the divergence it exists to repair \
             is still there and the responder's own 30s throttle is the likely \
             reason (#5510 X2)"
        );

        // And it must still terminate — retrying is bounded, not indefinite.
        let mut ticks = 0;
        while op_manager.interest_manager.outstanding_resync_retries() > 0 && ticks < 4000 {
            tokio::time::advance(Duration::from_millis(500)).await;
            tokio::task::yield_now().await;
            let _ = drain_resync_targets(&mut notification_rx, key);
            ticks += 1;
        }
        assert_eq!(
            op_manager.interest_manager.outstanding_resync_retries(),
            0,
            "retrying an unanswered repair must stay bounded by windows_served \
             and attempts — otherwise X2's fix trades a lost repair for a leaked \
             slot"
        );
    }

    /// #5510 review round 3 (X1): a pair whose contract is over the global emit
    /// cap spawns ONE chain, not one per drop.
    ///
    /// The cap path used to `cancel` the per-pair reservation, releasing the
    /// window. That looked like the considerate thing to do — the sender could
    /// then retry as soon as tokens refilled — and it was, right up until a
    /// follow-up chain existed. With the window released, every later drop on
    /// the pair reaches the cap afresh, is refused afresh, and spawns a chain of
    /// its own, so one hot pair drains the 256 node-wide slots in seconds while
    /// all of them wait on the same empty bucket.
    ///
    /// Keeping the reservation makes those later drops `WindowHeld`, so they
    /// hand their debt to the chain already carrying it. This asserts the slot
    /// count, because that is the resource the old shape exhausted.
    #[tokio::test(start_paused = true)]
    async fn a_capped_pair_spawns_one_chain_however_many_drops_arrive() {
        let (op_manager, mut notification_rx, _guard) =
            build_queue_full_test_node("capped-pair-one-chain-5510").await;
        let key = ContractKey::from_id_and_code(
            ContractInstanceId::new([89u8; 32]),
            CodeHash::new([90u8; 32]),
        );
        let _held = hold_contracts(&op_manager, &[key]).await;
        let target: SocketAddr = "127.0.0.1:13900".parse().unwrap();

        // Empty the contract's emit bucket so every repair attempt is refused.
        while op_manager
            .ring
            .resync_emit_limiter
            .check_and_record(*key.id())
        {}

        let before = op_manager.interest_manager.outstanding_resync_retries();
        for _ in 0..12 {
            send_dropped_broadcast_resync_request(
                &op_manager,
                key,
                target,
                Transaction::new::<UpdateMsg>(),
                DroppedBroadcastReason::RateLimited,
            )
            .await;
            tokio::task::yield_now().await;
        }

        let spawned = op_manager.interest_manager.outstanding_resync_retries() - before;
        assert_eq!(
            spawned, 1,
            "twelve drops on one capped pair must produce ONE chain, not one \
             each: gate 1 is per (contract, sender), and keeping the reservation \
             on a cap refusal is what makes the later drops hit it. Got \
             {spawned} chains, which is the slot-drain shape X1 removed"
        );
        assert!(
            drain_resync_targets(&mut notification_rx, key).is_empty(),
            "and nothing may be emitted while the cap refuses"
        );
    }

    /// #5510 review round 3 (T2): a chain that is refused on every attempt
    /// still terminates and gives its slot back.
    ///
    /// [`MAX_COALESCED_RESYNC_WINDOWS`] counts SERVED windows, so a chain that
    /// never serves one would never reach it — that is exactly why
    /// [`MAX_COALESCED_RESYNC_ATTEMPTS`] exists as a separate bound. Without it
    /// a pair whose contract sits permanently over the global emit cap holds one
    /// of the 256 node-wide slots indefinitely, which is the refreshable-cap
    /// shape `.claude/rules/code-style.md` forbids.
    ///
    /// The cap is kept drained for the whole run, so every re-reservation is
    /// refused and `windows_served` never advances past its starting value.
    #[tokio::test(start_paused = true)]
    async fn a_chain_refused_on_every_attempt_terminates_and_frees_its_slot() {
        let (op_manager, mut notification_rx, _guard) =
            build_queue_full_test_node("attempts-bound-5510").await;
        let key = ContractKey::from_id_and_code(
            ContractInstanceId::new([83u8; 32]),
            CodeHash::new([84u8; 32]),
        );
        let _held = hold_contracts(&op_manager, &[key]).await;
        let target: SocketAddr = "127.0.0.1:13700".parse().unwrap();

        // First drop: granted, spawns a chain holding a #4862 slot.
        send_dropped_broadcast_resync_request(
            &op_manager,
            key,
            target,
            Transaction::new::<UpdateMsg>(),
            DroppedBroadcastReason::RateLimited,
        )
        .await;
        assert_eq!(
            drain_resync_targets(&mut notification_rx, key),
            vec![target],
            "the first drop must emit immediately"
        );
        assert!(
            op_manager.interest_manager.outstanding_resync_retries() > 0,
            "precondition: a chain must be holding a slot"
        );

        // A second drop inside the window records the debt the chain will try,
        // and keep trying, to serve.
        send_dropped_broadcast_resync_request(
            &op_manager,
            key,
            target,
            Transaction::new::<UpdateMsg>(),
            DroppedBroadcastReason::RateLimited,
        )
        .await;

        // Keep the bucket empty for the whole run so every re-reservation is
        // refused. Draining on each tick is what makes this a test of the
        // ATTEMPTS bound rather than of the window bound.
        let mut ticks = 0;
        while op_manager.interest_manager.outstanding_resync_retries() > 0 && ticks < 4000 {
            while op_manager
                .ring
                .resync_emit_limiter
                .check_and_record(*key.id())
            {}
            tokio::time::advance(Duration::from_millis(500)).await;
            tokio::task::yield_now().await;
            let _ = drain_resync_targets(&mut notification_rx, key);
            ticks += 1;
        }

        assert_eq!(
            op_manager.interest_manager.outstanding_resync_retries(),
            0,
            "a chain refused on every attempt MUST still terminate and return its \
             node-wide slot; without the attempts bound it re-reserves forever, \
             and 256 such pairs starve every later pair of both the #4862 \
             re-delivery and the trailing repair"
        );
    }

    /// #5510 trailing edge, the case the throttle used to swallow silently.
    ///
    /// Sequence, all on one `(contract, sender)`:
    ///
    /// 1. a drop emits the immediate `ResyncRequest`;
    /// 2. a SECOND drop inside the window emits nothing at the time — that is the
    ///    #4251 bound working as designed, and before this change it was also the
    ///    end of the story: nothing ever re-sent it;
    /// 3. at the reservation deadline exactly ONE coalesced `ResyncRequest` fires.
    ///
    /// The count is taken in two windows rather than as a single total, and that
    /// is what makes it a real test of the DEADLINE rather than of the emit. A
    /// total-only assertion passes just as happily if the trailing emit fires
    /// immediately. Verified against the mutation: deleting the
    /// `while ... < reservation_deadline` wait makes this FAIL, because the sweep
    /// then consumes the flag before step 2 has set it and no fourth request ever
    /// appears.
    ///
    /// The 2 requests counted before the deadline are the #4862 retry's, which is
    /// unrelated machinery: no `ResyncResponse` can arrive in this harness, so its
    /// correlation gate never trips and it uses its full budget.
    #[tokio::test(start_paused = true)]
    async fn drop_swallowed_by_throttle_window_emits_one_coalesced_resync_at_the_deadline() {
        let (op_manager, mut notification_rx, _guard) =
            build_queue_full_test_node("coalesced-resync-5510-deadline").await;
        let key = ContractKey::from_id_and_code(
            ContractInstanceId::new([21u8; 32]),
            CodeHash::new([22u8; 32]),
        );
        // The chain's coalesced emit is gated on holding the contract, so the
        // node must actually hold it or this measures the gate, not the repair.
        let _held = hold_contracts(&op_manager, &[key]).await;
        let sender_addr: SocketAddr = "127.0.0.1:12400".parse().unwrap();

        let drop_once = |bytes: Vec<u8>| {
            drive_relay_broadcast_to(
                &op_manager,
                Transaction::new::<UpdateMsg>(),
                key,
                DeltaOrFullState::Delta(bytes),
                Vec::new(),
                sender_addr,
                crate::ring::broadcast_coverage::CoveredPeers::empty(),
            )
        };

        // (1) First drop → the immediate ResyncRequest.
        let r1 = drop_once(vec![1, 2, 3]).await;
        assert!(
            r1.as_ref()
                .err()
                .is_some_and(|e| e.is_contract_queue_full()),
            "expected a queue-full error from the stand-in handler, got {r1:?}"
        );
        assert_eq!(
            drain_resync_targets(&mut notification_rx, key),
            vec![sender_addr],
            "the first drop must emit its immediate ResyncRequest"
        );

        // (2) Second drop, different bytes so it is not a dedup hit, inside the
        //     held window → refused by the throttle, nothing emitted NOW.
        let r2 = drop_once(vec![4, 5, 6]).await;
        assert!(
            r2.as_ref()
                .err()
                .is_some_and(|e| e.is_contract_queue_full()),
            "second broadcast should still hit queue-full, got {r2:?}"
        );
        assert!(
            drain_resync_targets(&mut notification_rx, key).is_empty(),
            "a second drop inside the throttle window MUST NOT emit immediately — \
             that is the #4251 amplification bound"
        );

        // Run to just BEFORE the reservation deadline. Only the #4862 retries
        // may fire in here; the trailing sweep must not have run yet.
        let mut before_deadline = Vec::new();
        for _ in 0..58 {
            tokio::time::advance(Duration::from_millis(500)).await;
            tokio::task::yield_now().await;
            before_deadline.extend(drain_resync_targets(&mut notification_rx, key));
        }
        assert_eq!(
            before_deadline.len(),
            QUEUE_FULL_RESYNC_MAX_RETRIES as usize,
            "before the deadline only the #4862 retries may fire ({} of them); the \
             coalesced repair must WAIT for the window to close, got {:?}",
            QUEUE_FULL_RESYNC_MAX_RETRIES,
            before_deadline
        );

        // (3) Cross the deadline. Stop 1s past it: a fresh reservation is taken
        //     by the coalesced emit, and ITS first retry is >= 1.6s out, so it
        //     cannot contaminate this count.
        let mut at_deadline = Vec::new();
        for _ in 0..4 {
            tokio::time::advance(Duration::from_millis(500)).await;
            tokio::task::yield_now().await;
            at_deadline.extend(drain_resync_targets(&mut notification_rx, key));
        }
        assert_eq!(
            at_deadline,
            vec![sender_addr],
            "exactly ONE coalesced ResyncRequest must fire when the window closes \
             — one full-state response repairs every drop the window swallowed, so \
             one request covers any number of them (#5510)"
        );
    }

    /// #5510 review finding 10: at the node-wide slot cap the immediate
    /// `ResyncRequest` still goes out and NO follow-up task is spawned.
    ///
    /// The primitive (`try_reserve_resync_retry_slot` returning `None` at cap)
    /// has its own test; this covers the BRANCH that reads it, which is where a
    /// mistake would matter — an early `return` moved above the immediate
    /// dispatch would silently drop the repair entirely on exactly the loaded
    /// node that needs it most, and the primitive's test would stay green.
    #[tokio::test(start_paused = true)]
    async fn at_the_slot_cap_the_immediate_resync_still_sends_and_nothing_spawns() {
        let (op_manager, mut notification_rx, _guard) =
            build_queue_full_test_node("slot-cap-branch-5510").await;
        let key = ContractKey::from_id_and_code(
            ContractInstanceId::new([61u8; 32]),
            CodeHash::new([62u8; 32]),
        );
        let sender_addr: SocketAddr = "127.0.0.1:12900".parse().unwrap();

        // Hold every slot, so the branch under test is the one taken.
        let held: Vec<_> = (0..crate::ring::interest::MAX_OUTSTANDING_QUEUE_FULL_RESYNC_RETRIES)
            .map(|_| {
                op_manager
                    .interest_manager
                    .try_reserve_resync_retry_slot()
                    .expect("slots must be available up to the cap")
            })
            .collect();
        assert!(
            op_manager
                .interest_manager
                .try_reserve_resync_retry_slot()
                .is_none(),
            "precondition: the cap must now refuse"
        );
        let at_cap = op_manager.interest_manager.outstanding_resync_retries();

        let r1 = drive_relay_broadcast_to(
            &op_manager,
            Transaction::new::<UpdateMsg>(),
            key,
            DeltaOrFullState::Delta(vec![1, 2, 3]),
            Vec::new(),
            sender_addr,
            crate::ring::broadcast_coverage::CoveredPeers::empty(),
        )
        .await;
        assert!(
            r1.as_ref()
                .err()
                .is_some_and(|e| e.is_contract_queue_full()),
            "expected a queue-full error from the stand-in handler, got {r1:?}"
        );

        assert_eq!(
            drain_resync_targets(&mut notification_rx, key),
            vec![sender_addr],
            "at the slot cap the IMMEDIATE ResyncRequest must still be sent — the \
             cap withholds the follow-ups, never the repair itself"
        );
        assert_eq!(
            op_manager.interest_manager.outstanding_resync_retries(),
            at_cap,
            "no follow-up task may be spawned at the cap"
        );

        // Nothing further may appear: with no task there is no re-delivery and
        // no trailing sweep. Run well past a full reservation window.
        for _ in 0..70 {
            tokio::time::advance(Duration::from_millis(500)).await;
            tokio::task::yield_now().await;
        }
        assert!(
            drain_resync_targets(&mut notification_rx, key).is_empty(),
            "with no follow-up task, neither the #4862 re-delivery nor the #5510 \
             trailing sweep may fire"
        );

        drop(held);
    }

    /// #5510 review finding 1: the follow-up chain is bounded to
    /// [`MAX_COALESCED_RESYNC_WINDOWS`] consecutive windows.
    ///
    /// Without the bound a chain re-reserves for as long as its windows keep
    /// swallowing drops, so a pair under sustained load holds its node-wide slot
    /// forever; 256 such pairs — reachable by legitimately busy traffic, not
    /// only by an attacker — permanently starve every later pair of both the
    /// #4862 re-delivery and the trailing repair. That is the refreshable-cap
    /// shape `.claude/rules/code-style.md` forbids.
    ///
    /// # Why this is a source pin and not a behavioural test
    ///
    /// A behavioural test cannot reach the bound with today's constants, and I
    /// measured that rather than assuming it. Two reasons, both structural:
    ///
    /// 1. Advancing a chain past a window needs a GRANTED coalesced emit, which
    ///    needs a token from the global per-contract cap — `EMIT_BURST` 2,
    ///    refilling once per `EMIT_REFILL_INTERVAL` (60s) against a 30s
    ///    re-reservation. A chain therefore runs out of tokens long before three
    ///    granted windows, and terminates on the cap instead.
    /// 2. Feeding drops in to keep the flag set does not extend the existing
    ///    chain: a drop that arrives when no reservation is held takes its own
    ///    reservation and spawns a SEPARATE chain, so the injected traffic
    ///    creates siblings rather than consecutive swallowed windows for one.
    ///
    /// So the bound is defence in depth — it binds only if the emit cap is ever
    /// loosened or another caller reaches this loop — and a behavioural test
    /// asserting termination would pass whether or not the bound existed. It did:
    /// the first version of this test passed with `windows_used >= MAX` mutated
    /// to `false`, which is exactly the vacuous pin this repo's testing rules
    /// warn about. Pinning the mechanism is the honest alternative; deleting the
    /// check fails this test.
    ///
    /// `a_cap_refused_coalesced_emit_keeps_its_debt_and_the_chain_terminates`
    /// covers the termination that IS reachable today.
    #[test]
    fn the_followup_chain_is_bounded_by_a_window_count() {
        let src = include_str!("op_ctx_task.rs");
        let start = src
            .find("fn spawn_resync_followups(")
            .expect("spawn_resync_followups not found");
        let rest = &src[start + 1..];
        let end = [
            "\nasync fn ",
            "\nfn ",
            "\npub async fn ",
            "\npub fn ",
            "\npub(",
            // Item openers too (review round 3, C4): consts and statics sit
            // between these fns, and their doc comments would otherwise fall
            // inside the slice.
            "\nconst ",
            "\nstatic ",
            "\npub(crate) const ",
            "\npub(crate) static ",
        ]
        .iter()
        .filter_map(|kw| rest.find(kw))
        .min()
        .unwrap_or(rest.len());
        let body: String = src[start..start + 1 + end]
            .chars()
            .filter(|c| !c.is_whitespace())
            .collect();

        assert!(
            body.contains(
                "letmutwindows_served:u32=matchstart{FollowupStart::Reserved{..}=>1,\
                 FollowupStart::OwedOnly{..}=>0,};"
                    .replace(' ', "")
                    .as_str()
            ),
            "the chain must COUNT the windows it has SERVED, starting at 1 only \
             when the caller holds a granted reservation — a chain that starts \
             owing has served none, and starting it at 1 would spend a window it \
             never got"
        );
        let compare = body
            .find(
                "ifwindows_served>=MAX_COALESCED_RESYNC_WINDOWS||\
                 attempts>=MAX_COALESCED_RESYNC_ATTEMPTS{"
                    .replace(' ', "")
                    .as_str(),
            )
            .expect(
                "the chain must bound BOTH its served windows and its total \
                 attempts — without the first it re-reserves without limit and \
                 holds its node-wide slot forever (#5510 finding 1); without the \
                 second a chain that is only ever refused never reaches the \
                 window bound at all, and holds the slot just as long (review \
                 round 2)",
            );
        assert!(
            body[compare..].contains("return;"),
            "reaching a bound must RETURN, so the slot guard drops and the slot \
             goes back to the node-wide pool"
        );

        let grant = body
            .find("Ok((next_deadline,next_correlated))=>{")
            .expect("the coalesced re-reservation must match on its Result");
        let window_held = body
            .find("Err(ReserveRefusal::WindowHeld)=>{")
            .expect("a sibling-held window must be a distinct arm");
        let increment = body
            .find("windows_served+=1;")
            .expect("the window count must be incremented, or the bound can never fire");
        assert!(
            increment > grant && increment < window_held,
            "the increment ({increment}) must sit INSIDE the granted arm \
             ({grant}..{window_held}) — counting a REFUSAL as a served window is \
             the review-round-2 defect: the global emit cap refills once per 60s \
             against a 30s re-reservation, so two refusals exhausted a bound of 3 \
             and the debt was dropped at t=90 having never been served once"
        );
        assert_eq!(
            body.matches("windows_served+=1;").count(),
            1,
            "exactly one increment: a second one on a refusal path would \
             reintroduce the same defect"
        );
        assert!(
            increment > compare,
            "the increment ({increment}) must come AFTER the bound check \
             ({compare}) — incrementing first would let the chain serve one fewer \
             window than the constant names"
        );
        assert!(
            body.contains("attempts+=1;"),
            "the attempt count must be incremented every pass, or its bound can \
             never fire on a chain that is only ever refused"
        );
    }

    /// #5510 review finding 11: when the GLOBAL per-contract emit cap refuses
    /// the coalesced request, the chain gives up within its window bound and
    /// releases its node-wide slot.
    ///
    /// This is the path finding 16 made non-trivial. The flag is consumed at the
    /// window's close, BEFORE the gates run, so a naive implementation that
    /// #5510 review round 2: the trailing coalesced emit re-checks the HOSTING
    /// gate, and does so only for chains descended from a rate-limiter drop.
    ///
    /// `node.rs` emits the rate-limited repair only when
    /// `should_summarize_or_broadcast` holds, because asking a peer to
    /// full-state us a contract we do not hold is pure waste — it costs the
    /// peer a WASM summarize and us the transfer, and we discard the result.
    /// A chain lives for tens of seconds across several windows, so a contract
    /// evicted after the first emit would otherwise keep being requested by a
    /// gate that ran once, long ago.
    ///
    /// The split under test is IMMEDIATE vs CHAIN, not one origin vs the other
    /// (review round 3, correcting round 2). The immediate queue-full emit
    /// (#4857) has never been hosting-gated and still is not — that argument is
    /// about a path that predates this PR. It does not extend to the chain,
    /// which is new code with a ~180s lifetime, so EVERY coalesced emit is
    /// gated whatever it descended from.
    ///
    /// Written as a discriminating pair for that reason: two contracts, one
    /// driven with each origin, identical otherwise, on a node that holds
    /// neither. Both must emit immediately and neither must emit at the
    /// window's close. If the gate were absent, both would emit twice; if it
    /// were (wrongly) moved onto the immediate emit as well, neither would emit
    /// at all. Only the intended split produces one emit each.
    #[tokio::test(start_paused = true)]
    async fn the_coalesced_emit_rechecks_hosting_for_every_origin() {
        let (op_manager, mut notification_rx, _guard) =
            build_queue_full_test_node("coalesced-hosting-recheck-5510").await;
        // A contract EACH, not two senders on one: the global per-contract emit
        // cap is `EMIT_BURST` 2, which one immediate plus one coalesced emit
        // exactly exhausts. Sharing a contract between the two arms starves the
        // second coalesced emit on the cap, and the test then reads that as the
        // hosting gate — passing for entirely the wrong reason on the arm that
        // is supposed to stay silent.
        let key_gated = ContractKey::from_id_and_code(
            ContractInstanceId::new([71u8; 32]),
            CodeHash::new([72u8; 32]),
        );
        let key_ungated = ContractKey::from_id_and_code(
            ContractInstanceId::new([73u8; 32]),
            CodeHash::new([74u8; 32]),
        );
        let target: SocketAddr = "127.0.0.1:13400".parse().unwrap();

        // The premise, asserted rather than assumed: this node holds NEITHER
        // contract. Without these the test would pass just as happily on a node
        // that does, having exercised nothing.
        for key in [key_gated, key_ungated] {
            assert!(
                !op_manager.ring.should_summarize_or_broadcast(&key),
                "fixture premise: the node must not hold {key}, or the re-check \
                 under test never fires"
            );
        }

        // First drop on each pair: both reserve and emit immediately. The entry
        // point is deliberately NOT gated — `node.rs` already decided.
        // Two drops on each pair. The first reserves and emits immediately — the
        // entry point is deliberately NOT gated, because `node.rs` already
        // decided. The second is refused by the held window and records the debt
        // the chain serves at its close.
        for _ in 0..2 {
            for (key, reason) in [
                (key_gated, DroppedBroadcastReason::RateLimited),
                (key_ungated, DroppedBroadcastReason::QueueFull),
            ] {
                send_dropped_broadcast_resync_request(
                    &op_manager,
                    key,
                    target,
                    Transaction::new::<UpdateMsg>(),
                    reason,
                )
                .await;
            }
        }
        let immediate = drain_resync_events(&mut notification_rx);
        assert_eq!(
            immediate,
            vec![(key_gated, target), (key_ungated, target)],
            "each pair must emit exactly its immediate request, in call order: \
             the hosting re-check belongs to the trailing emit, not the entry \
             point, and the second drop is inside the held window"
        );

        // Cross the reservation deadline, discarding the #4862 re-deliveries
        // that fire before it — those are unrelated machinery and fire for both
        // pairs alike.
        for _ in 0..58 {
            tokio::time::advance(Duration::from_millis(500)).await;
            tokio::task::yield_now().await;
            let _ = drain_resync_events(&mut notification_rx);
        }
        let mut at_deadline = Vec::new();
        for _ in 0..4 {
            tokio::time::advance(Duration::from_millis(500)).await;
            tokio::task::yield_now().await;
            at_deadline.extend(drain_resync_events(&mut notification_rx));
        }
        let gated_emits: Vec<_> = at_deadline
            .iter()
            .filter_map(|(k, t)| (*k == key_gated).then_some(*t))
            .collect();
        let ungated_emits: Vec<_> = at_deadline
            .iter()
            .filter_map(|(k, t)| (*k == key_ungated).then_some(*t))
            .collect();

        assert!(
            gated_emits.is_empty(),
            "the rate-limiter-origin chain must re-check hosting and abandon its \
             coalesced emit for a contract this node does not hold, got \
             {gated_emits:?}"
        );
        assert!(
            ungated_emits.is_empty(),
            "the QUEUE-FULL chain must abandon its coalesced emit too. #4857's \
             hosting exemption covers its IMMEDIATE emit, which this test already \
             observed firing; extending that exemption to a chain that re-emits \
             for ~180s would be inventing a new behaviour, not preserving an old \
             one. Got {ungated_emits:?}"
        );
    }

    /// returned on refusal would drop the repair silently — and it is not a
    /// corner: the emit cap refills once per 60s while re-reservation runs once
    /// per 30s, so refusal is the common case. The chain therefore keeps the
    /// debt and retries, which makes the WINDOW BOUND (finding 1) the thing that
    /// has to stop it. Both properties are asserted here: nothing is emitted
    /// while the cap refuses, and the slot comes back rather than being held for
    /// the life of the node.
    #[tokio::test(start_paused = true)]
    async fn a_cap_refused_coalesced_emit_keeps_its_debt_and_the_chain_terminates() {
        let (op_manager, mut notification_rx, _guard) =
            build_queue_full_test_node("coalesced-cap-refused-5510").await;
        let key = ContractKey::from_id_and_code(
            ContractInstanceId::new([63u8; 32]),
            CodeHash::new([64u8; 32]),
        );
        let _held = hold_contracts(&op_manager, &[key]).await;
        let sender_addr: SocketAddr = "127.0.0.1:13100".parse().unwrap();

        // Drop 1: takes the reservation, spends one of the contract's two emit
        // tokens, and spawns the chain.
        let r1 = drive_relay_broadcast_to(
            &op_manager,
            Transaction::new::<UpdateMsg>(),
            key,
            DeltaOrFullState::Delta(vec![1, 2, 3]),
            Vec::new(),
            sender_addr,
            crate::ring::broadcast_coverage::CoveredPeers::empty(),
        )
        .await;
        assert!(
            r1.as_ref()
                .err()
                .is_some_and(|e| e.is_contract_queue_full()),
            "expected a queue-full error, got {r1:?}"
        );
        assert_eq!(
            drain_resync_targets(&mut notification_rx, key),
            vec![sender_addr],
            "the immediate ResyncRequest must fire"
        );
        assert_eq!(
            op_manager.interest_manager.outstanding_resync_retries(),
            1,
            "the chain must have taken a slot"
        );

        // Drain the contract's REMAINING emit token directly, so every later
        // coalesced attempt is refused by the global cap on this frozen clock.
        while op_manager
            .ring
            .resync_emit_limiter
            .check_and_record(*key.id())
        {}

        // Drop 2 inside the held window: throttled, flag set, nothing emitted.
        let r2 = drive_relay_broadcast_to(
            &op_manager,
            Transaction::new::<UpdateMsg>(),
            key,
            DeltaOrFullState::Delta(vec![4, 5, 6]),
            Vec::new(),
            sender_addr,
            crate::ring::broadcast_coverage::CoveredPeers::empty(),
        )
        .await;
        assert!(
            r2.as_ref()
                .err()
                .is_some_and(|e| e.is_contract_queue_full()),
            "expected a queue-full error, got {r2:?}"
        );
        assert!(
            drain_resync_targets(&mut notification_rx, key).is_empty(),
            "a second drop inside the window must not emit immediately"
        );

        // Phase 1 — the first window closes at t=30s and the coalesced emit is
        // refused, because the contract has no emit token. Nothing may go out
        // between the close and the cap's 60s refill: an emit past the cap is
        // the #4251 storm the cap exists to bound.
        //
        // The two emits before t=30s are the #4862 re-deliveries of the ALREADY
        // authorized initial request, which deliberately consult no gate; they
        // are drained here so the phase-2 assertion sees only new emits.
        for _ in 0..64 {
            tokio::time::advance(Duration::from_millis(500)).await;
            tokio::task::yield_now().await;
            drain_resync_targets(&mut notification_rx, key);
        }
        let mut during_refusal = Vec::new();
        for _ in 0..50 {
            tokio::time::advance(Duration::from_millis(500)).await;
            tokio::task::yield_now().await;
            during_refusal.extend(drain_resync_targets(&mut notification_rx, key));
        }
        assert!(
            during_refusal.is_empty(),
            "while the global emit cap refuses, the chain must emit NOTHING. \
             Got {during_refusal:?}"
        );
        assert!(
            op_manager.interest_manager.outstanding_resync_retries() > 0,
            "and it must still be HOLDING its debt rather than having given up — \
             consuming the flag at the window close and returning on the refusal \
             would lose the repair, and with the cap refusing 1/60s against a \
             1/30s re-reservation that is the common case, not a corner (#5510 \
             finding 16)"
        );

        // Phase 2 — the cap refills at t=60s and the remembered debt is finally
        // paid: the coalesced request goes out, and the chain then terminates
        // and gives its node-wide slot back.
        let mut after_refill = Vec::new();
        for _ in 0..200 {
            if op_manager.interest_manager.outstanding_resync_retries() == 0 {
                break;
            }
            tokio::time::advance(Duration::from_millis(500)).await;
            tokio::task::yield_now().await;
            after_refill.extend(drain_resync_targets(&mut notification_rx, key));
        }
        assert!(
            !after_refill.is_empty(),
            "once the cap refills, the debt remembered across the refusal MUST be \
             paid — otherwise a drop swallowed by a window whose coalesced emit \
             was capped is lost to anti-entropy (#5510 finding 16)"
        );
        assert_eq!(
            op_manager.interest_manager.outstanding_resync_retries(),
            0,
            "the chain MUST terminate within MAX_COALESCED_RESYNC_WINDOWS and \
             release its node-wide slot — holding it indefinitely is how 256 \
             pairs would starve every later pair (#5510 finding 1)"
        );
    }

    /// #5510 negative control for the test above: with NO drop swallowed by the
    /// window, nothing extra fires when it closes.
    ///
    /// Without this, the deadline test would still pass if the sweep emitted
    /// unconditionally — which would be a new unthrottled emission per window per
    /// pair, exactly the #4251 shape the throttle exists to prevent.
    #[tokio::test(start_paused = true)]
    async fn no_drop_during_the_window_emits_nothing_when_it_closes() {
        let (op_manager, mut notification_rx, _guard) =
            build_queue_full_test_node("coalesced-resync-5510-control").await;
        let key = ContractKey::from_id_and_code(
            ContractInstanceId::new([23u8; 32]),
            CodeHash::new([24u8; 32]),
        );
        let sender_addr: SocketAddr = "127.0.0.1:12500".parse().unwrap();

        // ONE drop only. No second broadcast, so the window swallows nothing.
        let r1 = drive_relay_broadcast_to(
            &op_manager,
            Transaction::new::<UpdateMsg>(),
            key,
            DeltaOrFullState::Delta(vec![1, 2, 3]),
            Vec::new(),
            sender_addr,
            crate::ring::broadcast_coverage::CoveredPeers::empty(),
        )
        .await;
        assert!(
            r1.as_ref()
                .err()
                .is_some_and(|e| e.is_contract_queue_full()),
            "expected a queue-full error from the stand-in handler, got {r1:?}"
        );
        assert_eq!(
            drain_resync_targets(&mut notification_rx, key),
            vec![sender_addr],
            "the drop must emit its immediate ResyncRequest"
        );

        // Run past the reservation deadline.
        let mut emitted = Vec::new();
        for _ in 0..64 {
            tokio::time::advance(Duration::from_millis(500)).await;
            tokio::task::yield_now().await;
            emitted.extend(drain_resync_targets(&mut notification_rx, key));
        }
        assert_eq!(
            emitted.len(),
            QUEUE_FULL_RESYNC_MAX_RETRIES as usize,
            "with nothing swallowed by the window, the ONLY additional requests \
             may be the #4862 retries — a sweep that emits unconditionally would \
             be a new per-window unthrottled emission (#4251), got {emitted:?}"
        );
    }

    /// Issue #4857 behavioral reproduction (STREAMING full-state path): the
    /// large-room case the round-2 review flagged. A delivered streaming
    /// full-state broadcast caches the peer summary, so a `ContractQueueFull`
    /// drop here poisons the sender identically to the delta path — and the
    /// streaming Err arm previously had NO resync branch. `apply_streaming_broadcast`
    /// (steps 4-9 of `drive_relay_broadcast_to_streaming`) MUST now emit the same
    /// throttled `ResyncRequest`.
    ///
    /// On `origin/main` (and on the round-1 fix, which only patched the
    /// non-streaming driver) this FAILS: `first` is empty. With the round-2 fix
    /// the first drop emits one ResyncRequest and the second is throttled.
    ///
    /// Note (#4857 P2): as in the delta test, the trailing-retry task's first
    /// re-dispatch is ~1.6-2.4s out on the throttle's clock, well beyond this
    /// synchronous non-`start_paused` test's window, so the counts asserted here
    /// are unaffected.
    #[tokio::test]
    async fn queue_full_streaming_broadcast_emits_throttled_resync_request() {
        let (op_manager, mut notification_rx, _guard) =
            build_queue_full_test_node("queue-full-resync-4857-streaming").await;
        let key = ContractKey::from_id_and_code(
            ContractInstanceId::new([9u8; 32]),
            CodeHash::new([10u8; 32]),
        );
        let sender_addr: SocketAddr = "127.0.0.1:12200".parse().unwrap();

        // First streaming full-state → dropped queue-full → MUST emit a ResyncRequest.
        let r1 = apply_streaming_broadcast(
            &op_manager,
            Transaction::new::<UpdateMsg>(),
            key,
            vec![1, 2, 3],
            Vec::new(),
            sender_addr,
            crate::ring::broadcast_coverage::CoveredPeers::empty(),
        )
        .await;
        assert!(
            r1.as_ref()
                .err()
                .is_some_and(|e| e.is_contract_queue_full()),
            "expected a queue-full error from the stand-in handler, got {r1:?}"
        );
        assert_eq!(
            drain_resync_targets(&mut notification_rx, key),
            vec![sender_addr],
            "a queue-full streaming full-state drop MUST emit exactly one \
             ResyncRequest (heals #4857 on the streaming path)"
        );

        // Second streaming full-state with DIFFERENT bytes (not a dedup hit)
        // within the throttle window → still queue-full, ResyncRequest throttled.
        let r2 = apply_streaming_broadcast(
            &op_manager,
            Transaction::new::<UpdateMsg>(),
            key,
            vec![4, 5, 6],
            Vec::new(),
            sender_addr,
            crate::ring::broadcast_coverage::CoveredPeers::empty(),
        )
        .await;
        assert!(
            r2.as_ref()
                .err()
                .is_some_and(|e| e.is_contract_queue_full()),
            "second streaming broadcast should still hit queue-full, got {r2:?}"
        );
        assert!(
            drain_resync_targets(&mut notification_rx, key).is_empty(),
            "a second streaming queue-full drop within the throttle window MUST \
             NOT emit another ResyncRequest (bounds #4251 amplification)"
        );
    }

    /// A generic contract-exec rejection: `is_contract_exec_rejection() == true`
    /// (recorded by the merge backoff as the Invalid class) but NOT
    /// `is_invalid_update_rejection` (so the driver does not spawn summary-back)
    /// and NOT a wasm timeout. Used by the behavioral backoff-gate tests.
    #[cfg(test)]
    fn exec_reject_err(key: ContractKey) -> crate::contract::ExecutorError {
        crate::contract::ExecutorError::from(
            freenet_stdlib::client_api::RequestError::ContractError(
                freenet_stdlib::client_api::ContractError::update_exec_error(
                    key,
                    "contract merge failed (test poison)",
                ),
            ),
        )
    }

    /// A `ContractQueueFull` executor error (transient backpressure). Satisfies
    /// `is_contract_queue_full()` but NOT `is_contract_exec_rejection()`, so the
    /// backoff recording gate MUST skip it (queue-full is load, not poison).
    fn queue_full_err() -> crate::contract::ExecutorError {
        crate::contract::ExecutorError::other(crate::contract::ContractQueueFull)
    }

    /// What the exec-reject stand-in handler returns for a DELTA `UpdateQuery`.
    #[derive(Clone, Copy)]
    enum DeltaOutcome {
        /// Fail with a contract-exec rejection (recorded by the backoff).
        ExecReject,
        /// Fail with `ContractQueueFull` (NOT recorded — transient load).
        QueueFull,
        /// Succeed with `state_changed: true` — a clean DELTA merge that advanced
        /// state.
        Success,
        /// Succeed with `state_changed: false` — a NO-OP delta merge (e.g. an
        /// idempotent re-apply). The state did NOT advance, so the contract-side
        /// backoff clear must NOT fire (#4864 round-5 item 4).
        SuccessNoChange,
    }

    /// Build a node whose stand-in contract handler resolves every DELTA
    /// `UpdateQuery` per `delta_outcome` (fail-exec-reject / fail-queue-full /
    /// succeed) and SUCCEEDS every full-state `UpdateQuery`, while counting the
    /// `UpdateQuery` events that reach it. `GetSummaryQuery` (from the
    /// post-success proactive-summary spawn) is answered benignly. The counter
    /// lets a test assert whether the backoff PREVENTS executor invocation
    /// (#4864 review testing H1 / P1).
    async fn build_broadcast_test_node(
        id: &str,
        delta_outcome: DeltaOutcome,
    ) -> (
        Arc<OpManager>,
        crate::node::EventLoopNotificationsReceiver,
        std::sync::Arc<std::sync::atomic::AtomicUsize>,
        Box<dyn std::any::Any>,
    ) {
        use std::sync::atomic::Ordering;
        let config_args = crate::config::ConfigArgs {
            id: Some(id.to_string()),
            mode: Some(crate::contract::OperationMode::Local),
            ..Default::default()
        };
        let node_config =
            crate::node::NodeConfig::new(config_args.build().await.expect("build Config"))
                .await
                .expect("build NodeConfig");
        let (notification_rx, notification_tx) = crate::node::event_loop_notification_channel();
        let (ops_ch_channel, mut ch_channel, wait_for_event) =
            crate::contract::contract_handler_channel();
        let connection_manager = crate::ring::ConnectionManager::new(&node_config);
        let (result_router_tx, result_router_rx) = tokio::sync::mpsc::channel(100);
        let task_monitor = crate::node::background_task_monitor::BackgroundTaskMonitor::new();
        let op_manager = Arc::new(
            OpManager::new(
                notification_tx,
                ops_ch_channel,
                &node_config,
                crate::tracing::DynamicRegister::new(vec![]),
                connection_manager,
                result_router_tx,
                &task_monitor,
            )
            .expect("build OpManager"),
        );
        op_manager.ring.attach_op_manager(&op_manager);
        let self_addr: SocketAddr = "127.0.0.1:12000".parse().unwrap();
        op_manager
            .ring
            .connection_manager
            .set_own_addr_local_for_test(self_addr);

        let update_query_count = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let counter = update_query_count.clone();
        let handler = tokio::spawn(async move {
            while let Ok((id, ev, _priority)) = ch_channel.recv_from_sender().await {
                #[allow(
                    clippy::wildcard_enum_match_arm,
                    reason = "a stand-in handler that serves exactly the two events this test drives; any other event is an unexpected-input panic, and enumerating all 20+ ContractHandlerEvent variants to reach the same panic would only rot"
                )]
                let response = match ev {
                    ContractHandlerEvent::GetQuery { .. } => ContractHandlerEvent::GetResponse {
                        key: None,
                        response: Ok(StoreResponse {
                            state: None,
                            contract: None,
                        }),
                    },
                    ContractHandlerEvent::GetSummaryQuery { key } => {
                        ContractHandlerEvent::GetSummaryResponse {
                            key,
                            summary: Ok(StateSummary::from(Vec::new())),
                        }
                    }
                    ContractHandlerEvent::UpdateQuery { key, data, .. } => {
                        counter.fetch_add(1, Ordering::Relaxed);
                        let is_delta = matches!(
                            &data,
                            UpdateData::Delta(_) | UpdateData::RelatedDelta { .. }
                        );
                        if is_delta {
                            match delta_outcome {
                                DeltaOutcome::ExecReject => ContractHandlerEvent::UpdateResponse {
                                    new_value: Err(exec_reject_err(key)),
                                    state_changed: false,
                                },
                                DeltaOutcome::QueueFull => ContractHandlerEvent::UpdateResponse {
                                    new_value: Err(queue_full_err()),
                                    state_changed: false,
                                },
                                DeltaOutcome::Success => ContractHandlerEvent::UpdateResponse {
                                    new_value: Ok(WrappedState::from(vec![1u8, 2, 3])),
                                    state_changed: true,
                                },
                                DeltaOutcome::SuccessNoChange => {
                                    ContractHandlerEvent::UpdateResponse {
                                        new_value: Ok(WrappedState::from(vec![1u8, 2, 3])),
                                        state_changed: false,
                                    }
                                }
                            }
                        } else {
                            ContractHandlerEvent::UpdateResponse {
                                new_value: Ok(WrappedState::from(vec![1u8, 2, 3])),
                                state_changed: true,
                            }
                        }
                    }
                    other => {
                        panic!("unexpected handler event in exec-reject stand-in: {other:?}")
                    }
                };
                if ch_channel.send_to_sender(id, response).await.is_err() {
                    break;
                }
            }
        });

        let guard: Box<dyn std::any::Any> =
            Box::new((handler, result_router_rx, task_monitor, wait_for_event));
        (op_manager, notification_rx, update_query_count, guard)
    }

    /// Convenience wrapper: a node whose stand-in handler fails every delta with
    /// a contract-exec rejection (the common case for the backoff-gate tests).
    async fn build_exec_reject_test_node(
        id: &str,
    ) -> (
        Arc<OpManager>,
        crate::node::EventLoopNotificationsReceiver,
        std::sync::Arc<std::sync::atomic::AtomicUsize>,
        Box<dyn std::any::Any>,
    ) {
        build_broadcast_test_node(id, DeltaOutcome::ExecReject).await
    }

    /// #5510: a delta that FAILS TO APPLY increments the delta-failure resync
    /// counter, and a queue-full drop does NOT.
    ///
    /// This is what stops `test_full_state_send_no_incorrect_caching`'s new
    /// assertion from being vacuous. That test asserts
    /// `delta_failure_resyncs() == 0`, having previously asserted on the TOTAL
    /// resync count; if the counter were never incremented the assertion would
    /// pass forever and the #2763 summary-caching regression it guards would
    /// walk straight through it. So: prove the counter FIRES on the cause it
    /// names, and prove it does NOT fire on a different cause that also emits a
    /// ResyncRequest — otherwise it would be the same over-broad proxy under a
    /// new name.
    #[tokio::test]
    async fn a_failed_delta_is_counted_separately_from_a_queue_full_drop() {
        // (a) FIRES on a delta-apply failure.
        crate::config::GlobalTestMetrics::reset();
        let (op_manager, mut notification_rx, _queries, _guard) =
            build_broadcast_test_node("delta-failure-metric-5510", DeltaOutcome::ExecReject).await;
        let key = ContractKey::from_id_and_code(
            ContractInstanceId::new([81u8; 32]),
            CodeHash::new([82u8; 32]),
        );
        let sender_addr: SocketAddr = "127.0.0.1:13300".parse().unwrap();

        let r = drive_relay_broadcast_to(
            &op_manager,
            Transaction::new::<UpdateMsg>(),
            key,
            DeltaOrFullState::Delta(vec![1, 2, 3]),
            Vec::new(),
            sender_addr,
            crate::ring::broadcast_coverage::CoveredPeers::empty(),
        )
        .await;
        assert!(r.is_err(), "the stand-in handler must reject the delta");
        assert_eq!(
            crate::config::GlobalTestMetrics::delta_failure_resyncs(),
            1,
            "a delta that failed to apply MUST be counted as a delta-failure \
             resync — this is the #2763 signal, and a counter that never fires \
             turns its assertion into a no-op"
        );
        drain_resync_targets(&mut notification_rx, key);

        // (b) does NOT fire on a queue-full drop, which also emits a
        //     ResyncRequest but is a different cause entirely.
        crate::config::GlobalTestMetrics::reset();
        let (qf_manager, mut qf_rx, _qf_guard) =
            build_queue_full_test_node("delta-failure-metric-5510-qf").await;
        let qf_key = ContractKey::from_id_and_code(
            ContractInstanceId::new([83u8; 32]),
            CodeHash::new([84u8; 32]),
        );
        let qf_sender: SocketAddr = "127.0.0.1:13400".parse().unwrap();

        let qf = drive_relay_broadcast_to(
            &qf_manager,
            Transaction::new::<UpdateMsg>(),
            qf_key,
            DeltaOrFullState::Delta(vec![4, 5, 6]),
            Vec::new(),
            qf_sender,
            crate::ring::broadcast_coverage::CoveredPeers::empty(),
        )
        .await;
        assert!(
            qf.as_ref()
                .err()
                .is_some_and(|e| e.is_contract_queue_full()),
            "expected a queue-full error, got {qf:?}"
        );
        assert_eq!(
            drain_resync_targets(&mut qf_rx, qf_key),
            vec![qf_sender],
            "precondition: the queue-full drop DID emit a ResyncRequest, so this \
             is a real discrimination test rather than a scenario with no resync"
        );
        assert_eq!(
            crate::config::GlobalTestMetrics::delta_failure_resyncs(),
            0,
            "a queue-full drop emits a ResyncRequest but NO delta failed, so it \
             must not be counted as one — counting it would rebuild the same \
             over-broad proxy #5510 replaced"
        );
    }

    /// #4864 review (testing H1): the load-bearing #4861 behavior — the backoff
    /// PREVENTS executor invocation in the REAL driver. After the (N=3) Invalid
    /// threshold trips, the next broadcast's merge is SKIPPED: no additional
    /// `UpdateQuery` reaches the handler, the suppression metric is bumped, and
    /// no `ResyncRequest` is emitted while suppressed.
    #[tokio::test]
    async fn backoff_gate_skips_merge_after_invalid_threshold_trips() {
        use std::sync::atomic::Ordering;
        crate::config::GlobalTestMetrics::reset();
        let (op_manager, mut notification_rx, update_queries, _guard) =
            build_exec_reject_test_node("backoff-gate-4864").await;
        let key = ContractKey::from_id_and_code(
            ContractInstanceId::new([21u8; 32]),
            CodeHash::new([22u8; 32]),
        );
        let sender: SocketAddr = "127.0.0.1:12200".parse().unwrap();

        // Three consecutive delta failures (distinct bytes so none is a dedup
        // hit) trip the Invalid threshold; each runs the merge → 1 UpdateQuery.
        for bytes in [vec![1u8], vec![2u8], vec![3u8]] {
            let r = drive_relay_broadcast_to(
                &op_manager,
                Transaction::new::<UpdateMsg>(),
                key,
                DeltaOrFullState::Delta(bytes),
                Vec::new(),
                sender,
                crate::ring::broadcast_coverage::CoveredPeers::empty(),
            )
            .await;
            assert!(r.is_err(), "each poison delta must fail the merge");
            let _ = drain_resync_targets(&mut notification_rx, key); // failure path may emit
        }
        assert_eq!(
            update_queries.load(Ordering::Relaxed),
            3,
            "3 failing merges reached the executor"
        );
        let suppressed_before = crate::config::GlobalTestMetrics::merges_suppressed_by_backoff();

        // The 4th broadcast: the backoff has tripped → the merge is SKIPPED.
        let r4 = drive_relay_broadcast_to(
            &op_manager,
            Transaction::new::<UpdateMsg>(),
            key,
            DeltaOrFullState::Delta(vec![4u8]),
            Vec::new(),
            sender,
            crate::ring::broadcast_coverage::CoveredPeers::empty(),
        )
        .await;
        assert!(
            r4.is_ok(),
            "a suppressed broadcast completes Ok (like a dedup skip)"
        );
        assert_eq!(
            update_queries.load(Ordering::Relaxed),
            3,
            "the suppressed merge MUST NOT reach the executor (no 4th UpdateQuery)"
        );
        assert!(
            crate::config::GlobalTestMetrics::merges_suppressed_by_backoff() > suppressed_before,
            "the suppressed merge MUST bump merges_suppressed_by_backoff"
        );
        assert!(
            drain_resync_targets(&mut notification_rx, key).is_empty(),
            "no ResyncRequest may be emitted while the merge is suppressed"
        );
    }

    /// #4864 review (testing #2): a full-state success does NOT reset the backoff
    /// (strictly delta-only). Two delta failures + a full-state success leave the
    /// consecutive count intact, so a 3rd delta failure still trips (proving no
    /// reset) and the following delta is suppressed.
    #[tokio::test]
    async fn full_state_success_does_not_reset_backoff() {
        use std::sync::atomic::Ordering;
        crate::config::GlobalTestMetrics::reset();
        let (op_manager, mut notification_rx, update_queries, _guard) =
            build_exec_reject_test_node("backoff-fullstate-noreset-4864").await;
        let key = ContractKey::from_id_and_code(
            ContractInstanceId::new([31u8; 32]),
            CodeHash::new([32u8; 32]),
        );
        let sender: SocketAddr = "127.0.0.1:12300".parse().unwrap();

        // Two delta failures (count = 2, below the trip threshold).
        for bytes in [vec![1u8], vec![2u8]] {
            let driven = drive_relay_broadcast_to(
                &op_manager,
                Transaction::new::<UpdateMsg>(),
                key,
                DeltaOrFullState::Delta(bytes),
                Vec::new(),
                sender,
                crate::ring::broadcast_coverage::CoveredPeers::empty(),
            )
            .await;
            // Pin the PREMISE: these two must actually fail, or the backoff
            // count below never advances and the test passes vacuously.
            assert!(
                driven.is_err(),
                "the stand-in executor must reject this delta, got {driven:?}"
            );
            let _ = drain_resync_targets(&mut notification_rx, key);
        }
        // A full-state broadcast SUCCEEDS via the stand-in — but must NOT reset
        // the consecutive count (strictly delta-only reset).
        let ok = drive_relay_broadcast_to(
            &op_manager,
            Transaction::new::<UpdateMsg>(),
            key,
            DeltaOrFullState::FullState(vec![9, 9, 9]),
            Vec::new(),
            sender,
            crate::ring::broadcast_coverage::CoveredPeers::empty(),
        )
        .await;
        assert!(
            ok.is_ok(),
            "full-state merge should succeed via the stand-in"
        );
        let _ = drain_resync_targets(&mut notification_rx, key);

        // A 3rd delta failure now trips (count 2 → 3), proving the success did
        // NOT reset it to 0 (else the count would only reach 1 here).
        let third = drive_relay_broadcast_to(
            &op_manager,
            Transaction::new::<UpdateMsg>(),
            key,
            DeltaOrFullState::Delta(vec![3u8]),
            Vec::new(),
            sender,
            crate::ring::broadcast_coverage::CoveredPeers::empty(),
        )
        .await;
        assert!(
            third.is_err(),
            "the tripping delta must actually fail, got {third:?}"
        );
        let _ = drain_resync_targets(&mut notification_rx, key);
        let queries_after_trip = update_queries.load(Ordering::Relaxed);

        // The following delta is SUPPRESSED (merge skipped) — the backoff tripped
        // across the full-state success, so the count was never reset.
        let r = drive_relay_broadcast_to(
            &op_manager,
            Transaction::new::<UpdateMsg>(),
            key,
            DeltaOrFullState::Delta(vec![4u8]),
            Vec::new(),
            sender,
            crate::ring::broadcast_coverage::CoveredPeers::empty(),
        )
        .await;
        assert!(r.is_ok());
        assert_eq!(
            update_queries.load(Ordering::Relaxed),
            queries_after_trip,
            "the backoff tripped across the full-state success (no reset) — the \
             following delta merge is skipped"
        );
    }

    /// #4864 review P1 (the adversarial regression, driven through the REAL
    /// driver): one sender tripping its Invalid channel must NOT suppress a
    /// DIFFERENT sender's delta — the second sender's merge still reaches the
    /// executor. A contract-wide gate would blackout the healthy sender here,
    /// which is the peer-triggerable-blackout this per-sender rework prevents.
    #[tokio::test]
    async fn backoff_gate_is_scoped_per_sender_in_driver() {
        use std::sync::atomic::Ordering;
        crate::config::GlobalTestMetrics::reset();
        let (op_manager, mut notification_rx, update_queries, _guard) =
            build_exec_reject_test_node("backoff-per-sender-4864").await;
        let key = ContractKey::from_id_and_code(
            ContractInstanceId::new([41u8; 32]),
            CodeHash::new([42u8; 32]),
        );
        let sender_a: SocketAddr = "127.0.0.1:12400".parse().unwrap();
        let sender_b: SocketAddr = "127.0.0.1:12500".parse().unwrap();

        // Sender A trips its own Invalid channel (3 consecutive delta failures).
        for bytes in [vec![1u8], vec![2u8], vec![3u8]] {
            let driven = drive_relay_broadcast_to(
                &op_manager,
                Transaction::new::<UpdateMsg>(),
                key,
                DeltaOrFullState::Delta(bytes),
                Vec::new(),
                sender_a,
                crate::ring::broadcast_coverage::CoveredPeers::empty(),
            )
            .await;
            // Pin the PREMISE: A must actually fail 3 times, or it never trips
            // and the per-sender scoping below is never exercised.
            assert!(
                driven.is_err(),
                "the stand-in executor must reject A's delta, got {driven:?}"
            );
            let _ = drain_resync_targets(&mut notification_rx, key);
        }
        assert_eq!(
            update_queries.load(Ordering::Relaxed),
            3,
            "A's 3 failing merges reached the executor"
        );

        // A's next delta is now suppressed (A's channel tripped).
        let ra = drive_relay_broadcast_to(
            &op_manager,
            Transaction::new::<UpdateMsg>(),
            key,
            DeltaOrFullState::Delta(vec![4u8]),
            Vec::new(),
            sender_a,
            crate::ring::broadcast_coverage::CoveredPeers::empty(),
        )
        .await;
        assert!(ra.is_ok(), "A's suppressed broadcast completes Ok");
        assert_eq!(
            update_queries.load(Ordering::Relaxed),
            3,
            "A's suppressed delta MUST NOT reach the executor"
        );

        // Sender B's delta (distinct payload) STILL reaches the executor — the
        // contract stays live for healthy senders despite A's backoff. It fails
        // via the stand-in, so B's channel starts counting, but the merge RAN.
        let rb = drive_relay_broadcast_to(
            &op_manager,
            Transaction::new::<UpdateMsg>(),
            key,
            DeltaOrFullState::Delta(vec![5u8]),
            Vec::new(),
            sender_b,
            crate::ring::broadcast_coverage::CoveredPeers::empty(),
        )
        .await;
        let _ = drain_resync_targets(&mut notification_rx, key);
        assert!(
            rb.is_err(),
            "B's delta reaches the merge (fails via stand-in)"
        );
        assert_eq!(
            update_queries.load(Ordering::Relaxed),
            4,
            "B's delta MUST reach the executor despite A's per-sender backoff"
        );
    }

    /// #4864 review item 7(b): `ContractQueueFull` is transient backpressure, not
    /// poison — the driver's `is_contract_exec_rejection()` gate must NOT record
    /// it, so no amount of consecutive queue-full failures trips the backoff.
    /// Every delta keeps reaching the executor (no suppression).
    #[tokio::test]
    async fn queue_full_failures_do_not_trip_backoff() {
        use std::sync::atomic::Ordering;
        crate::config::GlobalTestMetrics::reset();
        let (op_manager, mut notification_rx, update_queries, _guard) =
            build_broadcast_test_node("backoff-queuefull-4864", DeltaOutcome::QueueFull).await;
        let key = ContractKey::from_id_and_code(
            ContractInstanceId::new([51u8; 32]),
            CodeHash::new([52u8; 32]),
        );
        let sender: SocketAddr = "127.0.0.1:12600".parse().unwrap();

        // Five consecutive queue-full delta failures (well past the N=3 Invalid
        // trip threshold). None may be recorded to the backoff.
        for bytes in [vec![1u8], vec![2u8], vec![3u8], vec![4u8], vec![5u8]] {
            let driven = drive_relay_broadcast_to(
                &op_manager,
                Transaction::new::<UpdateMsg>(),
                key,
                DeltaOrFullState::Delta(bytes),
                Vec::new(),
                sender,
                crate::ring::broadcast_coverage::CoveredPeers::empty(),
            )
            .await;
            // Pin the PREMISE: every one of the five must be a queue-full
            // failure, else "5 failures did not trip the backoff" proves nothing.
            assert!(
                driven.is_err(),
                "the stand-in executor must report queue-full, got {driven:?}"
            );
            let _ = drain_resync_targets(&mut notification_rx, key);
        }
        assert_eq!(
            update_queries.load(Ordering::Relaxed),
            5,
            "every queue-full delta must still reach the executor — queue-full \
             must NOT trip the backoff (it is transient load, not poison)"
        );
        assert_eq!(
            crate::config::GlobalTestMetrics::merges_suppressed_by_backoff(),
            0,
            "no merge may be suppressed — queue-full never records a backoff entry"
        );
        assert_eq!(
            op_manager.ring.merge_backoff.contracts_in_backoff(),
            0,
            "no contract may be in backoff after only queue-full failures"
        );
    }

    /// #4864 round-4 P1 (item 3): the #4857 queue-full resync heal must be bound
    /// by the GLOBAL per-contract emit cap. A flood of DISTINCT senders (each
    /// passing its own per-(contract, sender) throttle) sending queue-full deltas
    /// for ONE contract must emit at most `EMIT_BURST` ResyncRequests total — not
    /// one per sender. Without the global cap this would be one per sender.
    #[tokio::test]
    async fn queue_full_resync_flood_is_bounded_by_global_emit_cap() {
        crate::config::GlobalTestMetrics::reset();
        let (op_manager, mut notification_rx, _update_queries, _guard) =
            build_broadcast_test_node("queuefull-emit-cap-4864", DeltaOutcome::QueueFull).await;
        let key = ContractKey::from_id_and_code(
            ContractInstanceId::new([71u8; 32]),
            CodeHash::new([72u8; 32]),
        );
        // Six DISTINCT senders (each clears its own per-sender throttle) each send
        // a distinct-payload queue-full delta for the SAME contract.
        let sender_count = 6u16;
        for i in 0..sender_count {
            let sender: SocketAddr = format!("127.0.0.1:{}", 13000 + i).parse().unwrap();
            let driven = drive_relay_broadcast_to(
                &op_manager,
                Transaction::new::<UpdateMsg>(),
                key,
                DeltaOrFullState::Delta(vec![i as u8]),
                Vec::new(),
                sender,
                crate::ring::broadcast_coverage::CoveredPeers::empty(),
            )
            .await;
            // Pin the PREMISE: each sender's delta must actually hit queue-full,
            // or the emit cap below is never put under pressure.
            assert!(
                driven.is_err(),
                "the stand-in executor must report queue-full, got {driven:?}"
            );
        }
        // The global per-contract emit cap (EMIT_BURST = 2) bounds total emissions
        // regardless of sender count — NOT one per sender.
        let emissions = drain_resync_targets(&mut notification_rx, key);
        let burst = crate::ring::resync_rate_limit::EMIT_BURST as usize;
        assert_eq!(
            emissions.len(),
            burst,
            "queue-full resync emissions ({}) must be bounded by the global \
             per-contract cap (EMIT_BURST = {}), not one per sender ({sender_count})",
            emissions.len(),
            burst
        );
    }

    /// Pin (#4864 round-4 item 3): the queue-full resync heal helper must route
    /// through BOTH the per-(contract, sender) throttle AND the global
    /// per-contract emit cap — the latter was the #4857 bypass.
    #[test]
    fn queue_full_resync_helper_goes_through_global_emit_cap() {
        let src = include_str!("op_ctx_task.rs");
        // #5510: the gates moved into `reserve_resync_emit`, the one function
        // both emit paths (the initial drop and the trailing coalesced repair)
        // go through. Pinning it there is what stops a second emit path being
        // hand-inlined without the cap.
        let start = src
            .find("async fn reserve_resync_emit(")
            .expect("reserve_resync_emit not found");
        let after = &src[start + 1..];
        let end = after
            .find("\nasync fn ")
            .or_else(|| after.find("\n#[cfg(test)]"))
            .unwrap_or(after.len());
        let body = &src[start..start + 1 + end];
        assert!(
            body.contains("resync_emit_limiter"),
            "the queue-full resync helper must route through the GLOBAL \
             per-contract emit cap (resync_emit_limiter) (#4864 round-4)"
        );
        assert!(
            body.contains("begin_resync_request"),
            "and keep the per-(contract, sender) throttle"
        );
    }

    /// #4864 round-8 pin (Codex P1): every PRODUCTION `ResyncRequest` emission
    /// MUST be preceded by an `outstanding_resync_requests.record(...)` so the
    /// receive arm can correlate (and consume) the matching `ResyncResponse`. A
    /// future emit site that forgets to record would make its legitimate response
    /// look unsolicited (a silent heal regression) — and the record is the whole
    /// basis of the receive-side DoS gate, so it must never drift from the emit.
    #[test]
    fn every_production_resync_request_emit_records_outstanding() {
        let src = include_str!("op_ctx_task.rs");
        // Bound to PRODUCTION code (before the test module) so the mock emit in
        // the harness below is not counted.
        let prod_end = src.find("\nmod tests {").unwrap_or(src.len());
        let prod = &src[..prod_end];

        // EXEMPT the #4862 queue-full RETRY (`resend_dropped_broadcast_resync_request`).
        // It RE-DELIVERS an emit whose `outstanding_resync_requests.record(...)`
        // was ALREADY made by `send_dropped_broadcast_resync_request` (the initial). It
        // deliberately does NOT re-record, because re-recording would re-authorize
        // a REDUNDANT `ResyncResponse` — the opposite of round-8's drop-the-
        // redundant intent (and would re-run the full-state WASM merge the record
        // exists to bound). In the bridge-drop case the initial record is still
        // outstanding when the retry's response arrives (so it IS authorized);
        // once that record is consumed, a further retry's response is correctly
        // dropped. So the retry's re-send is legitimately un-recorded — skip it.
        let retry_start = prod
            .find("async fn resend_dropped_broadcast_resync_request(")
            .expect("resend_dropped_broadcast_resync_request (the #4862 retry) not found");
        let retry_end = retry_start
            + 1
            + prod[retry_start + 1..]
                .find("\nasync fn ")
                .unwrap_or(prod.len() - retry_start - 1);

        // ALSO exempt `dispatch_resync_request` (#5510). It is the shared
        // enqueue, split out so the initial drop and the trailing coalesced
        // repair send through one body instead of two hand-inlined copies. It
        // holds no gate and makes no decision: every caller reaches it only
        // after `reserve_resync_emit` has taken both gates AND made the
        // `outstanding_resync_requests.record(...)` this pin is about. The
        // guarantee is therefore intact — it has just moved one call up.
        //
        // The exemption is narrow ON PURPOSE: it is keyed to this function's
        // name, so a NEW emit site elsewhere is still caught. Before adding
        // another exemption, check that the record really is made on every path
        // that reaches it — an emit whose response is dropped unapplied by the
        // #4864 gate is a silently useless repair, which is the failure this
        // pin exists to prevent.
        let dispatch_start = prod
            .find("async fn dispatch_resync_request(")
            .expect("dispatch_resync_request (the #5510 shared enqueue) not found");
        let dispatch_end = dispatch_start
            + 1
            + prod[dispatch_start + 1..]
                .find("\nasync fn ")
                .unwrap_or(prod.len() - dispatch_start - 1);

        let mut emits = 0usize;
        let mut cursor = 0usize;
        while let Some(rel) = prod[cursor..].find("message: InterestMessage::ResyncRequest") {
            let pos = cursor + rel;
            cursor = pos + 1;
            // Skip the exempt retry re-delivery and the shared enqueue (see above).
            if (retry_start..retry_end).contains(&pos)
                || (dispatch_start..dispatch_end).contains(&pos)
            {
                continue;
            }
            emits += 1;
            // Look back to the start of the ENCLOSING production fn for the record
            // call that authorizes the ResyncResponse this emit will provoke
            // (fmt-robust: match the field identifier, which survives
            // line-wrapping of the method chain).
            //
            // This was a fixed 600-byte lookback until #4863 moved the queue-full
            // helper's record ABOVE the retry spawn — the retry's conditional gate
            // reads that record, so it must exist before the task can observe it —
            // which pushed the record out of a byte window that was always an
            // arbitrary proxy for "same statement sequence". Scoping to the
            // enclosing fn keeps the real guarantee (an emit is preceded by its
            // record, on the same code path) without the brittle distance.
            // Anchor on the LAST fn-opening token before the emit. Must cover
            // visibility-prefixed forms (`pub async fn`, `pub(crate) async fn`):
            // matching only "\nasync fn "/"\nfn " would skip past a `pub` fn and
            // anchor to an EARLIER one, whose window may contain an unrelated
            // record — a false pass for exactly the regression this pin catches.
            let fn_start = [
                "\nasync fn ",
                "\nfn ",
                "\npub async fn ",
                "\npub fn ",
                "\npub(",
                // Item openers, not just fn openers (review round 3, C4).
                // `reserve_resync_emit` is followed by `const
                // MAX_COALESCED_RESYNC_*` and `static SLOT_CAP_LOG` before the
                // next `fn`, so a fn-only terminator ran the slice straight
                // through their DOC COMMENTS — and a pin asserting on a
                // mechanism could then be satisfied by prose describing it
                // rather than by the code.
                "\nconst ",
                "\nstatic ",
                "\npub(crate) const ",
                "\npub(crate) static ",
            ]
            .iter()
            .filter_map(|kw| prod[..pos].rfind(kw))
            .max()
            .unwrap_or(0);
            // Require the identifier to be part of an actual `.record(` call, not
            // merely mentioned. #4863 added the first NON-recording use of this
            // field (`is_outstanding`), so a bare identifier match would let a
            // READ satisfy a pin whose entire purpose is to require a WRITE.
            // Whitespace-stripped so rustfmt line-wrapping of the method chain
            // cannot break the match.
            let window: String = prod[fn_start..pos]
                .chars()
                .filter(|c| !c.is_whitespace())
                .collect();
            assert!(
                window.contains("outstanding_resync_requests.record("),
                "a production ResyncRequest emit at byte {pos} is NOT preceded by an \
                 outstanding_resync_requests.record(...) in the same fn — the \
                 receive arm would drop its response as unsolicited (#4864 round-8)"
            );
        }
        // #5510: `dispatch_resync_request` is now the shared enqueue, so its CALL
        // SITES are emit sites for this pin's purposes and must carry the same
        // guarantee. Each must be preceded, in its own enclosing fn, by the
        // `reserve_resync_emit(` that takes both gates AND makes the record.
        //
        // Without this the exemption above would be a hole: a new caller could
        // enqueue a ResyncRequest with no record at all and the loop would never
        // see it, because the only literal emit lives in the exempt function.
        // `spawn_resync_followups` is the one call site where the ordering is
        // LOOP-CARRIED rather than textual: its dispatch fires only when
        // `dispatch_pending` is set, and that flag is set ONLY on the `Some(..)`
        // arm of `reserve_resync_emit`, one iteration earlier. A positional
        // check cannot see that, so the mechanism is asserted directly below and
        // the fn is skipped in the positional scan. Weakening the scan to "the
        // identifier appears somewhere in the fn" instead would have accepted a
        // dispatch that no reserve gates at all.
        let followups = {
            let start = prod
                .find("fn spawn_resync_followups(")
                .expect("spawn_resync_followups not found");
            let rest = &prod[start + 1..];
            // Terminate on ANY fn opener, not just `async fn` — the sibling
            // below this one is a plain `fn` (review finding 19).
            let end = [
                "\nasync fn ",
                "\nfn ",
                "\npub async fn ",
                "\npub fn ",
                "\npub(",
                // Item openers, not just fn openers (review round 3, C4).
                // `reserve_resync_emit` is followed by `const
                // MAX_COALESCED_RESYNC_*` and `static SLOT_CAP_LOG` before the
                // next `fn`, so a fn-only terminator ran the slice straight
                // through their DOC COMMENTS — and a pin asserting on a
                // mechanism could then be satisfied by prose describing it
                // rather than by the code.
                "\nconst ",
                "\nstatic ",
                "\npub(crate) const ",
                "\npub(crate) static ",
            ]
            .iter()
            .filter_map(|kw| rest.find(kw))
            .min()
            .unwrap_or(rest.len());
            &prod[start..start + 1 + end]
        };
        let reserve_in_followups = followups.find("reserve_resync_emit(").expect(
            "spawn_resync_followups must gate its dispatch on reserve_resync_emit \
             (#4864 round-8/#5510)",
        );
        assert!(
            followups.contains("let mut dispatch_pending = false;"),
            "the loop's dispatch flag must start FALSE — the caller dispatches \
             its own window's request, so a chain that started with it true would \
             double-send (#4862 P2/#5510)"
        );
        let set_pending = followups.find("dispatch_pending = true;").expect(
            "spawn_resync_followups must set dispatch_pending after a granted \
             reservation",
        );
        assert!(
            reserve_in_followups < set_pending,
            "dispatch_pending ({set_pending}) may only be set AFTER \
             reserve_resync_emit ({reserve_in_followups}) — it is what makes the \
             loop's dispatch correspond to a granted, correlated reservation"
        );
        assert_eq!(
            followups.matches("dispatch_pending = true;").count(),
            1,
            "exactly one site may arm the loop's dispatch; a second one could arm \
             it without a reservation"
        );
        assert!(
            followups.contains("if dispatch_pending {"),
            "the loop's dispatch must be guarded by dispatch_pending"
        );
        let followups_start = prod
            .find("fn spawn_resync_followups(")
            .expect("spawn_resync_followups not found");
        let followups_end = followups_start + 1 + followups.len();

        let mut dispatches = 0usize;
        let mut cursor = 0usize;
        while let Some(rel) = prod[cursor..].find("dispatch_resync_request(") {
            let pos = cursor + rel;
            cursor = pos + 1;
            // Skip the definition itself and the loop-carried site above.
            if (dispatch_start..dispatch_end).contains(&pos)
                || (followups_start..followups_end).contains(&pos)
            {
                continue;
            }
            dispatches += 1;
            let fn_start = [
                "\nasync fn ",
                "\nfn ",
                "\npub async fn ",
                "\npub fn ",
                "\npub(",
                // Item openers, not just fn openers (review round 3, C4).
                // `reserve_resync_emit` is followed by `const
                // MAX_COALESCED_RESYNC_*` and `static SLOT_CAP_LOG` before the
                // next `fn`, so a fn-only terminator ran the slice straight
                // through their DOC COMMENTS — and a pin asserting on a
                // mechanism could then be satisfied by prose describing it
                // rather than by the code.
                "\nconst ",
                "\nstatic ",
                "\npub(crate) const ",
                "\npub(crate) static ",
            ]
            .iter()
            .filter_map(|kw| prod[..pos].rfind(kw))
            .max()
            .unwrap_or(0);
            let window: String = prod[fn_start..pos]
                .chars()
                .filter(|c| !c.is_whitespace())
                .collect();
            assert!(
                window.contains("reserve_resync_emit("),
                "a dispatch_resync_request call at byte {pos} is NOT preceded by a \
                 reserve_resync_emit(...) in the same fn — it would enqueue a \
                 ResyncRequest with no gates and no correlation record, and the \
                 receive arm would drop its response unapplied (#4864 round-8/#5510)"
            );
        }

        assert!(
            emits + dispatches >= 2,
            "expected at least the dropped-broadcast and delta-failure ResyncRequest \
             emit sites in production ({emits} literal + {dispatches} via the shared \
             enqueue) — has the emit path moved?"
        );
    }

    /// Guard (#4903 review / invariant 3): the UPDATE and broadcast paths must
    /// NEVER stamp hosting recency. "UPDATE churn never counts as genuine
    /// access" is what keeps a zero-demand storm contract cost-evictable
    /// (`hosting-invariants.md` invariant 3: a storm contract whose only
    /// activity is its own UPDATE churn stays evictable) — a future refactor
    /// that routes an UPDATE through `host_contract` / `record_get_access` /
    /// `touch_hosting` / `mark_local_client_access` would make storms
    /// self-protecting and silently disarm cost-pressure eviction. This pin
    /// scans the PRODUCTION regions of the update op modules, the broadcast
    /// queue, AND the executor apply chokepoint (`executor_impl.rs`, #4903
    /// review L3 — the PUT/UPDATE state-apply path, which today reports only
    /// `ExecCpuMicros` and must never grow a recency stamp on the UPDATE side)
    /// for any recency-stamping call, with comment text stripped so prose
    /// references stay legal.
    #[test]
    fn update_and_broadcast_paths_never_stamp_hosting_recency() {
        // Strip line comments so documentation may mention the forbidden
        // calls without tripping the pin; only real code counts.
        fn code_only(src: &str) -> String {
            src.lines()
                .map(|line| line.split("//").next().unwrap_or(""))
                .collect::<Vec<_>>()
                .join("\n")
        }

        let sources: [(&str, &str); 4] = [
            (
                "operations/update/op_ctx_task.rs",
                include_str!("op_ctx_task.rs"),
            ),
            ("operations/update.rs", include_str!("../update.rs")),
            (
                "node/network_bridge/broadcast_queue.rs",
                include_str!("../../node/network_bridge/broadcast_queue.rs"),
            ),
            (
                "contract/executor/runtime/executor_impl.rs",
                include_str!("../../contract/executor/runtime/executor_impl.rs"),
            ),
        ];
        // Every entry point that stamps hosting recency (`recency_seq` /
        // `last_genuine_access`) or the local-client renewal lease
        // (`local_client_last_access`). Matched as method calls (leading dot)
        // so type/fn definitions elsewhere don't count.
        let forbidden = [
            ".host_contract(",
            ".record_get_access(",
            ".touch_hosting(",
            ".touch_with_demand(",
            ".record_contract_access(",
            ".record_access(",
            ".record_access_with_demand(",
            ".mark_local_client_access(",
        ];
        for (name, src) in sources {
            let prod_end = src.find("\nmod tests {").unwrap_or(src.len());
            let prod = code_only(&src[..prod_end]);
            for needle in forbidden {
                assert!(
                    !prod.contains(needle),
                    "{name} production code must not call `{needle}` — an UPDATE/\
                     broadcast path stamping hosting recency would let a storm \
                     contract protect itself from cost eviction (invariant 3)"
                );
            }
        }
    }

    /// #4864 round-4 (item 9): a memoized payload replayed through the REAL
    /// `drive_relay_broadcast_to` by a SECOND sender is hard-skipped — the merge
    /// never reaches the executor, that sender's Invalid channel never trips (its
    /// OWN novel delta still merges), and no ResyncRequest is emitted. Seed the
    /// memo directly with THREE DISTINCT payloads (so each replay is a genuine
    /// memo hit rather than a `BroadcastDedupCache` hit — the dedup cache is
    /// content-addressed too, so an identical replay would be swallowed by dedup
    /// before reaching the backoff gate).
    #[tokio::test]
    async fn memoized_payload_replay_from_second_sender_is_hard_skipped() {
        use std::sync::atomic::Ordering;
        crate::config::GlobalTestMetrics::reset();
        let (op_manager, mut notification_rx, update_queries, _guard) =
            build_exec_reject_test_node("memo-replay-4864").await;
        let key = ContractKey::from_id_and_code(
            ContractInstanceId::new([83u8; 32]),
            CodeHash::new([84u8; 32]),
        );
        let sender_a: SocketAddr = "127.0.0.1:13300".parse().unwrap();
        let sender_b: SocketAddr = "127.0.0.1:13400".parse().unwrap();

        // Seed the contract-wide memo with three distinct poison delta payloads
        // (as if sender A had already failed them). record_failure memoizes each.
        let poisons = [vec![91u8], vec![92u8], vec![93u8]];
        for p in &poisons {
            let h = crate::ring::merge_backoff::merge_payload_hash(true, p);
            op_manager.ring.merge_backoff.record_failure(
                key.id(),
                sender_a,
                crate::ring::merge_backoff::MergeFailureClass::Invalid,
                h,
            );
        }

        // Sender B replays each memoized payload: each is a hard memo skip.
        for p in &poisons {
            let r = drive_relay_broadcast_to(
                &op_manager,
                Transaction::new::<UpdateMsg>(),
                key,
                DeltaOrFullState::Delta(p.clone()),
                Vec::new(),
                sender_b,
                crate::ring::broadcast_coverage::CoveredPeers::empty(),
            )
            .await;
            assert!(
                r.is_ok(),
                "a memoized-payload skip completes Ok (like a dedup skip)"
            );
        }
        assert_eq!(
            update_queries.load(Ordering::Relaxed),
            0,
            "B's replays of memoized payloads MUST NOT reach the executor"
        );
        assert_eq!(
            crate::config::GlobalTestMetrics::merges_suppressed_by_backoff(),
            poisons.len() as u64,
            "each replay must be a memo skip (bumps merges_suppressed_by_backoff), \
             not a dedup skip"
        );
        assert!(
            drain_resync_targets(&mut notification_rx, key).is_empty(),
            "a memo skip must NOT emit a ResyncRequest"
        );

        // B's channel never tripped — its OWN novel delta still reaches the merge.
        let novel = drive_relay_broadcast_to(
            &op_manager,
            Transaction::new::<UpdateMsg>(),
            key,
            DeltaOrFullState::Delta(vec![94u8]),
            Vec::new(),
            sender_b,
            crate::ring::broadcast_coverage::CoveredPeers::empty(),
        )
        .await;
        let _ = drain_resync_targets(&mut notification_rx, key);
        assert!(
            novel.is_err(),
            "B's novel delta reaches the merge (fails via stand-in)"
        );
        assert_eq!(
            update_queries.load(Ordering::Relaxed),
            1,
            "B's novel delta MUST reach the executor — the memo skips never advanced \
             B's channel"
        );
    }

    /// #4864 review item 6: a successful client-local DELTA driven through the
    /// REAL `drive_client_update` clears the CONTRACT-WIDE side (Timeout + memo)
    /// via `record_success_local`, but must NOT clear a remote sender's Invalid
    /// channel (a local success says nothing about that sender's fork).
    #[tokio::test]
    async fn client_local_delta_success_clears_contract_side_not_other_senders() {
        let (op_manager, _notification_rx, _update_queries, _guard) =
            build_broadcast_test_node("backoff-client-local-4864", DeltaOutcome::Success).await;
        let key = ContractKey::from_id_and_code(
            ContractInstanceId::new([61u8; 32]),
            CodeHash::new([62u8; 32]),
        );
        // Make the local node host the contract so drive_client_update's
        // target=None path applies the update locally instead of erroring.
        op_manager.ring.host_contract(
            key,
            100,
            crate::ring::AccessType::Put,
            crate::ring::HostingCause::Other,
        );

        let remote_sender: SocketAddr = "127.0.0.1:12700".parse().unwrap();
        let probe_sender: SocketAddr = "127.0.0.1:12800".parse().unwrap();

        // Seed a contract-wide Timeout (trips at 1) and a remote sender's tripped
        // Invalid channel (3 consecutive).
        op_manager.ring.merge_backoff.record_failure(
            key.id(),
            remote_sender,
            crate::ring::merge_backoff::MergeFailureClass::Timeout,
            0x01,
        );
        for h in [0x11u64, 0x12, 0x13] {
            op_manager.ring.merge_backoff.record_failure(
                key.id(),
                remote_sender,
                crate::ring::merge_backoff::MergeFailureClass::Invalid,
                h,
            );
        }
        // Precondition: the contract-wide Timeout suppresses a fresh probe sender.
        assert_eq!(
            op_manager
                .ring
                .merge_backoff
                .check(key.id(), probe_sender, 0x99),
            crate::ring::merge_backoff::MergeDecision::InBackoff,
            "seeded contract-wide Timeout should suppress before the client success"
        );

        // Drive a successful client-local DELTA through the real driver.
        let outcome = drive_client_update(
            &op_manager,
            Transaction::new::<UpdateMsg>(),
            key,
            UpdateData::Delta(StateDelta::from(vec![7u8, 7, 7])),
            RelatedContracts::default(),
        )
        .await;
        assert!(
            outcome.is_ok(),
            "client-local delta merge should succeed via the stand-in: {outcome:?}"
        );

        // Contract-wide side (Timeout + memo) is cleared: the probe sender, which
        // has no Invalid channel of its own, is now Allowed.
        assert_eq!(
            op_manager
                .ring
                .merge_backoff
                .check(key.id(), probe_sender, 0x99),
            crate::ring::merge_backoff::MergeDecision::Allow,
            "record_success_local must clear the contract-wide Timeout + memo"
        );
        // But the remote sender's Invalid channel is UNTOUCHED — still suppressed.
        assert_eq!(
            op_manager
                .ring
                .merge_backoff
                .check(key.id(), remote_sender, 0x99),
            crate::ring::merge_backoff::MergeDecision::InBackoff,
            "a local client's success must NOT clear a remote sender's Invalid \
             channel (it says nothing about that sender's fork)"
        );
    }

    /// #4864 round-4 addendum (CI rule-review): the client-path mirror of
    /// `full_state_success_does_not_reset_backoff`. A successful client-local
    /// FULL-STATE update (`is_delta_update == false`) driven through the REAL
    /// `drive_client_update` must NOT reset the backoff — `record_success_local`
    /// is gated `is_delta_update`, and a full-state apply is fork-flippable so it
    /// proves nothing about convergence. Closes the bot warning that the
    /// delta-only gating was source-scrape-only. (With round-4 item 1 the
    /// contract-side clear also needs `changed`, but the `is_delta` gate fires
    /// first, so this holds regardless of `changed`.)
    #[tokio::test]
    async fn client_local_full_state_success_does_not_reset_backoff() {
        let (op_manager, _notification_rx, _update_queries, _guard) =
            build_broadcast_test_node("backoff-client-fullstate-4864", DeltaOutcome::Success).await;
        let key = ContractKey::from_id_and_code(
            ContractInstanceId::new([63u8; 32]),
            CodeHash::new([64u8; 32]),
        );
        op_manager.ring.host_contract(
            key,
            100,
            crate::ring::AccessType::Put,
            crate::ring::HostingCause::Other,
        );

        let remote_sender: SocketAddr = "127.0.0.1:15100".parse().unwrap();
        let probe_sender: SocketAddr = "127.0.0.1:15200".parse().unwrap();

        // Seed a contract-wide Timeout + a remote sender's tripped Invalid channel.
        op_manager.ring.merge_backoff.record_failure(
            key.id(),
            remote_sender,
            crate::ring::merge_backoff::MergeFailureClass::Timeout,
            0x01,
        );
        for h in [0x11u64, 0x12, 0x13] {
            op_manager.ring.merge_backoff.record_failure(
                key.id(),
                remote_sender,
                crate::ring::merge_backoff::MergeFailureClass::Invalid,
                h,
            );
        }
        assert_eq!(
            op_manager
                .ring
                .merge_backoff
                .check(key.id(), probe_sender, 0x99),
            crate::ring::merge_backoff::MergeDecision::InBackoff,
            "seeded contract-wide Timeout should suppress before the client update"
        );

        // Drive a successful client-local FULL-STATE update (is_delta_update=false).
        let outcome = drive_client_update(
            &op_manager,
            Transaction::new::<UpdateMsg>(),
            key,
            UpdateData::State(State::from(vec![9u8, 9, 9])),
            RelatedContracts::default(),
        )
        .await;
        assert!(
            outcome.is_ok(),
            "client-local full-state update should succeed via the stand-in: {outcome:?}"
        );

        // The backoff is NOT reset: the contract-wide Timeout still suppresses the
        // probe (a full-state apply is not convergence evidence, delta-only reset).
        assert_eq!(
            op_manager
                .ring
                .merge_backoff
                .check(key.id(), probe_sender, 0x99),
            crate::ring::merge_backoff::MergeDecision::InBackoff,
            "a client FULL-STATE success must NOT clear the contract-wide Timeout \
             (record_success_local is gated is_delta_update)"
        );
        // ... and the remote sender's Invalid channel is likewise untouched.
        assert_eq!(
            op_manager
                .ring
                .merge_backoff
                .check(key.id(), remote_sender, 0x99),
            crate::ring::merge_backoff::MergeDecision::InBackoff,
            "a client FULL-STATE success must NOT clear a remote sender's Invalid channel"
        );
    }

    /// #4864 round-5 item 4: a successful client-local DELTA that did NOT advance
    /// state (`state_changed == false`, e.g. an idempotent re-apply) driven
    /// through the REAL `drive_client_update` must NOT clear the contract-side
    /// backoff — `record_success_local` is gated `changed`, so a no-op success is
    /// not a state-generation signal. This closes the round-4 no-op-wipe bug end
    /// to end (previously not drivable: no harness variant returned Ok with
    /// `state_changed: false`).
    #[tokio::test]
    async fn client_local_no_op_delta_success_does_not_clear_contract_side() {
        let (op_manager, _notification_rx, _update_queries, _guard) = build_broadcast_test_node(
            "backoff-client-nochange-4864",
            DeltaOutcome::SuccessNoChange,
        )
        .await;
        let key = ContractKey::from_id_and_code(
            ContractInstanceId::new([65u8; 32]),
            CodeHash::new([66u8; 32]),
        );
        op_manager.ring.host_contract(
            key,
            100,
            crate::ring::AccessType::Put,
            crate::ring::HostingCause::Other,
        );

        let remote_sender: SocketAddr = "127.0.0.1:15300".parse().unwrap();
        let probe_sender: SocketAddr = "127.0.0.1:15400".parse().unwrap();

        // Seed a contract-wide Timeout + a remote sender's tripped Invalid channel.
        op_manager.ring.merge_backoff.record_failure(
            key.id(),
            remote_sender,
            crate::ring::merge_backoff::MergeFailureClass::Timeout,
            0x01,
        );
        for h in [0x11u64, 0x12, 0x13] {
            op_manager.ring.merge_backoff.record_failure(
                key.id(),
                remote_sender,
                crate::ring::merge_backoff::MergeFailureClass::Invalid,
                h,
            );
        }
        assert_eq!(
            op_manager
                .ring
                .merge_backoff
                .check(key.id(), probe_sender, 0x99),
            crate::ring::merge_backoff::MergeDecision::InBackoff,
            "seeded contract-wide Timeout should suppress before the client update"
        );

        // Drive a client-local DELTA (is_delta_update == true) that SUCCEEDS with
        // state_changed == false via the stand-in.
        let outcome = drive_client_update(
            &op_manager,
            Transaction::new::<UpdateMsg>(),
            key,
            UpdateData::Delta(StateDelta::from(vec![7u8, 7, 7])),
            RelatedContracts::default(),
        )
        .await;
        assert!(
            outcome.is_ok(),
            "no-op client-local delta should succeed via the stand-in: {outcome:?}"
        );

        // The contract-side backoff is NOT cleared (the state did not advance): the
        // probe is still suppressed by the contract-wide Timeout.
        assert_eq!(
            op_manager
                .ring
                .merge_backoff
                .check(key.id(), probe_sender, 0x99),
            crate::ring::merge_backoff::MergeDecision::InBackoff,
            "a no-op (changed=false) client delta success must NOT clear the \
             contract-wide Timeout (record_success_local is gated on changed)"
        );
        // ... and the remote sender's Invalid channel is likewise untouched.
        assert_eq!(
            op_manager
                .ring
                .merge_backoff
                .check(key.id(), remote_sender, 0x99),
            crate::ring::merge_backoff::MergeDecision::InBackoff,
            "a no-op client delta success must NOT clear a remote sender's Invalid channel"
        );
    }

    /// Pin (#4864 round-5 item 4): the three success call sites must pass the REAL
    /// `changed` field, not a literal `true` — a literal would re-introduce the
    /// round-4 no-op-wipe bug (wiping the contract-side Timeout/memo on every
    /// no-op merge) while passing every other test.
    #[test]
    fn success_sites_pass_real_changed_flag_not_a_literal() {
        let src = include_str!("op_ctx_task.rs");
        // Both drive_client_update success arms pass execution.changed.
        let client = extract_fn_body(src, "async fn drive_client_update(");
        let client_calls = client
            .matches("record_success_local(key.id(), execution.changed)")
            .count();
        assert!(
            client_calls >= 2,
            "both drive_client_update success arms must pass the REAL \
             execution.changed to record_success_local (found {client_calls}, \
             expected >= 2) — not a literal `true` (#4864 round-5 item 4)"
        );
        assert!(
            !client.contains("record_success_local(key.id(), true)"),
            "drive_client_update must NOT hardcode `true` for the changed flag"
        );
        // The relay broadcast success arm passes result.changed AS THE ARGUMENT of
        // record_success_from_sender (anchored to the call, not a loose match on
        // the whole body — #4864 round-6 item 6a).
        let relay = broadcast_to_driver_src();
        let call = relay.find(".record_success_from_sender(").expect(
            "drive_relay_broadcast_to must clear the backoff via record_success_from_sender",
        );
        let call_args = &relay[call..(call + 160).min(relay.len())];
        assert!(
            call_args.contains("result.changed"),
            "record_success_from_sender must be passed the REAL result.changed \
             (found in its arg list), not a literal `true` (#4864 round-5 item 4)"
        );
    }

    /// Pin (#4864 round-5 item 6): the CHANGED streaming full-state apply arm MUST
    /// call `invalidate_payload_memo` inside the `if exec.changed` gate — same
    /// reason as the node.rs resync-apply site: a state-advancing full-state apply
    /// invalidates the memo's premise. Dropping it silently widens the staleness
    /// corner to the full 10-min TTL. (The streaming path still must NOT reset the
    /// backoff — covered by `broadcast_to_streaming_has_backoff_wiring`.)
    #[test]
    fn streaming_changed_apply_invalidates_payload_memo() {
        let src = include_str!("op_ctx_task.rs");
        let st = extract_fn_body(src, "async fn apply_streaming_broadcast(");
        let changed_gate = st
            .find("if exec.changed")
            .expect("streaming Ok arm must gate on `if exec.changed`");
        // Brace-match the `if exec.changed { ... }` block so we assert containment,
        // not just relative order (#4864 round-6 item 6b).
        let open_brace = changed_gate
            + st[changed_gate..]
                .find('{')
                .expect("if exec.changed block must have a body");
        let mut depth = 0usize;
        let mut close_brace = None;
        for (i, ch) in st[open_brace..].char_indices() {
            match ch {
                '{' => depth += 1,
                '}' => {
                    depth -= 1;
                    if depth == 0 {
                        close_brace = Some(open_brace + i);
                        break;
                    }
                }
                _ => {}
            }
        }
        let close_brace = close_brace.expect("if exec.changed block must be balanced");
        let invalidate = concat!("invalidate_", "payload_memo(");
        let inv_pos = st.find(invalidate).expect(
            "the CHANGED streaming full-state apply must call invalidate_payload_memo \
             (#4864 round-5 item 6)",
        );
        assert!(
            open_brace < inv_pos && inv_pos < close_brace,
            "invalidate_payload_memo ({inv_pos}) must be INSIDE the `if exec.changed \
             {{ .. }}` block ({open_brace}..{close_brace}) so a no-op streaming apply \
             does not invalidate the memo"
        );
    }

    /// #4864 round-9 item 3: the NON-streaming CHANGED full-state success arms
    /// (relay `drive_relay_broadcast_to` + client `drive_client_update`) MUST also
    /// invalidate the failed-payload memo (memo only, NO backoff reset), so a
    /// payload that failed against old state doesn't stay KnownFailedPayload for up
    /// to FAILED_PAYLOAD_TTL after a full state advances it — the omission the
    /// streaming and resync-apply paths did NOT have. Mirror of
    /// `streaming_changed_apply_invalidates_payload_memo` for the other two drivers.
    #[test]
    fn nonstreaming_changed_fullstate_success_invalidates_payload_memo() {
        let src = include_str!("op_ctx_task.rs");
        let invalidate = concat!("invalidate_", "payload_memo(");
        for (fn_needle, changed_expr) in [
            (
                "async fn drive_relay_broadcast_to(",
                "else if result.changed",
            ),
            ("async fn drive_client_update(", "else if execution.changed"),
        ] {
            let body = extract_fn_body(src, fn_needle);
            let gate = body.find(changed_expr).unwrap_or_else(|| {
                panic!(
                    "`{fn_needle}` must have an `{changed_expr}` full-state success arm \
                     (#4864 round-9 item 3)"
                )
            });
            // Brace-match the else-if block; the memo invalidation must be INSIDE it
            // (so a NO-op full-state apply does not invalidate the memo).
            let open_brace = gate
                + body[gate..]
                    .find('{')
                    .expect("else-if block must have a body");
            let mut depth = 0usize;
            let mut close = None;
            for (i, ch) in body[open_brace..].char_indices() {
                match ch {
                    '{' => depth += 1,
                    '}' => {
                        depth -= 1;
                        if depth == 0 {
                            close = Some(open_brace + i);
                            break;
                        }
                    }
                    _ => {}
                }
            }
            let close = close.expect("else-if block must be balanced");
            let inv = body
                .find(invalidate)
                .unwrap_or_else(|| panic!("`{fn_needle}` must call invalidate_payload_memo"));
            assert!(
                open_brace < inv && inv < close,
                "`{fn_needle}`: invalidate_payload_memo must be INSIDE the \
                 `{changed_expr}` full-state block (memo-only, gated on changed)"
            );
        }
    }

    /// Issue #4857 (P2): the immediate queue-full `ResyncRequest` rides a lossy
    /// path (event loop → `P2pBridge::try_send`) and can be dropped under the
    /// same backpressure that caused the queue-full drop. If it is, the sender
    /// never clears its poisoned summary and a one-off change diverges until the
    /// ~5-min heartbeat. `send_dropped_broadcast_resync_request` therefore schedules a
    /// BOUNDED trailing retry that re-dispatches the ResyncRequest within the
    /// single reserved throttle window.
    ///
    /// This test drives ONE queue-full drop, then advances virtual time (which,
    /// with no injected clock override, IS the throttle's clock) and asserts the
    /// trailing retry re-emits exactly `QUEUE_FULL_RESYNC_MAX_RETRIES` additional
    /// ResyncRequests (all to the original sender) — proving the resync heals
    /// without waiting on the heartbeat, even though no further broadcast occurs
    /// (the rarely-changing-field case). Time advances stay under the 30s
    /// reservation deadline so the deadline gate does not trip here (the
    /// deadline-stop behavior is covered by
    /// `queue_full_resync_retry_stops_after_reservation_deadline`).
    #[tokio::test(start_paused = true)]
    async fn queue_full_resync_retry_redispatches_within_reservation() {
        let (op_manager, mut notification_rx, _guard) =
            build_queue_full_test_node("queue-full-resync-4857-retry").await;
        let key = ContractKey::from_id_and_code(
            ContractInstanceId::new([9u8; 32]),
            CodeHash::new([10u8; 32]),
        );
        let sender_addr: SocketAddr = "127.0.0.1:12300".parse().unwrap();

        // One queue-full drop → immediate ResyncRequest + a spawned trailing
        // retry task. No second broadcast is issued (the rarely-changing field
        // case), so the only way further ResyncRequests appear is the retry.
        let r1 = drive_relay_broadcast_to(
            &op_manager,
            Transaction::new::<UpdateMsg>(),
            key,
            DeltaOrFullState::Delta(vec![1, 2, 3]),
            Vec::new(),
            sender_addr,
            crate::ring::broadcast_coverage::CoveredPeers::empty(),
        )
        .await;
        assert!(
            r1.as_ref()
                .err()
                .is_some_and(|e| e.is_contract_queue_full()),
            "expected a queue-full error from the stand-in handler, got {r1:?}"
        );
        assert_eq!(
            drain_resync_targets(&mut notification_rx, key),
            vec![sender_addr],
            "the immediate (pre-retry) ResyncRequest must fire exactly once"
        );

        // Let the spawned retry task run through its bounded, jittered waits.
        // The retry paces on the throttle's clock via a `now()`-poll, so advance
        // the (paused) clock in small steps and yield between them to let the
        // poll loop make progress; drain after each. 40 × 500ms = 20s total,
        // safely under the 30s reservation deadline, and we stop as soon as both
        // retries have fired. The extra rounds after that prove the retry does
        // NOT keep emitting past the bound.
        let mut retries = Vec::new();
        for _ in 0..40 {
            if retries.len() >= QUEUE_FULL_RESYNC_MAX_RETRIES as usize {
                break;
            }
            tokio::time::advance(Duration::from_millis(500)).await;
            tokio::task::yield_now().await;
            retries.extend(drain_resync_targets(&mut notification_rx, key));
        }

        assert_eq!(
            retries.len(),
            QUEUE_FULL_RESYNC_MAX_RETRIES as usize,
            "the trailing retry must re-dispatch the ResyncRequest exactly \
             QUEUE_FULL_RESYNC_MAX_RETRIES times within the single reservation, \
             so a bridge-dropped resync heals before the ~5-min heartbeat — and \
             must NOT exceed that bound (storm bound #4251)"
        );
        assert!(
            retries.iter().all(|t| *t == sender_addr),
            "every retry ResyncRequest must target the original sender, got {retries:?}"
        );
    }

    /// Issue #4863: the #4862 trailing retry must be CONDITIONAL — it must NOT
    /// re-dispatch once the first queue-full `ResyncRequest` is observed to have
    /// landed. Every `ResyncRequest` makes the sender clear its cached summary
    /// of us and re-send FULL contract state, so a blind retry lands up to
    /// `QUEUE_FULL_RESYNC_MAX_RETRIES` redundant full-state `ResyncResponse`s on
    /// a receiver whose queue was full moments ago — the exact amplification the
    /// #4251 storm bound exists to avoid.
    ///
    /// The observed-landed signal is the #4864 round-8 correlation record: the
    /// `ResyncResponse` receive arm (`node.rs`) require-and-CONSUMES the
    /// `(contract, sender)` entry before applying, so the entry's absence is an
    /// END-TO-END confirmation that the request landed, the sender answered, and
    /// we authorized the answer.
    ///
    /// This drives ONE queue-full drop, then simulates the response landing
    /// exactly the way the receive arm does (consume the record), then advances
    /// the throttle's clock across the whole retry burst window. NO retry may
    /// fire. It also asserts the node-wide retry slot (#4862 P1) is RELEASED on
    /// the skip rather than burned.
    #[tokio::test(start_paused = true)]
    async fn queue_full_resync_retry_skipped_once_response_landed() {
        let (op_manager, mut notification_rx, _guard) =
            build_queue_full_test_node("queue-full-resync-4863-landed").await;
        let key = ContractKey::from_id_and_code(
            ContractInstanceId::new([17u8; 32]),
            CodeHash::new([18u8; 32]),
        );
        let sender_addr: SocketAddr = "127.0.0.1:12700".parse().unwrap();

        let r1 = drive_relay_broadcast_to(
            &op_manager,
            Transaction::new::<UpdateMsg>(),
            key,
            DeltaOrFullState::Delta(vec![1, 2, 3]),
            Vec::new(),
            sender_addr,
            crate::ring::broadcast_coverage::CoveredPeers::empty(),
        )
        .await;
        assert!(
            r1.as_ref()
                .err()
                .is_some_and(|e| e.is_contract_queue_full()),
            "expected a queue-full error from the stand-in handler, got {r1:?}"
        );
        assert_eq!(
            drain_resync_targets(&mut notification_rx, key),
            vec![sender_addr],
            "the immediate (pre-retry) ResyncRequest must fire exactly once"
        );

        // The sender's `ResyncResponse` arrives: `node.rs`'s receive arm
        // require-and-consumes the outstanding correlation record before applying
        // the full state. Doing exactly that here IS the "first request landed"
        // event, from the retry's point of view.
        assert!(
            op_manager
                .ring
                .outstanding_resync_requests
                .consume(*key.id(), sender_addr),
            "the helper must have recorded the outstanding request, so the \
             matching ResyncResponse is authorized once (#4864 round-8)"
        );

        // Advance the throttle's clock across the whole retry burst (20s, under
        // the 30s reservation deadline) — the same pacing the blind-retry test
        // uses to observe both retries.
        let mut retries = Vec::new();
        for _ in 0..40 {
            tokio::time::advance(Duration::from_millis(500)).await;
            tokio::task::yield_now().await;
            retries.extend(drain_resync_targets(&mut notification_rx, key));
        }

        assert!(
            retries.is_empty(),
            "the first ResyncRequest already landed (its ResyncResponse was \
             received and consumed), so the trailing retry MUST NOT re-dispatch \
             — each redundant request costs another full-state ResyncResponse \
             onto a just-saturated receiver (#4863). Got {retries:?}"
        );
        // #5510 changed WHEN the slot is released, not whether. The task now also
        // owns the trailing-edge sweep, so it holds the slot until the
        // reservation window closes rather than returning as soon as the retry
        // burst is skipped. Assert the property that actually matters — the slot
        // is never LEAKED — by running past the deadline and requiring it back.
        // Nothing was swallowed by this window, so the chain ends there.
        for _ in 0..70 {
            if op_manager.interest_manager.outstanding_resync_retries() == 0 {
                break;
            }
            tokio::time::advance(Duration::from_millis(500)).await;
            tokio::task::yield_now().await;
        }
        assert_eq!(
            op_manager.interest_manager.outstanding_resync_retries(),
            0,
            "the follow-up task must free its node-wide slot once the reservation \
             window closes with nothing swallowed — a slot held forever would \
             starve the node-wide cap (#4862 P1/#5510)"
        );
    }

    /// Issue #4863 (edge case): the conditional gate is re-evaluated on EVERY
    /// attempt, not just the first. When the `ResyncResponse` lands between
    /// retry 1 and retry 2, retry 1 legitimately fired (nothing had landed yet)
    /// but retry 2 MUST be skipped.
    ///
    /// This is the partial-heal case a first-attempt-only check would miss, and
    /// it also proves the gate stops the burst rather than merely delaying it.
    #[tokio::test(start_paused = true)]
    async fn queue_full_resync_retry_stops_at_the_attempt_after_response_lands() {
        let (op_manager, mut notification_rx, _guard) =
            build_queue_full_test_node("queue-full-resync-4863-midburst").await;
        let key = ContractKey::from_id_and_code(
            ContractInstanceId::new([19u8; 32]),
            CodeHash::new([20u8; 32]),
        );
        let sender_addr: SocketAddr = "127.0.0.1:12800".parse().unwrap();

        let r1 = drive_relay_broadcast_to(
            &op_manager,
            Transaction::new::<UpdateMsg>(),
            key,
            DeltaOrFullState::Delta(vec![1, 2, 3]),
            Vec::new(),
            sender_addr,
            crate::ring::broadcast_coverage::CoveredPeers::empty(),
        )
        .await;
        assert!(
            r1.as_ref()
                .err()
                .is_some_and(|e| e.is_contract_queue_full()),
            "expected a queue-full error from the stand-in handler, got {r1:?}"
        );
        assert_eq!(
            drain_resync_targets(&mut notification_rx, key),
            vec![sender_addr],
            "the immediate (pre-retry) ResyncRequest must fire exactly once"
        );

        // Nothing has landed yet → the FIRST retry must still fire (the #4857
        // heal for a genuinely bridge-dropped request is preserved).
        let mut retries = Vec::new();
        for _ in 0..20 {
            tokio::time::advance(Duration::from_millis(500)).await;
            tokio::task::yield_now().await;
            retries.extend(drain_resync_targets(&mut notification_rx, key));
            if !retries.is_empty() {
                break;
            }
        }
        assert_eq!(
            retries.len(),
            1,
            "with nothing landed, the first trailing retry MUST still fire \
             (preserves the #4857 heal), got {retries:?}"
        );

        // NOW the response lands (receive arm consumes the correlation record).
        assert!(
            op_manager
                .ring
                .outstanding_resync_requests
                .consume(*key.id(), sender_addr),
            "the outstanding record must still be present before the response lands"
        );

        // Advance across the remaining burst window; retry 2 must be skipped.
        for _ in 0..30 {
            tokio::time::advance(Duration::from_millis(500)).await;
            tokio::task::yield_now().await;
            retries.extend(drain_resync_targets(&mut notification_rx, key));
        }
        assert_eq!(
            retries.len(),
            1,
            "once the response lands mid-burst, every REMAINING attempt must be \
             skipped — the gate is re-evaluated per attempt (#4863). Got {retries:?}"
        );
        // #5510: as above, the slot is now held until the window closes rather
        // than until the retry burst ends. Run past the deadline and require it
        // back; nothing was swallowed here, so the chain ends.
        for _ in 0..70 {
            if op_manager.interest_manager.outstanding_resync_retries() == 0 {
                break;
            }
            tokio::time::advance(Duration::from_millis(500)).await;
            tokio::task::yield_now().await;
        }
        assert_eq!(
            op_manager.interest_manager.outstanding_resync_retries(),
            0,
            "the follow-up task must exit and free its node-wide slot once the \
             reservation window closes (#4862 P1/#5510)"
        );
    }

    /// Issue #4857 (P2, review P2-A): the trailing retry burst is anchored to
    /// the reservation deadline on the throttle's OWN clock. If the injected
    /// clock crosses that deadline before a retry's target is reached, the retry
    /// task MUST stop — never dispatching into a fresh reservation window. This
    /// makes the #4251 `1 + MAX`-per-window bound rigorous even when the caller's
    /// blocking `notify_node_event(...).await` consumed most of the window.
    ///
    /// Here the (paused) clock — which, with no override, IS the throttle's
    /// clock — is advanced PAST the 30s `RESYNC_REQUEST_MIN_INTERVAL` before any
    /// retry's jittered target elapses, so the deadline gate fires first and
    /// NO retry is emitted.
    #[tokio::test(start_paused = true)]
    async fn queue_full_resync_retry_stops_after_reservation_deadline() {
        let (op_manager, mut notification_rx, _guard) =
            build_queue_full_test_node("queue-full-resync-4857-deadline").await;
        let key = ContractKey::from_id_and_code(
            ContractInstanceId::new([11u8; 32]),
            CodeHash::new([12u8; 32]),
        );
        let sender_addr: SocketAddr = "127.0.0.1:12400".parse().unwrap();

        let r1 = drive_relay_broadcast_to(
            &op_manager,
            Transaction::new::<UpdateMsg>(),
            key,
            DeltaOrFullState::Delta(vec![1, 2, 3]),
            Vec::new(),
            sender_addr,
            crate::ring::broadcast_coverage::CoveredPeers::empty(),
        )
        .await;
        assert!(
            r1.as_ref()
                .err()
                .is_some_and(|e| e.is_contract_queue_full()),
            "expected a queue-full error from the stand-in handler, got {r1:?}"
        );
        assert_eq!(
            drain_resync_targets(&mut notification_rx, key),
            vec![sender_addr],
            "the immediate (pre-retry) ResyncRequest must fire exactly once"
        );

        // Let the retry task reach its first poll wait, then jump the clock PAST
        // the 30s reservation deadline (RESYNC_REQUEST_MIN_INTERVAL, interest.rs).
        tokio::task::yield_now().await;
        tokio::time::advance(Duration::from_secs(35)).await;
        // Give the task several scheduling turns to observe the elapsed deadline
        // and return without dispatching.
        let mut retries = Vec::new();
        for _ in 0..5 {
            tokio::task::yield_now().await;
            retries.extend(drain_resync_targets(&mut notification_rx, key));
        }

        assert!(
            retries.is_empty(),
            "once the injected clock passes the reservation deadline, the retry \
             MUST stop and emit no further ResyncRequests (P2-A), got {retries:?}"
        );
    }

    /// Issue #4857 (P2, DST backstop): the retry gates on the injected throttle
    /// clock, but that clock may be a DECOUPLED `hosting_time_source_override`
    /// (`SharedMockTimeSource`) that a sim FREEZES before the reservation
    /// deadline. If termination depended SOLELY on the injected clock, the poll
    /// would spin forever (a `start_paused` test hangs; a real sim leaks a task
    /// per drop). The tokio-clock liveness backstop must terminate the task
    /// regardless.
    ///
    /// This test injects a mock clock, FREEZES it (never advances it) below the
    /// deadline, then lets ONLY tokio time advance (`start_paused` auto-advance).
    /// It asserts (a) the retry task TERMINATES within a bounded tokio budget —
    /// a spin would make the `timeout` fail instead of hanging — and (b) NO
    /// retry dispatches (the frozen injected clock never crosses a target).
    #[tokio::test(start_paused = true)]
    async fn queue_full_resync_retry_terminates_when_injected_clock_frozen() {
        let (op_manager, mut notification_rx, _guard) = build_queue_full_test_node_with_clock(
            "queue-full-resync-4857-frozen-clock",
            Some(
                Arc::new(crate::util::time_source::SharedMockTimeSource::new())
                    as crate::util::time_source::DynTimeSource,
            ),
        )
        .await;
        let key = ContractKey::from_id_and_code(
            ContractInstanceId::new([13u8; 32]),
            CodeHash::new([14u8; 32]),
        );
        let sender_addr: SocketAddr = "127.0.0.1:12500".parse().unwrap();

        // Deadline FAR in the injected-clock future: the mock is frozen at ~0,
        // so `now >= target` and `now >= reservation_deadline` never hold and
        // only the tokio backstop can stop the task.
        let reservation_deadline = op_manager.interest_manager.now() + Duration::from_secs(30);

        // This test drives the retry DIRECTLY, so it does what the real caller
        // (`send_dropped_broadcast_resync_request`) does and records the outstanding
        // request first.
        //
        // Note this record is INERT here, and deliberately so: the injected clock
        // is frozen at ~0, so `now >= target` never holds and the wait loop can
        // only exit via the tokio liveness backstop — the #4863 landed-gate below
        // it is never reached. The record keeps the fixture faithful to the real
        // caller (and makes the test strictly stricter if the gate ever does
        // become reachable on this path) but it is NOT what makes this test pass,
        // and this test does NOT cover the gate on the frozen-clock path.
        let _correlated = op_manager
            .ring
            .outstanding_resync_requests
            .record(*key.id(), sender_addr);

        let op_mgr = op_manager.clone();
        let handle = tokio::spawn(async move {
            resend_dropped_broadcast_resync_request(
                &op_mgr,
                key,
                sender_addr,
                Transaction::new::<UpdateMsg>(),
                reservation_deadline,
                true,
            )
            .await;
        });

        // The mock clock stays FROZEN. Under `start_paused`, tokio auto-advances
        // while everything is parked, driving the poll's sleeps and — via the
        // backstop — the ~30s tokio floor. `timeout` converts a spin into a
        // failure rather than an indefinite hang.
        let terminated = tokio::time::timeout(Duration::from_secs(300), handle).await;
        assert!(
            terminated.is_ok(),
            "the retry task MUST terminate via the tokio liveness backstop when \
             the injected clock is frozen — a spin would hang here (DST safety, \
             #4857 P2)"
        );
        terminated.unwrap().expect("retry task must not panic");

        assert!(
            drain_resync_targets(&mut notification_rx, key).is_empty(),
            "a frozen injected clock never crosses a retry target, so no retry \
             ResyncRequest should dispatch"
        );
    }

    /// Issue #4862 (P1): the node-wide retry-task cap bounds outstanding retry
    /// tasks even when the throttle LRU evicts active reservations under key
    /// churn. Reserving up to the cap succeeds; the next reservation is refused
    /// (so `send_dropped_broadcast_resync_request` skips the spawn — the immediate
    /// ResyncRequest still sends); freeing a slot re-admits exactly one. The
    /// outstanding count never exceeds the cap (hard CAS cap). Uses a per-node
    /// OpManager so the count is isolated from other tests.
    #[tokio::test]
    async fn queue_full_resync_retry_slot_cap_bounds_outstanding_tasks() {
        let (op_manager, _rx, _guard) =
            build_queue_full_test_node("queue-full-resync-4857-cap").await;
        let im = &op_manager.interest_manager;
        let cap = crate::ring::interest::MAX_OUTSTANDING_QUEUE_FULL_RESYNC_RETRIES;

        // Reserve up to the cap — simulating `cap` outstanding retry tasks that
        // LRU churn would have spawned. Every reservation within the cap succeeds.
        let mut slots = Vec::new();
        for i in 0..cap {
            let slot = im
                .try_reserve_resync_retry_slot()
                .unwrap_or_else(|| panic!("reservation {i} within the cap must succeed"));
            slots.push(slot);
        }
        assert_eq!(
            im.outstanding_resync_retries(),
            cap,
            "outstanding count must reach exactly the cap"
        );

        // At cap: a further reservation is refused (the caller skips the spawn;
        // the immediate ResyncRequest is unaffected), and the count does NOT grow.
        assert!(
            im.try_reserve_resync_retry_slot().is_none(),
            "at cap, a further retry-slot reservation MUST be refused (#4862 P1)"
        );
        assert_eq!(
            im.outstanding_resync_retries(),
            cap,
            "a refused reservation must not increment the count (hard cap)"
        );

        // Freeing one slot re-admits exactly one.
        slots.pop();
        assert_eq!(
            im.outstanding_resync_retries(),
            cap - 1,
            "dropping a slot guard must free it"
        );
        let readmit = im.try_reserve_resync_retry_slot();
        assert!(
            readmit.is_some(),
            "a freed slot must re-admit exactly one reservation"
        );
        assert_eq!(
            im.outstanding_resync_retries(),
            cap,
            "re-admission returns the count to the cap"
        );

        drop(slots);
        drop(readmit);
        assert_eq!(
            im.outstanding_resync_retries(),
            0,
            "every slot guard frees its slot on drop"
        );
    }

    /// Issue #4862 (P2): when the initial (blocking) enqueue consumes most of the
    /// reservation window, a fixed `now + jittered_delay` first target would
    /// already be PAST the deadline → the retry would return with ZERO attempts,
    /// failing to heal the exact backpressure case. The clamp pulls the first
    /// target into the remaining window so ≥1 retry still dispatches within
    /// `[now, reservation_deadline)`.
    ///
    /// Simulated by calling the retry with a deadline only 500ms out (well below
    /// a jittered attempt-1 delay of ~1.6-2.4s) and asserting a retry fires
    /// before it — without the clamp, none would.
    #[tokio::test(start_paused = true)]
    async fn queue_full_resync_retry_fires_within_a_short_remaining_window() {
        let (op_manager, mut notification_rx, _guard) =
            build_queue_full_test_node("queue-full-resync-4857-shortwindow").await;
        let key = ContractKey::from_id_and_code(
            ContractInstanceId::new([15u8; 32]),
            CodeHash::new([16u8; 32]),
        );
        let sender_addr: SocketAddr = "127.0.0.1:12600".parse().unwrap();

        // Deadline only 500ms out — much less than jittered_resync_retry_delay(1)
        // (~1.6-2.4s), so a fixed first target would exceed it.
        let reservation_deadline = op_manager.interest_manager.now() + Duration::from_millis(500);
        // Driving the retry DIRECTLY, so record the outstanding request the way
        // the real caller (`send_dropped_broadcast_resync_request`) does — otherwise the
        // #4863 landed-gate skips every attempt and this test would fail for a
        // reason unrelated to the clamp it is pinning. (Unlike the frozen-clock
        // test, the gate IS reachable here — the 500ms deadline clamps the first
        // target to ~250ms, which the injected clock does reach.)
        let _correlated = op_manager
            .ring
            .outstanding_resync_requests
            .record(*key.id(), sender_addr);
        let op_mgr = op_manager.clone();
        let handle = tokio::spawn(async move {
            resend_dropped_broadcast_resync_request(
                &op_mgr,
                key,
                sender_addr,
                Transaction::new::<UpdateMsg>(),
                reservation_deadline,
                true,
            )
            .await;
        });

        // Advance up to the deadline in small steps; a clamped retry must fire.
        let mut fired = Vec::new();
        for _ in 0..20 {
            tokio::time::advance(Duration::from_millis(50)).await;
            tokio::task::yield_now().await;
            fired.extend(drain_resync_targets(&mut notification_rx, key));
            if !fired.is_empty() {
                break;
            }
        }
        tokio::time::timeout(Duration::from_secs(5), handle)
            .await
            .expect("the resend task must finish once the deadline passes")
            .expect("the resend task must not panic");

        assert!(
            !fired.is_empty(),
            "with a short remaining window the clamp must still fire ≥1 retry \
             within [now, reservation_deadline) (#4862 P2), got none"
        );
        assert!(
            fired.iter().all(|t| *t == sender_addr),
            "retry must target the original sender, got {fired:?}"
        );
    }

    /// Issue #4857 (P2) storm-bound pin: the trailing retry must (1) be spawned
    /// only AFTER both rate-limit gates pass (the per-sender `begin_resync_request`
    /// reservation and the global `resync_emit_limiter` emit cap), never on a
    /// throttled/suppressed drop, and (2) create NO new reservation and stay
    /// bounded, so the #4251 steady-state cap (≤ `1 + QUEUE_FULL_RESYNC_MAX_RETRIES`
    /// ResyncRequests per (contract, peer) per 30s window) is preserved.
    #[test]
    fn queue_full_resync_retry_is_bounded_and_reservation_scoped() {
        let src = include_str!("op_ctx_task.rs");
        let fn_body = |name: &str| -> &str {
            let start = src.find(name).unwrap_or_else(|| panic!("{name} not found"));
            let after = &src[start + 1..];
            // Terminate on ANY fn opener, not just `async fn` (review finding
            // 19). Several plain `fn`s sit between the async ones this pin
            // slices: `jittered_resync_retry_delay` between the two resync
            // fns, `seed_sender_summary_from_broadcast` before the broadcast
            // driver, and — added by this PR — `spawn_resync_followups`
            // between `reserve_resync_emit` and the next async fn. With an
            // `async fn`-only terminator a slice ran straight through them,
            // and a gate deleted from the sliced function would still satisfy
            // the pin as long as the same identifier appeared anywhere
            // downstream. Taking the MINIMUM rather than the first match
            // matters: the openers are not in source order.
            let end = [
                "\nasync fn ",
                "\nfn ",
                "\npub async fn ",
                "\npub fn ",
                "\npub(",
                // Item openers, not just fn openers (review round 3, C4).
                // `reserve_resync_emit` is followed by `const
                // MAX_COALESCED_RESYNC_*` and `static SLOT_CAP_LOG` before the
                // next `fn`, so a fn-only terminator ran the slice straight
                // through their DOC COMMENTS — and a pin asserting on a
                // mechanism could then be satisfied by prose describing it
                // rather than by the code.
                "\nconst ",
                "\nstatic ",
                "\npub(crate) const ",
                "\npub(crate) static ",
            ]
            .iter()
            .filter_map(|kw| after.find(kw))
            .chain(after.find("\n#[cfg(test)]"))
            .min()
            .unwrap_or(after.len());
            &src[start..start + 1 + end]
        };

        // (1) The retry is scheduled only AFTER both rate-limit gates pass — the
        // per-(contract, sender) throttle reservation (`begin_resync_request`) and
        // the global per-contract emit cap (`resync_emit_limiter`, #4864) — so it
        // never runs on a throttled or globally-suppressed drop.
        // #5510 re-anchor. The gates are in `reserve_resync_emit` and the spawn
        // is in `spawn_resync_followups`, so the ordering is now checked in two
        // steps: the entry point calls reserve BEFORE spawn, and reserve holds
        // both gates. That is the same guarantee — nothing is scheduled on a
        // throttled or globally-suppressed drop — expressed against the current
        // shape.
        let entry = fn_body("async fn send_dropped_broadcast_resync_request(");
        let reserve = entry
            .find("reserve_resync_emit(")
            .expect("the entry point must take the gates via reserve_resync_emit (#4251/#4864)");
        let followups = entry
            .find("spawn_resync_followups(")
            .expect("the entry point must schedule the follow-ups (heal #4857 P2 / #5510)");
        assert!(
            reserve < followups,
            "the gates ({reserve}) MUST be taken before the follow-up task is \
             spawned ({followups}), so a throttled or globally-suppressed drop \
             schedules nothing"
        );

        let helper = fn_body("async fn reserve_resync_emit(");
        let gate = helper.find(".begin_resync_request_or_note_drop(").expect(
            "helper must gate on begin_resync_request_or_note_drop (per-sender \
             throttle, #4251)",
        );

        // #5510 round 3: the refusal and the debt-note must be ONE call.
        //
        // The two-step (`begin_resync_request` -> `None`, then a separate
        // `note_resync_drop_during_window`) has a TOCTOU hole that the
        // generation check does NOT close: between the two lock acquisitions
        // the owning chain can reach its deadline, take the flag, read false,
        // conclude nothing is owed, and free its slot — after which the note
        // lands on an entry nobody is watching and the drop is never repaired.
        // Silently, which is the exact failure #5510 exists to close.
        assert!(
            !helper.contains(".begin_resync_request("),
            "reserve_resync_emit must NOT call the two-step begin_resync_request; \
             the atomic begin_resync_request_or_note_drop exists because the \
             two-step loses a swallowed drop under a multi-thread runtime"
        );
        assert!(
            !helper.contains("note_resync_drop_during_window"),
            "reserve_resync_emit must NOT note the debt as a separate lock \
             acquisition — the note belongs inside the same critical section as \
             the refusal that caused it"
        );
        let global_cap = helper
            .find("resync_emit_limiter")
            .expect("helper must gate on the global per-contract emit cap (#4864)");
        let record = helper
            .find("outstanding_resync_requests")
            .expect("helper must record the outstanding request the follow-ups rely on");
        assert!(
            gate < record && global_cap < record,
            "the correlation record MUST come AFTER both the begin_resync_request \
             reservation and the global emit-cap gate, so the follow-ups only run \
             within a granted, globally-authorized reservation (never on a \
             throttled or suppressed drop)"
        );
        // #5510: the spawn itself moved to `spawn_resync_followups`, which the
        // entry point calls only after the gates (asserted above). The slot and
        // Drop-guard assertions below therefore read that function.
        let helper = fn_body("fn spawn_resync_followups(");
        let spawn = helper
            .find("GlobalExecutor::spawn(")
            .expect("the follow-ups must be a spawned background task");

        // (1a) The spawn is gated on a node-wide slot (#4862 P1) so LRU churn
        // cannot spawn unbounded overlapping tasks, and the slot is moved INTO
        // the task (freed on completion via its Drop guard).
        let reserve = helper.find("try_reserve_resync_retry_slot(").expect(
            "the spawn must be gated on interest_manager.try_reserve_resync_retry_slot() (#4862 P1)",
        );
        assert!(
            reserve < spawn,
            "the slot reservation must gate the spawn (P1): a spawn without a slot \
             would defeat the node-wide retry-task cap"
        );
        assert!(
            helper.contains("let _slot = slot;"),
            "the retry slot guard must be moved into the spawned task so it frees \
             on task completion (#4862 P1)"
        );

        // (1b) The follow-ups are spawned CONCURRENTLY, BEFORE the blocking
        // immediate enqueue (#4862 P2), so the retry timer runs DURING a
        // backpressured send — otherwise a send that consumed the whole window
        // would leave the task starting past the deadline with zero retries.
        // #5510: the enqueue moved into `dispatch_resync_request`, so the
        // ordering is now read off the entry point's call sequence, which is
        // where it is actually decided.
        let immediate_send = entry
            .find("dispatch_resync_request(")
            .expect("the entry point must perform the immediate (blocking) enqueue");
        assert!(
            followups < immediate_send,
            "the follow-up spawn ({followups}) MUST come BEFORE the blocking \
             dispatch_resync_request ({immediate_send}) (#4862 P2), so the retry \
             timer runs during a backpressured enqueue"
        );

        // The deadline is threaded into the follow-ups so the burst is anchored
        // to THIS reservation window (review P2-A). It rides INSIDE
        // `FollowupStart::Reserved` (review round 2, so a chain that owes a
        // repair cannot be handed a reservation it does not hold), so the pin
        // reads the threading at the entry point where it is decided rather than
        // looking for a bare parameter name in the helper.
        assert!(
            entry.contains("deadline: reservation_deadline"),
            "the entry point must thread the reservation deadline from \
             begin_resync_request into FollowupStart::Reserved (anchor to the \
             window, P2-A)"
        );
        assert!(
            helper.contains("FollowupStart::Reserved {")
                && helper.contains("FollowupStart::OwedOnly {"),
            "the follow-up chain must take its starting deadline from the \
             variant it was given — reading a deadline from anywhere else would \
             detach the burst from the reservation window it is bounded by"
        );

        // (1d) #4863: the correlation record MUST be written BEFORE the spawn.
        // The retry's conditional gate READS that record to decide whether the
        // first request already landed; a task spawned before the record exists
        // could observe "absent", conclude "landed", and skip the heal. #5510:
        // the record is in `reserve_resync_emit` (asserted present above), and
        // the entry point calls it before the spawn — asserted as
        // `reserve < followups` at the top of this test. Re-state it here with
        // the record's own anchor so deleting the record is caught too.
        assert!(
            fn_body("async fn reserve_resync_emit(").contains(".record(*key.id(), sender_addr)"),
            "reserve_resync_emit must record the outstanding request for \
             receive-arm correlation (#4864 round-8), and it must do so there \
             because the entry point calls it before spawning the follow-ups"
        );

        // (2) The retry body RE-DELIVERS the one already-authorized emit: it must
        // NOT re-consult the per-sender throttle (begin_resync_request), the
        // global emit cap (resync_emit_limiter), re-record the correlation entry
        // (`.record(`), or CONSUME it (`.consume(` — that one-shot authorization
        // belongs to the receive arm; consuming it here would make the genuine
        // ResyncResponse look unsolicited). So the retry creates no new
        // reservation and consumes no extra global token (storm bound #4251 /
        // #4864 cap).
        let retry = fn_body("async fn resend_dropped_broadcast_resync_request(");
        assert!(
            !retry.contains("begin_resync_request")
                && !retry.contains("resync_emit_limiter")
                && !retry.contains(".record(")
                && !retry.contains(".consume("),
            "resend_dropped_broadcast_resync_request must NOT re-consult the throttle or \
             the global emit cap, and must neither re-record NOR consume the \
             outstanding-request entry — its sends belong to the caller's single \
             granted reservation, and the entry's consumption belongs to the \
             ResyncResponse receive arm (storm bound #4251 / #4864 cap)"
        );

        // (2a) #4863: every re-dispatch is GATED on the outstanding-request entry
        // still being present (read-only). A blind retry re-sends a request that
        // already landed, which makes the sender clear its summary and ship FULL
        // STATE again onto a receiver whose queue was just full.
        let landed_gate = retry.find("is_outstanding(").expect(
            "the retry must gate each re-dispatch on \
             outstanding_resync_requests.is_outstanding(...) (#4863)",
        );
        let retry_emit = retry
            .find("try_notify_node_event(")
            .expect("the retry must emit via the non-blocking try_notify_node_event");
        assert!(
            landed_gate < retry_emit,
            "the #4863 landed-gate ({landed_gate}) MUST be checked BEFORE the \
             re-dispatch ({retry_emit}) — checking after would send the redundant \
             request anyway"
        );
        assert!(
            retry.contains("in 1..=QUEUE_FULL_RESYNC_MAX_RETRIES"),
            "the retry loop must be bounded by QUEUE_FULL_RESYNC_MAX_RETRIES"
        );
        assert!(
            retry.contains("try_notify_node_event"),
            "retries must use the NON-blocking try_notify_node_event \
             (bounded-channel rule; blocking would pin the task up to 30s)"
        );
        // (3) Both the delay pacing and the stop condition are driven by the
        // throttle's OWN clock (`interest_manager.now()`), not tokio's or a
        // fresh wall clock, so sims that advance the injected clock are
        // deterministic and the burst can never cross the reservation window
        // (review P2-A/P2-B).
        assert!(
            retry.contains("interest_manager.now()"),
            "the retry must pace + gate on interest_manager.now() (the throttle's \
             injected clock), not tokio's independent clock (P2-B)"
        );
        assert!(
            retry.contains("reservation_deadline"),
            "the retry must stop once interest_manager.now() reaches the \
             reservation deadline (P2-A)"
        );
        // (4) A tokio-clock liveness backstop floors the task at one reservation
        // interval so a DECOUPLED injected clock that freezes before its deadline
        // cannot make the poll spin forever (DST safety). The injected deadline
        // above stays primary; this is purely an additional floor.
        assert!(
            retry.contains("started.elapsed()") && retry.contains("RESYNC_REQUEST_MIN_INTERVAL"),
            "the retry must have a tokio-clock liveness backstop \
             (started.elapsed() >= RESYNC_REQUEST_MIN_INTERVAL) so a frozen \
             injected clock cannot make it spin — while keeping the injected \
             reservation deadline as the primary stop"
        );
    }

    /// Issue #4857 (P2): the whole trailing-retry burst, measured from
    /// task-start on the throttle's clock, comfortably fits inside the single
    /// 30s `RESYNC_REQUEST_MIN_INTERVAL` reservation. This is a belt-and-braces
    /// check: the retry is ALSO hard-stopped at the reservation deadline (see
    /// `queue_full_resync_retry_stops_after_reservation_deadline`), so it can
    /// never spill into the next reservation even if this span check regressed.
    /// Pins the worst-case span against the constants.
    #[test]
    fn resync_retry_span_stays_within_throttle_window() {
        // Worst case = sum_{n=1..=MAX} n * BASE * 1.2 (largest jitter factor).
        let max = QUEUE_FULL_RESYNC_MAX_RETRIES as u64;
        let triangular = max * (max + 1) / 2; // sum of 1..=MAX
        let worst_case = QUEUE_FULL_RESYNC_RETRY_BASE_DELAY.mul_f64(triangular as f64 * 1.2);
        // RESYNC_REQUEST_MIN_INTERVAL is 30s (crate::ring::interest.rs); kept
        // private there, so pin the literal here with a cross-reference.
        assert!(
            worst_case < Duration::from_secs(30),
            "worst-case retry span {worst_case:?} must stay under the 30s \
             RESYNC_REQUEST_MIN_INTERVAL reservation window (interest.rs)"
        );
        // #4863: the whole retry span must ALSO stay inside the correlation
        // entry's TTL. The conditional gate reads `is_outstanding`, which goes
        // false once the entry expires — so a TTL at or below the retry span
        // would silently turn the #4862 heal into a no-op for later attempts
        // (they would read "landed" and skip) with entirely green CI. Nothing
        // else couples these two constants, so pin the relationship here.
        assert!(
            worst_case < crate::ring::resync_rate_limit::OUTSTANDING_RESYNC_TTL,
            "worst-case retry span {worst_case:?} must stay under the outstanding \
             correlation TTL {:?} — otherwise a later attempt reads its own \
             expired entry as 'landed' and silently skips the #4862 heal (#4863)",
            crate::ring::resync_rate_limit::OUTSTANDING_RESYNC_TTL
        );
    }

    /// Issue #4857 (P2): the jittered retry delay must stay within the ±20%
    /// band around `attempt * QUEUE_FULL_RESYNC_RETRY_BASE_DELAY` the
    /// retry/backoff rule mandates — never below 0.8× (spin-loop guard) and
    /// strictly below 1.2× (half-open `random_range`). Sampled across many
    /// seeds so a bad jitter formula can't slip through on one lucky seed.
    #[test]
    fn jittered_resync_retry_delay_stays_within_bounds() {
        for attempt in 1..=QUEUE_FULL_RESYNC_MAX_RETRIES {
            let lo = QUEUE_FULL_RESYNC_RETRY_BASE_DELAY.mul_f64(attempt as f64 * 0.8);
            let hi = QUEUE_FULL_RESYNC_RETRY_BASE_DELAY.mul_f64(attempt as f64 * 1.2);
            for seed in 0..128u64 {
                let _guard = GlobalRng::seed_guard(seed);
                for _ in 0..16 {
                    let d = jittered_resync_retry_delay(attempt);
                    assert!(
                        d >= lo,
                        "delay {d:?} below 0.8× for attempt {attempt} (seed {seed})"
                    );
                    assert!(
                        d < hi,
                        "delay {d:?} at/above 1.2× for attempt {attempt} (seed {seed})"
                    );
                }
            }
        }
    }

    /// Issue #4251 follow-up: the four `run_relay_*` driver wrappers each
    /// log at WARN when the inner driver returns an error. PR #4253 gated
    /// the amplification side effects (auto-fetch, ResyncRequest) on
    /// `is_contract_queue_full()` inside the drivers, but left the WARN at
    /// the wrapper boundary unconditional. On a hot contract (production
    /// `4PjqN55KUCidW8vJvw5fhy5fe5maxXKNrWSyK33QjjVq` saturating its
    /// per-contract queue) the broadcast wrapper alone emitted
    /// ~40 WARNs/sec — 148k lines in a single hour on `nova`. Each wrapper
    /// MUST drop queue-full to DEBUG with `event = "queue_full"` and keep
    /// other errors at WARN. Regressing any of these re-opens the spam.
    #[test]
    fn run_relay_wrappers_gate_queue_full_log_severity() {
        let src = include_str!("op_ctx_task.rs");
        for wrapper in [
            "async fn run_relay_request_update(",
            "async fn run_relay_broadcast_to(",
            "async fn run_relay_request_update_streaming(",
            "async fn run_relay_broadcast_to_streaming(",
        ] {
            let start = src
                .find(wrapper)
                .unwrap_or_else(|| panic!("{wrapper} not found"));
            let after = &src[start + 1..];
            let end = after
                .find("\nasync fn ")
                .or_else(|| after.find("\n#[cfg(test)]"))
                .unwrap_or(after.len());
            let body = &src[start..start + 1 + end];

            assert!(
                body.contains("is_contract_queue_full()"),
                "{wrapper} must gate its WARN log on \
                 err.is_contract_queue_full() — see issue #4251 and PR #4253"
            );
            assert!(
                body.contains("event = \"queue_full\""),
                "{wrapper} must tag the DEBUG branch with \
                 event = \"queue_full\" so log filtering / telemetry can \
                 distinguish queue-full backpressure from real failures"
            );
            assert!(
                body.contains("tracing::debug!") && body.contains("tracing::warn!"),
                "{wrapper} must keep BOTH a debug! (queue_full) and a warn! \
                 (real failures) call — an inversion that maps queue_full to \
                 warn would re-open the spam"
            );
        }
    }

    /// Pin: `BroadcastToStreaming` driver must classify failures via
    /// `log_broadcast_to_streaming_failure` and spawn auto-fetch or
    /// summary-back based on the result (mirror of slice A invariant).
    #[test]
    fn broadcast_to_streaming_classifies_failures() {
        let src = include_str!("op_ctx_task.rs");
        let start = src
            .find("async fn apply_streaming_broadcast(")
            .expect("apply_streaming_broadcast not found");
        let after = &src[start + 1..];
        let end = after
            .find("\nasync fn ")
            .or_else(|| after.find("\n#[cfg(test)]"))
            .unwrap_or(after.len());
        let driver_src = &src[start..start + 1 + end];

        assert!(
            driver_src.contains("log_broadcast_to_streaming_failure"),
            "apply_streaming_broadcast must call \
             log_broadcast_to_streaming_failure for failure classification"
        );
        assert!(
            driver_src.contains("try_auto_fetch_contract"),
            "apply_streaming_broadcast must call try_auto_fetch_contract \
             on real (non-benign) failures for self-heal"
        );
        assert!(
            driver_src.contains("send_summary_back_on_rejection"),
            "apply_streaming_broadcast must spawn \
             send_summary_back_on_rejection on is_invalid_update_rejection"
        );
    }

    /// Pin: `BroadcastToStreaming` driver must spawn the proactive
    /// summary notification on successful state change (mirrors slice
    /// A invariant at `broadcast_to_spawns_proactive_summary`).
    #[test]
    fn broadcast_to_streaming_spawns_proactive_summary() {
        let src = include_str!("op_ctx_task.rs");
        let start = src
            .find("async fn apply_streaming_broadcast(")
            .expect("apply_streaming_broadcast not found");
        let after = &src[start + 1..];
        let end = after
            .find("\nasync fn ")
            .or_else(|| after.find("\n#[cfg(test)]"))
            .unwrap_or(after.len());
        let driver_src = &src[start..start + 1 + end];
        assert!(
            driver_src.contains("send_proactive_summary_notification"),
            "apply_streaming_broadcast must spawn \
             send_proactive_summary_notification on successful state change"
        );
    }

    /// Pin: the streaming RAII guard must remove from
    /// `active_relay_update_txs` and touch the slice-C-specific
    /// counters on drop (not the slice A counters).
    #[test]
    fn streaming_raii_guard_clears_dedup_and_streaming_counters() {
        let src = include_str!("op_ctx_task.rs");
        let drop_start = src
            .find("impl Drop for RelayUpdateStreamingInflightGuard")
            .expect("RelayUpdateStreamingInflightGuard Drop impl not found");
        let drop_body = &src[drop_start..drop_start + 700];
        assert!(
            drop_body.contains("active_relay_update_txs"),
            "streaming guard Drop must remove from active_relay_update_txs"
        );
        assert!(
            drop_body.contains("RELAY_UPDATE_STREAMING_INFLIGHT.fetch_sub"),
            "streaming guard Drop must decrement RELAY_UPDATE_STREAMING_INFLIGHT"
        );
        assert!(
            drop_body.contains("RELAY_UPDATE_STREAMING_COMPLETED_TOTAL.fetch_add"),
            "streaming guard Drop must increment \
             RELAY_UPDATE_STREAMING_COMPLETED_TOTAL"
        );
    }

    /// Pin: `log_broadcast_to_streaming_failure` must stay
    /// `pub(crate)` so the streaming driver can reuse the shared
    /// classifier. Re-privatising it would silently break the
    /// driver's failure branch.
    #[test]
    fn log_broadcast_to_streaming_failure_is_pub_crate() {
        let src = include_str!("../update.rs");
        assert!(
            src.contains("pub(crate) fn log_broadcast_to_streaming_failure("),
            "log_broadcast_to_streaming_failure must remain pub(crate) — \
             reused by apply_streaming_broadcast"
        );
    }

    /// Pin: the deprecated `UpdateMsg::Broadcasting` variant has been
    /// removed. Re-introducing it would shift bincode discriminant
    /// tags and silently break wire compatibility with peers gated by
    /// the bumped `min-compatible-version`. If a future change needs
    /// a new variant, append it at the end of the enum to preserve
    /// existing tags.
    #[test]
    fn deprecated_broadcasting_variant_must_not_return() {
        let src = include_str!("../update.rs");
        assert!(
            !src.contains("Broadcasting {"),
            "UpdateMsg::Broadcasting must remain deleted; appending new variants \
             at the end of UpdateMsg preserves bincode discriminant tags."
        );
    }

    /// Issue #4010: `deliver_outcome` must record the UPDATE outcome to
    /// the dashboard `op_stats.updates` counter (the driver bypass
    /// at `handle_pure_network_message_v1` skips the legacy
    /// `report_result` recording path) and gate the call on
    /// `!client_tx.is_sub_operation()` for parity with SUBSCRIBE.
    /// Walks brace depth so the test does not depend on any nearby
    /// section comments staying named the same.
    #[test]
    fn deliver_outcome_records_update_op_result() {
        const SOURCE: &str = include_str!("op_ctx_task.rs");
        let prod = production_source(SOURCE);
        let body = extract_fn_body(prod, "fn deliver_outcome(");

        assert!(
            body.contains("record_op_result"),
            "deliver_outcome must call record_op_result so the dashboard \
             UPDATE counter advances on driver terminal replies. \
             Issue #4010."
        );
        assert!(
            body.contains("OpType::Update"),
            "record_op_result inside deliver_outcome must be passed \
             OpType::Update (not Get/Put/Subscribe)."
        );
        assert!(
            body.contains("!client_tx.is_sub_operation()"),
            "deliver_outcome must gate record_op_result on \
             `!client_tx.is_sub_operation()` (note the leading `!`) for \
             parity with SUBSCRIBE; defensive against a future change \
             that routes a sub-op tx through run_client_update and \
             silently inflates the user-facing dashboard counter. \
             Issue #4010."
        );
    }

    /// Pure-function unit test: `classify_update_outcome_for_op_stats`
    /// covers every `DriverOutcome` variant. Pins the success/failure
    /// mapping against accidental `success: !success` flips. Issue #4010.
    #[test]
    fn classify_update_outcome_covers_all_variants() {
        use freenet_stdlib::client_api::{ContractResponse, ErrorKind, HostResponse};
        use freenet_stdlib::prelude::{CodeHash, ContractInstanceId, ContractKey, StateSummary};

        let key = ContractKey::from_id_and_code(
            ContractInstanceId::new([7u8; 32]),
            CodeHash::new([8u8; 32]),
        );
        let publish_ok = DriverOutcome::Publish(Ok(HostResponse::ContractResponse(
            ContractResponse::UpdateResponse {
                key,
                summary: StateSummary::from(Vec::<u8>::new()),
            },
        )));
        let publish_err = DriverOutcome::Publish(Err(ErrorKind::OperationError {
            cause: "synthetic".into(),
        }
        .into()));
        let infra = DriverOutcome::InfrastructureError(OpError::UnexpectedOpState);

        assert!(classify_update_outcome_for_op_stats(&publish_ok));
        assert!(!classify_update_outcome_for_op_stats(&publish_err));
        assert!(!classify_update_outcome_for_op_stats(&infra));
    }

    /// Regression for #4066: when the originator is non-hosting and
    /// `super::update_contract` fails because the local executor lacks
    /// contract code/params (the "missing contract parameters" error
    /// from `runtime.rs:920-953`), the client-initiated remote-target
    /// branch must self-heal by triggering `try_auto_fetch_contract`
    /// against the chosen forward target and synthesise a retriable
    /// `OperationError` instead of returning a raw `OpError` to the
    /// driver loop. Without this, every UPDATE from a non-hosting
    /// originator returns a hard failure to the client and never
    /// primes the local store, so subsequent retries also fail.
    ///
    /// The discriminator is the NARROW
    /// `is_missing_contract_parameters` predicate, NOT the broader
    /// `!is_contract_exec_rejection` (skeptical-review callout on
    /// PR #4072). The broad form negates errors like `Deser`,
    /// `InvalidState`, `InvalidDelta`, `Other`, `DoublePut`,
    /// `InvalidArrayLength`, and storage errors — auto-fetching on
    /// any of those is wasted work and an amplification vector
    /// (every malformed delta would trigger a wire-level GET).
    #[test]
    fn drive_client_update_remote_target_auto_fetches_on_missing_params() {
        const SOURCE: &str = include_str!("op_ctx_task.rs");
        let prod = production_source(SOURCE);
        let body = extract_fn_body(prod, "async fn drive_client_update(");

        // Locate the remote-target arm. The `Some(target) =>` arm of the
        // match on the proximity/ring-routed target peer is the only
        // place `update_contract` is called for forwarding.
        let arm = body
            .find("Some(target) =>")
            .expect("drive_client_update must have a Some(target) remote-target arm");
        let arm_body = &body[arm..];

        assert!(
            arm_body.contains("is_missing_contract_parameters"),
            "remote-target arm must distinguish missing-code/params errors \
             via the NARROW `is_missing_contract_parameters` predicate, \
             NOT `!is_contract_exec_rejection` — see #4066. Auto-fetching \
             on the broader negation triggers unnecessary GETs for \
             malformed-input and storage failures."
        );
        assert!(
            !arm_body.contains("!err.is_contract_exec_rejection()"),
            "remote-target arm must NOT use the broad \
             `!is_contract_exec_rejection` discriminator — see the \
             skeptical-review callout on PR #4072 #4. Use the narrow \
             `is_missing_contract_parameters` predicate instead."
        );
        assert!(
            arm_body.contains("try_auto_fetch_contract"),
            "remote-target arm must call `op_manager.try_auto_fetch_contract` \
             when the local apply fails with the missing-params error so the \
             contract code/params get pulled from the chosen target peer. \
             See #4066."
        );
    }

    /// The local-only branch (`None =>`, no remote target) must
    /// surface `is_hosting_contract` / `state_store` divergence
    /// explicitly instead of letting it bubble as an opaque
    /// `OpError`. There is no remote peer to auto-fetch from, so the
    /// only correct behavior is a structured error so operators can
    /// correlate the inconsistency. Skeptical-review callout on
    /// PR #4072 #1.
    #[test]
    fn drive_client_update_local_branch_surfaces_ring_state_store_divergence() {
        const SOURCE: &str = include_str!("op_ctx_task.rs");
        let prod = production_source(SOURCE);
        let body = extract_fn_body(prod, "async fn drive_client_update(");

        // Locate the local-only branch (no remote target).
        let arm = body
            .find("None =>")
            .expect("drive_client_update must have a None local-only arm");
        // Bound at the next match arm.
        let arm_body = &body[arm..];
        let arm_end = arm_body[1..]
            .find("Some(target) =>")
            .map(|p| p + 1)
            .unwrap_or(arm_body.len());
        let arm_slice = &arm_body[..arm_end];

        assert!(
            arm_slice.contains("is_missing_contract_parameters"),
            "local-only arm must explicitly handle the \
             `is_missing_contract_parameters` failure — see #4066. \
             Without this, an `is_hosting_contract`/`state_store` \
             divergence surfaces as an opaque OpError rather than a \
             structured error operators can act on."
        );
        assert!(
            arm_slice.contains("ring_state_store_inconsistency"),
            "local-only arm must log the divergence with phase = \
             \"ring_state_store_inconsistency\" so the inconsistency \
             is searchable in operator dashboards (#4066)"
        );
    }

    /// Truncate the source at the `#[cfg(test)]` marker so the pin
    /// tests don't see test-module code or comments (avoids false
    /// positives where a test asserts a substring that also appears in
    /// a sibling test).
    fn production_source(full: &str) -> &str {
        let cutoff = full
            .find("#[cfg(test)]")
            .expect("file must have a #[cfg(test)] section");
        &full[..cutoff]
    }

    /// Walk brace depth from the opening `{` of the named fn to the
    /// matching `}`. Returns the body slice. Robust to nearby section
    /// comments being renamed.
    fn extract_fn_body<'a>(source: &'a str, signature_prefix: &str) -> &'a str {
        let start = source
            .find(signature_prefix)
            .unwrap_or_else(|| panic!("could not find {signature_prefix}"));
        let brace = source[start..]
            .find('{')
            .expect("fn signature must have a body");
        let body_start = start + brace + 1;
        let bytes = source.as_bytes();
        let mut depth: i32 = 1;
        let mut i = body_start;
        while i < bytes.len() {
            match bytes[i] {
                b'{' => depth += 1,
                b'}' => {
                    depth -= 1;
                    if depth == 0 {
                        return &source[body_start..i];
                    }
                }
                _ => {}
            }
            i += 1;
        }
        panic!("unterminated fn body for {signature_prefix}");
    }

    /// Issue #4828: `record_update_received` must be recorded by BOTH
    /// broadcast-apply paths, not just the streaming one. Before #4828 the
    /// sole call site in the crate sat in the streaming path, but
    /// `broadcast_queue.rs` only picks the streaming variant above
    /// `streaming_threshold` (default 64 KB), so ordinary small deltas —
    /// the normal River case — were invisible. `cards.rs` renders
    /// `updates_received` as the ONLY UPDATE number for subscriber nodes,
    /// so an actively-receiving subscriber displayed 0.
    ///
    /// Both paths must record AFTER their `if !changed` early-return, so
    /// the counter tracks updates that actually mutated local state rather
    /// than no-op re-broadcasts. The streaming path applies via the
    /// extracted `apply_streaming_broadcast` helper (#4857), so its record
    /// lives there and the streaming driver must delegate to it — pinned
    /// below so the delegation cannot silently break the streaming record.
    #[test]
    fn both_broadcast_drivers_record_update_received() {
        let full = include_str!("op_ctx_task.rs");
        let src = production_source(full);
        // Each broadcast path maps to the function that owns its `!changed`
        // guard and its record call: the non-streaming driver records
        // inline, the streaming driver records via `apply_streaming_broadcast`.
        for sig in [
            "async fn drive_relay_broadcast_to(",
            "async fn apply_streaming_broadcast(",
        ] {
            let body = extract_fn_body(src, sig);
            // Exactly once: a second call double-counts every changed
            // broadcast on this path; zero calls rots the counter.
            let record_calls = body.matches("record_update_received()").count();
            assert_eq!(
                record_calls, 1,
                "{sig} must call record_update_received() EXACTLY once — \
                 `updates_received` is the only UPDATE number the dashboard \
                 shows for subscriber nodes, and broadcast_queue.rs picks the \
                 NON-streaming variant for every payload under \
                 streaming_threshold (64 KB), i.e. the normal small-delta \
                 case. Found {record_calls}. Issue #4828."
            );
            // Must sit AFTER the entire `if !changed { ... }` guard block —
            // not merely after some `return` inside it, which a nested
            // early-return could fake. Brace-match the guard block and
            // require the record after its closing `}`, so no in-block path
            // can record a no-op re-broadcast as a received update.
            let changed_guard = body
                .find("if !changed {")
                .unwrap_or_else(|| panic!("{sig} must keep its `if !changed` early-return"));
            let guard_open = changed_guard
                + body[changed_guard..]
                    .find('{')
                    .expect("the `!changed` guard must have an opening brace");
            let guard_end = {
                let bytes = body.as_bytes();
                let mut depth = 0i32;
                let mut end = None;
                for (i, &b) in bytes.iter().enumerate().skip(guard_open) {
                    match b {
                        b'{' => depth += 1,
                        b'}' => {
                            depth -= 1;
                            if depth == 0 {
                                end = Some(i);
                                break;
                            }
                        }
                        _ => {}
                    }
                }
                end.expect("the `!changed` guard block must have a matching closing brace")
            };
            let record_pos = body
                .find("record_update_received()")
                .expect("checked above");
            assert!(
                record_pos > guard_end,
                "{sig} must call record_update_received() AFTER the entire \
                 `if !changed` guard block (past its closing brace), so no \
                 in-block early-return path can record on a no-op broadcast. \
                 Issue #4828."
            );
        }
        // The streaming driver records via the extracted helper (#4857), so
        // it must delegate to apply_streaming_broadcast EXACTLY once and must
        // NOT record directly — zero delegations means the streaming path
        // never records (counter rots for large payloads); a direct call or a
        // second delegation would double-count.
        let streaming_driver = extract_fn_body(src, "async fn drive_relay_broadcast_to_streaming(");
        let delegations = streaming_driver
            .matches("apply_streaming_broadcast(")
            .count();
        assert_eq!(
            delegations, 1,
            "drive_relay_broadcast_to_streaming must delegate to \
             apply_streaming_broadcast EXACTLY once (found {delegations}); \
             that helper owns the streaming record_update_received() call \
             (#4857). Zero means the streaming path never records; two would \
             double-count. Issue #4828."
        );
        assert!(
            !streaming_driver.contains("record_update_received()"),
            "drive_relay_broadcast_to_streaming must NOT call \
             record_update_received() directly — the record lives in \
             apply_streaming_broadcast (#4857). A direct call here would \
             double-count the streaming path. Issue #4828."
        );
    }

    /// #5090: every received payload is terminally classified by an RAII guard,
    /// and successful no-ops are marked before returning.
    #[test]
    fn both_broadcast_apply_paths_record_receiver_outcome_before_noop_return() {
        let full = include_str!("op_ctx_task.rs");
        let src = production_source(full);
        for sig in [
            "async fn drive_relay_broadcast_to(",
            "async fn apply_streaming_broadcast(",
        ] {
            let body = extract_fn_body(src, sig);
            assert_eq!(
                body.matches("receiver_terminal_guard(").count(),
                1,
                "{sig} must create exactly one terminal receiver guard"
            );
            let record = body
                .find("receiver_terminal.mark_applied(changed, updated_value.len())")
                .unwrap_or_else(|| panic!("{sig} must classify successful apply outcome"));
            let no_op_return = body
                .find("if !changed {")
                .unwrap_or_else(|| panic!("{sig} must retain its no-op early return"));
            assert!(
                record < no_op_return,
                "{sig} must count a successfully executed no-op before returning"
            );
            for marker in [
                "receiver_terminal.mark_dedup();",
                "receiver_terminal.mark_backoff();",
            ] {
                let marker_offset = body.find(marker).unwrap_or_else(|| {
                    panic!("{sig} must classify {marker} before its early return")
                });
                let return_offset =
                    body[marker_offset..]
                        .find("return Ok(());")
                        .unwrap_or_else(|| {
                            panic!("{sig} must retain the early return following {marker}")
                        });
                assert!(
                    return_offset > 0,
                    "{sig} must classify {marker} before its early return"
                );
            }
        }
    }

    /// Pin (telemetry accuracy / scope): `relayed_updates_total` counts
    /// relayed UPDATE *requests* only. Both request entries record
    /// `record_relayed_update`; the broadcast fan-out entries must NOT (they
    /// are update-mesh fan-out, not a relayed request — counting them would
    /// conflate relay volume with fan-out volume).
    #[test]
    fn relayed_update_counter_scope_requests_only() {
        let full = include_str!("op_ctx_task.rs");
        let src = production_source(full);
        for sig in [
            "pub(crate) async fn start_relay_request_update(",
            "pub(crate) async fn start_relay_request_update_streaming(",
        ] {
            assert!(
                extract_fn_body(src, sig).contains("record_relayed_update()"),
                "{sig} must record the relayed UPDATE request counter"
            );
        }
        for sig in [
            "pub(crate) async fn start_relay_broadcast_to(",
            "pub(crate) async fn start_relay_broadcast_to_streaming(",
        ] {
            assert!(
                !extract_fn_body(src, sig).contains("record_relayed_update()"),
                "{sig} must NOT record relayed_updates_total (fan-out, not a \
                 relayed request)"
            );
        }
    }

    // ── Empty `sender_summary_bytes` must not poison the peer-summary cache ──
    //
    // Regression coverage for the poisoning already documented on
    // `PayloadArm::FullNoOurSummary`: every producer builds the wire field as
    // `our_summary…unwrap_or_default()`, so a sender that could not summarize
    // itself ships empty bytes, and the receiver used to cache `Some(empty)`
    // as that peer's summary.

    /// Register a connected peer and return `(addr, PeerKey)`.
    fn connect_test_peer(
        op_manager: &OpManager,
        port: u16,
        loc: f64,
    ) -> (SocketAddr, crate::ring::PeerKey) {
        let keypair = crate::transport::TransportKeypair::new();
        let addr: SocketAddr = format!("127.0.0.1:{port}").parse().unwrap();
        op_manager.ring.connection_manager.add_connection(
            crate::ring::Location::new(loc),
            addr,
            keypair.public().clone(),
            false,
        );
        (addr, crate::ring::PeerKey::from(keypair.public().clone()))
    }

    #[tokio::test]
    async fn broadcast_with_empty_sender_summary_does_not_cache_an_empty_summary() {
        let (op_manager, _rx, _h) =
            build_queue_full_test_node_with_clock("empty-summary-no-cache", None).await;
        let key = crate::operations::test_utils::make_contract_key(7);
        let (addr, peer_key) = connect_test_peer(&op_manager, 12401, 0.3);

        let cached = seed_sender_summary_from_broadcast(&op_manager, &key, &[], addr);

        assert!(!cached, "an empty wire summary must not be cached");
        assert_eq!(
            op_manager
                .interest_manager
                .get_peer_summary(&key, &peer_key),
            None,
            "caching Some(empty) is the #4965-review poisoning: the entry then \
             reports no SummaryMissingReason and looks healthy while being wrong"
        );
    }

    #[tokio::test]
    async fn broadcast_with_real_sender_summary_is_cached() {
        let (op_manager, _rx, _h) =
            build_queue_full_test_node_with_clock("empty-summary-real-cached", None).await;
        let key = crate::operations::test_utils::make_contract_key(8);
        let (addr, peer_key) = connect_test_peer(&op_manager, 12402, 0.4);

        let cached = seed_sender_summary_from_broadcast(&op_manager, &key, &[1, 2, 3], addr);

        assert!(cached, "a real summary must still be cached (#4952)");
        assert_eq!(
            op_manager
                .interest_manager
                .get_peer_summary(&key, &peer_key),
            Some(StateSummary::from(vec![1u8, 2, 3])),
            "the #4952 seeding behaviour must survive the empty-bytes guard"
        );
    }

    #[tokio::test]
    async fn empty_sender_summary_preserves_a_previously_cached_summary() {
        // The load-bearing case for "skip, don't clear": a peer that already
        // has a good cached summary must not lose it because ONE broadcast
        // arrived from a sender that transiently could not summarize itself.
        let (op_manager, _rx, _h) =
            build_queue_full_test_node_with_clock("empty-summary-preserves", None).await;
        let key = crate::operations::test_utils::make_contract_key(9);
        let (addr, peer_key) = connect_test_peer(&op_manager, 12403, 0.5);

        assert!(seed_sender_summary_from_broadcast(
            &op_manager,
            &key,
            &[9, 9],
            addr
        ));

        let cached = seed_sender_summary_from_broadcast(&op_manager, &key, &[], addr);

        assert!(!cached);
        assert_eq!(
            op_manager
                .interest_manager
                .get_peer_summary(&key, &peer_key),
            Some(StateSummary::from(vec![9u8, 9])),
            "an empty broadcast field says the SENDER could not summarize \
             itself; it is not a claim about the peer, so it must never \
             discard a good cached summary (contrast ClearedByNoneReport, \
             where the peer explicitly reported None)"
        );
    }

    #[tokio::test]
    async fn empty_sender_summary_from_an_unknown_peer_is_a_no_op() {
        // Boundary: empty bytes AND an unresolvable sender. Must not panic and
        // must not create an entry.
        let (op_manager, _rx, _h) =
            build_queue_full_test_node_with_clock("empty-summary-unknown-peer", None).await;
        let key = crate::operations::test_utils::make_contract_key(10);
        let unknown: SocketAddr = "127.0.0.1:12499".parse().unwrap();

        assert!(!seed_sender_summary_from_broadcast(
            &op_manager,
            &key,
            &[],
            unknown
        ));
        assert!(!seed_sender_summary_from_broadcast(
            &op_manager,
            &key,
            &[4, 2],
            unknown
        ));
        assert!(
            op_manager
                .interest_manager
                .get_interested_peers(&key)
                .is_empty()
        );
    }

    /// Source pin: BOTH broadcast receive paths must go through the guard.
    ///
    /// The pure tests above cannot catch a driver that re-inlines a bare
    /// `upsert_peer_summary(&key, &sender_key, StateSummary::from(bytes))`,
    /// which is exactly the shape being removed here and exactly the
    /// `.claude/rules/operations.md` failure mode (#3791): a data-layer test
    /// that stays green after the dispatch site is reverted.
    #[test]
    fn both_broadcast_receive_paths_route_through_the_empty_summary_guard() {
        let src = include_str!("op_ctx_task.rs");
        // Cut at `mod tests` (NOT `#[cfg(test)]`) so these needles cannot
        // match this test module via `include_str!`.
        let prod = &src[..src.find("\nmod tests {").expect("tests module not found")];

        for func in [
            "async fn drive_relay_broadcast_to(",
            "async fn apply_streaming_broadcast(",
        ] {
            // Brace-matched, so the window is EXACTLY this function. An
            // earlier version bounded it with `find("\nasync fn ")`, which
            // over-read into the following functions: the positive needle
            // could then be satisfied by a sibling's body, and the negative
            // one could trip on unrelated code while naming the wrong function.
            let body: String = extract_fn_body(prod, func)
                .chars()
                .filter(|c| !c.is_whitespace())
                .collect();

            assert!(
                body.contains("seed_sender_summary_from_broadcast(op_manager,&key,&sender_summary_bytes,sender_addr)"),
                "{func} must seed the sender summary through \
                 seed_sender_summary_from_broadcast, which refuses EMPTY bytes \
                 (not usable evidence about the peer under either cause) \
                 instead of caching Some(empty)"
            );
            assert!(
                !body.contains("upsert_peer_summary("),
                "{func} must not re-inline a bare upsert_peer_summary — that \
                 re-opens the empty-summary poisoning the guard exists to close"
            );
        }
    }

    #[tokio::test]
    async fn empty_sender_summary_still_refreshes_an_existing_interest_ttl() {
        // A broadcast arriving is liveness evidence even when its summary is
        // not usable, so declining to cache must not let the entry age out at
        // INTEREST_TTL. On main the unconditional upsert refreshed the TTL for
        // free; this pins that we kept that side effect.
        let (op_manager, _rx, _h) =
            build_queue_full_test_node_with_clock("empty-summary-ttl", None).await;
        let key = crate::operations::test_utils::make_contract_key(11);
        let (addr, peer_key) = connect_test_peer(&op_manager, 12404, 0.6);

        assert!(seed_sender_summary_from_broadcast(
            &op_manager,
            &key,
            &[5, 5],
            addr
        ));
        let before = op_manager
            .interest_manager
            .get_peer_interest(&key, &peer_key)
            .expect("entry seeded")
            .last_refreshed;

        seed_sender_summary_from_broadcast(&op_manager, &key, &[], addr);

        let entry = op_manager
            .interest_manager
            .get_peer_interest(&key, &peer_key)
            .expect("entry must survive an empty-summary broadcast");
        assert!(
            entry.last_refreshed >= before,
            "an empty-summary broadcast must still refresh the interest TTL"
        );
        assert_eq!(
            entry.summary,
            Some(StateSummary::from(vec![5u8, 5])),
            "refreshing the TTL must not disturb the cached summary"
        );
    }

    #[tokio::test]
    async fn empty_sender_summary_does_not_create_an_interest_entry() {
        // The other half: refresh what exists, never manufacture an entry from
        // bytes that carry no information.
        let (op_manager, _rx, _h) =
            build_queue_full_test_node_with_clock("empty-summary-no-create", None).await;
        let key = crate::operations::test_utils::make_contract_key(12);
        let (addr, _peer_key) = connect_test_peer(&op_manager, 12405, 0.7);

        seed_sender_summary_from_broadcast(&op_manager, &key, &[], addr);

        assert!(
            op_manager
                .interest_manager
                .get_interested_peers(&key)
                .is_empty(),
            "an empty summary must not manufacture an interest entry"
        );
    }

    #[tokio::test]
    async fn single_byte_sender_summary_is_cached() {
        // Boundary against the emptiness check: one byte is a real summary.
        let (op_manager, _rx, _h) =
            build_queue_full_test_node_with_clock("empty-summary-one-byte", None).await;
        let key = crate::operations::test_utils::make_contract_key(13);
        let (addr, peer_key) = connect_test_peer(&op_manager, 12406, 0.8);

        assert!(seed_sender_summary_from_broadcast(
            &op_manager,
            &key,
            &[0u8],
            addr
        ));
        assert_eq!(
            op_manager
                .interest_manager
                .get_peer_summary(&key, &peer_key),
            Some(StateSummary::from(vec![0u8])),
            "a single zero byte is a real summary, not an absent one"
        );
    }
}
