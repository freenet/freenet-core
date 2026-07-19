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

use either::Either;
use freenet_stdlib::client_api::{ContractResponse, ErrorKind, HostResponse};
use freenet_stdlib::prelude::*;

use crate::client_events::HostResult;
use crate::config::GlobalExecutor;
use crate::contract::{ContractHandlerEvent, StoreResponse};
use crate::message::{DeltaOrFullState, InterestMessage, NetMessage, NodeEvent, Transaction};
use crate::node::OpManager;
use crate::operations::OpError;
use crate::operations::bootstrap::bootstrap_gateway_target;
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
            let UpdateExecution {
                value: _,
                summary,
                changed,
                ..
            } = match super::update_contract(
                op_manager,
                key,
                update_data,
                related_contracts,
                crate::contract::Priority::ClientLocal,
            )
            .await
            {
                Ok(execution) => execution,
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
            )
            .await;

            let UpdateExecution {
                value: updated_value,
                summary,
                changed: _,
                ..
            } = match local_apply {
                Ok(execution) => execution,
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
            summary: _,
            changed,
            ..
        } = super::update_contract(
            op_manager,
            key,
            UpdateData::State(State::from(value.clone())),
            related_contracts.clone(),
            crate::contract::Priority::NetworkRelay,
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

/// On a queue-full broadcast drop, emit a RATE-LIMITED `ResyncRequest` to the
/// sender so it clears its poisoned summary cache of us and re-sends the change
/// it believes we already have (#4857).
///
/// A `ContractQueueFull` drop is SILENT: the receiver never applied the delta /
/// full state, but the SENDER cached its own summary as ours on send-Ok
/// (`broadcast_queue.rs::record_delivery_to_interest`), so it believes we are
/// current and will never re-send the dropped change. Left unhealed, a
/// rarely-changing field (member_info, a ban, a config) diverges permanently
/// until the ~5-min InterestSync heartbeat happens to correct it. An inbound
/// `ResyncRequest` makes the sender clear that cached summary (node.rs handler)
/// and re-send full state.
///
/// Issue #4251 suppressed this entirely because one request per dropped delta
/// amplifies into a full-state storm onto the same saturated queue (~40
/// ResyncRequests/sec on a hot contract). The rate limit
/// (`InterestManager::should_send_resync_request`) bounds that to at most one
/// request per (contract, sender) per `RESYNC_REQUEST_MIN_INTERVAL`.
///
/// Shared by both broadcast drivers (`drive_relay_broadcast_to` and
/// `drive_relay_broadcast_to_streaming`) so the two queue-full paths cannot
/// drift. The caller keeps auto-fetch suppressed on queue-full (we already hold
/// the contract; a GET onto the same saturated queue is pure amplification).
///
/// do NOT re-add an unthrottled emission — see #4251/#4857.
async fn send_queue_full_resync_request(
    op_manager: &OpManager,
    key: ContractKey,
    sender_addr: SocketAddr,
    incoming_tx: Transaction,
) {
    if op_manager
        .interest_manager
        .should_send_resync_request(&key, sender_addr)
    {
        tracing::info!(
            tx = %incoming_tx,
            contract = %key,
            target = %sender_addr,
            event = "resync_request_sent",
            reason = "queue_full",
            "UPDATE relay: sending rate-limited ResyncRequest after queue-full drop (#4857)"
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
                "UPDATE relay: failed to send queue-full ResyncRequest"
            );
        }
    } else {
        tracing::debug!(
            tx = %incoming_tx,
            contract = %key,
            sender = %sender_addr,
            event = "queue_full_resync_throttled",
            "UPDATE relay: queue-full ResyncRequest throttled (rate limit) to avoid amplification"
        );
    }
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

    let sender_summary = StateSummary::from(sender_summary_bytes.clone());

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
    let payload_hash = crate::ring::merge_backoff::merge_payload_hash(is_delta, &payload_bytes);
    match op_manager.ring.merge_backoff.check(key.id(), payload_hash) {
        crate::ring::merge_backoff::MergeDecision::Allow => {}
        decision => {
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
    )
    .await;

    let UpdateExecution {
        value: updated_value,
        summary: update_summary,
        changed,
        ..
    } = match update_result {
        Ok(result) => {
            // Merge succeeded → the contract's state advanced, so any prior
            // merge-failure backoff for it is stale. Clear it (#4861).
            op_manager.ring.merge_backoff.record_success(key.id());
            result
        }
        Err(err) => {
            // Record the merge failure for the per-contract backoff (#4861),
            // but ONLY for genuine contract-exec failures (the merge ran and the
            // contract rejected it or it timed out). `is_contract_exec_rejection`
            // excludes queue-full (transient load) and missing-contract failures
            // (healed by the auto-fetch below) — backing either off would be
            // wrong. A timeout gets the longer Timeout-class cooldown.
            if err.is_contract_exec_rejection() {
                let class = if err.is_wasm_timeout() {
                    crate::ring::merge_backoff::MergeFailureClass::Timeout
                } else {
                    crate::ring::merge_backoff::MergeFailureClass::Invalid
                };
                op_manager
                    .ring
                    .merge_backoff
                    .record_failure(key.id(), class, payload_hash);
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
            let queue_full = err.is_contract_queue_full();

            if is_delta && !queue_full {
                // Delta application failed → send ResyncRequest. Mirrors
                // update.rs:710-758. Rate-limited per-contract (#4861): a poison
                // contract that fails deltas from many senders must not drive a
                // full-state resync storm. When the emit is suppressed, skip the
                // sender summary-clear too — it is part of the resync handshake,
                // so clearing it without emitting would just force the sender to
                // full-state us on the next interest cycle.
                if op_manager.ring.resync_emit_limiter.check_and_record(*key.id()) {
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
                        op_manager
                            .interest_manager
                            .update_peer_summary(&key, &sender_key, None);
                    }

                    tracing::info!(
                        tx = %incoming_tx,
                        contract = %key,
                        target = %sender_addr,
                        event = "resync_request_sent",
                        "UPDATE relay: sending ResyncRequest after delta failure"
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
                send_queue_full_resync_request(op_manager, key, sender_addr, incoming_tx).await;
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
    // 100ms-per-contract throttle inside the task.
    let op_mgr = op_manager.clone();
    let summary = update_summary.clone();
    GlobalExecutor::spawn(async move {
        super::send_proactive_summary_notification(&op_mgr, &key, sender_addr, summary).await;
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
pub(crate) async fn start_relay_broadcast_to_streaming(
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

async fn run_relay_broadcast_to_streaming(
    guard: RelayUpdateStreamingInflightGuard,
    op_manager: Arc<OpManager>,
    incoming_tx: Transaction,
    key: ContractKey,
    stream_id: StreamId,
    total_size: u64,
    sender_addr: SocketAddr,
) {
    let _guard = guard;

    if let Err(err) = drive_relay_broadcast_to_streaming(
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
        summary: _,
        changed,
        ..
    } = super::update_contract(
        op_manager,
        key,
        UpdateData::State(State::from(value.clone())),
        related_contracts,
        crate::contract::Priority::NetworkRelay,
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
    )
    .await
}

/// Apply an assembled streaming full-state broadcast (steps 4-9 of
/// `drive_relay_broadcast_to_streaming`): update the sender's cached summary,
/// dedup, WASM-merge the full state, classify failures, and fan out the
/// proactive summary. Extracted from the driver so the #4857 queue-full heal on
/// the streaming path is unit-testable without the transport stream plumbing
/// (steps 1-3: claim + assemble + deserialize).
async fn apply_streaming_broadcast(
    op_manager: &OpManager,
    incoming_tx: Transaction,
    key: ContractKey,
    state_bytes: Vec<u8>,
    sender_summary_bytes: Vec<u8>,
    sender_addr: SocketAddr,
) -> Result<(), OpError> {
    // Step 4: update sender's cached summary.
    let sender_summary = StateSummary::from(sender_summary_bytes.clone());
    if let Some(sender_pkl) = op_manager
        .ring
        .connection_manager
        .get_peer_by_addr(sender_addr)
    {
        let sender_key = crate::ring::PeerKey::from(sender_pkl.pub_key().clone());
        op_manager.interest_manager.update_peer_summary(
            &key,
            &sender_key,
            Some(sender_summary.clone()),
        );
    }

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
    match op_manager.ring.merge_backoff.check(key.id(), stream_payload_hash) {
        crate::ring::merge_backoff::MergeDecision::Allow => {}
        decision => {
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
    )
    .await;

    let UpdateExecution {
        value: updated_value,
        summary: streaming_update_summary,
        changed,
        ..
    } = match update_result {
        Ok(exec) => {
            op_manager.ring.merge_backoff.record_success(key.id());
            exec
        }
        Err(err) => {
            // Record poison-contract merge failures for the backoff (#4861),
            // same classification as the non-streaming driver: only genuine
            // contract-exec rejections, timeout → longer cooldown.
            if err.is_contract_exec_rejection() {
                let class = if err.is_wasm_timeout() {
                    crate::ring::merge_backoff::MergeFailureClass::Timeout
                } else {
                    crate::ring::merge_backoff::MergeFailureClass::Invalid
                };
                op_manager
                    .ring
                    .merge_backoff
                    .record_failure(key.id(), class, stream_payload_hash);
            }

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
            } else if err.is_contract_queue_full() {
                // Issue #4857 on the STREAMING full-state path: identical
                // poisoning to the non-streaming driver. A delivered streaming
                // full-state broadcast caches the peer summary
                // (broadcast_queue.rs → record_streaming_delivery →
                // record_delivery_to_interest), so a queue-full drop here leaves
                // the sender believing we are current while our subsequent CRDT
                // deltas merge onto a diverged base WITHOUT error — the
                // delta-apply-failure heal never fires. Emit the same
                // rate-limited ResyncRequest as the non-streaming path.
                send_queue_full_resync_request(op_manager, key, sender_addr, incoming_tx).await;
            }
            return Err(err);
        }
    };

    // Step 8: telemetry — broadcast applied + proactive summary.
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

    let op_mgr = op_manager.clone();
    let summary = streaming_update_summary.clone();
    GlobalExecutor::spawn(async move {
        super::send_proactive_summary_notification(&op_mgr, &key, sender_addr, summary).await;
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
            .find("merge_backoff.check(")
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

    /// Pin (#4861): a successful merge MUST clear the backoff (state advanced),
    /// and a failure MUST be recorded — but ONLY for genuine contract-exec
    /// rejections. Gating `record_failure` on `is_contract_exec_rejection()`
    /// excludes queue-full (transient load) and missing-contract failures
    /// (healed by auto-fetch); backing either off would block recovery.
    #[test]
    fn broadcast_to_backoff_records_success_and_gated_failure() {
        let driver_src = broadcast_to_driver_src();
        assert!(
            driver_src.contains("merge_backoff.record_success("),
            "drive_relay_broadcast_to must clear the backoff on a successful merge"
        );
        let record_pos = driver_src
            .find(".record_failure(")
            .expect("merge_backoff.record_failure missing in BroadcastTo driver");
        // The record_failure must be inside an is_contract_exec_rejection guard.
        let guard_pos = driver_src
            .find("if err.is_contract_exec_rejection() {")
            .expect("record_failure must be gated on is_contract_exec_rejection");
        assert!(
            guard_pos < record_pos,
            "merge_backoff.record_failure MUST be gated on \
             is_contract_exec_rejection() so queue-full and missing-contract \
             failures do NOT create a backoff entry (#4861)"
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
        let gate_pos = driver_src
            .find("resync_emit_limiter.check_and_record(")
            .expect("ResyncRequest emit must be gated by resync_emit_limiter");
        let summary_clear_pos = driver_src
            .find("update_peer_summary(&key, &sender_key, None)")
            .expect("sender summary-clear missing");
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

    /// Pin (#4861): the streaming full-state driver must carry the same
    /// backoff gate + record wiring (a poison contract must be quarantined on
    /// the streaming path too).
    #[test]
    fn broadcast_to_streaming_has_backoff_wiring() {
        let src = include_str!("op_ctx_task.rs");
        let start = src
            .find("async fn drive_relay_broadcast_to_streaming(")
            .expect("streaming driver not found");
        let after = &src[start + 1..];
        let end = after
            .find("\nasync fn ")
            .or_else(|| after.find("\n#[cfg(test)]"))
            .unwrap_or(after.len());
        let driver_src = &src[start..start + 1 + end];

        let backoff_pos = driver_src
            .find("merge_backoff.check(")
            .expect("streaming driver missing merge_backoff.check gate");
        let merge_pos = driver_src
            .find("super::update_contract(")
            .expect("streaming driver missing update_contract");
        assert!(
            backoff_pos < merge_pos,
            "streaming driver's merge_backoff.check MUST run before update_contract"
        );
        assert!(
            driver_src.contains("merge_backoff.record_success("),
            "streaming driver must clear the backoff on a successful merge"
        );
        assert!(
            driver_src.contains(".record_failure(") && driver_src.contains("is_contract_exec_rejection()"),
            "streaming driver must record gated failures like the non-streaming driver"
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

    /// Issue #4857 / #4251 reconciliation pin. On a queue-full broadcast drop,
    /// BOTH broadcast drivers must emit a RATE-LIMITED `ResyncRequest` — neither
    /// suppress it (permanent divergence, #4857) nor emit it unthrottled
    /// (full-state storm, #4251). Both route through the shared
    /// `send_queue_full_resync_request` helper, which nests the
    /// `InterestMessage::ResyncRequest` emit INSIDE the `should_send_resync_request`
    /// rate-limit gate. Auto-fetch stays suppressed on each driver's queue-full arm.
    #[test]
    fn broadcast_to_sends_throttled_resync_on_queue_full() {
        let src = include_str!("op_ctx_task.rs");

        // Slice a top-level `async fn NAME(` body out of the source (up to the
        // next top-level `async fn` or the test module).
        let fn_body = |name: &str| -> &str {
            let start = src.find(name).unwrap_or_else(|| panic!("{name} not found"));
            let after = &src[start + 1..];
            let end = after
                .find("\nasync fn ")
                .or_else(|| after.find("\n#[cfg(test)]"))
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
        let ns = fn_body("async fn drive_relay_broadcast_to(");
        assert!(
            ns.contains("let queue_full = err.is_contract_queue_full();"),
            "drive_relay_broadcast_to must still classify queue-full (#4251)"
        );
        let ns_arm = queue_full_arm(ns, "} else if queue_full {");
        assert!(
            ns_arm.contains("send_queue_full_resync_request("),
            "drive_relay_broadcast_to queue-full arm MUST delegate to \
             send_queue_full_resync_request (heals #4857)"
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
        let st = fn_body("async fn apply_streaming_broadcast(");
        let st_arm = queue_full_arm(st, "} else if err.is_contract_queue_full() {");
        assert!(
            st_arm.contains("send_queue_full_resync_request("),
            "apply_streaming_broadcast queue-full arm MUST delegate to \
             send_queue_full_resync_request — otherwise #4857 stays OPEN on the \
             streaming full-state path"
        );
        assert!(
            !st_arm.contains("try_auto_fetch_contract"),
            "streaming queue-full arm must NOT auto-fetch (#4251)"
        );

        // The shared helper nests the emit INSIDE the throttle gate: the
        // `should_send_resync_request` check comes BEFORE the emit, which comes
        // BEFORE the throttled `else` branch. An emit outside the gate would
        // re-open the #4251 storm.
        let helper = fn_body("async fn send_queue_full_resync_request(");
        let gate = helper
            .find(".should_send_resync_request(")
            .expect("helper must gate on should_send_resync_request (rate limit, #4251)");
        let emit = helper
            .find("InterestMessage::ResyncRequest")
            .expect("helper must emit InterestMessage::ResyncRequest (heal, #4857)");
        let gate_else = helper
            .find("} else {")
            .expect("helper must have a throttled `else` branch");
        assert!(
            gate < emit && emit < gate_else,
            "the ResyncRequest emit MUST be nested INSIDE the \
             `if should_send_resync_request {{ .. }}` gate (before the throttled \
             `else`), so an unthrottled emission cannot re-open the #4251 storm"
        );
    }

    /// Drain the event-loop notification channel (non-blocking) and return the
    /// targets of every `ResyncRequest(key)` emitted since the last drain.
    fn drain_resync_targets(
        rx: &mut crate::node::EventLoopNotificationsReceiver,
        key: ContractKey,
    ) -> Vec<SocketAddr> {
        let mut targets = Vec::new();
        while let Ok(ev) = rx.notifications_receiver.try_recv() {
            if let Either::Right(NodeEvent::SendInterestMessage {
                target,
                message: InterestMessage::ResyncRequest { key: k },
            }) = ev
            {
                if k == key {
                    targets.push(target);
                }
            }
        }
        targets
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
        // Distinct `id` per caller: `ConfigArgs::build` derives a Local-mode data
        // directory from it, so two concurrently-running tests sharing an id race
        // on that directory (flaky "No such file or directory").
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
        op_manager.ring.connection_manager.set_own_addr(self_addr);

        let handler = tokio::spawn(async move {
            while let Ok((id, ev, _priority)) = ch_channel.recv_from_sender().await {
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
}
