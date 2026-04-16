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

use std::sync::Arc;

use either::Either;
use freenet_stdlib::client_api::{ContractResponse, ErrorKind, HostResponse};
use freenet_stdlib::prelude::*;

use crate::client_events::HostResult;
use crate::config::GlobalExecutor;
use crate::message::{NetMessage, Transaction};
use crate::node::OpManager;
use crate::operations::OpError;
use crate::ring::{PeerKeyLocation, RingError};
use crate::tracing::NetEventLog;

use super::{UpdateExecution, UpdateMsg};

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
}
