//! Task-per-transaction driver for client-initiated CONNECT (#1454 phase 2c slice 2).
//!
//! Replaces the legacy `WaitingForResponses` joiner state machine. Driver
//! holds joiner state in task locals, sends a single `ConnectMsg::Request`
//! to the first-hop gateway via `OpCtx::send_to_and_collect_replies`, and
//! drains a multi-reply receiver until `target_connections` acceptances
//! land or an overall timeout fires.
//!
//! Side effects mirrored from legacy `process_message::ConnectMsg::Response`:
//!
//! - `NetEventLog::connect_request_sent` before send
//! - `NetEventLog::connect_response_received` per Response
//! - `NodeEvent::ExpectPeerConnection { addr }` per new acceptor
//! - `NodeEvent::ConnectPeer { peer, tx, callback, is_gw: false }` then
//!   await callback (sequential per acceptor — matches legacy)
//! - on hole-punch success: `record_acceptor_outcome(addr, true, now)`
//! - on hole-punch failure: emit `ConnectMsg::ConnectFailed` to gateway,
//!   stay in loop awaiting next Response (legacy behavior)
//! - on `accepted >= target_connections`: reset jitter counter,
//!   `gateway_backoff.record_success(gw_addr)`, `record_connection_success(target_loc)`
//!
//! `Rejected`: `record_connection_failure(desired_location, Rejected)`,
//! `gateway_backoff.record_failure(gw_addr)`, exit driver.
//!
//! `ObservedAddress` stays on legacy `load_or_init` path — that path
//! already does `set_own_addr` / `update_location` as a stateless side
//! effect even when no op exists (connect.rs:1471-1488, 1498-1517).
//!
//! `ConnectFailed` is emitted by this driver but never received by it —
//! it flows downstream toward the relay chain. Relay handling lands in
//! slice 1.

use std::net::SocketAddr;

use either::Either;
use freenet_stdlib::prelude::*;
use tokio::sync::mpsc;

use crate::message::{NetMessage, NetMessageV1, NodeEvent, Transaction};
use crate::node::OpManager;
use crate::operations::OpError;
use crate::ring::{ConnectionFailureReason, Location, PeerKeyLocation};
use crate::tracing::NetEventLog;

use super::{ConnectMsg, ConnectOp};

/// Drive a client-initiated CONNECT to completion.
///
/// Replaces the legacy `send_gateway_connect` + `process_message`
/// joiner state machine for the originator. Returns when the joiner
/// has accumulated `target_connections` accepted peers, the gateway
/// rejects, or the bypass receiver closes.
pub(crate) async fn start_client_connect(
    gateway: PeerKeyLocation,
    gateway_addr: SocketAddr,
    op_manager: &OpManager,
    own: PeerKeyLocation,
    desired_location: Location,
) -> Result<(), OpError> {
    let ttl = op_manager
        .ring
        .max_hops_to_live
        .max(1)
        .min(u8::MAX as usize) as u8;
    let target_connections = op_manager.ring.connection_manager.min_connections;

    let failed_addrs = op_manager.ring.connection_manager.recently_failed_addrs();
    let connected_addrs = op_manager.ring.connection_manager.connected_peer_addrs();
    tracing::debug!(
        failed = failed_addrs.len(),
        connected = connected_addrs.len(),
        "pre-populating bloom filter with excluded peer addresses"
    );
    let mut exclude_addrs = failed_addrs;
    exclude_addrs.extend(connected_addrs);

    // ConnectOp::initiate_join_request still returns a legacy ConnectOp
    // value; slice 2 originator does not push it into ops.connect (the
    // driver owns state in task locals). Drop it. Future cleanup: split
    // the bloom-filter / Request-message construction off into a free
    // helper so the legacy ConnectOp allocation is avoided entirely.
    let (tx, _legacy_op, request_msg) = ConnectOp::initiate_join_request(
        own.clone(),
        gateway.clone(),
        desired_location,
        ttl,
        target_connections,
        op_manager.connect_forward_estimator.clone(),
        &exclude_addrs,
    );

    if let Some(event) = NetEventLog::connect_request_sent(
        &tx,
        &op_manager.ring,
        desired_location,
        own,
        gateway.clone(),
        ttl,
        true, // is_initial
    ) {
        op_manager.ring.register_events(Either::Left(event)).await;
    }

    tracing::debug!(
        gateway = %gateway.pub_key(),
        tx = %tx,
        target_connections,
        ttl,
        "Initiating gateway connect (task-per-tx)"
    );

    let mut ctx = op_manager.op_ctx(tx);
    // capacity = target_connections * 2 (room for one response + one
    // rejection per slot). Bypass uses try_send; over-capacity drops
    // additional replies with an error log.
    let capacity = target_connections.saturating_mul(2).max(2);
    let receiver = match ctx
        .send_to_and_collect_replies(
            gateway_addr,
            NetMessage::V1(NetMessageV1::Connect(request_msg)),
            capacity,
        )
        .await
    {
        Ok(rx) => rx,
        Err(e) => {
            // Mirror legacy cleanup at send_gateway_connect:2659-2664
            op_manager
                .ring
                .connection_manager
                .prune_in_transit_connection(gateway_addr);
            return Err(e);
        }
    };

    let outcome = drive_client_connect_inner(
        tx,
        gateway,
        gateway_addr,
        desired_location,
        target_connections,
        receiver,
        op_manager,
    )
    .await;

    op_manager.release_pending_op_slot(tx).await;
    outcome
}

/// Inner driver loop. Drains `receiver` until `target_connections`
/// acceptances accumulate or a fatal Rejected arrives.
async fn drive_client_connect_inner(
    tx: Transaction,
    gateway: PeerKeyLocation,
    gateway_addr: SocketAddr,
    desired_location: Location,
    target_connections: usize,
    mut receiver: mpsc::Receiver<NetMessage>,
    op_manager: &OpManager,
) -> Result<(), OpError> {
    use std::collections::HashSet;

    let mut accepted: HashSet<PeerKeyLocation> = HashSet::with_capacity(target_connections);

    while let Some(msg) = receiver.recv().await {
        let connect_msg = match msg {
            NetMessage::V1(NetMessageV1::Connect(c)) => c,
            other => {
                tracing::warn!(
                    tx = %tx,
                    "connect driver received unexpected message kind: {:?}",
                    other
                );
                continue;
            }
        };

        match connect_msg {
            ConnectMsg::Response { payload, .. } => {
                if let Some(event) = NetEventLog::connect_response_received(
                    &tx,
                    &op_manager.ring,
                    payload.acceptor.clone(),
                ) {
                    op_manager.ring.register_events(Either::Left(event)).await;
                }

                let acceptor_addr = match payload.acceptor.socket_addr() {
                    Some(a) => a,
                    None => {
                        tracing::warn!(
                            tx = %tx,
                            "connect driver: acceptor missing socket_addr; skipping"
                        );
                        continue;
                    }
                };

                if !accepted.insert(payload.acceptor.clone()) {
                    // Duplicate acceptor — ignore.
                    continue;
                }

                op_manager
                    .notify_node_event(NodeEvent::ExpectPeerConnection {
                        addr: acceptor_addr,
                    })
                    .await?;

                let (callback, mut rx) = mpsc::channel(1);
                op_manager
                    .notify_node_event(NodeEvent::ConnectPeer {
                        peer: payload.acceptor.clone(),
                        tx,
                        callback,
                        is_gw: false,
                    })
                    .await?;

                let hole_punch_ok = match rx.recv().await {
                    Some(Ok((peer_id, _remaining))) => {
                        tracing::info!(
                            %peer_id,
                            tx = %tx,
                            elapsed_ms = tx.elapsed().as_millis(),
                            "connect driver: joined peer"
                        );
                        let now = tokio::time::Instant::now();
                        op_manager.ring.connection_manager.record_acceptor_outcome(
                            acceptor_addr,
                            true,
                            now,
                        );
                        true
                    }
                    Some(Err(_)) => {
                        tracing::warn!(
                            tx = %tx,
                            elapsed_ms = tx.elapsed().as_millis(),
                            "connect driver: ConnectPeer failed"
                        );
                        false
                    }
                    None => {
                        tracing::warn!(
                            tx = %tx,
                            acceptor = %payload.acceptor,
                            "connect driver: ConnectPeer callback closed without result"
                        );
                        false
                    }
                };

                if !hole_punch_ok {
                    accepted.remove(&payload.acceptor);
                    let failed_msg = ConnectMsg::ConnectFailed {
                        id: tx,
                        failed_acceptor_addr: acceptor_addr,
                    };
                    tracing::info!(
                        tx = %tx,
                        failed_acceptor = %acceptor_addr,
                        gateway = %gateway_addr,
                        "connect driver: sending ConnectFailed to gateway for re-routing"
                    );
                    if let Err(e) = op_manager
                        .notify_node_event(NodeEvent::SendNetMessage {
                            target: gateway_addr,
                            msg: Box::new(NetMessage::V1(NetMessageV1::Connect(failed_msg))),
                        })
                        .await
                    {
                        tracing::warn!(
                            tx = %tx,
                            error = %e,
                            "connect driver: failed to emit ConnectFailed"
                        );
                    }
                    continue;
                }

                if accepted.len() >= target_connections {
                    op_manager
                        .ring
                        .connection_manager
                        .reset_connect_jitter_failures();
                    {
                        let mut backoff = op_manager.gateway_backoff.lock();
                        backoff.record_success(gateway_addr);
                    }
                    op_manager.ring.record_connection_success(desired_location);
                    tracing::info!(
                        tx = %tx,
                        accepted = accepted.len(),
                        target_connections,
                        "connect driver: target reached, completing"
                    );
                    return Ok(());
                }
            }
            ConnectMsg::Rejected {
                desired_location: dl,
                ..
            } => {
                tracing::info!(
                    tx = %tx,
                    desired_location = %dl,
                    "connect driver: explicit rejection from relay"
                );
                op_manager
                    .ring
                    .record_connection_failure(dl, ConnectionFailureReason::Rejected);
                {
                    let mut backoff = op_manager.gateway_backoff.lock();
                    backoff.record_failure(gateway_addr);
                }
                return Ok(());
            }
            other @ ConnectMsg::Request { .. }
            | other @ ConnectMsg::ObservedAddress { .. }
            | other @ ConnectMsg::ConnectFailed { .. } => {
                // Driver bypass at node.rs:976-985 forwards only Response
                // and Rejected variants. Reaching this arm means a future
                // change to the bypass `matches!` started routing one of
                // these variants without expanding driver semantics; log
                // and drop. Specifically:
                //   Request          — joiner does not receive Requests
                //   ObservedAddress  — handled by legacy load_or_init
                //                      stateless side effect
                //   ConnectFailed    — emitted by this driver, never
                //                      received by it
                tracing::debug!(
                    tx = %tx,
                    "connect driver: ignoring unexpected variant on bypass: {}",
                    other
                );
            }
        }
    }

    // Channel closed: bypass slot torn down or executor shutting down.
    // Treat as benign completion — accepted acceptors (if any) already
    // landed in the ring via NodeEvent::ConnectPeer.
    tracing::debug!(
        tx = %tx,
        accepted = accepted.len(),
        target_connections,
        gateway = %gateway.pub_key(),
        "connect driver: receiver closed"
    );
    Ok(())
}
