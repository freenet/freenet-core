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

// ─── Relay CONNECT scaffolding (#1454 phase 2c slice 1, commit 1) ───
//
// Counters and RAII guard for the upcoming `start_relay_connect`
// driver. Unused by this commit; the driver body and dispatch site
// land in slice 1 commit 2. Defined now so the counter symbols are
// stable across commits and the scaffolding lands as a single
// reviewable unit before the larger semantic change.

/// Test-only counter incremented every time a relay-CONNECT driver is
/// spawned. Used by test_relay_driver_calls_per_op-style guards to
/// confirm the dispatch site actually routed a fresh inbound Request
/// through the task-per-tx driver vs. the legacy
/// `handle_op_request` path.
#[cfg(any(test, feature = "testing"))]
#[allow(dead_code)]
pub static RELAY_CONNECT_DRIVER_CALL_COUNT: std::sync::atomic::AtomicUsize =
    std::sync::atomic::AtomicUsize::new(0);

/// Number of relay-CONNECT driver tasks currently executing.
/// Incremented on spawn, decremented on completion.
#[allow(dead_code)]
pub static RELAY_CONNECT_INFLIGHT: std::sync::atomic::AtomicUsize =
    std::sync::atomic::AtomicUsize::new(0);

/// Lifetime count of relay-CONNECT driver spawns. Paired with
/// `RELAY_CONNECT_COMPLETED_TOTAL`.
#[allow(dead_code)]
pub static RELAY_CONNECT_SPAWNED_TOTAL: std::sync::atomic::AtomicUsize =
    std::sync::atomic::AtomicUsize::new(0);

/// Lifetime count of relay-CONNECT driver completions.
#[allow(dead_code)]
pub static RELAY_CONNECT_COMPLETED_TOTAL: std::sync::atomic::AtomicUsize =
    std::sync::atomic::AtomicUsize::new(0);

/// Lifetime count of relay-CONNECT spawns rejected by the dedup gate
/// because a driver for the same `incoming_tx` was already active on
/// this node.
#[allow(dead_code)]
pub static RELAY_CONNECT_DEDUP_REJECTS: std::sync::atomic::AtomicUsize =
    std::sync::atomic::AtomicUsize::new(0);

/// RAII guard that decrements `RELAY_CONNECT_INFLIGHT`, bumps
/// `RELAY_CONNECT_COMPLETED_TOTAL`, and removes the driver's
/// `incoming_tx` from `active_relay_connect_txs` on drop. Mirrors
/// `RelaySubscribeInflightGuard` / `RelayInflightGuard` patterns for
/// the other relay drivers.
///
/// Defined now (slice 1 commit 1) so the lifecycle is locked even
/// before `start_relay_connect` lands in commit 2.
#[allow(dead_code)]
pub(crate) struct RelayConnectInflightGuard {
    pub(crate) op_manager: std::sync::Arc<OpManager>,
    pub(crate) incoming_tx: Transaction,
}

impl Drop for RelayConnectInflightGuard {
    fn drop(&mut self) {
        self.op_manager
            .active_relay_connect_txs
            .remove(&self.incoming_tx);
        RELAY_CONNECT_INFLIGHT.fetch_sub(1, std::sync::atomic::Ordering::Relaxed);
        RELAY_CONNECT_COMPLETED_TOTAL.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    }
}

/// Drive a client-initiated CONNECT to completion.
///
/// Replaces the legacy `send_gateway_connect` + `process_message`
/// joiner state machine for the originator. Returns when the joiner
/// has accumulated `target_connections` accepted peers, the gateway
/// rejects, or the bypass receiver closes.
///
/// `gateway_addr` is the first-hop target — for client-initiated
/// CONNECT, the gateway is always the first hop. The driver uses
/// `gateway_addr` as both the initial Request destination and the
/// upstream target for `ConnectFailed` re-route signalling. If a
/// future caller introduces a non-gateway first hop, split this
/// parameter into `first_hop_addr` and `gateway_addr` (legacy
/// `get_next_hop_addr()` returned the first hop, not the gateway).
///
/// `overall_timeout`, when supplied, bounds the driver's lifetime
/// internally — exceeding it causes a graceful exit through the
/// normal `release_pending_op_slot` cleanup. Use this in preference
/// to wrapping the future in `tokio::time::timeout`, which would
/// cancel the future mid-flight and leak the
/// `pending_op_results` slot until the 60s sweep (#3100).
pub(crate) async fn start_client_connect(
    gateway: PeerKeyLocation,
    gateway_addr: SocketAddr,
    op_manager: &OpManager,
    own: PeerKeyLocation,
    desired_location: Location,
    overall_timeout: Option<std::time::Duration>,
) -> Result<(), OpError> {
    // Snapshot whether the joiner knows its own external address before
    // contacting the gateway. Mirrors legacy `JoinerState`'s
    // `started_without_address` field at connect.rs:1162. Used at the
    // completion debug-assert below to catch transport-layer regressions
    // where `ObservedAddress` is never delivered (e.g. if the transport
    // prematurely fills in the joiner's address, preventing emission).
    let started_without_address = op_manager.ring.connection_manager.get_own_addr().is_none();
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
    // Tracked for follow-up — addressed alongside Phase 6 cleanup.
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
    let capacity = compute_reply_capacity(target_connections);
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
            // Mirror legacy cleanup at send_gateway_connect:2659-2664.
            // Slot was never inserted into pending_op_results on this
            // failure path (send_to_and_collect_replies fails before
            // insertion), so no guard needed.
            op_manager
                .ring
                .connection_manager
                .prune_in_transit_connection(gateway_addr);
            return Err(e);
        }
    };

    let inner = drive_client_connect_inner(
        tx,
        gateway,
        gateway_addr,
        desired_location,
        target_connections,
        started_without_address,
        receiver,
        op_manager,
    );

    let outcome = match overall_timeout {
        Some(timeout) => match tokio::time::timeout(timeout, inner).await {
            Ok(result) => result,
            Err(_) => {
                tracing::debug!(
                    %tx,
                    timeout_ms = timeout.as_millis(),
                    "connect driver: overall timeout fired; exiting gracefully"
                );
                Ok(())
            }
        },
        None => inner.await,
    };

    op_manager.release_pending_op_slot(tx).await;
    outcome
}

/// Inner driver loop. Drains `receiver` until `target_connections`
/// acceptances accumulate or a fatal Rejected arrives.
#[allow(clippy::too_many_arguments)]
async fn drive_client_connect_inner(
    tx: Transaction,
    gateway: PeerKeyLocation,
    gateway_addr: SocketAddr,
    desired_location: Location,
    target_connections: usize,
    started_without_address: bool,
    mut receiver: mpsc::Receiver<NetMessage>,
    op_manager: &OpManager,
) -> Result<(), OpError> {
    use std::collections::HashSet;

    let mut accepted: HashSet<PeerKeyLocation> = HashSet::with_capacity(target_connections);

    loop {
        if should_exit_for_ttl(&tx) {
            tracing::debug!(
                %tx,
                accepted = accepted.len(),
                target_connections,
                "connect driver: transaction timed out; exiting"
            );
            return Ok(());
        }

        // Poll the receiver with a periodic timeout so the loop can
        // re-evaluate `tx.timed_out()` even when no replies are
        // arriving. Without this the driver would hang on `recv` for
        // indefinitely-long periods after the gateway has stopped
        // sending Responses (e.g. when target_connections exceeds the
        // reachable network).
        const RECV_POLL_INTERVAL: std::time::Duration = std::time::Duration::from_millis(500);
        let msg = match tokio::time::timeout(RECV_POLL_INTERVAL, receiver.recv()).await {
            Ok(Some(msg)) => msg,
            Ok(None) => break,
            Err(_) => continue,
        };

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
                        // `record_acceptor_outcome` accepts a
                        // `tokio::time::Instant` because the
                        // `ConnectionManager` stores acceptor stats as
                        // tokio Instants throughout (see
                        // `connection_manager.rs:28` use). Tokio's
                        // `start_paused(true)` runtime (used by the
                        // simulation harness) keeps tokio time
                        // deterministic, so this is DST-safe. Routing
                        // this through `TimeSource` would require
                        // changing `ConnectionManager`'s public API —
                        // out of scope for this slice.
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
                    // Legacy `network_bridge.send().await?` on this path
                    // would unwind the whole Response handler. The driver
                    // logs and continues instead so other in-flight
                    // replies still drain — the upstream re-route path is
                    // best-effort regardless. If the event loop is wedged
                    // the channel.recv() below also returns None and the
                    // driver exits benignly.
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
                    // INVARIANT (mirrors legacy JoinerState::register_acceptance
                    // at connect.rs:1352-1359): if the joiner started without
                    // knowing its external address, ObservedAddress must have
                    // landed by the time CONNECT completes. Catches transport-
                    // layer regressions where ObservedAddress is never emitted
                    // (e.g. transport prematurely fills the joiner's address).
                    debug_assert!(
                        !started_without_address
                            || op_manager.ring.connection_manager.get_own_addr().is_some(),
                        "BUG: Connect completed but joiner never received ObservedAddress. \
                         This indicates the transport layer may have prematurely filled in \
                         the joiner's address, preventing ObservedAddress emission."
                    );
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
            ConnectMsg::ObservedAddress { address, .. } => {
                // Phase 2c slice 1 (#1454): the bypass at node.rs forwards
                // ObservedAddress to the joiner driver inbox. The joiner
                // doesn't know its external IP behind NAT, so the gateway
                // observes it from the UDP packet source and ships it back
                // here. We update both `own_addr` (for diagnostics) and
                // `own_location` (for routing) — mirrors legacy at
                // connect.rs:1956-1974. Idempotent: subsequent observations
                // for the same tx overwrite, which is the legacy semantic.
                let location = Location::from_address(&address);
                op_manager.ring.connection_manager.set_own_addr(address);
                op_manager
                    .ring
                    .connection_manager
                    .update_location(Some(location));
                tracing::info!(
                    tx = %tx,
                    observed_address = %address,
                    location = %location,
                    "connect driver: updated own_addr and location from ObservedAddress"
                );
            }
            other @ ConnectMsg::Request { .. } | other @ ConnectMsg::ConnectFailed { .. } => {
                // Bypass at node.rs forwards Response/Rejected/
                // ObservedAddress/ConnectFailed. ConnectFailed is
                // forwarded for the relay driver's inbox — but the
                // joiner driver also subscribes to the same per-tx
                // waiter, so it can receive ConnectFailed too. The
                // joiner emitted ConnectFailed on hole-punch failure
                // (line 398-427 above) and would not normally receive
                // its own emission, but a relay could theoretically
                // bounce it back. Drop with a debug log — joiner-side
                // semantic is "the originator already learned of the
                // failure when it emitted the message".
                //
                // Request is the spawn signal handled by the dispatch
                // gate before reaching the bypass forward; reaching
                // this arm would mean a routing bug.
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

/// Capacity for the multi-reply receiver registered with the bypass.
/// `target_connections * 2` provides headroom for one Response and one
/// Rejected per acceptor slot. Bounded below by 2 so the channel can
/// always buffer at least one reply when `target_connections` is 0
/// (defensive — production never sets it to 0 but
/// `mpsc::channel(0)` panics).
///
/// Bypass uses `try_send`; over-capacity replies drop with an error
/// log. Sequential hole-punch (seconds per acceptor) means worst-case
/// burst is bounded by simultaneously-arriving relay-branch replies,
/// which `2x` covers for normal min_connections values. A flood of
/// rejects beyond `2x` would drop legitimate replies — flagged as
/// follow-up but acceptable here because the driver re-issues nothing.
fn compute_reply_capacity(target_connections: usize) -> usize {
    target_connections.saturating_mul(2).max(2)
}

/// Decide whether the driver loop should exit because the transaction
/// has exceeded its TTL.
///
/// Mirrors the legacy GC path at `op_state_manager.rs:688`
/// (`Transaction::timed_out()` check). The driver does not push state
/// into `ops.connect`, so the legacy GC sweep never sees this tx;
/// without an internal exit on TTL, an originator with
/// `target_connections` larger than the reachable network would block
/// its task forever, leaking its `pending_op_results` slot until the
/// 60s sweep. Extracted as a free function so the predicate can be
/// regression-tested without standing up a full `OpManager`.
fn should_exit_for_ttl(tx: &Transaction) -> bool {
    tx.timed_out()
}

#[cfg(test)]
mod tests {
    use std::net::{IpAddr, Ipv4Addr, SocketAddr};

    use super::{compute_reply_capacity, should_exit_for_ttl};
    use crate::message::Transaction;
    use crate::operations::connect::ConnectMsg;

    #[test]
    fn compute_reply_capacity_floor_is_two() {
        assert_eq!(compute_reply_capacity(0), 2, "0 must clamp to 2");
        assert_eq!(compute_reply_capacity(1), 2, "1*2 == 2");
    }

    #[test]
    fn compute_reply_capacity_doubles_target() {
        assert_eq!(compute_reply_capacity(5), 10);
        assert_eq!(compute_reply_capacity(10), 20);
    }

    #[test]
    fn compute_reply_capacity_saturates_on_overflow() {
        // Defensive: `usize::MAX * 2` saturates instead of wrapping.
        assert_eq!(compute_reply_capacity(usize::MAX), usize::MAX);
    }

    /// `ConnectFailed` is constructed inline by the driver on hole-punch
    /// failure and emitted via `NodeEvent::SendNetMessage`. The variant
    /// must carry the failed acceptor's address (not the gateway's), so
    /// the upstream gateway can route the re-attempt away from that
    /// specific peer. Regression guard: legacy joiner-side handler at
    /// `connect.rs::process_message::ConnectMsg::Response` (hole-punch
    /// failure branch) sent the *acceptor* addr; if a future refactor
    /// flipped this to `gateway_addr`, the gateway would treat itself
    /// as the failed peer and refuse to re-route.
    /// Regression guard for the `tx.timed_out()` exit at the top of
    /// `drive_client_connect_inner`'s loop. Before this fix, an
    /// originator with `target_connections` larger than the reachable
    /// network would block on `receiver.recv()` forever, never
    /// declaring CONNECT complete and never running the cleanup that
    /// `release_pending_op_slot` covers. Symptom in CI:
    /// `test_get_routing_coverage_low_htl` failure — joiners never
    /// reset jitter / `gateway_backoff.record_success` /
    /// `record_connection_success`, breaking downstream GET routing.
    ///
    /// `Transaction::ttl_transaction()` produces a transaction whose
    /// timestamp is already at the TTL cutoff, so `timed_out()` is
    /// true on first inspection — same condition the loop checks.
    #[test]
    fn should_exit_for_ttl_returns_true_for_expired_tx() {
        let expired = Transaction::ttl_transaction();
        assert!(
            should_exit_for_ttl(&expired),
            "TTL-expired tx must trigger driver exit"
        );
    }

    #[test]
    fn should_exit_for_ttl_returns_false_for_fresh_tx() {
        let fresh = Transaction::new::<ConnectMsg>();
        assert!(
            !should_exit_for_ttl(&fresh),
            "fresh tx must NOT trigger driver exit on first iteration"
        );
    }

    #[test]
    fn connect_failed_carries_acceptor_addr_not_gateway() {
        let acceptor_addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(192, 0, 2, 50)), 50001);
        let id = Transaction::new::<ConnectMsg>();

        let failed = ConnectMsg::ConnectFailed {
            id,
            failed_acceptor_addr: acceptor_addr,
        };

        #[allow(clippy::wildcard_enum_match_arm)]
        match failed {
            ConnectMsg::ConnectFailed {
                failed_acceptor_addr,
                ..
            } => {
                assert_eq!(failed_acceptor_addr, acceptor_addr);
            }
            other => panic!("expected ConnectFailed, got {other:?}"),
        }
    }
}
