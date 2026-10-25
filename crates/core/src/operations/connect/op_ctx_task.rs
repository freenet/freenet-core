//! Task-per-transaction drivers for CONNECT.
//!
//! The joiner driver holds state in task locals, sends a single
//! `ConnectMsg::Request` to the first-hop gateway via
//! `OpCtx::send_to_and_collect_replies`, and drains a multi-reply
//! receiver until `target_connections` acceptances arrive or an
//! overall timeout fires. A gateway join started with no ring connection
//! dials its acceptors concurrently and, if the gateway does not answer in
//! time, counts toward redialling the gateway link (#5858).
//!
//! Side effects per reply variant:
//!
//! - `Response`: `NetEventLog::connect_response_received`,
//!   `NodeEvent::ExpectPeerConnection`, `NodeEvent::ConnectPeer`,
//!   then await the callback, up to `MAX_CONCURRENT_ACCEPTOR_DIALS`
//!   acceptors at once (the rest queue). On
//!   hole-punch success: `record_acceptor_outcome(addr, true, now)`.
//!   On hole-punch failure: emit `ConnectMsg::ConnectFailed`
//!   downstream; stay in loop awaiting more Responses. When
//!   `accepted >= target_connections`: reset jitter, record gateway
//!   + location success.
//! - `Rejected`: record connection failure, exit driver.
//! - `ObservedAddress`: handled by the joiner driver inbox —
//!   `set_own_addr` / `update_location`.
//! - `ConnectFailed`: emitted by this driver, never received by it.

use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;

use either::Either;
use freenet_stdlib::prelude::*;
use tokio::sync::mpsc;
use tokio::time::Instant;

use crate::message::{NetMessage, NetMessageV1, NodeEvent, Transaction};
use crate::node::{OpManager, WaiterReply};
use crate::operations::OpError;
use crate::ring::{ConnectionFailureReason, Location, PeerKeyLocation};
use crate::tracing::NetEventLog;

use super::{
    ConnectMsg, ConnectRequest, ForwardAttempt, RejectReason, RelayEnv, RelayState,
    dispatch_expect_connection_from,
};

// ─── Relay CONNECT scaffolding ───────────────────────────────────────
//
// Counters and RAII guard for `start_relay_connect`.

/// Test-only counter incremented every time a relay-CONNECT driver is
/// spawned. Used by test_relay_driver_calls_per_op-style guards to
/// confirm the dispatch site actually routed a fresh inbound Request
/// through the driver vs. the legacy
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

/// Why a client-initiated CONNECT was issued, where that changes what the
/// driver does with an acceptance.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ClientConnectKind {
    /// Gateway join, ring acquisition.
    Standard,
    /// Periodic gateway version probe (#3677). Never treated as a bootstrap
    /// join: it runs outside the bootstrap pacer (and is skipped while this
    /// peer has no connection), so a probe that hears nothing does not count
    /// toward redialling the gateway link.
    VersionProbe,
    /// Route-to-self nearest-neighbor lattice probe (`Ring::connection_maintenance`),
    /// issued in scheduler `generation`: an acceptor that is not a lattice edge
    /// is reported as a probe miss for that generation
    /// (`ConnectionManager::record_lattice_probe_result`, #5814).
    LatticeProbe { generation: u64 },
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
///
/// `tx` is supplied by the caller so the joiner-side transaction id
/// can be registered with `LiveTransactionTracker` before the driver
/// task is spawned. Allocating tx inside the driver would race the
/// caller's `add_transaction` registration against the first inbound
/// Response, which can arrive before the spawn completes on a busy
/// runtime.
#[allow(clippy::too_many_arguments)]
pub(crate) async fn start_client_connect(
    tx: Transaction,
    gateway: PeerKeyLocation,
    gateway_addr: SocketAddr,
    op_manager: &OpManager,
    own: PeerKeyLocation,
    desired_location: Location,
    overall_timeout: Option<std::time::Duration>,
    kind: ClientConnectKind,
) -> Result<(), OpError> {
    start_client_connect_counted(
        tx,
        gateway,
        gateway_addr,
        op_manager,
        own,
        desired_location,
        overall_timeout,
        kind,
    )
    .await
    .map(|_| ())
}

/// [`start_client_connect`], returning how many acceptors the CONNECT
/// connected.
#[allow(clippy::too_many_arguments)]
pub(crate) async fn start_client_connect_counted(
    tx: Transaction,
    gateway: PeerKeyLocation,
    gateway_addr: SocketAddr,
    op_manager: &OpManager,
    own: PeerKeyLocation,
    desired_location: Location,
    overall_timeout: Option<std::time::Duration>,
    kind: ClientConnectKind,
) -> Result<usize, OpError> {
    // Count this CONNECT toward the connection_maintenance acquisition throttle.
    // `start_client_connect` is the single entry for every *self-initiated*
    // (originator) CONNECT — ring acquisition (`Ring::acquire_new`), gateway
    // joins (`join_ring_request`), and version probes (`gateway_version_probe`)
    // all route through here — so registering once here keeps the throttle's
    // storm-safety cap honest across all of them. Relayed CONNECTs go through
    // `start_relay_connect` and are deliberately NOT counted (#4348). Registered
    // before the first send below so it precedes any reply or completion.
    op_manager.ring.live_tx_tracker.register_acquisition(tx);

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

    let request_msg =
        super::prepare_join_request(tx, &own, &gateway, desired_location, ttl, &exclude_addrs);

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
        "Initiating gateway connect"
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

    let mut connected = 0;
    let inner = drive_client_connect(
        tx,
        gateway,
        gateway_addr,
        desired_location,
        target_connections,
        started_without_address,
        receiver,
        op_manager,
        kind,
        &mut connected,
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
                Ok(ClientConnectEnd::Finished)
            }
        },
        None => inner.await,
    };
    let outcome = outcome.map(|_| connected);

    op_manager.release_pending_op_slot(tx).await;
    outcome
}

/// Bootstrapping CONNECTs through `gateway_addr` went without the
/// `ObservedAddress` a live gateway sends as soon as it has the request, past
/// `GATEWAY_ANSWER_DEADLINE`, for `SILENT_ROUNDS_BEFORE_REDIAL` rounds in a
/// row (#5858). The gateway has
/// most likely dropped this transport as a zombie, and with no close message
/// in the protocol every later CONNECT on it would vanish too. Drop our end so
/// the next round dials the gateway afresh (a fresh intro on the gateway's
/// side resets a stale session, so this is safe even if the link was alive).
/// Never drops a ring connection.
///
/// Waits (bounded) for the event loop to handle the drop. The next round's
/// CONNECT goes out on the op-execution channel, which the event loop can
/// otherwise serve first, sending it over the link about to be dropped.
///
/// Returns false if the drop could not be queued, so the caller can undo the
/// redial it recorded.
async fn drop_dead_gateway_link(
    op_manager: &OpManager,
    tx: Transaction,
    gateway_addr: SocketAddr,
) -> bool {
    if op_manager.ring.connection_manager.is_in_ring(gateway_addr) {
        return true;
    }
    tracing::info!(
        %tx,
        gateway = %gateway_addr,
        "connect driver: gateway did not answer bootstrapping CONNECTs; dropping the link to redial"
    );
    let (done, mut dropped) = mpsc::channel(1);
    if let Err(e) = op_manager.try_notify_node_event(NodeEvent::DropStaleGatewayLink {
        addr: gateway_addr,
        done,
    }) {
        tracing::warn!(
            %tx,
            gateway = %gateway_addr,
            error = %e,
            "connect driver: could not queue drop of a dead gateway link; next round reuses it"
        );
        return false;
    }
    if tokio::time::timeout(STALE_LINK_DROP_WAIT, dropped.recv())
        .await
        .is_err()
    {
        tracing::debug!(
            %tx,
            gateway = %gateway_addr,
            "connect driver: dead gateway link drop not confirmed in time; continuing"
        );
    }
    true
}

/// Bound on waiting for the event loop to drop a dead gateway link.
const STALE_LINK_DROP_WAIT: std::time::Duration = std::time::Duration::from_secs(2);

/// Inner driver loop. Drains `receiver` until `target_connections`
/// acceptors are connected, a Rejected arrives (after which the dials already
/// started are still finished), or the transaction ends. `connected_dials`
/// counts the acceptors connected, as they connect, so a caller that cuts
/// the driver short still has it.
#[allow(clippy::too_many_arguments)]
async fn drive_client_connect(
    tx: Transaction,
    gateway: PeerKeyLocation,
    gateway_addr: SocketAddr,
    desired_location: Location,
    target_connections: usize,
    started_without_address: bool,
    mut receiver: mpsc::Receiver<WaiterReply>,
    op_manager: &OpManager,
    kind: ClientConnectKind,
    connected_dials: &mut usize,
) -> Result<ClientConnectEnd, OpError> {
    use futures::StreamExt;
    use futures::stream::FuturesUnordered;
    use std::collections::HashSet;

    use super::bootstrap::{
        AcceptorDialQueue, GATEWAY_ANSWER_DEADLINE, MAX_CONCURRENT_ACCEPTOR_DIALS, Offer,
        acceptor_limit,
    };

    let mut accepted: HashSet<PeerKeyLocation> = HashSet::with_capacity(target_connections);
    // `connected_dials`: acceptors actually connected. `accepted` also holds
    // acceptors still queued or being dialled, so it cannot decide that the
    // target is reached.
    // A gateway join sent with no ring connection (#5858), decided once for
    // this transaction. It collects replies to its TTL like any CONNECT (live
    // replies are often slow), but dials concurrently and checks that the
    // gateway is there.
    let bootstrapping = kind == ClientConnectKind::Standard
        && op_manager.ring.connection_manager.connection_count() == 0;
    // The gateway sends `ObservedAddress` as soon as it has the request, but
    // only while this peer's address is unknown; otherwise any reply shows
    // the gateway is there, and silence proves nothing. A join still waiting
    // for it at the deadline counts as silent toward redialling the link,
    // and keeps collecting.
    // Only through a configured gateway: a cached-peer reconnect is also a
    // Standard join at zero connections, but its first hop is no gateway.
    let started = Instant::now();
    let mut heard_gateway = false;
    let through_gateway = op_manager
        .configured_gateways
        .iter()
        .any(|gw| gw.socket_addr() == Some(gateway_addr));
    let mut answer_deadline = (bootstrapping && started_without_address && through_gateway)
        .then(|| started + GATEWAY_ANSWER_DEADLINE);
    // Reply collection ends on a Rejected, the TTL, or the waiter closing, but
    // acceptors already taken are still dialled to the end (bounded by
    // `DIAL_DRAIN_LIMIT`): a failed punch still gets its ConnectFailed, a
    // queued acceptor still gets its ConnectPeer, and a successful one is not
    // counted against the gateway.
    let mut collection_end: Option<(CollectionEnd, Instant)> = None;
    // Acceptors connected when a Rejected arrived: only a dial still in flight
    // then can spare the gateway its backoff.
    let mut connected_at_reject: usize = 0;
    // While bootstrapping, acceptors are dialled concurrently (bounded), so
    // one unreachable acceptor cannot hold the next one past its hole-punch
    // window (#5858). A peer that already has a connection keeps dialling
    // one at a time, as before.
    // Each acceptor carries when its Response arrived, so the dial can
    // report how long it waited in the queue (telemetry).
    let mut dial_queue: AcceptorDialQueue<(PeerKeyLocation, Instant)> = AcceptorDialQueue::new(
        if bootstrapping {
            MAX_CONCURRENT_ACCEPTOR_DIALS
        } else {
            1
        },
        acceptor_limit(target_connections),
    );
    let mut dials = FuturesUnordered::new();

    loop {
        if collection_end.is_none() && should_exit_for_ttl(&tx) {
            tracing::debug!(
                %tx,
                accepted = accepted.len(),
                target_connections,
                "connect driver: transaction timed out; finishing started dials"
            );
            collection_end = Some((CollectionEnd::Ended, Instant::now()));
        }
        if collection_end.is_some() {
            // No more gateway answers are expected; a CONNECT ending is not
            // a gateway's silence.
            answer_deadline = None;
        }
        if answer_deadline.is_some_and(|deadline| Instant::now() >= deadline) {
            answer_deadline = None;
            let redial = op_manager.bootstrap_pacer.lock().gateway_silent(
                gateway_addr,
                started,
                Instant::now(),
            );
            if redial && !drop_dead_gateway_link(op_manager, tx, gateway_addr).await {
                // Not dropped: let the next silent join try again rather
                // than wait out the redial interval for a redial that never
                // happened.
                op_manager
                    .bootstrap_pacer
                    .lock()
                    .redial_not_sent(gateway_addr);
            }
        }
        if let Some((end, since)) = collection_end {
            if !dial_queue.busy() || Instant::now() >= since + DIAL_DRAIN_LIMIT {
                // Gateway rotation backoff (2c). A peer with a connection is
                // always rotated, as before #5858; a bootstrapping one is
                // spared only if a dial still in flight at the Rejected
                // connected.
                let spared = bootstrapping && *connected_dials > connected_at_reject;
                // Overlapping bootstrap CONNECTs already in flight when the
                // gateway was backed off add nothing: the backoff escalates
                // once per rejection of a CONNECT sent after it, as with
                // one round at a time.
                if end == CollectionEnd::Rejected
                    && !spared
                    && (!bootstrapping
                        || op_manager.bootstrap_pacer.lock().gateway_rejected(
                            gateway_addr,
                            started,
                            Instant::now(),
                        ))
                {
                    op_manager
                        .gateway_backoff
                        .lock()
                        .record_failure(gateway_addr);
                }
                return Ok(ClientConnectEnd::Finished);
            }
        }

        // Poll the receiver with a periodic timeout so the loop can
        // re-evaluate `tx.timed_out()` even when no replies are
        // arriving. Without this the driver would hang on `recv` for
        // indefinitely-long periods after the gateway has stopped
        // sending Responses (e.g. when target_connections exceeds the
        // reachable network).
        const RECV_POLL_INTERVAL: std::time::Duration = std::time::Duration::from_millis(500);
        let event = if let Some((_, since)) = collection_end {
            // Collection is over; only the taken acceptors' dials are left.
            match tokio::time::timeout_at(since + DIAL_DRAIN_LIMIT, dials.next()).await {
                Ok(dial) => DriverEvent::Dial(dial),
                Err(_) => continue,
            }
        } else {
            crate::deterministic_select! {
                reply = tokio::time::timeout(RECV_POLL_INTERVAL, receiver.recv()) => DriverEvent::Reply(reply),
                dial = dials.next(), if !dials.is_empty() => DriverEvent::Dial(dial),
            }
        };
        let reply = match event {
            DriverEvent::Reply(reply) => reply,
            DriverEvent::Dial(None) => continue,
            DriverEvent::Dial(Some(AcceptorDial {
                acceptor,
                acceptor_addr,
                result,
            })) => {
                if let Some((next, answered_at)) = dial_queue.finish() {
                    dials.push(begin_acceptor_dial(op_manager, tx, next, answered_at).await?);
                }
                // The address the connection was made to, which can differ from
                // the advertised one when an existing transport is reused.
                let (hole_punch_ok, connected_addr) = match result {
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
                        (true, peer_id)
                    }
                    Some(Err(_)) => {
                        tracing::warn!(
                            tx = %tx,
                            elapsed_ms = tx.elapsed().as_millis(),
                            "connect driver: ConnectPeer failed"
                        );
                        (false, acceptor_addr)
                    }
                    None => {
                        tracing::warn!(
                            tx = %tx,
                            acceptor = %acceptor,
                            "connect driver: ConnectPeer callback closed without result"
                        );
                        (false, acceptor_addr)
                    }
                };

                // Lattice-probe result (#5814). The route-to-self probe aims at
                // the nearest UNCONNECTED peer (and near-terminus relays may also
                // accept). If an acceptor neither fills nor tightens a lattice
                // side, that is evidence the side is tight: report a miss, and
                // once both sides have missed the scheduler sleeps until the
                // lattice changes or its re-check, instead of probing every
                // tau_max and keeping a non-lattice link each time. Classified
                // before the failure branch below can `continue`: at
                // max_connections our own cap refuses a non-lattice acceptor,
                // which arrives here as a failure, but the probe did land there
                // (otherwise a peer at max would never sleep). A lattice edge
                // that could not be connected is reported as a failed hit, so
                // the scheduler re-checks on its retry ladder. An established link
                // is kept: dropping it would leave it dead on the far end until
                // its idle timeout, since the transport has no close message.
                if let ClientConnectKind::LatticeProbe { generation } = kind {
                    let cm = &op_manager.ring.connection_manager;
                    if cm.record_lattice_probe_result(
                        Location::from_address(&connected_addr),
                        connected_addr,
                        generation,
                        hole_punch_ok,
                        cm.at_max_connections(),
                    ) {
                        tracing::debug!(
                            tx = %tx,
                            acceptor = %acceptor_addr,
                            generation,
                            "connect driver: lattice probe found no closer neighbor"
                        );
                    }
                }

                if !hole_punch_ok {
                    accepted.remove(&acceptor);
                    if collection_end.is_some() {
                        // Nobody reads replies any more: a re-route would
                        // only reserve a new acceptor for nothing, and with
                        // the gateway pruned it would start a fresh handshake
                        // outside the pacer for a dead transaction.
                        continue;
                    }
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

                *connected_dials += 1;
                if *connected_dials >= target_connections {
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
                    return Ok(ClientConnectEnd::Finished);
                }
                continue;
            }
        };
        let msg = match reply {
            Ok(Some(WaiterReply::Reply(msg))) => {
                let first_answer = !heard_gateway
                    && (!started_without_address
                        || matches!(
                            &msg,
                            NetMessage::V1(NetMessageV1::Connect(
                                ConnectMsg::ObservedAddress { .. }
                            ))
                        ));
                if bootstrapping && first_answer {
                    heard_gateway = true;
                    answer_deadline = None;
                    op_manager
                        .bootstrap_pacer
                        .lock()
                        .gateway_answered(gateway_addr, Instant::now());
                }
                msg
            }
            Ok(Some(WaiterReply::PeerDisconnected { peer })) => {
                // The gateway we sent the CONNECT Request to was pruned
                // mid-flight (#4313). No further Responses will arrive on
                // this waiter; end collection with whatever was accepted.
                tracing::debug!(
                    %tx,
                    %peer,
                    accepted = accepted.len(),
                    "connect driver: gateway disconnected mid-connect; ending reply collection"
                );
                collection_end = Some((CollectionEnd::Ended, Instant::now()));
                continue;
            }
            Ok(None) => {
                tracing::debug!(
                    tx = %tx,
                    accepted = accepted.len(),
                    target_connections,
                    gateway = %gateway.pub_key(),
                    "connect driver: receiver closed"
                );
                collection_end = Some((CollectionEnd::Ended, Instant::now()));
                continue;
            }
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

                if payload.acceptor.socket_addr().is_none() {
                    tracing::warn!(
                        tx = %tx,
                        "connect driver: acceptor missing socket_addr; skipping"
                    );
                    continue;
                }

                if accepted.contains(&payload.acceptor) {
                    // Duplicate acceptor — ignore.
                    continue;
                }

                // `accepted` only grows with what the queue takes, so both
                // stay within `acceptor_limit`.
                match dial_queue.offer((payload.acceptor.clone(), Instant::now())) {
                    Offer::Dial((acceptor, answered_at)) => {
                        accepted.insert(acceptor.clone());
                        dials.push(
                            begin_acceptor_dial(op_manager, tx, acceptor, answered_at).await?,
                        );
                    }
                    Offer::Queued => {
                        accepted.insert(payload.acceptor.clone());
                    }
                    Offer::Refused => {
                        tracing::debug!(
                            tx = %tx,
                            acceptor = %payload.acceptor,
                            "connect driver: acceptor limit reached; ignoring Response"
                        );
                    }
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
                // Under min_connections a Rejected reflects the *acceptor's*
                // capacity (a saturated gateway neighborhood), NOT a property
                // of our desired location `dl`. Stamping the full escalating
                // 30s→600s location backoff on `dl` (which is a jitter of our
                // OWN location) traps late joiners that cluster near the same
                // saturated gateways: every retry routes into the same wall and
                // re-stamps an ever-longer backoff until the node is parked
                // below min for minutes (the bootstrap dead zone).
                //
                // At/above min: keep the full escalating backoff (a Rejected
                // there is a real "stop probing this region" signal).
                //
                // Below min: apply a SHORT, non-escalating backoff instead of
                // the escalating one. This still throttles THIS connect-driver
                // retry cadence (so the joiner stops re-issuing CONNECTs into
                // the saturated neighborhood every fast-tick, bounding the
                // network-wide rejection storm) but never ramps toward the
                // 600s trap, so the under-connected node keeps probing and 2b's
                // widened re-route can find spare capacity. The maintenance
                // loop still bypasses location backoff entirely under min via
                // should_respect_location_backoff(); this is the connect-driver
                // counterpart. Throttled refinement of the #4348 "under-min
                // ignores location backoff" escape. See #4362.
                let current_connections = op_manager.ring.connection_manager.connection_count();
                let min_connections = op_manager.ring.connection_manager.min_connections;
                if current_connections >= min_connections {
                    op_manager
                        .ring
                        .record_connection_failure(dl, ConnectionFailureReason::Rejected);
                } else {
                    op_manager.ring.record_connection_short_reject_backoff(dl);
                    tracing::debug!(
                        tx = %tx,
                        desired_location = %dl,
                        current_connections,
                        min_connections,
                        "connect driver: under min_connections, applying short \
                         non-escalating reject backoff (#4362)"
                    );
                }
                // Gateway rotation backoff: even under min we want the next
                // attempt to advance to a DIFFERENT gateway so we escape the
                // saturated neighborhood (2c). Recorded once the dials already
                // started have finished (top of the loop), and only if none of
                // them connected.
                connected_at_reject = *connected_dials;
                collection_end = Some((CollectionEnd::Rejected, Instant::now()));
                continue;
            }
            ConnectMsg::ObservedAddress { address, .. } => {
                // The joiner doesn't know its external IP behind NAT;
                // the gateway observes it from the UDP packet source
                // and ships it back. Update both `own_addr` (for
                // diagnostics) and `own_location` (for routing).
                // Idempotent: subsequent observations overwrite.
                let location = Location::from_address(&address);
                // GLOBAL-ADDR-WRITE-OK: production. A real node learning its
                // peer-observed address is exactly what the external_address
                // mirror is for (#4918).
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
}

/// Why a client CONNECT stopped collecting replies.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum CollectionEnd {
    /// A relay rejected the request.
    Rejected,
    /// The TTL passed, the gateway was pruned, or the waiter closed.
    Ended,
}

/// How long a CONNECT that has stopped collecting keeps finishing the dials
/// of acceptors it already took: ten hole-punch deadlines, room for the
/// queued acceptors of a peer that dials one at a time.
pub(crate) const DIAL_DRAIN_LIMIT: std::time::Duration = std::time::Duration::from_secs(
    10 * crate::transport::connection_handler::NAT_TRAVERSAL_DEADLINE.as_secs(),
);

/// How the client CONNECT driver ended.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ClientConnectEnd {
    /// Target reached, transaction expired, rejected, or the waiter closed.
    Finished,
}

/// What woke the client driver.
enum DriverEvent<R, D> {
    Reply(R),
    Dial(Option<D>),
}

/// The outcome of one acceptor dial started by [`begin_acceptor_dial`].
struct AcceptorDial {
    acceptor: PeerKeyLocation,
    acceptor_addr: SocketAddr,
    result: Option<crate::message::ConnectResult>,
}

/// Starts the hole punch to `acceptor`, whose Response arrived at
/// `answered_at`, and returns a future that resolves with its outcome, so
/// several acceptors can be dialled at once.
async fn begin_acceptor_dial(
    op_manager: &OpManager,
    tx: Transaction,
    acceptor: PeerKeyLocation,
    answered_at: Instant,
) -> Result<impl std::future::Future<Output = AcceptorDial> + Send + 'static, OpError> {
    // The Response arm only queues acceptors that have a socket address.
    let acceptor_addr = acceptor.socket_addr().ok_or(OpError::UnexpectedOpState)?;
    op_manager
        .notify_node_event(NodeEvent::ExpectPeerConnection {
            addr: acceptor_addr,
            peer_key: acceptor.pub_key().clone(),
        })
        .await?;

    let (callback, mut rx) = mpsc::channel(1);
    op_manager
        .notify_node_event(NodeEvent::ConnectPeer {
            peer: acceptor.clone(),
            tx,
            callback,
            is_gw: false,
            origin: crate::transport::NatAttemptOrigin::ConnectJoiner,
        })
        .await?;
    // The acceptor started punching when it accepted, before its Response
    // reached us; time spent queued here comes out of its 3 s window. Taken
    // only once the dial is handed to the event loop, so the wait includes
    // any time the two sends above were blocked, and a dial that was never
    // sent is not counted.
    crate::transport::TRANSPORT_METRICS
        .record_dial_queue_wait(acceptor_addr, answered_at.elapsed());
    Ok(async move {
        let result = rx.recv().await;
        AcceptorDial {
            acceptor,
            acceptor_addr,
            result,
        }
    })
}

/// Capacity for the multi-reply receiver registered with the bypass.
/// `target_connections * 2` provides headroom for one Response and one
/// Rejected per acceptor slot. Bounded below by 2 so the channel can
/// always buffer at least one reply when `target_connections` is 0
/// (defensive — production never sets it to 0 but
/// `mpsc::channel(0)` panics).
///
/// Bypass uses `try_send`; over-capacity replies drop with a debug
/// log (`node::try_forward_driver_reply`). The driver drains the
/// receiver while hole punches run (up to
/// `MAX_CONCURRENT_ACCEPTOR_DIALS` at once, the rest queued in the
/// driver, not the channel), so the worst-case burst is bounded by
/// simultaneously-arriving relay-branch replies, which `2x` covers for
/// normal min_connections values. A flood of
/// rejects beyond `2x` would drop legitimate replies — flagged as
/// follow-up but acceptable here because the driver re-issues nothing.
fn compute_reply_capacity(target_connections: usize) -> usize {
    target_connections.saturating_mul(2).max(2)
}

// ─── Relay CONNECT driver ────────────────────────────────────────────
//
// Owns the relay-side `Request → forward + observed + (optional
// accept) + (optional reject)` flow plus the `Response / Rejected
// / ObservedAddress / ConnectFailed` re-entries that arrive for the
// same tx after the initial Request. State (`RelayState`, `recency`,
// `forward_attempts`) lives in driver task locals.
//
// Re-entries arrive via the bypass at
// `node.rs::handle_pure_network_message_v1`, which forwards them
// into the per-tx multi-reply receiver registered by
// `OpCtx::send_to_and_collect_replies`.

const RELAY_RECV_POLL_INTERVAL: std::time::Duration = std::time::Duration::from_millis(500);

/// Spawn a relay driver for a fresh inbound `ConnectMsg::Request`.
///
/// Gated by the dispatch site in `node.rs::handle_pure_network_message_v1`
/// (commit 3) on `source_addr.is_some() && !has_connect_op(id) &&
/// !active_relay_connect_txs.contains(id)`. The driver owns the entire
/// transaction lifetime in task locals — initial `handle_request`
/// decision, downstream forward, upstream emits (ObservedAddress,
/// Response, Rejected), and re-entry processing (Response, Rejected,
/// ObservedAddress, ConnectFailed) — and exits when:
///
/// 1. A successful Response is forwarded upstream (joiner satisfied
///    on this branch),
/// 2. A `Rejected` is forwarded upstream after retries exhausted,
/// 3. The transaction TTL expires (`should_exit_for_ttl`),
/// 4. Or the bypass receiver closes (executor shutdown).
///
/// Returns immediately after spawning. All side effects publish from
/// the spawned task.
///
/// # Dedup gate
///
/// `active_relay_connect_txs.insert(incoming_tx)` returns `false` if
/// a driver for the same tx is already in flight on this node (e.g.
/// gateway retransmit, multi-path Request arriving via two distinct
/// hops). The duplicate is dropped silently — no `Rejected` upstream,
/// because the in-flight driver will service the original. The
/// telemetry counter `RELAY_CONNECT_DEDUP_REJECTS` increments so
/// re-entry storms are visible.
pub(crate) async fn start_relay_connect(
    op_manager: Arc<OpManager>,
    incoming_tx: Transaction,
    payload: ConnectRequest,
    upstream_addr: SocketAddr,
) -> Result<(), OpError> {
    #[cfg(any(test, feature = "testing"))]
    RELAY_CONNECT_DRIVER_CALL_COUNT.fetch_add(1, std::sync::atomic::Ordering::SeqCst);

    if !op_manager.active_relay_connect_txs.insert(incoming_tx) {
        RELAY_CONNECT_DEDUP_REJECTS.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        tracing::debug!(
            tx = %incoming_tx,
            %upstream_addr,
            phase = "relay_connect_dedup_reject",
            "CONNECT relay: duplicate Request for in-flight tx, dropping"
        );
        return Ok(());
    }

    tracing::debug!(
        tx = %incoming_tx,
        desired_location = %payload.desired_location,
        ttl = payload.ttl,
        %upstream_addr,
        phase = "relay_connect_start",
        "CONNECT relay: spawning driver"
    );

    // Construct guard + bump counters BEFORE spawn so the dedup-set
    // entry is paired with a Drop even if the spawned future is dropped
    // before its first poll. Same pattern as PUT/SUBSCRIBE relay.
    RELAY_CONNECT_INFLIGHT.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    RELAY_CONNECT_SPAWNED_TOTAL.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    let guard = RelayConnectInflightGuard {
        op_manager: op_manager.clone(),
        incoming_tx,
    };

    crate::operations::GlobalExecutor::spawn(run_relay_connect(
        guard,
        op_manager,
        incoming_tx,
        payload,
        upstream_addr,
    ));
    Ok(())
}

async fn run_relay_connect(
    guard: RelayConnectInflightGuard,
    op_manager: Arc<OpManager>,
    incoming_tx: Transaction,
    payload: ConnectRequest,
    upstream_addr: SocketAddr,
) {
    let _guard = guard;

    if let Err(err) = drive_relay_connect(&op_manager, incoming_tx, payload, upstream_addr).await {
        tracing::warn!(
            tx = %incoming_tx,
            error = %err,
            phase = "relay_connect_error",
            "CONNECT relay: driver returned error"
        );
    }

    // Release the per-tx pending_op_results slot at driver exit. The
    // multi-reply waiter installed by send_to_and_collect_replies stays
    // in the slot until the 60s sweep otherwise; explicit release is
    // the same pattern used by PUT/SUBSCRIBE/UPDATE relay drivers.
    tokio::task::yield_now().await;
    op_manager.release_pending_op_slot(incoming_tx).await;
}

async fn drive_relay_connect(
    op_manager: &Arc<OpManager>,
    incoming_tx: Transaction,
    payload: ConnectRequest,
    upstream_addr: SocketAddr,
) -> Result<(), OpError> {
    let desired_location = payload.desired_location;

    // Build relay state in task locals. Mirrors `ConnectOp::new_relay`
    // at connect.rs:1184, but the state never leaves this task.
    //
    // Bloom-filter rekeying: incoming `payload.visited.hash_keys` are
    // zeroed (`#[serde(skip)]` over the wire). Re-key against the
    // transaction id so subsequent `mark_visited` / `probably_visited`
    // checks operate on a properly-keyed bloom — same step
    // `new_relay` performs at connect.rs:1193.
    let mut request = payload;
    request.visited = request.visited.with_transaction(&incoming_tx);

    let mut state = RelayState {
        upstream_addr,
        request,
        forwarded_to: None,
        forwarded_at: None,
        observed_sent: false,
        accepted_locally: false,
        response_forwarded: false,
    };
    let mut recency: HashMap<PeerKeyLocation, Instant> = HashMap::new();
    let mut forward_attempts: HashMap<PeerKeyLocation, ForwardAttempt> = HashMap::new();

    // Snapshot the estimator so the driver works on a stable read; the
    // `record_forward_outcome`-equivalent below writes back via the
    // RwLock, matching legacy semantics at connect.rs:1545.
    let estimator = op_manager.connect_forward_estimator.read().clone();

    // Initial Request handling. Admission RAII guard scope is held only
    // across the decision (handle_request + initial outbound emits) so
    // the slot count tracks "concurrent admissions" not "in-flight
    // relay drivers" — the receive loop below runs without the guard.
    let initial_actions = {
        let _admission = match op_manager.ring.connection_manager.try_admit_connect() {
            Some(g) => g,
            None => {
                tracing::info!(
                    tx = %incoming_tx,
                    desired_location = %state.request.desired_location,
                    %upstream_addr,
                    "CONNECT relay: gateway overloaded, rejecting request"
                );
                if let Some(event) = NetEventLog::connect_rejected(
                    &incoming_tx,
                    &op_manager.ring,
                    state.request.desired_location,
                    "gateway overloaded",
                ) {
                    op_manager.ring.register_events(Either::Left(event)).await;
                }
                let mut ctx = op_manager.op_ctx(incoming_tx);
                ctx.send_fire_and_forget(
                    upstream_addr,
                    NetMessage::V1(NetMessageV1::Connect(ConnectMsg::Rejected {
                        id: incoming_tx,
                        desired_location: state.request.desired_location,
                    })),
                )
                .await?;
                return Ok(());
            }
        };

        let env = RelayEnv::new(op_manager);
        let now = Instant::now();
        state.handle_request(&env, &recency, &mut forward_attempts, &estimator, now)
    };

    // Telemetry: connect_request_received (mirrors connect.rs:1595)
    if let Some(event) = NetEventLog::connect_request_received(
        &incoming_tx,
        &op_manager.ring,
        state.request.desired_location,
        state.request.joiner.clone(),
        upstream_addr,
        initial_actions
            .forward
            .as_ref()
            .map(|(peer, _)| peer.clone()),
        initial_actions.accept.is_some(),
        state.request.ttl,
    ) {
        op_manager.ring.register_events(Either::Left(event)).await;
    }

    // ObservedAddress: emit toward upstream/joiner (legacy connect.rs:1608).
    if let Some((_target, address)) = initial_actions.observed_address {
        tracing::debug!(
            tx = %incoming_tx,
            observed_address = %address,
            sending_to = %upstream_addr,
            "Sending ObservedAddress to joiner"
        );
        let mut ctx = op_manager.op_ctx(incoming_tx);
        ctx.send_fire_and_forget(
            upstream_addr,
            NetMessage::V1(NetMessageV1::Connect(ConnectMsg::ObservedAddress {
                id: incoming_tx,
                address,
            })),
        )
        .await?;
    }

    // Local accept (promote joiner BEFORE sending Response upstream;
    // legacy connect.rs:1636).
    if let Some(ref accept) = initial_actions.accept {
        dispatch_expect_connection_from(op_manager, incoming_tx, accept.joiner.clone()).await?;
    }

    // Forward downstream (legacy connect.rs:1641). The forward decision
    // populates `state.forwarded_to` via `RelayState::forward_to_peer`.
    // The downstream waiter is registered via `send_to_and_collect_replies`
    // BEFORE the request hits the wire, so any inbound reply for this tx
    // lands in `receiver` once the bypass at node.rs forwards it.
    let downstream_receiver = match initial_actions.forward {
        Some((next, request)) => {
            recency.insert(next.clone(), Instant::now());
            match next.socket_addr() {
                Some(addr) => {
                    let mut ctx = op_manager.op_ctx(incoming_tx);
                    let capacity =
                        compute_reply_capacity(op_manager.ring.connection_manager.min_connections);
                    match ctx
                        .send_to_and_collect_replies(
                            addr,
                            NetMessage::V1(NetMessageV1::Connect(ConnectMsg::Request {
                                id: incoming_tx,
                                payload: request,
                            })),
                            capacity,
                        )
                        .await
                    {
                        Ok(rx) => Some(rx),
                        Err(e) => {
                            tracing::warn!(
                                tx = %incoming_tx,
                                target = %addr,
                                error = %e,
                                "CONNECT relay: failed to dispatch downstream Request"
                            );
                            None
                        }
                    }
                }
                None => {
                    tracing::warn!(
                        tx = %incoming_tx,
                        next_peer = %next.pub_key(),
                        "CONNECT relay: next hop has no socket address; skipping forward"
                    );
                    None
                }
            }
        }
        None => None,
    };

    // Local emit Response upstream after the forward (legacy connect.rs:1689).
    if let Some(accept) = initial_actions.accept {
        if let Some(event) = NetEventLog::connect_response_sent(
            &incoming_tx,
            &op_manager.ring,
            accept.response.acceptor.clone(),
            state.request.joiner.clone(),
        ) {
            op_manager.ring.register_events(Either::Left(event)).await;
        }
        let mut ctx = op_manager.op_ctx(incoming_tx);
        ctx.send_fire_and_forget(
            upstream_addr,
            NetMessage::V1(NetMessageV1::Connect(ConnectMsg::Response {
                id: incoming_tx,
                payload: accept.response,
            })),
        )
        .await?;
    }

    if let Some(reject_reason) = initial_actions.rejected {
        // Pass the SPECIFIC cause, never a constant: the terminus-rejection log
        // lines are `debug!` (#5335) and `release_max_level_info` compiles them
        // out of release builds, so this event's reason string is the only
        // thing distinguishing the three causes in production — "no uphill
        // peers available" (connectivity), "uphill budget exhausted" (the
        // amplification bound), and "TTL exhausted" (reach). See
        // `RejectReason::as_event_reason`, which owns those strings.
        if let Some(event) = NetEventLog::connect_rejected(
            &incoming_tx,
            &op_manager.ring,
            state.request.desired_location,
            reject_reason.as_event_reason(),
        ) {
            op_manager.ring.register_events(Either::Left(event)).await;
        }
        let mut ctx = op_manager.op_ctx(incoming_tx);
        ctx.send_fire_and_forget(
            upstream_addr,
            NetMessage::V1(NetMessageV1::Connect(ConnectMsg::Rejected {
                id: incoming_tx,
                desired_location: state.request.desired_location,
            })),
        )
        .await?;
        return Ok(());
    }

    // No downstream waiter installed (forward target had no socket
    // address, or no forward branch was selected and no accept either).
    // The driver has nothing more to do.
    let Some(mut receiver) = downstream_receiver else {
        return Ok(());
    };

    // Drain re-entry replies until terminal-or-TTL.
    loop {
        if should_exit_for_ttl(&incoming_tx) {
            tracing::debug!(
                tx = %incoming_tx,
                phase = "relay_connect_ttl_exit",
                "CONNECT relay: transaction timed out; exiting"
            );
            return Ok(());
        }

        let msg = match tokio::time::timeout(RELAY_RECV_POLL_INTERVAL, receiver.recv()).await {
            Ok(Some(WaiterReply::Reply(msg))) => msg,
            Ok(Some(WaiterReply::PeerDisconnected { peer })) => {
                // Downstream peer pruned mid-flight (#4313); no further
                // re-entry replies will arrive. End the relay driver.
                tracing::debug!(
                    tx = %incoming_tx,
                    %peer,
                    "CONNECT relay: downstream peer disconnected; exiting"
                );
                return Ok(());
            }
            Ok(None) => return Ok(()),
            Err(_) => continue,
        };

        let connect_msg = match msg {
            NetMessage::V1(NetMessageV1::Connect(c)) => c,
            other => {
                tracing::warn!(
                    tx = %incoming_tx,
                    "CONNECT relay: unexpected message kind: {:?}",
                    other
                );
                continue;
            }
        };

        match connect_msg {
            ConnectMsg::Response { payload, .. } => {
                // Forward Response toward upstream (joiner side); record
                // forward outcome for the downstream peer that produced it.
                if let Some(ref fwd) = state.forwarded_to.clone() {
                    op_manager.connect_forward_estimator.write().record(
                        fwd,
                        desired_location,
                        true,
                    );
                }
                tracing::debug!(
                    tx = %incoming_tx,
                    %upstream_addr,
                    acceptor_pub_key = %payload.acceptor.pub_key(),
                    forwarded_from = ?state.forwarded_to,
                    "CONNECT relay: forwarding Response upstream"
                );
                let mut ctx = op_manager.op_ctx(incoming_tx);
                ctx.send_fire_and_forget(
                    upstream_addr,
                    NetMessage::V1(NetMessageV1::Connect(ConnectMsg::Response {
                        id: incoming_tx,
                        payload,
                    })),
                )
                .await?;
                state.response_forwarded = true;
                // Stay in the loop so a subsequent `ConnectFailed` can
                // be re-routed downstream (legacy semantic). The driver
                // exits naturally on TTL or channel closure.
            }
            ConnectMsg::Rejected {
                desired_location: dl,
                ..
            } => {
                // Mirror connect.rs:2002-2114. Record forward failure;
                // re-run handle_request for retry (different uphill peer
                // OR accept locally OR forward Rejected upstream).
                let failed_peer = state.forwarded_to.take();
                state.forwarded_at = None;
                if let Some(ref fwd) = failed_peer {
                    op_manager
                        .connect_forward_estimator
                        .write()
                        .record(fwd, dl, false);
                    forward_attempts.remove(fwd);
                }

                let now = Instant::now();
                let env = RelayEnv::new(op_manager);
                let estimator = op_manager.connect_forward_estimator.read().clone();
                let retry =
                    state.handle_request(&env, &recency, &mut forward_attempts, &estimator, now);

                if let Some((peer, forward_req)) = retry.forward {
                    recency.insert(peer.clone(), now);
                    tracing::debug!(
                        tx = %incoming_tx,
                        failed_peer = ?failed_peer,
                        retry_peer = %peer.pub_key(),
                        "CONNECT relay: retrying with different uphill peer after rejection"
                    );
                    if let Some(addr) = peer.socket_addr() {
                        let mut ctx = op_manager.op_ctx(incoming_tx);
                        ctx.send_fire_and_forget(
                            addr,
                            NetMessage::V1(NetMessageV1::Connect(ConnectMsg::Request {
                                id: incoming_tx,
                                payload: forward_req,
                            })),
                        )
                        .await?;
                    }
                } else if let Some(accept) = retry.accept {
                    tracing::info!(
                        tx = %incoming_tx,
                        failed_peer = ?failed_peer,
                        acceptor = %accept.response.acceptor.pub_key(),
                        "CONNECT relay: accepting locally after uphill rejection"
                    );
                    if let Some(event) = NetEventLog::connect_response_sent(
                        &incoming_tx,
                        &op_manager.ring,
                        accept.response.acceptor.clone(),
                        accept.joiner.clone(),
                    ) {
                        op_manager.ring.register_events(Either::Left(event)).await;
                    }
                    dispatch_expect_connection_from(op_manager, incoming_tx, accept.joiner).await?;
                    let mut ctx = op_manager.op_ctx(incoming_tx);
                    ctx.send_fire_and_forget(
                        upstream_addr,
                        NetMessage::V1(NetMessageV1::Connect(ConnectMsg::Response {
                            id: incoming_tx,
                            payload: accept.response,
                        })),
                    )
                    .await?;
                } else {
                    tracing::debug!(
                        tx = %incoming_tx,
                        %upstream_addr,
                        failed_peer = ?failed_peer,
                        "CONNECT relay: forwarding rejection upstream (no retry peers)"
                    );
                    // This `else` is reached by re-running the SAME
                    // `handle_request` whose terminus arms carry a specific
                    // cause, so a constant here would reintroduce exactly the
                    // conflation #5335 removed from the initial path — and on
                    // live traffic this retry path is roughly half of this
                    // driver's rejections, so it is not a corner case.
                    //
                    // The generic fallback is for the case where
                    // `handle_request` returned no forward, no accept AND no
                    // rejection. That is genuinely reachable, but NOT via the
                    // already-forwarded arm: `forwarded_to.take()` above sets
                    // it to `None` before the call, and that arm is gated on
                    // `forwarded_to.is_some()`. The reachable path is
                    // `accepted_locally == true` — near-terminus acceptance
                    // accepts while ALSO forwarding, so when that forward is
                    // later rejected we re-enter here with every terminus arm
                    // gated off by `!accepted_locally` and nothing set. A
                    // different situation from a terminus rejection, so it
                    // keeps its own wording rather than borrowing a cause.
                    let reason = retry
                        .rejected
                        .map(RejectReason::as_event_reason)
                        .unwrap_or("relay no retry peers available");
                    if let Some(event) =
                        NetEventLog::connect_rejected(&incoming_tx, &op_manager.ring, dl, reason)
                    {
                        op_manager.ring.register_events(Either::Left(event)).await;
                    }
                    let mut ctx = op_manager.op_ctx(incoming_tx);
                    ctx.send_fire_and_forget(
                        upstream_addr,
                        NetMessage::V1(NetMessageV1::Connect(ConnectMsg::Rejected {
                            id: incoming_tx,
                            desired_location: dl,
                        })),
                    )
                    .await?;
                    return Ok(());
                }
            }
            ConnectMsg::ObservedAddress { address, .. } => {
                // Joiner-bound, forward upstream unchanged. The relay
                // never updates its own_addr from a downstream
                // ObservedAddress — that's the joiner's role.
                let mut ctx = op_manager.op_ctx(incoming_tx);
                ctx.send_fire_and_forget(
                    upstream_addr,
                    NetMessage::V1(NetMessageV1::Connect(ConnectMsg::ObservedAddress {
                        id: incoming_tx,
                        address,
                    })),
                )
                .await?;
            }
            ConnectMsg::ConnectFailed {
                failed_acceptor_addr,
                ..
            } => {
                // Mirror connect.rs:2125-2273. Two routing modes:
                //   (a) response_forwarded == true: forward downstream
                //       to forwarded_to so the closer relay can re-route
                //       with its local knowledge; clear response_forwarded.
                //   (b) Otherwise: try local re-route via handle_request
                //       (mark failed acceptor visited, retry forward,
                //       accept locally, or propagate ConnectFailed upstream).
                let now = Instant::now();
                if state.response_forwarded {
                    if let Some(ref fwd) = state.forwarded_to {
                        if let Some(fwd_addr) = fwd.socket_addr() {
                            op_manager.ring.connection_manager.record_acceptor_outcome(
                                failed_acceptor_addr,
                                false,
                                now,
                            );
                            tracing::debug!(
                                tx = %incoming_tx,
                                forwarded_to_addr = %fwd_addr,
                                failed_acceptor = %failed_acceptor_addr,
                                "CONNECT relay: forwarding ConnectFailed downstream"
                            );
                            let mut ctx = op_manager.op_ctx(incoming_tx);
                            ctx.send_fire_and_forget(
                                fwd_addr,
                                NetMessage::V1(NetMessageV1::Connect(ConnectMsg::ConnectFailed {
                                    id: incoming_tx,
                                    failed_acceptor_addr,
                                })),
                            )
                            .await?;
                            state.response_forwarded = false;
                            continue;
                        }
                    }
                }

                let failed_peer = state.forwarded_to.clone();
                op_manager.ring.connection_manager.record_acceptor_outcome(
                    failed_acceptor_addr,
                    false,
                    now,
                );
                if let Some(ref fwd) = failed_peer {
                    op_manager.connect_forward_estimator.write().record(
                        fwd,
                        desired_location,
                        false,
                    );
                    forward_attempts.remove(fwd);
                }

                state.request.visited.mark_visited(failed_acceptor_addr);
                state.forwarded_to = None;
                state.forwarded_at = None;
                state.response_forwarded = false;

                let env = RelayEnv::new(op_manager);
                let estimator = op_manager.connect_forward_estimator.read().clone();
                let retry =
                    state.handle_request(&env, &recency, &mut forward_attempts, &estimator, now);

                if let Some((peer, forward_req)) = retry.forward {
                    recency.insert(peer.clone(), now);
                    tracing::debug!(
                        tx = %incoming_tx,
                        failed_acceptor = %failed_acceptor_addr,
                        retry_peer = %peer.pub_key(),
                        "CONNECT relay: re-routing to different peer after ConnectFailed"
                    );
                    if let Some(addr) = peer.socket_addr() {
                        let mut ctx = op_manager.op_ctx(incoming_tx);
                        ctx.send_fire_and_forget(
                            addr,
                            NetMessage::V1(NetMessageV1::Connect(ConnectMsg::Request {
                                id: incoming_tx,
                                payload: forward_req,
                            })),
                        )
                        .await?;
                    }
                } else if let Some(accept) = retry.accept {
                    tracing::info!(
                        tx = %incoming_tx,
                        failed_acceptor = %failed_acceptor_addr,
                        acceptor = %accept.response.acceptor.pub_key(),
                        "CONNECT relay: accepting locally after ConnectFailed"
                    );
                    if let Some(event) = NetEventLog::connect_response_sent(
                        &incoming_tx,
                        &op_manager.ring,
                        accept.response.acceptor.clone(),
                        accept.joiner.clone(),
                    ) {
                        op_manager.ring.register_events(Either::Left(event)).await;
                    }
                    dispatch_expect_connection_from(op_manager, incoming_tx, accept.joiner).await?;
                    let mut ctx = op_manager.op_ctx(incoming_tx);
                    ctx.send_fire_and_forget(
                        upstream_addr,
                        NetMessage::V1(NetMessageV1::Connect(ConnectMsg::Response {
                            id: incoming_tx,
                            payload: accept.response,
                        })),
                    )
                    .await?;
                } else {
                    // KNOWN GAP (#5335): `retry.rejected` may carry a specific
                    // cause here, and this branch deliberately emits NO
                    // `connect_rejected` event. In a release build this
                    // `debug!` is compiled out by `release_max_level_info`, so
                    // on this path the cause is recorded NOWHERE — not in a
                    // log, not in an event, on this node or any other. State
                    // that plainly rather than implying it is relocated: a
                    // `ConnectFailed` is handled in exactly one place (the arm
                    // this branch belongs to) and its outcomes are forward
                    // downstream, re-route, accept locally, or propagate
                    // further upstream. At NO hop does it turn into a
                    // `Rejected` or emit `connect_rejected`, so there is
                    // nothing upstream that re-records this.
                    //
                    // It is nonetheless deliberate, for a reason that survives
                    // the above: the terminal outcome here is ConnectFailed
                    // PROPAGATION, which is a different decision from a
                    // rejection. Folding it into `connect_rejected` would make
                    // one event name cover two decisions and inflate
                    // `connect_rejects_emitted` across this release boundary,
                    // breaking comparability of the counter. No event variant
                    // exists for "propagated ConnectFailed" and adding one is a
                    // telemetry schema change beyond the scope of #5335.
                    //
                    // So: an accepted, documented loss on this path, not a
                    // covered case. Revisit if it is ever sized in production.
                    tracing::debug!(
                        tx = %incoming_tx,
                        %upstream_addr,
                        failed_acceptor = %failed_acceptor_addr,
                        reject_reason = ?retry.rejected,
                        "CONNECT relay: propagating ConnectFailed upstream (no re-route)"
                    );
                    let mut ctx = op_manager.op_ctx(incoming_tx);
                    ctx.send_fire_and_forget(
                        upstream_addr,
                        NetMessage::V1(NetMessageV1::Connect(ConnectMsg::ConnectFailed {
                            id: incoming_tx,
                            failed_acceptor_addr,
                        })),
                    )
                    .await?;
                }
            }
            ConnectMsg::Request { .. } => {
                // Driver bypass at node.rs filters Request — the
                // dispatch gate handles it as the spawn signal. If a
                // Request reaches the driver inbox the bypass logic
                // has regressed. Drop with a warning so future
                // refactors notice.
                tracing::warn!(
                    tx = %incoming_tx,
                    "CONNECT relay: unexpected Request on driver inbox; bypass logic regressed"
                );
            }
        }
    }
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

    /// Source-literal regression guard: `start_relay_connect` MUST
    /// insert into `active_relay_connect_txs` before any work happens,
    /// so `RelayConnectInflightGuard` can clean it up on driver exit.
    /// Mirrors the pattern enforced for PUT/SUBSCRIBE/UPDATE relay
    /// drivers (`start_relay_subscribe_checks_dedup_gate` family).
    #[test]
    fn start_relay_connect_inserts_into_dedup_set() {
        const SOURCE: &str = include_str!("op_ctx_task.rs");
        let fn_start = SOURCE
            .find("pub(crate) async fn start_relay_connect(")
            .expect("start_relay_connect not found — driver removed or renamed");
        let fn_end = SOURCE[fn_start..]
            .find("\n}\n")
            .expect("start_relay_connect has no closing brace");
        let body = &SOURCE[fn_start..fn_start + fn_end];
        assert!(
            body.contains("active_relay_connect_txs.insert(incoming_tx)"),
            "start_relay_connect MUST insert(incoming_tx) into the dedup \
             set before spawning. Without this, racing duplicate Requests \
             spawn N drivers and leak `RelayConnectInflightGuard`s."
        );
        assert!(
            body.contains("RELAY_CONNECT_DEDUP_REJECTS"),
            "start_relay_connect MUST increment RELAY_CONNECT_DEDUP_REJECTS \
             when the dedup gate fires; without it, dedup storms are \
             invisible to telemetry."
        );
    }

    /// Source-literal guard: the relay driver runs the full re-entry
    /// state machine (Response/Rejected/ObservedAddress/ConnectFailed)
    /// on its inbox receiver. Removing any of these branches would
    /// strand the corresponding re-entry.
    #[test]
    fn drive_relay_connect_handles_all_reentry_variants() {
        const SOURCE: &str = include_str!("op_ctx_task.rs");
        let fn_start = SOURCE
            .find("async fn drive_relay_connect(")
            .expect("drive_relay_connect not found");
        // Window ends at `should_exit_for_ttl` definition.
        let fn_end = SOURCE[fn_start..]
            .find("/// Decide whether the driver loop should exit")
            .expect("drive_relay_connect has no successor function — guard outdated")
            + fn_start;
        let body = &SOURCE[fn_start..fn_end];

        for variant in &[
            "ConnectMsg::Response { payload, .. }",
            "ConnectMsg::Rejected {",
            "ConnectMsg::ObservedAddress { address, .. }",
            "ConnectMsg::ConnectFailed {",
        ] {
            assert!(
                body.contains(variant),
                "drive_relay_connect MUST match `{variant}`. \
                 Removing this arm strands the variant on legacy \
                 process_message after commit 4 retires it."
            );
        }

        assert!(
            body.contains("should_exit_for_ttl(&incoming_tx)"),
            "drive_relay_connect MUST poll should_exit_for_ttl in the \
             receive loop; without it a wedged tx leaks the \
             pending_op_results slot until the 60s sweep."
        );
    }

    /// Source-literal guard: the relay driver's admission RAII guard
    /// MUST be dropped before entering the receive loop. Holding the
    /// guard for the full driver lifetime would starve the gateway
    /// admission slot under load (driver lifetimes >> handle_request
    /// decision latencies). See plan risk #4.
    #[test]
    fn relay_driver_admission_guard_scoped_to_initial_actions() {
        const SOURCE: &str = include_str!("op_ctx_task.rs");
        let fn_start = SOURCE
            .find("async fn drive_relay_connect(")
            .expect("drive_relay_connect not found");
        let body = &SOURCE[fn_start..];

        let guard_decl_pos = body
            .find("let _admission = match op_manager.ring.connection_manager.try_admit_connect()")
            .expect(
                "driver no longer binds admission guard as `let _admission = match ... try_admit_connect()`. \
                 The RAII guard MUST be bound to `_admission` (drops at end of initial-actions scope) \
                 — not held across the recv loop. See plan risk #4 (admission slot starvation)."
            );
        let receive_loop_pos = body
            .find("loop {\n        if should_exit_for_ttl")
            .expect("driver no longer has the recv loop with TTL exit");

        assert!(
            guard_decl_pos < receive_loop_pos,
            "admission gate (`let _admission = match ... try_admit_connect()`) \
             must appear BEFORE the recv loop with TTL exit, so the guard \
             drops at the end of its block scope and does not span the \
             receive loop."
        );
    }

    /// Regression guard for the retry-accept ring-promotion invariant
    /// (#3838 / PR #3893; tracked by #3894).
    ///
    /// The CONNECT relay accepts a joiner from THREE production sites:
    ///
    /// 1. the happy-path terminus/near-terminus accept
    ///    (`initial_actions.accept`),
    /// 2. the `Rejected` retry-accept ("accepting locally after uphill
    ///    rejection"), and
    /// 3. the `ConnectFailed` retry-accept ("accepting locally after
    ///    ConnectFailed").
    ///
    /// EVERY accept site MUST call `dispatch_expect_connection_from` to
    /// promote the joiner's transient transport connection into a ring
    /// connection (it emits `NodeEvent::ExpectPeerConnection` +
    /// `NodeEvent::ConnectPeer { is_gw: false }`). The original #3838 bug
    /// was exactly a retry branch that sent the `ConnectResponse` upstream
    /// but DROPPED the promotion call, stranding the joiner in
    /// `transient_connections` on the acceptor — invisible to
    /// `get_peer_by_addr` (subscribe interest registration, broadcast
    /// fan-out), so UPDATEs never reached it.
    ///
    /// PR #3893 made the data-level drop unrepresentable by bundling the
    /// response + joiner into `AcceptOutcome` (covered by the unit test
    /// `connect.rs::relay_retry_acceptance_bundles_joiner_with_response`).
    /// This source-scrape pin closes the remaining gap at the *driver*
    /// level: it fails the build if a future edit re-inlines an accept
    /// branch that forwards the response without dispatching the promotion.
    ///
    /// A behavioural simulation test was investigated for #3894 but is not
    /// viable: the retry-accept branches are not deterministically reached
    /// by emergent SimNetwork topology (a sparse-ring run exercised only the
    /// terminus / near-terminus accept paths, never the `Rejected` /
    /// `ConnectFailed` retries), and `FaultConfig` cannot inject a CONNECT
    /// rejection to force them. Forcing the branch via capacity races would
    /// be seed/timing-dependent (a flaky test). This pin is the deterministic
    /// guard for the same invariant.
    #[test]
    fn every_relay_accept_site_promotes_joiner_to_ring() {
        const SOURCE: &str = include_str!("op_ctx_task.rs");

        // Restrict to production code: the `#[cfg(test)]` module below must
        // not contribute (or mask) call sites.
        let prod_end = SOURCE
            .find("\n#[cfg(test)]\n")
            .expect("expected a `#[cfg(test)]` module in op_ctx_task.rs");
        let prod = &SOURCE[..prod_end];

        // 1. Exactly three accept sites, each pairing its `accept` with the
        //    promotion dispatch. The count is load-bearing: an added accept
        //    branch without a matching dispatch would NOT raise this above 3,
        //    so the per-branch checks below are also required.
        let dispatch_sites = prod
            .matches("dispatch_expect_connection_from(op_manager, incoming_tx,")
            .count();
        assert_eq!(
            dispatch_sites, 3,
            "expected exactly 3 `dispatch_expect_connection_from` call sites in \
             the CONNECT relay driver (happy-path accept + `Rejected` retry-accept \
             + `ConnectFailed` retry-accept), found {dispatch_sites}. If you added \
             or removed an accept site, update this pin AND ensure the new site \
             promotes the joiner — see #3838 / PR #3893."
        );

        // 2. Each retry-accept branch, anchored by its unique log message,
        //    MUST dispatch the promotion BEFORE it forwards the
        //    `ConnectMsg::Response` upstream. The Response send is the
        //    branch's terminal action, so requiring the dispatch to precede
        //    it pins both presence ("the branch promotes the joiner") and the
        //    documented ordering ("promote joiner BEFORE sending Response
        //    upstream"). Without the dispatch the joiner is accepted but never
        //    promoted — the #3838 signature.
        for (anchor, branch) in [
            (
                "CONNECT relay: accepting locally after uphill rejection",
                "`Rejected` retry-accept",
            ),
            (
                "CONNECT relay: accepting locally after ConnectFailed",
                "`ConnectFailed` retry-accept",
            ),
        ] {
            let anchor_pos = prod.find(anchor).unwrap_or_else(|| {
                panic!(
                    "retry-accept anchor log line not found: {anchor:?}. The {branch} \
                     branch was renamed or removed — update this pin and verify the \
                     branch still promotes the joiner (#3838 / PR #3893)."
                )
            });
            let window = &prod[anchor_pos..];
            // The branch's terminal action is forwarding the ConnectResponse
            // upstream. Bound the window to that send so we only inspect this
            // branch's body, independent of how the surrounding arms are
            // structured (`} else {`, early return, etc.).
            let response_send_offset = window
                .find("NetMessage::V1(NetMessageV1::Connect(ConnectMsg::Response {")
                .unwrap_or_else(|| {
                    panic!(
                        "{branch} branch (anchored at {anchor:?}) no longer forwards a \
                         ConnectMsg::Response upstream — update this pin and verify the \
                         branch still promotes the joiner before replying (#3838)."
                    )
                });
            let dispatch_offset =
                window.find("dispatch_expect_connection_from(op_manager, incoming_tx,");
            assert!(
                dispatch_offset.is_some_and(|d| d < response_send_offset),
                "{branch} branch (anchored at {anchor:?}) forwards the ConnectResponse \
                 upstream WITHOUT first calling `dispatch_expect_connection_from`. \
                 This re-introduces #3838: the joiner is accepted but never promoted \
                 from `transient_connections` to a ring connection, so subscribe \
                 interest registration and broadcast fan-out can't find it."
            );
        }

        // 3. The happy-path accept branch must dispatch too, so the pin is
        //    not satisfied solely by the retry branches.
        let happy_pos = prod
            .find("if let Some(ref accept) = initial_actions.accept {")
            .expect(
                "happy-path accept branch `if let Some(ref accept) = initial_actions.accept` \
                 not found — renamed or removed; update this pin and verify it still \
                 promotes the joiner.",
            );
        let happy_window = &prod[happy_pos..];
        let happy_dispatch =
            happy_window.find("dispatch_expect_connection_from(op_manager, incoming_tx,");
        let happy_block_end = happy_window.find("\n    }").unwrap_or(happy_window.len());
        assert!(
            happy_dispatch.is_some_and(|d| d < happy_block_end),
            "happy-path accept branch (`initial_actions.accept`) must call \
             `dispatch_expect_connection_from` to promote the joiner to a ring \
             connection before forwarding the response upstream (#3838 / PR #3893)."
        );
    }

    /// Pins the wiring of the lattice-probe miss report (#5814), which the
    /// model test in `ring/lattice_degree_model.rs` exercises only through the
    /// shared predicate and scheduler: (1) the maintenance loop tags its
    /// route-to-self probe as `ClientConnectKind::LatticeProbe` and nothing
    /// else, and `acquire_new` forwards the kind; (2) the client driver, after a
    /// successful hole punch, classifies each probe acceptor with
    /// `record_lattice_probe_result`, and does not drop it. Without (2) the
    /// scheduler never learns the lattice is tight, probes forever, and degree
    /// creeps.
    #[test]
    fn lattice_probe_miss_is_wired() {
        const DRIVER: &str = include_str!("op_ctx_task.rs");
        const RING: &str = include_str!("../../ring.rs");
        // Production code only (before the first test module), with `//`
        // comments removed so a commented-out call does not count.
        let prod = |src: &str| -> String {
            src[..src
                .find("\n#[cfg(test)]\nmod ")
                .expect("expected a `#[cfg(test)]` test module")]
                .lines()
                .map(|line| line.split_once("//").map_or(line, |(code, _)| code))
                .collect::<Vec<_>>()
                .join("\n")
        };
        // Whitespace removed, so formatting changes do not matter.
        let squash = |s: &str| -> String { s.split_whitespace().collect() };
        // The item starting at `start`, up to its closing line `end`, squashed.
        let item = |src: &str, start: &str, end: &str| -> String {
            let rest = &src[src
                .find(start)
                .unwrap_or_else(|| panic!("{start} not found"))..];
            squash(&rest[..rest.find(end).unwrap_or_else(|| panic!("end of {start}"))])
        };

        // (1) ring.rs, inside `connection_maintenance`: the scheduler reads
        // this peer's connection manager before the acquisition drain, and
        // exactly one acquisition is tagged as a lattice probe (chosen by the
        // probe-target comparison, with the scheduler's generation);
        // `acquire_new` forwards the kind; and the router snapshot task
        // exports the miss count.
        let ring = prod(RING);
        let maintenance = item(&ring, "async fn connection_maintenance(", "\n    }\n");
        let tick = maintenance
            .find("lattice_probe.tick_for(&self.connection_manager,")
            .expect("connection_maintenance must drive LatticeProbeScheduler from its own ConnectionManager");
        let drain = maintenance
            .find("whileletSome(ideal_location)=pending_conn_adds.pop_first()")
            .expect("acquisition drain loop not found in connection_maintenance");
        assert!(
            tick < drain,
            "the probe must be scheduled before the drain, so it launches in the same tick"
        );
        assert!(
            maintenance.contains(
                "ifletSome(interval)=outcome.fired{self.connection_manager.record_lattice_probe_issued();pending_conn_adds.insert(me);"
            ),
            "the probe target must be queued only when the scheduler fires"
        );
        assert!(
            maintenance.contains("#[cfg(not(test))]letprobe_timing=lattice_probe_timing::production();")
                && maintenance.contains(
                    "LatticeProbeScheduler::new(self.time_source.now(),self.connection_manager.lattice_probe_misses(),probe_timing,)"
                ),
            "connection_maintenance must run the scheduler on the production timing"
        );
        assert!(
            maintenance.contains(
                "GlobalRng::random_range(lattice_probe_timing::JITTER_LOW..=lattice_probe_timing::JITTER_HIGH,)"
            ),
            "the probe intervals must be jittered with the production bounds"
        );
        assert_eq!(
            squash(&ring)
                .matches("ClientConnectKind::LatticeProbe{")
                .count(),
            1,
            "ring.rs must tag exactly one acquisition as a lattice probe"
        );
        assert!(
            maintenance.contains(
                "iflattice_probe_target==Some(ideal_location){ClientConnectKind::LatticeProbe{generation:lattice_probe.generation(),}"
            ),
            "the drain in connection_maintenance must tag the probe target with the current generation"
        );
        let acquire = item(&ring, "async fn acquire_new(", "\n    }\n");
        assert!(
            acquire.contains("kind:ClientConnectKind,")
                && acquire.contains("ideal_location,None,kind,)"),
            "acquire_new must forward its kind to start_client_connect"
        );
        let snapshot = item(
            &ring,
            "async fn emit_router_snapshot_telemetry(",
            "\n    }\n",
        );
        assert!(
            snapshot.contains("snapshot.lattice_probe_misses=Some(cm.lattice_probe_miss_total());"),
            "the router snapshot task must export the probe miss count"
        );

        // (2) The client driver threads the kind through and classifies every
        // lattice-probe acceptor for the probe's generation once the hole-punch
        // result is known, before the failure branch can `continue`, and
        // never drops it.
        let driver = prod(DRIVER);
        let start = item(
            &driver,
            "pub(crate) async fn start_client_connect(",
            "\n}\n",
        );
        assert!(
            start.contains("kind:ClientConnectKind,") && start.contains("overall_timeout,kind,)"),
            "start_client_connect must forward its kind to start_client_connect_counted"
        );
        let counted = item(
            &driver,
            "pub(crate) async fn start_client_connect_counted(",
            "\n}\n",
        );
        assert!(
            counted.contains("kind:ClientConnectKind,")
                && counted.contains("op_manager,kind,&mutconnected,);"),
            "start_client_connect_counted must forward its kind to the driver"
        );
        let body = item(&driver, "async fn drive_client_connect(", "\n}\n");
        let joined = body
            .find("\"connectdriver:joinedpeer\"")
            .expect("hole-punch result handling not found");
        let gate = body
            .find("ifletClientConnectKind::LatticeProbe{generation}=kind{")
            .expect("lattice-probe miss report not found in drive_client_connect");
        let failed = body
            .find("if!hole_punch_ok{")
            .expect("hole-punch failure branch not found");
        assert!(
            joined < gate && gate < failed,
            "the miss report must run once the hole-punch result is known and before the \
             failure branch can `continue`, so a cap-refused acceptor still counts"
        );
        let gate_block = &body[gate..failed];
        assert!(
            gate_block.contains(
                "ifcm.record_lattice_probe_result(Location::from_address(&connected_addr),connected_addr,generation,hole_punch_ok,cm.at_max_connections(),)"
            ),
            "every probe acceptor must be classified, for the probe's generation"
        );
        assert!(
            !body.contains("NodeEvent::DropConnection(acceptor_addr")
                && !gate_block.contains("continue;"),
            "a probe acceptor must be kept (a dropped link stays dead on the far end)"
        );
    }
}

/// Behaviour of the client driver while this peer has no ring connection
/// (#5858), against a real `OpManager` whose event-loop notifications this
/// test answers itself. Paused time: hole-punch delays and long silences cost
/// nothing.
#[cfg(test)]
mod driver_timing_tests {
    use std::net::{IpAddr, Ipv4Addr, SocketAddr};
    use std::sync::Arc;
    use std::time::Duration;

    use either::Either;
    use tokio::sync::mpsc;
    use tokio::time::Instant;

    use super::{ClientConnectEnd, ClientConnectKind};
    use crate::message::{NetMessage, NetMessageV1, NodeEvent, Transaction};
    use crate::node::{OpManager, WaiterReply};
    use crate::operations::connect::bootstrap::GATEWAY_ANSWER_DEADLINE;
    use crate::operations::connect::{ConnectMsg, ConnectResponse};
    use crate::ring::{Location, PeerKeyLocation};
    use crate::transport::TransportKeypair;
    use crate::transport::connection_handler::NAT_TRAVERSAL_DEADLINE;

    /// [`super::drive_client_connect`] without the connected count.
    #[allow(clippy::too_many_arguments)]
    async fn drive_client_connect_inner(
        tx: Transaction,
        gateway: PeerKeyLocation,
        gateway_addr: SocketAddr,
        desired_location: Location,
        target_connections: usize,
        started_without_address: bool,
        receiver: mpsc::Receiver<WaiterReply>,
        op_manager: &OpManager,
        kind: ClientConnectKind,
    ) -> Result<ClientConnectEnd, crate::operations::OpError> {
        let mut connected = 0;
        super::drive_client_connect(
            tx,
            gateway,
            gateway_addr,
            desired_location,
            target_connections,
            started_without_address,
            receiver,
            op_manager,
            kind,
            &mut connected,
        )
        .await
    }

    async fn op_manager_with_events(
        id: &str,
    ) -> (
        Arc<OpManager>,
        mpsc::Receiver<Either<NetMessage, NodeEvent>>,
    ) {
        let (op_manager, events, op_execution) = op_manager_with_channels(id).await;
        std::mem::forget(op_execution);
        (op_manager, events)
    }

    async fn op_manager_with_channels(
        id: &str,
    ) -> (
        Arc<OpManager>,
        mpsc::Receiver<Either<NetMessage, NodeEvent>>,
        mpsc::Receiver<(mpsc::Sender<WaiterReply>, NetMessage, Option<SocketAddr>)>,
    ) {
        op_manager_at(id, true).await
    }

    /// A fresh joiner: it does not know its own address yet, so its gateway
    /// joins expect an `ObservedAddress`.
    async fn unaddressed_op_manager(
        id: &str,
    ) -> (
        Arc<OpManager>,
        mpsc::Receiver<Either<NetMessage, NodeEvent>>,
        mpsc::Receiver<(mpsc::Sender<WaiterReply>, NetMessage, Option<SocketAddr>)>,
    ) {
        op_manager_at(id, false).await
    }

    const GATEWAY_PORTS: [u16; 7] = [24862, 24864, 24867, 24868, 24869, 24882, 24883];

    async fn op_manager_at(
        id: &str,
        addressed: bool,
    ) -> (
        Arc<OpManager>,
        mpsc::Receiver<Either<NetMessage, NodeEvent>>,
        mpsc::Receiver<(mpsc::Sender<WaiterReply>, NetMessage, Option<SocketAddr>)>,
    ) {
        let config_args = crate::config::ConfigArgs {
            id: Some(id.to_string()),
            mode: Some(crate::contract::OperationMode::Local),
            ..Default::default()
        };
        let mut node_config =
            crate::node::NodeConfig::new(config_args.build().await.expect("build Config"))
                .await
                .expect("build NodeConfig");
        // The ports the liveness tests use as gateways; anything else is a
        // non-gateway first hop.
        for port in GATEWAY_PORTS {
            let gateway = peer(port);
            let location = Location::from_address(&gateway.socket_addr().unwrap());
            node_config
                .gateways
                .push(crate::node::InitPeerNode::new(gateway, location));
        }
        let (notification_rx, notification_tx) = crate::node::event_loop_notification_channel();
        let (ops_ch_channel, ch_channel, _wait_for_event) =
            crate::contract::contract_handler_channel();
        let connection_manager = crate::ring::ConnectionManager::new(&node_config);
        let (result_router_tx, result_router_rx) = mpsc::channel(100);
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
        // Not attached to its ring: the ring's maintenance loop then parks
        // instead of running its own zero-connection bootstrap through the
        // gateways configured above, which would send CONNECTs these tests
        // do not expect and count join attempts in the process-global
        // network_status that other tests assert on.
        if addressed {
            op_manager
                .ring
                .connection_manager
                .set_own_addr_local_for_test("127.0.0.1:14858".parse().unwrap());
        } else {
            op_manager.ring.connection_manager.clear_own_addr_for_test();
        }
        std::mem::forget((ch_channel, result_router_rx));
        (
            op_manager,
            notification_rx.notifications_receiver,
            notification_rx.op_execution_receiver,
        )
    }

    fn peer(port: u16) -> PeerKeyLocation {
        let addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), port);
        PeerKeyLocation::new(TransportKeypair::new().public().clone(), addr)
    }

    fn response(tx: Transaction, acceptor: &PeerKeyLocation) -> WaiterReply {
        WaiterReply::Reply(NetMessage::V1(NetMessageV1::Connect(
            ConnectMsg::Response {
                id: tx,
                payload: ConnectResponse {
                    acceptor: acceptor.clone(),
                },
            },
        )))
    }

    /// Answers `ConnectPeer` like the event loop: an `unreachable` acceptor
    /// fails after the NAT traversal deadline; any other acceptor connects if
    /// it is dialled within its own punch window, which opens when its
    /// Response was sent (`t0`). Returns when each acceptor was dialled.
    fn fake_event_loop(
        mut events: mpsc::Receiver<Either<NetMessage, NodeEvent>>,
        unreachable: SocketAddr,
        t0: Instant,
    ) -> tokio::task::JoinHandle<Vec<(SocketAddr, Duration)>> {
        tokio::spawn(async move {
            let mut dialled = Vec::new();
            while let Some(event) = events.recv().await {
                let Either::Right(NodeEvent::ConnectPeer { peer, callback, .. }) = event else {
                    continue;
                };
                let addr = peer.socket_addr().expect("acceptor address");
                let at = t0.elapsed();
                dialled.push((addr, at));
                let in_window = at < NAT_TRAVERSAL_DEADLINE;
                tokio::spawn(async move {
                    tokio::time::sleep(NAT_TRAVERSAL_DEADLINE).await;
                    let result = if addr != unreachable && in_window {
                        Ok((addr, None))
                    } else {
                        Err(())
                    };
                    // The driver may already have ended; nothing to do then.
                    callback.send(result).await.ok();
                });
                if dialled.len() == 2 {
                    return dialled;
                }
            }
            dialled
        })
    }

    #[tokio::test(start_paused = true)]
    async fn a_reachable_acceptor_is_dialled_while_an_unreachable_one_is_still_failing() {
        let (op_manager, events) = op_manager_with_events("driver-concurrent-dials").await;
        let gateway = peer(24858);
        let (unreachable, reachable) = (peer(24859), peer(24860));
        let tx = Transaction::new::<ConnectMsg>();
        let (replies_tx, replies_rx) = mpsc::channel(8);
        let t0 = Instant::now();
        let fake = fake_event_loop(events, unreachable.socket_addr().unwrap(), t0);
        replies_tx.send(response(tx, &unreachable)).await.unwrap();
        replies_tx.send(response(tx, &reachable)).await.unwrap();

        let driver = drive_client_connect_inner(
            tx,
            gateway.clone(),
            gateway.socket_addr().unwrap(),
            Location::new(0.5),
            op_manager.ring.connection_manager.min_connections,
            false,
            replies_rx,
            &op_manager,
            ClientConnectKind::Standard,
        );
        let dialled = tokio::select! {
            dialled = fake => dialled.expect("fake event loop"),
            _ = driver => panic!("driver ended before both acceptors were dialled"),
        };
        let reachable_at = dialled
            .iter()
            .find(|(a, _)| Some(*a) == reachable.socket_addr())
            .map(|(_, at)| *at)
            .expect("the reachable acceptor must be dialled");
        assert!(
            reachable_at < NAT_TRAVERSAL_DEADLINE,
            "the reachable acceptor was dialled {reachable_at:?} after its Response, after its \
             punch window closed: the driver waited for the unreachable one first"
        );
        drop(replies_tx);
    }

    /// While alive, the driver's handling of `ObservedAddress` (which calls
    /// `set_own_addr`) leaves the process-global `network_status` address
    /// alone, as for a simulated node, so these tests cannot race
    /// `test_set_own_addr_atomic_with_network_status`. The test runtime is
    /// single-threaded, so the thread-local marker covers the driver.
    struct NoGlobalAddress;

    impl NoGlobalAddress {
        fn new() -> Self {
            crate::ring::topology_registry::set_current_network_name("driver-timing-tests");
            Self
        }
    }

    impl Drop for NoGlobalAddress {
        fn drop(&mut self) {
            crate::ring::topology_registry::clear_current_network_name();
        }
    }

    fn observed_address(tx: Transaction) -> WaiterReply {
        WaiterReply::Reply(NetMessage::V1(NetMessageV1::Connect(
            ConnectMsg::ObservedAddress {
                id: tx,
                address: "127.0.0.1:14858".parse().unwrap(),
            },
        )))
    }

    /// A bootstrapping CONNECT is not cut short by silence (#5858): live
    /// replies are often slow, so it keeps collecting, and a re-routed
    /// acceptor that answers 30 s after a failed dial is still dialled.
    #[tokio::test(start_paused = true)]
    async fn a_bootstrapping_connect_keeps_collecting_through_a_long_silence() {
        let _marker = NoGlobalAddress::new();
        let (op_manager, events) = op_manager_with_events("driver-bootstrap-slow").await;
        let gateway = peer(24875);
        let (bad, late) = (peer(24876), peer(24877));
        let log = scripted_event_loop(events, vec![bad.socket_addr().unwrap()]);
        let tx = Transaction::new::<ConnectMsg>();
        let (replies_tx, replies_rx) = mpsc::channel(8);
        replies_tx.send(observed_address(tx)).await.unwrap();
        replies_tx.send(response(tx, &bad)).await.unwrap();
        let late_reply = late.clone();
        let feeder = async move {
            tokio::time::sleep(Duration::from_secs(30)).await;
            replies_tx.send(response(tx, &late_reply)).await.unwrap();
            tokio::time::sleep(Duration::from_secs(10)).await;
            // The transaction ends.
            drop(replies_tx);
        };
        let started = Instant::now();
        let (end, ()) = tokio::join!(
            drive_client_connect_inner(
                tx,
                gateway.clone(),
                gateway.socket_addr().unwrap(),
                Location::new(0.5),
                op_manager.ring.connection_manager.min_connections,
                true,
                replies_rx,
                &op_manager,
                ClientConnectKind::Standard,
            ),
            feeder
        );
        assert_eq!(end.expect("driver result"), ClientConnectEnd::Finished);
        assert!(
            started.elapsed() >= Duration::from_secs(40),
            "the CONNECT ended after {:?}, before its replies stopped",
            started.elapsed()
        );
        let log = log.lock();
        assert_eq!(log.connect_failed, vec![bad.socket_addr().unwrap()]);
        assert!(
            log.dials
                .iter()
                .any(|(a, _)| Some(*a) == late.socket_addr()),
            "the late acceptor must be dialled; dials {:?}",
            log.dials
        );
    }

    #[tokio::test(start_paused = true)]
    async fn a_connected_peers_connect_keeps_collecting_acceptors() {
        let (op_manager, events) = op_manager_with_events("driver-connected-peer").await;
        std::mem::forget(events);
        let existing = peer(24863);
        let addr = existing.socket_addr().unwrap();
        assert!(op_manager.ring.connection_manager.add_connection(
            Location::from_address(&addr),
            addr,
            existing.pub_key().clone(),
            false,
        ));
        let gateway = peer(24864);
        let tx = Transaction::new::<ConnectMsg>();
        let (replies_tx, replies_rx) = mpsc::channel(8);
        let outcome = tokio::time::timeout(
            Duration::from_secs(30),
            drive_client_connect_inner(
                tx,
                gateway.clone(),
                gateway.socket_addr().unwrap(),
                Location::new(0.5),
                op_manager.ring.connection_manager.min_connections,
                false,
                replies_rx,
                &op_manager,
                ClientConnectKind::Standard,
            ),
        )
        .await;
        assert!(
            outcome.is_err(),
            "a peer with a ring connection keeps its TTL-bounded collection"
        );
        drop(replies_tx);
    }

    /// Plays the event loop and the gateway: each request is answered with
    /// `answer(tx)`, if any, and its reply channel is then held open, so the
    /// CONNECT runs until the caller's overall timeout.
    fn scripted_gateway(
        mut op_execution: mpsc::Receiver<(
            mpsc::Sender<WaiterReply>,
            NetMessage,
            Option<SocketAddr>,
        )>,
        answer: fn(Transaction) -> Option<WaiterReply>,
    ) -> tokio::task::JoinHandle<()> {
        tokio::spawn(async move {
            let mut held = Vec::new();
            while let Some((reply, msg, _target)) = op_execution.recv().await {
                if let NetMessage::V1(NetMessageV1::Connect(ConnectMsg::Request { id, .. })) = msg {
                    if let Some(first) = answer(id) {
                        reply.send(first).await.ok();
                    }
                }
                held.push(reply);
            }
        })
    }

    fn drops_link(
        events: &mut mpsc::Receiver<Either<NetMessage, NodeEvent>>,
        gateway_addr: SocketAddr,
    ) -> bool {
        let mut dropped = false;
        while let Ok(event) = events.try_recv() {
            if matches!(event, Either::Right(NodeEvent::DropStaleGatewayLink { addr, .. }) if addr == gateway_addr)
            {
                dropped = true;
            }
        }
        dropped
    }

    /// One CONNECT through the real entry point, ended by an overall timeout
    /// past the gateway answer deadline.
    async fn connect_once(
        op_manager: &OpManager,
        gateway: &PeerKeyLocation,
        kind: ClientConnectKind,
    ) {
        let own = op_manager.ring.connection_manager.own_location();
        super::start_client_connect(
            Transaction::new::<ConnectMsg>(),
            gateway.clone(),
            gateway.socket_addr().unwrap(),
            op_manager,
            own,
            Location::new(0.5),
            Some(GATEWAY_ANSWER_DEADLINE + Duration::from_secs(5)),
            kind,
        )
        .await
        .expect("driver result");
    }

    /// Two gateway joins in a row through `gateway_addr`, each answered with
    /// `answer`; returns whether the first, then the second, dropped the link.
    async fn two_joins(
        id: &str,
        port: u16,
        addressed: bool,
        answer: fn(Transaction) -> Option<WaiterReply>,
    ) -> (bool, bool) {
        let _marker = NoGlobalAddress::new();
        let (op_manager, mut events, op_execution) = op_manager_at(id, addressed).await;
        let gateway = peer(port);
        let gateway_addr = gateway.socket_addr().unwrap();
        let script = scripted_gateway(op_execution, answer);
        connect_once(&op_manager, &gateway, ClientConnectKind::Standard).await;
        let first = drops_link(&mut events, gateway_addr);
        connect_once(&op_manager, &gateway, ClientConnectKind::Standard).await;
        let second = drops_link(&mut events, gateway_addr);
        script.abort();
        (first, second)
    }

    /// Through the real entry point: gateway joins with no `ObservedAddress`
    /// by the deadline drop the link on the second in a row, while the join
    /// is still running.
    #[tokio::test(start_paused = true)]
    async fn start_client_connect_drops_a_gateway_link_that_never_answers() {
        let (first, second) = two_joins("driver-dead-link-entry", 24867, false, |_| None).await;
        assert!(
            !first,
            "one silent round may be a slow gateway: the link must be kept"
        );
        assert!(
            second,
            "a second silent round in a row must drop the link for a redial"
        );
    }

    /// Gateway liveness is the gateway's own `ObservedAddress`, not whether an
    /// acceptor answered: with it the link is kept however quiet the rest of
    /// the CONNECT is, and an acceptor's Response alone does not show it.
    #[tokio::test(start_paused = true)]
    async fn gateway_liveness_is_the_gateways_observed_address() {
        let answered = two_joins("driver-liveness-observed", 24862, false, |tx| {
            Some(observed_address(tx))
        })
        .await;
        assert_eq!(
            answered,
            (false, false),
            "a gateway that answers keeps its link"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn an_acceptor_response_alone_does_not_show_the_gateway_link() {
        let acceptor_only = two_joins("driver-liveness-response", 24864, false, |tx| {
            Some(response(tx, &peer(24863)))
        })
        .await;
        assert_eq!(
            acceptor_only,
            (false, true),
            "an acceptor's Response without the gateway's ObservedAddress does not show the link"
        );
    }

    /// A slow gateway is not a dead one: two overlapping joins whose
    /// `ObservedAddress` arrives 10 s after each request, inside the deadline,
    /// keep the link.
    #[tokio::test(start_paused = true)]
    async fn a_gateway_that_answers_within_the_deadline_keeps_its_link() {
        let _marker = NoGlobalAddress::new();
        let (op_manager, mut events, mut op_execution) =
            unaddressed_op_manager("driver-liveness-slow").await;
        let gateway = peer(24882);
        let gateway_addr = gateway.socket_addr().unwrap();
        let script = tokio::spawn(async move {
            let mut answers = Vec::new();
            while let Some((reply, msg, _target)) = op_execution.recv().await {
                if let NetMessage::V1(NetMessageV1::Connect(ConnectMsg::Request { id, .. })) = msg {
                    answers.push(tokio::spawn(async move {
                        tokio::time::sleep(Duration::from_secs(10)).await;
                        reply.send(observed_address(id)).await.ok();
                        // Hold the channel open: the join runs to its timeout.
                        std::future::pending::<()>().await;
                    }));
                }
            }
        });
        tokio::join!(
            connect_once(&op_manager, &gateway, ClientConnectKind::Standard),
            connect_once(&op_manager, &gateway, ClientConnectKind::Standard),
        );
        assert!(
            !drops_link(&mut events, gateway_addr),
            "answers inside the deadline must not count as silence"
        );
        script.abort();
    }

    /// Overlapping joins through one gateway are judged against when each
    /// was sent (review item 8): a join sent before the gateway's last answer
    /// says nothing about the link now. Join A (t=0) goes unanswered, join B
    /// (t=5 s) is answered at once; A's deadline then passes, and one more
    /// unanswered join C must not make A and C two silent rounds in a row.
    #[tokio::test(start_paused = true)]
    async fn an_overlapping_join_sent_before_the_gateways_answer_is_not_counted() {
        let _marker = NoGlobalAddress::new();
        let (op_manager, mut events, mut op_execution) =
            unaddressed_op_manager("driver-liveness-overlap").await;
        let gateway = peer(24883);
        let gateway_addr = gateway.socket_addr().unwrap();
        let script = tokio::spawn(async move {
            let mut held = Vec::new();
            let mut nth = 0;
            while let Some((reply, msg, _target)) = op_execution.recv().await {
                if let NetMessage::V1(NetMessageV1::Connect(ConnectMsg::Request { id, .. })) = msg {
                    nth += 1;
                    if nth == 2 {
                        reply.send(observed_address(id)).await.ok();
                    }
                }
                held.push(reply);
            }
        });
        let a = connect_once(&op_manager, &gateway, ClientConnectKind::Standard);
        let b = async {
            tokio::time::sleep(Duration::from_secs(5)).await;
            connect_once(&op_manager, &gateway, ClientConnectKind::Standard).await;
            // B's ObservedAddress taught this peer its address; C starts
            // unaddressed again so it is judged.
            op_manager.ring.connection_manager.clear_own_addr_for_test();
        };
        tokio::join!(a, b);
        connect_once(&op_manager, &gateway, ClientConnectKind::Standard).await;
        assert!(
            !drops_link(&mut events, gateway_addr),
            "A was sent before the gateway answered B; it must not count toward a redial"
        );
        script.abort();
    }

    /// A cached-peer reconnect is a Standard join at zero connections too,
    /// but through a peer that is no gateway: its silence says nothing about
    /// a gateway link and never drops one (review nit).
    #[tokio::test(start_paused = true)]
    async fn a_join_through_a_non_gateway_never_counts_toward_a_redial() {
        let (first, second) = two_joins("driver-liveness-cached", 24884, false, |_| None).await;
        assert_eq!((first, second), (false, false));
    }

    /// If the drop of a dead link cannot be queued (the event loop's channel
    /// is gone or full), the recorded redial is undone (Codex re-review P2):
    /// otherwise the link would wait out MIN_REDIAL_INTERVAL for a redial that
    /// never happened.
    #[tokio::test(start_paused = true)]
    async fn a_redial_that_could_not_be_queued_is_undone() {
        let (op_manager, events, op_execution) =
            unaddressed_op_manager("driver-redial-unsent").await;
        // The event loop is gone: every notification fails to queue.
        drop(events);
        let gateway = peer(24867);
        let gateway_addr = gateway.socket_addr().unwrap();
        let script = scripted_gateway(op_execution, |_| None);
        connect_once(&op_manager, &gateway, ClientConnectKind::Standard).await;
        connect_once(&op_manager, &gateway, ClientConnectKind::Standard).await;
        let now = Instant::now();
        assert!(
            op_manager
                .bootstrap_pacer
                .lock()
                .gateway_silent(gateway_addr, now, now),
            "the next silent join must ask for the redial again"
        );
        script.abort();
    }

    /// A peer that knows its address gets no `ObservedAddress`, so silence
    /// proves nothing about its gateway link.
    #[tokio::test(start_paused = true)]
    async fn an_addressed_peers_silent_joins_do_not_drop_the_link() {
        let dropped = two_joins("driver-liveness-addressed", 24869, true, |_| None).await;
        assert_eq!(dropped, (false, false));
    }

    /// A version probe runs outside the bootstrap pacer, so its silence must
    /// not count toward redialling the gateway link: two silent probes and
    /// then one silent join drop nothing.
    #[tokio::test(start_paused = true)]
    async fn a_silent_version_probe_does_not_count_toward_a_redial() {
        let (op_manager, mut events, op_execution) =
            unaddressed_op_manager("driver-probe-silent").await;
        let gateway = peer(24868);
        let gateway_addr = gateway.socket_addr().unwrap();
        let script = scripted_gateway(op_execution, |_| None);
        for kind in [
            ClientConnectKind::VersionProbe,
            ClientConnectKind::VersionProbe,
            ClientConnectKind::Standard,
        ] {
            connect_once(&op_manager, &gateway, kind).await;
        }
        assert!(
            !drops_link(&mut events, gateway_addr),
            "a probe's silence redialled the gateway link"
        );
        script.abort();
    }

    /// What the scripted event loop saw.
    #[derive(Debug, Default)]
    struct DialLog {
        /// Each acceptor dialled, and when (from the start of the test).
        dials: Vec<(SocketAddr, Duration)>,
        /// Most dials in flight at once.
        max_in_flight: usize,
        /// Acceptors the driver reported to the gateway as failed.
        connect_failed: Vec<SocketAddr>,
        /// The origin each dial was tagged with.
        origins: Vec<crate::transport::NatAttemptOrigin>,
    }

    /// Plays the event loop: every dial takes the NAT traversal deadline and
    /// fails for the acceptors in `unreachable`, connects otherwise.
    fn scripted_event_loop(
        mut events: mpsc::Receiver<Either<NetMessage, NodeEvent>>,
        unreachable: Vec<SocketAddr>,
    ) -> Arc<parking_lot::Mutex<DialLog>> {
        let log = Arc::new(parking_lot::Mutex::new(DialLog::default()));
        let in_flight = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let t0 = Instant::now();
        let shared = log.clone();
        tokio::spawn(async move {
            use std::sync::atomic::Ordering::SeqCst;
            while let Some(event) = events.recv().await {
                match event {
                    Either::Right(NodeEvent::ConnectPeer {
                        peer,
                        callback,
                        origin,
                        ..
                    }) => {
                        let addr = peer.socket_addr().expect("acceptor address");
                        let now_in_flight = in_flight.fetch_add(1, SeqCst) + 1;
                        {
                            let mut log = shared.lock();
                            log.dials.push((addr, t0.elapsed()));
                            log.origins.push(origin);
                            log.max_in_flight = log.max_in_flight.max(now_in_flight);
                        }
                        let ok = !unreachable.contains(&addr);
                        let in_flight = in_flight.clone();
                        tokio::spawn(async move {
                            tokio::time::sleep(NAT_TRAVERSAL_DEADLINE).await;
                            in_flight.fetch_sub(1, SeqCst);
                            let result = if ok { Ok((addr, None)) } else { Err(()) };
                            callback.send(result).await.ok();
                        });
                    }
                    Either::Right(NodeEvent::SendNetMessage { msg, .. }) => {
                        if let NetMessage::V1(NetMessageV1::Connect(ConnectMsg::ConnectFailed {
                            failed_acceptor_addr,
                            ..
                        })) = *msg
                        {
                            shared.lock().connect_failed.push(failed_acceptor_addr);
                        }
                    }
                    Either::Left(_) | Either::Right(_) => {}
                }
            }
        });
        log
    }

    /// Bookkeeping a successful CONNECT must leave, set up to fail first.
    fn record_prior_failures(op_manager: &OpManager, gateway_addr: SocketAddr, desired: Location) {
        op_manager
            .gateway_backoff
            .lock()
            .record_failure(gateway_addr);
        op_manager.ring.record_connection_failure(
            desired,
            crate::ring::ConnectionFailureReason::RoutingFailed,
        );
        op_manager
            .ring
            .connection_manager
            .increment_connect_jitter_failures(Instant::now());
        assert!(
            op_manager
                .gateway_backoff
                .lock()
                .is_in_backoff(gateway_addr)
        );
        assert!(op_manager.ring.is_in_connection_backoff(desired));
        assert!(
            op_manager
                .ring
                .connection_manager
                .connect_jitter_failure_count()
                > 0
        );
    }

    fn assert_success_recorded(
        op_manager: &OpManager,
        gateway_addr: SocketAddr,
        desired: Location,
    ) {
        assert!(
            !op_manager
                .gateway_backoff
                .lock()
                .is_in_backoff(gateway_addr),
            "the gateway's backoff must be cleared"
        );
        assert!(
            !op_manager.ring.is_in_connection_backoff(desired),
            "the target's connection backoff must be cleared"
        );
        assert_eq!(
            op_manager
                .ring
                .connection_manager
                .connect_jitter_failure_count(),
            0,
            "the jitter failure count must be reset"
        );
    }

    /// The dial-queue wait is recorded only once `ConnectPeer` is handed to
    /// the event loop: a dial whose send fails records nothing, and a dial
    /// whose sends were held up counts the time they were held.
    #[tokio::test(start_paused = true)]
    async fn the_dial_queue_wait_is_recorded_only_after_the_dial_is_sent() {
        use crate::transport::metrics::DialQueueWait;
        // The wait log is process-global and keyed only by address, so these
        // ports (24920..=24922) must not be used by any other test in the
        // crate: under plain `cargo test`, a test that dials one of them
        // leaves a wait this test then reads as its own.
        let waits = |p: &PeerKeyLocation| {
            crate::transport::TRANSPORT_METRICS.dial_queue_waits_for(p.socket_addr().unwrap())
        };
        let tx = Transaction::new::<ConnectMsg>();

        // The event loop is gone: the dial is never sent.
        let (op_manager, events) = op_manager_with_events("dial-wait-unsent").await;
        drop(events);
        let unsent = peer(24920);
        assert!(
            super::begin_acceptor_dial(&op_manager, tx, unsent.clone(), Instant::now())
                .await
                .is_err(),
            "premise: the dial cannot be sent"
        );
        assert!(waits(&unsent).is_empty(), "an unsent dial records no wait");

        // `ExpectPeerConnection` goes out but `ConnectPeer` cannot: the
        // channel has room for exactly one event, and the event loop stops
        // accepting before taking more.
        let (op_manager, mut events) = op_manager_with_events("dial-wait-half-sent").await;
        while op_manager
            .try_notify_node_event(NodeEvent::TransactionCompleted(tx))
            .is_ok()
        {}
        drop(events.recv().await.expect("a filler event"));
        let half = peer(24922);
        let half_addr = half.socket_addr().unwrap();
        let closing_loop = tokio::spawn(async move {
            tokio::time::sleep(Duration::from_secs(1)).await;
            events.close();
            let mut expected = false;
            while let Some(event) = events.recv().await {
                match event {
                    Either::Right(NodeEvent::ExpectPeerConnection { addr, .. })
                        if addr == half_addr =>
                    {
                        expected = true;
                    }
                    Either::Right(NodeEvent::ConnectPeer { .. }) => panic!("ConnectPeer was sent"),
                    Either::Left(_) | Either::Right(_) => {}
                }
            }
            expected
        });
        assert!(
            super::begin_acceptor_dial(&op_manager, tx, half.clone(), Instant::now())
                .await
                .is_err(),
            "premise: ConnectPeer cannot be sent"
        );
        assert!(
            closing_loop.await.expect("event loop"),
            "premise: ExpectPeerConnection was sent"
        );
        assert!(
            waits(&half).is_empty(),
            "a dial whose ConnectPeer was not sent records no wait"
        );

        // The event loop's channel is full and is not drained for 4 s, so the
        // sends block that long: the recorded wait must span it.
        let (op_manager, mut events) = op_manager_with_events("dial-wait-held").await;
        let mut filler = 0usize;
        while op_manager
            .try_notify_node_event(NodeEvent::TransactionCompleted(tx))
            .is_ok()
        {
            filler += 1;
        }
        assert!(filler > 0, "premise: the channel filled");
        let held = peer(24921);
        let answered_at = Instant::now();
        let slow_loop = tokio::spawn(async move {
            tokio::time::sleep(Duration::from_secs(4)).await;
            while let Some(event) = events.recv().await {
                if let Either::Right(NodeEvent::ConnectPeer { callback, .. }) = event {
                    return callback;
                }
            }
            panic!("no ConnectPeer");
        });
        let dial = super::begin_acceptor_dial(&op_manager, tx, held.clone(), answered_at)
            .await
            .expect("sent");
        assert_eq!(waits(&held), vec![DialQueueWait::Ge3S]);
        drop(slow_loop.await.expect("event loop"));
        drop(dial);
    }

    /// An acceptor's punch towards the joiner it accepted is tagged
    /// `ConnectAcceptor`.
    #[tokio::test]
    async fn an_acceptors_dial_to_its_joiner_is_tagged_connect_acceptor() {
        let (op_manager, mut events) = op_manager_with_events("acceptor-origin").await;
        let joiner = peer(24880);
        let tx = Transaction::new::<ConnectMsg>();
        super::super::dispatch_expect_connection_from(&op_manager, tx, joiner.clone())
            .await
            .expect("dispatch");
        loop {
            let event = tokio::time::timeout(Duration::from_secs(5), events.recv())
                .await
                .expect("a ConnectPeer is sent")
                .expect("events open");
            if let Either::Right(NodeEvent::ConnectPeer { peer, origin, .. }) = event {
                assert_eq!(peer, joiner);
                assert_eq!(origin, crate::transport::NatAttemptOrigin::ConnectAcceptor);
                break;
            }
        }
    }

    /// A peer that already has a connection (#5858 leaves it unchanged):
    /// acceptors are dialled one at a time, a failed one is reported to the
    /// gateway, and the CONNECT ends when `target_connections` acceptors have
    /// actually connected, not when that many have answered.
    #[tokio::test(start_paused = true)]
    async fn a_connected_peers_connect_dials_one_at_a_time_and_ends_at_the_target() {
        let (op_manager, events) = op_manager_with_events("driver-connected-target").await;
        let existing = peer(24870);
        let existing_addr = existing.socket_addr().unwrap();
        assert!(op_manager.ring.connection_manager.add_connection(
            Location::from_address(&existing_addr),
            existing_addr,
            existing.pub_key().clone(),
            false,
        ));
        let gateway = peer(24871);
        let gateway_addr = gateway.socket_addr().unwrap();
        let (bad, good, other) = (peer(24872), peer(24873), peer(24874));
        let desired = Location::new(0.5);
        record_prior_failures(&op_manager, gateway_addr, desired);
        let log = scripted_event_loop(events, vec![bad.socket_addr().unwrap()]);
        let tx = Transaction::new::<ConnectMsg>();
        let (replies_tx, replies_rx) = mpsc::channel(8);
        for acceptor in [&bad, &good, &other] {
            replies_tx.send(response(tx, acceptor)).await.unwrap();
        }
        let end = tokio::time::timeout(
            Duration::from_secs(50),
            drive_client_connect_inner(
                tx,
                gateway.clone(),
                gateway_addr,
                desired,
                2,
                false,
                replies_rx,
                &op_manager,
                ClientConnectKind::Standard,
            ),
        )
        .await
        .expect("the target is reachable well inside the TTL")
        .expect("driver result");
        assert_eq!(end, ClientConnectEnd::Finished);
        let log = log.lock();
        let dialled: Vec<_> = log.dials.iter().map(|(a, _)| *a).collect();
        assert_eq!(
            dialled,
            vec![
                bad.socket_addr().unwrap(),
                good.socket_addr().unwrap(),
                other.socket_addr().unwrap()
            ],
            "two answered acceptors are not two connections: the third must be dialled"
        );
        assert_eq!(log.max_in_flight, 1, "a connected peer dials one at a time");
        assert_eq!(log.connect_failed, vec![bad.socket_addr().unwrap()]);
        assert_eq!(
            log.origins,
            vec![crate::transport::NatAttemptOrigin::ConnectJoiner; 3],
            "a joiner's acceptor dials are tagged ConnectJoiner"
        );
        // Each waited in the queue from its Response until the dial before
        // it ended: `bad` at once, the others for one or two hole-punch
        // deadlines (3 s each).
        {
            use crate::transport::metrics::DialQueueWait;
            let waits = |p: &PeerKeyLocation| {
                crate::transport::TRANSPORT_METRICS.dial_queue_waits_for(p.socket_addr().unwrap())
            };
            assert_eq!(waits(&bad), vec![DialQueueWait::Lt100Ms]);
            assert_eq!(waits(&good), vec![DialQueueWait::Ge3S]);
            assert_eq!(waits(&other), vec![DialQueueWait::Ge3S]);
        }
        assert_success_recorded(&op_manager, gateway_addr, desired);
        drop(replies_tx);
    }

    /// A peer with a ring connection (it dials one acceptor at a time).
    fn connect_existing_peer(op_manager: &OpManager, port: u16) {
        let existing = peer(port);
        let addr = existing.socket_addr().unwrap();
        assert!(op_manager.ring.connection_manager.add_connection(
            Location::from_address(&addr),
            addr,
            existing.pub_key().clone(),
            false,
        ));
    }

    /// Feeds `replies` to a CONNECT, then ends its collection with `last`
    /// (`None`: the waiter closes). Returns what the event loop saw.
    async fn collection_ends_after(
        op_manager: &OpManager,
        events: mpsc::Receiver<Either<NetMessage, NodeEvent>>,
        unreachable: Vec<SocketAddr>,
        target_connections: usize,
        replies: Vec<WaiterReply>,
        last: Option<WaiterReply>,
    ) -> Arc<parking_lot::Mutex<DialLog>> {
        let log = scripted_event_loop(events, unreachable);
        let gateway = peer(24890);
        let (replies_tx, replies_rx) = mpsc::channel(64);
        for reply in replies {
            replies_tx.send(reply).await.unwrap();
        }
        if let Some(last) = last {
            replies_tx.send(last).await.unwrap();
        }
        drop(replies_tx);
        let end = tokio::time::timeout(
            Duration::from_secs(60),
            drive_client_connect_inner(
                Transaction::new::<ConnectMsg>(),
                gateway.clone(),
                gateway.socket_addr().unwrap(),
                Location::new(0.5),
                target_connections,
                false,
                replies_rx,
                op_manager,
                ClientConnectKind::Standard,
            ),
        )
        .await
        .expect("the drain is bounded")
        .expect("driver result");
        assert_eq!(end, ClientConnectEnd::Finished);
        // Let the scripted event loop take the driver's last notification.
        tokio::time::sleep(Duration::from_millis(10)).await;
        log
    }

    /// When the waiter closes, acceptors already taken are still dialled,
    /// queued ones included (review item 2: a connected peer dials one at a
    /// time, so the second acceptor is queued when the waiter closes), but a
    /// failed punch asks for no re-route (re-review N1).
    #[tokio::test(start_paused = true)]
    async fn a_closed_waiter_still_finishes_started_and_queued_dials() {
        let (op_manager, events) = op_manager_with_events("driver-drain-closed").await;
        connect_existing_peer(&op_manager, 24891);
        let tx = Transaction::new::<ConnectMsg>();
        let (bad, good) = (peer(24892), peer(24893));
        let log = collection_ends_after(
            &op_manager,
            events,
            vec![bad.socket_addr().unwrap()],
            op_manager.ring.connection_manager.min_connections,
            vec![response(tx, &bad), response(tx, &good)],
            None,
        )
        .await;
        let log = log.lock();
        let dialled: Vec<_> = log.dials.iter().map(|(a, _)| *a).collect();
        assert_eq!(
            dialled,
            vec![bad.socket_addr().unwrap(), good.socket_addr().unwrap()]
        );
        assert!(
            log.connect_failed.is_empty(),
            "no re-route once collection has ended: nobody would read it"
        );
    }

    /// Simulation time for transaction TTLs on this (single-threaded) test
    /// runtime, cleared on drop so it cannot leak into the next test.
    struct SimulationClock;

    impl SimulationClock {
        fn set(ms: u64) -> Self {
            crate::config::GlobalSimulationTime::set_time_ms(ms);
            Self
        }
    }

    impl Drop for SimulationClock {
        fn drop(&mut self) {
            crate::config::GlobalSimulationTime::clear_time();
        }
    }

    /// The same when the transaction's TTL runs out while a dial is in flight
    /// (re-review testing 2). Deterministic: the TTL reads the simulation
    /// clock, which the test moves past the TTL 1 s into a 3 s dial (paused
    /// tokio time).
    #[tokio::test(start_paused = true)]
    async fn a_ttl_expiring_mid_dial_still_lets_the_dial_finish() {
        let (op_manager, events) = op_manager_with_events("driver-drain-ttl").await;
        let _log = scripted_event_loop(events, vec![]);
        const EPOCH_MS: u64 = 1_700_000_000_000;
        let _clock = SimulationClock::set(EPOCH_MS);
        let tx = Transaction::new::<ConnectMsg>();
        let gateway = peer(24896);
        let acceptor = peer(24897);
        let (replies_tx, replies_rx) = mpsc::channel(8);
        replies_tx.send(response(tx, &acceptor)).await.unwrap();
        let expire = async {
            tokio::time::sleep(Duration::from_secs(1)).await;
            crate::config::GlobalSimulationTime::set_time_ms(
                EPOCH_MS + crate::config::OPERATION_TTL.as_millis() as u64 + 1_000,
            );
        };
        let mut connected = 0;
        let started = Instant::now();
        let (end, ()) = tokio::join!(
            super::drive_client_connect(
                tx,
                gateway.clone(),
                gateway.socket_addr().unwrap(),
                Location::new(0.5),
                op_manager.ring.connection_manager.min_connections,
                false,
                replies_rx,
                &op_manager,
                ClientConnectKind::Standard,
                &mut connected,
            ),
            expire
        );
        assert_eq!(end.expect("driver result"), ClientConnectEnd::Finished);
        assert_eq!(connected, 1, "the dial in flight at the TTL was finished");
        assert!(
            started.elapsed() >= NAT_TRAVERSAL_DEADLINE,
            "the CONNECT waited for its dial"
        );
        drop(replies_tx);
    }

    /// The same when the gateway is pruned mid-flight.
    #[tokio::test(start_paused = true)]
    async fn a_pruned_gateway_still_lets_started_dials_finish() {
        let (op_manager, events) = op_manager_with_events("driver-drain-pruned").await;
        let tx = Transaction::new::<ConnectMsg>();
        let bad = peer(24894);
        let log = collection_ends_after(
            &op_manager,
            events,
            vec![bad.socket_addr().unwrap()],
            op_manager.ring.connection_manager.min_connections,
            vec![response(tx, &bad)],
            Some(WaiterReply::PeerDisconnected {
                peer: peer(24890).socket_addr().unwrap(),
            }),
        )
        .await;
        let log = log.lock();
        assert_eq!(log.dials.len(), 1);
        assert!(
            log.connect_failed.is_empty(),
            "no re-route through a pruned gateway: it would start a fresh handshake"
        );
    }

    /// A first hop that keeps sending distinct Responses gets no more than
    /// `acceptor_limit` dials out of one CONNECT (review item 1).
    #[tokio::test(start_paused = true)]
    async fn one_connect_dials_no_more_than_its_acceptor_limit() {
        let (op_manager, events) = op_manager_with_events("driver-acceptor-limit").await;
        let tx = Transaction::new::<ConnectMsg>();
        let acceptors: Vec<_> = (0..10).map(|i| peer(24900 + i)).collect();
        let unreachable = acceptors.iter().map(|a| a.socket_addr().unwrap()).collect();
        let target = 2;
        let log = collection_ends_after(
            &op_manager,
            events,
            unreachable,
            target,
            acceptors.iter().map(|a| response(tx, a)).collect(),
            None,
        )
        .await;
        assert_eq!(
            log.lock().dials.len(),
            crate::operations::connect::bootstrap::acceptor_limit(target)
        );
    }

    fn rejected(tx: Transaction) -> WaiterReply {
        WaiterReply::Reply(NetMessage::V1(NetMessageV1::Connect(
            ConnectMsg::Rejected {
                id: tx,
                desired_location: Location::new(0.5),
            },
        )))
    }

    /// A Rejected arriving while an acceptor is being dialled (a near-terminus
    /// relay accepted and forwarded on, and a later hop rejected) must not
    /// abandon that dial. Returns the end, what the event loop saw, and
    /// whether the gateway was backed off.
    async fn rejected_mid_dial(
        id: &str,
        acceptor_reachable: bool,
    ) -> (ClientConnectEnd, Vec<SocketAddr>, bool) {
        rejected_mid_dial_as(id, acceptor_reachable, false).await
    }

    async fn rejected_mid_dial_as(
        id: &str,
        acceptor_reachable: bool,
        connected_peer: bool,
    ) -> (ClientConnectEnd, Vec<SocketAddr>, bool) {
        let (op_manager, events) = op_manager_with_events(id).await;
        if connected_peer {
            connect_existing_peer(&op_manager, 24895);
        }
        let gateway = peer(24880);
        let gateway_addr = gateway.socket_addr().unwrap();
        let acceptor = peer(24881);
        let unreachable = if acceptor_reachable {
            vec![]
        } else {
            vec![acceptor.socket_addr().unwrap()]
        };
        let log = scripted_event_loop(events, unreachable);
        let tx = Transaction::new::<ConnectMsg>();
        let (replies_tx, replies_rx) = mpsc::channel(8);
        replies_tx.send(response(tx, &acceptor)).await.unwrap();
        replies_tx.send(rejected(tx)).await.unwrap();
        let end = tokio::time::timeout(
            Duration::from_secs(30),
            drive_client_connect_inner(
                tx,
                gateway.clone(),
                gateway_addr,
                Location::new(0.5),
                op_manager.ring.connection_manager.min_connections,
                false,
                replies_rx,
                &op_manager,
                ClientConnectKind::Standard,
            ),
        )
        .await
        .expect("a rejected CONNECT ends once its dial has")
        .expect("driver result");
        // Let the scripted event loop take the driver's last notification.
        tokio::time::sleep(Duration::from_millis(10)).await;
        assert_eq!(
            log.lock().dials.len(),
            1,
            "the acceptor that answered before the rejection is dialled"
        );
        let failed = log.lock().connect_failed.clone();
        let backed_off = op_manager
            .gateway_backoff
            .lock()
            .is_in_backoff(gateway_addr);
        drop(replies_tx);
        (end, failed, backed_off)
    }

    #[tokio::test(start_paused = true)]
    async fn a_rejection_lets_a_started_dial_finish_and_spares_the_gateway_if_it_connects() {
        let (end, failed, backed_off) = rejected_mid_dial("driver-reject-ok", true).await;
        assert_eq!(end, ClientConnectEnd::Finished);
        assert!(failed.is_empty());
        assert!(
            !backed_off,
            "the CONNECT produced a connection: the gateway must not be backed off"
        );
    }

    /// A peer that already has a connection is rotated off a rejecting
    /// gateway whatever its dials do, as on main (rule 2c, review item 3).
    #[tokio::test(start_paused = true)]
    async fn a_connected_peer_is_always_rotated_off_a_rejecting_gateway() {
        let (end, failed, backed_off) =
            rejected_mid_dial_as("driver-reject-connected", true, true).await;
        assert_eq!(end, ClientConnectEnd::Finished);
        assert!(failed.is_empty());
        assert!(
            backed_off,
            "a connected peer always backs off a rejecting gateway"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn a_rejection_drains_a_failing_dial_without_a_reroute() {
        let (end, failed, backed_off) = rejected_mid_dial("driver-reject-fail", false).await;
        assert_eq!(end, ClientConnectEnd::Finished);
        assert!(
            failed.is_empty(),
            "after a Rejected nobody reads a re-routed acceptor's Response"
        );
        assert!(backed_off, "nothing connected: the gateway is backed off");
    }

    #[tokio::test(start_paused = true)]
    async fn a_dead_gateway_link_is_dropped_but_never_a_ring_connection() {
        let (op_manager, mut events) = op_manager_with_events("driver-dead-link").await;
        let gateway = peer(24865).socket_addr().unwrap();
        let tx = Transaction::new::<ConnectMsg>();
        let drop = {
            let op_manager = op_manager.clone();
            tokio::spawn(async move {
                super::drop_dead_gateway_link(&op_manager, tx, gateway).await;
            })
        };
        let done = match events.recv().await {
            Some(Either::Right(NodeEvent::DropStaleGatewayLink { addr, done })) => {
                assert_eq!(addr, gateway);
                done
            }
            other => panic!("expected a DropStaleGatewayLink for the gateway, got {other:?}"),
        };
        tokio::task::yield_now().await;
        assert!(
            !drop.is_finished(),
            "the caller must wait for the event loop to drop the link"
        );
        done.try_send(()).unwrap();
        tokio::time::timeout(Duration::from_millis(100), drop)
            .await
            .expect("the caller returns once the drop is confirmed")
            .unwrap();

        let in_ring = peer(24866);
        let in_ring_addr = in_ring.socket_addr().unwrap();
        assert!(op_manager.ring.connection_manager.add_connection(
            Location::from_address(&in_ring_addr),
            in_ring_addr,
            in_ring.pub_key().clone(),
            false,
        ));
        super::drop_dead_gateway_link(&op_manager, tx, in_ring_addr).await;
        assert!(
            events.try_recv().is_err(),
            "a gateway that is a ring connection must not be dropped"
        );
    }
}
