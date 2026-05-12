//! Task-per-transaction drivers for CONNECT.
//!
//! The joiner driver holds state in task locals, sends a single
//! `ConnectMsg::Request` to the first-hop gateway via
//! `OpCtx::send_to_and_collect_replies`, and drains a multi-reply
//! receiver until `target_connections` acceptances arrive or an
//! overall timeout fires.
//!
//! Side effects per reply variant:
//!
//! - `Response`: `NetEventLog::connect_response_received`,
//!   `NodeEvent::ExpectPeerConnection`, `NodeEvent::ConnectPeer`,
//!   then await the callback (sequential per acceptor). On
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
use crate::node::OpManager;
use crate::operations::OpError;
use crate::ring::{ConnectionFailureReason, Location, PeerKeyLocation};
use crate::tracing::NetEventLog;

use super::{
    ConnectMsg, ConnectRequest, ForwardAttempt, RelayEnv, RelayState,
    dispatch_expect_connection_from,
};

// ─── Relay CONNECT scaffolding ───────────────────────────────────────
//
// Counters and RAII guard for `start_relay_connect`.

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
///
/// `tx` is supplied by the caller so the joiner-side transaction id
/// can be registered with `LiveTransactionTracker` before the driver
/// task is spawned. Allocating tx inside the driver would race the
/// caller's `add_transaction` registration against the first inbound
/// Response, which can arrive before the spawn completes on a busy
/// runtime.
pub(crate) async fn start_client_connect(
    tx: Transaction,
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
                // The joiner doesn't know its external IP behind NAT;
                // the gateway observes it from the UDP packet source
                // and ships it back. Update both `own_addr` (for
                // diagnostics) and `own_location` (for routing).
                // Idempotent: subsequent observations overwrite.
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
            "CONNECT relay (task-per-tx): duplicate Request for in-flight tx, dropping"
        );
        return Ok(());
    }

    tracing::debug!(
        tx = %incoming_tx,
        desired_location = %payload.desired_location,
        ttl = payload.ttl,
        %upstream_addr,
        phase = "relay_connect_start",
        "CONNECT relay (task-per-tx): spawning driver"
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
            "CONNECT relay (task-per-tx): driver returned error"
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
                    "CONNECT relay (task-per-tx): gateway overloaded, rejecting request"
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
                                "CONNECT relay (task-per-tx): failed to dispatch downstream Request"
                            );
                            None
                        }
                    }
                }
                None => {
                    tracing::warn!(
                        tx = %incoming_tx,
                        next_peer = %next.pub_key(),
                        "CONNECT relay (task-per-tx): next hop has no socket address; skipping forward"
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

    if initial_actions.rejected {
        if let Some(event) = NetEventLog::connect_rejected(
            &incoming_tx,
            &op_manager.ring,
            state.request.desired_location,
            "rejected by handle_request",
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
                "CONNECT relay (task-per-tx): transaction timed out; exiting"
            );
            return Ok(());
        }

        let msg = match tokio::time::timeout(RELAY_RECV_POLL_INTERVAL, receiver.recv()).await {
            Ok(Some(msg)) => msg,
            Ok(None) => return Ok(()),
            Err(_) => continue,
        };

        let connect_msg = match msg {
            NetMessage::V1(NetMessageV1::Connect(c)) => c,
            other => {
                tracing::warn!(
                    tx = %incoming_tx,
                    "CONNECT relay (task-per-tx): unexpected message kind: {:?}",
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
                    "CONNECT relay (task-per-tx): forwarding Response upstream"
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
                        "CONNECT relay (task-per-tx): retrying with different uphill peer after rejection"
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
                        "CONNECT relay (task-per-tx): accepting locally after uphill rejection"
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
                        "CONNECT relay (task-per-tx): forwarding rejection upstream (no retry peers)"
                    );
                    if let Some(event) = NetEventLog::connect_rejected(
                        &incoming_tx,
                        &op_manager.ring,
                        dl,
                        "relay no retry peers available",
                    ) {
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
                                "CONNECT relay (task-per-tx): forwarding ConnectFailed downstream"
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
                        "CONNECT relay (task-per-tx): re-routing to different peer after ConnectFailed"
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
                        "CONNECT relay (task-per-tx): accepting locally after ConnectFailed"
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
                        failed_acceptor = %failed_acceptor_addr,
                        "CONNECT relay (task-per-tx): propagating ConnectFailed upstream (no re-route)"
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
                    "CONNECT relay (task-per-tx): unexpected Request on driver inbox; bypass logic regressed"
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

    /// Source-literal guard: relay driver runs the full re-entry
    /// state machine (Response/Rejected/ObservedAddress/ConnectFailed)
    /// on its inbox receiver. Removing any of these branches would
    /// strand the corresponding re-entry on legacy `process_message`,
    /// which is being retired in commit 4.
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
}
