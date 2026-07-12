//! Task-per-transaction GET drivers.
//!
//! Each entry point — client-initiated, relay, sub-op, targeted
//! sub-op — owns routing state in task locals. No `GetOp` is pushed
//! into `OpManager.ops.get`.
//!
//! # Client-initiated flow
//!
//! 1. Loop calling [`OpCtx::send_and_await`] with a fresh
//!    `Transaction` per attempt (single-use-per-tx constraint).
//! 2. Terminal `Response{Found}`: store the returned state into the
//!    local executor via `PutQuery`, run hosting / access-tracking /
//!    announce side effects, publish
//!    `HostResponse::ContractResponse::GetResponse`. The reply
//!    bypass intercepts before `process_message` would run on the
//!    originator, so the driver is the only place these side effects
//!    happen.
//! 3. Terminal `ResponseStreaming`: deliver the final payload via a
//!    store re-query using the contract key from the envelope.
//! 4. Terminal Request-echo: the pre-send local-cache shortcut in
//!    `client_events.rs` already returned the state; the driver
//!    resolves the `ContractKey` for telemetry and client delivery.
//! 5. `Response{NotFound}`: advance to next peer.
//! 6. Timeout / wire-error: advance or exhaust.
//!
//! # Connection-drop latency
//!
//! Disconnect detection is event-driven: when a peer disconnects,
//! `Ring::prune_connection` enumerates `LiveTransactionTracker` orphans
//! and `P2pBridge::handle_orphaned_transactions` emits
//! `NodeEvent::TransactionCompleted` for each, which closes the
//! parked driver's `pending_op_results` waiter. Wake latency is
//! sub-millisecond (#4154). `OPERATION_TTL` (60 s) remains the upper
//! bound for cases where the downstream peer never formally
//! disconnects — silent stalls, slow-loris, partition-without-prune.

use std::net::SocketAddr;
use std::sync::Arc;

use freenet_stdlib::client_api::{ContractResponse, ErrorKind, HostResponse};
use freenet_stdlib::prelude::*;

use crate::client_events::HostResult;
use crate::config::{GlobalExecutor, OPERATION_TTL};
use crate::contract::{ContractHandlerEvent, StoreResponse};
use crate::message::{NetMessage, NetMessageV1, NodeEvent, Transaction};
use crate::node::NetworkBridge;
use crate::node::OpManager;
#[rustfmt::skip]
use crate::operations::op_ctx::{
    AdvanceOutcome, AttemptOutcome, RetryDriver, RetryLoopOutcome, drive_retry_loop,
};
use crate::operations::OpError;
use crate::operations::VisitedPeers;
use crate::operations::bootstrap::bootstrap_gateway_target;
use crate::ring::{Location, PeerKeyLocation};
use crate::router::{RouteEvent, RouteOutcome};
use crate::transport::peer_connection::StreamId;

use super::{GetMsg, GetMsgResult, GetStreamingPayload};
use crate::operations::orphan_streams::{OrphanStreamError, STREAM_CLAIM_TIMEOUT};

/// Test-only counter that increments every time `start_client_get` is
/// called. Used by integration tests to verify that a GET actually
/// routed through the driver rather than being satisfied
/// by the `client_events.rs` local-cache shortcut.
#[cfg(any(test, feature = "testing"))]
pub static DRIVER_CALL_COUNT: std::sync::atomic::AtomicUsize =
    std::sync::atomic::AtomicUsize::new(0);

/// Test-only counter that increments every time `start_relay_get` is
/// called. Used by integration tests to verify that relay dispatch
/// actually routed a fresh inbound Request through the driver
/// driver (and not through the legacy `handle_op_request` path that
/// continues to serve originator loop-back, GC-spawned retries, and
/// `start_targeted_op` UPDATE-triggered auto-fetches).
#[cfg(any(test, feature = "testing"))]
pub static RELAY_DRIVER_CALL_COUNT: std::sync::atomic::AtomicUsize =
    std::sync::atomic::AtomicUsize::new(0);

/// Number of relay-GET driver tasks currently executing. Incremented
/// on spawn, decremented on completion (both success and error paths).
/// Surfaced via the periodic `memory_stats` dump so we can correlate
/// RSS growth with spawn/complete rate and rule in/out a spawn storm
/// vs. per-task memory bloat as the cause of phase-5 OOMs.
pub static RELAY_INFLIGHT: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(0);

/// Lifetime count of relay-GET driver spawns. Paired with
/// `RELAY_COMPLETED_TOTAL` so the `memory_stats` dump can show whether
/// spawn rate exceeds completion rate (drivers stuck in retry loops)
/// vs. balanced (per-task bloat is the culprit).
pub static RELAY_SPAWNED_TOTAL: std::sync::atomic::AtomicUsize =
    std::sync::atomic::AtomicUsize::new(0);

/// Lifetime count of relay-GET driver completions. See
/// `RELAY_SPAWNED_TOTAL`.
pub static RELAY_COMPLETED_TOTAL: std::sync::atomic::AtomicUsize =
    std::sync::atomic::AtomicUsize::new(0);

/// Lifetime count of relay-GET spawns rejected by the dedup gate
/// because a driver for the same `incoming_tx` was already active on
/// this node. Surfaced via `memory_stats` so we can confirm the gate
/// fires under fault-loss retry retransmissions.
pub static RELAY_DEDUP_REJECTS: std::sync::atomic::AtomicUsize =
    std::sync::atomic::AtomicUsize::new(0);

/// Test-only counter that increments every time the relay GET driver
/// commits to forking + piping a streaming response to a REMOTE upstream
/// (the `Terminal::Streaming` forward branch). Incremented after the
/// loopback safety-net check and a successful `claim_or_wait`, before the
/// header send. NOT incremented on the loopback branch or the
/// `AlreadyClaimed` dedup branch. Used by `streaming_e2e` to prove a GET
/// genuinely relayed through a streaming hop rather than falling back to
/// store-and-forward. Mirrors `RELAY_PUT_STREAMING_DRIVER_CALL_COUNT`.
#[cfg(any(test, feature = "testing"))]
pub static RELAY_GET_STREAMING_FORWARD_COUNT: std::sync::atomic::AtomicUsize =
    std::sync::atomic::AtomicUsize::new(0);

/// Start a client-initiated GET, returning as soon as the task has been
/// spawned (mirrors legacy `request_get` timing).
///
/// The caller must have already registered a result waiter for
/// `client_tx` via `op_manager.ch_outbound.waiting_for_transaction_result`.
/// This function does NOT touch the waiter; it only drives the
/// ring/network side and publishes the terminal result to
/// `result_router_tx` keyed by `client_tx`.
#[allow(clippy::too_many_arguments)]
pub(crate) async fn start_client_get(
    op_manager: Arc<OpManager>,
    client_tx: Transaction,
    instance_id: ContractInstanceId,
    return_contract_code: bool,
    subscribe: bool,
    blocking_subscribe: bool,
    // `Some(client_id)` when the caller registered a client subscription up-front
    // (subscribe=true with a listener). The driver removes it on any terminal
    // that delivered no state (dead-end NotFound / infra error) so a phantom
    // client subscription does not linger until disconnect (step 10 §1e). `None`
    // when no client subscription was registered.
    client_sub_cleanup: Option<crate::client_events::ClientId>,
) -> Result<Transaction, OpError> {
    // Phase 7 egress self-block (#4300): refuse to originate a GET for
    // a contract this node has banned, BEFORE spawning the driver.
    // Mirrors the receive-side `GetMsg::Request` drop in node.rs (PR
    // #4299). The client gets a typed `ContractBanned` error.
    crate::operations::reject_if_contract_banned(&op_manager, &instance_id)?;

    // Test-only: count driver invocations so integration tests can
    // assert the driver was actually called (as opposed to
    // `client_events.rs`'s local-cache shortcut satisfying the GET).
    // Removed under #[cfg(not(any(test, feature = "testing")))].
    #[cfg(any(test, feature = "testing"))]
    DRIVER_CALL_COUNT.fetch_add(1, std::sync::atomic::Ordering::SeqCst);

    tracing::debug!(
        tx = %client_tx,
        contract = %instance_id,
        "get: spawning client-initiated task"
    );

    // Fire-and-forget spawn; same rationale as PUT 3a's `start_client_put`.
    // Failures are published to the client via `result_router_tx`, not
    // via this function's return value. Not registered with
    // `BackgroundTaskMonitor`: per-transaction task that terminates via
    // happy path, exhaustion, timeout, or infra error.
    //
    // Atomic admission gate + counter bump (closes the drain race
    // window). See `OpManager::admit_client_op` for the race
    // analysis. The guard is held for the lifetime of the spawned
    // driver; see `ClientOpGuard` rustdoc for the broader
    // shutdown-drain contract.
    let inflight_guard = match op_manager.admit_client_op() {
        Some(g) => g,
        None => return Err(OpError::NodeShuttingDown),
    };
    GlobalExecutor::spawn(async move {
        let _inflight_guard = inflight_guard;
        run_client_get(
            op_manager,
            client_tx,
            instance_id,
            return_contract_code,
            subscribe,
            blocking_subscribe,
            client_sub_cleanup,
        )
        .await;
    });

    Ok(client_tx)
}

async fn run_client_get(
    op_manager: Arc<OpManager>,
    client_tx: Transaction,
    instance_id: ContractInstanceId,
    return_contract_code: bool,
    subscribe: bool,
    blocking_subscribe: bool,
    client_sub_cleanup: Option<crate::client_events::ClientId>,
) {
    // RAII guard: clears the ops.get entry that the originator
    // loopback pushes via legacy process_message::GetMsg::Request
    // (#4066; full rationale in .claude/rules/operations.md).
    let _completion_guard = ClientGetCompletionGuard {
        op_manager: op_manager.clone(),
        client_tx,
    };

    let outcome = drive_client_get(
        op_manager.clone(),
        client_tx,
        instance_id,
        return_contract_code,
        subscribe,
        blocking_subscribe,
        client_sub_cleanup,
    )
    .await;
    deliver_outcome(&op_manager, client_tx, outcome);
}

/// RAII guard that calls `op_manager.completed(client_tx)` on drop,
/// covering every exit path including panics. Documented at the
/// guard installation site in `run_client_get`.
struct ClientGetCompletionGuard {
    op_manager: Arc<OpManager>,
    client_tx: Transaction,
}

impl Drop for ClientGetCompletionGuard {
    fn drop(&mut self) {
        self.op_manager.completed(self.client_tx);
    }
}

/// GET driver has exactly two outcomes, matching PUT 3a.
#[derive(Debug)]
enum DriverOutcome {
    /// The driver produced a `HostResult` that must be published via
    /// `result_router_tx`.
    Publish(HostResult),
    /// A genuine infrastructure failure escaped the driver loop.
    InfrastructureError(OpError),
}

async fn drive_client_get(
    op_manager: Arc<OpManager>,
    client_tx: Transaction,
    instance_id: ContractInstanceId,
    return_contract_code: bool,
    subscribe: bool,
    blocking_subscribe: bool,
    client_sub_cleanup: Option<crate::client_events::ClientId>,
) -> DriverOutcome {
    match drive_client_get_inner(
        &op_manager,
        client_tx,
        instance_id,
        return_contract_code,
        subscribe,
        blocking_subscribe,
        client_sub_cleanup,
    )
    .await
    {
        Ok(outcome) => outcome,
        Err(err) => {
            // Infra error: the GET delivered no state, so tear down any client
            // subscription registered up-front (step 10 §1e) before surfacing the
            // failure — same rationale as the Exhausted arm below.
            if let Some(client_id) = client_sub_cleanup {
                op_manager
                    .ring
                    .remove_client_subscription(&instance_id, client_id);
            }
            DriverOutcome::InfrastructureError(err)
        }
    }
}

async fn drive_client_get_inner(
    op_manager: &Arc<OpManager>,
    client_tx: Transaction,
    instance_id: ContractInstanceId,
    return_contract_code: bool,
    subscribe: bool,
    blocking_subscribe: bool,
    client_sub_cleanup: Option<crate::client_events::ClientId>,
) -> Result<DriverOutcome, OpError> {
    let htl = op_manager.ring.max_hops_to_live;

    // Pre-select initial target for the driver's retry state. Actual
    // routing is done by `process_message` on the loop-back; this is
    // just so `advance_to_next_peer` has a starting "tried" set.
    //
    // At the client-API boundary we only have an instance_id — the
    // full ContractKey (which includes the code hash) isn't known
    // until a terminal reply with `GetMsgResult::Found` arrives.
    // `k_closest_potentially_hosting` accepts either.
    let mut tried: Vec<std::net::SocketAddr> = Vec::new();
    if let Some(own_addr) = op_manager.ring.connection_manager.get_own_addr() {
        tried.push(own_addr);
    }
    let initial_target = op_manager
        .ring
        .k_closest_potentially_hosting(&instance_id, tried.as_slice(), 1)
        .into_iter()
        .next();
    let current_target = match initial_target {
        Some(peer) => {
            if let Some(addr) = peer.socket_addr() {
                tried.push(addr);
            }
            peer
        }
        // Bootstrap fallback (#4361): with an empty ring, attribute the
        // attempt to a configured gateway — the loopback relay's own
        // fallback forwards there, so the gateway IS the real first hop.
        // `own_location()` (below) remains only for the genuinely
        // isolated case (no ring, no gateways).
        None => match bootstrap_gateway_target(op_manager, |addr| tried.contains(&addr)) {
            Some((gw, addr)) => {
                tracing::info!(
                    %instance_id,
                    gateway = %addr,
                    "GET client: ring empty — initial target falls back to configured gateway"
                );
                tried.push(addr);
                gw
            }
            None => op_manager.ring.connection_manager.own_location(),
        },
    };

    let mut driver = GetRetryDriver {
        op_manager,
        instance_id,
        htl,
        tried,
        retries: 0,
        current_target,
        attempt_visited: VisitedPeers::new(&client_tx),
        request_sent_at: None,
        response_received_at: None,
    };

    let (loop_result, streaming_assembly) = drive_get_with_assembly_retry(
        op_manager,
        client_tx,
        "get",
        &mut driver,
        /* emit_route_failure_on_retry */ true,
    )
    .await;

    // `op_manager.completed(client_tx)` runs from the
    // `ClientGetCompletionGuard` in `run_client_get` AFTER this fn
    // returns (and the side-effect block below has run). Do not add
    // explicit `completed()` calls in the arms here — doing so would
    // race the originator-side side effects (`cache_contract_locally`,
    // `maybe_subscribe_child`) because once the tx is in `ops.completed`, any
    // `op_manager.push(...)` for that tx is silently dropped (see
    // `op_state_manager.rs::push` short-circuit on
    // `ops.completed.contains`).

    match loop_result {
        RetryLoopOutcome::Done(terminal) => {
            // Mirror the originator-side side effects that the legacy
            // `process_message` Response{Found} branch does
            // (`get.rs:2218–2450`): PutQuery the fetched state into
            // the local executor so re-GETs / local-cache checks / the
            // hosting LRU see it, announce hosting, and record the
            // access. Without this, a client-initiated GET succeeds
            // on the wire but the requesting node never stores the
            // contract — which broke `test_get_routing_coverage_low_htl`
            // and `test_auto_fetch_from_update_sender` on CI when the
            // bypass was first introduced.
            //
            // The bypass at `node.rs::handle_pure_network_message_v1`
            // intercepts the terminal reply BEFORE `process_message`
            // runs on the originator, so the driver is the only
            // place these side effects can happen.
            // Payload accounting for the router's `transfer_rate_estimator`
            // (router.rs:443) and `response_start_time_estimator`
            // (router.rs:405). Set on the InlineFound/Streaming success
            // paths; `Terminal::LocalCompletion` is the loopback
            // Request-echo with no network round-trip — both stay zero
            // AND we explicitly null the captured per-attempt timing
            // below (see `timed_outcome`) to avoid poisoning the
            // response-time model with sub-millisecond loopback latency
            // samples attributed to `driver.current_target`.
            let mut payload_size: usize = 0;
            let mut transfer_duration: std::time::Duration = std::time::Duration::ZERO;

            let reply_key = match &terminal {
                Terminal::InlineFound {
                    key,
                    state,
                    contract,
                    hop_count: _,
                } => {
                    // InlineFound delivers state + optional contract in
                    // the same envelope as the Response header, so the
                    // payload transfer time is effectively zero — the
                    // bytes arrived alongside the response. The router
                    // deliberately skips `transfer_rate_estimator`
                    // observations with `transfer_time.is_zero()`
                    // (router.rs:443) so InlineFound feeds response-time
                    // only; the legacy `GetStats` shape did the same
                    // thing for the same reason (no measurable transfer
                    // phase for an envelope-bundled payload).
                    payload_size =
                        state.size() + contract.as_ref().map(|c| c.data().len()).unwrap_or(0);
                    cache_contract_locally(
                        op_manager,
                        *key,
                        state.clone(),
                        contract.clone(),
                        true, // client driver: this node initiated the GET
                    )
                    .await;
                    *key
                }
                Terminal::Streaming {
                    key, total_size, ..
                } => {
                    // `total_size` is the wire-authoritative payload byte
                    // count from the `ResponseStreaming` header. Using it
                    // here (rather than re-querying the local store after
                    // `cache_contract_locally`) prevents a TOCTOU race
                    // where a concurrent UPDATE for the same contract
                    // would mis-attribute a different payload's size to
                    // this GET's transfer-rate observation, and side-steps
                    // a separate "store evicted before re-query"
                    // failure mode (the LRU has finite capacity).
                    payload_size = *total_size as usize;
                    // Stream assembly (with per-candidate retry, #4345)
                    // already ran inside `drive_get_with_assembly_retry`
                    // — consume its timing here. `None` means every
                    // assembly attempt failed: leave `transfer_duration`
                    // at ZERO so the router skips the rate sample at
                    // router.rs:443 (the response-time observation still
                    // goes through because a header DID arrive).
                    if let Some(duration) = streaming_assembly.transfer_duration {
                        transfer_duration = duration;
                    }
                    *key
                }
                Terminal::LocalCompletion => {
                    // Request-echo: the pre-send local-cache shortcut
                    // in `client_events.rs` already returned a cached
                    // state directly, so reaching here means
                    // `request_get`'s fallback cached it. The store
                    // already has the bytes; just resolve the key.
                    match lookup_stored_key(op_manager, &instance_id).await {
                        Some(k) => k,
                        None => synthetic_key(&instance_id),
                    }
                }
            };

            let host_result = build_host_response(
                op_manager,
                &instance_id,
                return_contract_code,
                streaming_assembly.error.as_deref(),
            )
            .await;

            // GET-auto-subscribe REMOVED (piece E, demand-driven hosting).
            // A successful GET with subscribe=false previously installed a
            // durable subscription here (AUTO_SUBSCRIBE_ON_GET). That is the
            // "GET-auto-subscribe" anti-pattern: it manufactures ongoing demand
            // no client requested and pins an advertised, self-renewing copy
            // regardless of real interest. Under the demand-driven model the GET
            // still hosts on the return path via `cache_contract_locally` above
            // (bounded + evicted by piece A's demand gauge — the whole
            // distinction from relay-caching), and a client that wants ongoing
            // freshness sets `subscribe=true`, handled by `maybe_subscribe_child`
            // below. do NOT re-add auto-subscribe here — see hosting-invariants
            // (invariants 1 & 2, anti-patterns table).

            // Emit routing event + telemetry — `report_result` (which
            // normally does both) doesn't run because the bypass
            // intercepted the Response. Without this, the router's
            // prediction model never receives GET success feedback.
            //
            // Prefer the timed `Success` variant so the router's
            // `response_start_time_estimator` and
            // `transfer_rate_estimator` receive observations. Fall back
            // to `SuccessUntimed` only when the reply was a loopback
            // Request-echo (`Terminal::LocalCompletion`) — there is no
            // wire round-trip to measure, so the captured per-attempt
            // timing reflects pure in-process latency and would poison
            // the response-time model with sub-millisecond samples
            // attributed to the selected target peer. The other paths
            // (InlineFound + Streaming) do measure real wire round-trip
            // time. Without timed Success on those paths the dashboard's
            // Response Time and Transfer Rate charts stay permanently
            // empty because those estimators are fed exclusively from
            // `RouteOutcome::Success`. The success flag tracks the
            // actual client-visible outcome (`host_result.is_ok()`), not
            // the wire-level reply — if the store re-query returned
            // nothing the client sees `OperationError` and telemetry
            // must agree.
            let contract_location = Location::from(&reply_key);
            let timed_outcome = if matches!(terminal, Terminal::LocalCompletion) {
                None
            } else {
                match (driver.request_sent_at, driver.response_received_at) {
                    (Some(sent), Some(received)) if received >= sent => {
                        Some(RouteOutcome::Success {
                            time_to_response_start: received.duration_since(sent),
                            payload_size,
                            payload_transfer_time: transfer_duration,
                        })
                    }
                    (Some(sent), Some(received)) => {
                        // `tokio::time::Instant` is monotonic, so this
                        // arm should be unreachable on real hardware.
                        // Logging instead of silently masking guards
                        // against a clock-source regression slipping in
                        // unnoticed.
                        tracing::warn!(
                            tx = %client_tx,
                            sent_at_elapsed_secs = sent.elapsed().as_secs_f64(),
                            received_at_elapsed_secs = received.elapsed().as_secs_f64(),
                            "get: received < sent on winning attempt; \
                             falling back to SuccessUntimed"
                        );
                        None
                    }
                    _ => None,
                }
            };
            let route_event = RouteEvent {
                peer: driver.current_target.clone(),
                contract_location,
                outcome: if host_result.is_ok() {
                    timed_outcome.unwrap_or(RouteOutcome::SuccessUntimed)
                } else {
                    RouteOutcome::Failure
                },
                op_type: Some(crate::node::network_status::OpType::Get),
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
                crate::node::network_status::OpType::Get,
                host_result.is_ok(),
            );

            // Explicit-subscribe hand-off. Mirrors PUT 3a's
            // `maybe_subscribe_child` — subscribe is never handled in
            // the terminal-result construction to avoid double-subscribe
            // (commit 494a3c69). This only runs when the client set
            // `subscribe=true`; it is now the ONLY subscribe path on GET
            // (piece E removed the AUTO_SUBSCRIBE_ON_GET fallback above —
            // a subscribe=false GET hosts under the demand gauge but does
            // not subscribe).
            maybe_subscribe_child(
                op_manager,
                client_tx,
                reply_key,
                subscribe,
                blocking_subscribe,
            )
            .await;

            Ok(DriverOutcome::Publish(host_result))
        }
        RetryLoopOutcome::Exhausted(cause) => {
            // Exhaustion after retry loop means every peer either responded
            // NotFound or the operation could not be forwarded at all
            // (e.g., isolated gateway with zero ring connections). Surface
            // as `ContractResponse::NotFound` so client integrations
            // (`fdev`, `apps/freenet-ping`, integration tests) can
            // distinguish "contract genuinely absent" from "operation
            // failed". Log the cause — silently discarding it made the
            // empty-ring bootstrap failure invisible (#4361).
            tracing::info!(
                tx = %client_tx,
                %instance_id,
                ring_connections = op_manager.ring.connection_manager.connection_count(),
                %cause,
                "GET client: retry loop exhausted — returning NotFound to client"
            );
            // Dead-end GET: no state was delivered, so tear down any client
            // subscription registered up-front (step 10 §1e) — otherwise it
            // lingers as a phantom (contract_in_use && !state) until the client
            // disconnects. Safe no-op when nothing was registered.
            if let Some(client_id) = client_sub_cleanup {
                op_manager
                    .ring
                    .remove_client_subscription(&instance_id, client_id);
            }
            Ok(DriverOutcome::Publish(Ok(HostResponse::ContractResponse(
                freenet_stdlib::client_api::ContractResponse::NotFound { instance_id },
            ))))
        }
        RetryLoopOutcome::Unexpected => Err(OpError::UnexpectedOpState),
        RetryLoopOutcome::InfraError(err) => Err(err),
    }
}

// --- Retry-driver state and classification ---

struct GetRetryDriver<'a> {
    op_manager: &'a OpManager,
    instance_id: ContractInstanceId,
    htl: usize,
    tried: Vec<std::net::SocketAddr>,
    retries: usize,
    current_target: PeerKeyLocation,
    attempt_visited: VisitedPeers,
    /// Wall-clock at the start of the most recent `build_request` call.
    /// Paired with `response_received_at` (captured in `classify`) to
    /// feed `time_to_response_start` into the router's isotonic
    /// regression. The winning attempt's values are used. `tokio::time::Instant`
    /// is auto-paused in test builds with `start_paused(true)`, so
    /// simulation tests see virtual elapsed time rather than wall-clock —
    /// matches the rest of `crates/core/`'s timing primitives.
    request_sent_at: Option<tokio::time::Instant>,
    /// Wall-clock when `classify` last returned a `Terminal` outcome.
    /// Reset to `None` until a terminal arrives; non-terminal classify
    /// outcomes (Retry / Unexpected) leave it untouched and the next
    /// `build_request` overwrites `request_sent_at`.
    response_received_at: Option<tokio::time::Instant>,
}

/// Terminal value for the GET driver.
///
/// Carries the bytes needed to (a) store the contract in the local
/// executor via `PutQuery` — matching the side effect that the legacy
/// `process_message` Response{Found} branch performs at
/// `get.rs:2329` — and (b) build the client-facing
/// `HostResponse::GetResponse`.
#[derive(Debug)]
enum Terminal {
    /// Inline Response{Found}: state and optional contract arrived in
    /// the reply envelope; driver stores them locally via PutQuery.
    InlineFound {
        key: ContractKey,
        state: WrappedState,
        contract: Option<ContractContainer>,
        /// Forward-path hop count from the originating Request to the
        /// storer, as carried on the wire Response. Preserved across the
        /// relay bubble-up chain (relays do NOT increment on the return
        /// path). Used by tracing to populate `GetSuccess.hop_count`.
        hop_count: usize,
    },
    /// ResponseStreaming: the envelope references a stream_id whose
    /// bytes arrive separately via the orphan stream registry. The
    /// driver claims the stream, awaits assembly, and caches the
    /// assembled state + contract locally — mirroring what the
    /// legacy `process_message` streaming branch does at
    /// `get.rs:2721-3196`. `total_size` carries the wire-authoritative
    /// payload byte count from the streaming header so the driver
    /// doesn't have to re-query the store post-assembly (which would
    /// race a concurrent UPDATE for the same contract).
    Streaming {
        key: ContractKey,
        stream_id: StreamId,
        includes_contract: bool,
        total_size: u64,
    },
    /// Request-echo from `forward_pending_op_result_if_completed` —
    /// state was already in the local store (via the pre-send
    /// local-cache shortcut), so no new PutQuery is needed. The
    /// driver just resolves the key from the store.
    LocalCompletion,
}

/// Classify a reply into a driver outcome. Extracted from the
/// `RetryDriver::classify` impl so it's reachable from unit tests.
fn classify(reply: NetMessage) -> AttemptOutcome<Terminal> {
    match reply {
        NetMessage::V1(NetMessageV1::Get(GetMsg::Response {
            result:
                GetMsgResult::Found {
                    key,
                    value:
                        StoreResponse {
                            state: Some(state),
                            contract,
                        },
                },
            hop_count,
            ..
        })) => AttemptOutcome::Terminal(Terminal::InlineFound {
            key,
            state,
            contract,
            hop_count,
        }),
        NetMessage::V1(NetMessageV1::Get(GetMsg::Response {
            result: GetMsgResult::Found { value, .. },
            ..
        })) => {
            tracing::warn!(?value, "get: Response{{Found}} arrived without state");
            AttemptOutcome::Unexpected
        }
        NetMessage::V1(NetMessageV1::Get(GetMsg::Response {
            result: GetMsgResult::NotFound,
            ..
        })) => AttemptOutcome::Retry,
        NetMessage::V1(NetMessageV1::Get(GetMsg::ResponseStreaming {
            key,
            stream_id,
            includes_contract,
            total_size,
            ..
        })) => AttemptOutcome::Terminal(Terminal::Streaming {
            key,
            stream_id,
            includes_contract,
            total_size,
        }),
        NetMessage::V1(NetMessageV1::Get(GetMsg::Request { .. })) => {
            AttemptOutcome::Terminal(Terminal::LocalCompletion)
        }
        // Explicit non-terminal `GetMsg` variants. These should never
        // reach the driver — the bypass at
        // `node.rs::handle_pure_network_message_v1` gates forwarding
        // on terminal variants only — so their arrival here
        // indicates a bug in the bypass gate.
        NetMessage::V1(NetMessageV1::Get(
            GetMsg::ForwardingAck { .. } | GetMsg::ResponseStreamingAck { .. },
        )) => AttemptOutcome::Unexpected,
        // Non-GET NetMessage variants (or any future `GetMsg` variant
        // added without updating this match) fall through to
        // Unexpected. If a new GetMsg variant is added, this arm must
        // be audited — this driver has explicit handling for every
        // variant above, and the bypass filter in node.rs must be
        // extended in lockstep.
        _ => AttemptOutcome::Unexpected,
    }
}

impl RetryDriver for GetRetryDriver<'_> {
    type Terminal = Terminal;

    fn new_attempt_tx(&mut self) -> Transaction {
        let tx = Transaction::new::<GetMsg>();
        self.attempt_visited = VisitedPeers::new(&tx);
        // Bootstrap fallback failover (#4361): the wire target for a
        // client GET is re-picked per attempt by the originator-loopback
        // relay, whose selection state is fresh each attempt. Without
        // carrying the client's tried set into the new attempt's visited
        // bloom, every retry would re-select the same first configured
        // gateway — the client-side advance's "next gateway" pick would
        // never reach the wire and multi-gateway failover would be
        // bookkeeping only. Carry `tried` minus the current target (which
        // IS this attempt's intended destination) so the relay's fallback
        // skips gateways that already failed and converges on the same
        // gateway the client driver selected — keeping stream claims and
        // route telemetry (both attributed via `current_target`) aligned
        // with the actual wire hop.
        //
        // The carried bloom travels the attempt's entire forward path, so
        // a failed gateway is excluded at every hop of that attempt, not
        // only at the loopback relay — bounded (empty-ring originators,
        // <= MAX_RETRIES attempts) and re-keyed each retry.
        //
        // Gated on the empty-ring case so normal-path retry routing
        // semantics are unchanged. The gate re-reads `connection_count()`
        // and can race ring promotion between attempts; both directions
        // degrade to a single wasted or spuriously-failed attempt (never
        // a loop or hang) — see the #4364 review for the trace.
        if self.op_manager.ring.connection_manager.connection_count() == 0 {
            carry_tried_into_visited(
                &mut self.attempt_visited,
                &self.tried,
                self.current_target.socket_addr(),
            );
        }
        tx
    }

    fn build_request(&mut self, attempt_tx: Transaction) -> NetMessage {
        // Capture send time per attempt. The winning attempt's timestamp
        // is read after the retry loop returns to compute
        // `time_to_response_start` for routing-prediction telemetry.
        self.request_sent_at = Some(tokio::time::Instant::now());
        NetMessage::from(GetMsg::Request {
            id: attempt_tx,
            instance_id: self.instance_id,
            fetch_contract: true,
            htl: self.htl,
            visited: self.attempt_visited.clone(),
            subscribe: false,
        })
    }

    fn classify(&mut self, reply: NetMessage) -> AttemptOutcome<Terminal> {
        let outcome = classify(reply);
        if matches!(outcome, AttemptOutcome::Terminal(_)) {
            // Only capture on Terminal so non-terminal Retry/Unexpected
            // outcomes leave the field as None until a terminal arrives.
            self.response_received_at = Some(tokio::time::Instant::now());
        }
        outcome
    }

    fn advance(&mut self) -> AdvanceOutcome {
        match advance_to_next_peer(
            self.op_manager,
            &self.instance_id,
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

// --- Assembly-retry wrapper (#4345) ---

/// Outcome metadata for the streaming-assembly step of a GET.
#[derive(Default)]
struct AssemblyOutcome {
    /// Wall-clock duration of the successful claim + assemble +
    /// deserialize + local cache write, fed to the router's
    /// transfer-rate estimator. `None` when the terminal was not
    /// `Streaming` or when every assembly attempt failed. The
    /// composite measurement matches what the legacy
    /// `GetStats::record_transfer_end` captured at `get.rs:1542`.
    transfer_duration: Option<std::time::Duration>,
    /// Last assembly failure cause when assembly never succeeded.
    /// Threaded into the synthesized client error so the failure is
    /// diagnosable instead of the generic store-lookup message.
    error: Option<String>,
}

/// Drive the shared GET retry loop, treating stream-assembly failure
/// as a retryable attempt failure (#4345).
///
/// The shared loop classifies the `ResponseStreaming` *header* as
/// terminal (`op_ctx.rs` pins the Terminal arm to return
/// synchronously), so stream assembly necessarily runs after the loop
/// returns. Before this wrapper existed, an assembly failure —
/// fragments lost on a lossy path, the sender aborting on cwnd-wait
/// timeout, a relay's pipe dying after the header was forwarded —
/// burned the whole GET: the driver logged a WARN, fell through to
/// the store re-query, and synthesized a client error with the
/// `MAX_RETRIES` budget untouched. One transport hiccup failed the op
/// even though other candidates could serve it. The relay driver
/// already treats header-without-stream as a retryable routing
/// failure (`drive_relay_get_inner`'s claim-failure `continue`); this
/// gives the originator the same semantics.
///
/// On assembly failure the wrapper advances the driver to the next
/// candidate (consuming the shared retry budget) and re-enters the
/// loop with a fresh attempt transaction — reusing the previous tx
/// would collide with the relay dedup gates (`active_relay_get_txs`)
/// while the failed attempt's relay chain is still draining.
///
/// Once a streaming header has been seen, exhaustion can never surface
/// as `Exhausted` (which callers map to a false `NotFound` for a
/// contract that provably exists). Both exhaustion shapes are covered:
/// the assembly-time `advance()` exhaustion keeps the original
/// `Done(Streaming)` outcome, and a wire exhaustion on a re-entered
/// loop (every remaining candidate NotFound / timeout) is converted
/// back to the remembered `Done(Streaming)` header. Either way the
/// client sees an operation error carrying the assembly cause.
async fn drive_get_with_assembly_retry(
    op_manager: &OpManager,
    first_tx: Transaction,
    op_label: &str,
    driver: &mut GetRetryDriver<'_>,
    emit_route_failure_on_retry: bool,
) -> (RetryLoopOutcome<Terminal>, AssemblyOutcome) {
    let mut attempt_tx = first_tx;
    let mut assembly = AssemblyOutcome::default();
    // Streaming-header fields remembered across an assembly-failure
    // retry. A header proves the contract exists, so if the re-entered
    // loop then exhausts on the wire (every remaining candidate
    // NotFound / timeout), the exhaustion must NOT surface as a false
    // `NotFound` to the client — convert it back to the remembered
    // `Done(Streaming)` so the caller synthesizes an operation error
    // carrying the assembly cause. Pre-#4345 this state was
    // unreachable (assembly failure never re-entered the loop).
    let mut failed_header: Option<(ContractKey, StreamId, bool, u64)> = None;

    let outcome = loop {
        let result = drive_retry_loop(op_manager, attempt_tx, op_label, driver).await;

        // Only a streaming terminal has a post-loop assembly step;
        // everything else passes through unchanged. Match by reference
        // (the fields are all Copy) so `result` stays whole — every
        // give-up exit below is then `break result`, returning the
        // original `Done(Streaming)` outcome without rebuilding it.
        let (key, stream_id, includes_contract, total_size) = match &result {
            RetryLoopOutcome::Done(Terminal::Streaming {
                key,
                stream_id,
                includes_contract,
                total_size,
            }) => (*key, *stream_id, *includes_contract, *total_size),
            RetryLoopOutcome::Done(Terminal::InlineFound { .. } | Terminal::LocalCompletion) => {
                // A non-streaming terminal delivered the state inline,
                // so any earlier assembly failure is moot — clear the
                // remembered error or an unrelated store-lookup miss
                // (eviction race) would be mislabeled as an assembly
                // failure by the caller's error synthesis.
                assembly.error = None;
                break result;
            }
            RetryLoopOutcome::Exhausted(cause) => {
                if let Some((key, stream_id, includes_contract, total_size)) = failed_header {
                    let prior = assembly.error.take().unwrap_or_default();
                    assembly.error = Some(format!(
                        "{prior}; retries after assembly failure exhausted: {cause}"
                    ));
                    // The remembered header's `response_received_at`
                    // belongs to the FIRST attempt, while
                    // `request_sent_at` was overwritten by the last
                    // (exhausted) attempt — the pair is incoherent, and
                    // leaving it set makes the caller's received<sent
                    // guard fire its "clock-source regression" WARN on
                    // every conversion. Clear both so the caller's
                    // timed-outcome capture falls through to None.
                    driver.request_sent_at = None;
                    driver.response_received_at = None;
                    break RetryLoopOutcome::Done(Terminal::Streaming {
                        key,
                        stream_id,
                        includes_contract,
                        total_size,
                    });
                }
                break result;
            }
            RetryLoopOutcome::Unexpected | RetryLoopOutcome::InfraError(_) => break result,
        };

        // Uses `current_target` as the sender address — accurate for
        // the single-hop response case where the responder equals the
        // selected target; relays pipe the stream hop-by-hop so the
        // fragments arrive from the adjacent hop either way.
        let Some(peer_addr) = driver.current_target.socket_addr() else {
            tracing::warn!(
                %key,
                "get: current_target has no socket_addr; \
                 cannot claim orphan stream"
            );
            assembly.error = Some(
                "selected target has no socket address; \
                 cannot claim the response stream"
                    .to_string(),
            );
            break result;
        };

        let stream_start = tokio::time::Instant::now();
        match assemble_and_cache_stream(op_manager, peer_addr, stream_id, key, includes_contract)
            .await
        {
            Ok(()) => {
                assembly.transfer_duration = Some(stream_start.elapsed());
                assembly.error = None;
                break result;
            }
            Err(e) => {
                // Capture the failing peer BEFORE advance() replaces
                // `current_target` — the routing penalty must land on
                // the candidate whose header never became a stream.
                let failed_target = driver.current_target.clone();
                match driver.advance() {
                    AdvanceOutcome::Next => {
                        tracing::warn!(
                            %key,
                            error = %e,
                            retries = driver.retries,
                            "get: stream assembly failed; \
                             retrying against next candidate (#4345)"
                        );
                        // Penalize the failed candidate so the router
                        // learns (mirrors the relay driver's
                        // claim-failure handling). Only on the
                        // advancing path: when the budget is
                        // exhausted, the caller's final route event
                        // (driven by the failed host_result) already
                        // records the Failure for the last target —
                        // emitting here too would double-count it.
                        if emit_route_failure_on_retry {
                            emit_get_route_failure(op_manager, attempt_tx, &failed_target, &key)
                                .await;
                        }
                        #[cfg(any(test, feature = "testing"))]
                        assembly_fault_injection::ASSEMBLY_RETRY_COUNT
                            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                        assembly.error = Some(e);
                        // Remember the header so a later wire
                        // exhaustion converts back to Done(Streaming)
                        // instead of surfacing a false NotFound.
                        failed_header = Some((key, stream_id, includes_contract, total_size));
                        attempt_tx = driver.new_attempt_tx();
                        continue;
                    }
                    AdvanceOutcome::Exhausted => {
                        tracing::warn!(
                            %key,
                            error = %e,
                            "get: stream assembly failed and retry budget \
                             exhausted — state will not be cached locally"
                        );
                        assembly.error = Some(e);
                        break result;
                    }
                }
            }
        }
    };

    (outcome, assembly)
}

/// Emit a routing-failure observation for a candidate whose streaming
/// header never became a completed stream. Mirrors the route-event
/// emission in `drive_client_get_inner`'s final outcome block.
async fn emit_get_route_failure(
    op_manager: &OpManager,
    tx: Transaction,
    peer: &PeerKeyLocation,
    key: &ContractKey,
) {
    let route_event = RouteEvent {
        peer: peer.clone(),
        contract_location: Location::from(key),
        outcome: RouteOutcome::Failure,
        op_type: Some(crate::node::network_status::OpType::Get),
    };
    if let Some(log_event) =
        crate::tracing::NetEventLog::route_event(&tx, &op_manager.ring, &route_event)
    {
        op_manager
            .ring
            .register_events(either::Either::Left(log_event))
            .await;
    }
    op_manager.ring.routing_finished(route_event);
}

/// Test-only fault injection for `assemble_and_cache_stream` (#4345).
///
/// Keyed by `ContractKey` so concurrently-running tests in the same
/// process cannot consume each other's injection budget. A test arms
/// `n` failures for its own (unique) contract key; the first `n`
/// assembly attempts for that key fail with a synthetic error,
/// exercising the driver's assembly-retry path deterministically.
#[cfg(any(test, feature = "testing"))]
pub mod assembly_fault_injection {
    use freenet_stdlib::prelude::ContractKey;
    use std::collections::HashMap;
    use std::sync::atomic::AtomicUsize;
    use std::sync::{Mutex, OnceLock};

    /// Lifetime count of assembly-failure retries actually taken by
    /// GET drivers (incremented when the driver advances to the next
    /// candidate after an assembly failure). Tests snapshot
    /// before/after to prove the retry path genuinely fired.
    pub static ASSEMBLY_RETRY_COUNT: AtomicUsize = AtomicUsize::new(0);

    fn budgets() -> &'static Mutex<HashMap<ContractKey, usize>> {
        static BUDGETS: OnceLock<Mutex<HashMap<ContractKey, usize>>> = OnceLock::new();
        BUDGETS.get_or_init(|| Mutex::new(HashMap::new()))
    }

    /// Arm `n` injected assembly failures for `key`.
    pub fn inject_failures(key: ContractKey, n: usize) {
        budgets().lock().expect("poisoned").insert(key, n);
    }

    /// Consume one injected failure for `key`, if armed.
    pub(crate) fn consume(key: &ContractKey) -> bool {
        let mut map = budgets().lock().expect("poisoned");
        match map.get_mut(key) {
            Some(n) if *n > 0 => {
                *n -= 1;
                if *n == 0 {
                    map.remove(key);
                }
                true
            }
            _ => false,
        }
    }
}

// --- Host-response construction ---

/// Query the local contract store for `(state, contract)` and package
/// a client-facing `HostResponse::ContractResponse::GetResponse`.
///
/// If the local store doesn't have the state (which should not happen
/// on the happy path — `process_message` stores before the terminal
/// reply fires), synthesize an operation error matching the shape
/// `to_host_result` produces on NotFound.
///
/// `assembly_error` carries the last stream-assembly failure cause when
/// the streaming path exhausted its retry budget without ever caching
/// the state (#4345). It makes the synthesized client error diagnostic
/// — "stream assembly failed: …" — instead of the generic store-lookup
/// message that conflates assembly failures with eviction races.
async fn build_host_response(
    op_manager: &OpManager,
    instance_id: &ContractInstanceId,
    return_contract_code: bool,
    assembly_error: Option<&str>,
) -> HostResult {
    let lookup = op_manager
        .notify_contract_handler(ContractHandlerEvent::GetQuery {
            instance_id: *instance_id,
            return_contract_code,
        })
        .await;

    match lookup {
        Ok(ContractHandlerEvent::GetResponse {
            key: Some(resolved_key),
            response:
                Ok(StoreResponse {
                    state: Some(state),
                    contract,
                }),
        }) => {
            // Strip contract code if client didn't ask for it. The
            // node always pulls WASM for local caching/validation,
            // but the client-facing payload obeys their flag.
            let client_contract = if return_contract_code { contract } else { None };
            Ok(HostResponse::ContractResponse(
                ContractResponse::GetResponse {
                    key: resolved_key,
                    contract: client_contract,
                    state,
                },
            ))
        }
        _ => {
            let cause = synthesized_get_error_cause(instance_id, assembly_error);
            tracing::warn!(
                contract = %instance_id,
                %cause,
                "get: terminal reply classified success but local \
                 store lookup returned no state; synthesizing client error"
            );
            Err(ErrorKind::OperationError {
                cause: cause.into(),
            }
            .into())
        }
    }
}

/// Fallback `ContractKey` for telemetry when we have neither a
/// remote-reply key nor a store-resolved key. Zero code-hash is the
/// documented sentinel — routing telemetry only needs a `Location`
/// derived from the instance_id, which is preserved here.
fn synthetic_key(instance_id: &ContractInstanceId) -> ContractKey {
    ContractKey::from_id_and_code(*instance_id, CodeHash::new([0u8; 32]))
}

/// Store the fetched contract state in the local executor and run
/// the originator-side hosting side effects. Mirrors the legacy
/// `process_message` Response{Found} branch at `get.rs:2218-2450`.
///
/// Returns `true` when this node holds the contract's current state after the
/// call (`state_matches || put_persisted`) — D-CACHE-RET (step 10 §1a). Relay
/// callers gate downstream-subscriber registration on this so a peer registers
/// demand only after it actually holds state; registering after state makes the
/// phantom (`contract_in_use && !contract_state_present`) unrepresentable.
///
/// 1. **Idempotency short-circuit (issue #2018)** — re-query the
///    local store first. If the existing bytes match the incoming
///    state, skip `PutQuery` entirely so contracts that enforce
///    identical-state rejection in `update_state()` aren't invoked
///    redundantly.
/// 2. **Unconditional hosting refresh** — `record_get_access` and
///    interest-manager eviction cleanup run for BOTH the
///    state-matches short-circuit AND the PutQuery-failed error paths.
///    Legacy behaviour at `get.rs:2420-2435` continues these side
///    effects on error so re-GETs keep refreshing the hosting LRU/TTL
///    even when the executor rejected the write.
/// 3. **`mark_local_client_access` gated on `is_client_requester`** —
///    the sticky flag that moves the contract into the
///    `contracts_needing_renewal()` set (see `ring/hosting.rs:889`) must
///    only fire when THIS node is the client-originating requester.
///    Relay peers that merely cache a forwarded Found MUST NOT set it,
///    or they permanently pay subscription-renewal cost for contracts
///    no client on this node ever asked for. Legacy mirror:
///    `get.rs:2260-2262, :2353-2355, :3056-3058`
///    all gate the call on `is_original_requester =
///    upstream_addr.is_none()`.
/// 4. **Newly-hosted announcement** — only runs when the local
///    store actually transitioned from no-state to has-state
///    (i.e., `access_result.is_new && put_persisted`). NOT gated on
///    `is_client_requester`; legacy announces on any first-time relay
///    cache too (`get.rs:2278, 2370`).
// NAMING LANDMINE: "cache"/"store" here means HOSTING, not a lesser cache tier. A
// peer stores a contract because a GET routed through it, and a routed GET is a
// demand signal, so the peer HOSTS the contract (WASM+state, kept fresh in the
// update mesh) until LRU eviction. There is no cache tier. Slated to be renamed
// hosting-* AFTER 0.2.94. See .claude/rules/hosting-invariants.md + epic #4642.
async fn cache_contract_locally(
    op_manager: &OpManager,
    key: ContractKey,
    state: WrappedState,
    contract: Option<ContractContainer>,
    is_client_requester: bool,
) -> bool {
    // EXPERIMENT-ONLY (findability_probe): suppress RELAY-path GET-return caching
    // so the seeded copy cannot self-scatter along successful GET return paths
    // (the confound that invalidated an earlier lone-holder run). The client
    // requester's own final store (is_client_requester=true) is KEPT so find-rate
    // stays measurable — it lands at the requester (a GET origin, never on another
    // requester's key-ward path) so it cannot confound routing. NOT for ship.
    #[cfg(any(test, feature = "testing"))]
    if !is_client_requester && crate::operations::findability_probe::scatter_disabled() {
        // Experiment suppressed the relay-path store, so no state was cached
        // here — report not-present so a caller gating registration on the
        // return value does not register a downstream against uncached state.
        return false;
    }
    let state_size = state.size() as u64;

    // (1) Idempotency short-circuit: re-query the local store FIRST.
    // Comparing bytes against the incoming state avoids re-invoking
    // `update_state()` in contracts that reject identical updates
    // (regression guard for issue #2018 / PR #2018).
    let local_state = op_manager
        .notify_contract_handler(ContractHandlerEvent::GetQuery {
            instance_id: *key.id(),
            return_contract_code: false,
        })
        .await;
    let state_matches = matches!(
        &local_state,
        Ok(ContractHandlerEvent::GetResponse {
            response: Ok(StoreResponse {
                state: Some(local),
                ..
            }),
            ..
        }) if local.as_ref() == state.as_ref(),
    );

    // (2) Decide whether PutQuery must run. If local state already
    // matches or we lack the contract code (issue #2306), skip the
    // PutQuery but STILL run hosting side effects below so LRU/TTL
    // refresh does not depend on the write path.
    let put_persisted = if state_matches {
        tracing::debug!(
            %key,
            "get: local state matches, skipping redundant PutQuery"
        );
        false
    } else if let Some(contract_code) = contract {
        match op_manager
            .notify_contract_handler(ContractHandlerEvent::PutQuery {
                key,
                state,
                related_contracts: RelatedContracts::default(),
                contract: Some(contract_code),
            })
            .await
        {
            Ok(ContractHandlerEvent::PutResponse {
                new_value: Ok(_), ..
            }) => true,
            Ok(ContractHandlerEvent::PutResponse {
                new_value: Err(err),
                ..
            }) => {
                tracing::warn!(
                    %key,
                    %err,
                    "get: PutQuery rejected by executor"
                );
                false
            }
            Ok(other) => {
                tracing::warn!(
                    %key,
                    ?other,
                    "get: PutQuery returned unexpected event"
                );
                false
            }
            Err(err) => {
                tracing::warn!(
                    %key,
                    %err,
                    "get: PutQuery failed"
                );
                false
            }
        }
    } else {
        // No contract code + state differs — we can't cache (issue #2306).
        // Still refresh hosting side effects below so re-GET TTL bookkeeping
        // isn't gated on the write path.
        tracing::debug!(
            %key,
            "get: skipping local cache — contract code missing"
        );
        false
    };

    // (3) Hosting side effects ALWAYS run (state_matches, put_persisted,
    // or put failed). This mirrors the legacy invariant that a
    // successful wire-level GET must refresh the hosting LRU/TTL
    // regardless of what the local executor did with the state.
    let access_result = op_manager.ring.record_get_access(key, state_size);
    // Sticky flag gating subscription renewal; only the client-originating
    // node sets it. Relays that pass through a Found MUST NOT taint their
    // own hosting cache with another node's client access. Legacy mirror:
    // get.rs:2260-2262 / :2353-2355 / :3056-3058 all guard on
    // `is_original_requester = upstream_addr.is_none()`.
    if is_client_requester {
        op_manager.ring.mark_local_client_access(&key);
    }

    // Sync the InterestManager for any subscribed contract the subscriber-primary
    // eviction shed + tore down (#4642 invariant 3). Run BEFORE the
    // `unregister_local_hosting` loop below so that call observes the now-zeroed
    // subscriber counts and reports full interest loss (→ retraction via
    // `removed_contracts`). Without this, ghost `interested_peers` /
    // `peer_contracts` / `local_client_count` entries survive and mis-target
    // UPDATE broadcasts / inflate upstream interest counts (PR #4734 Fix 1).
    for teardown in &access_result.evicted_in_use_teardown {
        op_manager.interest_manager.remove_evicted_in_use(
            &teardown.key,
            &teardown.downstream_peers,
            teardown.local_client_count,
        );
    }

    let mut removed_contracts = Vec::new();
    for (evicted_key, expected_generation) in &access_result.evicted {
        if op_manager
            .interest_manager
            .unregister_local_hosting(evicted_key)
        {
            removed_contracts.push(*evicted_key);
        }
        // Reclaim on-disk storage for the evicted contract so the hosting
        // budget is a real disk bound (subscription- and generation-gated
        // inside the helper).
        crate::operations::reclaim_evicted_contract(op_manager, *evicted_key, *expected_generation);
    }

    // Reconcile-controller SHADOW comparison (keystone step-2, #4642),
    // HOST-FORMATION site (GET cache path). About to (conditionally) announce
    // hosting; does the controller agree? Focused on `Announce`. Actual =
    // `{Announce}` iff production announces a NOT-yet-advertised host this event
    // (`is_new && put_persisted` AND not already advertised — the announce is
    // idempotent), else `{}`. Built BEFORE the announce so `is_advertised`
    // reflects the pre-announce state. DRIVES NOTHING.
    {
        let will_announce = access_result.is_new && put_persisted;
        op_manager.record_reconcile_shadow_event(
            crate::node::network_status::ReconcileShadowSite::HostFormation,
            &key,
            &[crate::ring::reconcile::Action::Announce],
            |inputs| {
                if will_announce && !inputs.is_advertised {
                    vec![crate::ring::reconcile::Action::Announce]
                } else {
                    Vec::new()
                }
            },
        );
    }

    // (4) Newly-hosted announcement gates on BOTH first-time access
    // AND the fact that we actually persisted new state. Without
    // persistence there's nothing to announce hosting for.
    if access_result.is_new && put_persisted {
        crate::operations::announce_contract_hosted(op_manager, &key).await;
        // Directed-subscribe placement (#4404): best-effort nudge the node to
        // consider migrating this freshly-hosted contract toward a closer
        // neighbor. Dropped silently if the event channel is full — the next
        // hosting/peer event re-triggers consideration.
        if let Err(err) =
            op_manager.try_notify_node_event(NodeEvent::ConsiderContractMigration { key })
        {
            tracing::debug!(%key, %err, "ConsiderContractMigration emit dropped (GET)");
        }
        let became_interested = op_manager.interest_manager.register_local_hosting(&key);
        let added = if became_interested { vec![key] } else { vec![] };
        if !added.is_empty() || !removed_contracts.is_empty() {
            crate::operations::broadcast_change_interests(op_manager, added, removed_contracts)
                .await;
        }
    } else if !removed_contracts.is_empty() {
        crate::operations::broadcast_change_interests(op_manager, vec![], removed_contracts).await;
    }

    // D-CACHE-RET: current state is held locally iff it already matched or we
    // just persisted it. Relay callers gate register_downstream_subscriber on
    // this (register-after-state, step 10 §1a).
    state_matches || put_persisted
}

/// Claim an orphan stream, await assembly, deserialize the payload,
/// and cache the contract state locally.
///
/// Mirrors the originator-side streaming branch of the legacy
/// `process_message` at `get.rs:2721-3196`. The driver is the only
/// place this can run for driver GETs because the bypass at
/// `node.rs::handle_pure_network_message_v1` forwards the
/// `ResponseStreaming` envelope to the driver before
/// `handle_op_request` — `process_message` never executes on the
/// originator for driver ops (`load_or_init` would return
/// `OpNotPresent`).
///
/// `peer_addr` is the sender's transport address — we use
/// `driver.current_target.socket_addr()`. Multi-hop streamed GET IS now
/// supported: relays fork+pipe the stream and re-key it with a fresh
/// outbound `stream_id` per hop (#4307), so the originator always claims
/// the stream keyed by its `current_target` (the immediate next hop),
/// regardless of how many relays the response traversed. See #3883.
async fn assemble_and_cache_stream(
    op_manager: &OpManager,
    peer_addr: std::net::SocketAddr,
    stream_id: StreamId,
    expected_key: ContractKey,
    includes_contract: bool,
) -> Result<(), String> {
    // Test-only deterministic fault injection (#4345). Returning before
    // the claim mirrors the production failure (the inbound stream is
    // left orphaned for GC), so the retry path is exercised end-to-end.
    #[cfg(any(test, feature = "testing"))]
    if assembly_fault_injection::consume(&expected_key) {
        return Err("injected stream assembly failure (assembly_fault_injection test hook)".into());
    }

    let handle = match op_manager
        .orphan_stream_registry()
        .claim_or_wait(peer_addr, stream_id, STREAM_CLAIM_TIMEOUT)
        .await
    {
        Ok(h) => h,
        Err(OrphanStreamError::AlreadyClaimed) => {
            tracing::debug!(
                %peer_addr,
                %stream_id,
                "stream already claimed (dedup)"
            );
            return Ok(());
        }
        Err(e) => return Err(format!("claim_or_wait: {e}")),
    };

    let bytes = handle
        .assemble()
        .await
        .map_err(|e| format!("stream assembly: {e}"))?;

    let payload: GetStreamingPayload =
        bincode::deserialize(&bytes).map_err(|e| format!("deserialize: {e}"))?;

    if payload.key != expected_key {
        return Err(format!(
            "stream key mismatch: expected {expected_key}, got {}",
            payload.key
        ));
    }

    let Some(state) = payload.value.state else {
        return Err("stream payload has no state".into());
    };

    let contract = if includes_contract {
        payload.value.contract
    } else {
        None
    };

    // This helper runs on the client-driver (originator) side. The relay
    // driver also claims streams now (the `Terminal::Streaming` arm, #4307),
    // but it uses its own inline assemble+cache with `local_client_access =
    // false`; this path is the originator, which IS the client requester, so
    // the sticky `local_client_access` flag applies here.
    cache_contract_locally(op_manager, payload.key, state, contract, true).await;
    Ok(())
}

/// Re-query the local store for the contract key, used on the
/// LocalCompletion path where the Request echo carries only an
/// instance_id.
async fn lookup_stored_key(
    op_manager: &OpManager,
    instance_id: &ContractInstanceId,
) -> Option<ContractKey> {
    let lookup = op_manager
        .notify_contract_handler(ContractHandlerEvent::GetQuery {
            instance_id: *instance_id,
            return_contract_code: false,
        })
        .await;

    match lookup {
        Ok(ContractHandlerEvent::GetResponse {
            key: Some(key),
            response: Ok(_),
        }) => Some(key),
        _ => None,
    }
}

// --- Peer advance ---

/// Maximum routing rounds before giving up. Matches PUT's
/// `MAX_PEER_ADVANCEMENTS_NON_STREAMING = 3` and SUBSCRIBE's driver.
/// With typical ring fan-out of 3–5 peers per k_closest call, 3
/// rounds covers 9–15 distinct peers. GET kept the legacy
/// `MAX_RETRIES` name because (a) GETs are never streaming today
/// (no large-payload class on the wire), so the streaming/non-
/// streaming split PUT needed doesn't apply, and (b) renaming would
/// touch unrelated code paths.
const MAX_RETRIES: usize = 3;

/// Maximum advertised hosts a relay forwards to, once its single greedy
/// routing hop is exhausted, before giving up (hosting redesign piece C,
/// invariant 5). Bounded small so the terminal consult adds at most a couple
/// of extra forwards per relay on the NotFound return path — see the consult
/// site in `drive_relay_get_inner` for why this does not re-introduce the
/// per-hop fan-out amplification (local-lookup-gated, advertised-only,
/// bubble-up halts at the first Found).
const MAX_TERMINAL_CONSULT_HOSTS: usize = 2;

/// Carry the client driver's tried set into a fresh attempt's visited
/// bloom, excluding the attempt's intended destination (#4361 / #4364
/// review H1). Split out so unit tests can pin the exclusion behavior
/// without constructing an `OpManager`:
///
/// - dropping the carry entirely resurrects the "failover never reaches
///   the wire" bug (the per-attempt loopback relay re-picks the first
///   configured gateway forever);
/// - dropping the current-target exclusion makes single-gateway retries
///   exhaust instantly (the relay would skip the client's own pick).
fn carry_tried_into_visited(
    visited: &mut VisitedPeers,
    tried: &[SocketAddr],
    current_target_addr: Option<SocketAddr>,
) {
    for addr in tried {
        if Some(*addr) != current_target_addr {
            visited.mark_visited(*addr);
        }
    }
}

fn advance_to_next_peer(
    op_manager: &OpManager,
    instance_id: &ContractInstanceId,
    tried: &mut Vec<std::net::SocketAddr>,
    retries: &mut usize,
) -> Option<(PeerKeyLocation, std::net::SocketAddr)> {
    if *retries >= MAX_RETRIES {
        return None;
    }
    *retries += 1;

    let peer = match op_manager
        .ring
        .k_closest_potentially_hosting(instance_id, tried.as_slice(), 1)
        .into_iter()
        .next()
    {
        Some(peer) => peer,
        None => {
            // Bootstrap fallback (#4361): empty ring → route via a
            // configured gateway instead of silently exhausting.
            return match bootstrap_gateway_target(op_manager, |addr| tried.contains(&addr)) {
                Some((gw, addr)) => {
                    tracing::info!(
                        %instance_id,
                        gateway = %addr,
                        "GET advance: ring empty — falling back to configured gateway"
                    );
                    tried.push(addr);
                    Some((gw, addr))
                }
                None => {
                    tracing::debug!(
                        %instance_id,
                        tried = tried.len(),
                        retries = *retries,
                        "GET advance: no routing candidates — exhausted"
                    );
                    None
                }
            };
        }
    };
    let Some(addr) = peer.socket_addr() else {
        // Rare but possible — `k_closest_potentially_hosting` can return
        // addressless candidates (ring.rs pushes them past the addr-gated
        // filters), and an addressless pick is unusable as a wire target.
        tracing::warn!(
            %instance_id,
            peer = ?peer,
            "GET advance: selected routing candidate has no socket address — treating as exhausted"
        );
        return None;
    };
    tried.push(addr);
    Some((peer, addr))
}

// --- Subscribe child ---

/// Start a post-GET subscription if requested. Mirrors
/// `put::op_ctx_task::maybe_subscribe_child` verbatim.
///
/// For `blocking_subscribe = true`, awaits the subscribe driver inline.
/// For `blocking_subscribe = false`, spawns a fire-and-forget task.
pub(crate) async fn maybe_subscribe_child(
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
        subscribe::run_client_subscribe(op_manager.clone(), *key.id(), child_tx).await;
    } else {
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
                "get: infrastructure error; publishing synthesized client error"
            );
            let synthesized: HostResult = Err(ErrorKind::OperationError {
                cause: format!("GET failed: {err}").into(),
            }
            .into());
            op_manager.send_client_result(client_tx, synthesized);
        }
    }
}

// ── Sub-operation GET driver ────────────────────────────────────
//
// Entry point for node-internal GETs that have no client (executor's
// `local_state_or_from_network` and subscribe's `fetch_contract_if_missing`).
// Reuses the `GetRetryDriver` retry loop but skips auto-subscribe,
// explicit-subscribe hand-off, telemetry route-event emission, and
// result-router publication. The terminal `(GetResult)` (or error) is
// delivered through a oneshot receiver returned to the caller — fire-and-
// forget callers drop the receiver, awaiting callers (executor) await it.

/// Outcome of a sub-op GET task. `Found` carries the wire-level reply
/// payload assembled into a `GetResult`; `NotFound` indicates retry
/// exhaustion or driver classification miss; `Infra` carries an
/// infrastructure failure (channel closed, OpCtx bug, etc.).
#[derive(Debug)]
pub(crate) enum SubOpGetOutcome {
    Found(crate::operations::get::GetResult),
    NotFound(String),
    Infra(OpError),
}

/// Spawn a sub-operation GET. Returns the transaction id and a
/// oneshot receiver carrying the [`SubOpGetOutcome`]. Callers that
/// just want the side effect of caching the contract locally (e.g.,
/// `subscribe::fetch_contract_if_missing`) may drop the receiver
/// immediately. Callers that need the resolved `GetResult` (e.g.,
/// `executor::local_state_or_from_network`) await the receiver.
pub(crate) fn start_sub_op_get(
    op_manager: &OpManager,
    instance_id: ContractInstanceId,
    return_contract_code: bool,
) -> (Transaction, tokio::sync::oneshot::Receiver<SubOpGetOutcome>) {
    let op_manager = Arc::new(op_manager.clone());

    let tx = Transaction::new::<GetMsg>();
    let (out_tx, out_rx) = tokio::sync::oneshot::channel();

    tracing::debug!(
        %tx,
        contract = %instance_id,
        "get: spawning sub-op task"
    );

    GlobalExecutor::spawn(run_sub_op_get(
        op_manager,
        tx,
        instance_id,
        return_contract_code,
        None,
        Some(out_tx),
    ));

    (tx, out_rx)
}

/// Spawn a sub-operation GET that targets a specific peer for its
/// first hop (the legacy `start_targeted_op` migration site for
/// UPDATE's auto-fetch). Fire-and-forget — the outcome is logged and
/// the caller relies on the side effect (contract cached locally).
///
/// Used by `OpManager::try_auto_fetch_contract` when an UPDATE
/// broadcast fails because the recipient lacks the contract's
/// parameters: the directed target is the UPDATE sender, who is
/// known to host the contract. If the directed peer responds
/// `NotFound`, the retry loop falls back to `k_closest_potentially_hosting`
/// just like a regular sub-op GET.
pub(crate) fn start_targeted_sub_op_get(
    op_manager: &OpManager,
    instance_id: ContractInstanceId,
    target: PeerKeyLocation,
) -> Transaction {
    let op_manager = Arc::new(op_manager.clone());

    let tx = Transaction::new::<GetMsg>();

    tracing::debug!(
        %tx,
        contract = %instance_id,
        ?target,
        "get: spawning targeted sub-op task (UPDATE auto-fetch)"
    );

    GlobalExecutor::spawn(run_sub_op_get(
        op_manager,
        tx,
        instance_id,
        true, // auto-fetch always wants the contract code
        Some(target),
        None,
    ));

    tx
}

async fn run_sub_op_get(
    op_manager: Arc<OpManager>,
    tx: Transaction,
    instance_id: ContractInstanceId,
    return_contract_code: bool,
    initial_target: Option<PeerKeyLocation>,
    out_tx: Option<tokio::sync::oneshot::Sender<SubOpGetOutcome>>,
) {
    // RAII guard: same loopback-cleanup as ClientGetCompletionGuard (#4066).
    let _completion_guard = SubOpGetCompletionGuard {
        op_manager: op_manager.clone(),
        tx,
    };

    let outcome = drive_sub_op_get(
        &op_manager,
        tx,
        instance_id,
        return_contract_code,
        initial_target,
    )
    .await;

    if let Some(out_tx) = out_tx {
        // Receiver drop is acceptable — fire-and-forget callers expect this.
        #[allow(clippy::let_underscore_must_use)]
        let _ = out_tx.send(outcome);
    } else {
        // Fire-and-forget targeted auto-fetch. Log the outcome so operators
        // can correlate UPDATE auto-fetch attempts to success/failure.
        match outcome {
            SubOpGetOutcome::Found(_) => tracing::info!(
                %tx,
                contract = %instance_id,
                "get (driver targeted sub-op): UPDATE auto-fetch succeeded"
            ),
            SubOpGetOutcome::NotFound(cause) => tracing::warn!(
                %tx,
                contract = %instance_id,
                cause,
                "get (driver targeted sub-op): UPDATE auto-fetch did not find state"
            ),
            SubOpGetOutcome::Infra(err) => tracing::warn!(
                %tx,
                contract = %instance_id,
                error = %err,
                "get (driver targeted sub-op): UPDATE auto-fetch hit infra error"
            ),
        }
    }
}

/// RAII guard counterpart to `ClientGetCompletionGuard` for sub-op
/// GET drivers.
struct SubOpGetCompletionGuard {
    op_manager: Arc<OpManager>,
    tx: Transaction,
}

impl Drop for SubOpGetCompletionGuard {
    fn drop(&mut self) {
        self.op_manager.completed(self.tx);
    }
}

async fn drive_sub_op_get(
    op_manager: &Arc<OpManager>,
    tx: Transaction,
    instance_id: ContractInstanceId,
    return_contract_code: bool,
    initial_target: Option<PeerKeyLocation>,
) -> SubOpGetOutcome {
    let htl = op_manager.ring.max_hops_to_live;

    let mut tried: Vec<std::net::SocketAddr> = Vec::new();
    if let Some(own_addr) = op_manager.ring.connection_manager.get_own_addr() {
        tried.push(own_addr);
    }
    let initial_target = initial_target.or_else(|| {
        op_manager
            .ring
            .k_closest_potentially_hosting(&instance_id, tried.as_slice(), 1)
            .into_iter()
            .next()
    });
    let current_target = match initial_target {
        Some(peer) => {
            if let Some(addr) = peer.socket_addr() {
                tried.push(addr);
            }
            peer
        }
        // Bootstrap fallback (#4361) — same rationale as the client
        // driver's initial pick: sub-op GETs (auto-fetch, related-contract
        // fetches) hit the same empty-ring blind spot during bootstrap.
        None => match bootstrap_gateway_target(op_manager, |addr| tried.contains(&addr)) {
            Some((gw, addr)) => {
                tracing::info!(
                    %instance_id,
                    gateway = %addr,
                    "GET sub-op: ring empty — initial target falls back to configured gateway"
                );
                tried.push(addr);
                gw
            }
            None => op_manager.ring.connection_manager.own_location(),
        },
    };

    let mut driver = GetRetryDriver {
        op_manager,
        instance_id,
        htl,
        tried,
        retries: 0,
        current_target,
        attempt_visited: VisitedPeers::new(&tx),
        request_sent_at: None,
        response_received_at: None,
    };

    let (loop_result, streaming_assembly) = drive_get_with_assembly_retry(
        op_manager,
        tx,
        "get-subop",
        &mut driver,
        // Sub-op GETs don't feed the router (no RouteEvent is emitted
        // on the sub-op path), so skip the per-retry failure events.
        /* emit_route_failure_on_retry */
        false,
    )
    .await;

    // `op_manager.completed(tx)` runs from the
    // `SubOpGetCompletionGuard` in `run_sub_op_get` AFTER this fn
    // returns. See the matching comment in `drive_client_get_inner`.

    match loop_result {
        RetryLoopOutcome::Done(terminal) => {
            match &terminal {
                Terminal::InlineFound {
                    key,
                    state,
                    contract,
                    hop_count: _,
                } => {
                    cache_contract_locally(
                        op_manager,
                        *key,
                        state.clone(),
                        contract.clone(),
                        false, // sub-op: not a client-originating GET
                    )
                    .await;
                }
                // Stream assembly (with per-candidate retry, #4345)
                // already ran inside `drive_get_with_assembly_retry`;
                // on persistent failure the store re-query below
                // returns NotFound with the assembly cause attached.
                Terminal::Streaming { .. } => {}
                Terminal::LocalCompletion => {}
            }

            // Resolve a GetResult by re-querying the local store. Mirrors
            // `build_host_response` but returns the raw `GetResult` shape
            // instead of a HostResponse.
            let lookup = op_manager
                .notify_contract_handler(ContractHandlerEvent::GetQuery {
                    instance_id,
                    return_contract_code,
                })
                .await;
            match lookup {
                Ok(ContractHandlerEvent::GetResponse {
                    key: Some(_),
                    response:
                        Ok(StoreResponse {
                            state: Some(state),
                            contract,
                        }),
                }) => {
                    let client_contract = if return_contract_code { contract } else { None };
                    SubOpGetOutcome::Found(crate::operations::get::GetResult::new(
                        state,
                        client_contract,
                    ))
                }
                _ => SubOpGetOutcome::NotFound(sub_op_not_found_cause(
                    &instance_id,
                    streaming_assembly.error.as_deref(),
                )),
            }
        }
        RetryLoopOutcome::Exhausted(cause) => SubOpGetOutcome::NotFound(cause),
        RetryLoopOutcome::Unexpected => SubOpGetOutcome::Infra(OpError::UnexpectedOpState),
        RetryLoopOutcome::InfraError(err) => SubOpGetOutcome::Infra(err),
    }
}

/// Cause string for a client GET whose terminal reply was classified
/// success but whose state never reached the local store. Carries the
/// assembly failure cause when the streaming path exhausted its retry
/// budget (#4345); otherwise the generic store-lookup message (covers
/// eviction races and executor rejections). Pure helper so both arms
/// are unit-testable without a real `OpManager`.
fn synthesized_get_error_cause(
    instance_id: &ContractInstanceId,
    assembly_error: Option<&str>,
) -> String {
    match assembly_error {
        Some(e) => format!(
            "GET response stream assembly failed for {instance_id} \
             after exhausting retries: {e}"
        ),
        None => format!("GET succeeded on wire but local store lookup failed for {instance_id}"),
    }
}

/// Sub-op variant of [`synthesized_get_error_cause`]: the store-miss
/// cause with the assembly failure appended when one occurred (#4345).
fn sub_op_not_found_cause(
    instance_id: &ContractInstanceId,
    assembly_error: Option<&str>,
) -> String {
    match assembly_error {
        Some(e) => format!("{}: {e}", missing_state_cause(instance_id)),
        None => missing_state_cause(instance_id),
    }
}

/// Cause string for a sub-op GET that succeeded on the wire but
/// produced no state in the local store on re-query. Extracted as a
/// pure helper so the message format is unit-testable without a
/// real `OpManager`.
fn missing_state_cause(instance_id: &ContractInstanceId) -> String {
    format!(
        "sub-op GET wire-level success but local store lookup returned no state for {instance_id}"
    )
}

// ── Relay GET driver (#3883) ────────────────────────────────────
//
// `start_relay_get` is the entry point for the relay (non-originator) GET
// driver.  It is called from `node.rs` dispatch (commit 2) when
// an incoming `GetMsg::Request` arrives with `source_addr.is_some()` — i.e.
// from a real remote peer rather than the originator's own loop-back.
//
// The driver owns all routing / forwarding / retry state in task locals.
// No `GetOp` is stored in `OpManager.ops.get` for any relay hop this driver
// handles.  Responses are bubbled back upstream via `OpCtx::send_to_and_await`
// targeting the downstream peer, and the reply (Found / NotFound) is forwarded
// to `upstream_addr` via a fire-and-forget `conn_manager.send`.
//
// ResponseStreaming relay forwarding (chunk pipe-through) IS now implemented
// in the `Terminal::Streaming` arm of `drive_relay_get_inner` (fork + pipe;
// issue #4307). Still out of scope for the driver:
//   - GC-spawned retries.
//   - start_targeted_op (UPDATE-triggered auto-fetch).

/// Start a relay (non-originator) GET driver.
///
/// Called from `node.rs` dispatch when an incoming `GetMsg::Request`
/// arrives from a remote peer (`source_addr.is_some()`).  The driver
/// owns routing/forwarding/retry state in its task locals — no `GetOp`
/// is stored in `OpManager.ops.get` for this transaction.
///
/// Originator loop-back (`source_addr.is_none()`) is mapped by `node.rs`
/// dispatch to `upstream_addr = own_addr` and driven by this same driver
/// (the legacy `process_message` path is deleted); the loopback edge is
/// handled by the safety-net branches that cache locally instead of
/// piping/replying to self.
///
/// # Scope (#3883)
///
/// Migrated:
/// - `GetMsg::Request` relay arm: HTL check, local-cache lookup (with
///   interest gate), forward-or-respond decision.
/// - `GetMsg::Response{NotFound}` retry to next alternative peer.
/// - `GetMsg::Response{Found}` bubble-up to upstream.
/// - `GetMsg::ResponseStreaming` relay forwarding (fork + pipe, #4307).
/// - `ForwardingAck` send-before-forward.
///
/// NOT migrated (stays on legacy path):
/// - GC-spawned retries.
/// - `start_targeted_op` (UPDATE-triggered auto-fetch).
#[allow(clippy::too_many_arguments)]
pub(crate) async fn start_relay_get<CB>(
    op_manager: Arc<OpManager>,
    conn_manager: CB,
    incoming_tx: Transaction,
    instance_id: ContractInstanceId,
    htl: usize,
    upstream_addr: SocketAddr,
    visited: VisitedPeers,
    fetch_contract: bool,
    subscribe: bool,
) -> Result<(), OpError>
where
    CB: NetworkBridge + Clone + Send + 'static,
{
    // Test-only: count relay driver invocations so regression tests can
    // assert the dispatch gate in node.rs actually routed a fresh inbound
    // Request through the driver rather than the legacy
    // `handle_op_request` fallthrough (phase-3b loopback, GC retries,
    // `start_targeted_op`).
    #[cfg(any(test, feature = "testing"))]
    RELAY_DRIVER_CALL_COUNT.fetch_add(1, std::sync::atomic::Ordering::SeqCst);

    // Dedup gate: a relay driver for this incoming_tx is already live on
    // this node. The duplicate inbound Request is a retransmission, GC
    // retry, or routing bloom false-positive — don't spawn a second
    // driver. Returning Ok drops the Request silently (matches legacy
    // semantics where a duplicate Request hitting an in-flight op
    // returned `OpNotAvailable::Running` and was dropped by the caller).
    if !op_manager.active_relay_get_txs.insert(incoming_tx) {
        RELAY_DEDUP_REJECTS.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        tracing::debug!(
            tx = %incoming_tx,
            %instance_id,
            %upstream_addr,
            phase = "relay_dedup_reject",
            "GET relay: duplicate Request for in-flight tx, dropping"
        );
        return Ok(());
    }

    tracing::debug!(
        tx = %incoming_tx,
        %instance_id,
        htl,
        %upstream_addr,
        phase = "relay_start",
        "GET relay: spawning driver"
    );

    GlobalExecutor::spawn(run_relay_get(
        op_manager,
        conn_manager,
        incoming_tx,
        instance_id,
        htl,
        upstream_addr,
        visited,
        fetch_contract,
        subscribe,
    ));
    Ok(())
}

/// RAII guard that decrements `RELAY_INFLIGHT`, bumps
/// `RELAY_COMPLETED_TOTAL`, and removes the driver's `incoming_tx`
/// from the per-node dedup set on drop. Using a guard (vs. matching
/// every exit path) keeps the counters and dedup set correct under
/// panics and early returns.
struct RelayInflightGuard {
    op_manager: Arc<OpManager>,
    incoming_tx: Transaction,
}

impl Drop for RelayInflightGuard {
    fn drop(&mut self) {
        self.op_manager
            .active_relay_get_txs
            .remove(&self.incoming_tx);
        RELAY_INFLIGHT.fetch_sub(1, std::sync::atomic::Ordering::Relaxed);
        RELAY_COMPLETED_TOTAL.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    }
}

#[allow(clippy::too_many_arguments)]
async fn run_relay_get<CB>(
    op_manager: Arc<OpManager>,
    conn_manager: CB,
    incoming_tx: Transaction,
    instance_id: ContractInstanceId,
    htl: usize,
    upstream_addr: SocketAddr,
    visited: VisitedPeers,
    fetch_contract: bool,
    subscribe: bool,
) where
    CB: NetworkBridge + Clone + Send + 'static,
{
    RELAY_INFLIGHT.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    RELAY_SPAWNED_TOTAL.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    let _guard = RelayInflightGuard {
        op_manager: op_manager.clone(),
        incoming_tx,
    };

    let drive_result = drive_relay_get(
        &op_manager,
        &conn_manager,
        incoming_tx,
        instance_id,
        htl,
        upstream_addr,
        visited,
        fetch_contract,
        subscribe,
    )
    .await;

    if let Err(err) = &drive_result {
        tracing::warn!(
            tx = %incoming_tx,
            %instance_id,
            error = %err,
            phase = "relay_infra_error",
            "GET relay: infrastructure error in driver"
        );
    }

    // Release the per-tx `pending_op_results` slot at driver exit.
    // `relay_send_found` / `relay_send_not_found` / the send_to_and_await
    // in the retry loop all leave `pending_op_results[incoming_tx]`
    // populated with an is_closed sender that would only be reclaimed
    // by the 60s periodic sweep; sending `TransactionCompleted` now
    // removes the entry immediately so `test_pending_op_results_bounded`
    // stays under its leak ceiling.
    //
    // Originator-loopback exception: when this driver runs on the
    // originator's own node (`upstream_addr == own_addr`), the
    // `pending_op_results` callback for `incoming_tx` is the
    // originator's `send_and_await` waiter — NOT one this driver
    // installed. Releasing it here would emit `TransactionCompleted`
    // and remove the originator's callback BEFORE the loopback
    // `GetMsg::Response` arrives at the bypass, racing the
    // originator's wait. The originator's own driver completion
    // path (via `ClientGetCompletionGuard` →
    // `release_pending_op_slot`) cleans up the slot.
    //
    // Yield first so any in-flight `send_fire_and_forget` has time to
    // run through the event loop's `handle_op_execution` and actually
    // INSERT into `pending_op_results`. `release_pending_op_slot` uses
    // the higher-priority notification channel, so without the yield
    // the TransactionCompleted arrives before the insert and the
    // cleanup is a no-op (per the caveat in `OpCtx::send_fire_and_forget`
    // docs).
    let own_addr = op_manager.ring.connection_manager.get_own_addr();
    let originator_loopback = Some(upstream_addr) == own_addr;
    tokio::task::yield_now().await;
    if !originator_loopback {
        op_manager.release_pending_op_slot(incoming_tx).await;
    }
}

// NAMING LANDMINE: "relay" is a fossil of the hollow-relay firefight (#3763).
// Forwarding a GET does NOT make a peer a hollow relay; a routed GET is demand, so
// the peer HOSTS the contract (routing and hosting co-occur; there is no
// forward-without-hosting for GET/PUT). The only true hollow relay was the
// standalone SUBSCRIBE hop, being retired (subscribe folds into GET/PUT). Slated for
// removal/rename by piece D (chain peers become real hosts). Do not name new code
// "relay". See .claude/rules/hosting-invariants.md terminology + epic #4642.
#[allow(clippy::too_many_arguments)]
async fn drive_relay_get<CB>(
    op_manager: &Arc<OpManager>,
    conn_manager: &CB,
    incoming_tx: Transaction,
    instance_id: ContractInstanceId,
    htl: usize,
    upstream_addr: SocketAddr,
    visited: VisitedPeers,
    fetch_contract: bool,
    subscribe: bool,
) -> Result<(), OpError>
where
    CB: NetworkBridge + Clone + Send + 'static,
{
    match drive_relay_get_inner(
        op_manager,
        conn_manager,
        incoming_tx,
        instance_id,
        htl,
        upstream_addr,
        visited,
        fetch_contract,
        subscribe,
    )
    .await
    {
        Ok(()) => Ok(()),
        Err(err) => {
            tracing::warn!(
                tx = %incoming_tx,
                %instance_id,
                error = %err,
                phase = "relay_inner_error",
                "GET relay: inner driver returned error; sending NotFound upstream"
            );
            // On infrastructure error, send NotFound upstream so the upstream
            // doesn't time out waiting for us.
            let hop_count = op_manager.ring.max_hops_to_live.saturating_sub(htl);
            relay_send_not_found(
                op_manager,
                incoming_tx,
                instance_id,
                upstream_addr,
                hop_count,
            )
            .await;
            Err(err)
        }
    }
}

/// Local fallback tuple: key, state, optional contract code.
type LocalFallback = (ContractKey, WrappedState, Option<ContractContainer>);

/// Relay-side local cache lookup with interest gate.
///
/// Mirrors `GetMsg::Request` match arm at `get.rs:1369-1433`.
///
/// Returns `(local_value, local_fallback)`:
/// - `local_value`: Some if we should serve immediately (active interest or
///   originator).  None if we should forward.
/// - `local_fallback`: Some if we have stale local state with no active
///   interest — used as a fallback if all downstream peers return NotFound.
async fn check_local_with_interest_gate(
    op_manager: &OpManager,
    instance_id: &ContractInstanceId,
    fetch_contract: bool,
) -> (Option<LocalFallback>, Option<LocalFallback>) {
    let get_result = op_manager
        .notify_contract_handler(ContractHandlerEvent::GetQuery {
            instance_id: *instance_id,
            return_contract_code: fetch_contract,
        })
        .await;

    let raw_local = match get_result {
        Ok(ContractHandlerEvent::GetResponse {
            key: Some(key),
            response:
                Ok(StoreResponse {
                    state: Some(state),
                    contract,
                }),
        }) => {
            if fetch_contract && contract.is_none() {
                // State available but contract code missing — cannot serve,
                // must forward to get WASM from the network.
                None
            } else {
                Some((key, state, contract))
            }
        }
        _ => None,
    };

    match raw_local {
        None => (None, None),
        Some((key, state, contract)) => {
            // Interest gate (mirrors get.rs:1408-1433):
            // Relay peers actively hosting serve immediately.
            // Peers with only stale LRU cache (no active interest) defer to
            // the network but keep the local value as fallback.
            if !op_manager.interest_manager.has_local_interest(&key) {
                // Stale cache only — defer to network, keep as fallback.
                tracing::debug!(
                    %instance_id,
                    "GET relay: stale cache (no local interest), will forward"
                );
                (None, Some((key, state, contract)))
            } else {
                // Actively hosting with interest — serve immediately.
                (Some((key, state, contract)), None)
            }
        }
    }
}

/// Select the next downstream peer for relay forwarding.
///
/// Uses `new_visited` (bloom filter) as the primary skip list for
/// `k_closest_potentially_hosting` — this already encodes the upstream's
/// skip list plus this peer and the upstream address.  `tried` is used
/// for retry-count tracking and to ensure we don't revisit peers we've
/// explicitly attempted in this driver invocation.
///
/// Returns None when exhausted (`retries >= MAX_RELAY_RETRIES`).
fn relay_advance_to_next_peer(
    op_manager: &OpManager,
    instance_id: &ContractInstanceId,
    tried: &mut Vec<SocketAddr>,
    retries: &mut usize,
    new_visited: &VisitedPeers,
) -> Option<(PeerKeyLocation, SocketAddr)> {
    // Legacy relay does NOT retry alternative peers at each hop — it
    // forwards once and bubbles back whatever downstream returned. The
    // phase-5 migration introduced a 3-peer retry loop here
    // that compounded fan-out to 3^HTL per origination under virtual
    // time (ci-fault-loss run 24602255580 showed 16.9M spawns in 95s
    // with single-use tx reuse still in place). Cap at 1 to match
    // legacy semantics exactly. If downstream fails, we bubble
    // NotFound to upstream and let the originator's client driver
    // handle cross-peer retries (which IS legitimate in
    // `drive_client_get_inner`'s retry loop).
    const MAX_RELAY_RETRIES: usize = 1;
    if *retries >= MAX_RELAY_RETRIES {
        return None;
    }
    *retries += 1;

    // Use new_visited as the skip list so upstream's visited set and our own
    // marks are both respected.
    let peer = match op_manager
        .ring
        .k_closest_potentially_hosting(instance_id, new_visited.clone(), 1)
        .into_iter()
        .next()
    {
        Some(peer) => peer,
        None => {
            // Bootstrap fallback (#4361): empty ring → forward via a
            // configured gateway instead of silently exhausting. Respects
            // both the request's visited bloom (cross-hop loop prevention —
            // a gateway the request already traversed is never re-picked)
            // and `tried` (exact local exclusion). Stays within
            // MAX_RELAY_RETRIES, so the 3^HTL fan-out guard above is
            // unaffected.
            return match bootstrap_gateway_target(op_manager, |addr| {
                tried.contains(&addr) || new_visited.probably_visited(addr)
            }) {
                Some((gw, addr)) => {
                    tracing::info!(
                        %instance_id,
                        gateway = %addr,
                        "GET relay advance: ring empty — forwarding to configured gateway"
                    );
                    tried.push(addr);
                    Some((gw, addr))
                }
                None => {
                    tracing::debug!(
                        %instance_id,
                        "GET relay advance: no routing candidates — exhausted"
                    );
                    None
                }
            };
        }
    };
    let Some(addr) = peer.socket_addr() else {
        // Rare but possible — see the matching guard in
        // `advance_to_next_peer` for why addressless candidates can leak
        // out of `k_closest_potentially_hosting`.
        tracing::warn!(
            %instance_id,
            peer = ?peer,
            "GET relay advance: selected routing candidate has no socket address — treating as exhausted"
        );
        return None;
    };
    // Double-check against tried (exact exclusion, no bloom false positives).
    if tried.contains(&addr) {
        tracing::debug!(
            %instance_id,
            peer = %addr,
            "GET relay advance: candidate already tried — exhausted"
        );
        return None;
    }
    tried.push(addr);
    Some((peer, addr))
}

/// Build and send a `GetMsg::Response{NotFound}` to `upstream_addr`.
///
/// `hop_count` is the forward-path hop count to embed in the Response. For
/// a relay that's producing the NotFound itself (HTL exhaustion or downstream
/// exhaustion), this is `max_htl - incoming_htl`. For an originator-side
/// originator-loopback NotFound from `start_client_get` after exhaustion,
/// pass 0 (no remote hops traversed).
async fn relay_send_not_found(
    op_manager: &OpManager,
    tx: Transaction,
    instance_id: ContractInstanceId,
    upstream_addr: SocketAddr,
    hop_count: usize,
) {
    let msg = NetMessage::from(GetMsg::Response {
        id: tx,
        instance_id,
        result: GetMsgResult::NotFound,
        hop_count,
    });
    let mut ctx = op_manager.op_ctx(tx);
    // Originator-loopback: when this driver runs on the originator's
    // own node (`upstream_addr == own_addr`), routing the wire-bound
    // `send_fire_and_forget(own_addr, ...)` would attempt a
    // self-connection (no peer entry exists for own_addr). Use
    // `send_local_loopback` to route as `InboundMessage` instead, so
    // the bypass at `handle_pure_network_message_v1` can forward the
    // terminal Response to the originator's `pending_op_results`
    // waiter. Mirrors `relay_put_send_response`'s loopback branch.
    let own_addr = op_manager.ring.connection_manager.get_own_addr();
    let send_result = if Some(upstream_addr) == own_addr {
        ctx.send_local_loopback(msg).await
    } else {
        ctx.send_fire_and_forget(upstream_addr, msg).await
    };
    if let Err(err) = send_result {
        tracing::warn!(
            tx = %tx,
            %instance_id,
            %upstream_addr,
            error = %err,
            "GET relay: failed to send NotFound upstream"
        );
    }
}

/// Build and send a `GetMsg::Response{Found}` (inline, non-streaming) to
/// `upstream_addr`.  Returns an error on serialization failure.
///
/// `hop_count` is the forward-path hop count to embed in the Response. For
/// the storer's own initial Found, this is `max_htl - incoming_htl`. For a
/// relay bubbling up a downstream Found, this is the downstream Response's
/// `hop_count` (preserved through the bubble-up chain).
#[allow(clippy::too_many_arguments)]
async fn relay_send_found<CB>(
    op_manager: &OpManager,
    conn_manager: &CB,
    tx: Transaction,
    instance_id: ContractInstanceId,
    upstream_addr: SocketAddr,
    key: ContractKey,
    state: WrappedState,
    contract: Option<ContractContainer>,
    hop_count: usize,
) -> Result<(), OpError>
where
    CB: NetworkBridge + Clone + Send + 'static,
{
    let mut ctx = op_manager.op_ctx(tx);
    // Originator-loopback: route via `send_local_loopback` to avoid
    // the wire-bound self-connection failure. See `relay_send_not_found`
    // for the full rationale. No streaming over loopback — there's no
    // network benefit and it avoids loopback-stream complexity.
    let own_addr = op_manager.ring.connection_manager.get_own_addr();
    if Some(upstream_addr) == own_addr {
        let msg = NetMessage::from(GetMsg::Response {
            id: tx,
            instance_id,
            result: GetMsgResult::Found {
                key,
                value: StoreResponse {
                    state: Some(state),
                    contract,
                },
            },
            hop_count,
        });
        return ctx
            .send_local_loopback(msg)
            .await
            .map_err(|_| OpError::NotificationError);
    }

    // Remote upstream: stream large payloads instead of falling back to
    // message-level store-and-forward. The legacy GET producer (#3586,
    // removed by the task-per-tx migration) did exactly this; restoring
    // it here closes #4307. Mirrors `drive_relay_put`'s streaming-upgrade
    // producer branch (put/op_ctx_task.rs).
    //
    // Capture `includes_contract` BEFORE moving `contract` into the
    // payload, since the wire header needs it but serialization consumes
    // the contract.
    let includes_contract = contract.is_some();
    let payload = GetStreamingPayload {
        key,
        value: StoreResponse {
            state: Some(state),
            contract,
        },
    };
    let payload_bytes = match bincode::serialize(&payload) {
        Ok(b) => b,
        Err(e) => {
            return Err(OpError::NotificationChannelError(format!(
                "Failed to serialize GET relay forward payload: {e}"
            )));
        }
    };

    if crate::operations::should_use_streaming(op_manager.streaming_threshold, payload_bytes.len())
    {
        let sid = StreamId::next_operations();
        tracing::info!(
            tx = %tx,
            contract = %key,
            target = %upstream_addr,
            total_size = payload_bytes.len(),
            %sid,
            phase = "relay_streaming_forward",
            "GET relay: payload exceeds threshold, streaming response upstream"
        );
        // NOTE: `hop_count` is intentionally NOT carried on the streaming
        // wire path — the `ResponseStreaming` variant has no such field
        // (unlike the inline `Response{Found}` below, which propagates
        // `hop_count`). This matches the legacy streaming producer's
        // limitation; transfer-rate routing observations on the originator
        // come from `total_size` instead, so the lost hop depth does not
        // degrade routing.
        let header = NetMessage::from(GetMsg::ResponseStreaming {
            id: tx,
            instance_id,
            stream_id: sid,
            key,
            total_size: payload_bytes.len() as u64,
            includes_contract,
        });
        // Embed a serialized copy of the metadata header in fragment #1
        // (fix #2757). On a lossy link the separately-sent fire-and-forget
        // header can be lost while fragments arrive — without self-describing
        // metadata the upstream waiter never matches the orphan stream and
        // the GET times out. Mirrors the forward path's `embedded`.
        let embedded = bincode::serialize(&header).ok().map(bytes::Bytes::from);
        if embedded.is_none() {
            tracing::warn!(
                tx = %tx,
                target = %upstream_addr,
                %sid,
                "GET relay: failed to serialize ResponseStreaming header for \
                 fragment-#1 embedding; sending stream without embedded metadata"
            );
        }
        // Install nothing — the upstream already has its waiter (it sent
        // us the Request). Send the metadata header first, then push the
        // raw fragments. Ordering matches the PUT producer.
        ctx.send_fire_and_forget(upstream_addr, header)
            .await
            .map_err(|_| OpError::NotificationError)?;
        conn_manager
            .send_stream(
                upstream_addr,
                sid,
                bytes::Bytes::from(payload_bytes),
                embedded,
            )
            .await
            .map_err(|e| {
                OpError::NotificationChannelError(format!("get relay send_stream failed: {e}"))
            })?;
        Ok(())
    } else {
        // Below threshold: inline Response{Found} (legacy behavior).
        let StoreResponse { state, contract } = payload.value;
        let msg = NetMessage::from(GetMsg::Response {
            id: tx,
            instance_id,
            result: GetMsgResult::Found {
                key,
                value: StoreResponse { state, contract },
            },
            hop_count,
        });
        ctx.send_fire_and_forget(upstream_addr, msg)
            .await
            .map_err(|_| OpError::NotificationError)
    }
}

#[allow(clippy::too_many_arguments)]
async fn drive_relay_get_inner<CB>(
    op_manager: &Arc<OpManager>,
    conn_manager: &CB,
    incoming_tx: Transaction,
    instance_id: ContractInstanceId,
    htl: usize,
    upstream_addr: SocketAddr,
    visited: VisitedPeers,
    fetch_contract: bool,
    subscribe: bool,
) -> Result<(), OpError>
where
    CB: NetworkBridge + Clone + Send + 'static,
{
    let ring_max_htl = op_manager.ring.max_hops_to_live.max(1);
    let htl = htl.min(ring_max_htl);

    // ── Short-circuit 1: HTL = 0 ────────────────────────────────────────
    if htl == 0 {
        tracing::warn!(
            tx = %incoming_tx,
            %instance_id,
            %upstream_addr,
            htl = 0,
            phase = "not_found",
            "GET relay: HTL exhausted — sending NotFound upstream"
        );
        if let Some(event) = crate::tracing::NetEventLog::get_not_found(
            &incoming_tx,
            &op_manager.ring,
            instance_id,
            Some(op_manager.ring.max_hops_to_live),
        ) {
            op_manager
                .ring
                .register_events(either::Either::Left(event))
                .await;
        }
        // HTL=0 path: the original Request reached us with htl already at 0,
        // meaning the request traversed the full max_htl forward hops to get
        // here. hop_count = max_htl - 0 = max_htl.
        let hop_count = op_manager.ring.max_hops_to_live;
        // EXPERIMENT-ONLY (findability_probe): record the HTL-exhaustion GET
        // terminus. NOT for ship.
        #[cfg(any(test, feature = "testing"))]
        {
            let visited = visited.clone();
            crate::operations::findability_probe::record_terminus(
                &op_manager.ring,
                crate::operations::findability_probe::ProbeOpKind::Get,
                &incoming_tx,
                crate::ring::Location::from(&instance_id),
                htl,
                hop_count,
                crate::operations::findability_probe::ProbeStopReason::HtlZero,
                move |addr| visited.probably_visited(addr),
            );
        }
        relay_send_not_found(
            op_manager,
            incoming_tx,
            instance_id,
            upstream_addr,
            hop_count,
        )
        .await;
        return Ok(());
    }

    // ── Update visited set: mark this peer and the upstream ─────────────
    let mut new_visited = visited.with_transaction(&incoming_tx);
    if let Some(own_addr) = op_manager.ring.connection_manager.get_own_addr() {
        new_visited.mark_visited(own_addr);
    }
    new_visited.mark_visited(upstream_addr);

    // ── Short-circuit 2: Local cache hit with interest ───────────────────
    let (local_value, local_fallback) =
        check_local_with_interest_gate(op_manager, &instance_id, fetch_contract).await;

    if let Some((key, state, contract)) = local_value {
        tracing::info!(
            tx = %incoming_tx,
            %instance_id,
            contract = %key,
            phase = "complete",
            "GET relay: contract found locally (active interest) — sending Found upstream"
        );

        // EXPERIMENT-ONLY (findability_probe): record that greedy routing REACHED
        // a live local copy at this peer (for the single-seeded-copy experiment
        // this is the rank-0 holder). NOT for ship.
        #[cfg(any(test, feature = "testing"))]
        {
            let visited = new_visited.clone();
            let hop_count = op_manager.ring.max_hops_to_live.saturating_sub(htl);
            crate::operations::findability_probe::record_terminus(
                &op_manager.ring,
                crate::operations::findability_probe::ProbeOpKind::Get,
                &incoming_tx,
                crate::ring::Location::from(&instance_id),
                htl,
                hop_count,
                crate::operations::findability_probe::ProbeStopReason::Found,
                move |addr| visited.probably_visited(addr),
            );
        }

        // Register interest for the requester (mirrors get.rs:1447-1476).
        // register-after-state (step 10 §1a): safe to register BEFORE the cache
        // call below because this is the local-hit arm — `local_value` is
        // `Some`, so the contract's state is already present on disk. The
        // phantom (contract_in_use && !state) is impossible here.
        if subscribe {
            crate::operations::subscribe::register_downstream_subscriber(
                op_manager,
                &key,
                upstream_addr,
                None,
                None,
                &incoming_tx,
                " (relay loopback, piggybacked on GET)",
            )
            .await;
        } else if let Some(pkl) = op_manager
            .ring
            .connection_manager
            .get_peer_by_addr(upstream_addr)
        {
            let peer_key = crate::ring::interest::PeerKey::from(pkl.pub_key);
            let is_new = op_manager
                .interest_manager
                .register_peer_interest(&key, peer_key, None, false);
            if is_new {
                // #4359 (MUST-FIX 1): a remote GET with subscribe=false still
                // registers the requester's interest, making them a viable
                // broadcast target. Flush any deferred fresh-contract broadcast
                // so a cold-id PUT that gave up reaches this requester.
                op_manager.flush_pending_broadcast_on_interest(&key).await;
            }
        }

        // Forward upstream FIRST to keep cache work off the critical
        // path (issue #4155). `cache_contract_locally` enqueues a
        // GetQuery + PutQuery on the single-threaded `contract_handling`
        // event loop; the PutQuery runs WASM `validate_state` (and may
        // recursively `RequestRelated` with a 10s
        // `RELATED_FETCH_TIMEOUT`). When that queue is backed up,
        // per-hop dwell can reach many seconds. Forwarding first drops
        // the upstream-visible per-hop dwell from O(validate_state) to
        // O(channel_send).
        //
        // Caching ALWAYS runs after forwarding, even when forwarding
        // returns Err — this preserves the legacy invariant that LRU/TTL
        // bookkeeping and `announce_contract_hosted` fire on any successful
        // wire-level GET. The send error is deferred via `let send_result`
        // and propagated after the cache call.
        //
        // R4 (this site) and R10 read from the local store, so the
        // `state_matches` short-circuit inside `cache_contract_locally`
        // skips the expensive PutQuery — the perf win is concentrated
        // at R12a (downstream-Found bubble-up). We still reorder R4/R10
        // uniformly so the invariant "forward before cache" holds across
        // every relay-driver callsite and a future refactor can't
        // accidentally re-introduce cache-before-forward at any one site.
        //
        // The forwarded bytes get re-validated at the next hop /
        // originator, so the relay doesn't need to validate locally to
        // forward safely. `is_client_requester=false` on the cache
        // call: relay peers must not set the sticky
        // `local_client_access` flag — that's exclusively for the
        // client-originating node. Legacy mirror: `get.rs:2260-2262`
        // gates on `is_original_requester`.
        // R4 (local cache hit): this relay IS the storer. hop_count =
        // max_htl - htl_we_received.
        let hop_count = op_manager.ring.max_hops_to_live.saturating_sub(htl);
        let send_result = relay_send_found(
            op_manager,
            conn_manager,
            incoming_tx,
            instance_id,
            upstream_addr,
            key,
            state.clone(),
            contract.clone(),
            hop_count,
        )
        .await;
        cache_contract_locally(op_manager, key, state, contract, false).await;
        send_result?;
        return Ok(());
    }

    // ── Retry loop: forward to downstream peers ──────────────────────────
    // `tried` tracks the exact addrs we've attempted for retry counting.
    // The bloom filter `new_visited` acts as the skip list for
    // `k_closest_potentially_hosting` (it already has own_addr and
    // upstream_addr marked in).
    let mut tried: Vec<SocketAddr> = Vec::new();
    if let Some(own_addr) = op_manager.ring.connection_manager.get_own_addr() {
        tried.push(own_addr);
    }
    tried.push(upstream_addr);

    let mut retries: usize = 0;

    // Terminal advertisement consult (hosting redesign piece C, invariant 5).
    // Lazily built the first time location routing is exhausted; each entry
    // is an advertised host to forward to (off the direct routing path).
    let mut local_fallback = local_fallback;
    let mut consult_targets: Option<std::vec::IntoIter<(PeerKeyLocation, SocketAddr)>> = None;
    // True while the current attempt targets a consulted advertised host (so
    // its Found reply is attributed to the consult telemetry, not routing).
    // Assigned by every non-returning arm of the peer-selection match below.
    let mut consult_active: bool;
    // True when the most recent forward on `incoming_tx` produced NO reply
    // (timeout or send-failure). In that state the awaited peer may still send
    // a LATE reply on the reused tx, which would satisfy a freshly-registered
    // consult waiter and bubble a false result — so we do NOT consult after a
    // failed forward (Codex P2). Reset by any forward that received a reply.
    let mut last_forward_failed = false;
    // True once at least one location-routing forward has happened. When
    // `relay_advance_to_next_peer` returns None on the FIRST iteration (no
    // routing candidate at all), consulting is a no-op — every advertised host
    // is either a visited connection (excluded) or a not-ready/transient one
    // that k_closest deliberately skips — so we skip the consult there for
    // counter accuracy and parity with SUBSCRIBE's `candidates.is_empty()`
    // branch. The consult's value is reaching an unvisited advertised neighbor
    // the single greedy forward (MAX_RELAY_RETRIES) skipped, which requires a
    // routing forward to have occurred.
    let mut did_forward = false;

    loop {
        // Pick next downstream peer.
        let (peer, peer_addr) = match relay_advance_to_next_peer(
            op_manager,
            &instance_id,
            &mut tried,
            &mut retries,
            &new_visited,
        ) {
            Some(p) => {
                consult_active = false;
                did_forward = true;
                p
            }
            None => {
                // Location routing exhausted → this relay is a terminus for
                // the key. Prefer any (stale) local fallback we already hold —
                // existing behavior, not a dead-end.
                if let Some((key, state, contract)) = local_fallback.take() {
                    tracing::info!(
                        tx = %incoming_tx,
                        %instance_id,
                        contract = %key,
                        "GET relay: all peers exhausted — serving local fallback"
                    );
                    // Forward upstream FIRST to keep cache off the critical
                    // path (issue #4155 — see top-of-loop comment above the
                    // R4 callsite for the full rationale). Cache ALWAYS
                    // runs after forwarding so LRU/TTL bookkeeping fires
                    // even when the forward errors. Relay path:
                    // `is_client_requester=false` on the cache call.
                    // R10 (local fallback): this relay is acting as the
                    // storer; same hop_count semantics as R4.
                    let hop_count = op_manager.ring.max_hops_to_live.saturating_sub(htl);
                    let send_result = relay_send_found(
                        op_manager,
                        conn_manager,
                        incoming_tx,
                        instance_id,
                        upstream_addr,
                        key,
                        state.clone(),
                        contract.clone(),
                        hop_count,
                    )
                    .await;
                    cache_contract_locally(op_manager, key, state, contract, false).await;
                    send_result?;
                    return Ok(());
                }

                // This relay has exhausted its single greedy routing forward
                // (MAX_RELAY_RETRIES = 1) without finding the contract, and
                // holds no local copy. Before declaring a findability
                // dead-end, consult the host advertisements our neighbors
                // broadcast for this key (invariant 5) and try the closest
                // UNVISITED advertised hosts the retry cap skipped — the "one
                // hop off the routing path" case.
                //
                // `consult_advertised_hosts` returns ONLY peers that already
                // advertised hosting — nothing is pushed or cached, so this is
                // not the speculative-pre-replication anti-pattern. Each
                // candidate is marked tried + visited so the existing dedup
                // prevents loops, and the forward below carries HTL-1.
                //
                // This does NOT re-introduce the removed 3^HTL per-hop retry
                // fan-out: a *local* lookup gates the forward, so a
                // truly-absent contract (no neighbor advertised it) triggers
                // ZERO extra network forwards; and each relay adds at most
                // MAX_TERMINAL_CONSULT_HOSTS forwards, only on the NotFound
                // return path. It can fire at an intermediate relay (not only
                // the deepest terminus), which is intended: the advertised host
                // is a neighbor of THIS relay, invisible to the deeper terminus.
                //
                // LOAD-BEARING assumption: "an advertised host almost always
                // HAS the contract, so the bubble-up halts at the first Found."
                // The rare exception is a host that evicted the contract but
                // whose removal-announce (`on_contract_unhosted`) was lost while
                // it is still a ring connection — the consult can then turn it
                // into a re-routing relay (branching 1 → toward relay fan-out).
                // That branching is bounded and does NOT recreate the 3^HTL
                // tx-minting bug: (a) HTL-1 caps depth; (b) the visited bloom is
                // carried on the forward so no peer is re-probed; (c) the
                // consult only targets CURRENTLY-CONNECTED advertised peers; and
                // (d) eviction retracts the advertisement, so the window is
                // small and self-closing. If a future change weakens any of
                // these, re-evaluate this site.
                //
                // Only consult when the previous forward received a reply; a
                // timed-out / send-failed forward may still reply late on the
                // reused tx and satisfy the consult waiter (Codex P2). And only
                // consult when a routing forward actually happened
                // (`did_forward`) — a no-candidate terminus consult is a no-op
                // (parity with SUBSCRIBE). Either way, skip the consult and
                // report NotFound — the same outcome as before the consult.
                // EXPERIMENT-ONLY (findability_probe): when scatter is disabled,
                // also suppress the piece-C advertisement consult so the GET-reach
                // measurement isolates PURE greedy routing (the consult is a
                // separate advertisement-based findability layer). NOT for ship.
                #[cfg(any(test, feature = "testing"))]
                let consult_suppressed = crate::operations::findability_probe::scatter_disabled();
                #[cfg(not(any(test, feature = "testing")))]
                let consult_suppressed = false;
                let consult_candidate = if last_forward_failed || !did_forward || consult_suppressed
                {
                    None
                } else {
                    let targets = consult_targets.get_or_insert_with(|| {
                        let tried_snapshot = tried.clone();
                        let visited_snapshot = new_visited.clone();
                        crate::operations::consult_advertised_hosts(
                            op_manager,
                            &instance_id,
                            MAX_TERMINAL_CONSULT_HOSTS,
                            move |addr| {
                                tried_snapshot.contains(&addr)
                                    || visited_snapshot.probably_visited(addr)
                            },
                        )
                        .into_iter()
                    });
                    targets.next()
                };
                if let Some((consult_peer, consult_addr)) = consult_candidate {
                    tried.push(consult_addr);
                    new_visited.mark_visited(consult_addr);
                    consult_active = true;
                    tracing::debug!(
                        tx = %incoming_tx,
                        %instance_id,
                        target = %consult_addr,
                        "GET relay: terminus consulting advertised host off routing path"
                    );
                    (consult_peer, consult_addr)
                } else {
                    // Dead-end confirmed: no usable advertised host (or every
                    // advertised host was already tried / also failed). Record
                    // a still-not-found outcome only if a consult actually ran
                    // (`consult_targets` was built) — not when it was skipped
                    // after a failed forward. A resolved consult would have
                    // returned from the Found arm above, so this is reached
                    // only when no consult forward succeeded.
                    if consult_targets.is_some() {
                        crate::operations::record_terminal_consult_outcome(false);
                    }
                    tracing::warn!(
                        tx = %incoming_tx,
                        %instance_id,
                        %upstream_addr,
                        phase = "not_found",
                        "GET relay: all peers exhausted — sending NotFound upstream"
                    );
                    // Exhaustion NotFound: this relay is reporting its own
                    // exhaustion of downstream candidates. hop_count is its
                    // forward depth (max_htl - htl).
                    let hop_count = op_manager.ring.max_hops_to_live.saturating_sub(htl);
                    // EXPERIMENT-ONLY (findability_probe): the pure-routing
                    // GET-reach terminus — greedy routing (+ consult) could not
                    // reach the copy from here. The closer-neighbor field splits
                    // connectivity-gap from routing-give-up. NOT for ship.
                    #[cfg(any(test, feature = "testing"))]
                    {
                        let visited = new_visited.clone();
                        let tried_snapshot = tried.clone();
                        crate::operations::findability_probe::record_terminus(
                            &op_manager.ring,
                            crate::operations::findability_probe::ProbeOpKind::Get,
                            &incoming_tx,
                            crate::ring::Location::from(&instance_id),
                            htl,
                            hop_count,
                            crate::operations::findability_probe::ProbeStopReason::RoutingExhausted,
                            move |addr| {
                                tried_snapshot.contains(&addr) || visited.probably_visited(addr)
                            },
                        );
                    }
                    if let Some(event) = crate::tracing::NetEventLog::get_not_found(
                        &incoming_tx,
                        &op_manager.ring,
                        instance_id,
                        Some(hop_count),
                    ) {
                        op_manager
                            .ring
                            .register_events(either::Either::Left(event))
                            .await;
                    }
                    relay_send_not_found(
                        op_manager,
                        incoming_tx,
                        instance_id,
                        upstream_addr,
                        hop_count,
                    )
                    .await;
                    return Ok(());
                }
            }
        };

        // NOTE: legacy path sends a ForwardingAck upstream before forwarding
        // downstream so the upstream's GC can extend its timer on slow
        // multi-hop chains. In the driver relay driver we OMIT that
        // ack: the ack carried `incoming_tx` (which equals the upstream's
        // `attempt_tx` on a relay-to-relay hop), so it matched the
        // upstream's capacity-1 `pending_op_results` waiter and arrived
        // FIRST, before the real Response. `classify` returned
        // `Unexpected`, causing upstream to abandon the downstream probe
        // and spawn a new attempt. That amplified to 3^HTL spawns per
        // origination under ci-fault-loss, driving the 63GB RSS explosion
        // (workflow runs 24600168871 / 24600634908 / 24601267577).
        //
        // Upstream's `send_to_and_await` still has its full OPERATION_TTL
        // (60s) to receive the real Response, which is the same timeout
        // legacy had minus the ack-driven extension. That extension is
        // only observable under chains longer than 60s wall-clock, which
        // `min_success_rate=0.80` tolerates at ci-fault-loss parameters.

        tracing::debug!(
            tx = %incoming_tx,
            %instance_id,
            target = %peer,
            target_addr = %peer_addr,
            phase = "forward",
            "GET relay: forwarding request to downstream peer"
        );

        // Emit get_request telemetry for the relay forward.
        let new_htl = htl.saturating_sub(1);
        if let Some(event) = crate::tracing::NetEventLog::get_request(
            &incoming_tx,
            &op_manager.ring,
            instance_id,
            peer.clone(),
            new_htl,
        ) {
            op_manager
                .ring
                .register_events(either::Either::Left(event))
                .await;
        }

        // Forward downstream using the INCOMING tx (end-to-end preservation,
        // matching legacy relay semantics). Minting a fresh `attempt_tx`
        // per retry was the 3^HTL amplifier: downstream peers saw every
        // retry as a brand-new Request, spawned a fresh relay subtree,
        // and the aggregate spawn rate reached 7M/100s in ci-fault-loss
        // (workflow run 24601908758). Reusing `incoming_tx` keeps the
        // pending_op_results callback bound to a single key per hop;
        // retries re-register the same callback after the previous
        // attempt's receiver was consumed by timeout or reply.
        let request = NetMessage::from(GetMsg::Request {
            id: incoming_tx,
            instance_id,
            fetch_contract,
            htl: new_htl,
            visited: new_visited.clone(),
            subscribe,
        });

        // Originator-loopback: when the relay driver runs on the
        // originator's own node, `incoming_tx == client_tx`. Calling
        // `send_to_and_await(peer_addr, ...)` would install a NEW
        // `pending_op_results[client_tx]` callback OVERWRITING the
        // originator's `start_client_get` waiter (`handle_op_execution`
        // calls `state.pending_op_results.insert(...)` which overwrites).
        // The peer's downstream Response would then land in the
        // relay-driver's callback and never reach the client driver.
        //
        // Fix: in originator-loopback, fire-and-forget the downstream
        // Request and exit without awaiting — `send_fire_and_forget`
        // dispatches a callback whose receiver is dropped, so
        // `handle_op_execution` skips the insert (closed-callback
        // guard at p2p_protoc.rs). The peer's eventual Response
        // returns over the wire, and the bypass at
        // `handle_pure_network_message_v1` forwards it to the
        // originator's still-installed callback. The client driver
        // owns retry semantics; the relay driver has nothing to add.
        // Mirrors `drive_relay_put`'s originator-loopback branch.
        let own_addr = op_manager.ring.connection_manager.get_own_addr();
        let originator_loopback = Some(upstream_addr) == own_addr;
        let mut ctx = op_manager.op_ctx(incoming_tx);
        if originator_loopback {
            if let Err(err) = ctx.send_fire_and_forget(peer_addr, request).await {
                tracing::warn!(
                    tx = %incoming_tx,
                    target = %peer,
                    error = %err,
                    "GET relay (loopback, loopback): \
                     send_fire_and_forget failed"
                );
                // Fall through to NotFound — client driver waiter
                // owns the retry decision.
                return Err(err);
            }
            // Exit driver: client driver owns the response handling.
            return Ok(());
        }

        let round_trip =
            tokio::time::timeout(OPERATION_TTL, ctx.send_to_and_await(peer_addr, request)).await;

        // Always release the slot so the next retry can re-register.
        op_manager.release_pending_op_slot(incoming_tx).await;

        let reply = match round_trip {
            Ok(Ok(reply)) => reply,
            Ok(Err(err)) => {
                tracing::warn!(
                    tx = %incoming_tx,
                    target = %peer,
                    error = %err,
                    "GET relay: send_to_and_await failed; advancing to next peer"
                );
                // Feed the relay's failed peer choice into the local Router
                // so future routing decisions de-prioritize this peer. Without
                // this hook, only originator-side failures train the router
                // and per-peer dashboard panels stay empty on relay-heavy nodes.
                crate::operations::record_relay_route_event(
                    op_manager,
                    peer.clone(),
                    crate::ring::Location::from(&instance_id),
                    crate::router::RouteOutcome::Failure,
                    crate::node::network_status::OpType::Get,
                );
                // No reply received — the awaited peer may reply late on the
                // reused tx, so do NOT consult after this (Codex P2).
                last_forward_failed = true;
                // Continue loop to try next peer.
                new_visited.mark_visited(peer_addr);
                continue;
            }
            Err(_elapsed) => {
                tracing::warn!(
                    tx = %incoming_tx,
                    target = %peer,
                    timeout_secs = OPERATION_TTL.as_secs(),
                    "GET relay: attempt timed out; advancing to next peer"
                );
                crate::operations::record_relay_route_event(
                    op_manager,
                    peer.clone(),
                    crate::ring::Location::from(&instance_id),
                    crate::router::RouteOutcome::Failure,
                    crate::node::network_status::OpType::Get,
                );
                // Timed out with no reply — the peer may reply late on the
                // reused tx, so do NOT consult after this (Codex P2).
                last_forward_failed = true;
                new_visited.mark_visited(peer_addr);
                continue;
            }
        };

        // A reply was received on `incoming_tx` (the only path past the
        // `round_trip` match), so the awaited peer will not reply late — it is
        // safe to consult after this attempt (Codex P2). Reset regardless of
        // how the reply classifies below.
        last_forward_failed = false;

        // Classify the reply.
        let attempt_outcome = classify(reply);
        // Terminal-consult telemetry is NOT recorded here: attributing a Found
        // reply from a consulted host at classify time would be premature,
        // because delivery of that Found can still fail by a fallible
        // side-effect below — `send_result?` for InlineFound, `claim_or_wait`
        // for Streaming (the #4345 "classify terminal before a fallible side
        // effect" anti-pattern). `resolved_found` is instead recorded inside
        // the InlineFound / Streaming arms AFTER that side-effect succeeds, so
        // a send/claim failure correctly leaves the op to record
        // still-not-found at the exhaustion arm.
        match attempt_outcome {
            AttemptOutcome::Terminal(Terminal::InlineFound {
                key,
                state,
                contract,
                hop_count: downstream_hop_count,
            }) => {
                tracing::info!(
                    tx = %incoming_tx,
                    %instance_id,
                    contract = %key,
                    phase = "relay_found",
                    "GET relay: downstream returned Found — forwarding upstream then caching locally"
                );

                // Feed the relay's successful peer choice into the local
                // Router. Without this hook, only originator-side successes
                // train the failure-probability model and per-peer dashboard
                // panels stay empty on relay-heavy nodes. In-memory only,
                // safe to run before forwarding.
                crate::operations::record_relay_route_event(
                    op_manager,
                    peer.clone(),
                    crate::ring::Location::from(&instance_id),
                    crate::router::RouteOutcome::SuccessUntimed,
                    crate::node::network_status::OpType::Get,
                );

                // Bubble up to upstream FIRST to keep cache off the
                // critical path (issue #4155 — full rationale at the
                // R4 callsite earlier in this fn). This is THE site
                // where the perf win matters: a freshly-arrived
                // downstream payload has no `state_matches`
                // short-circuit yet, so the PutQuery runs full WASM
                // `validate_state` + `update_state`. Caching ALWAYS
                // runs after forwarding so LRU/TTL and
                // `announce_contract_hosted` semantics are preserved
                // even on send error.
                // Preserve the storer-side hop_count so the originator
                // sees the *forward* depth from Request to storer, not
                // the depth from Request to this relay.
                let send_result = relay_send_found(
                    op_manager,
                    conn_manager,
                    incoming_tx,
                    instance_id,
                    upstream_addr,
                    key,
                    state.clone(),
                    contract.clone(),
                    downstream_hop_count,
                )
                .await;

                // Cache locally (relay opportunistically caches forwarded
                // Found payloads). `is_client_requester=false` so this node
                // does NOT set `mark_local_client_access` — the sticky flag
                // belongs to the upstream client-originating node, not a
                // forwarder. `announce_contract_hosted` DOES fire when
                // `access_result.is_new && put_persisted` so first-time
                // hosting at a relay is still broadcast (matches legacy
                // `get.rs:2370` which announces on any first-time relay
                // cache).
                let state_present =
                    cache_contract_locally(op_manager, key, state, contract, false).await;
                send_result?;

                // Register the requester as a downstream subscriber (subscribe
                // only), gated on actually holding state after the cache
                // (register-after-state, step 10 §1a). Registering only once we
                // hold state makes the phantom (contract_in_use && !state)
                // unrepresentable and closes the findability gap where a
                // subscribe=true GET relayed a Found through this hop but never
                // recorded the requester's demand (so updates never reached it).
                // On cache failure (put rejected / no contract code) we register
                // NOTHING; the chain hole heals on the requester's next renewal.
                if subscribe && state_present {
                    crate::operations::subscribe::register_downstream_subscriber(
                        op_manager,
                        &key,
                        upstream_addr,
                        None,
                        None,
                        &incoming_tx,
                        " (relay, inline Found GET response)",
                    )
                    .await;
                }
                // Delivery succeeded (`send_result?` did not return): a
                // consulted advertised host closed the dead-end. Record now,
                // AFTER the fallible upstream forward, not at classify time.
                // This arm returns, so exactly one consult outcome is recorded.
                if consult_active {
                    crate::operations::record_terminal_consult_outcome(true);
                }
                return Ok(());
            }
            AttemptOutcome::Terminal(Terminal::Streaming {
                key,
                stream_id,
                includes_contract,
                total_size,
            }) => {
                // Downstream answered with a streaming response. Restore the
                // relay fork+pipe forward (reverted #3586 / port plan §7,
                // issue #4307): claim the inbound stream, fork it, send the
                // metadata header upstream, then pipe fragments onward. The
                // relay caches a best-effort local copy from the held handle
                // AFTER initiating the pipe. Mirrors
                // `drive_relay_put_streaming`.
                let own_addr = op_manager.ring.connection_manager.get_own_addr();

                tracing::info!(
                    tx = %incoming_tx,
                    %instance_id,
                    contract = %key,
                    target = %peer,
                    %stream_id,
                    total_size,
                    phase = "relay_streaming_forward",
                    "GET relay: downstream returned ResponseStreaming — forwarding stream upstream"
                );

                // ── Step 1: Claim the inbound stream (atomic dedup). The
                //    DOWNSTREAM peer (`peer_addr`) is the responder.
                let handle = match op_manager
                    .orphan_stream_registry()
                    .claim_or_wait(peer_addr, stream_id, STREAM_CLAIM_TIMEOUT)
                    .await
                {
                    Ok(h) => h,
                    Err(OrphanStreamError::AlreadyClaimed) => {
                        // Dedup, NOT a routing failure: another task already
                        // owns this stream. Do NOT record a Failure route
                        // event here — the downstream peer behaved correctly.
                        tracing::debug!(
                            tx = %incoming_tx,
                            %stream_id,
                            "GET relay: inbound stream already claimed (dedup), skipping"
                        );
                        return Ok(());
                    }
                    Err(e) => {
                        tracing::warn!(
                            tx = %incoming_tx,
                            %stream_id,
                            target = %peer,
                            error = %e,
                            "GET relay: orphan stream claim failed; advancing to next peer"
                        );
                        // The downstream advertised a `ResponseStreaming`
                        // header but never delivered an assemblable stream
                        // (timeout / claim error) — a genuine routing failure
                        // for this peer. Train the router the same way the
                        // transport-failure arms above do, so a peer that
                        // stalls on stream delivery gets de-prioritised
                        // instead of being re-selected. (AlreadyClaimed above
                        // is deliberately NOT counted — that's dedup, not a
                        // failure.)
                        crate::operations::record_relay_route_event(
                            op_manager,
                            peer.clone(),
                            crate::ring::Location::from(&instance_id),
                            crate::router::RouteOutcome::Failure,
                            crate::node::network_status::OpType::Get,
                        );
                        // Treat as a failed attempt — try another peer.
                        new_visited.mark_visited(peer_addr);
                        continue;
                    }
                };

                // NOTE: the terminal-consult `resolved_found` for a streaming
                // reply is recorded only AFTER the payload is actually
                // delivered — the loopback cache (below) or the remote
                // `pipe_stream` success (Step 3). Recording here, right after
                // the claim, would be a false positive: assembly, the header
                // send, and the pipe can all still fail without the requester
                // receiving the Found result.

                // ── Step 2: Loopback safety net. If upstream IS us
                //    (originator-loopback edge — rare), assemble + cache
                //    locally instead of piping to self. This branch is
                //    cache-only with NO wire Response: relay-loopback
                //    initiators (GC-spawned retries / start_targeted_op
                //    auto-fetch) only need the local cache populated — the
                //    interactive client GET uses the separate
                //    `drive_client_get_inner` driver, which owns its own
                //    client delivery.
                if Some(upstream_addr) == own_addr {
                    match handle.assemble().await {
                        Ok(bytes) => match bincode::deserialize::<GetStreamingPayload>(&bytes) {
                            Ok(payload) => {
                                if let Some(state) = payload.value.state {
                                    let contract = if includes_contract {
                                        payload.value.contract
                                    } else {
                                        None
                                    };
                                    cache_contract_locally(
                                        op_manager,
                                        payload.key,
                                        state,
                                        contract,
                                        false,
                                    )
                                    .await;
                                    // Loopback delivery succeeded (state cached
                                    // locally): a consulted advertised host
                                    // closed the dead-end.
                                    if consult_active {
                                        crate::operations::record_terminal_consult_outcome(true);
                                    }
                                } else {
                                    tracing::warn!(
                                        tx = %incoming_tx,
                                        %stream_id,
                                        "GET relay (loopback): streamed payload has no state"
                                    );
                                }
                            }
                            Err(e) => {
                                tracing::warn!(
                                    tx = %incoming_tx,
                                    %stream_id,
                                    error = %e,
                                    "GET relay (loopback): failed to deserialize streamed payload"
                                );
                            }
                        },
                        Err(e) => {
                            tracing::warn!(
                                tx = %incoming_tx,
                                %stream_id,
                                error = %e,
                                "GET relay (loopback): stream assembly failed"
                            );
                        }
                    }
                    return Ok(());
                }

                // ── Step 3: Forward path (remote upstream). Fork, send
                //    metadata header, pipe — BEFORE any local assemble/cache.
                //    Re-key with a FRESH outbound stream_id per hop.
                let outbound_sid = StreamId::next_operations();
                let forked = handle.fork();
                let header = GetMsg::ResponseStreaming {
                    id: incoming_tx,
                    instance_id,
                    stream_id: outbound_sid,
                    key,
                    total_size,
                    includes_contract,
                };
                let header_net = NetMessage::from(header);
                // Serialize metadata for embedding in fragment #1 (fix #2757).
                let embedded = bincode::serialize(&header_net).ok().map(bytes::Bytes::from);

                // Committed to forking + piping to a remote upstream: count
                // this streaming-forward hop (test-only; see counter doc).
                #[cfg(any(test, feature = "testing"))]
                RELAY_GET_STREAMING_FORWARD_COUNT.fetch_add(1, std::sync::atomic::Ordering::SeqCst);

                // Send the metadata header BEFORE piping. The upstream
                // already has its waiter installed (it sent us the Request),
                // so a fire-and-forget header suffices.
                op_manager
                    .op_ctx(incoming_tx)
                    .send_fire_and_forget(upstream_addr, header_net)
                    .await
                    .map_err(|_| OpError::NotificationError)?;

                // `pipe_stream` spawns a background task and returns
                // promptly, so this does not block on the full transfer.
                //
                // Do NOT `?` this error: the `ResponseStreaming` header has
                // already gone upstream (see send above), so the upstream is
                // committed to assembling a stream. Propagating the error
                // would bubble up to `drive_relay_get`, which sends a
                // compensating `Response{NotFound}` to the SAME upstream — a
                // contradictory double-signal that stalls the upstream ~60s
                // on `claim_or_wait`. Instead, log and return cleanly so the
                // upstream's claim simply times out without a NotFound. We do
                // NOT fall through to the local-cache step: the held `handle`
                // may be partial after a pipe failure.
                if let Err(e) = conn_manager
                    .pipe_stream(upstream_addr, outbound_sid, forked, embedded)
                    .await
                {
                    tracing::warn!(
                        tx = %incoming_tx,
                        %upstream_addr,
                        %outbound_sid,
                        error = %e,
                        "GET relay: pipe_stream failed AFTER the ResponseStreaming \
                         header already went upstream; letting the upstream's claim \
                         time out cleanly rather than sending a contradictory NotFound"
                    );
                    return Ok(());
                }

                // Remote delivery committed: the ResponseStreaming header went
                // upstream and `pipe_stream` started successfully. A consulted
                // advertised host closed the dead-end — record now, AFTER the
                // fallible header send + pipe, not at claim time.
                if consult_active {
                    crate::operations::record_terminal_consult_outcome(true);
                }

                // ── Step 4: Record the relay route event for the downstream
                //    peer that served the stream (it behaved correctly).
                crate::operations::record_relay_route_event(
                    op_manager,
                    peer.clone(),
                    crate::ring::Location::from(&instance_id),
                    crate::router::RouteOutcome::SuccessUntimed,
                    crate::node::network_status::OpType::Get,
                );

                // ── Step 6: Best-effort local cache (legacy parity — relays
                //    cache forwarded payloads). `fork()` SHARES the buffer,
                //    so the held `handle` can still assemble. On error, log
                //    and continue — the pipe already succeeded. The returned
                //    bool (state-present-after) gates Step 7's registration.
                let state_present = match handle.assemble().await {
                    Ok(bytes) => match bincode::deserialize::<GetStreamingPayload>(&bytes) {
                        Ok(payload) => {
                            if let Some(state) = payload.value.state {
                                let contract = if includes_contract {
                                    payload.value.contract
                                } else {
                                    None
                                };
                                cache_contract_locally(
                                    op_manager,
                                    payload.key,
                                    state,
                                    contract,
                                    false,
                                )
                                .await
                            } else {
                                tracing::warn!(
                                    tx = %incoming_tx,
                                    %stream_id,
                                    "GET relay: streamed payload has no state; skipping local cache"
                                );
                                false
                            }
                        }
                        Err(e) => {
                            tracing::warn!(
                                tx = %incoming_tx,
                                %stream_id,
                                error = %e,
                                "GET relay: failed to deserialize streamed payload for local cache"
                            );
                            false
                        }
                    },
                    Err(e) => {
                        tracing::warn!(
                            tx = %incoming_tx,
                            %stream_id,
                            error = %e,
                            "GET relay: stream assembly failed for local cache (pipe already sent)"
                        );
                        false
                    }
                };

                // ── Step 7: Subscriber-forwarding parity (subscribe only).
                //    register-after-state (step 10 §1a): moved AFTER the Step 6
                //    cache and gated on `state_present`. Registering the
                //    requester as a downstream subscriber only once we actually
                //    hold state makes the phantom (contract_in_use && !state)
                //    unrepresentable. On stream-assembly failure we register
                //    NOTHING (was: register-before-cache, which left a phantom on
                //    assembly failure); the chain hole heals on the requester's
                //    next renewal — acceptable (D-CACHE-RET).
                if subscribe && state_present {
                    crate::operations::subscribe::register_downstream_subscriber(
                        op_manager,
                        &key,
                        upstream_addr,
                        None,
                        None,
                        &incoming_tx,
                        " (relay, streamed GET response)",
                    )
                    .await;
                }

                return Ok(());
            }
            AttemptOutcome::Terminal(Terminal::LocalCompletion) => {
                // A relay driver should never receive a Request-echo because
                // `send_to_and_await` targets a specific remote peer (not
                // loopback). If this arrives, it is a local protocol bug —
                // not a peer-routing failure. Do NOT record a route event.
                tracing::warn!(
                    tx = %incoming_tx,
                    %instance_id,
                    "GET relay: unexpected LocalCompletion (Request-echo) — trying next peer"
                );
                new_visited.mark_visited(peer_addr);
                continue;
            }
            AttemptOutcome::Retry => {
                // Downstream peer correctly answered NotFound. The peer
                // behaved well at the protocol level — it just doesn't host
                // this contract. Record `SuccessUntimed`: the failure-
                // probability model should treat this peer as healthy, even
                // though the relay tries another candidate for *this*
                // contract location.
                tracing::debug!(
                    tx = %incoming_tx,
                    target = %peer,
                    "GET relay: downstream returned NotFound; advancing to next peer"
                );
                crate::operations::record_relay_route_event(
                    op_manager,
                    peer.clone(),
                    crate::ring::Location::from(&instance_id),
                    crate::router::RouteOutcome::SuccessUntimed,
                    crate::node::network_status::OpType::Get,
                );
                // Mark the failed peer so future iterations don't re-select it.
                new_visited.mark_visited(peer_addr);
                // Loop iterates to next peer.
                continue;
            }
            AttemptOutcome::Unexpected => {
                // Unexpected reply variant — could be a local bug or a peer
                // misbehaviour. Without knowing which, do NOT record a route
                // event; the helper's invariant is one event per
                // unambiguously-attributable observation.
                tracing::warn!(
                    tx = %incoming_tx,
                    target = %peer,
                    "GET relay: unexpected reply variant; advancing to next peer"
                );
                new_visited.mark_visited(peer_addr);
                continue;
            }
        }
    }
}

// ── End of relay GET driver ──────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    fn dummy_key() -> ContractKey {
        ContractKey::from_id_and_code(ContractInstanceId::new([1u8; 32]), CodeHash::new([2u8; 32]))
    }

    fn dummy_tx() -> Transaction {
        Transaction::new::<GetMsg>()
    }

    /// `start_client_get` must acquire a `ClientOpGuard` before the
    /// `GlobalExecutor::spawn` and move it into the spawned future.
    /// See the sibling pin in `put/op_ctx_task.rs` for the full
    /// rationale (shutdown drain depends on the per-driver counter).
    #[test]
    fn start_client_get_acquires_inflight_guard_before_spawn() {
        let src = include_str!("op_ctx_task.rs");
        let entry = src
            .find("pub(crate) async fn start_client_get(")
            .expect("start_client_get must exist");
        let after_spawn = src[entry..]
            .find("GlobalExecutor::spawn(")
            .expect("start_client_get must spawn a driver task");
        let before_spawn = &src[entry..entry + after_spawn];
        assert!(
            before_spawn.contains("op_manager.admit_client_op()"),
            "start_client_get must call op_manager.admit_client_op() \
             before GlobalExecutor::spawn (atomic admission gate + \
             counter bump; closes the Codex r2 TOCTOU)."
        );
        assert!(
            before_spawn.contains("OpError::NodeShuttingDown"),
            "start_client_get must early-return OpError::NodeShuttingDown \
             on a refused admission."
        );
        let spawned = &src[entry + after_spawn..];
        let block_end = spawned
            .find("\n    Ok(client_tx)")
            .expect("start_client_get must return Ok(client_tx)");
        let spawn_block = &spawned[..block_end];
        assert!(
            spawn_block.contains("let _inflight_guard = inflight_guard;"),
            "the ClientOpGuard must be moved into the spawned future \
             via `let _inflight_guard = inflight_guard;`."
        );
    }

    /// Phase 7 egress self-block pin (#4300). `start_client_get` MUST
    /// reject a banned contract BEFORE spawning the driver task. Mirrors
    /// the receive-side `get_dispatch_gates_banned_contracts` pin in
    /// `ring/contract_ban_list.rs`. See the sibling pin in
    /// `put/op_ctx_task.rs` for the full rationale.
    #[test]
    fn start_client_get_gates_banned_contracts_before_spawn() {
        let src = include_str!("op_ctx_task.rs");
        let entry = src
            .find("pub(crate) async fn start_client_get(")
            .expect("start_client_get must exist");
        let after_spawn = src[entry..]
            .find("GlobalExecutor::spawn(")
            .expect("start_client_get must spawn a driver task");
        let before_spawn = &src[entry..entry + after_spawn];
        assert!(
            before_spawn.contains("reject_if_contract_banned"),
            "start_client_get must call reject_if_contract_banned() \
             before GlobalExecutor::spawn so a banned contract's GET is \
             rejected with a typed error instead of being driven to peers \
             (#4300 egress self-block)."
        );
    }

    // --- Bootstrap gateway fallback (#4361) ---
    //
    // The pure-selection unit tests for `select_bootstrap_gateway` now live
    // alongside the helper in `operations/bootstrap.rs`. The source pin below
    // stays here because it asserts on GET's own call sites.

    /// Source pin (#4361): the GET peer-selection sites — client
    /// initial pick, sub-op initial pick, client advance, relay advance —
    /// must all consult `bootstrap_gateway_target` when
    /// `k_closest_potentially_hosting` yields nothing. A refactor that
    /// drops any of these calls silently reintroduces the empty-ring
    /// instant-NotFound bug. Uses `extract_fn_body` (brace-matched) so
    /// the pins cannot false-pass on neighboring code or false-fail on
    /// comment growth.
    #[test]
    fn all_selection_sites_use_bootstrap_gateway_fallback() {
        let src = production_source();

        for entry in [
            "async fn drive_client_get_inner",
            "async fn drive_sub_op_get",
            "fn advance_to_next_peer",
            "fn relay_advance_to_next_peer",
        ] {
            let body = extract_fn_body(src, entry);
            assert!(
                body.contains("bootstrap_gateway_target("),
                "{entry} must consult bootstrap_gateway_target when \
                 k_closest_potentially_hosting yields no candidates (#4361)"
            );
        }

        // The relay fallback's exclusion predicate must respect the
        // request's visited bloom — dropping the `probably_visited` half
        // would silently reintroduce gateway<->relay bounce (cross-hop
        // loop prevention; testing review of #4364).
        let relay_body = extract_fn_body(src, "fn relay_advance_to_next_peer");
        assert!(
            relay_body.contains("new_visited.probably_visited"),
            "relay_advance_to_next_peer's fallback exclusion must include \
             new_visited.probably_visited (#4361 cross-hop loop prevention)"
        );

        // The client driver must carry its tried set into each new
        // attempt's visited bloom — without it the per-attempt loopback
        // relay re-picks the same first gateway and multi-gateway
        // failover never reaches the wire (skeptical review H1 on
        // #4364). The carry's exclusion semantics are behaviorally
        // pinned by the `carry_tried_into_visited_*` tests below.
        let nat_body = extract_fn_body(src, "fn new_attempt_tx");
        assert!(
            nat_body.contains("carry_tried_into_visited("),
            "GetRetryDriver::new_attempt_tx must carry tried addrs into \
             the fresh attempt_visited bloom so gateway failover reaches \
             the wire (#4361 / #4364 H1)"
        );
        assert!(
            nat_body.contains("connection_count()"),
            "the failover carry must stay gated on the empty-ring case so \
             normal-path retry routing semantics are unchanged (#4364)"
        );
    }

    /// Behavioral pin for the failover carry (#4364 H1): previously
    /// tried addrs must be excluded from the next attempt, while the
    /// attempt's intended destination must NOT be — marking it would
    /// make the loopback relay skip the client's own pick, and
    /// single-gateway retries would exhaust instantly.
    #[test]
    fn carry_tried_into_visited_excludes_current_target() {
        let tx = dummy_tx();
        let mut visited = VisitedPeers::new(&tx);
        let own = SocketAddr::from(([127, 0, 0, 1], 4000));
        let gw1 = SocketAddr::from(([127, 0, 0, 1], 4001));
        let gw2 = SocketAddr::from(([127, 0, 0, 1], 4002));

        carry_tried_into_visited(&mut visited, &[own, gw1, gw2], Some(gw2));

        assert!(
            visited.probably_visited(own),
            "own addr must be carried into the bloom"
        );
        assert!(
            visited.probably_visited(gw1),
            "a previously tried gateway must be excluded from the next attempt"
        );
        assert!(
            !visited.probably_visited(gw2),
            "the attempt's intended destination must NOT be excluded"
        );
    }

    /// With no current target (genuinely isolated retry), everything
    /// tried is carried.
    #[test]
    fn carry_tried_into_visited_marks_all_without_current_target() {
        let tx = dummy_tx();
        let mut visited = VisitedPeers::new(&tx);
        let own = SocketAddr::from(([127, 0, 0, 1], 4000));

        carry_tried_into_visited(&mut visited, &[own], None);

        assert!(visited.probably_visited(own));
    }

    #[test]
    fn classify_response_found_is_inline_terminal() {
        let tx = dummy_tx();
        let key = dummy_key();
        let msg = NetMessage::V1(NetMessageV1::Get(GetMsg::Response {
            id: tx,
            instance_id: *key.id(),
            result: GetMsgResult::Found {
                key,
                value: StoreResponse {
                    state: Some(WrappedState::new(vec![1u8])),
                    contract: None,
                },
            },
            hop_count: 0,
        }));
        assert!(matches!(
            classify(msg),
            AttemptOutcome::Terminal(Terminal::InlineFound { .. })
        ));
    }

    /// Regression pin: `classify` MUST extract the wire-carried hop_count
    /// from `Response{Found}` and propagate it into `Terminal::InlineFound`
    /// unchanged.  The relay bubble-up at line 2355 uses this field as the
    /// hop_count for the upstream Response; a regression that synthesised 0
    /// here (or dropped the field) would silently collapse the storer's
    /// forward-path depth at every relay, defeating the whole PR.  The
    /// bincode-roundtrip unit test in `get.rs` catches the wire format only.
    /// This pins the classifier side.
    #[test]
    fn classify_response_found_preserves_hop_count() {
        for hc in [0_usize, 1, 4, 10, 64] {
            let tx = dummy_tx();
            let key = dummy_key();
            let msg = NetMessage::V1(NetMessageV1::Get(GetMsg::Response {
                id: tx,
                instance_id: *key.id(),
                result: GetMsgResult::Found {
                    key,
                    value: StoreResponse {
                        state: Some(WrappedState::new(vec![1u8])),
                        contract: None,
                    },
                },
                hop_count: hc,
            }));
            match classify(msg) {
                AttemptOutcome::Terminal(Terminal::InlineFound { hop_count, .. }) => {
                    assert_eq!(
                        hop_count, hc,
                        "classifier must preserve hop_count={hc} unchanged"
                    );
                }
                AttemptOutcome::Terminal(_)
                | AttemptOutcome::Retry
                | AttemptOutcome::Unexpected => {
                    panic!("expected Terminal(InlineFound) for hop_count={hc}");
                }
            }
        }
    }

    #[test]
    fn classify_response_notfound_is_retry() {
        let tx = dummy_tx();
        let key = dummy_key();
        let msg = NetMessage::V1(NetMessageV1::Get(GetMsg::Response {
            id: tx,
            instance_id: *key.id(),
            result: GetMsgResult::NotFound,
            hop_count: 0,
        }));
        assert!(matches!(classify(msg), AttemptOutcome::Retry));
    }

    #[test]
    fn classify_response_streaming_is_streaming_terminal() {
        let tx = dummy_tx();
        let key = dummy_key();
        let msg = NetMessage::V1(NetMessageV1::Get(GetMsg::ResponseStreaming {
            id: tx,
            instance_id: *key.id(),
            stream_id: crate::transport::peer_connection::StreamId::next(),
            key,
            total_size: 1024,
            includes_contract: true,
        }));
        assert!(matches!(
            classify(msg),
            AttemptOutcome::Terminal(Terminal::Streaming { .. })
        ));
    }

    #[test]
    fn classify_forwarding_ack_is_unexpected() {
        let tx = dummy_tx();
        let key = dummy_key();
        let msg = NetMessage::V1(NetMessageV1::Get(GetMsg::ForwardingAck {
            id: tx,
            instance_id: *key.id(),
        }));
        assert!(
            matches!(classify(msg), AttemptOutcome::Unexpected),
            "ForwardingAck must NOT be classified as terminal"
        );
    }

    #[test]
    fn classify_response_streaming_ack_is_unexpected() {
        let tx = dummy_tx();
        let msg = NetMessage::V1(NetMessageV1::Get(GetMsg::ResponseStreamingAck {
            id: tx,
            stream_id: crate::transport::peer_connection::StreamId::next(),
        }));
        assert!(matches!(classify(msg), AttemptOutcome::Unexpected));
    }

    #[test]
    fn classify_request_echo_is_local_completion() {
        // When process_message completes locally (no next hop, contract
        // already cached), the Request is echoed back via
        // forward_pending_op_result_if_completed.
        let tx = dummy_tx();
        let key = dummy_key();
        let msg = NetMessage::V1(NetMessageV1::Get(GetMsg::Request {
            id: tx,
            instance_id: *key.id(),
            fetch_contract: true,
            htl: 5,
            visited: VisitedPeers::new(&tx),
            subscribe: false,
        }));
        assert!(matches!(
            classify(msg),
            AttemptOutcome::Terminal(Terminal::LocalCompletion)
        ));
    }

    #[test]
    fn classify_response_found_without_state_is_unexpected() {
        // Defensive: if a peer somehow returns Found but the inner
        // StoreResponse has no state, the driver must NOT build an
        // InlineFound Terminal with a missing state.
        let tx = dummy_tx();
        let key = dummy_key();
        let msg = NetMessage::V1(NetMessageV1::Get(GetMsg::Response {
            id: tx,
            instance_id: *key.id(),
            result: GetMsgResult::Found {
                key,
                value: StoreResponse {
                    state: None,
                    contract: None,
                },
            },
            hop_count: 0,
        }));
        assert!(matches!(classify(msg), AttemptOutcome::Unexpected));
    }

    #[test]
    fn classify_unexpected_for_non_get_message() {
        let tx = dummy_tx();
        let msg = NetMessage::V1(NetMessageV1::Aborted(tx));
        assert!(matches!(classify(msg), AttemptOutcome::Unexpected));
    }

    #[test]
    fn max_retries_boundary_exhausts_at_limit() {
        let mut retries: usize = 0;
        for _ in 0..MAX_RETRIES {
            assert!(retries < MAX_RETRIES, "should not exhaust before limit");
            retries += 1;
        }
        assert!(
            retries >= MAX_RETRIES,
            "should exhaust at MAX_RETRIES={MAX_RETRIES}"
        );
    }

    /// Regression for the peer-dashboard prediction-graph data-collection
    /// regression: the client GET driver must emit a TIMED
    /// `RouteOutcome::Success { time_to_response_start, payload_size,
    /// payload_transfer_time }` on the success path so the router's
    /// `response_start_time_estimator` and `transfer_rate_estimator`
    /// receive observations. Before this fix the driver always emitted
    /// `RouteOutcome::SuccessUntimed`, leaving the Response Time and
    /// Transfer Rate charts on the peer dashboard permanently empty.
    #[test]
    fn drive_client_get_inner_emits_timed_success_on_success_path() {
        let src = production_source();
        let inner_body = extract_fn_body(src, "async fn drive_client_get_inner(");
        let build_body =
            extract_fn_body(src, "fn build_request(&mut self, attempt_tx: Transaction)");
        let classify_body = extract_fn_body(src, "fn classify(&mut self, reply: NetMessage)");

        // Driver must capture send time at attempt construction. The
        // body must use `tokio::time::Instant::now()` — `std::time::Instant::now()`
        // is banned in `crates/core/` by Rule Lint (see
        // .claude/rules/code-style.md) AND would not virtualize under
        // `start_paused(true)` in DST simulation tests.
        assert!(
            build_body.contains("self.request_sent_at = Some(tokio::time::Instant::now())"),
            "GetRetryDriver::build_request must capture request_sent_at via \
             tokio::time::Instant::now(); without this the Done branch has \
             no `time_to_response_start`."
        );
        // Driver must capture receive time on Terminal classification.
        assert!(
            classify_body.contains("self.response_received_at = Some(tokio::time::Instant::now())"),
            "GetRetryDriver::classify must capture response_received_at on \
             Terminal via tokio::time::Instant::now(); without this the Done \
             branch has no `time_to_response_start`."
        );
        // Done branch must construct a timed Success with all three fields.
        assert!(
            inner_body.contains("RouteOutcome::Success {")
                && inner_body.contains("time_to_response_start")
                && inner_body.contains("payload_size")
                && inner_body.contains("payload_transfer_time"),
            "drive_client_get_inner Done branch must build RouteOutcome::Success \
             with time_to_response_start + payload_size + payload_transfer_time. \
             Emitting SuccessUntimed instead leaves the router's response-time \
             and transfer-rate estimators with zero observations forever \
             (Failure-Probability-only peer-dashboard regression)."
        );
        // SuccessUntimed remains as a fallback when timing was not captured
        // (e.g., Request-echo / LocalCompletion paths).
        assert!(
            inner_body.contains("RouteOutcome::SuccessUntimed"),
            "Done branch must keep a SuccessUntimed fallback for paths \
             without timing capture."
        );
        // The streaming path must use the wire-authoritative `total_size`
        // rather than re-querying the store after assembly. Re-querying
        // races a concurrent UPDATE on the same contract and can also
        // return 0 if the LRU evicted the entry under cache pressure —
        // both failure modes silently zero out the transfer-rate sample.
        assert!(
            inner_body.contains("payload_size = *total_size as usize"),
            "Streaming branch must use wire-authoritative total_size for \
             payload_size, not a post-assembly store re-query."
        );
        // The `Terminal::LocalCompletion` arm is a loopback Request-echo
        // with no wire round-trip. Emitting timed Success there would
        // poison the response-time model with sub-millisecond loopback
        // samples attributed to the selected target peer.
        assert!(
            inner_body.contains("matches!(terminal, Terminal::LocalCompletion)"),
            "Done branch must skip timed Success for Terminal::LocalCompletion \
             (loopback Request-echo — no network round-trip to measure)."
        );
    }

    /// Regression: when the GET retry loop exhausts all peers without a
    /// Terminal classification, no `RouteOutcome::Success` is emitted.
    /// A future "always emit a final route-event" refactor would silently
    /// poison the success estimator with timed entries from peers that
    /// returned NotFound for the whole retry chain.
    #[test]
    fn drive_client_get_inner_exhausted_does_not_emit_success() {
        let src = production_source();
        let inner_body = extract_fn_body(src, "async fn drive_client_get_inner(");
        let exhausted_pos = inner_body
            .find("RetryLoopOutcome::Exhausted(")
            .expect("Done/Exhausted match must exist");
        let exhausted_arm = &inner_body[exhausted_pos..];
        // Bound the arm to the next match-arm by clipping at the next
        // top-level `RetryLoopOutcome::` token.
        let arm_end = exhausted_arm[1..]
            .find("RetryLoopOutcome::")
            .map(|p| p + 1)
            .unwrap_or(exhausted_arm.len());
        let arm_body = &exhausted_arm[..arm_end];
        assert!(
            !arm_body.contains("RouteOutcome::Success {"),
            "Exhausted arm must NOT emit a timed RouteOutcome::Success — \
             that would poison the response-time/transfer-rate estimators \
             with synthetic data for peers that never produced a Terminal."
        );
        assert!(
            !arm_body.contains("routing_finished("),
            "Exhausted arm must NOT call routing_finished — no wire success \
             attribution is appropriate for a peer set that returned NotFound."
        );
    }

    #[test]
    fn driver_outcome_exhausted_produces_client_error() {
        let cause = "GET to contract failed after 3 attempts".to_string();
        let outcome: DriverOutcome = match RetryLoopOutcome::<()>::Exhausted(cause) {
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

    /// Extract the non-test source of `op_ctx_task.rs` by truncating
    /// at the `#[cfg(test)]` marker. Used by the bug-reproduction
    /// source-scrape tests below so that comments inside this test
    /// module don't create false positives.
    fn production_source() -> &'static str {
        const FULL: &str = include_str!("op_ctx_task.rs");
        let cutoff = FULL
            .find("#[cfg(test)]")
            .expect("file must have a #[cfg(test)] section");
        // The str literal outlives `cutoff`; slicing gives a &'static str.
        &FULL[..cutoff]
    }

    /// Isolate the body of a named function inside production source.
    fn extract_fn_body<'a>(source: &'a str, signature_prefix: &str) -> &'a str {
        let start = source
            .find(signature_prefix)
            .unwrap_or_else(|| panic!("could not find {signature_prefix}"));
        // Find the opening `{` of the body.
        let brace = source[start..].find('{').expect("fn sig must have body");
        let body_start = start + brace + 1;
        // Walk to the matching closing brace, tracking nesting.
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

    /// Pin (step 10 §1e): the client-GET driver MUST tear down an up-front client
    /// subscription on a dead-end (NotFound) so it does not linger as a phantom
    /// (`contract_in_use && !contract_state_present`) until the client
    /// disconnects. The `RetryLoopOutcome::Exhausted` arm (which publishes
    /// NotFound) must call `remove_client_subscription` BEFORE publishing, and the
    /// driver must thread `client_sub_cleanup`.
    #[test]
    fn client_get_driver_removes_client_subscription_on_dead_end() {
        let src = include_str!("op_ctx_task.rs");
        let body = extract_fn_body(src, "async fn drive_client_get_inner(");
        assert!(
            body.contains("client_sub_cleanup"),
            "drive_client_get_inner must thread client_sub_cleanup so a dead-end GET \
             can tear down the up-front client subscription (step 10 §1e)"
        );
        let exhausted = body
            .find("RetryLoopOutcome::Exhausted(")
            .expect("drive_client_get_inner must have an Exhausted arm");
        let notfound = body[exhausted..]
            .find("NotFound { instance_id }")
            .map(|o| exhausted + o)
            .expect("the Exhausted arm must publish NotFound");
        assert!(
            body[exhausted..notfound].contains("remove_client_subscription("),
            "the Exhausted (NotFound) arm must call remove_client_subscription BEFORE \
             publishing NotFound — else a dead-end subscribe=true GET leaves a phantom \
             client subscription (step 10 §1e)"
        );
    }

    /// Bug #3 reproduction: the legacy Response{Found} branch at
    /// `get.rs:2218-2241` reads the local store via
    /// `ContractHandlerEvent::GetQuery` FIRST, compares the stored bytes
    /// against the incoming `value`, and skips the `PutQuery` entirely
    /// when they match. This prevents re-invoking `update_state()` on
    /// contracts that implement idempotency checks (see #2018). The
    /// driver's `cache_contract_locally` must replicate
    /// this idempotency short-circuit.
    #[test]
    fn cache_contract_locally_has_state_matches_short_circuit() {
        let src = production_source();
        let body = extract_fn_body(src, "async fn cache_contract_locally(");
        // A proper idempotency short-circuit reads the local state
        // first (GetQuery BEFORE PutQuery) and compares bytes.
        let get_pos = body
            .find("ContractHandlerEvent::GetQuery")
            .unwrap_or(usize::MAX);
        let put_pos = body
            .find("ContractHandlerEvent::PutQuery")
            .unwrap_or(usize::MAX);
        let has_byte_compare = body.contains("as_ref() ==") || body.contains("state_matches");
        let has_short_circuit = get_pos < put_pos && has_byte_compare;
        assert!(
            has_short_circuit,
            "cache_contract_locally is missing the state_matches idempotency \
             short-circuit from the legacy Response{{Found}} branch \
             (get.rs:2218-2241). Without it the driver re-invokes PutQuery \
             on identical state — regressing issue #2018 for contracts \
             that enforce idempotency in update_state()."
        );
    }

    /// Bug #4 reproduction: the legacy branch at `get.rs:2420-2435`
    /// logs on PutQuery error but **continues** to run the hosting /
    /// interest / access-tracking side effects. Hosting LRU / TTL
    /// must refresh for ANY successful wire-level GET, not only when
    /// the local store write succeeded.
    ///
    /// After the fix the side effects live OUTSIDE the PutQuery match:
    /// the match result feeds a `put_persisted: bool` and
    /// `record_get_access` / `announce_contract_hosted` run after the
    /// match closes — reachable from state-matches, PutQuery-Ok, and
    /// PutQuery-Err paths alike. We pin that structure here by
    /// requiring the `record_get_access` call to appear AFTER the
    /// PutResponse match arms (identified by the `Err(err)` arm) in
    /// the source order.
    #[test]
    fn cache_contract_locally_runs_side_effects_on_put_error() {
        let src = production_source();
        let body = extract_fn_body(src, "async fn cache_contract_locally(");
        let err_arm = body
            .find("new_value: Err(")
            .expect("PutResponse Err arm must exist");
        let side_effect = body
            .find("record_get_access")
            .expect("record_get_access must be called");
        assert!(
            side_effect > err_arm,
            "record_get_access must run AFTER the PutResponse match \
             (outside both Ok and Err arms) so hosting LRU/TTL refresh on \
             any successful wire-level GET — including when the local \
             executor rejects the PutQuery. The legacy branch at \
             get.rs:2420-2435 continues these side effects on error; \
             the driver must match."
        );
    }

    /// Pin (PR #4734 Fix 1): the GET eviction handler must sync the
    /// InterestManager for any subscribed contract the subscriber-primary
    /// eviction shed + tore down. `teardown_evicted_in_use_contract` clears the
    /// hosting maps, but the InterestManager lives on `OpManager`, so a dropped
    /// `remove_evicted_in_use` call leaves ghost `interested_peers` /
    /// `peer_contracts` / `local_client_count` entries that mis-target UPDATE
    /// broadcasts and do NOT self-heal. A refactor that drops the call must trip
    /// this pin (the "manually-inlined side effect after a task-per-tx
    /// migration" class — see .claude/rules/bug-prevention-patterns.md).
    #[test]
    fn cache_contract_locally_syncs_interest_on_subscribed_eviction() {
        let src = production_source();
        let body = extract_fn_body(src, "async fn cache_contract_locally(");
        assert!(
            body.contains("evicted_in_use_teardown"),
            "GET eviction handler must consume access_result.evicted_in_use_teardown"
        );
        assert!(
            body.contains("remove_evicted_in_use"),
            "GET eviction handler must call interest_manager.remove_evicted_in_use \
             to sync the InterestManager after a subscribed eviction"
        );
    }

    /// Piece E (demand-driven hosting): GET-auto-subscribe was REMOVED.
    /// A successful GET with `subscribe=false` must NOT install a durable
    /// subscription — that is the "GET-auto-subscribe" anti-pattern
    /// (hosting-invariants, invariants 1 & 2). The driver must therefore
    /// never call `auto_subscribe_on_get_response`; the only subscribe path
    /// left is the explicit-`subscribe=true` `maybe_subscribe_child`. This
    /// pairs with `test_driver_inline_get_does_not_auto_subscribe`
    /// (integration), which asserts the requesting node does NOT subscribe.
    #[test]
    fn driver_does_not_auto_subscribe_on_get_response() {
        let src = production_source();
        assert!(
            !src.contains("auto_subscribe_on_get_response"),
            "GET must NOT auto-subscribe (piece E removed AUTO_SUBSCRIBE_ON_GET). \
             A GET with subscribe=false hosts on the return path only under the \
             demand gauge (cache_contract_locally); ongoing freshness requires an \
             explicit subscribe=true. do NOT re-add auto_subscribe_on_get_response \
             — see hosting-invariants."
        );
        // Belt-and-braces: catch a re-subscribe reintroduced via a DIFFERENT
        // mechanism than the removed helper. The GET driver installs durable
        // subscriptions ONLY through `maybe_subscribe_child` (a subscribe sub-op
        // spawned on the explicit subscribe=true path); it must never call
        // `ring.subscribe(` directly, which would re-open the GET-auto-subscribe
        // anti-pattern regardless of the helper name.
        assert!(
            !src.contains("ring.subscribe("),
            "GET driver must NOT call ring.subscribe( directly — the only durable \
             subscribe path is maybe_subscribe_child (explicit subscribe=true). A \
             direct ring.subscribe( in this driver re-introduces GET-auto-subscribe \
             under a new name — see hosting-invariants (invariants 1 & 2)."
        );
        // The explicit subscribe path must still be wired in.
        assert!(
            src.contains("maybe_subscribe_child"),
            "the explicit subscribe=true path (maybe_subscribe_child) must remain"
        );
    }

    /// Bug #5 reproduction (source-level): `record_op_result` in the
    /// Done arm must NOT be emitted unconditionally as success — if
    /// `build_host_response` returned `Err`, the telemetry should
    /// reflect failure. Otherwise the router's prediction model and
    /// the network_status dashboard say "GET succeeded" while the
    /// client sees an OperationError.
    #[test]
    fn record_op_result_reflects_host_result_outcome() {
        const SOURCE: &str = include_str!("op_ctx_task.rs");
        // Find the Done arm (the `RetryLoopOutcome::Done` branch).
        let done_arm_start = SOURCE
            .find("RetryLoopOutcome::Done(")
            .expect("Done arm must exist");
        let next_arm = SOURCE[done_arm_start..]
            .find("RetryLoopOutcome::Exhausted")
            .expect("Exhausted arm must follow");
        let arm = &SOURCE[done_arm_start..done_arm_start + next_arm];
        // Locate the record_op_result call inside the arm.
        let call_pos = arm
            .find("record_op_result")
            .expect("record_op_result must be called in Done arm");
        // Get the surrounding ~200 chars to inspect the success flag.
        let tail = &arm[call_pos..];
        let call_window = &tail[..tail.len().min(200)];
        // Unconditional `true` is the bug. A proper implementation
        // derives the success flag from `host_result.is_ok()` or a
        // similarly named value.
        let looks_unconditional = call_window.contains("true,") && !call_window.contains("is_ok()");
        assert!(
            !looks_unconditional,
            "record_op_result in the Done arm is passed an unconditional \
             `true`. The success flag must track `host_result.is_ok()` so \
             telemetry does not diverge from the client-visible outcome. \
             Call window: {call_window}"
        );
    }

    /// Non-bug: per #3757, the node ALWAYS requests contract code on
    /// the wire regardless of what the client asked for, so it can
    /// cache WASM for validation/hosting. The driver matches legacy
    /// `start_op` behaviour (`get.rs:59` hard-codes the same value).
    /// This test pins the intentional choice so a future refactor
    /// doesn't silently reintroduce client-flag pass-through.
    #[test]
    fn driver_hardcodes_fetch_contract_true_per_issue_3757() {
        let src = production_source();
        let build_body = extract_fn_body(
            src,
            "fn build_request(&mut self, attempt_tx: Transaction) -> NetMessage {",
        );
        assert!(
            build_body.contains("fetch_contract: true,"),
            "GetMsg::Request.fetch_contract must stay hard-coded `true` — \
             the node needs WASM for local validation/hosting regardless of \
             the client's return_contract_code preference (issue #3757 / \
             get.rs:52-55)."
        );
    }

    /// Bug #1 regression: `Terminal::Streaming` in the Done arm must
    /// invoke `assemble_and_cache_stream` so that streamed GET
    /// responses actually write the contract state into the local
    /// executor. Without this call, a cold-cache client GET of a
    /// \>threshold contract succeeds on the wire but leaves the
    /// originator's local store empty — the client gets
    /// `OperationError` via `build_host_response`'s re-query miss.
    ///
    /// The simulation-level driver-isolation tests for this path are
    /// `#[ignore]`'d pending infrastructure work (#3883); this
    /// source-scrape pins the wiring so the call can't be silently
    /// removed.
    #[test]
    fn streaming_terminal_calls_assemble_and_cache_stream() {
        let src = production_source();
        // Since #4345, assembly runs inside `drive_get_with_assembly_retry`
        // (so a failed assembly can advance to the next candidate and
        // re-enter the retry loop). Both drivers must route through the
        // wrapper, and the wrapper must perform the assemble+cache.
        let client_body = extract_fn_body(src, "async fn drive_client_get_inner(");
        assert!(
            client_body.contains("drive_get_with_assembly_retry("),
            "drive_client_get_inner must drive the loop via \
             `drive_get_with_assembly_retry`. Without it, cold-cache \
             streaming GETs return OperationError because nothing writes \
             the local store (bug #1 in PR #3884 review), and assembly \
             failures burn the whole GET without consuming the retry \
             budget (#4345)."
        );
        let sub_op_body = extract_fn_body(src, "async fn drive_sub_op_get(");
        assert!(
            sub_op_body.contains("drive_get_with_assembly_retry("),
            "drive_sub_op_get must drive the loop via \
             `drive_get_with_assembly_retry` — the sub-op path has the \
             same assembly-failure-burns-the-op gap as the client path \
             (#4345)."
        );
        let wrapper_body = extract_fn_body(src, "async fn drive_get_with_assembly_retry(");
        assert!(
            wrapper_body.contains("assemble_and_cache_stream("),
            "drive_get_with_assembly_retry must call \
             `assemble_and_cache_stream` on the Streaming terminal."
        );
    }

    /// #4345: a failed stream assembly must be a *retryable* attempt
    /// failure — advance to the next candidate, fresh attempt tx — and
    /// exhaustion must still surface as `Done` (OperationError), never
    /// as `Exhausted` (which the caller maps to a false `NotFound`:
    /// a streaming header proves the contract exists).
    #[test]
    fn assembly_failure_advances_with_fresh_tx_and_never_exhausts_to_notfound() {
        let src = production_source();
        let body = extract_fn_body(src, "async fn drive_get_with_assembly_retry(");

        // Anchor on the assembly call first so a future earlier
        // `Err(e)` arm (e.g. around the claim) can't silently shift
        // the slice this test inspects.
        let assemble_call = body
            .find("match assemble_and_cache_stream")
            .expect("wrapper must match on assemble_and_cache_stream");
        let err_arm = body[assemble_call..]
            .find("Err(e) =>")
            .map(|p| p + assemble_call)
            .expect("wrapper must handle assembly Err");
        let err_body = &body[err_arm..];

        // The routing penalty must land on the candidate whose header
        // never became a stream — capture it BEFORE advance() mutates
        // `current_target`.
        let capture_pos = err_body
            .find("let failed_target = driver.current_target.clone()")
            .expect("Err arm must capture the failing target");
        let advance_pos = err_body
            .find("driver.advance()")
            .expect("Err arm must call driver.advance()");
        assert!(
            capture_pos < advance_pos,
            "failed_target must be captured BEFORE driver.advance() — \
             advance() replaces current_target, so capturing after \
             penalizes the wrong (fresh) candidate."
        );

        // Re-entry must use a fresh attempt tx: reusing the previous tx
        // collides with the relay dedup gates (`active_relay_get_txs`)
        // while the failed attempt's relay chain is still draining.
        assert!(
            err_body.contains("attempt_tx = driver.new_attempt_tx()"),
            "assembly-failure retry must re-enter the loop with a fresh \
             attempt tx via driver.new_attempt_tx()"
        );

        // The advancing path must penalize the failed candidate (the
        // router only learns about header-without-stream peers from
        // this call) and must remember the header for the
        // wire-exhaustion conversion below.
        let next_arm = err_body
            .find("AdvanceOutcome::Next =>")
            .expect("Err arm must handle Next");
        let next_body = &err_body[next_arm
            ..err_body
                .find("AdvanceOutcome::Exhausted =>")
                .expect("Err arm must handle Exhausted")];
        assert!(
            next_body.contains("emit_get_route_failure("),
            "the advancing path must emit a route failure for the \
             failed candidate (gated on emit_route_failure_on_retry)"
        );
        assert!(
            next_body
                .contains("failed_header = Some((key, stream_id, includes_contract, total_size))"),
            "the advancing path must remember the streaming header so a \
             later wire exhaustion can't surface as a false NotFound"
        );

        // Exhaustion keeps the Done(Streaming) outcome. The wrapper
        // only reaches the assembly Err arm after `result` matched
        // `Done(Terminal::Streaming { .. })`, so `break result` returns
        // that original outcome unchanged.
        let exhausted_arm = err_body
            .find("AdvanceOutcome::Exhausted =>")
            .expect("Err arm must handle Exhausted");
        let exhausted_body = &err_body[exhausted_arm..];
        assert!(
            exhausted_body.contains("break result"),
            "exhausted assembly retries must break with the original \
             Done(Streaming) outcome (`break result`) so the client sees \
             OperationError, NOT RetryLoopOutcome::Exhausted (which maps \
             to a false NotFound for a contract that provably exists)."
        );
        assert!(
            !exhausted_body.contains("RetryLoopOutcome::Exhausted("),
            "exhausted assembly retries must not be converted into \
             RetryLoopOutcome::Exhausted"
        );
        // ...and the budget-exhausted path must NOT emit a route
        // failure: the caller's final route event (driven by the
        // failed host_result) already records the Failure for the
        // last target — emitting here too would double-count it.
        assert!(
            !exhausted_body.contains("emit_get_route_failure("),
            "the budget-exhausted path must not emit a per-retry route \
             failure — the caller's final Failure event covers the last \
             target (double-count otherwise)"
        );

        // Wire exhaustion AFTER a failed assembly must convert back to
        // the remembered Done(Streaming): a header proved the contract
        // exists, so RetryLoopOutcome::Exhausted (mapped to NotFound by
        // both callers) would be a false NotFound. This state is only
        // reachable post-#4345 (assembly failure re-enters the loop).
        let exhausted_passthrough = body
            .find("RetryLoopOutcome::Exhausted(cause) =>")
            .expect("wrapper must have an Exhausted pass-through arm");
        let passthrough_body = &body[exhausted_passthrough..];
        assert!(
            passthrough_body.contains(
                "if let Some((key, stream_id, includes_contract, total_size)) = failed_header"
            ),
            "the Exhausted pass-through must check failed_header"
        );
        assert!(
            passthrough_body.contains("break RetryLoopOutcome::Done(Terminal::Streaming {"),
            "wire exhaustion after a seen streaming header must convert \
             back to Done(Streaming) so the client sees OperationError, \
             not a false NotFound"
        );
    }

    /// #4345: the synthesized client error must carry the assembly
    /// failure cause when one occurred, and fall back to the generic
    /// store-lookup message otherwise. Behavioral coverage for the
    /// cause construction both callers use on the exhaustion path.
    #[test]
    fn synthesized_error_causes_carry_assembly_failure() {
        let instance_id = ContractInstanceId::new([0xC1; 32]);

        let with_assembly = synthesized_get_error_cause(
            &instance_id,
            Some("stream assembly: no fragments received within inactivity timeout"),
        );
        assert!(
            with_assembly.contains("stream assembly failed")
                && with_assembly.contains("no fragments received"),
            "assembly-exhaustion cause must be diagnostic: {with_assembly}"
        );

        let without = synthesized_get_error_cause(&instance_id, None);
        assert!(
            without.contains("local store lookup failed"),
            "fallback cause must keep the store-lookup message: {without}"
        );
        assert!(
            !without.contains("assembly"),
            "fallback cause must not mention assembly: {without}"
        );

        let sub_with = sub_op_not_found_cause(&instance_id, Some("injected failure"));
        assert!(
            sub_with.contains(&missing_state_cause(&instance_id))
                && sub_with.contains("injected failure"),
            "sub-op cause must append the assembly failure: {sub_with}"
        );
        assert_eq!(
            sub_op_not_found_cause(&instance_id, None),
            missing_state_cause(&instance_id)
        );
    }

    /// #4345: the injection budget is keyed by contract so parallel
    /// tests in one process can't consume each other's failures.
    #[test]
    fn assembly_fault_injection_is_per_key_and_bounded() {
        let key_a = ContractKey::from_id_and_code(
            ContractInstanceId::new([0xA1; 32]),
            CodeHash::new([0xA2; 32]),
        );
        let key_b = ContractKey::from_id_and_code(
            ContractInstanceId::new([0xB1; 32]),
            CodeHash::new([0xB2; 32]),
        );

        // Unarmed keys never fail.
        assert!(!assembly_fault_injection::consume(&key_b));

        assembly_fault_injection::inject_failures(key_a, 2);
        assert!(assembly_fault_injection::consume(&key_a));
        // Other keys are unaffected while a budget is armed.
        assert!(!assembly_fault_injection::consume(&key_b));
        assert!(assembly_fault_injection::consume(&key_a));
        // Budget exhausted → assembly succeeds again.
        assert!(!assembly_fault_injection::consume(&key_a));
    }

    /// Pure-data regression test for the streaming payload shape the
    /// driver deserializes. Locks down the invariant that
    /// `GetStreamingPayload` round-trips via bincode, so a regression
    /// that changes the wire format would break `assemble_and_cache_stream`
    /// loudly at this level instead of silently producing an empty
    /// store (bug #1 class).
    #[test]
    fn streaming_payload_round_trips_via_bincode() {
        let key = dummy_key();
        let state_bytes = vec![0x42u8; 512];
        let payload = GetStreamingPayload {
            key,
            value: StoreResponse {
                state: Some(WrappedState::new(state_bytes.clone())),
                contract: None,
            },
        };
        let encoded = bincode::serialize(&payload).expect("bincode encode");
        let decoded: GetStreamingPayload = bincode::deserialize(&encoded).expect("bincode decode");
        assert_eq!(decoded.key, key);
        assert_eq!(
            decoded.value.state.as_ref().map(|s| s.as_ref().to_vec()),
            Some(state_bytes),
            "state bytes must round-trip through the streaming payload"
        );
    }

    /// Bug #1 follow-through: `assemble_and_cache_stream` must claim
    /// the stream by `(peer_addr, stream_id)`, await assembly, and
    /// check the key matches before caching. The source-scrape
    /// verifies the function's structure hasn't been simplified in a
    /// way that would skip any of those steps.
    #[test]
    fn assemble_and_cache_stream_performs_claim_assemble_key_check() {
        let src = production_source();
        let body = extract_fn_body(src, "async fn assemble_and_cache_stream(");

        assert!(
            body.contains("orphan_stream_registry") && body.contains("claim_or_wait"),
            "assemble_and_cache_stream must claim the stream via \
             orphan_stream_registry().claim_or_wait()"
        );
        assert!(
            body.contains(".assemble()") && body.contains(".await"),
            "assemble_and_cache_stream must await stream assembly"
        );
        assert!(
            body.contains("GetStreamingPayload") && body.contains("bincode::deserialize"),
            "assemble_and_cache_stream must deserialize the payload \
             as GetStreamingPayload"
        );
        assert!(
            body.contains("payload.key != expected_key"),
            "assemble_and_cache_stream must verify the stream payload's \
             key matches the expected ContractKey — a mismatch would \
             silently cache the wrong contract under the expected key"
        );
        assert!(
            body.contains("cache_contract_locally"),
            "assemble_and_cache_stream must delegate the actual write \
             and hosting side effects to cache_contract_locally"
        );
    }

    /// Guard against subscribe firing when the client did not request it.
    /// Source-scrape to verify `maybe_subscribe_child` short-circuits on
    /// `!subscribe`. Mirrors the spirit of PUT 3a's
    /// `finalize_put_at_originator_never_subscribes_from_driver`.
    #[test]
    fn maybe_subscribe_child_short_circuits_on_false() {
        const SOURCE: &str = include_str!("op_ctx_task.rs");
        let fn_start = SOURCE
            .find("async fn maybe_subscribe_child(")
            .expect("maybe_subscribe_child must exist");
        let body = &SOURCE[fn_start..];
        body.find("if !subscribe {")
            .expect("maybe_subscribe_child must short-circuit on !subscribe");
    }

    // ── Relay driver source-scrape tests (#3883) ─────────────────────────────
    //
    // These tests validate the structural invariants of the relay GET driver
    // without requiring a full simulation harness.  They mirror the pattern
    // established by the client driver tests above.

    /// T1: HTL=0 short-circuit must send NotFound upstream without entering
    /// the retry loop.  The source must contain the htl==0 guard BEFORE the
    /// first `relay_advance_to_next_peer` call.
    #[test]
    fn relay_driver_htl_zero_guard_precedes_retry_loop() {
        let src = production_source();
        let body = extract_fn_body(src, "async fn drive_relay_get_inner<CB>(");
        let htl_guard = body
            .find("if htl == 0")
            .expect("drive_relay_get_inner must have an `if htl == 0` guard");
        let retry_loop = body
            .find("relay_advance_to_next_peer")
            .expect("drive_relay_get_inner must call relay_advance_to_next_peer");
        assert!(
            htl_guard < retry_loop,
            "HTL=0 guard must appear BEFORE the retry loop; \
             otherwise HTL exhaustion enters the retry loop and forwards unnecessarily"
        );
    }

    /// T2: Local cache short-circuit must send Found upstream without entering
    /// the retry loop.  The source must contain the local-cache check BEFORE
    /// `relay_advance_to_next_peer` and AFTER the HTL guard.
    #[test]
    fn relay_driver_local_cache_check_precedes_retry_loop() {
        let src = production_source();
        let body = extract_fn_body(src, "async fn drive_relay_get_inner<CB>(");
        let local_check = body
            .find("check_local_with_interest_gate")
            .expect("drive_relay_get_inner must call check_local_with_interest_gate");
        let retry_loop = body
            .find("relay_advance_to_next_peer")
            .expect("drive_relay_get_inner must call relay_advance_to_next_peer");
        assert!(
            local_check < retry_loop,
            "Local-cache check must appear BEFORE the retry loop; \
             otherwise a hosting relay forwards instead of answering immediately"
        );
    }

    /// T3: Interest gate in `check_local_with_interest_gate` — stale cache
    /// (no local interest) must NOT return a local_value (must go to fallback).
    #[test]
    fn check_local_with_interest_gate_has_interest_check() {
        let src = production_source();
        let body = extract_fn_body(src, "async fn check_local_with_interest_gate(");
        assert!(
            body.contains("has_local_interest"),
            "check_local_with_interest_gate must call `has_local_interest` to gate \
             whether stale cache is served immediately or deferred to network"
        );
        // The stale-cache branch must put the value into the fallback slot,
        // not into local_value.
        assert!(
            body.contains("None, Some("),
            "check_local_with_interest_gate must return (None, Some(fallback)) \
             when there is local state but no active interest"
        );
    }

    /// T4: Downstream `Response{Found}` must forward upstream FIRST,
    /// then cache locally. This pins the issue #4155 fix: caching runs
    /// WASM `validate_state` on the single-threaded contract_handling
    /// event loop and can take seconds. Forwarding first keeps cache
    /// time off the upstream-visible critical path. The relay STILL
    /// caches (just after forwarding) so local LRU/TTL and
    /// `announce_contract_hosted` semantics are preserved.
    ///
    /// Reverting this ordering — putting `cache_contract_locally`
    /// before `relay_send_found` — re-introduces the per-hop dwell
    /// latency that #4155 was opened to fix.
    #[test]
    fn relay_driver_forwards_upstream_before_caching_on_found_response() {
        let src = production_source();
        let body = extract_fn_body(src, "async fn drive_relay_get_inner<CB>(");
        // Find the InlineFound arm inside the loop.
        let found_arm = body
            .find("Terminal::InlineFound {")
            .expect("drive_relay_get_inner must handle Terminal::InlineFound");
        let tail = &body[found_arm..];
        // The arm must STILL cache locally — caching is opportunistic
        // but load-bearing for hosting LRU/TTL.
        let cache_pos = tail.find("cache_contract_locally").unwrap_or(usize::MAX);
        let send_pos = tail.find("relay_send_found").unwrap_or(usize::MAX);
        assert!(
            cache_pos != usize::MAX,
            "relay InlineFound arm must STILL call cache_contract_locally — \
             caching is opportunistic but load-bearing for LRU/TTL and \
             `announce_contract_hosted` semantics"
        );
        assert!(
            send_pos != usize::MAX,
            "relay InlineFound arm must call relay_send_found"
        );
        assert!(
            send_pos < cache_pos,
            "issue #4155: relay_send_found must be called BEFORE \
             cache_contract_locally in the InlineFound arm. Caching first \
             blocks the upstream-visible forward behind WASM validate_state \
             on the single-threaded contract_handling event loop, which is \
             the root cause of the per-hop dwell observed in #4155"
        );
    }

    /// T4b: All three relay-driver callsites must forward upstream
    /// BEFORE caching locally (issue #4155). Mirrors T4 for the other
    /// two relay paths that the source-scrape
    /// `all_relay_callsites_pass_is_client_requester_false` test already
    /// recognizes (R4 immediate-serve, R10 exhaustion fallback, R12a
    /// bubble-up).
    #[test]
    fn all_relay_callsites_forward_before_caching() {
        let src = production_source();
        let body = extract_fn_body(src, "async fn drive_relay_get_inner<CB>(");

        // For each relay-style block in the function, when both
        // `relay_send_found(` and `cache_contract_locally(` appear, the
        // forward must come first. We scan all paired occurrences.
        let send_positions: Vec<usize> = body
            .match_indices("relay_send_found(")
            .map(|(i, _)| i)
            .collect();
        let cache_positions: Vec<usize> = body
            .match_indices("cache_contract_locally(")
            .map(|(i, _)| i)
            .collect();
        assert_eq!(
            send_positions.len(),
            3,
            "drive_relay_get_inner should have exactly three \
             `relay_send_found(` callsites (R4 immediate, R10 fallback, \
             R12a bubble-up). Found {} — a refactor has changed the shape.",
            send_positions.len(),
        );
        assert_eq!(
            cache_positions.len(),
            5,
            "drive_relay_get_inner should have exactly five \
             `cache_contract_locally(` callsites (R4, R10, R12a, plus the \
             two #4307 streaming-arm caches). Found {} — a refactor has \
             changed the shape.",
            cache_positions.len(),
        );
        // Pair the three `relay_send_found` callsites (R4, R10, R12a) with
        // their corresponding inline-Found caches and verify each `send`
        // comes before its `cache`. The streaming-arm caches (positions
        // 4 and 5) forward via `pipe_stream` rather than `relay_send_found`
        // and are covered by `relay_streaming_arm_forwards_before_caching`,
        // so they are intentionally excluded from this positional pairing.
        // The three sends all precede the streaming arm in source order, so
        // zipping against the first three caches is correct.
        for (idx, (send_pos, cache_pos)) in send_positions
            .iter()
            .zip(cache_positions.iter())
            .enumerate()
        {
            assert!(
                send_pos < cache_pos,
                "issue #4155: relay callsite #{idx} must forward upstream \
                 (`relay_send_found`) BEFORE caching locally \
                 (`cache_contract_locally`). Found send at {send_pos}, \
                 cache at {cache_pos}."
            );
        }
    }

    /// T5: Exhaustion path must send NotFound upstream.
    #[test]
    fn relay_driver_exhaustion_sends_not_found_upstream() {
        let src = production_source();
        let body = extract_fn_body(src, "async fn drive_relay_get_inner<CB>(");
        // Find the exhaustion branch (None arm of relay_advance_to_next_peer).
        assert!(
            body.contains("relay_send_not_found"),
            "drive_relay_get_inner must call `relay_send_not_found` on exhaustion"
        );
    }

    /// Piece C (invariant 5): the relay terminus must consult neighbor host
    /// advertisements BEFORE declaring a NotFound dead-end. The consult call
    /// must appear before `relay_send_not_found` in source order so the
    /// advertised-host forward is attempted first.
    #[test]
    fn relay_terminus_consults_advertised_hosts_before_not_found() {
        let src = production_source();
        let body = extract_fn_body(src, "async fn drive_relay_get_inner<CB>(");
        let consult_pos = body.find("consult_advertised_hosts").expect(
            "drive_relay_get_inner must consult advertised hosts at the routing \
             terminus (hosting redesign piece C, invariant 5)",
        );
        // Anchor on the exhaustion dead-end marker (unique to the consult's
        // else branch); the driver has an earlier HTL-exhaustion NotFound, so
        // a bare `relay_send_not_found` anchor would match the wrong site.
        let deadend_pos = body
            .find("Dead-end confirmed")
            .expect("consult exhaustion branch must confirm the dead-end before NotFound");
        assert!(
            consult_pos < deadend_pos,
            "the terminal advertisement consult must run BEFORE the NotFound \
             send so an off-path advertised host gets a chance to answer"
        );
        assert!(
            body.contains("relay_send_not_found"),
            "drive_relay_get_inner must still send NotFound on a true dead-end"
        );
        // The consult outcome must be recorded so the telemetry measures
        // whether the consult closes dead-ends.
        assert!(
            body.contains("record_terminal_consult_outcome"),
            "drive_relay_get_inner must record the terminal consult outcome"
        );
    }

    /// Piece C P2 (Codex): the consult reuses `incoming_tx`, so it must NOT run
    /// after a forward that got NO reply (timeout / send-failure) — a late
    /// reply from the timed-out peer could satisfy the consult waiter and
    /// bubble a false NotFound. Pin that both failure arms flag
    /// `last_forward_failed` and the consult is gated on it.
    #[test]
    fn consult_skipped_after_failed_forward() {
        let src = production_source();
        let body = extract_fn_body(src, "async fn drive_relay_get_inner<CB>(");
        // Both no-reply arms set the guard.
        assert!(
            body.matches("last_forward_failed = true").count() >= 2,
            "both the send-failure and timeout arms must set last_forward_failed = true"
        );
        // A received reply clears it.
        assert!(
            body.contains("last_forward_failed = false"),
            "a received reply must reset last_forward_failed so the consult can run"
        );
        // The consult is gated on the guard.
        assert!(
            body.contains("if last_forward_failed"),
            "the consult must be gated on !last_forward_failed (skip after a no-reply forward)"
        );
    }

    /// T6 (superseded): Originally required `relay_send_forwarding_ack`
    /// to be emitted before the downstream send. That ack collided with
    /// the upstream relay driver's capacity-1 `pending_op_results`
    /// waiter (the ack carried `incoming_tx`, which equals the
    /// upstream's `attempt_tx` on a relay-to-relay hop). The waiter
    /// would fire on the ack, return `AttemptOutcome::Unexpected`, and
    /// cause the upstream to immediately retry with a fresh tx — which
    /// the downstream's dispatch gate saw as a brand-new Request and
    /// spawned another full relay subtree. Net amplification was
    /// 3^HTL spawns per origination, observed as 6.8M spawns and 63GB
    /// RSS in workflow runs 24600168871 / 24600634908 / 24601267577.
    ///
    /// Fix: drop the ack entirely from the driver relay driver.
    /// Upstream's `send_to_and_await` still has OPERATION_TTL (60s) to
    /// receive the real Response, which is what legacy effectively had
    /// minus the ack-driven timer extension.
    #[test]
    fn relay_driver_does_not_send_forwarding_ack() {
        let src = production_source();
        let body = extract_fn_body(src, "async fn drive_relay_get_inner<CB>(");
        assert!(
            !body.contains("relay_send_forwarding_ack"),
            "drive_relay_get_inner must NOT call relay_send_forwarding_ack \
             — the ack collided with upstream's attempt_tx waiter and \
             amplified spawns (workflow 24600634908: 6.8M spawns, 63GB RSS)"
        );
        // A literal `GetMsg::ForwardingAck { ... }` construction would reintroduce
        // the bug. Comments/doc-strings mentioning "ForwardingAck" are fine.
        assert!(
            !body.contains("GetMsg::ForwardingAck {"),
            "drive_relay_get_inner must not construct GetMsg::ForwardingAck — \
             dropped to break the spawn-amplification cycle"
        );
    }

    /// T7: `relay_advance_to_next_peer` must respect incoming `VisitedPeers`
    /// by using the bloom filter as the skip list for `k_closest_potentially_hosting`.
    #[test]
    fn relay_advance_uses_visited_bloom_filter_as_skip_list() {
        let src = production_source();
        let body = extract_fn_body(src, "fn relay_advance_to_next_peer(");
        assert!(
            body.contains("k_closest_potentially_hosting"),
            "relay_advance_to_next_peer must use k_closest_potentially_hosting"
        );
        assert!(
            body.contains("new_visited"),
            "relay_advance_to_next_peer must pass new_visited as the skip list \
             so the upstream's VisitedPeers is respected"
        );
    }

    /// T8: Classify reuse — relay driver must call `classify` (the same
    /// function the client driver uses) for reply classification.
    #[test]
    fn relay_driver_reuses_classify_function() {
        let src = production_source();
        let body = extract_fn_body(src, "async fn drive_relay_get_inner<CB>(");
        assert!(
            body.contains("classify(reply)"),
            "drive_relay_get_inner must call `classify(reply)` to classify \
             downstream replies — reusing the client driver's classify avoids \
             duplicate terminal-variant handling"
        );
    }

    /// T9 (commit 2 version): start_relay_get is now live — the #[allow(dead_code)]
    /// attribute must have been removed when dispatch was wired in node.rs.
    ///
    /// This test replaces the commit-1 version that asserted the attribute WAS
    /// present. Now it asserts the attribute is ABSENT so a future refactor can't
    /// accidentally re-suppress the live driver with #[allow(dead_code)].
    #[test]
    fn relay_entry_point_no_longer_annotated_dead_code_after_commit2() {
        let src = production_source();
        let fn_start = src
            .find("pub(crate) async fn start_relay_get<CB>(")
            .expect("start_relay_get must exist");
        // Look in the 500 chars before the fn signature for the attribute.
        let window_start = fn_start.saturating_sub(500);
        let window = &src[window_start..fn_start];
        assert!(
            !window.contains("#[allow(dead_code)]"),
            "start_relay_get must NOT be annotated with #[allow(dead_code)] in commit 2 \
             — the driver is now live (wired in node.rs dispatch). Remove the attribute \
             to keep dead-code warnings effective."
        );
    }

    // ── Coverage-gap tests for PR #3896 (follow-up to T1-T9) ────────────────
    //
    // The T1-T9 suite above covers top-level structural ordering (guard-before-
    // loop, cache-before-send, ack-before-request, etc). The tests below
    // cover the behavioral contracts T1-T9 do NOT pin:
    //
    //   - G1/G2: dispatch gates in node.rs (source_addr=None loopback and
    //     has_get_op=true both fall through to legacy)
    //   - A   : HTL=0 emits NotFound upstream frame (not just logs-and-
    //     returns)
    //   - G/R10: downstream NotFound → exhaustion path; stale-cache
    //     fallback answers Found instead of NotFound when local state
    //     exists but interest is absent
    //   - R7  : ForwardingAck emitted exactly once, not per retry
    //   - R8  : wire-side Request propagation carries htl-1 and
    //     updated visited bloom
    //   - R12a→R13: send_found failure → compensating NotFound in outer
    //     `drive_relay_get` error funnel
    //   - R12b: streaming-downstream currently WARN+continue (regression
    //     guard until follow-up PR migrates streaming relay)
    //   - N   : concurrent same-key GET: gate condition at dispatch is
    //     per-tx (has_get_op), NOT per-key (by design; documented)
    //
    // All tests below are source-scrape style to match the T1-T9 shape —
    // they catch accidental code reorders or deletions without needing a
    // full turmoil harness. Behavioral coverage for the happy path is
    // already provided by `test_get_routing_coverage_low_htl` and
    // `test_get_reliability_*` (nightly).

    // ── Dispatch shape pin ────────────────────────────────────────────────
    //
    // The GET arm in node.rs dispatches every wire variant
    // unconditionally to a driver. Originator loopback
    // (`source_addr=None`) is mapped to `upstream_addr=own_addr`.
    // Structural guard lives at
    // `node::tests::callback_forward_tests::get_branch_dispatches_relay_driver`.

    #[test]
    fn relay_dispatch_target_is_start_relay_get() {
        const NODE_RS: &str = include_str!("../../node.rs");
        let arm_start = NODE_RS
            .find("NetMessageV1::Get(ref op) => {")
            .expect("Get arm must exist in handle_pure_network_message_v1");
        let next_variant = NODE_RS[arm_start..]
            .find("NetMessageV1::Update(ref op) => {")
            .expect("Update arm must follow Get arm")
            + arm_start;
        let arm = &NODE_RS[arm_start..next_variant];
        assert!(
            arm.contains("get::op_ctx_task::start_relay_get("),
            "GET arm must dispatch fresh inbound Request to \
             `get::op_ctx_task::start_relay_get`."
        );
    }

    // ── A: HTL=0 actually sends a NotFound frame upstream ──────────────────

    /// The HTL=0 short-circuit in `drive_relay_get_inner` must call
    /// `relay_send_not_found(..., upstream_addr)` (not just log+return).
    /// If this frame isn't emitted, the upstream peer waits for the full
    /// `OPERATION_TTL` (60s) instead of seeing a fast NotFound — a
    /// regression would show up as slow tail-latency under HTL-pressure,
    /// not as a functional failure.
    #[test]
    fn htl_zero_guard_emits_not_found_upstream_frame() {
        let src = production_source();
        let body = extract_fn_body(src, "async fn drive_relay_get_inner<CB>(");
        // Isolate the `if htl == 0 { ... }` block (before the retry loop).
        let guard_start = body.find("if htl == 0").expect("htl==0 guard must exist");
        let after_guard = &body[guard_start..];
        // Closing brace of the guard block — walk until depth returns to 0.
        let body_open = after_guard
            .find('{')
            .expect("htl==0 guard must have a body");
        let bytes = after_guard.as_bytes();
        let mut depth: i32 = 1;
        let mut i = body_open + 1;
        while i < bytes.len() && depth > 0 {
            match bytes[i] {
                b'{' => depth += 1,
                b'}' => depth -= 1,
                _ => {}
            }
            i += 1;
        }
        let guard_block = &after_guard[body_open..i];
        assert!(
            guard_block.contains("relay_send_not_found"),
            "HTL=0 guard block must call `relay_send_not_found(...)` so the \
             upstream gets a prompt NotFound frame instead of hanging until \
             OPERATION_TTL. Guard body was: {guard_block}"
        );
        assert!(
            guard_block.contains("upstream_addr"),
            "HTL=0 NotFound must be sent to `upstream_addr`, not any other \
             peer. Guard body was: {guard_block}"
        );
        assert!(
            guard_block.contains("return Ok(())"),
            "HTL=0 guard must return early after sending NotFound; otherwise \
             execution would fall into the retry loop with htl=0."
        );
    }

    // ── G/R10: Exhaustion path — NotFound vs stale-cache fallback ──────────

    /// Exhaustion (None from `relay_advance_to_next_peer`) must branch on
    /// `local_fallback`: with stale local state, serve Found from that
    /// state (R10); without, send NotFound (G/R11). A regression where
    /// both branches send NotFound silently loses availability for stale-
    /// cache relays (they would forward then lose the fallback even though
    /// they still hold the state locally).
    #[test]
    fn exhaustion_branches_on_local_fallback_presence() {
        let src = production_source();
        let body = extract_fn_body(src, "async fn drive_relay_get_inner<CB>(");
        // The exhaustion (terminus) path disposes of the request in this order:
        //   1. serve a local stale-cache fallback if present (Found, R10);
        //   2. else consult neighbor host advertisements (piece C) and forward
        //      to an advertised off-path host;
        //   3. else send NotFound upstream.
        // Assert all three dispositions exist in that source order.
        assert!(
            body.contains("local_fallback.take()"),
            "Exhaustion arm must branch on `local_fallback`; otherwise the \
             stale-cache fallback semantic (R3d → R10) is lost."
        );
        let fallback_pos = body
            .find("serving local fallback")
            .expect("exhaustion must serve the stale local copy when `local_fallback` is Some");
        let consult_pos = body.find("consult_advertised_hosts").expect(
            "exhaustion must consult advertised hosts before NotFound (piece C, invariant 5)",
        );
        let not_found_pos = body
            .find("all peers exhausted — sending NotFound upstream")
            .expect("exhaustion must send NotFound when there is no fallback or advertised host");
        assert!(
            fallback_pos < consult_pos && consult_pos < not_found_pos,
            "Exhaustion order must be: serve local fallback (Found) → consult \
             advertised hosts → NotFound. Got fallback={fallback_pos}, \
             consult={consult_pos}, not_found={not_found_pos}."
        );
        assert!(
            body.contains("relay_send_found") && body.contains("relay_send_not_found"),
            "Exhaustion arm must be able to send both Found (fallback) and \
             NotFound (true dead-end)."
        );
    }

    /// `check_local_with_interest_gate` must return the local state in
    /// the fallback slot (not `local_value`) when the relay holds state
    /// but `has_local_interest` is false. This locks down the interest
    /// gate: hosting relays serve immediately, stale-cache relays defer
    /// to the network and hold local state as a fallback.
    #[test]
    fn interest_gate_returns_fallback_when_not_actively_hosting() {
        let src = production_source();
        let body = extract_fn_body(src, "async fn check_local_with_interest_gate(");
        // The interest-gated branch structure must be:
        //   if !has_local_interest(&key) {
        //     (None, Some((key, state, contract)))
        //   } else {
        //     (Some((key, state, contract)), None)
        //   }
        let neg_gate = body
            .find("if !op_manager.interest_manager.has_local_interest(&key)")
            .expect("interest gate must check `!has_local_interest` branch explicitly");
        let tail = &body[neg_gate..];
        let stale_pos = tail.find("(None, Some(").expect("stale branch must exist");
        let active_pos = tail.find("(Some(").expect("active branch must exist");
        // active branch (Some(...), None) must come AFTER the stale branch in
        // the `else` arm.
        assert!(
            stale_pos < active_pos,
            "`if !has_local_interest` arm (stale → fallback slot) must appear \
             before the `else` arm (active → local_value slot) in source order."
        );
    }

    // ── R7 (superseded): ForwardingAck removed from driver relay ──────
    //
    // Superseded by `relay_driver_does_not_send_forwarding_ack` (T6
    // rewrite). The original latch logic was pinned here to ensure the
    // ack fired exactly once per relay invocation, which was the legacy
    // behavior. In the migration the ack's `id` collides
    // with the upstream relay driver's `attempt_tx` on its capacity-1
    // `pending_op_results` waiter — causing 3^HTL spawn amplification.
    // The ack is now never sent; see the T6 test for the invariant.

    // ── R8: Wire-side Request propagation (htl-1 + updated visited) ────────

    /// Each retry iteration must build a downstream `GetMsg::Request` with:
    ///   - `htl: new_htl` where `new_htl = htl.saturating_sub(1)`
    ///   - `visited: new_visited.clone()` (the bloom filter containing
    ///     own_addr and upstream_addr plus the caller's skip set)
    ///   - `id: incoming_tx` (reused across retries — matches legacy
    ///     end-to-end tx preservation; see the amplification comment
    ///     on that line for why the original per-iteration `attempt_tx`
    ///     caused 7M spawns / 100s in ci-fault-loss)
    ///
    /// Without `new_visited.clone()` the downstream relay's skip list is
    /// missing upstream hops, leading to loop formation.
    #[test]
    fn forwarded_request_decrements_htl_and_propagates_visited() {
        let src = production_source();
        let body = extract_fn_body(src, "async fn drive_relay_get_inner<CB>(");
        // The forwarded Request must be built with the decremented HTL.
        assert!(
            body.contains("let new_htl = htl.saturating_sub(1);"),
            "Retry loop must compute `new_htl = htl.saturating_sub(1)` so \
             the forwarded Request carries one-less HTL — otherwise the \
             downstream relay would forward with the same HTL and loops \
             could form."
        );
        // The forwarded Request body must thread `new_htl` and `new_visited`.
        let request_build = body
            .find("NetMessage::from(GetMsg::Request")
            .expect("retry loop must build the forwarded GetMsg::Request");
        // Take the next ~400 chars after the `GetMsg::Request {` opening as
        // the struct-literal window.
        let window = &body[request_build..(request_build + 400).min(body.len())];
        assert!(
            window.contains("htl: new_htl"),
            "Forwarded GetMsg::Request must set `htl: new_htl` (not `htl`) so \
             the decremented value actually propagates on the wire. Window: {window}"
        );
        assert!(
            window.contains("visited: new_visited.clone()"),
            "Forwarded GetMsg::Request must set `visited: new_visited.clone()` \
             so the downstream sees the updated skip set — without this, \
             the downstream could forward back to peers the upstream already \
             tried, forming routing loops. Window: {window}"
        );
        assert!(
            window.contains("id: incoming_tx"),
            "Forwarded GetMsg::Request must reuse `id: incoming_tx` (legacy \
             end-to-end tx preservation). Minting a fresh tx per retry (the \
             original phase-5 implementation) caused downstream to treat each \
             retry as a brand-new Request and spawn a fresh relay subtree, \
             producing 7M spawns in 100s of ci-fault-loss and 63GB RSS."
        );
    }

    /// The `new_visited` bloom filter must be seeded with `own_addr` and
    /// `upstream_addr` BEFORE the retry loop runs, so
    /// `k_closest_potentially_hosting` can't select the upstream (loop) or
    /// this peer (self-forward). Regression risk: if either mark is
    /// missing, routing may loop back immediately.
    #[test]
    fn visited_bloom_seeded_with_own_and_upstream_before_loop() {
        let src = production_source();
        let body = extract_fn_body(src, "async fn drive_relay_get_inner<CB>(");
        let loop_start = body.find("loop {").expect("retry loop must exist");
        let pre_loop = &body[..loop_start];
        assert!(
            pre_loop.contains("new_visited.mark_visited(upstream_addr);"),
            "`new_visited.mark_visited(upstream_addr)` must run BEFORE the \
             retry loop so downstream routing can't immediately hand back \
             to upstream."
        );
        // own_addr is guarded by `if let Some(...)` so check for the mark call.
        assert!(
            pre_loop.contains("new_visited.mark_visited(own_addr);"),
            "`new_visited.mark_visited(own_addr)` must run BEFORE the retry \
             loop so we don't route back to ourselves."
        );
    }

    // ── R12a→R13: send_found failure → compensating NotFound ───────────────

    /// When `relay_send_found` fails (serialization or transport error
    /// surfaced as `OpError::NotificationError`), the `?` propagation in
    /// `drive_relay_get_inner` surfaces the error to `drive_relay_get`,
    /// which must catch it and send a compensating NotFound upstream —
    /// otherwise the upstream waits for the full OPERATION_TTL on an op
    /// the relay has already abandoned.
    #[test]
    fn drive_relay_get_sends_compensating_not_found_on_inner_err() {
        let src = production_source();
        let body = extract_fn_body(src, "async fn drive_relay_get<CB>(");
        // Expected shape:
        //   match drive_relay_get_inner(...).await {
        //     Ok(()) => Ok(()),
        //     Err(err) => {
        //       tracing::warn!(...);
        //       relay_send_not_found(...).await;
        //       Err(err)
        //     }
        //   }
        let err_arm = body
            .find("Err(err) =>")
            .expect("drive_relay_get must have an Err arm");
        let tail = &body[err_arm..];
        assert!(
            tail.contains("relay_send_not_found"),
            "`drive_relay_get` Err arm must call `relay_send_not_found` so \
             that an inner infrastructure error (including `relay_send_found` \
             failures surfaced via `?`) produces a compensating NotFound \
             upstream instead of a silent 60s upstream timeout."
        );
        assert!(
            tail.contains("upstream_addr"),
            "The compensating NotFound in `drive_relay_get` must target \
             `upstream_addr`, not any other peer."
        );
        assert!(
            tail.contains("Err(err)"),
            "`drive_relay_get` Err arm must re-raise `Err(err)` after \
             sending the compensating NotFound, so `run_relay_get`'s \
             infra-error log still fires."
        );
    }

    /// `relay_send_found` must map fire-and-forget send failures to
    /// `OpError::NotificationError` so the `?` in `drive_relay_get_inner`
    /// surfaces them to the outer error funnel. A `?` on an `Ok` unit
    /// type would be a dead propagation; a non-OpError return would
    /// break the error funnel.
    #[test]
    fn relay_send_found_maps_send_failure_to_op_error() {
        let src = production_source();
        let body = extract_fn_body(src, "async fn relay_send_found<CB>(");
        assert!(
            body.contains("map_err(|_| OpError::NotificationError)"),
            "`relay_send_found` must map fire-and-forget send errors to \
             `OpError::NotificationError` so `?` surfaces them to the outer \
             `drive_relay_get` error funnel and triggers the compensating \
             NotFound."
        );
    }

    // ── #4307: Relay streaming-forward regression guards ───────────────────

    /// `relay_send_found` must stream large payloads to a remote upstream
    /// instead of falling back to inline store-and-forward. Regression
    /// guard for #4307 (the task-per-tx migration dropped this producer).
    /// Mirrors PUT's producer pin.
    #[test]
    fn relay_send_found_streams_large_payload_to_remote() {
        let src = production_source();
        let body = extract_fn_body(src, "async fn relay_send_found<CB>(");
        assert!(
            body.contains("should_use_streaming"),
            "`relay_send_found` must gate the remote-upstream path on \
             `should_use_streaming` so large payloads stream (#4307)."
        );
        assert!(
            body.contains("ResponseStreaming"),
            "`relay_send_found` must emit a `GetMsg::ResponseStreaming` \
             metadata header on the streaming branch (#4307)."
        );
        assert!(
            body.contains("conn_manager") && body.contains("send_stream"),
            "`relay_send_found` must call `conn_manager.send_stream(..)` to \
             push raw fragments on the streaming branch (#4307)."
        );
    }

    /// The relay `Terminal::Streaming` arm must FORWARD (send the metadata
    /// header + pipe fragments) BEFORE assembling/caching locally — the
    /// #4155 forward-before-cache invariant applied to the streaming path.
    #[test]
    fn relay_streaming_arm_forwards_before_caching() {
        let src = production_source();
        let body = extract_fn_body(src, "async fn drive_relay_get_inner<CB>(");
        let arm_start = body
            .find("AttemptOutcome::Terminal(Terminal::Streaming {")
            .expect("Streaming arm must exist in relay driver");
        let tail = &body[arm_start..];
        let pipe = tail
            .find("pipe_stream")
            .expect("Streaming arm must call pipe_stream to forward upstream");
        // The forward-path assemble/cache (best-effort local cache after
        // the pipe) are the LAST occurrences in the arm. The loopback
        // safety-net assemble/cache appear earlier but are inside a
        // separate `return Ok(())` branch (upstream == own_addr), so we
        // anchor on the final occurrences to verify the FORWARD path
        // forwards before it caches.
        let assemble = tail
            .rfind(".assemble()")
            .expect("Streaming arm must assemble for local cache");
        let cache = tail
            .rfind("cache_contract_locally")
            .expect("Streaming arm must cache locally");
        assert!(
            pipe < assemble,
            "Streaming arm forward path must `pipe_stream` BEFORE blocking \
             on `handle.assemble()` (forward-before-cache, #4155/#4307)."
        );
        assert!(
            pipe < cache,
            "Streaming arm forward path must forward (pipe_stream) BEFORE \
             `cache_contract_locally` (forward-before-cache, #4155/#4307)."
        );
    }

    /// The relay `Terminal::Streaming` arm must re-key the outbound stream
    /// with a FRESH `StreamId::next_operations()` and `fork()` the inbound
    /// handle so it can both pipe and cache. Never reuse the inbound id.
    #[test]
    fn relay_streaming_arm_uses_fresh_outbound_stream_id() {
        let src = production_source();
        let body = extract_fn_body(src, "async fn drive_relay_get_inner<CB>(");
        let arm_start = body
            .find("AttemptOutcome::Terminal(Terminal::Streaming {")
            .expect("Streaming arm must exist in relay driver");
        let tail = &body[arm_start..];
        let clip = tail[1..]
            .find("AttemptOutcome::")
            .map(|p| p + 1)
            .unwrap_or(tail.len());
        let arm = &tail[..clip];
        assert!(
            arm.contains("let outbound_sid = StreamId::next_operations()"),
            "Streaming arm must mint a fresh outbound stream id via \
             `let outbound_sid = StreamId::next_operations()` per hop (#4307)."
        );
        assert!(
            arm.contains(".fork()"),
            "Streaming arm must `fork()` the inbound handle so it can pipe \
             AND assemble for local cache (#4307)."
        );
        // The fresh `outbound_sid` (NOT the inbound `stream_id`) must be the
        // one re-keyed into the forwarded header AND piped. Without these the
        // arm could mint `outbound_sid` yet still pipe/advertise the inbound
        // id — exactly the re-keying bug this pin guards against.
        assert!(
            arm.contains("stream_id: outbound_sid"),
            "Streaming arm must re-key the forwarded `ResponseStreaming` \
             header with the FRESH `outbound_sid` (`stream_id: outbound_sid`), \
             not the inbound `stream_id` (#4307)."
        );
        assert!(
            arm.contains(".pipe_stream(upstream_addr, outbound_sid"),
            "Streaming arm must pipe the forked handle under the FRESH \
             `outbound_sid` (`pipe_stream(upstream_addr, outbound_sid, ..)`), \
             not the inbound `stream_id` (#4307)."
        );
    }

    /// Pin the deliberate non-`?` handling of a `pipe_stream` failure in the
    /// relay `Terminal::Streaming` arm. Once the `ResponseStreaming` header
    /// has gone upstream, the upstream is committed to assembling a stream;
    /// propagating the pipe error would bubble to `drive_relay_get` and send
    /// a compensating `Response{NotFound}` to that SAME upstream — a
    /// contradictory double-signal that stalls it ~60s on `claim_or_wait`.
    /// The arm therefore logs and `return Ok(())` instead. This invariant is
    /// hard to exercise behaviourally (the mock bridge's `pipe_stream` can't
    /// fail), so pin the SHAPE: a future refactor flipping `return Ok(())`
    /// to `?`/`return Err(..)`/a NotFound send here would silently regress.
    #[test]
    fn relay_streaming_arm_pipe_failure_returns_ok_without_notfound() {
        let src = production_source();
        let body = extract_fn_body(src, "async fn drive_relay_get_inner<CB>(");
        let arm_start = body
            .find("AttemptOutcome::Terminal(Terminal::Streaming {")
            .expect("Streaming arm must exist in relay driver");
        let tail = &body[arm_start..];
        let clip = tail[1..]
            .find("AttemptOutcome::")
            .map(|p| p + 1)
            .unwrap_or(tail.len());
        let arm = &tail[..clip];

        // Isolate the `if let Err(e) = conn_manager.pipe_stream(..) { .. }`
        // block: from the `pipe_stream` call to the end of the arm.
        let pipe_pos = arm
            .find(".pipe_stream(upstream_addr, outbound_sid")
            .expect("Streaming arm must call pipe_stream on the forward path");
        let pipe_tail = &arm[pipe_pos..];
        // The error-handling block runs until Step 4 (route-event recording).
        let block_end = pipe_tail
            .find("record_relay_route_event")
            .expect("route-event recording must follow the pipe on success");
        let err_block = &pipe_tail[..block_end];

        assert!(
            err_block.contains("if let Err(e) = conn_manager")
                || arm[..pipe_pos].contains("if let Err(e) = conn_manager"),
            "pipe_stream must be guarded by an `if let Err(e) = ..` block, \
             not `?`-propagated (the header already went upstream)."
        );
        assert!(
            err_block.contains("return Ok(())"),
            "On `pipe_stream` failure AFTER the header was sent upstream, the \
             streaming arm MUST `return Ok(())` (let the upstream's claim time \
             out cleanly), NOT propagate the error — a propagated error makes \
             `drive_relay_get` send a contradictory `Response{{NotFound}}` to \
             the same upstream and stalls it ~60s. #4307."
        );
        assert!(
            !err_block.contains("relay_send_not_found"),
            "The `pipe_stream`-failure block must NOT send a NotFound \
             upstream — the upstream is already committed to a stream."
        );
    }

    // ── R12b (SUPERSEDED): streaming-downstream WARN+continue guard ────────

    /// SUPERSEDED by #4307: relay streaming-forward IS now implemented (the
    /// `Terminal::Streaming` arm forks + pipes the stream upstream), so the
    /// WARN+skip invariant this test pinned no longer holds. Kept as
    /// historical documentation per `git-workflow.md` ("superseded by a
    /// semantic change → add `#[ignore]`, keep as historical documentation").
    /// The behaviour it used to assert is now inverted and pinned by
    /// `relay_send_found_streams_large_payload_to_remote`,
    /// `relay_streaming_arm_forwards_before_caching`,
    /// `relay_streaming_arm_uses_fresh_outbound_stream_id`, and
    /// `relay_streaming_arm_pipe_failure_returns_ok_without_notfound`. This
    /// test references the pre-#4307 non-generic `drive_relay_get_inner`
    /// signature and the old WARN+skip arm on purpose — it is a snapshot of
    /// the prior contract, not a live guard, and is never executed.
    #[test]
    #[ignore = "superseded: relay streaming forwarding now implemented (#4307); kept as historical documentation per git-workflow.md"]
    fn streaming_downstream_is_currently_warned_and_skipped() {
        let src = production_source();
        let body = extract_fn_body(src, "async fn drive_relay_get_inner(");
        let streaming_arm_start = body
            .find("AttemptOutcome::Terminal(Terminal::Streaming { .. })")
            .expect("Streaming arm must exist in relay driver");
        let tail = &body[streaming_arm_start..];
        // Bound at the next arm start.
        let clip = tail[1..]
            .find("AttemptOutcome::")
            .map(|p| p + 1)
            .unwrap_or(tail.len());
        let arm = &tail[..clip];
        assert!(
            arm.contains("continue;"),
            "Streaming arm must `continue;` to try the next peer — streaming \
             relay forwarding is out of scope for #3883 (see port plan §7). \
             A future PR will replace this with a proper chunk pipe-through."
        );
        assert!(
            arm.contains("new_visited.mark_visited(peer_addr)"),
            "Streaming arm must mark the streaming peer as visited so the \
             retry loop doesn't re-select it next iteration."
        );
        assert!(
            !arm.contains("relay_send_found"),
            "Streaming arm must NOT call `relay_send_found` — doing so \
             without the chunk payload would send a Found frame with \
             `state: None` (Unexpected at the upstream classify). See R12b \
             gap in port plan risk register."
        );
        assert!(
            !arm.contains("orphan_stream_registry"),
            "Streaming arm must NOT claim the stream via \
             `orphan_stream_registry` — that's the migrated behavior for a \
             follow-up PR. Doing it here without the upstream pipe would \
             leak stream state."
        );
    }

    // ── R9: send_and_await Err/timeout outcomes advance to next peer ───────

    /// Both `Ok(Err(..))` (channel error / event loop gone) and
    /// `Err(Elapsed)` (timeout) arms of the `tokio::time::timeout` wrapper
    /// must:
    ///   - mark the failed peer with `new_visited.mark_visited(peer_addr)`
    ///   - `continue` to the next loop iteration
    ///
    /// A regression where either arm `break`s or `return Err(...)`s would
    /// abandon remaining candidates and send a premature NotFound.
    #[test]
    fn send_and_await_failure_arms_continue_to_next_peer() {
        let src = production_source();
        let body = extract_fn_body(src, "async fn drive_relay_get_inner<CB>(");
        // Match on the outer `round_trip` match arms.
        let round_trip = body
            .find("let reply = match round_trip {")
            .expect("round_trip match must exist");
        let tail = &body[round_trip..];
        // Clip at the end of the match (the `classify(reply)` call that
        // follows it — now bound to `let attempt_outcome = classify(reply);`).
        let clip = tail
            .find("classify(reply)")
            .expect("classify call must follow round_trip match");
        let match_body = &tail[..clip];
        // Channel-error arm: `Ok(Err(err)) => { ... continue; }`.
        let err_arm = match_body
            .find("Ok(Err(err)) =>")
            .expect("channel-error arm must exist");
        let err_tail = &match_body[err_arm..];
        let err_arm_end = err_tail
            .find("Err(_elapsed) =>")
            .expect("timeout arm must follow channel-error arm");
        let err_block = &err_tail[..err_arm_end];
        assert!(
            err_block.contains("continue;"),
            "`Ok(Err(_))` arm must `continue;` so the loop tries the next \
             peer. A regression that returns or breaks here would abandon \
             remaining candidates."
        );
        assert!(
            err_block.contains("mark_visited(peer_addr)"),
            "`Ok(Err(_))` arm must mark the failing peer as visited so the \
             next `relay_advance_to_next_peer` call doesn't re-select it."
        );
        // Timeout arm.
        let timeout_arm = match_body
            .find("Err(_elapsed) =>")
            .expect("timeout arm must exist");
        let timeout_tail = &match_body[timeout_arm..];
        // Clip at the end of the match block.
        let timeout_end = timeout_tail.rfind("};").unwrap_or(timeout_tail.len());
        let timeout_block = &timeout_tail[..timeout_end];
        assert!(
            timeout_block.contains("continue;"),
            "`Err(_elapsed)` (timeout) arm must `continue;` so the loop \
             tries the next peer. A regression that returns here would \
             mean any slow peer permanently blocks the relay."
        );
        assert!(
            timeout_block.contains("mark_visited(peer_addr)"),
            "`Err(_elapsed)` (timeout) arm must mark the timing-out peer \
             as visited; otherwise the retry loop might re-select it and \
             time out again."
        );
    }

    // ── Per-tx independence pin ───────────────────────────────────────────
    //
    // Per-tx independence is enforced by `active_relay_get_txs` —
    // `start_relay_get` rejects a duplicate inbound Request whose
    // `incoming_tx` already has a live driver via the
    // `Relay*InflightGuard` RAII pattern. Dedup is keyed on
    // transaction id (NOT contract instance id), so two concurrent
    // relay GETs for the *same* contract from different upstreams each
    // get their own driver task and their own upstream reply.
    #[test]
    fn relay_inflight_dedup_is_per_transaction_not_per_contract() {
        const SRC: &str = include_str!("../get/op_ctx_task.rs");
        let guard_start = SRC
            .find("RelayGetInflightGuard")
            .expect("RelayGetInflightGuard must exist");
        let window = &SRC[guard_start..(guard_start + 4000).min(SRC.len())];
        assert!(
            window.contains("active_relay_get_txs"),
            "Relay GET inflight dedup must use `active_relay_get_txs` \
             (per-transaction DashSet)."
        );
        // Compose the negative-match needles at runtime so this test's
        // own source doesn't match against `include_str!`.
        let per_key_needle_a = format!("active_relay_get_{}", "keys");
        let per_key_needle_b = format!("active_relay_get_{}", "instance_ids");
        assert!(
            !window.contains(&per_key_needle_a) && !window.contains(&per_key_needle_b),
            "Inflight dedup must NOT key on contract instance id; per-key \
             dedup would break the upstream-reply invariant."
        );
    }

    /// Pin: `run_relay_get` MUST skip `release_pending_op_slot` when
    /// running in originator-loopback mode (`upstream_addr ==
    /// own_addr`). The `pending_op_results` callback for `incoming_tx`
    /// in that mode is the originator's `send_and_await` waiter, not
    /// one this driver installed; releasing it would emit
    /// `TransactionCompleted` and remove the originator's callback
    /// BEFORE the loopback `GetMsg::Response` reaches the bypass
    /// (notifications channel has higher priority than op_execution
    /// in priority_select). Repro:
    /// `test_get_notfound_no_forwarding_targets`.
    #[test]
    fn run_relay_get_skips_release_in_originator_loopback() {
        let src = include_str!("../get/op_ctx_task.rs");
        let fn_start = src
            .find("async fn run_relay_get<CB>(")
            .expect("run_relay_get not found");
        // Bound the search window: `drive_relay_get` follows
        // `run_relay_get` in source order, gated by the
        // `clippy::too_many_arguments` attr.
        let fn_end = src[fn_start..]
            .find("\n#[allow(clippy::too_many_arguments)]\nasync fn drive_relay_get<CB>(")
            .expect("end-of-run_relay_get marker not found")
            + fn_start;
        let body = &src[fn_start..fn_end];
        // The release call must be guarded by an own_addr comparison.
        let release_pos = body
            .find("release_pending_op_slot(incoming_tx)")
            .expect("release_pending_op_slot call not found in run_relay_get");
        let preceding = &body[..release_pos];
        assert!(
            preceding.rfind("get_own_addr()").is_some(),
            "run_relay_get must call get_own_addr() before \
             release_pending_op_slot to gate the release on the \
             upstream != own_addr case"
        );
        assert!(
            preceding.rfind("!originator_loopback").is_some(),
            "run_relay_get must guard release_pending_op_slot with the \
             `!originator_loopback` check (originator-loopback exception)"
        );
        assert!(
            body.contains("originator_loopback"),
            "run_relay_get must compute originator_loopback to gate the \
             release path"
        );
    }

    /// Pin: `relay_send_not_found` and `relay_send_found` MUST use
    /// `send_local_loopback` when `upstream_addr == own_addr`.
    /// Sending wire-bound `send_fire_and_forget(own_addr, ...)` would
    /// attempt a UDP self-connection that has no peer entry, failing
    /// silently. Routing as `InboundMessage` via `send_local_loopback`
    /// lands at the bypass in `handle_pure_network_message_v1` and
    /// forwards to the originator's `pending_op_results` waiter.
    #[test]
    fn relay_send_responses_use_local_loopback_on_own_addr() {
        let src = include_str!("../get/op_ctx_task.rs");
        for fn_name in [
            "async fn relay_send_not_found(",
            "async fn relay_send_found<CB>(",
        ] {
            let fn_start = src
                .find(fn_name)
                .unwrap_or_else(|| panic!("{fn_name} not found"));
            // Bound the search at the next async fn declaration.
            let fn_end = src[fn_start + fn_name.len()..]
                .find("\nasync fn ")
                .map(|idx| idx + fn_start + fn_name.len())
                .unwrap_or(src.len());
            let body = &src[fn_start..fn_end];
            assert!(
                body.contains("send_local_loopback("),
                "{fn_name} must call send_local_loopback for \
                 upstream==own_addr (originator-loopback path)"
            );
            assert!(
                body.contains("get_own_addr()"),
                "{fn_name} must compare upstream_addr to \
                 connection_manager.get_own_addr() to detect loopback"
            );
        }
    }

    /// Pin: `drive_relay_get_inner`'s retry loop MUST switch to
    /// `send_fire_and_forget` (no waiter install) when the relay
    /// driver runs in originator-loopback. `send_to_and_await`
    /// would `state.pending_op_results.insert(*tx, callback)`,
    /// silently OVERWRITING the originator's `start_client_get`
    /// waiter (`incoming_tx == client_tx` in loopback). The peer's
    /// downstream Response would then land in the relay-driver's
    /// callback and never reach the client driver. After loopback
    /// dispatch, the driver returns immediately so the client
    /// driver's still-installed callback receives the Response via
    /// the bypass and owns retry semantics. Mirrors
    /// `drive_relay_put`'s originator-loopback branch.
    #[test]
    fn drive_relay_get_loopback_uses_fire_and_forget() {
        let src = include_str!("../get/op_ctx_task.rs");
        let fn_start = src
            .find("async fn drive_relay_get_inner<CB>(")
            .expect("drive_relay_get_inner not found");
        let fn_end = src[fn_start + "async fn drive_relay_get_inner<CB>(".len()..]
            .find("\nasync fn ")
            .map(|idx| idx + fn_start + "async fn drive_relay_get_inner<CB>(".len())
            .unwrap_or(src.len());
        let body = &src[fn_start..fn_end];
        assert!(
            body.contains("originator_loopback"),
            "drive_relay_get_inner must compute originator_loopback \
             in the retry loop to gate the waiter-install branch"
        );
        // `send_fire_and_forget(peer_addr` must appear in the loopback
        // branch (compose at runtime so this test's own source doesn't
        // match the negative-grep).
        let needle = format!("send_fire_and_{}(peer_addr,", "forget");
        assert!(
            body.contains(&needle),
            "drive_relay_get_inner loopback branch must use \
             send_fire_and_forget(peer_addr, ...) to avoid overwriting \
             the originator's pending_op_results waiter"
        );
    }

    // ── C1: cache_contract_locally must gate mark_local_client_access ──────

    /// Regression guard for the relay hosting-cache taint bug caught in
    /// PR #3896 review. The legacy GET branch gates `mark_local_client_access`
    /// on `is_original_requester = upstream_addr.is_none()` (see
    /// `get.rs:2260-2262, :2353-2355, :3056-3058`). The driver
    /// MUST respect that gate: relay peers that merely cache a forwarded
    /// Found must NOT set the sticky `local_client_access` flag, or they
    /// permanently pay subscription-renewal cost for contracts no client
    /// on the relay node ever asked for.
    ///
    /// Source-scrape invariant: `cache_contract_locally` must accept an
    /// `is_client_requester: bool` parameter and gate the
    /// `mark_local_client_access` call on it.
    #[test]
    fn cache_contract_locally_gates_mark_local_client_access_on_requester() {
        let src = production_source();
        // Function signature must carry the new parameter.
        let sig_start = src
            .find("async fn cache_contract_locally(")
            .expect("cache_contract_locally must exist");
        let sig_window = &src[sig_start..(sig_start + 400).min(src.len())];
        assert!(
            sig_window.contains("is_client_requester: bool"),
            "`cache_contract_locally` must accept `is_client_requester: bool` \
             so relay callsites can opt out of the sticky local_client_access \
             flag. Sig window: {sig_window}"
        );
        // Body must gate `mark_local_client_access` on the flag.
        let body = extract_fn_body(src, "async fn cache_contract_locally(");
        let gate_pos = body
            .find("if is_client_requester {")
            .expect("mark_local_client_access must be gated on is_client_requester");
        let mark_pos = body
            .find("mark_local_client_access")
            .expect("mark_local_client_access must still be called under the gate");
        assert!(
            gate_pos < mark_pos,
            "`if is_client_requester {{` must precede the \
             `mark_local_client_access` call; otherwise the gate is \
             structurally useless."
        );
        // Negative check: there must be NO unconditional (unguarded) call.
        let guarded_snippet =
            "if is_client_requester {\n        op_manager.ring.mark_local_client_access";
        assert!(
            body.contains(guarded_snippet),
            "The `mark_local_client_access` call must be directly inside the \
             `if is_client_requester {{ ... }}` block. Current body structure \
             may have split the guard from the call.\n{body}"
        );
    }

    /// All relay-driver cache callsites must pass `false`.
    /// Relay caches forwarded Found payloads but MUST NOT flag them as
    /// client-accessed on the relay's own hosting cache. There are five
    /// relay cache callsites in `drive_relay_get_inner`:
    ///   1. Active-interest local Found (R4, immediate serve)
    ///   2. Exhaustion fallback (R10, stale local state)
    ///   3. Downstream Found bubble-up (R12a, forwarded payload)
    ///   4. Streaming loopback safety-net (#4307, upstream == own_addr)
    ///   5. Streaming forward best-effort cache (#4307, after pipe_stream)
    #[test]
    fn all_relay_callsites_pass_is_client_requester_false() {
        let src = production_source();
        let body = extract_fn_body(src, "async fn drive_relay_get_inner<CB>(");
        let callsites: Vec<usize> = body
            .match_indices("cache_contract_locally(")
            .map(|(i, _)| i)
            .collect();
        assert_eq!(
            callsites.len(),
            5,
            "drive_relay_get_inner should have exactly five \
             cache_contract_locally callsites (R4 immediate, R10 fallback, \
             R12a bubble-up, plus the two #4307 streaming-arm caches). \
             Found {} — a refactor has changed the shape.",
            callsites.len(),
        );
        // Each callsite must terminate with `, false)` (the
        // `is_client_requester` argument). The body contains non-ASCII
        // comment decorations, so walk by char rather than by byte to avoid
        // slicing on a multi-byte boundary.
        for (idx, &pos) in callsites.iter().enumerate() {
            let tail = &body[pos..];
            // End-of-args: first matching closing `)` at depth 0.
            let mut depth: i32 = 0;
            let mut end: Option<usize> = None;
            for (i, c) in tail.char_indices() {
                match c {
                    '(' => depth += 1,
                    ')' => {
                        depth -= 1;
                        if depth == 0 {
                            end = Some(i);
                            break;
                        }
                    }
                    _ => {}
                }
            }
            let end = end.unwrap_or_else(|| {
                panic!("unterminated cache_contract_locally call at relay callsite #{idx}")
            });
            let call = &tail[..=end];
            // The last argument must be `false`. Normalize whitespace.
            let normalized: String = call.chars().filter(|c| !c.is_whitespace()).collect();
            assert!(
                normalized.ends_with("false,).await")
                    || normalized.ends_with("false)")
                    || normalized.ends_with("false,)"),
                "Relay callsite #{idx} of cache_contract_locally must pass \
                 `false` for is_client_requester (relay is not the client). \
                 Call text: {call}"
            );
        }
    }

    /// The `client_driver`-side callsite (drive_client_get_inner's
    /// InlineFound arm) must pass `true`. Streaming assembly
    /// (`assemble_and_cache_stream`) also must pass `true` — the stream
    /// is only claimed by the client driver.
    #[test]
    fn client_driver_and_streaming_callsites_pass_is_client_requester_true() {
        let src = production_source();
        // Client driver InlineFound arm.
        let drive_body = extract_fn_body(src, "async fn drive_client_get_inner(");
        let drive_call_pos = drive_body
            .find("cache_contract_locally(")
            .expect("client driver must cache_contract_locally on InlineFound");
        let drive_window =
            &drive_body[drive_call_pos..(drive_call_pos + 400).min(drive_body.len())];
        let drive_normalized: String = drive_window
            .chars()
            .take_while(|&c| c != ';')
            .filter(|c| !c.is_whitespace())
            .collect();
        assert!(
            drive_normalized.contains("true"),
            "Client driver's cache_contract_locally call must pass `true` for \
             is_client_requester (this node initiated the GET). Call window: \
             {drive_window}"
        );
        // Streaming assembly.
        let stream_body = extract_fn_body(src, "async fn assemble_and_cache_stream(");
        let stream_call_pos = stream_body
            .find("cache_contract_locally(")
            .expect("assemble_and_cache_stream must cache_contract_locally");
        let stream_window =
            &stream_body[stream_call_pos..(stream_call_pos + 400).min(stream_body.len())];
        let stream_normalized: String = stream_window
            .chars()
            .take_while(|&c| c != ';')
            .filter(|c| !c.is_whitespace())
            .collect();
        assert!(
            stream_normalized.contains("true"),
            "assemble_and_cache_stream must pass `true` for is_client_requester \
             (stream is only claimed by the client-originating driver). Call \
             window: {stream_window}"
        );
    }

    /// Docstring-level guard: the legacy-mirror references in
    /// `cache_contract_locally`'s docstring (`get.rs:2260-2262, :2353-2355,
    /// :3056-3058`) name the three legacy sites where the
    /// `is_original_requester` gate lives. If those line numbers ever stop
    /// referring to `mark_local_client_access` call sites, the docstring
    /// has rotted and future maintenance may miss the invariant.
    ///
    /// This test anchors on the function-name reference rather than the
    /// line numbers themselves (line numbers drift on unrelated edits) —
    /// it just verifies the docstring still cites the function by name so
    /// readers can grep for it.
    #[test]
    fn cache_contract_locally_docstring_cites_legacy_mirror() {
        let src = production_source();
        let fn_pos = src
            .find("async fn cache_contract_locally(")
            .expect("cache_contract_locally must exist");
        // Look in the 1500 chars before the fn for the docstring.
        let window_start = fn_pos.saturating_sub(1500);
        let window = &src[window_start..fn_pos];
        assert!(
            window.contains("is_client_requester"),
            "cache_contract_locally docstring must describe the \
             is_client_requester parameter so callers know which value to pass."
        );
        assert!(
            window.contains("mark_local_client_access"),
            "cache_contract_locally docstring must name the legacy call \
             (`mark_local_client_access`) it gates on is_client_requester."
        );
    }

    // ── Counter: relay driver call count is wired ──────────────────────────

    /// The `RELAY_DRIVER_CALL_COUNT` test counter must be incremented at
    /// the top of `start_relay_get` under `#[cfg(any(test, feature =
    /// "testing"))]`. Behavioral tests use this to assert the dispatch
    /// gate routed a fresh inbound Request through the driver rather
    /// than through the legacy fallthrough.
    #[test]
    fn relay_driver_call_count_incremented_on_entry() {
        let src = production_source();
        // Counter declaration must exist.
        assert!(
            src.contains("pub static RELAY_DRIVER_CALL_COUNT"),
            "RELAY_DRIVER_CALL_COUNT static must be declared for behavioral \
             tests to verify the dispatch gate routes through the relay driver."
        );
        // Increment must appear in the body of start_relay_get, under cfg.
        let body = extract_fn_body(src, "pub(crate) async fn start_relay_get<CB>(");
        assert!(
            body.contains("RELAY_DRIVER_CALL_COUNT.fetch_add(1"),
            "`start_relay_get` must increment `RELAY_DRIVER_CALL_COUNT` at \
             entry so integration tests can assert dispatch-gate coverage."
        );
        // The increment must be cfg-gated to avoid shipping test code.
        let counter_pos = body.find("RELAY_DRIVER_CALL_COUNT.fetch_add").unwrap();
        // Walk back ~200 chars for the cfg attribute.
        let pre = &body[counter_pos.saturating_sub(200)..counter_pos];
        assert!(
            pre.contains("#[cfg(any(test, feature = \"testing\"))]"),
            "`RELAY_DRIVER_CALL_COUNT` increment must be gated behind \
             `#[cfg(any(test, feature = \"testing\"))]` so it doesn't ship \
             in release builds."
        );
    }

    /// Pin: `start_sub_op_get` MUST exist as the sub-op GET entry
    /// point. Removing it would force legacy `get::start_op` +
    /// `request_get` revival.
    #[test]
    fn start_sub_op_get_entry_must_exist() {
        let src = include_str!("op_ctx_task.rs");
        let needle = "pub(crate) fn start_sub_op_get(";
        assert!(
            src.contains(needle),
            "start_sub_op_get fn must remain — sub-op GET migration entry point"
        );
    }

    /// Pin: sub-op driver MUST NOT auto-subscribe or hand off to a
    /// child SUBSCRIBE. The caller (executor's
    /// `local_state_or_from_network` or subscribe's
    /// `fetch_contract_if_missing`) drives those side effects when
    /// appropriate. Doubling them up risks duplicate SUBSCRIBE op
    /// dispatch.
    #[test]
    fn sub_op_driver_skips_auto_subscribe_and_maybe_subscribe_child() {
        let src = include_str!("op_ctx_task.rs");
        // Boundary `\n}\n\n/// Cause` matches the literal closing brace
        // of `drive_sub_op_get` followed by the docstring of the
        // immediately-following `missing_state_cause` helper. This
        // anchors the pin to the function body proper, not the helpers
        // and comment blocks that follow.
        let body = src
            .split("async fn drive_sub_op_get(")
            .nth(1)
            .expect("drive_sub_op_get must exist")
            .split("\n}\n\n/// Cause")
            .next()
            .expect("end of drive_sub_op_get body");
        // Future-proofing: `auto_subscribe_on_get_response` no longer exists
        // (piece E removed GET-auto-subscribe); this stays as an absence-pin so
        // a re-added helper of that name would trip here too.
        assert!(
            !body.contains("auto_subscribe_on_get_response"),
            "drive_sub_op_get must NOT call auto_subscribe_on_get_response — \
             sub-op caller owns subscribe semantics"
        );
        assert!(
            !body.contains("maybe_subscribe_child"),
            "drive_sub_op_get must NOT call maybe_subscribe_child — \
             sub-op caller owns subscribe semantics"
        );
        assert!(
            !body.contains("send_client_result"),
            "drive_sub_op_get must NOT publish to result_router via \
             send_client_result — sub-op tx has no client waiter"
        );
    }

    // ── Behavioral coverage for sub-op outcome variants ───────────────────

    /// `start_sub_op_get` must return a tx that is NOT a sub-operation
    /// in the structural sense (no `parent` field) — sub-op GETs are
    /// node-internal but flat in the transaction-tree sense, mirroring
    /// the legacy `request_get` path which used `Transaction::new::<>`.
    /// Regression: if a sub-op tx grew a parent, `is_sub_operation()`
    /// would suppress the legacy `cb.response()` callback path that any
    /// future migration might still rely on.
    #[test]
    fn sub_op_tx_has_no_parent() {
        let tx = crate::message::Transaction::new::<super::GetMsg>();
        assert!(
            !tx.is_sub_operation(),
            "sub-op GET tx must be flat (no parent) — matches legacy \
             request_get + start_op shape"
        );
    }

    /// Behavioral guard: missing-state cause string MUST contain the
    /// instance_id so operators can correlate the warning to a contract.
    /// This is the message surfaced by `SubOpGetOutcome::NotFound` when
    /// the wire layer returned Found but the local store re-query
    /// produced no state — a real failure mode (e.g., contract handler
    /// rejected the PutQuery, or the store was wiped between Response
    /// and re-query).
    #[test]
    fn missing_state_cause_includes_instance_id() {
        use freenet_stdlib::prelude::ContractInstanceId;
        let id = ContractInstanceId::new([0xAB; 32]);
        let cause = super::missing_state_cause(&id);
        assert!(
            cause.contains(&id.to_string()),
            "missing_state_cause must reference the instance_id; got {cause:?}"
        );
        assert!(
            cause.contains("local store"),
            "missing_state_cause must mention local store re-query failure; got {cause:?}"
        );
    }

    /// Behavioral guard: the fire-and-forget caller (subscribe's
    /// `fetch_contract_if_missing`) drops the oneshot receiver
    /// immediately. The driver task's `out_tx.send(outcome)` MUST NOT
    /// panic in that case — the `let _ =` pattern guarantees graceful
    /// receiver-dropped handling. This test exercises the oneshot
    /// shape independently of the driver to lock in that contract.
    #[test]
    fn oneshot_send_after_receiver_drop_is_silent() {
        let (tx, rx) = tokio::sync::oneshot::channel::<super::SubOpGetOutcome>();
        drop(rx);
        // Mirrors the `let _ = out_tx.send(outcome)` pattern in run_sub_op_get.
        // Should return Err(value) without panicking.
        let result = tx.send(super::SubOpGetOutcome::Infra(
            crate::operations::OpError::UnexpectedOpState,
        ));
        assert!(
            result.is_err(),
            "send after receiver drop must return Err, not panic"
        );
    }

    /// Behavioral guard: `SubOpGetOutcome` discriminates the three
    /// distinct delivery cases. Pattern-matching exhaustiveness is
    /// already enforced by the compiler at the executor consumer site
    /// (`local_state_or_from_network`); this test locks in the
    /// constructor shape so future variant additions trip the test.
    #[test]
    fn sub_op_outcome_variants_constructible() {
        use freenet_stdlib::prelude::WrappedState;

        let state = WrappedState::from(vec![1u8, 2, 3]);
        let found =
            super::SubOpGetOutcome::Found(crate::operations::get::GetResult::new(state, None));
        let not_found = super::SubOpGetOutcome::NotFound("exhausted".to_string());
        let infra = super::SubOpGetOutcome::Infra(crate::operations::OpError::UnexpectedOpState);

        assert!(matches!(found, super::SubOpGetOutcome::Found(_)));
        assert!(matches!(not_found, super::SubOpGetOutcome::NotFound(_)));
        assert!(matches!(infra, super::SubOpGetOutcome::Infra(_)));
    }

    /// Pin: each transport-level failure arm of `drive_relay_get_inner`
    /// records a routing event for the failing peer. Without these
    /// hooks, the per-peer dashboard's failure-probability model is
    /// trained only on originated ops and the symptom this PR fixes
    /// reappears for the relay path. Source-scrape because the
    /// behaviour is positional inside the retry loop and a deletion
    /// would not break any unit-test assertion otherwise.
    #[test]
    fn drive_relay_get_inner_records_route_events_on_transport_failure() {
        let src = include_str!("op_ctx_task.rs");
        let body = extract_fn_body(src, "async fn drive_relay_get_inner<CB>(");

        // The send_to_and_await error arm and the timeout arm must
        // record `Failure` for the chosen peer. Identify each by its
        // log-message phrase, then check the arm's body up to the
        // `continue` for the helper call.
        for log_phrase in [
            "send_to_and_await failed; advancing to next peer",
            "attempt timed out; advancing to next peer",
            // #4307: a downstream that advertised a ResponseStreaming header
            // but never delivered an assemblable stream is also a routing
            // failure for that peer — same training as the transport arms.
            "orphan stream claim failed; advancing to next peer",
        ] {
            let pos = body.unwrap_or_default_pos(log_phrase);
            let after = &body[pos..pos + 1500.min(body.len() - pos)];
            assert!(
                after.contains("record_relay_route_event")
                    && after.contains("RouteOutcome::Failure"),
                "drive_relay_get_inner arm for `{log_phrase}` must call \
                 record_relay_route_event with RouteOutcome::Failure. \
                 Without this, transport failures from relay-forwarded \
                 GETs are dropped and the per-peer failure-probability \
                 model regresses to the originator-only state that \
                 motivated PR #4051."
            );
        }

        // The InlineFound success arm must record SuccessUntimed.
        let pos = body.unwrap_or_default_pos("downstream returned Found");
        let after = &body[pos..pos + 1500.min(body.len() - pos)];
        assert!(
            after.contains("record_relay_route_event")
                && after.contains("RouteOutcome::SuccessUntimed"),
            "drive_relay_get_inner InlineFound arm must call \
             record_relay_route_event with RouteOutcome::SuccessUntimed."
        );

        // The Retry/NotFound arm must record SuccessUntimed too — see
        // the outcome-attribution rationale in operations.rs::record_relay_route_event
        // rustdoc. A peer answering NotFound has not failed.
        let pos = body.unwrap_or_default_pos("downstream returned NotFound; advancing");
        let after = &body[pos..pos + 1500.min(body.len() - pos)];
        assert!(
            after.contains("record_relay_route_event")
                && after.contains("RouteOutcome::SuccessUntimed"),
            "drive_relay_get_inner Retry (NotFound) arm must call \
             record_relay_route_event with RouteOutcome::SuccessUntimed \
             (NotFound is a correct protocol response, not a routing \
             failure). Recording it as Failure would systematically \
             de-prioritise peers that don't host the queried contract."
        );
    }

    /// Regression for #4066: the originator's first
    /// `send_and_await(target=None)` loops back as an InboundMessage
    /// with `source_addr=None`, which falls through to legacy
    /// `process_message` (the relay-driver dispatch is gated on
    /// `source_addr.is_some()`) and pushes a `GetOp` into
    /// `OpManager.ops.get` keyed by `client_tx`. If the driver task
    /// exits without `op_manager.completed()` having been called for
    /// `client_tx`, the legacy entry lingers until the periodic GC
    /// sweep at OPERATION_TTL=60s (which races the driver's
    /// per-attempt timeout, also 60s) and emits a misleading
    /// `phase=get_timeout` log + duplicate `OperationError` to the
    /// client.
    ///
    /// Implemented via an RAII guard installed at the top of
    /// `run_client_get` so cleanup runs on every exit path —
    /// including a panic inside the driver loop, which the previous
    /// per-arm `completed()` calls did not cover (skeptical-reviewer
    /// callout on PR #4070, "panic / abort path still leaks").
    /// Pin both the guard installation and the Drop impl that clears
    /// the legacy entry.
    #[test]
    fn run_client_get_installs_completion_guard() {
        let src = production_source();
        let body = extract_fn_body(src, "async fn run_client_get(");
        assert!(
            body.contains("ClientGetCompletionGuard {"),
            "run_client_get must install a ClientGetCompletionGuard so the \
             legacy ops.get entry created by the originator-loopback Request \
             is cleared on every exit path including a driver panic (#4066). \
             Per-arm completed() calls in drive_client_get_inner are NOT \
             enough — a panic inside drive_retry_loop or any of its awaits \
             aborts the task before reaching the match block, leaving the \
             entry to be reaped by the GC sweep that emits phantom \
             phase=get_timeout."
        );
        assert!(
            body.contains("let _completion_guard = ClientGetCompletionGuard"),
            "ClientGetCompletionGuard must be bound to a guard binding \
             (`let _completion_guard = ...`) so it lives for the full \
             function scope. A bare `let _ = ...` would drop it immediately."
        );

        // The guard's Drop must call op_manager.completed.
        let drop_impl = src
            .split("impl Drop for ClientGetCompletionGuard")
            .nth(1)
            .expect("ClientGetCompletionGuard must have an explicit Drop impl");
        let drop_body = drop_impl
            .split("fn drop")
            .nth(1)
            .expect("ClientGetCompletionGuard Drop impl must define fn drop");
        // Bound the search to the body of fn drop — the next `}` after
        // the opening `{` belongs to the inner block; we just need to
        // confirm `completed(` appears somewhere inside the impl block.
        assert!(
            drop_body.contains(".completed(self.client_tx)"),
            "ClientGetCompletionGuard::drop must call \
             op_manager.completed(self.client_tx) — see #4066"
        );
    }

    /// Same invariant for the sub-op driver: callers (executor's
    /// `local_state_or_from_network`, subscribe's
    /// `fetch_contract_if_missing`) feed the sub-op outcome into their
    /// own logic, but the originator-loopback path that creates the
    /// legacy `ops.get` entry runs on the SAME node, keyed by the
    /// sub-op `tx`. Without the guard, the sub-op's caller observes a
    /// clean failure but the node still emits `phase=get_timeout` 60s
    /// later for the orphaned entry.
    #[test]
    fn run_sub_op_get_installs_completion_guard() {
        let src = production_source();
        let body = extract_fn_body(src, "async fn run_sub_op_get(");
        assert!(
            body.contains("SubOpGetCompletionGuard {"),
            "run_sub_op_get must install a SubOpGetCompletionGuard so the \
             legacy ops.get entry created by the originator-loopback Request \
             is cleared on every exit path — see \
             run_client_get_installs_completion_guard for the failure mode \
             (#4066)"
        );
        assert!(
            body.contains("let _completion_guard = SubOpGetCompletionGuard"),
            "SubOpGetCompletionGuard must be bound to a guard binding so it \
             lives for the full function scope"
        );

        let drop_impl = src
            .split("impl Drop for SubOpGetCompletionGuard")
            .nth(1)
            .expect("SubOpGetCompletionGuard must have an explicit Drop impl");
        let drop_body = drop_impl
            .split("fn drop")
            .nth(1)
            .expect("SubOpGetCompletionGuard Drop impl must define fn drop");
        assert!(
            drop_body.contains(".completed(self.tx)"),
            "SubOpGetCompletionGuard::drop must call \
             op_manager.completed(self.tx) — see #4066"
        );
    }

    trait FindOrPanic {
        fn unwrap_or_default_pos(&self, needle: &str) -> usize;
    }
    impl FindOrPanic for str {
        fn unwrap_or_default_pos(&self, needle: &str) -> usize {
            self.find(needle).unwrap_or_else(|| {
                panic!("expected `{needle}` to appear in drive_relay_get_inner body")
            })
        }
    }
}
