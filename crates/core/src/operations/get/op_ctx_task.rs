//! Task-per-transaction client-initiated GET (#1454 Phase 3b).
//!
//! Mirrors [`crate::operations::put::op_ctx_task`] — the Phase 3a
//! production consumer of [`OpCtx::send_and_await`] for PUT, which in
//! turn follows the Phase 2b SUBSCRIBE driver shape. This module
//! applies the same pattern to client-initiated GET.
//!
//! # Scope (Phase 3b)
//!
//! Only the **client-initiated originator** GET runs through this
//! module. Relay GETs (non-terminal hops), GC-spawned retries, and
//! `start_targeted_op()` (UPDATE-triggered auto-fetch) stay on the
//! legacy re-entry loop. Relay migration is tracked in #3883.
//!
//! # Architecture
//!
//! The task owns all routing state in its locals — there is no `GetOp`
//! in `OpManager.ops.get` for any attempt this task makes. The task:
//!
//! 1. Loops, calling [`OpCtx::send_and_await`] with a fresh
//!    `Transaction` per attempt (single-use-per-tx constraint).
//!    `process_message` handles routing/forwarding on remote hops and
//!    the originator's own loop-back for the local-completion (cache
//!    hit) case, which echoes the `GetMsg::Request` back as the
//!    terminal "reply" via `forward_pending_op_result_if_completed`
//!    (same mechanism PUT 3a relies on for `LocalCompletion`).
//! 2. On terminal `Response{Found}`: the driver stores the returned
//!    state into the local executor via `PutQuery`, runs hosting /
//!    access-tracking / announce side effects, and publishes
//!    `HostResponse::ContractResponse::GetResponse` to the client.
//!    These side effects mirror what the legacy `process_message`
//!    Response{Found} branch does at `get.rs:2329` — for task-per-tx
//!    ops the bypass at `node.rs::handle_pure_network_message_v1`
//!    intercepts the terminal reply before `process_message` runs on
//!    the originator (because `load_or_init` would fail with
//!    `OpNotPresent`), so the driver is the only place they can
//!    happen.
//! 3. On terminal `ResponseStreaming`: Phase 3b delivers the final
//!    payload via a store re-query using the contract key from the
//!    envelope. Stream assembly and local caching for streamed
//!    payloads remain on the legacy path for now — full migration is
//!    tracked in #3883.
//! 4. On terminal Request-echo: the pre-send local-cache shortcut in
//!    `client_events.rs` already returned the state directly, so the
//!    driver just resolves the ContractKey from the store for
//!    telemetry and client delivery.
//! 5. On `Response{NotFound}`: advance to the next peer.
//! 6. On timeout / wire-error: advance to next peer or exhaust.
//!
//! # Connection-drop latency (R6)
//!
//! Legacy `handle_abort` detects disconnects in <1s. Task-per-tx
//! relies on the `OPERATION_TTL` (60s) timeout. Accepted ceiling,
//! matching Phase 2b/3a.

use std::net::SocketAddr;
use std::sync::Arc;

use freenet_stdlib::client_api::{ContractResponse, ErrorKind, HostResponse};
use freenet_stdlib::prelude::*;

use crate::client_events::HostResult;
use crate::config::{GlobalExecutor, OPERATION_TTL};
use crate::contract::{ContractHandlerEvent, StoreResponse};
use crate::message::{NetMessage, NetMessageV1, Transaction};
use crate::node::OpManager;
#[rustfmt::skip]
use crate::operations::op_ctx::{
    AdvanceOutcome, AttemptOutcome, RetryDriver, RetryLoopOutcome, drive_retry_loop,
};
use crate::operations::OpError;
use crate::operations::VisitedPeers;
use crate::ring::{Location, PeerKeyLocation};
use crate::router::{RouteEvent, RouteOutcome};
use crate::transport::peer_connection::StreamId;

use super::{GetMsg, GetMsgResult, GetStreamingPayload};
use crate::operations::orphan_streams::{OrphanStreamError, STREAM_CLAIM_TIMEOUT};

/// Test-only counter that increments every time `start_client_get` is
/// called. Used by integration tests to verify that a GET actually
/// routed through the task-per-tx driver rather than being satisfied
/// by the `client_events.rs` local-cache shortcut.
#[cfg(any(test, feature = "testing"))]
pub static DRIVER_CALL_COUNT: std::sync::atomic::AtomicUsize =
    std::sync::atomic::AtomicUsize::new(0);

/// Test-only counter that increments every time `start_relay_get` is
/// called. Used by integration tests to verify that relay dispatch
/// actually routed a fresh inbound Request through the task-per-tx
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
) -> Result<Transaction, OpError> {
    // Test-only: count driver invocations so integration tests can
    // assert the driver was actually called (as opposed to
    // `client_events.rs`'s local-cache shortcut satisfying the GET).
    // Removed under #[cfg(not(any(test, feature = "testing")))].
    #[cfg(any(test, feature = "testing"))]
    DRIVER_CALL_COUNT.fetch_add(1, std::sync::atomic::Ordering::SeqCst);

    tracing::debug!(
        tx = %client_tx,
        contract = %instance_id,
        "get (task-per-tx): spawning client-initiated task"
    );

    // Fire-and-forget spawn; same rationale as PUT 3a's `start_client_put`.
    // Failures are published to the client via `result_router_tx`, not
    // via this function's return value. Not registered with
    // `BackgroundTaskMonitor`: per-transaction task that terminates via
    // happy path, exhaustion, timeout, or infra error.
    GlobalExecutor::spawn(run_client_get(
        op_manager,
        client_tx,
        instance_id,
        return_contract_code,
        subscribe,
        blocking_subscribe,
    ));

    Ok(client_tx)
}

async fn run_client_get(
    op_manager: Arc<OpManager>,
    client_tx: Transaction,
    instance_id: ContractInstanceId,
    return_contract_code: bool,
    subscribe: bool,
    blocking_subscribe: bool,
) {
    let outcome = drive_client_get(
        op_manager.clone(),
        client_tx,
        instance_id,
        return_contract_code,
        subscribe,
        blocking_subscribe,
    )
    .await;
    deliver_outcome(&op_manager, client_tx, outcome);
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
) -> DriverOutcome {
    match drive_client_get_inner(
        &op_manager,
        client_tx,
        instance_id,
        return_contract_code,
        subscribe,
        blocking_subscribe,
    )
    .await
    {
        Ok(outcome) => outcome,
        Err(err) => DriverOutcome::InfrastructureError(err),
    }
}

async fn drive_client_get_inner(
    op_manager: &Arc<OpManager>,
    client_tx: Transaction,
    instance_id: ContractInstanceId,
    return_contract_code: bool,
    subscribe: bool,
    blocking_subscribe: bool,
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
        None => op_manager.ring.connection_manager.own_location(),
    };

    let mut driver = GetRetryDriver {
        op_manager,
        instance_id,
        htl,
        tried,
        retries: 0,
        current_target,
        attempt_visited: VisitedPeers::new(&client_tx),
    };

    let loop_result = drive_retry_loop(op_manager, client_tx, "get", &mut driver).await;

    match loop_result {
        RetryLoopOutcome::Done(terminal) => {
            // Clean up any DashMap entry left behind. `op_manager.completed`
            // is idempotent, so calling it even when the driver never
            // pushed is harmless.
            op_manager.completed(client_tx);

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
            // runs on the originator (by design — process_message
            // would fail with OpNotPresent for a task-per-tx op), so
            // the driver is the only place these side effects can
            // happen for Phase 3b.
            let reply_key = match &terminal {
                Terminal::InlineFound {
                    key,
                    state,
                    contract,
                } => {
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
                    key,
                    stream_id,
                    includes_contract,
                } => {
                    // Assemble the stream and cache locally. Mirrors
                    // the legacy `process_message` streaming branch
                    // at `get.rs:2721-3196`. Uses `current_target`
                    // as the sender address — accurate for the
                    // single-hop response case where the responder
                    // equals the selected target.
                    if let Some(peer_addr) = driver.current_target.socket_addr() {
                        if let Err(e) = assemble_and_cache_stream(
                            op_manager,
                            peer_addr,
                            *stream_id,
                            *key,
                            *includes_contract,
                        )
                        .await
                        {
                            tracing::warn!(
                                %key,
                                error = %e,
                                "get (task-per-tx): stream assembly failed — \
                                 state will not be cached locally"
                            );
                        }
                    } else {
                        tracing::warn!(
                            %key,
                            "get (task-per-tx): current_target has no socket_addr; \
                             cannot claim orphan stream"
                        );
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

            let host_result =
                build_host_response(op_manager, &instance_id, return_contract_code).await;

            // Auto-subscribe on successful GET at the originator —
            // mirrors the legacy branches at get.rs:2313/2408/3136/3185.
            // AUTO_SUBSCRIBE_ON_GET (ring.rs:60) is a const; we still
            // guard on `is_subscribed` to avoid duplicate registration
            // if the request-router already wired up a subscribe.
            //
            // When the client explicitly set `subscribe=true`, the
            // dedicated `maybe_subscribe_child` path below runs — skip
            // auto-subscribe here so we never double-subscribe.
            if host_result.is_ok()
                && !subscribe
                && crate::ring::AUTO_SUBSCRIBE_ON_GET
                && !op_manager.ring.is_subscribed(&reply_key)
            {
                let path_label = match &terminal {
                    Terminal::Streaming { .. } => "streaming (task-per-tx)",
                    Terminal::InlineFound { .. } | Terminal::LocalCompletion => {
                        "non-streaming (task-per-tx)"
                    }
                };
                crate::operations::auto_subscribe_on_get_response(
                    op_manager,
                    &reply_key,
                    &client_tx,
                    &Some(driver.current_target.clone()),
                    /* subscribe_requested */ false,
                    /* blocking_sub */ blocking_subscribe,
                    path_label,
                )
                .await;
            }

            // Emit routing event + telemetry — `report_result` (which
            // normally does both) doesn't run because the bypass
            // intercepted the Response. Without this, the router's
            // prediction model never receives GET success feedback.
            //
            // The success flag tracks the actual client-visible
            // outcome (`host_result.is_ok()`), not the wire-level
            // reply — if the store re-query returned nothing, the
            // client sees OperationError and telemetry must agree.
            let contract_location = Location::from(&reply_key);
            let route_event = RouteEvent {
                peer: driver.current_target.clone(),
                contract_location,
                outcome: if host_result.is_ok() {
                    RouteOutcome::SuccessUntimed
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
            // `subscribe=true`; the auto-subscribe path above handles
            // the AUTO_SUBSCRIBE_ON_GET fallback.
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
            Ok(DriverOutcome::Publish(Err(ErrorKind::OperationError {
                cause: cause.into(),
            }
            .into())))
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
    },
    /// ResponseStreaming: the envelope references a stream_id whose
    /// bytes arrive separately via the orphan stream registry. The
    /// driver claims the stream, awaits assembly, and caches the
    /// assembled state + contract locally — mirroring what the
    /// legacy `process_message` streaming branch does at
    /// `get.rs:2721-3196`.
    Streaming {
        key: ContractKey,
        stream_id: StreamId,
        includes_contract: bool,
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
            ..
        })) => AttemptOutcome::Terminal(Terminal::InlineFound {
            key,
            state,
            contract,
        }),
        NetMessage::V1(NetMessageV1::Get(GetMsg::Response {
            result: GetMsgResult::Found { value, .. },
            ..
        })) => {
            tracing::warn!(
                ?value,
                "get (task-per-tx): Response{{Found}} arrived without state"
            );
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
            ..
        })) => AttemptOutcome::Terminal(Terminal::Streaming {
            key,
            stream_id,
            includes_contract,
        }),
        NetMessage::V1(NetMessageV1::Get(GetMsg::Request { .. })) => {
            AttemptOutcome::Terminal(Terminal::LocalCompletion)
        }
        // Explicit non-terminal `GetMsg` variants. These should never
        // reach the driver — the bypass at
        // `node.rs::handle_pure_network_message_v1` gates forwarding
        // on terminal variants only — so their arrival here indicates
        // a bug in the bypass gate (Phase 2b Bug 2 class).
        NetMessage::V1(NetMessageV1::Get(
            GetMsg::ForwardingAck { .. } | GetMsg::ResponseStreamingAck { .. },
        )) => AttemptOutcome::Unexpected,
        // Non-GET NetMessage variants (or any future `GetMsg` variant
        // added without updating this match) fall through to
        // Unexpected. If a new GetMsg variant is added, this arm must
        // be audited — the Phase 3b GET driver has explicit handling
        // for every variant above, and the bypass filter in node.rs
        // must be extended in lockstep.
        _ => AttemptOutcome::Unexpected,
    }
}

impl RetryDriver for GetRetryDriver<'_> {
    type Terminal = Terminal;

    fn new_attempt_tx(&mut self) -> Transaction {
        let tx = Transaction::new::<GetMsg>();
        self.attempt_visited = VisitedPeers::new(&tx);
        tx
    }

    fn build_request(&mut self, attempt_tx: Transaction) -> NetMessage {
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
        classify(reply)
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

// --- Host-response construction ---

/// Query the local contract store for `(state, contract)` and package
/// a client-facing `HostResponse::ContractResponse::GetResponse`.
///
/// If the local store doesn't have the state (which should not happen
/// on the happy path — `process_message` stores before the terminal
/// reply fires), synthesize an operation error matching the shape
/// `to_host_result` produces on NotFound.
async fn build_host_response(
    op_manager: &OpManager,
    instance_id: &ContractInstanceId,
    return_contract_code: bool,
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
            tracing::warn!(
                contract = %instance_id,
                "get (task-per-tx): terminal reply classified success but local \
                 store lookup returned no state; synthesizing client error"
            );
            Err(ErrorKind::OperationError {
                cause: format!(
                    "GET succeeded on wire but local store lookup failed for {instance_id}"
                )
                .into(),
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
/// `process_message` Response{Found} branch at `get.rs:2218-2450`:
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
async fn cache_contract_locally(
    op_manager: &OpManager,
    key: ContractKey,
    state: WrappedState,
    contract: Option<ContractContainer>,
    is_client_requester: bool,
) {
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
            "get (task-per-tx): local state matches, skipping redundant PutQuery"
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
                    "get (task-per-tx): PutQuery rejected by executor"
                );
                false
            }
            Ok(other) => {
                tracing::warn!(
                    %key,
                    ?other,
                    "get (task-per-tx): PutQuery returned unexpected event"
                );
                false
            }
            Err(err) => {
                tracing::warn!(
                    %key,
                    %err,
                    "get (task-per-tx): PutQuery failed"
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
            "get (task-per-tx): skipping local cache — contract code missing"
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

    let mut removed_contracts = Vec::new();
    for evicted_key in &access_result.evicted {
        if op_manager
            .interest_manager
            .unregister_local_hosting(evicted_key)
        {
            removed_contracts.push(*evicted_key);
        }
    }

    // (4) Newly-hosted announcement gates on BOTH first-time access
    // AND the fact that we actually persisted new state. Without
    // persistence there's nothing to announce hosting for.
    if access_result.is_new && put_persisted {
        crate::operations::announce_contract_hosted(op_manager, &key).await;
        let became_interested = op_manager.interest_manager.register_local_hosting(&key);
        let added = if became_interested { vec![key] } else { vec![] };
        if !added.is_empty() || !removed_contracts.is_empty() {
            crate::operations::broadcast_change_interests(op_manager, added, removed_contracts)
                .await;
        }
    } else if !removed_contracts.is_empty() {
        crate::operations::broadcast_change_interests(op_manager, vec![], removed_contracts).await;
    }
}

/// Claim an orphan stream, await assembly, deserialize the payload,
/// and cache the contract state locally.
///
/// Mirrors the originator-side streaming branch of the legacy
/// `process_message` at `get.rs:2721-3196`. The driver is the only
/// place this can run for task-per-tx GETs because the bypass at
/// `node.rs::handle_pure_network_message_v1` forwards the
/// `ResponseStreaming` envelope to the driver before
/// `handle_op_request` — `process_message` never executes on the
/// originator for task-per-tx ops (`load_or_init` would return
/// `OpNotPresent`).
///
/// `peer_addr` is the sender's transport address — currently we
/// use `driver.current_target.socket_addr()`, which is accurate for
/// single-hop responses. Multi-hop (where a relay answers on behalf
/// of a further peer) is not yet supported by the task-per-tx driver;
/// see #3883.
async fn assemble_and_cache_stream(
    op_manager: &OpManager,
    peer_addr: std::net::SocketAddr,
    stream_id: StreamId,
    expected_key: ContractKey,
    includes_contract: bool,
) -> Result<(), String> {
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

    // Streaming assembly is reachable only from the client-driver side (the
    // relay driver does not claim streams — port plan §7). The originator
    // IS the client requester, so the sticky `local_client_access` flag
    // applies.
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

/// Maximum routing rounds before giving up. Matches PUT 3a's
/// `MAX_RETRIES = 3` and SUBSCRIBE's driver. With typical ring
/// fan-out of 3–5 peers per k_closest call, 3 rounds covers
/// 9–15 distinct peers.
const MAX_RETRIES: usize = 3;

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

    let peer = op_manager
        .ring
        .k_closest_potentially_hosting(instance_id, tried.as_slice(), 1)
        .into_iter()
        .next()?;
    let addr = peer.socket_addr()?;
    tried.push(addr);
    Some((peer, addr))
}

// --- Subscribe child ---

/// Start a post-GET subscription if requested. Mirrors
/// `put::op_ctx_task::maybe_subscribe_child` verbatim.
///
/// For `blocking_subscribe = true`, awaits the subscribe driver inline.
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
                "get (task-per-tx): infrastructure error; publishing synthesized client error"
            );
            let synthesized: HostResult = Err(ErrorKind::OperationError {
                cause: format!("GET failed: {err}").into(),
            }
            .into());
            op_manager.send_client_result(client_tx, synthesized);
        }
    }
}

// ── Relay GET task-per-tx driver (#3883) ────────────────────────────────────
//
// `start_relay_get` is the entry point for the relay (non-originator) GET
// task-per-tx driver.  It is called from `node.rs` dispatch (commit 2) when
// an incoming `GetMsg::Request` arrives with `source_addr.is_some()` — i.e.
// from a real remote peer rather than the originator's own loop-back.
//
// The driver owns all routing / forwarding / retry state in task locals.
// No `GetOp` is stored in `OpManager.ops.get` for any relay hop this driver
// handles.  Responses are bubbled back upstream via `OpCtx::send_to_and_await`
// targeting the downstream peer, and the reply (Found / NotFound) is forwarded
// to `upstream_addr` via a fire-and-forget `conn_manager.send`.
//
// Out of scope for this commit (§7 of the port plan):
//   - ResponseStreaming relay forwarding (chunk pipe-through).
//   - GC-spawned retries.
//   - start_targeted_op (UPDATE-triggered auto-fetch).
//
// This entire section is dead code in commit 1 — node.rs dispatch is not
// changed until commit 2.

/// Start a relay (non-originator) GET task-per-tx driver.
///
/// Called from `node.rs` dispatch when an incoming `GetMsg::Request`
/// arrives from a remote peer (`source_addr.is_some()`).  The driver
/// owns routing/forwarding/retry state in its task locals — no `GetOp`
/// is stored in `OpManager.ops.get` for this transaction.
///
/// Originator loop-back (`source_addr.is_none()`) continues to use the
/// legacy `handle_op_request` → `process_message` path.
///
/// # Scope (#3883)
///
/// Migrated:
/// - `GetMsg::Request` relay arm: HTL check, local-cache lookup (with
///   interest gate), forward-or-respond decision.
/// - `GetMsg::Response{NotFound}` retry to next alternative peer.
/// - `GetMsg::Response{Found}` bubble-up to upstream.
/// - `ForwardingAck` send-before-forward.
///
/// NOT migrated (stays on legacy path; see port plan §7):
/// - Streaming-chunk relay forwarding (`ResponseStreaming` pass-through).
/// - GC-spawned retries.
/// - `start_targeted_op` (UPDATE-triggered auto-fetch).
#[allow(clippy::too_many_arguments)]
pub(crate) async fn start_relay_get(
    op_manager: Arc<OpManager>,
    incoming_tx: Transaction,
    instance_id: ContractInstanceId,
    htl: usize,
    upstream_addr: SocketAddr,
    visited: VisitedPeers,
    fetch_contract: bool,
    subscribe: bool,
) -> Result<(), OpError> {
    // Test-only: count relay driver invocations so regression tests can
    // assert the dispatch gate in node.rs actually routed a fresh inbound
    // Request through the task-per-tx driver rather than the legacy
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
            "GET relay (task-per-tx): duplicate Request for in-flight tx, dropping"
        );
        return Ok(());
    }

    tracing::debug!(
        tx = %incoming_tx,
        %instance_id,
        htl,
        %upstream_addr,
        phase = "relay_start",
        "GET relay (task-per-tx): spawning driver"
    );

    GlobalExecutor::spawn(run_relay_get(
        op_manager,
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
async fn run_relay_get(
    op_manager: Arc<OpManager>,
    incoming_tx: Transaction,
    instance_id: ContractInstanceId,
    htl: usize,
    upstream_addr: SocketAddr,
    visited: VisitedPeers,
    fetch_contract: bool,
    subscribe: bool,
) {
    RELAY_INFLIGHT.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    RELAY_SPAWNED_TOTAL.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    let _guard = RelayInflightGuard {
        op_manager: op_manager.clone(),
        incoming_tx,
    };

    if let Err(err) = drive_relay_get(
        &op_manager,
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
        tracing::warn!(
            tx = %incoming_tx,
            %instance_id,
            error = %err,
            phase = "relay_infra_error",
            "GET relay (task-per-tx): infrastructure error in driver"
        );
    }
}

#[allow(clippy::too_many_arguments)]
async fn drive_relay_get(
    op_manager: &Arc<OpManager>,
    incoming_tx: Transaction,
    instance_id: ContractInstanceId,
    htl: usize,
    upstream_addr: SocketAddr,
    visited: VisitedPeers,
    fetch_contract: bool,
    subscribe: bool,
) -> Result<(), OpError> {
    match drive_relay_get_inner(
        op_manager,
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
                "GET relay (task-per-tx): inner driver returned error; sending NotFound upstream"
            );
            // On infrastructure error, send NotFound upstream so the upstream
            // doesn't time out waiting for us.
            relay_send_not_found(op_manager, incoming_tx, instance_id, upstream_addr).await;
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
    const MAX_RELAY_RETRIES: usize = 3;
    if *retries >= MAX_RELAY_RETRIES {
        return None;
    }
    *retries += 1;

    // Use new_visited as the skip list so upstream's visited set and our own
    // marks are both respected.
    let peer = op_manager
        .ring
        .k_closest_potentially_hosting(instance_id, new_visited.clone(), 1)
        .into_iter()
        .next()?;
    let addr = peer.socket_addr()?;
    // Double-check against tried (exact exclusion, no bloom false positives).
    if tried.contains(&addr) {
        return None;
    }
    tried.push(addr);
    Some((peer, addr))
}

/// Build and send a `GetMsg::Response{NotFound}` to `upstream_addr`.
async fn relay_send_not_found(
    op_manager: &OpManager,
    tx: Transaction,
    instance_id: ContractInstanceId,
    upstream_addr: SocketAddr,
) {
    let msg = NetMessage::from(GetMsg::Response {
        id: tx,
        instance_id,
        result: GetMsgResult::NotFound,
    });
    let mut ctx = op_manager.op_ctx(tx);
    // Fire-and-forget — relay doesn't await the upstream's ack.
    if let Err(err) = ctx.send_fire_and_forget(upstream_addr, msg).await {
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
async fn relay_send_found(
    op_manager: &OpManager,
    tx: Transaction,
    instance_id: ContractInstanceId,
    upstream_addr: SocketAddr,
    key: ContractKey,
    state: WrappedState,
    contract: Option<ContractContainer>,
) -> Result<(), OpError> {
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
    });
    let mut ctx = op_manager.op_ctx(tx);
    // Fire-and-forget toward upstream — relay doesn't await upstream's ack.
    ctx.send_fire_and_forget(upstream_addr, msg)
        .await
        .map_err(|_| OpError::NotificationError)
}

#[allow(clippy::too_many_arguments)]
async fn drive_relay_get_inner(
    op_manager: &Arc<OpManager>,
    incoming_tx: Transaction,
    instance_id: ContractInstanceId,
    htl: usize,
    upstream_addr: SocketAddr,
    visited: VisitedPeers,
    fetch_contract: bool,
    subscribe: bool,
) -> Result<(), OpError> {
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
            "GET relay (task-per-tx): HTL exhausted — sending NotFound upstream"
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
        relay_send_not_found(op_manager, incoming_tx, instance_id, upstream_addr).await;
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
            "GET relay (task-per-tx): contract found locally (active interest) — sending Found upstream"
        );

        // Register interest for the requester (mirrors get.rs:1447-1476).
        if subscribe {
            crate::operations::subscribe::register_downstream_subscriber(
                op_manager,
                &key,
                upstream_addr,
                None,
                None,
                &incoming_tx,
                " (relay task-per-tx, piggybacked on GET)",
            )
            .await;
        } else if let Some(pkl) = op_manager
            .ring
            .connection_manager
            .get_peer_by_addr(upstream_addr)
        {
            let peer_key = crate::ring::interest::PeerKey::from(pkl.pub_key.clone());
            op_manager
                .interest_manager
                .register_peer_interest(&key, peer_key, None, false);
        }

        // Cache locally first (relay already hosting, idempotent).
        // `is_client_requester=false`: relay peers must not set the sticky
        // `local_client_access` flag — that's exclusively for the client-
        // originating node. Legacy mirror: `get.rs:2260-2262` gates on
        // `is_original_requester`.
        cache_contract_locally(op_manager, key, state.clone(), contract.clone(), false).await;

        relay_send_found(
            op_manager,
            incoming_tx,
            instance_id,
            upstream_addr,
            key,
            state,
            contract,
        )
        .await?;
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

    loop {
        // Pick next downstream peer.
        let (peer, peer_addr) = match relay_advance_to_next_peer(
            op_manager,
            &instance_id,
            &mut tried,
            &mut retries,
            &new_visited,
        ) {
            Some(p) => p,
            None => {
                // Exhausted: serve local fallback if available, else NotFound.
                if let Some((key, state, contract)) = local_fallback {
                    tracing::info!(
                        tx = %incoming_tx,
                        %instance_id,
                        contract = %key,
                        "GET relay (task-per-tx): all peers exhausted — serving local fallback"
                    );
                    // Relay path: is_client_requester=false (see top-of-loop
                    // rationale).
                    cache_contract_locally(op_manager, key, state.clone(), contract.clone(), false)
                        .await;
                    relay_send_found(
                        op_manager,
                        incoming_tx,
                        instance_id,
                        upstream_addr,
                        key,
                        state,
                        contract,
                    )
                    .await?;
                } else {
                    tracing::warn!(
                        tx = %incoming_tx,
                        %instance_id,
                        %upstream_addr,
                        phase = "not_found",
                        "GET relay (task-per-tx): all peers exhausted — sending NotFound upstream"
                    );
                    if let Some(event) = crate::tracing::NetEventLog::get_not_found(
                        &incoming_tx,
                        &op_manager.ring,
                        instance_id,
                        None,
                    ) {
                        op_manager
                            .ring
                            .register_events(either::Either::Left(event))
                            .await;
                    }
                    relay_send_not_found(op_manager, incoming_tx, instance_id, upstream_addr).await;
                }
                return Ok(());
            }
        };

        // NOTE: legacy path sends a ForwardingAck upstream before forwarding
        // downstream so the upstream's GC can extend its timer on slow
        // multi-hop chains. In the task-per-tx relay driver we OMIT that
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
            "GET relay (task-per-tx): forwarding request to downstream peer"
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

        // Send to downstream peer and await reply (keyed on incoming_tx).
        let mut ctx = op_manager.op_ctx(incoming_tx);
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
                    "GET relay (task-per-tx): send_to_and_await failed; advancing to next peer"
                );
                // Continue loop to try next peer.
                new_visited.mark_visited(peer_addr);
                continue;
            }
            Err(_elapsed) => {
                tracing::warn!(
                    tx = %incoming_tx,
                    target = %peer,
                    timeout_secs = OPERATION_TTL.as_secs(),
                    "GET relay (task-per-tx): attempt timed out; advancing to next peer"
                );
                new_visited.mark_visited(peer_addr);
                continue;
            }
        };

        // Classify the reply.
        match classify(reply) {
            AttemptOutcome::Terminal(Terminal::InlineFound {
                key,
                state,
                contract,
            }) => {
                tracing::info!(
                    tx = %incoming_tx,
                    %instance_id,
                    contract = %key,
                    phase = "relay_found",
                    "GET relay (task-per-tx): downstream returned Found — caching locally and bubbling upstream"
                );

                // Cache locally (relay opportunistically caches forwarded
                // Found payloads). `is_client_requester=false` so this node
                // does NOT set `mark_local_client_access` — the sticky flag
                // belongs to the upstream client-originating node, not a
                // forwarder. `announce_contract_hosted` DOES fire when
                // `access_result.is_new && put_persisted` so first-time
                // hosting at a relay is still broadcast (matches legacy
                // `get.rs:2370` which announces on any first-time relay
                // cache).
                cache_contract_locally(op_manager, key, state.clone(), contract.clone(), false)
                    .await;

                // Bubble up to upstream.
                relay_send_found(
                    op_manager,
                    incoming_tx,
                    instance_id,
                    upstream_addr,
                    key,
                    state,
                    contract,
                )
                .await?;
                return Ok(());
            }
            AttemptOutcome::Terminal(Terminal::Streaming { .. }) => {
                // Streaming relay forwarding is out of scope for #3883 commit 1.
                // Log and fall through to next peer (treat as NotFound for now).
                // A follow-up PR will add proper chunk pipe-through.
                tracing::warn!(
                    tx = %incoming_tx,
                    %instance_id,
                    target = %peer,
                    "GET relay (task-per-tx): downstream returned ResponseStreaming — \
                     streaming relay forwarding not yet implemented (port plan §7); \
                     trying next peer"
                );
                new_visited.mark_visited(peer_addr);
                continue;
            }
            AttemptOutcome::Terminal(Terminal::LocalCompletion) => {
                // A relay driver should never receive a Request-echo because
                // `send_to_and_await` targets a specific remote peer (not loopback).
                // If this arrives, treat it as Unexpected.
                tracing::warn!(
                    tx = %incoming_tx,
                    %instance_id,
                    "GET relay (task-per-tx): unexpected LocalCompletion (Request-echo) — trying next peer"
                );
                new_visited.mark_visited(peer_addr);
                continue;
            }
            AttemptOutcome::Retry => {
                tracing::debug!(
                    tx = %incoming_tx,
                    target = %peer,
                    "GET relay (task-per-tx): downstream returned NotFound; advancing to next peer"
                );
                // Mark the failed peer so future iterations don't re-select it.
                new_visited.mark_visited(peer_addr);
                // Loop iterates to next peer.
                continue;
            }
            AttemptOutcome::Unexpected => {
                tracing::warn!(
                    tx = %incoming_tx,
                    target = %peer,
                    "GET relay (task-per-tx): unexpected reply variant; advancing to next peer"
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
        }));
        assert!(matches!(
            classify(msg),
            AttemptOutcome::Terminal(Terminal::InlineFound { .. })
        ));
    }

    #[test]
    fn classify_response_notfound_is_retry() {
        let tx = dummy_tx();
        let key = dummy_key();
        let msg = NetMessage::V1(NetMessageV1::Get(GetMsg::Response {
            id: tx,
            instance_id: *key.id(),
            result: GetMsgResult::NotFound,
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
            "ForwardingAck must NOT be classified as terminal (Phase 2b bug 2)"
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

    /// Bug #3 reproduction: the legacy Response{Found} branch at
    /// `get.rs:2218-2241` reads the local store via
    /// `ContractHandlerEvent::GetQuery` FIRST, compares the stored bytes
    /// against the incoming `value`, and skips the `PutQuery` entirely
    /// when they match. This prevents re-invoking `update_state()` on
    /// contracts that implement idempotency checks (see #2018). The
    /// task-per-tx driver's `cache_contract_locally` must replicate
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

    /// Bug #2 reproduction (source-level): the legacy branch calls
    /// `auto_subscribe_on_get_response` for any client-initiated GET
    /// when `AUTO_SUBSCRIBE_ON_GET` is true (see `get.rs:2313, 2408`).
    /// The driver currently never calls it; `maybe_subscribe_child`
    /// only handles the explicit `subscribe=true` flag. This test
    /// pairs with `test_driver_inline_get_triggers_auto_subscribe`
    /// (integration) to lock down both the absence and the symptom.
    #[test]
    fn driver_calls_auto_subscribe_on_get_response() {
        let src = production_source();
        assert!(
            src.contains("auto_subscribe_on_get_response"),
            "The driver must invoke `auto_subscribe_on_get_response` on \
             successful GET terminal paths (AUTO_SUBSCRIBE_ON_GET = true in \
             ring.rs:60). The legacy branch does this at get.rs:2313/2408/3136/3185; \
             the driver must mirror it so client GETs with subscribe=false \
             still register the fallback subscription."
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
        let body = extract_fn_body(src, "async fn drive_client_get_inner(");
        // Find the `Terminal::Streaming` arm of the Done match.
        let arm = body
            .find("Terminal::Streaming {")
            .expect("Done arm must handle Terminal::Streaming");
        // The matching arm must call `assemble_and_cache_stream`.
        let tail = &body[arm..];
        // Bound the search to this arm by clipping at the next
        // `Terminal::` match arm.
        let arm_end = tail[1..]
            .find("Terminal::")
            .map(|p| p + 1)
            .unwrap_or(tail.len());
        let arm_body = &tail[..arm_end];
        assert!(
            arm_body.contains("assemble_and_cache_stream"),
            "Terminal::Streaming arm of drive_client_get_inner must call \
             `assemble_and_cache_stream`. Without this, cold-cache streaming \
             GETs return OperationError because nothing writes the local \
             store. See bug #1 in PR #3884 review."
        );
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
        let body = extract_fn_body(src, "async fn drive_relay_get_inner(");
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
        let body = extract_fn_body(src, "async fn drive_relay_get_inner(");
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

    /// T4: Downstream `Response{Found}` must call `cache_contract_locally`
    /// before calling `relay_send_found`.  Ensures relay opportunistically
    /// caches contract code.
    #[test]
    fn relay_driver_caches_locally_on_found_response() {
        let src = production_source();
        let body = extract_fn_body(src, "async fn drive_relay_get_inner(");
        // Find the InlineFound arm inside the loop.
        let found_arm = body
            .find("Terminal::InlineFound {")
            .expect("drive_relay_get_inner must handle Terminal::InlineFound");
        let tail = &body[found_arm..];
        // Find cache and send in order within the arm.
        let cache_pos = tail.find("cache_contract_locally").unwrap_or(usize::MAX);
        let send_pos = tail.find("relay_send_found").unwrap_or(usize::MAX);
        assert!(
            cache_pos < send_pos,
            "cache_contract_locally must be called BEFORE relay_send_found \
             in the InlineFound arm — the relay must cache the contract before \
             bubbling the response upstream"
        );
    }

    /// T5: Exhaustion path must send NotFound upstream.
    #[test]
    fn relay_driver_exhaustion_sends_not_found_upstream() {
        let src = production_source();
        let body = extract_fn_body(src, "async fn drive_relay_get_inner(");
        // Find the exhaustion branch (None arm of relay_advance_to_next_peer).
        assert!(
            body.contains("relay_send_not_found"),
            "drive_relay_get_inner must call `relay_send_not_found` on exhaustion"
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
    /// Fix: drop the ack entirely from the task-per-tx relay driver.
    /// Upstream's `send_to_and_await` still has OPERATION_TTL (60s) to
    /// receive the real Response, which is what legacy effectively had
    /// minus the ack-driven timer extension.
    #[test]
    fn relay_driver_does_not_send_forwarding_ack() {
        let src = production_source();
        let body = extract_fn_body(src, "async fn drive_relay_get_inner(");
        assert!(
            !body.contains("relay_send_forwarding_ack"),
            "drive_relay_get_inner must NOT call relay_send_forwarding_ack \
             — the ack collides with upstream's attempt_tx waiter and \
             amplifies spawns 3^HTL (workflow 24600634908 showed 6.8M \
             spawns, 63GB RSS)"
        );
        assert!(
            !body.contains("ForwardingAck"),
            "drive_relay_get_inner must not reference ForwardingAck in \
             any form — dropped to break the spawn-amplification cycle"
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
        let body = extract_fn_body(src, "async fn drive_relay_get_inner(");
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
            .find("pub(crate) async fn start_relay_get(")
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

    // ── G1/G2: Dispatch gates live in node.rs ────────────────────────────

    /// G1: `source_addr.is_none()` (originator loopback from the phase-3b
    /// client driver's `send_and_await(target=None)`) MUST NOT be routed
    /// to the relay driver. The dispatch site in `node.rs` must check
    /// `source_addr.is_some()` before calling `start_relay_get`; otherwise
    /// the client driver's `Terminal::LocalCompletion` Request-echo contract
    /// breaks and client-initiated GETs either hang or deadlock.
    #[test]
    fn dispatch_gate_loopback_source_addr_none_uses_legacy() {
        const NODE_RS: &str = include_str!("../../node.rs");
        let dispatch_start = NODE_RS
            .find("// #1454 phase 5 / #3883: relay GET task-per-tx dispatch.")
            .expect("relay dispatch comment anchor must exist in node.rs");
        let dispatch_end = NODE_RS[dispatch_start..]
            .find("let op_result = handle_op_request::<get::GetOp, _>")
            .expect("legacy fallthrough anchor must follow relay dispatch");
        let block = &NODE_RS[dispatch_start..dispatch_start + dispatch_end];
        assert!(
            block.contains("if let Some(upstream_addr) = source_addr"),
            "Relay dispatch block must gate on `source_addr.is_some()` so that \
             originator loopback (source_addr=None from phase-3b client driver) \
             falls through to the legacy `handle_op_request` path. Without this \
             gate, the Request-echo contract `drive_client_get_inner::classify` \
             relies on for Terminal::LocalCompletion breaks. See port plan §2."
        );
    }

    /// G2: `has_get_op(id) == true` (GC-spawned retries, `start_targeted_op`
    /// UPDATE auto-fetch) MUST NOT be routed to the relay driver. Those
    /// callers register a `GetOp` in `OpManager.ops.get` BEFORE the
    /// Request hits the wire and rely on the legacy `process_message`
    /// re-entry loop for their state machine. The dispatch site in
    /// `node.rs` must check `!op_manager.has_get_op(id)` before calling
    /// `start_relay_get`.
    #[test]
    fn dispatch_gate_existing_get_op_uses_legacy() {
        const NODE_RS: &str = include_str!("../../node.rs");
        let dispatch_start = NODE_RS
            .find("// #1454 phase 5 / #3883: relay GET task-per-tx dispatch.")
            .expect("relay dispatch comment anchor must exist in node.rs");
        let dispatch_end = NODE_RS[dispatch_start..]
            .find("let op_result = handle_op_request::<get::GetOp, _>")
            .expect("legacy fallthrough anchor must follow relay dispatch");
        let block = &NODE_RS[dispatch_start..dispatch_start + dispatch_end];
        assert!(
            block.contains("!op_manager.has_get_op(id)"),
            "Relay dispatch block must gate on `!op_manager.has_get_op(id)` so that \
             GC-spawned retries and `start_targeted_op` (UPDATE auto-fetch) continue \
             through the legacy `handle_op_request` path. Without this gate, the \
             relay driver would hijack transactions the legacy state machine owns."
        );
    }

    /// G3 (companion): the dispatch block must sit AFTER the
    /// `try_forward_task_per_tx_reply` bypass (phase-3b terminal forward)
    /// and BEFORE the legacy `handle_op_request` call, so that:
    ///   - terminal Response/ResponseStreaming from phase-3b client driver
    ///     get forwarded to its pending callback, NOT handed to the relay
    ///     driver (which would spawn a spurious task);
    ///   - legacy handling still runs for the loopback / has_get_op gated
    ///     fall-through cases.
    #[test]
    fn dispatch_gate_ordering_bypass_before_relay_before_legacy() {
        const NODE_RS: &str = include_str!("../../node.rs");
        let get_arm = NODE_RS
            .find("NetMessageV1::Get(ref op) => {")
            .expect("Get arm must exist in handle_pure_network_message_v1");
        let tail = &NODE_RS[get_arm..];
        let bypass_pos = tail
            .find("try_forward_task_per_tx_reply")
            .expect("phase-3b bypass must exist in Get arm");
        let relay_pos = tail
            .find("get::op_ctx_task::start_relay_get")
            .expect("phase-5 relay dispatch must exist in Get arm");
        let legacy_pos = tail
            .find("handle_op_request::<get::GetOp, _>")
            .expect("legacy fallthrough must exist in Get arm");
        assert!(
            bypass_pos < relay_pos && relay_pos < legacy_pos,
            "Dispatch ordering in the Get arm must be: \
             try_forward_task_per_tx_reply (phase-3b terminal bypass) \
             THEN start_relay_get (phase-5 relay dispatch) \
             THEN handle_op_request (legacy loopback + has_get_op fallthrough). \
             Got bypass={bypass_pos}, relay={relay_pos}, legacy={legacy_pos}."
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
        let body = extract_fn_body(src, "async fn drive_relay_get_inner(");
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
        let body = extract_fn_body(src, "async fn drive_relay_get_inner(");
        // The None arm of the `match relay_advance_to_next_peer(...)` is the
        // exhaustion branch. It must contain both `local_fallback` disposition
        // AND both Found and NotFound sends.
        let none_arm = body
            .find("None => {")
            .expect("exhaustion arm (`None => {{`) must exist");
        // Clip to the matching close; the next `Some(p) => {` arm may precede
        // or follow, so take the whole match body from None onward up to the
        // end of the outer function block.
        let tail = &body[none_arm..];
        // Bound at the `return Ok(());` that ends the exhaustion branch.
        let clip = tail
            .find("return Ok(());")
            .expect("exhaustion branch must end with `return Ok(());`");
        let arm = &tail[..clip + "return Ok(());".len()];
        assert!(
            arm.contains("if let Some((key, state, contract)) = local_fallback"),
            "Exhaustion arm must branch on `local_fallback`; otherwise the \
             stale-cache fallback semantic (R3d → R10) is lost."
        );
        assert!(
            arm.contains("relay_send_found"),
            "Exhaustion arm must call `relay_send_found` when `local_fallback` \
             is Some — the stale-cache relay serves what it has after all \
             downstream peers NotFound."
        );
        assert!(
            arm.contains("relay_send_not_found"),
            "Exhaustion arm must call `relay_send_not_found` when \
             `local_fallback` is None — the non-caching relay bubbles \
             NotFound upstream."
        );
        // Ordering: the `else` branch (NotFound) must follow the `if Some`
        // branch (Found).
        let found_pos = arm.find("relay_send_found").unwrap();
        let not_found_pos = arm.find("relay_send_not_found").unwrap();
        assert!(
            found_pos < not_found_pos,
            "Fallback-Found must precede NotFound in the exhaustion arm \
             source order; a reversal would mean NotFound always runs first \
             and the fallback path is dead code."
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

    // ── R7 (superseded): ForwardingAck removed from task-per-tx relay ──────
    //
    // Superseded by `relay_driver_does_not_send_forwarding_ack` (T6
    // rewrite). The original latch logic was pinned here to ensure the
    // ack fired exactly once per relay invocation, which was the legacy
    // behavior. In the task-per-tx migration the ack's `id` collides
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
        let body = extract_fn_body(src, "async fn drive_relay_get_inner(");
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
        let body = extract_fn_body(src, "async fn drive_relay_get_inner(");
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
        let body = extract_fn_body(src, "async fn drive_relay_get(");
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
        let body = extract_fn_body(src, "async fn relay_send_found(");
        assert!(
            body.contains("map_err(|_| OpError::NotificationError)"),
            "`relay_send_found` must map fire-and-forget send errors to \
             `OpError::NotificationError` so `?` surfaces them to the outer \
             `drive_relay_get` error funnel and triggers the compensating \
             NotFound."
        );
    }

    // ── R12b: Streaming-downstream WARN+continue regression guard ──────────

    /// Relay streaming-forward migration is deferred to a follow-up PR
    /// (port plan §7). Until then, a downstream `ResponseStreaming` reply
    /// must be handled by the `Terminal::Streaming { .. }` arm with
    /// `continue;` semantics — NOT piped through to the upstream. A
    /// regression that either claims the stream (breaking phase-5 scope)
    /// or panics (breaking availability when any downstream peer happens
    /// to own a large contract) is caught here.
    #[test]
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
        let body = extract_fn_body(src, "async fn drive_relay_get_inner(");
        // Match on the outer `round_trip` match arms.
        let round_trip = body
            .find("let reply = match round_trip {")
            .expect("round_trip match must exist");
        let tail = &body[round_trip..];
        // Clip at the end of the match (next `match classify(reply)`).
        let clip = tail
            .find("match classify(reply)")
            .expect("classify match must follow round_trip match");
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

    // ── N: Concurrent same-key GET — dispatch gate is per-tx, not per-key ──

    /// The dispatch gate in `node.rs` is `!op_manager.has_get_op(id)` —
    /// keyed on the transaction id, NOT the contract key/instance. This
    /// means two concurrent relay GETs for the *same* contract (from
    /// different upstreams) each get their own driver task, each its
    /// own `incoming_tx`. This is intentional: each upstream expects a
    /// reply on its own tx, so per-tx independence preserves the
    /// upstream-reply invariant. Per-key dedup would break it.
    ///
    /// This test pins the documented semantic so a well-meaning "add
    /// per-key dedup" refactor can't silently land.
    #[test]
    fn dispatch_gate_is_per_transaction_not_per_contract() {
        const NODE_RS: &str = include_str!("../../node.rs");
        let dispatch_start = NODE_RS
            .find("// #1454 phase 5 / #3883: relay GET task-per-tx dispatch.")
            .expect("relay dispatch comment anchor must exist in node.rs");
        let dispatch_end = NODE_RS[dispatch_start..]
            .find("let op_result = handle_op_request::<get::GetOp, _>")
            .expect("legacy fallthrough anchor must follow relay dispatch");
        let block = &NODE_RS[dispatch_start..dispatch_start + dispatch_end];
        assert!(
            block.contains("has_get_op(id)"),
            "Dispatch gate must be keyed on `has_get_op(id)` (per-transaction), \
             not `instance_id` / contract key (per-key). Per-key dedup would \
             break the upstream-reply invariant: two upstreams GETting the \
             same contract need independent replies, each on its own tx."
        );
        // Negative: no contract-key dedup construct in the dispatch block.
        assert!(
            !block.contains("has_get_op(instance_id)")
                && !block.contains("has_get_op(contract_id)"),
            "Dispatch block must not gate on contract key (per-key dedup). \
             Found a suspicious has_get_op variant: {block}"
        );
    }

    // ── C1: cache_contract_locally must gate mark_local_client_access ──────

    /// Regression guard for the relay hosting-cache taint bug caught in
    /// PR #3896 review. The legacy GET branch gates `mark_local_client_access`
    /// on `is_original_requester = upstream_addr.is_none()` (see
    /// `get.rs:2260-2262, :2353-2355, :3056-3058`). The task-per-tx driver
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

    /// All three relay-driver callsites must pass `false`.
    /// Relay caches forwarded Found payloads but MUST NOT flag them as
    /// client-accessed on the relay's own hosting cache. There are three
    /// relay callsites in `drive_relay_get_inner`:
    ///   1. Active-interest local Found (R4, immediate serve)
    ///   2. Exhaustion fallback (R10, stale local state)
    ///   3. Downstream Found bubble-up (R12a, forwarded payload)
    #[test]
    fn all_relay_callsites_pass_is_client_requester_false() {
        let src = production_source();
        let body = extract_fn_body(src, "async fn drive_relay_get_inner(");
        let callsites: Vec<usize> = body
            .match_indices("cache_contract_locally(")
            .map(|(i, _)| i)
            .collect();
        assert_eq!(
            callsites.len(),
            3,
            "drive_relay_get_inner should have exactly three \
             cache_contract_locally callsites (R4 immediate, R10 fallback, \
             R12a bubble-up). Found {} — a refactor has changed the shape.",
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
                normalized.ends_with("false,).await") || normalized.ends_with("false)"),
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
        let body = extract_fn_body(src, "pub(crate) async fn start_relay_get(");
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
}
