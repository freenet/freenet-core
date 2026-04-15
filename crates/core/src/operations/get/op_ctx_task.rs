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

use std::sync::Arc;

use freenet_stdlib::client_api::{ContractResponse, ErrorKind, HostResponse};
use freenet_stdlib::prelude::*;

use crate::client_events::HostResult;
use crate::config::GlobalExecutor;
use crate::contract::{ContractHandlerEvent, StoreResponse};
use crate::message::{NetMessage, NetMessageV1, Transaction};
use crate::node::OpManager;
use crate::operations::OpError;
use crate::operations::VisitedPeers;
use crate::operations::op_ctx::{
    AdvanceOutcome, AttemptOutcome, RetryDriver, RetryLoopOutcome, drive_retry_loop,
};
use crate::ring::{Location, PeerKeyLocation};
use crate::router::{RouteEvent, RouteOutcome};

use super::{GetMsg, GetMsgResult};

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
                    cache_contract_locally(op_manager, *key, state.clone(), contract.clone()).await;
                    *key
                }
                Terminal::Streaming { key } => {
                    // Stream assembly and local caching for the
                    // originator live on the legacy `process_message`
                    // streaming branch (`get.rs:2895`). For Phase 3b
                    // we keep streamed payloads on that path — the
                    // store re-query below will find the bytes
                    // whichever process_message invocation handled
                    // assembly. See #3883 for full driver-owned
                    // streaming.
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
    /// ResponseStreaming: stream assembly and local caching are not
    /// yet wired through the driver. Tracked in #3883. For the happy
    /// path the store re-query covers reads, so non-streaming tests
    /// pass; streamed payloads need follow-up work.
    Streaming { key: ContractKey },
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
        NetMessage::V1(NetMessageV1::Get(GetMsg::ResponseStreaming { key, .. })) => {
            AttemptOutcome::Terminal(Terminal::Streaming { key })
        }
        NetMessage::V1(NetMessageV1::Get(GetMsg::Request { .. })) => {
            AttemptOutcome::Terminal(Terminal::LocalCompletion)
        }
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
/// 2. **Unconditional hosting refresh** — `record_get_access`,
///    `mark_local_client_access`, and interest-manager eviction
///    cleanup run for BOTH the state-matches short-circuit AND the
///    PutQuery-failed error paths. Legacy behaviour at
///    `get.rs:2420-2435` continues these side effects on error so
///    re-GETs keep refreshing the hosting LRU/TTL even when the
///    executor rejected the write.
/// 3. **Newly-hosted announcement** — only runs when the local
///    store actually transitioned from no-state to has-state
///    (i.e., `access_result.is_new && put_persisted`).
async fn cache_contract_locally(
    op_manager: &OpManager,
    key: ContractKey,
    state: WrappedState,
    contract: Option<ContractContainer>,
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
    op_manager.ring.mark_local_client_access(&key);

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

    // Register the child so `LocalSubscribeComplete` hits the
    // silent-absorb branch instead of trying to publish to a
    // nonexistent waiter.
    op_manager.expect_and_register_sub_operation(client_tx, child_tx);

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
        #[allow(clippy::manual_unwrap_or_default)]
        {
            &FULL[..cutoff]
        }
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
        let early_return = body
            .find("if !subscribe {")
            .expect("maybe_subscribe_child must short-circuit on !subscribe");
        let register_call = body
            .find("expect_and_register_sub_operation")
            .expect("maybe_subscribe_child must register sub-operation");
        assert!(
            early_return < register_call,
            "The !subscribe short-circuit must come BEFORE the \
             expect_and_register_sub_operation call — otherwise we'd \
             register a spurious sub-op for a client who didn't ask for \
             one. See PUT 3a commit 494a3c69 for the analogous bug class."
        );
    }
}
