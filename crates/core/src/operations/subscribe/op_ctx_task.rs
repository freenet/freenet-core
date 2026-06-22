//! Task-per-transaction SUBSCRIBE drivers.
//!
//! Each entry point — client-initiated, executor auto-subscribe,
//! renewal, relay — owns its routing state in task locals. There is
//! no `ops.subscribe` DashMap; per-node dedup is enforced via
//! `OpManager.active_relay_subscribe_txs`.
//!
//! # Architecture
//!
//! 1. [`super::prepare_initial_request`] decides target peer vs
//!    local-completion vs give-up.
//! 2. Network target: loop calls [`OpCtx::send_and_await`][ocx] with
//!    a **fresh `Transaction` per attempt** (required by
//!    `send_and_await`'s single-use-per-tx contract). Replies route
//!    via the SUBSCRIBE bypass in `handle_pure_network_message_v1`.
//! 3. `Subscribed` → delivers via `result_router_tx` keyed by
//!    `client_tx`.
//! 4. `NotFound` → breadth retry → fresh k_closest → exhaustion.
//! 5. `send_and_await` timeout → treated as peer timeout.
//!
//! # Client-visible tx vs per-attempt tx
//!
//! - **`client_tx`**: allocated once by the WS handler, registered
//!   with `ch_outbound.waiting_for_transaction_result`, delivered to
//!   via `result_router_tx`. Never passed to `send_and_await`.
//! - **`attempt_tx`**: fresh per retry. Used for the wire Request,
//!   the `OpCtx`, and the `pending_op_results` callback slot.
//!
//! The split is mandatory: `OpCtx::send_and_await` fires once per
//! Transaction.
//!
//! [ocx]: crate::operations::OpCtx::send_and_await

use std::collections::HashSet;
use std::sync::Arc;
use std::time::Duration;

use freenet_stdlib::client_api::{ContractResponse, ErrorKind, HostResponse};
use freenet_stdlib::prelude::ContractInstanceId;

use crate::client_events::HostResult;
use crate::config::{GlobalExecutor, GlobalRng, OPERATION_TTL};
use crate::message::{NetMessage, NetMessageV1, Transaction};
use crate::node::OpManager;
use crate::operations::{OpError, VisitedPeers};
use crate::ring::{PeerKeyLocation, RingError};

use super::{
    InitialRequest, MAX_BREADTH, MAX_RETRIES, RETRY_BASE_DELAY, SubscribeMsg, SubscribeMsgResult,
    complete_local_subscription, prepare_initial_request, register_downstream_subscriber,
};

/// Start a client-initiated subscribe, returning as soon as the task has
/// been spawned (mirrors legacy `subscribe_with_id` timing).
///
/// The caller must have already registered a result waiter for `client_tx`
/// via `op_manager.ch_outbound.waiting_for_transaction_result` or
/// `waiting_for_subscription_result`. This function does NOT touch the
/// waiter; it only drives the ring/network side and publishes the terminal
/// result to `result_router_tx` keyed by `client_tx`.
///
/// Returns `Ok(client_tx)` once the task has been spawned. The spawned task
/// owns the rest of the subscribe lifetime.
pub(crate) async fn start_client_subscribe(
    op_manager: Arc<OpManager>,
    instance_id: ContractInstanceId,
    client_tx: Transaction,
) -> Result<Transaction, OpError> {
    // Phase 7 egress self-block (#4300): refuse to originate a
    // SUBSCRIBE for a contract this node has banned, BEFORE spawning
    // the driver, so we don't register interest in something we have
    // decided to reject. Mirrors the receive-side `SubscribeMsg::Request`
    // drop in node.rs (PR #4299). The client gets a typed
    // `ContractBanned` error.
    crate::operations::reject_if_contract_banned(&op_manager, &instance_id)?;

    tracing::debug!(
        tx = %client_tx,
        contract = %instance_id,
        "subscribe: spawning client-initiated task"
    );

    // Spawn the driver. The task is fire-and-forget from this function's
    // perspective — failures are delivered to the client via
    // `result_router_tx`, not via the return value of this function, to
    // match legacy `request_subscribe` behaviour (it pushes the op via
    // `notify_op_change` and returns `Ok(())` immediately, letting the
    // event loop drive the rest).
    //
    // Not registered with `BackgroundTaskMonitor`: per the decision tree
    // in `.claude/rules/code-style.md` under "WHEN spawning tasks with
    // `GlobalExecutor::spawn`", the monitor is for tasks that must run
    // for the node's lifetime. This driver is per-transaction and
    // terminates on its own via one of:
    //
    //   1. Happy path: `send_and_await` returns a terminal reply, loop exits.
    //   2. Exhaustion path: `advance_to_next_peer` returns `None`, loop exits.
    //   3. Per-attempt timeout: each `send_and_await` is wrapped in
    //      `tokio::time::timeout(OPERATION_TTL, ...)`; a timed-out attempt
    //      advances to the next peer or falls into the exhaustion path.
    //   4. `OpCtx::send_and_await` infrastructure error (executor channel
    //      closed / receiver dropped): surfaces as `DriverOutcome::InfrastructureError`
    //      and exits via `deliver_outcome`.
    //
    // Amplification ceiling: the WS SUBSCRIBE request path enforces
    // `MAX_SUBSCRIPTIONS_PER_CLIENT = 50` upstream via
    // `notify_contract_handler(RegisterSubscriberListener)` →
    // `runtime.rs:623` (Executor::register_subscription), which rejects
    // the registration BEFORE `subscribe_with_id` is called. A client
    // that tries to open more than 50 in-flight subscribes gets a
    // `SubscriberLimit` error from the contract handler and never
    // reaches this spawn site. In-flight task count is therefore
    // bounded by `num_clients * 50`, not unbounded.
    //
    // Leak detection: if the driver somehow gets stuck without exiting
    // any of the four paths above, `test_pending_op_results_bounded`
    // (which watches `pending_op_inserts - pending_op_removes`) will
    // flag the leak during simulation tests. Because every
    // `send_and_await` attempt both inserts (via `handle_op_execution`)
    // and removes (via `release_pending_op_slot`) a `pending_op_results`
    // slot, a stuck task would show up as a widening insert/remove gap.
    // Atomic admission gate + counter bump (closes the drain race
    // window). See `OpManager::admit_client_op` for the race
    // analysis. The guard is held for the lifetime of the spawned
    // driver. (`run_client_subscribe` is also called inline from
    // PUT's blocking_subscribe path; that caller is already counted
    // under PUT's guard, so we attach the guard only at the spawned
    // entry, not inside `run_client_subscribe`.)
    let inflight_guard = match op_manager.admit_client_op() {
        Some(g) => g,
        None => return Err(OpError::NodeShuttingDown),
    };
    GlobalExecutor::spawn(async move {
        let _inflight_guard = inflight_guard;
        run_client_subscribe(op_manager, instance_id, client_tx).await;
    });

    Ok(client_tx)
}

/// Start a node-initiated **directed** subscribe toward a specific first hop.
///
/// Used by the directed-subscribe placement path (#4404): on receiving a
/// [`SubscribeHint`](crate::message::NetMessageV1::SubscribeHint), the
/// recipient subscribes to `key` routed THROUGH `holder` (the hint sender,
/// the current holder) rather than via greedy ring selection — a plain
/// greedy subscribe would route away from the holder. Fetching the contract
/// as part of the subscribe causes the recipient to host it, migrating the
/// contract toward its ideal ring location.
///
/// Fire-and-forget: there is no client waiter. Like `start_client_subscribe`,
/// failures are swallowed inside the spawned driver (no result delivery).
/// Mirrors `start_client_subscribe`'s admission/ban guards but threads
/// `first_hop = Some(holder)` into the driver.
pub(crate) fn start_directed_subscribe(
    op_manager: Arc<OpManager>,
    key: freenet_stdlib::prelude::ContractKey,
    holder: PeerKeyLocation,
) {
    let instance_id = *key.id();

    // Egress self-block (#4300): refuse to originate a directed subscribe for
    // a contract this node has banned, mirroring `start_client_subscribe`.
    if crate::operations::reject_if_contract_banned(&op_manager, &instance_id).is_err() {
        tracing::debug!(
            contract = %instance_id,
            "directed subscribe: contract banned, ignoring hint"
        );
        return;
    }

    let inflight_guard = match op_manager.admit_client_op() {
        Some(g) => g,
        None => return,
    };
    let directed_tx = Transaction::new::<SubscribeMsg>();
    tracing::debug!(
        tx = %directed_tx,
        contract = %instance_id,
        holder = ?holder.socket_addr(),
        "directed subscribe: spawning hint-initiated task"
    );
    GlobalExecutor::spawn(async move {
        let _inflight_guard = inflight_guard;
        let outcome = drive_client_subscribe(
            op_manager.clone(),
            instance_id,
            directed_tx,
            /* is_renewal */ false,
            /* first_hop */ Some(holder),
        )
        .await;
        // Internal (migration) subscribe: there is NO client waiter. The
        // hosting side effects (host_contract, announce) already ran inside the
        // driver's `finalize_originator_subscribe`, so completion is just a log
        // here. Deliberately NOT routed through `deliver_outcome`: that would
        // count this toward the user-facing SUBSCRIBE op-stats and push a result
        // into `result_router_tx` that no client consumes (skewing stats and
        // occupying router capacity during a migration cascade). Best-effort:
        // a failed directed subscribe is retried on the next migration trigger.
        match outcome {
            DriverOutcome::Publish(Ok(_)) | DriverOutcome::SkipAlreadyDelivered => {
                tracing::debug!(
                    tx = %directed_tx,
                    contract = %instance_id,
                    "directed subscribe: completed (now hosting/subscribed)"
                );
            }
            DriverOutcome::Publish(Err(e)) => {
                tracing::debug!(
                    tx = %directed_tx,
                    contract = %instance_id,
                    error = ?e,
                    "directed subscribe: did not complete (best-effort; will retry on next trigger)"
                );
            }
            DriverOutcome::InfrastructureError(err) => {
                tracing::debug!(
                    tx = %directed_tx,
                    contract = %instance_id,
                    error = %err,
                    "directed subscribe: infrastructure error (best-effort)"
                );
            }
        }
    });
}

/// Drive a client-initiated subscribe to completion and publish the result
/// to `result_router_tx` keyed by `client_tx`.
///
/// Runs inside a spawned task. Never panics — any error is converted into
/// a `HostResult::Err` and delivered through `result_router_tx`.
///
/// `pub(crate)` so PUT's driver can `.await` this inline for
/// blocking-subscribe children without spawning a separate task.
pub(crate) async fn run_client_subscribe(
    op_manager: Arc<OpManager>,
    instance_id: ContractInstanceId,
    client_tx: Transaction,
) {
    // `is_renewal=false` here is hard-wired: every caller of this entry
    // point is a fresh client-style subscribe (WS, sub-op fallback) that
    // needs the responder to send state. Renewals call
    // [`run_renewal_subscribe`] directly, which goes through
    // [`drive_client_subscribe_inner`] with `is_renewal=true` AND a
    // different delivery path (returned outcome instead of
    // `result_router_tx`).
    let outcome = drive_client_subscribe(
        op_manager.clone(),
        instance_id,
        client_tx,
        /* is_renewal */ false,
        /* first_hop */ None,
    )
    .await;
    deliver_outcome(&op_manager, client_tx, instance_id, outcome);
}

/// Outcome of a renewal subscribe (`ring::connection_maintenance`).
///
/// Renewals do not deliver through `result_router_tx`; the renewal task
/// owns its own outcome observation for backoff bookkeeping
/// (`SubscriptionRecoveryGuard`) and `subscription_renewal_outcome`
/// telemetry. This enum carries the three semantic buckets the renewal
/// site needs to distinguish:
///
/// - [`Self::Success`]: a `Subscribed` reply was received (or local
///   completion) — record success, clear backoff.
/// - [`Self::Failed`]: subscribe could not complete (no peers, exhausted
///   retries, downstream errors) — record failure, apply backoff. The
///   `reason` field is opaque telemetry text — DO NOT parse it.
/// - [`Self::ChannelCongestion`]: a notification-channel error was
///   raised. Both [`OpError::NotificationError`] and
///   [`OpError::NotificationChannelError`] route here. Treated as a
///   local resource issue, NOT a protocol failure — clear pending mark
///   without penalising the contract on backoff.
///
/// Note on the channel-congestion bucket: the legacy `ring.rs` renewal
/// site only matched [`OpError::NotificationChannelError`] for the
/// no-penalty arm. The driver renewal driver also routes
/// [`OpError::NotificationError`] (returned from
/// [`mpsc::Sender::send`]'s `Err(SendError(_))` via [`OpError::from`])
/// into [`Self::ChannelCongestion`] because both indicate the same
/// underlying condition (a bounded notification channel could not
/// accept the operation). Treating them differently was a legacy
/// asymmetry, not a load-bearing distinction.
#[derive(Debug)]
pub(crate) enum RenewalOutcome {
    Success,
    Failed { reason: String },
    ChannelCongestion,
}

/// Drive a renewal subscribe to completion and return the outcome directly
/// instead of routing through `result_router_tx` / `SessionActor`.
///
/// Reuses [`drive_client_subscribe_inner`] with `is_renewal=true` so the
/// wire request, retry loop, and local-completion path are identical to
/// the client-initiated driver. The only divergence is delivery: results
/// are returned to the caller for backoff bookkeeping and renewal-cycle
/// telemetry rather than published via `send_client_result`.
///
/// # Congestion semantics
///
/// The legacy renewal site at `ring.rs::connection_maintenance` used
/// `notify_op_change_nonblocking` to fail-fast under congestion of the
/// 2048-cap event-loop notification channel. The driver renewal
/// driver dispatches each attempt through `OpCtx::send_to_and_await`,
/// which uses the separate `op_execution_sender` channel; that channel
/// has its own bound and behaviour, so the legacy fail-fast contract no
/// longer applies at this layer.
///
/// Two safeguards bound the blast radius:
///
/// 1. Each renewal runs in its own spawned task — a blocked
///    `op_execution_sender.send().await` only stalls that one renewal
///    task, not the renewal-spawning loop in `connection_maintenance`.
///    The spawn loop continues to honour the per-tick channel-capacity
///    backpressure check on `notifications_sender` (see
///    `RENEWAL_DEFER_CAPACITY_FRACTION` / `RENEWAL_STOP_CAPACITY_FRACTION`).
///
/// 2. The renewal-spawn site wraps this future in a
///    `tokio::time::timeout` matching the renewal cycle interval, so a
///    stuck driver can't outlive its `mark_subscription_pending` mark
///    and accumulate behind successive cycles.
///
/// `op_execution_sender` failures surface as `OpError::NotificationError`
/// (from `From<SendError<_>>`) and route into
/// [`RenewalOutcome::ChannelCongestion`] — see [`classify_renewal_result`].
pub(crate) async fn run_renewal_subscribe(
    op_manager: Arc<OpManager>,
    instance_id: ContractInstanceId,
    renewal_tx: Transaction,
) -> RenewalOutcome {
    let result = drive_client_subscribe_inner(
        &op_manager,
        instance_id,
        renewal_tx,
        /* is_renewal */ true,
        /* first_hop */ None,
    )
    .await;
    classify_renewal_result(result)
}

/// Pure classifier mapping `drive_client_subscribe_inner`'s result into
/// [`RenewalOutcome`]. Factored out so the wildcard `OpError` arm has
/// direct unit-test coverage — without this a future `OpError` variant
/// representing local resource pressure could silently land in
/// [`RenewalOutcome::Failed`] (penalising the contract on backoff and
/// reopening the #3763 storm regression vector).
fn classify_renewal_result(result: Result<DriverOutcome, OpError>) -> RenewalOutcome {
    let infra_err = match result {
        Ok(DriverOutcome::Publish(Ok(_))) | Ok(DriverOutcome::SkipAlreadyDelivered) => {
            return RenewalOutcome::Success;
        }
        Ok(DriverOutcome::Publish(Err(host_err))) => {
            return RenewalOutcome::Failed {
                reason: host_err.to_string(),
            };
        }
        Ok(DriverOutcome::InfrastructureError(err)) | Err(err) => err,
    };

    // Wildcard intentional: every other OpError variant maps to the
    // same renewal-failure bucket. Future variants should default to
    // `Failed` unless they represent local resource pressure (in
    // which case extend the channel-congestion arm explicitly AND add
    // a unit test in `classify_renewal_result_*` below).
    #[allow(clippy::wildcard_enum_match_arm)]
    match infra_err {
        OpError::NotificationError | OpError::NotificationChannelError(_) => {
            RenewalOutcome::ChannelCongestion
        }
        other => RenewalOutcome::Failed {
            reason: other.to_string(),
        },
    }
}

/// Drive an executor-initiated SUBSCRIBE (`executor::subscribe`) to
/// completion and return the outcome directly to the caller via the
/// function return value.
///
/// The executor's `subscribe()` (in `contract::executor::runtime`) calls
/// this in place of the legacy `op_request(SubscribeContract)` →
/// `notify_op_change` mediator path. Like
/// [`run_renewal_subscribe`], delivery happens through the return type
/// instead of through `result_router_tx` / `SessionActor`: the executor
/// is an internal caller with no client waiter to publish to.
///
/// # Local-hit handling
///
/// Unlike [`drive_client_subscribe_inner`], this entry point pre-checks
/// the local-completion case via [`prepare_initial_request`] and handles
/// it inline (interest registration + `op_manager.completed(tx)`). It
/// deliberately does NOT call [`complete_local_subscription`], which
/// would emit `NodeEvent::LocalSubscribeComplete` and trip the
/// standalone-parent branch in `p2p_protoc.rs` that publishes a result
/// keyed on `executor_tx` to `result_router_tx`. The executor has no
/// client waiter for `executor_tx`, so the publish would consume a
/// `result_router_tx` slot and trigger a "no waiting clients" warning.
/// Renewals avoid this via the `is_renewal` short-circuit in the
/// handler (`p2p_protoc.rs:2072`); executor auto-subscribes avoid it
/// here.
///
/// # Network-hit handling
///
/// For the non-local case, delegate to [`drive_client_subscribe_inner`]
/// with `is_renewal=false` so the wire request, retry loop, and
/// per-attempt telemetry are identical to the client-initiated driver.
/// The responder sends full state because executor auto-subscribes are
/// not renewals — the executor invokes them when a contract is first
/// being put / updated and needs the responder to acknowledge with
/// state.
///
/// # Returns
///
/// `Ok(())` on a successful subscribe (network or local).
/// `Err(ExecutorSubscribeError)` on exhaustion / infrastructure
/// failure. The executor wraps the error into
/// `ExecutorError::other(...)` at the call site.
pub(crate) async fn run_executor_subscribe(
    op_manager: Arc<OpManager>,
    instance_id: ContractInstanceId,
    executor_tx: Transaction,
) -> Result<(), ExecutorSubscribeError> {
    // Pre-check local hit so we can skip the
    // `NodeEvent::LocalSubscribeComplete` round-trip (see rustdoc).
    match prepare_initial_request(
        &op_manager,
        executor_tx,
        instance_id,
        /* is_renewal */ false,
        /* first_hop */ None,
    )
    .await
    {
        Ok(InitialRequest::LocallyComplete { key }) => {
            tracing::debug!(
                %key,
                tx = %executor_tx,
                "executor subscribe: local hit, completing inline"
            );
            // Mirror the interest-registration side-effect of
            // `complete_local_subscription` for non-renewal locals.
            // Skip the `LocalSubscribeComplete` event because the
            // executor has no client waiter on `executor_tx`.
            let became_interested = op_manager.interest_manager.add_local_client(&key);
            if became_interested {
                crate::operations::broadcast_change_interests(&op_manager, vec![key], vec![]).await;
            }
            op_manager.completed(executor_tx);
            return Ok(());
        }
        Ok(InitialRequest::NoHostingPeers) => {
            return Err(ExecutorSubscribeError::Infra(
                RingError::NoHostingPeers(instance_id).into(),
            ));
        }
        Ok(InitialRequest::PeerNotJoined) => {
            return Err(ExecutorSubscribeError::Infra(
                RingError::PeerNotJoined.into(),
            ));
        }
        Ok(InitialRequest::NetworkRequest { .. }) => {
            // Network case falls through to the inner driver, which
            // re-runs `prepare_initial_request` itself. The double
            // call is benign — the function is read-only against
            // the ring + state store, and the second call sees the
            // same ring state because `executor_tx` doesn't move
            // between calls. If state changes between the two
            // calls and the second observes a local hit, the inner
            // driver's `complete_local_subscription` would emit
            // `LocalSubscribeComplete`, but that's a transient race
            // (single result-router slot, no client waiter) and
            // self-heals on next executor invocation.
        }
        Err(err) => return Err(ExecutorSubscribeError::Infra(err)),
    }

    let result = drive_client_subscribe_inner(
        &op_manager,
        instance_id,
        executor_tx,
        /* is_renewal */ false,
        /* first_hop */ None,
    )
    .await;
    classify_executor_subscribe_result(result)
}

/// Outcome of an executor-driven subscribe, carrying the structured
/// failure cause separately from infrastructure errors. The executor's
/// `subscribe()` wraps both into `ExecutorError::other`, but the
/// distinction matters for any future caller that wants to discriminate
/// "couldn't reach a peer" from "local notification channel closed".
///
/// Modelled as a separate enum (rather than reusing
/// `OpError::NotificationChannelError`) so the wire-level exhaustion
/// case has a meaningful name in the taxonomy.
#[derive(Debug, thiserror::Error)]
pub(crate) enum ExecutorSubscribeError {
    /// Wire-level exhaustion (driver retried + gave up, or remote peer
    /// rejected with NotFound). Carries the underlying client-error
    /// text for telemetry / logging.
    #[error("executor subscribe: network exhausted: {0}")]
    NetworkExhausted(String),
    /// Local infrastructure failure surfaced by
    /// [`drive_client_subscribe_inner`] (e.g.,
    /// [`OpError::NotificationError`], peer-not-joined, ring error).
    #[error("executor subscribe: {0}")]
    Infra(#[source] OpError),
}

/// Pure classifier mapping `drive_client_subscribe_inner`'s result into
/// `Result<(), ExecutorSubscribeError>` for executor-side delivery.
/// Factored out so the conversion has direct unit-test coverage.
fn classify_executor_subscribe_result(
    result: Result<DriverOutcome, OpError>,
) -> Result<(), ExecutorSubscribeError> {
    match result {
        Ok(DriverOutcome::Publish(Ok(_))) | Ok(DriverOutcome::SkipAlreadyDelivered) => Ok(()),
        Ok(DriverOutcome::Publish(Err(host_err))) => Err(ExecutorSubscribeError::NetworkExhausted(
            host_err.to_string(),
        )),
        Ok(DriverOutcome::InfrastructureError(err)) | Err(err) => {
            Err(ExecutorSubscribeError::Infra(err))
        }
    }
}

/// Outcome of the driver, carrying an explicit signal for "local completion
/// already published to the router, no follow-up `result_router_tx` send
/// needed". Using an enum here instead of piggybacking on `OpManager::is_completed`
/// makes the skip condition explicit and unshareable with any other path
/// that might happen to mark the same tx completed (review finding M3).
#[derive(Debug)]
enum DriverOutcome {
    /// The driver produced a `HostResult` that must be published via
    /// `result_router_tx`.
    Publish(HostResult),
    /// Local completion already published via
    /// `NodeEvent::LocalSubscribeComplete` inside
    /// `complete_local_subscription`. The driver must NOT publish a
    /// second result for this tx — doing so would duplicate the
    /// `HostResponse::ContractResponse(SubscribeResponse)` the
    /// `LocalSubscribeComplete` handler already pushed.
    SkipAlreadyDelivered,
    /// A genuine infrastructure failure escaped the driver loop
    /// (e.g., executor channel closed, unexpected reply variant).
    /// `deliver_outcome` converts this into a synthesized client error.
    InfrastructureError(OpError),
}

/// The inner driver: returns a [`DriverOutcome`] describing how the
/// subscribe terminated and whether the delivery side-effect has already
/// been applied.
async fn drive_client_subscribe(
    op_manager: Arc<OpManager>,
    instance_id: ContractInstanceId,
    client_tx: Transaction,
    is_renewal: bool,
    first_hop: Option<PeerKeyLocation>,
) -> DriverOutcome {
    match drive_client_subscribe_inner(&op_manager, instance_id, client_tx, is_renewal, first_hop)
        .await
    {
        Ok(outcome) => outcome,
        Err(err) => DriverOutcome::InfrastructureError(err),
    }
}

async fn drive_client_subscribe_inner(
    op_manager: &Arc<OpManager>,
    instance_id: ContractInstanceId,
    client_tx: Transaction,
    is_renewal: bool,
    first_hop: Option<PeerKeyLocation>,
) -> Result<DriverOutcome, OpError> {
    // Decide: local-completion, give up, or send to the network.
    // `prepare_initial_request` uses `client_tx` for its visited-peers
    // bloom filter seed and telemetry; this is fine because the bloom
    // filter is per-attempt-first-peer only and telemetry correlates on
    // the client-visible tx (matching legacy behaviour for the first
    // attempt).
    // Retain the directed holder (if any) so the post-Subscribed body fetch can
    // be routed THROUGH it rather than greedily. A greedy sub-op GET toward the
    // key could dead-end at the same close non-hosting cluster the migration is
    // resolving, leaving this peer subscribed-but-bodyless. `None` for ordinary
    // (non-directed) subscribes, which keep the greedy fetch.
    let directed_holder = first_hop.clone();
    let initial =
        prepare_initial_request(op_manager, client_tx, instance_id, is_renewal, first_hop).await?;

    let (target_peer, target_addr, mut visited, mut alternatives, htl) = match initial {
        InitialRequest::LocallyComplete { key } => {
            // Local completion reuses the existing helper. It publishes
            // via `NodeEvent::LocalSubscribeComplete` → `result_router_tx`,
            // so the driver MUST NOT deliver a second time — return
            // `SkipAlreadyDelivered` to explicitly signal that to
            // `deliver_outcome`.
            complete_local_subscription(op_manager, client_tx, key, is_renewal).await?;
            return Ok(DriverOutcome::SkipAlreadyDelivered);
        }
        InitialRequest::NoHostingPeers => {
            return Ok(DriverOutcome::Publish(Err(ErrorKind::OperationError {
                cause: format!("no remote peers available for subscription to {instance_id}")
                    .into(),
            }
            .into())));
        }
        InitialRequest::PeerNotJoined => {
            return Err(RingError::PeerNotJoined.into());
        }
        InitialRequest::NetworkRequest {
            target,
            target_addr,
            visited,
            alternatives,
            htl,
        } => (target, target_addr, visited, alternatives, htl),
    };

    tracing::debug!(
        tx = %client_tx,
        contract = %instance_id,
        target = %target_addr,
        "subscribe: initial target selected, entering retry loop"
    );

    // Initial state for the retry loop. The first iteration uses
    // `target_peer` (as the current target) from `prepare_initial_request`;
    // subsequent attempts pull from `advance_to_next_peer`.
    //
    // `prepare_initial_request` has already emitted a
    // `NetEventLog::subscribe_request` event keyed on `client_tx` for the
    // first attempt (mirroring legacy behaviour). Subsequent attempts
    // re-emit inside the loop using the per-attempt tx so retries are
    // visible in the event log (review finding L4).
    let mut tried_peers: HashSet<std::net::SocketAddr> = HashSet::new();
    tried_peers.insert(target_addr);
    let mut retries: usize = 0;
    let mut attempts_at_hop: usize = 1;
    let mut current_target: PeerKeyLocation = target_peer;
    let mut current_target_addr: std::net::SocketAddr = target_addr;
    let mut is_first_attempt = true;

    // Renewal task budget (issue #4350). `recover_orphaned_subscriptions`
    // wraps each renewal task in an outer cancel deadline
    // (`Ring::renewal_outer_cancel`); before this fix the driver's per-attempt
    // wait used the global `OPERATION_TTL` (60 s) while the outer deadline was
    // only 25 s, so a peer replying between 25 s and 60 s was killed by the
    // outer cancel mid-`await`, discarding the in-flight reply onto the
    // already-dropped renewal receiver.
    //
    // The renewal path now gives itself a total budget
    // (`Ring::RENEWAL_TASK_BUDGET`, 20 s) and clamps EACH attempt's timeout to
    // the budget remaining until that deadline. This bounds not just the first
    // attempt but every retry: no attempt can still be awaiting when the outer
    // cancel fires, so a slow peer produces at most one clean, in-task timeout
    // per cycle (clean failure + telemetry on the exhaustion branch below)
    // instead of a mid-await cancellation that discards an in-flight reply. The
    // outer cancel is sized (in `renewal_outer_cancel`) to also clear the
    // driver's post-loop `release_pending_op_slot` cleanup. Client / executor /
    // directed subscribes have no outer cancel deadline and keep `OPERATION_TTL`
    // with no budget (`renewal_deadline = None`).
    let renewal_deadline =
        is_renewal.then(|| tokio::time::Instant::now() + crate::ring::Ring::RENEWAL_TASK_BUDGET);

    loop {
        // For renewals, clamp this attempt's wait to the budget remaining until
        // the task deadline (capped at `RENEWAL_PER_ATTEMPT_TIMEOUT`). When the
        // remaining budget is too small to complete a useful round-trip, stop
        // cleanly and let the next recovery cycle retry rather than starting an
        // attempt that the outer cancel would cut short. Non-renewal paths use
        // the global `OPERATION_TTL`.
        let attempt_timeout = match renewal_deadline {
            Some(deadline) => {
                let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
                if remaining < crate::ring::Ring::RENEWAL_MIN_ATTEMPT_BUDGET {
                    tracing::debug!(
                        tx = %client_tx,
                        contract = %instance_id,
                        retries,
                        remaining_ms = remaining.as_millis() as u64,
                        "subscribe renewal: task budget exhausted; stopping before outer cancel"
                    );
                    return Ok(DriverOutcome::Publish(Err(ErrorKind::OperationError {
                        cause: format!(
                            "renewal to {instance_id} ran out of task budget after {} rounds",
                            retries + 1
                        )
                        .into(),
                    }
                    .into())));
                }
                remaining.min(crate::ring::Ring::RENEWAL_PER_ATTEMPT_TIMEOUT)
            }
            None => OPERATION_TTL,
        };

        // Fresh attempt tx: single-use-per-tx for send_and_await.
        let attempt_tx = Transaction::new::<SubscribeMsg>();

        tracing::debug!(
            tx = %client_tx,
            attempt_tx = %attempt_tx,
            target = %current_target_addr,
            retries,
            attempts_at_hop,
            "subscribe: sending attempt"
        );

        // Per-attempt telemetry (review finding L4). `prepare_initial_request`
        // emits the first-attempt event on `client_tx`; every retry after
        // that emits on the fresh `attempt_tx` so the event log captures
        // the full retry chain.
        if !is_first_attempt {
            if let Some(event) = crate::tracing::NetEventLog::subscribe_request(
                &attempt_tx,
                &op_manager.ring,
                instance_id,
                current_target.clone(),
                htl,
            ) {
                op_manager
                    .ring
                    .register_events(either::Either::Left(event))
                    .await;
            }
        }
        is_first_attempt = false;

        let request = SubscribeMsg::Request {
            id: attempt_tx,
            instance_id,
            htl,
            visited: visited.clone(),
            is_renewal,
        };

        // Dispatch via `send_to_and_await` so the Request reaches `current_target_addr`
        // on the wire instead of looping back to `process_message` as a local
        // `InboundMessage`. The local short-circuit would synthesize a success
        // reply when the contract is cached locally (e.g., after a prior GET),
        // but the home node would never see the request and never register us
        // as a downstream subscriber — so subsequent UPDATE broadcasts would
        // never reach this peer. See issue #3838 and the doc comment on
        // `OpCtx::send_to_and_await`.
        let mut ctx = op_manager.op_ctx(attempt_tx);
        // Capture wall-clock at send for routing-prediction telemetry. The
        // winning attempt's `request_sent_at` paired with the time of
        // terminal-reply arrival feeds the router's
        // `response_start_time_estimator` via `RouteOutcome::Success`.
        // `tokio::time::Instant` is auto-paused in test builds with
        // `start_paused(true)`, so simulation tests see virtual elapsed
        // time rather than wall-clock — matches the rest of
        // `crates/core/`'s timing primitives.
        let request_sent_at = tokio::time::Instant::now();
        let round_trip = tokio::time::timeout(
            attempt_timeout,
            ctx.send_to_and_await(current_target_addr, NetMessage::from(request)),
        )
        .await;

        // Release the per-attempt `pending_op_results` slot before any
        // retry or return. Without this emission entries would only be
        // reclaimed by the 60 s periodic sweep of closed senders
        // (p2p_protoc.rs:960-987), which lags the real completion by
        // several seconds per attempt and causes the
        // `test_pending_op_results_bounded` regression guard to flag a
        // leak. We emit the event regardless of outcome (success, wire
        // error, timeout) because the inserted callback slot is keyed
        // only on `attempt_tx` — its lifetime matches the attempt, not
        // the reply classification.
        //
        // `release_pending_op_slot` uses a timeout-wrapped `send().await`
        // rather than `try_send` so the cleanup survives transient
        // notification-channel backpressure (review finding M1). We are
        // inside a spawned task, not an event loop, so `send().await` is
        // within the channel-safety rules.
        op_manager.release_pending_op_slot(attempt_tx).await;

        let reply = match round_trip {
            Ok(Ok(reply)) => reply,
            Ok(Err(err)) => {
                // `send_and_await` infrastructure failure (executor
                // channel closed, receiver dropped). Distinct from a
                // wire-level `NotFound` for observability purposes:
                // emit a structured `outcome=wire_error` field so log
                // analytics can tell them apart. Retry behaviour is
                // the same as NotFound — from "should we try another
                // peer?" the answer is yes — so the downstream logic
                // is shared (review finding T-4).
                tracing::warn!(
                    tx = %client_tx,
                    attempt_tx = %attempt_tx,
                    target = %current_target_addr,
                    retries,
                    attempts_at_hop,
                    outcome = "wire_error",
                    error = %err,
                    "subscribe: send_and_await failed; advancing to next peer"
                );
                match advance_to_next_peer(
                    op_manager,
                    &instance_id,
                    &mut visited,
                    &mut tried_peers,
                    &mut alternatives,
                    &mut retries,
                    &mut attempts_at_hop,
                )
                .await
                {
                    Some((next_target, next_addr)) => {
                        current_target = next_target;
                        current_target_addr = next_addr;
                        // Phase 2b spin-loop guard (#3808): pace retries
                        // with a small jittered delay before re-sending.
                        // Without this the driver re-issues immediately on
                        // each reply, burning the whole retry budget at
                        // network speed against a contract no peer hosts.
                        sleep_before_retry().await;
                        continue;
                    }
                    None => {
                        return Ok(DriverOutcome::Publish(Err(ErrorKind::OperationError {
                            cause: format!(
                                "subscribe to {instance_id} failed after {} rounds (last peer error: {err})",
                                retries + 1
                            )
                            .into(),
                        }
                        .into())));
                    }
                }
            }
            Err(_) => {
                // `attempt_timeout` elapsed without the peer producing a
                // terminal reply (`OPERATION_TTL` for client/executor/directed
                // subscribes; for renewals, the remaining task budget clamped
                // at `RENEWAL_PER_ATTEMPT_TIMEOUT` — see #4350). Distinct from
                // `wire_error` (which is
                // an infrastructure failure on the executor/send side)
                // and from `not_found` (a legitimate wire-level
                // response). `outcome=timeout` (review finding T-4).
                tracing::warn!(
                    tx = %client_tx,
                    attempt_tx = %attempt_tx,
                    target = %current_target_addr,
                    retries,
                    attempts_at_hop,
                    outcome = "timeout",
                    timeout_secs = attempt_timeout.as_secs(),
                    is_renewal,
                    "subscribe: attempt timed out; advancing to next peer"
                );
                match advance_to_next_peer(
                    op_manager,
                    &instance_id,
                    &mut visited,
                    &mut tried_peers,
                    &mut alternatives,
                    &mut retries,
                    &mut attempts_at_hop,
                )
                .await
                {
                    Some((next_target, next_addr)) => {
                        current_target = next_target;
                        current_target_addr = next_addr;
                        // Phase 2b spin-loop guard (#3808): pace retries
                        // with a small jittered delay before re-sending.
                        // Without this the driver re-issues immediately on
                        // each reply, burning the whole retry budget at
                        // network speed against a contract no peer hosts.
                        sleep_before_retry().await;
                        continue;
                    }
                    None => {
                        // #3445: emit a terminal telemetry event so a
                        // timed-out client subscribe is no longer invisible
                        // on the dashboard. Keyed on `client_tx` (the same
                        // tx the originating `subscribe_request` was keyed
                        // on) so the request pairs with this outcome.
                        if let Some(event) = crate::tracing::NetEventLog::subscribe_timeout(
                            &client_tx,
                            &op_manager.ring,
                            instance_id,
                            retries + 1,
                        ) {
                            op_manager
                                .ring
                                .register_events(either::Either::Left(event))
                                .await;
                        }
                        return Ok(DriverOutcome::Publish(Err(ErrorKind::OperationError {
                            cause: format!(
                                "subscribe to {instance_id} timed out after {} rounds",
                                retries + 1
                            )
                            .into(),
                        }
                        .into())));
                    }
                }
            }
        };

        // Classify the terminal reply.
        match classify_reply(&reply) {
            ReplyClass::Subscribed { key } => {
                let time_to_response_start = request_sent_at.elapsed();
                tracing::info!(
                    tx = %client_tx,
                    attempt_tx = %attempt_tx,
                    contract = %key,
                    target = %current_target_addr,
                    retries,
                    attempts_at_hop,
                    outcome = "subscribed",
                    "subscribe: subscribed"
                );

                // Feed the routing-prediction model on the client-initiated
                // SUBSCRIBE success path. The legacy `subscribe.rs::outcome()`
                // emitted `ContractOpSuccess { first_response_time, payload_size: 0,
                // payload_transfer_time: ZERO }` so the router's
                // `response_start_time_estimator` learned from every successful
                // subscribe (payload_size==0 deliberately skips
                // `transfer_rate_estimator`). The migration of this
                // path stopped emitting any RouteEvent at all, leaving the
                // response-time estimator with zero observations from
                // client-initiated subscribes. Restore that feedback so the
                // peer dashboard's Response Time chart populates again.
                let contract_location = crate::ring::Location::from(&key);
                let route_event = crate::router::RouteEvent {
                    peer: current_target.clone(),
                    contract_location,
                    outcome: crate::router::RouteOutcome::Success {
                        time_to_response_start,
                        payload_size: 0,
                        payload_transfer_time: std::time::Duration::ZERO,
                    },
                    op_type: Some(crate::node::network_status::OpType::Subscribe),
                };
                if let Some(log_event) = crate::tracing::NetEventLog::route_event(
                    &client_tx,
                    &op_manager.ring,
                    &route_event,
                ) {
                    op_manager
                        .ring
                        .register_events(either::Either::Left(log_event))
                        .await;
                }
                op_manager.ring.routing_finished(route_event);
                // Run originator-side finalization. This is the single
                // place that owns the post-Subscribed side effects:
                // upstream-peer registration (#3874), lease install,
                // backoff clear, contract-body fetch (#4223), neighbor
                // announce, and local-interest bookkeeping. Keeping all
                // of it inside `finalize_originator_subscribe` lets the
                // executor-subscribe entry (`run_executor_subscribe`)
                // and any future driver entry inherit the same shape
                // by calling the same helper.
                super::finalize_originator_subscribe(
                    op_manager,
                    key,
                    current_target_addr,
                    is_renewal,
                    directed_holder.clone(),
                )
                .await;
                return Ok(DriverOutcome::Publish(Ok(HostResponse::ContractResponse(
                    ContractResponse::SubscribeResponse {
                        key,
                        subscribed: true,
                    },
                ))));
            }
            ReplyClass::NotFound => {
                // Wire-level NotFound from a legitimate peer response.
                // Distinct from `wire_error` (executor/send failure)
                // and `timeout` (no terminal reply at all), so log
                // analytics can count "contract not found at this
                // peer" separately from infrastructure issues (review
                // finding T-4).
                tracing::debug!(
                    tx = %client_tx,
                    attempt_tx = %attempt_tx,
                    target = %current_target_addr,
                    retries,
                    attempts_at_hop,
                    outcome = "not_found",
                    "subscribe: NotFound from peer; advancing to next peer"
                );
                match advance_to_next_peer(
                    op_manager,
                    &instance_id,
                    &mut visited,
                    &mut tried_peers,
                    &mut alternatives,
                    &mut retries,
                    &mut attempts_at_hop,
                )
                .await
                {
                    Some((next_target, next_addr)) => {
                        current_target = next_target;
                        current_target_addr = next_addr;
                        // Phase 2b spin-loop guard (#3808): pace retries
                        // with a small jittered delay before re-sending.
                        // Without this the driver re-issues immediately on
                        // each reply, burning the whole retry budget at
                        // network speed against a contract no peer hosts.
                        sleep_before_retry().await;
                        continue;
                    }
                    None => {
                        return Ok(DriverOutcome::Publish(Err(ErrorKind::OperationError {
                            cause: format!(
                                "contract {instance_id} not found after exhaustive search"
                            )
                            .into(),
                        }
                        .into())));
                    }
                }
            }
            ReplyClass::Unexpected => {
                tracing::warn!(
                    tx = %client_tx,
                    attempt_tx = %attempt_tx,
                    "subscribe: unexpected terminal reply"
                );
                return Err(OpError::UnexpectedOpState);
            }
        }
    }
}

/// Classification of a terminal reply delivered to `send_and_await`.
#[derive(Debug)]
enum ReplyClass {
    Subscribed {
        key: freenet_stdlib::prelude::ContractKey,
    },
    NotFound,
    /// Any reply that shouldn't be a terminal for the originator
    /// (e.g., a `Request`, `Unsubscribe`, or `ForwardingAck`). These
    /// indicate a classification bug upstream and are surfaced as errors.
    Unexpected,
}

fn classify_reply(msg: &NetMessage) -> ReplyClass {
    match msg {
        NetMessage::V1(NetMessageV1::Subscribe(SubscribeMsg::Response { result, .. })) => {
            match result {
                SubscribeMsgResult::Subscribed { key } => ReplyClass::Subscribed { key: *key },
                SubscribeMsgResult::NotFound => ReplyClass::NotFound,
            }
        }
        _ => ReplyClass::Unexpected,
    }
}

/// Advance the task-local routing state to the next peer to try
/// (NotFound + alternatives-exhausted logic).
///
/// Thin wrapper around [`advance_to_next_peer_impl`] that binds the
/// `fresh_candidates` hook to `op_manager.ring.k_closest_potentially_hosting`.
/// Splitting the bind out keeps the retry decision logic unit-testable
/// without needing a full `OpManager` + `Ring` setup (review finding
/// Testing #2).
async fn advance_to_next_peer(
    op_manager: &OpManager,
    instance_id: &ContractInstanceId,
    visited: &mut VisitedPeers,
    tried_peers: &mut HashSet<std::net::SocketAddr>,
    alternatives: &mut Vec<PeerKeyLocation>,
    retries: &mut usize,
    attempts_at_hop: &mut usize,
) -> Option<(PeerKeyLocation, std::net::SocketAddr)> {
    advance_to_next_peer_impl(
        instance_id,
        visited,
        tried_peers,
        alternatives,
        retries,
        attempts_at_hop,
        |instance_id, visited| {
            op_manager
                .ring
                .k_closest_potentially_hosting(instance_id, visited, MAX_BREADTH)
        },
    )
}

/// Core decision logic, parameterized on a `fresh_candidates` hook so
/// tests can drive it without a real `Ring`.
///
/// Priority:
/// 1. If `attempts_at_hop < MAX_BREADTH` and `alternatives` is non-empty,
///    take the next alternative (breadth retry at the same hop, FIFO
///    order to match legacy `handle_op_response`).
/// 2. Otherwise, if `retries < MAX_RETRIES`, call `fresh_candidates` with
///    the accumulated `visited` filter, reset `attempts_at_hop` to 1,
///    and increment `retries`.
/// 3. Otherwise, return `None` (exhausted).
///
/// Mutates all state references on a successful advance. The hook is a
/// synchronous `Fn` rather than `async fn` because the real
/// `k_closest_potentially_hosting` is also synchronous; this keeps the
/// signature simple and `impl`-backed. No `.await` inside the body means
/// the whole decision function can be a plain (non-async) fn.
fn advance_to_next_peer_impl<F>(
    instance_id: &ContractInstanceId,
    visited: &mut VisitedPeers,
    tried_peers: &mut HashSet<std::net::SocketAddr>,
    alternatives: &mut Vec<PeerKeyLocation>,
    retries: &mut usize,
    attempts_at_hop: &mut usize,
    mut fresh_candidates: F,
) -> Option<(PeerKeyLocation, std::net::SocketAddr)>
where
    F: FnMut(&ContractInstanceId, &VisitedPeers) -> Vec<PeerKeyLocation>,
{
    // 1. Breadth retry at the same hop. Use FIFO (`remove(0)`) to match the
    //    legacy `handle_op_response` ordering (`subscribe.rs:949, 1821`).
    //    `alternatives` is built in closest-first order by
    //    `k_closest_potentially_hosting`, so FIFO means we try the best
    //    candidate we have not yet tried. LIFO (`pop()`) would iterate in
    //    reverse-distance order and diverge from legacy routing behaviour.
    if *attempts_at_hop < MAX_BREADTH {
        while !alternatives.is_empty() {
            let candidate = alternatives.remove(0);
            if let Some(addr) = candidate.socket_addr() {
                if tried_peers.contains(&addr) || visited.probably_visited(addr) {
                    continue;
                }
                tried_peers.insert(addr);
                visited.mark_visited(addr);
                *attempts_at_hop += 1;
                tracing::debug!(
                    %instance_id,
                    target = %addr,
                    attempts_at_hop = *attempts_at_hop,
                    "subscribe: breadth retry with next alternative"
                );
                return Some((candidate, addr));
            }
        }
    }

    // 2. Fresh k_closest round.
    if *retries < MAX_RETRIES {
        *retries += 1;
        *attempts_at_hop = 1;
        let mut fresh = fresh_candidates(instance_id, visited);
        while !fresh.is_empty() {
            let candidate = fresh.remove(0);
            if let Some(addr) = candidate.socket_addr() {
                if tried_peers.contains(&addr) || visited.probably_visited(addr) {
                    continue;
                }
                tried_peers.insert(addr);
                visited.mark_visited(addr);
                // Rest of `fresh` becomes the new alternatives pool for
                // subsequent breadth retries at this new hop.
                *alternatives = fresh;
                tracing::debug!(
                    %instance_id,
                    target = %addr,
                    retries = *retries,
                    "subscribe: fresh k_closest round found new target"
                );
                return Some((candidate, addr));
            }
            // Skip candidate without socket addr, try next from fresh.
        }
        tracing::debug!(
            %instance_id,
            retries = *retries,
            "subscribe: fresh k_closest round returned no usable candidates"
        );
    }

    // 3. Exhausted.
    None
}

/// Compute the jittered inter-attempt delay for one subscribe retry.
///
/// Applies ±20% random jitter to [`RETRY_BASE_DELAY`] using [`GlobalRng`]
/// (deterministic under the simulation harness) so synchronised retry
/// waves across many clients are spread out. Pulled out as a pure
/// function so the jitter bounds are unit-testable without a runtime.
fn jittered_retry_delay() -> Duration {
    // Sample in [0.8, 1.2): at least the ±20% the retry/backoff rule
    // mandates. `random_range` over an f64 range is half-open, so the
    // realised multiplier never reaches 1.2 exactly — still well within
    // the "at least ±20%" requirement and below the 1s plain-sleep cap.
    let factor: f64 = GlobalRng::random_range(0.8..1.2);
    RETRY_BASE_DELAY.mul_f64(factor)
}

/// Sleep the jittered inter-attempt delay before the next subscribe
/// attempt.
///
/// `tokio::time::sleep` is auto-paused in test builds with
/// `start_paused(true)` (the same primitive this file already uses for
/// `request_sent_at`), so simulation tests advance virtual time rather
/// than blocking on a wall clock. The delay is bounded well under 1s
/// ([`RETRY_BASE_DELAY`] = 50ms × <1.2), which the retry/backoff rule
/// explicitly exempts from the interruptible-sleep requirement: there is
/// no per-attempt cancellation signal in this task-per-tx driver, and a
/// sub-100ms sleep cannot meaningfully delay task teardown.
async fn sleep_before_retry() {
    tokio::time::sleep(jittered_retry_delay()).await;
}

/// Publish the driver's outcome to the client, routing on the explicit
/// [`DriverOutcome`] variant rather than inferring delivery state from
/// [`OpManager::is_completed`].
///
/// - [`DriverOutcome::Publish`] routes the contained `HostResult` through
///   [`OpManager::send_client_result`], which both pushes to
///   `result_router_tx` and emits `NodeEvent::TransactionCompleted(client_tx)`
///   so `p2p_protoc`'s `tx_to_client` table is reclaimed.
/// - [`DriverOutcome::SkipAlreadyDelivered`] is a deliberate no-op:
///   `complete_local_subscription` has already delivered the result via
///   `NodeEvent::LocalSubscribeComplete`, and a second send would
///   publish a duplicate `HostResponse::ContractResponse(SubscribeResponse)`
///   to the client.
/// - [`DriverOutcome::InfrastructureError`] is converted into a
///   synthesized client-facing `HostResult::Err` and then published via
///   `send_client_result`. This path is for errors that do not fit the
///   user-visible error shape (e.g., `OpError::NotificationError` from
///   `send_and_await`) — everything else the driver builds an explicit
///   `DriverOutcome::Publish(Err(...))` for.
///
/// Dashboard op_stats success classifier (issue #4010). Extracted so the
/// success/failure mapping per `DriverOutcome` variant has direct unit
/// coverage; `deliver_outcome` itself takes an `OpManager` which is
/// expensive to construct in tests.
fn classify_subscribe_outcome_for_op_stats(outcome: &DriverOutcome) -> bool {
    matches!(
        outcome,
        DriverOutcome::Publish(Ok(_)) | DriverOutcome::SkipAlreadyDelivered
    )
}

fn deliver_outcome(
    op_manager: &OpManager,
    client_tx: Transaction,
    instance_id: ContractInstanceId,
    outcome: DriverOutcome,
) {
    // Dashboard op_stats SUBSCRIBE counter (issue #4010). The legacy
    // `report_result` recording path is bypassed for driver
    // terminal replies (see `node::record_op_result` rustdoc), so
    // every terminal outcome must be recorded here.
    //
    // Sub-operation gate: `run_client_subscribe` is also the entry point
    // for sub-op SUBSCRIBE spawned by PUT/GET (`maybe_subscribe_child`)
    // and `start_subscription_request`, both of which create a child tx
    // via `Transaction::new_child_of`. Those are background plumbing,
    // not user-initiated SUBSCRIBE attempts; counting them would inflate
    // the user-facing dashboard counter every time a PUT/GET completes.
    if !client_tx.is_sub_operation() {
        let success = classify_subscribe_outcome_for_op_stats(&outcome);
        crate::node::network_status::record_op_result(
            crate::node::network_status::OpType::Subscribe,
            success,
        );
    }

    match outcome {
        DriverOutcome::Publish(result) => {
            op_manager.send_client_result(client_tx, result);
        }
        DriverOutcome::SkipAlreadyDelivered => {
            tracing::debug!(
                tx = %client_tx,
                "subscribe: local completion already published; \
                 skipping result_router_tx"
            );
        }
        DriverOutcome::InfrastructureError(err) => {
            tracing::warn!(
                tx = %client_tx,
                contract = %instance_id,
                error = %err,
                "subscribe: infrastructure error; \
                 publishing synthesized client error"
            );
            let synthesized: HostResult = Err(ErrorKind::OperationError {
                cause: format!("subscribe failed: {err}").into(),
            }
            .into());
            op_manager.send_client_result(client_tx, synthesized);
        }
    }
}

// ── Relay SUBSCRIBE driver ───────────────────────────────────────────────────

/// Counter: relay SUBSCRIBE drivers currently in flight. Decremented in
/// the RAII guard on driver exit. Diagnostic for `FREENET_MEMORY_STATS`.
pub static RELAY_SUBSCRIBE_INFLIGHT: std::sync::atomic::AtomicUsize =
    std::sync::atomic::AtomicUsize::new(0);

/// Counter: total relay SUBSCRIBE drivers ever spawned on this node.
pub static RELAY_SUBSCRIBE_SPAWNED_TOTAL: std::sync::atomic::AtomicUsize =
    std::sync::atomic::AtomicUsize::new(0);

/// Counter: total relay SUBSCRIBE drivers that exited (any path).
pub static RELAY_SUBSCRIBE_COMPLETED_TOTAL: std::sync::atomic::AtomicUsize =
    std::sync::atomic::AtomicUsize::new(0);

/// Counter: duplicate inbound `SubscribeMsg::Request` rejected by the
/// per-node dedup gate (`active_relay_subscribe_txs`).
pub static RELAY_SUBSCRIBE_DEDUP_REJECTS: std::sync::atomic::AtomicUsize =
    std::sync::atomic::AtomicUsize::new(0);

/// Counter: number of times `start_relay_subscribe` was invoked. Used by
/// structural pin tests to prove the dispatch gate routes fresh inbound
/// `SubscribeMsg::Request` through the driver rather than
/// legacy `handle_op_request`.
#[cfg(any(test, feature = "testing"))]
pub static RELAY_SUBSCRIBE_DRIVER_CALL_COUNT: std::sync::atomic::AtomicUsize =
    std::sync::atomic::AtomicUsize::new(0);

/// Spawn a relay driver for a fresh inbound
/// `SubscribeMsg::Request`.
///
/// The driver owns local-hit response + single downstream forward +
/// upstream bubble-up in its task locals.
///
/// Notable omissions:
///
/// - `ForwardingAck` is NOT emitted — the ack shares `incoming_tx`
///   with the reply and would satisfy upstream's capacity-1
///   `pending_op_results` waiter before the real `Response`.
/// - Cross-peer retries are NOT attempted at the relay — the relay
///   breadth/retry loop was the memory-explosion amplifier (see
///   `project_1454_phase5_memory.md`). Originator owns retry.
pub(crate) async fn start_relay_subscribe(
    op_manager: Arc<OpManager>,
    incoming_tx: Transaction,
    instance_id: ContractInstanceId,
    htl: usize,
    visited: VisitedPeers,
    is_renewal: bool,
    upstream_addr: std::net::SocketAddr,
) -> Result<(), OpError> {
    #[cfg(any(test, feature = "testing"))]
    RELAY_SUBSCRIBE_DRIVER_CALL_COUNT.fetch_add(1, std::sync::atomic::Ordering::SeqCst);

    if !op_manager.active_relay_subscribe_txs.insert(incoming_tx) {
        RELAY_SUBSCRIBE_DEDUP_REJECTS.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        tracing::debug!(
            tx = %incoming_tx,
            %instance_id,
            %upstream_addr,
            phase = "relay_subscribe_dedup_reject",
            "SUBSCRIBE relay: duplicate Request for in-flight tx, replying NotFound"
        );
        // Emit an immediate `NotFound` back to the rejected upstream so
        // its `send_to_and_await` returns fast instead of stalling the
        // full `OPERATION_TTL` (60 s). Silently dropping the duplicate
        // loses the upstream's retry budget and produces a spurious
        // "this contract is unreachable" verdict even when the winning
        // driver fulfills the subscription. The reply uses a fresh
        // `OpCtx` scoped to `incoming_tx` — the fire-and-forget send
        // does not touch the winning driver's `pending_op_results`
        // slot (that slot is keyed on the attempt_tx the WINNER
        // forwarded downstream, not on `incoming_tx` at this node).
        // Forward depth at this dedup-rejecting node = max_htl - htl.
        // The winning in-flight driver will report its own (possibly
        // deeper) hop_count to upstream — but for this rejected duplicate
        // path the upstream sees this node's depth, which is correct from
        // the duplicate's perspective.
        let hop_count = op_manager.ring.max_hops_to_live.saturating_sub(htl);
        let response = NetMessage::from(SubscribeMsg::Response {
            id: incoming_tx,
            instance_id,
            result: SubscribeMsgResult::NotFound,
            hop_count,
        });
        let mut ctx = op_manager.op_ctx(incoming_tx);
        if let Err(err) = ctx.send_fire_and_forget(upstream_addr, response).await {
            tracing::debug!(
                tx = %incoming_tx,
                %upstream_addr,
                error = %err,
                "SUBSCRIBE relay: dedup-reject NotFound send failed"
            );
        }
        return Ok(());
    }

    // Construct the guard IMMEDIATELY after `insert` so a panic in any
    // intervening work (fmt-allocating logs, future OOM) cannot leak
    // the dedup-set entry. The guard's Drop clears the entry; without
    // the guard, there is no TTL and no recovery path. (Review M3,
    // skeptical #3932.)
    RELAY_SUBSCRIBE_INFLIGHT.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    RELAY_SUBSCRIBE_SPAWNED_TOTAL.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    let guard = RelaySubscribeInflightGuard {
        op_manager: op_manager.clone(),
        incoming_tx,
    };

    tracing::debug!(
        tx = %incoming_tx,
        %instance_id,
        htl,
        %upstream_addr,
        is_renewal,
        phase = "relay_subscribe_start",
        "SUBSCRIBE relay: spawning driver"
    );

    GlobalExecutor::spawn(run_relay_subscribe(
        guard,
        op_manager,
        incoming_tx,
        instance_id,
        htl,
        visited,
        is_renewal,
        upstream_addr,
    ));
    Ok(())
}

/// RAII guard that decrements `RELAY_SUBSCRIBE_INFLIGHT`, bumps
/// `RELAY_SUBSCRIBE_COMPLETED_TOTAL`, and removes the driver's
/// `incoming_tx` from `active_relay_subscribe_txs` on drop.
struct RelaySubscribeInflightGuard {
    op_manager: Arc<OpManager>,
    incoming_tx: Transaction,
}

impl Drop for RelaySubscribeInflightGuard {
    fn drop(&mut self) {
        self.op_manager
            .active_relay_subscribe_txs
            .remove(&self.incoming_tx);
        RELAY_SUBSCRIBE_INFLIGHT.fetch_sub(1, std::sync::atomic::Ordering::Relaxed);
        RELAY_SUBSCRIBE_COMPLETED_TOTAL.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    }
}

#[allow(clippy::too_many_arguments)]
async fn run_relay_subscribe(
    guard: RelaySubscribeInflightGuard,
    op_manager: Arc<OpManager>,
    incoming_tx: Transaction,
    instance_id: ContractInstanceId,
    htl: usize,
    visited: VisitedPeers,
    is_renewal: bool,
    upstream_addr: std::net::SocketAddr,
) {
    let _guard = guard;

    if let Err(err) = drive_relay_subscribe(
        &op_manager,
        incoming_tx,
        instance_id,
        htl,
        visited,
        is_renewal,
        upstream_addr,
    )
    .await
    {
        tracing::warn!(
            tx = %incoming_tx,
            %instance_id,
            error = %err,
            phase = "relay_subscribe_error",
            "SUBSCRIBE relay: driver returned error"
        );
    }

    // Release the pending_op_results slot at driver exit. Same reasoning
    // as GET/PUT relay — `send_to_and_await` leaves a closed sender in
    // the slot that only the 60s sweep would reclaim without this
    // explicit release.
    tokio::task::yield_now().await;
    op_manager.release_pending_op_slot(incoming_tx).await;
}

/// Drive a fresh inbound relay `SubscribeMsg::Request`: fast-path on
/// local contract hit, otherwise forward to the closest potentially
/// hosting peer and bubble the Response upstream.
///
/// # Intentional signal drop: `PeerHealthTracker` on downstream timeout
///
/// The legacy relay Request arm attached a `SubscribeStats {
/// target_peer, contract_location, request_sent_at }` to the
/// `AwaitingResponse` state so that downstream timeouts (reported via
/// `handle_abort`) fed `PeerHealthTracker` and the failure estimator.
/// The driver DROPS this signal on the relay node: on
/// `send_to_and_await` timeout we bubble `NotFound` upstream and exit
/// without touching `PeerHealthTracker`.
///
/// This is deliberate for slice A. Cross-peer retry at the relay was
/// the phase-5 memory-explosion amplifier (see `project_1454_phase5_memory.md`
/// and PR #3896's fix stack) — the driver design moves ALL
/// cross-peer retry to the originator's client-init driver. The
/// originator's `send_to_and_await` timeout path there is the correct
/// site for peer-health updates; reporting them at a relay that does
/// not retry adds no actionable signal and makes the symmetry with
/// GET/PUT/UPDATE slice A harder to reason about.
///
/// Follow-up: if peer-health reporting at the relay becomes necessary
/// for other reasons (e.g. targeting a specific slow peer cluster
/// without originating traffic), extend `OpCtx::send_to_and_await`
/// to emit a stat update on `Err(_elapsed)` via a hook rather than
/// reintroducing the signal on each relay driver.
async fn drive_relay_subscribe(
    op_manager: &Arc<OpManager>,
    incoming_tx: Transaction,
    instance_id: ContractInstanceId,
    htl: usize,
    visited: VisitedPeers,
    is_renewal: bool,
    upstream_addr: std::net::SocketAddr,
) -> Result<(), OpError> {
    tracing::info!(
        tx = %incoming_tx,
        %instance_id,
        htl,
        %upstream_addr,
        is_renewal,
        phase = "relay_subscribe_request",
        "SUBSCRIBE relay: processing Request"
    );

    // ── Step 1: Local-hit fast path (with brief wait-for-in-flight-PUT) ──
    if let Some(key) = super::wait_for_contract_with_timeout(
        op_manager,
        instance_id,
        super::CONTRACT_WAIT_TIMEOUT_MS,
    )
    .await?
    {
        // Register the subscribing peer as a downstream subscriber.
        // The relay driver path does not know the requester's
        // `TransportPublicKey` (the legacy path had it because the op
        // state carried `requester_pub_key`); `register_downstream_subscriber`
        // falls back to addr-based lookup in that case, which is adequate
        // for relay hops where the upstream is a direct connection.
        register_downstream_subscriber(
            op_manager,
            &key,
            upstream_addr,
            None,
            Some(upstream_addr),
            &incoming_tx,
            " (driver relay local hit)",
        )
        .await;

        tracing::info!(
            tx = %incoming_tx,
            contract = %key,
            is_renewal,
            phase = "relay_subscribe_local_hit",
            "SUBSCRIBE relay: fulfilled locally, sending Response"
        );
        // Local-hit storer arm: this node hosts the contract.
        // hop_count = max_htl - htl_we_received.
        let hop_count = op_manager.ring.max_hops_to_live.saturating_sub(htl);
        return relay_subscribe_send_response(
            op_manager,
            incoming_tx,
            instance_id,
            SubscribeMsgResult::Subscribed { key },
            upstream_addr,
            hop_count,
        )
        .await;
    }

    // ── Step 2: HTL / candidate selection ────────────────────────────────
    if htl == 0 {
        tracing::warn!(
            tx = %incoming_tx,
            contract = %instance_id,
            htl = 0,
            phase = "relay_subscribe_not_found",
            "SUBSCRIBE relay: HTL exhausted"
        );
        // HTL=0 path: the request traversed the full max_htl forward hops
        // to reach us. hop_count = max_htl - 0 = max_htl.
        let hop_count = op_manager.ring.max_hops_to_live;
        if let Some(event) = crate::tracing::NetEventLog::subscribe_not_found(
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
        return relay_subscribe_send_response(
            op_manager,
            incoming_tx,
            instance_id,
            SubscribeMsgResult::NotFound,
            upstream_addr,
            hop_count,
        )
        .await;
    }

    // Restore hash keys after deserialization + mark ourselves + upstream
    // as visited so we never loop back.
    let own_addr = op_manager.ring.connection_manager.peer_addr()?;
    let mut new_visited = visited.with_transaction(&incoming_tx);
    new_visited.mark_visited(own_addr);
    new_visited.mark_visited(upstream_addr);

    let mut candidates =
        op_manager
            .ring
            .k_closest_potentially_hosting(&instance_id, &new_visited, MAX_BREADTH);

    if candidates.is_empty() {
        tracing::warn!(
            tx = %incoming_tx,
            contract = %instance_id,
            phase = "relay_subscribe_not_found",
            "SUBSCRIBE relay: no closer peers to forward"
        );
        // No-candidates exhaustion: this relay is reporting its own
        // exhaustion of downstream candidates. hop_count = max_htl - htl
        // (the forward depth of THIS relay).
        let hop_count = op_manager.ring.max_hops_to_live.saturating_sub(htl);
        if let Some(event) = crate::tracing::NetEventLog::subscribe_not_found(
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
        return relay_subscribe_send_response(
            op_manager,
            incoming_tx,
            instance_id,
            SubscribeMsgResult::NotFound,
            upstream_addr,
            hop_count,
        )
        .await;
    }

    let next_hop = candidates.remove(0);
    let next_addr = match next_hop.socket_addr() {
        Some(addr) => addr,
        None => {
            tracing::error!(
                tx = %incoming_tx,
                %instance_id,
                target_pub_key = %next_hop.pub_key(),
                "SUBSCRIBE relay: next hop has no socket address"
            );
            // Forward-failure at THIS relay; same depth as no-candidates.
            let hop_count = op_manager.ring.max_hops_to_live.saturating_sub(htl);
            return relay_subscribe_send_response(
                op_manager,
                incoming_tx,
                instance_id,
                SubscribeMsgResult::NotFound,
                upstream_addr,
                hop_count,
            )
            .await;
        }
    };
    new_visited.mark_visited(next_addr);

    // ── Step 3: Forward downstream, await Response ───────────────────────
    let new_htl = htl.saturating_sub(1);

    if let Some(event) = crate::tracing::NetEventLog::subscribe_request(
        &incoming_tx,
        &op_manager.ring,
        instance_id,
        next_hop.clone(),
        new_htl,
    ) {
        op_manager
            .ring
            .register_events(either::Either::Left(event))
            .await;
    }

    tracing::debug!(
        tx = %incoming_tx,
        %instance_id,
        peer_addr = %next_addr,
        htl = new_htl,
        phase = "relay_subscribe_forward",
        "SUBSCRIBE relay: forwarding to next hop"
    );

    let forward = NetMessage::from(SubscribeMsg::Request {
        id: incoming_tx,
        instance_id,
        htl: new_htl,
        visited: new_visited,
        is_renewal,
    });

    let mut ctx = op_manager.op_ctx(incoming_tx);
    let round_trip =
        tokio::time::timeout(OPERATION_TTL, ctx.send_to_and_await(next_addr, forward)).await;

    // Release the pending_op_results slot installed by send_to_and_await.
    // Mirrors PUT/GET relay — the upstream fire-and-forget reply below
    // would otherwise leak a slot entry per driver run.
    op_manager.release_pending_op_slot(incoming_tx).await;

    let reply = match round_trip {
        Ok(Ok(reply)) => reply,
        Ok(Err(err)) => {
            tracing::warn!(
                tx = %incoming_tx,
                %instance_id,
                target = %next_addr,
                error = %err,
                "SUBSCRIBE relay: send_to_and_await failed"
            );
            crate::operations::record_relay_route_event(
                op_manager,
                next_hop.clone(),
                crate::ring::Location::from(&instance_id),
                crate::router::RouteOutcome::Failure,
                crate::node::network_status::OpType::Subscribe,
            );
            // Forward failure at THIS relay: report our own depth.
            let hop_count = op_manager.ring.max_hops_to_live.saturating_sub(htl);
            return relay_subscribe_send_response(
                op_manager,
                incoming_tx,
                instance_id,
                SubscribeMsgResult::NotFound,
                upstream_addr,
                hop_count,
            )
            .await;
        }
        Err(_elapsed) => {
            tracing::warn!(
                tx = %incoming_tx,
                %instance_id,
                target = %next_addr,
                timeout_secs = OPERATION_TTL.as_secs(),
                "SUBSCRIBE relay: downstream timed out"
            );
            crate::operations::record_relay_route_event(
                op_manager,
                next_hop.clone(),
                crate::ring::Location::from(&instance_id),
                crate::router::RouteOutcome::Failure,
                crate::node::network_status::OpType::Subscribe,
            );
            // Same as send-failure arm.
            let hop_count = op_manager.ring.max_hops_to_live.saturating_sub(htl);
            return relay_subscribe_send_response(
                op_manager,
                incoming_tx,
                instance_id,
                SubscribeMsgResult::NotFound,
                upstream_addr,
                hop_count,
            )
            .await;
        }
    };

    // ── Step 4: Classify reply, register requester if Subscribed, bubble up ──
    //
    // Feed the relay's downstream-peer choice into the local Router so
    // future routing decisions are informed by relay-observed outcomes
    // (see operations.rs::record_relay_route_event).
    //
    // `bubble_hop_count` is the wire `hop_count` to forward upstream:
    // - downstream Subscribed/NotFound: preserve the downstream's value
    //   (relays do NOT increment on the return path);
    // - unexpected variant: fall back to this relay's own depth
    //   (`max_htl - htl`) — we know nothing about what produced `other`.
    let (result, bubble_hop_count) = match reply {
        NetMessage::V1(NetMessageV1::Subscribe(SubscribeMsg::Response {
            result: SubscribeMsgResult::Subscribed { key },
            hop_count: downstream_hop_count,
            ..
        })) => {
            // Relay-side Subscribed registration — mirror legacy
            // subscribe.rs:1690 arm. DO NOT call ring.subscribe /
            // announce_contract_hosted here; a relay is not itself a
            // subscriber. See subscribe.rs:1655–1688 for the full
            // reasoning (prevents the #3763 subscription storm feedback
            // loop).
            register_downstream_subscriber(
                op_manager,
                &key,
                upstream_addr,
                None,
                Some(upstream_addr),
                &incoming_tx,
                " (driver relay registration on Response)",
            )
            .await;

            tracing::info!(
                tx = %incoming_tx,
                contract = %key,
                phase = "relay_subscribe_bubble",
                "SUBSCRIBE relay: downstream Subscribed; bubbling upstream"
            );
            crate::operations::record_relay_route_event(
                op_manager,
                next_hop.clone(),
                crate::ring::Location::from(&key),
                crate::router::RouteOutcome::SuccessUntimed,
                crate::node::network_status::OpType::Subscribe,
            );
            (SubscribeMsgResult::Subscribed { key }, downstream_hop_count)
        }
        NetMessage::V1(NetMessageV1::Subscribe(SubscribeMsg::Response {
            result: SubscribeMsgResult::NotFound,
            hop_count: downstream_hop_count,
            ..
        })) => {
            // Downstream peer correctly answered NotFound. The peer
            // behaved well at the protocol level — it just doesn't host
            // this contract. Record SuccessUntimed so the model treats
            // this peer as healthy. See `record_relay_route_event` rustdoc.
            tracing::debug!(
                tx = %incoming_tx,
                %instance_id,
                phase = "relay_subscribe_bubble_not_found",
                "SUBSCRIBE relay: downstream NotFound; bubbling upstream"
            );
            crate::operations::record_relay_route_event(
                op_manager,
                next_hop.clone(),
                crate::ring::Location::from(&instance_id),
                crate::router::RouteOutcome::SuccessUntimed,
                crate::node::network_status::OpType::Subscribe,
            );
            (SubscribeMsgResult::NotFound, downstream_hop_count)
        }
        other => {
            // Unexpected reply variant: unclear whether it's a local
            // bug or peer misbehaviour. Do NOT record a route event;
            // the helper invariant is one event per unambiguous attribution.
            tracing::warn!(
                tx = %incoming_tx,
                %instance_id,
                reply_variant = ?std::mem::discriminant(&other),
                "SUBSCRIBE relay: unexpected reply variant; treating as NotFound"
            );
            let hop_count = op_manager.ring.max_hops_to_live.saturating_sub(htl);
            (SubscribeMsgResult::NotFound, hop_count)
        }
    };

    relay_subscribe_send_response(
        op_manager,
        incoming_tx,
        instance_id,
        result,
        upstream_addr,
        bubble_hop_count,
    )
    .await
}

/// Send `SubscribeMsg::Response` upstream (fire-and-forget: upstream
/// relay awaits via its own `send_to_and_await`, no reply expected).
///
/// `hop_count` is the forward-path depth to embed in the Response. For a
/// relay that fulfilled locally (this node hosts the contract), pass
/// `max_htl - incoming_htl`. For an HTL-exhaustion / no-candidates /
/// forward-failure relay, pass that same `max_htl - incoming_htl` (it's
/// the depth at which this relay reported the failure). For a relay
/// bubbling a downstream Response upstream, pass the downstream's
/// `hop_count` verbatim — relays do NOT increment on the return path.
async fn relay_subscribe_send_response(
    op_manager: &OpManager,
    incoming_tx: Transaction,
    instance_id: ContractInstanceId,
    result: SubscribeMsgResult,
    upstream_addr: std::net::SocketAddr,
    hop_count: usize,
) -> Result<(), OpError> {
    let response = NetMessage::from(SubscribeMsg::Response {
        id: incoming_tx,
        instance_id,
        result,
        hop_count,
    });
    let mut ctx = op_manager.op_ctx(incoming_tx);
    ctx.send_fire_and_forget(upstream_addr, response).await
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::message::NetMessageV1;
    use crate::operations::connect::ConnectMsg;

    fn fresh_tx() -> Transaction {
        Transaction::new::<SubscribeMsg>()
    }

    /// `start_client_subscribe` must acquire a `ClientOpGuard` before
    /// the `GlobalExecutor::spawn` and move it into the spawned
    /// future. Note: `run_client_subscribe` itself does NOT install
    /// the guard, because PUT's blocking-subscribe path calls it
    /// inline (already counted under PUT's guard). The guard
    /// therefore lives at the spawn site, not inside the function.
    /// See sibling pin in `put/op_ctx_task.rs` for the shutdown-drain
    /// rationale.
    #[test]
    fn start_client_subscribe_acquires_inflight_guard_before_spawn() {
        let src = include_str!("op_ctx_task.rs");
        let entry = src
            .find("pub(crate) async fn start_client_subscribe(")
            .expect("start_client_subscribe must exist");
        let after_spawn = src[entry..]
            .find("GlobalExecutor::spawn(")
            .expect("start_client_subscribe must spawn a driver task");
        let before_spawn = &src[entry..entry + after_spawn];
        assert!(
            before_spawn.contains("op_manager.admit_client_op()"),
            "start_client_subscribe must call op_manager.admit_client_op() \
             before GlobalExecutor::spawn (atomic admission gate + \
             counter bump; closes the Codex r2 TOCTOU)."
        );
        assert!(
            before_spawn.contains("OpError::NodeShuttingDown"),
            "start_client_subscribe must early-return OpError::NodeShuttingDown \
             on a refused admission."
        );
        let spawned = &src[entry + after_spawn..];
        let block_end = spawned
            .find("\n    Ok(client_tx)")
            .expect("start_client_subscribe must return Ok(client_tx)");
        let spawn_block = &spawned[..block_end];
        assert!(
            spawn_block.contains("let _inflight_guard = inflight_guard;"),
            "the ClientOpGuard must be moved into the spawned future."
        );
    }

    /// Phase 7 egress self-block pin (#4300). `start_client_subscribe`
    /// MUST reject a banned contract BEFORE spawning the driver, so we
    /// don't register interest in a contract we have decided to reject.
    /// Mirrors the receive-side `subscribe_dispatch_gates_banned_contracts`
    /// pin in `ring/contract_ban_list.rs`. See the sibling pin in
    /// `put/op_ctx_task.rs` for the full rationale.
    #[test]
    fn start_client_subscribe_gates_banned_contracts_before_spawn() {
        let src = include_str!("op_ctx_task.rs");
        let entry = src
            .find("pub(crate) async fn start_client_subscribe(")
            .expect("start_client_subscribe must exist");
        let after_spawn = src[entry..]
            .find("GlobalExecutor::spawn(")
            .expect("start_client_subscribe must spawn a driver task");
        let before_spawn = &src[entry..entry + after_spawn];
        assert!(
            before_spawn.contains("reject_if_contract_banned"),
            "start_client_subscribe must call reject_if_contract_banned() \
             before GlobalExecutor::spawn so a banned contract's SUBSCRIBE \
             is rejected with a typed error instead of registering interest \
             (#4300 egress self-block)."
        );
    }

    /// `start_directed_subscribe` (the SubscribeHint receive path) MUST keep
    /// the same ban + admission guards as `start_client_subscribe` before it
    /// spawns a driver — a network-triggered directed subscribe must not bypass
    /// the egress ban check (#4300) or the inflight admission gate. And it must
    /// NOT route its outcome through `deliver_outcome` (the client result path):
    /// there is no client waiter, so doing so would skew the user-facing
    /// SUBSCRIBE op-stats and push an unconsumed result into result_router_tx
    /// during a migration cascade (Codex r2).
    #[test]
    fn start_directed_subscribe_guards_and_completes_internally() {
        let src = include_str!("op_ctx_task.rs");
        let entry = src
            .find("pub(crate) fn start_directed_subscribe(")
            .expect("start_directed_subscribe must exist");
        let after_spawn_off = src[entry..]
            .find("GlobalExecutor::spawn(")
            .expect("start_directed_subscribe must spawn a driver task");
        let before_spawn = &src[entry..entry + after_spawn_off];
        assert!(
            before_spawn.contains("reject_if_contract_banned"),
            "start_directed_subscribe must call reject_if_contract_banned() before spawn"
        );
        assert!(
            before_spawn.contains("op_manager.admit_client_op()"),
            "start_directed_subscribe must acquire the inflight admission guard before spawn"
        );
        // Bound the spawned-block scan to this function (up to the next fn).
        let rest = &src[entry + after_spawn_off..];
        let block_end = rest.find("\npub(crate) async fn ").unwrap_or(rest.len());
        let spawn_block = &rest[..block_end];
        assert!(
            spawn_block.contains("let _inflight_guard = inflight_guard;"),
            "the inflight guard must be moved into the spawned directed-subscribe future"
        );
        // Check for the CALL form `deliver_outcome(` — the explanatory comment in
        // the function mentions the name without a paren, so this won't false-trip.
        assert!(
            !spawn_block.contains("deliver_outcome("),
            "start_directed_subscribe must complete internally (log only), NOT via \
             deliver_outcome() — there is no client waiter for a migration subscribe"
        );
    }

    #[test]
    fn classify_reply_subscribed() {
        use freenet_stdlib::prelude::{CodeHash, ContractInstanceId, ContractKey};
        let key = ContractKey::from_id_and_code(
            ContractInstanceId::new([3u8; 32]),
            CodeHash::new([4u8; 32]),
        );
        let tx = fresh_tx();
        let msg = NetMessage::V1(NetMessageV1::Subscribe(SubscribeMsg::Response {
            id: tx,
            instance_id: *key.id(),
            result: SubscribeMsgResult::Subscribed { key },
            hop_count: 0,
        }));
        match classify_reply(&msg) {
            ReplyClass::Subscribed { key: got } => assert_eq!(got, key),
            other @ (ReplyClass::NotFound | ReplyClass::Unexpected) => {
                panic!("expected Subscribed, got {other:?}")
            }
        }
    }

    #[test]
    fn classify_reply_not_found() {
        let instance_id = freenet_stdlib::prelude::ContractInstanceId::new([4u8; 32]);
        let tx = fresh_tx();
        let msg = NetMessage::V1(NetMessageV1::Subscribe(SubscribeMsg::Response {
            id: tx,
            instance_id,
            result: SubscribeMsgResult::NotFound,
            hop_count: 0,
        }));
        assert!(matches!(classify_reply(&msg), ReplyClass::NotFound));
    }

    /// Regression pin: the SUBSCRIBE bubble-up path MUST preserve the
    /// wire-carried `hop_count` from the downstream Response unchanged.
    /// A regression that synthesised 0 here (or dropped the field) would
    /// silently collapse the storer's forward-path depth at every relay,
    /// defeating the whole PR.  The bincode-roundtrip test in
    /// `subscribe/tests.rs` catches the wire format only; this pin tests
    /// that destructuring the message keeps the field intact and that the
    /// classifier doesn't conditionally reject based on `hop_count`.
    #[test]
    fn classify_reply_preserves_hop_count_subscribed() {
        use freenet_stdlib::prelude::{CodeHash, ContractInstanceId, ContractKey};
        let key = ContractKey::from_id_and_code(
            ContractInstanceId::new([3u8; 32]),
            CodeHash::new([4u8; 32]),
        );
        for hc in [0_usize, 1, 4, 10, 64] {
            let tx = fresh_tx();
            let msg = NetMessage::V1(NetMessageV1::Subscribe(SubscribeMsg::Response {
                id: tx,
                instance_id: *key.id(),
                result: SubscribeMsgResult::Subscribed { key },
                hop_count: hc,
            }));
            assert!(
                matches!(classify_reply(&msg), ReplyClass::Subscribed { .. }),
                "Subscribed with hop_count={hc} must still classify"
            );
            // Destructure the message to assert the field was preserved.
            match msg {
                NetMessage::V1(NetMessageV1::Subscribe(SubscribeMsg::Response {
                    hop_count: got,
                    ..
                })) => assert_eq!(got, hc, "Subscribed hop_count preserved ({hc})"),
                _ => unreachable!(),
            }
        }
    }

    #[test]
    fn classify_reply_preserves_hop_count_not_found() {
        let instance_id = freenet_stdlib::prelude::ContractInstanceId::new([4u8; 32]);
        for hc in [0_usize, 1, 4, 10, 64] {
            let tx = fresh_tx();
            let msg = NetMessage::V1(NetMessageV1::Subscribe(SubscribeMsg::Response {
                id: tx,
                instance_id,
                result: SubscribeMsgResult::NotFound,
                hop_count: hc,
            }));
            assert!(
                matches!(classify_reply(&msg), ReplyClass::NotFound),
                "NotFound with hop_count={hc} must still classify"
            );
            match msg {
                NetMessage::V1(NetMessageV1::Subscribe(SubscribeMsg::Response {
                    hop_count: got,
                    ..
                })) => assert_eq!(got, hc, "NotFound hop_count preserved ({hc})"),
                _ => unreachable!(),
            }
        }
    }

    #[test]
    fn classify_reply_unexpected_for_request() {
        // A `Request` arriving as a "reply" is structurally wrong — it's
        // what the ORIGINATOR sends, not receives. The classifier must
        // flag it so the caller can surface an error rather than silently
        // retry.
        let instance_id = freenet_stdlib::prelude::ContractInstanceId::new([5u8; 32]);
        let tx = fresh_tx();
        let msg = NetMessage::V1(NetMessageV1::Subscribe(SubscribeMsg::Request {
            id: tx,
            instance_id,
            htl: 5,
            visited: super::VisitedPeers::new(&tx),
            is_renewal: false,
        }));
        assert!(matches!(classify_reply(&msg), ReplyClass::Unexpected));
    }

    #[test]
    fn classify_reply_unexpected_for_non_subscribe_variant() {
        // An arbitrary non-subscribe message in the reply slot indicates
        // a routing bug upstream; classifier must surface it.
        let tx = Transaction::new::<ConnectMsg>();
        let msg = NetMessage::V1(NetMessageV1::Aborted(tx));
        assert!(matches!(classify_reply(&msg), ReplyClass::Unexpected));
    }

    // ──────────────────────────────────────────────────────────────
    // Retry-logic coverage for `advance_to_next_peer_impl` (review
    // finding Testing #2). The impl is parameterized on a
    // `fresh_candidates` closure so these tests can drive it without
    // building a full `OpManager` + `Ring`. Each test pins one
    // distinct transition in the retry decision tree.
    // ──────────────────────────────────────────────────────────────

    /// Helper: construct a synthetic `PeerKeyLocation` with a
    /// predictable socket address. Uses `PeerKeyLocation::random()` for
    /// the `pub_key` (cached per-thread so it's cheap) and then
    /// overrides the address with one we control so test assertions
    /// can match on it. The actual location is derived from the
    /// address by the ring code, which is irrelevant for these tests —
    /// `advance_to_next_peer_impl` only looks at `socket_addr()`.
    fn peer_at(addr: &str) -> PeerKeyLocation {
        let mut p = PeerKeyLocation::random();
        p.set_addr(addr.parse().expect("valid socket addr"));
        p
    }

    fn contract_id() -> ContractInstanceId {
        ContractInstanceId::new([9u8; 32])
    }

    #[test]
    fn advance_breadth_retry_returns_next_alternative_fifo() {
        // Setup: three alternatives, none tried yet, attempts_at_hop
        // below MAX_BREADTH, retries at 0. The helper should pop the
        // FIRST alternative (FIFO — closest-first) and return it.
        let id = contract_id();
        let tx = fresh_tx();
        let mut visited = VisitedPeers::new(&tx);
        let mut tried_peers: HashSet<std::net::SocketAddr> = HashSet::new();
        let a = peer_at("10.0.0.1:1001");
        let b = peer_at("10.0.0.2:1002");
        let c = peer_at("10.0.0.3:1003");
        let a_addr = a.socket_addr().unwrap();
        let mut alternatives = vec![a.clone(), b.clone(), c.clone()];
        let mut retries = 0usize;
        let mut attempts_at_hop = 1usize;

        let result = advance_to_next_peer_impl(
            &id,
            &mut visited,
            &mut tried_peers,
            &mut alternatives,
            &mut retries,
            &mut attempts_at_hop,
            |_, _| panic!("breadth retry path must not call fresh_candidates"),
        );

        let (picked, picked_addr) = result.expect("breadth retry should return an alternative");
        assert_eq!(picked_addr, a_addr, "must pick FIRST alternative (FIFO)");
        assert_eq!(picked.socket_addr(), Some(a_addr));
        assert_eq!(attempts_at_hop, 2, "attempts_at_hop must increment");
        assert_eq!(retries, 0, "retries must not change on breadth retry");
        assert_eq!(alternatives.len(), 2, "one alternative consumed");
        assert!(tried_peers.contains(&a_addr));
        assert!(visited.probably_visited(a_addr));
    }

    #[test]
    fn advance_breadth_retry_skips_already_visited() {
        // Setup: first alternative is already in visited bloom; helper
        // must skip it and take the next one.
        let id = contract_id();
        let tx = fresh_tx();
        let mut visited = VisitedPeers::new(&tx);
        let a = peer_at("10.0.0.1:1001");
        let b = peer_at("10.0.0.2:1002");
        let a_addr = a.socket_addr().unwrap();
        let b_addr = b.socket_addr().unwrap();
        visited.mark_visited(a_addr); // A was already tried earlier
        let mut tried_peers: HashSet<std::net::SocketAddr> = HashSet::new();
        tried_peers.insert(a_addr);
        let mut alternatives = vec![a, b];
        let mut retries = 0usize;
        let mut attempts_at_hop = 1usize;

        let result = advance_to_next_peer_impl(
            &id,
            &mut visited,
            &mut tried_peers,
            &mut alternatives,
            &mut retries,
            &mut attempts_at_hop,
            |_, _| panic!("should find B before falling through"),
        );

        let (_, picked_addr) = result.expect("should pick B after skipping A");
        assert_eq!(picked_addr, b_addr);
        assert!(alternatives.is_empty(), "both A and B consumed");
    }

    #[test]
    fn advance_fresh_round_triggered_when_alternatives_exhausted() {
        // Setup: alternatives empty, attempts_at_hop below MAX_BREADTH.
        // The impl should bypass the breadth branch (nothing to pop)
        // and call `fresh_candidates`, resetting attempts_at_hop to 1
        // and incrementing retries.
        let id = contract_id();
        let tx = fresh_tx();
        let mut visited = VisitedPeers::new(&tx);
        let mut tried_peers: HashSet<std::net::SocketAddr> = HashSet::new();
        let mut alternatives: Vec<PeerKeyLocation> = Vec::new();
        let mut retries = 0usize;
        let mut attempts_at_hop = 1usize;

        let fresh_peer = peer_at("10.0.0.5:1005");
        let fresh_addr = fresh_peer.socket_addr().unwrap();
        let mut fresh_calls = 0;

        let result = advance_to_next_peer_impl(
            &id,
            &mut visited,
            &mut tried_peers,
            &mut alternatives,
            &mut retries,
            &mut attempts_at_hop,
            |got_id, _got_visited| {
                fresh_calls += 1;
                assert_eq!(got_id, &contract_id(), "passes through instance_id");
                vec![fresh_peer.clone()]
            },
        );

        assert_eq!(
            fresh_calls, 1,
            "fresh_candidates must be called exactly once"
        );
        let (_, picked_addr) = result.expect("fresh round should find a peer");
        assert_eq!(picked_addr, fresh_addr);
        assert_eq!(retries, 1, "retries incremented on fresh round");
        assert_eq!(
            attempts_at_hop, 1,
            "attempts_at_hop reset to 1 on fresh round"
        );
    }

    #[test]
    fn advance_fresh_round_after_max_breadth_hit() {
        // Setup: attempts_at_hop at MAX_BREADTH (the breadth guard
        // rejects further breadth retries even with alternatives
        // available). The impl must immediately fall through to the
        // fresh_candidates branch.
        let id = contract_id();
        let tx = fresh_tx();
        let mut visited = VisitedPeers::new(&tx);
        let mut tried_peers: HashSet<std::net::SocketAddr> = HashSet::new();
        // Alternatives are present, but breadth is already exhausted.
        let unused_alt = peer_at("10.0.0.1:1001");
        let mut alternatives = vec![unused_alt.clone()];
        let mut retries = 0usize;
        let mut attempts_at_hop = MAX_BREADTH;

        let fresh_peer = peer_at("10.0.0.5:1005");
        let fresh_addr = fresh_peer.socket_addr().unwrap();

        let result = advance_to_next_peer_impl(
            &id,
            &mut visited,
            &mut tried_peers,
            &mut alternatives,
            &mut retries,
            &mut attempts_at_hop,
            |_, _| vec![fresh_peer.clone()],
        );

        let (_, picked_addr) = result.expect("fresh round should run");
        assert_eq!(picked_addr, fresh_addr);
        // The unused alt must still be in `alternatives` OR have been
        // replaced by the remainder of `fresh` — check that we did NOT
        // consume it via the breadth branch.
        assert_eq!(retries, 1, "went through fresh round, not breadth");
        assert_eq!(attempts_at_hop, 1, "attempts_at_hop reset by fresh round");
    }

    #[test]
    fn advance_exhausted_after_max_retries() {
        // Setup: retries at MAX_RETRIES, no alternatives. Both guards
        // reject; helper must return None.
        let id = contract_id();
        let tx = fresh_tx();
        let mut visited = VisitedPeers::new(&tx);
        let mut tried_peers: HashSet<std::net::SocketAddr> = HashSet::new();
        let mut alternatives: Vec<PeerKeyLocation> = Vec::new();
        let mut retries = MAX_RETRIES;
        let mut attempts_at_hop = 1usize;

        let result = advance_to_next_peer_impl(
            &id,
            &mut visited,
            &mut tried_peers,
            &mut alternatives,
            &mut retries,
            &mut attempts_at_hop,
            |_, _| panic!("fresh_candidates must not be called when retries == MAX"),
        );

        assert!(result.is_none(), "exhausted case returns None");
        assert_eq!(retries, MAX_RETRIES, "retries unchanged when exhausted");
    }

    #[test]
    fn advance_exhausted_when_fresh_round_returns_empty() {
        // Setup: below MAX_RETRIES, alternatives empty. fresh_candidates
        // returns empty Vec (e.g., the ring has no candidates left after
        // accounting for visited filter). Helper must return None AND
        // have incremented retries.
        let id = contract_id();
        let tx = fresh_tx();
        let mut visited = VisitedPeers::new(&tx);
        let mut tried_peers: HashSet<std::net::SocketAddr> = HashSet::new();
        let mut alternatives: Vec<PeerKeyLocation> = Vec::new();
        let mut retries = 0usize;
        let mut attempts_at_hop = 1usize;

        let result = advance_to_next_peer_impl(
            &id,
            &mut visited,
            &mut tried_peers,
            &mut alternatives,
            &mut retries,
            &mut attempts_at_hop,
            |_, _| Vec::new(),
        );

        assert!(result.is_none());
        assert_eq!(
            retries, 1,
            "retries incremented even though fresh round was empty \
             — the round was 'attempted', it just found nothing"
        );
    }

    #[test]
    fn advance_fresh_round_leftover_becomes_new_alternatives() {
        // Setup: fresh_candidates returns 3 peers; helper picks the
        // first, and the remaining 2 MUST be written back to
        // `alternatives` so subsequent breadth retries can use them.
        let id = contract_id();
        let tx = fresh_tx();
        let mut visited = VisitedPeers::new(&tx);
        let mut tried_peers: HashSet<std::net::SocketAddr> = HashSet::new();
        let mut alternatives: Vec<PeerKeyLocation> = Vec::new();
        let mut retries = 0usize;
        let mut attempts_at_hop = 1usize;

        let p1 = peer_at("10.0.0.1:1001");
        let p2 = peer_at("10.0.0.2:1002");
        let p3 = peer_at("10.0.0.3:1003");
        let p1_addr = p1.socket_addr().unwrap();
        let p2_addr = p2.socket_addr().unwrap();
        let p3_addr = p3.socket_addr().unwrap();

        let result = advance_to_next_peer_impl(
            &id,
            &mut visited,
            &mut tried_peers,
            &mut alternatives,
            &mut retries,
            &mut attempts_at_hop,
            |_, _| vec![p1.clone(), p2.clone(), p3.clone()],
        );

        let (_, picked) = result.expect("fresh round returns first candidate");
        assert_eq!(picked, p1_addr);
        assert_eq!(
            alternatives.len(),
            2,
            "rest of fresh becomes new alternatives"
        );
        let alt_addrs: Vec<_> = alternatives
            .iter()
            .filter_map(|p| p.socket_addr())
            .collect();
        assert!(alt_addrs.contains(&p2_addr));
        assert!(alt_addrs.contains(&p3_addr));
    }

    // ── Relay SUBSCRIBE driver structural pin tests. Anchor on
    // the `start_relay_subscribe` entry-point fn so module-level
    // docs referencing variant names don't contaminate the scan.

    fn relay_section(src: &str) -> &str {
        let start = src
            .find("pub(crate) async fn start_relay_subscribe(")
            .expect("start_relay_subscribe not found");
        let end = src
            .find("\n#[cfg(test)]")
            .expect("test module marker not found");
        &src[start..end]
    }

    /// Pin: dispatch entry must insert into `active_relay_subscribe_txs`
    /// BEFORE spawning. Same dedup-gate-ordering invariant as GET/PUT/
    /// UPDATE — without this, duplicate inbound `SubscribeMsg::Request`
    /// for an in-flight tx would spawn redundant drivers.
    #[test]
    fn start_relay_subscribe_checks_dedup_gate() {
        let src = include_str!("op_ctx_task.rs");
        let relay = relay_section(src);
        let window_start = relay
            .find("pub(crate) async fn start_relay_subscribe(")
            .expect("entry-point not found");
        let spawn_pos = relay[window_start..]
            .find("GlobalExecutor::spawn(run_relay_subscribe(")
            .expect("spawn site not found")
            + window_start;
        let insert_pos = relay[window_start..]
            .find("active_relay_subscribe_txs.insert(incoming_tx)")
            .expect("dedup insert site not found")
            + window_start;
        assert!(
            insert_pos < spawn_pos,
            "active_relay_subscribe_txs.insert MUST happen before GlobalExecutor::spawn"
        );
    }

    /// Pin: dedup rejection must emit `SubscribeMsgResult::NotFound`
    /// back to the rejected upstream. Silently dropping the duplicate
    /// request stalls the upstream's `send_to_and_await` for the full
    /// `OPERATION_TTL` (60 s), wasting its retry budget.
    #[test]
    fn dedup_rejection_emits_not_found_reply() {
        let src = include_str!("op_ctx_task.rs");
        let relay = relay_section(src);
        let after_insert = relay
            .split("active_relay_subscribe_txs.insert(incoming_tx)")
            .nth(1)
            .expect("dedup insert site not found");
        // Look at the body between the insert and the early return.
        let window_end = after_insert
            .find("return Ok(())")
            .expect("early return after dedup-reject not found");
        let window = &after_insert[..window_end];
        assert!(
            window.contains("SubscribeMsgResult::NotFound"),
            "dedup-reject path must emit SubscribeMsgResult::NotFound to \
             the rejected upstream so its send_to_and_await returns fast"
        );
        assert!(
            window.contains("send_fire_and_forget"),
            "dedup-reject NotFound must be sent via send_fire_and_forget \
             (not send_to_and_await — upstream's own waiter owns its slot)"
        );
    }

    /// Pin: dedup rejection must bump `RELAY_SUBSCRIBE_DEDUP_REJECTS`.
    #[test]
    fn dedup_rejection_increments_counter() {
        let src = include_str!("op_ctx_task.rs");
        let relay = relay_section(src);
        let after_insert = relay
            .split("active_relay_subscribe_txs.insert(incoming_tx)")
            .nth(1)
            .expect("dedup insert site not found");
        let window = &after_insert[..500.min(after_insert.len())];
        assert!(
            window.contains("RELAY_SUBSCRIBE_DEDUP_REJECTS.fetch_add"),
            "dedup gate must increment RELAY_SUBSCRIBE_DEDUP_REJECTS on rejection"
        );
    }

    /// Pin: RAII guard must clear `active_relay_subscribe_txs` + bump
    /// completion counters on drop.
    #[test]
    fn raii_guard_clears_dedup_set_on_drop() {
        let src = include_str!("op_ctx_task.rs");
        let drop_start = src
            .find("impl Drop for RelaySubscribeInflightGuard")
            .expect("RelaySubscribeInflightGuard Drop impl not found");
        let drop_body = &src[drop_start..drop_start + 600];
        assert!(
            drop_body.contains("active_relay_subscribe_txs"),
            "RelaySubscribeInflightGuard::drop must remove from active_relay_subscribe_txs"
        );
        assert!(
            drop_body.contains("RELAY_SUBSCRIBE_INFLIGHT.fetch_sub"),
            "RelaySubscribeInflightGuard::drop must decrement RELAY_SUBSCRIBE_INFLIGHT"
        );
        assert!(
            drop_body.contains("RELAY_SUBSCRIBE_COMPLETED_TOTAL.fetch_add"),
            "RelaySubscribeInflightGuard::drop must increment RELAY_SUBSCRIBE_COMPLETED_TOTAL"
        );
    }

    /// Pin: driver forwards downstream via `send_to_and_await` — SUBSCRIBE
    /// relay IS req/response. Fire-and-forget here would lose the reply.
    #[test]
    fn drive_relay_subscribe_forwards_via_send_to_and_await() {
        let src = include_str!("op_ctx_task.rs");
        let driver_start = src
            .find("async fn drive_relay_subscribe(")
            .expect("drive_relay_subscribe not found");
        let driver_end = src[driver_start..]
            .find("\nasync fn relay_subscribe_send_response(")
            .expect("driver body end not found")
            + driver_start;
        let driver_src = &src[driver_start..driver_end];
        assert!(
            driver_src.contains("ctx.send_to_and_await("),
            "drive_relay_subscribe must forward downstream via send_to_and_await"
        );
    }

    /// Pin: relay forward must reuse `incoming_tx`. Minting a fresh tx
    /// at each hop was the phase-5 GET 3^HTL amplifier.
    #[test]
    fn drive_relay_subscribe_reuses_incoming_tx_on_forward() {
        let src = include_str!("op_ctx_task.rs");
        let driver_start = src
            .find("async fn drive_relay_subscribe(")
            .expect("drive_relay_subscribe not found");
        let driver_end = src[driver_start..]
            .find("\nasync fn relay_subscribe_send_response(")
            .expect("driver body end not found")
            + driver_start;
        let driver_src = &src[driver_start..driver_end];
        let forward_pos = driver_src
            .find("SubscribeMsg::Request {")
            .expect("forward SubscribeMsg::Request not found in driver");
        let forward_window = &driver_src[forward_pos..forward_pos + 400];
        assert!(
            forward_window.contains("id: incoming_tx"),
            "relay forward must reuse incoming_tx"
        );
        assert!(
            !forward_window.contains("Transaction::new::<SubscribeMsg>()"),
            "relay forward must NOT mint a fresh Transaction"
        );
    }

    /// Pin: relay upstream reply MUST use `send_fire_and_forget`.
    /// Upstream's own `send_to_and_await` owns the reply slot.
    #[test]
    fn relay_subscribe_send_response_is_fire_and_forget() {
        let src = include_str!("op_ctx_task.rs");
        let fn_start = src
            .find("async fn relay_subscribe_send_response(")
            .expect("relay_subscribe_send_response not found");
        let fn_end = src[fn_start..]
            .find("\n}\n")
            .expect("function body end not found")
            + fn_start;
        let fn_src = &src[fn_start..fn_end];
        assert!(
            fn_src.contains("send_fire_and_forget"),
            "relay_subscribe_send_response must use send_fire_and_forget for the upstream response"
        );
    }

    /// Pin: relay driver MUST NOT emit `ForwardingAck`. The ack would
    /// share `incoming_tx` with the reply and satisfy upstream's
    /// capacity-1 `pending_op_results` waiter before the real Response.
    #[test]
    fn relay_subscribe_does_not_emit_forwarding_ack() {
        let src = include_str!("op_ctx_task.rs");
        let relay = relay_section(src);
        assert!(
            !relay.contains("SubscribeMsg::ForwardingAck {"),
            "relay SUBSCRIBE driver must NOT construct a ForwardingAck — \
             sharing incoming_tx with the reply collides with upstream's waiter"
        );
    }

    /// Pin: relay driver MUST call `register_downstream_subscriber`
    /// on BOTH the local-hit and downstream-Subscribed paths. Missing
    /// this registration breaks UPDATE propagation to the original
    /// requester (see subscribe.rs:1655–1688 for full reasoning).
    #[test]
    fn relay_subscribe_registers_downstream_on_both_paths() {
        let src = include_str!("op_ctx_task.rs");
        let driver_start = src
            .find("async fn drive_relay_subscribe(")
            .expect("drive_relay_subscribe not found");
        let driver_end = src[driver_start..]
            .find("\nasync fn relay_subscribe_send_response(")
            .expect("driver body end not found")
            + driver_start;
        let driver_src = &src[driver_start..driver_end];
        let hits = driver_src
            .matches("register_downstream_subscriber(")
            .count();
        assert!(
            hits >= 2,
            "driver must call register_downstream_subscriber on BOTH local-hit \
             and downstream-Subscribed paths (found {hits})"
        );
    }

    /// Pin: relay driver MUST NOT call `ring.subscribe` or
    /// `announce_contract_hosted` on behalf of the relayed Subscribed
    /// response. A relay is not itself a subscriber; doing so would
    /// install a lease and trigger #3763 subscription-storm feedback
    /// loops via the renewal cycle.
    #[test]
    fn relay_subscribe_does_not_install_lease_on_relayed_response() {
        let src = include_str!("op_ctx_task.rs");
        let driver_start = src
            .find("async fn drive_relay_subscribe(")
            .expect("drive_relay_subscribe not found");
        let driver_end = src[driver_start..]
            .find("\nasync fn relay_subscribe_send_response(")
            .expect("driver body end not found")
            + driver_start;
        let driver_src = &src[driver_start..driver_end];
        // Strip line comments so doc strings that mention these call-sites
        // as negative constraints do not trip the substring scan.
        let stripped: String = driver_src
            .lines()
            .map(|line| match line.find("//") {
                Some(idx) => &line[..idx],
                None => line,
            })
            .collect::<Vec<_>>()
            .join("\n");
        assert!(
            !stripped.contains("ring.subscribe("),
            "relay driver must NOT call ring.subscribe on relayed response"
        );
        assert!(
            !stripped.contains("complete_subscription_request"),
            "relay driver must NOT call complete_subscription_request on relayed response"
        );
        assert!(
            !stripped.contains("announce_contract_hosted"),
            "relay driver must NOT call announce_contract_hosted on relayed response"
        );
    }

    // ── classify_renewal_result tests ────────────────────────────────────
    //
    // The renewal classifier is the load-bearing decision point for
    // backoff bookkeeping. Misclassification of a transient channel error
    // as `Failed` would penalise the contract on backoff and reopen the
    // #3763 storm regression vector. These tests pin the mapping so a
    // future `OpError` variant can't silently land in the wrong bucket.

    #[test]
    fn classify_renewal_result_publish_ok_is_success() {
        use freenet_stdlib::client_api::{ContractResponse, HostResponse};
        use freenet_stdlib::prelude::{CodeHash, ContractInstanceId, ContractKey};
        let key = ContractKey::from_id_and_code(
            ContractInstanceId::new([1u8; 32]),
            CodeHash::new([2u8; 32]),
        );
        let result = Ok(DriverOutcome::Publish(Ok(HostResponse::ContractResponse(
            ContractResponse::SubscribeResponse {
                key,
                subscribed: true,
            },
        ))));
        assert!(matches!(
            classify_renewal_result(result),
            RenewalOutcome::Success
        ));
    }

    #[test]
    fn classify_renewal_result_skip_already_delivered_is_success() {
        let result = Ok(DriverOutcome::SkipAlreadyDelivered);
        assert!(matches!(
            classify_renewal_result(result),
            RenewalOutcome::Success
        ));
    }

    #[test]
    fn classify_renewal_result_publish_err_is_failed() {
        let host_err: freenet_stdlib::client_api::ClientError =
            freenet_stdlib::client_api::ErrorKind::OperationError {
                cause: "downstream peer rejected".into(),
            }
            .into();
        let result = Ok(DriverOutcome::Publish(Err(host_err)));
        match classify_renewal_result(result) {
            RenewalOutcome::Failed { reason } => {
                assert!(
                    reason.contains("downstream peer rejected"),
                    "reason should include the host-error cause; got: {reason}"
                );
            }
            RenewalOutcome::Success | RenewalOutcome::ChannelCongestion => {
                panic!("expected Failed, got non-Failed variant")
            }
        }
    }

    #[test]
    fn classify_renewal_result_notification_error_is_channel_congestion() {
        let result = Ok(DriverOutcome::InfrastructureError(
            OpError::NotificationError,
        ));
        assert!(matches!(
            classify_renewal_result(result),
            RenewalOutcome::ChannelCongestion
        ));
    }

    #[test]
    fn classify_renewal_result_notification_channel_error_is_channel_congestion() {
        let result = Ok(DriverOutcome::InfrastructureError(
            OpError::NotificationChannelError("channel full".into()),
        ));
        assert!(matches!(
            classify_renewal_result(result),
            RenewalOutcome::ChannelCongestion
        ));
    }

    #[test]
    fn classify_renewal_result_outer_err_notification_is_channel_congestion() {
        // `drive_client_subscribe_inner` propagates `OpError` via `?` from
        // `prepare_initial_request` / `complete_local_subscription`. A
        // NotificationError surfaced through the outer Err path must also
        // bucket into ChannelCongestion (not Failed) — otherwise renewals
        // racing the ring-update channel get an unwarranted backoff.
        let result: Result<DriverOutcome, OpError> = Err(OpError::NotificationError);
        assert!(matches!(
            classify_renewal_result(result),
            RenewalOutcome::ChannelCongestion
        ));
    }

    #[test]
    fn classify_renewal_result_unexpected_op_state_is_failed() {
        // Any OpError that is NOT a notification-channel error maps to
        // Failed. Picking UnexpectedOpState as a representative.
        let result: Result<DriverOutcome, OpError> = Err(OpError::UnexpectedOpState);
        assert!(matches!(
            classify_renewal_result(result),
            RenewalOutcome::Failed { .. }
        ));
    }

    #[test]
    fn classify_renewal_result_ring_error_no_hosting_peers_is_failed() {
        // Mirrors the "no remote peers available" exhaustion path in
        // `drive_client_subscribe_inner`. Must apply backoff (Failed),
        // not the no-penalty congestion bucket.
        let instance_id = freenet_stdlib::prelude::ContractInstanceId::new([7u8; 32]);
        let result: Result<DriverOutcome, OpError> =
            Err(OpError::RingError(RingError::NoHostingPeers(instance_id)));
        assert!(matches!(
            classify_renewal_result(result),
            RenewalOutcome::Failed { .. }
        ));
    }

    // ── classify_executor_subscribe_result tests ─────────────────────
    //
    // Executor auto-subscribe delivers Result<(), OpError> directly.
    // These pin the mapping so a future `OpError` / `DriverOutcome`
    // shape change can't silently shift the executor's success
    // criterion.

    #[test]
    fn classify_executor_subscribe_publish_ok_is_ok() {
        use freenet_stdlib::client_api::{ContractResponse, HostResponse};
        use freenet_stdlib::prelude::{CodeHash, ContractInstanceId, ContractKey};
        let key = ContractKey::from_id_and_code(
            ContractInstanceId::new([8u8; 32]),
            CodeHash::new([9u8; 32]),
        );
        let result = Ok(DriverOutcome::Publish(Ok(HostResponse::ContractResponse(
            ContractResponse::SubscribeResponse {
                key,
                subscribed: true,
            },
        ))));
        assert!(classify_executor_subscribe_result(result).is_ok());
    }

    #[test]
    fn classify_executor_subscribe_skip_already_delivered_is_ok() {
        let result = Ok(DriverOutcome::SkipAlreadyDelivered);
        assert!(classify_executor_subscribe_result(result).is_ok());
    }

    #[test]
    fn classify_executor_subscribe_publish_err_is_network_exhausted() {
        let host_err: freenet_stdlib::client_api::ClientError =
            freenet_stdlib::client_api::ErrorKind::OperationError {
                cause: "downstream peer rejected".into(),
            }
            .into();
        let result = Ok(DriverOutcome::Publish(Err(host_err)));
        match classify_executor_subscribe_result(result) {
            Err(ExecutorSubscribeError::NetworkExhausted(reason)) => {
                assert!(
                    reason.contains("downstream peer rejected"),
                    "reason should include host-error cause; got: {reason}"
                );
            }
            Ok(_) | Err(ExecutorSubscribeError::Infra(_)) => {
                panic!("expected NetworkExhausted variant")
            }
        }
    }

    #[test]
    fn classify_executor_subscribe_infrastructure_error_is_infra() {
        let result = Ok(DriverOutcome::InfrastructureError(
            OpError::NotificationError,
        ));
        match classify_executor_subscribe_result(result) {
            Err(ExecutorSubscribeError::Infra(OpError::NotificationError)) => {}
            other => panic!("expected Infra(NotificationError), got {other:?}"),
        }
    }

    #[test]
    fn classify_executor_subscribe_outer_err_propagates_as_infra() {
        let result: Result<DriverOutcome, OpError> = Err(OpError::UnexpectedOpState);
        match classify_executor_subscribe_result(result) {
            Err(ExecutorSubscribeError::Infra(OpError::UnexpectedOpState)) => {}
            other => panic!("expected Infra(UnexpectedOpState), got {other:?}"),
        }
    }

    // ── run_executor_subscribe body pin tests ─────────────────────────
    //
    // Behavioral end-to-end tests of `run_executor_subscribe` would
    // require spinning a full `OpManager` (no shared test fixture
    // exists), which is non-trivial for a unit-test layer. Pin tests
    // below substitute by asserting the load-bearing source-level
    // invariants — most importantly that the `LocallyComplete` arm
    // does NOT call `complete_local_subscription` (which would emit
    // `NodeEvent::LocalSubscribeComplete` and trip the
    // standalone-parent branch in `p2p_protoc.rs` that publishes a
    // result keyed on `executor_tx` to `result_router_tx` with no
    // client waiter).

    #[test]
    fn run_executor_subscribe_locally_complete_skips_local_subscribe_complete_event() {
        let src = include_str!("op_ctx_task.rs");
        let body = src
            .split("pub(crate) async fn run_executor_subscribe(")
            .nth(1)
            .expect("run_executor_subscribe must exist")
            .split(
                "
}",
            )
            .next()
            .expect("closing brace of run_executor_subscribe");
        // Strip line comments so doc strings that mention the absent
        // helper as a negative constraint do not trip the substring
        // scan.
        let stripped: String = body
            .lines()
            .map(|line| match line.find("//") {
                Some(idx) => &line[..idx],
                None => line,
            })
            .collect::<Vec<_>>()
            .join("\n");
        // Locally-complete branch must NOT delegate to the helper that
        // emits the local-completion event. Compose the needle at
        // runtime so the test itself doesn't trip the scan.
        let helper = ["complete_local_", "subscription"].concat();
        assert!(
            !stripped.contains(&helper),
            "run_executor_subscribe must NOT call the local-completion \
             helper on the LocallyComplete branch — would publish a \
             result for executor_tx that has no client waiter. Handle \
             interest registration + op_manager.completed inline."
        );
        // Locally-complete branch must still register local interest
        // and complete the op (replaces the side effects of
        // complete_local_subscription).
        assert!(
            body.contains("interest_manager.add_local_client"),
            "run_executor_subscribe LocallyComplete arm must register \
             local interest (mirrors complete_local_subscription side \
             effect)"
        );
        assert!(
            body.contains("op_manager.completed("),
            "run_executor_subscribe LocallyComplete arm must call \
             op_manager.completed() to drop the under_progress slot"
        );
    }

    #[test]
    fn run_executor_subscribe_no_hosting_peers_maps_to_infra_ring_error() {
        let src = include_str!("op_ctx_task.rs");
        let body = src
            .split("pub(crate) async fn run_executor_subscribe(")
            .nth(1)
            .expect("run_executor_subscribe must exist")
            .split(
                "
}",
            )
            .next()
            .expect("closing brace of run_executor_subscribe");
        // Both PeerNotJoined and NoHostingPeers must wrap as
        // `ExecutorSubscribeError::Infra(...)` so the executor's
        // `subscribe()` sees a structured failure cause rather than
        // an opaque string.
        assert!(
            body.contains("RingError::NoHostingPeers"),
            "run_executor_subscribe must surface NoHostingPeers via \
             RingError::NoHostingPeers"
        );
        assert!(
            body.contains("RingError::PeerNotJoined"),
            "run_executor_subscribe must surface PeerNotJoined via \
             RingError::PeerNotJoined"
        );
        assert!(
            body.contains("ExecutorSubscribeError::Infra"),
            "run_executor_subscribe early-return error branches must \
             wrap as ExecutorSubscribeError::Infra(...)"
        );
    }

    /// Issue #4010: `deliver_outcome` must record the SUBSCRIBE outcome
    /// to the dashboard `op_stats.subscribes` counter (the driver
    /// bypass at `handle_pure_network_message_v1` skips the legacy
    /// `report_result` recording path), and it must NOT record sub-op
    /// SUBSCRIBE attempts (those are background plumbing spawned by
    /// PUT/GET, not user-initiated subscribes).
    ///
    /// Source-level regression pin: `deliver_outcome` must call
    /// `record_op_result(OpType::Subscribe, ...)` and gate the call on
    /// `!client_tx.is_sub_operation()`. Walks brace depth from the
    /// opening `{` so the test does not depend on any nearby section
    /// comments staying named the same.
    #[test]
    fn deliver_outcome_records_subscribe_op_result() {
        const SOURCE: &str = include_str!("op_ctx_task.rs");
        let prod = production_source(SOURCE);
        let body = extract_fn_body(prod, "fn deliver_outcome(");

        assert!(
            body.contains("record_op_result"),
            "deliver_outcome must call record_op_result so the dashboard \
             SUBSCRIBE counter advances on driver terminal replies. \
             Issue #4010."
        );
        assert!(
            body.contains("OpType::Subscribe"),
            "record_op_result inside deliver_outcome must be passed \
             OpType::Subscribe (not Get/Put/Update)."
        );
        assert!(
            body.contains("!client_tx.is_sub_operation()"),
            "deliver_outcome must gate record_op_result on \
             `!client_tx.is_sub_operation()` (note the leading `!`) so \
             sub-op SUBSCRIBE spawned by PUT/GET \
             (`maybe_subscribe_child`, `start_subscription_request`) \
             does not inflate the user-facing dashboard counter. \
             A flipped guard would silently re-introduce the inflation \
             reported by Codex review. Issue #4010."
        );
    }

    /// Issue #4010 negative-path pin: renewal subscribes
    /// (`run_renewal_subscribe`) and executor auto-subscribes
    /// (`run_executor_subscribe`) MUST NOT call `deliver_outcome` or
    /// `record_op_result` directly. They have their own return-value
    /// delivery paths and are background traffic that would inflate the
    /// user-facing dashboard counter (renewals fire every 2 minutes per
    /// active subscription).
    #[test]
    fn renewal_and_executor_subscribe_do_not_record_op_stats() {
        const SOURCE: &str = include_str!("op_ctx_task.rs");
        let prod = production_source(SOURCE);

        let renewal_body = extract_fn_body(prod, "pub(crate) async fn run_renewal_subscribe(");
        assert!(
            !renewal_body.contains("deliver_outcome"),
            "run_renewal_subscribe must NOT route through deliver_outcome \
             — that would record renewals against the dashboard SUBSCRIBE \
             counter. Issue #4010."
        );
        assert!(
            !renewal_body.contains("record_op_result"),
            "run_renewal_subscribe must NOT call record_op_result. \
             Issue #4010."
        );

        let executor_body = extract_fn_body(prod, "pub(crate) async fn run_executor_subscribe(");
        assert!(
            !executor_body.contains("deliver_outcome"),
            "run_executor_subscribe must NOT route through deliver_outcome \
             — that would record executor auto-subscribes against the \
             dashboard SUBSCRIBE counter. Issue #4010."
        );
        assert!(
            !executor_body.contains("record_op_result"),
            "run_executor_subscribe must NOT call record_op_result. \
             Issue #4010."
        );
    }

    /// Pure-function unit test: `classify_subscribe_outcome_for_op_stats`
    /// covers every `DriverOutcome` variant. Pins the success/failure
    /// mapping against accidental `success: !success` flips and against
    /// dropping `SkipAlreadyDelivered` from the success arm (which would
    /// silently undercount local-hit subscribes). Issue #4010.
    #[test]
    fn classify_subscribe_outcome_covers_all_variants() {
        use freenet_stdlib::client_api::{ContractResponse, ErrorKind, HostResponse};
        use freenet_stdlib::prelude::{CodeHash, ContractInstanceId, ContractKey};

        let key = ContractKey::from_id_and_code(
            ContractInstanceId::new([7u8; 32]),
            CodeHash::new([8u8; 32]),
        );
        let publish_ok = DriverOutcome::Publish(Ok(HostResponse::ContractResponse(
            ContractResponse::SubscribeResponse {
                key,
                subscribed: true,
            },
        )));
        let publish_err = DriverOutcome::Publish(Err(ErrorKind::OperationError {
            cause: "synthetic".into(),
        }
        .into()));
        let skip = DriverOutcome::SkipAlreadyDelivered;
        let infra = DriverOutcome::InfrastructureError(OpError::UnexpectedOpState);

        assert!(classify_subscribe_outcome_for_op_stats(&publish_ok));
        assert!(!classify_subscribe_outcome_for_op_stats(&publish_err));
        assert!(classify_subscribe_outcome_for_op_stats(&skip));
        assert!(!classify_subscribe_outcome_for_op_stats(&infra));
    }

    /// Regression for the peer-dashboard prediction-graph data-collection
    /// regression: the client-initiated SUBSCRIBE driver must emit a
    /// `RouteEvent` carrying `RouteOutcome::Success { time_to_response_start,
    /// payload_size: 0, payload_transfer_time: ZERO }` on the Subscribed
    /// terminal branch. Before this fix the driver emitted NO route event
    /// at all on the originator's success path (the relay branch did, but
    /// the originator did not), so the router's
    /// `response_start_time_estimator` saw zero observations from
    /// client-initiated subscribes — contributing to the
    /// Failure-Probability-only peer-dashboard regression.
    #[test]
    fn drive_client_subscribe_inner_emits_timed_success_on_subscribed() {
        const SOURCE: &str = include_str!("op_ctx_task.rs");
        let prod = production_source(SOURCE);
        let body = extract_fn_body(prod, "async fn drive_client_subscribe_inner(");

        // Send-time capture must precede the round-trip await and MUST
        // use `tokio::time::Instant` — `std::time::Instant` is banned in
        // `crates/core/` by Rule Lint (see .claude/rules/code-style.md)
        // and would not virtualize under `start_paused(true)` in DST
        // simulation tests.
        assert!(
            body.contains("let request_sent_at = tokio::time::Instant::now();"),
            "drive_client_subscribe_inner must capture request_sent_at via \
             tokio::time::Instant::now() before send_to_and_await; without \
             this the Subscribed branch has no time_to_response_start for \
             the router."
        );

        // Subscribed branch must build a timed RouteOutcome::Success with
        // payload_size=0 (matches the legacy `subscribe.rs::outcome()`
        // semantics — SUBSCRIBE has no payload, so transfer_rate_estimator
        // is intentionally not fed).
        let subscribed_pos = body
            .find("ReplyClass::Subscribed { key }")
            .expect("Subscribed arm must exist");
        let after = &body[subscribed_pos..];
        assert!(
            after.contains("RouteOutcome::Success")
                && after.contains("time_to_response_start")
                && after.contains("payload_size: 0")
                && after.contains("payload_transfer_time: std::time::Duration::ZERO"),
            "Subscribed branch must build RouteOutcome::Success with \
             time_to_response_start + payload_size: 0 + payload_transfer_time: ZERO. \
             Emitting nothing leaves the router's response-time estimator \
             with zero observations from client-initiated subscribes."
        );
        assert!(
            after.contains("op_manager.ring.routing_finished("),
            "Subscribed branch must hand the RouteEvent to \
             `routing_finished` so the router actually learns from it."
        );
    }

    /// #3445 regression (source-scrape pin): the client-initiated SUBSCRIBE
    /// driver must emit a `NetEventLog::subscribe_timeout` telemetry event on
    /// the branch where every candidate peer timed out without a terminal
    /// reply. Before the fix this branch returned an `OperationError` with no
    /// telemetry, so a timed-out subscribe left a `subscribe_request` on the
    /// dashboard with no paired outcome (the River container contract showed
    /// 196 requests and 0 outcomes — all silent timeouts).
    ///
    /// This guards against a future migration silently dropping the call, the
    /// telemetry-counter-rot failure mode from
    /// `.claude/rules/bug-prevention-patterns.md`.
    #[test]
    fn drive_client_subscribe_inner_emits_timeout_telemetry_on_exhaustion() {
        const SOURCE: &str = include_str!("op_ctx_task.rs");
        let prod = production_source(SOURCE);
        let body = extract_fn_body(prod, "async fn drive_client_subscribe_inner(");

        // The timeout-exhausted branch is identified by its terminal cause
        // string; the telemetry emission must appear before that return.
        let timeout_return = body
            .find("timed out after {} rounds")
            .expect("timeout-exhausted branch must exist");
        let before_timeout_return = &body[..timeout_return];
        assert!(
            before_timeout_return.contains("NetEventLog::subscribe_timeout("),
            "the subscribe timeout-exhausted branch must emit \
             NetEventLog::subscribe_timeout(..) before returning the \
             OperationError; otherwise a timed-out subscribe is invisible on \
             the telemetry dashboard (#3445)."
        );
        // It must actually be registered into the event pipeline, not built
        // and dropped.
        let emit_pos = before_timeout_return
            .rfind("NetEventLog::subscribe_timeout(")
            .unwrap();
        assert!(
            before_timeout_return[emit_pos..].contains("register_events("),
            "the subscribe_timeout event must be handed to \
             op_manager.ring.register_events(..) so it reaches the telemetry \
             pipeline (#3445)."
        );
    }

    /// Issue #4350 pin: the renewal driver must (a) establish a task budget
    /// (`renewal_deadline`) from `Ring::RENEWAL_TASK_BUDGET` on the renewal
    /// path, (b) clamp EACH attempt's timeout to the budget remaining until
    /// that deadline (so retries — not just the first attempt — can't outlive
    /// the outer cancel), and (c) feed that per-attempt `attempt_timeout` into
    /// the `tokio::time::timeout` wrapping `send_to_and_await`. Non-renewal
    /// paths keep `OPERATION_TTL`. A regression that hardcoded `OPERATION_TTL`
    /// for renewals, or that used a fixed per-attempt timeout instead of the
    /// remaining budget, would let a slow peer's reply be killed by the 25 s
    /// outer cancel in `recover_orphaned_subscriptions`, re-introducing the
    /// discarded-reply drops. The behavioural effect is covered by
    /// `op_ctx::tests::renewal_per_attempt_timeout_fires_before_outer_cancel`.
    #[test]
    fn renewal_driver_clamps_attempt_timeout_to_remaining_budget() {
        const SOURCE: &str = include_str!("op_ctx_task.rs");
        let prod = production_source(SOURCE);
        let body = extract_fn_body(prod, "async fn drive_client_subscribe_inner(");

        // (a) The renewal task budget is established from the config-tied
        // constant and gated on the renewal path.
        assert!(
            body.contains("let renewal_deadline =")
                && body.contains("is_renewal.then(")
                && body.contains("crate::ring::Ring::RENEWAL_TASK_BUDGET"),
            "drive_client_subscribe_inner must establish a renewal task \
             deadline from Ring::RENEWAL_TASK_BUDGET on the is_renewal path \
             (issue #4350)."
        );
        // (b) Each attempt's timeout is the remaining budget, capped at the
        // per-attempt ceiling — not a fixed constant — and non-renewal keeps
        // OPERATION_TTL.
        assert!(
            body.contains("saturating_duration_since")
                && body.contains("crate::ring::Ring::RENEWAL_PER_ATTEMPT_TIMEOUT")
                && body.contains("crate::ring::Ring::RENEWAL_MIN_ATTEMPT_BUDGET")
                && body.contains("None => OPERATION_TTL"),
            "each renewal attempt must clamp its timeout to the remaining \
             budget (min RENEWAL_PER_ATTEMPT_TIMEOUT, floor \
             RENEWAL_MIN_ATTEMPT_BUDGET), with OPERATION_TTL on the non-renewal \
             path (issue #4350)."
        );
        // (c) The per-attempt round-trip must be wrapped in the computed
        // `attempt_timeout`, not a hardcoded constant.
        let timeout_pos = body
            .find("ctx.send_to_and_await(current_target_addr")
            .expect("send_to_and_await call site must exist");
        let before = &body[..timeout_pos];
        let wrap_pos = before
            .rfind("tokio::time::timeout(")
            .expect("send_to_and_await must be wrapped in tokio::time::timeout");
        assert!(
            before[wrap_pos..].contains("attempt_timeout"),
            "the per-attempt send_to_and_await must be wrapped in \
             tokio::time::timeout(attempt_timeout, ..), not a hardcoded \
             OPERATION_TTL (issue #4350)."
        );
    }

    /// Truncate the source string at the `#[cfg(test)]` marker so the
    /// pin tests never see code or comments inside the test module
    /// (avoids false positives where a test asserts a substring that
    /// also appears in a sibling test).
    fn production_source(full: &str) -> &str {
        let cutoff = full
            .find("#[cfg(test)]")
            .expect("file must have a #[cfg(test)] section");
        &full[..cutoff]
    }

    /// Walk brace depth from the opening `{` of the named fn to the
    /// matching `}`. Returns the body slice (excluding the braces).
    /// Robust to nearby section comments being renamed.
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

    // ---- #3808: inter-attempt retry pacing (jitter + delay) ----

    /// The jittered delay must always stay within the ±20% band around
    /// `RETRY_BASE_DELAY` the retry/backoff rule mandates: never below
    /// 0.8× (could degenerate to a spin loop) and strictly below 1.2×
    /// (the half-open `random_range` upper bound). Sampled across many
    /// seeds so a bad jitter formula can't slip through on one lucky seed.
    #[test]
    fn jittered_retry_delay_stays_within_plus_minus_20_percent() {
        let lo = RETRY_BASE_DELAY.mul_f64(0.8);
        let hi = RETRY_BASE_DELAY.mul_f64(1.2);
        for seed in 0..256u64 {
            let _guard = GlobalRng::seed_guard(seed);
            for _ in 0..32 {
                let d = jittered_retry_delay();
                assert!(
                    d >= lo,
                    "delay {d:?} below 0.8× base {lo:?} (seed {seed}) — \
                     would weaken the spin-loop guard"
                );
                assert!(
                    d < hi,
                    "delay {d:?} at/above 1.2× base {hi:?} (seed {seed})"
                );
            }
        }
    }

    /// The delay must be bounded well under the 1s plain-`sleep` cap the
    /// retry/backoff rule sets for non-interruptible sleeps. A regression
    /// that bumped `RETRY_BASE_DELAY` past ~830ms would silently violate
    /// that exemption; pin the worst case here.
    #[test]
    fn jittered_retry_delay_under_one_second_cap() {
        for seed in 0..64u64 {
            let _guard = GlobalRng::seed_guard(seed);
            for _ in 0..32 {
                assert!(
                    jittered_retry_delay() < Duration::from_secs(1),
                    "inter-attempt delay must stay under the 1s plain-sleep cap"
                );
            }
        }
    }

    /// Under a fixed seed the jitter is deterministic (it routes through
    /// `GlobalRng`, not `rand::thread_rng()`), so the simulation harness
    /// reproduces identical pacing across runs. Guards against a
    /// regression to non-deterministic randomness.
    #[test]
    fn jittered_retry_delay_is_deterministic_under_seed() {
        let first = {
            let _guard = GlobalRng::seed_guard(0xC0FFEE);
            [jittered_retry_delay(), jittered_retry_delay()]
        };
        let second = {
            let _guard = GlobalRng::seed_guard(0xC0FFEE);
            [jittered_retry_delay(), jittered_retry_delay()]
        };
        assert_eq!(first, second, "jitter must be GlobalRng-deterministic");
    }

    /// Source-scrape pin: every `advance_to_next_peer` success arm in the
    /// driver loop MUST pace the next attempt via `sleep_before_retry`
    /// before its `continue`. This is the #3808 DoS guard — a retry path
    /// that skips the delay re-opens the topology-wide spin loop. The
    /// count is tied to the three terminal-outcome arms (wire_error,
    /// timeout, not_found) that advance and retry.
    #[test]
    fn every_retry_advance_paces_before_continue() {
        let src = include_str!("op_ctx_task.rs");
        let body = extract_fn_body(src, "async fn drive_client_subscribe_inner(");
        let advance_arms = body.matches("current_target_addr = next_addr;").count();
        let paced = body.matches("sleep_before_retry().await;").count();
        assert_eq!(
            advance_arms, 3,
            "expected 3 advance-and-retry arms (wire_error, timeout, not_found)"
        );
        assert_eq!(
            paced, advance_arms,
            "every advance-and-retry arm must call sleep_before_retry() \
             before continue (#3808 spin-loop guard)"
        );
    }
}
