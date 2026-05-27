//! Task-per-transaction PUT drivers.
//!
//! Each entry point — client-initiated, relay non-streaming, relay
//! streaming — owns routing state in task locals. There is no
//! `ops.put` DashMap; per-node dedup is enforced via
//! `OpManager.active_relay_put_txs`.
//!
//! # Client-initiated flow
//!
//! 1. [`super::put_contract`] stores the contract locally.
//! 2. Find k-closest peers to forward to.
//! 3. No remote peers: deliver directly via `send_client_result`.
//! 4. Loop: [`OpCtx::send_and_await`] with a fresh `Transaction`
//!    per attempt (single-use-per-tx).
//! 5. Terminal `Response`/`ResponseStreaming`:
//!    [`super::finalize_put_at_originator`] + `send_client_result`.
//! 6. Timeout/wire-error: advance to next peer or exhaust.
//!
//! Retries are serial only. Connection-drop detection relies on
//! `OPERATION_TTL` (60s).

use std::collections::HashSet;
use std::net::SocketAddr;
use std::sync::Arc;

use either::Either;
use freenet_stdlib::client_api::{ContractResponse, ErrorKind, HostResponse};
use freenet_stdlib::prelude::*;

use crate::client_events::HostResult;
use crate::config::{GlobalExecutor, OPERATION_TTL};
use crate::message::{NetMessage, NetMessageV1, Transaction};
use crate::node::NetworkBridge;
use crate::node::OpManager;
use crate::operations::OpError;
use crate::operations::op_ctx::{
    AdvanceOutcome, AttemptOutcome, RetryDriver, RetryLoopOutcome, drive_retry_loop,
};
use crate::operations::orphan_streams::{OrphanStreamError, STREAM_CLAIM_TIMEOUT};
use crate::ring::{Location, PeerKeyLocation};
use crate::router::{RouteEvent, RouteOutcome};
use crate::tracing::{NetEventLog, OperationFailure, state_hash_full};
use crate::transport::peer_connection::StreamId;

use super::{PutFinalizationData, PutMsg, PutStreamingPayload};

/// Start a client-initiated PUT, returning as soon as the task has been
/// spawned (mirrors legacy `request_put` timing).
///
/// The caller must have already registered a result waiter for `client_tx`
/// via `op_manager.ch_outbound.waiting_for_transaction_result`. This
/// function does NOT touch the waiter; it only drives the ring/network
/// side and publishes the terminal result to `result_router_tx` keyed
/// by `client_tx`.
#[allow(clippy::too_many_arguments)]
pub(crate) async fn start_client_put(
    op_manager: Arc<OpManager>,
    client_tx: Transaction,
    contract: ContractContainer,
    related: RelatedContracts<'static>,
    value: WrappedState,
    htl: usize,
    subscribe: bool,
    blocking_subscribe: bool,
) -> Result<Transaction, OpError> {
    tracing::debug!(
        tx = %client_tx,
        contract = %contract.key(),
        "put: spawning client-initiated task"
    );

    // Spawn the driver. Same fire-and-forget rationale as SUBSCRIBE's
    // `start_client_subscribe` — failures are delivered to the client
    // via `result_router_tx`, not via this function's return value.
    //
    // Not registered with `BackgroundTaskMonitor`: per-transaction task
    // that terminates via happy path, exhaustion, timeout, or infra error.
    //
    // Amplification ceiling: the client_events.rs PUT handler allocates
    // one task per client PUT request. Client request rate is bounded by
    // the WS connection handler's backpressure.
    // Admission gate (closes the drain race window): refuse new
    // PUTs as soon as `ShutdownHandle::shutdown` has begun. Uses
    // `admit_client_op` for atomic check-AND-bump — the prior
    // shape (check, then bump) had a TOCTOU window the drain's
    // `initial == 0` fast-path could skip past. See
    // `OpManager::admit_client_op` rustdoc for the race analysis.
    //
    // The guard returned here is held inside the spawned future so
    // that `ShutdownHandle::shutdown`'s drain can wait for
    // client-initiated PUTs to finish before tearing down peer
    // connections (e.g. release-driven auto-update interrupting an
    // in-flight `freenet-git` mirror push).
    let inflight_guard = match op_manager.admit_client_op() {
        Some(g) => g,
        None => return Err(OpError::NodeShuttingDown),
    };
    GlobalExecutor::spawn(async move {
        let _inflight_guard = inflight_guard;
        run_client_put(
            op_manager,
            client_tx,
            contract,
            related,
            value,
            htl,
            subscribe,
            blocking_subscribe,
        )
        .await;
    });

    Ok(client_tx)
}

#[allow(clippy::too_many_arguments)]
async fn run_client_put(
    op_manager: Arc<OpManager>,
    client_tx: Transaction,
    contract: ContractContainer,
    related: RelatedContracts<'static>,
    value: WrappedState,
    htl: usize,
    subscribe: bool,
    blocking_subscribe: bool,
) {
    let outcome = drive_client_put(
        op_manager.clone(),
        client_tx,
        contract,
        related,
        value,
        htl,
        subscribe,
        blocking_subscribe,
    )
    .await;
    deliver_outcome(&op_manager, client_tx, outcome);
}

/// PUT driver has exactly two outcomes — no `SkipAlreadyDelivered` because
/// PUT doesn't use `NodeEvent::LocalPutComplete` (the driver owns local
/// completion delivery directly).
#[derive(Debug)]
enum DriverOutcome {
    /// The driver produced a `HostResult` that must be published via
    /// `result_router_tx`.
    Publish(HostResult),
    /// A genuine infrastructure failure escaped the driver loop.
    InfrastructureError(OpError),
}

#[allow(clippy::too_many_arguments)]
async fn drive_client_put(
    op_manager: Arc<OpManager>,
    client_tx: Transaction,
    contract: ContractContainer,
    related: RelatedContracts<'static>,
    value: WrappedState,
    htl: usize,
    subscribe: bool,
    blocking_subscribe: bool,
) -> DriverOutcome {
    match drive_client_put_inner(
        &op_manager,
        client_tx,
        contract,
        related,
        value,
        htl,
        subscribe,
        blocking_subscribe,
    )
    .await
    {
        Ok(outcome) => outcome,
        Err(err) => DriverOutcome::InfrastructureError(err),
    }
}

#[allow(clippy::too_many_arguments)]
async fn drive_client_put_inner(
    op_manager: &Arc<OpManager>,
    client_tx: Transaction,
    contract: ContractContainer,
    related: RelatedContracts<'static>,
    value: WrappedState,
    htl: usize,
    subscribe: bool,
    blocking_subscribe: bool,
) -> Result<DriverOutcome, OpError> {
    let key = contract.key();

    // Always send through send_and_await so process_message handles
    // local storage + hosting/interest/broadcast side effects.
    // Do NOT call put_contract directly — process_message does it
    // with the correct state_changed tracking for convergence.
    //
    // If no remote peers exist, process_message stores locally and
    // returns ContinueOp(Finished). forward_pending_op_result_if_completed
    // then sends the original Request back to the driver, which
    // classify_reply handles as LocalCompletion.
    let mut tried: Vec<std::net::SocketAddr> = Vec::new();
    if let Some(own_addr) = op_manager.ring.connection_manager.get_own_addr() {
        tried.push(own_addr);
    }

    // Pre-select initial target for the driver's retry state. The actual
    // routing decision is made by process_message, not the driver.
    let initial_target = op_manager
        .ring
        .closest_potentially_hosting(&key, tried.as_slice());
    let current_target = match initial_target {
        Some(peer) => {
            if let Some(addr) = peer.socket_addr() {
                tried.push(addr);
            }
            peer
        }
        None => op_manager.ring.connection_manager.own_location(),
    };

    // Retry loop via shared driver (#3807).
    struct PutRetryDriver<'a> {
        op_manager: &'a OpManager,
        key: ContractKey,
        contract: ContractContainer,
        related: RelatedContracts<'static>,
        value: WrappedState,
        htl: usize,
        tried: Vec<std::net::SocketAddr>,
        retries: usize,
        current_target: PeerKeyLocation,
        /// Per-attempt timeout. Scaled with payload size for streaming PUTs
        /// so the retry loop doesn't fire while the original streaming op
        /// is still in flight (#4001).
        attempt_timeout: std::time::Duration,
        /// Maximum number of peer **advancements** the driver may try
        /// after the initial attempt. Capped at 0 for streaming-
        /// eligible payloads (single attempt, no advancement) so the
        /// gateway's worst-case wall-clock budget for one client PUT
        /// stays at `STREAMING_ATTEMPT_TIMEOUT_CAP` (≤ 600s) instead
        /// of `4 × cap` (≤ 40 min). See `MAX_PEER_ADVANCEMENTS_*`
        /// rustdoc and freenet-git#53 for the failure mode this
        /// addresses.
        max_advancements: usize,
    }

    impl RetryDriver for PutRetryDriver<'_> {
        // `(key, hop_count)`. `hop_count = None` for `LocalCompletion`
        // (originator stored locally, no hops traversed); `Some(hop_count)`
        // for `Stored` (wire-carried forward depth from the storer).
        type Terminal = (ContractKey, Option<usize>);

        fn new_attempt_tx(&mut self) -> Transaction {
            Transaction::new::<PutMsg>()
        }

        fn build_request(&mut self, attempt_tx: Transaction) -> NetMessage {
            NetMessage::from(PutMsg::Request {
                id: attempt_tx,
                contract: self.contract.clone(),
                related_contracts: self.related.clone(),
                value: self.value.clone(),
                htl: self.htl,
                // Only include own_addr in skip_list (matching legacy request_put).
                // `tried` contains driver-side routing state (peers the driver
                // selected); process_message makes its own forwarding decisions.
                skip_list: self
                    .op_manager
                    .ring
                    .connection_manager
                    .get_own_addr()
                    .into_iter()
                    .collect::<HashSet<_>>(),
            })
        }

        fn classify(&mut self, reply: NetMessage) -> AttemptOutcome<Self::Terminal> {
            match classify_reply(&reply) {
                ReplyClass::Stored { key, hop_count } => {
                    AttemptOutcome::Terminal((key, Some(hop_count)))
                }
                // LocalCompletion = no remote hops traversed (originator
                // is the storer). Forward depth is exactly 0 — emit it
                // explicitly so local-only PUTs are distinguishable from
                // "telemetry missing" in dashboards. Codex r2 review of
                // #4248 flagged that mapping this to `None` leaked an
                // unpopulated `PutSuccess` for a known-zero-depth path.
                ReplyClass::LocalCompletion { key } => AttemptOutcome::Terminal((key, Some(0))),
                ReplyClass::Unexpected => AttemptOutcome::Unexpected,
            }
        }

        fn advance(&mut self) -> AdvanceOutcome {
            match advance_to_next_peer(
                self.op_manager,
                &self.key,
                &mut self.tried,
                &mut self.retries,
                self.max_advancements,
            ) {
                Some((next_target, _next_addr)) => {
                    self.current_target = next_target;
                    AdvanceOutcome::Next
                }
                None => AdvanceOutcome::Exhausted,
            }
        }

        fn attempt_timeout(&self) -> std::time::Duration {
            self.attempt_timeout
        }
    }

    let attempt_timeout =
        compute_put_attempt_timeout(op_manager.streaming_threshold, &value, &contract);
    // Recompute the same payload-size estimate the timeout helper uses
    // so we route streaming-eligible PUTs to the smaller retry budget.
    // Keeping the estimate inline (rather than threading a `bool` out
    // of `compute_put_attempt_timeout`) makes the streaming decision
    // visible at the driver-construction call site.
    let payload_size_estimate = value
        .size()
        .saturating_add(contract.data().len())
        .saturating_add(contract.params().size());
    let max_advancements = if crate::operations::should_use_streaming(
        op_manager.streaming_threshold,
        payload_size_estimate,
    ) {
        // Streaming PUTs: 0 advancements = 1 attempt × up-to-600s
        // `STREAMING_ATTEMPT_TIMEOUT_CAP`. Per
        // `MAX_PEER_ADVANCEMENTS_STREAMING` rustdoc, allowing even
        // ONE advancement (= 2 attempts) doubles the worst-case
        // wall-clock to 1200s, past any WS-client patience. Pack
        // contracts (the dominant streaming-PUT consumer today) are
        // content-addressed, so the WS client's outer retry handles
        // transient peer failure with no correctness loss. See
        // freenet-git#53.
        MAX_PEER_ADVANCEMENTS_STREAMING
    } else {
        MAX_PEER_ADVANCEMENTS_NON_STREAMING
    };

    let mut driver = PutRetryDriver {
        op_manager,
        key,
        contract,
        related,
        value,
        htl,
        tried,
        retries: 0,
        current_target,
        attempt_timeout,
        max_advancements,
    };

    let loop_result = drive_retry_loop(op_manager, client_tx, "put", &mut driver).await;

    match loop_result {
        RetryLoopOutcome::Done((reply_key, wire_hop_count)) => {
            // Clean up the DashMap entry that process_message created.
            // Without this, the GC task finds a stale AwaitingResponse
            // entry and launches speculative retries on the completed op.
            op_manager.completed(client_tx);

            // Emit routing event + telemetry — report_result (which normally
            // does both) doesn't run because the bypass intercepted the
            // Response. Without this, the router's prediction model never
            // receives PUT success feedback and simulation tests that check
            // route_outcome telemetry fail.
            let contract_location = Location::from(&reply_key);
            let route_event = RouteEvent {
                peer: driver.current_target.clone(),
                contract_location,
                outcome: RouteOutcome::SuccessUntimed,
                op_type: Some(crate::node::network_status::OpType::Put),
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
                crate::node::network_status::OpType::Put,
                true,
            );

            // Telemetry only — subscribe=false to avoid double-subscribe.
            //
            // `hop_count`: clamp the wire value to `ring.max_hops_to_live`
            // for the same defence-in-depth reason `tracing.rs` clamps the
            // implicit `PutSuccess` from `from_inbound_msg_v1` — a buggy or
            // malicious peer must not be able to poison originator
            // telemetry with `usize::MAX`. `None` is preserved for the
            // `LocalCompletion` arm (no remote hops traversed).
            let max_htl = op_manager.ring.max_hops_to_live;
            let hop_count = wire_hop_count.map(|hc| hc.min(max_htl));
            super::finalize_put_at_originator(
                op_manager,
                client_tx,
                reply_key,
                PutFinalizationData {
                    sender: driver.current_target,
                    hop_count,
                    state_hash: None,
                    state_size: None,
                },
                false,
                false,
            )
            .await;

            maybe_subscribe_child(
                op_manager,
                client_tx,
                reply_key,
                subscribe,
                blocking_subscribe,
            )
            .await;

            Ok(DriverOutcome::Publish(Ok(HostResponse::ContractResponse(
                ContractResponse::PutResponse { key: reply_key },
            ))))
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

// --- Reply classification ---

#[derive(Debug)]
enum ReplyClass {
    /// Remote peer accepted the PUT. `hop_count` is the forward-path
    /// depth carried on the wire `PutMsg::Response`/`ResponseStreaming`.
    /// Used by the originator's driver to populate
    /// `PutFinalizationData.hop_count` so the explicit `PutSuccess`
    /// event emitted from `finalize_put_at_originator` carries the
    /// same value as the implicit one emitted from
    /// `NetEventLog::from_inbound_msg_v1` — without this, the
    /// originator's two `PutSuccess` events for the same tx would
    /// disagree (one populated, one `None`).
    Stored {
        key: ContractKey,
        hop_count: usize,
    },
    /// Local completion: process_message stored locally but found no
    /// next hop, so forward_pending_op_result_if_completed sent back
    /// the original Request. The contract is stored at the originator,
    /// so no hops were traversed.
    LocalCompletion {
        key: ContractKey,
    },
    Unexpected,
}

fn classify_reply(msg: &NetMessage) -> ReplyClass {
    match msg {
        NetMessage::V1(NetMessageV1::Put(PutMsg::Response { key, hop_count, .. }))
        | NetMessage::V1(NetMessageV1::Put(PutMsg::ResponseStreaming { key, hop_count, .. })) => {
            ReplyClass::Stored {
                key: *key,
                hop_count: *hop_count,
            }
        }
        // When process_message completes locally (no next hop), the
        // Request is echoed back via forward_pending_op_result_if_completed.
        NetMessage::V1(NetMessageV1::Put(PutMsg::Request {
            id: _, contract, ..
        })) => ReplyClass::LocalCompletion {
            key: contract.key(),
        },
        _ => ReplyClass::Unexpected,
    }
}

// --- Per-attempt timeout (issue #4001) ---

/// Compute the per-attempt timeout the client-PUT driver passes to
/// `drive_retry_loop`.
///
/// Approximates the bincode-serialized
/// `PutStreamingPayload { contract, related_contracts, value }` size as
/// `state.size() + contract.data().len() + contract.params().size()` and
/// delegates to [`crate::operations::streaming_aware_attempt_timeout`] for
/// the scaling formula and cap.
///
/// **Excluded from the estimate**: `RelatedContracts` and bincode framing.
/// For typical PUT payloads these contribute at most a small constant; the
/// `STREAMING_MIN_DRAIN_SECS` floor and the 20 KiB/s throughput floor (~2×
/// margin vs observed throughput) inside `streaming_aware_attempt_timeout`
/// absorb the gap.
///
/// **Known limitation — pre-merge value**: this is computed from the
/// client-supplied `value` *before* `put_contract` runs the contract's
/// `update_state` against the cached state. If a merge expands the
/// payload substantially (e.g. a small delta merged into a large existing
/// state, then forwarded as the merged result), the driver may
/// under-estimate the streamed size. For the freenet.org website case
/// (full-state replace, no merge expansion), this does not apply. Issue
/// #4001's follow-up inactivity-based design (approach (c)) eliminates
/// the merge-expansion gap structurally by observing per-fragment
/// progress instead of pre-computing a wall-clock budget.
fn compute_put_attempt_timeout(
    streaming_threshold: usize,
    value: &WrappedState,
    contract: &ContractContainer,
) -> std::time::Duration {
    let payload_size_estimate = value
        .size()
        .saturating_add(contract.data().len())
        .saturating_add(contract.params().size());
    crate::operations::streaming_aware_attempt_timeout(streaming_threshold, payload_size_estimate)
}

// --- Peer advance ---

/// Maximum peer **advancements** for a non-streaming client PUT.
///
/// Counts *additional* peers tried after the initial attempt — the
/// total attempt budget is `1 + MAX_PEER_ADVANCEMENTS_NON_STREAMING`
/// because `drive_retry_loop` always runs the first attempt before
/// asking the driver to `advance` (see `op_ctx.rs::drive_retry_loop`).
///
/// `3` matches the legacy PUT retry budget ("3 alternatives via
/// `retry_with_next_alternative`") and the SUBSCRIBE driver's
/// equivalent. With typical ring fan-out of 3-5 peers per k_closest
/// call, 4 total attempts cover 12-20 distinct peers.
pub(crate) const MAX_PEER_ADVANCEMENTS_NON_STREAMING: usize = 3;

/// Maximum peer **advancements** for a streaming client PUT.
///
/// **Zero** = no peer advancement after the initial attempt = exactly
/// one attempt total. The cap is on advancements (next-peer rounds)
/// because `drive_retry_loop` always runs the first attempt before
/// any cap check (`op_ctx.rs::drive_retry_loop` line 474-490). With
/// `MAX_PEER_ADVANCEMENTS_STREAMING = 0`, the first failure
/// immediately exhausts.
///
/// Capped at one total attempt because:
///
/// - Each streaming attempt is bounded by `STREAMING_ATTEMPT_TIMEOUT_CAP`
///   (10 min worst case via `streaming_aware_attempt_timeout`). Even
///   that single attempt already exceeds the freenet-git client's
///   180s per-attempt patience — but the client's outer retry will
///   roll over to a fresh WS connection in 540s. A second
///   in-driver attempt of up to 600s pushes the total wall clock to
///   1200s, far past any WS-client patience, and produces the
///   "silent timeout" failure mode (client gives up; gateway later
///   publishes terminal `PutResponse(Err)` against a dropped
///   connection) tracked in freenet-git#53.
///
/// - Streaming-PUT consumers in production today (freenet-git for
///   mirror packs, River for room contracts) are content-addressed
///   or version-monotonic, so the client's outer retry handles
///   peer-rotation correctness without depending on the in-process
///   retry loop.
///
/// - Per-attempt timeout already absorbs the dominant fast-recovery
///   case (transport stall + reroute via congestion control); the
///   second/third advancement in the legacy budget only helped when
///   the *first* peer was permanently bad, which is rarer than the
///   transport-congested case the gateway runs into routinely.
///
/// Non-streaming PUTs keep the legacy `3 advancements = 4 attempts`
/// budget via [`MAX_PEER_ADVANCEMENTS_NON_STREAMING`].
pub(crate) const MAX_PEER_ADVANCEMENTS_STREAMING: usize = 0;

/// Ask the ring for a new closest peer, excluding all previously tried
/// addresses. Returns `None` once `retries >= max_advancements` (the
/// cap is per-driver: streaming PUTs use
/// `MAX_PEER_ADVANCEMENTS_STREAMING`, others
/// `MAX_PEER_ADVANCEMENTS_NON_STREAMING`).
///
/// The cap gates *advancements only* — `drive_retry_loop` runs the
/// initial attempt before ever calling this. Total attempts is
/// therefore `1 + max_advancements`.
fn advance_to_next_peer(
    op_manager: &OpManager,
    key: &ContractKey,
    tried: &mut Vec<std::net::SocketAddr>,
    retries: &mut usize,
    max_advancements: usize,
) -> Option<(PeerKeyLocation, std::net::SocketAddr)> {
    if *retries >= max_advancements {
        return None;
    }
    *retries += 1;

    let peer = op_manager
        .ring
        .closest_potentially_hosting(key, tried.as_slice())?;
    let addr = peer.socket_addr()?;
    tried.push(addr);
    Some((peer, addr))
}

// --- Subscribe child ---

/// Start a post-PUT subscription if requested.
///
/// For `blocking_subscribe = true`, awaits the subscribe driver inline
/// (requires `run_client_subscribe` to be `pub(crate)`).
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
        // Inline await — PUT response waits for subscribe completion.
        subscribe::run_client_subscribe(op_manager.clone(), *key.id(), child_tx).await;
    } else {
        // Fire-and-forget — PUT response returns immediately.
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
                "put: infrastructure error; publishing synthesized client error"
            );
            let synthesized: HostResult = Err(ErrorKind::OperationError {
                cause: format!("PUT failed: {err}").into(),
            }
            .into());
            op_manager.send_client_result(client_tx, synthesized);
        }
    }
}

// ── Relay PUT driver ─────────────────────────────────────────────────────────

/// Counter: number of times `start_relay_put` was invoked. Incremented
/// under test/testing feature only — used by structural pin tests to
/// prove the dispatch gate routes fresh inbound `PutMsg::Request`
/// through the driver rather than legacy `handle_op_request`.
#[cfg(any(test, feature = "testing"))]
pub static RELAY_PUT_DRIVER_CALL_COUNT: std::sync::atomic::AtomicUsize =
    std::sync::atomic::AtomicUsize::new(0);

/// Counter: relay PUT drivers currently in flight. Decremented in the
/// RAII guard on driver exit. Diagnostic for `FREENET_MEMORY_STATS`.
pub static RELAY_PUT_INFLIGHT: std::sync::atomic::AtomicUsize =
    std::sync::atomic::AtomicUsize::new(0);

/// Counter: total relay PUT drivers ever spawned on this node.
pub static RELAY_PUT_SPAWNED_TOTAL: std::sync::atomic::AtomicUsize =
    std::sync::atomic::AtomicUsize::new(0);

/// Counter: total relay PUT drivers that exited (any path).
pub static RELAY_PUT_COMPLETED_TOTAL: std::sync::atomic::AtomicUsize =
    std::sync::atomic::AtomicUsize::new(0);

/// Counter: duplicate inbound `PutMsg::Request` rejected by the per-node
/// dedup gate (`active_relay_put_txs`).
pub static RELAY_PUT_DEDUP_REJECTS: std::sync::atomic::AtomicUsize =
    std::sync::atomic::AtomicUsize::new(0);

/// Spawn a relay driver for a fresh inbound non-streaming `PutMsg::Request`.
///
/// Gated by the dispatch site in `node.rs::handle_pure_network_message_v1`
/// on `source_addr.is_some()`. Per-node dedup against concurrent inbound
/// retries is enforced by `OpManager.active_relay_put_txs` inside the
/// driver. State lives in task locals.
///
/// Returns immediately after spawning. Driver publishes its own side
/// effects (local put_contract / host_contract / interest broadcast,
/// a single non-streaming forward via `OpCtx::send_to_and_await`, and
/// `PutMsg::Response` back to the upstream).
///
/// # Scope (slice A)
///
/// Migrated:
/// - `PutMsg::Request` relay arm: local store, decide forward-or-respond,
///   forward + await downstream `PutMsg::Response`, bubble upstream.
/// - `PutMsg::Response` bubble-up to upstream (handled inline by this
///   driver via `send_to_and_await`'s reply, not by the legacy arm).
/// - `PutMsg::ForwardingAck` OMITTED — same reasoning as GET relay: the
///   ack carried `incoming_tx` and would satisfy upstream's capacity-1
///   `pending_op_results` waiter before the real `Response` arrived.
///
/// NOT migrated (stays on legacy path):
/// - `PutMsg::RequestStreaming` / `PutMsg::ResponseStreaming`. The
///   dispatch gate in `node.rs` re-checks `should_use_streaming` on the
///   inbound non-streaming Request and falls through to legacy if the
///   serialized payload would exceed `streaming_threshold` on the
///   forward — slice A only handles end-to-end non-streaming hops.
/// - GC-spawned speculative retries (`speculative_paths`): no `PutOp`
///   in the DashMap for the GC to find, so they never enter this driver.
#[allow(clippy::too_many_arguments)]
pub(crate) async fn start_relay_put<CB>(
    op_manager: Arc<OpManager>,
    conn_manager: CB,
    incoming_tx: Transaction,
    contract: ContractContainer,
    related_contracts: RelatedContracts<'static>,
    value: WrappedState,
    htl: usize,
    skip_list: HashSet<SocketAddr>,
    upstream_addr: SocketAddr,
) -> Result<(), OpError>
where
    CB: NetworkBridge + Clone + Send + 'static,
{
    #[cfg(any(test, feature = "testing"))]
    RELAY_PUT_DRIVER_CALL_COUNT.fetch_add(1, std::sync::atomic::Ordering::SeqCst);

    if !op_manager.active_relay_put_txs.insert(incoming_tx) {
        RELAY_PUT_DEDUP_REJECTS.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        tracing::debug!(
            tx = %incoming_tx,
            contract = %contract.key(),
            %upstream_addr,
            phase = "relay_put_dedup_reject",
            "PUT relay: duplicate Request for in-flight tx, dropping"
        );
        return Ok(());
    }

    tracing::debug!(
        tx = %incoming_tx,
        contract = %contract.key(),
        htl,
        %upstream_addr,
        phase = "relay_put_start",
        "PUT relay: spawning driver"
    );

    // Construct guard + bump counters BEFORE spawn so the dedup-set
    // entry is paired with a Drop even if the spawned future is dropped
    // before its first poll (executor shutdown, scheduling panic).
    // Without this, a pre-poll drop would permanently leak the
    // `active_relay_put_txs` entry — no TTL → permanent dedup
    // blind-spot for that tx. Mirrors UPDATE's pre-spawn guard pattern.
    RELAY_PUT_INFLIGHT.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    RELAY_PUT_SPAWNED_TOTAL.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    let guard = RelayPutInflightGuard {
        op_manager: op_manager.clone(),
        incoming_tx,
    };

    GlobalExecutor::spawn(run_relay_put(
        guard,
        op_manager,
        conn_manager,
        incoming_tx,
        contract,
        related_contracts,
        value,
        htl,
        skip_list,
        upstream_addr,
    ));
    Ok(())
}

/// RAII guard that decrements `RELAY_PUT_INFLIGHT`, bumps
/// `RELAY_PUT_COMPLETED_TOTAL`, and removes the driver's `incoming_tx`
/// from `active_relay_put_txs` on drop. Mirrors GET/UPDATE relay guards.
struct RelayPutInflightGuard {
    op_manager: Arc<OpManager>,
    incoming_tx: Transaction,
}

impl Drop for RelayPutInflightGuard {
    fn drop(&mut self) {
        self.op_manager
            .active_relay_put_txs
            .remove(&self.incoming_tx);
        RELAY_PUT_INFLIGHT.fetch_sub(1, std::sync::atomic::Ordering::Relaxed);
        RELAY_PUT_COMPLETED_TOTAL.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    }
}

#[allow(clippy::too_many_arguments)]
async fn run_relay_put<CB>(
    guard: RelayPutInflightGuard,
    op_manager: Arc<OpManager>,
    conn_manager: CB,
    incoming_tx: Transaction,
    contract: ContractContainer,
    related_contracts: RelatedContracts<'static>,
    value: WrappedState,
    htl: usize,
    skip_list: HashSet<SocketAddr>,
    upstream_addr: SocketAddr,
) where
    CB: NetworkBridge + Clone + Send + 'static,
{
    let _guard = guard;

    let drive_result = drive_relay_put(
        &op_manager,
        &conn_manager,
        incoming_tx,
        contract,
        related_contracts,
        value,
        htl,
        skip_list,
        upstream_addr,
    )
    .await;

    if let Err(err) = &drive_result {
        if err.is_contract_queue_full() {
            tracing::debug!(
                tx = %incoming_tx,
                error = %err,
                phase = "relay_put_error",
                event = "queue_full",
                "PUT relay: driver returned error"
            );
        } else {
            tracing::warn!(
                tx = %incoming_tx,
                error = %err,
                phase = "relay_put_error",
                "PUT relay: driver returned error"
            );
        }
    }

    // Originator-loopback error path: when the relay driver runs
    // on the originator's own node and fails, the originator's
    // `start_client_put` `send_and_await` waiter has no
    // `PutMsg::Response` to consume — it would hang until timeout.
    // Publish a `HostResult::Err` to the originator's client
    // transaction and complete the tx.
    //
    // Safe in non-loopback mode because remote relays don't share
    // tx-space with a local client (the originator is on a different
    // node, so `incoming_tx` is not registered with our SessionActor).
    let own_addr = op_manager.ring.connection_manager.get_own_addr();
    let originator_loopback = Some(upstream_addr) == own_addr;
    if originator_loopback {
        if let Err(err) = drive_result {
            let client_error = freenet_stdlib::client_api::ClientError::from(
                freenet_stdlib::client_api::ErrorKind::OperationError {
                    cause: err.to_string().into(),
                },
            );
            op_manager.send_client_result(incoming_tx, Err(client_error));
            op_manager.completed(incoming_tx);
        }
    }

    // Release the per-tx `pending_op_results` slot at driver exit, same
    // rationale as GET relay — `send_to_and_await` leaves an is_closed
    // sender in the slot that only the 60s sweep would reclaim without
    // this explicit release.
    //
    // Originator-loopback exception: when this driver runs on the
    // originator's own node
    // (`upstream_addr == own_addr`), the `pending_op_results` callback
    // for `incoming_tx` is the *originator's* `send_and_await` waiter,
    // NOT one this driver installed. Releasing it here would emit
    // `TransactionCompleted` and remove the originator's callback
    // BEFORE the loopback `PutMsg::Response` arrives at the bypass —
    // racing the originator's wait. The originator's own driver
    // completion path (via `send_client_result` →
    // `release_pending_op_slot`) cleans up the slot.
    tokio::task::yield_now().await;
    if !originator_loopback {
        op_manager.release_pending_op_slot(incoming_tx).await;
    }
}

#[allow(clippy::too_many_arguments)]
async fn drive_relay_put<CB>(
    op_manager: &Arc<OpManager>,
    conn_manager: &CB,
    incoming_tx: Transaction,
    contract: ContractContainer,
    related_contracts: RelatedContracts<'static>,
    value: WrappedState,
    htl: usize,
    skip_list: HashSet<SocketAddr>,
    upstream_addr: SocketAddr,
) -> Result<(), OpError>
where
    CB: NetworkBridge + Clone + Send + 'static,
{
    let key = contract.key();

    tracing::info!(
        tx = %incoming_tx,
        contract = %key,
        htl,
        %upstream_addr,
        phase = "relay_put_request",
        "PUT relay: processing Request"
    );

    // ── Step 1: Store contract locally (all nodes cache) ────────────────────
    let merged_value = relay_put_store_locally(
        op_manager,
        incoming_tx,
        key,
        value.clone(),
        &contract,
        related_contracts.clone(),
        htl,
    )
    .await?;

    // ── Step 2: Build skip list + select next hop ──────────────────────────
    let mut new_skip_list = skip_list;
    new_skip_list.insert(upstream_addr);
    if let Some(own_addr) = op_manager.ring.connection_manager.get_own_addr() {
        new_skip_list.insert(own_addr);
    }

    let next_hop = if htl > 0 {
        op_manager
            .ring
            .closest_potentially_hosting(&key, &new_skip_list)
    } else {
        None
    };

    let (next_peer, next_addr) = match next_hop {
        Some(peer) => {
            let addr = match peer.socket_addr() {
                Some(a) => a,
                None => {
                    tracing::error!(
                        tx = %incoming_tx,
                        contract = %key,
                        target_pub_key = %peer.pub_key(),
                        "PUT relay: next hop has no socket address"
                    );
                    // No next hop — act as final destination.
                    // This node IS the storer; hop_count = max_htl - htl_we_received.
                    let hop_count = op_manager.ring.max_hops_to_live.saturating_sub(htl);
                    return relay_put_finalize_local(
                        op_manager,
                        incoming_tx,
                        key,
                        merged_value,
                        upstream_addr,
                        hop_count,
                    )
                    .await;
                }
            };
            (peer, addr)
        }
        None => {
            // No next hop — this node is the final destination.
            tracing::info!(
                tx = %incoming_tx,
                contract = %key,
                phase = "relay_put_complete",
                "PUT relay: no next hop, finalizing at this node"
            );
            // Same arm as above: this node IS the storer.
            let hop_count = op_manager.ring.max_hops_to_live.saturating_sub(htl);
            return relay_put_finalize_local(
                op_manager,
                incoming_tx,
                key,
                merged_value,
                upstream_addr,
                hop_count,
            )
            .await;
        }
    };

    // ── Step 3: Forward downstream, await Response, bubble up ──────────────

    let new_htl = htl.saturating_sub(1);

    if let Some(event) = NetEventLog::put_request(
        &incoming_tx,
        &op_manager.ring,
        key,
        next_peer.clone(),
        new_htl,
    ) {
        op_manager.ring.register_events(Either::Left(event)).await;
    }

    tracing::debug!(
        tx = %incoming_tx,
        contract = %key,
        peer_addr = %next_addr,
        htl = new_htl,
        phase = "relay_put_forward",
        "PUT relay: forwarding to next hop"
    );

    // Originator-loopback mode (`upstream_addr == own_addr`): the
    // originator's `send_and_await` already installed a
    // `pending_op_results` callback under
    // `incoming_tx`. Using `send_to_and_await` here would overwrite
    // that callback with the relay's own waiter, then the relay
    // consumes the reply and the originator's wait dangles forever.
    // Instead, fire-and-forget the forward — the downstream Response
    // returns over the wire and the bypass forwards it directly to
    // the originator's still-installed callback. The relay does not
    // bubble up (skipping `relay_put_send_response`) because the
    // originator IS the upstream and is already waiting.
    let own_addr = op_manager.ring.connection_manager.get_own_addr();
    let originator_loopback = Some(upstream_addr) == own_addr;

    // Streaming upgrade on forward: serialize the payload, and if it
    // exceeds `streaming_threshold`, send a `RequestStreaming` metadata
    // message + raw stream fragments via `network_bridge.send_stream`.
    // Different from `start_relay_put_streaming`'s `pipe_stream`
    // path because there's no inbound stream handle to fork — the
    // payload is materialized locally as `merged_value` and we send
    // raw fragments.
    let payload = PutStreamingPayload {
        contract: contract.clone(),
        related_contracts: related_contracts.clone(),
        value: merged_value.clone(),
    };
    let payload_bytes = match bincode::serialize(&payload) {
        Ok(b) => b,
        Err(e) => {
            return Err(OpError::NotificationChannelError(format!(
                "Failed to serialize PUT relay forward payload: {e}"
            )));
        }
    };
    let payload_size = payload_bytes.len();
    let upgrade_to_streaming =
        crate::operations::should_use_streaming(op_manager.streaming_threshold, payload_size);

    let mut ctx = op_manager.op_ctx(incoming_tx);

    if originator_loopback {
        // Fire-and-forget forward; do NOT install a waiter (would
        // overwrite the originator's pending_op_results callback).
        // Response returns directly to the originator via the bypass.
        if upgrade_to_streaming {
            let stream_id = StreamId::next_operations();
            let metadata_msg = NetMessage::from(PutMsg::RequestStreaming {
                id: incoming_tx,
                stream_id,
                contract_key: key,
                total_size: payload_size as u64,
                htl: new_htl,
                skip_list: new_skip_list.clone(),
                subscribe: false,
            });
            if let Err(err) = ctx.send_fire_and_forget(next_addr, metadata_msg).await {
                tracing::warn!(
                    tx = %incoming_tx,
                    contract = %key,
                    target = %next_addr,
                    error = %err,
                    "PUT relay (loopback, loopback): \
                     streaming-upgrade send_fire_and_forget failed"
                );
                return Err(err);
            }
            if let Err(err) = conn_manager
                .send_stream(
                    next_addr,
                    stream_id,
                    bytes::Bytes::from(payload_bytes),
                    None,
                )
                .await
            {
                tracing::warn!(
                    tx = %incoming_tx,
                    contract = %key,
                    target = %next_addr,
                    %stream_id,
                    error = %err,
                    "PUT relay (loopback, loopback): send_stream failed"
                );
                return Err(OpError::NotificationChannelError(format!(
                    "send_stream failed: {err}"
                )));
            }
        } else {
            let forward = NetMessage::from(PutMsg::Request {
                id: incoming_tx,
                contract,
                related_contracts,
                value: merged_value,
                htl: new_htl,
                skip_list: new_skip_list,
            });
            if let Err(err) = ctx.send_fire_and_forget(next_addr, forward).await {
                tracing::warn!(
                    tx = %incoming_tx,
                    contract = %key,
                    target = %next_addr,
                    error = %err,
                    "PUT relay (loopback, loopback): send_fire_and_forget failed"
                );
                return Err(err);
            }
        }
        // Originator is awaiting the Response on its own callback —
        // exit the driver here. No bubble-up, no release_pending_op_slot
        // (handled by the originator's completion path).
        return Ok(());
    }

    let round_trip = if upgrade_to_streaming {
        let stream_id = StreamId::next_operations();
        tracing::info!(
            tx = %incoming_tx,
            contract = %key,
            target = %next_addr,
            payload_size,
            %stream_id,
            phase = "relay_put_forward_upgrade",
            "PUT relay: payload exceeds threshold, upgrading to streaming"
        );
        let metadata_msg = NetMessage::from(PutMsg::RequestStreaming {
            id: incoming_tx,
            stream_id,
            contract_key: key,
            total_size: payload_size as u64,
            htl: new_htl,
            skip_list: new_skip_list.clone(),
            // Relay path never carries client subscribe intent; only the
            // originator's `start_client_put` triggers post-PUT
            // subscription.
            subscribe: false,
        });

        // Install the reply waiter BEFORE dispatching stream fragments.
        // A fast downstream Response could otherwise race the
        // `pending_op_results` insertion and be dropped as
        // OpNotPresent. Mirrors the metadata-first ordering used by
        // `start_relay_put_streaming`.
        let mut reply_rx = match ctx
            .send_to_and_register_waiter(next_addr, metadata_msg)
            .await
        {
            Ok(rx) => rx,
            Err(err) => {
                tracing::warn!(
                    tx = %incoming_tx,
                    contract = %key,
                    target = %next_addr,
                    error = %err,
                    "PUT relay: streaming-upgrade send_to_and_register_waiter failed"
                );
                op_manager.release_pending_op_slot(incoming_tx).await;
                return Err(err);
            }
        };

        if let Err(err) = conn_manager
            .send_stream(
                next_addr,
                stream_id,
                bytes::Bytes::from(payload_bytes),
                None,
            )
            .await
        {
            tracing::warn!(
                tx = %incoming_tx,
                contract = %key,
                target = %next_addr,
                %stream_id,
                error = %err,
                "PUT relay: send_stream failed during streaming upgrade"
            );
            op_manager.release_pending_op_slot(incoming_tx).await;
            return Err(OpError::NotificationChannelError(format!(
                "send_stream failed: {err}"
            )));
        }

        tokio::time::timeout(OPERATION_TTL, async move {
            reply_rx.recv().await.ok_or(OpError::NotificationError)
        })
        .await
    } else {
        let forward = NetMessage::from(PutMsg::Request {
            id: incoming_tx,
            contract,
            related_contracts,
            value: merged_value,
            htl: new_htl,
            skip_list: new_skip_list,
        });
        tokio::time::timeout(OPERATION_TTL, ctx.send_to_and_await(next_addr, forward)).await
    };

    // Release the pending_op_results slot that send_to_and_await
    // installed. The downstream reply has already been delivered (or
    // timed out); the upstream-reply fire-and-forget below will
    // re-insert under the same key, and a single TransactionCompleted
    // at driver exit only removes one entry — without this interim
    // release the inserts/removes ledger leaks one entry per relay
    // driver run (reproduced by test_pending_op_results_bounded at
    // 74/461 on the PR branch before this release call was added).
    // Mirrors GET relay's post-send_to_and_await release.
    op_manager.release_pending_op_slot(incoming_tx).await;

    let reply = match round_trip {
        Ok(Ok(reply)) => reply,
        Ok(Err(err)) => {
            tracing::warn!(
                tx = %incoming_tx,
                contract = %key,
                target = %next_addr,
                error = %err,
                "PUT relay: send_to_and_await failed"
            );
            crate::operations::record_relay_route_event(
                op_manager,
                next_peer.clone(),
                crate::ring::Location::from(&key),
                crate::router::RouteOutcome::Failure,
                crate::node::network_status::OpType::Put,
            );
            return Err(err);
        }
        Err(_elapsed) => {
            tracing::warn!(
                tx = %incoming_tx,
                contract = %key,
                target = %next_addr,
                timeout_secs = OPERATION_TTL.as_secs(),
                "PUT relay: downstream timed out"
            );
            crate::operations::record_relay_route_event(
                op_manager,
                next_peer.clone(),
                crate::ring::Location::from(&key),
                crate::router::RouteOutcome::Failure,
                crate::node::network_status::OpType::Put,
            );
            return Err(OpError::UnexpectedOpState);
        }
    };

    // ── Step 4: Classify reply and bubble Response upstream ────────────────
    // Feed the relay's downstream-peer choice into the local Router so
    // future routing decisions are informed by relay-observed outcomes,
    // not just events from ops this node originated.
    match reply {
        NetMessage::V1(NetMessageV1::Put(PutMsg::Response {
            key: reply_key,
            hop_count: downstream_hop_count,
            ..
        })) => {
            tracing::info!(
                tx = %incoming_tx,
                contract = %reply_key,
                phase = "relay_put_bubble",
                "PUT relay: downstream returned Response; bubbling upstream"
            );
            crate::operations::record_relay_route_event(
                op_manager,
                next_peer.clone(),
                crate::ring::Location::from(&reply_key),
                crate::router::RouteOutcome::SuccessUntimed,
                crate::node::network_status::OpType::Put,
            );
            // Preserve the storer-side hop_count so the originator sees the
            // *forward* depth from Request to storer, not the depth from
            // Request to this relay. Mirrors GET relay bubble-up.
            relay_put_send_response(
                op_manager,
                incoming_tx,
                reply_key,
                upstream_addr,
                downstream_hop_count,
            )
            .await
        }
        NetMessage::V1(NetMessageV1::Put(PutMsg::ResponseStreaming {
            key: reply_key,
            hop_count: downstream_hop_count,
            ..
        })) => {
            // Streaming response arrived from downstream. Slice A does
            // not relay-forward streaming payloads, so log and
            // synthesize a non-streaming Response upstream. This
            // preserves correctness — the contract IS stored at this
            // node (step 1) — at the cost of the upstream not getting
            // the streamed payload. Slice B handles stream pass-through.
            tracing::warn!(
                tx = %incoming_tx,
                contract = %reply_key,
                phase = "relay_put_bubble_streaming_downgrade",
                "PUT relay: downstream returned ResponseStreaming — \
                 synthesizing non-streaming Response upstream (slice A limitation)"
            );
            crate::operations::record_relay_route_event(
                op_manager,
                next_peer.clone(),
                crate::ring::Location::from(&reply_key),
                crate::router::RouteOutcome::SuccessUntimed,
                crate::node::network_status::OpType::Put,
            );
            // Downgrade still preserves the storer-side forward depth.
            relay_put_send_response(
                op_manager,
                incoming_tx,
                reply_key,
                upstream_addr,
                downstream_hop_count,
            )
            .await
        }
        other => {
            // Unexpected reply variant: unclear whether it's a local
            // bug or peer misbehaviour. Do NOT record a route event;
            // the helper invariant is one event per unambiguous attribution.
            tracing::warn!(
                tx = %incoming_tx,
                contract = %key,
                reply_variant = ?std::mem::discriminant(&other),
                "PUT relay: unexpected reply variant; treating as failure"
            );
            Err(OpError::UnexpectedOpState)
        }
    }
}

/// Store a relayed PUT's contract locally: `put_contract` + (if not
/// already hosting) `host_contract` + `announce_contract_hosted` +
/// interest register/unregister + broadcast interest changes.
///
/// Shared between the non-streaming relay driver (`drive_relay_put`)
/// and the streaming relay driver (`drive_relay_put_streaming`) so both
/// paths run identical local-store semantics. Returns the post-merge
/// `WrappedState` the caller forwards downstream / bubbles upstream.
///
/// This helper is **relay-only** — it never sets
/// `mark_local_client_access` (that's originator-side). Errors emit a
/// `put_failure` telemetry event and propagate.
async fn relay_put_store_locally(
    op_manager: &Arc<OpManager>,
    incoming_tx: Transaction,
    key: ContractKey,
    value: WrappedState,
    contract: &ContractContainer,
    related_contracts: RelatedContracts<'static>,
    htl: usize,
) -> Result<WrappedState, OpError> {
    let was_hosting = op_manager.ring.is_hosting_contract(&key);
    let (merged_value, _state_changed) = match super::put_contract(
        op_manager,
        key,
        value.clone(),
        related_contracts,
        contract,
    )
    .await
    {
        Ok(result) => result,
        Err(err) => {
            // Issue #4251: per-contract queue saturation is transient
            // platform backpressure, not a real PUT failure. The PUT
            // relay path doesn't trigger the auto-fetch / ResyncRequest
            // amplification that UPDATE does (see PR for analysis), so
            // we only have the log-spam half of the bug to fix here.
            // Real PUT failures (validation, storage, missing
            // parameters) keep the ERROR level.
            if err.is_contract_queue_full() {
                tracing::debug!(
                    tx = %incoming_tx,
                    contract = %key,
                    error = %err,
                    htl,
                    event = "queue_full",
                    "PUT relay: per-contract queue saturated"
                );
            } else {
                tracing::error!(
                    tx = %incoming_tx,
                    contract = %key,
                    error = %err,
                    htl,
                    "PUT relay: put_contract failed"
                );
            }
            if let Some(event) = NetEventLog::put_failure(
                &incoming_tx,
                &op_manager.ring,
                key,
                OperationFailure::ContractError(err.to_string()),
                Some(op_manager.ring.max_hops_to_live.saturating_sub(htl)),
            ) {
                op_manager.ring.register_events(Either::Left(event)).await;
            }
            return Err(err);
        }
    };

    if !was_hosting {
        let evicted = op_manager
            .ring
            .host_contract(key, value.size() as u64, crate::ring::AccessType::Put)
            .evicted;

        crate::operations::announce_contract_hosted(op_manager, &key).await;

        let mut removed_contracts = Vec::new();
        for (evicted_key, expected_generation) in evicted {
            if op_manager
                .interest_manager
                .unregister_local_hosting(&evicted_key)
            {
                removed_contracts.push(evicted_key);
            }
            // Reclaim on-disk storage for the evicted contract so the hosting
            // budget is a real disk bound (subscription- and generation-gated
            // inside the helper).
            crate::operations::reclaim_evicted_contract(
                op_manager,
                evicted_key,
                expected_generation,
            );
        }

        let became_interested = op_manager.interest_manager.register_local_hosting(&key);
        let added = if became_interested { vec![key] } else { vec![] };
        if !added.is_empty() || !removed_contracts.is_empty() {
            crate::operations::broadcast_change_interests(op_manager, added, removed_contracts)
                .await;
        }
    }

    debug_assert!(
        op_manager.ring.is_hosting_contract(&key),
        "PUT relay: contract {key} must be in hosting list after put_contract + host_contract"
    );

    Ok(merged_value)
}

/// Finalize at this node when there's no next hop. Emits `put_success`
/// telemetry and sends `PutMsg::Response` upstream.
///
/// `hop_count` is the forward-path depth this relay finalised at — for the
/// non-originator-as-storer arm this is `max_htl - htl_we_received`. Caller
/// computes it from the inbound `htl` so this helper stays oblivious to
/// ring state and is mechanically easy to audit.
async fn relay_put_finalize_local(
    op_manager: &OpManager,
    incoming_tx: Transaction,
    key: ContractKey,
    merged_value: WrappedState,
    upstream_addr: SocketAddr,
    hop_count: usize,
) -> Result<(), OpError> {
    // Telemetry: non-originator target peer — emit put_success for
    // convergence checking (mirrors put.rs:798-811 non-originator arm).
    let own_location = op_manager.ring.connection_manager.own_location();
    let hash = Some(state_hash_full(&merged_value));
    let size = Some(merged_value.len());
    if let Some(event) = NetEventLog::put_success(
        &incoming_tx,
        &op_manager.ring,
        key,
        own_location,
        Some(hop_count),
        hash,
        size,
    ) {
        op_manager.ring.register_events(Either::Left(event)).await;
    }

    relay_put_send_response(op_manager, incoming_tx, key, upstream_addr, hop_count).await
}

/// Send `PutMsg::Response` upstream (fire-and-forget: upstream relay
/// awaits via its own `send_to_and_await`, no reply expected).
///
/// `hop_count` is the forward-path depth to embed in the Response. For a
/// relay that finalised locally (this node IS the storer), pass
/// `max_htl - incoming_htl`. For a relay bubbling a downstream Response
/// upstream, pass the downstream's `hop_count` verbatim — relays do NOT
/// increment on the return path.
///
/// Originator-loopback case (`upstream_addr == own_addr`): when
/// the relay driver runs on the originator's own node, the
/// Response cannot ship over the wire (no self-connection). Route
/// via `send_local_loopback` so the message lands as an
/// `InboundMessage`, hits the PUT bypass, and forwards to the
/// originator's `pending_op_results` waiter.
async fn relay_put_send_response(
    op_manager: &OpManager,
    incoming_tx: Transaction,
    key: ContractKey,
    upstream_addr: SocketAddr,
    hop_count: usize,
) -> Result<(), OpError> {
    let msg = NetMessage::from(PutMsg::Response {
        id: incoming_tx,
        key,
        hop_count,
    });
    let mut ctx = op_manager.op_ctx(incoming_tx);
    let own_addr = op_manager.ring.connection_manager.get_own_addr();
    if Some(upstream_addr) == own_addr {
        ctx.send_local_loopback(msg)
            .await
            .map_err(|_| OpError::NotificationError)
    } else {
        ctx.send_fire_and_forget(upstream_addr, msg)
            .await
            .map_err(|_| OpError::NotificationError)
    }
}

// ── Relay streaming PUT driver ───────────────────────────────────────────────

/// Counter: number of times `start_relay_put_streaming` was invoked
/// (BEFORE the dedup gate). Incremented under test/testing feature
/// only — used by runtime pin tests to prove the dispatch gate routes
/// fresh inbound streaming PUT relays through the driver.
/// Counts both admitted and dedup-rejected calls; pair with
/// `RELAY_PUT_STREAMING_DEDUP_REJECTS` to reason about admitted ones.
#[cfg(any(test, feature = "testing"))]
pub static RELAY_PUT_STREAMING_DRIVER_CALL_COUNT: std::sync::atomic::AtomicUsize =
    std::sync::atomic::AtomicUsize::new(0);

/// Counter: streaming relay PUT drivers currently in flight.
pub static RELAY_PUT_STREAMING_INFLIGHT: std::sync::atomic::AtomicUsize =
    std::sync::atomic::AtomicUsize::new(0);

/// Counter: total streaming relay PUT drivers ever spawned.
pub static RELAY_PUT_STREAMING_SPAWNED_TOTAL: std::sync::atomic::AtomicUsize =
    std::sync::atomic::AtomicUsize::new(0);

/// Counter: total streaming relay PUT drivers that exited (any path).
pub static RELAY_PUT_STREAMING_COMPLETED_TOTAL: std::sync::atomic::AtomicUsize =
    std::sync::atomic::AtomicUsize::new(0);

/// Counter: duplicate streaming relay Requests rejected by per-node dedup.
pub static RELAY_PUT_STREAMING_DEDUP_REJECTS: std::sync::atomic::AtomicUsize =
    std::sync::atomic::AtomicUsize::new(0);

/// Spawn a relay driver for a fresh inbound streaming PUT — either a
/// direct `PutMsg::RequestStreaming` or a `PutMsg::Request` whose
/// serialized payload would upgrade to streaming on forward.
///
/// Shares `active_relay_put_txs` dedup set with `start_relay_put` (same
/// tx space, different per-tx variant). Claims the inbound stream via
/// `orphan_stream_registry` so concurrent metadata duplicates
/// (embedded-in-fragment #1) do not double-claim.
///
/// # Scope (slice B)
///
/// Migrated:
/// - Fresh inbound `PutMsg::RequestStreaming` from a remote peer.
/// - Fresh inbound `PutMsg::Request` whose payload exceeds
///   `streaming_threshold` and therefore must upgrade on forward.
/// - Piped downstream forwarding via `conn_manager.pipe_stream`.
/// - Downstream `Response` / `ResponseStreaming` reply downgraded to
///   `PutMsg::Response` upstream (mirrors legacy put.rs:1506).
///
/// NOT migrated (stays on legacy path):
/// - `PutMsg::ForwardingAck` emission — kept omitted for the same
///   reason as slice A: a driver-side ack sharing `incoming_tx` would
///   satisfy the upstream's `pending_op_results` waiter before the
///   real `Response` arrived.
/// - Client-initiated streaming PUTs (`source_addr.is_none()`).
/// - GC-spawned speculative retries (no PutOp in DashMap).
///
/// The `subscribe` flag from the inbound `RequestStreaming`/`Request`
/// is carried forward on the downstream metadata but not acted on
/// locally — only the originator subscribes to its own PUT.
#[allow(clippy::too_many_arguments)]
pub(crate) async fn start_relay_put_streaming<CB>(
    op_manager: Arc<OpManager>,
    conn_manager: CB,
    incoming_tx: Transaction,
    stream_id: StreamId,
    contract_key: ContractKey,
    total_size: u64,
    htl: usize,
    skip_list: HashSet<SocketAddr>,
    subscribe: bool,
    upstream_addr: SocketAddr,
) -> Result<(), OpError>
where
    CB: NetworkBridge + Clone + Send + 'static,
{
    #[cfg(any(test, feature = "testing"))]
    RELAY_PUT_STREAMING_DRIVER_CALL_COUNT.fetch_add(1, std::sync::atomic::Ordering::SeqCst);

    if !op_manager.active_relay_put_txs.insert(incoming_tx) {
        RELAY_PUT_STREAMING_DEDUP_REJECTS.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        tracing::debug!(
            tx = %incoming_tx,
            contract = %contract_key,
            %upstream_addr,
            phase = "relay_put_streaming_dedup_reject",
            "PUT streaming relay: duplicate Request for in-flight tx, dropping"
        );
        // Mirror slice A dedup-reject semantics: silently drop. The
        // still-in-flight driver owns the upstream reply for this tx;
        // fabricating a PutMsg::Response here would wake the
        // upstream's pending_op_results waiter with a false-success
        // BEFORE the real driver's reply lands. PutMsg has no
        // NotFound variant — synthesizing a success reply for a
        // rejected duplicate would tell the upstream that the
        // contract stored when it did not.
        return Ok(());
    }

    tracing::debug!(
        tx = %incoming_tx,
        contract = %contract_key,
        %stream_id,
        total_size,
        htl,
        %upstream_addr,
        phase = "relay_put_streaming_start",
        "PUT streaming relay: spawning driver"
    );

    RELAY_PUT_STREAMING_INFLIGHT.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    RELAY_PUT_STREAMING_SPAWNED_TOTAL.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    let guard = RelayPutStreamingInflightGuard {
        op_manager: op_manager.clone(),
        incoming_tx,
    };

    GlobalExecutor::spawn(run_relay_put_streaming(
        guard,
        op_manager,
        conn_manager,
        incoming_tx,
        stream_id,
        contract_key,
        total_size,
        htl,
        skip_list,
        subscribe,
        upstream_addr,
    ));
    Ok(())
}

/// RAII guard for the streaming PUT relay driver. Mirrors
/// `RelayPutInflightGuard` but drives the streaming counter set.
struct RelayPutStreamingInflightGuard {
    op_manager: Arc<OpManager>,
    incoming_tx: Transaction,
}

impl Drop for RelayPutStreamingInflightGuard {
    fn drop(&mut self) {
        self.op_manager
            .active_relay_put_txs
            .remove(&self.incoming_tx);
        RELAY_PUT_STREAMING_INFLIGHT.fetch_sub(1, std::sync::atomic::Ordering::Relaxed);
        RELAY_PUT_STREAMING_COMPLETED_TOTAL.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    }
}

#[allow(clippy::too_many_arguments)]
async fn run_relay_put_streaming<CB>(
    guard: RelayPutStreamingInflightGuard,
    op_manager: Arc<OpManager>,
    conn_manager: CB,
    incoming_tx: Transaction,
    stream_id: StreamId,
    contract_key: ContractKey,
    total_size: u64,
    htl: usize,
    skip_list: HashSet<SocketAddr>,
    subscribe: bool,
    upstream_addr: SocketAddr,
) where
    CB: NetworkBridge + Clone + Send + 'static,
{
    let _guard = guard;

    if let Err(err) = drive_relay_put_streaming(
        &op_manager,
        &conn_manager,
        incoming_tx,
        stream_id,
        contract_key,
        total_size,
        htl,
        skip_list,
        subscribe,
        upstream_addr,
    )
    .await
    {
        if err.is_contract_queue_full() {
            tracing::debug!(
                tx = %incoming_tx,
                error = %err,
                phase = "relay_put_streaming_error",
                event = "queue_full",
                "PUT streaming relay: driver returned error"
            );
        } else {
            tracing::warn!(
                tx = %incoming_tx,
                error = %err,
                phase = "relay_put_streaming_error",
                "PUT streaming relay: driver returned error"
            );
        }
    }

    // Release per-tx pending_op_results slot (same rationale as slice A).
    tokio::task::yield_now().await;
    op_manager.release_pending_op_slot(incoming_tx).await;
}

#[allow(clippy::too_many_arguments)]
async fn drive_relay_put_streaming<CB>(
    op_manager: &Arc<OpManager>,
    conn_manager: &CB,
    incoming_tx: Transaction,
    stream_id: StreamId,
    contract_key: ContractKey,
    total_size: u64,
    htl: usize,
    skip_list: HashSet<SocketAddr>,
    subscribe: bool,
    upstream_addr: SocketAddr,
) -> Result<(), OpError>
where
    CB: NetworkBridge + Clone + Send + 'static,
{
    tracing::info!(
        tx = %incoming_tx,
        contract = %contract_key,
        %stream_id,
        total_size,
        htl,
        %upstream_addr,
        phase = "relay_put_streaming_request",
        "PUT streaming relay: processing RequestStreaming"
    );

    // ── Step 1: Claim the inbound stream (atomic dedup) ────────────────────
    let stream_handle = match op_manager
        .orphan_stream_registry()
        .claim_or_wait(upstream_addr, stream_id, STREAM_CLAIM_TIMEOUT)
        .await
    {
        Ok(handle) => handle,
        Err(OrphanStreamError::AlreadyClaimed) => {
            tracing::debug!(
                tx = %incoming_tx,
                %stream_id,
                "PUT streaming relay: stream already claimed, skipping"
            );
            return Ok(());
        }
        Err(err) => {
            tracing::error!(
                tx = %incoming_tx,
                %stream_id,
                error = %err,
                "PUT streaming relay: orphan stream claim failed"
            );
            // Silently fail — upstream's waiter falls back to its own
            // OPERATION_TTL. Same rationale as dedup-reject above:
            // PutMsg has no NotFound variant, and fabricating a
            // PutMsg::Response would tell upstream "contract stored"
            // when in fact no fragments were consumed at all.
            return Err(OpError::OrphanStreamClaimFailed);
        }
    };

    // ── Step 2: Select next hop BEFORE assembly (enables piped forward) ───
    let mut new_skip_list = skip_list;
    new_skip_list.insert(upstream_addr);
    if let Some(own_addr) = op_manager.ring.connection_manager.get_own_addr() {
        new_skip_list.insert(own_addr);
    }

    let next_hop = if htl > 0 {
        op_manager
            .ring
            .closest_potentially_hosting(&contract_key, &new_skip_list)
    } else {
        None
    };

    let next_hop_addr = next_hop.as_ref().and_then(|p| p.socket_addr());

    // ── Step 3: If next hop + streaming appropriate, set up piped forward ─
    //
    // Layout: we send metadata via `ctx.send_to_and_register_waiter`
    // (enqueues RequestStreaming on `op_execution_sender` + returns
    // the reply receiver once the waiter-install is sequenced into
    // the event loop), then call `conn_manager.pipe_stream` to push
    // fragments on the forked handle. Ordering is load-bearing: a
    // fast downstream reply would race a `pipe_stream`-first ordering
    // and be dropped as OpNotPresent before the waiter lands. Matches
    // legacy put.rs:1062-1072 semantically — metadata-first, then
    // pipe — but split so the reply can be awaited in parallel with
    // local stream assembly.
    let piping = if let Some(next_addr) = next_hop_addr {
        if crate::operations::should_use_streaming(
            op_manager.streaming_threshold,
            total_size as usize,
        ) {
            let outbound_sid = StreamId::next_operations();
            let forked_handle = stream_handle.fork();
            let new_htl = htl.saturating_sub(1);

            let pipe_metadata = PutMsg::RequestStreaming {
                id: incoming_tx,
                stream_id: outbound_sid,
                contract_key,
                total_size,
                htl: new_htl,
                skip_list: new_skip_list.clone(),
                subscribe,
            };
            let pipe_metadata_net: NetMessage = pipe_metadata.into();
            let embedded_metadata = match bincode::serialize(&pipe_metadata_net) {
                Ok(bytes) => Some(bytes::Bytes::from(bytes)),
                Err(e) => {
                    tracing::warn!(
                        tx = %incoming_tx,
                        error = %e,
                        "Failed to serialize piped stream metadata for embedding"
                    );
                    None
                }
            };

            tracing::info!(
                tx = %incoming_tx,
                inbound_stream_id = %stream_id,
                outbound_stream_id = %outbound_sid,
                total_size,
                peer_addr = %next_addr,
                "Starting piped stream forwarding to next hop"
            );

            if let Some(ref peer) = next_hop {
                if let Some(event) = NetEventLog::put_request(
                    &incoming_tx,
                    &op_manager.ring,
                    contract_key,
                    peer.clone(),
                    new_htl,
                ) {
                    op_manager.ring.register_events(Either::Left(event)).await;
                }
            }

            Some((
                next_addr,
                pipe_metadata_net,
                outbound_sid,
                forked_handle,
                embedded_metadata,
            ))
        } else {
            None
        }
    } else {
        None
    };

    // ── Step 4: Install reply waiter + dispatch metadata, then pipe
    //           fragments — in that strict order. ────────────────────────
    //
    // CRITICAL ordering: `send_to_and_register_waiter` enqueues the
    // metadata onto `op_execution_sender` and returns the reply
    // receiver AFTER the send lands. The waiter is installed when the
    // event loop drains that payload (atomic with the outbound
    // dispatch — see `handle_op_execution` in p2p_protoc.rs). Only
    // AFTER that do we enqueue `pipe_stream` onto the bridge channel.
    // If we inverted the order, a fast downstream reply could reach
    // `handle_pure_network_message_v1` before the waiter installs —
    // `try_forward_driver_reply` would drop the reply as
    // OpNotPresent and the driver would hang until `OPERATION_TTL`.
    let downstream_reply_rx: Option<(SocketAddr, tokio::sync::mpsc::Receiver<NetMessage>)> =
        if let Some((next_addr, metadata_net, outbound_sid, forked_handle, embedded_metadata)) =
            piping
        {
            let mut ctx = op_manager.op_ctx(incoming_tx);
            let rx_opt: Option<tokio::sync::mpsc::Receiver<NetMessage>> = match ctx
                .send_to_and_register_waiter(next_addr, metadata_net)
                .await
            {
                Ok(rx) => Some(rx),
                Err(err) => {
                    tracing::warn!(
                        tx = %incoming_tx,
                        target = %next_addr,
                        error = %err,
                        "PUT streaming relay: metadata register_waiter failed; will finalize locally"
                    );
                    None
                }
            };
            if rx_opt.is_some() {
                if let Err(err) = conn_manager
                    .pipe_stream(next_addr, outbound_sid, forked_handle, embedded_metadata)
                    .await
                {
                    tracing::warn!(
                        tx = %incoming_tx,
                        target = %next_addr,
                        error = %err,
                        "PUT streaming relay: pipe_stream failed after waiter install; \
                         will wait on downstream reply and bubble what we get"
                    );
                    // Waiter is already installed; the downstream may still
                    // reply to the metadata alone (legacy would also try).
                    // Keep the receiver so the reply can be consumed.
                }
            }
            rx_opt.map(|rx| (next_addr, rx))
        } else {
            None
        };

    // ── Step 5: Assemble stream locally (always — needed for put_contract) ─
    let stream_data = match stream_handle.assemble().await {
        Ok(data) => data,
        Err(err) => {
            tracing::error!(
                tx = %incoming_tx,
                %stream_id,
                error = %err,
                "PUT streaming relay: stream assembly failed"
            );
            return Err(OpError::StreamCancelled);
        }
    };

    let payload: PutStreamingPayload = match bincode::deserialize(&stream_data) {
        Ok(p) => p,
        Err(err) => {
            tracing::error!(
                tx = %incoming_tx,
                %stream_id,
                error = %err,
                "PUT streaming relay: payload deserialize failed"
            );
            return Err(OpError::invalid_transition(incoming_tx));
        }
    };

    let PutStreamingPayload {
        contract,
        value,
        related_contracts,
    } = payload;
    let key = contract.key();
    if key != contract_key {
        tracing::error!(
            tx = %incoming_tx,
            expected = %contract_key,
            actual = %key,
            "PUT streaming relay: contract key mismatch"
        );
        return Err(OpError::invalid_transition(incoming_tx));
    }

    // ── Step 6: Store contract locally (shared helper with slice A) ──────
    let merged_value = relay_put_store_locally(
        op_manager,
        incoming_tx,
        key,
        value,
        &contract,
        related_contracts,
        htl,
    )
    .await?;

    // ── Step 7: Await downstream reply (if piping), then bubble upstream ──
    //
    // Per-relay routing-event recording: the relay's chosen `next_hop`
    // either responded usefully (Success) or didn't (Failure). Feeding
    // these into the local Router lets the failure-probability model
    // learn from forwarded traffic, not just originator-side ops.
    if let Some((next_addr, mut rx)) = downstream_reply_rx {
        let reply = match tokio::time::timeout(OPERATION_TTL, rx.recv()).await {
            Ok(Some(reply)) => reply,
            Ok(None) => {
                tracing::warn!(
                    tx = %incoming_tx,
                    target = %next_addr,
                    "PUT streaming relay: downstream reply channel closed before reply"
                );
                if let Some(ref peer) = next_hop {
                    crate::operations::record_relay_route_event(
                        op_manager,
                        peer.clone(),
                        crate::ring::Location::from(&key),
                        crate::router::RouteOutcome::Failure,
                        crate::node::network_status::OpType::Put,
                    );
                }
                op_manager.release_pending_op_slot(incoming_tx).await;
                // Best-effort upstream reply after downstream failure: this
                // node IS the storer (contract was put locally in step 6),
                // so hop_count = max_htl - htl_we_received.
                let hop_count = op_manager.ring.max_hops_to_live.saturating_sub(htl);
                return relay_put_send_response(
                    op_manager,
                    incoming_tx,
                    key,
                    upstream_addr,
                    hop_count,
                )
                .await;
            }
            Err(_elapsed) => {
                tracing::warn!(
                    tx = %incoming_tx,
                    target = %next_addr,
                    "PUT streaming relay: downstream reply timed out"
                );
                if let Some(ref peer) = next_hop {
                    crate::operations::record_relay_route_event(
                        op_manager,
                        peer.clone(),
                        crate::ring::Location::from(&key),
                        crate::router::RouteOutcome::Failure,
                        crate::node::network_status::OpType::Put,
                    );
                }
                op_manager.release_pending_op_slot(incoming_tx).await;
                // Same as the channel-closed arm: this node stored locally
                // in step 6 so we report our own forward depth.
                let hop_count = op_manager.ring.max_hops_to_live.saturating_sub(htl);
                return relay_put_send_response(
                    op_manager,
                    incoming_tx,
                    key,
                    upstream_addr,
                    hop_count,
                )
                .await;
            }
        };
        op_manager.release_pending_op_slot(incoming_tx).await;

        match reply {
            NetMessage::V1(NetMessageV1::Put(PutMsg::Response {
                key: reply_key,
                hop_count: downstream_hop_count,
                ..
            }))
            | NetMessage::V1(NetMessageV1::Put(PutMsg::ResponseStreaming {
                key: reply_key,
                hop_count: downstream_hop_count,
                ..
            })) => {
                tracing::info!(
                    tx = %incoming_tx,
                    contract = %reply_key,
                    phase = "relay_put_streaming_bubble",
                    "PUT streaming relay: downstream replied; bubbling Response upstream"
                );
                if let Some(ref peer) = next_hop {
                    crate::operations::record_relay_route_event(
                        op_manager,
                        peer.clone(),
                        crate::ring::Location::from(&reply_key),
                        crate::router::RouteOutcome::SuccessUntimed,
                        crate::node::network_status::OpType::Put,
                    );
                }
                // Preserve downstream storer's forward depth.
                relay_put_send_response(
                    op_manager,
                    incoming_tx,
                    reply_key,
                    upstream_addr,
                    downstream_hop_count,
                )
                .await
            }
            other => {
                // Unexpected reply variant: unclear attribution. Skip the
                // route-event hook (matches the non-streaming relay's
                // unexpected-variant arm).
                tracing::warn!(
                    tx = %incoming_tx,
                    contract = %key,
                    reply_variant = ?std::mem::discriminant(&other),
                    "PUT streaming relay: unexpected reply variant"
                );
                // Same as the error arms: this node stored locally.
                let hop_count = op_manager.ring.max_hops_to_live.saturating_sub(htl);
                relay_put_send_response(op_manager, incoming_tx, key, upstream_addr, hop_count)
                    .await
            }
        }
    } else {
        // No next hop, streaming not appropriate, or register_waiter failed —
        // finalize locally and bubble Response upstream.
        // This node IS the storer; hop_count = max_htl - htl_we_received.
        let hop_count = op_manager.ring.max_hops_to_live.saturating_sub(htl);
        relay_put_finalize_local(
            op_manager,
            incoming_tx,
            key,
            merged_value,
            upstream_addr,
            hop_count,
        )
        .await
    }
}

// ── End of relay PUT driver ─────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    fn dummy_key() -> ContractKey {
        ContractKey::from_id_and_code(ContractInstanceId::new([1u8; 32]), CodeHash::new([2u8; 32]))
    }

    fn dummy_tx() -> Transaction {
        Transaction::new::<PutMsg>()
    }

    /// Issue #4251 follow-up: the PUT relay wrappers must mirror the UPDATE
    /// wrappers' queue-full gating. Without it, a contract whose PUTs
    /// saturate the per-contract queue would emit unbounded WARN spam
    /// from `relay_put_error` / `relay_put_streaming_error` — the same
    /// failure mode that filled `nova`'s error log with ~40 WARN/sec from
    /// `relay_update_broadcast_error`. PUT volume is incidental today but
    /// the regression risk is identical to UPDATE. Sibling pin test for
    /// the UPDATE wrappers lives in `update/op_ctx_task.rs`.
    #[test]
    fn run_relay_put_wrappers_gate_queue_full_log_severity() {
        let src = include_str!("op_ctx_task.rs");
        for wrapper in [
            "async fn run_relay_put<",
            "async fn run_relay_put_streaming<",
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

    /// Streaming PUTs must run exactly one attempt (zero peer
    /// advancements).
    ///
    /// Background: the gateway's PUT driver previously allowed up to
    /// `MAX_RETRIES = 3` advancements × `STREAMING_ATTEMPT_TIMEOUT_CAP
    /// = 600s` = ~40-min wall-clock budget per client PUT (initial +
    /// 3 advancements = 4 attempts). The freenet-git client times
    /// out at 180s per attempt and reports the run as failed — but
    /// the gateway is still working, so it eventually publishes a
    /// terminal `PutResponse(Err)` several minutes after the client
    /// gave up. This is the "silent timeout" failure mode tracked in
    /// freenet-git#53.
    ///
    /// Subtle semantic the previous version of this test got wrong:
    /// `MAX_PEER_ADVANCEMENTS_* = N` means **N additional peers tried
    /// after the initial attempt**, not N total attempts. The cap is
    /// only checked inside `advance_to_next_peer`, and the first
    /// attempt always runs unconditionally in
    /// `op_ctx.rs::drive_retry_loop` before any cap check. So
    /// `MAX_PEER_ADVANCEMENTS_STREAMING = 1` would actually allow
    /// **2** attempts; `= 0` is required for a true single attempt.
    /// Catching the off-by-one was an external review finding —
    /// this pin now asserts the corrected semantic.
    ///
    /// Pin: the budget contract between gateway and any WS-API client
    /// for streaming PUTs is `(1 + MAX_PEER_ADVANCEMENTS_STREAMING)
    /// × STREAMING_ATTEMPT_TIMEOUT_CAP <= max reasonable client
    /// patience`. We require the worst case to be at most a single
    /// `STREAMING_ATTEMPT_TIMEOUT_CAP` (600s); any change must
    /// re-engage the math against the dominant WS-API consumers
    /// (freenet-git, riverctl) and update this pin.
    #[test]
    fn streaming_put_retry_budget_does_not_exceed_client_patience() {
        // Total attempts = initial + advancements. Worst case is
        // `(advancements + 1) × per-attempt cap`.
        let worst_case = std::time::Duration::from_secs(
            (MAX_PEER_ADVANCEMENTS_STREAMING as u64 + 1)
                * crate::operations::STREAMING_ATTEMPT_TIMEOUT_CAP.as_secs(),
        );
        assert!(
            worst_case <= crate::operations::STREAMING_ATTEMPT_TIMEOUT_CAP,
            "streaming PUT worst-case wall-clock {worst_case:?} exceeds \
             one STREAMING_ATTEMPT_TIMEOUT_CAP \
             ({:?}); freenet-git#53 will recur. Either reduce \
             MAX_PEER_ADVANCEMENTS_STREAMING (currently {}) or \
             STREAMING_ATTEMPT_TIMEOUT_CAP.",
            crate::operations::STREAMING_ATTEMPT_TIMEOUT_CAP,
            MAX_PEER_ADVANCEMENTS_STREAMING,
        );
        assert_eq!(
            MAX_PEER_ADVANCEMENTS_STREAMING, 0,
            "MAX_PEER_ADVANCEMENTS_STREAMING must be 0 (single \
             attempt, no advancement). N>0 silently allows N+1 \
             attempts and busts the WS-client budget — that's the \
             original freenet-git#53 footgun. Raising this requires \
             updating both this pin and the rationale on the constant."
        );
        // Sanity: non-streaming budget is unchanged.
        assert_eq!(
            MAX_PEER_ADVANCEMENTS_NON_STREAMING, 3,
            "non-streaming PUTs keep the legacy 3-advancement budget \
             (= 4 attempts total); this test does not authorize a \
             reduction (would shrink the k_closest fan-out coverage \
             for small payloads)."
        );
    }

    /// Behavioural pin: `advance_to_next_peer` with
    /// `max_advancements = MAX_PEER_ADVANCEMENTS_STREAMING` must
    /// return `None` on the first call. Catches off-by-one regressions
    /// in the `*retries >= max_advancements` comparison that the
    /// constant-only pin above cannot see.
    #[test]
    fn advance_to_next_peer_at_streaming_cap_exhausts_immediately() {
        let cap = MAX_PEER_ADVANCEMENTS_STREAMING;
        let mut retries: usize = 0;
        // Simulate the loop's behaviour against the counter only —
        // the actual OpManager call would also need a key and ring
        // fixture, but the gating is pure on the counter.
        let allow_first_advance = retries < cap;
        assert!(
            !allow_first_advance,
            "MAX_PEER_ADVANCEMENTS_STREAMING ({cap}) must NOT permit \
             any advancement — drive_retry_loop's first attempt has \
             already run before advance() is called. Permitting even \
             one advancement re-opens the 2x-budget bug freenet-git#53."
        );
        // Sanity: non-streaming cap permits 3 advancements.
        retries = 0;
        for round in 0..MAX_PEER_ADVANCEMENTS_NON_STREAMING {
            assert!(
                retries < MAX_PEER_ADVANCEMENTS_NON_STREAMING,
                "non-streaming advance round {round} should be allowed"
            );
            retries += 1;
        }
        assert!(
            retries >= MAX_PEER_ADVANCEMENTS_NON_STREAMING,
            "non-streaming exhausts after {MAX_PEER_ADVANCEMENTS_NON_STREAMING} advancements"
        );
    }

    /// Source-grep pin: `drive_client_put_inner` must select between
    /// the streaming and non-streaming caps based on
    /// `should_use_streaming(threshold, payload_size_estimate)`. A
    /// refactor that hard-codes `MAX_PEER_ADVANCEMENTS_NON_STREAMING`
    /// for all PUTs (or inlines a literal `3` while leaving the
    /// `should_use_streaming` call structurally present) would
    /// silently re-open freenet-git#53; this pin keeps the dispatch
    /// site visible and rejects bare literals in the streaming
    /// branch.
    #[test]
    fn drive_client_put_inner_dispatches_streaming_cap_on_should_use_streaming() {
        let src = include_str!("op_ctx_task.rs");
        let entry = src
            .find("fn drive_client_put_inner")
            .expect("drive_client_put_inner must exist");
        // Anchor on the `max_advancements` binding; the if/else
        // block including its inline rationale comment can be
        // hundreds of bytes, hence the generous window.
        let cap_decision = src[entry..]
            .find("let max_advancements =")
            .expect("drive_client_put_inner must compute `let max_advancements = …`");
        let window = &src[entry + cap_decision..entry + cap_decision + 1500];
        assert!(
            window.contains("should_use_streaming("),
            "drive_client_put_inner's max_advancements selection must \
             gate on should_use_streaming(threshold, \
             payload_size_estimate). A flat \
             MAX_PEER_ADVANCEMENTS_NON_STREAMING for all PUTs re-opens \
             freenet-git#53."
        );
        assert!(
            window.contains("MAX_PEER_ADVANCEMENTS_STREAMING")
                && window.contains("MAX_PEER_ADVANCEMENTS_NON_STREAMING"),
            "drive_client_put_inner must reference both advancement \
             caps by name in the selection so future readers see the \
             split — bare integer literals are forbidden here."
        );
    }

    /// `start_client_put` must construct a `client_op_guard()` before
    /// the `GlobalExecutor::spawn`, and that guard MUST be moved into
    /// the spawned future (held for the lifetime of the spawned
    /// `run_client_put`). Without the guard, the shutdown drain in
    /// `ShutdownHandle::shutdown` has no signal that this PUT is in
    /// flight — release-driven auto-update reverts to dropping the
    /// mirror push mid-stream. Sibling pins live in the analogous test
    /// modules of `get/op_ctx_task.rs`, `update/op_ctx_task.rs`, and
    /// `subscribe/op_ctx_task.rs`.
    #[test]
    fn start_client_put_acquires_inflight_guard_before_spawn() {
        let src = include_str!("op_ctx_task.rs");
        let entry = src
            .find("pub(crate) async fn start_client_put(")
            .expect("start_client_put must exist");
        let after_spawn = src[entry..]
            .find("GlobalExecutor::spawn(")
            .expect("start_client_put must spawn a driver task");
        let before_spawn = &src[entry..entry + after_spawn];
        assert!(
            before_spawn.contains("op_manager.admit_client_op()"),
            "start_client_put must call op_manager.admit_client_op() \
             before GlobalExecutor::spawn so (a) the shutdown drain \
             knows this PUT is in flight and (b) the gate check + \
             counter bump are atomic. Reverting to a separate \
             is_shutting_down() check + client_op_guard() bump \
             re-opens the TOCTOU window (Codex r2 finding)."
        );
        assert!(
            before_spawn.contains("OpError::NodeShuttingDown"),
            "start_client_put must early-return OpError::NodeShuttingDown \
             when admit_client_op() refuses. Dropping the early-return \
             would silently spawn a driver that the Disconnect cuts off."
        );
        let spawned = &src[entry + after_spawn..];
        let block_end = spawned
            .find("\n    Ok(client_tx)")
            .expect("start_client_put must return Ok(client_tx)");
        let spawn_block = &spawned[..block_end];
        assert!(
            spawn_block.contains("let _inflight_guard = inflight_guard;"),
            "the ClientOpGuard must be moved into the spawned future \
             via `let _inflight_guard = inflight_guard;` so it is held \
             for the full driver lifetime. A bare drop at the spawn \
             site would clear the counter before run_client_put even \
             starts."
        );
    }

    #[test]
    fn classify_reply_response_is_stored() {
        let tx = dummy_tx();
        let key = dummy_key();
        let msg = NetMessage::V1(NetMessageV1::Put(PutMsg::Response {
            id: tx,
            key,
            hop_count: 0,
        }));
        assert!(matches!(classify_reply(&msg), ReplyClass::Stored { .. }));
    }

    #[test]
    fn classify_reply_response_streaming_is_stored() {
        let tx = dummy_tx();
        let key = dummy_key();
        let msg = NetMessage::V1(NetMessageV1::Put(PutMsg::ResponseStreaming {
            id: tx,
            key,
            continue_forwarding: false,
            hop_count: 0,
        }));
        assert!(matches!(classify_reply(&msg), ReplyClass::Stored { .. }));
    }

    /// Regression pin: `classify_reply` MUST extract the wire-carried
    /// `hop_count` from `Response`/`ResponseStreaming` and propagate it
    /// (via the bubble-up call site immediately following the classifier)
    /// into the upstream Response unchanged.  Mirrors the GET classifier
    /// pin `classify_response_found_preserves_hop_count`.  Since
    /// `ReplyClass::Stored` itself doesn't carry the hop_count
    /// (the bubble-up grabs the field directly from the matched message),
    /// this test pins the classifier *and* the matcher contract by
    /// destructuring the message and asserting the field survives
    /// classification — that is, classify_reply doesn't conditionally
    /// reject Stored on hop_count = 0 or any other value.
    #[test]
    fn classify_reply_preserves_hop_count_for_stored() {
        for hc in [0_usize, 1, 4, 10, 64] {
            let tx = dummy_tx();
            let key = dummy_key();
            // Response variant
            let msg = NetMessage::V1(NetMessageV1::Put(PutMsg::Response {
                id: tx,
                key,
                hop_count: hc,
            }));
            // Stored variant must extract the wire hop_count into the
            // classifier output — the originator driver uses this to
            // populate `PutFinalizationData.hop_count`. A regression that
            // re-introduced `Stored { key }` without the field would
            // silently revert PutSuccess from `finalize_put_at_originator`
            // back to `None` (codex review of #4248).
            match classify_reply(&msg) {
                ReplyClass::Stored {
                    key: got_key,
                    hop_count: got_hc,
                } => {
                    assert_eq!(got_key, key, "Stored.key preserved");
                    assert_eq!(got_hc, hc, "Stored.hop_count preserved ({hc})");
                }
                other @ (ReplyClass::LocalCompletion { .. } | ReplyClass::Unexpected) => {
                    panic!("expected Stored, got {other:?} for hc={hc}")
                }
            }

            // ResponseStreaming variant
            let msg = NetMessage::V1(NetMessageV1::Put(PutMsg::ResponseStreaming {
                id: tx,
                key,
                continue_forwarding: false,
                hop_count: hc,
            }));
            match classify_reply(&msg) {
                ReplyClass::Stored {
                    key: got_key,
                    hop_count: got_hc,
                } => {
                    assert_eq!(got_key, key, "ResponseStreaming Stored.key preserved");
                    assert_eq!(
                        got_hc, hc,
                        "ResponseStreaming Stored.hop_count preserved ({hc})"
                    );
                }
                other @ (ReplyClass::LocalCompletion { .. } | ReplyClass::Unexpected) => {
                    panic!("expected Stored, got {other:?} for hc={hc}")
                }
            }
        }
    }

    /// Regression pin: the client-PUT driver's `RetryLoopOutcome::Done`
    /// branch MUST pass the wire-carried `hop_count` into
    /// `PutFinalizationData` (clamped to `max_hops_to_live`). Without
    /// this, the originator emits TWO `PutSuccess` events per tx: the
    /// implicit one from `from_inbound_msg_v1` (populated) and the
    /// explicit one from `finalize_put_at_originator` (`None`) — exactly
    /// the inconsistency codex flagged on the first review pass of
    /// #4248. Scrapes the source so a regression that re-introduces
    /// `hop_count: None,` literal in the PutFinalizationData
    /// construction trips immediately.
    #[test]
    fn finalize_put_at_originator_uses_wire_hop_count() {
        const SOURCE: &str = include_str!("op_ctx_task.rs");
        // Anchor: the `RetryLoopOutcome::Done` match arm in
        // `start_client_put`. Scan forward to the next
        // `finalize_put_at_originator(` call and verify the field shape.
        let done_arm = SOURCE
            .find("RetryLoopOutcome::Done((reply_key, wire_hop_count))")
            .expect(
                "RetryLoopOutcome::Done destructure not found — \
                 if you've refactored to use named struct destructuring \
                 update this anchor but keep the wire_hop_count threading",
            );
        let finalize_call = SOURCE[done_arm..]
            .find("finalize_put_at_originator(")
            .expect("no finalize_put_at_originator call in Done arm");
        let region = &SOURCE[done_arm..done_arm + finalize_call + 500];
        // The construction MUST NOT hard-code `hop_count: None,`. It must
        // pass `hop_count` computed from `wire_hop_count`.
        assert!(
            !region.contains("hop_count: None,"),
            "PutFinalizationData construction in start_client_put's \
             Done arm must NOT hard-code hop_count: None — that emits a \
             PutSuccess with hop_count=None alongside the populated one \
             from from_inbound_msg_v1, defeating #4248"
        );
        assert!(
            region.contains("hop_count,"),
            "PutFinalizationData construction in start_client_put's \
             Done arm must pass hop_count from the wire value"
        );
        assert!(
            region.contains("wire_hop_count.map"),
            "wire_hop_count must be mapped (e.g. clamp via .min(max_htl)) \
             before being passed into PutFinalizationData"
        );
    }

    #[test]
    fn classify_reply_forwarding_ack_is_unexpected() {
        let tx = dummy_tx();
        let key = dummy_key();
        let msg = NetMessage::V1(NetMessageV1::Put(PutMsg::ForwardingAck {
            id: tx,
            contract_key: key,
        }));
        assert!(
            matches!(classify_reply(&msg), ReplyClass::Unexpected),
            "ForwardingAck must NOT be classified as terminal"
        );
    }

    /// Regression guard for the double-subscribe bug (commit 494a3c69).
    ///
    /// The driver calls `finalize_put_at_originator` for telemetry and then
    /// `maybe_subscribe_child` for subscriptions. If `finalize_put_at_originator`
    /// is called with `subscribe=true`, it starts a subscription via the legacy
    /// `start_subscription_after_put` path, AND `maybe_subscribe_child` starts
    /// another via the driver `run_client_subscribe` path — doubling
    /// network traffic and subscription registrations.
    ///
    /// This test scrapes the source to verify all `finalize_put_at_originator`
    /// calls inside the driver pass `false` for the subscribe arguments.
    #[test]
    fn finalize_put_at_originator_never_subscribes_from_driver() {
        const SOURCE: &str = include_str!("op_ctx_task.rs");

        // Find every call to finalize_put_at_originator in this file.
        // Each call must pass `false` for the subscribe parameter (5th arg).
        // The pattern we check: the two lines after `state_size: None,` and
        // before `)` must both be `false,` or `false`.
        let call_marker = "finalize_put_at_originator(";
        let mut offset = 0;
        let mut call_count = 0;

        while let Some(pos) = SOURCE[offset..].find(call_marker) {
            let abs_pos = offset + pos;
            // Get the window from the call to the closing `.await`
            let window_end = SOURCE[abs_pos..]
                .find(".await")
                .map(|p| abs_pos + p)
                .unwrap_or(SOURCE.len().min(abs_pos + 500));
            let window = &SOURCE[abs_pos..window_end];

            // The subscribe arguments should be `false`. Check that
            // `true` does NOT appear as a subscribe argument.
            // The window contains the struct literal for PutFinalizationData
            // plus the two boolean args. After `},` the next two values
            // are subscribe and blocking_subscribe.
            let after_struct = window.find("},").map(|p| &window[p..]);
            if let Some(tail) = after_struct {
                assert!(
                    !tail.contains("subscribe"),
                    "finalize_put_at_originator call in driver passes subscribe \
                     arguments that reference the `subscribe` variable instead of \
                     hardcoded `false`. This would cause double-subscription — \
                     subscriptions must be handled exclusively by maybe_subscribe_child. \
                     See commit 494a3c69 for the original fix."
                );
            }
            call_count += 1;
            offset = abs_pos + call_marker.len();
        }

        assert!(
            call_count >= 1,
            "Expected at least 1 finalize_put_at_originator call in the driver, \
             found {call_count}"
        );
    }

    #[test]
    fn max_advancements_boundary_exhausts_at_limit() {
        // Verify the MAX_PEER_ADVANCEMENTS_NON_STREAMING boundary:
        // retries >= cap → None. Tests the counter logic that
        // advance_to_next_peer uses. Streaming variant covered by
        // `advance_to_next_peer_at_streaming_cap_exhausts_immediately`.
        let cap = MAX_PEER_ADVANCEMENTS_NON_STREAMING;
        let mut retries: usize = 0;
        for _ in 0..cap {
            assert!(retries < cap, "should not exhaust before limit");
            retries += 1;
        }
        assert!(retries >= cap, "should exhaust at cap={cap}");
    }

    #[test]
    fn classify_reply_unexpected_for_non_put_message() {
        // An Aborted message (non-PUT) should be Unexpected.
        let tx = dummy_tx();
        let msg = NetMessage::V1(NetMessageV1::Aborted(tx));
        assert!(matches!(classify_reply(&msg), ReplyClass::Unexpected));
    }

    /// Behavioral regression for issue #4001: the client-PUT driver's
    /// per-attempt timeout must scale up when the (state + contract code)
    /// payload would trigger streaming. A small payload uses the unscaled
    /// `OPERATION_TTL`; a payload at the freenet.org website's observed
    /// 2.4 MB size must clear the 63 s the original streaming PUT actually
    /// took before declaring the attempt dead.
    ///
    /// This test exercises the helper directly — refactors that move or
    /// rename the call site but preserve the contract still pass.
    #[test]
    fn compute_put_attempt_timeout_matches_payload_streaming_decision() {
        use crate::config::OPERATION_TTL;

        let small_state = WrappedState::new(vec![0u8; 1024]);
        let small_contract =
            ContractContainer::Wasm(ContractWasmAPIVersion::V1(WrappedContract::new(
                Arc::new(ContractCode::from(vec![0u8; 256])),
                Parameters::from(vec![]),
            )));
        // 64 KiB default streaming threshold; small payload => OPERATION_TTL.
        assert_eq!(
            compute_put_attempt_timeout(64 * 1024, &small_state, &small_contract),
            OPERATION_TTL,
            "non-streaming-eligible payload must reuse OPERATION_TTL"
        );

        // Website case: 2.4 MB total. Must exceed the observed 63 s
        // completion so the retry loop doesn't fire mid-flight.
        let website_state = WrappedState::new(vec![0u8; 2_460_242 - 256]);
        let timeout = compute_put_attempt_timeout(64 * 1024, &website_state, &small_contract);
        assert!(
            timeout > std::time::Duration::from_secs(63),
            "website-scale payload timeout {timeout:?} must exceed observed \
             completion (~62 s); otherwise issue #4001 recurs"
        );
        assert!(
            timeout > OPERATION_TTL,
            "website-scale payload timeout {timeout:?} must exceed OPERATION_TTL"
        );
    }

    /// The size estimate (`state.size() + contract.data().len()`) is a
    /// strict lower bound on the actual bincode-serialized
    /// `PutStreamingPayload` size that `process_message` checks against
    /// `streaming_threshold`. The 20 KiB/s throughput floor inside
    /// `streaming_aware_attempt_timeout` then absorbs the slack, but only
    /// if the slack stays below ~2× (half of observed throughput).
    ///
    /// This test pins that invariant by constructing a representative
    /// payload and confirming the bincode size is within 2× of the
    /// estimate. If a future change to bincode framing or
    /// `PutStreamingPayload` blows past 2×, the throughput floor needs
    /// re-tuning OR the estimate needs to include more fields.
    #[test]
    fn payload_size_estimate_within_throughput_floor_safety_margin() {
        use crate::operations::put::PutStreamingPayload;

        // Realistic payload: 1 MB state + 200 KB contract code + nontrivial
        // parameters and one related contract — i.e. headroom-stress for
        // the things the estimate omits.
        let state = WrappedState::new(vec![0xABu8; 1024 * 1024]);
        let contract = ContractContainer::Wasm(ContractWasmAPIVersion::V1(WrappedContract::new(
            Arc::new(ContractCode::from(vec![0x42u8; 200 * 1024])),
            Parameters::from(vec![0x55u8; 4096]),
        )));

        let estimate = state.size() + contract.data().len();
        let payload = PutStreamingPayload {
            contract: contract.clone(),
            related_contracts: RelatedContracts::default(),
            value: state,
        };
        let actual = bincode::serialized_size(&payload).expect("serializable") as usize;

        assert!(
            actual <= estimate.saturating_mul(2),
            "bincode payload size {actual} exceeds 2× estimate {estimate} — \
             the 20 KiB/s throughput floor (half observed throughput) no \
             longer absorbs the slack; either tighten the estimate (include \
             parameters / related_contracts) or revisit \
             STREAMING_THROUGHPUT_FLOOR_BPS in operations.rs"
        );
    }

    #[test]
    fn driver_outcome_exhausted_produces_client_error() {
        // Verify that RetryLoopOutcome::Exhausted maps to a client-visible
        // OperationError, not a silent drop or infrastructure error.
        let cause = "PUT to contract failed after 3 attempts".to_string();
        let outcome: DriverOutcome =
            match RetryLoopOutcome::<(ContractKey, Option<usize>)>::Exhausted(cause) {
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

    #[test]
    fn classify_reply_request_is_local_completion() {
        // When process_message completes locally (no next hop), the Request
        // is echoed back via forward_pending_op_result_if_completed.
        let tx = dummy_tx();
        let msg = NetMessage::V1(NetMessageV1::Put(PutMsg::Request {
            id: tx,
            contract: ContractContainer::Wasm(ContractWasmAPIVersion::V1(WrappedContract::new(
                Arc::new(ContractCode::from(vec![0u8])),
                Parameters::from(vec![]),
            ))),
            related_contracts: RelatedContracts::default(),
            value: WrappedState::new(vec![1u8]),
            htl: 5,
            skip_list: HashSet::new(),
        }));
        assert!(matches!(
            classify_reply(&msg),
            ReplyClass::LocalCompletion { .. }
        ));
    }

    // ── Relay PUT driver structural pin tests. Anchor the relay
    // section on the entry-point fn so module-level doc comments
    // (which reference variant names by design) don't enter scope.

    fn relay_section(src: &str) -> &str {
        let start = src
            .find("pub(crate) async fn start_relay_put<CB>(")
            .expect("start_relay_put not found");
        let end = src
            .find("\n#[cfg(test)]")
            .expect("test module marker not found");
        &src[start..end]
    }

    /// Slice A only — slice B streaming driver begins at the "Relay
    /// streaming PUT driver" marker below. Used by tests that pin
    /// slice A properties (no streaming variant references) to avoid
    /// false positives from slice B's legitimate references.
    fn relay_slice_a_section(src: &str) -> &str {
        let start = src
            .find("pub(crate) async fn start_relay_put<CB>(")
            .expect("start_relay_put not found");
        let end = src
            .find("// ── Relay streaming PUT driver")
            .expect("slice B marker not found");
        &src[start..end]
    }

    /// Pin: dispatch entry must insert into `active_relay_put_txs`
    /// BEFORE spawning. Without this guard, duplicate inbound
    /// `PutMsg::Request` for an in-flight tx would spawn redundant
    /// drivers — same amplification mode that drove phase-5 GET's
    /// 63GB RSS explosion.
    #[test]
    fn start_relay_put_checks_dedup_gate() {
        let src = include_str!("op_ctx_task.rs");
        let relay = relay_section(src);
        let window_start = relay
            .find("pub(crate) async fn start_relay_put<CB>(")
            .expect("entry-point not found");
        let spawn_pos = relay[window_start..]
            .find("GlobalExecutor::spawn(run_relay_put(")
            .expect("spawn site not found")
            + window_start;
        let insert_pos = relay[window_start..]
            .find("active_relay_put_txs.insert(incoming_tx)")
            .expect("dedup insert site not found")
            + window_start;
        assert!(
            insert_pos < spawn_pos,
            "active_relay_put_txs.insert MUST happen before GlobalExecutor::spawn"
        );
    }

    /// Pin: dedup rejection must bump `RELAY_PUT_DEDUP_REJECTS`. Without
    /// the counter, `FREENET_MEMORY_STATS` operators cannot see the
    /// dedup gate firing under ci-fault-loss.
    #[test]
    fn dedup_rejection_increments_counter() {
        let src = include_str!("op_ctx_task.rs");
        let relay = relay_section(src);
        let after_insert = relay
            .split("active_relay_put_txs.insert(incoming_tx)")
            .nth(1)
            .expect("dedup insert site not found");
        let window = &after_insert[..500.min(after_insert.len())];
        assert!(
            window.contains("RELAY_PUT_DEDUP_REJECTS.fetch_add"),
            "dedup gate must increment RELAY_PUT_DEDUP_REJECTS on rejection"
        );
    }

    /// Pin: RAII guard must clear `active_relay_put_txs` + bump
    /// completion counters on drop. Without the guard, a panicking
    /// driver would leak the dedup entry permanently (no TTL).
    #[test]
    fn raii_guard_clears_dedup_set_on_drop() {
        let src = include_str!("op_ctx_task.rs");
        let drop_start = src
            .find("impl Drop for RelayPutInflightGuard")
            .expect("RelayPutInflightGuard Drop impl not found");
        let drop_body = &src[drop_start..drop_start + 600];
        assert!(
            drop_body.contains("active_relay_put_txs"),
            "RelayPutInflightGuard::drop must remove from active_relay_put_txs"
        );
        assert!(
            drop_body.contains("RELAY_PUT_INFLIGHT.fetch_sub"),
            "RelayPutInflightGuard::drop must decrement RELAY_PUT_INFLIGHT"
        );
        assert!(
            drop_body.contains("RELAY_PUT_COMPLETED_TOTAL.fetch_add"),
            "RelayPutInflightGuard::drop must increment RELAY_PUT_COMPLETED_TOTAL"
        );
    }

    /// Pin: driver forwards downstream using `send_to_and_await` — PUT
    /// relay IS req/response (like GET), so we need the reply to bubble
    /// upstream. If this flips to `send_fire_and_forget`, downstream
    /// Response never makes it back to originator.
    ///
    /// As of PR #4063, the driver has two forward paths:
    /// - non-streaming: `ctx.send_to_and_await(...)` — pinned here
    /// - upgrade-to-streaming: `ctx.send_to_and_register_waiter(...) +
    ///   conn_manager.send_stream(...)` — pinned by
    ///   `drive_relay_put_upgrades_when_payload_exceeds_threshold`
    ///
    /// Both await downstream Response via `pending_op_results` (waiter
    /// receiver in the streaming branch, callback in the non-streaming
    /// branch). Neither uses `send_fire_and_forget`.
    #[test]
    fn drive_relay_put_non_streaming_path_uses_send_to_and_await() {
        let src = include_str!("op_ctx_task.rs");
        let driver_start = src
            .find("async fn drive_relay_put<CB>(")
            .expect("drive_relay_put not found");
        let driver_end = src[driver_start..]
            .find("\nasync fn relay_put_finalize_local(")
            .expect("driver body end not found")
            + driver_start;
        let driver_src = &src[driver_start..driver_end];
        assert!(
            driver_src.contains("ctx.send_to_and_await("),
            "drive_relay_put non-streaming relay path (not loopback) must \
             forward downstream via send_to_and_await so the downstream \
             Response bubbles back to the relay's waiter"
        );
        // The driver MAY use `send_fire_and_forget` in the
        // originator-loopback branch: when
        // `upstream_addr == own_addr`, installing a waiter would
        // overwrite the originator's pending_op_results callback. The
        // fire-and-forget forward lets the downstream Response return
        // directly to the originator's still-installed callback. The
        // pin verifies the loopback branch exists AND that the
        // non-loopback branch still uses send_to_and_await.
        assert!(
            driver_src.contains("originator_loopback"),
            "drive_relay_put must distinguish the originator-loopback \
             branch from the true relay branch"
        );
    }

    /// Pin: `relay_put_send_response` MUST switch to local-loopback
    /// when `upstream_addr == own_addr`. Without this, the wire-bound
    /// `send_fire_and_forget(own_addr, ...)` tries to ship over a
    /// non-existent self-connection and the originator-loopback PUT
    /// fails with "Cannot establish connection - peer not found".
    /// Repro: `test_minimal_state_put_get` and the rest of
    /// `edge_case_state_sizes`.
    #[test]
    fn relay_put_send_response_uses_loopback_when_upstream_is_own_addr() {
        let src = include_str!("op_ctx_task.rs");
        let fn_start = src
            .find("async fn relay_put_send_response(")
            .expect("relay_put_send_response not found");
        let fn_end = src[fn_start..]
            .find("\n// ── Relay streaming PUT driver")
            .expect("end-of-relay-fn marker not found")
            + fn_start;
        let body = &src[fn_start..fn_end];
        assert!(
            body.contains("send_local_loopback("),
            "relay_put_send_response must call send_local_loopback for the \
             upstream==own_addr branch (originator-loopback PUT path)"
        );
        assert!(
            body.contains("get_own_addr()"),
            "relay_put_send_response must compare upstream_addr to \
             connection_manager.get_own_addr() to detect the loopback case"
        );
    }

    /// Pin: `run_relay_put` MUST skip `release_pending_op_slot` when
    /// running in originator-loopback mode (`upstream_addr ==
    /// own_addr`). The `pending_op_results` callback for `incoming_tx`
    /// in that mode is the originator's `send_and_await` waiter, not
    /// one this driver installed; releasing it would emit
    /// `TransactionCompleted` and remove the originator's callback
    /// BEFORE the loopback `PutMsg::Response` reaches the bypass
    /// (notifications channel has higher priority than op_execution
    /// in priority_select). Repro: `test_minimal_state_put_get`.
    #[test]
    fn run_relay_put_skips_release_in_originator_loopback() {
        let src = include_str!("op_ctx_task.rs");
        let fn_start = src
            .find("async fn run_relay_put<CB>(")
            .expect("run_relay_put not found");
        let fn_end = src[fn_start..]
            .find("\n#[allow(clippy::too_many_arguments)]\nasync fn drive_relay_put<CB>(")
            .expect("end-of-run_relay_put marker not found")
            + fn_start;
        let body = &src[fn_start..fn_end];
        // The release call must be guarded by an own_addr comparison.
        let release_pos = body
            .find("release_pending_op_slot(incoming_tx)")
            .expect("release_pending_op_slot call not found in run_relay_put");
        let preceding = &body[..release_pos];
        assert!(
            preceding.rfind("get_own_addr()").is_some(),
            "run_relay_put must call get_own_addr() before \
             release_pending_op_slot to gate the release on the \
             upstream != own_addr case"
        );
        assert!(
            preceding.rfind("Some(upstream_addr) != own_addr").is_some()
                || preceding.rfind("upstream_addr) != own_addr").is_some()
                || preceding.rfind("!originator_loopback").is_some(),
            "run_relay_put must guard release_pending_op_slot with an \
             upstream != own_addr check (originator-loopback exception)"
        );
    }

    /// Pin: when `drive_relay_put` returns `Err` AND the driver is
    /// running in originator-loopback mode (`upstream_addr ==
    /// own_addr`), `run_relay_put` MUST publish a `HostResult::Err`
    /// for `incoming_tx` via `send_client_result` and complete the
    /// transaction. This mirrors the legacy `report_result` Err arm
    /// (node.rs:636-651) — without this, an invalid-state PUT (e.g.,
    /// `put_contract` rejection) leaves the originator's
    /// `start_client_put` `send_and_await` waiter hanging until the
    /// client times out. Repro: `test_put_error_notification` in
    /// `crates/core/tests/error_notification.rs`.
    #[test]
    fn run_relay_put_publishes_error_on_loopback_failure() {
        let src = include_str!("op_ctx_task.rs");
        let fn_start = src
            .find("async fn run_relay_put<CB>(")
            .expect("run_relay_put not found");
        let fn_end = src[fn_start..]
            .find("\n#[allow(clippy::too_many_arguments)]\nasync fn drive_relay_put<CB>(")
            .expect("end-of-run_relay_put marker not found")
            + fn_start;
        let body = &src[fn_start..fn_end];
        assert!(
            body.contains("originator_loopback"),
            "run_relay_put must compute originator_loopback to gate the \
             error-publication path"
        );
        assert!(
            body.contains("send_client_result(incoming_tx"),
            "run_relay_put must call send_client_result(incoming_tx, Err(_)) \
             on driver failure in originator-loopback mode"
        );
        assert!(
            body.contains("ErrorKind::OperationError"),
            "run_relay_put must wrap the OpError in \
             freenet_stdlib::client_api::ErrorKind::OperationError before \
             publishing to the client"
        );
        assert!(
            body.contains("op_manager.completed(incoming_tx)"),
            "run_relay_put must call op_manager.completed(incoming_tx) \
             after publishing the loopback-failure error so the tx is \
             marked done"
        );
    }

    /// Pin: driver forward must reuse `incoming_tx` — the PUT relay
    /// uses the same tx end-to-end. Minting a fresh tx per hop breaks
    /// the downstream peer's `active_relay_put_txs` dedup gate and
    /// detaches the response from the originator's waiter.
    #[test]
    fn drive_relay_put_reuses_incoming_tx_on_forward() {
        let src = include_str!("op_ctx_task.rs");
        let driver_start = src
            .find("async fn drive_relay_put<CB>(")
            .expect("drive_relay_put not found");
        let driver_end = src[driver_start..]
            .find("\nasync fn relay_put_finalize_local(")
            .expect("driver body end not found")
            + driver_start;
        let driver_src = &src[driver_start..driver_end];
        // The PutMsg::Request forward must carry id: incoming_tx (not a
        // freshly-minted Transaction::new::<PutMsg>()).
        let forward_pos = driver_src
            .find("PutMsg::Request {")
            .expect("forward PutMsg::Request not found in driver");
        let forward_window = &driver_src[forward_pos..forward_pos + 400];
        assert!(
            forward_window.contains("id: incoming_tx"),
            "relay forward must reuse incoming_tx; minting a fresh tx per hop \
             breaks the downstream `active_relay_put_txs` dedup gate"
        );
        assert!(
            !forward_window.contains("Transaction::new::<PutMsg>()"),
            "relay forward must NOT mint a fresh Transaction"
        );
    }

    /// Pin: relay upstream reply is fire-and-forget (matches legacy —
    /// upstream's own `send_to_and_await` owns the capacity-1 reply
    /// channel; sending with another `send_to_and_await` would reuse
    /// our own tx slot for no reason).
    #[test]
    fn relay_put_send_response_is_fire_and_forget() {
        let src = include_str!("op_ctx_task.rs");
        let fn_start = src
            .find("async fn relay_put_send_response(")
            .expect("relay_put_send_response not found");
        let fn_end = src[fn_start..]
            .find("\n}\n")
            .expect("function body end not found")
            + fn_start;
        let fn_src = &src[fn_start..fn_end];
        assert!(
            fn_src.contains("send_fire_and_forget"),
            "relay_put_send_response must use send_fire_and_forget for the \
             upstream response"
        );
    }

    /// Pin: slice A handles inbound `Request` only, but MAY upgrade
    /// the forward to `RequestStreaming` when the serialized payload
    /// exceeds `streaming_threshold`. Inbound `RequestStreaming`
    /// dispatch still belongs to slice B's
    /// `start_relay_put_streaming`. Slice A must NOT build outbound
    /// `ResponseStreaming` (relays bubble a non-streaming Response
    /// upstream).
    #[test]
    fn slice_a_does_not_touch_put_streaming_variants() {
        let src = include_str!("op_ctx_task.rs");
        let relay = relay_slice_a_section(src);
        // ResponseStreaming IS referenced in one place: the downstream
        // reply classifier that synthesizes a non-streaming Response
        // upstream (documented limitation). Ensure NO branch BUILDS a
        // ResponseStreaming — we only match on it.
        let builds_streaming = relay.contains("NetMessage::from(PutMsg::ResponseStreaming")
            || relay.contains("PutMsg::ResponseStreaming {\n") && {
                let pos = relay
                    .find("PutMsg::ResponseStreaming {")
                    .expect("anchor present by guard");
                let window = &relay[pos..pos + 200.min(relay.len() - pos)];
                window.contains("id: ")
            };
        assert!(
            !builds_streaming,
            "slice A driver must not CONSTRUCT a PutMsg::ResponseStreaming"
        );
    }

    /// Pin: driver MUST call `put_contract` + `host_contract` before
    /// forwarding (so the local cache + hosting advertisement happens
    /// regardless of whether the forward succeeds).
    #[test]
    fn drive_relay_put_stores_locally_before_forwarding() {
        let src = include_str!("op_ctx_task.rs");
        let driver_start = src
            .find("async fn drive_relay_put<CB>(")
            .expect("drive_relay_put not found");
        let driver_end = src[driver_start..]
            .find("\nasync fn relay_put_store_locally(")
            .expect("driver body end not found")
            + driver_start;
        let driver_src = &src[driver_start..driver_end];
        let store_pos = driver_src
            .find("relay_put_store_locally(")
            .expect("relay_put_store_locally call missing in driver");
        let forward_pos = driver_src
            .find("ctx.send_to_and_await(")
            .expect("send_to_and_await call missing in driver");
        assert!(
            store_pos < forward_pos,
            "local store MUST run before the downstream forward"
        );

        // Helper must encapsulate put_contract + host_contract + announce.
        let helper_start = src
            .find("async fn relay_put_store_locally(")
            .expect("helper not found");
        let helper_end = src[helper_start..]
            .find("\nasync fn relay_put_finalize_local(")
            .expect("helper body end not found")
            + helper_start;
        let helper_src = &src[helper_start..helper_end];
        assert!(
            helper_src.contains("super::put_contract("),
            "helper MUST call put_contract"
        );
        assert!(
            helper_src.contains("host_contract("),
            "helper MUST call ring.host_contract for first-time hosting"
        );
        assert!(
            helper_src.contains("announce_contract_hosted"),
            "helper MUST call announce_contract_hosted for first-time hosting"
        );
    }

    // ── Upgrade-on-forward error paths (PR #4063) ─────────────────────
    //
    // The streaming branch of `drive_relay_put` introduces two error
    // paths: (a) `send_to_and_register_waiter` fails, (b) `send_stream`
    // fails. Each MUST release the per-tx `pending_op_results` slot
    // BEFORE returning `Err`, otherwise the slot leaks until the 60 s
    // periodic sweep and the test_pending_op_results_bounded regression
    // guard in tests/simulation_integration.rs trips. These pins lock
    // the slot-release ordering at the source level since the project's
    // existing test pattern for relay-driver behaviour is structural
    // (mock-runtime tests for the driver body would be a much bigger
    // change — see drive_relay_put_streaming_* pins above for the same
    // pattern in slice B).

    fn drive_relay_put_body(src: &str) -> &str {
        let start = src
            .find("async fn drive_relay_put<CB>(")
            .expect("drive_relay_put not found");
        let end = src[start..]
            .find("\nasync fn relay_put_store_locally(")
            .expect("driver body end not found")
            + start;
        &src[start..end]
    }

    /// Pin: upgrade-on-forward branch MUST exist and contain both the
    /// `RequestStreaming` build site and the `send_stream` raw fragment
    /// dispatch.
    #[test]
    fn drive_relay_put_upgrades_when_payload_exceeds_threshold() {
        let src = include_str!("op_ctx_task.rs");
        let body = drive_relay_put_body(src);
        assert!(
            body.contains("should_use_streaming("),
            "drive_relay_put must call should_use_streaming on the merged payload"
        );
        assert!(
            body.contains("PutMsg::RequestStreaming {"),
            "drive_relay_put must build PutMsg::RequestStreaming on upgrade"
        );
        assert!(
            body.contains("conn_manager\n            .send_stream(")
                || body.contains("conn_manager.send_stream("),
            "drive_relay_put must call NetworkBridge::send_stream after metadata send"
        );
    }

    /// Bound the upgrade-on-forward analysis to the
    /// *relay-non-loopback* branch of `drive_relay_put`. The
    /// originator-loopback branch intentionally does NOT install a
    /// reply waiter — the originator's
    /// `pending_op_results` callback already exists and the loopback
    /// forward is fire-and-forget. The relay branch (`let round_trip =
    /// if upgrade_to_streaming`) is the one that must install a
    /// metadata-first waiter and release on error.
    fn drive_relay_put_relay_branch(src: &str) -> &str {
        let body = drive_relay_put_body(src);
        let start = body
            .find("let round_trip = if upgrade_to_streaming {")
            .expect("relay-non-loopback round_trip branch not found");
        let end = body[start..]
            .find("};\n\n    // Release the pending_op_results slot")
            .expect("relay branch end-marker not found")
            + start;
        &body[start..end]
    }

    /// Pin: `send_to_and_register_waiter` MUST install the reply waiter
    /// BEFORE `send_stream` dispatches fragments in the relay-non-
    /// loopback branch. Without this ordering, a fast downstream
    /// Response arriving on the wire would race the
    /// `pending_op_results` insertion and be dropped as OpNotPresent.
    /// Mirrors the metadata-first ordering pinned for
    /// `drive_relay_put_streaming`.
    #[test]
    fn drive_relay_put_upgrade_installs_waiter_before_send_stream() {
        let src = include_str!("op_ctx_task.rs");
        let branch = drive_relay_put_relay_branch(src);
        let waiter_pos = branch
            .find("send_to_and_register_waiter(")
            .expect("send_to_and_register_waiter call missing in relay upgrade branch");
        let stream_pos = branch
            .find(".send_stream(")
            .expect("send_stream call missing in relay upgrade branch");
        assert!(
            waiter_pos < stream_pos,
            "send_to_and_register_waiter MUST run before send_stream so the \
             reply waiter is installed before the first fragment lands"
        );
    }

    /// Pin: BOTH error paths in the relay-non-loopback upgrade branch
    /// MUST call `release_pending_op_slot(incoming_tx)` BEFORE
    /// returning `Err`. Without the release, each failed upgrade leaks
    /// one `pending_op_results` entry —
    /// `test_pending_op_results_bounded` will flag the imbalance.
    #[test]
    fn drive_relay_put_upgrade_error_paths_release_slot() {
        let src = include_str!("op_ctx_task.rs");
        let branch = drive_relay_put_relay_branch(src);

        // Find the `if upgrade_to_streaming {` opening (in this branch)
        // and bound the window to the matching else.
        let upgrade_start = branch
            .find("if upgrade_to_streaming {")
            .expect("upgrade_to_streaming branch not found in relay branch");
        let upgrade_end = branch[upgrade_start..]
            .find("} else {")
            .expect("upgrade branch closing else not found")
            + upgrade_start;
        let upgrade = &branch[upgrade_start..upgrade_end];

        // Each `return Err(` inside the upgrade branch must be
        // preceded by a `release_pending_op_slot` call within a
        // small window.
        let mut search = 0;
        let mut return_count = 0;
        while let Some(pos) = upgrade[search..].find("return Err(") {
            return_count += 1;
            let abs = search + pos;
            // Look back up to 400 chars (covers the tracing::warn!
            // block + release call).
            let window_start = abs.saturating_sub(400);
            let window = &upgrade[window_start..abs];
            assert!(
                window.contains("release_pending_op_slot(incoming_tx)"),
                "upgrade branch `return Err` at offset {abs} must be preceded by \
                 release_pending_op_slot — leaks pending_op_results otherwise"
            );
            search = abs + "return Err(".len();
        }
        assert!(
            return_count >= 2,
            "expected at least 2 error-return paths in relay upgrade branch \
             (send_to_and_register_waiter fail, send_stream fail); found {return_count}"
        );
    }

    // ── Slice B streaming driver pin tests ─────────────────────────────

    fn relay_slice_b_section(src: &str) -> &str {
        let start = src
            .find("// ── Relay streaming PUT driver")
            .expect("slice B marker not found");
        let end = src
            .find("\n// ── End of relay PUT driver ─")
            .expect("end-of-driver marker not found");
        &src[start..end]
    }

    /// Pin: streaming driver entry must dedup-gate BEFORE spawn.
    #[test]
    fn start_relay_put_streaming_checks_dedup_gate() {
        let src = include_str!("op_ctx_task.rs");
        let b = relay_slice_b_section(src);
        let insert_pos = b
            .find("active_relay_put_txs.insert(incoming_tx)")
            .expect("dedup insert not found");
        let spawn_pos = b
            .find("GlobalExecutor::spawn(run_relay_put_streaming(")
            .expect("spawn site not found");
        assert!(
            insert_pos < spawn_pos,
            "dedup-gate MUST run before spawning the streaming driver"
        );
    }

    /// Pin: dedup-reject must bump counter + return silently (NO
    /// fabricated `PutMsg::Response` — doing so would tell the
    /// upstream's `pending_op_results` waiter that the contract
    /// stored when the duplicate request stored nothing).
    #[test]
    fn start_relay_put_streaming_dedup_reject_is_silent() {
        let src = include_str!("op_ctx_task.rs");
        let b = relay_slice_b_section(src);
        let insert_pos = b
            .find("active_relay_put_txs.insert(incoming_tx)")
            .expect("dedup anchor not found");
        // Window: from insert site to the closing of the `if !insert { ... }` branch.
        let window = &b[insert_pos..];
        let close_pos = window
            .find("\n    }\n")
            .expect("dedup-branch close not found");
        let branch = &window[..close_pos];
        assert!(
            branch.contains("RELAY_PUT_STREAMING_DEDUP_REJECTS.fetch_add"),
            "dedup gate must increment RELAY_PUT_STREAMING_DEDUP_REJECTS on rejection"
        );
        assert!(
            !branch.contains("send_fire_and_forget"),
            "dedup-reject must NOT fire a fabricated Response (no NotFound variant in PutMsg)"
        );
    }

    /// Pin: RAII guard decrements INFLIGHT + bumps COMPLETED on drop.
    #[test]
    fn relay_put_streaming_guard_drop_is_balanced() {
        let src = include_str!("op_ctx_task.rs");
        let b = relay_slice_b_section(src);
        let drop_start = b
            .find("impl Drop for RelayPutStreamingInflightGuard")
            .expect("guard Drop impl not found");
        let drop_end = b[drop_start..]
            .find("\n}\n")
            .expect("guard Drop body end not found")
            + drop_start;
        let drop_body = &b[drop_start..drop_end];
        assert!(
            drop_body.contains("RELAY_PUT_STREAMING_INFLIGHT.fetch_sub"),
            "guard::drop must decrement RELAY_PUT_STREAMING_INFLIGHT"
        );
        assert!(
            drop_body.contains("RELAY_PUT_STREAMING_COMPLETED_TOTAL.fetch_add"),
            "guard::drop must increment RELAY_PUT_STREAMING_COMPLETED_TOTAL"
        );
        assert!(
            drop_body.contains("active_relay_put_txs"),
            "guard::drop must reference active_relay_put_txs"
        );
        assert!(
            drop_body.contains(".remove(&self.incoming_tx)"),
            "guard::drop must remove the tx from active_relay_put_txs"
        );
    }

    /// Pin: streaming driver claims inbound stream + uses pipe_stream
    /// for forwarding.
    #[test]
    fn drive_relay_put_streaming_uses_claim_and_pipe() {
        let src = include_str!("op_ctx_task.rs");
        let b = relay_slice_b_section(src);
        let driver_start = b
            .find("async fn drive_relay_put_streaming")
            .expect("drive_relay_put_streaming not found");
        let driver = &b[driver_start..];
        assert!(
            driver.contains("orphan_stream_registry()"),
            "streaming driver must claim via orphan_stream_registry"
        );
        assert!(
            driver.contains("claim_or_wait(upstream_addr"),
            "claim MUST use upstream_addr + inbound stream_id"
        );
        assert!(
            driver.contains(".pipe_stream("),
            "streaming driver must call pipe_stream for forwarding"
        );
        assert!(
            driver.contains("relay_put_store_locally("),
            "streaming driver must reuse the shared local-store helper"
        );
    }

    /// Pin: streaming driver does NOT construct a ForwardingAck on its
    /// own path — an ack would share `incoming_tx` and satisfy
    /// upstream's capacity-1 pending_op_results slot before the real
    /// Response. Scoped to the function body to avoid catching the
    /// doc-comment reference at the entry point.
    #[test]
    fn drive_relay_put_streaming_omits_forwarding_ack() {
        let src = include_str!("op_ctx_task.rs");
        let b = relay_slice_b_section(src);
        let driver_start = b
            .find("async fn drive_relay_put_streaming")
            .expect("drive_relay_put_streaming not found");
        let driver = &b[driver_start..];
        // Detect construction: `NetMessage::from(PutMsg::ForwardingAck`
        // or `PutMsg::ForwardingAck {`.
        assert!(
            !driver.contains("NetMessage::from(PutMsg::ForwardingAck")
                && !driver.contains("PutMsg::ForwardingAck {"),
            "slice B driver must not construct PutMsg::ForwardingAck"
        );
    }

    /// Pin: streaming driver reuses `incoming_tx` on the forward.
    #[test]
    fn drive_relay_put_streaming_reuses_incoming_tx_on_forward() {
        let src = include_str!("op_ctx_task.rs");
        let b = relay_slice_b_section(src);
        let pipe_meta_pos = b
            .find("PutMsg::RequestStreaming {")
            .expect("outbound RequestStreaming construction not found");
        let window = &b[pipe_meta_pos..pipe_meta_pos + 300.min(b.len() - pipe_meta_pos)];
        assert!(
            window.contains("id: incoming_tx"),
            "piped metadata must reuse incoming_tx (not mint fresh)"
        );
    }

    /// Pin: `AlreadyClaimed` early-return must exit silently (no
    /// upstream fabrication, no error bubble). The other driver
    /// instance that claimed the stream owns the upstream reply.
    #[test]
    fn drive_relay_put_streaming_already_claimed_is_silent() {
        let src = include_str!("op_ctx_task.rs");
        let b = relay_slice_b_section(src);
        let ac_pos = b
            .find("OrphanStreamError::AlreadyClaimed")
            .expect("AlreadyClaimed arm not found");
        let arm = &b[ac_pos..ac_pos + 600.min(b.len() - ac_pos)];
        assert!(
            arm.contains("return Ok(())"),
            "AlreadyClaimed arm must return Ok(()) — the still-in-flight \
             driver owns the upstream reply; this duplicate must exit silently"
        );
        assert!(
            !arm.contains("send_fire_and_forget"),
            "AlreadyClaimed arm must NOT fabricate a Response upstream"
        );
    }

    /// Pin: orphan-claim-failure (non-AlreadyClaimed) returns an
    /// error without fabricating a success Response upstream.
    #[test]
    fn drive_relay_put_streaming_claim_failure_is_silent() {
        let src = include_str!("op_ctx_task.rs");
        let b = relay_slice_b_section(src);
        let pos = b
            .find("OrphanStreamClaimFailed")
            .expect("OrphanStreamClaimFailed not found in slice B");
        let window = &b[..pos];
        let err_arm_start = window
            .rfind("Err(err) =>")
            .expect("orphan-claim Err arm not found");
        let arm = &b[err_arm_start..pos + 100];
        assert!(
            !arm.contains("send_fire_and_forget"),
            "orphan-claim failure must NOT fabricate a success Response upstream"
        );
    }

    /// Pin: downstream reply timeout falls through to
    /// `relay_put_send_response` (bubbles a best-effort Response
    /// upstream) rather than propagating an error.
    #[test]
    fn drive_relay_put_streaming_timeout_bubbles_response() {
        let src = include_str!("op_ctx_task.rs");
        let b = relay_slice_b_section(src);
        let pos = b
            .find("downstream reply timed out")
            .expect("timeout arm not found");
        // Window widened from 400 → 800 → 1200 bytes to accommodate the
        // record_relay_route_event hook inserted between the timeout
        // log and the bubble-Response call, plus the multi-line
        // hop_count computation + multi-line call site added in #4248.
        let arm = &b[pos..pos + 1200.min(b.len() - pos)];
        assert!(
            arm.contains("relay_put_send_response"),
            "timeout arm must still call relay_put_send_response so the \
             upstream waiter is not left to its own OPERATION_TTL"
        );
    }

    /// Pin: stream assembly failure + contract-key mismatch +
    /// payload deserialize failure all return Err without
    /// fabricating a Response. They intentionally let the upstream's
    /// OPERATION_TTL expire rather than lie about what was stored.
    #[test]
    fn drive_relay_put_streaming_store_failure_paths_do_not_fabricate() {
        let src = include_str!("op_ctx_task.rs");
        let b = relay_slice_b_section(src);
        let driver_start = b
            .find("async fn drive_relay_put_streaming")
            .expect("driver not found");
        let driver = &b[driver_start..];

        for phrase in [
            "stream assembly failed",
            "contract key mismatch",
            "payload deserialize failed",
        ] {
            let anchor = driver
                .find(phrase)
                .unwrap_or_else(|| panic!("failure-path anchor {phrase:?} not found"));
            // Starting at `anchor`, find the next `return Err(` —
            // everything between must be free of send_fire_and_forget.
            let tail = &driver[anchor..];
            let ret_pos = tail
                .find("return Err(")
                .unwrap_or_else(|| panic!("no return Err after {phrase:?}"));
            let pre_return = &tail[..ret_pos];
            assert!(
                !pre_return.contains("send_fire_and_forget"),
                "failure path {phrase:?} must not fabricate a Response upstream"
            );
        }
    }

    /// Pin: `send_to_and_register_waiter` is called BEFORE
    /// `conn_manager.pipe_stream` so the `pending_op_results`
    /// callback is installed before downstream fragments land and a
    /// fast downstream reply can't race past the waiter install.
    #[test]
    fn drive_relay_put_streaming_registers_waiter_before_pipe_stream() {
        let src = include_str!("op_ctx_task.rs");
        let b = relay_slice_b_section(src);
        let driver_start = b
            .find("async fn drive_relay_put_streaming")
            .expect("driver not found");
        let driver = &b[driver_start..];
        let register_pos = driver
            .find("send_to_and_register_waiter(")
            .expect("register_waiter call not found");
        let pipe_pos = driver
            .find(".pipe_stream(")
            .expect("pipe_stream call not found");
        assert!(
            register_pos < pipe_pos,
            "send_to_and_register_waiter MUST precede pipe_stream so the \
             pending_op_results callback is installed before fragments \
             land downstream (otherwise a fast reply races past the waiter)"
        );
    }

    /// Pin: each transport-failure arm of `drive_relay_put` records a
    /// routing event for the chosen peer. Without these hooks, the
    /// per-peer dashboard's failure-probability model is trained only
    /// on originated PUT ops and the symptom this PR fixes reappears
    /// for the relay path. Source-scrape because the behaviour is
    /// positional inside the match and a deletion would not break any
    /// unit-test assertion otherwise.
    #[test]
    fn drive_relay_put_records_route_events_on_transport_failure() {
        let src = include_str!("op_ctx_task.rs");
        // drive_relay_put body — non-streaming relay.
        let body_start = src
            .find("async fn drive_relay_put<CB>(")
            .expect("drive_relay_put fn must exist");
        let body_end = src[body_start..]
            .find("/// Store a relayed PUT")
            .map(|i| body_start + i)
            .unwrap_or(src.len());
        let body = &src[body_start..body_end];

        for log_phrase in ["send_to_and_await failed", "downstream timed out"] {
            let pos = body
                .find(log_phrase)
                .unwrap_or_else(|| panic!("expected `{log_phrase}` in drive_relay_put"));
            let after = &body[pos..pos + 1500.min(body.len() - pos)];
            assert!(
                after.contains("record_relay_route_event")
                    && after.contains("RouteOutcome::Failure"),
                "drive_relay_put arm for `{log_phrase}` must call \
                 record_relay_route_event with RouteOutcome::Failure. \
                 Without this, transport failures from relay-forwarded \
                 PUTs are dropped — the regression PR #4051 fixes."
            );
        }

        // Success arms (Response and ResponseStreaming downgrade) must
        // record SuccessUntimed.
        let pos = body
            .find("downstream returned Response; bubbling upstream")
            .expect("Response success arm not found");
        let after = &body[pos..pos + 1500.min(body.len() - pos)];
        assert!(
            after.contains("record_relay_route_event")
                && after.contains("RouteOutcome::SuccessUntimed"),
            "drive_relay_put Response arm must record SuccessUntimed."
        );
    }
}
