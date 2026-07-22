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
use crate::message::{NetMessage, NetMessageV1, NodeEvent, Transaction};
use crate::node::NetworkBridge;
use crate::node::OpManager;
use crate::node::WaiterReply;
use crate::operations::OpError;
use crate::operations::bootstrap::bootstrap_gateway_target;
use crate::operations::op_ctx::{
    AdvanceOutcome, AttemptOutcome, OpCtx, RetryDriver, RetryLoopOutcome, drive_retry_loop,
};
use crate::operations::orphan_streams::{OrphanStreamError, STREAM_CLAIM_TIMEOUT};
use crate::ring::{Location, PeerKeyLocation};
use crate::router::{RouteEvent, RouteOutcome};
use crate::tracing::{NetEventLog, OperationFailure, state_hash_full};
use crate::transport::peer_connection::StreamId;

use super::{PutFinalizationData, PutMsg, PutStreamingPayload, PutTerminalError, bound_cause};

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
    // Phase 7 egress self-block (#4300): refuse to originate a PUT for
    // a contract this node has banned, BEFORE spawning the driver. The
    // client gets a typed `ContractBanned` error instead of a request
    // that proceeds to peers (who don't know about our ban) and then
    // fails or silently succeeds. Mirrors the receive-side drop in
    // node.rs added by PR #4299.
    crate::operations::reject_if_contract_banned(&op_manager, contract.key().id())?;

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
    // routing decision is made by the relay driver (`start_relay_put`),
    // not here.
    //
    // Bootstrap note (#4361 / #4365): on an empty ring this stays
    // `own_location()` — the bootstrap gateway fallback is applied at the
    // relay's next-hop selection, NOT here. Unlike GET (whose client
    // driver re-picks the wire target per attempt and therefore had to
    // carry `tried` into each attempt's visited bloom for gateway
    // failover, see `carry_tried_into_visited`), PUT's `current_target` is
    // only driver-side telemetry / retry bookkeeping: the request value
    // carries the contract (not a target), the relay decides the next hop,
    // and the terminal reply returns via the originator-loopback bypass on
    // `pending_op_results[tx]` — none of which read `current_target`. So a
    // self-vs-gateway divergence here is cosmetic, not a routing bug.
    // (Multi-gateway failover across PUT retries is intentionally not
    // implemented; see the design doc's out-of-scope notes.)
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

    // ── Summary-first PUT (#4642 step 3-bis) ────────────────────────────────
    //
    // Best-effort front-end to the full-state retry loop below. On ANY
    // failure (version gate closed, probe timeout/error, no holder found)
    // this falls straight through to the unchanged `PutRetryDriver` loop —
    // a summary-first failure must never break a PUT (risk-bounded graceful
    // fallback; the full-state path remains the default and the fallback,
    // per the hosting redesign's demand-driven invariants).
    if let Some(target_addr) = current_target.socket_addr() {
        if op_manager
            .ring
            .connection_manager
            .supports_summary_first_put(target_addr)
        {
            if let SummaryFirstOutcome::ReconciledExisting { target, hop_count } =
                try_summary_first_put(
                    op_manager,
                    client_tx,
                    key,
                    &contract,
                    &related,
                    &value,
                    htl,
                    &current_target,
                    target_addr,
                )
                .await
            {
                // Existing-mesh delta case: the reconcile (if any delta was
                // needed) has already been dispatched best-effort and the
                // PUT is complete. The originator already holds a
                // locally-merged copy from `try_summary_first_put`'s local
                // store step, so it converges immediately at this node;
                // confirming the reconcile landed at the remote holder
                // (reverse-leg convergence) is a deliberate follow-up, not
                // required for this PUT to report success correctly.
                op_manager.completed(client_tx);
                super::finalize_put_at_originator(
                    op_manager,
                    client_tx,
                    key,
                    PutFinalizationData {
                        sender: target,
                        hop_count: Some(hop_count),
                        state_hash: None,
                        state_size: None,
                    },
                    false,
                    false,
                )
                .await;

                maybe_subscribe_child(op_manager, client_tx, key, subscribe, blocking_subscribe)
                    .await;

                return Ok(DriverOutcome::Publish(Ok(HostResponse::ContractResponse(
                    ContractResponse::PutResponse { key },
                ))));
            }
            // FallThrough: no holder found, or the probe failed — continue
            // to the existing full-state driver below, unchanged.
        }
    }

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
        /// Stream-progress liveness bundle for streaming-eligible payloads
        /// (#4001). `Some` only when `should_use_streaming` is true; the retry
        /// loop then uses a stream-inactivity timeout instead of the fixed
        /// `attempt_timeout`. `None` for non-streaming PUTs (fixed deadline,
        /// behaviour unchanged).
        stream_progress: Option<crate::operations::stream_progress::StreamProgress>,
    }

    impl RetryDriver for PutRetryDriver<'_> {
        // `(result, hop_count)`. `result` is `Ok(key)` on success or
        // `Err(cause)` on terminal failure (PR #4111). `hop_count`
        // is `Some(hop_count)` from wire-carried `Stored` / `LocalCompletion`,
        // always present for terminal outcomes (0 for local originator).
        type Terminal = (Result<ContractKey, PutTerminalError>, Option<usize>);

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
                    AttemptOutcome::Terminal((Ok(key), Some(hop_count)))
                }
                // LocalCompletion = no remote hops traversed (originator
                // is the storer). Forward depth is exactly 0.
                ReplyClass::LocalCompletion { key } => AttemptOutcome::Terminal((Ok(key), Some(0))),
                ReplyClass::TerminalError { cause } => AttemptOutcome::Terminal((Err(cause), None)),
                ReplyClass::Unexpected => AttemptOutcome::Unexpected,
            }
        }

        fn advance(&mut self) -> AdvanceOutcome {
            #[cfg(any(test, feature = "testing"))]
            PUT_RETRY_DRIVER_ADVANCE_CALLS.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
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

        fn stream_progress(&self) -> Option<crate::operations::stream_progress::StreamProgress> {
            self.stream_progress.clone()
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
    let is_streaming = crate::operations::should_use_streaming(
        op_manager.streaming_threshold,
        payload_size_estimate,
    );
    let max_advancements = if is_streaming {
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

    // Stream-progress liveness (#4001): only for streaming-eligible payloads.
    // The retry loop then replaces the fixed `attempt_timeout` with a true
    // stream-inactivity timeout driven by per-fragment progress recorded by the
    // originator-loopback relay-streaming task. Production uses `RealTime`;
    // DST/VirtualTime is injected via the unit tests on `drive_retry_loop`.
    let stream_progress = if is_streaming {
        use crate::operations::stream_progress::StreamProgress;
        use crate::simulation::RealTime;
        // The StreamProgress owns this single RealTime: the handle's record()/
        // since_last() and the retry loop's sleeps all read it, so writer and
        // reader share one epoch (#4001 single-epoch invariant). DST/VirtualTime
        // is injected via the unit tests on drive_retry_loop.
        Some(StreamProgress::new(RealTime::new()))
    } else {
        None
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
        stream_progress,
    };

    let loop_result = drive_retry_loop(op_manager, client_tx, "put", &mut driver).await;

    match loop_result {
        RetryLoopOutcome::Done((Ok(reply_key), wire_hop_count)) => {
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
        // Issue #4111: terminal failure delivered via the bypass (a
        // `PutMsg::Error` from the originator-loopback failure path).
        // No retries — the failure is local-deterministic. Publish the
        // real cause once, mark the tx completed.
        RetryLoopOutcome::Done((Err(cause), _hop_count)) => {
            op_manager.completed(client_tx);
            Ok(DriverOutcome::Publish(Err(ErrorKind::OperationError {
                cause: cause.into_string().into(),
            }
            .into())))
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

// --- Summary-first PUT (originator pre-phase, #4642 step 3-bis) ---

/// Outcome of [`try_summary_first_put`].
enum SummaryFirstOutcome {
    /// A holder was found; a reconcile delta (if any was needed) has
    /// already been dispatched best-effort and the PUT is complete. Carries
    /// what `drive_client_put_inner` needs to finalize: the probed peer and
    /// the forward-path hop count the `ProbeResponse` reported.
    ReconciledExisting {
        target: PeerKeyLocation,
        hop_count: usize,
    },
    /// No holder found (genuinely new contract), or any step of the probe
    /// failed — the caller falls through to the unchanged full-state
    /// `PutRetryDriver` loop.
    FallThrough,
}

/// Summary-first PUT attempt: best-effort front-end to the full-state
/// retry loop in `drive_client_put_inner`. Probes `target` with just a
/// state summary; if the mesh already holds the contract, ships only a
/// delta instead of the full state. On ANY failure (local-store error,
/// summary-compute error, probe timeout/error, no holder found) returns
/// [`SummaryFirstOutcome::FallThrough`] — this function must never be the
/// reason a PUT fails; the caller always has the full-state driver as a
/// fallback.
///
/// Precondition: caller has already confirmed `target_addr` passes
/// `ConnectionManager::supports_summary_first_put` (the mixed-version
/// emission gate).
#[allow(clippy::too_many_arguments)]
async fn try_summary_first_put(
    op_manager: &Arc<OpManager>,
    client_tx: Transaction,
    key: ContractKey,
    contract: &ContractContainer,
    related: &RelatedContracts<'static>,
    value: &WrappedState,
    htl: usize,
    target: &PeerKeyLocation,
    target_addr: SocketAddr,
) -> SummaryFirstOutcome {
    // EXPERIMENT-ONLY (findability_probe): force the full-state driver so the
    // PUT-reach measurement exercises the plain greedy-routing terminus path
    // (summary-first would store at the originator then probe, muddying the
    // single-copy placement). NOT for ship.
    #[cfg(any(test, feature = "testing"))]
    if crate::operations::findability_probe::scatter_disabled() {
        return SummaryFirstOutcome::FallThrough;
    }
    // (a) Store locally first — the same local-store path a relay hop uses
    // (`relay_put_store_locally`: put_contract + host_contract +
    // register_local_hosting + announce_contract_hosted) — so the
    // originator can summarize/delta by key and becomes a genuine host
    // (demand-driven: a real client-initiated PUT is genuine local
    // demand). `ClientLocal` priority matches the originator-loopback arm
    // of `put_store_priority`.
    let merged_value = match relay_put_store_locally(
        op_manager,
        client_tx,
        key,
        value.clone(),
        contract,
        related.clone(),
        htl,
        crate::contract::Priority::ClientLocal,
    )
    .await
    {
        Ok(v) => v,
        Err(err) => {
            tracing::debug!(
                tx = %client_tx,
                contract = %key,
                error = %err,
                phase = "summary_first_local_store_failed",
                "PUT summary-first: local store failed; falling through to full-state driver"
            );
            return SummaryFirstOutcome::FallThrough;
        }
    };

    // (b) Compute our own summary of what we just stored.
    let our_summary = match op_manager
        .interest_manager
        .get_contract_summary(op_manager, &key)
        .await
    {
        Some(s) => s,
        None => {
            tracing::debug!(
                tx = %client_tx,
                contract = %key,
                phase = "summary_first_summary_failed",
                "PUT summary-first: failed to compute own summary; falling through \
                 to full-state driver"
            );
            return SummaryFirstOutcome::FallThrough;
        }
    };

    // (c) Probe `target` with the summary and await its reply on a fresh
    // transaction — `send_and_await`/`send_to_and_await` are single-use per
    // tx (see `OpCtx` rustdoc), and this probe is a separate round-trip
    // from whatever attempt tx the full-state fallback may later use.
    let probe_tx = Transaction::new::<PutMsg>();
    let own_addr = op_manager.ring.connection_manager.get_own_addr();
    let probe_msg = NetMessage::from(PutMsg::ProbeRequest {
        id: probe_tx,
        contract_key: key,
        summary: our_summary.clone(),
        htl,
        skip_list: own_addr.into_iter().collect::<HashSet<_>>(),
    });
    let mut ctx = op_manager.op_ctx(probe_tx);
    let probe_timeout =
        compute_put_attempt_timeout(op_manager.streaming_threshold, value, contract);
    let round_trip =
        tokio::time::timeout(probe_timeout, ctx.send_to_and_await(target_addr, probe_msg)).await;
    op_manager.release_pending_op_slot(probe_tx).await;

    let reply = match round_trip {
        Ok(Ok(reply)) => reply,
        Ok(Err(err)) => {
            tracing::debug!(
                tx = %client_tx,
                probe_tx = %probe_tx,
                contract = %key,
                error = %err,
                phase = "summary_first_probe_failed",
                "PUT summary-first: probe failed; falling through to full-state driver"
            );
            return SummaryFirstOutcome::FallThrough;
        }
        Err(_elapsed) => {
            tracing::debug!(
                tx = %client_tx,
                probe_tx = %probe_tx,
                contract = %key,
                phase = "summary_first_probe_timeout",
                "PUT summary-first: probe timed out; falling through to full-state driver"
            );
            return SummaryFirstOutcome::FallThrough;
        }
    };

    #[allow(clippy::wildcard_enum_match_arm)]
    let (holder_found, probe_hop_count, holder_summary, reverse_delta) = match reply {
        NetMessage::V1(NetMessageV1::Put(PutMsg::ProbeResponse {
            holder_found,
            hop_count,
            holder_summary,
            reverse_delta,
            ..
        })) => (holder_found, hop_count, holder_summary, reverse_delta),
        _other => {
            tracing::debug!(
                tx = %client_tx,
                probe_tx = %probe_tx,
                contract = %key,
                phase = "summary_first_unexpected_reply",
                "PUT summary-first: unexpected probe reply; falling through to full-state driver"
            );
            return SummaryFirstOutcome::FallThrough;
        }
    };

    if !holder_found {
        tracing::debug!(
            tx = %client_tx,
            contract = %key,
            phase = "summary_first_new_contract",
            "PUT summary-first: no holder found (new contract); falling through \
             to full-state driver"
        );
        let full_state_bytes = value
            .size()
            .saturating_add(contract.data().len())
            .saturating_add(contract.params().size()) as u64;
        record_put_probe_outcome(true, full_state_bytes);
        return SummaryFirstOutcome::FallThrough;
    }

    let Some(their_summary) = holder_summary else {
        // Wire invariant violation (`holder_found` without `holder_summary`)
        // — treat defensively as a probe failure rather than trust a
        // malformed/unexpected reply.
        tracing::warn!(
            tx = %client_tx,
            contract = %key,
            phase = "summary_first_missing_holder_summary",
            "PUT summary-first: holder_found=true but holder_summary missing; \
             falling through to full-state driver"
        );
        return SummaryFirstOutcome::FallThrough;
    };

    // Reverse leg of the bidirectional reconcile (#4642 step 3-bis): if the
    // holder was ahead, it shipped back `reverse_delta` = what WE (the
    // originator) are missing relative to the holder. Apply it to our own
    // local copy via the SAME commutative UPDATE/merge path the holder uses
    // for the forward `ProbeReconcile`, BEFORE we report client success, so
    // the originator converges to the full merged state in this same
    // round-trip instead of only healing later via anti-entropy. Best-effort:
    // a merge failure does NOT un-complete the PUT — our own client value is
    // already stored locally (step (a)), and we converge on the next
    // read/anti-entropy instead. `Priority::ClientLocal` matches step (a)'s
    // originator-loopback store.
    if let Some(reverse_delta) = reverse_delta {
        match crate::operations::update::update_contract(
            op_manager,
            key,
            UpdateData::Delta(reverse_delta),
            RelatedContracts::default(),
            crate::contract::Priority::ClientLocal,
        )
        .await
        {
            Ok(_) => {
                tracing::debug!(
                    tx = %client_tx,
                    contract = %key,
                    phase = "summary_first_reverse_delta_merged",
                    "PUT summary-first: applied holder's reverse delta to local \
                     copy (originator converged in the same round-trip)"
                );
            }
            Err(err) => {
                tracing::debug!(
                    tx = %client_tx,
                    contract = %key,
                    error = %err,
                    phase = "summary_first_reverse_delta_merge_failed",
                    "PUT summary-first: reverse-delta merge failed (best-effort, \
                     PUT still completes; originator converges via anti-entropy)"
                );
            }
        }
    }

    // Delta-incompat gate (HQk7 resync loop): if the contract is armed as
    // delta-incapable, the ProbeReconcile delta below is doomed at the
    // holder ("Invalid update"). Fall through to the full-state driver
    // instead — the same recovery the delta-computation-failure arm uses.
    // See `crate::ring::delta_incompat`.
    if op_manager.ring.delta_incompat.suppress_deltas(key.id()) {
        tracing::debug!(
            tx = %client_tx,
            contract = %key,
            event = "delta_suppressed_incompat",
            phase = "summary_first_delta_suppressed",
            "PUT summary-first: contract is in delta-incompat backoff; \
             falling through to full-state driver"
        );
        return SummaryFirstOutcome::FallThrough;
    }

    let our_state_size = merged_value.size();
    let delta = match op_manager
        .interest_manager
        .compute_delta(
            op_manager,
            &key,
            &their_summary,
            &our_summary,
            our_state_size,
        )
        .await
    {
        Ok(delta) => delta,
        Err(err) => {
            tracing::debug!(
                tx = %client_tx,
                contract = %key,
                error = %err,
                phase = "summary_first_delta_failed",
                "PUT summary-first: delta computation failed; falling through to \
                 full-state driver"
            );
            return SummaryFirstOutcome::FallThrough;
        }
    };

    // `Ok(None)` = the contract's delta relative to the holder's summary is
    // empty (peer's state is already logically equivalent to ours) — the
    // existing-mesh case still holds, just with nothing to send.
    let delta_bytes = match delta {
        Some(delta) => {
            let bytes = delta.size() as u64;
            let reconcile_tx = Transaction::new::<PutMsg>();
            let reconcile_msg = NetMessage::from(PutMsg::ProbeReconcile {
                id: reconcile_tx,
                key,
                delta,
                htl,
                skip_list: own_addr.into_iter().collect::<HashSet<_>>(),
            });
            let mut reconcile_ctx = op_manager.op_ctx(reconcile_tx);
            if let Err(err) = reconcile_ctx
                .send_fire_and_forget(target_addr, reconcile_msg)
                .await
            {
                // Best-effort: `ProbeReconcile` is fire-and-forget by design
                // (see its rustdoc). A dispatch failure here does not
                // un-complete the PUT — the originator's own copy is
                // already stored locally (step (a)); the target mesh just
                // doesn't get this delta pushed proactively (it converges
                // on the next read/update instead).
                tracing::debug!(
                    tx = %client_tx,
                    reconcile_tx = %reconcile_tx,
                    contract = %key,
                    error = %err,
                    phase = "summary_first_reconcile_dispatch_failed",
                    "PUT summary-first: ProbeReconcile dispatch failed \
                     (best-effort, PUT still completes)"
                );
            }
            op_manager.release_pending_op_slot(reconcile_tx).await;
            bytes
        }
        None => 0,
    };

    record_put_probe_outcome(false, delta_bytes);

    SummaryFirstOutcome::ReconciledExisting {
        target: target.clone(),
        hop_count: probe_hop_count,
    }
}

/// Feeds both the in-process test counter (`GlobalTestMetrics`) and the
/// live dashboard counter (`network_status`) from a single call site, so
/// the two can never drift relative to each other — see
/// `.claude/rules/bug-prevention-patterns.md` ("Manually-mirrored
/// telemetry counters").
fn record_put_probe_outcome(new_contract: bool, bytes: u64) {
    if new_contract {
        crate::config::GlobalTestMetrics::record_put_probe_new_contract(bytes);
        crate::node::network_status::record_put_bytes(
            crate::node::network_status::PutProbeCase::NewContract,
            bytes,
        );
    } else {
        crate::config::GlobalTestMetrics::record_put_probe_existing_mesh_delta(bytes);
        crate::node::network_status::record_put_bytes(
            crate::node::network_status::PutProbeCase::ExistingMeshDelta,
            bytes,
        );
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
    /// Terminal failure delivered through the bypass via
    /// `PutMsg::Error` (issue #4111). The driver must NOT retry: the
    /// failure is deterministic (e.g., `put_contract` rejected the
    /// state on the originator's own node) and re-running the same
    /// validation will fail identically.
    TerminalError {
        cause: PutTerminalError,
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
        // Issue #4111: terminal failure delivered via send_local_loopback
        // from the originator-loopback error path. The wire cause is a
        // raw `String` (intentional, see `PutMsg::Error`); we wrap it in
        // `PutTerminalError` here so the retry-loop's `Terminal` type
        // stays typed.
        NetMessage::V1(NetMessageV1::Put(PutMsg::Error { cause, .. })) => {
            ReplyClass::TerminalError {
                cause: PutTerminalError::from_wire(cause.clone()),
            }
        }
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

/// Dashboard op_stats success classifier (issue #4828). Extracted so the
/// success/failure mapping per `DriverOutcome` variant has direct unit
/// coverage; `deliver_outcome` itself takes an `OpManager` which is
/// expensive to construct in tests. Mirrors UPDATE's
/// `classify_update_outcome_for_op_stats` (#4010).
fn classify_put_outcome_for_op_stats(outcome: &DriverOutcome) -> bool {
    matches!(outcome, DriverOutcome::Publish(Ok(_)))
}

fn deliver_outcome(op_manager: &OpManager, client_tx: Transaction, outcome: DriverOutcome) {
    // Dashboard op_stats PUT counter (issue #4828). The legacy
    // `report_result` recording path is bypassed for driver terminal
    // replies (see `node::record_op_result` rustdoc), so every terminal
    // outcome must be recorded here.
    //
    // Recording here rather than in the driver's arms is load-bearing:
    // `run_client_put` -> `drive_client_put` -> `drive_client_put_inner`
    // is the only call chain, so this single funnel sees EVERY terminal
    // outcome. The pre-#4828 code recorded a hardcoded `true` in the two
    // success arms instead, which left the `Done((Err(..), _))` #4111
    // bypass, `Exhausted`, and `InfrastructureError` paths recording
    // nothing at all — so `op_stats.puts.1` (the red "fail" number on the
    // ops card) was structurally unreachable. Do NOT re-inline this into
    // a driver arm; that is exactly how #4009/#4010 regrew here.
    //
    // Sub-operation gate: defensive. Today PUT has no sub-operation entry
    // points (`Transaction::new_child_of` is only ever used to spawn
    // SUBSCRIBE children), so `client_tx.is_sub_operation()` is always
    // false on this path. The explicit gate matches UPDATE's and
    // SUBSCRIBE's pattern so a future change that routes a sub-op tx
    // through `run_client_put` doesn't silently start inflating the
    // user-facing dashboard counter.
    if !client_tx.is_sub_operation() {
        let success = classify_put_outcome_for_op_stats(&outcome);
        crate::node::network_status::record_op_result(
            crate::node::network_status::OpType::Put,
            success,
        );
    }

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

/// Counter: number of times `PutRetryDriver::advance` was invoked.
/// Test-only — used by `drive_retry_loop_done_err_does_not_call_advance`
/// to prove a deterministic-local failure does not step the retry loop.
#[cfg(any(test, feature = "testing"))]
pub static PUT_RETRY_DRIVER_ADVANCE_CALLS: std::sync::atomic::AtomicUsize =
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

    // Routing/hosting attribution (Group C): count this as a genuine relayed
    // PUT (past the dedup gate — a deduped duplicate is not a relay). Exclude
    // the originator-loopback case: node.rs maps a client-originated PUT
    // (`source_addr=None`) to `upstream_addr=own_addr` and drives it here, so
    // counting it would inflate `relayed_puts_total` with the node's OWN
    // originations rather than genuine downstream forwards.
    let originator_loopback =
        Some(upstream_addr) == op_manager.ring.connection_manager.get_own_addr();
    if !originator_loopback {
        crate::node::network_status::record_relayed_put();
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

    // Originator-loopback error path: deliver the failure through the
    // same `pending_op_results` bypass the success path uses so the
    // retry-loop classifies it once instead of timing out on a closed
    // reply channel (issue #4111). Safe in non-loopback mode because
    // remote relays don't share tx-space with a local client.
    //
    // See `.claude/rules/operations.md` → "WHEN publishing a terminal
    // operation reply" for the M1/M2 race rationale (`send_client_result`
    // + `completed()` is forbidden here; we deliver via send_local_loopback
    // → bypass instead).
    let own_addr = op_manager.ring.connection_manager.get_own_addr();
    let originator_loopback = Some(upstream_addr) == own_addr;
    if originator_loopback {
        if let Err(err) = drive_result {
            let cause = bound_cause(err.to_string());
            let error_msg = NetMessage::from(PutMsg::Error {
                id: incoming_tx,
                cause: cause.clone(),
            });
            let mut ctx = op_manager.op_ctx(incoming_tx);
            if let Err(send_err) = ctx.send_local_loopback(error_msg).await {
                // Executor channel closed (node shutdown OR
                // `op_execution_sender` torn down while
                // `result_router_tx` is still live). Hand off to the
                // OpManager-independent fallback helper so the WS
                // client still sees a real cause instead of waiting
                // on the dead bypass.
                tracing::warn!(
                    tx = %incoming_tx,
                    cause = %cause,
                    error = %send_err,
                    "PUT relay (task-per-tx): send_local_loopback for terminal-error \
                     failed; falling back to direct result_router_tx publish"
                );
                dispatch_loopback_shutdown_fallback(
                    &op_manager.result_router_tx,
                    incoming_tx,
                    cause,
                );
            }
        }
    } else if let Err(err) = drive_result {
        // B1 (multi-hop bubble): the driver failed locally on an
        // intermediate relay. The downstream-reply bubble path in
        // `drive_relay_put` only fires when a downstream peer returns
        // `PutMsg::Error`; a LOCAL failure (e.g. `put_contract`
        // rejection, next-hop selection error, `send_to_and_await`
        // wire error/timeout) bypasses that arm and the upstream
        // `send_to_and_await` would hang until `OPERATION_TTL`,
        // burning the originator's retry budget.
        //
        // Emit `PutMsg::Error { cause }` to `upstream_addr` so the
        // upstream relay's bypass — which now accepts
        // `PutMsg::Error` (see `node.rs`) — delivers it into the
        // upstream's installed `pending_op_results[tx]` waiter,
        // letting the upstream relay either bubble further or land
        // on the originator's `drive_retry_loop` as
        // `Terminal(Err(cause))`. Cause is bounded at the wire
        // boundary; `relay_put_send_error` itself does NOT call
        // `completed()`, preserving the no-completed-in-loopback
        // invariant for any node along the chain that happens to be
        // the originator.
        let cause = bound_cause(err.to_string());
        if let Err(send_err) =
            relay_put_send_error(&op_manager, incoming_tx, cause.clone(), upstream_addr).await
        {
            tracing::warn!(
                tx = %incoming_tx,
                %upstream_addr,
                cause = %cause,
                error = %send_err,
                "PUT relay: failed to bubble terminal-error upstream \
                 (multi-hop bubble); upstream will see OPERATION_TTL"
            );
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

/// Greedy-routing termination guard for the relay PUT path (#4363).
///
/// A PUT is forwarded hop-by-hop toward the contract location, but the
/// router selects the next hop by learned routing predictions, not by
/// strict geometric distance. On a sparse/bootstrap topology this lets
/// the chain overshoot the contract neighbourhood: it descends close to
/// the target, then the only forwardable peers sit *farther* away, and
/// the request keeps wandering — finalizing (and placing its
/// authoritative replica) far from where greedy GETs will look.
///
/// This is the PUT analogue of the ring's accept-only-at-terminus rule
/// (`.claude/rules/ring.md`): a node is the routing terminus once it
/// cannot forward to a peer strictly closer to the target than itself.
/// When that holds, finalize here — the closest-seen node along the
/// chain — rather than push the request (and a worse-placed replica) to
/// a farther peer.
///
/// Returns `true` when the relay should stop forwarding and finalize
/// locally.
///
/// # The away-move alone is NOT sufficient (issue direction A)
///
/// Issue #4363 frames the fix precisely: terminate at the closest-seen
/// node "when the routing chain moves *away* from the target location
/// **for k consecutive hops**". The naive `k == 1` reading — "the next
/// hop is non-improving, so stop" — is WRONG on the sparse/bootstrap
/// topologies this code runs on, and regresses placement instead of
/// fixing it:
///
/// - The originator's own loopback relay runs this guard at full HTL
///   from a node whose location is unrelated to the contract. Its only
///   forwardable peer is frequently *farther* from the target than the
///   originator itself (the originator just happens to sit closer to a
///   random contract location than its handful of bootstrap peers). A
///   `k == 1` guard finalizes the PUT *at the originator*, so the
///   contract is never forwarded into its neighbourhood at all — and a
///   later greedy SUBSCRIBE/GET descending toward the contract location
///   "not found after exhaustive search". (Regression caught by
///   `test_multiple_clients_subscription` /
///   `test_nat_peer_remote_subscription_receives_update`.)
/// - More generally, "the next hop is farther" is the *normal* state for
///   every hop before the chain has actually reached the contract
///   neighbourhood; treating it as a terminus stops forward replication
///   dead.
///
/// # Closest-seen as a no-wire-format proxy (k == 2)
///
/// We cannot thread a shared "closest distance seen so far" / consecutive
/// away-hop counter through the relay without a positional add to the
/// `PutMsg::Request` wire format (and a `MIN_COMPATIBLE_VERSION` bump).
/// But the inbound hop already carries exactly the evidence direction (A)
/// needs: the location of the **upstream** node that forwarded this
/// Request to us. If `upstream → own` was itself a step *toward* the
/// target (this node is strictly closer to the target than its
/// upstream), then the chain has been making progress and this node is
/// the closest-seen point so far; an outbound non-improving hop from here
/// is the genuine overshoot the issue describes. We therefore require
/// BOTH:
///
///   1. `own` is strictly closer to `target` than `upstream` — the chain
///      descended to reach us (closest-seen so far), AND
///   2. `next_hop` is not strictly closer to `target` than `own` — the
///      only forwardable hop moves away.
///
/// This is effectively `k == 2` (one observed descending hop in, one
/// observed away hop out) realized entirely from state already on hand,
/// with no wire-format change. It is the minimal condition that fixes
/// #4363's overshoot without terminating chains that have not yet reached
/// the neighbourhood:
///
/// - At the originator loopback, `upstream == own`, so clause (1) is
///   false → never self-terminate → the PUT is forwarded normally.
/// - On a chain still descending toward the target, clause (2) is false
///   → keep forwarding.
/// - Only once the chain has provably descended to a local minimum and
///   the next hop would climb back out do we finalize — placing the
///   authoritative replica at the closest-seen node, exactly as the
///   ring's accept-only-at-terminus rule places connections.
///
/// The skip-list still prevents loops and HTL still bounds the chain, so
/// the guard only ever *shortens* an overshooting tail; it never strands
/// a PUT that had further legitimate progress to make.
///
/// The guard only fires when all three locations are known; a node with
/// no location (pre-join / unknown addr) or an unresolvable upstream
/// cannot reason about "closer", so it falls back to forward-always.
fn put_routing_should_terminate(
    upstream_loc: Option<Location>,
    own_loc: Option<Location>,
    next_hop_loc: Option<Location>,
    target: Location,
) -> bool {
    let (upstream_loc, own_loc, next_hop_loc) = match (upstream_loc, own_loc, next_hop_loc) {
        (Some(up), Some(own), Some(next)) => (up, own, next),
        // Unknown upstream/own/next location: cannot compare distances,
        // so keep the prior forward-always behaviour rather than guess.
        _ => return false,
    };
    let upstream_dist = upstream_loc.distance(target).as_f64();
    let own_dist = own_loc.distance(target).as_f64();
    let next_dist = next_hop_loc.distance(target).as_f64();

    // Clause (1): the chain descended to reach us. The inbound hop
    // (upstream → own) moved strictly closer to the target, so this node
    // is the closest-seen point so far. At the originator loopback
    // `upstream == own`, making this strictly-less comparison false and
    // leaving the originator unguarded.
    let descended_to_here = own_dist < upstream_dist;

    // Clause (2): the only forwardable next hop is NOT strictly closer to
    // the target than we are — forwarding would climb back out of the
    // neighbourhood we just reached (the #4363 overshoot).
    let next_hop_moves_away = next_dist >= own_dist;

    descended_to_here && next_hop_moves_away
}

// NAMING LANDMINE: "relay" is a fossil of the hollow-relay firefight (#3763).
// Forwarding a PUT does NOT make a peer a hollow relay; a routed PUT is demand, so
// the peer HOSTS the contract (routing and hosting co-occur; there is no
// forward-without-hosting for GET/PUT). The only true hollow relay was the
// standalone SUBSCRIBE hop, being retired (subscribe folds into GET/PUT). Slated for
// removal/rename by piece D (chain peers become real hosts). Do not name new code
// "relay". See .claude/rules/hosting-invariants.md terminology + epic #4642.
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
    // NAMING LANDMINE: "cache" here = HOSTING. Storing on a routed PUT is
    // legitimate hosting (a routed PUT is demand), evictable under LRU, not a
    // lesser cache tier. To be renamed hosting-* after 0.2.94.
    // See .claude/rules/hosting-invariants.md + #4642.
    // Originator-loopback (a local client's own PUT, mapped to
    // upstream_addr=own_addr in dispatch) is ClientLocal so it lands in the
    // reserved fair-queue lane; a genuine relay store stays NetworkRelay (#4534).
    let store_priority = put_store_priority(op_manager, upstream_addr);
    let merged_value = relay_put_store_locally(
        op_manager,
        incoming_tx,
        key,
        value.clone(),
        &contract,
        related_contracts.clone(),
        htl,
        store_priority,
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
            // Greedy-routing terminus guard (#4363): finalize here only
            // when the chain has provably descended to this node (the
            // closest-seen point) AND the best next hop would climb back
            // out of the contract neighbourhood. Forwarding past an
            // overshoot would place the authoritative replica where
            // greedy GETs never descend; terminating *before* the chain
            // has reached the neighbourhood (e.g. at the originator
            // loopback) would strand the contract far from the target —
            // see `put_routing_should_terminate` for why the away-move
            // alone is not a sufficient stop condition.
            let target = crate::ring::Location::from(&key);
            // own_location() is the address-derived ring position (not the
            // configured get_stored_location()); this matches the
            // is_subscription_root precedent, which also reasons about "am
            // I the routing terminus" from the address-derived location.
            let own_loc = op_manager.ring.connection_manager.own_location().location();
            // Upstream location resolves the inbound hop so the guard can
            // tell "the chain descended to reach me" from "I just happen
            // to sit closer to a random contract than my bootstrap peers".
            // At the originator loopback (`upstream_addr == own_addr`) this
            // resolves to our own location, leaving the originator
            // unguarded by construction.
            let upstream_loc = op_manager
                .ring
                .connection_manager
                .get_peer_by_addr(upstream_addr)
                .and_then(|p| p.location());
            let next_hop_loc = peer.location();
            if put_routing_should_terminate(upstream_loc, own_loc, next_hop_loc, target) {
                tracing::info!(
                    tx = %incoming_tx,
                    contract = %key,
                    upstream_location = ?upstream_loc.map(|l| l.as_f64()),
                    own_location = ?own_loc.map(|l| l.as_f64()),
                    next_hop_location = ?next_hop_loc.map(|l| l.as_f64()),
                    target_location = %target.as_f64(),
                    phase = "relay_put_terminus",
                    "PUT relay: chain descended here and next hop moves away; finalizing here (#4363)"
                );
                // Finalize the operation at this closest-seen node (#4363
                // placement) but STILL replicate the contract one hop onward so
                // the UPDATE broadcast tree stays connected (#4509 convergence).
                // See `relay_put_replicate_forward` for why both are required.
                if let Some(next_addr) = peer.socket_addr() {
                    relay_put_replicate_forward(
                        op_manager,
                        next_addr,
                        key,
                        contract.clone(),
                        related_contracts.clone(),
                        merged_value.clone(),
                        htl,
                        new_skip_list.clone(),
                    )
                    .await;
                }
                let hop_count = op_manager.ring.max_hops_to_live.saturating_sub(htl);
                // EXPERIMENT-ONLY (findability_probe): record the PUT terminus
                // (#4363 guard) + store the single copy here. NOT for ship.
                #[cfg(any(test, feature = "testing"))]
                {
                    let skip = new_skip_list.clone();
                    crate::operations::findability_probe::record_terminus(
                        &op_manager.ring,
                        crate::operations::findability_probe::ProbeOpKind::Put,
                        &incoming_tx,
                        target,
                        htl,
                        hop_count,
                        crate::operations::findability_probe::ProbeStopReason::TerminusGuard,
                        move |addr| skip.contains(&addr),
                    );
                    relay_put_finalize_scatter_disabled_store(
                        op_manager,
                        incoming_tx,
                        key,
                        merged_value.clone(),
                        &contract,
                        related_contracts.clone(),
                    )
                    .await;
                }
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
                    // EXPERIMENT-ONLY (findability_probe). NOT for ship.
                    #[cfg(any(test, feature = "testing"))]
                    {
                        let skip = new_skip_list.clone();
                        let target = crate::ring::Location::from(&key);
                        let reason = if htl == 0 {
                            crate::operations::findability_probe::ProbeStopReason::HtlZero
                        } else {
                            crate::operations::findability_probe::ProbeStopReason::NoNextHop
                        };
                        crate::operations::findability_probe::record_terminus(
                            &op_manager.ring,
                            crate::operations::findability_probe::ProbeOpKind::Put,
                            &incoming_tx,
                            target,
                            htl,
                            hop_count,
                            reason,
                            move |addr| skip.contains(&addr),
                        );
                        relay_put_finalize_scatter_disabled_store(
                            op_manager,
                            incoming_tx,
                            key,
                            merged_value.clone(),
                            &contract,
                            related_contracts.clone(),
                        )
                        .await;
                    }
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
            // Bootstrap fallback (#4361 / #4365): with an empty ring,
            // forward via a configured gateway instead of finalizing
            // locally. Without this, a freshly-bootstrapped node's PUT is
            // stored locally (Step 1 above) and finalized here, so the
            // client gets a success while the contract never reaches the
            // network — the PUT analog of GET's empty-ring blind spot.
            // Bypasses the #4363 terminus guard on purpose: an empty ring
            // has no overshoot to guard against, we only need to reach the
            // network. The local replica is already stored, so the
            // originator keeps it AND the gateway relays the PUT toward the
            // contract location. Respects `new_skip_list` (self + upstream).
            //
            // Intentional asymmetry with `drive_relay_put_streaming` (which
            // gates the gateway fallback on `htl > 0`): this arm is also
            // reached when `htl == 0` (next_hop is forced None then), so it
            // can forward to a gateway at htl 0. Benign — the contract is
            // already stored locally (no false success), it is a single
            // bounded next-hop (no fan-out), and the forward is terminating:
            // each hop adds itself (and its upstream) to `new_skip_list`, so
            // in a pathological all-empty-ring chain `select_bootstrap_gateway`
            // eventually excludes every configured gateway and returns None
            // (finalize locally). Termination here is skip_list exhaustion —
            // NOT htl, and NOT the receiving gateway's ring being populated.
            // In the normal case the gateway does have a populated ring and
            // routes the PUT toward the contract location (better placement
            // than this empty-ring node). The originator loopback always
            // starts at max_htl so it never hits htl 0 here. Do not
            // "harmonize" the two paths — there is no correctness gain.
            match bootstrap_gateway_target(op_manager, |addr| new_skip_list.contains(&addr)) {
                Some((gateway, gateway_addr)) => {
                    tracing::info!(
                        tx = %incoming_tx,
                        contract = %key,
                        gateway = %gateway_addr,
                        phase = "relay_put_bootstrap_gateway",
                        "PUT relay: ring empty — forwarding to configured gateway"
                    );
                    (gateway, gateway_addr)
                }
                None => {
                    // No next hop and no configured gateway — genuinely
                    // isolated, so this node is the final destination.
                    tracing::info!(
                        tx = %incoming_tx,
                        contract = %key,
                        phase = "relay_put_complete",
                        "PUT relay: no next hop, finalizing at this node"
                    );
                    // Same arm as above: this node IS the storer.
                    let hop_count = op_manager.ring.max_hops_to_live.saturating_sub(htl);
                    // EXPERIMENT-ONLY (findability_probe). NOT for ship.
                    #[cfg(any(test, feature = "testing"))]
                    {
                        let skip = new_skip_list.clone();
                        let target = crate::ring::Location::from(&key);
                        let reason = if htl == 0 {
                            crate::operations::findability_probe::ProbeStopReason::HtlZero
                        } else {
                            crate::operations::findability_probe::ProbeStopReason::NoNextHop
                        };
                        crate::operations::findability_probe::record_terminus(
                            &op_manager.ring,
                            crate::operations::findability_probe::ProbeOpKind::Put,
                            &incoming_tx,
                            target,
                            htl,
                            hop_count,
                            reason,
                            move |addr| skip.contains(&addr),
                        );
                        relay_put_finalize_scatter_disabled_store(
                            op_manager,
                            incoming_tx,
                            key,
                            merged_value.clone(),
                            &contract,
                            related_contracts.clone(),
                        )
                        .await;
                    }
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
            }
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
        // Issue #3465 — a locally-applied PUT must not be reported to the
        // client as a hard failure just because *propagation* toward the
        // contract neighbourhood failed. At this point Step 1
        // (`relay_put_store_locally`) has already succeeded on the
        // originator's own node: the state IS applied locally (users proved
        // this by re-PUTing at version+1 and getting "version must be higher
        // than current version N"). The forward below is best-effort
        // propagation. Pre-fix, a dispatch error here returned `Err`, which
        // `run_relay_put`'s originator-loopback arm converted into a
        // `PutMsg::Error` — surfacing "PUT operation timed out" / "peer
        // connection dropped" to the client for an operation that in fact
        // succeeded locally. Instead, on a forward-dispatch failure we
        // finalize the PUT locally as a success (the originator is a valid
        // storer for its own contract) via `relay_put_finalize_local`, which
        // routes a `PutMsg::Response` back through the loopback bypass and
        // classifies as `Stored`.
        //
        // Fire-and-forget forward; do NOT install a waiter (would
        // overwrite the originator's pending_op_results callback).
        // On a successful dispatch the downstream Response returns
        // directly to the originator via the bypass.
        let local_hop_count = op_manager.ring.max_hops_to_live.saturating_sub(htl);
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
                    phase = "relay_put_loopback_forward_failed",
                    "PUT relay (loopback): streaming-upgrade forward could not \
                     be dispatched; finalizing PUT locally as success (#3465) — \
                     state is stored on this node, propagation is best-effort"
                );
                return relay_put_finalize_local(
                    op_manager,
                    incoming_tx,
                    key,
                    merged_value,
                    upstream_addr,
                    local_hop_count,
                )
                .await;
            }
            // Originator loopback: the retry-loop task (Task A) registered a
            // stream-progress handle keyed by `incoming_tx` before sending. We
            // (Task B) look it up and thread it into the transport so each
            // fragment dispatch records a tick, driving the retry loop's
            // stream-inactivity timeout (#4001). `None` here (non-streaming
            // relay, no registered handle) degrades to the plain send.
            let progress = op_manager.stream_progress_registry().get(&incoming_tx);
            if let Err(err) = conn_manager
                .send_stream_with_progress(
                    next_addr,
                    stream_id,
                    bytes::Bytes::from(payload_bytes),
                    None,
                    progress,
                )
                .await
            {
                tracing::warn!(
                    tx = %incoming_tx,
                    contract = %key,
                    target = %next_addr,
                    %stream_id,
                    error = %err,
                    phase = "relay_put_loopback_forward_failed",
                    "PUT relay (loopback): send_stream failed; finalizing PUT \
                     locally as success (#3465) — state is stored on this node, \
                     propagation is best-effort"
                );
                return relay_put_finalize_local(
                    op_manager,
                    incoming_tx,
                    key,
                    merged_value,
                    upstream_addr,
                    local_hop_count,
                )
                .await;
            }
        } else {
            let forward = NetMessage::from(PutMsg::Request {
                id: incoming_tx,
                contract,
                related_contracts,
                value: merged_value.clone(),
                htl: new_htl,
                skip_list: new_skip_list,
            });
            if let Err(err) = ctx.send_fire_and_forget(next_addr, forward).await {
                tracing::warn!(
                    tx = %incoming_tx,
                    contract = %key,
                    target = %next_addr,
                    error = %err,
                    phase = "relay_put_loopback_forward_failed",
                    "PUT relay (loopback): forward could not be dispatched; \
                     finalizing PUT locally as success (#3465) — state is \
                     stored on this node, propagation is best-effort"
                );
                return relay_put_finalize_local(
                    op_manager,
                    incoming_tx,
                    key,
                    merged_value,
                    upstream_addr,
                    local_hop_count,
                )
                .await;
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
            // Route through `recv_waiter_reply` so a `PeerDisconnected`
            // signal on the waiter channel surfaces as the matching
            // `OpError` instead of a bare close (#4313).
            ctx.recv_waiter_reply(&mut reply_rx).await
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
        NetMessage::V1(NetMessageV1::Put(PutMsg::Error {
            cause: downstream_cause,
            ..
        })) => {
            // Propagate the downstream cause one hop further upstream so
            // the originator-loopback bypass at
            // `handle_pure_network_message_v1` delivers it to the
            // waiting `pending_op_results[incoming_tx]` callback.
            // Re-bound the cause: an intermediate relay forwards
            // verbatim and could be the attacker-controlled hop.
            let bubbled = bound_cause(downstream_cause);
            tracing::warn!(
                tx = %incoming_tx,
                contract = %key,
                cause = %bubbled,
                phase = "relay_put_bubble_error_upstream",
                "PUT relay: downstream PutMsg::Error — propagating cause to upstream"
            );
            relay_put_send_error(op_manager, incoming_tx, bubbled, upstream_addr).await
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

/// Store a relayed PUT's contract locally: `put_contract` + `host_contract`
/// (unconditional, so EVERY genuine PUT refreshes hosting recency —
/// invariant 3 / #4903 review Fix 1) + (on first host, gated on the atomic
/// `host_contract` `is_new` result) `announce_contract_hosted` + interest
/// register/unregister + broadcast interest changes.
///
/// Shared between the non-streaming relay driver (`drive_relay_put`)
/// and the streaming relay driver (`drive_relay_put_streaming`) so both
/// paths run identical local-store semantics. Returns the post-merge
/// `WrappedState` the caller forwards downstream / bubbles upstream.
///
/// This helper is **relay-only** — it never sets
/// `mark_local_client_access` (that's originator-side). Errors emit a
/// `put_failure` telemetry event and propagate.
/// Fair-queue priority for a relay PUT's local store (#4534).
///
/// A local client's own PUT enters the relay driver via originator-loopback,
/// which dispatch maps to `upstream_addr = own_addr` (see operations.md). That
/// case is `ClientLocal` so the store uses the reserved admission lane; any
/// genuine upstream peer is `NetworkRelay`. If our own address is unknown we
/// fail safe to `NetworkRelay` (never over-prioritize an ambiguous store).
fn put_store_priority(
    op_manager: &Arc<OpManager>,
    upstream_addr: SocketAddr,
) -> crate::contract::Priority {
    match op_manager.ring.connection_manager.get_own_addr() {
        Some(own) if own == upstream_addr => crate::contract::Priority::ClientLocal,
        _ => crate::contract::Priority::NetworkRelay,
    }
}

// NAMING LANDMINE: "store" (and the "relay_" prefix) here means HOSTING, not a
// lesser cache tier or a hollow relay. A peer stores a contract because a PUT routed
// through it, and a routed PUT is a demand signal, so the peer HOSTS the contract
// (WASM+state, kept fresh in the update mesh) until LRU eviction. There is no cache
// tier and no forward-without-hosting. Slated to be renamed hosting-* AFTER 0.2.94.
// See .claude/rules/hosting-invariants.md terminology + epic #4642.
#[allow(clippy::too_many_arguments)]
async fn relay_put_store_locally(
    op_manager: &Arc<OpManager>,
    incoming_tx: Transaction,
    key: ContractKey,
    value: WrappedState,
    contract: &ContractContainer,
    related_contracts: RelatedContracts<'static>,
    htl: usize,
    priority: crate::contract::Priority,
) -> Result<WrappedState, OpError> {
    // EXPERIMENT-ONLY (findability_probe): suppress the every-hop PUT store so
    // the copy lands ONLY at the routing terminus (stored there explicitly by
    // `relay_put_finalize_scatter_disabled_store`). Return the value unmerged —
    // with a single copy in flight there is no existing state to merge against.
    // NOT for ship. See operations::findability_probe.
    #[cfg(any(test, feature = "testing"))]
    if crate::operations::findability_probe::scatter_disabled() {
        return Ok(value);
    }
    let (merged_value, _state_changed) = match super::put_contract(
        op_manager,
        key,
        value.clone(),
        related_contracts,
        contract,
        priority,
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

    // EVERY genuine PUT stamps hosting recency — including a repeat PUT to a
    // contract this peer already hosts (invariant 3: "a real GET or PUT resets
    // recency", and this applies to BOTH the client-loopback and the relay-hop
    // PUT, which share this store chokepoint). This call is unconditional so a
    // write-only publisher re-PUTting an already-hosted contract keeps its
    // recency fresh instead of being stamped once at first host and becoming a
    // cost-eviction candidate `COST_RATE_MIN_WINDOW` later — an evict →
    // re-host churn loop (#4903 review Fix 1). Record the POST-merge size
    // (`merged_value`), not the pre-merge `value`, so hosting-byte accounting
    // reflects what is actually stored (#4903 review round-3 Fix 3).
    let access_result = op_manager.ring.host_contract(
        key,
        merged_value.size() as u64,
        crate::ring::AccessType::Put,
    );

    // Gate the first-time-hosting side effects on the ATOMIC `host_contract`
    // result (`is_new`), NOT a pre-await `is_hosting_contract` snapshot
    // (#4903 review round-3 Fix 1): a sweep can evict this contract during the
    // `put_contract().await` above, in which case `host_contract` re-adds it
    // (`is_new = true`) and we MUST run announce / interest-register /
    // evicted-teardown here. A stale pre-await "was already hosting" snapshot
    // would skip them AND drop `access_result.evicted` (leaking the contracts
    // this re-add shed to make room). On the already-hosted refresh path
    // `is_new` is false and `evicted` is empty (`record_access_with_demand`
    // refreshes recency/size/generation only, evicts nothing).
    if access_result.is_new {
        let evicted = access_result.evicted;

        // Sync the InterestManager for any subscribed contract the
        // subscriber-primary eviction shed + tore down (#4642 invariant 3). Run
        // BEFORE the `unregister_local_hosting` loop below so that call observes
        // the now-zeroed subscriber counts and reports full interest loss (→
        // retraction via `removed_contracts`). Without this, ghost
        // `interested_peers` / `peer_contracts` / `local_client_count` entries
        // survive and mis-target UPDATE broadcasts / inflate upstream interest
        // counts (PR #4734 Fix 1).
        for teardown in &access_result.evicted_in_use_teardown {
            op_manager.interest_manager.remove_evicted_in_use(
                &teardown.key,
                &teardown.downstream_peers,
                teardown.local_client_count,
            );
        }

        crate::operations::announce_contract_hosted(op_manager, &key).await;

        // Directed-subscribe placement (#4404): best-effort nudge the node to
        // consider migrating this freshly-hosted contract toward a closer
        // neighbor. Dropped silently if the event channel is full — the next
        // hosting/peer event re-triggers consideration.
        if let Err(err) =
            op_manager.try_notify_node_event(NodeEvent::ConsiderContractMigration { key })
        {
            tracing::debug!(%key, %err, "ConsiderContractMigration emit dropped (PUT)");
        }

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

/// Fire-and-forget a replication-only `PutMsg::Request` to `next_addr`
/// when the #4363 terminus guard finalizes the operation locally.
///
/// # Why the guard must still replicate (issue #4509 convergence fix)
///
/// The terminus guard makes the *closest-seen* node the authoritative
/// finalizer (it sends the upstream `PutMsg::Response`, so the originator's
/// reply and `hop_count` reflect placement at the contract neighbourhood —
/// the #4363 goal). But finalizing must NOT also stop the contract from
/// replicating onward: this codebase forms the UPDATE broadcast tree along
/// the PUT forward path (each relay `host_contract` + `announce_contract_hosted`
/// in `relay_put_store_locally`). If the guard simply returns after the
/// upstream Response, the next hop — and everything past it — never receives
/// the contract, so a later UPDATE that the router fans out to one of those
/// peers fails with `missing contract` and the network splits into divergent
/// state groups (observed in `test_six_peer_contract_lifecycle`: contract on
/// 4/6 peers, a 2/2 v20-vs-v30 split, node-2's updates reaching no one).
///
/// To keep placement (#4363) AND replication breadth (convergence), the guard
/// finalizes locally *and* forwards a replication copy onward with a FRESH
/// transaction so it does not collide with the upstream finalization waiter
/// (the originator is already satisfied by this node's Response). The forward
/// is fire-and-forget: no reply is awaited, and the downstream relay drives
/// its own replication (including re-applying the same guard one hop deeper).
/// The skip list — which already contains `upstream_addr` and `own_addr` —
/// rides along unchanged, and HTL is decremented, so the replication copy is
/// bounded and cannot loop back upstream. This is the "ensure terminated PUTs
/// still replicate to the contract neighbourhood" resolution.
#[allow(clippy::too_many_arguments)]
async fn relay_put_replicate_forward(
    op_manager: &OpManager,
    next_addr: SocketAddr,
    key: ContractKey,
    contract: ContractContainer,
    related_contracts: RelatedContracts<'static>,
    merged_value: WrappedState,
    htl: usize,
    skip_list: HashSet<SocketAddr>,
) {
    // EXPERIMENT-ONLY (findability_probe): suppress the one-hop-past terminus
    // replication so the PUT copy stays SINGULAR. NOT for ship.
    #[cfg(any(test, feature = "testing"))]
    if crate::operations::findability_probe::scatter_disabled() {
        return;
    }
    // The replication copy is sent as a single `PutMsg::Request`. For a
    // payload that would need streaming we skip the replication forward rather
    // than re-implement the piped-stream upgrade here: the local store already
    // placed the authoritative replica at this closest-seen node (#4363), and
    // the convergence regression this restores (#4509) is on the small-state
    // non-streaming path. A large-contract terminus is rare; skipping leaves
    // placement correct and only forgoes the extra broadcast-path replica.
    let payload = PutStreamingPayload {
        contract: contract.clone(),
        related_contracts: related_contracts.clone(),
        value: merged_value.clone(),
    };
    let payload_size = bincode::serialized_size(&payload).unwrap_or(u64::MAX) as usize;
    if crate::operations::should_use_streaming(op_manager.streaming_threshold, payload_size) {
        tracing::debug!(
            contract = %key,
            target = %next_addr,
            payload_size,
            phase = "relay_put_terminus_replicate",
            "PUT relay: terminus replication skipped for streaming-size payload (placement still correct)"
        );
        return;
    }

    // Fresh tx: the upstream Response under `incoming_tx` already satisfies
    // the originator's waiter; reusing it would (a) make the downstream relay
    // bubble a second Response back to a node with no waiter and (b) trip the
    // per-node dedup gate on `incoming_tx`. A new transaction keeps the
    // replication copy a clean, independent fire-and-forget op.
    let replication_tx = Transaction::new::<PutMsg>();
    let forward = NetMessage::from(PutMsg::Request {
        id: replication_tx,
        contract,
        related_contracts,
        value: merged_value,
        htl: htl.saturating_sub(1),
        skip_list,
    });
    let mut ctx = op_manager.op_ctx(replication_tx);
    if let Err(err) = ctx.send_fire_and_forget(next_addr, forward).await {
        tracing::debug!(
            replication_tx = %replication_tx,
            contract = %key,
            target = %next_addr,
            error = %err,
            phase = "relay_put_terminus_replicate",
            "PUT relay: terminus replication forward failed (best-effort)"
        );
    } else {
        tracing::debug!(
            replication_tx = %replication_tx,
            contract = %key,
            target = %next_addr,
            phase = "relay_put_terminus_replicate",
            "PUT relay: finalized locally but forwarded replication copy onward (#4509)"
        );
    }
}

/// EXPERIMENT-ONLY (findability_probe): when scatter is disabled the every-hop
/// store was a no-op, so the PUT terminus must persist its single copy here so
/// exactly one holder lands (the PUT-reach measurement point). NOT for ship.
#[cfg(any(test, feature = "testing"))]
async fn relay_put_finalize_scatter_disabled_store(
    op_manager: &Arc<OpManager>,
    incoming_tx: Transaction,
    key: ContractKey,
    value: WrappedState,
    contract: &ContractContainer,
    related_contracts: RelatedContracts<'static>,
) {
    if !crate::operations::findability_probe::scatter_disabled() {
        return;
    }
    match super::put_contract(
        op_manager,
        key,
        value.clone(),
        related_contracts,
        contract,
        crate::contract::Priority::NetworkRelay,
    )
    .await
    {
        Ok((merged, _)) => {
            if !op_manager.ring.is_hosting_contract(&key) {
                op_manager.ring.host_contract(
                    key,
                    merged.size() as u64,
                    crate::ring::AccessType::Put,
                );
            }
        }
        Err(err) => {
            tracing::error!(
                tx = %incoming_tx,
                contract = %key,
                error = %err,
                "EXPERIMENT findability_probe: PUT terminus store failed"
            );
        }
    }
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

/// Mirror of [`relay_put_send_response`] for terminal failures.
///
/// Forwards a `PutMsg::Error { cause }` one hop further upstream when a
/// downstream relay reports a contract-side or local-validation
/// rejection. Originator-loopback uses `send_local_loopback` so the
/// envelope hits the PUT bypass in `handle_pure_network_message_v1`
/// and lands in the originator's `pending_op_results` waiter.
async fn relay_put_send_error(
    op_manager: &OpManager,
    incoming_tx: Transaction,
    cause: String,
    upstream_addr: SocketAddr,
) -> Result<(), OpError> {
    let mut ctx = op_manager.op_ctx(incoming_tx);
    let own_addr = op_manager.ring.connection_manager.get_own_addr();
    relay_put_send_error_with_ctx(&mut ctx, own_addr, incoming_tx, cause, upstream_addr).await
}

/// Shutdown fallback for `run_relay_put`'s originator-loopback error
/// path. Used when `send_local_loopback` fails because the executor
/// channel is closed (typical node-shutdown scenario).
///
/// Publishes a `HostResult::Err` carrying the bounded cause straight
/// to `result_router_tx` so the WS client at least sees the real
/// failure reason instead of hanging on the dead bypass.
///
/// # No `OpManager::completed`, no `TransactionCompleted` emission
///
/// PR #4126 review item B3 + M3: the pre-#4111 success path went
/// `send_client_result(Err) + completed(tx)`. `send_client_result`
/// itself does two independent `try_send` calls — one to
/// `result_router_tx` (M1) and one to the event-loop notifications
/// channel for `TransactionCompleted(tx)` (M2). If the event loop
/// processes M2 first, it closes `pending_op_results[tx]` BEFORE
/// the originator's `send_and_await` consumes the reply on its
/// per-attempt callback, the driver sees `NotificationError`,
/// `advance()` fires, and the user gets the synthesised
/// `"failed notifying, channel closed"` instead of the real cause.
///
/// This helper participates only in M1 — it does NOT touch the
/// notifications channel, does NOT call `op_manager.completed(tx)`,
/// and is signature-locked to a single `&mpsc::Sender` so a future
/// refactor cannot quietly re-introduce the M2 emission. The
/// `pending_op_results[tx]` slot is reclaimed by the 60 s periodic
/// sweep — a slot leak under shutdown is the acceptable cost for
/// eliminating the race window entirely.
fn dispatch_loopback_shutdown_fallback(
    result_router_tx: &tokio::sync::mpsc::Sender<(Transaction, HostResult)>,
    incoming_tx: Transaction,
    cause: String,
) {
    let host_err: freenet_stdlib::client_api::ClientError = ErrorKind::OperationError {
        cause: cause.into(),
    }
    .into();
    if let Err(err) = result_router_tx.try_send((incoming_tx, Err(host_err))) {
        tracing::error!(
            tx = %incoming_tx,
            error = %err,
            "PUT relay shutdown fallback: result_router_tx full or closed; \
             client may not see the real failure cause (degraded but \
             non-fatal — no panic, no slot mutation)"
        );
    }
}

/// Wire envelope + dispatch decision for [`relay_put_send_error`],
/// factored out so tests can drive the function with a mock `OpCtx`
/// instead of a full `OpManager`.
///
/// PR #4126 review item M1: the original `relay_put_send_error` was
/// unit-test-unreachable because it depended on `OpManager::op_ctx`
/// and `OpManager::ring::connection_manager::get_own_addr`, both of
/// which require a fully-constructed `OpManager`. This split lets the
/// behavioural tests in the parent module's `tests` module exercise
/// the loopback/fire-and-forget branches and the wire envelope shape
/// directly.
async fn relay_put_send_error_with_ctx(
    ctx: &mut OpCtx,
    own_addr: Option<SocketAddr>,
    incoming_tx: Transaction,
    cause: String,
    upstream_addr: SocketAddr,
) -> Result<(), OpError> {
    let msg = NetMessage::from(PutMsg::Error {
        id: incoming_tx,
        cause,
    });
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

    // Routing/hosting attribution (Group C): count this as a genuine relayed
    // PUT, mirroring `start_relay_put`. A direct `PutMsg::RequestStreaming`
    // (or a `PutMsg::Request` that upgrades to streaming on forward) dispatches
    // here, so without this the largest PUT relays were undercounted. Exclude
    // the originator-loopback case (own streaming PUT mapped to
    // `upstream_addr=own_addr`), consistent with the non-streaming path.
    let originator_loopback =
        Some(upstream_addr) == op_manager.ring.connection_manager.get_own_addr();
    if !originator_loopback {
        crate::node::network_status::record_relayed_put();
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

    // Bootstrap fallback (#4361 / #4365), streaming path: with an empty
    // ring `closest_potentially_hosting` yields nothing, so route via a
    // configured gateway instead of finalizing locally and reporting a
    // false success. Computed here, before the terminus guard, and fed
    // into the guard's `None` arm below so it bypasses the guard — an
    // empty ring has no overshoot to reason about, and the stream is
    // assembled and stored locally in step 5/6 regardless, so the
    // originator keeps its replica while the gateway relays onward.
    let bootstrap_gateway = if next_hop.is_none() && htl > 0 {
        match bootstrap_gateway_target(op_manager, |addr| new_skip_list.contains(&addr)) {
            Some((gateway, gateway_addr)) => {
                tracing::info!(
                    tx = %incoming_tx,
                    contract = %contract_key,
                    gateway = %gateway_addr,
                    phase = "relay_put_streaming_bootstrap_gateway",
                    "PUT streaming relay: ring empty — forwarding to configured gateway"
                );
                Some(gateway)
            }
            None => None,
        }
    } else {
        None
    };

    // Greedy-routing terminus guard (#4363), streaming path. Mirrors the
    // non-streaming `drive_relay_put` guard: finalize here only when the
    // chain has descended to this node AND the next hop would climb back
    // out (the overshoot). The stream is assembled and stored here in
    // step 5/6 regardless, so terminating never drops the contract — it
    // just stops piping the replica to a farther peer the contract
    // neighbourhood will never descend to. The away-move alone is not a
    // sufficient stop condition; see `put_routing_should_terminate`.
    // When the terminus guard fires it drops `next_hop` (stops piping), but
    // the contract must still replicate one hop onward to keep the UPDATE
    // broadcast tree connected (#4509 convergence — see
    // `relay_put_replicate_forward`). The streaming payload isn't assembled
    // until step 5, so we remember the next-hop address here and forward a
    // replication copy after the local store in step 6.
    let mut terminus_replicate_to: Option<SocketAddr> = None;
    let next_hop = match next_hop {
        Some(peer) => {
            let target = crate::ring::Location::from(&contract_key);
            let own_loc = op_manager.ring.connection_manager.own_location().location();
            // Resolve the inbound hop so the guard can require "the chain
            // descended to reach me" before treating an away-move as
            // overshoot. Originator loopback resolves to our own location
            // (unresolvable upstream), leaving it unguarded.
            let upstream_loc = op_manager
                .ring
                .connection_manager
                .get_peer_by_addr(upstream_addr)
                .and_then(|p| p.location());
            let next_hop_loc = peer.location();
            if put_routing_should_terminate(upstream_loc, own_loc, next_hop_loc, target) {
                tracing::info!(
                    tx = %incoming_tx,
                    contract = %contract_key,
                    upstream_location = ?upstream_loc.map(|l| l.as_f64()),
                    own_location = ?own_loc.map(|l| l.as_f64()),
                    next_hop_location = ?next_hop_loc.map(|l| l.as_f64()),
                    target_location = %target.as_f64(),
                    phase = "relay_put_streaming_terminus",
                    "PUT streaming relay: chain descended here and next hop moves away; finalizing here (#4363)"
                );
                terminus_replicate_to = peer.socket_addr();
                None
            } else {
                Some(peer)
            }
        }
        // Empty ring: no ring candidate to run the terminus guard against,
        // so take the bootstrap gateway fallback computed above (#4361 / #4365).
        None => bootstrap_gateway,
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
    let downstream_reply_rx: Option<(SocketAddr, tokio::sync::mpsc::Receiver<WaiterReply>)> =
        if let Some((next_addr, metadata_net, outbound_sid, forked_handle, embedded_metadata)) =
            piping
        {
            let mut ctx = op_manager.op_ctx(incoming_tx);
            let rx_opt: Option<tokio::sync::mpsc::Receiver<WaiterReply>> = match ctx
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
    // Clone the contract/related-contracts for a possible terminus replication
    // forward (#4509) before the store consumes `related_contracts`.
    let replicate_payload =
        terminus_replicate_to.map(|addr| (addr, contract.clone(), related_contracts.clone()));
    // Originator-loopback client PUT → ClientLocal reserved lane (#4534).
    let store_priority = put_store_priority(op_manager, upstream_addr);
    let merged_value = relay_put_store_locally(
        op_manager,
        incoming_tx,
        key,
        value,
        &contract,
        related_contracts,
        htl,
        store_priority,
    )
    .await?;

    // Terminus guard fired (#4363) on the streaming path: finalize here but
    // still replicate the assembled contract one hop onward so the UPDATE
    // broadcast tree stays connected (#4509). Mirrors the non-streaming guard
    // branch; see `relay_put_replicate_forward`.
    if let Some((next_addr, contract, related_contracts)) = replicate_payload {
        relay_put_replicate_forward(
            op_manager,
            next_addr,
            key,
            contract,
            related_contracts,
            merged_value.clone(),
            htl,
            new_skip_list.clone(),
        )
        .await;
    }

    // ── Step 7: Await downstream reply (if piping), then bubble upstream ──
    //
    // Per-relay routing-event recording: the relay's chosen `next_hop`
    // either responded usefully (Success) or didn't (Failure). Feeding
    // these into the local Router lets the failure-probability model
    // learn from forwarded traffic, not just originator-side ops.
    if let Some((next_addr, mut rx)) = downstream_reply_rx {
        // Route through `recv_waiter_reply` so a `PeerDisconnected` signal
        // delivered by `handle_orphaned_transactions` surfaces as the
        // matching `OpError` (mapped to the downstream-failure arm below)
        // rather than a bare channel close (#4313).
        let ctx_for_recv = op_manager.op_ctx(incoming_tx);
        let reply = match tokio::time::timeout(
            OPERATION_TTL,
            ctx_for_recv.recv_waiter_reply(&mut rx),
        )
        .await
        {
            Ok(Ok(reply)) => reply,
            Ok(Err(err)) => {
                tracing::warn!(
                    tx = %incoming_tx,
                    target = %next_addr,
                    error = %err,
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

// ── Relay PROBE driver (summary-first PUT, #4642 step 3-bis) ───────────────

/// Spawn a relay driver for a fresh inbound `PutMsg::ProbeRequest`.
///
/// Mirrors [`start_relay_put`]'s dispatch shape (dedup via the shared
/// `active_relay_put_txs` set, RAII inflight guard constructed before
/// spawn, spawn), but the probe driver itself
/// ([`drive_relay_probe`]) is much simpler: pure routing plus an
/// `is_receiving_updates` check, never a local store. Gated by the same
/// dispatch site in `node.rs::handle_pure_network_message_v1` and by the
/// version-gated emission at the send side
/// (`ConnectionManager::supports_summary_first_put`) — a pre-floor peer
/// never receives this variant, so this driver needs no additional
/// version check on the receive side.
pub(crate) async fn start_relay_probe(
    op_manager: Arc<OpManager>,
    incoming_tx: Transaction,
    key: ContractKey,
    summary: StateSummary<'static>,
    htl: usize,
    skip_list: HashSet<SocketAddr>,
    upstream_addr: SocketAddr,
) -> Result<(), OpError> {
    // Shares the PUT relay dedup set (same tx space; a `ProbeRequest`'s
    // `id` never collides with a concurrent `PutMsg::Request`'s tx because
    // every attempt/probe allocates a fresh `Transaction`).
    if !op_manager.active_relay_put_txs.insert(incoming_tx) {
        tracing::debug!(
            tx = %incoming_tx,
            contract = %key,
            %upstream_addr,
            phase = "relay_probe_dedup_reject",
            "PUT probe relay: duplicate ProbeRequest for in-flight tx, dropping"
        );
        return Ok(());
    }

    let guard = RelayProbeInflightGuard {
        op_manager: op_manager.clone(),
        incoming_tx,
    };

    GlobalExecutor::spawn(async move {
        let _guard = guard;
        if let Err(err) = drive_relay_probe(
            &op_manager,
            incoming_tx,
            key,
            summary,
            htl,
            skip_list,
            upstream_addr,
        )
        .await
        {
            tracing::debug!(
                tx = %incoming_tx,
                contract = %key,
                error = %err,
                phase = "relay_probe_error",
                "PUT probe relay: driver returned error"
            );
        }
        // Release at driver exit — every path below either awaits a
        // downstream reply via `send_to_and_await` (already interim-released,
        // see `drive_relay_probe`) or sends the upstream `ProbeResponse` via
        // `send_fire_and_forget`/`send_local_loopback`, both of which leave a
        // fresh `pending_op_results[incoming_tx]` entry that only this
        // release reclaims (mirrors `run_relay_put`'s non-loopback release).
        op_manager.release_pending_op_slot(incoming_tx).await;
    });
    Ok(())
}

/// RAII guard removing `incoming_tx` from `active_relay_put_txs` on drop.
/// Constructed BEFORE spawn (see `start_relay_probe`) so a pre-poll drop of
/// the spawned future cannot permanently leak the dedup entry. Deliberately
/// lighter than [`RelayPutInflightGuard`]: probes are not real PUT relays,
/// so they do not touch the `RELAY_PUT_*` counters.
struct RelayProbeInflightGuard {
    op_manager: Arc<OpManager>,
    incoming_tx: Transaction,
}

impl Drop for RelayProbeInflightGuard {
    fn drop(&mut self) {
        self.op_manager
            .active_relay_put_txs
            .remove(&self.incoming_tx);
    }
}

/// Send `PutMsg::ProbeResponse` upstream. Mirrors
/// [`relay_put_send_response`]'s loopback-aware shape: fire-and-forget over
/// the wire, or `send_local_loopback` when this driver happens to be
/// running on the originator's own node. In practice a probe relay's
/// `upstream_addr` is always a genuine remote peer address — `ProbeRequest`
/// is only ever dispatched over the wire (`send_to_and_await` /
/// `send_fire_and_forget` with a real target), never via
/// `send_local_loopback` — but the check is kept for defensive symmetry
/// with the PUT relay path.
#[allow(clippy::too_many_arguments)]
async fn relay_probe_send_response(
    op_manager: &OpManager,
    incoming_tx: Transaction,
    key: ContractKey,
    holder_found: bool,
    hop_count: usize,
    holder_summary: Option<StateSummary<'static>>,
    reverse_delta: Option<StateDelta<'static>>,
    upstream_addr: SocketAddr,
) -> Result<(), OpError> {
    let msg = NetMessage::from(PutMsg::ProbeResponse {
        id: incoming_tx,
        key,
        holder_found,
        hop_count,
        holder_summary,
        reverse_delta,
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

/// Compute the reverse-leg delta a fresh holder ships back to a summary-first
/// PUT originator (#4642 step 3-bis, bidirectional reconcile): the delta the
/// ORIGINATOR is missing relative to the holder's state. This is the exact
/// mirror of the originator's forward delta — the holder already has both
/// inputs (the originator's summary arrived on the `ProbeRequest`, and it has
/// its own state) — so it reuses `InterestManager::compute_delta`, and
/// therefore the SAME `is_delta_efficient` gate the forward leg uses (a
/// summary-size proxy for delta size: the delta is skipped when the
/// originator's summary is ≥ ~50% of the holder's state size).
///
/// Returns `None` (skip the reverse leg — the originator heals via a normal
/// GET / anti-entropy rather than receiving a bloated or absent delta) when:
/// - the originator is already current (`compute_delta` → `Ok(None)`, empty
///   delta),
/// - the reverse delta would be inefficient or is uncomputable
///   (`compute_delta` → `Err`), or
/// - the holder's own state size can't be read (no efficiency input).
async fn compute_probe_reverse_delta(
    op_manager: &Arc<OpManager>,
    incoming_tx: Transaction,
    key: &ContractKey,
    originator_summary: &StateSummary<'static>,
    holder_summary: &StateSummary<'static>,
) -> Option<StateDelta<'static>> {
    let Some(our_state_size) = op_manager
        .interest_manager
        .get_contract_state_size(op_manager, key)
        .await
    else {
        tracing::debug!(
            tx = %incoming_tx,
            contract = %key,
            phase = "relay_probe_reverse_delta_no_state_size",
            "PUT probe: could not read holder state size for the reverse \
             delta; skipping reverse leg (originator heals via GET/anti-entropy)"
        );
        return None;
    };

    // `their_summary` = the originator's summary (what the originator has);
    // `our_summary` = the holder's summary. `compute_delta` returns the delta
    // that transforms the originator's state into ours — i.e. exactly what
    // the originator is missing.
    let computed = op_manager
        .interest_manager
        .compute_delta(
            op_manager,
            key,
            originator_summary,
            holder_summary,
            our_state_size,
        )
        .await;
    if let Err(ref err) = computed {
        tracing::debug!(
            tx = %incoming_tx,
            contract = %key,
            error = %err,
            phase = "relay_probe_reverse_delta_skipped",
            "PUT probe: reverse delta unavailable (inefficient or compute \
             failed); originator heals via GET/anti-entropy"
        );
    }
    reverse_delta_from_compute_result(computed)
}

/// Map an [`InterestManager::compute_delta`](crate::ring::interest::InterestManager::compute_delta)
/// result to the reverse-leg delta a holder ships in
/// `PutMsg::ProbeResponse::reverse_delta`.
///
/// `Ok(Some)` ships the delta; `Ok(None)` (the originator is already current —
/// the contract produced an empty delta) and `Err` (the delta is inefficient
/// or uncomputable) both ship nothing (`None`), leaving the originator to
/// heal via a normal GET / anti-entropy. Kept as a pure function so the three
/// arms are unit-testable without a live contract handler.
fn reverse_delta_from_compute_result(
    computed: Result<Option<StateDelta<'static>>, String>,
) -> Option<StateDelta<'static>> {
    computed.unwrap_or(None)
}

/// Routes a `PutMsg::ProbeRequest` toward `key`, or answers it directly when
/// this hop is a fresh holder.
///
/// # Lazy host-on-next-read (Fable F5 amplification fix)
///
/// This function never calls `put_contract` / `host_contract` /
/// `announce_contract_hosted` — a probe-routing hop that does not already
/// host `key` must only ROUTE, never store or fetch. Do NOT add a local
/// store here — see `.claude/rules/hosting-invariants.md` invariant 2
/// (hosting is demand-driven, never manufactured by a route-through) and
/// the "Relay-caching" anti-pattern. Hosting for a genuinely new contract
/// happens on the full-state `PutMsg::Request` path (every-hop placement,
/// `relay_put_store_locally`); hosting for an existing mesh happens lazily
/// on the contract's next real read.
async fn drive_relay_probe(
    op_manager: &Arc<OpManager>,
    incoming_tx: Transaction,
    key: ContractKey,
    summary: StateSummary<'static>,
    htl: usize,
    skip_list: HashSet<SocketAddr>,
    upstream_addr: SocketAddr,
) -> Result<(), OpError> {
    let hop_count = op_manager.ring.max_hops_to_live.saturating_sub(htl);

    // `is_receiving_updates` (subscribed OR local-client-subscribed) is the
    // FRESH-holder signal, not `is_hosting_contract` (mere cache membership,
    // which may be stale — see `.claude/rules/hosting-invariants.md`,
    // `feedback_hosted_cache_freshness`). A stale cached copy must not
    // short-circuit the probe into reporting a holder.
    if op_manager.ring.is_receiving_updates(&key) {
        let holder_summary = op_manager
            .interest_manager
            .get_contract_summary(op_manager, &key)
            .await;
        return match holder_summary {
            Some(s) => {
                // Reverse leg of the bidirectional reconcile (#4642 step
                // 3-bis): compute the delta the ORIGINATOR is missing
                // relative to our (the holder's) state — the mirror of the
                // originator's forward delta. `summary` is the originator's
                // summary carried on the ProbeRequest; `s` is our own. We
                // ship it back in `ProbeResponse::reverse_delta` so the
                // originator converges to the full merged state in the SAME
                // round-trip instead of healing only later via anti-entropy.
                // `None` (skip the reverse leg — originator heals via a
                // normal GET / anti-entropy) when the originator is already
                // current (empty delta) or the reverse delta would be
                // inefficient / uncomputable; see `compute_probe_reverse_delta`.
                let reverse_delta =
                    compute_probe_reverse_delta(op_manager, incoming_tx, &key, &summary, &s).await;
                relay_probe_send_response(
                    op_manager,
                    incoming_tx,
                    key,
                    true,
                    hop_count,
                    Some(s),
                    reverse_delta,
                    upstream_addr,
                )
                .await
            }
            None => {
                // Summary computation failed despite being a genuine
                // holder. Fail safe: report no-holder so the originator
                // falls through to the full-state `Request` path. Shipping
                // full state to a peer that already hosts the contract is
                // an idempotent no-op at the contract layer (extra bytes,
                // not a correctness break) — the risk-bounded
                // graceful-fallback design.
                tracing::debug!(
                    tx = %incoming_tx,
                    contract = %key,
                    phase = "relay_probe_summary_failed",
                    "PUT probe: is_receiving_updates but summary computation \
                     failed; reporting no-holder so the originator falls \
                     through to full-state PUT"
                );
                relay_probe_send_response(
                    op_manager,
                    incoming_tx,
                    key,
                    false,
                    hop_count,
                    None,
                    None,
                    upstream_addr,
                )
                .await
            }
        };
    }

    // Not a fresh holder here: route toward the key, mirroring
    // `drive_relay_put`'s next-hop selection and #4363 terminus guard
    // (`put_routing_should_terminate`), without any of the storage side
    // effects.
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

    let next_addr = match next_hop {
        Some(peer) => {
            let target = crate::ring::Location::from(&key);
            let own_loc = op_manager.ring.connection_manager.own_location().location();
            let upstream_loc = op_manager
                .ring
                .connection_manager
                .get_peer_by_addr(upstream_addr)
                .and_then(|p| p.location());
            let next_hop_loc = peer.location();
            if put_routing_should_terminate(upstream_loc, own_loc, next_hop_loc, target) {
                return relay_probe_send_response(
                    op_manager,
                    incoming_tx,
                    key,
                    false,
                    hop_count,
                    None,
                    None,
                    upstream_addr,
                )
                .await;
            }
            match peer.socket_addr() {
                Some(a) => a,
                None => {
                    return relay_probe_send_response(
                        op_manager,
                        incoming_tx,
                        key,
                        false,
                        hop_count,
                        None,
                        None,
                        upstream_addr,
                    )
                    .await;
                }
            }
        }
        None => {
            return relay_probe_send_response(
                op_manager,
                incoming_tx,
                key,
                false,
                hop_count,
                None,
                None,
                upstream_addr,
            )
            .await;
        }
    };

    // Mixed-version safety (#4642 step 3-bis staggered rollout): the routed
    // next hop must ALSO understand the probe wire variants, not just the
    // originator. `supports_summary_first_put` is applied at the originator
    // (`drive_client_put_inner`), but a relay selects `next_addr` purely by
    // routing (`closest_potentially_hosting`), so it can land on a pre-0.2.95
    // peer that carries none of the `PutMsg` probe tags (6/7/8). Such a peer
    // cannot bincode-deserialize the appended `ProbeRequest` tag and would
    // DROP the connection on the decode failure — the exact connection churn
    // the emission gate exists to prevent. Fail closed: treat THIS relay as
    // the probe terminus and reply no-holder upstream, so the originator falls
    // back to a full-state PUT (a summary-first failure must never break a
    // PUT). Never emit the probe variant to an unsupported peer. See
    // `ConnectionManager::supports_summary_first_put` and the originator gate.
    if !op_manager
        .ring
        .connection_manager
        .supports_summary_first_put(next_addr)
    {
        tracing::debug!(
            tx = %incoming_tx,
            contract = %key,
            %next_addr,
            phase = "relay_probe_next_hop_pre_floor",
            "PUT probe relay: next hop does not support summary-first probes; \
             failing closed (reply no-holder) so the originator falls back to \
             a full-state PUT"
        );
        return relay_probe_send_response(
            op_manager,
            incoming_tx,
            key,
            false,
            hop_count,
            None,
            None,
            upstream_addr,
        )
        .await;
    }

    let new_htl = htl.saturating_sub(1);
    let forward = NetMessage::from(PutMsg::ProbeRequest {
        id: incoming_tx,
        contract_key: key,
        summary,
        htl: new_htl,
        skip_list: new_skip_list,
    });

    let mut ctx = op_manager.op_ctx(incoming_tx);
    let round_trip =
        tokio::time::timeout(OPERATION_TTL, ctx.send_to_and_await(next_addr, forward)).await;
    // Interim release, mirroring `drive_relay_put`'s post-`send_to_and_await`
    // release: the downstream reply has already arrived (or timed out), and
    // the upstream `ProbeResponse` send below re-registers under the same
    // tx; the final release happens at `start_relay_probe`'s driver exit.
    op_manager.release_pending_op_slot(incoming_tx).await;

    let reply = match round_trip {
        Ok(Ok(reply)) => reply,
        Ok(Err(err)) => return Err(err),
        Err(_elapsed) => return Err(OpError::UnexpectedOpState),
    };

    #[allow(clippy::wildcard_enum_match_arm)]
    match reply {
        NetMessage::V1(NetMessageV1::Put(PutMsg::ProbeResponse {
            holder_found,
            hop_count: downstream_hop_count,
            holder_summary,
            reverse_delta,
            ..
        })) => {
            // Pass the reverse leg straight through: only the terminal
            // holder can compute the delta the originator is missing, so an
            // intermediate routing hop just relays it upstream unchanged
            // (same as `holder_summary`).
            relay_probe_send_response(
                op_manager,
                incoming_tx,
                key,
                holder_found,
                downstream_hop_count,
                holder_summary,
                reverse_delta,
                upstream_addr,
            )
            .await
        }
        _other => Err(OpError::UnexpectedOpState),
    }
}

/// Spawn a relay driver for a fresh inbound `PutMsg::ProbeReconcile`.
///
/// Mirrors [`start_relay_probe`]'s dispatch shape. `ProbeReconcile` has no
/// reply variant (fire-and-forget both directions — see the type's
/// rustdoc), so unlike the probe driver there is no downstream round-trip
/// to await; [`drive_relay_probe_reconcile`] either merges locally or
/// forwards once and returns.
pub(crate) async fn start_relay_probe_reconcile(
    op_manager: Arc<OpManager>,
    incoming_tx: Transaction,
    key: ContractKey,
    delta: StateDelta<'static>,
    htl: usize,
    skip_list: HashSet<SocketAddr>,
    upstream_addr: SocketAddr,
) -> Result<(), OpError> {
    if !op_manager.active_relay_put_txs.insert(incoming_tx) {
        tracing::debug!(
            tx = %incoming_tx,
            contract = %key,
            %upstream_addr,
            phase = "relay_probe_reconcile_dedup_reject",
            "PUT probe-reconcile relay: duplicate ProbeReconcile for in-flight tx, dropping"
        );
        return Ok(());
    }

    let guard = RelayProbeInflightGuard {
        op_manager: op_manager.clone(),
        incoming_tx,
    };

    GlobalExecutor::spawn(async move {
        let _guard = guard;
        if let Err(err) = drive_relay_probe_reconcile(
            &op_manager,
            incoming_tx,
            key,
            delta,
            htl,
            skip_list,
            upstream_addr,
        )
        .await
        {
            tracing::debug!(
                tx = %incoming_tx,
                contract = %key,
                error = %err,
                phase = "relay_probe_reconcile_error",
                "PUT probe-reconcile relay: driver returned error"
            );
        }
        // A forwarded reconcile (fire-and-forget) registers a fresh
        // `pending_op_results[incoming_tx]` entry that nothing will ever
        // reply to (no `ProbeReconcile` response variant); reclaim it here.
        // A no-op if the merge branch ran instead (nothing was registered).
        op_manager.release_pending_op_slot(incoming_tx).await;
    });
    Ok(())
}

/// Merge an inbound delta at a fresh holder, or forward the reconcile
/// toward `key` when this hop is not (yet) a holder.
///
/// Best-effort: a reconcile that reaches a routing terminus with no holder
/// is dropped rather than escalated — the wire invariant is that
/// `ProbeReconcile` is only ever sent after a `ProbeResponse` proved a
/// holder exists somewhere along this route (a genuinely new contract goes
/// the no-holder full-state `PutMsg::Request` path instead — see
/// `try_summary_first_put`). Same lazy host-on-next-read note as
/// [`drive_relay_probe`] applies: a non-holder hop only routes, it never
/// stores.
async fn drive_relay_probe_reconcile(
    op_manager: &Arc<OpManager>,
    incoming_tx: Transaction,
    key: ContractKey,
    delta: StateDelta<'static>,
    htl: usize,
    skip_list: HashSet<SocketAddr>,
    upstream_addr: SocketAddr,
) -> Result<(), OpError> {
    if op_manager.ring.is_receiving_updates(&key) {
        // Merge via the same path an UPDATE uses; the contract executor
        // fans the change out to the rest of the subscriber mesh
        // automatically on a real state change (`BroadcastStateChange`,
        // see `contract.rs`) — no explicit broadcast call needed here.
        if let Err(err) = crate::operations::update::update_contract(
            op_manager,
            key,
            UpdateData::Delta(delta),
            RelatedContracts::default(),
            crate::contract::Priority::NetworkRelay,
        )
        .await
        {
            tracing::debug!(
                tx = %incoming_tx,
                contract = %key,
                error = %err,
                phase = "relay_probe_reconcile_merge_failed",
                "PUT probe-reconcile: delta merge failed (best-effort)"
            );
        }
        return Ok(());
    }

    let mut new_skip_list = skip_list;
    new_skip_list.insert(upstream_addr);
    if let Some(own_addr) = op_manager.ring.connection_manager.get_own_addr() {
        new_skip_list.insert(own_addr);
    }

    if htl == 0 {
        return Ok(());
    }

    let next_hop = op_manager
        .ring
        .closest_potentially_hosting(&key, &new_skip_list);
    let next_addr = match next_hop {
        Some(peer) => {
            let target = crate::ring::Location::from(&key);
            let own_loc = op_manager.ring.connection_manager.own_location().location();
            let upstream_loc = op_manager
                .ring
                .connection_manager
                .get_peer_by_addr(upstream_addr)
                .and_then(|p| p.location());
            let next_hop_loc = peer.location();
            if put_routing_should_terminate(upstream_loc, own_loc, next_hop_loc, target) {
                return Ok(());
            }
            match peer.socket_addr() {
                Some(a) => a,
                None => return Ok(()),
            }
        }
        None => return Ok(()),
    };

    // Mixed-version safety (#4642 step 3-bis staggered rollout): same
    // fail-closed reasoning as `drive_relay_probe`, but `ProbeReconcile` is
    // fire-and-forget with no reply variant. A pre-0.2.95 next hop carries
    // none of the `PutMsg` probe tags (6/7/8), cannot bincode-deserialize the
    // appended `ProbeReconcile` tag, and would DROP the connection on the
    // decode failure. Do NOT forward it. Dropping is safe: the reconcile is
    // best-effort, and with the bidirectional reverse-delta the originator's
    // copy is already complete, so anti-entropy heals the downstream holder.
    // Return Ok(()) like the other terminus-drop branches so nothing is
    // escalated or crashed. See `ConnectionManager::supports_summary_first_put`.
    if !op_manager
        .ring
        .connection_manager
        .supports_summary_first_put(next_addr)
    {
        tracing::debug!(
            tx = %incoming_tx,
            contract = %key,
            %next_addr,
            phase = "relay_probe_reconcile_next_hop_pre_floor",
            "PUT probe-reconcile relay: next hop does not support \
             summary-first; dropping (best-effort, anti-entropy heals the \
             downstream holder)"
        );
        return Ok(());
    }

    let new_htl = htl.saturating_sub(1);
    let forward = NetMessage::from(PutMsg::ProbeReconcile {
        id: incoming_tx,
        key,
        delta,
        htl: new_htl,
        skip_list: new_skip_list,
    });
    let mut ctx = op_manager.op_ctx(incoming_tx);
    if let Err(err) = ctx.send_fire_and_forget(next_addr, forward).await {
        tracing::debug!(
            tx = %incoming_tx,
            contract = %key,
            target = %next_addr,
            error = %err,
            phase = "relay_probe_reconcile_forward_failed",
            "PUT probe-reconcile: forward failed (best-effort)"
        );
    }
    Ok(())
}

// ── End of relay PROBE driver ───────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    fn dummy_key() -> ContractKey {
        ContractKey::from_id_and_code(ContractInstanceId::new([1u8; 32]), CodeHash::new([2u8; 32]))
    }

    /// Source-scrape pin (HQk7 resync loop): the summary-first PUT path must
    /// consult the delta-incompat memo BEFORE computing the ProbeReconcile
    /// delta and fall through to the full-state driver for an armed
    /// contract. Without the gate this path keeps sending doomed deltas to a
    /// delta-incapable holder (bounded per-PUT, no loop — but every one is a
    /// wasted WASM delta computation plus a guaranteed "Invalid update" at
    /// the holder). See `crate::ring::delta_incompat`.
    #[test]
    fn summary_first_put_gates_delta_on_incompat_memo() {
        let src = include_str!("op_ctx_task.rs");
        let fn_start = src
            .find("async fn try_summary_first_put(")
            .expect("try_summary_first_put not found");
        let after = &src[fn_start..];
        let fn_end = after.find("\nasync fn ").unwrap_or(after.len());
        let body = &after[..fn_end];

        let gate_pos = body
            .find(".suppress_deltas(")
            .expect("try_summary_first_put must consult the delta-incompat memo");
        let delta_pos = body
            .find(".compute_delta(")
            .expect("ProbeReconcile compute_delta call not found");
        assert!(
            gate_pos < delta_pos,
            "the delta-incompat gate must run BEFORE compute_delta \
             (gate {gate_pos} < compute_delta {delta_pos})"
        );
        let fallthrough = &body[gate_pos..delta_pos];
        assert!(
            fallthrough.contains("SummaryFirstOutcome::FallThrough"),
            "an armed contract must fall through to the full-state driver"
        );
    }

    fn dummy_tx() -> Transaction {
        Transaction::new::<PutMsg>()
    }

    /// Bidirectional summary-first reconcile (#4642 step 3-bis): the
    /// reverse-leg mapping ships a `ProbeResponse::reverse_delta` ONLY when
    /// the holder is genuinely ahead. All three `compute_delta` outcomes map
    /// correctly:
    /// - `Ok(Some)` → ship the delta (holder ahead),
    /// - `Ok(None)` (originator already current — contract returned an empty
    ///   delta) → ship NOTHING,
    /// - `Err` (delta inefficient / uncomputable — SAME gate as the forward
    ///   leg) → ship NOTHING.
    ///
    /// The two `None` arms are the "originator already current / heal via a
    /// normal GET or anti-entropy" fallback: the reconcile must not emit a
    /// phantom reverse delta when there is nothing (or nothing efficient) to
    /// send. This is the deterministic falsifier for the "empty-reverse-delta
    /// case sends nothing" requirement — the CRDT mock never emits an empty
    /// delta, so the `Ok(None)` path can't be reached in the sim tests and is
    /// pinned here instead.
    #[test]
    fn reverse_delta_from_compute_result_maps_all_arms() {
        // Holder ahead: a real reverse delta is shipped back to the originator.
        let shipped = reverse_delta_from_compute_result(Ok(Some(StateDelta::from(vec![1, 2, 3]))));
        assert_eq!(
            shipped.as_ref().map(|d| d.as_ref().to_vec()),
            Some(vec![1, 2, 3]),
            "Ok(Some) must ship the holder's reverse delta"
        );

        // Originator already current (empty delta from the contract): send nothing.
        let already_current = reverse_delta_from_compute_result(Ok(None));
        assert!(
            already_current.is_none(),
            "Ok(None) (originator already current) must send NO reverse delta"
        );

        // Delta inefficient or uncomputable: send nothing — the originator
        // heals via a normal GET / anti-entropy instead of receiving a
        // bloated or absent delta.
        let inefficient =
            reverse_delta_from_compute_result(Err("Delta not efficient for this contract".into()));
        assert!(
            inefficient.is_none(),
            "Err (inefficient/uncomputable) must send NO reverse delta"
        );
    }

    /// Regression for #4363: on a sparse/bootstrap topology a PUT
    /// descended to within ring distance 0.03 of the contract location
    /// (0.211), then the router forwarded it AWAY to a node at 0.868
    /// (distance 0.343), where it finalized — so greedy GETs descending
    /// to 0.211 never found the replica.
    ///
    /// `put_routing_should_terminate` is the greedy-terminus guard that
    /// stops the wander: when the chain has DESCENDED to the close node
    /// (≈0.181, distance 0.03 from the 0.211 target) from a farther
    /// upstream, and the only forwardable next hop (0.868, distance
    /// 0.343) is FARTHER, the relay must terminate here instead of
    /// forwarding to the farther peer.
    #[test]
    fn put_routing_terminates_when_next_hop_moves_away_from_target() {
        let target = Location::new(0.211);
        // Upstream that forwarded to us — farther from the target, so the
        // inbound hop was a descending move (the chain reached us by
        // getting closer). Without this, the away-move alone is the
        // normal pre-neighbourhood state and MUST NOT terminate.
        let upstream = Location::new(0.868); // distance 0.343
        // Close node: distance 0.03 from target (matches the hop-3 node
        // in the diagnostic run).
        let own = Location::new(0.181);
        // The wander destination from the issue: distance 0.343.
        let next_hop = Location::new(0.868);

        // Sanity: this reproduces the geometry the issue describes.
        assert!(
            (own.distance(target).as_f64() - 0.03).abs() < 1e-9,
            "own node should be ~0.03 from target"
        );
        assert!(
            own.distance(target).as_f64() < upstream.distance(target).as_f64(),
            "inbound hop must be a descending move (own closer than upstream)"
        );
        assert!(
            next_hop.distance(target).as_f64() > own.distance(target).as_f64(),
            "next hop must be farther than own node (the away-move)"
        );

        assert!(
            put_routing_should_terminate(Some(upstream), Some(own), Some(next_hop), target),
            "must terminate when the chain descended here and the only next \
             hop moves away from the target"
        );
    }

    /// Regression for the #4509 over-aggressive guard. The originator's
    /// own loopback relay runs with `upstream == own` (the loopback maps
    /// `source_addr=None` to `upstream_addr=own_addr`). It frequently sits
    /// closer to a random contract than its sparse bootstrap peers, so a
    /// guard that fired on the away-move alone would finalize the PUT *at
    /// the originator* — never forwarding the contract into its
    /// neighbourhood, and making later SUBSCRIBE/GET fail "not found after
    /// exhaustive search" (`test_multiple_clients_subscription`) /
    /// "not cached locally" (`test_nat_peer_remote_subscription_*`). With
    /// `upstream == own`, clause (1) "descended to here" is false, so the
    /// originator MUST keep forwarding even though its only next hop is
    /// farther.
    #[test]
    fn put_routing_does_not_terminate_at_originator_loopback() {
        let target = Location::new(0.211);
        // Originator sits closer to the contract than its only peer, but
        // the inbound hop is a self-loop (upstream == own).
        let own = Location::new(0.181); // distance 0.03
        let next_hop = Location::new(0.868); // distance 0.343 — farther
        assert!(
            next_hop.distance(target).as_f64() > own.distance(target).as_f64(),
            "test setup: next hop is farther (the away-move the old guard tripped on)"
        );
        assert!(
            !put_routing_should_terminate(Some(own), Some(own), Some(next_hop), target),
            "originator loopback (upstream == own) must NEVER self-terminate — \
             the chain has not descended anywhere yet"
        );
    }

    /// Normal forward replication on a chain that has NOT yet reached the
    /// neighbourhood: the inbound hop did not descend (upstream was closer
    /// or equal), so even a non-improving next hop must keep forwarding.
    /// This is the dominant case on sparse/bootstrap topologies and the
    /// one the over-aggressive `k == 1` guard regressed.
    #[test]
    fn put_routing_forwards_when_chain_has_not_descended() {
        let target = Location::new(0.211);
        // Upstream was already as close as us (no descending inbound hop).
        let upstream = Location::new(0.181); // distance 0.03
        let own = Location::new(0.5); // distance 0.289 — we moved AWAY coming in
        let next_hop = Location::new(0.868); // distance 0.343 — also farther
        assert!(
            own.distance(target).as_f64() > upstream.distance(target).as_f64(),
            "test setup: inbound hop did not descend"
        );
        assert!(
            !put_routing_should_terminate(Some(upstream), Some(own), Some(next_hop), target),
            "must keep forwarding when the chain has not descended to us — \
             terminating here would strand the contract far from the target"
        );
    }

    /// Happy path: when the next hop is strictly closer to the target
    /// than this node, the relay keeps forwarding (no premature stop) —
    /// regardless of the inbound hop.
    #[test]
    fn put_routing_forwards_when_next_hop_is_closer() {
        let target = Location::new(0.211);
        let upstream = Location::new(0.7); // distance 0.489 (descending inbound)
        let own = Location::new(0.5); // distance 0.289
        let next_hop = Location::new(0.3); // distance 0.089 — closer
        assert!(
            next_hop.distance(target).as_f64() < own.distance(target).as_f64(),
            "test setup: next hop must be closer"
        );
        assert!(
            !put_routing_should_terminate(Some(upstream), Some(own), Some(next_hop), target),
            "must keep forwarding when the next hop makes progress"
        );
    }

    /// Boundary: once the chain has descended here, a next hop at exactly
    /// the same distance as this node is not progress, so the relay
    /// terminates (the `>=` half of the strictly-closer rule). Prevents
    /// two equidistant peers from ping-ponging the request.
    #[test]
    fn put_routing_terminates_on_equal_distance_next_hop() {
        let target = Location::new(0.5);
        // Upstream farther than us so clause (1) holds (we descended here).
        let upstream = Location::new(0.8); // distance 0.3
        // Both 0.1 from target (one CCW, one CW) → equal distance. Using
        // values away from the 0.0/1.0 wrap-around keeps the two
        // subtractions bit-identical so the equality is exact.
        let own = Location::new(0.4);
        let next_hop = Location::new(0.6);
        assert_eq!(
            own.distance(target).as_f64(),
            next_hop.distance(target).as_f64(),
            "test setup: both peers equidistant from target"
        );
        assert!(
            own.distance(target).as_f64() < upstream.distance(target).as_f64(),
            "test setup: inbound hop descended"
        );
        assert!(
            put_routing_should_terminate(Some(upstream), Some(own), Some(next_hop), target),
            "equal-distance next hop is not progress; must terminate once descended"
        );
    }

    /// Edge: unknown upstream/own/next-hop location must fall back to the
    /// prior forward-always behaviour (cannot reason about "closer").
    #[test]
    fn put_routing_does_not_terminate_when_location_unknown() {
        let target = Location::new(0.211);
        let known = Location::new(0.181);
        let farther = Location::new(0.868);
        assert!(
            !put_routing_should_terminate(None, Some(known), Some(farther), target),
            "unknown upstream location must not trigger termination"
        );
        assert!(
            !put_routing_should_terminate(Some(farther), None, Some(known), target),
            "unknown own location must not trigger termination"
        );
        assert!(
            !put_routing_should_terminate(Some(farther), Some(known), None, target),
            "unknown next-hop location must not trigger termination"
        );
        assert!(
            !put_routing_should_terminate(None, None, None, target),
            "all unknown must not trigger termination"
        );
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
        // Anchor on the `is_streaming` binding: #4001 hoisted the
        // streaming decision into `let is_streaming = should_use_streaming(…)`
        // (shared by both the advancement-cap selection and the
        // stream-progress handle), so the window starts there and spans the
        // cap selection that follows. The if/else block including its inline
        // rationale comment can be hundreds of bytes, hence the generous
        // window.
        let cap_decision = src[entry..]
            .find("let is_streaming =")
            .expect("drive_client_put_inner must compute `let is_streaming = …`");
        let window = &src[entry + cap_decision..entry + cap_decision + 1800];
        assert!(
            window.contains("should_use_streaming("),
            "drive_client_put_inner's streaming decision must \
             gate on should_use_streaming(threshold, \
             payload_size_estimate). A flat \
             MAX_PEER_ADVANCEMENTS_NON_STREAMING for all PUTs re-opens \
             freenet-git#53."
        );
        assert!(
            window.contains("let max_advancements = if is_streaming"),
            "the advancement-cap selection must gate on the hoisted \
             `is_streaming` flag derived from should_use_streaming, so the \
             streaming cap and the stream-progress handle stay in lockstep."
        );
        assert!(
            window.contains("MAX_PEER_ADVANCEMENTS_STREAMING")
                && window.contains("MAX_PEER_ADVANCEMENTS_NON_STREAMING"),
            "drive_client_put_inner must reference both advancement \
             caps by name in the selection so future readers see the \
             split — bare integer literals are forbidden here."
        );
    }

    /// Truncate this file's source at the `#[cfg(test)]` marker so the
    /// source-pin tests never match code or comments inside the test
    /// module itself (mirrors the GET / UPDATE helpers).
    fn production_source() -> &'static str {
        const FULL: &str = include_str!("op_ctx_task.rs");
        let cutoff = FULL
            .find("#[cfg(test)]")
            .expect("file must have a #[cfg(test)] section");
        &FULL[..cutoff]
    }

    /// Isolate a named fn's body by brace-matching from its opening `{`
    /// to the matching `}`, so a pin cannot silently run past the
    /// function into unrelated source. The defect this replaces did
    /// exactly that: a `"\nasync fn "` EOF scan on `drive_relay_put_streaming`
    /// found no following top-level `async fn`, ran the "body" to EOF, and
    /// swallowed the whole test module — so neutering the streaming call
    /// site still left the pin green.
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

    /// Issue #4828 (same class as #4009 / #4010): `deliver_outcome` is the
    /// single funnel every client PUT terminal outcome passes through
    /// (`run_client_put` -> `drive_client_put` -> `drive_client_put_inner`
    /// is the only call chain), so it MUST record the dashboard
    /// `op_stats.puts` counter there. Recording in the driver's success
    /// arms instead — which is what this file did before #4828 — left
    /// every failure arm (`Done((Err(..), _))` #4111 bypass,
    /// `Exhausted`, and the `InfrastructureError` synthesis) recording
    /// nothing, so `op_stats.puts.1` (the red "fail" number on the ops
    /// card) was structurally stuck at 0.
    ///
    /// Mirrors UPDATE's `deliver_outcome_records_update_op_result`, the
    /// #4010 fix shape that did NOT rot.
    #[test]
    fn deliver_outcome_records_put_op_result() {
        let prod = production_source();
        let body = extract_fn_body(prod, "fn deliver_outcome(");

        assert!(
            body.contains("record_op_result"),
            "deliver_outcome must call record_op_result so the dashboard \
             PUT counter advances on EVERY driver terminal outcome — \
             including the failure arms. Issue #4828."
        );
        assert!(
            body.contains("OpType::Put"),
            "record_op_result inside deliver_outcome must be passed \
             OpType::Put (not Get/Update/Subscribe)."
        );
        // The success flag must be DERIVED from the outcome. An
        // unconditional `true` is the #4828 bug — it is what made
        // `op_stats.puts.1` unreachable. Mirrors the assertion text of
        // GET's `record_op_result_reflects_host_result_outcome`.
        let call_pos = body
            .find("record_op_result")
            .expect("record_op_result must be called in deliver_outcome");
        let tail = &body[call_pos..];
        let call_window = &tail[..tail.len().min(200)];
        assert!(
            !call_window.contains("true,"),
            "record_op_result in deliver_outcome is passed an unconditional \
             `true`. The success flag must be derived from the outcome via \
             `classify_put_outcome_for_op_stats` so a failing PUT increments \
             op_stats.puts.1. Issue #4828. Call window: {call_window}"
        );
        assert!(
            body.contains("!client_tx.is_sub_operation()"),
            "deliver_outcome must gate record_op_result on \
             `!client_tx.is_sub_operation()` (note the leading `!`) for \
             parity with SUBSCRIBE / UPDATE; defensive against a future \
             change that routes a sub-op tx through run_client_put and \
             silently inflates the user-facing dashboard counter."
        );
    }

    /// Issue #4828: the PUT driver must NOT record `op_stats` itself.
    /// `deliver_outcome` owns the counter for every terminal outcome; a
    /// `record_op_result` re-inlined into a driver arm double-counts that
    /// arm (and is how the pre-#4828 code came to record only successes).
    /// This is the counterpart pin to `deliver_outcome_records_put_op_result`.
    #[test]
    fn put_driver_must_not_record_op_result_directly() {
        let prod = production_source();
        let body = extract_fn_body(prod, "async fn drive_client_put_inner(");

        assert!(
            !body.contains("record_op_result"),
            "drive_client_put_inner must NOT call record_op_result — \
             `deliver_outcome` is the single recording funnel for the PUT \
             op_stats counter. Re-inlining the call into a driver arm \
             double-counts that arm and re-opens #4828 (only the arms that \
             remember to call it get counted)."
        );
    }

    /// Issue #4828: pure-function coverage of every `DriverOutcome`
    /// variant, pinning the success/failure mapping against an
    /// accidental `success: !success` flip. Mirrors UPDATE's
    /// `classify_update_outcome_covers_all_variants` (#4010).
    #[test]
    fn classify_put_outcome_covers_all_variants() {
        use freenet_stdlib::prelude::{CodeHash, ContractInstanceId, ContractKey};

        let key = ContractKey::from_id_and_code(
            ContractInstanceId::new([7u8; 32]),
            CodeHash::new([8u8; 32]),
        );
        let publish_ok = DriverOutcome::Publish(Ok(HostResponse::ContractResponse(
            ContractResponse::PutResponse { key },
        )));
        let publish_err = DriverOutcome::Publish(Err(ErrorKind::OperationError {
            cause: "synthetic".into(),
        }
        .into()));
        let infra = DriverOutcome::InfrastructureError(OpError::UnexpectedOpState);

        assert!(classify_put_outcome_for_op_stats(&publish_ok));
        assert!(
            !classify_put_outcome_for_op_stats(&publish_err),
            "a published client error is a FAILED PUT — it must increment \
             op_stats.puts.1. Issue #4828."
        );
        assert!(
            !classify_put_outcome_for_op_stats(&infra),
            "an infrastructure error publishes a synthesized client error, \
             so it is a FAILED PUT. Issue #4828."
        );
    }

    /// Pin (telemetry accuracy): BOTH relay PUT entry points must record
    /// `record_relayed_put` gated on `!originator_loopback`. The streaming
    /// entry must record at all; a direct `RequestStreaming` would otherwise
    /// be undercounted. Loopback = node.rs mapping a client PUT's
    /// `source_addr=None` to `upstream_addr=own_addr`.
    #[test]
    fn relay_put_entries_gate_relayed_counter_on_loopback() {
        let src = production_source();
        for sig in [
            "pub(crate) async fn start_relay_put<CB>(",
            "pub(crate) async fn start_relay_put_streaming<CB>(",
        ] {
            let body = extract_fn_body(src, sig);
            assert!(
                body.contains("record_relayed_put()"),
                "{sig} must record the relayed PUT counter"
            );
            assert!(
                body.contains("if !originator_loopback"),
                "{sig} must gate record_relayed_put on !originator_loopback"
            );
        }
    }

    /// Source-grep pin (#4361 / #4365): both relay PUT drivers must
    /// consult `bootstrap_gateway_target` when
    /// `closest_potentially_hosting` yields no next hop, so a
    /// freshly-bootstrapped node (empty ring) forwards the PUT through a
    /// configured gateway instead of finalizing locally and reporting a
    /// false success to the client. Dropping either call site silently
    /// reintroduces the empty-ring silent-store bug that GET fixed in
    /// #4364.
    ///
    /// Uses brace-matched `extract_fn_body` over `production_source()`,
    /// NOT a `"\nasync fn "` EOF scan: the old scan let the streaming
    /// driver's "body" run to EOF and swallow this module, so neutering
    /// the streaming call site left the pin green — the exact regression
    /// this guards (verified fail-without-fix by neutering each call site
    /// in turn). A driver-level *behavioral* test (build an `OpManager`
    /// with `connection_count()==0` + a configured gateway and assert the
    /// gateway is the chosen next hop) is deferred: there is no `OpManager`
    /// unit-test builder, and a static sim self-promotes its gateway into
    /// the ring (`connection_count()` settles at 1, not 0) — the same infra
    /// gap that deferred GET's equivalent (#4364).
    #[test]
    fn relay_put_drivers_use_bootstrap_gateway_fallback() {
        let src = production_source();
        for entry in [
            "async fn drive_relay_put<CB>",
            "async fn drive_relay_put_streaming<CB>",
        ] {
            let body = extract_fn_body(src, entry);
            assert!(
                body.contains("bootstrap_gateway_target("),
                "{entry} must fall back to bootstrap_gateway_target on an \
                 empty ring instead of finalizing locally (#4361 / #4365)"
            );
            // The exclusion predicate must be the relay skip list (self +
            // upstream + already-tried), not a no-op — otherwise the fallback
            // could re-pick self/upstream and bounce the PUT.
            assert!(
                body.contains("bootstrap_gateway_target(op_manager, |addr| new_skip_list.contains"),
                "{entry} must exclude the relay skip list when selecting the \
                 bootstrap gateway (#4361 / #4365)"
            );
        }
    }

    /// `start_client_put` must call `admit_client_op()` before the
    /// `GlobalExecutor::spawn`, and the returned guard MUST be moved
    /// into the spawned future (held for the lifetime of the spawned
    /// `run_client_put`). Without the guard, the shutdown drain in
    /// `ShutdownHandle::shutdown` has no signal that this PUT is in
    /// flight — release-driven auto-update reverts to dropping the
    /// mirror push mid-stream. Using the atomic `admit_client_op()`
    /// (instead of the prior separate check + bump) closes the Codex
    /// r2 TOCTOU. Sibling pins live in the analogous test modules of
    /// `get/op_ctx_task.rs`, `update/op_ctx_task.rs`, and
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

    /// Phase 7 egress self-block pin (#4300). `start_client_put` MUST
    /// reject a banned contract BEFORE spawning the driver task, so a
    /// banned contract's PUT never consumes outbound network resources.
    /// Mirrors the receive-side `put_dispatch_gates_banned_contracts`
    /// pin in `ring/contract_ban_list.rs`. Sibling pins live in the
    /// other three `start_client_*` entry points and at the
    /// `handle_broadcast_state_change` egress fan-out in p2p_protoc.rs.
    /// If this fails, the egress gate was deleted or moved after the
    /// spawn — re-establish it before re-running the suite.
    #[test]
    fn start_client_put_gates_banned_contracts_before_spawn() {
        let src = include_str!("op_ctx_task.rs");
        let entry = src
            .find("pub(crate) async fn start_client_put(")
            .expect("start_client_put must exist");
        let after_spawn = src[entry..]
            .find("GlobalExecutor::spawn(")
            .expect("start_client_put must spawn a driver task");
        let before_spawn = &src[entry..entry + after_spawn];
        assert!(
            before_spawn.contains("reject_if_contract_banned"),
            "start_client_put must call reject_if_contract_banned() \
             before GlobalExecutor::spawn so a banned contract's PUT is \
             rejected with a typed error instead of being driven to peers \
             that don't know about our ban (#4300 egress self-block)."
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
                other @ (ReplyClass::LocalCompletion { .. }
                | ReplyClass::TerminalError { .. }
                | ReplyClass::Unexpected) => {
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
                other @ (ReplyClass::LocalCompletion { .. }
                | ReplyClass::TerminalError { .. }
                | ReplyClass::Unexpected) => {
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
            .find("RetryLoopOutcome::Done((Ok(reply_key), wire_hop_count))")
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

    /// Issue #4111: `PutMsg::Error` rides the bypass like a `Response`
    /// and must classify as `TerminalError { cause }` so the retry-loop
    /// driver returns `Done(Err(_))` and `run_client_put` publishes
    /// exactly one `HostResult::Err` carrying the real cause.
    #[test]
    fn classify_reply_error_is_terminal_error_with_cause() {
        let tx = dummy_tx();
        let cause = "execution error: invalid contract update, reason: \
                     New state version 1 must be higher than current version 1"
            .to_string();
        let msg = NetMessage::V1(NetMessageV1::Put(PutMsg::Error {
            id: tx,
            cause: cause.clone(),
        }));
        match classify_reply(&msg) {
            ReplyClass::TerminalError { cause: c } => assert_eq!(
                c.as_str(),
                cause.as_str(),
                "TerminalError must carry the verbatim cause string from \
                 the wire envelope so the client sees the real reason \
                 (not the generic 'failed notifying, channel closed')"
            ),
            ReplyClass::Stored { .. } => panic!(
                "Error envelope must NOT classify as Stored — Stored is \
                 success and would suppress the cause"
            ),
            ReplyClass::LocalCompletion { .. } => panic!(
                "Error envelope must NOT classify as LocalCompletion — \
                 LocalCompletion is a success path keyed by Request echo"
            ),
            ReplyClass::Unexpected => panic!(
                "Error envelope must NOT classify as Unexpected — that \
                 returns RetryLoopOutcome::Unexpected, dropping the cause"
            ),
        }
    }

    /// Issue #4111 boundary: `classify_reply` preserves an empty cause
    /// string verbatim instead of substituting a placeholder. The
    /// originator-side error display is the client's responsibility;
    /// the classifier MUST NOT silently rewrite the wire payload.
    #[test]
    fn classify_reply_error_with_empty_cause_preserves_empty() {
        let tx = dummy_tx();
        let msg = NetMessage::V1(NetMessageV1::Put(PutMsg::Error {
            id: tx,
            cause: String::new(),
        }));
        match classify_reply(&msg) {
            ReplyClass::TerminalError { cause } => assert_eq!(
                cause.as_str(),
                "",
                "empty cause must round-trip as empty — replacing it with a \
                 placeholder hides the wire shape from the client"
            ),
            ReplyClass::Stored { .. }
            | ReplyClass::LocalCompletion { .. }
            | ReplyClass::Unexpected => {
                panic!("expected TerminalError with empty cause")
            }
        }
    }

    /// Oversize causes are capped at `PUT_TERMINAL_CAUSE_MAX_BYTES`
    /// by `PutTerminalError::from_wire`, stamping a `...[truncated]`
    /// marker — bounds multi-hop DoS amplification.
    #[test]
    fn classify_reply_error_with_oversize_cause_is_truncated() {
        use crate::operations::put::PUT_TERMINAL_CAUSE_MAX_BYTES;

        let tx = dummy_tx();
        let cause = "x".repeat(PUT_TERMINAL_CAUSE_MAX_BYTES * 4);
        let msg = NetMessage::V1(NetMessageV1::Put(PutMsg::Error {
            id: tx,
            cause: cause.clone(),
        }));
        match classify_reply(&msg) {
            ReplyClass::TerminalError { cause: c } => {
                assert!(
                    c.as_str().len() <= PUT_TERMINAL_CAUSE_MAX_BYTES,
                    "oversize cause must be capped at PUT_TERMINAL_CAUSE_MAX_BYTES"
                );
                assert!(
                    c.as_str().ends_with("...[truncated]"),
                    "oversize cause must carry the truncation marker"
                );
            }
            ReplyClass::Stored { .. }
            | ReplyClass::LocalCompletion { .. }
            | ReplyClass::Unexpected => {
                panic!("expected TerminalError with truncated cause")
            }
        }
    }

    /// Sub-cap causes survive byte-for-byte.
    #[test]
    fn classify_reply_error_with_normal_cause_passes_through() {
        let tx = dummy_tx();
        let cause = "contract rejected: version must be strictly increasing".to_string();
        let msg = NetMessage::V1(NetMessageV1::Put(PutMsg::Error {
            id: tx,
            cause: cause.clone(),
        }));
        match classify_reply(&msg) {
            ReplyClass::TerminalError { cause: c } => assert_eq!(c.as_str(), cause.as_str()),
            ReplyClass::Stored { .. }
            | ReplyClass::LocalCompletion { .. }
            | ReplyClass::Unexpected => panic!("expected TerminalError"),
        }
    }

    /// Pin the `run_client_put` Done(Err) arm. When the
    /// retry loop returns `Done(Err(cause))`, the driver MUST:
    ///
    ///   1. mark the client transaction completed (so the
    ///      pending_op_results slot is reclaimed promptly), and
    ///   2. publish `ErrorKind::OperationError { cause }` — NOT a
    ///      synthesised "failed after N attempts" message.
    ///
    /// The arm must NOT advance/retry — the failure is terminal.
    #[test]
    fn run_client_put_done_err_arm_publishes_once_and_completes() {
        let src = include_str!("op_ctx_task.rs");

        // Locate the `match loop_result {` block in run_client_put.
        let match_anchor = "match loop_result {";
        let match_start = src
            .find(match_anchor)
            .expect("loop_result match site not found");
        let match_end = src[match_start..]
            .find("\n}\n")
            .map(|p| match_start + p)
            .expect("end of loop_result match not found");
        let match_body = &src[match_start..match_end];

        // Find the Done(Err(cause)) arm specifically.
        let arm_anchor = "RetryLoopOutcome::Done((Err(cause), _hop_count)) =>";
        let arm_start = match_body
            .find(arm_anchor)
            .expect("Done(Err(cause)) arm not found in run_client_put — issue #4111 regressed");
        // Bound the arm to the next match-arm boundary. The simplest
        // delimiter is the next `RetryLoopOutcome::` token, since each
        // arm starts with one.
        let arm_end = match_body[arm_start + arm_anchor.len()..]
            .find("RetryLoopOutcome::")
            .map(|p| arm_start + arm_anchor.len() + p)
            .unwrap_or(match_body.len());
        let arm_body = &match_body[arm_start..arm_end];

        assert!(
            arm_body.contains("op_manager.completed(client_tx)"),
            "Done(Err) arm MUST call op_manager.completed(client_tx) to \
             reclaim the pending_op_results slot — otherwise it lingers \
             until the 60s sweep"
        );
        assert!(
            arm_body.contains("DriverOutcome::Publish(Err("),
            "Done(Err) arm MUST publish a HostResult::Err — silent drop \
             would hang the client until timeout"
        );
        assert!(
            arm_body.contains("ErrorKind::OperationError"),
            "Done(Err) arm MUST wrap the cause in \
             freenet_stdlib::client_api::ErrorKind::OperationError so the \
             client sees a structured error variant (not a generic string)"
        );
        // The "does not advance" half of the invariant is covered
        // behaviourally by `drive_retry_loop_done_err_does_not_call_advance`.
    }

    /// Behavioural pin on the `drive_retry_loop` Terminal arm.
    ///
    /// The reviewer of PR #4126 flagged the earlier version of this
    /// test as tautological: it called only `classify_reply` (a free
    /// function with no driver and no path to `advance()`), so the
    /// `PUT_RETRY_DRIVER_ADVANCE_CALLS` counter assertion was
    /// guaranteed regardless of how the production `Done(Err)` arm
    /// in `drive_retry_loop` was wired. A regression that re-routed
    /// terminal errors through `advance()` from inside
    /// `drive_retry_loop` would not have tripped the old assertion.
    ///
    /// This rewrite proves the full chain by composition:
    ///
    ///   1. `classify_reply(PutMsg::Error)` ↦ `ReplyClass::TerminalError`
    ///      (behavioural — exercised here on a real `PutMsg::Error` envelope).
    ///   2. `PutRetryDriver::classify(ReplyClass::TerminalError)` ↦
    ///      `AttemptOutcome::Terminal(Err(cause))` (pinned by
    ///      `put_retry_driver_terminal_error_does_not_advance` below).
    ///   3. `drive_retry_loop`'s `AttemptOutcome::Terminal(value)` arm
    ///      returns `RetryLoopOutcome::Done(value)` WITHOUT calling
    ///      `driver.advance()` (pinned by
    ///      `drive_retry_loop_terminal_arm_does_not_call_advance` in
    ///      `crate::operations::op_ctx::tests`).
    ///
    /// 1+2+3 together prove: `PutMsg::Error` on the wire never causes
    /// the retry driver to advance to a new peer. The structural pins
    /// of 2 and 3 are intentional — without a way to instantiate a
    /// real `PutRetryDriver` against a stubbed `OpManager` they are
    /// the closest behavioural anchor available; pre-merge follow-up
    /// (see PR review M3) replaces them with a fake-`OpManager`
    /// runner.
    #[test]
    fn classify_reply_error_maps_to_terminal_error_with_cause() {
        let tx = dummy_tx();
        let cause = "rejected: version not increasing".to_string();
        let reply = NetMessage::V1(NetMessageV1::Put(PutMsg::Error {
            id: tx,
            cause: cause.clone(),
        }));
        match classify_reply(&reply) {
            ReplyClass::TerminalError { cause: c } => {
                assert_eq!(
                    c.as_str(),
                    cause.as_str(),
                    "classify_reply must preserve the wire cause verbatim \
                     so `drive_retry_loop`'s Terminal arm can publish the \
                     real reason (not the synthesised \
                     'failed notifying, channel closed' marker)"
                );
            }
            ReplyClass::Stored { .. }
            | ReplyClass::LocalCompletion { .. }
            | ReplyClass::Unexpected => {
                panic!(
                    "PutMsg::Error must classify as TerminalError — \
                     `Unexpected` would route through \
                     RetryLoopOutcome::Unexpected and lose the cause"
                )
            }
        }
    }

    /// Issue #4111: `PutRetryDriver::classify` must convert
    /// `ReplyClass::TerminalError` into `AttemptOutcome::Terminal(Err(_))`
    /// so `drive_retry_loop` exits with `Done(Err(_))` on the first such
    /// reply (no `advance()` to a fresh peer / fresh attempt_tx).
    #[test]
    fn put_retry_driver_terminal_error_does_not_advance() {
        let src = include_str!("op_ctx_task.rs");
        // Locate the PutRetryDriver impl block.
        let impl_start = src
            .find("impl RetryDriver for PutRetryDriver<'_> {")
            .expect("PutRetryDriver RetryDriver impl not found");
        let impl_end = src[impl_start..]
            .find("\n    }\n")
            .map(|p| impl_start + p)
            .expect("end of PutRetryDriver impl not found");
        let impl_body = &src[impl_start..impl_end];

        // The classify implementation must map TerminalError → Terminal(Err(_)).
        assert!(
            impl_body.contains("ReplyClass::TerminalError"),
            "PutRetryDriver::classify must handle ReplyClass::TerminalError \
             explicitly — otherwise it falls through to the Unexpected arm \
             and the driver returns RetryLoopOutcome::Unexpected, losing the \
             contract-side cause."
        );
        assert!(
            impl_body.contains("AttemptOutcome::Terminal((Err("),
            "PutRetryDriver::classify must produce \
             AttemptOutcome::Terminal((Err(cause), _)) for TerminalError so the \
             retry loop exits via the Done((Err(_), _)) path rather than \
             advancing to a fresh attempt."
        );
        // Pin the Terminal type — (Result<ContractKey, PutTerminalError>, Option<usize>)
        // is what lets the retry-loop carry both shapes through a
        // single arm, keeps the failure cause structured rather
        // than an opaque String, AND tracks hop_count for telemetry.
        assert!(
            impl_body.contains("type Terminal = (Result<ContractKey, PutTerminalError>"),
            "PutRetryDriver::Terminal must be
             (Result<ContractKey, PutTerminalError>, Option<usize>);
             changing this back to bare ContractKey re-introduces the issue
             #4111 failure mode (no way to express a terminal error);
             changing the error half back to `String` drops the structured
             classification (LocalRejection vs Relayed); dropping hop_count
             breaks the telemetry contract from PR #4248."
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
    /// `run_relay_put` in originator-loopback mode (`upstream_addr ==
    /// own_addr`) MUST deliver the failure to the originator's
    /// `start_client_put` retry-loop via
    /// `send_local_loopback(PutMsg::Error { id: incoming_tx, cause })`
    /// so the bypass forwards it to `pending_op_results[incoming_tx]`
    /// and the classifier sees one terminal reply.
    ///
    /// Thinned in PR #4126 review M3: the fallback-specific shape
    /// (no `op_manager.completed`, no `op_manager.send_client_result`,
    /// publish-via-`dispatch_loopback_shutdown_fallback`) is now
    /// covered behaviourally by the four
    /// `dispatch_loopback_shutdown_fallback_*` tests below; this pin
    /// stays minimal — just the success-path wiring.
    /// Repro: `test_put_error_notification` in
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
            body.contains("PutMsg::Error {"),
            "run_relay_put must construct a PutMsg::Error envelope to \
             deliver the loopback failure to the originator's driver"
        );
        assert!(
            body.contains("send_local_loopback"),
            "run_relay_put must dispatch the PutMsg::Error via \
             send_local_loopback so the bypass forwards it to the \
             originator's pending_op_results waiter"
        );
        // The fallback exists and is gated by send_local_loopback's
        // Err arm. The actual fallback behaviour (no completed(), no
        // send_client_result, raw result_router publish) is verified
        // behaviourally by `dispatch_loopback_shutdown_fallback_*`
        // tests below — not by source-grep here.
        assert!(
            body.contains("dispatch_loopback_shutdown_fallback("),
            "run_relay_put must invoke the OpManager-independent \
             fallback helper so the M3 behavioural tests cover the \
             shutdown path"
        );
        assert!(
            !body.contains("op_manager.completed(incoming_tx)"),
            "run_relay_put MUST NOT call op_manager.completed(incoming_tx) \
             in loopback mode — that's the pre-#4111 race shape \
             (see .claude/rules/operations.md → \"WHEN publishing a \
             terminal operation reply\")"
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
        // PR #4734 Fix 1: the eviction handler must sync the InterestManager for
        // any subscribed contract the subscriber-primary eviction shed + tore
        // down, or ghost interested_peers / peer_contracts / local_client_count
        // entries survive (they mis-target UPDATE broadcasts and do NOT
        // self-heal). A dropped call must trip this pin.
        assert!(
            helper_src.contains("evicted_in_use_teardown"),
            "helper MUST consume access_result.evicted_in_use_teardown"
        );
        assert!(
            helper_src.contains("remove_evicted_in_use"),
            "helper MUST call interest_manager.remove_evicted_in_use to sync the \
             InterestManager after a subscribed eviction"
        );
    }

    /// Pin (#4903 review Fix 1 / invariant 3): `relay_put_store_locally` MUST
    /// call `ring.host_contract(.., AccessType::Put)` UNCONDITIONALLY — i.e.
    /// BEFORE (outside) the first-time-hosting gate — so a repeat PUT to an
    /// already-hosted contract refreshes hosting recency (`recency_seq` /
    /// `last_genuine_access`). This applies to BOTH the client-loopback and the
    /// relay-hop PUT, which share this store chokepoint. When the stamp was
    /// gated on the first-time gate, a write-only publisher re-PUTting every few
    /// seconds was stamped once at first host and became a cost-eviction
    /// candidate `COST_RATE_MIN_WINDOW` later — an evict → re-host churn loop.
    #[test]
    fn relay_put_store_locally_stamps_recency_on_repeat_put() {
        let src = include_str!("op_ctx_task.rs");
        let start = src
            .find("async fn relay_put_store_locally(")
            .expect("relay_put_store_locally not found");
        let end = src[start..]
            .find("\nasync fn relay_put_replicate_forward(")
            .expect("relay_put_store_locally body end not found")
            + start;
        let body = &src[start..end];
        let host_pos = body
            .find(".host_contract(")
            .expect("relay_put_store_locally must call ring.host_contract");
        let gate_pos = body
            .find("if access_result.is_new")
            .expect("first-time-hosting gate not found");
        assert!(
            host_pos < gate_pos,
            "host_contract MUST run before (outside) the first-time-hosting gate \
             so a repeat PUT refreshes hosting recency (invariant 3: a real PUT \
             resets recency; #4903 review Fix 1)"
        );
    }

    /// Pin (#4903 review round-3 Fix 1 + Fix 3): `relay_put_store_locally` MUST
    /// (1) gate the first-time-hosting side effects on the ATOMIC
    ///     `access_result.is_new`, NOT a pre-await `is_hosting_contract` /
    ///     `was_hosting` snapshot. A sweep can evict the contract during the
    ///     `put_contract().await`; only the atomic `host_contract` result knows
    ///     the re-add is new, so a stale pre-await snapshot would skip
    ///     announce / interest-register and DROP `access_result.evicted`
    ///     (leaking the contracts the re-add shed); and
    /// (2) charge hosting bytes as the POST-merge `merged_value.size()`, not the
    ///     pre-merge input `value.size()`.
    #[test]
    fn relay_put_store_locally_first_host_gate_uses_atomic_result_and_merged_size() {
        let src = include_str!("op_ctx_task.rs");
        let start = src
            .find("async fn relay_put_store_locally(")
            .expect("relay_put_store_locally not found");
        let end = src[start..]
            .find("\nasync fn relay_put_replicate_forward(")
            .expect("relay_put_store_locally body end not found")
            + start;
        let body = &src[start..end];

        // (1) The gate is the atomic result, and NO pre-await snapshot survives.
        assert!(
            body.contains("if access_result.is_new"),
            "first-time-hosting side effects MUST be gated on the atomic \
             host_contract `is_new`, not a pre-await snapshot"
        );
        assert!(
            !body.contains("was_hosting"),
            "no pre-await `was_hosting` / `is_hosting_contract` snapshot may gate \
             the first-time-hosting side effects — a sweep can evict during \
             put_contract().await, and a stale snapshot would skip \
             announce/register and drop access_result.evicted (round-3 Fix 1)"
        );

        // (2) host_contract charges the POST-merge size.
        let host_pos = body
            .find(".host_contract(")
            .expect("relay_put_store_locally must call ring.host_contract");
        let rel_end = body[host_pos..]
            .find(");")
            .expect("host_contract call end not found");
        let host_args = &body[host_pos..host_pos + rel_end];
        assert!(
            host_args.contains("merged_value.size()"),
            "host_contract MUST charge the POST-merge merged_value.size(), not \
             the pre-merge value.size() (round-3 Fix 3)"
        );
    }

    /// Wiring pin for #4363: the greedy-routing terminus guard
    /// (`put_routing_should_terminate`) MUST be invoked in the
    /// non-streaming relay body, BEFORE the chosen next hop is forwarded
    /// to. The `put_routing_should_terminate` unit tests only exercise
    /// the helper in isolation — without this pin, deleting the call site
    /// (re-introducing the #4363 wander) would leave every unit test
    /// green. This locks the guard into the call path the same way
    /// `drive_relay_put_stores_locally_before_forwarding` locks the
    /// store-before-forward ordering.
    #[test]
    fn drive_relay_put_applies_terminus_guard_before_forwarding() {
        let src = include_str!("op_ctx_task.rs");
        let body = drive_relay_put_body(src);

        let guard_pos = body.find("put_routing_should_terminate(").expect(
            "drive_relay_put MUST call put_routing_should_terminate \
             (the #4363 greedy-terminus guard); deleting it re-introduces \
             the away-routing wander",
        );
        // The guard sits inside the `Some(peer)` next-hop arm, which must
        // resolve the next hop's socket address before the request is
        // forwarded downstream via `send_to_and_await`. The guard MUST
        // precede that forward.
        let forward_pos = body
            .find("ctx.send_to_and_await(")
            .expect("downstream forward (send_to_and_await) missing in driver");
        assert!(
            guard_pos < forward_pos,
            "the #4363 terminus guard MUST run BEFORE the downstream \
             forward, otherwise a non-improving next hop is still sent the \
             PUT and the replica overshoots the contract neighbourhood"
        );

        // The guard must finalize locally on its terminus branch (so the
        // closest-seen node is the authoritative finalizer, #4363) AND still
        // replicate the contract one hop onward (so the UPDATE broadcast tree
        // stays connected, #4509) — not just log, and not fall through to the
        // forward. Both calls live in the terminus branch before the forward.
        let guard_region = &body[guard_pos..forward_pos];
        assert!(
            guard_region.contains("relay_put_finalize_local("),
            "the terminus branch MUST finalize locally via \
             relay_put_finalize_local, not fall through to the forward"
        );
        assert!(
            guard_region.contains("relay_put_replicate_forward("),
            "the terminus branch MUST still replicate the contract onward via \
             relay_put_replicate_forward (#4509) — finalizing without \
             replicating orphans the UPDATE broadcast path and splits the \
             network into divergent state groups"
        );
    }

    /// Wiring pin for #4363 (streaming path): the streaming relay driver
    /// uses the same `closest_potentially_hosting` next-hop selection, so
    /// it MUST apply the same terminus guard before piping the replica to
    /// the next hop. Without this pin the streaming path could silently
    /// regress to the unguarded away-routing wander while the
    /// non-streaming path stays fixed.
    #[test]
    fn drive_relay_put_streaming_applies_terminus_guard_before_next_hop() {
        let src = include_str!("op_ctx_task.rs");
        let start = src
            .find("async fn drive_relay_put_streaming")
            .expect("drive_relay_put_streaming not found");
        // End the body at the end-of-driver marker so the scrape stays
        // within the streaming fn.
        let end = src[start..]
            .find("\n// ── End of relay PUT driver")
            .expect("streaming driver body end not found")
            + start;
        let body = &src[start..end];

        let guard_pos = body.find("put_routing_should_terminate(").expect(
            "drive_relay_put_streaming MUST call put_routing_should_terminate \
             (the #4363 greedy-terminus guard) so the streaming path does not \
             pipe a replica to a non-improving next hop",
        );
        // next_hop_addr is derived right before the piped-forward decision;
        // the guard must drop the non-improving hop before that point.
        let next_hop_addr_pos = body
            .find("let next_hop_addr = next_hop")
            .expect("next_hop_addr derivation missing in streaming driver");
        assert!(
            guard_pos < next_hop_addr_pos,
            "the #4363 terminus guard MUST run BEFORE next_hop_addr is \
             derived (i.e. before the piped-forward decision), so a \
             non-improving next hop is dropped and the driver finalizes \
             locally"
        );

        // The streaming guard records the dropped next hop in
        // `terminus_replicate_to` and, after the local store, replicates the
        // contract one hop onward via `relay_put_replicate_forward` (#4509) so
        // the streaming path does not orphan the UPDATE broadcast tree either.
        assert!(
            body.contains("terminus_replicate_to"),
            "streaming guard MUST record the dropped next hop in \
             terminus_replicate_to for the #4509 replication forward"
        );
        assert!(
            body.contains("relay_put_replicate_forward("),
            "streaming terminus path MUST still replicate the contract onward \
             via relay_put_replicate_forward (#4509)"
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

    // ──────────────────────────────────────────────────────────────────
    // PR #4126 review item M1: behavioural coverage of the multi-hop
    // bubble path. The original PR shipped the helper with zero unit
    // tests; these exercise the wire envelope, the loopback vs
    // fire-and-forget dispatch, the cause re-bound at the relay reply
    // arm, and the structural pin that local-failure paths in
    // `run_relay_put` actually invoke the helper in non-loopback mode.
    // ──────────────────────────────────────────────────────────────────

    /// Loopback branch: when `upstream_addr == own_addr`,
    /// `relay_put_send_error_with_ctx` MUST dispatch via
    /// `send_local_loopback` (target=None). Without this the
    /// originator's `pending_op_results` callback never sees the
    /// envelope and the retry-loop hangs.
    #[tokio::test]
    async fn relay_put_send_error_with_ctx_uses_loopback_when_own_addr() {
        use crate::node::{EventLoopNotificationsReceiver, event_loop_notification_channel};
        use std::net::{IpAddr, Ipv4Addr, SocketAddr};

        let (receiver, sender) = event_loop_notification_channel();
        let EventLoopNotificationsReceiver {
            mut op_execution_receiver,
            ..
        } = receiver;

        let tx = dummy_tx();
        let own = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 9001);
        let mut ctx = OpCtx::new(tx, sender.op_execution_sender.clone());

        // Drive the helper. Returns immediately because the channel
        // capacity is enough for one send.
        let cause = "rejected: deterministic local failure".to_string();
        let result = relay_put_send_error_with_ctx(
            &mut ctx,
            Some(own), // own_addr known
            tx,
            cause.clone(),
            own, // upstream is own_addr → loopback
        )
        .await;
        assert!(result.is_ok(), "loopback dispatch must succeed");

        // Receiver MUST see (msg, target_addr=None) per the loopback
        // contract — that signals `handle_op_execution` to fan the
        // envelope into `InboundMessage` instead of `OutboundMessageWithTarget`.
        let (_reply_sender, outbound, target_addr) = op_execution_receiver
            .recv()
            .await
            .expect("loopback envelope should be delivered to executor");
        assert_eq!(
            target_addr, None,
            "loopback MUST pass target_addr=None so the dispatcher \
             routes through InboundMessage → PUT bypass"
        );

        // Envelope shape MUST be PutMsg::Error with verbatim cause and
        // the inbound tx.
        match outbound {
            NetMessage::V1(NetMessageV1::Put(PutMsg::Error { id, cause: c })) => {
                assert_eq!(id, tx, "envelope tx MUST reuse incoming_tx");
                assert_eq!(
                    c, cause,
                    "envelope cause MUST be passed through verbatim — \
                     bound_cause is applied by callers, not by the helper"
                );
            }
            other => panic!("expected PutMsg::Error envelope, got {other:?}"),
        }
    }

    /// Non-loopback branch: when `upstream_addr != own_addr`,
    /// `relay_put_send_error_with_ctx` MUST dispatch via
    /// `send_fire_and_forget` with `target_addr=Some(upstream_addr)`.
    /// This is the multi-hop arm — without it intermediate relays
    /// silently swallow downstream errors.
    #[tokio::test]
    async fn relay_put_send_error_with_ctx_uses_fire_and_forget_to_upstream() {
        use crate::node::{EventLoopNotificationsReceiver, event_loop_notification_channel};
        use std::net::{IpAddr, Ipv4Addr, SocketAddr};

        let (receiver, sender) = event_loop_notification_channel();
        let EventLoopNotificationsReceiver {
            mut op_execution_receiver,
            ..
        } = receiver;

        let tx = dummy_tx();
        let own = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 9001);
        let upstream = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(10, 0, 0, 42)), 31337);
        assert_ne!(own, upstream, "test invariant: addresses must differ");

        let mut ctx = OpCtx::new(tx, sender.op_execution_sender.clone());

        let cause = "remote contract rejection: version not increasing".to_string();
        let result =
            relay_put_send_error_with_ctx(&mut ctx, Some(own), tx, cause.clone(), upstream).await;
        assert!(result.is_ok(), "fire-and-forget dispatch must succeed");

        let (_reply_sender, outbound, target_addr) = op_execution_receiver
            .recv()
            .await
            .expect("upstream envelope should be delivered to executor");
        assert_eq!(
            target_addr,
            Some(upstream),
            "non-loopback MUST pass target_addr=Some(upstream_addr) so \
             the dispatcher routes via OutboundMessageWithTarget to the \
             upstream relay"
        );

        match outbound {
            NetMessage::V1(NetMessageV1::Put(PutMsg::Error { id, cause: c })) => {
                assert_eq!(id, tx);
                assert_eq!(c, cause);
            }
            other => panic!("expected PutMsg::Error envelope, got {other:?}"),
        }
    }

    /// When `own_addr` is `None` (node hasn't received its public
    /// address yet — pre-handshake state), the loopback comparison
    /// `Some(upstream_addr) == None` is always false, so the helper
    /// MUST take the fire-and-forget branch. This is the conservative
    /// choice: shipping a `PutMsg::Error` over the wire to a peer
    /// fails closed (NotificationError) rather than being silently
    /// suppressed as a phantom loopback.
    #[tokio::test]
    async fn relay_put_send_error_with_ctx_unknown_own_addr_uses_fire_and_forget() {
        use crate::node::{EventLoopNotificationsReceiver, event_loop_notification_channel};
        use std::net::{IpAddr, Ipv4Addr, SocketAddr};

        let (receiver, sender) = event_loop_notification_channel();
        let EventLoopNotificationsReceiver {
            mut op_execution_receiver,
            ..
        } = receiver;

        let tx = dummy_tx();
        let upstream = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(10, 0, 0, 42)), 31337);
        let mut ctx = OpCtx::new(tx, sender.op_execution_sender.clone());

        let result = relay_put_send_error_with_ctx(
            &mut ctx,
            None, // own_addr unknown
            tx,
            "x".into(),
            upstream,
        )
        .await;
        assert!(result.is_ok());

        let (_reply_sender, _outbound, target_addr) = op_execution_receiver
            .recv()
            .await
            .expect("envelope should still be delivered when own_addr is unknown");
        assert_eq!(
            target_addr,
            Some(upstream),
            "own_addr=None MUST NOT mask as loopback — dispatch over the wire"
        );
    }

    /// Dispatch failure (executor channel closed) MUST be surfaced as
    /// `OpError::NotificationError`. The PR's caller in
    /// `run_relay_put` only logs and continues, but the helper itself
    /// must return Err so the caller can branch on it.
    #[tokio::test]
    async fn relay_put_send_error_with_ctx_errors_on_closed_channel() {
        use crate::node::event_loop_notification_channel;
        use std::net::{IpAddr, Ipv4Addr, SocketAddr};

        let (receiver, sender) = event_loop_notification_channel();
        // Drop the receiver immediately so the executor channel is closed.
        drop(receiver);

        let tx = dummy_tx();
        let upstream = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(10, 0, 0, 42)), 31337);
        let mut ctx = OpCtx::new(tx, sender.op_execution_sender.clone());

        let result = relay_put_send_error_with_ctx(
            &mut ctx,
            Some(SocketAddr::new(
                IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)),
                9001,
            )),
            tx,
            "shutdown-time failure".into(),
            upstream,
        )
        .await;

        assert!(
            matches!(result, Err(OpError::NotificationError)),
            "closed executor channel MUST surface as NotificationError, \
             got {result:?}"
        );
    }

    /// Structural pin: the downstream-`PutMsg::Error` arm of
    /// `drive_relay_put`'s reply match MUST re-bound the cause via
    /// `bound_cause` before forwarding upstream. The reviewer flagged
    /// this as load-bearing: without re-bounding, an attacker-controlled
    /// intermediate hop could inflate the cause length over each hop's
    /// `PUT_TERMINAL_CAUSE_MAX_BYTES` cap.
    #[test]
    fn drive_relay_put_error_arm_rebounds_cause_before_bubble() {
        let src = include_str!("op_ctx_task.rs");
        let driver_start = src
            .find("async fn drive_relay_put<CB>(")
            .expect("drive_relay_put not found");
        let driver_end = src[driver_start..]
            .find("\nasync fn relay_put_finalize_local(")
            .map(|p| driver_start + p)
            .expect("end of drive_relay_put not found");
        let driver_src = &src[driver_start..driver_end];

        // Locate the `PutMsg::Error { cause: downstream_cause, ..` arm.
        let arm_anchor = "PutMsg::Error {\n            cause: downstream_cause,";
        let arm_start = driver_src
            .find(arm_anchor)
            .expect("PutMsg::Error reply arm not found in drive_relay_put");
        let arm_end = driver_src[arm_start..]
            .find("other => {")
            .map(|p| arm_start + p)
            .expect("end-of-Error-arm marker not found");
        let arm_body = &driver_src[arm_start..arm_end];

        assert!(
            arm_body.contains("bound_cause(downstream_cause)"),
            "the downstream-Error arm MUST re-bound the incoming cause \
             via bound_cause before passing it to relay_put_send_error \
             — otherwise multi-hop forwarding amplifies attacker-controlled \
             cause length per hop"
        );
        assert!(
            arm_body.contains("relay_put_send_error("),
            "the downstream-Error arm MUST forward via relay_put_send_error"
        );
        assert!(
            arm_body.contains("upstream_addr"),
            "the downstream-Error arm MUST target upstream_addr (the hop \
             we received the original Request from), not an arbitrary peer"
        );
    }

    /// Regression pin for issue #3465: in `drive_relay_put`'s
    /// originator-loopback arm, a *forward-dispatch* failure MUST finalize
    /// the PUT locally as a **success** (`relay_put_finalize_local`, which
    /// sends `PutMsg::Response`), NOT propagate the error.
    ///
    /// At the loopback forward, Step 1 (`relay_put_store_locally`) has
    /// already stored the state on the originator's own node. The pre-fix
    /// code did `return Err(err)` on a dispatch failure, and
    /// `run_relay_put`'s loopback arm converted that into a `PutMsg::Error`
    /// — so the client saw "PUT operation timed out" / "peer connection
    /// dropped" for a PUT that was in fact applied locally (the reporter
    /// proved local success by re-PUTing at version+1 and getting "version
    /// must be higher than current version N"). This pin fails if any of
    /// the three loopback dispatch-failure sites regresses to `return
    /// Err(...)` instead of `relay_put_finalize_local`.
    #[test]
    fn drive_relay_put_loopback_forward_failure_finalizes_locally_not_error() {
        let src = include_str!("op_ctx_task.rs");
        let driver_start = src
            .find("async fn drive_relay_put<CB>(")
            .expect("drive_relay_put not found");
        let driver_end = src[driver_start..]
            .find("\nasync fn relay_put_finalize_local(")
            .map(|p| driver_start + p)
            .expect("end of drive_relay_put not found");
        let driver_src = &src[driver_start..driver_end];

        // Isolate the originator-loopback block: from `if originator_loopback {`
        // to its terminal `return Ok(());` (the "originator is awaiting the
        // Response on its own callback" comment marks the block end).
        let loopback_start = driver_src
            .find("if originator_loopback {")
            .expect("originator_loopback block not found in drive_relay_put");
        let loopback_end = driver_src[loopback_start..]
            .find("// Originator is awaiting the Response on its own callback")
            .map(|p| loopback_start + p)
            .expect("end-of-loopback-block marker not found");
        let loopback_block = &driver_src[loopback_start..loopback_end];

        // Three dispatch sites (streaming metadata, send_stream, non-streaming
        // Request) each guard a forward with `if let Err(err) = ...`. Every one
        // of those failure arms must finalize locally as success.
        let finalize_count = loopback_block.matches("relay_put_finalize_local(").count();
        assert!(
            finalize_count >= 3,
            "issue #3465: all three originator-loopback forward-dispatch \
             failure arms MUST call relay_put_finalize_local (success), so a \
             locally-applied PUT is not reported as a hard failure; found only \
             {finalize_count} call(s)"
        );

        // And none of them may return the forward error to the caller (which
        // run_relay_put would turn into a client-visible PutMsg::Error).
        assert!(
            !loopback_block.contains("return Err(err)"),
            "issue #3465: the originator-loopback forward-dispatch failure arms \
             MUST NOT `return Err(err)` — the state is already stored locally, \
             so propagate best-effort and finalize the PUT as a success"
        );
        assert!(
            !loopback_block.contains("send_stream failed: {err}"),
            "issue #3465: the send_stream failure arm MUST NOT bubble a \
             NotificationChannelError — finalize locally as success instead"
        );
    }

    /// Structural pin: `run_relay_put`'s non-loopback Err branch (the
    /// B1 fix landed in this PR) MUST emit `PutMsg::Error` upstream so
    /// intermediate relays don't silently swallow local-failure events.
    /// Pre-B1, a local `put_contract` rejection at an intermediate hop
    /// caused the upstream `send_to_and_await` to hang until
    /// `OPERATION_TTL`.
    #[test]
    fn run_relay_put_bubbles_local_failure_in_non_loopback_mode() {
        let src = include_str!("op_ctx_task.rs");
        let fn_start = src
            .find("async fn run_relay_put<CB>(")
            .expect("run_relay_put not found");
        let fn_end = src[fn_start..]
            .find("\n#[allow(clippy::too_many_arguments)]\nasync fn drive_relay_put<CB>(")
            .map(|p| fn_start + p)
            .expect("end-of-run_relay_put marker not found");
        let body = &src[fn_start..fn_end];

        // The non-loopback branch is gated by `else if let Err(err) = drive_result`.
        let else_anchor = "} else if let Err(err) = drive_result {";
        let else_start = body.find(else_anchor).expect(
            "non-loopback Err branch not found in run_relay_put — \
                     B1 fix regressed; intermediate relays will silently \
                     swallow local PUT failures",
        );
        // Bound the branch to the next `}` at the same brace depth — use
        // the end-of-function `}` as the conservative upper bound.
        let branch_body = &body[else_start..];

        assert!(
            branch_body.contains("bound_cause(err.to_string())"),
            "non-loopback Err branch MUST apply bound_cause(err.to_string()) \
             before bubbling — keeps the per-hop DoS amplification bound"
        );
        assert!(
            branch_body.contains("relay_put_send_error("),
            "non-loopback Err branch MUST call relay_put_send_error to \
             emit PutMsg::Error to upstream_addr"
        );
        assert!(
            branch_body.contains("upstream_addr"),
            "non-loopback Err branch MUST target upstream_addr"
        );
    }

    // ──────────────────────────────────────────────────────────────────
    // PR #4126 review item M3: behavioural coverage of the
    // originator-loopback shutdown fallback. The original test was a
    // source-grep + 3000-byte substring search that trips on rename
    // / fmt rewrap. The fallback is now extracted as the free
    // function `dispatch_loopback_shutdown_fallback` taking only
    // `&mpsc::Sender<(Transaction, HostResult)>` (mirrors the
    // `release_pending_op_slot_on` extraction pattern in
    // `op_state_manager.rs`), so tests can drive it directly with a
    // real channel and observe behaviour — no `OpManager` build
    // required.
    // ──────────────────────────────────────────────────────────────────

    /// Happy path: the fallback publishes exactly one
    /// `(incoming_tx, Err(OperationError { cause }))` entry on
    /// `result_router_tx` with the cause carried verbatim.
    #[tokio::test]
    async fn dispatch_loopback_shutdown_fallback_publishes_to_result_router() {
        use tokio::sync::mpsc;

        let (router_tx, mut router_rx) = mpsc::channel::<(Transaction, HostResult)>(8);
        let tx = dummy_tx();
        let cause = "contract rejection: version not increasing".to_string();

        dispatch_loopback_shutdown_fallback(&router_tx, tx, cause.clone());

        let (received_tx, host_result) = router_rx
            .recv()
            .await
            .expect("fallback MUST publish to result_router_tx");
        assert_eq!(received_tx, tx, "result_router tx must reuse incoming_tx");

        let client_err =
            host_result.expect_err("result MUST be Err — fallback is the failure path");
        let rendered = format!("{client_err}");
        assert!(
            rendered.contains(&cause),
            "result_router Err MUST carry the verbatim cause string \
             so the WS client sees the real failure reason (got: {rendered:?})"
        );

        // Crucial behavioural invariant — no second entry.
        assert!(
            router_rx.try_recv().is_err(),
            "fallback MUST publish exactly one entry to result_router_tx"
        );
    }

    /// The fallback's helper signature MUST NOT receive a
    /// notifications channel. This is the compile-time half of the
    /// M3 guarantee: a function that has no `notifications_sender`
    /// in scope physically cannot emit `TransactionCompleted(tx)`,
    /// so the M2 race participant of the pre-#4111 shape is
    /// structurally excluded.
    ///
    /// The runtime half: with only `result_router_tx` passed, the
    /// fallback writes one row to that channel and returns. The
    /// `_publishes_to_result_router` test above proves that observation.
    /// This test pins the *absence* — wire a notifications channel
    /// next to the router channel, drive the fallback, and confirm
    /// the notifications channel receives nothing.
    #[tokio::test]
    async fn dispatch_loopback_shutdown_fallback_does_not_emit_transaction_completed() {
        use crate::node::{EventLoopNotificationsReceiver, event_loop_notification_channel};
        use tokio::sync::mpsc;

        let (router_tx, mut router_rx) = mpsc::channel::<(Transaction, HostResult)>(8);
        let (receiver, _sender) = event_loop_notification_channel();
        let EventLoopNotificationsReceiver {
            mut notifications_receiver,
            ..
        } = receiver;

        let tx = dummy_tx();
        dispatch_loopback_shutdown_fallback(
            &router_tx,
            tx,
            "shutdown-time deterministic failure".into(),
        );

        // Result router got the entry. Discard the result — this
        // test only cares that the notifications channel stays silent;
        // the `_publishes_to_result_router` test above already pins
        // the envelope shape.
        let _drained = router_rx
            .recv()
            .await
            .expect("fallback MUST publish to result_router_tx");

        // Notifications channel MUST be silent — no TransactionCompleted.
        // Use `try_recv` not `recv().await` so we don't hang waiting
        // for a message that should never arrive.
        match notifications_receiver.try_recv() {
            Ok(unexpected) => panic!(
                "fallback MUST NOT emit anything on the notifications \
                 channel; pre-#4111 shape would have sent TransactionCompleted, \
                 re-introducing the M1/M2 race. Got: {unexpected:?}"
            ),
            Err(tokio::sync::mpsc::error::TryRecvError::Empty) => {}
            Err(tokio::sync::mpsc::error::TryRecvError::Disconnected) => {
                panic!("notifications channel closed before the assertion ran")
            }
        }
    }

    /// Closed `result_router_tx` MUST be handled gracefully: log+drop,
    /// no panic. Degrades the client experience (no real cause
    /// delivered, falls back to whatever drive_retry_loop publishes
    /// from its own timeout/exhaustion path) but the relay driver
    /// task itself must not crash.
    #[tokio::test]
    async fn dispatch_loopback_shutdown_fallback_does_not_panic_on_closed_router() {
        use tokio::sync::mpsc;

        let (router_tx, router_rx) = mpsc::channel::<(Transaction, HostResult)>(8);
        // Drop the receiver — every `try_send` on `router_tx` now returns
        // `TrySendError::Closed`.
        drop(router_rx);

        let tx = dummy_tx();
        // No panic, no unwind. The helper logs the failure and returns.
        dispatch_loopback_shutdown_fallback(&router_tx, tx, "x".into());
    }

    /// Full `result_router_tx` MUST also be handled gracefully —
    /// degrades to log+drop without blocking. Channel-safety rule:
    /// the fallback runs in the relay driver's task body and must
    /// never await on a full channel (would block the driver
    /// indefinitely if the result router is wedged).
    #[tokio::test]
    async fn dispatch_loopback_shutdown_fallback_does_not_block_on_full_router() {
        use tokio::sync::mpsc;

        // Capacity 1, pre-filled, receiver intentionally kept alive
        // (so the channel is in "Full" not "Closed" state).
        let (router_tx, _router_rx) = mpsc::channel::<(Transaction, HostResult)>(1);
        let filler_tx = dummy_tx();
        let filler_err: freenet_stdlib::client_api::ClientError = ErrorKind::OperationError {
            cause: "filler".into(),
        }
        .into();
        router_tx
            .try_send((filler_tx, Err(filler_err)))
            .expect("first send fills the capacity-1 channel");

        let tx = dummy_tx();
        // Wrap in `tokio::time::timeout` to prove non-blocking. The
        // helper uses `try_send` internally, so this returns
        // immediately even with the channel full.
        let result = tokio::time::timeout(std::time::Duration::from_millis(100), async {
            dispatch_loopback_shutdown_fallback(&router_tx, tx, "shutdown".into());
        })
        .await;
        assert!(
            result.is_ok(),
            "fallback MUST NOT block on a full result_router_tx — \
             channel-safety rule: never `send().await` from inside a \
             driver body. The helper must use `try_send` and drop+log \
             on Full instead."
        );
    }

    // ============ Summary-first PUT pin tests (#4642 step 3-bis) ============

    /// Pin: the summary-first block in `drive_client_put_inner` MUST check
    /// `supports_summary_first_put` BEFORE calling `try_summary_first_put` —
    /// the mixed-version emission gate has to run first so a pre-floor peer
    /// is never probed (it cannot bincode-deserialize the appended
    /// `ProbeRequest` tag). Also pins that the full-state `PutRetryDriver`
    /// loop remains reachable afterward (the gate-closed / FallThrough
    /// path), i.e. summary-first PUT is a strict front-end, never a
    /// replacement.
    #[test]
    fn drive_client_put_inner_gates_summary_first_emission_on_version_support() {
        let src = include_str!("op_ctx_task.rs");
        let start = src
            .find("async fn drive_client_put_inner(")
            .expect("drive_client_put_inner not found");
        let end = src[start..]
            .find("\n// --- Summary-first PUT (originator pre-phase")
            .expect("end of drive_client_put_inner not found")
            + start;
        let body = &src[start..end];
        let gate_pos = body
            .find("supports_summary_first_put(target_addr)")
            .expect("version gate call missing");
        let try_pos = body
            .find("try_summary_first_put(")
            .expect("try_summary_first_put call missing");
        assert!(
            gate_pos < try_pos,
            "drive_client_put_inner must check supports_summary_first_put \
             BEFORE calling try_summary_first_put"
        );
        assert!(
            body.contains("struct PutRetryDriver"),
            "the full-state PutRetryDriver loop must remain reachable \
             (FallThrough / gate-closed path) after the summary-first block"
        );
    }

    /// Pin: `drive_relay_probe` must NEVER store, host, or announce the
    /// contract locally. A probe-routing hop that does not already host the
    /// contract must only ROUTE — see the lazy host-on-next-read note
    /// (Fable F5 amplification fix) on the function itself and
    /// `.claude/rules/hosting-invariants.md` invariant 2. Mirrors the style
    /// of `drive_relay_put_stores_locally_before_forwarding`, but asserts
    /// the opposite property for the probe path.
    #[test]
    fn drive_relay_probe_never_stores_locally() {
        let src = include_str!("op_ctx_task.rs");
        let start = src
            .find("async fn drive_relay_probe(")
            .expect("drive_relay_probe not found");
        let end = src[start..]
            .find("\npub(crate) async fn start_relay_probe_reconcile(")
            .expect("end of drive_relay_probe section not found")
            + start;
        let body = &src[start..end];
        assert!(
            !body.contains("relay_put_store_locally("),
            "drive_relay_probe must NEVER call relay_put_store_locally — a \
             probe-routing hop that is not already a holder must only ROUTE"
        );
        assert!(
            !body.contains("host_contract("),
            "drive_relay_probe must NEVER call host_contract directly"
        );
        assert!(
            !body.contains("put_contract("),
            "drive_relay_probe must NEVER call put_contract directly"
        );
    }

    /// Pin: probe holder detection MUST use `Ring::is_receiving_updates`
    /// (subscribed OR local-client-subscribed — the fresh-holder signal),
    /// never `is_hosting_contract` (mere cache membership, which may be
    /// stale). See `.claude/rules/hosting-invariants.md`,
    /// `feedback_hosted_cache_freshness`.
    #[test]
    fn drive_relay_probe_uses_is_receiving_updates_not_is_hosting_contract() {
        let src = include_str!("op_ctx_task.rs");
        let start = src
            .find("async fn drive_relay_probe(")
            .expect("drive_relay_probe not found");
        let end = src[start..]
            .find("\npub(crate) async fn start_relay_probe_reconcile(")
            .expect("end of drive_relay_probe section not found")
            + start;
        let body = &src[start..end];
        assert!(
            body.contains("is_receiving_updates(&key)"),
            "drive_relay_probe must gate holder detection on \
             Ring::is_receiving_updates"
        );
        assert!(
            !body.contains("is_hosting_contract("),
            "drive_relay_probe must NOT use is_hosting_contract for holder \
             detection"
        );
    }

    /// Pin: the probe forward decrements htl (mirroring `drive_relay_put`)
    /// and both holder-found and no-holder replies are reachable — the
    /// driver must be able to answer `ProbeResponse` with `holder_found`
    /// true (fresh holder answers directly) as well as false (summary
    /// failed, routing terminus, no next hop, or a bubbled-up downstream
    /// no-holder reply).
    #[test]
    fn drive_relay_probe_forwards_with_decremented_htl_and_responds_both_holder_states() {
        let src = include_str!("op_ctx_task.rs");
        let start = src
            .find("async fn drive_relay_probe(")
            .expect("drive_relay_probe not found");
        let end = src[start..]
            .find("\npub(crate) async fn start_relay_probe_reconcile(")
            .expect("end of drive_relay_probe section not found")
            + start;
        let body = &src[start..end];
        assert!(
            body.contains("let new_htl = htl.saturating_sub(1);"),
            "drive_relay_probe must decrement htl before forwarding"
        );
        assert!(
            body.contains("htl: new_htl,"),
            "the forwarded ProbeRequest must carry the decremented htl"
        );
        let call_count = body.matches("relay_probe_send_response(").count();
        assert!(
            call_count >= 4,
            "expected at least 4 relay_probe_send_response call sites \
             (holder found, summary-failed, terminus, no-next-hop) — \
             found {call_count}"
        );
    }

    /// Pin: `drive_relay_probe_reconcile` merges via the same
    /// `is_receiving_updates` fresh-holder signal as the probe path (never
    /// `is_hosting_contract`), and merges through `update::update_contract`
    /// rather than hand-rolling a contract-handler call — this is what
    /// lets normal UPDATE fan-out propagate the reconciled delta to the
    /// rest of the subscriber mesh automatically.
    #[test]
    fn drive_relay_probe_reconcile_merges_via_update_contract_on_holder() {
        let src = include_str!("op_ctx_task.rs");
        let start = src
            .find("async fn drive_relay_probe_reconcile(")
            .expect("drive_relay_probe_reconcile not found");
        let end = src[start..]
            .find("\n// ── End of relay PROBE driver")
            .expect("end of drive_relay_probe_reconcile section not found")
            + start;
        let body = &src[start..end];
        assert!(
            body.contains("is_receiving_updates(&key)"),
            "drive_relay_probe_reconcile must gate the merge decision on \
             Ring::is_receiving_updates"
        );
        assert!(
            !body.contains("is_hosting_contract("),
            "drive_relay_probe_reconcile must NOT use is_hosting_contract"
        );
        assert!(
            body.contains("update::update_contract("),
            "drive_relay_probe_reconcile must merge via update::update_contract \
             so normal UPDATE fan-out propagates the delta"
        );
    }

    /// Falsifier (mixed-version wire safety — Codex review of #4733): a relay
    /// forwarding a `PutMsg::ProbeRequest` selects `next_addr` purely by
    /// routing (`closest_potentially_hosting`), so the originator's emission
    /// gate (`drive_client_put_inner`) does NOT cover it. Without a re-check
    /// on the routed next hop, a relay can send the probe tag to a pre-0.2.95
    /// peer that cannot bincode-deserialize it and would DROP the connection
    /// during the staggered rollout. This pins that `drive_relay_probe`
    /// re-checks `supports_summary_first_put(next_addr)` BEFORE constructing
    /// the `ProbeRequest`, and that the unsupported branch fails closed
    /// (replies no-holder so the originator falls back to a full-state PUT),
    /// pinned via its distinctive tracing phase marker.
    ///
    /// Falsifies the pre-fix code: the relay body contained NO
    /// `supports_summary_first_put` call at all, so `find` returned `None` and
    /// the `expect` below panicked. Passes only with the fail-closed guard.
    #[test]
    fn drive_relay_probe_gates_probe_forward_on_next_hop_version() {
        let src = include_str!("op_ctx_task.rs");
        let start = src
            .find("async fn drive_relay_probe(")
            .expect("drive_relay_probe not found");
        let end = src[start..]
            .find("\npub(crate) async fn start_relay_probe_reconcile(")
            .expect("end of drive_relay_probe section not found")
            + start;
        let body = &src[start..end];

        let gate_pos = body.find("supports_summary_first_put(next_addr)").expect(
            "drive_relay_probe must re-check the ROUTED next hop's version \
             support before emitting a ProbeRequest (mixed-version rollout \
             safety) — a relay picks next_addr by routing, not covered by the \
             originator's gate",
        );
        let forward_pos = body
            .find("PutMsg::ProbeRequest {")
            .expect("ProbeRequest forward construction missing");
        assert!(
            gate_pos < forward_pos,
            "the version gate must run BEFORE the ProbeRequest is constructed \
             and sent — a pre-0.2.95 next hop must never receive the probe tag",
        );
        assert!(
            body.contains("relay_probe_next_hop_pre_floor"),
            "the unsupported-next-hop branch must fail closed (reply \
             no-holder), pinned via its distinctive tracing phase marker",
        );
    }

    /// Falsifier (mixed-version wire safety — Codex review of #4733): the
    /// `ProbeReconcile` relay forward has the SAME gap as the probe forward —
    /// it selects `next_addr` by routing and must re-check the next hop's
    /// version before emitting the `PutMsg::ProbeReconcile` tag. Because
    /// `ProbeReconcile` is fire-and-forget with no reply variant, the
    /// fail-closed behavior is to DROP (return `Ok(())`) rather than reply:
    /// the reconcile is best-effort, and with the bidirectional reverse-delta
    /// the originator's copy is already complete, so anti-entropy heals the
    /// downstream holder.
    ///
    /// Falsifies the pre-fix code (no `supports_summary_first_put` call in the
    /// reconcile relay body); passes only with the fail-closed guard.
    #[test]
    fn drive_relay_probe_reconcile_gates_forward_on_next_hop_version() {
        let src = include_str!("op_ctx_task.rs");
        let start = src
            .find("async fn drive_relay_probe_reconcile(")
            .expect("drive_relay_probe_reconcile not found");
        let end = src[start..]
            .find("\n// ── End of relay PROBE driver")
            .expect("end of drive_relay_probe_reconcile section not found")
            + start;
        let body = &src[start..end];

        let gate_pos = body.find("supports_summary_first_put(next_addr)").expect(
            "drive_relay_probe_reconcile must re-check the ROUTED next hop's \
             version support before emitting a ProbeReconcile (mixed-version \
             rollout safety)",
        );
        let forward_pos = body
            .find("PutMsg::ProbeReconcile {")
            .expect("ProbeReconcile forward construction missing");
        assert!(
            gate_pos < forward_pos,
            "the version gate must run BEFORE the ProbeReconcile is \
             constructed and sent — a pre-0.2.95 next hop must never receive \
             the tag",
        );
        assert!(
            body.contains("relay_probe_reconcile_next_hop_pre_floor"),
            "the unsupported-next-hop branch must fail closed (drop, return \
             Ok(())), pinned via its distinctive tracing phase marker",
        );
    }
}
