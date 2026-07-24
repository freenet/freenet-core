//! Per-transaction execution context for the driver model.
//!
//! Ships the `OpCtx` struct and its `send_and_await` round-trip
//! primitive. Each transaction is owned and driven by a single
//! task; state lives in task locals rather than the legacy
//! `OpManager.ops.*` DashMap.

use std::net::SocketAddr;

use tokio::sync::mpsc;

use crate::message::{MessageStats, NetMessage, Transaction};
use crate::node::{OpExecutionPayload, WaiterReply};
use crate::operations::OpError;

/// Per-transaction execution context for the driver model.
///
/// An `OpCtx` binds a single [`Transaction`] to the channel used to
/// drive its network round-trip through the event loop. The context
/// is [`Send`] but intentionally not [`Clone`].
pub(crate) struct OpCtx {
    tx: Transaction,
    op_execution_sender: mpsc::Sender<OpExecutionPayload>,
}

impl OpCtx {
    /// Construct a new context bound to `tx`.
    ///
    /// `pub(crate)` because the only legitimate constructors are
    /// [`crate::node::OpManager::op_ctx`] and the in-module tests.
    pub(crate) fn new(
        tx: Transaction,
        op_execution_sender: mpsc::Sender<OpExecutionPayload>,
    ) -> Self {
        Self {
            tx,
            op_execution_sender,
        }
    }

    /// The transaction this context is bound to.
    ///
    /// Kept on the API for future per-tx inbox / `OpRegistry`
    /// lookups; current production callers hold the attempt tx in a
    /// local instead.
    #[allow(dead_code)]
    pub fn tx(&self) -> Transaction {
        self.tx
    }

    /// Send `msg` through the event loop and await a single reply
    /// keyed by the same [`Transaction`].
    ///
    /// # Single-use per [`Transaction`]
    ///
    /// The reply callback fires exactly once per tx because the
    /// `completed`/`under_progress` dedup sets suppress subsequent
    /// dispatches. A second call with the same tx will hang on
    /// `response_receiver.recv()`. Multi-attempt protocols (e.g.
    /// SUBSCRIBE's fallback to alternative peers) must allocate a
    /// fresh [`Transaction`] per attempt.
    ///
    /// # Deadlock risk
    ///
    /// `response_receiver.recv()` has no timeout. The reply side
    /// uses `try_send` so the pure-network-message handler cannot
    /// block; the remaining risk is on the caller side. Callers
    /// must guarantee the awaiting task is not the sole driver of
    /// completion, or wrap the await in an explicit timeout (see
    /// `.claude/rules/channel-safety.md`).
    ///
    /// # Where to call this
    ///
    /// Must be called from an op task (spawned via
    /// [`crate::config::GlobalExecutor::spawn`]), not from the main
    /// event loop. The internal `.send().await` on
    /// `op_execution_sender` is bounded and can block; spawned tasks
    /// are OK, event loops are not.
    ///
    /// # Set up state, then send
    ///
    /// All task-local state the reply handler will read (retry
    /// counters, visited-peer filters, pending sub-ops) must be
    /// initialized before calling this method.
    ///
    /// # Terminal reply, not success reply
    ///
    /// The returned [`NetMessage`] is whatever the pipeline produced
    /// when `is_operation_completed` flipped true, including
    /// non-success terminal states (e.g.
    /// `SubscribeMsg::Response::NotFound`). `Ok(reply)` does NOT
    /// imply protocol success — callers must inspect the reply.
    ///
    /// # Errors
    ///
    /// Returns [`OpError::NotificationError`] if the executor channel
    /// is closed (send failure) or the reply receiver is dropped
    /// (receiver hang-up).
    #[allow(dead_code)]
    pub async fn send_and_await(&mut self, msg: NetMessage) -> Result<NetMessage, OpError> {
        self.send_and_await_inner(msg, None).await
    }

    /// Like [`Self::send_and_await`] but dispatches the message directly to
    /// `target_addr` over the network instead of looping it back to the local
    /// event loop as an `InboundMessage`.
    ///
    /// This is the load-bearing variant for ops that need their first hop to
    /// reach a remote peer even when local processing of the same message
    /// would short-circuit. The motivating case is client-initiated SUBSCRIBE
    /// when the contract is already cached locally (#3838): looping the
    /// `Subscribe::Request` back through `process_message` hits the
    /// "originator has contract locally" branch and synthesizes a success
    /// reply without the gateway ever seeing the request, so the home node
    /// never learns we want updates. By emitting
    /// [`crate::node::network_bridge::p2p_protoc::ConnEvent::OutboundMessageWithTarget`]
    /// instead, the request reaches the home node and triggers
    /// `register_downstream_subscriber` there. The reply still flows back via
    /// the same `pending_op_results` callback registered by
    /// `handle_op_execution`.
    pub async fn send_to_and_await(
        &mut self,
        target_addr: SocketAddr,
        msg: NetMessage,
    ) -> Result<NetMessage, OpError> {
        self.send_and_await_inner(msg, Some(target_addr)).await
    }

    /// Fire-and-forget send: dispatch `msg` to `target_addr` over the network
    /// without awaiting a reply.
    ///
    /// The message flows through `handle_op_execution` as
    /// [`ConnEvent::OutboundMessageWithTarget`], the same path used by
    /// [`Self::send_to_and_await`]. The difference is that the response
    /// receiver is dropped immediately — the `pending_op_results` callback
    /// becomes reclaimable on the next periodic sweep (its receiver half is
    /// closed, so `Sender::is_closed()` returns `true`).
    ///
    /// # Cleanup of the `pending_op_results` entry
    ///
    /// Do **not** call `release_pending_op_slot` immediately after this
    /// method. `release_pending_op_slot` sends `TransactionCompleted` on
    /// the notification channel (higher priority than op_execution), so it
    /// can arrive at the event loop *before* `handle_op_execution` inserts
    /// the callback — making the cleanup a no-op. Instead, let the caller's
    /// subsequent `send_client_result` emit `TransactionCompleted`, which
    /// runs after `handle_op_execution` has processed the outbound message.
    ///
    /// Load-bearing primitive for UPDATE: applies the update locally
    /// and fires a `RequestUpdate` to a remote peer without waiting
    /// for acknowledgement.
    pub async fn send_fire_and_forget(
        &mut self,
        target_addr: SocketAddr,
        msg: NetMessage,
    ) -> Result<(), OpError> {
        debug_assert_eq!(
            msg.id(),
            &self.tx,
            "OpCtx::send_fire_and_forget: msg.id must match ctx.tx"
        );

        let (response_sender, _response_receiver) = mpsc::channel::<WaiterReply>(1);
        // _response_receiver is dropped here. The sender stored in
        // pending_op_results becomes is_closed() == true, reclaimable by
        // the 60s sweep or an explicit release_pending_op_slot call.

        self.op_execution_sender
            .send((response_sender, msg, Some(target_addr)))
            .await
            .map_err(|_| OpError::NotificationError)
    }

    /// Fire-and-forget loopback: dispatch `msg` to the local event loop
    /// as an `InboundMessage` (target=None semantics) without awaiting
    /// a reply.
    ///
    /// Used by `relay_put_finalize_local` when the relay driver runs
    /// on the originator's own node (originator-loopback PUT).
    /// Sending a wire-bound `PutMsg::Response` to `own_addr` fails —
    /// there's no self-connection. Routing it through `InboundMessage`
    /// lands at `handle_pure_network_message_v1`'s PUT bypass and
    /// forwards to the originator's `pending_op_results` waiter via
    /// `try_forward_driver_reply`.
    pub async fn send_local_loopback(&mut self, msg: NetMessage) -> Result<(), OpError> {
        debug_assert_eq!(
            msg.id(),
            &self.tx,
            "OpCtx::send_local_loopback: msg.id must match ctx.tx"
        );

        let (response_sender, _response_receiver) = mpsc::channel::<WaiterReply>(1);

        self.op_execution_sender
            .send((response_sender, msg, None))
            .await
            .map_err(|_| OpError::NotificationError)
    }

    /// Dispatch `msg` to `target_addr` and return the reply receiver
    /// without awaiting the response.
    ///
    /// Callers that need to interleave the reply await with other work
    /// (e.g., driving a stream-fork piping path in parallel with the
    /// downstream request/reply round-trip) use this primitive so the
    /// `pending_op_results` waiter is installed BEFORE the caller
    /// kicks off side work. Installing the waiter first closes a race
    /// where a fast downstream reply would land on the event loop
    /// before `handle_op_execution` has inserted the callback —
    /// `try_forward_driver_reply` would then drop the reply as
    /// `OpNotPresent`.
    ///
    /// Await the returned receiver to get the terminal reply:
    ///
    /// ```ignore
    /// let mut rx = ctx.send_to_and_register_waiter(addr, msg).await?;
    /// // ... side work that must run in parallel ...
    /// match rx.recv().await {
    ///     Some(reply) => /* ... */,
    ///     None => /* channel closed — treat as infra error */,
    /// }
    /// ```
    ///
    /// # Errors
    ///
    /// Returns [`OpError::NotificationError`] if the event-loop
    /// `op_execution_sender` channel is closed (receiver dropped —
    /// typically only happens during node shutdown). The caller
    /// should treat this as an infrastructure failure.
    pub async fn send_to_and_register_waiter(
        &mut self,
        target_addr: SocketAddr,
        msg: NetMessage,
    ) -> Result<mpsc::Receiver<WaiterReply>, OpError> {
        debug_assert_eq!(
            msg.id(),
            &self.tx,
            "OpCtx::send_to_and_register_waiter: msg.id must match ctx.tx"
        );

        let (response_sender, response_receiver) = mpsc::channel::<WaiterReply>(1);

        self.op_execution_sender
            .send((response_sender, msg, Some(target_addr)))
            .await
            .map_err(|_| OpError::NotificationError)?;

        Ok(response_receiver)
    }

    /// Dispatch `msg` and return a multi-reply receiver. Caller drains
    /// the receiver until its own completion predicate fires.
    ///
    /// Load-bearing primitive for ops with fan-in semantics:
    /// CONNECT sends a single Request and expects up to
    /// `target_connections` ConnectResponses over time as relay
    /// branches accept the joiner. `send_and_await` and
    /// `send_to_and_register_waiter` use a capacity-1 channel and
    /// cannot model fan-in.
    ///
    /// # When the bypass forwards multiple replies
    ///
    /// `node::try_forward_driver_reply` calls `try_send` on the
    /// stored callback. With a capacity > 1 channel, multiple inbound
    /// replies for the same tx land in the channel without dropping each
    /// other (capacity-1 callers would receive only the first because the
    /// receiver hangs up after `recv().await` resolves once and the second
    /// `try_send` fails as `Closed`). Bypass call sites that want
    /// multi-reply behaviour must opt in by NOT returning early after the
    /// first forward; the channel-side capacity governs how many concurrent
    /// replies can be buffered before `try_send` drops.
    ///
    /// # Cleanup
    ///
    /// The `pending_op_results` slot is NOT cleaned up by this primitive.
    /// Callers must call `OpManager::release_pending_op_slot(tx)` when the
    /// driver task exits, or the entry will live until the 60s sweep.
    ///
    /// # Errors
    ///
    /// Returns [`OpError::NotificationError`] if the event-loop
    /// `op_execution_sender` channel is closed (receiver dropped —
    /// typically only happens during node shutdown).
    #[allow(dead_code)]
    pub async fn send_and_collect_replies(
        &mut self,
        msg: NetMessage,
        capacity: usize,
    ) -> Result<mpsc::Receiver<WaiterReply>, OpError> {
        debug_assert_eq!(
            msg.id(),
            &self.tx,
            "OpCtx::send_and_collect_replies: msg.id must match ctx.tx"
        );
        debug_assert!(
            capacity >= 1,
            "OpCtx::send_and_collect_replies: capacity must be >= 1"
        );

        let (response_sender, response_receiver) = mpsc::channel::<WaiterReply>(capacity);

        self.op_execution_sender
            .send((response_sender, msg, None))
            .await
            .map_err(|_| OpError::NotificationError)?;

        Ok(response_receiver)
    }

    /// Like [`Self::send_and_collect_replies`] but dispatches to a specific
    /// target instead of looping back to the local event loop.
    ///
    /// CONNECT joiner sends Request to its first-hop gateway via this
    /// primitive: the request travels over the network, fans out through
    /// relays, and ConnectResponses bubble back along distinct paths to
    /// the joiner.
    pub async fn send_to_and_collect_replies(
        &mut self,
        target_addr: SocketAddr,
        msg: NetMessage,
        capacity: usize,
    ) -> Result<mpsc::Receiver<WaiterReply>, OpError> {
        debug_assert_eq!(
            msg.id(),
            &self.tx,
            "OpCtx::send_to_and_collect_replies: msg.id must match ctx.tx"
        );
        debug_assert!(
            capacity >= 1,
            "OpCtx::send_to_and_collect_replies: capacity must be >= 1"
        );

        let (response_sender, response_receiver) = mpsc::channel::<WaiterReply>(capacity);

        self.op_execution_sender
            .send((response_sender, msg, Some(target_addr)))
            .await
            .map_err(|_| OpError::NotificationError)?;

        Ok(response_receiver)
    }

    async fn send_and_await_inner(
        &mut self,
        msg: NetMessage,
        target_addr: Option<SocketAddr>,
    ) -> Result<NetMessage, OpError> {
        debug_assert_eq!(
            msg.id(),
            &self.tx,
            "OpCtx::send_and_await: msg.id must match ctx.tx"
        );

        let (response_sender, mut response_receiver) = mpsc::channel::<WaiterReply>(1);

        self.op_execution_sender
            .send((response_sender, msg, target_addr))
            .await
            .map_err(|_| OpError::NotificationError)?;

        self.recv_waiter_reply(&mut response_receiver).await
    }

    /// Await the next reply on a `pending_op_results[tx]` waiter channel.
    ///
    /// Single chokepoint: the `WaiterReply` enum forces every caller to
    /// handle the terminal `PeerDisconnected` signal the prune path
    /// delivers through the channel (#4313). On a `PeerDisconnected`
    /// item the awaited peer was pruned mid-flight, so the driver should
    /// advance to another route; a bare channel close with no terminal
    /// item is a genuine teardown and falls back to `NotificationError`.
    pub(crate) async fn recv_waiter_reply(
        &self,
        receiver: &mut mpsc::Receiver<WaiterReply>,
    ) -> Result<NetMessage, OpError> {
        match receiver.recv().await {
            Some(WaiterReply::Reply(reply)) => {
                // A mismatched reply tx would be a bug in the bypass
                // layer; catch it in debug builds.
                debug_assert_eq!(
                    reply.id(),
                    &self.tx,
                    "recv_waiter_reply: reply tx must match ctx.tx"
                );
                Ok(reply)
            }
            Some(WaiterReply::PeerDisconnected { peer }) => Err(OpError::PeerDisconnected { peer }),
            // Genuine teardown with no terminal signal (executor torn
            // down, callback dropped): pre-#4313 fallback.
            None => Err(OpError::NotificationError),
        }
    }
}

// ─────────────────────────────────────────────────────────────────
// Shared retry-loop driver.
//
// Encapsulates the tx-split + timeout + cleanup + classification
// boilerplate. Op-specific pieces are provided via `RetryDriver`.
// ─────────────────────────────────────────────────────────────────

use crate::config::OPERATION_TTL;
use crate::node::OpManager;

/// What `classify` returns for each terminal reply.
#[allow(dead_code)] // Retry used by SUBSCRIBE; all variants needed for future ops
pub(crate) enum AttemptOutcome<T> {
    /// The reply is a terminal success. Carries the caller's domain value.
    Terminal(T),
    /// The peer couldn't help but we should retry (e.g., NotFound).
    Retry,
    /// A reply variant that should never reach the driver (e.g.,
    /// ForwardingAck slipping past the bypass filter).
    Unexpected,
}

/// What `advance` returns.
pub(crate) enum AdvanceOutcome {
    /// Retry with the next peer.
    Next,
    /// All peers exhausted.
    Exhausted,
}

/// Terminal outcome of the shared retry loop.
#[allow(dead_code)] // InfraError used when send_and_await returns Err inside the loop
pub(crate) enum RetryLoopOutcome<T> {
    /// A terminal reply was classified successfully.
    Done(T),
    /// All peers exhausted after retries. Carries a human-readable cause.
    Exhausted(String),
    /// An unexpected reply variant was received.
    Unexpected,
    /// Infrastructure error (executor channel closed, etc.).
    InfraError(OpError),
}

/// Op-specific behaviour for the shared retry loop.
///
/// Implementors hold their own routing state (tried peers, retry
/// counters, etc.) as fields, avoiding the borrow-conflict that
/// closure-based APIs would hit when `build_request` reads state
/// that `advance` mutates.
pub(crate) trait RetryDriver {
    /// The domain value extracted from a successful terminal reply.
    type Terminal;

    /// Allocate a fresh `Transaction` for retry attempts.
    fn new_attempt_tx(&mut self) -> Transaction;

    /// Construct the wire message for this attempt.
    fn build_request(&mut self, attempt_tx: Transaction) -> NetMessage;

    /// Classify a terminal reply from the network.
    fn classify(&mut self, reply: NetMessage) -> AttemptOutcome<Self::Terminal>;

    /// Pick the next peer or signal exhaustion.
    fn advance(&mut self) -> AdvanceOutcome;

    /// Per-attempt wall-clock timeout. Defaults to [`OPERATION_TTL`].
    ///
    /// PUT overrides this to scale with payload size: large streaming
    /// payloads need significantly longer than `OPERATION_TTL` to
    /// complete a single attempt, otherwise the retry loop fires
    /// while the original streaming op is still in flight (#4001).
    fn attempt_timeout(&self) -> std::time::Duration {
        OPERATION_TTL
    }

    /// Optional per-attempt stream-progress liveness for streaming PUTs (#4001).
    ///
    /// Defaults to `None`: GET / SUBSCRIBE and non-streaming drivers use the
    /// fixed [`Self::attempt_timeout`] path unchanged. When `Some`, the retry
    /// loop replaces the fixed per-attempt deadline with a stream-inactivity
    /// timeout driven by the returned [`StreamProgress`] (Notify + atomic
    /// tiebreak) bounded by the hard
    /// [`crate::operations::STREAMING_ATTEMPT_TIMEOUT_CAP`] ceiling. PUT
    /// overrides this only for streaming-eligible payloads. The seam is
    /// additive: a later GET/SUBSCRIBE PR can plug into the same default.
    fn stream_progress(&self) -> Option<crate::operations::stream_progress::StreamProgress> {
        None
    }
}

/// Maximum cheap, fast retries of a `NotificationError` (local callback
/// dropped without a reply) on the same peer before falling through to
/// peer advancement.
///
/// `NotificationError` from `send_and_await` has two sources. First,
/// `op_execution_sender.send()` fails — receiver dropped (genuine
/// shutdown, will keep failing). Second, `response_receiver.recv()`
/// returns `None` — the event loop received the request but dropped
/// the callback without a reply, which is a transient startup-window
/// race (a freshly-booted gateway whose downstream handler isn't
/// ready yet for the first streaming PUT). The second case recovers
/// within milliseconds; the first will exhaust the budget and
/// surface promptly. Both are decoupled from "is the chosen peer
/// slow/dead", so they MUST NOT count against the per-driver
/// peer-advancement cap (which exists to bound wall-clock budget
/// against slow transport-stall timeouts — freenet-git#53).
///
/// 3 retries with cumulative jittered delay under 300 ms adds
/// negligible wall-clock vs a single `STREAMING_ATTEMPT_TIMEOUT_CAP`
/// (600 s) and stays well under any WS-client per-attempt patience.
const MAX_INFRA_RETRIES: usize = 3;

/// Why an attempt ended without a terminal reply.
///
/// [`drive_retry_loop`] treats all three the same way (advance to the next
/// peer), but they carry very different diagnostic meaning AND very different
/// effective budgets. They used to be collapsed into a bare `()`, which left
/// the shared `outcome="timeout"` warning reporting
/// [`RetryDriver::attempt_timeout`] unconditionally — a budget the streaming
/// path never enforces.
///
/// That misattribution has real cost: issue #4912 reported a gateway PUT
/// failing with `timeout_secs=117`, sending the investigation after a
/// ~2-minute per-attempt budget, when the attempt had actually been abandoned
/// by the 30 s [`STREAM_OP_INACTIVITY_TIMEOUT`] stall check ~30 s in. The
/// logged number and the elapsed time disagreed by 4x with nothing in the log
/// to explain the gap.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum AttemptTimeout {
    /// The fixed per-attempt deadline ([`RetryDriver::attempt_timeout`])
    /// elapsed — the non-streaming GET / SUBSCRIBE / PUT path.
    Deadline,
    /// Streaming PUT: no fragment arrived for a whole
    /// [`STREAM_OP_INACTIVITY_TIMEOUT`] window (a confirmed stall).
    StreamStall,
    /// Streaming PUT: the hard [`STREAMING_ATTEMPT_TIMEOUT_CAP`] ceiling was
    /// reached while fragments were still dribbling in.
    StreamCeiling,
}

impl AttemptTimeout {
    /// Stable, greppable label for the `timeout_kind` log field.
    pub(crate) fn label(self) -> &'static str {
        match self {
            Self::Deadline => "attempt_deadline",
            Self::StreamStall => "stream_stall",
            Self::StreamCeiling => "stream_ceiling",
        }
    }

    /// The budget that ACTUALLY governed this attempt.
    ///
    /// Only [`Self::Deadline`] is bounded by the driver's `attempt_timeout`;
    /// the two streaming variants are bounded by their own constants. Logging
    /// `attempt_timeout` for those is what made #4912 hard to read.
    pub(crate) fn budget(self, attempt_timeout: std::time::Duration) -> std::time::Duration {
        match self {
            Self::Deadline => attempt_timeout,
            Self::StreamStall => crate::operations::stream_progress::STREAM_OP_INACTIVITY_TIMEOUT,
            Self::StreamCeiling => crate::operations::STREAMING_ATTEMPT_TIMEOUT_CAP,
        }
    }
}

/// Await a single streaming attempt under a stream-inactivity timeout (#4001).
///
/// Returns `Ok(reply)` when the terminal reply arrives, and the specific
/// [`AttemptTimeout`] variant when the attempt is abandoned (no fragment for
/// [`STREAM_OP_INACTIVITY_TIMEOUT`], or the hard
/// [`STREAMING_ATTEMPT_TIMEOUT_CAP`] ceiling is reached). Both abandon paths
/// map to the shared `outcome="timeout"` advance arm in [`drive_retry_loop`],
/// which reports the variant so the log names the condition that really fired.
///
/// The three concurrent arms:
/// 1. **Terminal reply** — `ctx.send_and_await(request)`; identical to the
///    non-streaming path's awaited future, just not wrapped in a fixed timeout.
/// 2. **Inactivity loop** — wakes on each `Notify` ping and, on each inactivity
///    sleep expiry, re-reads `last_progress` via the atomic; only declares a
///    stall when the atomic CONFIRMS no fragment landed in the race window
///    (the tiebreak that closes the `Notify` lost-wakeup gap).
/// 3. **Hard ceiling** — a sibling [`STREAMING_ATTEMPT_TIMEOUT_CAP`] sleep,
///    OUTSIDE the inactivity loop, so a stream that keeps dribbling fragments
///    forever still can't hold the driver hostage indefinitely.
///
/// All sleeps go through the driver's `TimeSource` (via [`StreamProgress`]) so
/// the path is VirtualTime-correct under DST — `tokio::time::sleep`/`timeout`
/// would not advance under VirtualTime.
async fn await_streaming_attempt(
    ctx: &mut OpCtx,
    request: NetMessage,
    progress: &crate::operations::stream_progress::StreamProgress,
) -> Result<Result<NetMessage, OpError>, AttemptTimeout> {
    use crate::operations::STREAMING_ATTEMPT_TIMEOUT_CAP;
    use crate::operations::stream_progress::STREAM_OP_INACTIVITY_TIMEOUT;

    let handle = progress.handle();
    let round_trip = ctx.send_and_await(request);
    tokio::pin!(round_trip);

    // Arm 3: hard ceiling sibling — a single sleep started once, raced against
    // the terminal reply and the inactivity loop.
    let ceiling = progress.sleep(STREAMING_ATTEMPT_TIMEOUT_CAP);
    tokio::pin!(ceiling);

    loop {
        // Arm 2 (inner): one inactivity window. Re-created each iteration so a
        // `Notify` ping resets the clock by restarting the sleep.
        let inactivity = progress.sleep(STREAM_OP_INACTIVITY_TIMEOUT);

        tokio::select! {
            // Arm 1: terminal reply (unchanged semantics).
            reply = &mut round_trip => return Ok(reply),
            // Arm 3: hard ceiling.
            _ = &mut ceiling => return Err(AttemptTimeout::StreamCeiling),
            // Arm 2: a fragment landed — reset the inactivity window.
            _ = handle.notified() => continue,
            // Arm 2: inactivity window elapsed with no Notify ping. Before
            // declaring a stall, re-read the atomic: a fragment that landed in
            // the Notify race window (notify_one permit consumed elsewhere, or
            // store-then-no-wake ordering) makes `since_last` < the window, so
            // reset and continue.
            _ = inactivity => {
                // `since_last()` reads the handle's own clock — the SAME clock
                // record() writes to — so this elapsed value is the true gap
                // since the last fragment (#4001 single-epoch invariant).
                // Only a confirmed stall returns `AttemptTimeout::StreamStall`.
                let elapsed = handle.since_last();
                if elapsed < STREAM_OP_INACTIVITY_TIMEOUT {
                    continue;
                }
                return Err(AttemptTimeout::StreamStall);
            }
        }
    }
}

/// Drive a retry loop against the network.
///
/// The first attempt reuses `client_tx` so telemetry correlates with the
/// client-visible transaction. Subsequent attempts use
/// [`RetryDriver::new_attempt_tx`].
///
/// The loop handles:
/// - `tokio::time::timeout(driver.attempt_timeout(), ...)` wrapping
/// - `release_pending_op_slot` cleanup on every exit path
/// - Structured `outcome=wire_error|timeout` logging
/// - Cheap fast retries of `NotificationError` (callback drop) on
///   the same peer, decoupled from the peer-advancement cap — see
///   [`MAX_INFRA_RETRIES`]
pub(crate) async fn drive_retry_loop<D: RetryDriver>(
    op_manager: &OpManager,
    client_tx: Transaction,
    op_label: &str,
    driver: &mut D,
) -> RetryLoopOutcome<D::Terminal> {
    let mut is_first_attempt = true;
    let mut attempt_count: usize = 0;
    let mut infra_retries: usize = 0;

    loop {
        let attempt_tx = if is_first_attempt {
            client_tx
        } else {
            driver.new_attempt_tx()
        };
        is_first_attempt = false;
        attempt_count += 1;

        let request = driver.build_request(attempt_tx);

        let attempt_timeout = driver.attempt_timeout();
        let mut ctx = op_manager.op_ctx(attempt_tx);

        // `round_trip: Result<Result<NetMessage, OpError>, AttemptTimeout>` —
        // the `Err` arm models "attempt timed out" (fixed deadline OR streaming
        // stall/ceiling) so the downstream match arms below are shared by both
        // paths, while still naming WHICH condition fired for the log.
        let round_trip = match driver.stream_progress() {
            // Non-streaming path (GET / SUBSCRIBE / non-streaming PUT):
            // UNCHANGED fixed-deadline `tokio::time::timeout`.
            None => tokio::time::timeout(attempt_timeout, ctx.send_and_await(request))
                .await
                .map_err(|_| AttemptTimeout::Deadline),
            // Streaming PUT path (#4001): replace the fixed deadline with a
            // stream-inactivity timeout driven by real per-fragment progress,
            // bounded by the STREAMING_ATTEMPT_TIMEOUT_CAP hard ceiling.
            //
            // The retry-loop task and the originator-loopback relay-streaming
            // task share only `attempt_tx`, so we publish the progress handle
            // into the `OpManager`-owned registry BEFORE sending; the loopback
            // relay looks it up by `attempt_tx` and records per fragment.
            // `StreamProgressGuard` removes the entry on Drop — so it is cleaned
            // up on EVERY exit (success, stall, ceiling, error) AND if this
            // future is cancelled or panics mid-`await`. It cannot leak.
            Some(progress) => {
                let _progress_guard = crate::operations::stream_progress::StreamProgressGuard::new(
                    op_manager.stream_progress_registry().clone(),
                    attempt_tx,
                    progress.handle(),
                );
                await_streaming_attempt(&mut ctx, request, &progress).await
            }
        };

        // Release the per-attempt pending_op_results slot regardless
        // of outcome. Without this, slots are only reclaimed by the
        // 60s periodic sweep.
        op_manager.release_pending_op_slot(attempt_tx).await;

        let reply = match round_trip {
            Ok(Ok(reply)) => reply,
            // Fast infra-retry path: a `NotificationError` is a local
            // callback drop (event loop received the request but
            // didn't deliver a reply), NOT a slow peer-stall. Retry
            // the SAME peer with a fresh attempt_tx — decoupled from
            // the peer-advancement cap. See `MAX_INFRA_RETRIES` doc
            // for the budget analysis. Capped to avoid burning CPU in
            // a true shutdown (where the receiver is genuinely
            // dropped and will keep failing).
            Ok(Err(OpError::NotificationError)) if infra_retries < MAX_INFRA_RETRIES => {
                infra_retries += 1;
                tracing::debug!(
                    tx = %client_tx,
                    attempt_tx = %attempt_tx,
                    attempt = attempt_count,
                    infra_retry = infra_retries,
                    "{op_label}: infrastructure error (callback dropped); \
                     retrying same peer with fresh attempt_tx"
                );
                // Brief jittered delay so the receiver side can
                // recover from whatever transient state caused the
                // drop (e.g., a downstream handler still initializing
                // on a freshly-booted gateway). Base `50 ms ×
                // infra_retries` with ±20% jitter via `GlobalRng`
                // (deterministic under simulation, ±20% per the
                // `code-style.md` retry/backoff rule). Worst-case
                // cumulative is 50+100+150 ms × 1.2 = 360 ms —
                // negligible against the per-attempt timeout cap.
                let base_ms = 50 * infra_retries as u64;
                let jitter_factor = crate::config::GlobalRng::random_range::<f64, _>(0.8..1.2);
                let sleep_ms = (base_ms as f64 * jitter_factor) as u64;
                tokio::time::sleep(std::time::Duration::from_millis(sleep_ms)).await;
                continue;
            }
            Ok(Err(err)) => {
                tracing::warn!(
                    tx = %client_tx,
                    attempt_tx = %attempt_tx,
                    attempt = attempt_count,
                    outcome = "wire_error",
                    error = %err,
                    "{op_label}: send_and_await failed; advancing"
                );
                match driver.advance() {
                    AdvanceOutcome::Next => continue,
                    AdvanceOutcome::Exhausted => {
                        let peer_attempts = attempt_count.saturating_sub(infra_retries);
                        return RetryLoopOutcome::Exhausted(format!(
                            "{op_label} failed after {peer_attempts} peer attempt(s) \
                             ({infra_retries} infra-retries on same peer; last error: {err})"
                        ));
                    }
                }
            }
            Err(cause) => {
                tracing::warn!(
                    tx = %client_tx,
                    attempt_tx = %attempt_tx,
                    attempt = attempt_count,
                    outcome = "timeout",
                    timeout_kind = cause.label(),
                    timeout_secs = cause.budget(attempt_timeout).as_secs(),
                    "{op_label}: attempt timed out; advancing"
                );
                match driver.advance() {
                    AdvanceOutcome::Next => continue,
                    AdvanceOutcome::Exhausted => {
                        let peer_attempts = attempt_count.saturating_sub(infra_retries);
                        return RetryLoopOutcome::Exhausted(format!(
                            "{op_label} timed out after {peer_attempts} peer attempt(s) \
                             ({infra_retries} infra-retries on same peer)"
                        ));
                    }
                }
            }
        };

        match driver.classify(reply) {
            AttemptOutcome::Terminal(value) => {
                return RetryLoopOutcome::Done(value);
            }
            AttemptOutcome::Retry => {
                tracing::debug!(
                    tx = %client_tx,
                    attempt_tx = %attempt_tx,
                    attempt = attempt_count,
                    outcome = "retry",
                    "{op_label}: peer indicated retry; advancing"
                );
                match driver.advance() {
                    AdvanceOutcome::Next => continue,
                    AdvanceOutcome::Exhausted => {
                        return RetryLoopOutcome::Exhausted(format!(
                            "{op_label} exhausted all peers after {attempt_count} attempts"
                        ));
                    }
                }
            }
            AttemptOutcome::Unexpected => {
                tracing::warn!(
                    tx = %client_tx,
                    attempt_tx = %attempt_tx,
                    attempt = attempt_count,
                    "{op_label}: unexpected terminal reply"
                );
                return RetryLoopOutcome::Unexpected;
            }
        }
    }
}

#[cfg(test)]
const _: fn() = || {
    fn assert_send<T: Send>() {}
    assert_send::<OpCtx>();
};

#[cfg(test)]
mod tests {
    use super::*;
    use crate::message::NetMessageV1;
    use crate::node::{EventLoopNotificationsReceiver, event_loop_notification_channel};
    use crate::operations::connect::ConnectMsg;
    use tokio::time::{Duration, timeout};

    /// Behavioural pin on `drive_retry_loop`'s `AttemptOutcome::Terminal`
    /// arm: it MUST return `RetryLoopOutcome::Done(value)` synchronously,
    /// WITHOUT calling `driver.advance()`.
    ///
    /// Why this is a source-pin: a fully behavioural test requires
    /// instantiating an `OpManager` (the function takes `&OpManager` to
    /// build the per-attempt `OpCtx` and to send `TransactionCompleted`),
    /// which costs too much for a unit test. The structural pin
    /// scopes the assertion to the Terminal arm itself — the production
    /// code under test is exactly five lines — and a regression that
    /// routes Terminal through `advance()` would unambiguously contain
    /// the substring `.advance()` inside this arm.
    ///
    /// Pairs with `classify_reply_error_maps_to_terminal_error_with_cause`
    /// in `crate::operations::put::op_ctx_task::tests` to cover the full
    /// PUT-error chain (PutMsg::Error → TerminalError → Terminal →
    /// Done(Err) without advance).
    #[test]
    fn drive_retry_loop_terminal_arm_does_not_call_advance() {
        const SOURCE: &str = include_str!("op_ctx.rs");

        let fn_anchor = "pub(crate) async fn drive_retry_loop<D: RetryDriver>(";
        let fn_start = SOURCE.find(fn_anchor).expect(
            "drive_retry_loop not found — signature has been renamed, \
             update this guard",
        );

        // The `match driver.classify(reply) {` block contains the
        // Terminal arm we care about. Locate it.
        let classify_match = SOURCE[fn_start..]
            .find("match driver.classify(reply) {")
            .map(|p| fn_start + p)
            .expect("`match driver.classify(reply)` not found in drive_retry_loop");

        let terminal_arm_anchor = "AttemptOutcome::Terminal(value) =>";
        let arm_start = SOURCE[classify_match..]
            .find(terminal_arm_anchor)
            .map(|p| classify_match + p)
            .expect(
                "Terminal arm `AttemptOutcome::Terminal(value) =>` not \
                 found — the production arm shape has changed; update \
                 this guard with the new shape",
            );

        // The Terminal arm is delimited by the next `AttemptOutcome::`
        // sibling arm. Anchor on that.
        let arm_end = SOURCE[arm_start + terminal_arm_anchor.len()..]
            .find("AttemptOutcome::")
            .map(|p| arm_start + terminal_arm_anchor.len() + p)
            .expect("end-of-Terminal-arm marker (next AttemptOutcome::) not found");

        let arm_body = &SOURCE[arm_start..arm_end];

        assert!(
            arm_body.contains("return RetryLoopOutcome::Done(value);"),
            "Terminal arm MUST `return RetryLoopOutcome::Done(value);` \
             synchronously — re-routing terminal replies into the retry \
             loop would burn the budget on a deterministic failure and \
             re-introduce the M1/M2 race the PR fixes.\n\
             Arm body:\n{arm_body}"
        );
        assert!(
            !arm_body.contains(".advance()"),
            "Terminal arm MUST NOT call driver.advance() — terminal \
             classifications are by definition non-retriable. A \
             regression that calls advance() here would burn the \
             retry budget against the same deterministic failure and \
             surface the synthesised 'failed notifying, channel \
             closed' marker instead of the real cause (issue #4111).\n\
             Arm body:\n{arm_body}"
        );
        assert!(
            !arm_body.contains("continue"),
            "Terminal arm MUST NOT `continue` — that would skip the \
             return and fall back to the next loop iteration, which \
             would re-`send_and_await` against the SAME closed \
             attempt-tx channel and surface NotificationError.\n\
             Arm body:\n{arm_body}"
        );
    }

    /// Build a synthetic terminal reply keyed by `tx`. Mirrors
    /// `node::tests::callback_forward_tests::dummy_reply` but lets the
    /// caller supply the transaction so both sides of the round-trip agree.
    /// The helper only looks at `NetMessage::id()`, so the tx-only
    /// `Aborted` variant is sufficient payload.
    fn dummy_reply_with_tx(tx: Transaction) -> NetMessage {
        NetMessage::V1(NetMessageV1::Aborted(tx))
    }

    /// Unwrap a `WaiterReply::Reply` in collect-replies tests; panic on the
    /// terminal `PeerDisconnected` variant (not expected on the happy path).
    fn expect_reply(item: WaiterReply) -> NetMessage {
        match item {
            WaiterReply::Reply(msg) => msg,
            other => panic!("expected WaiterReply::Reply, got: {other:?}"),
        }
    }

    /// Happy path: `send_and_await` fires an outbound message, the fake
    /// executor reads it, replies with a terminal message keyed by the
    /// same tx, and `send_and_await` returns `Ok(reply)`.
    ///
    /// "Happy path" here means "the round-trip mechanics work" — NOT
    /// "the reply represents success". `send_and_await`'s `Ok(reply)`
    /// contract is that the caller receives whatever terminal message
    /// the op pipeline produced, including non-success terminals like
    /// `SubscribeMsg::Response::NotFound`. The `NetMessageV1::Aborted`
    /// variant used here is deliberately orthogonal to "success" — it
    /// only carries a `Transaction`, so the assertion is purely on the
    /// tx-routing mechanics. Callers of `send_and_await` must inspect
    /// the returned `NetMessage` to decide what actually happened.
    #[tokio::test]
    async fn send_and_await_returns_reply_on_completion() {
        let (receiver, sender) = event_loop_notification_channel();
        let EventLoopNotificationsReceiver {
            mut op_execution_receiver,
            ..
        } = receiver;

        let tx = Transaction::new::<ConnectMsg>();
        let mut ctx = OpCtx::new(tx, sender.op_execution_sender.clone());

        // Executor task: receive the outbound message and fire a synthetic
        // terminal reply keyed by the same tx.
        let executor = tokio::spawn(async move {
            let (reply_sender, outbound, target_addr) = op_execution_receiver
                .recv()
                .await
                .expect("outbound msg should be delivered");
            assert_eq!(outbound.id(), &tx, "outbound msg tx must match the ctx tx");
            assert_eq!(
                target_addr, None,
                "send_and_await should not specify a target"
            );
            reply_sender
                .try_send(WaiterReply::Reply(dummy_reply_with_tx(tx)))
                .expect("capacity-1 reply channel should accept the first send");
        });

        let outbound = dummy_reply_with_tx(tx);
        let reply = timeout(Duration::from_secs(1), ctx.send_and_await(outbound))
            .await
            .expect("send_and_await should complete quickly")
            .expect("send_and_await should return Ok");

        assert_eq!(reply.id(), &tx, "reply tx must match ctx tx");

        executor
            .await
            .expect("executor task should complete without panicking");
    }

    /// Regression for #3838. `send_to_and_await` MUST forward the
    /// caller-provided target address through the op execution channel so
    /// `handle_op_execution` can dispatch to that peer via
    /// `OutboundMessageWithTarget` instead of looping the message back as a
    /// local `InboundMessage`. The local-loop variant short-circuits in
    /// `process_message` when the contract is cached locally, which is how
    /// `test_ping_blocked_peers` was failing in the merge queue: the
    /// gateway never saw the Subscribe Request, so it never registered the
    /// node as a downstream subscriber, so UPDATE broadcasts never reached
    /// it.
    ///
    /// This test asserts the channel-level invariant. The full integration
    /// path (Request reaching the gateway, gateway responding, reply
    /// classification) is covered by `test_ping_blocked_peers`, but that
    /// test takes ~30 s to run; this one runs in milliseconds and pins the
    /// contract so a regression that drops the target on the floor would
    /// fail here first.
    #[tokio::test]
    async fn send_to_and_await_forwards_target_address() {
        use std::net::{IpAddr, Ipv4Addr, SocketAddr};

        let (receiver, sender) = event_loop_notification_channel();
        let EventLoopNotificationsReceiver {
            mut op_execution_receiver,
            ..
        } = receiver;

        let tx = Transaction::new::<ConnectMsg>();
        let mut ctx = OpCtx::new(tx, sender.op_execution_sender.clone());
        let expected_target = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 42)), 31337);

        let executor = tokio::spawn(async move {
            let (reply_sender, outbound, target_addr) = op_execution_receiver
                .recv()
                .await
                .expect("outbound msg should be delivered");
            assert_eq!(outbound.id(), &tx, "outbound msg tx must match the ctx tx");
            assert_eq!(
                target_addr,
                Some(expected_target),
                "send_to_and_await must propagate the caller's target address \
                 through the op execution channel (regression for #3838)"
            );
            reply_sender
                .try_send(WaiterReply::Reply(dummy_reply_with_tx(tx)))
                .expect("capacity-1 reply channel should accept the first send");
        });

        let outbound = dummy_reply_with_tx(tx);
        let reply = timeout(
            Duration::from_secs(1),
            ctx.send_to_and_await(expected_target, outbound),
        )
        .await
        .expect("send_to_and_await should complete quickly")
        .expect("send_to_and_await should return Ok");

        assert_eq!(reply.id(), &tx, "reply tx must match ctx tx");

        executor
            .await
            .expect("executor task should complete without panicking");
    }

    /// Regression for issue #4350. The renewal driver wraps each
    /// `send_to_and_await` attempt in a per-attempt timeout, and
    /// `recover_orphaned_subscriptions` wraps the whole renewal future in an
    /// **outer** cancel deadline. Before the fix the per-attempt wait was the
    /// global `OPERATION_TTL` (60 s) while the outer deadline was only 25 s, so
    /// a peer that replied between 25 s and 60 s tripped the OUTER cancel first
    /// — dropping the future (and its capacity-1 reply receiver) mid-`await`,
    /// so the in-flight reply landed on a closed receiver.
    ///
    /// This pins the corrected nesting at the timeout layer where the bug
    /// lives, in virtual time (`start_paused = true`): with a per-attempt
    /// timeout strictly *below* the outer deadline, a slow peer makes the INNER
    /// per-attempt timeout fire first, returning a clean `Elapsed` to the
    /// renewal task while the outer deadline still has budget left — so the
    /// task records its own failure instead of being cancelled mid-flight.
    ///
    /// It also asserts the inverse to prove the test actually discriminates:
    /// the pre-fix shape (per-attempt wait > outer deadline) is killed by the
    /// outer cancel, which is exactly the discarded-reply path #4350 removes.
    #[tokio::test(start_paused = true)]
    async fn renewal_per_attempt_timeout_fires_before_outer_cancel() {
        use crate::ring::Ring;
        use std::net::{IpAddr, Ipv4Addr, SocketAddr};

        // Current production constants/relationship from #4350.
        let per_attempt = Ring::RENEWAL_PER_ATTEMPT_TIMEOUT; // 20 s
        let outer = Ring::renewal_outer_cancel(); // 55 s (budget + cleanup + margin)
        assert!(
            per_attempt < outer,
            "test precondition: per-attempt ({per_attempt:?}) must be below the \
             outer cancel deadline ({outer:?}) — the #4350 invariant"
        );

        let target = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 7)), 31337);

        // --- Fixed shape: outer(per_attempt(send_to_and_await)) ---
        // A slow peer that never replies within the 20 s per-attempt window.
        // With the fix the inner per-attempt timeout fires first, well before
        // the 55 s outer cancel; the task gets a clean Elapsed to classify.
        {
            let (receiver, sender) = event_loop_notification_channel();
            let EventLoopNotificationsReceiver {
                mut op_execution_receiver,
                ..
            } = receiver;
            let tx = Transaction::new::<ConnectMsg>();
            let mut ctx = OpCtx::new(tx, sender.op_execution_sender.clone());

            // Slow peer holds the reply sender well past the per-attempt
            // timeout (but `peer.abort()` removes the timer so it can't advance
            // virtual time past the deadline).
            let peer = tokio::spawn(async move {
                let (reply_sender, _outbound, _target) = op_execution_receiver
                    .recv()
                    .await
                    .expect("outbound msg should be delivered");
                tokio::time::sleep(outer * 100).await;
                drop(reply_sender);
            });

            let outbound = dummy_reply_with_tx(tx);
            let outer_result = tokio::time::timeout(
                outer,
                tokio::time::timeout(per_attempt, ctx.send_to_and_await(target, outbound)),
            )
            .await;

            // The OUTER deadline must NOT fire: the inner per-attempt timeout
            // resolves first, so `tokio::time::timeout(outer, ..)` returns Ok.
            let inner_result = outer_result.expect(
                "outer cancel deadline must NOT fire — the per-attempt timeout \
                 resolves first (issue #4350); an outer Err here means the \
                 future was cancelled mid-await, discarding the in-flight reply",
            );
            // The inner per-attempt timeout DID fire (slow peer exceeded it),
            // returning a clean Elapsed the renewal task can classify.
            assert!(
                inner_result.is_err(),
                "the per-attempt timeout should elapse for a slow peer, yielding \
                 a clean in-task failure rather than an outer-cancel drop"
            );
            peer.abort();
        }

        // --- Pre-fix shape (historical bug, literal values) ---
        // Proves the test discriminates: the original bug was a 25 s outer
        // deadline wrapping a 60 s `OPERATION_TTL` per-attempt wait. A peer
        // replying between 25 s and 60 s tripped the outer cancel first,
        // dropping the future (and its receiver) mid-await. Modelled with
        // literal values so it stays a faithful record of the pre-fix shape
        // even as the production constants evolve.
        {
            let old_outer = Duration::from_secs(25);
            let old_per_attempt = OPERATION_TTL; // 60 s
            let reply_delay = Duration::from_secs(40); // in (25 s, 60 s)
            assert!(old_outer < reply_delay && reply_delay < old_per_attempt);

            let (receiver, sender) = event_loop_notification_channel();
            let EventLoopNotificationsReceiver {
                mut op_execution_receiver,
                ..
            } = receiver;
            let tx = Transaction::new::<ConnectMsg>();
            let mut ctx = OpCtx::new(tx, sender.op_execution_sender.clone());

            let peer = tokio::spawn(async move {
                let (reply_sender, _outbound, _target) = op_execution_receiver
                    .recv()
                    .await
                    .expect("outbound msg should be delivered");
                tokio::time::sleep(reply_delay).await;
                // Expected to fail: the receiver is dropped by the outer cancel
                // in this buggy pre-fix shape. Swallow it deliberately.
                drop(reply_sender.try_send(WaiterReply::Reply(dummy_reply_with_tx(tx))));
            });

            let outbound = dummy_reply_with_tx(tx);
            let outer_result = tokio::time::timeout(
                old_outer,
                tokio::time::timeout(old_per_attempt, ctx.send_to_and_await(target, outbound)),
            )
            .await;

            assert!(
                outer_result.is_err(),
                "pre-fix shape (per-attempt wait > outer deadline) MUST be \
                 killed by the outer cancel — this is the discarded-reply path \
                 #4350 removes; if this passes the test no longer discriminates"
            );
            peer.await.expect("peer task should not panic");
        }
    }

    /// Regression for issue #4350, **multi-attempt** case (Codex review P2).
    /// A single shorter per-attempt timeout is not enough on its own: when a
    /// renewal advances to a second peer (e.g. after a fast NotFound from the
    /// first), a fresh full-length per-attempt timeout on that second attempt
    /// could still outlive the outer cancel, so its reply would be dropped
    /// mid-await — the #4350 bug, just on the retry.
    ///
    /// The driver closes this by clamping EACH attempt to the budget remaining
    /// until its task deadline (`Ring::RENEWAL_TASK_BUDGET`, kept below the
    /// outer cancel). This pins that behaviour at the timeout layer in virtual
    /// time: attempt 1 replies fast (consuming little budget) and attempt 2 is
    /// a slow peer clamped to the *remaining* budget. Both resolve inside the
    /// task deadline, so the outer cancel never fires even across the retry.
    #[tokio::test(start_paused = true)]
    async fn renewal_clamps_retry_attempt_to_remaining_budget() {
        use crate::ring::Ring;
        use std::net::{IpAddr, Ipv4Addr, SocketAddr};

        let outer = Ring::renewal_outer_cancel(); // 55 s
        let budget = Ring::RENEWAL_TASK_BUDGET; // 20 s, < outer
        let per_attempt_cap = Ring::RENEWAL_PER_ATTEMPT_TIMEOUT;
        let min_attempt = Ring::RENEWAL_MIN_ATTEMPT_BUDGET;
        assert!(budget < outer, "task budget must be below the outer cancel");

        let target = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 9)), 31337);

        // Model the driver's loop: a single task deadline, each attempt's
        // timeout clamped to the remaining budget. Attempt 1's peer replies
        // fast; attempt 2's peer never replies (slow peer) and must be cut by
        // the clamped per-attempt timeout, NOT the outer cancel.
        let deadline = tokio::time::Instant::now() + budget;

        let sequence = async {
            let mut rounds = 0u32;
            let mut last_attempt_timed_out = false;
            loop {
                let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
                if remaining < min_attempt {
                    break (rounds, last_attempt_timed_out);
                }
                let attempt_timeout = remaining.min(per_attempt_cap);

                let (receiver, sender) = event_loop_notification_channel();
                let EventLoopNotificationsReceiver {
                    mut op_execution_receiver,
                    ..
                } = receiver;
                let tx = Transaction::new::<ConnectMsg>();
                let mut ctx = OpCtx::new(tx, sender.op_execution_sender.clone());

                // Round 0: peer replies fast (1 s). Later rounds: slow peer
                // that never replies within the clamped window.
                let fast = rounds == 0;
                let peer = tokio::spawn(async move {
                    let (reply_sender, _outbound, _target) = op_execution_receiver
                        .recv()
                        .await
                        .expect("outbound msg should be delivered");
                    if fast {
                        tokio::time::sleep(Duration::from_secs(1)).await;
                        drop(reply_sender.try_send(WaiterReply::Reply(dummy_reply_with_tx(tx))));
                    } else {
                        // Slow peer: hold the reply sender (so the driver's
                        // recv() doesn't see a hang-up and actually waits) far
                        // beyond any deadline. `peer.abort()` below removes this
                        // timer so it can't advance virtual time. The clamped
                        // per-attempt timeout must cut the driver's await first.
                        tokio::time::sleep(outer * 100).await;
                        drop(reply_sender);
                    }
                });

                let outbound = dummy_reply_with_tx(tx);
                let attempt =
                    tokio::time::timeout(attempt_timeout, ctx.send_to_and_await(target, outbound))
                        .await;
                last_attempt_timed_out = attempt.is_err();
                rounds += 1;
                // Drop (don't await) the slow peer so its indefinite hold can't
                // advance virtual time past the deadline. The fast peer has
                // already replied by here.
                peer.abort();

                // After a fast reply, keep retrying (models advance_to_next_peer);
                // after a slow-peer timeout we've demonstrated the clamp held.
                if last_attempt_timed_out || rounds >= 10 {
                    break (rounds, last_attempt_timed_out);
                }
            }
        };

        let outer_result = tokio::time::timeout(outer, sequence).await;
        assert!(
            outer_result.is_ok(),
            "the outer cancel deadline must NOT fire — every clamped attempt \
             completes within the task budget, so retries can't be killed \
             mid-await (issue #4350, Codex P2)"
        );
        let (rounds, last_timed_out) = outer_result.unwrap();
        assert!(
            rounds >= 2,
            "expected at least two attempts (fast reply then clamped slow retry), got {rounds}"
        );
        assert!(
            last_timed_out,
            "the slow second attempt must be cut by its own clamped timeout \
             inside the task budget, not left awaiting the outer cancel"
        );
    }

    #[tokio::test]
    async fn send_and_await_errors_on_dropped_receiver() {
        let (receiver, sender) = event_loop_notification_channel();
        let EventLoopNotificationsReceiver {
            mut op_execution_receiver,
            ..
        } = receiver;

        let tx = Transaction::new::<ConnectMsg>();
        let mut ctx = OpCtx::new(tx, sender.op_execution_sender.clone());

        // Executor task: receive the outbound message and drop `reply_sender`
        // without firing anything. The caller must observe the hang-up as
        // `NotificationError`.
        let executor = tokio::spawn(async move {
            let (reply_sender, _outbound, _target_addr) = op_execution_receiver
                .recv()
                .await
                .expect("outbound msg should be delivered");
            drop(reply_sender);
        });

        let outbound = dummy_reply_with_tx(tx);
        let result = timeout(Duration::from_secs(1), ctx.send_and_await(outbound))
            .await
            .expect("send_and_await should not hang when reply_sender is dropped");

        assert!(
            matches!(result, Err(OpError::NotificationError)),
            "expected NotificationError on dropped reply_sender, got {result:?}"
        );

        executor
            .await
            .expect("executor task should complete without panicking");
    }

    #[tokio::test]
    async fn send_and_await_errors_on_closed_sender() {
        let (receiver, sender) = event_loop_notification_channel();
        // Drop the receiver immediately so the executor channel is closed
        // before we can send.
        drop(receiver);

        let tx = Transaction::new::<ConnectMsg>();
        let mut ctx = OpCtx::new(tx, sender.op_execution_sender.clone());

        let outbound = dummy_reply_with_tx(tx);
        let result = ctx.send_and_await(outbound).await;

        assert!(
            matches!(result, Err(OpError::NotificationError)),
            "expected NotificationError on closed executor channel, got {result:?}"
        );
    }

    /// `send_fire_and_forget` delivers the message through the op execution
    /// channel with the caller-supplied target address, then drops the
    /// response receiver. The executor sees `Some(target_addr)` — the same
    /// `OutboundMessageWithTarget` path that `send_to_and_await` uses.
    #[tokio::test]
    async fn send_fire_and_forget_delivers_message_with_target() {
        use std::net::{IpAddr, Ipv4Addr, SocketAddr};

        let (receiver, sender) = event_loop_notification_channel();
        let EventLoopNotificationsReceiver {
            mut op_execution_receiver,
            ..
        } = receiver;

        let tx = Transaction::new::<ConnectMsg>();
        let mut ctx = OpCtx::new(tx, sender.op_execution_sender.clone());
        let expected_target = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(10, 0, 0, 1)), 9000);

        let executor = tokio::spawn(async move {
            let (reply_sender, outbound, target_addr) = op_execution_receiver
                .recv()
                .await
                .expect("outbound msg should be delivered");
            assert_eq!(outbound.id(), &tx, "outbound msg tx must match the ctx tx");
            assert_eq!(
                target_addr,
                Some(expected_target),
                "send_fire_and_forget must propagate the target address"
            );
            // The response receiver was dropped by send_fire_and_forget,
            // so the sender's is_closed() should be true.
            assert!(
                reply_sender.is_closed(),
                "response receiver should be dropped (callback reclaimable)"
            );
        });

        let outbound = dummy_reply_with_tx(tx);
        timeout(
            Duration::from_secs(1),
            ctx.send_fire_and_forget(expected_target, outbound),
        )
        .await
        .expect("send_fire_and_forget should complete quickly")
        .expect("send_fire_and_forget should return Ok");

        executor
            .await
            .expect("executor task should complete without panicking");
    }

    /// Regression: the originator-loopback PUT path uses
    /// `send_local_loopback` to deliver `PutMsg::Response` back to
    /// the originator's `pending_op_results` waiter when the relay
    /// driver runs on the originator's own node
    /// (`upstream_addr == own_addr`). Contract: target=None so
    /// `handle_op_execution` dispatches as `InboundMessage` rather
    /// than trying to ship over a non-existent self-connection.
    #[tokio::test]
    async fn send_local_loopback_passes_none_target() {
        let (receiver, sender) = event_loop_notification_channel();
        let EventLoopNotificationsReceiver {
            mut op_execution_receiver,
            ..
        } = receiver;

        let tx = Transaction::new::<ConnectMsg>();
        let mut ctx = OpCtx::new(tx, sender.op_execution_sender.clone());

        let executor = tokio::spawn(async move {
            let (reply_sender, outbound, target_addr) = op_execution_receiver
                .recv()
                .await
                .expect("outbound msg should be delivered");
            assert_eq!(outbound.id(), &tx, "outbound msg tx must match the ctx tx");
            assert_eq!(
                target_addr, None,
                "send_local_loopback MUST pass target_addr=None so \
                 handle_op_execution dispatches as InboundMessage"
            );
            // The response receiver was dropped by send_local_loopback,
            // so the sender's is_closed() should be true. This is what
            // tells `handle_op_execution` to skip the
            // `pending_op_results` insert (its `if callback.is_closed()`
            // branch).
            assert!(
                reply_sender.is_closed(),
                "response receiver should be dropped so handle_op_execution \
                 skips the pending_op_results insert (would otherwise \
                 overwrite the originator's callback)"
            );
        });

        let outbound = dummy_reply_with_tx(tx);
        timeout(Duration::from_secs(1), ctx.send_local_loopback(outbound))
            .await
            .expect("send_local_loopback should complete quickly")
            .expect("send_local_loopback should return Ok");

        executor
            .await
            .expect("executor task should complete without panicking");
    }

    /// Issue #4111: the originator-loopback PUT failure path emits a
    /// `PutMsg::Error { id, cause }` via `send_local_loopback`. This
    /// pins the contract from the *sender* side: an Error envelope
    /// flows through the same primitive as a successful Response, with
    /// `target=None` and the same `is_closed()` response-receiver
    /// signal so `handle_op_execution` skips the `pending_op_results`
    /// insert and the originator's pre-installed callback survives.
    ///
    /// (The reception side — that the bypass actually forwards Error
    /// to the originator's waiter — is pinned by
    /// `put_branch_bypass_forwards_error` in `node.rs`.)
    #[tokio::test]
    async fn send_local_loopback_carries_put_error_to_event_loop() {
        use crate::message::NetMessageV1;
        use crate::operations::put::PutMsg;

        let (receiver, sender) = event_loop_notification_channel();
        let EventLoopNotificationsReceiver {
            mut op_execution_receiver,
            ..
        } = receiver;

        // Allocate the tx as a PutMsg tx so `Transaction::is_for::<PutMsg>()`
        // is consistent across the round-trip.
        let tx = Transaction::new::<PutMsg>();
        let mut ctx = OpCtx::new(tx, sender.op_execution_sender.clone());
        let cause = "contract rejected: invalid update".to_string();

        let executor_cause = cause.clone();
        let executor = tokio::spawn(async move {
            let (reply_sender, outbound, target_addr) = op_execution_receiver
                .recv()
                .await
                .expect("PutMsg::Error envelope should be delivered");

            assert_eq!(
                outbound.id(),
                &tx,
                "Error envelope tx must match the ctx tx — debug_assert in \
                 send_local_loopback should already enforce this"
            );
            assert_eq!(
                target_addr, None,
                "send_local_loopback MUST pass target_addr=None for the \
                 PutMsg::Error envelope so handle_op_execution dispatches \
                 it as InboundMessage (same path as a Response loopback)"
            );

            // Verify the wire payload is the Error variant with the
            // intended cause — not a placeholder or truncated string.
            // The catch-all arm covers the long tail of NetMessage
            // variants (Get/Subscribe/Update/Aborted/Connect/...) which
            // are irrelevant here; listing each one in the assertion
            // would obscure the actual contract.
            #[allow(clippy::wildcard_enum_match_arm)]
            match outbound {
                NetMessage::V1(NetMessageV1::Put(PutMsg::Error {
                    id,
                    cause: decoded_cause,
                })) => {
                    assert_eq!(id, tx, "Error.id must equal the originator tx");
                    assert_eq!(
                        decoded_cause, executor_cause,
                        "Error.cause must be the verbatim cause string"
                    );
                }
                other => panic!(
                    "expected NetMessage::V1(Put(Error {{ .. }})), got {:?}",
                    std::any::type_name_of_val(&other)
                ),
            }

            assert!(
                reply_sender.is_closed(),
                "send_local_loopback drops its response receiver, so \
                 handle_op_execution must observe is_closed() on the \
                 reply sender and skip the pending_op_results insert — \
                 otherwise it would clobber the originator's pre-installed \
                 callback (the one waiting for this Error)"
            );
        });

        let outbound = NetMessage::V1(NetMessageV1::Put(PutMsg::Error {
            id: tx,
            cause: cause.clone(),
        }));
        timeout(Duration::from_secs(1), ctx.send_local_loopback(outbound))
            .await
            .expect("send_local_loopback should complete quickly")
            .expect("send_local_loopback should return Ok");

        executor
            .await
            .expect("executor task should complete without panicking");
    }

    /// `send_local_loopback` errors with `NotificationError` when the
    /// executor channel is already closed (same contract as
    /// `send_fire_and_forget` / `send_and_await`).
    #[tokio::test]
    async fn send_local_loopback_errors_on_closed_sender() {
        let (receiver, sender) = event_loop_notification_channel();
        drop(receiver);

        let tx = Transaction::new::<ConnectMsg>();
        let mut ctx = OpCtx::new(tx, sender.op_execution_sender.clone());

        let outbound = dummy_reply_with_tx(tx);
        let result = ctx.send_local_loopback(outbound).await;

        assert!(
            matches!(result, Err(OpError::NotificationError)),
            "expected NotificationError on closed executor channel, got {result:?}"
        );
    }

    /// `send_fire_and_forget` errors with `NotificationError` when the
    /// executor channel is already closed (same contract as `send_and_await`).
    #[tokio::test]
    async fn send_fire_and_forget_errors_on_closed_sender() {
        let (receiver, sender) = event_loop_notification_channel();
        drop(receiver);

        let tx = Transaction::new::<ConnectMsg>();
        let mut ctx = OpCtx::new(tx, sender.op_execution_sender.clone());

        let outbound = dummy_reply_with_tx(tx);
        let result = ctx
            .send_fire_and_forget(
                std::net::SocketAddr::new(
                    std::net::IpAddr::V4(std::net::Ipv4Addr::LOCALHOST),
                    1234,
                ),
                outbound,
            )
            .await;

        assert!(
            matches!(result, Err(OpError::NotificationError)),
            "expected NotificationError on closed executor channel, got {result:?}"
        );
    }

    /// Documents the "single-use per `Transaction`" constraint on
    /// Pin the doc claim that a second `send_and_await` on the same
    /// tx hangs forever. The reply helper fires exactly once per tx
    /// (the `completed`/`under_progress` dedup sets short-circuit
    /// subsequent dispatches in `OpManager`); the second
    /// `send_and_await` call has its callback registered but
    /// `forward_pending_op_result_if_completed` never runs.
    ///
    /// The test simulates the dedup effect by holding the second
    /// `reply_sender` alive without firing it. It does NOT guard the
    /// real dedup logic — that lives in `OpManager` and is covered
    /// by integration tests.
    #[tokio::test]
    async fn send_and_await_second_call_hangs_as_documented() {
        use tokio::sync::oneshot;

        let (receiver, sender) = event_loop_notification_channel();
        let EventLoopNotificationsReceiver {
            mut op_execution_receiver,
            ..
        } = receiver;

        let tx = Transaction::new::<ConnectMsg>();
        let mut ctx = OpCtx::new(tx, sender.op_execution_sender.clone());

        // Shutdown signal the test sends once the second call has
        // been observed hanging for the expected window. The executor
        // releases its held `reply_sender_2` only after this fires,
        // avoiding a permanent leak if something goes wrong.
        let (shutdown_tx, shutdown_rx) = oneshot::channel::<()>();

        let executor = tokio::spawn(async move {
            // First outbound: fire the terminal reply and let the first
            // `send_and_await` resolve normally.
            let (reply_sender_1, _first, _target_addr_1) = op_execution_receiver
                .recv()
                .await
                .expect("first outbound delivered");
            reply_sender_1
                .try_send(WaiterReply::Reply(dummy_reply_with_tx(tx)))
                .expect("first reply accepted");

            // Second outbound: hold `reply_sender_2` alive — do NOT
            // drop it, do NOT fire it. Models what the real dedup
            // does at the `OpManager` level: the reply callback is
            // installed, `forward_pending_op_result_if_completed`
            // never runs again (the op is already in `completed`),
            // so the reply channel never produces a message.
            let (reply_sender_2, _second, _target_addr_2) = op_execution_receiver
                .recv()
                .await
                .expect("second outbound delivered");

            // Wait for the test to signal it has observed the hang, then
            // drop the held sender cleanly so no resources leak. The
            // `Result` is discarded via `drop` to satisfy the crate-level
            // `clippy::let_underscore_must_use = "deny"` lint.
            drop(shutdown_rx.await);
            drop(reply_sender_2);
        });

        // First call: resolves normally.
        let first = timeout(
            Duration::from_secs(1),
            ctx.send_and_await(dummy_reply_with_tx(tx)),
        )
        .await
        .expect("first send_and_await should complete quickly")
        .expect("first send_and_await should return Ok");
        assert_eq!(first.id(), &tx);

        // Second call: expected to hang until our timeout elapses because
        // the fake executor holds `reply_sender_2` without firing.
        //
        // 500 ms is ~5× the resolution time of the first call under
        // uncontended local runs and ~2.5× what the other three tests
        // give themselves (1 s default timeout, but those complete
        // effectively instantly). That window is wide enough to survive
        // CI overload — the Simulation job runs four fdev sims in
        // parallel on the same runner — without meaningfully slowing
        // the test suite. Keep it at 500 ms; if this becomes flaky,
        // the root cause is scheduler starvation, not the constant.
        let second = timeout(
            Duration::from_millis(500),
            ctx.send_and_await(dummy_reply_with_tx(tx)),
        )
        .await;
        assert!(
            second.is_err(),
            "second send_and_await should have elapsed per the single-use-per-tx doc; got {second:?}"
        );

        // Release the executor so it can clean up. Send errors are
        // ignored — if the executor already dropped its receiver (e.g.,
        // because an earlier `expect` panicked), the shutdown signal is
        // redundant. Explicit `match` is used instead of `let _ =` or
        // `drop(...)` to satisfy both `clippy::let_underscore_must_use =
        // "deny"` (the `Result` is `#[must_use]`) and
        // `clippy::dropping_copy_types` (`Result<(), ()>` is `Copy`).
        match shutdown_tx.send(()) {
            Ok(()) | Err(()) => {}
        }
        executor
            .await
            .expect("executor task should complete without panicking");
    }

    /// `RetryDriver::attempt_timeout`'s default value is the unscaled
    /// [`OPERATION_TTL`] that non-streaming and pre-#4001 op drivers
    /// (GET / SUBSCRIBE) rely on. Drivers that need a different
    /// Source-grep pin for the fast infra-retry path in
    /// `drive_retry_loop`. A `NotificationError` (local callback
    /// dropped without a reply) is a transient infra hiccup, NOT a
    /// slow peer-stall — it MUST be retried on the SAME peer without
    /// calling `driver.advance()`, so it doesn't burn the per-driver
    /// peer-advancement cap. Reverting to a shape that lumps it in
    /// with `Ok(Err(err))` re-opens the freenet-git mirror's
    /// `test_large_state_put_get` failure (transient startup-window
    /// callback drop → exhausts the streaming cap of 0 advancements →
    /// surfaces as `put failed after 1 attempts`).
    ///
    /// The path also MUST NOT loop forever in a true shutdown
    /// (receiver genuinely dropped) — `MAX_INFRA_RETRIES` caps the
    /// retry count and falls through to the regular `advance()` arm
    /// after exhaustion.
    #[test]
    fn drive_retry_loop_has_fast_infra_retry_path() {
        let src = include_str!("op_ctx.rs");
        let loop_pos = src
            .find("pub(crate) async fn drive_retry_loop")
            .expect("drive_retry_loop must exist");
        let body = &src[loop_pos..];
        assert!(
            body.contains("MAX_INFRA_RETRIES"),
            "drive_retry_loop must reference the MAX_INFRA_RETRIES cap"
        );
        assert!(
            body.contains("Ok(Err(OpError::NotificationError))"),
            "drive_retry_loop must pattern-match `Ok(Err(OpError::NotificationError))` \
             to route the infra-retry path — bare `Ok(Err(err))` would lump it with \
             real wire errors and call advance()"
        );
        assert!(
            body.contains("if infra_retries < MAX_INFRA_RETRIES"),
            "the NotificationError arm must be guarded by \
             `if infra_retries < MAX_INFRA_RETRIES` so a true shutdown \
             doesn't loop forever burning CPU"
        );
        // The infra-retry arm must `continue` (re-attempt same peer)
        // without calling `driver.advance()` in its body. Carve out a
        // window between the NotificationError pattern and the next
        // `match` (the regular wire_error arm) and assert it doesn't
        // contain `driver.advance()`.
        let infra_arm_start = body
            .find("Ok(Err(OpError::NotificationError))")
            .expect("matched above");
        let next_arm_start = body[infra_arm_start..]
            .find("Ok(Err(err)) => {")
            .expect("regular wire_error arm follows infra-retry arm");
        let infra_arm = &body[infra_arm_start..infra_arm_start + next_arm_start];
        assert!(
            !infra_arm.contains("driver.advance()"),
            "infra-retry arm MUST NOT call driver.advance() — the whole \
             point is to decouple cheap callback-drop retries from the \
             slow peer-stall budget. Arm body:\n{infra_arm}"
        );
        assert!(
            infra_arm.contains("continue;"),
            "infra-retry arm must `continue;` to re-attempt the same peer \
             with a fresh attempt_tx"
        );
    }

    /// per-attempt timeout — currently only PUT, for streaming-payload
    /// scaling per #4001 — must override explicitly. Pin the default so
    /// a refactor that changes the trait can't silently shift behaviour
    /// for the drivers that don't override it.
    #[test]
    fn retry_driver_default_attempt_timeout_is_operation_ttl() {
        struct DefaultDriver;
        impl RetryDriver for DefaultDriver {
            type Terminal = ();
            fn new_attempt_tx(&mut self) -> Transaction {
                unreachable!()
            }
            fn build_request(&mut self, _attempt_tx: Transaction) -> NetMessage {
                unreachable!()
            }
            fn classify(&mut self, _reply: NetMessage) -> AttemptOutcome<()> {
                unreachable!()
            }
            fn advance(&mut self) -> AdvanceOutcome {
                unreachable!()
            }
        }
        let d = DefaultDriver;
        assert_eq!(d.attempt_timeout(), OPERATION_TTL);
    }

    /// `send_and_collect_replies` returns a receiver that buffers
    /// multiple inbound replies for the same tx. Used by CONNECT
    /// joiners that fan-in N `ConnectResponse`s from distinct relay
    /// branches against a single outbound `ConnectMsg::Request`.
    ///
    /// Channel-level invariant only: the executor fires three
    /// replies before the caller drains, all three must land in
    /// order.
    #[tokio::test]
    async fn send_and_collect_replies_buffers_multiple_inbound_replies() {
        let (receiver, sender) = event_loop_notification_channel();
        let EventLoopNotificationsReceiver {
            mut op_execution_receiver,
            ..
        } = receiver;

        let tx = Transaction::new::<ConnectMsg>();
        let mut ctx = OpCtx::new(tx, sender.op_execution_sender.clone());

        let executor = tokio::spawn(async move {
            let (reply_sender, outbound, target_addr) = op_execution_receiver
                .recv()
                .await
                .expect("outbound msg should be delivered");
            assert_eq!(outbound.id(), &tx, "outbound msg tx must match the ctx tx");
            assert_eq!(
                target_addr, None,
                "send_and_collect_replies should not specify a target"
            );
            for _ in 0..3 {
                reply_sender
                    .try_send(WaiterReply::Reply(dummy_reply_with_tx(tx)))
                    .expect("capacity-N reply channel should accept all sends within capacity");
            }
            // Hold reply_sender to keep the channel open while the caller drains.
            reply_sender
        });

        let outbound = dummy_reply_with_tx(tx);
        let mut rx = timeout(
            Duration::from_secs(1),
            ctx.send_and_collect_replies(outbound, 4),
        )
        .await
        .expect("send_and_collect_replies should complete quickly")
        .expect("send_and_collect_replies should return Ok");

        let _reply_sender = executor
            .await
            .expect("executor task should complete without panicking");

        for i in 0..3 {
            let reply = timeout(Duration::from_secs(1), rx.recv())
                .await
                .unwrap_or_else(|_| panic!("recv #{i} should yield buffered reply"));
            let reply = expect_reply(reply.unwrap_or_else(|| panic!("recv #{i} returned None")));
            assert_eq!(reply.id(), &tx, "reply #{i} tx must match ctx tx");
        }
    }

    /// `send_to_and_collect_replies` propagates the caller-supplied target
    /// address, mirroring the `send_to_and_await` regression for #3838.
    /// CONNECT slice 2 will use this to route the joiner's outbound
    /// `Request` to the gateway over the wire (not through a local
    /// `InboundMessage` short-circuit).
    #[tokio::test]
    async fn send_to_and_collect_replies_forwards_target_address() {
        use std::net::{IpAddr, Ipv4Addr, SocketAddr};

        let (receiver, sender) = event_loop_notification_channel();
        let EventLoopNotificationsReceiver {
            mut op_execution_receiver,
            ..
        } = receiver;

        let tx = Transaction::new::<ConnectMsg>();
        let mut ctx = OpCtx::new(tx, sender.op_execution_sender.clone());
        let expected_target = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 7)), 9090);

        let executor = tokio::spawn(async move {
            let (reply_sender, outbound, target_addr) = op_execution_receiver
                .recv()
                .await
                .expect("outbound msg should be delivered");
            assert_eq!(outbound.id(), &tx);
            assert_eq!(
                target_addr,
                Some(expected_target),
                "send_to_and_collect_replies must propagate target_addr"
            );
            reply_sender
                .try_send(WaiterReply::Reply(dummy_reply_with_tx(tx)))
                .expect("capacity-N channel accepts first send");
            reply_sender
        });

        let outbound = dummy_reply_with_tx(tx);
        let mut rx = timeout(
            Duration::from_secs(1),
            ctx.send_to_and_collect_replies(expected_target, outbound, 2),
        )
        .await
        .expect("send_to_and_collect_replies should complete quickly")
        .expect("send_to_and_collect_replies should return Ok");

        let _reply_sender = executor
            .await
            .expect("executor task should complete without panicking");

        let reply = expect_reply(
            timeout(Duration::from_secs(1), rx.recv())
                .await
                .expect("recv should yield buffered reply")
                .expect("recv returned None"),
        );
        assert_eq!(reply.id(), &tx);
    }

    /// Capacity-1 collect_replies is the degenerate case: one buffered
    /// reply, then `try_send` fails on the executor side. This pins the
    /// expectation that the caller is free to choose `capacity = 1` if
    /// fan-in turns out to be unnecessary, without a panic from the
    /// `debug_assert!(capacity >= 1)` invariant.
    #[tokio::test]
    async fn send_and_collect_replies_capacity_one_works_like_single_shot() {
        let (receiver, sender) = event_loop_notification_channel();
        let EventLoopNotificationsReceiver {
            mut op_execution_receiver,
            ..
        } = receiver;

        let tx = Transaction::new::<ConnectMsg>();
        let mut ctx = OpCtx::new(tx, sender.op_execution_sender.clone());

        let executor = tokio::spawn(async move {
            let (reply_sender, _outbound, _target_addr) = op_execution_receiver
                .recv()
                .await
                .expect("outbound msg should be delivered");
            reply_sender
                .try_send(WaiterReply::Reply(dummy_reply_with_tx(tx)))
                .expect("capacity-1 channel accepts first send");
            reply_sender
        });

        let outbound = dummy_reply_with_tx(tx);
        let mut rx = ctx
            .send_and_collect_replies(outbound, 1)
            .await
            .expect("send_and_collect_replies should return Ok");

        let _reply_sender = executor.await.expect("executor task should complete");

        let reply = expect_reply(rx.recv().await.expect("first recv should yield reply"));
        assert_eq!(reply.id(), &tx);
    }

    /// A `WaiterReply::Reply` item resolves to the carried `NetMessage`.
    #[tokio::test]
    async fn recv_waiter_reply_returns_reply_on_reply_item() {
        let (_receiver, sender) = event_loop_notification_channel();
        let tx = Transaction::new::<ConnectMsg>();
        let ctx = OpCtx::new(tx, sender.op_execution_sender.clone());

        let (waiter_sender, mut rx) = mpsc::channel::<WaiterReply>(1);
        waiter_sender
            .try_send(WaiterReply::Reply(dummy_reply_with_tx(tx)))
            .expect("capacity-1 channel accepts first send");

        let reply = ctx
            .recv_waiter_reply(&mut rx)
            .await
            .expect("Reply item must resolve to Ok");
        assert_eq!(reply.id(), &tx);
    }

    /// A `WaiterReply::PeerDisconnected` item (the prune signal) surfaces as
    /// `OpError::PeerDisconnected` carrying the peer — NOT the FORBIDDEN_MARKER
    /// (#4313). Delivering it before the sender drops is what makes this
    /// deterministic; see the concurrency regression below.
    #[tokio::test]
    async fn recv_waiter_reply_returns_peer_disconnected_signal() {
        let (_receiver, sender) = event_loop_notification_channel();
        let tx = Transaction::new::<ConnectMsg>();
        let ctx = OpCtx::new(tx, sender.op_execution_sender.clone());
        let peer: SocketAddr = "1.2.3.4:5678"
            .parse()
            .expect("test peer addr must be valid");

        // Deliver the cause, then drop the sender — mirrors the
        // `TransactionOrphaned` handler's send-before-drop.
        let (waiter_sender, mut rx) = mpsc::channel::<WaiterReply>(1);
        waiter_sender
            .try_send(WaiterReply::PeerDisconnected { peer })
            .expect("capacity-1 channel accepts first send");
        drop(waiter_sender);

        let err = ctx
            .recv_waiter_reply(&mut rx)
            .await
            .expect_err("PeerDisconnected item must surface as Err");
        assert!(
            matches!(err, OpError::PeerDisconnected { peer: p } if p == peer),
            "expected PeerDisconnected({peer}), got: {err:?}"
        );
    }

    /// A bare channel close with no terminal item is a genuine teardown
    /// and falls back to `NotificationError` (pre-#4313 semantics).
    #[tokio::test]
    async fn recv_waiter_reply_falls_back_to_notification_error_on_bare_close() {
        let (_receiver, sender) = event_loop_notification_channel();
        let tx = Transaction::new::<ConnectMsg>();
        let ctx = OpCtx::new(tx, sender.op_execution_sender.clone());

        let (waiter_sender, mut rx) = mpsc::channel::<WaiterReply>(1);
        drop(waiter_sender);

        let err = ctx
            .recv_waiter_reply(&mut rx)
            .await
            .expect_err("closed channel must surface as Err");
        assert!(
            matches!(err, OpError::NotificationError),
            "expected NotificationError fallback, got: {err:?}"
        );
    }

    /// #4313 regression: the prune path delivers `PeerDisconnected` through
    /// the channel BEFORE dropping the sender, so a parked driver reads it
    /// deterministically even under multi-thread scheduling. The deleted
    /// orphan-cause registry approach raced a `drop_entry` against this
    /// `recv` and lost ~99.99% of the time, surfacing the FORBIDDEN_MARKER.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn parked_driver_always_observes_peer_disconnected_under_churn() {
        let (_receiver, sender) = event_loop_notification_channel();
        let peer: SocketAddr = "9.9.9.9:9999".parse().expect("valid addr");

        for _ in 0..2_000 {
            let tx = Transaction::new::<ConnectMsg>();
            let ctx = OpCtx::new(tx, sender.op_execution_sender.clone());
            let (waiter_sender, mut rx) = mpsc::channel::<WaiterReply>(1);

            let driver = tokio::spawn(async move { ctx.recv_waiter_reply(&mut rx).await });

            // Let the driver actually park on recv() before the wake.
            tokio::task::yield_now().await;

            // Production handler order: send the cause, THEN drop the sender.
            #[allow(clippy::let_underscore_must_use)]
            let _ = waiter_sender.try_send(WaiterReply::PeerDisconnected { peer });
            drop(waiter_sender);

            match driver.await.expect("driver task panicked") {
                Err(OpError::PeerDisconnected { peer: observed }) => {
                    assert_eq!(observed, peer);
                }
                other => panic!("expected PeerDisconnected, got: {other:?}"),
            }
        }
    }

    // -------------------------------------------------------------------------
    // #4001 stream-inactivity timeout regression tests.
    //
    // These drive `await_streaming_attempt` — the core of `drive_retry_loop`'s
    // streaming branch — directly, which avoids standing up an `OpManager`.
    // They run under tokio's `start_paused` virtual clock with `RealTime` as
    // the `TimeSource`; `RealTime::sleep`/`now` delegate to tokio's (paused,
    // auto-advancing) clock, so virtual time advances deterministically when
    // all tasks are idle. This is the same pattern as
    // `renewal_per_attempt_timeout_fires_before_outer_cancel`.
    //
    // `await_streaming_attempt` returns `Ok(reply)` on the terminal reply,
    // `AttemptTimeout::StreamStall` on a stream-inactivity stall, and
    // `AttemptTimeout::StreamCeiling` on the hard ceiling. The non-streaming
    // `None` arm (a fixed `OPERATION_TTL`/scaled deadline) instead fires
    // `AttemptTimeout::Deadline` at the fixed deadline regardless of fragment
    // progress — which is exactly the #4001 self-retry-while-in-flight bug
    // these tests guard against.
    // -------------------------------------------------------------------------

    use crate::operations::stream_progress::{
        STREAM_OP_INACTIVITY_TIMEOUT, StreamProgress, StreamProgressHandle,
    };
    // `TimeSource` is needed for `RealTime::now()` in the elapsed-time asserts.
    use crate::simulation::{RealTime, TimeSource};

    /// Build an `(OpCtx, StreamProgress)` pair plus the executor receiver, all
    /// sharing a fresh `RealTime` (tokio paused clock). The returned handle is
    /// the SAME one bundled in the `StreamProgress` (so its record()/since_last()
    /// read the loop's clock — the #4001 single-epoch invariant). The receiver
    /// lets a test spawn a fake peer that replies on its own schedule.
    fn streaming_attempt_fixture() -> (
        OpCtx,
        Transaction,
        StreamProgressHandle,
        StreamProgress,
        mpsc::Receiver<crate::node::OpExecutionPayload>,
    ) {
        let (receiver, sender) = event_loop_notification_channel();
        let EventLoopNotificationsReceiver {
            op_execution_receiver,
            ..
        } = receiver;
        let tx = Transaction::new::<ConnectMsg>();
        let ctx = OpCtx::new(tx, sender.op_execution_sender.clone());
        let progress = StreamProgress::new(RealTime::new());
        let handle = progress.handle();
        (ctx, tx, handle, progress, op_execution_receiver)
    }

    /// A fragment lands every ~10 s of virtual time for 120 s (well past the
    /// 60 s `OPERATION_TTL` cliff that the OLD fixed-deadline path would fire
    /// at), then the peer replies at ~125 s. The streaming-inactivity path MUST
    /// return the terminal reply (`Ok`) — it MUST NOT time out — because the
    /// 30 s inactivity window is continuously reset by progress.
    ///
    /// This FAILS against the pre-#4001 fixed-deadline behaviour: a 60 s
    /// (or even payload-scaled) deadline fires `Err(())` at 60 s while the
    /// stream is still flowing, producing the version-conflict self-retry.
    #[tokio::test(start_paused = true)]
    async fn streaming_put_does_not_retry_while_chunks_flow() {
        let (mut ctx, tx, handle, progress, mut rx) = streaming_attempt_fixture();

        // Fake peer: record progress every 10 s for 120 s, reply at 125 s.
        let producer = tokio::spawn(async move {
            let (reply_sender, _outbound, _target) =
                rx.recv().await.expect("outbound msg should be delivered");
            for _ in 0..12 {
                tokio::time::sleep(Duration::from_secs(10)).await;
                handle.record();
            }
            tokio::time::sleep(Duration::from_secs(5)).await;
            reply_sender
                .try_send(WaiterReply::Reply(dummy_reply_with_tx(tx)))
                .expect("capacity-1 reply channel accepts the reply");
        });

        let outbound = dummy_reply_with_tx(tx);
        let result = await_streaming_attempt(&mut ctx, outbound, &progress).await;
        assert!(
            result.is_ok(),
            "stream that keeps flowing for 125 s must deliver the terminal \
             reply, not time out — a fixed 60 s deadline would have fired the \
             #4001 self-retry here"
        );
        assert_eq!(result.unwrap().unwrap().id(), &tx);
        producer.await.expect("producer task panicked");
    }

    /// Progress flows for 40 s then stops. The 30 s inactivity timeout MUST
    /// fire ~70 s in (40 s of progress + 30 s of silence), returning
    /// `AttemptTimeout::StreamStall` exactly once. Asserts the stall is
    /// detected on a genuinely dead stream.
    #[tokio::test(start_paused = true)]
    async fn streaming_put_retries_on_true_stall() {
        let (mut ctx, tx, handle, progress, mut rx) = streaming_attempt_fixture();

        // Fake peer: progress every 10 s for 40 s, then go silent forever
        // (never replies). The inactivity timeout must fire.
        let producer = tokio::spawn(async move {
            let (_reply_sender, _outbound, _target) =
                rx.recv().await.expect("outbound msg should be delivered");
            for _ in 0..4 {
                tokio::time::sleep(Duration::from_secs(10)).await;
                handle.record();
            }
            // Hold the reply sender open but never reply; sleep beyond the
            // ceiling so the task doesn't drop the channel and synthesise a
            // close before the inactivity timeout fires.
            tokio::time::sleep(Duration::from_secs(3600)).await;
        });

        let start = RealTime::new();
        let t0 = start.now();
        let outbound = dummy_reply_with_tx(tx);
        let result = await_streaming_attempt(&mut ctx, outbound, &progress).await;
        let elapsed = start.now().saturating_sub(t0);
        assert!(result.is_err(), "a stalled stream must time out (Err)");
        // Stall detected after 40 s progress + 30 s silence ≈ 70 s, and well
        // before the 600 s ceiling.
        assert!(
            elapsed >= Duration::from_secs(60) && elapsed < Duration::from_secs(120),
            "inactivity stall should fire ~70 s in (40 s progress + 30 s idle), \
             got {elapsed:?}"
        );
        producer.abort();
    }

    /// A stream that records a fragment forever (never replies, never stalls
    /// for 30 s) MUST still be bounded: the hard 600 s
    /// `STREAMING_ATTEMPT_TIMEOUT_CAP` ceiling fires and returns
    /// `AttemptTimeout::StreamCeiling`.
    #[tokio::test(start_paused = true)]
    async fn streaming_put_hard_ceiling_fires() {
        let (mut ctx, tx, handle, progress, mut rx) = streaming_attempt_fixture();

        // Fake peer: keep recording progress every 10 s indefinitely so the
        // inactivity window NEVER expires — only the ceiling can stop it.
        let producer = tokio::spawn(async move {
            let (_reply_sender, _outbound, _target) =
                rx.recv().await.expect("outbound msg should be delivered");
            loop {
                tokio::time::sleep(Duration::from_secs(10)).await;
                handle.record();
            }
        });

        let start = RealTime::new();
        let t0 = start.now();
        let outbound = dummy_reply_with_tx(tx);
        let result = await_streaming_attempt(&mut ctx, outbound, &progress).await;
        let elapsed = start.now().saturating_sub(t0);
        assert!(result.is_err(), "the hard ceiling must abandon the attempt");
        assert!(
            elapsed >= crate::operations::STREAMING_ATTEMPT_TIMEOUT_CAP,
            "ceiling must fire at/after STREAMING_ATTEMPT_TIMEOUT_CAP (600 s), \
             got {elapsed:?}"
        );
        producer.abort();
    }

    /// The atomic tiebreak: a fragment whose `record()` store lands inside the
    /// inactivity window — but whose `Notify` ping is lost (the realistic race
    /// `Notify` alone cannot cover) — MUST be observed via the `since_last`
    /// atomic re-check and suppress a false stall.
    ///
    /// To exercise the *atomic* path deterministically (not the task scheduler),
    /// each fragment lands at `window - RACE_SLACK` of virtual time: every
    /// window expiry therefore finds `since_last < window` and resets via the
    /// atomic, keeping the attempt alive until the reply. A poll-/Notify-only
    /// implementation that ignored the atomic would falsely stall.
    #[tokio::test(start_paused = true)]
    async fn streaming_put_progress_during_inactivity_race() {
        // Land each fragment just inside the window so the atomic re-check —
        // not a fresh timer — is what keeps the stream alive.
        const RACE_SLACK: Duration = Duration::from_millis(500);
        let (mut ctx, tx, handle, progress, mut rx) = streaming_attempt_fixture();

        let producer = tokio::spawn(async move {
            let (reply_sender, _outbound, _target) =
                rx.recv().await.expect("outbound msg should be delivered");
            // Several fragments landing just before each inactivity boundary.
            for _ in 0..5 {
                tokio::time::sleep(STREAM_OP_INACTIVITY_TIMEOUT - RACE_SLACK).await;
                handle.record();
            }
            tokio::time::sleep(Duration::from_secs(5)).await;
            reply_sender
                .try_send(WaiterReply::Reply(dummy_reply_with_tx(tx)))
                .expect("capacity-1 reply channel accepts the reply");
        });

        let outbound = dummy_reply_with_tx(tx);
        let result = await_streaming_attempt(&mut ctx, outbound, &progress).await;
        assert!(
            result.is_ok(),
            "a fragment landing inside the inactivity window must reset it via \
             the atomic tiebreak, not trigger a false stall"
        );
        assert_eq!(result.unwrap().unwrap().id(), &tx);
        producer.await.expect("producer task panicked");
    }

    /// Control: a driver whose `stream_progress()` returns `None` (every
    /// non-streaming op: GET / SUBSCRIBE / small PUT) is byte-identical to the
    /// pre-#4001 fixed-deadline path — `drive_retry_loop` takes the
    /// `None => tokio::time::timeout(...)` arm. Pin that the default trait hook
    /// is `None` so the streaming branch is never entered for those drivers.
    #[test]
    fn non_streaming_put_uses_fixed_timeout() {
        struct NonStreamingDriver;
        impl RetryDriver for NonStreamingDriver {
            type Terminal = ();
            fn new_attempt_tx(&mut self) -> Transaction {
                unreachable!()
            }
            fn build_request(&mut self, _attempt_tx: Transaction) -> NetMessage {
                unreachable!()
            }
            fn classify(&mut self, _reply: NetMessage) -> AttemptOutcome<()> {
                unreachable!()
            }
            fn advance(&mut self) -> AdvanceOutcome {
                unreachable!()
            }
        }
        assert!(
            NonStreamingDriver.stream_progress().is_none(),
            "non-streaming drivers MUST return None so drive_retry_loop keeps \
             the unchanged fixed-deadline timeout path"
        );

        // Source pin: the `None` arm of the stream_progress branch is the
        // unchanged `tokio::time::timeout` path.
        const SRC: &str = include_str!("op_ctx.rs");
        let loop_pos = SRC
            .find("pub(crate) async fn drive_retry_loop")
            .expect("drive_retry_loop must exist");
        let body = &SRC[loop_pos..];
        let branch_pos = body
            .find("match driver.stream_progress() {")
            .expect("drive_retry_loop must branch on driver.stream_progress()");
        let after = &body[branch_pos..];
        let none_arm = after.find("None =>").expect("None arm must exist");
        let some_arm = after
            .find("Some(progress) =>")
            .expect("Some arm must exist");
        assert!(
            none_arm < some_arm,
            "None arm must precede Some arm in the stream_progress branch"
        );
        let none_body = &after[none_arm..some_arm];
        assert!(
            none_body
                .contains("tokio::time::timeout(attempt_timeout, ctx.send_and_await(request))"),
            "the None arm MUST remain the unchanged fixed-deadline \
             tokio::time::timeout path; non-streaming ops must not regress"
        );
    }

    /// Regression pin for #4912: each timeout condition reports the budget
    /// that ACTUALLY governed it, not the driver's fixed `attempt_timeout`.
    ///
    /// Before the fix all three conditions collapsed to `Err(())` and the
    /// shared warning logged `attempt_timeout` unconditionally. On the nova
    /// gateway that printed `timeout_secs=117` for an attempt a 30 s stream
    /// stall had abandoned — a 4x discrepancy with nothing in the log to
    /// explain it, which sent the investigation after the wrong mechanism.
    #[test]
    fn attempt_timeout_budget_matches_the_condition_that_fired() {
        use crate::operations::STREAMING_ATTEMPT_TIMEOUT_CAP;
        use crate::operations::stream_progress::STREAM_OP_INACTIVITY_TIMEOUT;

        // A deliberately distinctive value so a mix-up is unambiguous, and
        // one that matches neither streaming constant.
        let attempt_timeout = Duration::from_secs(117);

        assert_eq!(
            AttemptTimeout::Deadline.budget(attempt_timeout),
            attempt_timeout,
            "the fixed-deadline path IS bounded by attempt_timeout"
        );
        assert_eq!(
            AttemptTimeout::StreamStall.budget(attempt_timeout),
            STREAM_OP_INACTIVITY_TIMEOUT,
            "a stream stall is bounded by the inactivity window, NOT attempt_timeout"
        );
        assert_eq!(
            AttemptTimeout::StreamCeiling.budget(attempt_timeout),
            STREAMING_ATTEMPT_TIMEOUT_CAP,
            "the hard ceiling is bounded by the cap, NOT attempt_timeout"
        );

        // The specific #4912 misreport: a stall must never be described
        // using the driver's per-attempt budget.
        assert_ne!(
            AttemptTimeout::StreamStall.budget(attempt_timeout),
            attempt_timeout,
            "#4912 regression: a 30s stream stall reported as a 117s \
             attempt deadline is exactly the misattribution this fixes"
        );

        // Labels must be distinct, or `timeout_kind` cannot disambiguate.
        let labels = [
            AttemptTimeout::Deadline.label(),
            AttemptTimeout::StreamStall.label(),
            AttemptTimeout::StreamCeiling.label(),
        ];
        let unique: std::collections::HashSet<&str> = labels.iter().copied().collect();
        assert_eq!(
            unique.len(),
            labels.len(),
            "timeout_kind labels must be distinct"
        );
    }

    /// The two streaming abandon paths must stay distinguishable at their
    /// return sites — collapsing them back to one value would reintroduce
    /// #4912's ambiguity even with the enum in place.
    #[test]
    fn streaming_stall_and_ceiling_return_distinct_causes() {
        const SRC: &str = include_str!("op_ctx.rs");
        let fn_pos = SRC
            .find("async fn await_streaming_attempt")
            .expect("await_streaming_attempt must exist");
        let body = &SRC[fn_pos..];
        let end = body
            .find("\n/// Drive a retry loop against the network.")
            .expect("await_streaming_attempt must be followed by drive_retry_loop's docs");
        let body = &body[..end];

        assert!(
            body.contains("return Err(AttemptTimeout::StreamCeiling)"),
            "the hard-ceiling arm must report StreamCeiling"
        );
        assert!(
            body.contains("return Err(AttemptTimeout::StreamStall)"),
            "the confirmed-stall arm must report StreamStall"
        );
        assert!(
            !body.contains("Err(())"),
            "streaming abandon paths must not regress to an untyped Err(()) — \
             that is what made #4912's log unreadable"
        );
    }

    /// Source pin: the shared `outcome="timeout"` warning must report the
    /// condition-specific budget, never `attempt_timeout` unconditionally.
    #[test]
    fn timeout_warning_reports_condition_specific_budget() {
        const SRC: &str = include_str!("op_ctx.rs");
        let loop_pos = SRC
            .find("pub(crate) async fn drive_retry_loop")
            .expect("drive_retry_loop must exist");
        let body = &SRC[loop_pos..];
        let warn_pos = body
            .find("outcome = \"timeout\"")
            .expect("drive_retry_loop must emit the outcome=timeout warning");
        // Bound the window to the warning's own field list.
        let window = &body[warn_pos..warn_pos + 400];

        assert!(
            window.contains("timeout_kind = cause.label()"),
            "the timeout warning MUST name which condition fired via timeout_kind"
        );
        assert!(
            window.contains("timeout_secs = cause.budget(attempt_timeout)"),
            "the timeout warning MUST report the budget that actually governed \
             the attempt, derived from the cause"
        );
        assert!(
            !window.contains("timeout_secs = attempt_timeout.as_secs()"),
            "#4912 regression: logging attempt_timeout unconditionally \
             misreports streaming stalls/ceilings by up to 20x"
        );
    }
}
