//! Per-transaction execution context for the task-per-tx model.
//!
//! Ships the `OpCtx` struct and its `send_and_await` round-trip
//! primitive. Each transaction is owned and driven by a single
//! task; state lives in task locals rather than the legacy
//! `OpManager.ops.*` DashMap.

use std::net::SocketAddr;

use tokio::sync::mpsc;

use crate::message::{MessageStats, NetMessage, Transaction};
use crate::node::OpExecutionPayload;
use crate::operations::OpError;

/// Per-transaction execution context for the task-per-tx model.
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

        let (response_sender, _response_receiver) = mpsc::channel::<NetMessage>(1);
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
    /// `try_forward_task_per_tx_reply`.
    pub async fn send_local_loopback(&mut self, msg: NetMessage) -> Result<(), OpError> {
        debug_assert_eq!(
            msg.id(),
            &self.tx,
            "OpCtx::send_local_loopback: msg.id must match ctx.tx"
        );

        let (response_sender, _response_receiver) = mpsc::channel::<NetMessage>(1);

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
    /// `try_forward_task_per_tx_reply` would then drop the reply as
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
    ) -> Result<mpsc::Receiver<NetMessage>, OpError> {
        debug_assert_eq!(
            msg.id(),
            &self.tx,
            "OpCtx::send_to_and_register_waiter: msg.id must match ctx.tx"
        );

        let (response_sender, response_receiver) = mpsc::channel::<NetMessage>(1);

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
    /// `node::try_forward_task_per_tx_reply` calls `try_send` on the
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
    ) -> Result<mpsc::Receiver<NetMessage>, OpError> {
        debug_assert_eq!(
            msg.id(),
            &self.tx,
            "OpCtx::send_and_collect_replies: msg.id must match ctx.tx"
        );
        debug_assert!(
            capacity >= 1,
            "OpCtx::send_and_collect_replies: capacity must be >= 1"
        );

        let (response_sender, response_receiver) = mpsc::channel::<NetMessage>(capacity);

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
    ) -> Result<mpsc::Receiver<NetMessage>, OpError> {
        debug_assert_eq!(
            msg.id(),
            &self.tx,
            "OpCtx::send_to_and_collect_replies: msg.id must match ctx.tx"
        );
        debug_assert!(
            capacity >= 1,
            "OpCtx::send_to_and_collect_replies: capacity must be >= 1"
        );

        let (response_sender, response_receiver) = mpsc::channel::<NetMessage>(capacity);

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

        let (response_sender, mut response_receiver) = mpsc::channel::<NetMessage>(1);

        self.op_execution_sender
            .send((response_sender, msg, target_addr))
            .await
            .map_err(|_| OpError::NotificationError)?;

        match response_receiver.recv().await {
            Some(reply) => {
                // Debug-only defense-in-depth. In a release build a
                // mismatched reply tx would silently return to the
                // caller (the reply is for a *different* transaction).
                // That would be a correctness bug in whatever mislabeled
                // the message in `p2p_protoc::pending_op_results`, not a
                // failure mode of `send_and_await` itself — the assert
                // exists to catch such bugs at development time.
                debug_assert_eq!(
                    reply.id(),
                    &self.tx,
                    "OpCtx::send_and_await: reply tx must match ctx.tx"
                );
                Ok(reply)
            }
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
pub(crate) async fn drive_retry_loop<D: RetryDriver>(
    op_manager: &OpManager,
    client_tx: Transaction,
    op_label: &str,
    driver: &mut D,
) -> RetryLoopOutcome<D::Terminal> {
    let mut is_first_attempt = true;
    let mut attempt_count: usize = 0;

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
        let round_trip = tokio::time::timeout(attempt_timeout, ctx.send_and_await(request)).await;

        // Release the per-attempt pending_op_results slot regardless
        // of outcome. Without this, slots are only reclaimed by the
        // 60s periodic sweep.
        op_manager.release_pending_op_slot(attempt_tx).await;

        let reply = match round_trip {
            Ok(Ok(reply)) => reply,
            Ok(Err(err)) => {
                tracing::warn!(
                    tx = %client_tx,
                    attempt_tx = %attempt_tx,
                    attempt = attempt_count,
                    outcome = "wire_error",
                    error = %err,
                    "{op_label} (task-per-tx): send_and_await failed; advancing"
                );
                match driver.advance() {
                    AdvanceOutcome::Next => continue,
                    AdvanceOutcome::Exhausted => {
                        return RetryLoopOutcome::Exhausted(format!(
                            "{op_label} failed after {attempt_count} attempts (last error: {err})"
                        ));
                    }
                }
            }
            Err(_) => {
                tracing::warn!(
                    tx = %client_tx,
                    attempt_tx = %attempt_tx,
                    attempt = attempt_count,
                    outcome = "timeout",
                    timeout_secs = attempt_timeout.as_secs(),
                    "{op_label} (task-per-tx): attempt timed out; advancing"
                );
                match driver.advance() {
                    AdvanceOutcome::Next => continue,
                    AdvanceOutcome::Exhausted => {
                        return RetryLoopOutcome::Exhausted(format!(
                            "{op_label} timed out after {attempt_count} attempts"
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
                    "{op_label} (task-per-tx): peer indicated retry; advancing"
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
                    "{op_label} (task-per-tx): unexpected terminal reply"
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

    /// Build a synthetic terminal reply keyed by `tx`. Mirrors
    /// `node::tests::callback_forward_tests::dummy_reply` but lets the
    /// caller supply the transaction so both sides of the round-trip agree.
    /// The helper only looks at `NetMessage::id()`, so the tx-only
    /// `Aborted` variant is sufficient payload.
    fn dummy_reply_with_tx(tx: Transaction) -> NetMessage {
        NetMessage::V1(NetMessageV1::Aborted(tx))
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
                .try_send(dummy_reply_with_tx(tx))
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
                .try_send(dummy_reply_with_tx(tx))
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
                .try_send(dummy_reply_with_tx(tx))
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
                    .try_send(dummy_reply_with_tx(tx))
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
            let reply = reply.unwrap_or_else(|| panic!("recv #{i} returned None"));
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
                .try_send(dummy_reply_with_tx(tx))
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

        let reply = timeout(Duration::from_secs(1), rx.recv())
            .await
            .expect("recv should yield buffered reply")
            .expect("recv returned None");
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
                .try_send(dummy_reply_with_tx(tx))
                .expect("capacity-1 channel accepts first send");
            reply_sender
        });

        let outbound = dummy_reply_with_tx(tx);
        let mut rx = ctx
            .send_and_collect_replies(outbound, 1)
            .await
            .expect("send_and_collect_replies should return Ok");

        let _reply_sender = executor.await.expect("executor task should complete");

        let reply = rx.recv().await.expect("first recv should yield reply");
        assert_eq!(reply.id(), &tx);
    }
}
