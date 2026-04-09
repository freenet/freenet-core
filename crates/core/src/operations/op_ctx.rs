//! Per-transaction execution context for the task-per-tx model (#1454).
//!
//! This module ships the `OpCtx` struct and its `send_and_await` round-trip
//! primitive as dormant scaffolding. See the struct-level docs for the Phase
//! 2a scope boundary.

use tokio::sync::mpsc;

use crate::message::{MessageStats, NetMessage, Transaction};
use crate::operations::OpError;

/// Per-transaction execution context for the task-per-tx model (#1454).
///
/// An `OpCtx` binds a single [`Transaction`] to the channel used to drive its
/// network round-trip through the event loop. Each transaction is owned and
/// driven by a single task; the context is [`Send`] but intentionally not
/// [`Clone`].
///
/// # Phase 2a scope
///
/// Phase 2a (#1454) ships this type as scaffolding only. The wider API
/// sketched in the design doc (`spawn_sub`, per-tx WS fanout via `notify`,
/// per-tx inbox, `OpRegistry`, and so on) is deferred. The only caller in
/// Phase 2a is the unit tests in this module — no op in `operations/` has
/// been migrated yet. Phase 2b will add the first production caller by
/// migrating SUBSCRIBE's client-initiated path.
#[allow(dead_code)] // Phase 2a scaffolding: first production caller lands in Phase 2b (#1454).
pub(crate) struct OpCtx {
    tx: Transaction,
    op_execution_sender: mpsc::Sender<(mpsc::Sender<NetMessage>, NetMessage)>,
}

#[allow(dead_code)] // Phase 2a scaffolding: first production caller lands in Phase 2b (#1454).
impl OpCtx {
    /// Construct a new context bound to `tx`.
    ///
    /// `pub(crate)` because the only legitimate constructors are
    /// [`crate::node::OpManager::op_ctx`] and the in-module tests.
    pub(crate) fn new(
        tx: Transaction,
        op_execution_sender: mpsc::Sender<(mpsc::Sender<NetMessage>, NetMessage)>,
    ) -> Self {
        Self {
            tx,
            op_execution_sender,
        }
    }

    /// The transaction this context is bound to.
    pub fn tx(&self) -> Transaction {
        self.tx
    }

    /// Send `msg` through the event loop and await a single reply keyed by
    /// the same [`Transaction`]. This is the "round-trip primitive" for the
    /// async sub-transaction refactor tracked in #1454.
    ///
    /// # Scaffolding reach
    ///
    /// As of Phase 1 (#1454, PR #3802), the reply callback inserted into
    /// `p2p_protoc::pending_op_results` is fired for every network-terminating
    /// op variant: PUT, GET, SUBSCRIBE, CONNECT, and UPDATE (see
    /// `node::forward_pending_op_result_if_completed` and the branches of
    /// `handle_pure_network_message_v1`). SUBSCRIBE's
    /// `complete_local_subscription` path (`operations/subscribe.rs`) does
    /// NOT pass through `handle_pure_network_message_v1`, so a caller
    /// targeting a locally-completed subscribe would still hang on
    /// `response_receiver.recv()` below. Closing that gap is Phase 2b work.
    ///
    /// # Deadlock risk
    ///
    /// `response_receiver.recv()` has no timeout. The reply side
    /// (`node::forward_pending_op_result_if_completed`) uses `try_send`
    /// against this capacity-1 channel so it can never block the
    /// pure-network-message handler; the remaining risk is strictly on the
    /// caller side (the task awaiting below). Phase 2b's author must
    /// guarantee that the awaiting task is not the sole driver of completion,
    /// or add an explicit timeout around `response_receiver.recv()`
    /// (see `.claude/rules/channel-safety.md`).
    ///
    /// # Single-use per [`Transaction`]
    ///
    /// The Phase 1 helper fires exactly once per tx because the `completed` /
    /// `under_progress` dedup sets suppress subsequent dispatches. Calling
    /// `send_and_await` a second time on the same `OpCtx` (or a different
    /// `OpCtx` sharing the same `Transaction`) will hang forever on
    /// `response_receiver.recv()` because no second callback will fire.
    /// Callers that need to retry a multi-attempt protocol (e.g.,
    /// SUBSCRIBE's fallback to alternative peers) must use a fresh
    /// [`Transaction`] per attempt. This is a known constraint; Phase 2b's
    /// planner must work around it.
    ///
    /// # Where to call this
    ///
    /// Must be called from an op task (e.g., one spawned via
    /// [`crate::config::GlobalExecutor::spawn`]), not from within the main
    /// event loop or a `priority_select` arm. The internal `.send().await`
    /// on `op_execution_sender` is bounded and can block; spawned tasks are
    /// OK, event loops are not. See `.claude/rules/channel-safety.md`.
    ///
    /// # Push-before-send
    ///
    /// `send_and_await` is the task-per-tx model's network send primitive,
    /// so the push-before-send invariant from `.claude/rules/operations.md`
    /// applies here directly: **any state the op will need when the reply
    /// arrives must be in place before calling this method**. The exact
    /// meaning of "in place" depends on which execution path the caller
    /// sits on:
    ///
    /// - **Legacy path** (ops still using `handle_op_result` and
    ///   `notify_op_change`): the caller must have already called
    ///   `op_manager.push(tx, updated_state).await?` for the same `tx`
    ///   before `send_and_await`, otherwise a fast response can race the
    ///   push and hit `OpNotAvailable` when the pipeline tries to look up
    ///   the op.
    /// - **Task-per-tx path** (Phase 2b and later): the op state lives in
    ///   task locals owned by the task that created this `OpCtx`. The
    ///   invariant becomes "initialize task-local state before calling
    ///   `send_and_await`": all fields the task will need when processing
    ///   the reply (retry counters, `visited` peer filters, pending
    ///   sub-operations, etc.) must be set before the `await` so that the
    ///   reply handler reads consistent state. There is no `op_manager`
    ///   DashMap race in this path because the state never leaves the
    ///   task, but the conceptual ordering rule is the same.
    ///
    /// In both cases, the rule is simple: **set up state, then send**.
    ///
    /// # Terminal reply, not success reply
    ///
    /// The returned [`NetMessage`] is whatever the op pipeline produced when
    /// `is_operation_completed` flipped true, including non-success terminal
    /// states (e.g., a `SubscribeMsg::Response::NotFound`). Callers inspect
    /// the reply to decide what happened. `Ok(reply)` does NOT imply the
    /// underlying protocol succeeded.
    ///
    /// # Errors
    ///
    /// Returns [`OpError::NotificationError`] if the executor channel is
    /// closed (send failure) or the reply receiver is dropped (receiver
    /// hang-up). Both indicate the round-trip could not complete.
    pub async fn send_and_await(&mut self, msg: NetMessage) -> Result<NetMessage, OpError> {
        debug_assert_eq!(
            msg.id(),
            &self.tx,
            "OpCtx::send_and_await: msg.id must match ctx.tx"
        );

        let (response_sender, mut response_receiver) = mpsc::channel::<NetMessage>(1);

        self.op_execution_sender
            .send((response_sender, msg))
            .await
            .map_err(|_| OpError::NotificationError)?;

        match response_receiver.recv().await {
            Some(reply) => {
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
            let (reply_sender, outbound) = op_execution_receiver
                .recv()
                .await
                .expect("outbound msg should be delivered");
            assert_eq!(outbound.id(), &tx, "outbound msg tx must match the ctx tx");
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
            let (reply_sender, _outbound) = op_execution_receiver
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

    /// Documents the "single-use per `Transaction`" constraint on
    /// [`OpCtx::send_and_await`]. The doc comment asserts that a second
    /// call on the same context "will hang forever" because Phase 1's
    /// reply helper fires exactly once per tx — this test drives that
    /// scenario with a fake executor that replies to the first outbound
    /// message and then **keeps the second `reply_sender` alive without
    /// firing it**, which is exactly what Phase 1's real dedup
    /// (`completed` / `under_progress` sets in `OpManager`) does at the
    /// pipeline level: the second `notify_op_execution` arrives, the
    /// callback is registered in `pending_op_results`, and
    /// `forward_pending_op_result_if_completed` never runs because the
    /// op is already `completed`. From `send_and_await`'s perspective
    /// the reply channel stays open forever with no message.
    ///
    /// The assertion wraps the second call in a short [`timeout`] and
    /// asserts it elapses with `Err(Elapsed)` rather than resolving to
    /// `Ok(_)` or `Err(NotificationError)`. A regression that made the
    /// second call *fail fast* (e.g., by adding a runtime check or a
    /// timeout inside `send_and_await`) would surface as `Ok(_)` from
    /// the outer `timeout` and break this assertion — at which point
    /// the doc comment on `send_and_await` must be updated to match.
    ///
    /// What this test does and does not guard against:
    /// - **Does** guard against doc drift: if a future refactor adds a
    ///   timeout or error path to the second call, the assertion shape
    ///   breaks and the doc must be updated.
    /// - **Does not** guard against the structural source of the hang
    ///   (Phase 1's `completed` / `under_progress` dedup sets). Those
    ///   live in `OpManager` and are not constructed here; this test
    ///   simulates their effect directly by holding the reply sender
    ///   alive without firing. A regression in the real dedup logic
    ///   would not be caught here — it would be caught by integration
    ///   tests that exercise the full pipeline.
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
            let (reply_sender_1, _first) = op_execution_receiver
                .recv()
                .await
                .expect("first outbound delivered");
            reply_sender_1
                .try_send(dummy_reply_with_tx(tx))
                .expect("first reply accepted");

            // Second outbound: hold `reply_sender_2` alive — do NOT drop
            // it, do NOT fire it. This models what Phase 1's real dedup
            // does at the `OpManager` level: the reply callback is
            // installed, `forward_pending_op_result_if_completed`
            // never runs again (because the op is already in the
            // `completed` set), so the reply channel simply never
            // produces a message or closes. From `send_and_await`'s
            // viewpoint this is indistinguishable from a hang.
            let (reply_sender_2, _second) = op_execution_receiver
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
        // the fake executor holds `reply_sender_2` without firing. A
        // short timeout keeps CI fast while still exercising the
        // "will hang forever" behavior documented on `send_and_await`.
        let second = timeout(
            Duration::from_millis(200),
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
}
