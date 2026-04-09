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
}
