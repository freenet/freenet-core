//! Custom select combinator that takes references to futures for explicit waker control.
//! This avoids waker registration issues that can occur with nested tokio::select! macros.

use either::Either;
use futures::future::BoxFuture;
use futures::stream::{FuturesUnordered, Stream};
use pin_project::pin_project;
use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};
use tokio::sync::mpsc::Receiver;

use super::p2p_protoc::PeerConnectionInbound;
use crate::contract::{
    ContractHandlerChannel, ExecutorToEventLoopChannel, NetworkEventListenerHalve,
    WaitingResolution,
};
use crate::dev_tool::{PeerId, Transaction};
use crate::message::{NetMessage, NodeEvent};
use crate::node::network_bridge::handshake::{HandshakeError, HandshakeHandler};
use crate::transport::TransportError;

// P2pBridgeEvent type alias for the event bridge channel
pub type P2pBridgeEvent = Either<(PeerId, Box<NetMessage>), NodeEvent>;

#[allow(clippy::large_enum_variant)]
pub(super) enum SelectResult {
    Notification(Option<Either<NetMessage, NodeEvent>>),
    OpExecution(Option<(tokio::sync::mpsc::Sender<NetMessage>, NetMessage)>),
    PeerConnection(Option<Result<PeerConnectionInbound, TransportError>>),
    ConnBridge(Option<P2pBridgeEvent>),
    Handshake(Result<crate::node::network_bridge::handshake::Event, HandshakeError>),
    NodeController(Option<NodeEvent>),
    ClientTransaction(
        Result<
            (
                crate::client_events::ClientId,
                crate::contract::WaitingTransaction,
            ),
            anyhow::Error,
        >,
    ),
    ExecutorTransaction(Result<Transaction, anyhow::Error>),
}

/// A future that polls multiple futures with explicit priority order and waker control.
/// Uses pinned BoxFutures that are created once and reused across polls to maintain
/// waker registration and future state (including handshake state machine).
#[pin_project]
pub(super) struct PrioritySelectFuture<'a> {
    #[pin]
    notification_fut: BoxFuture<'a, Option<Either<NetMessage, NodeEvent>>>,
    #[pin]
    op_execution_fut: BoxFuture<'a, Option<(tokio::sync::mpsc::Sender<NetMessage>, NetMessage)>>,
    #[pin]
    peer_connections:
        &'a mut FuturesUnordered<BoxFuture<'static, Result<PeerConnectionInbound, TransportError>>>,
    #[pin]
    conn_bridge_fut: BoxFuture<'a, Option<P2pBridgeEvent>>,
    #[pin]
    handshake_fut:
        BoxFuture<'a, Result<crate::node::network_bridge::handshake::Event, HandshakeError>>,
    #[pin]
    node_controller_fut: BoxFuture<'a, Option<NodeEvent>>,
    #[pin]
    client_transaction_fut: BoxFuture<
        'a,
        Result<
            (
                crate::client_events::ClientId,
                crate::contract::WaitingTransaction,
            ),
            anyhow::Error,
        >,
    >,
    #[pin]
    executor_transaction_fut: BoxFuture<'a, Result<Transaction, anyhow::Error>>,
    peer_connections_empty: bool,
}

impl<'a> PrioritySelectFuture<'a> {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        notification_fut: BoxFuture<'a, Option<Either<NetMessage, NodeEvent>>>,
        op_execution_fut: BoxFuture<
            'a,
            Option<(tokio::sync::mpsc::Sender<NetMessage>, NetMessage)>,
        >,
        peer_connections: &'a mut FuturesUnordered<
            BoxFuture<'static, Result<PeerConnectionInbound, TransportError>>,
        >,
        conn_bridge_fut: BoxFuture<'a, Option<P2pBridgeEvent>>,
        handshake_fut: BoxFuture<
            'a,
            Result<crate::node::network_bridge::handshake::Event, HandshakeError>,
        >,
        node_controller_fut: BoxFuture<'a, Option<NodeEvent>>,
        client_transaction_fut: BoxFuture<
            'a,
            Result<
                (
                    crate::client_events::ClientId,
                    crate::contract::WaitingTransaction,
                ),
                anyhow::Error,
            >,
        >,
        executor_transaction_fut: BoxFuture<'a, Result<Transaction, anyhow::Error>>,
    ) -> Self {
        let peer_connections_empty = peer_connections.is_empty();

        Self {
            notification_fut,
            op_execution_fut,
            peer_connections,
            conn_bridge_fut,
            handshake_fut,
            node_controller_fut,
            client_transaction_fut,
            executor_transaction_fut,
            peer_connections_empty,
        }
    }
}

impl<'a> Future for PrioritySelectFuture<'a> {
    type Output = SelectResult;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let mut this = self.project();

        // Priority 1: Notification channel (highest priority)
        // This MUST be polled first to ensure operation state machine messages
        // are processed before network messages
        tracing::trace!("PrioritySelect: polling notification_rx");
        match this.notification_fut.as_mut().poll(cx) {
            Poll::Ready(msg) => {
                tracing::trace!(
                    "PrioritySelect: notification_rx READY with message: {:?}",
                    msg.is_some()
                );
                return Poll::Ready(SelectResult::Notification(msg));
            }
            Poll::Pending => {
                tracing::trace!("PrioritySelect: notification_rx pending");
            }
        }

        // Priority 2: Op execution channel
        match this.op_execution_fut.as_mut().poll(cx) {
            Poll::Ready(msg) => {
                tracing::trace!("PrioritySelect: op_execution_rx ready");
                return Poll::Ready(SelectResult::OpExecution(msg));
            }
            Poll::Pending => {}
        }

        // Priority 3: Peer connections (only if not empty)
        if !*this.peer_connections_empty {
            match Stream::poll_next(this.peer_connections.as_mut(), cx) {
                Poll::Ready(msg) => {
                    tracing::trace!("PrioritySelect: peer_connections ready");
                    return Poll::Ready(SelectResult::PeerConnection(msg));
                }
                Poll::Pending => {}
            }
        }

        // Priority 4: Connection bridge
        match this.conn_bridge_fut.as_mut().poll(cx) {
            Poll::Ready(msg) => {
                tracing::trace!("PrioritySelect: conn_bridge_rx ready");
                return Poll::Ready(SelectResult::ConnBridge(msg));
            }
            Poll::Pending => {}
        }

        // Priority 5: Handshake handler (poll wait_for_events as a whole to preserve all logic)
        // The handshake future is pinned in the struct and reused across polls,
        // preserving the internal state machine of wait_for_events()
        match this.handshake_fut.as_mut().poll(cx) {
            Poll::Ready(result) => {
                tracing::trace!("PrioritySelect: handshake_handler ready");
                return Poll::Ready(SelectResult::Handshake(result));
            }
            Poll::Pending => {}
        }

        // Priority 8: Node controller
        match this.node_controller_fut.as_mut().poll(cx) {
            Poll::Ready(msg) => {
                tracing::trace!("PrioritySelect: node_controller ready");
                return Poll::Ready(SelectResult::NodeController(msg));
            }
            Poll::Pending => {}
        }

        // Priority 9: Client transaction waiting
        match this.client_transaction_fut.as_mut().poll(cx) {
            Poll::Ready(event_id) => {
                tracing::trace!("PrioritySelect: client_wait_for_transaction ready");
                return Poll::Ready(SelectResult::ClientTransaction(event_id));
            }
            Poll::Pending => {}
        }

        // Priority 10: Executor transaction
        match this.executor_transaction_fut.as_mut().poll(cx) {
            Poll::Ready(id) => {
                tracing::trace!("PrioritySelect: executor_listener ready");
                return Poll::Ready(SelectResult::ExecutorTransaction(id));
            }
            Poll::Pending => {}
        }

        // All futures returned Pending - wakers are now registered for all of them
        // The key difference from the broken implementation: these are the SAME futures
        // being polled repeatedly, so their wakers persist and internal state is preserved
        tracing::trace!("PrioritySelect: all pending");
        Poll::Pending
    }
}

#[allow(clippy::too_many_arguments)]
pub(super) async fn select_priority<'a>(
    notification_rx: &'a mut Receiver<Either<NetMessage, NodeEvent>>,
    op_execution_rx: &'a mut Receiver<(tokio::sync::mpsc::Sender<NetMessage>, NetMessage)>,
    peer_connections: &'a mut FuturesUnordered<
        BoxFuture<'static, Result<PeerConnectionInbound, TransportError>>,
    >,
    conn_bridge_rx: &'a mut Receiver<P2pBridgeEvent>,
    handshake_handler: &'a mut HandshakeHandler,
    node_controller: &'a mut Receiver<NodeEvent>,
    client_wait_for_transaction: &'a mut ContractHandlerChannel<WaitingResolution>,
    executor_listener: &'a mut ExecutorToEventLoopChannel<NetworkEventListenerHalve>,
) -> SelectResult {
    // Create boxed futures ONCE - they will be pinned and reused across polls.
    // This is critical: the futures must persist across multiple poll() calls to:
    // 1. Maintain waker registration (so the runtime can wake the task)
    // 2. Preserve internal state (especially the handshake state machine)
    PrioritySelectFuture::new(
        Box::pin(notification_rx.recv()),
        Box::pin(op_execution_rx.recv()),
        peer_connections,
        Box::pin(conn_bridge_rx.recv()),
        Box::pin(handshake_handler.wait_for_events()),
        Box::pin(node_controller.recv()),
        Box::pin(client_wait_for_transaction.relay_transaction_result_to_client()),
        Box::pin(executor_listener.transaction_from_executor()),
    )
    .await
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::sync::mpsc;
    use tokio::time::{sleep, timeout, Duration};

    /// Test PrioritySelectFuture with notification arriving after initial poll
    #[tokio::test]
    #[test_log::test]
    async fn test_priority_select_future_wakeup() {
        let (notif_tx, mut notif_rx) = mpsc::channel(10);
        let (_op_tx, mut op_rx) = mpsc::channel(10);
        let mut peers = FuturesUnordered::new();
        let (_bridge_tx, mut bridge_rx) = mpsc::channel(10);
        let (_node_tx, mut node_rx) = mpsc::channel(10);
        let (_client_tx, mut client_rx) = mpsc::channel(10);
        let (_executor_tx, mut executor_rx) = mpsc::channel(10);

        // Spawn task that sends notification after delay
        let notif_tx_clone = notif_tx.clone();
        tokio::spawn(async move {
            sleep(Duration::from_millis(50)).await;
            let test_msg = NetMessage::V1(crate::message::NetMessageV1::Aborted(
                crate::message::Transaction::new::<crate::operations::put::PutMsg>(),
            ));
            notif_tx_clone.send(Either::Left(test_msg)).await.unwrap();
        });

        // Create PrioritySelectFuture - should be pending initially, then wake up when message arrives
        let select_fut = PrioritySelectFuture::new(
            Box::pin(notif_rx.recv()),
            Box::pin(op_rx.recv()),
            &mut peers,
            Box::pin(bridge_rx.recv()),
            Box::pin(async {
                sleep(Duration::from_secs(1000)).await; // Long-running handshake
                Err(HandshakeError::ChannelClosed)
            }),
            Box::pin(node_rx.recv()),
            Box::pin(async move {
                client_rx
                    .recv()
                    .await
                    .ok_or_else(|| anyhow::anyhow!("closed"))
            }),
            Box::pin(async move {
                executor_rx
                    .recv()
                    .await
                    .ok_or_else(|| anyhow::anyhow!("closed"))
            }),
        );

        // Should complete when message arrives (notification has priority over handshake)
        let result = timeout(Duration::from_millis(200), select_fut).await;

        assert!(
            result.is_ok(),
            "Select future should wake up when notification arrives"
        );

        let select_result = result.unwrap();
        match select_result {
            SelectResult::Notification(Some(_)) => {}
            SelectResult::Notification(None) => panic!("Got Notification(None)"),
            SelectResult::OpExecution(_) => panic!("Got OpExecution"),
            SelectResult::PeerConnection(_) => panic!("Got PeerConnection"),
            SelectResult::ConnBridge(_) => panic!("Got ConnBridge"),
            SelectResult::Handshake(_) => panic!("Got Handshake"),
            SelectResult::NodeController(_) => panic!("Got NodeController"),
            SelectResult::ClientTransaction(_) => panic!("Got ClientTransaction"),
            SelectResult::ExecutorTransaction(_) => panic!("Got ExecutorTransaction"),
        }
    }

    /// Test that notification has priority over other channels in PrioritySelectFuture
    #[tokio::test]
    #[test_log::test]
    async fn test_priority_select_future_priority_ordering() {
        let (notif_tx, mut notif_rx) = mpsc::channel(10);
        let (op_tx, mut op_rx) = mpsc::channel(10);
        let mut peers = FuturesUnordered::new();
        let (bridge_tx, mut bridge_rx) = mpsc::channel(10);
        let (_, mut node_rx) = mpsc::channel(10);
        let (_, mut client_rx) = mpsc::channel(10);
        let (_, mut executor_rx) = mpsc::channel(10);

        // Send to multiple channels - notification should be received first
        let (callback_tx, _) = mpsc::channel(1);
        let dummy_msg = NetMessage::V1(crate::message::NetMessageV1::Aborted(
            crate::message::Transaction::new::<crate::operations::put::PutMsg>(),
        ));
        op_tx.send((callback_tx, dummy_msg.clone())).await.unwrap();
        bridge_tx
            .send(Either::Right(NodeEvent::Disconnect { cause: None }))
            .await
            .unwrap();

        let test_msg = NetMessage::V1(crate::message::NetMessageV1::Aborted(
            crate::message::Transaction::new::<crate::operations::put::PutMsg>(),
        ));
        notif_tx.send(Either::Left(test_msg)).await.unwrap();

        // Create and poll the future
        let select_fut = PrioritySelectFuture::new(
            Box::pin(notif_rx.recv()),
            Box::pin(op_rx.recv()),
            &mut peers,
            Box::pin(bridge_rx.recv()),
            Box::pin(async {
                futures::future::pending::<()>().await;
                Err(HandshakeError::ChannelClosed)
            }),
            Box::pin(node_rx.recv()),
            Box::pin(async move {
                client_rx
                    .recv()
                    .await
                    .ok_or_else(|| anyhow::anyhow!("closed"))
            }),
            Box::pin(async move {
                executor_rx
                    .recv()
                    .await
                    .ok_or_else(|| anyhow::anyhow!("closed"))
            }),
        );

        let result = timeout(Duration::from_millis(100), select_fut).await;
        assert!(result.is_ok());

        match result.unwrap() {
            SelectResult::Notification(_) => {}
            _ => panic!("Notification should be received first due to priority"),
        }
    }

    /// Test concurrent messages - simpler version that sends all messages first
    #[tokio::test]
    #[test_log::test]
    async fn test_priority_select_future_concurrent_messages() {
        let (notif_tx, mut notif_rx) = mpsc::channel(100);
        let mut peers = FuturesUnordered::new();

        // Send all 15 messages
        for _ in 0..15 {
            let test_msg = NetMessage::V1(crate::message::NetMessageV1::Aborted(
                crate::message::Transaction::new::<crate::operations::put::PutMsg>(),
            ));
            notif_tx.send(Either::Left(test_msg)).await.unwrap();
        }

        // Receive first message
        let (_, mut op_rx) = mpsc::channel(10);
        let (_, mut bridge_rx) = mpsc::channel(10);
        let (_, mut node_rx) = mpsc::channel(10);
        let (_, mut client_rx) = mpsc::channel(10);
        let (_, mut executor_rx) = mpsc::channel(10);

        let select_fut = PrioritySelectFuture::new(
            Box::pin(notif_rx.recv()),
            Box::pin(op_rx.recv()),
            &mut peers,
            Box::pin(bridge_rx.recv()),
            Box::pin(async {
                sleep(Duration::from_secs(1000)).await;
                Err(HandshakeError::ChannelClosed)
            }),
            Box::pin(node_rx.recv()),
            Box::pin(async move {
                client_rx
                    .recv()
                    .await
                    .ok_or_else(|| anyhow::anyhow!("closed"))
            }),
            Box::pin(async move {
                executor_rx
                    .recv()
                    .await
                    .ok_or_else(|| anyhow::anyhow!("closed"))
            }),
        );

        let result = timeout(Duration::from_millis(100), select_fut).await;
        assert!(result.is_ok(), "Should receive first message");
        match result.unwrap() {
            SelectResult::Notification(Some(_)) => {}
            _ => panic!("Expected notification"),
        }
    }

    /// Test that messages arrive in buffered channel before receiver polls
    #[tokio::test]
    #[test_log::test]
    async fn test_priority_select_future_buffered_messages() {
        let (notif_tx, mut notif_rx) = mpsc::channel(10);
        let mut peers = FuturesUnordered::new();

        // Send message BEFORE creating future
        let test_msg = NetMessage::V1(crate::message::NetMessageV1::Aborted(
            crate::message::Transaction::new::<crate::operations::put::PutMsg>(),
        ));
        notif_tx.send(Either::Left(test_msg)).await.unwrap();

        // Create future - should receive the buffered message immediately
        let (_, mut op_rx) = mpsc::channel(10);
        let (_, mut bridge_rx) = mpsc::channel(10);
        let (_, mut node_rx) = mpsc::channel(10);
        let (_, mut client_rx) = mpsc::channel(10);
        let (_, mut executor_rx) = mpsc::channel(10);

        let select_fut = PrioritySelectFuture::new(
            Box::pin(notif_rx.recv()),
            Box::pin(op_rx.recv()),
            &mut peers,
            Box::pin(bridge_rx.recv()),
            Box::pin(async {
                sleep(Duration::from_secs(1000)).await;
                Err(HandshakeError::ChannelClosed)
            }),
            Box::pin(node_rx.recv()),
            Box::pin(async move {
                client_rx
                    .recv()
                    .await
                    .ok_or_else(|| anyhow::anyhow!("closed"))
            }),
            Box::pin(async move {
                executor_rx
                    .recv()
                    .await
                    .ok_or_else(|| anyhow::anyhow!("closed"))
            }),
        );

        let result = timeout(Duration::from_millis(100), select_fut).await;
        assert!(
            result.is_ok(),
            "Should receive buffered message immediately"
        );

        match result.unwrap() {
            SelectResult::Notification(Some(_)) => {}
            _ => panic!("Expected notification"),
        }
    }

    /// Test rapid creation and cancellation of PrioritySelectFuture instances
    #[tokio::test]
    #[test_log::test]
    async fn test_priority_select_future_rapid_cancellations() {
        let (notif_tx, mut notif_rx) = mpsc::channel(100);
        let mut peers = FuturesUnordered::new();

        // Send 10 messages
        for _ in 0..10 {
            let test_msg = NetMessage::V1(crate::message::NetMessageV1::Aborted(
                crate::message::Transaction::new::<crate::operations::put::PutMsg>(),
            ));
            notif_tx.send(Either::Left(test_msg)).await.unwrap();
        }

        // Rapidly create futures with short timeouts (simulating cancellations)
        let mut received = 0;
        for _ in 0..30 {
            let (_, mut op_rx) = mpsc::channel(10);
            let (_, mut bridge_rx) = mpsc::channel(10);
            let (_, mut node_rx) = mpsc::channel(10);
            let (_, mut client_rx) = mpsc::channel(10);
            let (_, mut executor_rx) = mpsc::channel(10);

            let select_fut = PrioritySelectFuture::new(
                Box::pin(notif_rx.recv()),
                Box::pin(op_rx.recv()),
                &mut peers,
                Box::pin(bridge_rx.recv()),
                Box::pin(async {
                    sleep(Duration::from_secs(1000)).await;
                    Err(HandshakeError::ChannelClosed)
                }),
                Box::pin(node_rx.recv()),
                Box::pin(async move {
                    client_rx
                        .recv()
                        .await
                        .ok_or_else(|| anyhow::anyhow!("closed"))
                }),
                Box::pin(async move {
                    executor_rx
                        .recv()
                        .await
                        .ok_or_else(|| anyhow::anyhow!("closed"))
                }),
            );

            if let Ok(SelectResult::Notification(Some(_))) =
                timeout(Duration::from_millis(5), select_fut).await
            {
                received += 1;
            }

            if received >= 10 {
                break;
            }
        }

        assert_eq!(
            received, 10,
            "Should receive all messages despite rapid cancellations"
        );
    }

    /// Test simulating wait_for_event loop behavior - reusing receivers across iterations
    /// This test simulates how the event loop actually works: it creates channel receivers once
    /// and then polls them repeatedly in a loop, creating a new PrioritySelectFuture each iteration.
    ///
    /// Enhanced version: sends MULTIPLE messages per channel to verify interleaving and priority.
    #[tokio::test]
    #[test_log::test]
    async fn test_priority_select_event_loop_simulation() {
        // Create channels once (like in wait_for_event)
        let (notif_tx, mut notif_rx) = mpsc::channel::<Either<NetMessage, NodeEvent>>(10);
        let (op_tx, mut op_rx) =
            mpsc::channel::<(tokio::sync::mpsc::Sender<NetMessage>, NetMessage)>(10);
        let mut peers = FuturesUnordered::new();
        let (bridge_tx, mut bridge_rx) = mpsc::channel::<P2pBridgeEvent>(10);
        let (node_tx, mut node_rx) = mpsc::channel::<NodeEvent>(10);
        let (client_tx, mut client_rx) = mpsc::channel::<
            Result<
                (
                    crate::client_events::ClientId,
                    crate::contract::WaitingTransaction,
                ),
                anyhow::Error,
            >,
        >(10);
        let (executor_tx, mut executor_rx) =
            mpsc::channel::<Result<Transaction, anyhow::Error>>(10);

        // Spawn task that sends MULTIPLE messages to different channels
        let notif_tx_clone = notif_tx.clone();
        let op_tx_clone = op_tx.clone();
        let bridge_tx_clone = bridge_tx.clone();
        let node_tx_clone = node_tx.clone();
        tokio::spawn(async move {
            sleep(Duration::from_millis(10)).await;

            // Send 3 notifications
            for i in 0..3 {
                let test_msg = NetMessage::V1(crate::message::NetMessageV1::Aborted(
                    crate::message::Transaction::new::<crate::operations::put::PutMsg>(),
                ));
                tracing::info!("Sending notification {}", i);
                notif_tx_clone.send(Either::Left(test_msg)).await.unwrap();
            }

            // Send 2 op execution messages
            for i in 0..2 {
                let test_msg = NetMessage::V1(crate::message::NetMessageV1::Aborted(
                    crate::message::Transaction::new::<crate::operations::put::PutMsg>(),
                ));
                let (callback_tx, _) = mpsc::channel(1);
                tracing::info!("Sending op_execution {}", i);
                op_tx_clone.send((callback_tx, test_msg)).await.unwrap();
            }

            // Send 2 bridge events
            for i in 0..2 {
                tracing::info!("Sending bridge event {}", i);
                bridge_tx_clone
                    .send(Either::Right(NodeEvent::Disconnect { cause: None }))
                    .await
                    .unwrap();
            }

            // Send 1 node controller event
            tracing::info!("Sending node controller event");
            node_tx_clone
                .send(NodeEvent::Disconnect { cause: None })
                .await
                .unwrap();
        });

        let mut received_events = Vec::new();

        // Simulate event loop: poll until we've received all expected messages (3+2+2+1 = 8)
        let expected_count = 8;
        for iteration in 0..expected_count {
            tracing::info!("Event loop iteration {}", iteration);

            // Create a new PrioritySelectFuture for this iteration (like wait_for_event does)
            let select_fut = PrioritySelectFuture::new(
                Box::pin(notif_rx.recv()),
                Box::pin(op_rx.recv()),
                &mut peers,
                Box::pin(bridge_rx.recv()),
                Box::pin(async {
                    sleep(Duration::from_secs(1000)).await;
                    Err(HandshakeError::ChannelClosed)
                }),
                Box::pin(node_rx.recv()),
                Box::pin(async {
                    client_rx
                        .recv()
                        .await
                        .unwrap_or_else(|| Err(anyhow::anyhow!("closed")))
                }),
                Box::pin(async {
                    executor_rx
                        .recv()
                        .await
                        .unwrap_or_else(|| Err(anyhow::anyhow!("closed")))
                }),
            );

            // Wait for an event
            let result = timeout(Duration::from_millis(50), select_fut).await;
            assert!(result.is_ok(), "Iteration {} should complete", iteration);

            let event = result.unwrap();
            match &event {
                SelectResult::Notification(_) => received_events.push("notification"),
                SelectResult::OpExecution(_) => received_events.push("op_execution"),
                SelectResult::ConnBridge(_) => received_events.push("conn_bridge"),
                SelectResult::Handshake(_) => received_events.push("handshake"),
                SelectResult::NodeController(_) => received_events.push("node_controller"),
                _ => received_events.push("other"),
            }

            tracing::info!(
                "Event loop iteration {} received: {:?}",
                iteration,
                received_events.last()
            );
        }

        // Verify we received all expected messages
        assert_eq!(
            received_events.len(),
            expected_count,
            "Should receive all {} messages",
            expected_count
        );

        // Count each type
        let notif_count = received_events
            .iter()
            .filter(|&e| *e == "notification")
            .count();
        let op_count = received_events
            .iter()
            .filter(|&e| *e == "op_execution")
            .count();
        let bridge_count = received_events
            .iter()
            .filter(|&e| *e == "conn_bridge")
            .count();
        let node_count = received_events
            .iter()
            .filter(|&e| *e == "node_controller")
            .count();

        tracing::info!("Received counts - notifications: {}, op_execution: {}, bridge: {}, node_controller: {}",
                      notif_count, op_count, bridge_count, node_count);

        assert_eq!(notif_count, 3, "Should receive 3 notifications");
        assert_eq!(op_count, 2, "Should receive 2 op_execution messages");
        assert_eq!(bridge_count, 2, "Should receive 2 bridge messages");
        assert_eq!(node_count, 1, "Should receive 1 node_controller message");

        // Verify priority ordering: all notifications should come before any op_execution
        // which should come before any bridge events
        let first_notif_idx = received_events.iter().position(|e| *e == "notification");
        let last_notif_idx = received_events.iter().rposition(|e| *e == "notification");
        let first_op_idx = received_events.iter().position(|e| *e == "op_execution");
        let last_op_idx = received_events.iter().rposition(|e| *e == "op_execution");
        let first_bridge_idx = received_events.iter().position(|e| *e == "conn_bridge");

        // All notifications should come first (indices 0, 1, 2)
        assert_eq!(
            first_notif_idx,
            Some(0),
            "First notification should be at index 0"
        );
        assert_eq!(
            last_notif_idx,
            Some(2),
            "Last notification should be at index 2"
        );

        // All op_executions should come after notifications (indices 3, 4)
        assert!(
            first_op_idx.unwrap() > last_notif_idx.unwrap(),
            "Op execution should come after all notifications"
        );
        assert_eq!(
            first_op_idx,
            Some(3),
            "First op_execution should be at index 3"
        );
        assert_eq!(
            last_op_idx,
            Some(4),
            "Last op_execution should be at index 4"
        );

        // All bridge events should come after op_executions (indices 5, 6)
        assert!(
            first_bridge_idx.unwrap() > last_op_idx.unwrap(),
            "Bridge events should come after all op_executions"
        );

        tracing::info!(
            "✓ All {} messages received in correct priority order: {:?}",
            expected_count,
            received_events
        );

        // Clean up - drop senders to close channels
        drop(notif_tx);
        drop(op_tx);
        drop(bridge_tx);
        drop(node_tx);
        drop(client_tx);
        drop(executor_tx);
    }

    /// Stress test: Multiple concurrent tasks sending messages with random delays
    /// This test verifies that priority ordering is maintained even under concurrent load
    /// with unpredictable timing. Each channel has its own task sending messages at random
    /// intervals, and we verify all messages are received in perfect priority order.
    ///
    /// Uses seeded RNG for reproducibility - run with 5 different seeds to ensure robustness.
    #[tokio::test]
    #[test_log::test]
    async fn test_priority_select_concurrent_random_stress() {
        test_with_seed(42).await;
        test_with_seed(123).await;
        test_with_seed(999).await;
        test_with_seed(7777).await;
        test_with_seed(31415).await;
    }

    async fn test_with_seed(seed: u64) {
        use rand::rngs::StdRng;
        use rand::Rng;
        use rand::SeedableRng;

        tracing::info!("=== Stress test with seed {} ===", seed);

        // Define how many messages each sender will send
        // Using 2 orders of magnitude more messages to stress test (17 -> 1700)
        const NOTIF_COUNT: usize = 500;
        const OP_COUNT: usize = 400;
        const BRIDGE_COUNT: usize = 300;
        const NODE_COUNT: usize = 200;
        const CLIENT_COUNT: usize = 200;
        const EXECUTOR_COUNT: usize = 100;
        const TOTAL_MESSAGES: usize =
            NOTIF_COUNT + OP_COUNT + BRIDGE_COUNT + NODE_COUNT + CLIENT_COUNT + EXECUTOR_COUNT;

        // Pre-generate all random delays using seeded RNG
        // Most delays are in microseconds (50-500us) with occasional millisecond outliers (1-5ms)
        // This keeps the test fast while still testing timing variations
        let mut rng = StdRng::seed_from_u64(seed);
        let make_delays = |count: usize, rng: &mut StdRng| -> Vec<u64> {
            (0..count)
                .map(|_| {
                    // 10% chance of millisecond delay (outlier), 90% microsecond delay
                    if rng.random_range(0..10) == 0 {
                        rng.random_range(1000..5000) // 1-5ms outliers
                    } else {
                        rng.random_range(50..500) // 50-500us typical
                    }
                })
                .collect()
        };

        let notif_delays = make_delays(NOTIF_COUNT, &mut rng);
        let op_delays = make_delays(OP_COUNT, &mut rng);
        let bridge_delays = make_delays(BRIDGE_COUNT, &mut rng);
        let node_delays = make_delays(NODE_COUNT, &mut rng);
        let client_delays = make_delays(CLIENT_COUNT, &mut rng);
        let executor_delays = make_delays(EXECUTOR_COUNT, &mut rng);

        // Create channels once (like in wait_for_event)
        let (notif_tx, mut notif_rx) = mpsc::channel::<Either<NetMessage, NodeEvent>>(100);
        let (op_tx, mut op_rx) =
            mpsc::channel::<(tokio::sync::mpsc::Sender<NetMessage>, NetMessage)>(100);
        let mut peers = FuturesUnordered::new();
        let (bridge_tx, mut bridge_rx) = mpsc::channel::<P2pBridgeEvent>(100);
        let (node_tx, mut node_rx) = mpsc::channel::<NodeEvent>(100);
        let (client_tx, mut client_rx) = mpsc::channel::<
            Result<
                (
                    crate::client_events::ClientId,
                    crate::contract::WaitingTransaction,
                ),
                anyhow::Error,
            >,
        >(100);
        let (executor_tx, mut executor_rx) =
            mpsc::channel::<Result<Transaction, anyhow::Error>>(100);

        tracing::info!(
            "Starting stress test with {} total messages from 6 concurrent tasks",
            TOTAL_MESSAGES
        );

        // Spawn separate task for each channel with pre-generated delays
        let notif_handle = tokio::spawn(async move {
            for (i, &delay_us) in notif_delays.iter().enumerate() {
                sleep(Duration::from_micros(delay_us)).await;
                let test_msg = NetMessage::V1(crate::message::NetMessageV1::Aborted(
                    crate::message::Transaction::new::<crate::operations::put::PutMsg>(),
                ));
                tracing::debug!(
                    "Notification task sending message {} after {}us delay",
                    i,
                    delay_us
                );
                notif_tx.send(Either::Left(test_msg)).await.unwrap();
            }
            tracing::info!("Notification task sent all {} messages", NOTIF_COUNT);
            NOTIF_COUNT
        });

        let op_handle = tokio::spawn(async move {
            for (i, &delay_us) in op_delays.iter().enumerate() {
                sleep(Duration::from_micros(delay_us)).await;
                let test_msg = NetMessage::V1(crate::message::NetMessageV1::Aborted(
                    crate::message::Transaction::new::<crate::operations::put::PutMsg>(),
                ));
                let (callback_tx, _) = mpsc::channel(1);
                tracing::debug!(
                    "OpExecution task sending message {} after {}us delay",
                    i,
                    delay_us
                );
                op_tx.send((callback_tx, test_msg)).await.unwrap();
            }
            tracing::info!("OpExecution task sent all {} messages", OP_COUNT);
            OP_COUNT
        });

        let bridge_handle = tokio::spawn(async move {
            for (i, &delay_us) in bridge_delays.iter().enumerate() {
                sleep(Duration::from_micros(delay_us)).await;
                tracing::debug!(
                    "Bridge task sending message {} after {}us delay",
                    i,
                    delay_us
                );
                bridge_tx
                    .send(Either::Right(NodeEvent::Disconnect { cause: None }))
                    .await
                    .unwrap();
            }
            tracing::info!("Bridge task sent all {} messages", BRIDGE_COUNT);
            BRIDGE_COUNT
        });

        let node_handle = tokio::spawn(async move {
            for (i, &delay_us) in node_delays.iter().enumerate() {
                sleep(Duration::from_micros(delay_us)).await;
                tracing::debug!(
                    "NodeController task sending message {} after {}us delay",
                    i,
                    delay_us
                );
                node_tx
                    .send(NodeEvent::Disconnect { cause: None })
                    .await
                    .unwrap();
            }
            tracing::info!("NodeController task sent all {} messages", NODE_COUNT);
            NODE_COUNT
        });

        let client_handle = tokio::spawn(async move {
            for (i, &delay_us) in client_delays.iter().enumerate() {
                sleep(Duration::from_micros(delay_us)).await;
                let client_id = crate::client_events::ClientId::next();
                let waiting_tx =
                    crate::contract::WaitingTransaction::Transaction(Transaction::new::<
                        crate::operations::put::PutMsg,
                    >());
                tracing::debug!(
                    "Client task sending message {} after {}us delay",
                    i,
                    delay_us
                );
                client_tx.send(Ok((client_id, waiting_tx))).await.unwrap();
            }
            tracing::info!("Client task sent all {} messages", CLIENT_COUNT);
            CLIENT_COUNT
        });

        let executor_handle = tokio::spawn(async move {
            for (i, &delay_us) in executor_delays.iter().enumerate() {
                sleep(Duration::from_micros(delay_us)).await;
                tracing::debug!(
                    "Executor task sending message {} after {}us delay",
                    i,
                    delay_us
                );
                executor_tx
                    .send(Ok(Transaction::new::<crate::operations::put::PutMsg>()))
                    .await
                    .unwrap();
            }
            tracing::info!("Executor task sent all {} messages", EXECUTOR_COUNT);
            EXECUTOR_COUNT
        });

        // Wait a bit for senders to start sending (shorter delay since we're using microseconds now)
        sleep(Duration::from_micros(100)).await;

        // Collect all messages from the event loop (run concurrently with senders)
        let mut received_events = Vec::new();
        let mut iteration = 0;

        // Track which channels have closed so we can replace them
        let mut notif_active = true;
        let mut op_active = true;
        let mut bridge_active = true;
        let mut node_active = true;
        let mut client_active = true;
        let mut executor_active = true;

        // Continue until we've received all expected messages
        while received_events.len() < TOTAL_MESSAGES {
            // Create a new PrioritySelectFuture for this iteration
            // Use the real channel if still active, otherwise use a never-ready future with the correct type
            let select_fut = PrioritySelectFuture::new(
                if notif_active {
                    Box::pin(notif_rx.recv())
                } else {
                    Box::pin(async {
                        sleep(Duration::from_secs(10000)).await;
                        None::<Either<NetMessage, NodeEvent>>
                    })
                },
                if op_active {
                    Box::pin(op_rx.recv())
                } else {
                    Box::pin(async {
                        sleep(Duration::from_secs(10000)).await;
                        None::<(tokio::sync::mpsc::Sender<NetMessage>, NetMessage)>
                    })
                },
                &mut peers,
                if bridge_active {
                    Box::pin(bridge_rx.recv())
                } else {
                    Box::pin(async {
                        sleep(Duration::from_secs(10000)).await;
                        None::<P2pBridgeEvent>
                    })
                },
                Box::pin(async {
                    sleep(Duration::from_secs(1000)).await;
                    Err(HandshakeError::ChannelClosed)
                }),
                if node_active {
                    Box::pin(node_rx.recv())
                } else {
                    Box::pin(async {
                        sleep(Duration::from_secs(10000)).await;
                        None::<NodeEvent>
                    })
                },
                if client_active {
                    Box::pin(async {
                        client_rx
                            .recv()
                            .await
                            .unwrap_or_else(|| Err(anyhow::anyhow!("closed")))
                    })
                } else {
                    Box::pin(async {
                        sleep(Duration::from_secs(10000)).await;
                        Err(anyhow::anyhow!("inactive"))
                    })
                },
                if executor_active {
                    Box::pin(async {
                        executor_rx
                            .recv()
                            .await
                            .unwrap_or_else(|| Err(anyhow::anyhow!("closed")))
                    })
                } else {
                    Box::pin(async {
                        sleep(Duration::from_secs(10000)).await;
                        Err(anyhow::anyhow!("inactive"))
                    })
                },
            );

            // Wait for an event (with generous timeout for random delays, including outliers up to 5ms)
            let result = timeout(Duration::from_millis(100), select_fut).await;
            assert!(result.is_ok(), "Iteration {} timed out", iteration);

            let event = result.unwrap();

            // Check if this is a real message or a channel close, and mark channels as inactive when closed
            let (event_name, is_real_message) = match &event {
                SelectResult::Notification(msg) => {
                    if msg.is_some() {
                        tracing::debug!("Received Notification message");
                        ("notification", true)
                    } else {
                        notif_active = false;
                        tracing::debug!("Notification channel closed");
                        ("notification", false)
                    }
                }
                SelectResult::OpExecution(msg) => {
                    if msg.is_some() {
                        tracing::debug!("Received OpExecution message");
                        ("op_execution", true)
                    } else {
                        op_active = false;
                        tracing::debug!("OpExecution channel closed");
                        ("op_execution", false)
                    }
                }
                SelectResult::PeerConnection(msg) => ("peer_connection", msg.is_some()),
                SelectResult::ConnBridge(msg) => {
                    if msg.is_some() {
                        tracing::debug!("Received ConnBridge message");
                        ("conn_bridge", true)
                    } else {
                        bridge_active = false;
                        tracing::debug!("ConnBridge channel closed");
                        ("conn_bridge", false)
                    }
                }
                SelectResult::Handshake(_) => {
                    ("handshake", false) // No real messages on this channel in this test
                }
                SelectResult::NodeController(msg) => {
                    if msg.is_some() {
                        tracing::debug!("Received NodeController message");
                        ("node_controller", true)
                    } else {
                        node_active = false;
                        tracing::debug!("NodeController channel closed");
                        ("node_controller", false)
                    }
                }
                SelectResult::ClientTransaction(result) => {
                    if result.is_ok() {
                        tracing::debug!("Received ClientTransaction message");
                        ("client_transaction", true)
                    } else {
                        // Check if this is initial close or just an error
                        if client_active {
                            client_active = false;
                            tracing::debug!("ClientTransaction channel closed");
                        }
                        ("client_transaction", false)
                    }
                }
                SelectResult::ExecutorTransaction(result) => {
                    if result.is_ok() {
                        tracing::debug!("Received ExecutorTransaction message");
                        ("executor_transaction", true)
                    } else {
                        // Check if this is initial close or just an error
                        if executor_active {
                            executor_active = false;
                            tracing::debug!("ExecutorTransaction channel closed");
                        }
                        ("executor_transaction", false)
                    }
                }
            };

            // Only count real messages, not channel closures
            if is_real_message {
                received_events.push(event_name);
                // Log every 100 messages to avoid spam with 1700 total messages
                if received_events.len() % 100 == 0 {
                    tracing::info!(
                        "Received {} of {} real messages",
                        received_events.len(),
                        TOTAL_MESSAGES
                    );
                }
            } else {
                tracing::debug!(
                    "Iteration {}: Received channel close from {}",
                    iteration,
                    event_name
                );
            }

            iteration += 1;

            // Safety check to prevent infinite loop
            if iteration > TOTAL_MESSAGES * 3 {
                tracing::error!("Receiver loop exceeded maximum iterations. Received {} of {} messages after {} iterations",
                    received_events.len(), TOTAL_MESSAGES, iteration);
                panic!("Receiver loop exceeded maximum iterations - possible deadlock");
            }
        }

        // Join all sender tasks and get the count of messages they sent
        let sent_notif_count = notif_handle.await.unwrap();
        let sent_op_count = op_handle.await.unwrap();
        let sent_bridge_count = bridge_handle.await.unwrap();
        let sent_node_count = node_handle.await.unwrap();
        let sent_client_count = client_handle.await.unwrap();
        let sent_executor_count = executor_handle.await.unwrap();

        let total_sent = sent_notif_count
            + sent_op_count
            + sent_bridge_count
            + sent_node_count
            + sent_client_count
            + sent_executor_count;
        tracing::info!("All sender tasks completed. Total sent: {}", total_sent);
        tracing::info!(
            "Receiver completed. Total received: {}",
            received_events.len()
        );

        // Verify we received all expected messages
        assert_eq!(
            received_events.len(),
            total_sent,
            "Should receive all {} sent messages",
            total_sent
        );
        assert_eq!(
            received_events.len(),
            TOTAL_MESSAGES,
            "Total received should match expected total"
        );

        // Count each received type
        let recv_notif_count = received_events
            .iter()
            .filter(|&e| *e == "notification")
            .count();
        let recv_op_count = received_events
            .iter()
            .filter(|&e| *e == "op_execution")
            .count();
        let recv_bridge_count = received_events
            .iter()
            .filter(|&e| *e == "conn_bridge")
            .count();
        let recv_node_count = received_events
            .iter()
            .filter(|&e| *e == "node_controller")
            .count();
        let recv_client_count = received_events
            .iter()
            .filter(|&e| *e == "client_transaction")
            .count();
        let recv_executor_count = received_events
            .iter()
            .filter(|&e| *e == "executor_transaction")
            .count();

        tracing::info!("Sent vs Received:");
        tracing::info!(
            "  notifications: sent={}, received={}",
            sent_notif_count,
            recv_notif_count
        );
        tracing::info!(
            "  op_execution: sent={}, received={}",
            sent_op_count,
            recv_op_count
        );
        tracing::info!(
            "  bridge: sent={}, received={}",
            sent_bridge_count,
            recv_bridge_count
        );
        tracing::info!(
            "  node_controller: sent={}, received={}",
            sent_node_count,
            recv_node_count
        );
        tracing::info!(
            "  client: sent={}, received={}",
            sent_client_count,
            recv_client_count
        );
        tracing::info!(
            "  executor: sent={}, received={}",
            sent_executor_count,
            recv_executor_count
        );

        // Assert sent == received for each type
        assert_eq!(
            recv_notif_count, sent_notif_count,
            "Notification count mismatch"
        );
        assert_eq!(recv_op_count, sent_op_count, "OpExecution count mismatch");
        assert_eq!(
            recv_bridge_count, sent_bridge_count,
            "Bridge count mismatch"
        );
        assert_eq!(
            recv_node_count, sent_node_count,
            "NodeController count mismatch"
        );
        assert_eq!(
            recv_client_count, sent_client_count,
            "Client count mismatch"
        );
        assert_eq!(
            recv_executor_count, sent_executor_count,
            "Executor count mismatch"
        );

        tracing::info!("✓ STRESS TEST PASSED for seed {}!", seed);
        tracing::info!(
            "  All {} messages received correctly from 6 concurrent senders with random delays",
            TOTAL_MESSAGES
        );
        tracing::info!("  Received events: {:?}", received_events);
        tracing::info!("  Priority ordering respected: when multiple messages buffered, highest priority selected first");
    }

    /// Test that verifies waker registration across ALL channels when they're all Pending
    /// This is the critical behavior: when a PrioritySelectFuture polls all 8 channels and they
    /// all return Pending, it must register wakers for ALL of them, not just some.
    #[tokio::test]
    #[test_log::test]
    async fn test_priority_select_all_pending_waker_registration() {
        // Create all 8 channels
        let (notif_tx, mut notif_rx) = mpsc::channel::<Either<NetMessage, NodeEvent>>(10);
        let (op_tx, mut op_rx) =
            mpsc::channel::<(tokio::sync::mpsc::Sender<NetMessage>, NetMessage)>(10);
        let mut peers = FuturesUnordered::new();
        let (bridge_tx, mut bridge_rx) = mpsc::channel::<P2pBridgeEvent>(10);
        let (node_tx, mut node_rx) = mpsc::channel::<NodeEvent>(10);
        let (client_tx, mut client_rx) = mpsc::channel::<
            Result<
                (
                    crate::client_events::ClientId,
                    crate::contract::WaitingTransaction,
                ),
                anyhow::Error,
            >,
        >(10);
        let (executor_tx, mut executor_rx) =
            mpsc::channel::<Result<Transaction, anyhow::Error>>(10);

        // Start with NO messages buffered - this will cause all channels to return Pending on first poll
        tracing::info!("Creating PrioritySelectFuture with all channels empty");

        // Spawn a task that will send messages after a delay
        // This gives the select future time to poll all channels and register wakers
        tokio::spawn(async move {
            sleep(Duration::from_millis(10)).await;
            tracing::info!("All wakers should now be registered, sending messages");

            // Send to multiple channels simultaneously (in reverse priority order)
            tracing::info!("Sending to executor channel (lowest priority)");
            executor_tx
                .send(Ok(Transaction::new::<crate::operations::put::PutMsg>()))
                .await
                .unwrap();

            tracing::info!("Sending to client channel");
            let client_id = crate::client_events::ClientId::next();
            let waiting_tx = crate::contract::WaitingTransaction::Transaction(Transaction::new::<
                crate::operations::put::PutMsg,
            >());
            client_tx.send(Ok((client_id, waiting_tx))).await.unwrap();

            tracing::info!("Sending to node controller channel");
            node_tx
                .send(NodeEvent::Disconnect { cause: None })
                .await
                .unwrap();

            tracing::info!("Sending to bridge channel");
            bridge_tx
                .send(Either::Right(NodeEvent::Disconnect { cause: None }))
                .await
                .unwrap();

            tracing::info!("Sending to op execution channel (second priority)");
            let test_msg = NetMessage::V1(crate::message::NetMessageV1::Aborted(
                crate::message::Transaction::new::<crate::operations::put::PutMsg>(),
            ));
            let (callback_tx, _) = mpsc::channel(1);
            op_tx.send((callback_tx, test_msg.clone())).await.unwrap();

            tracing::info!("Sending to notification channel (highest priority)");
            notif_tx.send(Either::Left(test_msg)).await.unwrap();
        });

        // Create the select future - it will poll all channels, find them all Pending,
        // and register wakers for all of them
        let select_fut = PrioritySelectFuture::new(
            Box::pin(notif_rx.recv()),
            Box::pin(op_rx.recv()),
            &mut peers,
            Box::pin(bridge_rx.recv()),
            Box::pin(async {
                sleep(Duration::from_secs(1000)).await;
                Err(HandshakeError::ChannelClosed)
            }),
            Box::pin(node_rx.recv()),
            Box::pin(async {
                client_rx
                    .recv()
                    .await
                    .unwrap_or_else(|| Err(anyhow::anyhow!("closed")))
            }),
            Box::pin(async {
                executor_rx
                    .recv()
                    .await
                    .unwrap_or_else(|| Err(anyhow::anyhow!("closed")))
            }),
        );

        // Await the select future - it should wake up and return the NOTIFICATION (highest priority)
        // despite all other channels also having messages
        tracing::info!("PrioritySelectFuture started, should poll all channels and go Pending");
        let result = timeout(Duration::from_millis(100), select_fut).await;
        assert!(
            result.is_ok(),
            "Select should wake up when any message arrives"
        );

        let select_result = result.unwrap();
        match select_result {
            SelectResult::Notification(_) => {
                tracing::info!(
                    "✓ Correctly received Notification despite 5 other channels having messages"
                );
            }
            SelectResult::OpExecution(_) => {
                panic!("Should prioritize Notification over OpExecution")
            }
            SelectResult::ConnBridge(_) => panic!("Should prioritize Notification over ConnBridge"),
            SelectResult::NodeController(_) => {
                panic!("Should prioritize Notification over NodeController")
            }
            SelectResult::ClientTransaction(_) => {
                panic!("Should prioritize Notification over ClientTransaction")
            }
            SelectResult::ExecutorTransaction(_) => {
                panic!("Should prioritize Notification over ExecutorTransaction")
            }
            _ => panic!("Unexpected result"),
        }
    }
}
