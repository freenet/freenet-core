//! Custom select combinator that takes references to futures for explicit waker control.
//! This avoids waker registration issues that can occur with nested tokio::select! macros.

use either::Either;
use futures::future::BoxFuture;
use futures::stream::{FuturesUnordered, Stream};
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

/// A future that polls multiple futures with explicit priority order and waker control
pub(super) struct PrioritySelectFuture<'a> {
    notification_rx: &'a mut Receiver<Either<NetMessage, NodeEvent>>,
    op_execution_rx: &'a mut Receiver<(tokio::sync::mpsc::Sender<NetMessage>, NetMessage)>,
    peer_connections:
        &'a mut FuturesUnordered<BoxFuture<'static, Result<PeerConnectionInbound, TransportError>>>,
    conn_bridge_rx: &'a mut Receiver<P2pBridgeEvent>,
    handshake_handler: &'a mut HandshakeHandler,
    node_controller: &'a mut Receiver<NodeEvent>,
    client_wait_for_transaction: &'a mut ContractHandlerChannel<WaitingResolution>,
    executor_listener: &'a mut ExecutorToEventLoopChannel<NetworkEventListenerHalve>,
    peer_connections_empty: bool,
}

impl<'a> PrioritySelectFuture<'a> {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
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
    ) -> Self {
        let peer_connections_empty = peer_connections.is_empty();

        Self {
            notification_rx,
            op_execution_rx,
            peer_connections,
            conn_bridge_rx,
            handshake_handler,
            node_controller,
            client_wait_for_transaction,
            executor_listener,
            peer_connections_empty,
        }
    }
}

impl<'a> Future for PrioritySelectFuture<'a> {
    type Output = SelectResult;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        // Priority 1: Notification channel (highest priority)
        // This MUST be polled first to ensure operation state machine messages
        // are processed before network messages
        {
            let mut notif_fut = Box::pin(self.notification_rx.recv());
            match notif_fut.as_mut().poll(cx) {
                Poll::Ready(msg) => {
                    tracing::trace!("PrioritySelect: notification_rx ready");
                    return Poll::Ready(SelectResult::Notification(msg));
                }
                Poll::Pending => {}
            }
        }

        // Priority 2: Op execution channel
        {
            let mut op_exec_fut = Box::pin(self.op_execution_rx.recv());
            match op_exec_fut.as_mut().poll(cx) {
                Poll::Ready(msg) => {
                    tracing::trace!("PrioritySelect: op_execution_rx ready");
                    return Poll::Ready(SelectResult::OpExecution(msg));
                }
                Poll::Pending => {}
            }
        }

        // Priority 3: Peer connections (only if not empty)
        if !self.peer_connections_empty {
            match Stream::poll_next(Pin::new(&mut *self.peer_connections), cx) {
                Poll::Ready(msg) => {
                    tracing::trace!("PrioritySelect: peer_connections ready");
                    return Poll::Ready(SelectResult::PeerConnection(msg));
                }
                Poll::Pending => {}
            }
        }

        // Priority 4: Connection bridge
        {
            let mut bridge_fut = Box::pin(self.conn_bridge_rx.recv());
            match bridge_fut.as_mut().poll(cx) {
                Poll::Ready(msg) => {
                    tracing::trace!("PrioritySelect: conn_bridge_rx ready");
                    return Poll::Ready(SelectResult::ConnBridge(msg));
                }
                Poll::Pending => {}
            }
        }

        // Priority 5: Handshake handler (poll wait_for_events as a whole to preserve all logic)
        // We poll this as a boxed future to avoid nested select! waker registration issues
        // while still keeping all the handshake event handling logic intact
        {
            let mut handshake_fut = Box::pin(self.handshake_handler.wait_for_events());
            match handshake_fut.as_mut().poll(cx) {
                Poll::Ready(result) => {
                    tracing::trace!("PrioritySelect: handshake_handler ready");
                    return Poll::Ready(SelectResult::Handshake(result));
                }
                Poll::Pending => {}
            }
        }

        // Priority 8: Node controller
        {
            let mut controller_fut = Box::pin(self.node_controller.recv());
            match controller_fut.as_mut().poll(cx) {
                Poll::Ready(msg) => {
                    tracing::trace!("PrioritySelect: node_controller ready");
                    return Poll::Ready(SelectResult::NodeController(msg));
                }
                Poll::Pending => {}
            }
        }

        // Priority 9: Client transaction waiting
        {
            let mut client_fut = Box::pin(
                self.client_wait_for_transaction
                    .relay_transaction_result_to_client(),
            );
            match client_fut.as_mut().poll(cx) {
                Poll::Ready(event_id) => {
                    tracing::trace!("PrioritySelect: client_wait_for_transaction ready");
                    return Poll::Ready(SelectResult::ClientTransaction(event_id));
                }
                Poll::Pending => {}
            }
        }

        // Priority 10: Executor transaction
        {
            let mut executor_fut = Box::pin(self.executor_listener.transaction_from_executor());
            match executor_fut.as_mut().poll(cx) {
                Poll::Ready(id) => {
                    tracing::trace!("PrioritySelect: executor_listener ready");
                    return Poll::Ready(SelectResult::ExecutorTransaction(id));
                }
                Poll::Pending => {}
            }
        }

        // All futures returned Pending - wakers are now registered for all of them
        // This is the key: we registered wakers for ALL futures in a single poll call
        // No nested select! means no waker confusion
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
    PrioritySelectFuture::new(
        notification_rx,
        op_execution_rx,
        peer_connections,
        conn_bridge_rx,
        handshake_handler,
        node_controller,
        client_wait_for_transaction,
        executor_listener,
    )
    .await
}
