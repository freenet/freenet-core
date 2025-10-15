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
/// Uses stack-allocated futures (generic types) to avoid heap allocations.
#[pin_project]
pub(super) struct PrioritySelectFuture<
    'a,
    NotifFut,
    OpExecFut,
    ConnBridgeFut,
    HandshakeFut,
    NodeCtrlFut,
    ClientTxFut,
    ExecTxFut,
> {
    #[pin]
    notification_fut: NotifFut,
    #[pin]
    op_execution_fut: OpExecFut,
    #[pin]
    peer_connections:
        &'a mut FuturesUnordered<BoxFuture<'static, Result<PeerConnectionInbound, TransportError>>>,
    #[pin]
    conn_bridge_fut: ConnBridgeFut,
    #[pin]
    handshake_fut: HandshakeFut,
    #[pin]
    node_controller_fut: NodeCtrlFut,
    #[pin]
    client_transaction_fut: ClientTxFut,
    #[pin]
    executor_transaction_fut: ExecTxFut,
    peer_connections_empty: bool,
}

impl<'a, NotifFut, OpExecFut, ConnBridgeFut, HandshakeFut, NodeCtrlFut, ClientTxFut, ExecTxFut>
    PrioritySelectFuture<
        'a,
        NotifFut,
        OpExecFut,
        ConnBridgeFut,
        HandshakeFut,
        NodeCtrlFut,
        ClientTxFut,
        ExecTxFut,
    >
{
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        notification_fut: NotifFut,
        op_execution_fut: OpExecFut,
        peer_connections: &'a mut FuturesUnordered<
            BoxFuture<'static, Result<PeerConnectionInbound, TransportError>>,
        >,
        conn_bridge_fut: ConnBridgeFut,
        handshake_fut: HandshakeFut,
        node_controller_fut: NodeCtrlFut,
        client_transaction_fut: ClientTxFut,
        executor_transaction_fut: ExecTxFut,
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

impl<'a, NotifFut, OpExecFut, ConnBridgeFut, HandshakeFut, NodeCtrlFut, ClientTxFut, ExecTxFut>
    Future
    for PrioritySelectFuture<
        'a,
        NotifFut,
        OpExecFut,
        ConnBridgeFut,
        HandshakeFut,
        NodeCtrlFut,
        ClientTxFut,
        ExecTxFut,
    >
where
    NotifFut: Future<Output = Option<Either<NetMessage, NodeEvent>>>,
    OpExecFut: Future<Output = Option<(tokio::sync::mpsc::Sender<NetMessage>, NetMessage)>>,
    ConnBridgeFut: Future<Output = Option<P2pBridgeEvent>>,
    HandshakeFut:
        Future<Output = Result<crate::node::network_bridge::handshake::Event, HandshakeError>>,
    NodeCtrlFut: Future<Output = Option<NodeEvent>>,
    ClientTxFut: Future<
        Output = Result<
            (
                crate::client_events::ClientId,
                crate::contract::WaitingTransaction,
            ),
            anyhow::Error,
        >,
    >,
    ExecTxFut: Future<Output = Result<Transaction, anyhow::Error>>,
{
    type Output = SelectResult;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let mut this = self.project();

        // Priority 1: Notification channel (highest priority)
        // This MUST be polled first to ensure operation state machine messages
        // are processed before network messages
        match this.notification_fut.as_mut().poll(cx) {
            Poll::Ready(msg) => {
                tracing::trace!("PrioritySelect: notification_rx ready");
                return Poll::Ready(SelectResult::Notification(msg));
            }
            Poll::Pending => {}
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
    // Create stack-allocated futures once - they will be reused across polls
    PrioritySelectFuture::new(
        notification_rx.recv(),
        op_execution_rx.recv(),
        peer_connections,
        conn_bridge_rx.recv(),
        handshake_handler.wait_for_events(),
        node_controller.recv(),
        client_wait_for_transaction.relay_transaction_result_to_client(),
        executor_listener.transaction_from_executor(),
    )
    .await
}
