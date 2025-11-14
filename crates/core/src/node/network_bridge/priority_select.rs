//! Custom select combinator that takes references to futures for explicit waker control.
//! This avoids waker registration issues that can occur with nested tokio::select! macros.

use either::Either;
use futures::{future::BoxFuture, stream::FuturesUnordered, Stream};
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
use crate::transport::TransportError;

// P2pBridgeEvent type alias for the event bridge channel
pub type P2pBridgeEvent = Either<(PeerId, Box<NetMessage>), NodeEvent>;

#[allow(clippy::large_enum_variant)]
#[derive(Debug)]
pub(super) enum SelectResult {
    Notification(Option<Either<NetMessage, NodeEvent>>),
    OpExecution(Option<(tokio::sync::mpsc::Sender<NetMessage>, NetMessage)>),
    PeerConnection(Option<Result<PeerConnectionInbound, TransportError>>),
    ConnBridge(Option<P2pBridgeEvent>),
    Handshake(Option<crate::node::network_bridge::handshake::Event>),
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

/// Trait for types that can relay client transaction results
pub(super) trait ClientTransactionRelay: Send + Unpin {
    fn relay_transaction_result_to_client(
        &mut self,
    ) -> impl Future<
        Output = Result<
            (
                crate::client_events::ClientId,
                crate::contract::WaitingTransaction,
            ),
            anyhow::Error,
        >,
    > + Send;
}

/// Trait for types that can receive transactions from executor
pub(super) trait ExecutorTransactionReceiver: Send + Unpin {
    fn transaction_from_executor(
        &mut self,
    ) -> impl Future<Output = anyhow::Result<Transaction>> + Send;
}

impl ClientTransactionRelay for ContractHandlerChannel<WaitingResolution> {
    fn relay_transaction_result_to_client(
        &mut self,
    ) -> impl Future<
        Output = Result<
            (
                crate::client_events::ClientId,
                crate::contract::WaitingTransaction,
            ),
            anyhow::Error,
        >,
    > + Send {
        self.relay_transaction_result_to_client()
    }
}

impl ExecutorTransactionReceiver for ExecutorToEventLoopChannel<NetworkEventListenerHalve> {
    fn transaction_from_executor(
        &mut self,
    ) -> impl Future<Output = anyhow::Result<Transaction>> + Send {
        self.transaction_from_executor()
    }
}

/// Type alias for the production PrioritySelectStream with concrete types
pub(super) type ProductionPrioritySelectStream = PrioritySelectStream<
    super::handshake::HandshakeHandler,
    ContractHandlerChannel<WaitingResolution>,
    ExecutorToEventLoopChannel<NetworkEventListenerHalve>,
>;

/// Generic stream-based priority select that owns simple Receivers as streams
/// and holds references to complex event sources.
/// This fixes the lost wakeup race condition (issue #1932) by keeping the stream
/// alive across loop iterations, maintaining waker registration.
pub(super) struct PrioritySelectStream<H, C, E>
where
    H: Stream<Item = crate::node::network_bridge::handshake::Event> + Unpin,
    C: ClientTransactionRelay,
    E: ExecutorTransactionReceiver,
{
    // Streams created from owned receivers
    notification: tokio_stream::wrappers::ReceiverStream<Either<NetMessage, NodeEvent>>,
    op_execution:
        tokio_stream::wrappers::ReceiverStream<(tokio::sync::mpsc::Sender<NetMessage>, NetMessage)>,
    conn_bridge: tokio_stream::wrappers::ReceiverStream<P2pBridgeEvent>,
    node_controller: tokio_stream::wrappers::ReceiverStream<NodeEvent>,

    // FuturesUnordered already implements Stream (owned)
    peer_connections:
        FuturesUnordered<BoxFuture<'static, Result<PeerConnectionInbound, TransportError>>>,

    // HandshakeHandler now implements Stream directly - maintains state across polls
    // Generic to allow testing with mocks
    handshake_handler: H,

    // These two are owned and we create futures from them that poll their internal state
    // Generic to allow testing with mocks
    client_wait_for_transaction: C,
    executor_listener: E,

    // Track which channels have been reported as closed (to avoid infinite loop of closure notifications)
    notification_closed: bool,
    op_execution_closed: bool,
    conn_bridge_closed: bool,
    node_controller_closed: bool,
}

impl<H, C, E> PrioritySelectStream<H, C, E>
where
    H: Stream<Item = crate::node::network_bridge::handshake::Event> + Unpin,
    C: ClientTransactionRelay,
    E: ExecutorTransactionReceiver,
{
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        notification_rx: Receiver<Either<NetMessage, NodeEvent>>,
        op_execution_rx: Receiver<(tokio::sync::mpsc::Sender<NetMessage>, NetMessage)>,
        conn_bridge_rx: Receiver<P2pBridgeEvent>,
        handshake_handler: H,
        node_controller: Receiver<NodeEvent>,
        client_wait_for_transaction: C,
        executor_listener: E,
        peer_connections: FuturesUnordered<
            BoxFuture<'static, Result<PeerConnectionInbound, TransportError>>,
        >,
    ) -> Self {
        use tokio_stream::wrappers::ReceiverStream;

        Self {
            notification: ReceiverStream::new(notification_rx),
            op_execution: ReceiverStream::new(op_execution_rx),
            conn_bridge: ReceiverStream::new(conn_bridge_rx),
            node_controller: ReceiverStream::new(node_controller),
            peer_connections,
            handshake_handler,
            client_wait_for_transaction,
            executor_listener,
            notification_closed: false,
            op_execution_closed: false,
            conn_bridge_closed: false,
            node_controller_closed: false,
        }
    }

    /// Add a new peer connection task to the stream
    pub fn push_peer_connection(
        &mut self,
        task: BoxFuture<'static, Result<PeerConnectionInbound, TransportError>>,
    ) {
        self.peer_connections.push(task);
    }
}

impl<H, C, E> Stream for PrioritySelectStream<H, C, E>
where
    H: Stream<Item = crate::node::network_bridge::handshake::Event> + Unpin,
    C: ClientTransactionRelay,
    E: ExecutorTransactionReceiver,
{
    type Item = SelectResult;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();

        // Track if any channel closed (to report after checking all sources)
        let mut first_closed_channel: Option<SelectResult> = None;

        // Priority 1: Notification channel (highest priority)
        if !this.notification_closed {
            match Pin::new(&mut this.notification).poll_next(cx) {
                Poll::Ready(Some(msg)) => {
                    return Poll::Ready(Some(SelectResult::Notification(Some(msg))))
                }
                Poll::Ready(None) => {
                    // Channel closed - record it and mark as closed to avoid re-polling
                    this.notification_closed = true;
                    if first_closed_channel.is_none() {
                        first_closed_channel = Some(SelectResult::Notification(None));
                    }
                }
                Poll::Pending => {}
            }
        }

        // Priority 2: Op execution
        if !this.op_execution_closed {
            match Pin::new(&mut this.op_execution).poll_next(cx) {
                Poll::Ready(Some(msg)) => {
                    return Poll::Ready(Some(SelectResult::OpExecution(Some(msg))))
                }
                Poll::Ready(None) => {
                    // Channel closed - record it and mark as closed to avoid re-polling
                    this.op_execution_closed = true;
                    if first_closed_channel.is_none() {
                        first_closed_channel = Some(SelectResult::OpExecution(None));
                    }
                }
                Poll::Pending => {}
            }
        }

        // Priority 3: Peer connections (only if not empty)
        if !this.peer_connections.is_empty() {
            match Pin::new(&mut this.peer_connections).poll_next(cx) {
                Poll::Ready(msg) => return Poll::Ready(Some(SelectResult::PeerConnection(msg))),
                Poll::Pending => {}
            }
        }

        // Priority 4: Connection bridge
        if !this.conn_bridge_closed {
            match Pin::new(&mut this.conn_bridge).poll_next(cx) {
                Poll::Ready(Some(msg)) => {
                    return Poll::Ready(Some(SelectResult::ConnBridge(Some(msg))))
                }
                Poll::Ready(None) => {
                    // Channel closed - record it and mark as closed to avoid re-polling
                    this.conn_bridge_closed = true;
                    if first_closed_channel.is_none() {
                        first_closed_channel = Some(SelectResult::ConnBridge(None));
                    }
                }
                Poll::Pending => {}
            }
        }

        // Priority 5: Handshake handler (now implements Stream)
        // Poll the handshake handler stream - it maintains state across polls
        match Pin::new(&mut this.handshake_handler).poll_next(cx) {
            Poll::Ready(Some(event)) => {
                return Poll::Ready(Some(SelectResult::Handshake(Some(event))))
            }
            Poll::Ready(None) => {
                if first_closed_channel.is_none() {
                    first_closed_channel = Some(SelectResult::Handshake(None));
                }
            }
            Poll::Pending => {}
        }

        // Priority 6: Node controller
        if !this.node_controller_closed {
            match Pin::new(&mut this.node_controller).poll_next(cx) {
                Poll::Ready(Some(msg)) => {
                    return Poll::Ready(Some(SelectResult::NodeController(Some(msg))))
                }
                Poll::Ready(None) => {
                    // Channel closed - record it and mark as closed to avoid re-polling
                    this.node_controller_closed = true;
                    if first_closed_channel.is_none() {
                        first_closed_channel = Some(SelectResult::NodeController(None));
                    }
                }
                Poll::Pending => {}
            }
        }

        // Priority 7: Client transaction
        let client_fut = this
            .client_wait_for_transaction
            .relay_transaction_result_to_client();
        tokio::pin!(client_fut);
        match client_fut.poll(cx) {
            Poll::Ready(result) => {
                return Poll::Ready(Some(SelectResult::ClientTransaction(result)))
            }
            Poll::Pending => {}
        }

        // Priority 8: Executor transaction
        let executor_fut = this.executor_listener.transaction_from_executor();
        tokio::pin!(executor_fut);
        match executor_fut.poll(cx) {
            Poll::Ready(result) => {
                return Poll::Ready(Some(SelectResult::ExecutorTransaction(result)))
            }
            Poll::Pending => {}
        }

        // If a channel closed and nothing else is ready, report the closure
        if let Some(closed) = first_closed_channel {
            return Poll::Ready(Some(closed));
        }

        // All pending
        Poll::Pending
    }
}

#[cfg(test)]
mod tests;
