//! Custom select combinator that takes references to futures for explicit waker control.
//! This avoids waker registration issues that can occur with nested tokio::select! macros.

use either::Either;
use futures::Stream;
use std::pin::Pin;
use std::task::{Context, Poll};
use tokio::sync::mpsc::Receiver;

use crate::client_events::ClientId;
use crate::contract::WaitingTransaction;

use super::p2p_protoc::ConnEvent;
use crate::contract::{
    ContractHandlerChannel, ExecutorToEventLoopChannel, NetworkEventListenerHalve,
    WaitingResolution,
};
use crate::dev_tool::Transaction;
use crate::message::{NetMessage, NodeEvent};
use crate::ring::PeerKeyLocation;

// P2pBridgeEvent type alias for the event bridge channel
pub type P2pBridgeEvent = Either<(PeerKeyLocation, Box<NetMessage>), NodeEvent>;

#[allow(clippy::large_enum_variant)]
#[derive(Debug)]
pub(super) enum SelectResult {
    Notification(Option<Either<NetMessage, NodeEvent>>),
    OpExecution(Option<(tokio::sync::mpsc::Sender<NetMessage>, NetMessage)>),
    PeerConnection(Option<ConnEvent>),
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

/// Trait for types that can relay client transaction results via a receiver
pub(super) trait ClientTransactionRelay: Send + Unpin {
    /// Take the underlying receiver for streaming
    fn take_receiver(&mut self) -> Option<Receiver<(ClientId, WaitingTransaction)>>;
}

/// Trait for types that can receive transactions from executor via a receiver
pub(super) trait ExecutorTransactionReceiver: Send + Unpin {
    /// Take the underlying receiver for streaming
    fn take_receiver(&mut self) -> Option<Receiver<Transaction>>;
}

impl ClientTransactionRelay for ContractHandlerChannel<WaitingResolution> {
    fn take_receiver(&mut self) -> Option<Receiver<(ClientId, WaitingTransaction)>> {
        self.take_wait_for_res_receiver()
    }
}

impl ExecutorTransactionReceiver for ExecutorToEventLoopChannel<NetworkEventListenerHalve> {
    fn take_receiver(&mut self) -> Option<Receiver<Transaction>> {
        self.take_waiting_for_op_receiver()
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
    conn_events: tokio_stream::wrappers::ReceiverStream<ConnEvent>,

    // HandshakeHandler now implements Stream directly - maintains state across polls
    // Generic to allow testing with mocks
    handshake_handler: H,

    // Client transaction stream - taken from the handler at construction time
    // to avoid recreating futures on each poll (fixes waker registration issue)
    client_transaction_stream:
        Option<tokio_stream::wrappers::ReceiverStream<(ClientId, WaitingTransaction)>>,

    // Executor transaction stream - taken from the channel at construction time
    // to avoid recreating futures on each poll (fixes waker registration issue)
    executor_transaction_stream: Option<tokio_stream::wrappers::ReceiverStream<Transaction>>,

    // Keep the original types for callback() and other methods that need them
    #[allow(dead_code)]
    client_wait_for_transaction: C,
    #[allow(dead_code)]
    executor_listener: E,

    // Track which channels have been reported as closed (to avoid infinite loop of closure notifications)
    notification_closed: bool,
    op_execution_closed: bool,
    conn_bridge_closed: bool,
    node_controller_closed: bool,
    conn_events_closed: bool,
    client_transaction_closed: bool,
    executor_transaction_closed: bool,
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
        mut client_wait_for_transaction: C,
        mut executor_listener: E,
        conn_events: Receiver<ConnEvent>,
    ) -> Self {
        use tokio_stream::wrappers::ReceiverStream;

        // Take the receivers from the handlers and convert to streams.
        // This fixes the waker registration issue - streams maintain their wakers
        // properly across polls, unlike fresh futures created on each poll.
        let client_transaction_stream = client_wait_for_transaction
            .take_receiver()
            .map(ReceiverStream::new);
        let executor_transaction_stream =
            executor_listener.take_receiver().map(ReceiverStream::new);

        Self {
            notification: ReceiverStream::new(notification_rx),
            op_execution: ReceiverStream::new(op_execution_rx),
            conn_bridge: ReceiverStream::new(conn_bridge_rx),
            node_controller: ReceiverStream::new(node_controller),
            conn_events: ReceiverStream::new(conn_events),
            handshake_handler,
            client_transaction_stream,
            executor_transaction_stream,
            client_wait_for_transaction,
            executor_listener,
            notification_closed: false,
            op_execution_closed: false,
            conn_bridge_closed: false,
            node_controller_closed: false,
            conn_events_closed: false,
            client_transaction_closed: false,
            executor_transaction_closed: false,
        }
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

        // Priority 3: Peer connection events
        if !this.conn_events_closed {
            match Pin::new(&mut this.conn_events).poll_next(cx) {
                Poll::Ready(Some(event)) => {
                    return Poll::Ready(Some(SelectResult::PeerConnection(Some(event))))
                }
                Poll::Ready(None) => {
                    this.conn_events_closed = true;
                    if first_closed_channel.is_none() {
                        first_closed_channel = Some(SelectResult::PeerConnection(None));
                    }
                }
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

        // Priority 7: Client transaction (now using stream instead of recreating futures)
        if !this.client_transaction_closed {
            if let Some(ref mut stream) = this.client_transaction_stream {
                match Pin::new(stream).poll_next(cx) {
                    Poll::Ready(Some(result)) => {
                        return Poll::Ready(Some(SelectResult::ClientTransaction(Ok(result))))
                    }
                    Poll::Ready(None) => {
                        this.client_transaction_closed = true;
                        if first_closed_channel.is_none() {
                            first_closed_channel = Some(SelectResult::ClientTransaction(Err(
                                anyhow::anyhow!("channel closed"),
                            )));
                        }
                    }
                    Poll::Pending => {}
                }
            }
        }

        // Priority 8: Executor transaction (now using stream instead of recreating futures)
        if !this.executor_transaction_closed {
            if let Some(ref mut stream) = this.executor_transaction_stream {
                match Pin::new(stream).poll_next(cx) {
                    Poll::Ready(Some(tx)) => {
                        return Poll::Ready(Some(SelectResult::ExecutorTransaction(Ok(tx))))
                    }
                    Poll::Ready(None) => {
                        this.executor_transaction_closed = true;
                        if first_closed_channel.is_none() {
                            first_closed_channel = Some(SelectResult::ExecutorTransaction(Err(
                                anyhow::anyhow!("channel closed"),
                            )));
                        }
                    }
                    Poll::Pending => {}
                }
            }
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
