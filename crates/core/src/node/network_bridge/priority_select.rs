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
// Re-export P2pBridgeEvent from p2p_protoc
pub(crate) use super::p2p_protoc::P2pBridgeEvent;

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
    C: Stream<Item = (ClientId, WaitingTransaction)> + Unpin,
    E: Stream<Item = Transaction> + Unpin,
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

    // Client transaction handler - implements Stream directly
    client_transaction_handler: C,

    // Executor transaction handler - implements Stream directly
    executor_transaction_handler: E,

    // Track which channels have been reported as closed (to avoid infinite loop of closure notifications)
    notification_closed: bool,
    op_execution_closed: bool,
    conn_bridge_closed: bool,
    node_controller_closed: bool,
    conn_events_closed: bool,
    handshake_closed: bool,
    client_transaction_closed: bool,
    executor_transaction_closed: bool,

    /// Counts consecutive items returned from Tier-1 (P1-P6) channels.
    /// When this reaches MAX_HIGH_PRIORITY_BURST, Tier-2 (P7-P8) channels
    /// are force-polled first to prevent starvation.
    high_priority_streak: u32,
}

impl<H, C, E> PrioritySelectStream<H, C, E>
where
    H: Stream<Item = crate::node::network_bridge::handshake::Event> + Unpin,
    C: Stream<Item = (ClientId, WaitingTransaction)> + Unpin,
    E: Stream<Item = Transaction> + Unpin,
{
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        notification_rx: Receiver<Either<NetMessage, NodeEvent>>,
        op_execution_rx: Receiver<(tokio::sync::mpsc::Sender<NetMessage>, NetMessage)>,
        conn_bridge_rx: Receiver<P2pBridgeEvent>,
        handshake_handler: H,
        node_controller: Receiver<NodeEvent>,
        client_transaction_handler: C,
        executor_transaction_handler: E,
        conn_events: Receiver<ConnEvent>,
    ) -> Self {
        use tokio_stream::wrappers::ReceiverStream;

        Self {
            notification: ReceiverStream::new(notification_rx),
            op_execution: ReceiverStream::new(op_execution_rx),
            conn_bridge: ReceiverStream::new(conn_bridge_rx),
            node_controller: ReceiverStream::new(node_controller),
            conn_events: ReceiverStream::new(conn_events),
            handshake_handler,
            client_transaction_handler,
            executor_transaction_handler,
            notification_closed: false,
            op_execution_closed: false,
            conn_bridge_closed: false,
            node_controller_closed: false,
            conn_events_closed: false,
            handshake_closed: false,
            client_transaction_closed: false,
            executor_transaction_closed: false,
            high_priority_streak: 0,
        }
    }

    /// Maximum consecutive Tier-1 (P1-P6) items before force-polling Tier-2 (P7-P8).
    /// Prevents starvation of client/executor transaction channels under sustained
    /// high-priority traffic. See issue #3074.
    pub(crate) const MAX_HIGH_PRIORITY_BURST: u32 = 32;
}

impl<H, C, E> Stream for PrioritySelectStream<H, C, E>
where
    H: Stream<Item = crate::node::network_bridge::handshake::Event> + Unpin,
    C: Stream<Item = (ClientId, WaitingTransaction)> + Unpin,
    E: Stream<Item = Transaction> + Unpin,
{
    type Item = SelectResult;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();

        // Track if any channel closed (to report after checking all sources)
        let mut first_closed_channel: Option<SelectResult> = None;

        // Track whether Tier-2 channels were already polled in Phase 1
        // to avoid double-polling in Phase 2.
        let mut tier2_polled_in_phase1 = false;

        // Phase 1: Anti-starvation — force-poll Tier-2 (P7/P8) when the
        // high-priority streak has reached the burst limit.
        let force_low_priority = this.high_priority_streak >= Self::MAX_HIGH_PRIORITY_BURST;

        if force_low_priority {
            tracing::debug!(
                streak = this.high_priority_streak,
                "Anti-starvation: forcing poll of Tier-2 channels"
            );
            tier2_polled_in_phase1 = true;

            // Force-poll P7: Client transaction handler
            if !this.client_transaction_closed {
                match Pin::new(&mut this.client_transaction_handler).poll_next(cx) {
                    Poll::Ready(Some(result)) => {
                        this.high_priority_streak = 0;
                        return Poll::Ready(Some(SelectResult::ClientTransaction(Ok(result))));
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

            // Force-poll P8: Executor transaction handler
            if !this.executor_transaction_closed {
                match Pin::new(&mut this.executor_transaction_handler).poll_next(cx) {
                    Poll::Ready(Some(tx)) => {
                        this.high_priority_streak = 0;
                        return Poll::Ready(Some(SelectResult::ExecutorTransaction(Ok(tx))));
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

            // Neither P7 nor P8 yielded an item. Decide whether to reset streak:
            if this.client_transaction_closed && this.executor_transaction_closed {
                // Both channels are permanently closed — no more Tier-2 items
                // possible. Reset streak so we stop force-polling.
                this.high_priority_streak = 0;
            }
            // Otherwise, at least one channel is open but Pending. Keep
            // streak >= MAX so force-poll fires again on the very next
            // poll_next call — whether that returns a Tier-1 item
            // (incrementing streak past MAX) or re-enters Phase 1 directly.
        }

        // Phase 2: Normal priority polling (P1-P6, then P7-P8)

        // Priority 1: Notification channel (highest priority)
        if !this.notification_closed {
            match Pin::new(&mut this.notification).poll_next(cx) {
                Poll::Ready(Some(msg)) => {
                    this.high_priority_streak += 1;
                    return Poll::Ready(Some(SelectResult::Notification(Some(msg))));
                }
                Poll::Ready(None) => {
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
                    this.high_priority_streak += 1;
                    return Poll::Ready(Some(SelectResult::OpExecution(Some(msg))));
                }
                Poll::Ready(None) => {
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
                    this.high_priority_streak += 1;
                    return Poll::Ready(Some(SelectResult::PeerConnection(Some(event))));
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
                    this.high_priority_streak += 1;
                    return Poll::Ready(Some(SelectResult::ConnBridge(Some(msg))));
                }
                Poll::Ready(None) => {
                    this.conn_bridge_closed = true;
                    if first_closed_channel.is_none() {
                        first_closed_channel = Some(SelectResult::ConnBridge(None));
                    }
                }
                Poll::Pending => {}
            }
        }

        // Priority 5: Handshake handler (implements Stream directly)
        if !this.handshake_closed {
            match Pin::new(&mut this.handshake_handler).poll_next(cx) {
                Poll::Ready(Some(event)) => {
                    this.high_priority_streak += 1;
                    return Poll::Ready(Some(SelectResult::Handshake(Some(event))));
                }
                Poll::Ready(None) => {
                    this.handshake_closed = true;
                    if first_closed_channel.is_none() {
                        first_closed_channel = Some(SelectResult::Handshake(None));
                    }
                }
                Poll::Pending => {}
            }
        }

        // Priority 6: Node controller
        if !this.node_controller_closed {
            match Pin::new(&mut this.node_controller).poll_next(cx) {
                Poll::Ready(Some(msg)) => {
                    this.high_priority_streak += 1;
                    return Poll::Ready(Some(SelectResult::NodeController(Some(msg))));
                }
                Poll::Ready(None) => {
                    this.node_controller_closed = true;
                    if first_closed_channel.is_none() {
                        first_closed_channel = Some(SelectResult::NodeController(None));
                    }
                }
                Poll::Pending => {}
            }
        }

        // Priority 7: Client transaction handler
        // Skip if already polled during Phase 1 force-poll
        if !tier2_polled_in_phase1 && !this.client_transaction_closed {
            match Pin::new(&mut this.client_transaction_handler).poll_next(cx) {
                Poll::Ready(Some(result)) => {
                    this.high_priority_streak = 0;
                    return Poll::Ready(Some(SelectResult::ClientTransaction(Ok(result))));
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

        // Priority 8: Executor transaction handler
        // Skip if already polled during Phase 1 force-poll
        if !tier2_polled_in_phase1 && !this.executor_transaction_closed {
            match Pin::new(&mut this.executor_transaction_handler).poll_next(cx) {
                Poll::Ready(Some(tx)) => {
                    this.high_priority_streak = 0;
                    return Poll::Ready(Some(SelectResult::ExecutorTransaction(Ok(tx))));
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
