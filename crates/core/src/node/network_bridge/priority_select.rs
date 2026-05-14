//! Custom select combinator that takes references to futures for explicit waker control.
//! This avoids waker registration issues that can occur with nested tokio::select! macros.

use either::Either;
use futures::Stream;
use std::collections::VecDeque;
use std::pin::Pin;
use std::task::{Context, Poll};
use tokio::sync::mpsc::Receiver;

use crate::client_events::ClientId;
use crate::contract::WaitingTransaction;

use super::OpExecutionPayload;
use super::p2p_protoc::ConnEvent;
use crate::contract::{ContractHandlerChannel, ExecutorTransactionStream, WaitingResolution};
use crate::dev_tool::Transaction;
use crate::message::{NetMessage, NodeEvent};
// Re-export P2pBridgeEvent from p2p_protoc
pub(crate) use super::p2p_protoc::P2pBridgeEvent;

#[allow(clippy::large_enum_variant)]
#[derive(Debug)]
pub(super) enum SelectResult {
    Notification(Option<Either<NetMessage, NodeEvent>>),
    OpExecution(Option<OpExecutionPayload>),
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
    ExecutorTransactionStream,
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
    op_execution: tokio_stream::wrappers::ReceiverStream<OpExecutionPayload>,
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
    /// When this reaches MAX_HIGH_PRIORITY_BURST, Tier-2 (P7-P8) and
    /// the Handshake channel are force-polled first to prevent starvation.
    high_priority_streak: u32,

    /// P7/P8 items batch-drained during a Phase 1 force-poll. Returned with
    /// absolute priority in Phase 0 so a backlog clears in O(N) rather than
    /// O(N × burst). See `poll_next` Phase 1 for the full rationale (#4056).
    p7_buffer: VecDeque<(ClientId, WaitingTransaction)>,
    p8_buffer: VecDeque<Transaction>,

    /// Closure of the P7/P8 channels detected during a Phase 1 drain that
    /// also collected items into the buffer. We defer emitting the
    /// `SelectResult::*(Err(channel closed))` notification until the
    /// buffer drains; otherwise the buffer return preempts the closure
    /// signal and the consumer never learns the channel closed (the
    /// `client_transaction_closed` / `executor_transaction_closed` flags
    /// keep us from re-polling, so the signal would be lost forever).
    p7_close_pending: bool,
    p8_close_pending: bool,
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
        op_execution_rx: Receiver<OpExecutionPayload>,
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
            p7_buffer: VecDeque::new(),
            p8_buffer: VecDeque::new(),
            p7_close_pending: false,
            p8_close_pending: false,
        }
    }

    /// Maximum consecutive unprotected (P1-P6) items before force-polling protected
    /// channels (Handshake, P7 Client tx, P8 Executor tx). Prevents starvation
    /// of connection lifecycle events and client/executor transactions under
    /// sustained high-priority traffic. See issues #3074, #3224.
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

        // Phase 0: Return P7/P8 items batch-drained by a previous Phase 1
        // force-poll. Absolute priority over Tier-1 — see Phase 1 below
        // (#4056). Yield to handshake first, though, so a long buffer drain
        // doesn't delay connection-lifecycle events past the bound added
        // in #3224.
        let buffer_has_items = !this.p7_buffer.is_empty() || !this.p8_buffer.is_empty();
        if buffer_has_items && !this.handshake_closed {
            match Pin::new(&mut this.handshake_handler).poll_next(cx) {
                Poll::Ready(Some(event)) => {
                    this.high_priority_streak = 0;
                    return Poll::Ready(Some(SelectResult::Handshake(Some(event))));
                }
                Poll::Ready(None) => {
                    this.handshake_closed = true;
                    // Don't preempt buffer drain with the closure signal;
                    // it'll be re-discovered by Phase 1/2 once the buffer
                    // empties.
                }
                Poll::Pending => {}
            }
        }
        if let Some(item) = this.p7_buffer.pop_front() {
            this.high_priority_streak = 0;
            return Poll::Ready(Some(SelectResult::ClientTransaction(Ok(item))));
        }
        if let Some(item) = this.p8_buffer.pop_front() {
            this.high_priority_streak = 0;
            return Poll::Ready(Some(SelectResult::ExecutorTransaction(Ok(item))));
        }
        // Buffer just drained; emit any closure signal Phase 1 deferred
        // so the consumer learns the channel closed exactly once.
        if this.p7_close_pending {
            this.p7_close_pending = false;
            return Poll::Ready(Some(SelectResult::ClientTransaction(Err(anyhow::anyhow!(
                "channel closed"
            )))));
        }
        if this.p8_close_pending {
            this.p8_close_pending = false;
            return Poll::Ready(Some(SelectResult::ExecutorTransaction(Err(
                anyhow::anyhow!("channel closed"),
            ))));
        }

        // Track if any channel closed (to report after checking all sources)
        let mut first_closed_channel: Option<SelectResult> = None;

        // Track whether protected channels were already polled in Phase 1
        // to avoid double-polling in Phase 2.
        let mut force_polled_in_phase1 = false;

        // Phase 1: Anti-starvation — force-poll Handshake and Tier-2 (P7/P8)
        // when the high-priority streak has reached the burst limit.
        // Handshake events are rare but critical for connection lifecycle;
        // without this protection, sustained notification traffic can
        // indefinitely starve new peer connections (see #3224).
        let force_low_priority = this.high_priority_streak >= Self::MAX_HIGH_PRIORITY_BURST;

        if force_low_priority {
            tracing::debug!(
                streak = this.high_priority_streak,
                "Anti-starvation: forcing poll of Handshake + Tier-2 channels"
            );
            force_polled_in_phase1 = true;
            crate::config::GlobalTestMetrics::record_anti_starvation_trigger();

            // Force-poll Handshake: connection lifecycle events
            if !this.handshake_closed {
                match Pin::new(&mut this.handshake_handler).poll_next(cx) {
                    Poll::Ready(Some(event)) => {
                        this.high_priority_streak = 0;
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

            // Force-poll P7 and P8: drain all currently-available items
            // into their buffers, then return one (P7 first). Subsequent
            // `poll_next` calls return the rest via Phase 0. Single-item
            // draining caused #4056: P7 backlogs took N × burst events to
            // clear under tier-1 flood.
            //
            // We drain BOTH channels before returning so a sustained P7
            // flood can't starve P8 — under the old single-poll approach,
            // P8 was reachable here only when P7 was empty in the same
            // call (and the same is true today inside Phase 2's strict
            // priority order).
            if !this.client_transaction_closed {
                loop {
                    match Pin::new(&mut this.client_transaction_handler).poll_next(cx) {
                        Poll::Ready(Some(result)) => {
                            this.p7_buffer.push_back(result);
                        }
                        Poll::Ready(None) => {
                            this.client_transaction_closed = true;
                            // If items were buffered before the closure, defer
                            // emitting the closure signal until Phase 0 has
                            // returned them; otherwise the buffer return would
                            // preempt and the closure would be lost forever.
                            if this.p7_buffer.is_empty() {
                                if first_closed_channel.is_none() {
                                    first_closed_channel = Some(SelectResult::ClientTransaction(
                                        Err(anyhow::anyhow!("channel closed")),
                                    ));
                                }
                            } else {
                                this.p7_close_pending = true;
                            }
                            break;
                        }
                        Poll::Pending => break,
                    }
                }
            }
            if !this.executor_transaction_closed {
                loop {
                    match Pin::new(&mut this.executor_transaction_handler).poll_next(cx) {
                        Poll::Ready(Some(tx)) => {
                            this.p8_buffer.push_back(tx);
                        }
                        Poll::Ready(None) => {
                            this.executor_transaction_closed = true;
                            if this.p8_buffer.is_empty() {
                                if first_closed_channel.is_none() {
                                    first_closed_channel = Some(SelectResult::ExecutorTransaction(
                                        Err(anyhow::anyhow!("channel closed")),
                                    ));
                                }
                            } else {
                                this.p8_close_pending = true;
                            }
                            break;
                        }
                        Poll::Pending => break,
                    }
                }
            }
            if let Some(result) = this.p7_buffer.pop_front() {
                this.high_priority_streak = 0;
                return Poll::Ready(Some(SelectResult::ClientTransaction(Ok(result))));
            }
            if let Some(tx) = this.p8_buffer.pop_front() {
                this.high_priority_streak = 0;
                return Poll::Ready(Some(SelectResult::ExecutorTransaction(Ok(tx))));
            }

            // Neither Handshake, P7, nor P8 yielded an item. Decide whether to reset streak:
            if this.handshake_closed
                && this.client_transaction_closed
                && this.executor_transaction_closed
            {
                // All protected channels are permanently closed — no more items
                // possible. Reset streak so we stop force-polling.
                this.high_priority_streak = 0;
            }
            // Otherwise, at least one channel is open but Pending. Keep
            // streak >= MAX so force-poll fires again on the very next
            // poll_next call — whether that returns a Tier-1 item
            // (incrementing streak past MAX) or re-enters Phase 1 directly.
        }

        // Phase 2: Normal priority polling
        //
        // Priority order rationale:
        //   P1 Notification    — operation state changes, highest volume
        //   P2 Handshake       — connection lifecycle (rare but blocks new peers)
        //   P3 Op execution    — operation message dispatch
        //   P4 Peer connection — established connection events
        //   P5 Conn bridge     — bridge commands
        //   P6 Node controller — node-level events
        //   P7 Client tx       — client API transactions (protected by anti-starvation)
        //   P8 Executor tx     — executor transactions (protected by anti-starvation)
        //
        // Handshake was promoted from P5→P2 to prevent notification traffic
        // from starving new peer connections on loaded gateways (#3224).
        // It is also protected by the Phase 1 anti-starvation mechanism
        // as a safety net (skip in Phase 2 if already polled in Phase 1).

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

        // Priority 2: Handshake handler — connection lifecycle events
        // Skip if already polled during Phase 1 force-poll
        if !force_polled_in_phase1 && !this.handshake_closed {
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

        // Priority 3: Op execution
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

        // Priority 4: Peer connection events
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

        // Priority 5: Connection bridge
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
        if !force_polled_in_phase1 && !this.client_transaction_closed {
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
        if !force_polled_in_phase1 && !this.executor_transaction_closed {
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
