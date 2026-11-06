//! How a connection's data reaches a path that carries it (#5940, e5b-6).
//!
//! The connection's task receives as well as sends (`peer_connection_listener`
//! runs both), so a send there must not wait for a path: a QUIC send can wait on
//! credit, flow control or an acknowledgement. A message of one packet goes
//! through [`DataRoute::queue_short`], which never waits: a full queue hands the
//! message back for UDP. One worker per path sends what is queued. Everything
//! larger runs as a task of its own (`peer_connection::quic_send`).
//!
//! The worker is not one of the attempt's tasks, which are aborted when the
//! attempt ends. When the connection closes, its sends fail at once, it closes
//! the queue, and what it holds goes on UDP by the table in `path::route`, so a
//! path that ends loses only the sends the receiver may already have. The route
//! gives no order between messages: separate uni streams can overtake each
//! other, and the operations layer already takes a stream before its metadata.

use std::future::Future;
#[cfg(any(test, feature = "testing"))]
use std::net::SocketAddr;
use std::pin::Pin;
use std::sync::Arc;

use futures::StreamExt;
use futures::stream::FuturesUnordered;
use tokio::sync::mpsc;

use super::driver::PathClock;
use crate::simulation::TimeSource;
use crate::transport::TransportError;
use crate::transport::connection_handler::SerializedMessage;
use crate::transport::quic::config::MAX_UNI_STREAMS;
use crate::transport::quic::path::{
    BoundedSend, NextStep, QuicPath, QuicSendEvent, SendDeadlines, SendKind,
};

/// Messages one path holds waiting for the worker. At about one packet each that
/// is at most about 300 KiB per path.
pub(in crate::transport) const SHORT_QUEUE_CAPACITY: usize = 256;

/// Sends the worker runs at once. Without a cap it would drain the queue into
/// futures and the queue would never fill. More than this cannot hold a stream.
const SHORTS_IN_FLIGHT: usize = MAX_UNI_STREAMS as usize;

/// Sends a message on the UDP session. A trait because a session is not generic
/// over the connection's socket and clock.
pub(in crate::transport) trait UdpShorts: Send + Sync {
    /// Send one message of one packet. Cancellation safe, and flight size is
    /// charged only for a packet the socket took.
    fn send_short(
        &self,
        payload: SerializedMessage,
    ) -> Pin<Box<dyn Future<Output = Result<(), TransportError>> + Send + '_>>;
}

/// Where a route records what its sends did, for a test. Nothing in production.
#[derive(Clone, Default)]
pub(in crate::transport) struct RouteNote {
    #[cfg(any(test, feature = "testing"))]
    trace: Option<(super::sim_trace::NodeTrace, SocketAddr, u64)>,
}

impl RouteNote {
    #[cfg(any(test, feature = "testing"))]
    pub(in crate::transport) fn new(
        trace: Option<super::sim_trace::NodeTrace>,
        remote: SocketAddr,
        session: u64,
    ) -> Self {
        Self {
            trace: trace.map(|trace| (trace, remote, session)),
        }
    }

    pub(in crate::transport) fn note(&self, what: impl FnOnce() -> String) {
        #[cfg(any(test, feature = "testing"))]
        if let Some((trace, remote, session)) = &self.trace {
            trace.record(*remote, *session, what());
        }
        #[cfg(not(any(test, feature = "testing")))]
        let _ = what;
    }
}

/// A path that carries data, with its short queue. Clones share both.
#[derive(Clone)]
pub(in crate::transport) struct DataRoute {
    path: Arc<QuicPath>,
    /// The clock quinn runs on. A send's deadlines are on it, not on the
    /// connection's clock, which differs in simulation.
    clock: PathClock,
    shorts: mpsc::Sender<SerializedMessage>,
    note: RouteNote,
}

impl DataRoute {
    /// A route over `path`, and the worker to spawn for it.
    pub(in crate::transport) fn new(
        path: Arc<QuicPath>,
        clock: PathClock,
        udp: Arc<dyn UdpShorts>,
        note: RouteNote,
    ) -> (Self, impl Future<Output = ()> + Send + 'static) {
        let (shorts, queue) = mpsc::channel(SHORT_QUEUE_CAPACITY);
        let worker = run_worker(Arc::clone(&path), clock.clone(), queue, udp, note.clone());
        (
            Self {
                path,
                clock,
                shorts,
                note,
            },
            worker,
        )
    }

    pub(in crate::transport) fn path(&self) -> &QuicPath {
        &self.path
    }

    /// The path, for a test that watches when it is let go.
    #[cfg(test)]
    pub(in crate::transport) fn path_handle(&self) -> Arc<QuicPath> {
        Arc::clone(&self.path)
    }

    /// Whether `other` is this route, sharing its queue.
    #[cfg(test)]
    pub(in crate::transport) fn is(&self, other: &Self) -> bool {
        self.shorts.same_channel(&other.shorts)
    }

    pub(in crate::transport) fn clock(&self) -> &PathClock {
        &self.clock
    }

    pub(in crate::transport) fn note(&self, what: impl FnOnce() -> String) {
        self.note.note(what);
    }

    /// Take `payload` for the path, or hand it back for UDP: the queue is full,
    /// or the path has ended. Never waits.
    pub(in crate::transport) fn queue_short(
        &self,
        payload: SerializedMessage,
    ) -> Result<(), SerializedMessage> {
        match self.shorts.try_send(payload) {
            Ok(()) => Ok(()),
            Err(mpsc::error::TrySendError::Full(payload)) => {
                self.path
                    .stats()
                    .record_send_event(QuicSendEvent::ShortOverflowUdp);
                Err(payload)
            }
            Err(mpsc::error::TrySendError::Closed(payload)) => Err(payload),
        }
    }
}

type ShortSend = Pin<Box<dyn Future<Output = (SerializedMessage, BoundedSend)> + Send>>;

/// Send what the queue takes until the path has ended and nothing is left.
/// A task, and no reference to the connection's receive state.
async fn run_worker(
    path: Arc<QuicPath>,
    clock: PathClock,
    mut queue: mpsc::Receiver<SerializedMessage>,
    udp: Arc<dyn UdpShorts>,
    note: RouteNote,
) {
    let connection = path.quinn_connection();
    let mut in_flight = FuturesUnordered::<ShortSend>::new();
    let mut queue_open = true;
    let mut connected = true;
    while queue_open || !in_flight.is_empty() {
        crate::deterministic_select! {
            _ = async {
                if connected {
                    connection.closed().await;
                } else {
                    std::future::pending::<()>().await;
                }
            } => {
                // From here a new message goes on UDP from the connection's task,
                // and `recv` yields what is left, then the end.
                connected = false;
                queue.close();
            },
            settled = in_flight.next(), if !in_flight.is_empty() => {
                if let Some((payload, outcome)) = settled {
                    note.note(|| format!("sent a short on the path: {outcome:?}"));
                    let step = path.settle(SendKind::Short, &outcome, false, clock.now_nanos());
                    if step == NextStep::SendOnUdp {
                        send_on_udp(&path, udp.as_ref(), &note, payload).await;
                    }
                }
            },
            message = queue.recv(), if queue_open && in_flight.len() < SHORTS_IN_FLIGHT => {
                match message {
                    Some(payload) if connected && path.sends_data() && !path.is_suspect() => {
                        in_flight.push(send_on_path(&path, &clock, payload));
                    }
                    Some(payload) => send_on_udp(&path, udp.as_ref(), &note, payload).await,
                    None => queue_open = false,
                }
            },
        }
    }
}

fn send_on_path(path: &Arc<QuicPath>, clock: &PathClock, payload: SerializedMessage) -> ShortSend {
    let path = Arc::clone(path);
    let clock = clock.clone();
    Box::pin(async move {
        let deadlines = SendDeadlines::short(payload.len() as u64);
        let outcome = path.send_short_bounded(&clock, deadlines, &payload).await;
        (payload, outcome)
    })
}

/// Hand a message to the UDP session, once. A send error there costs what a lost
/// datagram costs a short message on UDP: `outbound_short_message` retries nothing
/// either.
async fn send_on_udp(
    path: &QuicPath,
    udp: &dyn UdpShorts,
    note: &RouteNote,
    payload: SerializedMessage,
) {
    match udp.send_short(payload).await {
        Ok(()) => note.note(|| "sent a short on UDP from the path's queue".to_string()),
        Err(error) => {
            path.stats().record_send_event(QuicSendEvent::UdpSendFailed);
            tracing::debug!(%error, "A short message from a QUIC path was not sent on UDP");
        }
    }
}
