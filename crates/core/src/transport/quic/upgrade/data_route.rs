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
use crate::transport::metrics::ShortDropCause;
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
        Self::with_fallback(path, clock, Some(udp), note)
    }

    /// A route over a native connection's path, which has no UDP session: a
    /// short the path cannot send is dropped and counted, never sent elsewhere.
    #[cfg_attr(not(test), allow(dead_code))]
    pub(in crate::transport) fn native(
        path: Arc<QuicPath>,
        clock: PathClock,
        note: RouteNote,
    ) -> (Self, impl Future<Output = ()> + Send + 'static) {
        Self::with_fallback(path, clock, None, note)
    }

    fn with_fallback(
        path: Arc<QuicPath>,
        clock: PathClock,
        udp: Option<Arc<dyn UdpShorts>>,
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

    /// Take `payload` for a native connection's path, which has no UDP
    /// session to hand it to. Never waits. A message refused because the queue
    /// is full is counted as `native_short_queue_full`, not as a dropped short; one refused because the path has
    /// ended is not, since the connection is over.
    pub(in crate::transport) fn queue_native_short(
        &self,
        payload: SerializedMessage,
    ) -> Result<(), ShortRefused> {
        match self.shorts.try_send(payload) {
            Ok(()) => Ok(()),
            Err(mpsc::error::TrySendError::Full(_)) => {
                self.path
                    .stats()
                    .record_send_event(QuicSendEvent::NativeShortQueueFull);
                Err(ShortRefused::Full)
            }
            Err(mpsc::error::TrySendError::Closed(_)) => Err(ShortRefused::Closed),
        }
    }

    /// Take `payload` for the path, or hand it back for UDP: the queue is full,
    /// or the path has ended. Never waits. On a path with no UDP fallback
    /// (`QuicPathLimits::effective_udp_fallback`) nothing is handed back: a
    /// payload the queue cannot take is dropped and counted, as every other
    /// send the path cannot carry is, and the caller sees it taken.
    pub(in crate::transport) fn queue_short(
        &self,
        payload: SerializedMessage,
    ) -> Result<(), SerializedMessage> {
        let refused = match self.shorts.try_send(payload) {
            Ok(()) => return Ok(()),
            Err(mpsc::error::TrySendError::Full(payload)) => {
                if self.path.udp_fallback() {
                    self.path
                        .stats()
                        .record_send_event(QuicSendEvent::ShortOverflowUdp);
                }
                (payload, ShortDropCause::QueueFull)
            }
            Err(mpsc::error::TrySendError::Closed(payload)) => (payload, ShortDropCause::PathEnded),
        };
        let (refused, cause) = refused;
        if self.path.udp_fallback() {
            return Err(refused);
        }
        self.path.stats().record_short_dropped(cause);
        self.note(|| {
            "dropped a short: the queue is full or the path has ended, and there is no UDP \
             fallback"
                .to_string()
        });
        Ok(())
    }
}

/// Why a native connection's short was not queued.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(in crate::transport) enum ShortRefused {
    /// The queue is full: the path is behind. The connection carries on.
    Full,
    /// The path has ended.
    Closed,
}

type ShortSend = Pin<Box<dyn Future<Output = (SerializedMessage, BoundedSend, u32)> + Send>>;

/// Send what the queue takes until the path has ended and nothing is left.
/// A task, and no reference to the connection's receive state.
async fn run_worker(
    path: Arc<QuicPath>,
    clock: PathClock,
    mut queue: mpsc::Receiver<SerializedMessage>,
    udp: Option<Arc<dyn UdpShorts>>,
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
                if let Some((payload, outcome, attempt)) = settled {
                    note.note(|| format!("sent a short on the path: {outcome:?}"));
                    if connected && path.sends_data() && path.retry_not_opened(&outcome, attempt) {
                        // A native path has no UDP session: send it again on the
                        // path rather than drop it. Not settled, so the attempt
                        // that did not open is not counted as a drop.
                        in_flight.push(send_on_path(&path, &clock, payload, attempt + 1));
                    } else {
                        path.note_retry_outcome(&outcome, attempt);
                        let step = path.settle(SendKind::Short, &outcome, false, clock.now_nanos());
                        if step == NextStep::SendOnUdp {
                            send_elsewhere(
                                &path,
                                udp.as_deref(),
                                &note,
                                payload,
                                ShortDropCause::NoUdpSession,
                            )
                            .await;
                        }
                    }
                }
            },
            message = queue.recv(), if queue_open && in_flight.len() < SHORTS_IN_FLIGHT => {
                match message {
                    Some(payload) if connected && path.sends_data() && !path.is_suspect() => {
                        in_flight.push(send_on_path(&path, &clock, payload, 0));
                    }
                    Some(payload) => {
                        let why = unsent_cause(connected, path.sends_data());
                        send_elsewhere(&path, udp.as_deref(), &note, payload, why).await;
                    }
                    None => queue_open = false,
                }
            },
        }
    }
}

/// Why the worker did not send a queued Short on the path, from what its test
/// for sending read (`connected && sends_data() && !is_suspect()`): the path
/// ended, the peer refused data, or the path is suspect with no UDP session to
/// take the message (suspect is entered only where UDP is the fallback).
fn unsent_cause(connected: bool, sends_data: bool) -> ShortDropCause {
    if !connected {
        ShortDropCause::PathEnded
    } else if !sends_data {
        ShortDropCause::PeerRefusedData
    } else {
        ShortDropCause::NoUdpSession
    }
}

fn send_on_path(
    path: &Arc<QuicPath>,
    clock: &PathClock,
    payload: SerializedMessage,
    attempt: u32,
) -> ShortSend {
    let path = Arc::clone(path);
    let clock = clock.clone();
    Box::pin(async move {
        let deadlines = SendDeadlines::short(payload.len() as u64);
        let outcome = path.send_short_bounded(&clock, deadlines, &payload).await;
        (payload, outcome, attempt)
    })
}

/// A message the path does not send: on the UDP session if there is one, else
/// dropped and counted (a native connection, whose path has ended).
async fn send_elsewhere(
    path: &QuicPath,
    udp: Option<&dyn UdpShorts>,
    note: &RouteNote,
    payload: SerializedMessage,
    why: ShortDropCause,
) {
    match udp {
        // A path with no UDP fallback (`QuicPathLimits::effective_udp_fallback`)
        // gives up what it cannot carry, whether or not a session exists.
        Some(udp) if path.udp_fallback() => send_on_udp(path, udp, note, payload).await,
        _ => drop_short(path, note, why),
    }
}

/// Give up a Short the native path does not take, and count why.
fn drop_short(path: &QuicPath, note: &RouteNote, cause: ShortDropCause) {
    path.stats().record_short_dropped(cause);
    note.note(|| {
        "dropped a short: the native path does not take it (ended, or the peer \
         refused data) and there is no UDP fallback"
            .to_string()
    });
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

#[cfg(test)]
mod tests {
    use super::*;

    /// Each reason the worker's test for sending on the path can fail has its own
    /// cause, in the test's own order: a suspect path (connected, sending data)
    /// is not labelled as the peer having refused data.
    #[test]
    fn an_unsent_short_is_labelled_by_the_reason_it_was_not_sent() {
        assert_eq!(unsent_cause(false, true), ShortDropCause::PathEnded);
        assert_eq!(unsent_cause(false, false), ShortDropCause::PathEnded);
        assert_eq!(unsent_cause(true, false), ShortDropCause::PeerRefusedData);
        assert_eq!(unsent_cause(true, true), ShortDropCause::NoUdpSession);
    }
}
