//! Streams and messages past one packet on a QUIC path (#5940, e5b-6).
//!
//! Each runs as a task in `PeerConnection::outbound_stream_futures`, as a UDP
//! stream does, so the transfer's result is accounted for in one place and nothing
//! waits in the connection's task. A send ends by the table in
//! `quic::path::route`: done, gone, or handed to the UDP code with the same id.
//!
//! No UDP state moves for a send that stays on the path, so flight size is only
//! touched by `send_stream` and `pipe_stream`, which release what they charged by
//! the rules of #4345.
//!
//! A send that goes on UDP is reported as started twice under one id, since the
//! UDP code reports its own start.

use std::net::SocketAddr;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};
use std::task::{Context, Poll};
use std::time::Duration;

use aes_gcm::Aes128Gcm;
use bytes::Bytes;
use futures::Stream;
use tokio::sync::oneshot;

use super::outbound_stream;
use super::streaming::StreamError;
use super::{OutboundStreamResult, StreamId};
use crate::simulation::TimeSource;
use crate::tracing::{TransferDirection, TransferTransport};
use crate::transport::congestion_control::CongestionController;
use crate::transport::metrics::{
    PipeDropCause, emit_transfer_completed, emit_transfer_failed, emit_transfer_started,
};
use crate::transport::quic::path::{
    BoundedSend, NextStep, QuicSendError, SendDeadlines, SendFailure, SendKind,
};
use crate::transport::quic::upgrade::data_route::DataRoute;
use crate::transport::sent_packet_tracker::SentPacketTracker;
use crate::transport::token_bucket::TokenBucket;
use crate::transport::{
    IncomingStream, Socket, StreamProgressObserver, StreamSendOutcome, TransferStats,
    TransportError,
};

/// What `send_stream` and `pipe_stream` take of a connection, to start one on the
/// UDP session from a task.
pub(super) struct UdpStreamSender<S, T: TimeSource> {
    pub(super) last_packet_id: Arc<AtomicU32>,
    pub(super) socket: Arc<S>,
    pub(super) remote_addr: SocketAddr,
    pub(super) outbound_symmetric_key: Aes128Gcm,
    pub(super) sent_tracker: Arc<parking_lot::Mutex<SentPacketTracker<T>>>,
    pub(super) token_bucket: Arc<TokenBucket<T>>,
    pub(super) congestion_controller: Arc<CongestionController<T>>,
    pub(super) time_source: T,
}

impl<S: Socket, T: TimeSource> UdpStreamSender<S, T> {
    pub(super) async fn send_stream(
        self,
        stream_id: StreamId,
        data: Bytes,
        metadata: Option<Bytes>,
        completion_tx: Option<oneshot::Sender<StreamSendOutcome>>,
        progress: Option<Arc<dyn StreamProgressObserver>>,
    ) -> OutboundStreamResult {
        outbound_stream::send_stream(
            stream_id,
            self.last_packet_id,
            self.socket,
            self.remote_addr,
            data,
            self.outbound_symmetric_key,
            self.sent_tracker,
            self.token_bucket,
            self.congestion_controller,
            self.time_source,
            metadata,
            completion_tx,
            progress,
        )
        .await
    }

    pub(super) async fn pipe_stream(
        self,
        stream_id: StreamId,
        inbound_handle: IncomingStream,
        metadata: Option<Bytes>,
        progress: Option<Arc<dyn StreamProgressObserver>>,
    ) -> OutboundStreamResult {
        outbound_stream::pipe_stream(
            inbound_handle,
            stream_id,
            self.last_packet_id,
            self.socket,
            self.remote_addr,
            self.outbound_symmetric_key,
            self.sent_tracker,
            self.token_bucket,
            self.congestion_controller,
            self.time_source,
            metadata,
            progress,
        )
        .await
    }
}

/// Where a stream the path cannot deliver goes: the UDP session, as a boxed
/// [`UdpStreamSender`] so the path's send code is not generic over the socket.
/// A native connection has none (`None` in the `*_with` functions), and its path
/// gives such a send up (`QuicPath::settle`).
pub(super) trait UdpStreams: Send {
    fn send_stream(
        self: Box<Self>,
        stream_id: StreamId,
        data: Bytes,
        metadata: Option<Bytes>,
        completion_tx: Option<oneshot::Sender<StreamSendOutcome>>,
        progress: Option<Arc<dyn StreamProgressObserver>>,
    ) -> Pin<Box<dyn std::future::Future<Output = OutboundStreamResult> + Send>>;

    fn pipe_stream(
        self: Box<Self>,
        stream_id: StreamId,
        inbound_handle: IncomingStream,
        metadata: Option<Bytes>,
        progress: Option<Arc<dyn StreamProgressObserver>>,
    ) -> Pin<Box<dyn std::future::Future<Output = OutboundStreamResult> + Send>>;
}

impl<S: Socket, T: TimeSource> UdpStreams for UdpStreamSender<S, T> {
    fn send_stream(
        self: Box<Self>,
        stream_id: StreamId,
        data: Bytes,
        metadata: Option<Bytes>,
        completion_tx: Option<oneshot::Sender<StreamSendOutcome>>,
        progress: Option<Arc<dyn StreamProgressObserver>>,
    ) -> Pin<Box<dyn std::future::Future<Output = OutboundStreamResult> + Send>> {
        Box::pin(UdpStreamSender::send_stream(
            *self,
            stream_id,
            data,
            metadata,
            completion_tx,
            progress,
        ))
    }

    fn pipe_stream(
        self: Box<Self>,
        stream_id: StreamId,
        inbound_handle: IncomingStream,
        metadata: Option<Bytes>,
        progress: Option<Arc<dyn StreamProgressObserver>>,
    ) -> Pin<Box<dyn std::future::Future<Output = OutboundStreamResult> + Send>> {
        Box::pin(UdpStreamSender::pipe_stream(
            *self,
            stream_id,
            inbound_handle,
            metadata,
            progress,
        ))
    }
}

/// A transfer on a path, for the same telemetry as a UDP one. The figures that
/// come from the UDP congestion controller and sent tracker do not exist here,
/// so they are left out (`None`) of the event and 0 in the stats.
struct PathTransfer<T> {
    stream_id: StreamId,
    remote_addr: SocketAddr,
    bytes: u64,
    time_source: T,
    started: Duration,
}

impl<T: TimeSource> PathTransfer<T> {
    fn start(stream_id: StreamId, remote_addr: SocketAddr, bytes: u64, time_source: T) -> Self {
        emit_transfer_started(
            stream_id.0 as u64,
            remote_addr,
            bytes,
            TransferDirection::Send,
            TransferTransport::Quic,
        );
        let started = time_source.now();
        Self {
            stream_id,
            remote_addr,
            bytes,
            time_source,
            started,
        }
    }

    fn elapsed(&self) -> Duration {
        self.time_source.now().saturating_sub(self.started)
    }

    fn completed(self) -> OutboundStreamResult {
        let elapsed = self.elapsed();
        let stream_id = self.stream_id.0 as u64;
        emit_transfer_completed(
            stream_id,
            self.remote_addr,
            self.bytes,
            elapsed.as_millis() as u64,
            self.bytes * 1000 / elapsed.as_millis().max(1) as u64,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            TransferDirection::Send,
            TransferTransport::Quic,
        );
        Ok(TransferStats {
            stream_id,
            remote_addr: self.remote_addr,
            bytes_transferred: self.bytes,
            elapsed,
            peak_cwnd_bytes: None,
            final_cwnd_bytes: None,
            slowdowns_triggered: 0,
            base_delay: Duration::ZERO,
            final_ssthresh_bytes: 0,
            min_ssthresh_floor_bytes: 0,
            total_timeouts: 0,
            final_flightsize: 0,
            configured_rate: 0,
        })
    }

    /// The transfer is gone: stream-scoped, like a UDP stream that failed. The
    /// connection stays up, and the operation layer retries with a fresh id.
    fn failed(self, outcome: &BoundedSend) -> OutboundStreamResult {
        emit_transfer_failed(
            self.stream_id.0 as u64,
            self.remote_addr,
            0,
            format!("QUIC send: {outcome:?}"),
            self.elapsed().as_millis() as u64,
            TransferDirection::Send,
            TransferTransport::Quic,
        );
        Err(TransportError::OutboundStreamFailed(self.remote_addr))
    }
}

/// What the broadcast queue hears of a stream that is not sent again. Only `Sent`
/// is `Delivered`. A `MaybeDelivered` is not resent (the peer may hold it), and it
/// is `Dropped`: on `Delivered` the queue records the peer as holding this state,
/// and quinn may still have held part of the stream unsent when the deadline
/// passed or the connection closed. Nothing would correct that belief but
/// anti-entropy, since the UDP session stays up. `Dropped` costs at most one
/// full-state send that was not needed.
fn stream_send_outcome(outcome: &BoundedSend) -> StreamSendOutcome {
    match outcome {
        BoundedSend::Sent => StreamSendOutcome::Delivered,
        BoundedSend::MaybeDelivered(_) | BoundedSend::Failed(_) => StreamSendOutcome::Dropped,
    }
}

/// Tell the broadcast queue how a stream ended. Neither outcome is an end-to-end
/// acknowledgement.
fn complete(completion_tx: Option<oneshot::Sender<StreamSendOutcome>>, outcome: StreamSendOutcome) {
    if let Some(tx) = completion_tx {
        // The receiver may be gone if the queue timed out.
        let _ignored = tx.send(outcome);
    }
}

/// A message past one packet, as a Short on the path. On UDP it is a stream of
/// its own, under `stream_id`, and a Short that did not go through leaves nothing
/// at the receiver.
pub(super) async fn send_large_short_on_path<S: Socket, T: TimeSource>(
    route: DataRoute,
    stream_id: StreamId,
    data: Bytes,
    udp: UdpStreamSender<S, T>,
) -> OutboundStreamResult {
    let (remote_addr, time_source) = (udp.remote_addr, udp.time_source.clone());
    send_large_short_with(
        route,
        stream_id,
        data,
        Some(Box::new(udp)),
        remote_addr,
        time_source,
    )
    .await
}

/// [`send_large_short_on_path`] with an optional fallback.
pub(super) async fn send_large_short_with<T: TimeSource>(
    route: DataRoute,
    stream_id: StreamId,
    data: Bytes,
    udp: Option<Box<dyn UdpStreams>>,
    remote_addr: SocketAddr,
    time_source: T,
) -> OutboundStreamResult {
    let bytes = data.len() as u64;
    let transfer = PathTransfer::start(stream_id, remote_addr, bytes, time_source);
    let mut attempt = 0;
    let outcome = loop {
        let outcome = route
            .path()
            .send_short_bounded(route.clock(), SendDeadlines::short(bytes), &data)
            .await;
        route.note(|| format!("sent a large short on the path: {outcome:?}"));
        if !route.path().retry_not_opened(&outcome, attempt) {
            break outcome;
        }
        attempt += 1;
    };
    route.path().note_retry_outcome(&outcome, attempt);
    match route.path().settle(
        SendKind::LargeShort,
        &outcome,
        false,
        route.clock().now_nanos(),
    ) {
        NextStep::Done => transfer.completed(),
        NextStep::SendOnUdp => match udp {
            Some(udp) => {
                let sent = udp.send_stream(stream_id, data, None, None, None).await;
                charge_fallback_if_sent(&route, &outcome, bytes, &sent);
                sent
            }
            None => transfer.failed(&outcome),
        },
        NextStep::GiveUp => transfer.failed(&outcome),
    }
}

/// A Stream on the path, in the shape of `send_stream`. `completion_tx` is told
/// how it ended, or goes with the message to UDP.
pub(super) async fn send_stream_on_path<S: Socket, T: TimeSource>(
    route: DataRoute,
    stream_id: StreamId,
    data: Bytes,
    metadata: Option<Bytes>,
    completion_tx: Option<oneshot::Sender<StreamSendOutcome>>,
    progress: Option<Arc<dyn StreamProgressObserver>>,
    udp: UdpStreamSender<S, T>,
) -> OutboundStreamResult {
    let (remote_addr, time_source) = (udp.remote_addr, udp.time_source.clone());
    send_stream_with(
        route,
        stream_id,
        data,
        metadata,
        completion_tx,
        progress,
        Some(Box::new(udp)),
        remote_addr,
        time_source,
    )
    .await
}

/// [`send_stream_on_path`] with an optional fallback.
#[allow(clippy::too_many_arguments)]
pub(super) async fn send_stream_with<T: TimeSource>(
    route: DataRoute,
    stream_id: StreamId,
    data: Bytes,
    metadata: Option<Bytes>,
    completion_tx: Option<oneshot::Sender<StreamSendOutcome>>,
    progress: Option<Arc<dyn StreamProgressObserver>>,
    udp: Option<Box<dyn UdpStreams>>,
    remote_addr: SocketAddr,
    time_source: T,
) -> OutboundStreamResult {
    let bytes = data.len() as u64;
    let transfer = PathTransfer::start(stream_id, remote_addr, bytes, time_source);
    let mut attempt = 0;
    let outcome = loop {
        let outcome = route
            .path()
            .send_stream_bounded(
                route.clock(),
                route.path().stream_deadlines(bytes),
                stream_id,
                metadata.as_deref(),
                data.clone(),
                progress.as_deref(),
            )
            .await;
        route.note(|| format!("sent a stream on the path: {outcome:?}"));
        if !route.path().retry_not_opened(&outcome, attempt) {
            break outcome;
        }
        attempt += 1;
    };
    route.path().note_retry_outcome(&outcome, attempt);
    match route
        .path()
        .settle(SendKind::Stream, &outcome, false, route.clock().now_nanos())
    {
        NextStep::Done => {
            complete(completion_tx, stream_send_outcome(&outcome));
            transfer.completed()
        }
        NextStep::SendOnUdp => match udp {
            Some(udp) => {
                let sent = udp
                    .send_stream(stream_id, data, metadata, completion_tx, progress)
                    .await;
                charge_fallback_if_sent(&route, &outcome, bytes, &sent);
                sent
            }
            None => {
                complete(completion_tx, StreamSendOutcome::Dropped);
                transfer.failed(&outcome)
            }
        },
        NextStep::GiveUp => {
            complete(completion_tx, StreamSendOutcome::Dropped);
            transfer.failed(&outcome)
        }
    }
}

/// An inbound stream forwarded as a Stream on the path, in the shape of
/// `pipe_stream`. A fallback to UDP reads the stream from its start: a chunk the
/// path took is gone from its reader but not from `inbound_handle`, and no stream
/// was opened, so the receiver has none of it.
pub(super) async fn pipe_stream_on_path<S: Socket, T: TimeSource>(
    route: DataRoute,
    stream_id: StreamId,
    inbound_handle: IncomingStream,
    metadata: Option<Bytes>,
    progress: Option<Arc<dyn StreamProgressObserver>>,
    udp: UdpStreamSender<S, T>,
) -> OutboundStreamResult {
    let (remote_addr, time_source) = (udp.remote_addr, udp.time_source.clone());
    pipe_stream_with(
        route,
        stream_id,
        inbound_handle,
        metadata,
        progress,
        Some(Box::new(udp)),
        remote_addr,
        time_source,
    )
    .await
}

/// [`pipe_stream_on_path`] with an optional fallback.
#[allow(clippy::too_many_arguments)]
pub(super) async fn pipe_stream_with<T: TimeSource>(
    route: DataRoute,
    stream_id: StreamId,
    inbound_handle: IncomingStream,
    metadata: Option<Bytes>,
    progress: Option<Arc<dyn StreamProgressObserver>>,
    udp: Option<Box<dyn UdpStreams>>,
    remote_addr: SocketAddr,
    time_source: T,
) -> OutboundStreamResult {
    let bytes = inbound_handle.total_bytes();
    let quic_fed = inbound_handle.is_quic_fed();
    let transfer = PathTransfer::start(stream_id, remote_addr, bytes, time_source);
    let mut source = WatchedSource::new(inbound_handle.chunks());
    // The source's own gap (5 s for a UDP-fed stream, the source path's gap
    // for a QUIC-fed one, whose chunks also end when its reader does), but no
    // longer than an estimate from below of what this path's receiver waits
    // between two pieces (`QuicPath::receiver_gap`): past its gap the receiver
    // has given up on the stream, and piping on would write into a stream
    // nobody assembles and could report it sent. A slow upstream feeding a
    // fast downstream therefore still fails at about the downstream gap, as
    // on UDP. Both are taken once, when the pipe starts. A fallback to UDP pipes
    // with UDP's 5 s.
    let source_gap = inbound_handle
        .fragment_gap_limit()
        .min(route.path().receiver_gap());
    let deadlines = route
        .path()
        .stream_deadlines(bytes)
        .with_source_gap(source_gap);
    let outcome = route
        .path()
        .send_stream_chunks_bounded(
            route.clock(),
            deadlines,
            stream_id,
            bytes,
            metadata.as_deref(),
            &mut source,
            progress.as_deref(),
        )
        .await;
    route.note(|| format!("sent a pipe on the path: {outcome:?}"));
    let stalled = source.stalled();
    let step = route
        .path()
        .settle(SendKind::Pipe, &outcome, stalled, route.clock().now_nanos());
    record_pipe_end(&route, quic_fed, &outcome, stalled, step);
    match step {
        NextStep::Done => transfer.completed(),
        NextStep::SendOnUdp => match udp {
            Some(udp) => {
                let sent = udp
                    .pipe_stream(stream_id, inbound_handle, metadata, progress)
                    .await;
                charge_fallback_if_sent(&route, &outcome, bytes, &sent);
                sent
            }
            None => transfer.failed(&outcome),
        },
        NextStep::GiveUp => transfer.failed(&outcome),
    }
}

/// Count a pipe that ended `sent` or `dropped` (as `QuicPath::settle` counted it
/// in `quic_data_send_pipe_*`) by what fed its inbound stream, and a dropped one
/// by cause (`quic_pipe_source_*`, `quic_pipe_dropped_*`). A pipe that went on
/// UDP, or ended `MaybeDelivered`, is in neither.
fn record_pipe_end(
    route: &DataRoute,
    quic_fed: bool,
    outcome: &BoundedSend,
    stalled: bool,
    step: NextStep,
) {
    let node = route.path().stats().node();
    match pipe_end(step, outcome, stalled) {
        Some(PipeEnd::Sent) => node.record_pipe_sent(quic_fed),
        Some(PipeEnd::Dropped(cause)) => node.record_pipe_dropped(quic_fed, cause),
        None => {}
    }
}

/// How a pipe's end is counted in `quic_pipe_source_*`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum PipeEnd {
    Sent,
    Dropped(PipeDropCause),
}

/// The count a pipe that settled to `step` takes: `sent` for a `Sent` outcome
/// that is done, `dropped` (with its cause) for one given up, and none for one
/// that went on UDP or ended `MaybeDelivered`, as `quic_data_send_pipe_sent`
/// and `quic_data_send_pipe_dropped` count them.
fn pipe_end(step: NextStep, outcome: &BoundedSend, stalled: bool) -> Option<PipeEnd> {
    match step {
        NextStep::Done if matches!(outcome, BoundedSend::Sent) => Some(PipeEnd::Sent),
        NextStep::GiveUp => Some(PipeEnd::Dropped(pipe_drop_cause(outcome, stalled))),
        NextStep::Done | NextStep::SendOnUdp => None,
    }
}

/// Why a pipe was dropped: its source failed, a deadline ran out while it
/// waited for its source (`stalled`, as the stall count reads it), or anything
/// else, which is this path's.
fn pipe_drop_cause(outcome: &BoundedSend, stalled: bool) -> PipeDropCause {
    // Every variant named, so a new one does not compile until it has a cause.
    match outcome {
        BoundedSend::Failed(SendFailure::Error(
            QuicSendError::Source(_) | QuicSendError::LengthMismatch { .. },
        )) => PipeDropCause::SourceError,
        BoundedSend::Failed(SendFailure::TimedOut) if stalled => PipeDropCause::SourceStallTimeout,
        BoundedSend::Failed(
            SendFailure::TimedOut
            | SendFailure::NotOpened(_)
            | SendFailure::Paced
            | SendFailure::Error(
                QuicSendError::InvalidFrame(_)
                | QuicSendError::Stopped(_)
                | QuicSendError::ConnectionLost(_)
                | QuicSendError::Closed,
            ),
        )
        | BoundedSend::Sent
        | BoundedSend::MaybeDelivered(_) => PipeDropCause::OwnPath,
    }
}

/// After a message the pacer refused went on UDP: charge the path's pacer for
/// it if UDP sent it (`QuicPath::charge_fallback`). A UDP send that failed is
/// charged nothing, so a fallback that sent nothing leaves no debt; one that
/// failed partway is charged nothing either, which errs below the limit.
fn charge_fallback_if_sent(
    route: &DataRoute,
    outcome: &BoundedSend,
    bytes: u64,
    sent: &OutboundStreamResult,
) {
    if sent.is_ok() {
        route.path().charge_fallback(outcome, bytes);
    }
}

/// A chunk source that remembers whether its reader is waiting for it. A pipe
/// whose upstream stalls reaches a send deadline with a healthy path, and the path
/// must not count that against itself.
struct WatchedSource<St> {
    inner: St,
    waiting: AtomicBool,
}

impl<St> WatchedSource<St> {
    fn new(inner: St) -> Self {
        Self {
            inner,
            waiting: AtomicBool::new(false),
        }
    }

    /// Whether the last read of the source found nothing yet.
    fn stalled(&self) -> bool {
        self.waiting.load(Ordering::Relaxed)
    }
}

impl<St> Stream for WatchedSource<St>
where
    St: Stream<Item = Result<Bytes, StreamError>> + Unpin,
{
    type Item = St::Item;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();
        let polled = Pin::new(&mut this.inner).poll_next(cx);
        this.waiting.store(polled.is_pending(), Ordering::Relaxed);
        polled
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::transport::quic::path::{MaybeDelivered, NotOpened};

    /// A pipe is counted in `quic_pipe_source_*` exactly when
    /// `QuicPath::settle` counts it `sent` or `dropped`: one that went on UDP
    /// (`SendOnUdp`, counted `not_opened_udp` or `failed_udp`) or ended
    /// `MaybeDelivered` is in neither family, so each pair sums to its
    /// `quic_data_send_pipe_<result>`.
    #[test]
    fn a_pipe_is_counted_only_when_it_ends_sent_or_dropped() {
        let timed_out = BoundedSend::Failed(SendFailure::TimedOut);
        let cases = [
            (NextStep::Done, BoundedSend::Sent, Some(PipeEnd::Sent)),
            (
                NextStep::Done,
                BoundedSend::MaybeDelivered(MaybeDelivered::TimedOut),
                None,
            ),
            (
                NextStep::Done,
                BoundedSend::MaybeDelivered(MaybeDelivered::ConnectionLost),
                None,
            ),
            (
                NextStep::SendOnUdp,
                BoundedSend::Failed(SendFailure::NotOpened(NotOpened::TimedOut)),
                None,
            ),
            (
                NextStep::SendOnUdp,
                BoundedSend::Failed(SendFailure::Error(QuicSendError::Stopped(u64::from(
                    crate::transport::quic::path::stop_code::NOT_ACCEPTING_DATA,
                )))),
                None,
            ),
            (
                NextStep::GiveUp,
                timed_out,
                Some(PipeEnd::Dropped(PipeDropCause::OwnPath)),
            ),
        ];
        for (step, outcome, want) in cases {
            assert_eq!(
                pipe_end(step, &outcome, false),
                want,
                "{step:?} {outcome:?}"
            );
        }
        assert_eq!(
            pipe_end(
                NextStep::GiveUp,
                &BoundedSend::Failed(SendFailure::TimedOut),
                true
            ),
            Some(PipeEnd::Dropped(PipeDropCause::SourceStallTimeout))
        );
    }
}
