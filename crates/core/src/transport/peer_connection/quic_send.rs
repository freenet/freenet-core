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
use crate::tracing::TransferDirection;
use crate::transport::congestion_control::CongestionController;
use crate::transport::metrics::{
    emit_transfer_completed, emit_transfer_failed, emit_transfer_started,
};
use crate::transport::quic::path::{BoundedSend, NextStep, SendDeadlines, SendKind};
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
    let bytes = data.len() as u64;
    let transfer = PathTransfer::start(stream_id, udp.remote_addr, bytes, udp.time_source.clone());
    let outcome = route
        .path()
        .send_short_bounded(route.clock(), SendDeadlines::short(bytes), &data)
        .await;
    route.note(|| format!("sent a large short on the path: {outcome:?}"));
    match route.path().settle(
        SendKind::LargeShort,
        &outcome,
        false,
        route.clock().now_nanos(),
    ) {
        NextStep::Done => transfer.completed(),
        NextStep::SendOnUdp => {
            let sent = udp.send_stream(stream_id, data, None, None, None).await;
            charge_fallback_if_sent(&route, &outcome, bytes, &sent);
            sent
        }
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
    let bytes = data.len() as u64;
    let transfer = PathTransfer::start(stream_id, udp.remote_addr, bytes, udp.time_source.clone());
    let outcome = route
        .path()
        .send_stream_bounded(
            route.clock(),
            SendDeadlines::stream(bytes),
            stream_id,
            metadata.as_deref(),
            data.clone(),
            progress.as_deref(),
        )
        .await;
    route.note(|| format!("sent a stream on the path: {outcome:?}"));
    match route
        .path()
        .settle(SendKind::Stream, &outcome, false, route.clock().now_nanos())
    {
        NextStep::Done => {
            complete(completion_tx, stream_send_outcome(&outcome));
            transfer.completed()
        }
        NextStep::SendOnUdp => {
            let sent = udp
                .send_stream(stream_id, data, metadata, completion_tx, progress)
                .await;
            charge_fallback_if_sent(&route, &outcome, bytes, &sent);
            sent
        }
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
    let bytes = inbound_handle.total_bytes();
    let transfer = PathTransfer::start(stream_id, udp.remote_addr, bytes, udp.time_source.clone());
    let mut source = WatchedSource::new(inbound_handle.chunks());
    let outcome = route
        .path()
        .send_stream_chunks_bounded(
            route.clock(),
            SendDeadlines::stream(bytes),
            stream_id,
            bytes,
            metadata.as_deref(),
            &mut source,
            progress.as_deref(),
        )
        .await;
    route.note(|| format!("sent a pipe on the path: {outcome:?}"));
    let stalled = source.stalled();
    match route
        .path()
        .settle(SendKind::Pipe, &outcome, stalled, route.clock().now_nanos())
    {
        NextStep::Done => transfer.completed(),
        NextStep::SendOnUdp => {
            let sent = udp
                .pipe_stream(stream_id, inbound_handle, metadata, progress)
                .await;
            charge_fallback_if_sent(&route, &outcome, bytes, &sent);
            sent
        }
        NextStep::GiveUp => transfer.failed(&outcome),
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
