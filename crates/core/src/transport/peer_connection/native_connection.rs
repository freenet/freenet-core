//! A QUIC-native connection as the node sees a connection (#5940, A5a): a
//! [`PeerConnectionApi`] over a QUIC path with no UDP session under it.
//!
//! Built from a connection that finished its Hello exchange
//! (`quic::native::setup`). Its path carries data from birth and has no UDP
//! session to fall back to ([`QuicPathLimits::NATIVE`]):
//! - a send the path cannot deliver is given up (the operation layer retries,
//!   as after a UDP stream failure), and a short the full queue hands back fails
//!   its send; nothing is ever sent elsewhere;
//! - any end of the path ends the connection, and `recv` then returns an error,
//!   with [`CloseReason::IdleTimeout`] when quinn timed it out;
//! - what `recv` hands the node goes through the same record as a UDP
//!   connection's ([`InboundDispatch`]), so a stream's metadata is not
//!   dispatched twice and a stream id is not reused;
//! - the receipt stamp is written by the path's acknowledgement watch, so the
//!   identity rules see a native peer as heard from while its path moves.
//!
//! Its peer's key was proven in the TLS handshake and its version in the
//! Hello. Liveness is quinn's: the 15 s keepalive and the idle timeout.

#[cfg(test)]
mod tests;

use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use futures::StreamExt;
use futures::stream::FuturesUnordered;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;

use super::inbound_budget::SharedInboundBudget;
use super::inbound_dispatch::{InboundDispatch, TakenStream};
use super::quic_send;
use super::streaming::{
    MAX_INBOUND_STREAM_BYTES_PER_CONNECTION, MAX_INBOUND_STREAMS_PER_CONNECTION,
};
use super::{
    COMPLETED_STREAM_MEMORY, DEDUP_CACHE_CAPACITY, OutboundStreamResult, StreamId, fits_one_packet,
};
use crate::config::GlobalExecutor;
use crate::simulation::TimeSource;
use crate::transport::boundary::{
    CloseReason, InboundStreamRegistry, ObservedSend, PeerConnectionApi, ReceiptStamp,
    StreamProgressObserver, StreamSendOutcome,
};
use crate::transport::crypto::TransportPublicKey;
use crate::transport::metrics::QuicCloseCode;
use crate::transport::quic::QuicConnection;
use crate::transport::quic::path::{
    InboundShort, InboundStreamArrival, QuicPath, QuicPathLimits, data_channels,
};
use crate::transport::quic::upgrade::data_route::{DataRoute, RouteNote};
use crate::transport::quic::upgrade::driver::PathClock;
use crate::transport::{IncomingStream, TRANSPORT_METRICS, TransportError, normalize_mapped_addr};

/// A QUIC-native connection to one peer.
pub(crate) struct NativePeerConnection {
    remote_addr: SocketAddr,
    /// From the peer's Hello.
    remote_version: (u8, u8, u16),
    /// The key the peer proved in the TLS handshake.
    peer_key: TransportPublicKey,
    route: DataRoute,
    /// The path's inbox, each channel `None` once it has ended. The path's
    /// receive side lets go of both when the connection ends; whatever one
    /// still holds is delivered before the connection reads as ended.
    shorts: Option<mpsc::Receiver<InboundShort>>,
    streams: Option<mpsc::Receiver<InboundStreamArrival>>,
    /// Kept open, unread: a native path sends no control frames, and one that
    /// arrives is dropped and counted by the path when the channel is full.
    _controls: tokio::sync::mpsc::Receiver<bytes::Bytes>,
    dispatch: InboundDispatch,
    budget: SharedInboundBudget,
    /// When the path last moved (its acknowledgement watch), on the path's clock.
    receipt_nanos: Arc<AtomicU64>,
    clock: PathClock,
    outbound: FuturesUnordered<JoinHandle<OutboundStreamResult>>,
    close_reason: Option<CloseReason>,
}

impl NativePeerConnection {
    /// Wrap `connection`, whose Hello exchange gave `remote_version`, in a path
    /// that carries data from birth. `windows` is the node's buffer budget,
    /// which sets the path's windows, as for an upgrade's data path.
    pub(crate) fn new(
        connection: QuicConnection,
        remote_version: (u8, u8, u16),
        clock: PathClock,
        windows: Option<&Arc<crate::transport::quic::budget::QuicBudget>>,
        note: RouteNote,
    ) -> Self {
        let remote_addr = normalize_mapped_addr(connection.inner().remote_address());
        let peer_key = connection.peer_key().transport_key().clone();
        let budget = SharedInboundBudget::new(
            MAX_INBOUND_STREAMS_PER_CONNECTION,
            MAX_INBOUND_STREAM_BYTES_PER_CONNECTION,
        );
        let (senders, inbox) = data_channels();
        let (path, controls) = QuicPath::with_budget(
            connection,
            clock.clone(),
            QuicPathLimits::NATIVE,
            budget.clone(),
            senders,
            windows,
        );
        path.enable_data();
        let receipt_nanos = Arc::new(AtomicU64::new(clock.now_nanos()));
        let (stamp, stamp_clock) = (Arc::clone(&receipt_nanos), clock.clone());
        path.report_receipts_to(Box::new(move || {
            stamp.store(stamp_clock.now_nanos(), Ordering::Release);
        }));
        let (route, worker) = DataRoute::native(Arc::new(path), clock.clone(), note);
        route.path().count_as_data_path();
        // Bounded by the path: it ends when the connection does.
        drop(GlobalExecutor::spawn(worker));
        Self {
            remote_addr,
            remote_version,
            peer_key,
            route,
            shorts: Some(inbox.shorts),
            streams: Some(inbox.streams),
            _controls: controls,
            dispatch: InboundDispatch {
                inbound_stream_registry: None,
                dispatched_msg_hashes: lru::LruCache::new(DEDUP_CACHE_CAPACITY),
                completed_inbound_streams: lru::LruCache::new(COMPLETED_STREAM_MEMORY),
            },
            budget,
            receipt_nanos,
            clock,
            outbound: FuturesUnordered::new(),
            close_reason: None,
        }
    }

    /// The path, for tests.
    #[cfg(test)]
    pub(crate) fn path(&self) -> &QuicPath {
        self.route.path()
    }

    /// Send one serialized message: queued for the path if it fits one packet
    /// (never waits; a full queue fails this send), otherwise as a large short
    /// in a task of its own.
    fn route_serialized(&mut self, data: Vec<u8>) -> Result<ObservedSend, TransportError> {
        if !fits_one_packet(data.len() as u64) {
            let stream_id = StreamId::next_transport_internal();
            self.spawn(quic_send::send_large_short_with(
                self.route.clone(),
                stream_id,
                data.into(),
                None,
                self.remote_addr,
                self.clock.clone(),
            ));
            return Ok(ObservedSend::Unobserved);
        }
        match self.route.queue_short(data) {
            Ok(()) => Ok(ObservedSend::Unobserved),
            Err(_) => Err(TransportError::ConnectionClosed(self.remote_addr)),
        }
    }

    fn spawn(
        &mut self,
        send: impl std::future::Future<Output = OutboundStreamResult> + Send + 'static,
    ) {
        self.outbound.push(GlobalExecutor::spawn(send));
    }

    /// Count a finished stream task, as `PeerConnection::recv` does.
    fn account(&self, result: Result<OutboundStreamResult, tokio::task::JoinError>) {
        match result {
            Ok(Ok(stats)) => TRANSPORT_METRICS.record_transfer_completed(&stats),
            Ok(Err(error)) => {
                TRANSPORT_METRICS.record_transfer_failed();
                tracing::debug!(
                    peer_addr = %self.remote_addr,
                    %error,
                    "A native stream send failed; the operation layer retries"
                );
            }
            Err(error) => {
                TRANSPORT_METRICS.record_transfer_failed();
                tracing::warn!(
                    peer_addr = %self.remote_addr,
                    %error,
                    "A native stream send task ended without a result"
                );
            }
        }
    }

    /// The next message for the node, or the connection's end.
    async fn recv_next(&mut self) -> Result<Vec<u8>, TransportError> {
        loop {
            if self.shorts.is_none() && self.streams.is_none() {
                return Err(self.ended());
            }
            // Cancel-safe: each arm's future loses nothing when dropped
            // (`next_or_end` over `mpsc::Receiver::recv`, `FuturesUnordered::next`),
            // and each body returns or loops without waiting.
            crate::deterministic_select! {
                short = next_or_end(&mut self.shorts) => {
                    let Some(short) = short else {
                        continue;
                    };
                    let bytes = self.dispatch.take_short(short, &self.budget);
                    TRANSPORT_METRICS.record_inbound_completed(bytes.len() as u64);
                    return Ok(bytes);
                },
                arrival = next_or_end(&mut self.streams) => {
                    let Some(arrival) = arrival else {
                        continue;
                    };
                    match self.dispatch.take_stream(arrival, &self.budget, self.remote_addr) {
                        TakenStream::IdInUse => TRANSPORT_METRICS.record_inbound_stream_outcome(
                            crate::transport::metrics::InboundStreamOutcome::StreamIdOnBothPaths,
                        ),
                        TakenStream::Dropped => {}
                        TakenStream::Registered { message, .. } => {
                            if let Some(message) = message {
                                return Ok(message);
                            }
                        }
                    }
                },
                finished = self.outbound.next(), if !self.outbound.is_empty() => {
                    if let Some(result) = finished {
                        self.account(result);
                    }
                },
            }
        }
    }

    /// The connection ended: the path's receive side let go of the inbox and
    /// everything in it was delivered.
    fn ended(&mut self) -> TransportError {
        if self.route.path().idle_timed_out() {
            self.close_reason = Some(CloseReason::IdleTimeout);
        }
        TransportError::ConnectionClosed(self.remote_addr)
    }
}

/// The node let the peer go: close the connection now, since stream tasks
/// still running hold the path and would keep it open until they end.
impl Drop for NativePeerConnection {
    fn drop(&mut self) {
        self.route.path().uncount_as_data_path();
        self.route.path().close(QuicCloseCode::NodeDroppedPeer);
    }
}

/// The next message of `channel`, or `None` once when it ends, after which
/// `channel` is `None` and this waits forever.
async fn next_or_end<M>(channel: &mut Option<mpsc::Receiver<M>>) -> Option<M> {
    if let Some(receiver) = channel {
        let message = receiver.recv().await;
        if message.is_none() {
            *channel = None;
        }
        return message;
    }
    std::future::pending().await
}

impl PeerConnectionApi for NativePeerConnection {
    fn remote_addr(&self) -> SocketAddr {
        self.remote_addr
    }

    fn remote_version(&self) -> Option<(u8, u8, u16)> {
        Some(self.remote_version)
    }

    fn receipt_stamp(&self) -> Option<ReceiptStamp> {
        let clock = self.clock.clone();
        // No UDP side: its time is 0, so the stamp reads the path's alone.
        Some(ReceiptStamp::new(
            Arc::new(AtomicU64::new(0)),
            Arc::clone(&self.receipt_nanos),
            Arc::new(move || clock.now_nanos()),
        ))
    }

    fn close_reason(&self) -> Option<CloseReason> {
        self.close_reason
    }

    fn send_bytes(
        &mut self,
        data: Vec<u8>,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<(), TransportError>> + Send + '_>>
    {
        let sent = self.route_serialized(data).map(|_| ());
        Box::pin(async move { sent })
    }

    fn send_bytes_observed(
        &mut self,
        data: Vec<u8>,
        completion_tx: tokio::sync::oneshot::Sender<StreamSendOutcome>,
    ) -> std::pin::Pin<
        Box<dyn std::future::Future<Output = Result<ObservedSend, TransportError>> + Send + '_>,
    > {
        // No UDP stream to observe: a short or a large short on the path.
        drop(completion_tx);
        let sent = self.route_serialized(data);
        Box::pin(async move { sent })
    }

    fn recv(
        &mut self,
    ) -> std::pin::Pin<
        Box<dyn std::future::Future<Output = Result<Vec<u8>, TransportError>> + Send + '_>,
    > {
        Box::pin(self.recv_next())
    }

    fn set_inbound_stream_registry(&mut self, registry: Arc<dyn InboundStreamRegistry>) {
        self.dispatch.inbound_stream_registry = Some(registry);
    }

    /// The key was proven in the handshake; a promotion under another key is
    /// refused and counted, as on a UDP connection that holds a key.
    fn set_peer_identity(&mut self, key: TransportPublicKey) {
        let kind = if key == self.peer_key {
            crate::transport::metrics::ConnIdentity::TransportKeySame
        } else {
            tracing::warn!(
                peer_addr = %self.remote_addr,
                held = %self.peer_key,
                promoted = %key,
                "Native connection promoted under a key other than the one it proved; \
                 keeping the proven key"
            );
            crate::transport::metrics::ConnIdentity::TransportKeyRefused
        };
        TRANSPORT_METRICS.record_conn_identity(self.remote_addr, kind);
    }

    fn send_stream_data(
        &mut self,
        stream_id: StreamId,
        data: bytes::Bytes,
        metadata: Option<bytes::Bytes>,
        completion_tx: Option<tokio::sync::oneshot::Sender<StreamSendOutcome>>,
        progress: Option<Arc<dyn StreamProgressObserver>>,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<(), TransportError>> + Send + '_>>
    {
        self.spawn(quic_send::send_stream_with(
            self.route.clone(),
            stream_id,
            data,
            metadata,
            completion_tx,
            progress,
            None,
            self.remote_addr,
            self.clock.clone(),
        ));
        Box::pin(async { Ok(()) })
    }

    fn pipe_stream_data(
        &mut self,
        outbound_stream_id: StreamId,
        inbound_handle: IncomingStream,
        metadata: Option<bytes::Bytes>,
        progress: Option<Arc<dyn StreamProgressObserver>>,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<(), TransportError>> + Send + '_>>
    {
        self.spawn(quic_send::pipe_stream_with(
            self.route.clone(),
            outbound_stream_id,
            inbound_handle,
            metadata,
            progress,
            None,
            self.remote_addr,
            self.clock.clone(),
        ));
        Box::pin(async { Ok(()) })
    }
}
