//! The per-connection boundary between the transport and the node.
//!
//! Everything above the transport (the network bridge and, through it, the
//! operations) talks to a connection only through [`PeerConnectionApi`] and
//! the types in this module. A second transport implementation satisfies the
//! boundary by implementing this trait.

use std::net::SocketAddr;

use futures::Future;

use super::TransportError;
use super::peer_connection::StreamId;
use super::peer_connection::streaming::StreamHandle;
use super::peer_connection::streaming_buffer::LockFreeStreamBuffer;

// =============================================================================
// Type-erased peer connection trait
// =============================================================================
//
// This trait abstracts over `PeerConnection<S>` to allow the event loop to work
// with connections without being generic over the socket type. This enables
// using the same event loop code for both production (UdpTransportSocket) and testing
// (InMemorySocket) without propagating generics through the entire codebase.

/// Outcome carried by the broadcast-queue stream-completion oneshot.
///
/// The broadcast queue holds a semaphore permit while a streaming broadcast is
/// in flight and releases it when the completion signal fires. Historically the
/// signal was a bare `()` and the queue treated *any* signal as a successful
/// delivery (issue #4235). That conflated two unrelated facts:
///
/// 1. **Permit release** — the permit MUST be released in every terminal case
///    (delivered, dropped, timed out) so concurrency slots are never leaked.
/// 2. **Delivery success** — only an actual transfer completion should refresh
///    the peer's interest TTL and cache its summary.
///
/// Several drop paths (peer channel closed, congestion timeout per #4145,
/// no connection, transport send error) fire the signal purely to release the
/// permit. Carrying an explicit outcome lets the queue release the permit in
/// all cases while only treating `Delivered` as a real send.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum BroadcastDeliveryOutcome {
    /// All stream fragments were transmitted (the last fragment was handed to
    /// the packet sender). Signaled after the final fragment leaves the sender,
    /// NOT after a receiver ACK — this path has no end-to-end delivery
    /// acknowledgement. It is the strongest "the send actually happened" signal
    /// the broadcast queue can observe, as distinct from the message being
    /// dropped before/while transmitting.
    Delivered,
    /// The stream fragment was dropped before/while transferring (channel
    /// closed, congestion timeout, no connection, transport send error). The
    /// permit is released but the message was NOT delivered.
    Dropped,
}

/// Observer of an outbound stream's progress, passed into
/// [`PeerConnectionApi::send_stream_data`] and
/// [`PeerConnectionApi::pipe_stream_data`].
///
/// The transport calls [`Self::on_fragment_sent`] each time it hands one
/// fragment of the stream to the packet sender; the caller treats that as a
/// liveness signal. The call carries no timestamp: an observer that measures
/// time reads its OWN clock, so the transport's clock (with its own epoch)
/// never mixes with the observer's (#4001 single-epoch invariant). It runs on
/// the send path once per fragment, so it must not block.
pub(crate) trait StreamProgressObserver: Send + Sync {
    /// One more fragment of the stream was handed to the packet sender.
    fn on_fragment_sent(&self);
}

/// Where a connection delivers inbound operations-level streams, set with
/// [`PeerConnectionApi::set_inbound_stream_registry`].
///
/// An operations stream's fragments can arrive before the message that
/// announces it (RequestStreaming/ResponseStreaming). The connection
/// therefore hands each new stream over as soon as its first fragment
/// arrives, and the layer above claims it by `(peer address, stream id)`
/// when it processes the announcing message.
pub(crate) trait InboundStreamRegistry: Send + Sync {
    /// A new operations stream `stream_id` from `peer_addr` has started
    /// arriving; `handle` reads it as its fragments land.
    fn register_inbound_stream(
        &self,
        peer_addr: SocketAddr,
        stream_id: StreamId,
        handle: StreamHandle,
    );

    /// The connection from `peer_addr` is gone: cancel the streams among
    /// `streams` that nobody has claimed. Each stream is matched by its
    /// buffer, so a stream with the same id from another connection, or one
    /// already claimed, is left alone.
    fn cancel_unclaimed_streams(
        &self,
        peer_addr: SocketAddr,
        streams: &mut dyn Iterator<Item = (StreamId, std::sync::Weak<LockFreeStreamBuffer>)>,
    );
}

/// Type-erased interface for peer connections.
///
/// This trait provides the minimal interface needed by `peer_connection_listener`
/// and related event loop code to communicate with a peer. By boxing connections
/// as `Box<dyn PeerConnectionApi>`, we avoid making the event loop generic over
/// the socket type.
pub(crate) trait PeerConnectionApi: Send {
    /// Returns the remote peer's socket address.
    fn remote_addr(&self) -> SocketAddr;

    /// Returns the remote peer's negotiated protocol version, if known.
    ///
    /// `None` when the version was not exchanged (e.g. the joiner->gateway
    /// path whose `AckConnection` carries no version). Callers must treat
    /// `None` as "unsupported" when gating version-dependent message types.
    fn remote_version(&self) -> Option<(u8, u8, u16)>;

    /// Whether `recv` ended this connection because nothing arrived for the
    /// idle timeout. Read after `recv` returns an error, to label the
    /// disconnect in telemetry. Defaults to `false` for test doubles.
    fn closed_by_idle_timeout(&self) -> bool {
        false
    }

    /// Whether `recv` ended this connection because the peer had more
    /// refused inbound streams outstanding than are remembered. Read after
    /// `recv` returns an error, to label the disconnect. Defaults to `false`.
    fn closed_by_stream_refusals(&self) -> bool {
        false
    }

    /// Sends one serialized message to the remote peer.
    ///
    /// `data` is the complete message, already serialized by the caller. The
    /// transport moves the bytes without interpreting them: it is the
    /// counterpart of [`Self::recv`], which hands back the same bytes at the
    /// other end. The caller knows `data.len()`, so it can attribute outbound
    /// bytes by message kind without serializing twice.
    fn send(
        &mut self,
        data: Vec<u8>,
    ) -> std::pin::Pin<Box<dyn Future<Output = Result<(), TransportError>> + Send + '_>>;

    /// Receives raw bytes from the remote peer.
    ///
    /// Returns the deserialized message bytes. The caller is responsible for
    /// deserializing into the appropriate message type.
    fn recv(
        &mut self,
    ) -> std::pin::Pin<Box<dyn Future<Output = Result<Vec<u8>, TransportError>> + Send + '_>>;

    /// Sets the registry that receives this connection's inbound
    /// operations streams (see [`InboundStreamRegistry`]).
    ///
    /// This should be called by the node layer after connection establishment,
    /// before any messages are processed.
    fn set_inbound_stream_registry(&mut self, registry: std::sync::Arc<dyn InboundStreamRegistry>);

    /// Sends raw stream data to the remote peer using the given operations-level StreamId.
    ///
    /// The data is fragmented and sent via the transport's outbound stream mechanism,
    /// using the provided `stream_id` (which should have the operations marker bit set)
    /// so that the receiver routes fragments through the orphan registry instead of
    /// the legacy InboundStream decode path.
    ///
    /// If `completion_tx` is provided, it will be signaled when the stream transfer
    /// completes, carrying a [`BroadcastDeliveryOutcome`] that distinguishes an
    /// actual delivery from a drop (see #4235). Used by the broadcast queue to
    /// release its semaphore permit in all cases while only treating a real
    /// delivery as a successful send.
    fn send_stream_data(
        &mut self,
        stream_id: StreamId,
        data: bytes::Bytes,
        metadata: Option<bytes::Bytes>,
        completion_tx: Option<tokio::sync::oneshot::Sender<BroadcastDeliveryOutcome>>,
        progress: Option<std::sync::Arc<dyn StreamProgressObserver>>,
    ) -> std::pin::Pin<Box<dyn Future<Output = Result<(), TransportError>> + Send + '_>>;

    /// Pipes an inbound stream to the remote peer, forwarding fragments as they arrive.
    ///
    /// Unlike `send_stream_data` which takes complete data and fragments it, this reads
    /// from an existing `StreamHandle` and forwards each fragment incrementally without
    /// waiting for full reassembly. This enables low-latency forwarding at intermediate nodes.
    ///
    /// The `outbound_stream_id` should be created via `StreamId::next_operations()` for
    /// operations-level routing at the receiver.
    fn pipe_stream_data(
        &mut self,
        outbound_stream_id: StreamId,
        inbound_handle: StreamHandle,
        metadata: Option<bytes::Bytes>,
        progress: Option<std::sync::Arc<dyn StreamProgressObserver>>,
    ) -> std::pin::Pin<Box<dyn Future<Output = Result<(), TransportError>> + Send + '_>>;
}
