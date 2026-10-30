//! The per-connection boundary between the transport and the node.
//!
//! Once a connection is established, everything above the transport (the
//! network bridge and, through it, the operations) talks to it only through
//! [`PeerConnectionApi`] and the types in this module. The UDP transport is
//! the only implementation. A second one could not yet implement this trait
//! alone, because the boundary still carries UDP assumptions.
//!
//! # Known UDP assumptions still in the boundary
//!
//! - The node opens a transport as a [`TransportHandle`], but only the UDP
//!   transport can be opened (`open_udp_transport`), and its tuning
//!   (`UdpTransportConfig`: bandwidth limits, LEDBAT slow-start threshold,
//!   congestion control) is UDP-specific.
//! - [`InboundExpectations::expect_incoming`] takes a `SocketAddr` but keeps
//!   only its IP, not a peer identity, and shares its set with outbound
//!   connect.
//! - The node matches an accepted connection to its pending expectations by
//!   `SocketAddr` (`InboundMatcher` in `handshake.rs`).
//! - [`ListenerExit::Error`] carries a UDP-shaped [`TransportError`], and the
//!   event loop's log lines for a stopped listener still say "UDP listen
//!   task".
//! - [`OutboundConnector::connect`] carries UDP shape: it registers the
//!   remote IP as an expected handshake source (overlapping the inbound
//!   `expect_incoming`), it is split into enqueue and handshake stages
//!   because the UDP listener takes requests through a bounded queue (a
//!   QUIC transport would be a single `connect().await`), and it dials by
//!   the X25519 [`TransportPublicKey`] where QUIC identifies a peer by its
//!   certificate.
//! - [`TransportError`]: the UDP transport's own errors (socket send and
//!   I/O failures, handshake decryption, its wire encoding) sit inside the
//!   opaque [`TransportError::Udp`], which nothing above the transport can
//!   look into; the bridge classifies errors only through
//!   [`TransportError::scope`] and constructs only neutral variants. That
//!   last part is a convention nothing enforces: the public `From` impls for
//!   `io::Error`, `bincode::Error` and `DecryptionError` still turn a plain
//!   `?` anywhere into `Udp(..)`, and node code must not rely on them. The
//!   other variants are transport-neutral, including `ProtocolVersionMismatch`
//!   (version negotiation exists under any transport), whose Display text is
//!   parsed by `network_status::classify_transport_error`.
//! - Stream ids are chosen by the SENDER ([`StreamId::next`]), not by the
//!   transport: this is deliberate and stays (see [`StreamId`]).
//! - [`IncomingStream`] reports progress in fragments
//!   ([`IncomingStream::received_fragments`]), and its errors are
//!   `StreamError`.
//! - [`IncomingStream`] and [`IncomingChunks`] still wrap concrete UDP types
//!   (`StreamHandle`, `StreamingInboundStream`); a second transport turns
//!   them into an enum or a trait object.
//! - A pipe must forward fragments the receiver accepts: every fragment but
//!   #1 and the last exactly full, within `ceil(total / F) + 1` slots. The
//!   UDP pipe re-chunks its input to that (`Rechunk` in `outbound_stream.rs`).
//! - [`IncomingStream::total_bytes`] assumes a length declared up front. UDP
//!   carries it in every fragment; under QUIC the per-stream header that
//!   carries the [`StreamId`] would carry it too.
//! - [`IncomingStream::fork`] promises an independent reader that replays the
//!   stream from its start, so a QUIC transport would need a tee buffer.
//! - Why a connection closed is read out of band, through
//!   [`PeerConnectionApi::close_reason`] after `recv` fails, rather than
//!   carried by the error.
//! - [`CloseReason::InboundStreamRefusals`] is a concept of the UDP
//!   transport's fragment-based inbound stream registry.
//! - Peers are identified by `SocketAddr`.

use std::net::SocketAddr;
use std::sync::Arc;

use futures::Future;

use super::peer_connection::streaming::StreamHandle;
use super::peer_connection::streaming_buffer::LockFreeStreamBuffer;
use super::{TransportError, TransportPublicKey};

/// Names one stream sent with [`PeerConnectionApi::send_stream_data`] or
/// [`PeerConnectionApi::pipe_stream_data`].
///
/// The SENDER chooses it ([`StreamId::next`]) and announces it in an
/// operations message (e.g. `GetMsg::ResponseStreaming`); the receiver
/// claims the stream from its [`InboundStreamRegistry`] by (sender address,
/// `StreamId`). So **every transport must deliver the sender's `StreamId`
/// with the stream**: the UDP transport puts it in every fragment, and a QUIC
/// transport would write it in a per-stream header ahead of the payload,
/// since QUIC's own stream ids are assigned by the transport and cannot be
/// chosen by the sender.
///
/// On the wire it is a `u32`, serialized transparently, and its values are
/// part of the protocol with existing peers. Ids from [`StreamId::next`] have
/// the top bit set; ids without it belong to a transport's own streams
/// ([`StreamId::next_transport_internal`]). The UDP transport relies on that
/// split to route a stream (see [`StreamId::is_caller_named`]).
///
/// The split is a convention, not a guarantee: both allocators share one
/// per-thread counter, so once it passes 2^31 an internal id also has the top
/// bit, and each thread's 100,000-id block overlaps the next thread's after
/// 100,000 allocations. Receivers key streams by (peer, id), so a collision
/// needs both within one connection's live streams.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
#[repr(transparent)]
#[serde(transparent)]
pub struct StreamId(pub(in crate::transport) u32);

/// Each thread's counter starts at its own block, so ids from parallel
/// simulation threads start apart (they overlap after `STREAM_ID_BLOCK`
/// allocations on one thread).
const STREAM_ID_BLOCK: u32 = 100_000;

/// Top bit of ids from [`StreamId::next`].
const CALLER_NAMED_BIT: u32 = 0x8000_0000;

thread_local! {
    static STREAM_ID_COUNTER: std::cell::Cell<u32> = {
        let idx = crate::config::GlobalRng::thread_index();
        std::cell::Cell::new((idx as u32) * STREAM_ID_BLOCK)
    };
}

fn next_stream_counter() -> u32 {
    STREAM_ID_COUNTER.with(|c| {
        let v = c.get();
        c.set(v + 1);
        v
    })
}

impl StreamId {
    /// A fresh id for a stream this node sends with `send_stream_data` or
    /// `pipe_stream_data`.
    pub fn next() -> Self {
        Self(next_stream_counter() | CALLER_NAMED_BIT)
    }

    /// A fresh id for a stream a transport opens for its own purposes (the
    /// UDP transport's oversized messages). Distinct from ids from
    /// [`Self::next`] until the shared counter passes 2^31 (see the type
    /// docs).
    pub(crate) fn next_transport_internal() -> Self {
        Self(next_stream_counter())
    }

    /// Whether this id came from [`Self::next`]: a stream named by the
    /// sender's caller rather than by its transport.
    pub(crate) fn is_caller_named(&self) -> bool {
        self.0 & CALLER_NAMED_BIT != 0
    }

    /// Reset the stream ID counter to initial state for this thread.
    /// Thread-local, so safe for parallel test execution.
    pub fn reset_counter() {
        let idx = crate::config::GlobalRng::thread_index();
        STREAM_ID_COUNTER.with(|c| c.set((idx as u32) * STREAM_ID_BLOCK));
    }
}

impl std::fmt::Display for StreamId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(f)
    }
}

#[cfg(test)]
impl<'a> arbitrary::Arbitrary<'a> for StreamId {
    fn arbitrary(u: &mut arbitrary::Unstructured<'a>) -> arbitrary::Result<Self> {
        Ok(Self(u.arbitrary()?))
    }
}

// =============================================================================
// Type-erased peer connection trait
// =============================================================================
//
// This trait abstracts over `PeerConnection<S>` so the event loop works with
// connections without being generic over the socket type: the same event loop
// code serves production (UdpTransportSocket) and testing (InMemorySocket).

/// Outcome carried by the stream-completion oneshot of
/// [`PeerConnectionApi::send_stream_data`].
///
/// The broadcast queue, its main user, holds a semaphore permit while a
/// streaming broadcast is in flight and releases it when the completion signal
/// fires. Historically the signal was a bare `()` and the queue treated *any*
/// signal as a successful delivery (issue #4235). That conflated two unrelated
/// facts:
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
pub(crate) enum StreamSendOutcome {
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

/// Everything the node holds of an open transport.
pub(crate) struct TransportHandle {
    /// Opens outbound connections.
    pub(crate) connector: Arc<dyn OutboundConnector>,
    /// Yields inbound connections.
    pub(crate) acceptor: Box<dyn InboundAcceptor>,
    /// Registers peers expected to connect in.
    pub(crate) expected_inbound: Arc<dyn InboundExpectations>,
    /// Resolves when the transport's listener stops. The node treats that as
    /// fatal.
    pub(crate) listener: ListenerLiveness,
}

/// Registers peers the node expects to connect in, so the transport accepts
/// their handshake. It shares its state with [`OutboundConnector::connect`],
/// which registers the remote the same way when the node dials out.
pub(crate) trait InboundExpectations: Send + Sync {
    /// Expect a connection from `remote_addr`. The UDP transport keeps only
    /// the IP: the port is discarded, so any port on that IP is expected.
    fn expect_incoming(&self, remote_addr: SocketAddr);
}

/// Resolves, once, when the transport's listener stops.
pub(crate) type ListenerLiveness = std::pin::Pin<Box<dyn Future<Output = ListenerExit> + Send>>;

/// How a transport's listener stopped.
#[derive(Debug)]
pub(crate) enum ListenerExit {
    /// It returned this error.
    Error(TransportError),
    /// It panicked; the message is the join error's text.
    Panicked(String),
    /// It was cancelled; the message is the join error's text.
    Cancelled(String),
}

impl ListenerExit {
    /// Classify a listener task's join result.
    pub(crate) fn from_join(result: Result<TransportError, tokio::task::JoinError>) -> Self {
        match result {
            Ok(error) => ListenerExit::Error(error),
            Err(join_err) if join_err.is_panic() => ListenerExit::Panicked(join_err.to_string()),
            Err(join_err) => ListenerExit::Cancelled(join_err.to_string()),
        }
    }
}

/// Yields connections that remote peers opened to this node. The node owns
/// one as `Box<dyn InboundAcceptor>`.
pub(crate) trait InboundAcceptor: Send {
    /// The next inbound connection, or `None` once the transport has stopped
    /// accepting. Cancel-safe: dropping the future before it completes loses
    /// no connection, so it may be polled inside `select!`.
    fn next_connection(&mut self) -> NextConnection<'_>;
}

/// What [`InboundAcceptor::next_connection`] returns.
pub(crate) type NextConnection<'a> =
    std::pin::Pin<Box<dyn Future<Output = Option<Box<dyn PeerConnectionApi>>> + Send + 'a>>;

/// The second stage of [`OutboundConnector::connect`]: resolves when the
/// handshake completes or fails.
pub(crate) type PendingConnection = std::pin::Pin<
    Box<dyn Future<Output = Result<Box<dyn PeerConnectionApi>, TransportError>> + Send>,
>;

/// Opens outbound connections. The node holds one as
/// `Arc<dyn OutboundConnector>`.
pub(crate) trait OutboundConnector: Send + Sync {
    /// Ask the transport to connect to `remote_addr`, whose transport key is
    /// `remote_public_key`. `origin` says why, for telemetry only.
    ///
    /// Contract: the first stage registers `remote_addr`'s IP as an expected
    /// handshake source, so the transport accepts the peer's reply; this
    /// overlaps with registering an expected inbound connection.
    ///
    /// There are two stages. The returned future hands the attempt to the
    /// transport, which may mean waiting for room in the UDP listener's
    /// bounded queue. The [`PendingConnection`] it yields resolves when the
    /// handshake completes or fails. The node's connect timeout covers only
    /// the second stage; the enqueue stage is deliberately left unbounded to
    /// match the behaviour before this trait existed. That is a known gap: a
    /// stalled listener with a full queue makes the connect wait forever
    /// (#5947).
    fn connect(
        &self,
        remote_public_key: TransportPublicKey,
        remote_addr: SocketAddr,
        origin: super::NatAttemptOrigin,
    ) -> std::pin::Pin<Box<dyn Future<Output = PendingConnection> + Send + '_>>;
}

/// Why a connection's `recv` ended it, as reported by
/// [`PeerConnectionApi::close_reason`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum CloseReason {
    /// Nothing arrived for the idle timeout.
    IdleTimeout,
    /// The peer had more refused inbound streams outstanding than are
    /// remembered.
    InboundStreamRefusals,
}

/// An inbound stream, as the layers above the transport see it: claimed from
/// the [`InboundStreamRegistry`], read whole with [`Self::assemble`], or handed
/// back to a transport to forward with [`PeerConnectionApi::pipe_stream_data`].
///
/// Cloning gives another handle on the same stream; [`Self::fork`] gives an
/// independent reader. The representation is private: today it is the UDP
/// transport's reassembly handle, and a second transport would turn it into
/// an enum or a trait object without changing this API.
#[derive(Clone, Debug)]
pub struct IncomingStream(StreamHandle);

impl IncomingStream {
    /// Total length the sender declared for the stream, in bytes.
    pub fn total_bytes(&self) -> u64 {
        self.0.total_bytes()
    }

    /// Wait for the whole stream and return its bytes.
    pub async fn assemble(&self) -> Result<Vec<u8>, super::StreamError> {
        self.0.assemble().await
    }

    /// An independent reader of the same stream, e.g. to pipe it onward while
    /// also assembling it locally.
    pub fn fork(&self) -> Self {
        Self(self.0.fork())
    }

    /// Units of the stream received so far, as the transport counts them
    /// (UDP: fragments). For progress reporting only.
    pub fn received_fragments(&self) -> usize {
        self.0.received_fragments()
    }

    /// Units the whole stream takes, as the transport counts them (UDP:
    /// fragments). For progress reporting only.
    pub fn total_fragments(&self) -> usize {
        self.0.total_fragments()
    }

    /// Abandon the stream: anyone waiting on it gets `StreamError::Cancelled`.
    pub(crate) fn cancel(&self) {
        self.0.cancel()
    }

    /// Whether the stream has been abandoned.
    pub(crate) fn is_cancelled(&self) -> bool {
        self.0.is_cancelled()
    }

    /// Whether this is the stream `identity` names. The registry uses it to
    /// match an orphan to the connection that received it.
    pub(crate) fn is(&self, identity: &StreamIdentity) -> bool {
        self.0.shares_buffer(&identity.0)
    }

    /// A [`StreamIdentity`] naming this stream.
    #[cfg(test)]
    pub(crate) fn identity(&self) -> StreamIdentity {
        StreamIdentity(self.0.buffer_weak())
    }

    /// The stream's data, chunk by chunk, in order, as it arrives. Chunk
    /// sizes are the receiving transport's (UDP: one per fragment). Reading
    /// does not consume the stream: [`Self::assemble`] and other readers
    /// still see all of it. See [`IncomingChunks`] for what the consumer
    /// must check itself.
    pub fn chunks(&self) -> IncomingChunks {
        IncomingChunks {
            inner: self.0.stream(),
            done: false,
        }
    }
}

/// The data of an [`IncomingStream`], from [`IncomingStream::chunks`].
///
/// - It yields `Err` once, if the stream is cancelled or fails, and then ends.
/// - It has **no inactivity timer**: if the sender stalls, it waits forever.
///   A consumer must bring its own timeout (the UDP pipe uses
///   `STREAM_INACTIVITY_TIMEOUT`).
/// - Under UDP it ends cleanly only once every declared byte was yielded: a
///   sender that filled every fragment slot short gets an `Err` instead
///   (#5445). A consumer should still count bytes against
///   [`IncomingStream::total_bytes`] (the UDP pipe does), since that is the
///   contract a second transport must also meet.
/// - Polling it again after dropping a `next()` future loses nothing, so it
///   may be read inside `select!`.
pub struct IncomingChunks {
    inner: super::StreamingInboundStream,
    done: bool,
}

impl futures::Stream for IncomingChunks {
    type Item = Result<bytes::Bytes, super::StreamError>;

    fn poll_next(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        if self.done {
            return std::task::Poll::Ready(None);
        }
        let item = std::pin::Pin::new(&mut self.inner).poll_next(cx);
        if matches!(item, std::task::Poll::Ready(Some(Err(_)) | None)) {
            self.done = true;
        }
        item
    }
}

/// Names one inbound stream without keeping it alive, so a transport can say
/// which streams a connection received after it has let go of them. Two
/// [`IncomingStream`]s that are the same stream (clones or forks) both match
/// its identity; any other stream does not.
///
/// The representation is private: today it is a weak reference to the UDP
/// transport's reassembly buffer.
#[derive(Clone, Debug)]
pub struct StreamIdentity(std::sync::Weak<LockFreeStreamBuffer>);

impl StreamIdentity {
    /// The identity of the UDP stream reassembled into `buffer`.
    pub(in crate::transport) fn from_udp_buffer(
        buffer: std::sync::Weak<LockFreeStreamBuffer>,
    ) -> Self {
        Self(buffer)
    }
}

impl From<StreamHandle> for IncomingStream {
    fn from(handle: StreamHandle) -> Self {
        Self(handle)
    }
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
    /// arriving; `stream` reads it as its data lands.
    fn register_inbound_stream(
        &self,
        peer_addr: SocketAddr,
        stream_id: StreamId,
        stream: IncomingStream,
    );

    /// The connection from `peer_addr` is gone: cancel the streams among
    /// `streams` that nobody has claimed. Each stream is matched by its
    /// [`StreamIdentity`], so a stream with the same id from another
    /// connection, or one already claimed, is left alone.
    fn cancel_unclaimed_streams(
        &self,
        peer_addr: SocketAddr,
        streams: &mut dyn Iterator<Item = (StreamId, StreamIdentity)>,
    );
}

/// When a connection last received an authenticated packet, readable from
/// outside the task that owns the connection. Cheap to clone; every clone
/// reads the same stamp.
#[derive(Clone)]
pub(crate) struct ReceiptStamp {
    last_nanos: std::sync::Arc<std::sync::atomic::AtomicU64>,
    now_nanos: std::sync::Arc<dyn Fn() -> u64 + Send + Sync>,
}

impl ReceiptStamp {
    /// `last_nanos` is the time of the last authenticated receipt and
    /// `now_nanos` reads the clock it was stamped with.
    pub(crate) fn new(
        last_nanos: std::sync::Arc<std::sync::atomic::AtomicU64>,
        now_nanos: std::sync::Arc<dyn Fn() -> u64 + Send + Sync>,
    ) -> Self {
        Self {
            last_nanos,
            now_nanos,
        }
    }

    /// Time since the connection last received an authenticated packet.
    pub(crate) fn since_last_receipt(&self) -> std::time::Duration {
        let last = self.last_nanos.load(std::sync::atomic::Ordering::Acquire);
        std::time::Duration::from_nanos((self.now_nanos)().saturating_sub(last))
    }
}

impl std::fmt::Debug for ReceiptStamp {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ReceiptStamp")
            .field("since_last_receipt", &self.since_last_receipt())
            .finish()
    }
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

    /// When this connection last received an authenticated packet (any
    /// packet that decrypted under the session key, keepalives included).
    /// `None` when the connection does not track it, as for test doubles.
    fn receipt_stamp(&self) -> Option<ReceiptStamp> {
        None
    }

    /// Why `recv` ended this connection, when the transport knows. Read
    /// after `recv` returns an error, to label the disconnect in telemetry.
    /// `None` means an ordinary transport closure. Defaults to `None` for
    /// test doubles.
    fn close_reason(&self) -> Option<CloseReason> {
        None
    }

    /// Sends one serialized message to the remote peer.
    ///
    /// `data` is the complete message, already serialized by the caller. The
    /// transport moves the bytes without interpreting them: it is the
    /// counterpart of [`Self::recv`], which hands back the same bytes at the
    /// other end. The caller knows `data.len()`, so it can attribute outbound
    /// bytes by message kind without serializing twice.
    ///
    /// Not named `send`: `PeerConnection::send<D: Serialize>` serializes its
    /// argument, and on a concrete connection it would win method resolution
    /// and serialize these bytes a second time.
    fn send_bytes(
        &mut self,
        data: Vec<u8>,
    ) -> std::pin::Pin<Box<dyn Future<Output = Result<(), TransportError>> + Send + '_>>;

    /// Receives raw bytes from the remote peer.
    ///
    /// Returns one message's bytes, exactly as the remote passed them to
    /// [`Self::send_bytes`]. The caller deserializes them.
    ///
    /// Must be cancel-safe: the node polls a fresh `recv()` in a `select!`
    /// on every loop pass and drops it whenever another branch wins, so an
    /// implementation must not dequeue a message before its first `.await`
    /// that can be lost when the future is dropped.
    fn recv(
        &mut self,
    ) -> std::pin::Pin<Box<dyn Future<Output = Result<Vec<u8>, TransportError>> + Send + '_>>;

    /// Sets the registry that receives this connection's inbound
    /// operations streams (see [`InboundStreamRegistry`]).
    ///
    /// This should be called by the node layer after connection establishment,
    /// before any messages are processed.
    fn set_inbound_stream_registry(&mut self, registry: std::sync::Arc<dyn InboundStreamRegistry>);

    /// Sends raw stream data to the remote peer as the stream `stream_id`
    /// (from [`StreamId::next`]), which the transport delivers with the
    /// stream so the receiver's [`InboundStreamRegistry`] gets it under that
    /// id. The UDP transport fragments the data, and its receiver routes the
    /// fragments to the registry because the id is caller-named.
    ///
    /// If `completion_tx` is provided, it will be signaled when the stream transfer
    /// completes, carrying a [`StreamSendOutcome`] that distinguishes an
    /// actual delivery from a drop (see #4235). Used by the broadcast queue to
    /// release its semaphore permit in all cases while only treating a real
    /// delivery as a successful send.
    fn send_stream_data(
        &mut self,
        stream_id: StreamId,
        data: bytes::Bytes,
        metadata: Option<bytes::Bytes>,
        completion_tx: Option<tokio::sync::oneshot::Sender<StreamSendOutcome>>,
        progress: Option<std::sync::Arc<dyn StreamProgressObserver>>,
    ) -> std::pin::Pin<Box<dyn Future<Output = Result<(), TransportError>> + Send + '_>>;

    /// Pipes an inbound stream to the remote peer, forwarding its data as it
    /// arrives.
    ///
    /// Unlike `send_stream_data`, which takes complete data, this reads from an
    /// [`IncomingStream`] and forwards it incrementally without waiting for full
    /// reassembly. This enables low-latency forwarding at intermediate nodes.
    ///
    /// The `outbound_stream_id` should be created via `StreamId::next()` for
    /// operations-level routing at the receiver.
    fn pipe_stream_data(
        &mut self,
        outbound_stream_id: StreamId,
        inbound_handle: IncomingStream,
        metadata: Option<bytes::Bytes>,
        progress: Option<std::sync::Arc<dyn StreamProgressObserver>>,
    ) -> std::pin::Pin<Box<dyn Future<Output = Result<(), TransportError>> + Send + '_>>;
}

#[cfg(test)]
mod stream_id_tests {
    use super::StreamId;

    /// The values are part of the wire protocol with existing peers: the two
    /// allocators share one per-thread counter starting at the thread's
    /// block, ids from `next` carry the top bit, and transport-internal ids
    /// do not. These are exactly the values `next_operations` and `next`
    /// produced before `StreamId` moved into the boundary.
    #[test]
    fn stream_id_values_are_unchanged() {
        StreamId::reset_counter();
        let base = (crate::config::GlobalRng::thread_index() as u32) * 100_000;
        let internal = StreamId::next_transport_internal();
        let named = StreamId::next();
        let internal_again = StreamId::next_transport_internal();
        assert_eq!(internal.0, base);
        assert_eq!(named.0, (base + 1) | 0x8000_0000);
        assert_eq!(internal_again.0, base + 2);
        assert!(named.is_caller_named());
        assert!(!internal.is_caller_named());
        assert!(!internal_again.is_caller_named());
    }

    /// A `StreamId` is serialized as its bare `u32`, as before.
    #[test]
    fn stream_id_serializes_as_a_bare_u32() {
        let id = StreamId(0x8000_0007);
        assert_eq!(
            bincode::serialize(&id).unwrap(),
            bincode::serialize(&0x8000_0007u32).unwrap()
        );
        assert_eq!(
            bincode::deserialize::<StreamId>(&bincode::serialize(&0x8000_0007u32).unwrap())
                .unwrap(),
            id
        );
    }
}

#[cfg(test)]
mod incoming_stream_tests {
    use super::{IncomingStream, StreamId};
    use crate::transport::StreamError;
    use crate::transport::peer_connection::streaming::StreamHandle;
    use crate::transport::peer_connection::streaming_buffer::FRAGMENT_PAYLOAD_SIZE;

    fn handle_with(fragments: usize) -> (StreamHandle, Vec<u8>) {
        let total = fragments * FRAGMENT_PAYLOAD_SIZE;
        let payload: Vec<u8> = (0..total).map(|i| i as u8).collect();
        let handle = StreamHandle::new(StreamId::next(), total as u64);
        for (n, chunk) in payload.chunks(FRAGMENT_PAYLOAD_SIZE).enumerate() {
            handle
                .push_fragment(n as u32 + 1, bytes::Bytes::copy_from_slice(chunk))
                .unwrap();
        }
        (handle, payload)
    }

    /// Each `IncomingStream` method reports what the UDP handle it wraps
    /// reports.
    #[tokio::test]
    async fn incoming_stream_reads_the_udp_handle_it_wraps() {
        let (handle, payload) = handle_with(3);
        let stream = IncomingStream::from(handle.clone());

        assert_eq!(stream.total_bytes(), handle.total_bytes());
        assert_eq!(stream.received_fragments(), handle.received_fragments());
        assert_eq!(stream.total_fragments(), handle.total_fragments());
        assert!(stream.is(&IncomingStream::from(handle.clone()).identity()));
        assert_eq!(stream.assemble().await.unwrap(), payload);

        let forked = stream.fork();
        assert_eq!(
            forked.assemble().await.unwrap(),
            payload,
            "fork reads it too"
        );
    }

    /// `chunks` yields the whole stream in order, one chunk per UDP
    /// fragment, and reading it consumes nothing: `assemble` and a second
    /// `chunks` still see all of it.
    #[tokio::test]
    async fn incoming_stream_chunks_read_the_stream_without_consuming_it() {
        use futures::StreamExt;
        let (handle, payload) = handle_with(3);
        let stream = IncomingStream::from(handle.clone());

        let chunks: Vec<_> = stream.chunks().collect().await;
        assert_eq!(chunks.len(), handle.received_fragments());
        let read: Vec<u8> = chunks
            .into_iter()
            .flat_map(|c| c.expect("chunk").to_vec())
            .collect();
        assert_eq!(read, payload);

        assert_eq!(stream.assemble().await.unwrap(), payload);
        let again: Vec<u8> = stream
            .chunks()
            .map(|c| c.expect("chunk").to_vec())
            .collect::<Vec<_>>()
            .await
            .concat();
        assert_eq!(again, payload);
    }

    /// After a cancel, `chunks` yields the error once and then ends, so a
    /// consumer that skips errors cannot spin on it.
    #[tokio::test]
    async fn incoming_chunks_end_after_an_error() {
        use futures::StreamExt;
        let handle = StreamHandle::new(StreamId::next(), 10_000);
        let stream = IncomingStream::from(handle);
        stream.cancel();
        let mut chunks = stream.chunks();
        assert!(matches!(
            chunks.next().await,
            Some(Err(StreamError::Cancelled))
        ));
        assert!(chunks.next().await.is_none());
        assert!(chunks.next().await.is_none());
    }

    /// On a stream still arriving, the declared length, the units received
    /// and the units expected are three different numbers, so none of them
    /// can stand in for another.
    #[test]
    fn incoming_stream_reports_a_partial_stream_as_partial() {
        let total = 3 * FRAGMENT_PAYLOAD_SIZE;
        let handle = StreamHandle::new(StreamId::next(), total as u64);
        handle
            .push_fragment(1, bytes::Bytes::from(vec![7u8; FRAGMENT_PAYLOAD_SIZE]))
            .unwrap();
        let stream = IncomingStream::from(handle.clone());

        assert_eq!(stream.total_bytes(), total as u64, "declared, not received");
        assert_eq!(handle.received_bytes(), FRAGMENT_PAYLOAD_SIZE as u64);
        assert_eq!(stream.received_fragments(), 1);
        assert_eq!(stream.total_fragments(), handle.total_fragments());
        assert!(stream.total_fragments() >= 3, "premise: more than received");
    }

    /// A stream's identity matches every handle on that stream, clones and
    /// forks included, and holding it does not keep the stream alive.
    #[test]
    fn stream_identity_names_one_stream_without_holding_it() {
        let (handle, _) = handle_with(1);
        let stream = IncomingStream::from(handle);
        let strong_before = stream.0.buffer_strong_count();
        let identity = stream.identity();
        assert_eq!(
            stream.0.buffer_strong_count(),
            strong_before,
            "an identity is not a reference to the stream"
        );
        assert!(stream.is(&identity));
        assert!(stream.clone().is(&identity));
        assert!(stream.fork().is(&identity), "a fork reads the same stream");
        let (other, _) = handle_with(1);
        assert!(!IncomingStream::from(other).is(&identity));
    }

    /// A stream does not claim another stream's buffer.
    #[test]
    fn incoming_stream_does_not_share_an_unrelated_buffer() {
        let (handle, _) = handle_with(1);
        let (other, _) = handle_with(1);
        let stream = IncomingStream::from(handle);
        assert!(!stream.is(&IncomingStream::from(other).identity()));
    }

    /// Cancelling through the boundary type cancels the stream itself.
    #[tokio::test]
    async fn incoming_stream_cancel_reaches_the_stream() {
        let handle = StreamHandle::new(StreamId::next(), 10_000);
        let stream = IncomingStream::from(handle.clone());
        let forked = stream.fork();
        assert!(!stream.is_cancelled());
        stream.cancel();
        assert!(handle.is_cancelled());
        assert!(stream.is_cancelled());
        assert!(
            !forked.is_cancelled(),
            "a fork is an independent reader: cancelling the original leaves it"
        );
        assert!(matches!(
            stream.assemble().await,
            Err(StreamError::Cancelled)
        ));
    }
}
