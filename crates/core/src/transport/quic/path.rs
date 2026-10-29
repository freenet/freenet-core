//! A QUIC connection used as a transport path to one peer (#5940, step e3).
//!
//! Dead code in this release: nothing in the node builds a [`QuicPath`] yet.
//! Step e5 moves a peer's traffic onto one once the connection is
//! established, and keeps the UDP session as the fallback.
//!
//! Each message gets its own QUIC unidirectional stream in the format of
//! [`super::framing`]. quinn allows the peer [`MAX_UNI_STREAMS`] streams at a
//! time, so a sender with more messages in flight waits in `open_uni` until
//! the receiver has read one to its end: that limit is the path's
//! back-pressure.
//!
//! Sending: [`QuicPath::send_short`], [`QuicPath::send_stream`] (from bytes),
//! [`QuicPath::send_stream_chunks`] (from a stream of chunks, such as an
//! inbound stream being piped onward) and [`QuicPath::send_control`]. Each
//! returns once the peer's QUIC stack has acknowledged every byte, or with
//! the code the peer stopped the stream with ([`QuicSendError::Stopped`]).
//! A stop is seen only if it arrives before every byte is acknowledged: a
//! message that fits in the flow-control window can be acknowledged before
//! the receiver reads its header, and its refusal then reads as success at
//! the sender, as a refused stream does on the UDP transport (refusals do
//! not change its ACKs). The receiver counts every refusal either way.
//! The same holds for a message the receiver drops because its channel is
//! full: it was acknowledged, so the sender sees `Ok`. A successful send
//! means the bytes reached the peer's QUIC stack, never that the peer
//! delivered them (e5: sent is not delivered).
//!
//! Sends can wait without limit against a peer that does not cooperate:
//! `open_uni` waits for a stream credit, a write waits for flow-control
//! credit, and the final wait for acknowledgement has no deadline of its
//! own (only quinn's connection idle timeout ends it, and keepalives keep a
//! connection alive). e5 callers MUST bound every send with a timeout.
//!
//! Cancel safety: dropping a send future before it returns resets the
//! stream ([`reset_code::SENDER_ABORTED`]), so the receiver sees
//! `ResetByPeer` and cancels its handle; it never sees a stream that ends
//! early as if it were complete or truncated. A dropped short message or
//! control frame is lost. A send future dropped while it waits for
//! acknowledgement (after `finish` was issued) is NOT reset: every byte
//! was already written, so the message may still be delivered. For e5, a
//! timeout that fires during that wait means "maybe delivered", not
//! "failed".
//!
//! Known quinn cost (quinn 0.11.12): `SendStream::stopped()` registers an
//! `Arc<Notify>` in the connection's `stopped` map, removed only when quinn
//! reports the stream Finished or Stopped. A stream this side resets gets
//! neither event (`reset_acked` emits none), so each locally reset stream
//! leaves one entry until the connection closes. Every send registers
//! (its writes race `stopped()`), so the entries are bounded by the sends
//! this side reset on the connection over its lifetime (a source that
//! failed, a cancelled send). A stop the peer sent is reported as Stopped
//! and removes its entry, so answering it with a reset adds none. The path
//! counts its resets and closes the connection past
//! [`recycle::SEND_ABORT_LIMIT`] (`close_code::SEND_ABORT_LIMIT`), so the
//! peer falls back to UDP and a new connection starts with an empty map. A
//! write that fails because the peer stopped the stream is not a reset of
//! this side's and is not counted.
//! Worth an upstream quinn issue (not filed).
//!
//! The receive task also runs the path's acknowledgement watch
//! ([`recycle::watch_acknowledgements`]): a peer that keeps sending but
//! acknowledges none of our packets is not a conforming QUIC peer (RFC 9000
//! 13.2.1 requires acknowledging ack-eliciting packets within
//! max_ack_delay), so the path is closed (`close_code::PEER_NOT_ACKNOWLEDGING`).
//!
//! Both recycle codes ask the peer to fall back to UDP for this connection
//! only, not for good. e5 constraint: after either, the pair may upgrade
//! again after its normal re-upgrade backoff; neither side may treat the
//! code as a permanent downgrade of the peer.
//!
//! Receiving: one task per connection accepts every uni stream and reads all
//! of them concurrently (no task per stream), parsing the header and then:
//! - `Short` → [`QuicInbound::shorts`], a bounded channel;
//! - `Stream` → the inbound adapter ([`inbound`]), which feeds the payload
//!   into an ordinary [`StreamHandle`] and announces it on
//!   [`QuicInbound::streams`];
//! - `Control` → [`QuicInbound::controls`], a bounded channel.
//!
//! The task only ever uses `try_send`: a full channel drops the message and
//! counts it (`QuicInboundOutcome`), it never waits, so a slow consumer
//! cannot stall the other streams of the connection.
//!
//! Not done here, on purpose:
//! - Dedup. The same message sent twice (once on UDP, once on QUIC after a
//!   fallback) is delivered twice. e5 dedups short messages with
//!   `PeerConnection::is_duplicate_dispatch` and streams with
//!   `completed_inbound_streams`, where both transports meet.
//! - Exporting the QUIC-specific counters. They are per path
//!   ([`QuicPath::stats`]); the refusals and evictions the UDP transport also
//!   has are counted in `TRANSPORT_METRICS` as well, under the same
//!   `InboundStreamOutcome` labels. Telemetry for the rest is e2/e5.
//!
//! [`MAX_UNI_STREAMS`]: super::config::MAX_UNI_STREAMS
//! [`StreamHandle`]: crate::transport::peer_connection::streaming::StreamHandle

pub(crate) mod inbound;

#[cfg(test)]
mod tests;

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use bytes::Bytes;
use futures::{Stream, StreamExt};
use tokio::sync::mpsc;

use super::QuicConnection;
use super::framing::{FrameHeader, HeaderError};
use super::identity::PeerKey;
use super::recycle::{self, SendAborts};
use crate::config::GlobalExecutor;
use crate::simulation::TimeSource;
use crate::transport::StreamId;
use crate::transport::metrics::{InboundStreamOutcome, QuicCloseCode};
use crate::transport::peer_connection::embeds_metadata;
use crate::transport::peer_connection::inbound_budget::SharedInboundBudget;
use crate::transport::peer_connection::streaming::StreamError;
#[cfg(test)]
use crate::transport::peer_connection::streaming::{
    MAX_INBOUND_STREAM_BYTES_PER_CONNECTION, MAX_INBOUND_STREAMS_PER_CONNECTION,
};

pub(crate) use inbound::{InboundShort, InboundStreamArrival};

/// Application error codes a receiver stops a stream with (STOP_SENDING),
/// and the sender sees as [`QuicSendError::Stopped`]. Range `0x10..=0x1f`,
/// apart from the close and reset codes (see `super::error`).
pub(crate) mod stop_code {
    /// The header was refused (`HeaderError`).
    pub(crate) const HEADER_REFUSED: u32 = 0x11;
    /// The stream did not fit the connection's inbound budget.
    pub(crate) const BUDGET_REFUSED: u32 = 0x12;
    /// A stream with this id is already held on this path.
    pub(crate) const STREAM_ID_IN_USE: u32 = 0x13;
    /// The channel the message goes to was full.
    pub(crate) const CHANNEL_FULL: u32 = 0x14;
    /// More bytes arrived than the header declared.
    pub(crate) const OVERLONG: u32 = 0x15;
    /// Evicted: idle too long, or older than its age limit.
    pub(crate) const EVICTED: u32 = 0x16;
    /// The stream's handle was cancelled on the receiving side (its
    /// consumer is gone).
    pub(crate) const CANCELLED: u32 = 0x17;
    /// The receiver's stream buffer refused a fragment (a shape it cannot
    /// hold; not expected). The same message fails the same way again.
    pub(crate) const FRAGMENT_REFUSED: u32 = 0x18;
    /// Every stop code, for the range and duplicate checks in
    /// `super::error`. A source-scrape test checks that each `const` in this
    /// module is listed.
    pub(crate) const ALL: [u32; 8] = [
        HEADER_REFUSED,
        BUDGET_REFUSED,
        STREAM_ID_IN_USE,
        CHANNEL_FULL,
        OVERLONG,
        EVICTED,
        CANCELLED,
        FRAGMENT_REFUSED,
    ];
}

/// Application error codes a sender resets a stream with (RESET_STREAM).
/// Range `0x20..=0x2f` (see `super::error`).
pub(crate) mod reset_code {
    /// The sender gave up: its chunk source failed, or the send was
    /// dropped.
    pub(crate) const SENDER_ABORTED: u32 = 0x21;
    /// The chunk source delivered a different number of bytes than the
    /// frame declared. The same message fails the same way again.
    pub(crate) const LENGTH_MISMATCH: u32 = 0x22;
    /// Every reset code (see `stop_code::ALL`).
    pub(crate) const ALL: [u32; 2] = [SENDER_ABORTED, LENGTH_MISMATCH];
}

/// Capacity of [`QuicInbound::shorts`]. Each queued message still holds its
/// inbound budget charge, so the budget bounds the bytes queued.
#[cfg_attr(not(test), allow(dead_code))]
pub(crate) const SHORT_CHANNEL_CAPACITY: usize = 256;
/// Capacity of [`QuicInbound::streams`]. Each queued stream holds its charge.
#[cfg_attr(not(test), allow(dead_code))]
pub(crate) const STREAM_CHANNEL_CAPACITY: usize = 256;
/// Capacity of [`QuicInbound::controls`]; control frames are at most
/// `MAX_CONTROL_BYTES` each.
#[cfg_attr(not(test), allow(dead_code))]
pub(crate) const CONTROL_CHANNEL_CAPACITY: usize = 16;

/// The receive-side limits of a path.
#[derive(Debug, Clone, Copy)]
pub(crate) struct QuicPathLimits {
    /// Inbound streams (Short and Stream) holding a budget charge at once,
    /// for the test-only [`QuicPath::with_limits`], which gives the path a
    /// budget of its own. A node path ([`QuicPath::with_budget`]) takes its
    /// limits from the budget, so this field does not exist outside tests.
    #[cfg(test)]
    pub(crate) max_streams: usize,
    /// Bytes of inbound budget charges at once; see `max_streams`.
    #[cfg(test)]
    pub(crate) max_bytes: u64,
    pub(crate) short_capacity: usize,
    pub(crate) stream_capacity: usize,
    pub(crate) control_capacity: usize,
    /// Sends this side may reset before the path is closed
    /// ([`recycle::SEND_ABORT_LIMIT`]).
    pub(crate) max_send_aborts: u64,
    /// The receive task panics on its first stream (supervisor test).
    #[cfg(test)]
    pub(crate) panic_in_receive: bool,
}

impl QuicPathLimits {
    /// The channel capacities above, and for a test path with a budget of
    /// its own, inbound limits equal to one peer's UDP allowance
    /// (`MAX_INBOUND_STREAMS_PER_CONNECTION`,
    /// `MAX_INBOUND_STREAM_BYTES_PER_CONNECTION`). A node path built with
    /// [`QuicPath::with_budget`] takes its inbound limits from the budget.
    #[cfg_attr(not(test), allow(dead_code))]
    pub(crate) const DEFAULT: Self = Self {
        #[cfg(test)]
        max_streams: MAX_INBOUND_STREAMS_PER_CONNECTION,
        #[cfg(test)]
        max_bytes: MAX_INBOUND_STREAM_BYTES_PER_CONNECTION,
        short_capacity: SHORT_CHANNEL_CAPACITY,
        stream_capacity: STREAM_CHANNEL_CAPACITY,
        control_capacity: CONTROL_CHANNEL_CAPACITY,
        max_send_aborts: recycle::SEND_ABORT_LIMIT,
        #[cfg(test)]
        panic_in_receive: false,
    };
}

/// Declares the path's outcome enum with `COUNT`, `ALL` and `label` from one
/// list, so the counter array cannot fall out of step with the variants.
macro_rules! quic_outcomes {
    ($(#[$emeta:meta])* enum $name:ident { $($(#[$meta:meta])* $variant:ident => $label:literal,)+ }) => {
        $(#[$emeta])*
        #[derive(Debug, Clone, Copy, PartialEq, Eq)]
        pub(crate) enum $name { $($(#[$meta])* $variant,)+ }

        #[allow(dead_code)]
        impl $name {
            pub(crate) const ALL: [Self; [$(Self::$variant),+].len()] = [$(Self::$variant),+];
            pub(crate) const COUNT: usize = Self::ALL.len();
            pub(crate) fn label(self) -> &'static str {
                match self { $(Self::$variant => $label,)+ }
            }
        }
    };
}

quic_outcomes! {
    /// What became of an inbound QUIC stream, beyond the refusals and
    /// evictions it shares with the UDP transport (`InboundStreamOutcome`).
    enum QuicInboundOutcome {
        /// A short message was queued on `shorts`.
        DeliveredShort => "delivered_short",
        /// A stream was announced on `streams` (it may still fail later).
        AnnouncedStream => "announced_stream",
        /// A stream received every declared byte and its end.
        CompletedStream => "completed_stream",
        /// A control frame was queued on `controls`.
        DeliveredControl => "delivered_control",
        /// The stream ended inside the header.
        HeaderTruncated => "header_truncated",
        /// Header refused: unknown version.
        HeaderUnknownVersion => "header_unknown_version",
        /// Header refused: unknown kind.
        HeaderUnknownKind => "header_unknown_kind",
        /// Header refused: declared length 0 or above `MAX_STREAM_TOTAL_BYTES`
        /// (also counted as `InboundStreamOutcome::RefusedInvalidLength`).
        HeaderInvalidLength => "header_invalid_length",
        /// Header refused: control frame length 0 or above the limit.
        HeaderInvalidControlLength => "header_invalid_control_length",
        /// Header refused: metadata above `MAX_METADATA_BYTES`.
        HeaderMetadataTooLong => "header_metadata_too_long",
        /// Header refused: metadata on a Short or Control frame.
        HeaderUnexpectedMetadata => "header_unexpected_metadata",
        /// Header refused: a stream id on a Short or Control frame.
        HeaderUnexpectedStreamId => "header_unexpected_stream_id",
        /// Header refused: a Stream id that is not caller-named.
        HeaderNotCallerNamed => "header_not_caller_named",
        /// A stream with no budget charge (still in its header, or a
        /// control frame) idle past the idle timeout.
        IdleUncharged => "idle_uncharged",
        /// A stream with no budget charge past `uncharged_age_limit`.
        AgedUncharged => "aged_uncharged",
        /// A Stream whose id is already held on this path.
        StreamIdInUse => "stream_id_in_use",
        /// `shorts` was full; the message was dropped.
        ShortDroppedFull => "short_dropped_full",
        /// `streams` was full; the stream was refused.
        StreamDroppedFull => "stream_dropped_full",
        /// `controls` was full; the frame was dropped.
        ControlDroppedFull => "control_dropped_full",
        /// The stream ended before its declared length.
        Truncated => "truncated",
        /// The stream carried more than its declared length.
        Overlong => "overlong",
        /// The sender reset the stream.
        ResetByPeer => "reset_by_peer",
        /// The connection was lost with the stream unfinished.
        ConnectionLost => "connection_lost",
        /// Its handle was cancelled on this side while it was being received.
        CancelledLocally => "cancelled_locally",
        /// Any other read error.
        ReadFailed => "read_failed",
        /// The receive task panicked; the connection was closed.
        ReceiveTaskPanicked => "receive_task_panicked",
    }
}

impl QuicInboundOutcome {
    fn of_header_error(error: &HeaderError) -> Self {
        match error {
            HeaderError::Truncated(_) => Self::HeaderTruncated,
            HeaderError::UnknownVersion(_) => Self::HeaderUnknownVersion,
            HeaderError::UnknownKind(_) => Self::HeaderUnknownKind,
            HeaderError::InvalidLength(_) => Self::HeaderInvalidLength,
            HeaderError::InvalidControlLength(_) => Self::HeaderInvalidControlLength,
            HeaderError::MetadataTooLong(_) => Self::HeaderMetadataTooLong,
            HeaderError::UnexpectedMetadata => Self::HeaderUnexpectedMetadata,
            HeaderError::UnexpectedStreamId => Self::HeaderUnexpectedStreamId,
            HeaderError::NotCallerNamed(_) => Self::HeaderNotCallerNamed,
        }
    }
}

/// Per-path counters. Tests read these, never the process-wide ones, which
/// every test in the process writes.
pub(crate) struct QuicPathStats {
    quic: [AtomicU64; QuicInboundOutcome::COUNT],
    shared: [AtomicU64; InboundStreamOutcome::COUNT],
}

impl QuicPathStats {
    fn new() -> Self {
        Self {
            quic: std::array::from_fn(|_| AtomicU64::new(0)),
            shared: std::array::from_fn(|_| AtomicU64::new(0)),
        }
    }

    fn record(&self, outcome: QuicInboundOutcome) {
        self.quic[outcome as usize].fetch_add(1, Ordering::Relaxed);
    }

    /// Count an outcome the UDP transport also has: here and process-wide.
    fn record_shared(&self, outcome: InboundStreamOutcome) {
        crate::transport::TRANSPORT_METRICS.record_inbound_stream_outcome(outcome);
        self.shared[outcome as usize].fetch_add(1, Ordering::Relaxed);
    }

    #[cfg_attr(not(test), allow(dead_code))]
    pub(crate) fn count(&self, outcome: QuicInboundOutcome) -> u64 {
        self.quic[outcome as usize].load(Ordering::Relaxed)
    }

    #[cfg_attr(not(test), allow(dead_code))]
    pub(crate) fn shared_count(&self, outcome: InboundStreamOutcome) -> u64 {
        self.shared[outcome as usize].load(Ordering::Relaxed)
    }
}

/// The receiving ends of a path. Dropping one makes its messages count as
/// dropped (`*DroppedFull`), as a full channel does.
#[cfg_attr(not(test), allow(dead_code))]
pub(crate) struct QuicInbound {
    pub(crate) shorts: mpsc::Receiver<InboundShort>,
    pub(crate) streams: mpsc::Receiver<InboundStreamArrival>,
    pub(crate) controls: mpsc::Receiver<Bytes>,
}

/// Why a send failed. All are scoped to the one message: the connection
/// may still carry others, except after `ConnectionLost`.
#[derive(Debug, thiserror::Error)]
pub(crate) enum QuicSendError {
    /// The header would be refused by the receiver (a length of 0 or above
    /// the limit, metadata too long, a stream id that is not caller-named).
    /// Not transient.
    #[error("invalid frame: {0}")]
    InvalidFrame(#[from] HeaderError),
    /// The chunk source delivered a different number of bytes than declared.
    /// The stream was reset.
    #[error("declared {declared} bytes, the source delivered {delivered}")]
    LengthMismatch { declared: u64, delivered: u64 },
    /// The chunk source failed. The stream was reset.
    #[error("the chunk source failed: {0}")]
    Source(StreamError),
    /// The receiver stopped the stream with this code ([`stop_code`]).
    #[error("the peer stopped the stream with code {0}")]
    Stopped(u64),
    #[error(transparent)]
    ConnectionLost(#[from] quinn::ConnectionError),
    /// The stream was closed locally (not expected).
    #[error("the stream was closed")]
    Closed,
}

impl From<quinn::WriteError> for QuicSendError {
    fn from(error: quinn::WriteError) -> Self {
        match error {
            quinn::WriteError::Stopped(code) => Self::Stopped(code.into_inner()),
            quinn::WriteError::ConnectionLost(error) => Self::ConnectionLost(error),
            quinn::WriteError::ClosedStream | quinn::WriteError::ZeroRttRejected => Self::Closed,
        }
    }
}

/// A QUIC connection to one peer, with the framing on top. Dropping it
/// closes the connection (through [`QuicConnection`]) and ends the receive
/// task; streams still being received are then cancelled.
pub(crate) struct QuicPath {
    connection: QuicConnection,
    stats: Arc<QuicPathStats>,
    receive: tokio::task::JoinHandle<()>,
    /// Sends this side reset; closes the connection past the limit.
    aborts: Arc<SendAborts>,
    #[cfg(test)]
    budget: SharedInboundBudget,
}

impl std::fmt::Debug for QuicPath {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("QuicPath")
            .field("connection", &self.connection)
            .finish_non_exhaustive()
    }
}

impl Drop for QuicPath {
    fn drop(&mut self) {
        self.receive.abort();
    }
}

impl QuicPath {
    /// A path over `connection` with the default limits and a budget of its
    /// own. Tests only; nodes must use [`Self::with_budget`]. `time` drives
    /// the idle and age limits of inbound streams; use the clock quinn runs
    /// on.
    #[cfg(test)]
    pub(crate) fn new<T: TimeSource>(connection: QuicConnection, time: T) -> (Self, QuicInbound) {
        Self::with_limits(connection, time, QuicPathLimits::DEFAULT)
    }

    /// [`Self::new`] with given limits.
    ///
    /// Tests only; nodes must use [`Self::with_budget`]. The path has its
    /// own inbound budget of `limits.max_streams` and `limits.max_bytes`, so
    /// a peer that also had a UDP session would hold two allowances.
    #[cfg(test)]
    pub(crate) fn with_limits<T: TimeSource>(
        connection: QuicConnection,
        time: T,
        limits: QuicPathLimits,
    ) -> (Self, QuicInbound) {
        let budget = SharedInboundBudget::new(limits.max_streams, limits.max_bytes);
        Self::with_budget(connection, time, limits, budget)
    }

    /// [`Self::with_limits`], charging inbound streams to `budget` instead
    /// of a budget of its own: pass the peer's UDP connection's
    /// (`PeerConnection::inbound_budget`), so the peer has one reassembly
    /// allowance across both paths (the QUIC memory gate). `budget` brings
    /// its own limits; `limits.max_streams` and `limits.max_bytes` are not
    /// used. The path's charges are keyed `InboundKey::Quic` and
    /// `InboundKey::QuicShort`, so they never meet or release a UDP charge
    /// (its admission releases gone QUIC charges only; see
    /// `inbound_budget::Releaser`).
    ///
    /// For e5, the effects of sharing across the two paths' lifetimes:
    /// - Two QUIC paths on one budget at once (a replacement built before
    ///   the old one is gone) share the `Quic` namespace: a stream id held
    ///   by one is refused on the other as `StreamIdInUse`.
    /// - If the UDP `PeerConnection` is replaced (reconnect or fallback)
    ///   while this path lives, the new connection gets a fresh budget and
    ///   the peer has two allowances again. Hand the path the new handle or
    ///   rebuild it on the new connection's budget.
    /// - A UDP stream refused because this path filled the budget is
    ///   remembered as refused (`RefusedStreams`), and
    ///   `MAX_REMEMBERED_REFUSALS` outstanding refusals close the UDP
    ///   connection. That is UDP's own overload behaviour, but now the other
    ///   path can cause it.
    /// - A QUIC charge whose consumer never lets go blocks UDP admissions
    ///   too: the UDP sweep evicts only UDP streams, and the QUIC path's own
    ///   eviction cannot end a charge a consumer still holds.
    /// - A gone UDP charge holds room against this path until the UDP
    ///   connection's next 5 s `timeout_check` sweep (or its own admission
    ///   under pressure), so this path can refuse with `BUDGET_REFUSED` for
    ///   up to about 5 s on room that is free.
    /// - The path may close itself (`recycle`: `close_code::SEND_ABORT_LIMIT`
    ///   or `close_code::PEER_NOT_ACKNOWLEDGING`). That is a request to fall
    ///   back to UDP for this connection, not a permanent downgrade: e5 must
    ///   allow a new upgrade after its normal backoff.
    /// - The shared `Arc` outlives either owner: this path keeps the UDP
    ///   connection's charges (and their room) after the `PeerConnection`
    ///   drops, and gone UDP charges are then released by nobody, since a
    ///   QUIC path never releases a UDP charge. Do not keep a path on the
    ///   handle of a connection that has gone.
    #[cfg_attr(not(test), allow(dead_code))]
    pub(in crate::transport) fn with_budget<T: TimeSource>(
        connection: QuicConnection,
        time: T,
        limits: QuicPathLimits,
        budget: SharedInboundBudget,
    ) -> (Self, QuicInbound) {
        let stats = Arc::new(QuicPathStats::new());
        let (shorts_tx, shorts) = mpsc::channel(limits.short_capacity);
        let (streams_tx, streams) = mpsc::channel(limits.stream_capacity);
        let (controls_tx, controls) = mpsc::channel(limits.control_capacity);
        #[cfg_attr(not(test), allow(unused_mut))]
        let mut receiver = inbound::Receiver::new(
            time.clone(),
            budget.clone(),
            stats.clone(),
            shorts_tx,
            streams_tx,
            controls_tx,
        );
        #[cfg(test)]
        {
            receiver.panic_on_stream = limits.panic_in_receive;
        }
        let aborts = Arc::new(SendAborts::new(limits.max_send_aborts, connection.closer()));
        let receive = GlobalExecutor::spawn(supervise_receive(
            connection.inner().clone(),
            connection.closer(),
            Arc::new(receiver),
            stats.clone(),
            time,
            connection.cap_holds(),
        ));
        (
            Self {
                connection,
                stats,
                receive,
                aborts,
                #[cfg(test)]
                budget,
            },
            QuicInbound {
                shorts,
                streams,
                controls,
            },
        )
    }

    /// The key the peer proved in the handshake.
    #[cfg_attr(not(test), allow(dead_code))]
    pub(crate) fn peer_key(&self) -> &PeerKey {
        self.connection.peer_key()
    }

    /// The receive side's counters.
    #[cfg_attr(not(test), allow(dead_code))]
    pub(crate) fn stats(&self) -> &QuicPathStats {
        &self.stats
    }

    /// Sends this side reset on this path so far.
    #[cfg(test)]
    pub(crate) fn send_aborts(&self) -> u64 {
        self.aborts.count()
    }

    /// Streams and bytes charged to the inbound budget (peer-wide when the
    /// budget is shared), after releasing the gone QUIC charges.
    #[cfg(test)]
    pub(crate) fn charged(&self) -> (usize, u64) {
        let mut budget = self.budget.lock();
        budget.release_gone(crate::transport::peer_connection::inbound_budget::Releaser::Quic);
        (budget.stream_count(), budget.charged_bytes())
    }

    /// Streams charged right now (peer-wide when the budget is shared),
    /// WITHOUT releasing gone charges first: what the receive side itself
    /// released.
    #[cfg(test)]
    pub(crate) fn charged_streams_now(&self) -> usize {
        self.budget.lock().stream_count()
    }

    /// The connection, for tests that write raw streams.
    #[cfg(test)]
    pub(crate) fn connection(&self) -> &QuicConnection {
        &self.connection
    }

    /// Send one serialized message.
    #[cfg_attr(not(test), allow(dead_code))]
    pub(crate) async fn send_short(&self, payload: &[u8]) -> Result<(), QuicSendError> {
        let header = FrameHeader::short(payload.len() as u64)?;
        let mut send = self.open(&header, &[]).await?;
        send.write_all(payload).await?;
        send.finish().await
    }

    /// Send one control frame.
    #[cfg_attr(not(test), allow(dead_code))]
    pub(crate) async fn send_control(&self, payload: &[u8]) -> Result<(), QuicSendError> {
        let header = FrameHeader::control(payload.len() as u64)?;
        let mut send = self.open(&header, &[]).await?;
        send.write_all(payload).await?;
        send.finish().await
    }

    /// Send stream `stream_id` (caller-named) with `payload`, announced by
    /// `metadata` ([`frame_metadata`] says what goes in the frame).
    #[cfg_attr(not(test), allow(dead_code))]
    pub(crate) async fn send_stream(
        &self,
        stream_id: StreamId,
        metadata: Option<&[u8]>,
        payload: Bytes,
    ) -> Result<(), QuicSendError> {
        let metadata = frame_metadata(metadata);
        let header = FrameHeader::stream(stream_id, payload.len() as u64, metadata.len())?;
        let mut send = self.open(&header, metadata).await?;
        send.write_chunk(payload).await?;
        send.finish().await
    }

    /// Send stream `stream_id` (caller-named) of `total_len` bytes from
    /// `chunks`, announced by `metadata` ([`frame_metadata`] says what goes
    /// in the frame). If the source fails, or delivers
    /// more or fewer than `total_len` bytes, the stream is reset
    /// ([`reset_code::SENDER_ABORTED`], or [`reset_code::LENGTH_MISMATCH`]
    /// for a wrong length) and the receiver cancels its handle.
    /// No byte past `total_len` is written.
    #[cfg_attr(not(test), allow(dead_code))]
    pub(crate) async fn send_stream_chunks<S>(
        &self,
        stream_id: StreamId,
        total_len: u64,
        metadata: Option<&[u8]>,
        mut chunks: S,
    ) -> Result<(), QuicSendError>
    where
        S: Stream<Item = Result<Bytes, StreamError>> + Unpin,
    {
        let metadata = frame_metadata(metadata);
        let header = FrameHeader::stream(stream_id, total_len, metadata.len())?;
        let mut send = self.open(&header, metadata).await?;
        let mut delivered: u64 = 0;
        let failure = loop {
            match send.race(chunks.next()).await? {
                None if delivered == total_len => break None,
                None => {
                    break Some(QuicSendError::LengthMismatch {
                        declared: total_len,
                        delivered,
                    });
                }
                Some(Err(error)) => break Some(QuicSendError::Source(error)),
                Some(Ok(chunk)) => {
                    let len = chunk.len() as u64;
                    if delivered.saturating_add(len) > total_len {
                        break Some(QuicSendError::LengthMismatch {
                            declared: total_len,
                            delivered: delivered.saturating_add(len),
                        });
                    }
                    delivered += len;
                    send.write_chunk(chunk).await?;
                }
            }
        };
        match failure {
            None => send.finish().await,
            Some(error) => {
                let code = match &error {
                    QuicSendError::LengthMismatch { .. } => reset_code::LENGTH_MISMATCH,
                    QuicSendError::InvalidFrame(_)
                    | QuicSendError::Source(_)
                    | QuicSendError::Stopped(_)
                    | QuicSendError::ConnectionLost(_)
                    | QuicSendError::Closed => reset_code::SENDER_ABORTED,
                };
                send.abort(code);
                Err(error)
            }
        }
    }

    /// Open a uni stream and write the header and metadata. Waits for a
    /// stream credit when the peer's [`super::config::MAX_UNI_STREAMS`] are
    /// in use.
    async fn open(&self, header: &FrameHeader, metadata: &[u8]) -> Result<Outgoing, QuicSendError> {
        let mut send = Outgoing::new(
            self.connection.inner().open_uni().await?,
            self.aborts.clone(),
        );
        send.write_all(&header.encode()).await?;
        if !metadata.is_empty() {
            send.write_all(metadata).await?;
        }
        Ok(send)
    }
}

/// The metadata a `Stream` frame carries: `metadata` if the UDP transport
/// would embed it in fragment #1 ([`embeds_metadata`], at most
/// `MAX_METADATA_BYTES` = 1120 bytes), else none, as `send_stream` does on
/// UDP. As there, the caller sends the announcing message separately as a
/// short message too, and that copy carries any length.
///
/// Empty metadata is sent as none (`metadata_len` 0) and arrives as
/// `None`, deliberately: the header has no other way to mark "present but
/// empty", and an empty announcing message carries nothing to dispatch.
/// (UDP would deliver `Some` of zero bytes.)
fn frame_metadata(metadata: Option<&[u8]>) -> &[u8] {
    metadata
        .filter(|metadata| embeds_metadata(metadata.len()))
        .unwrap_or_default()
}

/// Run the receive task, and close the connection if it ends while the
/// connection is still open. It ends on its own only when the connection
/// has ended; any other end (a panic) leaves a connection on which nothing
/// would be read again, so it is counted and the connection is closed with
/// `close_code::DOWNGRADE`: the QUIC path is broken on this side, and the
/// peer should fall back to the UDP session. Dropping the path aborts this
/// task, which is not counted.
///
/// The path's acknowledgement watch ([`recycle::watch_acknowledgements`])
/// runs beside it in this task, with its own panic containment: a panic in
/// the watch is logged and the receive task goes on. When the watch closes
/// the connection, the receive task then ends as the connection does.
async fn supervise_receive<T: TimeSource>(
    connection: quinn::Connection,
    closer: super::QuicCloser,
    receiver: Arc<inbound::Receiver<T>>,
    stats: Arc<QuicPathStats>,
    time: T,
    cap_holds: Arc<super::socket::CapHolds>,
) {
    use futures::FutureExt;
    let mut receive = std::pin::pin!(
        std::panic::AssertUnwindSafe(inbound::receive(connection.clone(), receiver)).catch_unwind()
    );
    let watch = std::panic::AssertUnwindSafe(recycle::watch_acknowledgements(
        connection.clone(),
        closer.clone(),
        time,
        cap_holds,
    ))
    .catch_unwind();
    let ended = tokio::select! {
        ended = &mut receive => ended,
        watched = watch => {
            if watched.is_err() {
                // The path still works; only guard 2 is gone for it.
                tracing::error!(
                    remote = %connection.remote_address(),
                    "QUIC path: the acknowledgement watch panicked; the path stays \
                     up without it"
                );
            }
            receive.await
        }
    };
    if ended.is_err() {
        stats.record(QuicInboundOutcome::ReceiveTaskPanicked);
        tracing::error!(
            remote = %connection.remote_address(),
            "QUIC path: the receive task panicked; closing the connection"
        );
    }
    // Only if still open and not already being closed (QuicCloser).
    closer.close(QuicCloseCode::Downgrade);
}

/// A uni stream being written that sees the peer stop it even while a
/// write is blocked.
///
/// quinn (quinn-proto 0.11.19) reports STOP_SENDING to a blocked writer
/// only when the block is the stream's own flow control: a write blocked on
/// the CONNECTION's flow control returns `Blocked` before the stopped state
/// is checked, so it waits forever. The stream is then never reset, the
/// receiver never learns its final size, and the connection-level credit
/// its bytes hold is never returned, which blocks every other stream on the
/// connection. So every write races the stream's `stopped()` future and a
/// stop resets the stream at once.
struct Outgoing {
    send: quinn::SendStream,
    stopped: Pin<Box<dyn Future<Output = StoppedResult> + Send + Sync>>,
    /// Finished or reset: dropping it then sends nothing more.
    closed: bool,
    /// The path's count of sends reset on this side.
    aborts: Arc<SendAborts>,
}

type StoppedResult = Result<Option<quinn::VarInt>, quinn::StoppedError>;

impl Outgoing {
    fn new(send: quinn::SendStream, aborts: Arc<SendAborts>) -> Self {
        let stopped = Box::pin(send.stopped());
        Self {
            send,
            stopped,
            closed: false,
            aborts,
        }
    }

    /// Run `fut` unless the peer stops the stream first. Everything that
    /// waits while the stream is open goes through here: the writes, and
    /// waiting for the next chunk of a source, so a stopped stream is reset
    /// at once instead of holding its uni-stream credit (and, for a blocked
    /// write, the connection's flow-control credit).
    async fn race<F: Future>(&mut self, fut: F) -> Result<F::Output, QuicSendError> {
        tokio::select! {
            out = fut => Ok(out),
            stopped = &mut self.stopped => Err(self.stopped_early(stopped)),
        }
    }

    async fn write_all(&mut self, buf: &[u8]) -> Result<(), QuicSendError> {
        let Self { send, stopped, .. } = self;
        let written = tokio::select! {
            written = send.write_all(buf) => written,
            result = stopped => return Err(self.stopped_early(result)),
        };
        self.written(written)
    }

    async fn write_chunk(&mut self, chunk: Bytes) -> Result<(), QuicSendError> {
        let Self { send, stopped, .. } = self;
        let written = tokio::select! {
            written = send.write_chunk(chunk) => written,
            result = stopped => return Err(self.stopped_early(result)),
        };
        self.written(written)
    }

    /// The outcome of a write. A stop the peer sent is handled as in
    /// [`Self::stopped_early`]: on STOP_SENDING both the write and the
    /// `stopped` future become ready, and `select!` may pick the write.
    /// Returned through `?` instead, the stream would be left open and
    /// `Drop` would reset it as an abort of this side's own
    /// ([`SendAborts`]), which it is not.
    fn written(&mut self, written: Result<(), quinn::WriteError>) -> Result<(), QuicSendError> {
        match written {
            Ok(()) => Ok(()),
            Err(quinn::WriteError::Stopped(code)) => Err(self.stopped_early(Ok(Some(code)))),
            Err(
                error @ (quinn::WriteError::ConnectionLost(_)
                | quinn::WriteError::ClosedStream
                | quinn::WriteError::ZeroRttRejected),
            ) => Err(error.into()),
        }
    }

    /// The peer stopped the stream (or the connection ended) mid-write:
    /// reset it so its connection-level credit comes back.
    fn stopped_early(&mut self, stopped: StoppedResult) -> QuicSendError {
        self.closed = true;
        match stopped {
            Ok(Some(code)) => {
                self.send.reset(code).ok();
                QuicSendError::Stopped(code.into_inner())
            }
            // Only after a finish, which has not happened.
            Ok(None) => QuicSendError::Closed,
            Err(quinn::StoppedError::ConnectionLost(error)) => QuicSendError::ConnectionLost(error),
            Err(quinn::StoppedError::ZeroRttRejected) => QuicSendError::Closed,
        }
    }

    /// Abandon the stream with `code` (a [`reset_code`]).
    fn abort(mut self, code: u32) {
        self.reset(code);
    }

    /// Reset the stream on this side, and count it ([`SendAborts`]) if the
    /// reset was sent. Not used for a stop the peer sent
    /// ([`Self::stopped_early`]): quinn reports that stream as Stopped, so
    /// it leaves nothing in quinn's `stopped` map.
    fn reset(&mut self, code: u32) {
        if !self.closed {
            self.closed = true;
            if self.send.reset(code.into()).is_ok() {
                self.aborts.record();
            }
        }
    }

    /// End the stream and wait until the peer has acknowledged every byte or
    /// stopped it.
    async fn finish(mut self) -> Result<(), QuicSendError> {
        self.closed = true;
        self.send.finish().map_err(|_| QuicSendError::Closed)?;
        match (&mut self.stopped).await {
            Ok(None) => Ok(()),
            Ok(Some(code)) => Err(QuicSendError::Stopped(code.into_inner())),
            Err(quinn::StoppedError::ConnectionLost(error)) => {
                Err(QuicSendError::ConnectionLost(error))
            }
            Err(quinn::StoppedError::ZeroRttRejected) => Err(QuicSendError::Closed),
        }
    }
}

impl Drop for Outgoing {
    /// A stream dropped before it was finished (a send future cancelled
    /// mid-message, or an error return) is reset. quinn's own drop would
    /// FINISH it, and the receiver would take a cut-short message for one
    /// that simply ended early.
    fn drop(&mut self) {
        self.reset(reset_code::SENDER_ABORTED);
    }
}
