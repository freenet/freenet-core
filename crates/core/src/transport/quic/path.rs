//! A QUIC connection used as a transport path to one peer (#5940, step e3).
//!
//! The upgrade driver ([`super::upgrade::driver`]) builds a [`QuicPath`] in shadow
//! mode and sends only control frames on it; data sends wait for e5b.
//! Step e5 moves a peer's traffic onto one once the connection is
//! established, and keeps the UDP session as the fallback.
//!
//! Each message gets its own QUIC unidirectional stream in the format of
//! [`super::framing`]. quinn allows the peer [`MAX_UNI_STREAMS`] streams at a
//! time, so a sender with more messages in flight waits in `open_uni` until
//! the receiver has read one to its end: that limit is the path's
//! back-pressure.
//!
//! Sending: [`QuicPath::send_short_bounded`],
//! [`QuicPath::send_stream_bounded`] (from bytes),
//! [`QuicPath::send_stream_chunks_bounded`] (from a stream of chunks, such as
//! an inbound stream being piped onward) and
//! [`QuicPath::send_control_bounded`]. Each returns once the peer's QUIC stack
//! has acknowledged every byte, with the code the peer stopped the stream with
//! ([`QuicSendError::Stopped`]), or at its deadline ([`bounded`]).
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
//! connection alive). Every send the node makes goes through a
//! `send_*_bounded` method ([`bounded`]), which also says whether a timed-out
//! message may have been delivered; the forms without a deadline exist only
//! in tests.
//!
//! Cancel safety: dropping a send future before it returns resets the
//! stream ([`reset_code::SENDER_ABORTED`]), so the receiver sees
//! `ResetByPeer` and cancels its handle; it never sees a stream that ends
//! early as if it were complete or truncated. A dropped short message or
//! control frame is lost. A send future dropped while it waits for
//! acknowledgement (after `finish` was issued) is NOT reset: every byte
//! was already written, so the message may still be delivered:
//! [`bounded_send`] reports a timeout there as `MaybeDelivered`, not
//! `Failed`.
//!
//! Streamed messages, and short messages that do not fit a packet, take one
//! permit each, at most `config::STREAMED_SEND_LIMIT` at once, which keeps uni
//! stream slots for short messages and control frames on a steady path (see
//! that constant for what steady means and where a slot can be missing). The
//! stream holds its permit until it is acknowledged or stopped, also past a
//! deadline that reported `MaybeDelivered` (a task waits for the end), or
//! until it is reset. A send waiting for a permit ends when the connection
//! does, as `open_uni` does. A send from a source waits for the first chunk
//! before it takes a permit or opens a stream, so a source with nothing yet
//! holds neither.
//!
//! A message is written in pieces of at most `FRAGMENT_PAYLOAD_SIZE`, each
//! returning once quinn has buffered it, so the gap deadline
//! ([`SendDeadlines::gap`]) and a `StreamProgressObserver` follow flow control
//! as they follow the UDP sender's fragments.
//!
//! A path in shadow mode
//! ([`QuicPathLimits::SHADOW`]) refuses inbound data with
//! [`stop_code::NOT_ACCEPTING_DATA`] before charging anything.
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
//! - `Short` → [`QuicDataInbox::shorts`], a bounded channel;
//! - `Stream` → the inbound adapter ([`inbound`]), which feeds the payload
//!   into an ordinary [`StreamHandle`] and announces it on
//!   [`QuicDataInbox::streams`];
//! - `Control` → the path's control channel, bounded.
//!
//! A Short or Stream whose channel is full waits for room in its own reader
//! future (`reserve`, raced with the stream's age limit), not in the task: the
//! other readers and the accept loop carry on. A Short waits after its payload
//! is read and BEFORE the stream's end is read (reading the end frees the
//! stream in quinn and returns its credit), and the waiting reader keeps its
//! `RecvStream`, so at most `MAX_UNI_STREAMS` readers wait on a path, charged
//! or not, and a slow consumer slows the sender instead of losing messages the
//! sender has seen acknowledged. Past the age limit the stream is stopped
//! (`stop_code::EVICTED`), which the sender sees because the stream is still
//! open. A reader that is waiting when the path ends is dropped with the
//! receive task and counted (`ShortAbandonedWaiting`, `StreamAbandonedWaiting`). A Control frame still uses `try_send` and is
//! dropped and counted (`QuicInboundOutcome`) when its channel is full.
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

pub(crate) mod bounded;
pub(crate) mod inbound;

#[cfg(test)]
mod tests;

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use bytes::Bytes;
use futures::{Stream, StreamExt};
use tokio::sync::mpsc;

use super::QuicConnection;
use super::config::STREAMED_SEND_LIMIT;
use super::control::{ControlError, QuicControl};
use super::framing::{FrameHeader, HeaderError};
use super::identity::PeerKey;
use super::recycle::{self, SendAborts};
use crate::config::GlobalExecutor;
use crate::simulation::TimeSource;
use crate::transport::StreamId;
use crate::transport::boundary::StreamProgressObserver;
use crate::transport::metrics::{InboundStreamOutcome, QuicCloseCode};
use crate::transport::peer_connection::inbound_budget::SharedInboundBudget;
use crate::transport::peer_connection::streaming::StreamError;
#[cfg(test)]
use crate::transport::peer_connection::streaming::{
    MAX_INBOUND_STREAM_BYTES_PER_CONNECTION, MAX_INBOUND_STREAMS_PER_CONNECTION,
};
use crate::transport::peer_connection::streaming_buffer::FRAGMENT_PAYLOAD_SIZE;
use crate::transport::peer_connection::{embeds_metadata, fits_one_packet};

#[cfg_attr(not(test), allow(unused_imports))]
pub(crate) use bounded::{BoundedSend, MaybeDelivered, NotOpened, SendDeadlines, SendFailure};
use bounded::{SendProgress, bounded_send};
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
    /// The path carries control frames only (shadow mode): a Short or
    /// Stream frame is refused before anything is charged. Data may be sent
    /// on a path only after caps negotiation said the peer accepts it, so
    /// this is a counter of a sender that did not, and not a fallback signal
    /// a sender can rely on: a small message can be fully acknowledged before
    /// the stop arrives, and the sender then sees `Sent` for what was
    /// discarded. A sender that does see the stop has wrong gating: it logs
    /// it and sends on the UDP session, since a retry on the same path fails
    /// the same way.
    pub(crate) const NOT_ACCEPTING_DATA: u32 = 0x19;
    /// Every stop code, for the range and duplicate checks in
    /// `super::error`. A source-scrape test checks that each `const` in this
    /// module is listed.
    pub(crate) const ALL: [u32; 9] = [
        HEADER_REFUSED,
        BUDGET_REFUSED,
        STREAM_ID_IN_USE,
        CHANNEL_FULL,
        OVERLONG,
        EVICTED,
        CANCELLED,
        FRAGMENT_REFUSED,
        NOT_ACCEPTING_DATA,
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

/// Capacity of [`QuicDataInbox::shorts`]. A queued message of more than one
/// packet holds its inbound budget charge, so the budget bounds those bytes; a
/// one-packet message is uncharged and bounded by this capacity (about one
/// packet each) plus the readers waiting for room, at most `MAX_UNI_STREAMS`.
#[cfg_attr(not(test), allow(dead_code))]
pub(crate) const SHORT_CHANNEL_CAPACITY: usize = 256;
/// Capacity of [`QuicDataInbox::streams`]. Each queued stream holds its charge.
#[cfg_attr(not(test), allow(dead_code))]
pub(crate) const STREAM_CHANNEL_CAPACITY: usize = 256;
/// Capacity of the path's control channel; control frames are at most
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
    /// Capacity of a test path's own short channel. A node's data channels
    /// belong to its session ([`data_channels`]).
    #[cfg(test)]
    pub(crate) short_capacity: usize,
    /// Capacity of a test path's own stream channel; see `short_capacity`.
    #[cfg(test)]
    pub(crate) stream_capacity: usize,
    pub(crate) control_capacity: usize,
    /// Sends this side may reset before the path is closed
    /// ([`recycle::SEND_ABORT_LIMIT`]).
    pub(crate) max_send_aborts: u64,
    /// Whether the path accepts data (Short and Stream frames). A path in
    /// shadow mode ([`Self::SHADOW`]) carries control frames only and
    /// refuses data with [`stop_code::NOT_ACCEPTING_DATA`] right after the
    /// header, before anything is charged to the inbound budget.
    pub(crate) accept_data: bool,
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
        #[cfg(test)]
        short_capacity: SHORT_CHANNEL_CAPACITY,
        #[cfg(test)]
        stream_capacity: STREAM_CHANNEL_CAPACITY,
        control_capacity: CONTROL_CHANNEL_CAPACITY,
        max_send_aborts: recycle::SEND_ABORT_LIMIT,
        accept_data: true,
        #[cfg(test)]
        panic_in_receive: false,
    };

    /// [`Self::DEFAULT`] for a path in shadow mode: control frames only.
    #[cfg_attr(not(test), allow(dead_code))]
    pub(crate) const SHADOW: Self = Self {
        accept_data: false,
        ..Self::DEFAULT
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
        /// The path ended while a Short waited for room in `shorts`; the
        /// message was dropped with the receive task.
        ShortAbandonedWaiting => "short_abandoned_waiting",
        /// The path ended while a Stream waited for room in `streams`; the
        /// announcement was dropped with the receive task.
        StreamAbandonedWaiting => "stream_abandoned_waiting",
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
        /// A Short or Stream frame on a path that accepts no data (shadow
        /// mode); stopped before anything was charged.
        RefusedNotAcceptingData => "refused_not_accepting_data",
        /// A control payload of a version this node does not read.
        ControlUnknownVersion => "control_unknown_version",
        /// A control payload of a type this node does not know.
        ControlUnknownType => "control_unknown_type",
        /// A control payload that is empty or of the wrong length.
        ControlMalformed => "control_malformed",
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

    fn of_control_error(error: &ControlError) -> Self {
        match error {
            ControlError::UnknownVersion(_) => Self::ControlUnknownVersion,
            ControlError::UnknownType(_) => Self::ControlUnknownType,
            ControlError::Empty | ControlError::WrongLength(_) => Self::ControlMalformed,
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

/// Where the paths of one UDP session deliver data. The session holds these for
/// its lifetime and each path it builds takes a clone, so a message that arrived
/// before its path ended is still delivered. A reader still waiting for room
/// in a full channel when the path ends is not: it is dropped and counted.
#[derive(Clone)]
pub(in crate::transport) struct QuicDataSenders {
    shorts: mpsc::Sender<InboundShort>,
    streams: mpsc::Sender<InboundStreamArrival>,
}

/// The receiving ends of [`QuicDataSenders`], held by the connection's `recv`.
/// Dropping one makes later messages count as dropped (`*DroppedFull`), as a
/// full channel does.
pub(in crate::transport) struct QuicDataInbox {
    pub(in crate::transport) shorts: mpsc::Receiver<InboundShort>,
    pub(in crate::transport) streams: mpsc::Receiver<InboundStreamArrival>,
}

/// The data channels of one UDP session.
pub(in crate::transport) fn data_channels() -> (QuicDataSenders, QuicDataInbox) {
    channels_of(SHORT_CHANNEL_CAPACITY, STREAM_CHANNEL_CAPACITY)
}

fn channels_of(short_capacity: usize, stream_capacity: usize) -> (QuicDataSenders, QuicDataInbox) {
    let (shorts_tx, shorts) = mpsc::channel(short_capacity);
    let (streams_tx, streams) = mpsc::channel(stream_capacity);
    (
        QuicDataSenders {
            shorts: shorts_tx,
            streams: streams_tx,
        },
        QuicDataInbox { shorts, streams },
    )
}

/// The receiving ends of a test path, which has channels of its own. Dropping
/// one makes its messages count as dropped (`*DroppedFull`), as a full channel
/// does.
#[cfg(test)]
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
    /// One permit per streamed message in flight
    /// ([`STREAMED_SEND_LIMIT`]), so the remaining uni stream slots stay
    /// for short messages and control frames. Never closed.
    stream_slots: Arc<tokio::sync::Semaphore>,
    /// Whether this path may send data: both sides set the data bit
    /// ([`Self::enable_data`]). Off for a path in shadow, where a fragment taken
    /// from an inbound handle cannot outlive its charge because nothing is ever
    /// forwarded.
    send_data: std::sync::atomic::AtomicBool,
    /// Where the path's watch reports that the peer was heard from.
    heard: Arc<recycle::PeerHeard>,
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
        let (path, inbound) = Self::with_budget_and_own_channels(connection, time, limits, budget);
        // A test path has no negotiation: one that accepts data sends it too.
        if limits.accept_data {
            path.enable_data();
        }
        (path, inbound)
    }

    /// [`Self::with_budget`] with channels of the path's own, for tests.
    #[cfg(test)]
    pub(in crate::transport) fn with_budget_and_own_channels<T: TimeSource>(
        connection: QuicConnection,
        time: T,
        limits: QuicPathLimits,
        budget: SharedInboundBudget,
    ) -> (Self, QuicInbound) {
        let (data, inbox) = channels_of(limits.short_capacity, limits.stream_capacity);
        let (path, controls) = Self::with_budget(connection, time, limits, budget, data);
        let QuicDataInbox { shorts, streams } = inbox;
        (
            path,
            QuicInbound {
                shorts,
                streams,
                controls,
            },
        )
    }

    /// A path over `connection`, charging inbound streams to `budget` and
    /// delivering data to `data`, the session's channels; returns the receiver of
    /// its control frames. For `budget`, pass the peer's UDP connection's
    /// (`PeerConnection::inbound_budget`), so the peer has one reassembly
    /// allowance across both paths (the QUIC memory gate). `budget` brings
    /// its own limits; `limits.max_streams` and `limits.max_bytes` are not
    /// used. The path's charges are keyed `InboundKey::Quic` and
    /// `InboundKey::QuicShort`, so they never meet or release a UDP charge
    /// (its admission releases gone QUIC charges only; see
    /// `inbound_budget::Releaser`).
    ///
    /// The effects of sharing across the two paths' lifetimes:
    /// - Two QUIC paths on one budget at once (a replacement built before
    ///   the old one is gone) share the `Quic` namespace: a stream id held
    ///   by one is refused on the other as `StreamIdInUse`.
    /// - A UDP stream refused because this path filled the budget is
    ///   remembered as refused (`RefusedStreams`), and
    ///   `MAX_REMEMBERED_REFUSALS` outstanding refusals close the UDP
    ///   connection. That is UDP's own overload behaviour, but now the other
    ///   path can cause it.
    /// - A QUIC charge whose consumer never lets go blocks UDP admissions
    ///   too: the UDP sweep evicts only UDP streams, and the QUIC path's own
    ///   eviction cannot end a charge a consumer still holds. It is counted
    ///   (`InboundStreamOutcome::CompleteChargePastAge`).
    /// - A gone UDP charge holds room against this path until the UDP
    ///   connection's next 5 s `timeout_check` sweep (or its own admission
    ///   under pressure), so this path can refuse with `BUDGET_REFUSED` for
    ///   up to about 5 s on room that is free.
    /// - The path may close itself (`recycle`: `close_code::SEND_ABORT_LIMIT`
    ///   or `close_code::PEER_NOT_ACKNOWLEDGING`). That is a request to fall
    ///   back to UDP for this connection, not a permanent downgrade.
    /// - The shared `Arc` outlives either owner: this path keeps the UDP
    ///   connection's charges (and their room) after the `PeerConnection`
    ///   drops, and gone UDP charges are then released by nobody, since a
    ///   QUIC path never releases a UDP charge. A session's path does not
    ///   outlive its connection: dropping the connection ends the session.
    pub(in crate::transport) fn with_budget<T: TimeSource>(
        connection: QuicConnection,
        time: T,
        limits: QuicPathLimits,
        budget: SharedInboundBudget,
        data: QuicDataSenders,
    ) -> (Self, mpsc::Receiver<Bytes>) {
        let stats = Arc::new(QuicPathStats::new());
        let (controls_tx, controls) = mpsc::channel(limits.control_capacity);
        #[cfg_attr(not(test), allow(unused_mut))]
        let mut receiver = inbound::Receiver::new(
            time.clone(),
            budget.clone(),
            stats.clone(),
            data.shorts,
            data.streams,
            controls_tx,
            limits.accept_data,
        );
        #[cfg(test)]
        {
            receiver.panic_on_stream = limits.panic_in_receive;
        }
        let aborts = Arc::new(SendAborts::new(limits.max_send_aborts, connection.closer()));
        let heard = Arc::new(recycle::PeerHeard::default());
        let receive = GlobalExecutor::spawn(supervise_receive(
            connection.inner().clone(),
            connection.closer(),
            Arc::new(receiver),
            stats.clone(),
            time,
            connection.cap_holds(),
            heard.clone(),
        ));
        (
            Self {
                connection,
                stats,
                receive,
                aborts,
                stream_slots: Arc::new(tokio::sync::Semaphore::new(STREAMED_SEND_LIMIT as usize)),
                send_data: std::sync::atomic::AtomicBool::new(false),
                heard,
                #[cfg(test)]
                budget,
            },
            controls,
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

    /// The counters, outliving the path (tests read them after dropping it).
    #[cfg(test)]
    pub(crate) fn stats_handle(&self) -> Arc<QuicPathStats> {
        self.stats.clone()
    }

    /// Sends this side reset on this path so far.
    #[cfg(test)]
    pub(crate) fn send_aborts(&self) -> u64 {
        self.aborts.count()
    }

    /// Streamed-message permits free right now.
    #[cfg(test)]
    pub(crate) fn free_stream_slots(&self) -> usize {
        self.stream_slots.available_permits()
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

    /// Parse a control frame from the path's control channel. A payload this
    /// node refuses ([`ControlError`]) is counted on the path
    /// (`QuicInboundOutcome::Control*`) and returned, for the upgrade machine.
    /// This is the only place refusals are counted, so a consumer must decode
    /// through it and not through [`QuicControl::decode`].
    pub(crate) fn decode_control(&self, payload: &[u8]) -> Result<QuicControl, ControlError> {
        QuicControl::decode(payload).inspect_err(|error| {
            self.stats
                .record(QuicInboundOutcome::of_control_error(error));
            tracing::debug!(%error, "QUIC path: refusing a control frame");
        })
    }

    /// Close the connection with `code`, which the peer reads in its connection
    /// error.
    pub(in crate::transport) fn close(&self, code: QuicCloseCode) {
        self.connection.close(code);
    }

    /// The quinn connection, to wait for its end.
    pub(in crate::transport) fn quinn_connection(&self) -> quinn::Connection {
        self.connection.inner().clone()
    }

    /// Both sides set the data bit: this path may send data. Accepting data is
    /// separate, set by [`QuicPathLimits::accept_data`] when the path is built.
    pub(in crate::transport) fn enable_data(&self) {
        self.send_data
            .store(true, std::sync::atomic::Ordering::Release);
    }

    /// From now on, call `report` when the peer is heard from on this path
    /// ([`recycle::watch_acknowledgements`]).
    pub(in crate::transport) fn report_receipts_to(&self, report: Box<dyn Fn() + Send + Sync>) {
        self.heard.report_to(report);
    }

    /// Whether this path may send data.
    #[cfg(any(test, feature = "testing"))]
    pub(in crate::transport) fn sends_data(&self) -> bool {
        self.send_data.load(std::sync::atomic::Ordering::Acquire)
    }

    /// A data send must not happen on a shadow path. See `send_data`.
    fn debug_assert_carries_data(&self) {
        debug_assert!(
            self.send_data.load(std::sync::atomic::Ordering::Acquire),
            "a shadow QUIC path sends control frames only"
        );
    }

    /// Send one serialized message. Tests only, with no deadline; nodes use
    /// [`Self::send_short_bounded`].
    #[cfg(test)]
    pub(super) async fn send_short(&self, payload: &[u8]) -> Result<(), QuicSendError> {
        self.send_short_with(payload, SendProgress::new()).await
    }

    /// Send one serialized message, within `deadlines` on `time` ([`bounded`];
    /// a node passes [`SendDeadlines::short`]). One that does not fit a packet
    /// takes a streamed-message permit, as [`Self::send_stream_bounded`] does.
    #[cfg_attr(not(test), allow(dead_code))]
    pub(crate) async fn send_short_bounded<T: TimeSource>(
        &self,
        time: &T,
        deadlines: impl Into<SendDeadlines>,
        payload: &[u8],
    ) -> BoundedSend {
        bounded_send(time, deadlines, |progress| {
            self.send_short_with(payload, progress)
        })
        .await
    }

    async fn send_short_with(
        &self,
        payload: &[u8],
        progress: SendProgress,
    ) -> Result<(), QuicSendError> {
        self.debug_assert_carries_data();
        let header = FrameHeader::short(payload.len() as u64)?;
        let slot = if fits_one_packet(payload.len() as u64) {
            None
        } else {
            Some(self.stream_slot().await?)
        };
        let mut send = self.open(&header, &[], slot, progress).await?;
        for piece in payload.chunks(FRAGMENT_PAYLOAD_SIZE) {
            send.write_all(piece).await?;
        }
        send.finish().await
    }

    /// Send one control frame. Tests only, with no deadline; nodes use
    /// [`Self::send_control_bounded`].
    #[cfg(test)]
    pub(super) async fn send_control(&self, payload: &[u8]) -> Result<(), QuicSendError> {
        self.send_control_with(payload, SendProgress::new()).await
    }

    async fn send_control_with(
        &self,
        payload: &[u8],
        progress: SendProgress,
    ) -> Result<(), QuicSendError> {
        let header = FrameHeader::control(payload.len() as u64)?;
        let mut send = self.open(&header, &[], None, progress).await?;
        send.write_all(payload).await?;
        send.finish().await
    }

    /// Send `control` as a control frame, within `timeout` on `time`
    /// ([`bounded`]).
    #[cfg_attr(not(test), allow(dead_code))]
    pub(crate) async fn send_control_bounded<T: TimeSource>(
        &self,
        time: &T,
        timeout: Duration,
        control: QuicControl,
    ) -> BoundedSend {
        let payload = control.encode();
        bounded_send(time, timeout, |progress| {
            self.send_control_with(&payload[..], progress)
        })
        .await
    }

    /// [`Self::send_stream_bounded`] with no deadline. Tests only.
    #[cfg(test)]
    pub(super) async fn send_stream(
        &self,
        stream_id: StreamId,
        metadata: Option<&[u8]>,
        payload: Bytes,
    ) -> Result<(), QuicSendError> {
        self.send_stream_with(stream_id, metadata, payload, None, SendProgress::new())
            .await
    }

    /// Send stream `stream_id` (caller-named) with `payload`, announced by
    /// `metadata` ([`frame_metadata`] says what goes in the frame), within
    /// `deadlines` on `time` ([`bounded`]; a node passes
    /// [`SendDeadlines::stream`]). Takes one of the path's streamed-message
    /// permits ([`STREAMED_SEND_LIMIT`]), waiting for one if all are taken, and
    /// the stream holds it until it is acknowledged, stopped or reset.
    /// `observer` hears of each fragment-sized piece quinn has buffered.
    #[cfg_attr(not(test), allow(dead_code))]
    pub(crate) async fn send_stream_bounded<T: TimeSource>(
        &self,
        time: &T,
        deadlines: impl Into<SendDeadlines>,
        stream_id: StreamId,
        metadata: Option<&[u8]>,
        payload: Bytes,
        observer: Option<&dyn StreamProgressObserver>,
    ) -> BoundedSend {
        bounded_send(time, deadlines, |progress| {
            self.send_stream_with(stream_id, metadata, payload, observer, progress)
        })
        .await
    }

    async fn send_stream_with(
        &self,
        stream_id: StreamId,
        metadata: Option<&[u8]>,
        payload: Bytes,
        observer: Option<&dyn StreamProgressObserver>,
        progress: SendProgress,
    ) -> Result<(), QuicSendError> {
        self.debug_assert_carries_data();
        let metadata = frame_metadata(metadata);
        let header = FrameHeader::stream(stream_id, payload.len() as u64, metadata.len())?;
        let slot = self.stream_slot().await?;
        let mut send = self.open(&header, metadata, Some(slot), progress).await?;
        send.write_fragments(payload, observer).await?;
        send.finish().await
    }

    /// [`Self::send_stream_chunks_bounded`] with no deadline. Tests only.
    #[cfg(test)]
    pub(super) async fn send_stream_chunks<S>(
        &self,
        stream_id: StreamId,
        total_len: u64,
        metadata: Option<&[u8]>,
        mut chunks: S,
    ) -> Result<(), QuicSendError>
    where
        S: Stream<Item = Result<Bytes, StreamError>> + Unpin,
    {
        let first = chunks.next().await;
        self.send_stream_chunks_with(
            stream_id,
            total_len,
            metadata,
            first,
            chunks,
            None,
            SendProgress::new(),
        )
        .await
    }

    /// Send stream `stream_id` (caller-named) of `total_len` bytes from
    /// `chunks`, announced by `metadata` ([`frame_metadata`] says what goes
    /// in the frame), within `deadlines` on `time` ([`bounded`]; a node passes
    /// [`SendDeadlines::stream`]). If the source fails, or delivers more or
    /// fewer than `total_len` bytes, the stream is reset
    /// ([`reset_code::SENDER_ABORTED`], or [`reset_code::LENGTH_MISMATCH`] for a
    /// wrong length) and the receiver cancels its handle. No byte past
    /// `total_len` is written. Takes a streamed-message permit as
    /// [`Self::send_stream_bounded`] does, and tells `observer` the same.
    ///
    /// Waits for the first chunk before anything else, for as long as
    /// [`SendDeadlines::gap`] allows, so a source that has nothing yet holds no
    /// permit and no stream; `open` and `total` run from when it arrives. A
    /// source that ends or fails before its first chunk opens no stream and
    /// has nothing to reset. A later stall past the gap resets the stream.
    /// A chunk taken before a [`SendFailure::NotOpened`] is lost, so a caller
    /// sending another way starts again from a fresh source.
    #[cfg_attr(not(test), allow(dead_code))]
    #[allow(clippy::too_many_arguments)]
    pub(crate) async fn send_stream_chunks_bounded<T, S>(
        &self,
        time: &T,
        deadlines: impl Into<SendDeadlines>,
        stream_id: StreamId,
        total_len: u64,
        metadata: Option<&[u8]>,
        mut chunks: S,
        observer: Option<&dyn StreamProgressObserver>,
    ) -> BoundedSend
    where
        T: TimeSource,
        S: Stream<Item = Result<Bytes, StreamError>> + Unpin,
    {
        let deadlines = deadlines.into();
        let first = tokio::select! {
            first = chunks.next() => first,
            () = time.sleep_cancelable(deadlines.gap.unwrap_or(deadlines.total)) => {
                return BoundedSend::Failed(SendFailure::NotOpened(NotOpened::TimedOut));
            }
        };
        bounded_send(time, deadlines, |progress| {
            self.send_stream_chunks_with(
                stream_id, total_len, metadata, first, chunks, observer, progress,
            )
        })
        .await
    }

    #[allow(clippy::too_many_arguments)]
    async fn send_stream_chunks_with<S>(
        &self,
        stream_id: StreamId,
        total_len: u64,
        metadata: Option<&[u8]>,
        first: Option<Result<Bytes, StreamError>>,
        mut chunks: S,
        observer: Option<&dyn StreamProgressObserver>,
        progress: SendProgress,
    ) -> Result<(), QuicSendError>
    where
        S: Stream<Item = Result<Bytes, StreamError>> + Unpin,
    {
        self.debug_assert_carries_data();
        let metadata = frame_metadata(metadata);
        let header = FrameHeader::stream(stream_id, total_len, metadata.len())?;
        let first = match first {
            Some(Ok(chunk)) => chunk,
            Some(Err(error)) => return Err(QuicSendError::Source(error)),
            None => {
                return Err(QuicSendError::LengthMismatch {
                    declared: total_len,
                    delivered: 0,
                });
            }
        };
        let slot = self.stream_slot().await?;
        let mut send = self.open(&header, metadata, Some(slot), progress).await?;
        let mut delivered: u64 = 0;
        let mut next = Some(first);
        let failure = loop {
            let item = match next.take() {
                Some(chunk) => Some(Ok(chunk)),
                None => send.race(chunks.next()).await?,
            };
            match item {
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
                    send.write_fragments(chunk, observer).await?;
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

    /// One of the permits that cap streamed messages at
    /// [`STREAMED_SEND_LIMIT`]; waits while all are taken, or until the
    /// connection ends: the semaphore is never closed, so a waiter would
    /// otherwise sit out its deadline holding on to a path that has ended.
    async fn stream_slot(&self) -> Result<tokio::sync::OwnedSemaphorePermit, QuicSendError> {
        let connection = self.connection.inner();
        tokio::select! {
            permit = Arc::clone(&self.stream_slots).acquire_owned() => {
                permit.map_err(|_| QuicSendError::Closed)
            }
            error = connection.closed() => Err(error.into()),
        }
    }

    /// Open a uni stream and write the header and metadata. Waits for a
    /// stream credit when the peer's [`super::config::MAX_UNI_STREAMS`] are
    /// in use. The stream takes over `slot`.
    async fn open(
        &self,
        header: &FrameHeader,
        metadata: &[u8],
        slot: Option<tokio::sync::OwnedSemaphorePermit>,
        progress: SendProgress,
    ) -> Result<Outgoing, QuicSendError> {
        let stream = self.connection.inner().open_uni().await?;
        progress.mark_opened();
        let mut send = Outgoing::new(stream, self.aborts.clone(), slot, progress);
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
    heard: Arc<recycle::PeerHeard>,
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
        heard,
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
    /// Told of each write, and of the end of the stream ([`bounded_send`]).
    progress: SendProgress,
    /// The streamed-message permit, if this send takes one. Returned when the
    /// stream is acknowledged, stopped or reset.
    slot: Option<tokio::sync::OwnedSemaphorePermit>,
    /// The end of the stream was sent and its outcome is not known yet.
    awaiting_ack: bool,
}

type StoppedResult = Result<Option<quinn::VarInt>, quinn::StoppedError>;

impl Outgoing {
    fn new(
        send: quinn::SendStream,
        aborts: Arc<SendAborts>,
        slot: Option<tokio::sync::OwnedSemaphorePermit>,
        progress: SendProgress,
    ) -> Self {
        let stopped = Box::pin(send.stopped());
        Self {
            send,
            stopped,
            closed: false,
            aborts,
            progress,
            slot,
            awaiting_ack: false,
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

    /// Write `chunk` in pieces of at most `FRAGMENT_PAYLOAD_SIZE`, telling
    /// `observer` of each. Each returns once quinn has buffered it, so they
    /// follow flow control.
    async fn write_fragments(
        &mut self,
        mut chunk: Bytes,
        observer: Option<&dyn StreamProgressObserver>,
    ) -> Result<(), QuicSendError> {
        while !chunk.is_empty() {
            let fragment = chunk.split_to(chunk.len().min(FRAGMENT_PAYLOAD_SIZE));
            self.write_chunk(fragment).await?;
            if let Some(observer) = observer {
                observer.on_fragment_sent();
            }
        }
        Ok(())
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
            Ok(()) => {
                self.progress.step();
                Ok(())
            }
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
        self.progress.mark_finished();
        self.awaiting_ack = true;
        let stopped = (&mut self.stopped).await;
        self.awaiting_ack = false;
        match stopped {
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
    ///
    /// A stream dropped while it waits for its acknowledgement goes on to the
    /// end, so its permit waits with it, in a small task. There are at most
    /// [`STREAMED_SEND_LIMIT`] per path, and each ends with the connection.
    fn drop(&mut self) {
        self.reset(reset_code::SENDER_ABORTED);
        if !self.awaiting_ack {
            return;
        }
        let Some(slot) = self.slot.take() else {
            return;
        };
        let Ok(runtime) = tokio::runtime::Handle::try_current() else {
            return;
        };
        let stopped = std::mem::replace(&mut self.stopped, Box::pin(std::future::pending()));
        drop(runtime.spawn(async move {
            let _ended = stopped.await;
            drop(slot);
        }));
    }
}
