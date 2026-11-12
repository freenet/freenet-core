//! A QUIC connection used as a transport path to one peer (#5940, step e3).
//!
//! The upgrade driver ([`super::upgrade::driver`]) builds a [`QuicPath`] and sends
//! control frames on it. With both sides' data bit a peer's traffic moves onto
//! the path ([`super::upgrade::data_route`]), and the UDP session stays up as the
//! fallback ([`route`] says when a message goes back to it).
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
//! acknowledgement (after `finish` was issued) is not reset: every byte
//! was already written, so the message may still be delivered:
//! [`bounded_send`] reports a timeout there as `MaybeDelivered`, not
//! `Failed`. The exception is a one-packet send that holds no permit once
//! [`ACK_WAITS_WITHOUT_PERMIT`] such sends already wait on the path: its
//! stream is reset so quinn drops its data with its epoch, and the message
//! may then not arrive (`Outgoing`'s drop).
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
//! other readers and the accept loop carry on. A waiting reader reads on (a
//! byte past the declared end is `Overlong` at once), so quinn holds nothing
//! for it, and reading the stream's end frees quinn's slot. A charged message
//! is bounded by its charge; a one-packet Short, uncharged, lowers the peer's
//! stream limit by one while it waits (`inbound::StreamLimit`), so at most
//! `MAX_UNI_STREAMS` of them wait on a path, and a slow consumer slows the
//! sender instead of losing messages the sender has seen acknowledged. Past
//! the age limit the stream is stopped
//! (`stop_code::EVICTED`). A sender still waiting for its acknowledgement sees
//! the stop; one whose bytes were all acknowledged has already reported `Sent`,
//! and the message is lost, as a message dropped from a full channel is on UDP.
//! A reader that is waiting when the path ends is dropped with the
//! receive task and counted (`ShortAbandonedWaiting`, `StreamAbandonedWaiting`). A Control frame still uses `try_send` and is
//! dropped and counted (`QuicInboundOutcome`) when its channel is full.
//!
//! Not done here, on purpose:
//! - Dedup. A message goes on UDP only when the outcome proves the receiver
//!   cannot deliver it ([`route`]), so a Short needs none. A stream id on both
//!   paths is refused where both transports meet (`completed_inbound_streams`).
//!
//! Counts: per path ([`QuicPath::stats`]), and node-wide in the same step, in
//! `metrics::quic_data` (exported as `quic_data_*`, `quic_inbound_*` and the
//! rest listed there). The refusals and evictions the UDP transport also has are
//! also counted under the shared `InboundStreamOutcome` labels.
//!
//! [`MAX_UNI_STREAMS`]: super::config::MAX_UNI_STREAMS
//! [`StreamHandle`]: crate::transport::peer_connection::streaming::StreamHandle

pub(crate) mod bounded;
pub(crate) mod inbound;
pub(in crate::transport) mod pacer;

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicU8, AtomicU64, AtomicUsize, Ordering};
use std::time::Duration;

use bytes::Bytes;
use futures::{Stream, StreamExt};
use tokio::sync::mpsc;

use super::QuicConnection;
use super::config::{MAX_UNI_STREAMS, STREAMED_SEND_LIMIT};
use super::control::{ControlError, QuicControl};
use super::framing::{FrameHeader, HeaderError};
use super::identity::PeerKey;
use super::recycle::{self, SendAborts};
use crate::config::GlobalExecutor;
use crate::simulation::TimeSource;
use crate::transport::StreamId;
use crate::transport::boundary::StreamProgressObserver;
use crate::transport::metrics::{InboundStreamOutcome, QuicCloseCode, QuicDataCounters};
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
use bounded::{ConnectionProgress, SendProgress, bounded_send};
pub(crate) use inbound::{InboundShort, InboundStreamArrival};
#[cfg(test)]
pub(crate) use route::STALLED_SENDS_LIMIT;
pub(crate) use route::{NextStep, QuicSendEvent, QuicSendResult, SendKind};

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
    /// A code's name in telemetry (`quic_data_stop_<label>`); `unknown` for a
    /// code not in [`ALL`].
    pub(crate) fn label(code: u32) -> &'static str {
        match code {
            HEADER_REFUSED => "header_refused",
            BUDGET_REFUSED => "budget_refused",
            STREAM_ID_IN_USE => "stream_id_in_use",
            CHANNEL_FULL => "channel_full",
            OVERLONG => "overlong",
            EVICTED => "evicted",
            CANCELLED => "cancelled",
            FRAGMENT_REFUSED => "fragment_refused",
            NOT_ACCEPTING_DATA => "not_accepting_data",
            _ => "unknown",
        }
    }

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
/// packet each) plus the readers waiting for room, at most `MAX_UNI_STREAMS`
/// (`inbound::StreamLimit`).
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
    /// The uni streams a data path's peer may open at first, raised with
    /// load up to [`MAX_UNI_STREAMS`] (`inbound::StreamLimit`).
    pub(crate) start_streams: u32,
    /// Where the path's counts are also kept node-wide; `None` is
    /// `TRANSPORT_METRICS`. A test passes its own, since every test in a process
    /// writes the global one.
    pub(crate) counters: Option<&'static QuicDataCounters>,
    /// The receive task panics on its first short message (supervisor test).
    #[cfg(test)]
    pub(crate) panic_in_receive: bool,
    /// The acknowledgement watch panics once the path is in a loss-rate bucket
    /// (supervisor test).
    #[cfg(test)]
    pub(crate) panic_in_watch: bool,
    /// The receive side's gap cap (`inbound::MAX_GAPS_PER_CONNECTION`), lower
    /// in a test that trips it.
    #[cfg(test)]
    pub(crate) max_gaps: usize,
    /// The cap of the connection's staged bytes for a test path with no
    /// buffer budget (`inbound::ConnStaging`); `None` keeps the default
    /// ([`staging_cap_without_meter`]). A metered path's follows its credit.
    #[cfg(test)]
    pub(crate) staging_cap: Option<usize>,
    /// While the flag is set, the receive side cancels the handle of the
    /// next Stream it admits, before its announce, and clears the flag: a
    /// test of how a stream cancelled that early ends.
    #[cfg(test)]
    pub(crate) cancel_next_admitted: Option<&'static std::sync::atomic::AtomicBool>,
}

/// The cap of a connection's staged bytes on a path with no buffer budget
/// meter: twice the credit its connection starts with. A shadow path keeps the
/// endpoint's initial window (`config::INITIAL_RECEIVE_WINDOW_BYTES`); a data
/// path without a meter (tests only: every node path has the budget) is taken
/// to run at the floor window.
fn staging_cap_without_meter(accept_data: bool) -> usize {
    let credit = if accept_data {
        super::budget::MIN_WINDOW_BYTES
    } else {
        super::config::INITIAL_RECEIVE_WINDOW_BYTES
    };
    super::budget::staging_cap(credit)
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
        // The production start in every build, tests and simulations
        // included; a test whose premise is the full limit sets it.
        start_streams: super::config::DATA_START_UNI_STREAMS,
        counters: None,
        #[cfg(test)]
        panic_in_receive: false,
        #[cfg(test)]
        panic_in_watch: false,
        #[cfg(test)]
        max_gaps: inbound::MAX_GAPS_PER_CONNECTION,
        #[cfg(test)]
        staging_cap: None,
        #[cfg(test)]
        cancel_next_admitted: None,
    };

    /// [`Self::DEFAULT`] for a path in shadow mode: control frames only.
    #[cfg_attr(not(test), allow(dead_code))]
    pub(crate) const SHADOW: Self = Self {
        accept_data: false,
        ..Self::DEFAULT
    };

    /// [`Self::DEFAULT`] with the peer let open every stream from the
    /// start, for tests whose premise is the full limit. Tests only.
    #[cfg(test)]
    pub(crate) const FULL_STREAMS: Self = Self {
        start_streams: MAX_UNI_STREAMS,
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
        /// The first refused data frame closed the path with DOWNGRADE (once
        /// per path).
        ClosedRefusingData => "closed_refusing_data",
        /// A control payload of a version this node does not read.
        ControlUnknownVersion => "control_unknown_version",
        /// A control payload of a type this node does not know.
        ControlUnknownType => "control_unknown_type",
        /// A control payload that is empty or of the wrong length.
        ControlMalformed => "control_malformed",
        /// A Short past one packet did not fit the inbound budget
        /// (`stop_code::BUDGET_REFUSED`; also counted under the shared refusal).
        ShortRefusedBudget => "short_refused_budget",
        /// A Stream did not fit the inbound budget (as above).
        StreamRefusedBudget => "stream_refused_budget",
        /// The connection's streams held more gaps (received ranges past the
        /// first, per stream) than the connection's cap (`budget::gap_cap`); the
        /// stream that went past it was stopped (`stop_code::EVICTED`).
        TooManyRanges => "too_many_ranges",
        /// The connection went past its gap cap a
        /// second time within `inbound::STAGED_TRIP_WINDOW`, so the path was
        /// closed with OVER_BUDGET (once per path). The gaps of streams it
        /// stopped stay counted (`inbound::ConnRanges::keep`).
        ClosedTooManyRanges => "closed_too_many_ranges",
        /// Staging what the stream had read before its header, metadata or
        /// control body was taken would have passed the connection's staging
        /// cap (`inbound::ConnStaging`), so the stream was stopped
        /// (`stop_code::EVICTED`).
        EvictedStaged => "evicted_staged",
        /// A second stream passed the staging cap within
        /// `inbound::STAGED_TRIP_WINDOW`, so the path was closed with
        /// OVER_BUDGET (once per path).
        ClosedStagedOverCap => "closed_staged_over_cap",
        /// A stream reached its age limit while its idle timer was paused
        /// because the buffer budget was throttling the connection
        /// (`budget::ConnMeter::throttled_since`); stopped
        /// (`stop_code::EVICTED`), and not counted as an idle or age eviction.
        ThrottledPastAge => "throttled_past_age",
        /// The peer's stream limit doubled with load (`inbound::StreamLimit`).
        StreamLimitRaised => "stream_limit_raised",
    }
}

mod route;

#[cfg(test)]
pub(in crate::transport) mod tests;

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
    /// Where the counts are also kept node-wide: `TRANSPORT_METRICS`, or a
    /// test's own ([`QuicPathLimits::counters`]).
    node: &'static QuicDataCounters,
    quic: [AtomicU64; QuicInboundOutcome::COUNT],
    shared: [AtomicU64; InboundStreamOutcome::COUNT],
    /// Data sends by [`SendKind`] and [`QuicSendResult`].
    sends: [[AtomicU64; QuicSendResult::COUNT]; SendKind::ALL.len()],
    send_events: [AtomicU64; QuicSendEvent::COUNT],
    /// Stops the peer sent to data sends, in the order of [`stop_code::ALL`].
    stops: [AtomicU64; stop_code::ALL.len()],
}

impl QuicPathStats {
    fn new(node: &'static QuicDataCounters) -> Self {
        Self {
            node,
            quic: std::array::from_fn(|_| AtomicU64::new(0)),
            shared: std::array::from_fn(|_| AtomicU64::new(0)),
            sends: std::array::from_fn(|_| std::array::from_fn(|_| AtomicU64::new(0))),
            send_events: std::array::from_fn(|_| AtomicU64::new(0)),
            stops: std::array::from_fn(|_| AtomicU64::new(0)),
        }
    }

    /// Count an outcome here and node-wide (`quic_inbound_<label>`).
    fn record(&self, outcome: QuicInboundOutcome) {
        self.node.record_inbound(outcome);
        self.quic[outcome as usize].fetch_add(1, Ordering::Relaxed);
    }

    /// Count an outcome the UDP transport also has: here and process-wide.
    fn record_shared(&self, outcome: InboundStreamOutcome) {
        crate::transport::TRANSPORT_METRICS.record_inbound_stream_outcome(outcome);
        self.shared[outcome as usize].fetch_add(1, Ordering::Relaxed);
    }

    /// The data-send counts are kept here and node-wide (`metrics::quic_data`).
    fn record_send(&self, kind: SendKind, result: QuicSendResult) {
        self.node.record_send(kind, result);
        self.sends[kind as usize][result as usize].fetch_add(1, Ordering::Relaxed);
    }

    /// Count something else that happened to a data send.
    pub(crate) fn record_send_event(&self, event: QuicSendEvent) {
        self.node.record_event(event);
        self.send_events[event as usize].fetch_add(1, Ordering::Relaxed);
    }

    /// Count a stop from the peer. A code this node does not know is counted only
    /// as a dropped send.
    fn record_stop(&self, code: u64) {
        self.node.record_stop(code);
        if let Some(index) = stop_code::ALL.iter().position(|c| u64::from(*c) == code) {
            self.stops[index].fetch_add(1, Ordering::Relaxed);
        }
    }

    #[cfg_attr(not(test), allow(dead_code))]
    pub(crate) fn send_count(&self, kind: SendKind, result: QuicSendResult) -> u64 {
        self.sends[kind as usize][result as usize].load(Ordering::Relaxed)
    }

    /// Data sends of every kind that ended in `result`.
    #[cfg_attr(not(any(test, feature = "testing")), allow(dead_code))]
    pub(crate) fn send_total(&self, result: QuicSendResult) -> u64 {
        SendKind::ALL
            .iter()
            .map(|kind| self.send_count(*kind, result))
            .sum()
    }

    #[cfg_attr(not(test), allow(dead_code))]
    pub(crate) fn send_event_count(&self, event: QuicSendEvent) -> u64 {
        self.send_events[event as usize].load(Ordering::Relaxed)
    }

    #[cfg_attr(not(test), allow(dead_code))]
    pub(crate) fn stop_count(&self, code: u32) -> u64 {
        stop_code::ALL
            .iter()
            .position(|c| *c == code)
            .map_or(0, |index| self.stops[index].load(Ordering::Relaxed))
    }

    /// The node-wide counts this path's are added to.
    pub(crate) fn node(&self) -> &'static QuicDataCounters {
        self.node
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
    /// Cancelled one-packet sends still waiting for their acknowledgement
    /// ([`AckWaits`]).
    ack_waits: Arc<AckWaits>,
    /// The path's share of the node's buffer budget, for a path that reads
    /// data (`budget::ConnMeter`); `None` for a shadow path.
    meter: Option<Arc<super::budget::ConnMeter>>,
    /// One permit per streamed message in flight
    /// ([`STREAMED_SEND_LIMIT`]), so the remaining uni stream slots stay
    /// for short messages and control frames. Never closed.
    stream_slots: Arc<tokio::sync::Semaphore>,
    /// Control frames taking their stream ([`Self::control_first`]); a data
    /// send opens no stream while any is.
    controls_opening: Arc<ControlsOpening>,
    /// Whether this path may send data, one of the `SEND_DATA_*` values. Off for a
    /// path in shadow, where a fragment taken from an inbound handle cannot
    /// outlive its charge because nothing is ever forwarded.
    send_data: AtomicU8,
    /// Whether this path is counted in the node's `quic_live_data_paths` gauge:
    /// from [`Self::count_as_data_path`] until the peer refuses data or
    /// [`Self::uncount_as_data_path`], whichever is first, so it is lowered once.
    counted_as_data_path: std::sync::atomic::AtomicBool,
    /// Send timeouts while nothing is acknowledged ([`route::StallCount`]).
    stall: parking_lot::Mutex<route::StallCount>,
    /// The [`bounded::QuietProof`] sends that ran out of gap left, read (not
    /// taken) by every settle; a proof of the same counts is merged into it.
    quiet: Arc<parking_lot::Mutex<Option<bounded::QuietProof>>>,
    /// The connection's acknowledged count and when it was last seen to
    /// move, on the path's clock ([`bounded::AckRecord`]); the count at build
    /// and the build time to start, as for the stall count, so a path dark
    /// from its start has its first sends' cap holds credited for one
    /// `route::hold_credit` after it is built (seconds at a high cap, minutes
    /// at a low one).
    last_acked: Arc<bounded::AckRecord>,
    /// The connection's count of acknowledged packets, `None` if its controller
    /// does not keep one.
    ack_progress: Option<recycle::AckProgress>,
    /// A send timed out with nothing acknowledged for the window: the path may
    /// have gone dark, and new sends go on UDP until the connection's count of
    /// acknowledged packets moves from the one held here ([`Self::is_suspect`]).
    /// Under one lock, so entering and leaving cannot interleave.
    suspect_since_acked: parking_lot::Mutex<Option<u64>>,
    /// Whether `suspect_since_acked` is set, so a path that is not suspect is
    /// told so without the lock.
    suspect: std::sync::atomic::AtomicBool,
    /// Where the path's watch reports that the peer was heard from.
    heard: Arc<recycle::PeerHeard>,
    /// What paces the path's data sends at the connection's bandwidth limit,
    /// once set ([`Self::pace_with`]); unset, as with no limit, nothing is
    /// paced. Control frames are never paced.
    pacer: std::sync::OnceLock<Arc<dyn pacer::SendPacer>>,
    #[cfg(test)]
    budget: SharedInboundBudget,
    /// The receive side's count of received ranges. Tests only.
    #[cfg(test)]
    inbound_ranges: Arc<inbound::ConnRanges>,
    /// The receive side's staged bytes. Tests only.
    #[cfg(test)]
    inbound_staging: Arc<inbound::ConnStaging>,
}

/// The path never had both data bits: nothing may send data on it.
const SEND_DATA_NEVER: u8 = 0;
/// Both sides set the data bit ([`QuicPath::enable_data`]).
const SEND_DATA_ON: u8 = 1;
/// The peer refused data it had said it accepts ([`QuicPath::stop_sending_data`]).
/// New sends stay off the path; a send already started finds out for itself.
const SEND_DATA_REFUSED: u8 = 2;

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
        let (path, inbound) =
            Self::with_budget_and_own_channels(connection, time, limits, budget, None);
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
        windows: Option<&Arc<super::budget::QuicBudget>>,
    ) -> (Self, QuicInbound) {
        let (data, inbox) = channels_of(limits.short_capacity, limits.stream_capacity);
        let (path, controls) = Self::with_budget(connection, time, limits, budget, data, windows);
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
        windows: Option<&Arc<super::budget::QuicBudget>>,
    ) -> (Self, mpsc::Receiver<Bytes>) {
        let stats = Arc::new(QuicPathStats::new(
            limits
                .counters
                .unwrap_or(&crate::transport::TRANSPORT_METRICS.quic_data),
        ));
        let (controls_tx, controls) = mpsc::channel(limits.control_capacity);
        #[cfg(test)]
        let staging_cap = limits
            .staging_cap
            .unwrap_or_else(|| staging_cap_without_meter(limits.accept_data));
        #[cfg(not(test))]
        let staging_cap = staging_cap_without_meter(limits.accept_data);
        let mut receiver = inbound::Receiver::new(
            time.clone(),
            budget.clone(),
            stats.clone(),
            data,
            controls_tx,
            limits.accept_data,
            connection.closer(),
            staging_cap,
        );
        #[cfg(test)]
        {
            receiver.panic_on_stream = limits.panic_in_receive;
            receiver.cancel_next_admitted = limits.cancel_next_admitted;
            receiver.set_gap_cap(limits.max_gaps);
        }
        // A path that reads data lets the peer open
        // `config::DATA_START_UNI_STREAMS` streams (the endpoint starts each
        // connection at `config::INITIAL_UNI_STREAMS`), raised with load up
        // to `MAX_UNI_STREAMS`, less one per one-packet Short waiting for room
        // (`inbound::StreamLimit`).
        if limits.accept_data {
            receiver.set_stream_limit(
                connection.inner().clone(),
                limits.start_streams,
                MAX_UNI_STREAMS,
            );
        }
        // A path that reads data takes a share of the node's buffer budget,
        // which sets its windows from here on; a shadow path keeps the
        // endpoint's floor windows.
        let meter = windows.filter(|_| limits.accept_data).map(|windows| {
            Arc::new(windows.register(
                Arc::new(connection.inner().clone()),
                receiver.ranges(),
                receiver.staging(),
                time.now_nanos(),
            ))
        });
        if let Some(meter) = &meter {
            receiver.set_meter(meter.clone());
        }
        #[cfg(test)]
        let inbound_ranges = receiver.ranges();
        #[cfg(test)]
        let inbound_staging = receiver.staging();
        let receiver_rtt = receiver.path_rtt();
        let aborts = Arc::new(SendAborts::new(
            limits.max_send_aborts,
            connection.closer(),
            stats.node(),
        ));
        // Read once: quinn keeps one controller per connection while migration is
        // off (`config`), so the record stays the connection's. The ack watch
        // compares records by pointer because it samples, and does not need to here.
        let ack_progress = recycle::AckProgress::of(connection.inner());
        if ack_progress.is_none() {
            tracing::warn!(
                remote = %connection.inner().remote_address(),
                "QUIC path: the congestion controller counts no acknowledgements; every \
                 send timeout counts toward the stall close"
            );
        }
        let built_at = time.now_nanos();
        let stall = route::StallCount::new(
            ack_progress.as_ref().map(recycle::AckProgress::acked),
            built_at,
        );
        let heard = Arc::new(recycle::PeerHeard::default());
        #[cfg_attr(not(test), allow(unused_mut))]
        let mut report = recycle::PathReport::new(
            connection.inner().clone(),
            stats.node(),
            built_at,
            meter.clone(),
        );
        #[cfg(test)]
        {
            report.panic_once_bucketed = limits.panic_in_watch;
        }
        let ack_waits = Arc::new(AckWaits::new(stats.clone()));
        let receive = GlobalExecutor::spawn(supervise_receive(
            connection.inner().clone(),
            connection.closer(),
            Arc::new(receiver),
            stats.clone(),
            time,
            recycle::AckWatchParts {
                cap_holds: connection.cap_holds(),
                rtt: receiver_rtt,
                heard: heard.clone(),
                report,
            },
        ));
        (
            Self {
                connection,
                stats,
                receive,
                aborts,
                ack_waits,
                meter,
                stream_slots: Arc::new(tokio::sync::Semaphore::new(STREAMED_SEND_LIMIT as usize)),
                controls_opening: Arc::default(),
                send_data: AtomicU8::new(SEND_DATA_NEVER),
                counted_as_data_path: std::sync::atomic::AtomicBool::new(false),
                stall: parking_lot::Mutex::new(stall),
                quiet: Arc::default(),
                last_acked: Arc::new(bounded::AckRecord::new(
                    ack_progress.as_ref().map_or(0, recycle::AckProgress::acked),
                    built_at,
                )),
                ack_progress,
                suspect_since_acked: parking_lot::Mutex::new(None),
                suspect: std::sync::atomic::AtomicBool::new(false),
                heard,
                pacer: std::sync::OnceLock::new(),
                #[cfg(test)]
                budget,
                #[cfg(test)]
                inbound_ranges,
                #[cfg(test)]
                inbound_staging,
            },
            controls,
        )
    }

    /// The code of the close this node sent on the path's connection, if it
    /// sent one.
    #[cfg_attr(not(any(test, feature = "testing")), allow(dead_code))]
    pub(in crate::transport) fn closed_with(&self) -> Option<QuicCloseCode> {
        self.connection.closer().closed_with()
    }

    /// Pace this path's data sends with `pacer` (`pacer` module): the
    /// connection's token bucket when a bandwidth limit is set. A second call
    /// changes nothing.
    pub(in crate::transport) fn pace_with(&self, pacer: Arc<dyn pacer::SendPacer>) {
        let _already = self.pacer.set(pacer);
    }

    /// A message of `bytes` that `outcome` sent on UDP instead, after the UDP
    /// send succeeded: if the pacer refused it, charge the whole message to the
    /// pacer as debt, since UDP put all of it on the wire, so the path's next
    /// sends wait for it. A Short refused after its stream opened also paid for
    /// the writes it made on QUIC; those bytes were sent too. The debt is
    /// bounded ([`pacer::FALLBACK_DEBT`]), which is what keeps the charge from
    /// failing streams already open. Returns what was charged.
    pub(in crate::transport) fn charge_fallback(&self, outcome: &BoundedSend, bytes: u64) -> u64 {
        let refused = matches!(
            outcome,
            BoundedSend::Failed(SendFailure::NotOpened(NotOpened::Paced) | SendFailure::Paced)
        );
        let (true, Some(pacer)) = (refused, self.pacer.get()) else {
            return 0;
        };
        pacer.charge(usize::try_from(bytes).unwrap_or(usize::MAX)) as u64
    }

    /// The pacing for a data send, if the path is paced.
    fn pacing(&self) -> Option<pacer::Pacing> {
        self.pacer.get().map(|pacer| pacer::Pacing {
            pacer: Arc::clone(pacer),
        })
    }

    /// The most gaps the receive side's streams held at once. Tests only.
    #[cfg(test)]
    pub(in crate::transport) fn inbound_gaps_peak(&self) -> usize {
        self.inbound_ranges.peak()
    }

    /// Staged bytes the receive side holds now. Tests only.
    #[cfg(test)]
    pub(in crate::transport) fn inbound_staged(&self) -> usize {
        self.inbound_staging.held()
    }

    /// Charges the inbound budget holds now. Tests only.
    #[cfg(test)]
    pub(in crate::transport) fn inbound_budget_streams(&self) -> usize {
        self.budget.lock().stream_count()
    }

    /// The path's buffer-budget meter, if it reads data under a budget.
    #[cfg(test)]
    pub(in crate::transport) fn meter(&self) -> Option<&Arc<super::budget::ConnMeter>> {
        self.meter.as_ref()
    }

    /// The receive side's staging cap now. Tests only.
    #[cfg(test)]
    pub(in crate::transport) fn inbound_staging_cap(&self) -> usize {
        self.inbound_staging.cap()
    }

    /// The most the receive side staged at once, per mille of its cap.
    /// Tests only.
    #[cfg(test)]
    pub(in crate::transport) fn inbound_staged_peak_permille(&self) -> usize {
        self.inbound_staging.peak_permille()
    }

    /// Gaps the receive side counts now. Tests only.
    #[cfg(test)]
    pub(in crate::transport) fn inbound_gaps_held(&self) -> usize {
        self.inbound_ranges.held()
    }

    /// Gaps of stopped streams the receive side keeps counted. Tests only.
    #[cfg(test)]
    pub(in crate::transport) fn inbound_gaps_kept(&self) -> usize {
        self.inbound_ranges.kept()
    }

    /// Whether the path's data sends are paced.
    #[cfg(test)]
    pub(in crate::transport) fn is_paced(&self) -> bool {
        self.pacer.get().is_some()
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

    /// Cancelled sends without a permit waiting for their acknowledgement.
    #[cfg(test)]
    pub(crate) fn ack_waits(&self) -> &AckWaits {
        &self.ack_waits
    }

    /// Sends this side reset on this path so far.
    #[cfg(test)]
    pub(crate) fn send_aborts(&self) -> u64 {
        self.aborts.count()
    }

    /// The sends holding one of the path's send-window epochs, 0 without a
    /// budget meter.
    #[cfg(test)]
    pub(crate) fn send_epoch_holders(&self) -> usize {
        self.meter
            .as_ref()
            .map_or(0, |meter| meter.send_epoch_holders())
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

    /// An estimate, from below, of the gap the receiver of a stream sent on
    /// this path allows between two pieces before it gives up:
    /// [`quic_fragment_gap`] of the path's minimum round trip, under the idle
    /// limit every reader uses. A pipe into this path waits on its source no
    /// longer than this. The receiver times its gap from its own smoothed
    /// round trip; this end's smoothed round trip also carries its own
    /// queueing, so it can sit above the receiver's, and the minimum is used
    /// instead. That errs short, which costs at most a pipe dropped early
    /// (floored at 5 s), and makes a pipe that writes into a stream its
    /// receiver has given up on unlikely, not impossible.
    ///
    /// [`quic_fragment_gap`]: crate::transport::peer_connection::streaming::quic_fragment_gap
    pub(crate) fn receiver_gap(&self) -> Duration {
        crate::transport::peer_connection::streaming::quic_fragment_gap(
            self.connection.inner().stats().path.min_rtt,
            crate::transport::peer_connection::streaming_handle_idle_timeout(),
        )
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
        self.send_data.store(SEND_DATA_ON, Ordering::Release);
    }

    /// The peer said it accepts no data on this path, so this node's gating or
    /// the peer's is wrong. Logged once per path. New sends go on UDP from now
    /// on. This side does not close the path: the refusing receiver does, with
    /// DOWNGRADE, [`inbound::REFUSAL_CLOSE_DELAY`] after its first refusal, and
    /// this side falls back on that close.
    fn stop_sending_data(&self) {
        self.stats.record_send_event(QuicSendEvent::DataRefused);
        // No longer a path that carries data.
        self.uncount_as_data_path();
        let was_on = self
            .send_data
            .compare_exchange(
                SEND_DATA_ON,
                SEND_DATA_REFUSED,
                Ordering::AcqRel,
                Ordering::Acquire,
            )
            .is_ok();
        if was_on {
            tracing::warn!(
                remote = %self.connection.inner().remote_address(),
                "QUIC path: the peer refused data after both sides set the data bit; \
                 sending on UDP from now on"
            );
        }
    }

    /// Count this path in the node's `quic_live_data_paths` gauge, once: its
    /// route was installed. Undone by [`Self::uncount_as_data_path`] or a refusal.
    pub(in crate::transport) fn count_as_data_path(&self) {
        if !self.counted_as_data_path.swap(true, Ordering::AcqRel) {
            self.stats.node().data_path_started();
        }
    }

    /// Take this path out of the gauge if it is in it: its attempt went. A path
    /// the peer refused data on is out already, so nothing is lowered twice.
    pub(in crate::transport) fn uncount_as_data_path(&self) {
        if self.counted_as_data_path.swap(false, Ordering::AcqRel) {
            self.stats.node().data_path_ended();
        }
    }

    /// From now on, call `report` when the peer is heard from on this path
    /// ([`recycle::watch_acknowledgements`]).
    pub(in crate::transport) fn report_receipts_to(&self, report: Box<dyn Fn() + Send + Sync>) {
        self.heard.report_to(report);
    }

    /// A send timed out on this path with nothing acknowledged for the window
    /// (`route::StallCount`), when the count read `acked`. A path that went dark
    /// would otherwise take every new send until it is recycled: each short ends
    /// `MaybeDelivered(TimedOut)`, is never resent and is lost, and an operation's
    /// retry lands in the same place. Sends already on the path stay there, since
    /// resending them could deliver a message twice.
    pub(in crate::transport) fn become_suspect(&self, acked: u64) {
        let mut since = self.suspect_since_acked.lock();
        let entered = since.replace(acked).is_none();
        self.suspect.store(true, Ordering::Release);
        drop(since);
        if entered {
            self.stats.record_send_event(QuicSendEvent::SuspectEntered);
            tracing::debug!(
                remote = %self.connection.inner().remote_address(),
                "QUIC path: sends time out and nothing is acknowledged; new data \
                 goes on UDP until the peer acknowledges again"
            );
        }
    }

    /// Whether the path is suspect: new data goes on UDP. It stops being suspect
    /// the first time this is asked after the connection acknowledged something
    /// new, which quinn's keepalive brings within its interval on a live path.
    ///
    /// A path that stays dark ends on quinn's idle timeout (120 s in
    /// production), not on the stall close: once it is suspect no new send
    /// reaches it, so `route::STALLED_SENDS_LIMIT` timeouts in a row happen only
    /// if that many sends were already on it. Nothing is lost meanwhile, since
    /// new data goes on UDP.
    pub(in crate::transport) fn is_suspect(&self) -> bool {
        if !self.suspect.load(Ordering::Acquire) {
            return false;
        }
        let Some(progress) = &self.ack_progress else {
            return false;
        };
        let mut since = self.suspect_since_acked.lock();
        match *since {
            Some(acked) if progress.acked() == acked => true,
            Some(_) => {
                *since = None;
                self.suspect.store(false, Ordering::Release);
                drop(since);
                self.stats.record_send_event(QuicSendEvent::SuspectLeft);
                false
            }
            None => false,
        }
    }

    /// Whether a new data send may start on this path.
    pub(in crate::transport) fn sends_data(&self) -> bool {
        self.send_data.load(Ordering::Acquire) == SEND_DATA_ON
    }

    /// A data send must not happen on a shadow path. See `send_data`. One may
    /// race a refusal, so a refused path passes.
    fn debug_assert_carries_data(&self) {
        debug_assert_ne!(
            self.send_data.load(Ordering::Acquire),
            SEND_DATA_NEVER,
            "a shadow QUIC path sends control frames only"
        );
    }

    /// What a send's gap reads of this path's connection
    /// ([`bounded::ConnectionProgress`]); `None` when the controller counts no
    /// acknowledgements, and then only the send's own gap applies.
    fn connection_progress(&self) -> Option<ConnectionProgress> {
        self.ack_progress.clone().map(|acks| ConnectionProgress {
            acks,
            cap_holds: self.connection.cap_holds(),
            quiet: self.quiet.clone(),
            last_acked: self.last_acked.clone(),
        })
    }

    /// Send one serialized message. Tests only, with no deadline; nodes use
    /// [`Self::send_short_bounded`].
    #[cfg(test)]
    pub(super) async fn send_short(&self, payload: &[u8]) -> Result<(), QuicSendError> {
        self.send_short_with(payload, None, SendProgress::new())
            .await
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
        bounded_send(time, deadlines, self.connection_progress(), |progress| {
            self.send_short_with(payload, None, progress)
        })
        .await
    }

    async fn send_short_with(
        &self,
        payload: &[u8],
        observer: Option<&dyn StreamProgressObserver>,
        progress: SendProgress,
    ) -> Result<(), QuicSendError> {
        self.debug_assert_carries_data();
        let header = FrameHeader::short(payload.len() as u64)?;
        // A Short past one packet is a stream on UDP, and paced as one; a Short
        // of one packet is not, as on UDP.
        let (slot, pacing) = if fits_one_packet(payload.len() as u64) {
            (None, None)
        } else {
            (Some(self.stream_slot().await?), self.pacing())
        };
        let mut send = self
            .open(&header, &[], slot, pacing, progress, Opener::Data)
            .await?;
        for piece in payload.chunks(FRAGMENT_PAYLOAD_SIZE) {
            send.take_turns().await;
            send.write_paced(piece).await?;
            if let Some(observer) = observer {
                observer.on_fragment_sent();
            }
        }
        send.finish().await
    }

    /// Send one control frame. Tests only, with no deadline; nodes use
    /// [`Self::send_control_bounded`].
    #[cfg(test)]
    pub(super) async fn send_control(&self, payload: &[u8]) -> Result<(), QuicSendError> {
        self.send_control_with(payload, SendProgress::new(), None)
            .await
    }

    async fn send_control_with(
        &self,
        payload: &[u8],
        progress: SendProgress,
        first: Option<ControlFirst>,
    ) -> Result<(), QuicSendError> {
        let header = FrameHeader::control(payload.len() as u64)?;
        // Never paced: a control frame must not wait behind data.
        let opened = self
            .open(&header, &[], None, None, progress, Opener::Control)
            .await;
        // The stream is ours (or the send failed): data may open again.
        drop(first);
        let mut send = opened?;
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
        self.send_control_first(time, timeout, control, self.control_first())
            .await
    }

    /// Reserve the next stream for a control frame: until the frame sent
    /// with `first` ([`Self::send_control_first`]) has its stream, or its
    /// send ends, no data send on this path opens one. Taken before the
    /// send is spawned, so data enabled after it cannot take the peer's
    /// stream slots first: a client's `ReadyAck` goes out in the same step
    /// that enables its data, and with the peer's slots all taken by data
    /// it would wait out its deadline and fail the upgrade.
    pub(crate) fn control_first(&self) -> ControlFirst {
        self.controls_opening.pending.fetch_add(1, Ordering::SeqCst);
        ControlFirst(Arc::clone(&self.controls_opening))
    }

    /// Control frames holding a reservation now. Tests only.
    #[cfg(test)]
    pub(crate) fn controls_reserved(&self) -> usize {
        self.controls_opening.pending.load(Ordering::SeqCst)
    }

    /// [`Self::send_control_bounded`] with a reservation taken earlier
    /// ([`Self::control_first`]).
    pub(crate) async fn send_control_first<T: TimeSource>(
        &self,
        time: &T,
        timeout: Duration,
        control: QuicControl,
        first: ControlFirst,
    ) -> BoundedSend {
        let payload = control.encode();
        bounded_send(time, timeout, None, |progress| {
            self.send_control_with(&payload[..], progress, Some(first))
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
        bounded_send(time, deadlines, self.connection_progress(), |progress| {
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
        let mut send = self
            .open(
                &header,
                metadata,
                Some(slot),
                self.pacing(),
                progress,
                Opener::Data,
            )
            .await?;
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
        let first = first_chunk(chunks.next().await, total_len)?;
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
    /// [`SendDeadlines::source_gap`] (or `gap`) and `total` allow, so a source that has nothing
    /// yet holds no permit and no stream. `open` runs from when it arrives;
    /// `total` counts the wait. A wait that runs out or a connection lost
    /// during it is [`SendFailure::NotOpened`]. A source that ends or fails
    /// before its first chunk opens no stream and has nothing to reset, and is
    /// [`SendFailure::Error`], since the message cannot go another way either.
    /// A later stall past the gap resets the stream. A chunk taken before a
    /// [`SendFailure::NotOpened`] is lost, so a caller sending another way
    /// starts again from a fresh source.
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
        let started = time.now_nanos();
        let connection = self.connection.inner();
        let wait = deadlines
            .gap_for(true)
            .map_or(deadlines.total, |gap| gap.min(deadlines.total));
        let first = tokio::select! {
            first = chunks.next() => first,
            () = time.sleep_cancelable(wait) => {
                return BoundedSend::Failed(SendFailure::NotOpened(NotOpened::TimedOut));
            }
            error = connection.closed() => {
                return BoundedSend::Failed(SendFailure::NotOpened(NotOpened::ConnectionLost(error)));
            }
        };
        let first = match first_chunk(first, total_len) {
            Ok(first) => first,
            Err(error) => return BoundedSend::Failed(SendFailure::Error(error)),
        };
        let waited = Duration::from_nanos(time.now_nanos().saturating_sub(started));
        if waited >= deadlines.total {
            return BoundedSend::Failed(SendFailure::NotOpened(NotOpened::TimedOut));
        }
        let deadlines = SendDeadlines {
            total: deadlines.total - waited,
            ..deadlines
        };
        bounded_send(time, deadlines, self.connection_progress(), |progress| {
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
        first: Bytes,
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
        let slot = self.stream_slot().await?;
        let source_wait = progress.clone();
        let mut send = self
            .open(
                &header,
                metadata,
                Some(slot),
                self.pacing(),
                progress,
                Opener::Data,
            )
            .await?;
        let mut delivered: u64 = 0;
        let mut next = Some(first);
        let failure = loop {
            let item = match next.take() {
                Some(chunk) => Some(Ok(chunk)),
                None => {
                    source_wait.waiting_on_source(true);
                    let item = send.race(chunks.next()).await;
                    source_wait.waiting_on_source(false);
                    item?
                }
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
        if let Ok(permit) = Arc::clone(&self.stream_slots).try_acquire_owned() {
            self.stats.node.record_permit(false);
            return Ok(permit);
        }
        self.stats.node.record_permit(true);
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
    /// in use. The stream takes over `slot`. With `pacing`, the header and
    /// metadata are taken from the pacer before the stream opens, so a
    /// refusal opens nothing, and the stream's later writes are paced too.
    /// A data send first waits for control frames reserved ahead of it
    /// ([`Self::control_first`]), within its own open deadline.
    async fn open(
        &self,
        header: &FrameHeader,
        metadata: &[u8],
        slot: Option<tokio::sync::OwnedSemaphorePermit>,
        pacing: Option<pacer::Pacing>,
        progress: SendProgress,
        opener: Opener,
    ) -> Result<Outgoing, QuicSendError> {
        if opener == Opener::Data {
            self.controls_opening.wait().await;
        }
        let paid = match &pacing {
            Some(pacing) => {
                let bytes = super::framing::HEADER_LEN + metadata.len();
                Some(pace(pacing, bytes, &progress).await?)
            }
            None => None,
        };
        // Polled once by hand, to tell an open that had to wait for the peer's
        // stream credit from one that did not (`quic_open_uni_waited`).
        let mut open = std::pin::pin!(self.connection.inner().open_uni());
        let opened = match futures::poll!(open.as_mut()) {
            std::task::Poll::Ready(opened) => {
                self.stats.node.record_open(false);
                opened
            }
            std::task::Poll::Pending => {
                self.stats.node.record_open(true);
                // Counted if the send's deadline drops this wait: the peer's
                // stream limit was full for all of it.
                let waiting = CreditWait(Some((&self.stats, &progress)));
                let opened = open.await;
                waiting.disarm();
                opened
            }
        };
        let stream = opened?;
        progress.mark_opened();
        let epoch = self.meter.as_ref().and_then(|meter| meter.send_epoch());
        let mut send = Outgoing::new(stream, self.aborts.clone(), slot, pacing, progress, epoch);
        send.meter = self.meter.clone();
        send.ack_waits = Some(self.ack_waits.clone());
        send.write_all(&header.encode()).await?;
        if !metadata.is_empty() {
            send.write_all(metadata).await?;
        }
        if let Some(paid) = paid {
            paid.spent();
        }
        Ok(send)
    }
}

/// Who opens a stream ([`QuicPath::open`]).
#[derive(Clone, Copy, PartialEq, Eq)]
enum Opener {
    Data,
    Control,
}

/// Control frames reserved ahead of data ([`QuicPath::control_first`]).
#[derive(Default)]
struct ControlsOpening {
    pending: AtomicUsize,
    opened: tokio::sync::Notify,
}

impl ControlsOpening {
    /// Wait until no control frame holds a reservation.
    async fn wait(&self) {
        loop {
            let opened = self.opened.notified();
            tokio::pin!(opened);
            opened.as_mut().enable();
            if self.pending.load(Ordering::SeqCst) == 0 {
                return;
            }
            opened.await;
        }
    }
}

/// A control frame's reservation of the next stream on its path
/// ([`QuicPath::control_first`]); dropped once the frame has its stream or
/// its send ends.
pub(crate) struct ControlFirst(Arc<ControlsOpening>);

impl Drop for ControlFirst {
    fn drop(&mut self) {
        if self.0.pending.fetch_sub(1, Ordering::SeqCst) == 1 {
            self.0.opened.notify_waiters();
        }
    }
}

/// A send waiting in `open_uni` for a stream credit; dropped armed at the
/// send's open deadline (the deadline ended the wait), it counts
/// [`QuicSendEvent::StreamLimitFull`]. A drop before it (the send cancelled
/// by a teardown or a shutdown) is not counted.
struct CreditWait<'a>(Option<(&'a QuicPathStats, &'a SendProgress)>);

impl CreditWait<'_> {
    fn disarm(mut self) {
        self.0 = None;
    }
}

impl Drop for CreditWait<'_> {
    fn drop(&mut self) {
        if let Some((stats, progress)) = self.0
            && progress.open_deadline_passed()
        {
            stats.record_send_event(QuicSendEvent::StreamLimitFull);
        }
    }
}

/// Take `bytes` from `pacing` and wait for them, with no more than the send's
/// current deadline leaves (`pacer` module). A refusal marks `progress`, and
/// the error it returns only ends the send, which then ends as at that
/// deadline. Dropping the returned bytes before [`pacer::PacedBytes::spent`]
/// gives them back, as does dropping this future during its wait.
async fn pace(
    pacing: &pacer::Pacing,
    bytes: usize,
    progress: &SendProgress,
) -> Result<pacer::PacedBytes, QuicSendError> {
    match pacing.take(bytes, progress.time_left()) {
        pacer::Paced::Taken { bytes, wait } => {
            if !wait.is_zero() {
                pacing.pacer.sleep(wait).await;
            }
            Ok(bytes)
        }
        pacer::Paced::Refused => {
            progress.refuse();
            Err(QuicSendError::Closed)
        }
    }
}

/// A frame that cannot be built, for `route`'s tests.
#[cfg(test)]
fn framing_error_for_tests() -> HeaderError {
    FrameHeader::short(0).expect_err("a Short needs a length")
}

/// The first item of a chunk source: a source that ends or fails before it
/// gives a chunk opens no stream, so there is nothing to reset.
fn first_chunk(
    first: Option<Result<Bytes, StreamError>>,
    total_len: u64,
) -> Result<Bytes, QuicSendError> {
    match first {
        Some(Ok(chunk)) => Ok(chunk),
        Some(Err(error)) => Err(QuicSendError::Source(error)),
        None => Err(QuicSendError::LengthMismatch {
            declared: total_len,
            delivered: 0,
        }),
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
    watch: recycle::AckWatchParts,
) {
    use futures::FutureExt;
    let mut receive = std::pin::pin!(
        std::panic::AssertUnwindSafe(inbound::receive(connection.clone(), receiver)).catch_unwind()
    );
    // Outside the watch, so a panic the watch contains does not end the path's
    // report: it ends with this task ([`recycle::PathReport`]).
    let recycle::AckWatchParts {
        cap_holds,
        rtt,
        heard,
        mut report,
    } = watch;
    let watch = std::panic::AssertUnwindSafe(recycle::watch_acknowledgements(
        connection.clone(),
        closer.clone(),
        time,
        cap_holds,
        heard,
        rtt,
        &mut report,
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
    /// A piece of the message has been written ([`Self::take_turns`]).
    wrote_a_piece: bool,
    /// The send's pacing, for a paced data send ([`Self::write_paced`]).
    pacing: Option<pacer::Pacing>,
    /// The send-window epoch the stream was opened in (`budget::ConnMeter`),
    /// held until its data is acknowledged, so a send window lowered since
    /// stays charged while quinn may still hold the stream's data.
    epoch: Option<Arc<()>>,
    /// The path's budget meter, woken by each write so a path that only sends
    /// still has its send window evaluated (`budget::ConnMeter::note_send`).
    meter: Option<Arc<super::budget::ConnMeter>>,
    /// The path's count of cancelled sends waiting for their acknowledgement
    /// without a permit ([`AckWaits`]).
    ack_waits: Option<Arc<AckWaits>>,
}

type StoppedResult = Result<Option<quinn::VarInt>, quinn::StoppedError>;

impl Outgoing {
    fn new(
        send: quinn::SendStream,
        aborts: Arc<SendAborts>,
        slot: Option<tokio::sync::OwnedSemaphorePermit>,
        pacing: Option<pacer::Pacing>,
        progress: SendProgress,
        epoch: Option<Arc<()>>,
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
            wrote_a_piece: false,
            pacing,
            epoch,
            meter: None,
            ack_waits: None,
        }
    }

    /// Take `bytes` from the send's pacer, if it has one, and wait for them,
    /// racing a stop from the peer as every wait on an open stream does. A
    /// refusal ends the send as at its current deadline ([`pace`]).
    async fn pace(&mut self, bytes: usize) -> Result<Option<pacer::PacedBytes>, QuicSendError> {
        let Some(pacing) = self.pacing.clone() else {
            return Ok(None);
        };
        match pacing.take(bytes, self.progress.time_left()) {
            pacer::Paced::Taken { bytes, wait } => {
                if !wait.is_zero() {
                    self.race(pacing.pacer.sleep(wait)).await?;
                }
                Ok(Some(bytes))
            }
            pacer::Paced::Refused => {
                self.progress.refuse();
                Err(QuicSendError::Closed)
            }
        }
    }

    /// [`Self::write_all`], paced.
    async fn write_paced(&mut self, buf: &[u8]) -> Result<(), QuicSendError> {
        let paid = self.pace(buf.len()).await?;
        self.write_all(buf).await?;
        if let Some(paid) = paid {
            paid.spent();
        }
        Ok(())
    }

    /// Let the path's other streamed sends run before this send's next piece.
    ///
    /// quinn gives a write that fits the shared send window all of it without
    /// yielding, so a stream with more to send would take every byte of credit
    /// in turn and the others would wait for it to finish: longer than the 5 s
    /// the receiver gives a stream to make progress, on a slow link. Yielding
    /// gives each stream a fragment in turn, as far as a step of credit reaches:
    /// quinn wakes blocked writers last in, first out, so with more writers than
    /// a step of credit has fragments, some can lag. The smallest window is
    /// about 14 fragments a step.
    ///
    /// Before a piece and not after it, and never before the first: a yield
    /// after the last piece lets the connection driver send the data before
    /// the end of the stream is queued, which puts the end in a datagram of
    /// its own, and a message of one piece has nothing to share. Only while
    /// another streamed send holds a permit: a lone stream would pay a
    /// scheduler pass per fragment for nothing. The check reads permits, so
    /// a stream waiting for its acknowledgement past a deadline counts too.
    async fn take_turns(&mut self) {
        if !std::mem::replace(&mut self.wrote_a_piece, true) {
            return;
        }
        let shared = self.slot.as_ref().is_some_and(|slot| {
            slot.semaphore().available_permits() + 1 < STREAMED_SEND_LIMIT as usize
        });
        if shared {
            tokio::task::yield_now().await;
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
        self.written(written, buf.len())
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
            self.take_turns().await;
            let fragment = chunk.split_to(chunk.len().min(FRAGMENT_PAYLOAD_SIZE));
            let paid = self.pace(fragment.len()).await?;
            self.write_chunk(fragment).await?;
            if let Some(paid) = paid {
                paid.spent();
            }
            if let Some(observer) = observer {
                observer.on_fragment_sent();
            }
        }
        Ok(())
    }

    async fn write_chunk(&mut self, chunk: Bytes) -> Result<(), QuicSendError> {
        let len = chunk.len();
        let Self { send, stopped, .. } = self;
        let written = tokio::select! {
            written = send.write_chunk(chunk) => written,
            result = stopped => return Err(self.stopped_early(result)),
        };
        self.written(written, len)
    }

    /// The outcome of a write of `len` bytes. A stop the peer sent is handled as in
    /// [`Self::stopped_early`]: on STOP_SENDING both the write and the
    /// `stopped` future become ready, and `select!` may pick the write.
    /// Returned through `?` instead, the stream would be left open and
    /// `Drop` would reset it as an abort of this side's own
    /// ([`SendAborts`]), which it is not.
    fn written(
        &mut self,
        written: Result<(), quinn::WriteError>,
        len: usize,
    ) -> Result<(), QuicSendError> {
        match written {
            Ok(()) => {
                self.progress.step();
                if let Some(meter) = &self.meter {
                    meter.note_send(len as u64);
                }
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

    /// Reset a stream whose end was already sent, so quinn discards what the
    /// peer has not acknowledged (quinn-proto 0.11.19 `SendState::reset` from
    /// `DataSent`). Counted as an abort ([`SendAborts`]): `stopped()` was
    /// polled while the stream waited for its acknowledgement, so the reset
    /// leaves an entry in quinn's `stopped` map, which is what the abort limit
    /// bounds. A stream already acknowledged in full is not reset.
    fn reset_finished(&mut self) {
        if self.send.reset(reset_code::SENDER_ABORTED.into()).is_ok() {
            self.aborts.record();
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
    /// end, so its permit and its send epoch wait with it, in a small task
    /// that ends with the stream's acknowledgement or with the connection.
    /// A send holding a permit is one of at most [`STREAMED_SEND_LIMIT`]. One
    /// without a permit (a one-packet Short) holds only its epoch, and a peer
    /// that acknowledges other traffic while withholding some acknowledgements
    /// could keep any number of those waiting, so at most
    /// [`ACK_WAITS_WITHOUT_PERMIT`] wait per path; past that the epoch is
    /// released at once and counted ([`QuicSendEvent::AckWaitReleased`]), and
    /// the stream is reset, which makes quinn discard its unacknowledged data
    /// ([`Self::reset_finished`]): releasing the epoch alone would let a send
    /// window lowered since the stream opened leave the charge while quinn
    /// still held the packet. The Short may then not arrive; it reported
    /// `MaybeDelivered` already, so its sender does not count on it.
    fn drop(&mut self) {
        self.reset(reset_code::SENDER_ABORTED);
        if !self.awaiting_ack {
            return;
        }
        let (slot, epoch) = (self.slot.take(), self.epoch.take());
        if slot.is_none() && epoch.is_none() {
            return;
        }
        // With no runtime the node is stopping, and neither has users.
        if tokio::runtime::Handle::try_current().is_err() {
            return;
        }
        let waiting = if slot.is_none() {
            match self.ack_waits.as_ref().map(AckWaits::enter) {
                Some(Some(waiting)) => Some(waiting),
                // Past the bound: the epoch goes now, and so does the
                // stream's data, so the release matches the memory freed.
                Some(None) => {
                    self.reset_finished();
                    return;
                }
                None => None,
            }
        } else {
            None
        };
        let stopped = std::mem::replace(&mut self.stopped, Box::pin(std::future::pending()));
        // Bounded as above, and ending with the acknowledgement or the
        // connection, so the handle is not monitored.
        drop(GlobalExecutor::spawn(async move {
            let _ended = stopped.await;
            drop(slot);
            drop(epoch);
            drop(waiting);
        }));
    }
}

/// The most cancelled sends without a permit that wait for their
/// acknowledgement on one path, each holding its send epoch in a task
/// ([`Outgoing`]'s drop).
pub(crate) const ACK_WAITS_WITHOUT_PERMIT: usize = super::config::MAX_UNI_STREAMS as usize;

/// A path's count of cancelled sends without a permit waiting for their
/// acknowledgement.
pub(crate) struct AckWaits {
    waiting: std::sync::atomic::AtomicUsize,
    /// [`ACK_WAITS_WITHOUT_PERMIT`], lower in a test.
    limit: std::sync::atomic::AtomicUsize,
    stats: Arc<QuicPathStats>,
}

/// One cancelled send waiting for its acknowledgement; dropping it frees its
/// place.
pub(crate) struct AckWait(Arc<AckWaits>);

impl AckWaits {
    fn new(stats: Arc<QuicPathStats>) -> Self {
        Self {
            waiting: std::sync::atomic::AtomicUsize::new(0),
            limit: std::sync::atomic::AtomicUsize::new(ACK_WAITS_WITHOUT_PERMIT),
            stats,
        }
    }

    /// A place to wait, or `None` (counted) when [`ACK_WAITS_WITHOUT_PERMIT`]
    /// already wait.
    fn enter(this: &Arc<Self>) -> Option<AckWait> {
        let limit = this.limit.load(Ordering::Relaxed);
        let entered = this
            .waiting
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |waiting| {
                (waiting < limit).then_some(waiting + 1)
            })
            .is_ok();
        if entered {
            Some(AckWait(this.clone()))
        } else {
            this.stats.record_send_event(QuicSendEvent::AckWaitReleased);
            None
        }
    }

    #[cfg(test)]
    pub(crate) fn waiting(&self) -> usize {
        self.waiting.load(Ordering::Acquire)
    }

    #[cfg(test)]
    pub(crate) fn set_limit(&self, limit: usize) {
        self.limit.store(limit, Ordering::Relaxed);
    }
}

impl Drop for AckWait {
    fn drop(&mut self) {
        self.0.waiting.fetch_sub(1, Ordering::AcqRel);
    }
}
