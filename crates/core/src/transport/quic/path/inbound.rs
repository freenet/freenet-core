//! The receive side of a [`QuicPath`](super::QuicPath), and the inbound
//! adapter that feeds a QUIC stream into an ordinary [`StreamHandle`].
//!
//! The adapter exists so that everything above the transport stays as it
//! is: the payload of a `Stream` frame goes into the same `StreamHandle`
//! (and `LockFreeStreamBuffer`) the UDP transport fills, cut into fragments
//! of the UDP shape ([`Rechunker`]), so `fork`, `assemble`, `pipe_stream`,
//! `StreamIdentity` and the orphan registry work unchanged.
//!
//! Every stream is read unordered from its first byte (`read_chunk(_, false)`,
//! [`BodyReader::next_piece`]): quinn hands over each piece as it arrives,
//! gap or not, so quinn holds nothing for a stream beyond the chunks
//! the reader has not polled yet. Unordered reads return the peer's credit for
//! every byte read, so what the reader keeps is bounded here instead:
//! - Before a stream's header, metadata or control body has been taken, the
//!   bytes read for it are staged in one buffer per stream ([`Staging`]),
//!   spanning from the read position to the highest byte staged. Every
//!   connection's staged buffers are charged, at the size of their allocation,
//!   against one cap ([`ConnStaging`], twice the connection's credit W + debt
//!   on a data path), checked before each buffer grows. A read that would pass
//!   it stops its stream (`EVICTED`, counted `evicted_staged`); a second within
//!   [`STAGED_TRIP_WINDOW`] closes the path with OVER_BUDGET.
//! - Once the header is known, a `Stream`'s payload goes straight into the
//!   charged buffer at its offset and a `Short`'s body into one buffer of its
//!   declared length, and both stop at the declared end.
//!
//! What a gap still costs is one received range (charged at `GAP_BYTES`,
//! with our mirror, [`RangeMirror`]), capped per connection (`R_conn`, one per
//! KiB of the window, `budget::gap_cap`; [`MAX_GAPS_PER_CONNECTION`] without a
//! meter). A stopped stream's gaps stay counted ([`ConnRanges::keep`]), since
//! we cannot observe when the peer ends it; a second time past the cap within
//! [`STAGED_TRIP_WINDOW`] closes the path.
//!
//! A reader never stops reading: a stream waiting for room in the delivery
//! channel keeps reading, so the wait never leaves data with quinn. Bytes past
//! the declared end fail the stream (`Overlong`), and the stream's end may be
//! read before the stream is announced. That frees quinn's slot for it, so a
//! charged message that waits is bounded by its charge, and an uncharged
//! one-packet `Short` lowers the peer's stream limit by one while it waits
//! ([`StreamLimit`]): at most the stream limit of them wait, and none is
//! refused after its sender may have seen it acknowledged.
//!
//! Admission follows the UDP transport's rules (`.claude/rules/transport.md`,
//! "WHEN admitting or sending a stream"):
//! - Before anything is created from a declared length, the header has
//!   passed `check_declared_stream_length` (in `FrameHeader::decode`) and
//!   the stream has been charged to the path's `InboundStreamBudget`, with
//!   the same cost (`ChargeOverhead`). Short messages are charged too: they
//!   carry what the UDP transport sends as transport-internal streams. The
//!   budget may be the peer's UDP connection's own (`QuicPath::with_budget`);
//!   charges here are keyed `InboundKey::Quic` and `InboundKey::QuicShort`,
//!   so they never meet the UDP side's `InboundKey::Udp` ones.
//! - A refusal is counted by reason in the shared `InboundStreamOutcome`
//!   counters (path and process-wide) and the stream is stopped
//!   (STOP_SENDING, [`stop_code`]), which tells the sender; nothing more is
//!   read from it.
//! - The charge is held until all reassembly state is gone, observed through
//!   a `Weak` (the buffer for a stream; for a short message, a token that
//!   travels with it until the consumer takes its payload).
//! - Completion is by BYTES: a stream is complete when exactly `total_len`
//!   bytes have arrived and then the stream's end. A stream that ends early
//!   is `Truncated`, one with more bytes is `Overlong`; either cancels the
//!   handle, so its consumers see `StreamError::Cancelled`, stream-scoped.
//!   The fragment that completes the buffer is held back until the end of
//!   the stream is seen, so an overlong stream never looks complete.
//! - Idle and age eviction use the UDP limits (`streaming_handle_idle_timeout`,
//!   `inbound_stream_age_limit`) and counters (`EvictedIdle`, `EvictedAge`).
//!
//! Differences from the UDP path, and why:
//! - Eviction is checked by a timer per stream on the path's clock, when the
//!   deadline passes, instead of on the connection's `timeout_check` tick.
//!   Every QUIC chunk is new bytes (quinn delivers each byte once), so every
//!   chunk is progress.
//! - A refused stream id is not remembered (`RefusedStreams`). A refused
//!   QUIC stream is stopped as a whole, so none of its bytes can arrive
//!   later; a new QUIC stream with the same id carries a whole stream from
//!   its first byte and is judged afresh. `ClosedTooManyRefusals` cannot
//!   happen here.
//! - A `Stream` whose id is already held on the path is refused
//!   (`StreamIdInUse`). Completed ids are not remembered here: that dedup is in
//!   the connection's stream arm, which sees both transports
//!   (`completed_inbound_streams`).
//! - Header refusals, truncation, overlong streams, resets and full channels
//!   are counted in the path's own `QuicInboundOutcome` counters. A refused
//!   header with an invalid declared length is also counted as
//!   `InboundStreamOutcome::RefusedInvalidLength`, as the UDP path counts it.
//!
//! Memory: a chunk quinn hands out may share an allocation larger than
//! itself, so the reader never keeps one. Every read copies: staged bytes into the stream's staging buffer, a short or
//! control body into one buffer that grows as data arrives and ends exactly
//! the declared length, and a `Stream`'s payload into fragments (the
//! `Rechunker`), each a buffer of exactly its length filled at its offsets, so
//! nothing the reader keeps pins a datagram. A fragment `Bytes` may therefore
//! outlive the inbound charge.
//!
//! Simulation: `VirtualSleep` has no deregistration on drop, so each
//! stream's last armed timer stays in `VirtualTime`'s pending wakeups until
//! its deadline passes (one per ended stream; the timer is not replaced on
//! progress, only when it fires).
//!
//! Streams with no budget charge get an age limit too, so a peer cannot hold
//! one of its stream slots by trickling bytes: [`HEADER_PHASE_AGE`] until
//! the header is read, then [`uncharged_age_limit`] (control frames, and
//! Shorts of one packet).
//!
//! Stream id lifetime: a `Stream` id is held on the path from admission
//! until all of its state is gone (the reader is done and every handle
//! clone dropped). The charge is released when the reader ends if it held
//! the last clone, and otherwise checked again whenever a stream with the
//! same id arrives, before it is refused as `StreamIdInUse`. Completed ids
//! are not remembered, so once released an id can be used again.

use std::collections::{BTreeMap, HashMap};
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU32, AtomicU64, AtomicUsize, Ordering};
use std::time::Duration;

use bytes::{Bytes, BytesMut};
use futures::StreamExt;
use futures::stream::FuturesUnordered;
use tokio::sync::mpsc;

use super::super::budget::ConnMeter;
use super::super::framing::{FrameHeader, FrameKind, HEADER_LEN, HeaderError};
use super::{QuicInboundOutcome, QuicPathStats, stop_code};
use crate::simulation::TimeSource;
use crate::transport::StreamId;
use crate::transport::metrics::{InboundStreamOutcome, QuicCloseCode};
use crate::transport::peer_connection::inbound_budget::{
    BudgetRejection, InboundKey, InboundStreamBudget, LegacyStreamToken, Liveness, Releaser,
    SharedInboundBudget,
};
use crate::transport::peer_connection::streaming::{
    PathRtt, QuicSource, QuicSourceEnd, QuicSourcePath, StreamError, StreamHandle, ThrottleProbe,
};
use crate::transport::peer_connection::streaming_buffer::FRAGMENT_PAYLOAD_SIZE;
use crate::transport::peer_connection::{
    fits_one_packet, inbound_stream_age_limit, streaming_handle_idle_timeout,
};

/// A short message from the peer. It holds its inbound budget charge until
/// its payload is taken.
pub(crate) struct InboundShort {
    payload: Bytes,
    /// What the charge is held under, so the consumer can release it at once.
    /// `None` for a message of one packet or less, which is not charged (as
    /// on UDP).
    key: Option<InboundKey>,
    _charge: Option<Arc<LegacyStreamToken>>,
}

impl std::fmt::Debug for InboundShort {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("InboundShort")
            .field("len", &self.payload.len())
            .finish_non_exhaustive()
    }
}

impl InboundShort {
    #[cfg(test)]
    pub(crate) fn payload(&self) -> &Bytes {
        &self.payload
    }

    /// The payload; its charge is released.
    #[cfg(test)]
    pub(crate) fn into_payload(self) -> Bytes {
        self.payload
    }

    /// The payload and the key of its charge, if it holds one; the state is
    /// then gone: release it with `release_if_gone`. Otherwise it holds room until something needs
    /// room or the UDP sweep runs.
    pub(in crate::transport) fn into_parts(self) -> (Bytes, Option<InboundKey>) {
        (self.payload, self.key)
    }
}

/// A stream from the peer, announced once its header and metadata have
/// arrived and before its payload. Its bytes arrive in `handle` as they are
/// received; the handle is cancelled if the stream fails.
///
/// `handle` is the transport-internal `StreamHandle`; the connection wraps it in
/// a `boundary::IncomingStream` before anything above the transport sees it.
#[derive(Debug)]
pub(crate) struct InboundStreamArrival {
    pub(crate) stream_id: StreamId,
    /// The message announcing the stream, if the sender sent one.
    pub(crate) metadata: Option<Bytes>,
    pub(crate) handle: StreamHandle,
}

/// Everything the receive task needs, shared by all its stream readers.
pub(in crate::transport::quic) struct Receiver<T> {
    time: T,
    budget: SharedInboundBudget,
    stats: Arc<QuicPathStats>,
    shorts: mpsc::Sender<InboundShort>,
    streams: mpsc::Sender<InboundStreamArrival>,
    controls: mpsc::Sender<Bytes>,
    /// Source of the budget keys short messages are charged under.
    next_short_key: AtomicU32,
    /// Whether Short and Stream frames are read (`QuicPathLimits::accept_data`).
    accept_data: bool,
    /// Closes the path on the first data frame it refuses ([`Self::accept_data`]).
    closer: super::super::QuicCloser,
    /// The connection's gaps over all its streams.
    ranges: Arc<ConnRanges>,
    /// The connection's share of the node's buffer budget, for a path that
    /// reads data (`budget::ConnMeter`); told of every read.
    meter: Option<Arc<ConnMeter>>,
    /// The connection's staged bytes, against their cap ([`ConnStaging`]).
    staging: Arc<ConnStaging>,
    /// The peer's uni-stream limit, lowered while one-packet Shorts wait for
    /// room ([`StreamLimit`]).
    stream_limit: Arc<StreamLimit>,
    /// The connection's round trip, for the consumers of the streams it
    /// fills (`StreamHandle::fragment_gap_limit`): set at each stream
    /// admitted and at each tick of the path's acknowledgement watch.
    rtt: Arc<PathRtt>,
    /// Panic on the next short message, to test the supervisor.
    #[cfg(test)]
    pub(in crate::transport::quic) panic_on_stream: bool,
    /// `QuicPathLimits::cancel_next_admitted`.
    #[cfg(test)]
    pub(in crate::transport::quic) cancel_next_admitted:
        Option<&'static std::sync::atomic::AtomicBool>,
}

impl<T: TimeSource> Receiver<T> {
    /// The connection's gap count.
    pub(in crate::transport::quic) fn ranges(&self) -> Arc<ConnRanges> {
        self.ranges.clone()
    }

    /// The connection's staged bytes, whose cap the meter sets.
    pub(in crate::transport::quic) fn staging(&self) -> Arc<ConnStaging> {
        self.staging.clone()
    }

    /// Let the peer of `connection` open `start` uni streams, raised with
    /// load up to `max`, less one for each one-packet Short waiting for room
    /// ([`StreamLimit`]). Before any stream is read.
    pub(in crate::transport::quic) fn set_stream_limit(
        &mut self,
        connection: quinn::Connection,
        start: u32,
        max: u32,
    ) {
        let set = move |limit: u32| {
            connection.set_max_concurrent_uni_streams(quinn::VarInt::from_u32(limit));
        };
        self.stream_limit = Arc::new(StreamLimit::new(Some(Box::new(set)), start, max));
        self.stream_limit.apply();
    }

    fn throttled(&self) -> bool {
        self.meter
            .as_ref()
            .is_some_and(|meter| meter.throttled_since(self.time.now_nanos()).is_some())
    }

    /// For the consumers of the streams this path fills: whether the budget
    /// throttles the connection now, on the path's clock. The probe holds the
    /// meter weakly: every clone and fork of a stream's handle shares it, and
    /// a handle can outlive its connection (the orphan registry, an op, a
    /// pipe), while dropping the meter is what takes the connection's windows
    /// out of the budget (`ConnMeter`'s drop). A connection gone is not
    /// throttled.
    fn throttle_probe(&self) -> Option<ThrottleProbe> {
        let meter = Arc::downgrade(self.meter.as_ref()?);
        let time = self.time.clone();
        Some(ThrottleProbe(Arc::new(move || {
            meter
                .upgrade()
                .is_some_and(|meter| meter.throttled_since(time.now_nanos()).is_some())
        })))
    }

    /// A data stream delivered all it declared. Its reader's ticket no
    /// longer holds back a raise ([`LimitTicket`]).
    fn completed(&self, reader: &mut BodyReader<T>) {
        let gapped = reader.ticket.as_mut().is_some_and(|ticket| {
            ticket.vote = Vote::Completed;
            ticket.gapped
        });
        let room = || {
            self.meter
                .as_ref()
                .is_none_or(|meter| meter.budget_has_room())
        };
        if self
            .stream_limit
            .on_completed(self.throttled(), gapped, room)
        {
            self.stats.record(QuicInboundOutcome::StreamLimitRaised);
        }
    }

    /// Tell `meter` of every read from now on. Before any stream is read.
    pub(in crate::transport::quic) fn set_meter(&mut self, meter: Arc<ConnMeter>) {
        self.meter = Some(meter);
    }

    /// The connection's round trip as the receiver keeps it, for the path's
    /// acknowledgement watch to keep current.
    pub(in crate::transport::quic) fn path_rtt(&self) -> Arc<PathRtt> {
        self.rtt.clone()
    }

    /// Replace the gap cap, before any stream is read. Tests only.
    #[cfg(test)]
    pub(in crate::transport::quic) fn set_gap_cap(&mut self, cap: usize) {
        self.ranges = Arc::new(ConnRanges::new(
            cap,
            super::super::config::MAX_UNI_STREAMS as usize,
        ));
    }

    /// `staging_cap` is the cap of the connection's staged bytes until a meter
    /// sets it ([`ConnStaging`]).
    #[allow(clippy::too_many_arguments)]
    pub(in crate::transport::quic) fn new(
        time: T,
        budget: SharedInboundBudget,
        stats: Arc<QuicPathStats>,
        data: super::QuicDataSenders,
        controls: mpsc::Sender<Bytes>,
        accept_data: bool,
        closer: super::super::QuicCloser,
        staging_cap: usize,
    ) -> Self {
        let super::QuicDataSenders { shorts, streams } = data;
        let node = stats.node();
        Self {
            time,
            budget,
            stats,
            shorts,
            streams,
            controls,
            next_short_key: AtomicU32::new(1),
            accept_data,
            rtt: Arc::new(PathRtt::new(closer.rtt())),
            closer,
            ranges: Arc::new(ConnRanges::new(
                MAX_GAPS_PER_CONNECTION,
                super::super::config::MAX_UNI_STREAMS as usize,
            )),
            meter: None,
            staging: Arc::new(ConnStaging::new(staging_cap, node)),
            stream_limit: Arc::new(StreamLimit::new(None, 0, 0)),
            #[cfg(test)]
            panic_on_stream: false,
            #[cfg(test)]
            cancel_next_admitted: None,
        }
    }
}

/// How long after its first refused data frame a path that accepts no data
/// closes: long enough for the stops it sent to reach the sender first.
pub(in crate::transport::quic) const REFUSAL_CLOSE_DELAY: Duration = Duration::from_secs(2);

/// The uni streams the peer may open: a base that starts low and rises with
/// load, lowered by one for each one-packet Short that waits for room in a
/// full channel.
///
/// **Waiting Shorts.** A waiting reader reads on, and reading the stream's
/// end frees quinn's slot for it, which would let the peer open another
/// stream at once. A one-packet Short is not charged (its sender may already
/// read it as `Sent`, so refusing it could lose it), so without this nothing
/// would bound how many wait. Lowering the limit holds back new stream credit
/// instead (quinn grants none while the streams it counts reach the limit),
/// so at most the limit of such Shorts wait, each one packet, and the
/// sender's further messages find no stream and go on UDP. Charged messages
/// are bounded by their charge and need none of this.
///
/// **The base.** A data path starts at [`DATA_START_UNI_STREAMS`]. It
/// doubles, up to [`MAX_UNI_STREAMS`], once the peer has had every stream it
/// may open busy at once and then as many streams again have completed
/// with no gap on any read (a gapped completion counts for nothing but does
/// not start the run over), with none failing in between, and, at the
/// completion that would raise, the budget not throttling the connection
/// and the node's buffer budget charged at most three quarters
/// (`budget::QuicBudget::has_room`). A stream is busy while it holds a QUIC
/// slot for the peer: from its accept until its end is read, it is stopped,
/// or its reader ends ([`LimitTicket`]). A reader waiting for channel room
/// or a consumer reads on, and reads the end once the stream is whole, so a
/// slow consumer here does not keep the stream busy and earns the peer no
/// raise. A stream that ends
/// any way but a completion, a reset by its sender or the connection
/// ending (a refused header, a duplicate id, a budget refusal, a failed
/// read) starts the run over. A raise is never taken back: quinn cannot
/// take stream credit back, and lowering on load would churn.
///
/// Changed only from the path's receive task, where every reader runs, so
/// the limits quinn is told arrive in the order the counts moved.
///
/// [`DATA_START_UNI_STREAMS`]: super::super::config::DATA_START_UNI_STREAMS
/// [`MAX_UNI_STREAMS`]: super::super::config::MAX_UNI_STREAMS
struct StreamLimit {
    /// Tells quinn the limit; `None` on a path that reads no data.
    set: Option<Box<dyn Fn(u32) + Send + Sync>>,
    base: AtomicU32,
    max: u32,
    waiting: AtomicU32,
    /// Streams busy now ([`LimitTicket`]).
    busy: AtomicU32,
    /// Every stream the peer may open was busy at once since the base was
    /// last set or a stream last failed.
    saturated: AtomicBool,
    /// Streams completed since then.
    completed: AtomicU32,
}

impl StreamLimit {
    fn new(set: Option<Box<dyn Fn(u32) + Send + Sync>>, base: u32, max: u32) -> Self {
        Self {
            set,
            base: AtomicU32::new(base),
            max,
            waiting: AtomicU32::new(0),
            busy: AtomicU32::new(0),
            saturated: AtomicBool::new(false),
            completed: AtomicU32::new(0),
        }
    }

    fn apply(&self) {
        if let Some(set) = &self.set {
            let base = self.base.load(Ordering::Relaxed);
            let waiting = self.waiting.load(Ordering::Relaxed);
            set(base.saturating_sub(waiting));
        }
    }

    /// One more Short waits, until the guard is dropped.
    fn hold(&self) -> StreamLimitHold<'_> {
        self.waiting.fetch_add(1, Ordering::Relaxed);
        self.apply();
        StreamLimitHold(self)
    }

    /// The peer opened a stream: busy until its ticket says otherwise.
    fn open(self: &Arc<Self>) -> LimitTicket {
        let busy = self.busy.fetch_add(1, Ordering::Relaxed).saturating_add(1);
        if busy >= self.base.load(Ordering::Relaxed) {
            self.saturated.store(true, Ordering::Relaxed);
        }
        LimitTicket {
            limit: self.clone(),
            busy: true,
            gapped: false,
            vote: Vote::Failed,
        }
    }

    /// A stream delivered all it declared; `throttled` if the budget is
    /// holding the connection back now, `gapped` if the stream ever had a
    /// gap (it then counts for nothing, and does not start the run over);
    /// `room` says whether the node's buffer budget has room
    /// (`budget::QuicBudget::has_room`), asked only of the completion that
    /// would raise. True if the base was raised.
    fn on_completed(&self, throttled: bool, gapped: bool, room: impl FnOnce() -> bool) -> bool {
        if self.set.is_none() || !self.saturated.load(Ordering::Relaxed) || gapped {
            return false;
        }
        let completed = self
            .completed
            .fetch_add(1, Ordering::Relaxed)
            .saturating_add(1);
        let base = self.base.load(Ordering::Relaxed);
        if completed < base || throttled || base >= self.max || !room() {
            return false;
        }
        self.base
            .store(base.saturating_mul(2).min(self.max), Ordering::Relaxed);
        self.restart();
        self.apply();
        true
    }

    /// A stream failed: a pending raise needs a fresh run of load.
    fn on_failed(&self) {
        self.restart();
    }

    fn restart(&self) {
        self.saturated.store(false, Ordering::Relaxed);
        self.completed.store(0, Ordering::Relaxed);
    }

    /// The base now. Tests only.
    #[cfg(test)]
    fn base(&self) -> u32 {
        self.base.load(Ordering::Relaxed)
    }

    /// Streams busy now. Tests only.
    #[cfg(test)]
    fn busy(&self) -> u32 {
        self.busy.load(Ordering::Relaxed)
    }
}

struct StreamLimitHold<'a>(&'a StreamLimit);

impl Drop for StreamLimitHold<'_> {
    fn drop(&mut self) {
        self.0.waiting.fetch_sub(1, Ordering::Relaxed);
        self.0.apply();
    }
}

/// How a stream's end counts toward a raise of the stream limit.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Vote {
    /// Not completed: starts the run over. What a stream counts as unless
    /// its reader says otherwise, so a new way for a stream to end cannot
    /// let a failing peer keep earning a raise.
    Failed,
    /// Reset by its sender, the connection ended, or ended by this node
    /// ([`ended_locally`]): counts neither way.
    Neutral,
    /// Delivered whole ([`Receiver::completed`]).
    Completed,
}

/// One accepted stream's place in its path's [`StreamLimit`]: busy until
/// [`Self::idle`] or the drop, and its [`Vote`], cast on the drop.
struct LimitTicket {
    limit: Arc<StreamLimit>,
    busy: bool,
    /// The stream had a gap at some read.
    gapped: bool,
    vote: Vote,
}

/// The stream ended on this side's doing (its consumer cancelled it, or the
/// node's consumer of new streams is gone): counts neither way, as a reset
/// by the peer does not. Holding it against the peer would restart its run
/// toward a raise for what this node did.
fn ended_locally<T: TimeSource>(reader: &mut BodyReader<T>) {
    if let Some(ticket) = &mut reader.ticket {
        ticket.vote = Vote::Neutral;
    }
}

impl LimitTicket {
    /// The stream no longer waits on the peer.
    fn idle(&mut self) {
        if std::mem::take(&mut self.busy) {
            self.limit.busy.fetch_sub(1, Ordering::Relaxed);
        }
    }
}

impl Drop for LimitTicket {
    fn drop(&mut self) {
        self.idle();
        if self.vote == Vote::Failed {
            self.limit.on_failed();
        }
    }
}

/// Accept every uni stream of `connection` and read them all concurrently
/// in this one task, until the connection ends; then let the readers see the
/// loss and end. A reader may wait for room in a full channel, but only that
/// reader: the readers are separate futures, so one stream cannot hold up
/// another.
pub(in crate::transport::quic) async fn receive<T: TimeSource>(
    connection: quinn::Connection,
    receiver: Arc<Receiver<T>>,
) {
    let mut readers = FuturesUnordered::new();
    // Kept across iterations, so a pending accept is never dropped.
    let mut accept = std::pin::pin!(connection.accept_uni());
    // This path's peak of streams read at once (`quic_streams_open_*`).
    let mut peak = crate::transport::metrics::StreamsOpenPeak::default();
    // A path that sends but reads little still has its windows evaluated
    // when its sends progress (`budget::ConnMeter::note_send`), here in the
    // task that reads, so the meter's evaluations never run concurrently.
    let meter = receiver.meter.clone();
    let mut sending = std::pin::pin!(send_progress(&meter));
    loop {
        tokio::select! {
            () = &mut sending => {
                if let Some(meter) = &meter {
                    meter.maybe_evaluate(receiver.time.now_nanos());
                }
                sending.set(send_progress(&meter));
            }
            accepted = &mut accept => match accepted {
                Ok(recv) => {
                    let ticket = receiver.stream_limit.open();
                    readers.push(read_one(recv, receiver.clone(), ticket));
                    receiver.stats.node().record_streams_open(
                        &mut peak,
                        u32::try_from(readers.len()).unwrap_or(u32::MAX),
                    );
                    accept.set(connection.accept_uni());
                }
                Err(error) => {
                    tracing::debug!(%error, "QUIC path: no more inbound streams");
                    break;
                }
            },
            Some(()) = readers.next(), if !readers.is_empty() => {}
        }
    }
    while readers.next().await.is_some() {}
}

/// Completes when a send on the path made progress; never without a meter.
async fn send_progress(meter: &Option<Arc<ConnMeter>>) {
    match meter {
        Some(meter) => meter.send_progress().await,
        None => std::future::pending().await,
    }
}

/// Why reading a stream stopped before it was done.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ReadFailure {
    /// The stream ended after this many bytes of the part being read.
    Truncated(usize),
    /// More bytes than declared.
    Overlong,
    Idle,
    Age,
    Reset,
    ConnectionLost,
    /// The connection's streams held more gaps than
    /// [`MAX_GAPS_PER_CONNECTION`].
    TooManyRanges,
    /// quinn delivered a byte twice, which an unordered read never does.
    Overlap,
    /// Staging the bytes just read would have taken the connection's staged
    /// bytes past their cap ([`ConnStaging`]).
    StagedOverCap,
    /// The age limit came while the idle timer was paused for our own
    /// throttle ([`BodyReader::read_piece`]).
    ThrottledPastAge,
    Other,
}

impl From<quinn::ReadError> for ReadFailure {
    /// Through the one quinn error mapping (`quic::error`), so a read
    /// failure is classified the same way here and in a `QuicError`.
    fn from(error: quinn::ReadError) -> Self {
        use crate::transport::QuicErrorKind;
        match QuicErrorKind::from(error) {
            QuicErrorKind::Reset { .. } => Self::Reset,
            QuicErrorKind::ConnectionLost
            | QuicErrorKind::Refused { .. }
            | QuicErrorKind::TimedOut
            | QuicErrorKind::EndpointDown
            | QuicErrorKind::Protocol { .. } => Self::ConnectionLost,
            // A read never maps to Stopped or InvalidFrame.
            QuicErrorKind::StreamClosed
            | QuicErrorKind::Stopped { .. }
            | QuicErrorKind::InvalidFrame => Self::Other,
        }
    }
}

/// Whether a stream holds a budget charge, which decides how an idle or
/// age eviction is counted.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Charged {
    No,
    Yes,
}

/// Reads one QUIC stream with the idle and age limits applied, unordered from
/// its first byte.
///
/// One timer at a time: it is armed for the earlier of the idle and age
/// deadlines and, when it fires, re-armed if progress has moved the idle
/// deadline. So a stream costs one sleep per idle period, not one per chunk.
struct BodyReader<T: TimeSource> {
    recv: quinn::RecvStream,
    time: T,
    idle_nanos: u64,
    /// Start time plus the age limit (the uncharged one until admitted).
    age_deadline: u64,
    last_progress: u64,
    timer: Pin<Box<dyn Future<Output = ()> + Send>>,
    /// The deadline `timer` is armed for.
    timer_at: u64,
    /// One past the last offset this stream may carry, once its header is
    /// known; a byte past it is `Overlong`.
    end: Option<u64>,
    /// quinn reported the end of the stream, and so freed its state.
    finished: bool,
    /// quinn reported a reset, and so freed the stream's state.
    reset: bool,
    /// Every byte range received on this stream, merged.
    ranges: RangeMirror,
    /// The connection's count of ranges over all its streams.
    conn_ranges: Arc<ConnRanges>,
    /// The connection's budget meter, told of each read.
    meter: Option<Arc<ConnMeter>>,
    /// Bytes read but not yet taken ([`Self::read_exact`]).
    staging: Staging,
    /// The admitted `Stream`'s source, where a failure records why the
    /// stream ended, for consumers that assemble it ([`Receiver::fail`]).
    source: Option<Arc<QuicSource>>,
    /// The idle timer is paused for the connection's throttle: set when
    /// the throttle pauses it, cleared by progress and when the throttle
    /// has lapsed.
    paused: bool,
    /// A byte has arrived on the stream.
    progressed: bool,
    /// The stream's place in its path's stream limit; `None` in tests that
    /// build a reader alone.
    ticket: Option<LimitTicket>,
}

impl<T: TimeSource> BodyReader<T> {
    /// A reader with the idle timeout and an age limit of
    /// [`header_phase_age_limit`] until the header is read
    /// ([`uncharged_age_limit`] after it); admission replaces the age limit with the
    /// stream's own ([`Self::set_age_limit`]).
    fn new(
        recv: quinn::RecvStream,
        time: T,
        idle: Duration,
        conn_ranges: Arc<ConnRanges>,
        meter: Option<Arc<ConnMeter>>,
        conn_staging: Arc<ConnStaging>,
    ) -> Self {
        let now = time.now_nanos();
        let idle_nanos = nanos(idle);
        let age_deadline = now.saturating_add(nanos(header_phase_age_limit()));
        let timer_at = now.saturating_add(idle_nanos).min(age_deadline);
        let timer = time.sleep_until(timer_at);
        Self {
            recv,
            time,
            idle_nanos,
            age_deadline,
            last_progress: now,
            timer,
            timer_at,
            end: None,
            finished: false,
            reset: false,
            ranges: RangeMirror::default(),
            conn_ranges,
            meter,
            staging: Staging::new(conn_staging),
            source: None,
            paused: false,
            progressed: false,
            ticket: None,
        }
    }

    fn deadline(&self) -> u64 {
        self.last_progress
            .saturating_add(self.idle_nanos)
            .min(self.age_deadline)
    }

    /// The stream may carry bytes up to `end` (exclusive), counted from its
    /// first byte (the header's). `Overlong` if a byte past it was already
    /// read.
    fn set_end(&mut self, end: u64) -> Result<(), ReadFailure> {
        if self.ranges.last_stop() > end {
            return Err(ReadFailure::Overlong);
        }
        self.end = Some(end);
        Ok(())
    }

    /// Every byte up to the declared end has been read; only the end of the
    /// stream itself is left, which [`Self::next_piece`] would read next.
    fn all_read(&self) -> bool {
        self.end.is_some_and(|end| self.ranges.is_exactly(end))
    }

    /// The next piece of the stream in arrival order, with its offset, or
    /// `None` at the end. Read unordered (`read_chunk(max, false)`), so quinn's
    /// releases each chunk as soon as it arrives, gap or not: the
    /// bytes then live only where the caller copies them, which is charged or
    /// staged against a cap.
    async fn next_piece(&mut self) -> Result<Option<(u64, Bytes)>, ReadFailure> {
        self.read_piece(usize::MAX).await
    }

    /// What one `read_chunk` returned, with the declared end and the
    /// connection's gap cap applied.
    fn on_read(
        &mut self,
        read: Result<Option<quinn::Chunk>, quinn::ReadError>,
    ) -> Result<Option<(u64, Bytes)>, ReadFailure> {
        match read {
            Ok(Some(chunk)) => {
                let now = self.time.now_nanos();
                self.last_progress = now;
                self.paused = false;
                self.progressed = true;
                let start = chunk.offset;
                let stop = start.saturating_add(chunk.bytes.len() as u64);
                if self.end.is_some_and(|end| stop > end) {
                    return Err(ReadFailure::Overlong);
                }
                let prefix = self.ranges.prefix();
                let added = self.ranges.insert(start, stop)?;
                if let Some(meter) = &self.meter {
                    // Every byte read is a credit quinn returns; only bytes that
                    // extend the in-order prefix are delivery that grows a window.
                    meter.on_read(
                        stop - start,
                        self.ranges.prefix().saturating_sub(prefix),
                        now,
                    );
                }
                if !self.conn_ranges.add(added, now) {
                    return Err(ReadFailure::TooManyRanges);
                }
                // A hole between two received runs, or before the first: a
                // first packet that arrives after a later one (lost, or
                // reordered) leaves no run at offset 0. A lost last packet
                // leaves no hole to see: until its retransmission the stream
                // reads as one whose sender is slow, so it is not counted.
                let gapped = self.ranges.gaps() > 0
                    || (self.ranges.last_stop() > 0 && self.ranges.prefix() == 0);
                if let Some(ticket) = &mut self.ticket {
                    ticket.gapped |= gapped;
                }
                Ok(Some((start, chunk.bytes)))
            }
            Ok(None) => {
                self.finished = true;
                if let Some(ticket) = &mut self.ticket {
                    ticket.idle();
                }
                Ok(None)
            }
            Err(error) => {
                let failure = ReadFailure::from(error);
                // quinn frees the stream's state when it reports a reset
                // (`Chunks::next`), and all of it with the connection.
                self.reset = matches!(failure, ReadFailure::Reset | ReadFailure::ConnectionLost);
                Err(failure)
            }
        }
    }

    /// One `read_chunk(max, false)`, with the idle and age limits, the
    /// declared end and the connection's gap cap applied.
    async fn read_piece(&mut self, max: usize) -> Result<Option<(u64, Bytes)>, ReadFailure> {
        if self.finished {
            return Ok(None);
        }
        loop {
            // Biased so a read that is ready wins over a timer that fired at
            // the same time: a read is progress for the idle timer. The timer
            // cannot starve: each call returns after one read, and the timer
            // is polled again on the next call; a read is ready only while the
            // peer sends, and every byte it sends is bounded (staged against a
            // cap, or stopped at the declared end).
            // Cancellation: quinn's `read_chunk` is cancel-safe (a dropped
            // read loses no data), and the timer is kept across calls.
            tokio::select! {
                biased;
                read = self.recv.read_chunk(max, false) => return self.on_read(read),
                () = &mut self.timer => {
                    let now = self.time.now_nanos();
                    let throttle = self
                        .meter
                        .as_ref()
                        .and_then(|meter| meter.throttled_since(now));
                    // A pause lasts only while the throttle does, so an age
                    // limit reached after it lapsed counts as `Age`.
                    if throttle.is_none() {
                        self.paused = false;
                    }
                    // Idle first, as the UDP sweep does.
                    if now >= self.last_progress.saturating_add(self.idle_nanos)
                        && !self.pause_for_throttle(now, throttle)
                    {
                        return Err(ReadFailure::Idle);
                    }
                    if now >= self.age_deadline {
                        return Err(if self.paused {
                            ReadFailure::ThrottledPastAge
                        } else {
                            ReadFailure::Age
                        });
                    }
                    self.timer_at = self.deadline();
                    self.timer = self.time.sleep_until(self.timer_at);
                }
            }
        }
    }

    /// The idle timeout passed at `now`. If the budget is throttling the
    /// connection (since `began`) and this stream was not already idle when
    /// the throttle began (a byte arrived on it in the idle period before),
    /// the slowdown is ours, not the peer's: the idle timer is paused
    /// (restarted from now) and true is returned. A stream already silent
    /// then, or one that never received a byte, keeps its idle timer
    /// (buffer-budget design M6). The age limit still applies.
    ///
    /// The throttle cannot tell why the connection was held back, so a peer
    /// that sends flat out and fills the window it was given counts as
    /// throttled too (`budget::Evaluation::throttled`), pausing the idle
    /// timers of its own other streams that were active when it began. That
    /// gives a peer nothing it could not have by trickling a byte per idle
    /// period: each stream still ends at its age limit.
    fn pause_for_throttle(&mut self, now: u64, began: Option<u64>) -> bool {
        if !self.progressed || !idle_is_ours(self.last_progress, self.idle_nanos, began) {
            return false;
        }
        self.paused = true;
        self.last_progress = now;
        true
    }

    /// Start the age limit, from now. The armed timer is kept when it
    /// fires no later than the new deadline (it re-arms when it fires), so
    /// no extra sleep is left behind (`VirtualSleep` has no deregistration
    /// on drop, the same reason `QuicRuntime` keeps its armed sleep).
    fn set_age_limit(&mut self, limit: Duration) {
        self.age_deadline = self.time.now_nanos().saturating_add(nanos(limit));
        self.rearm_if_later_than(self.deadline());
    }

    fn rearm_if_later_than(&mut self, deadline: u64) {
        if self.timer_at > deadline {
            self.timer_at = deadline;
            self.timer = self.time.sleep_until(deadline);
        }
    }

    /// Wait for `ready`, but not past the age deadline: `Err(Age)` if that
    /// comes first. For a stream whose end has been read: nothing is left to
    /// read, so idleness does not apply (a consumer is being waited on, not
    /// the peer). When the wait ends in `Ok`, idleness restarts from that
    /// moment. Arms one new sleep for the wait and, if the idle deadline is
    /// earlier than the age deadline, another after it, so call it only when
    /// `ready` is not already available.
    async fn until_age<F: Future>(&mut self, ready: F) -> Result<F::Output, ReadFailure> {
        debug_assert!(self.finished, "a reader that waits keeps reading");
        if self.timer_at != self.age_deadline {
            self.timer_at = self.age_deadline;
            self.timer = self.time.sleep_until(self.timer_at);
        }
        let out = tokio::select! {
            out = ready => out,
            () = &mut self.timer => return Err(ReadFailure::Age),
        };
        // Re-arming alone would fail a wait longer than the idle timeout as
        // Idle at once, so progress is reset first.
        self.last_progress = self.time.now_nanos();
        self.timer_at = self.deadline();
        self.timer = self.time.sleep_until(self.timer_at);
        Ok(out)
    }

    /// Wait for `ready` while reading on: the reader never leaves data with
    /// quinn. Every byte up to the declared end has been read, so a byte read
    /// now is past it (`Overlong`); the end of the stream may be read here,
    /// which frees quinn's slot, and the wait then goes on to the age limit
    /// ([`Self::until_age`]).
    async fn read_while_waiting<F: Future>(&mut self, ready: F) -> Result<F::Output, ReadFailure> {
        debug_assert!(self.all_read());
        let mut ready = std::pin::pin!(ready);
        if !self.finished {
            enum Next<O> {
                Ready(O),
                Read(Result<Option<(u64, Bytes)>, ReadFailure>),
            }
            let next = tokio::select! {
                out = &mut ready => Next::Ready(out),
                read = self.next_piece() => Next::Read(read),
            };
            match next {
                Next::Ready(out) => return Ok(out),
                Next::Read(Ok(Some(_))) => return Err(ReadFailure::Overlong),
                Next::Read(Ok(None)) => {}
                Next::Read(Err(failure)) => return Err(failure),
            }
        }
        self.until_age(ready).await
    }

    /// Exactly `len` bytes (`len` > 0) from the read position, the next bytes
    /// not yet taken. Pieces are read unordered and staged ([`Staging`]) until
    /// the bytes wanted are all here; pieces past them stay staged for the next
    /// take. The result is a buffer of its own: no chunk (and no datagram it is
    /// a slice of) is kept.
    async fn read_exact(&mut self, len: usize) -> Result<Bytes, ReadFailure> {
        let want = self.staging.taken.saturating_add(len as u64);
        while self.ranges.contiguous_from(self.staging.taken) < want {
            match self.next_piece().await? {
                Some((offset, bytes)) => self.staging.stage(offset, &bytes, self.end)?,
                None => {
                    let got = self
                        .ranges
                        .contiguous_from(self.staging.taken)
                        .saturating_sub(self.staging.taken);
                    return Err(ReadFailure::Truncated(clamp(got)));
                }
            }
        }
        Ok(self.staging.take(len))
    }

    /// [`Self::read_exact`], but a piece read meanwhile at or past both the
    /// wanted bytes' end and what is already staged goes to `beyond` instead
    /// of being staged (an admitted Stream's payload, which has a charged
    /// buffer to go to, so it is never staged twice). A piece across that
    /// point is split. `beyond` returning false ends the read with
    /// `ReadFailure::Other`; it has recorded why.
    async fn read_exact_with(
        &mut self,
        len: usize,
        mut beyond: impl FnMut(u64, &[u8]) -> bool,
    ) -> Result<Bytes, ReadFailure> {
        let want = self.staging.taken.saturating_add(len as u64);
        while self.ranges.contiguous_from(self.staging.taken) < want {
            match self.next_piece().await? {
                Some((offset, bytes)) => {
                    let split = want.max(self.staging.stop());
                    let stop = offset.saturating_add(bytes.len() as u64);
                    let cut = usize::try_from(split.saturating_sub(offset))
                        .unwrap_or(usize::MAX)
                        .min(bytes.len());
                    if cut > 0 {
                        self.staging.stage(offset, &bytes[..cut], self.end)?;
                    }
                    if stop > split && !beyond(offset + cut as u64, &bytes[cut..]) {
                        return Err(ReadFailure::Other);
                    }
                }
                None => {
                    let got = self
                        .ranges
                        .contiguous_from(self.staging.taken)
                        .saturating_sub(self.staging.taken);
                    return Err(ReadFailure::Truncated(clamp(got)));
                }
            }
        }
        Ok(self.staging.take(len))
    }

    /// The rest of the stream up to its declared end, which is `len` bytes
    /// from `start` (the read position after the header), into one buffer
    /// written at each piece's offset: the staged pieces first, then pieces
    /// read unordered ([`Self::next_piece`]). The buffer grows to the highest
    /// offset received, never past the declared end, which the charge (or one
    /// packet, for an uncharged message) bounds. `progress` is told the bytes
    /// held after each piece.
    async fn read_rest_unordered_with(
        &mut self,
        start: u64,
        len: usize,
        mut progress: impl FnMut(usize),
    ) -> Result<Bytes, ReadFailure> {
        let mut out: Vec<u8> = Vec::new();
        let mut held = 0usize;
        let mut staged = Ok(());
        self.staging.drain(&self.ranges, |offset, bytes| {
            if staged.is_ok() {
                staged = put_at(&mut out, start, len, offset, bytes);
                held = held.saturating_add(bytes.len());
            }
        });
        staged?;
        progress(held);
        while !self.all_read() {
            match self.next_piece().await? {
                Some((offset, bytes)) => {
                    put_at(&mut out, start, len, offset, &bytes)?;
                    held = held.saturating_add(bytes.len());
                    progress(held);
                }
                None => return Err(ReadFailure::Truncated(held)),
            }
        }
        out.shrink_to_fit();
        Ok(Bytes::from(out))
    }

    /// The end of the stream, with nothing more before it.
    async fn expect_end(&mut self) -> Result<(), ReadFailure> {
        match self.next_piece().await? {
            None => Ok(()),
            Some(_) => Err(ReadFailure::Overlong),
        }
    }

    /// Stop the stream (STOP_SENDING) with `code`. Harmless if it already
    /// ended. Nothing more is read from it.
    fn stop(&mut self, code: u32) {
        self.recv.stop(code.into()).ok();
        if let Some(ticket) = &mut self.ticket {
            ticket.idle();
        }
    }
}

impl<T: TimeSource> Drop for BodyReader<T> {
    fn drop(&mut self) {
        let gaps = self.ranges.gaps();
        if self.finished || self.reset {
            self.conn_ranges.remove(gaps);
        } else {
            // A stream stopped unfinished keeps its gaps counted, since
            // nothing tells us when the peer ends it. See `ConnRanges::keep`.
            self.conn_ranges.keep(gaps, self.time.now_nanos());
        }
    }
}

/// Whether a stream that last made progress at `last_progress` and has now
/// passed the idle timeout `idle_nanos` is idle because of our own throttle,
/// which began at `throttle_began` if it is on: it was not already idle when
/// the throttle began.
fn idle_is_ours(last_progress: u64, idle_nanos: u64, throttle_began: Option<u64>) -> bool {
    throttle_began.is_some_and(|began| last_progress.saturating_add(idle_nanos) > began)
}

/// Copy `bytes`, read at stream offset `offset`, into `out`, the buffer of a
/// body of `len` bytes starting at stream offset `start`. `out` grows to the
/// highest offset written, doubling up to `len` (so it holds at most twice
/// what has arrived) and ending exact once the caller shrinks it; a byte
/// past `len` is `Overlong`.
fn put_at(
    out: &mut Vec<u8>,
    start: u64,
    len: usize,
    offset: u64,
    bytes: &[u8],
) -> Result<(), ReadFailure> {
    let Some(at) = offset
        .checked_sub(start)
        .and_then(|at| usize::try_from(at).ok())
    else {
        return Err(ReadFailure::Other);
    };
    let stop = at.saturating_add(bytes.len());
    if stop > len {
        return Err(ReadFailure::Overlong);
    }
    if out.len() < stop {
        if stop > out.capacity() {
            let target = out.capacity().saturating_mul(2).max(stop).min(len);
            out.reserve_exact(target - out.len());
        }
        out.resize(stop, 0);
    }
    out[at..stop].copy_from_slice(bytes);
    Ok(())
}

/// How long after a stream is stopped for passing the connection's staging cap
/// a second one closes the path ([`ConnStaging`]).
pub(in crate::transport::quic) const STAGED_TRIP_WINDOW: Duration = Duration::from_secs(30);

/// Charged per staging buffer besides its allocation: the allocator's
/// bookkeeping and the reader's handle on it.
pub(in crate::transport::quic) const STAGING_BUFFER_OVERHEAD_BYTES: usize = 64;

/// The size of the allocation the allocator makes for `bytes`, rounded up to
/// its size class: four classes per doubling, as jemalloc's (at most a quarter
/// over `bytes`). Charged for a staging buffer so its cost is what it occupies.
pub(in crate::transport::quic) fn allocation_class(bytes: usize) -> usize {
    if bytes <= 16 {
        return 16;
    }
    let order = usize::BITS - 1 - (bytes - 1).leading_zeros();
    let step = 1usize << order.saturating_sub(2);
    bytes.div_ceil(step).saturating_mul(step)
}

/// A connection's staged bytes: what its readers hold in [`Staging`] buffers,
/// charged at their allocation size, against a cap. A data path's meter sets
/// the cap to twice the connection's credit (`budget::staging_cap`); a path
/// without one keeps the cap it was built with.
#[derive(Debug)]
pub(in crate::transport::quic) struct ConnStaging {
    held: AtomicUsize,
    /// The most held at once, and the cap then, as a per-mille of it.
    /// Tests only.
    #[cfg(test)]
    peak_permille: AtomicUsize,
    cap: AtomicUsize,
    /// When a stream was last stopped for passing the cap, in the time
    /// source's nanoseconds plus one (0: never).
    last_trip: AtomicU64,
    /// Told of each new level, as a percentage of the cap
    /// (`quic_staged_peak_pct`).
    node: &'static crate::transport::metrics::QuicDataCounters,
}

impl ConnStaging {
    pub(in crate::transport::quic) fn new(
        cap: usize,
        node: &'static crate::transport::metrics::QuicDataCounters,
    ) -> Self {
        Self {
            held: AtomicUsize::new(0),
            #[cfg(test)]
            peak_permille: AtomicUsize::new(0),
            cap: AtomicUsize::new(cap),
            last_trip: AtomicU64::new(0),
            node,
        }
    }

    /// Charge `bytes` more; false, charging nothing, if that would pass the cap.
    fn try_add(&self, bytes: usize) -> bool {
        let cap = self.cap.load(Ordering::Relaxed);
        let added = self
            .held
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |held| {
                held.checked_add(bytes).filter(|after| *after <= cap)
            });
        match added {
            Ok(before) => {
                let pct = (before + bytes).saturating_mul(100) / cap.max(1);
                self.node.record_staged_level(pct as u64);
                #[cfg(test)]
                self.peak_permille.fetch_max(
                    (before + bytes).saturating_mul(1000) / cap.max(1),
                    Ordering::Relaxed,
                );
                true
            }
            Err(_) => false,
        }
    }

    fn release(&self, bytes: usize) {
        // The closure never refuses, so this cannot fail.
        self.held
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |held| {
                Some(held.saturating_sub(bytes))
            })
            .ok();
    }

    /// A stream was stopped for passing the cap at `now`; true if another was
    /// within [`STAGED_TRIP_WINDOW`] before, which closes the path.
    fn trip(&self, now: u64) -> bool {
        let before = self
            .last_trip
            .swap(now.saturating_add(1), Ordering::Relaxed);
        before != 0 && now.saturating_sub(before - 1) <= nanos(STAGED_TRIP_WINDOW)
    }

    /// Set the cap, from the connection's credit (`budget::staging_cap`),
    /// but never below what is staged now: a shrink must not leave staged
    /// bytes over the cap, which would evict the next honest piece and leave
    /// the bytes held uncharged. The meter charges this cap. Called from the
    /// receive task only, as every staging read is, so nothing is staged
    /// between the two loads.
    pub(in crate::transport::quic) fn set_cap(&self, cap: usize) {
        let held = self.held.load(Ordering::Relaxed);
        self.cap.store(cap.max(held), Ordering::Relaxed);
    }

    /// The cap now.
    pub(in crate::transport::quic) fn cap(&self) -> usize {
        self.cap.load(Ordering::Relaxed)
    }

    /// Stage `bytes` as a reader would; false past the cap. Tests only.
    #[cfg(test)]
    pub(in crate::transport::quic) fn add_for_test(&self, bytes: usize) -> bool {
        self.try_add(bytes)
    }

    /// The most staged at once, per mille of the cap then. Tests only.
    #[cfg(test)]
    pub(in crate::transport::quic) fn peak_permille(&self) -> usize {
        self.peak_permille.load(Ordering::Relaxed)
    }

    /// Staged bytes charged now. Tests only.
    #[cfg(test)]
    pub(in crate::transport::quic) fn held(&self) -> usize {
        self.held.load(Ordering::Relaxed)
    }
}

/// One stream's bytes read ahead of what its reader has taken: one buffer
/// spanning from the first byte not yet taken to the highest byte staged, so
/// gaps between pieces are part of the allocation and charged as such, and a
/// piece costs what it occupies however small it is. The buffer is charged to
/// the connection's [`ConnStaging`] before it grows, at its allocation size
/// plus [`STAGING_BUFFER_OVERHEAD_BYTES`], and released when it is emptied or
/// dropped.
struct Staging {
    conn: Arc<ConnStaging>,
    /// Stream offset of `buf[0]`.
    base: u64,
    buf: Vec<u8>,
    /// Stream offset of the first byte not yet taken (`base` <= `taken`).
    taken: u64,
    /// What this buffer is charged now.
    charged: usize,
}

impl Staging {
    fn new(conn: Arc<ConnStaging>) -> Self {
        Self {
            conn,
            base: 0,
            buf: Vec::new(),
            taken: 0,
            charged: 0,
        }
    }

    /// Stage `bytes`, read at stream offset `offset` (at or past `taken`:
    /// quinn delivers each byte once and the reader's mirror refuses a
    /// repeat). `StagedOverCap` if the buffer would have to grow past what
    /// the connection's cap leaves; it is then left as it was.
    fn stage(&mut self, offset: u64, bytes: &[u8], end: Option<u64>) -> Result<(), ReadFailure> {
        if bytes.is_empty() {
            return Ok(());
        }
        if self.buf.is_empty() {
            self.base = self.taken;
        }
        let Some(at) = offset
            .checked_sub(self.base)
            .and_then(|at| usize::try_from(at).ok())
        else {
            return Err(ReadFailure::Other);
        };
        let need = at.saturating_add(bytes.len());
        if need > self.buf.capacity() {
            // To the allocator's size class of what is needed (at most a
            // quarter over, so growth stays geometric), never past the
            // declared end once it is known: the charge stays within 1.25 x
            // the span staged.
            let mut target = allocation_class(need);
            if let Some(end) = end {
                let span = usize::try_from(end.saturating_sub(self.base)).unwrap_or(usize::MAX);
                target = target.min(span).max(need);
            }
            let charge = allocation_class(target) + STAGING_BUFFER_OVERHEAD_BYTES;
            if !self.conn.try_add(charge.saturating_sub(self.charged)) {
                return Err(ReadFailure::StagedOverCap);
            }
            self.charged = self.charged.max(charge);
            self.buf.reserve_exact(target - self.buf.len());
        }
        if self.buf.len() < need {
            self.buf.resize(need, 0);
        }
        self.buf[at..need].copy_from_slice(bytes);
        Ok(())
    }

    /// One past the highest stream offset the buffer covers (`taken` if it
    /// is empty).
    fn stop(&self) -> u64 {
        if self.buf.is_empty() {
            self.taken
        } else {
            self.base.saturating_add(self.buf.len() as u64)
        }
    }

    /// Take the next `len` bytes, which the caller has checked are all
    /// staged. The buffer is freed once everything staged has been taken.
    fn take(&mut self, len: usize) -> Bytes {
        let from = usize::try_from(self.taken - self.base).unwrap_or(usize::MAX);
        let out = Bytes::copy_from_slice(&self.buf[from..from + len]);
        self.taken = self.taken.saturating_add(len as u64);
        if self.taken >= self.base.saturating_add(self.buf.len() as u64) {
            self.free();
        }
        out
    }

    /// Hand every staged byte not yet taken to `each`, as the received runs
    /// `ranges` records, and free the buffer. Taken runs past the received
    /// ones are not handed out (they hold no data, only the gap's zeros).
    fn drain(&mut self, ranges: &RangeMirror, mut each: impl FnMut(u64, &[u8])) {
        let stop = self.base.saturating_add(self.buf.len() as u64);
        for (start, end) in ranges.iter() {
            let (start, end) = (start.max(self.taken), end.min(stop));
            if start < end {
                let (from, to) = ((start - self.base) as usize, (end - self.base) as usize);
                each(start, &self.buf[from..to]);
            }
        }
        self.taken = self.taken.max(stop);
        self.free();
    }

    fn free(&mut self) {
        self.buf = Vec::new();
        self.base = self.taken;
        self.conn.release(std::mem::take(&mut self.charged));
    }
}

impl Drop for Staging {
    fn drop(&mut self) {
        self.conn.release(self.charged);
    }
}

/// The gap cap of a path with no budget meter (a shadow path, which reads no
/// data, or a test path): what a path at the floor window gets
/// (`budget::gap_cap`). Gaps are counted per stream as received ranges past
/// the first ([`RangeMirror::gaps`]), over open streams and the stopped ones
/// that still count ([`ConnRanges::keep`]). A contiguous stream costs
/// nothing here: its one range is bounded by the stream limit. Each gap is
/// charged about 70 B (`budget::GAP_BYTES`) and costs the peer at least one
/// byte of credit. A metered path's cap
/// follows its window: one gap per KiB of W + debt, at least this many
/// (`budget::ConnMeter`). Honest loss leaves about loss x packets in flight,
/// far fewer.
pub(in crate::transport::quic) const MAX_GAPS_PER_CONNECTION: usize =
    super::super::budget::MIN_GAPS_PER_CONNECTION;

/// How long a stopped stream's gaps count in full before up to
/// [`KEPT_GAPS_RELEASED`] of them leave the count. An honest peer ends a
/// stream it was asked to stop (STOP_SENDING) well within this; we cannot
/// observe when it does.
const KEPT_GAPS_GRACE: Duration = Duration::from_secs(30);

/// Gaps of one stopped stream that leave the count after
/// [`KEPT_GAPS_GRACE`]; any more stay counted. Honest loss on one stream
/// leaves fewer than this, so an honest stop costs nothing once its grace
/// is over.
const KEPT_GAPS_RELEASED: usize = 32;

/// The gaps of stopped streams that still count. Nothing is read from a
/// stream after its stop, so each stopped stream's gaps are frozen, and a
/// stopped stream holds one of the peer's stream slots until it ends, so at
/// most `limit` of them count at once. So the count is the sum of the `limit` largest frozen counts, not of all of them,
/// and does not grow with the connection's age. After the grace period each
/// counts [`KEPT_GAPS_RELEASED`] less. What may be held but not counted: at
/// most `limit` x 32 gaps x ~70 B (about 143 KB at 64), plus the same again
/// for entries dropped from the list (below).
#[derive(Debug, Default)]
struct Frozen {
    /// (grace end, gaps) per stopped stream that still counts; the grace
    /// end is `u64::MAX` once it is over and the 32 have been taken off.
    entries: Vec<(u64, usize)>,
    /// What `entries` adds to the connection's count now.
    counted: usize,
}

/// A connection's count of gaps over its streams, against
/// [`MAX_GAPS_PER_CONNECTION`].
#[derive(Debug)]
pub(in crate::transport::quic) struct ConnRanges {
    held: AtomicUsize,
    /// `R_conn`: set by the connection's meter as its window changes
    /// (`budget::gap_cap`), fixed for a path without one.
    cap: AtomicUsize,
    /// Most stopped streams that count at once: the stream limit.
    limit: usize,
    /// When a stream last went past the cap, in the time source's
    /// nanoseconds plus one (0: never).
    last_trip: AtomicU64,
    frozen: parking_lot::Mutex<Frozen>,
    /// Some entry of `frozen` is still in its grace period, so a read must
    /// check whether it has ended; false on the common path, which then
    /// takes no lock.
    grace_pending: AtomicBool,
    /// The most held at once. Tests only.
    #[cfg(test)]
    peak: AtomicUsize,
}

impl ConnRanges {
    /// A count against `cap`, for a connection whose peer may hold `limit`
    /// streams open at once.
    pub(in crate::transport::quic) fn new(cap: usize, limit: usize) -> Self {
        Self {
            held: AtomicUsize::new(0),
            cap: AtomicUsize::new(cap),
            limit: limit.max(1),
            last_trip: AtomicU64::new(0),
            frozen: parking_lot::Mutex::new(Frozen::default()),
            grace_pending: AtomicBool::new(false),
            #[cfg(test)]
            peak: AtomicUsize::new(0),
        }
    }

    /// A stream went past the cap at `now`; true if another had within
    /// [`STAGED_TRIP_WINDOW`] before, as for the staging cap. The trip stops
    /// the stream; a second within the window closes the path. An honest peer
    /// does not reach the cap once.
    fn trip(&self, now: u64) -> bool {
        let before = self
            .last_trip
            .swap(now.saturating_add(1), Ordering::Relaxed);
        before != 0 && now.saturating_sub(before - 1) <= nanos(STAGED_TRIP_WINDOW)
    }

    /// A stream's reader ended, stopped, with `gaps` that still count. Its open-stream gaps leave the count, and it joins [`Frozen`].
    fn keep(&self, gaps: usize, now: u64) {
        self.remove(gaps);
        let mut frozen = self.frozen.lock();
        if gaps > 0 {
            let due = now.saturating_add(nanos(KEPT_GAPS_GRACE));
            frozen.entries.push((due, gaps));
        }
        self.settle(&mut frozen, now);
    }

    /// Recount [`Frozen`] at `now`: the sum of the `limit` largest frozen
    /// counts, each [`KEPT_GAPS_RELEASED`] less once its grace is over.
    /// Entries that no longer count are dropped, and the list is cut back to
    /// `limit` once it reaches twice that: an entry outside the largest
    /// `limit` now can outrank one later only by the 32 a younger entry
    /// loses at its grace end, which is inside the stated uncounted bound.
    fn settle(&self, frozen: &mut Frozen, now: u64) {
        // An entry whose grace is over loses its 32 once, and is marked so
        // (grace end `u64::MAX`) that it is not taken down again.
        for entry in &mut frozen.entries {
            if entry.0 != u64::MAX && entry.0 <= now {
                *entry = (u64::MAX, entry.1.saturating_sub(KEPT_GAPS_RELEASED));
            }
        }
        frozen.entries.retain(|&(_, gaps)| gaps > 0);
        frozen
            .entries
            .sort_unstable_by_key(|&(_, gaps)| std::cmp::Reverse(gaps));
        if frozen.entries.len() >= 2 * self.limit {
            frozen.entries.truncate(self.limit);
        }
        let counted: usize = frozen
            .entries
            .iter()
            .take(self.limit)
            .map(|&(_, gaps)| gaps)
            .sum();
        let delta = counted as isize - frozen.counted as isize;
        frozen.counted = counted;
        self.change(delta);
        let pending = frozen.entries.iter().any(|&(due, _)| due != u64::MAX);
        self.grace_pending.store(pending, Ordering::Relaxed);
    }

    /// Recount [`Frozen`] at `now`, when any of its grace periods ends.
    fn release_due(&self, now: u64) {
        if !self.grace_pending.load(Ordering::Relaxed) {
            return;
        }
        let mut frozen = self.frozen.lock();
        if frozen
            .entries
            .iter()
            .any(|&(due, _)| due != u64::MAX && due <= now)
        {
            self.settle(&mut frozen, now);
        }
    }

    /// Gaps of stopped streams counted now. Tests only.
    #[cfg(test)]
    pub(in crate::transport::quic) fn kept(&self) -> usize {
        self.frozen.lock().counted
    }

    /// Change the count by `delta` at `now`; false if a new gap took it past
    /// the cap. A change that adds no gap never fails, even while kept gaps
    /// hold the count past the cap.
    fn add(&self, delta: isize, now: u64) -> bool {
        self.release_due(now);
        let held = self.change(delta);
        #[cfg(test)]
        self.peak.fetch_max(held, Ordering::Relaxed);
        held <= self.cap.load(Ordering::Relaxed) || delta <= 0
    }

    /// Set the cap, from the connection's window (`budget::gap_cap`).
    pub(in crate::transport::quic) fn set_cap(&self, cap: usize) {
        self.cap.store(cap, Ordering::Relaxed);
    }

    /// The cap now, which the budget charges.
    pub(in crate::transport::quic) fn cap(&self) -> usize {
        self.cap.load(Ordering::Relaxed)
    }

    /// Gaps counted now, kept ones included.
    pub(in crate::transport::quic) fn held_now(&self) -> usize {
        self.held.load(Ordering::Relaxed)
    }

    fn remove(&self, gaps: usize) {
        self.change(-(gaps as isize));
    }

    /// Change the count by `delta`, saturating at zero; the new count.
    fn change(&self, delta: isize) -> usize {
        let apply = |held: usize| {
            if delta >= 0 {
                held.saturating_add(delta.unsigned_abs())
            } else {
                held.saturating_sub(delta.unsigned_abs())
            }
        };
        let before = self
            .held
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |held| {
                Some(apply(held))
            })
            .unwrap_or_else(|held| held);
        apply(before)
    }

    /// The most gaps held at once. Tests only.
    #[cfg(test)]
    pub(in crate::transport::quic) fn peak(&self) -> usize {
        self.peak.load(Ordering::Relaxed)
    }

    /// Change the count as a read would. Tests only.
    #[cfg(test)]
    pub(in crate::transport::quic) fn add_for_test(&self, delta: isize) -> bool {
        self.add(delta, 0)
    }

    /// Gaps held now. Tests only.
    #[cfg(test)]
    pub(in crate::transport::quic) fn held(&self) -> usize {
        self.held.load(Ordering::Relaxed)
    }
}

/// The byte ranges received on one stream, merged, sorted by start.
#[derive(Debug, Default)]
struct RangeMirror {
    /// Received runs, start to one past the end, merged: none touch.
    /// Ordered by start, so each insert is logarithmic in the runs held
    /// (up to the connection's gap cap), whatever order pieces arrive in.
    ranges: BTreeMap<u64, u64>,
}

impl RangeMirror {
    #[cfg(test)]
    fn len(&self) -> usize {
        self.ranges.len()
    }

    /// The runs, in order. Tests only.
    #[cfg(test)]
    fn runs(&self) -> Vec<(u64, u64)> {
        self.iter().collect()
    }

    fn iter(&self) -> impl Iterator<Item = (u64, u64)> + '_ {
        self.ranges.iter().map(|(&start, &end)| (start, end))
    }

    /// Exactly `0..end` has been received.
    fn is_exactly(&self, end: u64) -> bool {
        self.ranges.len() == 1 && self.ranges.get(&0) == Some(&end)
    }

    /// One past the last byte of the in-order prefix: the end of the range
    /// starting at offset 0, or 0 if nothing there was received.
    fn prefix(&self) -> u64 {
        self.ranges.get(&0).copied().unwrap_or(0)
    }

    /// One past the highest byte received, or 0.
    fn last_stop(&self) -> u64 {
        self.ranges.last_key_value().map_or(0, |(_, &end)| end)
    }

    /// One past the last byte of the received run that starts at or covers
    /// `from`, or `from` if byte `from` has not been received.
    fn contiguous_from(&self, from: u64) -> u64 {
        match self.ranges.range(..=from).next_back() {
            Some((_, &end)) if end > from => end,
            _ => from,
        }
    }

    /// Ranges past the first: the gaps between them.
    fn gaps(&self) -> usize {
        self.ranges.len().saturating_sub(1)
    }

    /// Record `start..stop`; returns how the number of gaps changed (+1 for
    /// an isolated range after the first, 0 when it extends one, -1 when it
    /// joins two), or `Overlap` if any byte was already received.
    fn insert(&mut self, start: u64, stop: u64) -> Result<isize, ReadFailure> {
        if start >= stop {
            return Ok(0);
        }
        let before = self
            .ranges
            .range(..=start)
            .next_back()
            .map(|(&s, &e)| (s, e));
        if before.is_some_and(|(_, end)| end > start)
            || self.ranges.range(start..stop).next().is_some()
        {
            return Err(ReadFailure::Overlap);
        }
        let gaps = self.gaps();
        let mut merged = (start, stop);
        if let Some((s, e)) = before
            && e == start
        {
            self.ranges.remove(&s);
            merged.0 = s;
        }
        if let Some(e) = self.ranges.remove(&stop) {
            merged.1 = e;
        }
        self.ranges.insert(merged.0, merged.1);
        Ok(self.gaps() as isize - gaps as isize)
    }
}

/// Counts a reader dropped while it waits for room in a delivery channel.
/// Teardown aborts the receive task, which drops every parked reader without
/// running its code; this is the only record of those messages. Disarmed once
/// the wait ends by any other route.
struct WaitingGuard<'a> {
    stats: &'a QuicPathStats,
    outcome: QuicInboundOutcome,
}

impl<'a> WaitingGuard<'a> {
    fn new(stats: &'a QuicPathStats, outcome: QuicInboundOutcome) -> Self {
        Self { stats, outcome }
    }

    fn disarm(self) {
        std::mem::forget(self);
    }
}

impl Drop for WaitingGuard<'_> {
    fn drop(&mut self) {
        self.stats.record(self.outcome);
    }
}

fn nanos(duration: Duration) -> u64 {
    u64::try_from(duration.as_nanos()).unwrap_or(u64::MAX)
}

/// Age limit of a stream with no budget charge: one still in its header
/// phase, or a control frame. The smallest age limit a charged stream gets
/// (`inbound_stream_age_limit` of a tiny stream: `MIN_INBOUND_STREAM_AGE`,
/// or the simulation idle threshold under that accommodation).
fn uncharged_age_limit() -> Duration {
    inbound_stream_age_limit(0)
}

/// How long a stream may take to deliver its header. An honest header is
/// the stream's first 18 bytes, sent in its first packet, so a stream with
/// none after this long holds a stream slot (and what it staged) for
/// nothing. At least the idle timeout, so a simulation's longer idle
/// threshold is not cut short.
pub(in crate::transport::quic) const HEADER_PHASE_AGE: Duration = Duration::from_secs(30);

fn header_phase_age_limit() -> Duration {
    HEADER_PHASE_AGE.max(streaming_handle_idle_timeout())
}

/// Numbered fragments.
type Fragments = Vec<(u32, Bytes)>;

/// Cuts the payload of a stream of `total` bytes, arriving in pieces at any
/// offset, into fragments of the UDP stream shape: fragment `i` holds bytes
/// `[(i-1)F, iF)` with `F = FRAGMENT_PAYLOAD_SIZE`, so every fragment is full
/// except the last, none is empty, and there are `ceil(total / F)` of them
/// (the buffer has one more slot, for senders that shorten fragment #1).
///
/// A fragment is staged in a buffer of exactly its length until all its bytes
/// have arrived, then handed out. Staged bytes are payload bytes held once
/// (the buffer becomes the fragment), so they are inside the stream's charge.
/// The fragment that completes the stream is held back until
/// [`Self::finish`], which the caller calls only at the end of the stream, so
/// the buffer never reports a stream complete that then turns out to be longer
/// than declared.
struct Rechunker {
    total: u64,
    received: u64,
    fragments: u32,
    completed: u32,
    /// Fragments with some of their bytes: the buffer and how many it holds.
    partial: HashMap<u32, (BytesMut, usize)>,
    /// The fragment that completed the stream, until its end is seen.
    held: Option<(u32, Bytes)>,
}

impl Rechunker {
    fn new(total: u64) -> Self {
        let full = FRAGMENT_PAYLOAD_SIZE as u64;
        Self {
            total,
            received: 0,
            fragments: u32::try_from(total.div_ceil(full)).unwrap_or(u32::MAX),
            completed: 0,
            partial: HashMap::new(),
            held: None,
        }
    }

    fn fragment_len(&self, number: u32) -> usize {
        let full = FRAGMENT_PAYLOAD_SIZE as u64;
        let start = u64::from(number - 1) * full;
        (self.total - start).min(full) as usize
    }

    /// Take `data`, which starts at payload offset `offset`, returning the
    /// fragments it completed. Every fragment is a buffer of its own: none
    /// shares an allocation with `data` or with another fragment. The caller
    /// guarantees no byte is fed twice (`RangeMirror`).
    fn feed(&mut self, offset: u64, data: &[u8]) -> Result<Fragments, ReadFailure> {
        let len = data.len() as u64;
        if offset.checked_add(len).is_none_or(|stop| stop > self.total) {
            return Err(ReadFailure::Overlong);
        }
        self.received += len;
        let full = FRAGMENT_PAYLOAD_SIZE as u64;
        let mut out = Vec::new();
        let mut pos = offset;
        let mut rest = data;
        while !rest.is_empty() {
            let number = u32::try_from(pos / full + 1).unwrap_or(u32::MAX);
            let within = (pos % full) as usize;
            let frag_len = self.fragment_len(number);
            let take = (frag_len - within).min(rest.len());
            let (buf, filled) = self
                .partial
                .entry(number)
                .or_insert_with(|| (BytesMut::zeroed(frag_len), 0));
            buf[within..within + take].copy_from_slice(&rest[..take]);
            *filled += take;
            if *filled == frag_len {
                let (buf, _) = self
                    .partial
                    .remove(&number)
                    .expect("the entry was just filled");
                self.completed += 1;
                let fragment = buf.freeze();
                if self.completed == self.fragments {
                    self.held = Some((number, fragment));
                } else {
                    out.push((number, fragment));
                }
            }
            pos += take as u64;
            rest = &rest[take..];
        }
        Ok(out)
    }

    /// The last fragment to complete, once every declared byte has arrived and
    /// the stream has ended. The completion test is on BYTES, never on a count
    /// of fragments.
    fn finish(&mut self) -> Result<(u32, Bytes), ReadFailure> {
        if self.received < self.total {
            return Err(ReadFailure::Truncated(self.received as usize));
        }
        // Every byte arrived exactly once, so the last fragment completed.
        self.held
            .take()
            .ok_or(ReadFailure::Truncated(self.received as usize))
    }
}

/// Owns the reader's handle (or token) of a charged stream: on drop it
/// cancels the handle if the stream did not complete, recording on the
/// stream's source that the reader stopped (a reason a failure recorded
/// first stays; forks, which do not see the cancel, see this), drops it, and
/// then releases the charge if no other clone is left. It drops however the
/// reader ends: a return, a panic in its task, or the path or connection
/// dropping the reader. The reader keeps no
/// clone of its own, so the guard's is the last one the reader holds.
struct StreamGuard {
    budget: SharedInboundBudget,
    key: InboundKey,
    handle: Option<StreamHandle>,
    token: Option<Arc<LegacyStreamToken>>,
    complete: bool,
}

impl StreamGuard {
    fn handle(&self) -> &StreamHandle {
        // Set for every Stream guard at construction, and taken only in drop.
        self.handle
            .as_ref()
            .expect("a Stream guard holds its handle until dropped")
    }
}

impl Drop for StreamGuard {
    fn drop(&mut self) {
        if let Some(handle) = self.handle.take() {
            if !self.complete {
                handle.end_quic_source(QuicSourceEnd::Ended);
                handle.cancel();
            }
        }
        self.token = None;
        self.budget.lock().release_if_gone(self.key);
    }
}

/// Read one stream: its header, then by kind.
async fn read_one<T: TimeSource>(
    recv: quinn::RecvStream,
    receiver: Arc<Receiver<T>>,
    ticket: LimitTicket,
) {
    let mut reader = BodyReader::new(
        recv,
        receiver.time.clone(),
        streaming_handle_idle_timeout(),
        receiver.ranges.clone(),
        receiver.meter.clone(),
        receiver.staging.clone(),
    );
    reader.ticket = Some(ticket);
    let header = match reader.read_exact(HEADER_LEN).await {
        Ok(bytes) => FrameHeader::decode(&bytes),
        Err(ReadFailure::Truncated(got)) => Err(HeaderError::Truncated(got)),
        Err(failure) => {
            receiver.fail(&mut reader, failure, Charged::No);
            return;
        }
    };
    // On a short message only, so a test can have a stream in progress
    // when the task panics.
    #[cfg(test)]
    if receiver.panic_on_stream && matches!(&header, Ok(h) if h.kind() == FrameKind::Short) {
        panic!("test: the receive task panics");
    }
    let header = match header {
        Ok(header) => {
            let body = match header.kind() {
                FrameKind::Control => header.total_len(),
                FrameKind::Short | FrameKind::Stream => {
                    u64::from(header.metadata_len()).saturating_add(header.total_len())
                }
            };
            if let Err(failure) = reader.set_end((HEADER_LEN as u64).saturating_add(body)) {
                receiver.fail(&mut reader, failure, Charged::No);
                return;
            }
            // Past the header phase: the uncharged age limit, until
            // admission sets the stream's own.
            reader.set_age_limit(uncharged_age_limit());
            header
        }
        Err(error) => {
            receiver
                .stats
                .record(QuicInboundOutcome::of_header_error(&error));
            if matches!(error, HeaderError::InvalidLength(_)) {
                receiver
                    .stats
                    .record_shared(InboundStreamOutcome::RefusedInvalidLength);
            }
            tracing::debug!(%error, "QUIC path: refusing a stream header");
            reader.stop(stop_code::HEADER_REFUSED);
            return;
        }
    };
    // Shadow mode: refuse data before the budget is looked at, so a data
    // frame neither takes room from the UDP session sharing the budget nor
    // leaves a refusal there.
    //
    // The path is then closed with DOWNGRADE, on the first such frame. A Short of
    // one packet is acknowledged before the stop can reach its sender, which then
    // reads `Sent`, keeps sending on the path and loses every such message. The
    // close makes it fall back to UDP and negotiate the path again. Either side's
    // gating is wrong here, and both bits are read again on the next attempt.
    if !receiver.accept_data && header.kind() != FrameKind::Control {
        receiver
            .stats
            .record(QuicInboundOutcome::RefusedNotAcceptingData);
        reader.stop(stop_code::NOT_ACCEPTING_DATA);
        // The close waits, in this reader alone, so the stops already sent reach
        // the sender first: a sender that sees the close before a stream's stop
        // reads the stream as lost and drops it, where the stop sends it on UDP.
        // Every refused frame's reader waits and then asks; `QuicCloser::close`
        // is at most once per connection and a no-op once it has ended, so the
        // first close is the only one counted and logged. The waits are bounded
        // by the peer's stream credit and end with the connection.
        receiver.time.sleep(REFUSAL_CLOSE_DELAY).await;
        if receiver.closer.close(QuicCloseCode::Downgrade) {
            receiver
                .stats
                .record(QuicInboundOutcome::ClosedRefusingData);
            tracing::warn!(
                kind = ?header.kind(),
                "QUIC path: the peer sent data on a path where this node did not \
                 set its data bit; closing the path so the peer falls back to UDP"
            );
        }
        return;
    }
    match header.kind() {
        FrameKind::Short => receiver.read_short(reader, header).await,
        FrameKind::Stream => receiver.read_stream(reader, header).await,
        FrameKind::Control => receiver.read_control(reader, header).await,
    }
}

impl<T: TimeSource> Receiver<T> {
    /// Count a failed read and stop the stream where that tells the sender
    /// something.
    fn fail(&self, reader: &mut BodyReader<T>, failure: ReadFailure, charged: Charged) {
        if let Some(source) = &reader.source {
            source.end(match failure {
                ReadFailure::Idle => QuicSourceEnd::Idle,
                // The age limit, reached while a throttle paused the idle timer.
                ReadFailure::Age | ReadFailure::ThrottledPastAge => QuicSourceEnd::Age,
                ReadFailure::Truncated(_)
                | ReadFailure::Overlong
                | ReadFailure::Reset
                | ReadFailure::ConnectionLost
                | ReadFailure::TooManyRanges
                | ReadFailure::Overlap
                | ReadFailure::StagedOverCap
                | ReadFailure::Other => QuicSourceEnd::Ended,
            });
        }
        let stats = &self.stats;
        // Any other failure starts a pending raise of the stream limit over,
        // when the reader's ticket drops ([`LimitTicket`]).
        if matches!(failure, ReadFailure::Reset | ReadFailure::ConnectionLost)
            && let Some(ticket) = &mut reader.ticket
        {
            ticket.vote = Vote::Neutral;
        }
        match failure {
            ReadFailure::Truncated(_) => stats.record(QuicInboundOutcome::Truncated),
            ReadFailure::Overlong => {
                stats.record(QuicInboundOutcome::Overlong);
                reader.stop(stop_code::OVERLONG);
            }
            ReadFailure::Idle => {
                match charged {
                    Charged::Yes => stats.record_shared(InboundStreamOutcome::EvictedIdle),
                    Charged::No => stats.record(QuicInboundOutcome::IdleUncharged),
                }
                reader.stop(stop_code::EVICTED);
            }
            ReadFailure::Age => {
                match charged {
                    Charged::Yes => stats.record_shared(InboundStreamOutcome::EvictedAge),
                    Charged::No => stats.record(QuicInboundOutcome::AgedUncharged),
                }
                reader.stop(stop_code::EVICTED);
            }
            ReadFailure::ThrottledPastAge => {
                // Not the peer's fault: counted apart from `EvictedIdle` and
                // `EvictedAge`, so a budget that throttles too hard shows.
                stats.record(QuicInboundOutcome::ThrottledPastAge);
                reader.stop(stop_code::EVICTED);
            }
            ReadFailure::TooManyRanges => {
                stats.record(QuicInboundOutcome::TooManyRanges);
                reader.stop(stop_code::EVICTED);
                // The peer falls back to the UDP session and counts it as
                // `quic_fallback_over_budget_rx`.
                if self.ranges.trip(self.time.now_nanos())
                    && self.closer.close(QuicCloseCode::OverBudget)
                {
                    stats.record(QuicInboundOutcome::ClosedTooManyRanges);
                    tracing::warn!(
                        "QUIC path: the peer left more gaps than the connection's cap \
                         a second time within the window; closing the path so the peer falls back to UDP"
                    );
                }
            }
            ReadFailure::Overlap => {
                stats.record(QuicInboundOutcome::ReadFailed);
                reader.stop(stop_code::EVICTED);
            }
            ReadFailure::StagedOverCap => {
                stats.record(QuicInboundOutcome::EvictedStaged);
                reader.stop(stop_code::EVICTED);
                // The peer falls back to the UDP session and counts it as
                // `quic_fallback_over_budget_rx`.
                if self.staging.trip(self.time.now_nanos())
                    && self.closer.close(QuicCloseCode::OverBudget)
                {
                    stats.record(QuicInboundOutcome::ClosedStagedOverCap);
                    tracing::warn!(
                        "QUIC path: the peer's streams passed the connection's staging \
                         cap twice within the window; closing the path so the peer falls \
                         back to UDP"
                    );
                }
            }
            ReadFailure::Reset => stats.record(QuicInboundOutcome::ResetByPeer),
            ReadFailure::ConnectionLost => stats.record(QuicInboundOutcome::ConnectionLost),
            ReadFailure::Other => stats.record(QuicInboundOutcome::ReadFailed),
        }
        tracing::debug!(?failure, "QUIC path: inbound stream failed");
    }

    /// Count a budget refusal as the UDP transport does, and by frame kind
    /// (`kind`), and stop the stream.
    fn refuse(
        &self,
        reader: &mut BodyReader<T>,
        rejection: BudgetRejection,
        total: u64,
        kind: QuicInboundOutcome,
    ) {
        let outcome = match rejection {
            BudgetRejection::Bytes { .. } => InboundStreamOutcome::RefusedConnectionBytes,
            BudgetRejection::Streams { .. } => InboundStreamOutcome::RefusedConnectionStreams,
        };
        self.stats.record_shared(outcome);
        self.stats.record(kind);
        tracing::debug!(?rejection, total, "QUIC path: refusing an inbound stream");
        reader.stop(stop_code::BUDGET_REFUSED);
    }

    /// A key for a short message's charge that no held charge uses. Short
    /// keys are `InboundKey::QuicShort`, so they never meet a `Stream` id of
    /// this path or a stream id of the UDP connection sharing the budget.
    fn short_key(&self, budget: &InboundStreamBudget) -> InboundKey {
        loop {
            let n = self.next_short_key.fetch_add(1, Ordering::Relaxed);
            let key = InboundKey::QuicShort(n);
            if !budget.contains(key) {
                return key;
            }
        }
    }

    /// Charge a short message of `total` bytes to the inbound budget, under a
    /// key of its own; the guard holds the charge's only token.
    fn charge_short(&self, total: u64) -> Result<StreamGuard, BudgetRejection> {
        let token = LegacyStreamToken::new();
        let key = {
            let mut budget = self.budget.lock();
            budget.check(total, Releaser::Quic)?;
            let key = self.short_key(&budget);
            budget.charge(
                key,
                total,
                self.time.now_nanos(),
                inbound_stream_age_limit(total),
                Liveness::Task(Arc::downgrade(&token)),
            );
            key
        };
        Ok(StreamGuard {
            budget: self.budget.clone(),
            key,
            handle: None,
            token: Some(token),
            complete: false,
        })
    }

    async fn read_short(&self, mut reader: BodyReader<T>, header: FrameHeader) {
        let total = header.total_len();
        // A message of one packet or less is not charged, as on UDP: the
        // sender may see it acknowledged before any refusal arrives, so a
        // refusal could lose it with the sender reading `Sent`. Its cost is
        // bounded by the stream limit and the channel's capacity, unless it
        // has to wait for room (below).
        let mut guard = None;
        if !fits_one_packet(total) {
            match self.charge_short(total) {
                Ok(charge) => guard = Some(charge),
                Err(rejection) => {
                    self.refuse(
                        &mut reader,
                        rejection,
                        total,
                        QuicInboundOutcome::ShortRefusedBudget,
                    );
                    return;
                }
            }
            reader.set_age_limit(inbound_stream_age_limit(total));
        }
        let was_charged = if guard.is_some() {
            Charged::Yes
        } else {
            Charged::No
        };
        // The body is read unordered (the header gave its length): the staged
        // pieces first, then the rest as it arrives.
        let read = {
            let token = guard.as_ref().and_then(|guard| guard.token.as_ref());
            reader
                .read_rest_unordered_with(HEADER_LEN as u64, clamp(total), |held| {
                    if let Some(token) = token {
                        token.report(held as u64);
                    }
                })
                .await
        };
        let payload = match read {
            Ok(payload) => payload,
            Err(failure) => {
                self.fail(&mut reader, failure, was_charged);
                return;
            }
        };
        // Wait for room in the channel rather than drop the message: the sender
        // may have had every byte acknowledged, so a drop here would read as
        // `Sent`. Only this reader waits (each reader is its own future), so a
        // slow consumer does not hold up the other streams. The reader reads on
        // while it waits ([`BodyReader::read_while_waiting`]), so quinn holds
        // nothing for it; that may read the stream's end and free quinn's slot.
        // A charged message is bounded by its charge; an uncharged one lowers
        // the peer's stream limit while it waits ([`StreamLimit`]), so at most
        // the stream limit of them wait, as when a waiter held its slot. Past
        // the age limit the stream is stopped.
        let permit = match self.shorts.try_reserve() {
            Ok(permit) => permit,
            Err(mpsc::error::TrySendError::Full(())) => {
                let _held = guard.is_none().then(|| self.stream_limit.hold());
                let waiting =
                    WaitingGuard::new(&self.stats, QuicInboundOutcome::ShortAbandonedWaiting);
                let waited = reader.read_while_waiting(self.shorts.reserve()).await;
                waiting.disarm();
                match waited {
                    Ok(Ok(permit)) => permit,
                    // The consumer is gone: the connection is ending.
                    Ok(Err(_)) => {
                        self.stats.record(QuicInboundOutcome::ShortDroppedFull);
                        ended_locally(&mut reader);
                        return;
                    }
                    Err(failure) => {
                        self.fail(&mut reader, failure, was_charged);
                        return;
                    }
                }
            }
            Err(mpsc::error::TrySendError::Closed(())) => {
                self.stats.record(QuicInboundOutcome::ShortDroppedFull);
                ended_locally(&mut reader);
                return;
            }
        };
        if let Err(failure) = reader.expect_end().await {
            self.fail(&mut reader, failure, was_charged);
            return;
        }
        let (key, charge) = match guard.as_mut() {
            Some(guard) => {
                let Some(token) = guard.token.clone() else {
                    return;
                };
                token.mark_complete();
                guard.complete = true;
                (Some(guard.key), Some(token))
            }
            None => (None, None),
        };
        permit.send(InboundShort {
            payload,
            key,
            _charge: charge,
        });
        self.stats.record(QuicInboundOutcome::DeliveredShort);
        self.completed(&mut reader);
    }

    async fn read_stream(&self, mut reader: BodyReader<T>, header: FrameHeader) {
        let stream_id = header.stream_id();
        let total = header.total_len();
        let handle = {
            let mut budget = self.budget.lock();
            // A charge for this id whose state is already gone (an earlier
            // stream whose last handle was dropped after its reader ended)
            // must not refuse the id: nothing would release it otherwise.
            let key = InboundKey::Quic(stream_id);
            budget.release_if_gone(key);
            if budget.contains(key) {
                drop(budget);
                self.stats.record(QuicInboundOutcome::StreamIdInUse);
                reader.stop(stop_code::STREAM_ID_IN_USE);
                return;
            }
            if let Err(rejection) = budget.check(total, Releaser::Quic) {
                drop(budget);
                self.refuse(
                    &mut reader,
                    rejection,
                    total,
                    QuicInboundOutcome::StreamRefusedBudget,
                );
                return;
            }
            // Allocates only the buffer's chunk table (see
            // `LockFreeStreamBuffer`); slots come as fragments arrive.
            // A QUIC-fed handle: its consumers wait on this reader's rules,
            // not the UDP transport's 5 s fragment gap.
            let handle = StreamHandle::new_quic(stream_id, total);
            budget.charge(
                key,
                total,
                self.time.now_nanos(),
                inbound_stream_age_limit(total),
                Liveness::Buffer(handle.buffer_weak()),
            );
            handle
        };
        // The guard holds the reader's only clone of the handle, so when it
        // drops it can release the charge if no consumer holds one.
        let mut guard = StreamGuard {
            budget: self.budget.clone(),
            key: InboundKey::Quic(stream_id),
            handle: Some(handle),
            token: None,
            complete: false,
        };
        #[cfg(test)]
        if self
            .cancel_next_admitted
            .is_some_and(|next| next.swap(false, Ordering::Relaxed))
        {
            guard.handle().cancel();
        }
        reader.set_age_limit(inbound_stream_age_limit(total));
        reader.source = guard.handle().quic_source().cloned();
        if let Some(source) = &reader.source {
            // The round trip as of this stream; the watch keeps it current.
            self.rtt.set(self.closer.rtt());
            source.set_path(QuicSourcePath {
                rtt: self.rtt.clone(),
                idle: Duration::from_nanos(reader.idle_nanos),
                throttle: self.throttle_probe(),
            });
        }
        let payload_start = (HEADER_LEN as u64) + u64::from(header.metadata_len());
        let mut rechunker = Rechunker::new(total);
        let metadata = match header.metadata_len() {
            0 => None,
            len => {
                // Payload that arrives while the metadata is missing goes
                // to the charged buffer, not to staging.
                let handle = guard.handle().clone();
                let mut stopped = None;
                let read = reader
                    .read_exact_with(len as usize, |offset, piece| {
                        match self.feed_staged(
                            &mut rechunker,
                            &handle,
                            offset - payload_start,
                            piece,
                        ) {
                            Ok(()) => true,
                            Err(code) => {
                                stopped = Some(code);
                                false
                            }
                        }
                    })
                    .await;
                drop(handle);
                match (read, stopped) {
                    (Ok(metadata), _) => Some(metadata),
                    // Recorded where the piece was refused.
                    (Err(_), Some(code)) => {
                        if code == stop_code::CANCELLED {
                            ended_locally(&mut reader);
                        }
                        reader.stop(code);
                        return;
                    }
                    (Err(failure), None) => {
                        self.fail(&mut reader, failure, Charged::Yes);
                        return;
                    }
                }
            }
        };
        let arrival = InboundStreamArrival {
            stream_id,
            metadata,
            handle: guard.handle().clone(),
        };
        // Payload read before the stream was admitted was staged; it moves to
        // the charged buffer now.
        let mut fed = Ok(());
        let handle = guard.handle().clone();
        reader.staging.drain(&reader.ranges, |offset, piece| {
            if fed.is_ok() {
                fed = self.feed_staged(&mut rechunker, &handle, offset - payload_start, piece);
            }
        });
        drop(handle);
        if let Err(code) = fed {
            if code == stop_code::CANCELLED {
                ended_locally(&mut reader);
            }
            reader.stop(code);
            return;
        }
        // Wait for room rather than refuse: a small stream can be fully
        // acknowledged before this point, and a refusal would then read as
        // `Sent` at the sender. Only this reader waits (see `read_short`). The
        // stream keeps being read meanwhile, its payload into the charged
        // buffer, so quinn holds nothing for our wait. Idleness is judged while
        // payload is still missing (the peer is the one being waited on); once
        // it is all here, the reader reads on for the stream's end
        // ([`BodyReader::read_while_waiting`]), and then only the age limit
        // applies.
        let permit = match self.streams.try_reserve() {
            Ok(permit) => permit,
            Err(mpsc::error::TrySendError::Full(())) => {
                let waiting =
                    WaitingGuard::new(&self.stats, QuicInboundOutcome::StreamAbandonedWaiting);
                let mut reserve = std::pin::pin!(self.streams.reserve());
                let waited = loop {
                    // Its end may be read before the announce, which frees
                    // quinn's slot for the stream: the waiters are bounded by
                    // the inbound budget's charge, as on UDP. A stream that
                    // ends short fails at once (below).
                    if reader.all_read() {
                        break reader.read_while_waiting(&mut reserve).await;
                    }
                    enum Next<P> {
                        Room(P),
                        Piece(Result<Option<(u64, Bytes)>, ReadFailure>),
                    }
                    let next = tokio::select! {
                        room = &mut reserve => Next::Room(room),
                        piece = reader.next_piece() => Next::Piece(piece),
                    };
                    match next {
                        Next::Room(room) => break Ok(room),
                        Next::Piece(Ok(Some((offset, piece)))) => {
                            if !self.feed(
                                &mut reader,
                                &mut rechunker,
                                guard.handle(),
                                offset - payload_start,
                                &piece,
                            ) {
                                waiting.disarm();
                                return;
                            }
                        }
                        // The end before the declared length.
                        Next::Piece(Ok(None)) => {
                            break Err(ReadFailure::Truncated(
                                guard.handle().received_bytes() as usize
                            ));
                        }
                        Next::Piece(Err(failure)) => break Err(failure),
                    }
                };
                waiting.disarm();
                match waited {
                    Ok(Ok(permit)) => permit,
                    // The node's consumer of new streams is gone.
                    Ok(Err(_)) => {
                        self.stats.record(QuicInboundOutcome::StreamDroppedFull);
                        ended_locally(&mut reader);
                        reader.stop(stop_code::CHANNEL_FULL);
                        return;
                    }
                    Err(failure) => {
                        self.fail(&mut reader, failure, Charged::Yes);
                        return;
                    }
                }
            }
            Err(mpsc::error::TrySendError::Closed(())) => {
                self.stats.record(QuicInboundOutcome::StreamDroppedFull);
                ended_locally(&mut reader);
                reader.stop(stop_code::CHANNEL_FULL);
                return;
            }
        };
        permit.send(arrival);
        self.stats.record(QuicInboundOutcome::AnnouncedStream);

        loop {
            match reader.next_piece().await {
                Ok(Some((offset, piece))) => {
                    if !self.feed(
                        &mut reader,
                        &mut rechunker,
                        guard.handle(),
                        offset - payload_start,
                        &piece,
                    ) {
                        return;
                    }
                }
                Ok(None) => break,
                Err(failure) => {
                    self.fail(&mut reader, failure, Charged::Yes);
                    return;
                }
            }
        }
        let (number, last) = match rechunker.finish() {
            Ok(last) => last,
            Err(failure) => {
                self.fail(&mut reader, failure, Charged::Yes);
                return;
            }
        };
        if !self.push(&mut reader, guard.handle(), number, last) {
            return;
        }
        debug_assert_eq!(guard.handle().received_bytes(), total);
        guard.complete = true;
        self.stats.record(QuicInboundOutcome::CompletedStream);
        self.completed(&mut reader);
    }

    /// Feed one payload piece to `rechunker` and push the fragments it
    /// completes. False if the stream has ended (counted and stopped here).
    fn feed(
        &self,
        reader: &mut BodyReader<T>,
        rechunker: &mut Rechunker,
        handle: &StreamHandle,
        offset: u64,
        piece: &[u8],
    ) -> bool {
        let fragments = match rechunker.feed(offset, piece) {
            Ok(fragments) => fragments,
            Err(failure) => {
                self.fail(reader, failure, Charged::Yes);
                return false;
            }
        };
        for (number, fragment) in fragments {
            if !self.push(reader, handle, number, fragment) {
                return false;
            }
        }
        true
    }

    /// [`Self::feed`] for a staged piece, while the reader's staging is
    /// borrowed: counts why the stream must end and returns the stop code,
    /// which the caller sends.
    fn feed_staged(
        &self,
        rechunker: &mut Rechunker,
        handle: &StreamHandle,
        offset: u64,
        piece: &[u8],
    ) -> Result<(), u32> {
        // The declared end was checked against every staged byte
        // (`BodyReader::set_end`), so the rechunker takes every piece.
        let fragments = rechunker.feed(offset, piece).map_err(|_| {
            self.stats.record(QuicInboundOutcome::Overlong);
            stop_code::OVERLONG
        })?;
        for (number, fragment) in fragments {
            match handle.push_fragment(number, fragment) {
                Ok(_) => {}
                Err(StreamError::Cancelled) => {
                    self.stats.record(QuicInboundOutcome::CancelledLocally);
                    return Err(stop_code::CANCELLED);
                }
                Err(error) => {
                    debug_assert!(false, "fragment {number} refused: {error}");
                    self.stats.record(QuicInboundOutcome::ReadFailed);
                    return Err(stop_code::FRAGMENT_REFUSED);
                }
            }
        }
        Ok(())
    }

    /// Push one fragment. False if the stream must end: its handle was
    /// cancelled here (every consumer is gone or gave up), or the buffer
    /// refused the fragment, which the rechunker's shape rules out.
    fn push(
        &self,
        reader: &mut BodyReader<T>,
        handle: &StreamHandle,
        number: u32,
        fragment: Bytes,
    ) -> bool {
        match handle.push_fragment(number, fragment) {
            Ok(_) => true,
            Err(StreamError::Cancelled) => {
                self.stats.record(QuicInboundOutcome::CancelledLocally);
                ended_locally(reader);
                reader.stop(stop_code::CANCELLED);
                false
            }
            Err(error) => {
                debug_assert!(false, "fragment {number} refused: {error}");
                tracing::warn!(%error, number, "QUIC path: fragment refused by the stream buffer");
                self.stats.record(QuicInboundOutcome::ReadFailed);
                reader.stop(stop_code::FRAGMENT_REFUSED);
                false
            }
        }
    }

    async fn read_control(&self, mut reader: BodyReader<T>, header: FrameHeader) {
        let total = clamp(header.total_len());
        let payload = match reader.read_exact(total).await {
            Ok(payload) => payload,
            Err(failure) => {
                self.fail(&mut reader, failure, Charged::No);
                return;
            }
        };
        // The body stays charged to the connection's staging while the
        // stream's end is awaited, as staging charged it: a peer that
        // withholds the end would otherwise hold a body per stream outside
        // every charge.
        let charge = allocation_class(payload.len()) + STAGING_BUFFER_OVERHEAD_BYTES;
        let Some(_held) = StagedHold::new(reader.staging.conn.clone(), charge) else {
            // Unreachable: the body's own staging charge, no smaller, was
            // freed with no await since. If it ever is reached it is this
            // side's accounting, not the peer's doing, so it is not counted
            // as a trip toward closing the path.
            debug_assert!(false, "a control body's staging charge was refused");
            self.stats.record(QuicInboundOutcome::EvictedStaged);
            reader.stop(stop_code::EVICTED);
            return;
        };
        if let Err(failure) = reader.expect_end().await {
            self.fail(&mut reader, failure, Charged::No);
            return;
        }
        // A control frame read whole counts neither way toward a raise.
        if let Some(ticket) = &mut reader.ticket {
            ticket.vote = Vote::Neutral;
        }
        match self.controls.try_send(payload) {
            Ok(()) => self.stats.record(QuicInboundOutcome::DeliveredControl),
            Err(_) => self.stats.record(QuicInboundOutcome::ControlDroppedFull),
        }
    }
}

/// Bytes taken out of staging that stay charged to it until dropped.
struct StagedHold {
    conn: Arc<ConnStaging>,
    bytes: usize,
}

impl StagedHold {
    /// Charge `bytes`; `None` past the cap.
    fn new(conn: Arc<ConnStaging>, bytes: usize) -> Option<Self> {
        conn.try_add(bytes).then_some(Self { conn, bytes })
    }
}

impl Drop for StagedHold {
    fn drop(&mut self) {
        self.conn.release(self.bytes);
    }
}

/// A byte count as a read size. Declared lengths are at most
/// `MAX_STREAM_TOTAL_BYTES`, so this never saturates on a 64-bit target.
fn clamp(len: u64) -> usize {
    usize::try_from(len).unwrap_or(usize::MAX)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Bytes read above a gap are credit the meter counts as read, never as
    /// delivery that grows a window: the reader tells the meter only how far
    /// the stream's in-order prefix moved. Counting every byte read as
    /// delivered would let a peer grow its window by sending gapped data.
    #[test]
    fn bytes_read_above_a_gap_are_not_delivery() {
        use super::super::super::budget::{QuicBudget, WindowControl, floor_window_bytes};
        use super::super::super::tests::{bounded, drive, pair};
        bounded(|| async move {
            let net = crate::transport::test_net::subnet();
            let (a_addr, b_addr) = (net.addr(1), net.addr(2));
            let p = pair("quic-inbound-gapped-delivery", a_addr, b_addr, None).await;
            let permit =
                p.b.allow_peer(p.a_addr, p.a_keys.public())
                    .expect("allow-set has room");
            let (a, b) = (p.a.clone(), p.b.clone());
            let b_key = p.b_keys.public().clone();
            let time = p.time.clone();
            let (send, recv, server) = drive(&p.time, Duration::from_secs(10), async move {
                let (client, server) = tokio::join!(a.connect(b_addr, &b_key), async {
                    b.accept().await.expect("an attempt").establish().await
                });
                let client = client.expect("client handshake");
                let server = server.expect("server accepted");
                let mut send = client.inner().open_uni().await.expect("open stream");
                send.write_all(b"x").await.expect("write");
                let recv = server.inner().accept_uni().await.expect("accept stream");
                (send, recv, server)
            })
            .await;
            let counters: &'static crate::transport::metrics::QuicDataCounters =
                Box::leak(Box::new(crate::transport::metrics::QuicDataCounters::new()));
            let floor = floor_window_bytes(1 << 30);
            let budget = QuicBudget::new(64 << 20, floor, counters);
            let ranges = Arc::new(ConnRanges::new(MAX_GAPS_PER_CONNECTION, 64));
            let staging = Arc::new(ConnStaging::new(1 << 20, counters));
            let control: Arc<dyn WindowControl> = Arc::new(server.inner().clone());
            let meter = Arc::new(budget.register(
                control,
                ranges.clone(),
                staging.clone(),
                time.now_nanos(),
            ));
            let mut reader = BodyReader::new(
                recv,
                time.clone(),
                Duration::from_secs(30),
                ranges,
                Some(meter.clone()),
                staging,
            );
            let chunk = |offset: u64, len: usize| {
                Ok(Some(quinn::Chunk {
                    offset,
                    bytes: Bytes::from(vec![7u8; len]),
                }))
            };
            // The clock does not move, so the meter does not evaluate and
            // what it was told stays readable.
            reader
                .on_read(chunk(1000, 500))
                .expect("a piece above the gap");
            assert_eq!(
                meter.unevaluated(),
                (500, 0),
                "bytes above a gap are read, not delivered"
            );
            reader
                .on_read(chunk(2000, 300))
                .expect("another piece above a gap");
            assert_eq!(meter.unevaluated(), (800, 0));
            reader.on_read(chunk(0, 1000)).expect("the gap filled");
            assert_eq!(
                meter.unevaluated(),
                (1800, 1500),
                "filling the gap delivers it and what was above it, up to the next gap"
            );
            drop((reader, send, server, permit));
        })
    }

    const F: usize = FRAGMENT_PAYLOAD_SIZE;

    fn bytes(len: usize, seed: u8) -> Bytes {
        (0..len)
            .map(|i| (i as u8).wrapping_add(seed))
            .collect::<Vec<_>>()
            .into()
    }

    /// Feed `total` bytes in chunks of the given sizes, in order or last chunk
    /// first, and return the fragments handed out before `finish` (in the
    /// order they came) and every fragment, sorted by number.
    fn rechunk(total: usize, chunk_sizes: &[usize], reversed: bool) -> (Fragments, Fragments) {
        let data = bytes(total, 3);
        let mut pieces = Vec::new();
        let mut at = 0;
        for &size in chunk_sizes.iter().cycle() {
            if at == total {
                break;
            }
            let size = size.min(total - at);
            pieces.push((at, size));
            at += size;
        }
        if reversed {
            pieces.reverse();
        }
        let mut rechunker = Rechunker::new(total as u64);
        let mut early = Vec::new();
        for (at, size) in pieces {
            early.extend(rechunker.feed(at as u64, &data[at..at + size]).unwrap());
        }
        let mut all = early.clone();
        all.push(rechunker.finish().unwrap());
        all.sort_by_key(|(number, _)| *number);
        (early, all)
    }

    /// Every chunking of a stream, in order or out of order, gives the UDP
    /// fragment shape: fragment `i` is bytes `[(i-1)F, iF)`, all full but the
    /// last, none empty, numbered from 1, and the last to complete only from
    /// `finish`.
    #[test]
    fn rechunking_gives_the_udp_fragment_shape_whatever_the_chunks() {
        for total in [1, F - 1, F, F + 1, 2 * F, 2 * F + 1, 10 * F + 17] {
            for chunks in [
                &[1usize][..],
                &[7],
                &[F],
                &[F + 1],
                &[3 * F + 5],
                &[usize::MAX],
                &[F - 1, 2, 5000],
            ] {
                for reversed in [false, true] {
                    let (early, all) = rechunk(total, chunks, reversed);
                    let data = bytes(total, 3);
                    assert_eq!(all.len(), total.div_ceil(F), "{total} bytes in {chunks:?}");
                    assert_eq!(
                        early.len(),
                        all.len() - 1,
                        "the last fragment waits for finish"
                    );
                    for (i, (number, fragment)) in all.iter().enumerate() {
                        assert_eq!(*number as usize, i + 1);
                        let start = i * F;
                        let end = (start + F).min(total);
                        assert_eq!(&fragment[..], &data[start..end], "fragment {number}");
                        assert!(!fragment.is_empty());
                    }
                }
            }
        }
    }

    /// The buffer the fragments go into accepts exactly that shape and is
    /// complete only after the last.
    #[test]
    fn rechunked_fragments_complete_a_stream_buffer_only_at_the_last() {
        for total in [1, F, F + 1, 5 * F] {
            for reversed in [false, true] {
                let data = bytes(total, 3);
                let mut rechunker = Rechunker::new(total as u64);
                let mut pieces: Vec<_> = (0..total).step_by(777).collect();
                if reversed {
                    pieces.reverse();
                }
                let handle = StreamHandle::new(StreamId(0x8000_0001), total as u64);
                for at in pieces {
                    let end = (at + 777).min(total);
                    for (number, fragment) in rechunker.feed(at as u64, &data[at..end]).unwrap() {
                        assert_eq!(handle.push_fragment(number, fragment), Ok(true));
                        assert!(!handle.is_complete(), "complete before the last fragment");
                    }
                }
                let (number, last) = rechunker.finish().unwrap();
                assert_eq!(handle.push_fragment(number, last), Ok(true));
                assert!(handle.is_complete());
                assert_eq!(handle.try_assemble().unwrap(), data.to_vec());
            }
        }
    }

    /// A rechunked stream finishes only with every declared byte and no
    /// more: one short is `Truncated`, one past is `Overlong`, and a gap
    /// left open is truncated whichever side of it arrived.
    #[test]
    fn finish_requires_every_declared_byte_and_no_more() {
        for total in [1usize, F, F + 1, 3 * F] {
            let data = bytes(total, 0);
            let mut short = Rechunker::new(total as u64);
            short.feed(0, &data[..total - 1]).unwrap();
            assert_eq!(
                short.finish(),
                Err(ReadFailure::Truncated(total - 1)),
                "{total} bytes"
            );

            let mut long = Rechunker::new(total as u64);
            long.feed(0, &data).unwrap();
            assert_eq!(long.feed(total as u64, b"x"), Err(ReadFailure::Overlong));
            let mut long_in_one = Rechunker::new(total as u64);
            let mut more = data.to_vec();
            more.push(0);
            assert_eq!(long_in_one.feed(0, &more), Err(ReadFailure::Overlong));
            // A gap left open is truncated, whichever side of it arrived.
            if total > 1 {
                let mut gapped = Rechunker::new(total as u64);
                gapped.feed(1, &data[1..]).unwrap();
                assert_eq!(gapped.finish(), Err(ReadFailure::Truncated(total - 1)));
            }
        }
    }

    /// A fragment never points into the chunk it was cut from, even when the
    /// chunk is a whole fragment or more (one large allocation).
    #[test]
    fn fragments_share_no_allocation_with_the_chunk_or_each_other() {
        let total = 4 * F + 9;
        let chunk = bytes(total, 5);
        let range = chunk.as_ptr() as usize..chunk.as_ptr() as usize + chunk.len();
        let mut rechunker = Rechunker::new(total as u64);
        let mut all = rechunker.feed(0, &chunk).unwrap();
        all.push(rechunker.finish().unwrap());
        assert_eq!(all.len(), 5);
        let mut spans = Vec::new();
        for (number, fragment) in &all {
            let start = fragment.as_ptr() as usize;
            let end = start + fragment.len();
            assert!(
                end <= range.start || start >= range.end,
                "fragment {number} points into the chunk"
            );
            spans.push((start, end));
        }
        spans.sort_unstable();
        for pair in spans.windows(2) {
            assert!(pair[0].1 <= pair[1].0, "two fragments overlap");
        }
    }

    /// The mirror merges what touches, counts disjoint ranges, and refuses a
    /// byte received twice.
    fn conn_staging(cap: usize) -> Arc<ConnStaging> {
        Arc::new(ConnStaging::new(
            cap,
            Box::leak(Box::new(crate::transport::metrics::QuicDataCounters::new())),
        ))
    }

    /// Stage `bytes` at `offset` as the reader does: recorded in the mirror,
    /// then staged.
    fn stage(
        staging: &mut Staging,
        ranges: &mut RangeMirror,
        offset: u64,
        bytes: &[u8],
    ) -> Result<(), ReadFailure> {
        ranges.insert(offset, offset + bytes.len() as u64)?;
        staging.stage(offset, bytes, None)
    }

    /// A header arriving in pieces, last piece first, is staged, taken whole,
    /// and the buffer and its charge are gone once everything staged is taken.
    #[test]
    fn staged_pieces_are_taken_in_order_and_free_their_charge() {
        let conn = conn_staging(64 * 1024);
        let mut staging = Staging::new(conn.clone());
        let mut ranges = RangeMirror::default();
        let data: Vec<u8> = (0..18u8).collect();
        stage(&mut staging, &mut ranges, 10, &data[10..]).unwrap();
        assert!(conn.held() > 0);
        assert_eq!(ranges.contiguous_from(0), 0, "byte 0 is missing");
        stage(&mut staging, &mut ranges, 0, &data[..10]).unwrap();
        assert_eq!(ranges.contiguous_from(0), 18);
        assert_eq!(&staging.take(18)[..], &data[..]);
        assert_eq!(conn.held(), 0, "freed once all staged bytes are taken");
        drop(staging);
        assert_eq!(conn.held(), 0);
    }

    /// One byte far past the read position is charged for the span it makes
    /// the buffer cover, so it cannot hide a large allocation behind a small
    /// piece: past the cap it is refused and nothing is charged or kept.
    #[test]
    fn a_sparse_piece_is_charged_for_its_span_and_refused_past_the_cap() {
        let cap = 64 * 1024;
        let conn = conn_staging(cap);
        let mut staging = Staging::new(conn.clone());
        let mut ranges = RangeMirror::default();
        stage(&mut staging, &mut ranges, 1000, &[1]).unwrap();
        assert!(conn.held() >= 1001, "{}", conn.held());
        let before = conn.held();
        assert_eq!(
            staging.stage(cap as u64, &[1], None),
            Err(ReadFailure::StagedOverCap)
        );
        assert_eq!(conn.held(), before, "a refused piece charges nothing");
        assert!(staging.buf.capacity() < cap);
    }

    /// Many one-byte pieces cost the span they cover (plus one buffer's
    /// overhead), not an allocation each: tiny frames cannot multiply what is
    /// staged.
    #[test]
    fn tiny_pieces_cost_the_span_they_cover() {
        let conn = conn_staging(1 << 20);
        let mut staging = Staging::new(conn.clone());
        let mut ranges = RangeMirror::default();
        for i in 0..1000u64 {
            stage(&mut staging, &mut ranges, 1 + 2 * i, &[7]).unwrap();
        }
        let span = 2000;
        assert!(
            conn.held() <= allocation_class(2 * span) + STAGING_BUFFER_OVERHEAD_BYTES,
            "{}",
            conn.held()
        );
        assert_eq!(conn.held(), staging.charged);
    }

    /// Draining hands out only bytes that were received, by run, from the
    /// first byte not yet taken, and frees the buffer.
    #[test]
    fn draining_hands_out_the_received_runs_and_frees() {
        let conn = conn_staging(1 << 20);
        let mut staging = Staging::new(conn.clone());
        let mut ranges = RangeMirror::default();
        stage(&mut staging, &mut ranges, 0, b"head").unwrap();
        stage(&mut staging, &mut ranges, 10, b"tail").unwrap();
        stage(&mut staging, &mut ranges, 6, b"mi").unwrap();
        assert_eq!(&staging.take(4)[..], b"head");
        let mut runs = Vec::new();
        staging.drain(&ranges, |offset, bytes| runs.push((offset, bytes.to_vec())));
        assert_eq!(runs, vec![(6, b"mi".to_vec()), (10, b"tail".to_vec())]);
        assert_eq!(conn.held(), 0);
    }

    /// A second stream past the staging cap within the window closes the
    /// path; one after the window does not.
    #[test]
    fn a_second_trip_within_the_window_closes() {
        let conn = conn_staging(1024);
        let second = nanos(Duration::from_secs(1));
        assert!(!conn.trip(0), "a first trip only stops the stream");
        assert!(conn.trip(second));
        let later = second + nanos(STAGED_TRIP_WINDOW) + 1;
        assert!(!conn.trip(later), "outside the window it counts as a first");
    }

    #[test]
    fn allocation_classes_are_at_most_a_quarter_over() {
        for bytes in [1, 16, 17, 100, 1000, 4097, 65_537, 1 << 20, (1 << 20) + 1] {
            let class = allocation_class(bytes);
            assert!(class >= bytes, "{bytes} -> {class}");
            assert!(class <= (bytes * 5 / 4).max(16), "{bytes} -> {class}");
        }
    }

    #[test]
    fn the_range_mirror_merges_counts_and_refuses_overlap() {
        let mut m = RangeMirror::default();
        assert_eq!(m.insert(10, 20), Ok(0), "one range, no gap");
        assert_eq!(m.insert(30, 40), Ok(1));
        assert_eq!(m.len(), 2);
        assert_eq!(m.gaps(), 1);
        assert_eq!(m.insert(20, 25), Ok(0), "extends the first");
        assert_eq!(m.insert(25, 30), Ok(-1), "joins both");
        assert_eq!(m.runs(), vec![(10, 40)]);
        assert_eq!(m.gaps(), 0);
        assert_eq!(m.insert(0, 5), Ok(1));
        assert_eq!(m.insert(39, 41), Err(ReadFailure::Overlap));
        assert_eq!(m.insert(0, 1), Err(ReadFailure::Overlap));
        assert_eq!(m.insert(5, 10), Ok(-1));
        assert_eq!(m.runs(), vec![(0, 40)]);
        assert_eq!(m.insert(7, 7), Ok(0), "empty");
    }

    /// The connection's count is checked against its cap as gaps come and
    /// go, what a finished stream held goes back, and only a second trip past
    /// the cap within `STAGED_TRIP_WINDOW` asks for the close.
    #[test]
    fn the_connection_gap_count_has_a_cap() {
        let conn = ConnRanges::new(3, 64);
        assert!(conn.add(2, 0));
        assert!(conn.add(1, 0));
        assert!(!conn.add(1, 0), "past the cap");
        assert!(!conn.trip(0), "the first trip stops the stream only");
        assert!(conn.add(0, 0), "no new gap, so no trip, while past the cap");
        assert!(conn.add(-2, 0));
        assert_eq!(conn.held(), 2);
        conn.remove(1);
        assert_eq!(conn.held(), 1);
        conn.remove(5);
        assert_eq!(conn.held(), 0, "the count saturates at zero");
        let window = nanos(STAGED_TRIP_WINDOW);
        assert!(
            !conn.trip(window + 1),
            "a second trip past the window does not close"
        );
        assert!(conn.trip(2 * window), "a second within it closes");
    }

    /// A stopped stream's gaps stay counted through the grace period; then up
    /// to `KEPT_GAPS_RELEASED` of them leave the count, and the rest stay.
    #[test]
    fn a_stopped_streams_gaps_are_released_after_the_grace_period() {
        let conn = ConnRanges::new(1000, 64);
        let grace = nanos(KEPT_GAPS_GRACE);
        assert!(conn.add(5, 0));
        conn.keep(5, 0);
        assert!(conn.add(KEPT_GAPS_RELEASED as isize + 10, 10));
        conn.keep(KEPT_GAPS_RELEASED + 10, 10);
        assert_eq!(conn.held(), KEPT_GAPS_RELEASED + 15);
        assert!(conn.add(0, grace - 1));
        assert_eq!(conn.held(), KEPT_GAPS_RELEASED + 15, "not yet due");
        assert!(conn.add(0, grace));
        assert_eq!(conn.held(), KEPT_GAPS_RELEASED + 10, "the first stream's 5");
        conn.keep(0, grace + 10);
        assert_eq!(conn.held(), 10, "the second stream's first 32");
        assert_eq!(conn.kept(), 10);
        conn.keep(0, u64::MAX);
        assert_eq!(conn.held(), 10, "past 32 per stream, gaps stay counted");
    }

    /// Only the `limit` largest frozen counts count, since at most `limit`
    /// stopped streams count at once: a long-lived connection
    /// with many stops does not drift toward the cap. Here the limit is 2.
    #[test]
    fn only_the_largest_frozen_counts_count() {
        let conn = ConnRanges::new(1000, 2);
        for (gaps, at) in [(40, 0), (50, 1), (35, 2)] {
            assert!(conn.add(gaps, at));
            conn.keep(gaps as usize, at);
        }
        assert_eq!(conn.held(), 90, "50 + 40; the 35 does not count");
        let grace = nanos(KEPT_GAPS_GRACE);
        assert!(conn.add(0, grace + 2));
        assert_eq!(conn.held(), 26, "18 + 8, each 32 less after its grace");
        assert_eq!(conn.kept(), 26);
    }

    /// Many honest stops over a long life, each with a few gaps: the count
    /// stays bounded while they are fresh and falls to zero after the grace,
    /// so the connection never trips.
    #[test]
    fn many_honest_stops_do_not_drift_toward_the_cap() {
        // The cap of a 2 MiB window: 64 stopped streams of 20 gaps each
        // (1,280) fit under it, and the point is that they do not add up.
        let conn = ConnRanges::new(super::super::super::budget::gap_cap(2 << 20), 64);
        let step = nanos(Duration::from_millis(10));
        let mut now = 0;
        for _ in 0..100_000 {
            assert!(
                conn.add(20, now),
                "an honest stream tripped at {}",
                conn.held()
            );
            conn.keep(20, now);
            assert!(conn.held() <= 64 * 20);
            now += step;
        }
        assert!(conn.add(0, now + nanos(KEPT_GAPS_GRACE)));
        assert_eq!(conn.held(), 0);
        assert_eq!(conn.kept(), 0);
    }

    /// The receive side was written against quinn-proto 0.11.17 or later
    /// within 0.11, where unordered reads release each chunk as it arrives.
    /// A version outside that range must be re-read before it is taken.
    #[test]
    fn quinn_proto_is_one_whose_assembler_this_reader_was_written_for() {
        let versions = super::super::super::tests::locked_versions("quinn-proto");
        assert_eq!(
            versions.len(),
            1,
            "one quinn-proto in Cargo.lock: {versions:?}"
        );
        let parts: Vec<u64> = versions[0]
            .split('.')
            .map(|part| part.parse().expect("a numeric version"))
            .collect();
        assert_eq!(&parts[..2], &[0, 11], "quinn-proto {}", versions[0]);
        assert!(
            parts[2] >= 17,
            "quinn-proto {} predates MAX_CHUNKS",
            versions[0]
        );
    }

    fn limit(base: u32, max: u32) -> (Arc<StreamLimit>, Arc<parking_lot::Mutex<Vec<u32>>>) {
        let told = Arc::new(parking_lot::Mutex::new(Vec::new()));
        let seen = told.clone();
        let set = move |limit: u32| seen.lock().push(limit);
        (
            Arc::new(StreamLimit::new(Some(Box::new(set)), base, max)),
            told,
        )
    }

    /// Open `count` streams at once, then complete each (whole, gap-free).
    fn complete(limit: &Arc<StreamLimit>, count: u32, throttled: bool) -> bool {
        let mut tickets: Vec<_> = (0..count).map(|_| limit.open()).collect();
        tickets.iter_mut().fold(false, |raised, ticket| {
            ticket.vote = Vote::Completed;
            ticket.idle();
            limit.on_completed(throttled, false, || true) || raised
        })
    }

    /// Saturate, then complete `count` streams one at a time.
    fn run(limit: &Arc<StreamLimit>, count: u32, throttled: bool) -> bool {
        drop(saturate(limit));
        (0..count).fold(false, |raised, _| complete(limit, 1, throttled) || raised)
    }

    /// Every stream the base allows busy at once; dropped as completed.
    fn saturate(limit: &Arc<StreamLimit>) -> Vec<LimitTicket> {
        let mut tickets: Vec<_> = (0..limit.base()).map(|_| limit.open()).collect();
        for ticket in &mut tickets {
            ticket.vote = Vote::Neutral;
        }
        tickets
    }

    #[test]
    fn the_stream_limit_doubles_after_saturation_and_as_many_completions() {
        let (limit, told) = limit(16, 64);
        // Completions without every stream busy at once do not count.
        assert!(!(0..40).any(|_| complete(&limit, 1, false)));
        assert!(!complete(&limit, 15, false), "15 busy at once is one short");
        assert!(!run(&limit, 15, false), "15 completions are one short");
        assert!(complete(&limit, 1, false));
        assert_eq!(limit.base(), 32);
        assert!(run(&limit, 32, false));
        assert_eq!(limit.base(), 64);
        assert!(!run(&limit, 200, false), "never past the max");
        assert_eq!(*told.lock(), vec![32, 64]);
        assert_eq!(limit.busy(), 0);
    }

    /// A stream that is no longer busy (read whole, or its end read, and
    /// waiting on this node) does not count toward saturation, so a slow
    /// consumer here earns the peer nothing.
    #[test]
    fn only_busy_streams_saturate() {
        let (limit, _) = limit(16, 64);
        let mut waiting: Vec<_> = (0..15).map(|_| limit.open()).collect();
        for ticket in &mut waiting {
            ticket.idle();
        }
        let busy = limit.open();
        assert!(!limit.saturated.load(Ordering::Relaxed));
        drop(busy);
        drop(waiting);
        assert_eq!(limit.busy(), 0);
    }

    #[test]
    fn a_failure_or_a_throttle_holds_back_a_raise() {
        let (limit, _) = limit(16, 64);
        assert!(!run(&limit, 10, false));
        // A ticket dropped without a vote is a failure.
        drop(limit.open());
        assert!(
            !(0..20).any(|_| complete(&limit, 1, false)),
            "load starts over"
        );
        assert!(!run(&limit, 16, true), "not while throttled");
        assert!(complete(&limit, 1, false), "once the throttle is gone");
        assert_eq!(limit.base(), 32);
    }

    /// A reset by the sender or the connection ending counts neither way.
    #[test]
    fn a_neutral_end_keeps_the_run() {
        let (limit, _) = limit(16, 64);
        assert!(!run(&limit, 15, false));
        let mut reset = limit.open();
        reset.vote = Vote::Neutral;
        drop(reset);
        assert!(complete(&limit, 1, false));
    }

    /// A completion of a stream that had a gap does not count, and does not
    /// start the run over.
    #[test]
    fn a_gapped_completion_does_not_count() {
        let (limit, _) = limit(16, 64);
        assert!(!run(&limit, 15, false));
        assert!(!limit.on_completed(false, true, || true));
        assert!(complete(&limit, 1, false));
    }

    /// The completion that would raise asks whether the budget has room;
    /// with none it does not raise, and a later one with room does.
    #[test]
    fn a_raise_waits_for_room_in_the_budget() {
        let (limit, _) = limit(16, 64);
        assert!(!run(&limit, 15, false));
        let mut asked = 0;
        let mut ticket = limit.open();
        ticket.vote = Vote::Completed;
        ticket.idle();
        assert!(!limit.on_completed(false, false, || {
            asked += 1;
            false
        }));
        assert_eq!(asked, 1);
        assert_eq!(limit.base(), 16);
        drop(ticket);
        assert!(complete(&limit, 1, false));
        assert_eq!(limit.base(), 32);
    }

    #[test]
    fn waiting_shorts_lower_the_limit_below_the_base() {
        let (limit, told) = limit(16, 64);
        {
            let _a = limit.hold();
            let _b = limit.hold();
            assert_eq!(told.lock().last(), Some(&14));
            assert!(run(&limit, 16, false));
            assert_eq!(told.lock().last(), Some(&30), "a raise keeps the waiters");
        }
        assert_eq!(told.lock().last(), Some(&32));
    }

    #[test]
    fn a_path_without_data_never_raises() {
        let limit = Arc::new(StreamLimit::new(None, 0, 0));
        let _busy: Vec<_> = (0..100).map(|_| limit.open()).collect();
        assert!(!(0..100).any(|_| limit.on_completed(false, false, || true)));
    }

    #[test]
    fn only_a_stream_active_when_the_throttle_began_has_its_idle_paused() {
        let idle = 30;
        // No throttle: the peer is idle.
        assert!(!idle_is_ours(100, idle, None));
        // Progress 10 before a throttle that began at 110: active then.
        assert!(idle_is_ours(100, idle, Some(110)));
        // Already silent for the whole idle period when it began.
        assert!(!idle_is_ours(100, idle, Some(130)));
        assert!(!idle_is_ours(100, idle, Some(200)));
    }

    /// Each waiting one-packet Short takes one off the peer's stream limit
    /// while it waits, and the limit comes back as each leaves, to the full
    /// limit once none waits.
    #[test]
    fn waiting_shorts_lower_the_stream_limit_until_they_leave() {
        let told = Arc::new(parking_lot::Mutex::new(Vec::new()));
        let seen = told.clone();
        let limit = StreamLimit::new(Some(Box::new(move |n| seen.lock().push(n))), 64, 64);
        let a = limit.hold();
        let b = limit.hold();
        drop(a);
        let c = limit.hold();
        drop(b);
        drop(c);
        assert_eq!(*told.lock(), vec![63, 62, 63, 62, 63, 64]);
    }
}
