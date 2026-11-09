//! The receive side of a [`QuicPath`](super::QuicPath), and the inbound
//! adapter that feeds a QUIC stream into an ordinary [`StreamHandle`].
//!
//! The adapter exists so that everything above the transport stays as it
//! is: the payload of a `Stream` frame goes into the same `StreamHandle`
//! (and `LockFreeStreamBuffer`) the UDP transport fills, cut into fragments
//! of the UDP shape ([`Rechunker`]), so `fork`, `assemble`, `pipe_stream`,
//! `StreamIdentity` and the orphan registry work unchanged.
//!
//! Every stream is read unordered from its first byte
//! (`read_chunk(_, false)`, [`BodyReader::next_unordered`]): quinn's
//! assembler hands over each piece as it arrives, gap or not, so it holds
//! none of the stream's bytes waiting for a gap to fill (a peer that leaves a
//! gap cannot make quinn hold the bytes after it). The header, metadata,
//! short and control bodies are assembled in order from those pieces, copying
//! the ones that arrive early ([`BodyReader::next_chunk`]); a `Stream`'s
//! payload goes straight into its charged buffer at its offset. What a gap
//! still costs is one received range per stream (quinn's and our mirror,
//! [`RangeMirror`]), capped per connection ([`MAX_RANGES_PER_CONNECTION`]).
//! A stream waiting for room in the delivery channel keeps reading its
//! payload, so the wait never holds the peer's flow-control credit.
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
//!   (STOP_SENDING, [`stop_code`]), which tells the sender and discards
//!   whatever quinn still holds of it.
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
//! Memory: quinn hands out stream data as slices of the datagram it arrived
//! in, so keeping a 1-byte chunk keeps its whole ~1200-byte datagram. The
//! header, metadata, short and control reads therefore copy each chunk into
//! one buffer that grows as data arrives and ends exactly the declared
//! length (`ReadBuffer`), and copy pieces that arrive before their turn
//! (staged, unique bytes, so bounded by the connection's credit). The
//! payload of a `Stream` is copied into fragments by the `Rechunker`, each a
//! buffer of exactly its length filled at its offsets, so a fragment pins
//! only its own bytes whatever quinn's chunks were. A fragment `Bytes` may
//! therefore outlive the inbound charge.
//!
//! Simulation: `VirtualSleep` has no deregistration on drop, so each
//! stream's last armed timer stays in `VirtualTime`'s pending wakeups until
//! its deadline passes (one per ended stream; the timer is not replaced on
//! progress, only when it fires).
//!
//! Streams with no budget charge (in the header phase, and control frames)
//! get an age limit too ([`uncharged_age_limit`]), so a peer cannot hold
//! one of its `MAX_UNI_STREAMS` stream slots by trickling bytes.
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
use std::sync::atomic::{AtomicU32, AtomicUsize, Ordering};
use std::time::Duration;

use bytes::{Bytes, BytesMut};
use futures::StreamExt;
use futures::stream::FuturesUnordered;
use tokio::sync::mpsc;

use super::super::framing::{FrameHeader, FrameKind, HEADER_LEN, HeaderError};
use super::{QuicInboundOutcome, QuicPathStats, stop_code};
use crate::simulation::TimeSource;
use crate::transport::StreamId;
use crate::transport::metrics::{InboundStreamOutcome, QuicCloseCode};
use crate::transport::peer_connection::inbound_budget::{
    BudgetRejection, InboundKey, InboundStreamBudget, LegacyStreamToken, Liveness, Releaser,
    SharedInboundBudget,
};
use crate::transport::peer_connection::streaming::{StreamError, StreamHandle};
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
    /// The connection's received ranges over all its streams.
    ranges: Arc<ConnRanges>,
    /// Panic on the next stream, to test the supervisor.
    #[cfg(test)]
    pub(in crate::transport::quic) panic_on_stream: bool,
}

impl<T: TimeSource> Receiver<T> {
    /// The connection's range count. Tests only.
    #[cfg(test)]
    pub(in crate::transport::quic) fn ranges(&self) -> Arc<ConnRanges> {
        self.ranges.clone()
    }

    pub(in crate::transport::quic) fn new(
        time: T,
        budget: SharedInboundBudget,
        stats: Arc<QuicPathStats>,
        data: super::QuicDataSenders,
        controls: mpsc::Sender<Bytes>,
        accept_data: bool,
        closer: super::super::QuicCloser,
    ) -> Self {
        let super::QuicDataSenders { shorts, streams } = data;
        Self {
            time,
            budget,
            stats,
            shorts,
            streams,
            controls,
            next_short_key: AtomicU32::new(1),
            accept_data,
            closer,
            ranges: Arc::new(ConnRanges::new(MAX_RANGES_PER_CONNECTION)),
            #[cfg(test)]
            panic_on_stream: false,
        }
    }
}

/// How long after its first refused data frame a path that accepts no data
/// closes: long enough for the stops it sent to reach the sender first.
pub(in crate::transport::quic) const REFUSAL_CLOSE_DELAY: Duration = Duration::from_secs(2);

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
    loop {
        tokio::select! {
            accepted = &mut accept => match accepted {
                Ok(recv) => {
                    readers.push(read_one(recv, receiver.clone()));
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
    /// The connection's streams held more received ranges than
    /// [`MAX_RANGES_PER_CONNECTION`] (gaps the peer left).
    TooManyRanges,
    /// quinn delivered a byte twice, which an unordered read never does.
    Overlap,
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

/// Reads one QUIC stream with the idle and age limits applied.
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
    /// Idleness is not judged while this side is the one waiting (a full
    /// delivery channel): only the age limit applies.
    idle_paused: bool,
    /// The next stream offset an in-order read ([`Self::next_chunk`]) wants.
    next: u64,
    /// Bytes received past `next`, copied out of quinn's datagrams, by offset.
    /// Unique bytes, so their total is bounded by the connection's credit.
    staged: BTreeMap<u64, Bytes>,
    /// One past the last offset this stream may carry, once its header is
    /// known; a byte past it is `Overlong`.
    end: Option<u64>,
    /// quinn reported the end of the stream.
    finished: bool,
    /// Every byte range received on this stream, merged.
    ranges: RangeMirror,
    /// The connection's count of ranges over all its streams.
    conn_ranges: Arc<ConnRanges>,
}

impl<T: TimeSource> BodyReader<T> {
    /// A reader with the idle timeout and an age limit of
    /// [`uncharged_age_limit`]; admission replaces the age limit with the
    /// stream's own ([`Self::set_age_limit`]).
    fn new(recv: quinn::RecvStream, time: T, idle: Duration, conn_ranges: Arc<ConnRanges>) -> Self {
        let now = time.now_nanos();
        let idle_nanos = nanos(idle);
        let age_deadline = now.saturating_add(nanos(uncharged_age_limit()));
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
            idle_paused: false,
            next: 0,
            staged: BTreeMap::new(),
            end: None,
            finished: false,
            ranges: RangeMirror::default(),
            conn_ranges,
        }
    }

    fn deadline(&self) -> u64 {
        if self.idle_paused {
            return self.age_deadline;
        }
        self.last_progress
            .saturating_add(self.idle_nanos)
            .min(self.age_deadline)
    }

    /// The stream may carry bytes up to `end` (exclusive), counted from its
    /// first byte (the header's).
    fn set_end(&mut self, end: u64) {
        self.end = Some(end);
    }

    /// Stop judging idleness ([`Self::idle_paused`]); the armed timer is
    /// moved to the age deadline if it was set for the idle one.
    fn pause_idle(&mut self) {
        self.idle_paused = true;
        if self.timer_at != self.age_deadline {
            self.timer_at = self.age_deadline;
            self.timer = self.time.sleep_until(self.timer_at);
        }
    }

    /// Judge idleness again, from now.
    fn resume_idle(&mut self) {
        self.idle_paused = false;
        self.last_progress = self.time.now_nanos();
        self.timer_at = self.deadline();
        self.timer = self.time.sleep_until(self.timer_at);
    }

    /// The next piece of the stream in arrival order, with its offset, or
    /// `None` at the end. Read unordered (`read_chunk(max, false)`), so quinn's
    /// assembler releases each chunk as soon as it arrives, gap or not: the
    /// bytes then live only where the caller copies them. Checks the declared
    /// end and the connection's range cap.
    async fn next_unordered(&mut self) -> Result<Option<(u64, Bytes)>, ReadFailure> {
        if self.finished {
            return Ok(None);
        }
        loop {
            tokio::select! {
                read = self.recv.read_chunk(usize::MAX, false) => {
                    return match read {
                        Ok(Some(chunk)) => {
                            self.last_progress = self.time.now_nanos();
                            let start = chunk.offset;
                            let stop = start.saturating_add(chunk.bytes.len() as u64);
                            if self.end.is_some_and(|end| stop > end) {
                                return Err(ReadFailure::Overlong);
                            }
                            let added = self.ranges.insert(start, stop)?;
                            if !self.conn_ranges.add(added) {
                                return Err(ReadFailure::TooManyRanges);
                            }
                            Ok(Some((start, chunk.bytes)))
                        }
                        Ok(None) => {
                            self.finished = true;
                            Ok(None)
                        }
                        Err(error) => Err(error.into()),
                    };
                }
                () = &mut self.timer => {
                    let now = self.time.now_nanos();
                    // Idle first, as the UDP sweep does.
                    if !self.idle_paused
                        && now >= self.last_progress.saturating_add(self.idle_nanos)
                    {
                        return Err(ReadFailure::Idle);
                    }
                    if now >= self.age_deadline {
                        return Err(ReadFailure::Age);
                    }
                    self.timer_at = self.deadline();
                    self.timer = self.time.sleep_until(self.timer_at);
                }
            }
        }
    }

    /// Take out every staged piece, in offset order: the payload phase takes
    /// over from the in-order reads.
    fn take_staged(&mut self) -> Vec<(u64, Bytes)> {
        std::mem::take(&mut self.staged).into_iter().collect()
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

    /// The next chunk of at most `max` bytes (at least 1) at the in-order
    /// position, or `None` at the end of the stream. Pieces that arrive past
    /// the position are copied and staged until the gap before them fills.
    async fn next_chunk(&mut self, max: usize) -> Result<Option<Bytes>, ReadFailure> {
        loop {
            if let Some(entry) = self.staged.first_entry()
                && *entry.key() == self.next
            {
                let mut piece = entry.remove();
                if piece.len() > max {
                    let rest = piece.split_off(max);
                    self.staged.insert(self.next + max as u64, rest);
                }
                self.next += piece.len() as u64;
                return Ok(Some(piece));
            }
            match self.next_unordered().await? {
                None => return Ok(None),
                Some((offset, mut bytes)) if offset == self.next => {
                    if bytes.len() > max {
                        let rest = Bytes::copy_from_slice(&bytes[max..]);
                        bytes.truncate(max);
                        self.staged.insert(self.next + max as u64, rest);
                    }
                    self.next += bytes.len() as u64;
                    return Ok(Some(bytes));
                }
                Some((offset, bytes)) => {
                    // Copied, so the staged bytes do not keep quinn's datagram.
                    self.staged.insert(offset, Bytes::copy_from_slice(&bytes));
                }
            }
        }
    }

    /// Wait for `ready`, but not past the age deadline: `Err(Age)` if that
    /// comes first. Idleness does not apply during the wait: the peer is not
    /// the one being waited on, a consumer is. Holding the reader (and so the
    /// `RecvStream`) while waiting withholds the stream's credit, which is
    /// the back-pressure on the sender. When the wait ends in `Ok`, idleness
    /// restarts from that moment, so the rest of the stream is evicted idle
    /// as any other (a stall after a long wait must not run to the age
    /// limit). Arms one new sleep for the wait and, if the idle deadline is
    /// earlier than the age deadline, another after it, so call it only when
    /// `ready` is not already available.
    async fn until_age<F: Future>(&mut self, ready: F) -> Result<F::Output, ReadFailure> {
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

    /// Exactly `len` bytes (`len` > 0), copied into one buffer
    /// ([`ReadBuffer`]) that grows as data arrives and ends exactly `len`
    /// bytes long, so no chunk (and no datagram it is a slice of) is kept and
    /// nothing is allocated for bytes that have not arrived.
    async fn read_exact(&mut self, len: usize) -> Result<Bytes, ReadFailure> {
        self.read_exact_with(len, |_| {}).await
    }

    /// [`Self::read_exact`], calling `progress` with the bytes held so far
    /// after each chunk.
    async fn read_exact_with(
        &mut self,
        len: usize,
        mut progress: impl FnMut(usize),
    ) -> Result<Bytes, ReadFailure> {
        let mut out = ReadBuffer::new(len);
        while out.len() < len {
            match self.next_chunk(len - out.len()).await? {
                Some(chunk) => {
                    out.extend(&chunk);
                    progress(out.len());
                }
                None => return Err(ReadFailure::Truncated(out.len())),
            }
        }
        Ok(out.finish())
    }

    /// The end of the stream, with nothing more before it.
    async fn expect_end(&mut self) -> Result<(), ReadFailure> {
        match self.next_chunk(usize::MAX).await? {
            None => Ok(()),
            Some(_) => Err(ReadFailure::Overlong),
        }
    }

    /// Stop the stream (STOP_SENDING) with `code`. Harmless if it already
    /// ended.
    fn stop(&mut self, code: u32) {
        self.recv.stop(code.into()).ok();
    }
}

impl<T: TimeSource> Drop for BodyReader<T> {
    fn drop(&mut self) {
        self.conn_ranges.remove(self.ranges.len());
    }
}

/// Most byte ranges a connection's streams may hold at once, counted as
/// disjoint received ranges per stream ([`RangeMirror`]). Each range costs
/// about 70 B (quinn's `recvd` entry, which nothing in quinn caps, and our
/// mirror), and each gap costs the peer at least one byte of credit. One
/// range per KiB of the largest connection window (2,048, about 140 KB); honest
/// loss leaves about loss x packets in flight, far fewer.
pub(in crate::transport::quic) const MAX_RANGES_PER_CONNECTION: usize =
    (super::super::config::MAX_WINDOW_BYTES / 1024) as usize;

/// A connection's count of received ranges over its streams, against
/// [`MAX_RANGES_PER_CONNECTION`].
#[derive(Debug)]
pub(in crate::transport::quic) struct ConnRanges {
    held: AtomicUsize,
    cap: usize,
    /// The most held at once. Tests only.
    #[cfg(test)]
    peak: AtomicUsize,
}

impl ConnRanges {
    pub(in crate::transport::quic) fn new(cap: usize) -> Self {
        Self {
            held: AtomicUsize::new(0),
            cap,
            #[cfg(test)]
            peak: AtomicUsize::new(0),
        }
    }

    /// Change the count by `delta`; false if it is now past the cap.
    fn add(&self, delta: isize) -> bool {
        let held = if delta >= 0 {
            self.held.fetch_add(delta.unsigned_abs(), Ordering::Relaxed) + delta.unsigned_abs()
        } else {
            self.held
                .fetch_sub(delta.unsigned_abs(), Ordering::Relaxed)
                .saturating_sub(delta.unsigned_abs())
        };
        #[cfg(test)]
        self.peak.fetch_max(held, Ordering::Relaxed);
        held <= self.cap
    }

    fn remove(&self, ranges: usize) {
        self.held.fetch_sub(ranges, Ordering::Relaxed);
    }

    /// The most ranges held at once. Tests only.
    #[cfg(test)]
    pub(in crate::transport::quic) fn peak(&self) -> usize {
        self.peak.load(Ordering::Relaxed)
    }

    /// Ranges held now. Tests only.
    #[cfg(test)]
    pub(in crate::transport::quic) fn held(&self) -> usize {
        self.held.load(Ordering::Relaxed)
    }
}

/// The byte ranges received on one stream, merged, sorted by start.
#[derive(Debug, Default)]
struct RangeMirror {
    ranges: Vec<(u64, u64)>,
}

impl RangeMirror {
    fn len(&self) -> usize {
        self.ranges.len()
    }

    /// Record `start..stop`; returns how the number of ranges changed (+1 for
    /// an isolated range, 0 when it extends one, negative when it joins
    /// several), or `Overlap` if any byte was already received.
    fn insert(&mut self, start: u64, stop: u64) -> Result<isize, ReadFailure> {
        if start >= stop {
            return Ok(0);
        }
        // First range ending at or after `start`.
        let i = self.ranges.partition_point(|&(_, end)| end < start);
        // Ranges from `i` that touch or overlap `start..stop`.
        let mut j = i;
        while j < self.ranges.len() && self.ranges[j].0 <= stop {
            let (s, e) = self.ranges[j];
            if s < stop && e > start {
                return Err(ReadFailure::Overlap);
            }
            j += 1;
        }
        let before = self.ranges.len();
        let merged_start = if i < j {
            self.ranges[i].0.min(start)
        } else {
            start
        };
        let merged_stop = if i < j {
            self.ranges[j - 1].1.max(stop)
        } else {
            stop
        };
        self.ranges.splice(i..j, [(merged_start, merged_stop)]);
        Ok(self.ranges.len() as isize - before as isize)
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

/// What a [`ReadBuffer`] allocates before any data: a declared length is
/// not memory until the bytes arrive (a short message may declare tens of
/// MiB in an 18-byte header).
const READ_START_BYTES: usize = 64 * 1024;

/// The bytes of one read of a declared length: starts at
/// `min(len, READ_START_BYTES)`, grows by doubling but never past `len`, so
/// in steady state it holds at most twice what has arrived, and it ends
/// exactly `len` bytes long (one allocation, owned by the result). During a
/// reallocation the old and new buffers both exist for a moment, up to
/// about twice `len`; each stream reads into one `ReadBuffer` at a time.
struct ReadBuffer {
    buf: Vec<u8>,
    len: usize,
}

impl ReadBuffer {
    fn new(len: usize) -> Self {
        Self {
            buf: Vec::with_capacity(len.min(READ_START_BYTES)),
            len,
        }
    }

    fn len(&self) -> usize {
        self.buf.len()
    }

    fn extend(&mut self, chunk: &[u8]) {
        let needed = self.buf.len() + chunk.len();
        if needed > self.buf.capacity() {
            let target = (self.buf.capacity() * 2)
                .max(needed)
                .min(self.len.max(needed));
            self.buf.reserve_exact(target - self.buf.len());
        }
        self.buf.extend_from_slice(chunk);
    }

    fn finish(mut self) -> Bytes {
        self.buf.shrink_to_fit();
        Bytes::from(self.buf)
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
/// cancels the handle if the stream did not complete, drops it, and then
/// releases the charge if no other clone is left. The reader keeps no
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
                handle.cancel();
            }
        }
        self.token = None;
        self.budget.lock().release_if_gone(self.key);
    }
}

/// Read one stream: its header, then by kind.
async fn read_one<T: TimeSource>(recv: quinn::RecvStream, receiver: Arc<Receiver<T>>) {
    #[cfg(test)]
    if receiver.panic_on_stream {
        panic!("test: the receive task panics");
    }
    let mut reader = BodyReader::new(
        recv,
        receiver.time.clone(),
        streaming_handle_idle_timeout(),
        receiver.ranges.clone(),
    );
    let header = match reader.read_exact(HEADER_LEN).await {
        Ok(bytes) => FrameHeader::decode(&bytes),
        Err(ReadFailure::Truncated(got)) => Err(HeaderError::Truncated(got)),
        Err(failure) => {
            receiver.fail(&mut reader, failure, Charged::No);
            return;
        }
    };
    let header = match header {
        Ok(header) => {
            let body = match header.kind() {
                FrameKind::Control => header.total_len(),
                FrameKind::Short | FrameKind::Stream => {
                    u64::from(header.metadata_len()).saturating_add(header.total_len())
                }
            };
            reader.set_end((HEADER_LEN as u64).saturating_add(body));
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
        let stats = &self.stats;
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
            ReadFailure::TooManyRanges => {
                stats.record(QuicInboundOutcome::TooManyRanges);
                reader.stop(stop_code::EVICTED);
            }
            ReadFailure::Overlap => {
                stats.record(QuicInboundOutcome::ReadFailed);
                reader.stop(stop_code::EVICTED);
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

    async fn read_short(&self, mut reader: BodyReader<T>, header: FrameHeader) {
        let total = header.total_len();
        // A message of one packet or less is not charged, as on UDP: the
        // sender may see it acknowledged before any refusal arrives, so a
        // refusal could lose it with the sender reading `Sent`. Its cost is
        // bounded by the stream limit and the channel's capacity.
        let charged = !fits_one_packet(total);
        let mut guard = None;
        if charged {
            let token = LegacyStreamToken::new();
            let key = {
                let mut budget = self.budget.lock();
                if let Err(rejection) = budget.check(total, Releaser::Quic) {
                    drop(budget);
                    self.refuse(
                        &mut reader,
                        rejection,
                        total,
                        QuicInboundOutcome::ShortRefusedBudget,
                    );
                    return;
                }
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
            // The guard holds the reader's only reference to the token.
            guard = Some(StreamGuard {
                budget: self.budget.clone(),
                key,
                handle: None,
                token: Some(token),
                complete: false,
            });
            reader.set_age_limit(inbound_stream_age_limit(total));
        }
        let was_charged = if charged { Charged::Yes } else { Charged::No };
        // One buffer of the charged length (see `read_exact`).
        let read = {
            let token = guard.as_ref().and_then(|guard| guard.token.as_ref());
            reader
                .read_exact_with(clamp(total), |held| {
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
        // `Sent`. The wait comes BEFORE `expect_end`: reading the stream to its
        // end frees it in quinn and returns its MAX_STREAMS credit, so waiting
        // after that would not slow the sender, and the number of waiters (and
        // the memory they hold) would grow with the peer's upload. While the
        // stream is unfinished its credit is held, so at most `MAX_UNI_STREAMS`
        // readers wait, and a one-packet Short (uncharged) is capped the same
        // way. Only this reader waits (each reader is its own future), so a slow
        // consumer slows the sender and not the other streams. Past the age
        // limit the stream is stopped, which reaches the sender because the
        // stream is still open.
        let permit = match self.shorts.try_reserve() {
            Ok(permit) => permit,
            Err(mpsc::error::TrySendError::Full(())) => {
                let waiting =
                    WaitingGuard::new(&self.stats, QuicInboundOutcome::ShortAbandonedWaiting);
                let waited = reader.until_age(self.shorts.reserve()).await;
                waiting.disarm();
                match waited {
                    Ok(Ok(permit)) => permit,
                    // The consumer is gone: the connection is ending.
                    Ok(Err(_)) => {
                        self.stats.record(QuicInboundOutcome::ShortDroppedFull);
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
            let handle = StreamHandle::new(stream_id, total);
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
        reader.set_age_limit(inbound_stream_age_limit(total));
        let metadata = match header.metadata_len() {
            0 => None,
            len => match reader.read_exact(len as usize).await {
                Ok(metadata) => Some(metadata),
                Err(failure) => {
                    self.fail(&mut reader, failure, Charged::Yes);
                    return;
                }
            },
        };
        let arrival = InboundStreamArrival {
            stream_id,
            metadata,
            handle: guard.handle().clone(),
        };
        let payload_start = (HEADER_LEN as u64) + u64::from(header.metadata_len());
        let mut rechunker = Rechunker::new(total);
        // Payload pieces that arrived before the metadata was complete.
        for (offset, piece) in reader.take_staged() {
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
        // Wait for room rather than refuse: a small stream can be fully
        // acknowledged before this point, and a refusal would then read as
        // `Sent` at the sender. Only this reader waits (see `read_short`). The
        // payload keeps being read meanwhile, into the charged buffer, so quinn
        // holds none of it for our wait and the peer's credit is not held by
        // our consumer; idleness is not judged while we are the one waiting.
        let permit = match self.streams.try_reserve() {
            Ok(permit) => permit,
            Err(mpsc::error::TrySendError::Full(())) => {
                let waiting =
                    WaitingGuard::new(&self.stats, QuicInboundOutcome::StreamAbandonedWaiting);
                reader.pause_idle();
                let mut reserve = std::pin::pin!(self.streams.reserve());
                let waited = loop {
                    if reader.finished {
                        break reader.until_age(&mut reserve).await;
                    }
                    enum Next<P> {
                        Room(P),
                        Piece(Result<Option<(u64, Bytes)>, ReadFailure>),
                    }
                    let next = tokio::select! {
                        room = &mut reserve => Next::Room(room),
                        piece = reader.next_unordered() => Next::Piece(piece),
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
                        Next::Piece(Ok(None)) => {}
                        Next::Piece(Err(failure)) => break Err(failure),
                    }
                };
                waiting.disarm();
                reader.resume_idle();
                match waited {
                    Ok(Ok(permit)) => permit,
                    Ok(Err(_)) => {
                        self.stats.record(QuicInboundOutcome::StreamDroppedFull);
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
                reader.stop(stop_code::CHANNEL_FULL);
                return;
            }
        };
        permit.send(arrival);
        self.stats.record(QuicInboundOutcome::AnnouncedStream);

        loop {
            match reader.next_unordered().await {
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
        if let Err(failure) = reader.expect_end().await {
            self.fail(&mut reader, failure, Charged::No);
            return;
        }
        match self.controls.try_send(payload) {
            Ok(()) => self.stats.record(QuicInboundOutcome::DeliveredControl),
            Err(_) => self.stats.record(QuicInboundOutcome::ControlDroppedFull),
        }
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

    /// A read buffer allocates nothing for bytes that have not arrived: it
    /// starts at `READ_START_BYTES` whatever the declared length, grows by
    /// doubling but never past that length, and ends exactly that long.
    #[test]
    fn a_read_buffer_grows_with_the_data_and_ends_exact() {
        let declared = 40 * 1024 * 1024;
        let mut buffer = ReadBuffer::new(declared);
        buffer.extend(&[7]);
        assert!(
            buffer.buf.capacity() <= READ_START_BYTES,
            "{}",
            buffer.buf.capacity()
        );
        let mut small = ReadBuffer::new(10);
        small.extend(&[1; 4]);
        assert!(small.buf.capacity() <= 10);
        small.extend(&[2; 6]);
        let bytes = small.finish();
        assert_eq!(&bytes[..], &[1, 1, 1, 1, 2, 2, 2, 2, 2, 2]);
        assert_eq!(bytes.try_into_mut().unwrap().capacity(), 10);
        let len = 3 * READ_START_BYTES + 5;
        let mut grown = ReadBuffer::new(len);
        for _ in 0..len {
            grown.extend(&[0]);
            assert!(grown.buf.capacity() <= len);
        }
        assert_eq!(grown.finish().try_into_mut().unwrap().capacity(), len);
    }

    /// Completion is by bytes: a stream one byte short is truncated at
    /// `finish`, and a chunk past the declared length is overlong.
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
    #[test]
    fn the_range_mirror_merges_counts_and_refuses_overlap() {
        let mut m = RangeMirror::default();
        assert_eq!(m.insert(10, 20), Ok(1));
        assert_eq!(m.insert(30, 40), Ok(1));
        assert_eq!(m.len(), 2);
        assert_eq!(m.insert(20, 25), Ok(0), "extends the first");
        assert_eq!(m.insert(25, 30), Ok(-1), "joins both");
        assert_eq!(m.ranges, vec![(10, 40)]);
        assert_eq!(m.insert(0, 5), Ok(1));
        assert_eq!(m.insert(39, 41), Err(ReadFailure::Overlap));
        assert_eq!(m.insert(0, 1), Err(ReadFailure::Overlap));
        assert_eq!(m.insert(5, 10), Ok(-1));
        assert_eq!(m.ranges, vec![(0, 40)]);
        assert_eq!(m.insert(7, 7), Ok(0), "empty");
    }

    /// The connection's count is checked against its cap as ranges come and
    /// go, and what a stream held goes back when it ends.
    #[test]
    fn the_connection_range_count_has_a_cap() {
        let conn = ConnRanges::new(3);
        assert!(conn.add(2));
        assert!(conn.add(1));
        assert!(!conn.add(1), "past the cap");
        assert!(conn.add(-2));
        assert_eq!(conn.held(), 2);
        conn.remove(2);
        assert_eq!(conn.held(), 0);
    }

    /// The receive side relies on quinn-proto 0.11.17 or later within 0.11:
    /// `COMPACT_THRESHOLD` and `MAX_CHUNKS` bound what its assembler can hold
    /// for a stream between two of our reads, and unordered reads release each
    /// chunk as it arrives. A version outside that range must be re-read
    /// (`assembler.rs`) before it is taken.
    #[test]
    fn quinn_proto_is_one_whose_assembler_this_reader_was_written_for() {
        let lock =
            std::fs::read_to_string(concat!(env!("CARGO_MANIFEST_DIR"), "/../../Cargo.lock"))
                .expect("read Cargo.lock");
        let marker = "name = \"quinn-proto\"\nversion = \"";
        let versions: Vec<&str> = lock
            .match_indices(marker)
            .map(|(at, _)| {
                let rest = &lock[at + marker.len()..];
                &rest[..rest.find('"').expect("a quoted version")]
            })
            .collect();
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
}
