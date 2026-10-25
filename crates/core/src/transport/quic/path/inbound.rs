//! The receive side of a [`QuicPath`](super::QuicPath), and the inbound
//! adapter that feeds a QUIC stream into an ordinary [`StreamHandle`].
//!
//! The adapter exists so that everything above the transport stays as it
//! is: the payload of a `Stream` frame goes into the same `StreamHandle`
//! (and `LockFreeStreamBuffer`) the UDP transport fills, cut into fragments
//! of the UDP shape ([`Rechunker`]), so `fork`, `assemble`, `pipe_stream`,
//! `StreamIdentity` and the orphan registry work unchanged.
//!
//! Admission follows the UDP transport's rules (`.claude/rules/transport.md`,
//! "WHEN admitting or sending a stream"):
//! - Before anything is created from a declared length, the header has
//!   passed `check_declared_stream_length` (in `FrameHeader::decode`) and
//!   the stream has been charged to the path's `InboundStreamBudget`, with
//!   the same cost (`ChargeOverhead`). Short messages are charged too: they
//!   carry what the UDP transport sends as transport-internal streams.
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
//!   (`StreamIdInUse`). Completed ids are not remembered: that dedup is e5's,
//!   where it sees both transports (`completed_inbound_streams`).
//! - Header refusals, truncation, overlong streams, resets and full channels
//!   are counted in the path's own `QuicInboundOutcome` counters. A refused
//!   header with an invalid declared length is also counted as
//!   `InboundStreamOutcome::RefusedInvalidLength`, as the UDP path counts it.
//!
//! Memory: quinn hands out stream data as slices of the datagram it arrived
//! in, so keeping a 1-byte chunk keeps its whole ~1200-byte datagram. The
//! header, metadata, short and control reads therefore copy each chunk into
//! one buffer that grows as data arrives and ends exactly the declared
//! length (`ReadBuffer`), and keep no chunk. The payload of a `Stream` is
//! copied into fragments by the `Rechunker`, each in a fresh buffer of
//! exactly its length, except a chunk of at least a whole fragment, which
//! is kept as a slice: it pins at most one datagram (1200 bytes) per
//! fragment of `FRAGMENT_PAYLOAD_SIZE` (pinned by a const assertion).
//!
//! One exception: quinn-proto's `Assembler::defragment`, which the peer can
//! trigger (by sending data out of order or in many small frames), merges
//! buffered stream data into one allocation of up to the stream window,
//! and whole-fragment slices taken from that chunk then share it. That
//! memory is inside the stream's charge while its inbound state lives,
//! but a slice keeps the whole allocation alive. e5 constraint: a fragment
//! `Bytes` taken from an inbound `StreamHandle` must not outlive the
//! inbound charge (for example, kept in an outbound pipe's retransmit
//! state) without being copied.
//!
//! Simulation: `VirtualSleep` has no deregistration on drop, so each
//! stream's last armed timer stays in `VirtualTime`'s pending wakeups until
//! its deadline passes (one per ended stream; the timer is not replaced on
//! progress, only when it fires).
//! Streams with no budget charge (in the header phase, and control frames)
//! get an age limit too ([`uncharged_age_limit`]), so a peer cannot hold
//! one of its 256 stream slots by trickling bytes.
//!
//! Stream id lifetime: a `Stream` id is held on the path from admission
//! until all of its state is gone (the reader is done and every handle
//! clone dropped). The charge is released when the reader ends if it held
//! the last clone, and otherwise checked again whenever a stream with the
//! same id arrives, before it is refused as `StreamIdInUse`. Completed ids
//! are not remembered, so once released an id can be used again.

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};
use std::time::Duration;

use bytes::{Bytes, BytesMut};
use futures::StreamExt;
use futures::stream::FuturesUnordered;
use parking_lot::Mutex;
use tokio::sync::mpsc;

use super::super::framing::{FrameHeader, FrameKind, HEADER_LEN, HeaderError};
use super::{QuicInboundOutcome, QuicPathStats, stop_code};
use crate::simulation::TimeSource;
use crate::transport::StreamId;
use crate::transport::metrics::InboundStreamOutcome;
use crate::transport::peer_connection::inbound_budget::{
    BudgetRejection, InboundStreamBudget, LegacyStreamToken, Liveness,
};
use crate::transport::peer_connection::streaming::{StreamError, StreamHandle};
use crate::transport::peer_connection::streaming_buffer::FRAGMENT_PAYLOAD_SIZE;
use crate::transport::peer_connection::{inbound_stream_age_limit, streaming_handle_idle_timeout};

/// A short message from the peer. It holds its inbound budget charge until
/// its payload is taken.
pub(crate) struct InboundShort {
    payload: Bytes,
    _charge: Arc<LegacyStreamToken>,
}

impl std::fmt::Debug for InboundShort {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("InboundShort")
            .field("len", &self.payload.len())
            .finish_non_exhaustive()
    }
}

impl InboundShort {
    #[cfg_attr(not(test), allow(dead_code))]
    pub(crate) fn payload(&self) -> &Bytes {
        &self.payload
    }

    /// The payload; its charge is released.
    #[cfg_attr(not(test), allow(dead_code))]
    pub(crate) fn into_payload(self) -> Bytes {
        self.payload
    }
}

/// A stream from the peer, announced once its header and metadata have
/// arrived and before its payload. Its bytes arrive in `handle` as they are
/// received; the handle is cancelled if the stream fails.
///
/// `handle` is the transport-internal `StreamHandle`; e5 must wrap it in a
/// `boundary::IncomingStream` before anything above the transport sees it.
#[derive(Debug)]
#[cfg_attr(not(test), allow(dead_code))]
pub(crate) struct InboundStreamArrival {
    pub(crate) stream_id: StreamId,
    /// The message announcing the stream, if the sender sent one.
    pub(crate) metadata: Option<Bytes>,
    pub(crate) handle: StreamHandle,
}

/// Everything the receive task needs, shared by all its stream readers.
pub(in crate::transport::quic) struct Receiver<T> {
    time: T,
    budget: Arc<Mutex<InboundStreamBudget>>,
    stats: Arc<QuicPathStats>,
    shorts: mpsc::Sender<InboundShort>,
    streams: mpsc::Sender<InboundStreamArrival>,
    controls: mpsc::Sender<Bytes>,
    /// Source of the budget keys short messages are charged under.
    next_short_key: AtomicU32,
    /// Panic on the next stream, to test the supervisor.
    #[cfg(test)]
    pub(in crate::transport::quic) panic_on_stream: bool,
}

impl<T: TimeSource> Receiver<T> {
    pub(in crate::transport::quic) fn new(
        time: T,
        budget: Arc<Mutex<InboundStreamBudget>>,
        stats: Arc<QuicPathStats>,
        shorts: mpsc::Sender<InboundShort>,
        streams: mpsc::Sender<InboundStreamArrival>,
        controls: mpsc::Sender<Bytes>,
    ) -> Self {
        Self {
            time,
            budget,
            stats,
            shorts,
            streams,
            controls,
            next_short_key: AtomicU32::new(1),
            #[cfg(test)]
            panic_on_stream: false,
        }
    }
}

/// Accept every uni stream of `connection` and read them all concurrently
/// in this one task, until the connection ends; then let the readers see the
/// loss and end. Reading never waits on a consumer (only `try_send`), so one
/// stream cannot hold up another.
pub(in crate::transport::quic) async fn receive<T: TimeSource>(
    connection: quinn::Connection,
    receiver: Arc<Receiver<T>>,
) {
    let mut readers = FuturesUnordered::new();
    // Kept across iterations, so a pending accept is never dropped.
    let mut accept = std::pin::pin!(connection.accept_uni());
    loop {
        tokio::select! {
            accepted = &mut accept => match accepted {
                Ok(recv) => {
                    readers.push(read_one(recv, receiver.clone()));
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
}

impl<T: TimeSource> BodyReader<T> {
    /// A reader with the idle timeout and an age limit of
    /// [`uncharged_age_limit`]; admission replaces the age limit with the
    /// stream's own ([`Self::set_age_limit`]).
    fn new(recv: quinn::RecvStream, time: T, idle: Duration) -> Self {
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
        }
    }

    fn deadline(&self) -> u64 {
        self.last_progress
            .saturating_add(self.idle_nanos)
            .min(self.age_deadline)
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

    /// The next chunk of at most `max` bytes (at least 1), or `None` at the
    /// end of the stream.
    async fn next_chunk(&mut self, max: usize) -> Result<Option<Bytes>, ReadFailure> {
        loop {
            tokio::select! {
                read = self.recv.read_chunk(max, true) => {
                    return match read {
                        Ok(Some(chunk)) => {
                            self.last_progress = self.time.now_nanos();
                            Ok(Some(chunk.bytes))
                        }
                        Ok(None) => Ok(None),
                        Err(error) => Err(error.into()),
                    };
                }
                () = &mut self.timer => {
                    let now = self.time.now_nanos();
                    // Idle first, as the UDP sweep does.
                    if now >= self.last_progress.saturating_add(self.idle_nanos) {
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

/// Numbered fragments, in order.
type Fragments = Vec<(u32, Bytes)>;

// The Rechunker keeps a payload chunk of at least a whole fragment as a
// slice of its datagram, so a held fragment pins at most one datagram
// (except after quinn defragments, see the module doc). That
// is at most 1.2 x the fragment while a datagram is at most MAX_UDP_PAYLOAD;
// raising MAX_UDP_PAYLOAD past that must revisit the zero-copy path.
const _: () = assert!(
    super::super::config::MAX_UDP_PAYLOAD as usize * 10 <= FRAGMENT_PAYLOAD_SIZE * 12,
    "a kept datagram slice would pin more than 1.2 x a fragment"
);

/// Cuts a byte stream of `total` bytes into fragments of the UDP stream
/// shape: fragment `i` holds bytes `[(i-1)F, iF)` with
/// `F = FRAGMENT_PAYLOAD_SIZE`, so every fragment is full except the last,
/// none is empty, and there are `ceil(total / F)` of them (the buffer has
/// one more slot, for senders that shorten fragment #1).
///
/// The fragment that completes the stream is held back until
/// [`Self::finish`], which the caller calls only at the end of the stream,
/// so the buffer never reports a stream complete that then turns out to be
/// longer than declared.
struct Rechunker {
    total: u64,
    received: u64,
    /// Bytes handed out in fragments.
    emitted: u64,
    next_fragment: u32,
    pending: BytesMut,
}

impl Rechunker {
    fn new(total: u64) -> Self {
        Self {
            total,
            received: 0,
            emitted: 0,
            next_fragment: 1,
            pending: BytesMut::new(),
        }
    }

    /// Bytes still expected.
    fn remaining(&self) -> u64 {
        self.total - self.received
    }

    /// Take `chunk`, returning the fragments it completed, in order.
    fn feed(&mut self, mut chunk: Bytes) -> Result<Fragments, ReadFailure> {
        let len = chunk.len() as u64;
        if len > self.remaining() {
            return Err(ReadFailure::Overlong);
        }
        self.received += len;
        let full = FRAGMENT_PAYLOAD_SIZE;
        let mut out = Vec::new();
        while !chunk.is_empty() {
            // A fragment may go out only if it does not end the stream.
            let can_emit = self.emitted + (full as u64) < self.total;
            if self.pending.is_empty() && chunk.len() >= full && can_emit {
                out.push(self.emit(chunk.split_to(full)));
                continue;
            }
            if self.pending.is_empty() {
                // A fresh buffer of exactly this fragment's length, so it
                // never grows (a grown buffer would be kept, up to twice the
                // fragment) and never shares an allocation with the next one.
                let this_fragment = (self.total - self.emitted).min(full as u64) as usize;
                self.pending = BytesMut::with_capacity(this_fragment);
            }
            let take = (full - self.pending.len()).min(chunk.len());
            self.pending.extend_from_slice(&chunk.split_to(take));
            if self.pending.len() == full && can_emit {
                let fragment = std::mem::take(&mut self.pending).freeze();
                out.push(self.emit(fragment));
            }
        }
        Ok(out)
    }

    fn emit(&mut self, fragment: Bytes) -> (u32, Bytes) {
        self.emitted += fragment.len() as u64;
        let number = self.next_fragment;
        self.next_fragment += 1;
        (number, fragment)
    }

    /// The last fragment, once every declared byte has arrived and the
    /// stream has ended. The completion test is on BYTES, never on a count
    /// of fragments.
    fn finish(&mut self) -> Result<(u32, Bytes), ReadFailure> {
        if self.received < self.total {
            return Err(ReadFailure::Truncated(self.received as usize));
        }
        let last = std::mem::take(&mut self.pending).freeze();
        debug_assert!(!last.is_empty(), "a stream of {} bytes", self.total);
        Ok(self.emit(last))
    }
}

/// Owns the reader's handle (or token) of a charged stream: on drop it
/// cancels the handle if the stream did not complete, drops it, and then
/// releases the charge if no other clone is left. The reader keeps no
/// clone of its own, so the guard's is the last one the reader holds.
struct StreamGuard {
    budget: Arc<Mutex<InboundStreamBudget>>,
    key: StreamId,
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
    let mut reader = BodyReader::new(recv, receiver.time.clone(), streaming_handle_idle_timeout());
    let header = match reader.read_exact(HEADER_LEN).await {
        Ok(bytes) => FrameHeader::decode(&bytes),
        Err(ReadFailure::Truncated(got)) => Err(HeaderError::Truncated(got)),
        Err(failure) => {
            receiver.fail(&mut reader, failure, Charged::No);
            return;
        }
    };
    let header = match header {
        Ok(header) => header,
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
            ReadFailure::Reset => stats.record(QuicInboundOutcome::ResetByPeer),
            ReadFailure::ConnectionLost => stats.record(QuicInboundOutcome::ConnectionLost),
            ReadFailure::Other => stats.record(QuicInboundOutcome::ReadFailed),
        }
        tracing::debug!(?failure, "QUIC path: inbound stream failed");
    }

    /// Count a budget refusal as the UDP transport does, and stop the stream.
    fn refuse(&self, reader: &mut BodyReader<T>, rejection: BudgetRejection, total: u64) {
        let outcome = match rejection {
            BudgetRejection::Bytes { .. } => InboundStreamOutcome::RefusedConnectionBytes,
            BudgetRejection::Streams { .. } => InboundStreamOutcome::RefusedConnectionStreams,
        };
        self.stats.record_shared(outcome);
        tracing::debug!(?rejection, total, "QUIC path: refusing an inbound stream");
        reader.stop(stop_code::BUDGET_REFUSED);
    }

    /// A key for a short message's charge that no held charge uses. Short
    /// keys have the caller-named bit clear, so they never meet a `Stream`
    /// id (which has it set) in this path's budget.
    fn short_key(&self, budget: &InboundStreamBudget) -> StreamId {
        loop {
            let n = self.next_short_key.fetch_add(1, Ordering::Relaxed) & 0x7FFF_FFFF;
            let key = StreamId(n);
            if n != 0 && !budget.contains(key) {
                return key;
            }
        }
    }

    async fn read_short(&self, mut reader: BodyReader<T>, header: FrameHeader) {
        let total = header.total_len();
        let token = LegacyStreamToken::new();
        let key = {
            let mut budget = self.budget.lock();
            if let Err(rejection) = budget.check(total) {
                drop(budget);
                self.refuse(&mut reader, rejection, total);
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
        let mut guard = StreamGuard {
            budget: self.budget.clone(),
            key,
            handle: None,
            token: Some(token),
            complete: false,
        };
        reader.set_age_limit(inbound_stream_age_limit(total));
        // One buffer of the charged length (see `read_exact`).
        let read = {
            let token = guard.token.as_ref();
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
                self.fail(&mut reader, failure, Charged::Yes);
                return;
            }
        };
        if let Err(failure) = reader.expect_end().await {
            self.fail(&mut reader, failure, Charged::Yes);
            return;
        }
        let Some(token) = guard.token.clone() else {
            return;
        };
        token.mark_complete();
        guard.complete = true;
        let message = InboundShort {
            payload,
            _charge: token,
        };
        match self.shorts.try_send(message) {
            Ok(()) => self.stats.record(QuicInboundOutcome::DeliveredShort),
            Err(_) => {
                self.stats.record(QuicInboundOutcome::ShortDroppedFull);
                tracing::debug!(total, "QUIC path: short message channel full; dropping");
            }
        }
    }

    async fn read_stream(&self, mut reader: BodyReader<T>, header: FrameHeader) {
        let stream_id = header.stream_id();
        let total = header.total_len();
        let handle = {
            let mut budget = self.budget.lock();
            // A charge for this id whose state is already gone (an earlier
            // stream whose last handle was dropped after its reader ended)
            // must not refuse the id: nothing would release it otherwise.
            budget.release_if_gone(stream_id);
            if budget.contains(stream_id) {
                drop(budget);
                self.stats.record(QuicInboundOutcome::StreamIdInUse);
                reader.stop(stop_code::STREAM_ID_IN_USE);
                return;
            }
            if let Err(rejection) = budget.check(total) {
                drop(budget);
                self.refuse(&mut reader, rejection, total);
                return;
            }
            // Allocates only the buffer's chunk table (see
            // `LockFreeStreamBuffer`); slots come as fragments arrive.
            let handle = StreamHandle::new(stream_id, total);
            budget.charge(
                stream_id,
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
            key: stream_id,
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
        if self.streams.try_send(arrival).is_err() {
            self.stats.record(QuicInboundOutcome::StreamDroppedFull);
            reader.stop(stop_code::CHANNEL_FULL);
            return;
        }
        self.stats.record(QuicInboundOutcome::AnnouncedStream);

        let mut rechunker = Rechunker::new(total);
        while rechunker.remaining() > 0 {
            let chunk = match reader.next_chunk(clamp(rechunker.remaining())).await {
                Ok(Some(chunk)) => chunk,
                Ok(None) => break,
                Err(failure) => {
                    self.fail(&mut reader, failure, Charged::Yes);
                    return;
                }
            };
            let fragments = match rechunker.feed(chunk) {
                Ok(fragments) => fragments,
                Err(failure) => {
                    self.fail(&mut reader, failure, Charged::Yes);
                    return;
                }
            };
            for (number, fragment) in fragments {
                if !self.push(&mut reader, guard.handle(), number, fragment) {
                    return;
                }
            }
        }
        if let Err(failure) = reader.expect_end().await {
            self.fail(&mut reader, failure, Charged::Yes);
            return;
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

    /// Feed `total` bytes in chunks of the given sizes and return every
    /// fragment, the last from `finish`.
    fn rechunk(total: usize, chunk_sizes: &[usize]) -> (Fragments, Fragments) {
        let data = bytes(total, 3);
        let mut rechunker = Rechunker::new(total as u64);
        let mut early = Vec::new();
        let mut at = 0;
        for &size in chunk_sizes.iter().cycle() {
            if at == total {
                break;
            }
            let size = size.min(total - at);
            early.extend(rechunker.feed(data.slice(at..at + size)).unwrap());
            at += size;
        }
        let mut all = early.clone();
        all.push(rechunker.finish().unwrap());
        (early, all)
    }

    /// Every chunking of a stream gives the UDP fragment shape: fragment `i`
    /// is bytes `[(i-1)F, iF)`, all full but the last, none empty, numbered
    /// from 1, and the last one only from `finish`.
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
                let (early, all) = rechunk(total, chunks);
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

    /// The buffer the fragments go into accepts exactly that shape and is
    /// complete only after the last.
    #[test]
    fn rechunked_fragments_complete_a_stream_buffer_only_at_the_last() {
        for total in [1, F, F + 1, 5 * F] {
            let (_, all) = rechunk(total, &[777]);
            let handle = StreamHandle::new(StreamId(0x8000_0001), total as u64);
            let last = all.len() - 1;
            for (i, (number, fragment)) in all.into_iter().enumerate() {
                assert!(!handle.is_complete(), "complete before fragment {number}");
                assert_eq!(handle.push_fragment(number, fragment), Ok(true));
                assert_eq!(handle.is_complete(), i == last);
            }
            assert_eq!(handle.try_assemble().unwrap(), bytes(total, 3).to_vec());
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
            short.feed(data.slice(..total - 1)).unwrap();
            assert_eq!(
                short.finish(),
                Err(ReadFailure::Truncated(total - 1)),
                "{total} bytes"
            );

            let mut long = Rechunker::new(total as u64);
            long.feed(data.clone()).unwrap();
            assert_eq!(
                long.feed(Bytes::from_static(b"x")),
                Err(ReadFailure::Overlong)
            );
            let mut long_in_one = Rechunker::new(total as u64);
            let mut more = data.to_vec();
            more.push(0);
            assert_eq!(long_in_one.feed(more.into()), Err(ReadFailure::Overlong));
        }
    }
}
