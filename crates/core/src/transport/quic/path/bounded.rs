//! Sends with deadlines (#5940).
//!
//! A path's sends wait without limit against a peer that does not
//! cooperate (see the `path` module doc), so every send the node makes goes
//! through a `QuicPath::send_*_bounded` method, built on [`bounded_send`].
//! [`SendDeadlines`] says how long each part of a send may take. The outcome
//! says what the sender can know:
//!
//! - [`BoundedSend::Sent`]: the peer's QUIC stack acknowledged every byte.
//!   Not a delivery (a full channel at the receiver drops an acknowledged
//!   message), as on UDP.
//! - [`BoundedSend::MaybeDelivered`]: every byte was written and the end of
//!   the stream queued, then the deadline passed or the connection was lost
//!   while waiting for the acknowledgement. The peer may have the whole
//!   message. Nothing is reset: the stream is left to finish, and a
//!   streamed-message permit stays held until it does.
//! - [`BoundedSend::Failed`]: the message did not reach the peer as a whole.
//!   [`SendFailure::NotOpened`] means no stream ever existed, so the message
//!   can safely go another way. A deadline that passes later drops the send:
//!   the stream is reset (`reset_code::SENDER_ABORTED`), so the receiver
//!   discards what arrived. The peer may still have seen the stream: it then
//!   remembers the id, so a resend under that id is dropped, and it may have
//!   dispatched the stream's metadata. Only a fresh id may send the data again.
//!
//! A caller must not treat `MaybeDelivered` as `Failed`: the two sides may
//! disagree about whether the message arrived.

use std::future::Future;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU8, AtomicU64, Ordering};
use std::time::Duration;

use super::super::recycle::{AckProgress, cap_held};
use super::super::socket::{CapHoldReading, CapHolds};
use super::QuicSendError;
use super::route::{STALL_PROGRESS_WINDOW, hold_credit};
use crate::simulation::TimeSource;
use crate::transport::peer_connection::streaming::STREAM_INACTIVITY_TIMEOUT;
use crate::transport::peer_connection::{fits_one_packet, inbound_stream_age_limit};

/// How long a message of one packet may take to open its stream.
const SHORT_OPEN_TIMEOUT: Duration = Duration::from_secs(1);
/// How long a message of one packet may take as a whole.
const SHORT_TOTAL_TIMEOUT: Duration = Duration::from_secs(5);
/// How long a streamed message may take to get a permit and open its stream.
const STREAM_OPEN_TIMEOUT: Duration = Duration::from_secs(2);

/// How long the parts of a bounded send may take.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct SendDeadlines {
    /// From the start of the send to its stream being open, permit included.
    /// For a send from a source, the start is the first chunk arriving. A send
    /// past it fails with [`SendFailure::NotOpened`]. At most `gap`, which
    /// `bounded_send` asserts.
    pub(crate) open: Duration,
    /// The longest wait between two steps of progress once the stream is open,
    /// up to the end of the stream being sent. Not applied to the wait for the
    /// acknowledgement: quinn's writes return once it has buffered the bytes,
    /// so that wait includes a send window of backlog and says nothing about
    /// a stall. `None`: only `total` applies. A send given its connection's
    /// progress ([`ConnectionProgress`]) runs out of gap only once the
    /// connection has also gone [`STALL_PROGRESS_WINDOW`] with nothing newly
    /// acknowledged.
    pub(crate) gap: Option<Duration>,
    /// From the start of the send to its end, whatever the send is doing.
    pub(crate) total: Duration,
}

impl SendDeadlines {
    /// One deadline for the whole send.
    pub(crate) const fn within(timeout: Duration) -> Self {
        Self {
            open: timeout,
            gap: None,
            total: timeout,
        }
    }

    /// For a Short frame of `len` bytes: one that does not fit a packet holds
    /// its slot for a whole transfer, so it is timed as a stream is.
    #[cfg_attr(not(test), allow(dead_code))]
    pub(crate) fn short(len: u64) -> Self {
        if fits_one_packet(len) {
            Self {
                open: SHORT_OPEN_TIMEOUT,
                gap: None,
                total: SHORT_TOTAL_TIMEOUT,
            }
        } else {
            Self::stream(len)
        }
    }

    /// For a streamed message of `len` bytes. The gap is the one the UDP
    /// sender and the receiver work to, and past `total` the receiver evicts
    /// the stream anyway.
    #[cfg_attr(not(test), allow(dead_code))]
    pub(crate) fn stream(len: u64) -> Self {
        Self {
            open: STREAM_OPEN_TIMEOUT,
            gap: Some(STREAM_INACTIVITY_TIMEOUT),
            total: inbound_stream_age_limit(len),
        }
    }
}

impl From<Duration> for SendDeadlines {
    fn from(timeout: Duration) -> Self {
        Self::within(timeout)
    }
}

/// What became of a bounded send.
#[derive(Debug)]
#[cfg_attr(not(test), allow(dead_code))]
pub(crate) enum BoundedSend {
    /// The peer acknowledged every byte.
    Sent,
    /// The whole message was written; whether it arrived is unknown.
    MaybeDelivered(MaybeDelivered),
    /// The message did not arrive whole.
    Failed(SendFailure),
}

/// Why a written message may or may not have arrived.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum MaybeDelivered {
    /// The deadline passed while waiting for the acknowledgement.
    TimedOut,
    /// The connection was lost while waiting for the acknowledgement.
    ConnectionLost,
}

/// Why a bounded send failed.
#[derive(Debug)]
#[cfg_attr(not(test), allow(dead_code))]
pub(crate) enum SendFailure {
    /// No stream was opened, so the peer holds nothing of the message and the
    /// message can go another way. Covers the waits before `open_uni` returns;
    /// once it has returned, a failure writing the header is not this.
    NotOpened(NotOpened),
    /// A deadline passed after the stream was opened and before its end was
    /// sent. The stream was reset.
    TimedOut,
    /// The send failed (`QuicSendError`), before the end of the stream was
    /// sent, or after it with a stop from the peer. This includes a source that
    /// ended or failed before its first chunk, where no stream was opened: such
    /// a message cannot go another way either, so it is not `NotOpened`.
    Error(QuicSendError),
}

/// Why a send opened no stream.
#[derive(Debug)]
#[cfg_attr(not(test), allow(dead_code))]
pub(crate) enum NotOpened {
    /// The deadline passed waiting for the source's first chunk, a
    /// streamed-message permit or a stream credit.
    TimedOut,
    /// The connection was lost first.
    ConnectionLost(quinn::ConnectionError),
}

const STAGE_OPENING: u8 = 0;
const STAGE_OPENED: u8 = 1;
const STAGE_FINISHED: u8 = 2;

/// How far a send has got, written by the path as it goes and read by
/// [`bounded_send`], so one timer can serve the whole send.
#[derive(Clone)]
pub(super) struct SendProgress(Arc<ProgressState>);

struct ProgressState {
    stage: AtomicU8,
    /// When the send last made progress, on `now`.
    last_step: AtomicU64,
    /// The send is waiting for its own source, not for the connection.
    on_source: AtomicBool,
    /// The armed timer is past the send's own gap because of the connection's
    /// progress, so a wait for the source would bring the deadline earlier.
    extended: AtomicBool,
    /// Told when a wait for the source starts while `extended`.
    source_wait: tokio::sync::Notify,
    /// Timers armed by [`bounded_send`]. Tests only.
    #[cfg(test)]
    arms: AtomicU64,
    now: Box<dyn Fn() -> u64 + Send + Sync>,
}

impl SendProgress {
    /// For a send with no deadline, which reads no clock. Tests only.
    #[cfg(test)]
    pub(super) fn new() -> Self {
        Self(Arc::new(ProgressState {
            stage: AtomicU8::new(STAGE_OPENING),
            last_step: AtomicU64::new(0),
            on_source: AtomicBool::new(false),
            extended: AtomicBool::new(false),
            source_wait: tokio::sync::Notify::new(),
            #[cfg(test)]
            arms: AtomicU64::new(0),
            now: Box::new(|| 0),
        }))
    }

    fn on_clock<T: TimeSource>(time: &T) -> Self {
        let time = time.clone();
        Self(Arc::new(ProgressState {
            stage: AtomicU8::new(STAGE_OPENING),
            last_step: AtomicU64::new(time.now_nanos()),
            on_source: AtomicBool::new(false),
            extended: AtomicBool::new(false),
            source_wait: tokio::sync::Notify::new(),
            #[cfg(test)]
            arms: AtomicU64::new(0),
            now: Box::new(move || time.now_nanos()),
        }))
    }

    /// The stream exists.
    pub(super) fn mark_opened(&self) {
        self.step();
        self.0.stage.store(STAGE_OPENED, Ordering::Release);
    }

    /// One more part of the message was written.
    pub(super) fn step(&self) {
        self.0.last_step.store((self.0.now)(), Ordering::Release);
    }

    /// The send starts (`true`) or stops waiting for its source's next chunk.
    /// The connection's progress does not extend the gap meanwhile: the
    /// receiver gets nothing and ends the stream at its own idle timeout.
    ///
    /// With [`bounded_send`]'s loop this is a store-then-load handshake on two
    /// flags, both `SeqCst`: this side stores `on_source` then reads
    /// `extended`, the loop stores `extended` then reads `on_source`, so at
    /// least one sees the other's store, and either the loop arms its timer
    /// for the send's own gap or this wakes it to re-arm.
    pub(super) fn waiting_on_source(&self, waiting: bool) {
        self.0.on_source.store(waiting, Ordering::SeqCst);
        if waiting && self.0.extended.load(Ordering::SeqCst) {
            self.0.source_wait.notify_one();
        }
    }

    /// Timers [`bounded_send`] has armed for this send. Tests only.
    #[cfg(test)]
    pub(super) fn arms(&self) -> u64 {
        self.0.arms.load(Ordering::Relaxed)
    }

    /// Every byte was written and the end of the stream queued.
    pub(super) fn mark_finished(&self) {
        self.0.stage.store(STAGE_FINISHED, Ordering::Release);
    }

    fn stage(&self) -> u8 {
        self.0.stage.load(Ordering::Acquire)
    }

    /// When the send's current stage runs out, on the clock. While the stream
    /// is open the gap ends at the later of the send's own gap and
    /// `connection_until` (0 for none), unless the send waits for its source,
    /// and never after `total`.
    fn deadline(&self, deadlines: &SendDeadlines, started: u64, connection_until: u64) -> u64 {
        let total = started.saturating_add(nanos(deadlines.total));
        match self.stage() {
            STAGE_OPENING => started.saturating_add(nanos(deadlines.open)).min(total),
            STAGE_OPENED => deadlines.gap.map_or(total, |gap| {
                self.0
                    .last_step
                    .load(Ordering::Acquire)
                    .saturating_add(nanos(gap))
                    .max(if self.0.on_source.load(Ordering::SeqCst) {
                        0
                    } else {
                        connection_until
                    })
                    .min(total)
            }),
            _ => total,
        }
    }

    fn timed_out(&self) -> BoundedSend {
        match self.stage() {
            STAGE_OPENING => BoundedSend::Failed(SendFailure::NotOpened(NotOpened::TimedOut)),
            STAGE_OPENED => BoundedSend::Failed(SendFailure::TimedOut),
            _ => BoundedSend::MaybeDelivered(MaybeDelivered::TimedOut),
        }
    }
}

/// What a send reads of its connection's progress: the peer's newly
/// acknowledged packets (`recycle::AckProgress`) and the egress cap's holds
/// of QUIC transmits, the two signals the stall count (`route::StallCount`)
/// reads.
#[derive(Clone)]
pub(crate) struct ConnectionProgress {
    pub(crate) acks: AckProgress,
    pub(crate) cap_holds: Arc<CapHolds>,
    /// Where a send that ran out of gap leaves its [`QuietProof`]; the path's.
    pub(crate) quiet: Arc<parking_lot::Mutex<Option<QuietProof>>>,
    /// When the connection's acknowledged count was last seen to move, on the
    /// send's clock, or [`NO_ACK`]: the path's record, which the stall count
    /// and every send's watch keep. A send's watch starts from it, so a hold is
    /// credited when the connection, not only this send, was acknowledged
    /// within `route::hold_credit`.
    pub(crate) last_acked: Arc<AtomicU64>,
}

/// [`ConnectionProgress::last_acked`] when nothing has been seen acknowledged.
pub(crate) const NO_ACK: u64 = u64::MAX;

/// What a send that ran out of its gap saw of its connection: from `since`
/// to the timeout the acknowledged count stayed `acked` and the cap's hold
/// count `holds`. So if the connection still reads those counts, nothing on
/// it has moved since `since`, whichever send left the proof, and the stall
/// count (`route::StallCount::take`) dates the progress it sees from `since`
/// instead of from when it reads it. Without it, the first timeout after a
/// path goes dark mid-send sees that send's own early acknowledgements as new
/// and does not count, which delays the path going suspect by a window.
///
/// The counts do not cover a cap hold that is already live: it counts as
/// progress (`recycle::cap_held`) without moving either count, and can come
/// and go between two settles. So the proof also says whether any reading
/// during the send (its first, or a wake) saw a live hold, and the stall count
/// uses only a proof whose send saw none. That is enough: a hold that starts
/// during the send either moves `holds` past the proof's last reading, so the
/// proof stops matching, or was read live by a wake, which set the flag; and
/// one live at any time after `since` that started before it was live at
/// `since`, which is a reading of the send.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct QuietProof {
    pub(crate) acked: u64,
    pub(crate) holds: u64,
    pub(crate) since: u64,
    /// Some reading during the send saw a hold live.
    pub(crate) live_hold_seen: bool,
}

/// One send's view of [`ConnectionProgress`], sampled when its timer fires.
///
/// Under random loss the send window drains at Cubic's window per round
/// trip, so a write can wait past the 5 s gap while the connection keeps
/// being acknowledged. Such a send is moving, and failing it resets a stream
/// the peer is receiving. So while the stream is open the gap also runs to
/// [`STALL_PROGRESS_WINDOW`] after the connection last moved: a packet newly
/// acknowledged, or an egress cap hold (`recycle::cap_held`, as the stall
/// count and the ack watch read one). A send fails on its gap only once both
/// its own gap and that window have run out. Not while the send waits for
/// its own source ([`SendProgress::waiting_on_source`]): the connection is not
/// what holds it then, and the receiver, which gets nothing, ends the stream at
/// its own 5 s idle timeout.
///
/// The connection is read only at the timer's wakes, so a move is dated to
/// the wake that saw it, which can come up to one window after it. A send
/// that stops moving therefore ends at its own gap or at most twice
/// [`STALL_PROGRESS_WINDOW`] after the connection's last move, whichever is
/// later, and never after `total`. So a path dark before the send started
/// fails it at its own gap (5 s), and one that goes dark mid-send, whose last
/// acknowledgements are of this send's own first flight, fails it between
/// its gap and twice the window after those acknowledgements (12 to 22 s
/// with the 2 s open deadline as the first wake: 12 s when they came before
/// it, up to 22 s when one came just after it). Such a send leaves a
/// [`QuietProof`], so its timeout counts toward the stall close and the
/// path goes suspect at once.
struct ConnectionWatch {
    source: ConnectionProgress,
    acked: u64,
    cap: CapHoldReading,
    hold_unchanged_since: Option<u64>,
    /// When the watch began, on the send's clock.
    started: u64,
    /// The wake that last saw the connection move; `None` before the first.
    moved_at: Option<u64>,
    /// When the connection's acknowledged count last moved, from the path's
    /// record when the send starts, then each wake that saw it move; `None`
    /// before any. A cap hold is progress only within `route::hold_credit` of
    /// it.
    acked_at: Option<u64>,
    /// A reading saw a hold live ([`QuietProof::live_hold_seen`]).
    live_hold_seen: bool,
}

impl ConnectionWatch {
    fn new(source: ConnectionProgress, now: u64) -> Self {
        let cap = source.cap_holds.read();
        Self {
            acked: source.acks.acked(),
            cap,
            hold_unchanged_since: None,
            started: now,
            moved_at: None,
            acked_at: Some(source.last_acked.load(Ordering::Acquire)).filter(|at| *at != NO_ACK),
            live_hold_seen: cap.held_now,
            source,
        }
    }

    /// The send ran out of gap: leave what it saw for the stall count.
    fn prove_quiet(&self) {
        *self.source.quiet.lock() = Some(QuietProof {
            acked: self.acked,
            holds: self.cap.holds,
            since: self.moved_at.unwrap_or(self.started),
            live_hold_seen: self.live_hold_seen,
        });
    }

    /// Read the connection at `now`.
    fn observe(&mut self, now: u64) {
        let acked = self.source.acks.acked();
        let cap = self.source.cap_holds.read();
        let held = cap_held(&mut self.hold_unchanged_since, now, &cap, &self.cap);
        self.cap = cap;
        self.live_hold_seen |= cap.held_now;
        let acknowledged = acked != self.acked;
        if acknowledged {
            self.acked_at = Some(now);
            self.source.last_acked.fetch_max(now, Ordering::AcqRel);
        }
        // A held transmit on a connection nothing has acknowledged for the
        // window is no evidence the path is alive: a dark connection's own
        // probes wait for a saturated cap too.
        let held = held
            && self
                .acked_at
                .is_some_and(|at| now.saturating_sub(at) <= nanos(hold_credit(&cap)));
        if acknowledged || held {
            self.acked = acked;
            self.moved_at = Some(now);
        }
    }

    /// When the window after the connection's last seen move ends; 0 for none.
    fn until(&self) -> u64 {
        self.moved_at
            .map_or(0, |at| at.saturating_add(nanos(STALL_PROGRESS_WINDOW)))
    }
}

fn nanos(duration: Duration) -> u64 {
    u64::try_from(duration.as_nanos()).unwrap_or(u64::MAX)
}

/// Run the send `send` builds, until it returns or a deadline passes on
/// `time`. With `connection`, a send whose stream is open runs out of gap
/// only once its connection has also stopped moving ([`ConnectionWatch`]),
/// and one that does leaves a [`QuietProof`] for the path's stall count.
/// `send` gets the [`SendProgress`] to pass to the path's `*_with` send,
/// which is how a timeout is placed in the send. A closure that ignored it
/// would report every timeout as `NotOpened`, so this is private to the path,
/// whose `send_*_bounded` methods pass it on.
///
/// One timer at a time, as in `inbound::BodyReader`: armed for the current
/// deadline and, when it fires, re-armed if progress has moved the deadline.
/// So a send costs one sleep per gap, not one per fragment. A step only moves
/// the deadline later, so the armed timer is never too late. A wait for the
/// source can move it earlier (it drops the connection's window), so when the
/// timer is armed past the send's own gap it wakes the loop to re-arm
/// (`SendProgress::waiting_on_source` says why no wait is missed). The move
/// from `open` to `gap` could make it earlier only with a gap shorter than
/// `open`, which is asserted against below.
///
/// Cancel safety: dropping this future drops the send, with the same
/// effect as a timeout at that point.
pub(super) async fn bounded_send<T, F, Fut>(
    time: &T,
    deadlines: impl Into<SendDeadlines>,
    connection: Option<ConnectionProgress>,
    send: F,
) -> BoundedSend
where
    T: TimeSource,
    F: FnOnce(SendProgress) -> Fut,
    Fut: Future<Output = Result<(), QuicSendError>>,
{
    let deadlines = deadlines.into();
    debug_assert!(
        deadlines.gap.is_none_or(|gap| gap >= deadlines.open),
        "a gap shorter than `open` could leave the timer armed too late"
    );
    let started = time.now_nanos();
    // Only the gap reads the connection.
    let mut watch = deadlines
        .gap
        .and(connection)
        .map(|connection| ConnectionWatch::new(connection, started));
    let until = |watch: &Option<ConnectionWatch>| watch.as_ref().map_or(0, ConnectionWatch::until);
    let progress = SendProgress::on_clock(time);
    let send = send(progress.clone());
    tokio::pin!(send);
    loop {
        // `extended` first, from the connection's window alone, then the
        // deadline, which reads `on_source` (see `waiting_on_source`).
        let own = progress.deadline(&deadlines, started, 0);
        let window = until(&watch);
        progress.0.extended.store(window > own, Ordering::SeqCst);
        let deadline = progress.deadline(&deadlines, started, window);
        #[cfg(test)]
        progress.0.arms.fetch_add(1, Ordering::Relaxed);
        let wait = Duration::from_nanos(deadline.saturating_sub(time.now_nanos()));
        tokio::select! {
            result = &mut send => return outcome(result, &progress),
            () = progress.0.source_wait.notified() => {}
            () = time.sleep_cancelable(wait) => {
                let now = time.now_nanos();
                if let Some(watch) = watch.as_mut() {
                    watch.observe(now);
                }
                if now >= progress.deadline(&deadlines, started, until(&watch)) {
                    if progress.stage() == STAGE_OPENED
                        && let Some(watch) = watch.as_ref()
                    {
                        watch.prove_quiet();
                    }
                    return progress.timed_out();
                }
            }
        }
    }
}

/// The outcome of a send that returned.
pub(super) fn outcome(result: Result<(), QuicSendError>, progress: &SendProgress) -> BoundedSend {
    match (result, progress.stage()) {
        (Ok(()), _) => BoundedSend::Sent,
        (Err(QuicSendError::ConnectionLost(error)), STAGE_OPENING) => {
            BoundedSend::Failed(SendFailure::NotOpened(NotOpened::ConnectionLost(error)))
        }
        (Err(QuicSendError::ConnectionLost(_)), STAGE_FINISHED) => {
            BoundedSend::MaybeDelivered(MaybeDelivered::ConnectionLost)
        }
        (Err(error), _) => BoundedSend::Failed(SendFailure::Error(error)),
    }
}
