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
    /// After the stream was opened, the connection's bandwidth-limit pacer
    /// refused a write whose wait would have reached the send's deadline
    /// (`pacer` module). The stream was reset. This side held the send back,
    /// so it says nothing about the path.
    Paced,
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
    /// The connection's bandwidth-limit pacer refused the header's wait, which
    /// would have reached the open deadline (`pacer` module). Held back on this
    /// side, so it says nothing about the path.
    Paced,
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
    /// The send's deadlines and its start on `now`, so a pacer can refuse a
    /// wait that would reach the current one. `None`: no deadline.
    deadlines: Option<(SendDeadlines, u64)>,
    /// The path's pacer refused a write, so the send ends as at its current
    /// deadline (`pacer` module).
    refused: AtomicBool,
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
            deadlines: None,
            refused: AtomicBool::new(false),
        }))
    }

    fn on_clock<T: TimeSource>(time: &T, deadlines: SendDeadlines, started: u64) -> Self {
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
            deadlines: Some((deadlines, started)),
            refused: AtomicBool::new(false),
        }))
    }

    /// How long a pacer may make the send wait: until the send's own
    /// deadline for its stage (its own gap while the stream is open, never
    /// the connection's window), so no later than the deadline its timer
    /// enforces, which is never earlier. `Duration::MAX` for a send with none.
    ///
    /// Not the window: that would let the pacer's queue hold up to the window
    /// of the rate (about 10 s) where #6126 bounds it at the gap (5 s), so
    /// later opens, limited to their 2 s open deadline, would be refused for
    /// longer and more data would go to UDP. A live lossy path does not need
    /// it: its long waits are quinn's, after the pacer's take, and the write
    /// they end in is a step, so the next take is offered a fresh gap.
    /// A wait for the source is not a step, though: after a slow chunk the
    /// next take may be offered little or nothing, and a refusal mid-stream
    /// drops the message (`Failed(Paced)`). That is #6126's behaviour on main,
    /// not changed here.
    pub(super) fn time_left(&self) -> Duration {
        match &self.0.deadlines {
            Some((deadlines, started)) => Duration::from_nanos(
                self.deadline(deadlines, *started, 0)
                    .saturating_sub((self.0.now)()),
            ),
            None => Duration::MAX,
        }
    }

    /// The pacer refused a write: the send ends as at its current deadline.
    pub(super) fn refuse(&self) {
        self.0.refused.store(true, Ordering::Release);
    }

    fn refused(&self) -> bool {
        self.0.refused.load(Ordering::Acquire)
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
    /// The connection's progress does not extend the gap meanwhile: it is not
    /// what holds the send ([`ConnectionWatch`]).
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

    /// The outcome of a send the pacer refused, by how far it had got.
    fn paced(&self) -> BoundedSend {
        match self.stage() {
            STAGE_OPENING => BoundedSend::Failed(SendFailure::NotOpened(NotOpened::Paced)),
            _ => BoundedSend::Failed(SendFailure::Paced),
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
    /// The path's record of the connection's acknowledged count and when it
    /// was last seen to move ([`AckRecord`]), which the stall count and every
    /// send's watch keep. A send's watch starts from it, so a hold is credited
    /// when the connection, not only this send, was acknowledged within
    /// `route::hold_credit`.
    pub(crate) last_acked: Arc<AckRecord>,
}

/// [`AckSeen::at`] when nothing has been seen acknowledged.
pub(crate) const NO_ACK: u64 = u64::MAX;

/// The acknowledged count last seen on a connection and when it was seen to
/// move, on the path's clock ([`NO_ACK`] for never).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct AckSeen {
    pub(crate) count: u64,
    pub(crate) at: u64,
}

impl AckSeen {
    /// A settle that read the count `acked` (`None` without a counting
    /// controller) dates the last acknowledgement `dated`: forward when it saw
    /// a move, or back to a matching proof's `since` (`route::StallCount::take`).
    /// A reading below the record's count is stale (a watch dated a newer count
    /// since), so then the date only moves forward: going back on a stale count
    /// would remove hold credit the newer acknowledgement earned.
    pub(crate) fn settle(&mut self, acked: Option<u64>, dated: u64) {
        match acked {
            Some(acked) if acked < self.count => {
                if self.at == NO_ACK || dated > self.at {
                    self.at = dated;
                }
            }
            Some(acked) => {
                self.count = acked;
                self.at = dated;
            }
            None => self.at = dated,
        }
    }
}

/// A path's [`AckSeen`], kept by the stall count at settles and by every
/// send's watch at its start and wakes. The count is stored with the time so
/// that a move nobody has dated yet (an acknowledgement after the last settle
/// or wake) is seen as one: a send's watch dates it at its own start
/// ([`AckRecord::date_unseen`]), the earliest time this side knows of it,
/// rather than taking the older time as the last acknowledgement. One lock,
/// never held across an await; the stall count's settle takes it while
/// holding the stall lock, and nothing holding it takes the stall lock.
#[derive(Debug)]
pub(crate) struct AckRecord(parking_lot::Mutex<AckSeen>);

impl AckRecord {
    pub(crate) fn new(count: u64, at: u64) -> Self {
        Self(parking_lot::Mutex::new(AckSeen { count, at }))
    }

    /// The record now. Tests only.
    #[cfg(test)]
    pub(crate) fn seen(&self) -> AckSeen {
        *self.0.lock()
    }

    /// The count is `count` at `now`: if the record has not seen it, the
    /// connection was acknowledged since the record last moved, and it is
    /// dated `now`. Returns when the connection was last acknowledged, if
    /// ever. Acknowledged counts only grow, so only a count above the
    /// record's is a move.
    pub(crate) fn date_unseen(&self, count: u64, now: u64) -> Option<u64> {
        let mut seen = self.0.lock();
        // Only a higher count is a move: a caller holding a stale, lower
        // reading has seen nothing new.
        if count > seen.count {
            seen.count = count;
            seen.at = if seen.at == NO_ACK {
                now
            } else {
                seen.at.max(now)
            };
        }
        Some(seen.at).filter(|at| *at != NO_ACK)
    }

    /// Replace the record, under the caller's ordering (the stall lock).
    pub(crate) fn with<R>(&self, f: impl FnOnce(&mut AckSeen) -> R) -> R {
        f(&mut self.0.lock())
    }
}

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
///
/// Relies on both counts only growing: the connection's acknowledged count
/// (`recycle::AckProgress`, a counter of newly acknowledged packets) and the
/// cap's hold events (`socket::CapHolds::holds`). A count that could go back
/// would let a stale proof match again.
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
/// what holds it then, and a source that stalls is bounded by the send's own
/// gap, as a relayed stream on UDP is by pipe_stream's 5 s upstream
/// inactivity. (The receiver does not force this: a QUIC stream's reader
/// idles out at `streaming_handle_idle_timeout`, 30 s in production.)
///
/// The connection is read only at the timer's wakes, so a move is dated to
/// the wake that saw it, which can come up to one window after it. A send
/// that stops moving therefore ends at its own gap or at most twice
/// [`STALL_PROGRESS_WINDOW`] after the connection's last move, whichever is
/// later, and never after `total`. One that goes dark mid-send, whose last
/// acknowledgements are of this send's own first flight, fails between its
/// gap and twice the window after those acknowledgements (12 to 22 s with
/// the 2 s open deadline as the first wake: 12 s when they came before it,
/// up to 22 s when one came just after it). Such a send leaves a
/// [`QuietProof`], so its timeout counts toward the stall close and the path
/// goes suspect at once.
///
/// A path dark before the send started gives it no move to see, so with no
/// egress cap holding the connection's transmits the send fails at its own
/// gap (5 s). Where the cap holds them, a hold is credited for
/// `route::hold_credit` after the connection's last acknowledgement as the
/// path's record dates it ([`AckRecord`]: the build time on a new path, and
/// an acknowledgement the record has not seen is dated at the send's start),
/// so the send can run to that date plus `hold_credit` plus one window, never
/// past `total`. After an idle spell, a capped path that went dark gets one
/// such credit from its first new send: the keepalive acknowledgements before
/// it went dark are dated at that send's start.
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
    /// record when the send starts (a move the record has not seen is dated
    /// at the start), then each wake that saw it move; `None` before any. A
    /// cap hold is progress only within `route::hold_credit` of it.
    acked_at: Option<u64>,
    /// A reading saw a hold live ([`QuietProof::live_hold_seen`]).
    live_hold_seen: bool,
}

impl ConnectionWatch {
    fn new(source: ConnectionProgress, now: u64) -> Self {
        let cap = source.cap_holds.read();
        let acked = source.acks.acked();
        Self {
            acked,
            cap,
            hold_unchanged_since: None,
            started: now,
            moved_at: None,
            // A count the path's record has not seen moved since the record
            // last did, and is dated now (`AckRecord::date_unseen`).
            acked_at: source.last_acked.date_unseen(acked, now),
            live_hold_seen: cap.held_now,
            source,
        }
    }

    /// The send ran out of gap: leave what it saw for the stall count.
    ///
    /// The path has one slot, read (not taken) by every settle, so a later
    /// send's timeout may come before an earlier one's settle. A proof of the
    /// same counts is merged rather than replaced: both say nothing moved
    /// from their `since` to their timeout, and as the counts only grow, the
    /// two together say nothing moved from the earlier `since`, so the
    /// earlier send's settle still dates progress back to it. A live hold
    /// either saw is kept. A proof of other counts no longer matches the
    /// connection, so replacing it loses nothing.
    fn prove_quiet(&self) {
        let proof = QuietProof {
            acked: self.acked,
            holds: self.cap.holds,
            since: self.moved_at.unwrap_or(self.started),
            live_hold_seen: self.live_hold_seen,
        };
        let mut slot = self.source.quiet.lock();
        *slot = Some(match *slot {
            Some(left) if (left.acked, left.holds) == (proof.acked, proof.holds) => QuietProof {
                since: left.since.min(proof.since),
                live_hold_seen: left.live_hold_seen || proof.live_hold_seen,
                ..proof
            },
            _ => proof,
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
            self.source.last_acked.date_unseen(acked, now);
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
    let progress = SendProgress::on_clock(time, deadlines, started);
    let send = send(progress.clone());
    tokio::pin!(send);
    loop {
        // `extended` first, from the connection's window alone, then the
        // deadline, which reads `on_source` (see `waiting_on_source`). Then
        // `extended` again from the deadline actually armed: one armed at
        // the send's own gap (the send is waiting for its source) has
        // nothing a source wait could bring earlier, so later source waits
        // do not wake the loop to re-arm the same deadline.
        let own = progress.deadline(&deadlines, started, 0);
        let window = until(&watch);
        progress.0.extended.store(window > own, Ordering::SeqCst);
        let deadline = progress.deadline(&deadlines, started, window);
        if deadline <= own {
            progress.0.extended.store(false, Ordering::SeqCst);
        }
        #[cfg(test)]
        progress.0.arms.fetch_add(1, Ordering::Relaxed);
        let wait = Duration::from_nanos(deadline.saturating_sub(time.now_nanos()));
        tokio::select! {
            result = &mut send => {
                // A refusal returns an error only to end the send, which then
                // ends as a deadline would at that stage. It leaves no
                // `QuietProof`: this side held the send back.
                if progress.refused() {
                    return progress.paced();
                }
                return outcome(result, &progress);
            }
            () = progress.0.source_wait.notified() => {}
            () = time.sleep_cancelable(wait) => {
                let now = time.now_nanos();
                // Only the gap of an open stream reads the connection.
                if progress.stage() == STAGE_OPENED
                    && let Some(watch) = watch.as_mut()
                {
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
