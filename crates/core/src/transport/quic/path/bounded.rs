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
//!   the stream sent, then the deadline passed or the connection was lost
//!   while waiting for the acknowledgement. The peer may have the whole
//!   message. Nothing is reset: the stream is left to finish, and a
//!   streamed-message permit stays held until it does.
//! - [`BoundedSend::Failed`]: the message did not reach the peer as a whole.
//!   [`SendFailure::NotOpened`] means no stream ever existed, so the message
//!   can safely go another way. A deadline that passes later drops the send:
//!   the stream is reset (`reset_code::SENDER_ABORTED`), so the receiver
//!   discards what arrived.
//!
//! A caller must not treat `MaybeDelivered` as `Failed`: the two sides may
//! disagree about whether the message arrived.

use std::future::Future;
use std::sync::Arc;
use std::sync::atomic::{AtomicU8, AtomicU64, Ordering};
use std::time::Duration;

use super::QuicSendError;
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
    /// A send past it fails with [`SendFailure::NotOpened`].
    pub(crate) open: Duration,
    /// The longest wait between two steps of progress once the stream is open,
    /// up to the end of the stream being sent. Not applied to the wait for the
    /// acknowledgement: quinn's writes return once it has buffered the bytes,
    /// so that wait includes a send window of backlog and says nothing about
    /// a stall. `None`: only `total` applies.
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
    /// message can go another way.
    NotOpened(NotOpened),
    /// A deadline passed after the stream was opened and before its end was
    /// sent. The stream was reset.
    TimedOut,
    /// The send failed (`QuicSendError`), before the end of the stream was
    /// sent, or after it with a stop from the peer.
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
    now: Box<dyn Fn() -> u64 + Send + Sync>,
}

impl SendProgress {
    /// For a send with no deadline, which reads no clock. Tests only.
    #[cfg(test)]
    pub(super) fn new() -> Self {
        Self(Arc::new(ProgressState {
            stage: AtomicU8::new(STAGE_OPENING),
            last_step: AtomicU64::new(0),
            now: Box::new(|| 0),
        }))
    }

    fn on_clock<T: TimeSource>(time: &T) -> Self {
        let time = time.clone();
        Self(Arc::new(ProgressState {
            stage: AtomicU8::new(STAGE_OPENING),
            last_step: AtomicU64::new(time.now_nanos()),
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

    /// Every byte was written and the end of the stream sent.
    pub(super) fn mark_finished(&self) {
        self.0.stage.store(STAGE_FINISHED, Ordering::Release);
    }

    fn stage(&self) -> u8 {
        self.0.stage.load(Ordering::Acquire)
    }

    /// When the send's current stage runs out, on the clock.
    fn deadline(&self, deadlines: &SendDeadlines, started: u64) -> u64 {
        let total = started.saturating_add(nanos(deadlines.total));
        match self.stage() {
            STAGE_OPENING => started.saturating_add(nanos(deadlines.open)).min(total),
            STAGE_OPENED => deadlines.gap.map_or(total, |gap| {
                self.0
                    .last_step
                    .load(Ordering::Acquire)
                    .saturating_add(nanos(gap))
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

fn nanos(duration: Duration) -> u64 {
    u64::try_from(duration.as_nanos()).unwrap_or(u64::MAX)
}

/// Run the send `send` builds, until it returns or a deadline passes on
/// `time`. `send` gets the [`SendProgress`] to pass to the path's `*_with`
/// send, which is how a timeout is placed in the send. A closure that ignored
/// it would report every timeout as `NotOpened`, so this is private to the
/// path, whose `send_*_bounded` methods pass it on.
///
/// One timer at a time, as in `inbound::BodyReader`: armed for the current
/// deadline and, when it fires, re-armed if progress has moved the deadline.
/// So a send costs one sleep per gap, not one per fragment. A step only moves
/// the deadline later, so the armed timer is never too late. The move from
/// `open` to `gap` can make it earlier only with a gap shorter than the time
/// left of `open`, and the send's deadlines never have one.
///
/// Cancel safety: dropping this future drops the send, with the same
/// effect as a timeout at that point.
pub(super) async fn bounded_send<T, F, Fut>(
    time: &T,
    deadlines: impl Into<SendDeadlines>,
    send: F,
) -> BoundedSend
where
    T: TimeSource,
    F: FnOnce(SendProgress) -> Fut,
    Fut: Future<Output = Result<(), QuicSendError>>,
{
    let deadlines = deadlines.into();
    let started = time.now_nanos();
    let progress = SendProgress::on_clock(time);
    let send = send(progress.clone());
    tokio::pin!(send);
    loop {
        let deadline = progress.deadline(&deadlines, started);
        let wait = Duration::from_nanos(deadline.saturating_sub(time.now_nanos()));
        tokio::select! {
            result = &mut send => return outcome(result, &progress),
            () = time.sleep_cancelable(wait) => {
                if time.now_nanos() >= progress.deadline(&deadlines, started) {
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
