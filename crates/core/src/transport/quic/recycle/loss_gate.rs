//! A loss gate in front of Cubic: a loss counts as congestion only when the
//! path shows a queue, or when too much is being lost to ignore.
//!
//! # Why
//!
//! Cubic takes every loss as congestion and cuts its window by 30%. Home
//! peers lose packets for other reasons (Wi-Fi, cheap routers, mobile links):
//! telemetry on the UDP transport put a third of home nodes above 5%
//! retransmitted. Under 5% random loss Cubic's window collapses to a few
//! datagrams, and a QUIC path moves an 8 MiB message in minutes, or not at
//! all within the deadlines, where the UDP transport takes seconds. The UDP
//! transport is to be removed, so QUIC alone has to deliver under that loss.
//!
//! # How
//!
//! A drop-tail bottleneck drops a packet when its queue is full, so a loss
//! from congestion comes with the round trip above its minimum by the
//! queue's delay. A random loss comes with no queue. So [`LossGate`] passes a
//! loss to Cubic only when:
//!
//! - quinn declares persistent congestion (always passed), or
//! - the queue it estimates is at least [`queue_threshold`] of the minimum
//!   RTT ([`QUEUE_FRACTION`], at least [`QUEUE_FLOOR`], at most
//!   [`QUEUE_CEILING`]), or
//! - the smoothed share of bytes lost per round trip is at least
//!   [`LOSS_SHARE_CEILING`] (a policer, or a path too lossy to push), or
//! - there is no RTT sample yet.
//!
//! Otherwise the loss is ignored: quinn still retransmits the data, and the
//! window stays where it was.
//!
//! The queue estimate is the smallest RTT sample over this round trip and the
//! last one, less the smallest sample seen on the connection. Jitter only
//! adds delay, so the minimum over a round filters it, while a standing
//! queue raises every sample, the minimum too.
//!
//! # What this does to other traffic
//!
//! On a path with a queue the controller is Cubic once the queue reaches the
//! threshold, so it holds a standing queue of about the threshold instead of
//! filling the buffer, and it backs off for a competing flow. On a queue
//! shallower than the threshold a congestion loss looks random; the
//! loss-share ceiling is then what stops it. The e5b-9 lab measured both
//! (`QUIC-LOSS-CONTROLLER.md` in the transport redesign notes).
//!
//! The minimum RTT is never forgotten. If a route grows longer the estimate
//! reads the extra delay as a queue and every loss is passed: the controller
//! falls back to plain Cubic, the behaviour before this gate.

use std::any::Any;
use std::time::{Duration, Instant};

use quinn::congestion::{Controller, ControllerMetrics};

/// The queue delay, as a fraction of the minimum RTT, at which a loss counts
/// as congestion.
pub(crate) const QUEUE_FRACTION: f64 = 0.1;

/// The smallest queue delay threshold, for short paths where a tenth of the
/// RTT is within timer and scheduling noise.
pub(crate) const QUEUE_FLOOR: Duration = Duration::from_millis(2);

/// The largest queue delay threshold, so a shallow queue on a long path is
/// still seen.
pub(crate) const QUEUE_CEILING: Duration = Duration::from_millis(3);

/// The smoothed share of bytes lost per round trip at which every loss
/// counts, queue or not.
pub(crate) const LOSS_SHARE_CEILING: f64 = 0.25;

/// A round trip's loss share enters the smoothed share only when at least
/// this many bytes were acknowledged or lost in it: in a small window one
/// loss is a large share (2 of 8 is 25%).
const SHARE_MIN_BYTES: u64 = 16 * 1200;

/// The weight of a new round trip's share in the smoothed share.
const SHARE_GAIN: f64 = 0.25;

/// The round length before the first RTT sample.
const DEFAULT_ROUND: Duration = Duration::from_millis(100);

/// The queue delay at which a loss counts as congestion on a path whose
/// minimum RTT is `min_rtt`.
pub(crate) fn queue_threshold(min_rtt: Duration) -> Duration {
    min_rtt
        .mul_f64(QUEUE_FRACTION)
        .clamp(QUEUE_FLOOR, QUEUE_CEILING)
}

/// Why a loss was passed to the inner controller, or that it was not.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Verdict {
    /// quinn declared persistent congestion.
    Persistent,
    /// The smoothed loss share is at or above [`LOSS_SHARE_CEILING`].
    LossShare,
    /// The queue estimate is at or above [`queue_threshold`].
    Queue,
    /// No RTT sample yet, so no queue estimate.
    NoSample,
    /// None of the above: the loss is taken as random and ignored.
    Ignored,
}

/// The gate's state, apart from the controller it wraps.
#[derive(Debug, Clone, Default)]
pub(crate) struct GateState {
    min_rtt: Option<Duration>,
    srtt: Option<Duration>,
    round_min: Option<Duration>,
    last_round_min: Option<Duration>,
    round_start: Option<Instant>,
    round_acked: u64,
    round_lost: u64,
    share: f64,
    /// Losses passed to the inner controller, and ignored.
    pub(crate) passed: u64,
    pub(crate) ignored: u64,
}

impl GateState {
    /// An acknowledgement of `bytes` sent at `sent`, with quinn's smoothed
    /// RTT `srtt`.
    pub(crate) fn on_ack(&mut self, now: Instant, sent: Instant, bytes: u64, srtt: Duration) {
        let sample = now.saturating_duration_since(sent);
        self.min_rtt = Some(self.min_rtt.map_or(sample, |m| m.min(sample)));
        self.srtt = Some(srtt);
        self.roll(now);
        self.round_min = Some(self.round_min.map_or(sample, |m| m.min(sample)));
        self.round_acked += bytes;
    }

    /// A loss of `lost_bytes`; returns whether it counts as congestion.
    pub(crate) fn on_loss(&mut self, now: Instant, lost_bytes: u64, persistent: bool) -> Verdict {
        self.roll(now);
        self.round_lost += lost_bytes;
        let verdict = self.judge(persistent);
        if verdict == Verdict::Ignored {
            self.ignored += 1;
        } else {
            self.passed += 1;
        }
        verdict
    }

    /// The queue the path shows: the smallest RTT sample over this round and
    /// the last, less the smallest sample seen.
    pub(crate) fn queue(&self) -> Option<Duration> {
        let recent = match (self.round_min, self.last_round_min) {
            (Some(a), Some(b)) => Some(a.min(b)),
            (a, b) => a.or(b),
        };
        Some(recent?.saturating_sub(self.min_rtt?))
    }

    /// The smoothed share of bytes lost per round trip.
    #[cfg(test)]
    pub(crate) fn loss_share(&self) -> f64 {
        self.share
    }

    fn judge(&self, persistent: bool) -> Verdict {
        if persistent {
            return Verdict::Persistent;
        }
        if self.share >= LOSS_SHARE_CEILING {
            return Verdict::LossShare;
        }
        let (Some(queue), Some(min_rtt)) = (self.queue(), self.min_rtt) else {
            return Verdict::NoSample;
        };
        if queue >= queue_threshold(min_rtt) {
            Verdict::Queue
        } else {
            Verdict::Ignored
        }
    }

    /// Start a new round once a smoothed RTT has passed since this one began.
    fn roll(&mut self, now: Instant) {
        let round = self.srtt.unwrap_or(DEFAULT_ROUND);
        if let Some(start) = self.round_start {
            if now.saturating_duration_since(start) < round {
                return;
            }
        }
        let total = self.round_acked + self.round_lost;
        if total >= SHARE_MIN_BYTES {
            let share = self.round_lost as f64 / total as f64;
            self.share = (1.0 - SHARE_GAIN) * self.share + SHARE_GAIN * share;
        }
        self.round_start = Some(now);
        self.round_acked = 0;
        self.round_lost = 0;
        if self.round_min.is_some() {
            self.last_round_min = self.round_min.take();
        }
    }
}

/// A controller that passes losses to `inner` only through the gate (see the
/// module docs). Everything else is `inner`'s.
pub(crate) struct LossGate {
    inner: Box<dyn Controller>,
    pub(crate) state: GateState,
}

impl LossGate {
    pub(crate) fn new(inner: Box<dyn Controller>) -> Self {
        Self {
            inner,
            state: GateState::default(),
        }
    }

    /// Whether the controller inside is a `C`.
    #[cfg(test)]
    pub(crate) fn wraps<C: Controller + 'static>(&self) -> bool {
        let inner: &dyn Any = &*self.inner.clone_box().into_any();
        inner.is::<C>()
    }
}

impl Controller for LossGate {
    fn on_sent(&mut self, now: Instant, bytes: u64, last_packet_number: u64) {
        self.inner.on_sent(now, bytes, last_packet_number);
    }

    fn on_ack(
        &mut self,
        now: Instant,
        sent: Instant,
        bytes: u64,
        app_limited: bool,
        rtt: &quinn_proto::RttEstimator,
    ) {
        self.state.on_ack(now, sent, bytes, rtt.get());
        self.inner.on_ack(now, sent, bytes, app_limited, rtt);
    }

    fn on_end_acks(
        &mut self,
        now: Instant,
        in_flight: u64,
        app_limited: bool,
        largest_packet_num_acked: Option<u64>,
    ) {
        self.inner
            .on_end_acks(now, in_flight, app_limited, largest_packet_num_acked);
    }

    fn on_congestion_event(
        &mut self,
        now: Instant,
        sent: Instant,
        is_persistent_congestion: bool,
        lost_bytes: u64,
    ) {
        if self
            .state
            .on_loss(now, lost_bytes, is_persistent_congestion)
            != Verdict::Ignored
        {
            self.inner
                .on_congestion_event(now, sent, is_persistent_congestion, lost_bytes);
        }
    }

    fn on_mtu_update(&mut self, new_mtu: u16) {
        self.inner.on_mtu_update(new_mtu);
    }

    fn window(&self) -> u64 {
        self.inner.window()
    }

    fn metrics(&self) -> ControllerMetrics {
        self.inner.metrics()
    }

    fn clone_box(&self) -> Box<dyn Controller> {
        Box::new(Self {
            inner: self.inner.clone_box(),
            state: self.state.clone(),
        })
    }

    fn initial_window(&self) -> u64 {
        self.inner.initial_window()
    }

    fn into_any(self: Box<Self>) -> Box<dyn Any> {
        self
    }
}

#[cfg(test)]
mod tests;
