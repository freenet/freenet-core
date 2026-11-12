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
//! - it is an ECN congestion mark (quinn reports it with no lost bytes;
//!   the node's socket does not read ECN today, so none arrive), or
//! - the queue it estimates is at least [`queue_threshold`]: a tenth of the
//!   minimum RTT ([`QUEUE_FRACTION`]), at least [`QUEUE_FLOOR`], or
//! - the smoothed share of bytes lost per round trip is at least
//!   [`LOSS_SHARE_CEILING`] (a policer, or a path too lossy to push), or
//! - there is no RTT sample yet.
//!
//! Otherwise the loss is ignored: quinn still retransmits the data, and the
//! window stays where it was.
//!
//! The queue is judged at the time the lost packet was sent: the smallest RTT
//! of the packets sent within [`NEAR_WINDOW_FRACTION`] of an RTT of it (at
//! least [`NEAR_WINDOW_MIN`]), less the smallest RTT seen on the connection.
//! A queue that overflowed delayed the packets sent just before and after
//! the one it dropped; a random loss leaves its neighbours with whatever
//! queue there was, usually none. Jitter only adds delay, so the minimum
//! over the neighbours filters it. When no neighbour's RTT is known (a
//! packet sent more than [`SEND_BUCKETS`] buckets ago), the smallest RTT
//! over this round trip and the last one stands in.
//!
//! An earlier version judged every loss by that round minimum alone. A queue
//! that fills and drains each round (one flow on a shallow buffer) has a low
//! minimum, so its overflow losses read as random and the flow kept up to a
//! fifth of what it sent being dropped (e5b-9 lab).
//!
//! # What this does to other traffic
//!
//! On a drop-tail queue deeper than the threshold, an overflow loss comes
//! with the queue full, so it passes: with no random loss the controller is
//! exactly Cubic and fills the buffer as Cubic does. With random loss too,
//! the random losses taken while the queue is below the threshold are
//! ignored, so it holds more of the link than Cubic would. On a queue
//! shallower than the threshold a congestion loss looks random; the
//! loss-share ceiling is then what stops it, and a competing loss-based flow
//! keeps less of the link than next to Cubic.
//!
//! The figures cited here were measured with the `quic_lab_cold` and
//! `quic_lab_burst` benches (`crates/core/benches/transport/quic_lab.rs` on
//! the `lab/quic-cold-start` branch, "e5b-9 lab"); the tables are in PR
//! #6185 and issue #6183, which keep them if the branch goes.
//!
//! # Telemetry
//!
//! Every verdict is counted per connection ([`GateVerdicts`]) and reported
//! node-wide by the path's report as `quic_gate_loss_<verdict>`, with
//! `quic_gate_ended_queue_<bucket>` counting ended paths by the share of
//! their losses passed as a queue (`transport::metrics::quic_data`).
//!
//! The minimum RTT is never forgotten. If a route grows longer the estimate
//! reads the extra delay as a queue and every loss is passed: the controller
//! falls back to plain Cubic, the behaviour before this gate.

use std::any::Any;
use std::collections::VecDeque;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use quinn::congestion::{Controller, ControllerMetrics};

/// The queue delay, as a fraction of the minimum RTT, at which a loss counts
/// as congestion.
pub(crate) const QUEUE_FRACTION: f64 = 0.1;

/// The smallest queue delay threshold, for short paths where a tenth of the
/// RTT is within timer and scheduling noise.
///
/// There is deliberately no largest: a ceiling of 3 ms let the gate see
/// shallow queues on long paths, but 10 ms of jitter then read as a queue and
/// the gate passed random losses, so an 8 MiB message at 5% loss took as long
/// as with plain Cubic (e5b-9 lab).
pub(crate) const QUEUE_FLOOR: Duration = Duration::from_millis(2);

/// The smoothed share of bytes lost per round trip at which every loss
/// counts, queue or not.
pub(crate) const LOSS_SHARE_CEILING: f64 = 0.25;

/// A round trip's loss share enters the smoothed share only when at least
/// this many bytes were acknowledged or lost in it: in a small window one
/// loss is a large share (2 of 8 is 25%). A smaller round's bytes carry into
/// the next round.
const SHARE_MIN_BYTES: u64 = 16 * 1200;

/// The weight of a new round trip's share in the smoothed share.
const SHARE_GAIN: f64 = 0.25;

/// The round length before the first RTT sample.
const DEFAULT_ROUND: Duration = Duration::from_millis(100);

/// A lost packet's neighbours are the packets sent within this fraction of
/// the smoothed RTT of it, either side.
const NEAR_WINDOW_FRACTION: u32 = 4;

/// ... and never less than this.
const NEAR_WINDOW_MIN: Duration = Duration::from_millis(5);

/// RTT samples are kept as the smallest per span of send times, each span an
/// eighth of the smoothed RTT (at least [`BUCKET_MIN`]), for the last this
/// many spans: about four round trips, more than quinn takes to declare a
/// loss (9/8 of an RTT, or a probe timeout).
const SEND_BUCKETS: usize = 32;

/// The shortest span of send times one bucket covers.
const BUCKET_MIN: Duration = Duration::from_millis(1);

/// The queue delay at which a loss counts as congestion on a path whose
/// minimum RTT is `min_rtt`.
pub(crate) fn queue_threshold(min_rtt: Duration) -> Duration {
    min_rtt.mul_f64(QUEUE_FRACTION).max(QUEUE_FLOOR)
}

/// Why a loss was passed to the inner controller, or that it was not.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Verdict {
    /// quinn declared persistent congestion.
    Persistent,
    /// An ECN congestion mark (quinn reports it with no lost bytes): a
    /// router's own congestion signal, never random loss.
    Ecn,
    /// The smoothed loss share is at or above [`LOSS_SHARE_CEILING`].
    LossShare,
    /// The queue estimate is at or above [`queue_threshold`].
    Queue,
    /// No RTT sample yet, so no queue estimate.
    NoSample,
    /// None of the above: the loss is taken as random and ignored.
    Ignored,
}

impl Verdict {
    /// Every verdict, in the order of [`VERDICT_LABELS`].
    pub(crate) const ALL: [Verdict; GATE_VERDICTS] = [
        Verdict::Queue,
        Verdict::LossShare,
        Verdict::Persistent,
        Verdict::Ecn,
        Verdict::NoSample,
        Verdict::Ignored,
    ];

    fn index(self) -> usize {
        match self {
            Verdict::Queue => 0,
            Verdict::LossShare => 1,
            Verdict::Persistent => 2,
            Verdict::Ecn => 3,
            Verdict::NoSample => 4,
            Verdict::Ignored => 5,
        }
    }
}

/// The telemetry labels of the verdicts (`quic_gate_loss_<label>`), in the
/// order of [`Verdict::ALL`].
pub(crate) const VERDICT_LABELS: [&str; GATE_VERDICTS] = [
    "queue",
    "share",
    "persistent",
    "ecn",
    "no_sample",
    "ignored",
];

/// The number of verdicts.
pub(crate) const GATE_VERDICTS: usize = 6;

/// A connection's congestion events by the gate's verdict, since the
/// connection started. Shared by the gate and its clones (quinn's
/// `Connection::congestion_state`), so the path's report reads the live
/// counts (`recycle::PathReport`, `quic_gate_loss_<verdict>`).
#[derive(Debug, Default)]
pub(crate) struct GateVerdicts([AtomicU64; GATE_VERDICTS]);

impl GateVerdicts {
    fn record(&self, verdict: Verdict) {
        self.0[verdict.index()].fetch_add(1, Ordering::Relaxed);
    }

    /// The counts so far, in the order of [`Verdict::ALL`].
    pub(crate) fn read(&self) -> [u64; GATE_VERDICTS] {
        std::array::from_fn(|i| self.0[i].load(Ordering::Relaxed))
    }
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
    /// The smallest RTT of the packets sent in each recent span of send
    /// times: (start, end, smallest RTT), oldest first. In `queue_at` a span
    /// covers up to the next span's start, but at most a smoothed RTT past
    /// its own end. With the quarter-RTT neighbour window that lets a span
    /// judge a lost packet sent up to about 1.25 smoothed RTTs after the
    /// span's end (1.4 after its start), so after a longer idle gap a span
    /// from before the gap does not judge a loss after it. Ending each span at its own
    /// end instead left gaps between spans that a lost packet's neighbours
    /// often fell into: 8 MiB at 5% loss with 30 ms of jitter at 300 ms took
    /// over 15 s in 7 of 46 lab runs, against 2 of 32 with this rule.
    sends: VecDeque<(Instant, Instant, Duration)>,
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
        self.record_send(sent, sample);
    }

    /// A loss of `lost_bytes`, the last of them sent at `sent`; returns
    /// whether it counts as congestion.
    pub(crate) fn on_loss(
        &mut self,
        now: Instant,
        sent: Instant,
        lost_bytes: u64,
        persistent: bool,
    ) -> Verdict {
        self.roll(now);
        self.round_lost += lost_bytes;
        let verdict = self.judge(sent, lost_bytes, persistent);
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

    /// The queue around `sent`: the smallest RTT of the packets sent near it
    /// (see the module docs), less the smallest sample seen. `None` when no
    /// span near it is kept.
    pub(crate) fn queue_at(&self, sent: Instant) -> Option<Duration> {
        let window = (self.srtt? / NEAR_WINDOW_FRACTION).max(NEAR_WINDOW_MIN);
        let (from, to) = (sent.checked_sub(window).unwrap_or(sent), sent + window);
        let mut near: Option<Duration> = None;
        let srtt = self.srtt?;
        for (i, &(start, end, rtt)) in self.sends.iter().enumerate() {
            // A span covers up to the next one, but never more than a
            // smoothed RTT past its own end. With the neighbour window, a
            // loss sent up to about 1.25 smoothed RTTs after the span's end
            // can still be judged by it, so it does not reach across a
            // longer idle gap.
            let reach = end + srtt;
            let end = self
                .sends
                .get(i + 1)
                .map_or(reach, |&(next, _, _)| next.min(reach));
            if start <= to && end > from {
                near = Some(near.map_or(rtt, |m| m.min(rtt)));
            }
        }
        Some(near?.saturating_sub(self.min_rtt?))
    }

    /// Keep `sample` as the RTT of a packet sent at `sent`.
    fn record_send(&mut self, sent: Instant, sample: Duration) {
        let width = self
            .srtt
            .map_or(BUCKET_MIN, |srtt| (srtt / 8).max(BUCKET_MIN));
        match self.sends.back_mut() {
            Some((start, end, min)) if sent >= *start && sent < *end => {
                *min = (*min).min(sample);
            }
            Some((start, _, _)) if sent < *start => {
                // Acknowledged out of order: the span it was sent in, if kept.
                if let Some((_, _, min)) = self
                    .sends
                    .iter_mut()
                    .rev()
                    .find(|(s, e, _)| *s <= sent && sent < *e)
                {
                    *min = (*min).min(sample);
                }
            }
            _ => {
                if self.sends.len() == SEND_BUCKETS {
                    self.sends.pop_front();
                }
                self.sends.push_back((sent, sent + width, sample));
            }
        }
    }

    /// The smoothed share of bytes lost per round trip.
    #[cfg(test)]
    pub(crate) fn loss_share(&self) -> f64 {
        self.share
    }

    fn judge(&self, sent: Instant, lost_bytes: u64, persistent: bool) -> Verdict {
        if persistent {
            return Verdict::Persistent;
        }
        if lost_bytes == 0 {
            return Verdict::Ecn;
        }
        if self.share >= LOSS_SHARE_CEILING {
            return Verdict::LossShare;
        }
        let queue = self.queue_at(sent).or_else(|| self.queue());
        let (Some(queue), Some(min_rtt)) = (queue, self.min_rtt) else {
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
        // Bytes of a round too small to judge carry into the next, so the
        // share keeps moving when the window is small: a share above the
        // ceiling would otherwise never come down once Cubic's window fell
        // below [`SHARE_MIN_BYTES`].
        let total = self.round_acked + self.round_lost;
        if total >= SHARE_MIN_BYTES {
            let share = self.round_lost as f64 / total as f64;
            self.share = (1.0 - SHARE_GAIN) * self.share + SHARE_GAIN * share;
            self.round_acked = 0;
            self.round_lost = 0;
        }
        self.round_start = Some(now);
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
    verdicts: Arc<GateVerdicts>,
}

impl LossGate {
    pub(crate) fn new(inner: Box<dyn Controller>) -> Self {
        Self {
            inner,
            state: GateState::default(),
            verdicts: Arc::default(),
        }
    }

    /// The connection's verdict counts, shared with every clone.
    pub(crate) fn verdicts(&self) -> Arc<GateVerdicts> {
        self.verdicts.clone()
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
        let verdict = self
            .state
            .on_loss(now, sent, lost_bytes, is_persistent_congestion);
        self.verdicts.record(verdict);
        if verdict != Verdict::Ignored {
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
            verdicts: self.verdicts.clone(),
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
