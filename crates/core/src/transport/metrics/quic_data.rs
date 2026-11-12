//! Telemetry of QUIC data paths (#5940, e5b-8).
//!
//! Node-wide counts of what the paths' data sends and receives did, gathered from
//! the per-path counters (`QuicPathStats`) where each is recorded, so a count is
//! made once, at the decision. Exported per period in `transport_snapshot`, each
//! count only when non-zero and the gauge always. A family is the keys of one
//! label list under one prefix; no other key starts with a family's prefix, or
//! with the prefix its sibling families share (`quic_data_`), so summing either
//! never adds two kinds of count:
//!
//! | Key | What |
//! |---|---|
//! | `quic_data_send_<kind>_<result>` | data sends by kind and how they ended |
//! | `quic_data_event_<event>` | short overflow to UDP, refusals, stall closes, lost UDP sends, ack waits released at their bound |
//! | `quic_data_stop_<code>` | stops the peer sent to data sends, by code |
//! | `quic_inbound_<outcome>` | every `QuicInboundOutcome`, node-wide |
//! | `quic_enabled_data_paths` | paths where both sides set the data bit |
//! | `quic_live_data_paths` | paths carrying data now (a gauge, not reset, sent every period, 0 included); a path the peer refused data on is not one |
//! | `quic_sends`, `quic_open_uni_waited` | streams a path tried to open (data and control), and those whose open had to wait for a stream credit |
//! | `quic_streamed_sends`, `quic_stream_permit_waited` | streamed-send permits taken, and those that waited |
//! | `quic_bytes_sent`, `quic_bytes_received` | UDP payload bytes of QUIC paths, from quinn's counts |
//! | `quic_session_ended_with_data_path_<age>` | sessions that ended under a data path, by the path's age |
//! | `quic_sent_packets`, `quic_lost_packets`, `quic_lost_bytes`, `quic_congestion_events` | quinn's per-path counts (`PathStats`), shadow paths included, summed over the node's paths up to each path's last tick |
//! | `quic_loss_rate_<bucket>` | live paths that have sent at least 200 packets since they started, by their own lost/sent over that time (a gauge, not reset, every bucket sent every period, 0 included): `zero`, `lt_0_5pct`, `lt_1pct`, `lt_2pct`, `lt_5pct`, `lt_10pct`, `lt_20pct`, `ge_20pct` |
//! | `quic_ended_loss_rate_<bucket>` | paths that ended in the period, by the bucket they were in at the end (same buckets) |
//! | `quic_ended_paths_below_floor` | paths that ended in the period before sending 200 packets, so in no bucket |
//! | `quic_streams_sampled_paths`, `quic_streams_open_ge_<n>`, `quic_streams_peak_max` | per path and period, the peak of inbound streams read at once: the paths sampled, those whose peak reached each level, and the highest peak |
//! | `quic_gate_loss_<verdict>` | congestion events by the loss gate's verdict (`recycle::loss_gate`): passed to Cubic as `queue`, `share` (loss share above the ceiling), `persistent`, `ecn` (a congestion mark), `no_sample`, or `ignored` as random loss; summed over the node's paths up to each path's last tick. Over a path's life they sum to its `quic_congestion_events`; within one period they can differ by the events between the two reads of a tick |
//! | `quic_gate_ended_queue_<bucket>` | paths that ended in the period after at least 20 gate verdicts, by the share of them passed as `queue`: `lt_10pct`, `lt_50pct`, `ge_50pct`. A path whose losses mostly read as a queue runs as plain Cubic, whether the queue is real or jitter |
//! | `quic_send_resets`, `quic_path_ms` | sends a path reset on this side, and the time QUIC paths were up, shadow paths included, up to each path's last tick: resets per connection-hour is their ratio |
//! | `quic_budget_bytes`, `quic_budget_charged_bytes`, `quic_budget_charged_peak_bytes` | the node's QUIC buffer budget, what its data paths' windows have grown above their floor grants now (the floor grants are each path's fixed charge, not the budget's, so a node with every path at the floor reports 0 and uploads nothing; gauges, not reset, uploaded when not 0), and the most charged at once in the period (`quic::budget`) |
//! | `quic_staged_peak_pct` | the most any connection staged in the period (bytes read for streams whose header its reader had not taken yet) as a percentage of its staging cap (`quic::path::inbound::ConnStaging`); streams stopped at the cap are `quic_inbound_evicted_staged` |
//! | `quic_window_grew`, `quic_window_grow_refused`, `quic_window_shrank` | window-limited data paths whose receive window grew, those that found no room, and paths whose windows the budget lowered |
//! | `quic_send_resets_reached_<n>`, `quic_send_resets_path_max` | paths whose lifetime reset count reached each level in the period, and the highest count a reset in the period took a path to: how close paths get to `recycle::SEND_ABORT_LIMIT` |
//!
//! What the loss rate is: quinn's lost over sent (quinn-proto 0.11.19). `sent`
//! counts every packet quinn built, acknowledgement-only ones included. `lost`
//! counts quinn's loss declarations (packet or time threshold), and a packet
//! declared lost that is later acknowledged is not taken back, so spurious
//! declarations count as losses. Lost PLPMTUD probes are left out of `lost` while
//! sent probes are in `sent`; MTU discovery is off (`quic::config`), so no path
//! sends any. Packets still in flight when a connection closes are dropped
//! by quinn, not declared lost, so a path's final bucket can understate its
//! loss at the moment it ended. It is quinn's view of loss, not a measurement
//! of the link. A shadow path's packets are mostly keepalives and
//! acknowledgements, so its rate is of small packets and may not hold for
//! data-sized ones. After a contained panic in a path's acknowledgement
//! watch the path stops ticking, so its counts from then on arrive in one
//! report when it ends.
//!
//! The waits are the step-3 stream-limit bars: `quic_open_uni_waited` over
//! `quic_sends` and `quic_stream_permit_waited` over `quic_streamed_sends` below 0.5%, and
//! `quic_streams_open_ge_48` below 0.2% of `quic_streams_sampled_paths`. Both
//! wait counts are of attempts: a send that waits and then times out is counted.
//!
//! The reset counts are for the per-connection fixed charge
//! (`quic::config::QUIC_CONNECTION_CHARGE_BYTES`): each reset of a stream
//! whose `stopped()` was polled leaves an entry in quinn's `stopped` map until
//! the connection closes, so the charge takes the limit's worst case
//! (`SEND_ABORT_LIMIT` x `QUIC_STOPPED_ENTRY_BYTES`). Field rates and peaks well
//! under the limit are what would justify a lower charge or limit. A path whose
//! acknowledgement watch is off or panicked stops adding to `quic_path_ms`,
//! while its resets are still counted.

use std::sync::atomic::{AtomicU64, Ordering};

use parking_lot::Mutex;

use super::QuicPathAge;
use crate::transport::quic::path::{
    QuicInboundOutcome, QuicSendEvent, QuicSendResult, SendKind, stop_code,
};
use crate::transport::quic::recycle::loss_gate::{GATE_VERDICTS, VERDICT_LABELS, Verdict};

/// The gauge's key, uploaded in every snapshot ([`QuicDataSnapshot::upload_keys`]).
const LIVE_DATA_PATHS: &str = "quic_live_data_paths";

/// The loss-rate gauges' prefix; every bucket is uploaded in every snapshot.
const LOSS_RATE_PREFIX: &str = "quic_loss_rate_";

/// A path's loss rate is bucketed once it has sent this many packets since it
/// started; below it the rate says too little.
pub(crate) const LOSS_RATE_MIN_SENT: u64 = 200;

/// The upper bounds of the loss-rate buckets after `zero`, in lost packets per
/// 1000 sent; the last bucket is `ge_20pct`.
const LOSS_RATE_BOUNDS_PER_MILLE: [u64; 6] = [5, 10, 20, 50, 100, 200];

/// The loss-rate buckets' labels (`quic_loss_rate_<label>`), in order.
pub(crate) const LOSS_RATE_LABELS: [&str; 8] = [
    "zero",
    "lt_0_5pct",
    "lt_1pct",
    "lt_2pct",
    "lt_5pct",
    "lt_10pct",
    "lt_20pct",
    "ge_20pct",
];

/// The bucket of `lost` packets out of `sent`. Losses found in a window can
/// be of packets sent before it, so `lost` may pass `sent`: `ge_20pct`.
fn loss_rate_bucket(lost: u64, sent: u64) -> usize {
    if lost == 0 {
        return 0;
    }
    let per_mille_times_sent = lost.saturating_mul(1000);
    1 + LOSS_RATE_BOUNDS_PER_MILLE
        .iter()
        .take_while(|bound| per_mille_times_sent >= sent.saturating_mul(**bound))
        .count()
}

/// A path enters a `quic_gate_ended_queue_` bucket only after this many gate
/// verdicts; below it the share says too little.
pub(crate) const GATE_MIN_VERDICTS: u64 = 20;

/// The labels of the gate's per-path buckets
/// (`quic_gate_ended_queue_<label>`), by the share of verdicts passed as a
/// queue: under 10%, under 50%, 50% or more.
pub(crate) const GATE_QUEUE_LABELS: [&str; 3] = ["lt_10pct", "lt_50pct", "ge_50pct"];

/// The bucket of a path that had `queue` of `total` verdicts passed as a queue.
fn gate_queue_bucket(queue: u64, total: u64) -> usize {
    if queue.saturating_mul(10) < total {
        0
    } else if queue.saturating_mul(2) < total {
        1
    } else {
        2
    }
}

/// One path's loss-gate verdicts since it started. Owned by the path's report
/// (`recycle::PathReport`); dropping it, when the path ends, counts the path in
/// its `quic_gate_ended_queue_` bucket.
#[derive(Debug)]
pub(crate) struct PathGateWindow {
    node: &'static QuicDataCounters,
    verdicts: [u64; GATE_VERDICTS],
}

impl PathGateWindow {
    pub(crate) fn new(node: &'static QuicDataCounters) -> Self {
        Self {
            node,
            verdicts: [0; GATE_VERDICTS],
        }
    }
}

impl Drop for PathGateWindow {
    fn drop(&mut self) {
        let total: u64 = self.verdicts.iter().sum();
        if total >= GATE_MIN_VERDICTS {
            let bucket = gate_queue_bucket(self.verdicts[GATE_QUEUE], total);
            self.node.gate_ended_queue[bucket].fetch_add(1, Ordering::Relaxed);
        }
    }
}

/// The index of `queue` among the gate's verdicts (`loss_gate::Verdict::ALL`).
const GATE_QUEUE: usize = 0;
const _: () = assert!(matches!(Verdict::ALL[GATE_QUEUE], Verdict::Queue));

/// One path's packets since it started, and the loss-rate bucket it is counted
/// in. Owned by the path's acknowledgement watch; dropping it, however the watch
/// ends, takes the path out of its bucket.
#[derive(Debug)]
pub(crate) struct PathLossWindow {
    node: &'static QuicDataCounters,
    sent: u64,
    lost: u64,
    bucket: Option<usize>,
}

impl PathLossWindow {
    /// A window for a new path, counted in `node`.
    pub(crate) fn new(node: &'static QuicDataCounters) -> Self {
        Self {
            node,
            sent: 0,
            lost: 0,
            bucket: None,
        }
    }
}

impl PathLossWindow {
    /// Whether the path is in a loss-rate bucket. Tests only.
    #[cfg(test)]
    pub(crate) fn is_bucketed(&self) -> bool {
        self.bucket.is_some()
    }
}

impl Drop for PathLossWindow {
    /// The path ended: out of its bucket, and counted among the period's ended
    /// paths by that bucket, so paths that fail lossy still show; a path that
    /// never reached the floor is counted as ended below it.
    fn drop(&mut self) {
        let mut counts = self.node.loss.lock();
        match self.bucket.take() {
            Some(bucket) => {
                counts.rates[bucket] = counts.rates[bucket].saturating_sub(1);
                counts.ended[bucket] = counts.ended[bucket].saturating_add(1);
            }
            None => {
                counts.ended_below_floor = counts.ended_below_floor.saturating_add(1);
            }
        }
    }
}

/// The loss sums of the period and the live paths by bucket. Every field is
/// behind the one lock, so a report's sums and its bucket move can only be
/// written together, and a snapshot reads and resets them together: that one
/// critical section, in `record_path_loss` and `snapshot`, is what keeps them
/// in one snapshot.
#[derive(Debug, Clone, Copy, Default)]
struct LossCounts {
    sent_packets: u64,
    lost_packets: u64,
    lost_bytes: u64,
    congestion_events: u64,
    /// A gauge of live paths, not reset by a snapshot.
    rates: [u32; LOSS_RATE_LABELS.len()],
    /// Paths that ended in the period, by their last bucket.
    ended: [u64; LOSS_RATE_LABELS.len()],
    /// Paths that ended in the period before reaching the floor.
    ended_below_floor: u64,
}

/// The levels `quic_streams_open_ge_<n>` reports, up to the streamed-send limit
/// (`quic::config::STREAMED_SEND_LIMIT`), which is the bar.
pub(crate) const QUIC_STREAMS_OPEN_THRESHOLDS: [u32; 4] = [8, 16, 32, 48];

/// The levels `quic_send_resets_reached_<n>` reports, up to
/// `recycle::SEND_ABORT_LIMIT`: one more reset past the last closes the path.
pub(crate) const QUIC_SEND_RESET_LEVELS: [u64; 4] = [64, 512, 2048, 4096];
const _: () = assert!(
    QUIC_SEND_RESET_LEVELS[QUIC_SEND_RESET_LEVELS.len() - 1]
        == crate::transport::quic::recycle::SEND_ABORT_LIMIT,
    "the last level is the reset limit"
);

/// The streams-open counts of one period, under one lock with its period, as the
/// connection-level inbound concurrency counts are.
#[derive(Debug, Default, Clone, Copy)]
struct StreamsOpenCounts {
    paths_sampled: u32,
    ge: [u32; QUIC_STREAMS_OPEN_THRESHOLDS.len()],
    max: u32,
}

/// One path's peak of inbound streams read at once in the current period. Owned
/// by the path's receive task.
#[derive(Debug, Default)]
pub(crate) struct StreamsOpenPeak {
    /// The period the peak belongs to; 0 before the first sample.
    epoch: u64,
    peak: u32,
}

/// The node-wide counts. A field of `TransportMetrics`.
#[derive(Debug)]
pub(crate) struct QuicDataCounters {
    sends: [[AtomicU64; QuicSendResult::COUNT]; SendKind::ALL.len()],
    events: [AtomicU64; QuicSendEvent::COUNT],
    stops: [AtomicU64; stop_code::ALL.len()],
    inbound: [AtomicU64; QuicInboundOutcome::COUNT],
    data_enabled: AtomicU64,
    /// A gauge: paths with a data route now.
    data_paths: AtomicU64,
    sends_opened: AtomicU64,
    open_uni_waited: AtomicU64,
    permit_takes: AtomicU64,
    permit_waited: AtomicU64,
    bytes_sent: AtomicU64,
    bytes_received: AtomicU64,
    session_ended: [AtomicU64; QuicPathAge::COUNT],
    loss: Mutex<LossCounts>,
    streams_epoch: AtomicU64,
    streams: Mutex<StreamsOpenCounts>,
    send_resets: AtomicU64,
    path_ms: AtomicU64,
    send_resets_reached: [AtomicU64; QUIC_SEND_RESET_LEVELS.len()],
    send_resets_path_max: AtomicU64,
    budget_bytes: AtomicU64,
    budget_charged: AtomicU64,
    budget_charged_peak: AtomicU64,
    window_changes: [AtomicU64; 3],
    staged_peak_pct: AtomicU64,
    gate_loss: [AtomicU64; GATE_VERDICTS],
    gate_ended_queue: [AtomicU64; GATE_QUEUE_LABELS.len()],
}

impl QuicDataCounters {
    pub(crate) fn new() -> Self {
        let zero = || AtomicU64::new(0);
        Self {
            sends: std::array::from_fn(|_| std::array::from_fn(|_| zero())),
            events: std::array::from_fn(|_| zero()),
            stops: std::array::from_fn(|_| zero()),
            inbound: std::array::from_fn(|_| zero()),
            data_enabled: zero(),
            data_paths: zero(),
            sends_opened: zero(),
            open_uni_waited: zero(),
            permit_takes: zero(),
            permit_waited: zero(),
            bytes_sent: zero(),
            bytes_received: zero(),
            session_ended: std::array::from_fn(|_| zero()),
            loss: Mutex::new(LossCounts::default()),
            streams_epoch: AtomicU64::new(1),
            streams: Mutex::new(StreamsOpenCounts::default()),
            send_resets: zero(),
            path_ms: zero(),
            send_resets_reached: std::array::from_fn(|_| zero()),
            send_resets_path_max: zero(),
            budget_bytes: zero(),
            budget_charged: zero(),
            budget_charged_peak: zero(),
            window_changes: std::array::from_fn(|_| zero()),
            staged_peak_pct: zero(),
            gate_loss: std::array::from_fn(|_| zero()),
            gate_ended_queue: std::array::from_fn(|_| zero()),
        }
    }

    pub(crate) fn record_send(&self, kind: SendKind, result: QuicSendResult) {
        self.sends[kind as usize][result as usize].fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn record_event(&self, event: QuicSendEvent) {
        self.events[event as usize].fetch_add(1, Ordering::Relaxed);
    }

    /// A stop from the peer; a code this node does not know is not counted here.
    pub(crate) fn record_stop(&self, code: u64) {
        if let Some(index) = stop_code::ALL.iter().position(|c| u64::from(*c) == code) {
            self.stops[index].fetch_add(1, Ordering::Relaxed);
        }
    }

    pub(crate) fn record_inbound(&self, outcome: QuicInboundOutcome) {
        self.inbound[outcome as usize].fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn record_data_enabled(&self) {
        self.data_enabled.fetch_add(1, Ordering::Relaxed);
    }

    /// A path took a data route. Pair with [`Self::data_path_ended`].
    pub(crate) fn data_path_started(&self) {
        self.data_paths.fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn data_path_ended(&self) {
        // A failed update means the gauge was already 0: nothing to undo.
        if self
            .data_paths
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |n| n.checked_sub(1))
            .is_err()
        {
            tracing::debug!("quic_live_data_paths was already 0 when a data path ended");
        }
    }

    /// A stream was opened on a path, data or control; `waited` when the open did
    /// not complete on its first poll, so the peer's stream credit was used up.
    pub(crate) fn record_open(&self, waited: bool) {
        self.sends_opened.fetch_add(1, Ordering::Relaxed);
        if waited {
            self.open_uni_waited.fetch_add(1, Ordering::Relaxed);
        }
    }

    /// A streamed send took a permit; `waited` when none was free at once.
    pub(crate) fn record_permit(&self, waited: bool) {
        self.permit_takes.fetch_add(1, Ordering::Relaxed);
        if waited {
            self.permit_waited.fetch_add(1, Ordering::Relaxed);
        }
    }

    /// UDP payload bytes a path sent and received since its last report.
    pub(crate) fn record_bytes(&self, sent: u64, received: u64) {
        self.bytes_sent.fetch_add(sent, Ordering::Relaxed);
        self.bytes_received.fetch_add(received, Ordering::Relaxed);
    }

    /// A path's packet counts changed by `delta` since its last report. Summed
    /// node-wide for the period, and added to the path's own window, which runs
    /// from the path's start: once it holds [`LOSS_RATE_MIN_SENT`] sent packets
    /// the path is counted in the bucket of its lost/sent, and moved when that
    /// changes, so each live path is in at most one bucket. See the module doc
    /// for what the rate is. The sums and the bucket move are written under the
    /// one lock ([`LossCounts`]).
    pub(crate) fn record_path_loss(
        &self,
        window: &mut PathLossWindow,
        delta: crate::transport::quic::recycle::PathLoss,
    ) {
        debug_assert!(
            std::ptr::eq(window.node, self),
            "a window of these counters"
        );
        let mut counts = self.loss.lock();
        counts.sent_packets = counts.sent_packets.saturating_add(delta.sent_packets);
        counts.lost_packets = counts.lost_packets.saturating_add(delta.lost_packets);
        counts.lost_bytes = counts.lost_bytes.saturating_add(delta.lost_bytes);
        counts.congestion_events = counts
            .congestion_events
            .saturating_add(delta.congestion_events);
        window.sent = window.sent.saturating_add(delta.sent_packets);
        window.lost = window.lost.saturating_add(delta.lost_packets);
        let bucket =
            (window.sent >= LOSS_RATE_MIN_SENT).then(|| loss_rate_bucket(window.lost, window.sent));
        if bucket != window.bucket {
            if let Some(old) = window.bucket {
                counts.rates[old] = counts.rates[old].saturating_sub(1);
            }
            if let Some(new) = bucket {
                counts.rates[new] += 1;
            }
            window.bucket = bucket;
        }
    }

    /// A session ended while its path carried data, `age` after it became active.
    pub(crate) fn record_session_ended_with_data_path(&self, age: std::time::Duration) {
        self.session_ended[QuicPathAge::bucket(age) as usize].fetch_add(1, Ordering::Relaxed);
    }

    /// A path reads `open` inbound streams at once. Counted per path and period as
    /// `TransportMetrics::record_inbound_concurrency` counts connections: a path
    /// is sampled once a period and counted once in each level its peak reaches,
    /// and a sample that is no new peak in a period already counted takes no lock.
    pub(crate) fn record_streams_open(&self, path: &mut StreamsOpenPeak, open: u32) {
        if path.epoch == self.streams_epoch.load(Ordering::Relaxed) && open <= path.peak {
            return;
        }
        let mut counts = self.streams.lock();
        let epoch = self.streams_epoch.load(Ordering::Relaxed);
        if path.epoch != epoch {
            path.epoch = epoch;
            path.peak = 0;
            counts.paths_sampled += 1;
        }
        if open <= path.peak {
            return;
        }
        for (count, &level) in counts.ge.iter_mut().zip(&QUIC_STREAMS_OPEN_THRESHOLDS) {
            if path.peak < level && level <= open {
                *count += 1;
            }
        }
        path.peak = open;
        counts.max = counts.max.max(open);
    }

    /// A path reset a send on this side, its `path_count`th
    /// (`recycle::SendAborts`, while the connection is open).
    pub(crate) fn record_send_reset(&self, path_count: u64) {
        self.send_resets.fetch_add(1, Ordering::Relaxed);
        // A path's count rises by one per reset, so it equals each level once.
        if let Some(i) = QUIC_SEND_RESET_LEVELS
            .iter()
            .position(|level| *level == path_count)
        {
            self.send_resets_reached[i].fetch_add(1, Ordering::Relaxed);
        }
        self.send_resets_path_max
            .fetch_max(path_count, Ordering::Relaxed);
    }

    /// A path's loss gate gave `delta` more verdicts, in the order of
    /// `loss_gate::Verdict::ALL`, since its last report: summed node-wide for
    /// the period and added to the path's window.
    pub(crate) fn record_gate(&self, window: &mut PathGateWindow, delta: [u64; GATE_VERDICTS]) {
        debug_assert!(
            std::ptr::eq(window.node, self),
            "a window of these counters"
        );
        for (i, n) in delta.into_iter().enumerate() {
            if n != 0 {
                self.gate_loss[i].fetch_add(n, Ordering::Relaxed);
                window.verdicts[i] = window.verdicts[i].saturating_add(n);
            }
        }
    }

    /// A path was up for `ms` more milliseconds (`recycle::PathReport`).
    pub(crate) fn record_path_time(&self, ms: u64) {
        self.path_ms.fetch_add(ms, Ordering::Relaxed);
    }

    /// The node's QUIC buffer budget, set when the endpoint is built.
    pub(crate) fn set_budget_bytes(&self, bytes: u64) {
        self.budget_bytes.store(bytes, Ordering::Relaxed);
    }

    /// What the node's data paths are charged against the budget now.
    pub(crate) fn set_budget_charged(&self, bytes: u64) {
        self.budget_charged.store(bytes, Ordering::Relaxed);
        self.budget_charged_peak.fetch_max(bytes, Ordering::Relaxed);
    }

    /// What one evaluation did to a path's windows.
    pub(crate) fn record_window_change(
        &self,
        change: crate::transport::quic::budget::WindowChange,
    ) {
        use crate::transport::quic::budget::WindowChange;
        let index = match change {
            WindowChange::Grew => 0,
            WindowChange::Refused => 1,
            WindowChange::Shrank => 2,
        };
        self.window_changes[index].fetch_add(1, Ordering::Relaxed);
    }

    /// A connection's staged bytes reached `pct` percent of its staging cap.
    pub(crate) fn record_staged_level(&self, pct: u64) {
        self.staged_peak_pct.fetch_max(pct, Ordering::Relaxed);
    }

    /// The period's counts, reset when `reset`. The gauge is read, never reset.
    pub(crate) fn snapshot(&self, reset: bool) -> QuicDataSnapshot {
        let get = |a: &AtomicU64| {
            if reset {
                a.swap(0, Ordering::Relaxed)
            } else {
                a.load(Ordering::Relaxed)
            }
        };
        let loss = {
            let mut counts = self.loss.lock();
            let now = *counts;
            if reset {
                // The sums are of the period; the buckets are a gauge.
                *counts = LossCounts {
                    rates: now.rates,
                    ..LossCounts::default()
                };
            }
            now
        };
        let streams = {
            let mut counts = self.streams.lock();
            if reset {
                self.streams_epoch.fetch_add(1, Ordering::Relaxed);
                std::mem::take(&mut *counts)
            } else {
                *counts
            }
        };
        QuicDataSnapshot {
            sends: self.sends.iter().flatten().map(get).collect(),
            events: self.events.iter().map(get).collect(),
            stops: self.stops.iter().map(get).collect(),
            inbound: self.inbound.iter().map(get).collect(),
            data_enabled: get(&self.data_enabled),
            data_paths: self.data_paths.load(Ordering::Relaxed),
            sends_opened: get(&self.sends_opened),
            open_uni_waited: get(&self.open_uni_waited),
            permit_takes: get(&self.permit_takes),
            permit_waited: get(&self.permit_waited),
            bytes_sent: get(&self.bytes_sent),
            bytes_received: get(&self.bytes_received),
            session_ended_with_data_path: self.session_ended.iter().map(get).collect(),
            sent_packets: loss.sent_packets,
            lost_packets: loss.lost_packets,
            lost_bytes: loss.lost_bytes,
            congestion_events: loss.congestion_events,
            loss_rate: loss.rates.to_vec(),
            ended_loss_rate: loss.ended.to_vec(),
            ended_below_floor: loss.ended_below_floor,
            streams_open_paths_sampled: streams.paths_sampled,
            streams_open_ge: streams.ge.to_vec(),
            streams_open_max: streams.max,
            send_resets: get(&self.send_resets),
            path_ms: get(&self.path_ms),
            send_resets_reached: self.send_resets_reached.iter().map(get).collect(),
            send_resets_path_max: get(&self.send_resets_path_max),
            budget_bytes: self.budget_bytes.load(Ordering::Relaxed),
            budget_charged: self.budget_charged.load(Ordering::Relaxed),
            // The peak of the period starts again from what is charged now.
            budget_charged_peak: if reset {
                self.budget_charged_peak.swap(
                    self.budget_charged.load(Ordering::Relaxed),
                    Ordering::Relaxed,
                )
            } else {
                self.budget_charged_peak.load(Ordering::Relaxed)
            },
            window_grew: get(&self.window_changes[0]),
            window_grow_refused: get(&self.window_changes[1]),
            window_shrank: get(&self.window_changes[2]),
            staged_peak_pct: get(&self.staged_peak_pct),
            gate_loss: self.gate_loss.iter().map(get).collect(),
            gate_ended_queue: self.gate_ended_queue.iter().map(get).collect(),
        }
    }
}

/// The QUIC data counts of one period. Vectors, in the order of their label lists,
/// so a family can pass 32 entries; [`Self::keys`] names every value.
#[derive(Debug, Clone, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[cfg_attr(test, derive(arbitrary::Arbitrary))]
pub struct QuicDataSnapshot {
    /// By `SendKind`, then `QuicSendResult`.
    pub sends: Vec<u64>,
    pub events: Vec<u64>,
    /// In the order of `stop_code::ALL`.
    pub stops: Vec<u64>,
    pub inbound: Vec<u64>,
    pub data_enabled: u64,
    pub data_paths: u64,
    pub sends_opened: u64,
    pub open_uni_waited: u64,
    pub permit_takes: u64,
    pub permit_waited: u64,
    pub bytes_sent: u64,
    pub bytes_received: u64,
    /// By `QuicPathAge`.
    pub session_ended_with_data_path: Vec<u64>,
    pub sent_packets: u64,
    pub lost_packets: u64,
    pub lost_bytes: u64,
    pub congestion_events: u64,
    /// Live paths by loss-rate bucket, in the order of `LOSS_RATE_LABELS`: a
    /// gauge, not reset.
    pub loss_rate: Vec<u32>,
    /// Paths that ended in the period, by their last loss-rate bucket.
    pub ended_loss_rate: Vec<u64>,
    /// Paths that ended in the period before sending
    /// [`LOSS_RATE_MIN_SENT`] packets, so in no bucket.
    pub ended_below_floor: u64,
    pub streams_open_paths_sampled: u32,
    /// By `QUIC_STREAMS_OPEN_THRESHOLDS`.
    pub streams_open_ge: Vec<u32>,
    pub streams_open_max: u32,
    pub send_resets: u64,
    pub path_ms: u64,
    /// By `QUIC_SEND_RESET_LEVELS`.
    pub send_resets_reached: Vec<u64>,
    pub send_resets_path_max: u64,
    /// A gauge, not reset.
    pub budget_bytes: u64,
    /// A gauge, not reset.
    pub budget_charged: u64,
    pub budget_charged_peak: u64,
    pub window_grew: u64,
    pub window_grow_refused: u64,
    pub window_shrank: u64,
    pub staged_peak_pct: u64,
    /// Congestion events by loss-gate verdict, by `loss_gate::Verdict::ALL`.
    /// Defaulted, so JSON from before the gate decodes. This does not help
    /// bincode: the local event log drops a snapshot written by an older
    /// release (see `upload_json`).
    #[serde(default)]
    pub gate_loss: Vec<u64>,
    /// Paths that ended in the period, by `GATE_QUEUE_LABELS`.
    #[serde(default)]
    pub gate_ended_queue: Vec<u64>,
}

impl QuicDataSnapshot {
    /// Every key with its value, in a fixed order. A value with no label (a
    /// vector longer than its list, which only a decoded foreign snapshot has) is
    /// left out.
    pub fn keys(&self) -> Vec<(String, u64)> {
        self.keys_by_family()
            .into_iter()
            .map(|(_, key, value)| (key, value))
            .collect()
    }

    /// [`Self::keys`], each with the prefix of the family it is in, or `None` for
    /// a key of its own. The prefixes the keys are built from, so a test can check
    /// them against every key.
    pub(crate) fn keys_by_family(&self) -> Vec<(Option<&'static str>, String, u64)> {
        // Destructured, so a new field cannot go unexported.
        let Self {
            sends,
            events,
            stops,
            inbound,
            data_enabled,
            data_paths,
            sends_opened,
            open_uni_waited,
            permit_takes,
            permit_waited,
            bytes_sent,
            bytes_received,
            session_ended_with_data_path,
            sent_packets,
            lost_packets,
            lost_bytes,
            congestion_events,
            loss_rate,
            ended_loss_rate,
            ended_below_floor,
            streams_open_paths_sampled,
            streams_open_ge,
            streams_open_max,
            send_resets,
            path_ms,
            send_resets_reached,
            send_resets_path_max,
            budget_bytes,
            budget_charged,
            budget_charged_peak,
            window_grew,
            window_grow_refused,
            window_shrank,
            staged_peak_pct,
            gate_loss,
            gate_ended_queue,
        } = self;
        let mut keys = Vec::new();
        let mut family = |prefix: &'static str, labels: Vec<String>, values: Vec<u64>| {
            keys.extend(
                labels
                    .into_iter()
                    .zip(values)
                    .map(|(label, value)| (Some(prefix), format!("{prefix}{label}"), value)),
            );
        };
        family(
            "quic_data_send_",
            SendKind::ALL
                .iter()
                .flat_map(|kind| {
                    QuicSendResult::ALL
                        .iter()
                        .map(move |result| format!("{}_{}", kind.label(), result.label()))
                })
                .collect(),
            sends.clone(),
        );
        family(
            "quic_data_event_",
            QuicSendEvent::ALL
                .iter()
                .map(|e| e.label().to_string())
                .collect(),
            events.clone(),
        );
        family(
            "quic_data_stop_",
            stop_code::ALL
                .iter()
                .map(|code| stop_code::label(*code).to_string())
                .collect(),
            stops.clone(),
        );
        family(
            "quic_inbound_",
            QuicInboundOutcome::ALL
                .iter()
                .map(|o| o.label().to_string())
                .collect(),
            inbound.clone(),
        );
        family(
            "quic_session_ended_with_data_path_",
            QuicPathAge::ALL
                .iter()
                .map(|a| a.label().to_string())
                .collect(),
            session_ended_with_data_path.clone(),
        );
        family(
            LOSS_RATE_PREFIX,
            LOSS_RATE_LABELS.iter().map(|l| l.to_string()).collect(),
            loss_rate.iter().map(|n| u64::from(*n)).collect(),
        );
        family(
            "quic_ended_loss_rate_",
            LOSS_RATE_LABELS.iter().map(|l| l.to_string()).collect(),
            ended_loss_rate.clone(),
        );
        family(
            "quic_streams_open_ge_",
            QUIC_STREAMS_OPEN_THRESHOLDS
                .iter()
                .map(|level| level.to_string())
                .collect(),
            streams_open_ge.iter().map(|n| u64::from(*n)).collect(),
        );
        family(
            "quic_send_resets_reached_",
            QUIC_SEND_RESET_LEVELS
                .iter()
                .map(|level| level.to_string())
                .collect(),
            send_resets_reached.clone(),
        );
        family(
            "quic_gate_loss_",
            VERDICT_LABELS.iter().map(|l| l.to_string()).collect(),
            gate_loss.clone(),
        );
        family(
            "quic_gate_ended_queue_",
            GATE_QUEUE_LABELS.iter().map(|l| l.to_string()).collect(),
            gate_ended_queue.clone(),
        );
        for (key, value) in [
            ("quic_enabled_data_paths", *data_enabled),
            (LIVE_DATA_PATHS, *data_paths),
            ("quic_sends", *sends_opened),
            ("quic_open_uni_waited", *open_uni_waited),
            ("quic_streamed_sends", *permit_takes),
            ("quic_stream_permit_waited", *permit_waited),
            ("quic_bytes_sent", *bytes_sent),
            ("quic_bytes_received", *bytes_received),
            ("quic_sent_packets", *sent_packets),
            ("quic_lost_packets", *lost_packets),
            ("quic_lost_bytes", *lost_bytes),
            ("quic_congestion_events", *congestion_events),
            ("quic_ended_paths_below_floor", *ended_below_floor),
            (
                "quic_streams_sampled_paths",
                u64::from(*streams_open_paths_sampled),
            ),
            ("quic_streams_peak_max", u64::from(*streams_open_max)),
            ("quic_send_resets", *send_resets),
            ("quic_path_ms", *path_ms),
            ("quic_send_resets_path_max", *send_resets_path_max),
            ("quic_budget_bytes", *budget_bytes),
            ("quic_budget_charged_bytes", *budget_charged),
            ("quic_budget_charged_peak_bytes", *budget_charged_peak),
            ("quic_window_grew", *window_grew),
            ("quic_window_grow_refused", *window_grow_refused),
            ("quic_window_shrank", *window_shrank),
            ("quic_staged_peak_pct", *staged_peak_pct),
        ] {
            keys.push((None, key.to_string(), value));
        }
        keys
    }

    /// The keys the snapshot uploads: every count that is not 0, and the gauges
    /// (`quic_live_data_paths` and every `quic_loss_rate_` bucket) always, so a
    /// gauge of 0 is told apart from one not reported.
    pub fn upload_keys(&self) -> impl Iterator<Item = (String, u64)> {
        self.keys_by_family()
            .into_iter()
            .filter(|(family, key, value)| {
                *value != 0 || key == LIVE_DATA_PATHS || *family == Some(LOSS_RATE_PREFIX)
            })
            .map(|(_, key, value)| (key, value))
    }

    /// Whether the period counted anything. The gauge alone is not activity, as
    /// `quic_paths_active` is not.
    pub(super) fn has_activity(&self) -> bool {
        let QuicDataSnapshot {
            sends,
            events,
            stops,
            inbound,
            data_enabled,
            data_paths: _,
            sends_opened,
            open_uni_waited,
            permit_takes,
            permit_waited,
            bytes_sent,
            bytes_received,
            session_ended_with_data_path,
            sent_packets,
            lost_packets,
            lost_bytes,
            congestion_events,
            loss_rate: _,
            ended_loss_rate,
            ended_below_floor,
            streams_open_paths_sampled,
            streams_open_ge: _,
            streams_open_max: _,
            send_resets,
            path_ms,
            send_resets_reached,
            send_resets_path_max: _,
            budget_bytes: _,
            budget_charged: _,
            budget_charged_peak: _,
            window_grew,
            window_grow_refused,
            window_shrank,
            staged_peak_pct: _,
            gate_loss,
            gate_ended_queue,
        } = self;
        let any = |values: &[u64]| values.iter().any(|n| *n != 0);
        any(sends)
            || any(events)
            || any(stops)
            || any(inbound)
            || any(session_ended_with_data_path)
            || any(ended_loss_rate)
            || any(send_resets_reached)
            || any(gate_loss)
            || any(gate_ended_queue)
            || [
                *data_enabled,
                *sends_opened,
                *open_uni_waited,
                *permit_takes,
                *permit_waited,
                *bytes_sent,
                *bytes_received,
                *sent_packets,
                *lost_packets,
                *lost_bytes,
                *congestion_events,
                *ended_below_floor,
                u64::from(*streams_open_paths_sampled),
                *send_resets,
                *path_ms,
                *window_grew,
                *window_grow_refused,
                *window_shrank,
            ]
            .iter()
            .any(|n| *n != 0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Each family is counted where it is recorded, read back under its own keys
    /// with its own values, and reset by the snapshot, except the gauge.
    #[test]
    fn each_count_reaches_its_key_and_resets() {
        let counters = QuicDataCounters::new();
        for (i, kind) in SendKind::ALL.iter().enumerate() {
            for (j, result) in QuicSendResult::ALL.iter().enumerate() {
                for _ in 0..(i * 10 + j + 1) {
                    counters.record_send(*kind, *result);
                }
            }
        }
        counters.record_event(QuicSendEvent::StalledClosed);
        counters.record_stop(u64::from(stop_code::NOT_ACCEPTING_DATA));
        counters.record_stop(0xff);
        counters.record_inbound(QuicInboundOutcome::ShortDroppedFull);
        counters.record_data_enabled();
        counters.data_path_started();
        counters.data_path_started();
        counters.data_path_ended();
        counters.record_open(false);
        counters.record_open(true);
        counters.record_permit(true);
        counters.record_bytes(100, 200);
        counters.record_session_ended_with_data_path(std::time::Duration::from_secs(90));
        let mut path = StreamsOpenPeak::default();
        counters.record_streams_open(&mut path, 20);
        counters.record_streams_open(&mut path, 10);

        let snap = counters.snapshot(true);
        let keys: std::collections::BTreeMap<_, _> = snap.keys().into_iter().collect();
        assert_eq!(keys["quic_data_send_short_sent"], 1);
        assert_eq!(keys["quic_data_send_large_short_maybe_delivered"], 12);
        assert_eq!(keys["quic_data_send_pipe_dropped"], 35);
        assert_eq!(keys["quic_data_event_stalled_closed"], 1);
        assert_eq!(keys["quic_data_stop_not_accepting_data"], 1);
        assert_eq!(keys["quic_inbound_short_dropped_full"], 1);
        assert_eq!(keys["quic_enabled_data_paths"], 1);
        assert_eq!(keys["quic_live_data_paths"], 1);
        assert_eq!(keys["quic_sends"], 2);
        assert_eq!(keys["quic_open_uni_waited"], 1);
        assert_eq!(keys["quic_streamed_sends"], 1);
        assert_eq!(keys["quic_stream_permit_waited"], 1);
        assert_eq!(keys["quic_bytes_sent"], 100);
        assert_eq!(keys["quic_bytes_received"], 200);
        assert_eq!(keys["quic_session_ended_with_data_path_lt_10m"], 1);
        assert_eq!(keys["quic_streams_sampled_paths"], 1);
        assert_eq!(keys["quic_streams_open_ge_16"], 1);
        assert_eq!(keys["quic_streams_open_ge_32"], 0);
        assert_eq!(keys["quic_streams_peak_max"], 20);
        assert!(snap.has_activity());

        let after = counters.snapshot(true);
        assert_eq!(after.data_paths, 1, "the gauge is not reset");
        assert!(!after.has_activity(), "a gauge alone is not activity");
        assert!(after.upload_keys().all(|(key, value)| {
            key == "quic_live_data_paths" || (key.starts_with("quic_loss_rate_") && value == 0)
        }));
        // A new period counts the path again.
        counters.record_streams_open(&mut path, 5);
        assert_eq!(counters.snapshot(true).streams_open_paths_sampled, 1);
    }

    /// The gate's verdicts reach their keys and reset; a path that ends after
    /// enough verdicts is counted once in the bucket of its share passed as a
    /// queue, and one with too few in none.
    #[test]
    fn gate_verdicts_reach_their_keys_and_ended_paths_their_bucket() {
        let counters: &'static QuicDataCounters = Box::leak(Box::new(QuicDataCounters::new()));
        // queue, share, persistent, ecn, no_sample, ignored.
        let mut cubic_like = PathGateWindow::new(counters);
        counters.record_gate(&mut cubic_like, [12, 0, 0, 0, 0, 4]);
        counters.record_gate(&mut cubic_like, [3, 1, 0, 0, 0, 0]);
        let mut gated = PathGateWindow::new(counters);
        counters.record_gate(&mut gated, [1, 0, 1, 2, 1, 30]);
        let mut middle = PathGateWindow::new(counters);
        counters.record_gate(&mut middle, [5, 0, 0, 0, 0, 15]);
        let mut short = PathGateWindow::new(counters);
        counters.record_gate(&mut short, [19, 0, 0, 0, 0, 0]);
        let snap = counters.snapshot(true);
        assert!(snap.has_activity());
        let keys: std::collections::BTreeMap<_, _> = snap.keys().into_iter().collect();
        assert_eq!(keys["quic_gate_loss_queue"], 40);
        assert_eq!(keys["quic_gate_loss_share"], 1);
        assert_eq!(keys["quic_gate_loss_persistent"], 1);
        assert_eq!(keys["quic_gate_loss_ecn"], 2);
        assert_eq!(keys["quic_gate_loss_no_sample"], 1);
        assert_eq!(keys["quic_gate_loss_ignored"], 49);
        assert!(
            snap.gate_ended_queue.iter().all(|n| *n == 0),
            "no path ended"
        );

        drop((cubic_like, gated, middle, short));
        let ended = counters.snapshot(true);
        let keys: std::collections::BTreeMap<_, _> = ended.keys().into_iter().collect();
        // 1 of 35 (3%), 5 of 20 (25%), 15 of 20 (75%); 19 verdicts is too few.
        assert_eq!(keys["quic_gate_ended_queue_lt_10pct"], 1);
        assert_eq!(keys["quic_gate_ended_queue_lt_50pct"], 1);
        assert_eq!(keys["quic_gate_ended_queue_ge_50pct"], 1);
        assert_eq!(keys["quic_gate_loss_queue"], 0, "reset by the snapshot");
        assert!(ended.has_activity(), "an ended path alone is activity");
        assert!(!counters.snapshot(true).has_activity());
    }

    #[test]
    fn the_gate_bucket_bounds() {
        assert_eq!(gate_queue_bucket(0, 20), 0);
        assert_eq!(gate_queue_bucket(1, 20), 0);
        assert_eq!(gate_queue_bucket(2, 20), 1, "10% is not under 10%");
        assert_eq!(gate_queue_bucket(9, 20), 1);
        assert_eq!(gate_queue_bucket(10, 20), 2, "50% is not under 50%");
        assert_eq!(gate_queue_bucket(20, 20), 2);
    }

    /// The gauge goes out in every snapshot, 0 included, so 0 is told apart from
    /// not reported; a count of 0 does not.
    #[test]
    fn the_gauges_are_uploaded_at_zero_and_counts_are_not() {
        let counters = QuicDataCounters::new();
        let uploaded: Vec<_> = counters.snapshot(true).upload_keys().collect();
        assert_eq!(
            uploaded.len(),
            LOSS_RATE_LABELS.len() + 1,
            "the gauges and nothing else: {uploaded:?}"
        );
        assert!(uploaded.iter().all(|(key, value)| {
            *value == 0 && (key == "quic_live_data_paths" || key.starts_with("quic_loss_rate_"))
        }));
        counters.data_path_started();
        counters.record_open(false);
        let uploaded: std::collections::BTreeMap<_, _> =
            counters.snapshot(true).upload_keys().collect();
        assert_eq!(uploaded.len(), LOSS_RATE_LABELS.len() + 2, "{uploaded:?}");
        assert_eq!(uploaded["quic_live_data_paths"], 1);
        assert_eq!(uploaded["quic_sends"], 1);
    }

    /// The keys with the family each is in, from `keys_by_family`; every key the
    /// rest of the snapshot has, from its serde fields; and the prefix of each of
    /// its fields that is an array, which goes out as a key per label
    /// (`quic_upgrade_<outcome>`).
    #[allow(clippy::type_complexity)]
    fn all_keys() -> (
        Vec<(Option<&'static str>, String)>,
        Vec<String>,
        Vec<String>,
    ) {
        let ours = QuicDataCounters::new()
            .snapshot(false)
            .keys_by_family()
            .into_iter()
            .map(|(family, key, _)| (family, key))
            .collect();
        let json = serde_json::to_value(crate::transport::metrics::TransportSnapshot::default())
            .expect("serializes");
        let map = json.as_object().expect("an object");
        let others = map
            .keys()
            .filter(|key| *key != "quic_data")
            .cloned()
            .collect();
        let arrays = map
            .iter()
            .filter(|(_, value)| value.is_array())
            .map(|(key, _)| format!("{key}_"))
            .collect();
        (ours, others, arrays)
    }

    /// A key's prefixes that end at a `_` and have two words or more: `quic_data_`
    /// and `quic_data_send_` of `quic_data_send_short_sent`.
    fn stems(key: &str) -> impl Iterator<Item = &str> {
        key.match_indices('_')
            .map(move |(at, _)| &key[..=at])
            .skip(1)
    }

    /// Every key is distinct, and none is under a family it is not in: not under
    /// another family's prefix, nor under the prefix sibling families share
    /// (`quic_data_` of the send, event and stop families), nor under one of the
    /// snapshot's arrays (`quic_upgrade_`) or a prefix two of its other keys share
    /// (`quic_paths_`). The families come from the keys and the other keys from
    /// the snapshot, so a new key or family is checked without editing a list.
    #[test]
    fn no_key_is_under_another_family() {
        let (ours, others, arrays) = all_keys();
        let keys: Vec<&str> = ours
            .iter()
            .map(|(_, key)| key.as_str())
            .chain(others.iter().map(String::as_str))
            .collect();
        let distinct: std::collections::BTreeSet<_> = keys.iter().collect();
        assert_eq!(distinct.len(), keys.len(), "a key is used twice");

        let families: std::collections::BTreeSet<&str> =
            ours.iter().filter_map(|(family, _)| *family).collect();
        // A family's prefix, and each prefix it shares with a sibling, with the
        // families under it.
        let mut claimed: std::collections::BTreeMap<&str, Vec<&str>> = Default::default();
        for family in &families {
            claimed.entry(family).or_default().push(family);
            for parent in stems(family).filter(|stem| stem != family) {
                if families
                    .iter()
                    .any(|other| other != family && other.starts_with(parent))
                {
                    claimed.entry(parent).or_default().push(family);
                }
            }
        }
        for (key_family, key) in &ours {
            for (prefix, owners) in &claimed {
                if key.starts_with(prefix) {
                    assert!(
                        key_family.is_some_and(|f| owners.contains(&f)),
                        "{key} is under {prefix}, of {owners:?}"
                    );
                }
            }
        }
        for key in &others {
            for prefix in claimed.keys() {
                assert!(
                    !key.starts_with(prefix),
                    "the snapshot's {key} is under {prefix}"
                );
            }
        }
        // The prefixes two of the snapshot's own keys share.
        let mut shared: std::collections::BTreeMap<&str, u32> = Default::default();
        for key in &others {
            for stem in stems(key) {
                *shared.entry(stem).or_default() += 1;
            }
        }
        let taken = shared
            .iter()
            .filter(|(_, n)| **n >= 2)
            .map(|(stem, _)| *stem)
            .chain(arrays.iter().map(String::as_str));
        for stem in taken {
            for (_, key) in &ours {
                assert!(
                    !key.starts_with(stem),
                    "{key} is under the snapshot's {stem}"
                );
            }
        }
    }

    /// A path's peak counts once in each level it reaches, a level at its exact
    /// value included, and a new period counts it again from 0.
    #[test]
    fn a_peak_counts_once_per_level_and_period() {
        let counters = QuicDataCounters::new();
        let mut rising = StreamsOpenPeak::default();
        for open in [20, 40, 40] {
            counters.record_streams_open(&mut rising, open);
        }
        let snap = counters.snapshot(true);
        assert_eq!(snap.streams_open_paths_sampled, 1);
        assert_eq!(
            snap.streams_open_ge,
            vec![1, 1, 1, 0],
            "8, 16, 32 and not 48"
        );
        assert_eq!(snap.streams_open_max, 40);

        let mut at_16 = StreamsOpenPeak::default();
        counters.record_streams_open(&mut at_16, 16);
        let mut at_48 = StreamsOpenPeak::default();
        counters.record_streams_open(&mut at_48, 48);
        let mut at_7 = StreamsOpenPeak::default();
        counters.record_streams_open(&mut at_7, 7);
        let snap = counters.snapshot(true);
        assert_eq!(snap.streams_open_paths_sampled, 3);
        assert_eq!(snap.streams_open_ge, vec![2, 2, 1, 1]);
        assert_eq!(snap.streams_open_max, 48);

        // After the reset the rising path starts again: 9 is a new peak.
        counters.record_streams_open(&mut rising, 9);
        let snap = counters.snapshot(true);
        assert_eq!(snap.streams_open_paths_sampled, 1);
        assert_eq!(snap.streams_open_ge, vec![1, 0, 0, 0]);
        assert_eq!(snap.streams_open_max, 9);
    }

    fn loss(sent: u64, lost: u64) -> crate::transport::quic::recycle::PathLoss {
        crate::transport::quic::recycle::PathLoss {
            sent_packets: sent,
            lost_packets: lost,
            lost_bytes: lost * 1200,
            congestion_events: u64::from(lost > 0),
        }
    }

    /// Each bucket's bounds: a rate at a bound is in the bucket above it, and
    /// more losses than sends (losses of packets sent earlier) is `ge_20pct`.
    #[test]
    fn loss_rate_buckets_have_these_bounds() {
        for (lost, bucket) in [
            (0, "zero"),
            (1, "lt_0_5pct"),
            (4, "lt_0_5pct"),
            (5, "lt_1pct"),
            (9, "lt_1pct"),
            (10, "lt_2pct"),
            (19, "lt_2pct"),
            (20, "lt_5pct"),
            (49, "lt_5pct"),
            (50, "lt_10pct"),
            (99, "lt_10pct"),
            (100, "lt_20pct"),
            (199, "lt_20pct"),
            (200, "ge_20pct"),
            (5000, "ge_20pct"),
        ] {
            assert_eq!(
                LOSS_RATE_LABELS[loss_rate_bucket(lost, 1000)],
                bucket,
                "{lost}"
            );
        }
    }

    /// The sums take every report and are of the period. A live path is in no
    /// bucket until it has sent 200 packets since it started, then in exactly
    /// one, moved as its rate changes, across periods; a path that ends leaves
    /// its bucket.
    #[test]
    fn a_path_is_in_one_loss_bucket_once_it_has_sent_enough() {
        let counters: &'static QuicDataCounters = Box::leak(Box::new(QuicDataCounters::new()));
        let mut path = PathLossWindow::new(counters);
        let mut quiet = PathLossWindow::new(counters);
        counters.record_path_loss(&mut path, loss(150, 0));
        counters.record_path_loss(&mut quiet, loss(199, 3));
        assert_eq!(counters.snapshot(false).loss_rate.iter().sum::<u32>(), 0);
        counters.record_path_loss(&mut path, loss(50, 0));
        let snap = counters.snapshot(false);
        assert_eq!(
            snap.loss_rate,
            vec![1, 0, 0, 0, 0, 0, 0, 0],
            "200 sent, none lost"
        );
        // 3 lost of 400: under 1%.
        counters.record_path_loss(&mut path, loss(200, 3));
        let snap = counters.snapshot(true);
        assert_eq!(snap.loss_rate, vec![0, 0, 1, 0, 0, 0, 0, 0]);
        assert_eq!(snap.sent_packets, 599);
        assert_eq!(snap.lost_packets, 6);
        assert_eq!(snap.lost_bytes, 6 * 1200);
        assert_eq!(snap.congestion_events, 2);
        // The next period keeps the path's window: 50 more lost of 100 more
        // sent is 53 of 500, under 20%, and the sums start again.
        counters.record_path_loss(&mut path, loss(100, 50));
        let snap = counters.snapshot(true);
        assert_eq!(snap.loss_rate, vec![0, 0, 0, 0, 0, 0, 1, 0]);
        assert_eq!(snap.lost_packets, 50);
        // With no report, the gauge stays and the period is not active.
        let snap = counters.snapshot(true);
        assert_eq!(snap.loss_rate.iter().sum::<u32>(), 1, "a gauge");
        assert!(!snap.has_activity(), "a gauge alone is not activity");
        drop(path);
        drop(quiet);
        assert_eq!(
            counters.snapshot(true).loss_rate.iter().sum::<u32>(),
            0,
            "it ended"
        );
    }

    /// A path that ends leaves the gauge and is counted once among the period's
    /// ended paths by the bucket it was in; one that never reached 200 packets
    /// is counted as ended below the floor. The ended counts are of the period.
    #[test]
    fn a_path_that_ends_is_counted_by_its_last_bucket() {
        let counters: &'static QuicDataCounters = Box::leak(Box::new(QuicDataCounters::new()));
        let mut lossy = PathLossWindow::new(counters);
        let mut short = PathLossWindow::new(counters);
        counters.record_path_loss(&mut lossy, loss(300, 90));
        counters.record_path_loss(&mut short, loss(150, 0));
        assert_eq!(
            counters.snapshot(true).loss_rate,
            vec![0, 0, 0, 0, 0, 0, 0, 1]
        );
        drop(lossy);
        drop(short);
        let snap = counters.snapshot(true);
        assert_eq!(snap.loss_rate.iter().sum::<u32>(), 0);
        assert_eq!(snap.ended_loss_rate, vec![0, 0, 0, 0, 0, 0, 0, 1]);
        assert_eq!(snap.ended_below_floor, 1, "the short path");
        assert!(snap.has_activity(), "an ended path is activity");
        let next = counters.snapshot(true);
        assert_eq!(next.ended_loss_rate.iter().sum::<u64>(), 0);
        assert_eq!(next.ended_below_floor, 0);
        // Ended below the floor is activity on its own.
        drop(PathLossWindow::new(counters));
        assert!(counters.snapshot(true).has_activity());
    }

    /// Every loss-rate gauge is uploaded each period, an emptied bucket as 0,
    /// while a count of 0 (the ended family) is not.
    #[test]
    fn the_loss_rate_gauges_are_uploaded_at_zero() {
        let counters: &'static QuicDataCounters = Box::leak(Box::new(QuicDataCounters::new()));
        let mut path = PathLossWindow::new(counters);
        counters.record_path_loss(&mut path, loss(300, 90));
        drop(path);
        let uploaded: std::collections::HashMap<String, u64> =
            counters.snapshot(true).upload_keys().collect();
        for label in LOSS_RATE_LABELS {
            assert_eq!(
                uploaded.get(&format!("quic_loss_rate_{label}")),
                Some(&0),
                "{label} is uploaded at 0"
            );
        }
        assert_eq!(uploaded.get("quic_ended_loss_rate_ge_20pct"), Some(&1));
        assert!(!uploaded.contains_key("quic_ended_loss_rate_zero"));
        assert!(
            !uploaded.contains_key("quic_ended_paths_below_floor"),
            "a count of 0 is not uploaded"
        );
        // A path that ends below the floor is uploaded under its key.
        drop(PathLossWindow::new(counters));
        let uploaded: std::collections::HashMap<String, u64> =
            counters.snapshot(true).upload_keys().collect();
        assert_eq!(uploaded.get("quic_ended_paths_below_floor"), Some(&1));
    }

    /// Resets reach their keys: the total, each level a path's count passes,
    /// and the highest count; path time sums. All reset by the snapshot.
    #[test]
    fn send_resets_and_path_time_reach_their_keys_and_reset() {
        let counters = QuicDataCounters::new();
        // One path to 600 resets, another to 3.
        for count in 1..=600 {
            counters.record_send_reset(count);
        }
        for count in 1..=3 {
            counters.record_send_reset(count);
        }
        counters.record_path_time(2_000);
        counters.record_path_time(1_500);
        let snap = counters.snapshot(true);
        assert!(snap.has_activity());
        let keys: std::collections::BTreeMap<_, _> = snap.keys().into_iter().collect();
        assert_eq!(keys["quic_send_resets"], 603);
        assert_eq!(keys["quic_path_ms"], 3_500);
        assert_eq!(keys["quic_send_resets_reached_64"], 1);
        assert_eq!(keys["quic_send_resets_reached_512"], 1);
        assert_eq!(keys["quic_send_resets_reached_2048"], 0);
        assert_eq!(keys["quic_send_resets_reached_4096"], 0);
        assert_eq!(keys["quic_send_resets_path_max"], 600);

        let after = counters.snapshot(true);
        assert!(!after.has_activity());
        assert!(after.keys().iter().all(|(_, value)| *value == 0));
    }

    /// The reset and path-time counts are activity on their own, so a period with
    /// only them is uploaded.
    #[test]
    fn resets_or_path_time_alone_are_activity() {
        let counters = QuicDataCounters::new();
        counters.record_send_reset(1);
        assert!(counters.snapshot(true).has_activity(), "a reset");
        counters.record_path_time(1);
        assert!(counters.snapshot(true).has_activity(), "path time");
        let reached = QuicDataSnapshot {
            send_resets_reached: vec![0, 1, 0, 0],
            ..QuicDataSnapshot::default()
        };
        assert!(reached.has_activity(), "a level reached");
        assert!(!counters.snapshot(true).has_activity());
    }

    /// The snapshot round-trips through bincode, as the event log stores it.
    #[test]
    fn snapshot_round_trips_through_bincode() {
        let counters = QuicDataCounters::new();
        counters.record_send(SendKind::Stream, QuicSendResult::FailedUdp);
        counters.record_inbound(QuicInboundOutcome::StreamRefusedBudget);
        let snap = counters.snapshot(true);
        let back: QuicDataSnapshot =
            bincode::deserialize(&bincode::serialize(&snap).unwrap()).unwrap();
        assert_eq!(back, snap);
    }
}
