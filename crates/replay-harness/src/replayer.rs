//! Driver that feeds an [`EventStream`] into per-peer rolling stats and
//! ticks a [`Controller`] at a fixed cadence.

use std::collections::BTreeMap;
use std::time::Duration;

use crate::controllers::{Controller, ControllerView, PeerObservation, RateDecision};
use crate::event::{Event, EventStream, PeerKey};
use crate::rolling::RollingRttStats;

/// One row in the controller's decision trace.
#[derive(Debug, Clone)]
pub struct DecisionLog {
    pub at: Duration,
    pub decision: RateDecision,
    /// The aggregate rate AFTER applying the decision. Stable rate across
    /// many consecutive `Hold` decisions; the trace records every tick so
    /// downstream tooling can plot rate-over-time without re-deriving it.
    pub rate_after_bps: u64,
    /// Snapshot of the cross-peer state at the moment of the decision.
    /// Useful for "why did the controller (not) fire?" post-hoc analysis.
    pub n_peers_with_inflation: usize,
    pub shared_inflation: Option<Duration>,
}

/// Aggregate report returned by [`Replayer::run`].
#[derive(Debug, Clone, Default)]
pub struct ReplayReport {
    pub controller: String,
    pub ticks: usize,
    pub decisions_set: usize,
    pub final_rate_bps: u64,
    pub min_rate_bps: u64,
    pub max_rate_bps: u64,
    pub log: Vec<DecisionLog>,
}

impl ReplayReport {
    /// Decisions filtered to `RateDecision::Set` only. Useful for
    /// assertions that check "the controller fired N times during the
    /// contention period".
    pub fn fired(&self) -> Vec<&DecisionLog> {
        self.log
            .iter()
            .filter(|d| matches!(d.decision, RateDecision::Set { .. }))
            .collect()
    }

    /// Were any rate decisions made? Equivalent to `!self.fired().is_empty()`.
    pub fn fired_at_all(&self) -> bool {
        self.decisions_set > 0
    }
}

pub struct Replayer {
    tick_interval: Duration,
    starting_rate_bps: u64,
}

impl Replayer {
    /// New replayer with the standard 1 Hz tick cadence and a default
    /// 10 Mbps × 1 peer starting rate (the production FixedRate baseline).
    pub fn new() -> Self {
        Self {
            tick_interval: Duration::from_secs(1),
            starting_rate_bps: 1_250_000,
        }
    }

    pub fn with_tick_interval(mut self, interval: Duration) -> Self {
        self.tick_interval = interval;
        self
    }

    pub fn with_starting_rate_bps(mut self, rate: u64) -> Self {
        self.starting_rate_bps = rate;
        self
    }

    /// Drive `stream` through `controller`, returning a full decision trace.
    pub fn run<S, C>(&self, stream: S, mut controller: C) -> ReplayReport
    where
        S: EventStream,
        C: Controller,
    {
        // Buffer all events. For synthetic scenarios this is tiny; for
        // OTLP it's still bounded by a single file's worth of events
        // (the binary streams file-by-file). The EventStream contract
        // says events are in non-decreasing `at` order; we sort
        // defensively to harden against a slightly-out-of-order source.
        let mut events: Vec<Event> = collect_stream(stream);
        events.sort_by_key(|e| e.at());

        let end_time = events.last().map(|e| e.at()).unwrap_or(Duration::ZERO);

        let mut per_peer: BTreeMap<PeerKey, RollingRttStats> = BTreeMap::new();
        let mut reference: Option<RollingRttStats> = None;
        let mut log: Vec<DecisionLog> = Vec::new();
        let mut rate = self.starting_rate_bps;
        let mut min_rate = rate;
        let mut max_rate = rate;
        let mut decisions_set = 0usize;

        let mut ev_idx = 0usize;
        let mut now = Duration::ZERO;

        // Tick from t=tick_interval until one tick past the last event,
        // so we capture the final-window state. If there were no events,
        // we tick exactly once at t=tick_interval so the controller sees
        // an empty network.
        let stop_at = end_time + self.tick_interval;
        loop {
            now += self.tick_interval;
            if now > stop_at && ev_idx >= events.len() {
                break;
            }

            // Apply all events at or before this tick BEFORE running the
            // controller, so the controller sees the state as of `now`.
            while ev_idx < events.len() && events[ev_idx].at() <= now {
                apply_event(&events[ev_idx], &mut per_peer, &mut reference);
                ev_idx += 1;
            }

            tick_once(
                &mut controller,
                &per_peer,
                reference.as_ref(),
                now,
                &mut rate,
                &mut min_rate,
                &mut max_rate,
                &mut decisions_set,
                &mut log,
            );
        }

        ReplayReport {
            controller: controller.name().to_string(),
            ticks: log.len(),
            decisions_set,
            final_rate_bps: rate,
            min_rate_bps: min_rate,
            max_rate_bps: max_rate,
            log,
        }
    }
}

impl Default for Replayer {
    fn default() -> Self {
        Self::new()
    }
}

fn collect_stream<S: EventStream>(mut stream: S) -> Vec<Event> {
    let mut out = Vec::new();
    while let Some(ev) = stream.next_event() {
        out.push(ev);
    }
    out
}

fn apply_event(
    ev: &Event,
    per_peer: &mut BTreeMap<PeerKey, RollingRttStats>,
    reference: &mut Option<RollingRttStats>,
) {
    match ev {
        Event::RttSample { peer, at, rtt } => {
            per_peer
                .entry(peer.clone())
                .or_default()
                .record_at(*at, *rtt);
        }
        Event::ReferenceSample { at, rtt } => {
            reference.get_or_insert_default().record_at(*at, *rtt);
        }
        Event::PeerJoin { peer, .. } => {
            per_peer.entry(peer.clone()).or_default();
        }
        Event::PeerLeave { peer, .. } => {
            per_peer.remove(peer);
        }
    }
}

#[allow(clippy::too_many_arguments)]
fn tick_once<C: Controller>(
    controller: &mut C,
    per_peer: &BTreeMap<PeerKey, RollingRttStats>,
    reference: Option<&RollingRttStats>,
    now: Duration,
    rate: &mut u64,
    min_rate: &mut u64,
    max_rate: &mut u64,
    decisions_set: &mut usize,
    log: &mut Vec<DecisionLog>,
) {
    let observations: Vec<PeerObservation> = per_peer
        .iter()
        .filter_map(|(peer, stats)| {
            stats.snapshot_at(now).map(|snap| PeerObservation {
                peer: peer.clone(),
                snapshot: snap,
            })
        })
        .collect();

    let reference_snap = reference.and_then(|r| r.snapshot_at(now));

    let mut infl: Vec<Duration> = observations
        .iter()
        .filter_map(|p| p.snapshot.inflation)
        .collect();
    let n_peers_with_inflation = infl.len();
    let shared_inflation = if n_peers_with_inflation == 0 {
        None
    } else {
        infl.sort_unstable();
        Some(infl[infl.len() / 2])
    };

    let view = ControllerView {
        now,
        per_peer: &observations,
        reference: reference_snap,
        current_aggregate_rate_bps: *rate,
    };
    let decision = controller.tick(view);
    if let RateDecision::Set {
        aggregate_rate_bps, ..
    } = decision
    {
        *rate = aggregate_rate_bps;
        *min_rate = (*min_rate).min(aggregate_rate_bps);
        *max_rate = (*max_rate).max(aggregate_rate_bps);
        *decisions_set += 1;
    }
    log.push(DecisionLog {
        at: now,
        decision,
        rate_after_bps: *rate,
        n_peers_with_inflation,
        shared_inflation,
    });
}
