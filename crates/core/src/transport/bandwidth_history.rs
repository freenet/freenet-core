//! The last minute of upload and download rates, for the local dashboard's
//! bandwidth chart (#5152).
//!
//! The node's transfer counters ([`TRANSPORT_METRICS`]) are cumulative totals
//! with no history, so a rate chart needs something to sample them. A
//! background task reads both counters once a second and stores the rate over
//! each interval in a fixed-size buffer. Rates are computed once, when the
//! sample is taken, so a dashboard request only copies at most
//! [`MAX_SAMPLES`] small values and does no work that grows with uptime.
//!
//! What the rates measure is exactly what the counters measure: upload is
//! every byte a successful `send_to` put on the wire, download is bytes from
//! packets that authenticated against an established session (see the
//! comment on the counters in `metrics.rs`). UDP and IP headers are not
//! counted in either direction.
//!
//! The approach and the edge cases handled here (counter resets, backward
//! clocks, gaps longer than the window, bursts of samples) follow the
//! browser-side chart in #5218 by @Je3r.

use std::collections::VecDeque;
use std::sync::LazyLock;
use std::time::Duration;

use parking_lot::Mutex;

use super::metrics::TRANSPORT_METRICS;
use crate::node::background_task_monitor::BackgroundTaskMonitor;
use crate::simulation::{RealTime, TimeSource};

/// How far back the chart reaches.
pub(crate) const WINDOW: Duration = Duration::from_secs(60);

/// How often the sampler reads the counters.
const SAMPLE_INTERVAL: Duration = Duration::from_secs(1);

/// Samples closer together than this are folded into the next one rather
/// than recorded. Slightly under [`SAMPLE_INTERVAL`] so ordinary timer jitter
/// does not drop every other tick, while two samplers sharing the
/// process-global counters (several nodes in one test process) still record
/// about one sample a second between them instead of two.
const MIN_SAMPLE_SPACING: Duration = Duration::from_millis(900);

/// Hard cap on stored samples: one per [`MIN_SAMPLE_SPACING`] across the
/// window, plus one for the window's inclusive edge.
pub(crate) const MAX_SAMPLES: usize =
    (WINDOW.as_millis() / MIN_SAMPLE_SPACING.as_millis()) as usize + 1;

/// The process-wide history the dashboard reads.
pub(crate) static BANDWIDTH_HISTORY: LazyLock<SharedBandwidthHistory> =
    LazyLock::new(SharedBandwidthHistory::new);

/// The rate over the interval ending at `at`.
#[derive(Debug, Clone, Copy, PartialEq)]
struct Sample {
    at: Duration,
    up_bps: f64,
    down_bps: f64,
}

/// One point for the chart: the rate over the interval that ended `age`
/// before the dashboard asked.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct RateSample {
    pub age: Duration,
    pub up_bytes_per_sec: f64,
    pub down_bytes_per_sec: f64,
}

/// The counter readings the next sample is measured from.
#[derive(Debug, Clone, Copy)]
struct Baseline {
    at: Duration,
    sent: u64,
    received: u64,
}

/// Recent rates derived from two cumulative counters. Holds at most
/// [`MAX_SAMPLES`] samples, none older than [`WINDOW`].
#[derive(Debug, Default)]
pub(crate) struct BandwidthHistory {
    baseline: Option<Baseline>,
    samples: VecDeque<Sample>,
}

/// [`BandwidthHistory`] behind a lock, with the clock both the sampler and
/// the dashboard read so their timestamps share one epoch.
pub(crate) struct SharedBandwidthHistory {
    clock: RealTime,
    history: Mutex<BandwidthHistory>,
}

impl BandwidthHistory {
    /// Record the counter totals read at `now`.
    ///
    /// A sample is the average rate since the previous accepted reading, so
    /// bytes sent between two closely spaced readings are never lost: the
    /// early reading is skipped without moving the baseline, and the next
    /// accepted one covers the whole interval.
    pub(crate) fn record(&mut self, now: Duration, sent: u64, received: u64) {
        let Some(base) = self.baseline else {
            self.restart(now, sent, received);
            return;
        };
        let Some(elapsed) = now.checked_sub(base.at) else {
            // The clock went backwards: no interval to measure.
            self.restart(now, sent, received);
            return;
        };
        if sent < base.sent || received < base.received {
            // The counters never decrease in a running process. If they ever
            // do, the difference is not traffic, so start a fresh series
            // rather than plot a huge or negative rate.
            self.restart(now, sent, received);
            return;
        }
        if elapsed > WINDOW {
            // The sampler was stalled for longer than the chart shows. The
            // bytes from that gap cannot be placed in time, and every stored
            // point is now out of the window anyway.
            self.restart(now, sent, received);
            return;
        }
        if elapsed < MIN_SAMPLE_SPACING {
            return;
        }
        let secs = elapsed.as_secs_f64();
        self.samples.push_back(Sample {
            at: now,
            up_bps: (sent - base.sent) as f64 / secs,
            down_bps: (received - base.received) as f64 / secs,
        });
        self.baseline = Some(Baseline {
            at: now,
            sent,
            received,
        });
        while self
            .samples
            .front()
            .is_some_and(|s| now.saturating_sub(s.at) > WINDOW)
            || self.samples.len() > MAX_SAMPLES
        {
            self.samples.pop_front();
        }
    }

    /// The samples from the last [`WINDOW`] before `now`, oldest first.
    ///
    /// Filtering on `now` rather than on the newest sample means a stopped
    /// sampler shows as an emptying chart instead of a minute of old traffic
    /// presented as current.
    pub(crate) fn recent(&self, now: Duration) -> Vec<RateSample> {
        self.samples
            .iter()
            .filter_map(|s| {
                let age = now.checked_sub(s.at)?;
                (age <= WINDOW).then_some(RateSample {
                    age,
                    up_bytes_per_sec: s.up_bps,
                    down_bytes_per_sec: s.down_bps,
                })
            })
            .collect()
    }

    fn restart(&mut self, now: Duration, sent: u64, received: u64) {
        self.samples.clear();
        self.baseline = Some(Baseline {
            at: now,
            sent,
            received,
        });
    }
}

impl SharedBandwidthHistory {
    fn new() -> Self {
        Self {
            clock: RealTime::new(),
            history: Mutex::new(BandwidthHistory::default()),
        }
    }

    /// Read the transfer counters and record them.
    fn sample(&self) {
        let sent = TRANSPORT_METRICS.cumulative_bytes_sent();
        let received = TRANSPORT_METRICS.cumulative_bytes_received();
        let now = self.clock.now();
        self.history.lock().record(now, sent, received);
    }

    /// The last minute of rates, oldest first. At most [`MAX_SAMPLES`] items.
    pub(crate) fn recent(&self) -> Vec<RateSample> {
        let now = self.clock.now();
        self.history.lock().recent(now)
    }
}

/// Spawn the 1 Hz sampler and register it with the `BackgroundTaskMonitor`.
/// It only loads two atomics and takes an uncontended lock per tick.
pub(crate) fn spawn_sampler(monitor: &BackgroundTaskMonitor) {
    let handle = tokio::spawn(async move {
        let mut ticker = tokio::time::interval(SAMPLE_INTERVAL);
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        loop {
            ticker.tick().await;
            BANDWIDTH_HISTORY.sample();
        }
    });
    monitor.register("bandwidth_history_sampler", handle);
}

#[cfg(test)]
mod tests {
    use super::*;

    fn secs(s: f64) -> Duration {
        Duration::from_secs_f64(s)
    }

    #[test]
    fn first_reading_is_only_a_baseline() {
        let mut h = BandwidthHistory::default();
        h.record(secs(10.0), 1_000, 2_000);
        assert!(h.recent(secs(10.0)).is_empty());
    }

    #[test]
    fn rate_is_the_counter_difference_over_elapsed_time() {
        let mut h = BandwidthHistory::default();
        h.record(secs(10.0), 1_000, 2_000);
        h.record(secs(12.0), 5_000, 2_500);
        let recent = h.recent(secs(12.0));
        assert_eq!(recent.len(), 1);
        assert_eq!(recent[0].up_bytes_per_sec, 2_000.0);
        assert_eq!(recent[0].down_bytes_per_sec, 250.0);
        assert_eq!(recent[0].age, Duration::ZERO);
    }

    #[test]
    fn close_readings_fold_into_the_next_sample_without_losing_bytes() {
        let mut h = BandwidthHistory::default();
        h.record(secs(0.0), 0, 0);
        h.record(secs(0.5), 400, 0); // too soon: skipped, baseline kept
        h.record(secs(1.0), 1_000, 0);
        let recent = h.recent(secs(1.0));
        assert_eq!(recent.len(), 1);
        assert_eq!(recent[0].up_bytes_per_sec, 1_000.0);
    }

    #[test]
    fn decreasing_counter_restarts_instead_of_plotting_a_negative_rate() {
        let mut h = BandwidthHistory::default();
        h.record(secs(0.0), 1_000, 1_000);
        h.record(secs(1.0), 2_000, 2_000);
        h.record(secs(2.0), 10, 2_500);
        assert!(h.recent(secs(2.0)).is_empty());
        h.record(secs(3.0), 110, 2_600);
        let recent = h.recent(secs(3.0));
        assert_eq!(recent.len(), 1);
        assert_eq!(recent[0].up_bytes_per_sec, 100.0);
        assert_eq!(recent[0].down_bytes_per_sec, 100.0);
    }

    #[test]
    fn backward_clock_restarts() {
        let mut h = BandwidthHistory::default();
        h.record(secs(10.0), 0, 0);
        h.record(secs(11.0), 100, 100);
        h.record(secs(5.0), 200, 200);
        assert!(h.recent(secs(5.0)).is_empty());
    }

    #[test]
    fn gap_longer_than_the_window_is_not_stamped_as_current() {
        let mut h = BandwidthHistory::default();
        h.record(secs(0.0), 0, 0);
        h.record(secs(1.0), 100, 100);
        // 90 s of traffic that cannot be placed in the visible minute.
        h.record(secs(91.0), 9_000_100, 100);
        assert!(h.recent(secs(91.0)).is_empty());
        h.record(secs(92.0), 9_000_200, 100);
        let recent = h.recent(secs(92.0));
        assert_eq!(recent.len(), 1);
        assert_eq!(recent[0].up_bytes_per_sec, 100.0);
    }

    #[test]
    fn history_is_bounded_by_count_and_by_window() {
        let mut h = BandwidthHistory::default();
        // A fast sampler: one reading every MIN_SAMPLE_SPACING for ten minutes.
        let step = MIN_SAMPLE_SPACING.as_secs_f64();
        let mut t = 0.0;
        let mut total = 0u64;
        while t < 600.0 {
            h.record(secs(t), total, total);
            total += 1_000;
            t += step;
        }
        assert!(h.samples.len() <= MAX_SAMPLES);
        let now = secs(t - step);
        let recent = h.recent(now);
        assert!(!recent.is_empty());
        assert!(recent.iter().all(|s| s.age <= WINDOW));
    }

    #[test]
    fn recent_drops_samples_that_aged_out_while_the_sampler_was_stopped() {
        let mut h = BandwidthHistory::default();
        h.record(secs(0.0), 0, 0);
        h.record(secs(1.0), 100, 100);
        h.record(secs(2.0), 200, 200);
        assert_eq!(h.recent(secs(60.5)).len(), 2);
        assert_eq!(h.recent(secs(61.5)).len(), 1);
        assert!(h.recent(secs(70.0)).is_empty());
    }

    #[test]
    fn idle_node_records_zero_rates() {
        let mut h = BandwidthHistory::default();
        h.record(secs(0.0), 500, 500);
        h.record(secs(1.0), 500, 500);
        let recent = h.recent(secs(1.0));
        assert_eq!(recent.len(), 1);
        assert_eq!(recent[0].up_bytes_per_sec, 0.0);
        assert_eq!(recent[0].down_bytes_per_sec, 0.0);
    }
}
