//! Node-side windowed rollup for the always-on #4074 "shadow" floor-analysis
//! telemetry (`shadow_rtt_aggregate`, `shadow_rate_demand`,
//! `shadow_outbound_class`, `shadow_reference_ping`, `shadow_iface_tx`).
//!
//! ## Why this exists
//!
//! Each shadow aggregator samples a cheap process-global signal once per
//! second and used to emit one OTLP event *per sample*. Every production peer
//! reports to the central collector (nova) by default, so the three always-on
//! streams alone put `3 events/sec × fleet` into the collector — measured at
//! ~56% of all central telemetry volume (the top three event types by count).
//!
//! The per-second samples feed an **offline** distribution analysis (#4074
//! floor bounds), not any real-time controller — nothing reads them back on
//! the production data path. An offline distribution does not need every raw
//! 1 Hz sample transmitted individually: the node can fold a window of samples
//! into one rollup carrying the summary statistics (`mean` / `min` / `max` /
//! `p50`) and emit that once per window. The window's `max`/`p50` preserve the
//! burst peak and the median that the floor analysis actually consumes (`p50`
//! is a more robust central-tendency signal than `mean` for the bursty,
//! skewed byte-rate streams), while the record count drops by
//! [`SHADOW_ROLLUP_WINDOW_SECS`]x on the shadow slice.
//!
//! Backwards compatibility: each rollup keeps every original top-level field
//! name, now carrying the window **mean** in the same unit (a per-second rate
//! stays a per-second rate; a gauge stays that gauge, window-averaged), so any
//! existing consumer that reads the flat field keeps working. The distribution
//! fields (`*_min` / `*_max` / `*_p50`, `window_secs`, `samples`) are purely
//! additive.

/// Number of 1 Hz shadow samples folded into a single rollup emission.
///
/// All shadow aggregators tick at 1 Hz, so this is both the sample count per
/// rollup and the wall-clock window in seconds. Raising it cuts central
/// telemetry volume proportionally (30 → one shadow record per stream per 30 s,
/// i.e. a 30x reduction on the shadow slice) at the cost of coarser temporal
/// resolution; the per-window `min`/`max`/`p50` keep the in-window distribution
/// regardless, so the floor analysis loses no statistical signal — only the
/// ability to place an event to the exact second, which the 2–4 week
/// observation horizon does not need.
///
/// 30 s keeps two rollups per minute (ample for the diurnal/contention
/// patterns the analysis looks for) while capturing essentially the whole
/// ~56% shadow volume prize.
pub(crate) const SHADOW_ROLLUP_WINDOW_SECS: u32 = 30;

/// Accumulates the `u64` samples of one metric over a rollup window and
/// computes summary statistics. Cheap: a `Vec` of at most
/// [`SHADOW_ROLLUP_WINDOW_SECS`] entries, reset after each emission.
///
/// `Option` samples (a metric that is absent on some ticks, e.g.
/// `median_inflation_us` before enough RTT samples exist) are folded via
/// [`WindowedStat::record_opt`], which skips `None` so the stats reflect only
/// the ticks where the metric was actually measured.
#[derive(Default)]
pub(crate) struct WindowedStat {
    samples: Vec<u64>,
}

impl WindowedStat {
    /// Fold one sample into the window.
    pub(crate) fn record(&mut self, value: u64) {
        self.samples.push(value);
    }

    /// Fold an optional sample, skipping `None` (an unmeasured tick).
    pub(crate) fn record_opt(&mut self, value: Option<u64>) {
        if let Some(value) = value {
            self.samples.push(value);
        }
    }

    /// Number of measured samples in the window. Test-only: production paths
    /// track their own tick/sample counters on the per-stream window struct.
    #[cfg(test)]
    pub(crate) fn count(&self) -> usize {
        self.samples.len()
    }

    /// Smallest sample, or `None` if no sample was measured this window.
    pub(crate) fn min(&self) -> Option<u64> {
        self.samples.iter().copied().min()
    }

    /// Largest sample (the in-window burst peak), or `None`.
    pub(crate) fn max(&self) -> Option<u64> {
        self.samples.iter().copied().max()
    }

    /// Arithmetic mean, floored to an integer (integer division truncates
    /// toward zero), or `None`.
    ///
    /// Summed in `u128` so a full window of large `u64` byte counters cannot
    /// overflow before the divide.
    pub(crate) fn mean(&self) -> Option<u64> {
        if self.samples.is_empty() {
            return None;
        }
        let sum: u128 = self.samples.iter().map(|&v| v as u128).sum();
        Some((sum / self.samples.len() as u128) as u64)
    }

    /// Median (upper-middle value for an even sample count), or `None`.
    ///
    /// For an even sample count this returns `sorted[len / 2]`, the
    /// upper-middle of the two central values. Matches the upper-middle-median
    /// convention the existing per-connection RTT aggregate uses
    /// (`rolling_rtt_stats::cross_connection_median_inflation`, and the recent
    /// median in `RollingRttStats::snapshot`) so the two medians stay directly
    /// comparable in the offline analysis.
    pub(crate) fn p50(&self) -> Option<u64> {
        if self.samples.is_empty() {
            return None;
        }
        let mut sorted = self.samples.clone();
        sorted.sort_unstable();
        Some(sorted[sorted.len() / 2])
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn empty_stat_yields_none() {
        let stat = WindowedStat::default();
        assert_eq!(stat.count(), 0);
        assert_eq!(stat.min(), None);
        assert_eq!(stat.max(), None);
        assert_eq!(stat.mean(), None);
        assert_eq!(stat.p50(), None);
    }

    #[test]
    fn record_opt_skips_none_samples() {
        let mut stat = WindowedStat::default();
        stat.record_opt(None);
        stat.record_opt(Some(10));
        stat.record_opt(None);
        stat.record_opt(Some(20));
        // Only the two measured ticks count.
        assert_eq!(stat.count(), 2);
        assert_eq!(stat.min(), Some(10));
        assert_eq!(stat.max(), Some(20));
        assert_eq!(stat.mean(), Some(15));
    }

    #[test]
    fn summary_stats_over_a_window() {
        let mut stat = WindowedStat::default();
        for v in [5u64, 1, 9, 3, 7] {
            stat.record(v);
        }
        assert_eq!(stat.count(), 5);
        assert_eq!(stat.min(), Some(1));
        assert_eq!(stat.max(), Some(9));
        assert_eq!(stat.mean(), Some(5)); // (5+1+9+3+7)/5 = 25/5
        assert_eq!(stat.p50(), Some(5)); // sorted [1,3,5,7,9], index 2
    }

    #[test]
    fn mean_rounds_down_and_cannot_overflow() {
        let mut stat = WindowedStat::default();
        // A full 30 s window of the largest plausible per-second byte counter.
        for _ in 0..SHADOW_ROLLUP_WINDOW_SECS {
            stat.record(u64::MAX);
        }
        // u128 accumulation keeps this exact rather than overflowing.
        assert_eq!(stat.mean(), Some(u64::MAX));
    }

    #[test]
    fn p50_uses_upper_middle_median_for_even_counts() {
        let mut stat = WindowedStat::default();
        for v in [1u64, 2, 3, 4] {
            stat.record(v);
        }
        // sorted [1,2,3,4], len/2 == 2 → 3 (upper-middle of the two central
        // values 2 and 3; matches the RTT aggregate's median convention).
        assert_eq!(stat.p50(), Some(3));
    }
}
