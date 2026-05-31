//! Per-peer rolling RTT statistics with a baseline window and a recent window.
//!
//! This is an offline / single-threaded port of
//! `crates/core/src/transport/rolling_rtt_stats.rs`. The shape, window sizes,
//! and inflation semantics are deliberately identical to the production type
//! so the harness evaluates controllers against the same signal they would
//! see in flight. The copy is intentional — the production type evolves with
//! Phase 2 work, and we want the harness to be pinnable to a specific
//! algorithm vintage rather than silently re-baseline as `crates/core`
//! changes.

use std::collections::VecDeque;
use std::time::Duration;

/// Baseline window: 5 minutes (per the RFC, and the current
/// `rolling_rtt_stats.rs` constant).
pub const BASELINE_WINDOW: Duration = Duration::from_secs(300);

/// Recent window: 10 seconds.
pub const RECENT_WINDOW: Duration = Duration::from_secs(10);

/// Hard cap on retained samples per peer. Matches the production constant so
/// the same degradation behaviour applies at very high sample rates.
pub const MAX_SAMPLES: usize = 32_768;

/// Snapshot returned by [`RollingRttStats::snapshot_at`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RttSnapshot {
    /// Minimum RTT in the baseline window. `None` if no samples in window.
    pub baseline_min: Option<Duration>,
    /// Median RTT in the recent window (upper-middle for even N, matching
    /// production). `None` if no samples in window.
    pub recent_median: Option<Duration>,
    /// `recent_median - baseline_min`, saturating at zero. `None` if either
    /// input is `None`.
    pub inflation: Option<Duration>,
    /// Count of samples in the baseline window after pruning.
    pub baseline_samples: usize,
    /// Count of samples in the recent window.
    pub recent_samples: usize,
}

/// Rolling RTT statistics for a single peer. The harness drives time
/// explicitly via [`record_at`](Self::record_at) and
/// [`snapshot_at`](Self::snapshot_at).
#[derive(Debug, Default)]
pub struct RollingRttStats {
    /// (timestamp, rtt) pairs in ascending timestamp order.
    samples: VecDeque<(Duration, Duration)>,
}

impl RollingRttStats {
    pub fn new() -> Self {
        Self {
            samples: VecDeque::with_capacity(1024),
        }
    }

    /// Record an RTT sample observed at `at`. Older samples outside the
    /// baseline window are pruned; the hard `MAX_SAMPLES` cap is enforced.
    pub fn record_at(&mut self, at: Duration, rtt: Duration) {
        self.samples.push_back((at, rtt));
        let cutoff = at.saturating_sub(BASELINE_WINDOW);
        while let Some(&(ts, _)) = self.samples.front() {
            if ts < cutoff {
                self.samples.pop_front();
            } else {
                break;
            }
        }
        while self.samples.len() > MAX_SAMPLES {
            self.samples.pop_front();
        }
    }

    /// Compute a snapshot at the given wall-clock time.
    pub fn snapshot_at(&self, now: Duration) -> Option<RttSnapshot> {
        if self.samples.is_empty() {
            return None;
        }
        let baseline_cutoff = now.saturating_sub(BASELINE_WINDOW);
        let recent_cutoff = now.saturating_sub(RECENT_WINDOW);

        let mut baseline_min: Option<Duration> = None;
        let mut baseline_samples = 0usize;
        let mut recent: Vec<Duration> = Vec::new();
        for &(ts, rtt) in &self.samples {
            if ts < baseline_cutoff {
                continue;
            }
            baseline_samples += 1;
            baseline_min = Some(match baseline_min {
                Some(m) => m.min(rtt),
                None => rtt,
            });
            if ts >= recent_cutoff {
                recent.push(rtt);
            }
        }
        if baseline_samples == 0 {
            return None;
        }
        let recent_samples = recent.len();
        let recent_median = if recent.is_empty() {
            None
        } else {
            recent.sort_unstable();
            // Upper-middle, matching production.
            Some(recent[recent.len() / 2])
        };
        let inflation = match (baseline_min, recent_median) {
            (Some(b), Some(r)) => Some(r.saturating_sub(b)),
            _ => None,
        };
        Some(RttSnapshot {
            baseline_min,
            recent_median,
            inflation,
            baseline_samples,
            recent_samples,
        })
    }

    #[cfg(test)]
    pub fn stored(&self) -> usize {
        self.samples.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ms(n: u64) -> Duration {
        Duration::from_millis(n)
    }

    #[test]
    fn empty_snapshot_is_none() {
        let s = RollingRttStats::new();
        assert!(s.snapshot_at(ms(0)).is_none());
    }

    #[test]
    fn baseline_min_tracks_minimum() {
        let mut s = RollingRttStats::new();
        for (i, rtt) in [50u64, 80, 40, 60, 100].into_iter().enumerate() {
            s.record_at(ms(i as u64 * 100), ms(rtt));
        }
        let snap = s.snapshot_at(ms(1000)).unwrap();
        assert_eq!(snap.baseline_min, Some(ms(40)));
    }

    #[test]
    fn inflation_is_recent_minus_baseline() {
        let mut s = RollingRttStats::new();
        // 5 min ago: a fast sample (the baseline)
        s.record_at(Duration::from_secs(0), ms(20));
        // Recent: slower samples
        for t in [290u64, 291, 292, 293, 294] {
            s.record_at(Duration::from_secs(t), ms(80));
        }
        let snap = s.snapshot_at(Duration::from_secs(295)).unwrap();
        assert_eq!(snap.baseline_min, Some(ms(20)));
        assert_eq!(snap.recent_median, Some(ms(80)));
        assert_eq!(snap.inflation, Some(ms(60)));
    }

    #[test]
    fn samples_older_than_baseline_are_dropped() {
        let mut s = RollingRttStats::new();
        s.record_at(Duration::from_secs(0), ms(50));
        s.record_at(Duration::from_secs(400), ms(60));
        // First sample is 400s old when we record the second; baseline window
        // is 300s, so the first should have been pruned.
        let snap = s.snapshot_at(Duration::from_secs(400)).unwrap();
        assert_eq!(snap.baseline_samples, 1);
        assert_eq!(snap.baseline_min, Some(ms(60)));
    }

    #[test]
    fn recent_median_uses_upper_middle_for_even_length() {
        let mut s = RollingRttStats::new();
        for (i, rtt) in [40u64, 50, 60, 70].into_iter().enumerate() {
            s.record_at(ms(i as u64 * 100), ms(rtt));
        }
        let snap = s.snapshot_at(ms(400)).unwrap();
        // [40, 50, 60, 70] sorted, len/2 = 2 → 60.
        assert_eq!(snap.recent_median, Some(ms(60)));
    }
}
