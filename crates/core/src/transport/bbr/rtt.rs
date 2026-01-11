//! RTT tracking with windowed minimum filter.
//!
//! BBRv3 estimates the path's propagation delay by tracking the minimum
//! RTT observed over a time window. This module provides a lock-free
//! min_rtt filter and ProbeRTT scheduling logic.

use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::Duration;

use super::config::{MIN_RTT_FILTER_WINDOW, PROBE_RTT_DURATION, PROBE_RTT_INTERVAL};

/// A lock-free minimum RTT tracker.
///
/// Tracks the minimum RTT observed over a sliding window (typically 10 seconds).
/// Also manages ProbeRTT scheduling to ensure min_rtt stays fresh.
///
/// ## Design
///
/// - Single min_rtt value with timestamp
/// - Expires after MIN_RTT_FILTER_WINDOW (10s)
/// - ProbeRTT is scheduled when min_rtt expires or hasn't been updated
/// - Uses atomic operations for lock-free access
pub(crate) struct RttTracker {
    /// Minimum RTT in nanoseconds.
    min_rtt_nanos: AtomicU64,

    /// Timestamp when min_rtt was recorded (nanoseconds).
    min_rtt_stamp_nanos: AtomicU64,

    /// Timestamp of last ProbeRTT entry (nanoseconds).
    last_probe_rtt_nanos: AtomicU64,

    /// Next scheduled ProbeRTT time (nanoseconds).
    next_probe_rtt_nanos: AtomicU64,

    /// Time when we entered ProbeRTT (for duration tracking).
    probe_rtt_entry_nanos: AtomicU64,

    /// Whether we're currently in ProbeRTT and have drained inflight.
    probe_rtt_round_done: AtomicBool,

    /// Window duration for min_rtt filter (nanoseconds).
    filter_window_nanos: u64,

    /// ProbeRTT interval (nanoseconds).
    probe_interval_nanos: u64,

    /// ProbeRTT duration (nanoseconds).
    probe_duration_nanos: u64,
}

impl RttTracker {
    /// Create a new RTT tracker with default parameters.
    pub(crate) fn new() -> Self {
        Self::with_params(
            MIN_RTT_FILTER_WINDOW,
            PROBE_RTT_INTERVAL,
            PROBE_RTT_DURATION,
        )
    }

    /// Create a new RTT tracker with custom parameters.
    pub(crate) fn with_params(
        filter_window: Duration,
        probe_interval: Duration,
        probe_duration: Duration,
    ) -> Self {
        Self {
            min_rtt_nanos: AtomicU64::new(u64::MAX),
            min_rtt_stamp_nanos: AtomicU64::new(0),
            last_probe_rtt_nanos: AtomicU64::new(0),
            next_probe_rtt_nanos: AtomicU64::new(0),
            probe_rtt_entry_nanos: AtomicU64::new(0),
            probe_rtt_round_done: AtomicBool::new(false),
            filter_window_nanos: filter_window.as_nanos() as u64,
            probe_interval_nanos: probe_interval.as_nanos() as u64,
            probe_duration_nanos: probe_duration.as_nanos() as u64,
        }
    }

    /// Update min_rtt with a new RTT sample.
    ///
    /// # Arguments
    /// * `rtt` - RTT sample
    /// * `now_nanos` - Current time in nanoseconds
    ///
    /// # Returns
    /// `true` if this sample updated min_rtt, `false` otherwise.
    pub(crate) fn update(&self, rtt: Duration, now_nanos: u64) -> bool {
        let rtt_nanos = rtt.as_nanos() as u64;
        let current_min = self.min_rtt_nanos.load(Ordering::Acquire);
        let current_stamp = self.min_rtt_stamp_nanos.load(Ordering::Acquire);

        // Check if current min_rtt has expired
        let expired = now_nanos.saturating_sub(current_stamp) > self.filter_window_nanos;

        // Update if new sample is smaller OR if current min has expired
        if rtt_nanos < current_min || expired {
            self.min_rtt_nanos.store(rtt_nanos, Ordering::Release);
            self.min_rtt_stamp_nanos.store(now_nanos, Ordering::Release);
            return true;
        }

        false
    }

    /// Get the current min_rtt.
    ///
    /// # Returns
    /// The minimum RTT, or `None` if no valid sample exists.
    pub(crate) fn min_rtt(&self) -> Option<Duration> {
        let nanos = self.min_rtt_nanos.load(Ordering::Acquire);
        if nanos == u64::MAX {
            None
        } else {
            Some(Duration::from_nanos(nanos))
        }
    }

    /// Get min_rtt in nanoseconds.
    pub(crate) fn min_rtt_nanos(&self) -> u64 {
        self.min_rtt_nanos.load(Ordering::Acquire)
    }

    /// Check if min_rtt has expired and needs refresh via ProbeRTT.
    pub(crate) fn is_expired(&self, now_nanos: u64) -> bool {
        let stamp = self.min_rtt_stamp_nanos.load(Ordering::Acquire);
        now_nanos.saturating_sub(stamp) > self.filter_window_nanos
    }

    /// Check if it's time to enter ProbeRTT.
    ///
    /// ProbeRTT should be entered when:
    /// 1. min_rtt has expired, OR
    /// 2. The scheduled ProbeRTT time has arrived
    pub(crate) fn should_enter_probe_rtt(&self, now_nanos: u64) -> bool {
        // Check if min_rtt expired
        if self.is_expired(now_nanos) {
            return true;
        }

        // Check if scheduled time arrived
        let next = self.next_probe_rtt_nanos.load(Ordering::Acquire);
        next > 0 && now_nanos >= next
    }

    /// Record entry into ProbeRTT state.
    pub(crate) fn enter_probe_rtt(&self, now_nanos: u64) {
        self.probe_rtt_entry_nanos
            .store(now_nanos, Ordering::Release);
        self.probe_rtt_round_done.store(false, Ordering::Release);
    }

    /// Mark that inflight has been drained during ProbeRTT.
    pub(crate) fn mark_probe_rtt_round_done(&self) {
        self.probe_rtt_round_done.store(true, Ordering::Release);
    }

    /// Check if ProbeRTT duration has elapsed and we can exit.
    ///
    /// # Returns
    /// `true` if we should exit ProbeRTT, `false` otherwise.
    pub(crate) fn should_exit_probe_rtt(&self, now_nanos: u64) -> bool {
        let entry = self.probe_rtt_entry_nanos.load(Ordering::Acquire);
        let elapsed = now_nanos.saturating_sub(entry);
        let round_done = self.probe_rtt_round_done.load(Ordering::Acquire);

        elapsed >= self.probe_duration_nanos && round_done
    }

    /// Record exit from ProbeRTT and schedule next ProbeRTT.
    ///
    /// # Arguments
    /// * `now_nanos` - Current time in nanoseconds
    /// * `jitter_nanos` - Random jitter to add (0 for no jitter)
    pub(crate) fn exit_probe_rtt(&self, now_nanos: u64, jitter_nanos: i64) {
        self.last_probe_rtt_nanos
            .store(now_nanos, Ordering::Release);

        // Schedule next ProbeRTT with optional jitter
        let next = now_nanos
            .saturating_add(self.probe_interval_nanos)
            .saturating_add_signed(jitter_nanos);
        self.next_probe_rtt_nanos.store(next, Ordering::Release);
    }

    /// Reset min_rtt to unknown state (e.g., after a timeout).
    pub(crate) fn reset_min_rtt(&self) {
        self.min_rtt_nanos.store(u64::MAX, Ordering::Release);
        self.min_rtt_stamp_nanos.store(0, Ordering::Release);
    }
}

impl Default for RttTracker {
    fn default() -> Self {
        Self::new()
    }
}

impl std::fmt::Debug for RttTracker {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let min_rtt = self.min_rtt();
        f.debug_struct("RttTracker")
            .field("min_rtt", &min_rtt)
            .field(
                "min_rtt_stamp_nanos",
                &self.min_rtt_stamp_nanos.load(Ordering::Relaxed),
            )
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_rtt_tracker_basic() {
        let tracker = RttTracker::new();

        // Initially no min_rtt
        assert!(tracker.min_rtt().is_none());

        // Add first sample
        let updated = tracker.update(Duration::from_millis(50), 1_000_000_000);
        assert!(updated);
        assert_eq!(tracker.min_rtt(), Some(Duration::from_millis(50)));
    }

    #[test]
    fn test_rtt_tracker_keeps_minimum() {
        let tracker = RttTracker::new();

        tracker.update(Duration::from_millis(100), 1_000_000_000);
        tracker.update(Duration::from_millis(50), 2_000_000_000);
        tracker.update(Duration::from_millis(75), 3_000_000_000);

        // Should keep the minimum (50ms)
        assert_eq!(tracker.min_rtt(), Some(Duration::from_millis(50)));
    }

    #[test]
    fn test_rtt_tracker_expiration() {
        let tracker = RttTracker::with_params(
            Duration::from_secs(5),
            Duration::from_secs(5),
            Duration::from_millis(200),
        );

        // Add sample at t=0
        tracker.update(Duration::from_millis(50), 0);
        assert!(!tracker.is_expired(4_000_000_000)); // 4s - not expired

        // After 5s window, should be expired
        assert!(tracker.is_expired(6_000_000_000)); // 6s - expired
    }

    #[test]
    fn test_rtt_tracker_expired_update() {
        let tracker = RttTracker::with_params(
            Duration::from_secs(5),
            Duration::from_secs(5),
            Duration::from_millis(200),
        );

        // Add sample at t=0
        tracker.update(Duration::from_millis(50), 0);

        // Add larger sample after expiration - should update because expired
        let updated = tracker.update(Duration::from_millis(100), 6_000_000_000);
        assert!(updated);
        assert_eq!(tracker.min_rtt(), Some(Duration::from_millis(100)));
    }

    #[test]
    fn test_probe_rtt_scheduling() {
        let tracker = RttTracker::with_params(
            Duration::from_secs(10),
            Duration::from_secs(5),
            Duration::from_millis(200),
        );

        // Initially no ProbeRTT scheduled
        assert!(!tracker.should_enter_probe_rtt(1_000_000_000));

        // After min_rtt expires
        tracker.update(Duration::from_millis(50), 0);
        assert!(tracker.should_enter_probe_rtt(11_000_000_000)); // 11s > 10s window
    }

    #[test]
    fn test_probe_rtt_lifecycle() {
        let tracker = RttTracker::with_params(
            Duration::from_secs(10),
            Duration::from_secs(5),
            Duration::from_millis(200),
        );

        // Enter ProbeRTT
        tracker.enter_probe_rtt(1_000_000_000);

        // Shouldn't exit immediately
        assert!(!tracker.should_exit_probe_rtt(1_100_000_000)); // 100ms elapsed

        // Mark round done
        tracker.mark_probe_rtt_round_done();

        // Still shouldn't exit until 200ms
        assert!(!tracker.should_exit_probe_rtt(1_100_000_000));

        // Should exit after 200ms
        assert!(tracker.should_exit_probe_rtt(1_250_000_000)); // 250ms elapsed

        // Exit and schedule next
        tracker.exit_probe_rtt(1_250_000_000, 0);

        // Next ProbeRTT should be in 5s
        let next = tracker.next_probe_rtt_nanos.load(Ordering::Acquire);
        assert_eq!(next, 1_250_000_000 + 5_000_000_000);
    }
}
