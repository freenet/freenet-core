//! Bandwidth estimation using windowed maximum filter.
//!
//! BBRv3 estimates the bottleneck bandwidth by tracking the maximum
//! delivery rate observed over a recent time window. This module provides
//! a lock-free windowed max filter implementation.

use std::sync::atomic::{AtomicU64, Ordering};

use super::config::BW_FILTER_SIZE;

/// A lock-free windowed maximum filter for bandwidth estimation.
///
/// Tracks the maximum bandwidth (in bytes/sec) observed over a sliding
/// time window. Uses a ring buffer of (bandwidth, timestamp) samples
/// and returns the maximum bandwidth among non-expired samples.
///
/// ## Design
///
/// - Ring buffer of fixed size (BW_FILTER_SIZE)
/// - Each slot stores bandwidth and timestamp as atomic u64s
/// - Maximum is computed by scanning all slots and filtering by timestamp
/// - Lock-free for concurrent access from sender and ACK processing paths
pub(crate) struct BandwidthFilter {
    /// Ring buffer of bandwidth samples (bytes/sec).
    samples: [AtomicU64; BW_FILTER_SIZE],
    /// Timestamps for each sample (nanoseconds since epoch).
    timestamps: [AtomicU64; BW_FILTER_SIZE],
    /// Write index (modulo BW_FILTER_SIZE).
    write_idx: AtomicU64,
    /// Window duration in nanoseconds.
    window_nanos: u64,
}

impl BandwidthFilter {
    /// Create a new bandwidth filter with the given window duration.
    #[allow(clippy::declare_interior_mutable_const)]
    pub(crate) fn new(window: std::time::Duration) -> Self {
        // Initialize arrays with default values
        const ZERO: AtomicU64 = AtomicU64::new(0);
        Self {
            samples: [ZERO; BW_FILTER_SIZE],
            timestamps: [ZERO; BW_FILTER_SIZE],
            write_idx: AtomicU64::new(0),
            window_nanos: window.as_nanos() as u64,
        }
    }

    /// Add a new bandwidth sample.
    ///
    /// # Arguments
    /// * `bw` - Bandwidth in bytes/sec
    /// * `now_nanos` - Current time in nanoseconds
    pub(crate) fn update(&self, bw: u64, now_nanos: u64) {
        // Get next write index and advance
        let idx = self.write_idx.fetch_add(1, Ordering::AcqRel) as usize % BW_FILTER_SIZE;

        // Store sample and timestamp
        self.samples[idx].store(bw, Ordering::Release);
        self.timestamps[idx].store(now_nanos, Ordering::Release);
    }

    /// Get the maximum bandwidth in the current window.
    ///
    /// # Arguments
    /// * `now_nanos` - Current time in nanoseconds
    ///
    /// # Returns
    /// Maximum bandwidth in bytes/sec among non-expired samples,
    /// or 0 if no valid samples exist.
    pub(crate) fn max_bw(&self, now_nanos: u64) -> u64 {
        let cutoff = now_nanos.saturating_sub(self.window_nanos);
        let mut max = 0u64;

        for i in 0..BW_FILTER_SIZE {
            let timestamp = self.timestamps[i].load(Ordering::Acquire);
            if timestamp >= cutoff {
                let bw = self.samples[i].load(Ordering::Acquire);
                max = max.max(bw);
            }
        }

        max
    }

    /// Reset the filter, clearing all samples.
    pub(crate) fn reset(&self) {
        for i in 0..BW_FILTER_SIZE {
            self.samples[i].store(0, Ordering::Release);
            self.timestamps[i].store(0, Ordering::Release);
        }
        self.write_idx.store(0, Ordering::Release);
    }

    /// Check if the filter has any valid samples.
    pub(crate) fn has_samples(&self, now_nanos: u64) -> bool {
        let cutoff = now_nanos.saturating_sub(self.window_nanos);
        for i in 0..BW_FILTER_SIZE {
            let timestamp = self.timestamps[i].load(Ordering::Acquire);
            if timestamp >= cutoff && timestamp > 0 {
                return true;
            }
        }
        false
    }
}

impl std::fmt::Debug for BandwidthFilter {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BandwidthFilter")
            .field("window_nanos", &self.window_nanos)
            .field("write_idx", &self.write_idx.load(Ordering::Relaxed))
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    #[test]
    fn test_bandwidth_filter_basic() {
        let filter = BandwidthFilter::new(Duration::from_secs(10));

        // Add some samples
        filter.update(1_000_000, 1_000_000_000); // 1 MB/s at 1s
        filter.update(2_000_000, 2_000_000_000); // 2 MB/s at 2s
        filter.update(1_500_000, 3_000_000_000); // 1.5 MB/s at 3s

        // Max should be 2 MB/s within window
        let max = filter.max_bw(5_000_000_000); // at 5s
        assert_eq!(max, 2_000_000);
    }

    #[test]
    fn test_bandwidth_filter_expiration() {
        let filter = BandwidthFilter::new(Duration::from_secs(5));

        // Add a sample at time 0
        filter.update(1_000_000, 0);

        // Should be visible at 4s
        assert_eq!(filter.max_bw(4_000_000_000), 1_000_000);

        // Should expire at 6s
        assert_eq!(filter.max_bw(6_000_000_000), 0);
    }

    #[test]
    fn test_bandwidth_filter_windowed_max() {
        let filter = BandwidthFilter::new(Duration::from_secs(5));

        // Add samples at different times
        filter.update(100, 1_000_000_000); // 100 B/s at 1s
        filter.update(500, 2_000_000_000); // 500 B/s at 2s
        filter.update(300, 3_000_000_000); // 300 B/s at 3s

        // At 4s, all samples should be valid, max is 500
        assert_eq!(filter.max_bw(4_000_000_000), 500);

        // At 7s, only samples from 2s and 3s are valid (5s window)
        assert_eq!(filter.max_bw(7_000_000_000), 500);

        // At 8s, only sample from 3s is valid
        assert_eq!(filter.max_bw(8_000_000_000), 300);
    }

    #[test]
    fn test_bandwidth_filter_reset() {
        let filter = BandwidthFilter::new(Duration::from_secs(10));

        filter.update(1_000_000, 1_000_000_000);
        assert_eq!(filter.max_bw(2_000_000_000), 1_000_000);

        filter.reset();
        assert_eq!(filter.max_bw(2_000_000_000), 0);
    }

    #[test]
    fn test_bandwidth_filter_has_samples() {
        let filter = BandwidthFilter::new(Duration::from_secs(5));

        assert!(!filter.has_samples(1_000_000_000));

        filter.update(1000, 1_000_000_000);
        assert!(filter.has_samples(2_000_000_000));

        // After expiration
        assert!(!filter.has_samples(7_000_000_000));
    }
}
