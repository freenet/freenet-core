//! Lock-free atomic data structures for LEDBAT.
//!
//! This module provides thread-safe atomic implementations of delay filtering
//! and base delay history tracking as specified in RFC 6817.

use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::time::Duration;

use crate::simulation::TimeSource;

use super::config::{BASE_HISTORY_SIZE, DELAY_FILTER_SIZE};

/// Sentinel value indicating an empty slot in atomic delay arrays.
/// We use u64::MAX since no valid RTT would ever be this large (~584 years).
pub(crate) const EMPTY_DELAY_NANOS: u64 = u64::MAX;

/// Lock-free delay filter: MIN over recent samples (RFC 6817 Section 4.2).
///
/// Uses a fixed-size atomic ring buffer. Each slot stores RTT in nanoseconds.
/// Readers compute the minimum over all valid (non-empty) slots.
///
/// # Timing Assumptions
///
/// RTT values are stored as nanoseconds in `u64`. The cast from `Duration::as_nanos()`
/// (which returns `u128`) is safe because realistic RTT values are always far below
/// `u64::MAX` (~584 years). Network RTTs range from microseconds to seconds.
pub(crate) struct AtomicDelayFilter {
    /// Ring buffer of RTT samples (stored as nanoseconds)
    samples: [AtomicU64; DELAY_FILTER_SIZE],
    /// Write index (wraps around)
    write_index: AtomicUsize,
    /// Number of samples added (saturates at DELAY_FILTER_SIZE)
    sample_count: AtomicUsize,
}

impl AtomicDelayFilter {
    pub(crate) fn new() -> Self {
        Self {
            samples: std::array::from_fn(|_| AtomicU64::new(EMPTY_DELAY_NANOS)),
            write_index: AtomicUsize::new(0),
            sample_count: AtomicUsize::new(0),
        }
    }

    pub(crate) fn add_sample(&self, rtt: Duration) {
        // Safe cast: RTT values are always far below u64::MAX (~584 years)
        let nanos = rtt.as_nanos() as u64;
        let idx = self.write_index.fetch_add(1, Ordering::Relaxed) % DELAY_FILTER_SIZE;
        self.samples[idx].store(nanos, Ordering::Release);

        // Increment sample count, saturating at DELAY_FILTER_SIZE
        self.sample_count
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |count| {
                if count < DELAY_FILTER_SIZE {
                    Some(count + 1)
                } else {
                    None // Already saturated
                }
            })
            .ok();
    }

    pub(crate) fn filtered_delay(&self) -> Option<Duration> {
        let mut min_nanos = u64::MAX;
        for slot in &self.samples {
            let nanos = slot.load(Ordering::Acquire);
            if nanos != EMPTY_DELAY_NANOS && nanos < min_nanos {
                min_nanos = nanos;
            }
        }
        if min_nanos == u64::MAX {
            None
        } else {
            Some(Duration::from_nanos(min_nanos))
        }
    }

    pub(crate) fn is_ready(&self) -> bool {
        self.sample_count.load(Ordering::Acquire) >= 2
    }
}

/// Lock-free base delay history: 10-minute bucket tracking (RFC 6817 Section 4.1).
///
/// Uses atomic arrays for buckets and atomic values for current minute tracking.
/// All updates use compare-and-swap for thread safety.
///
/// # Timing Assumptions
///
/// Durations are stored as nanoseconds in `u64`. This limits representable
/// durations to ~584 years, which is acceptable for RTT measurements.
/// The epoch-based timing also assumes the controller won't run for 584+ years.
///
/// # Type Parameter
///
/// `T` is the time source used for timing operations. In production, this is
/// `RealTime` which uses `Instant::now()`. In tests, this can be
/// `VirtualTime` for deterministic virtual time testing.
pub(crate) struct AtomicBaseDelayHistory<T: TimeSource> {
    /// One bucket per minute, containing minimum delay observed in that minute (nanos)
    buckets: [AtomicU64; BASE_HISTORY_SIZE],
    /// Number of valid buckets (0 to BASE_HISTORY_SIZE)
    bucket_count: AtomicUsize,
    /// Next bucket index to write (wraps around)
    bucket_write_index: AtomicUsize,
    /// Minimum being tracked for current minute (nanos)
    current_minute_min: AtomicU64,
    /// Start time of current minute (nanos since epoch instant)
    current_minute_start_nanos: AtomicU64,
    /// Time source for getting current time
    time_source: T,
    /// Epoch in nanoseconds for time calculations
    epoch_nanos: u64,
}

impl<T: TimeSource> AtomicBaseDelayHistory<T> {
    pub(crate) fn new(time_source: T) -> Self {
        let epoch_nanos = time_source.now_nanos();
        Self {
            buckets: std::array::from_fn(|_| AtomicU64::new(EMPTY_DELAY_NANOS)),
            bucket_count: AtomicUsize::new(0),
            bucket_write_index: AtomicUsize::new(0),
            current_minute_min: AtomicU64::new(EMPTY_DELAY_NANOS),
            current_minute_start_nanos: AtomicU64::new(0),
            time_source,
            epoch_nanos,
        }
    }

    pub(crate) fn update(&self, rtt_sample: Duration) {
        // Safe cast: RTT values are always far below u64::MAX (~584 years)
        let rtt_nanos = rtt_sample.as_nanos() as u64;
        let now_nanos = self.time_source.now_nanos() - self.epoch_nanos;
        let minute_nanos = 60_000_000_000u64; // 60 seconds in nanos

        let minute_start = self.current_minute_start_nanos.load(Ordering::Acquire);

        // Check if we've rolled over to a new minute
        if now_nanos.saturating_sub(minute_start) >= minute_nanos {
            // Try to roll over to new minute (only one thread should succeed)
            let new_minute_start = now_nanos;
            if self
                .current_minute_start_nanos
                .compare_exchange(
                    minute_start,
                    new_minute_start,
                    Ordering::AcqRel,
                    Ordering::Acquire,
                )
                .is_ok()
            {
                // We won the race to roll over - atomically reset current minute
                // and capture the old minimum for bucket storage.
                //
                // Use swap to EMPTY first, then compete for new minimum.
                // This prevents the race where a losing thread's update_current_min
                // sets a smaller value that we then overwrite.
                let old_min = self
                    .current_minute_min
                    .swap(EMPTY_DELAY_NANOS, Ordering::AcqRel);

                if old_min != EMPTY_DELAY_NANOS {
                    // Write old minimum to bucket ring buffer
                    let idx =
                        self.bucket_write_index.fetch_add(1, Ordering::Relaxed) % BASE_HISTORY_SIZE;
                    self.buckets[idx].store(old_min, Ordering::Release);

                    // Increment bucket count, saturating at BASE_HISTORY_SIZE
                    self.bucket_count
                        .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |count| {
                            if count < BASE_HISTORY_SIZE {
                                Some(count + 1)
                            } else {
                                None
                            }
                        })
                        .ok();
                }

                // Now compete for the new minute's minimum
                self.update_current_min(rtt_nanos);
            } else {
                // Lost the race - another thread rolled over, update new minute's minimum
                self.update_current_min(rtt_nanos);
            }
        } else {
            // Still in current minute - update minimum
            self.update_current_min(rtt_nanos);
        }
    }

    /// Atomically update current minute minimum if new value is smaller
    fn update_current_min(&self, rtt_nanos: u64) {
        self.current_minute_min
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |current| {
                if rtt_nanos < current {
                    Some(rtt_nanos)
                } else {
                    None // Current is already smaller or equal
                }
            })
            .ok();
    }

    pub(crate) fn base_delay(&self) -> Duration {
        // Find minimum across all buckets
        let mut historical_min = u64::MAX;
        for bucket in &self.buckets {
            let nanos = bucket.load(Ordering::Acquire);
            if nanos != EMPTY_DELAY_NANOS && nanos < historical_min {
                historical_min = nanos;
            }
        }

        // Also consider current minute
        let current_min = self.current_minute_min.load(Ordering::Acquire);

        let result_nanos = match (historical_min != u64::MAX, current_min != EMPTY_DELAY_NANOS) {
            (true, true) => historical_min.min(current_min),
            (true, false) => historical_min,
            (false, true) => current_min,
            (false, false) => 10_000_000, // 10ms fallback in nanos
        };

        Duration::from_nanos(result_nanos)
    }
}
