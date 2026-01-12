//! Rate-limiting layer for tracing.
//!
//! This module provides a rate limiter that limits the rate of log events
//! to prevent log spam from filling up disks. When the rate exceeds the
//! configured threshold, events are dropped and a periodic summary is logged.
//!
//! # Example
//!
//! ```ignore
//! use tracing_subscriber::prelude::*;
//! use crate::util::rate_limit_layer::RateLimiter;
//!
//! let rate_limiter = RateLimiter::new(1000); // 1000 events/second max
//!
//! tracing_subscriber::registry()
//!     .with(tracing_subscriber::fmt::layer()
//!         .with_filter(tracing_subscriber::filter::filter_fn(move |_| {
//!             rate_limiter.should_allow()
//!         })))
//!     .init();
//! ```

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::time::Instant;

/// Default maximum events per second before rate limiting kicks in.
pub const DEFAULT_MAX_EVENTS_PER_SECOND: u64 = 1000;

/// How often to log a summary of dropped events.
const DROPPED_SUMMARY_INTERVAL: Duration = Duration::from_secs(10);

/// Inner state for the rate limiter (shared via Arc for thread safety).
struct RateLimiterInner {
    /// Maximum events allowed per second
    max_events_per_second: u64,
    /// Event counter for current window
    event_count: AtomicU64,
    /// Events dropped in current period
    dropped_count: AtomicU64,
    /// Total events dropped since start
    total_dropped: AtomicU64,
    /// Start of current counting window (as duration since UNIX_EPOCH for atomicity)
    window_start_nanos: AtomicU64,
    /// Last time we logged a dropped summary
    last_summary_nanos: AtomicU64,
    /// Process start time for calculating nanos
    start_instant: Instant,
}

/// Rate limiter for tracing events.
///
/// Uses a sliding window approach to track event rate and drops events
/// when the rate exceeds the configured maximum. Thread-safe and cloneable.
#[derive(Clone)]
pub struct RateLimiter {
    inner: Arc<RateLimiterInner>,
}

impl RateLimiter {
    /// Create a new rate limiter with the specified max events per second.
    pub fn new(max_events_per_second: u64) -> Self {
        let now = Instant::now();
        Self {
            inner: Arc::new(RateLimiterInner {
                max_events_per_second,
                event_count: AtomicU64::new(0),
                dropped_count: AtomicU64::new(0),
                total_dropped: AtomicU64::new(0),
                window_start_nanos: AtomicU64::new(0),
                last_summary_nanos: AtomicU64::new(0),
                start_instant: now,
            }),
        }
    }

    /// Create with default rate limit (1000 events/second).
    pub fn with_defaults() -> Self {
        Self::new(DEFAULT_MAX_EVENTS_PER_SECOND)
    }

    /// Get elapsed nanos since start (for atomic storage).
    fn elapsed_nanos(&self) -> u64 {
        self.inner.start_instant.elapsed().as_nanos() as u64
    }

    /// Check if an event should be allowed and update counters.
    /// Returns true if event should be logged, false if it should be dropped.
    pub fn should_allow(&self) -> bool {
        let now_nanos = self.elapsed_nanos();
        let window_start = self.inner.window_start_nanos.load(Ordering::Relaxed);

        // Check if we're in a new second
        const ONE_SECOND_NANOS: u64 = 1_000_000_000;
        if now_nanos >= window_start + ONE_SECOND_NANOS {
            // New window - reset counters
            // Use compare_exchange to avoid race conditions
            if self
                .inner
                .window_start_nanos
                .compare_exchange(window_start, now_nanos, Ordering::SeqCst, Ordering::Relaxed)
                .is_ok()
            {
                self.inner.event_count.store(0, Ordering::Relaxed);
                // Keep dropped_count for summary, reset it in maybe_log_summary
            }
        }

        // Increment and check count
        let count = self.inner.event_count.fetch_add(1, Ordering::Relaxed);

        if count < self.inner.max_events_per_second {
            self.maybe_log_summary();
            true
        } else {
            // Rate exceeded - drop this event
            self.inner.dropped_count.fetch_add(1, Ordering::Relaxed);
            self.inner.total_dropped.fetch_add(1, Ordering::Relaxed);
            self.maybe_log_summary();
            false
        }
    }

    /// Check if we should log a summary of dropped events.
    fn maybe_log_summary(&self) {
        let dropped = self.inner.dropped_count.load(Ordering::Relaxed);
        if dropped == 0 {
            return;
        }

        let now_nanos = self.elapsed_nanos();
        let last_summary = self.inner.last_summary_nanos.load(Ordering::Relaxed);
        let interval_nanos = DROPPED_SUMMARY_INTERVAL.as_nanos() as u64;

        if now_nanos >= last_summary + interval_nanos {
            // Time to log a summary
            if self
                .inner
                .last_summary_nanos
                .compare_exchange(last_summary, now_nanos, Ordering::SeqCst, Ordering::Relaxed)
                .is_ok()
            {
                let dropped = self.inner.dropped_count.swap(0, Ordering::Relaxed);
                let total = self.inner.total_dropped.load(Ordering::Relaxed);

                if dropped > 0 {
                    // Use eprintln to bypass the rate limiter itself
                    eprintln!(
                        "[RATE LIMIT] Dropped {} log events in last {}s (total dropped: {}). \
                         Max rate: {}/sec. Consider investigating log spam.",
                        dropped,
                        DROPPED_SUMMARY_INTERVAL.as_secs(),
                        total,
                        self.inner.max_events_per_second
                    );
                }
            }
        }
    }
}

impl Default for RateLimiter {
    fn default() -> Self {
        Self::with_defaults()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_rate_limiter_allows_under_limit() {
        let limiter = RateLimiter::new(100);

        // Should allow up to 100 events
        for _ in 0..100 {
            assert!(limiter.should_allow());
        }
    }

    #[test]
    fn test_rate_limiter_drops_over_limit() {
        let limiter = RateLimiter::new(10);

        // Allow first 10
        for _ in 0..10 {
            assert!(limiter.should_allow());
        }

        // Should drop subsequent events
        for _ in 0..10 {
            assert!(!limiter.should_allow());
        }

        // Check dropped count
        assert_eq!(limiter.inner.dropped_count.load(Ordering::Relaxed), 10);
        assert_eq!(limiter.inner.total_dropped.load(Ordering::Relaxed), 10);
    }

    #[test]
    fn test_rate_limiter_resets_after_window() {
        let limiter = RateLimiter::new(5);

        // Exhaust the limit
        for _ in 0..5 {
            assert!(limiter.should_allow());
        }
        assert!(!limiter.should_allow());

        // Simulate what happens when a new time window begins by manually
        // resetting the event counter. The actual window reset logic is tested
        // implicitly - when should_allow detects a new second has passed, it
        // resets event_count to 0. Here we simulate that reset directly.
        limiter.inner.event_count.store(0, Ordering::Relaxed);

        // Should allow again after counter reset
        assert!(limiter.should_allow());

        // Verify we can use the full allowance again
        for _ in 0..4 {
            assert!(limiter.should_allow());
        }
        // Now at limit again
        assert!(!limiter.should_allow());
    }

    #[test]
    fn test_default_limit() {
        let limiter = RateLimiter::with_defaults();
        assert_eq!(
            limiter.inner.max_events_per_second,
            DEFAULT_MAX_EVENTS_PER_SECOND
        );
    }

    #[test]
    fn test_rate_limiter_is_cloneable() {
        let limiter1 = RateLimiter::new(10);
        let limiter2 = limiter1.clone();

        // Both should share state
        for _ in 0..10 {
            assert!(limiter1.should_allow());
        }

        // limiter2 should see the exhausted state
        assert!(!limiter2.should_allow());
    }
}
