//! Unified exponential backoff utilities.
//!
//! This module provides reusable backoff primitives to avoid duplicating
//! exponential backoff logic throughout the codebase.
//!
//! # Components
//!
//! - [`ExponentialBackoff`]: Stateless delay calculator for simple retry scenarios
//! - [`TrackedBackoff`]: Per-key backoff tracker with automatic eviction
//!
//! # Example
//!
//! ```ignore
//! use std::time::Duration;
//! use crate::util::backoff::{ExponentialBackoff, TrackedBackoff};
//!
//! // Simple delay calculation
//! let backoff = ExponentialBackoff::new(
//!     Duration::from_secs(1),
//!     Duration::from_secs(60),
//! );
//! assert_eq!(backoff.delay(0), Duration::from_secs(1));
//! assert_eq!(backoff.delay(1), Duration::from_secs(2));
//! assert_eq!(backoff.delay(2), Duration::from_secs(4));
//!
//! // Per-key tracking
//! let mut tracker: TrackedBackoff<String> = TrackedBackoff::new(backoff, 100);
//! tracker.record_failure("peer1".to_string());
//! assert!(tracker.is_in_backoff(&"peer1".to_string()));
//! ```

use std::collections::HashMap;
use std::hash::Hash;
use std::time::Duration;
use tokio::time::Instant;

/// Stateless exponential backoff delay calculator.
///
/// Computes delays using the formula: `base * 2^attempt`, capped at `max`.
/// This is useful for simple retry loops where you don't need to track
/// per-target state.
///
/// # Formula
///
/// For attempt `n` (0-indexed):
/// - Attempt 0: `base`
/// - Attempt 1: `base * 2`
/// - Attempt 2: `base * 4`
/// - ...
/// - Capped at `max`
#[derive(Debug, Clone)]
pub struct ExponentialBackoff {
    /// Base delay (delay for first attempt, i.e., attempt 0)
    base: Duration,
    /// Maximum delay (cap)
    max: Duration,
}

impl ExponentialBackoff {
    /// Create a new exponential backoff calculator.
    ///
    /// # Arguments
    /// - `base`: Initial delay (for attempt 0)
    /// - `max`: Maximum delay cap
    pub const fn new(base: Duration, max: Duration) -> Self {
        Self { base, max }
    }

    /// Calculate the delay for a given attempt number (0-indexed).
    ///
    /// Returns `base * 2^attempt`, capped at `max`.
    #[inline]
    pub fn delay(&self, attempt: u32) -> Duration {
        // Cap exponent to avoid overflow (2^10 = 1024 is plenty)
        let exponent = attempt.min(10);
        let multiplier = 1u64 << exponent;
        let delay = self.base.saturating_mul(multiplier as u32);

        if delay > self.max {
            self.max
        } else {
            delay
        }
    }

    /// Calculate the delay for a given failure count (1-indexed).
    ///
    /// This is a convenience method that treats failure count as 1-indexed,
    /// so first failure (count=1) returns `base`, second (count=2) returns `base * 2`, etc.
    ///
    /// Returns `base * 2^(failures - 1)`, capped at `max`.
    /// Returns `Duration::ZERO` if `failures == 0`.
    #[inline]
    pub fn delay_for_failures(&self, failures: u32) -> Duration {
        if failures == 0 {
            return Duration::ZERO;
        }
        self.delay(failures - 1)
    }

    /// Get the base delay.
    #[inline]
    #[allow(dead_code)]
    pub fn base(&self) -> Duration {
        self.base
    }

    /// Get the maximum delay.
    #[inline]
    pub fn max(&self) -> Duration {
        self.max
    }
}

impl Default for ExponentialBackoff {
    /// Default backoff: 5 seconds base, 5 minutes max.
    fn default() -> Self {
        Self {
            base: Duration::from_secs(5),
            max: Duration::from_secs(300),
        }
    }
}

/// State for a single backoff entry.
#[derive(Debug, Clone)]
struct BackoffEntry {
    /// Number of consecutive failures
    consecutive_failures: u32,
    /// When the last failure occurred
    last_failure: Instant,
    /// When retry is allowed
    retry_after: Instant,
}

/// Per-key backoff tracker with automatic LRU eviction.
///
/// Tracks backoff state for multiple keys (e.g., peer addresses, locations).
/// When a key experiences failures, it enters a backoff period during which
/// `is_in_backoff()` returns true. Success clears the backoff.
///
/// # Type Parameter
///
/// `K` is the key type used to identify targets. Common choices:
/// - `SocketAddr` for per-peer tracking
/// - `Location` or bucket type for per-location tracking
/// - `ContractKey` for per-contract tracking
///
/// # Eviction
///
/// When the number of tracked entries exceeds `max_entries`, the oldest
/// entries (by `last_failure` time) are evicted.
#[derive(Debug)]
pub struct TrackedBackoff<K: Eq + Hash> {
    /// Backoff state per key
    entries: HashMap<K, BackoffEntry>,
    /// Backoff configuration
    config: ExponentialBackoff,
    /// Maximum number of entries before LRU eviction
    max_entries: usize,
}

impl<K: Eq + Hash + Clone> TrackedBackoff<K> {
    /// Create a new tracked backoff with the given configuration.
    pub fn new(config: ExponentialBackoff, max_entries: usize) -> Self {
        Self {
            entries: HashMap::new(),
            config,
            max_entries,
        }
    }

    /// Create a tracked backoff with default configuration.
    ///
    /// Uses 5-second base, 5-minute max, and 256 max entries.
    pub fn with_defaults() -> Self {
        Self::new(ExponentialBackoff::default(), 256)
    }

    /// Check if a key is currently in backoff.
    ///
    /// Returns `true` if retry should be delayed, `false` if retry is allowed.
    pub fn is_in_backoff(&self, key: &K) -> bool {
        if let Some(entry) = self.entries.get(key) {
            Instant::now() < entry.retry_after
        } else {
            false
        }
    }

    /// Get the remaining backoff duration for a key, if any.
    ///
    /// Returns `Some(duration)` if key is in backoff, `None` otherwise.
    pub fn remaining_backoff(&self, key: &K) -> Option<Duration> {
        self.entries.get(key).and_then(|entry| {
            let now = Instant::now();
            if now < entry.retry_after {
                Some(entry.retry_after - now)
            } else {
                None
            }
        })
    }

    /// Record a failure for a key.
    ///
    /// Increments the failure count and calculates the next retry time.
    pub fn record_failure(&mut self, key: K) {
        let now = Instant::now();

        let consecutive_failures = {
            let entry = self.entries.entry(key.clone()).or_insert(BackoffEntry {
                consecutive_failures: 0,
                last_failure: now,
                retry_after: now,
            });
            entry.consecutive_failures = entry.consecutive_failures.saturating_add(1);
            entry.last_failure = now;
            entry.consecutive_failures
        };

        let backoff = self.config.delay_for_failures(consecutive_failures);

        if let Some(entry) = self.entries.get_mut(&key) {
            entry.retry_after = now + backoff;
        }

        tracing::debug!(
            failures = consecutive_failures,
            backoff_secs = backoff.as_secs(),
            "Backoff recorded for key"
        );

        self.evict_if_needed();
    }

    /// Record a success for a key.
    ///
    /// Clears the backoff state for that key.
    pub fn record_success(&mut self, key: &K) {
        if self.entries.remove(key).is_some() {
            tracing::debug!("Backoff cleared for key");
        }
    }

    /// Get the consecutive failure count for a key.
    ///
    /// Returns 0 if the key has no recorded failures.
    pub fn failure_count(&self, key: &K) -> u32 {
        self.entries
            .get(key)
            .map(|e| e.consecutive_failures)
            .unwrap_or(0)
    }

    /// Clean up expired backoff entries.
    ///
    /// Removes entries that are both:
    /// 1. Past their retry time
    /// 2. Have been stale for longer than max backoff duration
    ///
    /// This prevents unbounded memory growth from old entries.
    pub fn cleanup_expired(&mut self) {
        let now = Instant::now();
        let max_stale = self.config.max();

        self.entries.retain(|_, entry| {
            let is_past_retry = now >= entry.retry_after;
            let is_stale = now.duration_since(entry.last_failure) > max_stale;
            !(is_past_retry && is_stale)
        });
    }

    /// Get the number of tracked entries.
    #[allow(dead_code)]
    pub fn len(&self) -> usize {
        self.entries.len()
    }

    /// Check if there are no tracked entries.
    #[allow(dead_code)]
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    /// Evict oldest entries if we exceed max_entries.
    fn evict_if_needed(&mut self) {
        while self.entries.len() > self.max_entries {
            let oldest = self
                .entries
                .iter()
                .min_by_key(|(_, entry)| entry.last_failure)
                .map(|(k, _)| k.clone());

            if let Some(key) = oldest {
                self.entries.remove(&key);
            } else {
                break;
            }
        }
    }

    /// Get a reference to the backoff configuration.
    pub fn config(&self) -> &ExponentialBackoff {
        &self.config
    }
}

impl<K: Eq + Hash + Clone> Default for TrackedBackoff<K> {
    fn default() -> Self {
        Self::with_defaults()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_exponential_delay_zero_indexed() {
        let backoff = ExponentialBackoff::new(Duration::from_secs(1), Duration::from_secs(60));

        // 0-indexed: delay(n) = base * 2^n
        assert_eq!(backoff.delay(0), Duration::from_secs(1)); // 1 * 2^0 = 1
        assert_eq!(backoff.delay(1), Duration::from_secs(2)); // 1 * 2^1 = 2
        assert_eq!(backoff.delay(2), Duration::from_secs(4)); // 1 * 2^2 = 4
        assert_eq!(backoff.delay(3), Duration::from_secs(8)); // 1 * 2^3 = 8
        assert_eq!(backoff.delay(4), Duration::from_secs(16)); // 1 * 2^4 = 16
        assert_eq!(backoff.delay(5), Duration::from_secs(32)); // 1 * 2^5 = 32
        assert_eq!(backoff.delay(6), Duration::from_secs(60)); // capped at max
    }

    #[test]
    fn test_delay_for_failures_one_indexed() {
        let backoff = ExponentialBackoff::new(Duration::from_secs(1), Duration::from_secs(300));

        // 1-indexed: delay_for_failures(n) = base * 2^(n-1)
        assert_eq!(backoff.delay_for_failures(0), Duration::ZERO);
        assert_eq!(backoff.delay_for_failures(1), Duration::from_secs(1)); // 1 * 2^0 = 1
        assert_eq!(backoff.delay_for_failures(2), Duration::from_secs(2)); // 1 * 2^1 = 2
        assert_eq!(backoff.delay_for_failures(3), Duration::from_secs(4)); // 1 * 2^2 = 4
        assert_eq!(backoff.delay_for_failures(4), Duration::from_secs(8)); // 1 * 2^3 = 8
    }

    #[test]
    fn test_backoff_capped_at_max() {
        let backoff = ExponentialBackoff::new(Duration::from_secs(10), Duration::from_secs(60));

        // 10 * 2^6 = 640, but should be capped at 60
        assert_eq!(backoff.delay(6), Duration::from_secs(60));
        assert_eq!(backoff.delay(10), Duration::from_secs(60));
        assert_eq!(backoff.delay(100), Duration::from_secs(60));
    }

    #[test]
    fn test_tracked_backoff_not_in_backoff_initially() {
        let tracker: TrackedBackoff<String> = TrackedBackoff::with_defaults();
        assert!(!tracker.is_in_backoff(&"test".to_string()));
    }

    #[test]
    fn test_tracked_backoff_after_failure() {
        let mut tracker: TrackedBackoff<String> = TrackedBackoff::with_defaults();
        let key = "test".to_string();

        tracker.record_failure(key.clone());
        assert!(tracker.is_in_backoff(&key));
        assert_eq!(tracker.failure_count(&key), 1);
    }

    #[test]
    fn test_tracked_backoff_cleared_on_success() {
        let mut tracker: TrackedBackoff<String> = TrackedBackoff::with_defaults();
        let key = "test".to_string();

        tracker.record_failure(key.clone());
        assert!(tracker.is_in_backoff(&key));

        tracker.record_success(&key);
        assert!(!tracker.is_in_backoff(&key));
        assert_eq!(tracker.failure_count(&key), 0);
    }

    #[test]
    fn test_tracked_backoff_consecutive_failures() {
        let config = ExponentialBackoff::new(Duration::from_secs(1), Duration::from_secs(300));
        let mut tracker: TrackedBackoff<String> = TrackedBackoff::new(config, 100);
        let key = "test".to_string();

        tracker.record_failure(key.clone());
        assert_eq!(tracker.failure_count(&key), 1);

        tracker.record_failure(key.clone());
        assert_eq!(tracker.failure_count(&key), 2);

        tracker.record_failure(key.clone());
        assert_eq!(tracker.failure_count(&key), 3);
    }

    #[test]
    fn test_tracked_backoff_eviction() {
        let config = ExponentialBackoff::new(Duration::from_secs(1), Duration::from_secs(300));
        let mut tracker: TrackedBackoff<u32> = TrackedBackoff::new(config, 5);

        // Add 10 entries
        for i in 0..10 {
            tracker.record_failure(i);
        }

        // Should have at most 5
        assert!(tracker.len() <= 5);
    }

    #[test]
    fn test_different_keys_tracked_separately() {
        let mut tracker: TrackedBackoff<String> = TrackedBackoff::with_defaults();

        tracker.record_failure("key1".to_string());

        assert!(tracker.is_in_backoff(&"key1".to_string()));
        assert!(!tracker.is_in_backoff(&"key2".to_string()));
    }

    #[test]
    fn test_remaining_backoff() {
        let config = ExponentialBackoff::new(Duration::from_secs(10), Duration::from_secs(300));
        let mut tracker: TrackedBackoff<String> = TrackedBackoff::new(config, 100);
        let key = "test".to_string();

        // No backoff initially
        assert!(tracker.remaining_backoff(&key).is_none());

        // After failure, should have remaining backoff
        tracker.record_failure(key.clone());
        let remaining = tracker.remaining_backoff(&key);
        assert!(remaining.is_some());
        // Should be close to 10 seconds
        assert!(remaining.unwrap() <= Duration::from_secs(10));
        assert!(remaining.unwrap() >= Duration::from_secs(9));
    }
}
