//! Exponential backoff for failed connection attempts.
//!
//! When connection attempts fail (either due to routing failure or connect operation failure),
//! we apply exponential backoff before retrying to avoid spamming small/saturated networks.

use std::collections::HashMap;
use std::time::{Duration, Instant};

use super::Location;

/// Tracks backoff state for failed connection targets.
///
/// Uses exponential backoff: `base_interval * 2^consecutive_failures` capped at `max_backoff`.
#[derive(Debug)]
pub struct ConnectionBackoff {
    /// Failed targets with their backoff state
    failed_targets: HashMap<LocationBucket, BackoffState>,
    /// Base backoff interval (first retry delay)
    base_interval: Duration,
    /// Maximum backoff interval (cap)
    max_backoff: Duration,
    /// Maximum number of tracked entries (LRU eviction when exceeded)
    max_entries: usize,
}

/// Backoff state for a target location.
#[derive(Debug, Clone)]
struct BackoffState {
    /// Number of consecutive failures
    consecutive_failures: u32,
    /// When the last failure occurred
    last_failure: Instant,
    /// When retry is allowed
    retry_after: Instant,
}

/// Bucket for location - we group nearby locations to avoid tracking too many entries.
/// Uses 256 buckets across the [0, 1) ring.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
struct LocationBucket(u8);

impl LocationBucket {
    fn from_location(loc: Location) -> Self {
        // Location is in [0, 1), multiply by 256 to get bucket
        let bucket = (loc.as_f64() * 256.0) as u8;
        Self(bucket)
    }
}

impl Default for ConnectionBackoff {
    fn default() -> Self {
        Self::new()
    }
}

impl ConnectionBackoff {
    /// Default base backoff interval (5 seconds)
    const DEFAULT_BASE_INTERVAL: Duration = Duration::from_secs(5);

    /// Default maximum backoff interval (5 minutes)
    const DEFAULT_MAX_BACKOFF: Duration = Duration::from_secs(300);

    /// Default maximum number of tracked entries
    const DEFAULT_MAX_ENTRIES: usize = 256;

    /// Create a new backoff tracker with default settings.
    pub fn new() -> Self {
        Self {
            failed_targets: HashMap::new(),
            base_interval: Self::DEFAULT_BASE_INTERVAL,
            max_backoff: Self::DEFAULT_MAX_BACKOFF,
            max_entries: Self::DEFAULT_MAX_ENTRIES,
        }
    }

    /// Create a new backoff tracker with custom settings.
    #[cfg(test)]
    pub fn with_config(base_interval: Duration, max_backoff: Duration, max_entries: usize) -> Self {
        Self {
            failed_targets: HashMap::new(),
            base_interval,
            max_backoff,
            max_entries,
        }
    }

    /// Check if a target location is currently in backoff.
    ///
    /// Returns `true` if we should skip this target, `false` if we can attempt connection.
    pub fn is_in_backoff(&self, target: Location) -> bool {
        let bucket = LocationBucket::from_location(target);
        if let Some(state) = self.failed_targets.get(&bucket) {
            Instant::now() < state.retry_after
        } else {
            false
        }
    }

    /// Record a connection failure for a target location.
    ///
    /// Increments the failure count and calculates the next retry time.
    pub fn record_failure(&mut self, target: Location) {
        let bucket = LocationBucket::from_location(target);
        let now = Instant::now();

        // Get or create the state and update failure count
        let consecutive_failures = {
            let state = self.failed_targets.entry(bucket).or_insert(BackoffState {
                consecutive_failures: 0,
                last_failure: now,
                retry_after: now,
            });
            state.consecutive_failures = state.consecutive_failures.saturating_add(1);
            state.last_failure = now;
            state.consecutive_failures
        };

        // Calculate backoff: base * 2^failures, capped at max
        let backoff = self.calculate_backoff(consecutive_failures);

        // Update retry_after
        if let Some(state) = self.failed_targets.get_mut(&bucket) {
            state.retry_after = now + backoff;
        }

        tracing::debug!(
            bucket = bucket.0,
            failures = consecutive_failures,
            backoff_secs = backoff.as_secs(),
            "Connection target in backoff"
        );

        // Evict oldest entries if we exceed max
        self.evict_if_needed();
    }

    /// Record a successful connection to a target location.
    ///
    /// Clears the backoff state for that location bucket.
    pub fn record_success(&mut self, target: Location) {
        let bucket = LocationBucket::from_location(target);
        if self.failed_targets.remove(&bucket).is_some() {
            tracing::debug!(bucket = bucket.0, "Connection target backoff cleared");
        }
    }

    /// Calculate backoff duration for a given failure count.
    fn calculate_backoff(&self, consecutive_failures: u32) -> Duration {
        // Cap the exponent to avoid overflow
        let exponent = consecutive_failures.min(10);
        let multiplier = 1u64 << exponent; // 2^exponent
        let backoff = self.base_interval.saturating_mul(multiplier as u32);

        // Cap at max backoff
        if backoff > self.max_backoff {
            self.max_backoff
        } else {
            backoff
        }
    }

    /// Evict oldest entries if we exceed max_entries.
    fn evict_if_needed(&mut self) {
        if self.failed_targets.len() <= self.max_entries {
            return;
        }

        // Find and remove the entry with oldest last_failure
        let oldest = self
            .failed_targets
            .iter()
            .min_by_key(|(_, state)| state.last_failure)
            .map(|(bucket, _)| *bucket);

        if let Some(bucket) = oldest {
            self.failed_targets.remove(&bucket);
        }
    }

    /// Clean up expired backoff entries (those past their retry time with low failure count).
    ///
    /// Called periodically to prevent unbounded growth.
    pub fn cleanup_expired(&mut self) {
        let now = Instant::now();

        // Remove entries that:
        // 1. Are past their retry time AND
        // 2. Have been in backoff for at least max_backoff (stale)
        self.failed_targets.retain(|_, state| {
            let is_past_retry = now >= state.retry_after;
            let is_stale = now.duration_since(state.last_failure) > self.max_backoff;
            !(is_past_retry && is_stale)
        });
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_backoff_not_in_backoff_initially() {
        let backoff = ConnectionBackoff::new();
        let loc = Location::new(0.5);
        assert!(!backoff.is_in_backoff(loc));
    }

    #[test]
    fn test_backoff_after_failure() {
        let mut backoff = ConnectionBackoff::new();
        let loc = Location::new(0.5);

        backoff.record_failure(loc);
        assert!(backoff.is_in_backoff(loc));
    }

    #[test]
    fn test_backoff_cleared_on_success() {
        let mut backoff = ConnectionBackoff::new();
        let loc = Location::new(0.5);

        backoff.record_failure(loc);
        assert!(backoff.is_in_backoff(loc));

        backoff.record_success(loc);
        assert!(!backoff.is_in_backoff(loc));
    }

    #[test]
    fn test_exponential_backoff_calculation() {
        let backoff =
            ConnectionBackoff::with_config(Duration::from_secs(1), Duration::from_secs(300), 256);

        // 2^0 = 1
        assert_eq!(backoff.calculate_backoff(1), Duration::from_secs(2));
        // 2^1 = 2
        assert_eq!(backoff.calculate_backoff(2), Duration::from_secs(4));
        // 2^2 = 4
        assert_eq!(backoff.calculate_backoff(3), Duration::from_secs(8));
        // 2^3 = 8
        assert_eq!(backoff.calculate_backoff(4), Duration::from_secs(16));
    }

    #[test]
    fn test_backoff_capped_at_max() {
        let backoff = ConnectionBackoff::with_config(
            Duration::from_secs(10),
            Duration::from_secs(60), // Max 60 seconds
            256,
        );

        // After many failures, should be capped at 60s
        assert_eq!(backoff.calculate_backoff(10), Duration::from_secs(60));
        assert_eq!(backoff.calculate_backoff(20), Duration::from_secs(60));
    }

    #[test]
    fn test_nearby_locations_share_bucket() {
        let mut backoff = ConnectionBackoff::new();

        // These should be in the same bucket (0.500 and 0.501)
        let loc1 = Location::new(0.500);
        let loc2 = Location::new(0.501);

        backoff.record_failure(loc1);
        // loc2 should also be in backoff since they share a bucket
        assert!(backoff.is_in_backoff(loc2));
    }

    #[test]
    fn test_distant_locations_different_buckets() {
        let mut backoff = ConnectionBackoff::new();

        // These should be in different buckets
        let loc1 = Location::new(0.1);
        let loc2 = Location::new(0.9);

        backoff.record_failure(loc1);
        // loc2 should NOT be in backoff
        assert!(!backoff.is_in_backoff(loc2));
    }

    #[test]
    fn test_eviction_when_max_entries_exceeded() {
        let mut backoff = ConnectionBackoff::with_config(
            Duration::from_secs(5),
            Duration::from_secs(300),
            10, // Very low max for testing
        );

        // Add more than max entries
        for i in 0..20 {
            let loc = Location::new(i as f64 / 256.0);
            backoff.record_failure(loc);
        }

        // Should have at most max_entries
        assert!(backoff.failed_targets.len() <= 10);
    }

    #[test]
    fn test_consecutive_failures_increase_backoff() {
        let mut backoff =
            ConnectionBackoff::with_config(Duration::from_secs(1), Duration::from_secs(300), 256);

        let loc = Location::new(0.5);
        let bucket = LocationBucket::from_location(loc);

        // First failure
        backoff.record_failure(loc);
        let first_failures = backoff
            .failed_targets
            .get(&bucket)
            .unwrap()
            .consecutive_failures;

        // Second failure
        backoff.record_failure(loc);
        let second_failures = backoff
            .failed_targets
            .get(&bucket)
            .unwrap()
            .consecutive_failures;

        assert_eq!(first_failures, 1);
        assert_eq!(second_failures, 2);
    }
}
