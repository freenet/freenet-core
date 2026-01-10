//! Exponential backoff for failed connection attempts by location.
//!
//! When connection attempts fail (either due to routing failure or connect operation failure),
//! we apply exponential backoff before retrying to avoid spamming small/saturated networks.
//!
//! This module uses location buckets to group nearby locations together, reducing memory
//! usage while still providing effective backoff for clustered targets.

use crate::util::backoff::{ExponentialBackoff, TrackedBackoff};
use std::time::Duration;

use super::Location;

/// Types of connection failures for backoff calculation.
///
/// Different failure types may require different backoff strategies:
/// - Routing failures suggest the network lacks peers in that direction
/// - Timeouts suggest the target is overloaded or unreachable
/// - Rejections mean the peer is at capacity (apply normal backoff)
/// - NAT/handshake failures are transient and less severe
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[allow(dead_code)] // Some variants are intentionally unused for future use
pub enum ConnectionFailureReason {
    /// Routing failed - no peer available to route through
    RoutingFailed,
    /// Connect operation timed out waiting for response
    Timeout,
    /// Remote peer rejected connection (at capacity, etc.)
    #[allow(dead_code)]
    Rejected,
    /// NAT hole punch failed
    #[allow(dead_code)]
    NatPunchFailed,
    /// Handshake error after connection established
    #[allow(dead_code)]
    HandshakeError,
    /// Transient error (network issue, retry soon)
    #[allow(dead_code)]
    TransientError,
}

/// Bucket for location - we group nearby locations to avoid tracking too many entries.
/// Uses 256 buckets across the [0, 1] ring.
///
/// Note: This intentionally groups nearby locations together. If a connection to one
/// location in a bucket fails, we'll delay retrying all locations in that bucket.
/// This is a tradeoff: it reduces memory usage and prevents rapid retries to
/// clustered locations, but may delay legitimate connections to nearby peers.
/// With 256 buckets, each covers ~0.4% of the ring.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
struct LocationBucket(u8);

impl LocationBucket {
    fn from_location(loc: Location) -> Self {
        // Location is in [0, 1], multiply by 256 and clamp to handle edge case at 1.0
        let bucket = (loc.as_f64() * 256.0).min(255.0) as u8;
        Self(bucket)
    }
}

/// Tracks backoff state for failed connection targets by location.
///
/// Uses exponential backoff: `base_interval * 2^(consecutive_failures-1)` capped at `max_backoff`.
/// First failure = base_interval, second = 2x, third = 4x, etc.
///
/// Locations are grouped into 256 buckets to reduce memory usage while still providing
/// effective backoff for nearby targets.
///
/// # Failure Escalation
///
/// Different failure types receive different backoff escalation:
/// - **Timeout failures**: Escalated (record 2 failures) - timeouts suggest peer overload
/// - **Routing failures**: Normal (record 1 failure) - indicates no routing path available
/// - **Other failures**: Normal (record 1 failure) - transient or capacity-related issues
///
/// This escalation ensures that unresponsive peers that timeout are backed off more
/// aggressively than peers that simply reject connections due to capacity.
#[derive(Debug)]
pub struct ConnectionBackoff {
    inner: TrackedBackoff<LocationBucket>,
}

impl Default for ConnectionBackoff {
    fn default() -> Self {
        Self::new()
    }
}

impl ConnectionBackoff {
    /// Default base backoff interval (30 seconds).
    ///
    /// This is set high enough that even the first failure creates meaningful backoff.
    /// Connect requests arrive approximately every 60 seconds (operation timeout interval),
    /// so a 30-second base ensures the first failure already blocks half of subsequent attempts.
    /// See issue #2595 for context.
    const DEFAULT_BASE_INTERVAL: Duration = Duration::from_secs(30);

    /// Default maximum backoff interval (10 minutes).
    ///
    /// With 30s base and exponential growth (30s → 60s → 120s → 240s → 480s → 600s),
    /// persistent failures quickly escalate to meaningful delays that prevent resource
    /// waste on known-unreachable peers. See issue #2595.
    const DEFAULT_MAX_BACKOFF: Duration = Duration::from_secs(600);

    /// Default maximum number of tracked entries
    const DEFAULT_MAX_ENTRIES: usize = 256;

    /// Escalation factor for timeout failures.
    ///
    /// Timeout failures are escalated by recording this many failures internally,
    /// which causes exponential backoff to escalate faster. A value of 2 means one
    /// timeout is treated as 2 consecutive failures, doubling the backoff delay.
    /// This ensures that unresponsive peers are backed off more aggressively than
    /// peers that simply reject due to capacity.
    const TIMEOUT_FAILURE_ESCALATION: u32 = 2;

    /// Create a new backoff tracker with default settings.
    pub fn new() -> Self {
        let config =
            ExponentialBackoff::new(Self::DEFAULT_BASE_INTERVAL, Self::DEFAULT_MAX_BACKOFF);
        Self {
            inner: TrackedBackoff::new(config, Self::DEFAULT_MAX_ENTRIES),
        }
    }

    /// Create a new backoff tracker with custom settings.
    #[cfg(test)]
    pub fn with_config(base_interval: Duration, max_backoff: Duration, max_entries: usize) -> Self {
        let config = ExponentialBackoff::new(base_interval, max_backoff);
        Self {
            inner: TrackedBackoff::new(config, max_entries),
        }
    }

    /// Check if a target location is currently in backoff.
    ///
    /// Returns `true` if we should skip this target, `false` if we can attempt connection.
    pub fn is_in_backoff(&self, target: Location) -> bool {
        let bucket = LocationBucket::from_location(target);
        self.inner.is_in_backoff(&bucket)
    }

    /// Record a connection failure for a target location.
    ///
    /// Increments the failure count and calculates the next retry time.
    /// This is a convenience method that records a routing failure (non-escalated).
    #[allow(dead_code)] // Used by tests and as backward-compatible API
    pub fn record_failure(&mut self, target: Location) {
        self.record_failure_with_reason(target, ConnectionFailureReason::RoutingFailed);
    }

    /// Record a connection failure with the given reason.
    ///
    /// Different failure types may result in different backoff calculations:
    /// - Timeout failures apply stronger backoff (escalate faster)
    /// - Transient errors apply minimal backoff
    /// - Other failures use normal exponential backoff
    pub fn record_failure_with_reason(
        &mut self,
        target: Location,
        reason: ConnectionFailureReason,
    ) {
        let bucket = LocationBucket::from_location(target);
        let failures_before = self.inner.failure_count(&bucket);

        // Apply different backoff based on failure type by recording multiple failures
        let num_failures_to_record = match reason {
            ConnectionFailureReason::Timeout => {
                // Timeout suggests overload - escalate faster using the configured escalation factor
                Self::TIMEOUT_FAILURE_ESCALATION
            }
            ConnectionFailureReason::TransientError => {
                // Transient errors resolve quickly - record just 1 failure
                1
            }
            ConnectionFailureReason::RoutingFailed
            | ConnectionFailureReason::Rejected
            | ConnectionFailureReason::NatPunchFailed
            | ConnectionFailureReason::HandshakeError => {
                // Normal exponential backoff - record 1 failure
                1
            }
        };

        // Record the appropriate number of failures to the internal tracker
        for _ in 0..num_failures_to_record {
            self.inner.record_failure(bucket);
        }

        // Calculate the actual backoff that will be applied
        let actual_failures_after = self.inner.failure_count(&bucket);
        let backoff = self
            .inner
            .config()
            .delay_for_failures(actual_failures_after);
        tracing::debug!(
            bucket = bucket.0,
            failures_before = failures_before,
            failures_after = actual_failures_after,
            reason = ?reason,
            backoff_secs = backoff.as_secs(),
            "Connection target in backoff (with reason)"
        );
    }

    /// Record a successful connection to a target location.
    ///
    /// Clears the backoff state for that location bucket.
    pub fn record_success(&mut self, target: Location) {
        let bucket = LocationBucket::from_location(target);
        if self.inner.failure_count(&bucket) > 0 {
            tracing::debug!(bucket = bucket.0, "Connection target backoff cleared");
        }
        self.inner.record_success(&bucket);
    }

    /// Clean up expired backoff entries (those past their retry time and stale).
    ///
    /// Removes entries that are both past their retry_after time AND have been
    /// in backoff for longer than max_backoff (i.e., stale entries that haven't
    /// had recent failures). Called periodically to prevent unbounded growth.
    pub fn cleanup_expired(&mut self) {
        self.inner.cleanup_expired();
    }

    /// Get the failure count for a location (for testing).
    #[cfg(test)]
    fn failure_count(&self, target: Location) -> u32 {
        let bucket = LocationBucket::from_location(target);
        self.inner.failure_count(&bucket)
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
        let config = ExponentialBackoff::new(Duration::from_secs(1), Duration::from_secs(300));

        // Formula: base * 2^(n-1) via delay_for_failures
        assert_eq!(config.delay_for_failures(1), Duration::from_secs(1));
        assert_eq!(config.delay_for_failures(2), Duration::from_secs(2));
        assert_eq!(config.delay_for_failures(3), Duration::from_secs(4));
        assert_eq!(config.delay_for_failures(4), Duration::from_secs(8));
    }

    #[test]
    fn test_backoff_capped_at_max() {
        let config = ExponentialBackoff::new(Duration::from_secs(10), Duration::from_secs(60));

        // After many failures, should be capped at 60s
        assert_eq!(config.delay_for_failures(10), Duration::from_secs(60));
        assert_eq!(config.delay_for_failures(20), Duration::from_secs(60));
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
        assert!(backoff.inner.len() <= 10);
    }

    #[test]
    fn test_consecutive_failures_increase_backoff() {
        let mut backoff =
            ConnectionBackoff::with_config(Duration::from_secs(1), Duration::from_secs(300), 256);

        let loc = Location::new(0.5);

        // First failure
        backoff.record_failure(loc);
        assert_eq!(backoff.failure_count(loc), 1);

        // Second failure
        backoff.record_failure(loc);
        assert_eq!(backoff.failure_count(loc), 2);
    }

    #[test]
    fn test_failure_reason_timeout_escalates_faster() {
        let mut backoff =
            ConnectionBackoff::with_config(Duration::from_secs(1), Duration::from_secs(300), 256);

        let loc1 = Location::new(0.5);
        let loc2 = Location::new(0.6);

        // Record single failures with different reasons
        backoff.record_failure_with_reason(loc1, ConnectionFailureReason::Timeout);
        backoff.record_failure_with_reason(loc2, ConnectionFailureReason::RoutingFailed);

        // Both should be in backoff
        assert!(backoff.is_in_backoff(loc1));
        assert!(backoff.is_in_backoff(loc2));

        // Timeout should have recorded 2 failures (escalated) while routing failure recorded 1
        assert_eq!(
            backoff.failure_count(loc1),
            2,
            "Timeout should escalate to 2 failures"
        );
        assert_eq!(
            backoff.failure_count(loc2),
            1,
            "Routing failure should stay at 1 failure"
        );

        // Verify that timeout produces longer backoff duration than routing failure
        // timeout with 2 failures: base * 2^(2-1) = 1s * 2 = 2s
        // routing_failed with 1 failure: base * 2^(1-1) = 1s * 1 = 1s
        let timeout_backoff = backoff.inner.config().delay_for_failures(2);
        let routing_backoff = backoff.inner.config().delay_for_failures(1);
        assert!(
            timeout_backoff > routing_backoff,
            "Timeout backoff should be longer"
        );
    }

    #[test]
    fn test_failure_reason_transient_error_minimal_backoff() {
        let mut backoff =
            ConnectionBackoff::with_config(Duration::from_secs(1), Duration::from_secs(300), 256);

        let loc = Location::new(0.5);

        // Record a transient error
        backoff.record_failure_with_reason(loc, ConnectionFailureReason::TransientError);

        // Should still record the failure
        assert_eq!(backoff.failure_count(loc), 1);
        // Transient errors should still be in backoff but with minimal escalation
        assert!(backoff.is_in_backoff(loc));
    }

    #[test]
    fn test_failure_reason_rejected_normal_backoff() {
        let mut backoff =
            ConnectionBackoff::with_config(Duration::from_secs(1), Duration::from_secs(300), 256);

        let loc = Location::new(0.5);

        // Record a rejection
        backoff.record_failure_with_reason(loc, ConnectionFailureReason::Rejected);

        // Should be in backoff with normal escalation
        assert_eq!(backoff.failure_count(loc), 1);
        assert!(backoff.is_in_backoff(loc));
    }

    #[test]
    fn test_all_failure_reasons_recorded() {
        let mut backoff =
            ConnectionBackoff::with_config(Duration::from_secs(1), Duration::from_secs(300), 256);

        let reasons = [
            ConnectionFailureReason::RoutingFailed,
            ConnectionFailureReason::Timeout,
            ConnectionFailureReason::Rejected,
            ConnectionFailureReason::NatPunchFailed,
            ConnectionFailureReason::HandshakeError,
            ConnectionFailureReason::TransientError,
        ];

        for (i, reason) in reasons.iter().enumerate() {
            let loc = Location::new(i as f64 / 256.0);
            backoff.record_failure_with_reason(loc, *reason);
            assert!(
                backoff.is_in_backoff(loc),
                "Location should be in backoff after {:?}",
                reason
            );
        }
    }

    #[test]
    fn test_success_clears_all_failure_reasons() {
        let mut backoff =
            ConnectionBackoff::with_config(Duration::from_secs(1), Duration::from_secs(300), 256);

        let loc = Location::new(0.5);

        // Record failure with reason
        backoff.record_failure_with_reason(loc, ConnectionFailureReason::Timeout);
        assert!(backoff.is_in_backoff(loc));

        // Success should clear it regardless of reason
        backoff.record_success(loc);
        assert!(!backoff.is_in_backoff(loc));
    }
}
