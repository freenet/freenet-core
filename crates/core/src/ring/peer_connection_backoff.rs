//! Exponential backoff for failed peer connection attempts.
//!
//! This module provides per-peer backoff tracking by socket address to prevent
//! rapid repeated connection attempts to the same peer. Unlike `ConnectionBackoff`
//! which uses location buckets, this tracks individual peers precisely.
//!
//! See issue #2484 for motivation: telemetry showed peers attempting connections
//! every 4 seconds to the same target, with 58% of attempts within 5 seconds of
//! the previous attempt.

use crate::util::backoff::{ExponentialBackoff, TrackedBackoff};
use std::net::SocketAddr;
use std::time::Duration;

/// Tracks backoff state for failed connection attempts to specific peers.
///
/// Uses exponential backoff: `base_interval * 2^(consecutive_failures-1)` capped at `max_backoff`.
/// First failure = base_interval, second = 2x, third = 4x, etc.
#[derive(Debug)]
pub struct PeerConnectionBackoff {
    inner: TrackedBackoff<SocketAddr>,
}

impl Default for PeerConnectionBackoff {
    fn default() -> Self {
        Self::new()
    }
}

impl PeerConnectionBackoff {
    /// Default base backoff interval (5 seconds)
    const DEFAULT_BASE_INTERVAL: Duration = Duration::from_secs(5);

    /// Default maximum backoff interval (5 minutes)
    const DEFAULT_MAX_BACKOFF: Duration = Duration::from_secs(300);

    /// Default maximum number of tracked entries
    const DEFAULT_MAX_ENTRIES: usize = 1024;

    /// Create a new backoff tracker with default settings.
    pub fn new() -> Self {
        let config = ExponentialBackoff::new(Self::DEFAULT_BASE_INTERVAL, Self::DEFAULT_MAX_BACKOFF);
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

    /// Check if a target peer is currently in backoff.
    ///
    /// Returns `true` if we should skip this target, `false` if we can attempt connection.
    pub fn is_in_backoff(&self, peer_addr: SocketAddr) -> bool {
        self.inner.is_in_backoff(&peer_addr)
    }

    /// Get the remaining backoff duration for a peer, if any.
    ///
    /// Returns `Some(duration)` if peer is in backoff, `None` otherwise.
    pub fn remaining_backoff(&self, peer_addr: SocketAddr) -> Option<Duration> {
        self.inner.remaining_backoff(&peer_addr)
    }

    /// Record a connection failure for a target peer.
    ///
    /// Increments the failure count and calculates the next retry time.
    pub fn record_failure(&mut self, peer_addr: SocketAddr) {
        let failures_before = self.inner.failure_count(&peer_addr);
        self.inner.record_failure(peer_addr);

        let backoff = self.inner.config().delay_for_failures(failures_before + 1);
        tracing::debug!(
            peer = %peer_addr,
            failures = failures_before + 1,
            backoff_secs = backoff.as_secs(),
            "Peer connection in backoff"
        );
    }

    /// Record a successful connection to a target peer.
    ///
    /// Clears the backoff state for that peer.
    pub fn record_success(&mut self, peer_addr: SocketAddr) {
        if self.inner.failure_count(&peer_addr) > 0 {
            tracing::debug!(peer = %peer_addr, "Peer connection backoff cleared");
        }
        self.inner.record_success(&peer_addr);
    }

    /// Clean up expired backoff entries (those past their retry time and stale).
    ///
    /// Removes entries that are both past their retry_after time AND have been
    /// in backoff for longer than max_backoff (i.e., stale entries that haven't
    /// had recent failures). Called periodically to prevent unbounded growth.
    pub fn cleanup_expired(&mut self) {
        self.inner.cleanup_expired();
    }

    /// Get the consecutive failure count for a peer (for testing).
    #[cfg(test)]
    fn failure_count(&self, peer_addr: SocketAddr) -> u32 {
        self.inner.failure_count(&peer_addr)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_not_in_backoff_initially() {
        let backoff = PeerConnectionBackoff::new();
        let addr: SocketAddr = "127.0.0.1:8080".parse().unwrap();
        assert!(!backoff.is_in_backoff(addr));
    }

    #[test]
    fn test_in_backoff_after_failure() {
        let mut backoff = PeerConnectionBackoff::new();
        let addr: SocketAddr = "127.0.0.1:8080".parse().unwrap();

        backoff.record_failure(addr);
        assert!(backoff.is_in_backoff(addr));
    }

    #[test]
    fn test_backoff_cleared_on_success() {
        let mut backoff = PeerConnectionBackoff::new();
        let addr: SocketAddr = "127.0.0.1:8080".parse().unwrap();

        backoff.record_failure(addr);
        assert!(backoff.is_in_backoff(addr));

        backoff.record_success(addr);
        assert!(!backoff.is_in_backoff(addr));
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
    fn test_different_peers_tracked_separately() {
        let mut backoff = PeerConnectionBackoff::new();
        let addr1: SocketAddr = "127.0.0.1:8080".parse().unwrap();
        let addr2: SocketAddr = "127.0.0.1:8081".parse().unwrap();

        backoff.record_failure(addr1);

        // addr1 should be in backoff, addr2 should not
        assert!(backoff.is_in_backoff(addr1));
        assert!(!backoff.is_in_backoff(addr2));
    }

    #[test]
    fn test_eviction_when_max_entries_exceeded() {
        let mut backoff = PeerConnectionBackoff::with_config(
            Duration::from_secs(5),
            Duration::from_secs(300),
            10, // Very low max for testing
        );

        // Add more than max entries
        for i in 0..20 {
            let addr: SocketAddr = format!("127.0.0.1:{}", 8080 + i).parse().unwrap();
            backoff.record_failure(addr);
        }

        // Should have at most max_entries
        assert!(backoff.inner.len() <= 10);
    }

    #[test]
    fn test_consecutive_failures_increase_backoff() {
        let mut backoff = PeerConnectionBackoff::with_config(
            Duration::from_secs(1),
            Duration::from_secs(300),
            1024,
        );
        let addr: SocketAddr = "127.0.0.1:8080".parse().unwrap();

        // First failure
        backoff.record_failure(addr);
        assert_eq!(backoff.failure_count(addr), 1);

        // Second failure
        backoff.record_failure(addr);
        assert_eq!(backoff.failure_count(addr), 2);
    }

    #[test]
    fn test_remaining_backoff() {
        let mut backoff = PeerConnectionBackoff::with_config(
            Duration::from_secs(10),
            Duration::from_secs(300),
            1024,
        );
        let addr: SocketAddr = "127.0.0.1:8080".parse().unwrap();

        // No backoff initially
        assert!(backoff.remaining_backoff(addr).is_none());

        // After failure, should have remaining backoff
        backoff.record_failure(addr);
        let remaining = backoff.remaining_backoff(addr);
        assert!(remaining.is_some());
        // Should be close to 10 seconds (allow for small timing variance)
        assert!(remaining.unwrap() <= Duration::from_secs(10));
        assert!(remaining.unwrap() >= Duration::from_secs(9));
    }
}
