//! Exponential backoff for failed peer connection attempts.
//!
//! This module provides per-peer backoff tracking by socket address to prevent
//! rapid repeated connection attempts to the same peer. Unlike `ConnectionBackoff`
//! which uses location buckets, this tracks individual peers precisely.
//!
//! See issue #2484 for motivation: telemetry showed peers attempting connections
//! every 4 seconds to the same target, with 58% of attempts within 5 seconds of
//! the previous attempt.

use std::collections::HashMap;
use std::net::SocketAddr;
use std::time::{Duration, Instant};

/// Tracks backoff state for failed connection attempts to specific peers.
///
/// Uses exponential backoff: `base_interval * 2^(consecutive_failures-1)` capped at `max_backoff`.
/// First failure = base_interval, second = 2x, third = 4x, etc.
#[derive(Debug)]
pub struct PeerConnectionBackoff {
    /// Failed targets with their backoff state, keyed by socket address
    failed_peers: HashMap<SocketAddr, BackoffState>,
    /// Base backoff interval (delay after first failure)
    base_interval: Duration,
    /// Maximum backoff interval (cap)
    max_backoff: Duration,
    /// Maximum number of tracked entries (LRU eviction when exceeded)
    max_entries: usize,
}

/// Backoff state for a target peer.
#[derive(Debug, Clone)]
struct BackoffState {
    /// Number of consecutive failures
    consecutive_failures: u32,
    /// When the last failure occurred
    last_failure: Instant,
    /// When retry is allowed
    retry_after: Instant,
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
        Self {
            failed_peers: HashMap::new(),
            base_interval: Self::DEFAULT_BASE_INTERVAL,
            max_backoff: Self::DEFAULT_MAX_BACKOFF,
            max_entries: Self::DEFAULT_MAX_ENTRIES,
        }
    }

    /// Create a new backoff tracker with custom settings.
    #[cfg(test)]
    pub fn with_config(base_interval: Duration, max_backoff: Duration, max_entries: usize) -> Self {
        Self {
            failed_peers: HashMap::new(),
            base_interval,
            max_backoff,
            max_entries,
        }
    }

    /// Check if a target peer is currently in backoff.
    ///
    /// Returns `true` if we should skip this target, `false` if we can attempt connection.
    pub fn is_in_backoff(&self, peer_addr: SocketAddr) -> bool {
        if let Some(state) = self.failed_peers.get(&peer_addr) {
            Instant::now() < state.retry_after
        } else {
            false
        }
    }

    /// Get the remaining backoff duration for a peer, if any.
    ///
    /// Returns `Some(duration)` if peer is in backoff, `None` otherwise.
    pub fn remaining_backoff(&self, peer_addr: SocketAddr) -> Option<Duration> {
        self.failed_peers.get(&peer_addr).and_then(|state| {
            let now = Instant::now();
            if now < state.retry_after {
                Some(state.retry_after - now)
            } else {
                None
            }
        })
    }

    /// Record a connection failure for a target peer.
    ///
    /// Increments the failure count and calculates the next retry time.
    pub fn record_failure(&mut self, peer_addr: SocketAddr) {
        let now = Instant::now();

        // Get or create the state and update failure count
        let consecutive_failures = {
            let state = self.failed_peers.entry(peer_addr).or_insert(BackoffState {
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
        if let Some(state) = self.failed_peers.get_mut(&peer_addr) {
            state.retry_after = now + backoff;
        }

        tracing::debug!(
            peer = %peer_addr,
            failures = consecutive_failures,
            backoff_secs = backoff.as_secs(),
            "Peer connection in backoff"
        );

        // Evict oldest entries if we exceed max
        self.evict_if_needed();
    }

    /// Record a successful connection to a target peer.
    ///
    /// Clears the backoff state for that peer.
    pub fn record_success(&mut self, peer_addr: SocketAddr) {
        if self.failed_peers.remove(&peer_addr).is_some() {
            tracing::debug!(peer = %peer_addr, "Peer connection backoff cleared");
        }
    }

    /// Calculate backoff duration for a given failure count.
    ///
    /// Uses formula: base_interval * 2^(consecutive_failures - 1)
    /// - 1st failure: base_interval (5s default)
    /// - 2nd failure: 2 * base_interval (10s)
    /// - 3rd failure: 4 * base_interval (20s)
    /// - etc., capped at max_backoff
    fn calculate_backoff(&self, consecutive_failures: u32) -> Duration {
        if consecutive_failures == 0 {
            return Duration::ZERO;
        }
        // Cap the exponent to avoid overflow (consecutive_failures - 1, max 10)
        let exponent = (consecutive_failures - 1).min(10);
        let multiplier = 1u64 << exponent; // 2^(consecutive_failures - 1)
        let backoff = self.base_interval.saturating_mul(multiplier as u32);

        // Cap at max backoff
        if backoff > self.max_backoff {
            self.max_backoff
        } else {
            backoff
        }
    }

    /// Evict oldest entries until we're at or below max_entries.
    fn evict_if_needed(&mut self) {
        while self.failed_peers.len() > self.max_entries {
            // Find and remove the entry with oldest last_failure
            let oldest = self
                .failed_peers
                .iter()
                .min_by_key(|(_, state)| state.last_failure)
                .map(|(addr, _)| *addr);

            if let Some(addr) = oldest {
                self.failed_peers.remove(&addr);
            } else {
                // No entries to remove (shouldn't happen, but avoid infinite loop)
                break;
            }
        }
    }

    /// Clean up expired backoff entries (those past their retry time and stale).
    ///
    /// Removes entries that are both past their retry_after time AND have been
    /// in backoff for longer than max_backoff (i.e., stale entries that haven't
    /// had recent failures). Called periodically to prevent unbounded growth.
    pub fn cleanup_expired(&mut self) {
        let now = Instant::now();

        // Remove entries that:
        // 1. Are past their retry time AND
        // 2. Have been in backoff for at least max_backoff (stale)
        self.failed_peers.retain(|_, state| {
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
        let backoff = PeerConnectionBackoff::with_config(
            Duration::from_secs(1),
            Duration::from_secs(300),
            1024,
        );

        // Formula: base * 2^(n-1)
        // 1st failure: 1s * 2^0 = 1s
        assert_eq!(backoff.calculate_backoff(1), Duration::from_secs(1));
        // 2nd failure: 1s * 2^1 = 2s
        assert_eq!(backoff.calculate_backoff(2), Duration::from_secs(2));
        // 3rd failure: 1s * 2^2 = 4s
        assert_eq!(backoff.calculate_backoff(3), Duration::from_secs(4));
        // 4th failure: 1s * 2^3 = 8s
        assert_eq!(backoff.calculate_backoff(4), Duration::from_secs(8));
    }

    #[test]
    fn test_backoff_capped_at_max() {
        let backoff = PeerConnectionBackoff::with_config(
            Duration::from_secs(10),
            Duration::from_secs(60), // Max 60 seconds
            1024,
        );

        // After many failures, should be capped at 60s
        assert_eq!(backoff.calculate_backoff(10), Duration::from_secs(60));
        assert_eq!(backoff.calculate_backoff(20), Duration::from_secs(60));
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
        assert!(backoff.failed_peers.len() <= 10);
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
        let first_failures = backoff
            .failed_peers
            .get(&addr)
            .unwrap()
            .consecutive_failures;

        // Second failure
        backoff.record_failure(addr);
        let second_failures = backoff
            .failed_peers
            .get(&addr)
            .unwrap()
            .consecutive_failures;

        assert_eq!(first_failures, 1);
        assert_eq!(second_failures, 2);
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
