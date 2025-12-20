//! Global bandwidth management for fair sharing across connections.
//!
//! This module provides a lock-free mechanism for distributing a total bandwidth
//! budget across multiple concurrent connections. Rather than using hierarchical
//! token buckets (which can interfere with LEDBAT), we use simple rate derivation:
//!
//! `per_connection_rate = total_limit / active_connections`
//!
//! Each connection's TokenBucket is updated to use this derived rate, while LEDBAT
//! remains the primary congestion controller.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

/// Default minimum bandwidth per connection (1 MB/s)
pub const DEFAULT_MIN_PER_CONNECTION: usize = 1_000_000;

/// Default total bandwidth limit (50 MB/s).
/// Used as a reasonable default when no explicit limit is configured.
pub const DEFAULT_TOTAL_LIMIT: usize = 50_000_000;

/// Global bandwidth manager using atomic connection counting.
///
/// This provides fair bandwidth sharing across all active connections without
/// the complexity and LEDBAT interference of hierarchical token buckets.
///
/// # How it works
///
/// 1. When a connection is created, call `register_connection()` to get its initial rate
/// 2. Periodically, connections call `current_per_connection_rate()` to check for updates
/// 3. When a connection closes, call `unregister_connection()` to rebalance
///
/// # Example
///
/// ```ignore
/// let manager = GlobalBandwidthManager::new(50_000_000, Some(1_000_000));
///
/// // Connection 1 created
/// let rate1 = manager.register_connection(); // 50 MB/s (only connection)
///
/// // Connection 2 created
/// let rate2 = manager.register_connection(); // 25 MB/s (50/2)
///
/// // Connection 1 checks for updates
/// let current = manager.current_per_connection_rate(); // 25 MB/s
///
/// // Connection 1 closes
/// manager.unregister_connection();
/// let rate2_new = manager.current_per_connection_rate(); // 50 MB/s again
/// ```
#[derive(Debug)]
pub struct GlobalBandwidthManager {
    /// Total bandwidth cap (bytes/sec)
    total_limit: usize,

    /// Active connection count (lock-free)
    connection_count: AtomicUsize,

    /// Minimum rate per connection (prevents starvation)
    min_per_connection: usize,
}

impl GlobalBandwidthManager {
    /// Create a new global bandwidth manager.
    ///
    /// # Arguments
    ///
    /// * `total_limit` - Total bandwidth in bytes/sec to distribute across connections
    /// * `min_per_connection` - Optional minimum rate per connection to prevent starvation
    pub fn new(total_limit: usize, min_per_connection: Option<usize>) -> Self {
        Self {
            total_limit,
            connection_count: AtomicUsize::new(0),
            min_per_connection: min_per_connection.unwrap_or(DEFAULT_MIN_PER_CONNECTION),
        }
    }

    /// Register a new connection and return its initial per-connection rate.
    ///
    /// Call this when a new connection is established.
    pub fn register_connection(&self) -> usize {
        let count = self.connection_count.fetch_add(1, Ordering::AcqRel) + 1;
        self.compute_per_connection_rate(count)
    }

    /// Unregister a closed connection and return the new per-connection rate.
    ///
    /// Call this when a connection is closed to rebalance bandwidth.
    /// Uses `fetch_update` to prevent atomic underflow if called when count is already 0.
    pub fn unregister_connection(&self) -> usize {
        let result =
            self.connection_count
                .fetch_update(Ordering::AcqRel, Ordering::Acquire, |count| {
                    if count > 0 {
                        Some(count - 1)
                    } else {
                        None // Don't decrement if already 0
                    }
                });

        match result {
            Ok(prev_count) => {
                // Successfully decremented from prev_count to prev_count - 1
                let new_count = prev_count.saturating_sub(1).max(1);
                self.compute_per_connection_rate(new_count)
            }
            Err(_) => {
                // Count was already 0, log warning and return rate for 1 connection
                tracing::warn!(
                    "unregister_connection called when count was already 0; \
                     possible double-unregister bug"
                );
                self.compute_per_connection_rate(1)
            }
        }
    }

    /// Get current per-connection rate (for existing connections to check for updates).
    pub fn current_per_connection_rate(&self) -> usize {
        let count = self.connection_count.load(Ordering::Acquire).max(1);
        self.compute_per_connection_rate(count)
    }

    /// Get the current number of active connections.
    pub fn connection_count(&self) -> usize {
        self.connection_count.load(Ordering::Acquire)
    }

    /// Get the total bandwidth limit.
    pub fn total_limit(&self) -> usize {
        self.total_limit
    }

    /// Get the minimum per-connection rate.
    pub fn min_per_connection(&self) -> usize {
        self.min_per_connection
    }

    /// Compute fair share per connection.
    fn compute_per_connection_rate(&self, count: usize) -> usize {
        let fair_share = self.total_limit / count.max(1);

        // Honor minimum to prevent starvation
        fair_share.max(self.min_per_connection)
    }
}

/// RAII handle that automatically unregisters a connection when dropped.
///
/// This ensures connections are properly unregistered even if the code
/// doesn't explicitly call `unregister_connection()`. Useful for external
/// consumers who want automatic cleanup.
///
/// # Note on Clone
///
/// Cloning this handle **registers a new connection**. This is intentional:
/// each handle represents a logical connection that needs its own bandwidth
/// allocation. If you need to share access to the same connection's handle,
/// wrap it in `Arc` instead of cloning.
///
/// # Example
///
/// ```ignore
/// let manager = Arc::new(GlobalBandwidthManager::new(50_000_000, None));
///
/// {
///     let handle = ConnectionBandwidthHandle::new(Arc::clone(&manager));
///     // Connection is registered, handle.current_rate() returns the rate
///
///     let cloned = handle.clone(); // This registers ANOTHER connection!
///     assert_eq!(manager.connection_count(), 2);
/// }
/// // Both handles dropped, both connections unregistered
/// assert_eq!(manager.connection_count(), 0);
/// ```
#[derive(Debug)]
pub struct ConnectionBandwidthHandle {
    manager: Arc<GlobalBandwidthManager>,
}

impl ConnectionBandwidthHandle {
    /// Create a new handle and register the connection.
    pub fn new(manager: Arc<GlobalBandwidthManager>) -> Self {
        manager.register_connection();
        Self { manager }
    }

    /// Get the current per-connection rate.
    pub fn current_rate(&self) -> usize {
        self.manager.current_per_connection_rate()
    }

    /// Get reference to the manager.
    pub fn manager(&self) -> &GlobalBandwidthManager {
        &self.manager
    }
}

impl Drop for ConnectionBandwidthHandle {
    fn drop(&mut self) {
        self.manager.unregister_connection();
    }
}

impl Clone for ConnectionBandwidthHandle {
    fn clone(&self) -> Self {
        // Cloning registers a new connection
        Self::new(Arc::clone(&self.manager))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_single_connection() {
        let manager = GlobalBandwidthManager::new(50_000_000, Some(1_000_000));

        let rate = manager.register_connection();
        assert_eq!(rate, 50_000_000); // Single connection gets full bandwidth
        assert_eq!(manager.connection_count(), 1);

        manager.unregister_connection();
        assert_eq!(manager.connection_count(), 0);
    }

    #[test]
    fn test_multiple_connections_fair_share() {
        let manager = GlobalBandwidthManager::new(50_000_000, Some(1_000_000));

        // First connection
        let rate1 = manager.register_connection();
        assert_eq!(rate1, 50_000_000);

        // Second connection - both should get 25 MB/s
        let rate2 = manager.register_connection();
        assert_eq!(rate2, 25_000_000);

        // Check current rate (both connections see the same)
        assert_eq!(manager.current_per_connection_rate(), 25_000_000);
        assert_eq!(manager.connection_count(), 2);

        // Third connection - each gets ~16.6 MB/s
        let rate3 = manager.register_connection();
        assert_eq!(rate3, 16_666_666); // 50M / 3

        // Remove one connection
        manager.unregister_connection();
        assert_eq!(manager.current_per_connection_rate(), 25_000_000);
    }

    #[test]
    fn test_minimum_enforcement() {
        let manager = GlobalBandwidthManager::new(10_000_000, Some(5_000_000));

        // Even with 10 connections, min is enforced
        for _ in 0..10 {
            manager.register_connection();
        }

        // 10M / 10 = 1M, but min is 5M
        assert_eq!(manager.current_per_connection_rate(), 5_000_000);
    }

    #[test]
    fn test_connection_handle_auto_unregister() {
        let manager = Arc::new(GlobalBandwidthManager::new(50_000_000, None));

        {
            let _handle1 = ConnectionBandwidthHandle::new(Arc::clone(&manager));
            assert_eq!(manager.connection_count(), 1);

            {
                let _handle2 = ConnectionBandwidthHandle::new(Arc::clone(&manager));
                assert_eq!(manager.connection_count(), 2);
            }
            // handle2 dropped
            assert_eq!(manager.connection_count(), 1);
        }
        // handle1 dropped
        assert_eq!(manager.connection_count(), 0);
    }

    #[test]
    fn test_handle_current_rate() {
        let manager = Arc::new(GlobalBandwidthManager::new(50_000_000, None));

        let handle1 = ConnectionBandwidthHandle::new(Arc::clone(&manager));
        assert_eq!(handle1.current_rate(), 50_000_000);

        let handle2 = ConnectionBandwidthHandle::new(Arc::clone(&manager));
        assert_eq!(handle1.current_rate(), 25_000_000);
        assert_eq!(handle2.current_rate(), 25_000_000);

        drop(handle2);
        assert_eq!(handle1.current_rate(), 50_000_000);
    }

    #[test]
    fn test_underflow_protection() {
        let manager = GlobalBandwidthManager::new(50_000_000, None);

        // Unregister without registering (shouldn't panic or underflow)
        let rate = manager.unregister_connection();
        assert!(rate > 0); // Should still return a valid rate

        // Count should stay at 0, not wrap to usize::MAX
        assert_eq!(manager.connection_count(), 0);

        // Multiple unregisters should all be safe
        manager.unregister_connection();
        manager.unregister_connection();
        assert_eq!(manager.connection_count(), 0);
    }

    #[test]
    fn test_handle_clone_registers_new_connection() {
        let manager = Arc::new(GlobalBandwidthManager::new(50_000_000, None));

        let handle1 = ConnectionBandwidthHandle::new(Arc::clone(&manager));
        assert_eq!(manager.connection_count(), 1);

        // Clone should register a NEW connection (not share the same one)
        let handle2 = handle1.clone();
        assert_eq!(
            manager.connection_count(),
            2,
            "Clone should register new connection"
        );

        // Both see the fair share rate
        assert_eq!(handle1.current_rate(), 25_000_000);
        assert_eq!(handle2.current_rate(), 25_000_000);

        drop(handle1);
        assert_eq!(manager.connection_count(), 1);
        assert_eq!(handle2.current_rate(), 50_000_000);

        drop(handle2);
        assert_eq!(manager.connection_count(), 0);
    }

    #[test]
    fn test_concurrent_register_unregister() {
        use std::thread;

        let manager = Arc::new(GlobalBandwidthManager::new(50_000_000, Some(1_000_000)));
        let mut handles = vec![];

        // Spawn 50 threads, each registering and unregistering 100 times
        for _ in 0..50 {
            let mgr = Arc::clone(&manager);
            let handle = thread::spawn(move || {
                for _ in 0..100 {
                    let rate = mgr.register_connection();
                    assert!(rate > 0, "Rate should be positive");
                    // Small work to increase chance of interleaving
                    std::hint::black_box(rate);
                    mgr.unregister_connection();
                }
            });
            handles.push(handle);
        }

        for handle in handles {
            handle.join().expect("Thread should not panic");
        }

        // After all threads finish, count should be exactly 0
        assert_eq!(
            manager.connection_count(),
            0,
            "All connections should be unregistered after concurrent operations"
        );
    }

    #[test]
    fn test_concurrent_rate_queries_during_churn() {
        use std::sync::atomic::{AtomicBool, Ordering as AtomicOrdering};
        use std::thread;
        use std::time::Duration;

        let manager = Arc::new(GlobalBandwidthManager::new(50_000_000, Some(1_000_000)));
        let running = Arc::new(AtomicBool::new(true));
        let mut handles = vec![];

        // Spawner thread: constantly registers/unregisters connections
        let mgr_clone = Arc::clone(&manager);
        let running_clone = Arc::clone(&running);
        let spawner = thread::spawn(move || {
            while running_clone.load(AtomicOrdering::Acquire) {
                mgr_clone.register_connection();
                thread::sleep(Duration::from_micros(10));
                mgr_clone.unregister_connection();
            }
        });

        // Query threads: constantly read current rate
        for _ in 0..10 {
            let mgr = Arc::clone(&manager);
            let running_clone = Arc::clone(&running);
            let handle = thread::spawn(move || {
                let mut iterations = 0;
                while running_clone.load(AtomicOrdering::Acquire) && iterations < 1000 {
                    let rate = mgr.current_per_connection_rate();
                    assert!(rate > 0, "Rate should always be positive");
                    iterations += 1;
                }
            });
            handles.push(handle);
        }

        // Let it run for a bit
        thread::sleep(Duration::from_millis(50));
        running.store(false, AtomicOrdering::Release);

        for handle in handles {
            handle.join().expect("Query thread should not panic");
        }
        spawner.join().expect("Spawner thread should not panic");
    }

    #[test]
    fn test_large_connection_count() {
        let manager = GlobalBandwidthManager::new(50_000_000, Some(1_000));

        // Register 10,000 connections
        for _ in 0..10_000 {
            manager.register_connection();
        }

        assert_eq!(manager.connection_count(), 10_000);

        // With 10K connections and 50MB total, fair share would be 5KB/sec
        // But minimum is 1KB, so each gets 5KB (which is > 1KB minimum)
        let rate = manager.current_per_connection_rate();
        assert_eq!(rate, 5_000, "Fair share should be 50M / 10K = 5KB");

        // Now test with higher minimum
        let manager2 = GlobalBandwidthManager::new(50_000_000, Some(10_000));
        for _ in 0..10_000 {
            manager2.register_connection();
        }
        // Fair share 5KB < minimum 10KB, so minimum is enforced
        assert_eq!(
            manager2.current_per_connection_rate(),
            10_000,
            "Minimum should be enforced"
        );
    }

    #[test]
    fn test_zero_total_limit() {
        let manager = GlobalBandwidthManager::new(0, Some(1_000_000));
        let rate = manager.register_connection();
        // Should return minimum even with 0 total (0 / 1 = 0, max with 1M = 1M)
        assert_eq!(rate, 1_000_000);
    }
}
