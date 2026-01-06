//! LRU+TTL cache for auto-subscriptions triggered by GET operations.
//!
//! This module implements a cache that tracks contracts for which we should maintain
//! subscriptions based on GET access patterns. The design goals are:
//!
//! 1. **Automatic subscription**: After a successful GET, auto-subscribe to receive
//!    updates for that contract, ensuring our cached state stays fresh.
//!
//! 2. **LRU eviction with TTL protection**: Contracts are evicted based on LRU order,
//!    but only after a minimum TTL has elapsed since last access. This prevents
//!    thrashing where a contract is evicted immediately after being accessed.
//!
//! 3. **Bounded memory**: The cache has a maximum entry count to prevent unbounded
//!    growth. Unlike the byte-budget `SeedingCache`, we use entry count here because
//!    subscriptions have roughly fixed overhead regardless of contract size.
//!
//! 4. **Testable time**: Uses the `TimeSource` trait for deterministic testing.

use freenet_stdlib::prelude::ContractKey;
use std::collections::{HashMap, VecDeque};
use std::time::{Duration, Instant};

use crate::util::time_source::TimeSource;

/// Whether to auto-subscribe to contracts after GET operations.
/// When enabled, peers automatically subscribe to contracts they fetch,
/// ensuring they receive updates and preventing state divergence.
pub const AUTO_SUBSCRIBE_ON_GET: bool = true;

/// Default maximum number of GET-triggered subscriptions to maintain.
pub const DEFAULT_MAX_ENTRIES: usize = 50;

/// Default minimum time before a subscription can be evicted (30 minutes).
pub const DEFAULT_MIN_TTL: Duration = Duration::from_secs(30 * 60);

/// Entry tracking a GET-triggered subscription.
#[derive(Debug, Clone)]
pub struct GetSubscriptionEntry {
    /// The contract key (stored for debugging/logging purposes)
    #[allow(dead_code)]
    pub key: ContractKey,
    /// Last time this contract was accessed (GET or UPDATE received)
    pub last_access: Instant,
}

/// LRU+TTL cache for GET-triggered auto-subscriptions.
///
/// The cache tracks contracts that were accessed via GET and should be subscribed to.
/// Eviction follows LRU order but respects a minimum TTL - contracts won't be evicted
/// until they've been in the cache for at least `min_ttl` since their last access.
///
/// This means the cache can temporarily exceed `max_entries` if all entries are
/// still within their TTL window. This is intentional to prevent thrashing.
pub struct GetSubscriptionCache<T: TimeSource> {
    /// Maximum entries before considering eviction
    max_entries: usize,
    /// Minimum time since last access before eviction is allowed
    min_ttl: Duration,
    /// LRU order - front is oldest, back is newest
    lru_order: VecDeque<ContractKey>,
    /// Entry metadata indexed by contract key
    entries: HashMap<ContractKey, GetSubscriptionEntry>,
    /// Time source for testability
    time_source: T,
}

impl<T: TimeSource> GetSubscriptionCache<T> {
    /// Create a new cache with specified limits.
    pub fn new(max_entries: usize, min_ttl: Duration, time_source: T) -> Self {
        Self {
            max_entries,
            min_ttl,
            lru_order: VecDeque::new(),
            entries: HashMap::new(),
            time_source,
        }
    }

    /// Record a GET access to a contract.
    ///
    /// If the contract is already cached, this refreshes its LRU position and timestamp.
    /// If not cached, this adds it and potentially evicts old entries.
    ///
    /// Returns contracts evicted from this local cache. Callers should NOT automatically
    /// remove subscription state for evicted contracts, as they may have active client
    /// subscriptions. The background sweep task handles proper cleanup with client checks.
    pub fn record_access(&mut self, key: ContractKey) -> Vec<ContractKey> {
        let now = self.time_source.now();
        let mut evicted = Vec::new();

        if self.entries.contains_key(&key) {
            // Already cached - refresh timestamp and LRU position
            if let Some(entry) = self.entries.get_mut(&key) {
                entry.last_access = now;
            }
            // Move to back of LRU (most recently used)
            self.lru_order.retain(|k| k != &key);
            self.lru_order.push_back(key);
        } else {
            // Not cached - need to add it
            // First, try to evict entries that exceed max_entries AND are past TTL
            while self.entries.len() >= self.max_entries {
                if let Some(oldest_key) = self.lru_order.front().cloned() {
                    if let Some(oldest_entry) = self.entries.get(&oldest_key) {
                        let age = now.saturating_duration_since(oldest_entry.last_access);
                        if age >= self.min_ttl {
                            // Entry is past TTL, safe to evict
                            self.lru_order.pop_front();
                            self.entries.remove(&oldest_key);
                            evicted.push(oldest_key);
                        } else {
                            // Oldest entry still within TTL - allow exceeding max_entries
                            // to prevent thrashing
                            break;
                        }
                    } else {
                        // Entry in LRU but not in map - clean up
                        self.lru_order.pop_front();
                    }
                } else {
                    break;
                }
            }

            // Add the new entry
            let entry = GetSubscriptionEntry {
                key,
                last_access: now,
            };
            self.entries.insert(key, entry);
            self.lru_order.push_back(key);
        }

        evicted
    }

    /// Touch/refresh an entry's timestamp without adding it if missing.
    ///
    /// Called when an UPDATE is received for a subscribed contract.
    /// This refreshes the TTL and LRU position, indicating the subscription
    /// is actively receiving updates.
    pub fn touch(&mut self, key: &ContractKey) {
        if let Some(entry) = self.entries.get_mut(key) {
            entry.last_access = self.time_source.now();
            // Move to back of LRU
            self.lru_order.retain(|k| k != key);
            self.lru_order.push_back(*key);
        }
    }

    /// Sweep for expired entries beyond max_entries limit.
    ///
    /// Returns contracts evicted from this local cache. Callers should check
    /// `has_client_subscriptions()` before removing subscription state, as
    /// evicted contracts may still have active client subscriptions.
    /// Called periodically by the background sweep task.
    pub fn sweep_expired(&mut self) -> Vec<ContractKey> {
        let now = self.time_source.now();
        let mut evicted = Vec::new();

        // Only sweep if we're over max_entries
        while self.entries.len() > self.max_entries {
            if let Some(oldest_key) = self.lru_order.front().cloned() {
                if let Some(oldest_entry) = self.entries.get(&oldest_key) {
                    let age = now.saturating_duration_since(oldest_entry.last_access);
                    if age >= self.min_ttl {
                        self.lru_order.pop_front();
                        self.entries.remove(&oldest_key);
                        evicted.push(oldest_key);
                    } else {
                        // Can't evict more - all remaining are within TTL
                        break;
                    }
                } else {
                    self.lru_order.pop_front();
                }
            } else {
                break;
            }
        }

        evicted
    }

    /// Check if a contract is in the cache.
    pub fn contains(&self, key: &ContractKey) -> bool {
        self.entries.contains_key(key)
    }

    /// Remove a contract from the cache.
    ///
    /// Called when an explicit client subscription is removed, or when
    /// unsubscribe is triggered externally.
    #[allow(dead_code)]
    pub fn remove(&mut self, key: &ContractKey) {
        self.entries.remove(key);
        self.lru_order.retain(|k| k != key);
    }

    /// Get the current number of entries.
    #[allow(dead_code)]
    pub fn len(&self) -> usize {
        self.entries.len()
    }

    /// Check if the cache is empty.
    #[allow(dead_code)]
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    /// Get the max entries limit.
    #[allow(dead_code)]
    pub fn max_entries(&self) -> usize {
        self.max_entries
    }

    /// Get the min TTL.
    #[allow(dead_code)]
    pub fn min_ttl(&self) -> Duration {
        self.min_ttl
    }

    /// Get all keys in LRU order (oldest first).
    #[cfg(test)]
    pub fn keys_lru_order(&self) -> Vec<ContractKey> {
        self.lru_order.iter().cloned().collect()
    }

    /// Get entry metadata for a contract.
    #[cfg(test)]
    pub fn get(&self, key: &ContractKey) -> Option<&GetSubscriptionEntry> {
        self.entries.get(key)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::util::time_source::SharedMockTimeSource;
    use freenet_stdlib::prelude::{CodeHash, ContractInstanceId};

    fn make_key(seed: u8) -> ContractKey {
        ContractKey::from_id_and_code(
            ContractInstanceId::new([seed; 32]),
            CodeHash::new([seed.wrapping_add(1); 32]),
        )
    }

    fn make_cache(
        max_entries: usize,
        min_ttl: Duration,
    ) -> (
        GetSubscriptionCache<SharedMockTimeSource>,
        SharedMockTimeSource,
    ) {
        let time_source = SharedMockTimeSource::new();
        let cache = GetSubscriptionCache::new(max_entries, min_ttl, time_source.clone());
        (cache, time_source)
    }

    #[test]
    fn test_empty_cache() {
        let (cache, _) = make_cache(10, Duration::from_secs(60));
        assert!(cache.is_empty());
        assert_eq!(cache.len(), 0);
        assert!(!cache.contains(&make_key(1)));
    }

    #[test]
    fn test_add_single_entry() {
        let (mut cache, _) = make_cache(10, Duration::from_secs(60));
        let key = make_key(1);

        let evicted = cache.record_access(key);

        assert!(evicted.is_empty());
        assert!(cache.contains(&key));
        assert_eq!(cache.len(), 1);
    }

    #[test]
    fn test_add_and_contains() {
        let (mut cache, _) = make_cache(10, Duration::from_secs(60));
        let key1 = make_key(1);
        let key2 = make_key(2);
        let key3 = make_key(3);

        cache.record_access(key1);
        cache.record_access(key2);

        assert!(cache.contains(&key1));
        assert!(cache.contains(&key2));
        assert!(!cache.contains(&key3));
        assert_eq!(cache.len(), 2);
    }

    #[test]
    fn test_lru_eviction_respects_min_ttl() {
        // Cache with max 2 entries, 60 second TTL
        let (mut cache, time) = make_cache(2, Duration::from_secs(60));
        let key1 = make_key(1);
        let key2 = make_key(2);
        let key3 = make_key(3);

        // Add two entries
        cache.record_access(key1);
        cache.record_access(key2);
        assert_eq!(cache.len(), 2);

        // Advance time by 30 seconds (under TTL)
        time.advance_time(Duration::from_secs(30));

        // Add third entry - should NOT evict because all entries under TTL
        let evicted = cache.record_access(key3);
        assert!(evicted.is_empty(), "Should not evict entries under TTL");
        assert_eq!(cache.len(), 3, "Cache should exceed max when all under TTL");
        assert!(cache.contains(&key1));
        assert!(cache.contains(&key2));
        assert!(cache.contains(&key3));
    }

    #[test]
    fn test_lru_eviction_after_ttl_exceeded() {
        // Cache with max 2 entries, 60 second TTL
        let (mut cache, time) = make_cache(2, Duration::from_secs(60));
        let key1 = make_key(1);
        let key2 = make_key(2);
        let key3 = make_key(3);

        // Add two entries
        cache.record_access(key1);
        cache.record_access(key2);

        // Advance time past TTL
        time.advance_time(Duration::from_secs(61));

        // Add third entry - should evict key1 (oldest)
        let evicted = cache.record_access(key3);
        assert_eq!(evicted, vec![key1]);
        assert_eq!(cache.len(), 2);
        assert!(!cache.contains(&key1));
        assert!(cache.contains(&key2));
        assert!(cache.contains(&key3));
    }

    #[test]
    fn test_access_refreshes_lru_position() {
        let (mut cache, time) = make_cache(2, Duration::from_secs(60));
        let key1 = make_key(1);
        let key2 = make_key(2);
        let key3 = make_key(3);

        // Add two entries
        cache.record_access(key1);
        time.advance_time(Duration::from_secs(1));
        cache.record_access(key2);

        // LRU order should be [key1, key2]
        assert_eq!(cache.keys_lru_order(), vec![key1, key2]);

        // Access key1 again - should move to back
        cache.record_access(key1);

        // LRU order should now be [key2, key1]
        assert_eq!(cache.keys_lru_order(), vec![key2, key1]);

        // Advance past TTL and add key3 - should evict key2 (now oldest)
        time.advance_time(Duration::from_secs(61));
        let evicted = cache.record_access(key3);

        assert_eq!(evicted, vec![key2]);
        assert!(cache.contains(&key1));
        assert!(!cache.contains(&key2));
        assert!(cache.contains(&key3));
    }

    #[test]
    fn test_touch_refreshes_ttl() {
        let (mut cache, time) = make_cache(2, Duration::from_secs(60));
        let key1 = make_key(1);
        let key2 = make_key(2);
        let key3 = make_key(3);

        // Add two entries
        cache.record_access(key1);
        cache.record_access(key2);

        // Advance time by 50 seconds
        time.advance_time(Duration::from_secs(50));

        // Touch key1 (simulating UPDATE received)
        cache.touch(&key1);

        // Advance another 15 seconds (key1 now at 15s, key2 at 65s)
        time.advance_time(Duration::from_secs(15));

        // Add key3 - should evict key2 (past TTL), NOT key1 (recently touched)
        let evicted = cache.record_access(key3);

        assert_eq!(evicted, vec![key2], "Should evict key2 which is past TTL");
        assert!(
            cache.contains(&key1),
            "key1 should remain (touched recently)"
        );
        assert!(cache.contains(&key3));
    }

    #[test]
    fn test_sweep_expired_finds_old_entries() {
        let (mut cache, time) = make_cache(2, Duration::from_secs(60));
        let key1 = make_key(1);
        let key2 = make_key(2);
        let key3 = make_key(3);

        // Add three entries (exceeds max)
        cache.record_access(key1);
        cache.record_access(key2);
        cache.record_access(key3);
        assert_eq!(cache.len(), 3);

        // Sweep immediately - nothing should be evicted (all under TTL)
        let evicted = cache.sweep_expired();
        assert!(evicted.is_empty());
        assert_eq!(cache.len(), 3);

        // Advance past TTL
        time.advance_time(Duration::from_secs(61));

        // Sweep should evict oldest entry to get back to max_entries
        let evicted = cache.sweep_expired();
        assert_eq!(evicted, vec![key1]);
        assert_eq!(cache.len(), 2);
    }

    #[test]
    fn test_cache_can_exceed_max_if_all_under_ttl() {
        let (mut cache, _) = make_cache(2, Duration::from_secs(60));
        let key1 = make_key(1);
        let key2 = make_key(2);
        let key3 = make_key(3);
        let key4 = make_key(4);

        // Add four entries in quick succession (all under TTL)
        cache.record_access(key1);
        cache.record_access(key2);
        cache.record_access(key3);
        cache.record_access(key4);

        // Cache should exceed max because all entries are under TTL
        assert_eq!(
            cache.len(),
            4,
            "Cache should allow exceeding max when all entries under TTL"
        );
        assert!(cache.contains(&key1));
        assert!(cache.contains(&key2));
        assert!(cache.contains(&key3));
        assert!(cache.contains(&key4));
    }

    #[test]
    fn test_touch_non_existent_is_no_op() {
        let (mut cache, _) = make_cache(10, Duration::from_secs(60));
        let key = make_key(1);

        // Touch a key that doesn't exist
        cache.touch(&key);

        // Should remain empty
        assert!(cache.is_empty());
        assert!(!cache.contains(&key));
    }

    #[test]
    fn test_remove() {
        let (mut cache, _) = make_cache(10, Duration::from_secs(60));
        let key1 = make_key(1);
        let key2 = make_key(2);

        cache.record_access(key1);
        cache.record_access(key2);
        assert_eq!(cache.len(), 2);

        cache.remove(&key1);

        assert_eq!(cache.len(), 1);
        assert!(!cache.contains(&key1));
        assert!(cache.contains(&key2));
        assert_eq!(cache.keys_lru_order(), vec![key2]);
    }

    #[test]
    fn test_remove_non_existent_is_no_op() {
        let (mut cache, _) = make_cache(10, Duration::from_secs(60));
        let key1 = make_key(1);
        let key2 = make_key(2);

        cache.record_access(key1);

        // Remove key that doesn't exist
        cache.remove(&key2);

        assert_eq!(cache.len(), 1);
        assert!(cache.contains(&key1));
    }

    #[test]
    fn test_multiple_evictions_per_add() {
        // When multiple entries are past TTL and cache is full
        let (mut cache, time) = make_cache(2, Duration::from_secs(60));
        let key1 = make_key(1);
        let key2 = make_key(2);
        let key3 = make_key(3);
        let key4 = make_key(4);

        // Add two entries
        cache.record_access(key1);
        cache.record_access(key2);

        // Advance past TTL
        time.advance_time(Duration::from_secs(61));

        // Both key1 and key2 are now past TTL
        // Adding key3 should evict key1
        let evicted1 = cache.record_access(key3);
        assert_eq!(evicted1, vec![key1]);

        // Adding key4 should evict key2
        let evicted2 = cache.record_access(key4);
        assert_eq!(evicted2, vec![key2]);

        assert_eq!(cache.len(), 2);
        assert!(cache.contains(&key3));
        assert!(cache.contains(&key4));
    }

    #[test]
    fn test_duplicate_add_refreshes_timestamp() {
        let (mut cache, time) = make_cache(10, Duration::from_secs(60));
        let key = make_key(1);

        // Add entry
        cache.record_access(key);
        let first_access = cache.get(&key).unwrap().last_access;

        // Advance time
        time.advance_time(Duration::from_secs(30));

        // Add same entry again
        let evicted = cache.record_access(key);

        assert!(evicted.is_empty());
        assert_eq!(cache.len(), 1); // Still just one entry
        let second_access = cache.get(&key).unwrap().last_access;
        assert!(
            second_access > first_access,
            "Timestamp should be refreshed"
        );
    }

    #[test]
    fn test_zero_max_entries() {
        // Edge case: with max_entries = 0, every add triggers eviction check
        let (mut cache, time) = make_cache(0, Duration::from_secs(60));
        let key1 = make_key(1);
        let key2 = make_key(2);

        // First add - cache is "full" but nothing to evict
        cache.record_access(key1);
        assert_eq!(cache.len(), 1);

        // Advance past TTL
        time.advance_time(Duration::from_secs(61));

        // Second add - should evict key1
        let evicted = cache.record_access(key2);
        assert_eq!(evicted, vec![key1]);
        assert_eq!(cache.len(), 1);
    }

    #[test]
    fn test_zero_ttl() {
        // Edge case: with TTL = 0, entries can be evicted immediately
        let (mut cache, _) = make_cache(2, Duration::ZERO);
        let key1 = make_key(1);
        let key2 = make_key(2);
        let key3 = make_key(3);

        cache.record_access(key1);
        cache.record_access(key2);

        // Third entry should immediately evict oldest
        let evicted = cache.record_access(key3);
        assert_eq!(evicted, vec![key1]);
        assert_eq!(cache.len(), 2);
    }
}
