//! Unified hosting cache for contract state caching.
//!
//! This module implements a byte-budget aware LRU cache with TTL protection for hosted contracts.
//! It unifies the previously separate `SeedingCache` and `GetSubscriptionCache` into a single
//! source of truth for which contracts a peer is hosting.
//!
//! # Design Principles
//!
//! 1. **Single source of truth**: All hosted contracts are tracked in one cache
//! 2. **Resource-aware eviction**: Byte-budget LRU with TTL protection
//! 3. **Subscription renewal**: All hosted contracts get subscription renewal
//! 4. **Access type tracking**: Records how contract was accessed (GET/PUT/SUBSCRIBE)

use freenet_stdlib::prelude::ContractKey;
use std::collections::{HashMap, VecDeque};
use std::time::Duration;
use tokio::time::Instant;

use crate::util::time_source::TimeSource;

/// Default hosting cache budget: 100MB
pub const DEFAULT_HOSTING_BUDGET_BYTES: u64 = 100 * 1024 * 1024;

/// Multiplier for TTL relative to subscription renewal interval.
/// Gives this many renewal attempts before eviction if renewals keep failing.
pub const TTL_RENEWAL_MULTIPLIER: u32 = 4;

/// Default minimum TTL before a hosted contract can be evicted.
/// Computed as TTL_RENEWAL_MULTIPLIER × SUBSCRIPTION_RENEWAL_INTERVAL.
pub const DEFAULT_MIN_TTL: Duration = Duration::from_secs(
    super::SUBSCRIPTION_RENEWAL_INTERVAL.as_secs() * TTL_RENEWAL_MULTIPLIER as u64,
);

/// Type of access that adds/refreshes a contract in the hosting cache.
///
/// Only certain operations should refresh the LRU position to prevent manipulation:
/// - GET: User requesting the contract
/// - PUT: User writing new state
/// - SUBSCRIBE: User subscribing to updates
///
/// UPDATE is explicitly excluded because contract creators control when updates happen,
/// which could be abused to keep contracts cached indefinitely.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AccessType {
    Get,
    Put,
    /// Used in tests and reserved for future use when explicit SUBSCRIBE triggers hosting
    #[cfg_attr(not(test), allow(dead_code))]
    Subscribe,
}

/// Metadata about a hosted contract.
#[derive(Debug, Clone)]
pub struct HostedContract {
    /// Size of the contract state in bytes
    pub size_bytes: u64,
    /// Last time this contract was accessed (via GET/PUT/SUBSCRIBE)
    pub last_accessed: Instant,
    /// Type of the last access
    pub access_type: AccessType,
}

/// Unified hosting cache that combines byte-budget LRU with TTL protection.
///
/// This cache maintains contracts that this peer is "hosting" - keeping available
/// for the network. The cache has:
/// - Byte budget: Large contracts consume more budget
/// - TTL protection: Contracts can't be evicted until min_ttl has passed
/// - LRU ordering: Oldest contracts evicted first when over budget
///
/// # Subscription Renewal
///
/// ALL contracts in this cache should have their subscriptions renewed automatically.
/// This is the key fix for the bug where GET-triggered subscriptions weren't being renewed.
pub struct HostingCache<T: TimeSource> {
    /// Maximum bytes to use for cached contracts
    budget_bytes: u64,
    /// Current total bytes used
    current_bytes: u64,
    /// Minimum time since last access before eviction is allowed
    min_ttl: Duration,
    /// LRU order - front is oldest, back is newest
    lru_order: VecDeque<ContractKey>,
    /// Contract metadata indexed by key
    contracts: HashMap<ContractKey, HostedContract>,
    /// Time source for testability
    time_source: T,
}

impl<T: TimeSource> HostingCache<T> {
    /// Create a new hosting cache with the given byte budget and TTL.
    pub fn new(budget_bytes: u64, min_ttl: Duration, time_source: T) -> Self {
        Self {
            budget_bytes,
            current_bytes: 0,
            min_ttl,
            lru_order: VecDeque::new(),
            contracts: HashMap::new(),
            time_source,
        }
    }

    /// Record an access to a contract, adding or refreshing it in the cache.
    ///
    /// If the contract is already cached, this refreshes its LRU position and timestamp.
    /// If not cached, this adds it and evicts old contracts if necessary.
    ///
    /// Returns the list of contracts that were evicted to make room (if any).
    /// Eviction respects TTL: contracts won't be evicted until min_ttl has passed.
    pub fn record_access(
        &mut self,
        key: ContractKey,
        size_bytes: u64,
        access_type: AccessType,
    ) -> Vec<ContractKey> {
        let now = self.time_source.now();
        let mut evicted = Vec::new();

        if let Some(existing) = self.contracts.get_mut(&key) {
            // Already cached - update size if changed and refresh position
            if existing.size_bytes != size_bytes {
                // Adjust byte accounting: add new size, subtract old size
                self.current_bytes = self
                    .current_bytes
                    .saturating_add(size_bytes)
                    .saturating_sub(existing.size_bytes);
                existing.size_bytes = size_bytes;
            }
            existing.last_accessed = now;
            existing.access_type = access_type;

            // Move to back of LRU (most recently used)
            self.lru_order.retain(|k| k != &key);
            self.lru_order.push_back(key);
        } else {
            // Not cached - need to add it
            // First, evict until we have room (respecting TTL)
            while self.current_bytes + size_bytes > self.budget_bytes && !self.lru_order.is_empty()
            {
                if let Some(oldest_key) = self.lru_order.front().cloned() {
                    if let Some(oldest) = self.contracts.get(&oldest_key) {
                        let age = now.saturating_duration_since(oldest.last_accessed);
                        if age >= self.min_ttl {
                            // Entry is past TTL, safe to evict
                            if let Some(removed) = self.contracts.remove(&oldest_key) {
                                self.current_bytes =
                                    self.current_bytes.saturating_sub(removed.size_bytes);
                                self.lru_order.pop_front();
                                evicted.push(oldest_key);
                            }
                        } else {
                            // Oldest entry still within TTL - allow exceeding budget
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

            // Add the new contract
            let contract = HostedContract {
                size_bytes,
                last_accessed: now,
                access_type,
            };
            self.contracts.insert(key, contract);
            self.lru_order.push_back(key);
            self.current_bytes = self.current_bytes.saturating_add(size_bytes);
        }

        evicted
    }

    /// Touch/refresh a contract's timestamp without adding it if missing.
    ///
    /// Called when UPDATE is received for a hosted contract.
    /// This refreshes the TTL and LRU position, indicating the contract
    /// is actively receiving updates.
    pub fn touch(&mut self, key: &ContractKey) {
        if let Some(existing) = self.contracts.get_mut(key) {
            existing.last_accessed = self.time_source.now();
            // Move to back of LRU
            self.lru_order.retain(|k| k != key);
            self.lru_order.push_back(*key);
        }
    }

    /// Check if a contract is in the cache.
    pub fn contains(&self, key: &ContractKey) -> bool {
        self.contracts.contains_key(key)
    }

    /// Get metadata about a hosted contract.
    #[allow(dead_code)] // Public API for introspection
    pub fn get(&self, key: &ContractKey) -> Option<&HostedContract> {
        self.contracts.get(key)
    }

    /// Get the current number of hosted contracts.
    pub fn len(&self) -> usize {
        self.contracts.len()
    }

    /// Check if the cache is empty.
    #[allow(dead_code)] // Public API for introspection
    pub fn is_empty(&self) -> bool {
        self.contracts.is_empty()
    }

    /// Get the current bytes used.
    #[allow(dead_code)] // Public API for introspection
    pub fn current_bytes(&self) -> u64 {
        self.current_bytes
    }

    /// Get the budget in bytes.
    #[allow(dead_code)] // Public API for introspection
    pub fn budget_bytes(&self) -> u64 {
        self.budget_bytes
    }

    /// Get all hosted contract keys in LRU order (oldest first).
    #[cfg(test)]
    pub fn keys_lru_order(&self) -> Vec<ContractKey> {
        self.lru_order.iter().cloned().collect()
    }

    /// Iterate over all hosted contract keys.
    pub fn iter(&self) -> impl Iterator<Item = ContractKey> + '_ {
        self.contracts.keys().cloned()
    }

    /// Sweep for contracts that are over budget and past TTL.
    ///
    /// The `should_retain` predicate is called for each candidate contract before eviction.
    /// If it returns `true`, the contract is skipped (kept in cache) even if over TTL.
    /// This allows protecting contracts with client subscriptions from eviction.
    ///
    /// Returns contracts evicted from this cache.
    pub fn sweep_expired<F>(&mut self, should_retain: F) -> Vec<ContractKey>
    where
        F: Fn(&ContractKey) -> bool,
    {
        let now = self.time_source.now();
        let mut evicted = Vec::new();
        let mut skipped_keys = Vec::new();

        // Only sweep if we're over budget
        while self.current_bytes > self.budget_bytes && !self.lru_order.is_empty() {
            if let Some(oldest_key) = self.lru_order.front().cloned() {
                if let Some(oldest) = self.contracts.get(&oldest_key) {
                    let age = now.saturating_duration_since(oldest.last_accessed);
                    if age >= self.min_ttl {
                        // Check if caller wants to retain this contract
                        if should_retain(&oldest_key) {
                            // Move to back of LRU (treat as recently accessed)
                            self.lru_order.pop_front();
                            skipped_keys.push(oldest_key);
                            continue;
                        }
                        if let Some(removed) = self.contracts.remove(&oldest_key) {
                            self.current_bytes =
                                self.current_bytes.saturating_sub(removed.size_bytes);
                            self.lru_order.pop_front();
                            evicted.push(oldest_key);
                        }
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

        // Re-add skipped keys to back of LRU order
        for key in skipped_keys {
            self.lru_order.push_back(key);
        }

        evicted
    }

    /// Load a contract entry from persisted data during startup.
    ///
    /// Unlike `record_access`, this uses a pre-computed last_accessed time
    /// and doesn't evict other contracts (we may be over budget after loading).
    ///
    /// # Arguments
    /// * `key` - The contract key
    /// * `size_bytes` - Size of the contract state
    /// * `access_type` - How the contract was last accessed (GET/PUT/SUBSCRIBE)
    /// * `last_access_age` - How long ago the contract was last accessed
    pub fn load_persisted_entry(
        &mut self,
        key: ContractKey,
        size_bytes: u64,
        access_type: AccessType,
        last_access_age: Duration,
    ) {
        // Skip if already loaded (shouldn't happen, but defensive)
        if self.contracts.contains_key(&key) {
            return;
        }

        // Calculate the last_accessed time from age
        let now = self.time_source.now();
        let last_accessed = now.checked_sub(last_access_age).unwrap_or(now);

        let contract = HostedContract {
            size_bytes,
            last_accessed,
            access_type,
        };

        self.contracts.insert(key, contract);
        self.current_bytes = self.current_bytes.saturating_add(size_bytes);
        // Note: LRU order will be sorted after all entries are loaded
    }

    /// Sort the LRU order by last_accessed time after bulk loading.
    ///
    /// Call this after `load_persisted_entry` calls are complete.
    pub fn finalize_loading(&mut self) {
        // Build LRU order from contracts sorted by last_accessed (oldest first)
        let mut entries: Vec<_> = self
            .contracts
            .iter()
            .map(|(k, v)| (*k, v.last_accessed))
            .collect();
        entries.sort_by_key(|(_, last_accessed)| *last_accessed);

        self.lru_order.clear();
        for (key, _) in entries {
            self.lru_order.push_back(key);
        }
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
        budget: u64,
        min_ttl: Duration,
    ) -> (HostingCache<SharedMockTimeSource>, SharedMockTimeSource) {
        let time_source = SharedMockTimeSource::new();
        let cache = HostingCache::new(budget, min_ttl, time_source.clone());
        (cache, time_source)
    }

    #[test]
    fn test_empty_cache() {
        let (cache, _) = make_cache(1000, Duration::from_secs(60));
        assert!(cache.is_empty());
        assert_eq!(cache.len(), 0);
        assert_eq!(cache.current_bytes(), 0);
        assert!(!cache.contains(&make_key(1)));
    }

    #[test]
    fn test_add_single_contract() {
        let (mut cache, _) = make_cache(1000, Duration::from_secs(60));
        let key = make_key(1);

        let evicted = cache.record_access(key, 100, AccessType::Get);

        assert!(evicted.is_empty());
        assert!(cache.contains(&key));
        assert_eq!(cache.len(), 1);
        assert_eq!(cache.current_bytes(), 100);

        let info = cache.get(&key).unwrap();
        assert_eq!(info.size_bytes, 100);
        assert_eq!(info.access_type, AccessType::Get);
    }

    #[test]
    fn test_refresh_existing_contract() {
        let (mut cache, time) = make_cache(1000, Duration::from_secs(60));
        let key = make_key(1);

        // First access
        cache.record_access(key, 100, AccessType::Get);
        let first_access = cache.get(&key).unwrap().last_accessed;

        // Advance time and access again
        time.advance_time(Duration::from_secs(10));
        cache.record_access(key, 100, AccessType::Put);

        // Should still be one contract, but updated
        assert_eq!(cache.len(), 1);
        assert_eq!(cache.current_bytes(), 100);

        let info = cache.get(&key).unwrap();
        assert_eq!(info.access_type, AccessType::Put);
        assert!(info.last_accessed > first_access);
    }

    #[test]
    fn test_lru_eviction_respects_ttl() {
        // Cache with max 200 bytes, 60 second TTL
        let (mut cache, time) = make_cache(200, Duration::from_secs(60));
        let key1 = make_key(1);
        let key2 = make_key(2);
        let key3 = make_key(3);

        // Add two entries
        cache.record_access(key1, 100, AccessType::Get);
        cache.record_access(key2, 100, AccessType::Get);
        assert_eq!(cache.current_bytes(), 200);

        // Advance time by 30 seconds (under TTL)
        time.advance_time(Duration::from_secs(30));

        // Add third entry - should NOT evict because all entries under TTL
        let evicted = cache.record_access(key3, 100, AccessType::Get);
        assert!(evicted.is_empty(), "Should not evict entries under TTL");
        assert_eq!(
            cache.len(),
            3,
            "Cache should exceed budget when all under TTL"
        );
        assert!(cache.contains(&key1));
        assert!(cache.contains(&key2));
        assert!(cache.contains(&key3));
    }

    #[test]
    fn test_lru_eviction_after_ttl() {
        // Cache with max 200 bytes, 60 second TTL
        let (mut cache, time) = make_cache(200, Duration::from_secs(60));
        let key1 = make_key(1);
        let key2 = make_key(2);
        let key3 = make_key(3);

        // Add two entries
        cache.record_access(key1, 100, AccessType::Get);
        cache.record_access(key2, 100, AccessType::Get);

        // Advance time past TTL
        time.advance_time(Duration::from_secs(61));

        // Add third entry - should evict key1 (oldest)
        let evicted = cache.record_access(key3, 100, AccessType::Get);
        assert_eq!(evicted, vec![key1]);
        assert_eq!(cache.len(), 2);
        assert!(!cache.contains(&key1));
        assert!(cache.contains(&key2));
        assert!(cache.contains(&key3));
    }

    #[test]
    fn test_access_refreshes_lru_position() {
        let (mut cache, time) = make_cache(200, Duration::from_secs(60));
        let key1 = make_key(1);
        let key2 = make_key(2);
        let key3 = make_key(3);

        // Add two contracts
        cache.record_access(key1, 100, AccessType::Get);
        cache.record_access(key2, 100, AccessType::Get);

        // Access key1 again - should move it to back of LRU
        cache.record_access(key1, 100, AccessType::Subscribe);

        // LRU order should now be [key2, key1]
        let order = cache.keys_lru_order();
        assert_eq!(order, vec![key2, key1]);

        // Advance past TTL and add key3 - should evict key2 (now oldest)
        time.advance_time(Duration::from_secs(61));
        let evicted = cache.record_access(key3, 100, AccessType::Get);

        assert_eq!(evicted, vec![key2]);
        assert!(cache.contains(&key1));
        assert!(!cache.contains(&key2));
        assert!(cache.contains(&key3));
    }

    #[test]
    fn test_touch_refreshes_ttl() {
        let (mut cache, time) = make_cache(200, Duration::from_secs(60));
        let key1 = make_key(1);
        let key2 = make_key(2);
        let key3 = make_key(3);

        // Add two entries
        cache.record_access(key1, 100, AccessType::Get);
        cache.record_access(key2, 100, AccessType::Get);

        // Advance time by 50 seconds
        time.advance_time(Duration::from_secs(50));

        // Touch key1 (simulating UPDATE received)
        cache.touch(&key1);

        // Advance another 15 seconds (key1 now at 15s, key2 at 65s)
        time.advance_time(Duration::from_secs(15));

        // Add key3 - should evict key2 (past TTL), NOT key1 (recently touched)
        let evicted = cache.record_access(key3, 100, AccessType::Get);

        assert_eq!(evicted, vec![key2], "Should evict key2 which is past TTL");
        assert!(
            cache.contains(&key1),
            "key1 should remain (touched recently)"
        );
        assert!(cache.contains(&key3));
    }

    #[test]
    fn test_large_contract_evicts_multiple() {
        let (mut cache, time) = make_cache(300, Duration::from_secs(60));

        let small1 = make_key(1);
        let small2 = make_key(2);
        let small3 = make_key(3);
        let large = make_key(4);

        // Add three small contracts
        cache.record_access(small1, 100, AccessType::Get);
        cache.record_access(small2, 100, AccessType::Get);
        cache.record_access(small3, 100, AccessType::Get);
        assert_eq!(cache.current_bytes(), 300);

        // Advance past TTL
        time.advance_time(Duration::from_secs(61));

        // Add one large contract - should evict two small ones
        let evicted = cache.record_access(large, 200, AccessType::Put);

        assert_eq!(evicted.len(), 2);
        assert_eq!(evicted[0], small1); // Oldest first
        assert_eq!(evicted[1], small2);
        assert!(!cache.contains(&small1));
        assert!(!cache.contains(&small2));
        assert!(cache.contains(&small3));
        assert!(cache.contains(&large));
    }

    #[test]
    fn test_sweep_expired() {
        let (mut cache, time) = make_cache(200, Duration::from_secs(60));
        let key1 = make_key(1);
        let key2 = make_key(2);
        let key3 = make_key(3);

        // Add three entries (exceeds budget)
        cache.record_access(key1, 100, AccessType::Get);
        cache.record_access(key2, 100, AccessType::Get);
        cache.record_access(key3, 100, AccessType::Get);
        assert_eq!(cache.len(), 3);
        assert_eq!(cache.current_bytes(), 300);

        // Sweep immediately - nothing should be evicted (all under TTL)
        let evicted = cache.sweep_expired(|_| false);
        assert!(evicted.is_empty());
        assert_eq!(cache.len(), 3);

        // Advance past TTL
        time.advance_time(Duration::from_secs(61));

        // Sweep should evict oldest entry to get back under budget
        let evicted = cache.sweep_expired(|_| false);
        assert_eq!(evicted, vec![key1]);
        assert_eq!(cache.current_bytes(), 200);
    }

    #[test]
    fn test_sweep_respects_should_retain() {
        let (mut cache, time) = make_cache(200, Duration::from_secs(60));
        let key1 = make_key(1);
        let key2 = make_key(2);
        let key3 = make_key(3);

        // Add three entries (exceeds budget)
        cache.record_access(key1, 100, AccessType::Get);
        cache.record_access(key2, 100, AccessType::Get);
        cache.record_access(key3, 100, AccessType::Get);

        // Advance past TTL
        time.advance_time(Duration::from_secs(61));

        // Sweep with predicate that retains key1
        let evicted = cache.sweep_expired(|k| *k == key1);

        // key1 should be retained, key2 evicted to get under budget
        assert_eq!(evicted, vec![key2]);
        assert!(cache.contains(&key1));
        assert!(!cache.contains(&key2));
        assert!(cache.contains(&key3));

        // key1 should now be at back of LRU (moved there when retained)
        assert_eq!(cache.current_bytes(), 200);
    }

    #[test]
    fn test_touch_non_existent_is_no_op() {
        let (mut cache, _) = make_cache(1000, Duration::from_secs(60));
        let key = make_key(1);

        // Touch a key that doesn't exist
        cache.touch(&key);

        // Should remain empty
        assert!(cache.is_empty());
        assert!(!cache.contains(&key));
    }

    #[test]
    fn test_access_types() {
        let (mut cache, _) = make_cache(1000, Duration::from_secs(60));
        let key = make_key(1);

        // Test each access type is recorded correctly
        cache.record_access(key, 100, AccessType::Get);
        assert_eq!(cache.get(&key).unwrap().access_type, AccessType::Get);

        cache.record_access(key, 100, AccessType::Put);
        assert_eq!(cache.get(&key).unwrap().access_type, AccessType::Put);

        cache.record_access(key, 100, AccessType::Subscribe);
        assert_eq!(cache.get(&key).unwrap().access_type, AccessType::Subscribe);
    }

    #[test]
    fn test_contract_size_change() {
        let (mut cache, _) = make_cache(1000, Duration::from_secs(60));
        let key = make_key(1);

        // Add contract with initial size
        cache.record_access(key, 100, AccessType::Get);
        assert_eq!(cache.current_bytes(), 100);
        assert_eq!(cache.get(&key).unwrap().size_bytes, 100);

        // Contract state grows
        cache.record_access(key, 200, AccessType::Put);
        assert_eq!(cache.current_bytes(), 200);
        assert_eq!(cache.get(&key).unwrap().size_bytes, 200);

        // Contract state shrinks
        cache.record_access(key, 150, AccessType::Put);
        assert_eq!(cache.current_bytes(), 150);
        assert_eq!(cache.get(&key).unwrap().size_bytes, 150);
    }
}
