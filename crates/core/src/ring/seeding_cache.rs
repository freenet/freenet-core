//! LRU-based seeding cache for contract state caching.
//!
//! This module implements byte-budget aware LRU caching for contracts. The design is based on
//! several key principles:
//!
//! 1. **Resource-aware eviction**: Large contracts consume more budget and may displace
//!    multiple small contracts when space is needed.
//!
//! 2. **Demand-driven retention**: Contracts are kept based on access patterns (GET/PUT/SUBSCRIBE),
//!    not arbitrary distance thresholds.
//!
//! 3. **Manipulation resistance**: Only GET, PUT, and SUBSCRIBE operations refresh a contract's
//!    position in the cache. UPDATE operations (controlled by contract creators) do not.
//!
//! 4. **Self-regulating proximity**: Peers near a contract's location naturally see more GETs,
//!    keeping nearby contracts fresh in their caches.

use freenet_stdlib::prelude::ContractKey;
use std::collections::{HashMap, VecDeque};
use std::time::Instant;

use crate::util::time_source::TimeSource;

/// Type of access that refreshes a contract's cache position.
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
    #[allow(dead_code)] // Reserved for future direct subscribe tracking
    Subscribe,
}

/// Metadata about a cached contract.
#[derive(Debug, Clone)]
#[allow(dead_code)] // Fields accessed via public API
pub struct CachedContract {
    /// The contract key
    pub key: ContractKey,
    /// Size of the contract state in bytes
    pub size_bytes: u64,
    /// Last time this contract was accessed (via GET/PUT/SUBSCRIBE)
    pub last_accessed: Instant,
    /// Type of the last access
    pub last_access_type: AccessType,
}

/// LRU cache for seeded contracts with byte-budget awareness.
///
/// This cache maintains contracts that this peer is "seeding" - keeping available
/// for the network even after local subscribers have left. The cache has a byte
/// budget rather than a simple count limit, allowing it to fairly handle contracts
/// of varying sizes.
pub struct SeedingCache<T: TimeSource> {
    /// Maximum bytes to use for cached contracts
    budget_bytes: u64,
    /// Current total bytes used
    current_bytes: u64,
    /// LRU order - front is oldest, back is newest
    lru_order: VecDeque<ContractKey>,
    /// Contract metadata indexed by key
    contracts: HashMap<ContractKey, CachedContract>,
    /// Time source for testability
    time_source: T,
}

impl<T: TimeSource> SeedingCache<T> {
    /// Create a new seeding cache with the given byte budget.
    pub fn new(budget_bytes: u64, time_source: T) -> Self {
        Self {
            budget_bytes,
            current_bytes: 0,
            lru_order: VecDeque::new(),
            contracts: HashMap::new(),
            time_source,
        }
    }

    /// Record an access to a contract, potentially adding it to the cache.
    ///
    /// If the contract is already cached, this refreshes its LRU position.
    /// If not cached, this adds it and evicts old contracts if necessary.
    ///
    /// Returns the list of contracts that were evicted to make room (if any).
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
                // Adjust byte accounting for size change
                if size_bytes > existing.size_bytes {
                    self.current_bytes = self
                        .current_bytes
                        .saturating_add(size_bytes - existing.size_bytes);
                } else {
                    self.current_bytes = self
                        .current_bytes
                        .saturating_sub(existing.size_bytes - size_bytes);
                }
                existing.size_bytes = size_bytes;
            }
            existing.last_accessed = now;
            existing.last_access_type = access_type;

            // Move to back of LRU (most recently used)
            // Note: This is O(n) which is acceptable for typical cache sizes (dozens to hundreds).
            // For thousands of contracts, consider using the `lru` crate or a linked list.
            self.lru_order.retain(|k| k != &key);
            self.lru_order.push_back(key);
        } else {
            // Not cached - need to add it
            // First, evict until we have room
            while self.current_bytes + size_bytes > self.budget_bytes && !self.lru_order.is_empty()
            {
                if let Some(oldest_key) = self.lru_order.pop_front() {
                    if let Some(removed) = self.contracts.remove(&oldest_key) {
                        self.current_bytes = self.current_bytes.saturating_sub(removed.size_bytes);
                        evicted.push(oldest_key);
                    }
                }
            }

            // Add the new contract
            let contract = CachedContract {
                key,
                size_bytes,
                last_accessed: now,
                last_access_type: access_type,
            };
            self.contracts.insert(key, contract);
            self.lru_order.push_back(key);
            self.current_bytes = self.current_bytes.saturating_add(size_bytes);
        }

        evicted
    }

    /// Check if a contract is in the cache.
    pub fn contains(&self, key: &ContractKey) -> bool {
        self.contracts.contains_key(key)
    }

    /// Get metadata about a cached contract.
    #[allow(dead_code)] // Public API for introspection
    pub fn get(&self, key: &ContractKey) -> Option<&CachedContract> {
        self.contracts.get(key)
    }

    /// Remove a contract from the cache.
    ///
    /// Returns the removed contract metadata, if it was present.
    pub fn remove(&mut self, key: &ContractKey) -> Option<CachedContract> {
        if let Some(removed) = self.contracts.remove(key) {
            self.lru_order.retain(|k| k != key);
            self.current_bytes = self.current_bytes.saturating_sub(removed.size_bytes);
            Some(removed)
        } else {
            None
        }
    }

    /// Get the current number of cached contracts.
    #[allow(dead_code)] // Public API for introspection
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

    /// Get all cached contract keys in LRU order (oldest first).
    #[cfg(test)]
    pub fn keys_lru_order(&self) -> Vec<ContractKey> {
        self.lru_order.iter().cloned().collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::util::time_source::MockTimeSource;
    use freenet_stdlib::prelude::ContractInstanceId;
    use std::time::Duration;

    fn make_key(seed: u8) -> ContractKey {
        ContractKey::from(ContractInstanceId::new([seed; 32]))
    }

    fn make_cache(budget: u64) -> SeedingCache<MockTimeSource> {
        let time_source = MockTimeSource::new(Instant::now());
        SeedingCache::new(budget, time_source)
    }

    #[test]
    fn test_empty_cache() {
        let cache = make_cache(1000);
        assert!(cache.is_empty());
        assert_eq!(cache.len(), 0);
        assert_eq!(cache.current_bytes(), 0);
        assert!(!cache.contains(&make_key(1)));
    }

    #[test]
    fn test_add_single_contract() {
        let mut cache = make_cache(1000);
        let key = make_key(1);

        let evicted = cache.record_access(key, 100, AccessType::Get);

        assert!(evicted.is_empty());
        assert!(cache.contains(&key));
        assert_eq!(cache.len(), 1);
        assert_eq!(cache.current_bytes(), 100);

        let info = cache.get(&key).unwrap();
        assert_eq!(info.size_bytes, 100);
        assert_eq!(info.last_access_type, AccessType::Get);
    }

    #[test]
    fn test_refresh_existing_contract() {
        let time_source = MockTimeSource::new(Instant::now());
        let mut cache = SeedingCache::new(1000, time_source.clone());
        let key = make_key(1);

        // First access
        cache.record_access(key, 100, AccessType::Get);
        let first_access = cache.get(&key).unwrap().last_accessed;

        // Advance time and access again
        let mut new_time_source = time_source.clone();
        new_time_source.advance_time(Duration::from_secs(10));
        cache.time_source = new_time_source;

        cache.record_access(key, 100, AccessType::Put);

        // Should still be one contract, but updated
        assert_eq!(cache.len(), 1);
        assert_eq!(cache.current_bytes(), 100); // Size unchanged

        let info = cache.get(&key).unwrap();
        assert_eq!(info.last_access_type, AccessType::Put);
        assert!(info.last_accessed > first_access);
    }

    #[test]
    fn test_lru_eviction_by_budget() {
        let mut cache = make_cache(250); // Can hold 2.5 contracts of 100 bytes

        let key1 = make_key(1);
        let key2 = make_key(2);
        let key3 = make_key(3);

        // Add first two contracts - should fit
        cache.record_access(key1, 100, AccessType::Get);
        cache.record_access(key2, 100, AccessType::Get);
        assert_eq!(cache.len(), 2);
        assert_eq!(cache.current_bytes(), 200);

        // Add third - should evict first
        let evicted = cache.record_access(key3, 100, AccessType::Get);

        assert_eq!(evicted.len(), 1);
        assert_eq!(evicted[0], key1);
        assert!(!cache.contains(&key1));
        assert!(cache.contains(&key2));
        assert!(cache.contains(&key3));
        assert_eq!(cache.len(), 2);
        assert_eq!(cache.current_bytes(), 200);
    }

    #[test]
    fn test_large_contract_evicts_multiple() {
        let mut cache = make_cache(300);

        let small1 = make_key(1);
        let small2 = make_key(2);
        let small3 = make_key(3);
        let large = make_key(4);

        // Add three small contracts
        cache.record_access(small1, 100, AccessType::Get);
        cache.record_access(small2, 100, AccessType::Get);
        cache.record_access(small3, 100, AccessType::Get);
        assert_eq!(cache.len(), 3);
        assert_eq!(cache.current_bytes(), 300);

        // Add one large contract - should evict two small ones
        let evicted = cache.record_access(large, 200, AccessType::Put);

        assert_eq!(evicted.len(), 2);
        assert_eq!(evicted[0], small1); // Oldest first
        assert_eq!(evicted[1], small2);
        assert!(!cache.contains(&small1));
        assert!(!cache.contains(&small2));
        assert!(cache.contains(&small3));
        assert!(cache.contains(&large));
        assert_eq!(cache.len(), 2);
        assert_eq!(cache.current_bytes(), 300);
    }

    #[test]
    fn test_access_refreshes_lru_position() {
        let mut cache = make_cache(250);

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

        // Add key3 - should evict key2 (now oldest)
        let evicted = cache.record_access(key3, 100, AccessType::Get);

        assert_eq!(evicted, vec![key2]);
        assert!(cache.contains(&key1));
        assert!(!cache.contains(&key2));
        assert!(cache.contains(&key3));
    }

    #[test]
    fn test_remove_contract() {
        let mut cache = make_cache(1000);
        let key = make_key(1);

        cache.record_access(key, 100, AccessType::Get);
        assert!(cache.contains(&key));
        assert_eq!(cache.current_bytes(), 100);

        let removed = cache.remove(&key);
        assert!(removed.is_some());
        assert_eq!(removed.unwrap().size_bytes, 100);
        assert!(!cache.contains(&key));
        assert_eq!(cache.current_bytes(), 0);
        assert!(cache.is_empty());
    }

    #[test]
    fn test_remove_nonexistent() {
        let mut cache = make_cache(1000);
        let key = make_key(1);

        let removed = cache.remove(&key);
        assert!(removed.is_none());
    }

    #[test]
    fn test_access_types() {
        let mut cache = make_cache(1000);
        let key = make_key(1);

        // Test each access type is recorded correctly
        cache.record_access(key, 100, AccessType::Get);
        assert_eq!(cache.get(&key).unwrap().last_access_type, AccessType::Get);

        cache.record_access(key, 100, AccessType::Put);
        assert_eq!(cache.get(&key).unwrap().last_access_type, AccessType::Put);

        cache.record_access(key, 100, AccessType::Subscribe);
        assert_eq!(
            cache.get(&key).unwrap().last_access_type,
            AccessType::Subscribe
        );
    }

    #[test]
    fn test_zero_budget_edge_case() {
        let mut cache = make_cache(0);
        let key = make_key(1);

        // Edge case: With zero budget, the eviction loop condition
        // `current_bytes + size_bytes > budget_bytes` is true (0 + 100 > 0),
        // but there's nothing to evict since the cache is empty.
        // The contract gets added anyway, exceeding the budget.
        // This documents that budget is a soft limit when the cache is empty.
        let evicted = cache.record_access(key, 100, AccessType::Get);

        assert!(evicted.is_empty()); // Nothing was evicted
        assert!(cache.contains(&key)); // Contract was added
        assert_eq!(cache.current_bytes(), 100); // Budget exceeded
    }

    #[test]
    fn test_exact_budget_fit() {
        let mut cache = make_cache(100);
        let key = make_key(1);

        let evicted = cache.record_access(key, 100, AccessType::Get);

        assert!(evicted.is_empty());
        assert!(cache.contains(&key));
        assert_eq!(cache.current_bytes(), 100);
        assert_eq!(cache.budget_bytes(), 100);
    }

    #[test]
    fn test_contract_larger_than_budget() {
        let mut cache = make_cache(100);

        // First add a small contract
        let small = make_key(1);
        cache.record_access(small, 50, AccessType::Get);

        // Now add one larger than budget - should evict small and still add
        let large = make_key(2);
        let evicted = cache.record_access(large, 150, AccessType::Get);

        assert_eq!(evicted, vec![small]);
        // Design decision: Large contracts are still added even if they exceed budget.
        // The budget is a soft limit - we evict as much as possible but don't refuse
        // to cache. This prevents contracts from becoming unfindable just because
        // they're large. The cache will naturally evict oversized contracts when
        // new contracts arrive, as they'll be displaced first.
        assert!(cache.contains(&large));
        assert_eq!(cache.current_bytes(), 150);
    }

    #[test]
    fn test_contract_size_change_increases() {
        let mut cache = make_cache(1000);
        let key = make_key(1);

        // Add contract with initial size
        cache.record_access(key, 100, AccessType::Get);
        assert_eq!(cache.current_bytes(), 100);
        assert_eq!(cache.get(&key).unwrap().size_bytes, 100);

        // Contract state grows (e.g., through PUT operation)
        cache.record_access(key, 200, AccessType::Put);
        assert_eq!(cache.current_bytes(), 200);
        assert_eq!(cache.get(&key).unwrap().size_bytes, 200);
    }

    #[test]
    fn test_contract_size_change_decreases() {
        let mut cache = make_cache(1000);
        let key = make_key(1);

        // Add contract with initial size
        cache.record_access(key, 200, AccessType::Get);
        assert_eq!(cache.current_bytes(), 200);

        // Contract state shrinks
        cache.record_access(key, 150, AccessType::Put);
        assert_eq!(cache.current_bytes(), 150);
        assert_eq!(cache.get(&key).unwrap().size_bytes, 150);
    }

    #[test]
    fn test_contract_size_change_triggers_no_eviction() {
        // Size changes to existing contracts don't trigger eviction
        // (only new contracts can trigger eviction)
        let mut cache = make_cache(200);
        let key1 = make_key(1);
        let key2 = make_key(2);

        // Add two contracts at 100 bytes each
        cache.record_access(key1, 100, AccessType::Get);
        cache.record_access(key2, 100, AccessType::Get);
        assert_eq!(cache.current_bytes(), 200);

        // key1 grows to 150 bytes - exceeds budget but doesn't evict key2
        // because we're updating an existing contract, not adding a new one
        cache.record_access(key1, 150, AccessType::Put);
        assert_eq!(cache.current_bytes(), 250); // Over budget
        assert!(cache.contains(&key1));
        assert!(cache.contains(&key2)); // key2 NOT evicted
    }
}
