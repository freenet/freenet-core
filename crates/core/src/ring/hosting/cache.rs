//! Unified hosting cache for contract state caching.
//!
//! This module implements a byte-budget aware LRU cache with TTL protection for hosted contracts.
//! It unifies the previously separate `SeedingCache` (now called hosting cache) and `GetSubscriptionCache` into a single
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

/// Default hosting storage budget (1 GiB). The operator-facing default lives in
/// `config.rs` as `DEFAULT_MAX_HOSTING_STORAGE`; keep the two consistent. This
/// constant is the in-code default used by tests and as a fallback.
pub const DEFAULT_HOSTING_BUDGET_BYTES: u64 = 1024 * 1024 * 1024;

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

/// Result of recording a contract access in the hosting cache.
#[derive(Debug)]
pub struct RecordAccessResult {
    /// Whether this contract was newly added (vs. refreshed existing)
    pub is_new: bool,
    /// Contracts that were evicted to make room
    pub evicted: Vec<ContractKey>,
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
    /// Whether a local client (HTTP/WebSocket) accessed this contract.
    /// Only flagged contracts get subscription renewal and local-cache serving.
    pub local_client_access: bool,
    /// When the local client last accessed this contract. Age-gates the renewal:
    /// contracts not accessed locally within SUBSCRIPTION_LEASE_DURATION stop
    /// being renewed (AGENTS.md cleanup exemption rule). None = never locally accessed.
    pub local_client_last_access: Option<Instant>,
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

    /// Evict past-TTL, non-retained contracts while the cache is over budget.
    ///
    /// Walks `lru_order` front-first (oldest first). An entry is evicted when
    /// it is past `min_ttl` since its last access AND `should_retain(key)` is
    /// `false`. Eviction stops as soon as `current_bytes <= budget_bytes`.
    /// Entries that are retained (in use) or still within `min_ttl` are kept
    /// even if that leaves the cache over budget.
    ///
    /// Uses a `retain()`-style walk so the LRU ordering of surviving entries
    /// is preserved. Returns the evicted keys.
    fn evict_over_budget<F>(&mut self, should_retain: &F) -> Vec<ContractKey>
    where
        F: Fn(&ContractKey) -> bool,
    {
        if self.current_bytes <= self.budget_bytes {
            return Vec::new();
        }

        let now = self.time_source.now();
        let mut evicted = Vec::new();

        self.lru_order.retain(|key| {
            if self.current_bytes <= self.budget_bytes {
                return true; // back under budget, stop evicting
            }
            if let Some(entry) = self.contracts.get(key) {
                let age = now.saturating_duration_since(entry.last_accessed);
                if age >= self.min_ttl && !should_retain(key) {
                    let size = entry.size_bytes;
                    self.contracts.remove(key);
                    self.current_bytes = self.current_bytes.saturating_sub(size);
                    evicted.push(*key);
                    false
                } else {
                    // Retained (in use) or still within TTL — keep it even
                    // if that leaves the cache over budget.
                    true
                }
            } else {
                false // orphaned LRU entry
            }
        });

        evicted
    }

    /// Record an access to a contract, adding or refreshing it in the cache.
    ///
    /// If the contract is already cached, this refreshes its LRU position and timestamp.
    /// If not cached, this adds it and evicts old contracts if necessary.
    ///
    /// Returns a `RecordAccessResult` containing:
    /// - `is_new`: Whether this contract was newly added (vs. refreshed existing)
    /// - `evicted`: Contracts that were evicted to make room (if any)
    ///
    /// Eviction respects TTL: contracts won't be evicted until min_ttl has
    /// passed. It also honors `should_retain`: a contract for which
    /// `should_retain(key)` returns `true` (e.g. one with an active client
    /// subscription, downstream subscriber, or network subscription) is
    /// never evicted, even past TTL. Evicting an in-use contract would
    /// orphan its on-disk state — the caller skips disk reclamation for it,
    /// but the contract is then gone from the cache and never revisited.
    ///
    /// The newly-inserted contract is age-0, so it can never be evicted by
    /// the same `record_access` call.
    pub fn record_access<F>(
        &mut self,
        key: ContractKey,
        size_bytes: u64,
        access_type: AccessType,
        should_retain: F,
    ) -> RecordAccessResult
    where
        F: Fn(&ContractKey) -> bool,
    {
        let now = self.time_source.now();

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

            RecordAccessResult {
                is_new: false,
                evicted: Vec::new(),
            }
        } else {
            // Not cached - insert the new entry first, then evict over-budget
            // entries. The new entry is age-0 so the eviction walk's
            // past-TTL check guarantees it is never evicted by this call.
            let contract = HostedContract {
                size_bytes,
                last_accessed: now,
                access_type,
                local_client_access: false,
                local_client_last_access: None,
            };
            self.contracts.insert(key, contract);
            self.lru_order.push_back(key);
            self.current_bytes = self.current_bytes.saturating_add(size_bytes);

            let evicted = self.evict_over_budget(&should_retain);

            RecordAccessResult {
                is_new: true,
                evicted,
            }
        }
    }

    /// Mark a contract as accessed by a local client (HTTP/WebSocket).
    /// No-op if the contract is not in the cache.
    pub fn mark_local_client_access(&mut self, key: &ContractKey) {
        if let Some(existing) = self.contracts.get_mut(key) {
            existing.local_client_access = true;
            existing.local_client_last_access = Some(self.time_source.now());
        }
    }

    /// Check if a contract was accessed by a local client.
    pub fn has_local_client_access(&self, key: &ContractKey) -> bool {
        self.contracts
            .get(key)
            .map(|c| c.local_client_access)
            .unwrap_or(false)
    }

    /// Check if a contract was accessed by a local client within the given duration.
    /// Returns false if never locally accessed or if the access is too old.
    pub fn has_recent_local_client_access(
        &self,
        key: &ContractKey,
        max_age: std::time::Duration,
    ) -> bool {
        let now = self.time_source.now();
        self.contracts
            .get(key)
            .and_then(|c| c.local_client_last_access)
            .map(|t| now.saturating_duration_since(t) < max_age)
            .unwrap_or(false)
    }

    /// Touch/refresh a contract's timestamp without adding it if missing.
    ///
    /// Called when a user GET serves a hosted contract from local cache.
    /// This refreshes the TTL and LRU position so actively requested
    /// contracts stay in the cache.
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

    /// Sweep for contracts past TTL when the cache is over budget.
    ///
    /// Only evicts when `current_bytes > budget_bytes`. Among over-budget
    /// entries, contracts past `min_ttl` since their last access are evicted
    /// (oldest first via LRU order) unless the `should_retain` predicate
    /// returns `true` (e.g., contracts with active client subscriptions or
    /// downstream subscribers).
    ///
    /// Uses `retain()` to preserve LRU ordering for non-evicted entries.
    /// Returns contracts evicted from this cache.
    pub fn sweep_expired<F>(&mut self, should_retain: F) -> Vec<ContractKey>
    where
        F: Fn(&ContractKey) -> bool,
    {
        // Over-budget eviction logic is shared with `record_access` via
        // `evict_over_budget`: it returns early when under budget, then
        // evicts past-TTL, non-retained entries oldest-first.
        self.evict_over_budget(&should_retain)
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
        local_client_access: bool,
    ) {
        // Skip if already loaded (shouldn't happen, but defensive)
        if self.contracts.contains_key(&key) {
            return;
        }

        // Calculate the last_accessed time from age
        let now = self.time_source.now();
        let last_accessed = now.checked_sub(last_access_age).unwrap_or(now);

        // Locally-accessed contracts loaded from disk get one renewal window
        // (set to "now") so they can re-establish subscriptions after restart.
        // If the user doesn't access them again, the age gate expires naturally.
        let local_client_last_access = if local_client_access { Some(now) } else { None };

        let contract = HostedContract {
            size_bytes,
            last_accessed,
            access_type,
            local_client_access,
            local_client_last_access,
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

        let result = cache.record_access(key, 100, AccessType::Get, |_| false);

        assert!(result.is_new);
        assert!(result.evicted.is_empty());
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
        cache.record_access(key, 100, AccessType::Get, |_| false);
        let first_access = cache.get(&key).unwrap().last_accessed;

        // Advance time and access again
        time.advance_time(Duration::from_secs(10));
        cache.record_access(key, 100, AccessType::Put, |_| false);

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
        cache.record_access(key1, 100, AccessType::Get, |_| false);
        cache.record_access(key2, 100, AccessType::Get, |_| false);
        assert_eq!(cache.current_bytes(), 200);

        // Advance time by 30 seconds (under TTL)
        time.advance_time(Duration::from_secs(30));

        // Add third entry - should NOT evict because all entries under TTL
        let result = cache.record_access(key3, 100, AccessType::Get, |_| false);
        assert!(
            result.evicted.is_empty(),
            "Should not evict entries under TTL"
        );
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
        cache.record_access(key1, 100, AccessType::Get, |_| false);
        cache.record_access(key2, 100, AccessType::Get, |_| false);

        // Advance time past TTL
        time.advance_time(Duration::from_secs(61));

        // Add third entry - should evict key1 (oldest)
        let result = cache.record_access(key3, 100, AccessType::Get, |_| false);
        assert!(result.is_new);
        assert_eq!(result.evicted, vec![key1]);
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
        cache.record_access(key1, 100, AccessType::Get, |_| false);
        cache.record_access(key2, 100, AccessType::Get, |_| false);

        // Access key1 again - should move it to back of LRU
        cache.record_access(key1, 100, AccessType::Subscribe, |_| false);

        // LRU order should now be [key2, key1]
        let order = cache.keys_lru_order();
        assert_eq!(order, vec![key2, key1]);

        // Advance past TTL and add key3 - should evict key2 (now oldest)
        time.advance_time(Duration::from_secs(61));
        let result = cache.record_access(key3, 100, AccessType::Get, |_| false);

        assert_eq!(result.evicted, vec![key2]);
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
        cache.record_access(key1, 100, AccessType::Get, |_| false);
        cache.record_access(key2, 100, AccessType::Get, |_| false);

        // Advance time by 50 seconds
        time.advance_time(Duration::from_secs(50));

        // Touch key1 (simulating UPDATE received)
        cache.touch(&key1);

        // Advance another 15 seconds (key1 now at 15s, key2 at 65s)
        time.advance_time(Duration::from_secs(15));

        // Add key3 - should evict key2 (past TTL), NOT key1 (recently touched)
        let result = cache.record_access(key3, 100, AccessType::Get, |_| false);

        assert_eq!(
            result.evicted,
            vec![key2],
            "Should evict key2 which is past TTL"
        );
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
        cache.record_access(small1, 100, AccessType::Get, |_| false);
        cache.record_access(small2, 100, AccessType::Get, |_| false);
        cache.record_access(small3, 100, AccessType::Get, |_| false);
        assert_eq!(cache.current_bytes(), 300);

        // Advance past TTL
        time.advance_time(Duration::from_secs(61));

        // Add one large contract - should evict two small ones
        let result = cache.record_access(large, 200, AccessType::Put, |_| false);

        assert_eq!(result.evicted.len(), 2);
        assert_eq!(result.evicted[0], small1); // Oldest first
        assert_eq!(result.evicted[1], small2);
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
        cache.record_access(key1, 100, AccessType::Get, |_| false);
        cache.record_access(key2, 100, AccessType::Get, |_| false);
        cache.record_access(key3, 100, AccessType::Get, |_| false);
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
        cache.record_access(key1, 100, AccessType::Get, |_| false);
        cache.record_access(key2, 100, AccessType::Get, |_| false);
        cache.record_access(key3, 100, AccessType::Get, |_| false);

        // Advance past TTL
        time.advance_time(Duration::from_secs(61));

        // Sweep with predicate that retains key1
        let evicted = cache.sweep_expired(|k| *k == key1);

        // key1 should be retained, key2 evicted to get under budget
        assert_eq!(evicted, vec![key2]);
        assert!(cache.contains(&key1));
        assert!(!cache.contains(&key2));
        assert!(cache.contains(&key3));
        assert_eq!(cache.current_bytes(), 200);
    }

    /// `record_access` must honor `should_retain`: an over-budget, past-TTL
    /// entry whose `should_retain` returns true is NOT evicted; an
    /// unretained past-TTL entry IS evicted.
    #[test]
    fn test_record_access_respects_should_retain() {
        let (mut cache, time) = make_cache(200, Duration::from_secs(60));
        let retained = make_key(1);
        let evictable = make_key(2);
        let trigger = make_key(3);

        // Fill the cache: `retained` (oldest) then `evictable`.
        cache.record_access(retained, 100, AccessType::Get, |_| false);
        cache.record_access(evictable, 100, AccessType::Get, |_| false);
        assert_eq!(cache.current_bytes(), 200);

        // Advance past TTL so both existing entries are eviction-eligible.
        time.advance_time(Duration::from_secs(61));

        // Insert a third entry, over budget. `retained` is the oldest, so a
        // naive LRU eviction would drop it first — but `should_retain`
        // protects it, so `evictable` (next oldest) must be evicted instead.
        let result = cache.record_access(trigger, 100, AccessType::Get, |k| *k == retained);

        assert_eq!(
            result.evicted,
            vec![evictable],
            "in-use (retained) contract must be skipped; the unretained \
             past-TTL contract must be evicted instead"
        );
        assert!(
            cache.contains(&retained),
            "retained contract must survive even past TTL and over budget"
        );
        assert!(!cache.contains(&evictable));
        assert!(cache.contains(&trigger));
    }

    /// The contract inserted by `record_access` is age-0, so it is never
    /// evicted by the same call even when the insert pushes over budget.
    #[test]
    fn test_record_access_never_evicts_the_new_entry() {
        let (mut cache, time) = make_cache(100, Duration::from_secs(60));
        let first = make_key(1);
        let second = make_key(2);

        cache.record_access(first, 100, AccessType::Get, |_| false);
        time.advance_time(Duration::from_secs(61));

        // Inserting `second` puts the cache over budget; `first` is past TTL
        // and unretained so it is evicted, but the just-inserted `second`
        // (age 0) must survive.
        let result = cache.record_access(second, 100, AccessType::Get, |_| false);
        assert_eq!(result.evicted, vec![first]);
        assert!(cache.contains(&second));
        assert!(!cache.contains(&first));
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
        cache.record_access(key, 100, AccessType::Get, |_| false);
        assert_eq!(cache.get(&key).unwrap().access_type, AccessType::Get);

        cache.record_access(key, 100, AccessType::Put, |_| false);
        assert_eq!(cache.get(&key).unwrap().access_type, AccessType::Put);

        cache.record_access(key, 100, AccessType::Subscribe, |_| false);
        assert_eq!(cache.get(&key).unwrap().access_type, AccessType::Subscribe);
    }

    #[test]
    fn test_contract_size_change() {
        let (mut cache, _) = make_cache(1000, Duration::from_secs(60));
        let key = make_key(1);

        // Add contract with initial size
        cache.record_access(key, 100, AccessType::Get, |_| false);
        assert_eq!(cache.current_bytes(), 100);
        assert_eq!(cache.get(&key).unwrap().size_bytes, 100);

        // Contract state grows
        cache.record_access(key, 200, AccessType::Put, |_| false);
        assert_eq!(cache.current_bytes(), 200);
        assert_eq!(cache.get(&key).unwrap().size_bytes, 200);

        // Contract state shrinks
        cache.record_access(key, 150, AccessType::Put, |_| false);
        assert_eq!(cache.current_bytes(), 150);
        assert_eq!(cache.get(&key).unwrap().size_bytes, 150);
    }

    #[test]
    fn test_iter_returns_all_hosted_keys() {
        let (mut cache, _) = make_cache(1000, Duration::from_secs(60));

        // Empty cache yields no keys
        assert_eq!(cache.iter().count(), 0);

        let key1 = make_key(1);
        let key2 = make_key(2);
        let key3 = make_key(3);

        cache.record_access(key1, 100, AccessType::Get, |_| false);
        cache.record_access(key2, 100, AccessType::Put, |_| false);
        cache.record_access(key3, 100, AccessType::Subscribe, |_| false);

        let keys: Vec<ContractKey> = cache.iter().collect();
        assert_eq!(keys.len(), 3);
        assert!(keys.contains(&key1));
        assert!(keys.contains(&key2));
        assert!(keys.contains(&key3));
    }

    /// Age gate: has_recent_local_client_access returns false after the
    /// max_age window expires. This is the TTL enforcement for the cleanup
    /// exemption rule (AGENTS.md).
    #[test]
    fn test_local_client_access_age_gate_expiry() {
        let lease = Duration::from_secs(480); // SUBSCRIPTION_LEASE_DURATION
        let (mut cache, time) = make_cache(10000, Duration::from_secs(60));
        let key = make_key(1);

        cache.record_access(key, 100, AccessType::Get, |_| false);
        cache.mark_local_client_access(&key);

        // Immediately after marking: recent access is true
        assert!(cache.has_local_client_access(&key));
        assert!(cache.has_recent_local_client_access(&key, lease));

        // Advance time just under the lease -- still recent
        time.advance_time(lease - Duration::from_secs(1));
        assert!(cache.has_recent_local_client_access(&key, lease));

        // Advance past the lease -- no longer recent
        time.advance_time(Duration::from_secs(2));
        assert!(
            !cache.has_recent_local_client_access(&key, lease),
            "Contract should exit renewal after lease expires"
        );

        // The flag itself is still set (sticky)
        assert!(
            cache.has_local_client_access(&key),
            "Flag should remain sticky even after age gate expires"
        );

        // Re-marking refreshes the timestamp
        cache.mark_local_client_access(&key);
        assert!(
            cache.has_recent_local_client_access(&key, lease),
            "Re-marking should refresh the age gate"
        );
    }
}
