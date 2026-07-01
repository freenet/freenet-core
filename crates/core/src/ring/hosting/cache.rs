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
use crate::wasm_runtime::read_total_ram_bytes;

/// Lower clamp for the RAM-scaled default hosting budget (128 MiB).
///
/// Even a genuinely tiny host should still be able to host a useful amount of
/// contract state. The budget tracks on-disk **state** bytes (not RSS), so this
/// floor is a disk allowance, not a memory reservation.
pub const MIN_DEFAULT_HOSTING_BUDGET_BYTES: u64 = 128 * 1024 * 1024;

/// Upper clamp for the RAM-scaled default hosting budget (1 GiB).
///
/// Equal to the historical flat default, on purpose: a host with ample RAM
/// (>= 8 GiB at the current divisor) resolves to exactly the previous 1 GiB
/// budget and sees NO behavior change. Only memory-constrained hosts get a
/// smaller, proportional budget. This makes the capability-relative default a
/// pure "small boxes get less" change (addresses #4565, gateway RSS -> OOM on
/// small boxes) rather than a budget increase for anyone.
pub const MAX_DEFAULT_HOSTING_BUDGET_BYTES: u64 = 1024 * 1024 * 1024;

/// Fraction of total system RAM used to size the default hosting budget: 1/8.
///
/// Mirrors the module cache's `DEFAULT_MODULE_CACHE_RAM_DIVISOR`. The divisor —
/// NOT the `MAX` ceiling — is the small-box protection (#4441): the fix that
/// replaced the flat budget must itself never OOM a small box, so the budget
/// scales DOWN with RAM below the ceiling instead of being a fixed
/// count/constant. 1/8 leaves the other 7/8 of RAM for the rest of the node
/// (state store, module caches, transport buffers, WASM arenas, the OS).
const DEFAULT_HOSTING_BUDGET_RAM_DIVISOR: u64 = 8;

/// Fallback total-RAM estimate (1 GiB) when the OS query fails. Conservative:
/// at 1 GiB the divisor yields 128 MiB (the floor), so an unknown-RAM host gets
/// the smallest sane budget rather than the max.
const FALLBACK_TOTAL_RAM_BYTES: u64 = 1024 * 1024 * 1024;

/// Default hosting-storage budget, scaled to the memory the node may use.
///
/// Replaces the historical flat 1 GiB default. Returns
/// `clamp(total_ram / DEFAULT_HOSTING_BUDGET_RAM_DIVISOR,
/// MIN_DEFAULT_HOSTING_BUDGET_BYTES, MAX_DEFAULT_HOSTING_BUDGET_BYTES)`
/// (currently `clamp(total_ram / 8, 128 MiB, 1 GiB)`), where `total_ram` is
/// `min(host RAM, cgroup limit)` from the SAME single source
/// [`crate::wasm_runtime::read_total_ram_bytes`] that sizes the module cache and
/// the A1 resource-utilization telemetry — so the node has one notion of "how
/// much memory do I have".
///
/// This is the single source of truth for the default budget: the operator-
/// facing `config::default_max_hosting_storage()` resolves to it (re-exported
/// via `ring::hosting`). The operator override
/// (`--max-hosting-storage` / `max_hosting_storage` config / env) always wins
/// when explicitly set; only this DEFAULT is RAM-scaled.
///
/// The budget tracks hosted contract **state** bytes only. WASM code blobs and
/// ReDb/SQLite database overhead are additional and not counted against it, so
/// this is not a hard bound on total on-disk usage.
pub fn default_hosting_budget_bytes() -> u64 {
    let total_ram = read_total_ram_bytes()
        .map(|v| v as u64)
        .unwrap_or(FALLBACK_TOTAL_RAM_BYTES);
    budget_for_ram(total_ram)
}

/// Pure clamp math behind [`default_hosting_budget_bytes`], split out so the
/// small-box / large-box boundary behavior is unit-testable without depending
/// on the test host's real RAM. Returns the hosting-cache byte budget for a
/// host with `total_ram` bytes of usable memory.
fn budget_for_ram(total_ram: u64) -> u64 {
    (total_ram / DEFAULT_HOSTING_BUDGET_RAM_DIVISOR).clamp(
        MIN_DEFAULT_HOSTING_BUDGET_BYTES,
        MAX_DEFAULT_HOSTING_BUDGET_BYTES,
    )
}

/// Point-in-time hosting-cache resource gauges for per-node telemetry.
///
/// Aggregate scalars only (never per-contract), emitted on the existing
/// `RouterSnapshot` cadence so the capability-relative budget's behavior is
/// observable in production (design principle: instrumentation is horizontal —
/// see `docs/design/hosting-eviction.md`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct HostingCacheStats {
    /// Configured byte budget (the RAM-scaled default, or the operator override).
    pub budget_bytes: u64,
    /// Current tracked contract-state bytes. Headroom ratio is
    /// `current_bytes / budget_bytes`, derived by the collector.
    pub current_bytes: u64,
    /// Number of contracts currently in the hosting cache.
    pub contract_count: u64,
    /// Monotonic count of contracts evicted specifically because the cache was
    /// over budget. The collector differences it across the cadence to derive a
    /// budget-triggered eviction rate.
    pub budget_evictions_total: u64,
}

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
    /// Contracts that were evicted to make room, paired with the
    /// state-write generation captured atomically under the hosting
    /// cache's write lock. Carried through `EvictContract` so the
    /// deletion-time guard in `RuntimePool::remove_contract` can detect
    /// a re-host that occurred between eviction and disk reclamation.
    pub evicted: Vec<(ContractKey, u64)>,
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
    /// State-write generation observed when this entry was inserted or
    /// refreshed. Captured atomically under the hosting cache's write
    /// lock — passed in by `HostingManager::record_contract_access` from
    /// `Ring::state_generation`. Carried through `EvictContract` and
    /// re-checked at deletion time to close the re-host race. See
    /// `RecordAccessResult.evicted` and `RuntimePool::remove_contract`.
    pub write_generation: u64,
    /// When this contract transitioned from "in use" to "no longer in use"
    /// (last subscriber / client / downstream subscriber went away). `Some`
    /// means the entry is now a recently-abandoned eviction candidate;
    /// `None` means the entry has never been abandoned, or has been
    /// re-accessed since its last abandonment.
    ///
    /// The abandonment hook moves the entry to the FRONT of the LRU order
    /// so that under disk pressure it is evicted before older-but-still-
    /// active entries. Once past TTL, `evict_over_budget` walks the LRU
    /// front-first as before — abandoned entries simply sit at the front.
    /// Refreshing the entry via `record_access` clears this field and
    /// moves the entry back to the LRU tail.
    ///
    /// Idle contracts with no subscribers but recent demand are NOT
    /// affected: they keep their natural LRU position. Only entries that
    /// were actively in use and then lost all in-use signals get bumped
    /// to the priority bucket, since their `last_accessed` would otherwise
    /// keep them at the LRU tail despite no longer mattering.
    ///
    /// The timestamp itself is written but not read by eviction logic
    /// (eviction priority comes purely from LRU position, which
    /// `record_abandonment` adjusts as a side effect). It is retained
    /// for the governance dashboard (PR #4270) so an operator can see
    /// how long ago a flagged contract was last actively used. A future
    /// refactor that removes the field can do so only after the
    /// dashboard reader is also dropped — code-first reviewer of #4260
    /// flagged this as a "currently write-only" foot-gun worth pinning
    /// in the rustdoc.
    pub abandoned_at: Option<Instant>,
}

/// Unified hosting cache that combines byte-budget LRU with TTL protection.
///
/// This cache maintains contracts that this peer is "hosting" - keeping available
/// for the network. The cache has:
/// - Byte budget: Large contracts consume more budget. The budget is measured
///   in tracked contract **state** bytes only; WASM code blobs and database
///   overhead are not counted against it.
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
    /// Monotonic count of contracts evicted because the cache was over budget.
    /// Only `evict_over_budget` increments it, so it counts budget-triggered
    /// evictions specifically (not TTL sweeps that found nothing over budget).
    /// Exposed via [`HostingCache::stats`] for per-node telemetry.
    budget_evictions_total: u64,
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
            budget_evictions_total: 0,
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
    fn evict_over_budget<F>(&mut self, should_retain: &F) -> Vec<(ContractKey, u64)>
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
                    // Capture the generation atomically under the cache's
                    // write lock so the `EvictContract` deletion-time
                    // guard can detect a re-host that races with this
                    // eviction. See `RuntimePool::remove_contract`.
                    let generation = entry.write_generation;
                    self.contracts.remove(key);
                    self.current_bytes = self.current_bytes.saturating_sub(size);
                    self.budget_evictions_total = self.budget_evictions_total.saturating_add(1);
                    evicted.push((*key, generation));
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
    /// - `evicted`: `(ContractKey, write_generation)` pairs for contracts
    ///   evicted to make room. The generation is captured atomically under
    ///   the cache's write lock so the `EvictContract` deletion-time guard
    ///   can detect a re-host that occurred after this call returned.
    ///
    /// Eviction respects TTL: age-0 entries are not eligible for eviction
    /// while `min_ttl > 0` (the production default — i.e. the new entry
    /// just inserted by this call cannot be evicted by the same call).
    /// It also honors `should_retain`: a contract for which `should_retain(key)` returns
    /// `true` (e.g. one with an active client subscription, downstream
    /// subscriber, or network subscription) is never evicted, even past
    /// TTL. Evicting an in-use contract would orphan its on-disk state —
    /// the caller skips disk reclamation for it, but the contract is then
    /// gone from the cache and never revisited.
    ///
    /// `write_generation` is the current state-write generation for this
    /// contract (from `HostingManager::state_generation`). It is stored on
    /// the freshly-inserted/refreshed `HostedContract` so that when this
    /// entry is later evicted, its `write_generation` snapshot travels
    /// with the `EvictContract` event and is compared against the
    /// then-current generation at deletion time.
    pub fn record_access<F>(
        &mut self,
        key: ContractKey,
        size_bytes: u64,
        access_type: AccessType,
        write_generation: u64,
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
            // Refresh the generation snapshot: a re-access is a fresh
            // "I'm hosting this state" assertion, so its captured generation
            // should track the current state-write generation.
            existing.write_generation = write_generation;
            // A re-access clears the abandoned bucket: the contract is
            // back in active use, so the priority-eviction marker no
            // longer applies and the entry returns to the LRU tail
            // below.
            existing.abandoned_at = None;

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
                write_generation,
                abandoned_at: None,
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
            // A fresh touch is evidence of demand — clear any abandoned
            // marker so the entry leaves the priority-eviction bucket.
            existing.abandoned_at = None;
            // Move to back of LRU
            self.lru_order.retain(|k| k != key);
            self.lru_order.push_back(*key);
        }
    }

    /// Mark `key` as recently abandoned and move it to the front of the
    /// LRU order so it is the first candidate for eviction under disk
    /// pressure.
    ///
    /// Called by `HostingManager` when a contract transitions from "in
    /// use" (has subscribers / clients / downstream peers) to "no longer
    /// in use" — the moment its `contract_in_use` predicate flips from
    /// `true` to `false`. The TTL gate in `evict_over_budget` still
    /// applies: an abandoned entry within `min_ttl` of its last access is
    /// not yet eligible. Once past TTL, abandoned entries are evicted
    /// before older-but-still-active entries because they now sit at the
    /// LRU front.
    ///
    /// No-op when `key` is absent (already evicted). Idempotent: calling
    /// it on an already-abandoned entry leaves the existing marker (and
    /// its earlier timestamp) intact, which is the right behavior — the
    /// first abandonment is what we want to time from.
    ///
    /// Network forgetfulness is explicitly preserved: this method does
    /// **not** evict the contract or shorten its TTL. It only changes the
    /// eviction *order* used when the cache is genuinely over budget. A
    /// contract that's abandoned but no longer than other entries' TTL
    /// stays cached just like before.
    pub fn record_abandonment(&mut self, key: &ContractKey) {
        if let Some(existing) = self.contracts.get_mut(key) {
            if existing.abandoned_at.is_none() {
                existing.abandoned_at = Some(self.time_source.now());
                // Move to FRONT of LRU so the next over-budget walk
                // evaluates this entry first.
                self.lru_order.retain(|k| k != key);
                self.lru_order.push_front(*key);
            }
        }
    }

    /// Update the cached `write_generation` snapshot for `key` to `new_gen`.
    ///
    /// Called paired with `HostingManager::bump_state_generation` from
    /// every state-write chokepoint (executor PUT/UPDATE and V2 delegate
    /// PUT/UPDATE). Without this refresh, an UPDATE (or re-PUT) to an
    /// already-hosted contract would leave the cached snapshot stuck at
    /// its `record_access`-time value while the `state_generation` counter
    /// kept advancing; a later eviction would carry the stale snapshot,
    /// the deletion-time generation guard in
    /// `RuntimePool::remove_contract` would see a mismatch, and disk
    /// reclamation would be permanently skipped — leaking the on-disk
    /// state and code blob.
    ///
    /// No-op when `key` is not in the cache: if the contract has been
    /// evicted between the bump and this refresh, the `EvictContract`
    /// already carried the pre-bump snapshot and the deletion-time guard
    /// will skip on the mismatch. That residual leak is narrower than
    /// the "permanent leak on every UPDATE" failure mode this method
    /// closes — see the call-site comment in `runtime.rs`.
    pub fn refresh_entry_generation(&mut self, key: &ContractKey, new_gen: u64) {
        if let Some(existing) = self.contracts.get_mut(key) {
            existing.write_generation = new_gen;
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

    /// Snapshot the cache's aggregate resource gauges under a single read for
    /// per-node telemetry. See [`HostingCacheStats`].
    pub fn stats(&self) -> HostingCacheStats {
        HostingCacheStats {
            budget_bytes: self.budget_bytes,
            current_bytes: self.current_bytes,
            contract_count: self.contracts.len() as u64,
            budget_evictions_total: self.budget_evictions_total,
        }
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
    /// Returns `(ContractKey, write_generation)` pairs for evicted contracts;
    /// the generation snapshot is carried through `EvictContract` so the
    /// deletion-time guard can detect a re-host race.
    pub fn sweep_expired<F>(&mut self, should_retain: F) -> Vec<(ContractKey, u64)>
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
            // Generation 0 = "no observed writes since restart". The
            // executor will bump on the next state write, so a re-PUT
            // after restart will cleanly close the race window against
            // any concurrent eviction.
            write_generation: 0,
            // Persisted entries start un-abandoned; an abandonment
            // transition would be re-observed by the running node if
            // subscribers fail to re-attach after restart.
            abandoned_at: None,
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

        let result = cache.record_access(key, 100, AccessType::Get, 0, |_| false);

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
        cache.record_access(key, 100, AccessType::Get, 0, |_| false);
        let first_access = cache.get(&key).unwrap().last_accessed;

        // Advance time and access again
        time.advance_time(Duration::from_secs(10));
        cache.record_access(key, 100, AccessType::Put, 0, |_| false);

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
        cache.record_access(key1, 100, AccessType::Get, 0, |_| false);
        cache.record_access(key2, 100, AccessType::Get, 0, |_| false);
        assert_eq!(cache.current_bytes(), 200);

        // Advance time by 30 seconds (under TTL)
        time.advance_time(Duration::from_secs(30));

        // Add third entry - should NOT evict because all entries under TTL
        let result = cache.record_access(key3, 100, AccessType::Get, 0, |_| false);
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
        cache.record_access(key1, 100, AccessType::Get, 0, |_| false);
        cache.record_access(key2, 100, AccessType::Get, 0, |_| false);

        // Advance time past TTL
        time.advance_time(Duration::from_secs(61));

        // Add third entry - should evict key1 (oldest)
        let result = cache.record_access(key3, 100, AccessType::Get, 0, |_| false);
        assert!(result.is_new);
        assert_eq!(result.evicted, vec![(key1, 0)]);
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
        cache.record_access(key1, 100, AccessType::Get, 0, |_| false);
        cache.record_access(key2, 100, AccessType::Get, 0, |_| false);

        // Access key1 again - should move it to back of LRU
        cache.record_access(key1, 100, AccessType::Subscribe, 0, |_| false);

        // LRU order should now be [key2, key1]
        let order = cache.keys_lru_order();
        assert_eq!(order, vec![key2, key1]);

        // Advance past TTL and add key3 - should evict key2 (now oldest)
        time.advance_time(Duration::from_secs(61));
        let result = cache.record_access(key3, 100, AccessType::Get, 0, |_| false);

        assert_eq!(result.evicted, vec![(key2, 0)]);
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
        cache.record_access(key1, 100, AccessType::Get, 0, |_| false);
        cache.record_access(key2, 100, AccessType::Get, 0, |_| false);

        // Advance time by 50 seconds
        time.advance_time(Duration::from_secs(50));

        // Touch key1 (simulating UPDATE received)
        cache.touch(&key1);

        // Advance another 15 seconds (key1 now at 15s, key2 at 65s)
        time.advance_time(Duration::from_secs(15));

        // Add key3 - should evict key2 (past TTL), NOT key1 (recently touched)
        let result = cache.record_access(key3, 100, AccessType::Get, 0, |_| false);

        assert_eq!(
            result.evicted,
            vec![(key2, 0)],
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
        cache.record_access(small1, 100, AccessType::Get, 0, |_| false);
        cache.record_access(small2, 100, AccessType::Get, 0, |_| false);
        cache.record_access(small3, 100, AccessType::Get, 0, |_| false);
        assert_eq!(cache.current_bytes(), 300);

        // Advance past TTL
        time.advance_time(Duration::from_secs(61));

        // Add one large contract - should evict two small ones
        let result = cache.record_access(large, 200, AccessType::Put, 0, |_| false);

        assert_eq!(result.evicted.len(), 2);
        assert_eq!(result.evicted[0], (small1, 0)); // Oldest first
        assert_eq!(result.evicted[1], (small2, 0));
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
        cache.record_access(key1, 100, AccessType::Get, 0, |_| false);
        cache.record_access(key2, 100, AccessType::Get, 0, |_| false);
        cache.record_access(key3, 100, AccessType::Get, 0, |_| false);
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
        assert_eq!(evicted, vec![(key1, 0)]);
        assert_eq!(cache.current_bytes(), 200);
    }

    #[test]
    fn test_sweep_respects_should_retain() {
        let (mut cache, time) = make_cache(200, Duration::from_secs(60));
        let key1 = make_key(1);
        let key2 = make_key(2);
        let key3 = make_key(3);

        // Add three entries (exceeds budget)
        cache.record_access(key1, 100, AccessType::Get, 0, |_| false);
        cache.record_access(key2, 100, AccessType::Get, 0, |_| false);
        cache.record_access(key3, 100, AccessType::Get, 0, |_| false);

        // Advance past TTL
        time.advance_time(Duration::from_secs(61));

        // Sweep with predicate that retains key1
        let evicted = cache.sweep_expired(|k| *k == key1);

        // key1 should be retained, key2 evicted to get under budget
        assert_eq!(evicted, vec![(key2, 0)]);
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
        cache.record_access(retained, 100, AccessType::Get, 0, |_| false);
        cache.record_access(evictable, 100, AccessType::Get, 0, |_| false);
        assert_eq!(cache.current_bytes(), 200);

        // Advance past TTL so both existing entries are eviction-eligible.
        time.advance_time(Duration::from_secs(61));

        // Insert a third entry, over budget. `retained` is the oldest, so a
        // naive LRU eviction would drop it first — but `should_retain`
        // protects it, so `evictable` (next oldest) must be evicted instead.
        let result = cache.record_access(trigger, 100, AccessType::Get, 0, |k| *k == retained);

        assert_eq!(
            result.evicted,
            vec![(evictable, 0)],
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

    /// The contract inserted by `record_access` is age-0, so while
    /// `min_ttl > 0` (the production default) it is never evicted by the
    /// same call even when the insert pushes over budget.
    #[test]
    fn test_record_access_never_evicts_the_new_entry() {
        let (mut cache, time) = make_cache(100, Duration::from_secs(60));
        let first = make_key(1);
        let second = make_key(2);

        cache.record_access(first, 100, AccessType::Get, 0, |_| false);
        time.advance_time(Duration::from_secs(61));

        // Inserting `second` puts the cache over budget; `first` is past TTL
        // and unretained so it is evicted, but the just-inserted `second`
        // (age 0) must survive.
        let result = cache.record_access(second, 100, AccessType::Get, 0, |_| false);
        assert_eq!(result.evicted, vec![(first, 0)]);
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
        cache.record_access(key, 100, AccessType::Get, 0, |_| false);
        assert_eq!(cache.get(&key).unwrap().access_type, AccessType::Get);

        cache.record_access(key, 100, AccessType::Put, 0, |_| false);
        assert_eq!(cache.get(&key).unwrap().access_type, AccessType::Put);

        cache.record_access(key, 100, AccessType::Subscribe, 0, |_| false);
        assert_eq!(cache.get(&key).unwrap().access_type, AccessType::Subscribe);
    }

    #[test]
    fn test_contract_size_change() {
        let (mut cache, _) = make_cache(1000, Duration::from_secs(60));
        let key = make_key(1);

        // Add contract with initial size
        cache.record_access(key, 100, AccessType::Get, 0, |_| false);
        assert_eq!(cache.current_bytes(), 100);
        assert_eq!(cache.get(&key).unwrap().size_bytes, 100);

        // Contract state grows
        cache.record_access(key, 200, AccessType::Put, 0, |_| false);
        assert_eq!(cache.current_bytes(), 200);
        assert_eq!(cache.get(&key).unwrap().size_bytes, 200);

        // Contract state shrinks
        cache.record_access(key, 150, AccessType::Put, 0, |_| false);
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

        cache.record_access(key1, 100, AccessType::Get, 0, |_| false);
        cache.record_access(key2, 100, AccessType::Put, 0, |_| false);
        cache.record_access(key3, 100, AccessType::Subscribe, 0, |_| false);

        let keys: Vec<ContractKey> = cache.iter().collect();
        assert_eq!(keys.len(), 3);
        assert!(keys.contains(&key1));
        assert!(keys.contains(&key2));
        assert!(keys.contains(&key3));
    }

    /// `record_access` MUST capture the supplied `write_generation` on
    /// the inserted entry AND surface it on the `RecordAccessResult.evicted`
    /// tuple when that entry is later evicted by a subsequent
    /// `record_access`. This is the load-bearing flow for the
    /// EvictContract re-host race fix — `RuntimePool::remove_contract`
    /// compares this captured generation against the then-current
    /// generation to decide whether a write raced ahead of the eviction.
    /// Regression test for PR #4212 review round C.
    #[test]
    fn test_record_access_carries_write_generation_through_eviction() {
        let (mut cache, time) = make_cache(100, Duration::from_secs(60));
        let evicted_key = make_key(1);
        let trigger_key = make_key(2);

        // Insert `evicted_key` with a specific captured generation.
        let captured_generation: u64 = 0xABCD_EF42;
        let first = cache.record_access(
            evicted_key,
            100,
            AccessType::Get,
            captured_generation,
            |_| false,
        );
        assert!(first.evicted.is_empty(), "first insert evicts nothing");
        assert_eq!(
            cache
                .get(&evicted_key)
                .expect("just inserted")
                .write_generation,
            captured_generation,
            "captured generation must be stored on the HostedContract entry"
        );

        // Advance past TTL and trigger an over-budget insert; this must
        // evict `evicted_key` and emit the captured generation alongside
        // it on the result tuple.
        time.advance_time(Duration::from_secs(61));
        let result = cache.record_access(
            trigger_key,
            100,
            AccessType::Get,
            999, // trigger's own generation is irrelevant to the eviction
            |_| false,
        );
        assert_eq!(
            result.evicted,
            vec![(evicted_key, captured_generation)],
            "evicted tuple must carry the generation captured atomically \
             when the evicted entry was inserted"
        );
    }

    /// Re-accessing an existing entry refreshes its captured generation.
    ///
    /// Rationale: a re-access (GET/PUT/SUBSCRIBE) is a fresh "I'm
    /// hosting this state" assertion. If a state write bumped the
    /// generation between the original insert and the refresh, the
    /// refresh must adopt the new generation so a later eviction
    /// captures the correct generation snapshot.
    #[test]
    fn test_record_access_refresh_updates_write_generation() {
        let (mut cache, _) = make_cache(1000, Duration::from_secs(60));
        let key = make_key(7);

        cache.record_access(key, 100, AccessType::Get, 1, |_| false);
        assert_eq!(cache.get(&key).unwrap().write_generation, 1);

        // Refresh with a higher generation (simulates a state write that
        // bumped the generation between the original insert and a later
        // GET/PUT/SUBSCRIBE re-access).
        cache.record_access(key, 100, AccessType::Put, 5, |_| false);
        assert_eq!(
            cache.get(&key).unwrap().write_generation,
            5,
            "re-access must refresh the captured generation snapshot"
        );
    }

    /// Age gate: has_recent_local_client_access returns false after the
    /// max_age window expires. This is the TTL enforcement for the cleanup
    /// exemption rule (AGENTS.md).
    #[test]
    fn test_local_client_access_age_gate_expiry() {
        let lease = Duration::from_secs(480); // SUBSCRIPTION_LEASE_DURATION
        let (mut cache, time) = make_cache(10000, Duration::from_secs(60));
        let key = make_key(1);

        cache.record_access(key, 100, AccessType::Get, 0, |_| false);
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

    // --- Recently-abandoned priority bucket (Phase 1) ---

    #[test]
    fn test_record_abandonment_moves_entry_to_lru_front() {
        let (mut cache, _) = make_cache(1000, Duration::from_secs(60));
        let key1 = make_key(1);
        let key2 = make_key(2);
        let key3 = make_key(3);

        cache.record_access(key1, 100, AccessType::Get, 0, |_| false);
        cache.record_access(key2, 100, AccessType::Get, 0, |_| false);
        cache.record_access(key3, 100, AccessType::Get, 0, |_| false);

        // Insertion order without abandonment: key1, key2, key3.
        assert_eq!(cache.keys_lru_order(), vec![key1, key2, key3]);

        // Abandon key3 — the most recently used — and it should jump to
        // the FRONT so disk-pressure eviction sees it first.
        cache.record_abandonment(&key3);
        assert_eq!(cache.keys_lru_order(), vec![key3, key1, key2]);

        let info = cache.get(&key3).unwrap();
        assert!(
            info.abandoned_at.is_some(),
            "Abandonment must record a timestamp"
        );
    }

    #[test]
    fn test_record_abandonment_is_idempotent() {
        let (mut cache, time) = make_cache(1000, Duration::from_secs(60));
        let key = make_key(1);
        cache.record_access(key, 100, AccessType::Get, 0, |_| false);

        cache.record_abandonment(&key);
        let first = cache.get(&key).unwrap().abandoned_at;
        assert!(first.is_some());

        time.advance_time(Duration::from_secs(5));
        cache.record_abandonment(&key);
        let second = cache.get(&key).unwrap().abandoned_at;

        // First abandonment timestamp wins — we want to time-from the
        // moment use ended, not from later re-marks.
        assert_eq!(first, second);
    }

    #[test]
    fn test_record_abandonment_missing_key_is_noop() {
        let (mut cache, _) = make_cache(1000, Duration::from_secs(60));
        let key = make_key(42);
        // Should not panic, should not insert anything.
        cache.record_abandonment(&key);
        assert!(!cache.contains(&key));
        assert_eq!(cache.len(), 0);
    }

    #[test]
    fn test_record_access_clears_abandoned_marker() {
        let (mut cache, _) = make_cache(1000, Duration::from_secs(60));
        let key = make_key(1);
        cache.record_access(key, 100, AccessType::Get, 0, |_| false);
        cache.record_abandonment(&key);
        assert!(cache.get(&key).unwrap().abandoned_at.is_some());

        // Fresh access — a re-subscribe or re-GET — clears the marker
        // and returns the entry to the LRU tail.
        cache.record_access(key, 100, AccessType::Get, 0, |_| false);
        assert!(
            cache.get(&key).unwrap().abandoned_at.is_none(),
            "record_access must clear abandoned_at"
        );
    }

    #[test]
    fn test_touch_clears_abandoned_marker() {
        let (mut cache, _) = make_cache(1000, Duration::from_secs(60));
        let key = make_key(1);
        cache.record_access(key, 100, AccessType::Get, 0, |_| false);
        cache.record_abandonment(&key);

        cache.touch(&key);
        assert!(
            cache.get(&key).unwrap().abandoned_at.is_none(),
            "touch must clear abandoned_at"
        );
    }

    #[test]
    fn test_abandoned_entry_evicted_before_active_under_pressure() {
        // Two entries, 100 bytes each, budget 200. Adding a third
        // pushes the cache 100 bytes over budget. Both existing entries
        // are past TTL. Without the abandonment bump key1 (oldest)
        // evicts; with the bump key2 (abandoned-and-bumped) evicts
        // even though key1 is older.
        let (mut cache, time) = make_cache(200, Duration::from_secs(60));
        let key1 = make_key(1);
        let key2 = make_key(2);
        let key3 = make_key(3);

        cache.record_access(key1, 100, AccessType::Get, 0, |_| false);
        cache.record_access(key2, 100, AccessType::Get, 0, |_| false);

        // key2 is abandoned (latest in use, just lost its subscribers).
        cache.record_abandonment(&key2);

        // Both past TTL.
        time.advance_time(Duration::from_secs(61));

        // Insert key3: now over budget, must evict one. The abandoned
        // bucket means key2 goes first, even though key1 is the older
        // entry by `last_accessed`.
        let result = cache.record_access(key3, 100, AccessType::Get, 0, |_| false);
        assert!(result.is_new);
        assert_eq!(
            result.evicted,
            vec![(key2, 0)],
            "abandoned entry must evict first"
        );
        assert!(cache.contains(&key1));
        assert!(!cache.contains(&key2));
        assert!(cache.contains(&key3));
    }

    #[test]
    fn test_idle_persistence_preserved_when_under_pressure() {
        // Two entries, 100 bytes each, budget 1000. No pressure at all.
        // Abandoning an entry MUST NOT evict it — the network's
        // persistence-beyond-active-demand property only yields to
        // disk pressure, never to abandonment alone.
        let (mut cache, time) = make_cache(1000, Duration::from_secs(60));
        let key1 = make_key(1);
        let key2 = make_key(2);

        cache.record_access(key1, 100, AccessType::Get, 0, |_| false);
        cache.record_access(key2, 100, AccessType::Get, 0, |_| false);

        cache.record_abandonment(&key2);
        time.advance_time(Duration::from_secs(3600));

        // No budget pressure → sweep_expired evicts nothing, abandoned
        // or otherwise.
        let evicted = cache.sweep_expired(|_| false);
        assert!(evicted.is_empty(), "no pressure → no eviction");
        assert!(cache.contains(&key1));
        assert!(cache.contains(&key2));
    }

    // --- Capability-relative default budget + telemetry (A2) ---

    const MIB: u64 = 1024 * 1024;
    const GIB: u64 = 1024 * 1024 * 1024;

    /// The default budget scales with the memory the node may use, clamped to a
    /// sane floor/ceiling: `clamp(total_ram / 8, 128 MiB, 1 GiB)`.
    #[test]
    fn budget_for_ram_scales_and_clamps() {
        // Tiny box: total_ram / 8 is below the floor -> clamped up to MIN.
        assert_eq!(budget_for_ram(512 * MIB), MIN_DEFAULT_HOSTING_BUDGET_BYTES);
        assert_eq!(budget_for_ram(GIB), MIN_DEFAULT_HOSTING_BUDGET_BYTES);

        // Mid-range boxes scale linearly with RAM. The #4565 target: a 2 GiB VM
        // gets 256 MiB, far below the old flat 1 GiB, so it stops accumulating
        // hosted state toward OOM.
        assert_eq!(budget_for_ram(2 * GIB), 256 * MIB);
        assert_eq!(budget_for_ram(4 * GIB), 512 * MIB);

        // Ample-RAM box (>= 8 GiB) hits the ceiling, which equals the historical
        // flat default, so large gateways see NO change.
        assert_eq!(budget_for_ram(8 * GIB), MAX_DEFAULT_HOSTING_BUDGET_BYTES);
        assert_eq!(budget_for_ram(128 * GIB), MAX_DEFAULT_HOSTING_BUDGET_BYTES);
    }

    /// #4441 shape guard: the budget is RAM-SCALED, not a fixed count/constant.
    /// The old fixed >1024-contract cap was removed because it thrashed/OOM'd;
    /// the replacement budget must vary with RAM in the scaling band, and a
    /// memory-constrained box must get strictly LESS than a large one (and less
    /// than the old flat 1 GiB default).
    #[test]
    fn budget_for_ram_is_ram_scaled_not_fixed() {
        let small = budget_for_ram(2 * GIB);
        let large = budget_for_ram(6 * GIB);
        assert!(
            small < large,
            "budget must vary with RAM within the band, got small={small} large={large}"
        );
        assert!(
            small < MAX_DEFAULT_HOSTING_BUDGET_BYTES,
            "a memory-constrained box must get less than the old flat 1 GiB default"
        );
        // Genuinely a function of RAM, not a constant: distinct in-band inputs
        // give distinct budgets.
        assert_ne!(budget_for_ram(2 * GIB), budget_for_ram(3 * GIB));
        assert_ne!(budget_for_ram(3 * GIB), budget_for_ram(4 * GIB));
    }

    /// The real resolved default always lands within the documented clamp,
    /// whatever RAM the test host has.
    #[test]
    fn default_hosting_budget_within_clamp() {
        let b = default_hosting_budget_bytes();
        assert!(
            (MIN_DEFAULT_HOSTING_BUDGET_BYTES..=MAX_DEFAULT_HOSTING_BUDGET_BYTES).contains(&b),
            "default {b} must be within [{}, {}]",
            MIN_DEFAULT_HOSTING_BUDGET_BYTES,
            MAX_DEFAULT_HOSTING_BUDGET_BYTES,
        );
    }

    /// The budget-triggered eviction counter increments once per over-budget
    /// eviction and is surfaced via `stats()`; a TTL-protected or no-pressure
    /// cache leaves it untouched. This is the telemetry A2 ships to observe its
    /// own eviction rate.
    #[test]
    fn test_budget_eviction_counter_tracks_over_budget_evictions() {
        let (mut cache, time) = make_cache(200, Duration::from_secs(60));
        let key1 = make_key(1);
        let key2 = make_key(2);
        let key3 = make_key(3);

        cache.record_access(key1, 100, AccessType::Get, 0, |_| false);
        cache.record_access(key2, 100, AccessType::Get, 0, |_| false);
        assert_eq!(cache.stats().budget_evictions_total, 0);

        // Over budget but every entry is under TTL: no budget eviction yet.
        cache.record_access(key3, 100, AccessType::Get, 0, |_| false);
        assert_eq!(
            cache.stats().budget_evictions_total,
            0,
            "TTL-protected entries must not count as budget evictions"
        );

        // Past TTL: a sweep now evicts the single over-budget entry.
        time.advance_time(Duration::from_secs(61));
        let evicted = cache.sweep_expired(|_| false);
        assert_eq!(
            evicted.len(),
            1,
            "one 100-byte entry brings 300 back to 200"
        );
        let stats = cache.stats();
        assert_eq!(stats.budget_evictions_total, 1);
        assert_eq!(stats.current_bytes, 200);
        assert_eq!(stats.contract_count, 2);
        assert_eq!(stats.budget_bytes, 200);

        // No pressure now (at budget): the counter is unchanged.
        let evicted = cache.sweep_expired(|_| false);
        assert!(evicted.is_empty(), "at budget -> no further eviction");
        assert_eq!(
            cache.stats().budget_evictions_total,
            1,
            "counter must not advance with no budget pressure"
        );
    }
}
