//! Byte-budget-bounded LRU cache for compiled WASM modules.
//!
//! # Why a byte budget instead of a count cap
//!
//! The compiled-module caches used to be count-bounded: a fixed
//! `LruCache<K, Module>` holding at most `DEFAULT_MODULE_CACHE_CAPACITY`
//! (1024) entries. That cap was wrong on both ends:
//!
//! - A node hosting **more than 1024 contracts** thrashes: every cold
//!   contract evicts a warm one, and the evicted module must be recompiled
//!   (Cranelift) on its next access. Under sustained load this is an
//!   eviction-recompilation cycle that pins CPU and stalls the
//!   single-threaded `contract_handling` loop. This is the root cause of
//!   issue #4441 (module-cache thrash / OOM / HANG on nodes hosting >1024
//!   contracts).
//! - The cap ignored module *size*. 1024 tiny modules and 1024 large
//!   modules have wildly different memory footprints, so a count cap can
//!   either waste the budget (many tiny modules well under the count) or
//!   blow past available RAM (many large modules at the count).
//!
//! [`ModuleCache`] instead bounds the cache by the **total compiled byte
//! size** of its entries. Each entry records its size once at insert time;
//! on insert, the least-recently-used entries are evicted until the running
//! `total_bytes` is back within `budget_bytes`. This lets a node hold as
//! many small modules as fit, while still bounding the absolute memory the
//! cache can consume regardless of how many contracts the node hosts.
//!
//! # Relationship to wasmtime memory ownership
//!
//! The cached value is a wasmtime [`Module`](wasmtime::Module), which owns
//! its compiled machine code via an internal `Arc<CodeMemory>`. Wasmtime
//! frees that compiled code when the last `Module` clone is dropped (see
//! `wasmtime_engine::tests::test_module_drop_frees_memory`), so evicting an
//! entry here genuinely releases the compiled code as long as no live
//! `RunningInstance` still holds a clone. There is **no** unbounded
//! engine-lifetime code accumulation (the historical Wasmer `code_memory`
//! growth this cache's docs used to cite does not apply to the wasmtime
//! backend).

use std::borrow::Borrow;
use std::hash::Hash;

use lru::LruCache;

/// A least-recently-used cache bounded by the total byte size of its values.
///
/// Unlike a plain count-capped `LruCache`, this evicts based on the sum of
/// per-entry byte sizes (`size_of_value`, supplied at insert time). On every
/// insert it evicts LRU entries until `total_bytes <= budget_bytes`.
///
/// `K` is the key type; `V` is the cached value (e.g. a compiled
/// `wasmtime::Module`). The cache never panics on a single oversized value:
/// if one value alone exceeds the budget it is still stored (so the contract
/// can run) and will be the first thing evicted on the next insert.
pub(crate) struct ModuleCache<K: Hash + Eq, V> {
    /// Inner LRU. The value is `(value, size_in_bytes)`; size is captured at
    /// insert time so eviction accounting never re-measures (re-measuring a
    /// `Module` would require an expensive `serialize()`).
    inner: LruCache<K, (V, usize)>,
    /// Running sum of the `size` component of every entry in `inner`.
    /// Invariant: equals the sum of all entries' recorded sizes.
    total_bytes: usize,
    /// Eviction threshold in bytes. After every insert, LRU entries are
    /// evicted until `total_bytes <= budget_bytes`.
    budget_bytes: usize,
}

impl<K: Hash + Eq, V> ModuleCache<K, V> {
    /// Create an empty cache with the given byte budget.
    ///
    /// `budget_bytes` is clamped to at least 1 so a degenerate budget of 0
    /// still permits exactly one (most-recently-used) entry to be resident,
    /// keeping the cache functional rather than evicting on every insert.
    pub(crate) fn new(budget_bytes: usize) -> Self {
        Self {
            // The LRU is unbounded by count; the byte budget is the only
            // bound. `usize::MAX` capacity means `LruCache` never evicts on
            // its own — we drive all eviction via the byte budget below.
            inner: LruCache::unbounded(),
            total_bytes: 0,
            budget_bytes: budget_bytes.max(1),
        }
    }

    /// Look up a key, marking it most-recently-used on a hit. Returns a
    /// reference to the cached value (not the size).
    pub(crate) fn get<Q>(&mut self, key: &Q) -> Option<&V>
    where
        K: Borrow<Q>,
        Q: Hash + Eq + ?Sized,
    {
        self.inner.get(key).map(|(v, _)| v)
    }

    /// Insert (or replace) a value, recording its byte size, then evict LRU
    /// entries until the total is within budget.
    ///
    /// If `key` was already present, its old size is removed from the running
    /// total before the new size is added (replacement, not double-count).
    pub(crate) fn insert(&mut self, key: K, value: V, size_bytes: usize) {
        // `put` returns the displaced value when the key already existed.
        // Account for the replaced entry's size so the running total stays
        // exact across overwrites.
        if let Some((_, old_size)) = self.inner.put(key, (value, size_bytes)) {
            self.total_bytes = self.total_bytes.saturating_sub(old_size);
        }
        self.total_bytes = self.total_bytes.saturating_add(size_bytes);
        self.evict_to_budget();
    }

    /// Evict least-recently-used entries until `total_bytes <= budget_bytes`,
    /// keeping at least the single most-recently-used entry resident even if
    /// it alone exceeds the budget (so an oversized contract can still run).
    fn evict_to_budget(&mut self) {
        while self.total_bytes > self.budget_bytes && self.inner.len() > 1 {
            if let Some((_, (_, evicted_size))) = self.inner.pop_lru() {
                self.total_bytes = self.total_bytes.saturating_sub(evicted_size);
            } else {
                break;
            }
        }
    }

    /// Remove a key if present, decrementing the running byte total. Returns
    /// the removed value (without its recorded size).
    pub(crate) fn remove<Q>(&mut self, key: &Q) -> Option<V>
    where
        K: Borrow<Q>,
        Q: Hash + Eq + ?Sized,
    {
        let (value, size) = self.inner.pop(key)?;
        self.total_bytes = self.total_bytes.saturating_sub(size);
        Some(value)
    }

    /// Total bytes currently tracked across all resident entries.
    ///
    /// Test-only observability: the production paths only `get`/`insert`/`remove`.
    #[cfg(test)]
    pub(crate) fn total_bytes(&self) -> usize {
        self.total_bytes
    }

    /// Number of resident entries (test-only observability).
    #[cfg(test)]
    pub(crate) fn len(&self) -> usize {
        self.inner.len()
    }

    /// The configured byte budget (test-only observability).
    #[cfg(test)]
    pub(crate) fn budget_bytes(&self) -> usize {
        self.budget_bytes
    }
}

/// Default per-cache byte budget for compiled WASM modules.
///
/// **Current value: 384 MiB per cache (contract cache and delegate cache each
/// get their own budget).**
///
/// # How this was chosen
///
/// A representative compiled contract module measures roughly 0.7-1.5 MiB
/// when serialized (`Module::serialize().len()`) under the backend's
/// `OptLevel::None` configuration — see the `module_cache_budget` integration
/// test, which measures and logs the real size of the bundled test contracts.
/// Production gateways host well over 1024 contracts on machines with tens of
/// GiB of RAM, so the previous 1024-*count* cap was both too small (it
/// thrashed past 1024 contracts) and size-blind.
///
/// 384 MiB holds on the order of 256-512 distinct compiled modules at the
/// measured per-module size — comfortably above a healthy gateway's working
/// set — while bounding the cache's absolute memory footprint. Because both
/// the contract and delegate caches are *shared* across all pool executors
/// (one instance each, not one per executor), the real ceiling is one budget
/// per cache type, not per executor. Operators on very large nodes can raise
/// this via `FREENET_MODULE_CACHE_BUDGET_BYTES` / the `module-cache-budget-bytes`
/// config field.
pub const DEFAULT_MODULE_CACHE_BUDGET_BYTES: usize = 384 * 1024 * 1024;

#[cfg(test)]
mod tests {
    use super::*;

    /// Eviction is by BYTES, not count: many small entries (far more than the
    /// old 1024 count cap) all fit when their total stays within budget.
    #[test]
    fn many_small_entries_fit_when_total_within_budget() {
        // Budget for 5000 entries of 1000 bytes each = 5_000_000 bytes.
        let mut cache: ModuleCache<u64, ()> = ModuleCache::new(5_000_000);
        for k in 0..5000u64 {
            cache.insert(k, (), 1000);
        }
        assert_eq!(
            cache.len(),
            5000,
            "5000 small entries (far above the old 1024 count cap) should all fit"
        );
        assert_eq!(cache.total_bytes(), 5_000_000);
        assert!(cache.total_bytes() <= cache.budget_bytes());
        // Every key is still resident.
        for k in 0..5000u64 {
            assert!(cache.get(&k).is_some(), "key {k} should still be cached");
        }
    }

    /// Eviction is by BYTES, not count: a handful of large entries is evicted
    /// well *below* the old 1024 count cap once their total exceeds budget.
    #[test]
    fn large_entries_evicted_below_old_count_cap() {
        // 10 MiB budget, 4 MiB entries: only 2 fit at a time (8 MiB),
        // a third forces eviction even though 3 << 1024.
        let budget = 10 * 1024 * 1024;
        let entry = 4 * 1024 * 1024;
        let mut cache: ModuleCache<u64, ()> = ModuleCache::new(budget);
        cache.insert(0, (), entry);
        cache.insert(1, (), entry);
        assert_eq!(cache.len(), 2);
        assert_eq!(cache.total_bytes(), 2 * entry);

        // Third large entry: total would be 12 MiB > 10 MiB budget, so the
        // LRU (key 0) is evicted.
        cache.insert(2, (), entry);
        assert!(
            cache.total_bytes() <= budget,
            "total_bytes {} must stay within budget {}",
            cache.total_bytes(),
            budget
        );
        assert_eq!(cache.len(), 2, "evicted below the old 1024 count cap");
        assert!(cache.get(&0).is_none(), "LRU entry 0 should be evicted");
        assert!(cache.get(&1).is_some(), "entry 1 should remain");
        assert!(cache.get(&2).is_some(), "entry 2 should remain");
    }

    /// `get` marks an entry most-recently-used, protecting it from the next
    /// eviction.
    #[test]
    fn get_refreshes_lru_recency() {
        let budget = 10 * 1024 * 1024;
        let entry = 4 * 1024 * 1024;
        let mut cache: ModuleCache<u64, ()> = ModuleCache::new(budget);
        cache.insert(0, (), entry);
        cache.insert(1, (), entry);
        // Touch key 0 so key 1 becomes the LRU.
        assert!(cache.get(&0).is_some());
        cache.insert(2, (), entry);
        assert!(
            cache.get(&1).is_none(),
            "untouched entry 1 should be evicted"
        );
        assert!(
            cache.get(&0).is_some(),
            "recently-touched entry 0 should remain"
        );
        assert!(cache.get(&2).is_some());
    }

    /// Replacing an existing key updates the running total instead of
    /// double-counting it.
    #[test]
    fn replacing_key_updates_total_bytes() {
        let mut cache: ModuleCache<u64, ()> = ModuleCache::new(1_000_000);
        cache.insert(0, (), 100);
        assert_eq!(cache.total_bytes(), 100);
        cache.insert(0, (), 250);
        assert_eq!(cache.len(), 1, "replacement, not a second entry");
        assert_eq!(
            cache.total_bytes(),
            250,
            "old size removed, new size added — no double count"
        );
    }

    /// A single value larger than the whole budget is still stored (so the
    /// contract can run) and is the lone resident entry.
    #[test]
    fn oversized_single_value_is_retained() {
        let mut cache: ModuleCache<u64, ()> = ModuleCache::new(1000);
        cache.insert(0, (), 5000);
        assert_eq!(cache.len(), 1, "oversized value still stored");
        assert_eq!(cache.total_bytes(), 5000);
        assert!(cache.get(&0).is_some());

        // Inserting a second entry evicts the oversized LRU one, never
        // leaving the cache empty mid-insert.
        cache.insert(1, (), 200);
        assert_eq!(cache.len(), 1);
        assert!(cache.get(&0).is_none(), "oversized LRU evicted");
        assert!(cache.get(&1).is_some());
        assert_eq!(cache.total_bytes(), 200);
    }

    /// A zero budget still keeps exactly one most-recently-used entry resident
    /// (clamped to a 1-byte budget) rather than evicting on every insert.
    #[test]
    fn zero_budget_keeps_one_entry() {
        let mut cache: ModuleCache<u64, ()> = ModuleCache::new(0);
        assert_eq!(cache.budget_bytes(), 1, "budget clamped up to 1");
        cache.insert(0, (), 100);
        cache.insert(1, (), 100);
        assert_eq!(cache.len(), 1, "at most one entry survives a zero budget");
        assert!(cache.get(&1).is_some(), "most-recent entry kept");
        assert!(cache.get(&0).is_none());
    }
}
