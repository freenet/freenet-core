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
use std::time::{Duration, Instant};

use lru::LruCache;

/// Minimum gap between "cache is evicting at budget" operator warnings, so a
/// memory-bound node logs the signal periodically instead of once per eviction.
const EVICTION_WARN_INTERVAL: Duration = Duration::from_secs(300);

/// Number of evictions within one [`EVICTION_WARN_INTERVAL`] window before the
/// cache emits a warning. A handful of evictions is normal churn; sustained
/// eviction means the working set genuinely exceeds the budget.
const EVICTION_WARN_THRESHOLD: u64 = 16;

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
    /// Human-readable label for operator warnings ("contract" / "delegate").
    label: &'static str,
    /// Evictions counted since the start of the current warn window.
    evictions_in_window: u64,
    /// Start of the current warn window; `None` until the first eviction.
    /// Uses real wall-clock `Instant` deliberately: this is a rate-limit on an
    /// operator log line, not simulated node behavior, so it must advance in
    /// real time regardless of any paused test clock.
    window_started_at: Option<Instant>,
}

impl<K: Hash + Eq, V> ModuleCache<K, V> {
    /// Create an empty cache with the given byte budget and an unlabeled
    /// ("module") operator-warning tag. Prefer [`Self::with_label`] in
    /// production so the eviction warning identifies the cache.
    ///
    /// `budget_bytes` is clamped to at least 1 so a degenerate budget of 0
    /// still permits exactly one (most-recently-used) entry to be resident,
    /// keeping the cache functional rather than evicting on every insert.
    pub(crate) fn new(budget_bytes: usize) -> Self {
        Self::with_label(budget_bytes, "module")
    }

    /// Like [`Self::new`] but with a label ("contract" / "delegate") used in the
    /// rate-limited "cache is evicting at budget" operator warning.
    pub(crate) fn with_label(budget_bytes: usize, label: &'static str) -> Self {
        Self {
            // The LRU is unbounded by count; the byte budget is the only
            // bound. `usize::MAX` capacity means `LruCache` never evicts on
            // its own — we drive all eviction via the byte budget below.
            inner: LruCache::unbounded(),
            total_bytes: 0,
            budget_bytes: budget_bytes.max(1),
            label,
            evictions_in_window: 0,
            window_started_at: None,
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
        let mut evicted_this_insert: u64 = 0;
        while self.total_bytes > self.budget_bytes && self.inner.len() > 1 {
            if let Some((_, (_, evicted_size))) = self.inner.pop_lru() {
                self.total_bytes = self.total_bytes.saturating_sub(evicted_size);
                evicted_this_insert += 1;
            } else {
                break;
            }
        }
        if evicted_this_insert > 0 {
            self.note_evictions(evicted_this_insert);
        }
    }

    /// Track eviction volume and emit a RATE-LIMITED operator warning when the
    /// cache is persistently evicting at its budget.
    ///
    /// A memory-bound operator otherwise has no signal that the node is
    /// recompiling modules because the working set exceeds the cache budget
    /// (the pre-#4441 "increase capacity" warning was removed with nothing
    /// replacing it). We warn at most once per [`EVICTION_WARN_INTERVAL`], and
    /// only after at least [`EVICTION_WARN_THRESHOLD`] evictions in the window,
    /// so normal churn stays quiet but sustained thrash is visible.
    fn note_evictions(&mut self, count: u64) {
        let now = Instant::now();
        let window_start = *self.window_started_at.get_or_insert(now);
        self.evictions_in_window = self.evictions_in_window.saturating_add(count);

        if now.duration_since(window_start) >= EVICTION_WARN_INTERVAL {
            if self.evictions_in_window >= EVICTION_WARN_THRESHOLD {
                tracing::warn!(
                    cache = self.label,
                    evictions = self.evictions_in_window,
                    window_secs = EVICTION_WARN_INTERVAL.as_secs(),
                    budget_bytes = self.budget_bytes,
                    total_bytes = self.total_bytes,
                    entries = self.inner.len(),
                    "compiled-WASM {} cache is persistently evicting at its byte budget — \
                     the hosted working set exceeds the cache, so modules are being \
                     recompiled on access. Raise FREENET_MODULE_CACHE_BUDGET_BYTES / \
                     module-cache-budget-bytes if this node has spare RAM, or accept the \
                     recompile cost if it is memory-bound.",
                    self.label,
                );
            }
            // Reset the window regardless of whether we warned, so the next
            // warning reflects fresh activity.
            self.evictions_in_window = 0;
            self.window_started_at = Some(now);
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

/// Lower clamp for the default contract-module cache budget (64 MiB).
///
/// Even a tiny VPS should be able to hold a healthy working set of compiled
/// modules; at the measured ~0.7-1.5 MiB per module this still caches tens of
/// contracts. Below this the cache would thrash on a normal node.
pub const MIN_DEFAULT_MODULE_CACHE_BUDGET_BYTES: usize = 64 * 1024 * 1024;

/// Upper clamp for the default contract-module cache budget (384 MiB).
///
/// A big gateway hosts well over 1024 contracts, but past a few hundred
/// resident modules the working-set benefit flattens while the absolute memory
/// cost keeps rising. 384 MiB holds ~256-512 modules at the measured per-module
/// size — comfortably above a healthy gateway's hot set. Operators who truly
/// need more raise it explicitly via the config override below.
pub const MAX_DEFAULT_MODULE_CACHE_BUDGET_BYTES: usize = 384 * 1024 * 1024;

/// Fraction of total system RAM used to size the default contract cache budget.
///
/// We give the *contract* cache up to 1/8 of physical RAM (clamped to the
/// MIN/MAX above). 1/8 leaves the other 7/8 for the rest of the node (state
/// store, transport buffers, the OS, jemalloc dirty-page retention, and the
/// WASM instance arenas), and the contract cache is only one of several memory
/// consumers. The delegate cache gets a smaller fraction of the *resulting*
/// budget — see `DELEGATE_MODULE_CACHE_BUDGET_DIVISOR` — so the COMBINED
/// default ceiling stays well under what would OOM a small box.
const DEFAULT_MODULE_CACHE_RAM_DIVISOR: usize = 8;

/// The delegate-module cache's default budget is the contract budget divided by
/// this. Delegates are far fewer and smaller than contracts on a typical node,
/// so a quarter share is generous. This keeps the COMBINED default ceiling at
/// `contract_budget * (1 + 1/4) = 1.25 × contract_budget` — at the MAX clamp
/// that is 480 MiB total, vs. the previous 768 MiB (384 MiB × 2) that applied
/// the full budget to BOTH caches and could OOM a small VPS (issue #4441 was
/// "OOM on a small box"; the fix must not itself OOM that box).
pub const DELEGATE_MODULE_CACHE_BUDGET_DIVISOR: usize = 4;

/// Fallback total-RAM estimate (1 GiB) when the OS query fails.
///
/// Conservative: at 1 GiB the divisor yields 128 MiB, between the MIN and MAX
/// clamps, so an unknown-RAM host gets a sane mid-range budget rather than the
/// max.
const FALLBACK_TOTAL_RAM_BYTES: usize = 1024 * 1024 * 1024;

/// Default per-cache byte budget for the **contract** compiled-module cache,
/// scaled to the host's physical RAM and clamped to a sane floor/ceiling.
///
/// Returns `clamp(total_ram / 8, 64 MiB, 384 MiB)`. The same fix for issue
/// #4441 (a node hosting >1024 contracts thrashed/OOM'd the old count cap) must
/// not itself OOM a small box: a fixed 384-MiB-per-cache default applied to
/// both the contract AND delegate caches meant a ~768 MiB compiled-code ceiling
/// regardless of host size. Scaling the contract budget to RAM and giving the
/// delegate cache only a fraction (`DELEGATE_MODULE_CACHE_BUDGET_DIVISOR`) keeps
/// the COMBINED default ceiling safe on small hosts while still letting big
/// gateways cache a large working set.
///
/// The explicit `--module-cache-budget-bytes` flag /
/// `FREENET_MODULE_CACHE_BUDGET_BYTES` env / `module-cache-budget-bytes` config
/// field always overrides this default when set.
///
/// A representative compiled contract module measures roughly 0.7-1.5 MiB when
/// serialized (`Module::serialize().len()`) under the backend's `OptLevel::None`
/// — see `test_compiled_module_size_is_in_expected_range` in
/// `wasm_runtime/tests/cache.rs`, which measures and logs the real size.
pub fn default_module_cache_budget_bytes() -> usize {
    let total_ram = read_total_ram_bytes().unwrap_or(FALLBACK_TOTAL_RAM_BYTES);
    (total_ram / DEFAULT_MODULE_CACHE_RAM_DIVISOR).clamp(
        MIN_DEFAULT_MODULE_CACHE_BUDGET_BYTES,
        MAX_DEFAULT_MODULE_CACHE_BUDGET_BYTES,
    )
}

/// Best-effort read of the host's total physical RAM in bytes.
///
/// Portable and dependency-free:
/// - On Linux, parse `MemTotal` from `/proc/meminfo` (reported in KiB).
/// - On other unix, fall back to `sysconf(_SC_PHYS_PAGES) * sysconf(_SC_PAGESIZE)`.
/// - Otherwise return `None` so the caller uses `FALLBACK_TOTAL_RAM_BYTES`.
///
/// Never panics; any failure (missing file, parse error, non-positive sysconf)
/// yields `None`.
fn read_total_ram_bytes() -> Option<usize> {
    #[cfg(target_os = "linux")]
    {
        // /proc/meminfo line: "MemTotal:       16331752 kB"
        let meminfo = std::fs::read_to_string("/proc/meminfo").ok()?;
        for line in meminfo.lines() {
            if let Some(rest) = line.strip_prefix("MemTotal:") {
                let kib: usize = rest.split_whitespace().next()?.parse().ok()?;
                return kib.checked_mul(1024);
            }
        }
        None
    }
    #[cfg(all(unix, not(target_os = "linux")))]
    {
        // SAFETY: sysconf is a pure read of a system constant; no pointers.
        let pages = unsafe { libc::sysconf(libc::_SC_PHYS_PAGES) };
        let page_size = unsafe { libc::sysconf(libc::_SC_PAGESIZE) };
        if pages > 0 && page_size > 0 {
            (pages as usize).checked_mul(page_size as usize)
        } else {
            None
        }
    }
    #[cfg(not(unix))]
    {
        None
    }
}

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

    /// `remove` decrements `total_bytes` by exactly the removed entry's recorded
    /// size and returns the value; removing a missing key is a no-op.
    #[test]
    fn remove_decrements_total_bytes() {
        let mut cache: ModuleCache<u64, &'static str> = ModuleCache::new(1_000_000);
        cache.insert(0, "a", 100);
        cache.insert(1, "b", 250);
        assert_eq!(cache.total_bytes(), 350);
        assert_eq!(cache.len(), 2);

        // Removing an existing key returns the value and subtracts its size.
        assert_eq!(cache.remove(&0), Some("a"));
        assert_eq!(
            cache.total_bytes(),
            250,
            "removed entry's 100 bytes subtracted"
        );
        assert_eq!(cache.len(), 1);

        // Removing a missing key is a no-op (total unchanged).
        assert_eq!(cache.remove(&42), None);
        assert_eq!(cache.total_bytes(), 250);
        assert_eq!(cache.len(), 1);

        // Removing the last entry zeroes the total.
        assert_eq!(cache.remove(&1), Some("b"));
        assert_eq!(cache.total_bytes(), 0);
        assert_eq!(cache.len(), 0);
    }

    /// The RAM-scaled default budget is always within the documented clamp
    /// `[64 MiB, 384 MiB]`, on any host (the `/proc/meminfo` read or its
    /// fallback both feed through the clamp).
    #[test]
    fn default_budget_is_within_clamp() {
        let b = default_module_cache_budget_bytes();
        assert!(
            (MIN_DEFAULT_MODULE_CACHE_BUDGET_BYTES..=MAX_DEFAULT_MODULE_CACHE_BUDGET_BYTES)
                .contains(&b),
            "default budget {b} must be within [{MIN_DEFAULT_MODULE_CACHE_BUDGET_BYTES}, \
             {MAX_DEFAULT_MODULE_CACHE_BUDGET_BYTES}]"
        );
    }

    /// The COMBINED default ceiling (contract budget + delegate budget) stays
    /// well under what would OOM a small box. With the delegate cache at
    /// `1/DELEGATE_MODULE_CACHE_BUDGET_DIVISOR` of the contract budget, the
    /// combined max is `MAX * (1 + 1/divisor)` — must be < 512 MiB.
    #[test]
    fn combined_default_ceiling_is_safe() {
        let contract = MAX_DEFAULT_MODULE_CACHE_BUDGET_BYTES;
        let delegate = contract / DELEGATE_MODULE_CACHE_BUDGET_DIVISOR;
        let combined = contract + delegate;
        assert!(
            combined < 512 * 1024 * 1024,
            "combined default ceiling {combined} must stay under 512 MiB so the \
             #4441 OOM fix doesn't itself OOM a small box (was 768 MiB pre-fix)"
        );
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
