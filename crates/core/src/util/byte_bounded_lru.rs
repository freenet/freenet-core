//! A count-capped LRU cache with a hard total-byte backstop.
//!
//! Extracted verbatim from `contract::executor` (where #4804 built it) so the
//! interest manager's delta cache can reuse the same primitive instead of
//! growing a second copy of the same accounting (#4805). The executor's use is
//! unchanged; see [`crate::contract::executor`]'s cache-sizing comment for how
//! that caller derives its count target and byte budget.
//!
//! The values these caches hold (`StateSummary` / `StateDelta`) are
//! CONTRACT-CONTROLLED and variable-size, so a count cap alone cannot bound RAM
//! — the "per-key collections influenced by external actors MUST be
//! size-bounded" amplification rule (#4565 OOM class). Every caller therefore
//! pairs a count target (coverage) with a byte budget (safety).

use std::num::NonZeroUsize;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use lru::LruCache;

/// Per-entry structural-overhead allowance (bytes) added to every value's
/// payload length when accounting for the byte budget.
///
/// Two jobs:
///   - It covers the real per-entry overhead the payload length ignores — the key
///     (`ContractKey` / `(ContractKey, u64, u64)`), the `LruCache` node's
///     prev/next pointers and boxed entry, and the map slot (~100-250 B combined).
///     Adding it on TOP of the payload makes the counted total a genuine upper
///     bound on retained RAM, so the byte budget is a true hard cap (not the
///     ~1.5x-of-budget story an un-floored weigher would give).
///   - It floors each entry's weight so even EMPTY values still count. A contract
///     legitimately returns an empty delta once a peer is current; without a
///     floor an unbounded stream of distinct zero-weight keys would never be
///     evicted and the entry count (with its uncounted overhead) would grow
///     without bound — the #4565 OOM class this budget exists to close (same
///     failure the closed PR #4794 fixed with its delta-cache floor). With the
///     floor the entry COUNT is capped at `byte_budget / CACHE_ENTRY_OVERHEAD_BYTES`.
pub(crate) const CACHE_ENTRY_OVERHEAD_BYTES: usize = 512;

/// Node-wide occupancy gauges that one or more [`ByteBoundedLruCache`]s publish
/// into, so the cache's real footprint is observable in production telemetry.
///
/// Every attached cache adds its contribution (entries, counted bytes, byte
/// budget) on attach and on every mutation, and REMOVES it on drop, so the
/// gauges always equal the sum over the caches currently alive. That makes them
/// correct whether a node holds one shared cache or several per-executor ones,
/// and keeps a replaced/dropped cache from leaving a phantom contribution.
///
/// `evictions` is a monotonic lifetime counter: every entry removed to honor
/// either bound (count cap or byte budget). Replacing a key's value is not an
/// eviction, and neither is refusing an oversized value (it was never cached).
#[derive(Debug, Default)]
pub(crate) struct ByteLruGauges {
    entries: AtomicU64,
    bytes: AtomicU64,
    budget_bytes: AtomicU64,
    evictions: AtomicU64,
}

/// Point-in-time read of [`ByteLruGauges`]. The four loads are independent, so a
/// read racing a put can be off by one entry; fine for a 5-minute gauge.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(crate) struct ByteLruGaugeSnapshot {
    pub entries: u64,
    pub bytes: u64,
    pub budget_bytes: u64,
    pub evictions_total: u64,
}

impl ByteLruGauges {
    pub(crate) fn snapshot(&self) -> ByteLruGaugeSnapshot {
        ByteLruGaugeSnapshot {
            entries: self.entries.load(Ordering::Relaxed),
            bytes: self.bytes.load(Ordering::Relaxed),
            budget_bytes: self.budget_bytes.load(Ordering::Relaxed),
            evictions_total: self.evictions.load(Ordering::Relaxed),
        }
    }

    /// Apply a signed change to a gauge. `fetch_sub` on a `u64` would wrap if a
    /// caller ever subtracted more than it added; the contributions here are
    /// exact (each cache removes only what it added), so this cannot underflow,
    /// but `saturating` keeps a future accounting slip from emitting 2^64.
    fn adjust(gauge: &AtomicU64, before: usize, after: usize) {
        if after >= before {
            gauge.fetch_add((after - before) as u64, Ordering::Relaxed);
        } else {
            let dec = (before - after) as u64;
            let mut current = gauge.load(Ordering::Relaxed);
            while let Err(actual) = gauge.compare_exchange_weak(
                current,
                current.saturating_sub(dec),
                Ordering::Relaxed,
                Ordering::Relaxed,
            ) {
                current = actual;
            }
        }
    }
}

/// A count-capped LRU cache with a hard total-byte backstop.
///
/// Wraps [`lru::LruCache`] with running byte accounting so eviction is driven by
/// EITHER bound, whichever binds first:
///
///   - the LRU's own COUNT cap (`inner.cap()`, which the caller sets for
///     coverage and may raise via [`Self::grow`] — the executor grows it to the
///     live hosted count; the interest manager leaves it at its fixed target),
///     and
///   - a fixed BYTE budget (`byte_budget`): after every insert, LRU entries are
///     popped until `total_bytes <= byte_budget`.
///
/// The values every caller stores (`StateSummary` / `StateDelta`) are
/// contract-controlled and variable-size, so the count cap ALONE cannot bound
/// RAM and the byte budget ALONE would make coverage a contract-size
/// assumption. Both together: small digests → count binds (coverage); large
/// values → bytes bind (safety). For how each caller derives its two numbers
/// see the cache-sizing comment in [`crate::contract::executor`] and the
/// delta-cache byte-backstop comment in [`crate::ring::interest`].
///
/// A single value whose accounted weight alone exceeds the whole budget is NOT
/// cached: [`Self::put`] returns early without inserting it. The values are
/// contract-controlled and can reach the WASM memory limit, so retaining even one
/// oversized entry (times every pool worker times both caches) would defeat the
/// hard cap and is a real OOM vector (#4565). This deliberately does NOT match
/// [`crate::wasm_runtime::ModuleCache`]'s "keep one oversized entry" handling:
/// that cache's values are trusted operator-supplied modules; these are not, so
/// they get no oversized exemption. Net: `total_bytes <= byte_budget` holds
/// STRICTLY after every put.
pub(crate) struct ByteBoundedLruCache<K: std::hash::Hash + Eq, V> {
    inner: LruCache<K, V>,
    /// Running sum of every resident entry's weight
    /// (`weigh(value) + CACHE_ENTRY_OVERHEAD_BYTES`). Invariant: equals
    /// the sum over all entries.
    total_bytes: usize,
    /// Hard eviction threshold in bytes.
    byte_budget: usize,
    /// Payload byte size of a value; the per-entry structural overhead is added
    /// on top in [`Self::entry_weight`].
    weigh: fn(&V) -> usize,
    /// Lifetime count of entries this cache evicted to honor either bound.
    evictions: u64,
    /// Optional node-wide gauges this cache publishes its occupancy into. See
    /// [`ByteLruGauges`]; `None` for callers that export nothing.
    gauges: Option<Arc<ByteLruGauges>>,
}

impl<K: std::hash::Hash + Eq, V> ByteBoundedLruCache<K, V> {
    pub(crate) fn new(count_cap: NonZeroUsize, byte_budget: usize, weigh: fn(&V) -> usize) -> Self {
        Self {
            inner: LruCache::new(count_cap),
            total_bytes: 0,
            byte_budget: byte_budget.max(1),
            weigh,
            evictions: 0,
            gauges: None,
        }
    }

    /// Publish this cache's occupancy into `gauges` from now on (and its budget
    /// immediately). The contribution is removed again when the cache drops.
    pub(crate) fn with_gauges(mut self, gauges: Arc<ByteLruGauges>) -> Self {
        ByteLruGauges::adjust(&gauges.budget_bytes, 0, self.byte_budget);
        ByteLruGauges::adjust(&gauges.entries, 0, self.inner.len());
        ByteLruGauges::adjust(&gauges.bytes, 0, self.total_bytes);
        self.gauges = Some(gauges);
        self
    }

    /// The hard byte budget this cache enforces.
    #[cfg(test)]
    pub(crate) fn byte_budget(&self) -> usize {
        self.byte_budget
    }

    /// Counted weight of one entry: payload length plus the per-entry structural
    /// overhead allowance (which also floors empty values above zero).
    fn entry_weight(&self, value: &V) -> usize {
        (self.weigh)(value).saturating_add(CACHE_ENTRY_OVERHEAD_BYTES)
    }

    /// Look up a key, marking it most-recently-used on a hit.
    pub(crate) fn get(&mut self, key: &K) -> Option<&V> {
        self.inner.get(key)
    }

    /// Insert (or replace) a value, then evict LRU entries until within the byte
    /// budget. A value whose accounted weight alone exceeds the budget is NOT
    /// cached (early return): the values are contract-controlled, so caching one
    /// would defeat the hard cap, and the caller already owns its own copy of the
    /// result, so caching buys nothing. Any pre-existing entry under the same key
    /// is left untouched (it was already within budget, so the invariant holds).
    pub(crate) fn put(&mut self, key: K, value: V) {
        let (len_before, bytes_before, evictions_before) =
            (self.inner.len(), self.total_bytes, self.evictions);
        self.put_inner(key, value);
        if let Some(g) = &self.gauges {
            ByteLruGauges::adjust(&g.entries, len_before, self.inner.len());
            ByteLruGauges::adjust(&g.bytes, bytes_before, self.total_bytes);
            g.evictions
                .fetch_add(self.evictions - evictions_before, Ordering::Relaxed);
        }
    }

    fn put_inner(&mut self, key: K, value: V) {
        let added = self.entry_weight(&value);
        // Skip-oversized guard (#4565): a single value larger than the whole
        // budget would otherwise stay resident (the pop-loop below keeps the MRU
        // entry), leaving total_bytes > byte_budget and breaking the hard cap.
        // StateSummary/StateDelta are contract-controlled and can reach the WASM
        // memory limit, so refuse to cache such a value at all. The caller already
        // owns its result; a later cache miss simply recomputes. Result:
        // total_bytes <= byte_budget holds STRICTLY after every put.
        if added > self.byte_budget {
            return;
        }
        // `push` returns the displaced entry: the OLD value when `key` already
        // existed, OR the LRU entry evicted to honor the COUNT cap. In BOTH cases
        // subtract its weight so the running total stays exact (a replace does not
        // grow the count, so it never also evicts — exactly one of the two).
        // `contains` (no recency update) tells a same-key REPLACE apart from a
        // count-cap EVICTION, which `push` reports identically.
        let replacing = self.inner.contains(&key);
        if let Some((_, displaced)) = self.inner.push(key, value) {
            self.total_bytes = self
                .total_bytes
                .saturating_sub(self.entry_weight(&displaced));
            if !replacing {
                self.evictions += 1;
            }
        }
        self.total_bytes = self.total_bytes.saturating_add(added);
        // Byte backstop: pop LRU entries until within budget. With the
        // skip-oversized guard above, the just-inserted entry alone is always
        // within budget, so this converges to total_bytes <= byte_budget every
        // time. The len() > 1 guard is kept as defense-in-depth but is no longer
        // what bounds the total (no entry can alone exceed the budget now).
        while self.total_bytes > self.byte_budget && self.inner.len() > 1 {
            match self.inner.pop_lru() {
                Some((_, evicted)) => {
                    self.total_bytes = self.total_bytes.saturating_sub(self.entry_weight(&evicted));
                    self.evictions += 1;
                }
                None => break,
            }
        }
    }

    /// The current COUNT cap.
    pub(crate) fn cap(&self) -> NonZeroUsize {
        self.inner.cap()
    }

    /// Grow the COUNT cap. Only ever grows (callers guard on `new > cap`), so this
    /// never evicts and the byte total stays exact. A shrink WOULD evict entries
    /// via `lru::resize` WITHOUT byte accounting, drifting `total_bytes` high, so a
    /// non-growing request is made a no-op in RELEASE too (not just a debug
    /// assert): the early return below is the real guard against a future
    /// non-monotonic caller.
    pub(crate) fn grow(&mut self, cap: NonZeroUsize) {
        debug_assert!(
            cap >= self.inner.cap(),
            "ByteBoundedLruCache::grow must not shrink (would leak byte accounting)"
        );
        // Release-safe guard: a shrink (or a no-op re-grow to the same cap) must
        // not reach `lru::resize`, which would evict without updating `total_bytes`.
        if cap <= self.inner.cap() {
            return;
        }
        self.inner.resize(cap);
    }

    #[cfg(test)]
    pub(crate) fn total_bytes(&self) -> usize {
        self.total_bytes
    }

    #[cfg(test)]
    pub(crate) fn len(&self) -> usize {
        self.inner.len()
    }

    #[cfg(test)]
    pub(crate) fn evictions(&self) -> u64 {
        self.evictions
    }
}

impl<K: std::hash::Hash + Eq, V> Drop for ByteBoundedLruCache<K, V> {
    /// Withdraw this cache's contribution from the shared gauges, so a dropped
    /// or replaced cache does not leave phantom entries/bytes/budget behind.
    /// The eviction counter is lifetime-monotonic and is deliberately kept.
    fn drop(&mut self) {
        if let Some(g) = &self.gauges {
            ByteLruGauges::adjust(&g.entries, self.inner.len(), 0);
            ByteLruGauges::adjust(&g.bytes, self.total_bytes, 0);
            ByteLruGauges::adjust(&g.budget_bytes, self.byte_budget, 0);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // `&Vec<u8>` (not `&[u8]`) is required here: this is passed as a
    // `fn(&V) -> usize` to `ByteBoundedLruCache::new` with `V = Vec<u8>`,
    // and a bare fn-item's signature must match the generic parameter's
    // instantiated type exactly — `&[u8]` would not coerce.
    #[allow(
        clippy::ptr_arg,
        reason = "must match ByteBoundedLruCache<_, Vec<u8>>'s fn(&V) -> usize weigh signature exactly"
    )]
    fn vec_len(v: &Vec<u8>) -> usize {
        v.len()
    }

    /// P1 regression: a contract-controlled cache VALUE is variable-size, so the
    /// COUNT cap alone cannot bound RAM. With a huge count cap but a modest byte
    /// budget, inserting many LARGE values must keep total retained bytes under
    /// the byte budget (holding far fewer than the count cap) — otherwise a
    /// large-summary/large-delta contract could pin gigabytes and OOM the node
    /// (#4565 class). Without the byte backstop the count cap would let all 200
    /// one-MiB values (~200 MiB) stay resident.
    #[test]
    fn byte_budget_bounds_ram_for_large_values() {
        let byte_budget = 8 * 1024 * 1024; // 8 MiB
        let count_cap = NonZeroUsize::new(65_536).unwrap(); // effectively unbounded here
        let mut cache: ByteBoundedLruCache<u64, Vec<u8>> =
            ByteBoundedLruCache::new(count_cap, byte_budget, vec_len);

        // Insert 200 distinct 1-MiB values (200 MiB total if unbounded).
        for i in 0..200u64 {
            cache.put(i, vec![0u8; 1024 * 1024]);
            assert!(
                cache.total_bytes() <= byte_budget,
                "total_bytes {} exceeded byte_budget {} after insert {}",
                cache.total_bytes(),
                byte_budget,
                i
            );
        }

        // At ~1 MiB/entry an 8 MiB budget holds <= 8 entries — nowhere near the
        // 65_536 count cap. The byte backstop, not the count cap, bound the RAM.
        assert!(
            cache.len() <= 8,
            "byte budget must hold far fewer than the count cap; held {}",
            cache.len()
        );
        assert!(
            cache.total_bytes() <= byte_budget,
            "final total_bytes {} must be within byte_budget {}",
            cache.total_bytes(),
            byte_budget
        );
    }

    /// P1 skip-oversized: a value whose accounted weight alone exceeds the
    /// byte budget is NOT cached at all, so `total_bytes` stays 0 and the
    /// cache stays empty. A within-budget value inserted afterward caches
    /// normally and stays within budget. Pins the skip-oversized fix (#4565).
    #[test]
    fn oversized_value_is_not_cached() {
        let byte_budget = 8 * 1024 * 1024; // 8 MiB
        let count_cap = NonZeroUsize::new(65_536).unwrap();
        let mut cache: ByteBoundedLruCache<u64, Vec<u8>> =
            ByteBoundedLruCache::new(count_cap, byte_budget, vec_len);

        // A 16-MiB value alone exceeds the 8-MiB budget, so it must be refused.
        cache.put(1, vec![0u8; 16 * 1024 * 1024]);
        assert!(
            cache.get(&1).is_none(),
            "an over-budget value must not be cached"
        );
        assert_eq!(
            cache.len(),
            0,
            "cache must stay empty after an over-budget put"
        );
        assert_eq!(
            cache.total_bytes(),
            0,
            "total_bytes must stay 0 when nothing was cached"
        );

        // A normal-sized value still caches and stays within budget.
        cache.put(2, vec![0u8; 1024]);
        assert!(cache.get(&2).is_some(), "a within-budget value must cache");
        assert_eq!(cache.len(), 1);
        assert!(
            cache.total_bytes() <= byte_budget,
            "total_bytes {} must stay within budget {}",
            cache.total_bytes(),
            byte_budget
        );
    }

    /// The per-entry overhead floor means even ZERO-length values count toward
    /// the budget, so an unbounded stream of distinct empty-value keys cannot
    /// accumulate without bound (the empty-delta failure PR #4794 fixed). Entry
    /// count is capped at `byte_budget / CACHE_ENTRY_OVERHEAD_BYTES`.
    #[test]
    fn empty_values_stay_entry_bounded() {
        // Budget for exactly 32 entries at the overhead floor.
        let byte_budget = 32 * CACHE_ENTRY_OVERHEAD_BYTES;
        let count_cap = NonZeroUsize::new(65_536).unwrap();
        let mut cache: ByteBoundedLruCache<u64, Vec<u8>> =
            ByteBoundedLruCache::new(count_cap, byte_budget, vec_len);

        for i in 0..2000u64 {
            cache.put(i, Vec::new()); // empty value → weigh == 0, floored to overhead
        }
        assert!(
            cache.len() <= 32,
            "empty values must still be evicted at the overhead floor; held {} (expected <= 32)",
            cache.len()
        );
    }

    /// In the normal case (small values, generous budget) the COUNT cap binds —
    /// this is the coverage guarantee at the unit level. With small values that
    /// never approach the byte budget, the cache holds exactly up to its count
    /// cap and evicting-by-count keeps the byte total exact.
    #[test]
    fn count_cap_binds_for_small_values() {
        let byte_budget = 32 * 1024 * 1024; // ample
        let count_cap = NonZeroUsize::new(4).unwrap();
        let mut cache: ByteBoundedLruCache<u64, Vec<u8>> =
            ByteBoundedLruCache::new(count_cap, byte_budget, vec_len);

        for i in 0..10u64 {
            cache.put(i, vec![7u8; 8]);
        }
        assert_eq!(cache.len(), 4, "count cap must bind for small values");
        // Byte total must match the 4 resident entries exactly (each 8 + overhead).
        assert_eq!(
            cache.total_bytes(),
            4 * (8 + CACHE_ENTRY_OVERHEAD_BYTES),
            "byte accounting must stay exact across count-cap evictions"
        );
        // Only the 4 most-recently-inserted keys survive.
        for i in 0..6u64 {
            assert!(cache.get(&i).is_none(), "key {i} should have been evicted");
        }
        for i in 6..10u64 {
            assert!(cache.get(&i).is_some(), "key {i} should be resident");
        }
    }

    /// Replacing an existing key must not double-count its bytes: the running
    /// total reflects the NEW value's size, not old + new.
    #[test]
    fn replacing_a_key_keeps_byte_total_exact() {
        let mut cache: ByteBoundedLruCache<u64, Vec<u8>> =
            ByteBoundedLruCache::new(NonZeroUsize::new(16).unwrap(), 32 * 1024 * 1024, vec_len);
        cache.put(1, vec![0u8; 100]);
        cache.put(1, vec![0u8; 300]);
        assert_eq!(cache.len(), 1);
        assert_eq!(
            cache.total_bytes(),
            300 + CACHE_ENTRY_OVERHEAD_BYTES,
            "replace must account only the new value, not old + new"
        );
    }

    /// Growing the count cap must not disturb byte accounting (it never evicts).
    #[test]
    fn grow_preserves_byte_total() {
        let mut cache: ByteBoundedLruCache<u64, Vec<u8>> =
            ByteBoundedLruCache::new(NonZeroUsize::new(2).unwrap(), 32 * 1024 * 1024, vec_len);
        cache.put(1, vec![0u8; 10]);
        cache.put(2, vec![0u8; 20]);
        let before = cache.total_bytes();
        cache.grow(NonZeroUsize::new(1024).unwrap());
        assert_eq!(cache.cap().get(), 1024);
        assert_eq!(
            cache.total_bytes(),
            before,
            "grow must not change byte total"
        );
        assert_eq!(cache.len(), 2, "grow must not evict");
    }

    /// The `cap <= inner.cap()` no-shrink guard in [`Self::grow`] makes a
    /// repeated grow to the SAME cap a no-op: it returns before
    /// `lru::resize`, so no entry is evicted and byte accounting is
    /// untouched. Pins the guard that stops a non-monotonic (equal-cap)
    /// caller from corrupting `total_bytes` via an un-accounted resize
    /// eviction. An equal cap also satisfies the no-shrink `debug_assert`,
    /// so this exercises the early return without tripping it.
    #[test]
    fn grow_with_equal_cap_is_noop() {
        let count_cap = NonZeroUsize::new(4).unwrap();
        let mut cache: ByteBoundedLruCache<u64, Vec<u8>> =
            ByteBoundedLruCache::new(count_cap, 32 * 1024 * 1024, vec_len);
        cache.put(1, vec![0u8; 10]);
        cache.put(2, vec![0u8; 20]);
        cache.put(3, vec![0u8; 30]);
        let len_before = cache.len();
        let bytes_before = cache.total_bytes();
        let cap_before = cache.cap().get();

        // Grow to the SAME cap the cache already has: the `cap <= inner.cap()`
        // guard returns early (equal cap also satisfies the no-shrink
        // debug_assert), so this is a pure no-op (no resize, hence no
        // un-accounted eviction).
        cache.grow(count_cap);

        assert_eq!(cache.len(), len_before, "equal-cap grow must not evict");
        assert_eq!(
            cache.total_bytes(),
            bytes_before,
            "equal-cap grow must not change the byte total"
        );
        assert_eq!(
            cache.cap().get(),
            cap_before,
            "equal-cap grow must not change the count cap"
        );
    }

    /// Evictions are counted for BOTH bounds, and a same-key replace is not an
    /// eviction. Without the replace distinction, every summary refresh after a
    /// state change would read as cache pressure.
    #[test]
    fn evictions_count_both_bounds_but_not_replacements() {
        // Count cap 2, ample bytes: the third distinct key evicts by COUNT.
        let mut cache: ByteBoundedLruCache<u64, Vec<u8>> =
            ByteBoundedLruCache::new(NonZeroUsize::new(2).unwrap(), 32 * 1024 * 1024, vec_len);
        cache.put(1, vec![0u8; 8]);
        cache.put(1, vec![0u8; 16]); // replace
        assert_eq!(cache.evictions(), 0, "a replace is not an eviction");
        cache.put(2, vec![0u8; 8]);
        cache.put(3, vec![0u8; 8]);
        assert_eq!(cache.evictions(), 1, "count-cap eviction must be counted");

        // Byte budget for exactly two 100-byte entries: the third evicts by BYTES.
        let budget = 2 * (100 + CACHE_ENTRY_OVERHEAD_BYTES);
        let mut cache: ByteBoundedLruCache<u64, Vec<u8>> =
            ByteBoundedLruCache::new(NonZeroUsize::new(1024).unwrap(), budget, vec_len);
        for i in 0..2u64 {
            cache.put(i, vec![0u8; 100]);
        }
        assert_eq!(cache.evictions(), 0, "everything fits: no eviction");
        cache.put(2, vec![0u8; 100]);
        assert_eq!(cache.evictions(), 1, "byte-budget eviction must be counted");
        // An oversized value is refused, never cached, so it evicts nothing.
        cache.put(9, vec![0u8; budget]);
        assert_eq!(
            cache.evictions(),
            1,
            "an oversized refusal is not an eviction"
        );
    }

    /// The gauges equal the sum over the LIVE caches attached to them: two
    /// caches add up, and dropping one withdraws exactly its contribution while
    /// the lifetime eviction total is kept.
    #[test]
    fn gauges_sum_live_caches_and_withdraw_on_drop() {
        let gauges = Arc::new(ByteLruGauges::default());
        let budget = 4 * (10 + CACHE_ENTRY_OVERHEAD_BYTES);
        let mut a: ByteBoundedLruCache<u64, Vec<u8>> =
            ByteBoundedLruCache::new(NonZeroUsize::new(1024).unwrap(), budget, vec_len)
                .with_gauges(gauges.clone());
        let mut b: ByteBoundedLruCache<u64, Vec<u8>> =
            ByteBoundedLruCache::new(NonZeroUsize::new(1024).unwrap(), budget, vec_len)
                .with_gauges(gauges.clone());
        assert_eq!(gauges.snapshot().budget_bytes, 2 * budget as u64);

        for i in 0..6u64 {
            a.put(i, vec![0u8; 10]); // 4 fit, 2 evicted
        }
        b.put(100, vec![0u8; 10]);
        b.put(100, vec![0u8; 10]); // replace: no change in entries/bytes
        let snap = gauges.snapshot();
        assert_eq!(snap.entries, (a.len() + b.len()) as u64);
        assert_eq!(snap.entries, 5);
        assert_eq!(snap.bytes, (a.total_bytes() + b.total_bytes()) as u64);
        assert_eq!(snap.evictions_total, 2);

        drop(a);
        let snap = gauges.snapshot();
        assert_eq!(snap.entries, 1, "dropping a cache withdraws its entries");
        assert_eq!(snap.bytes, b.total_bytes() as u64);
        assert_eq!(snap.budget_bytes, budget as u64);
        assert_eq!(snap.evictions_total, 2, "the eviction total is lifetime");

        drop(b);
        let snap = gauges.snapshot();
        assert_eq!((snap.entries, snap.bytes, snap.budget_bytes), (0, 0, 0));
    }
}
