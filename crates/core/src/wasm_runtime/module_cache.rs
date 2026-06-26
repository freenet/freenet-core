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
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use lru::LruCache;

/// A predicate that reports whether a cached key is currently "of interest" to
/// this node, i.e. something still depends on it being resident.
///
/// For the contract cache this is wired to
/// [`Ring::contract_in_use`](crate::ring::Ring::contract_in_use) (a live local
/// client subscription OR a downstream peer subscriber — deliberately NOT an
/// upstream-only subscription, which is unbounded). It is `None` for caches
/// that have no interest concept (the delegate cache, unlabeled/test caches),
/// in which case eviction is always pure byte-LRU regardless of the feature
/// flag.
///
/// Read inside `evict_to_budget` (under the cache `Mutex`); the predicate reads
/// only the Ring's subscription DashMaps and never the cache, so there is no
/// lock-order hazard (see `HostingManager::contract_in_use` rustdoc).
pub(crate) type InterestPredicate<K> = Arc<dyn Fn(&K) -> bool + Send + Sync>;

/// Environment variable that enables the interest-weighted (two-tier) eviction
/// policy for the contract module cache. Default OFF — with the flag unset (or
/// any value other than a recognized truthy string) eviction is EXACTLY the
/// pre-existing pure byte-LRU, so this whole change is inert until an operator
/// opts in. Mirrors the `FREENET_MODULE_CACHE_BUDGET_BYTES` env pattern.
///
/// When ON, an over-budget eviction first reclaims cold (no-interest) LRU
/// entries; only if the interested set ALONE still exceeds the byte budget does
/// it evict interested LRU entries too. The byte budget itself is never
/// weakened (that would re-open the #4565 OOM risk) — no contract is pinned
/// forever. See [`ModuleCache::evict_to_budget`].
pub(crate) const INTEREST_TIERED_ENV: &str = "FREENET_MODULE_CACHE_INTEREST_TIERED";

/// Parse the [`INTEREST_TIERED_ENV`] flag value into a bool. Recognizes the
/// usual truthy spellings; anything else (including unset) is `false`, so the
/// safe default is pure byte-LRU. Split out as a pure function so the parsing is
/// unit-testable without touching the process environment.
fn parse_interest_tiered_flag(value: Option<&str>) -> bool {
    matches!(
        value.map(|v| v.trim().to_ascii_lowercase()).as_deref(),
        Some("1" | "true" | "yes" | "on")
    )
}

/// Read [`INTEREST_TIERED_ENV`] from the process environment (default OFF).
pub(crate) fn interest_tiered_enabled() -> bool {
    parse_interest_tiered_flag(std::env::var(INTEREST_TIERED_ENV).ok().as_deref())
}

/// Minimum gap between "cache is evicting at budget" operator warnings, so a
/// memory-bound node logs the signal periodically instead of once per eviction.
const EVICTION_WARN_INTERVAL: Duration = Duration::from_secs(300);

/// Number of evictions within one [`EVICTION_WARN_INTERVAL`] window before the
/// cache emits a warning. A handful of evictions is normal churn; sustained
/// eviction means the working set genuinely exceeds the budget.
const EVICTION_WARN_THRESHOLD: u64 = 16;

/// How often a cache HIT (`get`) is allowed to recompute the interest shadow
/// gauges (cold-evictable vs interested bytes).
///
/// The `interest` predicate can flip for an already-cached contract WITHOUT any
/// cache mutation — e.g. a client disconnects or a downstream lease expires —
/// so recomputing the split only on insert/remove would leave the gauges (and
/// the `migration_admission_would_change` floor) stale on a read-mostly cache
/// (Codex review). Refreshing on `get` fixes that, but a full O(entries) scan on
/// every hot lookup is too expensive; the gauges are consumed only at the 5-min
/// snapshot cadence and at admission-gate time, so bounding the recompute to at
/// most once per this interval keeps `get` cheap while bounding staleness to the
/// interval on any node with ongoing contract traffic. Uses real wall-clock time
/// (same rationale as the eviction warn window): it rate-limits a telemetry
/// recompute, not node behavior, so it must advance in real time.
const INTEREST_SHADOW_REFRESH_INTERVAL: Duration = Duration::from_secs(10);

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
    /// Shared occupancy/eviction telemetry sink this cache publishes into, or
    /// `None` for unlabeled / test caches that don't report metrics. The `Arc`
    /// is constructed once at node startup (`Ring::new`) and threaded through
    /// `RuntimePool::new`; the `Ring` snapshot task reads from the same `Arc`.
    /// See [`ModuleCacheMetrics`].
    metrics: Option<Arc<ModuleCacheMetrics>>,
    /// Optional "is this key still of interest?" predicate (the contract cache
    /// wires it to `Ring::contract_in_use`; `None` for the delegate / test
    /// caches). Drives the interest-tier of eviction AND the always-on shadow
    /// metrics (cold-evictable vs interested bytes, would-reclassify count).
    /// See [`InterestPredicate`].
    interest: Option<InterestPredicate<K>>,
    /// Whether the interest-weighted (two-tier) eviction policy is ACTIVE.
    /// `true` only when both the [`INTEREST_TIERED_ENV`] flag is set AND an
    /// `interest` predicate is present. With it `false`, eviction is exactly the
    /// pre-existing pure byte-LRU — the shadow metrics are still computed (they
    /// only need the predicate), but they do not change eviction behavior. This
    /// is what makes the change inert by default and safe to merge.
    interest_tiered_active: bool,
    /// Last time a cache HIT recomputed the interest shadow gauges, throttled to
    /// [`INTEREST_SHADOW_REFRESH_INTERVAL`]. `None` until the first throttled
    /// refresh. Insert/remove always recompute (the entry set just changed);
    /// this only governs the `get`-path refresh that catches interest flips with
    /// no cache mutation. Real wall-clock `Instant` deliberately — see the
    /// constant's docs and `window_started_at`.
    last_interest_shadow_refresh: Option<Instant>,
}

// `K: Clone` is required so the two-tier eviction can name (clone) the LRU cold
// key it selects before `pop`-ing it; the real keys (`ContractKey`,
// `DelegateKey`) and every test key (`u64`) are `Clone`, so this is not a new
// restriction in practice.
impl<K: Hash + Eq + Clone, V> ModuleCache<K, V> {
    /// Create an empty cache with the given byte budget, an unlabeled
    /// ("module") operator-warning tag, and no metrics sink. Prefer
    /// [`Self::with_label`] in production so the eviction warning identifies
    /// the cache and so occupancy/eviction telemetry is published.
    ///
    /// `budget_bytes` is clamped to at least 1 so a degenerate budget of 0
    /// still permits exactly one (most-recently-used) entry to be resident,
    /// keeping the cache functional rather than evicting on every insert.
    pub(crate) fn new(budget_bytes: usize) -> Self {
        Self::with_label(budget_bytes, "module", None)
    }

    /// Like [`Self::new`] but with a label ("contract" / "delegate") used in the
    /// rate-limited "cache is evicting at budget" operator warning, and an
    /// optional shared [`ModuleCacheMetrics`] sink. Production passes
    /// `Some(metrics)` (the per-node `Arc` threaded from `Ring::new` through
    /// `RuntimePool::new`); tests that don't care about telemetry pass `None`.
    pub(crate) fn with_label(
        budget_bytes: usize,
        label: &'static str,
        metrics: Option<Arc<ModuleCacheMetrics>>,
    ) -> Self {
        Self::with_label_and_interest(budget_bytes, label, metrics, None)
    }

    /// Like [`Self::with_label`] but also wires an optional interest predicate
    /// (and reads the [`INTEREST_TIERED_ENV`] flag) so the contract cache can
    /// participate in interest-weighted (two-tier) eviction.
    ///
    /// The two-tier policy is ACTIVE only when the flag is set AND `interest` is
    /// `Some`. When `interest` is `Some` but the flag is unset, eviction stays
    /// pure byte-LRU (today's behavior) while the always-on shadow metrics are
    /// still computed from the predicate. When `interest` is `None`, this is
    /// identical to [`Self::with_label`].
    pub(crate) fn with_label_and_interest(
        budget_bytes: usize,
        label: &'static str,
        metrics: Option<Arc<ModuleCacheMetrics>>,
        interest: Option<InterestPredicate<K>>,
    ) -> Self {
        let budget_bytes = budget_bytes.max(1);
        let interest_tiered_active = interest.is_some() && interest_tiered_enabled();
        let cache = Self {
            // The LRU is unbounded by count; the byte budget is the only
            // bound. `usize::MAX` capacity means `LruCache` never evicts on
            // its own — we drive all eviction via the byte budget below.
            inner: LruCache::unbounded(),
            total_bytes: 0,
            budget_bytes,
            label,
            evictions_in_window: 0,
            window_started_at: None,
            metrics,
            interest,
            interest_tiered_active,
            last_interest_shadow_refresh: None,
        };
        // Publish the budget immediately, so an idle cache (a node that hasn't
        // compiled a module yet, or a delegate cache that's never used) reports
        // its configured budget instead of the atomic default 0. Otherwise the
        // collector's occupancy-% denominator is 0 until the first insert/remove
        // (#4440 — flagged by both PR reviewers).
        cache.record_occupancy(0, 0, budget_bytes);
        cache
    }

    /// Test-only constructor that wires an interest predicate AND forces the
    /// two-tier policy on/off explicitly, bypassing the [`INTEREST_TIERED_ENV`]
    /// read. This keeps the eviction-ordering tests deterministic and free of
    /// process-environment races (the env is process-global and shared across
    /// parallel tests). Production always goes through
    /// [`Self::with_label_and_interest`], which reads the env flag.
    #[cfg(test)]
    pub(crate) fn with_interest_for_test(
        budget_bytes: usize,
        label: &'static str,
        metrics: Option<Arc<ModuleCacheMetrics>>,
        interest: InterestPredicate<K>,
        tiered_active: bool,
    ) -> Self {
        let mut cache = Self::with_label_and_interest(budget_bytes, label, metrics, Some(interest));
        cache.interest_tiered_active = tiered_active;
        cache
    }

    /// Look up a key, marking it most-recently-used on a hit. Returns a
    /// reference to the cached value (not the size).
    ///
    /// Also opportunistically (throttled) refreshes the interest shadow gauges,
    /// so they track interest flips that happen with NO cache mutation (a client
    /// disconnect / lease expiry between inserts). See
    /// [`INTEREST_SHADOW_REFRESH_INTERVAL`].
    pub(crate) fn get<Q>(&mut self, key: &Q) -> Option<&V>
    where
        K: Borrow<Q>,
        Q: Hash + Eq + ?Sized,
    {
        self.maybe_refresh_interest_shadow();
        self.inner.get(key).map(|(v, _)| v)
    }

    /// Recompute the interest shadow gauges if at least
    /// [`INTEREST_SHADOW_REFRESH_INTERVAL`] has elapsed since the last
    /// `get`-driven refresh. No-op (no scan) for caches without a predicate or
    /// metrics sink. Bounds the cost of refreshing on the hot lookup path.
    fn maybe_refresh_interest_shadow(&mut self) {
        if self.interest.is_none() || self.metrics.is_none() {
            return;
        }
        let now = Instant::now();
        let due = match self.last_interest_shadow_refresh {
            Some(prev) => now.duration_since(prev) >= INTEREST_SHADOW_REFRESH_INTERVAL,
            None => true,
        };
        if due {
            self.last_interest_shadow_refresh = Some(now);
            self.record_interest_shadow();
        }
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
        self.record_occupancy(self.inner.len(), self.total_bytes, self.budget_bytes);
        self.record_interest_shadow();
    }

    /// Publish current occupancy into the metrics sink (no-op when this cache
    /// has no sink — unlabeled / test caches). Routed by this cache's `label`.
    fn record_occupancy(&self, entries: usize, total_bytes: usize, budget_bytes: usize) {
        if let Some(metrics) = &self.metrics {
            metrics.record_occupancy(self.label, entries, total_bytes, budget_bytes);
        }
    }

    /// Add to this cache's monotonic lifetime eviction counter in the metrics
    /// sink (no-op when this cache has no sink). Routed by this cache's `label`.
    fn add_evictions(&self, count: u64) {
        if let Some(metrics) = &self.metrics {
            metrics.add_evictions(self.label, count);
        }
    }

    /// Whether the cached `key` is currently of interest (in use), per the
    /// wired predicate. With no predicate (delegate / test caches) NOTHING is
    /// classified as interested, so two-tier eviction collapses to pure LRU.
    fn is_interested(&self, key: &K) -> bool {
        self.interest
            .as_ref()
            .map(|pred| pred(key))
            .unwrap_or(false)
    }

    /// Pick the least-recently-used key matching `want_interested` (clone it so
    /// we can `pop` it afterwards). `iter()` yields MRU→LRU, so `.rev()` walks
    /// LRU→MRU and the first match is the LRU among that tier. Returns `None`
    /// when no resident entry is in the requested tier.
    fn lru_key_in_tier(&self, want_interested: bool) -> Option<K> {
        self.inner
            .iter()
            .rev()
            .map(|(k, _)| k)
            .find(|k| self.is_interested(k) == want_interested)
            .cloned()
    }

    /// Whether any resident entry is cold (no interest). Early-terminating, so
    /// it is O(1) when the first entry checked is cold (the common case) — used
    /// by the flag-OFF shadow path to decide divergence without the full ordered
    /// `lru_key_in_tier` scan.
    fn has_cold_entry(&self) -> bool {
        self.inner.iter().any(|(k, _)| !self.is_interested(k))
    }

    /// Evict entries until `total_bytes <= budget_bytes`, keeping at least the
    /// single most-recently-used entry resident even if it alone exceeds the
    /// budget (so an oversized contract can still run).
    ///
    /// # Two policies, one strict byte budget
    ///
    /// - **Pure byte-LRU (default / flag OFF):** evict the absolute LRU entry
    ///   each step. Exactly the historical behavior.
    /// - **Interest-weighted two-tier (flag ON + predicate present):** evict the
    ///   LRU *cold* (no-interest) entry each step; only once NO cold entry
    ///   remains (the interested set alone still exceeds the budget) does it fall
    ///   back to evicting the LRU interested entry. No contract is pinned
    ///   forever, and the byte budget is never weakened — both policies loop
    ///   until `total_bytes <= budget_bytes`.
    ///
    /// Regardless of which policy is active, the always-on shadow counter
    /// `evictions_would_reclassify_total` is incremented for every step where the
    /// two-tier victim would differ from the plain-LRU victim, so the production
    /// data needed to justify flipping the flag is collected even with it OFF.
    ///
    /// # Cost of the default (flag-OFF) path
    ///
    /// The default path must NOT regress the historical O(evictions) pure-LRU
    /// cost (this runs under the shared module-cache mutex). It doesn't: the
    /// shadow `would_reclassify` check is O(1) when the absolute LRU is cold (the
    /// common, healthy case — two-tier would evict the same entry, no scan), and
    /// only walks the cache to confirm a colder cold entry exists when the LRU is
    /// *interested* AND the flag is OFF. The full LRU-ordered cold-key scan
    /// (`lru_key_in_tier`) runs ONLY when the flag is ON, where it is required to
    /// pick the victim. (Codex review: don't scan on every default eviction.)
    fn evict_to_budget(&mut self) {
        let mut evicted_this_insert: u64 = 0;
        let mut would_reclassify: u64 = 0;
        // With no interest predicate (delegate / unlabeled caches) plain LRU and
        // the two-tier policy are identical, so skip ALL tier work — these caches
        // keep EXACTLY today's pure-byte-LRU cost.
        let has_interest = self.interest.is_some();
        while self.total_bytes > self.budget_bytes && self.inner.len() > 1 {
            // The plain-LRU victim is always the absolute LRU.
            let plain_victim = self.inner.peek_lru().map(|(k, _)| k.clone());
            let victim = if has_interest {
                if self.interest_tiered_active {
                    // Flag ON: pick the LRU cold entry (fall back to plain LRU
                    // when none) — the only path that needs the ordered scan.
                    let two_tier_victim =
                        self.lru_key_in_tier(false).or_else(|| plain_victim.clone());
                    if plain_victim != two_tier_victim {
                        would_reclassify += 1;
                    }
                    two_tier_victim
                } else {
                    // Flag OFF: evict plain LRU. For the shadow counter, the
                    // two-tier policy would only DIVERGE when the plain victim is
                    // interested AND some cold entry exists to evict instead — so
                    // do the O(1) interest check first and only confirm a cold
                    // entry exists (early-terminating) in that rarer case. This
                    // keeps the healthy "LRU is cold" case at O(1) per step.
                    if plain_victim.as_ref().is_some_and(|k| self.is_interested(k))
                        && self.has_cold_entry()
                    {
                        would_reclassify += 1;
                    }
                    plain_victim
                }
            } else {
                plain_victim
            };

            let evicted_size = match victim {
                Some(key) => self.inner.pop(&key).map(|(_, size)| size),
                None => self.inner.pop_lru().map(|(_, (_, size))| size),
            };
            match evicted_size {
                Some(size) => {
                    self.total_bytes = self.total_bytes.saturating_sub(size);
                    evicted_this_insert += 1;
                }
                None => break,
            }
        }
        if would_reclassify > 0 {
            self.add_would_reclassify(would_reclassify);
        }
        if evicted_this_insert > 0 {
            self.note_evictions(evicted_this_insert);
            // Monotonic lifetime counter for telemetry; the collector differences
            // it across the snapshot cadence to derive an eviction rate. Distinct
            // from `evictions_in_window`, which resets per warn window (#4440).
            self.add_evictions(evicted_this_insert);
        }
    }

    /// Recompute and publish the always-on interest shadow gauges: the total
    /// bytes of resident COLD (no-interest, hence freely evictable) entries and
    /// of resident INTERESTED entries. These are independent of the feature flag
    /// (they only need the predicate) and are the per-node signal for whether
    /// flipping the flag would actually relieve pressure. No-op when this cache
    /// has no metrics sink, no predicate, or an unlabeled tag.
    fn record_interest_shadow(&self) {
        let Some(metrics) = &self.metrics else {
            return;
        };
        if self.interest.is_none() {
            return;
        }
        let mut cold_evictable_bytes: u64 = 0;
        let mut interested_bytes: u64 = 0;
        for (k, (_, size)) in self.inner.iter() {
            if self.is_interested(k) {
                interested_bytes = interested_bytes.saturating_add(*size as u64);
            } else {
                cold_evictable_bytes = cold_evictable_bytes.saturating_add(*size as u64);
            }
        }
        metrics.record_interest_shadow(self.label, cold_evictable_bytes, interested_bytes);
    }

    /// Add to this cache's monotonic would-reclassify counter (no-op without a
    /// sink). Routed by this cache's `label`.
    fn add_would_reclassify(&self, count: u64) {
        if let Some(metrics) = &self.metrics {
            metrics.add_would_reclassify(self.label, count);
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
        self.record_occupancy(self.inner.len(), self.total_bytes, self.budget_bytes);
        self.record_interest_shadow();
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

    /// Whether the interest-weighted two-tier policy is active on this cache
    /// (test-only observability).
    #[cfg(test)]
    pub(crate) fn interest_tiered_active(&self) -> bool {
        self.interest_tiered_active
    }
}

/// Atomic occupancy gauges plus a monotonic eviction counter for one cache.
#[derive(Default)]
struct CacheGauges {
    /// Resident entry count (last-write-wins gauge).
    entries: AtomicU64,
    /// Total compiled bytes resident (last-write-wins gauge).
    total_bytes: AtomicU64,
    /// Configured byte budget (effectively constant per run; reported so the
    /// collector can compute occupancy % without hardcoding the budget).
    budget_bytes: AtomicU64,
    /// Lifetime evictions (monotonic; the collector differences it across the
    /// snapshot cadence to derive an eviction rate).
    evictions_total: AtomicU64,
    /// SHADOW (always-on, flag-independent): bytes of resident COLD (no-interest)
    /// entries — the bytes the two-tier policy could freely reclaim. Last-write-
    /// wins gauge, recomputed on every insert/remove. `0` for caches with no
    /// interest predicate (delegate cache stays zero).
    cold_evictable_bytes: AtomicU64,
    /// SHADOW (always-on): bytes of resident INTERESTED (in-use) entries — the
    /// floor the two-tier policy would protect first. Last-write-wins gauge.
    interested_bytes: AtomicU64,
    /// SHADOW (always-on): lifetime count of eviction steps whose victim the
    /// two-tier policy WOULD pick differently from plain LRU (an interested
    /// absolute-LRU entry spared in favor of a cold one). Monotonic; collected
    /// even with the flag OFF so the production data to justify flipping it
    /// exists. The collector differences it across the cadence.
    evictions_would_reclassify_total: AtomicU64,
}

/// Per-node compiled-WASM module-cache occupancy + eviction telemetry (#4440),
/// routed by the cache's `"contract"` / `"delegate"` label.
///
/// The module caches live behind the contract-handler channel, unreachable from
/// the `Ring` telemetry-snapshot task that emits `router_snapshot`. The caches
/// therefore *publish* into this shared sink and the snapshot task *reads* it.
/// A node builds exactly one contract cache and one delegate cache
/// (`RuntimePool::new`), so one shared pair of gauges is correct.
///
/// Unlike the [`TRANSPORT_METRICS`](crate::transport::metrics::TRANSPORT_METRICS)
/// process-global it used to mirror, this is constructed once at node startup
/// (`Ring::new`) and threaded as an `Arc`: `Ring` keeps a clone for the snapshot
/// reader and `RuntimePool::new` clones it into each labeled `ModuleCache`. That
/// keeps the gauges per-node (so unit tests get isolated instances with no
/// cross-talk) while preserving the single-runtime-pool-per-process production
/// behavior. The cache-thrash that drove the #4441 incident was invisible to
/// central telemetry — these gauges make occupancy and eviction pressure
/// observable.
pub(crate) struct ModuleCacheMetrics {
    contract: CacheGauges,
    delegate: CacheGauges,
    /// SHADOW (always-on, flag-independent): lifetime count of placement-
    /// migration admission decisions where the current ≥90%-occupancy refuse
    /// gate (#4534) and a hypothetical "admit if there are enough cold-evictable
    /// bytes to make room" gate WOULD DISAGREE — i.e. the current gate refuses
    /// but the cold tier could be reclaimed to fit the migration. Published by
    /// the admission gate at `node.rs` (which has no per-cache handle), so it
    /// lives at the node level rather than under a cache label. Monotonic; the
    /// collector differences it across the cadence. The gate's BEHAVIOR is
    /// unchanged in this PR — this only measures the would-be delta (#4534 is a
    /// later, data-gated change).
    migration_admission_would_change_total: AtomicU64,
}

/// A point-in-time read of [`ModuleCacheMetrics`] for telemetry emission.
#[derive(Debug, Clone, Copy)]
pub(crate) struct ModuleCacheMetricsSnapshot {
    pub contract_entries: u64,
    pub contract_total_bytes: u64,
    pub contract_budget_bytes: u64,
    pub contract_evictions_total: u64,
    pub delegate_entries: u64,
    pub delegate_total_bytes: u64,
    pub delegate_budget_bytes: u64,
    pub delegate_evictions_total: u64,
    /// SHADOW (always-on): cold-evictable / interested byte split + the
    /// would-reclassify eviction count for the contract cache (the only cache
    /// with an interest predicate; delegate fields stay zero). See
    /// [`CacheGauges`].
    pub contract_cold_evictable_bytes: u64,
    pub contract_interested_bytes: u64,
    pub contract_evictions_would_reclassify_total: u64,
    /// SHADOW (always-on): node-level would-change count for the migration
    /// admission gate (#4534). See
    /// [`ModuleCacheMetrics::migration_admission_would_change_total`].
    pub migration_admission_would_change_total: u64,
}

impl ModuleCacheMetrics {
    /// Construct an empty metrics sink. Called once per node at startup
    /// (`Ring::new`) and shared via `Arc`; see the type-level docs.
    pub(crate) fn new() -> Self {
        Self {
            contract: CacheGauges::default(),
            delegate: CacheGauges::default(),
            migration_admission_would_change_total: AtomicU64::new(0),
        }
    }

    /// Route to the gauges for a labeled cache. Unlabeled / test caches
    /// (`ModuleCache::new`, label `"module"`) don't publish.
    fn gauges_for(&self, label: &str) -> Option<&CacheGauges> {
        match label {
            "contract" => Some(&self.contract),
            "delegate" => Some(&self.delegate),
            _ => None,
        }
    }

    /// Publish current occupancy for the named cache (last-write-wins). Cheap
    /// `Relaxed` atomics; the caller already holds the cache `Mutex`.
    fn record_occupancy(
        &self,
        label: &str,
        entries: usize,
        total_bytes: usize,
        budget_bytes: usize,
    ) {
        if let Some(g) = self.gauges_for(label) {
            g.entries.store(entries as u64, Ordering::Relaxed);
            g.total_bytes.store(total_bytes as u64, Ordering::Relaxed);
            g.budget_bytes.store(budget_bytes as u64, Ordering::Relaxed);
        }
    }

    /// Add to the named cache's monotonic lifetime eviction counter.
    fn add_evictions(&self, label: &str, count: u64) {
        if let Some(g) = self.gauges_for(label) {
            g.evictions_total.fetch_add(count, Ordering::Relaxed);
        }
    }

    /// Publish the always-on interest shadow gauges (cold-evictable / interested
    /// bytes) for the named cache (last-write-wins). No-op for unlabeled caches.
    fn record_interest_shadow(
        &self,
        label: &str,
        cold_evictable_bytes: u64,
        interested_bytes: u64,
    ) {
        if let Some(g) = self.gauges_for(label) {
            g.cold_evictable_bytes
                .store(cold_evictable_bytes, Ordering::Relaxed);
            g.interested_bytes
                .store(interested_bytes, Ordering::Relaxed);
        }
    }

    /// Add to the named cache's monotonic would-reclassify counter.
    fn add_would_reclassify(&self, label: &str, count: u64) {
        if let Some(g) = self.gauges_for(label) {
            g.evictions_would_reclassify_total
                .fetch_add(count, Ordering::Relaxed);
        }
    }

    /// Record one placement-migration admission decision where the current gate
    /// and the cold-evictable-aware gate WOULD disagree (#4534 shadow). Called
    /// from the admission gate; does NOT change the gate's behavior. Monotonic.
    pub(crate) fn record_migration_admission_would_change(&self) {
        self.migration_admission_would_change_total
            .fetch_add(1, Ordering::Relaxed);
    }

    /// Read all gauges for telemetry.
    pub(crate) fn snapshot(&self) -> ModuleCacheMetricsSnapshot {
        let load = |g: &CacheGauges| {
            (
                g.entries.load(Ordering::Relaxed),
                g.total_bytes.load(Ordering::Relaxed),
                g.budget_bytes.load(Ordering::Relaxed),
                g.evictions_total.load(Ordering::Relaxed),
            )
        };
        let (ce, ctb, cbb, cev) = load(&self.contract);
        let (de, dtb, dbb, dev) = load(&self.delegate);
        ModuleCacheMetricsSnapshot {
            contract_entries: ce,
            contract_total_bytes: ctb,
            contract_budget_bytes: cbb,
            contract_evictions_total: cev,
            delegate_entries: de,
            delegate_total_bytes: dtb,
            delegate_budget_bytes: dbb,
            delegate_evictions_total: dev,
            contract_cold_evictable_bytes: self
                .contract
                .cold_evictable_bytes
                .load(Ordering::Relaxed),
            contract_interested_bytes: self.contract.interested_bytes.load(Ordering::Relaxed),
            contract_evictions_would_reclassify_total: self
                .contract
                .evictions_would_reclassify_total
                .load(Ordering::Relaxed),
            migration_admission_would_change_total: self
                .migration_admission_would_change_total
                .load(Ordering::Relaxed),
        }
    }
}

/// Contract-module-cache occupancy as a percentage of the configured byte
/// budget, read lock-free from the per-node [`ModuleCacheMetrics`] sink (reach it
/// off `op_manager.ring.module_cache_metrics()`; a process-global until #4488).
///
/// Returns `None` when the budget gauge has not been published yet — i.e. no
/// runtime pool has been built on this node (early startup, or a test / tool
/// binary with no WASM runtime). Callers treat `None` as "no pressure signal".
///
/// Used by the placement-migration admission gate (#4534): once occupancy is at
/// or above the admission ceiling, the node stops accepting inbound
/// `SubscribeHint` migrations so the hosted working set cannot grow past cache
/// capacity and thrash (recompile-on-access), which had filled the contract fair
/// queue and produced client-facing "contract queue full" outages and gateway
/// OOMs on 0.2.80.
///
/// This is a deliberate PROXY for hosting pressure, not a direct measure of the
/// hosted set: it reflects *compiled-module-cache bytes*, which a module only
/// joins when it is first executed (summarize / GET / UPDATE), so it LAGS the
/// hosted set after a directed subscribe makes the node a holder. The lag is
/// acceptable here because (a) the hosted set is independently bounded by the
/// hosting cache's own byte budget and the node's ring-responsibility region, so
/// a burst cannot grow it without bound, and (b) the gate only ever *refuses*
/// new load — the failure mode of a stale read is at worst deferring one
/// migration that would have fit, never accepting one it should have refused
/// once occupancy has caught up.
pub(crate) fn contract_cache_occupancy_pct(metrics: &ModuleCacheMetrics) -> Option<u64> {
    let snapshot = metrics.snapshot();
    occupancy_pct(
        snapshot.contract_total_bytes,
        snapshot.contract_budget_bytes,
    )
}

/// Pure occupancy-percent computation, split out so the threshold arithmetic is
/// unit-testable without touching the process-global gauges. `None` when
/// `budget_bytes == 0` (gauge uninitialized): with no known budget there is no
/// meaningful occupancy, so callers must not infer pressure from it.
fn occupancy_pct(total_bytes: u64, budget_bytes: u64) -> Option<u64> {
    if budget_bytes == 0 {
        return None;
    }
    // `saturating_mul` guards the (practically impossible) overflow of
    // `total_bytes * 100`; at realistic cache sizes (a few GiB) it never trips.
    Some(total_bytes.saturating_mul(100) / budget_bytes)
}

/// SHADOW (always-on, behavior-neutral): would the current ≥`ceiling_pct`-
/// occupancy migration-admission refuse (#4534) flip to ADMIT under a
/// hypothetical "admit if there's enough cold-evictable cache to make room"
/// gate? Returns `true` only when the two gates DISAGREE for this snapshot:
/// the current gate refuses (occupancy at/above the ceiling) **and** the
/// cold-aware gate would admit because dropping all cold-evictable bytes would
/// pull occupancy below the ceiling (i.e. the protected interested floor alone
/// fits under it).
///
/// Pure (snapshot in, bool out) so it is unit-testable and so the live gate at
/// `node.rs` can call it without changing its own behavior — it only feeds the
/// `migration_admission_would_change_total` shadow counter. `None` budget means
/// no pressure signal, so the gates cannot disagree (current gate already
/// admits): returns `false`.
fn migration_admission_would_change(
    total_bytes: u64,
    interested_bytes: u64,
    budget_bytes: u64,
    ceiling_pct: u64,
) -> bool {
    let Some(occupancy) = occupancy_pct(total_bytes, budget_bytes) else {
        return false;
    };
    let current_refuses = occupancy >= ceiling_pct;
    // The cold-aware gate keeps only the interested floor; everything else is
    // cold-evictable. It would admit when that floor's occupancy is below the
    // ceiling — i.e. there is enough cold cache to reclaim to make room.
    let cold_aware_admits = match occupancy_pct(interested_bytes, budget_bytes) {
        Some(floor_pct) => floor_pct < ceiling_pct,
        None => false,
    };
    current_refuses && cold_aware_admits
}

/// Snapshot-driven wrapper over [`migration_admission_would_change`] for the
/// live admission gate. Reads the contract cache's total / interested bytes and
/// budget from `metrics` and reports whether the current ≥`ceiling_pct` refuse
/// would flip to admit under the cold-evictable-aware policy. Behavior-neutral —
/// callers use this ONLY to bump the shadow counter, never to gate.
pub(crate) fn migration_admission_would_change_now(
    metrics: &ModuleCacheMetrics,
    ceiling_pct: u64,
) -> bool {
    let s = metrics.snapshot();
    migration_admission_would_change(
        s.contract_total_bytes,
        s.contract_interested_bytes,
        s.contract_budget_bytes,
        ceiling_pct,
    )
}

/// Lower clamp for the default contract-module cache budget (64 MiB).
///
/// Even a tiny VPS should be able to hold a healthy working set of compiled
/// modules; at the measured ~0.7-1.5 MiB per module this still caches tens of
/// contracts. Below this the cache would thrash on a normal node.
pub const MIN_DEFAULT_MODULE_CACHE_BUDGET_BYTES: usize = 64 * 1024 * 1024;

/// Upper clamp for the default contract-module cache budget (1.5 GiB).
///
/// Two competing pressures set this ceiling:
///
/// - **Too low → large gateways thrash.** A production gateway hosts hundreds
///   of contracts and, post-#4404 placement migration, also carries a sizeable
///   *interest* set on contracts it doesn't hold (the #4441 incident saw ~85
///   such phantom-interest contracts on technic). At the measured ~1.5 MiB per
///   compiled module, the previous 384 MiB ceiling held only ~256 modules — so
///   the cache permanently evicts-and-recompiles a working set it can't fit,
///   pinning the single-threaded contract loop. nova (125 GiB RAM) is the
///   canonical case: `total_ram / 8` is ~15 GiB, so the *only* thing capping
///   its default cache was this clamp, and 384 MiB was well below its working
///   set. 1.5 GiB holds ~1000 modules at the measured size — comfortably above
///   a healthy gateway's hot set — and matches the 1 GiB default hosted-*state*
///   budget (`ring::DEFAULT_HOSTING_BUDGET_BYTES`): a node allowed ~1 GiB of
///   contract state should be able to cache the corresponding compiled code.
/// - **Too high → wasted memory on huge hosts.** Past ~1000 resident modules
///   the working-set benefit flattens while absolute memory cost keeps rising.
///   Without a ceiling, `total_ram / 8` would default a 125 GiB box to a
///   ~15 GiB compiled-module cache it can't benefit from. The clamp keeps the
///   default a bounded, defensible commitment.
///
/// This clamp only binds on hosts with more than
/// `MAX * DEFAULT_MODULE_CACHE_RAM_DIVISOR` (= 12 GiB) of RAM; below that the
/// `total_ram / 8` divisor binds first, so raising this ceiling does **not**
/// change the default on small/medium hosts (see
/// `default_module_cache_budget_bytes`). Operators who truly need more raise it
/// explicitly via the config override below (the explicit override is
/// unclamped).
pub const MAX_DEFAULT_MODULE_CACHE_BUDGET_BYTES: usize = 1536 * 1024 * 1024;

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
/// `contract_budget * (1 + 1/4) = 1.25 × contract_budget`. The key small-box
/// safety property is that BOTH caches scale from the same RAM-derived contract
/// budget, so the combined default is `1.25 × clamp(total_ram / 8, …)` — a 1 GiB
/// VPS gets `1.25 × 128 MiB = 160 MiB` combined, nowhere near OOM. (The original
/// pre-#4452 code applied the full budget to BOTH caches; this divisor is what
/// keeps the combined default safe regardless of where the MAX clamp sits.)
pub const DELEGATE_MODULE_CACHE_BUDGET_DIVISOR: usize = 4;

/// Fallback total-RAM estimate (1 GiB) when the OS query fails.
///
/// Conservative: at 1 GiB the divisor yields 128 MiB, between the MIN and MAX
/// clamps, so an unknown-RAM host gets a sane mid-range budget rather than the
/// max.
const FALLBACK_TOTAL_RAM_BYTES: usize = 1024 * 1024 * 1024;

/// Default per-cache byte budget for the **contract** compiled-module cache,
/// scaled to the memory the node may use (host RAM, or a smaller cgroup limit
/// when containerized — see [`read_total_ram_bytes`]) and clamped to a sane
/// floor/ceiling.
///
/// Returns `clamp(total_ram / DEFAULT_MODULE_CACHE_RAM_DIVISOR,
/// MIN_DEFAULT_MODULE_CACHE_BUDGET_BYTES, MAX_DEFAULT_MODULE_CACHE_BUDGET_BYTES)`
/// (currently `clamp(total_ram / 8, 64 MiB, 1.5 GiB)`). The same fix for issue
/// #4441 (a node hosting >1024 contracts thrashed/OOM'd the old count cap) must
/// not itself OOM a small box: the `total_ram / 8` divisor — not the absolute
/// MAX clamp — is the small-box protection, and giving the delegate cache only a
/// fraction (`DELEGATE_MODULE_CACHE_BUDGET_DIVISOR`) keeps the COMBINED default
/// ceiling safe on small hosts. The MAX clamp only binds on large hosts (>12 GiB
/// RAM); it exists to stop the divisor from handing a huge box a cache far
/// larger than any useful working set, not to protect small boxes. See
/// `MAX_DEFAULT_MODULE_CACHE_BUDGET_BYTES` for why 1.5 GiB.
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
    budget_for_ram(total_ram)
}

/// Pure clamp math behind [`default_module_cache_budget_bytes`], split out so the
/// small-box / large-box boundary behavior is unit-testable without depending on
/// the test host's real RAM. Returns the contract-cache byte budget for a host
/// with `total_ram` bytes of physical RAM.
fn budget_for_ram(total_ram: usize) -> usize {
    (total_ram / DEFAULT_MODULE_CACHE_RAM_DIVISOR).clamp(
        MIN_DEFAULT_MODULE_CACHE_BUDGET_BYTES,
        MAX_DEFAULT_MODULE_CACHE_BUDGET_BYTES,
    )
}

/// Best-effort read of the memory the node may actually use, in bytes.
///
/// This is the **minimum** of the host's physical RAM and any cgroup memory
/// limit applied to the process. Using the cgroup limit matters because raising
/// the cache clamp to 1.5 GiB would otherwise let a node inside a small
/// container (say a 2 GiB limit on a 128 GiB host) default to a ~1.5 GiB cache
/// sized from the *host* total and OOM the container. Taking the min means a
/// constrained container gets a budget scaled to its real limit, while a host
/// systemd service (the production gateways) sees no cgroup limit and uses
/// `MemTotal` as before.
///
/// Sources, in order:
/// - On Linux, physical RAM from `MemTotal` in `/proc/meminfo` (reported in
///   KiB), then min'd with the cgroup memory limit (cgroup v2 `memory.max`, or
///   v1 `memory.limit_in_bytes`) when one is present and is a real limit (not
///   the "unlimited" sentinel).
/// - On other unix, `sysconf(_SC_PHYS_PAGES) * sysconf(_SC_PAGESIZE)` (no cgroup
///   notion).
/// - Otherwise return `None` so the caller uses `FALLBACK_TOTAL_RAM_BYTES`.
///
/// Never panics; any failure (missing file, parse error, non-positive sysconf)
/// yields `None` for that source and falls back.
fn read_total_ram_bytes() -> Option<usize> {
    #[cfg(target_os = "linux")]
    {
        let phys = read_proc_meminfo_total_bytes();
        // Clamp to the cgroup limit when one applies. If we can read physical
        // RAM, take the min; if not, fall back to the cgroup limit alone (a
        // container with no readable /proc/meminfo still gets a sane bound).
        match (phys, read_cgroup_memory_limit_bytes()) {
            (Some(p), Some(c)) => Some(p.min(c)),
            (Some(p), None) => Some(p),
            (None, Some(c)) => Some(c),
            (None, None) => None,
        }
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

/// Parse physical RAM (bytes) from `/proc/meminfo`'s `MemTotal:` line.
#[cfg(target_os = "linux")]
fn read_proc_meminfo_total_bytes() -> Option<usize> {
    // /proc/meminfo line: "MemTotal:       16331752 kB"
    let meminfo = std::fs::read_to_string("/proc/meminfo").ok()?;
    parse_meminfo_total_bytes(&meminfo)
}

/// Pure parse of `MemTotal` (KiB) from `/proc/meminfo` contents into bytes.
#[cfg(target_os = "linux")]
fn parse_meminfo_total_bytes(meminfo: &str) -> Option<usize> {
    for line in meminfo.lines() {
        if let Some(rest) = line.strip_prefix("MemTotal:") {
            let kib: usize = rest.split_whitespace().next()?.parse().ok()?;
            return kib.checked_mul(1024);
        }
    }
    None
}

/// Best-effort read of the process's cgroup memory limit in bytes, or `None`
/// when there is no readable limit (or the limit is the "unlimited" sentinel).
///
/// Crucially, this resolves the **process's own** cgroup from
/// `/proc/self/cgroup` rather than only reading the mount-root files, AND walks
/// its ancestors. A process in a non-root cgroup *without* a private cgroup
/// namespace — e.g. a systemd service with `MemoryMax=`, or a Docker/Kubernetes
/// container sharing the host cgroup namespace — has its limit at
/// `/sys/fs/cgroup<path>/memory.max` (v2) or
/// `/sys/fs/cgroup/memory<path>/memory.limit_in_bytes` (v1), where `<path>`
/// comes from `/proc/self/cgroup`. Reading only the mount root would miss that
/// limit and size the cache from host RAM (Codex #4481 review).
///
/// cgroup memory limits are **hierarchical**: the effective cap is the tightest
/// `memory.max` over the process's own cgroup and every ancestor up to the root.
/// A child whose own `memory.max` is `max` can still be bounded by an ancestor
/// (the common Docker/k8s/systemd-slice shape). So we walk leaf→root and take
/// the MINIMUM real limit found (also covers the private-namespace case, where
/// `/proc/self/cgroup` reads `/` and the walk is just the root). If both the v2
/// and v1 hierarchies report a real limit (hybrid mount), the smaller binds.
///
/// v2 (`memory.max`) uses the literal `max` for "no limit"; v1
/// (`memory.limit_in_bytes`) uses a near-`u64::MAX` sentinel (e.g.
/// `9223372036854771712`). Anything at or above
/// [`CGROUP_UNLIMITED_THRESHOLD_BYTES`] is treated as "no limit" so a host-level
/// (effectively unlimited) cgroup doesn't pin the budget to a meaningless huge
/// number.
#[cfg(target_os = "linux")]
fn read_cgroup_memory_limit_bytes() -> Option<usize> {
    let (v2_rel, v1_rel) = cgroup_relative_paths();

    // cgroup v2 limits are HIERARCHICAL: a process is bound by the tightest
    // `memory.max` among its own cgroup AND every ancestor. A child whose own
    // `memory.max` is `max` can still be capped by an ancestor (a Docker/k8s
    // container or systemd slice limit). Walk leaf→root under
    // /sys/fs/cgroup<path> and take the MIN real limit found (Codex #4481).
    let v2 = min_cgroup_limit_over_ancestors("/sys/fs/cgroup", "memory.max", &v2_rel);

    // cgroup v1 (memory controller): /sys/fs/cgroup/memory<path>/...; also
    // hierarchical, so walk ancestors the same way.
    let v1 =
        min_cgroup_limit_over_ancestors("/sys/fs/cgroup/memory", "memory.limit_in_bytes", &v1_rel);

    // A node is bound by whichever hierarchy actually applies; if both report a
    // real limit (hybrid mount), the smaller binds.
    match (v2, v1) {
        (Some(a), Some(b)) => Some(a.min(b)),
        (Some(a), None) => Some(a),
        (None, Some(b)) => Some(b),
        (None, None) => None,
    }
}

/// Walk the cgroup path `rel` (e.g. `/system.slice/x`) from leaf to root under
/// `mount` (e.g. `/sys/fs/cgroup`), reading `file` (e.g. `memory.max`) at each
/// level, and return the **minimum** real limit found, or `None` if no level
/// carries a real limit. This implements the hierarchical-min semantics of
/// cgroup memory limits: the effective cap is the tightest limit on the chain.
#[cfg(target_os = "linux")]
fn min_cgroup_limit_over_ancestors(mount: &str, file: &str, rel: &str) -> Option<usize> {
    let mut best: Option<usize> = None;
    for path in cgroup_ancestor_limit_paths(mount, file, rel) {
        if let Ok(s) = std::fs::read_to_string(&path) {
            if let Some(v) = parse_cgroup_limit(&s) {
                best = Some(best.map_or(v, |b: usize| b.min(v)));
            }
        }
    }
    best
}

/// Build the leaf→root list of limit-file paths to check for a cgroup at `rel`
/// under `mount`, reading `file` at each level. For `rel = "/a/b"` this yields
/// `["{mount}/a/b/{file}", "{mount}/a/{file}", "{mount}/{file}"]` (own cgroup,
/// then each ancestor up to the mount root). Pure (no I/O) so the ancestor walk
/// is unit-testable.
#[cfg(target_os = "linux")]
fn cgroup_ancestor_limit_paths(mount: &str, file: &str, rel: &str) -> Vec<String> {
    let mut paths = Vec::new();
    let mut current = rel.trim_end_matches('/').to_string();
    loop {
        paths.push(format!("{mount}{current}/{file}"));
        if current.is_empty() {
            break; // reached the mount root
        }
        match current.rfind('/') {
            Some(idx) => current.truncate(idx),
            None => current.clear(),
        }
    }
    paths
}

/// Resolve the process's cgroup paths (relative to the controller mount) for the
/// v2 unified hierarchy and the v1 `memory` controller, from `/proc/self/cgroup`.
///
/// Returns `(v2_rel, v1_rel)`, each a leading-slash path like `/system.slice/x`
/// or `/` (root). On any read/parse failure both default to `/` (mount root), so
/// the caller still attempts the root files. See [`parse_proc_self_cgroup`].
#[cfg(target_os = "linux")]
fn cgroup_relative_paths() -> (String, String) {
    match std::fs::read_to_string("/proc/self/cgroup") {
        Ok(s) => parse_proc_self_cgroup(&s),
        Err(_) => ("/".to_string(), "/".to_string()),
    }
}

/// Pure parse of `/proc/self/cgroup` contents into `(v2_rel, v1_rel)` cgroup
/// paths. Lines are `hierarchy-id:controllers:path`. The v2 line has an empty
/// controller field and hierarchy id `0` (`0::/path`); the v1 `memory` line
/// lists `memory` among its controllers (`N:memory:/path` or
/// `N:cpu,memory:/path`). Missing entries default to `/`.
#[cfg(target_os = "linux")]
fn parse_proc_self_cgroup(contents: &str) -> (String, String) {
    let mut v2 = "/".to_string();
    let mut v1 = "/".to_string();
    for line in contents.lines() {
        // splitn(3) because the path itself may contain ':'.
        let mut parts = line.splitn(3, ':');
        let (Some(_id), Some(controllers), Some(path)) = (parts.next(), parts.next(), parts.next())
        else {
            continue;
        };
        if controllers.is_empty() {
            // cgroup v2 unified line ("0::/path").
            v2 = path.to_string();
        } else if controllers.split(',').any(|c| c == "memory") {
            // cgroup v1 memory controller.
            v1 = path.to_string();
        }
    }
    (v2, v1)
}

/// Above this, a cgroup limit is treated as "unlimited" rather than a real
/// bound. Covers cgroup v1's `~u64::MAX` sentinel and a host-root cgroup whose
/// limit equals (or rounds to) all of physical RAM. Far above any budget we'd
/// ever want, so clamping the cache to it would be a no-op anyway.
///
/// NOTE: 64-bit-only. `1 << 60` overflows a 32-bit `usize`; the project targets
/// only 64-bit platforms (see cross-compile.yml). If a 32-bit target is ever
/// added, switch this and `parse_cgroup_limit` to `u64`.
#[cfg(target_os = "linux")]
const CGROUP_UNLIMITED_THRESHOLD_BYTES: usize = 1 << 60; // 1 EiB

/// Parse a single cgroup memory-limit file value (bytes) into a real limit, or
/// `None` for the literal `max`, an unparseable value, zero, or an "unlimited"
/// sentinel at/above [`CGROUP_UNLIMITED_THRESHOLD_BYTES`].
#[cfg(target_os = "linux")]
fn parse_cgroup_limit(contents: &str) -> Option<usize> {
    let trimmed = contents.trim();
    if trimmed == "max" {
        return None;
    }
    let bytes: usize = trimmed.parse().ok()?;
    // Zero is not a meaningful budget bound; treat as absent. Sentinels at/above
    // the threshold mean "unlimited".
    if bytes == 0 || bytes >= CGROUP_UNLIMITED_THRESHOLD_BYTES {
        return None;
    }
    Some(bytes)
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
    /// `[MIN, MAX]`, on any host (the `/proc/meminfo` read or its fallback both
    /// feed through the clamp).
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

    /// Small-box safety: on a constrained VPS the COMBINED default (contract +
    /// delegate cache) stays a small fraction of RAM and never OOMs. This is the
    /// invariant the #4441 fix must preserve — the `total_ram / 8` divisor, not
    /// the absolute MAX clamp, is what protects small hosts, so raising the MAX
    /// clamp must not change this. Checked across a sweep of small/medium RAM
    /// sizes where the divisor (not the MAX clamp) binds.
    #[test]
    fn combined_default_ceiling_is_safe_on_small_box() {
        // RAM sizes from a tiny 512 MiB VPS up to 12 GiB — the largest host at
        // which the divisor still binds below the MAX clamp.
        for total_ram_gib_eighths in 1..=96u64 {
            // step in 1/8-GiB increments so we cover sub-GiB hosts too.
            let total_ram = (total_ram_gib_eighths as usize) * (128 * 1024 * 1024);
            let contract = budget_for_ram(total_ram);
            let delegate = contract / DELEGATE_MODULE_CACHE_BUDGET_DIVISOR;
            let combined = contract + delegate;
            // Hard floor that must hold at EVERY RAM size, including the tiniest
            // box where the MIN clamp binds: the combined default must never
            // exceed total RAM. This catches an unsafe MIN raise (e.g. a MIN
            // larger than a small box's RAM), which the fractional bound below
            // would skip because its guard scales with MIN.
            assert!(
                combined <= total_ram,
                "combined default {combined} must never exceed total RAM \
                 ({total_ram}) — a MIN raise above a small box's RAM would OOM it"
            );
            // The combined default must also stay within 1/4 of RAM, so the
            // node always has at least 3/4 of RAM for everything else (state
            // store, transport buffers, WASM instance arenas, the OS). Below the
            // MIN clamp a very small box gets a fixed 64+16 MiB floor, which is
            // still tiny in absolute terms; only enforce the fraction bound once
            // RAM is large enough that the fixed floor isn't the binding term.
            if total_ram
                >= 4 * (MIN_DEFAULT_MODULE_CACHE_BUDGET_BYTES
                    + MIN_DEFAULT_MODULE_CACHE_BUDGET_BYTES / DELEGATE_MODULE_CACHE_BUDGET_DIVISOR)
            {
                assert!(
                    combined <= total_ram / 4,
                    "combined default {combined} must stay within 1/4 of RAM \
                     ({total_ram}); the divisor, not the MAX clamp, protects small boxes"
                );
            }
        }
    }

    // Superseded: the combined default ceiling at the MAX clamp is no longer
    // bounded by an absolute 512 MiB. PR #4481 raised
    // MAX_DEFAULT_MODULE_CACHE_BUDGET_BYTES from 384 MiB to 1.5 GiB (so large
    // gateways stop module-cache thrashing), making the combined ceiling
    // 1.5 GiB + 384 MiB ≈ 1.9 GiB. The OLD invariant below (combined < 512 MiB)
    // mistakenly pinned the MAX clamp as the small-box protection; the actual
    // protection is the `total_ram / 8` divisor. Replaced by
    // `combined_default_ceiling_is_safe_on_small_box` (divisor-relative, swept
    // across RAM sizes) and `max_clamp_combined_ceiling_is_safe_at_binding_host`
    // (upper bound at the smallest host where MAX binds). Kept as historical
    // documentation of the old (incorrect) invariant per git-workflow.md.
    #[ignore = "superseded by #4481: MAX raised 384 MiB -> 1.5 GiB; see replacement tests"]
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

    /// Upper-bound guard (the other side of the net): the COMBINED default
    /// ceiling at the MAX clamp must stay a safe fraction of the smallest host
    /// where the MAX binds (`MAX × DEFAULT_MODULE_CACHE_RAM_DIVISOR` of RAM), so
    /// a future MAX raise can't silently hand a just-large-enough box a cache
    /// that crowds out the rest of the node. Without this, every other test in
    /// the suite would still pass if MAX were bumped to, say, 12 GiB (which would
    /// give a 12-16 GiB box a ~15 GiB combined cache → OOM).
    #[test]
    fn max_clamp_combined_ceiling_is_safe_at_binding_host() {
        let contract = MAX_DEFAULT_MODULE_CACHE_BUDGET_BYTES;
        let delegate = contract / DELEGATE_MODULE_CACHE_BUDGET_DIVISOR;
        let combined = contract + delegate;
        // The smallest host at which the MAX clamp binds (below this the divisor
        // binds first and the small-box test covers it).
        let binding_host_ram =
            MAX_DEFAULT_MODULE_CACHE_BUDGET_BYTES * DEFAULT_MODULE_CACHE_RAM_DIVISOR;
        assert!(
            combined <= binding_host_ram / 4,
            "combined default at the MAX clamp ({combined}) must stay within 1/4 \
             of the smallest host where MAX binds ({binding_host_ram}); a MAX \
             raise that breaks this would OOM a just-large-enough box"
        );
    }

    /// Regression for issue #4441: a large gateway (nova has 125 GiB RAM) must
    /// get a default contract-cache budget far above the old 384 MiB ceiling, so
    /// its working set (hundreds of hosted contracts plus the post-#4404
    /// phantom-interest set) fits without permanent evict-and-recompile thrash.
    ///
    /// At `total_ram / 8`, any host with >12 GiB RAM lands on the MAX clamp, so
    /// the budget equals the MAX. This test pins that the MAX is high enough to
    /// hold a realistic gateway working set (~1000 modules at the measured
    /// ~1.5 MiB) and is strictly larger than the pre-fix 384 MiB that caused the
    /// thrash.
    #[test]
    fn large_gateway_default_exceeds_old_clamp() {
        const OLD_MAX: usize = 384 * 1024 * 1024;
        const MEASURED_MODULE_SIZE: usize = 1536 * 1024; // ~1.5 MiB, per #4452

        // nova-sized host: 125 GiB.
        let nova_ram = 125 * 1024 * 1024 * 1024usize;
        let budget = budget_for_ram(nova_ram);
        assert_eq!(
            budget, MAX_DEFAULT_MODULE_CACHE_BUDGET_BYTES,
            "a >12 GiB host lands on the MAX clamp"
        );
        assert!(
            budget > OLD_MAX,
            "the new default ceiling {budget} must exceed the pre-#4441 384 MiB \
             clamp that caused gateway module-cache thrash"
        );
        let modules_held = budget / MEASURED_MODULE_SIZE;
        assert!(
            modules_held >= 900,
            "the default ceiling must hold a realistic gateway working set \
             (~1000 modules at ~1.5 MiB each); holds {modules_held}"
        );
    }

    /// cgroup limit parsing: a real byte value is a limit; the v2 `max`
    /// sentinel, the v1 near-`u64::MAX` sentinel, zero, and garbage are all
    /// "no limit" (`None`). This is what keeps a containerized node from
    /// defaulting its cache to host RAM and OOMing its cgroup (#4481 review).
    #[cfg(target_os = "linux")]
    #[test]
    fn parse_cgroup_limit_distinguishes_real_limits_from_sentinels() {
        // Real limits (with surrounding whitespace/newline as the kernel emits).
        assert_eq!(
            parse_cgroup_limit("2147483648\n"),
            Some(2 * 1024 * 1024 * 1024)
        );
        assert_eq!(parse_cgroup_limit("  536870912  "), Some(512 * 1024 * 1024));
        // cgroup v2 "no limit".
        assert_eq!(parse_cgroup_limit("max\n"), None);
        // cgroup v1 unlimited sentinel (~u64::MAX rounded to page size).
        assert_eq!(parse_cgroup_limit("9223372036854771712"), None);
        // A host-root cgroup whose "limit" is enormous → treated as unlimited.
        assert_eq!(parse_cgroup_limit(&format!("{}", 1usize << 61)), None);
        // Degenerate / unparseable values.
        assert_eq!(parse_cgroup_limit("0"), None);
        assert_eq!(parse_cgroup_limit(""), None);
        assert_eq!(parse_cgroup_limit("not-a-number"), None);
    }

    /// `MemTotal` parsing pulls the KiB value and converts to bytes; a file
    /// without the line yields `None` (caller falls back).
    #[cfg(target_os = "linux")]
    #[test]
    fn parse_meminfo_total_reads_kib_as_bytes() {
        let sample = "MemFree:  100 kB\nMemTotal:       16331752 kB\nBuffers:  1 kB\n";
        assert_eq!(parse_meminfo_total_bytes(sample), Some(16331752 * 1024));
        assert_eq!(parse_meminfo_total_bytes("SwapTotal: 0 kB\n"), None);
    }

    /// `/proc/self/cgroup` parsing resolves the process's OWN cgroup sub-path for
    /// both the v2 unified line (`0::/path`) and the v1 `memory` controller line,
    /// so a non-root cgroup's limit file is found at the right place rather than
    /// only the mount root (Codex #4481 review). Defaults to `/` when absent.
    #[cfg(target_os = "linux")]
    #[test]
    fn parse_proc_self_cgroup_resolves_own_path() {
        // Pure cgroup v2 (systemd service with MemoryMax=, host cgroup ns).
        let v2_only = "0::/system.slice/freenet-gateway.service\n";
        assert_eq!(
            parse_proc_self_cgroup(v2_only),
            (
                "/system.slice/freenet-gateway.service".to_string(),
                "/".to_string()
            )
        );

        // Hybrid / v1 with the memory controller grouped among several.
        let hybrid = "12:cpu,memory:/docker/abc123\n0::/docker/abc123\n";
        assert_eq!(
            parse_proc_self_cgroup(hybrid),
            ("/docker/abc123".to_string(), "/docker/abc123".to_string())
        );

        // v1 memory controller alone, plus an unrelated controller line.
        let v1 = "8:memory:/kubepods/pod1\n9:cpuset:/elsewhere\n";
        assert_eq!(
            parse_proc_self_cgroup(v1),
            ("/".to_string(), "/kubepods/pod1".to_string())
        );

        // Root cgroup / empty input → defaults to "/".
        assert_eq!(
            parse_proc_self_cgroup("0::/\n"),
            ("/".to_string(), "/".to_string())
        );
        assert_eq!(
            parse_proc_self_cgroup(""),
            ("/".to_string(), "/".to_string())
        );

        // A path containing ':' must survive splitn(3).
        let weird = "0::/odd:name/x\n";
        assert_eq!(
            parse_proc_self_cgroup(weird),
            ("/odd:name/x".to_string(), "/".to_string())
        );
    }

    /// The cgroup ancestor walk yields the own cgroup's limit file first, then
    /// each ancestor up to the mount root — so `min_cgroup_limit_over_ancestors`
    /// honors the HIERARCHICAL nature of cgroup memory limits (a `max` leaf under
    /// a limited ancestor is still bounded). Codex #4481 finding.
    #[cfg(target_os = "linux")]
    #[test]
    fn cgroup_ancestor_paths_walk_leaf_to_root() {
        assert_eq!(
            cgroup_ancestor_limit_paths("/sys/fs/cgroup", "memory.max", "/a/b/c"),
            vec![
                "/sys/fs/cgroup/a/b/c/memory.max".to_string(),
                "/sys/fs/cgroup/a/b/memory.max".to_string(),
                "/sys/fs/cgroup/a/memory.max".to_string(),
                "/sys/fs/cgroup/memory.max".to_string(),
            ]
        );
        // Root cgroup → only the mount-root file.
        assert_eq!(
            cgroup_ancestor_limit_paths("/sys/fs/cgroup", "memory.max", "/"),
            vec!["/sys/fs/cgroup/memory.max".to_string()]
        );
        // A single-level cgroup → own file then root.
        assert_eq!(
            cgroup_ancestor_limit_paths(
                "/sys/fs/cgroup/memory",
                "memory.limit_in_bytes",
                "/system.slice"
            ),
            vec![
                "/sys/fs/cgroup/memory/system.slice/memory.limit_in_bytes".to_string(),
                "/sys/fs/cgroup/memory/memory.limit_in_bytes".to_string(),
            ]
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

    /// `ModuleCacheMetrics` routes by label, isolates contract vs delegate, and
    /// treats unknown labels as no-ops. Tests the publishing logic on a LOCAL
    /// instance so it stays deterministic and never touches the process-global.
    #[test]
    fn module_cache_metrics_routing_and_isolation() {
        let m = ModuleCacheMetrics::new();
        m.record_occupancy("contract", 3, 300, 1000);
        m.add_evictions("contract", 2);
        // Delegate stays zero — the two caches must not bleed into each other.
        let s = m.snapshot();
        assert_eq!(s.contract_entries, 3);
        assert_eq!(s.contract_total_bytes, 300);
        assert_eq!(s.contract_budget_bytes, 1000);
        assert_eq!(s.contract_evictions_total, 2);
        assert_eq!(s.delegate_entries, 0);
        assert_eq!(s.delegate_evictions_total, 0);

        m.record_occupancy("delegate", 1, 50, 250);
        m.add_evictions("delegate", 5);
        // Evictions are monotonic (accumulate); occupancy is last-write-wins.
        m.add_evictions("contract", 4);
        m.record_occupancy("contract", 1, 100, 1000);
        let s = m.snapshot();
        assert_eq!(s.contract_evictions_total, 6, "evictions accumulate");
        assert_eq!(s.contract_entries, 1, "occupancy is last-write-wins");
        assert_eq!(s.delegate_entries, 1);
        assert_eq!(s.delegate_evictions_total, 5);

        // An unlabeled / test cache ("module") must not publish anywhere.
        m.record_occupancy("module", 999, 999, 999);
        m.add_evictions("module", 999);
        let s = m.snapshot();
        assert_eq!(s.contract_entries, 1, "unknown label is a no-op");
        assert_eq!(s.delegate_entries, 1, "unknown label is a no-op");
    }

    /// A `"contract"`-labeled cache that evicts publishes occupancy + the
    /// monotonic eviction counter into its *injected* metrics `Arc`.
    ///
    /// Because the sink is constructed locally and passed in (rather than a
    /// process-global), this test is fully isolated — it can assert exact
    /// absolute values with no cross-talk from concurrent tests, which proves
    /// the old `MODULE_CACHE_METRICS` static is gone. (#4488)
    #[test]
    fn evicting_contract_cache_publishes_to_injected_metrics() {
        let metrics = Arc::new(ModuleCacheMetrics::new());
        // Constructing the cache publishes the budget immediately (occupancy 0).
        // 10-byte budget, 8-byte entries → the 2nd insert evicts the 1st.
        let mut cache: ModuleCache<u64, ()> =
            ModuleCache::with_label(10, "contract", Some(metrics.clone()));
        assert_eq!(
            metrics.snapshot().contract_evictions_total,
            0,
            "a fresh injected sink starts at zero — no global cross-talk"
        );
        assert_eq!(
            metrics.snapshot().contract_budget_bytes,
            10,
            "construction publishes the configured budget"
        );

        cache.insert(0, (), 8);
        cache.insert(1, (), 8); // total 16 > 10, len 2 > 1 → evict LRU (key 0)
        assert_eq!(cache.len(), 1, "the test setup must actually evict");

        let s = metrics.snapshot();
        assert_eq!(
            s.contract_evictions_total, 1,
            "exactly one eviction recorded in this isolated sink"
        );
        assert_eq!(s.contract_entries, 1, "occupancy is last-write-wins");
        assert_eq!(
            s.delegate_evictions_total, 0,
            "the delegate gauges must not bleed from the contract cache"
        );
    }

    /// A cache built without a metrics sink (`None`) records nothing and never
    /// panics — the unlabeled / test-runtime path.
    #[test]
    fn cache_without_metrics_sink_is_a_no_op() {
        let mut cache: ModuleCache<u64, ()> = ModuleCache::with_label(10, "contract", None);
        cache.insert(0, (), 8);
        cache.insert(1, (), 8);
        assert_eq!(cache.len(), 1, "eviction still happens without a sink");
    }

    /// An uninitialized budget gauge yields no occupancy signal (the
    /// placement-migration gate treats `None` as "no backpressure" — #4534).
    #[test]
    fn occupancy_pct_is_none_when_budget_uninitialized() {
        assert_eq!(occupancy_pct(0, 0), None);
        assert_eq!(occupancy_pct(1_000, 0), None);
    }

    /// Occupancy is `total * 100 / budget`, including the over-budget transient
    /// (> 100%) the cache shows between an insert and the eviction that follows.
    #[test]
    fn occupancy_pct_computes_percentage_of_budget() {
        assert_eq!(occupancy_pct(0, 1_000), Some(0));
        assert_eq!(occupancy_pct(500, 1_000), Some(50));
        assert_eq!(occupancy_pct(900, 1_000), Some(90));
        assert_eq!(occupancy_pct(1_000, 1_000), Some(100));
        assert_eq!(occupancy_pct(1_500, 1_000), Some(150));
    }

    /// A fully-occupied 1.5 GiB budget must not overflow `total * 100`.
    #[test]
    fn occupancy_pct_handles_gib_scale_without_overflow() {
        // `1.5 GiB * 100` is ~1.6e11, well within u64; assert no overflow panic.
        let budget = 1_536u64 * 1024 * 1024;
        assert_eq!(occupancy_pct(budget, budget), Some(100));
        // Cleanly-divisible large budget to assert the 90% boundary exactly
        // (avoids integer-truncation noise from a non-divisible GiB budget).
        assert_eq!(occupancy_pct(900_000_000, 1_000_000_000), Some(90));
    }

    /// Floor-division must land 89.x% occupancy on the admit side of the 90%
    /// decision boundary and exactly-90% on the refuse side — the truncation
    /// edge the whole gate hinges on (#4534). 899/1000 = 89.9% → 89 (admit);
    /// 900/1000 = 90.0% → 90 (refuse).
    #[test]
    fn occupancy_pct_truncates_below_the_decision_boundary() {
        assert_eq!(occupancy_pct(899, 1_000), Some(89));
        assert_eq!(occupancy_pct(900, 1_000), Some(90));
        // Just over the boundary stays refused.
        assert_eq!(occupancy_pct(901, 1_000), Some(90));
    }

    // ========================================================================
    // Interest-weighted (two-tier) eviction (#4441/#4534).
    // ========================================================================

    /// Build an interest predicate over an explicit set of "interested" keys,
    /// shared so a test can mutate which keys are in use across inserts.
    fn interest_over(
        set: Arc<std::sync::Mutex<std::collections::HashSet<u64>>>,
    ) -> InterestPredicate<u64> {
        Arc::new(move |k: &u64| set.lock().unwrap().contains(k))
    }

    /// Flag parsing: only the recognized truthy spellings enable the policy;
    /// everything else (including unset) is the safe default OFF.
    #[test]
    fn interest_tiered_flag_parses_truthy_only() {
        for v in ["1", "true", "TRUE", " yes ", "On", "oN"] {
            assert!(parse_interest_tiered_flag(Some(v)), "{v:?} must be truthy");
        }
        for v in ["0", "false", "no", "off", "", "2", "enable", "y"] {
            assert!(
                !parse_interest_tiered_flag(Some(v)),
                "{v:?} must NOT enable the policy"
            );
        }
        assert!(
            !parse_interest_tiered_flag(None),
            "unset env must default OFF (pure byte-LRU)"
        );
    }

    /// With the flag OFF the eviction is EXACTLY today's pure byte-LRU even when
    /// an interest predicate is present: the absolute LRU is evicted regardless
    /// of interest. This is the property that makes the change inert by default.
    #[test]
    fn flag_off_evicts_absolute_lru_ignoring_interest() {
        let interested = Arc::new(std::sync::Mutex::new(std::collections::HashSet::new()));
        interested.lock().unwrap().insert(0u64); // key 0 is the in-use LRU
        let mut cache = ModuleCache::<u64, ()>::with_interest_for_test(
            10,
            "contract",
            None,
            interest_over(interested.clone()),
            false, // flag OFF → pure LRU
        );
        assert!(!cache.interest_tiered_active());
        cache.insert(0, (), 6); // interested, becomes LRU
        cache.insert(1, (), 6); // cold; total 12 > 10 → evict the absolute LRU (key 0)
        assert_eq!(cache.len(), 1);
        assert!(
            cache.get(&0).is_none(),
            "flag OFF: the interested-but-LRU entry is evicted, exactly as plain LRU"
        );
        assert!(cache.get(&1).is_some());
        assert!(cache.total_bytes() <= cache.budget_bytes());
    }

    /// With the flag ON the two-tier policy evicts the cold (no-interest) entry
    /// first and SPARES the interested one, even though the interested entry is
    /// the absolute LRU. Strict byte budget still holds.
    #[test]
    fn flag_on_evicts_cold_first_sparing_interested() {
        let interested = Arc::new(std::sync::Mutex::new(std::collections::HashSet::new()));
        interested.lock().unwrap().insert(0u64); // key 0 is in use
        let mut cache = ModuleCache::<u64, ()>::with_interest_for_test(
            10,
            "contract",
            None,
            interest_over(interested.clone()),
            true, // flag ON → two-tier
        );
        assert!(cache.interest_tiered_active());
        cache.insert(0, (), 6); // interested, becomes the absolute LRU
        cache.insert(1, (), 6); // cold; total 12 > 10 → two-tier evicts the COLD key 1
        assert_eq!(cache.len(), 1);
        assert!(
            cache.get(&0).is_some(),
            "two-tier: interested entry is spared even as the absolute LRU"
        );
        assert!(
            cache.get(&1).is_none(),
            "two-tier: the cold entry is evicted first"
        );
        assert!(cache.total_bytes() <= cache.budget_bytes());
    }

    /// When the interested set ALONE exceeds the byte budget, the two-tier
    /// policy MUST fall back to evicting interested LRU entries — the byte budget
    /// is never weakened (no contract is pinned forever). #4565 OOM guard.
    #[test]
    fn flag_on_evicts_interested_when_interested_alone_exceeds_budget() {
        let interested = Arc::new(std::sync::Mutex::new(std::collections::HashSet::new()));
        // ALL keys are interested.
        for k in 0..3u64 {
            interested.lock().unwrap().insert(k);
        }
        let mut cache = ModuleCache::<u64, ()>::with_interest_for_test(
            10,
            "contract",
            None,
            interest_over(interested.clone()),
            true,
        );
        cache.insert(0, (), 6); // interested, LRU
        cache.insert(1, (), 6); // interested; total 12 > 10, no cold to evict →
        // fall back to interested LRU (key 0)
        assert!(
            cache.total_bytes() <= cache.budget_bytes(),
            "byte budget is strict even when the whole working set is interested"
        );
        assert_eq!(cache.len(), 1);
        assert!(
            cache.get(&0).is_none(),
            "interested LRU is evicted when interested-alone exceeds budget — \
             nothing is pinned forever"
        );
        assert!(cache.get(&1).is_some());
    }

    /// Two-tier evicts as many cold entries as needed, then crosses into the
    /// interested tier only when cold alone can't free enough — and stops the
    /// moment the budget is met. Mixed working set.
    #[test]
    fn flag_on_drains_cold_then_crosses_into_interested() {
        let interested = Arc::new(std::sync::Mutex::new(std::collections::HashSet::new()));
        // keys 0,1 interested; keys 2,3 cold. Insert order LRU→MRU: 0,1,2,3.
        interested.lock().unwrap().insert(0u64);
        interested.lock().unwrap().insert(1u64);
        let mut cache = ModuleCache::<u64, ()>::with_interest_for_test(
            10,
            "contract",
            None,
            interest_over(interested.clone()),
            true,
        );
        cache.insert(0, (), 4); // interested
        cache.insert(1, (), 4); // interested; total 8
        // Insert cold key 2 (total 12 > 10): two-tier evicts the cold LRU (key 2
        // itself is MRU, so the only cold entry is key 2 — but it's MRU, so the
        // LRU cold is... key 2). Use a clearer sequence instead:
        // reset and drive a deterministic mixed eviction.
        cache.remove(&0);
        cache.remove(&1);
        // Fresh: insert cold 2, cold 3, interested 0, interested 1 (LRU→MRU).
        cache.insert(2, (), 4); // cold, LRU
        cache.insert(3, (), 4); // cold; total 8
        cache.insert(0, (), 4); // interested; total 12 > 10 → evict cold LRU (key 2)
        assert!(cache.total_bytes() <= cache.budget_bytes());
        assert!(cache.get(&2).is_none(), "cold LRU evicted first");
        assert!(cache.get(&3).is_some(), "second cold spared (budget met)");
        assert!(cache.get(&0).is_some(), "interested newcomer kept");
        // Now insert interested 1: total 12 > 10, one cold remains (key 3) →
        // evict it before any interested.
        cache.insert(1, (), 4);
        assert!(cache.total_bytes() <= cache.budget_bytes());
        assert!(
            cache.get(&3).is_none(),
            "remaining cold evicted before interested"
        );
        assert!(cache.get(&0).is_some(), "interested entries both spared");
        assert!(cache.get(&1).is_some());
    }

    /// The always-on shadow gauges (cold-evictable / interested bytes) are
    /// published from the interest predicate REGARDLESS of the flag, so the
    /// pressure split is observable before anyone flips the policy on.
    #[test]
    fn shadow_interest_bytes_published_with_flag_off() {
        let metrics = Arc::new(ModuleCacheMetrics::new());
        let interested = Arc::new(std::sync::Mutex::new(std::collections::HashSet::new()));
        interested.lock().unwrap().insert(0u64);
        let mut cache = ModuleCache::<u64, ()>::with_interest_for_test(
            1_000_000,
            "contract",
            Some(metrics.clone()),
            interest_over(interested.clone()),
            false, // flag OFF — gauges must STILL be computed
        );
        cache.insert(0, (), 100); // interested
        cache.insert(1, (), 250); // cold
        cache.insert(2, (), 300); // cold
        let s = metrics.snapshot();
        assert_eq!(s.contract_interested_bytes, 100, "only key 0 is interested");
        assert_eq!(
            s.contract_cold_evictable_bytes, 550,
            "keys 1 + 2 are cold-evictable"
        );
    }

    /// The would-reclassify shadow counter increments — even with the flag OFF —
    /// for each eviction step whose victim the two-tier policy would pick
    /// differently from plain LRU (an interested absolute-LRU spared for a cold
    /// entry). This is the signal that justifies flipping the flag.
    #[test]
    fn shadow_would_reclassify_counts_with_flag_off() {
        let metrics = Arc::new(ModuleCacheMetrics::new());
        let interested = Arc::new(std::sync::Mutex::new(std::collections::HashSet::new()));
        interested.lock().unwrap().insert(0u64); // interested LRU
        let mut cache = ModuleCache::<u64, ()>::with_interest_for_test(
            10,
            "contract",
            Some(metrics.clone()),
            interest_over(interested.clone()),
            false, // flag OFF: eviction is plain LRU, but the shadow still counts
        );
        cache.insert(0, (), 6); // interested, absolute LRU
        cache.insert(1, (), 6); // cold; plain LRU evicts key 0 (interested) while a
        // cold (key 1) exists → two-tier WOULD differ.
        let s = metrics.snapshot();
        assert!(
            cache.get(&0).is_none(),
            "flag OFF still evicts the interested LRU"
        );
        assert_eq!(
            s.contract_evictions_would_reclassify_total, 1,
            "exactly one eviction would have been reclassified by the two-tier policy"
        );
    }

    /// No would-reclassify when plain LRU and two-tier agree (the absolute LRU
    /// is already the cold one).
    #[test]
    fn shadow_would_reclassify_zero_when_policies_agree() {
        let metrics = Arc::new(ModuleCacheMetrics::new());
        let interested = Arc::new(std::sync::Mutex::new(std::collections::HashSet::new()));
        interested.lock().unwrap().insert(1u64); // the MRU is interested; LRU is cold
        let mut cache = ModuleCache::<u64, ()>::with_interest_for_test(
            10,
            "contract",
            Some(metrics.clone()),
            interest_over(interested.clone()),
            false,
        );
        cache.insert(0, (), 6); // cold, LRU
        cache.insert(1, (), 6); // interested; total 12 > 10 → both policies evict cold key 0
        let s = metrics.snapshot();
        assert!(cache.get(&0).is_none());
        assert_eq!(
            s.contract_evictions_would_reclassify_total, 0,
            "policies agree (LRU is already cold) → no reclassification"
        );
    }

    /// The delegate / no-predicate path never publishes interest shadow gauges
    /// and is always pure LRU regardless of the flag.
    #[test]
    fn no_predicate_cache_has_zero_interest_shadow() {
        let metrics = Arc::new(ModuleCacheMetrics::new());
        let mut cache: ModuleCache<u64, ()> =
            ModuleCache::with_label(10, "delegate", Some(metrics.clone()));
        assert!(
            !cache.interest_tiered_active(),
            "no predicate → never tiered"
        );
        cache.insert(0, (), 6);
        cache.insert(1, (), 6);
        let s = metrics.snapshot();
        // The delegate cache has no cold/interested split — both stay zero.
        assert_eq!(s.contract_cold_evictable_bytes, 0);
        assert_eq!(s.contract_interested_bytes, 0);
    }

    /// `migration_admission_would_change`: the gates disagree ONLY when the
    /// current gate refuses (occupancy ≥ ceiling) AND dropping all cold-evictable
    /// bytes would pull the interested floor below the ceiling.
    #[test]
    fn migration_admission_would_change_logic() {
        const CEIL: u64 = 90;
        // Over the ceiling but the interested floor alone is well under it →
        // cold-aware gate would admit → the gates DISAGREE.
        assert!(migration_admission_would_change(
            /* total */ 950, /* interested */ 100, /* budget */ 1000, CEIL
        ));
        // Over the ceiling AND the interested floor alone is still over it →
        // even the cold-aware gate refuses → the gates AGREE.
        assert!(!migration_admission_would_change(950, 950, 1000, CEIL));
        // Under the ceiling → current gate already admits → no disagreement.
        assert!(!migration_admission_would_change(500, 100, 1000, CEIL));
        // Budget uninitialized → no pressure signal → no disagreement.
        assert!(!migration_admission_would_change(950, 100, 0, CEIL));
        // Exactly at the ceiling counts as a refuse; floor under it → disagree.
        assert!(migration_admission_would_change(900, 100, 1000, CEIL));
    }

    /// The node-level migration-admission would-change counter is monotonic and
    /// surfaces in the snapshot.
    #[test]
    fn migration_admission_would_change_counter_accumulates() {
        let m = ModuleCacheMetrics::new();
        assert_eq!(m.snapshot().migration_admission_would_change_total, 0);
        m.record_migration_admission_would_change();
        m.record_migration_admission_would_change();
        assert_eq!(m.snapshot().migration_admission_would_change_total, 2);
    }

    /// The interest shadow gauges track interest flips that happen with NO cache
    /// mutation (a client disconnect / lease expiry), picked up on the next
    /// `get` via the throttled refresh — addressing the Codex review finding that
    /// they would otherwise stay stale on a read-mostly cache. The first `get`
    /// after the flip always refreshes (the throttle starts un-armed).
    #[test]
    fn interest_shadow_refreshes_on_get_after_interest_flip() {
        let metrics = Arc::new(ModuleCacheMetrics::new());
        let interested = Arc::new(std::sync::Mutex::new(std::collections::HashSet::new()));
        interested.lock().unwrap().insert(0u64);
        let mut cache = ModuleCache::<u64, ()>::with_interest_for_test(
            1_000_000,
            "contract",
            Some(metrics.clone()),
            interest_over(interested.clone()),
            false,
        );
        cache.insert(0, (), 100); // interested at insert time
        cache.insert(1, (), 250); // cold
        let s = metrics.snapshot();
        assert_eq!(s.contract_interested_bytes, 100);
        assert_eq!(s.contract_cold_evictable_bytes, 250);

        // Interest flips with NO cache mutation: key 0 is no longer in use.
        interested.lock().unwrap().remove(&0);
        // The stale gauges still say 100 interested until something reads.
        assert_eq!(metrics.snapshot().contract_interested_bytes, 100);

        // A `get` triggers the throttled refresh (un-armed → fires immediately),
        // and the gauges now reflect that NOTHING is interested.
        let _ = cache.get(&1);
        let s = metrics.snapshot();
        assert_eq!(
            s.contract_interested_bytes, 0,
            "interest flip with no cache mutation must be picked up on the next get"
        );
        assert_eq!(
            s.contract_cold_evictable_bytes, 350,
            "both entries are now cold-evictable"
        );
    }

    /// Thrash-reduction model (#4441): a small HOT interested working set is
    /// interleaved with a stream of cold one-shot contracts that overflow the
    /// budget. With the flag ON, the hot set stays resident the whole time, so a
    /// node would NEVER recompile it; with the flag OFF (plain byte-LRU) the cold
    /// churn repeatedly evicts the hot set, forcing the recompile-on-access
    /// thrash this change exists to relieve. We assert the resident-state
    /// difference at the cache level (each cold insert simulates a cold-contract
    /// access; a missing hot key would be a recompile in production).
    #[test]
    fn two_tier_keeps_hot_set_resident_under_cold_churn() {
        // Budget holds the 2 hot entries (20) plus a little cold headroom (1
        // cold entry of 10) — every additional cold insert must evict something.
        let budget = 30usize;
        let hot: [u64; 2] = [1000, 1001];
        let interested = Arc::new(std::sync::Mutex::new(std::collections::HashSet::new()));
        interested.lock().unwrap().insert(hot[0]);
        interested.lock().unwrap().insert(hot[1]);

        // Helper: prime the hot set, then run rounds of a COLD BURST (several
        // cold one-shot contracts back-to-back, no hot access in between — the
        // realistic shape where a stream of cold traffic pushes the hot set down
        // the recency order) followed by hot accesses. Returns how many times a
        // hot key was found MISSING at the start of a round (== a recompile in
        // production). The burst length (3) matches the budget's entry capacity
        // so plain LRU drains the hot set; two-tier protects it.
        let run = |tiered: bool| -> u32 {
            let mut cache = ModuleCache::<u64, ()>::with_interest_for_test(
                budget,
                "contract",
                None,
                interest_over(interested.clone()),
                tiered,
            );
            cache.insert(hot[0], (), 10);
            cache.insert(hot[1], (), 10);
            let mut hot_misses = 0u32;
            let mut cold_key = 0u64;
            for _round in 0..6 {
                // Cold burst: 3 cold accesses with no interleaved hot touch.
                for _ in 0..3 {
                    cache.insert(cold_key, (), 10);
                    cold_key += 1;
                }
                // Now the hot ops resume — recompile any hot key the burst evicted.
                for &h in &hot {
                    if cache.get(&h).is_none() {
                        hot_misses += 1;
                        cache.insert(h, (), 10);
                    }
                }
            }
            hot_misses
        };

        let tiered_misses = run(true);
        let lru_misses = run(false);
        assert_eq!(
            tiered_misses, 0,
            "two-tier policy must keep the hot interested set resident — zero recompiles"
        );
        assert!(
            lru_misses > 0,
            "plain byte-LRU evicts the hot set under cold churn (the #4441 thrash); \
             tiered={tiered_misses} lru={lru_misses}"
        );
    }
}
