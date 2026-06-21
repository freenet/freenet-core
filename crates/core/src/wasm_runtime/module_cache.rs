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
use std::sync::LazyLock;
use std::sync::atomic::{AtomicU64, Ordering};
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
        let budget_bytes = budget_bytes.max(1);
        // Publish the budget immediately, so an idle cache (a node that hasn't
        // compiled a module yet, or a delegate cache that's never used) reports
        // its configured budget instead of the atomic default 0. Otherwise the
        // collector's occupancy-% denominator is 0 until the first insert/remove
        // (#4440 — flagged by both PR reviewers).
        MODULE_CACHE_METRICS.record_occupancy(label, 0, 0, budget_bytes);
        Self {
            // The LRU is unbounded by count; the byte budget is the only
            // bound. `usize::MAX` capacity means `LruCache` never evicts on
            // its own — we drive all eviction via the byte budget below.
            inner: LruCache::unbounded(),
            total_bytes: 0,
            budget_bytes,
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
        MODULE_CACHE_METRICS.record_occupancy(
            self.label,
            self.inner.len(),
            self.total_bytes,
            self.budget_bytes,
        );
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
            // Monotonic lifetime counter for telemetry; the collector differences
            // it across the snapshot cadence to derive an eviction rate. Distinct
            // from `evictions_in_window`, which resets per warn window (#4440).
            MODULE_CACHE_METRICS.add_evictions(self.label, evicted_this_insert);
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
        MODULE_CACHE_METRICS.record_occupancy(
            self.label,
            self.inner.len(),
            self.total_bytes,
            self.budget_bytes,
        );
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

/// Process-global compiled-WASM module-cache occupancy + eviction telemetry
/// (#4440).
///
/// The module caches live behind the contract-handler channel, unreachable from
/// the `Ring` telemetry-snapshot task that emits `router_snapshot`. Like
/// [`TRANSPORT_METRICS`](crate::transport::metrics::TRANSPORT_METRICS), the
/// caches therefore *publish* into this process-global and the snapshot task
/// *reads* it. A node builds exactly one contract cache and one delegate cache
/// (`RuntimePool::new`), so one global pair of gauges is correct; this assumes a
/// single runtime pool per process (the production invariant). The cache-thrash
/// that drove the #4441 incident was invisible to central telemetry — these
/// gauges make occupancy and eviction pressure observable.
pub(crate) static MODULE_CACHE_METRICS: LazyLock<ModuleCacheMetrics> =
    LazyLock::new(ModuleCacheMetrics::new);

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
}

/// Per-cache module-cache telemetry, routed by the cache's `"contract"` /
/// `"delegate"` label. See [`MODULE_CACHE_METRICS`].
pub(crate) struct ModuleCacheMetrics {
    contract: CacheGauges,
    delegate: CacheGauges,
}

/// A point-in-time read of [`MODULE_CACHE_METRICS`] for telemetry emission.
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
}

impl ModuleCacheMetrics {
    fn new() -> Self {
        Self {
            contract: CacheGauges::default(),
            delegate: CacheGauges::default(),
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
        }
    }
}

/// Contract-module-cache occupancy as a percentage of the configured byte
/// budget, read lock-free from the process-global [`MODULE_CACHE_METRICS`].
///
/// Returns `None` when the budget gauge has not been published yet — i.e. no
/// runtime pool has been built in this process (early startup, or a test / tool
/// binary with no WASM runtime). Callers treat `None` as "no pressure signal".
///
/// Used by the placement-migration admission gate (#4534): once occupancy is at
/// or above the admission ceiling, the node stops accepting inbound
/// `SubscribeHint` migrations so the hosted working set cannot grow past cache
/// capacity and thrash (recompile-on-access), which had filled the contract fair
/// queue and produced client-facing "contract queue full" outages and gateway
/// OOMs on 0.2.80.
pub(crate) fn contract_cache_occupancy_pct() -> Option<u64> {
    let snapshot = MODULE_CACHE_METRICS.snapshot();
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

    /// A `"contract"`-labeled cache that evicts publishes to the process-global
    /// monotonic eviction counter. Asserts only the strict increase, which is
    /// race-safe under concurrent tests (the counter is `fetch_add`-only and
    /// never resets), unlike the last-write-wins occupancy gauges.
    #[test]
    fn evicting_contract_cache_increments_global_eviction_counter() {
        let before = MODULE_CACHE_METRICS.snapshot().contract_evictions_total;
        // 10-byte budget, 8-byte entries → the 2nd insert evicts the 1st.
        let mut cache: ModuleCache<u64, ()> = ModuleCache::with_label(10, "contract");
        cache.insert(0, (), 8);
        cache.insert(1, (), 8); // total 16 > 10, len 2 > 1 → evict LRU (key 0)
        assert_eq!(cache.len(), 1, "the test setup must actually evict");
        let after = MODULE_CACHE_METRICS.snapshot().contract_evictions_total;
        assert!(
            after > before,
            "eviction on a contract-labeled cache must bump the global counter \
             (before={before}, after={after})"
        );
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
}
