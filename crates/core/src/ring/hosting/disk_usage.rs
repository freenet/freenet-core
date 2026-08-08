//! Aggregate on-disk usage accounting for demand-driven hosting (#4683).
//!
//! The demand-driven hosting budget (#4642) sizes itself from **memory** only.
//! For a disk-constrained host, disk — not RAM — is the scarcest resource, so a
//! separate disk budget bounds aggregate on-disk usage. [`DiskUsageTracker`] is
//! the shared source of truth that this disk budget reads, and it now feeds two
//! live decisions, both wired in #4702:
//!
//! - **Eviction floor:** [`super::HostingManager::recompute_effective_budget`]
//!   installs `effective_budget = min(ram_budget, disk_budget)` on the ~60s
//!   sweep, so under disk pressure eviction sheds (subscriber-primary order)
//!   against the disk-constrained budget.
//! - **Pre-write PUT admission gate:** [`DiskUsageTracker::admit_state_write`]
//!   rejects a new state write that would push aggregate on-disk usage past the
//!   disk budget.
//!
//! The disk budget is `clamp(0.5 * (used + available), 128 MiB, 32 GiB)`, tunable
//! via `--hosting-disk-pct` / `--max-hosting-disk`. This module owns the
//! accounting + telemetry those two decisions read.
//!
//! # What is counted
//!
//! Three independently-measured consumers, summed by [`DiskUsageTracker::total_bytes`]:
//!
//! - **Hosted contract state** — the exact byte total of persisted contract
//!   state. Seeded once by summing every row's
//!   [`HostingMetadata::size_bytes`](crate::contract::storages::HostingMetadata)
//!   and thereafter maintained by signed deltas at the executor's state-write
//!   chokepoints (via [`super::HostingManager::record_state_write`]) and at
//!   reclamation (via [`super::HostingManager::record_state_removed`]). A small
//!   per-key size index makes the delta exact without re-reading the DB.
//! - **WASM code blobs** — the `*.wasm` files under `contracts_dir`. Re-walked
//!   (`du`) on seed and on each telemetry refresh; blobs dedupe by `code_hash`
//!   so a re-PUT of already-stored code adds nothing.
//! - **Wasmtime compile cache** — wasmtime writes it opaquely, so it is not
//!   delta-tracked; it is re-walked on each telemetry refresh. Cheap: bounded by
//!   the number of distinct compiled modules and self-pruned by wasmtime at its
//!   soft-size limit.
//!
//! Of the three, the compile cache is the one the hosting sweep **cannot
//! reclaim** — eviction sheds contract state, and wasmtime owns its cache
//! directory. So its soft limit has to be bounded by the same disk budget it is
//! charged against, or a disk-tight host can push `total_bytes()` past the
//! budget with bytes nothing in the node is able to free (#5014). Wasmtime fixes
//! that limit at `Cache::new`, before this tracker exists, which is what
//! [`startup_disk_budget_estimate`] is for.
//!
//! # Seeding discipline (fail-loud)
//!
//! [`DiskUsageTracker::seed`] mirrors the #4561 secrets `seeded_user_total`
//! discipline: it walks the real on-disk state ONCE and is **fail-loud** on I/O
//! error. A silently-too-low seed would defeat the admission gate (it
//! would admit writes that actually overflow disk), so a seed that cannot read
//! the truth must surface the error rather than start from an under-count.

use std::collections::{BTreeMap, HashMap};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

use freenet_stdlib::prelude::ContractKey;
use parking_lot::Mutex;

use crate::tracing::event_kind::{STATE_SIZE_BUCKET_COUNT, state_size_bucket};
use crate::wasm_runtime::MAX_STATE_SIZE;

/// A pre-write admission check rejected a state/code write because it would
/// push aggregate on-disk usage past the disk budget (#4683, admission gate live
/// since #4702).
///
/// Carries the numbers that made the decision so the caller can surface a
/// human-readable cause on the client `Err` and (for PUT) the `PutMsg::Error`
/// network abort. The write MUST NOT have touched disk when this is returned:
/// the gate runs BEFORE the store call, so no rollback is needed for the
/// rejected write itself.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct DiskBudgetExceeded {
    /// Aggregate on-disk bytes projected AFTER the rejected write would land
    /// (`total − old_for_key + new`). Strictly greater than `budget_bytes`.
    pub projected_bytes: u64,
    /// The aggregate disk budget the projection exceeded.
    pub budget_bytes: u64,
}

impl std::fmt::Display for DiskBudgetExceeded {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "disk budget exceeded: write would use {} bytes on disk, budget is {} bytes",
            self.projected_bytes, self.budget_bytes
        )
    }
}

/// Point-in-time on-disk usage gauges, one snapshot for telemetry.
///
/// Aggregate scalars only, emitted on the existing `RouterSnapshot` cadence
/// alongside the RAM-budget gauges so the disk-budget feature is observable in
/// production alongside the eviction floor and admission gate it feeds (#4702).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub(crate) struct DiskUsageStats {
    /// Persisted contract-state bytes (delta-tracked, seeded from redb rows).
    pub state_bytes: u64,
    /// Number of persisted contract states in the authoritative size index.
    pub state_count: u64,
    /// Exact state counts by fixed state-size bucket.
    pub state_size_bucket_counts: [u64; STATE_SIZE_BUCKET_COUNT],
    /// Exact state bytes by the same fixed state-size bucket.
    pub state_size_bucket_bytes: [u64; STATE_SIZE_BUCKET_COUNT],
    /// Largest persisted contract state, or zero when none are stored.
    pub state_max_bytes: u64,
    /// Persisted states above the runtime's hard state-size limit. This should
    /// always be zero; a nonzero value exposes an enforcement invariant break.
    pub state_over_limit_count: u64,
    /// Bytes held by persisted states above the hard state-size limit.
    pub state_over_limit_bytes: u64,
    /// Runtime hard state-size limit used for the invariant above.
    pub state_limit_bytes: u64,
    /// On-disk WASM code blob bytes (`du` of `contracts_dir/*.wasm`).
    pub wasm_bytes: u64,
    /// Wasmtime compile-cache bytes (`du` of the relocated cache dir).
    pub compile_cache_bytes: u64,
    /// Sum of the three above — the aggregate the disk budget bounds.
    pub total_bytes: u64,
}

/// Exact state-size index with incrementally maintained fixed-cardinality
/// aggregates. Snapshotting this structure is O(number of buckets), not
/// O(number of hosted contracts), so telemetry never pauses state commits for
/// a full-map clone.
#[derive(Default)]
struct StateSizeIndex {
    by_contract: HashMap<ContractKey, u64>,
    by_size: BTreeMap<u64, u64>,
    bucket_counts: [u64; STATE_SIZE_BUCKET_COUNT],
    bucket_bytes: [u64; STATE_SIZE_BUCKET_COUNT],
    over_limit_count: u64,
    over_limit_bytes: u64,
}

impl StateSizeIndex {
    fn get(&self, key: &ContractKey) -> Option<u64> {
        self.by_contract.get(key).copied()
    }

    fn insert(&mut self, key: ContractKey, size: u64) -> Option<u64> {
        let old = self.by_contract.insert(key, size);
        if let Some(old) = old {
            self.remove_size(old);
        }
        self.add_size(size);
        old
    }

    fn remove(&mut self, key: &ContractKey) -> Option<u64> {
        let size = self.by_contract.remove(key)?;
        self.remove_size(size);
        Some(size)
    }

    fn add_size(&mut self, size: u64) {
        let count = self.by_size.entry(size).or_default();
        *count = count.saturating_add(1);
        let bucket = state_size_bucket(size);
        self.bucket_counts[bucket] = self.bucket_counts[bucket].saturating_add(1);
        self.bucket_bytes[bucket] = self.bucket_bytes[bucket].saturating_add(size);
        if size > MAX_STATE_SIZE as u64 {
            self.over_limit_count = self.over_limit_count.saturating_add(1);
            self.over_limit_bytes = self.over_limit_bytes.saturating_add(size);
        }
    }

    fn remove_size(&mut self, size: u64) {
        if let std::collections::btree_map::Entry::Occupied(mut entry) = self.by_size.entry(size) {
            if *entry.get() <= 1 {
                entry.remove();
            } else {
                *entry.get_mut() -= 1;
            }
        }
        let bucket = state_size_bucket(size);
        self.bucket_counts[bucket] = self.bucket_counts[bucket].saturating_sub(1);
        self.bucket_bytes[bucket] = self.bucket_bytes[bucket].saturating_sub(size);
        if size > MAX_STATE_SIZE as u64 {
            self.over_limit_count = self.over_limit_count.saturating_sub(1);
            self.over_limit_bytes = self.over_limit_bytes.saturating_sub(size);
        }
    }
}

/// Signed-delta + seed-once tracker for aggregate hosting disk usage.
///
/// Cheap to read (three `AtomicU64` loads for [`Self::total_bytes`]); the only
/// expensive operations are [`Self::seed`] and the two `refresh_*` `du`-walks,
/// which run off the hot path (lazy seed on the first sweep tick, refresh on the
/// 60s telemetry cadence).
pub(crate) struct DiskUsageTracker {
    /// Persisted contract-state bytes. Maintained by signed deltas.
    state_bytes: AtomicU64,
    /// On-disk WASM blob bytes. Refreshed by `du`-walk.
    wasm_bytes: AtomicU64,
    /// Wasmtime compile-cache bytes. Refreshed by `du`-walk.
    compile_cache_bytes: AtomicU64,
    /// One-time seed guard (like the secrets `seeded_user_total` flag).
    seeded: AtomicBool,
    /// Per-contract last-known state size, so a state-write delta is exact
    /// (`new − previous_for_key`) without re-reading the DB at the chokepoint.
    /// Seeded from the same redb rows that seed `state_bytes`.
    ///
    /// A whole-map `Mutex`, NOT a `DashMap`: this is the documented
    /// cross-key-atomicity exception in `.claude/rules/code-style.md` ("WHEN
    /// writing async code" — DashMap exception). DashMap's per-shard locks
    /// cannot serialize the whole map against the single `seeded` flag flip,
    /// which is exactly what closes the seed/write TOCTOU documented on
    /// [`Self::seed`] and [`Self::record_state_write`]. Per-shard locking would
    /// reopen that race and turn the counter into a load-bearing under-count
    /// now that the admission gate reads it (#4702).
    state_sizes: Mutex<StateSizeIndex>,
    /// Directory holding `*.wasm` code blobs (mode-resolved `contracts_dir`).
    contracts_dir: PathBuf,
    /// Relocated wasmtime compile-cache directory (on the data-dir mount).
    compile_cache_dir: PathBuf,
}

impl DiskUsageTracker {
    /// Create an unseeded tracker. All counters start at zero; call
    /// [`Self::seed`] once before the counts are meaningful.
    pub(crate) fn new(contracts_dir: PathBuf, compile_cache_dir: PathBuf) -> Self {
        Self {
            state_bytes: AtomicU64::new(0),
            wasm_bytes: AtomicU64::new(0),
            compile_cache_bytes: AtomicU64::new(0),
            seeded: AtomicBool::new(false),
            state_sizes: Mutex::new(StateSizeIndex::default()),
            contracts_dir,
            compile_cache_dir,
        }
    }

    /// Whether [`Self::seed`] has already run successfully.
    pub(crate) fn is_seeded(&self) -> bool {
        self.seeded.load(Ordering::Acquire)
    }

    /// Aggregate on-disk bytes = state + wasm + compile-cache. The value the
    /// disk budget bounds. Cheap (three atomic loads).
    ///
    /// Read live by the eviction floor
    /// ([`super::HostingManager::recompute_effective_budget`]) and the pre-write
    /// admission gate ([`Self::admit_state_write`]), both wired in #4702, plus the
    /// telemetry snapshot path.
    pub(crate) fn total_bytes(&self) -> u64 {
        self.state_bytes
            .load(Ordering::Relaxed)
            .saturating_add(self.wasm_bytes.load(Ordering::Relaxed))
            .saturating_add(self.compile_cache_bytes.load(Ordering::Relaxed))
    }

    /// Snapshot all gauges for telemetry.
    pub(crate) fn stats(&self) -> DiskUsageStats {
        // The histogram and ordered size multiset are maintained at each
        // write/removal, so this lock is held only for a fixed-size copy.
        let sizes = self.state_sizes.lock();
        let state_count = sizes.by_contract.len() as u64;
        let state_size_bucket_counts = sizes.bucket_counts;
        let state_size_bucket_bytes = sizes.bucket_bytes;
        let state_bytes = state_size_bucket_bytes
            .iter()
            .copied()
            .fold(0u64, u64::saturating_add);
        let state_max_bytes = sizes.by_size.last_key_value().map_or(0, |(size, _)| *size);
        let state_over_limit_count = sizes.over_limit_count;
        let state_over_limit_bytes = sizes.over_limit_bytes;
        let state_limit_bytes = MAX_STATE_SIZE as u64;
        drop(sizes);

        let wasm_bytes = self.wasm_bytes.load(Ordering::Relaxed);
        let compile_cache_bytes = self.compile_cache_bytes.load(Ordering::Relaxed);
        DiskUsageStats {
            state_bytes,
            state_count,
            state_size_bucket_counts,
            state_size_bucket_bytes,
            state_max_bytes,
            state_over_limit_count,
            state_over_limit_bytes,
            state_limit_bytes,
            wasm_bytes,
            compile_cache_bytes,
            total_bytes: state_bytes
                .saturating_add(wasm_bytes)
                .saturating_add(compile_cache_bytes),
        }
    }

    /// Seed the state-bytes counter and per-key size index from an exact list of
    /// `(contract, state_size)` pairs (the caller reads these from redb rows so
    /// this module stays storage-backend-agnostic and unit-testable). Also runs
    /// the initial WASM + compile-cache `du`-walks.
    ///
    /// Idempotent-guarded: only the FIRST call takes effect; later calls are a
    /// no-op so a racing second sweep tick cannot double-count.
    ///
    /// Fail-loud contract: the caller MUST pass the true on-disk state total. A
    /// silently-too-low seed would let the admission gate admit
    /// overflowing writes — the exact failure the #4561 secrets seed discipline
    /// guards against.
    ///
    /// # Seed/write race (TOCTOU) closure
    ///
    /// The caller snapshots redb rows at some time `T0` while `seeded` is still
    /// false, then calls this. A concurrent [`Self::record_state_write`] whose
    /// bytes land AFTER `T0` is NOT in `state_rows`, but it is NOT dropped
    /// either: `record_state_write` always records its post-write size into
    /// `state_sizes` (even while unseeded), and this seed treats an
    /// already-present key as authoritative — the concurrent write's true size
    /// wins over the (older, possibly absent) redb-snapshot value. The final
    /// `state_bytes` is recomputed from the merged map under the same lock that
    /// serializes writes, so every write is counted exactly once regardless of
    /// whether its redb row made it into the snapshot. This is the lock-based
    /// close of the window flagged in the tracker-only PR's review (would
    /// otherwise be a load-bearing under-count now that the counter is a live
    /// gate input, #4702).
    pub(crate) fn seed<I>(&self, state_rows: I)
    where
        I: IntoIterator<Item = (ContractKey, u64)>,
    {
        // Take the write-serializing lock FIRST, then flip `seeded` under it, so
        // a concurrent `record_state_write` either (a) ran before us and its
        // size is already in `state_sizes` (we preserve it), or (b) blocks on
        // this lock and runs as a delta after we store the aggregate. It can
        // never fall in a gap where it is neither seeded-in nor deltaed-in.
        let mut sizes = self.state_sizes.lock();

        // Only the first seed wins. `compare_exchange` so a concurrent caller
        // that lost the race returns without touching any counter.
        if self
            .seeded
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .is_err()
        {
            return;
        }

        for (key, size) in state_rows {
            // A concurrent post-`T0` write may already have recorded this key's
            // true post-write size while we were unseeded. That value is newer
            // and authoritative — do NOT overwrite it with the stale snapshot.
            // Only rows not already buffered by such a write are inserted.
            if !sizes.by_contract.contains_key(&key) {
                sizes.insert(key, size);
            }
        }
        // Recompute the aggregate from the merged map so buffered concurrent
        // writes are reflected exactly.
        let total = sizes
            .by_contract
            .values()
            .copied()
            .fold(0u64, |acc, v| acc.saturating_add(v));
        self.state_bytes.store(total, Ordering::Relaxed);
        drop(sizes);

        self.wasm_bytes
            .store(du_walk_wasm(&self.contracts_dir), Ordering::Relaxed);
        self.compile_cache_bytes
            .store(du_walk(&self.compile_cache_dir), Ordering::Relaxed);
    }

    /// Apply a state-write at a chokepoint: set `key`'s tracked size to
    /// `new_size` and adjust `state_bytes` by the signed delta against the
    /// previous size for that key (0 if unseen).
    ///
    /// PUT of a new contract → `+new`. UPDATE of an existing one →
    /// `+(new − old)` (shrinking updates subtract). Called from
    /// [`super::HostingManager::record_state_write`] on the infallible
    /// post-write path (`Ring::commit_state_write`), so the counter only moves
    /// after the bytes actually landed.
    ///
    /// # Unseeded writes
    ///
    /// Even before [`Self::seed`] runs, this records `new_size` into
    /// `state_sizes` (but does NOT touch `state_bytes` — the aggregate is
    /// meaningless until seeded). This is what closes the seed/write TOCTOU: a
    /// write that races the seed leaves its true size in the map for the seed to
    /// pick up, instead of being silently dropped and permanently under-counted.
    /// Once seeded, it additionally applies the signed delta to `state_bytes`.
    /// The `state_sizes` lock serializes this against [`Self::seed`], so the
    /// seeded/unseeded branch is decided atomically with respect to the seed.
    pub(crate) fn record_state_write(&self, key: &ContractKey, new_size: u64) {
        let mut sizes = self.state_sizes.lock();
        let old = sizes.insert(*key, new_size).unwrap_or(0);
        // While unseeded, only buffer the size; `state_bytes` is recomputed from
        // the map at seed time, so applying a delta now would be wrong (and the
        // aggregate is not yet meaningful). The map insert above is the record
        // that the seed will honor.
        if !self.seeded.load(Ordering::Acquire) {
            return;
        }
        drop(sizes);
        if new_size >= old {
            self.state_bytes
                .fetch_add(new_size - old, Ordering::Relaxed);
        } else {
            // saturating_sub floors at 0: a delta can never drive the aggregate
            // negative even if the seed under-counted this key.
            let dec = old - new_size;
            let mut cur = self.state_bytes.load(Ordering::Relaxed);
            loop {
                let next = cur.saturating_sub(dec);
                match self.state_bytes.compare_exchange_weak(
                    cur,
                    next,
                    Ordering::Relaxed,
                    Ordering::Relaxed,
                ) {
                    Ok(_) => break,
                    Err(observed) => cur = observed,
                }
            }
        }
    }

    /// Remove `key`'s state contribution on eviction/reclamation: subtract its
    /// last-known size (floored at 0) and forget the key. Idempotent — a second
    /// removal of an already-forgotten key subtracts nothing (floor-at-0).
    ///
    /// While unseeded, only forgets the buffered size (mirrors the unseeded
    /// branch of [`Self::record_state_write`]): `state_bytes` is recomputed from
    /// the map at seed time, so removing the key from the map is sufficient and
    /// applying a delta now would be wrong.
    pub(crate) fn record_state_removed(&self, key: &ContractKey) {
        let mut sizes = self.state_sizes.lock();
        let removed = sizes.remove(key).unwrap_or(0);
        if !self.seeded.load(Ordering::Acquire) {
            return;
        }
        drop(sizes);
        if removed == 0 {
            return;
        }
        let mut cur = self.state_bytes.load(Ordering::Relaxed);
        loop {
            let next = cur.saturating_sub(removed);
            match self.state_bytes.compare_exchange_weak(
                cur,
                next,
                Ordering::Relaxed,
                Ordering::Relaxed,
            ) {
                Ok(_) => break,
                Err(observed) => cur = observed,
            }
        }
    }

    /// Record a newly-written (deduped) WASM code blob on the post-store success
    /// path: add `blob_len` to `wasm_bytes` so the aggregate reflects it
    /// immediately, WITHOUT waiting for the next 60s `refresh_wasm` du-walk
    /// (#4683). This closes two gaps the sweep-window-only accounting left open:
    ///
    /// 1. **Burst overrun:** within a single sweep window a burst of PUTs, each
    ///    carrying a distinct large code blob, would otherwise all see the same
    ///    stale `wasm_bytes` and each pass the gate, letting cumulative wasm
    ///    overrun the budget until the next sweep. Live accounting makes each
    ///    successive blob in the burst visible to the next admission check.
    /// 2. **Per-PUT double-count:** charging the blob here (before the state gate
    ///    runs on the same PUT) makes the state gate's `total_bytes()` include
    ///    the wasm just stored, so a PUT can no longer pass both the wasm gate
    ///    and the state gate independently and overshoot by `min(blob, state)`.
    ///
    /// Called ONLY for a blob that was not already on disk (the caller dedups via
    /// `fetch_contract_code`); a re-PUT of existing code adds nothing. The next
    /// `refresh_wasm` re-walk reconciles the counter against ground truth, so a
    /// missed/double delta self-heals within one sweep. Idempotency is the
    /// caller's responsibility (charge exactly once per newly-written blob).
    ///
    /// Applied even while unseeded: unlike state (whose per-key sizes are
    /// buffered in `state_sizes` and summed at seed time), the seed measures wasm
    /// with a fresh `du_walk_wasm` that already includes any blob written before
    /// the seed. To avoid double-counting a pre-seed blob (once here, once in the
    /// seed walk), skip the delta while unseeded — the seed walk is authoritative.
    pub(crate) fn record_wasm_write(&self, blob_len: u64) {
        if !self.seeded.load(Ordering::Acquire) {
            return;
        }
        self.wasm_bytes.fetch_add(blob_len, Ordering::Relaxed);
    }

    /// Remove a WASM code blob's contribution on contract removal: subtract
    /// `blob_len` from `wasm_bytes` (floored at 0). Mirror of
    /// [`Self::record_wasm_write`]; the next `refresh_wasm` reconciles. No-op
    /// while unseeded (the seed walk is authoritative, same as the write path).
    pub(crate) fn record_wasm_removed(&self, blob_len: u64) {
        if !self.seeded.load(Ordering::Acquire) {
            return;
        }
        if blob_len == 0 {
            return;
        }
        let mut cur = self.wasm_bytes.load(Ordering::Relaxed);
        loop {
            let next = cur.saturating_sub(blob_len);
            match self.wasm_bytes.compare_exchange_weak(
                cur,
                next,
                Ordering::Relaxed,
                Ordering::Relaxed,
            ) {
                Ok(_) => break,
                Err(observed) => cur = observed,
            }
        }
    }

    /// Pre-write admission check for a state write (#4683, live since #4702). Computes the
    /// aggregate on-disk bytes that WOULD result from replacing `key`'s current
    /// tracked state size with `new_size`
    /// (`projected = total − old_for_key + new_size`) and rejects if that
    /// exceeds `budget_bytes`.
    ///
    /// **Read-only.** The `+delta` is NOT applied here — it is deferred to the
    /// post-write success path ([`Self::record_state_write`], invoked from
    /// `Ring::commit_state_write`), mirroring the secrets `quota_commit`
    /// discipline (#4561): a rejected OR later-failed write never mutates the
    /// counter, so it leaves no phantom bytes behind.
    ///
    /// `old_for_key` is read from the same `state_sizes` index the delta path
    /// maintains, so the caller does not have to supply the previous size. A key
    /// not present in the index (fresh PUT) has `old = 0`, so the full `new_size`
    /// is charged.
    ///
    /// The boundary is inclusive-admit: `projected == budget_bytes` is admitted;
    /// only `projected > budget_bytes` rejects.
    pub(crate) fn admit_state_write(
        &self,
        key: &ContractKey,
        new_size: u64,
        budget_bytes: u64,
    ) -> Result<(), DiskBudgetExceeded> {
        // Hold the same lock the delta path takes so `total_bytes()` and the
        // per-key old size are read against a consistent snapshot: a concurrent
        // `record_state_write` cannot land its delta between our `old` read and
        // our `total` read and make the projection inconsistent.
        let sizes = self.state_sizes.lock();
        let old = sizes.get(key).unwrap_or(0);
        let total = self.total_bytes();
        drop(sizes);
        // projected = total − old + new (saturating so it can never wrap).
        let projected = total.saturating_sub(old).saturating_add(new_size);
        if projected > budget_bytes {
            Err(DiskBudgetExceeded {
                projected_bytes: projected,
                budget_bytes,
            })
        } else {
            Ok(())
        }
    }

    /// Pre-write admission check for a state **UPDATE** to an already-hosted
    /// contract (#4683 growth-only rule, live since #4702). Unlike [`Self::admit_state_write`]
    /// (used for a fresh PUT), an UPDATE is a mutation of an *existing*
    /// footprint, not a new admission. A CRDT merge frequently shrinks or holds
    /// the state size (`delta <= 0`); rejecting such a write would stall
    /// convergence without freeing any bytes (the old footprint is already on
    /// disk and counted), and for a relayed UPDATE the rejection is silently
    /// dropped, so nothing would signal the stall to anyone.
    ///
    /// Therefore this check is **growth-only**: when `new_size <= old_for_key`
    /// (the delta is non-positive) it admits unconditionally, even when the
    /// aggregate is already over budget. Only genuine growth (`new_size >
    /// old_for_key`) is subjected to the same projected-aggregate bound as
    /// `admit_state_write`.
    ///
    /// Read-only, same deferred-`+delta` discipline and inclusive-admit boundary
    /// as [`Self::admit_state_write`].
    pub(crate) fn admit_state_update(
        &self,
        key: &ContractKey,
        new_size: u64,
        budget_bytes: u64,
    ) -> Result<(), DiskBudgetExceeded> {
        // Hold the delta-path lock so the `old` read and the `total` read are a
        // consistent snapshot (same rationale as `admit_state_write`).
        let sizes = self.state_sizes.lock();
        let old = sizes.get(key).unwrap_or(0);
        // Non-positive delta (shrink or hold) never blocks convergence.
        if new_size <= old {
            return Ok(());
        }
        let total = self.total_bytes();
        drop(sizes);
        let projected = total.saturating_sub(old).saturating_add(new_size);
        if projected > budget_bytes {
            Err(DiskBudgetExceeded {
                projected_bytes: projected,
                budget_bytes,
            })
        } else {
            Ok(())
        }
    }

    /// Pre-write admission check for a WASM code-blob write (#4683, live since #4702).
    /// Charges `blob_len` on top of current aggregate usage
    /// (`projected = total + blob_len`) and rejects if it exceeds
    /// `budget_bytes`. Used only for a NEWLY-stored (deduped) code blob — a
    /// re-PUT of already-stored code adds nothing on disk and the caller skips
    /// the check for that case. Read-only, same inclusive-admit boundary as
    /// [`Self::admit_state_write`].
    pub(crate) fn admit_wasm_write(
        &self,
        blob_len: u64,
        budget_bytes: u64,
    ) -> Result<(), DiskBudgetExceeded> {
        let projected = self.total_bytes().saturating_add(blob_len);
        if projected > budget_bytes {
            Err(DiskBudgetExceeded {
                projected_bytes: projected,
                budget_bytes,
            })
        } else {
            Ok(())
        }
    }

    /// Re-measure the on-disk WASM blob total by `du`-walking `contracts_dir`.
    /// Cheap and re-run on the telemetry cadence; deduping is inherent (each
    /// distinct `code_hash` is one file).
    pub(crate) fn refresh_wasm(&self) {
        self.wasm_bytes
            .store(du_walk_wasm(&self.contracts_dir), Ordering::Relaxed);
    }

    /// Re-measure the wasmtime compile-cache total by `du`-walking its dir.
    /// Wasmtime writes the cache opaquely, so this re-walk (not a delta) is the
    /// only way to account for it.
    pub(crate) fn refresh_compile_cache(&self) {
        self.compile_cache_bytes
            .store(du_walk(&self.compile_cache_dir), Ordering::Relaxed);
    }

    /// Free bytes on the mount holding the tracked `contracts_dir` — the
    /// `available` term the disk budget sizes against (#4683). `None` when the
    /// platform query fails; the caller falls back to `u64::MAX`. The contracts
    /// dir shares the data-dir mount, which is where all tracked bytes (state,
    /// wasm, relocated compile cache) land, so it is the correct mount to probe.
    pub(crate) fn available_bytes(&self) -> Option<u64> {
        available_bytes(&self.contracts_dir)
    }
}

/// Recursively sum the byte size of every regular file under `dir`. A missing
/// directory (not yet created) or an unreadable entry contributes 0 rather than
/// erroring — the refresh path is best-effort telemetry, unlike the fail-loud
/// state seed.
fn du_walk(dir: &Path) -> u64 {
    let mut total: u64 = 0;
    let mut stack = vec![dir.to_path_buf()];
    while let Some(path) = stack.pop() {
        let Ok(entries) = std::fs::read_dir(&path) else {
            continue;
        };
        for entry in entries.flatten() {
            let Ok(file_type) = entry.file_type() else {
                continue;
            };
            if file_type.is_dir() {
                stack.push(entry.path());
            } else if file_type.is_file() {
                if let Ok(meta) = entry.metadata() {
                    total = total.saturating_add(meta.len());
                }
            }
        }
    }
    total
}

/// Like [`du_walk`] but only counts `*.wasm` files — the code-blob subset of
/// `contracts_dir` (which also holds the `local/` mode split). Directory
/// traversal is recursive so both the network and `local/` blobs are counted.
fn du_walk_wasm(dir: &Path) -> u64 {
    let mut total: u64 = 0;
    let mut stack = vec![dir.to_path_buf()];
    while let Some(path) = stack.pop() {
        let Ok(entries) = std::fs::read_dir(&path) else {
            continue;
        };
        for entry in entries.flatten() {
            let Ok(file_type) = entry.file_type() else {
                continue;
            };
            if file_type.is_dir() {
                stack.push(entry.path());
            } else if file_type.is_file() {
                let p = entry.path();
                if p.extension().and_then(|e| e.to_str()) == Some("wasm") {
                    if let Ok(meta) = entry.metadata() {
                        total = total.saturating_add(meta.len());
                    }
                }
            }
        }
    }
    total
}

/// Free bytes on the filesystem mount that holds `path`, as seen by an
/// unprivileged process (#4683). This is the `available` term the disk budget
/// adds to `freenet_used` to size itself against total reachable capacity.
///
/// Returns `None` when the platform query fails or the platform is unsupported;
/// the caller then falls back to `u64::MAX`, which makes `disk_budget_for` clamp
/// to its MAX cap (a free-space read that cannot be trusted must not silently
/// shrink the budget — the eviction floor degrades to "cap only" rather than to
/// zero). The value is `f_bavail * f_frsize` (blocks available to non-root ×
/// fragment size) — NOT `f_bfree`, so root-reserved blocks are excluded, matching
/// what the node can actually write.
///
/// The `path` must live on the SAME mount as the data dir the budget accounts
/// for: `statvfs` is per-mount, so the free-space basis has to be measured on
/// the mount the tracked bytes land on (this is why PR 1 relocated the wasmtime
/// cache onto the data-dir mount).
pub fn available_bytes(path: &Path) -> Option<u64> {
    #[cfg(unix)]
    {
        use std::ffi::CString;
        use std::os::unix::ffi::OsStrExt;

        let c_path = CString::new(path.as_os_str().as_bytes()).ok()?;
        // SAFETY: `statvfs` is an FFI call that reads the filesystem stats for a
        // NUL-terminated path into a caller-owned `libc::statvfs` buffer. We pass
        // a valid `CString` pointer (owned by `c_path`, alive for the call) and a
        // zeroed, correctly-sized, stack-owned out-buffer. It writes only into
        // that buffer and returns 0 on success / -1 on error (checked below); it
        // borrows no memory past the call. No aliasing or lifetime hazards.
        let stat = unsafe {
            let mut stat: libc::statvfs = std::mem::zeroed();
            if libc::statvfs(c_path.as_ptr(), &mut stat) != 0 {
                return None;
            }
            stat
        };
        // `f_bavail`/`f_frsize` are `c_ulong`/`fsblkcnt_t`; widen to u64 and
        // saturate the multiply so a pathological fs report can't overflow.
        let avail = stat.f_bavail as u64;
        let frsize = stat.f_frsize as u64;
        Some(avail.saturating_mul(frsize))
    }
    #[cfg(all(windows, not(unix)))]
    {
        use std::os::windows::ffi::OsStrExt;
        // GetDiskFreeSpaceExW wants a wide, NUL-terminated path.
        let mut wide: Vec<u16> = path.as_os_str().encode_wide().collect();
        wide.push(0);
        let mut free_bytes_available: u64 = 0;
        // SAFETY: FFI call into the Win32 API. `wide` is a valid NUL-terminated
        // UTF-16 buffer alive for the call; the three out-pointers are to
        // stack-owned u64s. We read only `free_bytes_available` and only on a
        // nonzero (success) return.
        let ok = unsafe {
            winapi::um::fileapi::GetDiskFreeSpaceExW(
                wide.as_ptr(),
                &mut free_bytes_available as *mut u64 as *mut _,
                std::ptr::null_mut(),
                std::ptr::null_mut(),
            )
        };
        if ok == 0 {
            None
        } else {
            Some(free_bytes_available)
        }
    }
    #[cfg(not(any(unix, windows)))]
    {
        let _ = path;
        None
    }
}

/// Bytes Freenet already occupies on the data-dir mount, measured WITHOUT a
/// [`DiskUsageTracker`] (#5014). Sums the same two `du`-measured terms the
/// tracker maintains — `*.wasm` code blobs under `contracts_dir` and the
/// relocated wasmtime compile cache — using the same walkers, so the two agree
/// on what "Freenet's own bytes" means.
///
/// The tracker's THIRD term, persisted contract-state bytes, is deliberately
/// omitted: it comes from redb rows, which are not readable before the storage
/// layer exists. Omitting it can only make the measured total SMALLER, and
/// [`startup_disk_budget_estimate`] is monotone non-decreasing in this value, so
/// the omission can only make the derived budget smaller — the conservative
/// direction for a bound whose failure mode is over-allocation.
///
/// # How much the omission costs, stated honestly
///
/// It is NOT immaterial, and it bites hardest on exactly the hosts the disk term
/// exists for. Worked example: a 16 GiB VM holding 1 GiB of contract state,
/// 50 MiB of wasm, a 50 MiB cache and 200 MiB free.
///
/// * Live: `used = 1024 + 50 + 50 = 1124 MiB`, `available = 200 MiB`, so at
///   pct = 0.5 the budget is ~662 MiB and a live-consistent disk term would be
///   ~165 MiB.
/// * Estimate: `used = 50 + 50 = 100 MiB`, so the basis is 300 MiB, the budget
///   150 MiB, and the resolved limit ~37 MiB — about 4.5x under.
///
/// The error is always in the safe direction (the cache is bounded more tightly
/// than the live budget licenses, never less), and the gap widens without bound
/// as `--max-hosting-storage` rises. But "the bound lands where the live budget
/// would put it" is false on a state-heavy, disk-tight host, and that is the
/// shape most likely to hit it, so do not restate this omission as immaterial.
///
/// The reason is the redb *row* total, not the database *file*: the file's size
/// is a plain `metadata()` read and needs no storage layer. Adding it is a real
/// improvement and is deliberately left to the follow-up that lands
/// `du_walk_shallow` over `db_dir` (#5033), which is where that walker comes
/// from. Until then the estimate is documented as a lower bound, not as an
/// approximation of the live budget.
pub(crate) fn measure_startup_disk_used(contracts_dir: &Path, compile_cache_dir: &Path) -> u64 {
    du_walk_wasm(contracts_dir).saturating_add(du_walk(compile_cache_dir))
}

/// Pure clamp math behind [`startup_disk_budget_estimate`] (#5014), with the
/// filesystem reads lifted out as parameters — the same determinism seam
/// [`super::HostingManager::recompute_effective_budget`] uses for `available`.
///
/// `available: None` means the platform free-space query failed, and falls back
/// to `u64::MAX` exactly as the live recompute does: a free-space read that
/// cannot be trusted must not silently shrink a budget (the result then clamps
/// to `max_hosting_disk`).
///
/// Delegates to [`disk_budget_for_clamped`](super::cache::disk_budget_for_clamped)
/// with the same MIN floor and operator cap as the live budget, so the start-time
/// estimate and the live value have exactly one implementation of the math.
pub(crate) fn startup_disk_budget_from_measurements(
    measured_used: u64,
    available: Option<u64>,
    pct: f64,
    max_hosting_disk: u64,
) -> u64 {
    super::cache::disk_budget_for_clamped(
        measured_used,
        available.unwrap_or(u64::MAX),
        pct,
        super::cache::MIN_DEFAULT_HOSTING_BUDGET_BYTES,
        max_hosting_disk,
    )
}

/// Start-time estimate of the aggregate disk budget, for consumers that must
/// size themselves BEFORE the [`DiskUsageTracker`] is configured and seeded
/// (#5014).
///
/// # Why this exists
///
/// The wasmtime on-disk compile cache is charged against the aggregate disk
/// budget ([`DiskUsageTracker::total_bytes`] sums state + wasm + compile-cache
/// bytes, and that total gates `admit_state_write` / `admit_wasm_write`), but
/// wasmtime fixes the cache's soft limit once, at `Cache::new` — inside
/// `Executor::from_config_with_shared_modules`, which runs before the tracker
/// exists. A limit derived from RAM needs nothing from disk and so left a
/// disk-tight, RAM-rich host completely unprotected. This gives that call site
/// the disk signal it needs at the only moment it can use it.
///
/// # It is an estimate, and it errs low
///
/// It reuses the live budget's own math ([`startup_disk_budget_from_measurements`])
/// on the same basis — `freenet_used + available` — but its `freenet_used` term
/// omits persisted contract state (see [`measure_startup_disk_used`]). Since
/// `disk_budget_for_clamped` is monotone non-decreasing in `freenet_used`, the
/// estimate is always `<=` the budget the first 60s recompute will install.
///
/// # Why it does NOT feed back on itself
///
/// The basis is `freenet_used + available`, not free space. Every byte the
/// compile cache writes moves one byte from `available` into `freenet_used` on
/// the SAME mount, so the sum is the mount's Freenet-reachable capacity and is
/// invariant under the cache's own size. That is why `measure_startup_disk_used`
/// must include the compile-cache walk: an estimate built on bare free space
/// would shrink when the cache is full and grow when it is empty, so each
/// restart would re-derive a different limit and the value would oscillate.
pub(crate) fn startup_disk_budget_estimate(
    contracts_dir: &Path,
    compile_cache_dir: &Path,
    pct: f64,
    max_hosting_disk: u64,
) -> u64 {
    startup_disk_budget_from_measurements(
        measure_startup_disk_used(contracts_dir, compile_cache_dir),
        available_bytes(contracts_dir),
        pct,
        max_hosting_disk,
    )
}

/// Percentage of the soft limit the startup prune deletes down to, matching
/// wasmtime's own `files_total_size_limit_percent_if_deleting` default of 70
/// (`wasmtime-internal-cache/src/config.rs`, applied in `worker.rs`).
///
/// Landing on the same target means the restart prune leaves the tree exactly
/// where wasmtime's own hourly cleanup would have left it, so the two mechanisms
/// agree on the steady state instead of fighting over it.
const COMPILE_CACHE_PRUNE_TARGET_PCT: u64 = 70;

/// Bring an already-oversized wasmtime compile cache under `soft_limit` at
/// startup by deleting its least-recently-modified files (#5014). Returns the
/// number of bytes reclaimed (0 when the tree already fits).
///
/// # Why this is needed at all
///
/// Lowering the soft limit does not shrink an existing cache. Wasmtime reads the
/// limit in exactly one place — its cleanup pass — and that pass is reachable
/// ONLY from the cache-*write* path (`handle_on_cache_update`), further gated by
/// a once-per-`cleanup_interval` (1h default) filesystem lock. The cache-*hit*
/// path never cleans up. So a node whose contract-blob set is stable performs
/// only cache GETs after a restart and its pre-fix, oversized cache persists
/// indefinitely.
///
/// That is precisely the node #5014 describes, and the loop is self-sustaining:
/// its oversized cache pushes `total_bytes()` past the budget, every
/// `admit_state_write` / `admit_wasm_write` rejects, so it cannot take on the new
/// contracts whose compiles would produce the cache misses that would trigger a
/// cleanup. Without this prune the fix bounds the cache on future starts and
/// leaves an already-wedged node wedged.
///
/// # Why deleting these files is safe
///
/// The compile cache is a pure derived artifact — every entry is regenerable by
/// recompiling the blob it was built from, and a missing entry is just a cache
/// miss.
///
/// - It runs at startup, before the first `Cache::new`, so wasmtime has not
///   opened anything under this tree.
/// - Wasmtime reads entries with `fs::read` into a `Vec`, never `mmap`, so no
///   live mapping can be invalidated even if the timing assumption above were
///   ever violated.
/// - Wasmtime `create_dir_all`s the tree again on its next write, so removing
///   files (or the whole tree) is self-healing rather than a permanent break.
/// - Wasmtime's own worker already does `fs::remove_file` / `remove_dir_all`
///   against this same tree; this is the same operation on the same cadence
///   boundary, not a new kind of access.
///
/// # Oldest-first, not wipe-everything
///
/// A full wipe is equally safe but strictly worse: every restart under a tight
/// budget would pay a whole recompile wave. Deleting by ascending mtime down to
/// [`COMPILE_CACHE_PRUNE_TARGET_PCT`] of the limit keeps the hot artifacts and
/// matches what wasmtime's cleanup would itself have done. Files whose mtime is
/// unreadable sort oldest (deleted first) — an entry we cannot even stat is the
/// least trustworthy thing in the tree — and the path is a deterministic
/// tiebreak so the outcome does not depend on directory iteration order.
///
/// Best-effort throughout: a file that fails to delete is skipped and not
/// counted as reclaimed, exactly like the `du` walks that measure this tree.
pub(crate) fn prune_compile_cache_to_limit(compile_cache_dir: &Path, soft_limit: u64) -> u64 {
    // Trigger on [`du_walk`] specifically — the same function
    // [`DiskUsageTracker::refresh_compile_cache`] uses to charge this tree to the
    // budget — so the prune fires on exactly the quantity the budget counts,
    // rather than on a second notion of the cache's size.
    let total = du_walk(compile_cache_dir);
    if total <= soft_limit {
        return 0;
    }
    let target = soft_limit
        .saturating_mul(COMPILE_CACHE_PRUNE_TARGET_PCT)
        .saturating_div(100);

    // (mtime, path, len), sorted oldest first with the path as a deterministic
    // tiebreak. Collected in full before any deletion so the walk cannot observe
    // its own effects.
    let mut entries: Vec<(std::time::SystemTime, PathBuf, u64)> = Vec::new();
    let mut stack = vec![compile_cache_dir.to_path_buf()];
    while let Some(path) = stack.pop() {
        let Ok(dir_entries) = std::fs::read_dir(&path) else {
            continue;
        };
        for entry in dir_entries.flatten() {
            let Ok(file_type) = entry.file_type() else {
                continue;
            };
            if file_type.is_dir() {
                stack.push(entry.path());
            } else if file_type.is_file() {
                let Ok(meta) = entry.metadata() else {
                    continue;
                };
                let mtime = meta.modified().unwrap_or(std::time::UNIX_EPOCH);
                entries.push((mtime, entry.path(), meta.len()));
            }
        }
    }
    entries.sort_by(|a, b| a.0.cmp(&b.0).then_with(|| a.1.cmp(&b.1)));

    let mut remaining = total;
    let mut reclaimed = 0u64;
    for (_, path, len) in entries {
        if remaining <= target {
            break;
        }
        if std::fs::remove_file(&path).is_ok() {
            remaining = remaining.saturating_sub(len);
            reclaimed = reclaimed.saturating_add(len);
        }
    }
    reclaimed
}

#[cfg(test)]
mod tests {
    use super::*;
    use freenet_stdlib::prelude::{CodeHash, ContractInstanceId, ContractKey};
    use std::io::Write;

    fn test_key(seed: u8) -> ContractKey {
        let instance = ContractInstanceId::new([seed; 32]);
        let code = CodeHash::new([seed; 32]);
        ContractKey::from_id_and_code(instance, code)
    }

    fn tracker() -> DiskUsageTracker {
        // Nonexistent dirs → du-walks contribute 0, isolating state-delta math.
        DiskUsageTracker::new(
            PathBuf::from("/nonexistent/contracts"),
            PathBuf::from("/nonexistent/cache"),
        )
    }

    #[test]
    fn seed_sums_state_rows_and_is_idempotent() {
        let t = tracker();
        assert!(!t.is_seeded());
        t.seed([(test_key(1), 100), (test_key(2), 50)]);
        assert!(t.is_seeded());
        assert_eq!(t.stats().state_bytes, 150);
        // A second seed must be a no-op (no double-count).
        t.seed([(test_key(3), 999)]);
        assert_eq!(t.stats().state_bytes, 150);
    }

    #[test]
    fn persisted_state_inventory_is_exact_at_every_bucket_boundary() {
        let t = tracker();
        let bounds = crate::tracing::event_kind::STATE_SIZE_BUCKET_UPPER_BOUNDS;
        let mut rows = Vec::new();
        let mut expected_counts = [0u64; STATE_SIZE_BUCKET_COUNT];
        let mut expected_bytes = [0u64; STATE_SIZE_BUCKET_COUNT];

        // Put one state exactly at each inclusive boundary, then one byte above
        // the hard limit to exercise the final bucket and invariant counter.
        for (index, size) in bounds.into_iter().enumerate() {
            rows.push((test_key(index as u8), size));
            expected_counts[index] += 1;
            expected_bytes[index] += size;
        }
        let over_limit = MAX_STATE_SIZE as u64 + 1;
        rows.push((test_key(250), over_limit));
        expected_counts[STATE_SIZE_BUCKET_COUNT - 1] = 1;
        expected_bytes[STATE_SIZE_BUCKET_COUNT - 1] = over_limit;

        t.seed(rows);
        let stats = t.stats();
        assert_eq!(stats.state_count, STATE_SIZE_BUCKET_COUNT as u64);
        assert_eq!(stats.state_size_bucket_counts, expected_counts);
        assert_eq!(stats.state_size_bucket_bytes, expected_bytes);
        assert_eq!(stats.state_max_bytes, over_limit);
        assert_eq!(stats.state_over_limit_count, 1);
        assert_eq!(stats.state_over_limit_bytes, over_limit);
        assert_eq!(stats.state_limit_bytes, MAX_STATE_SIZE as u64);
        assert_eq!(
            stats.state_size_bucket_counts.iter().sum::<u64>(),
            stats.state_count
        );
        assert_eq!(
            stats.state_size_bucket_bytes.iter().sum::<u64>(),
            stats.state_bytes
        );
    }

    #[test]
    fn persisted_state_inventory_tracks_bucket_crossing_and_removal() {
        let t = tracker();
        let key = test_key(1);
        t.seed([(key, 64 * 1024)]);

        t.record_state_write(&key, 64 * 1024 + 1);
        let moved = t.stats();
        assert_eq!(moved.state_size_bucket_counts[0], 0);
        assert_eq!(moved.state_size_bucket_counts[1], 1);
        assert_eq!(moved.state_size_bucket_bytes[1], 64 * 1024 + 1);
        assert_eq!(moved.state_max_bytes, 64 * 1024 + 1);

        t.record_state_removed(&key);
        let empty = t.stats();
        assert_eq!(empty.state_count, 0);
        assert_eq!(empty.state_size_bucket_counts, [0; STATE_SIZE_BUCKET_COUNT]);
        assert_eq!(empty.state_size_bucket_bytes, [0; STATE_SIZE_BUCKET_COUNT]);
        assert_eq!(empty.state_max_bytes, 0);
        assert_eq!(empty.state_over_limit_count, 0);
        assert_eq!(empty.state_over_limit_bytes, 0);
    }

    #[test]
    fn put_new_contract_adds_full_size() {
        let t = tracker();
        t.seed(std::iter::empty());
        t.record_state_write(&test_key(1), 200);
        assert_eq!(t.stats().state_bytes, 200);
        assert_eq!(t.total_bytes(), 200);
    }

    #[test]
    fn update_grow_adds_delta() {
        let t = tracker();
        t.seed([(test_key(1), 100)]);
        t.record_state_write(&test_key(1), 250);
        assert_eq!(t.stats().state_bytes, 250); // +150 delta, not +250
    }

    #[test]
    fn update_shrink_subtracts_delta() {
        let t = tracker();
        t.seed([(test_key(1), 300)]);
        t.record_state_write(&test_key(1), 100);
        assert_eq!(t.stats().state_bytes, 100); // -200 delta
    }

    #[test]
    fn evict_removes_full_size() {
        let t = tracker();
        t.seed([(test_key(1), 100), (test_key(2), 40)]);
        t.record_state_removed(&test_key(1));
        assert_eq!(t.stats().state_bytes, 40);
    }

    #[test]
    fn double_evict_floors_at_zero() {
        let t = tracker();
        t.seed([(test_key(1), 100)]);
        t.record_state_removed(&test_key(1));
        // Second removal of the same key subtracts nothing (key forgotten).
        t.record_state_removed(&test_key(1));
        assert_eq!(t.stats().state_bytes, 0);
    }

    #[test]
    fn removal_never_drives_aggregate_negative() {
        let t = tracker();
        // Seed under-counts key(1) (size 10) but the true write was larger.
        t.seed([(test_key(1), 10)]);
        // A shrink whose "old" (10) exceeds current would floor at 0, not wrap.
        t.record_state_write(&test_key(2), 5); // total now 15
        t.record_state_removed(&test_key(2)); // -5 -> 10
        t.record_state_removed(&test_key(1)); // -10 -> 0
        assert_eq!(t.stats().state_bytes, 0);
    }

    #[test]
    fn wasm_dedup_rewalk_counts_each_blob_once() {
        let dir = tempfile::tempdir().unwrap();
        let contracts = dir.path().join("contracts");
        std::fs::create_dir_all(contracts.join("local")).unwrap();
        // Two distinct blobs + a non-wasm file that must NOT be counted.
        let mut a = std::fs::File::create(contracts.join("aaaa.wasm")).unwrap();
        a.write_all(&[0u8; 100]).unwrap();
        let mut b = std::fs::File::create(contracts.join("local").join("bbbb.wasm")).unwrap();
        b.write_all(&[0u8; 40]).unwrap();
        let mut junk = std::fs::File::create(contracts.join("index.db")).unwrap();
        junk.write_all(&[0u8; 1000]).unwrap();

        let t = DiskUsageTracker::new(contracts.clone(), dir.path().join("cache"));
        t.seed(std::iter::empty());
        assert_eq!(t.stats().wasm_bytes, 140);
        // Re-walking is deduped by construction (same files) — idempotent.
        t.refresh_wasm();
        assert_eq!(t.stats().wasm_bytes, 140);
    }

    #[test]
    fn unseeded_write_is_buffered_and_survives_seed() {
        // A write that lands while the tracker is unseeded (its redb row not yet
        // in the snapshot the caller will pass to `seed`) must NOT be dropped:
        // its true size is buffered and the seed reconciles to it, rather than
        // permanently under-counting the key. Regression for the PR-1 seed/write
        // TOCTOU review finding.
        let t = tracker();
        // Write arrives before seed; aggregate not yet meaningful.
        t.record_state_write(&test_key(1), 200);
        assert!(!t.is_seeded());
        // Seed with a DIFFERENT key only (the racing write's redb row was not in
        // the snapshot). The buffered write must still be counted.
        t.seed([(test_key(2), 50)]);
        assert_eq!(t.stats().state_bytes, 250);
        assert_eq!(t.total_bytes(), 250);
    }

    #[test]
    fn seed_prefers_concurrent_write_size_over_stale_snapshot() {
        // If the same key appears both as a buffered post-seed write AND in the
        // redb snapshot, the newer write size (not the stale snapshot) wins, and
        // it is counted exactly once (no double-add).
        let t = tracker();
        t.record_state_write(&test_key(1), 300); // newer, post-snapshot size
        t.seed([(test_key(1), 100)]); // stale snapshot value for same key
        assert_eq!(t.stats().state_bytes, 300);
    }

    #[test]
    fn racing_write_against_seed_yields_exact_total() {
        use std::sync::Arc;
        use std::sync::atomic::{AtomicBool, Ordering as O};
        // Stress the seed/write race across threads: a writer thread hammers
        // `record_state_write` for a key NOT in the seed snapshot while the main
        // thread seeds. Regardless of interleaving, the final aggregate must
        // equal the true on-disk total (seed rows + the racing key's final size),
        // never under-count. Run many iterations to shake out orderings.
        for _ in 0..200 {
            let t = Arc::new(tracker());
            let go = Arc::new(AtomicBool::new(false));
            let writer = {
                let t = Arc::clone(&t);
                let go = Arc::clone(&go);
                std::thread::spawn(move || {
                    while !go.load(O::Acquire) {
                        std::hint::spin_loop();
                    }
                    // Final size for the racing key is 500.
                    t.record_state_write(&test_key(9), 100);
                    t.record_state_write(&test_key(9), 500);
                })
            };
            go.store(true, O::Release);
            // Seed with two unrelated keys totalling 150.
            t.seed([(test_key(1), 100), (test_key(2), 50)]);
            writer.join().unwrap();
            // True total = 150 (seeded) + 500 (racing key's final size). The
            // racing key must be present exactly once at its final size.
            assert_eq!(
                t.stats().state_bytes,
                650,
                "seed/write race under-counted or double-counted"
            );
        }
    }

    // --- Admission gate boundary (#4683, live since #4702) --------------------

    #[test]
    fn admit_state_write_boundary_projected_equals_budget_admits() {
        let t = tracker();
        // Seed 100 bytes used across one key.
        t.seed([(test_key(1), 100)]);
        // Fresh PUT of key(2): projected = 100 (old for key(2)=0) + 100 = 200.
        // Budget exactly 200 → projected == budget → ADMIT (inclusive).
        assert!(t.admit_state_write(&test_key(2), 100, 200).is_ok());
    }

    #[test]
    fn admit_state_write_boundary_projected_over_budget_rejects() {
        let t = tracker();
        t.seed([(test_key(1), 100)]);
        // projected = 100 + 100 = 200; budget 199 → 200 > 199 → REJECT.
        let err = t
            .admit_state_write(&test_key(2), 100, 199)
            .expect_err("over budget must reject");
        assert_eq!(err.projected_bytes, 200);
        assert_eq!(err.budget_bytes, 199);
    }

    #[test]
    fn admit_state_write_uses_old_size_for_existing_key() {
        let t = tracker();
        // key(1) already holds 100 bytes (total = 100).
        t.seed([(test_key(1), 100)]);
        // UPDATE key(1) to 150: projected = 100 − 100 + 150 = 150. Budget 150 →
        // admit; the OLD size is subtracted so an update is charged only its
        // delta, not its full new size.
        assert!(t.admit_state_write(&test_key(1), 150, 150).is_ok());
        // Same update against budget 149 → 150 > 149 → reject.
        assert!(t.admit_state_write(&test_key(1), 150, 149).is_err());
    }

    #[test]
    fn admit_state_write_shrink_always_within_budget() {
        let t = tracker();
        t.seed([(test_key(1), 300)]);
        // Shrinking update: projected = 300 − 300 + 50 = 50, well under a tiny
        // budget. A shrink can never be rejected (delta <= 0).
        assert!(t.admit_state_write(&test_key(1), 50, 60).is_ok());
    }

    #[test]
    fn admit_wasm_write_boundary() {
        let t = tracker();
        t.seed([(test_key(1), 100)]);
        // Charging a 100-byte blob: projected = 100 + 100 = 200.
        assert!(t.admit_wasm_write(100, 200).is_ok()); // == budget admits
        let err = t
            .admit_wasm_write(100, 199)
            .expect_err("over budget rejects");
        assert_eq!(err.projected_bytes, 200);
        assert_eq!(err.budget_bytes, 199);
    }

    #[test]
    fn admit_is_read_only_no_mutation() {
        let t = tracker();
        t.seed([(test_key(1), 100)]);
        // A rejected admit must not move the counter (deferred-delta discipline:
        // the +delta is applied later by record_state_write on success only).
        assert!(t.admit_state_write(&test_key(2), 1000, 200).is_err());
        assert_eq!(
            t.stats().state_bytes,
            100,
            "admit must not mutate state_bytes"
        );
        // An accepted admit is likewise read-only.
        assert!(t.admit_state_write(&test_key(2), 50, 1000).is_ok());
        assert_eq!(
            t.stats().state_bytes,
            100,
            "admit must not mutate state_bytes"
        );
    }

    // --- UPDATE growth-only admission (#4683, finding #1) ---------------------

    #[test]
    fn admit_state_update_shrink_over_budget_still_admits() {
        // The core convergence-safety property: when the aggregate is ALREADY
        // over budget, a shrinking (delta<0) UPDATE must still admit — rejecting
        // it would stall CRDT convergence without freeing any bytes, and a
        // relayed UPDATE rejection is silently dropped so nothing would signal
        // the stall. Concretely: total=300, budget=250 (over budget), key old=100
        // → new=60 projects to 260 > 250, which the HARD gate would reject, but
        // the growth-only UPDATE gate admits because delta = 60 − 100 <= 0.
        let t = tracker();
        t.seed([(test_key(1), 100), (test_key(2), 200)]);
        assert_eq!(t.total_bytes(), 300);
        // Hard gate would reject (projected 260 > 250)...
        assert!(t.admit_state_write(&test_key(1), 60, 250).is_err());
        // ...but the growth-only UPDATE gate admits the shrink.
        assert!(t.admit_state_update(&test_key(1), 60, 250).is_ok());
    }

    #[test]
    fn admit_state_update_hold_over_budget_still_admits() {
        // A size-holding UPDATE (delta == 0) is also non-positive → always admit,
        // even over budget.
        let t = tracker();
        t.seed([(test_key(1), 100), (test_key(2), 200)]);
        assert!(t.admit_state_update(&test_key(1), 100, 250).is_ok());
    }

    #[test]
    fn admit_state_update_growth_is_bounded() {
        // Genuine growth (delta > 0) is still subject to the aggregate bound.
        let t = tracker();
        t.seed([(test_key(1), 100)]);
        // Grow key(1) 100 → 150: projected = 100 − 100 + 150 = 150.
        assert!(t.admit_state_update(&test_key(1), 150, 150).is_ok()); // == budget
        let err = t
            .admit_state_update(&test_key(1), 150, 149)
            .expect_err("growth over budget must reject");
        assert_eq!(err.projected_bytes, 150);
        assert_eq!(err.budget_bytes, 149);
    }

    #[test]
    fn admit_state_update_is_read_only() {
        // Like admit_state_write, the growth-only check never mutates the counter.
        let t = tracker();
        t.seed([(test_key(1), 100)]);
        assert!(t.admit_state_update(&test_key(1), 50, 250).is_ok());
        assert_eq!(t.stats().state_bytes, 100);
        assert!(t.admit_state_update(&test_key(1), 5000, 200).is_err());
        assert_eq!(t.stats().state_bytes, 100);
    }

    // --- Live wasm delta accounting (#4683, finding #3) -----------------------

    #[test]
    fn record_wasm_write_makes_burst_visible_within_window() {
        // Without live accounting, a burst of distinct-code PUTs within one
        // du-walk window would all see the same stale wasm total and each pass
        // the gate, overrunning the budget. Live charging makes each blob visible
        // to the next admission check.
        let t = tracker();
        t.seed(std::iter::empty()); // 0 bytes on disk, seeded.
        let budget = 250u64;
        // First 100-byte blob: projected 0 + 100 = 100 <= 250 → admit, then charge.
        assert!(t.admit_wasm_write(100, budget).is_ok());
        t.record_wasm_write(100);
        // Second blob now sees 100 already charged: projected 100 + 100 = 200 → admit.
        assert!(t.admit_wasm_write(100, budget).is_ok());
        t.record_wasm_write(100);
        assert_eq!(t.stats().wasm_bytes, 200);
        // Third blob would overrun (200 + 100 = 300 > 250) → correctly rejected,
        // whereas stale accounting would have let it through.
        assert!(t.admit_wasm_write(100, budget).is_err());
    }

    #[test]
    fn record_wasm_write_prevents_per_put_double_count() {
        // On one PUT the wasm gate and the state gate each check independently
        // against the aggregate. Charging the wasm blob before the state gate
        // makes the state gate see it, so a PUT whose blob AND state individually
        // fit but jointly overshoot is caught. total=0, budget=250, blob=150,
        // state=150: each fits alone (150<=250) but 150+150=300 overshoots.
        let t = tracker();
        t.seed(std::iter::empty());
        let budget = 250u64;
        assert!(t.admit_wasm_write(150, budget).is_ok());
        t.record_wasm_write(150); // charge before the state gate, like the PUT path.
        // Now the state gate sees wasm=150: projected = 150 − 0 + 150 = 300 > 250.
        assert!(t.admit_state_write(&test_key(1), 150, budget).is_err());
    }

    #[test]
    fn record_wasm_removed_reverses_charge() {
        let t = tracker();
        t.seed(std::iter::empty());
        t.record_wasm_write(200);
        assert_eq!(t.stats().wasm_bytes, 200);
        t.record_wasm_removed(200);
        assert_eq!(t.stats().wasm_bytes, 0);
        // Floors at zero (over-removal cannot underflow).
        t.record_wasm_removed(500);
        assert_eq!(t.stats().wasm_bytes, 0);
    }

    #[test]
    fn record_wasm_write_noop_while_unseeded() {
        // Before seeding, the seed du-walk is authoritative for wasm; a pre-seed
        // delta would double-count against the walk. So charging while unseeded
        // must be a no-op.
        let t = tracker();
        t.record_wasm_write(100);
        assert!(!t.is_seeded());
        assert_eq!(t.stats().wasm_bytes, 0);
    }

    #[test]
    fn compile_cache_du_walk_seeds_and_refreshes() {
        let dir = tempfile::tempdir().unwrap();
        let cache = dir.path().join("cache");
        std::fs::create_dir_all(cache.join("sub")).unwrap();
        let mut f = std::fs::File::create(cache.join("sub").join("mod.cache")).unwrap();
        f.write_all(&[0u8; 512]).unwrap();

        let t = DiskUsageTracker::new(dir.path().join("contracts"), cache.clone());
        t.seed(std::iter::empty());
        assert_eq!(t.stats().compile_cache_bytes, 512);

        // Grow the cache; a refresh must observe the new total.
        let mut g = std::fs::File::create(cache.join("mod2.cache")).unwrap();
        g.write_all(&[0u8; 88]).unwrap();
        t.refresh_compile_cache();
        assert_eq!(t.stats().compile_cache_bytes, 600);
        assert_eq!(t.total_bytes(), 600);
    }

    /// `available_bytes` on a real, existing mount returns a plausible positive
    /// value; a nonexistent path returns `None` (the MAX-fallback case). We can't
    /// assert an exact figure (it's the live disk), only the shape.
    #[cfg(any(unix, windows))]
    #[test]
    fn available_bytes_reports_free_space_or_none() {
        let dir = tempfile::tempdir().unwrap();
        let avail = available_bytes(dir.path());
        assert!(
            avail.map(|v| v > 0).unwrap_or(true),
            "existing mount should report positive free space (or None if the \
             query is unsupported), got {avail:?}"
        );
        // A path that does not exist has no mount stats → None on unix (statvfs
        // fails); the caller treats None as u64::MAX.
        let missing = available_bytes(Path::new("/nonexistent/does/not/exist/xyz"));
        #[cfg(unix)]
        assert!(
            missing.is_none(),
            "statvfs on a nonexistent path should fail → None, got {missing:?}"
        );
        #[cfg(not(unix))]
        let _ = missing;
    }
}

/// Tests for the start-time disk-budget estimate (#5014) — the signal the
/// wasmtime compile-cache soft limit is sized from, resolved before the tracker
/// exists.
#[cfg(test)]
mod startup_estimate_tests {
    use super::{
        du_walk, measure_startup_disk_used, prune_compile_cache_to_limit,
        startup_disk_budget_estimate, startup_disk_budget_from_measurements,
    };
    use std::io::Write;
    use std::path::Path;

    const MIB: u64 = 1024 * 1024;
    const GIB: u64 = 1024 * MIB;
    /// `DEFAULT_MAX_HOSTING_DISK_BYTES`, restated so these tests pin the value
    /// the production caller passes rather than tracking a constant edit.
    const CAP: u64 = 32 * GIB;

    fn write_file(path: &Path, len: usize) {
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent).unwrap();
        }
        let mut f = std::fs::File::create(path).unwrap();
        f.write_all(&vec![0u8; len]).unwrap();
    }

    /// The measured-used term counts exactly the two things the live tracker
    /// `du`-measures: `*.wasm` blobs under the contracts dir (recursively, so
    /// the `local/` split counts) and EVERY file under the compile-cache dir
    /// (wasmtime writes `.stats` sidecars beside its artifacts).
    #[test]
    fn measured_used_counts_wasm_blobs_and_the_whole_compile_cache() {
        let dir = tempfile::tempdir().unwrap();
        let contracts = dir.path().join("contracts");
        let cache = dir.path().join("wasmtime-cache");
        std::fs::create_dir_all(&contracts).unwrap();
        std::fs::create_dir_all(&cache).unwrap();

        write_file(&contracts.join("a.wasm"), 1000);
        write_file(&contracts.join("local").join("b.wasm"), 2000);
        // Non-wasm files in the contracts dir are NOT code blobs and must not
        // be charged (the tracker's `du_walk_wasm` skips them).
        write_file(&contracts.join("index.db"), 9_000);
        // The compile cache is walked in full, sidecars included.
        write_file(&cache.join("sub").join("artifact"), 4000);
        write_file(&cache.join("sub").join("artifact.stats"), 8);

        assert_eq!(
            measure_startup_disk_used(&contracts, &cache),
            1000 + 2000 + 4000 + 8,
            "must count wasm blobs (recursively) + the entire compile-cache tree, \
             and nothing else"
        );
    }

    /// Missing directories contribute 0 rather than erroring — a first-ever
    /// start has neither dir populated, and the estimate must still resolve.
    #[test]
    fn measured_used_is_zero_when_nothing_exists_yet() {
        assert_eq!(
            measure_startup_disk_used(
                Path::new("/nonexistent/contracts"),
                Path::new("/nonexistent/cache"),
            ),
            0
        );
    }

    /// THE anti-feedback property (#5014): the estimate must not move when
    /// bytes shift between the compile cache and free space on the same mount.
    ///
    /// Moving `delta` bytes from `available` into `measured_used` is exactly
    /// what growing the compile cache does. If the estimate changed, each
    /// restart would re-derive a different soft limit from the previous run's
    /// cache size and the value would oscillate.
    ///
    /// Scope, precisely: this drives the PURE function with `measured_used` as a
    /// parameter, so what it pins is that `disk_budget_for_clamped`'s basis is
    /// `used + available`. It does NOT pin that `startup_disk_budget_estimate`
    /// actually feeds it the `du` walk — a version that passed `0` there (i.e.
    /// an estimate built on bare free space, the tempting simplification) would
    /// pass every line here. `estimate_composes_the_measured_walk_and_the_free_space_read`
    /// is what closes that.
    #[test]
    fn estimate_is_invariant_under_cache_growth_on_the_same_mount() {
        let capacity = 4 * GIB;
        for cache_bytes in [0, 32 * MIB, 128 * MIB, 512 * MIB, 2 * GIB, capacity] {
            let available = capacity - cache_bytes;
            assert_eq!(
                startup_disk_budget_from_measurements(cache_bytes, Some(available), 0.5, CAP),
                2 * GIB,
                "budget must depend on used+available (the mount's reachable \
                 capacity), not on how much of it the cache currently holds"
            );
        }
    }

    /// Monotone non-decreasing in the measured-used term. This is what makes
    /// omitting persisted state bytes SAFE: the estimate is then never larger
    /// than the live budget computed from the complete total.
    #[test]
    fn estimate_is_monotone_in_measured_used() {
        let available = 400 * MIB;
        let mut previous = 0;
        for used in [0, 1, 128 * MIB, 512 * MIB, 4 * GIB, 64 * GIB, u64::MAX] {
            let budget = startup_disk_budget_from_measurements(used, Some(available), 0.5, CAP);
            assert!(
                budget >= previous,
                "budget must not shrink as measured usage grows (used={used})"
            );
            previous = budget;
        }
        // Concretely: omitting a 300 MiB state term yields a SMALLER budget than
        // including it, never a larger one.
        let without_state =
            startup_disk_budget_from_measurements(512 * MIB, Some(available), 0.5, CAP);
        let with_state =
            startup_disk_budget_from_measurements(512 * MIB + 300 * MIB, Some(available), 0.5, CAP);
        assert!(without_state < with_state);
        assert_eq!(without_state, 456 * MIB);
    }

    /// An unreadable free-space query must not silently shrink the budget: it
    /// falls back to `u64::MAX`, which clamps to the operator cap. Same rule the
    /// live recompute applies.
    #[test]
    fn unreadable_free_space_clamps_to_the_cap_not_to_zero() {
        assert_eq!(
            startup_disk_budget_from_measurements(0, None, 0.5, CAP),
            CAP,
            "a free-space read we cannot trust must degrade to cap-only"
        );
        // And with a smaller operator cap, to that cap.
        assert_eq!(
            startup_disk_budget_from_measurements(0, None, 0.5, GIB),
            GIB
        );
    }

    /// Boundary values: zero capacity, a pct of zero, and an absurd basis all
    /// have to land inside the documented clamp instead of panicking, wrapping,
    /// or returning zero. A zero-capacity mount still yields the 128 MiB MIN.
    #[test]
    fn estimate_boundaries_stay_inside_the_clamp() {
        assert_eq!(
            startup_disk_budget_from_measurements(0, Some(0), 0.5, CAP),
            128 * MIB,
            "an empty mount must still resolve to the MIN floor, never 0"
        );
        assert_eq!(
            startup_disk_budget_from_measurements(0, Some(u64::MAX), 1.0, CAP),
            CAP,
            "an enormous basis must clamp to the cap, not wrap"
        );
        assert_eq!(
            startup_disk_budget_from_measurements(u64::MAX, Some(u64::MAX), 1.0, CAP),
            CAP,
            "saturating basis: no overflow into a small budget"
        );
        assert_eq!(
            startup_disk_budget_from_measurements(4 * GIB, Some(4 * GIB), 0.0, CAP),
            128 * MIB,
            "pct=0 collapses to the MIN floor"
        );
    }

    /// The impure entry point must resolve on real directories without
    /// panicking, and honor the clamp. Driven with `pct = 0.0` so the expected
    /// value is the MIN floor on every host — no dependence on the CI machine's
    /// free space, so this cannot flake.
    ///
    /// Deliberately makes NO claim about the `du` walk: at `pct = 0.0` the
    /// measured term is multiplied by zero, so any fixture written here would be
    /// inert. (An earlier version of this test wrote two files and then asserted
    /// a containment check against a function that clamps into exactly that
    /// interval by construction — vacuous on both counts, which is the anti-
    /// pattern this PR calls out elsewhere.) The composition is pinned by
    /// `estimate_composes_the_measured_walk_and_the_free_space_read`.
    #[test]
    fn estimate_on_real_dirs_resolves_the_min_floor_at_pct_zero() {
        let dir = tempfile::tempdir().unwrap();
        let contracts = dir.path().join("contracts");
        let cache = dir.path().join("wasmtime-cache");
        std::fs::create_dir_all(&contracts).unwrap();
        std::fs::create_dir_all(&cache).unwrap();

        assert_eq!(
            startup_disk_budget_estimate(&contracts, &cache, 0.0, CAP),
            128 * MIB,
            "pct=0 must resolve to the MIN floor regardless of the host's disk"
        );
    }

    /// Pin: [`startup_disk_budget_estimate`] must COMPOSE the two filesystem
    /// reads it is documented to compose.
    ///
    /// Both inputs are helper-internals-tested and call-site-untested otherwise,
    /// and each has a one-token mutation that compiles, keeps the whole suite
    /// green, and defeats the #5014 fix on every host in the world:
    ///
    /// * `available_bytes(contracts_dir)` → `None` makes the basis `u64::MAX`, so
    ///   the budget clamps to the operator cap and the disk term never binds —
    ///   the bound is simply switched off.
    /// * `measure_startup_disk_used(..)` → `0` is exactly "an estimate built on
    ///   bare free space", the oscillation this function's rustdoc says must not
    ///   happen.
    /// * swapping the two `&Path` arguments compiles (both are `&Path`) and
    ///   silently measures `*.wasm` under the cache dir plus everything under the
    ///   contracts dir — including the redb file the term is documented to omit.
    ///
    /// A runtime test cannot close these host-independently: `available` is the
    /// CI machine's real free space, so any assertion strong enough to see a
    /// 4 KiB fixture would be reading a number another process can move.
    #[test]
    fn estimate_composes_the_measured_walk_and_the_free_space_read() {
        let src = include_str!("disk_usage.rs");
        let body = src
            .split_once(concat!("pub(crate) fn ", "startup_disk_budget_estimate(\n"))
            .expect("startup_disk_budget_estimate must exist")
            .1
            .split_once("\n}\n")
            .expect("end marker of startup_disk_budget_estimate must exist")
            .0;
        let collapsed = body.split_whitespace().collect::<Vec<_>>().join(" ");

        assert!(
            collapsed.contains(concat!("measure_startup_", "disk_used(")),
            "the estimate must measure Freenet's own bytes — substituting a \
             constant rebuilds it on bare free space, which oscillates across \
             restarts (#5014)"
        );
        assert!(
            collapsed.contains("contracts_dir, compile_cache_dir"),
            "the measured-used term's two `&Path` arguments must stay in order: a \
             swap compiles and measures the wrong trees"
        );
        assert!(
            collapsed.contains(concat!("available_", "bytes(contracts_dir)")),
            "the estimate must read free space on the CONTRACTS mount — passing \
             `None` (or the cache dir, a different mount in principle) silently \
             disables the disk bound"
        );
    }

    /// The prune is a no-op while the tree fits: wasmtime's own hourly cleanup
    /// owns the steady state, and deleting artifacts a node is entitled to keep
    /// would buy nothing but recompiles.
    #[test]
    fn prune_leaves_a_cache_that_already_fits_untouched() {
        let dir = tempfile::tempdir().unwrap();
        let cache = dir.path().join("wasmtime-cache");
        write_file(&cache.join("a"), 400);
        write_file(&cache.join("b"), 400);

        assert_eq!(prune_compile_cache_to_limit(&cache, 1000), 0);
        assert_eq!(
            du_walk(&cache),
            800,
            "nothing may be deleted under the limit"
        );
        // Exactly at the limit is still "fits" — the trigger is strictly-greater.
        assert_eq!(prune_compile_cache_to_limit(&cache, 800), 0);
        assert_eq!(du_walk(&cache), 800);
    }

    /// The load-bearing behavior (#5014): an oversized cache is brought down to
    /// wasmtime's own 70%-of-limit target, oldest first, so a node that restarts
    /// under a newly-lowered limit stops carrying a cache sized by the old one.
    ///
    /// Without this the fix bounds future starts only: wasmtime reads the soft
    /// limit exclusively in its cleanup pass, that pass is reachable only from
    /// the cache-WRITE path, and a node with a stable contract-blob set performs
    /// only cache hits after a restart.
    #[test]
    fn prune_deletes_oldest_first_down_to_the_target() {
        let dir = tempfile::tempdir().unwrap();
        let cache = dir.path().join("wasmtime-cache");
        // Five 400-byte artifacts = 2000 bytes against a 1000-byte limit, so the
        // target is 700 and four must go: 2000 → 1600 → 1200 → 800 → 400.
        // Nested so the walk is exercised on a tree, not one flat directory.
        let paths = [
            cache.join("oldest"),
            cache.join("sub").join("second"),
            cache.join("third"),
            cache.join("sub").join("fourth"),
            cache.join("newest"),
        ];
        for (i, path) in paths.iter().enumerate() {
            write_file(path, 400);
            // Explicit mtimes: the outcome must not depend on how fast the test
            // host writes files, nor on directory iteration order.
            filetime::set_file_mtime(path, filetime::FileTime::from_unix_time(1000 + i as i64, 0))
                .unwrap();
        }

        assert_eq!(prune_compile_cache_to_limit(&cache, 1000), 1600);
        assert_eq!(
            du_walk(&cache),
            400,
            "the tree must end at or below 70% of the limit"
        );
        assert!(
            paths[4].exists(),
            "the most recently used artifact must survive — a full wipe is equally \
             safe but pays a whole recompile wave on every tight-budget restart"
        );
        for stale in &paths[..4] {
            assert!(!stale.exists(), "{stale:?} should have been pruned");
        }
    }

    /// A first-ever start has no cache dir at all, and the limit can legitimately
    /// be tiny. Neither may panic, and a zero limit must clear the tree rather
    /// than divide its way into leaving something behind.
    #[test]
    fn prune_boundaries_are_safe() {
        assert_eq!(
            prune_compile_cache_to_limit(Path::new("/nonexistent/cache"), 128 * MIB),
            0,
            "a missing cache dir is a first start, not an error"
        );

        let dir = tempfile::tempdir().unwrap();
        let cache = dir.path().join("wasmtime-cache");
        write_file(&cache.join("a"), 100);
        write_file(&cache.join("b"), 100);
        assert_eq!(prune_compile_cache_to_limit(&cache, 0), 200);
        assert_eq!(du_walk(&cache), 0);

        // u64::MAX must not overflow the `limit × 70` target computation.
        write_file(&cache.join("c"), 100);
        assert_eq!(prune_compile_cache_to_limit(&cache, u64::MAX), 0);
        assert_eq!(du_walk(&cache), 100);
    }

    /// The prune target must stay wasmtime's own post-cleanup target, so the
    /// restart prune and wasmtime's hourly cleanup agree on the steady state
    /// instead of one repeatedly undoing the other's idea of "enough".
    #[test]
    fn prune_target_matches_wasmtimes_own_cleanup_target() {
        assert_eq!(super::COMPILE_CACHE_PRUNE_TARGET_PCT, 70);
    }
}
