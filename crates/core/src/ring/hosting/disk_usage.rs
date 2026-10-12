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
//! # Measured and reported, NOT budgeted: the database file and webapp cache (#5007)
//!
//! Two more consumers are measured on the same 60s sweep and reported (local
//! dashboard, `RouterSnapshot` telemetry, diagnostics), but are NOT part of
//! [`DiskUsageTracker::total_bytes`] and so feed neither the eviction floor nor
//! any admission gate:
//!
//! - **The storage backend's database directory** ([`DiskUsageStats::db_file_bytes`]).
//!   redb is copy-on-write and only truncates *trailing* free space, so the
//!   interior dead space a busy node accumulates is invisible to the logical
//!   state total above. Measured on a production peer (2026-07): 583 MB of state
//!   against 2.68 GB of database. `db_file_bytes - state_bytes` is that dead
//!   space (plus live non-state rows and any migration backup beside the file).
//! - **The unpacked-webapp cache** ([`DiskUsageStats::webapp_cache_bytes`]),
//!   under the XDG cache dir: 1236 MiB / 82 entries on one peer before #5012
//!   capped it at 64 MiB.
//!
//! Charging them to the budget is deliberately deferred (#5853, "measure now,
//! enforce later"). Counting dead space in `used` would make every peer whose
//! database outgrew its free disk refuse fresh PUTs on upgrade, eviction cannot
//! free dead space, and the startup compaction that can is gated so a restart
//! does not reliably reclaim it. #5853 holds that analysis.
//!
//! These two walks charge **allocated blocks**, not apparent length (see
//! [`file_disk_bytes`]): redb's file is sparse on some peers (2.56 GiB apparent
//! against 1.71 GiB allocated on the production gateway). The budgeted WASM and
//! compile-cache walks keep counting apparent length, unchanged. A failed walk
//! of either reported directory keeps its previous value and warns, rather than
//! publishing 0 (see [`DiskUsageTracker::store_measurement`]).
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

/// The four directories the aggregate disk-usage tracker measures (#4683,
/// #5007).
///
/// A struct rather than four positional parameters because all four are
/// `PathBuf`: a transposed pair would compile, type-check, and silently
/// mis-account (the webapp cache measured as the database, say). Named fields
/// make the one production call site — `contract/handler.rs` — say what it
/// means, and give its pin test an unambiguous needle.
#[derive(Debug, Clone)]
pub(crate) struct HostingDiskPaths {
    /// Mode-resolved `contracts_dir`, holding the `*.wasm` code blobs.
    pub contracts_dir: PathBuf,
    /// Relocated wasmtime compile-cache dir (on the data-dir mount).
    pub wasmtime_cache_dir: PathBuf,
    /// Mode-resolved `db_dir`, holding the storage backend's database file
    /// (`db` for redb, `freenet.db` for sqlite) plus any sidecars. Measured and
    /// reported only.
    pub db_dir: PathBuf,
    /// Unpacked-webapp cache root (XDG cache dir). Measured and reported only.
    pub webapp_cache_dir: PathBuf,
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
    /// Allocated size of the storage backend's database directory (#5007),
    /// shallow walk. REPORTED ONLY: not part of `total_bytes`. `None` until a
    /// walk has succeeded, so a directory that was never readable is not
    /// reported as an empty one. A directory that does not exist is `Some(0)`.
    pub db_file_bytes: Option<u64>,
    /// Allocated size of the unpacked-webapp cache (#5007). REPORTED ONLY: not
    /// part of `total_bytes`. `None` until a walk has succeeded.
    pub webapp_cache_bytes: Option<u64>,
    /// `state_bytes + wasm_bytes + compile_cache_bytes`, the aggregate the disk
    /// budget bounds. Excludes `db_file_bytes` and `webapp_cache_bytes` (#5853).
    pub total_bytes: u64,
}

/// Sentinel stored in a reported gauge until its first successful walk.
const UNMEASURED: u64 = u64::MAX;

/// Read a reported gauge, mapping [`UNMEASURED`] to `None`.
fn measured(gauge: &AtomicU64) -> Option<u64> {
    let bytes = gauge.load(Ordering::Relaxed);
    (bytes != UNMEASURED).then_some(bytes)
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
/// expensive operations are [`Self::seed`] and the four `refresh_*` `du`-walks,
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
    /// Allocated size of the database directory (#5007). Reported only, never
    /// read by [`Self::total_bytes`] or any admission gate. [`UNMEASURED`]
    /// until a walk succeeds.
    db_file_bytes: AtomicU64,
    /// Allocated size of the unpacked-webapp cache (#5007). Reported only.
    /// [`UNMEASURED`] until a walk succeeds.
    webapp_cache_bytes: AtomicU64,
    /// Edge-trigger flags for "the last walk of this directory failed", one per
    /// reported gauge, so a warning about one directory cannot suppress a
    /// warning about the other. See [`Self::store_measurement`].
    db_file_measure_failed: AtomicBool,
    webapp_cache_measure_failed: AtomicBool,
    /// The directories all of the above are measured from.
    paths: HostingDiskPaths,
}

impl DiskUsageTracker {
    /// Create an unseeded tracker. The budgeted counters start at zero and the
    /// two reported gauges at "not measured"; call
    /// [`Self::seed`] once before the counts are meaningful.
    pub(crate) fn new(paths: HostingDiskPaths) -> Self {
        Self {
            state_bytes: AtomicU64::new(0),
            wasm_bytes: AtomicU64::new(0),
            compile_cache_bytes: AtomicU64::new(0),
            seeded: AtomicBool::new(false),
            state_sizes: Mutex::new(StateSizeIndex::default()),
            db_file_bytes: AtomicU64::new(UNMEASURED),
            webapp_cache_bytes: AtomicU64::new(UNMEASURED),
            db_file_measure_failed: AtomicBool::new(false),
            webapp_cache_measure_failed: AtomicBool::new(false),
            paths,
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
            db_file_bytes: measured(&self.db_file_bytes),
            webapp_cache_bytes: measured(&self.webapp_cache_bytes),
            total_bytes: state_bytes
                .saturating_add(wasm_bytes)
                .saturating_add(compile_cache_bytes),
        }
    }

    /// Seed the state-bytes counter and per-key size index from an exact list of
    /// `(contract, state_size)` pairs (the caller reads these from redb rows so
    /// this module stays storage-backend-agnostic and unit-testable). Also runs
    /// the initial WASM, compile-cache, database and webapp-cache walks.
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

        self.refresh_wasm();
        self.refresh_compile_cache();
        self.refresh_db_file();
        self.refresh_webapp_cache();
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
            .store(du_walk_wasm(&self.paths.contracts_dir), Ordering::Relaxed);
    }

    /// Re-measure the wasmtime compile-cache total by `du`-walking its dir.
    /// Wasmtime writes the cache opaquely, so this re-walk (not a delta) is the
    /// only way to account for it.
    pub(crate) fn refresh_compile_cache(&self) {
        self.compile_cache_bytes
            .store(du_walk(&self.paths.wasmtime_cache_dir), Ordering::Relaxed);
    }

    /// Store the result of one walk of a REPORTED directory, distinguishing
    /// "measured 0" from "could not measure" (#5007).
    ///
    /// A failed walk keeps the gauge's previous value and warns, because a 0
    /// published for an unreadable database directory would read in telemetry
    /// as a healthy node with no dead space, with nothing to contradict it. If
    /// no walk has succeeded yet, the previous value is [`UNMEASURED`], which
    /// [`DiskUsageTracker::stats`] reports as `None`.
    ///
    /// `NotFound` is a legitimate 0 (a store not created yet). The warning is
    /// edge-triggered per gauge, so a permanently unreadable directory logs
    /// once rather than once per 60s sweep, and logs again if the condition
    /// recurs after a recovery.
    fn store_measurement(
        &self,
        gauge: &AtomicU64,
        already_warned: &AtomicBool,
        dir: &Path,
        what: &'static str,
        walk: fn(&Path) -> std::io::Result<u64>,
    ) {
        match walk(dir) {
            Ok(bytes) => {
                gauge.store(bytes, Ordering::Relaxed);
                already_warned.store(false, Ordering::Relaxed);
            }
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => {
                gauge.store(0, Ordering::Relaxed);
                already_warned.store(false, Ordering::Relaxed);
            }
            Err(err) => {
                if !already_warned.swap(true, Ordering::Relaxed) {
                    tracing::warn!(
                        measurement = what,
                        dir = %dir.display(),
                        error = %err,
                        retained_bytes = ?measured(gauge),
                        "could not measure a hosting disk-usage directory; keeping \
                         the previous measurement rather than reporting 0"
                    );
                }
            }
        }
    }

    /// Re-measure the storage backend's database directory (#5007). REPORTED
    /// ONLY: the result never enters [`Self::total_bytes`] (#5853).
    ///
    /// A **shallow** walk: in `Network` mode `db_dir` is `<data>/db`, which
    /// contains the `local/` subtree belonging to the other operation mode, and
    /// those bytes are not this node's storage. The shallow walk still counts
    /// every file the backend writes beside the database (redb's `db` plus any
    /// `.backup` left by a migration, sqlite's `freenet.db` plus `-wal`/`-shm`).
    pub(crate) fn refresh_db_file(&self) {
        self.store_measurement(
            &self.db_file_bytes,
            &self.db_file_measure_failed,
            &self.paths.db_dir,
            "database_file",
            du_walk_shallow,
        );
    }

    /// Re-measure the unpacked-webapp cache (#5007), recursively. REPORTED
    /// ONLY: the result never enters [`Self::total_bytes`]. #5012 already caps
    /// this cache at 64 MiB with its own LRU, and hosting eviction has no lever
    /// on it.
    pub(crate) fn refresh_webapp_cache(&self) {
        self.store_measurement(
            &self.webapp_cache_bytes,
            &self.webapp_cache_measure_failed,
            &self.paths.webapp_cache_dir,
            "webapp_cache",
            du_walk_allocated,
        );
    }

    /// Free bytes on the mount holding the tracked `contracts_dir` — the
    /// `available` term the disk budget sizes against (#4683). `None` when the
    /// platform query fails; the caller falls back to `u64::MAX`. The contracts
    /// dir shares the data-dir mount, which is where all tracked bytes (state,
    /// wasm, relocated compile cache) land, so it is the correct mount to probe.
    pub(crate) fn available_bytes(&self) -> Option<u64> {
        available_bytes(&self.paths.contracts_dir)
    }

    /// Test-only: install a measured database-directory size directly, so a
    /// test can drive footprints far larger than any fixture could
    /// materialize (a sparse `set_len` file cannot stand in: the walk charges
    /// allocated blocks, so a hole measures 0).
    #[cfg(test)]
    pub(crate) fn set_db_file_bytes_for_test(&self, bytes: u64) {
        self.db_file_bytes.store(bytes, Ordering::Relaxed);
    }

    /// Test-only: install a measured webapp-cache size directly. See
    /// [`Self::set_db_file_bytes_for_test`].
    #[cfg(test)]
    pub(crate) fn set_webapp_cache_bytes_for_test(&self, bytes: u64) {
        self.webapp_cache_bytes.store(bytes, Ordering::Relaxed);
    }
}

/// Recursively sum the byte size of every regular file under `dir`. A missing
/// directory (not yet created) or an unreadable entry contributes 0 rather than
/// erroring — the refresh path is best-effort telemetry, unlike the fail-loud
/// state seed.
///
/// `pub(crate)` (re-exported as `ring::disk_directory_size_bytes`, #5014) so
/// the wasmtime on-disk compile-cache startup sizing
/// (`wasm_runtime::runtime::default_wasmtime_cache_size_bytes_for_dir`) can
/// measure a prior run's cache directory without duplicating this walk.
pub(crate) fn du_walk(dir: &Path) -> u64 {
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

/// The disk a single file actually consumes: its **allocated blocks**, not its
/// apparent length.
///
/// `Metadata::len()` is what `ls` reports — the file's logical extent. For a
/// **sparse** file that over-states consumption, because the holes occupy no
/// blocks at all. redb's database file is sparse on some peers: measured on the
/// production gateway (2026-07-29, stable across repeated samples),
/// `len()` reported 2,749,370,368 bytes against 1,839,210,496 bytes actually
/// allocated, a 49% over-statement. (The same file on another peer was not
/// sparse at all, so this varies with the peer's compaction history and cannot
/// be corrected by a constant.) Block slack on a small file IS charged, since
/// the filesystem spent that block; a hole is not. This is what `du` reports,
/// where `ls` reports length.
///
/// Used only for the REPORTED gauges (database directory, webapp cache). The
/// budgeted WASM and compile-cache walks, and [`du_walk`]'s other caller (the
/// wasmtime compile-cache sizing, which wasmtime itself bounds in apparent
/// bytes), keep counting apparent length.
///
/// `st_blocks` is defined in 512-byte units regardless of the filesystem's own
/// block size. Windows has no equivalent in `std`, so non-unix falls back to the
/// apparent length.
fn file_disk_bytes(meta: &std::fs::Metadata) -> u64 {
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt;
        meta.blocks().saturating_mul(512)
    }
    #[cfg(not(unix))]
    {
        meta.len()
    }
}

/// Sum the allocated size of every regular file **directly inside** `dir`,
/// without descending into subdirectories (#5007).
///
/// The non-recursive counterpart to [`du_walk_allocated`], for the database directory:
/// `db_dir` in `Network` mode contains the `local/` mode split, whose bytes
/// belong to a different node instance and must not be charged here.
///
/// Returns `Err` when `dir` itself cannot be read. That is deliberate and is the
/// difference between "measured 0" and "could not measure": see
/// [`DiskUsageTracker::store_measurement`]. An individual unreadable entry
/// inside a readable directory still contributes 0 (best effort).
///
/// A symlink to a regular file is followed and counted (an operator may have
/// moved the database file onto another disk and linked it back). Symlinks to
/// directories are not, so the walk stays shallow.
fn du_walk_shallow(dir: &Path) -> std::io::Result<u64> {
    let mut total: u64 = 0;
    for entry in std::fs::read_dir(dir)? {
        let Ok(entry) = entry else {
            continue;
        };
        let Ok(file_type) = entry.file_type() else {
            continue;
        };
        let meta = if file_type.is_symlink() {
            std::fs::metadata(entry.path())
        } else {
            entry.metadata()
        };
        if let Ok(meta) = meta {
            if meta.is_file() {
                total = total.saturating_add(file_disk_bytes(&meta));
            }
        }
    }
    Ok(total)
}

/// Recursively sum the allocated size of every regular file under `dir` (see
/// [`file_disk_bytes`]). Same error contract as [`du_walk_shallow`]: `Err` only
/// when the ROOT `dir` cannot be read; an unreadable subdirectory or entry
/// contributes 0. Unlike [`du_walk_shallow`], symlinks are not followed at
/// all, which keeps the recursion free of loops; a symlinked entry contributes
/// 0.
fn du_walk_allocated(dir: &Path) -> std::io::Result<u64> {
    let mut total: u64 = 0;
    let mut stack = vec![dir.to_path_buf()];
    let mut at_root = true;
    while let Some(path) = stack.pop() {
        let entries = match std::fs::read_dir(&path) {
            Ok(entries) => entries,
            Err(err) if at_root => return Err(err),
            Err(_) => continue,
        };
        at_root = false;
        for entry in entries.flatten() {
            let Ok(file_type) = entry.file_type() else {
                continue;
            };
            if file_type.is_dir() {
                stack.push(entry.path());
            } else if file_type.is_file() {
                if let Ok(meta) = entry.metadata() {
                    total = total.saturating_add(file_disk_bytes(&meta));
                }
            }
        }
    }
    Ok(total)
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

    /// Paths that all resolve to nothing, so every walk contributes 0.
    fn nonexistent_paths() -> HostingDiskPaths {
        HostingDiskPaths {
            contracts_dir: PathBuf::from("/nonexistent/contracts"),
            wasmtime_cache_dir: PathBuf::from("/nonexistent/cache"),
            db_dir: PathBuf::from("/nonexistent/db"),
            webapp_cache_dir: PathBuf::from("/nonexistent/webapp"),
        }
    }

    fn tracker() -> DiskUsageTracker {
        // Nonexistent dirs → du-walks contribute 0, isolating state-delta math.
        DiskUsageTracker::new(nonexistent_paths())
    }

    /// Write a file of `len` bytes at `path`, creating parents. Pseudo-random
    /// content, not zeros, so a filesystem with transparent compression cannot
    /// collapse the fixture and change what the allocation-based walks see.
    fn write_file(path: &Path, len: usize) {
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent).unwrap();
        }
        let mut state: u32 = 0x9E37_79B9;
        let bytes: Vec<u8> = (0..len)
            .map(|_| {
                state = state.wrapping_mul(1_664_525).wrapping_add(1_013_904_223);
                (state >> 24) as u8
            })
            .collect();
        let mut f = std::fs::File::create(path).unwrap();
        f.write_all(&bytes).unwrap();
    }

    /// The disk `path` occupies, re-derived independently of the code under
    /// test. Small fixtures occupy a whole filesystem block whose size varies,
    /// so the walk tests assert which files were counted against this rather
    /// than against a hard-coded byte count.
    fn alloc(path: &Path) -> u64 {
        let meta = std::fs::metadata(path).unwrap();
        #[cfg(unix)]
        {
            use std::os::unix::fs::MetadataExt;
            meta.blocks() * 512
        }
        #[cfg(not(unix))]
        {
            meta.len()
        }
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

        let t = DiskUsageTracker::new(HostingDiskPaths {
            contracts_dir: contracts.clone(),
            ..nonexistent_paths()
        });
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

        let t = DiskUsageTracker::new(HostingDiskPaths {
            wasmtime_cache_dir: cache.clone(),
            ..nonexistent_paths()
        });
        t.seed(std::iter::empty());
        assert_eq!(t.stats().compile_cache_bytes, 512);

        // Grow the cache; a refresh must observe the new total.
        let mut g = std::fs::File::create(cache.join("mod2.cache")).unwrap();
        g.write_all(&[0u8; 88]).unwrap();
        t.refresh_compile_cache();
        assert_eq!(t.stats().compile_cache_bytes, 600);
        assert_eq!(t.total_bytes(), 600);
    }

    // --- Reported, not budgeted: the database directory and webapp cache -----
    //
    // #5007 measures both; #5853 tracks charging them. Until then they must not
    // move the eviction floor or any admission gate.

    /// The property this PR is shipped on: however large the measured database
    /// directory and webapp cache are, the budgeted aggregate and all three
    /// admission gates answer exactly as a tracker that never measured them.
    ///
    /// The database size is injected at 40 GiB. Each gate is probed at its
    /// exact admit/refuse boundary computed from the figure WITHOUT it, so
    /// charging any part of it to any gate turns an admit into a refusal. (The
    /// eviction-budget side, where the 32 GiB cap matters, is
    /// `measured_db_size_does_not_change_the_eviction_budget` in `hosting.rs`.)
    #[test]
    fn reported_gauges_never_enter_the_budget_or_admission() {
        const GIB: u64 = 1024 * 1024 * 1024;
        let dir = tempfile::tempdir().unwrap();
        let webapp = dir.path().join("webapp");
        write_file(&webapp.join("aaaa").join("index.html"), 256 * 1024);

        let measured = DiskUsageTracker::new(HostingDiskPaths {
            webapp_cache_dir: webapp.clone(),
            ..nonexistent_paths()
        });
        let blind = tracker();
        let rows = [(test_key(1), 600u64), (test_key(2), 400u64)];
        measured.seed(rows);
        blind.seed(rows);
        measured.set_db_file_bytes_for_test(40 * GIB);

        let s = measured.stats();
        assert_eq!(
            s.db_file_bytes.unwrap(),
            40 * GIB,
            "the database size is reported"
        );
        assert!(
            s.webapp_cache_bytes.unwrap() > 0,
            "the webapp cache is reported"
        );
        assert_eq!(measured.total_bytes(), blind.total_bytes());
        assert_eq!(s.total_bytes, blind.stats().total_bytes);
        assert_eq!(s.total_bytes, 1000);

        // Budget exactly equal to the unmeasured projection: admitted by the
        // blind tracker, so it must be admitted by the measuring one too.
        let put_budget = 1000 + 100;
        assert!(
            blind
                .admit_state_write(&test_key(3), 100, put_budget)
                .is_ok()
        );
        assert!(
            measured
                .admit_state_write(&test_key(3), 100, put_budget)
                .is_ok()
        );
        assert!(
            measured
                .admit_state_write(&test_key(3), 101, put_budget)
                .is_err()
        );

        let update_budget = 1000 - 600 + 700;
        assert!(
            blind
                .admit_state_update(&test_key(1), 700, update_budget)
                .is_ok()
        );
        assert!(
            measured
                .admit_state_update(&test_key(1), 700, update_budget)
                .is_ok()
        );
        assert!(
            measured
                .admit_state_update(&test_key(1), 701, update_budget)
                .is_err()
        );

        let wasm_budget = 1000 + 50;
        assert!(blind.admit_wasm_write(50, wasm_budget).is_ok());
        assert!(measured.admit_wasm_write(50, wasm_budget).is_ok());
        assert!(measured.admit_wasm_write(51, wasm_budget).is_err());
    }

    /// An UPDATE that grows a contract is bounded exactly as before #5007, even
    /// when the measured database directory is far larger than the live state.
    /// Pins that no "growth fits in dead space" exemption is re-introduced
    /// without the bound #5853 says it needs.
    #[test]
    fn update_growth_gets_no_dead_space_exemption() {
        let t = tracker();
        t.seed([(test_key(1), 100)]);
        t.set_db_file_bytes_for_test(10_000);
        // Over budget on the budgeted figure (100 > 50); growth by 1 byte must
        // be refused even though it would fit inside the measured file.
        assert!(t.admit_state_update(&test_key(1), 101, 50).is_err());
        // Shrinking and holding stay admitted while over budget, as on main.
        assert!(t.admit_state_update(&test_key(1), 100, 50).is_ok());
        assert!(t.admit_state_update(&test_key(1), 99, 50).is_ok());
    }

    /// A directory that has NEVER been readable reports `None`, not 0: there is
    /// no previous measurement to keep, and 0 would read as a node with no dead
    /// space. The warning edge flag is set by the failure, cleared by the
    /// first success, and is per gauge.
    #[test]
    fn a_directory_never_measured_reports_none_and_flags_are_per_gauge() {
        let dir = tempfile::tempdir().unwrap();
        let db = dir.path().join("db");
        let webapp = dir.path().join("webapp");
        // A FILE where the directory should be: ENOTDIR, fails for root too.
        write_file(&db, 8);
        write_file(&webapp.join("aaaa").join("index.html"), 4096);

        let t = DiskUsageTracker::new(HostingDiskPaths {
            db_dir: db.clone(),
            webapp_cache_dir: webapp,
            ..nonexistent_paths()
        });
        t.seed(std::iter::empty());
        assert_eq!(t.stats().db_file_bytes, None, "never measured is not 0");
        assert!(t.stats().webapp_cache_bytes.is_some());
        assert!(t.db_file_measure_failed.load(Ordering::Relaxed));
        assert!(
            !t.webapp_cache_measure_failed.load(Ordering::Relaxed),
            "one directory's failure must not set the other's flag"
        );

        std::fs::remove_file(&db).unwrap();
        write_file(&db.join("db"), 4096);
        t.refresh_db_file();
        assert_eq!(t.stats().db_file_bytes, Some(alloc(&db.join("db"))));
        assert!(
            !t.db_file_measure_failed.load(Ordering::Relaxed),
            "a success re-arms the warning for the next failure"
        );
    }

    /// [`du_walk`] keeps counting APPARENT length. It feeds the budgeted WASM
    /// and compile-cache gauges and, via `ring::disk_directory_size_bytes`, the
    /// wasmtime compile-cache startup sizing (#5014, #5328), whose limit
    /// wasmtime enforces in apparent bytes. Switching it to allocated blocks
    /// would change those numbers on upgrade and could wipe a cache wasmtime
    /// considers within its limit.
    #[cfg(unix)]
    #[test]
    fn budgeted_walk_counts_apparent_length() {
        let dir = tempfile::tempdir().unwrap();
        // One byte allocates a whole block; a hole allocates none. Both differ
        // from their apparent length, in opposite directions.
        write_file(&dir.path().join("tiny"), 1);
        std::fs::File::create(dir.path().join("sparse"))
            .unwrap()
            .set_len(1024 * 1024)
            .unwrap();
        assert_ne!(
            alloc(&dir.path().join("tiny")) + alloc(&dir.path().join("sparse")),
            1 + 1024 * 1024,
            "fixture must make allocation and length disagree"
        );
        assert_eq!(du_walk(dir.path()), 1 + 1024 * 1024);
    }

    /// The reported walks charge allocation: a sparse file is charged its
    /// allocated blocks, not its length. Measured on the production gateway
    /// (2026-07-29): 2,749,370,368 bytes apparent against 1,839,210,496
    /// allocated for the redb file.
    #[cfg(unix)]
    #[test]
    fn sparse_file_is_charged_by_allocation_not_apparent_length() {
        const APPARENT: u64 = 64 * 1024 * 1024;
        let dir = tempfile::tempdir().unwrap();
        let db = dir.path().join("db");
        std::fs::create_dir_all(&db).unwrap();
        std::fs::File::create(db.join("db"))
            .unwrap()
            .set_len(APPARENT)
            .unwrap();
        assert_eq!(std::fs::metadata(db.join("db")).unwrap().len(), APPARENT);

        let t = DiskUsageTracker::new(HostingDiskPaths {
            db_dir: db.clone(),
            ..nonexistent_paths()
        });
        t.seed(std::iter::empty());
        let measured = t.stats().db_file_bytes.unwrap();
        assert_eq!(measured, alloc(&db.join("db")));
        assert!(
            measured < APPARENT / 2,
            "a hole occupies no disk (measured {measured} against {APPARENT} apparent)"
        );
    }

    /// The other direction: a file smaller than one block is charged the block.
    #[cfg(unix)]
    #[test]
    fn block_slack_on_a_small_file_is_charged() {
        let dir = tempfile::tempdir().unwrap();
        let db = dir.path().join("db");
        write_file(&db.join("db"), 1);

        let t = DiskUsageTracker::new(HostingDiskPaths {
            db_dir: db.clone(),
            ..nonexistent_paths()
        });
        t.seed(std::iter::empty());
        assert_eq!(t.stats().db_file_bytes.unwrap(), alloc(&db.join("db")));
        assert!(t.stats().db_file_bytes.unwrap() > 1);
    }

    /// A walk that CANNOT read the database directory keeps the previous value
    /// rather than reporting 0, which would read as a node with no dead space.
    /// The failure is injected by replacing the directory with a FILE
    /// (ENOTDIR), which also fails for root, unlike a permissions fixture.
    #[test]
    fn an_unreadable_database_directory_keeps_the_previous_measurement() {
        let dir = tempfile::tempdir().unwrap();
        let db = dir.path().join("db");
        write_file(&db.join("db"), 256 * 1024);

        let t = DiskUsageTracker::new(HostingDiskPaths {
            db_dir: db.clone(),
            ..nonexistent_paths()
        });
        t.seed([(test_key(1), 10)]);
        let measured = t.stats().db_file_bytes.unwrap();
        assert!(
            measured > 0,
            "fixture must measure something to then retain"
        );

        std::fs::remove_dir_all(&db).unwrap();
        write_file(&db, 8);
        assert!(std::fs::read_dir(&db).is_err(), "fixture must make it fail");

        t.refresh_db_file();
        assert_eq!(t.stats().db_file_bytes.unwrap(), measured);
        assert_eq!(t.total_bytes(), 10, "and the budgeted figure is unaffected");
    }

    /// Same rule for the webapp cache, which is walked recursively.
    #[test]
    fn an_unreadable_webapp_cache_keeps_the_previous_measurement() {
        let dir = tempfile::tempdir().unwrap();
        let webapp = dir.path().join("webapp");
        write_file(&webapp.join("aaaa").join("index.html"), 256 * 1024);

        let t = DiskUsageTracker::new(HostingDiskPaths {
            webapp_cache_dir: webapp.clone(),
            ..nonexistent_paths()
        });
        t.seed(std::iter::empty());
        let measured = t.stats().webapp_cache_bytes.unwrap();
        assert!(measured > 0);

        std::fs::remove_dir_all(&webapp).unwrap();
        write_file(&webapp, 8);
        t.refresh_webapp_cache();
        assert_eq!(t.stats().webapp_cache_bytes.unwrap(), measured);
    }

    /// A directory that does not exist is a measurement of zero, not a failure:
    /// that is the state of a store not yet created.
    #[test]
    fn a_missing_directory_measures_zero_rather_than_retaining() {
        let dir = tempfile::tempdir().unwrap();
        let db = dir.path().join("db");
        write_file(&db.join("db"), 256 * 1024);

        let t = DiskUsageTracker::new(HostingDiskPaths {
            db_dir: db.clone(),
            ..nonexistent_paths()
        });
        t.seed(std::iter::empty());
        assert!(t.stats().db_file_bytes.unwrap() > 0);

        std::fs::remove_dir_all(&db).unwrap();
        t.refresh_db_file();
        assert_eq!(
            t.stats().db_file_bytes.unwrap(),
            0,
            "gone means zero, not retained"
        );
    }

    /// A directory that fails and then disappears re-arms the warning: the
    /// `NotFound` arm clears the edge flag as well as reporting 0.
    #[test]
    fn a_failed_then_missing_directory_measures_zero_and_rearms_the_warning() {
        let dir = tempfile::tempdir().unwrap();
        let db = dir.path().join("db");
        write_file(&db, 8); // ENOTDIR
        let t = DiskUsageTracker::new(HostingDiskPaths {
            db_dir: db.clone(),
            ..nonexistent_paths()
        });
        t.seed(std::iter::empty());
        assert!(t.db_file_measure_failed.load(Ordering::Relaxed));

        std::fs::remove_file(&db).unwrap();
        t.refresh_db_file();
        assert_eq!(t.stats().db_file_bytes, Some(0));
        assert!(!t.db_file_measure_failed.load(Ordering::Relaxed));

        // And the next failure is flagged (warned) again.
        write_file(&db, 8);
        t.refresh_db_file();
        assert!(t.db_file_measure_failed.load(Ordering::Relaxed));
        assert_eq!(t.stats().db_file_bytes, Some(0), "retains the last value");
    }

    /// A database file symlinked from another disk is counted at its target's
    /// size; a symlinked directory is not descended into.
    #[cfg(unix)]
    #[test]
    fn db_measurement_follows_a_symlinked_database_file() {
        let dir = tempfile::tempdir().unwrap();
        let elsewhere = dir.path().join("elsewhere");
        let db = dir.path().join("db");
        write_file(&elsewhere.join("db"), 64 * 1024);
        write_file(&elsewhere.join("sub").join("big"), 256 * 1024);
        std::fs::create_dir_all(&db).unwrap();
        std::os::unix::fs::symlink(elsewhere.join("db"), db.join("db")).unwrap();
        std::os::unix::fs::symlink(elsewhere.join("sub"), db.join("sub")).unwrap();

        let t = DiskUsageTracker::new(HostingDiskPaths {
            db_dir: db,
            ..nonexistent_paths()
        });
        t.seed(std::iter::empty());
        assert_eq!(
            t.stats().db_file_bytes.unwrap(),
            alloc(&elsewhere.join("db"))
        );
    }

    /// A recoverable failure does not poison the gauge: once the directory is
    /// readable again the measurement resumes.
    #[test]
    fn measurement_resumes_after_a_transient_failure() {
        let dir = tempfile::tempdir().unwrap();
        let db = dir.path().join("db");
        write_file(&db.join("db"), 256 * 1024);

        let t = DiskUsageTracker::new(HostingDiskPaths {
            db_dir: db.clone(),
            ..nonexistent_paths()
        });
        t.seed(std::iter::empty());
        let first = t.stats().db_file_bytes.unwrap();

        std::fs::remove_dir_all(&db).unwrap();
        write_file(&db, 8);
        t.refresh_db_file();
        assert_eq!(t.stats().db_file_bytes.unwrap(), first);

        std::fs::remove_file(&db).unwrap();
        write_file(&db.join("db"), 512 * 1024);
        let grown = alloc(&db.join("db"));
        assert!(grown > first);
        t.refresh_db_file();
        assert_eq!(t.stats().db_file_bytes.unwrap(), grown);
    }

    /// `db_dir` in Network mode contains the `local/` subtree, whose bytes
    /// belong to the other operation mode. Files beside the database (a
    /// migration `.backup`, sqlite `-wal`) do count.
    #[test]
    fn db_measurement_is_shallow_and_skips_the_other_mode_split() {
        let dir = tempfile::tempdir().unwrap();
        let db = dir.path().join("db");
        write_file(&db.join("db"), 100);
        write_file(&db.join("db.backup"), 25);
        write_file(&db.join("local").join("db"), 64 * 1024);

        let t = DiskUsageTracker::new(HostingDiskPaths {
            db_dir: db.clone(),
            ..nonexistent_paths()
        });
        let siblings = alloc(&db.join("db")) + alloc(&db.join("db.backup"));
        assert!(alloc(&db.join("local").join("db")) > 0);
        t.seed(std::iter::empty());
        assert_eq!(t.stats().db_file_bytes.unwrap(), siblings);
    }

    #[test]
    fn refresh_db_file_observes_growth_between_sweeps() {
        let dir = tempfile::tempdir().unwrap();
        let db = dir.path().join("db");
        write_file(&db.join("db"), 200);

        let t = DiskUsageTracker::new(HostingDiskPaths {
            db_dir: db.clone(),
            ..nonexistent_paths()
        });
        let seeded = alloc(&db.join("db"));
        t.seed(std::iter::empty());
        assert_eq!(t.stats().db_file_bytes.unwrap(), seeded);

        write_file(&db.join("db"), 256 * 1024);
        let grown = alloc(&db.join("db"));
        assert!(grown > seeded, "fixture must actually grow the allocation");
        t.refresh_db_file();
        assert_eq!(t.stats().db_file_bytes.unwrap(), grown);
    }

    #[test]
    fn webapp_cache_is_measured_and_reported() {
        let dir = tempfile::tempdir().unwrap();
        let webapp = dir.path().join("webapp_cache");
        write_file(&webapp.join("aaaa").join("index.html"), 300);
        write_file(&webapp.join("bbbb").join("app.js"), 200);

        let t = DiskUsageTracker::new(HostingDiskPaths {
            webapp_cache_dir: webapp.clone(),
            ..nonexistent_paths()
        });
        let seeded = alloc(&webapp.join("aaaa").join("index.html"))
            + alloc(&webapp.join("bbbb").join("app.js"));
        t.seed(std::iter::empty());
        assert_eq!(t.stats().webapp_cache_bytes.unwrap(), seeded);

        write_file(&webapp.join("cccc").join("app.js"), 50);
        let grown = seeded + alloc(&webapp.join("cccc").join("app.js"));
        t.refresh_webapp_cache();
        assert_eq!(t.stats().webapp_cache_bytes.unwrap(), grown);
    }

    /// Four same-typed paths, each of which must feed exactly one gauge.
    /// Distinct sizes, differing by whole blocks, make any swap visible.
    #[test]
    fn all_four_measured_dirs_are_read_from_their_own_path() {
        const BLOCKS: usize = 64 * 1024;
        let dir = tempfile::tempdir().unwrap();
        let contracts = dir.path().join("contracts");
        let wasmtime = dir.path().join("wasmtime");
        let db = dir.path().join("db");
        let webapp = dir.path().join("webapp");
        write_file(&contracts.join("aaaa.wasm"), BLOCKS);
        write_file(&wasmtime.join("mod.cache"), 2 * BLOCKS);
        write_file(&db.join("db"), 3 * BLOCKS);
        write_file(&webapp.join("aaaa").join("index.html"), 4 * BLOCKS);
        // Budgeted walks: apparent length. Reported walks: allocation.
        let wasm = BLOCKS as u64;
        let compile = 2 * BLOCKS as u64;
        let dbf = alloc(&db.join("db"));
        let web = alloc(&webapp.join("aaaa").join("index.html"));
        assert!(dbf >= 3 * BLOCKS as u64 && web >= 4 * BLOCKS as u64);
        assert!(dbf < web, "fixture sizes must stay distinct");

        let t = DiskUsageTracker::new(HostingDiskPaths {
            contracts_dir: contracts,
            wasmtime_cache_dir: wasmtime,
            db_dir: db,
            webapp_cache_dir: webapp,
        });
        t.seed(std::iter::empty());
        let s = t.stats();
        assert_eq!(s.wasm_bytes, wasm);
        assert_eq!(s.compile_cache_bytes, compile);
        assert_eq!(s.db_file_bytes.unwrap(), dbf);
        assert_eq!(s.webapp_cache_bytes.unwrap(), web);
        assert_eq!(
            s.total_bytes,
            wasm + compile,
            "reported gauges not budgeted"
        );
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
