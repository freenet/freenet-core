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
//! - **Storage-backend footprint** — `max(state_bytes, db_file_bytes)`, see
//!   "Logical state vs the database file" below.
//! - **WASM code blobs** — the `*.wasm` files under `contracts_dir`. Re-walked
//!   (`du`) on seed and on each telemetry refresh; blobs dedupe by `code_hash`
//!   so a re-PUT of already-stored code adds nothing.
//! - **Wasmtime compile cache** — wasmtime writes it opaquely, so it is not
//!   delta-tracked; it is re-walked on each telemetry refresh. Cheap: bounded by
//!   the number of distinct compiled modules and self-pruned by wasmtime at its
//!   soft-size limit.
//!
//! A fourth consumer, the **unpacked-webapp cache**, is measured and REPORTED
//! but deliberately excluded from the budgeted aggregate — see "The webapp
//! cache is reported, not budgeted".
//!
//! # Logical state vs the database file (#5007)
//!
//! Until #5007 the storage term was the *logical* state total alone: the sum of
//! every hosted contract's serialized state bytes, seeded from
//! [`HostingMetadata::size_bytes`](crate::contract::storages::HostingMetadata)
//! rows and thereafter maintained by signed deltas at the executor's state-write
//! chokepoints (via [`super::HostingManager::record_state_write`]) and at
//! reclamation (via [`super::HostingManager::record_state_removed`]). A small
//! per-key size index makes those deltas exact without re-reading the DB.
//!
//! That figure under-counted the real footprint by roughly **10x** on a
//! long-lived peer: redb is copy-on-write and only ever truncates *trailing*
//! free space, so the interior dead space a busy node accumulates is invisible
//! to any row-level accounting. Measured on a production peer (2026-07): 583 MB
//! reported against 2.68 GB actually occupied.
//!
//! So the storage term is now `max(state_bytes, db_file_bytes)`, where
//! `db_file_bytes` is a **measurement** of the database directory taken on the
//! existing 60s sweep ([`DiskUsageTracker::refresh_db_file`]). The two halves
//! answer different questions and the `max` is what makes them compose:
//!
//! - `db_file_bytes` is exact but stale by up to one sweep window, and it is the
//!   only thing that can see dead space. It dominates in steady state.
//! - `state_bytes` is live and exact at the write chokepoint. It dominates only
//!   when a burst of writes within one sweep window has already pushed live
//!   bytes past the last measured file size — in which case the file must have
//!   grown at least that far, so the `max` is still a sound lower bound.
//!
//! Two properties follow, both load-bearing:
//!
//! 1. **Eviction cannot fictitiously shrink the figure.** Deleting rows frees
//!    redb pages for *reuse* without shrinking the file, so a decomposition like
//!    `state + (file − state)` would let a shedding node watch its own overhead
//!    term inflate to exactly cancel the state it just freed — an eviction
//!    treadmill that never converges. Under `max`, shedding state simply leaves
//!    the figure at the measured file size, which is the truth.
//! 2. **An unmeasured database degrades to the pre-#5007 behavior.**
//!    `db_file_bytes` is 0 until the first measurement (and stays 0 for a
//!    tracker configured with no database directory, e.g. unit tests), and
//!    `max(state, 0) == state`.
//!
//! The measurement is periodic rather than delta-tracked because a file length
//! is not something a write chokepoint can compute: redb extends the file when
//! it needs a new page, not per row, so a per-write `metadata()` would be a
//! syscall on the PUT hot path (under the `state_sizes` lock, which
//! [`DiskUsageTracker::admit_state_write`] holds across
//! [`DiskUsageTracker::total_bytes`]) in exchange for no extra accuracy. The
//! live `state_bytes` delta already captures the marginal cost of an in-flight
//! write; the sweep reconciles it against ground truth once a minute.
//!
//! The gap between the two — `db_file_bytes − state_bytes` — is the span of the
//! already-allocated file that contract state does not occupy. It is not merely
//! a reporting curiosity: growth that fits inside it costs no disk at all,
//! which is why [`DiskUsageTracker::admit_state_update`] admits such an UPDATE
//! even on an over-budget node. Refusing it would freeze a hosted contract's
//! state while the node kept serving and advertising it (invariant 1's stale
//! host) in exchange for zero bytes saved.
//!
//! That span is **not** the database's dead space, and this module is careful
//! not to call it that. It also holds every live non-state row — contract
//! params, hosting metadata, the contract and delegate indices,
//! broken-invariants, the two secrets indices, delegate origins, reserved
//! marker hashes, the compaction marker — plus all B-tree overhead. None of
//! that is reclaimable. The reclaimable figure needs the backend's own
//! allocator: [`DiskUsageStats::db_in_use_bytes`] is probed on the same sweep
//! and [`reclaimable_db_bytes`] is `file − in_use`, which is what a compaction
//! actually returns. Everything operator-facing (the over-budget warning's
//! restart advice, the home-page tooltip, the dashboard snapshot) reports the
//! reclaimable figure; only the UPDATE gate, which needs "room inside the
//! current file" rather than "room a restart frees", uses the wider span.
//!
//! # How a file is measured (allocated, not apparent)
//!
//! Every walk charges a file's **allocated blocks**, not its apparent length —
//! see [`file_disk_bytes`]. redb's file is sparse on some peers (measured:
//! 2.56 GiB apparent against 1.71 GiB allocated on the production gateway), so
//! charging the apparent length over-states the dominant term by up to ~50% and
//! makes the admission gate refuse writes the disk can still take. Allocation is
//! also the unit `available` is measured in (`statvfs` free blocks), so the two
//! terms of the budget's basis have to agree.
//!
//! A walk that CANNOT read its directory keeps the gauge's previous value and
//! warns, instead of recording 0 — see [`DiskUsageTracker::store_measurement`].
//! Recording 0 for an unreadable database directory would silently revert the
//! aggregate to the exact under-count this module exists to fix, and would look
//! like a healthy small node in telemetry.
//!
//! # The webapp cache is reported, not budgeted (#5007)
//!
//! The unpacked-webapp cache (XDG cache dir, `~/.cache/freenet/webapp_cache`)
//! was also entirely absent — measured at 1236 MiB / 82 entries on one peer.
//! [`DiskUsageTracker::refresh_webapp_cache`] now measures it on the same sweep
//! and [`DiskUsageStats::webapp_cache_bytes`] reports it, but it is NOT part of
//! [`DiskUsageTracker::total_bytes`]. Three reasons, all of which have to hold:
//!
//! - **It is already bounded.** `WEBAPP_CACHE_MAX_BYTES` (64 MiB, #5012) is a
//!   hard LRU cap. Charging a hard-capped 64 MiB against a hosting budget whose
//!   own floor is 128 MiB would spend up to half a small node's entire hosting
//!   allowance re-bounding something already bounded.
//! - **Hosting eviction cannot act on it.** The eviction sweep sheds hosted
//!   contracts; it has no lever on webapp entries. Charging a term the
//!   enforcement mechanism cannot move is the same failure shape as charging
//!   redb dead space, and it is why property 1 above matters.
//! - **It may not even be on the same mount.** The disk budget's basis is
//!   `used + available` where `available` is a `statvfs` of the *contracts-dir*
//!   mount. XDG cache and XDG data are the same mount on the standard layout but
//!   are not required to be, so folding cache bytes into that basis can be
//!   dimensionally wrong.
//!
//! Visibility was the actual defect for these bytes: nothing measured or
//! reported them. That is fixed; double-bounding them is not.
//!
//! # Seeding discipline (fail-loud)
//!
//! [`DiskUsageTracker::seed`] mirrors the #4561 secrets `seeded_user_total`
//! discipline: it walks the real on-disk state ONCE and is **fail-loud** on I/O
//! error. A silently-too-low seed would defeat the admission gate (it
//! would admit writes that actually overflow disk), so a seed that cannot read
//! the truth must surface the error rather than start from an under-count.

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

use freenet_stdlib::prelude::ContractKey;
use parking_lot::Mutex;

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
    /// (`db` for redb, `freenet.db` for sqlite) plus any sidecars.
    pub db_dir: PathBuf,
    /// Unpacked-webapp cache root (XDG cache dir). Measured and reported, NOT
    /// charged against the hosting budget — see the module docs.
    pub webapp_cache_dir: PathBuf,
}

/// Not-yet-measured sentinel for the database's in-use byte count (#5007
/// follow-up). Same idiom as `HostingManager::disk_budget_bytes`'s `u64::MAX`
/// start: 16 EiB is not a size any database reaches, so the sentinel cannot
/// collide with a real reading, and it keeps "never probed" distinguishable
/// from a genuine 0 (which would render the entire file as reclaimable).
const DB_IN_USE_UNMEASURED: u64 = u64::MAX;

/// Point-in-time on-disk usage gauges, one snapshot for telemetry.
///
/// Aggregate scalars only, emitted on the existing `RouterSnapshot` cadence
/// alongside the RAM-budget gauges so the disk-budget feature is observable in
/// production alongside the eviction floor and admission gate it feeds (#4702).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub(crate) struct DiskUsageStats {
    /// Persisted contract-state bytes (delta-tracked, seeded from redb rows).
    /// The *logical* total: `db_file_bytes − state_bytes` is the span of the
    /// file that state does not occupy, which is why both are reported
    /// separately (#5007). That span is NOT the reclaimable dead space — see
    /// [`Self::db_in_use_bytes`] and [`reclaimable_db_bytes`].
    pub state_bytes: u64,
    /// Measured byte size of the storage backend's database directory (#5007).
    /// 0 before the first sweep measurement.
    pub db_file_bytes: u64,
    /// Bytes the storage backend's page allocator reports as actually IN USE
    /// (#5007 follow-up). `db_file_bytes − db_in_use_bytes` is the database's
    /// true dead space: the part a compaction can reclaim.
    ///
    /// `None` until the first successful probe, and permanently on a backend
    /// that cannot report it (sqlite). Callers must not substitute
    /// `state_bytes` for it: `db_file_bytes − state_bytes` is "everything in
    /// the file that is not state payload", which structurally includes every
    /// live non-state row and all B-tree overhead, and is therefore NOT what a
    /// restart reclaims.
    pub db_in_use_bytes: Option<u64>,
    /// On-disk WASM code blob bytes (`du` of `contracts_dir/*.wasm`).
    pub wasm_bytes: u64,
    /// Wasmtime compile-cache bytes (`du` of the relocated cache dir).
    pub compile_cache_bytes: u64,
    /// Measured byte size of the unpacked-webapp cache (#5007). REPORTED ONLY —
    /// deliberately excluded from `total_bytes`; see the module docs.
    pub webapp_cache_bytes: u64,
    /// `max(state_bytes, db_file_bytes) + wasm_bytes + compile_cache_bytes` —
    /// the aggregate the disk budget bounds. Excludes `webapp_cache_bytes`.
    pub total_bytes: u64,
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
    state_sizes: Mutex<HashMap<ContractKey, u64>>,
    /// Measured byte size of the storage backend's database directory (#5007).
    /// Refreshed by a shallow walk on the 60s sweep, never delta-tracked (a file
    /// length is not computable at a write chokepoint). 0 until the first
    /// measurement, which makes [`Self::total_bytes`] degrade exactly to the
    /// pre-#5007 logical-state sum.
    db_file_bytes: AtomicU64,
    /// Bytes the storage backend's page allocator reports as actually in use
    /// (#5007 follow-up), probed on the same 60s sweep. `u64::MAX` is the
    /// not-yet-measured sentinel — the same idiom `disk_budget_bytes` uses, and
    /// safe because no database can be 16 EiB. Stays at the sentinel forever on
    /// a backend that cannot report it.
    ///
    /// REPORTED ONLY: it is deliberately not an input to [`Self::total_bytes`]
    /// or to any admission gate. The budget bounds the node's real FOOTPRINT,
    /// which is the file, not the part of the file that is live — a database
    /// with 2 GB of dead space occupies 2 GB of the operator's disk whether or
    /// not a compaction could give it back. This gauge exists so the dead-space
    /// figure the operator is shown, and the restart advice attached to it, are
    /// true.
    db_in_use_bytes: AtomicU64,
    /// Measured byte size of the unpacked-webapp cache (#5007). Refreshed on the
    /// same sweep. REPORTED ONLY — never part of [`Self::total_bytes`]; see the
    /// module docs for why double-bounding it would be wrong.
    webapp_cache_bytes: AtomicU64,
    /// Edge-trigger flags for "the last `du`-walk of this gauge failed", one per
    /// measured gauge (#5007 follow-up). A failed walk keeps the gauge's
    /// previous value and warns ONCE, instead of silently recording 0 and
    /// reverting the aggregate to the pre-#5007 under-count. See
    /// [`Self::store_measurement`]; separate flags so a warning about one
    /// directory cannot suppress a warning about another.
    wasm_measure_failed: AtomicBool,
    compile_cache_measure_failed: AtomicBool,
    db_file_measure_failed: AtomicBool,
    db_in_use_measure_failed: AtomicBool,
    webapp_cache_measure_failed: AtomicBool,
    /// The directories all of the above are measured from.
    paths: HostingDiskPaths,
}

impl DiskUsageTracker {
    /// Create an unseeded tracker. All counters start at zero; call
    /// [`Self::seed`] once before the counts are meaningful.
    pub(crate) fn new(paths: HostingDiskPaths) -> Self {
        Self {
            state_bytes: AtomicU64::new(0),
            wasm_bytes: AtomicU64::new(0),
            compile_cache_bytes: AtomicU64::new(0),
            seeded: AtomicBool::new(false),
            state_sizes: Mutex::new(HashMap::new()),
            db_file_bytes: AtomicU64::new(0),
            db_in_use_bytes: AtomicU64::new(DB_IN_USE_UNMEASURED),
            webapp_cache_bytes: AtomicU64::new(0),
            wasm_measure_failed: AtomicBool::new(false),
            compile_cache_measure_failed: AtomicBool::new(false),
            db_file_measure_failed: AtomicBool::new(false),
            db_in_use_measure_failed: AtomicBool::new(false),
            webapp_cache_measure_failed: AtomicBool::new(false),
            paths,
        }
    }

    /// Whether [`Self::seed`] has already run successfully.
    pub(crate) fn is_seeded(&self) -> bool {
        self.seeded.load(Ordering::Acquire)
    }

    /// The storage backend's real on-disk footprint (#5007):
    /// `max(state_bytes, db_file_bytes)`.
    ///
    /// Neither term alone is right. `db_file_bytes` is the only one that can see
    /// the database's dead space but is stale by up to one sweep window;
    /// `state_bytes` is live and exact at the write chokepoint but blind to
    /// everything except serialized row payloads. Taking the larger keeps the
    /// measured file as the floor (so shedding rows can never fictitiously
    /// shrink the figure — redb reuses freed pages rather than truncating) while
    /// still tracking a within-window write burst that has already pushed live
    /// bytes past the last measurement. See the module docs.
    fn storage_bytes(&self) -> u64 {
        self.state_bytes
            .load(Ordering::Relaxed)
            .max(self.db_file_bytes.load(Ordering::Relaxed))
    }

    /// Aggregate on-disk bytes = storage footprint + wasm + compile-cache. The
    /// value the disk budget bounds. Cheap (four atomic loads).
    ///
    /// Read live by the eviction floor
    /// ([`super::HostingManager::recompute_effective_budget`]) and the pre-write
    /// admission gate ([`Self::admit_state_write`]), both wired in #4702, plus the
    /// telemetry snapshot path.
    ///
    /// Excludes the webapp cache on purpose (#5007) — it is measured and
    /// reported via [`Self::stats`], but hosting eviction has no lever on it and
    /// #5012 already caps it. See the module docs.
    pub(crate) fn total_bytes(&self) -> u64 {
        self.storage_bytes()
            .saturating_add(self.wasm_bytes.load(Ordering::Relaxed))
            .saturating_add(self.compile_cache_bytes.load(Ordering::Relaxed))
    }

    /// Snapshot all gauges for telemetry.
    ///
    /// `total_bytes` delegates to [`Self::total_bytes`] rather than re-deriving
    /// the aggregate, so the budgeted formula has exactly one definition and a
    /// future change to it cannot land in one place and not the other.
    pub(crate) fn stats(&self) -> DiskUsageStats {
        DiskUsageStats {
            state_bytes: self.state_bytes.load(Ordering::Relaxed),
            db_file_bytes: self.db_file_bytes.load(Ordering::Relaxed),
            db_in_use_bytes: self.db_in_use_bytes(),
            wasm_bytes: self.wasm_bytes.load(Ordering::Relaxed),
            compile_cache_bytes: self.compile_cache_bytes.load(Ordering::Relaxed),
            webapp_cache_bytes: self.webapp_cache_bytes.load(Ordering::Relaxed),
            total_bytes: self.total_bytes(),
        }
    }

    /// Seed the state-bytes counter and per-key size index from an exact list of
    /// `(contract, state_size)` pairs (the caller reads these from redb rows so
    /// this module stays storage-backend-agnostic and unit-testable). Also runs
    /// the initial WASM, compile-cache, database-file and webapp-cache
    /// measurements, so the gauges the telemetry and budget paths read are
    /// populated by the time this returns rather than waiting for the first 60s
    /// sweep.
    ///
    /// The four measurements run AFTER the `seeded` flag flips, so a concurrent
    /// reader can briefly observe `is_seeded() == true` with the `du`-measured
    /// gauges still 0. That window is permissive, not blocking: the admission
    /// gate compares against `disk_budget_bytes`, which is `u64::MAX` until the
    /// first `recompute_effective_budget`, and that recompute runs after this
    /// seed in the same sweep iteration. The walks are deliberately not moved
    /// ahead of the flag — they must run outside the `state_sizes` lock, and
    /// measuring first would open the opposite window in which a WASM blob
    /// written between the walk and the flag is missed by both the walk and the
    /// (still-unseeded, therefore skipped) delta in
    /// [`Self::record_wasm_write`], which is an under-count of a gate input
    /// rather than an over-permissive gauge.
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
            match sizes.entry(key) {
                std::collections::hash_map::Entry::Occupied(_) => {}
                std::collections::hash_map::Entry::Vacant(v) => {
                    v.insert(size);
                }
            }
        }
        // Recompute the aggregate from the merged map so buffered concurrent
        // writes are reflected exactly.
        let total = sizes
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
        let old = sizes.get(key).copied().unwrap_or(0);
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
    /// aggregate is already over budget.
    ///
    /// # Growth absorbed inside the existing file is also admitted (#5007)
    ///
    /// Genuine growth still is not automatically a *disk* cost. redb reuses
    /// freed pages rather than truncating, so a node whose file already exceeds
    /// its live state (`db_file_bytes − state_bytes`, the gap #5007 made
    /// visible) absorbs growth up to that gap without the file growing by a
    /// single byte. This gate deliberately uses that WIDER span rather than the
    /// narrower reclaimable dead space ([`reclaimable_db_bytes`]): the question
    /// here is "does the file have to move", not "what would a compaction give
    /// back", and the file does not move for growth into any already-allocated
    /// page. The operator-facing reports use the narrower figure, because a
    /// restart claim can only be made about that one. The
    /// honest projection of the storage term is therefore
    /// `max(db_file_bytes, state_bytes − old + new)` — NOT
    /// `max(db_file_bytes, state_bytes) − old + new`, which charges the delta on
    /// top of a file that will not move.
    ///
    /// This matters because of what refusing costs. A rejected UPDATE aborts the
    /// write while the node keeps the contract, keeps serving it, and keeps
    /// advertising as a host — a copy that has dropped out of the update mesh
    /// but still answers reads, which `hosting-invariants.md` invariant 1 says
    /// must be impossible by construction. And for a relayed UPDATE the
    /// rejection is fire-and-forget: no `UpdateMsg::Error`, so no peer learns.
    /// Growth is the normal case for the flagship contract class (a River room
    /// appends messages), and #5007 widens the over-budget population by making
    /// `used` honest. Refusing a merge that consumes no disk would manufacture
    /// stale hosts and buy nothing — eviction has no lever on dead space either,
    /// so there is no shed that would have made room.
    ///
    /// So the bound applies only to growth that genuinely enlarges the
    /// footprint. When the projected storage term exceeds the current one, the
    /// increment is real and is subjected to the aggregate budget.
    ///
    /// Two bounded imprecisions, both self-correcting on the next 60s
    /// re-measurement: `state_bytes` counts state payload only, so
    /// `db_file_bytes − state_bytes` also includes live non-state rows (params,
    /// hosting metadata, indices, B-tree overhead) and slightly over-states what
    /// is truly reclaimable; and `db_file_bytes` is up to one sweep window
    /// stale.
    ///
    /// **Residual:** growth that exceeds the dead space on an over-budget node
    /// is still refused, and that stale-host window stays open. That case is
    /// genuine disk exhaustion where admitting the write would overflow the
    /// budget; closing it means de-registering hosting and re-homing the
    /// contract's subscribers (epic #4642, piece D), not widening this gate.
    ///
    /// Read-only, same deferred-`+delta` discipline and inclusive-admit boundary
    /// as [`Self::admit_state_write`].
    pub(crate) fn admit_state_update(
        &self,
        key: &ContractKey,
        new_size: u64,
        budget_bytes: u64,
    ) -> Result<(), DiskBudgetExceeded> {
        // Hold the delta-path lock so the `old` read and the storage reads are a
        // consistent snapshot (same rationale as `admit_state_write`).
        let sizes = self.state_sizes.lock();
        let old = sizes.get(key).copied().unwrap_or(0);
        // Non-positive delta (shrink or hold) never blocks convergence.
        if new_size <= old {
            return Ok(());
        }
        let state = self.state_bytes.load(Ordering::Relaxed);
        let db_file = self.db_file_bytes.load(Ordering::Relaxed);
        drop(sizes);

        let current_storage = state.max(db_file);
        let projected_storage = state
            .saturating_sub(old)
            .saturating_add(new_size)
            .max(db_file);
        // The growth fits inside the database file that is already on disk and
        // already counted: it costs no new bytes, so there is nothing for the
        // budget to protect and refusing would only stall convergence.
        if projected_storage <= current_storage {
            return Ok(());
        }

        let projected = projected_storage
            .saturating_add(self.wasm_bytes.load(Ordering::Relaxed))
            .saturating_add(self.compile_cache_bytes.load(Ordering::Relaxed));
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

    /// Store the result of one `du`-walk, distinguishing **"measured 0"** from
    /// **"could not measure"** (#5007 follow-up).
    ///
    /// Every walk in this module used to swallow its `read_dir` error and return
    /// 0. That is a silent regression to the pre-#5007 under-count on the term
    /// that now dominates the aggregate: a single EACCES (a packaging or user
    /// change), a transient EMFILE on a busy node, or the directory being
    /// replaced would set `db_file_bytes = 0`, `max(state, 0) == state` would
    /// revert the whole figure to exactly the number #5007 exists to fix, and
    /// telemetry would show a perfectly plausible `hosting_disk_db_bytes: 0`
    /// with no log to contradict it. Under EACCES it is permanent and
    /// indistinguishable from a healthy small node.
    ///
    /// So a failed walk leaves the gauge at its **previous** value and warns.
    /// This is the same fail-loud discipline the state seed already follows (see
    /// "Seeding discipline" in the module docs) — a measurement that cannot read
    /// the truth must surface that rather than publish an under-count.
    ///
    /// `NotFound` is NOT a failure: a directory that does not exist holds no
    /// bytes, and that is the legitimate state for a store that has not been
    /// created yet (and for unit-test trackers pointed at nonexistent paths).
    ///
    /// The warning is edge-triggered on a per-gauge flag, so a node whose
    /// database directory is permanently unreadable logs once rather than once
    /// per 60s sweep forever, and logs again if the condition recurs after a
    /// recovery.
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
                        retained_bytes = gauge.load(Ordering::Relaxed),
                        "could not measure a hosting disk-usage directory; keeping \
                         the previous measurement. Recording 0 here would silently \
                         restore the pre-#5007 under-count and read as a healthy \
                         small node"
                    );
                }
            }
        }
    }

    /// Re-measure the on-disk WASM blob total by `du`-walking `contracts_dir`.
    /// Cheap and re-run on the telemetry cadence; deduping is inherent (each
    /// distinct `code_hash` is one file). A walk that fails keeps the previous
    /// value — see [`Self::store_measurement`].
    pub(crate) fn refresh_wasm(&self) {
        self.store_measurement(
            &self.wasm_bytes,
            &self.wasm_measure_failed,
            &self.paths.contracts_dir,
            "wasm_blobs",
            du_walk_wasm,
        );
    }

    /// Re-measure the wasmtime compile-cache total by `du`-walking its dir.
    /// Wasmtime writes the cache opaquely, so this re-walk (not a delta) is the
    /// only way to account for it. A walk that fails keeps the previous value —
    /// see [`Self::store_measurement`].
    pub(crate) fn refresh_compile_cache(&self) {
        self.store_measurement(
            &self.compile_cache_bytes,
            &self.compile_cache_measure_failed,
            &self.paths.wasmtime_cache_dir,
            "compile_cache",
            du_walk,
        );
    }

    /// Re-measure the storage backend's database footprint (#5007).
    ///
    /// A **shallow** walk of `db_dir`, not a recursive one, and that is
    /// deliberate: in `Network` mode `db_dir` is `<data>/db`, which *contains*
    /// the `local/` subtree belonging to the other operation mode. Those bytes
    /// are not this node's storage and must not be charged to its budget. The
    /// shallow walk still picks up every file the backend itself writes beside
    /// the database — redb's `db` plus any `.backup` left by a version
    /// migration, sqlite's `freenet.db` plus its `-wal`/`-shm` sidecars — so it
    /// is backend-agnostic where a hardcoded filename would not be.
    ///
    /// Cheap: one `read_dir` over a directory holding a handful of entries,
    /// versus the recursive walks the wasm and compile-cache gauges already do
    /// on the same 60s cadence.
    ///
    /// A walk that FAILS keeps the previous value and warns rather than
    /// recording 0: this is the dominant term, and a phantom 0 here reverts the
    /// whole aggregate to the pre-#5007 under-count silently. See
    /// [`Self::store_measurement`].
    pub(crate) fn refresh_db_file(&self) {
        self.store_measurement(
            &self.db_file_bytes,
            &self.db_file_measure_failed,
            &self.paths.db_dir,
            "database_file",
            du_walk_shallow,
        );
    }

    /// Bytes the storage backend's allocator reports as actually in use, or
    /// `None` while the sentinel is still in place (never probed, or a backend
    /// that cannot report it).
    pub(crate) fn db_in_use_bytes(&self) -> Option<u64> {
        let raw = self.db_in_use_bytes.load(Ordering::Relaxed);
        (raw != DB_IN_USE_UNMEASURED).then_some(raw)
    }

    /// Record the result of one in-use probe (#5007 follow-up).
    ///
    /// Same fail-loud discipline as [`Self::store_measurement`]: a failed probe
    /// keeps the previous value and warns once (edge-triggered), rather than
    /// recording something that reads as a plausible measurement. Here the
    /// stakes are lower than for `db_file_bytes` — nothing budgets on this
    /// gauge — but a wrong value still puts a wrong number in front of an
    /// operator being told to restart their node, so it gets the same
    /// treatment.
    ///
    /// A backend that cannot report in-use bytes simply never calls this, and
    /// the sentinel keeps the figure reported as unknown instead of as zero
    /// (which would render the whole file as reclaimable dead space).
    pub(crate) fn store_db_in_use_bytes(&self, measured: Result<u64, String>) {
        match measured {
            Ok(bytes) => {
                self.db_in_use_bytes.store(bytes, Ordering::Relaxed);
                self.db_in_use_measure_failed
                    .store(false, Ordering::Relaxed);
            }
            Err(reason) => {
                if !self.db_in_use_measure_failed.swap(true, Ordering::Relaxed) {
                    tracing::warn!(
                        measurement = "database_in_use",
                        %reason,
                        retained_bytes = ?self.db_in_use_bytes(),
                        "could not read the storage backend's in-use byte count; \
                         keeping the previous value. The reported database dead \
                         space is stale until this recovers"
                    );
                }
            }
        }
    }

    /// Re-measure the unpacked-webapp cache (#5007).
    ///
    /// Recursive: the cache is a tree of unpacked bundles. REPORTED ONLY — the
    /// result never enters [`Self::total_bytes`]. Measuring it is the fix for
    /// the blind spot (nothing measured or reported these bytes at all);
    /// charging them to the hosting budget would double-bound something #5012
    /// already caps at 64 MiB, using a lever hosting eviction does not have.
    /// See the module docs. A walk that fails keeps the previous value — see
    /// [`Self::store_measurement`].
    pub(crate) fn refresh_webapp_cache(&self) {
        self.store_measurement(
            &self.webapp_cache_bytes,
            &self.webapp_cache_measure_failed,
            &self.paths.webapp_cache_dir,
            "webapp_cache",
            du_walk,
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

    /// Test-only: install a measured database-file size directly.
    ///
    /// Budget arithmetic has to be exercised across footprints up to tens of
    /// gigabytes, which no fixture can materialize on disk. It cannot be faked
    /// with a sparse file either: since the walks measure allocated blocks, a
    /// `set_len` file measures 0 and would make such a fixture silently vacuous.
    #[cfg(test)]
    pub(crate) fn set_db_file_bytes_for_test(&self, bytes: u64) {
        self.db_file_bytes.store(bytes, Ordering::Relaxed);
    }

    /// Test-only: install an allocator in-use figure directly, so the reporting
    /// mappings can be driven with a value distinct from every other gauge. The
    /// real probe (backend → tracker) is covered end-to-end against a real
    /// database in `hosting.rs`.
    #[cfg(test)]
    pub(crate) fn set_db_in_use_bytes_for_test(&self, bytes: u64) {
        self.db_in_use_bytes.store(bytes, Ordering::Relaxed);
    }
}

/// The bytes of the database directory that no live page occupies: the measured
/// size minus what the backend's allocator reports as in use (#5007 follow-up).
///
/// This is an UPPER BOUND on what a restart's compaction returns, not an
/// equality. `db_file_bytes` is a shallow walk of the whole directory, so an
/// orphaned `db.backup.<timestamp>` left by a schema migration
/// (`redb.rs::backup_and_remove_database`) is counted in it, is not a live page
/// of the current database, and therefore lands inside this figure — while
/// being precisely what a compaction can never touch. Nothing in the tree
/// deletes that file; only `rm` does. Reporting sites must route an operator
/// with a backup file to deletion and one with genuine live dead space to a
/// restart, rather than promising the whole figure back from either.
///
/// `None` when the in-use figure is unknown (never probed, or a backend that
/// cannot report it). The tempting substitute — `db_file_bytes − state_bytes` —
/// is NOT this figure: `state_bytes` counts contract-state payload only, so the
/// difference structurally includes every live non-state row (`contract_params`,
/// `hosting_metadata`, the contract/delegate indices, `broken_invariants`, the
/// secrets indices, delegate origins, reserved-marker hashes, the compaction
/// marker) plus all B-tree overhead. Telling an operator that whole span is
/// reclaimable — and that a restart will reclaim it — is wrong for the live
/// portion, which is why this helper exists and why the reporting sites use it
/// rather than the subtraction.
///
/// Free function rather than a method so the reporting sites (`hosting.rs`'s
/// over-budget warning, the home-page card, the dashboard snapshot) all derive
/// it the same way from a plain [`DiskUsageStats`].
pub(crate) fn reclaimable_db_bytes(stats: &DiskUsageStats) -> Option<u64> {
    stats
        .db_in_use_bytes
        .map(|in_use| stats.db_file_bytes.saturating_sub(in_use))
}

/// The disk a single file actually consumes: its **allocated blocks**, not its
/// apparent length (#5007 follow-up).
///
/// `Metadata::len()` is what `ls` reports — the file's logical extent. For a
/// **sparse** file that over-states consumption, because the holes occupy no
/// blocks at all. redb's database file is sparse on some peers: measured on the
/// production gateway (2026-07-29, stable across repeated samples),
/// `len()` reported 2,749,370,368 bytes against 1,839,210,496 bytes actually
/// allocated — a 910 MB / 49% over-charge on the term that now dominates the
/// aggregate. (The same file on another peer was not sparse at all, so this
/// varies with the peer's compaction history and cannot be corrected by a
/// constant.) Over-charging makes the admission gate refuse writes the disk can
/// still take: the mirror image of the under-count #5007 fixed, and no more
/// honest.
///
/// Allocated blocks are also the only unit that composes with the budget's other
/// term. The disk budget is `pct * (used + available)` where `available` is
/// `statvfs`'s `f_bavail * f_frsize` — real free blocks. `used` has to be in the
/// same currency, so block slack on a small file IS charged (the filesystem
/// genuinely spent that block, and `available` already reflects it) and a hole
/// is not. This makes the module's `du`-walk naming true: `du` reports
/// allocation, `ls` reports length.
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
/// The non-recursive counterpart to [`du_walk`], for the database directory:
/// `db_dir` in `Network` mode contains the `local/` mode split, whose bytes
/// belong to a different node instance and must not be charged here.
///
/// Returns `Err` when `dir` itself cannot be read. That is deliberate and is the
/// difference between "measured 0" and "could not measure": see
/// [`DiskUsageTracker::store_measurement`]. An individual unreadable entry
/// inside a readable directory still contributes 0 (best effort).
fn du_walk_shallow(dir: &Path) -> std::io::Result<u64> {
    let mut total: u64 = 0;
    for entry in std::fs::read_dir(dir)? {
        let Ok(entry) = entry else {
            continue;
        };
        let Ok(file_type) = entry.file_type() else {
            continue;
        };
        if file_type.is_file() {
            if let Ok(meta) = entry.metadata() {
                total = total.saturating_add(file_disk_bytes(&meta));
            }
        }
    }
    Ok(total)
}

/// Recursively sum the allocated size of every regular file under `dir`.
///
/// `Err` when the ROOT `dir` cannot be read (the caller keeps its previous
/// measurement rather than recording a phantom 0); an unreadable *sub*directory
/// or entry contributes 0, since a partial tree is still a better estimate than
/// discarding the whole measurement.
fn du_walk(dir: &Path) -> std::io::Result<u64> {
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

/// Like [`du_walk`] but only counts `*.wasm` files — the code-blob subset of
/// `contracts_dir` (which also holds the `local/` mode split). Directory
/// traversal is recursive so both the network and `local/` blobs are counted.
/// Same root-vs-subtree error contract as [`du_walk`].
fn du_walk_wasm(dir: &Path) -> std::io::Result<u64> {
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
                let p = entry.path();
                if p.extension().and_then(|e| e.to_str()) == Some("wasm") {
                    if let Ok(meta) = entry.metadata() {
                        total = total.saturating_add(file_disk_bytes(&meta));
                    }
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

    /// Paths that all resolve to nothing, so every `du`-walk contributes 0.
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

    /// Write a file of `len` bytes at `path`, creating parents.
    ///
    /// The content is a cheap pseudo-random stream, not zeros, so a filesystem
    /// with transparent compression (btrfs `compress`, ZFS) cannot collapse the
    /// fixture to a fraction of its blocks and quietly change what the walks
    /// measure. The walks charge ALLOCATED blocks, so fixture content matters.
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

    /// The disk `path` actually occupies, re-derived independently of the code
    /// under test.
    ///
    /// Fixture files are small, so their allocated size is the filesystem's
    /// block size rather than the byte count written; and the block size varies
    /// by filesystem, so a hard-coded expectation would be wrong somewhere. The
    /// walk tests below therefore assert *which files were counted*, and
    /// [`file_disk_bytes`]'s own rule — allocated, not apparent — is pinned
    /// separately by `sparse_file_is_charged_by_allocation_not_apparent_length`
    /// and `block_slack_on_a_small_file_is_charged`.
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
        let blobs =
            alloc(&contracts.join("aaaa.wasm")) + alloc(&contracts.join("local").join("bbbb.wasm"));
        t.seed(std::iter::empty());
        assert_eq!(
            t.stats().wasm_bytes,
            blobs,
            "both blobs counted, the non-wasm sibling not"
        );
        // Re-walking is deduped by construction (same files) — idempotent.
        t.refresh_wasm();
        assert_eq!(t.stats().wasm_bytes, blobs);
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

    /// Growth that fits inside the database's dead space is admitted even when
    /// the node is over budget, because it costs no disk.
    ///
    /// redb reuses freed pages rather than truncating, so a write that keeps
    /// total live state at or below the measured file size does not grow the
    /// file by a byte. Refusing it would abort the merge while the node kept
    /// serving and advertising the contract — invariant 1's stale host — and
    /// would free nothing, since eviction has no lever on dead space either.
    #[test]
    fn admit_state_update_growth_absorbed_by_dead_space_is_admitted_over_budget() {
        let t = tracker();
        t.seed([(test_key(1), 100)]);
        // 1000-byte file holding 100 bytes of live rows: 900 bytes of dead space.
        t.set_db_file_bytes_for_test(1000);
        assert_eq!(t.total_bytes(), 1000);

        // Well over budget: a fresh PUT is refused, as it should be.
        assert!(t.admit_state_write(&test_key(2), 1, 500).is_err());

        // The same node must still merge a growing UPDATE that fits in the dead
        // space: live state goes 100 → 900, the file does not move.
        assert!(
            t.admit_state_update(&test_key(1), 900, 500).is_ok(),
            "growth inside the dead space costs no disk and must not be refused"
        );
        // Right up to the file size.
        assert!(t.admit_state_update(&test_key(1), 1000, 500).is_ok());
    }

    /// The exemption stops exactly where the dead space does: growth that would
    /// push live state past the measured file DOES enlarge the footprint, and is
    /// held to the budget like any other growth.
    #[test]
    fn admit_state_update_growth_beyond_dead_space_is_still_bounded() {
        let t = tracker();
        t.seed([(test_key(1), 100)]);
        t.set_db_file_bytes_for_test(1000);

        let err = t
            .admit_state_update(&test_key(1), 1001, 1000)
            .expect_err("growth past the file must be charged");
        assert_eq!(
            err.projected_bytes, 1001,
            "projection is max(file, state − old + new), not the file plus the delta"
        );
        assert_eq!(err.budget_bytes, 1000);
        // …and is admitted when the budget has room for the real increment.
        assert!(t.admit_state_update(&test_key(1), 1001, 1001).is_ok());
    }

    /// The projection must not charge the delta on top of a file that will not
    /// move. Before this rule, `total − old + new` against a file-dominated
    /// total over-charged every growing UPDATE by its full delta.
    #[test]
    fn admit_state_update_does_not_charge_growth_twice_against_the_file() {
        let t = tracker();
        t.seed([(test_key(1), 100)]);
        t.set_db_file_bytes_for_test(1000);
        // Budget exactly at the measured file. Growing the key by 1 byte inside
        // 900 bytes of dead space must not be projected past it.
        assert!(
            t.admit_state_update(&test_key(1), 101, 1000).is_ok(),
            "the file is the footprint; the delta lands inside it"
        );
    }

    /// The exemption is scoped to UPDATEs. A fresh PUT on an over-budget node is
    /// still refused: admitting new tenants is the decision the budget exists to
    /// make, and a PUT has no already-hosted state whose merge would stall.
    #[test]
    fn dead_space_does_not_exempt_a_fresh_put() {
        let t = tracker();
        t.seed(std::iter::empty());
        t.set_db_file_bytes_for_test(1000);
        assert!(t.admit_state_write(&test_key(1), 1, 500).is_err());
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
        let nested = alloc(&cache.join("sub").join("mod.cache"));
        t.seed(std::iter::empty());
        assert_eq!(
            t.stats().compile_cache_bytes,
            nested,
            "the walk descends into subdirectories"
        );

        // Grow the cache; a refresh must observe the new total.
        let mut g = std::fs::File::create(cache.join("mod2.cache")).unwrap();
        g.write_all(&[0u8; 88]).unwrap();
        let both = nested + alloc(&cache.join("mod2.cache"));
        t.refresh_compile_cache();
        assert_eq!(t.stats().compile_cache_bytes, both);
        assert_eq!(t.total_bytes(), both);
    }

    // --- How a file is measured: allocation, not apparent length -------------

    /// A SPARSE file must be charged for the blocks it actually occupies, not
    /// for the length `ls` reports.
    ///
    /// Measured on the production gateway (2026-07-29): the redb database
    /// reported 2,749,370,368 bytes of apparent length against 1,839,210,496
    /// bytes allocated — a 910 MB / 49% over-charge on the term that dominates
    /// the aggregate. Over-charging makes the admission gate refuse writes the
    /// disk can still take, which is the same class of dishonesty as the
    /// under-count #5007 fixed, pointing the other way. (The same file on
    /// another peer was not sparse at all, so no constant correction exists.)
    #[cfg(unix)]
    #[test]
    fn sparse_file_is_charged_by_allocation_not_apparent_length() {
        const APPARENT: u64 = 64 * 1024 * 1024;
        let dir = tempfile::tempdir().unwrap();
        let db = dir.path().join("db");
        std::fs::create_dir_all(&db).unwrap();
        // A hole, not data: length without allocation, exactly the shape redb's
        // file takes on the peers where this was measured.
        std::fs::File::create(db.join("db"))
            .unwrap()
            .set_len(APPARENT)
            .unwrap();
        assert_eq!(
            std::fs::metadata(db.join("db")).unwrap().len(),
            APPARENT,
            "fixture must have the large apparent length we are testing against"
        );

        let t = DiskUsageTracker::new(HostingDiskPaths {
            db_dir: db.clone(),
            ..nonexistent_paths()
        });
        t.seed(std::iter::empty());
        let measured = t.stats().db_file_bytes;
        assert_eq!(
            measured,
            alloc(&db.join("db")),
            "the walk must report the allocated size"
        );
        assert!(
            measured < APPARENT / 2,
            "a hole occupies no disk, so charging it would over-state the \
             dominant term (measured {measured} against {APPARENT} apparent)"
        );
    }

    /// The other direction of the same rule: a file smaller than one filesystem
    /// block still consumes a whole block, and the budget's `available` term
    /// (`statvfs` free blocks) has already accounted for it. Charging the
    /// apparent length would under-state usage against a basis measured in
    /// blocks.
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
        assert_eq!(t.stats().db_file_bytes, alloc(&db.join("db")));
        assert!(
            t.stats().db_file_bytes > 1,
            "a one-byte file occupies a whole block, and `available` already \
             reflects that block being gone"
        );
    }

    // --- A failed measurement is not a measurement of zero -------------------

    /// A `du`-walk that CANNOT read its directory must keep the gauge's previous
    /// value, not record 0.
    ///
    /// Recording 0 for the database directory reverts `max(state, 0) == state`
    /// to exactly the pre-#5007 under-count, silently, with a perfectly
    /// plausible `hosting_disk_db_bytes: 0` in telemetry and nothing to
    /// contradict it. Under a persistent EACCES it is permanent and looks
    /// identical to a healthy small node.
    ///
    /// The failure is injected by replacing the directory with a FILE (ENOTDIR),
    /// which fails for root as well — a permissions-based fixture would silently
    /// pass by measuring successfully when the suite runs as root.
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
        let measured = t.stats().db_file_bytes;
        assert!(
            measured > 0,
            "fixture must measure something to then retain"
        );

        // The directory becomes unreadable (ENOTDIR here; EACCES/EMFILE in
        // production).
        std::fs::remove_dir_all(&db).unwrap();
        write_file(&db, 8);
        assert!(std::fs::read_dir(&db).is_err(), "fixture must make it fail");

        t.refresh_db_file();
        assert_eq!(
            t.stats().db_file_bytes,
            measured,
            "a failed measurement must not be published as zero"
        );
        assert_eq!(
            t.total_bytes(),
            measured,
            "and so the aggregate must not collapse to the pre-#5007 row sum"
        );
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
        let measured = t.stats().webapp_cache_bytes;
        assert!(measured > 0);

        std::fs::remove_dir_all(&webapp).unwrap();
        write_file(&webapp, 8);
        t.refresh_webapp_cache();
        assert_eq!(t.stats().webapp_cache_bytes, measured);
    }

    /// A directory that does not exist is a legitimate measurement of zero, NOT
    /// a failure: that is the state of a store not yet created, and of every
    /// unit-test tracker pointed at nonexistent paths. Retaining a stale value
    /// there would be its own kind of lie.
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
        assert!(t.stats().db_file_bytes > 0);

        std::fs::remove_dir_all(&db).unwrap();
        t.refresh_db_file();
        assert_eq!(t.stats().db_file_bytes, 0, "gone means zero, not retained");
    }

    /// A recoverable failure must not poison the gauge: once the directory is
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
        let first = t.stats().db_file_bytes;

        std::fs::remove_dir_all(&db).unwrap();
        write_file(&db, 8);
        t.refresh_db_file();
        assert_eq!(t.stats().db_file_bytes, first);

        // Readable again, and larger.
        std::fs::remove_file(&db).unwrap();
        write_file(&db.join("db"), 512 * 1024);
        let grown = alloc(&db.join("db"));
        assert!(grown > first);
        t.refresh_db_file();
        assert_eq!(t.stats().db_file_bytes, grown);
    }

    // --- Database-file measurement (#5007) -----------------------------------

    #[test]
    fn db_file_size_supersedes_logical_state_rows() {
        // The reported bug: the tracker summed logical state rows and was blind
        // to the database's own footprint, under-counting ~10x on a production
        // peer (583 MB reported vs 2.68 GB occupied). With the file measured,
        // the aggregate reflects the file, not the rows.
        let dir = tempfile::tempdir().unwrap();
        let db = dir.path().join("db");
        write_file(&db.join("db"), 4000);

        let t = DiskUsageTracker::new(HostingDiskPaths {
            db_dir: db.clone(),
            ..nonexistent_paths()
        });
        let file = alloc(&db.join("db"));
        // Logical rows total only 400 — the pre-#5007 figure.
        t.seed([(test_key(1), 300), (test_key(2), 100)]);
        assert_eq!(t.stats().state_bytes, 400, "logical rows still reported");
        assert_eq!(t.stats().db_file_bytes, file);
        assert_eq!(
            t.total_bytes(),
            file,
            "aggregate must reflect the database file, not the row sum"
        );
    }

    #[test]
    fn evicting_state_cannot_shrink_the_aggregate_below_the_measured_file() {
        // The property that keeps eviction from chasing its own tail. redb frees
        // pages for REUSE and never truncates interior dead space, so a
        // decomposition that re-derived "overhead" as `file − state` would
        // inflate the overhead by exactly what eviction just freed, leaving the
        // aggregate pinned above budget no matter how much the node sheds. Under
        // `max(state, file)` the aggregate simply stays at the measured file
        // size — which is the truth, and is a fixed point rather than a
        // treadmill.
        let dir = tempfile::tempdir().unwrap();
        let db = dir.path().join("db");
        write_file(&db.join("db"), 1000);

        let t = DiskUsageTracker::new(HostingDiskPaths {
            db_dir: db.clone(),
            ..nonexistent_paths()
        });
        let file = alloc(&db.join("db"));
        t.seed([(test_key(1), 600), (test_key(2), 400)]);
        assert_eq!(t.total_bytes(), file);

        // Shed everything. The file did not shrink, so neither does the figure.
        t.record_state_removed(&test_key(1));
        t.record_state_removed(&test_key(2));
        assert_eq!(t.stats().state_bytes, 0);
        assert_eq!(
            t.total_bytes(),
            file,
            "shedding rows must not fictitiously reduce the measured footprint"
        );
    }

    #[test]
    fn within_window_state_growth_exceeds_a_stale_db_measurement() {
        // The other half of the `max`: the file measurement is up to one sweep
        // window stale, so a burst of writes must still be visible to the
        // admission gate before the next measurement lands.
        let dir = tempfile::tempdir().unwrap();
        let db = dir.path().join("db");
        write_file(&db.join("db"), 500);

        let t = DiskUsageTracker::new(HostingDiskPaths {
            db_dir: db.clone(),
            ..nonexistent_paths()
        });
        let file = alloc(&db.join("db"));
        t.seed(std::iter::empty());
        assert_eq!(t.total_bytes(), file, "measured file dominates while idle");

        // A write burst pushes live bytes past the last measurement; the file
        // must have grown at least that far, so the live figure takes over.
        t.record_state_write(&test_key(1), file + 400);
        assert_eq!(t.total_bytes(), file + 400);
    }

    #[test]
    fn db_measurement_is_shallow_and_skips_the_other_mode_split() {
        // `db_dir` in Network mode contains the `local/` subtree, whose bytes
        // belong to a different node instance. A recursive walk would charge
        // them here.
        let dir = tempfile::tempdir().unwrap();
        let db = dir.path().join("db");
        write_file(&db.join("db"), 100);
        // Sidecars/backups written beside the database DO count (sqlite's
        // `-wal`, redb's migration `.backup`).
        write_file(&db.join("db.backup"), 25);
        write_file(&db.join("local").join("db"), 9999);

        let t = DiskUsageTracker::new(HostingDiskPaths {
            db_dir: db.clone(),
            ..nonexistent_paths()
        });
        let siblings = alloc(&db.join("db")) + alloc(&db.join("db.backup"));
        t.seed(std::iter::empty());
        assert_eq!(
            t.stats().db_file_bytes,
            siblings,
            "shallow walk counts siblings of the database but not the local/ split"
        );
        assert!(
            t.stats().db_file_bytes < siblings + alloc(&db.join("local").join("db")),
            "the local/ split must be excluded, and the fixture must make its \
             inclusion visible"
        );
    }

    #[test]
    fn absent_db_measurement_degrades_to_the_logical_row_sum() {
        // Backward-compat floor: a tracker with no readable database directory
        // (unit tests, a node whose store has not been created yet) behaves
        // exactly as it did before #5007. `max(state, 0) == state`.
        let t = tracker();
        t.seed([(test_key(1), 700)]);
        assert_eq!(t.stats().db_file_bytes, 0);
        assert_eq!(t.total_bytes(), 700);
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
        assert_eq!(t.total_bytes(), seeded);

        // The database grows (copy-on-write dead space, new pages, whatever) —
        // the next sweep's re-measure must pick it up. The growth spans several
        // filesystem blocks so it is visible whatever the block size is.
        write_file(&db.join("db"), 256 * 1024);
        let grown = alloc(&db.join("db"));
        assert!(grown > seeded, "fixture must actually grow the allocation");
        t.refresh_db_file();
        assert_eq!(t.stats().db_file_bytes, grown);
        assert_eq!(t.total_bytes(), grown);
    }

    #[test]
    fn admission_gate_charges_the_database_file_not_just_the_rows() {
        // The consumer that made the under-count load-bearing (#4702). With a
        // 1000-byte database and a 1000-byte budget the node is exactly full,
        // so any fresh PUT must be refused — even though the logical rows say
        // there is a full budget's worth of headroom.
        let dir = tempfile::tempdir().unwrap();
        let db = dir.path().join("db");
        write_file(&db.join("db"), 1000);

        let t = DiskUsageTracker::new(HostingDiskPaths {
            db_dir: db.clone(),
            ..nonexistent_paths()
        });
        // Budget == the measured file, so the node is exactly full.
        let budget = alloc(&db.join("db"));
        t.seed(std::iter::empty()); // zero logical rows
        assert!(
            t.admit_state_write(&test_key(1), 1, budget).is_err(),
            "a full database must refuse a fresh PUT even with no rows tracked"
        );
        // Sanity: the same call against the pre-#5007 figure would have admitted.
        let blind = tracker();
        blind.seed(std::iter::empty());
        assert!(blind.admit_state_write(&test_key(1), 1, budget).is_ok());
    }

    // --- Webapp cache: measured and reported, never budgeted (#5007) ----------

    #[test]
    fn webapp_cache_is_measured_and_reported() {
        // The second blind spot: these bytes were not measured at all (1236 MiB
        // across 82 entries on a real peer). They are now.
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
        assert_eq!(t.stats().webapp_cache_bytes, seeded);

        write_file(&webapp.join("cccc").join("app.js"), 50);
        let grown = seeded + alloc(&webapp.join("cccc").join("app.js"));
        t.refresh_webapp_cache();
        assert_eq!(t.stats().webapp_cache_bytes, grown);
    }

    #[test]
    fn webapp_cache_never_enters_the_budgeted_aggregate() {
        // Visibility without enforcement. #5012 already caps this directory at
        // 64 MiB with its own LRU, hosting eviction has no lever on it, and it
        // may sit on a different mount than the one the budget's free-space term
        // measures. Charging it here would double-bound it AND reintroduce the
        // "over budget with nothing to shed" shape.
        let dir = tempfile::tempdir().unwrap();
        let webapp = dir.path().join("webapp_cache");
        write_file(&webapp.join("aaaa").join("index.html"), 4096);

        let t = DiskUsageTracker::new(HostingDiskPaths {
            webapp_cache_dir: webapp.clone(),
            ..nonexistent_paths()
        });
        let cached = alloc(&webapp.join("aaaa").join("index.html"));
        t.seed([(test_key(1), 100)]);
        assert_eq!(t.stats().webapp_cache_bytes, cached, "measured");
        assert!(cached > 0, "fixture must have measurable bytes to exclude");
        assert_eq!(t.total_bytes(), 100, "but not budgeted");
        assert_eq!(t.stats().total_bytes, 100);
        // ...so it cannot influence the admission gate either.
        assert!(t.admit_state_write(&test_key(2), 100, 200).is_ok());
    }

    #[test]
    fn all_four_measured_dirs_are_read_from_their_own_path() {
        // Guards the transposition hazard `HostingDiskPaths` exists to make
        // unlikely: four same-typed paths, each of which must feed exactly one
        // gauge. Distinct sizes make any swap visible — and they have to differ
        // by whole filesystem blocks, since the walks charge allocation, so a
        // few bytes apart would collapse to the same measurement.
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
        let wasm = alloc(&contracts.join("aaaa.wasm"));
        let compile = alloc(&wasmtime.join("mod.cache"));
        let dbf = alloc(&db.join("db"));
        let web = alloc(&webapp.join("aaaa").join("index.html"));
        assert_eq!(
            [wasm, compile, dbf, web]
                .iter()
                .collect::<std::collections::HashSet<_>>()
                .len(),
            4,
            "fixture sizes must stay distinct or a transposition would be invisible"
        );

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
        assert_eq!(s.db_file_bytes, dbf);
        assert_eq!(s.webapp_cache_bytes, web);
        // Budgeted aggregate = max(state=0, db) + wasm + compile cache.
        assert_eq!(s.total_bytes, dbf + wasm + compile);
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
    // --- Reclaimable dead space vs the file-minus-state span (#5007 follow-up)

    /// The two spans are DIFFERENT numbers, and only one of them is dead space.
    ///
    /// `db_file − state` is "everything in the file that is not contract-state
    /// payload": interior free pages, yes, but also every live non-state row
    /// (params, hosting metadata, the contract/delegate indices,
    /// broken-invariants, the two secrets indices, delegate origins, reserved
    /// marker hashes, the compaction marker) and all B-tree overhead. Reporting
    /// that as dead space, with "needs a node restart" attached, tells the
    /// operator a compaction will return bytes it cannot.
    ///
    /// `db_file − in_use` is what the allocator says is free, which is what a
    /// compaction actually returns.
    #[test]
    fn reclaimable_is_allocator_derived_not_file_minus_state() {
        let stats = DiskUsageStats {
            state_bytes: 100,
            db_file_bytes: 1000,
            // 700 of the file is LIVE — most of it non-state rows and overhead.
            db_in_use_bytes: Some(700),
            wasm_bytes: 0,
            compile_cache_bytes: 0,
            webapp_cache_bytes: 0,
            total_bytes: 1000,
        };
        assert_eq!(
            reclaimable_db_bytes(&stats),
            Some(300),
            "reclaimable dead space is file − in_use"
        );
        assert_ne!(
            reclaimable_db_bytes(&stats),
            Some(stats.db_file_bytes - stats.state_bytes),
            "and it is NOT file − state, which here would overstate it 3x"
        );
    }

    /// With no in-use reading there is no reclaimable figure — callers must
    /// report the gap rather than silently substituting `file − state`.
    #[test]
    fn reclaimable_is_unknown_without_an_in_use_reading() {
        let stats = DiskUsageStats {
            state_bytes: 100,
            db_file_bytes: 1000,
            db_in_use_bytes: None,
            wasm_bytes: 0,
            compile_cache_bytes: 0,
            webapp_cache_bytes: 0,
            total_bytes: 1000,
        };
        assert_eq!(reclaimable_db_bytes(&stats), None);
    }

    /// An in-use reading larger than the measured file (the walks charge
    /// ALLOCATED blocks, so a sparse database file can measure smaller than the
    /// allocator's logical page count) saturates to zero rather than wrapping.
    #[test]
    fn reclaimable_saturates_when_in_use_exceeds_the_measured_file() {
        let stats = DiskUsageStats {
            state_bytes: 10,
            db_file_bytes: 500,
            db_in_use_bytes: Some(900),
            wasm_bytes: 0,
            compile_cache_bytes: 0,
            webapp_cache_bytes: 0,
            total_bytes: 500,
        };
        assert_eq!(reclaimable_db_bytes(&stats), Some(0));
    }

    /// The in-use gauge is REPORTED, never budgeted: installing one must not
    /// move `total_bytes` or any admission decision. The budget bounds the
    /// node's real footprint (the file), and a database with dead space occupies
    /// that disk whether or not a compaction could give it back.
    #[test]
    fn in_use_bytes_does_not_enter_the_budgeted_aggregate() {
        let t = tracker();
        t.seed(std::iter::empty());
        t.set_db_file_bytes_for_test(1000);
        let before = t.total_bytes();
        t.set_db_in_use_bytes_for_test(1);
        assert_eq!(
            t.total_bytes(),
            before,
            "the in-use figure is reporting-only; it must not change the \
             aggregate the disk budget bounds"
        );
        assert_eq!(t.stats().db_in_use_bytes, Some(1), "but it IS reported");
    }
}
