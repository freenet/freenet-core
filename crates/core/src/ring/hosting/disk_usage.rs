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

/// Point-in-time on-disk usage gauges, one snapshot for telemetry.
///
/// Aggregate scalars only, emitted on the existing `RouterSnapshot` cadence
/// alongside the RAM-budget gauges so the disk-budget feature is observable in
/// production alongside the eviction floor and admission gate it feeds (#4702).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub(crate) struct DiskUsageStats {
    /// Persisted contract-state bytes (delta-tracked, seeded from redb rows).
    /// The *logical* total: `db_file_bytes − state_bytes` is the database's dead
    /// space, which is why both are reported separately (#5007).
    pub state_bytes: u64,
    /// Measured byte size of the storage backend's database directory (#5007).
    /// 0 before the first sweep measurement.
    pub db_file_bytes: u64,
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
    /// Measured byte size of the unpacked-webapp cache (#5007). Refreshed on the
    /// same sweep. REPORTED ONLY — never part of [`Self::total_bytes`]; see the
    /// module docs for why double-bounding it would be wrong.
    webapp_cache_bytes: AtomicU64,
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
            webapp_cache_bytes: AtomicU64::new(0),
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
    pub(crate) fn stats(&self) -> DiskUsageStats {
        let state_bytes = self.state_bytes.load(Ordering::Relaxed);
        let db_file_bytes = self.db_file_bytes.load(Ordering::Relaxed);
        let wasm_bytes = self.wasm_bytes.load(Ordering::Relaxed);
        let compile_cache_bytes = self.compile_cache_bytes.load(Ordering::Relaxed);
        DiskUsageStats {
            state_bytes,
            db_file_bytes,
            wasm_bytes,
            compile_cache_bytes,
            webapp_cache_bytes: self.webapp_cache_bytes.load(Ordering::Relaxed),
            total_bytes: state_bytes
                .max(db_file_bytes)
                .saturating_add(wasm_bytes)
                .saturating_add(compile_cache_bytes),
        }
    }

    /// Seed the state-bytes counter and per-key size index from an exact list of
    /// `(contract, state_size)` pairs (the caller reads these from redb rows so
    /// this module stays storage-backend-agnostic and unit-testable). Also runs
    /// the initial WASM, compile-cache, database-file and webapp-cache
    /// measurements, so every gauge is meaningful the moment `is_seeded` flips
    /// (the telemetry and budget paths both gate on that flag).
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
        let old = sizes.get(key).copied().unwrap_or(0);
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
    pub(crate) fn refresh_db_file(&self) {
        self.db_file_bytes
            .store(du_walk_shallow(&self.paths.db_dir), Ordering::Relaxed);
    }

    /// Re-measure the unpacked-webapp cache (#5007).
    ///
    /// Recursive: the cache is a tree of unpacked bundles. REPORTED ONLY — the
    /// result never enters [`Self::total_bytes`]. Measuring it is the fix for
    /// the blind spot (nothing measured or reported these bytes at all);
    /// charging them to the hosting budget would double-bound something #5012
    /// already caps at 64 MiB, using a lever hosting eviction does not have.
    /// See the module docs.
    pub(crate) fn refresh_webapp_cache(&self) {
        self.webapp_cache_bytes
            .store(du_walk(&self.paths.webapp_cache_dir), Ordering::Relaxed);
    }

    /// Free bytes on the mount holding the tracked `contracts_dir` — the
    /// `available` term the disk budget sizes against (#4683). `None` when the
    /// platform query fails; the caller falls back to `u64::MAX`. The contracts
    /// dir shares the data-dir mount, which is where all tracked bytes (state,
    /// wasm, relocated compile cache) land, so it is the correct mount to probe.
    pub(crate) fn available_bytes(&self) -> Option<u64> {
        available_bytes(&self.paths.contracts_dir)
    }
}

/// Sum the byte size of every regular file **directly inside** `dir`, without
/// descending into subdirectories (#5007).
///
/// The non-recursive counterpart to [`du_walk`], for the database directory:
/// `db_dir` in `Network` mode contains the `local/` mode split, whose bytes
/// belong to a different node instance and must not be charged here. Same
/// best-effort error handling as [`du_walk`] — a missing directory or an
/// unreadable entry contributes 0.
fn du_walk_shallow(dir: &Path) -> u64 {
    let Ok(entries) = std::fs::read_dir(dir) else {
        return 0;
    };
    let mut total: u64 = 0;
    for entry in entries.flatten() {
        let Ok(file_type) = entry.file_type() else {
            continue;
        };
        if file_type.is_file() {
            if let Ok(meta) = entry.metadata() {
                total = total.saturating_add(meta.len());
            }
        }
    }
    total
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

    /// Write a file of `len` zero bytes at `path`, creating parents.
    fn write_file(path: &Path, len: usize) {
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent).unwrap();
        }
        let mut f = std::fs::File::create(path).unwrap();
        f.write_all(&vec![0u8; len]).unwrap();
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
            db_dir: db,
            ..nonexistent_paths()
        });
        // Logical rows total only 400 — the pre-#5007 figure.
        t.seed([(test_key(1), 300), (test_key(2), 100)]);
        assert_eq!(t.stats().state_bytes, 400, "logical rows still reported");
        assert_eq!(t.stats().db_file_bytes, 4000);
        assert_eq!(
            t.total_bytes(),
            4000,
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
            db_dir: db,
            ..nonexistent_paths()
        });
        t.seed([(test_key(1), 600), (test_key(2), 400)]);
        assert_eq!(t.total_bytes(), 1000);

        // Shed everything. The file did not shrink, so neither does the figure.
        t.record_state_removed(&test_key(1));
        t.record_state_removed(&test_key(2));
        assert_eq!(t.stats().state_bytes, 0);
        assert_eq!(
            t.total_bytes(),
            1000,
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
            db_dir: db,
            ..nonexistent_paths()
        });
        t.seed(std::iter::empty());
        assert_eq!(t.total_bytes(), 500, "measured file dominates while idle");

        // A write burst pushes live bytes past the last measurement; the file
        // must have grown at least that far, so the live figure takes over.
        t.record_state_write(&test_key(1), 900);
        assert_eq!(t.total_bytes(), 900);
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
            db_dir: db,
            ..nonexistent_paths()
        });
        t.seed(std::iter::empty());
        assert_eq!(
            t.stats().db_file_bytes,
            125,
            "shallow walk counts siblings of the database but not the local/ split"
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
        t.seed(std::iter::empty());
        assert_eq!(t.total_bytes(), 200);

        // The database grows (copy-on-write dead space, new pages, whatever) —
        // the next sweep's re-measure must pick it up.
        write_file(&db.join("db"), 1500);
        t.refresh_db_file();
        assert_eq!(t.stats().db_file_bytes, 1500);
        assert_eq!(t.total_bytes(), 1500);
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
            db_dir: db,
            ..nonexistent_paths()
        });
        t.seed(std::iter::empty()); // zero logical rows
        assert!(
            t.admit_state_write(&test_key(1), 1, 1000).is_err(),
            "a full database must refuse a fresh PUT even with no rows tracked"
        );
        // Sanity: the same call against the pre-#5007 figure would have admitted.
        let blind = tracker();
        blind.seed(std::iter::empty());
        assert!(blind.admit_state_write(&test_key(1), 1, 1000).is_ok());
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
        t.seed(std::iter::empty());
        assert_eq!(t.stats().webapp_cache_bytes, 500);

        write_file(&webapp.join("cccc").join("app.js"), 50);
        t.refresh_webapp_cache();
        assert_eq!(t.stats().webapp_cache_bytes, 550);
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
            webapp_cache_dir: webapp,
            ..nonexistent_paths()
        });
        t.seed([(test_key(1), 100)]);
        assert_eq!(t.stats().webapp_cache_bytes, 4096, "measured");
        assert_eq!(t.total_bytes(), 100, "but not budgeted");
        assert_eq!(t.stats().total_bytes, 100);
        // ...so it cannot influence the admission gate either.
        assert!(t.admit_state_write(&test_key(2), 100, 200).is_ok());
    }

    #[test]
    fn all_four_measured_dirs_are_read_from_their_own_path() {
        // Guards the transposition hazard `HostingDiskPaths` exists to make
        // unlikely: four same-typed paths, each of which must feed exactly one
        // gauge. Distinct sizes make any swap visible.
        let dir = tempfile::tempdir().unwrap();
        let contracts = dir.path().join("contracts");
        let wasmtime = dir.path().join("wasmtime");
        let db = dir.path().join("db");
        let webapp = dir.path().join("webapp");
        write_file(&contracts.join("aaaa.wasm"), 11);
        write_file(&wasmtime.join("mod.cache"), 22);
        write_file(&db.join("db"), 33);
        write_file(&webapp.join("aaaa").join("index.html"), 44);

        let t = DiskUsageTracker::new(HostingDiskPaths {
            contracts_dir: contracts,
            wasmtime_cache_dir: wasmtime,
            db_dir: db,
            webapp_cache_dir: webapp,
        });
        t.seed(std::iter::empty());
        let s = t.stats();
        assert_eq!(s.wasm_bytes, 11);
        assert_eq!(s.compile_cache_bytes, 22);
        assert_eq!(s.db_file_bytes, 33);
        assert_eq!(s.webapp_cache_bytes, 44);
        // Budgeted aggregate = max(state=0, db=33) + wasm + compile cache.
        assert_eq!(s.total_bytes, 33 + 11 + 22);
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
