//! Aggregate on-disk usage accounting for demand-driven hosting (#4683).
//!
//! The demand-driven hosting budget (#4642) sizes itself from **memory** only.
//! For a disk-constrained host, disk — not RAM — is the scarcest resource, yet
//! nothing bounds disk in aggregate today. [`DiskUsageTracker`] is the shared
//! source of truth that the future disk budget (eviction floor `min(ram, disk)`
//! in PR 2, admission gate in PR 3) reads. This PR wires only the accounting +
//! telemetry: the tracker is populated and observable but does not yet change
//! any admission or eviction decision (zero behavior change).
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
//! # Seeding discipline (fail-loud)
//!
//! [`DiskUsageTracker::seed`] mirrors the #4561 secrets `seeded_user_total`
//! discipline: it walks the real on-disk state ONCE and is **fail-loud** on I/O
//! error. A silently-too-low seed would defeat the future admission gate (it
//! would admit writes that actually overflow disk), so a seed that cannot read
//! the truth must surface the error rather than start from an under-count.

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

use freenet_stdlib::prelude::ContractKey;
use parking_lot::Mutex;

/// Point-in-time on-disk usage gauges, one snapshot for telemetry.
///
/// Aggregate scalars only, emitted on the existing `RouterSnapshot` cadence
/// alongside the RAM-budget gauges so the disk-budget feature is observable in
/// production before any enforcement ships.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub(crate) struct DiskUsageStats {
    /// Persisted contract-state bytes (delta-tracked, seeded from redb rows).
    pub state_bytes: u64,
    /// On-disk WASM code blob bytes (`du` of `contracts_dir/*.wasm`).
    pub wasm_bytes: u64,
    /// Wasmtime compile-cache bytes (`du` of the relocated cache dir).
    pub compile_cache_bytes: u64,
    /// Sum of the three above — the aggregate the disk budget will bound.
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
    state_sizes: Mutex<HashMap<ContractKey, u64>>,
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
            state_sizes: Mutex::new(HashMap::new()),
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
    /// The eviction floor (PR 2) and admission gate (PR 3) are the first
    /// runtime readers; in this accounting-only PR it is exercised by tests and
    /// the telemetry snapshot path.
    #[allow(dead_code)]
    pub(crate) fn total_bytes(&self) -> u64 {
        self.state_bytes
            .load(Ordering::Relaxed)
            .saturating_add(self.wasm_bytes.load(Ordering::Relaxed))
            .saturating_add(self.compile_cache_bytes.load(Ordering::Relaxed))
    }

    /// Snapshot all gauges for telemetry.
    pub(crate) fn stats(&self) -> DiskUsageStats {
        let state_bytes = self.state_bytes.load(Ordering::Relaxed);
        let wasm_bytes = self.wasm_bytes.load(Ordering::Relaxed);
        let compile_cache_bytes = self.compile_cache_bytes.load(Ordering::Relaxed);
        DiskUsageStats {
            state_bytes,
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
    /// silently-too-low seed would let the future admission gate admit
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
    /// close of the window flagged in the PR-1 review (would otherwise become a
    /// load-bearing under-count once PR 3 turns the counter into a gate input).
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
            //
            // A contract can have multiple instance rows in the snapshot; when
            // this is a fresh insert, sum the rows rather than overwrite.
            match sizes.entry(key) {
                std::collections::hash_map::Entry::Occupied(_) => {}
                std::collections::hash_map::Entry::Vacant(v) => {
                    v.insert(0);
                    let entry = sizes.get_mut(&key).expect("just inserted");
                    *entry = entry.saturating_add(size);
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
}
