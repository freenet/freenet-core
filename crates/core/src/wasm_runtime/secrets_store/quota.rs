//! Per-user on-disk secret-byte quota tracking for the hosted-mode
//! inactive-user TTL and storage cap (P5 of #4381, #4561).
//!
//! The [`QuotaTracker`] is a process-global singleton (via [`USER_QUOTA_TRACKER`])
//! so all pooled `SecretsStore` instances sharing a `secrets_dir` see the same
//! per-user counters.

use std::path::{Path, PathBuf};

use dashmap::DashMap;

use super::user::UserId;

/// Default per-user secret-storage quota: 4 MiB of on-disk ciphertext per
/// [`UserId`], summed across every delegate that user has secrets under
/// (#4561, P5 of #4381). Bounds a single hosted visitor's disk footprint so
/// they cannot fill the node's disk. Operator-overridable via
/// `--per-user-secret-quota=BYTES` / `per-user-secret-quota` in config; `0`
/// disables enforcement entirely.
pub const DEFAULT_PER_USER_SECRET_QUOTA_BYTES: usize = 4 * 1024 * 1024;

/// Default inactivity TTL for hosted users (#4561, P5 of #4381): 30 days in
/// seconds. After this much real-calendar inactivity a hosted user's entire
/// per-user footprint is reclaimed by the background sweep, keeping a public
/// "try Freenet" node a transient demo with bounded storage. Operator-
/// overridable via `--per-user-inactive-ttl=SECS` / `per-user-inactive-ttl`
/// in config; `0` disables the sweep. Single source of truth for the in-code
/// default so the operator-facing default and the config fallback never drift.
pub const DEFAULT_PER_USER_INACTIVE_TTL_SECS: u64 = 2_592_000;

/// Maximum users reclaimed in ONE sweep pass (#4561, clock-fault blast-radius
/// guard). The reclaim is destructive, and the per-user eligibility test trusts
/// wall-clock `now`; a forward clock jump (NTP correcting a bad RTC, a date
/// misconfig, a VM restored from a skewed snapshot) would make EVERY user's age
/// suddenly exceed the TTL, so an unbounded pass could silently delete the whole
/// user base at once. Capping the pass bounds that blast radius and — paired
/// with a LOUD warn when the cap is hit — surfaces the anomaly so an operator
/// can intervene before more is lost. Sized generously: a normal idle backlog
/// (hundreds of genuinely-abandoned users) drains over a few hourly sweeps,
/// while a clock fault can never wipe more than this many users per pass. The
/// remainder is simply deferred to the next sweep (the reclaim is idempotent and
/// crash-safe, so deferral is free).
pub const MAX_RECLAIMS_PER_SWEEP: usize = 256;

/// Multiplier on the sweep interval defining the largest plausible gap between
/// two consecutive sweeps (#4561, clock-fault detector). If wall-clock `now`
/// advanced by MORE than `sweep_interval * this` since the previous sweep, the
/// clock almost certainly jumped forward (or the node was suspended/restored
/// across a long gap) rather than the interval having elapsed normally, so the
/// sweep SKIPS reclaim for that pass and warns. This catches a forward jump
/// BETWEEN sweeps before ANY delete happens, and conservatively skips a single
/// pass after legitimate long downtime (the users restamp on reconnect and the
/// next pass proceeds normally). 8× an hourly interval is 8h — comfortably above
/// jitter/missed-tick delay, well below a real multi-day skew.
pub const SWEEP_MAX_GAP_INTERVAL_MULTIPLE: u64 = 8;

/// Process-wide tracker of per-[`UserId`] on-disk secret-byte totals.
///
/// # Why this is a process-global rather than a per-store field
///
/// Each pooled `Executor<Runtime>` builds its OWN [`super::store::SecretsStore`] with its
/// own in-memory index, but they all write to the SAME `secrets_dir`. A quota
/// counter living on one store would diverge across executors exactly the way
/// the per-executor secret index did before the export fix walked the disk
/// instead (see [`super::store::SecretsStore::walk_scope_secrets_on_disk`]). So the per-user
/// totals must live in ONE structure every executor's store references. A
/// process-global `LazyLock<DashMap>` is the established pattern in this
/// subsystem for precisely this "shared across all pooled executors" need —
/// see the several `LazyLock<DashMap<...>>` statics in `native_api.rs`. The
/// `UserId` key is a globally-unique 32-byte hash, so cross-node/cross-delegate
/// collisions are impossible and a single global namespace is correct.
///
/// # Concurrency — the no-CAS safety is NON-LOCAL
///
/// Delegate execution is SERIAL on the contract loop — only one `store_secret`
/// runs at a time node-wide — so the increment/decrement are never actually
/// contended and a plain map would suffice for correctness. The per-user value
/// is held as an [`AtomicU64`](std::sync::atomic::AtomicU64) as cheap insurance
/// (and so reads never need the `DashMap` write path), but no compare-and-swap
/// is required precisely because of that serial-execution guarantee.
///
/// WARNING: this safety rests on a property OUTSIDE this module. Two things
/// would race if delegate `process()` were ever offloaded to run concurrently
/// across pooled executors:
///
/// 1. the load-then-store decrement in [`Self::sub_saturating`] (lost update),
///    and
/// 2. the check→commit gap in `store_secret` — the quota admission check reads
///    the total, then the counter is bumped only AFTER the rename, so two
///    concurrent admissions could both pass against the same pre-write total
///    and jointly exceed the limit.
///
/// If delegate execution ever becomes concurrent, BOTH the decrement and the
/// admission/commit must move to a real compare-and-swap (or a per-user lock);
/// an `AtomicU64` alone is not enough. Keep this guarantee in mind before
/// parallelizing the contract loop.
///
/// # Keying — `(secrets_dir, UserId)`, NOT `UserId` alone
///
/// The counter is keyed by the OWNING STORE's `base_path` (its `secrets_dir`)
/// PLUS the `UserId`. Pooled executors of one node all build their stores
/// against the SAME `secrets_dir`, so they still share a user's counter (the
/// shared-counter property). But two INDEPENDENT stores with DIFFERENT
/// `base_path`s — e.g. several simulated nodes in one test process, or a future
/// multi-store host — that happen to see the same hosted token (same `UserId`)
/// must NOT collide: each enforces against its OWN on-disk tree. Keying on
/// `UserId` alone made store B reuse store A's count instead of seeding against
/// B's disk, causing false rejections / under-counts. The `base_path`
/// namespaces them.
///
/// # Seeding
///
/// A user's entry is seeded LAZILY on first touch by summing their on-disk blob
/// sizes (see [`super::store::SecretsStore::seeded_user_total`]); thereafter every op is O(1).
pub(super) struct QuotaTracker {
    /// `(secrets_dir, UserId)` -> total on-disk footprint bytes that user holds
    /// under that store's tree (active blobs + `.keys`), across all delegates.
    pub(super) per_user_bytes: DashMap<(PathBuf, UserId), std::sync::atomic::AtomicU64>,
}

impl QuotaTracker {
    pub(super) fn new() -> Self {
        Self {
            per_user_bytes: DashMap::new(),
        }
    }

    /// Current tracked total for `(base_path, user)`, or `None` if not yet
    /// seeded.
    pub(super) fn get(&self, base_path: &Path, user: &UserId) -> Option<u64> {
        // DashMap's `Borrow`-based lookup can't form a `&(PathBuf, UserId)` from
        // borrowed halves, so build the owned key. Allocation is on the
        // once-per-op quota path, not a hot inner loop.
        self.per_user_bytes
            .get(&(base_path.to_path_buf(), *user))
            .map(|e| e.value().load(std::sync::atomic::Ordering::Relaxed))
    }

    /// Seed `(base_path, user)`'s tracked total to `bytes` only if absent.
    /// Idempotent: a concurrent/repeat seed leaves the existing value untouched,
    /// so a once-per-process disk walk can never clobber live accounting.
    pub(super) fn seed_if_absent(&self, base_path: &Path, user: UserId, bytes: u64) {
        self.per_user_bytes
            .entry((base_path.to_path_buf(), user))
            .or_insert_with(|| std::sync::atomic::AtomicU64::new(bytes));
    }

    /// Add `delta` to the total (the entry MUST already be seeded).
    pub(super) fn add(&self, base_path: &Path, user: &UserId, delta: u64) {
        if let Some(e) = self.per_user_bytes.get(&(base_path.to_path_buf(), *user)) {
            e.value()
                .fetch_add(delta, std::sync::atomic::Ordering::Relaxed);
        }
    }

    /// Subtract `delta` from the total, saturating at 0 so a double-decrement or
    /// a removal of an untracked blob can never underflow.
    pub(super) fn sub_saturating(&self, base_path: &Path, user: &UserId, delta: u64) {
        if let Some(e) = self.per_user_bytes.get(&(base_path.to_path_buf(), *user)) {
            // Serial execution (one store op at a time node-wide) means this
            // load/store pair is never racing another writer; saturating_sub
            // is belt-and-suspenders against an accounting drift.
            let atomic = e.value();
            let cur = atomic.load(std::sync::atomic::Ordering::Relaxed);
            atomic.store(
                cur.saturating_sub(delta),
                std::sync::atomic::Ordering::Relaxed,
            );
        }
    }
}

/// The single process-wide quota tracker. Lazily initialized; shared by every
/// [`super::store::SecretsStore`] in the process (production pool executors and tests alike),
/// but namespaced per `secrets_dir` so stores over DIFFERENT trees don't collide.
pub(super) static USER_QUOTA_TRACKER: std::sync::LazyLock<QuotaTracker> =
    std::sync::LazyLock::new(QuotaTracker::new);

/// Apply a signed byte delta to a `(base_path, user)` footprint total: positive
/// → add, negative → saturating subtract. Used by `store_secret` to fold in the
/// value-blob delta and the `.keys`-registry delta (either can be negative on
/// an overwrite/registry-shrink). `i128` cannot overflow for any realistic
/// `u64` byte size pair.
pub(super) fn apply_signed_delta(base_path: &Path, user: &UserId, delta: i128) {
    use std::cmp::Ordering;
    match delta.cmp(&0) {
        Ordering::Greater => USER_QUOTA_TRACKER.add(base_path, user, delta as u64),
        Ordering::Less => USER_QUOTA_TRACKER.sub_saturating(base_path, user, (-delta) as u64),
        Ordering::Equal => {}
    }
}

/// Captured under the pre-write borrow in `store_secret` and consumed after the
/// rename + `register_key` commit to update the per-user footprint counter. We
/// stat the value blob's old size and the `.keys` registry's old size BEFORE
/// the write, then re-stat the real post-write sizes to fold in exact deltas.
pub(super) struct QuotaCommit {
    pub(super) user_id: UserId,
    pub(super) old_blob_size: u64,
    pub(super) new_blob_size: u64,
    pub(super) old_keys_size: u64,
}

/// Test-only view of a `(base_path, user)`'s currently-tracked total (`None` if
/// unseeded). Lets a test observe the SHARED process-global counter directly —
/// including confirming a write through one store over a given `secrets_dir` is
/// visible to another store over the SAME dir (and isolated from a different
/// dir).
#[cfg(test)]
pub(crate) fn quota_tracked_total_for_test(base_path: &Path, user: &UserId) -> Option<u64> {
    USER_QUOTA_TRACKER.get(base_path, user)
}

/// Test-only reset of a single `(base_path, user)` tracker entry, so each quota
/// test starts from a clean slate against the process-global tracker regardless
/// of run order.
#[cfg(test)]
pub(crate) fn quota_reset_user_for_test(base_path: &Path, user: &UserId) {
    USER_QUOTA_TRACKER
        .per_user_bytes
        .remove(&(base_path.to_path_buf(), *user));
}
