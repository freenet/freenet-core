//! Inactive-user TTL reclaim (#4561, P5 of #4381) — filesystem helpers,
//! activity stamping, and the periodic sweep task.
//!
//! A hosted node is meant to be a TRANSIENT demo: a web visitor gets a per-user
//! secret namespace, plays with Freenet, and either exports their data to their
//! own peer or walks away. The walk-away case must not let storage grow without
//! bound, so a background sweep reclaims a WHOLE user's data after `ttl_secs` of
//! real-calendar inactivity. None of this runs (or is even reachable) outside
//! hosted mode — the Local single-user node never enumerates or reclaims
//! anything, because its secrets live in `<base>/<delegate>/` (Local scope),
//! NOT under any `<...>/users/<user_id>/` tree the sweep touches.
//!
//! CARDINAL RULE: never reclaim an ACTIVE user (that is silent data loss).
//! Activity tracking is therefore conservative and DURABLE — see
//! [`stamp_user_last_seen`] / `read_user_last_seen` below.
//!
//! The functions here are FREE functions (not `SecretsStore` methods) on purpose:
//! the sweep runs from a background task that does NOT own a live `SecretsStore`
//! (the pooled executors each own their own). They operate only on the DURABLE
//! layer — the on-disk tree (source of truth), the shared ReDb index, and the
//! process-global quota tracker — all of which a restart rebuilds in-memory
//! caches from.

use std::{
    fs,
    path::{Path, PathBuf},
    // Wall-clock SystemTime (not the project-wide TimeSource trait) is the
    // correct abstraction here: thin_snapshots compares "now" against
    // file-name epoch_ms values that must remain meaningful across process
    // restarts. TimeSource returns simulation-relative Duration, which has
    // no stable epoch and can't be compared to persisted timestamps.
    time::SystemTime,
};

use freenet_stdlib::prelude::*;

use crate::contract::storages::Storage;

use super::quota::{MAX_RECLAIMS_PER_SWEEP, SWEEP_MAX_GAP_INTERVAL_MULTIPLE, USER_QUOTA_TRACKER};
use super::user::UserId;

/// Literal directory segment that holds the per-user secret namespaces under a
/// delegate dir (`<base>/<delegate>/users/<user_id>/…`) AND the delegate-
/// independent per-user activity markers (`<base>/users/<user_id>/.last_seen`).
/// A `bs58(32-byte)` name is always 43–44 chars, so the literal `"users"` can
/// never collide with a delegate dir, a `SecretsId` blob, or a `UserId` dir —
/// every walk that filters on [`decode_bs58_32`] skips it automatically.
pub(super) const USERS_DIR: &str = "users";

/// Per-user last-activity marker (#4561, P5 of #4381, inactive-user TTL).
/// Lives at `<base>/users/<user_id>/.last_seen` (delegate-INDEPENDENT — one
/// marker per user, not one per delegate) and stores the user's most recent
/// activity as a decimal UNIX-epoch-seconds string.
///
/// It is deliberately placed under the delegate-independent `<base>/users/`
/// subtree, NOT inside any `<delegate>/users/<user_id>/` secret tree, so it is
/// invisible to every quota / secret disk walk (those only ever descend into a
/// delegate dir, whose name is `bs58(32)` — `"users"` is skipped by
/// [`decode_bs58_32`]). The marker therefore never counts toward a user's
/// quota and never confuses secret enumeration.
pub(super) const LAST_SEEN_FILE: &str = ".last_seen";

/// `File::create` opens the file with the process umask, which on most
/// distros leaves it world-readable. Every secret blob we land at rest
/// MUST be owner-only. This helper opens at mode 0o600 on Unix in the
/// same syscall as the create — no window where the file is readable
/// under the umask. Windows: no-op (per-user profile dir + ACL is
/// already restrictive).
///
/// **Stale-tmp mode preservation.** `OpenOptions::mode` is only
/// honored on the *create* path: when the file already exists, the
/// existing mode is preserved and the `0o600` request is silently
/// ignored. The `truncate(true)` flag rewrites the *content* of a
/// surviving `.tmp` from a prior crashed run but leaves its mode
/// alone — which on an upgraded host can be the legacy 0o644 from
/// before this helper landed. Belt-and-suspenders: unlink any
/// pre-existing inode at `path` so the open always lands on a fresh
/// 0o600 file.
pub(crate) fn create_owner_only(path: &Path) -> std::io::Result<std::fs::File> {
    match std::fs::remove_file(path) {
        Ok(()) => {}
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
        Err(e) => return Err(e),
    }
    let mut opts = std::fs::OpenOptions::new();
    opts.write(true).create_new(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        opts.mode(0o600);
    }
    opts.open(path)
}

/// Ensure the secrets-root directory is mode 0o700 on Unix. Operators
/// who created the directory before the permission tightening landed
/// inherited the umask (often 0o755 = world-readable directory entries).
/// We `chmod` it down on every `SecretsStore::new` so a single restart
/// is sufficient to migrate. Windows: no-op.
#[cfg(unix)]
pub(crate) fn ensure_owner_only_dir(path: &Path) -> std::io::Result<()> {
    use std::os::unix::fs::PermissionsExt;
    let mut perms = std::fs::metadata(path)?.permissions();
    let mode = perms.mode() & 0o777;
    if mode != 0o700 {
        tracing::warn!(
            path = %path.display(),
            existing_mode = format_args!("{mode:o}"),
            "secrets directory was not 0o700; tightening to owner-only"
        );
        perms.set_mode(0o700);
        std::fs::set_permissions(path, perms)?;
    }
    Ok(())
}

#[cfg(not(unix))]
pub(crate) fn ensure_owner_only_dir(_path: &Path) -> std::io::Result<()> {
    Ok(())
}

/// Tighten EVERY directory segment from `base` (exclusive) down to `full`
/// (inclusive) to owner-only.
///
/// `create_dir_all(full)` materializes all missing intermediate segments
/// under the process umask (typically 0o755 = world-readable directory
/// entries), but the callers historically only chmodded the leaf. For the
/// per-user [`SecretScope::User`](super::user::SecretScope) path that leaf is
/// `<delegate>/users/<user_id>`, so the freshly-created `<delegate>/users`
/// and (on a delegate's first write) `<delegate>` would be left
/// world-traversable — a local user could enumerate `users/` subdir names
/// (the per-user id tags) and write timing, violating the owner-only-dir
/// invariant (#4141). This walks the components strictly below `base` and
/// applies [`ensure_owner_only_dir`] to each.
///
/// For [`SecretScope::Local`](super::user::SecretScope) `full == base/<delegate>`, so the only
/// segment below `base` is `<delegate>` and this performs the SAME single
/// chmod the pre-#4381 code did — Local behavior is byte-for-byte
/// unchanged. `base` itself is never touched here (it is tightened once in
/// [`super::store::SecretsStore::new`]). Best-effort by contract of its callers: they log
/// and continue on a chmod error rather than failing the primary write.
pub(super) fn ensure_owner_only_tree(base: &Path, full: &Path) -> std::io::Result<()> {
    // The relative path from base to full names exactly the segments we
    // must tighten. If `full` is not under `base` (should never happen —
    // both are derived from `self.base_path`), tighten only the leaf as a
    // conservative fallback.
    let Ok(rel) = full.strip_prefix(base) else {
        return ensure_owner_only_dir(full);
    };
    let mut current = base.to_path_buf();
    for component in rel.components() {
        current.push(component);
        ensure_owner_only_dir(&current)?;
    }
    Ok(())
}

/// Decode a base58 (BITCOIN alphabet) name into exactly 32 bytes, or `None` if
/// it isn't a valid 32-byte bs58 string. Used by the on-disk export enumeration
/// to recognize delegate-dir names and secret-blob filenames (both are
/// `bs58(<32-byte hash>)`) while rejecting every non-secret entry (`.keys`,
/// `*.tmp`, `node_kek`, `kek_backend`, ...), none of which decode to 32 bytes.
pub(super) fn decode_bs58_32(name: &str) -> Option<[u8; 32]> {
    let mut out = [0u8; 32];
    match bs58::decode(name)
        .with_alphabet(bs58::Alphabet::BITCOIN)
        .onto(&mut out)
    {
        // `onto` fills `out` and returns the number of bytes written; require
        // EXACTLY 32 so a shorter/longer bs58 string is rejected.
        Ok(32) => Some(out),
        _ => None,
    }
}

/// Path to a user's delegate-independent activity-marker directory,
/// `<base>/users/<user_id>/`. The `.last_seen` file lives directly inside it.
pub(super) fn user_activity_dir(base_path: &Path, user_id: &UserId) -> PathBuf {
    base_path.join(USERS_DIR).join(user_id.encode())
}

/// Wall-clock UNIX-epoch seconds.
///
/// WALL-CLOCK JUSTIFICATION (load-bearing — do not "fix" this to `TimeSource`):
/// the inactive-user TTL is a DURABLE CALENDAR-TIME threshold (e.g. 30 real
/// days) that must hold ACROSS PROCESS RESTARTS. That rules out both project
/// time abstractions. `TimeSource` returns simulation-relative `Duration` with
/// no stable epoch — it resets every test/process and cannot be compared to a
/// timestamp persisted to disk in a prior run. `Instant` is monotonic-from-boot
/// — it resets on restart, so a marker written before a restart would look "in
/// the future" afterwards. Only real wall-clock (`SystemTime` → unix epoch)
/// gives a timestamp that is still meaningful after the node restarts, which is
/// exactly what a durable 30-day TTL needs. This mirrors the existing wall-clock
/// exception already documented on this module's `SystemTime` import (snapshot
/// epoch_ms).
///
/// The TESTABLE core never calls this directly: `stamp_user_last_seen` and
/// `reclaim_inactive_users` take `now`/`ttl` as parameters so tests are fully
/// deterministic with no real clock. The thin non-test wrappers (the sweep task
/// and the WS `stamp_activity` hook) call THIS so there is exactly one
/// wall-clock source — including its clamp-to-0 behavior — and no drift.
pub fn wall_clock_unix_secs() -> u64 {
    SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        // A clock set before 1970 is absurd; clamp to 0 rather than panic so a
        // misconfigured host degrades to "looks very old" (eligible for reclaim
        // only if also past the TTL) instead of crashing the sweep.
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

/// Read a user's persisted last-activity timestamp (unix epoch seconds), or
/// `None` if the marker is absent or unreadable/corrupt.
///
/// A missing OR unparseable marker returns `None`. The caller
/// ([`reclaim_inactive_users`]) treats `None` CONSERVATIVELY — it does NOT
/// reclaim a user whose marker it cannot read, because "no readable activity
/// record" must never be interpreted as "definitely inactive". (A user that has
/// secrets but somehow no marker is left alone rather than risking deletion of
/// live data; the marker is (re)written on their next request anyway.)
pub(super) fn read_user_last_seen(base_path: &Path, user_id: &UserId) -> Option<u64> {
    let path = user_activity_dir(base_path, user_id).join(LAST_SEEN_FILE);
    let raw = fs::read_to_string(&path).ok()?;
    raw.trim().parse::<u64>().ok()
}

/// Stamp a user's `.last_seen` marker to `now` (unix epoch seconds), DEBOUNCED:
/// the marker is rewritten only if it is missing or older than
/// `debounce_secs`. Returns `true` iff a write actually happened (for tests /
/// observability); the common hot-path outcome is `false` (a single `stat`,
/// no write).
///
/// `now`/`debounce_secs` are parameters so the testable core is deterministic.
/// Production passes [`wall_clock_unix_secs`] and
/// [`super::user::DEFAULT_LAST_SEEN_DEBOUNCE_SECS`].
///
/// Best-effort and NON-FATAL: a stamp failure (disk full, race) is logged at
/// debug and swallowed. Losing a single stamp only risks a marker going at most
/// `debounce_secs` (≪ TTL) more stale than reality — the next request restamps
/// — so it can never cause a false reclaim within the 30-day window.
pub fn stamp_user_last_seen(
    base_path: &Path,
    user_id: &UserId,
    now: u64,
    debounce_secs: u64,
) -> bool {
    // Debounce: skip the write if the existing marker is recent enough.
    if let Some(prev) = read_user_last_seen(base_path, user_id) {
        if now.saturating_sub(prev) < debounce_secs {
            return false;
        }
    }
    let dir = user_activity_dir(base_path, user_id);
    if let Err(e) = fs::create_dir_all(&dir) {
        tracing::debug!(user_id = %user_id.encode(), error = %e, "last_seen: mkdir failed");
        return false;
    }
    // Tighten the activity subtree to owner-only, matching the secret tree's
    // at-rest posture. Best-effort: a marker is not secret (it's a timestamp),
    // but keeping the whole per-user tree 0o700 avoids leaking which user_ids
    // exist to other local users.
    ensure_owner_only_tree(base_path, &dir).ok();
    // Write via a temp file + rename so a concurrent reader never sees a
    // half-written value. The marker is tiny and the write is rare (debounced),
    // so the extra rename is negligible.
    let tmp = dir.join(format!("{LAST_SEEN_FILE}.tmp"));
    let final_path = dir.join(LAST_SEEN_FILE);
    let write_then_rename = || -> std::io::Result<()> {
        fs::write(&tmp, now.to_string())?;
        fs::rename(&tmp, &final_path)
    };
    match write_then_rename() {
        Ok(()) => true,
        Err(e) => {
            tracing::debug!(user_id = %user_id.encode(), error = %e, "last_seen: write failed");
            fs::remove_file(&tmp).ok();
            false
        }
    }
}

/// Enumerate every user that currently has an activity marker, i.e. every
/// `<base>/users/<user_id>/` directory. Returns the decoded [`UserId`]s.
///
/// This is the sweep's candidate set. A user only ever gets a marker by being
/// active in hosted mode (`stamp_user_last_seen`), so a Local single-user node
/// — which never stamps — produces an EMPTY set here, an extra guarantee on top
/// of the hosted-mode spawn gate that the sweep can never touch Local data.
///
/// A missing `<base>/users/` dir ⇒ no hosted users ⇒ empty. Any other read
/// error is logged and treated as empty for THIS pass (the sweep retries next
/// interval) rather than aborting the node.
pub(super) fn enumerate_marked_users(base_path: &Path) -> Vec<UserId> {
    let users_root = base_path.join(USERS_DIR);
    let rd = match fs::read_dir(&users_root) {
        Ok(rd) => rd,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Vec::new(),
        Err(e) => {
            tracing::warn!(error = %e, dir = %users_root.display(), "inactive-user sweep: cannot read users dir");
            return Vec::new();
        }
    };
    let mut out = Vec::new();
    for entry in rd.flatten() {
        if !entry.path().is_dir() {
            continue;
        }
        let Some(name) = entry.file_name().to_str().map(str::to_owned) else {
            continue;
        };
        if let Some(bytes) = decode_bs58_32(&name) {
            out.push(UserId::new(bytes));
        }
    }
    out
}

/// Enumerate every delegate that has on-disk state, by its 32-byte key. The
/// reclaim removes `<delegate>/users/<user_id>/` from EACH of these and the
/// matching ReDb index row. The `code_hash` half of `DelegateKey` is recovered
/// from the ReDb user-index when known, falling back to a zero placeholder
/// (inert for the path-only delete; the ReDb remove keys on the 96-byte
/// composite, so a wrong code_hash there would simply no-op — acceptable
/// because a real abandoned user's rows always carry the real code_hash from
/// when they were written, and we look them up below).
fn enumerate_delegate_keys(base_path: &Path) -> Vec<[u8; 32]> {
    let rd = match fs::read_dir(base_path) {
        Ok(rd) => rd,
        Err(_) => return Vec::new(),
    };
    let mut out = Vec::new();
    for entry in rd.flatten() {
        if !entry.path().is_dir() {
            continue;
        }
        let Some(name) = entry.file_name().to_str().map(str::to_owned) else {
            continue;
        };
        // Skip the literal `users/` activity-marker root; only real delegate
        // dirs (bs58-32) carry per-delegate user secret trees.
        if name == USERS_DIR {
            continue;
        }
        if let Some(bytes) = decode_bs58_32(&name) {
            out.push(bytes);
        }
    }
    out
}

/// Reclaim ONE user's ENTIRE durable footprint. IDEMPOTENT and CRASH-SAFE: a
/// re-run on a partially-reclaimed user finishes cleanly, so a crash mid-reclaim
/// is recovered by the next sweep.
///
/// Clears, in source-of-truth-first order (disk → derived caches):
///   1. DISK: `fs::remove_dir_all` on `<delegate>/users/<user_id>/` for every
///      delegate, then on `<base>/users/<user_id>/` (the activity marker tree).
///      `remove_dir_all` on an already-absent path is treated as success.
///   2. ReDb: `remove_user_secrets_index(delegate, user_id)` for every delegate
///      (idempotent — redb `remove` on an absent key is `Ok(None)`).
///   3. Process-global quota tracker: drop the `(base_path, user_id)` entry so
///      a future user re-using the same `user_id` re-seeds from a clean disk.
///
/// Ordering rationale: removing the on-disk blobs FIRST means that if we crash
/// between (1) and (2), the worst residue is a dangling ReDb index row pointing
/// at deleted blobs — which the read path tolerates (`MissingSecret`, no panic)
/// and which the next sweep's re-run removes. The reverse order could leave
/// readable blobs with no index, a worse (silently-resurrectable) state.
///
/// `db` is the SHARED ReDb handle (single-writer per process), so the sweep MUST
/// be handed the same `Storage` the executors use — never a second `Database`
/// open on the same file.
///
/// Convenience entry that loads THIS user's index rows itself (one table read)
/// and reclaims the single user. The production sweep does NOT use this — it
/// uses [`reclaim_user_with_index_rows`] with rows pre-grouped once per pass
/// ([`group_index_rows_by_user`]) to avoid re-reading the whole table per user.
/// This single-user entry exists only for tests, so it is `#[cfg(test)]`.
#[cfg(test)]
pub fn reclaim_user(base_path: &Path, db: &Storage, user_id: &UserId) {
    let delegate_keys = group_index_rows_by_user(db)
        .and_then(|mut m| m.remove(user_id))
        .unwrap_or_default();
    reclaim_user_with_index_rows(base_path, db, user_id, &delegate_keys);
}

/// Core reclaim with the user's ReDb index `DelegateKey`s already resolved (real
/// code_hashes). See [`reclaim_user`] for the ordering/crash-safety contract;
/// this is the variant the sweep drives with rows grouped once per pass.
fn reclaim_user_with_index_rows(
    base_path: &Path,
    db: &Storage,
    user_id: &UserId,
    index_delegate_keys: &[DelegateKey],
) {
    // (1) disk: remove the per-(delegate,user) secret subtree under EVERY
    // delegate. The dir name is key-only (`DelegateKey::encode()` renders the
    // 32-byte key), so a zero code_hash placeholder produces the correct path.
    for key_bytes in enumerate_delegate_keys(base_path) {
        let user_dir = base_path
            .join(DelegateKey::new(key_bytes, CodeHash::from(&[0u8; 32])).encode())
            .join(USERS_DIR)
            .join(user_id.encode());
        match fs::remove_dir_all(&user_dir) {
            Ok(()) => {}
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
            Err(e) => {
                tracing::warn!(
                    user_id = %user_id.encode(),
                    dir = %user_dir.display(),
                    error = %e,
                    "inactive-user reclaim: failed to remove user secret dir (will retry next sweep)"
                );
            }
        }
    }
    // (2) ReDb: delete EVERY index row for this user across all delegates,
    // keyed on the REAL code_hashes recorded in the table (not a guess) so the
    // 96-byte composite key matches and the row is actually removed.
    remove_user_index_rows(db, user_id, index_delegate_keys);

    // (1, marker tree) disk: remove the delegate-independent activity-marker
    // tree LAST, so that if we crash before this point the user still has a
    // marker and the next sweep re-evaluates them (and re-runs the idempotent
    // delete) rather than the marker vanishing while secrets linger.
    let marker_dir = user_activity_dir(base_path, user_id);
    match fs::remove_dir_all(&marker_dir) {
        Ok(()) => {}
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
        Err(e) => {
            tracing::warn!(
                user_id = %user_id.encode(),
                dir = %marker_dir.display(),
                error = %e,
                "inactive-user reclaim: failed to remove activity-marker dir (will retry next sweep)"
            );
        }
    }

    // (3) process-global quota tracker.
    USER_QUOTA_TRACKER
        .per_user_bytes
        .remove(&(base_path.to_path_buf(), *user_id));
}

/// Remove the supplied ReDb user-secrets-index rows for `user_id` (the
/// `DelegateKey`s carrying the REAL code_hash, so the 96-byte composite key
/// matches and the row is actually removed). `delegate_keys` is pre-filtered to
/// THIS user by the caller — see [`group_index_rows_by_user`], which loads the
/// whole table ONCE per sweep instead of once per reclaimed user. Idempotent: a
/// remove of an already-absent key is `Ok(None)`. Best-effort: a ReDb error is
/// logged and the next sweep retries.
fn remove_user_index_rows(db: &Storage, user_id: &UserId, delegate_keys: &[DelegateKey]) {
    for delegate_key in delegate_keys {
        if let Err(e) = db.remove_user_secrets_index(delegate_key, user_id.as_bytes()) {
            tracing::warn!(
                user_id = %user_id.encode(),
                error = %e,
                "inactive-user reclaim: failed to remove ReDb index row (will retry next sweep)"
            );
        }
    }
}

/// Load the ENTIRE user-secrets index ONCE and group the rows' `DelegateKey`s by
/// `UserId`. The sweep calls this a single time per pass, then reclaims each user
/// from their pre-grouped rows — O(T) total table reads instead of the O(R×T) of
/// re-loading the whole table for every one of R reclaimed users. Returns `None`
/// on a ReDb read error (the whole pass then bails, retrying next sweep) so a
/// transient DB error can never be mistaken for "this user has no index rows"
/// and leave a row dangling.
fn group_index_rows_by_user(
    db: &Storage,
) -> Option<std::collections::HashMap<UserId, Vec<DelegateKey>>> {
    let rows = match db.load_all_user_secrets_index() {
        Ok(rows) => rows,
        Err(e) => {
            tracing::warn!(
                error = %e,
                "inactive-user sweep: cannot read user-secrets index (will retry next sweep)"
            );
            return None;
        }
    };
    let mut by_user: std::collections::HashMap<UserId, Vec<DelegateKey>> =
        std::collections::HashMap::new();
    for ((delegate_key, uid_bytes), _secrets) in rows {
        by_user
            .entry(UserId::new(uid_bytes))
            .or_default()
            .push(delegate_key);
    }
    Some(by_user)
}

/// Outcome of one [`reclaim_inactive_users`] pass. Returned (not just a count)
/// so the sweep loop can warn on anomalies and advance its `prev_sweep_now`.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct SweepOutcome {
    /// Users whose entire footprint was reclaimed this pass.
    pub reclaimed: usize,
    /// `true` if the per-pass cap ([`MAX_RECLAIMS_PER_SWEEP`]) was hit and the
    /// remaining eligible users were deferred to the next pass — a LOUD signal
    /// (a clock fault or an unusually large backlog).
    pub capped: bool,
    /// `true` if the WHOLE pass was skipped because `now` advanced implausibly
    /// far since the previous sweep (suspected forward clock jump / long
    /// suspend). No reclaim happened.
    pub skipped_gap: bool,
}

/// One sweep pass: reclaim every user whose last activity is older than `ttl`,
/// with two clock-fault guards layered on the pure-age threshold.
///
/// Pure age threshold — NO "active" exemption that could become an unbounded GC
/// blind spot (satisfies the repo's cleanup-must-be-time-bounded rule).
///
/// SAFETY (never reclaim an active user, never silently mass-delete on a clock
/// fault):
///   * A user with NO readable marker is SKIPPED (conservative — see
///     [`read_user_last_seen`]).
///   * The marker is RE-READ immediately before deleting, so a user who
///     reconnected (and restamped) between `enumerate_marked_users` and the
///     delete is re-checked against `now` and spared (reconnect-during-sweep
///     race close).
///   * GAP DETECTOR: if `prev_sweep_now` is `Some` and `now` advanced by MORE
///     than `max_gap_secs` since it, the clock almost certainly jumped forward
///     (or the node was suspended across a long gap) — the WHOLE pass is skipped
///     (no delete) and `skipped_gap` is returned so the caller warns. This stops
///     a between-sweeps forward jump BEFORE any delete.
///   * PER-PASS CAP: at most `max_reclaims` users are reclaimed in one pass; the
///     rest are deferred (idempotent reclaim makes deferral free). This bounds
///     the blast radius of a clock fault that slips past the gap detector (e.g.
///     a skew already present at the FIRST post-restart sweep, where there is no
///     `prev_sweep_now`) and, via `capped`, surfaces the anomaly.
///
/// `now`/`ttl`/`prev_sweep_now`/`max_gap_secs`/`max_reclaims` are all parameters
/// so the testable core is deterministic (no real clock or consts buried
/// inside). Production passes [`wall_clock_unix_secs`], the operator TTL, the
/// previous pass's `now`, `sweep_interval * SWEEP_MAX_GAP_INTERVAL_MULTIPLE`,
/// and [`MAX_RECLAIMS_PER_SWEEP`]. `ttl == 0` is a no-op (callers also gate on
/// `ttl > 0`).
pub fn reclaim_inactive_users(
    base_path: &Path,
    db: &Storage,
    now: u64,
    ttl: u64,
    prev_sweep_now: Option<u64>,
    max_gap_secs: u64,
    max_reclaims: usize,
) -> SweepOutcome {
    // Defense in depth: a zero TTL means "disabled" and must never reclaim.
    // The spawn site already gates on this, but a direct call must be safe too.
    if ttl == 0 {
        return SweepOutcome::default();
    }

    // GAP DETECTOR: a forward clock jump between sweeps would make every user
    // look ancient at once. If `now` is implausibly far ahead of the previous
    // pass, skip this pass entirely rather than mass-delete. Conservative: a
    // legitimate long downtime also skips one pass (users restamp on reconnect;
    // the next pass proceeds). `max_gap_secs == 0` disables the detector (used
    // by tests / a first pass with no baseline).
    if let Some(prev) = prev_sweep_now {
        if max_gap_secs > 0 && now.saturating_sub(prev) > max_gap_secs {
            tracing::warn!(
                now,
                prev_sweep_now = prev,
                gap_secs = now.saturating_sub(prev),
                max_gap_secs,
                "inactive-user sweep: time advanced implausibly far since the last \
                 sweep (suspected forward clock jump or long suspend) — SKIPPING \
                 reclaim this pass to avoid a clock-fault mass-delete; will \
                 re-evaluate next pass"
            );
            return SweepOutcome {
                reclaimed: 0,
                capped: false,
                skipped_gap: true,
            };
        }
    }

    let mut reclaimed = 0usize;
    let mut capped = false;
    // Load the user-secrets index ONCE per pass and group by user, so each
    // reclaim uses pre-resolved rows instead of re-reading the whole table
    // (O(T) per pass, not O(R×T)). A read error bails the whole pass (retries
    // next sweep) rather than risk leaving rows dangling.
    let Some(mut index_by_user) = group_index_rows_by_user(db) else {
        return SweepOutcome::default();
    };

    for user_id in enumerate_marked_users(base_path) {
        // PER-PASS CAP: stop once we've reclaimed the cap this pass. The
        // remainder is deferred to the next sweep (reclaim is idempotent), which
        // bounds a clock fault's blast radius. `max_reclaims == 0` disables the
        // cap (treated as unbounded) for direct/test callers that want no limit.
        if max_reclaims > 0 && reclaimed >= max_reclaims {
            capped = true;
            break;
        }
        // RE-READ the marker right before deciding — this is the reconnect-race
        // close. If the user restamped after enumeration, `last_seen` is fresh
        // and they are spared. A vanished/unreadable marker ⇒ skip
        // (conservative).
        let Some(last_seen) = read_user_last_seen(base_path, &user_id) else {
            continue;
        };
        if now.saturating_sub(last_seen) > ttl {
            tracing::info!(
                user_id = %user_id.encode(),
                idle_secs = now.saturating_sub(last_seen),
                ttl_secs = ttl,
                "inactive-user sweep: reclaiming abandoned hosted user"
            );
            let delegate_keys = index_by_user.remove(&user_id).unwrap_or_default();
            reclaim_user_with_index_rows(base_path, db, &user_id, &delegate_keys);
            reclaimed += 1;
        }
    }

    if capped {
        tracing::warn!(
            reclaimed,
            cap = max_reclaims,
            "inactive-user sweep hit reclaim cap {max_reclaims} — possible clock \
             fault or large backlog; remaining users deferred to next sweep"
        );
    }
    SweepOutcome {
        reclaimed,
        capped,
        skipped_gap: false,
    }
}

/// Whether the node should spawn the inactive-user reclaim sweep. The sweep is
/// the only thing that ever enumerates or deletes per-user data, so this single
/// predicate is the load-bearing safety gate: it must be `true` ONLY in hosted
/// mode AND with a non-zero TTL. Extracted (rather than inlined at the spawn
/// site) so the conjunction is directly unit-testable — a regression that, say,
/// dropped the `hosted_mode` term would turn a Local node's sweep on, which this
/// test pins against.
pub fn should_spawn_inactive_user_sweep(hosted_mode: bool, ttl_secs: u64) -> bool {
    hosted_mode && ttl_secs > 0
}

/// Spawn the periodic inactive-user reclaim sweep and return its
/// [`JoinHandle`](tokio::task::JoinHandle). The CALLER owns the handle (so the
/// task is aborted when the node shuts down — same teardown discipline as the
/// other `Storage`-holding background tasks, #4401) and is responsible for
/// gating the spawn on hosted mode (this function does not re-check
/// `hosted_mode`; it only refuses to spawn a useless task when `ttl == 0`).
///
/// The task loops on a `tokio::time::interval` of `sweep_interval`; each tick
/// runs one [`reclaim_inactive_users`] pass against real wall-clock now. The
/// `Storage` handle (a cheap `Arc` clone) is moved into the task; it is the
/// SAME shared ReDb the executors use, so there is no second `Database` open.
///
/// Returns `None` when `ttl == 0` (sweep disabled) so the caller stores no
/// handle. A zero/oversmall `sweep_interval` is floored to 1s.
#[must_use = "the returned JoinHandle must be retained so the sweep is aborted on shutdown"]
pub fn spawn_inactive_user_sweep(
    base_path: PathBuf,
    db: Storage,
    ttl_secs: u64,
    sweep_interval_secs: u64,
) -> Option<tokio::task::JoinHandle<()>> {
    if ttl_secs == 0 {
        // Disabled: do not spawn. (Belt-and-suspenders; the node-side gate
        // already checks this, but a direct caller is safe too.)
        return None;
    }
    let interval = std::time::Duration::from_secs(sweep_interval_secs.max(1));
    // Largest plausible wall-clock advance between two consecutive sweeps; a
    // larger jump trips the gap detector. Derived from the interval, not a
    // const, so it scales with the operator's sweep cadence.
    let max_gap_secs = interval
        .as_secs()
        .saturating_mul(SWEEP_MAX_GAP_INTERVAL_MULTIPLE);
    let handle = crate::config::GlobalExecutor::spawn(async move {
        tracing::info!(
            ttl_secs,
            sweep_interval_secs = interval.as_secs(),
            max_gap_secs,
            max_reclaims_per_sweep = MAX_RECLAIMS_PER_SWEEP,
            "Inactive-user reclaim sweep started (hosted mode)"
        );
        let mut ticker = tokio::time::interval(interval);
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        // The first `tick()` fires immediately; skip it so we don't reclaim on
        // the very first event-loop turn at boot (a just-restarted node should
        // let its users restamp on reconnect before the first sweep).
        ticker.tick().await;
        // Previous pass's wall-clock `now`, for the forward-jump gap detector.
        // `None` for the first real pass (no baseline yet) — the per-pass cap is
        // the guard that still bounds a skewed-clock-at-first-pass fault.
        let mut prev_sweep_now: Option<u64> = None;
        loop {
            ticker.tick().await;
            let now = wall_clock_unix_secs();
            // The disk walk + per-user stats + ReDb reads are blocking calls;
            // keep them off the async reactor thread so a large `users/` tree
            // can't stall other tasks.
            let base = base_path.clone();
            let storage = db.clone();
            let result = tokio::task::spawn_blocking(move || {
                reclaim_inactive_users(
                    &base,
                    &storage,
                    now,
                    ttl_secs,
                    prev_sweep_now,
                    max_gap_secs,
                    MAX_RECLAIMS_PER_SWEEP,
                )
            })
            .await;
            // Advance the gap-detector baseline ONLY for a pass that genuinely
            // COMPLETED (`Ok`) AND did not itself skip on a suspected jump. Two
            // cases must hold the last TRUSTED baseline instead of advancing it:
            //   * a gap-SKIP (`skipped_gap`): advancing would let a *second*
            //     equally-jumped `now` look "normal" vs the first and delete;
            //   * a PANIC/cancel (`Err`): we never evaluated the clock against
            //     the users this pass, so `now` is unverified — treat it like a
            //     skip and hold the baseline (a panicked pass at a skewed clock
            //     must not arm the next pass to proceed). Panic and gap-skip are
            //     NOT conflated in the warn semantics — only in "don't advance".
            let ran = matches!(&result, Ok(o) if !o.skipped_gap);
            if ran {
                prev_sweep_now = Some(now);
            }
            match result {
                Ok(o) => {
                    if o.reclaimed > 0 {
                        tracing::info!(
                            reclaimed = o.reclaimed,
                            "inactive-user sweep: reclaimed abandoned users"
                        );
                    }
                }
                Err(e) => {
                    tracing::warn!(error = %e, "inactive-user sweep pass panicked/cancelled");
                }
            }
        }
    });
    Some(handle)
}
