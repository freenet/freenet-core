//! Tiered retention thinning for delegate secret snapshots.
//!
//! Each `(delegate, secret_id)` pair has its own snapshot history. Before a
//! `store_secret` overwrites an existing value with DIFFERENT content, the
//! previous ciphertext is moved into a `.snapshots/{secret_id}/{epoch_ms}`
//! file. (A write that leaves the plaintext unchanged is skipped — see
//! `SecretsStore::active_plaintext_matches` — since a second, differently-
//! nonced copy of a value we already hold buys no recovery.) Retention is
//! then thinned per the policy below: recent history stays dense, older
//! history is sampled at progressively coarser intervals (minute / hour /
//! day / week / month), an absolute `max_age` cap drops anything older
//! regardless of which clause selected it, and a per-secret
//! `max_total_bytes` budget drops the oldest survivors until the retained
//! history fits on disk. Worst case ~62 entries per secret in steady state,
//! and at most ~`max_total_bytes` of disk for any one secret.
//!
//! Snapshots are encrypted with the same key as the active secret — since
//! #4146 the delegate's DEK, derived from the node KEK (a delegate does not
//! configure its own cipher). Without that key the bytes are useless.

use std::collections::{BTreeSet, HashSet};
use std::fs;
use std::io::Write;
use std::path::{Path, PathBuf};
// Wall-clock SystemTime (not the project-wide TimeSource trait) is the
// correct abstraction here: snapshot file names embed epoch_ms so retention
// stays sortable across process restarts and across nodes that share a
// data directory. TimeSource returns simulation-relative Duration with no
// stable origin, which can't be persisted into a filename.
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use freenet_stdlib::prelude::SecretsId;

// The owner-only filesystem helpers live next door in `secrets_store`;
// the snapshot restore path reuses them so the secret-blob and the
// reversibility-snapshot writes share one perms discipline.
use super::secrets_store::{create_owner_only, ensure_owner_only_dir};

/// Subdirectory (relative to a delegate's secrets dir) holding the
/// per-secret snapshot history. Leading dot keeps it visually separate from
/// active secret files; the `get_secret` read path never descends into it.
pub const SNAPSHOTS_DIR: &str = ".snapshots";

/// Width of the zero-padded epoch-millis used to name snapshot files.
/// `u64::MAX` is 20 digits, so this width keeps lexicographic order
/// equivalent to chronological order for the lifetime of the universe.
pub const SNAPSHOT_NAME_WIDTH: usize = 20;

/// Maximum number of within-the-same-millisecond collision suffixes we'll
/// try before giving up. 1024 is generous: at burst of 1024 writes/ms a
/// node would be doing >1M ops/sec to a single secret, far beyond any
/// realistic delegate workload. Hitting the cap returns an error so the
/// caller can log instead of silently overwriting an existing snapshot.
const MAX_SNAPSHOT_COLLISION_SUFFIX: u32 = 1024;

/// Default per-secret byte budget for retained snapshot history
/// ([`RetentionPolicy::max_total_bytes`]).
///
/// The count-based tiers alone are unbounded in BYTES: ~62 retained
/// versions costs a few hundred KiB for a small secret and ~62 MiB for a
/// ~1 MiB one. Measured on a production peer, 130 MB of snapshots guarded
/// 6.8 MB of live secrets (19x), and a SINGLE ~1 MiB River room-state
/// secret accounted for 30.7 MB of that across 34 versions.
///
/// 3 MiB is the compromise:
/// - Small secrets (bytes to low-KiB) never come near it, so they keep the
///   full tiered history — deep history is nearly free there, and it is
///   where an operator most plausibly wants to walk back several versions.
/// - A ~1 MiB room state gets ~3 versions instead of ~30, which still
///   covers the case snapshots exist for (undo an accidental or buggy
///   overwrite) at a tenth of the disk.
/// - Even at the cap, one secret's history is ~2x the ENTIRE live secret
///   footprint of a measured production gateway (1.6 MB), so the budget is
///   generous in absolute terms.
///
/// Note this is a PER-SECRET bound, so a node's total snapshot footprint is
/// still `keys x 3 MiB` in the worst case; bounding the aggregate would need
/// a cross-secret sweep, which is deliberately out of scope here.
pub const DEFAULT_MAX_SNAPSHOT_BYTES_PER_SECRET: u64 = 3 * 1024 * 1024;

/// Floor on how many snapshots the byte budget is allowed to leave behind.
///
/// The budget evicts oldest-first, but it must never collapse the history to
/// a single entry: one survivor only supports "undo the last write", with no
/// room to walk back when the operator does not yet know which version they
/// want. Three is the smallest count that gives a real walk-back window, and
/// it is cheap where it binds:
///
/// - At the default 3 MiB budget the floor is INERT for any secret UNDER
///   ~1 MiB, because three of those versions already fit inside the budget.
///   Precisely: a snapshot costs `HEADER_LEN + plaintext + TAG_LEN` = 41
///   bytes of overhead, so three fit iff `3 x (n + 41) <= 3_145_728`, i.e.
///   plaintext `n <= 1_048_535` bytes. At exactly 1 MiB (1_048_576) the
///   floor already binds. So it takes effect only for the genuinely large
///   values (a ~1 MiB River room state and up) — exactly the case where the
///   budget is aggressive and a one-entry history would be thinnest.
/// - Worst case it retains `3 x largest_snapshot` rather than an unbounded
///   count, so it cannot reintroduce the version-count blowup this budget
///   exists to fix.
///
/// Deliberately not 5 (matching `keep_last`): by the same arithmetic that
/// would bind for every secret over ~614 KiB (`n > 629_104`) and raise the
/// worst case to 5x, buying two more versions of the values that cost the
/// most to keep.
///
/// Note this floor is what makes the budget's guarantee "at most
/// `max(max_total_bytes, 3 x largest snapshot)`", not a hard byte cap.
const MIN_SNAPSHOTS_KEPT_UNDER_BUDGET: usize = 3;

/// Environment variable overriding [`DEFAULT_MAX_SNAPSHOT_BYTES_PER_SECRET`],
/// in bytes. Parsed by [`parse_snapshot_budget`].
///
/// Exists because the budget is the one retention clause that can DELETE
/// pre-existing history on upgrade (see [`RetentionPolicy::max_total_bytes`]),
/// and the only other snapshot knob,
/// `FREENET_DISABLE_SECRET_SNAPSHOTS`, points the wrong way for an operator
/// who wants MORE history rather than none.
pub const SNAPSHOT_BUDGET_ENV: &str = "FREENET_SECRET_SNAPSHOT_BYTES_PER_SECRET";

/// One tier of a [`RetentionPolicy`]: aim to keep one snapshot per
/// `interval` of clock time, up to `max_count` snapshots in this tier.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RetentionBucket {
    pub interval: Duration,
    pub max_count: usize,
}

/// On-disk descriptor for a single snapshot, surfaced through the
/// list/restore API. The pair `(timestamp_ms, suffix)` is unique within
/// a snapshot directory and is what `SecretsStore::restore_snapshot`
/// matches against. `path` and `size_bytes` are advisory metadata for
/// callers (CLI display, sanity checks).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SnapshotMetadata {
    pub timestamp_ms: u64,
    pub suffix: Option<u32>,
    pub path: PathBuf,
    pub size_bytes: u64,
}

/// Tiered retention policy. The first `keep_last` snapshots (by recency) are
/// kept regardless of bucket coverage; each bucket independently selects
/// representatives at its own granularity; `max_age` is an absolute upper
/// bound that drops snapshots older than the threshold even if a recency or
/// bucket clause would otherwise keep them; and `max_total_bytes` is a
/// per-secret disk budget that drops the oldest survivors of all of the
/// above until the retained history fits. The absolute age cap satisfies the
/// cleanup-exemption rule in AGENTS.md: every retained entry has a finite
/// lifetime.
///
/// Every clause is subtractive: the count tiers propose a keep-set, and
/// `max_age` / `max_total_bytes` only ever remove from it. Nothing here can
/// resurrect a snapshot another clause dropped.
#[derive(Debug, Clone)]
pub struct RetentionPolicy {
    pub keep_last: usize,
    pub buckets: Vec<RetentionBucket>,
    /// Absolute upper bound on how old a snapshot may be and still be
    /// retained. `None` disables the cap (older snapshots may stick around
    /// indefinitely if `keep_last` or a long-interval bucket selects them).
    /// `Some(d)` drops snapshots older than `now - d` regardless of which
    /// clause would have kept them.
    pub max_age: Option<Duration>,
    /// Upper bound, in bytes, on the total on-disk size of the snapshots
    /// retained for ONE secret. `None` disables the budget (the count tiers
    /// then bound the number of versions but not their size, which is what
    /// let a ~1 MiB secret accumulate ~30 MiB of history). `Some(n)` drops
    /// the OLDEST otherwise-retained snapshots until the rest fit in `n`,
    /// never going below [`MIN_SNAPSHOTS_KEPT_UNDER_BUDGET`].
    ///
    /// Applied to the size the entries actually occupy on disk, so it bounds
    /// disk directly rather than proxying it through a version count.
    ///
    /// **Beware the `0` asymmetry when constructing this directly.**
    /// `Some(0)` here is the MOST aggressive budget there is — zero bytes
    /// allowed, so eviction runs until it hits the floor. Setting
    /// [`SNAPSHOT_BUDGET_ENV`] to the string `"0"` means the OPPOSITE:
    /// [`parse_snapshot_budget`] maps it to `None`, i.e. no budget at all.
    /// The env spelling is the operator-facing "turn this off" switch (an
    /// operator typing `0` means "no limit", not "limit of nothing"), while
    /// the field is the internal numeric bound and `Some(0)` is its natural
    /// extreme. Tests rely on the aggressive `Some(0)` reading. If you add a
    /// constructor that takes a byte count from anywhere operator-facing,
    /// route it through [`parse_snapshot_budget`] rather than wrapping it in
    /// `Some` yourself.
    ///
    /// **This clause is RETROACTIVE.** Unlike the count tiers, which a node
    /// upgrading into them satisfies gradually, the first thinning pass after
    /// this field starts being set collapses an existing over-budget history
    /// down to the budget in one go, and those deletions are irreversible.
    /// That is intended (it is how the ~30 MiB histories get reclaimed) but it
    /// is a one-way door, so it is documented for operators in
    /// `docs/secrets-at-rest.md` and overridable via [`SNAPSHOT_BUDGET_ENV`].
    /// It is also why [`Self::without_byte_budget`] exists: the RECOVERY path
    /// deliberately does not apply it.
    pub max_total_bytes: Option<u64>,
}

/// Resolve the per-secret byte budget from a raw [`SNAPSHOT_BUDGET_ENV`]
/// value.
///
/// - absent, empty, or whitespace-only → the built-in default (treated as
///   "operator said nothing").
/// - `0` → `None`, i.e. the budget is DISABLED and only the count tiers and
///   `max_age` bound the history. This is the escape hatch for an operator
///   who wants the pre-budget behavior back.
/// - any other valid `u64` → that many bytes.
/// - unparseable → the built-in default, with a warning.
///
/// Degrade-safe in one direction on purpose: a malformed value falls back to
/// the bounded default rather than to "unlimited" (which would silently
/// re-open the disk blowup) or to "0 bytes" (which would silently shrink
/// every history to the floor). Only an explicit, well-formed `0` disables
/// the budget.
pub fn parse_snapshot_budget(raw: Option<&str>) -> Option<u64> {
    let Some(raw) = raw else {
        return Some(DEFAULT_MAX_SNAPSHOT_BYTES_PER_SECRET);
    };
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        return Some(DEFAULT_MAX_SNAPSHOT_BYTES_PER_SECRET);
    }
    match trimmed.parse::<u64>() {
        // Explicit opt-out: count tiers + max_age still bound the history.
        Ok(0) => None,
        Ok(bytes) => Some(bytes),
        Err(err) => {
            tracing::warn!(
                env = SNAPSHOT_BUDGET_ENV,
                value = %trimmed,
                error = %err,
                "unparseable snapshot byte budget; falling back to the default"
            );
            Some(DEFAULT_MAX_SNAPSHOT_BYTES_PER_SECRET)
        }
    }
}

impl Default for RetentionPolicy {
    /// Default: keep the last 5 snapshots, plus one per minute for the last
    /// 10 minutes, one per hour for the last 24 hours, one per day for the
    /// last week, one per week for the last 4 weeks, one per month for the
    /// last 12 months, with an absolute 2-year ceiling so stale snapshots
    /// from secrets that stopped being written eventually age out, and a
    /// [`DEFAULT_MAX_SNAPSHOT_BYTES_PER_SECRET`] disk budget on top. Worst
    /// case ~62 entries per secret in steady state, and never more than
    /// `max(budget, MIN_SNAPSHOTS_KEPT_UNDER_BUDGET x largest snapshot)` of
    /// disk.
    ///
    /// PURE: every field is a compile-time constant, so two `default()`
    /// values are always identical. The operator override lives in
    /// [`RetentionPolicy::from_env`], deliberately NOT here — a `Default`
    /// impl that read `environ` would make every `SecretsStore` construction
    /// a `getenv`, which is a data race against any `setenv` in the same
    /// process (and unit tests are exactly where `setenv` happens).
    fn default() -> Self {
        const MIN: u64 = 60;
        const HOUR: u64 = 60 * MIN;
        const DAY: u64 = 24 * HOUR;
        const WEEK: u64 = 7 * DAY;
        const MONTH: u64 = 30 * DAY;
        const YEAR: u64 = 365 * DAY;
        Self {
            keep_last: 5,
            buckets: vec![
                RetentionBucket {
                    interval: Duration::from_secs(MIN),
                    max_count: 10,
                },
                RetentionBucket {
                    interval: Duration::from_secs(HOUR),
                    max_count: 24,
                },
                RetentionBucket {
                    interval: Duration::from_secs(DAY),
                    max_count: 7,
                },
                RetentionBucket {
                    interval: Duration::from_secs(WEEK),
                    max_count: 4,
                },
                RetentionBucket {
                    interval: Duration::from_secs(MONTH),
                    max_count: 12,
                },
            ],
            // Two years leaves clear headroom above the month bucket
            // (12 months ≈ 1 year) so the default month tier always gets
            // to keep its full 12 representatives. Beyond that, stale
            // ciphertext from secrets the user stopped writing to ages
            // out instead of lingering forever.
            max_age: Some(Duration::from_secs(2 * YEAR)),
            // Bounds the DISK the count tiers above would otherwise leave
            // unbounded: 62 versions is cheap for a small secret and ~62 MiB
            // for a ~1 MiB one. See the constant's docs for the sizing, and
            // `Self::from_env` for the operator override.
            max_total_bytes: Some(DEFAULT_MAX_SNAPSHOT_BYTES_PER_SECRET),
        }
    }
}

impl RetentionPolicy {
    /// [`Self::default`] with the byte budget taken from a raw
    /// [`SNAPSHOT_BUDGET_ENV`] string, per [`parse_snapshot_budget`].
    ///
    /// Split out from [`Self::from_env`] so the override is testable without
    /// touching process-global `environ`: `setenv` races every concurrent
    /// `getenv` in the process, and in a lib-test binary those `getenv`s are
    /// other tests constructing stores. Tests call this; only `from_env`
    /// reads the environment.
    pub fn with_budget_from(raw: Option<&str>) -> Self {
        Self {
            max_total_bytes: parse_snapshot_budget(raw),
            ..Self::default()
        }
    }

    /// [`Self::default`] with the operator's [`SNAPSHOT_BUDGET_ENV`] override
    /// applied.
    ///
    /// This is the ONLY place in this module that reads the environment, and
    /// it has no test that mutates `environ` — see [`Self::with_budget_from`]
    /// for why. Production stores are built from this (see
    /// `SecretsStore::new`); anything that thins with a real byte budget
    /// should use it rather than [`Self::default`], or the operator's
    /// override silently stops applying.
    pub fn from_env() -> Self {
        Self::with_budget_from(std::env::var(SNAPSHOT_BUDGET_ENV).ok().as_deref())
    }

    /// This policy with the byte budget removed; the count tiers and
    /// `max_age` are untouched.
    ///
    /// Used by the RECOVERY path (`SecretsStore::restore_snapshot` and
    /// `freenet secrets snapshot-restore`). A restore is what an operator
    /// runs when they are trying to get a value back, and it is the worst
    /// possible moment to garbage-collect history: applying the budget there
    /// could evict several older versions — including the one just restored
    /// FROM — as a side effect of the reversibility snapshot the restore
    /// itself adds. The budget is a disk-pressure heuristic, and a manual
    /// restore is not disk pressure.
    ///
    /// This is not an unbounded cleanup exemption: `max_age` still applies at
    /// the restore, so every entry keeps a finite lifetime, and the exemption
    /// is per-CALL, not a property stamped on the history.
    ///
    /// # How long the exemption actually holds
    ///
    /// Be precise about this, because the two restore entry points differ:
    ///
    /// - **`freenet secrets snapshot-restore` (the operator-facing path):**
    ///   the exemption holds for as long as the operator needs. The CLI
    ///   requires the node to be STOPPED (see `docs/secrets-at-rest.md`), so
    ///   no delegate write can intervene, and an operator can list, restore,
    ///   inspect, and restore again across the full retained history.
    ///
    /// - **`SecretsStore::restore_snapshot` (the in-process API):** the
    ///   exemption lasts only until the next `store_secret` for that secret.
    ///   `store_secret` thins with the FULL policy on every call — it is
    ///   gated on whether snapshots are enabled, not on whether this
    ///   particular write took a snapshot — so a live node whose delegate
    ///   touches that secret collapses the restored history to the floor,
    ///   including on one of the identical re-writes that skip snapshotting.
    ///   Do NOT read this method as buying a working window on a live node.
    ///   (Today that API has no production callers; it is exercised by tests
    ///   and available to embedders.)
    ///
    /// Gating `store_secret`'s thin on `needs_snapshot` — i.e. not
    /// garbage-collecting on a write that adds nothing to the history —
    /// would widen the live-node window. It is deliberately NOT done here:
    /// it is a real semantic change to when reclaim happens, the supported
    /// recovery procedure is the stopped-node CLI where it buys nothing, and
    /// a write that skips its snapshot still leaves a history that the
    /// operator's configured budget says is too large.
    pub fn without_byte_budget(&self) -> Self {
        Self {
            max_total_bytes: None,
            ..self.clone()
        }
    }

    /// Given `timestamps` sorted ascending, return the indices to KEEP.
    /// Indices not in the returned set should be deleted.
    ///
    /// Algorithm: walk newest-first. The first snapshot encountered in each
    /// `interval`-wide age slot wins that slot; subsequent snapshots in the
    /// same slot are eligible for deletion (unless covered by another tier
    /// or `keep_last`). Per tier, stop after `max_count` distinct slots.
    /// Finally, `max_age` filters: any otherwise-kept entry older than
    /// `now - max_age` is dropped.
    pub fn select_keep(&self, now: SystemTime, timestamps: &[SystemTime]) -> BTreeSet<usize> {
        let mut keep = BTreeSet::new();
        let n = timestamps.len();

        for i in n.saturating_sub(self.keep_last)..n {
            keep.insert(i);
        }

        for bucket in &self.buckets {
            if bucket.max_count == 0 {
                continue;
            }
            let secs = bucket.interval.as_secs().max(1);
            let mut slots_seen: HashSet<u64> = HashSet::new();
            for (i, ts) in timestamps.iter().enumerate().rev() {
                let age = now.duration_since(*ts).unwrap_or_default().as_secs();
                let slot = age / secs;
                if slots_seen.insert(slot) {
                    keep.insert(i);
                    if slots_seen.len() >= bucket.max_count {
                        break;
                    }
                }
            }
        }

        // Absolute age cap. Applied as a final filter so it overrides
        // every other clause. This is what satisfies AGENTS.md's rule
        // that GC exemptions must be time-bounded: even `keep_last`
        // entries are dropped once they cross `max_age`.
        // `duration_since` returns Err when the timestamp is in the
        // future (clock skew). We keep those rather than panic — they
        // are by definition not stale.
        if let Some(max_age) = self.max_age {
            keep.retain(|&i| {
                now.duration_since(timestamps[i])
                    .map(|age| age <= max_age)
                    .unwrap_or(true)
            });
        }

        keep
    }

    /// [`Self::select_keep`] plus the [`Self::max_total_bytes`] disk budget.
    ///
    /// `entries` is `(timestamp, size_bytes)` sorted ASCENDING by timestamp
    /// (same contract as `select_keep`'s `timestamps`); the returned indices
    /// are into `entries`. Everything `select_keep` already dropped stays
    /// dropped — the budget only ever removes MORE, so a snapshot excluded
    /// by `max_age` can never be resurrected by having spare byte budget.
    ///
    /// Eviction order is OLDEST-first, because the newest snapshot is the
    /// value the most recent write replaced and is what an operator undoing
    /// a bad overwrite reaches for. The floor at
    /// [`MIN_SNAPSHOTS_KEPT_UNDER_BUDGET`] means a secret whose single
    /// version is larger than the whole budget keeps that version and
    /// overshoots the budget, rather than being left with no history: the
    /// budget is a disk heuristic and must never be the reason user data
    /// stops being recoverable.
    pub fn select_keep_within_budget(
        &self,
        now: SystemTime,
        entries: &[(SystemTime, u64)],
    ) -> BTreeSet<usize> {
        let timestamps: Vec<SystemTime> = entries.iter().map(|(ts, _)| *ts).collect();
        let mut keep = self.select_keep(now, &timestamps);

        let Some(budget) = self.max_total_bytes else {
            return keep;
        };

        let mut total = keep
            .iter()
            .fold(0u64, |acc, &i| acc.saturating_add(entries[i].1));
        // `BTreeSet<usize>` iterates ascending, and `entries` is sorted
        // oldest-first, so this walks candidates from oldest to newest.
        let oldest_first: Vec<usize> = keep.iter().copied().collect();
        for i in oldest_first {
            if total <= budget || keep.len() <= MIN_SNAPSHOTS_KEPT_UNDER_BUDGET {
                break;
            }
            keep.remove(&i);
            total = total.saturating_sub(entries[i].1);
        }

        keep
    }
}

/// Path to the snapshot directory for a delegate secret, keyed on the
/// secret's on-disk encoded id (bs58 of the secret hash).
///
/// This is the form the `freenet secrets` CLI and the filesystem-level
/// [`restore_snapshot_file`] use: a [`SecretsId`] cannot be reconstructed
/// from the on-disk directory name alone (the pre-image `key` bytes are
/// not persisted, only their hash), so any tool that walks the secrets
/// tree must work from the encoded id string.
pub fn snapshot_dir_for_encoded(delegate_path: &Path, secret_encoded: &str) -> PathBuf {
    delegate_path.join(SNAPSHOTS_DIR).join(secret_encoded)
}

/// Path to the snapshot directory for a particular `(delegate, secret_id)`
/// pair. Snapshots are organized per secret so retention thinning of one
/// busy key cannot affect another.
pub fn snapshot_dir_for(delegate_path: &Path, key: &SecretsId) -> PathBuf {
    snapshot_dir_for_encoded(delegate_path, &key.encode())
}

/// Pick the snapshot file path for a write happening "now". Uses the
/// current epoch-millis as the base name, falling back to numeric
/// collision suffixes when multiple writes land in the same millisecond.
///
/// # Errors
/// `AlreadyExists` if [`MAX_SNAPSHOT_COLLISION_SUFFIX`] consecutive
/// suffixes are also taken — extremely unlikely in practice, but the
/// caller (snapshot subsystem) treats this as a best-effort failure and
/// logs rather than silently overwriting an existing snapshot.
pub fn next_snapshot_path(snap_dir: &Path) -> std::io::Result<PathBuf> {
    let stamp = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64;
    let unsuffixed = snap_dir.join(format!("{stamp:0width$}", width = SNAPSHOT_NAME_WIDTH));
    if !unsuffixed.exists() {
        return Ok(unsuffixed);
    }
    for suffix in 0u32..MAX_SNAPSHOT_COLLISION_SUFFIX {
        let candidate = snap_dir.join(format!(
            "{stamp:0width$}.{suffix}",
            width = SNAPSHOT_NAME_WIDTH
        ));
        if !candidate.exists() {
            return Ok(candidate);
        }
    }
    Err(std::io::Error::new(
        std::io::ErrorKind::AlreadyExists,
        format!(
            "snapshot path collision exhausted: {} already has {MAX_SNAPSHOT_COLLISION_SUFFIX} entries with stamp {stamp}",
            snap_dir.display()
        ),
    ))
}

/// Apply the retention policy to a snapshot directory: delete any file the
/// `policy` does not select relative to `now`, counting both its timestamp
/// (count tiers + `max_age`) and its on-disk size (`max_total_bytes`).
/// Files whose names don't parse as `{epoch_ms}` (with optional
/// `.{counter}` suffix) and non-regular-file entries are left untouched.
///
/// Best-effort: I/O errors during enumeration or unlink are logged but
/// do not propagate, since thinning is a maintenance operation that must
/// never fail the primary write path.
pub fn thin_snapshots(snap_dir: &Path, policy: &RetentionPolicy, now: SystemTime) {
    /// One candidate entry for a thinning pass.
    struct Candidate {
        timestamp: SystemTime,
        /// Collision-suffix ordering key for entries sharing `timestamp`.
        /// `(0, 0)` for the unsuffixed file, `(1, n)` for `.n` — the exact
        /// key [`list_snapshots`] sorts by. NUMERIC on purpose: sorting the
        /// file names as text puts `.10` before `.9`, which would let the
        /// byte budget evict the numerically-newest entry of a
        /// same-millisecond burst and keep an older sibling.
        suffix_key: (u8, u32),
        size_bytes: u64,
        path: PathBuf,
    }

    let mut entries: Vec<Candidate> = match fs::read_dir(snap_dir) {
        Ok(rd) => rd
            .filter_map(|res| match res {
                Ok(entry) => Some(entry),
                Err(err) => {
                    tracing::debug!("snapshot dir entry error in {snap_dir:?}: {err}");
                    None
                }
            })
            .filter_map(|entry| {
                // Skip directories, symlinks, anything that isn't a regular
                // file. A subdirectory whose name happens to be all digits
                // would otherwise produce a confusing "remove_file failed"
                // log on every thin pass.
                let is_file = entry.file_type().map(|ft| ft.is_file()).unwrap_or(false);
                if !is_file {
                    return None;
                }
                let path = entry.path();
                let (stamp, suffix) = parse_snapshot_name(&path)?;
                // A stat failure is charged as 0 bytes: it makes the entry
                // free against `max_total_bytes`, so it is RETAINED and some
                // other (measurable) entry is evicted instead. Erring toward
                // keeping is the right default for a best-effort disk
                // heuristic operating on user data.
                let size_bytes = match entry.metadata() {
                    Ok(md) => md.len(),
                    Err(err) => {
                        tracing::debug!(
                            "failed to stat snapshot {path:?}: {err}; charging 0 bytes"
                        );
                        0
                    }
                };
                Some(Candidate {
                    timestamp: UNIX_EPOCH + Duration::from_millis(stamp),
                    suffix_key: match suffix {
                        None => (0, 0),
                        Some(n) => (1, n),
                    },
                    size_bytes,
                    path,
                })
            })
            .collect(),
        Err(err) => {
            tracing::warn!("failed to read snapshot dir {snap_dir:?}: {err}");
            return;
        }
    };
    // Total order, not just by timestamp: same-millisecond collision entries
    // share a stamp, and the byte budget evicts oldest-first, so the tie has
    // to break deterministically — in the SAME order `list_snapshots` and the
    // restore disambiguation use, so "the newest snapshot" names the same
    // entry everywhere.
    entries.sort_by(|a, b| {
        a.timestamp
            .cmp(&b.timestamp)
            .then_with(|| a.suffix_key.cmp(&b.suffix_key))
    });

    let sized: Vec<(SystemTime, u64)> = entries
        .iter()
        .map(|c| (c.timestamp, c.size_bytes))
        .collect();
    let keep = policy.select_keep_within_budget(now, &sized);
    for (i, candidate) in entries.iter().enumerate() {
        if !keep.contains(&i) {
            if let Err(err) = fs::remove_file(&candidate.path) {
                tracing::warn!("failed to thin snapshot {:?}: {err}", candidate.path);
            }
        }
    }
}

/// Enumerate snapshots in `snap_dir`, sorted oldest-first.
///
/// Filenames that don't parse as `{digits}` or `{digits}.{digits}` and
/// non-regular-file entries are silently skipped (same rule that
/// [`thin_snapshots`] applies).
///
/// Returns an empty vector if `snap_dir` does not exist: a never-written
/// secret simply has no history, which is not an error.
///
/// # Errors
/// Surfaces every I/O error from enumerating the directory or stat'ing
/// individual entries. This is a read-only operation and the caller
/// (CLI, restore) needs to know if the disk is misbehaving rather than
/// silently truncating the listing.
pub fn list_snapshots(snap_dir: &Path) -> std::io::Result<Vec<SnapshotMetadata>> {
    let read_dir = match fs::read_dir(snap_dir) {
        Ok(rd) => rd,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(Vec::new()),
        Err(e) => return Err(e),
    };
    let mut out: Vec<SnapshotMetadata> = Vec::new();
    for entry in read_dir {
        let entry = entry?;
        let file_type = entry.file_type()?;
        if !file_type.is_file() {
            continue;
        }
        let path = entry.path();
        let Some((timestamp_ms, suffix)) = parse_snapshot_name(&path) else {
            continue;
        };
        let size_bytes = entry.metadata()?.len();
        out.push(SnapshotMetadata {
            timestamp_ms,
            suffix,
            path,
            size_bytes,
        });
    }
    // Sort key: `None` sorts before `Some(_)` (unsuffixed file came first
    // chronologically — it's the write that landed on a fresh stamp; the
    // suffixed variants came from later same-millisecond collisions).
    out.sort_by_key(|m| {
        (
            m.timestamp_ms,
            match m.suffix {
                None => (0u8, 0u32),
                Some(s) => (1, s),
            },
        )
    });
    Ok(out)
}

/// Parse a snapshot file name back into its epoch-millis timestamp and
/// optional numeric collision suffix.
///
/// Accepts only `{digits}` or `{digits}.{digits}`; any other shape
/// (including `42.tmp`, `foo`, `1.2.3`, etc.) returns `None` so stray
/// files in the snapshot directory are not mistaken for snapshots.
///
/// The suffix is surfaced through [`SnapshotMetadata`] so callers can
/// disambiguate multiple writes that landed in the same millisecond, and
/// [`thin_snapshots`] orders on it so its eviction agrees with what
/// [`list_snapshots`] reports.
pub(crate) fn parse_snapshot_name(path: &Path) -> Option<(u64, Option<u32>)> {
    let name = path.file_name()?.to_str()?;
    let (stamp_part, suffix_part) = match name.split_once('.') {
        Some((s, t)) => (s, Some(t)),
        None => (name, None),
    };
    if stamp_part.is_empty() || !stamp_part.bytes().all(|b| b.is_ascii_digit()) {
        return None;
    }
    let suffix = match suffix_part {
        Some(s) => {
            if s.is_empty() || !s.bytes().all(|b| b.is_ascii_digit()) {
                return None;
            }
            Some(s.parse().ok()?)
        }
        None => None,
    };
    Some((stamp_part.parse().ok()?, suffix))
}

/// Capture the current active secret file as a snapshot so an overwrite
/// (a `store_secret`, or a [`restore_snapshot_file`]) is reversible.
///
/// Hard-links the active inode into `.snapshots/{secret_encoded}/`
/// (falling back to a copy on filesystems without hard links, e.g. FAT
/// or some network mounts), after tightening both the `.snapshots/`
/// umbrella and the per-secret leaf to owner-only. `active_path` must be
/// the live secret file (`delegate_dir/{secret_encoded}`); a missing
/// active file is a no-op (nothing to preserve).
///
/// The active file is never mutated here, so a crash mid-snapshot loses
/// only the snapshot, never the live value. Best-effort by contract of
/// its callers: they log and continue on error rather than failing the
/// primary write/restore.
pub fn snapshot_active_value(
    delegate_dir: &Path,
    secret_encoded: &str,
    active_path: &Path,
) -> std::io::Result<()> {
    let snap_dir = snapshot_dir_for_encoded(delegate_dir, secret_encoded);
    fs::create_dir_all(&snap_dir)?;
    // Tighten BOTH the intermediate `.snapshots/` umbrella AND the
    // per-secret leaf. Only chmodding the leaf leaves `.snapshots/` at
    // the process umask (typically 0o755), which lets any local user
    // enumerate per-secret subdir names, write counts (epoch_ms
    // filenames), and write timing.
    let snap_parent = delegate_dir.join(SNAPSHOTS_DIR);
    if let Err(e) = ensure_owner_only_dir(&snap_parent) {
        tracing::warn!(path = %snap_parent.display(), error = %e, "chmod snapshots parent dir failed");
    }
    if let Err(e) = ensure_owner_only_dir(&snap_dir) {
        tracing::warn!(path = %snap_dir.display(), error = %e, "chmod snapshot dir failed");
    }
    let snap_path = next_snapshot_path(&snap_dir)?;
    match fs::hard_link(active_path, &snap_path) {
        Ok(()) => Ok(()),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(_) => {
            // Hard-link unsupported (FAT, cross-device, etc.). Copy is
            // slower but always works; the active file is not mutated
            // here so the copy can't tear.
            fs::copy(active_path, &snap_path).map(|_| ())
        }
    }
}

/// Error from the filesystem-level [`restore_snapshot_file`].
#[derive(Debug, thiserror::Error)]
pub enum RestoreError {
    /// No snapshot in the directory matched the requested `timestamp_ms`.
    #[error("no snapshot at timestamp_ms {0}")]
    NotFound(u64),
    /// Filesystem error during the find / copy / rename / fsync sequence.
    #[error(transparent)]
    Io(#[from] std::io::Error),
}

/// Restore the snapshot matching `timestamp_ms` onto the active secret
/// path, purely at the filesystem level — no cipher, no ReDb index.
///
/// `delegate_dir` is `<secrets_dir>/<delegate_encoded>`; `secret_encoded`
/// is the on-disk secret file name (bs58 of the secret hash). The active
/// secret file is `delegate_dir/{secret_encoded}`; its snapshot history
/// lives in `delegate_dir/.snapshots/{secret_encoded}/`.
///
/// Mirrors the durability discipline of `SecretsStore::store_secret`:
/// 1. snapshot the current active value first (so the restore is itself
///    reversible) when `snapshots_enabled`,
/// 2. copy the chosen snapshot to a sibling `.tmp`, fsync, then
///    atomically rename onto the active path.
///
/// History thinning is intentionally NOT done here — callers thin AFTER
/// their own post-restore bookkeeping. The node runtime thins only after
/// its ReDb index repair commits, so a failed repair cannot prune source
/// history a retry would need; the CLI thins right after a successful
/// restore. (Keeping thin out of the shared core is what makes the
/// runtime path byte-for-byte order-preserving vs. the pre-extraction
/// inline implementation.)
///
/// `suffix` selects among same-millisecond collision entries (which
/// [`list_snapshots`] / `snapshot-list` surface as the `suffix` column):
/// - `None` — the unsuffixed file wins, then the lowest-numbered suffix
///   (the historical `SecretsStore::restore_snapshot` behavior; the
///   common case, since a timestamp without collisions has exactly one
///   entry).
/// - `Some(n)` — restore exactly the `.n` collision entry, so an operator
///   can target a specific row from the listing rather than silently
///   getting the unsuffixed one.
///
/// Shared by `SecretsStore::restore_snapshot` (node runtime; passes
/// `None` and adds the in-memory + ReDb index repair) and the `freenet
/// secrets snapshot-restore` CLI (node stopped). Byte-level copy: the
/// restored ciphertext stays decryptable by whatever cipher wrote it.
///
/// # Errors
/// - [`RestoreError::NotFound`] if no snapshot matches `timestamp_ms`
///   (and, when `suffix` is `Some(n)`, the `.n` entry).
/// - [`RestoreError::Io`] for filesystem errors during the restore.
pub fn restore_snapshot_file(
    delegate_dir: &Path,
    secret_encoded: &str,
    timestamp_ms: u64,
    suffix: Option<u32>,
    snapshots_enabled: bool,
) -> Result<(), RestoreError> {
    let snap_dir = snapshot_dir_for_encoded(delegate_dir, secret_encoded);
    let secret_file_path = delegate_dir.join(secret_encoded);

    let entries = list_snapshots(&snap_dir)?;
    let chosen = match suffix {
        // Explicit selector: the exact `.n` collision entry. A missing
        // `.n` is NotFound (never a silent fallback to the unsuffixed
        // file, which would restore the wrong ciphertext).
        Some(want) => entries
            .iter()
            .find(|m| m.timestamp_ms == timestamp_ms && m.suffix == Some(want)),
        // Default disambiguation: unsuffixed file wins, then lowest-
        // numbered suffix. `None` sorts before `Some(_)` via the (0,0) key.
        None => entries
            .iter()
            .filter(|m| m.timestamp_ms == timestamp_ms)
            .min_by_key(|m| match m.suffix {
                None => (0u32, 0u32),
                Some(s) => (1, s),
            }),
    }
    .ok_or(RestoreError::NotFound(timestamp_ms))?;
    let chosen_path = chosen.path.clone();

    // Snapshot the value currently at the active path so the restore is
    // itself reversible. Best-effort: a failure here must not fail the
    // restore (the primary operation), only forfeit reversibility.
    if snapshots_enabled
        && secret_file_path.exists()
        && let Err(e) = snapshot_active_value(delegate_dir, secret_encoded, &secret_file_path)
    {
        tracing::warn!("failed to snapshot active value before restore for {secret_encoded}: {e}");
    }

    // Read snapshot ciphertext, write through a sibling tmp file with an
    // atomic rename so the active path never tears. `create_owner_only`
    // unlinks any surviving `.tmp` from a prior crashed run so the new
    // inode always lands at mode 0o600.
    let ciphertext = fs::read(&chosen_path)?;
    fs::create_dir_all(delegate_dir)?;
    if let Err(e) = ensure_owner_only_dir(delegate_dir) {
        tracing::warn!(path = %delegate_dir.display(), error = %e, "chmod delegate dir failed");
    }
    let tmp_path = secret_file_path.with_extension("tmp");
    {
        let mut file = create_owner_only(&tmp_path)?;
        file.write_all(&ciphertext)?;
        file.sync_all()?;
    }
    if let Err(err) = fs::rename(&tmp_path, &secret_file_path) {
        if let Err(rm_err) = fs::remove_file(&tmp_path) {
            tracing::debug!(
                "failed to clean up tmp file {tmp_path:?} after rename failure: {rm_err}"
            );
        }
        return Err(err.into());
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn t(now: SystemTime, secs_ago: u64) -> SystemTime {
        now - Duration::from_secs(secs_ago)
    }

    #[test]
    fn empty_input_keeps_nothing() {
        let p = RetentionPolicy::default();
        let now = SystemTime::now();
        assert!(p.select_keep(now, &[]).is_empty());
    }

    #[test]
    fn keeps_last_n_unconditionally() {
        let p = RetentionPolicy {
            keep_last: 3,
            buckets: vec![],
            max_age: None,
            max_total_bytes: None,
        };
        let now = SystemTime::now();
        // Five very-old snapshots: only the trailing 3 should survive.
        let ts: Vec<_> = (0..5).map(|i| t(now, 1_000_000 - i)).collect();
        let keep = p.select_keep(now, &ts);
        assert_eq!(keep.into_iter().collect::<Vec<_>>(), vec![2, 3, 4]);
    }

    #[test]
    fn minute_bucket_thins_dense_history() {
        // 600 snapshots at 1 second apart → 10 minutes of history.
        // Minute bucket with max_count=10 should pick one per minute slot.
        let p = RetentionPolicy {
            keep_last: 0,
            buckets: vec![RetentionBucket {
                interval: Duration::from_secs(60),
                max_count: 10,
            }],
            max_age: None,
            max_total_bytes: None,
        };
        let now = SystemTime::now();
        let ts: Vec<_> = (0..600).map(|i| t(now, 599 - i)).collect();
        let keep = p.select_keep(now, &ts);
        assert_eq!(keep.len(), 10, "expected one snapshot per minute slot");
    }

    #[test]
    fn burst_in_single_slot_collapses_to_one() {
        // 1000 snapshots all within the same second.
        let p = RetentionPolicy {
            keep_last: 0,
            buckets: vec![RetentionBucket {
                interval: Duration::from_secs(60),
                max_count: 10,
            }],
            max_age: None,
            max_total_bytes: None,
        };
        let now = SystemTime::now();
        let ts: Vec<_> = (0..1000).map(|_| t(now, 5)).collect();
        let keep = p.select_keep(now, &ts);
        assert_eq!(keep.len(), 1);
    }

    #[test]
    fn default_policy_caps_steady_state() {
        // One snapshot per minute for a year → 525_600 entries.
        // Default policy should keep at most ~60-ish.
        let p = RetentionPolicy::default();
        let now = SystemTime::now();
        let ts: Vec<_> = (0..525_600).map(|i| t(now, (525_599 - i) * 60)).collect();
        let keep = p.select_keep(now, &ts);
        assert!(
            keep.len() <= 70,
            "default policy should bound steady-state retention; got {}",
            keep.len()
        );
        assert!(
            keep.len() >= 30,
            "but should still preserve coverage across all tiers; got {}",
            keep.len()
        );
    }

    #[test]
    fn future_timestamps_treated_as_age_zero() {
        // Clock skew: a snapshot timestamped after `now`. unwrap_or_default
        // makes its age 0; it just lands in slot 0 like any recent snapshot.
        let p = RetentionPolicy {
            keep_last: 0,
            buckets: vec![RetentionBucket {
                interval: Duration::from_secs(60),
                max_count: 5,
            }],
            max_age: None,
            max_total_bytes: None,
        };
        let now = SystemTime::now();
        let ts = vec![now + Duration::from_secs(120), t(now, 30)];
        let keep = p.select_keep(now, &ts);
        // Both fall in slot 0 → only the newest (index 1, since input is by
        // call-order not timestamp) wins, but the algorithm walks rev so the
        // last index wins the slot. Either way exactly one survives.
        assert_eq!(keep.len(), 1);
    }

    /// `max_age` MUST override every other retention clause, including
    /// `keep_last`. AGENTS.md cleanup-exemption rule: time-bound it or
    /// don't ship it.
    #[test]
    fn max_age_overrides_keep_last() {
        let p = RetentionPolicy {
            keep_last: 5,
            buckets: vec![],
            max_age: Some(Duration::from_secs(60)),
            max_total_bytes: None,
        };
        let now = SystemTime::now();
        // 5 snapshots, all 1 hour old. keep_last=5 would normally keep all,
        // but max_age=60s drops every one.
        let ts: Vec<_> = (0..5).map(|i| t(now, 3600 - i)).collect();
        let keep = p.select_keep(now, &ts);
        assert!(
            keep.is_empty(),
            "max_age must trim stale entries even from keep_last"
        );
    }

    /// Future-dated snapshots (clock skew) are not dropped by max_age:
    /// `duration_since` returns Err, treated as "not stale".
    #[test]
    fn max_age_preserves_future_timestamps() {
        let p = RetentionPolicy {
            keep_last: 1,
            buckets: vec![],
            max_age: Some(Duration::from_secs(60)),
            max_total_bytes: None,
        };
        let now = SystemTime::now();
        let ts = vec![now + Duration::from_secs(120)];
        let keep = p.select_keep(now, &ts);
        assert_eq!(keep.len(), 1, "future-dated snapshot must survive max_age");
    }

    /// Mix of fresh and stale entries: only the fresh ones survive even
    /// when keep_last would have selected the stale ones too.
    #[test]
    fn max_age_drops_only_stale_entries() {
        let p = RetentionPolicy {
            keep_last: 10,
            buckets: vec![],
            max_age: Some(Duration::from_secs(120)),
            max_total_bytes: None,
        };
        let now = SystemTime::now();
        // 3 fresh (within 2 min), 3 stale (> 2 min). keep_last=10 selects
        // all 6, max_age=120s drops the stale half.
        let ts = vec![
            t(now, 1000), // stale
            t(now, 500),  // stale
            t(now, 200),  // stale
            t(now, 60),   // fresh
            t(now, 30),   // fresh
            t(now, 5),    // fresh
        ];
        let keep = p.select_keep(now, &ts);
        assert_eq!(
            keep.into_iter().collect::<Vec<_>>(),
            vec![3, 4, 5],
            "only fresh entries should remain"
        );
    }

    // ===== per-secret byte budget (`max_total_bytes`) =====

    /// Helper: a policy that keeps everything by count, so the tests below
    /// isolate the byte budget from the tiering.
    fn budget_only(max_total_bytes: Option<u64>) -> RetentionPolicy {
        RetentionPolicy {
            keep_last: usize::MAX,
            buckets: vec![],
            max_age: None,
            max_total_bytes,
        }
    }

    #[test]
    fn byte_budget_evicts_oldest_first() {
        let now = SystemTime::now();
        // Eight 100-byte snapshots, budget 450 => the four newest fit. Sized
        // so the answer is the BUDGET's, not the floor's.
        let entries: Vec<(SystemTime, u64)> =
            (0..8).map(|i| (t(now, 800 - i * 100), 100u64)).collect();
        let keep = budget_only(Some(450)).select_keep_within_budget(now, &entries);
        assert_eq!(
            keep.into_iter().collect::<Vec<_>>(),
            vec![4, 5, 6, 7],
            "budget must drop the OLDEST entries and keep the newest"
        );
    }

    /// The floor stops eviction at [`MIN_SNAPSHOTS_KEPT_UNDER_BUDGET`] even
    /// when the survivors blow the budget wide open. A disk heuristic must
    /// never be the reason an operator has no version to walk back to.
    #[test]
    fn byte_budget_floor_keeps_three_over_budget() {
        let now = SystemTime::now();
        // Ten 1 MiB snapshots against a 1-byte budget.
        let entries: Vec<(SystemTime, u64)> = (0..10)
            .map(|i| (t(now, (10 - i) * 100), 1024 * 1024u64))
            .collect();
        let keep = budget_only(Some(1)).select_keep_within_budget(now, &entries);
        assert_eq!(
            keep.iter().copied().collect::<Vec<_>>(),
            vec![7, 8, 9],
            "floor must retain the three NEWEST entries, not just any three"
        );
        assert_eq!(keep.len(), MIN_SNAPSHOTS_KEPT_UNDER_BUDGET);
    }

    #[test]
    fn byte_budget_none_keeps_everything() {
        let now = SystemTime::now();
        let entries: Vec<(SystemTime, u64)> = (0..4)
            .map(|i| (t(now, 400 - i * 100), 10_000_000u64))
            .collect();
        let keep = budget_only(None).select_keep_within_budget(now, &entries);
        assert_eq!(keep.len(), 4, "no budget => count tiers alone decide");
    }

    #[test]
    fn byte_budget_never_empties_the_history() {
        // A single snapshot far larger than the whole budget is KEPT: the
        // budget is a disk heuristic and must never be why a value stops
        // being recoverable.
        let now = SystemTime::now();
        let entries = vec![(t(now, 10), 50_000_000u64)];
        let keep = budget_only(Some(1024)).select_keep_within_budget(now, &entries);
        assert_eq!(keep.into_iter().collect::<Vec<_>>(), vec![0]);
    }

    #[test]
    fn byte_budget_zero_still_keeps_the_floor() {
        // Degenerate budget: floor's worth of entries survive, and they are
        // the newest (the values the last writes overwrote).
        let now = SystemTime::now();
        let entries: Vec<(SystemTime, u64)> =
            (0..5).map(|i| (t(now, 500 - i * 100), 100u64)).collect();
        let keep = budget_only(Some(0)).select_keep_within_budget(now, &entries);
        assert_eq!(keep.into_iter().collect::<Vec<_>>(), vec![2, 3, 4]);
    }

    #[test]
    fn byte_budget_exactly_at_budget_keeps_all() {
        // Boundary: total == budget is NOT over budget.
        let now = SystemTime::now();
        let entries: Vec<(SystemTime, u64)> =
            (0..5).map(|i| (t(now, 500 - i * 100), 100u64)).collect();
        let keep = budget_only(Some(500)).select_keep_within_budget(now, &entries);
        assert_eq!(keep.len(), 5);
        // One byte less and exactly one entry — the oldest — goes.
        let keep = budget_only(Some(499)).select_keep_within_budget(now, &entries);
        assert_eq!(keep.into_iter().collect::<Vec<_>>(), vec![1, 2, 3, 4]);
    }

    #[test]
    fn byte_budget_empty_input_keeps_nothing() {
        let now = SystemTime::now();
        assert!(
            budget_only(Some(1024))
                .select_keep_within_budget(now, &[])
                .is_empty()
        );
    }

    #[test]
    fn byte_budget_only_subtracts_never_resurrects() {
        // An entry dropped by `max_age` must stay dropped even though there
        // is plenty of byte budget left over.
        let p = RetentionPolicy {
            keep_last: 10,
            buckets: vec![],
            max_age: Some(Duration::from_secs(120)),
            max_total_bytes: Some(u64::MAX),
        };
        let now = SystemTime::now();
        let entries = vec![
            (t(now, 1000), 1u64), // stale
            (t(now, 500), 1),     // stale
            (t(now, 30), 1),      // fresh
        ];
        let keep = p.select_keep_within_budget(now, &entries);
        assert_eq!(keep.into_iter().collect::<Vec<_>>(), vec![2]);
    }

    #[test]
    fn byte_budget_counts_only_entries_the_tiers_kept() {
        // Entries the count tiers already dropped must not consume budget:
        // keep_last=2 leaves two 100-byte entries, which fit a 250 budget
        // even though the eight dropped ones would have blown it.
        let p = RetentionPolicy {
            keep_last: 2,
            buckets: vec![],
            max_age: None,
            max_total_bytes: Some(250),
        };
        let now = SystemTime::now();
        let entries: Vec<(SystemTime, u64)> =
            (0..10).map(|i| (t(now, 1000 - i * 10), 100u64)).collect();
        let keep = p.select_keep_within_budget(now, &entries);
        assert_eq!(keep.into_iter().collect::<Vec<_>>(), vec![8, 9]);
    }

    #[test]
    fn byte_budget_saturates_on_overflowing_sizes() {
        // Pathological sizes must not panic on overflow in debug builds.
        //
        // The running total saturates at `u64::MAX`, so subtracting the first
        // evicted entry's size takes it straight to 0 and the loop stops
        // early. Saturation therefore makes the budget UNDER-evict (keep
        // more), never over-evict — the correct direction for a disk
        // heuristic operating on user data. Only reachable with sizes summing
        // past 2^64 bytes, i.e. never in practice; what is asserted here is
        // the absence of a panic and the safe failure direction, not an exact
        // survivor set (which would be pinning the artifact).
        let now = SystemTime::now();
        let entries: Vec<(SystemTime, u64)> =
            (0..5).map(|i| (t(now, 500 - i * 100), u64::MAX)).collect();
        let keep = budget_only(Some(1024)).select_keep_within_budget(now, &entries);
        assert!(
            keep.len() >= MIN_SNAPSHOTS_KEPT_UNDER_BUDGET,
            "saturation must never evict below the floor; kept {}",
            keep.len()
        );
        assert!(
            keep.contains(&4),
            "the newest entry must survive regardless"
        );
    }

    // ===== `SNAPSHOT_BUDGET_ENV` parsing =====

    #[test]
    fn budget_env_absent_or_blank_uses_the_default() {
        for raw in [None, Some(""), Some("   "), Some("\t\n")] {
            assert_eq!(
                parse_snapshot_budget(raw),
                Some(DEFAULT_MAX_SNAPSHOT_BYTES_PER_SECRET),
                "raw={raw:?} must fall back to the default budget"
            );
        }
    }

    #[test]
    fn budget_env_zero_disables_the_budget() {
        // The escape hatch for an operator who wants the pre-budget
        // behaviour back: count tiers + max_age only.
        assert_eq!(parse_snapshot_budget(Some("0")), None);
        assert_eq!(parse_snapshot_budget(Some("  0  ")), None);
    }

    #[test]
    fn budget_env_accepts_explicit_byte_counts() {
        assert_eq!(parse_snapshot_budget(Some("1")), Some(1));
        assert_eq!(parse_snapshot_budget(Some("12345678")), Some(12_345_678));
        assert_eq!(parse_snapshot_budget(Some(" 4096 ")), Some(4096));
        assert_eq!(
            parse_snapshot_budget(Some(&u64::MAX.to_string())),
            Some(u64::MAX)
        );
        // `u64::from_str` accepts a leading `+`, and "+5" is an unambiguous
        // way of writing 5 bytes, so it is honoured rather than rejected.
        assert_eq!(parse_snapshot_budget(Some("+5")), Some(5));
    }

    /// Garbage must degrade to the DEFAULT, never to "unlimited" (which
    /// would silently re-open the disk blowup) and never to "0 bytes"
    /// (which would silently shrink every history to the floor).
    #[test]
    fn budget_env_garbage_degrades_to_the_default() {
        for raw in [
            "abc",
            "-1",
            "3.5",
            "3MiB",
            "1_000",
            "0x10",
            "99999999999999999999999", // overflows u64
            "5 bytes",
            "",
            " 3 MiB",
        ] {
            assert_eq!(
                parse_snapshot_budget(Some(raw)),
                Some(DEFAULT_MAX_SNAPSHOT_BYTES_PER_SECRET),
                "raw={raw:?} must degrade to the default budget"
            );
        }
    }

    /// The operator override must flow through to the policy. Pure: takes
    /// the raw env string rather than touching process-global `environ`, so
    /// nothing here races the `getenv` in `RetentionPolicy::from_env`.
    #[test]
    fn with_budget_from_applies_the_override() {
        // Unset behaves exactly like `default()`.
        assert_eq!(
            RetentionPolicy::with_budget_from(None).max_total_bytes,
            Some(DEFAULT_MAX_SNAPSHOT_BYTES_PER_SECRET)
        );
        // An explicit byte count is honoured...
        assert_eq!(
            RetentionPolicy::with_budget_from(Some("7340032")).max_total_bytes,
            Some(7 * 1024 * 1024)
        );
        // ...an explicit `0` disables the budget...
        assert_eq!(
            RetentionPolicy::with_budget_from(Some("0")).max_total_bytes,
            None
        );
        // ...and garbage degrades to the bounded default.
        assert_eq!(
            RetentionPolicy::with_budget_from(Some("three megabytes")).max_total_bytes,
            Some(DEFAULT_MAX_SNAPSHOT_BYTES_PER_SECRET)
        );
    }

    /// The override must change only the budget — a policy built from an
    /// env value keeps every count tier and the age cap.
    #[test]
    fn with_budget_from_touches_only_the_budget() {
        let d = RetentionPolicy::default();
        let overridden = RetentionPolicy::with_budget_from(Some("4096"));
        assert_eq!(overridden.max_total_bytes, Some(4096));
        assert_eq!(overridden.keep_last, d.keep_last);
        assert_eq!(overridden.max_age, d.max_age);
        assert_eq!(overridden.buckets, d.buckets);
    }

    /// `default()` is PURE — no environment read. If it ever starts
    /// consulting `environ` again, every `SecretsStore` construction becomes
    /// a `getenv` racing any `setenv` in the process.
    #[test]
    fn default_policy_is_pure() {
        assert_eq!(
            RetentionPolicy::default().max_total_bytes,
            Some(DEFAULT_MAX_SNAPSHOT_BYTES_PER_SECRET),
            "default() must be the built-in constant, independent of the environment"
        );
    }

    /// The environment read must live in exactly one place: `from_env` does
    /// it, `Default` does not.
    ///
    /// SOURCE pin, because neither half is behaviourally observable without
    /// a `setenv` — and removing `setenv` from this binary is the whole
    /// point (a `getenv`/`setenv` race is UB, and `Default` is reached from
    /// every `SecretsStore` construction). Needles are assembled with
    /// `concat!` so this test's own source cannot satisfy them, and each
    /// block is sliced out before searching so the negative assertion cannot
    /// be tripped by unrelated code.
    #[test]
    fn env_read_lives_only_in_from_env() {
        let src = include_str!("secret_snapshots.rs");
        let slice_from = |key: &str, end: &str| -> String {
            let start = src
                .find(key)
                .unwrap_or_else(|| panic!("source must still contain {key:?}"));
            let rest = &src[start..];
            let len = rest
                .find(end)
                .unwrap_or_else(|| panic!("no terminator {end:?} after {key:?}"));
            rest[..len].to_string()
        };

        // `from_env` MUST read the environment — it is the only thing it does.
        let from_env_body = slice_from(concat!("pub fn from_env()", " -> Self {"), "\n    }\n");
        assert!(
            from_env_body.contains(concat!("std::env::", "var(SNAPSHOT_BUDGET_ENV)")),
            "from_env must read SNAPSHOT_BUDGET_ENV, otherwise the operator \
             override is dead code; body was:\n{from_env_body}"
        );

        // `Default` MUST NOT: it is reached from every SecretsStore
        // construction, so a getenv there races any setenv in the process.
        let default_impl = slice_from(concat!("impl Default for ", "RetentionPolicy"), "\n}\n");
        assert!(
            !default_impl.contains(concat!("env::", "var")),
            "Default for RetentionPolicy must stay pure — no environment read. \
             Put the override in from_env instead; impl was:\n{default_impl}"
        );
    }

    // ===== recovery-path exemption =====

    #[test]
    fn without_byte_budget_drops_only_the_budget() {
        let p = RetentionPolicy::default();
        let stripped = p.without_byte_budget();
        assert_eq!(stripped.max_total_bytes, None);
        assert_eq!(stripped.keep_last, p.keep_last);
        assert_eq!(stripped.max_age, p.max_age);
        assert_eq!(stripped.buckets, p.buckets);
        // And it is what makes the selection budget-free.
        let now = SystemTime::now();
        let entries: Vec<(SystemTime, u64)> = (0..5)
            .map(|i| (t(now, 500 - i * 100), 10 * 1024 * 1024u64))
            .collect();
        assert_eq!(
            stripped.select_keep_within_budget(now, &entries).len(),
            5,
            "50 MiB of history must survive when the budget is stripped"
        );
        assert!(
            p.select_keep_within_budget(now, &entries).len() < 5,
            "test is vacuous unless the un-stripped policy would have evicted"
        );
    }

    /// The DEFAULT policy must bound bytes, not just version count. This is
    /// the behavioral pin on `DEFAULT_MAX_SNAPSHOT_BYTES_PER_SECRET` being
    /// wired into `RetentionPolicy::default()`.
    #[test]
    fn default_policy_bounds_bytes_per_secret() {
        let p = RetentionPolicy::default();
        // `default()` is pure (no environment read), so this is deterministic.
        let budget = p
            .max_total_bytes
            .expect("default policy must carry a byte budget");
        let now = SystemTime::now();
        // 40 versions of a ~1 MiB secret written one minute apart: the count
        // tiers alone would keep dozens (~40 MiB).
        let size = 1024 * 1024u64;
        let entries: Vec<(SystemTime, u64)> =
            (0..40).map(|i| (t(now, (39 - i) * 60), size)).collect();
        let keep = p.select_keep_within_budget(now, &entries);
        let total: u64 = keep.iter().map(|&i| entries[i].1).sum();
        assert!(
            total <= budget,
            "default policy must bound per-secret snapshot bytes; kept {} entries = {total} bytes",
            keep.len()
        );
        assert!(
            !keep.is_empty(),
            "default policy must still retain recoverable history"
        );
        // Sanity: the count tiers on their own really would have kept far more.
        let timestamps: Vec<SystemTime> = entries.iter().map(|(ts, _)| *ts).collect();
        assert!(
            p.select_keep(now, &timestamps).len() > keep.len(),
            "test is vacuous unless the byte budget is what trimmed the set"
        );
    }

    /// Policy that keeps everything by count, so filesystem-level tests
    /// isolate the byte budget from the tiering.
    fn fs_budget_only(max_total_bytes: Option<u64>) -> RetentionPolicy {
        RetentionPolicy {
            keep_last: usize::MAX,
            buckets: vec![],
            max_age: None,
            max_total_bytes,
        }
    }

    #[test]
    fn thin_snapshots_enforces_byte_budget() {
        let dir = tempfile::tempdir().expect("tempdir");
        let now_ms = recent_ms();
        // Eight 1000-byte snapshots, one second apart. Budget 4500 keeps the
        // newest four — comfortably above the floor, so this pins the BUDGET.
        for i in 0..8u64 {
            write_snapshot(dir.path(), now_ms - (8 - i) * 1000, &vec![b'x'; 1000]);
        }
        thin_snapshots(dir.path(), &fs_budget_only(Some(4500)), SystemTime::now());

        let remaining = list_snapshots(dir.path()).expect("list");
        assert_eq!(
            remaining.len(),
            4,
            "4500 bytes fits exactly four 1000B files"
        );
        assert_eq!(
            remaining.iter().map(|m| m.timestamp_ms).collect::<Vec<_>>(),
            vec![now_ms - 4000, now_ms - 3000, now_ms - 2000, now_ms - 1000],
            "the NEWEST four survive"
        );
    }

    #[test]
    fn thin_snapshots_byte_budget_stops_at_the_floor() {
        let dir = tempfile::tempdir().expect("tempdir");
        let now_ms = recent_ms();
        // Five 5000-byte snapshots against a 100-byte budget: the floor, not
        // the budget, decides how many survive.
        for i in 0..5u64 {
            write_snapshot(
                dir.path(),
                now_ms - (5 - i) * 1000,
                &vec![b'a' + i as u8; 5000],
            );
        }
        thin_snapshots(dir.path(), &fs_budget_only(Some(100)), SystemTime::now());

        let remaining = list_snapshots(dir.path()).expect("list");
        assert_eq!(
            remaining.len(),
            MIN_SNAPSHOTS_KEPT_UNDER_BUDGET,
            "the floor keeps its minimum even when that busts the budget"
        );
        let newest = remaining.last().expect("floor keeps at least one");
        assert_eq!(newest.timestamp_ms, now_ms - 1000, "newest survives");
        assert_eq!(fs::read(&newest.path).unwrap(), vec![b'a' + 4; 5000]);
    }

    /// A single snapshot larger than the whole budget is still retained: the
    /// budget must never leave a secret with no recoverable history.
    #[test]
    fn thin_snapshots_byte_budget_keeps_a_lone_oversized_snapshot() {
        let dir = tempfile::tempdir().expect("tempdir");
        let now_ms = recent_ms();
        write_snapshot(dir.path(), now_ms - 1000, &vec![b'b'; 50_000]);
        thin_snapshots(dir.path(), &fs_budget_only(Some(100)), SystemTime::now());

        let remaining = list_snapshots(dir.path()).expect("list");
        assert_eq!(remaining.len(), 1, "history must never be emptied");
        assert_eq!(fs::read(&remaining[0].path).unwrap(), vec![b'b'; 50_000]);
    }

    /// Same-millisecond collision entries must be ordered NUMERICALLY by
    /// suffix, not lexicographically by file name: text order puts `.10`
    /// before `.9`, which would let the budget evict the numerically-newest
    /// entry of a burst and keep an older sibling.
    #[test]
    fn thin_snapshots_orders_collision_suffixes_numerically() {
        let dir = tempfile::tempdir().expect("tempdir");
        let stamp = recent_ms();
        let base = format!("{stamp:0width$}", width = SNAPSHOT_NAME_WIDTH);
        // Unsuffixed plus `.0` ..= `.10` — twelve entries in one millisecond.
        fs::write(dir.path().join(&base), b"unsuffixed").unwrap();
        for n in 0..=10u32 {
            fs::write(dir.path().join(format!("{base}.{n}")), format!("body-{n}")).unwrap();
        }

        // Budget 0 => only the floor survives, and it must be the three
        // NUMERICALLY newest: `.8`, `.9`, `.10`.
        thin_snapshots(dir.path(), &fs_budget_only(Some(0)), SystemTime::now());

        let remaining = list_snapshots(dir.path()).expect("list");
        assert_eq!(
            remaining.iter().map(|m| m.suffix).collect::<Vec<_>>(),
            vec![Some(8), Some(9), Some(10)],
            "lexicographic ordering would have kept .1/.10 and dropped .9"
        );
    }

    /// The stamp half of [`parse_snapshot_name`], which is all the
    /// timestamp-ordering callers need.
    fn parse_snapshot_stamp(path: &Path) -> Option<u64> {
        parse_snapshot_name(path).map(|(ts, _)| ts)
    }

    #[test]
    fn parse_snapshot_stamp_accepts_valid_shapes() {
        use std::path::PathBuf;
        let pure = PathBuf::from("/tmp/snap/00000000000001234567");
        assert_eq!(parse_snapshot_stamp(&pure), Some(1_234_567));
        let suffixed = PathBuf::from("/tmp/snap/00000000000001234567.42");
        assert_eq!(parse_snapshot_stamp(&suffixed), Some(1_234_567));
    }

    #[test]
    fn parse_snapshot_stamp_rejects_garbage() {
        use std::path::PathBuf;
        // Non-digit body
        assert_eq!(parse_snapshot_stamp(&PathBuf::from("foo")), None);
        // Non-digit suffix (the previous lax implementation accepted these)
        assert_eq!(parse_snapshot_stamp(&PathBuf::from("123.tmp")), None);
        // Multiple dots
        assert_eq!(parse_snapshot_stamp(&PathBuf::from("123.4.5")), None);
        // Empty stamp
        assert_eq!(parse_snapshot_stamp(&PathBuf::from(".42")), None);
        // Empty suffix
        assert_eq!(parse_snapshot_stamp(&PathBuf::from("123.")), None);
        // Pure punctuation
        assert_eq!(parse_snapshot_stamp(&PathBuf::from("...")), None);
    }

    #[test]
    fn next_snapshot_path_uses_unsuffixed_when_free() {
        let dir = tempfile::tempdir().expect("tempdir");
        let p = next_snapshot_path(dir.path()).expect("path");
        // Default name is just digits, no dot.
        assert!(
            p.file_name()
                .unwrap()
                .to_str()
                .unwrap()
                .chars()
                .all(|c| c.is_ascii_digit())
        );
    }

    #[test]
    fn next_snapshot_path_falls_back_to_suffix_on_collision() {
        let dir = tempfile::tempdir().expect("tempdir");
        // Pre-create files at every plausible "now" stamp (we can't easily
        // pin the wall clock, so saturate the directory at and around the
        // current millisecond and assert the helper picks SOME free path).
        // Walk a small window of stamps and create the unsuffixed file at
        // each: this guarantees `next_snapshot_path` will see a collision
        // for at least one call.
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64;
        for offset in 0..3u64 {
            let p = dir
                .path()
                .join(format!("{:0width$}", now + offset, width = 20));
            std::fs::write(&p, b"").unwrap();
        }
        // Ask for a path; the first call may land on a free stamp (now+3+),
        // but if the clock hasn't ticked we'll hit collision and walk to a
        // suffixed name. Either way the result must be a free path that
        // the helper hasn't itself created (existence check).
        let p = next_snapshot_path(dir.path()).expect("path");
        assert!(!p.exists());
        let name = p.file_name().unwrap().to_str().unwrap();
        // It's either `{stamp}` or `{stamp}.{n}` with all-digit components.
        assert!(parse_snapshot_stamp(&p).is_some(), "name={name}");
    }

    #[test]
    fn zero_max_count_bucket_is_inert() {
        let p = RetentionPolicy {
            keep_last: 0,
            buckets: vec![RetentionBucket {
                interval: Duration::from_secs(60),
                max_count: 0,
            }],
            max_age: None,
            max_total_bytes: None,
        };
        let now = SystemTime::now();
        let ts: Vec<_> = (0..10).map(|i| t(now, i)).collect();
        assert!(p.select_keep(now, &ts).is_empty());
    }

    #[test]
    fn parse_snapshot_name_returns_suffix() {
        use std::path::PathBuf;
        assert_eq!(
            parse_snapshot_name(&PathBuf::from("00000000000001234567")),
            Some((1_234_567, None))
        );
        assert_eq!(
            parse_snapshot_name(&PathBuf::from("00000000000001234567.42")),
            Some((1_234_567, Some(42)))
        );
        // Same garbage cases as parse_snapshot_stamp must still reject.
        assert_eq!(parse_snapshot_name(&PathBuf::from("foo")), None);
        assert_eq!(parse_snapshot_name(&PathBuf::from("123.tmp")), None);
        assert_eq!(parse_snapshot_name(&PathBuf::from("123.4.5")), None);
        // Suffix that overflows u32 must be rejected (the on-disk format
        // caps at MAX_SNAPSHOT_COLLISION_SUFFIX, so a 12-digit suffix is
        // never a snapshot we wrote).
        assert_eq!(
            parse_snapshot_name(&PathBuf::from("123.999999999999")),
            None
        );
    }

    #[test]
    fn list_snapshots_returns_sorted_metadata() {
        let dir = tempfile::tempdir().expect("tempdir");
        // Write three snapshots out of order and a couple of stray files
        // that must be filtered out.
        for (stamp, body) in [
            (20u64, &b"newest"[..]),
            (5, &b"older"[..]),
            (10, &b"mid"[..]),
        ] {
            std::fs::write(
                dir.path()
                    .join(format!("{stamp:0width$}", width = SNAPSHOT_NAME_WIDTH)),
                body,
            )
            .unwrap();
        }
        std::fs::write(dir.path().join("README"), b"not a snapshot").unwrap();
        std::fs::write(dir.path().join("123.tmp"), b"not a snapshot").unwrap();

        let entries = list_snapshots(dir.path()).expect("list");
        let stamps: Vec<u64> = entries.iter().map(|m| m.timestamp_ms).collect();
        assert_eq!(stamps, vec![5, 10, 20], "must be sorted oldest-first");
        assert_eq!(entries[0].size_bytes, 5, "size_bytes wired up");
        assert!(entries.iter().all(|m| m.suffix.is_none()));
    }

    #[test]
    fn list_snapshots_orders_collision_suffixes() {
        let dir = tempfile::tempdir().expect("tempdir");
        let stamp = 42u64;
        let base = format!("{stamp:0width$}", width = SNAPSHOT_NAME_WIDTH);
        // Unsuffixed and two collision suffixes — must come out in
        // (timestamp, suffix) order with `None` first.
        std::fs::write(dir.path().join(&base), b"a").unwrap();
        std::fs::write(dir.path().join(format!("{base}.1")), b"bb").unwrap();
        std::fs::write(dir.path().join(format!("{base}.0")), b"ccc").unwrap();

        let entries = list_snapshots(dir.path()).expect("list");
        let suffixes: Vec<Option<u32>> = entries.iter().map(|m| m.suffix).collect();
        assert_eq!(suffixes, vec![None, Some(0), Some(1)]);
    }

    #[test]
    fn list_snapshots_missing_dir_is_empty() {
        let dir = tempfile::tempdir().expect("tempdir");
        let missing = dir.path().join("never-existed");
        let entries = list_snapshots(&missing).expect("missing dir is not an error");
        assert!(entries.is_empty());
    }

    fn write_snapshot(snap_dir: &Path, stamp: u64, body: &[u8]) {
        fs::create_dir_all(snap_dir).unwrap();
        fs::write(
            snap_dir.join(format!("{stamp:0width$}", width = SNAPSHOT_NAME_WIDTH)),
            body,
        )
        .unwrap();
    }

    /// A snapshot timestamp ~1 minute in the past: recent enough to
    /// survive the default retention policy's 2-year `max_age` cap, so
    /// tests that assert on post-restore snapshot counts aren't thrown
    /// off by legitimate thinning of an ancient (epoch-zero) timestamp.
    fn recent_ms() -> u64 {
        (SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64)
            - 60_000
    }

    #[test]
    fn restore_snapshot_file_replaces_active_with_snapshot() {
        let dir = tempfile::tempdir().expect("tempdir");
        let delegate_dir = dir.path().join("delegateA");
        let secret = "secretX";
        write_snapshot(
            &snapshot_dir_for_encoded(&delegate_dir, secret),
            1000,
            b"old-value",
        );
        fs::create_dir_all(&delegate_dir).unwrap();
        fs::write(delegate_dir.join(secret), b"current-value").unwrap();

        restore_snapshot_file(&delegate_dir, secret, 1000, None, true)
            .expect("restore must succeed");

        assert_eq!(fs::read(delegate_dir.join(secret)).unwrap(), b"old-value");
    }

    #[test]
    fn restore_snapshot_file_is_reversible() {
        // Restoring must first snapshot the current active value, so the
        // operation can itself be undone. After the restore the history
        // contains both the original snapshot and the captured prior
        // active value.
        let dir = tempfile::tempdir().expect("tempdir");
        let delegate_dir = dir.path().join("delegateA");
        let secret = "secretX";
        let snap_dir = snapshot_dir_for_encoded(&delegate_dir, secret);
        let stamp = recent_ms();
        write_snapshot(&snap_dir, stamp, b"old-value");
        fs::create_dir_all(&delegate_dir).unwrap();
        fs::write(delegate_dir.join(secret), b"current-value").unwrap();

        restore_snapshot_file(&delegate_dir, secret, stamp, None, true)
            .expect("restore must succeed");

        let snaps = list_snapshots(&snap_dir).expect("list");
        assert!(
            snaps.len() >= 2,
            "reversibility snapshot missing; got {}",
            snaps.len()
        );
        let bodies: Vec<Vec<u8>> = snaps.iter().map(|m| fs::read(&m.path).unwrap()).collect();
        assert!(
            bodies.iter().any(|b| b.as_slice() == b"current-value"),
            "prior active value was not snapshotted"
        );
    }

    #[test]
    fn restore_snapshot_file_unknown_timestamp_is_not_found() {
        let dir = tempfile::tempdir().expect("tempdir");
        let delegate_dir = dir.path().join("delegateA");
        let secret = "secretX";
        write_snapshot(
            &snapshot_dir_for_encoded(&delegate_dir, secret),
            1000,
            b"old",
        );
        fs::create_dir_all(&delegate_dir).unwrap();
        fs::write(delegate_dir.join(secret), b"current").unwrap();

        let err = restore_snapshot_file(&delegate_dir, secret, 999, None, true)
            .expect_err("unknown timestamp must error");
        assert!(matches!(err, RestoreError::NotFound(999)));
        // Active value untouched on the error path.
        assert_eq!(fs::read(delegate_dir.join(secret)).unwrap(), b"current");
    }

    #[test]
    fn restore_snapshot_file_missing_snapshot_dir_is_not_found() {
        let dir = tempfile::tempdir().expect("tempdir");
        let delegate_dir = dir.path().join("delegateA");
        // Secret never had a snapshot directory at all.
        let err = restore_snapshot_file(&delegate_dir, "neversnapshotted", 1, None, true)
            .expect_err("missing history must error");
        assert!(matches!(err, RestoreError::NotFound(1)));
    }

    #[test]
    fn restore_snapshot_file_prefers_unsuffixed_collision() {
        let dir = tempfile::tempdir().expect("tempdir");
        let delegate_dir = dir.path().join("delegateA");
        let secret = "secretX";
        let snap_dir = snapshot_dir_for_encoded(&delegate_dir, secret);
        fs::create_dir_all(&snap_dir).unwrap();
        let base = format!(
            "{stamp:0width$}",
            stamp = 50u64,
            width = SNAPSHOT_NAME_WIDTH
        );
        fs::write(snap_dir.join(&base), b"unsuffixed").unwrap();
        fs::write(snap_dir.join(format!("{base}.0")), b"suffix-zero").unwrap();
        fs::create_dir_all(&delegate_dir).unwrap();
        fs::write(delegate_dir.join(secret), b"current").unwrap();

        // snapshots_enabled=false isolates the disambiguation from the
        // reversibility snapshot.
        restore_snapshot_file(&delegate_dir, secret, 50, None, false)
            .expect("restore must succeed");
        assert_eq!(fs::read(delegate_dir.join(secret)).unwrap(), b"unsuffixed");
    }

    #[test]
    fn restore_snapshot_file_targets_explicit_suffix() {
        // The listing exposes collision suffixes; restore must be able to
        // target a specific one rather than silently picking unsuffixed.
        let dir = tempfile::tempdir().expect("tempdir");
        let delegate_dir = dir.path().join("delegateA");
        let secret = "secretX";
        let snap_dir = snapshot_dir_for_encoded(&delegate_dir, secret);
        fs::create_dir_all(&snap_dir).unwrap();
        let base = format!(
            "{stamp:0width$}",
            stamp = 50u64,
            width = SNAPSHOT_NAME_WIDTH
        );
        fs::write(snap_dir.join(&base), b"unsuffixed").unwrap();
        fs::write(snap_dir.join(format!("{base}.0")), b"suffix-zero").unwrap();
        fs::write(snap_dir.join(format!("{base}.1")), b"suffix-one").unwrap();
        fs::create_dir_all(&delegate_dir).unwrap();
        fs::write(delegate_dir.join(secret), b"current").unwrap();

        // Explicit suffix targets the exact `.n` entry.
        restore_snapshot_file(&delegate_dir, secret, 50, Some(1), false)
            .expect("restore must succeed");
        assert_eq!(fs::read(delegate_dir.join(secret)).unwrap(), b"suffix-one");

        // A missing suffix is NotFound, never a silent fallback to the
        // unsuffixed entry (which would restore the wrong ciphertext).
        let err = restore_snapshot_file(&delegate_dir, secret, 50, Some(9), false)
            .expect_err("missing suffix must error");
        assert!(matches!(err, RestoreError::NotFound(50)));
        // Active unchanged by the failed lookup.
        assert_eq!(fs::read(delegate_dir.join(secret)).unwrap(), b"suffix-one");
    }

    #[test]
    fn snapshot_active_value_missing_active_is_noop() {
        let dir = tempfile::tempdir().expect("tempdir");
        let delegate_dir = dir.path().join("delegateA");
        let secret = "secretX";
        // Active file does not exist: snapshotting must not create a
        // bogus entry.
        snapshot_active_value(&delegate_dir, secret, &delegate_dir.join(secret))
            .expect("missing active is not an error");
        let snaps = list_snapshots(&snapshot_dir_for_encoded(&delegate_dir, secret)).expect("list");
        assert!(
            snaps.is_empty(),
            "missing active must not produce a snapshot"
        );
    }

    #[cfg(unix)]
    #[test]
    fn restore_snapshot_file_writes_owner_only() {
        // The restore write path lands the active secret at 0o600 and the
        // snapshot dirs at 0o700 — the crypto-at-rest regression class
        // #4146 guarded for `store_secret`, here for the restore write.
        use std::os::unix::fs::PermissionsExt;
        let dir = tempfile::tempdir().expect("tempdir");
        let delegate_dir = dir.path().join("delegateA");
        let secret = "secretX";
        let snap_dir = snapshot_dir_for_encoded(&delegate_dir, secret);
        let stamp = recent_ms();
        write_snapshot(&snap_dir, stamp, b"old-value");
        fs::create_dir_all(&delegate_dir).unwrap();
        // Seed the active file owner-only, as `store_secret` always does
        // in production, so the hard-linked reversibility snapshot is 0o600.
        fs::write(delegate_dir.join(secret), b"current-value").unwrap();
        fs::set_permissions(delegate_dir.join(secret), fs::Permissions::from_mode(0o600)).unwrap();

        restore_snapshot_file(&delegate_dir, secret, stamp, None, true)
            .expect("restore must succeed");

        let mode = |p: &Path| fs::metadata(p).unwrap().permissions().mode() & 0o777;
        assert_eq!(
            mode(&delegate_dir.join(secret)),
            0o600,
            "restored active secret must be owner-only"
        );
        assert_eq!(
            mode(&delegate_dir.join(SNAPSHOTS_DIR)),
            0o700,
            ".snapshots umbrella must be owner-only"
        );
        assert_eq!(
            mode(&snap_dir),
            0o700,
            "per-secret snapshot dir must be owner-only"
        );
    }

    #[test]
    fn restore_snapshot_file_disabled_skips_reversibility_snapshot() {
        // `snapshots_enabled=false` replaces the active value but skips the
        // reversibility snapshot. (Thinning is no longer the core's job —
        // it lives in the callers — so the only history change a restore
        // makes here is the reversibility snapshot.)
        let dir = tempfile::tempdir().expect("tempdir");
        let delegate_dir = dir.path().join("delegateA");
        let secret = "secretX";
        let snap_dir = snapshot_dir_for_encoded(&delegate_dir, secret);
        write_snapshot(&snap_dir, 1000, b"v1000");
        fs::create_dir_all(&delegate_dir).unwrap();
        fs::write(delegate_dir.join(secret), b"current").unwrap();

        let before = list_snapshots(&snap_dir).unwrap().len();
        restore_snapshot_file(&delegate_dir, secret, 1000, None, false)
            .expect("restore must succeed");
        assert_eq!(fs::read(delegate_dir.join(secret)).unwrap(), b"v1000");
        assert_eq!(
            list_snapshots(&snap_dir).unwrap().len(),
            before,
            "disabled restore must not add a reversibility snapshot"
        );

        // Contrast: with `snapshots_enabled=true` the prior active value IS
        // snapshotted, so the history grows by exactly one (no thinning here).
        fs::write(delegate_dir.join(secret), b"current2").unwrap();
        restore_snapshot_file(&delegate_dir, secret, 1000, None, true)
            .expect("restore must succeed");
        assert_eq!(list_snapshots(&snap_dir).unwrap().len(), before + 1);
    }
}
