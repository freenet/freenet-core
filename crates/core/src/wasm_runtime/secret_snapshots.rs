//! Tiered retention thinning for delegate secret snapshots.
//!
//! Each `(delegate, secret_id)` pair has its own snapshot history. Before a
//! `store_secret` overwrites an existing value, the previous ciphertext is
//! moved into a `.snapshots/{secret_id}/{epoch_ms}` file. Retention is then
//! thinned per the policy below: recent history stays dense, older history
//! is sampled at progressively coarser intervals (minute / hour / day /
//! week / month), and an absolute `max_age` cap drops anything older
//! regardless of which clause selected it. Worst case ~62 entries per
//! secret in steady state.
//!
//! Snapshots are encrypted with whatever cipher the delegate had configured
//! when the snapshot was taken; without that cipher the bytes are useless.

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
/// representatives at its own granularity; and `max_age` is an absolute
/// upper bound that drops snapshots older than the threshold even if a
/// recency or bucket clause would otherwise keep them. The absolute cap
/// satisfies the cleanup-exemption rule in AGENTS.md: every retained
/// entry has a finite lifetime.
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
}

impl Default for RetentionPolicy {
    /// Default: keep the last 5 snapshots, plus one per minute for the last
    /// 10 minutes, one per hour for the last 24 hours, one per day for the
    /// last week, one per week for the last 4 weeks, one per month for the
    /// last 12 months, with an absolute 2-year ceiling so stale snapshots
    /// from secrets that stopped being written eventually age out. Worst
    /// case ~62 entries per secret in steady state.
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
        }
    }
}

impl RetentionPolicy {
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

/// Apply the retention policy to a snapshot directory: delete any file
/// whose timestamp is not selected by `policy` relative to `now`. Files
/// whose names don't parse as `{epoch_ms}` (with optional `.{counter}`
/// suffix) and non-regular-file entries are left untouched.
///
/// Best-effort: I/O errors during enumeration or unlink are logged but
/// do not propagate, since thinning is a maintenance operation that must
/// never fail the primary write path.
pub fn thin_snapshots(snap_dir: &Path, policy: &RetentionPolicy, now: SystemTime) {
    let mut entries: Vec<(SystemTime, PathBuf)> = match fs::read_dir(snap_dir) {
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
                let stamp = parse_snapshot_stamp(&path)?;
                Some((UNIX_EPOCH + Duration::from_millis(stamp), path))
            })
            .collect(),
        Err(err) => {
            tracing::warn!("failed to read snapshot dir {snap_dir:?}: {err}");
            return;
        }
    };
    entries.sort_by_key(|(ts, _)| *ts);

    let timestamps: Vec<SystemTime> = entries.iter().map(|(t, _)| *t).collect();
    let keep = policy.select_keep(now, &timestamps);
    for (i, (_, path)) in entries.iter().enumerate() {
        if !keep.contains(&i) {
            if let Err(err) = fs::remove_file(path) {
                tracing::warn!("failed to thin snapshot {path:?}: {err}");
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

/// Parse a snapshot file name back into its epoch-millis timestamp.
/// Accepts only `{digits}` or `{digits}.{digits}`; any other shape
/// (including `42.tmp`, `foo`, `1.2.3`, etc.) returns `None` so stray
/// files in the snapshot directory are not mistaken for snapshots.
pub(crate) fn parse_snapshot_stamp(path: &Path) -> Option<u64> {
    parse_snapshot_name(path).map(|(ts, _)| ts)
}

/// Like [`parse_snapshot_stamp`] but also returns the optional numeric
/// collision suffix. Surfaced through [`SnapshotMetadata`] so callers
/// can disambiguate multiple writes that landed in the same millisecond.
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
///    atomically rename onto the active path,
/// 3. thin the snapshot history per `retention`.
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
    retention: &RetentionPolicy,
    now: SystemTime,
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

    // Best-effort thin: the reversibility snapshot above may have pushed
    // the history one over the policy target. Self-correcting on the next
    // write even if this pass fails.
    if snapshots_enabled && snap_dir.exists() {
        thin_snapshots(&snap_dir, retention, now);
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

        restore_snapshot_file(
            &delegate_dir,
            secret,
            1000,
            None,
            true,
            &RetentionPolicy::default(),
            SystemTime::now(),
        )
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

        restore_snapshot_file(
            &delegate_dir,
            secret,
            stamp,
            None,
            true,
            &RetentionPolicy::default(),
            SystemTime::now(),
        )
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

        let err = restore_snapshot_file(
            &delegate_dir,
            secret,
            999,
            None,
            true,
            &RetentionPolicy::default(),
            SystemTime::now(),
        )
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
        let err = restore_snapshot_file(
            &delegate_dir,
            "neversnapshotted",
            1,
            None,
            true,
            &RetentionPolicy::default(),
            SystemTime::now(),
        )
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
        restore_snapshot_file(
            &delegate_dir,
            secret,
            50,
            None,
            false,
            &RetentionPolicy::default(),
            SystemTime::now(),
        )
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
        restore_snapshot_file(
            &delegate_dir,
            secret,
            50,
            Some(1),
            false,
            &RetentionPolicy::default(),
            SystemTime::now(),
        )
        .expect("restore must succeed");
        assert_eq!(fs::read(delegate_dir.join(secret)).unwrap(), b"suffix-one");

        // A missing suffix is NotFound, never a silent fallback to the
        // unsuffixed entry (which would restore the wrong ciphertext).
        let err = restore_snapshot_file(
            &delegate_dir,
            secret,
            50,
            Some(9),
            false,
            &RetentionPolicy::default(),
            SystemTime::now(),
        )
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

        restore_snapshot_file(
            &delegate_dir,
            secret,
            stamp,
            None,
            true,
            &RetentionPolicy::default(),
            SystemTime::now(),
        )
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
    fn restore_snapshot_file_disabled_skips_reversibility_and_thin() {
        // With `snapshots_enabled=false` the restore must still replace the
        // active value, but skip BOTH the reversibility snapshot and the
        // thin pass. Seed two ancient snapshots the default policy's 2-year
        // max_age WOULD prune if thinning ran — their survival proves it
        // didn't.
        let dir = tempfile::tempdir().expect("tempdir");
        let delegate_dir = dir.path().join("delegateA");
        let secret = "secretX";
        let snap_dir = snapshot_dir_for_encoded(&delegate_dir, secret);
        write_snapshot(&snap_dir, 1000, b"v1000");
        write_snapshot(&snap_dir, 2000, b"v2000");
        fs::create_dir_all(&delegate_dir).unwrap();
        fs::write(delegate_dir.join(secret), b"current").unwrap();

        let before = list_snapshots(&snap_dir).unwrap().len();
        restore_snapshot_file(
            &delegate_dir,
            secret,
            1000,
            None,
            false,
            &RetentionPolicy::default(),
            SystemTime::now(),
        )
        .expect("restore must succeed");

        assert_eq!(fs::read(delegate_dir.join(secret)).unwrap(), b"v1000");
        let after = list_snapshots(&snap_dir).unwrap();
        assert_eq!(
            after.len(),
            before,
            "disabled restore must neither add a reversibility snapshot nor thin"
        );
        assert_eq!(after.len(), 2);
    }
}
