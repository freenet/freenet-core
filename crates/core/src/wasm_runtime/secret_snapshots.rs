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
use std::path::{Path, PathBuf};
// Wall-clock SystemTime (not the project-wide TimeSource trait) is the
// correct abstraction here: snapshot file names embed epoch_ms so retention
// stays sortable across process restarts and across nodes that share a
// data directory. TimeSource returns simulation-relative Duration with no
// stable origin, which can't be persisted into a filename.
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use freenet_stdlib::prelude::SecretsId;

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

/// Path to the snapshot directory for a particular `(delegate, secret_id)`
/// pair. Snapshots are organized per secret so retention thinning of one
/// busy key cannot affect another.
pub fn snapshot_dir_for(delegate_path: &Path, key: &SecretsId) -> PathBuf {
    delegate_path.join(SNAPSHOTS_DIR).join(key.encode())
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

/// Parse a snapshot file name back into its epoch-millis timestamp.
/// Accepts only `{digits}` or `{digits}.{digits}`; any other shape
/// (including `42.tmp`, `foo`, `1.2.3`, etc.) returns `None` so stray
/// files in the snapshot directory are not mistaken for snapshots.
pub(crate) fn parse_snapshot_stamp(path: &Path) -> Option<u64> {
    let name = path.file_name()?.to_str()?;
    let (stamp_part, suffix_part) = match name.split_once('.') {
        Some((s, t)) => (s, Some(t)),
        None => (name, None),
    };
    if stamp_part.is_empty() || !stamp_part.bytes().all(|b| b.is_ascii_digit()) {
        return None;
    }
    if let Some(suffix) = suffix_part
        && (suffix.is_empty() || !suffix.bytes().all(|b| b.is_ascii_digit()))
    {
        return None;
    }
    stamp_part.parse().ok()
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
}
