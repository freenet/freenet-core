//! Tiered retention thinning for delegate secret snapshots.
//!
//! Each `(delegate, secret_id)` pair has its own snapshot history. Before a
//! `store_secret` overwrites an existing value, the previous ciphertext is
//! moved into a `.snapshots/{secret_id}/{epoch_ms}` file. Retention is then
//! thinned per the policy below: recent history stays dense, older history
//! is sampled at progressively coarser intervals (minute / hour / day /
//! week / month), bounded above by ~60 snapshots per secret in steady state.
//!
//! Snapshots are encrypted with whatever cipher the delegate had configured
//! when the snapshot was taken; without that cipher the bytes are useless.

use std::collections::{BTreeSet, HashSet};
use std::fs;
use std::path::{Path, PathBuf};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use freenet_stdlib::prelude::SecretsId;

/// Subdirectory (relative to a delegate's secrets dir) holding the
/// per-secret snapshot history. Leading dot keeps it visually separate from
/// active secret files; the `get_secret` read path never descends into it.
pub const SNAPSHOTS_DIR: &str = ".snapshots";

/// Path to the snapshot directory for a particular `(delegate, secret_id)`
/// pair. Snapshots are organized per secret so retention thinning of one
/// busy key cannot affect another.
pub fn snapshot_dir_for(delegate_path: &Path, key: &SecretsId) -> PathBuf {
    delegate_path.join(SNAPSHOTS_DIR).join(key.encode())
}

/// Apply the retention policy to a snapshot directory: delete any file
/// whose timestamp is not selected by `policy` relative to `now`. Files
/// whose names don't parse as `{epoch_ms}` (with optional `.{counter}`
/// suffix) are left untouched.
///
/// Best-effort: I/O errors during enumeration or unlink are logged but
/// do not propagate, since thinning is a maintenance operation that must
/// never fail the primary write path.
pub fn thin_snapshots(snap_dir: &Path, policy: &RetentionPolicy, now: SystemTime) {
    let mut entries: Vec<(SystemTime, PathBuf)> = match fs::read_dir(snap_dir) {
        Ok(rd) => rd
            .flatten()
            .filter_map(|e| {
                let path = e.path();
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
/// Accepts either `{stamp}` or `{stamp}.{counter}` (collision suffix);
/// returns `None` for any other shape.
fn parse_snapshot_stamp(path: &Path) -> Option<u64> {
    let name = path.file_name()?.to_str()?;
    let stamp_part = name.split_once('.').map(|(s, _)| s).unwrap_or(name);
    stamp_part.parse().ok()
}

/// One tier of a [`RetentionPolicy`]: aim to keep one snapshot per
/// `interval` of clock time, up to `max_count` snapshots in this tier.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RetentionBucket {
    pub interval: Duration,
    pub max_count: usize,
}

/// Tiered retention policy. The first `keep_last` snapshots (by recency) are
/// always kept regardless of age; on top of that, each bucket independently
/// selects representatives at its own granularity.
#[derive(Debug, Clone)]
pub struct RetentionPolicy {
    pub keep_last: usize,
    pub buckets: Vec<RetentionBucket>,
}

impl Default for RetentionPolicy {
    /// Default: keep the last 5 snapshots, plus one per minute for the last
    /// 10 minutes, one per hour for the last 24 hours, one per day for the
    /// last week, one per week for the last 4 weeks, one per month for the
    /// last 12 months. Worst-case ~62 entries per secret in steady state.
    fn default() -> Self {
        const MIN: u64 = 60;
        const HOUR: u64 = 60 * MIN;
        const DAY: u64 = 24 * HOUR;
        const WEEK: u64 = 7 * DAY;
        const MONTH: u64 = 30 * DAY;
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

        keep
    }
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
        };
        let now = SystemTime::now();
        let ts = vec![now + Duration::from_secs(120), t(now, 30)];
        let keep = p.select_keep(now, &ts);
        // Both fall in slot 0 → only the newest (index 1, since input is by
        // call-order not timestamp) wins, but the algorithm walks rev so the
        // last index wins the slot. Either way exactly one survives.
        assert_eq!(keep.len(), 1);
    }

    #[test]
    fn zero_max_count_bucket_is_inert() {
        let p = RetentionPolicy {
            keep_last: 0,
            buckets: vec![RetentionBucket {
                interval: Duration::from_secs(60),
                max_count: 0,
            }],
        };
        let now = SystemTime::now();
        let ts: Vec<_> = (0..10).map(|i| t(now, i)).collect();
        assert!(p.select_keep(now, &ts).is_empty());
    }
}
