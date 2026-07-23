//! Persistent record of previous runs' contracts, used to pick the
//! retention re-GET targets (24h / 48h / 7d windows).

use std::path::Path;

use anyhow::{Context, Result};
use freenet_stdlib::prelude::ContractInstanceId;
use serde::{Deserialize, Serialize};

/// Runs older than this are pruned; keeps the manifest a few entries long.
const KEEP_SECS: u64 = 8 * 24 * 3600;

/// Retention windows: newest run whose age falls inside the range is re-GET.
/// Ranges are generous (±ish 25%) so a late or skipped nightly run does not
/// silently drop the window.
const WINDOWS: &[(&str, u64, u64)] = &[
    ("24h", 20 * 3600, 30 * 3600),
    ("48h", 44 * 3600, 54 * 3600),
    ("7d", 156 * 3600, 180 * 3600),
];

#[derive(Serialize, Deserialize, Default)]
pub struct Manifest {
    pub runs: Vec<RunRecord>,
}

#[derive(Serialize, Deserialize)]
pub struct RunRecord {
    pub run_id: String,
    /// Unix epoch seconds of the run's PUT phase.
    pub timestamp: u64,
    pub contracts: Vec<ContractRecord>,
}

#[derive(Serialize, Deserialize)]
pub struct ContractRecord {
    pub id: ContractInstanceId,
    pub label: String,
    pub size: usize,
    /// blake3 of the PUT state; retention GETs verify against this.
    pub state_hash: String,
}

impl Manifest {
    pub fn load(path: &Path) -> Result<Self> {
        match std::fs::read(path) {
            Ok(bytes) => serde_json::from_slice(&bytes)
                .with_context(|| format!("parsing manifest {}", path.display())),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(Self::default()),
            Err(e) => Err(e).with_context(|| format!("reading manifest {}", path.display())),
        }
    }

    pub fn save(&self, path: &Path) -> Result<()> {
        let tmp = path.with_extension("tmp");
        std::fs::write(&tmp, serde_json::to_vec_pretty(self)?)?;
        std::fs::rename(&tmp, path)?;
        Ok(())
    }

    /// Newest run per retention window, oldest window first.
    pub fn retention_targets(&self, now: u64) -> Vec<(&'static str, &RunRecord)> {
        let mut out = Vec::new();
        for &(label, min_age, max_age) in WINDOWS {
            let hit = self
                .runs
                .iter()
                .filter(|r| {
                    let age = now.saturating_sub(r.timestamp);
                    age >= min_age && age <= max_age
                })
                .max_by_key(|r| r.timestamp);
            if let Some(run) = hit {
                out.push((label, run));
            }
        }
        out
    }

    /// Append this run and prune anything past the keep horizon.
    pub fn record_run(&mut self, run: RunRecord, now: u64) {
        self.runs.push(run);
        self.runs
            .retain(|r| now.saturating_sub(r.timestamp) <= KEEP_SECS);
        self.runs.sort_by_key(|r| r.timestamp);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const NOW: u64 = 1_800_000_000;
    const HOUR: u64 = 3600;

    fn run_at(id: &str, age_hours: u64) -> RunRecord {
        RunRecord {
            run_id: id.to_string(),
            timestamp: NOW - age_hours * HOUR,
            contracts: Vec::new(),
        }
    }

    fn windows(m: &Manifest) -> Vec<(&'static str, String)> {
        m.retention_targets(NOW)
            .into_iter()
            .map(|(w, r)| (w, r.run_id.clone()))
            .collect()
    }

    #[test]
    fn empty_manifest_has_no_targets() {
        assert!(windows(&Manifest::default()).is_empty());
    }

    #[test]
    fn each_window_selects_its_own_run() {
        let m = Manifest {
            runs: vec![run_at("d1", 24), run_at("d2", 48), run_at("d7", 168)],
        };
        assert_eq!(
            windows(&m),
            vec![
                ("24h", "d1".to_string()),
                ("48h", "d2".to_string()),
                ("7d", "d7".to_string()),
            ]
        );
    }

    #[test]
    fn window_with_no_run_is_skipped_without_shifting_the_others() {
        // Nothing in the 48h window: the 7d window must still resolve, and
        // the 24h run must not be dragged in to fill the gap.
        let m = Manifest {
            runs: vec![run_at("d1", 24), run_at("d7", 168)],
        };
        assert_eq!(
            windows(&m),
            vec![("24h", "d1".to_string()), ("7d", "d7".to_string())]
        );
    }

    #[test]
    fn window_bounds_are_inclusive_and_exclude_just_outside() {
        // 24h window is [20h, 30h].
        let inside = Manifest {
            runs: vec![run_at("lo", 20), run_at("hi", 30)],
        };
        // Newest wins when several runs land in one window.
        assert_eq!(windows(&inside), vec![("24h", "lo".to_string())]);

        let outside = Manifest {
            runs: vec![run_at("too-new", 19), run_at("too-old", 31)],
        };
        assert!(windows(&outside).is_empty());
    }

    #[test]
    fn newest_run_wins_inside_a_window() {
        let m = Manifest {
            runs: vec![run_at("older", 29), run_at("newer", 21)],
        };
        assert_eq!(windows(&m), vec![("24h", "newer".to_string())]);
    }

    #[test]
    fn record_run_prunes_past_the_keep_horizon_and_keeps_order() {
        let mut m = Manifest {
            runs: vec![run_at("ancient", 9 * 24), run_at("week", 7 * 24)],
        };
        m.record_run(run_at("today", 0), NOW);
        let ids: Vec<&str> = m.runs.iter().map(|r| r.run_id.as_str()).collect();
        assert_eq!(ids, vec!["week", "today"], "9-day-old run must be pruned");
    }

    #[test]
    fn a_run_still_needed_by_the_7d_window_survives_pruning() {
        // The keep horizon must outlive the widest window (180h) or the 7d
        // check would silently stop having anything to ask for.
        let mut m = Manifest::default();
        m.record_run(run_at("edge", 180 / 24), NOW);
        m.record_run(run_at("today", 0), NOW);
        assert_eq!(m.runs.len(), 2);
        assert!(KEEP_SECS >= WINDOWS.last().expect("windows non-empty").2);
    }
}
