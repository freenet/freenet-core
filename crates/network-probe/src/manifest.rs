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
