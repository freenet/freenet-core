//! Results, printed as JSON lines on stdout so the nightly workflow log is
//! machine-parseable. Every line carries an `event` tag: one `"run"` line
//! describing the conditions of the run, then one `"op"` line per operation.
//!
//! These types are both `Serialize` and `Deserialize` on purpose: `netcheck
//! emit` reads a report back with the very structs that wrote it, so renaming
//! a field cannot silently desynchronise the two halves.

use serde::{Deserialize, Serialize};

/// Conditions the run happened under. Without them a failure cannot be
/// attributed after the fact.
#[derive(Serialize, Deserialize, Default)]
pub struct RunMeta {
    pub scenario: String,
    pub run_id: String,
    pub gateway_ws: String,
    pub freenet_version: String,
    pub pinned_gateways: Vec<String>,
    pub ephemeral_peers: Vec<String>,
    pub duration_ms: u128,
}

#[derive(Serialize)]
struct Tagged<'a, T> {
    event: &'static str,
    #[serde(flatten)]
    inner: &'a T,
}

impl RunMeta {
    pub fn new(
        scenario: &str,
        run_id: String,
        gateway_ws: String,
        freenet_version: String,
        pinned_gateways: Vec<String>,
    ) -> Self {
        Self {
            scenario: scenario.to_string(),
            run_id,
            gateway_ws,
            freenet_version,
            pinned_gateways,
            ephemeral_peers: Vec::new(),
            duration_ms: 0,
        }
    }

    pub fn print(&self) {
        print_line(run_line(self), "run metadata");
    }
}

#[derive(Serialize, Deserialize)]
pub struct OpReport {
    /// 0-based position in the order the run actually executed.
    ///
    /// The GET order is shuffled per run, so position is no longer a
    /// deterministic function of `age` and survives nowhere else: every record
    /// is emitted with the same timestamp, and line order is lost once the
    /// report is parsed. Without this field the shuffle only destroys the
    /// age/position information it was meant to disentangle.
    ///
    /// Assigned by [`Report::push`], never by the caller — see
    /// [`Report::SEQ_ASSIGNED_ON_PUSH`].
    #[serde(default)]
    pub seq: usize,
    pub op: String,
    /// What was read: "0h" for this run's own contracts, otherwise the
    /// retention window of the run that published them.
    pub age: String,
    pub label: String,
    pub key: String,
    pub ok: bool,
    pub latency_ms: u128,
    pub size: usize,
    /// How many incoming errors this operation attributed to another contract
    /// and skipped. Always emitted, including as zero: "no stale error arrived
    /// during this op" is a measurement, and a missing field would be
    /// indistinguishable from a run that never counted.
    #[serde(default)]
    pub errors_ignored: usize,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

/// The exact line `RunMeta::print` writes. Public so the emitter's tests can
/// prove a report survives the round trip through the structs that wrote it.
pub fn run_line(run: &RunMeta) -> serde_json::Result<String> {
    serde_json::to_string(&Tagged {
        event: "run",
        inner: run,
    })
}

/// The exact line `Report::push` writes.
pub fn op_line(op: &OpReport) -> serde_json::Result<String> {
    serde_json::to_string(&Tagged {
        event: "op",
        inner: op,
    })
}

fn print_line(line: serde_json::Result<String>, what: &str) {
    match line {
        Ok(line) => println!("{line}"),
        Err(e) => eprintln!("{what} serialization failed: {e}"),
    }
}

#[derive(Default)]
pub struct Report {
    ops: Vec<OpReport>,
}

impl Report {
    /// Placeholder for [`OpReport::seq`] at construction sites: [`Report::push`]
    /// overwrites it with the record's real position.
    ///
    /// Position is stamped by the recorder rather than passed in by the caller,
    /// so it cannot drift from the order the run actually executed — the same
    /// reason a filter's own count must come from the filter and not from
    /// arithmetic at the call site.
    pub const SEQ_ASSIGNED_ON_PUSH: usize = 0;

    /// Record an operation and print its line as it happens, so a run that
    /// dies half way still leaves the operations it completed.
    pub fn push(&mut self, mut op: OpReport) {
        op.seq = self.ops.len();
        print_line(op_line(&op), "report");
        if !op.ok {
            eprintln!(
                "FAIL {} {} {} ({}): {}",
                op.op,
                op.age,
                op.label,
                op.key,
                op.error.as_deref().unwrap_or("unknown")
            );
        }
        self.ops.push(op);
    }

    /// The operations recorded so far, in the order they ran. Exists so a test
    /// can read back what was recorded; the run itself only ever writes.
    #[cfg(test)]
    pub fn ops(&self) -> &[OpReport] {
        &self.ops
    }

    /// Whether every recorded operation succeeded.
    pub fn all_ok(&self) -> bool {
        self.ops.iter().all(|o| o.ok)
    }

    /// One human-readable line on stderr, separate from the JSON on stdout.
    pub fn print_summary(&self) {
        let total = self.ops.len();
        let failed = self.ops.iter().filter(|o| !o.ok).count();
        eprintln!(
            "netcheck summary: {}/{} operations succeeded{}",
            total - failed,
            total,
            if failed > 0 { ", FAILURES PRESENT" } else { "" }
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn op(label: &str, seq: usize) -> OpReport {
        OpReport {
            seq,
            op: "get".to_string(),
            age: "0h".to_string(),
            label: label.to_string(),
            key: "abc123".to_string(),
            ok: true,
            latency_ms: 1,
            size: 1,
            errors_ignored: 0,
            error: None,
        }
    }

    #[test]
    fn every_record_carries_its_position_in_the_run() {
        // The whole point of the field: an analysis asking "did this fail
        // because it was late in the run?" needs the position of each record,
        // and the shuffled GET order means it can no longer be inferred from
        // the age. Seeded deliberately wrong here, because a caller that
        // passes the wrong index must not be able to publish it.
        let mut report = Report::default();
        report.push(op("small-0", 99));
        report.push(op("small-1", 99));
        report.push(op("small-2", 99));

        assert_eq!(
            report.ops().iter().map(|o| o.seq).collect::<Vec<_>>(),
            vec![0, 1, 2],
            "position must be stamped by the recorder, in the order records arrive"
        );
    }
}
