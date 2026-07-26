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
    pub op: String,
    /// What was read: "0h" for this run's own contracts, otherwise the
    /// retention window of the run that published them.
    pub age: String,
    pub label: String,
    pub key: String,
    pub ok: bool,
    pub latency_ms: u128,
    pub size: usize,
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
    /// Record an operation and print its line as it happens, so a run that
    /// dies half way still leaves the operations it completed.
    pub fn push(&mut self, op: OpReport) {
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
