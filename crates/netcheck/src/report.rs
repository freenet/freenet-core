//! Results, printed as JSON lines on stdout so the nightly workflow log is
//! machine-parseable. Every line carries an `event` tag: one `"run"` line
//! describing the conditions of the run, then one `"op"` line per operation.
//! Phase 3 will additionally emit these as OTLP events to nova's collector.

use serde::Serialize;

/// Conditions the run happened under. Without them a failure cannot be
/// attributed after the fact.
#[derive(Serialize)]
pub struct RunMeta {
    pub event: &'static str,
    pub run_id: String,
    pub gateway_ws: String,
    pub freenet_version: String,
    pub pinned_gateways: Vec<String>,
    pub ephemeral_peers: Vec<String>,
}

impl RunMeta {
    pub fn new(
        run_id: String,
        gateway_ws: String,
        freenet_version: String,
        pinned_gateways: Vec<String>,
    ) -> Self {
        Self {
            event: "run",
            run_id,
            gateway_ws,
            freenet_version,
            pinned_gateways,
            ephemeral_peers: Vec::new(),
        }
    }

    pub fn print(&self) {
        match serde_json::to_string(self) {
            Ok(line) => println!("{line}"),
            Err(e) => eprintln!("run metadata serialization failed: {e}"),
        }
    }
}

#[derive(Serialize)]
pub struct OpReport {
    pub op: &'static str,
    /// Retention window of the target run ("0h" for this run's contracts).
    pub age: &'static str,
    pub label: String,
    pub key: String,
    pub ok: bool,
    pub latency_ms: u128,
    pub size: usize,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

#[derive(Serialize)]
struct TaggedOp<'a> {
    event: &'static str,
    #[serde(flatten)]
    op: &'a OpReport,
}

#[derive(Default)]
pub struct Report {
    ops: Vec<OpReport>,
}

impl Report {
    pub fn push(&mut self, op: OpReport) {
        let tagged = TaggedOp {
            event: "op",
            op: &op,
        };
        match serde_json::to_string(&tagged) {
            Ok(line) => println!("{line}"),
            Err(e) => eprintln!("report serialization failed: {e}"),
        }
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

    pub fn all_ok(&self) -> bool {
        self.ops.iter().all(|o| o.ok)
    }

    pub fn print_summary(&self) {
        let total = self.ops.len();
        let failed = self.ops.iter().filter(|o| !o.ok).count();
        eprintln!(
            "netcheck summary: {}/{} operations succeeded{}",
            total - failed,
            total,
            if failed > 0 {
                " — FAILURES PRESENT"
            } else {
                ""
            }
        );
    }
}
