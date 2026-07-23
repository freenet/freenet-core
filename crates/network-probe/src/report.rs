//! Per-operation results, printed as JSON lines on stdout so the nightly
//! workflow log is machine-parseable. Phase 3 will additionally emit these
//! as OTLP events to nova's telemetry collector.

use serde::Serialize;

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

#[derive(Default)]
pub struct Report {
    ops: Vec<OpReport>,
}

impl Report {
    pub fn push(&mut self, op: OpReport) {
        match serde_json::to_string(&op) {
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
            "network-probe summary: {}/{} operations succeeded{}",
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
