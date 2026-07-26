//! `netcheck emit`: read a run's report and publish it to an OTLP collector.
//!
//! Separate from the check itself, and deliberately so: netcheck measures,
//! this decides which failures are severe, and the collector transports. What
//! counts as severe is deployment policy, not a property of the network.
//!
//! It lives in this crate rather than in a script because the translation
//! between the report and the published schema is the kind of coupling nothing
//! external can verify: reading the report back with [`OpReport`] and
//! [`RunMeta`], the structs that wrote it, makes a renamed field a compile
//! error instead of a silently missing column on a dashboard.

use std::io::Read;

use anyhow::{Context, Result};
use serde::Serialize;

use crate::EmitArgs;
use crate::report::{OpReport, RunMeta};
use crate::scenarios::put_get;

/// Dimensions whose failure is expected to happen occasionally and must not
/// page anyone, per scenario.
///
/// Keyed by scenario because a dimension label means whatever its own check
/// means by it: `put_get`'s retention windows are soft, since the hosting
/// cache is a byte-budget LRU and evicting a contract nobody wants is correct
/// behaviour rather than an outage. Another check reusing the label "24h"
/// must not inherit that.
///
/// A scenario absent here has no soft dimensions, so any failure is hard.
/// That is the safe default: a new check that fails, fails.
fn soft_dimensions(scenario: &str) -> &'static [&'static str] {
    if scenario == put_get::SCENARIO {
        &["24h", "48h", "7d"]
    } else {
        &[]
    }
}

#[derive(Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum Verdict {
    Pass,
    /// Only soft dimensions failed. A metric, not a page.
    Degraded,
    /// A hard operation failed.
    Fail,
    /// The check never ran, so it says nothing about the network.
    Error,
}

/// Numeric form of a dimension label, for ordering and plotting. Labels that
/// are not a duration (a hop, a phase) have none, and the consumer falls back
/// to showing the label as it is.
fn dimension_secs(dimension: &str) -> Option<i64> {
    let (value, unit) = dimension.split_at(dimension.len().checked_sub(1)?);
    let mult = match unit {
        "s" => 1,
        "m" => 60,
        "h" => 3600,
        "d" => 86400,
        _ => return None,
    };
    value.parse::<i64>().ok().map(|n| n * mult)
}

#[derive(Serialize)]
struct CheckOp<'a> {
    #[serde(rename = "type")]
    kind: &'static str,
    scenario: &'a str,
    run_id: &'a str,
    vantage: &'a str,
    op: &'a str,
    dimension: &'a str,
    dimension_secs: Option<i64>,
    contract_key: &'a str,
    ok: bool,
    latency_ms: u128,
    bytes: usize,
    error: Option<&'a str>,
    extra: serde_json::Value,
}

#[derive(Serialize)]
struct CheckRun<'a> {
    #[serde(rename = "type")]
    kind: &'static str,
    scenario: &'a str,
    run_id: &'a str,
    vantage: &'a str,
    duration_ms: u128,
    verdict: Verdict,
    ops_total: usize,
    ops_failed: usize,
    software_version: &'a str,
    conditions: serde_json::Value,
}

/// A parsed report: the run line, if it was reached, plus every operation.
#[derive(Default)]
pub struct ParsedReport {
    pub run: Option<RunMeta>,
    pub ops: Vec<OpReport>,
}

pub fn parse_report(input: &str) -> ParsedReport {
    let mut parsed = ParsedReport::default();
    for line in input.lines() {
        let line = line.trim();
        if !line.starts_with('{') {
            continue;
        }
        let Ok(value) = serde_json::from_str::<serde_json::Value>(line) else {
            continue;
        };
        match value.get("event").and_then(|e| e.as_str()) {
            Some("run") => {
                if let Ok(run) = serde_json::from_value(value) {
                    parsed.run = Some(run);
                }
            }
            Some("op") => {
                if let Ok(op) = serde_json::from_value(value) {
                    parsed.ops.push(op);
                }
            }
            _ => {}
        }
    }
    parsed
}

pub fn verdict(scenario: &str, ops: &[OpReport], exit_code: i32) -> Verdict {
    let soft = soft_dimensions(scenario);
    let (mut hard, mut degraded) = (0usize, 0usize);
    for op in ops.iter().filter(|o| !o.ok) {
        if soft.contains(&op.age.as_str()) {
            degraded += 1;
        } else {
            hard += 1;
        }
    }
    if hard > 0 {
        Verdict::Fail
    } else if degraded > 0 {
        Verdict::Degraded
    } else if exit_code != 0 {
        // Exited non-zero with nothing to blame it on: the check could not
        // run at all.
        Verdict::Error
    } else {
        Verdict::Pass
    }
}

/// Attributes carry routing only, and always as strings: a consumer reading
/// attribute values as string-or-double turns a boolean into a null. The
/// payload belongs in the body.
fn log_record<T: Serialize>(
    event_type: &str,
    vantage: &str,
    timestamp_ns: u128,
    body: &T,
) -> Result<serde_json::Value> {
    Ok(serde_json::json!({
        "timeUnixNano": timestamp_ns.to_string(),
        "body": { "stringValue": serde_json::to_string(body)? },
        "attributes": [
            { "key": "event_type", "value": { "stringValue": event_type } },
            { "key": "vantage", "value": { "stringValue": vantage } },
        ],
    }))
}

pub async fn run(args: EmitArgs) -> Result<bool> {
    let mut input = String::new();
    std::io::stdin()
        .read_to_string(&mut input)
        .context("reading the report on stdin")?;
    let parsed = parse_report(&input);
    if parsed.run.is_none() && parsed.ops.is_empty() {
        anyhow::bail!("no netcheck report found on stdin");
    }

    let run_meta = parsed.run.unwrap_or_default();
    // The scenario belongs to the run, not to how this was invoked: reading it
    // from the report means a second check cannot be filed under the first
    // because someone forgot a flag.
    let scenario = if run_meta.scenario.is_empty() {
        args.scenario.clone()
    } else {
        run_meta.scenario.clone()
    };
    let run_id = if run_meta.run_id.is_empty() {
        "unknown".to_string()
    } else {
        run_meta.run_id.clone()
    };
    let verdict = verdict(&scenario, &parsed.ops, args.exit_code);
    let failed = parsed.ops.iter().filter(|o| !o.ok).count();
    let now_ns = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .context("system clock before epoch")?
        .as_nanos();

    let mut records = Vec::with_capacity(parsed.ops.len() + 1);
    for op in &parsed.ops {
        records.push(log_record(
            "netcheck_op",
            &args.vantage,
            now_ns,
            &CheckOp {
                kind: "netcheck_op",
                scenario: &scenario,
                run_id: &run_id,
                vantage: &args.vantage,
                op: &op.op,
                dimension: &op.age,
                dimension_secs: dimension_secs(&op.age),
                contract_key: &op.key,
                ok: op.ok,
                latency_ms: op.latency_ms,
                bytes: op.size,
                error: op.error.as_deref(),
                extra: serde_json::json!({ "label": op.label }),
            },
        )?);
    }
    records.push(log_record(
        "netcheck_run",
        &args.vantage,
        now_ns,
        &CheckRun {
            kind: "netcheck_run",
            scenario: &scenario,
            run_id: &run_id,
            vantage: &args.vantage,
            duration_ms: run_meta.duration_ms,
            verdict,
            ops_total: parsed.ops.len(),
            ops_failed: failed,
            software_version: &run_meta.freenet_version,
            conditions: serde_json::json!({
                "gateway_ws": run_meta.gateway_ws,
                "pinned_gateways": run_meta.pinned_gateways,
                "ephemeral_peers": run_meta.ephemeral_peers,
                "exit_code": args.exit_code,
            }),
        },
    )?);

    let payload = serde_json::json!({
        "resourceLogs": [{ "scopeLogs": [{ "logRecords": records }] }]
    });
    eprintln!(
        "netcheck emit: run {run_id} ({scenario}), {} operations, {failed} failed",
        parsed.ops.len()
    );

    if args.dry_run {
        println!("{}", serde_json::to_string_pretty(&payload)?);
        return Ok(true);
    }

    let url = format!("{}/v1/logs", args.endpoint.trim_end_matches('/'));
    let response = reqwest::Client::new()
        .post(&url)
        .json(&payload)
        .send()
        .await
        .with_context(|| format!("posting to {url}"))?;
    anyhow::ensure!(
        response.status().is_success(),
        "collector at {url} returned {}",
        response.status()
    );
    Ok(true)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn op(op: &str, age: &str, ok: bool) -> OpReport {
        OpReport {
            op: op.to_string(),
            age: age.to_string(),
            label: "small-0".to_string(),
            key: "abc123".to_string(),
            ok,
            latency_ms: 42,
            size: 123,
            error: (!ok).then(|| "boom".to_string()),
        }
    }

    /// The reason this lives in the crate: a report written by `Report::push`
    /// must parse back into the same values. Rename a field in `OpReport` and
    /// this fails, instead of a column silently going blank downstream.
    #[test]
    fn a_report_round_trips_through_the_parser() {
        let written = op("get", "7d", false);
        let line = crate::report::op_line(&written).expect("serializes");
        let parsed = parse_report(&line);
        assert_eq!(parsed.ops.len(), 1);
        let read = &parsed.ops[0];
        assert_eq!(read.op, written.op);
        assert_eq!(read.age, written.age);
        assert_eq!(read.key, written.key);
        assert_eq!(read.ok, written.ok);
        assert_eq!(read.size, written.size);
        assert_eq!(read.error, written.error);
    }

    #[test]
    fn run_metadata_round_trips_and_carries_the_scenario() {
        let written = RunMeta::new(
            put_get::SCENARIO,
            "20260726-030000".to_string(),
            "ws://127.0.0.1:7509".to_string(),
            "0.2.108".to_string(),
            vec!["1.2.3.4:31337,ff".to_string()],
        );
        let parsed = parse_report(&crate::report::run_line(&written).expect("serializes"));
        let read = parsed.run.expect("run line parsed");
        assert_eq!(read.scenario, put_get::SCENARIO);
        assert_eq!(read.run_id, "20260726-030000");
        assert_eq!(read.pinned_gateways, written.pinned_gateways);
    }

    #[test]
    fn non_report_lines_are_ignored() {
        let parsed = parse_report("compiling netcheck\n{not json\n\nnetcheck summary: 8/8\n");
        assert!(parsed.run.is_none());
        assert!(parsed.ops.is_empty());
    }

    #[test]
    fn a_hard_failure_outranks_a_soft_one() {
        let ops = [op("get", "7d", false), op("put", "0h", false)];
        assert_eq!(verdict(put_get::SCENARIO, &ops, 0), Verdict::Fail);
    }

    #[test]
    fn only_soft_failures_are_degraded() {
        let ops = [op("get", "7d", false), op("get", "0h", true)];
        assert_eq!(verdict(put_get::SCENARIO, &ops, 0), Verdict::Degraded);
    }

    #[test]
    fn a_clean_run_passes_and_a_failed_start_is_an_error() {
        let ops = [op("get", "0h", true)];
        assert_eq!(verdict(put_get::SCENARIO, &ops, 0), Verdict::Pass);
        assert_eq!(verdict(put_get::SCENARIO, &[], 1), Verdict::Error);
    }

    /// A scenario nobody has classified must not inherit another's leniency.
    #[test]
    fn an_unknown_scenario_has_no_soft_dimensions() {
        let ops = [op("get", "7d", false)];
        assert_eq!(verdict("update_propagation", &ops, 0), Verdict::Fail);
    }

    #[test]
    fn dimension_secs_reads_durations_and_declines_the_rest() {
        assert_eq!(dimension_secs("0h"), Some(0));
        assert_eq!(dimension_secs("24h"), Some(86_400));
        assert_eq!(dimension_secs("7d"), Some(604_800));
        assert_eq!(dimension_secs("hop-1"), None);
        assert_eq!(dimension_secs(""), None);
    }
}
