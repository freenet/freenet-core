//! The nightly scenario: PUT via one gateway, GET everything back via a
//! fresh ephemeral peer joined through another, including previous runs'
//! contracts (retention).

use std::time::{Duration, SystemTime, UNIX_EPOCH};

use anyhow::{Context, Result};
use freenet_stdlib::prelude::*;

use crate::manifest::{ContractRecord, Manifest, RunRecord};
use crate::report::{OpReport, Report, RunMeta};
use crate::{PutGetArgs, client, contracts, ephemeral};

/// Name this scenario publishes under. A new check is a new value here,
/// never a new event type.
pub const SCENARIO: &str = "put_get";

pub async fn run(args: PutGetArgs) -> Result<bool> {
    let started = std::time::Instant::now();
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .context("system clock before epoch")?
        .as_secs();
    let run_id = chrono::Utc::now().format("%Y%m%d-%H%M%S").to_string();
    let op_timeout = Duration::from_secs(args.op_timeout_secs);

    eprintln!("netcheck put-get: run {run_id}");
    let mut meta = RunMeta::new(
        SCENARIO,
        run_id.clone(),
        args.gateway_ws.clone(),
        ephemeral::binary_version(&args.freenet_bin).await,
        args.gateway_spec.clone(),
    );
    if args.gateway_spec.is_empty() {
        eprintln!(
            "WARNING: no --gateway-spec given; the ephemeral peer will bootstrap from the \
             public index and may join through the same gateway the PUTs go through, which \
             weakens the check to a transfer test. See `ephemeral_peers` in the run report."
        );
    }
    let wasm = contracts::compile_contract_wasm(&args.contract_dir)?;
    let run_contracts = contracts::build_run_contracts(&wasm, &run_id, args.small_contracts)?;
    let mut manifest = Manifest::load(&args.manifest)?;
    let retention: Vec<(&'static str, &RunRecord)> = manifest.retention_targets(now);
    eprintln!(
        "retention targets this run: {:?}",
        retention
            .iter()
            .map(|(w, r)| (*w, r.run_id.as_str()))
            .collect::<Vec<_>>()
    );

    let mut report = Report::default();

    // ---- PUT phase, via the existing gateway ----
    let mut gw = client::connect(&args.gateway_ws, Duration::from_secs(15))
        .await
        .context("connecting to gateway ws api")?;
    let mut put_ok: Vec<&contracts::RunContract> = Vec::new();
    for c in &run_contracts {
        let key = c.contract.key();
        let attempt = client::put(
            &mut gw,
            c.contract.clone(),
            c.state.clone(),
            &c.label,
            op_timeout,
        )
        .await;
        let ok = attempt.outcome.is_ok();
        push_put_report(
            &mut report,
            &c.label,
            key.to_string(),
            c.state.as_ref().len(),
            attempt,
        );
        if ok {
            put_ok.push(c);
        }
    }
    client::disconnect(&mut gw).await;

    eprintln!(
        "{}/{} PUTs ok; settling {}s before booting the ephemeral getter...",
        put_ok.len(),
        run_contracts.len(),
        args.settle_secs
    );
    tokio::time::sleep(Duration::from_secs(args.settle_secs)).await;

    // ---- GET phase, via a fresh ephemeral peer ----
    let node = ephemeral::spawn_ephemeral(
        &args.freenet_bin,
        args.ephemeral_network_port,
        args.ephemeral_ws_port,
        &args.gateway_spec,
        args.ephemeral_dir.as_deref(),
    )
    .await?;

    let join_timeout = Duration::from_secs(args.join_timeout_secs);
    let mut peer = match client::connect(&node.ws_base, join_timeout).await {
        Ok(p) => p,
        Err(e) => {
            eprintln!("ephemeral node log tail:\n{}", node.log_tail());
            meta.duration_ms = started.elapsed().as_millis();
            meta.print();
            node.shutdown().await;
            return Err(e.context("connecting to ephemeral node ws api"));
        }
    };
    match client::wait_for_ring_join(&mut peer, join_timeout).await {
        Ok(peers) => meta.ephemeral_peers = peers,
        Err(e) => {
            eprintln!("ephemeral node log tail:\n{}", node.log_tail());
            meta.duration_ms = started.elapsed().as_millis();
            meta.print();
            node.shutdown().await;
            return Err(e);
        }
    }

    // One sequence covering this run's own contracts and every retention
    // window, interleaved so age is not the same variable as position in the
    // run (see `interleave`).
    let mut ops: Vec<GetOp> = Vec::new();
    for c in &put_ok {
        let key = c.contract.key();
        ops.push(GetOp {
            age: "0h",
            label: c.label.clone(),
            id: *key.id(),
            key: key.to_string(),
            size: c.state.as_ref().len(),
            verify: Verify::Identity(c),
        });
    }
    for (window, run) in &retention {
        for rec in &run.contracts {
            ops.push(GetOp {
                age: window,
                label: format!("{}/{}", run.run_id, rec.label),
                id: rec.id,
                key: rec.id.to_string(),
                size: rec.size,
                verify: Verify::Hash(&rec.state_hash),
            });
        }
    }
    let seed = interleave_seed(&run_id);
    interleave(&mut ops, seed);
    eprintln!(
        "GET order for this run (interleave seed {seed}, derived from run id {run_id}): {:?}",
        ops.iter().map(|o| o.age).collect::<Vec<_>>()
    );

    for op in &ops {
        let attempt = client::get(&mut peer, op.id, &op.label, op_timeout)
            .await
            .and_then(|(contract, state)| match op.verify {
                Verify::Identity(c) => {
                    anyhow::ensure!(
                        contract == c.contract,
                        "returned contract differs from what was PUT"
                    );
                    anyhow::ensure!(
                        state == c.state,
                        "returned state differs (got {} bytes, want {})",
                        state.as_ref().len(),
                        c.state.as_ref().len()
                    );
                    Ok(())
                }
                Verify::Hash(want) => {
                    let hash = blake3::hash(state.as_ref()).to_hex().to_string();
                    anyhow::ensure!(
                        hash == want,
                        "state hash mismatch (got {hash}, want {want})"
                    );
                    Ok(())
                }
            });
        push_get_report(
            &mut report,
            op.age,
            &op.label,
            op.key.clone(),
            op.size,
            attempt,
        );
    }

    client::disconnect(&mut peer).await;
    node.shutdown().await;

    // Record only successfully-PUT contracts so retention runs never chase
    // contracts that were never stored.
    if !put_ok.is_empty() {
        manifest.record_run(
            RunRecord {
                run_id,
                timestamp: now,
                contracts: put_ok
                    .iter()
                    .map(|c| ContractRecord {
                        id: *c.contract.key().id(),
                        label: c.label.clone(),
                        size: c.state.as_ref().len(),
                        state_hash: blake3::hash(c.state.as_ref()).to_hex().to_string(),
                    })
                    .collect(),
            },
            now,
        );
        manifest.save(&args.manifest)?;
    }

    meta.duration_ms = started.elapsed().as_millis();
    meta.print();
    report.print_summary();
    Ok(report.all_ok())
}

/// One GET the run will perform, and how to check what comes back.
struct GetOp<'a> {
    age: &'static str,
    label: String,
    id: ContractInstanceId,
    key: String,
    size: usize,
    verify: Verify<'a>,
}

enum Verify<'a> {
    /// This run's own contract: must come back byte-identical to what was PUT.
    Identity(&'a contracts::RunContract),
    /// A previous run's contract: checked against the recorded blake3.
    Hash(&'a str),
}

/// Seed for this run's GET ordering, derived from the run id.
///
/// Deterministic on purpose: the order is randomised to break the confound
/// described on [`interleave`], but an order nobody can reproduce trades one
/// diagnostic problem for another. The seed is logged next to the order it
/// produced, and re-deriving it from the run id replays that run exactly.
fn interleave_seed(run_id: &str) -> u64 {
    let digest = blake3::hash(run_id.as_bytes());
    let head: [u8; 8] = digest.as_bytes()[..8]
        .try_into()
        .expect("blake3 digest is 32 bytes");
    u64::from_le_bytes(head)
}

/// Shuffle the GET sequence so the age windows interleave.
///
/// The order used to be fixed: this run's 0h contracts, then 24h, then 48h,
/// then 7d, every night. That made "how old the contract is" and "how late in
/// the run the GET was issued" the same variable, so a failure clustered at the
/// end of a run could not be told apart from a failure of the oldest
/// contracts — which is exactly the question this check exists to answer.
fn interleave<T>(ops: &mut [T], seed: u64) {
    use rand::SeedableRng;
    use rand::seq::SliceRandom;

    let mut rng = rand::rngs::StdRng::seed_from_u64(seed);
    ops.shuffle(&mut rng);
}

/// Record a GET.
///
/// The latency written is what the operation actually took, on both arms. A
/// failure used to report the configured op timeout instead, which looked like
/// a measurement while being a constant: it made a fast terminal failure from
/// the node indistinguishable from the client giving up at its deadline. The
/// timeout is deliberately not a parameter here, so no path through this
/// function can synthesize one.
fn push_get_report(
    report: &mut Report,
    age: &'static str,
    label: &str,
    key: String,
    size: usize,
    attempt: client::Attempt<()>,
) {
    report.push(OpReport {
        seq: Report::SEQ_ASSIGNED_ON_PUSH,
        op: "get".to_string(),
        age: age.to_string(),
        label: label.to_string(),
        key,
        ok: attempt.outcome.is_ok(),
        latency_ms: attempt.latency.as_millis(),
        size,
        errors_ignored: attempt.errors_ignored,
        error: attempt.outcome.err().map(|e| format!("{e:#}")),
    });
}

/// Record a PUT. Same contract as [`push_get_report`], and for the same
/// reason: the PUT loop had the identical hardcoded-timeout defect, and its
/// failures land in the same report with the same field.
fn push_put_report(
    report: &mut Report,
    label: &str,
    key: String,
    size: usize,
    attempt: client::Attempt<()>,
) {
    report.push(OpReport {
        seq: Report::SEQ_ASSIGNED_ON_PUSH,
        op: "put".to_string(),
        // Every PUT is this run's own contract, so there is no other age.
        age: "0h".to_string(),
        label: label.to_string(),
        key,
        ok: attempt.outcome.is_ok(),
        latency_ms: attempt.latency.as_millis(),
        size,
        errors_ignored: attempt.errors_ignored,
        error: attempt.outcome.err().map(|e| format!("{e:#}")),
    });
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;

    use super::*;

    /// The GET sequence as it was built before interleaving: this run's own
    /// contracts, then each retention window in `WINDOWS` order. Items are
    /// unique so a shuffle that drops or duplicates one is visible.
    fn fixed_sequence() -> Vec<(&'static str, usize)> {
        let mut ops = Vec::new();
        for age in ["0h", "24h", "48h", "7d"] {
            for i in 0..3 {
                ops.push((age, i));
            }
        }
        ops
    }

    /// The sequence a run with this id would actually issue.
    fn sequence_for(run_id: &str) -> Vec<(&'static str, usize)> {
        let mut ops = fixed_sequence();
        interleave(&mut ops, interleave_seed(run_id));
        ops
    }

    #[test]
    fn the_get_order_is_not_the_fixed_age_sequence() {
        assert_ne!(
            sequence_for("20260811-051006"),
            fixed_sequence(),
            "the run must not issue 0h, then 24h, then 48h, then 7d in that order"
        );
    }

    #[test]
    fn the_oldest_contracts_are_not_always_last() {
        // The confound this fixes: with a fixed order the 7d GETs were always
        // at the end of every run, so "old contract" and "late in the run"
        // could not be told apart.
        let last: HashSet<&str> = (0..20)
            .map(|i| {
                sequence_for(&format!("2026081{i}-051006"))
                    .last()
                    .expect("sequence is non-empty")
                    .0
            })
            .collect();
        assert!(
            last.len() > 1,
            "the last GET of a run was always the {last:?} bucket across 20 runs; age is still \
             confounded with position in the run"
        );
    }

    #[test]
    fn interleaving_keeps_every_op_exactly_once() {
        // A shuffle that silently drops or duplicates an op would trade one
        // reporting defect for a worse one.
        let mut want = fixed_sequence();
        let mut got = sequence_for("20260811-051006");
        want.sort_unstable();
        got.sort_unstable();
        assert_eq!(got, want);
    }

    #[test]
    fn the_order_is_reproducible_from_the_run_id() {
        // Randomness nobody can replay is its own diagnostic problem: the seed
        // is derived from the run id and logged, so a run can be re-issued in
        // the order it actually used.
        assert_eq!(
            sequence_for("20260811-051006"),
            sequence_for("20260811-051006")
        );
        assert_eq!(
            interleave_seed("20260811-051006"),
            interleave_seed("20260811-051006")
        );
    }

    #[test]
    fn different_runs_get_different_orders() {
        let orders: HashSet<Vec<(&str, usize)>> = (0..20)
            .map(|i| sequence_for(&format!("2026081{i}-051006")))
            .collect();
        assert!(
            orders.len() > 1,
            "every run produced the same order; the seed is not reaching the shuffle"
        );
    }

    /// A failed operation that took `latency`, as the client would return it.
    fn failed_after(latency: Duration) -> client::Attempt<()> {
        client::Attempt {
            outcome: Err(anyhow::anyhow!(
                "stream assembly: no fragments received within inactivity timeout"
            )),
            latency,
            errors_ignored: 0,
        }
    }

    #[test]
    fn a_failed_get_reports_the_time_it_actually_took() {
        // The nightly's configured deadline. Before the fix every failure
        // reported exactly this, whatever it had really taken.
        let op_timeout = Duration::from_secs(120);
        let measured = Duration::from_millis(37);

        let mut report = Report::default();
        push_get_report(
            &mut report,
            "7d",
            "20260730-051006/small-2",
            "some-key".to_string(),
            1024,
            failed_after(measured),
        );

        let op = report.ops().last().expect("one op was recorded");
        assert!(!op.ok);
        assert_eq!(
            op.latency_ms,
            measured.as_millis(),
            "a failed op must carry its measured latency"
        );
        assert_ne!(
            op.latency_ms,
            op_timeout.as_millis(),
            "reporting the configured timeout makes a fast terminal failure look like the client \
             giving up at its deadline"
        );
    }

    #[test]
    fn a_failed_put_reports_the_time_it_actually_took() {
        // The PUT loop had the identical defect. Its failures land in the same
        // report under the same field, so it needs the same guard.
        let op_timeout = Duration::from_secs(120);
        let measured = Duration::from_millis(37);

        let mut report = Report::default();
        push_put_report(
            &mut report,
            "small-2",
            "some-key".to_string(),
            1024,
            failed_after(measured),
        );

        let op = report.ops().last().expect("one op was recorded");
        assert!(!op.ok);
        assert_eq!(
            op.latency_ms,
            measured.as_millis(),
            "a failed PUT must carry its measured latency"
        );
        assert_ne!(
            op.latency_ms,
            op_timeout.as_millis(),
            "reporting the configured timeout makes a fast terminal PUT failure look like the \
             client giving up at its deadline"
        );
    }

    #[test]
    fn a_report_records_how_many_errors_the_key_filter_absorbed() {
        // Without this the count reaches stderr only, i.e. the workflow log
        // rather than the artifact people analyse: "was there a stale error
        // during this run?" becomes unanswerable from `last-run.jsonl`.
        let mut report = Report::default();
        push_get_report(
            &mut report,
            "0h",
            "small-0",
            "some-key".to_string(),
            1024,
            client::Attempt {
                outcome: Ok(()),
                latency: Duration::from_millis(12),
                errors_ignored: 2,
            },
        );
        assert_eq!(report.ops().last().expect("one op").errors_ignored, 2);
    }

    /// The body of a column-0 free function, bounded to that function.
    ///
    /// Copied from `commands::auto_update`, which carries the incident this
    /// exists for (#5102): a bare `split_once` on a moved anchor does not fail,
    /// it matches a later occurrence — usually the pin's own assertion string —
    /// and the pin then passes vacuously under a name that says it is covered.
    fn fn_body<'a>(src: &'a str, signature: &str) -> &'a str {
        let at = src
            .find(signature)
            .unwrap_or_else(|| panic!("definition not found: {signature}"));
        let tests_at = src
            .find("\n#[cfg(test)]\nmod ")
            .map(|i| i + 1)
            .expect("test module not located — this guard cannot verify anything");
        assert!(
            at < tests_at,
            "`{signature}` matched inside the test module — this pin is scraping its own source \
             and would pass vacuously"
        );
        assert!(
            at == 0 || src.as_bytes()[at - 1] == b'\n',
            "fn_body only supports column-0 free functions; `{signature}` is indented (a \
             method?), where the `\\n}}\\n` end-anchor would slice to the end of the enclosing impl"
        );
        let after = &src[at + signature.len()..];
        let (body, _) = after
            .split_once("\n}\n")
            .unwrap_or_else(|| panic!("could not locate end of: {signature}"));
        assert!(
            !body.contains("\n#[cfg(test)]\nmod "),
            "scoped region for `{signature}` escaped into the test module — this pin would pass \
             vacuously"
        );
        body
    }

    #[test]
    fn the_run_shuffles_the_real_op_list_after_both_loops_have_filled_it() {
        // Every other ordering test drives `interleave` against a synthetic
        // vector, which says nothing about the call site. Moving the call three
        // lines up — above the retention loop — restores the exact confound
        // this change removes, every night, with tests and clippy green.
        let body = fn_body(
            include_str!("put_get.rs"),
            "pub async fn run(args: PutGetArgs) -> Result<bool> {",
        );
        let (before, after) = body
            .split_once("interleave(&mut ops, seed);")
            .expect("run() must shuffle the op list it is about to issue");

        assert!(
            before.contains("for c in &put_ok {"),
            "the shuffle must come after this run's own contracts are added, or they are not in it"
        );
        assert!(
            before.contains("for (window, run) in &retention {"),
            "the shuffle must come after the retention windows are added, or the oldest contracts \
             are back at the end of every run — the confound this exists to remove"
        );
        assert!(
            after.contains("for op in &ops {"),
            "the GETs must be issued after the shuffle, or it changed nothing"
        );
    }
}
