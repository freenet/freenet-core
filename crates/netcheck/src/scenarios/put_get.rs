//! The nightly scenario: PUT via one gateway, GET everything back via a
//! fresh ephemeral peer joined through another, including previous runs'
//! contracts (retention).
//!
//! The two phases can run in one process (`put-get`) or on two hosts (`put`,
//! then `get`), joined by a [`Handoff`] file. The nightly uses two hosts; why
//! is in this crate's README, under "Production use".

use std::path::Path;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use anyhow::{Context, Result};
use freenet_stdlib::prelude::*;
use serde::{Deserialize, Serialize};

use crate::manifest::{ContractRecord, Manifest, RunRecord};
use crate::report::{OpReport, Report, RunMeta};
use crate::{GetArgs, GetCmd, PutArgs, PutCmd, PutGetArgs, client, contracts, ephemeral};

/// Name this scenario publishes under. A new check is a new value here,
/// never a new event type.
pub const SCENARIO: &str = "put_get";

/// Everything the GET phase needs from the PUT phase.
///
/// Self-contained on purpose: the retention targets are resolved against the
/// manifest on the PUT host and travel in here, so the manifest never has to
/// leave that host and the GET host needs no state of its own.
#[derive(Serialize, Deserialize)]
pub struct Handoff {
    pub run_id: String,
    /// The gateway the PUTs went through, for the report's run line.
    pub gateway_ws: String,
    pub put_duration_ms: u128,
    /// Unix time, in milliseconds, at which the PUTs finished. The GET phase
    /// reports how long after this it started (`put_get_gap_ms`).
    pub puts_finished_unix_ms: u128,
    /// Set when the PUT host could not record this run in its manifest. The
    /// GETs still run, but the GET phase then fails, so a night that later
    /// retention windows will be missing is not reported as a clean one.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub manifest_error: Option<String>,
    /// This run's PUT records, in the order they ran. The GET phase replays
    /// them into its report so the published report covers the whole run.
    pub put_ops: Vec<OpReport>,
    /// What to GET, in build order; the GET phase shuffles it.
    pub targets: Vec<GetTarget>,
}

impl Handoff {
    pub fn load(path: &Path) -> Result<Self> {
        let bytes =
            std::fs::read(path).with_context(|| format!("reading handoff {}", path.display()))?;
        serde_json::from_slice(&bytes)
            .with_context(|| format!("parsing handoff {}", path.display()))
    }

    /// Temp file and rename, as for the manifest: a truncated handoff would
    /// fail the GET phase for a reason that has nothing to do with the network.
    pub fn save(&self, path: &Path) -> Result<()> {
        let tmp = path.with_extension("tmp");
        std::fs::write(&tmp, serde_json::to_vec_pretty(self)?)
            .with_context(|| format!("writing {}", tmp.display()))?;
        std::fs::rename(&tmp, path).with_context(|| format!("writing {}", path.display()))?;
        Ok(())
    }
}

/// One contract the GET phase must fetch, and what it must come back as.
#[derive(Serialize, Deserialize, Debug, PartialEq)]
pub struct GetTarget {
    /// "0h" for this run's own contracts, otherwise the retention window.
    pub age: String,
    pub label: String,
    pub id: ContractInstanceId,
    pub size: usize,
    /// blake3 of the state that was PUT.
    pub state_hash: String,
}

/// What the PUT phase produced, before anything is persisted.
///
/// The caller persists it in a fixed order: the handoff first, then the
/// manifest. A manifest that cannot be written must not cost the night its
/// GETs, so its failure is carried in the handoff instead of ending the run.
struct PutOutcome {
    handoff: Handoff,
    manifest: Manifest,
    /// This run's successful PUTs, for the manifest. None when nothing was
    /// stored, so later runs never chase contracts that do not exist.
    record: Option<RunRecord>,
    now: u64,
}

impl PutOutcome {
    /// Record this run's PUTs in the manifest. On failure the error is noted
    /// in the handoff and returned, for the caller to persist and report.
    fn record(&mut self, path: &Path) -> Result<()> {
        let Some(record) = self.record.take() else {
            return Ok(());
        };
        self.manifest.record_run(record, self.now);
        if let Err(e) = self.manifest.save(path) {
            let e = e.context(format!("recording this run in {}", path.display()));
            eprintln!("WARNING: {e:#}");
            self.handoff.manifest_error = Some(format!("{e:#}"));
            return Err(e);
        }
        Ok(())
    }
}

/// Both phases in one process, from one host. For local testing against
/// `local-gateway`; the nightly runs `put` and `get` on different hosts.
pub async fn run(args: PutGetArgs) -> Result<bool> {
    let started = Instant::now();
    let op_timeout = Duration::from_secs(args.op_timeout_secs);
    let run_id = new_run_id();
    let mut report = Report::default();
    let mut put = match put_phase(&args.put, run_id.clone(), op_timeout, &mut report).await {
        Ok(put) => put,
        Err(e) => return Err(report_put_failure(e, run_id, &args.put, &report)),
    };
    // Already carried in the handoff; the GET phase reports it.
    let _ = put.record(&args.put.manifest);

    eprintln!(
        "settling {}s before booting the ephemeral getter...",
        args.settle_secs
    );
    tokio::time::sleep(Duration::from_secs(args.settle_secs)).await;
    put.handoff.put_duration_ms = started.elapsed().as_millis();

    finish_get(&put.handoff, &args.get, op_timeout, report).await
}

/// The PUT half. Succeeds once the handoff is written, whether or not every
/// PUT did, and whether or not the manifest could be updated: both are in
/// the handoff, and the GET phase's verdict covers them. Failing here instead
/// would skip the GETs, and with them the retention windows the night was
/// still able to measure.
pub async fn run_put(args: PutCmd) -> Result<bool> {
    let run_id = new_run_id();
    let mut report = Report::default();
    let op_timeout = Duration::from_secs(args.op_timeout_secs);
    let mut put = match put_phase(&args.put, run_id.clone(), op_timeout, &mut report).await {
        Ok(put) => put,
        Err(e) => return Err(report_put_failure(e, run_id, &args.put, &report)),
    };
    // Before anything below can fail: this host's report is what gets
    // published when the GET side leaves none.
    print_put_run_line(run_id, &args.put, put.handoff.put_duration_ms);
    if let Some(vantage) = &args.vantage {
        stamp_vantage(&mut put.handoff.put_ops, vantage);
    }
    put.handoff.save(&args.handoff)?;
    eprintln!(
        "handoff written to {} ({} GET targets)",
        args.handoff.display(),
        put.handoff.targets.len()
    );
    if put.record(&args.put.manifest).is_err() {
        // Best effort: the handoff already on disk lacks only the note.
        put.handoff.save(&args.handoff)?;
    }
    Ok(true)
}

/// The GET half. Prints the whole run's report, PUTs included, so its output
/// is what gets published.
pub async fn run_get(args: GetCmd) -> Result<bool> {
    let handoff = Handoff::load(&args.handoff)?;
    let report = replay_puts(&handoff);
    finish_get(
        &handoff,
        &args.get,
        Duration::from_secs(args.op_timeout_secs),
        report,
    )
    .await
}

/// The GET phase, then the PUT host's manifest error if it had one: the GETs
/// ran and were reported, but a night whose run is missing from the manifest
/// must not end clean.
async fn finish_get(
    handoff: &Handoff,
    args: &GetArgs,
    op_timeout: Duration,
    report: Report,
) -> Result<bool> {
    let all_ok = get_phase(handoff, args, op_timeout, report).await?;
    if let Some(e) = &handoff.manifest_error {
        anyhow::bail!("the PUT host could not record this run for later retention windows: {e}");
    }
    Ok(all_ok)
}

/// A run's id: its start time, which is also what the GET order's seed is
/// derived from.
fn new_run_id() -> String {
    chrono::Utc::now().format("%Y%m%d-%H%M%S").to_string()
}

/// A PUT phase that could not finish still prints a run line, so the report
/// it leaves (any PUTs it made, and the line) can be published as a run that
/// could not complete instead of as nothing at all.
fn report_put_failure(
    e: anyhow::Error,
    run_id: String,
    args: &PutArgs,
    report: &Report,
) -> anyhow::Error {
    print_put_run_line(run_id, args, 0);
    report.print_summary();
    e
}

/// The `put` host's report is published only when the GET side left none, as
/// a run that could not complete. Without its own run line that record would
/// carry no run id, so it could not be matched to the night's handoff.
fn print_put_run_line(run_id: String, args: &PutArgs, duration_ms: u128) {
    let order_seed = Some(interleave_seed(&run_id));
    RunMeta {
        duration_ms,
        order_seed,
        ..RunMeta::new(
            SCENARIO,
            run_id,
            args.gateway_ws.clone(),
            String::new(),
            Vec::new(),
        )
    }
    .print();
}

/// Mark the PUT records with the host they ran on. The report is published
/// from the GET side, under the GET side's vantage, and the PUTs did not run
/// there.
fn stamp_vantage(put_ops: &mut [OpReport], vantage: &str) {
    for op in put_ops {
        op.vantage = Some(vantage.to_string());
    }
}

/// A report holding the handoff's PUT records, printed again as they are
/// added: the `put` host's stdout is not the published report, so without
/// this the PUTs would be missing from it, and a failed PUT from the verdict.
fn replay_puts(handoff: &Handoff) -> Report {
    let mut report = Report::default();
    for op in &handoff.put_ops {
        report.push(op.clone());
    }
    report
}

fn unix_ms() -> Result<u128> {
    Ok(SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .context("system clock before epoch")?
        .as_millis())
}

/// PUT this run's contracts through the gateway and resolve what the GET
/// phase must fetch. Persists nothing; see [`PutOutcome`].
async fn put_phase(
    args: &PutArgs,
    run_id: String,
    op_timeout: Duration,
    report: &mut Report,
) -> Result<PutOutcome> {
    let started = Instant::now();
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .context("system clock before epoch")?
        .as_secs();

    eprintln!("netcheck put: run {run_id}");
    let wasm = contracts::compile_contract_wasm(&args.contract_dir)?;
    let run_contracts = contracts::build_run_contracts(&wasm, &run_id, args.small_contracts)?;
    let manifest = Manifest::load(&args.manifest)?;
    let retention: Vec<(&'static str, &RunRecord)> = manifest.retention_targets(now);
    eprintln!(
        "retention targets this run: {:?}",
        retention
            .iter()
            .map(|(w, r)| (*w, r.run_id.as_str()))
            .collect::<Vec<_>>()
    );
    let seed = interleave_seed(&run_id);

    // Shuffled for the same reason the GET phase is. `build_run_contracts`
    // appends `large-1MB` after the smalls, so issuing them in vector order
    // put the only large contract last every single night: "the 1 MB PUT
    // failed" and "the last PUT failed" were one variable, which is the
    // age/position confound again on the size axis. Fixing one phase and not
    // the other would leave the report unable to answer the size question
    // while claiming to have fixed the ordering problem.
    let mut put_order: Vec<&contracts::RunContract> = run_contracts.iter().collect();
    interleave(&mut put_order, seed);
    eprintln!(
        "PUT order for this run (interleave seed {seed}): {:?}",
        put_order
            .iter()
            .map(|c| c.label.as_str())
            .collect::<Vec<_>>()
    );

    let mut gw = client::connect(&args.gateway_ws, Duration::from_secs(15))
        .await
        .context("connecting to gateway ws api")?;
    let mut put_ok: Vec<&contracts::RunContract> = Vec::new();
    for c in put_order {
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
            report,
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
    eprintln!("{}/{} PUTs ok", put_ok.len(), run_contracts.len());

    let targets = get_targets(&put_ok, &retention);
    let handoff = handoff_from(
        run_id.clone(),
        args.gateway_ws.clone(),
        started.elapsed().as_millis(),
        unix_ms()?,
        report,
        targets,
    );
    Ok(PutOutcome {
        record: run_record(run_id, now, &put_ok),
        handoff,
        manifest,
        now,
    })
}

/// The handoff for a finished PUT phase. Its own function so a test can hold
/// the one place the PUT records enter it: `put` exits 0 with a failed PUT by
/// design, so a handoff that dropped them would turn that failure green.
fn handoff_from(
    run_id: String,
    gateway_ws: String,
    put_duration_ms: u128,
    puts_finished_unix_ms: u128,
    report: &Report,
    targets: Vec<GetTarget>,
) -> Handoff {
    Handoff {
        run_id,
        gateway_ws,
        put_duration_ms,
        puts_finished_unix_ms,
        manifest_error: None,
        put_ops: report.ops().to_vec(),
        targets,
    }
}

/// This run's own contracts, then each retention window's, in build order.
fn get_targets(
    put_ok: &[&contracts::RunContract],
    retention: &[(&'static str, &RunRecord)],
) -> Vec<GetTarget> {
    let mut targets = Vec::new();
    for c in put_ok {
        targets.push(GetTarget {
            age: "0h".to_string(),
            label: c.label.clone(),
            id: *c.contract.key().id(),
            size: c.state.as_ref().len(),
            state_hash: blake3::hash(c.state.as_ref()).to_hex().to_string(),
        });
    }
    for (window, run) in retention {
        for rec in &run.contracts {
            targets.push(GetTarget {
                age: window.to_string(),
                label: format!("{}/{}", run.run_id, rec.label),
                id: rec.id,
                size: rec.size,
                state_hash: rec.state_hash.clone(),
            });
        }
    }
    targets
}

/// Boot the ephemeral getter, wait for it to join the ring, and GET every
/// target. `report` already holds this run's PUTs; the verdict covers both.
async fn get_phase(
    handoff: &Handoff,
    args: &GetArgs,
    op_timeout: Duration,
    mut report: Report,
) -> Result<bool> {
    let started = Instant::now();
    let run_id = &handoff.run_id;
    eprintln!("netcheck get: run {run_id}");
    let mut meta = RunMeta::new(
        SCENARIO,
        run_id.clone(),
        handoff.gateway_ws.clone(),
        ephemeral::binary_version(&args.freenet_bin).await,
        args.gateway_spec.clone(),
    );
    let seed = interleave_seed(run_id);
    meta.order_seed = Some(seed);
    meta.put_get_gap_ms = Some(unix_ms()?.saturating_sub(handoff.puts_finished_unix_ms));
    // Time spent in either phase. The gap between them is `put_get_gap_ms`.
    let duration = |started: Instant| handoff.put_duration_ms + started.elapsed().as_millis();
    if args.gateway_spec.is_empty() {
        eprintln!(
            "WARNING: no --gateway-spec given; the ephemeral peer will bootstrap from the \
             public index and may join through the same gateway the PUTs go through, which \
             weakens the check to a transfer test. See `ephemeral_peers` in the run report."
        );
    }

    let node = match ephemeral::spawn_ephemeral(
        &args.freenet_bin,
        args.ephemeral_network_port,
        args.ephemeral_ws_port,
        &args.gateway_spec,
        args.ephemeral_dir.as_deref(),
    )
    .await
    {
        Ok(node) => node,
        Err(e) => {
            meta.duration_ms = duration(started);
            meta.print();
            report.print_summary();
            return Err(e.context("booting the ephemeral getter"));
        }
    };

    let join_timeout = Duration::from_secs(args.join_timeout_secs);
    let mut peer = match client::connect(&node.ws_base, join_timeout).await {
        Ok(p) => p,
        Err(e) => {
            eprintln!("ephemeral node log tail:\n{}", node.log_tail());
            meta.duration_ms = duration(started);
            meta.print();
            report.print_summary();
            node.shutdown().await;
            return Err(e.context("connecting to ephemeral node ws api"));
        }
    };
    match client::wait_for_ring_join(&mut peer, join_timeout).await {
        Ok(joined) => {
            meta.ephemeral_peers = joined.peers;
            meta.ephemeral_addr = joined.own_addr;
            meta.ephemeral_location = joined.own_location;
        }
        Err(e) => {
            eprintln!("ephemeral node log tail:\n{}", node.log_tail());
            meta.duration_ms = duration(started);
            meta.print();
            report.print_summary();
            node.shutdown().await;
            return Err(e);
        }
    }

    // One sequence covering this run's own contracts and every retention
    // window, interleaved so age is not the same variable as position in the
    // run (see `interleave`).
    let mut ops: Vec<&GetTarget> = Vec::new();
    for t in &handoff.targets {
        ops.push(t);
    }
    interleave(&mut ops, seed);
    eprintln!(
        "GET order for this run (interleave seed {seed}, derived from run id {run_id}): {:?}",
        ops.iter().map(|o| o.age.as_str()).collect::<Vec<_>>()
    );

    for op in &ops {
        let attempt = client::get(&mut peer, op.id, &op.label, op_timeout)
            .await
            .and_then(|(contract, state)| verify_get(op, &contract, &state));
        push_get_report(
            &mut report,
            &op.age,
            &op.label,
            op.id.to_string(),
            op.size,
            attempt,
        );
    }

    client::disconnect(&mut peer).await;
    node.shutdown().await;

    meta.duration_ms = duration(started);
    meta.print();
    report.print_summary();
    Ok(report.all_ok())
}

/// A GET counts only if it returned the contract asked for, with the state
/// that was PUT.
///
/// The contract's key is recomputed from the code and parameters that came
/// back, not read from the key the response carries: that field is
/// deserialized as sent, so trusting it would accept any code under the right
/// label. The state is compared by blake3, so this is as strict as comparing
/// the bytes while needing only what the handoff carries.
fn verify_get(
    target: &GetTarget,
    contract: &ContractContainer,
    state: &WrappedState,
) -> Result<()> {
    let ContractContainer::Wasm(ContractWasmAPIVersion::V1(wrapped)) = contract else {
        anyhow::bail!("returned contract is not a V1 WASM contract");
    };
    let computed = ContractKey::from_params_and_code(wrapped.params(), &**wrapped.code());
    anyhow::ensure!(
        *computed.id() == target.id,
        "returned contract's code and parameters hash to {}, not the requested {}",
        computed.id(),
        target.id
    );
    let hash = blake3::hash(state.as_ref()).to_hex().to_string();
    anyhow::ensure!(
        hash == target.state_hash,
        "state hash mismatch (got {hash} for {} bytes, want {})",
        state.as_ref().len(),
        target.state_hash
    );
    Ok(())
}

/// This run's successful PUTs as a manifest entry, or None if there were
/// none, so retention runs never chase contracts that were never stored.
fn run_record(run_id: String, now: u64, put_ok: &[&contracts::RunContract]) -> Option<RunRecord> {
    if put_ok.is_empty() {
        return None;
    }
    Some(RunRecord {
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
    })
}

/// Seed for this run's GET ordering, derived from the run id.
///
/// Deterministic on purpose: the order is randomised to break the confound
/// described on [`interleave`], but an order nobody can reproduce trades one
/// diagnostic problem for another. The seed is logged next to the order it
/// produced, and re-deriving it from the run id replays that run exactly --
/// on any `rand` version, because [`interleave`] pins the generator, the seed
/// expansion, and the shuffle algorithm rather than inheriting any of them.
/// (The permutation still depends on how many ops the run had, which varies
/// with which retention windows the manifest had populated.)
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
    // Everything the permutation depends on is pinned here on purpose, because
    // a replay that is only usually reproducible is not reproducible.
    //
    // Three separate things in the `rand` stack are version-unstable, and
    // fixing one of them is not enough:
    //
    //  1. `StdRng`'s algorithm. Documented as NOT stable across releases. This
    //     is why the generator below is `ChaCha8Rng` from `rand_chacha`, which
    //     carries a portability guarantee.
    //  2. `SeedableRng::seed_from_u64`. It is `rand_core`'s DEFAULT expansion,
    //     not a ChaCha guarantee, and its own doc calls changing it merely
    //     "a value-breaking change" -- a convention, not a promise. So the
    //     seed bytes are built here rather than expanded by it.
    //  3. `SliceRandom::shuffle`. This is the one that is easy to miss: the
    //     algorithm lives in `rand`, not in the generator, and it changed
    //     between 0.8 and 0.9 (reverse Fisher-Yates with `gen_index`, vs. a
    //     forward pass driven by `IncreasingUniform`). Same seed, same
    //     byte-identical ChaCha stream, different permutation. So the shuffle
    //     is written out below rather than delegated.
    //
    // Every one of those would break replay silently while
    // `the_order_is_reproducible_from_the_run_id` stayed green, since that test
    // only compares one build against itself.
    let mut seed_bytes = [0u8; 32];
    seed_bytes[..8].copy_from_slice(&seed.to_le_bytes());
    let mut rng = <rand_chacha::ChaCha8Rng as rand_core10::SeedableRng>::from_seed(seed_bytes);

    // Fisher-Yates. The modulo bias is bounded by `len / 2^64` and `len` here
    // is at most a few dozen ops, so it is many orders of magnitude below the
    // point where it could shape a diagnosis.
    for i in (1..ops.len()).rev() {
        use rand_core10::Rng as _;
        let j = (rng.next_u64() % (i as u64 + 1)) as usize;
        ops.swap(i, j);
    }
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
    age: &str,
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
        vantage: None,
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
        vantage: None,
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

    /// Split `body` at a call that is actually executed.
    ///
    /// `split_once` on the call text alone is not enough: commenting the call
    /// out leaves the string in place, so the pin passes while the call is
    /// gone. That is the same vacuity `fn_body` guards against one level up,
    /// and it was reachable here — verified by commenting the call out and
    /// watching the pin stay green. Requiring the call to begin its line, after
    /// nothing but whitespace, is what makes deleting or disabling it fail.
    fn split_at_call<'a>(body: &'a str, call: &str) -> (&'a str, &'a str) {
        let at = body
            .match_indices(call)
            .find(|(i, _)| {
                let line_start = body[..*i].rfind('\n').map_or(0, |n| n + 1);
                body[line_start..*i].trim().is_empty()
            })
            .map(|(i, _)| i)
            .unwrap_or_else(|| {
                panic!(
                    "`{call}` is not called at statement position in this function — it is \
                     missing, or commented out, or buried inside another expression"
                )
            });
        (&body[..at], &body[at + call.len()..])
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
        let body = fn_body(include_str!("put_get.rs"), "async fn get_phase(");
        let (before, after) = split_at_call(body, "interleave(&mut ops, seed);");

        assert!(
            before.contains("for t in &handoff.targets {"),
            "the shuffle must come after every target (this run's own contracts AND the \
             retention windows, see `get_targets`) is added, or the oldest contracts are back at \
             the end of every run — the confound this exists to remove"
        );
        assert!(
            after.contains("for op in &ops {"),
            "the GETs must be issued after the shuffle, or it changed nothing"
        );
    }

    /// The PUT phase has the same confound on the size axis and needs the same
    /// call-site pin, for the same reason: shuffling a synthetic vector in a
    /// unit test says nothing about what `run` actually issues.
    #[test]
    fn the_run_shuffles_the_puts_so_the_large_contract_is_not_always_last() {
        let body = fn_body(include_str!("put_get.rs"), "async fn put_phase(");
        let (before, after) = split_at_call(body, "interleave(&mut put_order, seed);");

        assert!(
            before.contains("let mut put_order: Vec<&contracts::RunContract> ="),
            "the shuffle must come after the PUT order is collected"
        );
        assert!(
            after.contains("for c in put_order {"),
            "the PUTs must be issued from the shuffled order, or it changed nothing — \
             `build_run_contracts` appends `large-1MB` last, so vector order puts the only \
             large contract at the end of every run and confounds size with position"
        );
    }

    #[test]
    fn the_put_order_is_not_always_the_build_order() {
        // The property the call-site pin cannot check: that the shuffle
        // actually moves `large-1MB` off the end across runs.
        let last: HashSet<&str> = (0..20)
            .map(|i| {
                let mut order = vec!["small-0", "small-1", "small-2", "small-3", "large-1MB"];
                interleave(&mut order, interleave_seed(&format!("2026081{i}-051006")));
                *order.last().expect("non-empty")
            })
            .collect();
        assert!(
            last.len() > 1,
            "the last PUT of a run was always {last:?}; size is still confounded with position"
        );
    }

    fn two_contracts(run_id: &str) -> Vec<contracts::RunContract> {
        contracts::build_run_contracts(b"not wasm, only hashed", run_id, 1).expect("contracts")
    }

    fn retention_run(run_id: &str, labels: &[&str]) -> RunRecord {
        RunRecord {
            run_id: run_id.to_string(),
            timestamp: 0,
            contracts: two_contracts(run_id)
                .iter()
                .zip(labels)
                .map(|(c, label)| ContractRecord {
                    id: *c.contract.key().id(),
                    label: label.to_string(),
                    size: c.state.as_ref().len(),
                    state_hash: blake3::hash(c.state.as_ref()).to_hex().to_string(),
                })
                .collect(),
        }
    }

    #[test]
    fn the_handoff_names_this_runs_contracts_and_every_retention_window() {
        // The GET host has no manifest: whatever the handoff omits is never
        // fetched, and a missing retention window would look like a quiet night.
        let own = two_contracts("20261026-040000");
        let put_ok: Vec<&contracts::RunContract> = own.iter().collect();
        let d1 = retention_run("20261025-040000", &["small-0", "large-1MB"]);
        let d7 = retention_run("20261019-040000", &["small-0"]);
        let targets = get_targets(&put_ok, &[("24h", &d1), ("7d", &d7)]);

        let got: Vec<(&str, &str)> = targets
            .iter()
            .map(|t| (t.age.as_str(), t.label.as_str()))
            .collect();
        assert_eq!(
            got,
            vec![
                ("0h", "small-0"),
                ("0h", "large-1MB"),
                ("24h", "20261025-040000/small-0"),
                ("24h", "20261025-040000/large-1MB"),
                ("7d", "20261019-040000/small-0"),
            ]
        );
        assert_eq!(targets[0].id, *own[0].contract.key().id());
        assert_eq!(
            targets[1].state_hash,
            blake3::hash(own[1].state.as_ref()).to_hex().to_string(),
            "a 0h target must carry the hash of the state that was PUT"
        );
        assert_eq!(targets[4].state_hash, d7.contracts[0].state_hash);
    }

    #[test]
    fn a_get_passes_only_with_the_requested_contract_and_the_put_state() {
        let own = two_contracts("20261026-040000");
        let put_ok: Vec<&contracts::RunContract> = own.iter().collect();
        let targets = get_targets(&put_ok, &[]);
        let (small, large) = (&own[0], &own[1]);

        verify_get(&targets[0], &small.contract, &small.state).expect("the PUT itself verifies");
        let wrong_state = verify_get(&targets[0], &small.contract, &large.state)
            .expect_err("another state must not verify");
        assert!(
            wrong_state.to_string().contains("state hash mismatch"),
            "{wrong_state}"
        );
        let wrong_contract = verify_get(&targets[0], &large.contract, &small.state)
            .expect_err("another contract must not verify, even with the right state");
        assert!(
            wrong_contract.to_string().contains("not the requested"),
            "{wrong_contract}"
        );
    }

    #[test]
    fn a_get_is_checked_against_the_code_that_came_back_not_the_key_it_claims() {
        // The key travels in the response as a plain field. Another
        // contract's code under the requested contract's key must not pass.
        let own = two_contracts("20261026-040000");
        let put_ok: Vec<&contracts::RunContract> = own.iter().collect();
        let targets = get_targets(&put_ok, &[]);
        let ContractContainer::Wasm(ContractWasmAPIVersion::V1(small)) = &own[0].contract else {
            panic!("test contracts are V1 WASM")
        };
        let ContractContainer::Wasm(ContractWasmAPIVersion::V1(large)) = &own[1].contract else {
            panic!("test contracts are V1 WASM")
        };
        // `WrappedContract` is non-exhaustive, so the impostor is built the
        // way one would arrive: decoded from the wire, with the large
        // contract's code and parameters and the small contract's key. In
        // bincode the key is the struct's last field, so swapping the trailing
        // bytes swaps the key and nothing else.
        let mut wire = bincode::serialize(large).expect("serialize");
        let large_key = bincode::serialize(large.key()).expect("serialize key");
        let small_key = bincode::serialize(small.key()).expect("serialize key");
        assert!(
            wire.ends_with(&large_key) && large_key.len() == small_key.len(),
            "the key is no longer the trailing field; rebuild the impostor another way"
        );
        let at = wire.len() - large_key.len();
        wire[at..].copy_from_slice(&small_key);
        let impostor = ContractContainer::Wasm(ContractWasmAPIVersion::V1(
            bincode::deserialize(&wire).expect("deserialize the impostor"),
        ));
        assert_eq!(
            impostor.key().id(),
            &targets[0].id,
            "the impostor claims the right key"
        );
        let err = verify_get(&targets[0], &impostor, &own[0].state)
            .expect_err("code that hashes to another contract must not verify");
        assert!(err.to_string().contains("not the requested"), "{err}");
    }

    /// A report with one successful and one failed PUT.
    fn report_with_a_failed_put() -> Report {
        let mut report = Report::default();
        for (label, ok) in [("small-0", true), ("large-1MB", false)] {
            push_put_report(
                &mut report,
                label,
                "some-key".to_string(),
                1,
                client::Attempt {
                    outcome: if ok {
                        Ok(())
                    } else {
                        Err(anyhow::anyhow!("put failed"))
                    },
                    latency: Duration::from_millis(1),
                    errors_ignored: 0,
                },
            );
        }
        report
    }

    #[test]
    fn the_handoff_survives_the_trip_between_hosts() {
        let own = two_contracts("20261026-040000");
        let put_ok: Vec<&contracts::RunContract> = own.iter().collect();
        let d1 = retention_run("20261025-040000", &["small-0"]);
        let mut handoff = handoff_from(
            "20261026-040000".to_string(),
            "ws://127.0.0.1:7509".to_string(),
            1234,
            1_792_900_000_000,
            &report_with_a_failed_put(),
            get_targets(&put_ok, &[("24h", &d1)]),
        );
        handoff.manifest_error = Some("disk full".to_string());
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("handoff.json");
        handoff.save(&path).expect("save");
        let back = Handoff::load(&path).expect("load");

        assert_eq!(back.run_id, handoff.run_id);
        assert_eq!(back.gateway_ws, handoff.gateway_ws);
        assert_eq!(back.put_duration_ms, 1234);
        assert_eq!(back.puts_finished_unix_ms, 1_792_900_000_000);
        assert_eq!(back.manifest_error.as_deref(), Some("disk full"));
        assert_eq!(back.targets, handoff.targets);
        assert_eq!(
            serde_json::to_string(&back.put_ops).expect("json"),
            serde_json::to_string(&handoff.put_ops).expect("json")
        );
    }

    #[test]
    fn a_failed_put_reaches_the_get_reports_verdict() {
        // `put` exits 0 with a failed PUT on purpose (the GETs still run), so
        // the only way that failure reaches the verdict is through the
        // handoff the PUT phase builds and the report the GET phase replays.
        let handoff = handoff_from(
            "r".to_string(),
            String::new(),
            0,
            0,
            &report_with_a_failed_put(),
            Vec::new(),
        );
        let replayed = replay_puts(&handoff);
        assert_eq!(
            replayed
                .ops()
                .iter()
                .map(|o| (o.seq, o.op.as_str(), o.label.as_str(), o.ok))
                .collect::<Vec<_>>(),
            vec![(0, "put", "small-0", true), (1, "put", "large-1MB", false)]
        );
        assert!(!replayed.all_ok(), "a failed PUT must fail the run");

        // Both ends of that path, at the call sites: the PUT phase builds its
        // handoff with `handoff_from` from the report it recorded into, and
        // `get` starts from the replayed report.
        let src = include_str!("put_get.rs");
        let put = fn_body(src, "async fn put_phase(");
        let (_, after) = split_at_call(put, "let handoff = handoff_from(");
        assert!(
            after.contains("report,"),
            "the PUT phase's handoff must be built from the report its PUTs were recorded in"
        );
        let get = fn_body(src, "pub async fn run_get(");
        let (_, after) = split_at_call(get, "let report = replay_puts(&handoff);");
        assert!(
            after.contains("report,"),
            "the replayed report must be the one the GET phase records into"
        );
    }

    #[test]
    fn retention_state_is_written_on_the_put_host_after_the_handoff() {
        // The manifest lives on the PUT host (the GET host is a fresh runner
        // every night), and the handoff goes first: a manifest that cannot be
        // written must not cost the night its GETs.
        let src = include_str!("put_get.rs");
        let put = fn_body(src, "pub async fn run_put(");
        let (before, after) = split_at_call(put, "put.handoff.save(&args.handoff)?;");
        assert!(
            after.contains("put.record(&args.put.manifest)") && !before.contains(".record("),
            "`put` must record the run in the manifest, and only after writing the handoff"
        );
        let get = fn_body(src, "async fn get_phase(");
        assert!(
            !get.contains("manifest"),
            "the GET phase must not touch the manifest"
        );
        // A manifest error carried in the handoff must fail the GET side, after
        // the GETs have run and been reported.
        let finish = fn_body(src, "async fn finish_get(");
        let (_, after) = split_at_call(finish, "let all_ok = get_phase(");
        assert!(
            after.contains("if let Some(e) = &handoff.manifest_error {"),
            "a manifest error must fail the run once the GETs are done"
        );
    }

    #[test]
    fn a_manifest_that_cannot_be_written_is_noted_in_the_handoff() {
        let own = two_contracts("20261026-040000");
        let put_ok: Vec<&contracts::RunContract> = own.iter().collect();
        let mut put = PutOutcome {
            handoff: handoff_from(
                "20261026-040000".to_string(),
                String::new(),
                0,
                0,
                &Report::default(),
                Vec::new(),
            ),
            manifest: Manifest::default(),
            record: run_record("20261026-040000".to_string(), 1_800_000_000, &put_ok),
            now: 1_800_000_000,
        };
        let dir = tempfile::tempdir().expect("tempdir");
        let unwritable = dir.path().join("no-such-dir").join("manifest.json");
        put.record(&unwritable)
            .expect_err("a manifest in a missing directory cannot be written");
        assert!(
            put.handoff
                .manifest_error
                .as_deref()
                .is_some_and(|e| e.contains("manifest.json")),
            "the error must travel in the handoff: {:?}",
            put.handoff.manifest_error
        );

        let ok = dir.path().join("manifest.json");
        let mut put = PutOutcome {
            record: run_record("20261026-040000".to_string(), 1_800_000_000, &put_ok),
            manifest: Manifest::default(),
            ..put
        };
        put.handoff.manifest_error = None;
        put.record(&ok).expect("a writable manifest records");
        assert!(put.handoff.manifest_error.is_none());
        assert_eq!(Manifest::load(&ok).expect("load").runs.len(), 1);
    }

    #[test]
    fn a_successful_put_phase_prints_its_run_line() {
        // The `put` report is published when the GET side leaves none. Without
        // a run line, emit files it under run id "unknown". The line goes out
        // before the handoff is saved, so a failed save still leaves one.
        // `split_at_call` panics unless the call is at statement position.
        let put = fn_body(include_str!("put_get.rs"), "pub async fn run_put(");
        let (before, _) = split_at_call(put, "put.handoff.save(&args.handoff)?;");
        split_at_call(
            before,
            "print_put_run_line(run_id, &args.put, put.handoff.put_duration_ms);",
        );
    }

    #[test]
    fn put_records_carry_the_host_they_ran_on() {
        let mut ops = report_with_a_failed_put().ops().to_vec();
        stamp_vantage(&mut ops, "nova");
        assert!(ops.iter().all(|o| o.vantage.as_deref() == Some("nova")));

        let put = fn_body(include_str!("put_get.rs"), "pub async fn run_put(");
        let (before, after) =
            split_at_call(put, "stamp_vantage(&mut put.handoff.put_ops, vantage);");
        assert!(
            after.contains("put.handoff.save(&args.handoff)?;") && !before.contains(".save("),
            "the PUT records must be stamped before the handoff is first written"
        );
    }
}
