//! Shadow mode: the peer checks contracts it has sampled, and records what it
//! *would* remove.
//!
//! Everything else in this module tree is reachable only from `fdev` and from tests.
//! `decide` has been fully specified and tested since #5344 and has never once been
//! called by a running node, which means the mechanism the RFC describes does not yet
//! exist on the network in any mode — not even the mode that does nothing. This is
//! the piece that makes it exist: a bounded, opt-in loop that selects focus
//! contracts, replays the samples already being collected, and writes down the
//! verdict without acting on it.
//!
//! # What this deliberately does NOT do
//!
//! No removal, in any configuration reachable from here. No evidence propagation, no
//! author diagnostics, no network traffic of any kind. Those are the RFC's Phase 4;
//! this is Phase 3, "single/live peer shadow validation", whose whole job is to prove
//! that focus selection, sampling and local discovery behave on a real peer before
//! anything is shipped fleet-wide.
//!
//! It also generates no synchronization traffic to manufacture samples. It reads what
//! capture has already observed from ordinary traffic, which is the RFC's explicit
//! constraint: the peer is an observer, not a fuzzer.
//!
//! # Why probes run on a throwaway runtime
//!
//! Each probe builds a [`RuntimeOracle::standalone`], which is its own wasmtime
//! instance over its own scratch store, rather than borrowing the node's executor.
//! That costs a temp directory per probe and is worth it twice over: a probe cannot
//! touch hosted state, and a contract that misbehaves under probing cannot affect the
//! runtime serving real requests. The RFC's hard requirement is that conformance work
//! never delays normal contract synchronization; the cheapest way to honour that is
//! for the probe to share nothing with the path that does the synchronizing.

use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::time::Duration;

use freenet_stdlib::prelude::ContractInstanceId;
// tokio's clock rather than `std`: it honours `start_paused`, so a test can prove the
// time budget cuts a probe short without actually spending ten seconds doing it.
use tokio::time::Instant;

use super::bundle::ReplayBundle;
use super::focus::FocusSelector;
use super::generator::{GeneratorConfig, generate_cases};
use super::policy::{ConformanceAction, EnforcementMode, decide};
use super::property::{ConformanceProperty, PropertyOutcome};
use super::runtime_oracle::RuntimeOracle;
use super::verifier::verify_case;

/// How many contracts a peer watches at once.
///
/// Small on purpose. Watching more is not free — each focus contract is a probe's
/// worth of WASM execution per tick — and it buys less than it looks like it should,
/// because coverage across the network comes from every peer watching a *different*
/// couple of contracts, not from any one peer watching many. Two is the RFC's
/// "simple independent randomized policy" read literally.
const MAX_FOCUS_CONTRACTS: usize = 2;

/// How often a probe runs.
pub const PROBE_INTERVAL: Duration = Duration::from_secs(15 * 60);

/// Ticks before the focus set is reshuffled.
///
/// Rotation is what stops a contract that happens to be unlucky today from being the
/// only thing this peer ever looks at, and what stops an author who somehow learns
/// they are being watched from staying watched. Eight ticks at fifteen minutes is two
/// hours, long enough for a sampler to accumulate something worth checking and short
/// enough that a day covers many contracts.
const ROTATE_EVERY_TICKS: u32 = 8;

/// Cases checked per contract per tick.
///
/// The bound that makes "conformance never delays synchronization" true rather than
/// hoped for. Each case is a handful of WASM calls under the runtime's own fuel
/// limit, so this is a bounded number of bounded operations, and the probe yields
/// between cases.
const MAX_CASES_PER_PROBE: usize = 64;

/// Wall-clock ceiling for one contract's probe, checked between cases.
///
/// Belt to `MAX_CASES_PER_PROBE`'s braces: the case count bounds the work only if
/// each case is quick, and "quick" depends on a contract we do not control. A
/// contract that is merely slow rather than infinite would otherwise stretch a tick.
///
/// # What this does NOT bound, and why that is acceptable
///
/// It is checked BETWEEN cases, so a single long case overshoots it and is only
/// recorded as `timed_out` once it finishes. The overshoot is bounded but not small:
/// each WASM call traps at the runtime's epoch deadline (`max_execution_seconds`,
/// 5s by default), and a reconciliation case drives up to `MAX_RECONCILIATION_ROUNDS`
/// rounds of them, so a deliberately-slow contract can hold one case for far longer
/// than ten seconds. It cannot run unbounded.
///
/// Two reasons this is left as it is rather than tightened. The probe runs on a
/// blocking thread, so an overshoot costs CPU rather than event-loop latency, which is
/// the property that actually matters. And giving probes a shorter per-call deadline
/// than production would break the RFC's requirement that the local harness use the
/// production runtime configuration — a probe would then report resource exhaustion
/// where `fdev` reports a verdict, and the two would stop meaning the same thing.
const PROBE_TIME_BUDGET: Duration = Duration::from_secs(10);

/// Filename of the per-peer focus salt inside the capture directory.
const SALT_FILE: &str = "focus-salt";

/// Filename of the persisted focus epoch, beside the salt.
const EPOCH_FILE: &str = "focus-epoch";

/// What one tick did. Counters only — never contract state, never evidence bytes.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct ShadowReport {
    /// Contracts in focus this tick.
    pub focused: usize,
    /// Contracts probed, i.e. focus contracts whose code and samples were both
    /// usable. The gap between this and `focused` is the reach problem, and it is
    /// reported rather than inferred: a focus contract whose WASM is missing looks
    /// exactly like a clean one if you only count findings.
    pub probed: usize,
    /// Focus contracts skipped because their code could not be resolved.
    pub skipped_no_code: usize,
    /// Focus contracts skipped because the sampler held nothing worth checking.
    pub skipped_no_samples: usize,
    /// Cases actually verified.
    pub cases: usize,
    /// Outcomes that reached no verdict.
    pub inconclusive: usize,
    /// Violations that would have caused a removal, had enforcement been on. The
    /// number the RFC's Phase 5 gate is judged on.
    pub would_remove: usize,
    /// Diagnostic-severity findings, which never propose removal in any mode.
    pub reported: usize,
    /// Probes cut short by [`PROBE_TIME_BUDGET`].
    pub timed_out: usize,
}

/// The shadow loop's state, owned by the capture writer task.
pub struct ShadowRunner {
    mode: EnforcementMode,
    selector: FocusSelector,
    ticks: u32,
    /// Capture directory, so a rotation can persist the new epoch beside the salt.
    dir: PathBuf,
    /// `(contract, property)` pairs already logged this epoch, so a contract that
    /// fails the same law in forty cases produces one line rather than forty. Cleared
    /// on rotation, which is also what makes the log a per-epoch summary rather than
    /// an ever-growing set.
    logged: HashSet<(ContractInstanceId, ConformanceProperty)>,
}

impl ShadowRunner {
    /// Build a runner, loading or creating this peer's focus salt under `dir`.
    ///
    /// The salt is per-peer and secret. If it were derived from anything public — the
    /// peer's identity, its address, the contract set — an author could compute which
    /// peers are watching their contract, and either target those peers with
    /// well-behaved traffic or simply avoid them. It is therefore random, persisted,
    /// and readable only by the node's own user.
    pub fn new(dir: &Path, mode: EnforcementMode) -> Self {
        let salt = load_or_create_salt(dir);
        let epoch = load_epoch(dir);
        Self {
            mode,
            selector: FocusSelector::resuming_at(salt, MAX_FOCUS_CONTRACTS, epoch),
            ticks: 0,
            dir: dir.to_path_buf(),
            logged: HashSet::new(),
        }
    }

    pub fn epoch(&self) -> u64 {
        self.selector.epoch()
    }

    /// Run one probe tick over whatever the sampler currently holds.
    /// Choose this tick's focus contracts and copy out what a probe needs.
    ///
    /// Split from [`probe`] deliberately, and the split is load-bearing rather than
    /// tidiness. This half borrows the sampler map, so it has to run on the capture
    /// task, and it is cheap: a hash per candidate plus a bounded copy of the selected
    /// contracts' samples. The probe half is CPU-bound WASM execution under a
    /// ten-second-per-contract budget, and running THAT on the capture task would stop
    /// the task draining its observation queue for as long as it ran — dropping the
    /// very observations conformance exists to collect, from a mechanism whose stated
    /// requirement is that it never degrade normal operation. The periodic flush was
    /// made async for exactly this reason; a probe is the worse case, because it is
    /// CPU-bound and cannot be yielded away from by awaiting a syscall.
    pub(crate) fn select(
        &mut self,
        samplers: &HashMap<ContractInstanceId, super::capture::TrackedContract>,
    ) -> Vec<(ContractInstanceId, ReplayBundle)> {
        if self.mode == EnforcementMode::Disabled {
            return Vec::new();
        }

        self.ticks = self.ticks.wrapping_add(1);
        if self.ticks % ROTATE_EVERY_TICKS == 0 {
            self.selector.rotate();
            self.logged.clear();
            store_epoch(&self.dir, self.selector.epoch());
        }

        let candidates: Vec<ContractInstanceId> = samplers.keys().copied().collect();
        self.selector
            .select(&candidates)
            .into_iter()
            .filter_map(|instance| {
                let tracked = samplers.get(&instance)?;
                Some((instance, super::capture::bundle_for(instance, tracked)))
            })
            .collect()
    }

    pub(crate) fn mode(&self) -> EnforcementMode {
        self.mode
    }

    /// Fold a completed probe's findings into this epoch's log.
    ///
    /// Kept here rather than in [`probe`] because the dedup state is scoped to the
    /// epoch, which lives here, and because the probe runs on another task and must
    /// not need `&mut self` to do its work.
    pub(crate) fn record(&mut self, findings: &[Finding]) {
        for finding in findings {
            // One line per (contract, property) per epoch: a contract that fails the
            // same law in forty cases is one finding reported forty times, and a log
            // that says so forty times is harder to read, not more informative.
            if !self
                .logged
                .insert((finding.contract, finding.violation.property))
            {
                continue;
            }
            tracing::info!(
                contract = %finding.contract,
                property = ?finding.violation.property,
                epoch = self.selector.epoch(),
                would_remove = finding.would_remove,
                detail = %finding.violation.detail,
                "conformance shadow finding"
            );
        }
    }
}

/// One thing a probe found, carried back to the capture task to be logged.
#[derive(Debug, Clone)]
pub(crate) struct Finding {
    pub contract: ContractInstanceId,
    pub violation: super::property::Violation,
    pub would_remove: bool,
}

/// Run one probe over bundles already copied out of the sampler.
///
/// A free function over owned inputs, so the capture task can spawn it and carry on
/// draining its observation queue while it runs. Nothing here borrows the sampler map,
/// which is also what makes it structurally impossible for a probe to change what the
/// peer would have captured.
pub(crate) async fn probe(
    work: Vec<(ContractInstanceId, ReplayBundle)>,
    store: Option<PathBuf>,
    mode: EnforcementMode,
) -> (ShadowReport, Vec<Finding>) {
    probe_with_budget(work, store, mode, PROBE_TIME_BUDGET).await
}

/// [`probe`] with an explicit time budget.
///
/// Exists because the budget branch is otherwise unreachable from a test: it is
/// checked between cases against a real elapsed clock, and no fixture contract is
/// slow enough to trip a ten-second ceiling without making the suite take ten
/// seconds. Injecting it lets a test prove the check fires and stops the probe,
/// which is the part that can regress; the constant itself is a tuning choice.
pub(crate) async fn probe_with_budget(
    work: Vec<(ContractInstanceId, ReplayBundle)>,
    store: Option<PathBuf>,
    mode: EnforcementMode,
    budget: Duration,
) -> (ShadowReport, Vec<Finding>) {
    let mut report = ShadowReport {
        focused: work.len(),
        ..ShadowReport::default()
    };
    let mut findings = Vec::new();

    let Some(store) = store else {
        // No store means no code, and no code means no question was asked. Recorded as
        // a skip rather than returned as an empty report, which would read as a clean
        // run for contracts nobody could execute.
        report.skipped_no_code = work.len();
        return (report, findings);
    };

    for (instance, bundle) in work {
        probe_one(
            &instance,
            &bundle,
            &store,
            mode,
            budget,
            &mut report,
            &mut findings,
        )
        .await;
    }
    (report, findings)
}

#[allow(clippy::too_many_arguments)]
async fn probe_one(
    instance: &ContractInstanceId,
    bundle: &ReplayBundle,
    store: &Path,
    mode: EnforcementMode,
    budget: Duration,
    report: &mut ShadowReport,
    findings: &mut Vec<Finding>,
) {
    let corpus = bundle.to_corpus();
    if corpus.is_empty() {
        report.skipped_no_samples += 1;
        return;
    }

    let code = match bundle.resolve_code_from_store(store) {
        Ok(code) => code,
        Err(err) => {
            // Not a finding: this peer failed to ask the question. Counted as such
            // rather than passed off as a clean result, because a focus contract we
            // cannot execute looks exactly like a conforming one if you only count
            // violations.
            tracing::debug!(%instance, error = %err, "shadow probe has no code for a focus contract");
            report.skipped_no_code += 1;
            return;
        }
    };

    let mut oracle = match RuntimeOracle::standalone(code, bundle.parameters.clone()).await {
        Ok(oracle) => oracle,
        Err(err) => {
            // A contract whose WASM will not load is not a conformance finding either.
            tracing::debug!(%instance, error = %err, "shadow probe could not build an oracle");
            report.skipped_no_code += 1;
            return;
        }
    };

    let config = GeneratorConfig {
        max_cases: MAX_CASES_PER_PROBE,
        ..Default::default()
    };
    let cases = generate_cases(&corpus, &config);
    report.probed += 1;

    // The WASM runs here, on a blocking thread.
    //
    // `.claude/rules/contracts.md` forbids executing contracts on the main async
    // runtime, and `verify_case` drives synchronous WASM calls. Moving the probe to
    // another TASK would not help: tasks share the worker threads, and a node sized by
    // `available_parallelism()` has exactly one on a single-vCPU host, a supported
    // deployment. Only the CPU-bound loop moves — building the oracle stays async on
    // the runtime — so nothing here drives a future from a blocking thread.
    let cases: Vec<_> = cases.into_iter().take(MAX_CASES_PER_PROBE).collect();
    let instance_copy = *instance;
    let joined = tokio::task::spawn_blocking(move || {
        let mut out = ProbeOutcome::default();
        let started = Instant::now();
        for case in &cases {
            if started.elapsed() >= budget {
                out.timed_out = true;
                break;
            }
            let outcome = verify_case(&mut oracle, case);
            out.cases += 1;
            out.decisions.push(decide(mode, &outcome));
            out.inconclusive += usize::from(matches!(outcome, PropertyOutcome::Inconclusive(_)));
        }
        out
    })
    .await;

    let outcome = match joined {
        Ok(outcome) => outcome,
        Err(err) => {
            // A probe that died is not a finding about the contract.
            tracing::warn!(%instance, error = %err, "shadow probe thread failed");
            return;
        }
    };

    report.cases += outcome.cases;
    report.inconclusive += outcome.inconclusive;
    if outcome.timed_out {
        report.timed_out += 1;
    }

    for action in outcome.decisions {
        match action {
            ConformanceAction::Nothing => {}
            ConformanceAction::Report(violation) => {
                report.reported += 1;
                findings.push(Finding {
                    contract: instance_copy,
                    violation,
                    would_remove: false,
                });
            }
            ConformanceAction::WouldRemove(violation) => {
                report.would_remove += 1;
                findings.push(Finding {
                    contract: instance_copy,
                    violation,
                    would_remove: true,
                });
            }
            // Unreachable while nothing constructs `EnforcementMode::Enforce`, and
            // deliberately still handled: the point of routing shadow through the same
            // `decide` enforcement will use is that the two cannot drift. Counting a
            // removal as a would-remove keeps this honest if a future caller reaches
            // it before the removal path exists.
            ConformanceAction::Remove(violation) => {
                report.would_remove += 1;
                findings.push(Finding {
                    contract: instance_copy,
                    violation,
                    would_remove: true,
                });
            }
        }
    }
}

/// What one contract's blocking probe produced, carried back to the async side.
///
/// Decisions rather than outcomes: `PropertyOutcome` owns a `Violation`, and moving
/// the already-made decision keeps the blocking thread from needing anything about how
/// findings are recorded.
#[derive(Default)]
struct ProbeOutcome {
    cases: usize,
    inconclusive: usize,
    timed_out: bool,
    decisions: Vec<ConformanceAction>,
}

/// Read this peer's focus salt, creating one if it does not exist.
///
/// A failure to persist is not fatal: the peer runs with an ephemeral salt and
/// reshuffles its focus set on restart. That is worse than persisting — a peer that
/// re-rolls on every restart gives an author a way to keep re-rolling by causing
/// restarts — but it is much better than refusing to run, and it is logged.
fn load_or_create_salt(dir: &Path) -> [u8; 32] {
    let path = dir.join(SALT_FILE);
    if let Ok(bytes) = std::fs::read(&path) {
        if let Ok(salt) = <[u8; 32]>::try_from(bytes.as_slice()) {
            // Tighten an existing salt that is readable beyond its owner. A file
            // written by an older build, or copied about, keeps whatever mode it
            // arrived with, and a secret that is only protected at creation time is
            // only protected on the run that created it.
            #[cfg(unix)]
            {
                use std::os::unix::fs::PermissionsExt;
                if let Ok(meta) = std::fs::metadata(&path) {
                    if meta.permissions().mode() & 0o077 != 0 {
                        let _ =
                            std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o600));
                    }
                }
            }
            return salt;
        }
        tracing::warn!(
            path = %path.display(),
            "focus salt file is not 32 bytes; generating a new one"
        );
    }

    // OsRng, not `GlobalRng`, and this is the crypto-material exception in
    // `.claude/rules/code-style.md` rather than an oversight. The salt's entire job is
    // to be unpredictable to a contract author: anyone who can guess it can compute
    // which peers are watching their contract and either behave for those peers or
    // avoid them. `GlobalRng` is seeded for determinism, and a simulation seed is
    // exactly the kind of thing that leaks into a bug report.
    // Same `OsRng` the node uses for its own cipher key (`config::secret`), so this
    // does not introduce a second opinion about where entropy comes from.
    let mut salt = [0u8; 32];
    chacha20poly1305::aead::rand_core::RngCore::fill_bytes(
        &mut chacha20poly1305::aead::OsRng,
        &mut salt,
    );
    if let Err(err) = write_salt(&path, &salt) {
        tracing::warn!(
            path = %path.display(),
            error = %err,
            "could not persist the conformance focus salt; focus will reshuffle on restart"
        );
    }
    salt
}

fn write_salt(path: &Path, salt: &[u8; 32]) -> std::io::Result<()> {
    // Created owner-only, rather than written and then chmod-ed. The salt is what
    // stops a contract author predicting or influencing whether this peer is watching
    // them, and write-then-chmod leaves a window in which it is not yet protected.
    // The node tightens its umask at startup so that window is closed in practice
    // today, but this reuses the helper the project already built for secrets rather
    // than depending on a mitigation that lives somewhere else and could be narrowed.
    use std::io::Write;
    let mut file = crate::wasm_runtime::create_owner_only(path)?;
    file.write_all(salt)?;
    file.sync_all()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::conformance::capture::{Observation, TrackedContract, record};

    /// Drive one whole tick the way the capture task does: select on the writer
    /// side, probe on the other, fold the findings back in. Tests go through this
    /// rather than calling the halves directly, so they exercise the same sequence
    /// production uses and a change to that sequence cannot leave them passing.
    async fn tick(
        runner: &mut ShadowRunner,
        samplers: &HashMap<ContractInstanceId, TrackedContract>,
        store: Option<&Path>,
    ) -> ShadowReport {
        let work = runner.select(samplers);
        let (report, findings) = probe(work, store.map(|p| p.to_path_buf()), runner.mode()).await;
        runner.record(&findings);
        report
    }

    fn instance(n: u8) -> ContractInstanceId {
        ContractInstanceId::new([n; 32])
    }

    /// Build a sampler map by pushing observations through the real admission path,
    /// so a test never asserts against a `TrackedContract` the capture path could not
    /// actually have produced.
    fn samplers_for(
        id: ContractInstanceId,
        parameters: Vec<u8>,
        code_hash: [u8; 32],
        transitions: &[(Vec<u8>, Vec<u8>)],
    ) -> HashMap<ContractInstanceId, TrackedContract> {
        let mut samplers = HashMap::new();
        for (base, result) in transitions {
            record(
                &mut samplers,
                Observation {
                    contract: id,
                    code_hash,
                    parameters: parameters.clone(),
                    base_state: base.clone(),
                    incoming_state: Some(result.clone()),
                    delta: None,
                    result_state: result.clone(),
                    related: Vec::new(),
                },
            );
        }
        samplers
    }

    /// The salt must survive a restart, or an author gets a fresh roll of the dice
    /// every time the node bounces — which is a lever they can pull by any means that
    /// causes a restart.
    #[test]
    fn the_focus_salt_persists_across_runners() {
        let dir = tempfile::TempDir::new().expect("tempdir");
        let first = load_or_create_salt(dir.path());
        let second = load_or_create_salt(dir.path());
        assert_eq!(
            first, second,
            "a second runner in the same directory drew a different salt, so focus \
             reshuffles on every restart"
        );
    }

    /// The salt is the whole secret. World-readable on a shared host would hand an
    /// author the answer to "is this peer watching me".
    #[cfg(unix)]
    #[test]
    fn the_focus_salt_is_not_world_readable() {
        use std::os::unix::fs::PermissionsExt;
        let dir = tempfile::TempDir::new().expect("tempdir");
        let _ = load_or_create_salt(dir.path());
        let mode = std::fs::metadata(dir.path().join(SALT_FILE))
            .expect("salt file")
            .permissions()
            .mode()
            & 0o777;
        assert_eq!(
            mode, 0o600,
            "the focus salt is readable beyond its owner (mode {mode:o})"
        );
    }

    /// Two peers must not agree on who to watch, or the network watches one small
    /// set of contracts many times over and everything else never.
    #[test]
    fn two_peers_draw_different_salts() {
        let a = tempfile::TempDir::new().expect("tempdir");
        let b = tempfile::TempDir::new().expect("tempdir");
        assert_ne!(
            load_or_create_salt(a.path()),
            load_or_create_salt(b.path()),
            "two peers drew the same salt, so focus selection is not per-peer"
        );
    }

    /// A truncated or hand-edited salt file must not be adopted as a 32-byte salt.
    #[test]
    fn a_short_salt_file_is_replaced_rather_than_used() {
        let dir = tempfile::TempDir::new().expect("tempdir");
        std::fs::write(dir.path().join(SALT_FILE), [1u8, 2, 3]).expect("write short salt");
        let salt = load_or_create_salt(dir.path());
        assert_ne!(salt, [0u8; 32], "a replacement salt must not be all zeroes");
        assert_eq!(
            salt,
            load_or_create_salt(dir.path()),
            "the replacement salt was not persisted"
        );
    }

    /// Without a contract store there is no code, and without code no question was
    /// asked. That must show up as a skip, never as an empty report: a shadow period
    /// that could not execute anything and one that found nothing are the same shape
    /// in a findings-only log, and telling them apart is most of what this phase is
    /// for.
    #[tokio::test]
    async fn no_contract_store_reports_skips_rather_than_a_clean_run() {
        let dir = tempfile::TempDir::new().expect("tempdir");
        let mut runner = ShadowRunner::new(dir.path(), EnforcementMode::Shadow);
        let samplers = samplers_for(
            instance(1),
            vec![0],
            [0u8; 32],
            &[(vec![1], vec![1, 2]), (vec![2], vec![2, 3])],
        );

        let report = tick(&mut runner, &samplers, None).await;
        assert!(
            report.focused > 0,
            "nothing was selected, so this proves nothing"
        );
        assert_eq!(
            report.skipped_no_code, report.focused,
            "focus contracts with no resolvable code were not counted as skipped"
        );
        assert_eq!(
            report.cases, 0,
            "cases were checked without any contract code"
        );
        assert_eq!(report.would_remove, 0);
    }

    /// `Disabled` means nothing runs at all — not "runs but reports nothing".
    #[tokio::test]
    async fn disabled_does_no_work_whatsoever() {
        let dir = tempfile::TempDir::new().expect("tempdir");
        let mut runner = ShadowRunner::new(dir.path(), EnforcementMode::Disabled);
        let samplers = samplers_for(instance(1), vec![0], [0u8; 32], &[(vec![1], vec![1, 2])]);
        assert_eq!(
            tick(&mut runner, &samplers, None).await,
            ShadowReport::default(),
            "Disabled mode still selected or skipped contracts"
        );
    }

    /// Rotation is what stops one unlucky contract being the only thing this peer
    /// ever looks at. Asserting the epoch advances on the right tick, and not before.
    #[tokio::test]
    async fn focus_rotates_on_schedule_and_not_sooner() {
        let dir = tempfile::TempDir::new().expect("tempdir");
        let mut runner = ShadowRunner::new(dir.path(), EnforcementMode::Shadow);
        let samplers = samplers_for(instance(1), vec![0], [0u8; 32], &[(vec![1], vec![1, 2])]);

        for nth in 1..ROTATE_EVERY_TICKS {
            tick(&mut runner, &samplers, None).await;
            assert_eq!(
                runner.epoch(),
                0,
                "focus rotated early, on tick {nth} of {ROTATE_EVERY_TICKS}"
            );
        }
        tick(&mut runner, &samplers, None).await;
        assert_eq!(
            runner.epoch(),
            1,
            "focus did not rotate after {ROTATE_EVERY_TICKS} ticks"
        );
    }

    /// The time budget must actually stop a probe, not merely be declared.
    ///
    /// This module's own doc says tokio's clock was chosen so a test could prove the
    /// budget cuts a probe short — and until review pointed it out, no such test
    /// existed. A zero budget makes the check fire before the first case, which is the
    /// branch that can regress; the ten-second value itself is a tuning choice, not a
    /// behaviour.
    #[tokio::test(flavor = "multi_thread")]
    async fn the_time_budget_stops_a_probe() -> Result<(), Box<dyn std::error::Error>> {
        use freenet_stdlib::prelude::{
            ContractCode, ContractContainer, ContractWasmAPIVersion, Parameters, WrappedContract,
        };
        use std::sync::Arc;

        let wasm = crate::wasm_runtime::tests::get_test_module("test_contract_conformance")?;
        let code_hash = *blake3::hash(&wasm).as_bytes();
        let store_dir = crate::util::tests::get_temp_dir();
        std::fs::create_dir_all(store_dir.path())?;
        let db = crate::contract::storages::Storage::new(store_dir.path()).await?;
        let mut store =
            crate::wasm_runtime::ContractStore::new(store_dir.path().into(), 10_000_000, db)?;

        let parameters: Parameters<'static> = Parameters::from(vec![0u8]);
        let contract = WrappedContract::new(Arc::new(ContractCode::from(wasm)), parameters.clone());
        let id = *contract.key().id();
        store.store_contract(ContractContainer::Wasm(ContractWasmAPIVersion::V1(
            contract,
        )))?;

        let samplers = samplers_for(
            id,
            vec![0],
            code_hash,
            &[(vec![1], vec![1, 2]), (vec![2], vec![2, 3])],
        );
        let dir = tempfile::TempDir::new()?;
        let mut runner = ShadowRunner::new(dir.path(), EnforcementMode::Shadow);
        let work = runner.select(&samplers);
        assert!(!work.is_empty(), "nothing selected, so this proves nothing");

        let (report, _) = probe_with_budget(
            work,
            Some(store_dir.path().to_path_buf()),
            EnforcementMode::Shadow,
            Duration::ZERO,
        )
        .await;

        assert_eq!(
            report.timed_out, 1,
            "an exhausted budget did not stop the probe: {report:?}"
        );
        assert_eq!(
            report.cases, 0,
            "cases ran after the budget was already exhausted: {report:?}"
        );
        Ok(())
    }

    /// Rotation only means anything if it accumulates across restarts.
    ///
    /// A peer that reset to epoch zero on every restart would re-select the same first
    /// focus set forever, so an author able to cause restarts could pin a peer's
    /// attention — or keep it away from themselves indefinitely.
    #[tokio::test]
    async fn the_focus_epoch_survives_a_restart() {
        let dir = tempfile::TempDir::new().expect("tempdir");
        let samplers = samplers_for(instance(1), vec![0], [0u8; 32], &[(vec![1], vec![1, 2])]);

        let mut first = ShadowRunner::new(dir.path(), EnforcementMode::Shadow);
        for _ in 0..ROTATE_EVERY_TICKS {
            tick(&mut first, &samplers, None).await;
        }
        let rotated = first.epoch();
        assert!(rotated > 0, "the first runner never rotated");
        drop(first);

        let resumed = ShadowRunner::new(dir.path(), EnforcementMode::Shadow);
        assert_eq!(
            resumed.epoch(),
            rotated,
            "a restart lost the focus epoch, so rotation restarts from zero every time \
             the node bounces"
        );
    }

    /// The end-to-end proof: a real contract with a real defect, resolved from a real
    /// contract store, driven through focus selection and the probe loop, comes out as
    /// `would_remove` — and a conforming contract through the identical path does not.
    ///
    /// Both halves are needed. Without the conforming half this test would pass for a
    /// runner that flagged everything, which is the failure mode that matters most
    /// here: the RFC's whole deployment plan turns on shadow mode producing few enough
    /// prospective removals that every one can be understood.
    #[tokio::test(flavor = "multi_thread")]
    async fn a_real_defect_reaches_would_remove_and_a_conforming_contract_does_not()
    -> Result<(), Box<dyn std::error::Error>> {
        use freenet_stdlib::prelude::{
            ContractCode, ContractContainer, ContractWasmAPIVersion, Parameters, WrappedContract,
        };
        use std::sync::Arc;

        // Mode 1 of the shared fixture is last-write-wins, which breaks commutativity.
        // Mode 0 is the honest join-semilattice. Keep in sync with
        // `tests/test-contract-conformance/src/lib.rs`.
        const LAST_WRITE_WINS: u8 = 1;
        const CONFORMING: u8 = 0;

        let wasm = crate::wasm_runtime::tests::get_test_module("test_contract_conformance")?;
        let code_hash = *blake3::hash(&wasm).as_bytes();

        // A real store, written the way the node writes it. Reading it back is the
        // step with the versioned-header trap, so the test exercises the real reader
        // rather than a hand-rolled one.
        let store_dir = crate::util::tests::get_temp_dir();
        std::fs::create_dir_all(store_dir.path())?;
        let db = crate::contract::storages::Storage::new(store_dir.path()).await?;
        let mut store =
            crate::wasm_runtime::ContractStore::new(store_dir.path().into(), 10_000_000, db)?;

        let states = vec![
            (vec![1u8], vec![1u8, 2]),
            (vec![2u8], vec![2u8, 3]),
            (vec![1u8, 2], vec![1u8, 2, 3]),
            (vec![4u8], vec![4u8, 5]),
        ];

        let mut outcomes = Vec::new();
        for mode in [LAST_WRITE_WINS, CONFORMING] {
            let parameters: Parameters<'static> = Parameters::from(vec![mode]);
            let contract = WrappedContract::new(
                Arc::new(ContractCode::from(wasm.clone())),
                parameters.clone(),
            );
            let key = *contract.key();
            let id = *key.id();
            store.store_contract(ContractContainer::Wasm(ContractWasmAPIVersion::V1(
                contract,
            )))?;

            let samplers = samplers_for(id, vec![mode], code_hash, &states);
            let before = samplers.len();

            let dir = tempfile::TempDir::new()?;
            let mut runner = ShadowRunner::new(dir.path(), EnforcementMode::Shadow);
            let report = tick(&mut runner, &samplers, Some(store_dir.path())).await;

            assert_eq!(
                samplers.len(),
                before,
                "a probe changed the sampler set; shadow mode must not alter what the \
                 peer would have captured"
            );
            assert_eq!(
                report.probed, 1,
                "mode {mode} was not probed at all (skipped_no_code={}, \
                 skipped_no_samples={}), so its verdict means nothing",
                report.skipped_no_code, report.skipped_no_samples
            );
            assert!(
                report.cases > 0,
                "mode {mode} was probed but no case ran, so nothing was checked"
            );
            outcomes.push((mode, report));
        }

        let broken = &outcomes[0].1;
        let honest = &outcomes[1].1;

        assert!(
            broken.would_remove > 0,
            "a contract that provably breaks commutativity produced no prospective \
             removal: {broken:?}"
        );
        assert_eq!(
            honest.would_remove, 0,
            "a conforming contract produced a prospective removal, which is the false \
             positive the whole deployment plan is gated on not happening: {honest:?}"
        );
        Ok(())
    }
}

/// Read the persisted focus epoch, defaulting to zero.
///
/// A missing or unreadable file is not an error: a first run has no epoch, and a
/// corrupt one is better restarted from zero than guessed at. Losing it costs
/// rotation progress, not correctness.
fn load_epoch(dir: &Path) -> u64 {
    std::fs::read_to_string(dir.join(EPOCH_FILE))
        .ok()
        .and_then(|raw| raw.trim().parse().ok())
        .unwrap_or(0)
}

/// Persist the focus epoch beside the salt.
///
/// Best-effort, like the salt: a peer that cannot write it reshuffles from zero on
/// restart, which is worse but not broken, and it is logged rather than silent.
fn store_epoch(dir: &Path, epoch: u64) {
    let path = dir.join(EPOCH_FILE);
    if let Err(err) = std::fs::write(&path, epoch.to_string()) {
        tracing::warn!(
            path = %path.display(),
            error = %err,
            "could not persist the conformance focus epoch; focus rotation will \
             restart from zero after a restart"
        );
    }
}
