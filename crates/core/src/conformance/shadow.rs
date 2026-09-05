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
    ///
    /// Read it together with `would_remove_settled`. Since #5462 this total includes
    /// contracts that DO converge — a canonicalizing contract breaks idempotence by
    /// rewriting a non-canonical stored state once, and is reported honestly rather
    /// than suppressed. Comparing this number across that change, or against a
    /// future where the PUT install path canonicalizes, compares different
    /// populations.
    pub would_remove: usize,
    /// The subset of `would_remove` whose finding says the state SETTLED.
    ///
    /// Carried as its own counter because `would_remove` is a bare `usize` with
    /// nowhere to record that a finding is benign, and this is the number Phase 5
    /// is judged on. Leaving the distinction out would pre-filter the corpus by
    /// omission — the same move as the fixpoint gate #5462 removed, one layer up:
    /// the population would look uniformly removal-worthy when a large and
    /// knowable part of it is waiting on an install-path fix instead.
    ///
    /// Deliberately additive rather than subtracted from `would_remove`: the
    /// finding IS removal-eligible, and netting it off would be the severity fork
    /// the #5462 decision rejected, hidden in a metric.
    ///
    /// Read it as a CASE count, not a contract count, and do not read the ratio
    /// against `would_remove` as "the benign fraction of contracts". Idempotence
    /// draws a fixed handful of cases per probe while a contract breaking several
    /// properties feeds the total from each, so the multiplier is not equal across
    /// the two populations and the ratio systematically misstates the fraction. It
    /// tells you the class is present and roughly how loud, which is what it is
    /// for.
    ///
    /// `Indeterminate` findings are in `would_remove` and in NEITHER subset. That
    /// is the conservative default — an unknown class must not be counted benign —
    /// but it means a residual exists that this pair of numbers cannot show, and
    /// `Indeterminate` is adversarially reachable (see its rustdoc), so a nonzero
    /// gap between the total and the sum of what you can classify is itself worth
    /// looking at.
    pub would_remove_settled: usize,
    /// Diagnostic-severity findings, which never propose removal in any mode.
    pub reported: usize,
    /// Probes cut short by [`PROBE_TIME_BUDGET`].
    pub timed_out: usize,
    /// Focus contracts that reached a verdict: at least one case came back `Holds` or
    /// `Violated`. This is deliberately narrower than `probed` — a contract can be
    /// probed, run every one of its cases, and still form no opinion (every case
    /// `Inconclusive`), and `probed` alone cannot tell that apart from a contract that
    /// was genuinely judged and found clean. `recently_checked` on the dashboard must
    /// be built from this list, not from focus selection or from `probed`: selection
    /// only says a contract was a *candidate* to look at, and `probed` only says the
    /// code and samples were usable, neither says an opinion was formed.
    pub judged: Vec<JudgedContract>,
    /// Focus contracts we formed NO opinion about: skipped before probing (no code, no
    /// samples), a probe whose blocking task died or whose time budget was exhausted
    /// before any case ran, or a probe that ran cases but every one came back
    /// `Inconclusive`. Complements `judged` over the focus set — every focus contract
    /// lands in exactly one of the two, which is what makes them checkable against
    /// each other.
    pub without_verdict: usize,
}

/// Fold the focus contracts that had no samples yet into a tick's counts.
///
/// [`ShadowRunner::select`] returns them separately from the work set, because
/// [`probe`] cannot see them at all: no case ran for them, so nothing downstream of
/// the probe knows they existed. Every one of the three counters has to be told.
/// `focused` so it is the size of the focus SET rather than of the subset that
/// happened to have samples; `skipped_no_samples` so the reason is legible; and
/// `without_verdict` because `judged`/`without_verdict` are complements over the
/// focus set, and a warming-up contract lands in neither half unless it is put in
/// one. Warm-up is the ordinary state now that focus draws from the hosted set, so
/// this is the common path, not a corner.
///
/// A shared function rather than three lines at each caller. `capture::run_writer`
/// and the `probe_fixture_contract` test seam both hand a `ShadowReport` onward —
/// the writer to `status::publish`, the seam to whatever a test does with it — and
/// while the seam open-coded nothing at all, tests fed `report.without_verdict`
/// straight into `MergeCheckStatus::record` as a tick's unjudged count, a value
/// production would never publish while any focus contract was warming up. One
/// function is the only shape in which the seam cannot drift from the writer.
pub(crate) fn count_awaiting_samples(report: &mut ShadowReport, awaiting: usize) {
    report.focused += awaiting;
    report.skipped_no_samples += awaiting;
    report.without_verdict += awaiting;
}

/// One contract that reached a verdict, and how much looking that took.
///
/// The counts travel WITH the contract rather than being summed into the tick's
/// fleet-wide `cases`/`inconclusive` totals alone. The dashboard needs to say how
/// much was established about THIS contract: a contract with one verdict and 199
/// inconclusive cases rendered byte-identically to one with 200 verdicts while the
/// page could only reach the fleet-wide number, which is the same conflation of
/// "barely looked at" with "clean" that `conformance::status` exists to stop.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct JudgedContract {
    pub contract: ContractInstanceId,
    /// Cases that came back `Holds` or `Violated` for this contract this tick.
    pub verdicts: usize,
    /// Cases that came back `Inconclusive` for this contract this tick.
    pub inconclusive: usize,
}

impl ShadowReport {
    /// Whether this tick reached a verdict on `contract`.
    ///
    /// A helper rather than `judged.contains(..)`, which stopped compiling when the
    /// list gained its per-contract counts — and which callers would otherwise
    /// re-implement one `iter().any()` at a time.
    pub fn judged_contains(&self, contract: &ContractInstanceId) -> bool {
        self.judged.iter().any(|j| j.contract == *contract)
    }
}

/// The shadow loop's state, owned by the capture writer task.
/// Where this tick's focus candidates came from.
///
/// Reported on every tick rather than assumed. The two sources have very different
/// reach - the hosted set is the whole point of #5366's fix, the sampler set is the
/// horizon that bug created - and a peer degraded to the second one produces tick
/// lines that are otherwise indistinguishable from a healthy peer hosting few
/// contracts.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub(crate) enum CandidateSource {
    /// The contracts the peer hosts. The intended source.
    Hosted,
    /// The contracts the sampler already holds, used when no hosted source was
    /// registered. Keeps probing alive; does not cover the hosted set.
    ///
    /// The DEFAULT, deliberately, and this is load-bearing rather than arbitrary.
    /// `Hosted` was the default first, which made `FocusTick::default()` assert the
    /// hosted source was already in use before any draw had happened - and the writer's
    /// warm-up retry is guarded on `source != Hosted`, so the guard was false from the
    /// first tick and the retry arm was never once polled. A default must be the claim
    /// that is safe to be wrong about; "we are on the fallback" costs a retry, while
    /// "we are on the hosted set" silently cancels the recovery path.
    #[default]
    SamplerFallback,
}

impl CandidateSource {
    pub(crate) fn as_str(self) -> &'static str {
        match self {
            CandidateSource::Hosted => "hosted",
            CandidateSource::SamplerFallback => "sampler",
        }
    }
}

/// Choose the pool focus selects from, and say where it came from.
///
/// Pulled out of [`ShadowRunner::focus`] purely so it can be tested. The hosted branch
/// is the entire point of #5366's fix, and in-process it is unreachable from a test:
/// `set_hosted_contracts_source` no-ops unless capture is enabled, and capture reads
/// its switch from the environment on first touch. Reading the global inside `focus`
/// therefore made the branch that matters the one branch no test could take - swapping
/// the two arms passed the whole suite.
pub(crate) fn candidate_pool(
    hosted: Option<Vec<ContractInstanceId>>,
    fallback: &[ContractInstanceId],
) -> (Vec<ContractInstanceId>, CandidateSource) {
    match hosted {
        Some(hosted) => (hosted, CandidateSource::Hosted),
        None => (fallback.to_vec(), CandidateSource::SamplerFallback),
    }
}

/// Whether the writer should keep retrying its focus draw.
///
/// Extracted so the condition can be TESTED. As a bare `tokio::select!` arm guard it
/// could not be: nothing calls `run_writer`, and a source pin can only see that the arm
/// is written, never that its guard is permanently false. That is exactly how the first
/// version shipped - `CandidateSource` defaulted to `Hosted`, so the guard was false
/// from the first tick and the retry never ran once.
pub(crate) fn needs_hosted_warmup(
    wide: bool,
    last: &FocusTick,
    attempts: u32,
    budget: u32,
) -> bool {
    // Wide capture ignores focus entirely, so it has nothing to warm up.
    !wide && last.source != CandidateSource::Hosted && attempts < budget
}

/// One tick's focus decision, before any samples are gathered for it.
#[derive(Debug, Clone, Default)]
pub(crate) struct FocusTick {
    /// The contracts to watch this tick.
    pub selected: Vec<ContractInstanceId>,
    /// How many contracts were eligible. Distinguishes "this peer hosts almost
    /// nothing" from "selection is broken".
    pub candidates: usize,
    pub source: CandidateSource,
}

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

    /// Advance to the next probe tick, rotating the focus set when one is due.
    ///
    /// Separate from [`focus`](Self::focus) so focus can be computed at startup
    /// without consuming a tick. Rotation counts probe ticks rather than wall time so
    /// a peer that was down does not skip epochs it never sampled in.
    pub(crate) fn advance(&mut self) {
        self.ticks = self.ticks.wrapping_add(1);
        if self.ticks % ROTATE_EVERY_TICKS == 0 {
            self.selector.rotate();
            self.logged.clear();
            store_epoch(&self.dir, self.selector.epoch());
        }
    }

    /// The contracts this peer should be watching right now.
    ///
    /// Candidates are the contracts the peer HOSTS, per the RFC: "each peer
    /// independently selects one or a small bounded number of hosted contracts as
    /// focus contracts". They are deliberately not the contracts the sampler has
    /// collected. Sourcing them from the sampler is what caused #5366 - the sampler
    /// stops admitting new contracts at its tracking cap and never evicts, so the
    /// candidate pool froze at the first N contracts the peer ever observed and
    /// rotation reshuffled a fixed set forever. On the live capture peer that showed
    /// up as `tracking_at_cap=true` on every tick for hours, with focus unable to
    /// reach anything new no matter how long it ran.
    ///
    /// Hosted-sourced focus inverts the dependency: focus decides what to sample,
    /// rather than sampling deciding what can be focused. A freshly focused contract
    /// therefore starts with no samples and accumulates them from ordinary traffic,
    /// which is the intended behaviour and is reported as `skipped_no_samples`, not
    /// as a failure.
    ///
    /// `fallback` is used only when no hosted-contract source has been registered.
    /// Callers pass the sampler keys, which keeps a peer probing rather than going
    /// silent, but reinstates exactly the #5366 horizon - so the tick reports which
    /// source it used. `candidates=sampler` means focus has stopped covering the
    /// hosted set, a state that otherwise looks identical to a peer hosting little.
    pub(crate) fn focus(
        &self,
        hosted: Option<Vec<ContractInstanceId>>,
        fallback: &[ContractInstanceId],
    ) -> FocusTick {
        if self.mode == EnforcementMode::Disabled {
            return FocusTick::default();
        }
        let (candidates, source) = candidate_pool(hosted, fallback);
        FocusTick {
            selected: self.selector.select(&candidates),
            candidates: candidates.len(),
            source,
        }
    }

    /// Copy out what a probe needs for the focused contracts.
    ///
    /// Split from [`probe`] deliberately, and the split is load-bearing rather than
    /// tidiness. This half borrows the sampler map, so it has to run on the capture
    /// task, and it is cheap: a bounded copy of the selected contracts' samples. The
    /// probe half is CPU-bound WASM execution under a ten-second-per-contract budget,
    /// and running THAT on the capture task would stop the task draining its
    /// observation queue for as long as it ran - dropping the very observations
    /// conformance exists to collect, from a mechanism whose stated requirement is
    /// that it never degrade normal operation. The periodic flush was made async for
    /// exactly this reason; a probe is the worse case, because it is CPU-bound and
    /// cannot be yielded away from by awaiting a syscall.
    /// Returns the work plus the number of focused contracts that have no samples yet.
    ///
    /// That count is not incidental. `ShadowReport::focused` is derived from the work
    /// length, so a focus contract still warming up simply vanished: a tick that
    /// selected two contracts and found samples for one reported `focused=1,
    /// skipped_no_samples=0`, which reads as "one contract, fully checked" rather than
    /// "half the focus set had nothing to check". Warm-up is the NORMAL state now that
    /// focus picks from the hosted set rather than from what was already sampled, so
    /// hiding it would hide the common case.
    pub(crate) fn select(
        &self,
        focus: &FocusTick,
        samplers: &HashMap<ContractInstanceId, super::capture::TrackedContract>,
    ) -> (Vec<(ContractInstanceId, ReplayBundle)>, usize) {
        if self.mode == EnforcementMode::Disabled {
            return (Vec::new(), 0);
        }
        let work: Vec<_> = focus
            .selected
            .iter()
            .filter_map(|instance| {
                let tracked = samplers.get(instance)?;
                Some((*instance, super::capture::bundle_for(*instance, tracked)))
            })
            .collect();
        let awaiting_samples = focus.selected.len().saturating_sub(work.len());
        (work, awaiting_samples)
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
        report.without_verdict += work.len();
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

/// Count a removal-proposing decision, and whether its finding is the class that
/// converges.
///
/// One function for BOTH `WouldRemove` and `Remove` deliberately. The comment
/// between those two arms says they are handled together precisely so shadow and
/// enforcement cannot drift — counting the settled subset in only one of them
/// would have introduced that drift while the comment claimed otherwise.
fn count_would_remove(report: &mut ShadowReport, violation: &super::property::Violation) {
    report.would_remove += 1;
    if matches!(
        violation.settling,
        Some(crate::conformance::IdempotenceSettling::SettledAfter(_))
    ) {
        report.would_remove_settled += 1;
    }
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
    // Start the clock BEFORE the oracle is built, not after. Loading the code,
    // constructing the engine and compiling the module all happen first, and for a
    // cold or large contract that setup can exceed the whole budget on its own. Timing
    // only the case loop would let a probe blow through its advertised ceiling and
    // still report `timed_out == 0`, because the few cases that then ran were quick —
    // a ceiling that reports itself as respected while being exceeded is worse than no
    // ceiling, since it is the telemetry the next tuning decision would rest on.
    let started = Instant::now();

    let corpus = bundle.to_corpus();
    if corpus.is_empty() {
        report.skipped_no_samples += 1;
        // No cases means no opinion was formed. See `ShadowReport::without_verdict`.
        report.without_verdict += 1;
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
            report.without_verdict += 1;
            return;
        }
    };

    let mut oracle = match RuntimeOracle::standalone(code, bundle.parameters.clone()).await {
        Ok(oracle) => oracle,
        Err(err) => {
            // A contract whose WASM will not load is not a conformance finding either.
            tracing::debug!(%instance, error = %err, "shadow probe could not build an oracle");
            report.skipped_no_code += 1;
            report.without_verdict += 1;
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
    // Only what setup did not already spend.
    let Some(remaining) = budget.checked_sub(started.elapsed()) else {
        report.timed_out += 1;
        // Setup alone exhausted the budget: no case ran, so no opinion was formed.
        report.without_verdict += 1;
        return;
    };
    let joined = tokio::task::spawn_blocking(move || {
        let mut out = ProbeOutcome::default();
        let loop_started = Instant::now();
        for case in &cases {
            if loop_started.elapsed() >= remaining {
                out.timed_out = true;
                break;
            }
            let outcome = verify_case(&mut oracle, case);
            out.cases += 1;
            out.decisions.push(decide(mode, &outcome));
            let inconclusive = matches!(outcome, PropertyOutcome::Inconclusive(_));
            out.inconclusive += usize::from(inconclusive);
            // A verdict is `Holds` or `Violated` — anything that is not `Inconclusive`.
            // One is enough: the contract is no longer merely a candidate we looked at,
            // it is one we formed an opinion about.
            if !inconclusive {
                out.reached_verdict = true;
            }
        }
        out
    })
    .await;

    let outcome = match joined {
        Ok(outcome) => outcome,
        Err(err) => {
            // A probe that died is not a finding about the contract, and it formed no
            // opinion either — counted as such rather than silently dropped.
            tracing::warn!(%instance, error = %err, "shadow probe thread failed");
            report.without_verdict += 1;
            return;
        }
    };

    report.cases += outcome.cases;
    report.inconclusive += outcome.inconclusive;
    if outcome.timed_out {
        report.timed_out += 1;
    }

    // `recently_checked` on the dashboard must mean "we formed an opinion", not "we
    // looked". A contract whose every case came back `Inconclusive` was probed — it
    // consumed a full case budget — but reached no verdict, and rendering it as
    // "checked, no violation found" is exactly the conflation this subsystem exists to
    // prevent. See `ShadowReport::judged` / `without_verdict`.
    if outcome.reached_verdict {
        // The counts go with the contract, not only into the tick's totals: the
        // dashboard's clean branch has to say how much looking THIS contract got, and
        // a fleet-wide case count cannot answer that for a contract last probed forty
        // ticks ago. `verdicts` is what the case loop did NOT call inconclusive.
        report.judged.push(JudgedContract {
            contract: instance_copy,
            verdicts: outcome.cases.saturating_sub(outcome.inconclusive),
            inconclusive: outcome.inconclusive,
        });
    } else {
        report.without_verdict += 1;
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
                count_would_remove(report, &violation);
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
                count_would_remove(report, &violation);
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
    /// Whether any case in this batch reached `Holds` or `Violated`. See
    /// `ShadowReport::judged`.
    reached_verdict: bool,
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
                        if let Err(err) =
                            std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o600))
                        {
                            tracing::warn!(
                                path = %path.display(),
                                error = %err,
                                "could not tighten permissions on an existing focus \
                                 salt; it stays readable beyond its owner"
                            );
                        }
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
    // Owner-only like the salt beside it. The epoch alone reveals nothing without the
    // salt, so this is consistency rather than a hole being closed — but a directory
    // where one of two files that jointly determine focus is protected and the other
    // is not invites exactly the wrong inference about which parts matter.
    let written = crate::wasm_runtime::create_owner_only(&path).and_then(|mut file| {
        use std::io::Write;
        file.write_all(epoch.to_string().as_bytes())
    });
    if let Err(err) = written {
        tracing::warn!(
            path = %path.display(),
            error = %err,
            "could not persist the conformance focus epoch; focus rotation will \
             restart from zero after a restart"
        );
    }
}

/// Drive a REAL probe of the deliberately-defective fixture contract, end to end.
///
/// A seam, not a convenience. Everything downstream of a probe — `checked_contracts`,
/// `MergeCheckStatus::record`, `view_for`, the contract-detail card — was tested only
/// against hand-built `CheckedContract`s, so the assembly production actually runs had
/// no test touching it, and three review rounds of #5403 each found a defect somewhere
/// along it (focus selection fed in place of judged contracts; findings evicting
/// independently of their contract; duplicate rows on a contract's first record).
/// Tests reach the real inputs through here so a fixture cannot quietly disagree with
/// what a probe emits.
///
/// `mode` selects the fixture's defect — see `tests/test-contract-conformance`. Every
/// caller today passes mode 6 (NEVER_SETTLES), which breaks more than one merge law on
/// most generated cases and repeats each of them across the tick's cases: the input
/// that distinguishes a deduplicating assembly both from one that appends and from one
/// that collapses everything onto the contract. (This doc named mode 1,
/// LAST_WRITE_WINS, which nothing calls it with — a reader checking the fixture would
/// have found a different defect than the tests actually exercise.)
///
/// Returns the fixture's instance id alongside the probe's own two outputs, which are
/// exactly the pair `capture::run_writer` hands to `status::publish` — and the report
/// has been through `count_awaiting_samples`, the same fold the writer applies, so a
/// test feeding `report.without_verdict` to `MergeCheckStatus::record` is feeding a
/// number production would actually publish. The temp directories are dropped before
/// returning: the probe resolves and compiles the code while it runs, so nothing here
/// outlives the call.
#[cfg(test)]
pub(crate) async fn probe_fixture_contract(
    mode: u8,
) -> Result<(ContractInstanceId, ShadowReport, Vec<Finding>), Box<dyn std::error::Error>> {
    use freenet_stdlib::prelude::{
        ContractCode, ContractContainer, ContractWasmAPIVersion, Parameters, WrappedContract,
    };
    use std::sync::Arc;

    use crate::conformance::capture::{Observation, SamplingScope, TrackedContract, record};

    let wasm = crate::wasm_runtime::tests::get_test_module("test_contract_conformance")?;
    let code_hash = *blake3::hash(&wasm).as_bytes();
    let store_dir = crate::util::tests::get_temp_dir();
    std::fs::create_dir_all(store_dir.path())?;
    let db = crate::contract::storages::Storage::new(store_dir.path()).await?;
    let mut store =
        crate::wasm_runtime::ContractStore::new(store_dir.path().into(), 10_000_000, db)?;

    let parameters: Parameters<'static> = Parameters::from(vec![mode]);
    let contract = WrappedContract::new(Arc::new(ContractCode::from(wasm)), parameters.clone());
    let id = *contract.key().id();
    store.store_contract(ContractContainer::Wasm(ContractWasmAPIVersion::V1(
        contract,
    )))?;

    // Several distinct merges, so the generator has a corpus wide enough to build
    // many cases from. The fixture's state is a canonical (sorted, deduplicated)
    // byte set, which is what its own `validate_state` accepts.
    let transitions: &[(Vec<u8>, Vec<u8>)] = &[
        (vec![1], vec![1, 2]),
        (vec![2], vec![2, 3]),
        (vec![3], vec![3, 4]),
        (vec![1, 2], vec![1, 2, 5]),
    ];
    let mut samplers: HashMap<ContractInstanceId, TrackedContract> = HashMap::new();
    for (base, result) in transitions {
        let _evicted = record(
            &mut samplers,
            &SamplingScope::Wide,
            Observation {
                contract: id,
                code_hash,
                parameters: parameters.as_ref().to_vec(),
                base_state: base.clone(),
                incoming_state: Some(result.clone()),
                delta: None,
                result_state: result.clone(),
                related: Vec::new(),
            },
        );
    }

    let dir = tempfile::TempDir::new()?;
    let runner = ShadowRunner::new(dir.path(), EnforcementMode::Shadow);
    let focus = runner.focus(None, &samplers.keys().copied().collect::<Vec<_>>());
    let (work, awaiting) = runner.select(&focus, &samplers);
    assert!(
        !work.is_empty(),
        "the fixture contract was not selected, so a probe never ran and any \
         assertion downstream of this proves nothing"
    );

    let (mut report, findings) = probe(
        work,
        Some(store_dir.path().to_path_buf()),
        EnforcementMode::Shadow,
    )
    .await;
    // The same fold `capture::run_writer` applies before publishing. Discarding it
    // here made the seam diverge from production in exactly the counts tests read:
    // `report.without_verdict` fed to `MergeCheckStatus::record` would have been a
    // tick's unjudged count that a live peer could not produce while any focus
    // contract was still warming up.
    count_awaiting_samples(&mut report, awaiting);
    Ok((id, report, findings))
}

#[cfg(test)]
mod settled_counter_tests {
    use super::*;
    use crate::conformance::{
        ConformanceProperty, IdempotenceSettling, OutputDigest, Severity, Violation,
    };

    fn violation_with(settling: Option<IdempotenceSettling>) -> Violation {
        Violation {
            property: ConformanceProperty::StateIdempotence,
            severity: Severity::Violation,
            left: OutputDigest::of(&[1u8]),
            right: OutputDigest::of(&[2u8]),
            detail: "test".to_string(),
            settling,
        }
    }

    /// The settled subset is counted, and is ADDITIVE rather than netted off.
    ///
    /// `would_remove` is documented as the number Phase 5 is judged on, and since
    /// #5462 it includes contracts that converge. Subtracting the settled ones
    /// would be the severity fork the decision rejected, hidden in a metric; the
    /// finding really is removal-eligible. So the total must stay whole and the
    /// subset must be reported beside it.
    #[test]
    fn settled_findings_are_counted_beside_the_total_not_deducted_from_it() {
        let mut report = ShadowReport::default();

        count_would_remove(
            &mut report,
            &violation_with(Some(IdempotenceSettling::SettledAfter(1))),
        );
        count_would_remove(
            &mut report,
            &violation_with(Some(IdempotenceSettling::NeverSettled)),
        );
        count_would_remove(
            &mut report,
            &violation_with(Some(IdempotenceSettling::Indeterminate)),
        );
        count_would_remove(&mut report, &violation_with(None));

        assert_eq!(
            report.would_remove, 4,
            "every removal-proposing decision counts toward the total exactly ONCE, \
             including the settled ones: they are removal-eligible findings"
        );
        assert_eq!(
            report.would_remove_settled, 1,
            "only the SETTLED class is in the subset. `Indeterminate` must not be: \
             it is an unknown class, and counting an unknown as benign asserts the \
             very thing that variant exists to refuse. `NeverSettled` and the \
             properties carrying no settling data are excluded too"
        );
    }

    /// Each decision increments the total exactly once.
    ///
    /// Added after mutation: incrementing `would_remove` again alongside the shared
    /// helper survived, because the test above asserted a total that happened to
    /// match the doubled count. A drifting total is the specific thing the shared
    /// helper exists to prevent.
    #[test]
    fn a_single_decision_increments_the_total_exactly_once() {
        let mut report = ShadowReport::default();
        count_would_remove(&mut report, &violation_with(None));
        assert_eq!(report.would_remove, 1);
        assert_eq!(report.would_remove_settled, 0);
    }

    /// Pin: BOTH removal-proposing arms route through the one counter.
    ///
    /// The comment between them says they are handled together so shadow and
    /// enforcement cannot drift. A test of `count_would_remove` alone cannot see an
    /// arm that stopped calling it, which is exactly how the settled subset would
    /// silently start under-counting the moment enforcement became reachable.
    #[test]
    fn both_removal_arms_route_through_the_counter() {
        // Bounded by brace-counting to `probe_one`'s OWN body, not by splitting on
        // its signature and reading to end-of-file. An unbounded region silently
        // widens to swallow unrelated code — including this test module — and then
        // passes for the wrong reason. That is the trap the repo documents on its
        // other scrapes, and my first version of this pin walked into it.
        let src = include_str!("shadow.rs");
        let start = src
            .find("async fn probe_one(")
            .expect("probe_one not found");
        let after = &src[start..];
        let open = after.find('{').expect("probe_one has no body");
        let mut depth = 0usize;
        let mut end = open;
        for (i, c) in after[open..].char_indices() {
            match c {
                '{' => depth += 1,
                '}' => {
                    depth -= 1;
                    if depth == 0 {
                        end = open + i;
                        break;
                    }
                }
                _ => {}
            }
        }
        assert!(end > open, "could not bound probe_one's body");
        let body = &after[open..end];

        for arm in [
            "ConformanceAction::WouldRemove(violation) => {",
            "ConformanceAction::Remove(violation) => {",
        ] {
            let at = body
                .find(arm)
                .unwrap_or_else(|| panic!("arm missing: {arm}"));
            let window = &body[at..at + 200.min(body.len() - at)];
            assert!(
                window.contains("count_would_remove("),
                "{arm} must count through the shared helper, or the two counters \
                 drift while the comment beside them claims they cannot"
            );
            // And ONLY through it. Calling the helper does not stop an arm also
            // incrementing on its own, which double-counts the number Phase 5 is
            // judged on — and a test that calls the helper directly cannot see a
            // second increment at the call site.
            assert!(
                !window.contains("report.would_remove"),
                "{arm} touches the counters directly as well as through the shared \
                 helper; that double-counts `would_remove` and no helper-level test \
                 can observe it"
            );
        }
    }
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
        runner.advance();
        // No hosted-contract source is registered in unit tests, so focus falls back
        // to the sampler keys - which is exactly the fallback path production reports
        // as `candidate_source=sampler`.
        let focus = runner.focus(None, &samplers.keys().copied().collect::<Vec<_>>());
        let (work, awaiting) = runner.select(&focus, samplers);
        let (mut report, findings) =
            probe(work, store.map(|p| p.to_path_buf()), runner.mode()).await;
        // The writer's own fold, for the same reason `probe_fixture_contract` applies
        // it: a report that skipped it is one no live peer publishes.
        count_awaiting_samples(&mut report, awaiting);
        runner.record(&findings);
        report
    }

    /// The warm-up fold has to reach all three counters.
    ///
    /// `judged` and `without_verdict` are complements over the focus set, so a
    /// contract focus picked but the sampler had nothing for lands in NEITHER half
    /// unless this puts it in one — and the dashboard's unjudged total then silently
    /// omits it, rendering it as a contract nobody had a question about. Warm-up is
    /// the ordinary state now that focus draws from the hosted set, so this is the
    /// common path.
    ///
    /// Tested against the function rather than scraped out of `run_writer`: while the
    /// three lines lived at the call site, the only guard was a source pin, and the
    /// `probe_fixture_contract` seam applied none of them at all.
    #[test]
    fn awaiting_samples_reach_focused_skipped_and_without_verdict() {
        let mut report = ShadowReport {
            focused: 1,
            skipped_no_samples: 0,
            without_verdict: 0,
            probed: 1,
            ..Default::default()
        };
        count_awaiting_samples(&mut report, 3);
        assert_eq!(
            (
                report.focused,
                report.skipped_no_samples,
                report.without_verdict
            ),
            (4, 3, 3),
            "a warming-up focus contract must reach `focused` (or the focus set \
             reports as the smaller subset that had samples), `skipped_no_samples` \
             (or the reason is unstated) and `without_verdict` (or it counts as \
             neither judged nor unjudged): {report:?}"
        );
        assert_eq!(
            report.probed, 1,
            "the fold must not touch `probed` — no case ran for a warming-up contract"
        );
    }

    /// A fresh `FocusTick` must NOT claim the hosted source is already in use.
    ///
    /// This is the bug that shipped: `CandidateSource` derived `Default` with `Hosted`
    /// marked `#[default]`, so `FocusTick::default()` asserted the hosted set was in
    /// use before any draw had happened. The writer's warm-up retry is guarded on
    /// `source != Hosted`, so the guard was false from the first tick and the retry arm
    /// was never polled once - leaving exactly the startup gap it was added to close.
    #[test]
    fn a_default_focus_tick_does_not_claim_the_hosted_source() {
        assert_eq!(
            FocusTick::default().source,
            CandidateSource::SamplerFallback,
            "a focus decision that has not happened yet must not claim the hosted set"
        );
    }

    /// The warm-up keeps retrying while focus is still on the fallback.
    #[test]
    fn warmup_retries_while_focus_is_still_on_the_fallback() {
        let fallback = FocusTick {
            source: CandidateSource::SamplerFallback,
            ..FocusTick::default()
        };
        assert!(
            needs_hosted_warmup(false, &fallback, 0, 12),
            "warm-up gave up while focus was still on the sampler fallback"
        );
        // And specifically for a DEFAULT tick, which is the state at task start.
        assert!(
            needs_hosted_warmup(false, &FocusTick::default(), 0, 12),
            "warm-up was disabled before the first draw had even happened"
        );
    }

    /// It stops for good once a draw reaches the hosted set.
    #[test]
    fn warmup_stops_once_focus_reaches_the_hosted_set() {
        let hosted = FocusTick {
            source: CandidateSource::Hosted,
            ..FocusTick::default()
        };
        assert!(!needs_hosted_warmup(false, &hosted, 0, 12));
    }

    /// Wide capture never warms up: it ignores focus entirely.
    #[test]
    fn warmup_does_not_run_for_wide_capture() {
        assert!(!needs_hosted_warmup(true, &FocusTick::default(), 0, 12));
    }

    /// It gives up rather than ticking forever when no source ever appears.
    ///
    /// Reachable, not hypothetical: the node calls `capture::global()` - which spawns
    /// the writer - before it checks whether the daemon is disabled, and a disabled
    /// daemon idles indefinitely without ever constructing a `Ring` to register one.
    #[test]
    fn warmup_gives_up_after_its_attempt_budget() {
        assert!(
            !needs_hosted_warmup(false, &FocusTick::default(), 12, 12),
            "warm-up would retry forever on a node that never builds a ring"
        );
    }

    /// `focus` actually consults the hosted set it is handed.
    ///
    /// The wiring, not just `candidate_pool`: hardcoding `None` at the call site would
    /// silently reinstate #5366 in production, and until `focus` took the lookup as a
    /// parameter no test could tell the difference.
    #[tokio::test]
    async fn focus_selects_from_the_hosted_set_it_is_given() -> anyhow::Result<()> {
        let dir = tempfile::TempDir::new()?;
        let runner = ShadowRunner::new(dir.path(), EnforcementMode::Shadow);
        let hosted: Vec<_> = (10..40u8).map(instance).collect();

        let tick = runner.focus(Some(hosted.clone()), &[instance(1), instance(2)]);

        assert_eq!(tick.source, CandidateSource::Hosted);
        assert_eq!(tick.candidates, hosted.len());
        assert!(
            !tick.selected.is_empty(),
            "hosted candidates selected nothing"
        );
        for picked in &tick.selected {
            assert!(
                hosted.contains(picked),
                "focus selected {picked} which the peer does not host"
            );
        }
        Ok(())
    }

    /// The hosted set is preferred, and is reported as such.
    ///
    /// This branch is the entire point of #5366's fix and was, until `candidate_pool`
    /// was extracted, the one branch no test could reach: `focus` read a process-global
    /// that only registers when capture is enabled from the environment, so every test
    /// took the fallback and swapping the two arms passed the whole suite.
    #[test]
    fn candidates_come_from_the_hosted_set_when_one_is_available() {
        let hosted = vec![instance(7), instance(8), instance(9)];
        let sampler_keys = vec![instance(1), instance(2)];

        let (pool, source) = candidate_pool(Some(hosted.clone()), &sampler_keys);

        assert_eq!(pool, hosted, "focus did not draw from the hosted set");
        assert_eq!(source, CandidateSource::Hosted);
    }

    /// With no hosted source, focus keeps working off the sampler - and says so.
    ///
    /// The fallback reinstates exactly the #5366 horizon, so the distinction has to
    /// reach the logs; a peer degraded to it otherwise looks like one hosting little.
    #[test]
    fn candidates_fall_back_to_the_sampler_and_report_it() {
        let sampler_keys = vec![instance(1), instance(2)];

        let (pool, source) = candidate_pool(None, &sampler_keys);

        assert_eq!(pool, sampler_keys);
        assert_eq!(source, CandidateSource::SamplerFallback);
        assert_eq!(source.as_str(), "sampler");
    }

    /// An empty hosted set is NOT the same as an absent source.
    ///
    /// A peer that genuinely hosts nothing must report `hosted` with zero candidates,
    /// not silently borrow the sampler's contents and claim the fix is working.
    #[test]
    fn an_empty_hosted_set_does_not_fall_back_to_the_sampler() {
        let sampler_keys = vec![instance(1), instance(2)];

        let (pool, source) = candidate_pool(Some(Vec::new()), &sampler_keys);

        assert!(pool.is_empty(), "an empty hosted set borrowed sampler keys");
        assert_eq!(source, CandidateSource::Hosted);
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
            let _evicted = record(
                &mut samplers,
                &crate::conformance::capture::SamplingScope::Wide,
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
        // The #5403 review defect: a contract skipped before probing must not be
        // recorded as judged, and must count toward `without_verdict` — otherwise the
        // dashboard's `recently_checked` would carry a contract nothing looked at, and
        // its card would read "no violation found" for a contract with no code to run.
        assert!(
            report.judged.is_empty(),
            "a contract with no resolvable code was recorded as judged: {report:?}"
        );
        assert_eq!(
            report.without_verdict, report.focused,
            "a contract skipped for no code did not count toward without_verdict: \
             {report:?}"
        );
        assert_eq!(
            report.judged.len() + report.without_verdict,
            report.focused,
            "judged and without_verdict must be complements over the focus set, or \
             the dashboard silently under- or over-reports: {report:?}"
        );
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
        let runner = ShadowRunner::new(dir.path(), EnforcementMode::Shadow);
        let focus = runner.focus(None, &samplers.keys().copied().collect::<Vec<_>>());
        let (work, _awaiting) = runner.select(&focus, &samplers);
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

        for (mode, report) in [(LAST_WRITE_WINS, broken), (CONFORMING, honest)] {
            assert_eq!(
                report.judged.len() + report.without_verdict,
                report.focused,
                "mode {mode}: judged and without_verdict must be complements over \
                 the focus set, or the dashboard silently under- or over-reports: \
                 {report:?}"
            );
            assert_eq!(
                report.judged.len(),
                1,
                "mode {mode} reached a verdict but was not recorded as judged: \
                 {report:?}"
            );
        }

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

    /// A contract that is genuinely probed, and runs every one of its cases, but
    /// reaches NO verdict on any of them, must not be recorded as judged — and a
    /// SIBLING contract probed in the same tick that DOES reach a verdict must
    /// still land in `judged`. Both halves run in one tick on purpose: a version
    /// of the fix that stopped populating `judged` entirely (rather than
    /// correctly excluding the inconclusive-only contract) would still pass an
    /// inconclusive-only-contract-not-judged assertion in isolation, so the
    /// positive half is what actually pins the mechanism rather than just its
    /// absence.
    ///
    /// The #5403 review defect: `probe_one` incremented `report.probed` before any
    /// case ran and nothing distinguished "every case was `Inconclusive`" from "at
    /// least one case reached `Holds` or `Violated`". Both looked identical to a
    /// caller that only had `probed`, so a contract nothing was ever concluded about
    /// rendered on the dashboard exactly like a contract checked and found clean.
    ///
    /// Mode 7 (`REQUIRES_RELATED`) is the fixture built for the inconclusive half:
    /// `validate_state` asks for another contract's state, and `samplers_for` never
    /// supplies any (`related: Vec::new()`), so every case dead-ends at
    /// `Inconclusive::RelatedRequired` — a real, unforced instance of the failure
    /// mode, not a hand-built stub. Mode 0 (`CONFORMING`) is the sibling that
    /// reaches `Holds` on every case.
    #[tokio::test(flavor = "multi_thread")]
    async fn a_contract_whose_every_case_is_inconclusive_is_not_judged()
    -> Result<(), Box<dyn std::error::Error>> {
        use freenet_stdlib::prelude::{
            ContractCode, ContractContainer, ContractWasmAPIVersion, Parameters, WrappedContract,
        };
        use std::sync::Arc;

        // Keep in sync with `tests/test-contract-conformance/src/lib.rs`.
        const CONFORMING: u8 = 0;
        const REQUIRES_RELATED: u8 = 7;

        let wasm = crate::wasm_runtime::tests::get_test_module("test_contract_conformance")?;
        let code_hash = *blake3::hash(&wasm).as_bytes();

        let store_dir = crate::util::tests::get_temp_dir();
        std::fs::create_dir_all(store_dir.path())?;
        let db = crate::contract::storages::Storage::new(store_dir.path()).await?;
        let mut store =
            crate::wasm_runtime::ContractStore::new(store_dir.path().into(), 10_000_000, db)?;

        let transitions = &[(vec![1u8], vec![1u8, 2]), (vec![2u8], vec![2u8, 3])];

        // No `related` state is attached to any transition, so `validate_state`
        // never gets what it asks for and every case is inconclusive.
        let unjudged_parameters: Parameters<'static> = Parameters::from(vec![REQUIRES_RELATED]);
        let unjudged_contract = WrappedContract::new(
            Arc::new(ContractCode::from(wasm.clone())),
            unjudged_parameters.clone(),
        );
        let unjudged_id = *unjudged_contract.key().id();
        store.store_contract(ContractContainer::Wasm(ContractWasmAPIVersion::V1(
            unjudged_contract,
        )))?;
        let mut samplers =
            samplers_for(unjudged_id, vec![REQUIRES_RELATED], code_hash, transitions);

        // A genuinely conforming sibling, probed in the SAME tick, must still be
        // recorded as judged.
        let judged_parameters: Parameters<'static> = Parameters::from(vec![CONFORMING]);
        let judged_contract = WrappedContract::new(
            Arc::new(ContractCode::from(wasm)),
            judged_parameters.clone(),
        );
        let judged_id = *judged_contract.key().id();
        store.store_contract(ContractContainer::Wasm(ContractWasmAPIVersion::V1(
            judged_contract,
        )))?;
        samplers.extend(samplers_for(
            judged_id,
            vec![CONFORMING],
            code_hash,
            transitions,
        ));

        // Both candidates fit within MAX_FOCUS_CONTRACTS, so both are selected —
        // this is not testing which one focus happened to pick.
        assert_eq!(
            samplers.len(),
            2,
            "test setup did not produce two distinct contracts"
        );

        let dir = tempfile::TempDir::new()?;
        let mut runner = ShadowRunner::new(dir.path(), EnforcementMode::Shadow);
        let report = tick(&mut runner, &samplers, Some(store_dir.path())).await;

        assert_eq!(
            report.probed, 2,
            "both contracts were not probed, so this proves nothing: {report:?}"
        );
        assert!(
            report.cases > 0,
            "no case ran, so this proves nothing: {report:?}"
        );
        assert!(
            report.inconclusive > 0,
            "expected the REQUIRES_RELATED contract's cases to be inconclusive, or \
             the fixture no longer exercises this failure mode: {report:?}"
        );
        assert!(
            !report.judged_contains(&unjudged_id),
            "a contract whose every case was inconclusive was recorded as judged: \
             {report:?}"
        );
        assert!(
            report.judged_contains(&judged_id),
            "a contract that genuinely reached a verdict on every case was NOT \
             recorded as judged: {report:?}"
        );
        // The per-contract counts the dashboard's clean branch renders. The judged
        // sibling's cases all reached a verdict; if that ever stops being true the
        // page would claim more was established about it than was.
        let judged_record = report
            .judged
            .iter()
            .find(|j| j.contract == judged_id)
            .expect("the conforming sibling is judged");
        assert!(
            judged_record.verdicts > 0,
            "a judged contract carries no verdict count, so its page can only report \
             the node-wide number and cannot say how much looking IT got: {report:?}"
        );
        assert_eq!(
            judged_record.verdicts,
            report.cases - report.inconclusive,
            "the only contract to reach a verdict this tick must own every one of the \
             tick's verdicts; its per-contract count disagrees with the totals, so \
             the page and the log would report different things: {report:?}"
        );
        assert_eq!(
            report.judged.len(),
            1,
            "expected exactly the conforming sibling to be judged: {report:?}"
        );
        assert_eq!(
            report.without_verdict, 1,
            "a probed contract that never reached a verdict did not count toward \
             without_verdict: {report:?}"
        );
        assert_eq!(
            report.judged.len() + report.without_verdict,
            report.focused,
            "judged and without_verdict must be complements over the focus set, or \
             the dashboard silently under- or over-reports: {report:?}"
        );
        Ok(())
    }
}
