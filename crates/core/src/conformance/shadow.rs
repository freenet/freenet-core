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

use super::activity::MergeActivity;
use super::bundle::ReplayBundle;
use super::capture::RecordOutcome;
use super::focus::{FocusSelector, FocusSlot};
use super::generator::{GeneratorConfig, generate_cases};
use super::policy::{ConformanceAction, EnforcementMode, decide};
use super::property::{ConformanceProperty, PropertyOutcome, Severity};
use super::runtime_oracle::RuntimeOracle;
use super::verifier::verify_case;

/// How many contracts a peer watches at once.
///
/// Small on purpose. Watching more is not free — each focus contract is a probe's
/// worth of WASM execution per tick — and it buys less than it looks like it should,
/// because coverage across the network comes from every peer watching a *different*
/// couple of contracts, not from any one peer watching many. Two is one uniform slot
/// and one activity-weighted slot (see `focus::FocusSelector::select_split`); with
/// one, there would be no weighted slot at all.
const MAX_FOCUS_CONTRACTS: usize = 2;

/// How often a probe runs.
pub const PROBE_INTERVAL: Duration = Duration::from_secs(15 * 60);

/// Ticks in a focus period, after which the epoch advances and every score changes.
///
/// The period is what the uniform slot is fixed for, and what stops an author who
/// somehow learns they are being watched from staying watched. The weighted slot is
/// re-drawn at every tick from current activity and goes to the draw's current
/// winner; the keys are fixed for the period, so it only moves when activity reorders
/// the draw. Eight ticks at fifteen minutes is two hours,
/// long enough for a sampler to accumulate something worth checking and short enough
/// that a day covers many contracts.
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

/// Filename of the persisted focus period (ticks into it and its accounting), beside
/// the epoch, so a restart resumes the period rather than starting a partial one.
const PERIOD_FILE: &str = "focus-period";

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
    /// Only removal-eligible findings reach it (`policy::removal_tier`). A
    /// report-only finding is counted in `reported` instead, in Shadow as in every
    /// other mode, so findings anyone can produce on demand cannot move this number.
    /// That includes a `state_idempotence` finding whose state settled after a
    /// rewrite: before the report-only tier this total included that converging
    /// class, so comparing it across that change compares different populations.
    ///
    /// `Indeterminate` idempotence findings are still in it. That is the
    /// conservative default (an unknown class must not be counted benign), and
    /// `Indeterminate` is adversarially reachable (see its rustdoc), so this total
    /// can still include findings that a proof of non-settling would clear.
    pub would_remove: usize,
    /// Findings that never propose removal in any mode: diagnostics, and
    /// violations of a report-only tier (`policy::RemovalTier::ReportOnly`).
    ///
    /// The population changed with the report-only tier. Before it, this counted
    /// diagnostics only; it now also holds every report-only `Violation`,
    /// including the settled idempotence class that used to be in
    /// `would_remove`. `report_only_violations` is that second part on its own,
    /// so `reported - report_only_violations` is the old series.
    pub reported: usize,
    /// The subset of `reported` that are `Severity::Violation`s of a report-only
    /// tier: real law breaks the network does not act on.
    pub report_only_violations: usize,
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
    /// How many of those candidates have recent merge activity (#5913).
    ///
    /// Zero means the weighted slot was drawn uniformly this tick: the peer has seen
    /// no state-changing merge for anything it hosts recently, which is a fact about
    /// traffic, not about selection. Counted over the candidates only.
    pub active_candidates: usize,
    pub source: CandidateSource,
}

/// What one focus period established, reported when the period ends.
///
/// Built to answer the #5913 acceptance question (did this period judge anything?)
/// and, when it did not, to say why: no candidates, no activity, merges that never
/// arrived, or merges the sampler refused. Each count is produced where its decision
/// is made (selection, admission, probe), not re-derived from sizes afterwards.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub(crate) struct FocusPeriodSummary {
    /// The epoch that ended.
    pub epoch: u64,
    /// Whether the period's accounting is continuous: it began at a rotation, and
    /// its state survived every restart since (it is persisted beside the epoch,
    /// #5914 review round 2, K7). False for the first period on a fresh capture
    /// directory, or after the persisted period could not be read. It does not mean
    /// the peer was up throughout: merges during downtime were never observed.
    /// Counts are checkpointed at every tick, every draw, every verdict, the
    /// capture writer's 60 s flush and when the capture channel closes (#5914
    /// review rounds 3 and 4). The node's usual exit path calls
    /// `std::process::exit` without closing that channel, so ANY restart, graceful
    /// or not, can lose up to the last minute of merge and admission counts from a
    /// period that still reads `complete`. A verdict is written at once and, if that
    /// write fails, retried at the next flush, so a verdict is lost only when the
    /// checkpoint keeps failing until the process exits.
    /// Exclude incomplete periods when measuring.
    pub complete: bool,
    /// Size of the candidate pool at the period's last selection.
    pub candidates: usize,
    /// Candidates with recent activity at the period's last selection.
    pub active_candidates: usize,
    /// Focus stints: each contract selected, counted once per continuous stretch in
    /// focus. Usually the focus size; more when focus moved mid-period.
    pub stints: usize,
    /// Merges that reached the capture task for a contract while it was in focus.
    pub merges_seen: u64,
    /// Of those, merges from which the sampler retained something new.
    pub samples_admitted: u64,
    /// Of those, merges the sampler refused (too large, over budget, no slot).
    pub samples_refused: u64,
    /// Stints in which the sampler retained nothing new. Not the same as "never
    /// examined": samples retained from an earlier stint can still be judged.
    pub stints_without_new_samples: usize,
    /// Stints in which at least one probe reached a verdict on the contract.
    pub judged: usize,
    /// Stints in the uniform slot, and how many of them were judged.
    pub uniform_stints: usize,
    pub judged_uniform: usize,
    /// Stints in the weighted slot, and how many of them were judged.
    pub weighted_stints: usize,
    pub judged_weighted: usize,
    /// Sum, over weighted stints, of the contract's raw activity count (decayed
    /// merges, not the selection weight) when the stint was drawn. With
    /// `weighted_stints`, says whether the weighted slot actually landed on busy
    /// contracts or was effectively a uniform draw.
    pub weighted_selected_activity: u64,
}

/// Per-period bookkeeping for the focus contracts.
///
/// Bounded by the focus size: only the CURRENT focus contracts are held, and a
/// contract leaving focus is folded into the totals at once.
#[derive(Debug, Default)]
struct FocusPeriod {
    current: HashMap<ContractInstanceId, PeriodMember>,
    totals: FocusPeriodSummary,
}

#[derive(Debug, Clone, Copy, serde::Serialize, serde::Deserialize)]
struct PeriodMember {
    slot: FocusSlot,
    /// Activity when this stint was drawn.
    activity: u32,
    seen: u64,
    admitted: u64,
    /// Admitted since the most recent probe tick, and during the interval before it.
    /// A member is kept only while the second is non-zero (#5914 review round 2, K1).
    admitted_this_tick: u64,
    admitted_last_tick: u64,
    refused: u64,
    judged: bool,
}

impl PeriodMember {
    fn new(slot: FocusSlot, activity: u32) -> Self {
        Self {
            slot,
            activity,
            seen: 0,
            admitted: 0,
            admitted_this_tick: 0,
            admitted_last_tick: 0,
            refused: 0,
            judged: false,
        }
    }
}

impl FocusPeriod {
    /// Make `selected` the current focus set, folding any contract leaving it into
    /// the totals, and remember the pool it was drawn from.
    fn enter(
        &mut self,
        selected: &[(ContractInstanceId, FocusSlot, u32)],
        candidates: usize,
        active: usize,
    ) {
        let leaving: Vec<ContractInstanceId> = self
            .current
            .keys()
            .filter(|id| !selected.iter().any(|(s, _, _)| s == *id))
            .copied()
            .collect();
        for id in leaving {
            if let Some(member) = self.current.remove(&id) {
                self.fold(member);
            }
        }
        // A contract already in focus in the same slot continues its stint, with its
        // activity at selection not refreshed. A contract whose slot changed (the
        // weighted pick becomes the uniform pick when the peer stops hosting the old
        // one) ends its stint in the old slot and starts one in the new slot, so each
        // slot is credited only with what happened in it (#5914 review round 3, A2).
        // Its keep state carries over: it is the same sample being built. A new
        // contract starts a stint.
        for (id, slot, activity) in selected {
            let moved = match self.current.get(id) {
                Some(member) if member.slot != *slot => Some(*member),
                _ => None,
            };
            if let Some(old) = moved {
                self.fold(old);
                let mut fresh = PeriodMember::new(*slot, *activity);
                fresh.admitted_this_tick = old.admitted_this_tick;
                fresh.admitted_last_tick = old.admitted_last_tick;
                self.current.insert(*id, fresh);
            } else {
                self.current
                    .entry(*id)
                    .or_insert_with(|| PeriodMember::new(*slot, *activity));
            }
        }
        self.totals.candidates = candidates;
        self.totals.active_candidates = active;
    }

    fn fold(&mut self, member: PeriodMember) {
        let totals = &mut self.totals;
        totals.stints += 1;
        totals.merges_seen = totals.merges_seen.saturating_add(member.seen);
        totals.samples_admitted = totals.samples_admitted.saturating_add(member.admitted);
        totals.samples_refused = totals.samples_refused.saturating_add(member.refused);
        if member.admitted == 0 {
            totals.stints_without_new_samples += 1;
        }
        if member.judged {
            totals.judged += 1;
        }
        match member.slot {
            FocusSlot::Uniform => {
                totals.uniform_stints += 1;
                totals.judged_uniform += usize::from(member.judged);
            }
            FocusSlot::Weighted => {
                totals.weighted_stints += 1;
                totals.judged_weighted += usize::from(member.judged);
                totals.weighted_selected_activity = totals
                    .weighted_selected_activity
                    .saturating_add(u64::from(member.activity));
            }
        }
    }

    /// A probe tick: what each member admitted in the interval just ended becomes
    /// the evidence [`accruing`](Self::accruing) keeps it on.
    fn tick(&mut self) {
        for member in self.current.values_mut() {
            member.admitted_last_tick = std::mem::take(&mut member.admitted_this_tick);
        }
    }

    /// Current focus contracts accruing samples: the sampler admitted something new
    /// for them in the interval before the latest probe tick. A uniform one keeps its
    /// slot; a weighted one keeps its slot only while it still wins the weighted draw
    /// (`FocusSelector::select_split`).
    ///
    /// Only the latest interval counts (#5914 review round 2, K1). Keeping any
    /// member that had EVER admitted a sample this stint let one early sample lock
    /// the weighted slot for the rest of the period, against any burst elsewhere.
    fn accruing(&self) -> Vec<(ContractInstanceId, FocusSlot)> {
        self.current
            .iter()
            .filter(|(_, member)| member.admitted_last_tick > 0)
            .map(|(id, member)| (*id, member.slot))
            .collect()
    }

    /// Count a merge for a focus member; whether it was one.
    fn seen(&mut self, contract: &ContractInstanceId) -> bool {
        match self.current.get_mut(contract) {
            Some(member) => {
                member.seen = member.seen.saturating_add(1);
                true
            }
            None => false,
        }
    }

    fn admission(&mut self, contract: &ContractInstanceId, outcome: RecordOutcome) {
        let Some(member) = self.current.get_mut(contract) else {
            return;
        };
        match outcome {
            RecordOutcome::Admitted => {
                member.admitted = member.admitted.saturating_add(1);
                member.admitted_this_tick = member.admitted_this_tick.saturating_add(1);
            }
            RecordOutcome::Refused => member.refused = member.refused.saturating_add(1),
            RecordOutcome::AdmittedPartly => {
                member.admitted = member.admitted.saturating_add(1);
                member.admitted_this_tick = member.admitted_this_tick.saturating_add(1);
                member.refused = member.refused.saturating_add(1);
            }
            RecordOutcome::NothingNew | RecordOutcome::OutOfScope => {}
        }
    }

    fn judged(&mut self, contract: &ContractInstanceId) {
        if let Some(member) = self.current.get_mut(contract) {
            member.judged = true;
        }
    }

    fn without_new_samples_now(&self) -> usize {
        self.current
            .values()
            .filter(|member| member.admitted == 0)
            .count()
    }

    /// Close the period and start an empty one.
    fn finish(&mut self, epoch: u64, complete: bool) -> FocusPeriodSummary {
        let current: Vec<PeriodMember> = self.current.drain().map(|(_, m)| m).collect();
        for member in current {
            self.fold(member);
        }
        let summary = FocusPeriodSummary {
            epoch,
            complete,
            ..self.totals
        };
        *self = FocusPeriod::default();
        summary
    }
}

/// The shadow loop's state, owned by the capture writer task.
pub struct ShadowRunner {
    mode: EnforcementMode,
    selector: FocusSelector,
    ticks: u32,
    /// Capture directory, so a rotation can persist the new epoch beside the salt.
    dir: PathBuf,
    /// `(contract, property)` pairs already logged this epoch, and the most serious
    /// finding logged for each, so a contract that fails the same law in forty
    /// cases produces one line rather than forty. Cleared on rotation, which is also
    /// what makes the log a per-epoch summary rather than an ever-growing set.
    logged: HashMap<(ContractInstanceId, ConformanceProperty), LoggedSeverity>,
    /// State-changing merges observed per contract, decayed once per period and
    /// read CURRENT at every draw (#5913, #5914 review D1).
    ///
    /// Not frozen per period: frozen weights could not see a contract that was quiet
    /// before a period and bursts during it, and focused it less than a uniform draw
    /// would while it had the most to check. Re-reading at each probe tick churns
    /// little, because the draw's keys are fixed for the period: the weighted slot
    /// moves only when activity reorders the draw (a challenger beats the incumbent).
    /// An accruing uniform member is kept outright (see `FocusPeriod::accruing`).
    activity: MergeActivity,
    /// What the current period's focus contracts have and have not seen.
    period: FocusPeriod,
    /// Whether the current period's accounting is continuous (see
    /// [`FocusPeriodSummary::complete`]). Persisted with the period.
    period_complete: bool,
    /// The period was restored from disk and has not yet been re-drawn from the
    /// hosted set. While set, a draw that could only use the sampler fallback (the
    /// hosted source registers after the capture task starts) keeps the restored
    /// focus instead of replacing it with sampler keys.
    resumed: bool,
    /// The period changed since it was last persisted (merges or admissions on a
    /// focus member). Cleared by every write; see [`flush_period`](Self::flush_period).
    period_dirty: bool,
    /// [`salt_check`] of this runner's salt, written into the persisted period.
    salt_check: [u8; 8],
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
        let restored = load_period(dir, epoch, &salt);
        let resumed = restored.is_some();
        let (ticks, period, period_complete) = match restored {
            Some(saved) => (
                saved.ticks_into_period % ROTATE_EVERY_TICKS,
                FocusPeriod {
                    current: saved.members.into_iter().collect(),
                    totals: saved.totals,
                },
                saved.complete,
            ),
            None => (0, FocusPeriod::default(), false),
        };
        Self {
            mode,
            selector: FocusSelector::resuming_at(salt, MAX_FOCUS_CONTRACTS, epoch),
            ticks,
            dir: dir.to_path_buf(),
            logged: HashMap::new(),
            activity: MergeActivity::new(super::activity::MAX_TRACKED_ACTIVITY, salt),
            period,
            period_complete,
            resumed,
            period_dirty: false,
            salt_check: salt_check(&salt),
        }
    }

    /// Write the current period beside the epoch (best-effort).
    fn persist_period(&mut self) {
        let mut members: Vec<(ContractInstanceId, PeriodMember)> = self
            .period
            .current
            .iter()
            .map(|(id, member)| (*id, *member))
            .collect();
        members.sort_by(|(a, _), (b, _)| a.as_bytes().cmp(b.as_bytes()));
        // Clean exactly when the checkpoint is on disk, from whichever path wrote it
        // (a tick, a draw, a verdict or the flush): a failed write leaves the period
        // dirty, so the next 60 s flush retries it (#5914 review rounds 4 and final).
        let written = store_period(
            &self.dir,
            &PersistedPeriod {
                salt_check: self.salt_check,
                epoch: self.selector.epoch(),
                ticks_into_period: self.ticks % ROTATE_EVERY_TICKS,
                complete: self.period_complete,
                members,
                totals: self.period.totals,
            },
        );
        self.period_dirty = !written;
    }

    pub fn epoch(&self) -> u64 {
        self.selector.epoch()
    }

    /// Advance to the next probe tick, rotating the focus set when one is due.
    ///
    /// Separate from [`focus`](Self::focus) so focus can be computed at startup
    /// without consuming a tick. Rotation counts probe ticks rather than wall time so
    /// a peer that was down does not skip epochs it never sampled in.
    ///
    /// On rotation it also closes the focus period (logged, and returned so a test can
    /// read it) and decays the activity counts, so their half-life is one period.
    ///
    /// The period (ticks into it, its members and its counts) is persisted after every
    /// tick, so a restart resumes it on schedule and its summary stays `complete`
    /// (#5914 review round 2, K7). Activity is not persisted: a restarted peer's
    /// weighted slot draws uniformly until the first state-changing merge arrives.
    pub(crate) fn advance(&mut self) -> Option<FocusPeriodSummary> {
        self.ticks = self.ticks.wrapping_add(1);
        if self.ticks % ROTATE_EVERY_TICKS == 0 {
            let ended = self
                .period
                .finish(self.selector.epoch(), self.period_complete);
            self.period_complete = true;
            tracing::info!(
                epoch = ended.epoch,
                complete = ended.complete,
                candidates = ended.candidates,
                active_candidates = ended.active_candidates,
                stints = ended.stints,
                merges_seen = ended.merges_seen,
                samples_admitted = ended.samples_admitted,
                samples_refused = ended.samples_refused,
                stints_without_new_samples = ended.stints_without_new_samples,
                judged = ended.judged,
                uniform_stints = ended.uniform_stints,
                judged_uniform = ended.judged_uniform,
                weighted_stints = ended.weighted_stints,
                judged_weighted = ended.judged_weighted,
                weighted_selected_activity = ended.weighted_selected_activity,
                "conformance focus period ended"
            );
            self.selector.rotate();
            self.logged.clear();
            store_epoch(&self.dir, self.selector.epoch());
            self.activity.decay();
            self.resumed = false;
            self.persist_period();
            return Some(ended);
        }
        self.period.tick();
        self.persist_period();
        None
    }

    /// Count a merge the capture task observed.
    ///
    /// Called for EVERY observed merge, focused or not, because weighting has to
    /// know about contracts that are not in focus yet. It must stay ahead of the scope
    /// filter in `capture::run_writer`, which discards out-of-focus observations.
    /// `activity` is [`super::capture::activity_digest`]'s verdict: `None` for a merge
    /// that cannot count, otherwise its result-state digest, which counts unless the
    /// contract produced that state recently.
    pub(crate) fn note_merge(
        &mut self,
        contract: ContractInstanceId,
        activity: Option<super::activity::StateDigest>,
    ) {
        if let Some(digest) = activity {
            self.activity.record(contract, digest);
        }
        if self.period.seen(&contract) {
            self.period_dirty = true;
        }
    }

    /// Record what the sampler did with a merge, for the focus period's counts.
    pub(crate) fn note_admission(&mut self, contract: &ContractInstanceId, outcome: RecordOutcome) {
        if self.period.current.contains_key(contract) {
            self.period.admission(contract, outcome);
            self.period_dirty = true;
        }
    }

    /// Record which focus contracts a completed probe reached a verdict on.
    pub(crate) fn note_judged(&mut self, judged: &[JudgedContract]) {
        for contract in judged {
            self.period.judged(&contract.contract);
        }
        // Verdicts are what the acceptance measure counts, and arrive about once
        // per tick, so they are persisted at once rather than at the next flush.
        if !judged.is_empty() {
            self.persist_period();
        }
    }

    /// Persist the period if merges or admissions changed it since the last write.
    ///
    /// Called from the capture writer's periodic flush (`FLUSH_EVERY`, 60 s), so
    /// merge and admission counts are written at most once a minute instead of on
    /// every observation (#5914 review round 3, A1). A restart loses at most that
    /// interval's counts, which is the tolerance `FocusPeriodSummary::complete`
    /// states.
    pub(crate) fn flush_period(&mut self) {
        if self.period_dirty {
            self.persist_period();
        }
    }

    /// Current focus contracts the sampler has admitted nothing new for this period.
    pub(crate) fn focus_without_new_samples(&self) -> usize {
        self.period.without_new_samples_now()
    }

    /// Probe ticks into the current period, `0..ROTATE_EVERY_TICKS`. Zero on the
    /// tick that rotated.
    pub(crate) fn period_tick(&self) -> u32 {
        self.ticks % ROTATE_EVERY_TICKS
    }

    /// Contracts whose merge activity is being tracked.
    pub(crate) fn activity_tracked(&self) -> usize {
        self.activity.len()
    }

    /// Activity entries evicted at the tracking cap since this runner started.
    pub(crate) fn activity_evicted(&self) -> u64 {
        self.activity.evicted()
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
    ///
    /// Within the pool, one slot is a uniform salted draw and the rest are weighted by
    /// current merge activity ([`FocusSelector::select_split`]; #5913, #5914 review
    /// D1). A contract that receives no merges while focused cannot be sampled, so a
    /// fully uniform draw over a mostly-cold hosted set judged nothing; a fully
    /// weighted one let busy decoys crowd out quiet contracts. Activity only weights
    /// the pool; it never adds to it, so focus still comes from the hosted set.
    pub(crate) fn focus(
        &mut self,
        hosted: Option<Vec<ContractInstanceId>>,
        fallback: &[ContractInstanceId],
    ) -> FocusTick {
        if self.mode == EnforcementMode::Disabled {
            return FocusTick::default();
        }
        let (candidates, source) = candidate_pool(hosted, fallback);
        if self.resumed && source == CandidateSource::SamplerFallback {
            // Just restarted, hosted source not registered yet: keep watching what the
            // period was watching rather than replacing it with sampler keys and
            // folding fallback stints into a resumed period.
            let mut current: Vec<(ContractInstanceId, FocusSlot)> = self
                .period
                .current
                .iter()
                .map(|(id, member)| (*id, member.slot))
                .collect();
            current.sort_by(|(ia, sa), (ib, sb)| {
                sa.cmp(sb).then_with(|| ia.as_bytes().cmp(ib.as_bytes()))
            });
            return FocusTick {
                selected: current.into_iter().map(|(id, _)| id).collect(),
                candidates: candidates.len(),
                active_candidates: 0,
                source,
            };
        }
        if source == CandidateSource::Hosted {
            self.resumed = false;
        }
        let activity = &self.activity;
        let picks = self.selector.select_split(
            &candidates,
            |id| activity.count(id),
            &self.period.accruing(),
        );
        let active_candidates = candidates
            .iter()
            .collect::<HashSet<_>>()
            .into_iter()
            .filter(|id| activity.count(id) > 0)
            .count();
        let entered: Vec<(ContractInstanceId, FocusSlot, u32)> = picks
            .iter()
            .map(|(id, slot)| (*id, *slot, activity.count(id)))
            .collect();
        self.period
            .enter(&entered, candidates.len(), active_candidates);
        let selected = picks.into_iter().map(|(id, _)| id).collect();
        self.persist_period();
        FocusTick {
            selected,
            candidates: candidates.len(),
            active_candidates,
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
            if !self.should_log(finding) {
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

    /// Whether `finding` gets a log line, recording it if so.
    ///
    /// One line per (contract, property) per epoch: a contract that fails the same
    /// law in forty cases is one finding reported forty times, and a log that says
    /// so forty times is harder to read, not more informative. The exception is a
    /// MORE serious finding for a key already logged. A `state_idempotence` finding
    /// that settled (report-only) and one that did not (removal-eligible) share a
    /// key, so keeping only the first arrival would log `would_remove=false` for a
    /// contract the tick's `would_remove` count includes. Same rule as
    /// `status::CheckedContract::note_finding`.
    fn should_log(&mut self, finding: &Finding) -> bool {
        let seen = LoggedSeverity::of(finding);
        match self
            .logged
            .entry((finding.contract, finding.violation.property))
        {
            std::collections::hash_map::Entry::Vacant(slot) => {
                slot.insert(seen);
                true
            }
            std::collections::hash_map::Entry::Occupied(mut slot) => {
                if seen > *slot.get() {
                    slot.insert(seen);
                    true
                } else {
                    false
                }
            }
        }
    }
}

/// How serious a logged finding was, ordered so a larger value is the one a reader
/// most needs to see: a would-remove finding outranks a report, then the settling
/// class orders findings with the same decision.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
struct LoggedSeverity {
    would_remove: bool,
    settling_rank: u8,
}

impl LoggedSeverity {
    fn of(finding: &Finding) -> Self {
        Self {
            would_remove: finding.would_remove,
            settling_rank: super::status::settling_rank(finding.violation.settling),
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

/// Count a removal-proposing decision.
///
/// One function for BOTH `WouldRemove` and `Remove` deliberately. The comment
/// between those two arms says they are handled together precisely so shadow and
/// enforcement cannot drift, and one counter is the shape in which they cannot.
fn count_would_remove(report: &mut ShadowReport) {
    report.would_remove += 1;
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

    tally_decisions(report, findings, instance_copy, outcome.decisions);
}

/// Fold one probe's decisions into the tick's counters and findings.
///
/// Split out of [`probe_one`] so the mapping from [`ConformanceAction`] to counters
/// can be tested without running WASM: it is where a report-only finding has to land
/// in `reported` rather than `would_remove`.
fn tally_decisions(
    report: &mut ShadowReport,
    findings: &mut Vec<Finding>,
    contract: ContractInstanceId,
    decisions: Vec<ConformanceAction>,
) {
    for action in decisions {
        match action {
            ConformanceAction::Nothing => {}
            ConformanceAction::Report(violation) => {
                report.reported += 1;
                // Severity from the property, as `decide` reads it.
                if violation.property.severity() == Severity::Violation {
                    report.report_only_violations += 1;
                }
                findings.push(Finding {
                    contract,
                    violation,
                    would_remove: false,
                });
            }
            ConformanceAction::WouldRemove(violation) => {
                count_would_remove(report);
                findings.push(Finding {
                    contract,
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
                count_would_remove(report);
                findings.push(Finding {
                    contract,
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

/// What survives a restart of the current focus period.
#[derive(serde::Serialize, serde::Deserialize)]
struct PersistedPeriod {
    /// Which salt drew this period's focus ([`salt_check`]), so a changed salt does
    /// not resume another identity's period (#5914 review round 3, B3).
    salt_check: [u8; 8],
    epoch: u64,
    ticks_into_period: u32,
    complete: bool,
    members: Vec<(ContractInstanceId, PeriodMember)>,
    totals: FocusPeriodSummary,
}

/// A short, non-secret fingerprint of the focus salt for [`PersistedPeriod`].
///
/// Domain-separated and truncated, so the period file reveals nothing usable about
/// the salt itself, which is the secret that keeps focus unpredictable.
fn salt_check(salt: &[u8; 32]) -> [u8; 8] {
    let mut hasher = blake3::Hasher::new();
    hasher.update(b"freenet-conformance-period-salt-check-v1");
    hasher.update(salt);
    let mut check = [0u8; 8];
    check.copy_from_slice(&hasher.finalize().as_bytes()[..8]);
    check
}

/// Read the persisted period for `epoch` under `salt`. `None` when absent,
/// unreadable, for a different epoch or salt, or holding more members than there are
/// focus slots; the caller then starts an incomplete period.
fn load_period(dir: &Path, epoch: u64, salt: &[u8; 32]) -> Option<PersistedPeriod> {
    let raw = std::fs::read(dir.join(PERIOD_FILE)).ok()?;
    let saved: PersistedPeriod = match serde_json::from_slice(&raw) {
        Ok(saved) => saved,
        Err(err) => {
            tracing::warn!(
                error = %err,
                "conformance focus period file is unreadable; starting an incomplete period"
            );
            return None;
        }
    };
    if saved.members.len() > MAX_FOCUS_CONTRACTS {
        tracing::warn!(
            members = saved.members.len(),
            "conformance focus period file holds more members than focus slots; \
             starting an incomplete period"
        );
        return None;
    }
    (saved.epoch == epoch && saved.salt_check == salt_check(salt)).then_some(saved)
}

/// Persist the current period beside the epoch, returning whether it was written.
/// Best-effort like the epoch: a peer that cannot write it reports its next period
/// after a restart as incomplete, and retries at the next flush.
fn store_period(dir: &Path, period: &PersistedPeriod) -> bool {
    let path = dir.join(PERIOD_FILE);
    // Written to a temporary file and renamed over the old one, so a crash mid-write
    // leaves the previous checkpoint rather than a torn file (#5914 review round 3,
    // B4). Owner-only like the salt and epoch: it names the contracts this peer
    // watches.
    let staging = dir.join(format!("{PERIOD_FILE}.tmp"));
    let written = serde_json::to_vec(period)
        .map_err(std::io::Error::other)
        .and_then(|bytes| {
            crate::wasm_runtime::create_owner_only(&staging).and_then(|mut file| {
                use std::io::Write;
                // No fsync: the rename protects against a crashed process, which is
                // the case that matters; after a power loss an unreadable file just
                // starts an incomplete period. The checkpoint is written at every
                // draw and tick, so an fsync each time would cost more than it saves.
                file.write_all(&bytes)
            })
        })
        .and_then(|()| std::fs::rename(&staging, &path));
    match written {
        Ok(()) => true,
        Err(err) => {
            tracing::warn!(
                path = %path.display(),
                error = %err,
                "could not persist the conformance focus period; a restart will start an \
                 incomplete one"
            );
            false
        }
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
    let mut runner = ShadowRunner::new(dir.path(), EnforcementMode::Shadow);
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
        ConformanceProperty, IdempotenceSettling, OutputDigest, PropertyOutcome, Violation, decide,
    };

    fn violation_of(
        property: ConformanceProperty,
        settling: Option<IdempotenceSettling>,
    ) -> PropertyOutcome {
        PropertyOutcome::Violated(Violation {
            property,
            severity: property.severity(),
            left: OutputDigest::of(&[1u8]),
            right: OutputDigest::of(&[2u8]),
            detail: "test".to_string(),
            settling,
        })
    }

    /// A violation of `property` whose carried severity field says `severity`.
    fn forged(property: ConformanceProperty, severity: Severity) -> PropertyOutcome {
        let PropertyOutcome::Violated(violation) = violation_of(property, None) else {
            unreachable!("violation_of builds a violation")
        };
        PropertyOutcome::Violated(Violation {
            severity,
            ..violation
        })
    }

    fn tally(outcomes: &[PropertyOutcome]) -> (ShadowReport, Vec<Finding>) {
        let mut report = ShadowReport::default();
        let mut findings = Vec::new();
        let decisions = outcomes
            .iter()
            .map(|o| decide(EnforcementMode::Shadow, o))
            .collect();
        tally_decisions(
            &mut report,
            &mut findings,
            ContractInstanceId::new([9u8; 32]),
            decisions,
        );
        (report, findings)
    }

    /// Report-only findings land in `reported` and never in `would_remove`, through
    /// the same `decide` and tally the probe uses. `would_remove` is the number the
    /// enforcement gate is judged on, so this is the property the tier exists for.
    #[test]
    fn report_only_findings_never_reach_would_remove_in_shadow() {
        let report_only = [
            violation_of(
                ConformanceProperty::StateIdempotence,
                Some(IdempotenceSettling::SettledAfter(1)),
            ),
            violation_of(ConformanceProperty::TransitionPathAgreement, None),
            violation_of(ConformanceProperty::DeltaPermutationInvariance, None),
            violation_of(ConformanceProperty::PathAgreement, None),
            violation_of(ConformanceProperty::DeltaIdempotence, None),
            violation_of(ConformanceProperty::SelfDeltaEmpty, None),
            violation_of(ConformanceProperty::WholeStateSelfDelta, None),
        ];
        let (report, findings) = tally(&report_only);
        assert_eq!(report.would_remove, 0, "{findings:?}");
        assert_eq!(report.reported, report_only.len());
        // The first four are law breaks of a report-only tier, the last three
        // diagnostics: only the four are report-only violations.
        assert_eq!(report.report_only_violations, 4, "{findings:?}");

        // Forged severity fields, each direction tallied ON ITS OWN (together,
        // reading the field would lose one and gain one, and the total would not
        // move). `report_only_violations` reads severity from the property, as
        // `decide` does.
        let (report, _) = tally(&[forged(
            ConformanceProperty::PathAgreement,
            Severity::Diagnostic,
        )]);
        assert_eq!(
            report.report_only_violations, 1,
            "a law break whose field says Diagnostic"
        );
        let (report, _) = tally(&[forged(
            ConformanceProperty::DeltaIdempotence,
            Severity::Violation,
        )]);
        assert_eq!(
            report.report_only_violations, 0,
            "a diagnostic whose field says Violation"
        );
        assert!(findings.iter().all(|f| !f.would_remove));
    }

    /// The counterpart: removal-eligible findings still reach `would_remove`, an
    /// idempotence finding not shown to settle included. Without it, a tally that
    /// counted everything as reported would pass the test above.
    #[test]
    fn removal_eligible_findings_still_reach_would_remove_in_shadow() {
        let eligible = [
            violation_of(
                ConformanceProperty::StateIdempotence,
                Some(IdempotenceSettling::NeverSettled),
            ),
            violation_of(
                ConformanceProperty::StateIdempotence,
                Some(IdempotenceSettling::Indeterminate),
            ),
            violation_of(ConformanceProperty::StateCommutativity, None),
        ];
        let (report, findings) = tally(&eligible);
        assert_eq!(report.would_remove, eligible.len(), "{findings:?}");
        assert_eq!(report.reported, 0);
        assert_eq!(report.report_only_violations, 0);
        assert!(findings.iter().all(|f| f.would_remove));
    }

    fn finding(settling: IdempotenceSettling, would_remove: bool) -> Finding {
        let PropertyOutcome::Violated(violation) =
            violation_of(ConformanceProperty::StateIdempotence, Some(settling))
        else {
            unreachable!("violation_of builds a violation")
        };
        Finding {
            contract: ContractInstanceId::new([9u8; 32]),
            violation,
            would_remove,
        }
    }

    /// The finding log keeps one line per (contract, property) per epoch, but a
    /// later, more serious finding for the same key is logged too. Otherwise a
    /// settled (report-only) finding arriving first hides a non-settling
    /// (removal-eligible) one, and the log says `would_remove=false` for a contract
    /// the tick's `would_remove` count includes. With the decision equal, the
    /// higher settling rank wins, as in `status::CheckedContract::note_finding`.
    #[test]
    fn the_finding_log_does_not_let_a_report_only_finding_hide_a_removal() {
        let settled = finding(IdempotenceSettling::SettledAfter(1), false);
        let never = finding(IdempotenceSettling::NeverSettled, true);

        let dir = tempfile::TempDir::new().expect("temp dir");
        let mut runner = ShadowRunner::new(dir.path(), EnforcementMode::Shadow);
        assert!(runner.should_log(&settled), "first finding for the key");
        assert!(
            runner.should_log(&never),
            "a removal-eligible finding after a report-only one must be logged"
        );
        assert!(!runner.should_log(&never), "a repeat is not logged again");
        assert!(
            !runner.should_log(&settled),
            "a less serious finding after a more serious one is not logged"
        );

        let dir = tempfile::TempDir::new().expect("temp dir");
        let mut runner = ShadowRunner::new(dir.path(), EnforcementMode::Shadow);
        assert!(runner.should_log(&never));
        assert!(
            !runner.should_log(&settled),
            "arriving second, the settled finding adds nothing to the log"
        );

        // Same decision, higher settling rank: an `Indeterminate` line must not
        // hide a later `NeverSettled` one, the class enforcement can act on.
        let indeterminate = finding(IdempotenceSettling::Indeterminate, true);
        let dir = tempfile::TempDir::new().expect("temp dir");
        let mut runner = ShadowRunner::new(dir.path(), EnforcementMode::Shadow);
        assert!(runner.should_log(&indeterminate));
        assert!(
            runner.should_log(&never),
            "a higher settling rank with the same decision must be logged"
        );
        assert!(!runner.should_log(&indeterminate));
    }

    /// The same rule through the shipping entry point, `record`, read off the log
    /// lines it emits: a test of `should_log` alone passes if `record` stops
    /// consulting it.
    #[test]
    fn record_logs_a_later_removal_after_a_report_only_finding() {
        let settled = finding(IdempotenceSettling::SettledAfter(1), false);
        let never = finding(IdempotenceSettling::NeverSettled, true);
        let dir = tempfile::TempDir::new().expect("temp dir");
        let mut runner = ShadowRunner::new(dir.path(), EnforcementMode::Shadow);

        let (logs, guard) = crate::util::test_log_capture::install();
        runner.record(&[settled.clone(), never.clone(), never, settled]);
        drop(guard);
        let logs = logs.lock().unwrap().clone();
        let lines: Vec<&String> = logs
            .iter()
            .filter(|l| l.contains("conformance shadow finding"))
            .collect();
        assert_eq!(lines.len(), 2, "one line per first-seen severity: {logs:?}");
        assert!(lines[0].contains("would_remove=false"), "{lines:?}");
        assert!(lines[1].contains("would_remove=true"), "{lines:?}");
    }

    /// Each decision increments the total exactly once.
    ///
    /// Added after mutation: incrementing `would_remove` again alongside the shared
    /// helper survived an earlier version of these tests. A drifting total is the
    /// specific thing the shared helper exists to prevent.
    #[test]
    fn a_single_decision_increments_the_total_exactly_once() {
        let mut report = ShadowReport::default();
        count_would_remove(&mut report);
        assert_eq!(report.would_remove, 1);
    }

    /// Pin: BOTH removal-proposing arms route through the one counter.
    ///
    /// The comment between them says they are handled together so shadow and
    /// enforcement cannot drift. A test of `count_would_remove` alone cannot see an
    /// arm that stopped calling it, which is how the total would silently start
    /// under-counting the moment enforcement became reachable.
    #[test]
    fn both_removal_arms_route_through_the_counter() {
        // Bounded by brace-counting to `tally_decisions`'s OWN body, not by splitting on
        // its signature and reading to end-of-file. An unbounded region silently
        // widens to swallow unrelated code — including this test module — and then
        // passes for the wrong reason. That is the trap the repo documents on its
        // other scrapes, and my first version of this pin walked into it.
        let src = include_str!("shadow.rs");
        let start = src
            .find("\nfn tally_decisions(")
            .expect("tally_decisions not found");
        let after = &src[start..];
        let open = after.find('{').expect("tally_decisions has no body");
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
        assert!(end > open, "could not bound tally_decisions's body");
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
        tick_with_findings(runner, samplers, store).await.0
    }

    /// [`tick`], also returning the findings the probe handed to `record`.
    async fn tick_with_findings(
        runner: &mut ShadowRunner,
        samplers: &HashMap<ContractInstanceId, TrackedContract>,
        store: Option<&Path>,
    ) -> (ShadowReport, Vec<Finding>) {
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
        (report, findings)
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
        let mut runner = ShadowRunner::new(dir.path(), EnforcementMode::Shadow);
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

    /// A result-state digest no other call returns: "a merge that produced a new
    /// state".
    fn fresh_state() -> crate::conformance::activity::StateDigest {
        static NEXT: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(1);
        u128::from(NEXT.fetch_add(1, std::sync::atomic::Ordering::Relaxed))
    }

    /// Toggling between two states through the runner counts each state once.
    #[test]
    fn a_toggling_contract_gains_no_weight_after_its_first_two_states() {
        let dir = tempfile::TempDir::new().expect("tempdir");
        let mut runner = runner_with_salt(dir.path(), [31; 32]);
        let (a, b) = (fresh_state(), fresh_state());
        for i in 0..40 {
            runner.note_merge(instance(1), Some(if i % 2 == 0 { a } else { b }));
        }
        assert_eq!(runner.activity.count(&instance(1)), 2);
    }

    fn instance(n: u8) -> ContractInstanceId {
        ContractInstanceId::new([n; 32])
    }

    // ------------------------------------------------- activity-weighted focus (#5913)

    /// A runner whose salt is fixed, so a test of the wiring is deterministic rather
    /// than depending on whatever `OsRng` drew for this temp dir.
    fn runner_with_salt(dir: &Path, salt: [u8; 32]) -> ShadowRunner {
        std::fs::write(dir.join(SALT_FILE), salt).expect("write salt");
        ShadowRunner::new(dir, EnforcementMode::Shadow)
    }

    /// 60 hosted contracts, of which the first ten are busy.
    fn hosted_with_busy() -> (Vec<ContractInstanceId>, Vec<ContractInstanceId>) {
        let hosted: Vec<_> = (0..60u8).map(instance).collect();
        let busy = hosted[..10].to_vec();
        (hosted, busy)
    }

    /// A salt for which the focus draw over `hosted_with_busy` misses every busy
    /// contract when nothing is active, and hits one once the busy ones are. Searched
    /// for rather than hard-coded, so the precondition is checked rather than assumed.
    fn salt_where_activity_changes_the_draw() -> [u8; 32] {
        let (hosted, busy) = hosted_with_busy();
        (0..=255u8)
            .map(|n| [n; 32])
            .find(|salt| {
                let selector = FocusSelector::new(*salt, MAX_FOCUS_CONTRACTS);
                let quiet = selector.select_split(&hosted, |_| 0, &[]);
                let busy_draw =
                    selector.select_split(&hosted, |c| if busy.contains(c) { 40 } else { 0 }, &[]);
                quiet.iter().all(|(c, _)| !busy.contains(c))
                    && busy_draw.iter().any(|(c, _)| busy.contains(c))
            })
            .expect("no salt in 256 where activity changes the draw")
    }

    /// The wiring, end to end through the runner: merges counted by `note_merge`
    /// reach focus at the next probe tick, and focus then lands on a busy contract
    /// that the uniform draw for the same salt and epoch missed.
    ///
    /// This is also the restart path: a fresh runner has no activity, selects
    /// uniformly, and weights from its first probe tick rather than a period later.
    #[test]
    fn merges_observed_before_a_probe_tick_steer_focus_toward_busy_contracts() {
        let dir = tempfile::TempDir::new().expect("tempdir");
        let mut runner = runner_with_salt(dir.path(), salt_where_activity_changes_the_draw());
        let (hosted, busy) = hosted_with_busy();

        let before = runner.focus(Some(hosted.clone()), &[]);
        assert!(
            before.selected.iter().all(|c| !busy.contains(c)),
            "precondition: the uniform draw already includes a busy contract"
        );
        assert_eq!(before.active_candidates, 0, "no merges seen yet");

        for contract in &busy {
            for _ in 0..40 {
                runner.note_merge(*contract, Some(fresh_state()));
            }
        }
        assert!(runner.advance().is_none(), "the first tick must not rotate");
        let after = runner.focus(Some(hosted.clone()), &[]);

        assert_eq!(runner.epoch(), 0, "same epoch: only the weights changed");
        assert_eq!(after.active_candidates, busy.len());
        assert!(
            after.selected.iter().any(|c| busy.contains(c)),
            "ten of sixty hosted contracts were merging and focus picked neither of \
             them: activity observed by the runner is not reaching selection"
        );
    }

    /// Activity decays at each period boundary, so a contract that went quiet stops
    /// being favoured instead of holding its weight forever.
    #[test]
    fn activity_decays_once_per_period() {
        let dir = tempfile::TempDir::new().expect("tempdir");
        let mut runner = runner_with_salt(dir.path(), [5; 32]);
        for _ in 0..8 {
            runner.note_merge(instance(1), Some(fresh_state()));
        }
        for _ in 0..ROTATE_EVERY_TICKS - 1 {
            runner.advance();
        }
        assert_eq!(runner.activity.count(&instance(1)), 8, "decayed mid-period");
        runner.advance();
        assert_eq!(
            runner.activity.count(&instance(1)),
            4,
            "one period halves it"
        );
        for _ in 0..ROTATE_EVERY_TICKS * 3 {
            runner.advance();
        }
        assert_eq!(
            runner.activity_tracked(),
            0,
            "a contract with no merges for four periods is still tracked"
        );
    }

    /// A salt for which a burst on `burst`, starting after the period's first draw,
    /// wins the weighted slot at the next tick, while that first draw (made from the
    /// pre-burst activity) missed every burst contract.
    fn salt_where_a_burst_wins_the_weighted_slot(
        hosted: &[ContractInstanceId],
        before: &impl Fn(&ContractInstanceId) -> u32,
        during: &impl Fn(&ContractInstanceId) -> u32,
        burst: &[ContractInstanceId],
    ) -> [u8; 32] {
        (0..=255u8)
            .map(|n| [n; 32])
            .find(|salt| {
                let selector = FocusSelector::new(*salt, MAX_FOCUS_CONTRACTS);
                let first = selector.select_split(hosted, before, &[]);
                let next = selector.select_split(hosted, during, &[]);
                first.iter().all(|(c, _)| !burst.contains(c))
                    && next.iter().any(|(c, _)| burst.contains(c))
            })
            .expect("no salt in 256 has a burst win the weighted slot")
    }

    /// The bursty-evasion inversion (#5914 review D1): weights frozen at the start
    /// of a period could not see a contract that was quiet before the period and
    /// bursts during it, so it was focused LESS than under a uniform draw exactly
    /// while it had the most to check. Activity must reach the weighted slot at the
    /// next probe tick.
    #[test]
    fn a_burst_during_the_period_reaches_the_weighted_slot_at_the_next_tick() {
        let hosted: Vec<_> = (0..60u8).map(instance).collect();
        let burst: Vec<_> = (40..50u8).map(instance).collect();
        let before = |c: &ContractInstanceId| u32::from(*c == instance(59)) * 5;
        let during = |c: &ContractInstanceId| {
            if burst.contains(c) { 60 } else { before(c) }
        };
        let salt = salt_where_a_burst_wins_the_weighted_slot(&hosted, &before, &during, &burst);
        let dir = tempfile::TempDir::new().expect("tempdir");
        let mut runner = runner_with_salt(dir.path(), salt);

        for _ in 0..5 {
            runner.note_merge(instance(59), Some(fresh_state()));
        }
        runner.advance();
        let first = runner.focus(Some(hosted.clone()), &[]).selected;
        assert!(first.iter().all(|c| !burst.contains(c)), "precondition");

        for contract in &burst {
            for _ in 0..60 {
                runner.note_merge(*contract, Some(fresh_state()));
            }
        }
        assert!(runner.advance().is_none(), "still mid-period");
        let next = runner.focus(Some(hosted.clone()), &[]).selected;
        assert!(
            next.iter().any(|c| burst.contains(c)),
            "ten contracts started merging mid-period and the next tick still did not \
             focus on any of them: {next:?}"
        );
    }

    /// A uniform-slot contract that is accruing samples is not displaced mid-period
    /// when the peer starts hosting a contract that outscores it: moving it would
    /// abandon a sample being built.
    #[test]
    fn an_accruing_uniform_contract_is_not_displaced_by_a_new_candidate() {
        let all: Vec<_> = (0..61u8).map(instance).collect();
        let mut checked = 0;
        for salt in 0..64u8 {
            // A salt for which the contract the peer starts hosting later outscores
            // the uniform pick it has now.
            let selector = FocusSelector::new([salt; 32], MAX_FOCUS_CONTRACTS);
            let (newcomer, hosted) = (all[60], all[..60].to_vec());
            if selector.select(&all)[0] != newcomer {
                continue;
            }
            checked += 1;
            let dir = tempfile::TempDir::new().expect("tempdir");
            let mut runner = runner_with_salt(dir.path(), [salt; 32]);
            runner.advance();
            let uniform = runner.focus(Some(hosted.clone()), &[]).selected[0];
            runner.note_merge(uniform, Some(fresh_state()));
            runner.note_admission(&uniform, RecordOutcome::Admitted);
            runner.advance();
            let now = runner.focus(Some(all.clone()), &[]).selected;
            assert_eq!(
                now[0], uniform,
                "salt {salt}: an accruing uniform contract was displaced by a newcomer"
            );
        }
        assert!(checked > 0, "no salt exercised the case");
    }

    /// A focus contract is kept only while it keeps accruing: one that admitted a
    /// sample early in the period and nothing since is eligible to be re-drawn at the
    /// next tick (#5914 review round 2, K1). Under keep-if-ever-admitted, one early
    /// sample locked the weighted slot for the rest of the period against any burst.
    #[test]
    fn a_focus_contract_that_stopped_accruing_is_not_kept() {
        let dir = tempfile::TempDir::new().expect("tempdir");
        let mut runner = runner_with_salt(dir.path(), [21; 32]);
        let hosted: Vec<_> = (0..60u8).map(instance).collect();
        runner.advance();
        let first = runner.focus(Some(hosted.clone()), &[]).selected;
        let weighted = first[1];
        runner.note_merge(weighted, Some(fresh_state()));
        runner.note_admission(&weighted, RecordOutcome::Admitted);

        runner.advance();
        assert!(
            runner.period.accruing().iter().any(|(c, _)| *c == weighted),
            "a contract that admitted a sample in the last interval was not kept"
        );
        runner.focus(Some(hosted.clone()), &[]);

        // A whole interval with nothing new.
        runner.advance();
        assert!(
            runner.period.accruing().iter().all(|(c, _)| *c != weighted),
            "a contract that admitted nothing in the last interval is still locked in"
        );
    }

    /// Busy decoys plus a contract that bursts mid-period, through the real runner
    /// (#5914 review round 2, K1; modelled on the skeptical lens's simulation).
    ///
    /// Ten decoys merge (and are sampled) five times every tick, so under
    /// keep-while-accruing they hold the weighted slot for good; the target bursts at
    /// thirty merges a tick for four ticks in the middle of the third period. (At ten
    /// a tick the target's advantage over the uniform pair is real but within noise at
    /// 256 salts: the decoys sit at the weight cap, so a burster's weighted chance is
    /// about its weight over ten capped weights.) The design must focus the target during its burst
    /// on more salts than the uniform pair it replaced, or busy decoys have bought a
    /// blind spot.
    #[test]
    fn a_mid_period_burst_beats_busy_decoys_to_the_weighted_slot() {
        let hosted: Vec<_> = (0..60u8).map(instance).collect();
        let target = hosted[0];
        let decoys: Vec<_> = hosted[1..=10].to_vec();
        let (burst_from, burst_len) = (18u32, 4u32);
        let (mut caught, mut uniform_caught) = (0usize, 0usize);
        for salt in 0..=255u8 {
            let dir = tempfile::TempDir::new().expect("tempdir");
            let mut runner = runner_with_salt(dir.path(), [salt; 32]);
            let (mut hit, mut uniform_hit) = (false, false);
            for tick in 1..=burst_from + burst_len {
                runner.advance();
                let focus = runner.focus(Some(hosted.clone()), &[]).selected;
                let uniform_pair =
                    FocusSelector::resuming_at([salt; 32], MAX_FOCUS_CONTRACTS, runner.epoch())
                        .select(&hosted);
                let bursting = tick >= burst_from;
                let mut merges: Vec<ContractInstanceId> = decoys
                    .iter()
                    .flat_map(|d| std::iter::repeat_n(*d, 5))
                    .collect();
                if bursting {
                    merges.extend(std::iter::repeat_n(target, 30));
                    hit |= focus.contains(&target);
                    uniform_hit |= uniform_pair.contains(&target);
                }
                for contract in merges {
                    runner.note_merge(contract, Some(fresh_state()));
                    let outcome = if focus.contains(&contract) {
                        RecordOutcome::Admitted
                    } else {
                        RecordOutcome::OutOfScope
                    };
                    runner.note_admission(&contract, outcome);
                }
            }
            caught += usize::from(hit);
            uniform_caught += usize::from(uniform_hit);
        }
        assert!(
            caught > uniform_caught,
            "the burst was focused on {caught}/256 salts against {uniform_caught}/256 for \
             the uniform pair: busy decoys are locking the weighted slot"
        );
    }

    /// The tick-line telemetry the runner reports (S4): `period_tick` counts ticks
    /// into the period and is zero on the rotation tick; `activity_tracked` and
    /// `activity_evicted` are the tracker's own numbers.
    #[test]
    fn the_tick_telemetry_reports_the_runners_own_numbers() {
        let dir = tempfile::TempDir::new().expect("tempdir");
        let mut runner = runner_with_salt(dir.path(), [41; 32]);
        for tick in 1..ROTATE_EVERY_TICKS {
            runner.advance();
            assert_eq!(runner.period_tick(), tick);
        }
        assert!(runner.advance().is_some());
        assert_eq!(runner.period_tick(), 0, "the rotation tick is tick zero");

        let cap = crate::conformance::activity::MAX_TRACKED_ACTIVITY;
        for n in 0..=cap {
            let mut bytes = [0u8; 32];
            bytes[..8].copy_from_slice(&(n as u64).to_be_bytes());
            runner.note_merge(ContractInstanceId::new(bytes), Some(fresh_state()));
        }
        assert!(
            runner.activity_evicted() > 0,
            "one past the cap evicted nothing"
        );
        assert_eq!(
            runner.activity_tracked() as u64 + runner.activity_evicted(),
            cap as u64 + 1
        );
    }

    /// The runner breaks activity-eviction ties with ITS salt (F26): two peers with
    /// different salts evict different equally-quiet contracts.
    #[test]
    fn eviction_ties_use_the_peers_salt() {
        let cap = crate::conformance::activity::MAX_TRACKED_ACTIVITY;
        let ids: Vec<_> = (0..=cap)
            .map(|n| {
                let mut bytes = [0u8; 32];
                bytes[..8].copy_from_slice(&(n as u64).to_be_bytes());
                ContractInstanceId::new(bytes)
            })
            .collect();
        let survivors = |salt: u8| {
            let dir = tempfile::TempDir::new().expect("tempdir");
            let mut runner = runner_with_salt(dir.path(), [salt; 32]);
            for id in &ids {
                runner.note_merge(*id, Some(fresh_state()));
            }
            ids.iter()
                .filter(|id| runner.activity.count(id) > 0)
                .copied()
                .collect::<HashSet<_>>()
        };
        assert_ne!(
            survivors(1),
            survivors(2),
            "two salts evicted the same tied contracts: the runner's salt is not reaching \
             the tracker"
        );
    }

    /// Activity on contracts the peer does not host can neither be selected nor
    /// stop candidates' activity from counting (#5914 review T5: a stray merge on a
    /// non-candidate used to freeze the period as uniform).
    #[test]
    fn activity_outside_the_candidates_does_not_mask_candidate_activity() {
        let dir = tempfile::TempDir::new().expect("tempdir");
        let mut runner = runner_with_salt(dir.path(), [23; 32]);
        let hosted: Vec<_> = (0..20u8).map(instance).collect();
        runner.note_merge(instance(200), Some(fresh_state()));
        runner.advance();
        let first = runner.focus(Some(hosted.clone()), &[]);
        assert_eq!(
            first.active_candidates, 0,
            "a non-candidate counted as active"
        );
        assert!(!first.selected.contains(&instance(200)));

        for _ in 0..10 {
            runner.note_merge(instance(3), Some(fresh_state()));
        }
        runner.advance();
        let next = runner.focus(Some(hosted.clone()), &[]);
        assert_eq!(
            next.active_candidates, 1,
            "a candidate's activity was ignored because a non-candidate had merged first"
        );
    }

    /// A merge that cannot count as activity (no state change, or too large to
    /// sample) is not recorded at all.
    #[test]
    fn a_merge_that_is_not_activity_does_not_raise_weight() {
        let dir = tempfile::TempDir::new().expect("tempdir");
        let mut runner = runner_with_salt(dir.path(), [13; 32]);
        for _ in 0..100 {
            runner.note_merge(instance(1), None);
        }
        assert_eq!(runner.activity_tracked(), 0);
    }

    /// Feed a focus member `seen` merges with the given admission outcomes, the way
    /// `capture::run_writer` does: `note_merge` first, then `note_admission`.
    fn feed(runner: &mut ShadowRunner, contract: ContractInstanceId, outcomes: &[RecordOutcome]) {
        for outcome in outcomes {
            runner.note_merge(contract, Some(fresh_state()));
            runner.note_admission(&contract, *outcome);
        }
    }

    fn end_period(runner: &mut ShadowRunner) -> FocusPeriodSummary {
        let mut summary = None;
        for _ in 0..ROTATE_EVERY_TICKS {
            summary = summary.or(runner.advance());
        }
        summary.expect("the period never ended")
    }

    /// Every field of the period summary comes from its own source.
    ///
    /// The scenario gives the fields different values wherever it can (the review's
    /// surviving mutant was a field fed from another counter); where two fields
    /// legitimately coincide, the log-line pin below still catches a swap.
    #[test]
    fn the_period_summary_counts_each_field_from_its_own_source() {
        use RecordOutcome::{Admitted, NothingNew, Refused};
        let dir = tempfile::TempDir::new().expect("tempdir");
        let mut runner = runner_with_salt(dir.path(), [7; 32]);
        let hosted: Vec<_> = (0..20u8).map(instance).collect();
        let focus = runner.focus(Some(hosted.clone()), &[]);
        // Uniform slot first, then weighted.
        let (a, b) = (focus.selected[0], focus.selected[1]);

        // a: 6 seen, 3 admitted, 2 refused, 1 nothing new; judged.
        feed(
            &mut runner,
            a,
            &[Admitted, Admitted, Admitted, Refused, Refused, NothingNew],
        );
        // b: 4 seen, nothing admitted; judged from retained samples anyway.
        feed(
            &mut runner,
            b,
            &[NothingNew, NothingNew, NothingNew, NothingNew],
        );
        runner.note_judged(&[
            JudgedContract {
                contract: a,
                verdicts: 3,
                inconclusive: 0,
            },
            JudgedContract {
                contract: b,
                verdicts: 1,
                inconclusive: 0,
            },
        ]);
        assert_eq!(runner.focus_without_new_samples(), 1, "b has nothing new");

        // Every other contract becomes active, then b stops being hosted: a is kept
        // (it is accruing samples), the weighted slot is redrawn to some c.
        for contract in hosted.iter().filter(|c| **c != a && **c != b) {
            for _ in 0..7 {
                runner.note_merge(*contract, Some(fresh_state()));
            }
        }
        let without_b: Vec<_> = hosted.iter().copied().filter(|c| *c != b).collect();
        let second = runner.focus(Some(without_b), &[]).selected;
        assert_eq!(second[0], a, "a contract accruing samples was displaced");
        let c = second[1];
        feed(&mut runner, c, &[Admitted, Refused, Refused, Refused]);

        let summary = end_period(&mut runner);
        assert_eq!(
            summary,
            FocusPeriodSummary {
                epoch: 0,
                complete: false,
                candidates: 19,
                active_candidates: 19,
                stints: 3,
                merges_seen: 14,
                samples_admitted: 4,
                samples_refused: 5,
                stints_without_new_samples: 1,
                judged: 2,
                uniform_stints: 1,
                judged_uniform: 1,
                weighted_stints: 2,
                judged_weighted: 1,
                weighted_selected_activity: 7,
            }
        );
    }

    fn judged(contract: ContractInstanceId) -> JudgedContract {
        JudgedContract {
            contract,
            verdicts: 1,
            inconclusive: 0,
        }
    }

    /// A summary covers one period: the next starts from zero.
    #[test]
    fn each_period_summary_covers_only_its_own_period() {
        let dir = tempfile::TempDir::new().expect("tempdir");
        let mut runner = runner_with_salt(dir.path(), [7; 32]);
        let hosted: Vec<_> = (0..20u8).map(instance).collect();
        let first_focus = runner.focus(Some(hosted.clone()), &[]).selected;
        feed(
            &mut runner,
            first_focus[0],
            &[RecordOutcome::Admitted, RecordOutcome::Refused],
        );
        let first = end_period(&mut runner);
        assert_eq!((first.merges_seen, first.stints), (2, 2));

        runner.focus(Some(hosted.clone()), &[]);
        let second = end_period(&mut runner);
        assert_eq!(
            (
                second.merges_seen,
                second.samples_admitted,
                second.samples_refused,
                second.stints
            ),
            (0, 0, 0, 2),
            "the second period's summary still carries the first period's counts"
        );
        assert!(second.complete);
    }

    /// `judged_uniform` and `judged_weighted` say WHICH slot produced a verdict, which
    /// is what the uniform/weighted split exists to measure.
    #[test]
    fn a_verdict_is_attributed_to_the_slot_that_was_judged() {
        for judged_index in [0usize, 1] {
            let dir = tempfile::TempDir::new().expect("tempdir");
            let mut runner = runner_with_salt(dir.path(), [7; 32]);
            let hosted: Vec<_> = (0..20u8).map(instance).collect();
            let selected = runner.focus(Some(hosted), &[]).selected;
            // selected[0] is the uniform slot, selected[1] the weighted one.
            runner.note_judged(&[judged(selected[judged_index])]);
            let summary = end_period(&mut runner);
            let expected = if judged_index == 0 { (1, 0) } else { (0, 1) };
            assert_eq!(
                (summary.judged_uniform, summary.judged_weighted),
                expected,
                "judged slot {judged_index}"
            );
        }
    }

    /// `weighted_selected_activity` is the activity of WEIGHTED stints at the moment
    /// they were drawn: not the uniform stints', and not refreshed while the stint
    /// continues.
    #[test]
    fn weighted_selected_activity_is_the_weighted_stints_activity_when_drawn() {
        let dir = tempfile::TempDir::new().expect("tempdir");
        let mut runner = runner_with_salt(dir.path(), [7; 32]);
        let hosted: Vec<_> = (0..20u8).map(instance).collect();
        for contract in &hosted {
            for _ in 0..5 {
                runner.note_merge(*contract, Some(fresh_state()));
            }
        }
        let first = runner.focus(Some(hosted.clone()), &[]).selected;
        let weighted = first[1];
        // The weighted pick keeps winning after it gets much busier, so its stint continues.
        for _ in 0..60 {
            runner.note_merge(weighted, Some(fresh_state()));
        }
        let second = runner.focus(Some(hosted), &[]).selected;
        assert!(
            second.contains(&weighted),
            "precondition: the stint continues"
        );
        let summary = end_period(&mut runner);
        assert_eq!(
            summary.weighted_selected_activity, 5,
            "uniform stints' activity leaked in, or a continuing stint's was refreshed"
        );
    }

    /// Keep-if-accruing keys on what the sampler ADMITTED, not on merges merely seen:
    /// a focus contract whose merges all added nothing new is re-drawn.
    #[test]
    fn a_focus_contract_whose_merges_added_nothing_is_displaced() {
        let dir = tempfile::TempDir::new().expect("tempdir");
        let mut runner = runner_with_salt(dir.path(), [21; 32]);
        let hosted: Vec<_> = (0..60u8).map(instance).collect();
        let first = runner.focus(Some(hosted.clone()), &[]).selected;
        let weighted = first[1];
        for _ in 0..4 {
            runner.note_merge(weighted, Some(fresh_state()));
            runner.note_admission(&weighted, RecordOutcome::NothingNew);
        }
        for contract in hosted.iter().filter(|c| !first.contains(c)) {
            for _ in 0..100 {
                runner.note_merge(*contract, Some(fresh_state()));
            }
        }
        let now = runner.focus(Some(hosted), &[]).selected;
        assert_eq!(now[0], first[0], "the uniform slot does not move");
        assert_ne!(
            now[1], weighted,
            "a weighted contract that admitted nothing new was kept anyway"
        );
    }

    /// A continuing stint takes the slot of the draw that keeps it (#5914 review
    /// round 2, K4). When the peer stops hosting the uniform pick, the weighted pick
    /// can become the new uniform pick; leaving it labelled Weighted gave the period
    /// two Weighted members, so once both accrued, the split could keep only one and
    /// displaced an accruing contract.
    #[test]
    fn a_continuing_stint_takes_its_new_slot() {
        let hosted: Vec<_> = (0..10u8).map(instance).collect();
        let mut checked = 0;
        for salt in 0..=255u8 {
            let dir = tempfile::TempDir::new().expect("tempdir");
            let mut runner = runner_with_salt(dir.path(), [salt; 32]);
            let first = runner.focus(Some(hosted.clone()), &[]).selected;
            let (uniform, weighted) = (first[0], first[1]);
            let without_uniform: Vec<_> =
                hosted.iter().copied().filter(|c| *c != uniform).collect();
            let second = runner.focus(Some(without_uniform.clone()), &[]).selected;
            if second[0] != weighted {
                continue;
            }
            checked += 1;
            assert_eq!(
                runner.period.current.get(&weighted).map(|m| m.slot),
                Some(FocusSlot::Uniform),
                "salt {salt}: a stint that moved to the uniform slot kept its old label"
            );
            // Both accrue through an interval; neither may be displaced at the tick.
            for contract in &second {
                runner.note_merge(*contract, Some(fresh_state()));
                runner.note_admission(contract, RecordOutcome::Admitted);
            }
            runner.advance();
            let third = runner.focus(Some(without_uniform), &[]).selected;
            assert!(
                second.iter().all(|c| third.contains(c)),
                "salt {salt}: an accruing contract was displaced: {second:?} -> {third:?}"
            );
        }
        assert!(
            checked > 0,
            "no salt exercised the case; this test proves nothing"
        );
    }

    /// The first period on a fresh capture directory is incomplete; later ones are
    /// complete, including across a restart, which resumes the period.
    #[test]
    fn only_the_first_period_on_a_fresh_directory_is_incomplete() {
        let dir = tempfile::TempDir::new().expect("tempdir");
        let mut runner = runner_with_salt(dir.path(), [9; 32]);
        assert!(
            !end_period(&mut runner).complete,
            "first period on a fresh directory"
        );
        assert!(
            end_period(&mut runner).complete,
            "a period begun at a rotation"
        );
        drop(runner);

        let mut restarted = ShadowRunner::new(dir.path(), EnforcementMode::Shadow);
        assert_eq!(restarted.epoch(), 2);
        assert!(
            end_period(&mut restarted).complete,
            "a restart at a period boundary must not make the next period partial"
        );
    }

    /// A restart mid-period resumes the period on schedule with its counts and its
    /// focus (#5914 review round 2, K7). A peer that restarts more often than every
    /// two hours otherwise never produces a complete period.
    #[test]
    fn a_restart_mid_period_resumes_the_period() {
        let dir = tempfile::TempDir::new().expect("tempdir");
        let hosted: Vec<_> = (0..20u8).map(instance).collect();
        let mut first = runner_with_salt(dir.path(), [9; 32]);
        end_period(&mut first); // into a complete period
        let focus = first.focus(Some(hosted.clone()), &[]).selected;
        feed(
            &mut first,
            focus[0],
            &[
                RecordOutcome::Admitted,
                RecordOutcome::Admitted,
                RecordOutcome::Refused,
            ],
        );
        for _ in 0..3 {
            assert!(first.advance().is_none());
        }
        drop(first);

        let mut resumed = ShadowRunner::new(dir.path(), EnforcementMode::Shadow);
        // Before the hosted source registers, the restored focus is kept.
        let mut kept = resumed.focus(None, &[instance(99)]).selected;
        let mut was = focus.clone();
        kept.sort_by(|a, b| a.as_bytes().cmp(b.as_bytes()));
        was.sort_by(|a, b| a.as_bytes().cmp(b.as_bytes()));
        assert_eq!(
            kept, was,
            "a restart replaced the period's focus with sampler keys"
        );

        let mut ended = None;
        let mut ticks = 0;
        while ended.is_none() {
            ticks += 1;
            ended = resumed.advance();
        }
        assert_eq!(
            ticks,
            ROTATE_EVERY_TICKS - 3,
            "the period did not resume on schedule"
        );
        let ended = ended.expect("ended");
        assert!(ended.complete);
        assert_eq!(
            (
                ended.merges_seen,
                ended.samples_admitted,
                ended.samples_refused,
                ended.stints
            ),
            (3, 2, 1, 2),
            "the restart lost the period's counts"
        );
    }

    /// A verdict survives a restart straight away, and merge/admission counts do
    /// once the writer's periodic flush has run (#5914 review round 3, A1). Before,
    /// both were only written at the next tick or draw, so a restart lost them while
    /// the restored period still claimed `complete`.
    #[test]
    fn verdicts_and_flushed_counts_survive_a_restart() {
        let dir = tempfile::TempDir::new().expect("tempdir");
        let hosted: Vec<_> = (0..20u8).map(instance).collect();
        let mut first = runner_with_salt(dir.path(), [9; 32]);
        end_period(&mut first);
        let focus = first.focus(Some(hosted.clone()), &[]).selected;
        first.advance();
        feed(
            &mut first,
            focus[0],
            &[RecordOutcome::Admitted, RecordOutcome::Refused],
        );
        first.flush_period();
        first.note_judged(&[JudgedContract {
            contract: focus[0],
            verdicts: 1,
            inconclusive: 0,
        }]);
        drop(first);

        let mut resumed = ShadowRunner::new(dir.path(), EnforcementMode::Shadow);
        let ended = end_period(&mut resumed);
        assert!(ended.complete);
        assert_eq!(
            (
                ended.merges_seen,
                ended.samples_admitted,
                ended.samples_refused,
                ended.judged
            ),
            (2, 1, 1, 1),
            "counts written since the last tick were lost on restart"
        );
    }

    /// A checkpoint that fails to write leaves the period dirty, so the next flush
    /// retries it (#5914 review round 4).
    #[test]
    fn a_failed_checkpoint_is_retried_at_the_next_flush() {
        let dir = tempfile::TempDir::new().expect("tempdir");
        let hosted: Vec<_> = (0..20u8).map(instance).collect();
        let mut runner = runner_with_salt(dir.path(), [9; 32]);
        end_period(&mut runner);
        let focus = runner.focus(Some(hosted), &[]).selected;
        runner.advance();
        feed(&mut runner, focus[0], &[RecordOutcome::Admitted]);

        // A directory where the staging file goes makes the write fail.
        let staging = dir.path().join(format!("{PERIOD_FILE}.tmp"));
        std::fs::create_dir(&staging).expect("block the staging file");
        runner.flush_period();
        assert!(runner.period_dirty, "a failed checkpoint was marked clean");

        std::fs::remove_dir(&staging).expect("unblock");
        runner.flush_period();
        assert!(!runner.period_dirty);
        drop(runner);
        let ended = end_period(&mut ShadowRunner::new(dir.path(), EnforcementMode::Shadow));
        assert_eq!(
            ended.samples_admitted, 1,
            "the retried checkpoint was not written"
        );
    }

    /// Merge and admission counts survive a restart through `flush_period` alone,
    /// with no verdict to trigger an earlier write (#5914 review round 4: the A1
    /// test let note_judged's own persist carry them, so a no-op flush survived).
    /// Each case ends with one lone kind of update before the final flush, so a
    /// later write cannot carry an update that failed to mark the period dirty.
    #[test]
    fn flushed_merge_and_admission_counts_survive_a_restart() {
        for lone_admission_last in [false, true] {
            let dir = tempfile::TempDir::new().expect("tempdir");
            let hosted: Vec<_> = (0..20u8).map(instance).collect();
            let mut first = runner_with_salt(dir.path(), [9; 32]);
            end_period(&mut first);
            let focus = first.focus(Some(hosted), &[]).selected;
            first.advance();
            feed(
                &mut first,
                focus[0],
                &[RecordOutcome::Admitted, RecordOutcome::Refused],
            );
            first.flush_period();
            if lone_admission_last {
                first.note_admission(&focus[0], RecordOutcome::Admitted);
            } else {
                first.note_merge(focus[0], None);
            }
            first.flush_period();
            drop(first);
            let ended = end_period(&mut ShadowRunner::new(dir.path(), EnforcementMode::Shadow));
            let expected = if lone_admission_last {
                (2, 2, 1)
            } else {
                (3, 1, 1)
            };
            assert_eq!(
                (
                    ended.merges_seen,
                    ended.samples_admitted,
                    ended.samples_refused
                ),
                expected,
                "flush_period did not checkpoint the counts (lone admission last: \
                 {lone_admission_last})"
            );
        }
    }

    /// Without a flush (or a tick, draw or verdict) the counts since the last write
    /// are lost: the tolerance FocusPeriodSummary::complete documents, and evidence
    /// that the previous test's survival is the flush's doing.
    #[test]
    fn unflushed_merge_and_admission_counts_are_lost_on_restart() {
        let dir = tempfile::TempDir::new().expect("tempdir");
        let hosted: Vec<_> = (0..20u8).map(instance).collect();
        let mut first = runner_with_salt(dir.path(), [9; 32]);
        end_period(&mut first);
        let focus = first.focus(Some(hosted), &[]).selected;
        first.advance();
        feed(
            &mut first,
            focus[0],
            &[RecordOutcome::Admitted, RecordOutcome::Refused],
        );
        drop(first);
        let ended = end_period(&mut ShadowRunner::new(dir.path(), EnforcementMode::Shadow));
        assert_eq!(
            (
                ended.merges_seen,
                ended.samples_admitted,
                ended.samples_refused
            ),
            (0, 0, 0)
        );
    }

    /// A failed checkpoint from ANY write path, not only the flush, leaves the
    /// period dirty for the next flush to retry (#5914 final review). A verdict's own
    /// immediate write failing used to leave a clean flag, so the verdict was lost.
    #[test]
    fn a_failed_verdict_checkpoint_is_retried_at_the_next_flush() {
        let dir = tempfile::TempDir::new().expect("tempdir");
        let hosted: Vec<_> = (0..20u8).map(instance).collect();
        let mut runner = runner_with_salt(dir.path(), [9; 32]);
        end_period(&mut runner);
        let focus = runner.focus(Some(hosted), &[]).selected;
        runner.advance();
        assert!(
            !runner.period_dirty,
            "precondition: the tick checkpoint landed"
        );

        let staging = dir.path().join(format!("{PERIOD_FILE}.tmp"));
        std::fs::create_dir(&staging).expect("block the staging file");
        runner.note_judged(&[JudgedContract {
            contract: focus[0],
            verdicts: 1,
            inconclusive: 0,
        }]);
        assert!(
            runner.period_dirty,
            "a failed verdict checkpoint was left clean"
        );

        std::fs::remove_dir(&staging).expect("unblock");
        runner.flush_period();
        drop(runner);
        let ended = end_period(&mut ShadowRunner::new(dir.path(), EnforcementMode::Shadow));
        assert_eq!(
            ended.judged, 1,
            "the verdict was lost with its failed checkpoint"
        );
    }

    /// An AdmittedPartly outcome adds to `admitted` as well as `refused`, and keeps
    /// its member accruing (#5914 review round 4, A3 gap).
    #[test]
    fn a_partly_admitted_merge_counts_toward_admitted_and_keeps_its_member() {
        let dir = tempfile::TempDir::new().expect("tempdir");
        let mut runner = runner_with_salt(dir.path(), [9; 32]);
        let focus = runner
            .focus(Some((0..20u8).map(instance).collect()), &[])
            .selected;
        feed(&mut runner, focus[0], &[RecordOutcome::AdmittedPartly]);
        runner.advance();
        assert!(
            runner.period.accruing().iter().any(|(c, _)| *c == focus[0]),
            "a partly admitted merge did not count as accruing"
        );
        let ended = end_period(&mut runner);
        assert_eq!((ended.samples_admitted, ended.samples_refused), (1, 1));
    }

    /// A contract that changes slot mid-period ends one stint and starts another, so
    /// each slot is credited only with what happened in it (#5914 review round 3,
    /// A2). Relabelling the whole stint moved a weighted verdict to the uniform slot.
    #[test]
    fn a_slot_change_ends_one_stint_and_starts_another() {
        let hosted: Vec<_> = (0..10u8).map(instance).collect();
        let mut checked = 0;
        for salt in 0..=255u8 {
            let dir = tempfile::TempDir::new().expect("tempdir");
            let mut runner = runner_with_salt(dir.path(), [salt; 32]);
            let first = runner.focus(Some(hosted.clone()), &[]).selected;
            let (uniform, weighted) = (first[0], first[1]);
            runner.note_judged(&[JudgedContract {
                contract: weighted,
                verdicts: 1,
                inconclusive: 0,
            }]);
            let without_uniform: Vec<_> =
                hosted.iter().copied().filter(|c| *c != uniform).collect();
            let second = runner.focus(Some(without_uniform), &[]).selected;
            if second[0] != weighted {
                continue;
            }
            checked += 1;
            let ended = end_period(&mut runner);
            assert_eq!(
                (
                    ended.stints,
                    ended.uniform_stints,
                    ended.weighted_stints,
                    ended.judged_uniform,
                    ended.judged_weighted
                ),
                (4, 2, 2, 0, 1),
                "salt {salt}: a slot change re-attributed the stint: {ended:?}"
            );
        }
        assert!(checked > 0, "no salt exercised the case");
    }

    /// A period persisted under a different salt is not resumed (#5914 review round
    /// 3, B3): its members were drawn by another peer identity's scores, and resuming
    /// them would credit this salt's period with that draw.
    #[test]
    fn a_changed_salt_does_not_resume_the_old_period() {
        let dir = tempfile::TempDir::new().expect("tempdir");
        let hosted: Vec<_> = (0..20u8).map(instance).collect();
        let mut first = runner_with_salt(dir.path(), [9; 32]);
        end_period(&mut first);
        let picked = first.focus(Some(hosted), &[]).selected;
        first.advance();
        drop(first);
        let mut other = runner_with_salt(dir.path(), [10; 32]);
        assert!(
            other.period.current.is_empty(),
            "a period drawn under another salt was resumed: {picked:?}"
        );
        assert!(!end_period(&mut other).complete);
    }

    /// A persisted period cannot restore more members than there are focus slots
    /// (#5914 review round 3, B4): the file is local, but nothing else bounds it.
    #[test]
    fn a_restored_period_is_capped_at_the_focus_size() {
        let dir = tempfile::TempDir::new().expect("tempdir");
        let mut first = runner_with_salt(dir.path(), [9; 32]);
        end_period(&mut first);
        first.focus(Some((0..20u8).map(instance).collect()), &[]);
        first.advance();
        drop(first);
        let path = dir.path().join(PERIOD_FILE);
        let mut saved: serde_json::Value =
            serde_json::from_slice(&std::fs::read(&path).expect("read")).expect("json");
        let member = saved["members"][0].clone();
        let members = saved["members"].as_array_mut().expect("members");
        for n in 100..110u8 {
            let mut extra = member.clone();
            extra[0] = serde_json::to_value(instance(n)).expect("id");
            members.push(extra);
        }
        std::fs::write(&path, serde_json::to_vec(&saved).expect("json")).expect("write");
        let restored = ShadowRunner::new(dir.path(), EnforcementMode::Shadow);
        assert!(
            restored.period.current.len() <= MAX_FOCUS_CONTRACTS,
            "restored {} focus members",
            restored.period.current.len()
        );
    }

    /// A runner restarted into a complete period, with its draw persisted.
    fn restarted_mid_period(
        dir: &Path,
        hosted: &[ContractInstanceId],
    ) -> (ShadowRunner, Vec<ContractInstanceId>) {
        let mut first = runner_with_salt(dir, [9; 32]);
        end_period(&mut first);
        first.advance();
        let picked = first.focus(Some(hosted.to_vec()), &[]).selected;
        drop(first);
        (ShadowRunner::new(dir, EnforcementMode::Shadow), picked)
    }

    /// Resumption ends at the first hosted draw (B1): from then on the runner draws
    /// normally, including from the fallback if the source goes away again.
    #[test]
    fn a_resumed_runner_draws_normally_once_the_hosted_source_arrives() {
        let dir = tempfile::TempDir::new().expect("tempdir");
        let hosted: Vec<_> = (0..20u8).map(instance).collect();
        let (mut resumed, picked) = restarted_mid_period(dir.path(), &hosted);
        // The persisted draw (made at a draw, not a tick) is what resumes, in slot
        // order, and reports no activity it has not seen.
        let tick = resumed.focus(None, &[instance(99)]);
        assert_eq!(
            tick.selected, picked,
            "the resumed focus is not the persisted draw"
        );
        assert_eq!(tick.active_candidates, 0);

        let elsewhere: Vec<_> = (40..60u8).map(instance).collect();
        let drawn = resumed.focus(Some(elsewhere.clone()), &[]).selected;
        assert!(
            drawn.iter().all(|c| elsewhere.contains(c)),
            "a hosted draw kept old focus"
        );
        let fallback = resumed.focus(None, &[instance(99)]).selected;
        assert_eq!(
            fallback,
            vec![instance(99)],
            "resumption outlived the hosted draw"
        );
    }

    /// Resumption also ends at a rotation (B1).
    #[test]
    fn resumption_ends_at_rotation() {
        let dir = tempfile::TempDir::new().expect("tempdir");
        let hosted: Vec<_> = (0..20u8).map(instance).collect();
        let (mut resumed, _) = restarted_mid_period(dir.path(), &hosted);
        while resumed.advance().is_none() {}
        assert_eq!(
            resumed.focus(None, &[instance(99)]).selected,
            vec![instance(99)]
        );
    }

    /// A restart during the first, incomplete period stays incomplete (B1).
    #[test]
    fn a_restart_during_an_incomplete_period_stays_incomplete() {
        let dir = tempfile::TempDir::new().expect("tempdir");
        let mut first = runner_with_salt(dir.path(), [9; 32]);
        first.advance();
        first.advance();
        drop(first);
        let mut resumed = ShadowRunner::new(dir.path(), EnforcementMode::Shadow);
        let mut ticks = 0;
        let ended = loop {
            ticks += 1;
            if let Some(ended) = resumed.advance() {
                break ended;
            }
        };
        assert_eq!(
            ticks,
            ROTATE_EVERY_TICKS - 2,
            "the period did not resume on schedule"
        );
        assert!(
            !ended.complete,
            "a period that began before any rotation claimed complete"
        );
    }

    /// A period persisted for another epoch is not resumed (B6).
    #[test]
    fn a_period_from_another_epoch_is_not_resumed() {
        let dir = tempfile::TempDir::new().expect("tempdir");
        let hosted: Vec<_> = (0..20u8).map(instance).collect();
        let (_, _) = restarted_mid_period(dir.path(), &hosted);
        std::fs::write(dir.path().join(EPOCH_FILE), b"7").expect("epoch");
        let resumed = ShadowRunner::new(dir.path(), EnforcementMode::Shadow);
        assert_eq!(resumed.epoch(), 7);
        assert!(
            resumed.period.current.is_empty(),
            "another epoch's period resumed"
        );
    }

    /// Departed stints persist with the totals (B6), and an out-of-range tick count
    /// in the file is reduced into the period.
    #[test]
    fn restored_totals_include_departed_stints_and_ticks_are_reduced() {
        let dir = tempfile::TempDir::new().expect("tempdir");
        let hosted: Vec<_> = (0..20u8).map(instance).collect();
        let mut first = runner_with_salt(dir.path(), [9; 32]);
        end_period(&mut first);
        let picked = first.focus(Some(hosted.clone()), &[]).selected;
        feed(&mut first, picked[0], &[RecordOutcome::Admitted]);
        let rest: Vec<_> = hosted
            .iter()
            .copied()
            .filter(|c| !picked.contains(c))
            .collect();
        first.focus(Some(rest), &[]);
        first.advance();
        drop(first);

        let path = dir.path().join(PERIOD_FILE);
        let mut saved: serde_json::Value =
            serde_json::from_slice(&std::fs::read(&path).expect("read")).expect("json");
        saved["ticks_into_period"] = serde_json::json!(ROTATE_EVERY_TICKS + 3);
        std::fs::write(&path, serde_json::to_vec(&saved).expect("json")).expect("write");

        let mut resumed = ShadowRunner::new(dir.path(), EnforcementMode::Shadow);
        assert_eq!(resumed.period_tick(), 3);
        let ended = end_period(&mut resumed);
        assert_eq!(
            (ended.stints, ended.merges_seen),
            (4, 1),
            "departed stints were lost"
        );
    }

    /// An admission only counts toward keeping a member once its interval has ended
    /// at a tick: a draw in mid-interval does not see it (B6, K1).
    #[test]
    fn an_admission_keeps_a_member_only_after_the_tick() {
        let dir = tempfile::TempDir::new().expect("tempdir");
        let mut runner = runner_with_salt(dir.path(), [21; 32]);
        let hosted: Vec<_> = (0..20u8).map(instance).collect();
        let picked = runner.focus(Some(hosted), &[]).selected;
        feed(&mut runner, picked[0], &[RecordOutcome::Admitted]);
        assert!(
            runner.period.accruing().is_empty(),
            "kept before its interval ended"
        );
        runner.advance();
        assert_eq!(runner.period.accruing().len(), 1);
    }

    /// An unreadable persisted period starts an incomplete one rather than failing.
    #[test]
    fn an_unreadable_period_file_starts_an_incomplete_period() {
        let dir = tempfile::TempDir::new().expect("tempdir");
        let mut runner = runner_with_salt(dir.path(), [9; 32]);
        end_period(&mut runner);
        drop(runner);
        std::fs::write(dir.path().join(PERIOD_FILE), b"not a period").expect("write");
        let mut restarted = ShadowRunner::new(dir.path(), EnforcementMode::Shadow);
        assert!(!end_period(&mut restarted).complete);
    }

    /// A contract that leaves focus mid-period (the hosted set changed) is still
    /// counted, rather than vanishing from the period because it is no longer current.
    #[test]
    fn a_contract_leaving_focus_mid_period_is_still_counted() {
        let dir = tempfile::TempDir::new().expect("tempdir");
        let mut runner = runner_with_salt(dir.path(), [11; 32]);
        let hosted: Vec<_> = (0..20u8).map(instance).collect();
        let first = runner.focus(Some(hosted.clone()), &[]).selected;
        feed(&mut runner, first[0], &[RecordOutcome::NothingNew]);

        // The peer stops hosting both focus contracts.
        let shrunk: Vec<_> = hosted
            .iter()
            .copied()
            .filter(|c| !first.contains(c))
            .collect();
        let second = runner.focus(Some(shrunk.clone()), &[]).selected;
        assert!(second.iter().all(|c| !first.contains(c)));
        feed(&mut runner, second[0], &[RecordOutcome::Admitted]);

        let summary = end_period(&mut runner);
        assert_eq!(summary.stints, 4, "two departed stints plus two current");
        assert_eq!(
            summary.merges_seen, 2,
            "the departed stint's merge was kept"
        );
        assert_eq!(
            summary.stints_without_new_samples, 3,
            "both departed contracts and one current one admitted nothing"
        );
    }

    /// Pin: the "conformance focus period ended" line reports each summary field
    /// under its own name. A behavioural test of the summary cannot see the log, and
    /// a line that printed `judged` under `stints_without_new_samples` would mislead
    /// the acceptance measurement while every test stayed green.
    #[test]
    fn the_period_line_reports_every_field_under_its_own_name() {
        let src = include_str!("shadow.rs");
        let tests_at = src.find("\nmod tests {").expect("test module");
        let start = src
            .find("    pub(crate) fn advance(&mut self)")
            .expect("advance not found");
        assert!(start < tests_at, "matched inside the test module");
        let after = &src[start..];
        let open = after.find('{').expect("advance has no body");
        let mut depth = 0usize;
        let mut end = None;
        for (offset, ch) in after[open..].char_indices() {
            match ch {
                '{' => depth += 1,
                '}' => {
                    depth -= 1;
                    if depth == 0 {
                        end = Some(open + offset + 1);
                        break;
                    }
                }
                _ => {}
            }
        }
        let body: String = after[..end.expect("unbalanced")]
            .lines()
            .filter(|line| !line.trim_start().starts_with("//"))
            .collect::<Vec<_>>()
            .join("\n");
        assert!(body.contains("\"conformance focus period ended\""));
        for field in [
            "epoch",
            "complete",
            "candidates",
            "active_candidates",
            "stints",
            "merges_seen",
            "samples_admitted",
            "samples_refused",
            "stints_without_new_samples",
            "judged",
            "uniform_stints",
            "judged_uniform",
            "weighted_stints",
            "judged_weighted",
            "weighted_selected_activity",
        ] {
            let pair = format!("{field} = ended.{field},");
            assert_eq!(
                body.matches(&pair).count(),
                1,
                "the period-ended line does not report `{field}` from `ended.{field}`"
            );
        }
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
        let mut runner = ShadowRunner::new(dir.path(), EnforcementMode::Shadow);
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

    /// The real-WASM counterpart of the tier tests: a contract whose ONLY defect is
    /// a report-only law produces findings, every one of them reported, and no
    /// prospective removal.
    ///
    /// Mode 8 of the shared fixture takes the last write on a key collision on its
    /// delta path and the first on its merge path. Every other law holds on these
    /// states (pinned in `wasm_tests`), so the only finding available is
    /// `path_agreement`, which is report-only.
    #[tokio::test(flavor = "multi_thread")]
    async fn a_report_only_defect_is_reported_and_never_would_remove()
    -> Result<(), Box<dyn std::error::Error>> {
        use freenet_stdlib::prelude::{
            ContractCode, ContractContainer, ContractWasmAPIVersion, Parameters, WrappedContract,
        };
        use std::sync::Arc;

        // Keep in sync with `tests/test-contract-conformance/src/lib.rs`.
        const PATH_DISAGREEMENT: u8 = 8;

        let wasm = crate::wasm_runtime::tests::get_test_module("test_contract_conformance")?;
        let code_hash = *blake3::hash(&wasm).as_bytes();
        let store_dir = crate::util::tests::get_temp_dir();
        std::fs::create_dir_all(store_dir.path())?;
        let db = crate::contract::storages::Storage::new(store_dir.path()).await?;
        let mut store =
            crate::wasm_runtime::ContractStore::new(store_dir.path().into(), 10_000_000, db)?;

        let parameters: Parameters<'static> = Parameters::from(vec![PATH_DISAGREEMENT]);
        let contract = WrappedContract::new(Arc::new(ContractCode::from(wasm)), parameters.clone());
        let id = *contract.key().id();
        store.store_contract(ContractContainer::Wasm(ContractWasmAPIVersion::V1(
            contract,
        )))?;

        // Two writes to key 5 with different values, each reached from the same
        // base: the collision the two paths resolve differently.
        let states = vec![
            (vec![0x10u8], vec![0x10u8, 0x51]),
            (vec![0x10u8], vec![0x10u8, 0x52]),
        ];
        let samplers = samplers_for(id, vec![PATH_DISAGREEMENT], code_hash, &states);

        let dir = tempfile::TempDir::new()?;
        let mut runner = ShadowRunner::new(dir.path(), EnforcementMode::Shadow);
        let (report, findings) =
            tick_with_findings(&mut runner, &samplers, Some(store_dir.path())).await;

        assert_eq!(report.probed, 1, "the contract was not probed: {report:?}");
        assert!(
            findings
                .iter()
                .any(|f| f.violation.property == ConformanceProperty::PathAgreement),
            "the path disagreement produced no path_agreement finding, so the zero \
             below proves nothing: {findings:?}"
        );
        assert!(report.report_only_violations > 0, "{report:?}");
        assert_eq!(
            report.would_remove, 0,
            "a report-only finding reached would_remove in shadow: {report:?}"
        );
        assert!(findings.iter().all(|f| !f.would_remove), "{findings:?}");
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
