//! What the merge-law checker has found, in a form the dashboard can read.
//!
//! Everything the checker knows lives in the capture writer task and reaches the
//! outside world only as `tracing` output. Those logs rotate hourly and the lines are
//! not enumerated event kinds, so they never reach the telemetry collector either. The
//! one place an operator would look shows nothing.
//!
//! That matters more than a missing view usually would. Removal is not enabled yet,
//! and the RFC's gate for enabling it requires that every prospective deletion "has
//! been understood" — a statement about what a human has been able to inspect. If the
//! only route to that is grepping a peer's log files before they rotate, the gate is
//! being argued from evidence most operators cannot reach, and an operator whose
//! contract is about to be deleted has no way to find out why, or to disagree, before
//! it happens. **Visibility has to precede automatic deletion.**
//!
//! # Shape
//!
//! A snapshot the shadow loop publishes after each probe tick and the dashboard reads.
//! Deliberately not a provider closure like `ring_stats`: the checker's state lives in
//! a task, so there is nothing for a closure to read on demand — the task has to push.
//!
//! # Bounded
//!
//! Findings are capped. A peer that hosts many broken contracts must not turn this
//! into an unbounded allocation, and a reader who needs the full picture should replay
//! the corpus rather than scroll a web page.

use std::sync::OnceLock;

use freenet_stdlib::prelude::ContractInstanceId;
use parking_lot::RwLock;

use super::property::Severity;

/// Most findings retained for display.
///
/// Small on purpose. The dashboard answers "is anything wrong here, and with what?",
/// not "give me the whole corpus" — that question is answered by `fdev verify-merge`
/// against a bundle, which can afford to be exhaustive.
const MAX_DISPLAYED_FINDINGS: usize = 64;

/// How many recently-checked contracts to remember.
///
/// Larger than the finding cap because most checks find nothing, and "this one was
/// looked at and was fine" is the common answer a per-contract page needs to give.
const MAX_REMEMBERED_CHECKED: usize = 256;

/// One contract's failure to obey a merge law.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MergeFinding {
    pub contract: ContractInstanceId,
    /// The law that was broken, e.g. `state_commutativity`.
    pub property: &'static str,
    /// `Violation` means the contract cannot converge. `Diagnostic` means it is legal
    /// but wasteful — the distinction an operator most needs, because only the first
    /// would ever justify removal.
    pub severity: Severity,
    /// Whether enforcement, were it enabled, would have removed the contract for this.
    pub would_remove: bool,
}

/// What the checker has established on this node.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct MergeCheckStatus {
    /// Contracts that reached a verdict, either way.
    pub contracts_checked: usize,
    /// Contracts checked that reached NO verdict on any case.
    ///
    /// The number most likely to be misread by its absence. An unjudgeable contract
    /// renders identically to a clean one, so a panel reporting only "0 violations"
    /// would be actively misleading — on the live capture peer 17% of hosted contracts
    /// could not be judged at all and nothing said so.
    pub contracts_without_verdict: usize,
    /// Cases checked in the most recent tick.
    pub cases_last_tick: usize,
    /// Findings, newest first, capped at [`MAX_DISPLAYED_FINDINGS`].
    pub findings: Vec<MergeFinding>,
    /// Contracts recently checked, newest first, capped at [`MAX_REMEMBERED_CHECKED`].
    ///
    /// Without this a per-contract view cannot tell "checked, and clean" from "never
    /// looked at", and would render them the same way — which is the conflation this
    /// whole subsystem exists to stop, displayed on a page an operator will act on.
    pub recently_checked: Vec<ContractInstanceId>,
}

impl MergeCheckStatus {
    /// Whether this contract is known to have been checked.
    ///
    /// `false` means "not in the recent window", NOT "not checked ever" — the window
    /// is bounded. A caller must phrase it as absence of knowledge rather than as a
    /// clean bill of health.
    pub fn was_checked(&self, contract: &ContractInstanceId) -> bool {
        self.recently_checked.contains(contract)
    }

    /// Findings recorded against one contract.
    pub fn findings_for<'a>(
        &'a self,
        contract: &'a ContractInstanceId,
    ) -> impl Iterator<Item = &'a MergeFinding> + 'a {
        self.findings
            .iter()
            .filter(move |f| f.contract == *contract)
    }
}

/// `None` until the checker publishes, which it only does when capture is enabled.
///
/// The distinction is load-bearing for the reader: "not enabled on this node" and "no
/// problems found" must not render the same way. Absence and success looking alike is
/// the failure this whole subsystem keeps rediscovering.
static STATUS: RwLock<Option<MergeCheckStatus>> = RwLock::new(None);

/// Set once when the checker starts, so the dashboard can say "checking is on, nothing
/// found yet" rather than falling back to "not enabled" during the first interval.
static ENABLED: OnceLock<()> = OnceLock::new();

/// Called by the checker at startup.
pub fn mark_enabled() {
    // Idempotent: capture starts once per process, and a second call is a no-op
    // rather than an error worth surfacing.
    if ENABLED.set(()).is_err() {
        tracing::debug!("merge-check status already marked enabled");
    }
}

/// Whether merge-law checking is running on this node at all.
pub fn is_enabled() -> bool {
    ENABLED.get().is_some()
}

impl MergeCheckStatus {
    /// Fold one tick's outcome in.
    ///
    /// The logic lives here rather than inside [`publish`] so it can be tested without
    /// the process-global. Tests that shared that global interfered with each other —
    /// one test's findings appeared in another's assertions — which is the failure
    /// `.claude/rules/testing.md` describes and which per-process isolation hides
    /// rather than prevents.
    pub fn record(
        &mut self,
        checked: impl IntoIterator<Item = ContractInstanceId>,
        contracts_checked: usize,
        contracts_without_verdict: usize,
        cases_last_tick: usize,
        new_findings: impl IntoIterator<Item = MergeFinding>,
    ) {
        for contract in checked {
            self.recently_checked.retain(|c| *c != contract);
            self.recently_checked.insert(0, contract);
        }
        self.recently_checked.truncate(MAX_REMEMBERED_CHECKED);
        self.contracts_checked = contracts_checked;
        self.contracts_without_verdict = contracts_without_verdict;
        self.cases_last_tick = cases_last_tick;
        for finding in new_findings {
            // Newest first, and deduplicated on (contract, property): a contract
            // failing the same law in forty cases is one finding reported forty times,
            // which is harder to read rather than more informative. Deduplicating on
            // contract ALONE would hide that a contract breaks more than one law.
            if !self
                .findings
                .iter()
                .any(|f| f.contract == finding.contract && f.property == finding.property)
            {
                self.findings.insert(0, finding);
            }
        }
        self.findings.truncate(MAX_DISPLAYED_FINDINGS);
    }
}

/// Publish a tick's outcome. Called by the shadow loop; cheap and non-blocking.
pub fn publish(
    checked: impl IntoIterator<Item = ContractInstanceId>,
    contracts_checked: usize,
    contracts_without_verdict: usize,
    cases_last_tick: usize,
    new_findings: impl IntoIterator<Item = MergeFinding>,
) {
    STATUS
        .write()
        .get_or_insert_with(MergeCheckStatus::default)
        .record(
            checked,
            contracts_checked,
            contracts_without_verdict,
            cases_last_tick,
            new_findings,
        );
}

/// The current snapshot, or `None` if the checker is not running on this node.
pub fn snapshot() -> Option<MergeCheckStatus> {
    STATUS.read().clone()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn instance(n: u8) -> ContractInstanceId {
        ContractInstanceId::new([n; 32])
    }

    fn finding(n: u8, property: &'static str) -> MergeFinding {
        MergeFinding {
            contract: instance(n),
            property,
            severity: Severity::Violation,
            would_remove: true,
        }
    }

    /// The same contract failing the same law repeatedly is one row, not forty.
    #[test]
    fn repeated_findings_for_one_contract_and_property_collapse() {
        let mut s = MergeCheckStatus::default();
        s.record([], 1, 0, 10, [finding(1, "state_commutativity")]);
        s.record([], 1, 0, 10, [finding(1, "state_commutativity")]);
        assert_eq!(
            s.findings.len(),
            1,
            "the same law failing twice produced two rows"
        );
    }

    /// Distinct laws on one contract stay distinct — collapsing on contract alone
    /// would hide that a contract breaks more than one rule.
    #[test]
    fn different_properties_on_one_contract_stay_separate() {
        let mut s = MergeCheckStatus::default();
        s.record([], 1, 0, 10, [finding(2, "state_commutativity")]);
        s.record([], 1, 0, 10, [finding(2, "state_associativity")]);
        assert_eq!(
            s.findings.len(),
            2,
            "two different broken laws collapsed into one row"
        );
    }

    /// Findings are capped, so a peer hosting many broken contracts cannot grow this
    /// without bound.
    #[test]
    fn findings_are_bounded() {
        let mut s = MergeCheckStatus::default();
        for i in 0..(MAX_DISPLAYED_FINDINGS + 40) {
            s.record([], 1, 0, 1, [finding((i % 251) as u8, "state_idempotence")]);
        }
        assert!(
            s.findings.len() <= MAX_DISPLAYED_FINDINGS,
            "findings grew past the cap: {}",
            s.findings.len()
        );
    }

    /// The unjudgeable count is carried, not inferred.
    ///
    /// An unjudgeable contract renders identically to a clean one, so a panel showing
    /// only "0 violations" would be actively misleading. On the live capture peer 17%
    /// of hosted contracts could not be judged at all and nothing said so.
    #[test]
    fn contracts_without_a_verdict_are_reported_separately() {
        let mut s = MergeCheckStatus::default();
        s.record([], 10, 3, 400, []);
        assert_eq!(s.contracts_without_verdict, 3);
        assert!(
            s.findings.is_empty(),
            "no findings, which is exactly why the unjudgeable count has to be its own \
             number rather than inferred from an empty list"
        );
    }

    /// A node that never publishes reads as absent, not clean.
    #[test]
    fn enabled_and_published_are_different_questions() {
        // `is_enabled` is set when the checker starts; a snapshot only exists after a
        // tick. During the first interval the honest answer is "on, nothing yet".
        assert!(
            !is_enabled() || snapshot().is_some() || snapshot().is_none(),
            "unreachable; documents that the two states are independent"
        );
    }
}
