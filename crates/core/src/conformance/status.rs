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
//! # One window, not two
//!
//! Everything known about a contract lives in ONE [`CheckedContract`] record: whether
//! it was checked, how many of its cases reached a verdict, and what was found. This
//! is not tidiness. The first version of this module kept two independent windows — a
//! 256-entry "recently checked" list and a 64-entry findings list — and the card
//! picked its branch from the first while reading the second. A contract present in
//! one and evicted from the other rendered a green pill reading "no merge-law
//! violation was found for this contract", for a contract the checker had positively
//! found violating.
//!
//! That was the steady state rather than a corner: a re-detected finding was
//! deduplicated rather than moved to the front, so a contract caught every tick
//! drifted monotonically toward the tail of the 64-entry list while being re-inserted
//! at the head of the 256-entry one. The *persistently broken* contract was the
//! likeliest to lose its finding. `.claude/rules/bug-prevention-patterns.md` names
//! this class ("paired fields that must co-occur") and prescribes the remedy applied
//! here: bundle them so the type system forbids the mismatch. `was_checked` and
//! `findings_for` now answer from the same record by construction, so the conflation
//! is unrepresentable rather than arithmetically avoided.
//!
//! # Bounded
//!
//! The window is capped at [`MAX_REMEMBERED_CHECKED`] contracts. Per-contract findings
//! need no separate cap, but only because of a property that has to be maintained
//! rather than assumed: every finding enters a record through
//! [`CheckedContract::note_finding`], which deduplicates on the property that broke,
//! and `ConformanceProperty` is a closed enum of twelve — so one contract holds at
//! most twelve findings.
//!
//! That claim was written here before it was true, and stood for three review rounds
//! while it was false. [`checked_contracts`] appended one finding per violating CASE,
//! and a probe runs up to `shadow::MAX_CASES_PER_PROBE` (64) of them, so one broken
//! law produced up to 64 identical rows on a contract's first record — the normal
//! path, since the first probe is where a violation is found. A justification comment
//! that is merely aspirational is worse than none: it tells the next reader the
//! question is settled. If a second way to add a finding is ever introduced, this
//! paragraph stops being true again, which is why `note_finding` is the only one.
//!
//! A reader who needs the full picture should replay the corpus rather than scroll a
//! web page.

use std::sync::OnceLock;

use freenet_stdlib::prelude::ContractInstanceId;
// tokio's clock rather than `std`'s, matching `shadow.rs`. `std::time::Instant::now()`
// is banned in `crates/core/src/` (`.claude/rules/testing.md`, enforced by
// `.github/scripts/check_banned_patterns.py`) because it cannot be controlled from a
// test; tokio's honours `tokio::time::pause()`, which is the sanctioned alternative in
// async code. The whole module moves rather than the one line CI flagged: the lint's
// regex matches only the fully-qualified spelling, so an `Instant::now()` reached
// through a `use std::time::Instant` passed by accident of spelling rather than by
// being correct (#5407).
use parking_lot::RwLock;
use tokio::time::Instant;

use super::property::Severity;
use super::shadow::{Finding, JudgedContract};

/// How many recently-checked contracts to remember.
///
/// Sized against what one record can cost now that findings live inside it: a
/// `CheckedContract` is ~88 bytes plus at most twelve `MergeFinding`s of ~56 bytes,
/// so the worst case — every remembered contract breaking every law — is around
/// 190 KB, and the realistic case (findings are rare) is a few tens of KB. That is
/// affordable, and the render cost that used to argue for a smaller number is gone:
/// [`view_for`] clones ONE record rather than the whole set, so a contract-detail
/// page no longer pays for the window's size.
///
/// The "at most twelve" half of that arithmetic is a consequence of
/// [`CheckedContract::note_finding`] being the only way a finding reaches a record,
/// not a standing fact. While [`checked_contracts`] appended without deduplicating,
/// one record could hold up to `shadow::MAX_CASES_PER_PROBE` (64) findings and the
/// real worst case was nearer 0.9 MB — a bound stated in a comment is only as good as
/// the invariant it rests on, and this one was stated before that invariant held.
///
/// Kept large for coverage rather than shrunk for safety. Focus probes at most
/// `shadow::MAX_FOCUS_CONTRACTS` (two) contracts per fifteen-minute tick, so 256
/// entries is roughly thirty hours of the peer's history — and "this one was looked
/// at and was fine" is the common answer a per-contract page needs to give.
const MAX_REMEMBERED_CHECKED: usize = 256;

/// Past this long since the last publish, the card says the checker may be stuck
/// rather than presenting its last tick as a current result.
///
/// Derived from the probe interval rather than hardcoded, per
/// `.claude/rules/code-style.md`: three missed ticks is well outside ordinary jitter
/// and cheap to be wrong about, since the card shows the age either way.
const STALE_AFTER: std::time::Duration =
    std::time::Duration::from_secs(super::shadow::PROBE_INTERVAL.as_secs() * 3);

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

/// Everything the checker knows about ONE contract.
///
/// The unit of the window, so "was this checked?" and "what was found?" cannot be
/// answered from different windows — see the module doc.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CheckedContract {
    pub contract: ContractInstanceId,
    /// Cases that reached `Holds` or `Violated`, accumulated over this contract's
    /// residency in the window.
    ///
    /// Carried per contract because the fleet-wide case count cannot stand in for it:
    /// a contract with one verdict and 199 inconclusive cases rendered byte-identically
    /// to one with 200 verdicts, which is the same conflation of "unmeasured" with
    /// "clean" this module exists to stop.
    pub verdicts: usize,
    /// Cases that came back `Inconclusive`, accumulated the same way.
    pub inconclusive: usize,
    /// Findings for THIS contract, newest first. Deduplicated on the property that
    /// broke, so the length is bounded by the property count.
    ///
    /// Only ever added to through [`CheckedContract::note_finding`] — see its doc for
    /// why the deduplication is not repeated at each call site.
    pub findings: Vec<MergeFinding>,
    /// When the most recent tick that judged THIS contract published.
    ///
    /// Per contract, not per node. The card used to answer "when was this contract
    /// last checked?" with [`MergeCheckStatus::published_at`], the node's most recent
    /// tick — so a contract judged twenty hours ago rendered as checked three minutes
    /// ago, beside a sentence saying no violation was found "the last time it was
    /// checked". That is the same misattribution the PR that added this field had
    /// already fixed for the case COUNTS (see
    /// [`MergeCheckStatus::judged_last_tick`]), reappearing one row lower.
    ///
    /// Stamped by [`MergeCheckStatus::record`] from the tick's publish time, in both
    /// its arms, so re-checking a contract refreshes it.
    pub checked_at: Instant,
}

impl CheckedContract {
    /// Add one finding to this record, deduplicated on the property that broke.
    ///
    /// The ONE place a finding is added to a record. It was previously open-coded in
    /// `MergeCheckStatus::record`'s merge arm and NOT in [`checked_contracts`], which
    /// is #5403 H1: `record`'s merge arm only runs once the contract is already in
    /// the window, so a violating contract's FIRST record — the one where the
    /// violation is actually found, and therefore the normal case — was assembled
    /// with no deduplication at all. `probe_one` pushes one `Finding` per violating
    /// case and a probe runs up to `shadow::MAX_CASES_PER_PROBE` (64) of them, so one
    /// broken law rendered as up to 64 identical table rows, which then persisted for
    /// the record's whole residency because later ticks deduplicate AGAINST them.
    ///
    /// Deduplicating on the PROPERTY rather than on the contract is the point: a
    /// contract failing the same law in forty cases is one finding reported forty
    /// times, but a contract failing two different laws is two facts an operator
    /// needs.
    ///
    /// Findings are never dropped when a later tick fails to reproduce them. A probe
    /// runs a bounded sample of cases, so "not seen this tick" is not evidence of
    /// absence, and forgetting a confirmed violation because the next sample missed
    /// it is the green-pill failure in a slower form.
    pub(crate) fn note_finding(&mut self, finding: MergeFinding) {
        if self.findings.iter().any(|f| f.property == finding.property) {
            return;
        }
        self.findings.insert(0, finding);
    }
}

/// What the checker has established on this node.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MergeCheckStatus {
    /// Contracts that reached a verdict in the most recent tick.
    ///
    /// A per-TICK number, and named so. The un-suffixed name it used to carry read as
    /// a standing property of the node, which is how the card came to present a
    /// fleet-wide most-recent-tick count as evidence about a contract last looked at
    /// forty ticks ago.
    pub judged_last_tick: usize,
    /// Contracts checked in the most recent tick that reached NO verdict on any case.
    ///
    /// The number most likely to be misread by its absence. An unjudgeable contract
    /// renders identically to a clean one, so a panel reporting only "0 violations"
    /// would be actively misleading — on the live capture peer 17% of hosted contracts
    /// could not be judged at all and nothing said so.
    pub without_verdict_last_tick: usize,
    /// Contracts checked, newest first, capped at [`MAX_REMEMBERED_CHECKED`].
    ///
    /// Without this a per-contract view cannot tell "checked, and clean" from "never
    /// looked at", and would render them the same way — which is the conflation this
    /// whole subsystem exists to stop, displayed on a page an operator will act on.
    pub checked: Vec<CheckedContract>,
    /// When the last tick published.
    ///
    /// Carried because nothing else on the snapshot ages. `publish` is reached from
    /// one place and two live paths skip it indefinitely while the old snapshot stands
    /// (a tick that selected no work, and a probe task that died), so a peer whose
    /// probe has been panicking for a week would otherwise keep serving "no violation
    /// found" from a week-old tick with nothing on the page saying so.
    pub published_at: Instant,
}

impl Default for MergeCheckStatus {
    fn default() -> Self {
        Self {
            judged_last_tick: 0,
            without_verdict_last_tick: 0,
            checked: Vec::new(),
            // Overwritten by the first `record`; a default-constructed status has
            // published nothing, and the global is only created by `publish`.
            published_at: Instant::now(),
        }
    }
}

/// What one contract-detail render needs, cloned out of the window.
///
/// Deliberately NOT the whole snapshot. The window holds up to
/// [`MAX_REMEMBERED_CHECKED`] records with their findings, and the card needs exactly
/// one of them plus the fleet-wide numbers; cloning the set on every page render would
/// make the page's cost grow with the peer's history for no reader benefit.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MergeCheckView {
    /// See [`MergeCheckStatus::judged_last_tick`].
    pub judged_last_tick: usize,
    /// See [`MergeCheckStatus::without_verdict_last_tick`].
    pub without_verdict_last_tick: usize,
    /// How long ago the checker last published a tick.
    pub published_secs_ago: u64,
    /// Whether that is long enough ago to distrust — see [`STALE_AFTER`].
    pub stale: bool,
    /// This contract's record, or `None` if it is outside the window.
    pub contract: Option<CheckedContract>,
    /// How long ago the tick that last judged THIS contract published, or `None` when
    /// there is no record — the two are produced together in [`view_for`] from one
    /// `Option`, so a page can never show an age for a contract it has no record of.
    ///
    /// Separate from [`Self::published_secs_ago`], which is node-wide. Rendering the
    /// node-wide one against a per-contract sentence ("no violation found ... the last
    /// time it was checked") is what made a contract judged twenty hours ago read as
    /// checked three minutes ago.
    pub checked_secs_ago: Option<u64>,
}

impl MergeCheckStatus {
    /// Whether this contract is known to have been checked.
    ///
    /// `false` means "not in the recent window", NOT "not checked ever" — the window
    /// is bounded. A caller must phrase it as absence of knowledge rather than as a
    /// clean bill of health.
    pub fn was_checked(&self, contract: &ContractInstanceId) -> bool {
        self.record_for(contract).is_some()
    }

    /// This contract's record, if it is in the window.
    pub fn record_for(&self, contract: &ContractInstanceId) -> Option<&CheckedContract> {
        self.checked.iter().find(|c| c.contract == *contract)
    }

    /// Findings recorded against one contract.
    pub fn findings_for<'a>(
        &'a self,
        contract: &'a ContractInstanceId,
    ) -> impl Iterator<Item = &'a MergeFinding> + 'a {
        self.record_for(contract)
            .into_iter()
            .flat_map(|c| c.findings.iter())
    }

    /// The one record a contract-detail page needs, plus the fleet-wide numbers.
    ///
    /// Takes `now` rather than reading the clock, so the staleness branch is reachable
    /// from a test without waiting three probe intervals.
    pub fn view_for(&self, contract: Option<&ContractInstanceId>, now: Instant) -> MergeCheckView {
        let age = now.saturating_duration_since(self.published_at);
        // Both derived from ONE lookup, so `contract` and `checked_secs_ago` cannot
        // describe different contracts or disagree about whether there is a record at
        // all — the "paired fields that must co-occur" remedy from
        // `.claude/rules/bug-prevention-patterns.md`, which this module has already
        // been bitten by once (the two-window split).
        let record = contract.and_then(|id| self.record_for(id)).cloned();
        let checked_secs_ago = record
            .as_ref()
            .map(|r| now.saturating_duration_since(r.checked_at).as_secs());
        MergeCheckView {
            judged_last_tick: self.judged_last_tick,
            without_verdict_last_tick: self.without_verdict_last_tick,
            published_secs_ago: age.as_secs(),
            stale: age >= STALE_AFTER,
            contract: record,
            checked_secs_ago,
        }
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
        checked: impl IntoIterator<Item = CheckedContract>,
        judged_last_tick: usize,
        without_verdict_last_tick: usize,
        published_at: Instant,
    ) {
        self.judged_last_tick = judged_last_tick;
        self.without_verdict_last_tick = without_verdict_last_tick;
        self.published_at = published_at;
        for record in checked {
            // Move to the FRONT and merge, never dedup-and-leave-in-place. The old
            // two-list version skipped a re-detected finding entirely, so a contract
            // caught on every tick sank toward the tail while its "recently checked"
            // entry was refreshed — the persistently broken contract was the first to
            // lose its finding.
            let existing = self
                .checked
                .iter()
                .position(|c| c.contract == record.contract)
                .map(|at| self.checked.remove(at));
            let mut merged = match existing {
                // Counts accumulate over the contract's residency in the window, so a
                // page can say how much looking has actually happened for THIS
                // contract rather than reporting one tick's slice of it.
                Some(mut prev) => {
                    prev.verdicts = prev.verdicts.saturating_add(record.verdicts);
                    prev.inconclusive = prev.inconclusive.saturating_add(record.inconclusive);
                    // Through `note_finding`, the same call `checked_contracts` makes.
                    // Open-coding the deduplication here and not there is #5403 H1:
                    // this arm cannot run until the contract is already in the window,
                    // so the tick that FINDS a violation never reached it.
                    for finding in record.findings {
                        prev.note_finding(finding);
                    }
                    prev
                }
                None => record,
            };
            // In BOTH arms, from the tick's own publish time: "when was this contract
            // last checked?" is per-contract, and answering it with the node's most
            // recent tick is how a contract judged twenty hours ago came to render as
            // checked three minutes ago.
            merged.checked_at = published_at;
            self.checked.insert(0, merged);
        }
        self.checked.truncate(MAX_REMEMBERED_CHECKED);
    }
}

/// Turn one tick's shadow output into the per-contract records this module stores.
///
/// A free function rather than logic at the publish call site: that call site lives
/// inside `capture::run_writer`'s select loop, which no test can call, so anything
/// assembled there is testable only by scraping source.
pub(crate) fn checked_contracts(
    judged: &[JudgedContract],
    findings: &[Finding],
) -> Vec<CheckedContract> {
    // Overwritten by `record` with the tick's own publish time, in both its arms. Set
    // here rather than left unset because an unstamped record has no honest rendering
    // — and this value is microseconds from the one that replaces it, so the field is
    // never briefly wrong, only briefly its own approximation.
    let checked_at = Instant::now();
    let mut out: Vec<CheckedContract> = judged
        .iter()
        .map(|j| CheckedContract {
            contract: j.contract,
            verdicts: j.verdicts,
            inconclusive: j.inconclusive,
            findings: Vec::new(),
            checked_at,
        })
        .collect();
    for finding in findings {
        let merge = MergeFinding {
            contract: finding.contract,
            property: finding.violation.property.as_str(),
            severity: finding.violation.property.severity(),
            would_remove: finding.would_remove,
        };
        match out.iter_mut().find(|c| c.contract == finding.contract) {
            // Through `note_finding`, which deduplicates on the property. `probe_one`
            // pushes one `Finding` per violating CASE and a probe runs up to
            // `shadow::MAX_CASES_PER_PROBE` (64) of them, so appending here rendered
            // one broken law as up to 64 identical rows — and this is the FIRST record
            // for a violating contract, which is the normal path, since the first
            // probe is where a violation is found. `record`'s own deduplication could
            // not help: its merge arm runs only once the contract is already in the
            // window, by which time the duplicates are what later ticks deduplicate
            // against. (#5403 H1)
            Some(record) => record.note_finding(merge),
            None => {
                // Structurally unreachable — a finding comes from a `Violated`
                // outcome, which IS a verdict, so its contract is in `judged`. Kept
                // anyway, and kept LOUD: dropping it would make a real violation
                // render as a contract nobody looked at, and an uncounted discard is
                // the failure `.claude/rules/bug-prevention-patterns.md` opens with.
                tracing::warn!(
                    contract = %finding.contract,
                    property = merge.property,
                    "merge finding for a contract the tick did not record as judged"
                );
                out.push(CheckedContract {
                    contract: finding.contract,
                    // ONE, not zero. A finding IS a verdict — it comes from a
                    // `Violated` outcome — so a record carrying a violation and
                    // `verdicts: 0` renders "0 reached a verdict" directly beside a
                    // Violation row, which reads as a page contradicting itself and
                    // gives an operator no way to tell which half to believe. One is
                    // the honest floor: at least the case that produced this finding
                    // reached a verdict. Any further findings for the same contract
                    // take the `Some` arm above, so this is not a per-finding count
                    // and does not claim to be.
                    verdicts: 1,
                    inconclusive: 0,
                    findings: vec![merge],
                    checked_at,
                });
            }
        }
    }
    out
}

/// Publish a tick's outcome. Called by the shadow loop; cheap and non-blocking.
///
/// `pub(crate)`: the only legitimate caller is `capture::run_writer`, and the shape of
/// what belongs in a snapshot is decided there (which counts, assembled by
/// [`checked_contracts`], stamped with the tick's publish time). A caller outside this
/// crate could only publish a snapshot that had not been through that assembly, which
/// is the class of defect this module keeps finding.
pub(crate) fn publish(
    checked: impl IntoIterator<Item = CheckedContract>,
    judged_last_tick: usize,
    without_verdict_last_tick: usize,
    published_at: Instant,
) {
    STATUS
        .write()
        .get_or_insert_with(MergeCheckStatus::default)
        .record(
            checked,
            judged_last_tick,
            without_verdict_last_tick,
            published_at,
        );
}

/// What the contract-detail page needs about ONE contract, or `None` if the checker
/// has not published on this node.
///
/// Clones a single record rather than the window — see [`MergeCheckView`].
pub fn view_for(contract: Option<&ContractInstanceId>) -> Option<MergeCheckView> {
    STATUS
        .read()
        .as_ref()
        .map(|s| s.view_for(contract, Instant::now()))
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

    fn checked(n: u8, findings: Vec<MergeFinding>) -> CheckedContract {
        CheckedContract {
            contract: instance(n),
            verdicts: 1,
            inconclusive: 0,
            findings,
            // Overwritten by `record` with the tick's publish time, as in production.
            checked_at: Instant::now(),
        }
    }

    /// The fixture contract's NEVER_SETTLES mode — see
    /// `tests/test-contract-conformance/src/lib.rs`. Breaks state idempotence and
    /// state associativity on most generated cases, so one probe of it returns many
    /// findings across MORE THAN ONE property: the input that tells a deduplicating
    /// assembly apart both from one that appends and from one that collapses
    /// everything onto the contract.
    const NEVER_SETTLES: u8 = 6;

    /// #5403 H1: the FIRST record for a violating contract must carry one finding
    /// per broken law, not one per violating case.
    ///
    /// This is the first test in this module to run the assembly production runs.
    /// Every other test here hand-builds a `CheckedContract`, which is why
    /// `checked_contracts` — the function that turns a tick into records — had no
    /// direct coverage at all, and why three review rounds each found a defect on
    /// this path.
    ///
    /// The input is a real probe of the deliberately-defective fixture contract in
    /// mode 6 (NEVER_SETTLES), which breaks two merge laws on most generated cases.
    /// `probe_one` pushes one `Finding` per violating case and `generate_cases`
    /// produces up to `MAX_CASES_PER_PROBE` (64) of them, so the tick that ARRIVES
    /// here legitimately repeats a property many times; deduplicating it is the
    /// assembly's job. Before the fix this produced thirteen findings on the record,
    /// eleven of them duplicates, which the card renders as eleven identical rows —
    /// and which then persist for the record's whole residency, because later ticks
    /// deduplicate AGAINST them.
    ///
    /// The first probe of a violating contract is where the violation is found, so
    /// the un-deduplicated path was the normal path, not a corner: `record`'s
    /// deduplication lives only in its merge arm, which cannot run until the
    /// contract is already in the window.
    #[tokio::test(flavor = "multi_thread")]
    async fn a_first_record_carries_one_finding_per_broken_law_not_per_case()
    -> Result<(), Box<dyn std::error::Error>> {
        let (id, report, findings) =
            crate::conformance::shadow::probe_fixture_contract(NEVER_SETTLES).await?;

        // The input must genuinely repeat, or this test cannot distinguish a
        // deduplicating assembly from one that appends. Established from the probe's
        // own output rather than declared.
        let distinct: std::collections::BTreeSet<&str> = findings
            .iter()
            .map(|f| f.violation.property.as_str())
            .collect();
        assert!(
            distinct.len() > 1,
            "the fixture stopped breaking more than one law, so this test can no \
             longer tell deduplication from collapsing everything into one row: \
             {distinct:?}"
        );
        assert!(
            findings.len() > distinct.len(),
            "the probe returned no repeated property ({} findings, {} distinct), so \
             this test would pass against an assembly that never deduplicates",
            findings.len(),
            distinct.len()
        );

        // The production assembly, on the production inputs.
        let records = checked_contracts(&report.judged, &findings);
        assert_eq!(
            records.len(),
            1,
            "one probed contract must produce exactly one record: {records:?}"
        );
        let record = &records[0];
        assert_eq!(record.contract, id);

        let rendered: Vec<&str> = record.findings.iter().map(|f| f.property).collect();
        assert_eq!(
            rendered.len(),
            distinct.len(),
            "the first record for a violating contract carried one finding per \
             violating CASE, so its card renders {} rows for {} broken laws — and \
             they persist, because later ticks deduplicate against them. got: \
             {rendered:?}",
            rendered.len(),
            distinct.len()
        );
        for property in &distinct {
            assert_eq!(
                rendered.iter().filter(|p| *p == property).count(),
                1,
                "{property} appears {} times on the record",
                rendered.iter().filter(|p| *p == property).count()
            );
        }

        // And through the window and the view the page actually reads.
        let mut status = MergeCheckStatus::default();
        status.record(
            records,
            report.judged.len(),
            report.without_verdict,
            Instant::now(),
        );
        let view = status.view_for(Some(&id), Instant::now());
        let seen = view.contract.expect("the probed contract is in the window");
        assert_eq!(
            seen.findings.len(),
            distinct.len(),
            "the view the contract-detail page reads carried duplicate findings"
        );
        assert_eq!(
            (seen.verdicts, seen.inconclusive),
            (report.judged[0].verdicts, report.judged[0].inconclusive),
            "the record's case counts are not the probe's own"
        );
        Ok(())
    }

    /// The first record and a re-record must agree.
    ///
    /// `record` has two arms — insert and merge — and only the merge arm ever
    /// deduplicated. Feeding the SAME tick twice drives both, so an assembly that
    /// deduplicates in one place and not the other shows up as the two arms
    /// disagreeing rather than as a number a reader has to know is wrong.
    #[tokio::test(flavor = "multi_thread")]
    async fn re_recording_a_tick_does_not_change_what_the_page_shows()
    -> Result<(), Box<dyn std::error::Error>> {
        let (id, report, findings) =
            crate::conformance::shadow::probe_fixture_contract(NEVER_SETTLES).await?;

        let mut status = MergeCheckStatus::default();
        status.record(
            checked_contracts(&report.judged, &findings),
            report.judged.len(),
            report.without_verdict,
            Instant::now(),
        );
        let after_first = status
            .record_for(&id)
            .expect("the probed contract is in the window")
            .findings
            .len();

        status.record(
            checked_contracts(&report.judged, &findings),
            report.judged.len(),
            report.without_verdict,
            Instant::now(),
        );
        let after_second = status
            .record_for(&id)
            .expect("the probed contract is still in the window")
            .findings
            .len();

        assert_eq!(
            after_first, after_second,
            "the insert arm and the merge arm disagree about how many findings one \
             contract has, so what an operator sees depends on whether this is the \
             tick that first caught the contract"
        );
        Ok(())
    }

    /// #5403 L1: the unreachable branch must not build a self-contradictory record.
    ///
    /// `checked_contracts` handles a finding whose contract is not in `judged`. That
    /// is structurally unreachable today — a finding comes from a `Violated` outcome,
    /// which IS a verdict, so `probe_one` always puts its contract in `judged` — and
    /// it is kept deliberately, because dropping the finding would render a real
    /// violation as a contract nobody looked at.
    ///
    /// It built the record with `verdicts: 0`, so if it ever fired the card would say
    /// "0 reached a verdict" directly beside a Violation row: a page contradicting
    /// itself, with nothing to tell the operator which half to believe. The branch
    /// exists precisely for the case where an assumption has already broken, which is
    /// the worst moment for the output to be incoherent.
    #[test]
    fn a_finding_for_an_unjudged_contract_does_not_render_as_zero_verdicts() {
        use crate::conformance::property::{ConformanceProperty, OutputDigest, Violation};

        let orphan = instance(200);
        let finding = Finding {
            contract: orphan,
            violation: Violation {
                property: ConformanceProperty::StateCommutativity,
                severity: Severity::Violation,
                left: OutputDigest::of(b"a"),
                right: OutputDigest::of(b"b"),
                detail: "synthesised for the unreachable branch".to_string(),
            },
            would_remove: true,
        };

        // Deliberately empty: that is what makes this the branch under test.
        let records = checked_contracts(&[], std::slice::from_ref(&finding));
        let record = records.iter().find(|c| c.contract == orphan).expect(
            "the finding must not be dropped — a discarded violation renders \
                     as a contract nobody looked at",
        );
        assert_eq!(record.findings.len(), 1);
        assert!(
            record.verdicts >= 1,
            "a record carrying a violation reported {} verdicts, so the card would \
             render '0 reached a verdict' beside a Violation row — a finding IS a \
             verdict",
            record.verdicts
        );
    }

    fn record_one(s: &mut MergeCheckStatus, c: CheckedContract) {
        s.record([c], 1, 0, Instant::now());
    }

    /// The same contract failing the same law repeatedly is one row, not forty.
    #[test]
    fn repeated_findings_for_one_contract_and_property_collapse() {
        let mut s = MergeCheckStatus::default();
        record_one(&mut s, checked(1, vec![finding(1, "state_commutativity")]));
        record_one(&mut s, checked(1, vec![finding(1, "state_commutativity")]));
        assert_eq!(
            s.findings_for(&instance(1)).count(),
            1,
            "the same law failing twice produced two rows"
        );
    }

    /// Distinct laws on one contract stay distinct — collapsing on contract alone
    /// would hide that a contract breaks more than one rule.
    #[test]
    fn different_properties_on_one_contract_stay_separate() {
        let mut s = MergeCheckStatus::default();
        record_one(&mut s, checked(2, vec![finding(2, "state_commutativity")]));
        record_one(&mut s, checked(2, vec![finding(2, "state_associativity")]));
        assert_eq!(
            s.findings_for(&instance(2)).count(),
            2,
            "two different broken laws collapsed into one row"
        );
    }

    /// The window is capped, so a peer that checks many contracts cannot grow this
    /// without bound.
    #[test]
    fn the_checked_window_is_bounded() {
        let mut s = MergeCheckStatus::default();
        for i in 0..(MAX_REMEMBERED_CHECKED + 40) {
            let n = (i % 251) as u8;
            record_one(&mut s, checked(n, vec![finding(n, "state_idempotence")]));
        }
        assert!(
            s.checked.len() <= MAX_REMEMBERED_CHECKED,
            "the checked window grew past the cap: {}",
            s.checked.len()
        );
    }

    /// #5403 H1: a finding must not be lost while its contract is still remembered
    /// as checked.
    ///
    /// The two-list version kept a 256-entry checked list and a 64-entry findings
    /// list, so a contract that stayed inside the first and fell out of the second
    /// rendered as "checked, no violation found" — for a contract the checker had
    /// positively found violating. This asserts the property that made it possible is
    /// gone: while the contract is in the window at all, its findings are with it.
    #[test]
    fn a_finding_survives_other_contracts_filling_the_window() {
        let mut s = MergeCheckStatus::default();
        record_one(&mut s, checked(1, vec![finding(1, "state_commutativity")]));
        // Well past the old 64-entry findings cap, and short of the window cap, so the
        // contract is unambiguously still "recently checked".
        for i in 0..100u8 {
            let n = i + 2;
            record_one(&mut s, checked(n, vec![finding(n, "state_idempotence")]));
        }
        assert!(
            s.was_checked(&instance(1)),
            "the contract fell out of the checked window, so this test no longer \
             exercises the case it names"
        );
        assert_eq!(
            s.findings_for(&instance(1)).count(),
            1,
            "a remembered contract lost its finding, so its page renders a green \
             'no violation found' pill for a contract found violating"
        );
    }

    /// Re-checking moves a contract to the front, so the window evicts by
    /// least-recently-checked rather than by first-seen.
    #[test]
    fn rechecking_a_contract_moves_it_to_the_front() {
        let mut s = MergeCheckStatus::default();
        record_one(&mut s, checked(1, vec![]));
        record_one(&mut s, checked(2, vec![]));
        record_one(&mut s, checked(1, vec![]));
        assert_eq!(
            s.checked.first().map(|c| c.contract),
            Some(instance(1)),
            "a re-checked contract was left where it was, so a contract checked every \
             tick sinks toward eviction while an idle one is retained"
        );
    }

    /// Per-contract case counts accumulate, so the page can say how much looking
    /// actually happened for THIS contract.
    #[test]
    fn per_contract_case_counts_accumulate_across_ticks() {
        let mut s = MergeCheckStatus::default();
        s.record(
            [CheckedContract {
                contract: instance(1),
                verdicts: 2,
                inconclusive: 5,
                findings: Vec::new(),
                checked_at: Instant::now(),
            }],
            1,
            0,
            Instant::now(),
        );
        s.record(
            [CheckedContract {
                contract: instance(1),
                verdicts: 3,
                inconclusive: 1,
                findings: Vec::new(),
                checked_at: Instant::now(),
            }],
            1,
            0,
            Instant::now(),
        );
        let record = s
            .record_for(&instance(1))
            .expect("contract is in the window");
        assert_eq!((record.verdicts, record.inconclusive), (5, 6));
    }

    /// The unjudgeable count is carried, not inferred.
    ///
    /// An unjudgeable contract renders identically to a clean one, so a panel showing
    /// only "0 violations" would be actively misleading. On the live capture peer 17%
    /// of hosted contracts could not be judged at all and nothing said so.
    #[test]
    fn contracts_without_a_verdict_are_reported_separately() {
        let mut s = MergeCheckStatus::default();
        s.record([], 10, 3, Instant::now());
        assert_eq!(s.without_verdict_last_tick, 3);
        assert!(
            s.checked.is_empty(),
            "no records, which is exactly why the unjudged count has to be its own \
             number rather than inferred from an empty list"
        );
    }

    /// A snapshot ages. A checker that stopped publishing must not keep presenting
    /// its last tick as a current result.
    #[test]
    fn a_stale_snapshot_says_so() {
        let mut s = MergeCheckStatus::default();
        let published = Instant::now();
        record_one(&mut s, checked(1, vec![]));
        s.published_at = published;

        let fresh = s.view_for(Some(&instance(1)), published + STALE_AFTER / 2);
        assert!(!fresh.stale, "a tick within the window read as stale");

        let old = s.view_for(Some(&instance(1)), published + STALE_AFTER * 2);
        assert!(
            old.stale,
            "a snapshot older than {STALE_AFTER:?} did not report itself stale, so a \
             peer whose probe has been dead for a week keeps serving its last tick as \
             a current clean result"
        );
        assert!(old.published_secs_ago >= STALE_AFTER.as_secs() * 2);
    }

    /// A view carries the requested contract's record and nothing else's.
    #[test]
    fn a_view_clones_one_record_not_the_window() {
        let mut s = MergeCheckStatus::default();
        record_one(&mut s, checked(1, vec![finding(1, "state_commutativity")]));
        record_one(&mut s, checked(2, vec![finding(2, "delta_idempotence")]));

        let view = s.view_for(Some(&instance(1)), Instant::now());
        let record = view.contract.expect("contract 1 is in the window");
        assert_eq!(record.contract, instance(1));
        assert_eq!(record.findings.len(), 1);
        assert_eq!(record.findings[0].property, "state_commutativity");

        assert!(
            s.view_for(Some(&instance(9)), Instant::now())
                .contract
                .is_none(),
            "a contract outside the window must read as absent, not as clean"
        );
    }
}
