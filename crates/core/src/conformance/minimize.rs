//! Shrinking a failing case to the smallest witness that still fails.
//!
//! Two reasons this is not a nicety.
//!
//! **Evidence has a hard size bound.** A violation discovered using two 400 KB
//! states produces evidence over [`MAX_EVIDENCE_INPUT_BYTES`], which
//! [`ConformanceEvidence::check_bounds`] rejects. Without shrinking, the mechanism
//! would work well for contracts with small states and silently fail to propagate
//! anything about contracts with large ones. That is the wrong way round: the
//! large-state contracts are the expensive ones.
//!
//! **A small witness is a usable bug report.** The evidence that removes a contract
//! is also the most useful thing its author will ever receive, and "these two 400 KB
//! blobs disagree" is not a bug report. Two four-byte states that disagree is.
//!
//! The shrinking is deliberately unambitious: it substitutes smaller states drawn
//! from material the peer already has, and never invents inputs. Generating new
//! states would mean handing a contract bytes it never produced, which is the
//! fastest known route to a false positive.
//!
//! [`MAX_EVIDENCE_INPUT_BYTES`]: super::evidence::MAX_EVIDENCE_INPUT_BYTES
//! [`ConformanceEvidence::check_bounds`]: super::evidence::ConformanceEvidence::check_bounds

use super::oracle::ConformanceOracle;
use super::policy::{RemovalTier, removal_tier};
use super::property::{ConformanceProperty, PropertyOutcome};
use super::verifier::{Bytes, ConformanceCase, verify_case};

/// Bound on how many extra verifications a shrink may cost.
///
/// Shrinking runs the contract repeatedly, so it is charged against the same
/// suspicion budget as everything else here. Running out means the case is emitted
/// at whatever size it had reached, which is always at least as small as the
/// original.
pub const DEFAULT_MAX_SHRINK_ATTEMPTS: usize = 32;

#[derive(Debug, Clone)]
pub struct MinimizeConfig {
    pub max_attempts: usize,
}

impl Default for MinimizeConfig {
    fn default() -> Self {
        Self {
            max_attempts: DEFAULT_MAX_SHRINK_ATTEMPTS,
        }
    }
}

/// What a shrink accomplished. Returned for telemetry, so a peer can report that it
/// found something it could not shrink small enough to send.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MinimizeReport {
    pub original_bytes: usize,
    pub final_bytes: usize,
    pub attempts: usize,
}

/// Shrink `case` while it keeps producing a violation of the same property with
/// the same removal tier (`policy::removal_tier`).
///
/// The tier is part of what the finding is. A never-settling `StateIdempotence`
/// case can have a smaller input that settles after a rewrite: the same property,
/// but report-only. Swapping that in would turn removal-eligible evidence into
/// evidence no peer may act on.
///
/// Tier equality is deliberately coarse. It does not pin the settling class, so
/// a `NeverSettled` case may shrink to an `Indeterminate` one: both are
/// removal-eligible today, and pinning the class would refuse shrinks that change
/// nothing a receiving peer decides.
///
/// Returns the case unchanged if it does not currently violate anything: shrinking
/// a case that does not fail would be searching for a failure, which is a different
/// and much more dangerous activity.
///
/// `candidates` are other states the peer has observed for this same contract.
/// Only states the contract itself produced or accepted are ever substituted.
///
/// A caller that has already verified `case` should use [`minimize_toward`] with
/// the finding it checked, so the target cannot differ from what it decided on.
pub fn minimize<O: ConformanceOracle + ?Sized>(
    oracle: &mut O,
    case: &ConformanceCase,
    state_candidates: &[Bytes],
    delta_candidates: &[Bytes],
    config: &MinimizeConfig,
) -> (ConformanceCase, MinimizeReport) {
    let Some(target) = violated_finding(oracle, case) else {
        let bytes = case.input_bytes();
        let report = MinimizeReport {
            original_bytes: bytes,
            final_bytes: bytes,
            attempts: 0,
        };
        return (case.clone(), report);
    };
    // The verification that derived the target is charged to the budget too.
    shrink(
        oracle,
        case,
        target,
        state_candidates,
        delta_candidates,
        config,
        1,
    )
}

/// [`minimize`] toward a finding the caller already established: shrink `case`
/// while each trial violates `target.0` with removal tier `target.1`.
///
/// Re-deriving the target here would verify `case` again, and against a
/// contract whose verdict is not stable between runs (a nondeterministic one, or
/// one reading the clock across a boundary) that second answer can differ from
/// the one the caller decided on. The shrink would then chase a finding the
/// caller never checked.
pub fn minimize_toward<O: ConformanceOracle + ?Sized>(
    oracle: &mut O,
    case: &ConformanceCase,
    target: (ConformanceProperty, RemovalTier),
    state_candidates: &[Bytes],
    delta_candidates: &[Bytes],
    config: &MinimizeConfig,
) -> (ConformanceCase, MinimizeReport) {
    shrink(
        oracle,
        case,
        target,
        state_candidates,
        delta_candidates,
        config,
        0,
    )
}

/// The shrink both entry points share. `attempts_spent` is what the caller has
/// already charged to `config.max_attempts`.
fn shrink<O: ConformanceOracle + ?Sized>(
    oracle: &mut O,
    case: &ConformanceCase,
    finding: (ConformanceProperty, RemovalTier),
    state_candidates: &[Bytes],
    delta_candidates: &[Bytes],
    config: &MinimizeConfig,
    attempts_spent: usize,
) -> (ConformanceCase, MinimizeReport) {
    let original_bytes = case.input_bytes();
    let mut report = MinimizeReport {
        original_bytes,
        final_bytes: original_bytes,
        attempts: attempts_spent,
    };

    // Smallest first, so the first substitution that sticks is the best available
    // rather than merely an improvement.
    //
    // States and deltas are kept in SEPARATE pools. Sharing one pool meant a delta
    // slot could be filled with a serialized state — bytes the contract never
    // produced as a delta — which is exactly the "never invents inputs" promise this
    // module opens with. A contract whose delta decoder happens to accept state bytes
    // would then yield a witness demonstrating a law break on an input that cannot
    // occur, and the evidence would be unfalsifiable rather than merely wrong.
    let mut ordered_states: Vec<Bytes> = state_candidates.to_vec();
    ordered_states.sort_by_key(|c| c.len());
    let mut ordered_deltas: Vec<Bytes> = delta_candidates.to_vec();
    ordered_deltas.sort_by_key(|c| c.len());

    let mut current = case.clone();

    // States first, then deltas.
    //
    // Deltas matter as much as states and were once skipped, which quietly defeated
    // the whole exercise for the delta properties: for a delta-law violation the bulk
    // of the witness lives in the delta, so a large one could never be shrunk under
    // the evidence size bound and `check_bounds` simply rejected it. A minimizer that
    // cannot shrink the thing the finding is about is not minimizing.
    for slot in 0..current.states.len() {
        if !shrink_slot(
            oracle,
            &mut current,
            finding,
            &ordered_states,
            config,
            &mut report,
            |case, i| &mut case.states[i],
            slot,
        ) {
            break;
        }
    }
    for slot in 0..current.deltas.len() {
        if !shrink_slot(
            oracle,
            &mut current,
            finding,
            &ordered_deltas,
            config,
            &mut report,
            |case, i| &mut case.deltas[i],
            slot,
        ) {
            break;
        }
    }

    report.final_bytes = current.input_bytes();
    (current, report)
}

/// Try to replace one input slot with the smallest candidate that still fails the
/// same law with the same removal tier. Returns false when the attempt budget is
/// spent.
#[allow(clippy::too_many_arguments)]
fn shrink_slot<O: ConformanceOracle + ?Sized>(
    oracle: &mut O,
    current: &mut ConformanceCase,
    finding: (ConformanceProperty, RemovalTier),
    ordered: &[Bytes],
    config: &MinimizeConfig,
    report: &mut MinimizeReport,
    slot_of: impl Fn(&mut ConformanceCase, usize) -> &mut Bytes,
    slot: usize,
) -> bool {
    for candidate in ordered {
        if report.attempts >= config.max_attempts {
            return false;
        }
        {
            let existing = slot_of(current, slot);
            if candidate.len() >= existing.len() {
                // Ordered by size, so nothing later in this list is smaller either.
                break;
            }
        }

        let mut trial = current.clone();
        *slot_of(&mut trial, slot) = candidate.clone();
        report.attempts += 1;

        // The trial must fail the SAME law. A trial that fails a different one is a
        // different finding, and quietly swapping it in would relabel the evidence as
        // being about something it never demonstrated. The tier is compared for
        // the same reason (see `minimize`).
        if violated_finding(oracle, &trial) == Some(finding) {
            *current = trial;
            break;
        }
    }
    true
}

fn violated_finding<O: ConformanceOracle + ?Sized>(
    oracle: &mut O,
    case: &ConformanceCase,
) -> Option<(ConformanceProperty, RemovalTier)> {
    match verify_case(oracle, case) {
        PropertyOutcome::Violated(violation) => {
            Some((violation.property, removal_tier(&violation)))
        }
        PropertyOutcome::Holds | PropertyOutcome::Inconclusive(_) => None,
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use freenet_stdlib::prelude::{
        RelatedContracts, State, UpdateData, UpdateModification, ValidateResult,
    };

    use super::*;
    use crate::conformance::oracle::OracleError;

    /// Last-write-wins over opaque bytes: commutative only when both sides already
    /// agree, so any two distinct states are a witness. That makes it a good subject
    /// for shrinking, because many witnesses of different sizes exist.
    struct LastWriteWins;

    impl ConformanceOracle for LastWriteWins {
        fn validate_state(
            &mut self,
            _state: &[u8],
            _related: &RelatedContracts<'_>,
        ) -> Result<ValidateResult, OracleError> {
            Ok(ValidateResult::Valid)
        }

        fn update_state(
            &mut self,
            _state: &[u8],
            updates: &[UpdateData<'_>],
        ) -> Result<UpdateModification<'static>, OracleError> {
            match updates.first() {
                Some(UpdateData::State(incoming)) => Ok(UpdateModification::valid(State::from(
                    incoming.as_ref().to_vec(),
                ))),
                _ => Err(OracleError::contract("unsupported update")),
            }
        }

        fn summarize_state(&mut self, state: &[u8]) -> Result<Vec<u8>, OracleError> {
            Ok(state.to_vec())
        }

        fn get_state_delta(
            &mut self,
            state: &[u8],
            _summary: &[u8],
        ) -> Result<Vec<u8>, OracleError> {
            Ok(state.to_vec())
        }
    }

    /// Always holds: shrinking must refuse to touch it, because looking for a
    /// smaller *failing* input against a contract that is not failing is searching
    /// for a failure rather than reducing one.
    ///
    /// Merge is lexicographic max, which is a genuine join-semilattice: commutative,
    /// associative and idempotent. Returning the local state unchanged would NOT
    /// work here — that is mutual rejection, the #5153 defect, and it fails
    /// commutativity immediately.
    struct AlwaysConforming;

    impl ConformanceOracle for AlwaysConforming {
        fn validate_state(
            &mut self,
            _state: &[u8],
            _related: &RelatedContracts<'_>,
        ) -> Result<ValidateResult, OracleError> {
            Ok(ValidateResult::Valid)
        }

        fn update_state(
            &mut self,
            state: &[u8],
            updates: &[UpdateData<'_>],
        ) -> Result<UpdateModification<'static>, OracleError> {
            let mut merged = state.to_vec();
            for update in updates {
                if let UpdateData::State(incoming) = update {
                    let incoming = incoming.as_ref();
                    if incoming > merged.as_slice() {
                        merged = incoming.to_vec();
                    }
                }
            }
            Ok(UpdateModification::valid(State::from(merged)))
        }

        fn summarize_state(&mut self, state: &[u8]) -> Result<Vec<u8>, OracleError> {
            Ok(state.to_vec())
        }

        fn get_state_delta(
            &mut self,
            _state: &[u8],
            _summary: &[u8],
        ) -> Result<Vec<u8>, OracleError> {
            Ok(Vec::new())
        }
    }

    fn bytes(len: usize, fill: u8) -> Bytes {
        Arc::from(vec![fill; len].as_slice())
    }

    fn big_case() -> ConformanceCase {
        ConformanceCase::new(
            ConformanceProperty::StateCommutativity,
            vec![bytes(4096, 1), bytes(4096, 2)],
        )
    }

    /// The point of the exercise: a finding discovered with large states must come
    /// out small enough to fit in evidence, or it can never be propagated and the
    /// mechanism silently works only for contracts with small states.
    #[test]
    fn a_large_witness_shrinks_to_a_small_one() {
        let candidates = vec![bytes(2048, 3), bytes(4, 4), bytes(8, 5)];
        let (minimized, report) = minimize(
            &mut LastWriteWins,
            &big_case(),
            &candidates,
            &candidates,
            &MinimizeConfig::default(),
        );

        assert!(
            report.final_bytes < report.original_bytes,
            "shrinking achieved nothing: {report:?}"
        );
        assert!(
            report.final_bytes <= 16,
            "expected the smallest available witness, got {} bytes",
            report.final_bytes
        );
        // And it must still be the same finding it started as.
        assert_eq!(
            violated_finding(&mut LastWriteWins, &minimized).map(|(property, _)| property),
            Some(ConformanceProperty::StateCommutativity)
        );
    }

    /// Shrinking must never convert a violation into a non-violation. If it could,
    /// evidence would be emitted for a case that does not reproduce, and every
    /// recipient would correctly refuse to confirm it.
    #[test]
    fn shrinking_never_loses_the_violation() {
        let candidates: Vec<Bytes> = (1u8..20).map(|i| bytes(i as usize, i)).collect();
        let (minimized, _) = minimize(
            &mut LastWriteWins,
            &big_case(),
            &candidates,
            &candidates,
            &MinimizeConfig::default(),
        );
        assert!(verify_case(&mut LastWriteWins, &minimized).is_violation());
    }

    #[test]
    fn a_case_that_does_not_fail_is_returned_untouched() {
        let original = big_case();
        let candidates = vec![bytes(4, 9)];
        let (minimized, report) = minimize(
            &mut AlwaysConforming,
            &original,
            &candidates,
            &candidates,
            &MinimizeConfig::default(),
        );
        assert_eq!(minimized.states, original.states);
        assert_eq!(report.final_bytes, report.original_bytes);
    }

    /// Deltas must shrink too. For a delta-law violation the witness lives mostly in
    /// the delta, so a minimizer that only touched states left exactly those findings
    /// over the evidence size bound — where `check_bounds` rejects them and no peer
    /// ever sees them.
    #[test]
    fn a_large_delta_witness_shrinks_too() {
        // Order-dependent apply: concatenation. Any two distinct deltas witness it.
        struct Concat;
        impl ConformanceOracle for Concat {
            fn validate_state(
                &mut self,
                _state: &[u8],
                _related: &RelatedContracts<'_>,
            ) -> Result<ValidateResult, OracleError> {
                Ok(ValidateResult::Valid)
            }
            fn update_state(
                &mut self,
                state: &[u8],
                updates: &[UpdateData<'_>],
            ) -> Result<UpdateModification<'static>, OracleError> {
                let mut out = state.to_vec();
                for update in updates {
                    if let UpdateData::Delta(d) = update {
                        out.extend_from_slice(d.as_ref());
                    }
                }
                Ok(UpdateModification::valid(State::from(out)))
            }
            fn summarize_state(&mut self, state: &[u8]) -> Result<Vec<u8>, OracleError> {
                Ok(state.to_vec())
            }
            fn get_state_delta(
                &mut self,
                state: &[u8],
                _summary: &[u8],
            ) -> Result<Vec<u8>, OracleError> {
                Ok(state.to_vec())
            }
        }

        let case = ConformanceCase::new(
            ConformanceProperty::DeltaPermutationInvariance,
            vec![bytes(4, 1)],
        )
        .with_deltas(vec![bytes(4096, 2), bytes(4096, 3)]);
        let candidates = vec![bytes(2, 7), bytes(2, 8)];

        let (minimized, report) = minimize(
            &mut Concat,
            &case,
            &candidates,
            &candidates,
            &MinimizeConfig::default(),
        );
        assert!(
            report.final_bytes < report.original_bytes / 2,
            "the delta witness did not shrink: {report:?}"
        );
        assert!(verify_case(&mut Concat, &minimized).is_violation());
    }

    /// Shrinking must never move an observed STATE into a DELTA slot.
    ///
    /// This module opens by promising it never invents inputs, because handing a
    /// contract bytes it never produced is the fastest route to a false positive.
    /// One shared candidate pool quietly broke that promise: a delta slot could be
    /// filled with a serialized state. For a contract whose delta decoder happens to
    /// accept state bytes, the resulting witness demonstrates a law break on an input
    /// that cannot occur — evidence that is unfalsifiable rather than merely wrong.
    #[test]
    fn shrinking_never_puts_a_state_into_a_delta_slot() {
        // An append-on-delta contract: order-dependent, so
        // DeltaPermutationInvariance fails and there is a witness to shrink.
        struct Append;
        impl ConformanceOracle for Append {
            fn validate_state(
                &mut self,
                _state: &[u8],
                _related: &RelatedContracts<'_>,
            ) -> Result<ValidateResult, OracleError> {
                Ok(ValidateResult::Valid)
            }
            fn update_state(
                &mut self,
                state: &[u8],
                updates: &[UpdateData<'_>],
            ) -> Result<UpdateModification<'static>, OracleError> {
                let mut out = state.to_vec();
                for update in updates {
                    if let UpdateData::Delta(d) = update {
                        out.extend_from_slice(d.as_ref());
                    }
                }
                Ok(UpdateModification::valid(State::from(out)))
            }
            fn summarize_state(&mut self, state: &[u8]) -> Result<Vec<u8>, OracleError> {
                Ok(state.to_vec())
            }
            fn get_state_delta(
                &mut self,
                state: &[u8],
                _summary: &[u8],
            ) -> Result<Vec<u8>, OracleError> {
                Ok(state.to_vec())
            }
        }

        // Distinct alphabets, so the origin of any surviving byte is unambiguous.
        let state_pool = vec![bytes(4, 0xAA), bytes(8, 0xAB)];
        let delta_pool = vec![bytes(4, 0xDD), bytes(8, 0xDE)];

        let case = ConformanceCase::new(
            ConformanceProperty::DeltaPermutationInvariance,
            vec![bytes(4096, 1)],
        )
        .with_deltas(vec![bytes(4096, 2), bytes(4096, 3)]);

        let (minimized, _) = minimize(
            &mut Append,
            &case,
            &state_pool,
            &delta_pool,
            &MinimizeConfig::default(),
        );

        for (i, delta) in minimized.deltas.iter().enumerate() {
            assert!(
                !delta.iter().any(|b| *b == 0xAA || *b == 0xAB),
                "delta slot {i} contains bytes from the STATE pool, i.e. an input the \
                 contract never produced as a delta"
            );
        }
        for (i, state) in minimized.states.iter().enumerate() {
            assert!(
                !state.iter().any(|b| *b == 0xDD || *b == 0xDE),
                "state slot {i} contains bytes from the DELTA pool"
            );
        }
    }

    #[test]
    fn shrinking_respects_its_attempt_budget() {
        let candidates: Vec<Bytes> = (1u8..100).map(|i| bytes(i as usize, i)).collect();
        let config = MinimizeConfig { max_attempts: 3 };
        let (_, report) = minimize(
            &mut LastWriteWins,
            &big_case(),
            &candidates,
            &candidates,
            &config,
        );
        assert!(
            report.attempts <= config.max_attempts,
            "shrinking ran {} verifications against a budget of {}",
            report.attempts,
            config.max_attempts
        );
    }

    /// Nothing smaller to substitute means nothing to do. Worth pinning because the
    /// obvious loop bug here is to substitute a *larger* candidate and call it
    /// progress.
    #[test]
    fn no_smaller_candidate_leaves_the_case_alone() {
        let original = ConformanceCase::new(
            ConformanceProperty::StateCommutativity,
            vec![bytes(4, 1), bytes(4, 2)],
        );
        let candidates = vec![bytes(64, 3), bytes(128, 4)];
        let (minimized, report) = minimize(
            &mut LastWriteWins,
            &original,
            &candidates,
            &candidates,
            &MinimizeConfig::default(),
        );
        assert_eq!(minimized.states, original.states);
        assert_eq!(report.final_bytes, report.original_bytes);
    }

    /// Short states are canonicalized by sorting (settles after one rewrite, so
    /// report-only); longer ones grow on every merge (never settles, so
    /// removal-eligible). Both are `StateIdempotence` violations.
    ///
    /// fdev's `minimisation_keeps_a_removal_eligible_finding_removal_eligible` has
    /// its own copy (fdev cannot reach this test module); keep the two in step.
    struct SettlesOnlyWhenShort;

    impl ConformanceOracle for SettlesOnlyWhenShort {
        fn validate_state(
            &mut self,
            _state: &[u8],
            _related: &RelatedContracts<'_>,
        ) -> Result<ValidateResult, OracleError> {
            Ok(ValidateResult::Valid)
        }

        fn update_state(
            &mut self,
            state: &[u8],
            _updates: &[UpdateData<'_>],
        ) -> Result<UpdateModification<'static>, OracleError> {
            let mut out = state.to_vec();
            if out.len() > 4 {
                out.push(0);
            } else {
                out.sort_unstable();
            }
            Ok(UpdateModification::valid(State::from(out)))
        }

        fn summarize_state(&mut self, state: &[u8]) -> Result<Vec<u8>, OracleError> {
            Ok(state.to_vec())
        }

        fn get_state_delta(
            &mut self,
            state: &[u8],
            _summary: &[u8],
        ) -> Result<Vec<u8>, OracleError> {
            Ok(state.to_vec())
        }
    }

    /// Shrinking keeps the removal tier as well as the property. The only smaller
    /// candidate still breaks idempotence, but settles after a rewrite, which is
    /// report-only: accepting it would turn removal-eligible evidence into evidence
    /// no peer may act on.
    #[test]
    fn shrinking_does_not_trade_a_removal_eligible_finding_for_a_report_only_one() {
        let original =
            ConformanceCase::new(ConformanceProperty::StateIdempotence, vec![bytes(64, 7)]);
        let unsorted: Bytes = Arc::from(vec![3u8, 2, 1].as_slice());
        let PropertyOutcome::Violated(smaller) = verify_case(
            &mut SettlesOnlyWhenShort,
            &ConformanceCase::new(
                ConformanceProperty::StateIdempotence,
                vec![unsorted.clone()],
            ),
        ) else {
            panic!("the fixture's short state must break idempotence");
        };
        assert_eq!(
            removal_tier(&smaller),
            RemovalTier::ReportOnly,
            "fixture: the smaller candidate must be the report-only class"
        );
        assert_eq!(
            violated_finding(&mut SettlesOnlyWhenShort, &original),
            Some((
                ConformanceProperty::StateIdempotence,
                RemovalTier::RemovalEligible
            )),
            "fixture: the original must be the removal-eligible class"
        );

        let (minimized, _) = minimize(
            &mut SettlesOnlyWhenShort,
            &original,
            &[unsorted],
            &[],
            &MinimizeConfig::default(),
        );
        assert_eq!(minimized.states, original.states);
    }

    /// A contract whose verdict changes between runs: while `sorts`, every merge
    /// sorts (settles after one rewrite, report-only); otherwise every merge
    /// grows the state (never settles, removal-eligible).
    struct Flips {
        sorts: bool,
    }

    impl ConformanceOracle for Flips {
        fn validate_state(
            &mut self,
            _state: &[u8],
            _related: &RelatedContracts<'_>,
        ) -> Result<ValidateResult, OracleError> {
            Ok(ValidateResult::Valid)
        }

        fn update_state(
            &mut self,
            state: &[u8],
            _updates: &[UpdateData<'_>],
        ) -> Result<UpdateModification<'static>, OracleError> {
            let mut out = state.to_vec();
            if self.sorts {
                out.sort_unstable();
            } else {
                out.push(0);
            }
            Ok(UpdateModification::valid(State::from(out)))
        }

        fn summarize_state(&mut self, state: &[u8]) -> Result<Vec<u8>, OracleError> {
            Ok(state.to_vec())
        }

        fn get_state_delta(
            &mut self,
            state: &[u8],
            _summary: &[u8],
        ) -> Result<Vec<u8>, OracleError> {
            Ok(state.to_vec())
        }
    }

    /// `minimize_toward` shrinks toward the finding it is given, never one it
    /// re-derives. The caller checked a removal-eligible finding; by the time it
    /// minimizes, the contract has flipped and the case (and a smaller candidate)
    /// now settle. Re-deriving the target would shrink toward the report-only
    /// finding the caller never checked.
    #[test]
    fn minimize_toward_does_not_re_derive_its_target() {
        let original = ConformanceCase::new(
            ConformanceProperty::StateIdempotence,
            vec![Arc::from(vec![9u8, 8, 7, 6, 5, 4, 3, 2].as_slice())],
        );
        let mut oracle = Flips { sorts: true };
        let (minimized, _) = minimize_toward(
            &mut oracle,
            &original,
            (
                ConformanceProperty::StateIdempotence,
                RemovalTier::RemovalEligible,
            ),
            &[Arc::from(vec![2u8, 1].as_slice())],
            &[],
            &MinimizeConfig::default(),
        );
        assert_eq!(minimized.states, original.states);
    }
}
