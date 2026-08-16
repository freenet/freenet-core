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

/// Shrink `case` while it keeps producing a violation of the same property.
///
/// Returns the case unchanged if it does not currently violate anything: shrinking
/// a case that does not fail would be searching for a failure, which is a different
/// and much more dangerous activity.
///
/// `candidates` are other states the peer has observed for this same contract.
/// Only states the contract itself produced or accepted are ever substituted.
pub fn minimize<O: ConformanceOracle + ?Sized>(
    oracle: &mut O,
    case: &ConformanceCase,
    candidates: &[Bytes],
    config: &MinimizeConfig,
) -> (ConformanceCase, MinimizeReport) {
    let original_bytes = case.input_bytes();
    let mut report = MinimizeReport {
        original_bytes,
        final_bytes: original_bytes,
        attempts: 0,
    };

    let property = match violated_property(oracle, case) {
        Some(property) => property,
        None => return (case.clone(), report),
    };
    report.attempts += 1;

    // Smallest first, so the first substitution that sticks is the best available
    // rather than merely an improvement.
    let mut ordered: Vec<Bytes> = candidates.to_vec();
    ordered.sort_by_key(|c| c.len());

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
            property,
            &ordered,
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
            property,
            &ordered,
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
/// same law. Returns false when the attempt budget is spent.
#[allow(clippy::too_many_arguments)]
fn shrink_slot<O: ConformanceOracle + ?Sized>(
    oracle: &mut O,
    current: &mut ConformanceCase,
    property: ConformanceProperty,
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
        // being about something it never demonstrated.
        if violated_property(oracle, &trial) == Some(property) {
            *current = trial;
            break;
        }
    }
    true
}

fn violated_property<O: ConformanceOracle + ?Sized>(
    oracle: &mut O,
    case: &ConformanceCase,
) -> Option<ConformanceProperty> {
    match verify_case(oracle, case) {
        PropertyOutcome::Violated(violation) => Some(violation.property),
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
            violated_property(&mut LastWriteWins, &minimized),
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

        let (minimized, report) =
            minimize(&mut Concat, &case, &candidates, &MinimizeConfig::default());
        assert!(
            report.final_bytes < report.original_bytes / 2,
            "the delta witness did not shrink: {report:?}"
        );
        assert!(verify_case(&mut Concat, &minimized).is_violation());
    }

    #[test]
    fn shrinking_respects_its_attempt_budget() {
        let candidates: Vec<Bytes> = (1u8..100).map(|i| bytes(i as usize, i)).collect();
        let config = MinimizeConfig { max_attempts: 3 };
        let (_, report) = minimize(&mut LastWriteWins, &big_case(), &candidates, &config);
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
            &MinimizeConfig::default(),
        );
        assert_eq!(minimized.states, original.states);
        assert_eq!(report.final_bytes, report.original_bytes);
    }
}
