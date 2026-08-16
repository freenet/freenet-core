//! Verifier tests against pure-Rust fake contracts.
//!
//! Every check here is built the same way: start from a contract that genuinely
//! satisfies the merge laws, break exactly one law, and assert that exactly that
//! check fires. The negative half matters as much as the positive half — a detector
//! that flags legitimate behaviour is not a stricter detector, it is a broken one,
//! and #4295 is the precedent (the only enforcement mechanism ever shipped for this
//! class had a 100% false-positive rate in production).
//!
//! The conforming baseline is a join-semilattice over sets of bytes: state is the
//! sorted, deduplicated set, merge is union. Union is commutative, associative and
//! idempotent by construction, so any failure reported against it is a bug in the
//! verifier rather than in the fake.

use std::sync::Arc;

use freenet_stdlib::prelude::{
    ContractInstanceId, RelatedContracts, State, UpdateData, UpdateModification, ValidateResult,
};

use super::evidence::{
    ConformanceEvidence, EvidenceRejected, MAX_EVIDENCE_INPUT_BYTES, MAX_EVIDENCE_RELATED,
};
use super::generator::{Corpus, GeneratorConfig, generate_cases};
use super::oracle::{ConformanceOracle, OracleError};
use super::property::{ConformanceProperty, Inconclusive, PropertyOutcome, Severity};
use super::verifier::{Bytes, ConformanceCase, verify_case};

// ---------------------------------------------------------------- fake contract

type ValidateFn = Box<dyn FnMut(&[u8]) -> Result<ValidateResult, OracleError>>;
type BinaryFn = Box<dyn FnMut(&[u8], &[u8]) -> Result<Vec<u8>, OracleError>>;
type UnaryFn = Box<dyn FnMut(&[u8]) -> Result<Vec<u8>, OracleError>>;

/// A contract made of closures, so a test can break one law and leave the rest intact.
struct Fake {
    validate: ValidateFn,
    merge: BinaryFn,
    apply: BinaryFn,
    summarize: UnaryFn,
    delta: BinaryFn,
}

impl Fake {
    /// The baseline: a set-union join-semilattice. Conforming in every respect.
    fn conforming() -> Self {
        Self {
            validate: Box::new(|state| {
                // Canonical form is the only valid form: sorted and deduplicated.
                if is_canonical(state) {
                    Ok(ValidateResult::Valid)
                } else {
                    Ok(ValidateResult::Invalid)
                }
            }),
            merge: Box::new(|a, b| Ok(union(a, b))),
            apply: Box::new(|a, d| Ok(union(a, d))),
            summarize: Box::new(|a| Ok(a.to_vec())),
            delta: Box::new(|a, summary| Ok(difference(a, summary))),
        }
    }

    fn merging(
        mut self,
        f: impl FnMut(&[u8], &[u8]) -> Result<Vec<u8>, OracleError> + 'static,
    ) -> Self {
        self.merge = Box::new(f);
        self
    }

    fn applying(
        mut self,
        f: impl FnMut(&[u8], &[u8]) -> Result<Vec<u8>, OracleError> + 'static,
    ) -> Self {
        self.apply = Box::new(f);
        self
    }

    fn summarizing(
        mut self,
        f: impl FnMut(&[u8]) -> Result<Vec<u8>, OracleError> + 'static,
    ) -> Self {
        self.summarize = Box::new(f);
        self
    }

    fn deltaing(
        mut self,
        f: impl FnMut(&[u8], &[u8]) -> Result<Vec<u8>, OracleError> + 'static,
    ) -> Self {
        self.delta = Box::new(f);
        self
    }

    fn validating(
        mut self,
        f: impl FnMut(&[u8]) -> Result<ValidateResult, OracleError> + 'static,
    ) -> Self {
        self.validate = Box::new(f);
        self
    }
}

impl ConformanceOracle for Fake {
    fn validate_state(
        &mut self,
        state: &[u8],
        _related: &RelatedContracts<'_>,
    ) -> Result<ValidateResult, OracleError> {
        (self.validate)(state)
    }

    fn update_state(
        &mut self,
        state: &[u8],
        updates: &[UpdateData<'_>],
    ) -> Result<UpdateModification<'static>, OracleError> {
        let mut current = state.to_vec();
        for update in updates {
            current = match update {
                UpdateData::State(incoming) => (self.merge)(&current, incoming.as_ref())?,
                UpdateData::Delta(delta) => (self.apply)(&current, delta.as_ref())?,
                // `UpdateData` is `#[non_exhaustive]` in freenet-stdlib, so a
                // wildcard is unavoidable, but the known variants are named so a
                // new one shows up here rather than silently joining the catch-all.
                other @ (UpdateData::StateAndDelta { .. }
                | UpdateData::RelatedState { .. }
                | UpdateData::RelatedDelta { .. }
                | UpdateData::RelatedStateAndDelta { .. })
                | other => {
                    return Err(OracleError::runtime(format!(
                        "fake contract does not handle {other:?}"
                    )));
                }
            };
        }
        Ok(UpdateModification::valid(State::from(current)))
    }

    fn summarize_state(&mut self, state: &[u8]) -> Result<Vec<u8>, OracleError> {
        (self.summarize)(state)
    }

    fn get_state_delta(&mut self, state: &[u8], summary: &[u8]) -> Result<Vec<u8>, OracleError> {
        (self.delta)(state, summary)
    }
}

fn is_canonical(state: &[u8]) -> bool {
    state.windows(2).all(|w| w[0] < w[1])
}

fn union(a: &[u8], b: &[u8]) -> Vec<u8> {
    let mut out: Vec<u8> = a.iter().chain(b.iter()).copied().collect();
    out.sort_unstable();
    out.dedup();
    out
}

fn difference(a: &[u8], b: &[u8]) -> Vec<u8> {
    a.iter().copied().filter(|x| !b.contains(x)).collect()
}

fn bytes(values: &[u8]) -> Bytes {
    Arc::from(values)
}

fn case(property: ConformanceProperty, states: &[&[u8]]) -> ConformanceCase {
    ConformanceCase::new(property, states.iter().map(|s| bytes(s)).collect())
}

#[track_caller]
fn assert_violates(outcome: PropertyOutcome, property: ConformanceProperty) {
    match outcome {
        PropertyOutcome::Violated(v) => assert_eq!(v.property, property, "wrong property flagged"),
        other @ (PropertyOutcome::Holds | PropertyOutcome::Inconclusive(_)) => {
            panic!("expected a {property} violation, got {other:?}")
        }
    }
}

#[track_caller]
fn assert_holds(outcome: PropertyOutcome) {
    assert_eq!(outcome, PropertyOutcome::Holds, "expected the law to hold");
}

#[track_caller]
fn assert_inconclusive(outcome: PropertyOutcome, expected: Inconclusive) {
    match outcome {
        PropertyOutcome::Inconclusive(reason) => assert_eq!(reason, expected),
        other @ (PropertyOutcome::Holds | PropertyOutcome::Violated(_)) => {
            panic!("expected inconclusive ({expected}), got {other:?}")
        }
    }
}

// --------------------------------------------------- the conforming baseline holds

/// If this fails, every positive result below is meaningless: the baseline would be
/// flagging violations that are not there.
#[test]
fn conforming_contract_satisfies_every_state_law() {
    let mut fake = Fake::conforming();
    let (a, b, c): (&[u8], &[u8], &[u8]) = (&[1, 2], &[2, 3], &[4]);

    assert_holds(verify_case(
        &mut fake,
        &case(ConformanceProperty::StateIdempotence, &[a]),
    ));
    assert_holds(verify_case(
        &mut fake,
        &case(ConformanceProperty::StateCommutativity, &[a, b]),
    ));
    assert_holds(verify_case(
        &mut fake,
        &case(ConformanceProperty::StateAssociativity, &[a, b, c]),
    ));
    assert_holds(verify_case(
        &mut fake,
        &case(ConformanceProperty::EmittedStateValidity, &[a, b]),
    ));
    assert_holds(verify_case(
        &mut fake,
        &case(ConformanceProperty::UpdateDeterminism, &[a, b]),
    ));
    assert_holds(verify_case(
        &mut fake,
        &case(ConformanceProperty::SummaryDeterminism, &[a]),
    ));
    assert_holds(verify_case(
        &mut fake,
        &case(ConformanceProperty::DeltaDeterminism, &[a]),
    ));
    assert_holds(verify_case(
        &mut fake,
        &case(ConformanceProperty::SelfDeltaEmpty, &[a]),
    ));
    assert_holds(verify_case(
        &mut fake,
        &case(ConformanceProperty::ReconciliationCycle, &[a, b]),
    ));
    assert_holds(verify_case(
        &mut fake,
        &case(ConformanceProperty::DeltaIdempotence, &[a]).with_deltas(vec![bytes(&[9])]),
    ));
    assert_holds(verify_case(
        &mut fake,
        &case(ConformanceProperty::DeltaPermutationInvariance, &[a])
            .with_deltas(vec![bytes(&[9]), bytes(&[7])]),
    ));
}

// ------------------------------------------------------- the #5153 production cases

/// Last-write-wins: the update simply replaces the state. Commutative only when the
/// two states are already equal, which is exactly why it oscillates in production.
#[test]
fn last_write_wins_merge_fails_commutativity() {
    let mut fake = Fake::conforming().merging(|_a, b| Ok(b.to_vec()));
    assert_violates(
        verify_case(
            &mut fake,
            &case(ConformanceProperty::StateCommutativity, &[&[1, 2], &[2, 3]]),
        ),
        ConformanceProperty::StateCommutativity,
    );
}

/// Mutual rejection: each side keeps its own state and discards the other's, so
/// `merge(A,B) == A` and `merge(B,A) == B`. This is the #5153 pathology that burns
/// unbounded network traffic — the peers never stop trying.
#[test]
fn mutual_rejection_fails_commutativity() {
    let mut fake = Fake::conforming().merging(|a, _b| Ok(a.to_vec()));
    assert_violates(
        verify_case(
            &mut fake,
            &case(ConformanceProperty::StateCommutativity, &[&[1, 2], &[2, 3]]),
        ),
        ConformanceProperty::StateCommutativity,
    );
}

/// The same contract also proves non-convergence directly: the two peers exchange
/// deltas forever and neither ever moves.
#[test]
fn mutual_rejection_is_a_reconciliation_cycle() {
    let mut fake = Fake::conforming().applying(|a, _d| Ok(a.to_vec()));
    assert_violates(
        verify_case(
            &mut fake,
            &case(
                ConformanceProperty::ReconciliationCycle,
                &[&[1, 2], &[3, 4]],
            ),
        ),
        ConformanceProperty::ReconciliationCycle,
    );
}

// ------------------------------------------------------------- constructed defects

#[test]
fn non_idempotent_merge_is_caught() {
    // Merging a state with itself grows it: the classic counter-appended-per-merge
    // shape. At-least-once delivery makes this diverge on redelivery alone.
    let mut fake = Fake::conforming().merging(|a, b| {
        let mut out = union(a, b);
        out.push(0xff);
        Ok(out)
    });
    assert_violates(
        verify_case(
            &mut fake,
            &case(ConformanceProperty::StateIdempotence, &[&[1, 2]]),
        ),
        ConformanceProperty::StateIdempotence,
    );
}

/// Commutative and idempotent, but not associative: integer averaging.
///
/// `avg(avg(1,3),5) = 3` while `avg(1,avg(3,5)) = 2`. Worth having as a distinct
/// case because a detector that only checks pairs would call this contract clean.
#[test]
fn associativity_only_defect_needs_the_triple_check() {
    fn avg(a: &[u8], b: &[u8]) -> Vec<u8> {
        let (x, y) = (
            a.first().copied().unwrap_or(0),
            b.first().copied().unwrap_or(0),
        );
        vec![((x as u16 + y as u16) / 2) as u8]
    }
    let build = || {
        Fake::conforming()
            .merging(|a, b| Ok(avg(a, b)))
            .validating(|state| {
                if state.len() == 1 {
                    Ok(ValidateResult::Valid)
                } else {
                    Ok(ValidateResult::Invalid)
                }
            })
    };

    // Pairwise laws hold, so a pair-only detector sees nothing.
    let mut fake = build();
    assert_holds(verify_case(
        &mut fake,
        &case(ConformanceProperty::StateCommutativity, &[&[1], &[3]]),
    ));
    assert_holds(verify_case(
        &mut fake,
        &case(ConformanceProperty::StateIdempotence, &[&[1]]),
    ));

    let mut fake = build();
    assert_violates(
        verify_case(
            &mut fake,
            &case(ConformanceProperty::StateAssociativity, &[&[1], &[3], &[5]]),
        ),
        ConformanceProperty::StateAssociativity,
    );
}

#[test]
fn non_idempotent_delta_is_caught() {
    // Append rather than union: re-delivering the same delta appends again.
    let mut fake = Fake::conforming().applying(|a, d| {
        let mut out = a.to_vec();
        out.extend_from_slice(d);
        Ok(out)
    });
    assert_violates(
        verify_case(
            &mut fake,
            &case(ConformanceProperty::DeltaIdempotence, &[&[1, 2]]).with_deltas(vec![bytes(&[9])]),
        ),
        ConformanceProperty::DeltaIdempotence,
    );
}

#[test]
fn order_dependent_deltas_are_caught() {
    let mut fake = Fake::conforming().applying(|a, d| {
        let mut out = a.to_vec();
        out.extend_from_slice(d);
        Ok(out)
    });
    assert_violates(
        verify_case(
            &mut fake,
            &case(ConformanceProperty::DeltaPermutationInvariance, &[&[1, 2]])
                .with_deltas(vec![bytes(&[9]), bytes(&[7])]),
        ),
        ConformanceProperty::DeltaPermutationInvariance,
    );
}

#[test]
fn nondeterministic_summary_is_caught() {
    // The #4857 class: a summary that depends on something other than the state,
    // such as map iteration order. Two identical peers then look divergent forever.
    let mut counter = 0u8;
    let mut fake = Fake::conforming().summarizing(move |state| {
        counter = counter.wrapping_add(1);
        let mut out = state.to_vec();
        out.push(counter);
        Ok(out)
    });
    assert_violates(
        verify_case(
            &mut fake,
            &case(ConformanceProperty::SummaryDeterminism, &[&[1, 2]]),
        ),
        ConformanceProperty::SummaryDeterminism,
    );
}

#[test]
fn nondeterministic_update_is_caught() {
    let mut counter = 0u8;
    let mut fake = Fake::conforming().merging(move |a, b| {
        counter = counter.wrapping_add(1);
        let mut out = union(a, b);
        out.push(counter);
        Ok(out)
    });
    assert_violates(
        verify_case(
            &mut fake,
            &case(ConformanceProperty::UpdateDeterminism, &[&[1, 2], &[3]]),
        ),
        ConformanceProperty::UpdateDeterminism,
    );
}

#[test]
fn emitted_state_the_contract_would_reject_is_caught() {
    // Emits a non-canonical (unsorted) state that its own validate_state rejects.
    // The next peer to receive it drops it, so the network cannot converge.
    let mut fake = Fake::conforming().merging(|a, b| {
        let mut out = union(a, b);
        out.reverse();
        out.push(out[0]);
        Ok(out)
    });
    assert_violates(
        verify_case(
            &mut fake,
            &case(
                ConformanceProperty::EmittedStateValidity,
                &[&[1, 2], &[3, 4]],
            ),
        ),
        ConformanceProperty::EmittedStateValidity,
    );
}

// ------------------------------------------------------ #5072 / #5056 diagnostics

#[test]
fn non_empty_self_delta_is_reported_but_only_as_a_diagnostic() {
    // A delta against an exact summary of the same state should be empty. Returning
    // the whole state instead is the #5072 shape: wasteful, but it still converges,
    // so it must never be grounds for removal.
    let mut fake = Fake::conforming().deltaing(|state, _summary| Ok(state.to_vec()));

    let outcome = verify_case(
        &mut fake,
        &case(ConformanceProperty::SelfDeltaEmpty, &[&[1, 2, 3]]),
    );
    assert_violates(outcome.clone(), ConformanceProperty::SelfDeltaEmpty);
    assert!(
        !outcome.is_enforceable_violation(),
        "a wasteful self-delta must not be eligible as removal evidence"
    );
    assert_eq!(outcome.violation().unwrap().severity, Severity::Diagnostic);

    let outcome = verify_case(
        &mut fake,
        &case(ConformanceProperty::WholeStateSelfDelta, &[&[1, 2, 3]]),
    );
    assert_violates(outcome.clone(), ConformanceProperty::WholeStateSelfDelta);
    assert!(!outcome.is_enforceable_violation());
}

// ---------------------------------------------- legitimate behaviour must stay clean

/// Convergence is allowed to take more than one round. A contract whose delta only
/// carries one element at a time is slow, not broken, and flagging it would be the
/// #4295 mistake again.
#[test]
fn multi_round_convergence_is_not_a_cycle() {
    let mut fake = Fake::conforming().deltaing(|state, summary| {
        let mut missing = difference(state, summary);
        missing.truncate(1);
        Ok(missing)
    });
    assert_holds(verify_case(
        &mut fake,
        &case(
            ConformanceProperty::ReconciliationCycle,
            &[&[1, 2], &[3, 4]],
        ),
    ));
}

/// A contract that rejects an update it considers unauthorized (the River-style
/// signature-chain case) returns an error. One rejection is not a merge-law failure.
#[test]
fn a_rejected_update_is_inconclusive_not_a_violation() {
    let mut fake = Fake::conforming().merging(|_a, _b| {
        Err(OracleError::contract(
            "signature does not chain to the owner",
        ))
    });
    assert_inconclusive(
        verify_case(
            &mut fake,
            &case(ConformanceProperty::StateCommutativity, &[&[1, 2], &[2, 3]]),
        ),
        Inconclusive::ContractError("signature does not chain to the owner".into()),
    );
}

/// A contract waiting on a related contract has not failed anything; it has told us
/// we are missing context. Removing it for that would delete every contract that
/// composes with another.
#[test]
fn waiting_on_a_related_contract_is_inconclusive() {
    let mut fake = Fake::conforming().validating(|_| Ok(ValidateResult::RequestRelated(vec![])));
    assert_inconclusive(
        verify_case(
            &mut fake,
            &case(ConformanceProperty::StateCommutativity, &[&[1, 2], &[2, 3]]),
        ),
        Inconclusive::RelatedRequired,
    );
}

/// Laws say nothing about states the contract never would have accepted. Feeding a
/// contract garbage and calling the result a violation is how a fuzzer manufactures
/// false positives.
#[test]
fn states_the_contract_rejects_are_never_evidence() {
    let mut fake = Fake::conforming().merging(|_a, b| Ok(b.to_vec()));
    // [3, 1] is not canonical, so the baseline validate_state rejects it.
    assert_inconclusive(
        verify_case(
            &mut fake,
            &case(ConformanceProperty::StateCommutativity, &[&[3, 1], &[2, 3]]),
        ),
        Inconclusive::InputNotValid,
    );
}

/// Running out of fuel means we never saw the answer. It must not read as a defect,
/// or a contract could be removed for being slow on a busy peer.
#[test]
fn resource_exhaustion_is_inconclusive() {
    let mut fake = Fake::conforming().merging(|_a, _b| Err(OracleError::resource("out of gas")));
    match verify_case(
        &mut fake,
        &case(ConformanceProperty::StateCommutativity, &[&[1, 2], &[2, 3]]),
    ) {
        PropertyOutcome::Inconclusive(Inconclusive::ResourceLimit(_)) => {}
        other @ (PropertyOutcome::Holds
        | PropertyOutcome::Violated(_)
        | PropertyOutcome::Inconclusive(_)) => {
            panic!("expected a resource-limit inconclusive, got {other:?}")
        }
    }
}

#[test]
fn a_case_with_too_few_states_is_malformed_not_a_violation() {
    let mut fake = Fake::conforming();
    match verify_case(
        &mut fake,
        &case(ConformanceProperty::StateAssociativity, &[&[1], &[2]]),
    ) {
        PropertyOutcome::Inconclusive(Inconclusive::MalformedCase(_)) => {}
        other @ (PropertyOutcome::Holds
        | PropertyOutcome::Violated(_)
        | PropertyOutcome::Inconclusive(_)) => {
            panic!("expected malformed-case, got {other:?}")
        }
    }
}

// ------------------------------------------------------------------------ evidence

fn instance(seed: u8) -> ContractInstanceId {
    ContractInstanceId::new([seed; 32])
}

#[test]
fn evidence_round_trips_through_a_case() {
    let original = case(ConformanceProperty::StateCommutativity, &[&[1, 2], &[2, 3]]);
    let evidence = ConformanceEvidence::new(instance(7), vec![9, 9], &original, None);
    evidence.check_bounds().expect("bounds");

    let rebuilt = evidence.to_case();
    assert_eq!(rebuilt.property, original.property);
    assert_eq!(rebuilt.states, original.states);

    // And the rebuilt case reproduces the same verdict, which is the whole premise
    // of shipping evidence rather than a verdict.
    let mut fake = Fake::conforming().merging(|_a, b| Ok(b.to_vec()));
    assert_violates(
        verify_case(&mut fake, &rebuilt),
        ConformanceProperty::StateCommutativity,
    );
}

#[test]
fn evidence_id_ignores_observed_output_and_runtime() {
    let case = case(ConformanceProperty::StateCommutativity, &[&[1, 2], &[2, 3]]);
    let bare = ConformanceEvidence::new(instance(1), vec![], &case, None);

    let mut fake = Fake::conforming().merging(|_a, b| Ok(b.to_vec()));
    let observed = verify_case(&mut fake, &case).violation().cloned();
    let annotated = ConformanceEvidence::new(instance(1), vec![], &case, observed);

    // Two peers that discover the same defect must produce the same id, or
    // deduplication fails open and the same case circulates once per discoverer.
    assert_eq!(bare.id(), annotated.id());
}

#[test]
fn evidence_id_separates_instances_and_parameters() {
    let case = case(ConformanceProperty::StateCommutativity, &[&[1, 2], &[2, 3]]);
    let a = ConformanceEvidence::new(instance(1), vec![], &case, None);
    let b = ConformanceEvidence::new(instance(2), vec![], &case, None);
    let c = ConformanceEvidence::new(instance(1), vec![1], &case, None);
    assert_ne!(a.id(), b.id());
    assert_ne!(
        a.id(),
        c.id(),
        "same code with different parameters is a different instance"
    );
}

/// Length-prefixing matters: without it, `["ab"]` and `["a", "b"]` would hash the
/// same and two different reproducers would collide into one id.
#[test]
fn evidence_id_is_not_confused_by_blob_boundaries() {
    let split = case(ConformanceProperty::StateCommutativity, &[&[1], &[2]]);
    let joined = ConformanceCase::new(
        ConformanceProperty::StateCommutativity,
        vec![bytes(&[1, 2]), bytes(&[])],
    );
    let a = ConformanceEvidence::new(instance(1), vec![], &split, None);
    let b = ConformanceEvidence::new(instance(1), vec![], &joined, None);
    assert_ne!(a.id(), b.id());
}

#[test]
fn oversized_evidence_is_rejected_before_any_execution() {
    let big = vec![0u8; MAX_EVIDENCE_INPUT_BYTES + 1];
    let case = ConformanceCase::new(
        ConformanceProperty::StateIdempotence,
        vec![Arc::from(big.as_slice())],
    );
    let evidence = ConformanceEvidence::new(instance(1), vec![], &case, None);
    assert!(matches!(
        evidence.check_bounds(),
        Err(EvidenceRejected::TooLarge { .. })
    ));
}

#[test]
fn evidence_with_wrong_arity_is_rejected() {
    let case = ConformanceCase::new(
        ConformanceProperty::StateAssociativity,
        vec![bytes(&[1]), bytes(&[2])],
    );
    let evidence = ConformanceEvidence::new(instance(1), vec![], &case, None);
    assert!(matches!(
        evidence.check_bounds(),
        Err(EvidenceRejected::Arity { .. })
    ));
}

/// The fourth `check_bounds` branch. Every rejection branch needs its own test:
/// this is the untrusted front door, and a limit with no test is a limit that can be
/// silently removed. Related state is the branch an attacker would aim at, because
/// each entry is a full contract state and the cost is paid before any check runs.
#[test]
fn evidence_with_too_many_related_contracts_is_rejected() {
    let case = case(ConformanceProperty::StateIdempotence, &[&[1]]);
    let mut evidence = ConformanceEvidence::new(instance(1), vec![], &case, None);
    evidence.related = (0..=MAX_EVIDENCE_RELATED)
        .map(|i| (instance(i as u8), vec![i as u8]))
        .collect();
    assert!(
        evidence.related.len() > MAX_EVIDENCE_RELATED,
        "the fixture must actually exceed the limit or the assertion below is vacuous"
    );
    assert!(matches!(
        evidence.check_bounds(),
        Err(EvidenceRejected::TooManyRelated { .. })
    ));
}

/// The counterpart: exactly at the limit is allowed. Without this, an off-by-one
/// that rejected every piece of evidence would still pass the test above.
#[test]
fn evidence_at_the_related_contract_limit_is_accepted() {
    let case = case(ConformanceProperty::StateIdempotence, &[&[1]]);
    let mut evidence = ConformanceEvidence::new(instance(1), vec![], &case, None);
    evidence.related = (0..MAX_EVIDENCE_RELATED)
        .map(|i| (instance(i as u8), vec![i as u8]))
        .collect();
    assert!(evidence.check_bounds().is_ok());
}

#[test]
fn unsupported_schema_is_rejected() {
    let case = case(ConformanceProperty::StateIdempotence, &[&[1]]);
    let mut evidence = ConformanceEvidence::new(instance(1), vec![], &case, None);
    evidence.schema_version = 999;
    assert!(matches!(
        evidence.check_bounds(),
        Err(EvidenceRejected::UnsupportedSchema { .. })
    ));
}

// ----------------------------------------------------------------------- generator

#[test]
fn generator_is_deterministic() {
    let corpus = Corpus::from_states(vec![vec![1], vec![2], vec![3], vec![4]]);
    let config = GeneratorConfig::default();
    let first = generate_cases(&corpus, &config);
    let second = generate_cases(&corpus, &config);
    assert_eq!(first.len(), second.len());
    for (a, b) in first.iter().zip(second.iter()) {
        assert_eq!(a.property, b.property);
        assert_eq!(a.states, b.states);
    }
}

/// A case budget must narrow depth, not silently drop whole laws. If the generator
/// emitted every commutativity case before its first associativity case, a corpus
/// large enough to hit the budget would quietly stop checking associativity —
/// and nothing would say so.
#[test]
fn a_tight_case_budget_still_covers_every_law() {
    let states: Vec<Vec<u8>> = (1u8..=12).map(|i| vec![i]).collect();
    let corpus = Corpus {
        deltas: vec![Arc::from([9u8].as_slice()), Arc::from([7u8].as_slice())],
        ..Corpus::from_states(states)
    };
    let config = GeneratorConfig {
        max_cases: ConformanceProperty::ALL.len(),
        ..Default::default()
    };
    let cases = generate_cases(&corpus, &config);

    let mut seen: Vec<ConformanceProperty> = cases.iter().map(|c| c.property).collect();
    seen.sort_by_key(|p| p.as_str());
    seen.dedup();
    assert_eq!(
        seen.len(),
        ConformanceProperty::ALL.len(),
        "budget dropped whole properties instead of narrowing each one"
    );
}

/// Regression: captured summaries were never used. Every `DeltaDeterminism` case was
/// state-only, so the verifier fell back to `summarize(state)` and only ever
/// exercised the "peer is exactly up to date" case. The defects that matter live in
/// `get_state_delta(state, a summary from a peer at a different point)`, which is the
/// call the network actually makes, and no generated case reached it.
#[test]
fn generated_delta_cases_use_observed_summaries() {
    let corpus = Corpus {
        summaries: vec![bytes(&[1]), bytes(&[2, 3])],
        ..Corpus::from_states(vec![vec![1, 2], vec![2, 3]])
    };
    let cases = generate_cases(&corpus, &GeneratorConfig::default());

    let with_summary: Vec<_> = cases
        .iter()
        .filter(|c| c.property == ConformanceProperty::DeltaDeterminism && c.summary.is_some())
        .collect();
    assert!(
        !with_summary.is_empty(),
        "no generated case exercised get_state_delta against an observed summary"
    );
    // Both observed summaries should be reachable, not just the first.
    let used: std::collections::HashSet<Vec<u8>> = with_summary
        .iter()
        .map(|c| c.summary.as_ref().unwrap().to_vec())
        .collect();
    assert_eq!(
        used.len(),
        2,
        "only some observed summaries were used: {used:?}"
    );
}

#[test]
fn generator_deduplicates_oscillating_states() {
    // A contract flapping between two states offers the same bytes forever. Without
    // dedup, a thousand observations are a corpus of two pretending to be a thousand.
    let corpus =
        Corpus::from_states(vec![vec![1], vec![2], vec![1], vec![2], vec![1]]).deduplicated();
    assert_eq!(corpus.states.len(), 2);
}

#[test]
fn generated_cases_find_the_planted_defect() {
    // End to end: a corpus plus the generator plus the verifier, against a contract
    // with one planted bug, with no test telling it where to look.
    let corpus = Corpus::from_states(vec![vec![1, 2], vec![2, 3], vec![4]]);
    let cases = generate_cases(&corpus, &GeneratorConfig::default());
    let mut fake = Fake::conforming().merging(|_a, b| Ok(b.to_vec()));

    let found = cases
        .iter()
        .filter_map(|c| verify_case(&mut fake, c).violation().cloned())
        .any(|v| v.property == ConformanceProperty::StateCommutativity);
    assert!(found, "generated corpus missed a last-write-wins merge");
}

/// The counterpart: the same pipeline over a conforming contract reports nothing.
#[test]
fn generated_cases_report_nothing_against_a_conforming_contract() {
    let corpus = Corpus::from_states(vec![vec![1, 2], vec![2, 3], vec![4], vec![1, 4]]);
    let cases = generate_cases(&corpus, &GeneratorConfig::default());
    // Without this the test would pass by checking nothing at all, which is the
    // failure mode a "no false positives" assertion is most prone to.
    assert!(
        cases.len() > 10,
        "generator produced almost nothing to check"
    );
    let mut fake = Fake::conforming();
    for case in &cases {
        let outcome = verify_case(&mut fake, case);
        assert!(
            !outcome.is_violation(),
            "false positive on a conforming contract: {outcome:?} for {}",
            case.property
        );
    }
}

// -------------------------------------------------------------------------- bundle

#[test]
fn bundle_round_trips() {
    use super::bundle::{ReplayBundle, Transition};
    let mut bundle = ReplayBundle::new(vec![0, 1, 2, 3], vec![7]);
    bundle.states = vec![vec![1, 2], vec![2, 3]];
    bundle.deltas = vec![vec![9]];
    bundle.transitions = vec![Transition {
        base_state: vec![1, 2],
        delta: Some(vec![3]),
        incoming_state: None,
        summary: Some(vec![1, 2]),
        result_state: vec![1, 2, 3],
    }];

    let encoded = bundle.encode().expect("encode");
    let decoded = ReplayBundle::decode(&encoded).expect("decode");
    assert_eq!(bundle, decoded);

    // Transition endpoints become corpus states: a state a peer actually reached is
    // by construction a state the contract produced.
    let corpus = decoded.to_corpus();
    assert!(corpus.states.iter().any(|s| s.as_ref() == [1, 2, 3]));
    assert!(corpus.deltas.iter().any(|d| d.as_ref() == [3]));
}

/// A bundle must reproduce the exact same corpus it was written from. This is the
/// property the whole evidence model rests on: if replay produced a different set of
/// states, a finding reported by one peer would not be reproducible by another, and
/// "verify it yourself" would be meaningless.
#[test]
fn a_bundle_round_trips_to_an_identical_corpus() {
    use super::bundle::ReplayBundle;
    let mut bundle = ReplayBundle::new(vec![0, 1, 2, 3], vec![7]);
    bundle.states = vec![vec![1, 2], vec![2, 3], vec![4]];
    bundle.deltas = vec![vec![9]];
    bundle.summaries = vec![vec![1]];

    let decoded = ReplayBundle::decode(&bundle.encode().expect("encode")).expect("decode");
    let (before, after) = (bundle.to_corpus(), decoded.to_corpus());
    assert_eq!(before.states, after.states);
    assert_eq!(before.deltas, after.deltas);
    assert_eq!(before.summaries, after.summaries);

    // And the same corpus must generate the same cases, or two peers replaying it
    // would check different things.
    let config = GeneratorConfig::default();
    let (a, b) = (
        generate_cases(&before, &config),
        generate_cases(&after, &config),
    );
    assert_eq!(a.len(), b.len());
    for (x, y) in a.iter().zip(b.iter()) {
        assert_eq!(x.property, y.property);
        assert_eq!(x.states, y.states);
        assert_eq!(x.summary, y.summary);
    }
}

#[test]
fn a_foreign_file_is_not_mistaken_for_a_bundle() {
    use super::bundle::{BundleError, ReplayBundle};
    assert!(matches!(
        ReplayBundle::decode(b"definitely not a bundle"),
        Err(BundleError::BadMagic)
    ));
    // A file shorter than the header must not index past the end either.
    assert!(matches!(
        ReplayBundle::decode(b"FRNT"),
        Err(BundleError::BadMagic)
    ));
    assert!(matches!(
        ReplayBundle::decode(b""),
        Err(BundleError::BadMagic)
    ));
}

/// A corpus archived by an older build must be refused with a clear reason rather
/// than deserialized under the wrong field meanings. Silently misreading an archived
/// corpus would produce findings about a contract from data that never meant what
/// the reader thinks it means.
#[test]
fn a_bundle_from_an_unsupported_schema_is_refused() {
    use super::bundle::{BUNDLE_SCHEMA_VERSION, BundleError, ReplayBundle};
    let mut encoded = ReplayBundle::new(vec![0, 1], vec![])
        .encode()
        .expect("encode");
    // Bump the version in the header, leaving the magic intact.
    let bumped = BUNDLE_SCHEMA_VERSION + 1;
    encoded[8..10].copy_from_slice(&bumped.to_le_bytes());
    match ReplayBundle::decode(&encoded) {
        Err(BundleError::UnsupportedSchema { found, supported }) => {
            assert_eq!(found, bumped);
            assert_eq!(supported, BUNDLE_SCHEMA_VERSION);
        }
        other => panic!("expected an unsupported-schema refusal, got {other:?}"),
    }
}

/// Regression: a bundle exported without embedded code used to record an all-zero
/// `code_hash`, so it named no contract at all and could be replayed against any
/// WASM. A run against the wrong contract is worse than no run: findings and clean
/// results alike look authoritative and mean nothing.
#[test]
fn a_bundle_that_names_no_contract_is_refused() {
    use super::bundle::{BundleError, ReplayBundle};
    let mut bundle = ReplayBundle::new(vec![1, 2, 3], vec![]);
    bundle.code = None;
    bundle.code_hash = None;
    assert!(matches!(
        bundle.resolve_code(Some(vec![9, 9, 9])),
        Err(BundleError::UnidentifiedContract)
    ));
}

#[test]
fn supplied_code_must_match_the_bundle_it_replays() {
    use super::bundle::{BundleError, ReplayBundle};
    let mut bundle = ReplayBundle::new(vec![1, 2, 3], vec![]);
    bundle.code = None; // hash retained: the bundle still names its contract

    assert!(matches!(
        bundle.resolve_code(Some(vec![4, 5, 6])),
        Err(BundleError::CodeMismatch { .. })
    ));
    // The right code is accepted.
    assert_eq!(
        bundle.resolve_code(Some(vec![1, 2, 3])).unwrap(),
        vec![1, 2, 3]
    );
    // And with no code anywhere, the error says so rather than silently proceeding.
    assert!(matches!(
        bundle.resolve_code(None),
        Err(BundleError::MissingCode)
    ));
}

#[test]
fn embedded_bundle_code_is_verified_against_its_own_hash() {
    use super::bundle::{BundleError, ReplayBundle};
    let mut bundle = ReplayBundle::new(vec![1, 2, 3], vec![]);
    // Corrupt the embedded code, leaving the hash intact.
    bundle.code = Some(vec![1, 2, 4]);
    assert!(matches!(
        bundle.resolve_code(None),
        Err(BundleError::CodeMismatch { .. })
    ));
}

/// Right magic and right version, but a corrupt body. This must be a clean error,
/// not a panic: bundles come from disk and from other machines.
#[test]
fn a_corrupt_bundle_body_is_an_error_not_a_panic() {
    use super::bundle::{BundleError, ReplayBundle};
    let mut encoded = ReplayBundle::new(vec![0, 1], vec![])
        .encode()
        .expect("encode");
    encoded.truncate(encoded.len() - 1);
    assert!(matches!(
        ReplayBundle::decode(&encoded),
        Err(BundleError::Decode(_))
    ));
}
