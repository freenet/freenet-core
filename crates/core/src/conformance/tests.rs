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
use super::property::{
    ConformanceProperty, Inconclusive, PremiseSource, PropertyOutcome, Severity,
};
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
        &case(ConformanceProperty::PathAgreement, &[a, b]),
    ));
    // A transition a conforming contract really could have taken: `a` merged with
    // `b` is a state a peer at `a` reaches, and merging it back must be a no-op.
    assert_holds(verify_case(
        &mut fake,
        &case(
            ConformanceProperty::TransitionPathAgreement,
            &[a, &[1, 2, 3]],
        ),
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

/// The same contract also proves non-convergence directly: the two peers exchange,
/// and neither ever moves.
///
/// Both the merge and the delta apply must refuse, because the simulation models
/// the protocol's full-state fallback. A contract whose delta path is weak but whose
/// *merge* is sound converges the moment the states are exchanged whole, and is
/// correctly NOT a cycle — that distinction is the whole point of modelling the
/// fallback, and it is what stops a coarse summary from being called a defect.
#[test]
fn mutual_rejection_is_a_reconciliation_cycle() {
    let mut fake = Fake::conforming()
        .merging(|a, _b| Ok(a.to_vec()))
        .applying(|a, _d| Ok(a.to_vec()));
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
    // Grows on EVERY re-apply and never settles, which is what genuine
    // non-idempotence looks like: under at-least-once delivery, redelivering the
    // same state keeps mutating it.
    //
    // Appending a constant marker would not do — that stabilizes after one step and
    // is indistinguishable from a contract canonicalizing a raw stored state, which
    // is legitimate and must not be flagged. The length-derived byte keeps changing.
    let mut fake = Fake::conforming().merging(|a, b| {
        // Concatenate rather than union: a set union would dedup the growth marker
        // away and the state would settle after one step, which is the legitimate
        // canonicalization shape, not this one.
        let mut out = a.to_vec();
        out.extend_from_slice(b);
        out.push(out.len() as u8);
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
fn a_merge_that_only_reorders_bytes_is_named_as_an_encoding_problem() {
    // The #4295 false-positive class, and the reason the executor's own in-tree
    // probe compares byte MULTISETS rather than exact bytes: a contract whose
    // encoding is not canonical (a HashMap serialized in iteration order) emits the
    // same logical state in a different byte order depending on merge order.
    //
    // It is still reported — peers compare state by hash, so two holding the same
    // content in different order never see each other as converged — but the fix is
    // a deterministic encoding, not a change to the merge, and the finding has to
    // say which or the author looks in the wrong file.
    //
    // This test is also what makes the measurement trustworthy: without it, "none of
    // the live findings were reorderings" could equally mean the check never fires.
    let mut fake = Fake::conforming().merging(|current, incoming| {
        // Set union emitted in an order that depends on which side was the base:
        // same content, different serialization.
        let mut out: Vec<u8> = current.to_vec();
        for b in incoming {
            if !out.contains(b) {
                out.push(*b);
            }
        }
        Ok(out)
    });

    let outcome = verify_case(
        &mut fake,
        &case(ConformanceProperty::StateCommutativity, &[&[1, 2], &[3, 4]]),
    );

    let violation = match outcome {
        PropertyOutcome::Violated(v) => v,
        other @ (PropertyOutcome::Holds | PropertyOutcome::Inconclusive(_)) => {
            panic!("expected a commutativity violation, got {other:?}")
        }
    };
    assert!(
        violation.detail.contains("same bytes in a different order"),
        "a reordering must be named as an encoding problem; got: {}",
        violation.detail
    );
    assert!(
        violation.detail.contains("canonical"),
        "the finding should point at the encoding; got: {}",
        violation.detail
    );
}

#[test]
fn nondeterministic_delta_is_caught() {
    // Sibling of `nondeterministic_summary_is_caught`, and the one property whose
    // violation branch nothing else reaches: every other test that mentions
    // `DeltaDeterminism` feeds it a conforming contract and asserts it holds, which
    // would pass just as happily with the check deleted.
    //
    // A delta that varies across identical calls is worse than a varying summary,
    // because the recipient applies it: two peers asking the same holder for the
    // same difference get different answers and diverge without either side seeing
    // an error.
    let mut counter = 0u8;
    let mut fake = Fake::conforming().deltaing(move |state, _summary| {
        counter = counter.wrapping_add(1);
        let mut out = state.to_vec();
        out.push(counter);
        Ok(out)
    });
    assert_violates(
        verify_case(
            &mut fake,
            &case(ConformanceProperty::DeltaDeterminism, &[&[1, 2]]),
        ),
        ConformanceProperty::DeltaDeterminism,
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

/// A canonicalizing contract rewrites a non-canonical stored state once and then
/// settles. That first change is real and is NOT a defect.
///
/// This is the shape the repo already documents on
/// `executor_impl::probe_identical_input_idempotency`: the PUT install path stores
/// the client's raw bytes without running `update_state`, so a peer can be holding a
/// state its own contract has never normalized. A single-apply idempotence check
/// flags every such contract, which is why that probe iterates to a fixpoint and why
/// this one does too.
#[test]
fn a_canonicalizing_contract_is_not_flagged() {
    // Sorts and dedups whatever it is given, then is stable forever after.
    let mut fake = Fake::conforming()
        .merging(|a, b| Ok(union(a, b)))
        .validating(|_| Ok(ValidateResult::Valid));

    // A raw, non-canonical stored state: unsorted with a duplicate.
    let raw: &[u8] = &[3, 1, 3, 2];
    assert_holds(verify_case(
        &mut fake,
        &case(ConformanceProperty::StateIdempotence, &[raw]),
    ));
}

/// A contract whose delta path cannot express a particular divergence, but whose
/// merge is sound, converges in production and must not be called a cycle.
///
/// A coarse version clock under concurrent writes, or a compact/probabilistic
/// digest, legitimately yields an empty delta in both directions. A delta-only
/// simulation sees the pair repeat unchanged and calls that a proven cycle on the
/// very first round. The real protocol escalates to a full-state send instead, which
/// is why the simulation models that fallback.
#[test]
fn a_weak_delta_path_with_a_sound_merge_is_not_a_cycle() {
    let mut fake = Fake::conforming()
        // Summaries collide, so neither side ever believes it has anything to send.
        .summarizing(|_| Ok(vec![0]))
        .deltaing(|_state, _summary| Ok(Vec::new()));

    assert_holds(verify_case(
        &mut fake,
        &case(
            ConformanceProperty::ReconciliationCycle,
            &[&[1, 2], &[3, 4]],
        ),
    ));
}

/// A failure that does not happen again is not evidence.
///
/// Something outside the inputs moved between the two runs — the host clock being
/// the realistic candidate — and reporting it as a merge-law break would name the
/// wrong law about a contract that may be fine. Without this test the entire re-run
/// block in `verify_case` could be deleted and nothing would fail.
#[test]
fn a_violation_that_does_not_reproduce_is_not_reported() {
    // Misbehaves only on its first two merges, then is a well-behaved union
    // forever. The first run sees last-write-wins; every run after holds.
    let mut calls = 0u32;
    let mut fake = Fake::conforming().merging(move |a, b| {
        calls += 1;
        if calls <= 2 {
            Ok(b.to_vec()) // last-write-wins: A,B and B,A disagree
        } else {
            Ok(union(a, b))
        }
    });

    assert_inconclusive(
        verify_case(
            &mut fake,
            &case(ConformanceProperty::StateCommutativity, &[&[1, 2], &[2, 3]]),
        ),
        Inconclusive::NotReproducible,
    );
}

/// A contract whose merge output varies between identical calls is a real defect,
/// but it is a determinism defect. Rather than silently dropping it as
/// "not reproducible", the finding is re-issued under the property that names it.
#[test]
fn a_nondeterministic_merge_is_reported_as_nondeterminism_not_as_a_merge_law_break() {
    let mut calls = 0u8;
    let mut fake = Fake::conforming().merging(move |a, b| {
        calls = calls.wrapping_add(1);
        let mut out = union(a, b);
        out.push(calls);
        Ok(out)
    });

    assert_violates(
        verify_case(
            &mut fake,
            &case(ConformanceProperty::StateCommutativity, &[&[1, 2], &[2, 3]]),
        ),
        ConformanceProperty::UpdateDeterminism,
    );
}

/// Intermediate states get the same validity precondition as the inputs. A state the
/// contract rejects never reaches another peer, so continuing to merge on top of one
/// reasons about a history that cannot happen — which is how false positives are
/// manufactured, by this module's own rule.
///
/// Without this test, the `require_valid` calls on the associativity intermediates
/// could be deleted and nothing would fail.
#[test]
fn an_intermediate_the_contract_rejects_is_inconclusive() {
    // Accepts the three inputs, then rejects everything the merge produces.
    let mut validations = 0u32;
    let mut fake = Fake::conforming().validating(move |_state| {
        validations += 1;
        if validations <= 3 {
            Ok(ValidateResult::Valid)
        } else {
            Ok(ValidateResult::Invalid)
        }
    });

    assert_inconclusive(
        verify_case(
            &mut fake,
            &case(
                ConformanceProperty::StateAssociativity,
                &[&[1, 2], &[2, 3], &[4]],
            ),
        ),
        Inconclusive::InputNotValid,
    );
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

/// A host or WASM failure (a trap, a missing export, a store error) is not the
/// contract rejecting anything — it is the runtime executing it that broke. #5509:
/// this used to collapse into `ContractError`, accusing the contract for a defect
/// that may well be ours.
#[test]
fn a_runtime_failure_is_inconclusive_not_a_contract_error() {
    let mut fake = Fake::conforming().merging(|_a, _b| {
        Err(OracleError::runtime(
            "missing contract export: update_state",
        ))
    });
    assert_inconclusive(
        verify_case(
            &mut fake,
            &case(ConformanceProperty::StateCommutativity, &[&[1, 2], &[2, 3]]),
        ),
        Inconclusive::RuntimeError("missing contract export: update_state".into()),
    );
}

/// Neither culprit is removal-eligible, whatever the label says: `Inconclusive`
/// never reaches `PropertyOutcome::Violated`, so the split label above cannot make a
/// runtime bug more actionable against the contract than it already wasn't.
#[test]
fn a_runtime_failure_is_never_enforceable() {
    let outcome = PropertyOutcome::Inconclusive(Inconclusive::RuntimeError("trap".into()));
    assert!(!outcome.is_enforceable_violation());
}

/// `Inconclusive` derives `Serialize`/`Deserialize`, and bincode's default config
/// encodes an enum's variant as a little-endian `u32` index ahead of its payload —
/// so inserting a variant anywhere but the end silently renumbers every variant
/// declared after it. Nothing persists or ships a bare `Inconclusive` today (it
/// travels only inside an in-process `PropertyOutcome`, never serialized to disk or
/// wire), so this is not yet an observable break — but the type is `pub`,
/// non-exhaustive, and explicitly Serialize/Deserialize, which is exactly the shape
/// that acquires a wire consumer without every future editor noticing. Pinned the
/// same way as `InterestMessage` (`message.rs`,
/// `interest_message_wire_variant_indices_are_frozen`): freeze the index of every
/// variant that exists today, so a future insertion in the middle is caught here.
#[test]
fn inconclusive_wire_variant_indices_are_frozen() {
    fn variant_index(v: &Inconclusive) -> u32 {
        let bytes = bincode::serialize(v).expect("serialize Inconclusive");
        u32::from_le_bytes(bytes[..4].try_into().expect("variant index prefix"))
    }

    assert_eq!(variant_index(&Inconclusive::InputNotValid), 0);
    assert_eq!(variant_index(&Inconclusive::RelatedRequired), 1);
    assert_eq!(
        variant_index(&Inconclusive::ContractError(String::new())),
        2
    );
    assert_eq!(variant_index(&Inconclusive::NoOutputState), 3);
    assert_eq!(
        variant_index(&Inconclusive::ResourceLimit(String::new())),
        4
    );
    assert_eq!(variant_index(&Inconclusive::RoundLimit), 5);
    assert_eq!(
        variant_index(&Inconclusive::MalformedCase(String::new())),
        6
    );
    assert_eq!(variant_index(&Inconclusive::NoDeltaPath), 7);
    assert_eq!(variant_index(&Inconclusive::StateNotSettled), 8);
    assert_eq!(variant_index(&Inconclusive::NotReproducible), 9);
    // Appended (#5509): must stay LAST. A future variant goes after this one, not
    // before it.
    assert_eq!(
        variant_index(&Inconclusive::RuntimeError(String::new())),
        10,
        "RuntimeError must stay the last variant — insert new variants after it, \
         never before"
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

/// `EmittedStateValidity` is the one property that turns a validate-Invalid into a
/// violation, so it is the one that has to be provably gated on the inputs.
///
/// This is load-bearing for a claim made in `capture.rs`: that latest-wins related
/// state cannot produce a false accusation, because every input is validated against
/// the SAME related state the property then uses, so the only route to a violation is
/// a contract emitting a state it rejects after accepting both inputs. That argument
/// is only as good as the pre-validation loop in `verify_case`, and nothing pinned it
/// for this property.
///
/// The fake is arranged so that deleting the gate does not merely change the verdict,
/// it produces the false accusation itself: the input `[3, 1]` is non-canonical and
/// the merge emits it back, so an ungated run reports `Violated` against a contract
/// that was never asked a fair question.
#[test]
fn emitted_state_validity_is_gated_on_the_inputs_being_valid() {
    let mut fake = Fake::conforming().merging(|_a, _b| Ok(vec![3, 1]));

    assert_inconclusive(
        verify_case(
            &mut fake,
            &case(
                ConformanceProperty::EmittedStateValidity,
                &[&[3, 1], &[2, 3]],
            ),
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

/// A delta that is order-invariant but NOT re-delivery-safe: summing into a running
/// total. This is the CmRDT counter shape the whole delta-idempotence disagreement
/// is about, and no other fixture produces it — every other delta fake is either
/// both order-invariant and idempotent (union) or neither (append).
///
/// Two things are asserted: the idempotence check fires, and it is a Diagnostic, so
/// a counter-style contract cannot be removed for it while the question is open.
#[test]
fn a_commutative_but_non_idempotent_delta_is_a_diagnostic_not_a_violation() {
    let sum = |a: &[u8], d: &[u8]| -> Vec<u8> {
        let total: u32 =
            a.iter().map(|b| *b as u32).sum::<u32>() + d.iter().map(|b| *b as u32).sum::<u32>();
        vec![(total % 251) as u8]
    };
    let mut fake = Fake::conforming()
        .applying(move |a, d| Ok(sum(a, d)))
        .validating(|state| {
            if state.len() == 1 {
                Ok(ValidateResult::Valid)
            } else {
                Ok(ValidateResult::Invalid)
            }
        });

    let outcome = verify_case(
        &mut fake,
        &case(ConformanceProperty::DeltaIdempotence, &[&[10]]).with_deltas(vec![bytes(&[5])]),
    );
    assert_violates(outcome.clone(), ConformanceProperty::DeltaIdempotence);
    assert_eq!(outcome.violation().unwrap().severity, Severity::Diagnostic);
    assert!(
        !outcome.is_enforceable_violation(),
        "a counter-style delta must not be removal-eligible while the question of \
         whether any deployed contract relies on it is unanswered"
    );

    // And the ordering property, which IS enforceable, must stay clean for it —
    // summing is order-invariant. If this fired, downgrading DeltaIdempotence would
    // have bought nothing, because the same contract would still be accused.
    let mut fake = Fake::conforming()
        .applying(move |a, d| Ok(sum(a, d)))
        .validating(|state| {
            if state.len() == 1 {
                Ok(ValidateResult::Valid)
            } else {
                Ok(ValidateResult::Invalid)
            }
        });
    assert_holds(verify_case(
        &mut fake,
        &case(ConformanceProperty::DeltaPermutationInvariance, &[&[10]])
            .with_deltas(vec![bytes(&[5]), bytes(&[7])]),
    ));
}

/// `EmittedStateValidity` has its own `RequestRelated` arm, distinct from the shared
/// input-validation helper. It too could have been deleted with nothing failing.
#[test]
fn emitted_state_needing_related_context_is_inconclusive() {
    // Valid on the way in, but asks for related context when handed the merge result.
    let mut seen = 0u32;
    let mut fake = Fake::conforming().validating(move |_state| {
        seen += 1;
        // The two input states validate; the emitted state is the third call.
        if seen <= 2 {
            Ok(ValidateResult::Valid)
        } else {
            Ok(ValidateResult::RequestRelated(vec![]))
        }
    });
    assert_inconclusive(
        verify_case(
            &mut fake,
            &case(
                ConformanceProperty::EmittedStateValidity,
                &[&[1, 2], &[2, 3]],
            ),
        ),
        Inconclusive::RelatedRequired,
    );
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

// ------------------------------------------------------ #5394: disagreeing write paths

/// A contract whose state is a map keyed by the high nibble of each byte, with a
/// deliberate disagreement between its two write paths.
///
/// The merge path resolves a key collision by keeping the FIRST entry in ascending
/// order (`entry(k).or_insert(v)`); the delta path keeps the LAST (`insert(k, v)`).
/// Each rule on its own is a sound semilattice — min-per-key and max-per-key are
/// both commutative, associative and idempotent — so every property that compares
/// merge-to-merge or delta-to-delta holds. Only putting one path beside the other
/// reveals the defect. This is the #5394 shape: `insert` on the direct-apply path,
/// `entry().or_insert()` on the merge path, on a map keyed by a client-chosen
/// sequence number.
fn disagreeing_paths() -> Fake {
    Fake::conforming()
        .validating(|state| {
            if is_canonical(state) && state.windows(2).all(|w| w[0] >> 4 != w[1] >> 4) {
                Ok(ValidateResult::Valid)
            } else {
                Ok(ValidateResult::Invalid)
            }
        })
        .merging(|a, b| Ok(collapse_by_key(&union(a, b), false)))
        .applying(|a, d| Ok(collapse_by_key(&union(a, d), true)))
}

fn collapse_by_key(entries: &[u8], keep_last: bool) -> Vec<u8> {
    let mut out: Vec<u8> = Vec::new();
    for entry in union(entries, &[]) {
        match out.last().copied() {
            Some(previous) if previous >> 4 == entry >> 4 => {
                if keep_last {
                    let last = out.len() - 1;
                    out[last] = entry;
                }
            }
            _ => out.push(entry),
        }
    }
    out
}

/// The positive case: two states whose keys collide, so the two paths resolve the
/// collision differently and the contract cannot converge.
///
/// `0x51` and `0x52` are two writes to key 5 carrying different values — the two
/// retractions stamped with the same sequence number in the real defect.
#[test]
fn a_contract_whose_two_write_paths_disagree_is_caught() {
    let mut fake = disagreeing_paths();
    assert_violates(
        verify_case(
            &mut fake,
            &case(
                ConformanceProperty::PathAgreement,
                &[&[0x10, 0x51], &[0x10, 0x52]],
            ),
        ),
        ConformanceProperty::PathAgreement,
    );
}

/// The matched negative the acceptance test in #5394 asks for, and the half that
/// makes the positive above mean anything.
///
/// The SAME contract, with the SAME two write paths, on states whose keys do not
/// collide. A property that flagged every contract with both a delta and a merge
/// path would satisfy the test above while being worse than no property at all, and
/// this is what distinguishes the two.
#[test]
fn the_same_disagreeing_contract_is_silent_when_no_key_collides() {
    let mut fake = disagreeing_paths();
    assert_holds(verify_case(
        &mut fake,
        &case(
            ConformanceProperty::PathAgreement,
            &[&[0x10, 0x51], &[0x10, 0x62]],
        ),
    ));
}

/// Every OTHER law holds for the disagreeing contract, which is the whole claim
/// #5394 makes: a contract can satisfy the entire existing property set and still
/// diverge, because none of those properties ever puts one write path beside the
/// other.
///
/// Without this the new property could be riding on a defect the existing set
/// already catches, and the gap it is supposed to close would be unproven.
#[test]
fn the_disagreeing_contract_satisfies_every_pre_existing_law() {
    let (a, b, c): (&[u8], &[u8], &[u8]) = (&[0x10, 0x51], &[0x10, 0x52], &[0x23, 0x51]);
    for (property, states, deltas) in [
        (ConformanceProperty::StateIdempotence, vec![a], vec![]),
        (ConformanceProperty::StateCommutativity, vec![a, b], vec![]),
        (
            ConformanceProperty::StateAssociativity,
            vec![a, b, c],
            vec![],
        ),
        (
            ConformanceProperty::EmittedStateValidity,
            vec![a, b],
            vec![],
        ),
        (ConformanceProperty::UpdateDeterminism, vec![a, b], vec![]),
        (ConformanceProperty::SummaryDeterminism, vec![a], vec![]),
        (ConformanceProperty::DeltaDeterminism, vec![a], vec![]),
        (ConformanceProperty::ReconciliationCycle, vec![a, b], vec![]),
        (ConformanceProperty::SelfDeltaEmpty, vec![a], vec![]),
        (
            ConformanceProperty::DeltaIdempotence,
            vec![a],
            vec![bytes(&[0x52])],
        ),
        (
            ConformanceProperty::DeltaPermutationInvariance,
            vec![a],
            vec![bytes(&[0x52]), bytes(&[0x63])],
        ),
    ] {
        let mut fake = disagreeing_paths();
        let built = ConformanceCase::new(property, states.iter().map(|s| bytes(s)).collect())
            .with_deltas(deltas);
        // `Holds`, not merely "not a violation".
        //
        // The claim the whole #5394 gap argument rests on is that every OTHER law
        // HOLDS on this contract — a contract that satisfies the entire settled
        // property set and still diverges. `!is_violation()` is also satisfied by
        // `Inconclusive`, so a case that silently stopped being evaluated at all
        // would keep this test green while the claim it exists to support quietly
        // became unsupported.
        assert_eq!(
            verify_case(&mut fake, &built),
            PropertyOutcome::Holds,
            "{property} did not HOLD on the disagreeing-paths contract, so it no \
             longer demonstrates the #5394 gap (a contract that satisfies every \
             existing law and still diverges)"
        );
    }
}

/// A weak delta encoding is not a disagreement, and this is the guard that keeps the
/// property off a large legitimate class.
///
/// The contract is a plain union semilattice whose delta carries only PART of what
/// the other state holds — the shape of a coarse version clock or a compact digest,
/// which `a_weak_delta_path_with_a_sound_merge_is_not_a_cycle` already refuses to
/// accuse under `ReconciliationCycle`. Its delta path lands short of the merged
/// state on this round and catches up on the next one.
///
/// Without the re-merge guard in `path_agreement` this test fails: the raw
/// comparison `apply(base, delta) != merge(base, other)` is true here.
#[test]
fn a_delta_that_carries_only_part_of_the_other_state_is_not_a_disagreement() {
    // Ships only the smallest missing byte, so the delta path always lags.
    let mut fake = Fake::conforming()
        .deltaing(|state, summary| Ok(difference(state, summary).into_iter().take(1).collect()));

    let outcome = verify_case(
        &mut fake,
        &case(ConformanceProperty::PathAgreement, &[&[1], &[2, 3, 4]]),
    );
    assert_holds(outcome);

    // ...and the raw comparison really would have fired, so the assertion above is
    // not passing because the two paths happened to agree.
    let mut same = Fake::conforming()
        .deltaing(|state, summary| Ok(difference(state, summary).into_iter().take(1).collect()));
    let delta = same.get_state_delta(&[2, 3, 4], &[1]).expect("delta");
    let delta_path = union(&[1], &delta);
    let merge_path = union(&[1], &[2, 3, 4]);
    assert_ne!(
        delta_path, merge_path,
        "this fixture no longer exercises the partial-delta case the guard exists \
         for, so the assertion above passes for the wrong reason"
    );
}

/// An empty delta is the protocol's "nothing to send", so there is no delta path to
/// compare the merge path against and the honest answer is a refusal.
#[test]
fn a_contract_with_no_delta_path_is_inconclusive_rather_than_accused() {
    let mut fake = Fake::conforming()
        .summarizing(|_| Ok(vec![0]))
        .deltaing(|_state, _summary| Ok(Vec::new()));

    assert_inconclusive(
        verify_case(
            &mut fake,
            &case(ConformanceProperty::PathAgreement, &[&[1, 2], &[3, 4]]),
        ),
        Inconclusive::NoDeltaPath,
    );
}

/// A last-write-wins merge heals trivially under the guard, so this property
/// declines to pile a second accusation onto a defect `StateCommutativity` already
/// names. One law per property — the same rule `DeltaPermutationInvariance` follows
/// with respect to `DeltaIdempotence`.
#[test]
fn a_broken_merge_is_left_to_the_property_that_names_it() {
    let mut fake = Fake::conforming().merging(|_a, b| Ok(b.to_vec()));
    let outcome = verify_case(
        &mut fake,
        &case(ConformanceProperty::PathAgreement, &[&[1, 2], &[2, 3]]),
    );
    assert!(
        !outcome.is_violation(),
        "path agreement must not re-accuse a contract whose merge is the defect: \
         {outcome:?}"
    );
    assert_violates(
        verify_case(
            &mut fake,
            &case(ConformanceProperty::StateCommutativity, &[&[1, 2], &[2, 3]]),
        ),
        ConformanceProperty::StateCommutativity,
    );
}

/// A defect visible from only ONE of the two directions must still be found.
///
/// The pin for the second `path_agreement` call. The mode-8 fixture disagrees in
/// both directions, so a test built on it passes whether or not the reverse
/// direction is checked at all — which is a test that pins nothing. This contract's
/// delta path adds a byte the merge path never produces, but only when the BASE
/// already carries a marker, so exactly one of the two directions can see it.
///
/// The pair is listed with the marker state SECOND, which is the direction a
/// single-direction check would miss.
#[test]
fn a_defect_visible_from_only_one_direction_is_still_found() {
    const MARKER: u8 = 0xEE;
    const EXTRA: u8 = 0xFF;
    let make = || {
        Fake::conforming().applying(|base, delta| {
            let mut out = union(base, delta);
            if base.contains(&MARKER) {
                out = union(&out, &[EXTRA]);
            }
            Ok(out)
        })
    };

    // Marker state second: the forward direction (base = the plain state) agrees,
    // so only the reverse direction can find this.
    assert_violates(
        verify_case(
            &mut make(),
            &case(
                ConformanceProperty::PathAgreement,
                &[&[0x01, 0x02], &[0x01, MARKER]],
            ),
        ),
        ConformanceProperty::PathAgreement,
    );

    // And the forward direction really does agree, so the assertion above is not
    // passing because both directions happened to fire.
    let mut fake = make();
    let delta = fake
        .get_state_delta(&[0x01, MARKER], &[0x01, 0x02])
        .expect("delta");
    assert_eq!(
        union(&[0x01, 0x02], &delta),
        union(&[0x01, 0x02], &[0x01, MARKER]),
        "the forward direction must AGREE for this to pin the reverse one"
    );
}

/// Which state the corpus happened to list first must not decide whether a defect is
/// found. The generator emits each unordered pair once, so a one-directional check
/// would make the finding depend on file order.
#[test]
fn a_disagreement_is_found_from_either_order_of_the_pair() {
    for states in [
        [&[0x10u8, 0x51u8][..], &[0x10, 0x52][..]],
        [&[0x10, 0x52][..], &[0x10, 0x51][..]],
    ] {
        let mut fake = disagreeing_paths();
        assert_violates(
            verify_case(
                &mut fake,
                &case(ConformanceProperty::PathAgreement, &states),
            ),
            ConformanceProperty::PathAgreement,
        );
    }
}

// -------------------------------------- #5394: the transition-shaped half of the law

/// Build the `(base, result)` step a peer would have recorded, by running the
/// contract's own delta path — so the test cannot accidentally assert against a
/// result the contract would never have produced.
fn transition_case(fake: &mut Fake, base: &[u8], delta: &[u8]) -> ConformanceCase {
    let result = fake
        .update_state(
            base,
            &[UpdateData::Delta(
                freenet_stdlib::prelude::StateDelta::from(delta.to_vec()),
            )],
        )
        .expect("apply")
        .new_state
        .expect("new state")
        .into_bytes();
    ConformanceCase::new(
        ConformanceProperty::TransitionPathAgreement,
        vec![bytes(base), bytes(&result)],
    )
}

/// The positive case, and the one that reaches the defect #5394 was written from.
///
/// A peer at `[0x10, 0x51]` receives an op for key 5 carrying a different value and
/// its delta path lands on `[0x10, 0x52]`. Merging that state back into the base it
/// came from resurrects `0x51`, so no peer that receives it as a whole state can
/// reach where the peer that applied the delta already is.
#[test]
fn a_reached_state_the_merge_path_cannot_reproduce_is_caught() {
    let mut fake = disagreeing_paths();
    let built = transition_case(&mut fake, &[0x10, 0x51], &[0x52]);
    // The delta path really did move somewhere the base was not, so the case is not
    // asserting against a no-op transition.
    assert_ne!(
        built.states[0], built.states[1],
        "a transition that changed nothing proves nothing"
    );
    assert_violates(
        verify_case(&mut fake, &built),
        ConformanceProperty::TransitionPathAgreement,
    );
}

/// The matched negative: the SAME contract, the SAME two write paths, an op whose
/// key collides with nothing. A property that fired on every transition would pass
/// the test above while being worse than the gap it closes.
#[test]
fn the_same_contract_is_silent_on_a_transition_whose_key_does_not_collide() {
    let mut fake = disagreeing_paths();
    let built = transition_case(&mut fake, &[0x10, 0x51], &[0x62]);
    assert_ne!(
        built.states[0], built.states[1],
        "a transition that changed nothing proves nothing"
    );
    assert_holds(verify_case(&mut fake, &built));
}

/// A bounded collection that evicts by the merge's OWN ordering is a genuine
/// bounded semilattice and must not be accused.
///
/// This is the false-positive risk that decides the severity. "Keep the newest N"
/// is one of the most common shapes a real application writes, and the entries the
/// base would re-add on a merge are exactly the ones the cap drops again — so the
/// merge path reproduces what the delta path reached, and the law holds.
///
/// The contrast is `capped_collection_evicting_outside_the_merge_order_is_caught`
/// below: the same cap, evicting by something independent of that ordering, does
/// fire. Neither result is assumed; both are asserted.
#[test]
fn a_sound_bounded_collection_is_not_accused() {
    const CAP: usize = 3;
    let keep_largest = |a: &[u8], b: &[u8]| {
        let mut out = union(a, b);
        while out.len() > CAP {
            out.remove(0);
        }
        Ok(out)
    };
    let mut fake = Fake::conforming()
        .merging(keep_largest)
        .applying(keep_largest);

    let built = transition_case(&mut fake, &[1, 2], &[3, 4, 5]);
    assert_eq!(
        built.states[1].as_ref(),
        &[3, 4, 5],
        "the cap must actually have evicted something, or this tests nothing"
    );
    assert_holds(verify_case(&mut fake, &built));
}

/// The same cap, evicting by something INDEPENDENT of the merge's own ordering, is
/// caught — and that is correct rather than a false positive: such a contract is
/// already removal-eligible under `StateAssociativity`, for the same underlying
/// reason (an entry dropped early destroys information a different merge order
/// would have kept).
///
/// Asserted rather than left in prose, because it is the boundary the severity
/// argument rests on: the property distinguishes the two caps, and does not simply
/// flag every bounded collection.
#[test]
fn capped_collection_evicting_outside_the_merge_order_is_caught() {
    const CAP: usize = 3;
    let evict_by_content = |a: &[u8], b: &[u8]| {
        let mut out = union(a, b);
        while out.len() > CAP {
            let sum: usize = out.iter().map(|byte| *byte as usize).sum();
            out.remove(sum % out.len());
        }
        Ok(out)
    };
    let mut fake = Fake::conforming()
        .merging(evict_by_content)
        .applying(evict_by_content);

    let built = transition_case(&mut fake, &[1, 2], &[3, 4, 5]);
    assert_violates(
        verify_case(&mut fake, &built),
        ConformanceProperty::TransitionPathAgreement,
    );

    // ...and the SOUND cap above really is a different answer from this one, so the
    // pair together shows the property discriminates rather than flagging all caps.
    let mut sound = Fake::conforming()
        .merging(|a: &[u8], b: &[u8]| {
            let mut out = union(a, b);
            while out.len() > CAP {
                out.remove(0);
            }
            Ok(out)
        })
        .applying(|a: &[u8], b: &[u8]| {
            let mut out = union(a, b);
            while out.len() > CAP {
                out.remove(0);
            }
            Ok(out)
        });
    let sound_case = transition_case(&mut sound, &[1, 2], &[3, 4, 5]);
    assert_holds(verify_case(&mut sound, &sound_case));
}

/// The PARTIALLY-ORDERED cap: keep the at-most-N maximal elements under a causal
/// partial order, breaking ties among mutually incomparable survivors by a total
/// order.
///
/// This is the bounded-collection shape the first version of this property could not
/// rule out. Unlike "keep the largest N" it is not obviously a bounded semilattice,
/// and unlike the content-indexed eviction in
/// `capped_collection_evicting_outside_the_merge_order_is_caught` it does not look
/// arbitrary — it is the shape a version-vector or causal-log application actually
/// writes, and it survives the pairwise laws.
///
/// Brute-forced over its whole state space on 2026-08-23 (universe of five elements,
/// `1` causally after `5`, N = 2, ties by keeping the largest: 15 valid states) it
/// is commutative and idempotent with zero failures, and NOT associative — 532
/// failing triples. So it is already removal-eligible under `StateAssociativity`
/// before this property is consulted, which is what closes the gap: the transition
/// law condemns no contract the settled algebra acquits. It fires here too, which is
/// the consistency the semilattice argument in `TransitionPathAgreement`'s
/// documentation predicts rather than a second, independent accusation.
///
/// Both halves are asserted. Asserting only the firing would leave the important
/// half — that associativity already had it — as prose.
#[test]
fn a_partially_ordered_cap_is_already_caught_by_associativity() {
    const N: usize = 2;
    // `1` is causally after `5`, so a set holding both keeps only `1`.
    fn dominates(after: u8, before: u8) -> bool {
        after == 1 && before == 5
    }
    fn cap(a: &[u8], b: &[u8]) -> Result<Vec<u8>, OracleError> {
        let all = union(a, b);
        let mut out: Vec<u8> = all
            .iter()
            .copied()
            .filter(|x| !all.iter().any(|y| dominates(*y, *x)))
            .collect();
        // Ties among incomparable survivors go to the total order.
        while out.len() > N {
            out.remove(0);
        }
        Ok(out)
    }
    let fake = || {
        Fake::conforming()
            .merging(cap)
            .applying(cap)
            .validating(|state| {
                let canonical = cap(state, &[]).expect("cap is infallible");
                if canonical == state {
                    Ok(ValidateResult::Valid)
                } else {
                    Ok(ValidateResult::Invalid)
                }
            })
    };

    // The half that closes the gap: associativity already condemns it. The triple is
    // the smallest of the 532 the brute force found.
    assert_violates(
        verify_case(
            &mut fake(),
            &case(
                ConformanceProperty::StateAssociativity,
                &[&[1], &[2], &[3, 5]],
            ),
        ),
        ConformanceProperty::StateAssociativity,
    );

    // ...and the pairwise laws do NOT, which is why the shape looked plausible.
    for pairwise in [
        case(ConformanceProperty::StateCommutativity, &[&[1], &[3, 5]]),
        case(ConformanceProperty::StateIdempotence, &[&[2, 5]]),
    ] {
        let property = pairwise.property;
        assert_eq!(
            verify_case(&mut fake(), &pairwise),
            PropertyOutcome::Holds,
            "{property} must hold, or this fixture is not the hard case it claims \
             to be"
        );
    }

    // The transition law fires too, on the step the brute force identified.
    let mut oracle = fake();
    let built = transition_case(&mut oracle, &[2, 5], &[1, 3]);
    assert_eq!(
        built.states[1].as_ref(),
        &[2, 3],
        "the op must actually reach the measured state, or the assertion below is \
         about something else"
    );
    assert_violates(
        verify_case(&mut oracle, &built),
        ConformanceProperty::TransitionPathAgreement,
    );
}

/// A contract that rewrites a stored state into canonical form on first merge must
/// not be accused.
///
/// The PUT install path stores the client's raw bytes without ever running
/// `update_state`, so the state a peer holds — and therefore the `result` a
/// transition records — may not be canonical yet. Comparing against those raw bytes
/// would report that legitimate rewrite as a merge-law break, which is why `result`
/// is driven to a fixpoint first.
#[test]
fn a_canonicalizing_contract_is_not_accused_by_the_transition_law() {
    // Accepts a trailing marker byte but strips it on any merge, then stabilizes.
    const MARKER: u8 = 0xFF;
    let strip = |a: &[u8], b: &[u8]| {
        let mut out = union(a, b);
        out.retain(|byte| *byte != MARKER);
        Ok(out)
    };
    let mut fake = Fake::conforming().merging(strip).applying(|a, d| {
        // The delta path leaves the marker in place, so the recorded result is a
        // non-canonical state exactly as a PUT-installed one would be.
        Ok(union(a, d))
    });

    let built = transition_case(&mut fake, &[1, 2], &[MARKER]);
    assert_eq!(
        built.states[1].as_ref(),
        &[1, 2, MARKER],
        "the recorded result must be non-canonical, or the guard is untested"
    );
    assert_holds(verify_case(&mut fake, &built));
}

/// A merge that EMITS an invalid state is `EmittedStateValidity`'s defect, not this
/// one.
///
/// Both sides of the comparison get the same validity precondition, for the reason
/// every check in this module applies it: a state the contract itself rejects never
/// reaches another peer, so reasoning about it is reasoning about a history that
/// cannot happen. `path_agreement` validates both of its outputs already; without
/// the matching check here the transition branch validated only the settled result
/// and reported the emitted-invalid-state defect under its own name — accusing the
/// right contract under the wrong law.
#[test]
fn a_merge_that_emits_an_invalid_state_is_not_reported_under_the_transition_law() {
    // Merging two DIFFERENT states emits a state the contract rejects; merging a
    // state with itself does not.
    //
    // That asymmetry is the whole fixture. The transition branch already validated
    // `settled`, so a contract whose SELF-merge emits an invalid state trips that
    // older check and would make this test pass whether or not the new one exists —
    // which is exactly how the first version of this test was vacuous. Here `result`
    // reaches its fixpoint immediately and validates, so `merge(base, settled)` is
    // the only call that can produce an invalid state, and only the new check can
    // see it. Verified by mutation: deleting `require_valid(&merged)` makes this
    // report a `TransitionPathAgreement` violation instead.
    const MARKER: u8 = 0xFE;
    let mut fake = Fake::conforming()
        .merging(|a, b| {
            let mut out = union(a, b);
            if a != b {
                out.push(MARKER);
            }
            Ok(out)
        })
        .applying(|a, d| Ok(union(a, d)))
        .validating(|state| {
            if is_canonical(state) && !state.contains(&MARKER) {
                Ok(ValidateResult::Valid)
            } else {
                Ok(ValidateResult::Invalid)
            }
        });

    let built = case(
        ConformanceProperty::TransitionPathAgreement,
        &[&[1, 2], &[1, 2, 3]],
    );
    assert_inconclusive(verify_case(&mut fake, &built), Inconclusive::InputNotValid);
}

/// A result state that keeps rewriting itself cannot be judged here, and the defect
/// already has a name. Reporting it under this law would accuse the right contract
/// under the wrong one.
#[test]
fn a_result_state_that_never_settles_is_inconclusive_not_a_violation() {
    let mut fake = Fake::conforming()
        .validating(|_| Ok(ValidateResult::Valid))
        .merging(|a, b| {
            Ok(union(a, b)
                .iter()
                .map(|byte| byte.wrapping_add(1))
                .collect())
        });

    assert_inconclusive(
        verify_case(
            &mut fake,
            &case(
                ConformanceProperty::TransitionPathAgreement,
                &[&[1, 2], &[3, 4]],
            ),
        ),
        Inconclusive::StateNotSettled,
    );
}

/// Deduplication must never trade a provenanced delta for an unprovenanced twin.
///
/// A delta can legitimately arrive twice: once loose and once attached to the step
/// it was observed on. `ReplayBundle::to_corpus` builds exactly that shape, giving
/// bundle-level deltas no base by design and the transition's copy the base it was
/// applied to. First-seen-wins then keeps whichever the caller happened to push
/// first, and an unprovenanced delta is never paired at all — so
/// `delta_permutation_invariance` silently checks nothing while the corpus still
/// reports the same delta count.
///
/// Both orders are asserted. Only checking the order that happens to be broken today
/// would leave the invariant hostage to which list a future caller fills first.
#[test]
fn deduplicating_deltas_keeps_the_base_whichever_copy_arrives_first() {
    let delta = bytes(&[9]);
    let base = bytes(&[1, 2]);

    for (label, bases) in [
        ("unprovenanced copy first", vec![None, Some(base.clone())]),
        ("provenanced copy first", vec![Some(base.clone()), None]),
    ] {
        let corpus = Corpus {
            deltas: vec![delta.clone(), delta.clone()],
            delta_bases: bases,
            ..Corpus::from_states(vec![vec![1, 2]])
        }
        .deduplicated();
        assert_eq!(
            corpus.deltas.len(),
            1,
            "{label}: the duplicate must collapse"
        );
        assert_eq!(
            corpus.delta_base(0),
            Some(&base),
            "{label}: the surviving delta must keep the state it was applied to"
        );
    }
}

/// A corpus holding steps and no loose states is not empty.
///
/// `generate_cases` returns early on an empty corpus, so a states-only emptiness
/// test short-circuits before the transition queue is ever built — and the one
/// property that depends on provenance would silently check nothing while the run
/// exited 0. The endpoints happen to be pushed into `states` by `fdev --transition`
/// today, so this is a latent trap rather than a live one; it is exactly the kind
/// that a future caller (the sampler's own records, a bundle carrying only steps)
/// walks into.
#[test]
fn a_corpus_of_steps_alone_is_not_empty() {
    let steps_only = Corpus {
        transitions: vec![(bytes(&[1]), bytes(&[1, 2]))],
        ..Default::default()
    };
    assert!(
        !steps_only.is_empty(),
        "a recorded step is material to check, so a corpus holding one is not empty"
    );
    let config = GeneratorConfig {
        properties: vec![ConformanceProperty::TransitionPathAgreement],
        ..Default::default()
    };
    assert_eq!(
        generate_cases(&steps_only, &config).len(),
        1,
        "and the early return must not swallow it"
    );

    // The counterpart, so an `is_empty` that always returned false would fail here.
    assert!(Corpus::default().is_empty());
}

/// The transition branch is bounded like every other arity branch.
///
/// It does not pair anything so it does not grow quadratically, but it is linear in
/// a corpus a busy contract fills without limit, and an unbounded queue would let
/// one contract's step history crowd the interleave and decide which laws the case
/// budget reaches — the thing the interleave exists to prevent.
///
/// Strided, not truncated, for the same reason `paired_states` strides: steps arrive
/// in time order, so the first N are all from the same few minutes.
#[test]
fn the_transition_branch_is_bounded_and_strided() {
    let config = GeneratorConfig {
        properties: vec![ConformanceProperty::TransitionPathAgreement],
        max_transitions: 4,
        max_cases: 1024,
        ..Default::default()
    };
    let corpus = Corpus {
        transitions: (0..40u8).map(|i| (bytes(&[i]), bytes(&[i, 200]))).collect(),
        ..Corpus::from_states(vec![vec![1]])
    };
    let cases = generate_cases(&corpus, &config);
    assert_eq!(cases.len(), 4, "the cap must bind");
    // Strided over the whole history rather than the first four.
    //
    // Say what this actually pins: the four selected steps are spread across the
    // range, not the first four. Index 39 is NOT selected — a stride of 10 from 0
    // reaches 30 — so this is not a claim that the newest step is reachable. What it
    // rules out is truncation, under which the bases would be `[0, 1, 2, 3]` and a
    // contract would only ever be checked against its own oldest few minutes.
    let bases: Vec<u8> = cases.iter().map(|c| c.states[0][0]).collect();
    assert_eq!(bases, vec![0, 10, 20, 30]);
    assert_ne!(
        bases,
        vec![0, 1, 2, 3],
        "truncation is the thing this test exists to exclude, so name it"
    );
}

/// The generator must build transition cases ONLY from recorded provenance.
///
/// This is the pin that keeps the property from becoming an accusation of
/// last-write-wins against every conforming contract. "Merging B into A yields B" is
/// false for a union semilattice on an arbitrary pair; it is a law only when the
/// corpus witnesses that B was reached FROM A. If this property ever fell through to
/// the generic arity-2 branch — which pairs every state with every other — the
/// conforming baseline would start failing, and it would look like a real finding.
#[test]
fn transition_cases_come_only_from_recorded_provenance() {
    let config = GeneratorConfig {
        properties: vec![ConformanceProperty::TransitionPathAgreement],
        ..Default::default()
    };

    // Negative: plenty of states, no provenance, so nothing to check.
    let loose = Corpus::from_states(vec![vec![1], vec![2], vec![1, 2], vec![2, 3]]);
    assert!(
        generate_cases(&loose, &config).is_empty(),
        "states that merely appeared together are not a transition; pairing them \
         would accuse every conforming contract of last-write-wins"
    );

    // Positive: one recorded step yields exactly one case, in the recorded order.
    let witnessed = Corpus {
        transitions: vec![(bytes(&[1]), bytes(&[1, 2]))],
        ..Corpus::from_states(vec![vec![1], vec![1, 2]])
    };
    let cases = generate_cases(&witnessed, &config);
    assert_eq!(cases.len(), 1, "one recorded step is one case");
    assert_eq!(cases[0].states[0].as_ref(), &[1], "base comes first");
    assert_eq!(cases[0].states[1].as_ref(), &[1, 2], "result comes second");
}

/// A bundle must carry provenance across a round trip.
///
/// `to_corpus` flattens transitions into loose states, and before this it dropped
/// the ORDERING while doing so — which would leave a replayed capture unable to
/// check the one property that needs it, silently, and reading as a clean run.
#[test]
fn a_bundle_round_trip_preserves_transition_provenance() {
    let mut bundle = super::bundle::ReplayBundle::new(b"code".to_vec(), Vec::new());
    bundle.transitions.push(super::bundle::Transition {
        base_state: vec![1],
        result_state: vec![1, 2],
        ..Default::default()
    });

    let decoded =
        super::bundle::ReplayBundle::decode(&bundle.encode().expect("encode")).expect("decode");
    let corpus = decoded.to_corpus();
    assert_eq!(
        corpus.transitions,
        vec![(bytes(&[1]), bytes(&[1, 2]))],
        "a replayed capture must still know which state came first"
    );
}

fn instance(seed: u8) -> ContractInstanceId {
    ContractInstanceId::new([seed; 32])
}

#[test]
fn evidence_round_trips_through_a_case() {
    let original = case(ConformanceProperty::StateCommutativity, &[&[1, 2], &[2, 3]]);
    let evidence = ConformanceEvidence::new(instance(7), vec![9, 9], &original, None);
    evidence.check_bounds().expect("bounds");

    let rebuilt = evidence.to_case().expect("to_case");
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

/// Related state lives in a `HashMap` on the way in, whose iteration order differs
/// between peers and between runs. If that order reached the hash, the same
/// reproducer would get different ids on different peers, deduplication would fail
/// open, and one finding would circulate once per discoverer.
#[test]
fn evidence_id_does_not_depend_on_related_contract_ordering() {
    let case = case(ConformanceProperty::StateIdempotence, &[&[1]]);
    let mut forward = ConformanceEvidence::new(instance(1), vec![], &case, None);
    forward.related = vec![
        (instance(3), vec![3]),
        (instance(1), vec![1]),
        (instance(2), vec![2]),
    ];
    let mut reversed = forward.clone();
    reversed.related.reverse();

    assert_ne!(
        forward.related, reversed.related,
        "fixture failed: the two orderings are identical, so this proves nothing"
    );
    assert_eq!(forward.id(), reversed.id());
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

/// The fifth `check_bounds` branch, and the only one that is not about size or
/// schema: a property whose premise the evidence bytes cannot carry is refused
/// outright.
///
/// This is the branch that keeps the ship-inputs-not-verdicts design sound. Every
/// other law is a universally quantified identity over valid states, so a recipient
/// that re-executes the case re-establishes the whole premise and a fabricated case
/// can only surface a real defect sooner. `TransitionPathAgreement` is a law only
/// because the SENDER witnessed that `result` was reached from `base`, and that
/// witness is not in the bytes — so a fabricated pair from a conforming grow-only
/// contract would have every recipient independently confirm a removal-eligible
/// violation against a correct contract.
#[test]
fn evidence_for_a_property_that_is_not_self_verifying_is_refused() {
    let case = case(
        ConformanceProperty::TransitionPathAgreement,
        &[&[1], &[1, 2]],
    );
    let evidence = ConformanceEvidence::new(instance(1), vec![], &case, None);
    assert!(
        evidence.input_bytes() < MAX_EVIDENCE_INPUT_BYTES,
        "the fixture must be well within every OTHER bound, or this could pass for \
         the wrong reason"
    );
    assert_eq!(
        evidence.check_bounds(),
        Err(EvidenceRejected::NotSelfVerifying {
            property: ConformanceProperty::TransitionPathAgreement,
        })
    );
}

/// The counterpart: a self-verifying property with identical shape is accepted.
///
/// Without this, a `check_bounds` that rejected everything would satisfy the test
/// above.
#[test]
fn evidence_for_a_self_verifying_property_is_accepted() {
    let case = case(ConformanceProperty::StateCommutativity, &[&[1], &[1, 2]]);
    let evidence = ConformanceEvidence::new(instance(1), vec![], &case, None);
    assert_eq!(evidence.check_bounds(), Ok(()));
}

/// Every property must have a CONSIDERED answer to "can a recipient re-establish
/// this premise from the bytes alone?".
///
/// An exhaustive match already makes a new property fail to compile without an
/// answer. This pins which answer was given, so a new provenance-dependent property
/// lumped in with the self-verifying ones fails here instead of silently widening
/// the untrusted path — the exact hazard `TransitionPathAgreement` introduced.
///
/// The list below has two entries rather than one because the first version of this
/// pin recorded the wrong answer for a property that already existed:
/// `DeltaPermutationInvariance` was classified `EvidenceBytes`, and the loop at the
/// bottom of this test therefore asserted that a fabricated 1-state/2-delta case for
/// it must be ACCEPTED. That is a caution about the shape of this pin, not just its
/// contents — it records the answer given, and a wrong answer is pinned exactly as
/// firmly as a right one. Read the property's own documentation before adding to it.
///
/// If you are here because you added a property: decide whether re-executing the
/// case against a local copy of the contract re-establishes EVERYTHING the law
/// asserts. If any part of it rests on how the inputs were observed, it is
/// `LocalProvenance` and belongs in the list below.
#[test]
fn every_property_declares_whether_it_is_self_verifying() {
    let local: Vec<ConformanceProperty> = ConformanceProperty::ALL
        .iter()
        .copied()
        .filter(|p| p.premise_source() == PremiseSource::LocalProvenance)
        .collect();
    assert_eq!(
        local,
        vec![
            ConformanceProperty::DeltaPermutationInvariance,
            ConformanceProperty::TransitionPathAgreement,
        ],
        "the set of properties that cannot travel as evidence changed; if that is \
         deliberate, update this pin, and make sure `check_bounds` still refuses \
         every one of them"
    );
    assert_eq!(
        ConformanceProperty::ALL.len(),
        14,
        "a property was added or removed; say explicitly whether it is \
         self-verifying (see `ConformanceProperty::premise_source`) rather than \
         letting it inherit an answer, then update this count"
    );

    // The classification is not decorative: the gate reads it.
    for property in ConformanceProperty::ALL {
        let states = (0..property.state_arity())
            .map(|i| bytes(&[i as u8]))
            .collect();
        let deltas = (0..property.delta_arity())
            .map(|i| bytes(&[0x80 | i as u8]))
            .collect();
        let built = ConformanceCase::new(*property, states).with_deltas(deltas);
        let evidence = ConformanceEvidence::new(instance(1), vec![], &built, None);
        assert_eq!(
            evidence.check_bounds().is_ok(),
            property.is_self_verifying(),
            "{property}: check_bounds must accept exactly the self-verifying \
             properties"
        );
    }
}

/// `DeltaPermutationInvariance` is refused as evidence, with its own test rather
/// than only as a row in the pin above.
///
/// The pin above records the classification; this records WHY, so a future reader
/// weighing "surely a delta pair is just bytes" has the counterexample in front of
/// them. The generator pairs only deltas observed against the SAME base
/// (`generator::delta_pairs`), because deltas observed against different bases can be
/// causally sequenced. Evidence carries no base for a delta at all — `states` and
/// `deltas` and nothing else — so a recipient re-running the case reproduces the
/// comparison without the premise.
///
/// The concrete victim: an add-wins OR-set whose delta encodes tag adds and removes
/// and which does not tombstone a tag it has never seen. Sound in production, because
/// a delta is always `get_state_delta(sender, recipient_summary)`. Fed the
/// causally-sequenced pair `D1 = add A^t`, `D2 = remove t`, the two orders diverge —
/// and this property is `Severity::Violation`, which `policy::decide` maps to
/// `ConformanceAction::Remove` in Enforce mode. Every recipient would independently
/// confirm a removal against a correct contract.
#[test]
fn evidence_for_delta_permutation_invariance_is_refused() {
    let built = ConformanceCase::new(
        ConformanceProperty::DeltaPermutationInvariance,
        vec![bytes(&[1])],
    )
    .with_deltas(vec![bytes(&[0x80]), bytes(&[0x81])]);
    let evidence = ConformanceEvidence::new(instance(1), vec![], &built, None);
    assert!(
        evidence.input_bytes() < MAX_EVIDENCE_INPUT_BYTES,
        "the fixture must be well within every OTHER bound, or this could pass for \
         the wrong reason"
    );
    assert_eq!(
        evidence.states.len(),
        ConformanceProperty::DeltaPermutationInvariance.state_arity(),
        "the fixture must satisfy the arity check, or this could pass for the wrong \
         reason"
    );
    assert_eq!(
        evidence.deltas.len(),
        ConformanceProperty::DeltaPermutationInvariance.delta_arity(),
        "the fixture must satisfy the arity check, or this could pass for the wrong \
         reason"
    );
    assert_eq!(
        evidence.check_bounds(),
        Err(EvidenceRejected::NotSelfVerifying {
            property: ConformanceProperty::DeltaPermutationInvariance,
        })
    );
    assert_eq!(
        evidence.to_case().err(),
        Some(EvidenceRejected::NotSelfVerifying {
            property: ConformanceProperty::DeltaPermutationInvariance,
        }),
        "and the gate must hold at the point where a case is built, not only where \
         someone remembered to call `check_bounds`"
    );
}

/// Does `verify_case` hand this property the SUPPLIED `ConformanceCase::summary`?
///
/// Summary bytes are unvalidated in exactly the way delta bytes are: `check_bounds`
/// constrains their size and nothing else, and no `require_valid` analogue exists for
/// a summary. So a property that reads a supplied summary is in the same hazard class
/// as one that reads a delta, and
/// [`no_shippable_removal_eligible_property_consumes_unvalidated_bytes`] must see it.
///
/// A local exhaustive match rather than a method on `ConformanceProperty`: this is a
/// fact about `verify_case`'s branches, not part of the type's public contract, and
/// matching exhaustively inside the crate still makes a new variant fail to compile.
fn consumes_supplied_summary(property: ConformanceProperty) -> bool {
    match property {
        // `verifier.rs`'s `DeltaDeterminism` arm uses `case.summary` when present and
        // falls back to `summarize_state` only when it is `None`.
        ConformanceProperty::DeltaDeterminism => true,
        ConformanceProperty::StateIdempotence
        | ConformanceProperty::StateCommutativity
        | ConformanceProperty::StateAssociativity
        | ConformanceProperty::EmittedStateValidity
        | ConformanceProperty::UpdateDeterminism
        | ConformanceProperty::SummaryDeterminism
        | ConformanceProperty::DeltaIdempotence
        | ConformanceProperty::DeltaPermutationInvariance
        | ConformanceProperty::SelfDeltaEmpty
        | ConformanceProperty::WholeStateSelfDelta
        | ConformanceProperty::ReconciliationCycle
        | ConformanceProperty::PathAgreement
        | ConformanceProperty::TransitionPathAgreement => false,
    }
}

/// Can choosing the unvalidated bytes MANUFACTURE a verdict against a contract that
/// is in fact conforming?
///
/// This is the question the hazard is actually about, and for one shape of law the
/// answer is no by construction: a determinism law compares the contract against
/// ITSELF on byte-identical inputs, so the verdict is "the same call returned
/// different bytes twice". No choice of input makes a deterministic implementation
/// nondeterministic, so a fabricated input buys an attacker nothing an honest one
/// does not — it can only surface a real defect sooner.
///
/// That is NOT true of the comparison laws. `DeltaIdempotence` compares two
/// DIFFERENT executions, and `DeltaPermutationInvariance` two different orders, so
/// whether the comparison means anything depends on the bytes being ones the
/// protocol could have produced. Those stay in the hazard class.
fn verdict_survives_fabricated_bytes(property: ConformanceProperty) -> bool {
    match property {
        ConformanceProperty::UpdateDeterminism
        | ConformanceProperty::SummaryDeterminism
        | ConformanceProperty::DeltaDeterminism => true,
        ConformanceProperty::StateIdempotence
        | ConformanceProperty::StateCommutativity
        | ConformanceProperty::StateAssociativity
        | ConformanceProperty::EmittedStateValidity
        | ConformanceProperty::DeltaIdempotence
        | ConformanceProperty::DeltaPermutationInvariance
        | ConformanceProperty::SelfDeltaEmpty
        | ConformanceProperty::WholeStateSelfDelta
        | ConformanceProperty::ReconciliationCycle
        | ConformanceProperty::PathAgreement
        | ConformanceProperty::TransitionPathAgreement => false,
    }
}

/// The hazard class, stated once so a new property cannot re-enter it.
///
/// `verify_case` validates every STATE in a case through `require_valid`, and never
/// validates a delta or a supplied summary — there is no `require_valid` analogue for
/// either, because a contract exposes no "is this delta/summary well-formed" entry
/// point. So a property that (a) travels as evidence, (b) consumes delta or supplied
/// summary bytes, and (c) is removal-eligible would let an attacker choose bytes that
/// go straight into another peer's WASM and come back out as a removal verdict.
///
/// Two properties carry deltas. `DeltaPermutationInvariance` is `Violation` and is
/// kept off the wire by `premise_source`. `DeltaIdempotence` ships, and is safe only
/// because it is `Diagnostic`, which `policy::decide` never turns into a removal —
/// so its severity is not independently adjustable, and its own documentation says
/// so. This test is what makes that a rule rather than a note: promoting it without
/// revisiting `premise_source` in the same change fails here.
///
/// One property consumes a supplied SUMMARY: `DeltaDeterminism`, which both ships and
/// is `Violation`. It is in scope here — the earlier version of this test filtered on
/// `delta_arity() > 0` and so could not see it at all, which left the guard narrower
/// than the class its own docstring names. It is exempt by
/// [`verdict_survives_fabricated_bytes`], and only by that: a fabricated summary
/// cannot make a deterministic contract return two different answers to the same
/// call, so it buys an attacker nothing. The exemption is a predicate rather than a
/// name in a list precisely so a NEW summary-consuming property has to answer the
/// question rather than inherit the answer.
#[test]
fn no_shippable_removal_eligible_property_consumes_unvalidated_bytes() {
    let consumes_unvalidated =
        |p: ConformanceProperty| p.delta_arity() > 0 || consumes_supplied_summary(p);

    let hazardous: Vec<ConformanceProperty> = ConformanceProperty::ALL
        .iter()
        .copied()
        .filter(|p| {
            consumes_unvalidated(*p)
                && p.is_self_verifying()
                && p.severity() == Severity::Violation
                && !verdict_survives_fabricated_bytes(*p)
        })
        .collect();
    assert!(
        hazardous.is_empty(),
        "{hazardous:?}: a property that ships as evidence, runs attacker-chosen \
         delta or summary bytes through the WASM, and is removal-eligible is the \
         combination the evidence gate exists to prevent. Either mark it \
         `PremiseSource::LocalProvenance`, or drop it to `Severity::Diagnostic`, or \
         give those bytes a validity check first — do not simply update this test"
    );

    // The fixture must be able to fail: at least one property really does carry
    // deltas, so the delta half of the filter is not vacuously empty.
    assert!(
        ConformanceProperty::ALL
            .iter()
            .any(|p| p.delta_arity() > 0 && p.is_self_verifying()),
        "no property carries deltas as evidence any more, so the assertion above \
         proves nothing; delete it or re-aim it"
    );

    // And the SUMMARY half must reach something, or widening the filter was
    // decoration. Everything the widened filter catches before the determinism
    // exemption is applied, so this fails both when the summary arm goes dead and
    // when a SECOND property starts leaning on that exemption.
    let carried_only_by_the_exemption: Vec<ConformanceProperty> = ConformanceProperty::ALL
        .iter()
        .copied()
        .filter(|p| {
            consumes_unvalidated(*p) && p.is_self_verifying() && p.severity() == Severity::Violation
        })
        .collect();
    assert_eq!(
        carried_only_by_the_exemption,
        vec![ConformanceProperty::DeltaDeterminism],
        "the set of shippable removal-eligible properties consuming unvalidated \
         bytes has changed. `DeltaDeterminism` is here because a fabricated summary \
         cannot manufacture a nondeterminism verdict; anything joining it needs that \
         argument made for it in `verdict_survives_fabricated_bytes`, and anything \
         leaving means the summary arm of the filter above now tests nothing"
    );
}

/// `to_case` is the gate, not the doc comment above it.
///
/// It used to hand back a runnable case unconditionally, with a rustdoc line asking
/// the caller to have run `check_bounds`. Every caller today does; the hazard is the
/// caller that does not exist yet, because the receive path (#5377) is unbuilt. When
/// it lands, a convention is what stands between an arbitrary byte string and the
/// WASM runtime — so the check moved inside.
#[test]
fn to_case_refuses_what_check_bounds_refuses() {
    let big = vec![0u8; MAX_EVIDENCE_INPUT_BYTES + 1];
    let oversized = ConformanceCase::new(
        ConformanceProperty::StateIdempotence,
        vec![Arc::from(big.as_slice())],
    );
    let evidence = ConformanceEvidence::new(instance(1), vec![], &oversized, None);
    assert!(matches!(
        evidence.to_case(),
        Err(EvidenceRejected::TooLarge { .. })
    ));

    // Arity too, so this is not a test about size alone.
    let wrong_arity = ConformanceCase::new(
        ConformanceProperty::StateAssociativity,
        vec![bytes(&[1]), bytes(&[2])],
    );
    let evidence = ConformanceEvidence::new(instance(1), vec![], &wrong_arity, None);
    assert!(matches!(
        evidence.to_case(),
        Err(EvidenceRejected::Arity { .. })
    ));

    // And the counterpart, so a `to_case` that refused everything would fail here.
    let fine = case(ConformanceProperty::StateCommutativity, &[&[1], &[1, 2]]);
    let evidence = ConformanceEvidence::new(instance(1), vec![], &fine, None);
    assert!(evidence.to_case().is_ok());
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

/// The false positive found on the live network, kept as a permanent case.
///
/// A delta is `get_state_delta(sender_state, recipient_summary)`, so two deltas
/// observed against DIFFERENT bases can be causally sequenced: the later one computed
/// from a state that already contains the earlier one's effect. Permuting those asks
/// what happens in a situation the protocol never produces, and the answer is not a
/// property of the contract.
///
/// This is not hypothetical. Replaying a live capture reported a
/// `DeltaPermutationInvariance` violation against a real contract, and it came from
/// exactly such a pair; the finding did not survive once pairing was restricted to a
/// shared base. RFC #5320 requires every false positive to become a permanent
/// regression case, and this is that case — without it, reverting the rule would
/// silently restore the accusation, because every other test supplies deltas that
/// share a base or none at all.
#[test]
fn deltas_observed_against_different_bases_are_never_paired() {
    let base_one: Bytes = Arc::from([1u8, 2].as_slice());
    let base_two: Bytes = Arc::from([3u8, 4].as_slice());

    let corpus = Corpus {
        deltas: vec![Arc::from([9u8].as_slice()), Arc::from([7u8].as_slice())],
        // Same two deltas, but each seen against a different state.
        delta_bases: vec![Some(base_one), Some(base_two)],
        ..Corpus::from_states(vec![vec![1, 2], vec![3, 4]])
    };

    let cases = generate_cases(&corpus, &GeneratorConfig::default());
    let permutation_cases = cases
        .iter()
        .filter(|c| c.property == ConformanceProperty::DeltaPermutationInvariance)
        .count();

    assert_eq!(
        permutation_cases, 0,
        "deltas seen against different bases must not be paired: they may be causally \
         sequenced, and permuting them accuses contracts of an order-dependence the \
         network never exercises"
    );

    // And the guard must not be so blunt that it stops the check working at all:
    // the same deltas sharing a base ARE a legitimate pair.
    let shared: Bytes = Arc::from([1u8, 2].as_slice());
    let paired = Corpus {
        deltas: vec![Arc::from([9u8].as_slice()), Arc::from([7u8].as_slice())],
        delta_bases: vec![Some(shared.clone()), Some(shared)],
        ..Corpus::from_states(vec![vec![1, 2], vec![3, 4]])
    };
    let paired_cases = generate_cases(&paired, &GeneratorConfig::default())
        .iter()
        .filter(|c| c.property == ConformanceProperty::DeltaPermutationInvariance)
        .count();
    assert!(
        paired_cases > 0,
        "deltas sharing a base are the genuine concurrent-update case and must still \
         be checked, or the fix has simply disabled the property"
    );
}

/// A case budget must narrow depth, not silently drop whole laws. If the generator
/// emitted every commutativity case before its first associativity case, a corpus
/// large enough to hit the budget would quietly stop checking associativity —
/// and nothing would say so.
#[test]
fn a_tight_case_budget_still_covers_every_law() {
    let states: Vec<Vec<u8>> = (1u8..=12).map(|i| vec![i]).collect();
    // Both deltas share a base, so the permutation check has a legitimate pair to
    // work with. Without provenance the generator declines to pair them at all —
    // deliberately, since permuting causally-sequenced deltas asks a question the
    // protocol never poses — and this test would then fail for the right reason.
    let base: Bytes = Arc::from([1u8].as_slice());
    let corpus = Corpus {
        deltas: vec![Arc::from([9u8].as_slice()), Arc::from([7u8].as_slice())],
        delta_bases: vec![Some(base.clone()), Some(base.clone())],
        // `transition_path_agreement` is gated on recorded provenance and would
        // otherwise contribute no cases at all — which would look like the budget
        // dropping a law when in fact the corpus never offered one.
        transitions: vec![(base, Arc::from([1u8, 9].as_slice()))],
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
