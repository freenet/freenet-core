//! Executes one conformance property against one contract.
//!
//! The logic here is deterministic given the oracle, which is what lets a peer ship
//! a case to another peer and have it reach the same conclusion independently, and
//! what lets `fdev` reproduce a network finding offline.
//!
//! The *contract* is not guaranteed to be, and that is the interesting part. A
//! contract can reach for the host clock, so the same case run twice can genuinely
//! produce different outputs. [`verify_case`] therefore re-runs any check that was
//! about to report a violation and requires the same finding, with the same outputs,
//! before it will accuse anyone — see the note there. If the module were as
//! deterministic as the first sentence sounds, that machinery would be dead code.

use std::collections::HashSet;
use std::sync::Arc;

use freenet_stdlib::prelude::{RelatedContracts, State, StateDelta, UpdateData, ValidateResult};

use super::oracle::{ConformanceOracle, OracleError, OracleErrorKind};
use super::property::{
    ConformanceProperty, Inconclusive, OutputDigest, PropertyOutcome, Violation,
};

/// Shared, cheaply-clonable input bytes.
pub type Bytes = Arc<[u8]>;

/// How many reconciliation rounds to simulate before giving up.
///
/// The RFC allows legitimate multi-round convergence, so this cannot be small.
/// Running out of rounds is [`Inconclusive::RoundLimit`], never a violation — the
/// only way to fail this check is to revisit an exact state pair, which is proof of
/// a cycle rather than of slowness.
pub const MAX_RECONCILIATION_ROUNDS: usize = 16;

/// How many times to repeat a call when checking determinism.
pub const DETERMINISM_REPEATS: usize = 3;

/// How many times `merge(A, A)` may change the state before the state is judged
/// never to settle.
///
/// Matches `IDENTITY_PROBE_MAX_APPLIES` in the executor's own probe, which arrived
/// at the same number for the same reason: a canonicalizing contract legitimately
/// rewrites a non-canonical stored state once and then stabilizes.
pub const MAX_CANONICALIZATION_APPLIES: usize = 3;

/// One property plus the exact inputs to check it with.
///
/// Cases are the unit of work everywhere: the generator produces them, the verifier
/// consumes them, and [`ConformanceEvidence`](super::ConformanceEvidence) is a
/// serializable case plus enough provenance to interpret it.
#[derive(Debug, Clone)]
pub struct ConformanceCase {
    pub property: ConformanceProperty,
    /// Input states, in the order the property's definition names them (`A`, `B`, `C`).
    pub states: Vec<Bytes>,
    /// Input deltas, where the property takes them.
    pub deltas: Vec<Bytes>,
    /// The summary a delta was generated against, where that matters for replay.
    pub summary: Option<Bytes>,
    /// Related-contract state the contract needs in order to validate the inputs.
    pub related: RelatedContracts<'static>,
}

impl ConformanceCase {
    pub fn new(property: ConformanceProperty, states: Vec<Bytes>) -> Self {
        Self {
            property,
            states,
            deltas: Vec::new(),
            summary: None,
            related: RelatedContracts::default(),
        }
    }

    pub fn with_deltas(mut self, deltas: Vec<Bytes>) -> Self {
        self.deltas = deltas;
        self
    }

    pub fn with_summary(mut self, summary: Bytes) -> Self {
        self.summary = Some(summary);
        self
    }

    pub fn with_related(mut self, related: RelatedContracts<'static>) -> Self {
        self.related = related;
        self
    }

    /// Total input bytes. Used to keep evidence and corpora inside their budgets.
    pub fn input_bytes(&self) -> usize {
        self.states.iter().map(|s| s.len()).sum::<usize>()
            + self.deltas.iter().map(|d| d.len()).sum::<usize>()
            + self.summary.as_ref().map_or(0, |s| s.len())
    }

    fn arity_ok(&self) -> Result<(), Inconclusive> {
        let want_states = self.property.state_arity();
        if self.states.len() < want_states {
            return Err(Inconclusive::MalformedCase(format!(
                "{} needs {want_states} states, got {}",
                self.property,
                self.states.len()
            )));
        }
        let want_deltas = self.property.delta_arity();
        if self.deltas.len() < want_deltas {
            return Err(Inconclusive::MalformedCase(format!(
                "{} needs {want_deltas} deltas, got {}",
                self.property,
                self.deltas.len()
            )));
        }
        Ok(())
    }
}

/// Check one property. This is the shared entry point: `fdev` and the node both
/// call exactly this function, so a finding means the same thing in both places.
pub fn verify_case<O: ConformanceOracle + ?Sized>(
    oracle: &mut O,
    case: &ConformanceCase,
) -> PropertyOutcome {
    let first = match run(oracle, case) {
        Ok(outcome) => outcome,
        Err(reason) => return PropertyOutcome::Inconclusive(reason),
    };

    // A violation must reproduce before it is reported.
    //
    // Nothing here is free of the clock. A contract that prunes expired entries
    // against `time::now()` — the shape of the in-tree ping contract, with a
    // five-second TTL — can have two merges milliseconds apart straddle an expiry
    // boundary, and the two sides then genuinely differ for a reason that has
    // nothing to do with commutativity. Reported once, that is an accusation of
    // breaking a merge law, aimed at the wrong law and at a contract that is fine.
    //
    // Re-running costs one extra execution and only on the path that was about to
    // accuse someone. A defect that is actually algebraic reproduces every time; a
    // straddled deadline does not.
    if first.is_violation() {
        let second = match run(oracle, case) {
            Ok(outcome) => outcome,
            Err(reason) => return PropertyOutcome::Inconclusive(reason),
        };
        return match (&first, &second) {
            (PropertyOutcome::Violated(a), PropertyOutcome::Violated(b))
                if a.property == b.property && reproduced_identically(a, b) =>
            {
                second
            }
            // Disagreed with itself. The contract is nondeterministic, which is its
            // own defect with its own property, and is emphatically not proof of the
            // law this case was checking. Say so under the right name if we can:
            // reporting nothing at all would turn a real defect into a silent miss.
            _ => escalate_to_determinism(oracle, case),
        };
    }

    first
}

/// A check that failed once and not again means something outside the inputs moved.
/// Try to name that directly instead of just declining to answer.
///
/// Without this, a contract that varies its merge output between identical calls
/// escapes entirely: the law it appeared to break does not reproduce, and the
/// determinism check that would name it correctly repeats back-to-back and can miss
/// a coarse-grained clock. Reporting `UpdateDeterminism` when it does fire turns a
/// silent miss into a finding under the right property.
fn escalate_to_determinism<O: ConformanceOracle + ?Sized>(
    oracle: &mut O,
    case: &ConformanceCase,
) -> PropertyOutcome {
    if case.states.len() < ConformanceProperty::UpdateDeterminism.state_arity() {
        return PropertyOutcome::Inconclusive(Inconclusive::NotReproducible);
    }
    let determinism = ConformanceCase {
        property: ConformanceProperty::UpdateDeterminism,
        states: case.states.clone(),
        deltas: Vec::new(),
        summary: None,
        related: case.related.clone(),
    };
    match run(oracle, &determinism) {
        Ok(outcome @ PropertyOutcome::Violated(_)) => outcome,
        _ => PropertyOutcome::Inconclusive(Inconclusive::NotReproducible),
    }
}

/// Did the second run reproduce the *same* failure, or merely another failure?
///
/// Matching on the property alone is not enough, and getting this wrong makes the
/// whole re-run pointless for the case it exists to catch: a clock-dependent merge
/// reports `StateCommutativity` on every run, with different bytes each time, so a
/// property-only comparison waves it through as reproducible. The outputs have to
/// match too.
///
/// The determinism properties are the deliberate exception. For those, outputs
/// differing between runs *is* the defect being reported, so demanding that the
/// digests agree would suppress exactly the finding the check exists to make.
fn reproduced_identically(first: &Violation, second: &Violation) -> bool {
    match first.property {
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
        | ConformanceProperty::TransitionPathAgreement => {
            first.left == second.left && first.right == second.right
        }
    }
}

fn run<O: ConformanceOracle + ?Sized>(
    oracle: &mut O,
    case: &ConformanceCase,
) -> Result<PropertyOutcome, Inconclusive> {
    case.arity_ok()?;

    // Every property below is only meaningful over states the contract itself
    // considers valid. Checking laws against states a contract would never have
    // accepted is how you manufacture false positives.
    for state in &case.states {
        require_valid(oracle, state, &case.related)?;
    }

    match case.property {
        ConformanceProperty::StateIdempotence => {
            // Iterate to a fixpoint rather than demanding `merge(A, A) == A` on the
            // first try.
            //
            // A correct CANONICALIZING contract fails a single-apply check for a
            // reason that is not a defect: the PUT install path stores the client's
            // raw bytes without ever running `update_state`, so the state a peer
            // holds may not be canonical yet, and the first merge rewrites it into
            // canonical form. That first change is real. What matters is whether it
            // then STABILIZES. The in-tree probe already learned this the hard way
            // and iterates for exactly this reason — see the rustdoc on
            // `executor_impl::probe_identical_input_idempotency`, which says
            // flagging on the first change alone "would false-flag every such
            // contract".
            //
            // So a violation here means the state never settles: every re-apply
            // changes it again, which is genuine non-idempotence and does diverge
            // under at-least-once delivery.
            let a = &case.states[0];
            let mut current = a.to_vec();
            for _ in 0..MAX_CANONICALIZATION_APPLIES {
                let merged = merge(oracle, &current, &current)?;
                if merged == current {
                    return Ok(PropertyOutcome::Holds);
                }
                current = merged;
            }
            Ok(violation(
                case.property,
                &current,
                a,
                "merge(A, A) never reached a fixpoint: the state changes on every \
                 re-apply, so redelivery of the same state keeps mutating it",
            ))
        }

        ConformanceProperty::StateCommutativity => {
            let (a, b) = (&case.states[0], &case.states[1]);
            let ab = merge(oracle, a, b)?;
            let ba = merge(oracle, b, a)?;
            Ok(compare(
                case.property,
                &ab,
                &ba,
                "merge(A, B) must equal merge(B, A)",
            ))
        }

        ConformanceProperty::StateAssociativity => {
            let (a, b, c) = (&case.states[0], &case.states[1], &case.states[2]);
            let ab = merge(oracle, a, b)?;
            let bc = merge(oracle, b, c)?;
            // The intermediates get the same validity precondition as the inputs.
            // This function's own rule is that checking laws against states a
            // contract would never accept manufactures false positives, and an
            // intermediate the contract rejects is exactly such a state: the real
            // network drops it, so no peer ever merges on top of it.
            require_valid(oracle, &ab, &case.related)?;
            require_valid(oracle, &bc, &case.related)?;
            let ab_c = merge(oracle, &ab, c)?;
            let a_bc = merge(oracle, a, &bc)?;
            Ok(compare(
                case.property,
                &ab_c,
                &a_bc,
                "merge(merge(A, B), C) must equal merge(A, merge(B, C))",
            ))
        }

        ConformanceProperty::EmittedStateValidity => {
            // Merging two valid states must emit a state the contract still accepts.
            // A contract that produces states it would reject cannot converge: the
            // next peer to receive that state drops it.
            //
            // Two distinct states rather than a self-merge, because a self-merge is
            // already covered by StateIdempotence (if merge(A,A) == A and A is valid,
            // the result is trivially valid). The interesting failures happen when
            // combining different states produces something that breaks the
            // contract's own invariant, e.g. a union that exceeds a size cap.
            let (a, b) = (&case.states[0], &case.states[1]);
            let merged = merge(oracle, a, b)?;
            match oracle
                .validate_state(&merged, &case.related)
                .map_err(inconclusive_from)?
            {
                ValidateResult::Valid => Ok(PropertyOutcome::Holds),
                ValidateResult::RequestRelated(_) => Err(Inconclusive::RelatedRequired),
                ValidateResult::Invalid => Ok(PropertyOutcome::Violated(Violation {
                    property: case.property,
                    severity: case.property.severity(),
                    left: OutputDigest::of(&merged),
                    right: OutputDigest::of(a),
                    detail: "update_state emitted a state the contract rejects as invalid"
                        .to_string(),
                })),
            }
        }

        ConformanceProperty::UpdateDeterminism => {
            let (a, b) = (&case.states[0], &case.states[1]);
            let first = merge(oracle, a, b)?;
            for _ in 1..DETERMINISM_REPEATS {
                oracle.reset_instance();
                let again = merge(oracle, a, b)?;
                if again != first {
                    return Ok(PropertyOutcome::Violated(Violation {
                        property: case.property,
                        severity: case.property.severity(),
                        left: OutputDigest::of(&first),
                        right: OutputDigest::of(&again),
                        detail: "merge(A, B) returned different bytes on repeated identical calls"
                            .to_string(),
                    }));
                }
            }
            Ok(PropertyOutcome::Holds)
        }

        ConformanceProperty::SummaryDeterminism => {
            let a = &case.states[0];
            let first = oracle.summarize_state(a).map_err(inconclusive_from)?;
            for _ in 1..DETERMINISM_REPEATS {
                oracle.reset_instance();
                let again = oracle.summarize_state(a).map_err(inconclusive_from)?;
                if again != first {
                    return Ok(PropertyOutcome::Violated(Violation {
                        property: case.property,
                        severity: case.property.severity(),
                        left: OutputDigest::of(&first),
                        right: OutputDigest::of(&again),
                        detail: "summarize_state returned different bytes for the same state"
                            .to_string(),
                    }));
                }
            }
            Ok(PropertyOutcome::Holds)
        }

        ConformanceProperty::DeltaDeterminism => {
            let a = &case.states[0];
            let summary = match &case.summary {
                Some(s) => s.to_vec(),
                None => oracle.summarize_state(a).map_err(inconclusive_from)?,
            };
            let first = oracle
                .get_state_delta(a, &summary)
                .map_err(inconclusive_from)?;
            for _ in 1..DETERMINISM_REPEATS {
                oracle.reset_instance();
                let again = oracle
                    .get_state_delta(a, &summary)
                    .map_err(inconclusive_from)?;
                if again != first {
                    return Ok(PropertyOutcome::Violated(Violation {
                        property: case.property,
                        severity: case.property.severity(),
                        left: OutputDigest::of(&first),
                        right: OutputDigest::of(&again),
                        detail: "get_state_delta returned different bytes for the same inputs"
                            .to_string(),
                    }));
                }
            }
            Ok(PropertyOutcome::Holds)
        }

        ConformanceProperty::DeltaIdempotence => {
            let a = &case.states[0];
            let d = &case.deltas[0];
            let once = apply_delta(oracle, a, d)?;
            let twice = apply_delta(oracle, &once, d)?;
            Ok(compare(
                case.property,
                &once,
                &twice,
                "applying the same delta twice must equal applying it once",
            ))
        }

        ConformanceProperty::DeltaPermutationInvariance => {
            let a = &case.states[0];
            let (d1, d2) = (&case.deltas[0], &case.deltas[1]);
            // Ordering only. Re-delivery is deliberately NOT checked here.
            //
            // This property is `Severity::Violation`; delta idempotence is contested
            // and therefore `Severity::Diagnostic`. Re-applying D1 a third time here
            // would be a delta-idempotence check wearing an ordering check's name and
            // severity, so downgrading `DeltaIdempotence` would not actually spare a
            // counter-style contract — it would still be accused, under a property
            // whose description says nothing about re-delivery. One law per property.
            let a_then_d1 = apply_delta(oracle, a, d1)?;
            let forward = apply_delta(oracle, &a_then_d1, d2)?;
            let a_then_d2 = apply_delta(oracle, a, d2)?;
            let reverse = apply_delta(oracle, &a_then_d2, d1)?;
            Ok(compare(
                case.property,
                &forward,
                &reverse,
                "applying independent deltas in a different order reached a different state",
            ))
        }

        ConformanceProperty::SelfDeltaEmpty => {
            let a = &case.states[0];
            let summary = oracle.summarize_state(a).map_err(inconclusive_from)?;
            let delta = oracle
                .get_state_delta(a, &summary)
                .map_err(inconclusive_from)?;
            if delta.is_empty() {
                Ok(PropertyOutcome::Holds)
            } else {
                Ok(PropertyOutcome::Violated(Violation {
                    property: case.property,
                    severity: case.property.severity(),
                    left: OutputDigest::of(&delta),
                    right: OutputDigest::of(&[]),
                    detail: format!(
                        "delta against an exact summary of the same state is {} bytes, not empty",
                        delta.len()
                    ),
                }))
            }
        }

        ConformanceProperty::WholeStateSelfDelta => {
            // An empty state has nothing to save, so `delta.len() < a.len()` would be
            // `0 < 0` and report a "self-delta is 0 bytes against a 0 byte state"
            // diagnostic on every contract with an empty initial state. Noise, and
            // noise in a diagnostic is what makes people stop reading diagnostics.
            if case.states[0].is_empty() {
                return Ok(PropertyOutcome::Holds);
            }
            let a = &case.states[0];
            let summary = oracle.summarize_state(a).map_err(inconclusive_from)?;
            let delta = oracle
                .get_state_delta(a, &summary)
                .map_err(inconclusive_from)?;
            if delta.len() < a.len() {
                Ok(PropertyOutcome::Holds)
            } else {
                Ok(PropertyOutcome::Violated(Violation {
                    property: case.property,
                    severity: case.property.severity(),
                    left: OutputDigest::of(&delta),
                    right: OutputDigest::of(a),
                    detail: format!(
                        "self-delta is {} bytes against a {} byte state: synchronization saves nothing",
                        delta.len(),
                        a.len()
                    ),
                }))
            }
        }

        ConformanceProperty::ReconciliationCycle => {
            reconciliation_cycle(oracle, &case.states[0], &case.states[1], &case.related)
        }

        ConformanceProperty::PathAgreement => {
            // Both directions, because which state is "base" and which is "other" is
            // an artifact of the order the corpus happened to be listed in, not of
            // anything the contract does. The generator emits each unordered pair
            // once (`i < j`), so checking one direction would make catching a defect
            // depend on file order — the shape of a test that passes for the wrong
            // reason.
            let (a, b) = (&case.states[0], &case.states[1]);
            let forward = path_agreement(oracle, a, b, &case.related);
            if matches!(forward, Ok(PropertyOutcome::Violated(_))) {
                return forward;
            }
            let reverse = path_agreement(oracle, b, a, &case.related);
            if matches!(reverse, Ok(PropertyOutcome::Violated(_))) {
                return reverse;
            }
            // Neither direction found anything. One usable verdict is a verdict;
            // only refuse if neither direction could be evaluated at all.
            match (forward, reverse) {
                (Ok(outcome), _) => Ok(outcome),
                (Err(_), Ok(outcome)) => Ok(outcome),
                (Err(reason), Err(_)) => Err(reason),
            }
        }

        ConformanceProperty::TransitionPathAgreement => {
            // `states[0]` is the base a peer held, `states[1]` the result it
            // actually reached. The generator only ever builds this case from a
            // recorded transition, which is what makes the order meaningful: for an
            // arbitrary pair, "merging B into A yields B" is last-write-wins.
            let (base, result) = (&case.states[0], &case.states[1]);

            // Drive `result` to a fixpoint of its own merge before comparing.
            //
            // A canonicalizing contract legitimately rewrites a stored state the
            // first time it is merged — the PUT install path stores the client's raw
            // bytes without ever running `update_state` — so the state a peer holds
            // may not be canonical yet. Comparing against the raw bytes would report
            // that rewrite as a merge-law break. `StateIdempotence` iterates for the
            // same reason and to the same budget.
            let mut settled = result.to_vec();
            let mut reached_fixpoint = false;
            for _ in 0..MAX_CANONICALIZATION_APPLIES {
                let again = merge(oracle, &settled, &settled)?;
                if again == settled {
                    reached_fixpoint = true;
                    break;
                }
                settled = again;
            }
            if !reached_fixpoint {
                // A state that rewrites itself on every re-apply cannot be asked
                // whether some OTHER state absorbs into it, and the defect already
                // has a name. Accusing it here would name the wrong law.
                return Err(Inconclusive::StateNotSettled);
            }
            require_valid(oracle, &settled, &case.related)?;

            let merged = merge(oracle, base, &settled)?;
            // Both sides of the comparison get the same validity precondition, for
            // the same reason `path_agreement` validates both of its outputs: a
            // state the contract itself rejects never reaches another peer, so
            // reasoning about it is reasoning about a history that cannot happen.
            //
            // Without this, a merge that EMITS an invalid state is reported under
            // this property rather than under `EmittedStateValidity`, which is the
            // law that actually names that defect. One law per property is the rule
            // this module follows everywhere else.
            require_valid(oracle, &merged, &case.related)?;
            Ok(compare(
                case.property,
                &merged,
                &settled,
                "merging the settled form of a state a peer actually REACHED back \
                 into the state it came from did not reproduce that settled form, so \
                 the merge path cannot reach what the update path reached: every \
                 peer that receives this state merges it into something else, and \
                 the two can never agree",
            ))
        }
    }
}

/// One direction of [`ConformanceProperty::PathAgreement`]: does `base` reach the
/// same state whether it receives `other`'s information as a delta or whole?
///
/// The two calls mirror the two things production actually does with the same
/// update. `get_state_delta(other, summarize(base))` is the delta the SENDER
/// computes against the RECIPIENT's summary — the same orientation
/// [`reconciliation_cycle`] uses, and getting it backwards would be checking a call
/// the network never makes.
fn path_agreement<O: ConformanceOracle + ?Sized>(
    oracle: &mut O,
    base: &Bytes,
    other: &Bytes,
    related: &RelatedContracts<'static>,
) -> Result<PropertyOutcome, Inconclusive> {
    let base_summary = oracle.summarize_state(base).map_err(inconclusive_from)?;
    let delta = oracle
        .get_state_delta(other, &base_summary)
        .map_err(inconclusive_from)?;

    // No delta path exists to compare against, in either of the two ways production
    // can decline to take one.
    //
    // An empty delta is the protocol's "nothing to send", and a summary too coarse
    // to express the divergence produces one legitimately — the same contract shape
    // `reconciliation_cycle` models the full-state fallback for. An oversized delta
    // is refused by production's own gate and replaced with a whole-state send, so
    // the delta path is not the path this update would take. Checking either one
    // would be checking a call that never happens.
    if delta.is_empty() || delta_would_be_refused(&delta, other) {
        return Err(Inconclusive::NoDeltaPath);
    }

    let delta_path = apply_delta_bytes(oracle, base, &delta)?;
    let merge_path = merge(oracle, base, other)?;
    if delta_path == merge_path {
        return Ok(PropertyOutcome::Holds);
    }

    // Same validity precondition the inputs get, for the same reason: a state the
    // contract rejects never reaches another peer, so reasoning about it is
    // reasoning about a history that cannot happen.
    require_valid(oracle, &delta_path, related)?;
    require_valid(oracle, &merge_path, related)?;

    // Distinguish "the delta carried less than the whole state" from "the two paths
    // computed different things". This is the guard that keeps this property off a
    // large and legitimate class of contract, and without it the check is a
    // false-positive generator.
    //
    // A contract whose summary cannot express a particular divergence ships a delta
    // carrying only PART of what `other` holds. Its delta path lands short of the
    // merged state on this round and catches up on the next one — a weak delta
    // encoding, not a broken merge, and this module already declines to accuse it
    // under `ReconciliationCycle` for exactly this reason.
    //
    // Merging the whole state on top separates the two. For any merge that is a
    // genuine join, a delta derived from `other` can only move `base` somewhere
    // between `base` and `base ⊔ other`, so joining `other` back on top lands on
    // `base ⊔ other` either way and a partial delta heals. A delta path that
    // computed something the merge path cannot reach does not heal.
    //
    // The guard errs toward silence in every direction, which is the right direction
    // for a removal-eligible property. That includes when the merge itself is
    // unsound: a last-write-wins merge heals trivially, so this declines to pile a
    // second accusation onto a defect `StateCommutativity` already names.
    let healed = merge(oracle, &delta_path, other)?;
    if healed == merge_path {
        return Ok(PropertyOutcome::Holds);
    }

    Ok(compare(
        ConformanceProperty::PathAgreement,
        &delta_path,
        &merge_path,
        "the delta path and the merge path disagree: applying the delta the network \
         would have sent reached a different state from merging the sender's whole \
         state, and re-merging the whole state does not repair the difference, so a \
         peer that received this update as a delta can never agree with one that \
         received it as a state",
    ))
}

/// Simulate two peers exchanging summaries and deltas until they agree.
///
/// Both deltas in a round are computed against the pre-round states, because that is
/// what actually happens on the wire: the peers do not take turns. Convergence ends
/// the check; an exactly-repeated state pair is a proven cycle; running out of
/// rounds while still moving is inconclusive.
///
/// # Why a repeated pair is proof, and the two assumptions it rests on
///
/// The simulation is deterministic and carries no state between rounds, so the next
/// round is a pure function of the current `(left, right)` pair. Revisiting a pair
/// therefore means the process has entered a loop it can never leave, which is why
/// this is reported as a violation rather than as "slow". Legitimate multi-round
/// convergence never trips it: each round moves to a pair not seen before, and
/// running out of rounds is [`Inconclusive::RoundLimit`].
///
/// That argument depends on two things, and both are worth stating because a finding
/// from this check is the most model-dependent one this module produces:
///
/// 1. **The contract is deterministic.** If `summarize_state` or `get_state_delta`
///    varies between identical calls — a contract stamping the host clock into its
///    summary, say — then a repeated pair no longer implies a loop, and the finding
///    would be real but mislabelled: the defect is the nondeterminism, which
///    [`ConformanceProperty::SummaryDeterminism`] names directly. Such a contract
///    genuinely cannot converge either way, so this never accuses a *correct*
///    contract, but the property named may be the wrong one.
/// 2. **This exchange resembles the protocol's.** Peers are modelled as exchanging
///    simultaneously, each computing its delta against the other's pre-round summary.
///    A protocol that reconciles by some other schedule could in principle converge
///    where this model loops. Shadow mode is where that assumption gets tested
///    against real traffic, and it is a reason to treat early findings from this
///    check with more suspicion than the direct algebraic ones.
fn reconciliation_cycle<O: ConformanceOracle + ?Sized>(
    oracle: &mut O,
    a: &Bytes,
    b: &Bytes,
    related: &RelatedContracts<'static>,
) -> Result<PropertyOutcome, Inconclusive> {
    let mut left = a.to_vec();
    let mut right = b.to_vec();
    let mut seen: HashSet<([u8; 32], [u8; 32])> = HashSet::new();
    seen.insert((digest(&left), digest(&right)));

    for _ in 0..MAX_RECONCILIATION_ROUNDS {
        if left == right {
            return Ok(PropertyOutcome::Holds);
        }

        let left_summary = oracle.summarize_state(&left).map_err(inconclusive_from)?;
        let right_summary = oracle.summarize_state(&right).map_err(inconclusive_from)?;
        let to_left = oracle
            .get_state_delta(&right, &left_summary)
            .map_err(inconclusive_from)?;
        let to_right = oracle
            .get_state_delta(&left, &right_summary)
            .map_err(inconclusive_from)?;

        // Mirror the production size gate, per direction and before applying.
        //
        // `ring::interest::gate_delta_size` refuses a delta that is not meaningfully
        // smaller than the state and sends the whole state instead, so a contract
        // whose delta encoding is oversized never has that delta applied on the
        // network at all. Simulating the application anyway can walk the pair into a
        // repeat — for instance a delta that swaps the two peers' states — and
        // produce an enforceable cycle finding against a contract whose merge is
        // sound and which converges in production.
        // Gate against the SENDER's state in each direction: `to_left` was computed
        // from `right`, so `right` is what production would compare it against.
        let mut next_left = if delta_would_be_refused(&to_left, &right) {
            merge(oracle, &left, &right)?
        } else {
            apply_delta_bytes(oracle, &left, &to_left)?
        };
        let mut next_right = if delta_would_be_refused(&to_right, &left) {
            merge(oracle, &right, &left)?
        } else {
            apply_delta_bytes(oracle, &right, &to_right)?
        };

        // Model the protocol's full-state fallback.
        //
        // Without this the check accuses correct contracts. A summary that cannot
        // express a particular divergence — a coarse version clock under concurrent
        // writes, a compact or probabilistic digest — yields an empty delta in both
        // directions, the pair repeats unchanged, and a delta-only simulation calls
        // that a proven cycle on the very first round. The real protocol does not
        // give up there: when the delta path cannot carry the difference it sends the
        // whole state instead (`ring::interest::gate_delta_size` refuses a
        // state-sized delta and hands back a full-state send). A full-state send is
        // just `update_state(local, [State(remote)])`.
        //
        // Modelling it is what separates the two cases that otherwise look identical
        // here: a contract with a weak delta path converges once the states are
        // exchanged whole, while genuine mutual rejection (#5153) still refuses to
        // move, because refusing is what it does with any input.
        if next_left == left && next_right == right {
            next_left = merge(oracle, &left, &right)?;
            next_right = merge(oracle, &right, &left)?;
        }

        // A state the contract would reject never reaches another peer: the real
        // network drops it. Continuing to iterate on one would be reasoning about a
        // history that cannot happen.
        require_valid(oracle, &next_left, related)?;
        require_valid(oracle, &next_right, related)?;

        left = next_left;
        right = next_right;

        if left == right {
            return Ok(PropertyOutcome::Holds);
        }
        if !seen.insert((digest(&left), digest(&right))) {
            return Ok(PropertyOutcome::Violated(Violation {
                property: ConformanceProperty::ReconciliationCycle,
                severity: ConformanceProperty::ReconciliationCycle.severity(),
                left: OutputDigest::of(&left),
                right: OutputDigest::of(&right),
                detail: "two valid states revisited an exact state pair while still divergent: \
                     reconciliation cannot converge"
                    .to_string(),
            }));
        }
    }

    Err(Inconclusive::RoundLimit)
}

fn digest(bytes: &[u8]) -> [u8; 32] {
    *blake3::hash(bytes).as_bytes()
}

/// Would the network refuse to send this delta and ship the whole state instead?
///
/// Deliberately conservative relative to `ring::interest::gate_delta_size`: a delta
/// at least as large as the state it would replace carries no saving, and the
/// production path treats that case as a full-state send. Erring toward the fallback
/// only ever makes the simulation converge more readily, which is the safe direction
/// for a check whose failure mode is accusing a correct contract.
/// Whether production would refuse this delta and send the whole state instead.
///
/// `sender_state` is the state of the peer COMPUTING the delta, which is what
/// production compares against (`gate_delta_size`'s `our_state_size`) — not the
/// recipient's. Getting that backwards is not a rounding error: with asymmetric
/// state sizes it applies a delta the network would have replaced with a full state,
/// which can walk the pair into a repeat and manufacture an enforceable
/// reconciliation-cycle finding against a contract that converges in production. It
/// can also do the reverse and hide a real one.
///
/// The margin is production's, imported rather than approximated, for the same
/// reason.
fn delta_would_be_refused(delta: &[u8], sender_state: &[u8]) -> bool {
    !delta.is_empty()
        && delta.len()
            >= sender_state
                .len()
                .saturating_add(crate::ring::interest::MIN_FULL_STATE_SAVING_BYTES)
}

fn require_valid<O: ConformanceOracle + ?Sized>(
    oracle: &mut O,
    state: &[u8],
    related: &RelatedContracts<'static>,
) -> Result<(), Inconclusive> {
    match oracle
        .validate_state(state, related)
        .map_err(inconclusive_from)?
    {
        ValidateResult::Valid => Ok(()),
        ValidateResult::Invalid => Err(Inconclusive::InputNotValid),
        ValidateResult::RequestRelated(_) => Err(Inconclusive::RelatedRequired),
    }
}

/// `merge(base, other)` — apply `other` to `base` as a full-state update.
///
/// There is no separate `merge` entry point in the contract interface; a full-state
/// update *is* the merge, which is why a contract that "rejects" an incoming state by
/// returning its own state unchanged shows up as an ordinary commutativity failure.
fn merge<O: ConformanceOracle + ?Sized>(
    oracle: &mut O,
    base: &[u8],
    other: &[u8],
) -> Result<Vec<u8>, Inconclusive> {
    let update = UpdateData::State(State::from(other.to_vec()));
    apply_updates(oracle, base, &[update])
}

fn apply_delta<O: ConformanceOracle + ?Sized>(
    oracle: &mut O,
    base: &[u8],
    delta: &[u8],
) -> Result<Vec<u8>, Inconclusive> {
    apply_delta_bytes(oracle, base, delta)
}

fn apply_delta_bytes<O: ConformanceOracle + ?Sized>(
    oracle: &mut O,
    base: &[u8],
    delta: &[u8],
) -> Result<Vec<u8>, Inconclusive> {
    if delta.is_empty() {
        // An empty delta is the protocol's "nothing to send". Handing it to the
        // contract would test the contract's tolerance of a no-op, not a merge law.
        return Ok(base.to_vec());
    }
    let update = UpdateData::Delta(StateDelta::from(delta.to_vec()));
    apply_updates(oracle, base, &[update])
}

fn apply_updates<O: ConformanceOracle + ?Sized>(
    oracle: &mut O,
    base: &[u8],
    updates: &[UpdateData<'_>],
) -> Result<Vec<u8>, Inconclusive> {
    let modification = oracle
        .update_state(base, updates)
        .map_err(inconclusive_from)?;
    match modification.new_state {
        Some(state) => Ok(state.into_bytes()),
        None if modification.requires_dependencies() => Err(Inconclusive::RelatedRequired),
        // No state and no related-contract request. `UpdateModification` is
        // `#[non_exhaustive]` with only `valid()` and `requires()` as constructors,
        // so safe Rust cannot build this shape and no unit test here constructs it.
        // It is still reachable in production: the runtime deserializes whatever the
        // guest emitted, and a malformed or hostile contract can serialize exactly
        // this. Inconclusive rather than a violation, because it says the contract
        // answered nothing, not that a law was broken.
        None => Err(Inconclusive::NoOutputState),
    }
}

fn inconclusive_from(err: OracleError) -> Inconclusive {
    match err.kind {
        OracleErrorKind::Resource => Inconclusive::ResourceLimit(err.message),
        // A contract rejection and a host/WASM failure are both non-enforceable —
        // neither is a merge-law proof, and treating a trap as a violation would
        // mean removing a contract for a bug in the runtime executing it. That
        // severity call is shared and stays shared. The LABEL is not: a trap in the
        // host or the WASM module is not the contract's fault, and folding it into
        // `ContractError` names the wrong culprit and hides the one signal that
        // would tell us the harness itself has a bug (#5509).
        OracleErrorKind::Contract => Inconclusive::ContractError(err.message),
        OracleErrorKind::Runtime => Inconclusive::RuntimeError(err.message),
    }
}

fn compare(
    property: ConformanceProperty,
    left: &[u8],
    right: &[u8],
    detail: &str,
) -> PropertyOutcome {
    if left == right {
        return PropertyOutcome::Holds;
    }
    // Distinguish a reordering from a genuinely different result.
    //
    // Two outputs holding the same bytes in a different order are the signature of
    // non-canonical serialization — a `HashMap` iterated in insertion order, most
    // often — rather than of a merge that computed something different. The
    // distinction matters because the two call for opposite fixes: make the encoding
    // canonical, versus fix the merge. It is also the shape of the #4295
    // false-positive class, which is why the executor's in-tree probe compares byte
    // MULTISETS (`byte_multiset_eq`) rather than exact bytes.
    //
    // This still reports a violation. A reordering is a real problem for this
    // network: peers compare state by hash, so two peers holding the same logical
    // state in different byte order never recognise each other as converged. But
    // saying WHICH of the two it is costs one sort of an already-materialised buffer
    // on a path that is about to report a finding anyway, and an author told
    // "your merge is not commutative" when the truth is "your encoding is not
    // canonical" will look in the wrong place.
    let detail = if is_reordering(left, right) {
        &format!(
            "{detail}. NOTE: both results hold exactly the same bytes in a different \
             order, so the merge agreed on content and the ENCODING is not canonical \
             (e.g. a HashMap serialized in iteration order). Peers compare state by \
             hash, so this still prevents convergence, but the fix is a deterministic \
             encoding rather than a change to the merge"
        )
    } else {
        detail
    };
    violation(property, left, right, detail)
}

/// Whether two differing outputs are permutations of each other.
///
/// Sorting copies is affordable here because this runs only on the path that has
/// already decided to report a finding, never on the common passing path.
fn is_reordering(left: &[u8], right: &[u8]) -> bool {
    if left.len() != right.len() {
        return false;
    }
    let mut a = left.to_vec();
    let mut b = right.to_vec();
    a.sort_unstable();
    b.sort_unstable();
    a == b
}

fn violation(
    property: ConformanceProperty,
    left: &[u8],
    right: &[u8],
    detail: &str,
) -> PropertyOutcome {
    PropertyOutcome::Violated(Violation {
        property,
        severity: property.severity(),
        left: OutputDigest::of(left),
        right: OutputDigest::of(right),
        detail: detail.to_string(),
    })
}
