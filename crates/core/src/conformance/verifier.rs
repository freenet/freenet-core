//! Executes one conformance property against one contract.
//!
//! Everything here is deterministic given the oracle: the same case run twice on the
//! same contract produces the same outcome. That is what lets a peer ship a case to
//! another peer and have it reach the same conclusion independently, and it is what
//! lets `fdev` reproduce a network finding offline.

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
    match run(oracle, case) {
        Ok(outcome) => outcome,
        Err(reason) => PropertyOutcome::Inconclusive(reason),
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
            let a = &case.states[0];
            let merged = merge(oracle, a, a)?;
            Ok(compare(
                case.property,
                &merged,
                a,
                "merge(A, A) must equal A",
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
            let ab_c = merge(oracle, &ab, c)?;
            let bc = merge(oracle, b, c)?;
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
            let a_then_d1 = apply_delta(oracle, a, d1)?;
            let forward = apply_delta(oracle, &a_then_d1, d2)?;
            let a_then_d2 = apply_delta(oracle, a, d2)?;
            let reverse = apply_delta(oracle, &a_then_d2, d1)?;
            if forward != reverse {
                return Ok(violation(
                    case.property,
                    &forward,
                    &reverse,
                    "applying independent deltas in a different order reached a different state",
                ));
            }
            // Duplicates are a separate failure mode from ordering: at-least-once
            // delivery means a peer may legitimately see D1 twice around D2.
            let duplicated = apply_delta(oracle, &forward, d1)?;
            Ok(compare(
                case.property,
                &forward,
                &duplicated,
                "re-delivering an already-applied delta changed the state",
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
            reconciliation_cycle(oracle, &case.states[0], &case.states[1])
        }
    }
}

/// Simulate two peers exchanging summaries and deltas until they agree.
///
/// Both deltas in a round are computed against the pre-round states, because that is
/// what actually happens on the wire: the peers do not take turns. Convergence ends
/// the check; an exactly-repeated state pair is a proven cycle; running out of
/// rounds while still moving is inconclusive.
fn reconciliation_cycle<O: ConformanceOracle + ?Sized>(
    oracle: &mut O,
    a: &Bytes,
    b: &Bytes,
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

        let next_left = apply_delta_bytes(oracle, &left, &to_left)?;
        let next_right = apply_delta_bytes(oracle, &right, &to_right)?;

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
        None => Err(Inconclusive::NoOutputState),
    }
}

fn inconclusive_from(err: OracleError) -> Inconclusive {
    match err.kind {
        OracleErrorKind::Resource => Inconclusive::ResourceLimit(err.message),
        // A host or WASM failure is reported alongside a contract rejection because
        // neither is a merge-law proof, and treating a trap as a violation would
        // mean removing a contract for a bug in the runtime executing it.
        OracleErrorKind::Contract | OracleErrorKind::Runtime => {
            Inconclusive::ContractError(err.message)
        }
    }
}

fn compare(
    property: ConformanceProperty,
    left: &[u8],
    right: &[u8],
    detail: &str,
) -> PropertyOutcome {
    if left == right {
        PropertyOutcome::Holds
    } else {
        violation(property, left, right, detail)
    }
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
