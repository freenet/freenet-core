//! The conformance laws, and the shape of an answer about one of them.

use serde::{Deserialize, Serialize};

/// A single checkable law about a contract's algebra.
///
/// `merge(A, B)` is not a distinct contract entry point: it is
/// `update_state(A, [UpdateData::State(B)])`. A contract that "rejects" B returns A
/// unchanged, which is why mutual rejection (`merge(A,B) == A`, `merge(B,A) == B`)
/// shows up here as an ordinary commutativity failure rather than needing a rule of
/// its own.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[non_exhaustive]
pub enum ConformanceProperty {
    /// `merge(A, A) == A`
    StateIdempotence,
    /// `merge(A, B) == merge(B, A)`
    StateCommutativity,
    /// `merge(merge(A, B), C) == merge(A, merge(B, C))`
    StateAssociativity,
    /// Every state emitted by `update_state` is itself valid.
    EmittedStateValidity,
    /// `update_state` on identical inputs yields identical bytes.
    UpdateDeterminism,
    /// `summarize_state` on identical inputs yields identical bytes (#4857 class).
    SummaryDeterminism,
    /// `get_state_delta` on identical inputs yields identical bytes.
    DeltaDeterminism,
    /// `apply(apply(A, D), D) == apply(A, D)` — at-least-once delivery must be harmless.
    ///
    /// **This property is contested, and the disagreement is not resolved here.**
    /// The existing sampled probe in
    /// `contract::executor::runtime::executor_impl::maybe_probe_idempotency`
    /// deliberately exempts delta inputs, on the stated grounds that
    /// operation-based CRDTs (counters, append-logs) legitimately break byte
    /// equality on re-apply: an "increment by 1" delta yields `S+1` then `S+2`.
    /// The RFC takes the opposite position, and explicitly requires delta
    /// idempotence, because Freenet delivery is at-least-once — under which a
    /// contract that double-counts a redelivered increment is not a valid CmRDT,
    /// it is a contract that will silently corrupt its own state.
    ///
    /// The RFC is the newer and more specific document, so the check exists. But
    /// whether any deployed first-party contract actually relies on non-idempotent
    /// deltas is an empirical question that cannot be settled by reading code, and
    /// it must be answered by running `fdev conformance` against deployed WASM
    /// before this property is ever allowed to influence anything on the network.
    /// Until then it is a reporting signal for contract authors.
    DeltaIdempotence,
    /// Deltas applied in any order reach the same canonical state.
    ///
    /// The published `ContractInterface` requirement carries no independence
    /// qualifier — "the order in which these updates are applied should not affect
    /// the final state" — so order-dependence is a defect whether or not the deltas
    /// are causally related, and this checks exactly that.
    ///
    /// Worth knowing when reading a finding: the generator pairs deltas as observed,
    /// which means consecutive ones may be causally sequenced. So a finding here can
    /// also be read as "this delta encoding carries sequence", which is the same
    /// defect seen from the other side rather than a separate one.
    ///
    /// Re-delivery is deliberately not checked here; that is
    /// [`ConformanceProperty::DeltaIdempotence`], which is contested and reports at
    /// a lower severity. Folding it in would accuse a counter-style contract under
    /// this property's name and severity regardless of that.
    DeltaPermutationInvariance,
    /// A delta against an exact summary of the same state should be empty (#5072).
    SelfDeltaEmpty,
    /// A self-delta should not be as large as the state it is a delta against
    /// (#5072 / #5056).
    WholeStateSelfDelta,
    /// Two valid divergent states reconcile, rather than cycling forever (#5153).
    ReconciliationCycle,
}

/// How seriously a failed property should be taken.
///
/// The split matters because only [`Severity::Violation`] is ever allowed to become
/// removal evidence. The RFC is explicit that size alone is not a merge-law proof:
/// a contract whose self-delta is the whole state is wasteful, not unsound, and
/// deleting it would be deleting a working application over an efficiency
/// complaint.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum Severity {
    /// Breaks a merge law. Eligible (after independent verification) to justify removal.
    Violation,
    /// Wasteful or suspicious, but not a proof of non-convergence. Report only.
    Diagnostic,
}

impl ConformanceProperty {
    /// Every property, in a stable order. Used by the generator and by tests that
    /// assert no property is silently unhandled.
    pub const ALL: &'static [ConformanceProperty] = &[
        ConformanceProperty::StateIdempotence,
        ConformanceProperty::StateCommutativity,
        ConformanceProperty::StateAssociativity,
        ConformanceProperty::EmittedStateValidity,
        ConformanceProperty::UpdateDeterminism,
        ConformanceProperty::SummaryDeterminism,
        ConformanceProperty::DeltaDeterminism,
        ConformanceProperty::DeltaIdempotence,
        ConformanceProperty::DeltaPermutationInvariance,
        ConformanceProperty::SelfDeltaEmpty,
        ConformanceProperty::WholeStateSelfDelta,
        ConformanceProperty::ReconciliationCycle,
    ];

    pub fn severity(self) -> Severity {
        match self {
            ConformanceProperty::SelfDeltaEmpty | ConformanceProperty::WholeStateSelfDelta => {
                Severity::Diagnostic
            }
            // Diagnostic until the empirical question is settled, because that is
            // what this property's own documentation already promises. Saying "a
            // reporting signal, not allowed to influence anything on the network
            // until measured against deployed WASM" while handing it the same
            // enforcement weight as commutativity would make the comment a wish
            // rather than a rule — and it is exactly the kind of gap that closes
            // itself the day someone enables enforcement for the settled properties
            // and this one rides along unnoticed.
            ConformanceProperty::DeltaIdempotence => Severity::Diagnostic,
            ConformanceProperty::StateIdempotence
            | ConformanceProperty::StateCommutativity
            | ConformanceProperty::StateAssociativity
            | ConformanceProperty::EmittedStateValidity
            | ConformanceProperty::UpdateDeterminism
            | ConformanceProperty::SummaryDeterminism
            | ConformanceProperty::DeltaDeterminism
            | ConformanceProperty::DeltaPermutationInvariance
            | ConformanceProperty::ReconciliationCycle => Severity::Violation,
        }
    }

    /// How many distinct input states a case for this property must carry.
    pub fn state_arity(self) -> usize {
        match self {
            ConformanceProperty::StateIdempotence
            | ConformanceProperty::SummaryDeterminism
            | ConformanceProperty::DeltaDeterminism
            | ConformanceProperty::DeltaIdempotence
            | ConformanceProperty::DeltaPermutationInvariance
            | ConformanceProperty::SelfDeltaEmpty
            | ConformanceProperty::WholeStateSelfDelta => 1,
            ConformanceProperty::StateCommutativity
            | ConformanceProperty::EmittedStateValidity
            | ConformanceProperty::UpdateDeterminism
            | ConformanceProperty::ReconciliationCycle => 2,
            ConformanceProperty::StateAssociativity => 3,
        }
    }

    /// How many deltas a case for this property must carry.
    pub fn delta_arity(self) -> usize {
        match self {
            ConformanceProperty::DeltaIdempotence => 1,
            ConformanceProperty::DeltaPermutationInvariance => 2,
            ConformanceProperty::StateIdempotence
            | ConformanceProperty::StateCommutativity
            | ConformanceProperty::StateAssociativity
            | ConformanceProperty::EmittedStateValidity
            | ConformanceProperty::UpdateDeterminism
            | ConformanceProperty::SummaryDeterminism
            | ConformanceProperty::DeltaDeterminism
            | ConformanceProperty::SelfDeltaEmpty
            | ConformanceProperty::WholeStateSelfDelta
            | ConformanceProperty::ReconciliationCycle => 0,
        }
    }

    pub fn as_str(self) -> &'static str {
        match self {
            ConformanceProperty::StateIdempotence => "state_idempotence",
            ConformanceProperty::StateCommutativity => "state_commutativity",
            ConformanceProperty::StateAssociativity => "state_associativity",
            ConformanceProperty::EmittedStateValidity => "emitted_state_validity",
            ConformanceProperty::UpdateDeterminism => "update_determinism",
            ConformanceProperty::SummaryDeterminism => "summary_determinism",
            ConformanceProperty::DeltaDeterminism => "delta_determinism",
            ConformanceProperty::DeltaIdempotence => "delta_idempotence",
            ConformanceProperty::DeltaPermutationInvariance => "delta_permutation_invariance",
            ConformanceProperty::SelfDeltaEmpty => "self_delta_empty",
            ConformanceProperty::WholeStateSelfDelta => "whole_state_self_delta",
            ConformanceProperty::ReconciliationCycle => "reconciliation_cycle",
        }
    }
}

impl std::fmt::Display for ConformanceProperty {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

/// A hash and length standing in for a full output.
///
/// Evidence carries digests rather than the outputs themselves: recipients recompute
/// the outputs from the inputs anyway (that is the whole point of shipping evidence
/// instead of a verdict), so carrying megabytes of observed output would only inflate
/// the message and widen the DoS surface.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct OutputDigest {
    pub len: usize,
    pub hash: [u8; 32],
}

impl OutputDigest {
    pub fn of(bytes: &[u8]) -> Self {
        Self {
            len: bytes.len(),
            hash: *blake3::hash(bytes).as_bytes(),
        }
    }

    pub fn short_hash(&self) -> String {
        hex::encode(&self.hash[..6])
    }
}

impl std::fmt::Display for OutputDigest {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{} bytes, blake3:{}", self.len, self.short_hash())
    }
}

/// A reproduced failure: two executions that the law says must agree, and did not.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Violation {
    pub property: ConformanceProperty,
    pub severity: Severity,
    /// Digest of the left-hand side of the comparison (e.g. `merge(A, B)`).
    pub left: OutputDigest,
    /// Digest of the right-hand side (e.g. `merge(B, A)`).
    pub right: OutputDigest,
    /// Human-readable statement of what was compared. Diagnostics only; never parsed.
    pub detail: String,
}

impl std::fmt::Display for Violation {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{}: {} (left: {}; right: {})",
            self.property, self.detail, self.left, self.right
        )
    }
}

/// Why a check could not reach a verdict.
///
/// Every variant here is a *refusal to accuse*, and each one corresponds to a
/// legitimate contract behaviour that an earlier or naiver detector would have
/// misread as a defect.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[non_exhaustive]
pub enum Inconclusive {
    /// The contract does not consider an input state valid, so the laws say nothing
    /// about it. Feeding a contract states it never would have accepted proves nothing.
    InputNotValid,
    /// The contract needs a related contract's state to proceed. Not a defect: the
    /// River-style authorization chain does this routinely.
    RelatedRequired,
    /// The contract returned an error. A single rejection is not a conformance
    /// failure — contracts are supposed to reject updates they consider illegitimate.
    ContractError(String),
    /// `update_state` returned neither a new state nor a related-contract request.
    NoOutputState,
    /// Execution hit a fuel, memory or time limit, so we never saw the real answer.
    ResourceLimit(String),
    /// Reconciliation was still making progress when the round budget ran out.
    /// Legitimate multi-round convergence lives here.
    RoundLimit,
    /// The case was malformed for the property (wrong arity, missing delta).
    MalformedCase(String),
    /// The check failed once and then did not fail the same way again.
    ///
    /// Something outside the inputs moved between the two runs — the host clock is
    /// the realistic candidate. That is a defect of its own kind, but it is not
    /// evidence about the law this case was checking, and reporting it as such would
    /// name the wrong property and the wrong severity.
    NotReproducible,
}

impl std::fmt::Display for Inconclusive {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Inconclusive::InputNotValid => f.write_str("an input state is not valid"),
            Inconclusive::RelatedRequired => f.write_str("contract requires related state"),
            Inconclusive::ContractError(e) => write!(f, "contract error: {e}"),
            Inconclusive::NoOutputState => f.write_str("update produced no state"),
            Inconclusive::ResourceLimit(e) => write!(f, "resource limit: {e}"),
            Inconclusive::RoundLimit => f.write_str("reconciliation round budget exhausted"),
            Inconclusive::MalformedCase(e) => write!(f, "malformed case: {e}"),
            Inconclusive::NotReproducible => {
                f.write_str("the finding did not reproduce on a second run")
            }
        }
    }
}

/// The result of checking one property against one contract with one set of inputs.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum PropertyOutcome {
    /// The law held for these inputs. Says nothing about other inputs.
    Holds,
    /// The law was broken, reproducibly, by these inputs.
    Violated(Violation),
    /// No verdict. See [`Inconclusive`].
    Inconclusive(Inconclusive),
}

impl PropertyOutcome {
    pub fn is_violation(&self) -> bool {
        matches!(self, PropertyOutcome::Violated(_))
    }

    /// A violation severe enough to be eligible as removal evidence.
    ///
    /// Diagnostics are excluded here on purpose; see [`Severity`].
    pub fn is_enforceable_violation(&self) -> bool {
        // Derive the severity from the PROPERTY rather than reading the field the
        // `Violation` carries. `Violation` is `Deserialize` with public fields and
        // travels inside evidence, so the field is attacker-influenceable in any
        // future that feeds wire data here; the property is not. The two agree today
        // because `verify_case` fills the field from `property.severity()`, so this
        // costs nothing and removes the trust dependency rather than documenting it.
        matches!(self, PropertyOutcome::Violated(v) if v.property.severity() == Severity::Violation)
    }

    pub fn violation(&self) -> Option<&Violation> {
        match self {
            PropertyOutcome::Violated(v) => Some(v),
            PropertyOutcome::Holds | PropertyOutcome::Inconclusive(_) => None,
        }
    }
}
