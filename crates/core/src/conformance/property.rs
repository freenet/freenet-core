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
    /// it must be answered by running `fdev verify-merge` against deployed WASM
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
    /// The generator only pairs deltas observed against the SAME base state, which is
    /// what makes a finding here mean something. A delta is computed as
    /// `get_state_delta(sender_state, recipient_summary)`, so two deltas observed
    /// against different bases can be causally sequenced — the later one computed from
    /// a state that already contains the earlier one's effect — and permuting those
    /// asks what happens in a situation the protocol never produces. Same-base pairs
    /// are the genuine concurrent-independent-updates case. Deltas with no recorded
    /// base are not paired at all.
    ///
    /// So a finding here still reads as "this delta encoding carries sequence", and
    /// that is a real defect on this network rather than an artifact: delivery is
    /// out-of-order, so a delta that assumes a base will be applied out of order in
    /// production too.
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
    ///
    /// # Open decision: is this severity right?
    ///
    /// This is the only check here that is not pure algebra. It rests on a MODEL of
    /// how the protocol reconciles — which side sends what, when the full-state
    /// fallback triggers, how the delta size gate behaves — so a finding depends on
    /// that model being faithful, not just on the contract being wrong. Two model
    /// bugs were found in review already: the delta gate compared against the wrong
    /// peer's state (fixed), and the full-state fallback may fire where production
    /// would not, which would HIDE a real divergence rather than invent one.
    ///
    /// It is nevertheless `Severity::Violation`, i.e. removal-eligible, because a
    /// genuine reconciliation loop is exactly the #5153 shape this whole effort
    /// exists to catch.
    ///
    /// Ian's call (2026-08-17) was to leave it removal-eligible and settle the
    /// question when the active-but-not-enforcing phase is wired, with real shadow
    /// telemetry rather than a five-contract sample. Do not quietly downgrade it
    /// before then, and do not let Enforce become reachable without revisiting it.
    ///
    /// What would settle it, in either direction:
    ///
    /// - Shadow-mode counts of contracts flagged by THIS property and by no other
    ///   removal-eligible property. On the live corpus that number was zero: every
    ///   contract it flagged also broke commutativity, mutual rejection included. If
    ///   shadow keeps it at zero, the property earns nothing at this severity and
    ///   should drop to `Diagnostic`. If it is the sole finding for real contracts,
    ///   it is carrying signal the algebraic checks miss and should stay.
    /// - Any shadow finding from this property that turns out to converge in
    ///   production is a model bug, and should drop the severity immediately.
    ReconciliationCycle,
    /// The delta path and the merge path must reach the same state.
    ///
    /// A contract has two write paths into the same logical state: `update_state`
    /// handed a `Delta`, and `update_state` handed another peer's whole `State`.
    /// Which one a peer takes is decided by the *protocol* — `gate_delta_size`
    /// refuses an oversized delta and sends the whole state instead — not by the
    /// application. So two peers given the same information by different routes must
    /// end up with the same bytes, or they can never agree.
    ///
    /// Every other property here compares merge-to-merge or delta-to-delta.
    /// `StateCommutativity`, `StateAssociativity`, `DeltaPermutationInvariance` and
    /// `DeltaIdempotence` are all satisfiable by a contract whose two paths use
    /// *different combinators*, because none of them ever puts one path beside the
    /// other. The defect that motivated this (#5394) did exactly that: `insert`
    /// (last write wins) when handed a delta, `entry().or_insert()` (first write
    /// wins) when handed a state, on a map keyed by a client-chosen sequence number.
    /// A retraction applied as a delta is silently resurrected when a replica
    /// re-merges the same op as a state.
    ///
    /// # What is checked
    ///
    /// For a pair of observed states, in both orders:
    ///
    /// ```text
    /// delta  = get_state_delta(other, summarize(base))   // what the sender ships
    /// apply(base, delta)  ==  update_state(base, State(other))
    /// ```
    ///
    /// # Why this is `Severity::Violation` and not a false-positive generator
    ///
    /// A plain inequality here would accuse a large and legitimate class of
    /// contract: one whose summary is too coarse to express a particular divergence,
    /// so its delta carries only *part* of what the other state holds. Such a
    /// contract lands short of the merged state on this round and converges on the
    /// next one, and this module already refuses to accuse it under
    /// `ReconciliationCycle` for the same reason.
    ///
    /// So a disagreement is only reported when re-merging the whole state fails to
    /// repair it:
    ///
    /// ```text
    /// merge(apply(base, delta), other)  !=  merge(base, other)
    /// ```
    ///
    /// For any contract whose merge is a genuine join, that condition is
    /// unreachable: a delta derived from `other` can only move `base` somewhere
    /// between `base` and `base ⊔ other`, and joining `other` back on top lands on
    /// `base ⊔ other` either way. A partial delta therefore heals and is never
    /// reported; a delta path that computed something the merge path cannot reach
    /// does not heal and is. That is the same practical consequence as a
    /// non-commutative merge — two peers with the same information, permanently
    /// disagreeing — which is why the severity matches `StateCommutativity`.
    ///
    /// The guard errs toward silence in every direction, including when the merge
    /// itself is unsound: a last-write-wins merge heals trivially, so this property
    /// declines to pile a second accusation onto a defect `StateCommutativity`
    /// already names. One law per property.
    ///
    /// # What this does NOT establish, and what would settle it
    ///
    /// Like `ReconciliationCycle`, and unlike the pure algebra above it, this rests
    /// on a model of the protocol: that the sender computes its delta against the
    /// recipient's summary, and that production would actually take the delta path.
    /// The second is checked directly (`delta_would_be_refused`, production's own
    /// gate); the first is the same assumption `reconciliation_cycle` makes. Unlike
    /// `ReconciliationCycle` this is a single step with no schedule to get wrong,
    /// which is the narrower assumption of the two.
    ///
    /// Shadow-mode counts are what should settle whether the severity is right:
    /// contracts flagged by THIS property and by no other removal-eligible one. If
    /// that number stays at zero the property earns nothing at this severity; if it
    /// is the sole finding for real contracts, it is carrying signal the algebraic
    /// checks miss.
    ///
    /// # Two measured limits, both of them misses rather than false accusations
    ///
    /// **The guard is direction-sensitive.** Re-merging repairs the difference for
    /// one ordering of a pair and not the other — for the fixture in
    /// `tests/test-contract-conformance` (mode 8) it silences exactly one of
    /// `(A, B)` and `(B, A)`. Both directions are therefore checked for every pair.
    /// A corpus holding only one state of such a pair still misses the defect.
    ///
    /// **A contract whose delta IS its state is invisible to this check.** The delta
    /// is synthesized from the contract's own `get_state_delta`, so only a
    /// disagreement that the contract's own delta encoding can express can be seen.
    /// Measured against the #5394 artifacts on 2026-08-23: that contract's
    /// `get_state_delta` returns the whole sender state for every non-trivial pair
    /// in the corpus, and its `update_state` then treats a whole-state `Delta`
    /// exactly as it treats a `State`, so both paths agree on every pair and this
    /// property stays silent on the very defect it was written from. The
    /// disagreement there lives on an APPLICATION-level delta — a client `UPDATE`
    /// carrying one op — which `get_state_delta` never produces and which a
    /// states-only corpus does not carry.
    ///
    /// Closing that needs a second, transition-shaped form of the same law, over the
    /// captured deltas in `ReplayBundle::transitions`: for an observed
    /// `base + delta -> result`, `merge(base, result)` must equal `result`. It is
    /// deliberately NOT folded in here — it has a different arity and a different
    /// false-positive profile (a capped collection trips it), and one law per
    /// property is the rule this module follows everywhere else.
    PathAgreement,
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
        ConformanceProperty::PathAgreement,
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
            | ConformanceProperty::ReconciliationCycle
            | ConformanceProperty::PathAgreement => Severity::Violation,
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
            | ConformanceProperty::ReconciliationCycle
            | ConformanceProperty::PathAgreement => 2,
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
            | ConformanceProperty::ReconciliationCycle
            | ConformanceProperty::PathAgreement => 0,
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
            ConformanceProperty::PathAgreement => "path_agreement",
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
    /// There is no delta path for these inputs, so there is nothing to compare the
    /// merge path against.
    ///
    /// Either the contract produced an empty delta — the protocol's "nothing to
    /// send", which a summary too coarse to express the divergence produces
    /// legitimately — or the delta was large enough that production's own size gate
    /// would refuse it and send the whole state instead. In both cases the delta
    /// path is not the path this update would take on the network, and checking it
    /// would be checking a call that never happens.
    NoDeltaPath,
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
            Inconclusive::NoDeltaPath => {
                f.write_str("no delta path exists for these inputs to compare against")
            }
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
