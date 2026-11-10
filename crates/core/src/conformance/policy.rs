//! What a peer does with a conformance finding, and how serious the finding is.
//!
//! **Nodes do not remove contracts for breaking a merge law.** Removal was the
//! plan in RFC #5320 and is superseded, see
//! <https://github.com/freenet/freenet-core/issues/5906#issuecomment-6677346389>.
//! Verifier and shadow results are diagnostics, recorded locally. Instead, nodes
//! decline work that a contract with a lawful merge never needs: a payload already
//! merged into the node's current state is not merged again
//! (`ring::applied_payloads`).
//!
//! Two things are decided here:
//!
//! 1. [`decide`]: whether a verified outcome is reported at all. It is reported in
//!    every mode that runs, and nothing else happens to it.
//! 2. [`finding_class`]: whether a finding is [`FindingClass::Persistent`] (on its
//!    own, it shows a contract that does not converge or is not deterministic) or
//!    [`FindingClass::ReportOnly`] (anyone may be able to produce it on demand, or
//!    it describes a contract that converges after a bounded cost). The class
//!    decides which shadow count a finding lands in, whether `fdev` writes evidence
//!    for it, and which finding the minimizer may shrink toward. A
//!    [`Severity::Diagnostic`] finding is always `ReportOnly`.
//!
//! [`Severity::Diagnostic`]: super::property::Severity::Diagnostic

use serde::{Deserialize, Serialize};

use super::property::{ConformanceProperty, IdempotenceSettling, PropertyOutcome, Violation};

/// Whether the conformance mechanism runs.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
pub enum EnforcementMode {
    /// Nothing runs. Sampling and checking are off entirely.
    Disabled,
    /// The full mechanism runs and reports. Its findings are diagnostics recorded
    /// locally. The default.
    #[default]
    Shadow,
}

/// What the peer should do about one verified outcome.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ConformanceAction {
    /// The law held, the check reached no verdict, or the mechanism is disabled.
    /// Nothing to do.
    Nothing,
    /// Worth telling the author and the local diagnostics.
    Report(Violation),
}

impl ConformanceAction {
    pub fn violation(&self) -> Option<&Violation> {
        match self {
            ConformanceAction::Report(v) => Some(v),
            ConformanceAction::Nothing => None,
        }
    }
}

/// Decide what to do about an outcome the peer has verified *itself*.
///
/// The caller must have re-executed the case locally. This function deliberately
/// takes an outcome rather than evidence: there is no argument you can pass it that
/// means "another peer says so", because a remote claim is never grounds for
/// anything on its own.
pub fn decide(mode: EnforcementMode, outcome: &PropertyOutcome) -> ConformanceAction {
    match (mode, outcome) {
        (EnforcementMode::Disabled, _) => ConformanceAction::Nothing,
        (EnforcementMode::Shadow, PropertyOutcome::Violated(v)) => {
            ConformanceAction::Report(v.clone())
        }
        (EnforcementMode::Shadow, PropertyOutcome::Holds | PropertyOutcome::Inconclusive(_)) => {
            ConformanceAction::Nothing
        }
    }
}

/// How serious a finding is, separately from its [`Severity`].
///
/// [`Severity`]: super::property::Severity
///
/// Severity says what the finding IS: a `Violation` broke a merge law, and `fdev`
/// fails on it so the author fixes it. The class says whether the finding, on its
/// own, shows a contract that does not converge or is not deterministic. A law can
/// be broken for real and still be `ReportOnly`: the finding may be producible by
/// inputs anyone may supply, rest on a premise the bytes cannot carry, or describe
/// a contract that converges after a bounded cost. Folding that into the severity
/// would bend the classification, which is the move #5462 rejected.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum FindingClass {
    /// On its own, the finding shows a contract that does not converge or is not
    /// deterministic.
    Persistent,
    /// Reported, and not counted as persistent.
    ReportOnly,
}

/// The class a property's findings get before anything about the individual
/// finding is considered.
///
/// An exhaustive match with no wildcard arm, so a new property does not compile
/// until someone has decided its class.
///
/// The report-only set was decided on 2026-10-22 (merge-law conformance tracker
/// #5906): properties whose findings can be produced by inputs anyone may supply,
/// or that describe a contract which converges after a bounded cost, are
/// report-only.
pub fn property_class(property: ConformanceProperty) -> FindingClass {
    match property {
        // Diagnostics: wasteful, or (delta idempotence) a law nodes act on no
        // verdict for. Never a proof of non-convergence.
        ConformanceProperty::DeltaIdempotence
        | ConformanceProperty::SelfDeltaEmpty
        | ConformanceProperty::WholeStateSelfDelta => FindingClass::ReportOnly,
        // The delta-order law covers concurrent, independent deltas. Capture pairs
        // only deltas recorded against the same base, which is necessary for that
        // and not sufficient: it does not prove the two were concurrent. Until it
        // can, a finding here may be about a causally sequenced pair the law does
        // not cover.
        ConformanceProperty::DeltaPermutationInvariance => FindingClass::ReportOnly,
        // Both path-agreement forms rest on more than the contract's algebra: the
        // pairwise form on a model of which path the protocol takes, the transition
        // form on provenance (`result` was reached from `base`) that the bytes
        // cannot carry. Their argument for being persistent also assumed a
        // semilattice join, which heal-guarded associativity (#5915) no longer
        // implies.
        ConformanceProperty::PathAgreement | ConformanceProperty::TransitionPathAgreement => {
            FindingClass::ReportOnly
        }
        // `StateIdempotence` is refined per finding by `finding_class`: a state that
        // settles after a rewrite is report-only, anything else keeps this class.
        ConformanceProperty::StateIdempotence
        | ConformanceProperty::StateCommutativity
        | ConformanceProperty::StateAssociativity
        | ConformanceProperty::EmittedStateValidity
        | ConformanceProperty::UpdateDeterminism
        | ConformanceProperty::SummaryDeterminism
        | ConformanceProperty::DeltaDeterminism
        | ConformanceProperty::ReconciliationCycle => FindingClass::Persistent,
    }
}

/// The class of one finding: [`property_class`], refined by what this peer
/// recomputed about the finding.
///
/// The one refinement is idempotence. A `StateIdempotence` finding whose state
/// was rewritten and then held still ([`IdempotenceSettling::SettledAfter`]) is
/// report-only: the contract converges after a bounded cost, and the install path
/// stores raw client bytes without running the merge, so anyone who can PUT can
/// produce one. A finding that did not settle within the budget
/// ([`IdempotenceSettling::NeverSettled`]) or whose classification could not
/// complete ([`IdempotenceSettling::Indeterminate`]) keeps the property's class.
/// Neither is a proof that the state never settles; see
/// `tests::known_gap_idempotence_not_shown_to_settle_stays_persistent`.
///
/// This class is separate from the live re-apply probe
/// (`ring::broken_invariants`, fed by `contract::executor::runtime::reapply_probe`),
/// which suppresses propagation for a contract whose merge does not settle when an
/// incoming state is re-applied, independently of this class.
///
/// `settling` is read from the `Violation` because nothing else carries it. That
/// is sound because the callers re-executed the case themselves, so the
/// classification is local. It can also only LOWER the class: the arm keys on the
/// property, so a `settling` value on any other property is ignored, and every
/// other settling class leaves the property's class unchanged.
pub fn finding_class(violation: &Violation) -> FindingClass {
    match (violation.property, violation.settling) {
        (ConformanceProperty::StateIdempotence, Some(IdempotenceSettling::SettledAfter(_))) => {
            FindingClass::ReportOnly
        }
        (property, _) => property_class(property),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::conformance::property::Severity;
    use crate::conformance::property::{Inconclusive, OutputDigest};

    fn violation(property: ConformanceProperty) -> PropertyOutcome {
        PropertyOutcome::Violated(violation_of(property))
    }

    fn violation_of(property: ConformanceProperty) -> Violation {
        Violation {
            property,
            severity: property.severity(),
            left: OutputDigest::of(b"left"),
            right: OutputDigest::of(b"right"),
            detail: "test".to_string(),
            settling: None,
        }
    }

    /// A `Violation` whose carried severity disagrees with its property must not be
    /// able to make itself persistent.
    ///
    /// `Violation` is `Deserialize` with public fields and travels inside evidence,
    /// so in any future that feeds wire data to these functions the field is
    /// attacker-influenceable while the property is not.
    #[test]
    fn a_forged_severity_cannot_make_a_diagnostic_persistent() {
        // DeltaIdempotence is a diagnostic; claim otherwise in the struct.
        let forged = Violation {
            severity: Severity::Violation,
            detail: "claims to be a law break".to_string(),
            ..violation_of(ConformanceProperty::DeltaIdempotence)
        };
        assert!(
            !PropertyOutcome::Violated(forged.clone()).is_enforceable_violation(),
            "severity must come from the property, not from the carried field"
        );
        assert_eq!(finding_class(&forged), FindingClass::ReportOnly);
    }

    /// Every verified violation is reported while the mechanism runs, whatever its
    /// property or class, and nothing is reported for an outcome without one.
    #[test]
    fn shadow_reports_every_violation_and_nothing_else() {
        for property in ConformanceProperty::ALL {
            assert!(
                matches!(
                    decide(EnforcementMode::Shadow, &violation(*property)),
                    ConformanceAction::Report(_)
                ),
                "{property}"
            );
        }
        assert_eq!(
            decide(EnforcementMode::Shadow, &PropertyOutcome::Holds),
            ConformanceAction::Nothing
        );
        assert_eq!(
            decide(
                EnforcementMode::Shadow,
                &PropertyOutcome::Inconclusive(Inconclusive::RoundLimit)
            ),
            ConformanceAction::Nothing
        );
    }

    #[test]
    fn disabled_mode_does_nothing_at_all() {
        for property in ConformanceProperty::ALL {
            assert_eq!(
                decide(EnforcementMode::Disabled, &violation(*property)),
                ConformanceAction::Nothing
            );
        }
    }

    /// Shadow is the default because a mode that had to be opted *into* would mean
    /// the running state depends on every call site remembering to ask for it.
    #[test]
    fn the_default_mode_is_shadow() {
        assert_eq!(EnforcementMode::default(), EnforcementMode::Shadow);
    }

    fn idempotence_with(settling: Option<IdempotenceSettling>) -> Violation {
        Violation {
            settling,
            ..violation_of(ConformanceProperty::StateIdempotence)
        }
    }

    #[test]
    fn a_settled_idempotence_finding_is_report_only() {
        for rewrites in [1, 2, u32::MAX] {
            assert_eq!(
                finding_class(&idempotence_with(Some(IdempotenceSettling::SettledAfter(
                    rewrites
                )))),
                FindingClass::ReportOnly
            );
        }
    }

    /// The counterpart to the settled case: an idempotence finding that is NOT shown
    /// to settle keeps the property's class. Without this, making all of
    /// `StateIdempotence` report-only would pass the settled test above.
    ///
    /// KNOWN GAP, pinned on purpose. `NeverSettled` here means "did not settle
    /// within the budget" and `Indeterminate` means "classification failed" (a trap
    /// or a resource ceiling can cause it). Neither is a proof that the state never
    /// settles (#5906). This test records what the classification does today.
    #[test]
    fn known_gap_idempotence_not_shown_to_settle_stays_persistent() {
        for settling in [
            Some(IdempotenceSettling::NeverSettled),
            Some(IdempotenceSettling::Indeterminate),
            None,
        ] {
            assert_eq!(
                finding_class(&idempotence_with(settling)),
                FindingClass::Persistent,
                "{settling:?}"
            );
        }
    }

    /// `settling` refines idempotence only. A `SettledAfter` attached to any other
    /// property must not make it report-only.
    #[test]
    fn a_settling_value_on_another_property_does_not_change_its_class() {
        let v = Violation {
            settling: Some(IdempotenceSettling::SettledAfter(1)),
            ..violation_of(ConformanceProperty::StateCommutativity)
        };
        assert_eq!(finding_class(&v), FindingClass::Persistent);
    }

    /// The persistent properties, in `ConformanceProperty::ALL` order. Every other
    /// property is report-only.
    const PERSISTENT_PROPERTIES: &[ConformanceProperty] = &[
        ConformanceProperty::StateIdempotence,
        ConformanceProperty::StateCommutativity,
        ConformanceProperty::StateAssociativity,
        ConformanceProperty::EmittedStateValidity,
        ConformanceProperty::UpdateDeterminism,
        ConformanceProperty::SummaryDeterminism,
        ConformanceProperty::DeltaDeterminism,
        ConformanceProperty::ReconciliationCycle,
    ];

    /// Every property, through the per-finding function: an unrefined finding is
    /// persistent exactly when its property is listed, and report-only otherwise.
    #[test]
    fn every_property_has_the_listed_finding_class() {
        for property in ConformanceProperty::ALL {
            let expected = if PERSISTENT_PROPERTIES.contains(property) {
                FindingClass::Persistent
            } else {
                FindingClass::ReportOnly
            };
            assert_eq!(
                finding_class(&violation_of(*property)),
                expected,
                "{property}"
            );
        }
    }

    /// The whole partition, stated once. A property moving between classes changes
    /// what the shadow counts and `fdev`'s evidence mean, so it must show up as a
    /// change to this list, not only as a changed arm in `property_class`.
    #[test]
    fn the_persistent_properties_are_exactly_these() {
        let persistent: Vec<ConformanceProperty> = ConformanceProperty::ALL
            .iter()
            .copied()
            .filter(|p| property_class(*p) == FindingClass::Persistent)
            .collect();
        assert_eq!(persistent, PERSISTENT_PROPERTIES);
    }

    /// A diagnostic is report-only in the table, so a diagnostic can never be
    /// counted as a persistent finding.
    #[test]
    fn every_diagnostic_property_is_report_only() {
        for property in ConformanceProperty::ALL {
            if property.severity() == Severity::Diagnostic {
                assert_eq!(
                    property_class(*property),
                    FindingClass::ReportOnly,
                    "{property}"
                );
            }
        }
    }
}
