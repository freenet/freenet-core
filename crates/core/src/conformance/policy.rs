//! What a peer is allowed to *do* about a conformance finding.
//!
//! The RFC's deployment plan makes deletion the last step, after offline
//! validation, single-peer shadow validation, a full release of fleet-wide shadow
//! mode, and an explicit enforcement gate. That ordering is easy to state and easy
//! to erode: one plausible-looking commit that treats a violation as a removal, and
//! the gate is gone with nothing failing.
//!
//! So the ordering lives here, as a decision function with tests, rather than as
//! scattered `if` statements at the call sites. Three invariants are pinned:
//!
//! 1. Under [`EnforcementMode::Shadow`], **no input of any kind produces a removal.**
//! 2. A [`Severity::Diagnostic`] finding never proposes removal in *any* mode. A
//!    wasteful self-delta (#5072, #5056) is an efficiency report about a contract
//!    that still converges. Deleting an application over that would be absurd, and
//!    the distinction is exactly the sort that erodes once "violation" becomes one
//!    undifferentiated word.
//! 3. A [`RemovalTier::ReportOnly`] finding is a [`ConformanceAction::Report`] in
//!    every mode that runs, Shadow included. See [`removal_tier`].

use serde::{Deserialize, Serialize};

use super::property::{
    ConformanceProperty, IdempotenceSettling, PropertyOutcome, Severity, Violation,
};

/// How much authority the conformance mechanism currently has.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
pub enum EnforcementMode {
    /// Nothing runs. Sampling and checking are off entirely.
    Disabled,
    /// The full mechanism runs and reports, but nothing is ever removed.
    ///
    /// The default, and the only mode the RFC's plan permits until every gate in
    /// its Phase 5 list has been met on live data.
    #[default]
    Shadow,
    /// Verified violations remove the contract locally.
    ///
    /// Not reachable from configuration yet. It exists so the shadow-mode code path
    /// is the same code path enforcement will use, which is what makes shadow-mode
    /// measurements predictive of enforcement rather than merely adjacent to it.
    Enforce,
}

/// What the peer should do about one verified outcome.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ConformanceAction {
    /// The law held, or the check reached no verdict. Nothing to do.
    Nothing,
    /// Worth telling the author and the telemetry, but not grounds for removal.
    Report(Violation),
    /// Would have been removed had enforcement been enabled. This is the signal the
    /// shadow period exists to collect, and the number the enforcement gate is
    /// judged on.
    WouldRemove(Violation),
    /// Remove the contract locally: stop executing, serving, propagating and
    /// hosting it, and remember the id so ordinary traffic cannot immediately
    /// reintroduce it.
    Remove(Violation),
}

impl ConformanceAction {
    pub fn removes(&self) -> bool {
        matches!(self, ConformanceAction::Remove(_))
    }

    pub fn violation(&self) -> Option<&Violation> {
        match self {
            ConformanceAction::Report(v)
            | ConformanceAction::WouldRemove(v)
            | ConformanceAction::Remove(v) => Some(v),
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
///
/// Only a [`Severity::Violation`] whose [`removal_tier`] is
/// [`RemovalTier::RemovalEligible`] can propose a removal. Everything else that
/// was violated is a [`ConformanceAction::Report`].
pub fn decide(mode: EnforcementMode, outcome: &PropertyOutcome) -> ConformanceAction {
    let violation = match outcome {
        PropertyOutcome::Violated(v) => v.clone(),
        PropertyOutcome::Holds | PropertyOutcome::Inconclusive(_) => {
            return ConformanceAction::Nothing;
        }
    };

    // Severity comes from the property, not from the field the `Violation` carries:
    // the headline invariant here is that a diagnostic never proposes removal in any
    // mode, and that invariant should not rest on the caller never handing us a
    // deserialized `Violation`. Same expression, no trust dependency.
    match (
        mode,
        violation.property.severity(),
        removal_tier(&violation),
    ) {
        (EnforcementMode::Disabled, _, _) => ConformanceAction::Nothing,
        // Diagnostics are reports in every mode, enforcement included.
        (_, Severity::Diagnostic, _) => ConformanceAction::Report(violation),
        // So are report-only findings, and Shadow is not an exception: a
        // `WouldRemove` is the number the enforcement gate is judged on, and a
        // finding anyone can produce on demand must not be able to move it.
        (_, Severity::Violation, RemovalTier::ReportOnly) => ConformanceAction::Report(violation),
        (EnforcementMode::Shadow, Severity::Violation, RemovalTier::RemovalEligible) => {
            ConformanceAction::WouldRemove(violation)
        }
        (EnforcementMode::Enforce, Severity::Violation, RemovalTier::RemovalEligible) => {
            ConformanceAction::Remove(violation)
        }
    }
}

/// Whether a finding may ever propose a removal, in any mode.
///
/// A separate axis from [`Severity`] on purpose. Severity says what the finding
/// IS: a `Violation` broke a merge law, and `fdev` fails on it so the author fixes
/// it. This says what the NETWORK may do about it. A law can be broken for real and
/// still be no ground for removal, because removal needs more than a broken law:
/// the finding must not be producible by inputs anyone may supply, and the
/// contract must not converge after a bounded cost anyway. Folding that into the
/// severity would bend the classification to soften an enforcement consequence,
/// which is the move #5462 rejected.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum RemovalTier {
    /// [`ConformanceAction::WouldRemove`] in Shadow, [`ConformanceAction::Remove`]
    /// under Enforce.
    RemovalEligible,
    /// [`ConformanceAction::Report`] in every mode that runs, Shadow included.
    ReportOnly,
}

/// The tier a property's findings get before anything about the individual
/// finding is considered.
///
/// An exhaustive match with no wildcard arm, so a new property does not compile
/// until someone has decided whether it may lead to a removal.
///
/// The report-only set was decided on 2026-10-22 (merge-law conformance tracker
/// #5906): properties whose findings can be produced by inputs anyone may supply,
/// or that describe a contract which converges after a bounded cost, are reported
/// and never removed.
pub fn property_tier(property: ConformanceProperty) -> RemovalTier {
    match property {
        // Diagnostics: wasteful or contested, never a proof of non-convergence.
        // `decide` also refuses them by severity; listing them here keeps the tier
        // table a complete answer on its own.
        ConformanceProperty::DeltaIdempotence
        | ConformanceProperty::SelfDeltaEmpty
        | ConformanceProperty::WholeStateSelfDelta => RemovalTier::ReportOnly,
        // The delta-order law covers concurrent, independent deltas. Capture pairs
        // only deltas recorded against the same base, which is necessary for that
        // and not sufficient: it does not prove the two were concurrent. Until it
        // can, a finding here may be about a causally sequenced pair the law does
        // not cover.
        ConformanceProperty::DeltaPermutationInvariance => RemovalTier::ReportOnly,
        // Both path-agreement forms rest on more than the contract's algebra: the
        // pairwise form on a model of which path the protocol takes, the transition
        // form on provenance (`result` was reached from `base`) that the bytes
        // cannot carry. Their removal arguments also assumed a semilattice join,
        // which heal-guarded associativity (#5915) no longer implies.
        ConformanceProperty::PathAgreement | ConformanceProperty::TransitionPathAgreement => {
            RemovalTier::ReportOnly
        }
        // `StateIdempotence` is refined per finding by `removal_tier`: a state that
        // settles after a rewrite is report-only, anything else keeps this tier.
        ConformanceProperty::StateIdempotence
        | ConformanceProperty::StateCommutativity
        | ConformanceProperty::StateAssociativity
        | ConformanceProperty::EmittedStateValidity
        | ConformanceProperty::UpdateDeterminism
        | ConformanceProperty::SummaryDeterminism
        | ConformanceProperty::DeltaDeterminism
        | ConformanceProperty::ReconciliationCycle => RemovalTier::RemovalEligible,
    }
}

/// The tier of one finding: [`property_tier`], refined by what this peer
/// recomputed about the finding.
///
/// The one refinement is idempotence. A `StateIdempotence` finding whose state
/// was rewritten and then held still ([`IdempotenceSettling::SettledAfter`]) is
/// report-only: the contract converges after a bounded cost, and the install path
/// stores raw client bytes without running the merge, so anyone who can PUT can
/// produce one. A finding that did not settle within the budget
/// ([`IdempotenceSettling::NeverSettled`]) or whose classification could not
/// complete ([`IdempotenceSettling::Indeterminate`]) keeps the property's tier.
/// Neither is a proof that the state never settles, so both are a known gap that
/// must be closed before Enforce can become reachable; replacing the bounded budget
/// with a proof of non-settling is separate work (#5906).
///
/// This tier governs conformance REMOVAL only. The live re-apply probe
/// (`ring::broken_invariants`, fed by `contract::executor::runtime::reapply_probe`)
/// suppresses propagation for a contract whose merge does not settle when an
/// incoming state is re-applied, independently of this tier.
///
/// `settling` is read from the `Violation` because nothing else carries it. That
/// is sound for the same reason [`decide`] is: its caller re-executed the case
/// itself, so the classification is local. It can also only LOWER the tier: the
/// arm keys on the property, so a `settling` value on any other property is
/// ignored, and every other settling class leaves the property's tier unchanged.
pub fn removal_tier(violation: &Violation) -> RemovalTier {
    match (violation.property, violation.settling) {
        (ConformanceProperty::StateIdempotence, Some(IdempotenceSettling::SettledAfter(_))) => {
            RemovalTier::ReportOnly
        }
        (property, _) => property_tier(property),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::conformance::property::{Inconclusive, OutputDigest};

    fn violation(property: ConformanceProperty) -> PropertyOutcome {
        PropertyOutcome::Violated(Violation {
            property,
            severity: property.severity(),
            left: OutputDigest::of(b"left"),
            right: OutputDigest::of(b"right"),
            detail: "test".to_string(),
            settling: None,
        })
    }

    /// A `Violation` whose carried severity disagrees with its property must not be
    /// able to buy itself removal eligibility.
    ///
    /// `Violation` is `Deserialize` with public fields and travels inside evidence,
    /// so in any future that feeds wire data to these functions the field is
    /// attacker-influenceable while the property is not. The invariant this module
    /// leads with — a diagnostic never proposes removal in any mode — should hold
    /// because of what the property IS, not because every caller can be trusted to
    /// have built the struct honestly.
    #[test]
    fn a_forged_severity_cannot_make_a_diagnostic_removable() {
        // DeltaIdempotence is a diagnostic; claim otherwise in the struct.
        let forged = PropertyOutcome::Violated(Violation {
            property: ConformanceProperty::DeltaIdempotence,
            severity: Severity::Violation,
            left: OutputDigest::of(b"left"),
            right: OutputDigest::of(b"right"),
            detail: "claims to be enforceable".to_string(),
            settling: None,
        });

        assert!(
            !forged.is_enforceable_violation(),
            "severity must come from the property, not from the carried field"
        );
        assert!(
            !decide(EnforcementMode::Enforce, &forged).removes(),
            "a diagnostic property must not be removable even under Enforce, and even \
             when the struct says otherwise"
        );
    }

    /// The load-bearing test. If a future change makes shadow mode capable of
    /// removing anything, this fails, and the deployment plan's whole ordering
    /// depends on it holding.
    #[test]
    fn shadow_mode_never_removes_anything() {
        for property in ConformanceProperty::ALL {
            let action = decide(EnforcementMode::Shadow, &violation(*property));
            assert!(
                !action.removes(),
                "shadow mode proposed a removal for {property}"
            );
        }
        assert!(!decide(EnforcementMode::Shadow, &PropertyOutcome::Holds).removes());
        assert!(
            !decide(
                EnforcementMode::Shadow,
                &PropertyOutcome::Inconclusive(Inconclusive::RoundLimit)
            )
            .removes()
        );
    }

    /// A wasteful self-delta is an efficiency complaint about a contract that still
    /// converges. It must never propose removal, even once enforcement is on.
    #[test]
    fn diagnostics_never_propose_removal_even_under_enforcement() {
        for property in ConformanceProperty::ALL {
            if property.severity() != Severity::Diagnostic {
                continue;
            }
            let action = decide(EnforcementMode::Enforce, &violation(*property));
            assert!(
                matches!(action, ConformanceAction::Report(_)),
                "diagnostic {property} produced {action:?} under enforcement"
            );
        }
    }

    /// The counterpart: the check must be capable of proposing removal at all, or
    /// the two tests above would pass against a mechanism that does nothing.
    #[test]
    fn a_merge_law_break_would_be_removed_in_shadow_and_is_under_enforcement() {
        let outcome = violation(ConformanceProperty::StateCommutativity);
        assert!(matches!(
            decide(EnforcementMode::Shadow, &outcome),
            ConformanceAction::WouldRemove(_)
        ));
        assert!(decide(EnforcementMode::Enforce, &outcome).removes());
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
    /// the safe state depends on every call site remembering to ask for it.
    #[test]
    fn the_default_mode_is_shadow() {
        assert_eq!(EnforcementMode::default(), EnforcementMode::Shadow);
    }

    /// Every mode, written so a new one does not compile until it is listed here.
    fn every_mode() -> [EnforcementMode; 3] {
        let modes = [
            EnforcementMode::Disabled,
            EnforcementMode::Shadow,
            EnforcementMode::Enforce,
        ];
        for mode in modes {
            match mode {
                EnforcementMode::Disabled | EnforcementMode::Shadow | EnforcementMode::Enforce => {}
            }
        }
        modes
    }

    fn idempotence_with(settling: Option<IdempotenceSettling>) -> PropertyOutcome {
        PropertyOutcome::Violated(Violation {
            settling,
            ..violation_of(ConformanceProperty::StateIdempotence)
        })
    }

    fn violation_of(property: ConformanceProperty) -> Violation {
        match violation(property) {
            PropertyOutcome::Violated(v) => v,
            PropertyOutcome::Holds | PropertyOutcome::Inconclusive(_) => {
                unreachable!("violation() builds a violation")
            }
        }
    }

    /// A report-only finding is `Report` in every mode that runs, Shadow included,
    /// and `Nothing` when the mechanism is disabled.
    #[track_caller]
    fn assert_report_only_in_every_mode(outcome: &PropertyOutcome) {
        for mode in every_mode() {
            let action = decide(mode, outcome);
            match mode {
                EnforcementMode::Disabled => assert_eq!(action, ConformanceAction::Nothing),
                EnforcementMode::Shadow | EnforcementMode::Enforce => assert!(
                    matches!(action, ConformanceAction::Report(_)),
                    "a report-only finding produced {action:?} under {mode:?}"
                ),
            }
        }
    }

    #[test]
    fn a_settled_idempotence_finding_is_report_only_in_every_mode() {
        for rewrites in [1, 2, u32::MAX] {
            assert_report_only_in_every_mode(&idempotence_with(Some(
                IdempotenceSettling::SettledAfter(rewrites),
            )));
        }
    }

    #[test]
    fn transition_path_agreement_is_report_only_in_every_mode() {
        assert_report_only_in_every_mode(&violation(ConformanceProperty::TransitionPathAgreement));
    }

    #[test]
    fn delta_permutation_invariance_is_report_only_in_every_mode() {
        assert_report_only_in_every_mode(&violation(
            ConformanceProperty::DeltaPermutationInvariance,
        ));
    }

    #[test]
    fn path_agreement_is_report_only_in_every_mode() {
        assert_report_only_in_every_mode(&violation(ConformanceProperty::PathAgreement));
    }

    #[test]
    fn delta_idempotence_is_report_only_in_every_mode() {
        assert_report_only_in_every_mode(&violation(ConformanceProperty::DeltaIdempotence));
    }

    #[test]
    fn self_delta_empty_is_report_only_in_every_mode() {
        assert_report_only_in_every_mode(&violation(ConformanceProperty::SelfDeltaEmpty));
    }

    #[test]
    fn whole_state_self_delta_is_report_only_in_every_mode() {
        assert_report_only_in_every_mode(&violation(ConformanceProperty::WholeStateSelfDelta));
    }

    /// The counterpart to the settled case: an idempotence finding that is NOT shown
    /// to settle keeps the property's tier. Without this, making all of
    /// `StateIdempotence` report-only would pass the settled test above.
    ///
    /// KNOWN GAP, pinned on purpose. `NeverSettled` here means "did not settle
    /// within the budget" and `Indeterminate` means "classification failed" (a trap
    /// or a resource ceiling can cause it). Neither is a proof that the state never
    /// settles, and both must be resolved before Enforce can become reachable
    /// (#5906, proof of non-settling). When that lands this test changes with it;
    /// until then it records what the policy does today.
    #[test]
    fn known_gap_idempotence_not_shown_to_settle_stays_removal_eligible() {
        for settling in [
            Some(IdempotenceSettling::NeverSettled),
            Some(IdempotenceSettling::Indeterminate),
            None,
        ] {
            let outcome = idempotence_with(settling);
            assert!(
                matches!(
                    decide(EnforcementMode::Shadow, &outcome),
                    ConformanceAction::WouldRemove(_)
                ),
                "{settling:?} must stay would-remove in shadow"
            );
            assert!(
                decide(EnforcementMode::Enforce, &outcome).removes(),
                "{settling:?} must stay removable under enforcement"
            );
        }
    }

    /// `settling` refines idempotence only. A `SettledAfter` attached to any other
    /// property must not buy it the report-only tier.
    #[test]
    fn a_settling_value_on_another_property_does_not_change_its_tier() {
        let outcome = PropertyOutcome::Violated(Violation {
            settling: Some(IdempotenceSettling::SettledAfter(1)),
            ..violation_of(ConformanceProperty::StateCommutativity)
        });
        assert!(decide(EnforcementMode::Enforce, &outcome).removes());
    }

    /// The whole partition, stated once. A property moving between tiers is a
    /// decision about what the network may do, so it must show up as a change to
    /// this list, not only as a changed arm in `property_tier`.
    #[test]
    fn the_removal_eligible_properties_are_exactly_these() {
        let eligible: Vec<ConformanceProperty> = ConformanceProperty::ALL
            .iter()
            .copied()
            .filter(|p| property_tier(*p) == RemovalTier::RemovalEligible)
            .collect();
        assert_eq!(
            eligible,
            vec![
                ConformanceProperty::StateIdempotence,
                ConformanceProperty::StateCommutativity,
                ConformanceProperty::StateAssociativity,
                ConformanceProperty::EmittedStateValidity,
                ConformanceProperty::UpdateDeterminism,
                ConformanceProperty::SummaryDeterminism,
                ConformanceProperty::DeltaDeterminism,
                ConformanceProperty::ReconciliationCycle,
            ]
        );
        // And each of them really does reach `WouldRemove`, so the list is a fact
        // about `decide` and not only about the table.
        for property in eligible {
            assert!(
                matches!(
                    decide(EnforcementMode::Shadow, &violation(property)),
                    ConformanceAction::WouldRemove(_)
                ),
                "{property} is listed removal-eligible but shadow did not propose it"
            );
        }
    }

    /// A diagnostic is report-only in the table too, so the two refusals in
    /// `decide` cannot disagree about any property.
    #[test]
    fn every_diagnostic_property_is_report_only() {
        for property in ConformanceProperty::ALL {
            if property.severity() == Severity::Diagnostic {
                assert_eq!(
                    property_tier(*property),
                    RemovalTier::ReportOnly,
                    "{property}"
                );
            }
        }
    }
}
