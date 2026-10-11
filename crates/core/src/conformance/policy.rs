//! What a peer is allowed to *do* about a conformance finding.
//!
//! The RFC's deployment plan makes deletion the last step, after offline
//! validation, single-peer shadow validation, a full release of fleet-wide shadow
//! mode, and an explicit enforcement gate. That ordering is easy to state and easy
//! to erode: one plausible-looking commit that treats a violation as a removal, and
//! the gate is gone with nothing failing.
//!
//! So the ordering lives here, as a decision function with tests, rather than as
//! scattered `if` statements at the call sites. Two invariants are pinned:
//!
//! 1. Under [`EnforcementMode::Shadow`], **no input of any kind produces a removal.**
//! 2. A [`Severity::Diagnostic`] finding never proposes removal in *any* mode. A
//!    wasteful self-delta (#5072, #5056) is an efficiency report about a contract
//!    that still converges. Deleting an application over that would be absurd, and
//!    the distinction is exactly the sort that erodes once "violation" becomes one
//!    undifferentiated word.

use serde::{Deserialize, Serialize};

use super::property::{PropertyOutcome, Severity, Violation};

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
    match (mode, violation.property.severity()) {
        (EnforcementMode::Disabled, _) => ConformanceAction::Nothing,
        // Diagnostics are reports in every mode, enforcement included.
        (_, Severity::Diagnostic) => ConformanceAction::Report(violation),
        (EnforcementMode::Shadow, Severity::Violation) => ConformanceAction::WouldRemove(violation),
        (EnforcementMode::Enforce, Severity::Violation) => ConformanceAction::Remove(violation),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::conformance::property::{ConformanceProperty, Inconclusive, OutputDigest};

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
}
