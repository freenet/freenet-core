//! Pin every scenario against every controller. Adding a new
//! [`Scenario`](replay_harness::scenarios::Scenario) or a new
//! [`Controller`](replay_harness::Controller) only requires editing
//! this file in one place: extend [`controllers_under_test`] with the
//! new controller, or add the scenario to [`scenarios::all_scenarios`]
//! and the loop below picks it up.
//!
//! Important: not every controller passes every "sane controller"
//! expectation. The `RfcDraft` reference controller is intentionally
//! broken-on-purpose for one scenario (`idle_steady_state`) — that
//! failure is the demonstration of the noise-floor problem Phase 1
//! telemetry exposed. We assert the **documented** outcome per
//! (controller, scenario) rather than blanket-asserting the
//! scenario's expectation.

use std::collections::HashMap;

use replay_harness::controllers::{FixedRate, RfcDraft};
use replay_harness::scenarios::{Expectation, Scenario, all_scenarios};
use replay_harness::{Controller, Replayer};

/// Whether the given controller is currently *expected* to fire on the
/// given scenario. Anything not in the map defaults to the scenario's
/// own [`Expectation`].
///
/// This is intentionally explicit — it documents which controller has
/// which known failure modes against which scenario. A new controller
/// proposal can extend this map with its own expectations and the
/// scenario assertions adapt accordingly.
fn expected_fires(controller: &str, scenario: &str) -> bool {
    let overrides: HashMap<(&str, &str), bool> = [
        // RfcDraft fires on idle_steady_state because the 30 ms trigger
        // sits below the ambient overlay noise floor. This is the bug
        // the harness exists to demonstrate, and the failure mode any
        // Phase 2 candidate must NOT repeat.
        (("rfc_draft", "idle_steady_state"), true),
    ]
    .into_iter()
    .collect();
    if let Some(&v) = overrides.get(&(controller, scenario)) {
        return v;
    }
    matches!(
        all_scenarios()
            .iter()
            .find(|s| s.name == scenario)
            .map(|s| s.expectation),
        Some(Expectation::FiresAtLeastOnce)
    )
}

fn run<C: Controller>(s: &Scenario, controller: C) -> (String, bool /* fired */) {
    let report = Replayer::new().run(s.events.clone().into_iter(), controller);
    (report.controller, report.decisions_set > 0)
}

#[test]
fn fixed_rate_never_fires_on_any_scenario() {
    // Universal sanity check: a no-op controller never produces any
    // rate decisions, regardless of input. If this ever fails, the
    // bug is in the harness itself (not in the controller).
    for s in all_scenarios() {
        let (name, fired) = run(&s, FixedRate);
        assert!(!fired, "{name} fired on scenario {}", s.name);
    }
}

#[test]
fn rfc_draft_outcomes_match_documented_expectations() {
    for s in all_scenarios() {
        let (name, fired) = run(&s, RfcDraft::default());
        let expected = expected_fires(&name, s.name);
        assert_eq!(
            fired, expected,
            "{name} on {}: fired={fired}, expected={expected}",
            s.name
        );
    }
}

/// Sanity check on the scenario set itself: every scenario must declare
/// a meaningful expectation (not `Informational`) for v1, since the
/// informational tier exists for design-evaluation scenarios we haven't
/// authored yet.
#[test]
fn scenarios_declare_concrete_expectations() {
    for s in all_scenarios() {
        assert!(
            !matches!(s.expectation, Expectation::Informational),
            "scenario {} should declare NeverFires or FiresAtLeastOnce",
            s.name
        );
    }
}
