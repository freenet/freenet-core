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
use std::time::Duration;

use replay_harness::controllers::{FixedRate, LedbatPlusPlus, RfcDraft};
use replay_harness::scenarios::{Expectation, Scenario, all_scenarios, find};
use replay_harness::{Controller, RateDecision, Replayer};

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
        // RfcDraft fires on reference_diverges_from_overlay because it is
        // reference-blind: it sees correlated overlay inflation and cannot
        // tell overlay queueing from local uplink contention. The flat
        // reference path (which says "do NOT fire") is exactly the signal
        // it ignores. Pinned as the documented limitation a reference-aware
        // Phase 2 controller must fix — same overlay input as
        // reference_tracks_overlay, opposite correct action.
        (("rfc_draft", "reference_diverges_from_overlay"), true),
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
    let report = Replayer::new()
        .run_until(s.run_for)
        .run(s.events.clone().into_iter(), controller);
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

/// Strengthen the `correlated_inflation` pin: a controller passing
/// "decisions_set > 0" could in principle fire for the wrong reason
/// (e.g. a startup-tick spurious fire at t=2). The scenario sets a
/// clean 60 s baseline, then bursts at t=60s. The minimum *correctness*
/// invariant is: no fire during the baseline phase. Any controller
/// firing before the burst starts is reacting to the clean baseline,
/// which means it would also fire on a healthy idle network.
#[test]
fn rfc_draft_correlated_inflation_first_fire_is_inside_burst_window() {
    let s = find("correlated_inflation").expect("scenario exists");
    let report = Replayer::new()
        .run_until(s.run_for)
        .run(s.events.clone().into_iter(), RfcDraft::default());
    let fires = report.fired();
    assert!(!fires.is_empty(), "RfcDraft must fire during burst");
    let first_fire_at = fires[0].at;
    let burst_start = Duration::from_secs(60);
    assert!(
        first_fire_at >= burst_start,
        "first fire at {:?} is before burst_start ({:?}) — the \
         controller fired during the clean baseline phase",
        first_fire_at,
        burst_start,
    );
    // Sanity: every fire's decision is a Set with a non-trivial rate
    // change, not a no-op.
    for d in fires {
        let RateDecision::Set { .. } = &d.decision else {
            unreachable!("fired() filters to Set");
        };
        assert!(
            d.rate_after_bps < 1_250_000,
            "Set decisions must lower the rate; got rate_after_bps={}",
            d.rate_after_bps,
        );
    }
}

/// The LEDBAT++ death spiral, pinned. `single_packet_loss` is a transient
/// spike on a *single* peer. LEDBAT++ reacts to the worst connection, so it
/// cuts the aggregate rate hard and then recovers slowly; RfcDraft takes the
/// cross-peer median, rejects the outlier, and never moves the rate. The
/// contrast on identical input is the whole point of the scenario.
#[test]
fn ledbat_single_packet_loss_death_spiral() {
    let s = find("single_packet_loss").expect("scenario exists");

    // Derive the starting rate from a no-op FixedRate run rather than
    // hard-coding the Replayer default, so this test doesn't pin a magic number.
    let starting = Replayer::new()
        .run_until(s.run_for)
        .run(s.events.clone().into_iter(), FixedRate)
        .final_rate_bps;

    let ledbat = Replayer::new()
        .run_until(s.run_for)
        .run(s.events.clone().into_iter(), LedbatPlusPlus::default());

    // 1. A deep multiplicative cut actually happened.
    assert!(
        ledbat.min_rate_bps <= starting / 2,
        "LEDBAT++ should cut hard on the spike; min_rate_bps={} (> half of {starting})",
        ledbat.min_rate_bps,
    );
    // 2. Additive increase is capped at the starting rate — it never overshoots.
    assert!(
        ledbat.max_rate_bps <= starting,
        "max_rate_bps={} exceeded starting rate {starting}",
        ledbat.max_rate_bps,
    );

    // 3. The defining asymmetry: the cut is fast, the recovery is slow. Measure
    // ticks from the last pre-drop tick to the trough, vs ticks from the trough
    // back up to within 90% of the starting rate.
    let rates: Vec<u64> = ledbat.log.iter().map(|d| d.rate_after_bps).collect();
    let trough = *rates.iter().min().expect("non-empty trace");
    let trough_idx = rates
        .iter()
        .position(|&r| r == trough)
        .expect("trough is in the trace");
    let drop_start = rates[..trough_idx]
        .iter()
        .rposition(|&r| r >= starting)
        .unwrap_or(0);
    let drop_ticks = trough_idx - drop_start;
    let recover_to = (starting as f64 * 0.9) as u64;
    let recover_ticks = rates[trough_idx..]
        .iter()
        .position(|&r| r >= recover_to)
        .expect("LEDBAT++ should recover toward the ceiling within the run");
    assert!(
        recover_ticks > drop_ticks * 3,
        "death-spiral asymmetry not demonstrated: drop took {drop_ticks} ticks, \
         recovery took {recover_ticks} ticks",
    );

    // 4. Contrast: RfcDraft's cross-peer median rejects the single-peer outlier,
    // so it never changes the rate on the same input.
    let rfc = Replayer::new()
        .run_until(s.run_for)
        .run(s.events.clone().into_iter(), RfcDraft::default());
    assert_eq!(
        rfc.decisions_set, 0,
        "RfcDraft must hold on single_packet_loss (cross-peer median rejects the outlier)",
    );
    assert_eq!(rfc.min_rate_bps, starting);
    assert_eq!(rfc.max_rate_bps, starting);
}
