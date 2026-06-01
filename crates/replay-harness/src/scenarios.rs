//! Synthetic scenarios — programmatic RTT-sample streams that pin specific
//! controller behaviours.
//!
//! Each scenario:
//! - Is named (used in reports and the binary's `synthetic` subcommand).
//! - Builds an [`Vec<Event>`] of RTT samples (and optionally peer
//!   join/leave events).
//! - Carries an [`Expectation`] about what a sane controller MUST do
//!   (or MUST NOT do) on this input.
//!
//! Scenarios are run from the binary (`replay-harness synthetic <name>`)
//! and from `tests/scenarios_pin.rs`, which exercises every scenario
//! against every controller and asserts the expectations. Adding a new
//! scenario file in `scenarios/` and listing it in [`all_scenarios`] is
//! enough; no other wiring required.

use std::time::Duration;

use crate::event::Event;

pub mod correlated_inflation;
pub mod idle_steady_state;
pub mod single_peer_outlier;
pub mod small_n;

/// What a sane controller is expected to do on this scenario.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Expectation {
    /// The controller MUST NOT take any rate action across the run.
    /// Used for scenarios that simulate ambient overlay noise we don't
    /// want to react to (small-N, single-peer outlier, etc.).
    NeverFires,
    /// The controller MUST fire at least once during the run. Used for
    /// the scenarios that represent the cases the controller exists to
    /// detect (correlated inflation, reference tracking overlay).
    FiresAtLeastOnce,
    /// Used for scenarios where the right answer depends on the
    /// controller's design philosophy and we don't pin behaviour — the
    /// scenario is informational only, run for the decision trace.
    Informational,
}

#[derive(Debug, Clone)]
pub struct Scenario {
    pub name: &'static str,
    pub description: &'static str,
    pub expectation: Expectation,
    pub events: Vec<Event>,
    /// Suggested run duration. The replayer will tick past the last
    /// event by one interval, but synthetic scenarios often need the
    /// observation window to extend further (e.g. to confirm "controller
    /// fired" or "did NOT fire even after the suspect window").
    pub run_for: Duration,
}

pub fn all_scenarios() -> Vec<Scenario> {
    vec![
        idle_steady_state::scenario(),
        correlated_inflation::scenario(),
        single_peer_outlier::scenario(),
        small_n::scenario(),
    ]
}

/// Convenience to look up by name (for the CLI).
pub fn find(name: &str) -> Option<Scenario> {
    all_scenarios().into_iter().find(|s| s.name == name)
}
