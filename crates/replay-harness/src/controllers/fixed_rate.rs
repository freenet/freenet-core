//! `FixedRate` — the production default and the harness baseline.
//!
//! Never changes the aggregate rate. Useful as a sanity-check that the
//! harness itself works (every scenario should observe zero rate changes
//! against this controller), and as a comparison point in `compare` runs.

use super::{Controller, ControllerView, RateDecision};

#[derive(Debug, Default)]
pub struct FixedRate;

impl Controller for FixedRate {
    fn name(&self) -> &str {
        "fixed_rate"
    }

    fn tick(&mut self, _view: ControllerView<'_>) -> RateDecision {
        RateDecision::Hold
    }
}
