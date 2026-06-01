//! [`Controller`] trait and the view it sees each tick.

use std::time::Duration;

use crate::event::PeerKey;
use crate::rolling::RttSnapshot;

pub mod fixed_rate;
pub mod rfc_draft;

pub use fixed_rate::FixedRate;
pub use rfc_draft::RfcDraft;

/// Snapshot the harness hands to a controller at each tick.
#[derive(Debug, Clone)]
pub struct ControllerView<'a> {
    /// Wall-clock time of this tick (relative to scenario start / first
    /// event in OTLP replay).
    pub now: Duration,
    /// Per-peer snapshots, one entry per peer currently registered.
    /// Order is sorted by peer key so controllers are deterministic
    /// against the same input.
    pub per_peer: &'a [PeerObservation],
    /// Reference-path snapshot, if reference-ping events were observed.
    pub reference: Option<RttSnapshot>,
    /// Current aggregate outbound rate (bytes/sec). The controller's
    /// decisions are relative to this baseline.
    pub current_aggregate_rate_bps: u64,
}

#[derive(Debug, Clone)]
pub struct PeerObservation {
    pub peer: PeerKey,
    pub snapshot: RttSnapshot,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RateDecision {
    /// Leave the aggregate rate unchanged.
    Hold,
    /// Set the aggregate rate to a new value. `reason` is logged in the
    /// decision trace for post-hoc analysis.
    Set {
        aggregate_rate_bps: u64,
        reason: &'static str,
    },
}

impl RateDecision {
    pub fn is_hold(&self) -> bool {
        matches!(self, RateDecision::Hold)
    }
}

pub trait Controller {
    /// Stable name used in reports and assertions.
    fn name(&self) -> &str;

    /// Called once per tick (the harness drives ticks at the configured
    /// cadence — production uses 1 Hz).
    fn tick(&mut self, view: ControllerView<'_>) -> RateDecision;
}
