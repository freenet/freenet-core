use std::time::Duration;

pub(super) const SLOW_CONNECTION_EVALUATOR_WINDOW_DURATION: Duration = Duration::from_secs(5 * 60);
pub(super) const FAST_CONNECTION_EVALUATOR_WINDOW_DURATION: Duration = Duration::from_secs(60);
pub(super) const REQUEST_DENSITY_TRACKER_WINDOW_SIZE: usize = 10_000;
pub(super) const SOURCE_RAMP_UP_DURATION: Duration = Duration::from_secs(5 * 60);
pub(super) const OUTBOUND_REQUEST_COUNTER_WINDOW_SIZE: usize = 10000;
pub(super) const MINIMUM_DESIRED_RESOURCE_USAGE_PROPORTION: f64 = 0.5;
pub(super) const MAXIMUM_DESIRED_RESOURCE_USAGE_PROPORTION: f64 = 0.9;

/// Minimum connections before switching from location-based to Kleinberg target selection.
/// Below this threshold, peers target their own location to build local neighborhoods.
/// At or above this threshold, peers use Kleinberg 1/d sampling for optimal ring coverage.
pub(super) const DENSITY_SELECTION_THRESHOLD: usize = 5;

/// Maximum probability of attempting a topology swap on any single maintenance tick.
///
/// At 60s ticks with max probability, this means at most ~one swap every 10 minutes
/// even when topology deviation is very high. During normal operation the actual
/// probability is proportional to the gap deviation and will be much lower.
pub(super) const MAX_SWAP_PROB_PER_TICK: f64 = 0.1;

/// Minimum connections before topology swaps are considered.
///
/// Below this threshold the node is still bootstrapping and shouldn't be replacing
/// connections — it should be adding them. Must be >= DENSITY_SELECTION_THRESHOLD
/// so gap analysis is meaningful.
pub(super) const MIN_CONNECTIONS_FOR_SWAP: usize = DENSITY_SELECTION_THRESHOLD;
