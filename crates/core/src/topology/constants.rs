use std::time::Duration;

pub(super) const SLOW_CONNECTION_EVALUATOR_WINDOW_DURATION: Duration = Duration::from_secs(5 * 60);
pub(super) const FAST_CONNECTION_EVALUATOR_WINDOW_DURATION: Duration = Duration::from_secs(60);
pub(super) const REQUEST_DENSITY_TRACKER_WINDOW_SIZE: usize = 10_000;
pub(super) const SOURCE_RAMP_UP_DURATION: Duration = Duration::from_secs(5 * 60);
pub(super) const OUTBOUND_REQUEST_COUNTER_WINDOW_SIZE: usize = 10000;
pub(super) const MINIMUM_DESIRED_RESOURCE_USAGE_PROPORTION: f64 = 0.5;
pub(super) const MAXIMUM_DESIRED_RESOURCE_USAGE_PROPORTION: f64 = 0.9;

/// Minimum connections before switching from location-based to density-based target selection.
/// Below this threshold, peers target their own location to build local neighborhoods.
/// At or above this threshold, peers use density-based selection to optimize for request patterns.
pub(super) const DENSITY_SELECTION_THRESHOLD: usize = 5;
