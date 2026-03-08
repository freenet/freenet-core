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

/// Weight of topology value in the composite pruning score.
///
/// The composite score is: `normalized_routing + TOPOLOGY_WEIGHT * topology_value`.
/// Routing values are normalized to [0, 1] (relative to max among peers).
/// Topology values center around 1.0 (removal_gap / expected_gap).
/// With β=1.0, both components have comparable magnitude.
/// Increase to favor topology preservation; decrease to favor traffic-based optimization.
pub(super) const TOPOLOGY_WEIGHT: f64 = 1.0;

/// Topology protection threshold: connections whose removal would create a gap
/// larger than this multiple of the expected gap are never pruned (unless all
/// peers are critical, in which case a fallback picks the least important).
///
/// With k connections on [0,1], the expected removal gap is 2/(k+1).
/// A topology_value of 2.0 means the gap would be 2× expected — this peer
/// fills a critical position in the Kleinberg distribution.
pub(super) const TOPOLOGY_PROTECTION_THRESHOLD: f64 = 2.0;
