use std::time::Duration;

pub(super) const SLOW_CONNECTION_EVALUATOR_WINDOW_DURATION: Duration = Duration::from_secs(5 * 60);
pub(super) const FAST_CONNECTION_EVALUATOR_WINDOW_DURATION: Duration = Duration::from_secs(60);
pub(super) const REQUEST_DENSITY_TRACKER_WINDOW_SIZE: usize = 10_000;
pub(super) const SOURCE_RAMP_UP_DURATION: Duration = Duration::from_secs(5 * 60);
pub(super) const OUTBOUND_REQUEST_COUNTER_WINDOW_SIZE: usize = 10000;
pub(super) const MINIMUM_DESIRED_RESOURCE_USAGE_PROPORTION: f64 = 0.5;
pub(super) const MAXIMUM_DESIRED_RESOURCE_USAGE_PROPORTION: f64 = 0.9;

/// Cap on how far above `min_connections` the low-bandwidth heuristic can grow.
///
/// When bandwidth usage is below `MINIMUM_DESIRED_RESOURCE_USAGE_PROPORTION`,
/// `adjust_topology` adds connections — but only up to `min_connections * factor`.
/// Beyond this cap, low bandwidth is assumed to reflect low demand (few contract
/// operations), not insufficient connections.
///
/// Without this cap, every peer in a low-activity network perpetually initiates
/// CONNECT operations every 60s tick, generating ~90% of all network traffic.
/// See: <https://github.com/freenet/freenet-core/issues/3630>
pub(super) const LOW_USAGE_CONNECTION_GROWTH_FACTOR: f64 = 2.0;

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

/// Floor value for expected gap calculations when the per-side point count is
/// very low or zero. Used in two contexts:
///
/// 1. When a half-ring is completely empty (side_count=0), the expected gap is
///    set to this value so that the gap ratio (1.0 / EXPECTED_GAP_FLOOR = 100)
///    is large enough to always trigger a swap.
///
/// 2. As a floor for ln(k) when k=1 (ln(1)=0 would make expected gap 0,
///    causing division issues).
pub(super) const EXPECTED_GAP_FLOOR: f64 = 0.01;
