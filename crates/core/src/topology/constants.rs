use std::time::Duration;

pub(super) const SLOW_CONNECTION_EVALUATOR_WINDOW_DURATION: Duration = Duration::from_secs(5 * 60);
pub(super) const FAST_CONNECTION_EVALUATOR_WINDOW_DURATION: Duration = Duration::from_secs(60);
pub(super) const REQUEST_DENSITY_TRACKER_WINDOW_SIZE: usize = 10_000;
pub(super) const SOURCE_RAMP_UP_DURATION: Duration = Duration::from_secs(5 * 60);
pub(super) const OUTBOUND_REQUEST_COUNTER_WINDOW_SIZE: usize = 10000;
pub(super) const MINIMUM_DESIRED_RESOURCE_USAGE_PROPORTION: f64 = 0.5;
pub(super) const MAXIMUM_DESIRED_RESOURCE_USAGE_PROPORTION: f64 = 0.9;

/// Fraction of new connection targets that use density-weighted selection
/// (targeting locations with high request density) instead of pure Kleinberg.
///
/// When density data is available, each target has this probability of being
/// density-weighted. If density data is unavailable (e.g., during bootstrap),
/// all targets fall back to Kleinberg automatically.
///
/// This balances small-world routing reachability (Kleinberg 1/d) with
/// connection clustering near active contracts (density-weighted), which
/// is critical for subscription tree formation. See #3138.
pub(super) const DENSITY_TARGET_FRACTION: f64 = 0.5;
