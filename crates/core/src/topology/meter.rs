use std::collections::BTreeMap;
use std::sync::RwLock;
use std::time::Duration;
use tokio::time::Instant;

use freenet_stdlib::prelude::*;

use crate::ring::PeerKeyLocation;
use crate::topology::rate::Rate;

use super::running_average::RunningAverage;

// Default usage is assumed to be the 50th percentile of usage for the resource.
const DEFAULT_USAGE_PERCENTILE: f64 = 0.5;

// Recache the estimated usage rate this often
const ESTIMATED_USAGE_RATE_CACHE_TIME: Duration = Duration::from_secs(60);

/// A structure that keeps track of the usage of dynamic resources which are consumed over time.
/// It provides methods to report and query resource usage, both total and attributed to specific
/// sources.
pub(crate) struct Meter {
    attribution_meters: AttributionMeters,
    running_average_window_size: usize,
    cached_estimated_usage_rate: RwLock<BTreeMap<ResourceType, (Rate, Instant)>>,
}

impl Meter {
    /// Creates a new `Meter`.
    pub fn new_with_window_size(running_average_window_size: usize) -> Self {
        Meter {
            attribution_meters: RwLock::new(BTreeMap::new()),
            running_average_window_size,
            cached_estimated_usage_rate: RwLock::new(BTreeMap::new()),
        }
    }

    /// The measured usage rate for a resource attributed to a specific source.
    pub(crate) fn attributed_usage_rate(
        &self,
        attribution: &AttributionSource,
        resource: &ResourceType,
        at_time: Instant,
    ) -> Option<Rate> {
        let meters = self.attribution_meters.read().unwrap();
        match meters.get(attribution) {
            Some(attribution_meters) => {
                match attribution_meters.map.get(resource) {
                    Some(meter) => {
                        // Get the current measurement value
                        meter.get_rate_at_time(at_time)
                    }
                    None => Some(Rate::new(0.0, Duration::from_secs(1))), // No meter found for the given resource
                }
            }
            None => None, // No AttributionMeters found for the given attribution
        }
    }

    /// Returns the estimated usage rate for a resource of a given type.
    ///
    /// This function uses a percentile defined by `DEFAULT_USAGE_PERCENTILE` to estimate the usage rate
    /// for resources with unknown usage. It caches the estimated rates and refreshes them every
    /// `ESTIMATED_USAGE_RATE_CACHE_TIME` duration to avoid frequent recalculations.
    ///
    /// # Arguments
    ///
    /// * `resource` - A reference to the type of resource for which the usage rate is estimated.
    /// * `now` - The current `Instant` used to determine if the cached value is still valid.
    ///
    /// # Returns
    ///
    /// An `Option<Rate>` which is `Some(rate)` if an estimated rate is available, or `None` if it can't be determined.
    pub(crate) fn get_adjusted_usage_rate(
        &mut self,
        resource: &ResourceType,
        at_time: Instant,
    ) -> Option<Rate> {
        {
            let cache = self.cached_estimated_usage_rate.read().unwrap();
            if let Some((cached_rate, cached_time)) = cache.get(resource) {
                if at_time - *cached_time <= ESTIMATED_USAGE_RATE_CACHE_TIME {
                    return Some(*cached_rate);
                }
            }
        }

        match self.calculate_estimated_usage_rate(resource, at_time) {
            Some(estimated_usage_rate) => {
                let mut cache = self.cached_estimated_usage_rate.write().unwrap();
                cache.insert(*resource, (estimated_usage_rate, at_time));
                Some(estimated_usage_rate)
            }
            None => None,
        }
    }

    /// Returns a BTreeMap of AttributionSource to Rate of the usage rate for
    /// each attribution source. This does not adjust the usage rate for sources
    /// that are ramping up.
    pub(crate) fn get_usage_rates(
        &self,
        resource: &ResourceType,
        at_time: Instant,
    ) -> BTreeMap<AttributionSource, Rate> {
        let mut rates = BTreeMap::new();
        let meters = self.attribution_meters.read().unwrap();

        for (attribution, attribution_meter) in meters.iter() {
            if let Some(meter) = attribution_meter.map.get(resource) {
                if let Some(rate) = meter.get_rate_at_time(at_time) {
                    rates.insert(attribution.clone(), rate);
                }
            }
        }

        rates
    }

    /// Estimates the usage rate for a given resource type based on existing data.
    ///
    /// This function calculates the estimated usage rate by taking the 50th percentile value (or another
    /// specified percentile defined by [DEFAULT_USAGE_PERCENTILE]) from the set of known rates for the
    /// specified resource type. It disregards resources with no known rate (which may leader to
    /// higher estimates).
    ///
    /// # Arguments
    ///
    /// * `resource` - A reference to the resource type for which the usage rate is to be estimated.
    ///
    /// # Returns
    ///
    /// An `Option<Rate>` which is `Some(rate)` if an estimated rate can be determined from available data,
    /// or `None` if no data is available for the given resource type.
    ///
    /// # Panics
    ///
    /// This function may panic if `DEFAULT_USAGE_PERCENTILE` is set to an invalid value that is not within
    /// the range [0.0, 1.0].
    fn calculate_estimated_usage_rate(
        &self,
        resource: &ResourceType,
        at_time: Instant,
    ) -> Option<Rate> {
        let meters = self.attribution_meters.read().unwrap();
        let rates: Vec<Rate> = meters
            .values()
            // Filter out resources with no Rate and collect their rates
            .filter_map(|t| {
                t.map
                    .get(resource)
                    .and_then(|m| m.get_rate_at_time(at_time))
            })
            .collect();

        if rates.is_empty() {
            return None;
        }

        // Sort the collected rates
        let mut sorted_rates = rates;
        sorted_rates.sort_unstable(); // Using sort_unstable for potentially better performance

        // Calculate the index for the estimated usage rate
        let percentile_index =
            (DEFAULT_USAGE_PERCENTILE * sorted_rates.len() as f64).round() as usize;
        let estimated_index = percentile_index.min(sorted_rates.len().saturating_sub(1));

        sorted_rates.get(estimated_index).cloned()
    }

    /// Report the use of a resource. This should be done in the lowest-level
    /// functions that consume the resource, taking an AttributionMeter
    /// as a parameter.
    pub(crate) fn report(
        &mut self,
        attribution: &AttributionSource,
        resource: ResourceType,
        value: f64,
        at_time: Instant,
    ) {
        // Report the usage for a specific attribution
        let mut meters = self.attribution_meters.write().unwrap();
        let resource_map = meters
            .entry(attribution.clone())
            .or_insert_with(ResourceTotals::new);
        let resource_value = resource_map
            .map
            .entry(resource)
            .or_insert_with(|| RunningAverage::new(self.running_average_window_size));
        resource_value.insert_with_time(at_time, value);
    }
}

/// What a resource sample is attributed to.
///
/// Peer and Delegate variants are the original cost-attribution targets.
/// Contract was added as part of contract-hardening: every WASM call,
/// state write, broadcast, and message decode that has a `ContractInstanceId`
/// in scope can attribute its cost both to the originating peer AND to
/// the contract, so the per-contract governance scoring can run on the
/// same meter infrastructure that previously only fed peer-side
/// load-shedding.
///
/// See `docs/design/contract-hardening.md` — "Shared governance module".
#[allow(dead_code)] // variants constructed incrementally as reporters are wired up
#[derive(Eq, Hash, PartialEq, Clone, Debug)]
pub(crate) enum AttributionSource {
    Peer(PeerKeyLocation),
    Delegate(DelegateKey),
    Contract(ContractInstanceId),
}

impl PartialOrd for AttributionSource {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl AttributionSource {
    /// Whether this source can plausibly contribute samples to the given
    /// resource type. Used to filter the `source_creation_times`
    /// iteration in `topology::extrapolated_usage` so a Contract source
    /// (which never produces bandwidth samples) doesn't get a phantom
    /// non-zero bandwidth rate synthesized for it during its 5-min
    /// ramp-up window — that synthesized rate would otherwise inflate
    /// the topology's perceived bandwidth usage and trigger spurious
    /// connection removals every time a contract is reported.
    pub(crate) fn contributes_to(&self, resource: &ResourceType) -> bool {
        use AttributionSource::*;
        use ResourceType::*;
        // Enumerate every (source, resource) pair explicitly so a future
        // ResourceType variant fails the match exhaustiveness check
        // instead of silently falling into a default. Codex re-reviewer
        // of PR #4260 flagged the `_ => false` wildcard as a foot-gun:
        // a new resource added without revisiting this predicate would
        // silently treat every source as non-contributing to it.
        match (self, resource) {
            // Peer sources produce the bandwidth samples that drive
            // topology load-shedding.
            (Peer(_), InboundBandwidthBytes) => true,
            (Peer(_), OutboundBandwidthBytes) => true,
            (Peer(_), ExecCpuMicros) => false,
            (Peer(_), ExecFuelUnits) => false,
            (Peer(_), StateBytesWritten) => false,
            (Peer(_), BroadcastFanoutCost) => false,
            // Delegate sources predate this PR; their existing usage
            // pattern is bandwidth-relevant for accounting purposes.
            (Delegate(_), InboundBandwidthBytes) => true,
            (Delegate(_), OutboundBandwidthBytes) => true,
            (Delegate(_), ExecCpuMicros) => false,
            (Delegate(_), ExecFuelUnits) => false,
            (Delegate(_), StateBytesWritten) => false,
            (Delegate(_), BroadcastFanoutCost) => false,
            // Contract sources contribute the four contract-governance
            // resource types (CPU, fuel, state-bytes, fan-out cost) and
            // NEVER bandwidth — those are peer-attributed even when the
            // contract is the originator.
            (Contract(_), InboundBandwidthBytes) => false,
            (Contract(_), OutboundBandwidthBytes) => false,
            (Contract(_), ExecCpuMicros) => true,
            (Contract(_), ExecFuelUnits) => true,
            (Contract(_), StateBytesWritten) => true,
            (Contract(_), BroadcastFanoutCost) => true,
        }
    }
}

impl Ord for AttributionSource {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        // Variant discriminant defines the cross-variant ordering;
        // intra-variant comparisons use the inner key's natural ordering
        // (or its Debug formatting where the inner type doesn't implement
        // Ord — DelegateKey today, kept for cross-variant compat).
        fn rank(source: &AttributionSource) -> u8 {
            match source {
                AttributionSource::Peer(_) => 0,
                AttributionSource::Delegate(_) => 1,
                AttributionSource::Contract(_) => 2,
            }
        }
        match (self, other) {
            (AttributionSource::Peer(a), AttributionSource::Peer(b)) => a.cmp(b),
            (AttributionSource::Delegate(a), AttributionSource::Delegate(b)) => {
                // DelegateKey doesn't implement Ord; fall back to Debug.
                format!("{:?}", a).cmp(&format!("{:?}", b))
            }
            (AttributionSource::Contract(a), AttributionSource::Contract(b)) => a.cmp(b),
            (a, b) => rank(a).cmp(&rank(b)),
        }
    }
}

/// What kind of resource was consumed.
///
/// The first two variants (Inbound/OutboundBandwidthBytes) are the
/// peer-side cost dimensions used by `topology::adjust_topology` for
/// connection load-shedding.
///
/// The remaining variants were added for per-contract governance scoring:
/// CPU and fuel from WASM execution, on-disk state-write volume, and the
/// `Σ(subscriber × per-emit cost)` for state broadcast fan-out. Each is
/// reported alongside the corresponding `AttributionSource::Contract`
/// entry from the executor / runtime / broadcast pipeline.
#[derive(Eq, Hash, PartialEq, PartialOrd, Ord, Clone, Copy, Debug)]
pub(crate) enum ResourceType {
    InboundBandwidthBytes,
    OutboundBandwidthBytes,
    ExecCpuMicros,
    ExecFuelUnits,
    StateBytesWritten,
    BroadcastFanoutCost,
}

impl ResourceType {
    /// Resource types that participate in topology-side bandwidth
    /// capacity decisions (see `Limits::get` and
    /// `calculate_usage_proportion`). Non-bandwidth resources (CPU /
    /// fuel / state / fanout) are NOT included here: they are tracked
    /// by the meter for contract-governance purposes but have no
    /// rate-ceiling style limit configured.
    pub(crate) fn all() -> [ResourceType; 2] {
        [
            ResourceType::InboundBandwidthBytes,
            ResourceType::OutboundBandwidthBytes,
        ]
    }

    /// Every resource type the meter understands, including non-bandwidth
    /// resources added for contract governance.
    #[allow(dead_code)] // wired up incrementally by per-resource-type reporters
    pub(crate) fn all_tracked() -> [ResourceType; 6] {
        [
            ResourceType::InboundBandwidthBytes,
            ResourceType::OutboundBandwidthBytes,
            ResourceType::ExecCpuMicros,
            ResourceType::ExecFuelUnits,
            ResourceType::StateBytesWritten,
            ResourceType::BroadcastFanoutCost,
        ]
    }
}

type AttributionMeters = RwLock<BTreeMap<AttributionSource, ResourceTotals>>;

/// A structure that holds running averages of resource usage for different resource types.
struct ResourceTotals {
    pub map: BTreeMap<ResourceType, RunningAverage>,
}

impl ResourceTotals {
    fn new() -> Self {
        ResourceTotals {
            map: BTreeMap::new(),
        }
    }
}

// Tests
#[cfg(test)]
mod tests {

    use super::*;

    #[test]
    fn test_empty_meter() {
        let meter = Meter::new_with_window_size(100);

        assert!(
            meter
                .attributed_usage_rate(
                    &AttributionSource::Peer(PeerKeyLocation::random()),
                    &ResourceType::InboundBandwidthBytes,
                    Instant::now(),
                )
                .is_none()
        );
        assert!(meter.attribution_meters.read().unwrap().is_empty());
    }

    #[test]
    fn test_meter_attributed_usage() {
        let mut meter = Meter::new_with_window_size(100);

        // Test that the attributed usage is 0.0 for all resources
        let attribution = AttributionSource::Peer(PeerKeyLocation::random());
        assert!(
            meter
                .attributed_usage_rate(
                    &attribution,
                    &ResourceType::InboundBandwidthBytes,
                    Instant::now()
                )
                .is_none()
        );
        assert!(
            meter
                .attributed_usage_rate(
                    &attribution,
                    &ResourceType::OutboundBandwidthBytes,
                    Instant::now()
                )
                .is_none()
        );

        // Report some usage and test that the attributed usage is updated
        meter.report(
            &attribution,
            ResourceType::InboundBandwidthBytes,
            100.0,
            Instant::now(),
        );
        assert_eq!(
            meter
                .attributed_usage_rate(
                    &attribution,
                    &ResourceType::InboundBandwidthBytes,
                    Instant::now()
                )
                .unwrap()
                .per_second(),
            100.0
        );
    }

    #[test]
    fn test_meter_report() -> anyhow::Result<()> {
        let mut meter = Meter::new_with_window_size(100);

        // Report some usage and test that the total and attributed usage are updated
        let attribution = AttributionSource::Peer(PeerKeyLocation::random());
        meter.report(
            &attribution,
            ResourceType::InboundBandwidthBytes,
            100.0,
            Instant::now(),
        );
        assert_eq!(
            meter
                .attributed_usage_rate(
                    &attribution,
                    &ResourceType::InboundBandwidthBytes,
                    Instant::now()
                )
                .unwrap()
                .per_second(),
            100.0
        );

        // Report more usage and test that the total and attributed usage are updated
        meter.report(
            &attribution,
            ResourceType::InboundBandwidthBytes,
            200.0,
            Instant::now(),
        );
        assert_eq!(
            meter
                .attributed_usage_rate(
                    &attribution,
                    &ResourceType::InboundBandwidthBytes,
                    Instant::now()
                )
                .unwrap()
                .per_second(),
            300.0
        );

        // Report usage for a different attribution and test that the total and attributed usage are updated
        let other_attribution = AttributionSource::Peer(PeerKeyLocation::random());
        meter.report(
            &other_attribution,
            ResourceType::InboundBandwidthBytes,
            150.0,
            Instant::now(),
        );
        assert_eq!(
            meter
                .attributed_usage_rate(
                    &other_attribution,
                    &ResourceType::InboundBandwidthBytes,
                    Instant::now()
                )
                .unwrap()
                .per_second(),
            150.0
        );
        Ok(())
    }
}
