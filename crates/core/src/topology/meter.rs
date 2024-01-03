use std::collections::HashMap;
use std::hash::Hash;
use std::time::{Duration, Instant};

use dashmap::DashMap;
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
    cached_estimated_usage_rate: DashMap<ResourceType, (Rate, Instant)>,
}

impl Meter {
    /// Creates a new `Meter`.
    pub fn new_with_window_size(running_average_window_size: usize) -> Self {
        Meter {
            attribution_meters: AttributionMeters::new(),
            running_average_window_size,
            cached_estimated_usage_rate: DashMap::new(),
        }
    }

    /// The measured usage rate for a resource attributed to a specific source.
    pub(crate) fn attributed_usage_rate(
        &self,
        attribution: &AttributionSource,
        resource: &ResourceType,
        at_time: Instant,
    ) -> Option<Rate> {
        match self.attribution_meters.get(attribution) {
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
        if let Some(cached) = self.cached_estimated_usage_rate.get(resource) {
            let (cached_rate, cached_time) = cached.value();
            if at_time - *cached_time <= ESTIMATED_USAGE_RATE_CACHE_TIME {
                return Some(*cached_rate);
            }
        }

        match self.calculate_estimated_usage_rate(resource, at_time) {
            Some(estimated_usage_rate) => {
                self.cached_estimated_usage_rate
                    .insert(*resource, (estimated_usage_rate, at_time));
                Some(estimated_usage_rate)
            }
            None => None,
        }
    }

    /// Returns a HashMap of AttributionSource to Rate of the usage rate for
    /// each attribution source. This does not adjust the usage rate for sources
    /// that are ramping up.
    pub(crate) fn get_usage_rates(
        &self,
        resource: &ResourceType,
        at_time: Instant,
    ) -> HashMap<AttributionSource, Rate> {
        let mut rates = HashMap::new();

        for attribution_meter in self.attribution_meters.iter() {
            let attribution = attribution_meter.key();
            if let Some(rate) = self.attributed_usage_rate(attribution, resource, at_time) {
                rates.insert(attribution.clone(), rate);
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
        &mut self,
        resource: &ResourceType,
        at_time: Instant,
    ) -> Option<Rate> {
        let rates: Vec<Rate> = self
            .attribution_meters
            .iter()
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
    #[allow(dead_code)] // fixme: use this
    pub(crate) fn report(
        &mut self,
        attribution: &AttributionSource,
        resource: ResourceType,
        value: f64,
        at_time: Instant,
    ) {
        // Report the usage for a specific attribution
        let resource_map = self
            .attribution_meters
            .entry(attribution.clone())
            .or_insert_with(ResourceTotals::new);
        let mut resource_value = resource_map
            .map
            .entry(resource)
            .or_insert_with(|| RunningAverage::new(self.running_average_window_size));
        resource_value.insert_with_time(at_time, value);
    }
}

#[allow(dead_code)] // todo use this
#[derive(Eq, Hash, PartialEq, Clone, Debug)]
pub(crate) enum AttributionSource {
    Peer(PeerKeyLocation),
    Delegate(DelegateKey),
}

#[derive(Eq, Hash, PartialEq, Clone, Copy, Debug)]
pub(crate) enum ResourceType {
    InboundBandwidthBytes,
    OutboundBandwidthBytes,
}

impl ResourceType {
    pub(crate) fn all() -> [ResourceType; 2] {
        [
            ResourceType::InboundBandwidthBytes,
            ResourceType::OutboundBandwidthBytes,
        ]
    }
}

type AttributionMeters = DashMap<AttributionSource, ResourceTotals>;

/// A structure that holds running averages of resource usage for different resource types.
struct ResourceTotals {
    pub map: DashMap<ResourceType, RunningAverage>,
}

impl ResourceTotals {
    fn new() -> Self {
        ResourceTotals {
            map: DashMap::new(),
        }
    }
}

// Tests
#[cfg(test)]
mod tests {
    use crate::DynError;

    use super::*;

    #[test]
    fn test_empty_meter() {
        let meter = Meter::new_with_window_size(100);

        assert!(meter
            .attributed_usage_rate(
                &AttributionSource::Peer(PeerKeyLocation::random()),
                &ResourceType::InboundBandwidthBytes,
                Instant::now(),
            )
            .is_none());
        assert!(meter.attribution_meters.is_empty());
    }

    #[test]
    fn test_meter_attributed_usage() {
        let mut meter = Meter::new_with_window_size(100);

        // Test that the attributed usage is 0.0 for all resources
        let attribution = AttributionSource::Peer(PeerKeyLocation::random());
        assert!(meter
            .attributed_usage_rate(
                &attribution,
                &ResourceType::InboundBandwidthBytes,
                Instant::now()
            )
            .is_none());
        assert!(meter
            .attributed_usage_rate(
                &attribution,
                &ResourceType::OutboundBandwidthBytes,
                Instant::now()
            )
            .is_none());

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
    fn test_meter_report() -> Result<(), DynError> {
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
