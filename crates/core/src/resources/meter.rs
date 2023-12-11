use std::hash::Hash;
use std::time::{Duration, Instant};

use crate::resources::rate::Rate;
use dashmap::DashMap;
use itertools::Itertools;
use freenet_stdlib::prelude::*;

use crate::ring::PeerKeyLocation;

use super::running_average::RunningAverage;

// Sources younger than this will return default usage rather than actual usage as it
// may be ramping up and not yet fully representative of its long-term usage.
const SOURCE_RAMP_UP_TIME: Duration = Duration::from_secs(10 * 60); // 10 minutes

// Default usage is assumed to be the 50th percentile of usage for the resource.
const DEFAULT_USAGE_PERCENTILE: f64 = 0.5;

// Recache the estimated usage rate this often
const ESTIMATED_USAGE_RATE_CACHE_TIME: Duration = Duration::from_secs(60);

/// A structure that keeps track of the usage of dynamic resources which are consumed over time.
/// It provides methods to report and query resource usage, both total and attributed to specific
/// sources.
pub(super) struct Meter {
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

<<<<<<< HEAD
=======
    // TODO: This needs to move out of Meter and into Ring because it needs source.created_at time

    /// Get the sum of attributed usage rates for all resources of type `resource`, using attributed_usage_rate
    /// or extrapolated_usage_rate depending on the value of extrapolated
    pub fn resource_usage_rate(&mut self, resource: ResourceType, extrapolated : bool) -> Option<Rate> {
        let mut total = Rate::new(0.0, Duration::from_secs(1));
        for (source, totals) in *self.attribution_meters.get(&resource)? {
            let usage_rate = if extrapolated {
                self.extrapolated_usage_rate(&source, source.created_at(), &resource, &Instant::now())
            } else {
                self.attributed_usage_rate(&source, &resource)
            };
            if let Some(usage_rate) = usage_rate {
                total += usage_rate;
            }

        }
        Some(total)
    }

>>>>>>> c2215e60a7b73bc09af93c7c19a6d8bb9bc5f796
    /// The measured usage rate for a resource attributed to a specific source.
    pub(crate) fn attributed_usage_rate(
        &self,
        attribution: &AttributionSource,
        resource: &ResourceType,
    ) -> Option<Rate> {
        // Try to get a mutable reference to the AttributionMeters for the given attribution
        match self.attribution_meters.get_mut(&resource).get_mut(attribution) {
            Some(attribution_meters) => {
                // Try to get a mutable reference to the Meter for the given resource
                match attribution_meters.map.get_mut(&resource) {
                    Some(meter) => {
                        // Get the current measurement value
                        meter.get_rate()
                    }
                    None => Some(Rate::new(0.0, Duration::from_secs(1))), // No meter found for the given resource
                }
            }
            None => None, // No AttributionMeters found for the given attribution
        }
    }

    /// Get the expected usage rate for a resource based on the usage rates of all attribution
    /// sources.
    pub(crate) fn estimated_type_usage_rate(
        &mut self,
        resource: &ResourceType,
        now: &Instant,
    ) -> Option<Rate> {
        let should_update_cache = match self.cached_estimated_usage_rate.get(resource) {
            Some(eur) if now.duration_since(eur.value().1) > ESTIMATED_USAGE_RATE_CACHE_TIME => {
                true
            }
            None => true,
            _ => false,
        };

        if should_update_cache {
            let new_rate = self.update_cached_estimated_usage_rate(resource);
            if let Some(rate) = new_rate {
                self.cached_estimated_usage_rate
                    .insert(*resource, (rate.clone(), *now));
                return Some(rate);
            }
        }

        self.cached_estimated_usage_rate
            .get(resource)
            .map(|eur| eur.value().0)
    }

    fn update_cached_estimated_usage_rate(&self, resource: &ResourceType) -> Option<Rate> {

        let mut totals_for_type : Vec<Rate> = self.attribution_meters
            .get(&resource)?
            .iter()
            .map(|t| t.value().filter_map(|m| m.value().get_rate()))
            .sorted()
            .collect();

        // Calculate the estimated usage rate as the 50th percentile of usage for the resource
        if !totals_for_type.is_empty() {
            let estimated_index = (DEFAULT_USAGE_PERCENTILE * totals_for_type.len() as f64) as usize;
            let estimated_index = estimated_index.min(totals_for_type.len() - 1);
            Some(totals_for_type[estimated_index].clone())
        } else {
            None
        }
    }

    /// Report the use of a resource with multiple attribution sources, splitting the usage
    /// evenly between the sources.
    /// This should be done in the lowest-level functions that consume the resource, taking
    /// an AttributionMeter as a parameter. This will be useful for contracts with multiple
    /// subscribers - where the responsibility should be split evenly among the subscribers.
    pub(crate) fn report_split(
        &mut self,
        attributions: &[AttributionSource],
        resource: ResourceType,
        value: f64,
    ) {
        let split_value = value / attributions.len() as f64;
        for attribution in attributions {
            self.report(attribution, resource, split_value);
        }
    }

    /// Report the use of a resource. This should be done in the lowest-level
    /// functions that consume the resource, taking an AttributionMeter
    /// as a parameter.
    pub(crate) fn report(
        &mut self,
        attribution: &AttributionSource,
        resource: ResourceType,
        value: f64,
    ) {

        // Report the usage for a specific attribution
        let resource_map = self
            .attribution_meters
            .get(&resource)
            .entry(attribution.clone())
            .or_insert_with(ResourceTotals::new);
        let mut resource_value = resource_map
            .map
            .entry(resource)
            .or_insert_with(|| RunningAverage::new(self.running_average_window_size));
        resource_value.insert(value);
    }
}

#[derive(Eq, Hash, PartialEq, Clone, Debug)]
pub(crate) enum AttributionSource {
    Peer(PeerKeyLocation),
    Delegate(DelegateKey),
}

#[derive(Eq, Hash, PartialEq, Clone, Copy, Debug)]
pub enum ResourceType {
    InboundBandwidthBytes,
    OutboundBandwidthBytes,
    CpuInstructions,
}

pub const ALL_RESOURCE_TYPES: [ResourceType; 3] = [
    ResourceType::InboundBandwidthBytes,
    ResourceType::OutboundBandwidthBytes,
    ResourceType::CpuInstructions,
];

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
    use std::time::Duration;

    use super::*;

    #[test]
    fn test_meter() {
        let mut meter = Meter::new_with_window_size(100);

        // Test that the new Meter has empty totals_by_resource and attribution_meters
        assert!(meter.resource_usage_rate(ResourceType::InboundBandwidthBytes, false).is_none());
        assert!(meter.attribution_meters.is_empty());
    }

    #[test]
    fn test_meter_total_usage() {
        let mut meter = Meter::new_with_window_size(100);

        // Test that the total usage is 0.0 for all resources
        assert!(meter
            .resource_usage_rate(ResourceType::InboundBandwidthBytes, false)
            .is_none());
        assert!(meter
            .resource_usage_rate(ResourceType::OutboundBandwidthBytes, false)
            .is_none());
        assert!(meter
            .resource_usage_rate(ResourceType::CpuInstructions, false)
            .is_none());

        // Report some usage and test that the total usage is updated
        let attribution = AttributionSource::Peer(PeerKeyLocation::random());
        meter.report(&attribution, ResourceType::InboundBandwidthBytes, 100.0);
        assert_eq!(
            meter
                .resource_usage_rate(ResourceType::InboundBandwidthBytes, false)
                .unwrap()
                .per_second(),
            100.0
        );
    }

    #[test]
    fn test_meter_attributed_usage() {
        let mut meter = Meter::new_with_window_size(100);

        // Test that the attributed usage is 0.0 for all resources
        let attribution = AttributionSource::Peer(PeerKeyLocation::random());
        assert!(meter
            .attributed_usage_rate(&attribution, &ResourceType::InboundBandwidthBytes)
            .is_none());
        assert!(meter
            .attributed_usage_rate(&attribution, &ResourceType::OutboundBandwidthBytes)
            .is_none());
        assert!(meter
            .attributed_usage_rate(&attribution, &ResourceType::CpuInstructions)
            .is_none());

        // Report some usage and test that the attributed usage is updated
        meter.report(&attribution, ResourceType::InboundBandwidthBytes, 100.0);
        assert_eq!(
            meter
                .attributed_usage_rate(&attribution, &ResourceType::InboundBandwidthBytes)
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
        meter.report(&attribution, ResourceType::InboundBandwidthBytes, 100.0);
        assert_eq!(
            meter
                .resource_usage_rate(ResourceType::InboundBandwidthBytes, false)
                .unwrap()
                .per_second(),
            100.0
        );
        assert_eq!(
            meter
                .attributed_usage_rate(&attribution, &ResourceType::InboundBandwidthBytes)
                .unwrap()
                .per_second(),
            100.0
        );

        // Report more usage and test that the total and attributed usage are updated
        meter.report(&attribution, ResourceType::InboundBandwidthBytes, 200.0);
        assert_eq!(
            meter
                .resource_usage_rate(ResourceType::InboundBandwidthBytes, false)
                .unwrap()
                .per_second(),
            300.0
        );
        assert_eq!(
            meter
                .attributed_usage_rate(&attribution, &ResourceType::InboundBandwidthBytes)
                .unwrap()
                .per_second(),
            300.0
        );
        // Report usage for a different resource and test that the total and attributed usage are updated
        meter.report(&attribution, ResourceType::CpuInstructions, 50.0);
        assert_eq!(
            meter
                .resource_usage_rate(ResourceType::CpuInstructions, false)
                .unwrap()
                .per_second(),
            50.0
        );
        assert_eq!(
            meter
                .attributed_usage_rate(&attribution, &ResourceType::CpuInstructions)
                .unwrap()
                .per_second(),
            50.0
        );

        // Report usage for a different attribution and test that the total and attributed usage are updated
        let other_attribution = AttributionSource::Peer(PeerKeyLocation::random());
        meter.report(
            &other_attribution,
            ResourceType::InboundBandwidthBytes,
            150.0,
        );
        assert_eq!(
            meter
                .resource_usage_rate(ResourceType::InboundBandwidthBytes, false)
                .unwrap()
                .per_second(),
            450.0
        );
        assert_eq!(
            meter
                .attributed_usage_rate(&other_attribution, &ResourceType::InboundBandwidthBytes)
                .unwrap()
                .per_second(),
            150.0
        );
        Ok(())
    }
}
