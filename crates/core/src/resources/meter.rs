use std::hash::Hash;
use std::time::Duration;

use crate::resources::rate::Rate;
use dashmap::DashMap;
use freenet_stdlib::prelude::*;
use crate::resources::Bandwidth;

use crate::ring::PeerKeyLocation;

use super::running_average::RunningAverage;

/// A structure that keeps track of the usage of dynamic resources which are consumed over time.
/// It provides methods to report and query resource usage, both total and attributed to specific
/// sources.
pub(super) struct Meter {
    totals_by_resource: ResourceTotals,
    attribution_meters: AttributionMeters,
    running_average_window_size: usize,
}

impl Meter {
    /// Creates a new `Meter`.
    pub fn new_with_window_size(running_average_window_size: usize) -> Self {
        Meter {
            totals_by_resource: ResourceTotals::new(),
            attribution_meters: AttributionMeters::new(),
            running_average_window_size,
        }
    }

    pub fn resource_usage_rate(&self, resource: ResourceType) -> Option<Rate> {
        // Try to get a mutable reference to the Meter for the given resource
        match self.totals_by_resource.map.get_mut(&resource) {
            Some(meter) => {
                // Get the current measurement value
                meter.get_rate()
            }
            None => None, // No meter found for the given resource
        }
    }

    pub(crate) fn attributed_usage_rate(
        &self,
        attribution: &AttributionSource,
        resource: ResourceType,
    ) -> Option<Rate> {
        // Try to get a mutable reference to the AttributionMeters for the given attribution
        match self.attribution_meters.get_mut(attribution) {
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

    pub(crate) fn report_inbound_bandwidth(&mut self, attribution : &AttributionSource, bandwidth: Bandwidth) {
        self.report(attribution, ResourceType::InboundBandwidthBytes, bandwidth.into());
    }

    pub(crate) fn report_outbound_bandwidth(&mut self, attribution : &AttributionSource, bandwidth: Bandwidth) {
        self.report(attribution, ResourceType::OutboundBandwidthBytes, bandwidth.into());
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
        // Report the total usage for the resource
        let mut total_value = self
            .totals_by_resource
            .map
            .entry(resource)
            .or_insert_with(|| RunningAverage::new(self.running_average_window_size));
        total_value.insert(value);

        // Report the usage for a specific attribution
        let resource_map = self
            .attribution_meters
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
        let meter = Meter::new_with_window_size(100);

        // Test that the new Meter has empty totals_by_resource and attribution_meters
        assert!(meter.totals_by_resource.map.is_empty());
        assert!(meter.attribution_meters.is_empty());
    }

    #[test]
    fn test_meter_total_usage() {
        let mut meter = Meter::new_with_window_size(100);

        // Test that the total usage is 0.0 for all resources
        assert_eq!(
            meter
                .resource_usage_rate(ResourceType::InboundBandwidthBytes)
                .unwrap(),
            Rate::new(0.0, Duration::from_secs(1))
        );
        assert_eq!(
            meter
                .resource_usage_rate(ResourceType::OutboundBandwidthBytes)
                .unwrap(),
            Rate::new(0.0, Duration::from_secs(1))
        );
        assert_eq!(
            meter
                .resource_usage_rate(ResourceType::CpuInstructions)
                .unwrap(),
            Rate::new(0.0, Duration::from_secs(1))
        );

        // Report some usage and test that the total usage is updated
        let attribution = AttributionSource::Peer(PeerKeyLocation::random());
        meter.report(&attribution, ResourceType::InboundBandwidthBytes, 100.0);
        assert_eq!(
            meter
                .resource_usage_rate(ResourceType::InboundBandwidthBytes)
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
        assert_eq!(
            meter
                .attributed_usage_rate(&attribution, ResourceType::InboundBandwidthBytes)
                .unwrap()
                .per_second(),
            0.0
        );
        assert_eq!(
            meter
                .attributed_usage_rate(&attribution, ResourceType::OutboundBandwidthBytes)
                .unwrap()
                .per_second(),
            0.0
        );
        assert_eq!(
            meter
                .attributed_usage_rate(&attribution, ResourceType::CpuInstructions)
                .unwrap()
                .per_second(),
            0.0
        );

        // Report some usage and test that the attributed usage is updated
        meter.report(&attribution, ResourceType::InboundBandwidthBytes, 100.0);
        assert_eq!(
            meter
                .attributed_usage_rate(&attribution, ResourceType::InboundBandwidthBytes)
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
                .resource_usage_rate(ResourceType::InboundBandwidthBytes)
                .unwrap()
                .per_second(),
            100.0
        );
        assert_eq!(
            meter
                .attributed_usage_rate(&attribution, ResourceType::InboundBandwidthBytes)
                .unwrap()
                .per_second(),
            100.0
        );

        // Report more usage and test that the total and attributed usage are updated
        meter.report(&attribution, ResourceType::InboundBandwidthBytes, 200.0);
        assert_eq!(
            meter
                .resource_usage_rate(ResourceType::InboundBandwidthBytes)
                .unwrap()
                .per_second(),
            300.0
        );
        assert_eq!(
            meter
                .attributed_usage_rate(&attribution, ResourceType::InboundBandwidthBytes)
                .unwrap()
                .per_second(),
            300.0
        );
        // Report usage for a different resource and test that the total and attributed usage are updated
        meter.report(&attribution, ResourceType::CpuInstructions, 50.0);
        assert_eq!(
            meter
                .resource_usage_rate(ResourceType::CpuInstructions)
                .unwrap()
                .per_second(),
            50.0
        );
        assert_eq!(
            meter
                .attributed_usage_rate(&attribution, ResourceType::CpuInstructions)
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
                .resource_usage_rate(ResourceType::InboundBandwidthBytes)
                .unwrap()
                .per_second(),
            450.0
        );
        assert_eq!(
            meter
                .attributed_usage_rate(&other_attribution, ResourceType::InboundBandwidthBytes)
                .unwrap()
                .per_second(),
            150.0
        );
        Ok(())
    }
}
