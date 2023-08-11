use std::hash::Hash;

use dashmap::DashMap;
use locutus_runtime::{ContractInstanceId, DelegateKey};

use crate::ring::PeerKeyLocation;

use super::running_average::RunningAverage;

const RUNNING_AVERAGE_WINDOW_SIZE: usize = 20;

/// A structure that keeps track of the usage of dynamic resources which are consumed over time.
/// It provides methods to report and query resource usage, both total and attributed to specific sources.
pub struct Meter {
    totals_by_resource: ResourceTotals,
    attribution_meters: AttributionMeters,
}

impl Meter {
    /// Creates a new `Meter`.
    pub fn new() -> Self {
        Meter {
            totals_by_resource: ResourceTotals::new(),
            attribution_meters: AttributionMeters::new(),
        }
    }

    pub fn total_usage(&self, resource: ResourceType) -> f64 {
        // Try to get a mutable reference to the Meter for the given resource
        match self.totals_by_resource.map.get_mut(&resource) {
            Some(meter) => {
                // Get the current measurement value
                meter.per_second_measurement()
            }
            None => 0.0, // No meter found for the given resource
        }
    }

    pub(crate) fn attributed_usage(
        &self,
        attribution: &AttributionSource,
        resource: ResourceType,
    ) -> f64 {
        // Try to get a mutable reference to the AttributionMeters for the given attribution
        match self.attribution_meters.get_mut(attribution) {
            Some(attribution_meters) => {
                // Try to get a mutable reference to the Meter for the given resource
                match attribution_meters.map.get_mut(&resource) {
                    Some(meter) => {
                        // Get the current measurement value
                        meter.per_second_measurement()
                    }
                    None => 0.0, // No meter found for the given resource
                }
            }
            None => 0.0, // No AttributionMeters found for the given attribution
        }
    }

    /// Report the use of a resource. This should be done in the lowest-level
    /// functions that consume the resource, taking an AttributionMeter
    /// as a parameter.
    pub(crate) fn report(
        &self,
        attribution: &AttributionSource,
        resource: ResourceType,
        value: f64,
    ) {
        // Report the total usage for the resource
        let mut total_value = self
            .totals_by_resource
            .map
            .entry(resource)
            .or_insert_with(|| RunningAverage::new(RUNNING_AVERAGE_WINDOW_SIZE));
        total_value.insert(value);

        // Report the usage for a specific attribution
        let resource_map = self
            .attribution_meters
            .entry(attribution.clone())
            .or_insert_with(ResourceTotals::new);
        let mut resource_value = resource_map
            .map
            .entry(resource)
            .or_insert_with(|| RunningAverage::new(RUNNING_AVERAGE_WINDOW_SIZE));
        resource_value.insert(value);
    }
}

#[derive(Eq, Hash, PartialEq, Clone, Debug)]
pub(crate) enum AttributionSource {
    Peer(PeerKeyLocation),
    RelayedContract(ContractInstanceId),
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

    use super::*;

    #[test]
    fn test_meter() {
        let meter = Meter::new();

        // Test that the new Meter has empty totals_by_resource and attribution_meters
        assert!(meter.totals_by_resource.map.is_empty());
        assert!(meter.attribution_meters.is_empty());
    }

    #[test]
    fn test_meter_total_usage() {
        let meter = Meter::new();

        // Test that the total usage is 0.0 for all resources
        assert_eq!(meter.total_usage(ResourceType::InboundBandwidthBytes), 0.0);
        assert_eq!(meter.total_usage(ResourceType::OutboundBandwidthBytes), 0.0);
        assert_eq!(meter.total_usage(ResourceType::CpuInstructions), 0.0);

        // Report some usage and test that the total usage is updated
        let attribution = AttributionSource::Peer(PeerKeyLocation::random());
        meter.report(&attribution, ResourceType::InboundBandwidthBytes, 100.0);
        assert_eq!(
            meter.total_usage(ResourceType::InboundBandwidthBytes),
            100.0
        );
    }

    #[test]
    fn test_meter_attributed_usage() {
        let meter = Meter::new();

        // Test that the attributed usage is 0.0 for all resources
        let attribution = AttributionSource::Peer(PeerKeyLocation::random());
        assert_eq!(
            meter.attributed_usage(&attribution, ResourceType::InboundBandwidthBytes),
            0.0
        );
        assert_eq!(
            meter.attributed_usage(&attribution, ResourceType::OutboundBandwidthBytes),
            0.0
        );
        assert_eq!(
            meter.attributed_usage(&attribution, ResourceType::CpuInstructions),
            0.0
        );

        // Report some usage and test that the attributed usage is updated
        meter.report(&attribution, ResourceType::InboundBandwidthBytes, 100.0);
        assert_eq!(
            meter.attributed_usage(&attribution, ResourceType::InboundBandwidthBytes),
            100.0
        );
    }

    #[test]
    fn test_meter_report() -> Result<(), DynError> {
        let meter = Meter::new();

        // Report some usage and test that the total and attributed usage are updated
        let attribution = AttributionSource::Peer(PeerKeyLocation::random());
        meter.report(&attribution, ResourceType::InboundBandwidthBytes, 100.0);
        assert_eq!(
            meter.total_usage(ResourceType::InboundBandwidthBytes),
            100.0
        );
        assert_eq!(
            meter.attributed_usage(&attribution, ResourceType::InboundBandwidthBytes),
            100.0
        );

        // Report more usage and test that the total and attributed usage are updated
        meter.report(&attribution, ResourceType::InboundBandwidthBytes, 200.0);
        assert_eq!(
            meter.total_usage(ResourceType::InboundBandwidthBytes),
            300.0
        );
        assert_eq!(
            meter.attributed_usage(&attribution, ResourceType::InboundBandwidthBytes),
            300.0
        );
        // Report usage for a different resource and test that the total and attributed usage are updated
        meter.report(&attribution, ResourceType::CpuInstructions, 50.0);
        assert_eq!(meter.total_usage(ResourceType::CpuInstructions), 50.0);
        assert_eq!(
            meter.attributed_usage(&attribution, ResourceType::CpuInstructions),
            50.0
        );

        let bytes = crate::util::test::random_bytes_1024();
        let mut gen = arbitrary::Unstructured::new(&bytes);
        // Report usage for a different attribution and test that the total and attributed usage are updated
        let other_attribution =
            AttributionSource::RelayedContract(gen.arbitrary::<ContractInstanceId>()?);
        meter.report(
            &other_attribution,
            ResourceType::InboundBandwidthBytes,
            150.0,
        );
        assert_eq!(
            meter.total_usage(ResourceType::InboundBandwidthBytes),
            450.0
        );
        assert_eq!(
            meter.attributed_usage(&other_attribution, ResourceType::InboundBandwidthBytes),
            150.0
        );
        Ok(())
    }
}
