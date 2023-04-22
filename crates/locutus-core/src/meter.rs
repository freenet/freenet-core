use std::{hash::Hash, time::Instant};
use std::time::Duration;

use locutus_runtime::{ComponentKey, ContractInstanceId};
use running_average::RunningAverage;
use dashmap::DashMap;

use crate::ring::PeerKeyLocation;

const RUNNING_AVERAGE_WINDOW_SIZE: Duration = Duration::from_secs(10 * 60);

pub struct ResourceTotals {
    pub map : DashMap<ResourceType, RunningAverage<f64, Instant>>,
}

impl ResourceTotals {
    fn new() -> Self {
        ResourceTotals { map : DashMap::new() }
    }
}

type AttributionMeters = DashMap<AttributionSource, ResourceTotals>;

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
            Some(mut meter) => {
                // Get the current measurement value
                meter.measurement(Instant::now()).value().to_owned()
            }
            None => 0.0, // No meter found for the given resource
        }
    }

    pub fn attributed_usage(&self, attribution: &AttributionSource, resource: ResourceType) -> f64 {
        // Try to get a mutable reference to the AttributionMeters for the given attribution
        match self.attribution_meters.get_mut(attribution) {
            Some(attribution_meters) => {
                // Try to get a mutable reference to the Meter for the given resource
                match attribution_meters.map.get_mut(&resource) {
                    Some(mut meter) => {
                        // Get the current measurement value
                        meter.measurement(Instant::now()).value().to_owned()
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
    pub fn report(&self, time : Instant, attribution : &AttributionSource, resource: ResourceType, value: f64) {
        // Report the total usage for the resource
        let mut total_value = self.totals_by_resource.map.entry(resource).or_insert_with(|| {
            RunningAverage::new(RUNNING_AVERAGE_WINDOW_SIZE)
        });
        total_value.insert(time, value);

        // Report the usage for a specific attribution
        let resource_map = self.attribution_meters.entry(attribution.clone()).or_insert_with(|| {
            ResourceTotals::new()
        });
        let mut resource_value = resource_map.map.entry(resource).or_insert_with(|| {
            RunningAverage::new(RUNNING_AVERAGE_WINDOW_SIZE)
        });
        resource_value.insert(time, value);
    }
}

#[derive(Eq, Hash, PartialEq, Clone, Debug)] // Derive additional traits for AttributionSource
pub enum AttributionSource {
    Peer(PeerKeyLocation),
    RelayedContract(ContractInstanceId),
    Delegate(ComponentKey),
}

#[derive(Eq, Hash, PartialEq, Clone, Copy, Debug)] // Derive additional traits for ResourceType
pub enum ResourceType {
    InboundBandwidthBytes,
    OutboundBandwidthBytes,
    CpuInstructions,
}
