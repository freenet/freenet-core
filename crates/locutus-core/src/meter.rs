use std::{hash::Hash, time::Instant};
use std::time::Duration;

use locutus_runtime::{ComponentKey, ContractInstanceId};
use running_average::RunningAverage;
use dashmap::DashMap;

use crate::ring::PeerKeyLocation;

const RUNNING_AVERAGE_WINDOW_SIZE: Duration = Duration::from_secs(10 * 60);

pub struct ResourceTotals {
    map : DashMap<ResourceType, RunningAverage<f64, Instant>>,
}

impl ResourceTotals {
    fn new() -> Self {
        ResourceTotals { map : DashMap::new() }
    }
}

type AttributionMeters = DashMap<AttributionSource, ResourceTotals>;

/// A meter for tracking resource usage with attribution.
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

    /// Returns an `AttributionMeter` for the specified attribution source.
    pub fn attribution_meter(&self, attribution: AttributionSource) -> AttributionMeter {
        AttributionMeter {
            parent: self,
            attribution,
        }
    }

    /// Returns a reference to the total usage for each resource.
    pub fn total_resource_usage(&self) -> &ResourceTotals {
        &self.totals_by_resource
    }

    /// Returns a reference to the usage for each resource for a specific attribution.
    pub fn attributed_resource_usage(&self) -> &AttributionMeters {
        &self.attribution_meters
    }
}

/// A meter for tracking resource usage for a specific attribution source.
pub struct AttributionMeter<'a> {
    parent: &'a Meter,
    attribution: AttributionSource,
}

impl<'a> AttributionMeter<'a> {
    /// Report the use of a resource. This should be done in the lowest-level
    /// functions that consume the resource, taking an AttributionMeter
    /// as a parameter.
    pub fn report(&self, time : Instant, resource: ResourceType, value: f64) {
        // Report the total usage for the resource
        let mut total_value = self.parent.totals_by_resource.map.entry(resource).or_insert_with(|| {
            RunningAverage::new(RUNNING_AVERAGE_WINDOW_SIZE)
        });
        total_value.insert(time, value);

        // Report the usage for a specific attribution
        let resource_map = self.parent.attribution_meters.entry(self.attribution.clone()).or_insert_with(|| {
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
