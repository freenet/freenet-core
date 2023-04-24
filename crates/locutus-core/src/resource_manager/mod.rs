mod meter;
mod running_average;

use std::time::Instant;

use crate::ring::PeerKeyLocation;

use self::meter::{AttributionSource, Meter, ResourceType};

pub struct ResourceManager {
    limits: Limits,
    meter: Meter,
}

impl ResourceManager {
    pub fn new(limits: Limits) -> Self {
        ResourceManager {
            meter: Meter::new(),
            limits,
        }
    }

    pub fn meter(&self) -> &Meter {
        &self.meter
    }

    pub fn update_limits(&mut self, limits: Limits) {
        self.limits = limits;
    }

    /// Report the use of a resource.
    pub fn report(
        &self,
        _time: Instant,
        attribution: &AttributionSource,
        resource: ResourceType,
        value: f64,
    ) {
        self.meter.report(attribution, resource, value);
    }

    /// Returns a list of peers that should be deleted to bring the resource usage below the limit.
    pub fn should_delete_peers<P>(
        &self,
        resource_type: ResourceType,
        candidates: P,
    ) -> Vec<PeerKeyLocation>
    where
        P: IntoIterator<Item = (PeerKeyLocation, f64)>,
    {
        let total_usage: f64 = self.meter.total_usage(resource_type);
        let total_limit: f64 = self.limits.get(resource_type);
        if total_usage > total_limit {
            let mut candidate_costs = vec![];
            for (peer, value) in candidates {
                let cost = self.meter.attributed_usage(&AttributionSource::Peer(peer), resource_type);
                let cost_per_value = cost / value;
                candidate_costs.push(CandidateCost {
                    peer,
                    total_cost: cost,
                    cost_per_value,
                });
            }
            // sort candidate_costs by cost_per_value descending
            candidate_costs
                .sort_by(|a, b| b.cost_per_value.partial_cmp(&a.cost_per_value).unwrap());

            let mut to_delete = vec![];
            let excess_usage = total_usage - total_limit;
            let mut total_deleted_cost = 0.0;
            for candidate in candidate_costs {
                if total_deleted_cost >= excess_usage {
                    break;
                }
                total_deleted_cost += candidate.total_cost;
                to_delete.push(candidate.peer);
            }
            to_delete
        } else {
            vec![]
        }
    }
}

#[derive(Debug, Clone, Copy)]
struct CandidateCost {
    peer: PeerKeyLocation,
    total_cost: f64,
    cost_per_value: f64,
}

pub struct Limits {
    pub max_upstream_bandwidth: BytesPerSecond,
    pub max_downstream_bandwidth: BytesPerSecond,
    pub max_cpu_usage: InstructionsPerSecond,
    pub max_memory_usage: f64,
    pub max_storage_usage: f64,
}

impl Limits {
    pub fn get(&self, resource_type: ResourceType) -> f64 {
        match resource_type {
            ResourceType::OutboundBandwidthBytes => self.max_upstream_bandwidth.into(),
            ResourceType::InboundBandwidthBytes => self.max_downstream_bandwidth.into(),
            ResourceType::CpuInstructions => self.max_cpu_usage.into(),
            // TODO: Support non-flow resources like memory and storage use
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub struct BytesPerSecond(f64);
impl BytesPerSecond {
    pub fn new(bytes_per_second: f64) -> Self {
        BytesPerSecond(bytes_per_second)
    }
}

impl From<BytesPerSecond> for f64 {
    fn from(val: BytesPerSecond) -> Self {
        val.0
    }
}

#[derive(Debug, Clone, Copy)]
pub struct InstructionsPerSecond(f64);
impl InstructionsPerSecond {
    pub fn new(instructions_per_second: f64) -> Self {
        InstructionsPerSecond(instructions_per_second)
    }
}

impl From<InstructionsPerSecond> for f64 {
    fn from(val: InstructionsPerSecond) -> Self {
        val.0
    }
}

#[derive(Debug, Clone, Copy)]
pub struct ByteCount(u64); // Renamed from "Bytes"
impl ByteCount {
    pub fn new(bytes: u64) -> Self {
        ByteCount(bytes)
    }
}

impl From<ByteCount> for f64 {
    fn from(val: ByteCount) -> Self {
        val.0 as f64
    }
}


#[cfg(test)]
mod tests {
    use crate::resource_manager::{Limits, BytesPerSecond, InstructionsPerSecond, ResourceManager};

    use super::*;
    use std::time::Instant;

    #[test]
    fn test_resource_manager_report() {
        // Create a ResourceManager with arbitrary limits
        let limits = Limits {
            max_upstream_bandwidth: BytesPerSecond::new(1000.0),
            max_downstream_bandwidth: BytesPerSecond::new(1000.0),
            max_cpu_usage: InstructionsPerSecond::new(1000.0),
            max_memory_usage: 1000.0,
            max_storage_usage: 1000.0,
        };
        let resource_manager = ResourceManager::new(limits);

        // Report some usage and test that the total and attributed usage are updated
        let attribution = AttributionSource::Peer(PeerKeyLocation::random());
        let time = Instant::now();
        resource_manager.report(time, &attribution, ResourceType::InboundBandwidthBytes, 100.0);
        assert_eq!(
            resource_manager.meter().total_usage(ResourceType::InboundBandwidthBytes),
            100.0
        );
        assert_eq!(
            resource_manager.meter().attributed_usage(&attribution, ResourceType::InboundBandwidthBytes),
            100.0
        );
    }

    #[test]
    fn test_resource_manager_should_delete_peers() {
        // Create a ResourceManager with arbitrary limits
        let limits = Limits {
            max_upstream_bandwidth: BytesPerSecond::new(1000.0),
            max_downstream_bandwidth: BytesPerSecond::new(1000.0),
            max_cpu_usage: InstructionsPerSecond::new(1000.0),
            max_memory_usage: 1000.0,
            max_storage_usage: 1000.0,
        };
        let resource_manager = ResourceManager::new(limits);

        // Report some usage
        let peer1 = PeerKeyLocation::random();
        let attribution1 = AttributionSource::Peer(peer1);
        let peer2 = PeerKeyLocation::random();
        let attribution2 = AttributionSource::Peer(peer2);
        let time = Instant::now();
        resource_manager.report(time, &attribution1, ResourceType::InboundBandwidthBytes, 400.0);
        resource_manager.report(time, &attribution2, ResourceType::InboundBandwidthBytes, 500.0);

        // Test that no peers should be deleted when the total usage is below the limit
        let candidates = vec![
            (PeerKeyLocation::random(), 1.0),
            (PeerKeyLocation::random(), 1.0),
        ];
        let to_delete = resource_manager.should_delete_peers(ResourceType::InboundBandwidthBytes, candidates);

        // Test that no peers should be deleted when the total usage is below the limit
        assert_eq!(to_delete.len(), 0);

        // Report more usage to exceed the limit
        resource_manager.report(time, &attribution1, ResourceType::InboundBandwidthBytes, 200.0);

        // Test that peers should be deleted to bring the resource usage below the limit
        let candidates = vec![
            (peer1, 1.0),
            (peer2, 1.0),
        ];
        let to_delete = resource_manager.should_delete_peers(ResourceType::InboundBandwidthBytes, candidates);
        assert!(to_delete.len() == 1);

        // Test that the peer with the highest usage is deleted
        assert_eq!(to_delete[0], peer1);
    }
}
