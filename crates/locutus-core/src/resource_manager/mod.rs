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

/// Determines which peers should be deleted to reduce resource usage below
/// the specified limit.
///
/// Given a resource type and a list of candidate peers, this function
/// calculates which peers should be deleted in order to bring the total
/// resource usage below the specified limit. Each candidate peer is
/// accompanied by an `f64` value representing its usefulness, typically
/// measured as the number of requests sent to the peer over a certain
/// period of time.
///
/// Peers are prioritized for deletion based on their usefulness relative to
/// their current resource usage. The function returns a list of
/// `PeerKeyLocation` objects representing the peers that should be deleted.
/// If the total resource usage is already below the limit, an empty list is
/// returned.
///
/// The usefulness value for each peer must be greater than zero. To prevent
/// division by zero, any usefulness value less than or equal to 0.0001 is
/// treated as 0.0001.
///
/// # Parameters
/// - `resource_type`: The type of resource for which usage is being
///   measured.
/// - `candidates`: An iterator over `PeerValue` objects, where each
///   `PeerValue` consists of a peer and its associated usefulness value.
///
/// # Returns
/// A `Vec<PeerKeyLocation>` containing the peers that should be deleted to
/// bring resource usage below the limit. If no peers need to be deleted, an
/// empty vector is returned.
    pub fn should_delete_peers<P>(
        &self,
        resource_type: ResourceType,
        candidates: P,
    ) -> Vec<PeerKeyLocation>
    where
        P: IntoIterator<Item = PeerValue>,
    {
        let total_usage: f64 = self.meter.total_usage(resource_type);
        let total_limit: f64 = self.limits.get(resource_type);
        if total_usage > total_limit {
            let mut candidate_costs = vec![];
            for  PeerValue { peer, value } in candidates {
                let cost = self.meter.attributed_usage(&AttributionSource::Peer(peer), resource_type);
                const MIN_VALUE: f64 = 0.0001;
                let cost_per_value = cost / value.max(MIN_VALUE);
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
pub struct PeerValue { pub peer : PeerKeyLocation, pub value : f64 }

#[derive(Debug, Clone, Copy)]
struct CandidateCost {
    peer: PeerKeyLocation,
    total_cost: f64,
    cost_per_value: f64,
}

pub struct Limits {
    pub max_upstream_bandwidth: Bandwidth,
    pub max_downstream_bandwidth: Bandwidth,
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
pub struct Bandwidth(f64);
impl Bandwidth {
    pub fn new(bytes_per_second: f64) -> Self {
        Bandwidth(bytes_per_second)
    }
}

impl From<Bandwidth> for f64 {
    fn from(val: Bandwidth) -> Self {
        val.0
    }
}

#[derive(Debug, Clone, Copy)]
pub struct InstructionsPerSecond(f64);
impl InstructionsPerSecond {
    pub fn new(cpu_usage: f64) -> Self {
        InstructionsPerSecond(cpu_usage)
    }
}

impl From<InstructionsPerSecond> for f64 {
    fn from(val: InstructionsPerSecond) -> Self {
        val.0
    }
}

#[derive(Debug, Clone, Copy)]
pub struct ByteCount(u64); 
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
    use crate::resource_manager::{Limits, Bandwidth, InstructionsPerSecond, ResourceManager};

    use super::*;
    use std::time::Instant;

    #[test]
    fn test_resource_manager_report() {
        // Create a ResourceManager with arbitrary limits
        let limits = Limits {
            max_upstream_bandwidth: Bandwidth::new(1000.0),
            max_downstream_bandwidth: Bandwidth::new(1000.0),
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
            max_upstream_bandwidth: Bandwidth::new(1000.0),
            max_downstream_bandwidth: Bandwidth::new(1000.0),
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
            PeerValue { peer: peer1, value : 1.0},
            PeerValue { peer: peer2, value : 1.0},
        ];
        let to_delete = resource_manager.should_delete_peers(ResourceType::InboundBandwidthBytes, candidates.clone());

        // Test that no peers should be deleted when the total usage is below the limit
        assert_eq!(to_delete.len(), 0);

        // Report more usage to exceed the limit
        resource_manager.report(time, &attribution1, ResourceType::InboundBandwidthBytes, 200.0);

        let to_delete = resource_manager.should_delete_peers(ResourceType::InboundBandwidthBytes, candidates);
        assert!(to_delete.len() == 1);

        // Test that the peer with the highest usage is deleted
        assert_eq!(to_delete[0], peer1);
    }
}
