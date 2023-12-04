//! # Resource Management
//!
//! The resource management module is responsible for tracking resource usage,
//! ensuring that usage does not exceed specified limits, and ensure that those
//! resources are used to maximize the utility of the network. If limits are
//! exceeded then peers are removed until the usage is below the limit.git
//!
//! ## Resources
//!
//! The resource management module tracks usage of the following resources:
//!
//! * Upstream and downstream bandwidth
//! * CPU usage
//!
//! These resources will be tracked in the future:
//!
//! * Memory usage
//! * Storage
//!
//! ## Attribution
//!
//! When used this resource usage is attributed to either:
//!
//! * Remote
//!   * A connected peer
//! * Local
//!   * A local delegate
//!   * The user interface
//!
//! ## Resource allocation for contract subscriptions
//!
//! When one or more peers are subscribed to a contract, the required
//! resources will be allocated as follows:
//!
//! * Upstream bandwidth is allocated to the subscribed peer to which
//!   the data is sent
//! * Downstream bandwidth and CPU is split evenly between all subscribed
//!   peers for that contract
//!
//! ## Resource limits
//!
//! Resource limits should be set to ensure that the peer does not disrupt the
//! user's experience of their computer. We should choose intelligent defaults
//! but the limits should be user-configurable.
//!
//! ## Code component overview
//!
//! The [ResourceManager] is responsible for tracking resource usage and identifying
//! which peers to remove if/when limits are exceeded. It does this by identifying
//! the peers with the highest usage of the limit-exceeding resource relative to
//! their usefulness. The usefulness of a peer is determined by the number of
//! requests sent to that peer over a certain period of time.
//!
//! A [Meter] is used by the ResourceManager to tracking resource usage over time.
//! Resource usage is reported to the meter, which tracks the usage over a sliding
//! window of time. The meter is responsible for calculating the rate of resource
//! usage along with which peers (or delegates, or UIs) are responsible for that
//! usage.
//!
//! ## Future Improvements
//!
//! * Track non-flow resources like memory and storage
//! * Responses to specific requests will contain information about the resources used
//!   by downstream peers to fulfill the request, however how this information is used
//!   will require careful consideration.
#![allow(dead_code, unused)] // FIXME: remove after integration

mod meter;
mod predictive_meter;
pub mod rate;
mod running_average;

use std::collections::HashMap;
use self::meter::{AttributionSource, Meter, ResourceType};
use crate::ring::PeerKeyLocation;
use std::time::Instant;

pub(crate) struct ResourceManager {
    limits: Limits,
    meter: Meter,
}

impl ResourceManager {
    pub fn new(limits: Limits) -> Self {
        ResourceManager {
            meter: Meter::new_with_window_size(100),
            limits,
        }
    }

    pub fn update_limits(&mut self, limits: Limits) {
        self.limits = limits;
    }

    /// Report the use of a resource.
    pub(crate) fn report(
        &mut self,
        _time: Instant,
        attribution: &AttributionSource,
        resource: ResourceType,
        value: f64,
    ) {
        self.meter.report(attribution, resource, value);
    }

    /// Return the maximum resource usage rate of any usage type as a proportion
    /// of the limit for that usage type
    pub(crate) fn max_usage_rate(&self) -> UsageRates {
        let mut max = 0.0;
        let mut usage_rate_per_type = HashMap::new();
        for resource_type in ResourceType::iter() {
            let usage_rate = self.resource_usage_rate(resource_type);
            usage_rate_per_type.insert(resource_type, usage_rate);
            if usage_rate > max {
                max = usage_rate;
            }
        }
        UsageRates {
            usage_rate_per_type,
            max_usage_rate: max,
        }
    }

    /// Returns the current resource usage rate for a specified usage type as
    /// a fraction of the limit.
    pub(crate) fn resource_usage_rate(&self, resource_type: ResourceType) -> f64 {
        match self.meter.resource_usage_rate(resource_type) {
            Some(rate) => rate.per_second() / self.limits.get(resource_type),
            None => 0.0,
        }
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
    pub(crate) fn should_delete_peers<P>(
        &self,
        resource_type: ResourceType,
        candidates: P,
    ) -> Vec<PeerKeyLocation>
    where
        P: IntoIterator<Item = PeerValue>,
    {
        let total_usage = match self.meter.resource_usage_rate(resource_type) {
            Some(rate) => rate.per_second(),
            None => return vec![], // Or handle the error as appropriate
        };

        let total_limit: f64 = self.limits.get(resource_type);
        if total_usage > total_limit {
            let mut candidate_costs = vec![];
            for PeerValue { peer, value } in candidates {
                if let Some(cost) = self
                    .meter
                    .attributed_usage_rate(&AttributionSource::Peer(peer), resource_type)
                {
                    const MIN_VALUE: f64 = 0.0001;
                    let cost_per_second = cost.per_second();
                    let cost_per_value = cost_per_second / value.max(MIN_VALUE);
                    candidate_costs.push(CandidateCost {
                        peer,
                        total_cost: cost_per_second,
                        cost_per_value,
                    });
                } // Else, you might want to handle the case where cost is None
            }

            // Sort candidate_costs by cost_per_value descending
            candidate_costs.sort_by(|a, b| {
                b.cost_per_value
                    .partial_cmp(&a.cost_per_value)
                    .unwrap_or(std::cmp::Ordering::Equal)
            });

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
pub(crate) struct PeerValue {
    pub peer: PeerKeyLocation,
    pub value: f64,
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

pub(crate) struct UsageRates {
    pub usage_rate_per_type : HashMap<ResourceType, f64>,
    pub max_usage_rate: f64,
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub struct BytesPerSecond(f64);
impl BytesPerSecond {
    pub const fn new(bytes_per_second: f64) -> Self {
        BytesPerSecond(bytes_per_second)
    }
}

impl From<BytesPerSecond> for f64 {
    fn from(val: BytesPerSecond) -> Self {
        val.0
    }
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub struct InstructionsPerSecond(f64);
impl InstructionsPerSecond {
    pub const fn new(cpu_usage: f64) -> Self {
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
    use crate::resources::{BytesPerSecond, InstructionsPerSecond, Limits, ResourceManager};

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
        let mut resource_manager = ResourceManager::new(limits);

        // Report some usage and test that the total and attributed usage are updated
        let attribution = AttributionSource::Peer(PeerKeyLocation::random());
        let time = Instant::now();
        resource_manager.report(
            time,
            &attribution,
            ResourceType::InboundBandwidthBytes,
            100.0,
        );
        assert_eq!(
            resource_manager
                .meter
                .resource_usage_rate(ResourceType::InboundBandwidthBytes)
                .unwrap()
                .per_second(),
            100.0
        );
        assert_eq!(
            resource_manager
                .meter
                .attributed_usage_rate(&attribution, ResourceType::InboundBandwidthBytes)
                .unwrap()
                .per_second(),
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
        let mut resource_manager = ResourceManager::new(limits);

        // Report some usage
        let peer1 = PeerKeyLocation::random();
        let attribution1 = AttributionSource::Peer(peer1);
        let peer2 = PeerKeyLocation::random();
        let attribution2 = AttributionSource::Peer(peer2);
        let time = Instant::now();
        resource_manager.report(
            time,
            &attribution1,
            ResourceType::InboundBandwidthBytes,
            400.0,
        );
        resource_manager.report(
            time,
            &attribution2,
            ResourceType::InboundBandwidthBytes,
            500.0,
        );

        // Test that no peers should be deleted when the total usage is below the limit
        let candidates = vec![
            PeerValue {
                peer: peer1,
                value: 1.0,
            },
            PeerValue {
                peer: peer2,
                value: 1.0,
            },
        ];
        let to_delete = resource_manager
            .should_delete_peers(ResourceType::InboundBandwidthBytes, candidates.clone());

        // Test that no peers should be deleted when the total usage is below the limit
        assert_eq!(to_delete.len(), 0);

        // Report more usage to exceed the limit
        resource_manager.report(
            time,
            &attribution1,
            ResourceType::InboundBandwidthBytes,
            200.0,
        );

        let to_delete =
            resource_manager.should_delete_peers(ResourceType::InboundBandwidthBytes, candidates);
        assert_eq!(to_delete.len(), 1);

        // Test that the peer with the highest usage is deleted
        assert_eq!(to_delete[0], peer1);
    }

    #[test]
    fn test_update_limits() {
        let limits = Limits {
            max_upstream_bandwidth: BytesPerSecond::new(1000.0),
            max_downstream_bandwidth: BytesPerSecond::new(1000.0),
            max_cpu_usage: InstructionsPerSecond::new(1000.0),
            max_memory_usage: 1000.0,
            max_storage_usage: 1000.0,
        };
        let mut resource_manager = ResourceManager::new(limits);

        let new_limits = Limits {
            max_upstream_bandwidth: BytesPerSecond::new(2000.0),
            max_downstream_bandwidth: BytesPerSecond::new(2000.0),
            max_cpu_usage: InstructionsPerSecond::new(2000.0),
            max_memory_usage: 2000.0,
            max_storage_usage: 2000.0,
        };
        resource_manager.update_limits(new_limits);

        assert_eq!(
            resource_manager.limits.max_upstream_bandwidth,
            BytesPerSecond::new(2000.0)
        );
        assert_eq!(
            resource_manager.limits.max_downstream_bandwidth,
            BytesPerSecond::new(2000.0)
        );
        assert_eq!(
            resource_manager.limits.max_cpu_usage,
            InstructionsPerSecond::new(2000.0)
        );
        assert_eq!(resource_manager.limits.max_memory_usage, 2000.0);
        assert_eq!(resource_manager.limits.max_storage_usage, 2000.0);
    }
}
