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
pub mod rate;
mod running_average;

use self::meter::{AttributionSource, Meter, ResourceType};
use crate::resources::meter::ALL_RESOURCE_TYPES;
use crate::resources::rate::{Rate, RateProportion};
use crate::ring::{Connection, Location, PeerKeyLocation};
use crate::topology::request_density_tracker::RequestDensityTracker;
use crate::topology::TopologyManager;
use dashmap::DashMap;
use parking_lot::RwLock;
use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;
use std::time::Duration;
use std::time::Instant;

// TODO: Reevaluate this value once we have realistic data
const SOURCE_RAMP_UP_TIME: Duration = Duration::from_secs(5 * 60);

const REQUEST_DENSITY_TRACKER_SAMPLE_SIZE: usize = 5000;

pub(crate) struct ResourceManager {
    limits: Limits,
    meter: Meter,
    source_creation_times: DashMap<AttributionSource, Instant>,
    pub(crate) request_density_tracker: RwLock<RequestDensityTracker>,
    pub(crate) topology_manager: TopologyManager,
}

impl ResourceManager {
    pub fn new(
        limits: Limits,
        neighbors: Arc<RwLock<BTreeMap<Location, Vec<Connection>>>>,
    ) -> Self {
        ResourceManager {
            meter: Meter::new_with_window_size(100),
            limits,
            source_creation_times: DashMap::new(),
            request_density_tracker: RwLock::new(RequestDensityTracker::new(
                REQUEST_DENSITY_TRACKER_SAMPLE_SIZE,
                neighbors,
            )),
            topology_manager: TopologyManager::new(),
        }
    }

    pub fn update_limits(&mut self, limits: Limits) {
        self.limits = limits;
    }

    /// Report the use of a resource.
    pub(crate) fn report(
        &mut self,
        attribution: &AttributionSource,
        resource: ResourceType,
        value: f64,
        now: Instant,
    ) {
        if !self.source_creation_times.contains_key(attribution) {
            self.source_creation_times.insert(attribution.clone(), now);
        }

        self.meter.report(attribution, resource, value);
    }

    fn extrapolated_usage(&mut self, resource_type: ResourceType, now: Instant) -> Usage {
        let mut total_usage: Rate = Rate::new_per_second(0.0);
        let mut usage_per_source: HashMap<AttributionSource, Rate> = HashMap::new();
        for source_entry in self.source_creation_times.iter() {
            let source = source_entry.key();
            let creation_time = source_entry.value();
            let ramping_up = now.duration_since(*creation_time) <= SOURCE_RAMP_UP_TIME;
            let usage_rate: Option<Rate> = if ramping_up {
                self.meter.get_estimated_usage_rate(&resource_type, now)
            } else {
                self.meter.attributed_usage_rate(source, &resource_type)
            };
            total_usage += usage_rate.unwrap_or(Rate::new_per_second(0.0));
            usage_per_source.insert(
                source.clone(),
                usage_rate.unwrap_or(Rate::new_per_second(0.0)),
            );
        }

        Usage {
            total: total_usage,
            per_source: usage_per_source,
        }
    }

    // A function that will determine if any peers should be added or removed based on
    // the current resource usage, and either add or remove them
    fn adjust_topology(&mut self, resource_type: ResourceType, now: Instant) {
        let increase_usage_if_below_prop: RateProportion = RateProportion::new(0.5);
        let decrease_usage_if_above_prop: RateProportion = RateProportion::new(0.9);

        let usage = self.extrapolated_usage(resource_type, now);
        let total_limit: Rate = self.limits.get(resource_type);
        let usage_proportion = total_limit.proportion_of(&self.limits.get(resource_type));
        if usage_proportion < increase_usage_if_below_prop {
            tracing::debug!(
                "{:?} resource usage is too low, adding connections: {:?}",
                resource_type,
                usage_proportion
            );
            self.add_connections(&usage);
        } else if usage_proportion > decrease_usage_if_above_prop {
            tracing::debug!(
                "{:?} resource usage is too high, removing connections: {:?}",
                resource_type,
                usage_proportion
            );
            self.remove_connections(&usage);
        } else {
            tracing::debug!(
                "{:?} resource usage is within acceptable bounds: {:?}",
                resource_type,
                usage_proportion
            );
        }
    }

    /// Adds a single connection (one at a time to avoid overshooting)
    fn add_connections(&mut self, usage: &Usage) {
        let neighbors: BTreeMap<Location, usize> = BTreeMap::new();
        todo!("""
        Need to get the neighbors, Ring will need to keep Resources up-to-date
        """);
        let desired_location = self
            .request_density_tracker
            .read()
            .create_density_map(neighbors);
    }

    /// Calculates which peers should be deleted in order to bring the total
    /// resource usage below the specified limit.
    fn remove_connections(&mut self, usage: &Usage) {}
}

pub(crate) struct Usage {
    pub(crate) total: Rate,
    pub(crate) per_source: HashMap<AttributionSource, Rate>,
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
    pub max_upstream_bandwidth: Rate,
    pub max_downstream_bandwidth: Rate,
}

impl Limits {
    pub fn get(&self, resource_type: ResourceType) -> Rate {
        match resource_type {
            ResourceType::OutboundBandwidthBytes => self.max_upstream_bandwidth.clone(),
            ResourceType::InboundBandwidthBytes => self.max_downstream_bandwidth.clone(),
        }
    }
}

pub(crate) struct UsageRates {
    pub usage_rate_per_type: HashMap<ResourceType, RateProportion>,
    pub max_usage_rate: RateProportion,
}

#[cfg(test)]
mod tests {
    use crate::resources::{Limits, ResourceManager};

    use super::*;
    use std::time::Instant;

    #[test]
    fn test_resource_manager_report() {
        // Create a ResourceManager with arbitrary limits
        let limits = Limits {
            max_upstream_bandwidth: Rate::new_per_second(1000.0),
            max_downstream_bandwidth: Rate::new_per_second(1000.0),
        };
        let my_location = Some(Location::random());
        let mut resource_manager = ResourceManager::new(limits);

        // Report some usage and test that the total and attributed usage are updated
        let attribution = AttributionSource::Peer(PeerKeyLocation::random());
        let now = Instant::now();
        resource_manager.report(
            &attribution,
            ResourceType::InboundBandwidthBytes,
            100.0,
            now,
        );
        assert_eq!(
            resource_manager
                .meter
                .attributed_usage_rate(&attribution, &ResourceType::InboundBandwidthBytes)
                .unwrap()
                .per_second(),
            100.0
        );
    }

    #[test]
    fn test_resource_manager_should_delete_peers() {
        // Create a ResourceManager with arbitrary limits
        let limits = Limits {
            max_upstream_bandwidth: Rate::new_per_second(1000.0),
            max_downstream_bandwidth: Rate::new_per_second(1000.0),
        };
        let mut resource_manager = ResourceManager::new(limits);

        // Report some usage
        let peer1 = PeerKeyLocation::random();
        let attribution1 = AttributionSource::Peer(peer1);
        let peer2 = PeerKeyLocation::random();
        let attribution2 = AttributionSource::Peer(peer2);
        let now = Instant::now();
        resource_manager.report(
            &attribution1,
            ResourceType::InboundBandwidthBytes,
            400.0,
            now,
        );
        resource_manager.report(
            &attribution2,
            ResourceType::InboundBandwidthBytes,
            500.0,
            now,
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

        todo!("Verify deletion of peers")
    }

    #[test]
    fn test_update_limits() {
        let limits = Limits {
            max_upstream_bandwidth: Rate::new_per_second(1000.0),
            max_downstream_bandwidth: Rate::new_per_second(1000.0),
        };
        let mut resource_manager = ResourceManager::new(limits);

        let new_limits = Limits {
            max_upstream_bandwidth: Rate::new_per_second(2000.0),
            max_downstream_bandwidth: Rate::new_per_second(2000.0),
        };
        resource_manager.update_limits(new_limits);

        assert_eq!(
            resource_manager.limits.max_upstream_bandwidth,
            Rate::new_per_second(2000.0)
        );
        assert_eq!(
            resource_manager.limits.max_downstream_bandwidth,
            Rate::new_per_second(2000.0)
        );
    }
}
