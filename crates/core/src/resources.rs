//! # Resource Management
//!
//! The resource management module is responsible for tracking resource usage,
//! and recommending the addition or removal of peers in order to keep resource
//! usage within specified limits.
//!
//! ## Resources
//!
//! The [ResourceManager] tracks usage of the following resources:
//!
//! * Upstream and downstream bandwidth
//!
//! These resources will be tracked in the future:
//!
//! * CPU usage
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
//! resources should be allocated as follows:
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
//! ## Integration notes
//!
//! The user of the [ResourceManager] is responsible for calling the
//! [ResourceManager::report_resource_usage] function whenever a resource (currently
//! only bandwidth) is used, and the [ResourceManager::report_outbound_request]
//! function whenever a request is sent to a neighboring peer.
//!
//! The user of the resource manager should call the
//! [ResourceManager::adjust_topology] function at regular intervals, perhaps every 10
//! seconds. This function will return a [TopologyAdjustment] which will indicate
//! whether any peers should be added or removed, along with relevant
//! details.
//!
//! If there are fewer than [HARD_MINIMUM_NUM_PEERS] peers, the [ResourceManager] will
//! recommend adding peers at random locations until the minimum number of peers is
//! reached.
//!
//! ## Future Improvements
//!
//! * Track non-flow resources like memory and storage
//! * Responses to specific requests will contain information about the resources used
//!   by downstream peers to fulfill the request, however how this information is used
//!   will require careful consideration.
#![allow(dead_code, unused)] // FIXME: remove after integration

mod meter;
mod outbound_request_counter;
pub mod rate;
mod running_average;

use self::meter::{AttributionSource, Meter, ResourceType};
use crate::resources::meter::ALL_RESOURCE_TYPES;
use crate::resources::outbound_request_counter::OutboundRequestCounter;
use crate::resources::rate::{Rate, RateProportion};
use crate::ring::{Connection, Location, PeerKeyLocation};
use crate::topology::request_density_tracker::{
    DensityMap, DensityMapError, RequestDensityTracker,
};
use crate::topology::TopologyManager;
use anyhow::anyhow;
use dashmap::DashMap;
use parking_lot::RwLock;
use std::cmp::max;
use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::sync::{Arc, RwLockReadGuard};
use std::time::Duration;
use std::time::Instant;
use tracing::field::debug;
use tracing::{debug, error, event, info, span, trace, Level};
use tracing_subscriber::fmt::format::FmtSpan;
use tracing_subscriber::EnvFilter;

// TODO: Reevaluate this value once we have realistic data
const SOURCE_RAMP_UP_DURATION: Duration = Duration::from_secs(5 * 60);

const REQUEST_DENSITY_TRACKER_SAMPLE_SIZE: usize = 5000;

const OUTBOUND_REQUEST_COUNTER_WINDOW_SIZE: usize = 10000;

const MINIMUM_DESIRED_RESOURCE_USAGE_PROPORTION: f64 = 0.5;

const MAXIMUM_DESIRED_RESOURCE_USAGE_PROPORTION: f64 = 0.9;

const HARD_MINIMUM_NUM_PEERS: usize = 5;

const HARD_MAXIMUM_NUM_PEERS: usize = 200;

pub(crate) struct ResourceManager {
    limits: Limits,
    meter: Meter,
    source_creation_times: DashMap<AttributionSource, Instant>,
    pub(crate) request_density_tracker: RwLock<RequestDensityTracker>,
    pub(crate) topology_manager: TopologyManager,
    pub(crate) outbound_request_counter: OutboundRequestCounter,
}

impl ResourceManager {
    pub fn new(limits: Limits) -> Self {
        ResourceManager {
            meter: Meter::new_with_window_size(100),
            limits,
            source_creation_times: DashMap::new(),
            request_density_tracker: RwLock::new(RequestDensityTracker::new(
                REQUEST_DENSITY_TRACKER_SAMPLE_SIZE,
            )),
            topology_manager: TopologyManager::new(),
            outbound_request_counter: OutboundRequestCounter::new(
                OUTBOUND_REQUEST_COUNTER_WINDOW_SIZE,
            ),
        }
    }

    pub fn update_limits(&mut self, limits: Limits) {
        self.limits = limits;
    }

    pub(crate) fn report_resource_usage(
        &mut self,
        attribution: &AttributionSource,
        resource: ResourceType,
        value: f64,
        at_time: Instant,
    ) {
        if let Some(creation_time) = self.source_creation_times.get(attribution) {
            if at_time < *creation_time {
                self.source_creation_times
                    .insert(attribution.clone(), at_time);
            }
        } else {
            self.source_creation_times
                .insert(attribution.clone(), at_time);
        }

        self.meter.report(attribution, resource, value, at_time);
    }

    // TODO: This should be called when a request is sent to a neighbor,
    //       could be any type of request (GET, PUT, SUBSCRIBE)
    pub(crate) fn report_outbound_request(&mut self, peer: PeerKeyLocation, target: Location) {
        self.request_density_tracker.write().sample(target);
        self.outbound_request_counter.record_request(peer);
    }

    /// Calculate total usage for a resource type extrapolating usage for resources that
    /// are younger than [SOURCE_RAMP_UP_DURATION]
    fn extrapolated_usage(&mut self, resource_type: ResourceType, now: Instant) -> Usage {
        let function_span = span!(Level::DEBUG, "extrapolated_usage_function");
        let _enter = function_span.enter();

        debug!("Function called");
        debug!("Resource type: {:?}", resource_type);
        debug!("Current time: {:?}", now);

        let mut total_usage: Rate = Rate::new_per_second(0.0);
        let mut usage_per_source: HashMap<AttributionSource, Rate> = HashMap::new();

        // Step 1: Collect data
        let collect_data_span = span!(Level::DEBUG, "collect_data");
        let _collect_data_guard = collect_data_span.enter();
        debug!("Collecting data from source_creation_times");
        let mut usage_data = Vec::new();
        for source_entry in self.source_creation_times.iter() {
            let source = source_entry.key();
            let creation_time = source_entry.value();
            let ramping_up = now.duration_since(*creation_time) <= SOURCE_RAMP_UP_DURATION;
            debug!(
                "Source: {:?}, Creation time: {:?}, Ramping up: {}",
                source, creation_time, ramping_up
            );
            usage_data.push((source.clone(), ramping_up));
        }
        drop(_collect_data_guard);

        // Step 2: Process data
        let process_data_span = span!(Level::DEBUG, "process_data");
        let _process_data_guard = process_data_span.enter();
        debug!("Processing data for usage calculation");
        for (source, ramping_up) in usage_data {
            let usage_rate: Option<Rate> = if ramping_up {
                debug!("Source {:?} is ramping up", source);
                self.meter.get_adjusted_usage_rate(&resource_type, now)
            } else {
                debug!("Source {:?} is not ramping up", source);
                self.attributed_usage_rate(&source, &resource_type, now)
            };
            debug!("Usage rate for source {:?}: {:?}", source, usage_rate);
            total_usage += usage_rate.unwrap_or(Rate::new_per_second(0.0));
            usage_per_source.insert(source, usage_rate.unwrap_or(Rate::new_per_second(0.0)));
        }
        drop(_process_data_guard);

        debug!("Total usage: {:?}", total_usage);
        debug!("Usage per source: {:?}", usage_per_source);

        Usage {
            total: total_usage,
            per_source: usage_per_source,
        }
    }

    pub(crate) fn attributed_usage_rate(
        &mut self,
        source: &AttributionSource,
        resource_type: &ResourceType,
        now: Instant,
    ) -> Option<Rate> {
        self.meter.attributed_usage_rate(source, resource_type, now)
    }

    // A function that will determine if any peers should be added or removed based on
    // the current resource usage, and either add or remove them
    fn adjust_topology(
        &mut self,
        resource_type: ResourceType,
        neighbor_locations: &BTreeMap<Location, Vec<Connection>>,
        at_time: Instant,
    ) -> TopologyAdjustment {
        debug!(
            "Adjusting topology for {:?} at {:?}. Current neighbors: {:?}",
            resource_type,
            at_time,
            neighbor_locations.len()
        );

        if neighbor_locations.len() < HARD_MINIMUM_NUM_PEERS {
            let mut locations = Vec::new();
            let below_threshold = HARD_MINIMUM_NUM_PEERS - neighbor_locations.len();
            if (below_threshold > 0) {
                for i in 0..below_threshold {
                    locations.push(Location::random());
                }
                info!(
                    minimum_num_peers_hard_limit = HARD_MINIMUM_NUM_PEERS,
                    num_peers = neighbor_locations.len(),
                    to_add = below_threshold,
                    "Adding peers at random locations to reach minimum number of peers"
                );
            }
            return TopologyAdjustment::AddConnections(locations);
        }

        let increase_usage_if_below: RateProportion =
            RateProportion::new(MINIMUM_DESIRED_RESOURCE_USAGE_PROPORTION);
        let decrease_usage_if_above: RateProportion =
            RateProportion::new(MAXIMUM_DESIRED_RESOURCE_USAGE_PROPORTION);

        let usage = self.extrapolated_usage(resource_type, at_time);
        let total_limit: Rate = self.limits.get(resource_type);
        let usage_proportion = usage.total.proportion_of(&total_limit);

        // Detailed resource usage information
        debug!(
            "Resource type {:?} usage: {:?}, Total limit: {:?}, Usage proportion: {:?}",
            resource_type, usage.total, total_limit, usage_proportion
        );

        let adjustment: anyhow::Result<TopologyAdjustment> =
            if neighbor_locations.len() > HARD_MAXIMUM_NUM_PEERS {
                debug!(
                    "Number of neighbors ({:?}) is above maximum ({:?}), removing connections",
                    neighbor_locations.len(),
                    HARD_MAXIMUM_NUM_PEERS
                );
                Ok(self.select_connections_to_remove(&resource_type, &usage.total, at_time))
            } else if usage_proportion < increase_usage_if_below {
                debug!(
                    "{:?} resource usage ({:?}) is below threshold ({:?}), adding connections",
                    resource_type, usage_proportion, increase_usage_if_below
                );
                self.select_connections_to_add(&usage, neighbor_locations)
            } else if usage_proportion > decrease_usage_if_above {
                debug!(
                    "{:?} resource usage ({:?}) is above threshold ({:?}), removing connections",
                    resource_type, usage_proportion, decrease_usage_if_above
                );
                Ok(self.select_connections_to_remove(&resource_type, &usage.total, at_time))
            } else {
                debug!(
                    "{:?} resource usage is within acceptable bounds: {:?}",
                    resource_type, usage_proportion
                );
                Ok(TopologyAdjustment::NoChange)
            };

        match &adjustment {
            Ok(TopologyAdjustment::AddConnections(connections)) => {
                debug!("Added connections: {:?}", connections);
            }
            Ok(TopologyAdjustment::RemoveConnections(connections)) => {
                debug!("Removed connections: {:?}", connections);
            }
            Ok(TopologyAdjustment::NoChange) => {
                debug!("No topology change required.");
            }
            Err(e) => {
                tracing::error!("Couldn't adjust topology due to error: {:?}", e);
            }
        }

        adjustment.unwrap_or(TopologyAdjustment::NoChange)
    }

    fn select_connections_to_add(
        &mut self,
        usage: &Usage,
        neighbor_locations: &BTreeMap<Location, Vec<Connection>>,
    ) -> anyhow::Result<TopologyAdjustment> {
        let function_span = span!(Level::INFO, "add_connections");
        let _enter = function_span.enter();

        debug!("Starting to compute density map");
        let density_map = self
            .request_density_tracker
            .read()
            .create_density_map(neighbor_locations)?;
        debug!("Density map computed successfully");

        debug!("Attempting to get max density location");
        let max_density_location = match density_map.get_max_density() {
            Ok(location) => {
                debug!(location = ?location, "Max density location found");
                location
            }
            Err(e) => {
                error!("Failed to get max density location: {:?}", e);
                return Err(anyhow!(e));
            }
        };

        info!("Adding new connection to {:?}", max_density_location);
        Ok(TopologyAdjustment::AddConnections(vec![
            max_density_location,
        ]))
    }

    fn select_connections_to_remove(
        &mut self,
        resource_type: &ResourceType,
        usage_rate: &Rate,
        at_time: Instant,
    ) -> TopologyAdjustment {
        let function_span = span!(Level::INFO, "remove_connections");
        let _enter = function_span.enter();

        let mut worst: Option<(PeerKeyLocation, f64)> = None;

        for (source, source_usage) in self.meter.get_usage_rates(resource_type, at_time) {
            let loop_span = span!(Level::DEBUG, "source_loop", ?source);
            let _loop_enter = loop_span.enter();

            event!(Level::DEBUG, "Checking source");

            if let Some(creation_time) = self.source_creation_times.get(&source) {
                if Instant::now().duration_since(*creation_time) <= SOURCE_RAMP_UP_DURATION {
                    event!(Level::DEBUG, "Source is in ramp-up time, skipping");
                    continue;
                }
            } else {
                event!(Level::DEBUG, "No creation time for source, skipping");
                continue;
            }

            match source {
                AttributionSource::Peer(peer) => {
                    let peer_span = span!(Level::DEBUG, "peer_processing", ?peer);
                    let _peer_enter = peer_span.enter();

                    let value_per_usage = self.outbound_request_counter.get_request_count(&peer)
                        as f64
                        / source_usage.per_second();

                    event!(
                        Level::DEBUG,
                        request_count =
                            self.outbound_request_counter.get_request_count(&peer) as f64,
                        usage = source_usage.per_second(),
                        value_per_usage = value_per_usage
                    );

                    if let Some((_, worst_value_per_usage)) = worst {
                        if value_per_usage < worst_value_per_usage {
                            worst = Some((peer, value_per_usage));
                            event!(Level::DEBUG, "Found a worse peer");
                        }
                    } else {
                        worst = Some((peer, value_per_usage));
                        event!(Level::DEBUG, "Setting initial worst peer");
                    }
                }
                _ => {
                    event!(Level::DEBUG, "Non-peer source, skipping");
                }
            }
        }

        if let Some((peer, _)) = worst {
            event!(Level::INFO, action = "Recommend peer for removal", peer = ?peer);
            TopologyAdjustment::RemoveConnections(vec![peer])
        } else {
            event!(Level::WARN, "Couldn't find a suitable peer to remove");
            TopologyAdjustment::NoChange
        }
    }
}

#[derive(Debug, Clone)]
pub(crate) enum TopologyAdjustment {
    AddConnections(Vec<Location>),
    RemoveConnections(Vec<PeerKeyLocation>),
    NoChange,
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
    pub min_connections: usize,
    pub max_connections: usize,
}

impl Limits {
    pub fn get(&self, resource_type: ResourceType) -> Rate {
        match resource_type {
            ResourceType::OutboundBandwidthBytes => self.max_upstream_bandwidth,
            ResourceType::InboundBandwidthBytes => self.max_downstream_bandwidth,
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
    use crate::test_utils::with_tracing;
    use std::time::Instant;

    #[test]
    fn test_resource_manager_report() {
        with_tracing(|| {
            // Create a ResourceManager with arbitrary limits
            let limits = Limits {
                max_upstream_bandwidth: Rate::new_per_second(1000.0),
                max_downstream_bandwidth: Rate::new_per_second(1000.0),
                max_connections: 200,
                min_connections: 5,
            };
            let my_location = Some(Location::random());
            let mut resource_manager = ResourceManager::new(limits);

            // Report some usage and test that the total and attributed usage are updated
            let attribution = AttributionSource::Peer(PeerKeyLocation::random());
            let now = Instant::now();
            resource_manager.report_resource_usage(
                &attribution,
                ResourceType::InboundBandwidthBytes,
                100.0,
                now,
            );
            assert_eq!(
                resource_manager
                    .meter
                    .attributed_usage_rate(&attribution, &ResourceType::InboundBandwidthBytes, now)
                    .unwrap()
                    .per_second(),
                100.0
            );
        });
    }

    #[test]
    fn test_remove_connections() {
        with_tracing(|| {
            let mut resource_manager = setup_resource_manager(1000.0);
            let peers = generate_random_peers(5);
            // Total bw usage will be way higher than the limit of 1000
            let bw_usage_by_peer = vec![1000, 1100, 1200, 2000, 1600];
            // Report usage from outside the ramp-up time window so it isn't ignored
            let report_time = Instant::now() - SOURCE_RAMP_UP_DURATION - Duration::from_secs(30);
            report_resource_usage(
                &mut resource_manager,
                &peers,
                &bw_usage_by_peer,
                report_time,
            );
            let requests_per_peer = vec![20, 19, 18, 9, 9];
            report_outbound_requests(&mut resource_manager, &peers, &requests_per_peer);
            let worst_ix = find_worst_peer(&peers, &bw_usage_by_peer, &requests_per_peer);
            assert_eq!(worst_ix, 3);
            let worst_peer = &peers[worst_ix];
            let mut neighbor_locations = BTreeMap::new();
            for peer in &peers {
                neighbor_locations.insert(peer.location.unwrap(), vec![]);
            }

            let adjustment = resource_manager.adjust_topology(
                ResourceType::InboundBandwidthBytes,
                &neighbor_locations,
                Instant::now(),
            );
            match adjustment {
                TopologyAdjustment::RemoveConnections(peers) => {
                    assert_eq!(peers.len(), 1);
                    assert_eq!(peers[0], *worst_peer);
                }
                _ => panic!("Expected to remove a peer, adjustment was {:?}", adjustment),
            }
        });
    }

    #[test]
    fn test_add_connections() {
        with_tracing(|| {
            let mut resource_manager = setup_resource_manager(1000.0);
            // Generate 5 peers with locations specified in a vec!
            let mut peers: Vec<PeerKeyLocation> = generate_random_peers(5);
            let peer_locations: Vec<Location> = [0.1, 0.3, 0.5, 0.7, 0.9]
                .into_iter()
                .map(Location::new)
                .collect();
            for (ix, mut peer) in peers.iter_mut().enumerate() {
                peer.location = Some(peer_locations[ix]);
            }

            // Total bw usage will be way lower than MINIMUM_DESIRED_RESOURCE_USAGE_PROPORTION, triggering
            // the ResourceManager to add a connection
            let bw_usage_by_peer = vec![10, 20, 30, 25, 30];
            // Report usage from outside the ramp-up time window so it isn't ignored
            let report_time = Instant::now() - SOURCE_RAMP_UP_DURATION - Duration::from_secs(30);
            report_resource_usage(
                &mut resource_manager,
                &peers,
                &bw_usage_by_peer,
                report_time,
            );
            let requests_per_peer = vec![20, 19, 18, 9, 9];
            report_outbound_requests(&mut resource_manager, &peers, &requests_per_peer);

            let mut neighbor_locations = BTreeMap::new();
            for peer in &peers {
                neighbor_locations.insert(peer.location.unwrap(), vec![]);
            }

            let adjustment = resource_manager.adjust_topology(
                ResourceType::InboundBandwidthBytes,
                &neighbor_locations,
                Instant::now(),
            );

            match adjustment {
                TopologyAdjustment::AddConnections(locations) => {
                    assert_eq!(locations.len(), 1);
                    // Location should be between peers[0] and peers[1] because they have the highest
                    // number of requests per hour for any adjacent peers
                    assert!(locations[0] >= peers[0].location.unwrap());
                    assert!(locations[0] <= peers[1].location.unwrap());
                }
                _ => panic!(
                    "Expected to add a connection, adjustment was {:?}",
                    adjustment
                ),
            }
        });
    }

    // Test with no adjustment because the usage is within acceptable bounds
    #[test]
    fn test_no_adjustment() {
        with_tracing(|| {
            let mut resource_manager = setup_resource_manager(1000.0);
            let peers = generate_random_peers(5);
            // Total bw usage will be way lower than MINIMUM_DESIRED_RESOURCE_USAGE_PROPORTION, triggering
            // the ResourceManager to add a connection
            let bw_usage_by_peer = vec![150, 200, 100, 100, 200];
            // Report usage from outside the ramp-up time window so it isn't ignored
            let report_time = Instant::now() - SOURCE_RAMP_UP_DURATION - Duration::from_secs(30);
            report_resource_usage(
                &mut resource_manager,
                &peers,
                &bw_usage_by_peer,
                report_time,
            );
            let requests_per_peer = vec![20, 19, 18, 9, 9];
            report_outbound_requests(&mut resource_manager, &peers, &requests_per_peer);

            let mut neighbor_locations = BTreeMap::new();
            for peer in &peers {
                neighbor_locations.insert(peer.location.unwrap(), vec![]);
            }

            let adjustment = resource_manager.adjust_topology(
                ResourceType::InboundBandwidthBytes,
                &neighbor_locations,
                report_time,
            );

            match adjustment {
                TopologyAdjustment::NoChange => {}
                _ => panic!("Expected no adjustment, adjustment was {:?}", adjustment),
            }
        });
    }

    // Test with no peers
    #[test]
    fn test_no_peers() {
        with_tracing(|| {
            let mut resource_manager = setup_resource_manager(1000.0);
            let peers = generate_random_peers(0);
            // Total bw usage will be way lower than MINIMUM_DESIRED_RESOURCE_USAGE_PROPORTION, triggering
            // the ResourceManager to add a connection
            let bw_usage_by_peer = vec![];
            // Report usage from outside the ramp-up time window so it isn't ignored
            let report_time = Instant::now() - SOURCE_RAMP_UP_DURATION - Duration::from_secs(30);
            report_resource_usage(
                &mut resource_manager,
                &peers,
                &bw_usage_by_peer,
                report_time,
            );
            let requests_per_peer = vec![];
            report_outbound_requests(&mut resource_manager, &peers, &requests_per_peer);

            let mut neighbor_locations = BTreeMap::new();
            for peer in &peers {
                neighbor_locations.insert(peer.location.unwrap(), vec![]);
            }

            let adjustment = resource_manager.adjust_topology(
                ResourceType::InboundBandwidthBytes,
                &neighbor_locations,
                report_time,
            );

            match adjustment {
                TopologyAdjustment::AddConnections(v) => {}
                _ => panic!("Expected AddConnections, but was: {:?}", adjustment),
            }
        });
    }

    fn setup_resource_manager(rate: f64) -> ResourceManager {
        let limits = Limits {
            max_upstream_bandwidth: Rate::new_per_second(rate),
            max_downstream_bandwidth: Rate::new_per_second(rate),
            max_connections: 200,
            min_connections: 5,
        };
        ResourceManager::new(limits)
    }

    fn generate_random_peers(num_peers: usize) -> Vec<PeerKeyLocation> {
        (0..num_peers).map(|_| PeerKeyLocation::random()).collect()
    }

    fn report_resource_usage(
        resource_manager: &mut ResourceManager,
        peers: &[PeerKeyLocation],
        bw_usage_by_peer: &[usize],
        up_to_time: Instant,
    ) {
        for (i, peer) in peers.iter().enumerate() {
            // Report usage for the last 10 minutes, which is 2X longer than the ramp-up time
            for seconds in 1..600 {
                let report_time = up_to_time - Duration::from_secs(600 - seconds);
                trace!(
                    "Reporting {} bytes of inbound bandwidth for peer {:?} at {:?}",
                    bw_usage_by_peer[i],
                    peer,
                    report_time
                );
                resource_manager.report_resource_usage(
                    &AttributionSource::Peer(*peer),
                    ResourceType::InboundBandwidthBytes,
                    bw_usage_by_peer[i] as f64,
                    report_time,
                );
            }
        }
    }

    fn report_outbound_requests(
        resource_manager: &mut ResourceManager,
        peers: &[PeerKeyLocation],
        requests_per_peer: &[usize],
    ) {
        for (i, requests) in requests_per_peer.iter().enumerate() {
            for _ in 0..*requests {
                // For simplicity we'll just assume that the target location of the request is the
                // neighboring peer's own location
                resource_manager.report_outbound_request(peers[i], peers[i].location.unwrap());
            }
        }
    }

    fn find_worst_peer(
        peers: &Vec<PeerKeyLocation>,
        bw_usage_by_peer: &[usize],
        requests_per_peer: &[usize],
    ) -> usize {
        let mut values = vec![];
        for ix in 0..peers.len() {
            let peer = peers[ix];
            let value = requests_per_peer[ix] as f64 / bw_usage_by_peer[ix] as f64;
            values.push(value);
            debug!(
                "Peer {:?} has value {}/{} = {}",
                peer, requests_per_peer[ix], bw_usage_by_peer[ix], value
            );
        }
        let mut worst_ix = 0;
        for (ix, value) in values.iter().enumerate() {
            if *value < values[worst_ix] {
                worst_ix = ix;
            }
        }
        worst_ix
    }

    #[test]
    fn test_update_limits() {
        let limits = Limits {
            max_upstream_bandwidth: Rate::new_per_second(1000.0),
            max_downstream_bandwidth: Rate::new_per_second(1000.0),
            max_connections: 200,
            min_connections: 5,
        };
        let mut resource_manager = ResourceManager::new(limits);

        let new_limits = Limits {
            max_upstream_bandwidth: Rate::new_per_second(2000.0),
            max_downstream_bandwidth: Rate::new_per_second(2000.0),
            max_connections: 200,
            min_connections: 5,
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
