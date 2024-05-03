use crate::{message::TransactionType, ring::Location};
use anyhow::anyhow;
use connection_evaluator::ConnectionEvaluator;
use meter::Meter;
use outbound_request_counter::OutboundRequestCounter;
use request_density_tracker::{CachedDensityMap, RequestDensityTracker};
use std::cmp::Ordering;
use std::{
    collections::{BTreeMap, HashMap},
    time::Instant,
};
use tracing::{debug, error, event, info, span, Level};

pub mod connection_evaluator;
mod constants;
pub(crate) mod meter;
pub(crate) mod outbound_request_counter;
pub(crate) mod rate;
pub mod request_density_tracker;
pub(crate) mod running_average;
mod small_world_rand;

use crate::ring::{Connection, PeerKeyLocation};
use crate::topology::meter::{AttributionSource, ResourceType};
use crate::topology::rate::{Rate, RateProportion};
use constants::*;
use request_density_tracker::DensityMapError;

/// The goal of `TopologyManager` is to select new connections such that the
/// distribution of connections is as close as possible to the
/// distribution of outbound requests.
///
/// This is done by maintaining a `RequestDensityTracker` which tracks the
/// distribution of requests in the network. The `TopologyManager` uses this
/// tracker to create a `DensityMap` which is used to evaluate the density of
/// requests at a given location.
///
/// The `TopologyManager` uses the density map to select the best candidate
/// location, which is assumed to be close to peer connections that are
/// currently receiving a lot of requests. This should have the effect of
/// "balancing" out requests over time.
///
/// The `TopologyManager` also uses a `ConnectionEvaluator` to evaluate whether
/// a given connection is better than all other connections within a predefined
/// time window. The goal of this is to select the best connections over time
/// from incoming join requests.
pub(crate) struct TopologyManager {
    limits: Limits,
    meter: Meter,
    source_creation_times: HashMap<AttributionSource, Instant>,
    slow_connection_evaluator: ConnectionEvaluator,
    fast_connection_evaluator: ConnectionEvaluator,
    request_density_tracker: RequestDensityTracker,
    pub(crate) outbound_request_counter: OutboundRequestCounter,
    /// Must be updated when new neightbors are discovered.
    cached_density_map: CachedDensityMap,
    connection_acquisition_strategy: ConnectionAcquisitionStrategy,
}

impl TopologyManager {
    /// Create a new TopologyManager specifying the peer's own Location
    pub(crate) fn new(limits: Limits) -> Self {
        TopologyManager {
            meter: Meter::new_with_window_size(100),
            limits,
            source_creation_times: HashMap::new(),
            slow_connection_evaluator: ConnectionEvaluator::new(
                SLOW_CONNECTION_EVALUATOR_WINDOW_DURATION,
            ),
            fast_connection_evaluator: ConnectionEvaluator::new(
                FAST_CONNECTION_EVALUATOR_WINDOW_DURATION,
            ),
            request_density_tracker: RequestDensityTracker::new(
                REQUEST_DENSITY_TRACKER_WINDOW_SIZE,
            ),
            cached_density_map: CachedDensityMap::new(),
            outbound_request_counter: OutboundRequestCounter::new(
                OUTBOUND_REQUEST_COUNTER_WINDOW_SIZE,
            ),
            connection_acquisition_strategy: ConnectionAcquisitionStrategy::Fast,
        }
    }

    pub(crate) fn refresh_cache(
        &mut self,
        neighbor_locations: &BTreeMap<Location, Vec<Connection>>,
    ) -> Result<(), DensityMapError> {
        self.cached_density_map
            .set(&self.request_density_tracker, neighbor_locations)?;
        Ok(())
    }

    /// Record a request and the location it's targeting
    pub(crate) fn record_request(
        &mut self,
        recipient: PeerKeyLocation,
        target: Location,
        request_type: TransactionType,
    ) {
        debug!(%request_type, %recipient, "Recording request sent to peer");

        self.request_density_tracker.sample(target);
        self.outbound_request_counter.record_request(recipient);
    }

    /// Decide whether to accept a connection from a new candidate peer based on its location
    /// and current neighbors and request density, along with how it compares to other
    /// recent candidates.
    pub(crate) fn evaluate_new_connection(
        &mut self,
        candidate_location: Location,
        current_time: Instant,
    ) -> Result<bool, DensityMapError> {
        tracing::debug!(
            "Evaluating new connection for candidate location: {:?}",
            candidate_location
        );
        let density_map = self
            .cached_density_map
            .get()
            .ok_or(DensityMapError::EmptyNeighbors)?;
        let score = density_map.get_density_at(candidate_location)?;

        let accept = match self.connection_acquisition_strategy {
            ConnectionAcquisitionStrategy::Slow => {
                self.fast_connection_evaluator
                    .record_only_with_current_time(score, current_time);
                self.slow_connection_evaluator
                    .record_and_eval_with_current_time(score, current_time)
            }
            ConnectionAcquisitionStrategy::Fast => {
                self.slow_connection_evaluator
                    .record_only_with_current_time(score, current_time);
                self.fast_connection_evaluator
                    .record_and_eval_with_current_time(score, current_time)
            }
        };

        Ok(accept)
    }

    #[cfg(test)]
    /// Get the ideal location for a new connection based on current neighbors and request density
    fn get_best_candidate_location(
        &self,
        this_peer_location: &Location,
    ) -> Result<Location, DensityMapError> {
        let density_map = self
            .cached_density_map
            .get()
            .ok_or(DensityMapError::EmptyNeighbors)?;

        let best_location = match density_map.get_max_density() {
            Ok(location) => {
                debug!("Max density found at location: {:?}", location);
                location
            }
            Err(_) => {
                debug!(
                    "An error occurred while getting max density, falling back to random location"
                );
                *this_peer_location
            }
        };
        Ok(best_location)
    }

    #[cfg(test)]
    pub(self) fn update_limits(&mut self, limits: Limits) {
        self.limits = limits;
    }

    /// Report the use of a resource with multiple attribution sources, splitting the usage
    /// evenly between the sources.
    /// This should be done in the lowest-level functions that consume the resource, taking
    /// an AttributionMeter as a parameter. This will be useful for contracts with multiple
    /// subscribers - where the responsibility should be split evenly among the subscribers.
    #[allow(dead_code)] // todo: maybe use this
    pub(crate) fn report_split_resource_usage(
        &mut self,
        attributions: &[AttributionSource],
        resource: ResourceType,
        value: f64,
        at_time: Instant,
    ) {
        let split_value = value / attributions.len() as f64;
        for attribution in attributions {
            self.report_resource_usage(attribution, resource, split_value, at_time);
        }
    }

    #[allow(dead_code)] // fixme: use this
    pub(crate) fn report_resource_usage(
        &mut self,
        attribution: &AttributionSource,
        resource: ResourceType,
        amount: f64,
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

        self.meter.report(attribution, resource, amount, at_time);
    }

    /// Record an outbound request to a peer, along with the target Location of that request
    pub(crate) fn report_outbound_request(&mut self, peer: PeerKeyLocation, target: Location) {
        self.request_density_tracker.sample(target);
        self.outbound_request_counter.record_request(peer);
    }

    /// Calculate total usage for a resource type extrapolating usage for resources that
    /// are younger than [SOURCE_RAMP_UP_DURATION]
    fn extrapolated_usage(&mut self, resource_type: &ResourceType, now: Instant) -> Usage {
        let function_span = span!(Level::DEBUG, "extrapolated_usage_function");
        let _enter = function_span.enter();

        let mut total_usage: Rate = Rate::new_per_second(0.0);
        let mut usage_per_source: HashMap<AttributionSource, Rate> = HashMap::new();

        // Step 1: Collect data
        let collect_data_span = span!(Level::DEBUG, "collect_data");
        let _collect_data_guard = collect_data_span.enter();
        debug!("Collecting data from source_creation_times");
        let mut usage_data = Vec::new();
        for (source, creation_time) in self.source_creation_times.iter() {
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
                self.meter.get_adjusted_usage_rate(resource_type, now)
            } else {
                debug!("Source {:?} is not ramping up", source);
                self.attributed_usage_rate(&source, resource_type, now)
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
    pub(crate) fn adjust_topology(
        &mut self,
        neighbor_locations: &BTreeMap<Location, Vec<Connection>>,
        my_location: &Option<Location>,
        at_time: Instant,
    ) -> TopologyAdjustment {
        #[cfg(debug_assertions)]
        {
            thread_local! {
                static LAST_LOG: std::cell::RefCell<Instant> = std::cell::RefCell::new(Instant::now());
            }
            if LAST_LOG
                .with(|last_log| last_log.borrow().elapsed() > std::time::Duration::from_secs(10))
            {
                LAST_LOG.with(|last_log| {
                    tracing::trace!(
                        "Adjusting topology at {:?}. Current neighbors: {:?}",
                        at_time,
                        neighbor_locations.len()
                    );
                    *last_log.borrow_mut() = Instant::now();
                });
            }
        }

        if neighbor_locations.len() < self.limits.min_connections {
            let mut locations = Vec::new();
            let below_threshold = self.limits.min_connections - neighbor_locations.len();
            if below_threshold > 0 {
                for _i in 0..below_threshold {
                    match my_location {
                        Some(location) => {
                            // The first few connect messages should target the peer's own
                            // location (if known), to reduce the danger of a peer failing to
                            // cluster
                            locations.push(*location);
                        }
                        None => {
                            locations.push(Location::random());
                        }
                    }
                }
                #[cfg(debug_assertions)]
                {
                    thread_local! {
                        static LAST_LOG: std::cell::RefCell<Instant> = std::cell::RefCell::new(Instant::now());
                    }
                    if LAST_LOG.with(|last_log| {
                        last_log.borrow().elapsed() > std::time::Duration::from_secs(10)
                    }) {
                        LAST_LOG.with(|last_log| {
                            tracing::trace!(
                                minimum_num_peers_hard_limit = self.limits.min_connections,
                                num_peers = neighbor_locations.len(),
                                to_add = below_threshold,
                                "Adding peers at random locations to reach minimum number of peers"
                            );
                            *last_log.borrow_mut() = Instant::now();
                        });
                    }
                }
            }
            return TopologyAdjustment::AddConnections(locations);
        }

        let increase_usage_if_below: RateProportion =
            RateProportion::new(MINIMUM_DESIRED_RESOURCE_USAGE_PROPORTION);
        let decrease_usage_if_above: RateProportion =
            RateProportion::new(MAXIMUM_DESIRED_RESOURCE_USAGE_PROPORTION);

        let usage_rates = self.calculate_usage_proportion(at_time);

        let (resource_type, usage_proportion) = usage_rates.max_usage_rate;

        // Detailed resource usage information
        debug!("Usage proportions: {:?}", usage_rates);

        let adjustment: anyhow::Result<TopologyAdjustment> =
            if neighbor_locations.len() > self.limits.max_connections {
                debug!(
                    "Number of neighbors ({:?}) is above maximum ({:?}), removing connections",
                    neighbor_locations.len(),
                    self.limits.max_connections
                );

                self.update_connection_acquisition_strategy(ConnectionAcquisitionStrategy::Slow);

                Ok(self.select_connections_to_remove(&resource_type, at_time))
            } else if usage_proportion < increase_usage_if_below {
                debug!(
                    "{:?} resource usage ({:?}) is below threshold ({:?}), adding connections",
                    resource_type, usage_proportion, increase_usage_if_below
                );
                self.update_connection_acquisition_strategy(ConnectionAcquisitionStrategy::Fast);
                self.select_connections_to_add(neighbor_locations)
            } else if usage_proportion > decrease_usage_if_above {
                debug!(
                    "{:?} resource usage ({:?}) is above threshold ({:?}), removing connections",
                    resource_type, usage_proportion, decrease_usage_if_above
                );
                Ok(self.select_connections_to_remove(&resource_type, at_time))
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
                error!("Couldn't adjust topology due to error: {:?}", e);
            }
        }

        adjustment.unwrap_or(TopologyAdjustment::NoChange)
    }

    fn calculate_usage_proportion(&mut self, at_time: Instant) -> UsageRates {
        let mut usage_rate_per_type = HashMap::new();
        for resource_type in ResourceType::all() {
            let usage = self.extrapolated_usage(&resource_type, at_time);
            let proportion = usage.total.proportion_of(&self.limits.get(&resource_type));
            usage_rate_per_type.insert(resource_type, proportion);
        }

        let max_usage_rate = usage_rate_per_type
            .iter()
            .max_by(|(_, a), (_, b)| a.partial_cmp(b).unwrap_or(Ordering::Equal))
            .unwrap();

        UsageRates {
            // TODO: Is there a way to avoid this clone()?
            usage_rate_per_type: usage_rate_per_type.clone(),
            max_usage_rate: (*max_usage_rate.0, *max_usage_rate.1),
        }
    }

    /// modify the current connection acquisition strategy
    fn update_connection_acquisition_strategy(
        &mut self,
        new_strategy: ConnectionAcquisitionStrategy,
    ) {
        self.connection_acquisition_strategy = new_strategy;
    }

    fn select_connections_to_add(
        &mut self,
        neighbor_locations: &BTreeMap<Location, Vec<Connection>>,
    ) -> anyhow::Result<TopologyAdjustment> {
        let function_span = span!(Level::INFO, "add_connections");
        let _enter = function_span.enter();

        debug!("Starting to compute density map");
        let density_map = self
            .request_density_tracker
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
        exceeded_usage_for_resource_type: &ResourceType,
        at_time: Instant,
    ) -> TopologyAdjustment {
        let function_span = span!(Level::INFO, "remove_connections");
        let _enter = function_span.enter();

        let mut worst: Option<(PeerKeyLocation, f64)> = None;

        for (source, source_usage) in self
            .meter
            .get_usage_rates(exceeded_usage_for_resource_type, at_time)
        {
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

                    let request_count =
                        self.outbound_request_counter.get_request_count(&peer) as f64;

                    let value_per_usage = request_count / source_usage.per_second();

                    event!(
                        Level::DEBUG,
                        request_count = request_count,
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

#[derive(PartialEq, Debug, Clone, Copy)]
pub(crate) enum ConnectionAcquisitionStrategy {
    /// Acquire new connections slowly, be picky
    Slow,

    /// Acquire new connections aggressively, be less picky
    Fast,
}

#[cfg(test)]
mod tests {
    #[test]
    fn test_topology() {
        const NUM_REQUESTS: usize = 1_000;
        let mut topology_manager = TopologyManager::new(Limits {
            max_upstream_bandwidth: Rate::new_per_second(1000.0),
            max_downstream_bandwidth: Rate::new_per_second(1000.0),
            min_connections: 5,
            max_connections: 200,
        });
        let mut current_neighbors = std::collections::BTreeMap::new();

        // Insert neighbors from 0.0 to 0.9
        for i in 0..10 {
            current_neighbors.insert(Location::new(i as f64 / 10.0), vec![]);
        }

        let this_peer_location = Location::new(0.39);

        // Simulate a bunch of random requests clustered around 0.35
        for _ in 0..NUM_REQUESTS {
            let requested_location = random_location(&random_location(&this_peer_location));
            // todo: Is PeerKeyLocation unimportant for this test?
            topology_manager.record_request(
                PeerKeyLocation::random(),
                requested_location,
                TransactionType::Get,
            );
        }

        topology_manager
            .cached_density_map
            .set(
                &topology_manager.request_density_tracker,
                &current_neighbors,
            )
            .unwrap();

        let best_candidate_location = topology_manager
            .get_best_candidate_location(&this_peer_location)
            .unwrap();
        // Should be half way between 0.3 and 0.4 as that is where the most requests were.
        assert_eq!(best_candidate_location, Location::new(0.35));

        // call evaluate_new_connection for locations 0.0 to 1.0 at 0.01 intervals and find the
        // location with the highest score
        let mut best_score = 0.0;
        let mut best_location = Location::new(0.0);
        for i in 0..100 {
            let candidate_location = Location::new(i as f64 / 100.0);
            let score = topology_manager
                .cached_density_map
                .get()
                .unwrap()
                .get_density_at(candidate_location)
                .unwrap();
            if score > best_score {
                best_score = score;
                best_location = candidate_location;
            }
        }

        // Best location should be 0.4 as that is closest to 0.39 which is the peer's location and
        // the request epicenter
        assert_eq!(best_location, Location::new(0.4));
    }

    /// Generates a random location that is close to the current peer location with a small
    /// world distribution.
    fn random_location(this_peer_location: &Location) -> Location {
        tracing::debug!("Generating random location");
        let distance = small_world_rand::test_utils::random_link_distance(Distance::new(0.001));
        let location_f64 = if rand::random() {
            this_peer_location.as_f64() - distance.as_f64()
        } else {
            this_peer_location.as_f64() + distance.as_f64()
        };
        let location_f64 = location_f64.rem_euclid(1.0);
        Location::new(location_f64)
    }

    use super::*;
    use crate::ring::Distance;
    use crate::test_utils::with_tracing;
    use std::time::Duration;

    #[test]
    fn test_resource_manager_report() {
        with_tracing(|| {
            // Create a TopologyManager with arbitrary limits
            let limits = Limits {
                max_upstream_bandwidth: Rate::new_per_second(1000.0),
                max_downstream_bandwidth: Rate::new_per_second(1000.0),
                max_connections: 200,
                min_connections: 5,
            };
            let mut resource_manager = TopologyManager::new(limits);

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
            let mut resource_manager = setup_topology_manager(1000.0);
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

            let adjustment =
                resource_manager.adjust_topology(&neighbor_locations, &None, Instant::now());
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
            let mut resource_manager = setup_topology_manager(1000.0);
            // Generate 5 peers with locations specified in a vec!
            let mut peers: Vec<PeerKeyLocation> = generate_random_peers(5);
            let peer_locations: Vec<Location> = [0.1, 0.3, 0.5, 0.7, 0.9]
                .into_iter()
                .map(Location::new)
                .collect();
            for (ix, peer) in peers.iter_mut().enumerate() {
                peer.location = Some(peer_locations[ix]);
            }

            // Total bw usage will be way lower than MINIMUM_DESIRED_RESOURCE_USAGE_PROPORTION, triggering
            // the TopologyManager to add a connection
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

            let adjustment =
                resource_manager.adjust_topology(&neighbor_locations, &None, Instant::now());

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
            let mut resource_manager = setup_topology_manager(1000.0);
            let peers = generate_random_peers(5);
            // Total bw usage will be way lower than MINIMUM_DESIRED_RESOURCE_USAGE_PROPORTION, triggering
            // the TopologyManager to add a connection
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

            let adjustment =
                resource_manager.adjust_topology(&neighbor_locations, &None, report_time);

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
            let mut resource_manager = setup_topology_manager(1000.0);
            let peers = generate_random_peers(0);
            // Total bw usage will be way lower than MINIMUM_DESIRED_RESOURCE_USAGE_PROPORTION, triggering
            // the TopologyManager to add a connection
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

            let my_location = Location::random();

            let adjustment = resource_manager.adjust_topology(
                &neighbor_locations,
                &Some(my_location),
                report_time,
            );

            match adjustment {
                TopologyAdjustment::AddConnections(v) => {
                    assert!(!v.is_empty());
                    for location in v {
                        assert_eq!(location, my_location);
                    }
                }
                _ => panic!("Expected AddConnections, but was: {:?}", adjustment),
            }
        });
    }

    fn setup_topology_manager(max_downstream_rate: f64) -> TopologyManager {
        let limits = Limits {
            // This won't be used
            max_upstream_bandwidth: Rate::new_per_second(100000.0),
            max_downstream_bandwidth: Rate::new_per_second(max_downstream_rate),
            max_connections: 200,
            min_connections: 5,
        };
        TopologyManager::new(limits)
    }

    fn generate_random_peers(num_peers: usize) -> Vec<PeerKeyLocation> {
        (0..num_peers).map(|_| PeerKeyLocation::random()).collect()
    }

    fn report_resource_usage(
        resource_manager: &mut TopologyManager,
        peers: &[PeerKeyLocation],
        bw_usage_by_peer: &[usize],
        up_to_time: Instant,
    ) {
        for (i, peer) in peers.iter().enumerate() {
            // Report usage for the last 10 minutes, which is 2X longer than the ramp-up time
            for seconds in 1..600 {
                let report_time = up_to_time - Duration::from_secs(600 - seconds);
                tracing::trace!(
                    "Reporting {} bytes of inbound bandwidth for peer {:?} at {:?}",
                    bw_usage_by_peer[i],
                    peer,
                    report_time
                );
                resource_manager.report_resource_usage(
                    &AttributionSource::Peer(peer.clone()),
                    ResourceType::InboundBandwidthBytes,
                    bw_usage_by_peer[i] as f64,
                    report_time,
                );
            }
        }
    }

    fn report_outbound_requests(
        resource_manager: &mut TopologyManager,
        peers: &[PeerKeyLocation],
        requests_per_peer: &[usize],
    ) {
        for (i, requests) in requests_per_peer.iter().enumerate() {
            for _ in 0..*requests {
                // For simplicity we'll just assume that the target location of the request is the
                // neighboring peer's own location
                resource_manager
                    .report_outbound_request(peers[i].clone(), peers[i].location.unwrap());
            }
        }
    }

    fn find_worst_peer(
        peers: &[PeerKeyLocation],
        bw_usage_by_peer: &[usize],
        requests_per_peer: &[usize],
    ) -> usize {
        let mut values = vec![];
        for ix in 0..peers.len() {
            let peer = peers[ix].clone();
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
        let mut topology_manager = TopologyManager::new(limits);

        let new_limits = Limits {
            max_upstream_bandwidth: Rate::new_per_second(2000.0),
            max_downstream_bandwidth: Rate::new_per_second(2000.0),
            max_connections: 200,
            min_connections: 5,
        };
        topology_manager.update_limits(new_limits);

        assert_eq!(
            topology_manager.limits.max_upstream_bandwidth,
            Rate::new_per_second(2000.0)
        );
        assert_eq!(
            topology_manager.limits.max_downstream_bandwidth,
            Rate::new_per_second(2000.0)
        );
    }
}

#[derive(Debug, Clone)]
pub(crate) enum TopologyAdjustment {
    AddConnections(Vec<Location>),
    RemoveConnections(Vec<PeerKeyLocation>),
    NoChange,
}

struct Usage {
    total: Rate,
    #[allow(unused)]
    per_source: HashMap<AttributionSource, Rate>,
}

pub(crate) struct Limits {
    pub max_upstream_bandwidth: Rate,
    pub max_downstream_bandwidth: Rate,
    pub min_connections: usize,
    pub max_connections: usize,
}

impl Limits {
    pub fn get(&self, resource_type: &ResourceType) -> Rate {
        match resource_type {
            ResourceType::OutboundBandwidthBytes => self.max_upstream_bandwidth,
            ResourceType::InboundBandwidthBytes => self.max_downstream_bandwidth,
        }
    }
}

#[derive(Debug, Clone)]
struct UsageRates {
    #[allow(unused)]
    usage_rate_per_type: HashMap<ResourceType, RateProportion>,
    max_usage_rate: (ResourceType, RateProportion),
}
