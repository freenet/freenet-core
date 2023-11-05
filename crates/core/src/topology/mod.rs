#![allow(unused_variables, dead_code)]

use std::{collections::BTreeMap, rc::Rc, time::{Duration, Instant}};
use tracing::{debug, error, info};
use request_density_tracker::cached_density_map::CachedDensityMap;
use crate::ring::{Distance, Location};

use self::{request_density_tracker::DensityMapError, small_world_rand::random_link_distance};

mod request_density_tracker;
mod small_world_rand;
mod connection_evaluator;

const SLOW_CONNECTION_EVALUATOR_WINDOW_DURATION: Duration = Duration::from_secs(5 * 60);
const FAST_CONNECTION_EVALUATOR_WINDOW_DURATION: Duration = Duration::from_secs(1 * 60);
const REQUEST_DENSITY_TRACKER_WINDOW_SIZE: usize = 10_000;
const REGENERATE_DENSITY_MAP_INTERVAL: Duration = Duration::from_secs(60);
const RANDOM_CLOSEST_DISTANCE: f64 = 1.0 / 1000.0;

/// The goal of `TopologyManager` is to select new connections such that the
/// distribution of connections in the network is as close as possible to the
/// distribution of requests in the network. 
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
    slow_connection_evaluator: connection_evaluator::ConnectionEvaluator,
    fast_connection_evaluator: connection_evaluator::ConnectionEvaluator,
    request_density_tracker: request_density_tracker::RequestDensityTracker,
    cached_density_map: CachedDensityMap,
    this_peer_location: Location,
}

impl TopologyManager {
    /// Create a new TopologyManager specifying the peer's own Location
    pub(crate) fn new(this_peer_location : Location) -> Self {
        info!("Creating a new TopologyManager instance");
        TopologyManager {
            slow_connection_evaluator: connection_evaluator::ConnectionEvaluator::new(SLOW_CONNECTION_EVALUATOR_WINDOW_DURATION),
            fast_connection_evaluator: connection_evaluator::ConnectionEvaluator::new(FAST_CONNECTION_EVALUATOR_WINDOW_DURATION),
            request_density_tracker: request_density_tracker::RequestDensityTracker::new(REQUEST_DENSITY_TRACKER_WINDOW_SIZE),
            cached_density_map: CachedDensityMap::new(REGENERATE_DENSITY_MAP_INTERVAL),
            this_peer_location,
        }
    }

    /// Record a request and the location it's targeting
    pub(crate) fn record_request(&mut self, requested_location: Location, request_type : RequestType) {
        debug!("Recording request for location: {:?}", requested_location);
        self.request_density_tracker.sample(requested_location);
    }

    /// Decide whether to accept a connection from a new candidate peer based on its location
    /// and current neighbors and request density, along with how it compares to other
    /// recent candidates.
    pub(crate) fn evaluate_new_connection(&mut self, current_neighbors: &BTreeMap<Location, usize>, candidate_location: Location, acquisition_strategy : AcquisitionStrategy) -> Result<bool, DensityMapError> {
        self.evaluate_new_connection_with_current_time(current_neighbors, candidate_location, acquisition_strategy, Instant::now())
    }

    fn evaluate_new_connection_with_current_time(&mut self, current_neighbors: &BTreeMap<Location, usize>, candidate_location: Location, acquisition_strategy : AcquisitionStrategy, current_time : Instant) -> Result<bool, DensityMapError> {
        debug!("Evaluating new connection for candidate location: {:?}", candidate_location);
        let density_map = self.get_or_create_density_map(current_neighbors)?;
        let score = density_map.get_density_at(candidate_location)?;

        let accept = match acquisition_strategy {
            AcquisitionStrategy::Slow => {
                self.fast_connection_evaluator.record_only_with_current_time(score, current_time);
                self.slow_connection_evaluator.record_and_eval_with_current_time(score, current_time)
            },
            AcquisitionStrategy::Fast => {
                self.slow_connection_evaluator.record_only_with_current_time(score, current_time);
                self.fast_connection_evaluator.record_and_eval_with_current_time(score, current_time)
            },
        };
        
        Ok(accept)
    }

    /// Get the ideal location for a new connection based on current neighbors and request density
    pub(crate) fn get_best_candidate_location(&mut self, current_neighbors: &BTreeMap<Location, usize>) -> Result<Location, DensityMapError> {
        debug!("Retrieving best candidate location");
        let density_map = self.get_or_create_density_map(current_neighbors)?;
        
        let best_location = match density_map.get_max_density() {
            Ok(location) => {
                debug!("Max density found at location: {:?}", location);
                location
            },
            Err(_) => {
                error!("An error occurred while getting max density, falling back to random location");
                self.random_location()
            },
        };
        
        Ok(best_location)
    }

    /// Generates a random location that is close to the current peer location with a small
    /// world distribution.
    fn random_location(&self) -> Location {
        debug!("Generating random location");
        let distance = random_link_distance(Distance::new(RANDOM_CLOSEST_DISTANCE));
        let location_f64 = if rand::random() {
            self.this_peer_location.as_f64() - distance.as_f64()
        } else {
            self.this_peer_location.as_f64() + distance.as_f64()
        };
        let location_f64 = location_f64.rem_euclid(1.0);  // Ensure result is in [0.0, 1.0)
        Location::new(location_f64)
    }    

    fn get_or_create_density_map(&mut self, current_neighbors: &BTreeMap<Location, usize>) -> Result<Rc<request_density_tracker::DensityMap>, DensityMapError> {
        debug!("Getting or creating density map");
        self.cached_density_map.get_or_create(&self.request_density_tracker, current_neighbors)
    }
}

pub(crate) enum RequestType {
    Get,
    Put,
    Join,
    Subscribe
}

pub(crate) enum AcquisitionStrategy {
    /// Acquire new connections slowly, be picky
    Slow,

    /// Acquire new connections aggressively, be less picky
    Fast,
}

#[cfg(test)]
mod tests {
    use crate::ring::Location;
    use super::TopologyManager;

    #[test]
    fn test_topology_manager() {
        let mut topology_manager = TopologyManager::new(Location::new(0.39));
        let mut current_neighbors = std::collections::BTreeMap::new();

        // Insert neighbors from 0.0 to 0.9
        for i in 0..10 {
            current_neighbors.insert(Location::new(i as f64 / 10.0), 0);
        }

        let mut requests = vec![];
        // Simulate a bunch of random requests clustered around 0.35
        for _ in 0..1000 {
            let requested_location = topology_manager.random_location();
            topology_manager.record_request(requested_location, super::RequestType::Get);
            requests.push(requested_location);
        }

        let best_candidate_location = topology_manager.get_best_candidate_location(&current_neighbors).unwrap();
        // Should be half way between 0.3 and 0.4 as that is where the most requests were
        assert_eq!(best_candidate_location, Location::new(0.35));

        // call evaluate_new_connection for locations 0.0 to 1.0 at 0.01 intervals and find the
        // location with the highest score
        let mut best_score = 0.0;
        let mut best_location = Location::new(0.0);
        for i in 0..100 {
            let candidate_location = Location::new(i as f64 / 100.0);
            let score = topology_manager
                .get_or_create_density_map(&current_neighbors)
                .unwrap().get_density_at(candidate_location).unwrap();
            if score > best_score {
                best_score = score;
                best_location = candidate_location;
            }
        }

        // Best location should be 0.4 as that is closest to 0.39 which is the peer's location and
        // the request epicenter
        assert_eq!(best_location, Location::new(0.4));
    }
}
