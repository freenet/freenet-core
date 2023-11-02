#![allow(unused_variables, dead_code)]

use std::{time::Duration, collections::BTreeMap, rc::Rc};
use tracing::{info, debug, error};
use crate::ring::{Location, Distance};

use self::{request_density_tracker::DensityMapError, cached_density_map::CachedDensityMap, small_world_rand::random_link_distance};

mod request_density_tracker;
mod small_world_rand;
mod connection_evaluator;
mod cached_density_map;

const CONNECTION_EVALUATOR_WINDOW_DURATION: Duration = Duration::from_secs(5 * 60);
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
    connection_evaluator: connection_evaluator::ConnectionEvaluator,
    request_density_tracker: request_density_tracker::RequestDensityTracker,
    cached_density_map: CachedDensityMap,
    this_peer_location: Location,
}

impl TopologyManager {
    pub(crate) fn new(this_peer_location : Location) -> Self {
        info!("Creating a new TopologyManager instance");
        TopologyManager {
            connection_evaluator: connection_evaluator::ConnectionEvaluator::new(CONNECTION_EVALUATOR_WINDOW_DURATION),
            request_density_tracker: request_density_tracker::RequestDensityTracker::new(REQUEST_DENSITY_TRACKER_WINDOW_SIZE),
            cached_density_map: CachedDensityMap::new(REGENERATE_DENSITY_MAP_INTERVAL),
            this_peer_location: this_peer_location,
        }
    }

    pub(crate) fn record_request(&mut self, requested_location: Location) {
        debug!("Recording request for location: {:?}", requested_location);
        self.request_density_tracker.sample(requested_location);
    }

    pub(crate) fn evaluate_new_connection(&mut self, current_neighbors: &BTreeMap<Location, usize>, candidate_location: Location) -> Result<bool, DensityMapError> {
        debug!("Evaluating new connection for candidate location: {:?}", candidate_location);
        let density_map = self.get_or_create_density_map(current_neighbors)?;
        let score = density_map.get_density_at(candidate_location)?;

        Ok(self.connection_evaluator.record(score))
    }

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

