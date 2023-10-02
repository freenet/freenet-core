mod metric;
mod small_world_rand;

use std::collections::{BTreeMap, HashMap};
use rand::Rng;
use crate::ring::*;

const DEFAULT_MIN_DISTANCE : f64 = 0.01;

/// Identifies a location on the ring where
pub(crate) fn select_join_target_location(my_location: &Location, peer_statistics: &PeerStatistics) -> Location {
    let min_distance = peer_statistics
        .location_map
        .keys()
        .map(|location| location.distance(my_location))
        .min()
        .map_or(Distance::new(0.5), |d| d);

    let min_distance = if min_distance.as_f64() > DEFAULT_MIN_DISTANCE {
        min_distance
    } else {
        Distance::new(DEFAULT_MIN_DISTANCE)
    };

    let dist = small_world_rand::random_link_distance(min_distance);
    
    let mut rng = rand::thread_rng();
    let direction: f64 = if rng.gen_bool(0.5) { 1.0 } else { -1.0 };

    Location::new_rounded(my_location.as_f64() + (direction * dist.as_f64()))
}

pub(crate) struct RequestsPerMinute(f64);

pub(crate) struct PeerStatistics {
    pub(crate) location_map : BTreeMap<Location, Vec<RequestsPerMinute>>,
}
