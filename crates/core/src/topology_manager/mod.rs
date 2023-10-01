mod metric;
mod small_world_rand;

use std::collections::{BTreeMap, HashMap};
use rand::Rng;
use crate::ring::*;

/// Identifies a location on the ring where 
pub(crate) fn select_join_target_location(my_location : &Location, peer_statistics: &PeerStatistics) -> Location {
    let min_distance : f64 = peer_statistics.0.keys().map(|location| location.distance(my_location)).min().unwrap_or(Distance::new(0.5)).as_f64();

    let maximum_min_distance = 0.01;

    let min_distance = if min_distance > maximum_min_distance { min_distance } else { maximum_min_distance };
    if peer_statistics.0.len() < 10 {
        let dist = small_world_rand::random_link_distance(min_distance);

        return Location::new(0.0);
    }
    todo!()
}  
    

pub struct RequestsPerMinute(f64);

pub struct PeerStatistics(BTreeMap<Location, HashMap<PeerKeyLocation, RequestsPerMinute>>);
