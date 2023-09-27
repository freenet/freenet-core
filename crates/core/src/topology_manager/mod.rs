mod small_world_deviation_metric;

use std::collections::{BTreeMap, HashMap};
use rand::Rng;
use crate::ring::*;

pub(crate) struct TopologyManager {}

impl TopologyManager {

    pub(crate) fn select_join_target_location(&self, peer_requests: PeerStatistics) -> Location {
        let mut max_combined_rpm = 0.0;
        let mut max_combined_rpm_location = None;
    
        for (_, peer_key_location_map) in peer_requests.0.iter() {
            let mut peer_key_locations: Vec<_> = peer_key_location_map.iter().collect();
            peer_key_locations.sort_by_key(|(peer_key_location, _)| peer_key_location.location);
    
            let len = peer_key_locations.len();
            for i in 0..len {
                let next_i = (i + 1) % len;  // Wrap-around index
                let combined_rpm = peer_key_locations[i].1.0 + peer_key_locations[next_i].1.0;
    
                if combined_rpm > max_combined_rpm {
                    max_combined_rpm = combined_rpm;
                    max_combined_rpm_location = Some((
                        peer_key_locations[i].0.location,
                        peer_key_locations[next_i].0.location,
                    ));
                }
            }
        }
    
        if let Some((location1, location2)) = max_combined_rpm_location {
            let mut rng = rand::thread_rng();
            let random_location = if location1 < location2 {
                rng.gen_range(location1.unwrap().0 .. location2.unwrap().0)
            } else {
                // Wrap-around case
                let upper_weight = 1.0 - location1.unwrap().0;
                let lower_weight = location2.unwrap().0;
                let total_weight = upper_weight + lower_weight;
                let random_weight = rng.gen_range(0.0..total_weight);
        
                if random_weight < upper_weight {
                    location1.unwrap().0 + random_weight
                } else {
                    random_weight - upper_weight
                }
            };
            return Location(random_location);
        }
        
    
        Location::random()
    }  
    
}

pub struct RequestsPerMinute(f64);

pub struct PeerStatistics(BTreeMap<Location, HashMap<PeerKeyLocation, RequestsPerMinute>>);
