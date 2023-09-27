mod small_world_deviation_metric;

use std::collections::{BTreeMap, HashMap};
use rand::Rng;
use crate::ring::*;

pub(crate) struct TopologyManager {}

impl TopologyManager {

    /// Identifies a location on the ring where 
    pub(crate) fn select_join_target_location(&self, peer_requests: PeerStatistics) -> Location {
        
    }  
    
}

pub struct RequestsPerMinute(f64);

pub struct PeerStatistics(BTreeMap<Location, HashMap<PeerKeyLocation, RequestsPerMinute>>);
