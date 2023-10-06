#![allow(unused_variables, dead_code)]

mod metric;
mod simulation;
mod small_world_rand;

use rand::Rng;

use crate::ring::*;

use self::small_world_rand::random_link_distance;

const DEFAULT_MIN_DISTANCE: f64 = 0.01;

pub(crate) enum TopologyStrategy {
    Random,
    SmallWorld,
    LoadBalancing,
}

pub(crate) struct JoinTargetInfo {
    pub target: Location,
    pub threshold: Distance,
    pub strategy: TopologyStrategy,
}

impl TopologyStrategy {
    pub(crate) fn select_join_target_location(
        &self,
        my_location: &Location,
        peer_statistics: &PeerStatistics,
    ) -> JoinTargetInfo {
        match self {
            TopologyStrategy::Random => {
                random_strategy(my_location, peer_statistics)
            }
            TopologyStrategy::SmallWorld => small_world_metric_strategy(
                my_location,
                peer_statistics,
            ),
            TopologyStrategy::LoadBalancing => load_balancing_strategy(
                my_location,
                peer_statistics,
            ),
        }
    }
}

pub(crate) fn random_strategy(
    my_location: &Location,
    peer_statistics: &PeerStatistics,
) -> JoinTargetInfo {
    // Determine distance to closest neighboring peer
    let mut min_distance = Distance::new(DEFAULT_MIN_DISTANCE);
    for peer in peer_statistics.peers.iter() {
        let distance = my_location.distance(&peer.location);
        if distance < min_distance {
            min_distance = distance;
        }
    }
    let distance_to_target = random_link_distance(min_distance);

    let direction = if rand::thread_rng().gen_bool(0.5) { 1.0 } else { -1.0 };
    let target = Location::new_rounded(my_location.as_f64() * direction);
    let threshold = Distance::new(distance_to_target.as_f64() / 2.0);
    JoinTargetInfo {
        target,
        threshold,
        strategy: TopologyStrategy::Random,
    }
}

pub(crate) fn small_world_metric_strategy(
    my_location: &Location,
    peer_statistics: &PeerStatistics,
) -> JoinTargetInfo {
    unimplemented!()
}

pub(crate) fn load_balancing_strategy(
    my_location: &Location,
    peer_statistics: &PeerStatistics,
) -> JoinTargetInfo {
    unimplemented!()
}

pub(crate) fn select_strategy(num_neighbors: usize) -> TopologyStrategy {
    if num_neighbors < 10 {
        TopologyStrategy::Random
    } else {
        // Randomly select between the three strategies based on your criteria
        unimplemented!()
    }
}

pub(crate) struct RequestsPerMinute(f64);

pub(crate) struct PeerInfo {
    pub location: Location,
    pub requests_per_minute: RequestsPerMinute,
    pub strategy: TopologyStrategy,
}

pub(crate) struct PeerStatistics {
    pub(crate) peers: Vec<PeerInfo>,
}

impl PeerStatistics {
    pub(crate) fn new() -> Self {
        Self { peers: Vec::new() }
    }
}
