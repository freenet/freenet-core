#![allow(unused_variables, dead_code)]

mod metric;
mod simulation;
mod small_world_rand;

use crate::ring::*;

const DEFAULT_MIN_DISTANCE: f64 = 0.01;

pub(crate) enum TopologyStrategy {
    Simple,
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
            TopologyStrategy::Simple => {
                random_strategy(my_location, peer_statistics, TopologyStrategy::Simple)
            }
            TopologyStrategy::SmallWorld => small_world_metric_strategy(
                my_location,
                peer_statistics,
                TopologyStrategy::SmallWorld,
            ),
            TopologyStrategy::LoadBalancing => load_balancing_strategy(
                my_location,
                peer_statistics,
                TopologyStrategy::LoadBalancing,
            ),
        }
    }
}

pub(crate) fn random_strategy(
    my_location: &Location,
    peer_statistics: &PeerStatistics,
    strategy: TopologyStrategy,
) -> JoinTargetInfo {
    unimplemented!()
}

pub(crate) fn small_world_metric_strategy(
    my_location: &Location,
    peer_statistics: &PeerStatistics,
    strategy: TopologyStrategy,
) -> JoinTargetInfo {
    unimplemented!()
}

pub(crate) fn load_balancing_strategy(
    my_location: &Location,
    peer_statistics: &PeerStatistics,
    strategy: TopologyStrategy,
) -> JoinTargetInfo {
    unimplemented!()
}

pub(crate) fn select_strategy(num_neighbors: usize) -> TopologyStrategy {
    if num_neighbors < 10 {
        TopologyStrategy::Simple
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
