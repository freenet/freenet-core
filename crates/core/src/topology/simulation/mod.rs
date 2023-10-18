mod event_stat_tracker;
mod tests;

use crate::ring::Distance;
use super::*;
use event_stat_tracker::EventStatTracker;
use std::collections::{HashMap, HashSet};
use tracing::{debug, info, warn};

#[derive(Debug)]
struct Connections(HashMap<NodeRef, HashSet<NodeRef>>);

impl Connections {
    fn new() -> Self {
        Self(HashMap::new())
    }

    fn connect(&mut self, a: NodeRef, b: NodeRef) {
        // throw an error if a == b
        assert!(a != b, "Cannot connect a node to itself");

        info!("Connecting {:?} and {:?}", a, b);
        self.0.entry(a).or_default().insert(b);
        self.0.entry(b).or_default().insert(a);
    }

    fn disconnect(&mut self, a: NodeRef, b: NodeRef) {
        info!("Disconnecting {:?} and {:?}", a, b);
        self.0.entry(a).or_default().remove(&b);
        self.0.entry(b).or_default().remove(&a);
    }

}

#[derive(Debug)]
struct SimulatedNetwork {
    current_time: u64,
    nodes: Vec<SimulatedNode>,
    connections: Connections,
    requests: HashMap<NodeRef, (EventStatTracker, HashMap<NodeRef, EventStatTracker>)>, // origin node -> destination node -> requests
}

impl SimulatedNetwork {
    pub(crate) fn tick(&mut self) {
        self.current_time += 1;
    }

    pub(crate) fn new() -> Self {
        Self {
            current_time: 0,
            nodes: Vec::new(),
            connections: Connections::new(),
            requests: HashMap::new(),
        }
    }

    pub(crate) 

    fn add_node(&mut self) -> NodeRef {
        let index = self.nodes.len();
        let node = SimulatedNode {
            location: Location::random(),
            index,
        };
        debug!("Adding node {:?}", node);
        self.nodes.push(node);
        NodeRef { index }
    }

    fn route(&self, source: &NodeRef, destination: Location) -> Result<Vec<NodeRef>, RouteError> {
        info!("Routing from {:?} to {:?}", source, destination);
        let mut current = *source; // Dereference to copy
        let mut visited = Vec::new();
        let mut recent_nodes = Vec::new(); // To track the sequence of last N nodes

        loop {
            debug!("Current node: {:?}", current);

            // Check if we've reached the destination
            if self.nodes[current.index].location == destination {
                info!("Reached destination");
                return Ok(visited);
            }

            // Get current node's distance to the destination
            let current_distance = self.nodes[current.index]
                .location
                .distance(destination)
                .as_f64();
            debug!("Current distance to destination: {}", current_distance);

            // Find the closest connected node to the destination that hasn't been visited
            let closest_connections = match self.connections.0.get(&current) {
                Some(connections) => connections,
                None => {
                    // Handle the None case here. For example, you might want to return agit n Err value or break the loop.
                    return Err(RouteError::NoRoute);
                }
            };
            let (closest, closest_distance) = closest_connections
                .iter()
                .map(|&neighbor| {
                    let distance = self.nodes[neighbor.index]
                        .location
                        .distance(destination)
                        .as_f64();
                    (neighbor, distance)
                })
                .min_by(|a, b| a.1.partial_cmp(&b.1).unwrap_or(std::cmp::Ordering::Equal))
                .unwrap_or((current, current_distance));

            debug!(
                "Closest node: {:?}, distance: {}",
                closest, closest_distance
            );

            // Check for a loop by looking at the sequence of last N nodes
            recent_nodes.push(closest);
            if recent_nodes.len() > 10 {
                // Keep only the last 10 visited nodes in the list
                recent_nodes.remove(0);
            }
            if recent_nodes.len() > 2 && recent_nodes.first() == recent_nodes.last() {
                warn!("Loop detected");
                return Err(RouteError::Loop);
            }

            // Update visited nodes and current node
            visited.push(closest);
            current = closest;
        }
    }

    fn record_request(&mut self, a: &NodeRef, b: &NodeRef) {
        let (count, dest_map) = self
            .requests
            .entry(*a)
            .or_insert((EventStatTracker::new(), HashMap::new()));
        count.add_event(self.current_time);
        dest_map
            .entry(*b)
            .or_insert(EventStatTracker::new())
            .add_event(self.current_time);
    }

    fn reset_recorded_requests(&mut self) {
        self.requests.clear();
    }

    fn get_join_peers(
        &self,
        source: &NodeRef,
        target: Location,
        tolerance: Distance,
    ) -> Option<Vec<NodeRef>> {
        info!("Joining via {:?} with target {:?}", source, target);
        let join_route = self.route(source, target);
        let join_route: Vec<NodeRef> = match join_route {
            Ok(route) => route,
            Err(e) => {
                warn!("Error joining network: {:?}", e);
                return Option::None;
            }
        };

        let mut joiners = Vec::new();

        for node in join_route.iter() {
            let node_location = self.nodes[node.index].location;
            let distance = node_location.distance(target);
            if distance < tolerance {
                info!("Found node {:?} within tolerance", node);
                joiners.push(*node);
            }
        }

        Option::Some(joiners)
    }

    fn join(&mut self, node: NodeRef, target: Location, tolerance: Distance) {
        info!("Joining {:?} with target {:?}", node, target);
        let joiners = self.get_join_peers(&node, target, tolerance);
        let joiners: Vec<NodeRef> = match joiners {
            Some(joiners) => joiners,
            None => {
                warn!("Error joining network");
                return;
            }
        };

        for joiner in joiners.iter() {
            self.connections.connect(node, *joiner);
        }
    }

    pub(crate) fn add_and_assimilate_node(&mut self, strategy: TopologyStrategy, peer_statistics: &PeerStatistics) -> NodeRef {
        let new_node = self.add_node();
        
        let my_location = &self.nodes[new_node.index].location;
        
        let JoinTargetInfo { target, threshold, .. } = strategy.select_join_target_location(my_location, peer_statistics);
        
        self.join(new_node, target, threshold);
        
        new_node
    }

}

#[derive(Debug)]
enum RouteError {
    NoRoute,
    Loop,
    DeadEnd,
}

#[derive(Debug)]
struct SimulatedNode {
    index: usize,
    location: Location,
}

#[derive(Clone, Copy, Eq, PartialEq, Hash, Debug)]
struct NodeRef {
    index: usize,
}
