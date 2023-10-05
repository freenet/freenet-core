use crate::network_sim::Location;
use std::collections::{HashMap, HashSet};
use tracing::{debug, info, warn};
use tracing_subscriber::field::debug;

use super::*;

struct SimulatedNetwork {
    nodes: Vec<SimulatedNode>,
    connections: HashMap<NodeRef, HashSet<NodeRef>>,
    requests: HashMap<NodeRef, HashMap<NodeRef, u64>>, // origin node -> destination node -> requests
}

impl SimulatedNetwork {
    fn new() -> Self {
        Self {
            nodes: Vec::new(),
            connections: HashMap::new(),
            requests: HashMap::new(),
        }
    }

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

    fn connect(&mut self, a: NodeRef, b: NodeRef) {
        debug!("Connecting {:?} to {:?}", a, b);
        self.connections.entry(a).or_default().insert(b);
        self.connections.entry(b).or_default().insert(a);
    }

    fn disconnect(&mut self, a: NodeRef, b: NodeRef) {
        debug!("Disconnecting {:?} from {:?}", a, b);
        self.connections.entry(a).or_default().remove(&b);
        self.connections.entry(b).or_default().remove(&a);
    }

    fn route(&self, source: &NodeRef, destination: Location) -> Result<Vec<NodeRef>, RouteError> {
        debug!("Routing from {:?} to {:?}", source, destination);

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
            let closest_connections = match self.connections.get(&current) {
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

    fn join(
        &self,
        source: &NodeRef,
        target: Location,
        tolerance: Distance,
    ) -> Option<Vec<NodeRef>> {
        debug!("Joining via {:?} with target {:?}", source, target);
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
                joiners.push(node.clone());
            }
        }

        Option::Some(joiners)
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

#[cfg(test)]
mod tests;
