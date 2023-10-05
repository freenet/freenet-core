use std::collections::{HashMap, HashSet};
use crate::network_sim::Location;

use super::*;

struct SimulatedNetwork {
    nodes: Vec<SimulatedNode>,
    connections: HashMap<NodeRef, HashSet<NodeRef>>,
    requests : HashMap<NodeRef, HashMap<NodeRef, u64>>, // origin node -> destination node -> requests
}

impl SimulatedNetwork {
    fn new() -> Self {
        Self {
            nodes: Vec::new(),
            connections: HashMap::new(),
            requests: HashMap::new(),
        }
    }

    fn add_node(&mut self,) -> NodeRef {
        let index = self.nodes.len();
        let node = SimulatedNode { location: Location::random(), index};
        self.nodes.push(node);
        NodeRef { index }
    }

    fn connect(&mut self, a: NodeRef, b: NodeRef) {
        self.connections.entry(a).or_default().insert(b);
        self.connections.entry(b).or_default().insert(a);
    }

    fn disconnect(&mut self, a: NodeRef, b: NodeRef) {
        self.connections.entry(a).or_default().remove(&b);
        self.connections.entry(b).or_default().remove(&a);
    }

    fn route(&self, source: &NodeRef, destination: Location) -> Result<Vec<NodeRef>, RouteError> {
        let mut current = *source;  // Dereference to copy
        let mut visited = Vec::new();
    
        loop {
            // Check if we've reached the destination
            if self.nodes[current.index].location == destination {
                return Ok(visited);
            }
    
            // Get current node's distance to the destination
            let current_distance = self.nodes[current.index].location.distance(destination).as_f64();
    
            // Find the closest connected node to the destination that hasn't been visited
            let (closest, closest_distance) = self.connections[&current].iter()
                .map(|&neighbor| {
                    let distance = self.nodes[neighbor.index].location.distance(destination).as_f64();
                    (neighbor, distance)
                })
                .min_by(|a, b| a.1.partial_cmp(&b.1).unwrap_or(std::cmp::Ordering::Equal))
                .unwrap_or((current, current_distance));
    
            // Check for a loop
            if visited.contains(&closest) {
                return Err(RouteError::Loop);
            }
    
            // If no closer node is found, we've reached a dead end, which is a success condition in this sim
            if closest_distance >= current_distance {
                return Ok(visited);
            }
    
            // Update visited nodes and current node
            visited.push(closest);
            current = closest;
        }
    }
     
}

enum RouteError {
    NoRoute,
    Loop,
    DeadEnd,
}

struct SimulatedNode {
    index: usize,
    location: Location,
}

#[derive(Clone, Copy, Eq, PartialEq, Hash)]
struct NodeRef {
    index: usize,
}