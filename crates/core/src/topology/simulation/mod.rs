use std::collections::{HashMap, HashSet};
use crate::network_sim::Location;
use tracing::{debug, info, warn};
use tracing_subscriber::field::debug;

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
    
        let mut current = *source;  // Dereference to copy
        let mut visited = Vec::new();
        let mut recent_nodes = Vec::new();  // To track the sequence of last N nodes
    
        loop {
            debug!("Current node: {:?}", current);
    
            // Check if we've reached the destination
            if self.nodes[current.index].location == destination {
                info!("Reached destination");
                return Ok(visited);
            }
    
            // Get current node's distance to the destination
            let current_distance = self.nodes[current.index].location.distance(destination).as_f64();
            debug!("Current distance to destination: {}", current_distance);
    
            // Find the closest connected node to the destination that hasn't been visited
            let (closest, closest_distance) = self.connections[&current].iter()
                .map(|&neighbor| {
                    let distance = self.nodes[neighbor.index].location.distance(destination).as_f64();
                    (neighbor, distance)
                })
                .min_by(|a, b| a.1.partial_cmp(&b.1).unwrap_or(std::cmp::Ordering::Equal))
                .unwrap_or((current, current_distance));
    
            debug!("Closest node: {:?}, distance: {}", closest, closest_distance);
    
            // Check for a loop by looking at the sequence of last N nodes
            recent_nodes.push(closest);
            if recent_nodes.len() > 10 {  // Keep only the last 10 visited nodes in the list
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
mod tests {
    use super::*;

    fn setup() {
        // Initialize the tracer
        tracing_subscriber::fmt()
            .with_max_level(tracing::Level::DEBUG) // Adjust the level here
            .try_init()
            .unwrap_or(());
    }
    #[test]
    fn test_add_node() {
        setup();
        let mut net = SimulatedNetwork::new();
        let node_ref = net.add_node();
        assert_eq!(node_ref.index, 0);
        assert_eq!(net.nodes.len(), 1);
    }

    #[test]
    fn test_connect() {
        setup();
        let mut net = SimulatedNetwork::new();
        let a = net.add_node();
        let b = net.add_node();
        net.connect(a, b);
        assert!(net.connections.get(&a).unwrap().contains(&b));
        assert!(net.connections.get(&b).unwrap().contains(&a));
    }

    #[test]
    fn test_disconnect() {
        setup();
        let mut net = SimulatedNetwork::new();
        let a = net.add_node();
        let b = net.add_node();
        net.connect(a, b);
        net.disconnect(a, b);
        assert!(!net.connections.get(&a).unwrap().contains(&b));
        assert!(!net.connections.get(&b).unwrap().contains(&a));
    }

    #[test]
    fn test_route_success() {
        setup();
        let mut net = SimulatedNetwork::new();
        let a = net.add_node();
        let b = net.add_node();
        net.connect(a, b);

        // Mock a destination location that's the same as node b's location
        let destination = net.nodes[b.index].location;

        let result = net.route(&a, destination);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), vec![b]);
    }

    #[test]
    fn test_route_loop_error() {
        setup();

        let mut net = SimulatedNetwork::new();
        let a = net.add_node();
        let b = net.add_node();
        let c = net.add_node();
        net.connect(a, b);
        net.connect(b, c);
        net.connect(c, a);

        // Mock a destination location that's not reachable
        let destination = Location::random();  // Assume random will not match any node

        let result = net.route(&a, destination);
        assert!(matches!(result, Err(RouteError::Loop)));
    }

    #[test]
    fn test_route_dead_end() {
        setup();
        let mut net = SimulatedNetwork::new();
        let a = net.add_node();
        let destination = net.nodes[a.index].location;

        let result = net.route(&a, destination);
        assert!(result.is_ok());
        assert!(result.unwrap().is_empty());  // Since there's no closer node
    }
}
