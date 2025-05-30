//! Large Network Simulation Tests
//!
//! These tests demonstrate how to test large networks using simulation
//! instead of full node processes.

use std::collections::{HashMap, HashSet};
use testresult::TestResult;

type NodeId = usize;

/// Lightweight network simulator for testing topology and propagation
struct NetworkSimulator {
    nodes: Vec<NodeId>,
    connections: HashMap<NodeId, HashSet<NodeId>>,
}

impl NetworkSimulator {
    fn new(node_count: usize, connectivity_ratio: f64) -> Self {
        let nodes: Vec<NodeId> = (0..node_count).collect();
        let mut connections: HashMap<NodeId, HashSet<NodeId>> = HashMap::new();

        // Initialize empty connection sets
        for &node in &nodes {
            connections.insert(node, HashSet::new());
        }

        // Create connections using same algorithm as real test
        for i in 0..node_count {
            for j in (i + 1)..node_count {
                let (a, b) = if i < j { (i, j) } else { (j, i) };
                let hash_value = (a * 17 + b * 31 + a * b * 7) % 100;
                let should_connect = (hash_value as f64) < (connectivity_ratio * 100.0);

                if should_connect {
                    connections.get_mut(&i).unwrap().insert(j);
                    connections.get_mut(&j).unwrap().insert(i);
                }
            }
        }

        Self { nodes, connections }
    }

    /// Simulate message propagation through the network
    fn simulate_propagation(&self, start_node: NodeId) -> PropagationStats {
        let mut visited = HashSet::new();
        let mut current_wave = vec![start_node];
        let mut max_hops: usize = 0;

        while !current_wave.is_empty() {
            let mut next_wave = Vec::new();
            max_hops += 1;

            for &node in &current_wave {
                if visited.contains(&node) {
                    continue;
                }
                visited.insert(node);

                // Add connected nodes to next wave
                if let Some(connected) = self.connections.get(&node) {
                    for &neighbor in connected {
                        if !visited.contains(&neighbor) {
                            next_wave.push(neighbor);
                        }
                    }
                }
            }

            current_wave = next_wave;
        }

        PropagationStats {
            nodes_reached: visited.len(),
            total_nodes: self.nodes.len(),
            max_hops: max_hops.saturating_sub(1), // Subtract 1 since we start at hop 0
        }
    }

    fn get_topology_stats(&self) -> TopologyStats {
        let total_connections: usize = self.connections.values().map(|conns| conns.len()).sum();

        TopologyStats {
            total_nodes: self.nodes.len(),
            total_connections: total_connections / 2, // Each connection counted twice
            avg_connections: total_connections as f64 / self.nodes.len() as f64,
        }
    }
}

#[derive(Debug)]
struct PropagationStats {
    nodes_reached: usize,
    total_nodes: usize,
    max_hops: usize,
}

impl PropagationStats {
    fn coverage(&self) -> f64 {
        self.nodes_reached as f64 / self.total_nodes as f64
    }
}

#[derive(Debug)]
struct TopologyStats {
    total_nodes: usize,
    total_connections: usize,
    avg_connections: f64,
}

#[test]
fn test_1000_node_network_simulation() -> TestResult {
    const NETWORK_SIZE: usize = 1000;
    const CONNECTIVITY_RATIO: f64 = 0.1; // 10% connectivity

    println!(
        "Simulating {}-node network with {:.1}% connectivity",
        NETWORK_SIZE,
        CONNECTIVITY_RATIO * 100.0
    );

    let network = NetworkSimulator::new(NETWORK_SIZE, CONNECTIVITY_RATIO);
    let topology = network.get_topology_stats();

    println!("Network topology:");
    println!("  Nodes: {}", topology.total_nodes);
    println!("  Connections: {}", topology.total_connections);
    println!("  Avg connections/node: {:.2}", topology.avg_connections);

    // Test propagation from node 0 (gateway)
    let propagation = network.simulate_propagation(0);

    println!("Propagation results:");
    println!(
        "  Coverage: {:.1}% ({}/{})",
        propagation.coverage() * 100.0,
        propagation.nodes_reached,
        propagation.total_nodes
    );
    println!("  Max hops: {}", propagation.max_hops);

    // Verify reasonable propagation for sparse network
    assert!(
        propagation.coverage() > 0.3,
        "Coverage too low: {:.1}%",
        propagation.coverage() * 100.0
    );
    assert!(
        propagation.max_hops < 25,
        "Too many hops: {}",
        propagation.max_hops
    );

    Ok(())
}

#[test]
fn test_100_node_network_simulation() -> TestResult {
    const NETWORK_SIZE: usize = 100;
    const CONNECTIVITY_RATIO: f64 = 0.3; // 30% connectivity

    let network = NetworkSimulator::new(NETWORK_SIZE, CONNECTIVITY_RATIO);
    let propagation = network.simulate_propagation(0);

    // Higher connectivity should give better coverage
    assert!(
        propagation.coverage() > 0.8,
        "Coverage too low for medium network: {:.1}%",
        propagation.coverage() * 100.0
    );
    assert!(
        propagation.max_hops < 10,
        "Too many hops for medium network: {}",
        propagation.max_hops
    );

    Ok(())
}
