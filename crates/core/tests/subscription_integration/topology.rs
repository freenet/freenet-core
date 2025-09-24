use std::collections::HashMap;
use anyhow::Result;
use freenet::local_node::NodeConfig;
use freenet_stdlib::prelude::Location;
use rand::{Rng, SeedableRng};

#[derive(Debug, Clone)]
pub enum NetworkTopology {
    /// Linear chain: GW -> N1 -> N2 -> ... -> Nn
    Linear { nodes: usize },

    /// Star: GW in center, all nodes connect to GW
    Star { nodes: usize },

    /// Ring: Nodes form a ring with GW as entry point
    Ring { nodes: usize },

    /// Mesh: Partially connected mesh with configurable density
    Mesh { nodes: usize, connectivity: f64 },

    /// Custom topology from adjacency list
    Custom { adjacency: HashMap<usize, Vec<usize>> },
}

impl NetworkTopology {
    pub fn generate_configs(&self) -> Result<Vec<NodeConfig>> {
        let mut configs = Vec::new();
        let mut rng = rand::rngs::StdRng::seed_from_u64(12345);

        match self {
            NetworkTopology::Linear { nodes } => {
                // Gateway at position 0
                configs.push(self.create_node_config(0, true, &mut rng));

                // Chain of nodes
                for i in 1..*nodes {
                    configs.push(self.create_node_config(i, false, &mut rng));
                }
            }

            NetworkTopology::Star { nodes } => {
                // Gateway at center
                configs.push(self.create_node_config(0, true, &mut rng));

                // All other nodes connect to gateway
                for i in 1..*nodes {
                    configs.push(self.create_node_config(i, false, &mut rng));
                }
            }

            NetworkTopology::Ring { nodes } => {
                // Gateway is first node in ring
                configs.push(self.create_node_config(0, true, &mut rng));

                // Form ring topology
                for i in 1..*nodes {
                    configs.push(self.create_node_config(i, false, &mut rng));
                }
            }

            NetworkTopology::Mesh { nodes, connectivity: _ } => {
                // Gateway
                configs.push(self.create_node_config(0, true, &mut rng));

                // Mesh network nodes
                for i in 1..*nodes {
                    configs.push(self.create_node_config(i, false, &mut rng));
                }
            }

            NetworkTopology::Custom { adjacency } => {
                let max_node = adjacency.keys().max().copied().unwrap_or(0);

                for i in 0..=max_node {
                    configs.push(self.create_node_config(i, i == 0, &mut rng));
                }
            }
        }

        Ok(configs)
    }

    fn create_node_config(&self, index: usize, is_gateway: bool, rng: &mut impl Rng) -> NodeConfig {
        NodeConfig {
            location: if is_gateway {
                Location::new(0.5) // Gateway at center of ring
            } else {
                // Distribute nodes around the ring
                Location::new(rng.gen::<f64>())
            },
            is_gateway,
            peer_id: format!("peer_{}", index),
            key_pair: freenet::dev_tool::TransportKeypair::new(),
            network_api: Some(freenet::config::NetworkArgs::default()),
        }
    }

    pub fn get_connections(&self) -> HashMap<usize, Vec<usize>> {
        match self {
            NetworkTopology::Linear { nodes } => {
                let mut connections = HashMap::new();
                for i in 0..*nodes {
                    let mut peers = Vec::new();
                    if i > 0 {
                        peers.push(i - 1);
                    }
                    if i < nodes - 1 {
                        peers.push(i + 1);
                    }
                    connections.insert(i, peers);
                }
                connections
            }

            NetworkTopology::Star { nodes } => {
                let mut connections = HashMap::new();
                // Gateway connects to all
                connections.insert(0, (1..*nodes).collect());

                // All nodes connect to gateway
                for i in 1..*nodes {
                    connections.insert(i, vec![0]);
                }
                connections
            }

            NetworkTopology::Ring { nodes } => {
                let mut connections = HashMap::new();
                for i in 0..*nodes {
                    let prev = if i == 0 { nodes - 1 } else { i - 1 };
                    let next = if i == nodes - 1 { 0 } else { i + 1 };
                    connections.insert(i, vec![prev, next]);
                }
                connections
            }

            NetworkTopology::Mesh { nodes, connectivity } => {
                let mut connections = HashMap::new();
                let mut rng = rand::rngs::StdRng::seed_from_u64(42);

                for i in 0..*nodes {
                    let mut peers = Vec::new();
                    for j in 0..*nodes {
                        if i != j && rng.gen::<f64>() < *connectivity {
                            peers.push(j);
                        }
                    }
                    // Ensure gateway is connected to at least some nodes
                    if i == 0 && peers.is_empty() {
                        peers.push(1);
                    }
                    // Ensure non-gateway nodes connect to at least one peer
                    if i > 0 && peers.is_empty() {
                        peers.push(0);
                    }
                    connections.insert(i, peers);
                }
                connections
            }

            NetworkTopology::Custom { adjacency } => adjacency.clone(),
        }
    }

    pub fn visualize(&self) -> String {
        let connections = self.get_connections();
        let mut output = String::new();

        output.push_str("Network Topology:\n");
        output.push_str("=================\n");

        for (node, peers) in connections.iter() {
            let node_type = if *node == 0 { "GW" } else { "N" };
            let peers_str: Vec<String> = peers.iter()
                .map(|p| if *p == 0 { "GW".to_string() } else { format!("N{}", p) })
                .collect();

            output.push_str(&format!("{}{} -> [{}]\n",
                node_type,
                if *node == 0 { String::new() } else { node.to_string() },
                peers_str.join(", ")
            ));
        }

        output
    }
}