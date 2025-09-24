use std::collections::HashMap;
use std::time::Instant;

use freenet_stdlib::prelude::{ContractKey, Transaction};

#[derive(Debug, Clone)]
pub struct NetworkMonitor {
    pub connections: HashMap<(String, String), ConnectionStatus>,
    pub message_flows: Vec<MessageFlow>,
    pub contract_locations: HashMap<ContractKey, Vec<String>>,
    start_time: Instant,
}

#[derive(Debug, Clone)]
pub enum ConnectionStatus {
    Connected,
    Disconnected,
    Connecting,
    Failed,
}

#[derive(Debug, Clone)]
pub struct MessageFlow {
    pub from: String,
    pub to: String,
    pub message_type: String,
    pub transaction_id: Transaction,
    pub timestamp: Instant,
    pub payload_size: usize,
}

impl NetworkMonitor {
    pub fn new() -> Self {
        NetworkMonitor {
            connections: HashMap::new(),
            message_flows: Vec::new(),
            contract_locations: HashMap::new(),
            start_time: Instant::now(),
        }
    }

    pub fn record_connection(&mut self, from: String, to: String, status: ConnectionStatus) {
        self.connections.insert((from, to), status);
    }

    pub fn record_message(&mut self, flow: MessageFlow) {
        self.message_flows.push(flow);
    }

    pub fn record_contract_location(&mut self, contract: ContractKey, node_id: String) {
        self.contract_locations
            .entry(contract)
            .or_insert_with(Vec::new)
            .push(node_id);
    }

    pub fn generate_sequence_diagram(&self, tx: Transaction) -> String {
        let mut diagram = String::new();
        diagram.push_str("```mermaid\n");
        diagram.push_str("sequenceDiagram\n");
        diagram.push_str("    autonumber\n");

        // Find all participants
        let mut participants = std::collections::HashSet::new();
        for flow in &self.message_flows {
            if flow.transaction_id == tx {
                participants.insert(flow.from.clone());
                participants.insert(flow.to.clone());
            }
        }

        // Declare participants
        for participant in &participants {
            diagram.push_str(&format!("    participant {}\n", participant));
        }

        // Add message flows
        for flow in &self.message_flows {
            if flow.transaction_id == tx {
                let elapsed = flow.timestamp.duration_since(self.start_time);
                diagram.push_str(&format!(
                    "    {} ->> {}: {} [{:?}]\n",
                    flow.from, flow.to, flow.message_type, elapsed
                ));
            }
        }

        diagram.push_str("```\n");
        diagram
    }

    pub fn detect_partitions(&self) -> Vec<Vec<String>> {
        // Build adjacency list
        let mut adjacency: HashMap<String, Vec<String>> = HashMap::new();

        for ((from, to), status) in &self.connections {
            if matches!(status, ConnectionStatus::Connected) {
                adjacency.entry(from.clone())
                    .or_insert_with(Vec::new)
                    .push(to.clone());
                adjacency.entry(to.clone())
                    .or_insert_with(Vec::new)
                    .push(from.clone());
            }
        }

        // Find connected components using DFS
        let mut visited = std::collections::HashSet::new();
        let mut components = Vec::new();

        for node in adjacency.keys() {
            if !visited.contains(node) {
                let mut component = Vec::new();
                self.dfs(node, &adjacency, &mut visited, &mut component);
                components.push(component);
            }
        }

        components
    }

    fn dfs(
        &self,
        node: &str,
        adjacency: &HashMap<String, Vec<String>>,
        visited: &mut std::collections::HashSet<String>,
        component: &mut Vec<String>,
    ) {
        if visited.contains(node) {
            return;
        }

        visited.insert(node.to_string());
        component.push(node.to_string());

        if let Some(neighbors) = adjacency.get(node) {
            for neighbor in neighbors {
                self.dfs(neighbor, adjacency, visited, component);
            }
        }
    }

    pub fn get_network_stats(&self) -> NetworkStats {
        let total_connections = self.connections.len();
        let connected = self.connections.values()
            .filter(|s| matches!(s, ConnectionStatus::Connected))
            .count();

        let total_messages = self.message_flows.len();
        let unique_transactions = self.message_flows
            .iter()
            .map(|f| f.transaction_id)
            .collect::<std::collections::HashSet<_>>()
            .len();

        let average_payload_size = if total_messages > 0 {
            self.message_flows.iter().map(|f| f.payload_size).sum::<usize>() / total_messages
        } else {
            0
        };

        NetworkStats {
            total_connections,
            connected_connections: connected,
            total_messages,
            unique_transactions,
            average_payload_size,
            partitions: self.detect_partitions().len(),
        }
    }

    pub fn visualize_topology(&self) -> String {
        let mut output = String::new();
        output.push_str("Network Topology:\n");
        output.push_str("================\n\n");

        // Group connections by node
        let mut node_connections: HashMap<String, Vec<(String, ConnectionStatus)>> = HashMap::new();

        for ((from, to), status) in &self.connections {
            node_connections
                .entry(from.clone())
                .or_insert_with(Vec::new)
                .push((to.clone(), status.clone()));
        }

        // Display each node and its connections
        for (node, connections) in node_connections {
            output.push_str(&format!("{} connects to:\n", node));
            for (peer, status) in connections {
                output.push_str(&format!("  -> {} [{:?}]\n", peer, status));
            }
            output.push_str("\n");
        }

        // Show partitions if any
        let partitions = self.detect_partitions();
        if partitions.len() > 1 {
            output.push_str("WARNING: Network is partitioned!\n");
            output.push_str("Partitions:\n");
            for (i, partition) in partitions.iter().enumerate() {
                output.push_str(&format!("  Partition {}: {:?}\n", i + 1, partition));
            }
        }

        output
    }

    pub fn get_contract_distribution(&self) -> String {
        let mut output = String::new();
        output.push_str("Contract Distribution:\n");
        output.push_str("====================\n\n");

        for (contract, locations) in &self.contract_locations {
            output.push_str(&format!("Contract {}: {} copies\n", contract, locations.len()));
            for location in locations {
                output.push_str(&format!("  - {}\n", location));
            }
            output.push_str("\n");
        }

        output
    }
}

#[derive(Debug)]
pub struct NetworkStats {
    pub total_connections: usize,
    pub connected_connections: usize,
    pub total_messages: usize,
    pub unique_transactions: usize,
    pub average_payload_size: usize,
    pub partitions: usize,
}

impl std::fmt::Display for NetworkStats {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Network Statistics:\n")?;
        write!(f, "  Connections: {}/{} connected\n", self.connected_connections, self.total_connections)?;
        write!(f, "  Messages: {} total, {} unique transactions\n", self.total_messages, self.unique_transactions)?;
        write!(f, "  Average payload size: {} bytes\n", self.average_payload_size)?;
        write!(f, "  Network partitions: {}\n", self.partitions)?;
        Ok(())
    }
}