//! Subscription topology registry for simulation testing.
//!
//! This module provides a global registry for subscription topology snapshots,
//! enabling SimNetwork to validate subscription tree structure during tests.
//!
//! # Usage
//!
//! Nodes register their subscription state using `register_topology_snapshot()`.
//! SimNetwork queries the registry using `get_topology_snapshot()` or
//! `get_all_topology_snapshots()` to validate topology correctness.
//!
//! # Thread Safety
//!
//! The registry uses `DashMap` for thread-safe access without explicit locking.
//! Multiple nodes can register snapshots concurrently.

use dashmap::DashMap;
use freenet_stdlib::prelude::{ContractInstanceId, ContractKey};
use std::{
    collections::{HashMap, HashSet},
    net::SocketAddr,
    sync::LazyLock,
};

/// Global registry for subscription topology snapshots.
/// Key: (network_name, peer_address)
/// Value: TopologySnapshot for that peer
static TOPOLOGY_REGISTRY: LazyLock<DashMap<(String, SocketAddr), TopologySnapshot>> =
    LazyLock::new(DashMap::new);

/// A snapshot of a peer's subscription topology for a contract.
#[derive(Debug, Clone)]
pub struct ContractSubscription {
    /// The contract key
    pub contract_key: ContractKey,
    /// The upstream peer (if any) - where we receive updates from
    pub upstream: Option<SocketAddr>,
    /// Downstream peers - peers that receive updates from us
    pub downstream: Vec<SocketAddr>,
    /// Whether we're seeding this contract (have it cached)
    pub is_seeding: bool,
    /// Whether we have local client subscriptions for this contract
    pub has_client_subscriptions: bool,
}

/// A snapshot of a peer's complete subscription topology.
#[derive(Debug, Clone)]
pub struct TopologySnapshot {
    /// The peer's socket address
    pub peer_addr: SocketAddr,
    /// The peer's ring location (for proximity analysis)
    pub location: f64,
    /// Subscriptions per contract
    pub contracts: HashMap<ContractInstanceId, ContractSubscription>,
    /// Timestamp when this snapshot was taken (for staleness detection)
    pub timestamp_nanos: u64,
}

impl TopologySnapshot {
    /// Create a new empty topology snapshot for a peer.
    pub fn new(peer_addr: SocketAddr, location: f64) -> Self {
        Self {
            peer_addr,
            location,
            contracts: HashMap::new(),
            timestamp_nanos: 0,
        }
    }

    /// Add or update a contract subscription.
    pub fn set_contract(
        &mut self,
        contract_id: ContractInstanceId,
        subscription: ContractSubscription,
    ) {
        self.contracts.insert(contract_id, subscription);
    }

    /// Get the subscription for a contract.
    pub fn get_contract(&self, contract_id: &ContractInstanceId) -> Option<&ContractSubscription> {
        self.contracts.get(contract_id)
    }

    /// Check if this peer has an upstream for a contract.
    pub fn has_upstream(&self, contract_id: &ContractInstanceId) -> bool {
        self.contracts
            .get(contract_id)
            .map(|s| s.upstream.is_some())
            .unwrap_or(false)
    }

    /// Check if this peer is seeding a contract.
    pub fn is_seeding(&self, contract_id: &ContractInstanceId) -> bool {
        self.contracts
            .get(contract_id)
            .map(|s| s.is_seeding)
            .unwrap_or(false)
    }
}

/// Register a topology snapshot for a peer in a network.
pub fn register_topology_snapshot(network_name: &str, snapshot: TopologySnapshot) {
    let key = (network_name.to_string(), snapshot.peer_addr);
    TOPOLOGY_REGISTRY.insert(key, snapshot);
}

/// Get the topology snapshot for a specific peer in a network.
pub fn get_topology_snapshot(
    network_name: &str,
    peer_addr: &SocketAddr,
) -> Option<TopologySnapshot> {
    let key = (network_name.to_string(), *peer_addr);
    TOPOLOGY_REGISTRY.get(&key).map(|r| r.value().clone())
}

/// Get all topology snapshots for a network.
pub fn get_all_topology_snapshots(network_name: &str) -> Vec<TopologySnapshot> {
    TOPOLOGY_REGISTRY
        .iter()
        .filter(|entry| entry.key().0 == network_name)
        .map(|entry| entry.value().clone())
        .collect()
}

/// Clear all topology snapshots for a network.
pub fn clear_topology_snapshots(network_name: &str) {
    TOPOLOGY_REGISTRY.retain(|key, _| key.0 != network_name);
}

/// Clear all topology snapshots (for test cleanup).
pub fn clear_all_topology_snapshots() {
    TOPOLOGY_REGISTRY.clear();
}

/// Result of topology validation.
#[derive(Debug, Default)]
pub struct TopologyValidationResult {
    /// Bidirectional cycles detected (pairs of peers)
    pub bidirectional_cycles: Vec<(SocketAddr, SocketAddr)>,
    /// Orphan seeders (peers seeding without upstream or downstream)
    pub orphan_seeders: Vec<(SocketAddr, ContractInstanceId)>,
    /// Unreachable seeders (seeders that can't receive updates from source)
    pub unreachable_seeders: Vec<(SocketAddr, ContractInstanceId)>,
    /// Proximity violations (upstream is farther from contract than downstream)
    pub proximity_violations: Vec<ProximityViolation>,
    /// Total number of issues found
    pub issue_count: usize,
}

impl TopologyValidationResult {
    /// Check if the topology is healthy (no issues).
    pub fn is_healthy(&self) -> bool {
        self.issue_count == 0
    }
}

/// A proximity violation in upstream selection.
#[derive(Debug, Clone)]
pub struct ProximityViolation {
    pub contract_id: ContractInstanceId,
    pub downstream_addr: SocketAddr,
    pub upstream_addr: SocketAddr,
    pub downstream_location: f64,
    pub upstream_location: f64,
    pub contract_location: f64,
}

/// Validate the subscription topology for a contract across all peers in a network.
///
/// This function checks for:
/// - Bidirectional cycles that create isolated islands
/// - Orphan seeders without recovery paths
/// - Unreachable seeders that can't receive updates
/// - Proximity violations in upstream selection
pub fn validate_topology(
    network_name: &str,
    contract_id: &ContractInstanceId,
    contract_location: f64,
) -> TopologyValidationResult {
    let snapshots = get_all_topology_snapshots(network_name);
    let mut result = TopologyValidationResult::default();

    // Build a map of peer -> (upstream, downstream) for this contract
    let mut subscription_graph: HashMap<SocketAddr, (Option<SocketAddr>, Vec<SocketAddr>)> =
        HashMap::new();
    let mut peer_locations: HashMap<SocketAddr, f64> = HashMap::new();
    let mut seeders: HashSet<SocketAddr> = HashSet::new();

    for snapshot in &snapshots {
        peer_locations.insert(snapshot.peer_addr, snapshot.location);

        if let Some(sub) = snapshot.contracts.get(contract_id) {
            subscription_graph.insert(snapshot.peer_addr, (sub.upstream, sub.downstream.clone()));

            if sub.is_seeding {
                seeders.insert(snapshot.peer_addr);
            }
        }
    }

    // Check for bidirectional cycles
    for (&peer, (upstream, _)) in &subscription_graph {
        if let Some(upstream_addr) = upstream {
            // Check if the upstream also has us as their upstream (bidirectional)
            if let Some((their_upstream, _)) = subscription_graph.get(upstream_addr) {
                if their_upstream == &Some(peer) {
                    // Found a bidirectional cycle
                    let pair = if peer < *upstream_addr {
                        (peer, *upstream_addr)
                    } else {
                        (*upstream_addr, peer)
                    };
                    if !result.bidirectional_cycles.contains(&pair) {
                        result.bidirectional_cycles.push(pair);
                        result.issue_count += 1;
                    }
                }
            }
        }
    }

    // Check for orphan seeders
    for &seeder in &seeders {
        if let Some((upstream, downstream)) = subscription_graph.get(&seeder) {
            // Check if seeder is close to contract location (is source)
            let is_source = peer_locations
                .get(&seeder)
                .map(|loc| (loc - contract_location).abs() < 0.01)
                .unwrap_or(false);

            // Orphan if: not source, no upstream, no downstream
            if !is_source && upstream.is_none() && downstream.is_empty() {
                result.orphan_seeders.push((seeder, *contract_id));
                result.issue_count += 1;
            }
        }
    }

    // Check for proximity violations
    for (&peer, (upstream, _)) in &subscription_graph {
        if let Some(upstream_addr) = upstream {
            let peer_loc = peer_locations.get(&peer).copied().unwrap_or(0.0);
            let upstream_loc = peer_locations.get(upstream_addr).copied().unwrap_or(0.0);

            let peer_dist = ring_distance(peer_loc, contract_location);
            let upstream_dist = ring_distance(upstream_loc, contract_location);

            // Upstream should be closer to contract than downstream (with tolerance)
            if upstream_dist > peer_dist + 0.1 {
                result.proximity_violations.push(ProximityViolation {
                    contract_id: *contract_id,
                    downstream_addr: peer,
                    upstream_addr: *upstream_addr,
                    downstream_location: peer_loc,
                    upstream_location: upstream_loc,
                    contract_location,
                });
                result.issue_count += 1;
            }
        }
    }

    // Check for unreachable seeders using BFS from source
    let source_candidates: Vec<_> = peer_locations
        .iter()
        .filter(|(_, loc)| ((*loc) - contract_location).abs() < 0.05)
        .map(|(addr, _)| *addr)
        .collect();

    if !source_candidates.is_empty() {
        let mut reachable: HashSet<SocketAddr> = HashSet::new();
        let mut to_visit: Vec<SocketAddr> = source_candidates;

        while let Some(peer) = to_visit.pop() {
            if reachable.insert(peer) {
                if let Some((_, downstream)) = subscription_graph.get(&peer) {
                    to_visit.extend(downstream.iter().copied());
                }
            }
        }

        for &seeder in &seeders {
            if !reachable.contains(&seeder) {
                result.unreachable_seeders.push((seeder, *contract_id));
                result.issue_count += 1;
            }
        }
    }

    result
}

/// Calculate distance on the ring (handles wrap-around).
fn ring_distance(a: f64, b: f64) -> f64 {
    let diff = (a - b).abs();
    diff.min(1.0 - diff)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_contract_id(seed: u8) -> ContractInstanceId {
        ContractInstanceId::new([seed; 32])
    }

    fn make_contract_key(seed: u8) -> ContractKey {
        use freenet_stdlib::prelude::CodeHash;
        ContractKey::from_id_and_code(
            ContractInstanceId::new([seed; 32]),
            CodeHash::new([seed.wrapping_add(1); 32]),
        )
    }

    #[test]
    fn test_bidirectional_cycle_detection() {
        let network = "test-bidirectional";
        clear_topology_snapshots(network);

        let peer_a: SocketAddr = "10.0.1.1:5000".parse().unwrap();
        let peer_b: SocketAddr = "10.0.2.1:5000".parse().unwrap();
        let contract_id = make_contract_id(1);
        let contract_key = make_contract_key(1);

        // Create bidirectional cycle: A → B and B → A
        let mut snap_a = TopologySnapshot::new(peer_a, 0.3);
        snap_a.set_contract(
            contract_id,
            ContractSubscription {
                contract_key,
                upstream: Some(peer_b),
                downstream: vec![peer_b], // B is also downstream
                is_seeding: true,
                has_client_subscriptions: false,
            },
        );

        let mut snap_b = TopologySnapshot::new(peer_b, 0.4);
        snap_b.set_contract(
            contract_id,
            ContractSubscription {
                contract_key,
                upstream: Some(peer_a),
                downstream: vec![peer_a], // A is also downstream
                is_seeding: true,
                has_client_subscriptions: false,
            },
        );

        register_topology_snapshot(network, snap_a);
        register_topology_snapshot(network, snap_b);

        let result = validate_topology(network, &contract_id, 0.5);
        assert!(
            !result.bidirectional_cycles.is_empty(),
            "Should detect bidirectional cycle"
        );

        clear_topology_snapshots(network);
    }

    #[test]
    fn test_orphan_seeder_detection() {
        let network = "test-orphan";
        clear_topology_snapshots(network);

        let peer: SocketAddr = "10.0.1.1:5000".parse().unwrap();
        let contract_id = make_contract_id(1);
        let contract_key = make_contract_key(1);

        // Create orphan seeder: seeding but no upstream, no downstream
        let mut snap = TopologySnapshot::new(peer, 0.3);
        snap.set_contract(
            contract_id,
            ContractSubscription {
                contract_key,
                upstream: None,
                downstream: vec![],
                is_seeding: true,
                has_client_subscriptions: false,
            },
        );

        register_topology_snapshot(network, snap);

        let result = validate_topology(network, &contract_id, 0.5);
        assert!(
            !result.orphan_seeders.is_empty(),
            "Should detect orphan seeder"
        );

        clear_topology_snapshots(network);
    }
}
