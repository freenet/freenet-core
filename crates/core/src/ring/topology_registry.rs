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
use parking_lot::RwLock;
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

/// Global current network name for simulation testing.
/// Set by SimNetwork before starting nodes so that Ring can register topology snapshots
/// with the correct network name.
static CURRENT_NETWORK_NAME: LazyLock<RwLock<Option<String>>> = LazyLock::new(|| RwLock::new(None));

/// Set the current simulation network name.
/// Called by SimNetwork before starting nodes.
pub fn set_current_network_name(name: &str) {
    *CURRENT_NETWORK_NAME.write() = Some(name.to_string());
}

/// Get the current simulation network name.
/// Returns None if not in a simulation context.
pub fn get_current_network_name() -> Option<String> {
    CURRENT_NETWORK_NAME.read().clone()
}

/// Clear the current simulation network name.
/// Called when SimNetwork is dropped or test ends.
pub fn clear_current_network_name() {
    *CURRENT_NETWORK_NAME.write() = None;
}

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
    /// Disconnected upstream (seeders with downstream but no upstream, not a source)
    /// These are problematic because downstream peers depend on them but they can't receive updates
    pub disconnected_upstream: Vec<(SocketAddr, ContractInstanceId)>,
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
    validate_topology_from_snapshots(&snapshots, contract_id, contract_location)
}

/// Validates the subscription topology for a contract from provided snapshots.
///
/// Use this variant when you have captured snapshots and the global registry
/// may have been cleared (e.g., after SimNetwork::Drop).
///
/// Same validation as `validate_topology` but operates on the provided snapshots
/// instead of fetching from the global registry.
pub fn validate_topology_from_snapshots(
    snapshots: &[TopologySnapshot],
    contract_id: &ContractInstanceId,
    contract_location: f64,
) -> TopologyValidationResult {
    let mut result = TopologyValidationResult::default();

    // Build a map of peer -> (upstream, downstream) for this contract
    let mut subscription_graph: HashMap<SocketAddr, (Option<SocketAddr>, Vec<SocketAddr>)> =
        HashMap::new();
    let mut peer_locations: HashMap<SocketAddr, f64> = HashMap::new();
    let mut seeders: HashSet<SocketAddr> = HashSet::new();

    for snapshot in snapshots {
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

    // Source detection threshold: peers within 5% of ring distance to contract are considered sources
    const SOURCE_THRESHOLD: f64 = 0.05;

    // Check for orphan seeders and disconnected upstream
    for &seeder in &seeders {
        if let Some((upstream, downstream)) = subscription_graph.get(&seeder) {
            // Check if seeder is close to contract location (is source)
            // Use ring_distance for consistent wrap-around handling
            let is_source = peer_locations
                .get(&seeder)
                .map(|loc| ring_distance(*loc, contract_location) < SOURCE_THRESHOLD)
                .unwrap_or(false);

            // Orphan if: not source, no upstream, no downstream
            if !is_source && upstream.is_none() && downstream.is_empty() {
                result.orphan_seeders.push((seeder, *contract_id));
                result.issue_count += 1;
            }

            // Disconnected upstream: has downstream but no upstream (not a source)
            // This is problematic because downstream peers depend on us but we can't receive updates
            if !is_source && upstream.is_none() && !downstream.is_empty() {
                result.disconnected_upstream.push((seeder, *contract_id));
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
    // Use ring_distance for consistent wrap-around handling
    let source_candidates: Vec<_> = peer_locations
        .iter()
        .filter(|(_, loc)| ring_distance(**loc, contract_location) < SOURCE_THRESHOLD)
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

    #[test]
    fn test_disconnected_upstream_detection() {
        let network = "test-disconnected-upstream";
        clear_topology_snapshots(network);

        let peer: SocketAddr = "10.0.1.1:5000".parse().unwrap();
        let downstream_peer: SocketAddr = "10.0.2.1:5000".parse().unwrap();
        let contract_id = make_contract_id(1);
        let contract_key = make_contract_key(1);

        // Create disconnected upstream: has downstream but no upstream (not source)
        let mut snap = TopologySnapshot::new(peer, 0.3); // Location 0.3, contract at 0.5
        snap.set_contract(
            contract_id,
            ContractSubscription {
                contract_key,
                upstream: None,
                downstream: vec![downstream_peer], // Has downstream but no upstream
                is_seeding: true,
                has_client_subscriptions: false,
            },
        );

        register_topology_snapshot(network, snap);

        let result = validate_topology(network, &contract_id, 0.5);
        assert!(
            !result.disconnected_upstream.is_empty(),
            "Should detect disconnected upstream seeder"
        );

        clear_topology_snapshots(network);
    }

    #[test]
    fn test_ring_distance_wrap_around() {
        let network = "test-wrap-around";
        clear_topology_snapshots(network);

        let peer: SocketAddr = "10.0.1.1:5000".parse().unwrap();
        let contract_id = make_contract_id(1);
        let contract_key = make_contract_key(1);

        // Contract at location 0.02, peer at location 0.99
        // Ring distance should be 0.03 (through wrap-around), not 0.97
        // This peer should be considered a source (within 0.05 threshold)
        let mut snap = TopologySnapshot::new(peer, 0.99);
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

        // Contract location at 0.02 - peer at 0.99 is within 0.05 ring distance
        let result = validate_topology(network, &contract_id, 0.02);
        assert!(
            result.orphan_seeders.is_empty(),
            "Peer at 0.99 should be considered source for contract at 0.02 (ring distance 0.03)"
        );
        assert!(
            result.disconnected_upstream.is_empty(),
            "Peer at 0.99 should be considered source, not disconnected upstream"
        );

        clear_topology_snapshots(network);
    }

    #[test]
    fn test_source_seeder_not_orphan() {
        let network = "test-source-not-orphan";
        clear_topology_snapshots(network);

        let peer: SocketAddr = "10.0.1.1:5000".parse().unwrap();
        let contract_id = make_contract_id(1);
        let contract_key = make_contract_key(1);

        // Peer at same location as contract (0.5) - should be considered source
        let mut snap = TopologySnapshot::new(peer, 0.5);
        snap.set_contract(
            contract_id,
            ContractSubscription {
                contract_key,
                upstream: None,
                downstream: vec![], // No upstream, no downstream - but it's a source
                is_seeding: true,
                has_client_subscriptions: false,
            },
        );

        register_topology_snapshot(network, snap);

        let result = validate_topology(network, &contract_id, 0.5);
        assert!(
            result.orphan_seeders.is_empty(),
            "Source seeder should not be flagged as orphan"
        );

        clear_topology_snapshots(network);
    }

    #[test]
    fn test_proximity_violation_detection() {
        let network = "test-proximity-violation";
        clear_topology_snapshots(network);

        let peer: SocketAddr = "10.0.1.1:5000".parse().unwrap();
        let upstream_peer: SocketAddr = "10.0.2.1:5000".parse().unwrap();
        let contract_id = make_contract_id(1);
        let contract_key = make_contract_key(1);

        // Contract at 0.5
        // Downstream peer at 0.45 (distance 0.05 from contract)
        // Upstream peer at 0.8 (distance 0.3 from contract)
        // Violation: upstream is farther from contract than downstream
        let mut snap_downstream = TopologySnapshot::new(peer, 0.45);
        snap_downstream.set_contract(
            contract_id,
            ContractSubscription {
                contract_key,
                upstream: Some(upstream_peer),
                downstream: vec![],
                is_seeding: true,
                has_client_subscriptions: false,
            },
        );

        let mut snap_upstream = TopologySnapshot::new(upstream_peer, 0.8);
        snap_upstream.set_contract(
            contract_id,
            ContractSubscription {
                contract_key,
                upstream: None,
                downstream: vec![peer],
                is_seeding: true,
                has_client_subscriptions: false,
            },
        );

        register_topology_snapshot(network, snap_downstream);
        register_topology_snapshot(network, snap_upstream);

        let result = validate_topology(network, &contract_id, 0.5);
        assert!(
            !result.proximity_violations.is_empty(),
            "Should detect proximity violation when upstream is farther from contract than downstream"
        );

        // Verify the violation details
        let violation = &result.proximity_violations[0];
        assert_eq!(violation.downstream_addr, peer);
        assert_eq!(violation.upstream_addr, upstream_peer);

        clear_topology_snapshots(network);
    }

    #[test]
    fn test_unreachable_seeder_detection() {
        let network = "test-unreachable";
        clear_topology_snapshots(network);

        let source_peer: SocketAddr = "10.0.1.1:5000".parse().unwrap();
        let reachable_peer: SocketAddr = "10.0.2.1:5000".parse().unwrap();
        let unreachable_peer: SocketAddr = "10.0.3.1:5000".parse().unwrap();
        let contract_id = make_contract_id(1);
        let contract_key = make_contract_key(1);

        // Contract at 0.5
        // Source peer at 0.5 (is source, distance 0)
        // Reachable peer at 0.6 (connected to source as downstream)
        // Unreachable peer at 0.7 (not connected to anyone)

        let mut snap_source = TopologySnapshot::new(source_peer, 0.5);
        snap_source.set_contract(
            contract_id,
            ContractSubscription {
                contract_key,
                upstream: None,
                downstream: vec![reachable_peer],
                is_seeding: true,
                has_client_subscriptions: false,
            },
        );

        let mut snap_reachable = TopologySnapshot::new(reachable_peer, 0.6);
        snap_reachable.set_contract(
            contract_id,
            ContractSubscription {
                contract_key,
                upstream: Some(source_peer),
                downstream: vec![],
                is_seeding: true,
                has_client_subscriptions: false,
            },
        );

        // Unreachable peer - no upstream connection, not a source
        let mut snap_unreachable = TopologySnapshot::new(unreachable_peer, 0.7);
        snap_unreachable.set_contract(
            contract_id,
            ContractSubscription {
                contract_key,
                upstream: None, // No upstream!
                downstream: vec![],
                is_seeding: true,
                has_client_subscriptions: false,
            },
        );

        register_topology_snapshot(network, snap_source);
        register_topology_snapshot(network, snap_reachable);
        register_topology_snapshot(network, snap_unreachable);

        let result = validate_topology(network, &contract_id, 0.5);
        assert!(
            !result.unreachable_seeders.is_empty(),
            "Should detect unreachable seeder"
        );
        assert!(
            result
                .unreachable_seeders
                .iter()
                .any(|(addr, _)| *addr == unreachable_peer),
            "Unreachable peer should be in the list"
        );

        clear_topology_snapshots(network);
    }

    #[test]
    fn test_healthy_topology_no_issues() {
        let network = "test-healthy";
        clear_topology_snapshots(network);

        let source_peer: SocketAddr = "10.0.1.1:5000".parse().unwrap();
        let downstream_peer: SocketAddr = "10.0.2.1:5000".parse().unwrap();
        let contract_id = make_contract_id(1);
        let contract_key = make_contract_key(1);

        // Healthy topology: source → downstream (correct direction)
        // Contract at 0.5, source at 0.5 (is source), downstream at 0.6
        let mut snap_source = TopologySnapshot::new(source_peer, 0.5);
        snap_source.set_contract(
            contract_id,
            ContractSubscription {
                contract_key,
                upstream: None,
                downstream: vec![downstream_peer],
                is_seeding: true,
                has_client_subscriptions: false,
            },
        );

        let mut snap_downstream = TopologySnapshot::new(downstream_peer, 0.6);
        snap_downstream.set_contract(
            contract_id,
            ContractSubscription {
                contract_key,
                upstream: Some(source_peer),
                downstream: vec![],
                is_seeding: true,
                has_client_subscriptions: false,
            },
        );

        register_topology_snapshot(network, snap_source);
        register_topology_snapshot(network, snap_downstream);

        let result = validate_topology(network, &contract_id, 0.5);
        assert!(
            result.is_healthy(),
            "Healthy topology should have no issues, got: cycles={}, orphans={}, disconnected={}, unreachable={}, proximity={}",
            result.bidirectional_cycles.len(),
            result.orphan_seeders.len(),
            result.disconnected_upstream.len(),
            result.unreachable_seeders.len(),
            result.proximity_violations.len()
        );

        clear_topology_snapshots(network);
    }
}
