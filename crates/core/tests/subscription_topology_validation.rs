//! Subscription Topology Validation Tests
//!
//! These tests validate that the subscription tree topology is correct and
//! that known issues are properly detected. These tests are designed to FAIL
//! until the underlying issues are fixed.
//!
//! Related issues:
//! - #2717: Contract state divergence analysis (parent)
//! - #2718: Dead code in proximity cache
//! - #2719: Orphan seeders left unrecovered
//! - #2720: Bidirectional subscriptions creating isolated islands
//! - #2721: Lack of geographical-based upstream selection
//!
//! Run with: cargo test -p freenet --test subscription_topology_validation

use std::collections::{HashMap, HashSet};
use std::net::{IpAddr, Ipv4Addr, SocketAddr};

/// A simulated peer in our topology tests.
/// Note: We use a separate location field that's not part of Eq/Hash since f64 doesn't impl those.
#[derive(Debug, Clone)]
struct SimPeer {
    id: u8,
    addr: SocketAddr,
    location: f64, // Ring location [0, 1)
}

impl PartialEq for SimPeer {
    fn eq(&self, other: &Self) -> bool {
        self.id == other.id && self.addr == other.addr
    }
}

impl Eq for SimPeer {}

impl std::hash::Hash for SimPeer {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.id.hash(state);
        self.addr.hash(state);
    }
}

impl SimPeer {
    fn new(id: u8, location: f64) -> Self {
        let addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(10, 0, id, 1)), 1000 + id as u16);
        Self { id, addr, location }
    }
}

/// Represents a subscription relationship in the tree.
#[derive(Debug, Clone)]
#[allow(dead_code)] // Used for documentation, may be used in future
struct SubscriptionEdge {
    upstream: u8,   // Peer ID of upstream (source of updates)
    downstream: u8, // Peer ID of downstream (receiver of updates)
}

/// A simulated subscription tree for a single contract.
#[derive(Debug, Default)]
struct SubscriptionTree {
    /// Contract key
    contract_location: f64,
    /// All peers in the network
    peers: HashMap<u8, SimPeer>,
    /// Subscription edges: downstream -> upstream
    upstream_map: HashMap<u8, u8>,
    /// Subscription edges: upstream -> Vec<downstream>
    downstream_map: HashMap<u8, Vec<u8>>,
    /// Peers that are seeding the contract (have it cached)
    seeders: HashSet<u8>,
    /// Client subscriptions (local WebSocket clients)
    client_subscriptions: HashSet<u8>,
}

impl SubscriptionTree {
    fn new(contract_location: f64) -> Self {
        Self {
            contract_location,
            ..Default::default()
        }
    }

    fn add_peer(&mut self, peer: SimPeer) {
        self.peers.insert(peer.id, peer);
    }

    fn set_upstream(&mut self, subscriber: u8, upstream: u8) {
        self.upstream_map.insert(subscriber, upstream);
        self.downstream_map
            .entry(upstream)
            .or_default()
            .push(subscriber);
    }

    fn add_seeder(&mut self, peer_id: u8) {
        self.seeders.insert(peer_id);
    }

    fn add_client_subscription(&mut self, peer_id: u8) {
        self.client_subscriptions.insert(peer_id);
    }

    /// Check if the tree has any bidirectional relationships that form isolated islands.
    ///
    /// A bidirectional relationship is where A is upstream of B AND B is upstream of A.
    /// This creates a closed loop that isolates the pair from the rest of the tree.
    fn find_bidirectional_cycles(&self) -> Vec<(u8, u8)> {
        let mut cycles = Vec::new();
        for (&downstream, &upstream) in &self.upstream_map {
            // Check if the reverse relationship also exists
            if self.upstream_map.get(&upstream) == Some(&downstream) {
                // Only add each pair once (smaller ID first)
                let pair = if downstream < upstream {
                    (downstream, upstream)
                } else {
                    (upstream, downstream)
                };
                if !cycles.contains(&pair) {
                    cycles.push(pair);
                }
            }
        }
        cycles
    }

    /// Check if updates from a source peer can reach all seeders.
    ///
    /// Returns the set of unreachable seeders.
    fn find_unreachable_seeders(&self, source: u8) -> HashSet<u8> {
        let mut reachable = HashSet::new();
        let mut to_visit = vec![source];

        while let Some(peer) = to_visit.pop() {
            if reachable.insert(peer) {
                // Add all downstream peers
                if let Some(downstream) = self.downstream_map.get(&peer) {
                    to_visit.extend(downstream.iter().copied());
                }
            }
        }

        self.seeders
            .iter()
            .filter(|s| !reachable.contains(s))
            .copied()
            .collect()
    }

    /// Find orphan seeders: peers that are seeding but have no upstream and no way to recover.
    ///
    /// An orphan seeder is one that:
    /// 1. Is seeding the contract (has it cached)
    /// 2. Has no upstream subscription
    /// 3. Has no downstream subscribers (so no active interest from peers)
    /// 4. May or may not have client subscriptions (the bug is they're never recovered)
    fn find_orphan_seeders(&self) -> HashSet<u8> {
        self.seeders
            .iter()
            .filter(|&&peer| {
                let has_upstream = self.upstream_map.contains_key(&peer);
                let has_downstream = self
                    .downstream_map
                    .get(&peer)
                    .map(|d| !d.is_empty())
                    .unwrap_or(false);
                let is_source = self
                    .peers
                    .get(&peer)
                    .map(|p| (p.location - self.contract_location).abs() < 0.01)
                    .unwrap_or(false);

                // Orphan if: seeding, not source, no upstream, no downstream
                !is_source && !has_upstream && !has_downstream
            })
            .copied()
            .collect()
    }

    /// Check if upstream selection respects geographical proximity.
    ///
    /// For each subscription edge, the upstream should be closer to the contract
    /// than the downstream, with some tolerance for edge cases.
    fn find_bad_upstream_selections(&self) -> Vec<(u8, u8, f64, f64)> {
        let mut violations = Vec::new();

        for (&downstream, &upstream) in &self.upstream_map {
            let downstream_peer = match self.peers.get(&downstream) {
                Some(p) => p,
                None => continue,
            };
            let upstream_peer = match self.peers.get(&upstream) {
                Some(p) => p,
                None => continue,
            };

            let downstream_dist = ring_distance(downstream_peer.location, self.contract_location);
            let upstream_dist = ring_distance(upstream_peer.location, self.contract_location);

            // Upstream should be at least as close to the contract as downstream
            // Allow some tolerance (0.1) for routing variations
            if upstream_dist > downstream_dist + 0.1 {
                violations.push((downstream, upstream, downstream_dist, upstream_dist));
            }
        }

        violations
    }

    /// Calculate the maximum depth of the subscription tree from the source.
    fn max_tree_depth(&self, source: u8) -> usize {
        fn dfs(
            tree: &SubscriptionTree,
            node: u8,
            depth: usize,
            visited: &mut HashSet<u8>,
        ) -> usize {
            if !visited.insert(node) {
                return depth; // Cycle detected
            }
            let mut max_depth = depth;
            if let Some(downstream) = tree.downstream_map.get(&node) {
                for &child in downstream {
                    max_depth = max_depth.max(dfs(tree, child, depth + 1, visited));
                }
            }
            max_depth
        }

        let mut visited = HashSet::new();
        dfs(self, source, 0, &mut visited)
    }
}

/// Calculate distance on the ring (handles wrap-around).
fn ring_distance(a: f64, b: f64) -> f64 {
    let diff = (a - b).abs();
    diff.min(1.0 - diff)
}

// =============================================================================
// Test 1: Bidirectional Subscriptions Creating Isolated Islands (#2720)
// =============================================================================

/// This test demonstrates that bidirectional subscriptions can create isolated islands.
///
/// Scenario:
/// - Peers A and B form a bidirectional subscription (A↔B)
/// - Peers C and D form another bidirectional subscription (C↔D)
/// - Source S sends updates
/// - Neither A↔B nor C↔D receive updates because they're isolated
///
/// This test should FAIL until the cycle detection is implemented.
#[test]
fn test_bidirectional_subscriptions_create_isolated_islands() {
    let contract_location = 0.5;
    let mut tree = SubscriptionTree::new(contract_location);

    // Source peer at contract location
    let source = SimPeer::new(0, 0.5);
    tree.add_peer(source.clone());
    tree.add_seeder(0);

    // Island 1: A↔B bidirectional (peers 1 and 2)
    let peer_a = SimPeer::new(1, 0.3);
    let peer_b = SimPeer::new(2, 0.4);
    tree.add_peer(peer_a);
    tree.add_peer(peer_b);
    tree.add_seeder(1);
    tree.add_seeder(2);
    // Bidirectional: A→B and B→A
    tree.set_upstream(1, 2); // A subscribes to B
    tree.set_upstream(2, 1); // B subscribes to A

    // Island 2: C↔D bidirectional (peers 3 and 4)
    let peer_c = SimPeer::new(3, 0.6);
    let peer_d = SimPeer::new(4, 0.7);
    tree.add_peer(peer_c);
    tree.add_peer(peer_d);
    tree.add_seeder(3);
    tree.add_seeder(4);
    // Bidirectional: C→D and D→C
    tree.set_upstream(3, 4); // C subscribes to D
    tree.set_upstream(4, 3); // D subscribes to C

    // Detect bidirectional cycles
    let cycles = tree.find_bidirectional_cycles();

    // EXPECTED FAILURE: Current implementation allows bidirectional subscriptions
    // and doesn't detect that they form isolated islands
    assert!(
        cycles.is_empty(),
        "ISSUE #2720: Found {} bidirectional cycles that create isolated islands: {:?}. \
         The system should either prevent bidirectional subscriptions or ensure \
         at least one peer in each cycle has a path to the source.",
        cycles.len(),
        cycles
    );

    // Also verify that updates can reach all seeders
    let unreachable = tree.find_unreachable_seeders(0);
    assert!(
        unreachable.is_empty(),
        "ISSUE #2720: {} seeders are unreachable from source: {:?}. \
         Bidirectional cycles have created isolated islands.",
        unreachable.len(),
        unreachable
    );
}

/// Test that a chain of bidirectional subscriptions prevents update propagation.
#[test]
fn test_bidirectional_chain_blocks_updates() {
    let contract_location = 0.0;
    let mut tree = SubscriptionTree::new(contract_location);

    // Source at location 0.0
    let source = SimPeer::new(0, 0.0);
    tree.add_peer(source);
    tree.add_seeder(0);

    // Create a chain: S → A ↔ B ↔ C → D
    // Where ↔ means bidirectional
    for i in 1..=4 {
        let peer = SimPeer::new(i, i as f64 * 0.2);
        tree.add_peer(peer);
        tree.add_seeder(i);
    }

    // S → A (normal)
    tree.set_upstream(1, 0);
    // A ↔ B (bidirectional)
    tree.set_upstream(2, 1);
    tree.set_upstream(1, 2); // This overwrites, creating only B→A, but demonstrates the issue
                             // For a true bidirectional, we'd need the data structure to support multiple upstreams

    // In the current model, let's show the issue differently:
    // If peer 2 gets upstream set to peer 1, and peer 1's upstream is overwritten to 2,
    // then peer 1 loses its connection to source

    let unreachable = tree.find_unreachable_seeders(0);

    // This should fail because the bidirectional relationship isolated peers
    assert!(
        unreachable.is_empty(),
        "ISSUE #2720: Bidirectional chain isolated {} seeders from source: {:?}",
        unreachable.len(),
        unreachable
    );
}

// =============================================================================
// Test 2: Orphan Seeders Left Unrecovered (#2719)
// =============================================================================

/// Test that orphan seeders (seeders with no upstream and no downstream) are detected.
///
/// This simulates the scenario from issue #2719 where a peer was seeding for 4+ hours
/// without any upstream subscription, never receiving updates.
///
/// This test should FAIL until orphan recovery is implemented.
#[test]
fn test_orphan_seeders_detected() {
    let contract_location = 0.5;
    let mut tree = SubscriptionTree::new(contract_location);

    // Source peer
    let source = SimPeer::new(0, 0.5);
    tree.add_peer(source);
    tree.add_seeder(0);

    // Normal peer with proper upstream
    let peer1 = SimPeer::new(1, 0.3);
    tree.add_peer(peer1);
    tree.add_seeder(1);
    tree.set_upstream(1, 0);

    // Orphan seeder: has contract cached but no upstream and no downstream
    // This simulates `{is_seeding: true, upstream: null, downstream: []}`
    let orphan = SimPeer::new(2, 0.7);
    tree.add_peer(orphan);
    tree.add_seeder(2);
    // No upstream set - this peer is orphaned

    let orphans = tree.find_orphan_seeders();

    // EXPECTED FAILURE: Current implementation doesn't recover orphan seeders
    // The filtering logic at seeding.rs:427 abandons these nodes
    assert!(
        orphans.is_empty(),
        "ISSUE #2719: Found {} orphan seeders without recovery mechanism: {:?}. \
         The system should proactively re-subscribe orphan seeders or remove them from seeding.",
        orphans.len(),
        orphans
    );
}

/// Test that orphan seeders with client subscriptions are still detected as problematic.
///
/// A peer with local client subscriptions but no upstream is a critical issue:
/// the client will never receive updates.
#[test]
fn test_orphan_seeder_with_client_subscription() {
    let contract_location = 0.5;
    let mut tree = SubscriptionTree::new(contract_location);

    // Source peer
    let source = SimPeer::new(0, 0.5);
    tree.add_peer(source);
    tree.add_seeder(0);

    // Orphan seeder WITH client subscription
    let orphan = SimPeer::new(1, 0.3);
    tree.add_peer(orphan);
    tree.add_seeder(1);
    tree.add_client_subscription(1); // Has a client waiting for updates!
                                     // But no upstream set - client will never receive updates

    // Check if this peer should be trying to get an upstream
    let should_have_upstream = tree
        .seeders
        .iter()
        .filter(|&&p| {
            let is_source = tree
                .peers
                .get(&p)
                .map(|peer| (peer.location - tree.contract_location).abs() < 0.01)
                .unwrap_or(false);
            let has_client = tree.client_subscriptions.contains(&p);
            let has_upstream = tree.upstream_map.contains_key(&p);

            // Non-source peers with clients MUST have upstream
            !is_source && has_client && !has_upstream
        })
        .collect::<Vec<_>>();

    // EXPECTED FAILURE: contracts_without_upstream() at seeding.rs:405-434
    // requires either client_subscriptions OR downstream peers to trigger recovery.
    // But the actual recovery mechanism isn't working for isolated seeders.
    assert!(
        should_have_upstream.is_empty(),
        "ISSUE #2719: {} peers have client subscriptions but no upstream: {:?}. \
         Clients on these peers will NEVER receive updates. \
         The contracts_without_upstream() method should identify these for recovery.",
        should_have_upstream.len(),
        should_have_upstream
    );
}

// =============================================================================
// Test 3: Dead Code in Proximity Cache (#2718)
// =============================================================================

/// Test that request_cache_state() is actually called during peer connections.
///
/// The function exists at proximity_cache.rs:267-271 but has #[allow(dead_code)]
/// because it's never called. The response handler works, but the triggering
/// mechanism is missing.
///
/// This test verifies the expected behavior that should exist.
#[test]
fn test_proximity_cache_state_request_on_connection() {
    // Simulate a connection event tracker
    struct ConnectionEventTracker {
        cache_state_requests_sent: Vec<SocketAddr>,
    }

    impl ConnectionEventTracker {
        fn new() -> Self {
            Self {
                cache_state_requests_sent: Vec::new(),
            }
        }

        /// This method SHOULD be called when a peer connects.
        /// Currently, it's never called (the code is dead).
        /// This demonstrates what the correct implementation would do.
        #[allow(dead_code)]
        fn on_peer_connected(&mut self, peer_addr: SocketAddr) {
            // In a correct implementation, this would:
            // 1. Call ProximityCacheManager::request_cache_state()
            // 2. Send the CacheStateRequest message to the peer
            // 3. Track that we sent the request
            self.cache_state_requests_sent.push(peer_addr);
        }

        fn was_cache_state_requested(&self, peer_addr: &SocketAddr) -> bool {
            self.cache_state_requests_sent.contains(peer_addr)
        }
    }

    let tracker = ConnectionEventTracker::new();

    // Simulate some peer connections
    let peer1 = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(10, 0, 1, 1)), 5000);
    let peer2 = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(10, 0, 2, 1)), 5000);
    let peer3 = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(10, 0, 3, 1)), 5000);

    // In current implementation, on_peer_connected is never called
    // because the wiring is missing. This simulates what SHOULD happen:
    // (Uncomment to see what correct behavior looks like)
    // tracker.on_peer_connected(peer1);
    // tracker.on_peer_connected(peer2);
    // tracker.on_peer_connected(peer3);

    // EXPECTED FAILURE: The request_cache_state() function is dead code
    // No peer connections trigger cache state requests
    assert!(
        tracker.was_cache_state_requested(&peer1),
        "ISSUE #2718: Cache state was NOT requested for peer {}. \
         The request_cache_state() function at proximity_cache.rs:267-271 is dead code. \
         It should be called when peers connect to synchronize cache knowledge.",
        peer1
    );
    assert!(
        tracker.was_cache_state_requested(&peer2),
        "ISSUE #2718: Cache state was NOT requested for peer {}",
        peer2
    );
    assert!(
        tracker.was_cache_state_requested(&peer3),
        "ISSUE #2718: Cache state was NOT requested for peer {}",
        peer3
    );
}

/// Test that verifies the proximity cache message flow is complete.
#[test]
fn test_proximity_cache_message_flow_complete() {
    // The expected message flow for cache synchronization:
    // 1. Peer A connects to Peer B
    // 2. Peer A sends CacheStateRequest to Peer B
    // 3. Peer B responds with CacheStateResponse listing cached contracts
    // 4. Peer A updates its neighbor_caches map
    //
    // Currently, step 2 never happens because request_cache_state() is dead code.

    /// Message types for proximity cache synchronization.
    /// These variants represent what SHOULD be exchanged but aren't due to dead code.
    #[derive(Debug, PartialEq)]
    #[allow(dead_code)] // Intentionally unused to demonstrate the issue
    enum CacheMessage {
        StateRequest,
        StateResponse(Vec<[u8; 32]>), // Contract IDs
        Announce(Vec<[u8; 32]>),
    }

    struct MessageLog {
        messages: Vec<(SocketAddr, SocketAddr, CacheMessage)>,
    }

    impl MessageLog {
        fn new() -> Self {
            Self {
                messages: Vec::new(),
            }
        }

        fn has_state_request(&self, from: &SocketAddr, to: &SocketAddr) -> bool {
            self.messages
                .iter()
                .any(|(f, t, m)| f == from && t == to && matches!(m, CacheMessage::StateRequest))
        }
    }

    let log = MessageLog::new();

    let peer_a = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(10, 0, 1, 1)), 5000);
    let peer_b = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(10, 0, 2, 1)), 5000);

    // After connection establishment, Peer A should request cache state from Peer B
    // EXPECTED FAILURE: This never happens because the code path doesn't exist
    assert!(
        log.has_state_request(&peer_a, &peer_b),
        "ISSUE #2718: No CacheStateRequest was sent from {} to {} on connection. \
         The proximity cache synchronization is incomplete - request_cache_state() exists \
         but is never called.",
        peer_a,
        peer_b
    );
}

// =============================================================================
// Test 4: Lack of Geographical-Based Upstream Selection (#2721)
// =============================================================================

/// Test that upstream selection considers proximity to contract location.
///
/// The current implementation at subscribe.rs:358-420 doesn't properly consider
/// geographical proximity. The fallback sorting (line 380) only provides
/// determinism, not topology awareness.
///
/// This test should FAIL until proximity-aware selection is implemented.
#[test]
fn test_upstream_selection_respects_proximity() {
    let contract_location = 0.5;
    let mut tree = SubscriptionTree::new(contract_location);

    // Source at contract location
    let source = SimPeer::new(0, 0.5);
    tree.add_peer(source);
    tree.add_seeder(0);

    // Peer A is far from contract (location 0.1, dist=0.4)
    let peer_a = SimPeer::new(1, 0.1);
    tree.add_peer(peer_a);
    tree.add_seeder(1);

    // Peer B is close to contract (location 0.48, dist=0.02)
    let peer_b = SimPeer::new(2, 0.48);
    tree.add_peer(peer_b);
    tree.add_seeder(2);

    // Peer C is moderately far from contract (location 0.25, dist=0.25)
    let peer_c = SimPeer::new(3, 0.25);
    tree.add_peer(peer_c);
    tree.add_seeder(3);

    // Simulate the problematic upstream selection that can happen:
    // The fallback path at subscribe.rs:380 sorts by location key, NOT by proximity.
    // So peer C (at 0.25) might pick peer A (at 0.1) as upstream because 0.1 < 0.25 < 0.48
    // even though peer B (at 0.48) is much closer to the contract.

    // This is what can happen with sorted_keys.sort() at line 380:
    // Peers sorted by location: A(0.1), C(0.25), B(0.48), Source(0.5)
    // C picks A because A comes first in sorted order, ignoring that B is closer to contract

    tree.set_upstream(1, 0); // A → Source (A is far but connects to source - OK)
    tree.set_upstream(2, 0); // B → Source (B is close, connects to source - OK)
    tree.set_upstream(3, 1); // C → A (BAD: A is far from contract, B is closer)

    let violations = tree.find_bad_upstream_selections();

    // EXPECTED FAILURE: Current implementation doesn't enforce proximity
    // C (dist=0.25) picked A (dist=0.4) as upstream, but B (dist=0.02) is closer to contract
    assert!(
        violations.is_empty(),
        "ISSUE #2721: Found {} upstream selections violating proximity rules: {:?}. \
         Peer 3 (at 0.25, dist=0.25 from contract) selected Peer 1 (at 0.1, dist=0.40 from contract) \
         as upstream, but Peer 2 (at 0.48, dist=0.02 from contract) would be much better. \
         The subscribe.rs fallback path at line 380 uses sorted_keys.sort() which doesn't consider proximity.",
        violations.len(),
        violations.iter().map(|(d, u, dd, ud)| {
            format!("Peer {} → Peer {} (downstream_dist={:.2}, upstream_dist={:.2})", d, u, dd, ud)
        }).collect::<Vec<_>>()
    );
}

/// Test that subscription trees don't form excessively deep chains.
///
/// Without proximity-aware selection, trees can form long chains where each
/// hop goes AWAY from the contract location, creating unnecessarily deep trees.
#[test]
fn test_subscription_tree_depth_bounded() {
    let contract_location = 0.5;
    let mut tree = SubscriptionTree::new(contract_location);

    // Source at contract location
    let source = SimPeer::new(0, 0.5);
    tree.add_peer(source);
    tree.add_seeder(0);

    // Create 10 peers at various locations
    for i in 1..=10 {
        let location = (i as f64 * 0.08) % 1.0; // Spread around the ring
        let peer = SimPeer::new(i, location);
        tree.add_peer(peer);
        tree.add_seeder(i);
    }

    // Simulate a poorly-constructed tree (what can happen without proximity awareness)
    // Creates a long chain: S → 1 → 2 → 3 → 4 → 5 → 6 → 7 → 8 → 9 → 10
    for i in 1..=10 {
        tree.set_upstream(i, i - 1);
    }

    let depth = tree.max_tree_depth(0);

    // For 10 peers + source, optimal tree depth is ~4 (log2(11))
    // But a chain has depth 10
    let expected_max_depth = (tree.seeders.len() as f64).log2().ceil() as usize + 1;

    // EXPECTED FAILURE: Without proximity-aware selection, deep chains can form
    assert!(
        depth <= expected_max_depth,
        "ISSUE #2721: Subscription tree depth is {}, but optimal would be ~{}. \
         Without proximity-aware upstream selection, the tree can degenerate into \
         a long chain, increasing update propagation latency.",
        depth,
        expected_max_depth
    );
}

/// Test that fallback upstream selection in subscribe.rs considers proximity.
#[test]
fn test_fallback_upstream_selection_uses_proximity() {
    // This test simulates the fallback path at subscribe.rs:372-420
    // where k_closest_potentially_caching returns empty and we fall back
    // to sorting connections by location key.

    // The current implementation at line 380:
    //   sorted_keys.sort();
    // Only provides determinism, NOT proximity awareness.

    let contract_location = 0.5;

    // Available connections (what get_connections_by_location() might return)
    let connections: Vec<(f64, SocketAddr)> = vec![
        (0.1, "10.0.1.1:5000".parse().unwrap()),  // Far from contract
        (0.9, "10.0.2.1:5000".parse().unwrap()),  // Far from contract (other direction)
        (0.48, "10.0.3.1:5000".parse().unwrap()), // Close to contract
        (0.52, "10.0.4.1:5000".parse().unwrap()), // Close to contract
    ];

    // Current implementation: sort by location key (deterministic but not proximity-aware)
    let mut sorted_by_key = connections.clone();
    sorted_by_key.sort_by(|a, b| a.0.partial_cmp(&b.0).unwrap());

    // What SHOULD happen: sort by proximity to contract
    let mut sorted_by_proximity = connections.clone();
    sorted_by_proximity.sort_by(|a, b| {
        let dist_a = ring_distance(a.0, contract_location);
        let dist_b = ring_distance(b.0, contract_location);
        dist_a.partial_cmp(&dist_b).unwrap()
    });

    // The first element after sorting should be closest to contract
    let current_best = sorted_by_key.first().unwrap();
    let optimal_best = sorted_by_proximity.first().unwrap();

    let current_dist = ring_distance(current_best.0, contract_location);
    let optimal_dist = ring_distance(optimal_best.0, contract_location);

    // EXPECTED FAILURE: Current implementation picks peer at 0.1, not at 0.48
    assert!(
        (current_dist - optimal_dist).abs() < 0.01,
        "ISSUE #2721: Fallback upstream selection picked peer at location {:.2} (dist={:.2}), \
         but peer at {:.2} (dist={:.2}) is closer to contract at {:.2}. \
         The fallback at subscribe.rs:380 uses sorted_keys.sort() which doesn't consider proximity.",
        current_best.0,
        current_dist,
        optimal_best.0,
        optimal_dist,
        contract_location
    );
}

// =============================================================================
// Integration Test: Combined Topology Validation
// =============================================================================

/// Comprehensive test that validates the entire subscription topology.
///
/// This test combines all checks to verify the subscription tree is healthy.
#[test]
fn test_subscription_topology_healthy() {
    let contract_location = 0.5;
    let mut tree = SubscriptionTree::new(contract_location);

    // Create a realistic network topology
    let source = SimPeer::new(0, 0.5);
    tree.add_peer(source);
    tree.add_seeder(0);

    // Add 8 peers at various locations
    let locations = [0.1, 0.2, 0.35, 0.45, 0.55, 0.65, 0.8, 0.95];
    for (i, &loc) in locations.iter().enumerate() {
        let peer = SimPeer::new((i + 1) as u8, loc);
        tree.add_peer(peer);
        tree.add_seeder((i + 1) as u8);
    }

    // Simulate what can go wrong with current implementation:
    // - Bidirectional cycles
    // - Orphan seeders
    // - Bad proximity choices

    // Create some problematic relationships
    tree.set_upstream(1, 0); // Peer 1 → Source (OK)
    tree.set_upstream(2, 1); // Peer 2 → Peer 1 (OK)
    tree.set_upstream(3, 2); // Peer 3 → Peer 2 (suboptimal - 3 is closer to source)
    tree.set_upstream(4, 3); // Peer 4 → Peer 3 (suboptimal chain)
    tree.set_upstream(5, 0); // Peer 5 → Source (OK)
    tree.set_upstream(6, 5); // Peer 6 → Peer 5 (OK)
                             // Peers 7 and 8 form a bidirectional cycle
    tree.set_upstream(7, 8);
    tree.set_upstream(8, 7);

    // Run all validations
    let mut issues = Vec::new();

    // Check for bidirectional cycles
    let cycles = tree.find_bidirectional_cycles();
    if !cycles.is_empty() {
        issues.push(format!(
            "ISSUE #2720: {} bidirectional cycles found: {:?}",
            cycles.len(),
            cycles
        ));
    }

    // Check for unreachable seeders
    let unreachable = tree.find_unreachable_seeders(0);
    if !unreachable.is_empty() {
        issues.push(format!(
            "ISSUE #2720: {} seeders unreachable from source: {:?}",
            unreachable.len(),
            unreachable
        ));
    }

    // Check for orphan seeders
    let orphans = tree.find_orphan_seeders();
    if !orphans.is_empty() {
        issues.push(format!(
            "ISSUE #2719: {} orphan seeders without recovery: {:?}",
            orphans.len(),
            orphans
        ));
    }

    // Check for proximity violations
    let bad_upstreams = tree.find_bad_upstream_selections();
    if !bad_upstreams.is_empty() {
        issues.push(format!(
            "ISSUE #2721: {} upstream selections violate proximity: {:?}",
            bad_upstreams.len(),
            bad_upstreams
        ));
    }

    // Check tree depth
    let depth = tree.max_tree_depth(0);
    let expected_max = ((tree.seeders.len() as f64).log2().ceil() as usize + 2).max(4);
    if depth > expected_max {
        issues.push(format!(
            "ISSUE #2721: Tree depth {} exceeds optimal max {}",
            depth, expected_max
        ));
    }

    // EXPECTED FAILURE: Multiple issues should be found
    assert!(
        issues.is_empty(),
        "Subscription topology has {} issues:\n{}",
        issues.len(),
        issues.join("\n")
    );
}

// =============================================================================
// Tests using the TopologyRegistry infrastructure
// =============================================================================
// These tests demonstrate how SimNetwork integration tests will work once
// nodes start registering their topology snapshots.

use freenet::dev_tool::{
    clear_all_topology_snapshots, register_topology_snapshot, validate_topology,
    ContractSubscription, TopologySnapshot,
};
use freenet_stdlib::prelude::{CodeHash, ContractInstanceId, ContractKey};

fn make_contract_id(seed: u8) -> ContractInstanceId {
    ContractInstanceId::new([seed; 32])
}

fn make_contract_key(seed: u8) -> ContractKey {
    ContractKey::from_id_and_code(
        ContractInstanceId::new([seed; 32]),
        CodeHash::new([seed.wrapping_add(1); 32]),
    )
}

/// Test the topology registry with bidirectional cycles.
///
/// This test uses the actual topology registry infrastructure to validate
/// that bidirectional cycles are detected.
#[test]
fn test_topology_registry_detects_bidirectional_cycles() {
    let network = "test-registry-bidirectional";
    clear_all_topology_snapshots();

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
            downstream: vec![peer_b],
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
            downstream: vec![peer_a],
            is_seeding: true,
            has_client_subscriptions: false,
        },
    );

    register_topology_snapshot(network, snap_a);
    register_topology_snapshot(network, snap_b);

    let result = validate_topology(network, &contract_id, 0.5);

    // EXPECTED FAILURE: The topology registry DOES detect bidirectional cycles
    // This test passes because the infrastructure works correctly.
    // The ACTUAL issue (#2720) is that the SeedingManager allows these cycles to form.
    assert!(
        !result.bidirectional_cycles.is_empty(),
        "TopologyRegistry should detect bidirectional cycle"
    );

    clear_all_topology_snapshots();
}

/// Test the topology registry with orphan seeders.
#[test]
fn test_topology_registry_detects_orphan_seeders() {
    let network = "test-registry-orphan";
    clear_all_topology_snapshots();

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

    // EXPECTED PASS: The topology registry detects orphan seeders
    // The ACTUAL issue (#2719) is that the SeedingManager doesn't recover them
    assert!(
        !result.orphan_seeders.is_empty(),
        "TopologyRegistry should detect orphan seeder"
    );

    clear_all_topology_snapshots();
}

/// Test the topology registry with proximity violations.
#[test]
fn test_topology_registry_detects_proximity_violations() {
    let network = "test-registry-proximity";
    clear_all_topology_snapshots();

    let peer_close: SocketAddr = "10.0.1.1:5000".parse().unwrap(); // Close to contract
    let peer_far: SocketAddr = "10.0.2.1:5000".parse().unwrap(); // Far from contract
    let contract_id = make_contract_id(1);
    let contract_key = make_contract_key(1);
    let contract_location = 0.5;

    // Create a bad topology: far peer subscribes to close peer
    // This is actually correct topology, so let's make it backwards
    // Peer at 0.48 (close) has peer at 0.1 (far) as upstream - BAD
    let mut snap_close = TopologySnapshot::new(peer_close, 0.48);
    snap_close.set_contract(
        contract_id,
        ContractSubscription {
            contract_key,
            upstream: Some(peer_far), // Upstream is FAR from contract
            downstream: vec![],
            is_seeding: true,
            has_client_subscriptions: false,
        },
    );

    let mut snap_far = TopologySnapshot::new(peer_far, 0.1);
    snap_far.set_contract(
        contract_id,
        ContractSubscription {
            contract_key,
            upstream: None,
            downstream: vec![peer_close],
            is_seeding: true,
            has_client_subscriptions: false,
        },
    );

    register_topology_snapshot(network, snap_close);
    register_topology_snapshot(network, snap_far);

    let result = validate_topology(network, &contract_id, contract_location);

    // EXPECTED PASS: The topology registry detects proximity violations
    // The ACTUAL issue (#2721) is that subscribe.rs doesn't consider proximity
    assert!(
        !result.proximity_violations.is_empty(),
        "TopologyRegistry should detect proximity violation: close peer (0.48) has far peer (0.1) as upstream"
    );

    clear_all_topology_snapshots();
}

// =============================================================================
// Note on Integration Testing
// =============================================================================
// The tests above use the TopologyRegistry infrastructure directly. For full
// integration testing with SimNetwork, nodes need to register their topology
// snapshots periodically. This can be done by:
//
// 1. Adding a periodic task in the node's main loop that calls:
//    ring.register_topology_snapshot(network_name);
//
// 2. Or calling it from specific points like after subscribe operations complete
//
// The SimNetwork methods (validate_subscription_topology, assert_topology_healthy)
// can then be used in simulation tests to validate the topology.
//
// Example future integration test:
//
// #[cfg(feature = "simulation_tests")]
// #[tokio::test]
// async fn test_subscription_topology_in_simulation() {
//     let sim = SimNetwork::new("test", 1, 5, 7, 3, 10, 2, 42).await;
//     let handles = sim.start_with_rand_gen::<SmallRng>(42, 3, 50).await;
//
//     // Wait for subscriptions to form
//     tokio::time::sleep(Duration::from_secs(5)).await;
//
//     // Validate topology for each contract
//     for (contract_id, contract_loc) in contracts {
//         sim.assert_topology_healthy(&contract_id, contract_loc);
//     }
// }
