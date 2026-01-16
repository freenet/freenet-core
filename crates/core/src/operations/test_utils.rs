//! Test utilities for operations module testing.
//!
//! This module provides mock implementations and helper functions for unit testing
//! the operations module without requiring a full OpManager or network setup.

#![allow(dead_code)] // Test utilities may not all be used yet

use std::collections::HashMap;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::sync::{Arc, Mutex};

use crate::message::NetMessage;
use crate::node::{ConnectionError, NetworkBridge};
use crate::ring::PeerKeyLocation;
use crate::transport::TransportKeypair;
use freenet_stdlib::prelude::{CodeHash, ContractInstanceId, ContractKey};

type ConnResult<T> = std::result::Result<T, ConnectionError>;

/// A mock NetworkBridge that records all sent messages for verification.
///
/// This allows tests to:
/// - Verify what messages were sent
/// - Check the target addresses
/// - Simulate connection failures
#[derive(Clone, Default)]
pub struct MockNetworkBridge {
    /// Records of (target_addr, message) for all sent messages
    sent_messages: Arc<Mutex<Vec<(SocketAddr, NetMessage)>>>,
    /// Addresses that should fail when sending
    fail_addresses: Arc<Mutex<Vec<SocketAddr>>>,
    /// Addresses that have been "dropped"
    dropped_connections: Arc<Mutex<Vec<SocketAddr>>>,
}

impl MockNetworkBridge {
    pub fn new() -> Self {
        Self::default()
    }

    /// Configure an address to fail when sending
    pub fn fail_on_send(&self, addr: SocketAddr) {
        self.fail_addresses.lock().unwrap().push(addr);
    }

    /// Get all sent messages
    pub fn sent_messages(&self) -> Vec<(SocketAddr, NetMessage)> {
        self.sent_messages.lock().unwrap().clone()
    }

    /// Get sent messages to a specific address
    pub fn messages_to(&self, addr: SocketAddr) -> Vec<NetMessage> {
        self.sent_messages
            .lock()
            .unwrap()
            .iter()
            .filter(|(a, _)| *a == addr)
            .map(|(_, m)| m.clone())
            .collect()
    }

    /// Get all dropped connections
    pub fn dropped_connections(&self) -> Vec<SocketAddr> {
        self.dropped_connections.lock().unwrap().clone()
    }

    /// Clear all recorded data
    pub fn clear(&self) {
        self.sent_messages.lock().unwrap().clear();
        self.dropped_connections.lock().unwrap().clear();
    }
}

impl NetworkBridge for MockNetworkBridge {
    async fn send(&self, target_addr: SocketAddr, msg: NetMessage) -> ConnResult<()> {
        // Check if this address should fail
        if self.fail_addresses.lock().unwrap().contains(&target_addr) {
            return Err(ConnectionError::SendNotCompleted(target_addr));
        }

        self.sent_messages.lock().unwrap().push((target_addr, msg));
        Ok(())
    }

    async fn drop_connection(&mut self, peer_addr: SocketAddr) -> ConnResult<()> {
        self.dropped_connections.lock().unwrap().push(peer_addr);
        Ok(())
    }
}

/// Helper to create a test PeerKeyLocation with a specific port
pub fn make_peer(port: u16) -> PeerKeyLocation {
    let addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), port);
    let keypair = TransportKeypair::new();
    PeerKeyLocation::new(keypair.public().clone(), addr)
}

/// Helper to create a test PeerKeyLocation with a specific IP and port
pub fn make_peer_with_ip(ip: [u8; 4], port: u16) -> PeerKeyLocation {
    let addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(ip[0], ip[1], ip[2], ip[3])), port);
    let keypair = TransportKeypair::new();
    PeerKeyLocation::new(keypair.public().clone(), addr)
}

/// Helper to create a test ContractKey from a seed byte
pub fn make_contract_key(seed: u8) -> ContractKey {
    ContractKey::from_id_and_code(
        ContractInstanceId::new([seed; 32]),
        CodeHash::new([seed.wrapping_add(1); 32]),
    )
}

/// A mock ring that tracks routing decisions for testing.
///
/// This allows testing subscription and routing logic without a full Ring setup.
#[derive(Clone)]
pub struct MockRing {
    /// The "own" location for this mock peer
    pub own_location: PeerKeyLocation,
    /// Available candidates to return from k_closest queries
    pub candidates: Vec<PeerKeyLocation>,
    /// Contracts this mock is "seeding"
    pub seeding_contracts: Arc<Mutex<Vec<ContractKey>>>,
    /// Records of k_closest calls: (key, skip_count, k)
    pub k_closest_calls: Arc<Mutex<Vec<(ContractKey, usize, usize)>>>,
}

impl MockRing {
    pub fn new(own_location: PeerKeyLocation, candidates: Vec<PeerKeyLocation>) -> Self {
        Self {
            own_location,
            candidates,
            seeding_contracts: Arc::new(Mutex::new(Vec::new())),
            k_closest_calls: Arc::new(Mutex::new(Vec::new())),
        }
    }

    pub fn own_location(&self) -> &PeerKeyLocation {
        &self.own_location
    }

    pub fn is_seeding_contract(&self, key: &ContractKey) -> bool {
        self.seeding_contracts.lock().unwrap().contains(key)
    }

    pub fn seed_contract(&self, key: ContractKey, _size_bytes: u64) {
        let mut seeding = self.seeding_contracts.lock().unwrap();
        if !seeding.contains(&key) {
            seeding.push(key);
        }
    }

    pub fn record_get_access(&self, key: ContractKey, size_bytes: u64) {
        self.seed_contract(key, size_bytes);
    }

    pub fn record_subscribe_access(&self, key: ContractKey, size_bytes: u64) {
        self.seed_contract(key, size_bytes);
    }

    /// Simulates k_closest_potentially_caching
    pub fn k_closest_potentially_caching(
        &self,
        key: &ContractKey,
        skip_list: &[SocketAddr],
        k: usize,
    ) -> Vec<PeerKeyLocation> {
        self.k_closest_calls
            .lock()
            .unwrap()
            .push((*key, skip_list.len(), k));

        self.candidates
            .iter()
            .filter(|c| {
                c.socket_addr()
                    .map(|addr| !skip_list.contains(&addr))
                    .unwrap_or(true)
            })
            .take(k)
            .cloned()
            .collect()
    }

    /// Get all recorded k_closest calls
    pub fn get_k_closest_calls(&self) -> Vec<(ContractKey, usize, usize)> {
        self.k_closest_calls.lock().unwrap().clone()
    }
}

/// A simplified mock for broadcast target resolution
pub struct MockBroadcastResolver {
    /// Mapping from contract key to list of subscribers
    subscribers: HashMap<ContractKey, Vec<PeerKeyLocation>>,
}

impl MockBroadcastResolver {
    pub fn new() -> Self {
        Self {
            subscribers: HashMap::new(),
        }
    }

    pub fn add_subscriber(&mut self, key: ContractKey, subscriber: PeerKeyLocation) {
        self.subscribers.entry(key).or_default().push(subscriber);
    }

    pub fn get_broadcast_targets(
        &self,
        key: &ContractKey,
        _sender: &PeerKeyLocation,
    ) -> Vec<PeerKeyLocation> {
        self.subscribers.get(key).cloned().unwrap_or_default()
    }
}

impl Default for MockBroadcastResolver {
    fn default() -> Self {
        Self::new()
    }
}

// ============================================================================
// TestNodeBuilder - Unified test infrastructure for isolated node testing
// ============================================================================

use crate::contract::{ContractExecutor, Executor, MockRuntime, UpsertResult};
use crate::wasm_runtime::MockStateStorage;
use either::Either;
use freenet_stdlib::prelude::{
    ContractCode, ContractContainer, ContractWasmAPIVersion, Parameters, RelatedContracts,
    WrappedContract, WrappedState,
};

/// Builder for creating isolated node test environments.
///
/// This builder combines MockNetworkBridge, MockRing, and MockRuntime to create
/// a complete isolated node test environment without requiring network setup.
///
/// # Example
/// ```ignore
/// let node = TestNodeBuilder::new()
///     .with_contract(contract, initial_state)
///     .with_candidates(vec![peer1, peer2])
///     .build()
///     .await;
///
/// // Test operations
/// let result = node.put(contract_key, new_state).await;
/// assert!(result.is_ok());
///
/// // Verify messages were sent
/// assert_eq!(node.bridge.sent_messages().len(), 0); // Isolated node
/// ```
#[derive(Default)]
pub struct TestNodeBuilder {
    /// Contracts to pre-load into the node
    contracts: Vec<(ContractContainer, WrappedState)>,
    /// Candidate peers for routing queries
    candidates: Vec<PeerKeyLocation>,
    /// Own peer location (generated if not provided)
    own_location: Option<PeerKeyLocation>,
}

impl TestNodeBuilder {
    /// Create a new test node builder with default settings.
    pub fn new() -> Self {
        Self::default()
    }

    /// Add a contract with initial state to be loaded at startup.
    pub fn with_contract(mut self, contract: ContractContainer, state: WrappedState) -> Self {
        self.contracts.push((contract, state));
        self
    }

    /// Set the candidate peers for routing queries.
    pub fn with_candidates(mut self, candidates: Vec<PeerKeyLocation>) -> Self {
        self.candidates = candidates;
        self
    }

    /// Set the own peer location for this node.
    pub fn with_own_location(mut self, location: PeerKeyLocation) -> Self {
        self.own_location = Some(location);
        self
    }

    /// Build the test node with all configured settings.
    pub async fn build(self) -> TestNode {
        let own_location = self.own_location.unwrap_or_else(|| make_peer(4000));
        let bridge = MockNetworkBridge::new();
        let ring = MockRing::new(own_location.clone(), self.candidates);
        let storage = MockStateStorage::new();

        let mut executor = Executor::<MockRuntime, MockStateStorage>::new_mock_in_memory(
            "test", storage, None, None,
        )
        .await
        .expect("create test executor");

        // Pre-load contracts
        for (contract, state) in self.contracts {
            let key = contract.key();
            executor
                .upsert_contract_state(
                    key,
                    Either::Left(state),
                    RelatedContracts::default(),
                    Some(contract),
                )
                .await
                .expect("pre-load contract");
        }

        TestNode {
            bridge,
            ring,
            executor,
            own_location,
        }
    }
}

/// An isolated test node with mock network and ring.
///
/// This struct provides direct access to the executor and mock components
/// for testing node operations in isolation.
pub struct TestNode {
    /// Mock network bridge that records sent messages
    pub bridge: MockNetworkBridge,
    /// Mock ring for routing queries
    pub ring: MockRing,
    /// Contract executor with in-memory storage
    pub executor: Executor<MockRuntime, MockStateStorage>,
    /// This node's peer location
    pub own_location: PeerKeyLocation,
}

impl TestNode {
    /// Get the executor's state storage for direct inspection
    pub fn storage(&self) -> &MockStateStorage {
        // Note: MockStateStorage is cloneable, but for inspection we access through executor
        unimplemented!("Use executor.fetch_contract() for state inspection")
    }

    /// Put a contract with state into the node.
    pub async fn put(
        &mut self,
        contract: ContractContainer,
        state: WrappedState,
    ) -> Result<UpsertResult, Box<dyn std::error::Error>> {
        let key = contract.key();
        let result = self
            .executor
            .upsert_contract_state(
                key,
                Either::Left(state),
                RelatedContracts::default(),
                Some(contract),
            )
            .await?;
        Ok(result)
    }

    /// Update a contract's state (contract must already exist).
    pub async fn update(
        &mut self,
        key: ContractKey,
        state: WrappedState,
    ) -> Result<UpsertResult, Box<dyn std::error::Error>> {
        let result = self
            .executor
            .upsert_contract_state(key, Either::Left(state), RelatedContracts::default(), None)
            .await?;
        Ok(result)
    }

    /// Get a contract's current state.
    pub async fn get(
        &mut self,
        key: ContractKey,
    ) -> Result<Option<WrappedState>, Box<dyn std::error::Error>> {
        match self.executor.fetch_contract(key, false).await {
            Ok((state, _)) => Ok(state),
            Err(e) => Err(Box::new(std::io::Error::new(
                std::io::ErrorKind::NotFound,
                format!("Contract not found: {}", e),
            ))),
        }
    }
}

/// Create a test contract from code bytes.
pub fn make_test_contract(code_bytes: &[u8]) -> ContractContainer {
    let code = ContractCode::from(code_bytes.to_vec());
    let params = Parameters::from(vec![]);
    ContractContainer::Wasm(ContractWasmAPIVersion::V1(WrappedContract::new(
        Arc::new(code),
        params,
    )))
}

/// Create a test contract with specific parameters.
pub fn make_test_contract_with_params(code_bytes: &[u8], params: Vec<u8>) -> ContractContainer {
    let code = ContractCode::from(code_bytes.to_vec());
    let params = Parameters::from(params);
    ContractContainer::Wasm(ContractWasmAPIVersion::V1(WrappedContract::new(
        Arc::new(code),
        params,
    )))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::message::{NetMessageV1, Transaction};
    use crate::operations::connect::ConnectMsg;

    #[tokio::test]
    async fn mock_network_bridge_records_sent_messages() {
        let bridge = MockNetworkBridge::new();
        let target = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 5000);

        // Create a simple message for testing
        let tx = Transaction::new::<ConnectMsg>();
        let msg = NetMessage::V1(NetMessageV1::Aborted(tx));

        bridge.send(target, msg.clone()).await.unwrap();

        let sent = bridge.sent_messages();
        assert_eq!(sent.len(), 1);
        assert_eq!(sent[0].0, target);
    }

    #[tokio::test]
    async fn mock_network_bridge_can_fail_on_specific_addresses() {
        let bridge = MockNetworkBridge::new();
        let fail_addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 5001);
        let ok_addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 5002);

        bridge.fail_on_send(fail_addr);

        let tx = Transaction::new::<ConnectMsg>();
        let msg = NetMessage::V1(NetMessageV1::Aborted(tx));

        // Should fail for fail_addr
        assert!(bridge.send(fail_addr, msg.clone()).await.is_err());

        // Should succeed for ok_addr
        assert!(bridge.send(ok_addr, msg).await.is_ok());
    }

    #[tokio::test]
    async fn mock_network_bridge_records_dropped_connections() {
        let mut bridge = MockNetworkBridge::new();
        let addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 5000);

        bridge.drop_connection(addr).await.unwrap();

        let dropped = bridge.dropped_connections();
        assert_eq!(dropped.len(), 1);
        assert_eq!(dropped[0], addr);
    }

    #[test]
    fn mock_ring_tracks_seeding() {
        let own = make_peer(4000);
        let ring = MockRing::new(own, vec![]);

        let key = make_contract_key(1);
        assert!(!ring.is_seeding_contract(&key));

        ring.seed_contract(key, 100);
        assert!(ring.is_seeding_contract(&key));
    }

    #[test]
    fn mock_ring_k_closest_filters_skip_list() {
        let own = make_peer(4000);
        let peer1 = make_peer(5000);
        let peer2 = make_peer(5001);
        let peer3 = make_peer(5002);

        let ring = MockRing::new(own, vec![peer1.clone(), peer2.clone(), peer3.clone()]);

        let key = make_contract_key(1);
        let skip = vec![peer1.socket_addr().unwrap()];

        let result = ring.k_closest_potentially_caching(&key, &skip, 3);

        // peer1 should be skipped
        assert_eq!(result.len(), 2);
        assert!(!result.iter().any(|p| p == &peer1));
        assert!(result.iter().any(|p| p == &peer2));
        assert!(result.iter().any(|p| p == &peer3));
    }

    #[test]
    fn make_peer_creates_valid_peer_key_location() {
        let peer = make_peer(5000);
        assert!(peer.socket_addr().is_some());
        assert_eq!(peer.socket_addr().unwrap().port(), 5000);
    }

    #[test]
    fn make_contract_key_creates_unique_keys() {
        let key1 = make_contract_key(1);
        let key2 = make_contract_key(2);
        assert_ne!(key1, key2);
    }

    // ========================================================================
    // TestNodeBuilder and Isolated Node Tests (Stream E - Issue #1885)
    // ========================================================================

    /// Test that TestNodeBuilder creates a working node with pre-loaded contracts.
    #[tokio::test]
    async fn test_node_builder_basic() {
        let contract = make_test_contract(b"test_contract_1");
        let state = WrappedState::new(vec![1, 2, 3, 4, 5]);

        let mut node = TestNodeBuilder::new()
            .with_contract(contract.clone(), state.clone())
            .build()
            .await;

        // Verify the contract was loaded
        let retrieved = node.get(contract.key()).await.expect("get should succeed");
        assert_eq!(
            retrieved.unwrap().as_ref(),
            state.as_ref(),
            "Retrieved state should match"
        );
    }

    /// Test putting a large state (> 1MB) into an isolated node.
    ///
    /// This tests that isolated nodes can handle large contract states
    /// without network timeouts or memory issues.
    #[tokio::test]
    async fn test_isolated_node_put_large_state() {
        let mut node = TestNodeBuilder::new().build().await;

        // Create a large state (1MB+)
        let large_state_size = 1024 * 1024 + 100; // Just over 1MB
        let large_state = WrappedState::new(vec![0xAB; large_state_size]);
        let contract = make_test_contract(b"large_state_contract");
        let key = contract.key();

        // Put the large state
        let result = node
            .put(contract, large_state.clone())
            .await
            .expect("put should succeed");
        assert!(
            matches!(result, UpsertResult::Updated(_)),
            "Large state should be stored"
        );

        // Verify we can retrieve it
        let retrieved = node.get(key).await.expect("get should succeed");
        assert_eq!(
            retrieved.as_ref().map(|s| s.size()),
            Some(large_state_size),
            "Retrieved state should have same size"
        );
        assert_eq!(
            retrieved.unwrap().as_ref(),
            large_state.as_ref(),
            "Retrieved state should match"
        );

        // Verify no network messages were sent (isolated node)
        assert!(
            node.bridge.sent_messages().is_empty(),
            "Isolated node should not send network messages"
        );
    }

    /// Test concurrent updates to the same contract.
    ///
    /// This tests the CRDT merge behavior when multiple updates arrive
    /// for the same contract - larger hash should win.
    #[tokio::test]
    async fn test_isolated_node_concurrent_updates() {
        let mut node = TestNodeBuilder::new().build().await;

        // Create initial contract
        let contract = make_test_contract(b"concurrent_update_contract");
        let key = contract.key();
        let initial_state = WrappedState::new(vec![0]);

        node.put(contract, initial_state)
            .await
            .expect("initial put should succeed");

        // Create multiple "concurrent" updates with different values
        let state_a = WrappedState::new(vec![1, 1, 1]);
        let state_b = WrappedState::new(vec![2, 2, 2]);
        let state_c = WrappedState::new(vec![3, 3, 3]);

        // Determine which state has the largest hash (will be the winner)
        let hash_a = blake3::hash(state_a.as_ref());
        let hash_b = blake3::hash(state_b.as_ref());
        let hash_c = blake3::hash(state_c.as_ref());

        let winner = [
            (state_a.clone(), hash_a),
            (state_b.clone(), hash_b),
            (state_c.clone(), hash_c),
        ]
        .into_iter()
        .max_by_key(|(_, h)| *h.as_bytes())
        .map(|(s, _)| s)
        .unwrap();

        // Apply all updates in arbitrary order
        node.update(key, state_b.clone())
            .await
            .expect("update b should succeed");
        node.update(key, state_a.clone())
            .await
            .expect("update a should succeed");
        node.update(key, state_c.clone())
            .await
            .expect("update c should succeed");

        // Verify the winner state is stored (deterministic CRDT merge)
        let retrieved = node.get(key).await.expect("get should succeed");
        assert_eq!(
            retrieved.unwrap().as_ref(),
            winner.as_ref(),
            "Largest-hash state should win after concurrent updates"
        );
    }

    /// Test subscribing to a contract before it exists (should fail gracefully).
    ///
    /// Note: The TestNode doesn't implement full subscribe semantics,
    /// so this test verifies the get behavior for non-existent contracts.
    #[tokio::test]
    async fn test_isolated_node_get_before_put() {
        let mut node = TestNodeBuilder::new().build().await;

        // Try to get a contract that doesn't exist
        let non_existent_key = make_contract_key(99);
        let result = node.get(non_existent_key).await;

        // Should return an error for non-existent contract
        assert!(result.is_err(), "Get on non-existent contract should fail");
    }

    /// Test handling multiple contracts in the same isolated node.
    #[tokio::test]
    async fn test_isolated_node_multiple_contracts() {
        // Pre-load two contracts
        let contract1 = make_test_contract(b"multi_contract_1");
        let state1 = WrappedState::new(b"state_for_contract_1".to_vec());

        let contract2 = make_test_contract(b"multi_contract_2");
        let state2 = WrappedState::new(b"state_for_contract_2".to_vec());

        let mut node = TestNodeBuilder::new()
            .with_contract(contract1.clone(), state1.clone())
            .with_contract(contract2.clone(), state2.clone())
            .build()
            .await;

        // Add a third contract after build
        let contract3 = make_test_contract(b"multi_contract_3");
        let state3 = WrappedState::new(b"state_for_contract_3".to_vec());
        node.put(contract3.clone(), state3.clone())
            .await
            .expect("put contract3 should succeed");

        // Verify all three contracts exist with correct states
        let retrieved1 = node.get(contract1.key()).await.expect("get contract1");
        assert_eq!(retrieved1.unwrap().as_ref(), state1.as_ref());

        let retrieved2 = node.get(contract2.key()).await.expect("get contract2");
        assert_eq!(retrieved2.unwrap().as_ref(), state2.as_ref());

        let retrieved3 = node.get(contract3.key()).await.expect("get contract3");
        assert_eq!(retrieved3.unwrap().as_ref(), state3.as_ref());

        // Update contract2 and verify others are unaffected
        let new_state2 = WrappedState::new(b"updated_state_for_contract_2".to_vec());

        // Only update if new state has larger hash
        let old_hash = blake3::hash(state2.as_ref());
        let new_hash = blake3::hash(new_state2.as_ref());

        // Use state that will definitely win
        let winning_state2 = if new_hash.as_bytes() > old_hash.as_bytes() {
            new_state2
        } else {
            // Generate a state with guaranteed larger hash
            WrappedState::new(vec![0xFF; 100])
        };

        node.update(contract2.key(), winning_state2.clone())
            .await
            .expect("update contract2");

        // contract1 should be unchanged
        let check1 = node
            .get(contract1.key())
            .await
            .expect("get contract1 again");
        assert_eq!(
            check1.unwrap().as_ref(),
            state1.as_ref(),
            "Contract1 should be unchanged"
        );

        // contract2 should be updated
        let check2 = node
            .get(contract2.key())
            .await
            .expect("get contract2 again");
        assert_eq!(
            check2.unwrap().as_ref(),
            winning_state2.as_ref(),
            "Contract2 should be updated"
        );
    }

    /// Test that state validation failures are handled correctly.
    ///
    /// The MockRuntime doesn't perform actual contract validation,
    /// but we can test the CRDT merge rejection behavior.
    #[tokio::test]
    async fn test_isolated_node_state_validation_via_crdt() {
        let mut node = TestNodeBuilder::new().build().await;

        // Create contract with initial state that has a large hash
        let contract = make_test_contract(b"validation_test_contract");
        let key = contract.key();

        // Find a state with a large hash
        let state_with_large_hash = WrappedState::new(vec![0xFF; 50]);
        let state_with_small_hash = WrappedState::new(vec![0x00; 50]);

        // Determine which is actually larger
        let large_hash = blake3::hash(state_with_large_hash.as_ref());
        let small_hash = blake3::hash(state_with_small_hash.as_ref());

        let (winning, losing) = if large_hash.as_bytes() > small_hash.as_bytes() {
            (state_with_large_hash, state_with_small_hash)
        } else {
            (state_with_small_hash, state_with_large_hash)
        };

        // Put the winning state first
        node.put(contract, winning.clone())
            .await
            .expect("initial put should succeed");

        // Try to update with the losing state - should be rejected
        let result = node
            .update(key, losing.clone())
            .await
            .expect("update should not error");

        // The update should be rejected (CurrentWon means local state won)
        assert!(
            matches!(result, UpsertResult::CurrentWon(_)),
            "State with smaller hash should be rejected"
        );

        // Verify the winning state is still stored
        let retrieved = node.get(key).await.expect("get should succeed");
        assert_eq!(
            retrieved.unwrap().as_ref(),
            winning.as_ref(),
            "Original state should still be stored"
        );
    }

    /// Test that TestNodeBuilder properly configures MockRing with candidates.
    #[tokio::test]
    async fn test_node_builder_with_candidates() {
        let peer1 = make_peer(5001);
        let peer2 = make_peer(5002);
        let peer3 = make_peer(5003);

        let node = TestNodeBuilder::new()
            .with_candidates(vec![peer1.clone(), peer2.clone(), peer3.clone()])
            .build()
            .await;

        // Query k_closest through the mock ring
        let contract_key = make_contract_key(1);
        let candidates = node
            .ring
            .k_closest_potentially_caching(&contract_key, &[], 3);

        assert_eq!(candidates.len(), 3, "Should return all 3 candidates");
        assert!(candidates.contains(&peer1));
        assert!(candidates.contains(&peer2));
        assert!(candidates.contains(&peer3));

        // Verify k_closest call was recorded
        let calls = node.ring.get_k_closest_calls();
        assert_eq!(calls.len(), 1, "Should record 1 k_closest call");
        assert_eq!(calls[0].2, 3, "Should have requested k=3");
    }

    /// Test that own location is set correctly.
    #[tokio::test]
    async fn test_node_builder_with_own_location() {
        let custom_location = make_peer_with_ip([10, 0, 0, 1], 9000);

        let node = TestNodeBuilder::new()
            .with_own_location(custom_location.clone())
            .build()
            .await;

        assert_eq!(
            node.own_location.socket_addr(),
            custom_location.socket_addr(),
            "Own location should match configured value"
        );
        assert_eq!(
            node.ring.own_location().socket_addr(),
            custom_location.socket_addr(),
            "Ring's own location should match"
        );
    }
}
