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
use freenet_stdlib::prelude::{ContractInstanceId, ContractKey};

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
    ContractKey::from(ContractInstanceId::new([seed; 32]))
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

    pub fn should_seed(&self, _key: &ContractKey) -> bool {
        // In tests, always willing to seed
        true
    }

    pub fn is_seeding_contract(&self, key: &ContractKey) -> bool {
        self.seeding_contracts.lock().unwrap().contains(key)
    }

    pub fn seed_contract(&self, key: ContractKey) {
        let mut seeding = self.seeding_contracts.lock().unwrap();
        if !seeding.contains(&key) {
            seeding.push(key);
        }
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

        ring.seed_contract(key);
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
}
