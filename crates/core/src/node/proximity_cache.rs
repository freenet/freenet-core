//! Proximity cache manager for tracking which neighbors cache which contracts.
//!
//! This module enables UPDATE forwarding to nearby seeders who may not be explicitly
//! subscribed. When a peer caches a contract (via PUT or GET), it announces this to
//! its neighbors. Neighbors track this information and include known seeders in
//! UPDATE broadcast targets.
//!
//! # Design Note: Two Independent Mechanisms
//!
//! Freenet uses two complementary mechanisms for UPDATE propagation:
//!
//! 1. **Subscription Tree** (existing): Propagates updates to peers with downstream
//!    interest (users/peers subscribed through them). Tree structure rooted at
//!    contract location, extending toward clients. Tracked via `SeedingManager.subscribers`.
//!
//! 2. **Proximity Cache** (this module): Propagates updates to nearby seeders who
//!    cache the contract but may not be explicitly subscribed. Mesh structure among
//!    peers near the contract's location. Tracked via `ProximityCacheManager.neighbor_caches`.
//!
//! These mechanisms are kept independent because they have different lifecycles
//! (subscriptions are explicit; seeding follows cache eviction) and different data
//! structures. They are combined at the broadcast targeting point in
//! `OpManager::get_broadcast_targets_update()`, where HashSet naturally deduplicates
//! any overlap.

use std::{collections::HashSet, net::SocketAddr, sync::Arc};

use dashmap::{DashMap, DashSet};
use freenet_stdlib::prelude::{ContractInstanceId, ContractKey};
use tracing::{debug, info, trace};

use crate::message::ProximityCacheMessage;

/// Manages proximity-based cache tracking for UPDATE forwarding.
///
/// Tracks:
/// - Which contracts this node has cached locally
/// - Which contracts each connected neighbor has cached
///
/// This information is used to forward UPDATEs to seeders who cache a contract
/// but may not be explicitly subscribed to it.
pub struct ProximityCacheManager {
    /// Contracts we are caching locally.
    my_cache: Arc<DashSet<ContractInstanceId>>,

    /// What we know about our neighbors' caches.
    /// Maps neighbor address to the set of contracts they're caching.
    neighbor_caches: DashMap<SocketAddr, HashSet<ContractInstanceId>>,
}

impl Default for ProximityCacheManager {
    fn default() -> Self {
        Self::new()
    }
}

impl ProximityCacheManager {
    /// Create a new proximity cache manager.
    pub fn new() -> Self {
        Self {
            my_cache: Arc::new(DashSet::new()),
            neighbor_caches: DashMap::new(),
        }
    }

    /// Called when we cache a new contract (via PUT or successful GET).
    ///
    /// Returns a `CacheAnnounce` message to broadcast to neighbors if this
    /// is a newly cached contract, or `None` if we already had it cached.
    pub fn on_contract_cached(&self, contract_key: &ContractKey) -> Option<ProximityCacheMessage> {
        let contract_id = *contract_key.id();

        if self.my_cache.insert(contract_id) {
            info!(
                contract = %contract_key,
                "PROXIMITY_CACHE: Added contract to local cache"
            );

            Some(ProximityCacheMessage::CacheAnnounce {
                added: vec![contract_id],
                removed: vec![],
                is_response: false,
            })
        } else {
            trace!(
                contract = %contract_key,
                "PROXIMITY_CACHE: Contract already in local cache"
            );
            None
        }
    }

    /// Called when we evict a contract from our cache.
    ///
    /// Returns a `CacheAnnounce` message to broadcast to neighbors if the
    /// contract was in our cache, or `None` if it wasn't.
    #[allow(dead_code)]
    pub fn on_contract_evicted(&self, contract_key: &ContractKey) -> Option<ProximityCacheMessage> {
        let contract_id = *contract_key.id();

        if self.my_cache.remove(&contract_id).is_some() {
            debug!(
                contract = %contract_key,
                "PROXIMITY_CACHE: Removed contract from local cache"
            );

            Some(ProximityCacheMessage::CacheAnnounce {
                added: vec![],
                removed: vec![contract_id],
                is_response: false,
            })
        } else {
            None
        }
    }

    /// Process an incoming proximity cache message from a neighbor.
    ///
    /// Returns an optional response message to send back.
    pub fn handle_message(
        &self,
        from: SocketAddr,
        message: ProximityCacheMessage,
    ) -> Option<ProximityCacheMessage> {
        match message {
            ProximityCacheMessage::CacheAnnounce {
                added,
                removed,
                is_response,
            } => {
                // Get what we previously knew about this peer BEFORE updating.
                // This is needed to detect genuinely NEW contracts vs duplicates.
                let previously_known: HashSet<ContractInstanceId> = self
                    .neighbor_caches
                    .get(&from)
                    .map(|entry| entry.value().clone())
                    .unwrap_or_default();

                // Update neighbor's cache state
                if let Some(mut entry) = self.neighbor_caches.get_mut(&from) {
                    entry.extend(added.iter().copied());
                    for id in &removed {
                        entry.remove(id);
                    }
                } else if !added.is_empty() {
                    self.neighbor_caches
                        .insert(from, added.iter().copied().collect());
                }

                let neighbor_contracts = self
                    .neighbor_caches
                    .get(&from)
                    .map(|c| c.len())
                    .unwrap_or(0);

                info!(
                    peer = %from,
                    total_contracts = neighbor_contracts,
                    is_response = is_response,
                    "PROXIMITY_CACHE: Updated neighbor cache state"
                );

                // Don't respond to responses - this prevents ping-pong.
                // The is_response flag indicates the sender was already responding
                // to our announcement, so they already know we have these contracts.
                if is_response {
                    return None;
                }

                // Find contracts that are:
                // 1. NEW from this peer (not previously known from them)
                // 2. Also in our local cache
                // We respond so they can include us in their UPDATE targets.
                let overlapping: Vec<ContractInstanceId> = added
                    .iter()
                    .filter(|id| !previously_known.contains(id)) // Only NEW contracts
                    .filter(|id| self.my_cache.contains(*id)) // That we also have
                    .copied()
                    .collect();

                if !overlapping.is_empty() {
                    debug!(
                        peer = %from,
                        overlapping_count = overlapping.len(),
                        "PROXIMITY_CACHE: Responding with reciprocal announcement for shared contracts"
                    );
                    Some(ProximityCacheMessage::CacheAnnounce {
                        added: overlapping,
                        removed: vec![],
                        is_response: true, // Mark as response to prevent ping-pong
                    })
                } else {
                    None
                }
            }

            ProximityCacheMessage::CacheStateRequest => {
                let mut contracts: Vec<ContractInstanceId> =
                    self.my_cache.iter().map(|r| *r.key()).collect();
                // Sort for deterministic message order (DashSet iteration is non-deterministic)
                // ContractInstanceId doesn't impl Ord, so sort by string representation
                contracts.sort_by_key(|a| a.to_string());

                debug!(
                    peer = %from,
                    cache_size = contracts.len(),
                    "PROXIMITY_CACHE: Responding to cache state request"
                );

                Some(ProximityCacheMessage::CacheStateResponse { contracts })
            }

            ProximityCacheMessage::CacheStateResponse { contracts } => {
                let count = contracts.len();
                self.neighbor_caches
                    .insert(from, contracts.into_iter().collect());

                info!(
                    peer = %from,
                    contracts = count,
                    "PROXIMITY_CACHE: Received full cache state from neighbor"
                );

                None
            }
        }
    }

    /// Get the list of neighbors who have a specific contract cached.
    ///
    /// Used by UPDATE operations to find additional targets beyond explicit subscribers.
    pub fn neighbors_with_contract(&self, contract_key: &ContractKey) -> Vec<SocketAddr> {
        let contract_id = contract_key.id();

        let mut neighbors: Vec<SocketAddr> = self
            .neighbor_caches
            .iter()
            .filter(|entry| entry.value().contains(contract_id))
            .map(|entry| *entry.key())
            .collect();

        // Sort for deterministic iteration order (DashMap iteration is non-deterministic)
        neighbors.sort();

        if !neighbors.is_empty() {
            debug!(
                contract = %contract_key,
                neighbor_count = neighbors.len(),
                "PROXIMITY_CACHE: Found neighbors with contract"
            );
        }

        neighbors
    }

    /// Handle peer disconnection by removing their cache state.
    pub fn on_peer_disconnected(&self, addr: &SocketAddr) {
        if let Some((_, removed_cache)) = self.neighbor_caches.remove(addr) {
            debug!(
                peer = %addr,
                cached_contracts = removed_cache.len(),
                "PROXIMITY_CACHE: Removed disconnected peer from neighbor cache"
            );
        }
    }

    /// Called when a new ring connection is established.
    ///
    /// Returns a message to send to the peer to exchange cache state,
    /// enabling UPDATE forwarding to nearby seeders.
    pub fn on_ring_connection_established(
        &self,
        peer_addr: SocketAddr,
    ) -> Option<ProximityCacheMessage> {
        debug!(
            peer = %peer_addr,
            "PROXIMITY_CACHE: New ring connection, requesting cache state"
        );
        Some(ProximityCacheMessage::CacheStateRequest)
    }

    /// Generate a cache state request for a newly connected peer.
    #[allow(dead_code)]
    pub fn request_cache_state() -> ProximityCacheMessage {
        ProximityCacheMessage::CacheStateRequest
    }

    /// Check if we have a contract cached locally.
    #[allow(dead_code)]
    pub fn is_cached_locally(&self, contract_key: &ContractKey) -> bool {
        self.my_cache.contains(contract_key.id())
    }

    /// Get the number of contracts we're caching locally.
    #[allow(dead_code)]
    pub fn local_cache_size(&self) -> usize {
        self.my_cache.len()
    }

    /// Get the number of neighbors we're tracking.
    #[allow(dead_code)]
    pub fn neighbor_count(&self) -> usize {
        self.neighbor_caches.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use freenet_stdlib::prelude::{CodeHash, ContractInstanceId};

    fn test_contract_key() -> ContractKey {
        ContractKey::from_id_and_code(ContractInstanceId::new([1u8; 32]), CodeHash::new([2u8; 32]))
    }

    fn test_contract_key_2() -> ContractKey {
        ContractKey::from_id_and_code(ContractInstanceId::new([3u8; 32]), CodeHash::new([4u8; 32]))
    }

    #[test]
    fn test_cache_announcement_on_new_contract() {
        let manager = ProximityCacheManager::new();
        let key = test_contract_key();

        // First cache should return announcement
        let announcement = manager.on_contract_cached(&key);
        assert!(announcement.is_some());

        if let Some(ProximityCacheMessage::CacheAnnounce {
            added,
            removed,
            is_response,
        }) = announcement
        {
            assert_eq!(added.len(), 1);
            assert!(removed.is_empty());
            assert_eq!(added[0], *key.id());
            assert!(
                !is_response,
                "Initial announcement should not be a response"
            );
        } else {
            panic!("Expected CacheAnnounce");
        }

        // Second cache of same contract should return None
        assert!(manager.on_contract_cached(&key).is_none());
    }

    #[test]
    fn test_cache_eviction() {
        let manager = ProximityCacheManager::new();
        let key = test_contract_key();

        // Cache the contract
        manager.on_contract_cached(&key);

        // Eviction should return announcement
        let announcement = manager.on_contract_evicted(&key);
        assert!(announcement.is_some());

        if let Some(ProximityCacheMessage::CacheAnnounce {
            added,
            removed,
            is_response,
        }) = announcement
        {
            assert!(added.is_empty());
            assert_eq!(removed.len(), 1);
            assert_eq!(removed[0], *key.id());
            assert!(
                !is_response,
                "Eviction announcement should not be a response"
            );
        } else {
            panic!("Expected CacheAnnounce");
        }

        // Second eviction should return None
        assert!(manager.on_contract_evicted(&key).is_none());
    }

    #[test]
    fn test_neighbor_cache_tracking() {
        let manager = ProximityCacheManager::new();
        let key = test_contract_key();
        let neighbor: SocketAddr = "127.0.0.1:8000".parse().unwrap();

        // Initially no neighbors with contract
        assert!(manager.neighbors_with_contract(&key).is_empty());

        // Receive announcement from neighbor (initial, not a response)
        let msg = ProximityCacheMessage::CacheAnnounce {
            added: vec![*key.id()],
            removed: vec![],
            is_response: false,
        };
        manager.handle_message(neighbor, msg);

        // Now neighbor should be found
        let neighbors = manager.neighbors_with_contract(&key);
        assert_eq!(neighbors.len(), 1);
        assert_eq!(neighbors[0], neighbor);
    }

    #[test]
    fn test_cache_state_request_response() {
        let manager = ProximityCacheManager::new();
        let key1 = test_contract_key();
        let key2 = test_contract_key_2();
        let neighbor: SocketAddr = "127.0.0.1:8000".parse().unwrap();

        // Cache some contracts locally
        manager.on_contract_cached(&key1);
        manager.on_contract_cached(&key2);

        // Handle cache state request
        let response = manager.handle_message(neighbor, ProximityCacheMessage::CacheStateRequest);
        assert!(response.is_some());

        if let Some(ProximityCacheMessage::CacheStateResponse { contracts }) = response {
            assert_eq!(contracts.len(), 2);
            assert!(contracts.contains(key1.id()));
            assert!(contracts.contains(key2.id()));
        } else {
            panic!("Expected CacheStateResponse");
        }
    }

    #[test]
    fn test_peer_disconnection() {
        let manager = ProximityCacheManager::new();
        let key = test_contract_key();
        let neighbor: SocketAddr = "127.0.0.1:8000".parse().unwrap();

        // Add neighbor's cache info
        let msg = ProximityCacheMessage::CacheAnnounce {
            added: vec![*key.id()],
            removed: vec![],
            is_response: false,
        };
        manager.handle_message(neighbor, msg);

        assert_eq!(manager.neighbor_count(), 1);

        // Disconnect neighbor
        manager.on_peer_disconnected(&neighbor);

        assert_eq!(manager.neighbor_count(), 0);
        assert!(manager.neighbors_with_contract(&key).is_empty());
    }

    fn test_contract_key_3() -> ContractKey {
        ContractKey::from_id_and_code(ContractInstanceId::new([5u8; 32]), CodeHash::new([6u8; 32]))
    }

    // === Bidirectional Announcement Tests ===

    #[test]
    fn test_bidirectional_announcement_for_overlapping_contracts() {
        // Scenario: Node A has contract X cached. Node B announces it also has X.
        // Expected: Node A should respond with its own announcement so B knows A has X too.
        let manager_a = ProximityCacheManager::new();
        let key = test_contract_key();
        let node_b: SocketAddr = "127.0.0.1:8001".parse().unwrap();

        // Node A caches contract X
        manager_a.on_contract_cached(&key);

        // Node B announces it has contract X (initial announcement, not a response)
        let announcement_from_b = ProximityCacheMessage::CacheAnnounce {
            added: vec![*key.id()],
            removed: vec![],
            is_response: false,
        };

        // Node A should respond with a reciprocal announcement
        let response = manager_a.handle_message(node_b, announcement_from_b);
        assert!(
            response.is_some(),
            "Expected reciprocal announcement for overlapping contract"
        );

        if let Some(ProximityCacheMessage::CacheAnnounce {
            added,
            removed,
            is_response,
        }) = response
        {
            assert_eq!(
                added.len(),
                1,
                "Should announce the one overlapping contract"
            );
            assert_eq!(added[0], *key.id());
            assert!(removed.is_empty());
            assert!(
                is_response,
                "Reciprocal announcement should be marked as response"
            );
        } else {
            panic!("Expected CacheAnnounce response");
        }
    }

    #[test]
    fn test_no_response_for_non_overlapping_contracts() {
        // Scenario: Node A has contract X. Node B announces contract Y (which A doesn't have).
        // Expected: No response needed since there's no overlap.
        let manager_a = ProximityCacheManager::new();
        let key_x = test_contract_key();
        let key_y = test_contract_key_2();
        let node_b: SocketAddr = "127.0.0.1:8001".parse().unwrap();

        // Node A only has contract X
        manager_a.on_contract_cached(&key_x);

        // Node B announces contract Y (which A doesn't have)
        let announcement = ProximityCacheMessage::CacheAnnounce {
            added: vec![*key_y.id()],
            removed: vec![],
            is_response: false,
        };

        let response = manager_a.handle_message(node_b, announcement);
        assert!(
            response.is_none(),
            "No response expected when contracts don't overlap"
        );
    }

    #[test]
    fn test_partial_overlap_only_announces_overlapping() {
        // Scenario: Node A has contracts X and Y. Node B announces X and Z.
        // Expected: Node A responds with only X (the overlap), not Y.
        let manager_a = ProximityCacheManager::new();
        let key_x = test_contract_key();
        let key_y = test_contract_key_2();
        let key_z = test_contract_key_3();
        let node_b: SocketAddr = "127.0.0.1:8001".parse().unwrap();

        // Node A has X and Y
        manager_a.on_contract_cached(&key_x);
        manager_a.on_contract_cached(&key_y);

        // Node B announces X and Z (overlap is only X)
        let announcement = ProximityCacheMessage::CacheAnnounce {
            added: vec![*key_x.id(), *key_z.id()],
            removed: vec![],
            is_response: false,
        };

        let response = manager_a.handle_message(node_b, announcement);
        assert!(response.is_some(), "Expected response for partial overlap");

        if let Some(ProximityCacheMessage::CacheAnnounce {
            added,
            removed,
            is_response,
        }) = response
        {
            assert_eq!(
                added.len(),
                1,
                "Should only announce the overlapping contract X"
            );
            assert_eq!(added[0], *key_x.id());
            assert!(removed.is_empty());
            assert!(is_response, "Response should be marked as response");
        } else {
            panic!("Expected CacheAnnounce response");
        }
    }

    // === Ping-Pong Termination Tests ===

    #[test]
    fn test_ping_pong_terminates_after_one_round() {
        // This is the critical test for preventing infinite announcement loops.
        //
        // Scenario:
        // 1. Node A and B both have contract X
        // 2. A announces X to B (is_response=false)
        // 3. B responds with X (is_response=true, automatically set by handle_message)
        // 4. A receives B's response - should NOT respond because is_response=true
        //
        // The is_response flag explicitly prevents ping-pong by marking responses.

        let manager_a = ProximityCacheManager::new();
        let manager_b = ProximityCacheManager::new();
        let key = test_contract_key();
        let addr_a: SocketAddr = "127.0.0.1:8000".parse().unwrap();
        let addr_b: SocketAddr = "127.0.0.1:8001".parse().unwrap();

        // Both nodes cache contract X
        manager_a.on_contract_cached(&key);
        manager_b.on_contract_cached(&key);

        // Step 1: A announces to B (initial announcement, not a response)
        let a_announcement = ProximityCacheMessage::CacheAnnounce {
            added: vec![*key.id()],
            removed: vec![],
            is_response: false,
        };

        // Step 2: B receives A's announcement, generates reciprocal (with is_response=true)
        let b_response = manager_b.handle_message(addr_a, a_announcement);
        assert!(
            b_response.is_some(),
            "B should respond with reciprocal announcement"
        );

        // Verify B's response has is_response=true
        if let Some(ProximityCacheMessage::CacheAnnounce { is_response, .. }) = &b_response {
            assert!(is_response, "B's response should have is_response=true");
        }

        // Step 3: A receives B's response
        let a_second_response = manager_a.handle_message(addr_b, b_response.unwrap());

        // Step 4: A should NOT respond again - the loop terminates due to is_response=true
        assert!(
            a_second_response.is_none(),
            "Ping-pong must terminate: A should not respond to B's reciprocal announcement"
        );
    }

    #[test]
    fn test_ping_pong_with_multiple_contracts() {
        // More complex scenario with multiple overlapping contracts.
        // Ensures ping-pong termination works when multiple contracts are involved.

        let manager_a = ProximityCacheManager::new();
        let manager_b = ProximityCacheManager::new();
        let key_x = test_contract_key();
        let key_y = test_contract_key_2();
        let addr_a: SocketAddr = "127.0.0.1:8000".parse().unwrap();
        let addr_b: SocketAddr = "127.0.0.1:8001".parse().unwrap();

        // Both have X and Y
        manager_a.on_contract_cached(&key_x);
        manager_a.on_contract_cached(&key_y);
        manager_b.on_contract_cached(&key_x);
        manager_b.on_contract_cached(&key_y);

        // A announces both (initial, not a response)
        let a_announcement = ProximityCacheMessage::CacheAnnounce {
            added: vec![*key_x.id(), *key_y.id()],
            removed: vec![],
            is_response: false,
        };

        // B responds with both (reciprocal, is_response=true set automatically)
        let b_response = manager_b.handle_message(addr_a, a_announcement);
        assert!(b_response.is_some());

        if let Some(ProximityCacheMessage::CacheAnnounce {
            ref added,
            is_response,
            ..
        }) = b_response
        {
            assert_eq!(
                added.len(),
                2,
                "B should announce both overlapping contracts"
            );
            assert!(is_response, "B's response should have is_response=true");
        }

        // A receives B's response - should NOT respond because is_response=true
        let a_second = manager_a.handle_message(addr_b, b_response.unwrap());
        assert!(
            a_second.is_none(),
            "Ping-pong must terminate with multiple contracts"
        );
    }

    #[test]
    fn test_new_contract_after_initial_exchange_triggers_response() {
        // Ensures that after initial exchange, a genuinely NEW contract still triggers response.
        //
        // Scenario:
        // 1. A and B exchange announcements for contract X (ping-pong completes)
        // 2. Later, B caches contract Y which A also has
        // 3. B announces Y to A
        // 4. A should respond because Y is NEW (not previously known from B)

        let manager_a = ProximityCacheManager::new();
        let key_x = test_contract_key();
        let key_y = test_contract_key_2();
        let addr_b: SocketAddr = "127.0.0.1:8001".parse().unwrap();

        // A has both X and Y
        manager_a.on_contract_cached(&key_x);
        manager_a.on_contract_cached(&key_y);

        // Initial exchange: B announces X (initial announcement)
        let b_announces_x = ProximityCacheMessage::CacheAnnounce {
            added: vec![*key_x.id()],
            removed: vec![],
            is_response: false,
        };
        let response_to_x = manager_a.handle_message(addr_b, b_announces_x);
        assert!(
            response_to_x.is_some(),
            "A should respond to initial X announcement"
        );

        // Later: B announces Y (genuinely new, initial announcement)
        let b_announces_y = ProximityCacheMessage::CacheAnnounce {
            added: vec![*key_y.id()],
            removed: vec![],
            is_response: false,
        };
        let response_to_y = manager_a.handle_message(addr_b, b_announces_y);
        assert!(
            response_to_y.is_some(),
            "A should respond to NEW contract Y even after X exchange completed"
        );

        if let Some(ProximityCacheMessage::CacheAnnounce { added, .. }) = response_to_y {
            assert_eq!(added.len(), 1);
            assert_eq!(added[0], *key_y.id(), "Response should be for contract Y");
        }
    }

    #[test]
    fn test_re_announcing_known_contract_no_response() {
        // Edge case: B announces contract X that A already knows B has.
        // This could happen if B's announcement is retransmitted or duplicated.
        // A should not respond since X is not new information.

        let manager_a = ProximityCacheManager::new();
        let key = test_contract_key();
        let addr_b: SocketAddr = "127.0.0.1:8001".parse().unwrap();

        // A has contract X
        manager_a.on_contract_cached(&key);

        // First announcement from B (initial, not a response)
        let announcement = ProximityCacheMessage::CacheAnnounce {
            added: vec![*key.id()],
            removed: vec![],
            is_response: false,
        };
        let first_response = manager_a.handle_message(addr_b, announcement.clone());
        assert!(
            first_response.is_some(),
            "First announcement should get response"
        );

        // Duplicate/retransmitted announcement from B (still not a response)
        let second_response = manager_a.handle_message(addr_b, announcement);
        assert!(
            second_response.is_none(),
            "Duplicate announcement should not trigger response (already known)"
        );
    }

    #[test]
    fn test_bidirectional_awareness_enables_update_forwarding() {
        // Integration-style test verifying the end goal: both nodes can find each other
        // via neighbors_with_contract after the bidirectional exchange.

        let manager_a = ProximityCacheManager::new();
        let manager_b = ProximityCacheManager::new();
        let key = test_contract_key();
        let addr_a: SocketAddr = "127.0.0.1:8000".parse().unwrap();
        let addr_b: SocketAddr = "127.0.0.1:8001".parse().unwrap();

        // Both cache the contract
        manager_a.on_contract_cached(&key);
        manager_b.on_contract_cached(&key);

        // Before any exchange: neither knows about the other
        assert!(
            manager_a.neighbors_with_contract(&key).is_empty(),
            "A shouldn't know about B yet"
        );
        assert!(
            manager_b.neighbors_with_contract(&key).is_empty(),
            "B shouldn't know about A yet"
        );

        // A announces to B (initial announcement, not a response)
        let a_announcement = ProximityCacheMessage::CacheAnnounce {
            added: vec![*key.id()],
            removed: vec![],
            is_response: false,
        };
        let b_response = manager_b.handle_message(addr_a, a_announcement);

        // Now B knows about A
        let b_neighbors = manager_b.neighbors_with_contract(&key);
        assert_eq!(b_neighbors.len(), 1);
        assert_eq!(b_neighbors[0], addr_a);

        // A still doesn't know about B
        assert!(
            manager_a.neighbors_with_contract(&key).is_empty(),
            "A still shouldn't know about B"
        );

        // B's response goes to A
        assert!(b_response.is_some());
        let _ = manager_a.handle_message(addr_b, b_response.unwrap());

        // Now A also knows about B - bidirectional awareness achieved!
        let a_neighbors = manager_a.neighbors_with_contract(&key);
        assert_eq!(a_neighbors.len(), 1);
        assert_eq!(a_neighbors[0], addr_b);

        // Both can now forward updates to each other
    }
}
