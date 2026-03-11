//! Neighbor hosting manager for tracking which neighbors host which contracts.
//!
//! This module enables UPDATE forwarding to nearby hosts who may not be explicitly
//! subscribed. When a peer hosts a contract (via PUT or GET), it announces this to
//! its neighbors. Neighbors track this information and include known hosts in
//! UPDATE broadcast targets.
//!
//! # Design Note: Two Independent Mechanisms
//!
//! Freenet uses two complementary mechanisms for UPDATE propagation:
//!
//! 1. **Subscription Tree** (existing): Propagates updates to peers with downstream
//!    interest (users/peers subscribed through them). Tree structure rooted at
//!    contract location, extending toward clients. Tracked via `HostingManager.subscribers`.
//!
//! 2. **Neighbor Hosting** (this module): Propagates updates to nearby hosts who
//!    have the contract but may not be explicitly subscribed. Mesh structure among
//!    peers near the contract's location. Tracked via `NeighborHostingManager.neighbor_contracts`.
//!
//! These mechanisms are kept independent because they have different lifecycles
//! (subscriptions are explicit; hosting follows cache eviction) and different data
//! structures. They are combined at the broadcast targeting point in
//! `OpManager::get_broadcast_targets_update()`, where HashSet naturally deduplicates
//! any overlap.

use std::{collections::HashSet, sync::Arc};

use dashmap::{DashMap, DashSet};
use freenet_stdlib::prelude::{ContractInstanceId, ContractKey};
use tracing::{debug, info, trace};

use crate::message::NeighborHostingMessage;
use crate::transport::TransportPublicKey;

/// Result from handling a neighbor hosting message.
///
/// Contains an optional response message to send back, plus any contracts
/// discovered to overlap with the neighbor's hosted contracts (for proactive state sync).
pub struct NeighborHostingResult {
    /// Optional response message to send back to the neighbor.
    pub response: Option<NeighborHostingMessage>,
    /// Contracts that both we and the neighbor are hosting.
    /// Used to trigger proactive state sync via `BroadcastStateChange`.
    pub overlapping_contracts: Vec<ContractInstanceId>,
}

impl NeighborHostingResult {
    /// Shorthand for a response-only result with no overlapping contracts.
    fn response_only(response: NeighborHostingMessage) -> Self {
        Self {
            response: Some(response),
            overlapping_contracts: vec![],
        }
    }

    /// Shorthand for a result with no response and no overlapping contracts.
    fn empty() -> Self {
        Self {
            response: None,
            overlapping_contracts: vec![],
        }
    }
}

/// Manages neighbor hosting tracking for UPDATE forwarding.
///
/// Tracks:
/// - Which contracts this node is hosting locally
/// - Which contracts each connected neighbor is hosting
///
/// This information is used to forward UPDATEs to hosts who have a contract
/// but may not be explicitly subscribed to it.
pub struct NeighborHostingManager {
    /// Contracts we are hosting locally.
    my_contracts: Arc<DashSet<ContractInstanceId>>,

    /// What we know about our neighbors' hosted contracts.
    /// Maps neighbor public key to the set of contracts they're hosting.
    /// Keyed by TransportPublicKey (stable identity) rather than SocketAddr (mutable
    /// due to NAT/reconnection), so entries survive address changes.
    neighbor_contracts: DashMap<TransportPublicKey, HashSet<ContractInstanceId>>,
}

impl Default for NeighborHostingManager {
    fn default() -> Self {
        Self::new()
    }
}

impl NeighborHostingManager {
    /// Create a new neighbor hosting manager.
    pub fn new() -> Self {
        Self {
            my_contracts: Arc::new(DashSet::new()),
            neighbor_contracts: DashMap::new(),
        }
    }

    /// Called when we start hosting a new contract (via PUT or successful GET).
    ///
    /// Returns a `HostingAnnounce` message to broadcast to neighbors if this
    /// is a newly hosted contract, or `None` if we already had it.
    pub fn on_contract_hosted(&self, contract_key: &ContractKey) -> Option<NeighborHostingMessage> {
        let contract_id = *contract_key.id();

        if self.my_contracts.insert(contract_id) {
            info!(
                contract = %contract_key,
                "NEIGHBOR_HOSTING: Added contract to locally hosted"
            );

            Some(NeighborHostingMessage::HostingAnnounce {
                added: vec![contract_id],
                removed: vec![],
                is_response: false,
            })
        } else {
            trace!(
                contract = %contract_key,
                "NEIGHBOR_HOSTING: Contract already hosted locally"
            );
            None
        }
    }

    /// Called when we stop hosting a contract (eviction).
    ///
    /// Returns a `HostingAnnounce` message to broadcast to neighbors if the
    /// contract was hosted, or `None` if it wasn't.
    #[allow(dead_code)]
    pub fn on_contract_unhosted(
        &self,
        contract_key: &ContractKey,
    ) -> Option<NeighborHostingMessage> {
        let contract_id = *contract_key.id();

        if self.my_contracts.remove(&contract_id).is_some() {
            debug!(
                contract = %contract_key,
                "NEIGHBOR_HOSTING: Removed contract from locally hosted"
            );

            Some(NeighborHostingMessage::HostingAnnounce {
                added: vec![],
                removed: vec![contract_id],
                is_response: false,
            })
        } else {
            None
        }
    }

    /// Process an incoming neighbor hosting message from a neighbor.
    ///
    /// Returns a [`NeighborHostingResult`] containing an optional response message
    /// and any overlapping contracts discovered (for proactive state sync).
    pub fn handle_message(
        &self,
        from: &TransportPublicKey,
        message: NeighborHostingMessage,
    ) -> NeighborHostingResult {
        match message {
            NeighborHostingMessage::HostingAnnounce {
                added,
                removed,
                is_response,
            } => {
                // Get what we previously knew about this peer BEFORE updating.
                // This is needed to detect genuinely NEW contracts vs duplicates.
                let previously_known: HashSet<ContractInstanceId> = self
                    .neighbor_contracts
                    .get(from)
                    .map(
                        |entry: dashmap::mapref::one::Ref<
                            '_,
                            TransportPublicKey,
                            HashSet<ContractInstanceId>,
                        >| entry.value().clone(),
                    )
                    .unwrap_or_default();

                // Update neighbor's cache state
                if let Some(mut entry) = self.neighbor_contracts.get_mut(from) {
                    entry.extend(added.iter().copied());
                    for id in &removed {
                        entry.remove(id);
                    }
                } else if !added.is_empty() {
                    self.neighbor_contracts
                        .insert(from.clone(), added.iter().copied().collect());
                }
                crate::config::GlobalTestMetrics::record_neighbor_hosting_update();

                let neighbor_contracts: usize = self
                    .neighbor_contracts
                    .get(from)
                    .map(
                        |entry: dashmap::mapref::one::Ref<
                            '_,
                            TransportPublicKey,
                            HashSet<ContractInstanceId>,
                        >| entry.value().len(),
                    )
                    .unwrap_or(0);

                info!(
                    peer = %from,
                    total_contracts = neighbor_contracts,
                    is_response = is_response,
                    "NEIGHBOR_HOSTING: Updated neighbor hosting state"
                );

                // Don't respond to responses - this prevents ping-pong.
                // The is_response flag indicates the sender was already responding
                // to our announcement, so they already know we have these contracts.
                //
                // Returning empty overlapping_contracts here means only the initial
                // announcer's side triggers proactive state sync. This is intentional:
                // if the responding peer has newer state, the CRDT merge at the
                // broadcast recipient produces CurrentWon → emits its own
                // BroadcastStateChange back — so stale state self-corrects within
                // one round-trip.
                if is_response {
                    return NeighborHostingResult::empty();
                }

                // Find contracts that are:
                // 1. NEW from this peer (not previously known from them)
                // 2. Also in our local cache
                // We respond so they can include us in their UPDATE targets.
                let overlapping: Vec<ContractInstanceId> = added
                    .iter()
                    .filter(|id| !previously_known.contains(id)) // Only NEW contracts
                    .filter(|id| self.my_contracts.contains(*id)) // That we also have
                    .copied()
                    .collect();

                let response = if !overlapping.is_empty() {
                    debug!(
                        peer = %from,
                        overlapping_count = overlapping.len(),
                        "NEIGHBOR_HOSTING: Responding with reciprocal announcement for shared contracts"
                    );
                    Some(NeighborHostingMessage::HostingAnnounce {
                        added: overlapping.clone(),
                        removed: vec![],
                        is_response: true, // Mark as response to prevent ping-pong
                    })
                } else {
                    None
                };

                NeighborHostingResult {
                    response,
                    overlapping_contracts: overlapping,
                }
            }

            NeighborHostingMessage::CacheStateRequest => {
                let mut contracts: Vec<ContractInstanceId> =
                    self.my_contracts.iter().map(|r| *r.key()).collect();
                // Sort for deterministic message order (DashSet iteration is non-deterministic)
                // ContractInstanceId doesn't impl Ord, so sort by string representation
                contracts.sort_by_key(|a| a.to_string());

                debug!(
                    peer = %from,
                    hosted_count = contracts.len(),
                    "NEIGHBOR_HOSTING: Responding to hosting state request"
                );

                NeighborHostingResult::response_only(NeighborHostingMessage::CacheStateResponse {
                    contracts,
                })
            }

            NeighborHostingMessage::CacheStateResponse { contracts } => {
                let count = contracts.len();

                // Find contracts that overlap with our local cache BEFORE storing.
                // We need to announce these back so the peer knows we also have them.
                let overlapping: Vec<ContractInstanceId> = contracts
                    .iter()
                    .filter(|id| self.my_contracts.contains(*id))
                    .copied()
                    .collect();

                self.neighbor_contracts
                    .insert(from.clone(), contracts.into_iter().collect());
                crate::config::GlobalTestMetrics::record_neighbor_hosting_update();

                info!(
                    peer = %from,
                    contracts = count,
                    overlapping = overlapping.len(),
                    "NEIGHBOR_HOSTING: Received full hosting state from neighbor"
                );

                // CRITICAL: Send back our overlapping contracts so the peer can include us
                // in UPDATE broadcasts. Without this, peers that reconnect after missing an
                // update broadcast won't receive future updates because neighbors don't know
                // they're still caching the contract.
                //
                // This uses is_response=true to prevent ping-pong loops - the peer receiving
                // this announcement won't respond since they already know we requested their state.
                let response = if !overlapping.is_empty() {
                    debug!(
                        peer = %from,
                        overlapping_count = overlapping.len(),
                        "NEIGHBOR_HOSTING: Announcing our overlapping contracts to peer"
                    );
                    Some(NeighborHostingMessage::HostingAnnounce {
                        added: overlapping.clone(),
                        removed: vec![],
                        is_response: true, // Prevent ping-pong
                    })
                } else {
                    None
                };

                NeighborHostingResult {
                    response,
                    overlapping_contracts: overlapping,
                }
            }
        }
    }

    /// Get the list of neighbors who are hosting a specific contract.
    ///
    /// Returns TransportPublicKey (stable identity) rather than SocketAddr,
    /// so callers can resolve to current addresses via ConnectionManager.
    /// Used by UPDATE operations to find additional targets beyond explicit subscribers.
    pub fn neighbors_with_contract(&self, contract_key: &ContractKey) -> Vec<TransportPublicKey> {
        let contract_id = contract_key.id();

        let mut neighbors: Vec<TransportPublicKey> = self
            .neighbor_contracts
            .iter()
            .filter(
                |entry: &dashmap::mapref::multiple::RefMulti<
                    '_,
                    TransportPublicKey,
                    HashSet<ContractInstanceId>,
                >| entry.value().contains(contract_id),
            )
            .map(
                |entry: dashmap::mapref::multiple::RefMulti<
                    '_,
                    TransportPublicKey,
                    HashSet<ContractInstanceId>,
                >| entry.key().clone(),
            )
            .collect();

        // Sort for deterministic iteration order (DashMap iteration is non-deterministic)
        neighbors.sort();

        if !neighbors.is_empty() {
            debug!(
                contract = %contract_key,
                neighbor_count = neighbors.len(),
                "NEIGHBOR_HOSTING: Found neighbors with contract"
            );
        }

        neighbors
    }

    /// Handle peer disconnection by removing their hosting state.
    pub fn on_peer_disconnected(&self, pub_key: &TransportPublicKey) {
        if let Some((_, removed_cache)) = self.neighbor_contracts.remove(pub_key) {
            let count = removed_cache.len();
            debug!(
                peer = %pub_key,
                hosted_contracts = count,
                "NEIGHBOR_HOSTING: Removed disconnected peer from neighbor hosting"
            );
        }
    }

    /// Called when a new ring connection is established.
    ///
    /// Returns a message to send to the peer to exchange hosting state,
    /// enabling UPDATE forwarding to nearby hosts.
    pub fn on_ring_connection_established(
        &self,
        pub_key: &TransportPublicKey,
    ) -> Option<NeighborHostingMessage> {
        debug!(
            peer = %pub_key,
            "NEIGHBOR_HOSTING: New ring connection, requesting hosting state"
        );
        Some(NeighborHostingMessage::CacheStateRequest)
    }

    /// Initialize my_contracts from contracts loaded from disk.
    /// Must be called after loading the hosting cache and before ring connections establish.
    pub fn initialize_from_hosting_cache(
        &self,
        contract_ids: impl Iterator<Item = ContractInstanceId>,
    ) {
        let mut count = 0;
        for id in contract_ids {
            self.my_contracts.insert(id);
            count += 1;
        }
        if count > 0 {
            info!(
                count,
                "NEIGHBOR_HOSTING: Initialized local hosting from persisted contracts"
            );
        }
    }

    /// Check if we are hosting a contract locally.
    #[allow(dead_code)]
    pub fn is_hosted_locally(&self, contract_key: &ContractKey) -> bool {
        self.my_contracts.contains(contract_key.id())
    }

    /// Get the number of contracts we're hosting locally.
    #[allow(dead_code)]
    pub fn local_hosted_count(&self) -> usize {
        self.my_contracts.len()
    }

    /// Get the number of neighbors we're tracking.
    #[allow(dead_code)]
    pub fn neighbor_count(&self) -> usize {
        self.neighbor_contracts.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::transport::TransportKeypair;
    use freenet_stdlib::prelude::{CodeHash, ContractInstanceId};

    fn test_contract_key() -> ContractKey {
        ContractKey::from_id_and_code(ContractInstanceId::new([1u8; 32]), CodeHash::new([2u8; 32]))
    }

    fn test_contract_key_2() -> ContractKey {
        ContractKey::from_id_and_code(ContractInstanceId::new([3u8; 32]), CodeHash::new([4u8; 32]))
    }

    fn make_pub_key(_seed: u8) -> TransportPublicKey {
        TransportKeypair::new().public().clone()
    }

    #[test]
    fn test_cache_announcement_on_new_contract() {
        let manager = NeighborHostingManager::new();
        let key = test_contract_key();

        // First cache should return announcement
        let announcement = manager.on_contract_hosted(&key);
        assert!(announcement.is_some());

        if let Some(NeighborHostingMessage::HostingAnnounce {
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
            panic!("Expected HostingAnnounce");
        }

        // Second cache of same contract should return None
        assert!(manager.on_contract_hosted(&key).is_none());
    }

    #[test]
    fn test_cache_eviction() {
        let manager = NeighborHostingManager::new();
        let key = test_contract_key();

        // Cache the contract
        manager.on_contract_hosted(&key);

        // Eviction should return announcement
        let announcement = manager.on_contract_unhosted(&key);
        assert!(announcement.is_some());

        if let Some(NeighborHostingMessage::HostingAnnounce {
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
            panic!("Expected HostingAnnounce");
        }

        // Second eviction should return None
        assert!(manager.on_contract_unhosted(&key).is_none());
    }

    #[test]
    fn test_neighbor_hosting_tracking() {
        let manager = NeighborHostingManager::new();
        let key = test_contract_key();
        let neighbor = make_pub_key(1);

        // Initially no neighbors with contract
        assert!(manager.neighbors_with_contract(&key).is_empty());

        // Receive announcement from neighbor (initial, not a response)
        let msg = NeighborHostingMessage::HostingAnnounce {
            added: vec![*key.id()],
            removed: vec![],
            is_response: false,
        };
        manager.handle_message(&neighbor, msg);

        // Now neighbor should be found
        let neighbors = manager.neighbors_with_contract(&key);
        assert_eq!(neighbors.len(), 1);
        assert_eq!(neighbors[0], neighbor);
    }

    #[test]
    fn test_cache_state_request_response() {
        let manager = NeighborHostingManager::new();
        let key1 = test_contract_key();
        let key2 = test_contract_key_2();
        let neighbor = make_pub_key(1);

        // Cache some contracts locally
        manager.on_contract_hosted(&key1);
        manager.on_contract_hosted(&key2);

        // Handle cache state request
        let result = manager.handle_message(&neighbor, NeighborHostingMessage::CacheStateRequest);
        assert!(result.response.is_some());

        if let Some(NeighborHostingMessage::CacheStateResponse { contracts }) = result.response {
            assert_eq!(contracts.len(), 2);
            assert!(contracts.contains(key1.id()));
            assert!(contracts.contains(key2.id()));
        } else {
            panic!("Expected CacheStateResponse");
        }
    }

    #[test]
    fn test_peer_disconnection() {
        let manager = NeighborHostingManager::new();
        let key = test_contract_key();
        let neighbor = make_pub_key(1);

        // Add neighbor's cache info
        let msg = NeighborHostingMessage::HostingAnnounce {
            added: vec![*key.id()],
            removed: vec![],
            is_response: false,
        };
        manager.handle_message(&neighbor, msg);

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
        let manager_a = NeighborHostingManager::new();
        let key = test_contract_key();
        let node_b = make_pub_key(2);

        // Node A caches contract X
        manager_a.on_contract_hosted(&key);

        // Node B announces it has contract X (initial announcement, not a response)
        let announcement_from_b = NeighborHostingMessage::HostingAnnounce {
            added: vec![*key.id()],
            removed: vec![],
            is_response: false,
        };

        // Node A should respond with a reciprocal announcement
        let result = manager_a.handle_message(&node_b, announcement_from_b);
        assert!(
            result.response.is_some(),
            "Expected reciprocal announcement for overlapping contract"
        );
        assert_eq!(result.overlapping_contracts.len(), 1);
        assert_eq!(result.overlapping_contracts[0], *key.id());

        if let Some(NeighborHostingMessage::HostingAnnounce {
            added,
            removed,
            is_response,
        }) = result.response
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
            panic!("Expected HostingAnnounce response");
        }
    }

    #[test]
    fn test_no_response_for_non_overlapping_contracts() {
        // Scenario: Node A has contract X. Node B announces contract Y (which A doesn't have).
        // Expected: No response needed since there's no overlap.
        let manager_a = NeighborHostingManager::new();
        let key_x = test_contract_key();
        let key_y = test_contract_key_2();
        let node_b = make_pub_key(2);

        // Node A only has contract X
        manager_a.on_contract_hosted(&key_x);

        // Node B announces contract Y (which A doesn't have)
        let announcement = NeighborHostingMessage::HostingAnnounce {
            added: vec![*key_y.id()],
            removed: vec![],
            is_response: false,
        };

        let result = manager_a.handle_message(&node_b, announcement);
        assert!(
            result.response.is_none(),
            "No response expected when contracts don't overlap"
        );
        assert!(
            result.overlapping_contracts.is_empty(),
            "No overlapping contracts when contracts don't match"
        );
    }

    #[test]
    fn test_partial_overlap_only_announces_overlapping() {
        // Scenario: Node A has contracts X and Y. Node B announces X and Z.
        // Expected: Node A responds with only X (the overlap), not Y.
        let manager_a = NeighborHostingManager::new();
        let key_x = test_contract_key();
        let key_y = test_contract_key_2();
        let key_z = test_contract_key_3();
        let node_b = make_pub_key(2);

        // Node A has X and Y
        manager_a.on_contract_hosted(&key_x);
        manager_a.on_contract_hosted(&key_y);

        // Node B announces X and Z (overlap is only X)
        let announcement = NeighborHostingMessage::HostingAnnounce {
            added: vec![*key_x.id(), *key_z.id()],
            removed: vec![],
            is_response: false,
        };

        let result = manager_a.handle_message(&node_b, announcement);
        assert!(
            result.response.is_some(),
            "Expected response for partial overlap"
        );
        assert_eq!(
            result.overlapping_contracts.len(),
            1,
            "Only contract X should overlap"
        );
        assert_eq!(result.overlapping_contracts[0], *key_x.id());

        if let Some(NeighborHostingMessage::HostingAnnounce {
            added,
            removed,
            is_response,
        }) = result.response
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
            panic!("Expected HostingAnnounce response");
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

        let manager_a = NeighborHostingManager::new();
        let manager_b = NeighborHostingManager::new();
        let key = test_contract_key();
        let key_a = make_pub_key(1);
        let key_b = make_pub_key(2);

        // Both nodes cache contract X
        manager_a.on_contract_hosted(&key);
        manager_b.on_contract_hosted(&key);

        // Step 1: A announces to B (initial announcement, not a response)
        let a_announcement = NeighborHostingMessage::HostingAnnounce {
            added: vec![*key.id()],
            removed: vec![],
            is_response: false,
        };

        // Step 2: B receives A's announcement, generates reciprocal (with is_response=true)
        let b_result = manager_b.handle_message(&key_a, a_announcement);
        assert!(
            b_result.response.is_some(),
            "B should respond with reciprocal announcement"
        );

        // Verify B's response has is_response=true
        if let Some(NeighborHostingMessage::HostingAnnounce { is_response, .. }) =
            &b_result.response
        {
            assert!(is_response, "B's response should have is_response=true");
        }

        // Step 3: A receives B's response
        let a_second_result = manager_a.handle_message(&key_b, b_result.response.unwrap());

        // Step 4: A should NOT respond again - the loop terminates due to is_response=true
        assert!(
            a_second_result.response.is_none(),
            "Ping-pong must terminate: A should not respond to B's reciprocal announcement"
        );
        assert!(
            a_second_result.overlapping_contracts.is_empty(),
            "is_response=true must not trigger proactive state sync"
        );
    }

    #[test]
    fn test_ping_pong_with_multiple_contracts() {
        // More complex scenario with multiple overlapping contracts.
        // Ensures ping-pong termination works when multiple contracts are involved.

        let manager_a = NeighborHostingManager::new();
        let manager_b = NeighborHostingManager::new();
        let key_x = test_contract_key();
        let key_y = test_contract_key_2();
        let key_a = make_pub_key(1);
        let key_b = make_pub_key(2);

        // Both have X and Y
        manager_a.on_contract_hosted(&key_x);
        manager_a.on_contract_hosted(&key_y);
        manager_b.on_contract_hosted(&key_x);
        manager_b.on_contract_hosted(&key_y);

        // A announces both (initial, not a response)
        let a_announcement = NeighborHostingMessage::HostingAnnounce {
            added: vec![*key_x.id(), *key_y.id()],
            removed: vec![],
            is_response: false,
        };

        // B responds with both (reciprocal, is_response=true set automatically)
        let b_result = manager_b.handle_message(&key_a, a_announcement);
        assert!(b_result.response.is_some());

        if let Some(NeighborHostingMessage::HostingAnnounce {
            ref added,
            is_response,
            ..
        }) = b_result.response
        {
            assert_eq!(
                added.len(),
                2,
                "B should announce both overlapping contracts"
            );
            assert!(is_response, "B's response should have is_response=true");
        }

        // A receives B's response - should NOT respond because is_response=true
        let a_second = manager_a.handle_message(&key_b, b_result.response.unwrap());
        assert!(
            a_second.response.is_none(),
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

        let manager_a = NeighborHostingManager::new();
        let key_x = test_contract_key();
        let key_y = test_contract_key_2();
        let key_b = make_pub_key(2);

        // A has both X and Y
        manager_a.on_contract_hosted(&key_x);
        manager_a.on_contract_hosted(&key_y);

        // Initial exchange: B announces X (initial announcement)
        let b_announces_x = NeighborHostingMessage::HostingAnnounce {
            added: vec![*key_x.id()],
            removed: vec![],
            is_response: false,
        };
        let result_x = manager_a.handle_message(&key_b, b_announces_x);
        assert!(
            result_x.response.is_some(),
            "A should respond to initial X announcement"
        );

        // Later: B announces Y (genuinely new, initial announcement)
        let b_announces_y = NeighborHostingMessage::HostingAnnounce {
            added: vec![*key_y.id()],
            removed: vec![],
            is_response: false,
        };
        let result_y = manager_a.handle_message(&key_b, b_announces_y);
        assert!(
            result_y.response.is_some(),
            "A should respond to NEW contract Y even after X exchange completed"
        );

        if let Some(NeighborHostingMessage::HostingAnnounce { added, .. }) = result_y.response {
            assert_eq!(added.len(), 1);
            assert_eq!(added[0], *key_y.id(), "Response should be for contract Y");
        }
    }

    #[test]
    fn test_re_announcing_known_contract_no_response() {
        // Edge case: B announces contract X that A already knows B has.
        // This could happen if B's announcement is retransmitted or duplicated.
        // A should not respond since X is not new information.

        let manager_a = NeighborHostingManager::new();
        let key = test_contract_key();
        let key_b = make_pub_key(2);

        // A has contract X
        manager_a.on_contract_hosted(&key);

        // First announcement from B (initial, not a response)
        let announcement = NeighborHostingMessage::HostingAnnounce {
            added: vec![*key.id()],
            removed: vec![],
            is_response: false,
        };
        let first_result = manager_a.handle_message(&key_b, announcement.clone());
        assert!(
            first_result.response.is_some(),
            "First announcement should get response"
        );

        // Duplicate/retransmitted announcement from B (still not a response)
        let second_result = manager_a.handle_message(&key_b, announcement);
        assert!(
            second_result.response.is_none(),
            "Duplicate announcement should not trigger response (already known)"
        );
    }

    #[test]
    fn test_bidirectional_awareness_enables_update_forwarding() {
        // Integration-style test verifying the end goal: both nodes can find each other
        // via neighbors_with_contract after the bidirectional exchange.

        let manager_a = NeighborHostingManager::new();
        let manager_b = NeighborHostingManager::new();
        let key = test_contract_key();
        let key_a = make_pub_key(1);
        let key_b = make_pub_key(2);

        // Both cache the contract
        manager_a.on_contract_hosted(&key);
        manager_b.on_contract_hosted(&key);

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
        let a_announcement = NeighborHostingMessage::HostingAnnounce {
            added: vec![*key.id()],
            removed: vec![],
            is_response: false,
        };
        let b_result = manager_b.handle_message(&key_a, a_announcement);

        // Now B knows about A
        let b_neighbors = manager_b.neighbors_with_contract(&key);
        assert_eq!(b_neighbors.len(), 1);
        assert_eq!(b_neighbors[0], key_a);

        // A still doesn't know about B
        assert!(
            manager_a.neighbors_with_contract(&key).is_empty(),
            "A still shouldn't know about B"
        );

        // B's response goes to A
        assert!(b_result.response.is_some());
        let _ = manager_a.handle_message(&key_b, b_result.response.unwrap());

        // Now A also knows about B - bidirectional awareness achieved!
        let a_neighbors = manager_a.neighbors_with_contract(&key);
        assert_eq!(a_neighbors.len(), 1);
        assert_eq!(a_neighbors[0], key_b);

        // Both can now forward updates to each other
    }

    // === Reconnection Tests ===
    // These tests cover the scenario where a peer disconnects/reconnects and needs
    // to re-establish bidirectional awareness via CacheStateRequest/Response.

    #[test]
    fn test_cache_state_response_triggers_announcement_for_overlapping_contracts() {
        // CRITICAL: This test catches the bug where peers that reconnect after missing
        // an update broadcast don't receive future updates.
        //
        // Scenario:
        // 1. Node A has contract X cached
        // 2. Node B reconnects and has contract X cached
        // 3. A sends CacheStateRequest to B (via on_ring_connection_established)
        // 4. B responds with CacheStateResponse containing X
        // 5. A receives CacheStateResponse - should announce X back to B
        // 6. Now B knows A also has X and can include A in UPDATE broadcasts
        //
        // Without this fix, step 5 returned None and B never learned A has X.

        let manager_a = NeighborHostingManager::new();
        let key = test_contract_key();
        let key_b = make_pub_key(2);

        // A has contract X cached locally
        manager_a.on_contract_hosted(&key);

        // B reconnects and sends CacheStateResponse (as if responding to A's request)
        let b_state_response = NeighborHostingMessage::CacheStateResponse {
            contracts: vec![*key.id()],
        };

        // A handles the response - should return announcement for overlapping contract X
        let a_result = manager_a.handle_message(&key_b, b_state_response);

        assert!(
            a_result.response.is_some(),
            "CRITICAL: A must announce overlapping contracts when receiving CacheStateResponse"
        );
        assert_eq!(a_result.overlapping_contracts.len(), 1);
        assert_eq!(a_result.overlapping_contracts[0], *key.id());

        if let Some(NeighborHostingMessage::HostingAnnounce {
            added,
            removed,
            is_response,
        }) = a_result.response
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
                "Should be marked as response to prevent ping-pong"
            );
        } else {
            panic!("Expected HostingAnnounce, got something else");
        }
    }

    #[test]
    fn test_cache_state_response_no_announcement_when_no_overlap() {
        // When receiving CacheStateResponse with no overlapping contracts,
        // no announcement should be sent.

        let manager_a = NeighborHostingManager::new();
        let key_x = test_contract_key();
        let key_y = test_contract_key_2();
        let key_b = make_pub_key(2);

        // A only has contract X
        manager_a.on_contract_hosted(&key_x);

        // B has contract Y (no overlap with A)
        let b_state_response = NeighborHostingMessage::CacheStateResponse {
            contracts: vec![*key_y.id()],
        };

        let result = manager_a.handle_message(&key_b, b_state_response);
        assert!(
            result.response.is_none(),
            "No announcement needed when no contracts overlap"
        );
        assert!(
            result.overlapping_contracts.is_empty(),
            "No overlapping contracts when caches don't intersect"
        );

        // But B's contracts should still be tracked
        let b_neighbors = manager_a.neighbors_with_contract(&key_y);
        assert_eq!(b_neighbors.len(), 1);
        assert_eq!(b_neighbors[0], key_b);
    }

    #[test]
    fn test_reconnection_bidirectional_awareness_via_state_request_response() {
        // Full reconnection flow test:
        // 1. Both A and B have contract X
        // 2. B disconnects and reconnects
        // 3. A initiates cache state exchange with B
        // 4. After exchange, both should know about each other for UPDATE forwarding

        let manager_a = NeighborHostingManager::new();
        let manager_b = NeighborHostingManager::new();
        let key = test_contract_key();
        let key_a = make_pub_key(1);
        let key_b = make_pub_key(2);

        // Both have contract X cached
        manager_a.on_contract_hosted(&key);
        manager_b.on_contract_hosted(&key);

        // Initially neither knows about the other
        assert!(manager_a.neighbors_with_contract(&key).is_empty());
        assert!(manager_b.neighbors_with_contract(&key).is_empty());

        // Step 1: A establishes ring connection with B, generates CacheStateRequest
        let a_request = manager_a.on_ring_connection_established(&key_b);
        assert!(matches!(
            a_request,
            Some(NeighborHostingMessage::CacheStateRequest)
        ));

        // Step 2: B receives request, responds with its cache state
        let b_result = manager_b.handle_message(&key_a, a_request.unwrap());
        assert!(matches!(
            b_result.response,
            Some(NeighborHostingMessage::CacheStateResponse { .. })
        ));

        // Step 3: A receives B's state response
        // CRITICAL: A should now announce its overlapping contracts back to B
        let a_result = manager_a.handle_message(&key_b, b_result.response.unwrap());
        assert!(
            a_result.response.is_some(),
            "A must announce overlapping contracts after receiving B's state"
        );

        // A now knows B has contract X
        let a_neighbors = manager_a.neighbors_with_contract(&key);
        assert_eq!(a_neighbors.len(), 1, "A should know B has contract X");
        assert_eq!(a_neighbors[0], key_b);

        // Step 4: B receives A's announcement
        let b_final = manager_b.handle_message(&key_a, a_result.response.unwrap());
        // B should not respond (is_response=true prevents ping-pong)
        assert!(
            b_final.response.is_none(),
            "B should not respond to A's announcement (is_response=true)"
        );

        // B now knows A has contract X
        let b_neighbors = manager_b.neighbors_with_contract(&key);
        assert_eq!(b_neighbors.len(), 1, "B should know A has contract X");
        assert_eq!(b_neighbors[0], key_a);

        // SUCCESS: Both peers now have bidirectional awareness and can forward updates
    }

    #[test]
    fn test_initialize_from_hosting_cache() {
        let manager = NeighborHostingManager::new();
        let key1 = test_contract_key();
        let key2 = test_contract_key_2();

        // Initially empty
        assert_eq!(manager.local_hosted_count(), 0);

        // Initialize from hosting cache
        manager.initialize_from_hosting_cache(vec![*key1.id(), *key2.id()].into_iter());

        // Should now report both contracts
        assert_eq!(manager.local_hosted_count(), 2);
        assert!(manager.is_hosted_locally(&key1));
        assert!(manager.is_hosted_locally(&key2));

        // CacheStateRequest should return both
        let neighbor = make_pub_key(1);
        let result = manager.handle_message(&neighbor, NeighborHostingMessage::CacheStateRequest);
        if let Some(NeighborHostingMessage::CacheStateResponse { contracts }) = result.response {
            assert_eq!(contracts.len(), 2);
        } else {
            panic!("Expected CacheStateResponse");
        }

        // Caching an already-initialized contract should return None (no duplicate announcement)
        assert!(manager.on_contract_hosted(&key1).is_none());
    }
}
