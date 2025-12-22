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
            ProximityCacheMessage::CacheAnnounce { added, removed } => {
                // Update existing entry if we have one
                if let Some(mut entry) = self.neighbor_caches.get_mut(&from) {
                    entry.extend(added.iter().copied());
                    for id in &removed {
                        entry.remove(id);
                    }
                } else if !added.is_empty() {
                    // Only create new entry if there are contracts to add
                    // (avoid creating empty entries for remove-only announcements)
                    self.neighbor_caches
                        .insert(from, added.into_iter().collect());
                }

                let neighbor_contracts = self
                    .neighbor_caches
                    .get(&from)
                    .map(|c| c.len())
                    .unwrap_or(0);

                info!(
                    peer = %from,
                    total_contracts = neighbor_contracts,
                    "PROXIMITY_CACHE: Updated neighbor cache state"
                );

                None
            }

            ProximityCacheMessage::CacheStateRequest => {
                let contracts: Vec<ContractInstanceId> =
                    self.my_cache.iter().map(|r| *r.key()).collect();

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

        let neighbors: Vec<SocketAddr> = self
            .neighbor_caches
            .iter()
            .filter(|entry| entry.value().contains(contract_id))
            .map(|entry| *entry.key())
            .collect();

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

        if let Some(ProximityCacheMessage::CacheAnnounce { added, removed }) = announcement {
            assert_eq!(added.len(), 1);
            assert!(removed.is_empty());
            assert_eq!(added[0], *key.id());
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

        if let Some(ProximityCacheMessage::CacheAnnounce { added, removed }) = announcement {
            assert!(added.is_empty());
            assert_eq!(removed.len(), 1);
            assert_eq!(removed[0], *key.id());
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

        // Receive announcement from neighbor
        let msg = ProximityCacheMessage::CacheAnnounce {
            added: vec![*key.id()],
            removed: vec![],
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
        };
        manager.handle_message(neighbor, msg);

        assert_eq!(manager.neighbor_count(), 1);

        // Disconnect neighbor
        manager.on_peer_disconnected(&neighbor);

        assert_eq!(manager.neighbor_count(), 0);
        assert!(manager.neighbors_with_contract(&key).is_empty());
    }
}
