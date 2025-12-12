use super::seeding_cache::{AccessType, SeedingCache};
use super::{Location, PeerKeyLocation};
use crate::transport::ObservedAddr;
use crate::util::time_source::InstantTimeSrc;
use dashmap::{mapref::one::Ref as DmRef, DashMap};
use freenet_stdlib::prelude::ContractKey;
use parking_lot::RwLock;
use tracing::{info, warn};

/// Default seeding cache budget: 100MB
/// This can be made configurable via node configuration in the future.
const DEFAULT_SEEDING_BUDGET_BYTES: u64 = 100 * 1024 * 1024;

pub(crate) struct SeedingManager {
    /// The container for subscriber is a vec instead of something like a hashset
    /// that would allow for blind inserts of duplicate peers subscribing because
    /// of data locality, since we are likely to end up iterating over the whole sequence
    /// of subscribers more often than inserting, and anyways is a relatively short sequence
    /// then is more optimal to just use a vector for it's compact memory layout.
    subscribers: DashMap<ContractKey, Vec<PeerKeyLocation>>,
    /// LRU cache of contracts this peer is seeding, with byte-budget awareness.
    seeding_cache: RwLock<SeedingCache<InstantTimeSrc>>,
}

impl SeedingManager {
    /// Max number of subscribers for a contract.
    const MAX_SUBSCRIBERS: usize = 10;

    /// All subscribers, including the upstream subscriber.
    const TOTAL_MAX_SUBSCRIPTIONS: usize = Self::MAX_SUBSCRIBERS + 1;

    pub fn new() -> Self {
        Self {
            subscribers: DashMap::new(),
            seeding_cache: RwLock::new(SeedingCache::new(
                DEFAULT_SEEDING_BUDGET_BYTES,
                InstantTimeSrc::new(),
            )),
        }
    }

    /// Record an access to a contract (GET, PUT, or SUBSCRIBE).
    ///
    /// This adds the contract to the seeding cache if not present, or refreshes
    /// its LRU position if already cached. Returns the list of evicted contracts
    /// that need cleanup (unsubscription, state removal, etc.).
    ///
    /// The `size_bytes` should be the size of the contract state.
    ///
    /// # Eviction handling
    ///
    /// Currently, eviction only removes local subscriber tracking. Full subscription
    /// tree pruning (sending Unsubscribed to upstream peers) requires tracking
    /// upstream->downstream relationships per contract, which is planned for #2164.
    pub fn record_contract_access(
        &self,
        key: ContractKey,
        size_bytes: u64,
        access_type: AccessType,
    ) -> Vec<ContractKey> {
        let evicted = self
            .seeding_cache
            .write()
            .record_access(key, size_bytes, access_type);

        // Clean up subscribers for evicted contracts
        for evicted_key in &evicted {
            self.subscribers.remove(evicted_key);
        }

        evicted
    }

    /// Whether this node is currently caching/seeding this contract.
    #[inline]
    pub fn is_seeding_contract(&self, key: &ContractKey) -> bool {
        self.seeding_cache.read().contains(key)
    }

    /// Remove a contract from the seeding cache (for future use in cleanup paths).
    ///
    /// Returns true if the contract was present and removed.
    #[allow(dead_code)]
    pub fn remove_seeded_contract(&self, key: &ContractKey) -> bool {
        let removed = self.seeding_cache.write().remove(key).is_some();
        if removed {
            self.subscribers.remove(key);
        }
        removed
    }

    /// Will return an error in case the max number of subscribers has been added.
    ///
    /// The `upstream_addr` parameter is the transport-level address from which the subscribe
    /// message was received. This is used instead of the address embedded in `subscriber`
    /// because NAT peers may embed incorrect (e.g., loopback) addresses in their messages.
    /// The transport address is the only reliable way to route back to them.
    pub fn add_subscriber(
        &self,
        contract: &ContractKey,
        subscriber: PeerKeyLocation,
        upstream_addr: Option<ObservedAddr>,
    ) -> Result<(), ()> {
        // Use the transport-level address if available, otherwise fall back to the embedded address
        let subscriber = if let Some(addr) = upstream_addr {
            PeerKeyLocation::new(subscriber.pub_key.clone(), addr.socket_addr())
        } else {
            subscriber
        };
        let mut subs = self
            .subscribers
            .entry(*contract)
            .or_insert(Vec::with_capacity(Self::TOTAL_MAX_SUBSCRIPTIONS));
        let before = subs
            .iter()
            .map(|loc| format!("{:.8}", loc.pub_key))
            .collect::<Vec<_>>();
        info!(
            %contract,
            subscriber = %subscriber.pub_key,
            subscribers_before = ?before,
            current_len = subs.len(),
            "seeding_manager: attempting to add subscriber"
        );
        if subs.len() >= Self::MAX_SUBSCRIBERS {
            warn!(
                %contract,
                subscriber = %subscriber.pub_key,
                subscribers_before = ?before,
                "seeding_manager: max subscribers reached"
            );
            return Err(());
        }
        let subs_vec = subs.value_mut();
        match subs_vec.binary_search(&subscriber) {
            Ok(_) => {
                info!(
                    %contract,
                    subscriber = %subscriber.pub_key,
                    subscribers_before = ?before,
                    "seeding_manager: subscriber already registered"
                );
                Ok(())
            }
            Err(next_idx) => {
                if subs_vec.len() == Self::MAX_SUBSCRIBERS {
                    warn!(
                        %contract,
                        subscriber = %subscriber.pub_key,
                        subscribers_before = ?before,
                        "seeding_manager: max subscribers reached during insert"
                    );
                    Err(())
                } else {
                    subs_vec.insert(next_idx, subscriber);
                    let after = subs_vec
                        .iter()
                        .map(|loc| format!("{:.8}", loc.pub_key))
                        .collect::<Vec<_>>();
                    info!(
                        %contract,
                        subscribers_after = ?after,
                        "seeding_manager: subscriber added"
                    );
                    Ok(())
                }
            }
        }
    }

    pub fn subscribers_of(
        &self,
        contract: &ContractKey,
    ) -> Option<DmRef<'_, ContractKey, Vec<PeerKeyLocation>>> {
        self.subscribers.get(contract)
    }

    pub fn prune_subscriber(&self, loc: Location) {
        self.subscribers.alter_all(|contract_key, mut subs| {
            if let Some(pos) = subs.iter().position(|l| l.location() == Some(loc)) {
                let removed = subs[pos].clone();
                tracing::debug!(
                    %contract_key,
                    removed_peer = %removed.pub_key,
                    removed_location = ?removed.location(),
                    "seeding_manager: pruning subscriber due to location match"
                );
                subs.swap_remove(pos);
            }
            subs
        });
    }

    /// Remove a subscriber by peer ID from a specific contract
    pub fn remove_subscriber_by_peer(&self, contract: &ContractKey, peer: &crate::node::PeerId) {
        if let Some(mut subs) = self.subscribers.get_mut(contract) {
            if let Some(pos) = subs
                .iter()
                .position(|l| l.pub_key == peer.pub_key && l.socket_addr() == Some(peer.addr))
            {
                subs.swap_remove(pos);
                tracing::debug!(
                    contract = %contract,
                    peer = %peer,
                    "Removed peer from subscriber list"
                );
            }
        }
    }

    /// Get all subscriptions across all contracts
    pub fn all_subscriptions(&self) -> Vec<(ContractKey, Vec<PeerKeyLocation>)> {
        self.subscribers
            .iter()
            .map(|entry| (*entry.key(), entry.value().clone()))
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::node::PeerId;
    use crate::transport::TransportKeypair;
    use freenet_stdlib::prelude::{ContractInstanceId, ContractKey};
    use std::net::{IpAddr, Ipv4Addr, SocketAddr};

    // Helper to create test PeerIds without expensive key generation
    fn test_peer_id(id: u8) -> PeerId {
        let addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, id)), 1000 + id as u16);
        let pub_key = TransportKeypair::new().public().clone();
        PeerId::new(addr, pub_key)
    }

    #[test]
    fn test_remove_subscriber_by_peer() {
        let seeding_manager = SeedingManager::new();
        let contract_key = ContractKey::from(ContractInstanceId::new([1u8; 32]));

        // Create test peers
        let peer1 = test_peer_id(1);
        let peer2 = test_peer_id(2);
        let peer3 = test_peer_id(3);

        // Location is now computed from address automatically
        let peer_loc1 = PeerKeyLocation::new(peer1.pub_key.clone(), peer1.addr);
        let peer_loc2 = PeerKeyLocation::new(peer2.pub_key.clone(), peer2.addr);
        let peer_loc3 = PeerKeyLocation::new(peer3.pub_key.clone(), peer3.addr);

        // Add subscribers (test setup - no upstream_addr)
        assert!(seeding_manager
            .add_subscriber(&contract_key, peer_loc1.clone(), None)
            .is_ok());
        assert!(seeding_manager
            .add_subscriber(&contract_key, peer_loc2.clone(), None)
            .is_ok());
        assert!(seeding_manager
            .add_subscriber(&contract_key, peer_loc3.clone(), None)
            .is_ok());

        // Verify all subscribers are present
        {
            let subs = seeding_manager.subscribers_of(&contract_key).unwrap();
            assert_eq!(subs.len(), 3);
        }

        // Remove peer2
        seeding_manager.remove_subscriber_by_peer(&contract_key, &peer2);

        // Verify peer2 was removed
        {
            let subs = seeding_manager.subscribers_of(&contract_key).unwrap();
            assert_eq!(subs.len(), 2);
            assert!(!subs
                .iter()
                .any(|p| p.pub_key == peer2.pub_key && p.socket_addr() == Some(peer2.addr)));
            assert!(subs
                .iter()
                .any(|p| p.pub_key == peer1.pub_key && p.socket_addr() == Some(peer1.addr)));
            assert!(subs
                .iter()
                .any(|p| p.pub_key == peer3.pub_key && p.socket_addr() == Some(peer3.addr)));
        }

        // Remove peer1
        seeding_manager.remove_subscriber_by_peer(&contract_key, &peer1);

        // Verify peer1 was removed
        {
            let subs = seeding_manager.subscribers_of(&contract_key).unwrap();
            assert_eq!(subs.len(), 1);
            assert!(!subs
                .iter()
                .any(|p| p.pub_key == peer1.pub_key && p.socket_addr() == Some(peer1.addr)));
            assert!(subs
                .iter()
                .any(|p| p.pub_key == peer3.pub_key && p.socket_addr() == Some(peer3.addr)));
        }

        // Remove non-existent peer (should not error)
        seeding_manager.remove_subscriber_by_peer(&contract_key, &peer2);

        // Verify count unchanged
        {
            let subs = seeding_manager.subscribers_of(&contract_key).unwrap();
            assert_eq!(subs.len(), 1);
        }
    }

    #[test]
    fn test_remove_subscriber_from_nonexistent_contract() {
        let seeding_manager = SeedingManager::new();
        let contract_key = ContractKey::from(ContractInstanceId::new([2u8; 32]));
        let peer = test_peer_id(1);

        // Should not panic when removing from non-existent contract
        seeding_manager.remove_subscriber_by_peer(&contract_key, &peer);
    }

    fn make_contract_key(seed: u8) -> ContractKey {
        ContractKey::from(ContractInstanceId::new([seed; 32]))
    }

    #[test]
    fn test_record_contract_access_adds_to_cache() {
        use super::super::seeding_cache::AccessType;

        let seeding_manager = SeedingManager::new();
        let key = make_contract_key(1);

        // Initially not seeding
        assert!(!seeding_manager.is_seeding_contract(&key));

        // Record access
        let evicted = seeding_manager.record_contract_access(key, 1000, AccessType::Get);

        // Now seeding
        assert!(seeding_manager.is_seeding_contract(&key));
        assert!(evicted.is_empty()); // No eviction needed for small contract
    }

    #[test]
    fn test_record_contract_access_evicts_when_over_budget() {
        use super::super::seeding_cache::AccessType;

        let seeding_manager = SeedingManager::new();

        // Add a contract that takes up most of the budget (100MB default)
        let large_key = make_contract_key(1);
        let evicted = seeding_manager.record_contract_access(
            large_key,
            90 * 1024 * 1024, // 90MB
            AccessType::Get,
        );
        assert!(evicted.is_empty());
        assert!(seeding_manager.is_seeding_contract(&large_key));

        // Add another large contract that should cause eviction
        let another_large_key = make_contract_key(2);
        let evicted = seeding_manager.record_contract_access(
            another_large_key,
            20 * 1024 * 1024, // 20MB - total would be 110MB, over 100MB budget
            AccessType::Put,
        );

        // The first contract should have been evicted
        assert!(!evicted.is_empty());
        assert!(evicted.contains(&large_key));
        assert!(!seeding_manager.is_seeding_contract(&large_key));
        assert!(seeding_manager.is_seeding_contract(&another_large_key));
    }

    #[test]
    fn test_eviction_clears_subscribers() {
        use super::super::seeding_cache::AccessType;

        let seeding_manager = SeedingManager::new();

        // Add a contract and a subscriber
        let key = make_contract_key(1);
        seeding_manager.record_contract_access(key, 90 * 1024 * 1024, AccessType::Get);

        let peer = PeerKeyLocation::new(
            TransportKeypair::new().public().clone(),
            SocketAddr::new(IpAddr::V4(Ipv4Addr::new(10, 0, 0, 1)), 5000),
        );
        seeding_manager.add_subscriber(&key, peer, None).unwrap();
        assert!(seeding_manager.subscribers_of(&key).is_some());

        // Force eviction with another large contract
        let new_key = make_contract_key(2);
        let evicted =
            seeding_manager.record_contract_access(new_key, 20 * 1024 * 1024, AccessType::Get);

        // Original contract should be evicted and subscribers cleared
        assert!(evicted.contains(&key));
        assert!(seeding_manager.subscribers_of(&key).is_none());
    }

    #[test]
    fn test_add_subscriber_rejects_at_max_capacity() {
        let seeding_manager = SeedingManager::new();
        let contract_key = make_contract_key(1);

        // Add MAX_SUBSCRIBERS (10) subscribers
        for i in 0..10 {
            let peer = PeerKeyLocation::new(
                TransportKeypair::new().public().clone(),
                SocketAddr::new(IpAddr::V4(Ipv4Addr::new(10, 0, 0, i + 1)), 5000),
            );
            let result = seeding_manager.add_subscriber(&contract_key, peer, None);
            assert!(result.is_ok(), "Should accept subscriber {}", i);
        }

        // Verify we have 10 subscribers
        assert_eq!(
            seeding_manager.subscribers_of(&contract_key).unwrap().len(),
            10
        );

        // Try to add 11th subscriber - should fail
        let extra_peer = PeerKeyLocation::new(
            TransportKeypair::new().public().clone(),
            SocketAddr::new(IpAddr::V4(Ipv4Addr::new(10, 0, 0, 100)), 5000),
        );
        let result = seeding_manager.add_subscriber(&contract_key, extra_peer, None);
        assert!(result.is_err(), "Should reject subscriber beyond max");

        // Count should still be 10
        assert_eq!(
            seeding_manager.subscribers_of(&contract_key).unwrap().len(),
            10
        );
    }

    #[test]
    fn test_add_subscriber_allows_duplicate() {
        let seeding_manager = SeedingManager::new();
        let contract_key = make_contract_key(1);

        let peer = PeerKeyLocation::new(
            TransportKeypair::new().public().clone(),
            SocketAddr::new(IpAddr::V4(Ipv4Addr::new(10, 0, 0, 1)), 5000),
        );

        // Add same subscriber twice
        assert!(seeding_manager
            .add_subscriber(&contract_key, peer.clone(), None)
            .is_ok());
        assert!(seeding_manager
            .add_subscriber(&contract_key, peer.clone(), None)
            .is_ok());

        // Should only have 1 subscriber (deduplicated)
        assert_eq!(
            seeding_manager.subscribers_of(&contract_key).unwrap().len(),
            1
        );
    }

    #[test]
    fn test_add_subscriber_uses_upstream_addr_when_provided() {
        let seeding_manager = SeedingManager::new();
        let contract_key = make_contract_key(1);

        // Create a peer with one address embedded
        let embedded_addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 5000);
        let peer = PeerKeyLocation::new(TransportKeypair::new().public().clone(), embedded_addr);

        // Provide a different upstream address (as would happen with NAT)
        let observed_addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(203, 0, 113, 50)), 12345);
        let upstream = ObservedAddr::from(observed_addr);

        seeding_manager
            .add_subscriber(&contract_key, peer.clone(), Some(upstream))
            .unwrap();

        // The stored subscriber should have the observed address, not the embedded one
        let subs = seeding_manager.subscribers_of(&contract_key).unwrap();
        assert_eq!(subs.len(), 1);
        let stored = &subs[0];
        assert_eq!(stored.socket_addr(), Some(observed_addr));
        assert_eq!(stored.pub_key, peer.pub_key);
    }

    #[test]
    fn test_prune_subscriber_removes_by_location() {
        let seeding_manager = SeedingManager::new();
        let contract_key = make_contract_key(1);

        // Create peers with specific addresses (location is derived from address)
        let addr1 = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(10, 0, 0, 1)), 5000);
        let addr2 = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(10, 0, 0, 2)), 5001);

        let peer1 = PeerKeyLocation::new(TransportKeypair::new().public().clone(), addr1);
        let peer2 = PeerKeyLocation::new(TransportKeypair::new().public().clone(), addr2);

        seeding_manager
            .add_subscriber(&contract_key, peer1.clone(), None)
            .unwrap();
        seeding_manager
            .add_subscriber(&contract_key, peer2.clone(), None)
            .unwrap();

        assert_eq!(
            seeding_manager.subscribers_of(&contract_key).unwrap().len(),
            2
        );

        // Prune by peer1's location
        if let Some(loc) = peer1.location() {
            seeding_manager.prune_subscriber(loc);
        }

        // Should have removed peer1
        let subs = seeding_manager.subscribers_of(&contract_key).unwrap();
        assert_eq!(subs.len(), 1);
        assert!(!subs.iter().any(|p| p.socket_addr() == Some(addr1)));
        assert!(subs.iter().any(|p| p.socket_addr() == Some(addr2)));
    }

    #[test]
    fn test_all_subscriptions_returns_all() {
        let seeding_manager = SeedingManager::new();

        let key1 = make_contract_key(1);
        let key2 = make_contract_key(2);

        let peer1 = PeerKeyLocation::new(
            TransportKeypair::new().public().clone(),
            SocketAddr::new(IpAddr::V4(Ipv4Addr::new(10, 0, 0, 1)), 5000),
        );
        let peer2 = PeerKeyLocation::new(
            TransportKeypair::new().public().clone(),
            SocketAddr::new(IpAddr::V4(Ipv4Addr::new(10, 0, 0, 2)), 5001),
        );

        seeding_manager
            .add_subscriber(&key1, peer1.clone(), None)
            .unwrap();
        seeding_manager
            .add_subscriber(&key2, peer2.clone(), None)
            .unwrap();

        let all = seeding_manager.all_subscriptions();
        assert_eq!(all.len(), 2);

        // Verify both contracts are present
        let keys: Vec<_> = all.iter().map(|(k, _)| *k).collect();
        assert!(keys.contains(&key1));
        assert!(keys.contains(&key2));
    }

    #[test]
    fn test_is_seeding_contract() {
        use super::super::seeding_cache::AccessType;

        let seeding_manager = SeedingManager::new();
        let key = make_contract_key(1);

        assert!(!seeding_manager.is_seeding_contract(&key));

        seeding_manager.record_contract_access(key, 1000, AccessType::Get);

        assert!(seeding_manager.is_seeding_contract(&key));
    }

    /// Test that validates the broadcast target filtering logic used by
    /// `get_broadcast_targets_update` in update.rs.
    ///
    /// **Architecture Note (Issue #2075):**
    /// After decoupling local from network subscriptions, `get_broadcast_targets_update`
    /// simply filters out the sender from the subscriber list. This test validates
    /// that the seeding_manager correctly stores and retrieves network subscribers,
    /// which is the foundation for UPDATE broadcast targeting.
    #[test]
    fn test_subscribers_for_broadcast_targeting() {
        let seeding_manager = SeedingManager::new();
        let contract_key = ContractKey::from(ContractInstanceId::new([3u8; 32]));

        // Create network peers (not local clients)
        let peer1 = test_peer_id(1);
        let peer2 = test_peer_id(2);
        let peer3 = test_peer_id(3);

        let peer_loc1 = PeerKeyLocation::new(peer1.pub_key.clone(), peer1.addr);
        let peer_loc2 = PeerKeyLocation::new(peer2.pub_key.clone(), peer2.addr);
        let peer_loc3 = PeerKeyLocation::new(peer3.pub_key.clone(), peer3.addr);

        // Register network subscribers
        seeding_manager
            .add_subscriber(&contract_key, peer_loc1.clone(), None)
            .expect("should add peer1");
        seeding_manager
            .add_subscriber(&contract_key, peer_loc2.clone(), None)
            .expect("should add peer2");
        seeding_manager
            .add_subscriber(&contract_key, peer_loc3.clone(), None)
            .expect("should add peer3");

        // Retrieve subscribers (as get_broadcast_targets_update would)
        let subs = seeding_manager.subscribers_of(&contract_key).unwrap();

        // All network peers should be in the list
        assert_eq!(subs.len(), 3, "Should have 3 network subscribers");

        // Simulate filtering out the sender (as get_broadcast_targets_update does)
        // If peer1 is the sender of an UPDATE, it should be filtered out
        let sender_addr = peer1.addr;
        let broadcast_targets: Vec<_> = subs
            .iter()
            .filter(|pk| pk.socket_addr().as_ref() != Some(&sender_addr))
            .cloned()
            .collect();

        // Only peer2 and peer3 should receive the broadcast
        assert_eq!(
            broadcast_targets.len(),
            2,
            "Should exclude sender from broadcast targets"
        );
        assert!(
            broadcast_targets
                .iter()
                .any(|p| p.socket_addr() == Some(peer2.addr)),
            "peer2 should be in broadcast targets"
        );
        assert!(
            broadcast_targets
                .iter()
                .any(|p| p.socket_addr() == Some(peer3.addr)),
            "peer3 should be in broadcast targets"
        );
        assert!(
            !broadcast_targets
                .iter()
                .any(|p| p.socket_addr() == Some(peer1.addr)),
            "sender (peer1) should NOT be in broadcast targets"
        );
    }
}
