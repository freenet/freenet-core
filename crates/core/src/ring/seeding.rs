use super::{Location, PeerKeyLocation, Score};
use crate::transport::ObservedAddr;
use dashmap::{mapref::one::Ref as DmRef, DashMap};
use freenet_stdlib::prelude::ContractKey;
use tracing::{info, warn};

pub(crate) struct SeedingManager {
    /// The container for subscriber is a vec instead of something like a hashset
    /// that would allow for blind inserts of duplicate peers subscribing because
    /// of data locality, since we are likely to end up iterating over the whole sequence
    /// of subscribers more often than inserting, and anyways is a relatively short sequence
    /// then is more optimal to just use a vector for it's compact memory layout.
    subscribers: DashMap<ContractKey, Vec<PeerKeyLocation>>,
    /// Contracts this peer is seeding.
    seeding_contract: DashMap<ContractKey, Score>,
}

impl SeedingManager {
    /// Max number of subscribers for a contract.
    const MAX_SUBSCRIBERS: usize = 10;

    /// All subscribers, including the upstream subscriber.
    const TOTAL_MAX_SUBSCRIPTIONS: usize = Self::MAX_SUBSCRIBERS + 1;

    /// Max number of seeding contracts.
    const MAX_SEEDING_CONTRACTS: usize = 100;

    /// Min number of seeding contracts.
    const MIN_SEEDING_CONTRACTS: usize = Self::MAX_SEEDING_CONTRACTS / 4;

    pub fn new() -> Self {
        Self {
            subscribers: DashMap::new(),
            seeding_contract: DashMap::new(),
        }
    }

    /// Return if a contract is within appropiate seeding distance.
    pub fn should_seed(&self, key: &ContractKey, own_location: Location) -> bool {
        const CACHING_DISTANCE: f64 = 0.05;
        let caching_distance = super::Distance::new(CACHING_DISTANCE);
        if self.seeding_contract.len() < Self::MIN_SEEDING_CONTRACTS {
            return true;
        }
        let key_loc = Location::from(key);
        if self.seeding_contract.len() < Self::MAX_SEEDING_CONTRACTS {
            return own_location.distance(key_loc) <= caching_distance;
        }

        let contract_score = self.calculate_seed_score(key, own_location);
        let r = self
            .seeding_contract
            .iter()
            .min_by_key(|v| *v.value())
            .unwrap();
        let min_score = *r.value();
        contract_score > min_score
    }

    /// Add a new subscription for this peer.
    pub fn seed_contract(
        &self,
        key: ContractKey,
        own_location: Location,
    ) -> (Option<ContractKey>, Vec<PeerKeyLocation>) {
        let seed_score = self.calculate_seed_score(&key, own_location);
        let mut old_subscribers = vec![];
        let mut contract_to_drop = None;

        // FIXME: reproduce this condition in tests
        if self.seeding_contract.len() >= Self::MAX_SEEDING_CONTRACTS {
            if let Some(dropped_contract) = self
                .seeding_contract
                .iter()
                .min_by_key(|v| *v.value())
                .map(|entry| *entry.key())
            {
                self.seeding_contract.remove(&dropped_contract);
                if let Some((_, mut subscribers_of_contract)) =
                    self.subscribers.remove(&dropped_contract)
                {
                    std::mem::swap(&mut subscribers_of_contract, &mut old_subscribers);
                }
                contract_to_drop = Some(dropped_contract);
            }
        }

        self.seeding_contract.insert(key, seed_score);
        (contract_to_drop, old_subscribers)
    }

    fn calculate_seed_score(&self, key: &ContractKey, own_location: Location) -> Score {
        let key_loc = Location::from(key);
        let distance = key_loc.distance(own_location);
        let score = 0.5 - distance.as_f64();
        Score(score)
    }

    /// Whether this node already is seeding to this contract or not.
    #[inline]
    pub fn is_seeding_contract(&self, key: &ContractKey) -> bool {
        self.seeding_contract.contains_key(key)
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
                    "Removed peer {} from subscriber list for contract {}",
                    peer,
                    contract
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
    fn test_should_seed_always_true_below_min() {
        let seeding_manager = SeedingManager::new();
        let own_location = Location::new(0.5);

        // With no contracts seeded, should always return true regardless of distance
        assert!(seeding_manager.should_seed(&make_contract_key(1), own_location));

        // Add contracts up to MIN_SEEDING_CONTRACTS - 1 (which is 24)
        for i in 0..24 {
            seeding_manager
                .seeding_contract
                .insert(make_contract_key(i), Score(0.1));
        }

        // Should still be true because we're below MIN (25)
        assert_eq!(seeding_manager.seeding_contract.len(), 24);
        assert!(seeding_manager.should_seed(&make_contract_key(200), own_location));
    }

    #[test]
    fn test_should_seed_distance_check_between_min_and_max() {
        let seeding_manager = SeedingManager::new();

        // Seed MIN_SEEDING_CONTRACTS (25) contracts
        for i in 0..25 {
            seeding_manager
                .seeding_contract
                .insert(make_contract_key(i), Score(0.1));
        }
        assert_eq!(seeding_manager.seeding_contract.len(), 25);

        // Now distance check applies - we need a contract location close to own_location
        // Location::from(contract_key) gives a deterministic location based on key hash
        let test_key = make_contract_key(100);
        let key_location = Location::from(&test_key);

        // Own location very close to the key location should return true
        let close_location = Location::new(key_location.as_f64());
        assert!(seeding_manager.should_seed(&test_key, close_location));

        // Own location far from the key location should return false
        // Adding 0.5 to wrap around (locations are on a ring 0.0-1.0)
        let far_away = (key_location.as_f64() + 0.5).rem_euclid(1.0);
        let far_location = Location::new(far_away);
        assert!(!seeding_manager.should_seed(&test_key, far_location));
    }

    #[test]
    fn test_seed_contract_no_drop_below_max() {
        let seeding_manager = SeedingManager::new();
        let own_location = Location::new(0.5);

        // Add less than MAX contracts
        for i in 0..50 {
            seeding_manager
                .seeding_contract
                .insert(make_contract_key(i), Score(0.1));
        }

        let new_key = make_contract_key(200);
        let (dropped_contract, old_subscribers) =
            seeding_manager.seed_contract(new_key, own_location);

        // No contract should be dropped when below max
        assert!(dropped_contract.is_none());
        assert!(old_subscribers.is_empty());

        // New contract should be added
        assert!(seeding_manager.is_seeding_contract(&new_key));
        assert_eq!(seeding_manager.seeding_contract.len(), 51);
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
        let seeding_manager = SeedingManager::new();
        let key = make_contract_key(1);

        assert!(!seeding_manager.is_seeding_contract(&key));

        seeding_manager.seeding_contract.insert(key, Score(0.1));

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
