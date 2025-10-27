use super::{Location, PeerKeyLocation, Score};
use dashmap::{mapref::one::Ref as DmRef, DashMap};
use freenet_stdlib::prelude::ContractKey;

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
    pub fn add_subscriber(
        &self,
        contract: &ContractKey,
        subscriber: PeerKeyLocation,
    ) -> Result<(), ()> {
        let mut subs = self
            .subscribers
            .entry(*contract)
            .or_insert(Vec::with_capacity(Self::TOTAL_MAX_SUBSCRIPTIONS));
        if subs.len() >= Self::MAX_SUBSCRIBERS {
            return Err(());
        }
        if let Err(next_idx) = subs.value_mut().binary_search(&subscriber) {
            let subs = subs.value_mut();
            if subs.len() == Self::MAX_SUBSCRIBERS {
                return Err(());
            } else {
                subs.insert(next_idx, subscriber);
            }
        }
        Ok(())
    }

    pub fn subscribers_of(
        &self,
        contract: &ContractKey,
    ) -> Option<DmRef<'_, ContractKey, Vec<PeerKeyLocation>>> {
        self.subscribers.get(contract)
    }

    pub fn prune_subscriber(&self, loc: Location) {
        self.subscribers.alter_all(|_, mut subs| {
            if let Some(pos) = subs.iter().position(|l| l.location == Some(loc)) {
                subs.swap_remove(pos);
            }
            subs
        });
    }

    /// Remove a subscriber by peer ID from a specific contract
    pub fn remove_subscriber_by_peer(&self, contract: &ContractKey, peer: &crate::node::PeerId) {
        if let Some(mut subs) = self.subscribers.get_mut(contract) {
            if let Some(pos) = subs.iter().position(|l| &l.peer == peer) {
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

        let peer_loc1 = PeerKeyLocation {
            peer: peer1.clone(),
            location: Some(Location::try_from(0.1).unwrap()),
        };
        let peer_loc2 = PeerKeyLocation {
            peer: peer2.clone(),
            location: Some(Location::try_from(0.2).unwrap()),
        };
        let peer_loc3 = PeerKeyLocation {
            peer: peer3.clone(),
            location: Some(Location::try_from(0.3).unwrap()),
        };

        // Add subscribers
        assert!(seeding_manager
            .add_subscriber(&contract_key, peer_loc1.clone())
            .is_ok());
        assert!(seeding_manager
            .add_subscriber(&contract_key, peer_loc2.clone())
            .is_ok());
        assert!(seeding_manager
            .add_subscriber(&contract_key, peer_loc3.clone())
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
            assert!(!subs.iter().any(|p| p.peer == peer2));
            assert!(subs.iter().any(|p| p.peer == peer1));
            assert!(subs.iter().any(|p| p.peer == peer3));
        }

        // Remove peer1
        seeding_manager.remove_subscriber_by_peer(&contract_key, &peer1);

        // Verify peer1 was removed
        {
            let subs = seeding_manager.subscribers_of(&contract_key).unwrap();
            assert_eq!(subs.len(), 1);
            assert!(!subs.iter().any(|p| p.peer == peer1));
            assert!(subs.iter().any(|p| p.peer == peer3));
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
}
