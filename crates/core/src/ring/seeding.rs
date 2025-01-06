use std::collections::BTreeSet;
use dashmap::{DashMap, mapref::one::Ref as DmRef};
use crate::node::PeerId;
use super::{Location, PeerKeyLocation, Score};
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
    pub fn seed_contract(&self, key: ContractKey, own_location: Location) -> (Option<ContractKey>, Vec<PeerKeyLocation>) {
        let seed_score = self.calculate_seed_score(&key, own_location);
        let mut old_subscribers = vec![];
        let mut contract_to_drop = None;

        if self.seeding_contract.len() <= Self::MAX_SEEDING_CONTRACTS {
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
    ) -> Option<DmRef<ContractKey, Vec<PeerKeyLocation>>> {
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
}
