use super::{Location, PeerKeyLocation, Score};
use dashmap::{mapref::one::Ref as DmRef, DashMap};
use freenet_stdlib::prelude::ContractKey;
use std::fmt;

/// Unique identifier for a local client connection
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct ClientId(u64);

impl ClientId {
    pub fn new(id: u64) -> Self {
        ClientId(id)
    }
}

impl fmt::Display for ClientId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Client({})", self.0)
    }
}

/// Represents a subscriber to a contract, either remote or local
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Subscriber {
    /// A remote peer subscribing via network
    Remote(PeerKeyLocation),
    /// A local client subscribing on this node
    Local(ClientId),
}

impl Subscriber {
    /// Check if this is a remote subscriber with the given peer
    pub fn is_remote_peer(&self, peer: &crate::node::PeerId) -> bool {
        match self {
            Subscriber::Remote(loc) => &loc.peer == peer,
            Subscriber::Local(_) => false,
        }
    }

    /// Get the location if this is a remote subscriber
    pub fn location(&self) -> Option<Location> {
        match self {
            Subscriber::Remote(loc) => loc.location,
            Subscriber::Local(_) => None,
        }
    }
}

pub(crate) struct SeedingManager {
    /// The container for subscriber is a vec instead of something like a hashset
    /// that would allow for blind inserts of duplicate peers subscribing because
    /// of data locality, since we are likely to end up iterating over the whole sequence
    /// of subscribers more often than inserting, and anyways is a relatively short sequence
    /// then is more optimal to just use a vector for it's compact memory layout.
    subscribers: DashMap<ContractKey, Vec<Subscriber>>,
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
        let mut old_remote_subscribers = vec![];
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
                if let Some((_, subscribers_of_contract)) =
                    self.subscribers.remove(&dropped_contract)
                {
                    // Extract only remote subscribers to return
                    for sub in subscribers_of_contract {
                        if let Subscriber::Remote(peer_loc) = sub {
                            old_remote_subscribers.push(peer_loc);
                        }
                    }
                }
                contract_to_drop = Some(dropped_contract);
            }
        }

        self.seeding_contract.insert(key, seed_score);
        (contract_to_drop, old_remote_subscribers)
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
        self.add_subscriber_internal(contract, Subscriber::Remote(subscriber))
    }

    /// Add a local client subscriber
    pub fn add_local_subscriber(
        &self,
        contract: &ContractKey,
        client_id: ClientId,
    ) -> Result<(), ()> {
        self.add_subscriber_internal(contract, Subscriber::Local(client_id))
    }

    fn add_subscriber_internal(
        &self,
        contract: &ContractKey,
        subscriber: Subscriber,
    ) -> Result<(), ()> {
        let mut subs = self
            .subscribers
            .entry(*contract)
            .or_insert(Vec::with_capacity(Self::TOTAL_MAX_SUBSCRIPTIONS));
        if subs.len() >= Self::MAX_SUBSCRIBERS {
            return Err(());
        }

        // Check if subscriber already exists
        let exists = subs.value().iter().any(|s| s == &subscriber);
        if !exists {
            if subs.len() == Self::MAX_SUBSCRIBERS {
                return Err(());
            } else {
                subs.value_mut().push(subscriber);
            }
        }
        Ok(())
    }

    pub fn subscribers_of(
        &self,
        contract: &ContractKey,
    ) -> Option<DmRef<'_, ContractKey, Vec<Subscriber>>> {
        self.subscribers.get(contract)
    }

    /// Get only remote subscribers for backward compatibility
    pub fn remote_subscribers_of(&self, contract: &ContractKey) -> Vec<PeerKeyLocation> {
        self.subscribers
            .get(contract)
            .map(|subs| {
                subs.value()
                    .iter()
                    .filter_map(|s| match s {
                        Subscriber::Remote(loc) => Some(loc.clone()),
                        Subscriber::Local(_) => None,
                    })
                    .collect()
            })
            .unwrap_or_default()
    }

    pub fn prune_subscriber(&self, loc: Location) {
        self.subscribers.alter_all(|_, mut subs| {
            if let Some(pos) = subs.iter().position(|s| s.location() == Some(loc)) {
                subs.swap_remove(pos);
            }
            subs
        });
    }

    /// Get all subscriptions across all contracts (returns only remote subscribers for compatibility)
    pub fn all_subscriptions(&self) -> Vec<(ContractKey, Vec<PeerKeyLocation>)> {
        self.subscribers
            .iter()
            .map(|entry| {
                let remote_subs: Vec<PeerKeyLocation> = entry
                    .value()
                    .iter()
                    .filter_map(|s| match s {
                        Subscriber::Remote(loc) => Some(loc.clone()),
                        Subscriber::Local(_) => None,
                    })
                    .collect();
                (*entry.key(), remote_subs)
            })
            .collect()
    }

    /// Get all subscribers (both local and remote) for a contract
    pub fn all_subscribers(&self) -> Vec<(ContractKey, Vec<Subscriber>)> {
        self.subscribers
            .iter()
            .map(|entry| (*entry.key(), entry.value().clone()))
            .collect()
    }
}
