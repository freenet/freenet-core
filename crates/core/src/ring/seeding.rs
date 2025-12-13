use super::seeding_cache::{AccessType, SeedingCache};
use super::{Location, PeerKeyLocation};
use crate::node::PeerId;
use crate::transport::ObservedAddr;
use crate::util::time_source::InstantTimeSrc;
use dashmap::DashMap;
use freenet_stdlib::prelude::ContractKey;
use parking_lot::RwLock;
use std::collections::HashSet;
use std::net::SocketAddr;
use tracing::{debug, info, warn};

/// Default seeding cache budget: 100MB
/// This can be made configurable via node configuration in the future.
const DEFAULT_SEEDING_BUDGET_BYTES: u64 = 100 * 1024 * 1024;

/// Role of a peer in a subscription relationship for a specific contract.
///
/// The subscription system forms a tree where updates flow from the contract
/// source (root) down to all interested peers (leaves). Each peer in the tree
/// has relationships with its neighbors:
///
/// ```text
///                    [Source/Provider]
///                          |
///                    [Intermediate]  <-- has Upstream to Source
///                    /           \
///             [PeerA]           [PeerB]  <-- Downstream from Intermediate's perspective
/// ```
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SubscriptionRole {
    /// This peer subscribed through us - they expect updates FROM us.
    /// We are responsible for forwarding updates to them.
    /// When all downstream peers disconnect and there are no client subscriptions,
    /// we should unsubscribe from our upstream.
    Downstream,

    /// We subscribed through this peer - we expect updates FROM them.
    /// This is our source for contract updates.
    /// There should be at most one upstream per contract.
    Upstream,
}

/// A subscription entry tracking both the peer and their role in the subscription tree.
#[derive(Clone, Debug)]
pub struct SubscriptionEntry {
    pub peer: PeerKeyLocation,
    pub role: SubscriptionRole,
}

impl SubscriptionEntry {
    pub fn new(peer: PeerKeyLocation, role: SubscriptionRole) -> Self {
        Self { peer, role }
    }

    /// Check if this entry matches a given peer (by public key and address).
    pub fn matches_peer(&self, peer_id: &PeerId) -> bool {
        self.peer.pub_key == peer_id.pub_key && self.peer.socket_addr() == Some(peer_id.addr)
    }

    /// Check if this entry matches a given location.
    pub fn matches_location(&self, loc: Location) -> bool {
        self.peer.location() == Some(loc)
    }
}

/// Error type for subscription operations.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SubscriptionError {
    /// Maximum number of downstream subscribers reached for this contract.
    MaxSubscribersReached,
}

/// Result of removing a subscriber, indicating whether upstream notification is needed.
#[derive(Debug)]
pub struct RemoveSubscriberResult {
    /// The upstream peer to notify with Unsubscribed message, if pruning is needed.
    /// This is Some when:
    /// - The removed peer was downstream
    /// - No more downstream subscribers remain
    /// - No client subscriptions for this contract
    pub notify_upstream: Option<PeerKeyLocation>,
}

/// Result of pruning a peer from all contracts.
#[derive(Debug)]
pub struct PrunePeerResult {
    /// List of (contract, upstream) pairs where upstream notification is needed.
    pub notifications: Vec<(ContractKey, PeerKeyLocation)>,
}

pub(crate) struct SeedingManager {
    /// Subscriptions per contract with explicit upstream/downstream roles.
    /// This replaces the flat Vec<PeerKeyLocation> to enable proper tree pruning.
    subscriptions: DashMap<ContractKey, Vec<SubscriptionEntry>>,

    /// Contracts where a local client (WebSocket) is actively subscribed.
    /// Prevents upstream unsubscribe while client subscriptions exist, even if
    /// all network downstream peers have disconnected.
    client_subscriptions: DashMap<ContractKey, HashSet<crate::client_events::ClientId>>,

    /// LRU cache of contracts this peer is seeding, with byte-budget awareness.
    seeding_cache: RwLock<SeedingCache<InstantTimeSrc>>,
}

impl SeedingManager {
    /// Max number of downstream subscribers for a contract.
    const MAX_DOWNSTREAM: usize = 10;

    pub fn new() -> Self {
        Self {
            subscriptions: DashMap::new(),
            client_subscriptions: DashMap::new(),
            seeding_cache: RwLock::new(SeedingCache::new(
                DEFAULT_SEEDING_BUDGET_BYTES,
                InstantTimeSrc::new(),
            )),
        }
    }

    // ==================== Subscription Management ====================

    /// Add a downstream subscriber (a peer that wants updates FROM us).
    ///
    /// The `observed_addr` parameter is the transport-level address from which the subscribe
    /// message was received. This is used instead of the address embedded in `subscriber`
    /// because NAT peers may embed incorrect (e.g., loopback) addresses in their messages.
    pub fn add_downstream(
        &self,
        contract: &ContractKey,
        subscriber: PeerKeyLocation,
        observed_addr: Option<ObservedAddr>,
    ) -> Result<(), SubscriptionError> {
        // Use the transport-level address if available
        let subscriber = if let Some(addr) = observed_addr {
            PeerKeyLocation::new(subscriber.pub_key.clone(), addr.socket_addr())
        } else {
            subscriber
        };

        let mut subs = self.subscriptions.entry(*contract).or_default();

        // Count current downstream subscribers
        let downstream_count = subs
            .iter()
            .filter(|e| e.role == SubscriptionRole::Downstream)
            .count();

        if downstream_count >= Self::MAX_DOWNSTREAM {
            warn!(
                %contract,
                subscriber = %subscriber.pub_key,
                "add_downstream: max downstream subscribers reached"
            );
            return Err(SubscriptionError::MaxSubscribersReached);
        }

        // Check for duplicate
        let already_exists = subs.iter().any(|e| {
            e.role == SubscriptionRole::Downstream
                && e.peer.pub_key == subscriber.pub_key
                && e.peer.socket_addr() == subscriber.socket_addr()
        });

        if already_exists {
            info!(
                %contract,
                subscriber = %subscriber.pub_key,
                "add_downstream: subscriber already registered"
            );
            return Ok(());
        }

        let subscriber_addr = subscriber
            .socket_addr()
            .map(|a| a.to_string())
            .unwrap_or_else(|| "unknown".into());

        subs.push(SubscriptionEntry::new(
            subscriber,
            SubscriptionRole::Downstream,
        ));

        info!(
            %contract,
            subscriber = %subscriber_addr,
            downstream_count = downstream_count + 1,
            "add_downstream: registered new downstream subscriber"
        );

        Ok(())
    }

    /// Set the upstream source for a contract (the peer we get updates FROM).
    ///
    /// There can be at most one upstream per contract. If an upstream already exists,
    /// it will be replaced.
    pub fn set_upstream(&self, contract: &ContractKey, upstream: PeerKeyLocation) {
        let mut subs = self.subscriptions.entry(*contract).or_default();

        // Remove any existing upstream
        subs.retain(|e| e.role != SubscriptionRole::Upstream);

        let upstream_addr = upstream
            .socket_addr()
            .map(|a| a.to_string())
            .unwrap_or_else(|| "unknown".into());

        subs.push(SubscriptionEntry::new(upstream, SubscriptionRole::Upstream));

        info!(
            %contract,
            upstream = %upstream_addr,
            "set_upstream: registered upstream source"
        );
    }

    /// Get the upstream peer for a contract (if any).
    pub fn get_upstream(&self, contract: &ContractKey) -> Option<PeerKeyLocation> {
        self.subscriptions.get(contract).and_then(|subs| {
            subs.iter()
                .find(|e| e.role == SubscriptionRole::Upstream)
                .map(|e| e.peer.clone())
        })
    }

    /// Get all downstream subscribers for a contract (for broadcast targeting).
    pub fn get_downstream(&self, contract: &ContractKey) -> Vec<PeerKeyLocation> {
        self.subscriptions
            .get(contract)
            .map(|subs| {
                subs.iter()
                    .filter(|e| e.role == SubscriptionRole::Downstream)
                    .map(|e| e.peer.clone())
                    .collect()
            })
            .unwrap_or_default()
    }

    /// Check if we have any subscription entries for a contract.
    pub fn has_subscriptions(&self, contract: &ContractKey) -> bool {
        self.subscriptions
            .get(contract)
            .map(|subs| !subs.is_empty())
            .unwrap_or(false)
    }

    // ==================== Client Subscription Management ====================

    /// Register a client subscription for a contract (WebSocket client subscribed).
    pub fn add_client_subscription(
        &self,
        contract: &ContractKey,
        client_id: crate::client_events::ClientId,
    ) {
        self.client_subscriptions
            .entry(*contract)
            .or_default()
            .insert(client_id);
        debug!(
            %contract,
            %client_id,
            "add_client_subscription: registered client subscription"
        );
    }

    /// Remove a client subscription.
    /// Returns true if this was the last client subscription for this contract.
    pub fn remove_client_subscription(
        &self,
        contract: &ContractKey,
        client_id: crate::client_events::ClientId,
    ) -> bool {
        let mut no_more_subscriptions = false;

        if let Some(mut clients) = self.client_subscriptions.get_mut(contract) {
            clients.remove(&client_id);
            if clients.is_empty() {
                no_more_subscriptions = true;
            }
        }

        if no_more_subscriptions {
            self.client_subscriptions.remove(contract);
        }

        debug!(
            %contract,
            %client_id,
            no_more_client_subscriptions = no_more_subscriptions,
            "remove_client_subscription: removed client subscription"
        );

        no_more_subscriptions
    }

    /// Check if there are any client subscriptions for a contract.
    pub fn has_client_subscriptions(&self, contract: &ContractKey) -> bool {
        self.client_subscriptions
            .get(contract)
            .map(|clients| !clients.is_empty())
            .unwrap_or(false)
    }

    // ==================== Subscriber Removal & Pruning ====================

    /// Remove a subscriber by peer ID from a specific contract.
    ///
    /// Returns information about whether upstream notification is needed:
    /// - If the removed peer was downstream AND no more downstream remain AND no client subscriptions,
    ///   returns the upstream peer to notify with Unsubscribed.
    pub fn remove_subscriber(
        &self,
        contract: &ContractKey,
        peer: &PeerId,
    ) -> RemoveSubscriberResult {
        let mut notify_upstream = None;

        if let Some(mut subs) = self.subscriptions.get_mut(contract) {
            // Find and remove the peer
            if let Some(pos) = subs.iter().position(|e| e.matches_peer(peer)) {
                let removed = subs.swap_remove(pos);

                debug!(
                    %contract,
                    peer = %peer,
                    role = ?removed.role,
                    "remove_subscriber: removed peer"
                );

                // Only check for pruning if we removed a downstream subscriber
                if removed.role == SubscriptionRole::Downstream {
                    let has_downstream =
                        subs.iter().any(|e| e.role == SubscriptionRole::Downstream);
                    let has_client = self.has_client_subscriptions(contract);

                    if !has_downstream && !has_client {
                        // Find upstream to notify
                        notify_upstream = subs
                            .iter()
                            .find(|e| e.role == SubscriptionRole::Upstream)
                            .map(|e| e.peer.clone());

                        if notify_upstream.is_some() {
                            info!(
                                %contract,
                                "remove_subscriber: no downstream or client subscriptions, will notify upstream"
                            );
                        }

                        // Clean up the entire subscription entry
                        drop(subs);
                        self.subscriptions.remove(contract);
                    }
                }
            }
        }

        RemoveSubscriberResult { notify_upstream }
    }

    /// Prune all subscriptions for a peer that disconnected (by location).
    ///
    /// Returns a list of (contract, upstream) pairs where upstream notification is needed.
    pub fn prune_peer(&self, loc: Location) -> PrunePeerResult {
        let mut notifications = Vec::new();

        // Collect contracts that need modification to avoid holding locks
        let contracts_to_check: Vec<ContractKey> = self
            .subscriptions
            .iter()
            .filter(|entry| entry.value().iter().any(|e| e.matches_location(loc)))
            .map(|entry| *entry.key())
            .collect();

        for contract in contracts_to_check {
            if let Some(mut subs) = self.subscriptions.get_mut(&contract) {
                // Find the entry to remove
                if let Some(pos) = subs.iter().position(|e| e.matches_location(loc)) {
                    let removed = subs.swap_remove(pos);

                    debug!(
                        %contract,
                        removed_location = ?loc,
                        role = ?removed.role,
                        "prune_peer: removed peer by location"
                    );

                    // Check for pruning if we removed a downstream
                    if removed.role == SubscriptionRole::Downstream {
                        let has_downstream =
                            subs.iter().any(|e| e.role == SubscriptionRole::Downstream);
                        let has_client = self.has_client_subscriptions(&contract);

                        if !has_downstream && !has_client {
                            if let Some(upstream) = subs
                                .iter()
                                .find(|e| e.role == SubscriptionRole::Upstream)
                                .map(|e| e.peer.clone())
                            {
                                notifications.push((contract, upstream));
                            }

                            // Clean up
                            drop(subs);
                            self.subscriptions.remove(&contract);
                        }
                    }
                }
            }
        }

        if !notifications.is_empty() {
            info!(
                contracts_to_notify = notifications.len(),
                "prune_peer: will notify upstream for contracts with no remaining interest"
            );
        }

        PrunePeerResult { notifications }
    }

    // ==================== Seeding Cache Integration ====================

    /// Record an access to a contract (GET, PUT, or SUBSCRIBE).
    ///
    /// Returns a list of (evicted_contract, upstream_to_notify) pairs.
    /// The upstream should be sent an Unsubscribed message for each evicted contract.
    pub fn record_contract_access(
        &self,
        key: ContractKey,
        size_bytes: u64,
        access_type: AccessType,
    ) -> Vec<(ContractKey, Option<PeerKeyLocation>)> {
        let evicted = self
            .seeding_cache
            .write()
            .record_access(key, size_bytes, access_type);

        // Clean up subscriptions for evicted contracts and collect upstream notifications
        evicted
            .into_iter()
            .map(|evicted_key| {
                let upstream = self.get_upstream(&evicted_key);
                self.subscriptions.remove(&evicted_key);
                self.client_subscriptions.remove(&evicted_key);

                if upstream.is_some() {
                    info!(
                        contract = %evicted_key,
                        "record_contract_access: contract evicted, will notify upstream"
                    );
                }

                (evicted_key, upstream)
            })
            .collect()
    }

    /// Whether this node is currently caching/seeding this contract.
    #[inline]
    pub fn is_seeding_contract(&self, key: &ContractKey) -> bool {
        self.seeding_cache.read().contains(key)
    }

    /// Remove a contract from the seeding cache.
    ///
    /// Returns the upstream peer to notify if any.
    #[allow(dead_code)]
    pub fn remove_seeded_contract(&self, key: &ContractKey) -> Option<PeerKeyLocation> {
        let removed = self.seeding_cache.write().remove(key).is_some();
        if removed {
            let upstream = self.get_upstream(key);
            self.subscriptions.remove(key);
            self.client_subscriptions.remove(key);
            return upstream;
        }
        None
    }

    // ==================== Legacy API (for compatibility during migration) ====================

    /// Get all downstream subscribers for a contract.
    /// This is the replacement for the old `subscribers_of` method.
    pub fn subscribers_of(&self, contract: &ContractKey) -> Option<Vec<PeerKeyLocation>> {
        let downstream = self.get_downstream(contract);
        if downstream.is_empty() {
            None
        } else {
            Some(downstream)
        }
    }

    /// Get all subscriptions across all contracts (for debugging/introspection).
    pub fn all_subscriptions(&self) -> Vec<(ContractKey, Vec<PeerKeyLocation>)> {
        self.subscriptions
            .iter()
            .map(|entry| {
                let downstream: Vec<PeerKeyLocation> = entry
                    .value()
                    .iter()
                    .filter(|e| e.role == SubscriptionRole::Downstream)
                    .map(|e| e.peer.clone())
                    .collect();
                (*entry.key(), downstream)
            })
            .filter(|(_, subs)| !subs.is_empty())
            .collect()
    }

    /// Get detailed subscription info for debugging.
    pub fn subscription_details(
        &self,
        contract: &ContractKey,
    ) -> Option<(Option<SocketAddr>, Vec<SocketAddr>, bool)> {
        let subs = self.subscriptions.get(contract)?;

        let upstream = subs
            .iter()
            .find(|e| e.role == SubscriptionRole::Upstream)
            .and_then(|e| e.peer.socket_addr());

        let downstream: Vec<SocketAddr> = subs
            .iter()
            .filter(|e| e.role == SubscriptionRole::Downstream)
            .filter_map(|e| e.peer.socket_addr())
            .collect();

        let has_client = self.has_client_subscriptions(contract);

        Some((upstream, downstream, has_client))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::transport::TransportKeypair;
    use freenet_stdlib::prelude::ContractInstanceId;
    use std::net::{IpAddr, Ipv4Addr};

    fn test_peer_id(id: u8) -> PeerId {
        // Use different IP prefixes to get different locations
        // Location is computed from IP with last byte masked out,
        // so we vary the third octet to get different locations
        let addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(10, 0, id, 1)), 1000 + id as u16);
        let pub_key = TransportKeypair::new().public().clone();
        PeerId::new(addr, pub_key)
    }

    fn test_peer_loc(id: u8) -> PeerKeyLocation {
        let peer = test_peer_id(id);
        PeerKeyLocation::new(peer.pub_key, peer.addr)
    }

    fn make_contract_key(seed: u8) -> ContractKey {
        ContractKey::from(ContractInstanceId::new([seed; 32]))
    }

    // ==================== Basic Downstream/Upstream Tests ====================

    #[test]
    fn test_add_downstream_basic() {
        let manager = SeedingManager::new();
        let contract = make_contract_key(1);
        let peer = test_peer_loc(1);

        assert!(manager
            .add_downstream(&contract, peer.clone(), None)
            .is_ok());

        let downstream = manager.get_downstream(&contract);
        assert_eq!(downstream.len(), 1);
        assert_eq!(downstream[0].socket_addr(), peer.socket_addr());
    }

    #[test]
    fn test_add_downstream_with_observed_addr() {
        let manager = SeedingManager::new();
        let contract = make_contract_key(1);

        // Peer reports loopback address (behind NAT)
        let embedded_addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 5000);
        let peer = PeerKeyLocation::new(TransportKeypair::new().public().clone(), embedded_addr);

        // But we observed their real address
        let observed_addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(203, 0, 113, 50)), 12345);
        let observed = ObservedAddr::from(observed_addr);

        assert!(manager
            .add_downstream(&contract, peer.clone(), Some(observed))
            .is_ok());

        let downstream = manager.get_downstream(&contract);
        assert_eq!(downstream.len(), 1);
        assert_eq!(downstream[0].socket_addr(), Some(observed_addr));
        assert_eq!(downstream[0].pub_key, peer.pub_key);
    }

    #[test]
    fn test_add_downstream_duplicate() {
        let manager = SeedingManager::new();
        let contract = make_contract_key(1);
        let peer = test_peer_loc(1);

        assert!(manager
            .add_downstream(&contract, peer.clone(), None)
            .is_ok());
        assert!(manager
            .add_downstream(&contract, peer.clone(), None)
            .is_ok());

        // Should still only have 1 subscriber
        let downstream = manager.get_downstream(&contract);
        assert_eq!(downstream.len(), 1);
    }

    #[test]
    fn test_add_downstream_max_limit() {
        let manager = SeedingManager::new();
        let contract = make_contract_key(1);

        // Add MAX_DOWNSTREAM (10) subscribers
        for i in 0..10 {
            let peer = test_peer_loc(i + 1);
            assert!(
                manager.add_downstream(&contract, peer, None).is_ok(),
                "Should accept subscriber {}",
                i
            );
        }

        // 11th should fail
        let extra_peer = test_peer_loc(100);
        assert_eq!(
            manager.add_downstream(&contract, extra_peer, None),
            Err(SubscriptionError::MaxSubscribersReached)
        );

        assert_eq!(manager.get_downstream(&contract).len(), 10);
    }

    #[test]
    fn test_set_upstream_basic() {
        let manager = SeedingManager::new();
        let contract = make_contract_key(1);
        let upstream = test_peer_loc(1);

        manager.set_upstream(&contract, upstream.clone());

        let retrieved = manager.get_upstream(&contract);
        assert!(retrieved.is_some());
        assert_eq!(retrieved.unwrap().socket_addr(), upstream.socket_addr());
    }

    #[test]
    fn test_set_upstream_replaces_existing() {
        let manager = SeedingManager::new();
        let contract = make_contract_key(1);
        let upstream1 = test_peer_loc(1);
        let upstream2 = test_peer_loc(2);

        manager.set_upstream(&contract, upstream1.clone());
        manager.set_upstream(&contract, upstream2.clone());

        let retrieved = manager.get_upstream(&contract);
        assert!(retrieved.is_some());
        assert_eq!(retrieved.unwrap().socket_addr(), upstream2.socket_addr());
    }

    #[test]
    fn test_upstream_and_downstream_coexist() {
        let manager = SeedingManager::new();
        let contract = make_contract_key(1);
        let upstream = test_peer_loc(1);
        let downstream1 = test_peer_loc(2);
        let downstream2 = test_peer_loc(3);

        manager.set_upstream(&contract, upstream.clone());
        assert!(manager
            .add_downstream(&contract, downstream1.clone(), None)
            .is_ok());
        assert!(manager
            .add_downstream(&contract, downstream2.clone(), None)
            .is_ok());

        assert_eq!(
            manager.get_upstream(&contract).unwrap().socket_addr(),
            upstream.socket_addr()
        );
        assert_eq!(manager.get_downstream(&contract).len(), 2);
    }

    // ==================== Client Subscription Tests ====================

    #[test]
    fn test_client_subscription_basic() {
        let manager = SeedingManager::new();
        let contract = make_contract_key(1);
        let client_id = crate::client_events::ClientId::next();

        assert!(!manager.has_client_subscriptions(&contract));

        manager.add_client_subscription(&contract, client_id);
        assert!(manager.has_client_subscriptions(&contract));

        let no_more = manager.remove_client_subscription(&contract, client_id);
        assert!(no_more);
        assert!(!manager.has_client_subscriptions(&contract));
    }

    #[test]
    fn test_client_subscription_multiple_clients() {
        let manager = SeedingManager::new();
        let contract = make_contract_key(1);
        let client1 = crate::client_events::ClientId::next();
        let client2 = crate::client_events::ClientId::next();

        manager.add_client_subscription(&contract, client1);
        manager.add_client_subscription(&contract, client2);

        let no_more = manager.remove_client_subscription(&contract, client1);
        assert!(!no_more); // Still has client2
        assert!(manager.has_client_subscriptions(&contract));

        let no_more = manager.remove_client_subscription(&contract, client2);
        assert!(no_more);
        assert!(!manager.has_client_subscriptions(&contract));
    }

    // ==================== Pruning Tests ====================

    #[test]
    fn test_remove_subscriber_no_pruning_with_other_downstream() {
        let manager = SeedingManager::new();
        let contract = make_contract_key(1);
        let upstream = test_peer_loc(1);
        let downstream1 = test_peer_id(2);
        let downstream2 = test_peer_loc(3);

        manager.set_upstream(&contract, upstream.clone());
        assert!(manager
            .add_downstream(
                &contract,
                PeerKeyLocation::new(downstream1.pub_key.clone(), downstream1.addr),
                None
            )
            .is_ok());
        assert!(manager
            .add_downstream(&contract, downstream2.clone(), None)
            .is_ok());

        let result = manager.remove_subscriber(&contract, &downstream1);

        // Should NOT notify upstream because downstream2 still exists
        assert!(result.notify_upstream.is_none());
        assert_eq!(manager.get_downstream(&contract).len(), 1);
    }

    #[test]
    fn test_remove_subscriber_no_pruning_with_client_subscription() {
        let manager = SeedingManager::new();
        let contract = make_contract_key(1);
        let upstream = test_peer_loc(1);
        let downstream = test_peer_id(2);
        let client_id = crate::client_events::ClientId::next();

        manager.set_upstream(&contract, upstream.clone());
        assert!(manager
            .add_downstream(
                &contract,
                PeerKeyLocation::new(downstream.pub_key.clone(), downstream.addr),
                None
            )
            .is_ok());
        manager.add_client_subscription(&contract, client_id);

        let result = manager.remove_subscriber(&contract, &downstream);

        // Should NOT notify upstream because client subscription exists
        assert!(result.notify_upstream.is_none());
        assert!(manager.has_client_subscriptions(&contract));
    }

    #[test]
    fn test_remove_subscriber_triggers_pruning() {
        let manager = SeedingManager::new();
        let contract = make_contract_key(1);
        let upstream = test_peer_loc(1);
        let downstream = test_peer_id(2);

        manager.set_upstream(&contract, upstream.clone());
        assert!(manager
            .add_downstream(
                &contract,
                PeerKeyLocation::new(downstream.pub_key.clone(), downstream.addr),
                None
            )
            .is_ok());

        let result = manager.remove_subscriber(&contract, &downstream);

        // Should notify upstream because no downstream and no client subscriptions
        assert!(result.notify_upstream.is_some());
        assert_eq!(
            result.notify_upstream.unwrap().socket_addr(),
            upstream.socket_addr()
        );

        // Contract should be completely cleaned up
        assert!(manager.get_downstream(&contract).is_empty());
        assert!(manager.get_upstream(&contract).is_none());
    }

    #[test]
    fn test_remove_upstream_no_pruning() {
        let manager = SeedingManager::new();
        let contract = make_contract_key(1);
        let upstream = test_peer_id(1);

        manager.set_upstream(
            &contract,
            PeerKeyLocation::new(upstream.pub_key.clone(), upstream.addr),
        );

        let result = manager.remove_subscriber(&contract, &upstream);

        // Removing upstream should NOT trigger pruning notifications
        assert!(result.notify_upstream.is_none());
    }

    #[test]
    fn test_prune_peer_by_location() {
        let manager = SeedingManager::new();
        let contract1 = make_contract_key(1);
        let contract2 = make_contract_key(2);

        let upstream1 = test_peer_loc(1);
        let upstream2 = test_peer_loc(2);
        let downstream = test_peer_loc(3);

        manager.set_upstream(&contract1, upstream1.clone());
        manager.set_upstream(&contract2, upstream2.clone());
        assert!(manager
            .add_downstream(&contract1, downstream.clone(), None)
            .is_ok());
        assert!(manager
            .add_downstream(&contract2, downstream.clone(), None)
            .is_ok());

        // Prune the downstream peer
        let loc = downstream.location().unwrap();
        let result = manager.prune_peer(loc);

        // Should have notifications for both contracts
        assert_eq!(result.notifications.len(), 2);

        // Both contracts should be cleaned up
        assert!(manager.get_downstream(&contract1).is_empty());
        assert!(manager.get_downstream(&contract2).is_empty());
    }

    #[test]
    fn test_prune_peer_partial_with_client_subscription() {
        let manager = SeedingManager::new();
        let contract1 = make_contract_key(1);
        let contract2 = make_contract_key(2);

        let upstream1 = test_peer_loc(1);
        let upstream2 = test_peer_loc(2);
        let downstream = test_peer_loc(3);
        let client_id = crate::client_events::ClientId::next();

        manager.set_upstream(&contract1, upstream1.clone());
        manager.set_upstream(&contract2, upstream2.clone());
        assert!(manager
            .add_downstream(&contract1, downstream.clone(), None)
            .is_ok());
        assert!(manager
            .add_downstream(&contract2, downstream.clone(), None)
            .is_ok());

        // Add client subscription only to contract1
        manager.add_client_subscription(&contract1, client_id);

        let loc = downstream.location().unwrap();
        let result = manager.prune_peer(loc);

        // Should only notify for contract2 (contract1 has client subscription)
        assert_eq!(result.notifications.len(), 1);
        assert_eq!(result.notifications[0].0, contract2);
    }

    // ==================== Eviction Tests ====================

    #[test]
    fn test_eviction_returns_upstream() {
        let manager = SeedingManager::new();
        let key = make_contract_key(1);
        let upstream = test_peer_loc(1);

        // Record large contract
        manager.record_contract_access(key, 90 * 1024 * 1024, AccessType::Get);
        manager.set_upstream(&key, upstream.clone());

        // Force eviction
        let new_key = make_contract_key(2);
        let evictions = manager.record_contract_access(new_key, 20 * 1024 * 1024, AccessType::Get);

        // Should have eviction with upstream
        assert_eq!(evictions.len(), 1);
        assert_eq!(evictions[0].0, key);
        assert!(evictions[0].1.is_some());
        assert_eq!(
            evictions[0].1.as_ref().unwrap().socket_addr(),
            upstream.socket_addr()
        );
    }

    #[test]
    fn test_eviction_clears_client_subscriptions() {
        let manager = SeedingManager::new();
        let key = make_contract_key(1);
        let client_id = crate::client_events::ClientId::next();

        manager.record_contract_access(key, 90 * 1024 * 1024, AccessType::Get);
        manager.add_client_subscription(&key, client_id);
        assert!(manager.has_client_subscriptions(&key));

        // Force eviction
        let new_key = make_contract_key(2);
        manager.record_contract_access(new_key, 20 * 1024 * 1024, AccessType::Get);

        assert!(!manager.has_client_subscriptions(&key));
    }

    // ==================== Legacy API Tests ====================

    #[test]
    fn test_subscribers_of_returns_downstream_only() {
        let manager = SeedingManager::new();
        let contract = make_contract_key(1);
        let upstream = test_peer_loc(1);
        let downstream1 = test_peer_loc(2);
        let downstream2 = test_peer_loc(3);

        manager.set_upstream(&contract, upstream);
        assert!(manager
            .add_downstream(&contract, downstream1.clone(), None)
            .is_ok());
        assert!(manager
            .add_downstream(&contract, downstream2.clone(), None)
            .is_ok());

        let subs = manager.subscribers_of(&contract).unwrap();

        // Should only contain downstream, not upstream
        assert_eq!(subs.len(), 2);
        assert!(subs
            .iter()
            .any(|p| p.socket_addr() == downstream1.socket_addr()));
        assert!(subs
            .iter()
            .any(|p| p.socket_addr() == downstream2.socket_addr()));
    }

    #[test]
    fn test_all_subscriptions() {
        let manager = SeedingManager::new();
        let contract1 = make_contract_key(1);
        let contract2 = make_contract_key(2);
        let downstream1 = test_peer_loc(1);
        let downstream2 = test_peer_loc(2);

        assert!(manager
            .add_downstream(&contract1, downstream1, None)
            .is_ok());
        assert!(manager
            .add_downstream(&contract2, downstream2, None)
            .is_ok());

        let all = manager.all_subscriptions();
        assert_eq!(all.len(), 2);
    }

    #[test]
    fn test_subscription_details() {
        let manager = SeedingManager::new();
        let contract = make_contract_key(1);
        let upstream = test_peer_loc(1);
        let downstream = test_peer_loc(2);
        let client_id = crate::client_events::ClientId::next();

        manager.set_upstream(&contract, upstream.clone());
        assert!(manager
            .add_downstream(&contract, downstream.clone(), None)
            .is_ok());
        manager.add_client_subscription(&contract, client_id);

        let details = manager.subscription_details(&contract).unwrap();

        assert_eq!(details.0, upstream.socket_addr()); // upstream
        assert_eq!(details.1.len(), 1); // downstream
        assert!(details.2); // has_client
    }

    #[test]
    fn test_is_seeding_contract() {
        let manager = SeedingManager::new();
        let key = make_contract_key(1);

        assert!(!manager.is_seeding_contract(&key));

        manager.record_contract_access(key, 1000, AccessType::Get);

        assert!(manager.is_seeding_contract(&key));
    }
}
