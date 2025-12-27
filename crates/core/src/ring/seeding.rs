use super::seeding_cache::{AccessType, SeedingCache};
use super::{Location, PeerKeyLocation};
use crate::node::PeerId;
use crate::transport::ObservedAddr;
use crate::util::time_source::InstantTimeSrc;
use dashmap::{DashMap, DashSet};
use freenet_stdlib::prelude::{ContractInstanceId, ContractKey};
use parking_lot::RwLock;
use std::collections::HashSet;
use std::time::{Duration, Instant};
use tracing::{debug, info, warn};

/// Default seeding cache budget: 100MB
/// This can be made configurable via node configuration in the future.
const DEFAULT_SEEDING_BUDGET_BYTES: u64 = 100 * 1024 * 1024;

/// Initial backoff duration for subscription retries.
const INITIAL_SUBSCRIPTION_BACKOFF: Duration = Duration::from_secs(5);

/// Maximum backoff duration for subscription retries.
const MAX_SUBSCRIPTION_BACKOFF: Duration = Duration::from_secs(300); // 5 minutes

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
pub enum SubscriberType {
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
    pub role: SubscriberType,
}

impl SubscriptionEntry {
    pub fn new(peer: PeerKeyLocation, role: SubscriberType) -> Self {
        Self { peer, role }
    }

    /// Check if this entry matches a given peer.
    pub fn matches_peer(&self, peer_id: &PeerId) -> bool {
        self.peer.pub_key == peer_id.pub_key
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

/// Result of pruning a peer connection.
#[derive(Debug)]
pub struct PruneSubscriptionsResult {
    /// List of (contract, upstream) pairs where upstream notification is needed.
    pub notifications: Vec<(ContractKey, PeerKeyLocation)>,
    /// Transactions that were pending on the pruned peer and need to be retried or failed.
    pub orphaned_transactions: Vec<crate::message::Transaction>,
}

pub(crate) struct SeedingManager {
    /// Subscriptions per contract with explicit upstream/downstream roles.
    /// This replaces the flat Vec<PeerKeyLocation> to enable proper tree pruning.
    subscriptions: DashMap<ContractKey, Vec<SubscriptionEntry>>,

    /// Contracts where a local client (WebSocket) is actively subscribed.
    /// Prevents upstream unsubscribe while client subscriptions exist, even if
    /// all network downstream peers have disconnected.
    client_subscriptions: DashMap<ContractInstanceId, HashSet<crate::client_events::ClientId>>,

    /// LRU cache of contracts this peer is seeding, with byte-budget awareness.
    seeding_cache: RwLock<SeedingCache<InstantTimeSrc>>,

    /// Contracts with subscription requests currently in-flight.
    /// Prevents duplicate requests for the same contract.
    pending_subscription_requests: DashSet<ContractKey>,

    /// Exponential backoff state for subscription retries.
    /// Maps contract to (last_attempt_time, current_backoff_duration).
    subscription_backoff: DashMap<ContractKey, (Instant, Duration)>,
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
            pending_subscription_requests: DashSet::new(),
            subscription_backoff: DashMap::new(),
        }
    }

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
            .filter(|e| e.role == SubscriberType::Downstream)
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
            e.role == SubscriberType::Downstream
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
            SubscriberType::Downstream,
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
        subs.retain(|e| e.role != SubscriberType::Upstream);

        let upstream_addr = upstream
            .socket_addr()
            .map(|a| a.to_string())
            .unwrap_or_else(|| "unknown".into());

        subs.push(SubscriptionEntry::new(upstream, SubscriberType::Upstream));

        info!(
            %contract,
            upstream = %upstream_addr,
            "set_upstream: registered upstream source"
        );
    }

    /// Get the upstream peer for a contract (if any).
    #[allow(dead_code)] // Public API for debugging/future use
    pub fn get_upstream(&self, contract: &ContractKey) -> Option<PeerKeyLocation> {
        self.subscriptions.get(contract).and_then(|subs| {
            subs.iter()
                .find(|e| e.role == SubscriberType::Upstream)
                .map(|e| e.peer.clone())
        })
    }

    /// Check if a contract has an upstream subscription.
    pub fn has_upstream(&self, contract: &ContractKey) -> bool {
        self.subscriptions
            .get(contract)
            .map(|subs| subs.iter().any(|e| e.role == SubscriberType::Upstream))
            .unwrap_or(false)
    }

    /// Check if we're part of the subscription tree for this contract.
    ///
    /// Returns true if we have an upstream subscription (receiving updates) or
    /// downstream subscribers (forwarding updates). This indicates our cache
    /// is being kept fresh via network updates.
    ///
    /// Note: Local client subscriptions alone don't indicate fresh cache -
    /// a client can be subscribed while we have no network path for updates.
    pub fn is_in_subscription_tree(&self, contract: &ContractKey) -> bool {
        self.subscriptions
            .get(contract)
            .map(|subs| !subs.is_empty())
            .unwrap_or(false)
    }

    /// Get all contracts that we're seeding but don't have an upstream subscription for,
    /// AND where we have active interest (local client subscriptions or downstream peers).
    ///
    /// These are contracts where we may be "isolated" from the subscription tree and
    /// should attempt to establish an upstream connection when possible.
    ///
    /// IMPORTANT: We only want to re-subscribe if we have active interest. If a client
    /// disconnects and pruning occurs, we should NOT try to re-subscribe just because
    /// the contract is still in our cache.
    ///
    /// PERFORMANCE NOTE: This method iterates all seeded contracts. Callers should use
    /// `can_request_subscription()` to filter results before spawning subscription
    /// requests, which provides rate-limiting via exponential backoff. For very large
    /// caches (10,000+ contracts), consider adding result caching with a short TTL.
    pub fn contracts_without_upstream(&self) -> Vec<ContractKey> {
        // Get all contracts we're seeding from the cache
        let seeded_contracts: Vec<ContractKey> = self.seeding_cache.read().iter().collect();

        // Filter to contracts that:
        // 1. Don't have an upstream subscription
        // 2. Have active interest (local clients OR downstream peers)
        seeded_contracts
            .into_iter()
            .filter(|key| {
                if self.has_upstream(key) {
                    return false; // Already has upstream
                }

                // Check for active interest
                let has_clients = self.has_client_subscriptions(key.id());
                let has_downstream = self
                    .subscriptions
                    .get(key)
                    .map(|subs| subs.iter().any(|e| e.role == SubscriberType::Downstream))
                    .unwrap_or(false);

                has_clients || has_downstream
            })
            .collect()
    }

    /// Get all downstream subscribers for a contract (for broadcast targeting).
    pub fn get_downstream(&self, contract: &ContractKey) -> Vec<PeerKeyLocation> {
        self.subscriptions
            .get(contract)
            .map(|subs| {
                subs.iter()
                    .filter(|e| e.role == SubscriberType::Downstream)
                    .map(|e| e.peer.clone())
                    .collect()
            })
            .unwrap_or_default()
    }

    /// Register a client subscription for a contract (WebSocket client subscribed).
    pub fn add_client_subscription(
        &self,
        instance_id: &ContractInstanceId,
        client_id: crate::client_events::ClientId,
    ) {
        self.client_subscriptions
            .entry(*instance_id)
            .or_default()
            .insert(client_id);
        debug!(
            contract = %instance_id,
            %client_id,
            "add_client_subscription: registered client subscription"
        );
    }

    /// Remove a client subscription.
    /// Returns true if this was the last client subscription for this contract.
    pub fn remove_client_subscription(
        &self,
        instance_id: &ContractInstanceId,
        client_id: crate::client_events::ClientId,
    ) -> bool {
        let mut no_more_subscriptions = false;

        if let Some(mut clients) = self.client_subscriptions.get_mut(instance_id) {
            clients.remove(&client_id);
            if clients.is_empty() {
                no_more_subscriptions = true;
            }
        }

        if no_more_subscriptions {
            self.client_subscriptions.remove(instance_id);
        }

        debug!(
            contract = %instance_id,
            %client_id,
            no_more_client_subscriptions = no_more_subscriptions,
            "remove_client_subscription: removed client subscription"
        );

        no_more_subscriptions
    }

    /// Check if there are any client subscriptions for a contract.
    pub fn has_client_subscriptions(&self, instance_id: &ContractInstanceId) -> bool {
        self.client_subscriptions
            .get(instance_id)
            .map(|clients| !clients.is_empty())
            .unwrap_or(false)
    }

    /// Remove a client from ALL its subscriptions (used when client disconnects).
    ///
    /// This method:
    /// 1. Finds all contracts where this client was subscribed
    /// 2. Removes the client from each
    /// 3. For contracts where this was the last client AND no downstream remain,
    ///    returns the upstream to notify for pruning
    ///
    /// Returns a list of (contract, upstream) pairs that need Unsubscribed notification.
    pub fn remove_client_from_all_subscriptions(
        &self,
        client_id: crate::client_events::ClientId,
    ) -> Vec<(ContractKey, PeerKeyLocation)> {
        let mut notifications = Vec::new();

        // Find all contracts (by instance_id) where this client is subscribed
        let instance_ids_with_client: Vec<ContractInstanceId> = self
            .client_subscriptions
            .iter()
            .filter(|entry| entry.value().contains(&client_id))
            .map(|entry| *entry.key())
            .collect();

        for instance_id in instance_ids_with_client {
            // Remove client from this contract's subscriptions
            let was_last_client = self.remove_client_subscription(&instance_id, client_id);

            if was_last_client {
                // Find the full ContractKey in subscriptions that matches this instance_id
                // (subscriptions are keyed by ContractKey, client_subscriptions by ContractInstanceId)
                let matching_contract: Option<ContractKey> = self
                    .subscriptions
                    .iter()
                    .find(|entry| *entry.key().id() == instance_id)
                    .map(|entry| *entry.key());

                if let Some(contract) = matching_contract {
                    // Check if we need to prune upstream
                    if let Some(subs) = self.subscriptions.get(&contract) {
                        let has_downstream =
                            subs.iter().any(|e| e.role == SubscriberType::Downstream);

                        if !has_downstream {
                            // No downstream and no clients - need to notify upstream
                            if let Some(upstream) = subs
                                .iter()
                                .find(|e| e.role == SubscriberType::Upstream)
                                .map(|e| e.peer.clone())
                            {
                                info!(
                                    %contract,
                                    %client_id,
                                    "remove_client_from_all_subscriptions: client disconnect triggers pruning"
                                );
                                notifications.push((contract, upstream));
                            }

                            // Clean up the subscription entry
                            drop(subs);
                            self.subscriptions.remove(&contract);
                        }
                    }
                }
            }
        }

        debug!(
            %client_id,
            contracts_cleaned = notifications.len(),
            "remove_client_from_all_subscriptions: completed cleanup"
        );

        notifications
    }

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
                if removed.role == SubscriberType::Downstream {
                    let has_downstream = subs.iter().any(|e| e.role == SubscriberType::Downstream);
                    let has_client = self.has_client_subscriptions(contract.id());

                    if !has_downstream && !has_client {
                        // Find upstream to notify
                        notify_upstream = subs
                            .iter()
                            .find(|e| e.role == SubscriberType::Upstream)
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
    pub fn prune_subscriptions_for_peer(&self, loc: Location) -> PruneSubscriptionsResult {
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
                        "prune_subscriptions_for_peer: removed peer by location"
                    );

                    // Check for pruning if we removed a downstream
                    if removed.role == SubscriberType::Downstream {
                        let has_downstream =
                            subs.iter().any(|e| e.role == SubscriberType::Downstream);
                        let has_client = self.has_client_subscriptions(contract.id());

                        if !has_downstream && !has_client {
                            if let Some(upstream) = subs
                                .iter()
                                .find(|e| e.role == SubscriberType::Upstream)
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
                "prune_subscriptions_for_peer: will notify upstream for contracts with no remaining interest"
            );
        }

        PruneSubscriptionsResult {
            notifications,
            orphaned_transactions: Vec::new(),
        }
    }

    /// Record an access to a contract in the seeding cache.
    pub fn record_contract_access(
        &self,
        key: ContractKey,
        size_bytes: u64,
        access_type: AccessType,
    ) -> Vec<ContractKey> {
        self.seeding_cache
            .write()
            .record_access(key, size_bytes, access_type)
    }

    /// Whether this node is currently caching/seeding this contract.
    #[inline]
    pub fn is_seeding_contract(&self, key: &ContractKey) -> bool {
        self.seeding_cache.read().contains(key)
    }

    /// Get all downstream subscribers for a contract.
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
                    .filter(|e| e.role == SubscriberType::Downstream)
                    .map(|e| e.peer.clone())
                    .collect();
                (*entry.key(), downstream)
            })
            .filter(|(_, subs)| !subs.is_empty())
            .collect()
    }

    /// Get the number of contracts in the seeding cache.
    /// This is the actual count of contracts this node is caching/seeding.
    pub fn seeding_contracts_count(&self) -> usize {
        self.seeding_cache.read().len()
    }

    // --- Subscription retry spam prevention ---

    /// Check if a subscription request can be made for a contract.
    ///
    /// Returns false if:
    /// - A subscription request is already in-flight for this contract
    /// - The contract is in backoff period (recent failed attempt)
    pub fn can_request_subscription(&self, contract: &ContractKey) -> bool {
        // Check if request is already in-flight
        if self.pending_subscription_requests.contains(contract) {
            debug!(%contract, "subscription request already pending");
            return false;
        }

        // Check backoff
        if let Some(entry) = self.subscription_backoff.get(contract) {
            let (last_attempt, backoff_duration) = *entry;
            if last_attempt.elapsed() < backoff_duration {
                debug!(
                    %contract,
                    elapsed_secs = last_attempt.elapsed().as_secs(),
                    backoff_secs = backoff_duration.as_secs(),
                    "subscription request in backoff period"
                );
                return false;
            }
        }

        true
    }

    /// Mark a subscription request as in-flight.
    /// Returns false if already pending (should not proceed with request).
    pub fn mark_subscription_pending(&self, contract: ContractKey) -> bool {
        self.pending_subscription_requests.insert(contract)
    }

    /// Mark a subscription request as completed (success or failure).
    /// If success is false, applies exponential backoff.
    pub fn complete_subscription_request(&self, contract: &ContractKey, success: bool) {
        self.pending_subscription_requests.remove(contract);

        if success {
            // Clear any backoff on success
            self.subscription_backoff.remove(contract);
            info!(%contract, "subscription succeeded, cleared backoff");
        } else {
            // Apply exponential backoff on failure
            let backoff_duration =
                if let Some(mut entry) = self.subscription_backoff.get_mut(contract) {
                    // Update existing entry in place
                    let (ref mut last_attempt, ref mut backoff) = *entry;
                    *last_attempt = Instant::now();
                    let new_duration = (*backoff * 2).min(MAX_SUBSCRIPTION_BACKOFF);
                    *backoff = new_duration;
                    new_duration
                } else {
                    // Insert new entry with initial backoff
                    self.subscription_backoff
                        .insert(*contract, (Instant::now(), INITIAL_SUBSCRIPTION_BACKOFF));
                    INITIAL_SUBSCRIPTION_BACKOFF
                };

            info!(
                %contract,
                backoff_secs = backoff_duration.as_secs(),
                "subscription failed, applied backoff"
            );
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::transport::TransportKeypair;
    use freenet_stdlib::prelude::{CodeHash, ContractInstanceId};
    use std::net::{IpAddr, Ipv4Addr, SocketAddr};

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
        ContractKey::from_id_and_code(
            ContractInstanceId::new([seed; 32]),
            CodeHash::new([seed.wrapping_add(1); 32]),
        )
    }

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
    fn test_client_subscription_basic() {
        let manager = SeedingManager::new();
        let contract = make_contract_key(1);
        let client_id = crate::client_events::ClientId::next();

        assert!(!manager.has_client_subscriptions(contract.id()));

        manager.add_client_subscription(contract.id(), client_id);
        assert!(manager.has_client_subscriptions(contract.id()));

        let no_more = manager.remove_client_subscription(contract.id(), client_id);
        assert!(no_more);
        assert!(!manager.has_client_subscriptions(contract.id()));
    }

    #[test]
    fn test_client_subscription_multiple_clients() {
        let manager = SeedingManager::new();
        let contract = make_contract_key(1);
        let client1 = crate::client_events::ClientId::next();
        let client2 = crate::client_events::ClientId::next();

        manager.add_client_subscription(contract.id(), client1);
        manager.add_client_subscription(contract.id(), client2);

        let no_more = manager.remove_client_subscription(contract.id(), client1);
        assert!(!no_more); // Still has client2
        assert!(manager.has_client_subscriptions(contract.id()));

        let no_more = manager.remove_client_subscription(contract.id(), client2);
        assert!(no_more);
        assert!(!manager.has_client_subscriptions(contract.id()));
    }

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
        manager.add_client_subscription(contract.id(), client_id);

        let result = manager.remove_subscriber(&contract, &downstream);

        // Should NOT notify upstream because client subscription exists
        assert!(result.notify_upstream.is_none());
        assert!(manager.has_client_subscriptions(contract.id()));
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
    fn test_prune_subscriptions_for_peer_by_location() {
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
        let result = manager.prune_subscriptions_for_peer(loc);

        // Should have notifications for both contracts
        assert_eq!(result.notifications.len(), 2);

        // Both contracts should be cleaned up
        assert!(manager.get_downstream(&contract1).is_empty());
        assert!(manager.get_downstream(&contract2).is_empty());
    }

    #[test]
    fn test_prune_subscriptions_for_peer_partial_with_client_subscription() {
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
        manager.add_client_subscription(contract1.id(), client_id);

        let loc = downstream.location().unwrap();
        let result = manager.prune_subscriptions_for_peer(loc);

        // Should only notify for contract2 (contract1 has client subscription)
        assert_eq!(result.notifications.len(), 1);
        assert_eq!(result.notifications[0].0, contract2);
    }

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
    fn test_is_seeding_contract() {
        let manager = SeedingManager::new();
        let key = make_contract_key(1);

        assert!(!manager.is_seeding_contract(&key));

        manager.record_contract_access(key, 1000, AccessType::Get);

        assert!(manager.is_seeding_contract(&key));
    }

    #[test]
    fn test_remove_client_from_all_subscriptions_basic() {
        let manager = SeedingManager::new();
        let contract1 = make_contract_key(1);
        let contract2 = make_contract_key(2);
        let client_id = crate::client_events::ClientId::next();
        let upstream1 = test_peer_loc(1);
        let upstream2 = test_peer_loc(2);

        // Setup: client subscribed to 2 contracts
        manager.set_upstream(&contract1, upstream1.clone());
        manager.set_upstream(&contract2, upstream2.clone());
        manager.add_client_subscription(contract1.id(), client_id);
        manager.add_client_subscription(contract2.id(), client_id);

        assert!(manager.has_client_subscriptions(contract1.id()));
        assert!(manager.has_client_subscriptions(contract2.id()));

        // Remove client from all subscriptions
        let notifications = manager.remove_client_from_all_subscriptions(client_id);

        // Should return 2 notifications (one for each contract's upstream)
        assert_eq!(notifications.len(), 2);

        // Client subscriptions should be gone
        assert!(!manager.has_client_subscriptions(contract1.id()));
        assert!(!manager.has_client_subscriptions(contract2.id()));
    }

    #[test]
    fn test_remove_client_from_all_subscriptions_mixed_scenarios() {
        let manager = SeedingManager::new();
        let contract1 = make_contract_key(1); // Will prune (only this client, no downstream)
        let contract2 = make_contract_key(2); // Won't prune (has downstream)
        let contract3 = make_contract_key(3); // Won't prune (has other client)
        let client_id = crate::client_events::ClientId::next();
        let other_client = crate::client_events::ClientId::next();
        let upstream1 = test_peer_loc(1);
        let upstream2 = test_peer_loc(2);
        let upstream3 = test_peer_loc(3);
        let downstream2 = test_peer_loc(4);

        // Setup contract1: only client subscription
        manager.set_upstream(&contract1, upstream1.clone());
        manager.add_client_subscription(contract1.id(), client_id);

        // Setup contract2: client + downstream
        manager.set_upstream(&contract2, upstream2.clone());
        assert!(manager
            .add_downstream(&contract2, downstream2.clone(), None)
            .is_ok());
        manager.add_client_subscription(contract2.id(), client_id);

        // Setup contract3: client + other client
        manager.set_upstream(&contract3, upstream3.clone());
        manager.add_client_subscription(contract3.id(), client_id);
        manager.add_client_subscription(contract3.id(), other_client);

        // Remove client from all
        let notifications = manager.remove_client_from_all_subscriptions(client_id);

        // Should only notify upstream1 (contract1 pruned)
        assert_eq!(notifications.len(), 1);
        assert_eq!(notifications[0].0, contract1);
        assert_eq!(notifications[0].1.socket_addr(), upstream1.socket_addr());

        // contract2 should still have downstream
        assert!(!manager.get_downstream(&contract2).is_empty());

        // contract3 should still have other_client
        assert!(manager.has_client_subscriptions(contract3.id()));
    }

    #[test]
    fn test_seeding_contracts_count() {
        use super::super::seeding_cache::AccessType;

        let seeding_manager = SeedingManager::new();

        // Initially no contracts are seeded
        assert_eq!(seeding_manager.seeding_contracts_count(), 0);

        // Add first contract
        let key1 = make_contract_key(1);
        seeding_manager.record_contract_access(key1, 1000, AccessType::Put);
        assert_eq!(seeding_manager.seeding_contracts_count(), 1);

        // Add second contract
        let key2 = make_contract_key(2);
        seeding_manager.record_contract_access(key2, 1000, AccessType::Put);
        assert_eq!(seeding_manager.seeding_contracts_count(), 2);

        // Add third contract
        let key3 = make_contract_key(3);
        seeding_manager.record_contract_access(key3, 1000, AccessType::Get);
        assert_eq!(seeding_manager.seeding_contracts_count(), 3);

        // Re-accessing a contract doesn't increase count
        seeding_manager.record_contract_access(key1, 1000, AccessType::Get);
        assert_eq!(seeding_manager.seeding_contracts_count(), 3);
    }

    // ========== Tests for subscription backoff mechanism ==========

    #[test]
    fn test_subscription_backoff_initial_allowed() {
        let manager = SeedingManager::new();
        let contract = make_contract_key(1);

        // First request should be allowed (no backoff)
        assert!(manager.can_request_subscription(&contract));
    }

    #[test]
    fn test_subscription_backoff_pending_blocks() {
        let manager = SeedingManager::new();
        let contract = make_contract_key(1);

        // Mark as pending
        assert!(manager.mark_subscription_pending(contract));

        // While pending, further requests should be blocked
        assert!(!manager.can_request_subscription(&contract));

        // And marking pending again should fail
        assert!(!manager.mark_subscription_pending(contract));
    }

    #[test]
    fn test_subscription_backoff_success_clears() {
        let manager = SeedingManager::new();
        let contract = make_contract_key(1);

        // Mark pending and complete with success
        assert!(manager.mark_subscription_pending(contract));
        manager.complete_subscription_request(&contract, true);

        // Should be allowed again (success clears backoff)
        assert!(manager.can_request_subscription(&contract));
    }

    #[test]
    fn test_subscription_backoff_failure_blocks_temporarily() {
        let manager = SeedingManager::new();
        let contract = make_contract_key(1);

        // Mark pending and complete with failure
        assert!(manager.mark_subscription_pending(contract));
        manager.complete_subscription_request(&contract, false);

        // Should be blocked due to backoff
        assert!(
            !manager.can_request_subscription(&contract),
            "Should be blocked due to backoff after failure"
        );

        // Verify backoff was recorded
        assert!(
            manager.subscription_backoff.contains_key(&contract),
            "Backoff entry should exist after failure"
        );
    }

    #[test]
    fn test_subscription_backoff_exponential_growth() {
        let manager = SeedingManager::new();
        let contract = make_contract_key(1);

        // First failure: should set initial backoff (5 seconds)
        assert!(manager.mark_subscription_pending(contract));
        manager.complete_subscription_request(&contract, false);

        let backoff1 = manager
            .subscription_backoff
            .get(&contract)
            .map(|e| e.1)
            .unwrap();
        assert_eq!(backoff1, INITIAL_SUBSCRIPTION_BACKOFF);

        // Simulate time passing past the backoff
        manager
            .subscription_backoff
            .alter(&contract, |_, (_, dur)| {
                (Instant::now() - dur - Duration::from_secs(1), dur)
            });

        // Second failure: should double the backoff
        assert!(manager.mark_subscription_pending(contract));
        manager.complete_subscription_request(&contract, false);

        let backoff2 = manager
            .subscription_backoff
            .get(&contract)
            .map(|e| e.1)
            .unwrap();
        assert_eq!(backoff2, INITIAL_SUBSCRIPTION_BACKOFF * 2);
    }

    #[test]
    fn test_subscription_backoff_caps_at_maximum() {
        let manager = SeedingManager::new();
        let contract = make_contract_key(1);

        // Set backoff just below max
        manager.subscription_backoff.insert(
            contract,
            (
                Instant::now() - MAX_SUBSCRIPTION_BACKOFF,
                MAX_SUBSCRIPTION_BACKOFF / 2,
            ),
        );

        // Complete with failure - should cap at MAX
        assert!(manager.mark_subscription_pending(contract));
        manager.complete_subscription_request(&contract, false);

        let backoff = manager
            .subscription_backoff
            .get(&contract)
            .map(|e| e.1)
            .unwrap();
        assert_eq!(backoff, MAX_SUBSCRIPTION_BACKOFF);
    }

    // ========== Tests for contracts_without_upstream filtering ==========

    #[test]
    fn test_contracts_without_upstream_requires_active_interest() {
        use super::super::seeding_cache::AccessType;

        let manager = SeedingManager::new();
        let contract = make_contract_key(1);

        // Add contract to cache (seeding it)
        manager.record_contract_access(contract, 1000, AccessType::Put);

        // Contract is cached but has no active interest (no clients, no downstream)
        let contracts = manager.contracts_without_upstream();
        assert!(
            contracts.is_empty(),
            "Should not include contracts without active interest"
        );

        // Add a client subscription - now it has active interest
        let client_id = crate::client_events::ClientId::next();
        manager.add_client_subscription(contract.id(), client_id);

        let contracts = manager.contracts_without_upstream();
        assert_eq!(contracts.len(), 1);
        assert_eq!(contracts[0], contract);
    }

    #[test]
    fn test_contracts_without_upstream_excludes_with_upstream() {
        use super::super::seeding_cache::AccessType;

        let manager = SeedingManager::new();
        let contract = make_contract_key(1);
        let upstream = test_peer_loc(1);

        // Add contract to cache
        manager.record_contract_access(contract, 1000, AccessType::Put);

        // Add client subscription (active interest)
        let client_id = crate::client_events::ClientId::next();
        manager.add_client_subscription(contract.id(), client_id);

        // Before adding upstream, should be in list
        let contracts = manager.contracts_without_upstream();
        assert_eq!(contracts.len(), 1);

        // Add upstream subscription
        manager.set_upstream(&contract, upstream);

        // After adding upstream, should NOT be in list
        let contracts = manager.contracts_without_upstream();
        assert!(
            contracts.is_empty(),
            "Contracts with upstream should not be returned"
        );
    }

    #[test]
    fn test_contracts_without_upstream_with_downstream_only() {
        use super::super::seeding_cache::AccessType;

        let manager = SeedingManager::new();
        let contract = make_contract_key(1);
        let downstream = test_peer_loc(1);

        // Add contract to cache
        manager.record_contract_access(contract, 1000, AccessType::Put);

        // Add downstream subscriber (no client subscription, but downstream = active interest)
        manager.add_downstream(&contract, downstream, None).unwrap();

        let contracts = manager.contracts_without_upstream();
        assert_eq!(
            contracts.len(),
            1,
            "Should include contracts with downstream subscribers"
        );
        assert_eq!(contracts[0], contract);
    }
}
