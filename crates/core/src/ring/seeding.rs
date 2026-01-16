use super::get_subscription_cache::{GetSubscriptionCache, DEFAULT_MAX_ENTRIES, DEFAULT_MIN_TTL};
use super::seeding_cache::{AccessType, SeedingCache};
use super::{Location, PeerKeyLocation};
use crate::node::PeerId;
use crate::transport::ObservedAddr;
use crate::util::backoff::{ExponentialBackoff, TrackedBackoff};
use crate::util::time_source::InstantTimeSrc;
use dashmap::{DashMap, DashSet};
use freenet_stdlib::prelude::{ContractInstanceId, ContractKey};
use parking_lot::RwLock;
use std::collections::HashSet;
use std::time::Duration;
use tracing::{debug, info, warn};

/// Default seeding cache budget: 100MB
/// This can be made configurable via node configuration in the future.
const DEFAULT_SEEDING_BUDGET_BYTES: u64 = 100 * 1024 * 1024;

/// Initial backoff duration for subscription retries.
///
/// Set to 30 seconds to match connection backoff timing. Subscription requests
/// arrive at similar intervals to connection requests (~60s), so the same
/// rationale applies: shorter backoffs are ineffective. See issue #2595.
const INITIAL_SUBSCRIPTION_BACKOFF: Duration = Duration::from_secs(30);

/// Maximum backoff duration for subscription retries.
///
/// Set to 10 minutes to match connection backoff timing. See issue #2595.
const MAX_SUBSCRIPTION_BACKOFF: Duration = Duration::from_secs(600); // 10 minutes

/// Maximum number of tracked subscription backoff entries.
const MAX_SUBSCRIPTION_BACKOFF_ENTRIES: usize = 4096;

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
    /// Attempted to add self as subscriber (self-reference).
    SelfReference,
    /// Candidate upstream peer is not closer to the contract location than self.
    /// This prevents cycles and optimizes subscription trees toward the contract.
    NotCloserToContract,
}

/// Result of adding a downstream subscriber.
#[derive(Debug, PartialEq)]
pub struct AddDownstreamResult {
    /// Whether this was a new subscriber (vs a duplicate).
    pub is_new: bool,
    /// The current count of downstream subscribers.
    pub downstream_count: usize,
    /// The subscriber that was added (with resolved address).
    pub subscriber: super::PeerKeyLocation,
}

/// Result of adding a client subscription.
#[derive(Debug)]
pub struct AddClientSubscriptionResult {
    /// Whether this was the first client for this contract (seeding started).
    pub is_first_client: bool,
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
    /// Whether a subscriber was actually removed.
    pub removed: bool,
    /// The role of the removed subscriber (if any).
    pub removed_role: Option<SubscriberType>,
    /// Remaining downstream count after removal.
    pub downstream_count: usize,
}

/// Result of removing all subscriptions for a disconnected client.
#[derive(Debug)]
pub struct ClientDisconnectResult {
    /// Contracts that need upstream pruning notification (contract, upstream pairs).
    pub prune_notifications: Vec<(ContractKey, PeerKeyLocation)>,
    /// All contracts where this client had a subscription (for interest cleanup).
    pub affected_contracts: Vec<ContractKey>,
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

    /// LRU+TTL cache of contracts we should auto-subscribe to based on GET access.
    /// When a GET succeeds, we add the contract here and subscribe to receive updates.
    /// On eviction, the background sweep task cleans up local subscription state
    /// (unless there's an explicit client subscription).
    get_subscription_cache: RwLock<GetSubscriptionCache<InstantTimeSrc>>,

    /// Contracts with subscription requests currently in-flight.
    /// Prevents duplicate requests for the same contract.
    pending_subscription_requests: DashSet<ContractKey>,

    /// Exponential backoff state for subscription retries.
    /// Uses the unified TrackedBackoff to prevent subscription spam.
    subscription_backoff: RwLock<TrackedBackoff<ContractKey>>,
}

impl SeedingManager {
    /// Max number of downstream subscribers for a contract.
    const MAX_DOWNSTREAM: usize = 10;

    pub fn new() -> Self {
        let backoff_config =
            ExponentialBackoff::new(INITIAL_SUBSCRIPTION_BACKOFF, MAX_SUBSCRIPTION_BACKOFF);
        Self {
            subscriptions: DashMap::new(),
            client_subscriptions: DashMap::new(),
            seeding_cache: RwLock::new(SeedingCache::new(
                DEFAULT_SEEDING_BUDGET_BYTES,
                InstantTimeSrc::new(),
            )),
            get_subscription_cache: RwLock::new(GetSubscriptionCache::new(
                DEFAULT_MAX_ENTRIES,
                DEFAULT_MIN_TTL,
                InstantTimeSrc::new(),
            )),
            pending_subscription_requests: DashSet::new(),
            subscription_backoff: RwLock::new(TrackedBackoff::new(
                backoff_config,
                MAX_SUBSCRIPTION_BACKOFF_ENTRIES,
            )),
        }
    }

    /// Add a downstream subscriber (a peer that wants updates FROM us).
    ///
    /// The `observed_addr` parameter is the transport-level address from which the subscribe
    /// message was received. This is used instead of the address embedded in `subscriber`
    /// because NAT peers may embed incorrect (e.g., loopback) addresses in their messages.
    ///
    /// The `own_addr` parameter is our own network address, used to prevent self-references.
    ///
    /// Returns information about the operation for telemetry.
    ///
    /// Note: A peer CAN be both upstream and downstream simultaneously. This enables
    /// bidirectional update flow when network routing creates such relationships.
    /// The sender-filtering in update propagation prevents infinite loops.
    ///
    /// # Errors
    /// - `SelfReference`: The subscriber address matches our own address
    /// - `MaxSubscribersReached`: Maximum downstream subscribers limit reached
    pub fn add_downstream(
        &self,
        contract: &ContractKey,
        subscriber: PeerKeyLocation,
        observed_addr: Option<ObservedAddr>,
        own_addr: Option<std::net::SocketAddr>,
    ) -> Result<AddDownstreamResult, SubscriptionError> {
        // Use the transport-level address if available
        let subscriber = if let Some(addr) = observed_addr {
            PeerKeyLocation::new(subscriber.pub_key.clone(), addr.socket_addr())
        } else {
            subscriber
        };

        // Validate: prevent self-reference
        if let (Some(own), Some(sub_addr)) = (own_addr, subscriber.socket_addr()) {
            if own == sub_addr {
                warn!(
                    %contract,
                    subscriber = %sub_addr,
                    "add_downstream: rejected self-reference (subscriber is ourselves)"
                );
                return Err(SubscriptionError::SelfReference);
            }
        }

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
            return Ok(AddDownstreamResult {
                is_new: false,
                downstream_count,
                subscriber,
            });
        }

        let subscriber_addr = subscriber
            .socket_addr()
            .map(|a| a.to_string())
            .unwrap_or_else(|| "unknown".into());

        subs.push(SubscriptionEntry::new(
            subscriber.clone(),
            SubscriberType::Downstream,
        ));

        let new_count = downstream_count + 1;
        info!(
            %contract,
            subscriber = %subscriber_addr,
            downstream_count = new_count,
            "add_downstream: registered new downstream subscriber"
        );

        Ok(AddDownstreamResult {
            is_new: true,
            downstream_count: new_count,
            subscriber,
        })
    }

    /// Set the upstream source for a contract (the peer we get updates FROM).
    ///
    /// There can be at most one upstream per contract. If an upstream already exists,
    /// it will be replaced only if the new upstream is closer to the contract.
    ///
    /// The `own_location` parameter is our location on the ring, used for proximity validation.
    /// The `own_addr` parameter is our network address, used to prevent self-references.
    ///
    /// **Proximity validation**: Only accepts a peer as upstream if they are CLOSER to the
    /// contract location than we are. This creates a DAG that naturally flows toward the
    /// contract location, preventing cycles and optimizing subscription trees.
    ///
    /// Note: A peer CAN be both upstream and downstream simultaneously. This enables
    /// bidirectional update flow when network routing creates such relationships.
    /// The sender-filtering in update propagation prevents infinite loops.
    ///
    /// # Errors
    /// - `SelfReference`: The upstream address matches our own address
    /// - `NotCloserToContract`: The upstream is not closer to the contract than we are
    pub fn set_upstream(
        &self,
        contract: &ContractKey,
        upstream: PeerKeyLocation,
        own_location: Option<Location>,
        own_addr: Option<std::net::SocketAddr>,
    ) -> Result<(), SubscriptionError> {
        // Validate: prevent self-reference
        if let (Some(own), Some(up_addr)) = (own_addr, upstream.socket_addr()) {
            if own == up_addr {
                warn!(
                    %contract,
                    upstream = %up_addr,
                    "set_upstream: rejected self-reference (upstream is ourselves)"
                );
                return Err(SubscriptionError::SelfReference);
            }
        }

        // Validate: upstream must be closer to contract than we are
        // This prevents cycles and optimizes subscription trees
        if let (Some(own_loc), Some(upstream_loc)) = (own_location, upstream.location()) {
            let contract_location = Location::from(contract);
            let upstream_distance = upstream_loc.distance(contract_location);
            let self_distance = own_loc.distance(contract_location);

            if upstream_distance >= self_distance {
                let upstream_addr = upstream
                    .socket_addr()
                    .map(|a| a.to_string())
                    .unwrap_or_else(|| "unknown".into());
                debug!(
                    %contract,
                    upstream = %upstream_addr,
                    upstream_distance = %upstream_distance,
                    self_distance = %self_distance,
                    "set_upstream: rejected - upstream not closer to contract (prevents cycles)"
                );
                return Err(SubscriptionError::NotCloserToContract);
            }
        }

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

        Ok(())
    }

    /// Force-set upstream for notification purposes only.
    ///
    /// This is used when proximity validation fails but we still need to track the peer
    /// for pruning notifications. When we later unsubscribe, we need to notify this peer
    /// to remove us from their downstream list.
    ///
    /// Unlike `set_upstream`, this bypasses proximity validation and is only for
    /// maintaining correct subscription tree cleanup semantics.
    pub fn force_set_upstream_for_notifications(
        &self,
        contract: &ContractKey,
        upstream: PeerKeyLocation,
    ) {
        let mut subs = self.subscriptions.entry(*contract).or_default();

        // Remove any existing upstream
        subs.retain(|e| e.role != SubscriberType::Upstream);

        let upstream_addr = upstream
            .socket_addr()
            .map(|a| a.to_string())
            .unwrap_or_else(|| "unknown".into());

        subs.push(SubscriptionEntry::new(upstream, SubscriberType::Upstream));

        debug!(
            %contract,
            upstream = %upstream_addr,
            "force_set_upstream_for_notifications: registered upstream for pruning notifications only"
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
        let mut result: Vec<ContractKey> = seeded_contracts
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
            .collect();

        // Sort by contract ID for deterministic iteration order
        result.sort_by(|a, b| a.id().cmp(b.id()));
        result
    }

    /// Get all downstream subscribers for a contract (for broadcast targeting).
    pub fn get_downstream(&self, contract: &ContractKey) -> Vec<PeerKeyLocation> {
        let mut result: Vec<PeerKeyLocation> = self
            .subscriptions
            .get(contract)
            .map(|subs| {
                subs.iter()
                    .filter(|e| e.role == SubscriberType::Downstream)
                    .map(|e| e.peer.clone())
                    .collect()
            })
            .unwrap_or_default();
        // Sort for deterministic iteration order
        result.sort();
        result
    }

    /// Register a client subscription for a contract (WebSocket client subscribed).
    ///
    /// Returns information about the operation for telemetry.
    pub fn add_client_subscription(
        &self,
        instance_id: &ContractInstanceId,
        client_id: crate::client_events::ClientId,
    ) -> AddClientSubscriptionResult {
        let mut entry = self.client_subscriptions.entry(*instance_id).or_default();
        let is_first_client = entry.is_empty();
        entry.insert(client_id);
        debug!(
            contract = %instance_id,
            %client_id,
            is_first_client = is_first_client,
            "add_client_subscription: registered client subscription"
        );
        AddClientSubscriptionResult { is_first_client }
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
    /// Returns a [`ClientDisconnectResult`] with:
    /// - `prune_notifications`: contracts needing upstream pruning
    /// - `affected_contracts`: all contracts where the client was subscribed (for interest cleanup)
    pub fn remove_client_from_all_subscriptions(
        &self,
        client_id: crate::client_events::ClientId,
    ) -> ClientDisconnectResult {
        let mut prune_notifications = Vec::new();
        let mut affected_contracts = Vec::new();

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

            // Find the full ContractKey in subscriptions that matches this instance_id
            // (subscriptions are keyed by ContractKey, client_subscriptions by ContractInstanceId)
            let matching_contract: Option<ContractKey> = self
                .subscriptions
                .iter()
                .find(|entry| *entry.key().id() == instance_id)
                .map(|entry| *entry.key());

            if let Some(contract) = matching_contract {
                // Track all affected contracts for interest cleanup
                affected_contracts.push(contract);

                if was_last_client {
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
                                prune_notifications.push((contract, upstream));
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
            contracts_affected = affected_contracts.len(),
            contracts_pruned = prune_notifications.len(),
            "remove_client_from_all_subscriptions: completed cleanup"
        );

        ClientDisconnectResult {
            prune_notifications,
            affected_contracts,
        }
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
        let mut removed = false;
        let mut removed_role = None;
        let mut downstream_count = 0;

        if let Some(mut subs) = self.subscriptions.get_mut(contract) {
            // Find and remove the peer
            if let Some(pos) = subs.iter().position(|e| e.matches_peer(peer)) {
                let removed_entry = subs.swap_remove(pos);
                removed = true;
                removed_role = Some(removed_entry.role);

                debug!(
                    %contract,
                    peer = %peer,
                    role = ?removed_entry.role,
                    "remove_subscriber: removed peer"
                );

                // Count remaining downstream
                downstream_count = subs
                    .iter()
                    .filter(|e| e.role == SubscriberType::Downstream)
                    .count();

                // Only check for pruning if we removed a downstream subscriber
                if removed_entry.role == SubscriberType::Downstream {
                    let has_client = self.has_client_subscriptions(contract.id());

                    if downstream_count == 0 && !has_client {
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

        RemoveSubscriberResult {
            notify_upstream,
            removed,
            removed_role,
            downstream_count,
        }
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
    ///
    /// Returns the list of contracts that were evicted to make room (if any).
    /// Also cleans up backoff state for evicted contracts to prevent unbounded
    /// memory growth.
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

        // Clean up backoff state for evicted contracts to prevent unbounded memory growth.
        // When a contract is evicted from the cache, we no longer need its backoff entry
        // since we won't be attempting to re-subscribe to it.
        if !evicted.is_empty() {
            let mut backoff = self.subscription_backoff.write();
            for evicted_key in &evicted {
                backoff.record_success(evicted_key); // Clears the backoff entry
                self.pending_subscription_requests.remove(evicted_key);
            }
        }

        evicted
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
        let mut result: Vec<(ContractKey, Vec<PeerKeyLocation>)> = self
            .subscriptions
            .iter()
            .map(|entry| {
                let mut downstream: Vec<PeerKeyLocation> = entry
                    .value()
                    .iter()
                    .filter(|e| e.role == SubscriberType::Downstream)
                    .map(|e| e.peer.clone())
                    .collect();
                // Sort peers for deterministic order
                downstream.sort();
                (*entry.key(), downstream)
            })
            .filter(|(_, subs)| !subs.is_empty())
            .collect();
        // Sort by contract ID for deterministic iteration order
        result.sort_by(|(a, _), (b, _)| a.id().cmp(b.id()));
        result
    }

    /// Get the number of contracts in the seeding cache.
    /// This is the actual count of contracts this node is caching/seeding.
    pub fn seeding_contracts_count(&self) -> usize {
        self.seeding_cache.read().len()
    }

    /// Get the complete subscription state for all active subscriptions.
    ///
    /// Returns a list of tuples containing:
    /// - Contract key
    /// - Whether we're locally seeding (have client subscriptions)
    /// - Optional upstream peer
    /// - List of downstream subscribers
    ///
    /// This is used for periodic telemetry snapshots.
    pub fn get_all_subscription_states(
        &self,
    ) -> Vec<(
        ContractKey,
        bool,
        Option<PeerKeyLocation>,
        Vec<PeerKeyLocation>,
    )> {
        self.subscriptions
            .iter()
            .map(|entry| {
                let contract = *entry.key();
                let subs = entry.value();

                let is_seeding = self.has_client_subscriptions(contract.id());

                let upstream = subs
                    .iter()
                    .find(|e| e.role == SubscriberType::Upstream)
                    .map(|e| e.peer.clone());

                let downstream: Vec<PeerKeyLocation> = subs
                    .iter()
                    .filter(|e| e.role == SubscriberType::Downstream)
                    .map(|e| e.peer.clone())
                    .collect();

                (contract, is_seeding, upstream, downstream)
            })
            .collect()
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

        // Check backoff using the unified TrackedBackoff
        let backoff = self.subscription_backoff.read();
        if backoff.is_in_backoff(contract) {
            if let Some(remaining) = backoff.remaining_backoff(contract) {
                debug!(
                    %contract,
                    remaining_secs = remaining.as_secs(),
                    "subscription request in backoff period"
                );
            }
            return false;
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

        let mut backoff = self.subscription_backoff.write();

        if success {
            // Clear any backoff on success
            backoff.record_success(contract);
            info!(%contract, "subscription succeeded, cleared backoff");
        } else {
            // Apply exponential backoff on failure
            backoff.record_failure(*contract);
            if let Some(remaining) = backoff.remaining_backoff(contract) {
                info!(
                    %contract,
                    backoff_secs = remaining.as_secs(),
                    "subscription failed, applied backoff"
                );
            }
        }
    }

    // --- GET auto-subscription cache management ---

    /// Record a GET access to a contract for auto-subscription tracking.
    ///
    /// This adds the contract to the GET subscription cache. Returns any contracts
    /// evicted from the local cache. Callers should NOT automatically remove
    /// subscription state for evicted contracts, as they may have active client
    /// subscriptions. The background sweep task handles proper cleanup with client checks.
    ///
    /// Called after a successful GET operation to ensure we stay subscribed
    /// to contracts we're actively accessing.
    pub fn record_get_subscription(&self, key: ContractKey) -> Vec<ContractKey> {
        let evicted = self.get_subscription_cache.write().record_access(key);

        if !evicted.is_empty() {
            debug!(
                %key,
                evicted_count = evicted.len(),
                "GET subscription cache evicted entries"
            );
        }

        evicted
    }

    /// Refresh the access time for a contract in the GET subscription cache.
    ///
    /// Called when an UPDATE is received for a contract we're auto-subscribed to.
    /// This keeps actively-updated contracts from being evicted.
    pub fn touch_get_subscription(&self, key: &ContractKey) {
        self.get_subscription_cache.write().touch(key);
    }

    /// Sweep for expired entries in the GET subscription cache.
    ///
    /// Returns contracts evicted from this local cache. Callers should check
    /// `has_client_subscriptions()` before removing subscription state, as
    /// evicted contracts may still have active client subscriptions.
    /// Called periodically by the background sweep task.
    pub fn sweep_expired_get_subscriptions(&self) -> Vec<ContractKey> {
        self.get_subscription_cache.write().sweep_expired()
    }

    /// Check if a contract is in the GET subscription cache.
    #[allow(dead_code)]
    pub fn is_get_subscription(&self, key: &ContractKey) -> bool {
        self.get_subscription_cache.read().contains(key)
    }

    /// Remove a contract from the GET subscription cache.
    ///
    /// Called when we explicitly unsubscribe from a contract.
    #[allow(dead_code)]
    pub fn remove_get_subscription(&self, key: &ContractKey) {
        self.get_subscription_cache.write().remove(key);
    }

    /// Remove all subscription entries for a contract.
    ///
    /// Used when a GET subscription expires and we need to clean up local state.
    /// Does not send any network messages.
    pub fn remove_subscription(&self, key: &ContractKey) {
        self.subscriptions.remove(key);
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
            .add_downstream(&contract, peer.clone(), None, None)
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
            .add_downstream(&contract, peer.clone(), Some(observed), None)
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
            .add_downstream(&contract, peer.clone(), None, None)
            .is_ok());
        assert!(manager
            .add_downstream(&contract, peer.clone(), None, None)
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
                manager.add_downstream(&contract, peer, None, None).is_ok(),
                "Should accept subscriber {}",
                i
            );
        }

        // 11th should fail
        let extra_peer = test_peer_loc(100);
        assert_eq!(
            manager.add_downstream(&contract, extra_peer, None, None),
            Err(SubscriptionError::MaxSubscribersReached)
        );

        assert_eq!(manager.get_downstream(&contract).len(), 10);
    }

    #[test]
    fn test_set_upstream_basic() {
        let manager = SeedingManager::new();
        let contract = make_contract_key(1);
        let upstream = test_peer_loc(1);

        manager
            .set_upstream(&contract, upstream.clone(), None, None)
            .unwrap();

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

        manager
            .set_upstream(&contract, upstream1.clone(), None, None)
            .unwrap();
        manager
            .set_upstream(&contract, upstream2.clone(), None, None)
            .unwrap();

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

        manager
            .set_upstream(&contract, upstream.clone(), None, None)
            .unwrap();
        assert!(manager
            .add_downstream(
                &contract,
                PeerKeyLocation::new(downstream1.pub_key.clone(), downstream1.addr),
                None,
                None
            )
            .is_ok());
        assert!(manager
            .add_downstream(&contract, downstream2.clone(), None, None)
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

        manager
            .set_upstream(&contract, upstream.clone(), None, None)
            .unwrap();
        assert!(manager
            .add_downstream(
                &contract,
                PeerKeyLocation::new(downstream.pub_key.clone(), downstream.addr),
                None,
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

        manager
            .set_upstream(&contract, upstream.clone(), None, None)
            .unwrap();
        assert!(manager
            .add_downstream(
                &contract,
                PeerKeyLocation::new(downstream.pub_key.clone(), downstream.addr),
                None,
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

        manager
            .set_upstream(
                &contract,
                PeerKeyLocation::new(upstream.pub_key.clone(), upstream.addr),
                None,
                None,
            )
            .unwrap();

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

        manager
            .set_upstream(&contract1, upstream1.clone(), None, None)
            .unwrap();
        manager
            .set_upstream(&contract2, upstream2.clone(), None, None)
            .unwrap();
        assert!(manager
            .add_downstream(&contract1, downstream.clone(), None, None)
            .is_ok());
        assert!(manager
            .add_downstream(&contract2, downstream.clone(), None, None)
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

        manager
            .set_upstream(&contract1, upstream1.clone(), None, None)
            .unwrap();
        manager
            .set_upstream(&contract2, upstream2.clone(), None, None)
            .unwrap();
        assert!(manager
            .add_downstream(&contract1, downstream.clone(), None, None)
            .is_ok());
        assert!(manager
            .add_downstream(&contract2, downstream.clone(), None, None)
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

        manager
            .set_upstream(&contract, upstream, None, None)
            .unwrap();
        assert!(manager
            .add_downstream(&contract, downstream1.clone(), None, None)
            .is_ok());
        assert!(manager
            .add_downstream(&contract, downstream2.clone(), None, None)
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
        manager
            .set_upstream(&contract1, upstream1.clone(), None, None)
            .unwrap();
        manager
            .set_upstream(&contract2, upstream2.clone(), None, None)
            .unwrap();
        manager.add_client_subscription(contract1.id(), client_id);
        manager.add_client_subscription(contract2.id(), client_id);

        assert!(manager.has_client_subscriptions(contract1.id()));
        assert!(manager.has_client_subscriptions(contract2.id()));

        // Remove client from all subscriptions
        let result = manager.remove_client_from_all_subscriptions(client_id);

        // Should return 2 notifications (one for each contract's upstream)
        assert_eq!(result.prune_notifications.len(), 2);
        // Should report 2 affected contracts
        assert_eq!(result.affected_contracts.len(), 2);

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
        manager
            .set_upstream(&contract1, upstream1.clone(), None, None)
            .unwrap();
        manager.add_client_subscription(contract1.id(), client_id);

        // Setup contract2: client + downstream
        manager
            .set_upstream(&contract2, upstream2.clone(), None, None)
            .unwrap();
        assert!(manager
            .add_downstream(&contract2, downstream2.clone(), None, None)
            .is_ok());
        manager.add_client_subscription(contract2.id(), client_id);

        // Setup contract3: client + other client
        manager
            .set_upstream(&contract3, upstream3.clone(), None, None)
            .unwrap();
        manager.add_client_subscription(contract3.id(), client_id);
        manager.add_client_subscription(contract3.id(), other_client);

        // Remove client from all
        let result = manager.remove_client_from_all_subscriptions(client_id);

        // Should only notify upstream1 (contract1 pruned)
        assert_eq!(result.prune_notifications.len(), 1);
        assert_eq!(result.prune_notifications[0].0, contract1);
        assert_eq!(
            result.prune_notifications[0].1.socket_addr(),
            upstream1.socket_addr()
        );
        // Should report 3 affected contracts
        assert_eq!(result.affected_contracts.len(), 3);

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

        // Verify backoff was recorded via is_in_backoff check
        assert!(
            manager.subscription_backoff.read().is_in_backoff(&contract),
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

        // Verify backoff was recorded (failure count = 1)
        assert_eq!(
            manager.subscription_backoff.read().failure_count(&contract),
            1
        );

        // Second failure directly (don't need to wait - just testing exponential behavior)
        // Note: The TrackedBackoff internally tracks consecutive failures
        assert!(manager.mark_subscription_pending(contract));
        manager.complete_subscription_request(&contract, false);

        // Verify failure count increased (failure count = 2)
        assert_eq!(
            manager.subscription_backoff.read().failure_count(&contract),
            2
        );
    }

    #[test]
    fn test_subscription_backoff_caps_at_maximum() {
        use crate::util::backoff::ExponentialBackoff;

        // Test the ExponentialBackoff config directly to verify capping behavior
        let config =
            ExponentialBackoff::new(INITIAL_SUBSCRIPTION_BACKOFF, MAX_SUBSCRIPTION_BACKOFF);

        // After many failures, should be capped at max
        // delay_for_failures(10) = 5s * 2^9 = 2560s, but should be capped at 300s
        assert_eq!(config.delay_for_failures(10), MAX_SUBSCRIPTION_BACKOFF);
        assert_eq!(config.delay_for_failures(20), MAX_SUBSCRIPTION_BACKOFF);
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
        manager
            .set_upstream(&contract, upstream, None, None)
            .unwrap();

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
        manager
            .add_downstream(&contract, downstream, None, None)
            .unwrap();

        let contracts = manager.contracts_without_upstream();
        assert_eq!(
            contracts.len(),
            1,
            "Should include contracts with downstream subscribers"
        );
        assert_eq!(contracts[0], contract);
    }

    // ========== Tests for get_all_subscription_states ==========

    #[test]
    fn test_get_all_subscription_states_empty() {
        let manager = SeedingManager::new();
        let states = manager.get_all_subscription_states();
        assert!(states.is_empty());
    }

    #[test]
    fn test_get_all_subscription_states_complete() {
        let manager = SeedingManager::new();
        let contract = make_contract_key(1);
        let upstream = test_peer_loc(1);
        let downstream1 = test_peer_loc(2);
        let downstream2 = test_peer_loc(3);
        let client_id = crate::client_events::ClientId::next();

        // Setup: upstream, 2 downstream, and client subscription
        manager
            .set_upstream(&contract, upstream.clone(), None, None)
            .unwrap();
        assert!(manager
            .add_downstream(&contract, downstream1.clone(), None, None)
            .is_ok());
        assert!(manager
            .add_downstream(&contract, downstream2.clone(), None, None)
            .is_ok());
        manager.add_client_subscription(contract.id(), client_id);

        let states = manager.get_all_subscription_states();

        assert_eq!(states.len(), 1);
        let (key, is_seeding, upstream_opt, downstream_list) = &states[0];

        assert_eq!(*key, contract);
        assert!(*is_seeding); // has client subscription
        assert!(upstream_opt.is_some());
        assert_eq!(
            upstream_opt.as_ref().unwrap().socket_addr(),
            upstream.socket_addr()
        );
        assert_eq!(downstream_list.len(), 2);
    }

    #[test]
    fn test_get_all_subscription_states_multiple_contracts() {
        let manager = SeedingManager::new();
        let contract1 = make_contract_key(1);
        let contract2 = make_contract_key(2);

        manager
            .set_upstream(&contract1, test_peer_loc(1), None, None)
            .unwrap();
        manager
            .set_upstream(&contract2, test_peer_loc(2), None, None)
            .unwrap();
        assert!(manager
            .add_downstream(&contract1, test_peer_loc(3), None, None)
            .is_ok());

        let states = manager.get_all_subscription_states();

        assert_eq!(states.len(), 2);
    }

    #[test]
    fn test_get_all_subscription_states_upstream_only() {
        let manager = SeedingManager::new();
        let contract = make_contract_key(1);
        let upstream = test_peer_loc(1);

        // Only upstream, no downstream, no client
        manager
            .set_upstream(&contract, upstream.clone(), None, None)
            .unwrap();

        let states = manager.get_all_subscription_states();

        assert_eq!(states.len(), 1);
        let (key, is_seeding, upstream_opt, downstream_list) = &states[0];

        assert_eq!(*key, contract);
        assert!(!*is_seeding); // no client subscription
        assert!(upstream_opt.is_some());
        assert!(downstream_list.is_empty());
    }

    #[test]
    fn test_get_all_subscription_states_downstream_only() {
        let manager = SeedingManager::new();
        let contract = make_contract_key(1);
        let downstream = test_peer_loc(1);

        // Only downstream, no upstream, no client
        assert!(manager
            .add_downstream(&contract, downstream.clone(), None, None)
            .is_ok());

        let states = manager.get_all_subscription_states();

        assert_eq!(states.len(), 1);
        let (key, is_seeding, upstream_opt, downstream_list) = &states[0];

        assert_eq!(*key, contract);
        assert!(!*is_seeding); // no client subscription
        assert!(upstream_opt.is_none());
        assert_eq!(downstream_list.len(), 1);
    }

    // ========== Tests for self-reference and circular reference validation ==========

    #[test]
    fn test_add_downstream_rejects_self_reference() {
        let manager = SeedingManager::new();
        let contract = make_contract_key(1);

        // Create a peer with a known address
        let own_addr: SocketAddr = "192.168.1.1:5000".parse().unwrap();
        let self_peer = PeerKeyLocation::new(TransportKeypair::new().public().clone(), own_addr);

        // Try to add ourselves as downstream - should be rejected
        let result = manager.add_downstream(&contract, self_peer, None, Some(own_addr));
        assert_eq!(result, Err(SubscriptionError::SelfReference));
    }

    #[test]
    fn test_set_upstream_rejects_self_reference() {
        let manager = SeedingManager::new();
        let contract = make_contract_key(1);

        // Create a peer with a known address
        let own_addr: SocketAddr = "192.168.1.1:5000".parse().unwrap();
        let self_peer = PeerKeyLocation::new(TransportKeypair::new().public().clone(), own_addr);

        // Try to set ourselves as upstream - should be rejected
        let result = manager.set_upstream(&contract, self_peer, None, Some(own_addr));
        assert_eq!(result, Err(SubscriptionError::SelfReference));
    }

    #[test]
    fn test_bidirectional_subscription_allowed() {
        // A peer CAN be both upstream and downstream simultaneously.
        // This enables bidirectional update flow when network routing creates such relationships.
        let manager = SeedingManager::new();
        let contract = make_contract_key(1);
        let peer = test_peer_loc(1);

        // Set peer as upstream (we receive updates FROM them)
        manager
            .set_upstream(&contract, peer.clone(), None, None)
            .unwrap();

        // Add same peer as downstream (they receive updates FROM us)
        // This should succeed - bidirectional relationships are valid
        let result = manager.add_downstream(&contract, peer.clone(), None, None);
        assert!(
            result.is_ok(),
            "Bidirectional subscription should be allowed"
        );
        assert!(result.unwrap().is_new);

        // Verify both relationships exist
        assert_eq!(
            manager.get_upstream(&contract).unwrap().socket_addr(),
            peer.socket_addr()
        );
        assert!(manager.get_downstream(&contract).contains(&peer));
    }

    #[test]
    fn test_bidirectional_subscription_reverse_order() {
        // Test the reverse order: add downstream first, then set upstream
        let manager = SeedingManager::new();
        let contract = make_contract_key(1);
        let peer = test_peer_loc(1);

        // Add peer as downstream first
        manager
            .add_downstream(&contract, peer.clone(), None, None)
            .unwrap();

        // Now set same peer as upstream - should also succeed
        let result = manager.set_upstream(&contract, peer.clone(), None, None);
        assert!(
            result.is_ok(),
            "Bidirectional subscription should be allowed"
        );

        // Verify both relationships exist
        assert_eq!(
            manager.get_upstream(&contract).unwrap().socket_addr(),
            peer.socket_addr()
        );
        assert!(manager.get_downstream(&contract).contains(&peer));
    }

    #[test]
    fn test_valid_upstream_downstream_different_peers() {
        let manager = SeedingManager::new();
        let contract = make_contract_key(1);
        let upstream_peer = test_peer_loc(1);
        let downstream_peer = test_peer_loc(2);

        // Setting different peers as upstream and downstream should work
        manager
            .set_upstream(&contract, upstream_peer.clone(), None, None)
            .unwrap();
        let result = manager.add_downstream(&contract, downstream_peer.clone(), None, None);
        assert!(result.is_ok());

        // Verify both are registered correctly
        assert_eq!(
            manager.get_upstream(&contract).unwrap().socket_addr(),
            upstream_peer.socket_addr()
        );
        assert_eq!(manager.get_downstream(&contract).len(), 1);
    }

    // ========== Tests for proximity-based upstream validation (issue #2721) ==========

    #[test]
    fn test_set_upstream_accepts_closer_peer() {
        let manager = SeedingManager::new();
        let contract = make_contract_key(1);
        let contract_location = Location::from(&contract);

        let own_addr: SocketAddr = "192.168.1.1:5000".parse().unwrap();
        let upstream_addr: SocketAddr = "10.0.0.1:5000".parse().unwrap();
        let upstream_peer =
            PeerKeyLocation::new(TransportKeypair::new().public().clone(), upstream_addr);

        // Get the upstream's location (derived from IP)
        let upstream_location = upstream_peer
            .location()
            .expect("upstream should have location");

        // Place ourselves farther from the contract than the upstream
        // Since we can calculate distances, position ourselves on the opposite side
        let upstream_distance = upstream_location.distance(&contract_location);

        // Create our location such that our distance > upstream_distance
        // We'll be at the contract location + 0.4 (guaranteed > any realistic distance)
        let our_loc = Location::new_rounded(contract_location.as_f64() + 0.4);
        let our_distance = our_loc.distance(&contract_location);

        // The upstream should be accepted because they're closer
        if upstream_distance < our_distance {
            let result = manager.set_upstream(
                &contract,
                upstream_peer.clone(),
                Some(our_loc),
                Some(own_addr),
            );
            assert!(
                result.is_ok(),
                "Should accept upstream that is closer to contract than self"
            );
        } else {
            // If by chance our_loc is closer, just test with None to skip the check
            let result =
                manager.set_upstream(&contract, upstream_peer.clone(), None, Some(own_addr));
            assert!(
                result.is_ok(),
                "Should accept upstream when own_location is None (proximity check skipped)"
            );
        }
    }

    #[test]
    fn test_set_upstream_rejects_not_closer_peer() {
        let manager = SeedingManager::new();
        let contract = make_contract_key(1);
        let contract_location = Location::from(&contract);

        // Create an upstream peer
        let upstream_addr: SocketAddr = "10.0.0.1:5000".parse().unwrap();
        let upstream_peer =
            PeerKeyLocation::new(TransportKeypair::new().public().clone(), upstream_addr);

        // Place ourselves AT the contract location (distance 0)
        // No peer can be closer than distance 0, so this should always reject
        let own_loc = contract_location;

        let result = manager.set_upstream(&contract, upstream_peer.clone(), Some(own_loc), None);

        assert_eq!(
            result,
            Err(SubscriptionError::NotCloserToContract),
            "Should reject upstream that is not closer to contract than self (we are AT contract)"
        );
    }

    #[test]
    fn test_set_upstream_proximity_prevents_cycles() {
        // This test demonstrates that the proximity check prevents cycles
        // If A is closer to contract than B, then A cannot accept B as upstream
        // And if B is closer to contract than A, then B cannot accept A as upstream
        // This is a strict partial order, preventing cycles

        let manager = SeedingManager::new();
        let contract = make_contract_key(42);
        let contract_location = Location::from(&contract);

        // Create two peers A and B with known relative positions to contract
        let peer_a_addr: SocketAddr = "10.0.0.1:5000".parse().unwrap();
        let peer_a = PeerKeyLocation::new(TransportKeypair::new().public().clone(), peer_a_addr);

        let peer_b_addr: SocketAddr = "10.0.0.2:5000".parse().unwrap();
        let peer_b = PeerKeyLocation::new(TransportKeypair::new().public().clone(), peer_b_addr);

        // Get their locations (derived from IP)
        let loc_a = peer_a.location().expect("peer A should have location");
        let loc_b = peer_b.location().expect("peer B should have location");

        let dist_a = loc_a.distance(&contract_location);
        let dist_b = loc_b.distance(&contract_location);

        // One of them is closer to the contract - that one cannot accept the other as upstream
        if dist_a < dist_b {
            // A is closer - A cannot accept B as upstream
            let result = manager.set_upstream(&contract, peer_b.clone(), Some(loc_a), None);
            assert_eq!(
                result,
                Err(SubscriptionError::NotCloserToContract),
                "Closer peer (A) should not accept farther peer (B) as upstream"
            );
            // But B can accept A as upstream (A is closer to contract)
            let result = manager.set_upstream(&contract, peer_a.clone(), Some(loc_b), None);
            assert!(
                result.is_ok(),
                "Farther peer (B) should accept closer peer (A) as upstream"
            );
        } else if dist_b < dist_a {
            // B is closer - B cannot accept A as upstream
            let result = manager.set_upstream(&contract, peer_a.clone(), Some(loc_b), None);
            assert_eq!(
                result,
                Err(SubscriptionError::NotCloserToContract),
                "Closer peer (B) should not accept farther peer (A) as upstream"
            );
            // But A can accept B as upstream (B is closer to contract)
            let result = manager.set_upstream(&contract, peer_b.clone(), Some(loc_a), None);
            assert!(
                result.is_ok(),
                "Farther peer (A) should accept closer peer (B) as upstream"
            );
        }
        // If dist_a == dist_b, neither can accept the other (edge case)
    }

    #[test]
    fn test_set_upstream_skips_check_when_locations_unavailable() {
        let manager = SeedingManager::new();
        let contract = make_contract_key(1);

        let upstream_addr: SocketAddr = "10.0.0.1:5000".parse().unwrap();
        let upstream_peer =
            PeerKeyLocation::new(TransportKeypair::new().public().clone(), upstream_addr);

        // When own_location is None, proximity check should be skipped
        let result = manager.set_upstream(&contract, upstream_peer.clone(), None, None);
        assert!(
            result.is_ok(),
            "Should accept upstream when own_location is None"
        );

        // Clean up and test with own_addr but no own_location
        manager.remove_subscription(&contract);

        let own_addr: SocketAddr = "192.168.1.1:5000".parse().unwrap();
        let result = manager.set_upstream(&contract, upstream_peer.clone(), None, Some(own_addr));
        assert!(
            result.is_ok(),
            "Should accept upstream when own_location is None even with own_addr set"
        );
    }
}
