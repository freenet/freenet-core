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
    /// Attempted to create a circular reference (A↔B where A is both upstream and downstream of B).
    /// This would create isolated islands that don't receive updates from the main subscription tree.
    ///
    /// Note: This check prevents cycles when operations happen sequentially, but cannot prevent
    /// all cycles in a distributed system where two peers simultaneously subscribe to each other.
    /// For a complete solution, see proximity-based upstream selection which provides deterministic
    /// ordering. This check is defense-in-depth that catches the common sequential case.
    CircularReference,
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
    /// # Errors
    /// - `SelfReference`: The subscriber address matches our own address
    /// - `MaxSubscribersReached`: Maximum downstream subscribers limit reached
    /// - `CircularReference`: The subscriber is already our upstream (would create A↔B cycle)
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

        // Validate: prevent circular reference (A↔B)
        // If this peer is already our upstream for this contract, adding them as downstream
        // would create a cycle where updates loop between us instead of flowing from the tree root.
        // We check pub_key alone since that's the cryptographic identity - the same peer may
        // connect from different addresses (NAT, reconnect) but is still the same logical peer.
        let is_our_upstream = subs
            .iter()
            .any(|e| e.role == SubscriberType::Upstream && e.peer.pub_key == subscriber.pub_key);
        if is_our_upstream {
            warn!(
                %contract,
                subscriber = %subscriber.pub_key,
                "add_downstream: rejected circular reference (peer is already our upstream)"
            );
            return Err(SubscriptionError::CircularReference);
        }

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
    /// it will be replaced.
    ///
    /// The `own_addr` parameter is our own network address, used to prevent self-references.
    ///
    /// # Errors
    /// - `SelfReference`: The upstream address matches our own address
    /// - `CircularReference`: The upstream peer is already our downstream (would create A↔B cycle)
    pub fn set_upstream(
        &self,
        contract: &ContractKey,
        upstream: PeerKeyLocation,
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

        let mut subs = self.subscriptions.entry(*contract).or_default();

        // Validate: prevent circular reference (A↔B)
        // If this peer is already our downstream for this contract, setting them as upstream
        // would create a cycle where updates loop between us instead of flowing from the tree root.
        // We check pub_key alone since that's the cryptographic identity - the same peer may
        // connect from different addresses (NAT, reconnect) but is still the same logical peer.
        let is_our_downstream = subs
            .iter()
            .any(|e| e.role == SubscriberType::Downstream && e.peer.pub_key == upstream.pub_key);
        if is_our_downstream {
            warn!(
                %contract,
                upstream = %upstream.pub_key,
                "set_upstream: rejected circular reference (peer is already our downstream)"
            );
            return Err(SubscriptionError::CircularReference);
        }

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
    /// AND where we have active interest (local client subscriptions, downstream peers,
    /// or an existing subscription entry indicating we're part of the subscription tree).
    ///
    /// These are contracts where we may be "isolated" from the subscription tree and
    /// should attempt to establish an upstream connection when possible.
    ///
    /// A contract is considered to have active interest if ANY of:
    /// - It has local client subscriptions (clients actively want updates)
    /// - It has downstream peers (we need updates to forward to them)
    /// - It has an existing subscription entry (we're part of the subscription tree
    ///   and should recover our upstream connection, e.g., after upstream disconnected)
    ///
    /// Contracts that were intentionally pruned (no clients, no downstream, no entry)
    /// should NOT be auto-recovered to avoid re-subscribing after cleanup.
    ///
    /// PERFORMANCE NOTE: This method iterates all seeded contracts. Callers should use
    /// `can_request_subscription()` to filter results before spawning subscription
    /// requests, which provides rate-limiting via exponential backoff. For very large
    /// caches (10,000+ contracts), consider adding result caching with a short TTL.
    pub fn contracts_without_upstream(&self) -> Vec<ContractKey> {
        // Get all contracts from BOTH seeding cache AND subscriptions.
        // This ensures we recover intermediate forwarding nodes that may have been
        // evicted from the seeding cache (due to cache pressure) or were never cached
        // (e.g., added directly as downstream forwarding node) but still have downstream
        // subscribers. Issue #2717: Previously only checked seeding_cache, missing
        // orphaned intermediate nodes that had downstream peers but weren't in seeding_cache.
        let mut contracts_to_check: HashSet<ContractKey> =
            self.seeding_cache.read().iter().collect();
        contracts_to_check.extend(self.subscriptions.iter().map(|e| *e.key()));

        // Filter to contracts that:
        // 1. Don't have an upstream subscription
        // 2. Have active interest (local clients, downstream peers, or subscription entry)
        let mut result: Vec<ContractKey> = contracts_to_check
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

                // Also check if we have any subscription entry at all - this means we were
                // part of the subscription tree and should try to recover. This covers
                // the case where we had upstream that disconnected but no clients/downstream.
                let has_subscription_entry = self.subscriptions.contains_key(key);

                has_clients || has_downstream || has_subscription_entry
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

    /// Generate a topology snapshot for testing/validation.
    ///
    /// This creates a snapshot of the current subscription state for all contracts
    /// this peer is tracking. Used by SimNetwork for topology validation.
    #[cfg(any(test, feature = "testing"))]
    #[allow(dead_code)] // Used by Ring::register_topology_snapshot
    pub fn generate_topology_snapshot(
        &self,
        peer_addr: std::net::SocketAddr,
        location: f64,
    ) -> super::topology_registry::TopologySnapshot {
        use super::topology_registry::{ContractSubscription, TopologySnapshot};

        let mut snapshot = TopologySnapshot::new(peer_addr, location);

        // Add subscriptions for all contracts
        for entry in self.subscriptions.iter() {
            let contract_key = *entry.key();
            let subs = entry.value();

            let upstream = subs
                .iter()
                .find(|e| e.role == SubscriberType::Upstream)
                .and_then(|e| e.peer.socket_addr());

            let downstream: Vec<_> = subs
                .iter()
                .filter(|e| e.role == SubscriberType::Downstream)
                .filter_map(|e| e.peer.socket_addr())
                .collect();

            let is_seeding = self.seeding_cache.read().contains(&contract_key);
            let has_client_subscriptions = self.has_client_subscriptions(contract_key.id());

            snapshot.set_contract(
                *contract_key.id(),
                ContractSubscription {
                    contract_key,
                    upstream,
                    downstream,
                    is_seeding,
                    has_client_subscriptions,
                },
            );
        }

        // Also add contracts we're seeding but not subscribed to
        for contract_key in self.seeding_cache.read().iter() {
            if !snapshot.contracts.contains_key(contract_key.id()) {
                let has_client_subscriptions = self.has_client_subscriptions(contract_key.id());

                snapshot.set_contract(
                    *contract_key.id(),
                    ContractSubscription {
                        contract_key,
                        upstream: None,
                        downstream: vec![],
                        is_seeding: true,
                        has_client_subscriptions,
                    },
                );
            }
        }

        snapshot.timestamp_nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_nanos() as u64)
            .unwrap_or(0);

        snapshot
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
            .set_upstream(&contract, upstream.clone(), None)
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
            .set_upstream(&contract, upstream1.clone(), None)
            .unwrap();
        manager
            .set_upstream(&contract, upstream2.clone(), None)
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
            .set_upstream(&contract, upstream.clone(), None)
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
            .set_upstream(&contract, upstream.clone(), None)
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
            .set_upstream(&contract, upstream.clone(), None)
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
            .set_upstream(&contract1, upstream1.clone(), None)
            .unwrap();
        manager
            .set_upstream(&contract2, upstream2.clone(), None)
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
            .set_upstream(&contract1, upstream1.clone(), None)
            .unwrap();
        manager
            .set_upstream(&contract2, upstream2.clone(), None)
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

        manager.set_upstream(&contract, upstream, None).unwrap();
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
            .set_upstream(&contract1, upstream1.clone(), None)
            .unwrap();
        manager
            .set_upstream(&contract2, upstream2.clone(), None)
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
            .set_upstream(&contract1, upstream1.clone(), None)
            .unwrap();
        manager.add_client_subscription(contract1.id(), client_id);

        // Setup contract2: client + downstream
        manager
            .set_upstream(&contract2, upstream2.clone(), None)
            .unwrap();
        assert!(manager
            .add_downstream(&contract2, downstream2.clone(), None, None)
            .is_ok());
        manager.add_client_subscription(contract2.id(), client_id);

        // Setup contract3: client + other client
        manager
            .set_upstream(&contract3, upstream3.clone(), None)
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
    fn test_contracts_without_upstream_requires_active_interest_or_subscription_entry() {
        use super::super::seeding_cache::AccessType;

        let manager = SeedingManager::new();
        let contract = make_contract_key(1);

        // Add contract to cache (seeding it)
        manager.record_contract_access(contract, 1000, AccessType::Put);

        // Contract is cached but has no active interest (no clients, no downstream, no subscription entry)
        // This ensures intentionally pruned contracts don't get auto-recovered
        let contracts = manager.contracts_without_upstream();
        assert!(
            contracts.is_empty(),
            "Contracts without active interest should not be recovered"
        );

        // Add a client subscription - now it has active interest
        let client_id = crate::client_events::ClientId::next();
        manager.add_client_subscription(contract.id(), client_id);

        let contracts = manager.contracts_without_upstream();
        assert_eq!(contracts.len(), 1);
        assert_eq!(contracts[0], contract);
    }

    #[test]
    fn test_contracts_without_upstream_includes_orphaned_subscription_entry() {
        use super::super::seeding_cache::AccessType;

        let manager = SeedingManager::new();
        let contract = make_contract_key(1);
        let upstream = test_peer_loc(1);

        // Add contract to cache
        manager.record_contract_access(contract, 1000, AccessType::Put);

        // Set upstream (creates subscription entry)
        manager
            .set_upstream(&contract, upstream.clone(), None)
            .unwrap();

        // Verify it has upstream - should NOT be in the list
        let contracts = manager.contracts_without_upstream();
        assert!(
            contracts.is_empty(),
            "Contract with upstream should not be returned"
        );

        // Now simulate upstream disconnect: remove the upstream but keep the subscription entry
        // This is what happens when upstream peer disconnects
        if let Some(mut subs) = manager.subscriptions.get_mut(&contract) {
            subs.retain(|e| e.role != SubscriberType::Upstream);
        }

        // Now the contract has a subscription entry (even if empty) but no upstream
        // This represents an orphaned seeder that should be recovered
        let contracts = manager.contracts_without_upstream();
        assert_eq!(
            contracts.len(),
            1,
            "Orphaned contract with subscription entry should be recovered"
        );
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
        manager.set_upstream(&contract, upstream, None).unwrap();

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

    /// Issue #2717: Test that orphaned intermediate nodes (with downstream but not in seeding_cache)
    /// are properly recovered. This happens when a contract is evicted from seeding_cache due to
    /// cache pressure, but the peer still has downstream subscribers waiting for updates.
    #[test]
    fn test_contracts_without_upstream_intermediate_not_in_seeding_cache() {
        let manager = SeedingManager::new();
        let contract = make_contract_key(1);
        let downstream = test_peer_loc(1);

        // Directly add downstream subscriber WITHOUT adding to seeding_cache.
        // This simulates an intermediate forwarding node that was evicted from cache
        // but still has downstream peers.
        manager
            .add_downstream(&contract, downstream, None, None)
            .unwrap();

        // Verify contract is NOT in seeding_cache
        assert!(
            !manager.is_seeding_contract(&contract),
            "Contract should NOT be in seeding cache for this test"
        );

        // But it should still be detected as needing recovery because it has downstream
        let contracts = manager.contracts_without_upstream();
        assert_eq!(
            contracts.len(),
            1,
            "Should detect orphan intermediate node even when not in seeding_cache"
        );
        assert_eq!(contracts[0], contract);
    }

    /// Test that contracts appearing in BOTH seeding_cache AND subscriptions are deduplicated.
    /// The HashSet merge should ensure each contract appears only once in the result.
    #[test]
    fn test_contracts_without_upstream_deduplicates_when_in_both_sources() {
        use super::super::seeding_cache::AccessType;

        let manager = SeedingManager::new();
        let contract = make_contract_key(1);
        let downstream = test_peer_loc(1);

        // Add contract to seeding_cache via record_contract_access
        manager.record_contract_access(contract, 1000, AccessType::Put);
        assert!(
            manager.is_seeding_contract(&contract),
            "Contract should be in seeding cache"
        );

        // Also add downstream subscriber (creates subscription entry)
        manager
            .add_downstream(&contract, downstream, None, None)
            .unwrap();

        // Contract is now in BOTH seeding_cache AND subscriptions
        // It should appear exactly once in results (not duplicated)
        let contracts = manager.contracts_without_upstream();
        assert_eq!(
            contracts.len(),
            1,
            "Contract in both sources should appear exactly once (HashSet deduplication)"
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
            .set_upstream(&contract, upstream.clone(), None)
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
            .set_upstream(&contract1, test_peer_loc(1), None)
            .unwrap();
        manager
            .set_upstream(&contract2, test_peer_loc(2), None)
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
            .set_upstream(&contract, upstream.clone(), None)
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
        let result = manager.set_upstream(&contract, self_peer, Some(own_addr));
        assert_eq!(result, Err(SubscriptionError::SelfReference));
    }

    #[test]
    fn test_add_downstream_rejects_circular_reference() {
        // Adding a peer as downstream when they're already our upstream would create
        // a circular reference (A↔B) that isolates us from the main subscription tree.
        let manager = SeedingManager::new();
        let contract = make_contract_key(1);
        let peer = test_peer_loc(1);

        // Set peer as upstream (we receive updates FROM them)
        manager.set_upstream(&contract, peer.clone(), None).unwrap();

        // Try to add same peer as downstream - should be rejected to prevent cycle
        let result = manager.add_downstream(&contract, peer.clone(), None, None);
        assert_eq!(
            result,
            Err(SubscriptionError::CircularReference),
            "Circular reference should be rejected"
        );

        // Verify upstream still exists but no downstream was added
        assert_eq!(
            manager.get_upstream(&contract).unwrap().socket_addr(),
            peer.socket_addr()
        );
        assert!(manager.get_downstream(&contract).is_empty());
    }

    #[test]
    fn test_set_upstream_rejects_circular_reference() {
        // Setting a peer as upstream when they're already our downstream would create
        // a circular reference (A↔B) that isolates us from the main subscription tree.
        let manager = SeedingManager::new();
        let contract = make_contract_key(1);
        let peer = test_peer_loc(1);

        // Add peer as downstream first (they receive updates FROM us)
        manager
            .add_downstream(&contract, peer.clone(), None, None)
            .unwrap();

        // Try to set same peer as upstream - should be rejected to prevent cycle
        let result = manager.set_upstream(&contract, peer.clone(), None);
        assert_eq!(
            result,
            Err(SubscriptionError::CircularReference),
            "Circular reference should be rejected"
        );

        // Verify downstream still exists but no upstream was added
        assert!(manager.get_downstream(&contract).contains(&peer));
        assert!(manager.get_upstream(&contract).is_none());
    }

    #[test]
    fn test_valid_upstream_downstream_different_peers() {
        let manager = SeedingManager::new();
        let contract = make_contract_key(1);
        let upstream_peer = test_peer_loc(1);
        let downstream_peer = test_peer_loc(2);

        // Setting different peers as upstream and downstream should work
        manager
            .set_upstream(&contract, upstream_peer.clone(), None)
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

    #[test]
    fn test_circular_reference_detected_regardless_of_address() {
        // The same peer (by pub_key) connecting from a different address should still
        // be detected as circular. This prevents bypass via NAT or address change.
        let manager = SeedingManager::new();
        let contract = make_contract_key(1);

        // Create two PeerKeyLocations with same pub_key but different addresses
        let pub_key = TransportKeypair::new().public().clone();
        let addr1: SocketAddr = "10.0.0.1:5000".parse().unwrap();
        let addr2: SocketAddr = "10.0.0.2:6000".parse().unwrap();

        let peer_addr1 = PeerKeyLocation::new(pub_key.clone(), addr1);
        let peer_addr2 = PeerKeyLocation::new(pub_key.clone(), addr2);

        // Set peer (from addr1) as upstream
        manager
            .set_upstream(&contract, peer_addr1.clone(), None)
            .unwrap();

        // Try to add same peer (from addr2) as downstream - should be rejected
        // because it's the same logical peer (same pub_key)
        let result = manager.add_downstream(&contract, peer_addr2, None, None);
        assert_eq!(
            result,
            Err(SubscriptionError::CircularReference),
            "Same pub_key from different address should still be detected as circular"
        );

        // Verify upstream remains, no downstream added
        assert!(manager.get_upstream(&contract).is_some());
        assert!(manager.get_downstream(&contract).is_empty());
    }

    // ========== Issue #2773: Mutual downstream relationships ==========

    /// Tests that set_upstream succeeds even when peer is already downstream.
    ///
    /// ## Issue #2773 - Mutual Downstream Race Condition
    ///
    /// When two peers simultaneously subscribe to a contract, a race condition
    /// can cause both to add each other as downstream before either establishes
    /// upstream. This blocks subscription tree formation:
    ///
    /// 1. Peer A receives B's Subscribe → adds B as downstream
    /// 2. Peer B receives A's Subscribe → adds A as downstream
    /// 3. SubscriptionAccepted returns → both try set_upstream on each other
    /// 4. BUG: Both fail with CircularReference, leaving no upstream path
    ///
    /// ## Expected Fix (Option B)
    ///
    /// `set_upstream()` should remove the peer from downstream if present,
    /// prioritizing the upstream relationship. This test verifies that behavior.
    ///
    /// This test is IGNORED because it tests EXPECTED behavior after the fix.
    /// It will FAIL until Issue #2773 is implemented.
    ///
    /// Run with: `cargo test --ignored test_set_upstream_removes_from_downstream`
    ///
    /// See: https://github.com/freenet/freenet-core/issues/2773
    #[test]
    #[ignore = "Issue #2773: set_upstream should remove peer from downstream (not yet implemented)"]
    fn test_set_upstream_removes_from_downstream_issue_2773() {
        let manager = SeedingManager::new();
        let contract = make_contract_key(1);
        let peer = test_peer_loc(1);

        // First, add peer as downstream (simulating race condition outcome)
        manager
            .add_downstream(&contract, peer.clone(), None, None)
            .expect("Adding downstream should succeed");

        assert!(
            manager.get_downstream(&contract).contains(&peer),
            "Peer should be downstream"
        );
        assert!(manager.get_upstream(&contract).is_none(), "No upstream yet");

        // Now set the same peer as upstream
        // EXPECTED (after fix): This should SUCCEED and remove peer from downstream
        // CURRENT (bug): This returns CircularReference error
        let result = manager.set_upstream(&contract, peer.clone(), None);

        assert!(
            result.is_ok(),
            "set_upstream should succeed by removing peer from downstream first. \
             Got: {:?}. This test fails until Issue #2773 Option B is implemented.",
            result
        );

        // After fix: peer should be upstream, NOT downstream
        assert_eq!(
            manager.get_upstream(&contract).as_ref(),
            Some(&peer),
            "Peer should now be upstream"
        );
        assert!(
            !manager.get_downstream(&contract).contains(&peer),
            "Peer should no longer be downstream (moved to upstream)"
        );
    }

    /// Tests that mutual downstream deadlock is resolved by set_upstream fix.
    ///
    /// This simulates the full race condition from Issue #2773 and verifies
    /// that the fix (Option B) allows at least one peer to establish upstream.
    ///
    /// This test is IGNORED because it tests EXPECTED behavior after the fix.
    /// It will FAIL until Issue #2773 is implemented.
    ///
    /// Run with: `cargo test --ignored test_mutual_downstream_resolved`
    ///
    /// See: https://github.com/freenet/freenet-core/issues/2773
    #[test]
    #[ignore = "Issue #2773: mutual downstream should be resolvable (not yet implemented)"]
    fn test_mutual_downstream_resolved_issue_2773() {
        // Simulate the race condition: both peers independently add each other as downstream
        let manager_a = SeedingManager::new();
        let manager_b = SeedingManager::new();

        let contract = make_contract_key(1);

        let peer_a_keys = TransportKeypair::new();
        let peer_b_keys = TransportKeypair::new();
        let addr_a: SocketAddr = "10.0.0.1:5000".parse().unwrap();
        let addr_b: SocketAddr = "10.0.0.2:5000".parse().unwrap();

        let peer_a_loc = PeerKeyLocation::new(peer_a_keys.public().clone(), addr_a);
        let peer_b_loc = PeerKeyLocation::new(peer_b_keys.public().clone(), addr_b);

        // Race condition: both add each other as downstream
        manager_a
            .add_downstream(&contract, peer_b_loc.clone(), None, None)
            .expect("A adding B as downstream");
        manager_b
            .add_downstream(&contract, peer_a_loc.clone(), None, None)
            .expect("B adding A as downstream");

        // Verify mutual downstream state (the problematic condition)
        assert!(manager_a.get_downstream(&contract).contains(&peer_b_loc));
        assert!(manager_b.get_downstream(&contract).contains(&peer_a_loc));
        assert!(manager_a.get_upstream(&contract).is_none());
        assert!(manager_b.get_upstream(&contract).is_none());

        // Now both try to set upstream (simulating SubscriptionAccepted arriving)
        // EXPECTED (after fix): At least one should succeed
        // CURRENT (bug): Both fail with CircularReference
        let result_a = manager_a.set_upstream(&contract, peer_b_loc.clone(), None);
        let result_b = manager_b.set_upstream(&contract, peer_a_loc.clone(), None);

        // After fix: Both should succeed - each removes the other from downstream
        // and adds them as upstream
        assert!(
            result_a.is_ok(),
            "A setting B as upstream should succeed after fix. Got: {:?}",
            result_a
        );
        assert!(
            result_b.is_ok(),
            "B setting A as upstream should succeed after fix. Got: {:?}",
            result_b
        );

        // Verify the deadlock is broken: both have upstream established
        assert!(
            manager_a.get_upstream(&contract).is_some(),
            "A should have upstream after fix"
        );
        assert!(
            manager_b.get_upstream(&contract).is_some(),
            "B should have upstream after fix"
        );
    }
}
