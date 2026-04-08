//! Unified hosting and subscription management.
//!
//! # Architecture Overview
//!
//! This module manages contract hosting (which contracts a peer keeps available) and
//! subscription state (which contracts a peer is actively interested in).
//!
//! ## Key Design (2026-01 Unified Hosting Refactor)
//!
//! This module unifies the previously separate "hosting" and "GET subscription" caches
//! into a single `HostingCache` that serves as the source of truth for which contracts
//! this peer is hosting.
//!
//! 1. **Hosting ≠ automatic subscription renewal**: Hosted contracts are cached
//!    locally but only contracts with active client subscriptions, downstream
//!    subscribers, OR the `local_client_access` flag (#3769) get their
//!    subscriptions renewed. Relay-cached contracts (no local interest) serve
//!    as a recovery mechanism (last-resort data source) only.
//!
//! 2. **Subscriptions are lease-based**: Active subscriptions have a lease that expires
//!    unless renewed. Clients must re-subscribe periodically (every ~2 minutes).
//!
//! 3. **Single cache**: One `HostingCache` with byte-budget LRU and TTL protection.
//!
//! ## Data Flow
//!
//! - GET/PUT/SUBSCRIBE operations add contracts to the hosting cache
//! - Only locally-accessed or client-subscribed contracts get subscription renewal via `contracts_needing_renewal()`
//! - Active subscriptions prevent eviction from the hosting cache
//! - TTL protects recently accessed contracts from premature eviction

mod cache;

use crate::util::backoff::{ExponentialBackoff, TrackedBackoff};
use crate::util::time_source::{InstantTimeSrc, TimeSource};
pub use cache::{AccessType, RecordAccessResult};
use cache::{DEFAULT_HOSTING_BUDGET_BYTES, DEFAULT_MIN_TTL, HostingCache};
use dashmap::{DashMap, DashSet};
use freenet_stdlib::prelude::{ContractInstanceId, ContractKey};
use parking_lot::RwLock;
use std::collections::{HashMap, HashSet};
use std::time::Duration;
use tokio::time::Instant;
use tracing::{debug, info};

use super::interest::PeerKey;

// =============================================================================
// Constants
// =============================================================================

/// Renewal interval for subscriptions.
/// Clients should renew subscriptions at this interval to prevent expiry.
pub const SUBSCRIPTION_RENEWAL_INTERVAL: Duration = Duration::from_secs(120); // 2 minutes

/// Multiplier for lease duration relative to renewal interval.
/// Gives this many renewal attempts before subscription expires.
pub const LEASE_RENEWAL_MULTIPLIER: u32 = 4;

/// Subscription lease duration.
/// Subscriptions automatically expire after this duration unless renewed.
/// Computed as LEASE_RENEWAL_MULTIPLIER × SUBSCRIPTION_RENEWAL_INTERVAL.
pub const SUBSCRIPTION_LEASE_DURATION: Duration =
    Duration::from_secs(SUBSCRIPTION_RENEWAL_INTERVAL.as_secs() * LEASE_RENEWAL_MULTIPLIER as u64); // 8 minutes

/// Initial backoff duration for subscription retries.
const INITIAL_SUBSCRIPTION_BACKOFF: Duration = Duration::from_secs(15);

/// Maximum backoff duration for subscription retries.
///
/// Computed as 1/4 of SUBSCRIPTION_LEASE_DURATION so that a contract in
/// max-backoff always retries well before its subscription expires.
const MAX_SUBSCRIPTION_BACKOFF: Duration =
    Duration::from_secs(SUBSCRIPTION_LEASE_DURATION.as_secs() / 4); // 2 minutes

/// Maximum number of tracked subscription backoff entries.
const MAX_SUBSCRIPTION_BACKOFF_ENTRIES: usize = 4096;

/// Maximum number of downstream peer subscribers per contract.
/// Prevents network-level subscription amplification attacks.
const MAX_DOWNSTREAM_SUBSCRIBERS_PER_CONTRACT: usize = 512;

// =============================================================================
// Result Types
// =============================================================================

/// Result of adding a client subscription.
#[derive(Debug)]
pub struct AddClientSubscriptionResult {
    /// Whether this was the first client for this contract.
    pub is_first_client: bool,
}

/// Result of removing all subscriptions for a disconnected client.
#[derive(Debug)]
pub struct ClientDisconnectResult {
    /// All contracts where this client had a subscription (for cleanup).
    pub affected_contracts: Vec<ContractKey>,
}

/// Result of subscribing to a contract.
#[derive(Debug)]
#[allow(dead_code)] // Fields available for future telemetry/diagnostics
pub struct SubscribeResult {
    /// Whether this is a new subscription (vs renewal).
    pub is_new: bool,
    /// When the subscription will expire.
    pub expires_at: Instant,
}

// =============================================================================
// HostingManager
// =============================================================================

/// Manages contract hosting and subscription state.
///
/// # Subscription Model
///
/// Subscriptions are lease-based with automatic expiry:
/// - `subscribe()` creates or renews a subscription with a lease
/// - Subscriptions expire after `SUBSCRIPTION_LEASE_DURATION` (8 minutes)
/// - Clients must call `renew_subscription()` every `SUBSCRIPTION_RENEWAL_INTERVAL` (2 minutes)
/// - Expired subscriptions are removed by `expire_stale_subscriptions()`
///
/// # Hosting Model
///
/// Contracts are hosted based on access patterns:
/// - GET, PUT, SUBSCRIBE operations add contracts to the hosting cache
/// - Contracts with client or active subscriptions get renewal
/// - Active subscriptions and client subscriptions prevent eviction
/// - TTL protects recently accessed contracts from premature eviction
pub(crate) struct HostingManager {
    /// Active subscriptions with lease expiry timestamps.
    /// Key: ContractKey, Value: expiry timestamp
    active_subscriptions: DashMap<ContractKey, Instant>,

    /// Contracts where a local client (WebSocket) is actively subscribed.
    /// Prevents hosting cache eviction while client subscriptions exist.
    client_subscriptions: DashMap<ContractInstanceId, HashSet<crate::client_events::ClientId>>,

    /// Unified hosting cache with byte-budget LRU and TTL protection.
    /// This is the single source of truth for which contracts we're hosting.
    hosting_cache: RwLock<HostingCache<InstantTimeSrc>>,

    /// Downstream peers subscribed to contracts we host, with lease timestamps.
    /// Drives `should_unsubscribe_upstream()` decisions.
    ///
    /// Must be kept in sync with `InterestManager::interested_peers`
    /// (see `InterestManager` docs for the dual-tracking relationship).
    downstream_subscribers: DashMap<ContractKey, HashMap<PeerKey, Instant>>,

    /// Time source for downstream subscriber lease tracking.
    time_source: InstantTimeSrc,

    /// Contracts with subscription requests currently in-flight.
    pending_subscription_requests: DashSet<ContractKey>,

    /// Exponential backoff state for subscription retries.
    subscription_backoff: RwLock<TrackedBackoff<ContractKey>>,

    /// Storage reference for persisting/removing hosting metadata.
    /// Set after executor creation via `set_storage()`.
    #[cfg(feature = "redb")]
    storage: RwLock<Option<crate::contract::storages::Storage>>,
    #[cfg(all(feature = "sqlite", not(feature = "redb")))]
    storage: RwLock<Option<crate::contract::storages::Storage>>,
}

impl HostingManager {
    pub fn new() -> Self {
        let backoff_config =
            ExponentialBackoff::new(INITIAL_SUBSCRIPTION_BACKOFF, MAX_SUBSCRIPTION_BACKOFF);
        Self {
            active_subscriptions: DashMap::new(),
            client_subscriptions: DashMap::new(),
            hosting_cache: RwLock::new(HostingCache::new(
                DEFAULT_HOSTING_BUDGET_BYTES,
                DEFAULT_MIN_TTL,
                InstantTimeSrc::new(),
            )),
            downstream_subscribers: DashMap::new(),
            time_source: InstantTimeSrc::new(),
            pending_subscription_requests: DashSet::new(),
            subscription_backoff: RwLock::new(TrackedBackoff::new(
                backoff_config,
                MAX_SUBSCRIPTION_BACKOFF_ENTRIES,
            )),
            storage: RwLock::new(None),
        }
    }

    /// Set the storage reference for persisting hosting metadata.
    /// Must be called after executor creation.
    pub fn set_storage(&self, storage: crate::contract::storages::Storage) {
        *self.storage.write() = Some(storage);
    }

    // =========================================================================
    // Subscription Management (Lease-Based)
    // =========================================================================

    /// Subscribe to a contract with a lease.
    ///
    /// Creates a new subscription or renews an existing one. The subscription
    /// will expire after `SUBSCRIPTION_LEASE_DURATION` unless renewed.
    pub fn subscribe(&self, contract: ContractKey) -> SubscribeResult {
        let expires_at = self.time_source.now() + SUBSCRIPTION_LEASE_DURATION;
        let is_new = self
            .active_subscriptions
            .insert(contract, expires_at)
            .is_none();

        debug!(
            %contract,
            is_new,
            expires_in_secs = SUBSCRIPTION_LEASE_DURATION.as_secs(),
            "subscribe: {} subscription",
            if is_new { "created" } else { "renewed" }
        );

        SubscribeResult { is_new, expires_at }
    }

    /// Renew an existing subscription.
    ///
    /// Extends the lease by `SUBSCRIPTION_LEASE_DURATION` from now.
    /// Returns `true` if the subscription existed and was renewed.
    #[allow(dead_code)] // Used in tests, may be used for explicit renewal in future
    pub fn renew_subscription(&self, contract: &ContractKey) -> bool {
        if let Some(mut entry) = self.active_subscriptions.get_mut(contract) {
            *entry = self.time_source.now() + SUBSCRIPTION_LEASE_DURATION;
            debug!(%contract, "renew_subscription: lease extended");
            true
        } else {
            debug!(%contract, "renew_subscription: no active subscription to renew");
            false
        }
    }

    /// Unsubscribe from a contract.
    ///
    /// Removes the active subscription. The contract may still be hosted
    /// (in the hosting cache) until evicted by LRU.
    pub fn unsubscribe(&self, contract: &ContractKey) {
        if self.active_subscriptions.remove(contract).is_some() {
            crate::node::network_status::record_subscription_removed(&format!("{contract}"));
            debug!(%contract, "unsubscribe: removed active subscription");
        }
    }

    /// Check if we have an active (non-expired) subscription to a contract.
    pub fn is_subscribed(&self, contract: &ContractKey) -> bool {
        self.active_subscriptions
            .get(contract)
            .map(|expires_at| *expires_at > self.time_source.now())
            .unwrap_or(false)
    }

    /// Get all contracts with active subscriptions.
    pub fn get_subscribed_contracts(&self) -> Vec<ContractKey> {
        let now = self.time_source.now();
        let mut contracts: Vec<ContractKey> = self
            .active_subscriptions
            .iter()
            .filter(|entry| *entry.value() > now)
            .map(|entry| *entry.key())
            .collect();
        // Sort for deterministic ordering (critical for simulation tests)
        contracts.sort_by(|a, b| a.id().as_bytes().cmp(b.id().as_bytes()));
        contracts
    }

    /// Expire stale subscriptions and return the contracts that were expired.
    ///
    /// Should be called periodically by a background task.
    /// Force-expire a contract's subscription so it gets renewed through the
    /// current best route on the next recovery cycle. Used when a new closer
    /// connection has been established (not just initiated).
    pub fn force_subscription_renewal(&self, contract: &ContractKey) {
        if self.active_subscriptions.remove(contract).is_some() {
            tracing::info!(
                %contract,
                "force_subscription_renewal: expired subscription to trigger re-route"
            );
        }
    }

    pub fn expire_stale_subscriptions(&self) -> Vec<ContractKey> {
        let now = self.time_source.now();
        let mut expired = Vec::new();

        // Collect expired subscriptions
        self.active_subscriptions.retain(|contract, expires_at| {
            if *expires_at <= now {
                expired.push(*contract);
                false
            } else {
                true
            }
        });

        if !expired.is_empty() {
            for contract in &expired {
                crate::node::network_status::record_subscription_removed(&format!("{contract}"));
            }
            info!(
                expired_count = expired.len(),
                "expire_stale_subscriptions: expired stale subscriptions"
            );
        }

        expired
    }

    /// Get the number of active subscriptions.
    #[allow(dead_code)] // Used in tests, may be used for metrics in future
    pub fn active_subscription_count(&self) -> usize {
        let now = self.time_source.now();
        self.active_subscriptions
            .iter()
            .filter(|entry| *entry.value() > now)
            .count()
    }

    // =========================================================================
    // Client Subscription Management
    // =========================================================================

    /// Register a client subscription for a contract (WebSocket client subscribed).
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
            is_first_client,
            "add_client_subscription: registered"
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
            no_more_subscriptions,
            "remove_client_subscription: removed"
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
    pub fn remove_client_from_all_subscriptions(
        &self,
        client_id: crate::client_events::ClientId,
    ) -> ClientDisconnectResult {
        let mut affected_contracts = Vec::new();

        // Find all contracts where this client is subscribed
        // Sort for deterministic iteration order
        let mut instance_ids_with_client: Vec<ContractInstanceId> = self
            .client_subscriptions
            .iter()
            .filter(|entry| entry.value().contains(&client_id))
            .map(|entry| *entry.key())
            .collect();
        instance_ids_with_client.sort_by(|a, b| a.as_bytes().cmp(b.as_bytes()));

        for instance_id in instance_ids_with_client {
            self.remove_client_subscription(&instance_id, client_id);

            // Find matching ContractKey in active_subscriptions
            if let Some(contract) = self
                .active_subscriptions
                .iter()
                .find(|entry| *entry.key().id() == instance_id)
                .map(|entry| *entry.key())
            {
                affected_contracts.push(contract);
            }
        }

        debug!(
            %client_id,
            affected_count = affected_contracts.len(),
            "remove_client_from_all_subscriptions: removed"
        );

        ClientDisconnectResult { affected_contracts }
    }

    // =========================================================================
    // Downstream Subscriber Tracking
    // =========================================================================

    /// Record that a downstream peer is subscribed to a contract we host.
    /// Returns false if the downstream subscriber limit for this contract has been reached
    /// and the peer is not already tracked (existing peers can always renew).
    pub fn add_downstream_subscriber(&self, contract: &ContractKey, peer: PeerKey) -> bool {
        let mut entry = self.downstream_subscribers.entry(*contract).or_default();
        if !entry.contains_key(&peer) && entry.len() >= MAX_DOWNSTREAM_SUBSCRIBERS_PER_CONTRACT {
            tracing::warn!(
                contract = %contract,
                limit = MAX_DOWNSTREAM_SUBSCRIBERS_PER_CONTRACT,
                "Downstream subscriber limit reached, rejecting peer"
            );
            return false;
        }
        entry.insert(peer, self.time_source.now());
        true
    }

    /// Renew a downstream peer's subscription lease.
    /// Returns false if the peer is not currently tracked.
    #[allow(dead_code)] // Only used in tests
    pub fn renew_downstream_subscriber(&self, contract: &ContractKey, peer: &PeerKey) -> bool {
        if let Some(mut peers) = self.downstream_subscribers.get_mut(contract) {
            if peers.contains_key(peer) {
                peers.insert(peer.clone(), self.time_source.now());
                return true;
            }
        }
        false
    }

    /// Remove a downstream peer's subscription for a contract.
    /// Returns true if the peer was found and removed.
    pub fn remove_downstream_subscriber(&self, contract: &ContractKey, peer: &PeerKey) -> bool {
        let mut removed = false;
        if let Some(mut peers) = self.downstream_subscribers.get_mut(contract) {
            removed = peers.remove(peer).is_some();
        }
        if removed {
            // Remove the map entry if no peers remain
            self.downstream_subscribers
                .remove_if(contract, |_, peers| peers.is_empty());
        }
        removed
    }

    /// Check whether any downstream peers are subscribed to this contract.
    pub fn has_downstream_subscribers(&self, contract: &ContractKey) -> bool {
        self.downstream_subscribers
            .get(contract)
            .is_some_and(|peers| !peers.is_empty())
    }

    /// Remove downstream subscribers whose leases have expired.
    /// Returns each affected contract paired with the number of expired peers.
    pub fn expire_stale_downstream_subscribers(&self) -> Vec<(ContractKey, usize)> {
        let now = self.time_source.now();
        let mut expired_counts = Vec::new();

        let keys: Vec<ContractKey> = self
            .downstream_subscribers
            .iter()
            .map(|entry| *entry.key())
            .collect();

        for key in keys {
            if let Some(mut peers) = self.downstream_subscribers.get_mut(&key) {
                let before = peers.len();
                peers.retain(|_, last_renewed| {
                    now.duration_since(*last_renewed) < SUBSCRIPTION_LEASE_DURATION
                });
                let expired = before - peers.len();
                if expired > 0 {
                    expired_counts.push((key, expired));
                }
                if peers.is_empty() {
                    drop(peers);
                    self.downstream_subscribers
                        .remove_if(&key, |_, peers| peers.is_empty());
                }
            }
        }

        expired_counts
    }

    /// Check if a contract has no local clients and no downstream subscribers,
    /// meaning we can safely unsubscribe upstream.
    pub fn should_unsubscribe_upstream(&self, contract: &ContractKey) -> bool {
        if self.has_client_subscriptions(contract.id()) {
            return false;
        }
        !self.has_downstream_subscribers(contract)
    }

    // =========================================================================
    // Hosting Cache Management
    // =========================================================================

    /// Record a contract access in the hosting cache.
    ///
    /// This is the main entry point for adding contracts to the hosting cache.
    /// Cached contracts are retained for durability (stale fallback) but only
    /// those with active interest (client subscriptions or downstream subscribers)
    /// will have their subscriptions renewed.
    ///
    /// Returns a `RecordAccessResult` containing:
    /// - `is_new`: Whether this contract was newly added (vs. refreshed existing)
    /// - `evicted`: Contracts that were evicted to make room
    ///
    /// Automatically persists hosting metadata for the accessed contract and
    /// removes persisted metadata for evicted contracts.
    pub fn record_contract_access(
        &self,
        key: ContractKey,
        size_bytes: u64,
        access_type: AccessType,
    ) -> RecordAccessResult {
        let result = self
            .hosting_cache
            .write()
            .record_access(key, size_bytes, access_type);

        // Persist hosting metadata for the accessed contract
        if let Some(storage) = self.storage.read().as_ref() {
            #[cfg(feature = "redb")]
            {
                use crate::contract::storages::HostingMetadata;
                let now_ms = std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_millis() as u64;
                let access_type_u8 = match access_type {
                    AccessType::Get => 0,
                    AccessType::Put => 1,
                    AccessType::Subscribe => 2,
                };
                let code_hash: [u8; 32] = **key.code_hash();
                let local_client = self.hosting_cache.read().has_local_client_access(&key);
                let metadata = HostingMetadata::new(
                    now_ms,
                    access_type_u8,
                    size_bytes,
                    code_hash,
                    local_client,
                );
                if let Err(e) = storage.store_hosting_metadata(&key, metadata) {
                    tracing::warn!(
                        contract = %key,
                        error = %e,
                        "Failed to persist hosting metadata for accessed contract"
                    );
                }
            }
            #[cfg(all(feature = "sqlite", not(feature = "redb")))]
            {
                // For sqlite, we can't easily run async from a sync context
                // The metadata is persisted via StateStorage::store() when state is stored
                tracing::trace!(
                    contract = %key,
                    "Sqlite hosting metadata update deferred to state store"
                );
            }

            // Clean up persisted metadata for evicted contracts
            for evicted_key in &result.evicted {
                #[cfg(feature = "redb")]
                {
                    if let Err(e) = storage.remove_hosting_metadata(evicted_key) {
                        tracing::warn!(
                            contract = %evicted_key,
                            error = %e,
                            "Failed to remove persisted hosting metadata for evicted contract"
                        );
                    }
                }
                #[cfg(all(feature = "sqlite", not(feature = "redb")))]
                {
                    tracing::debug!(
                        contract = %evicted_key,
                        "Evicted contract - sqlite metadata cleanup deferred"
                    );
                }
            }
        }

        result
    }

    /// Check if a contract is in the hosting cache.
    pub fn is_hosting_contract(&self, key: &ContractKey) -> bool {
        self.hosting_cache.read().contains(key)
    }

    /// Get all hosted contract keys.
    pub fn hosting_contract_keys(&self) -> Vec<ContractKey> {
        self.hosting_cache.read().iter().collect()
    }

    /// Get the cached state size in bytes for a hosted contract.
    pub fn hosting_contract_size(&self, key: &ContractKey) -> u64 {
        self.hosting_cache
            .read()
            .get(key)
            .map(|c| c.size_bytes)
            .unwrap_or(0)
    }

    /// Get the number of contracts in the hosting cache.
    pub fn hosting_contracts_count(&self) -> usize {
        self.hosting_cache.read().len()
    }

    /// Check if we should continue hosting a contract.
    ///
    /// Returns true if:
    /// - We have an active subscription, OR
    /// - We have client subscriptions, OR
    /// - The contract is in our hosting cache
    #[cfg(test)]
    pub fn should_host(&self, contract: &ContractKey) -> bool {
        self.is_subscribed(contract)
            || self.has_client_subscriptions(contract.id())
            || self.is_hosting_contract(contract)
    }

    /// Check if this node is actively receiving updates for a contract.
    ///
    /// Returns true only if we have an active network subscription or local
    /// client subscriptions — conditions that guarantee our cached state is
    /// kept fresh. Unlike [`should_host()`](Self::should_host), this excludes
    /// the hosting LRU cache, which can retain contracts after their
    /// subscriptions expire (leaving stale state).
    pub fn is_receiving_updates(&self, contract: &ContractKey) -> bool {
        self.is_subscribed(contract) || self.has_client_subscriptions(contract.id())
    }

    /// Mark a contract as accessed by a local client (HTTP/WebSocket).
    ///
    /// Only contracts with this flag get subscription renewal and trusted
    /// local-cache serving. Persists to disk so it survives restarts.
    pub fn mark_local_client_access(&self, key: &ContractKey) {
        let already_set = self.hosting_cache.read().has_local_client_access(key);

        // Always refresh the timestamp (keeps the age gate alive) even if
        // the flag is already set. Only skip disk persistence for the flag.
        self.hosting_cache.write().mark_local_client_access(key);

        if already_set {
            return;
        }

        // Persist the updated flag to disk
        if let Some(storage) = self.storage.read().as_ref() {
            #[cfg(feature = "redb")]
            {
                if let Ok(Some(mut metadata)) = storage.get_hosting_metadata(key) {
                    metadata.local_client_access = true;
                    if let Err(e) = storage.store_hosting_metadata(key, metadata) {
                        tracing::warn!(
                            contract = %key,
                            error = %e,
                            "Failed to persist local_client_access flag"
                        );
                    }
                }
            }
            #[cfg(all(feature = "sqlite", not(feature = "redb")))]
            {
                // Sqlite persistence is deferred to the next state store call,
                // which uses MAX() to preserve the flag (see store_hosting_metadata).
                tracing::trace!(
                    contract = %key,
                    "Sqlite local_client_access persistence deferred to state store"
                );
            }
        }

        debug!(%key, "Marked contract as locally accessed by client");
    }

    /// Check if a contract was accessed by a local client.
    pub fn has_local_client_access(&self, key: &ContractKey) -> bool {
        self.hosting_cache.read().has_local_client_access(key)
    }

    /// Touch a contract in the hosting cache (refresh TTL without adding).
    ///
    /// Called when a user GET serves a hosted contract from local cache.
    pub fn touch_hosting(&self, key: &ContractKey) {
        self.hosting_cache.write().touch(key);
    }

    /// Sweep for expired entries in the hosting cache.
    ///
    /// Contracts are protected from eviction if they have client subscriptions
    /// OR downstream subscribers (other peers relying on us for updates).
    /// The downstream subscriber exemption is time-bounded: stale entries are
    /// removed by `expire_stale_downstream_subscribers()` (called periodically)
    /// after `SUBSCRIPTION_LEASE_DURATION` without renewal.
    /// Automatically removes persisted metadata for expired contracts.
    pub fn sweep_expired_hosting(&self) -> Vec<ContractKey> {
        let expired = self.hosting_cache.write().sweep_expired(|key| {
            self.has_client_subscriptions(key.id()) || self.has_downstream_subscribers(key)
        });

        // Clean up persisted metadata for expired contracts
        if !expired.is_empty() {
            if let Some(storage) = self.storage.read().as_ref() {
                for expired_key in &expired {
                    #[cfg(feature = "redb")]
                    {
                        if let Err(e) = storage.remove_hosting_metadata(expired_key) {
                            tracing::warn!(
                                contract = %expired_key,
                                error = %e,
                                "Failed to remove persisted hosting metadata for expired contract"
                            );
                        }
                    }
                    #[cfg(all(feature = "sqlite", not(feature = "redb")))]
                    {
                        tracing::debug!(
                            contract = %expired_key,
                            "Expired contract - sqlite metadata cleanup deferred"
                        );
                    }
                }
            }
        }

        expired
    }

    // =========================================================================
    // Subscription Retry Management (Backoff)
    // =========================================================================

    /// Check if a subscription request can be made for a contract.
    /// Returns false if request is already pending or in backoff period.
    pub fn can_request_subscription(&self, contract: &ContractKey) -> bool {
        if self.pending_subscription_requests.contains(contract) {
            return false;
        }
        !self.subscription_backoff.read().is_in_backoff(contract)
    }

    /// Mark a subscription request as in-flight.
    /// Returns false if already pending.
    pub fn mark_subscription_pending(&self, contract: ContractKey) -> bool {
        if self.pending_subscription_requests.contains(&contract) {
            return false;
        }
        self.pending_subscription_requests.insert(contract);
        true
    }

    /// Mark a subscription request as completed.
    /// If success is false, applies exponential backoff.
    pub fn complete_subscription_request(&self, contract: &ContractKey, success: bool) {
        self.pending_subscription_requests.remove(contract);
        if success {
            self.subscription_backoff.write().record_success(contract);
        } else {
            self.subscription_backoff.write().record_failure(*contract);
        }
    }

    // =========================================================================
    // Introspection / Telemetry
    // =========================================================================

    /// Get subscription state for all contracts (for telemetry).
    ///
    /// Returns: (contract, has_client_subscription, is_active_subscription, expires_at)
    pub fn get_subscription_states(&self) -> Vec<(ContractKey, bool, bool, Option<Instant>)> {
        let now = self.time_source.now();
        let mut states: Vec<_> = self
            .active_subscriptions
            .iter()
            .map(|entry| {
                let contract = *entry.key();
                let expires_at = *entry.value();
                let is_active = expires_at > now;
                let has_client = self.has_client_subscriptions(contract.id());
                (contract, has_client, is_active, Some(expires_at))
            })
            .collect();
        // Sort by contract key for deterministic ordering (critical for simulation tests)
        states.sort_by(|(a, _, _, _), (b, _, _, _)| a.id().as_bytes().cmp(b.id().as_bytes()));
        states
    }

    /// Get contracts that need subscription renewal.
    ///
    /// Returns contracts where:
    /// - We have an active subscription that will expire soon, OR
    /// - We have client subscriptions but no active network subscription
    ///
    /// Hosted contracts without active interest (no client subscriptions,
    /// no downstream subscribers) are intentionally NOT renewed. Contracts
    /// persisted to disk are kept as a recovery mechanism (last-resort PUT
    /// if the contract is lost from the network) but are not actively
    /// subscribed to avoid subscription accumulation.
    pub fn contracts_needing_renewal(&self) -> Vec<ContractKey> {
        let now = self.time_source.now();
        let renewal_threshold = now + SUBSCRIPTION_RENEWAL_INTERVAL;

        // Use HashSet for O(1) deduplication instead of O(n) Vec::contains
        let mut needs_renewal_set = HashSet::new();

        // 1. Contracts with soon-to-expire subscriptions
        // Collect and sort for deterministic iteration order
        let mut active_subs: Vec<_> = self
            .active_subscriptions
            .iter()
            .map(|entry| (*entry.key(), *entry.value()))
            .collect();
        active_subs.sort_by(|(a, _), (b, _)| a.id().as_bytes().cmp(b.id().as_bytes()));

        for (key, expires_at) in active_subs {
            if expires_at <= renewal_threshold && expires_at > now {
                needs_renewal_set.insert(key);
            }
        }

        // 2. Contracts with client subscriptions but no active network subscription
        // Collect and sort for deterministic iteration order
        let mut client_instance_ids: Vec<_> =
            self.client_subscriptions.iter().map(|e| *e.key()).collect();
        client_instance_ids.sort_by(|a, b| a.as_bytes().cmp(b.as_bytes()));

        for instance_id in client_instance_ids {
            // Find if we have an active subscription for this contract
            let has_active = self
                .active_subscriptions
                .iter()
                .any(|sub| sub.key().id() == &instance_id && *sub.value() > now);
            if !has_active {
                // Need to find the ContractKey - check hosting cache
                if let Some(contract) = self
                    .hosting_cache
                    .read()
                    .iter()
                    .find(|k| k.id() == &instance_id)
                {
                    needs_renewal_set.insert(contract);
                }
            }
        }

        // 3. Locally-accessed hosted contracts without active subscription.
        // Only contracts recently marked by local clients are renewed (#3769);
        // relay-cached contracts are excluded to prevent storms (#3763).
        // The age gate (SUBSCRIPTION_LEASE_DURATION) ensures contracts stop
        // being renewed if the local user hasn't accessed them recently,
        // satisfying the cleanup exemption rule (AGENTS.md).
        {
            let cache = self.hosting_cache.read();
            let now = self.time_source.now();
            for key in cache.iter() {
                if cache.has_recent_local_client_access(&key, SUBSCRIPTION_LEASE_DURATION)
                    && !self
                        .active_subscriptions
                        .get(&key)
                        .map(|e| *e > now)
                        .unwrap_or(false)
                {
                    needs_renewal_set.insert(key);
                }
            }
        }

        // Convert set to vec and sort for deterministic return order
        let mut result: Vec<ContractKey> = needs_renewal_set.into_iter().collect();
        result.sort_by(|a, b| a.id().as_bytes().cmp(b.id().as_bytes()));
        result
    }

    // =========================================================================
    // Topology Snapshot (for telemetry/visualization)
    // =========================================================================

    /// Generate a topology snapshot for this peer.
    ///
    /// In the simplified lease-based model (2026-01 refactor), we don't track
    /// upstream/downstream relationships. The snapshot shows which contracts
    /// we're hosting and which have client subscriptions.
    #[allow(dead_code)] // Called from Ring methods that may be behind feature gates
    pub fn generate_topology_snapshot(
        &self,
        peer_addr: std::net::SocketAddr,
        location: f64,
    ) -> super::topology_registry::TopologySnapshot {
        use super::topology_registry::{ContractSubscription, TopologySnapshot};

        let mut snapshot = TopologySnapshot::new(peer_addr, location);
        let now = self.time_source.now();

        // Add all hosted contracts
        // Collect and sort for deterministic iteration order
        let hosting_cache = self.hosting_cache.read();
        let mut hosted_contracts: Vec<_> = hosting_cache.iter().collect();
        hosted_contracts.sort_by(|a, b| a.id().as_bytes().cmp(b.id().as_bytes()));

        for contract_key in hosted_contracts {
            let has_client_subscriptions =
                self.client_subscriptions.contains_key(contract_key.id());

            snapshot.set_contract(
                *contract_key.id(),
                ContractSubscription {
                    contract_key,
                    upstream: None,     // No upstream tracking in lease-based model
                    downstream: vec![], // No downstream tracking in lease-based model
                    is_hosting: true,
                    has_client_subscriptions,
                },
            );
        }

        // Add subscribed contracts that might not be in hosting cache yet
        // Collect and sort for deterministic iteration order
        let mut active_subs: Vec<_> = self
            .active_subscriptions
            .iter()
            .map(|entry| (*entry.key(), *entry.value()))
            .collect();
        active_subs.sort_by(|(a, _), (b, _)| a.id().as_bytes().cmp(b.id().as_bytes()));

        for (contract_key, expires_at) in active_subs {
            if expires_at > now && !hosting_cache.contains(&contract_key) {
                let has_client_subscriptions =
                    self.client_subscriptions.contains_key(contract_key.id());

                snapshot.set_contract(
                    *contract_key.id(),
                    ContractSubscription {
                        contract_key,
                        upstream: None,
                        downstream: vec![],
                        is_hosting: false,
                        has_client_subscriptions,
                    },
                );
            }
        }

        // Use GlobalSimulationTime for deterministic timestamps in simulation tests
        snapshot.timestamp_nanos =
            crate::config::GlobalSimulationTime::current_time_ms() * 1_000_000;

        snapshot
    }
}

// =============================================================================
// Persistence Methods
// =============================================================================

impl HostingManager {
    /// Load hosting metadata from storage during startup.
    ///
    /// This restores the hosting cache from persisted data, allowing the peer
    /// to continue hosting contracts after a restart without losing LRU state.
    ///
    /// Also migrates legacy contracts that have state but no hosting metadata.
    /// This is critical for network upgrades - without migration, all peers would
    /// "forget" legacy contracts after upgrading.
    ///
    /// # Arguments
    /// * `storage` - The storage backend (ReDb or SqlitePool)
    /// * `code_hash_lookup` - Function to look up CodeHash from ContractInstanceId.
    ///   Uses ContractStore which has the id->code_hash mapping.
    ///
    /// # Returns
    /// The number of contracts loaded from storage (including migrated legacy contracts).
    #[cfg(feature = "redb")]
    pub fn load_from_storage<F>(
        &self,
        storage: &crate::contract::storages::Storage,
        code_hash_lookup: F,
    ) -> Result<usize, redb::Error>
    where
        F: Fn(&ContractInstanceId) -> Option<freenet_stdlib::prelude::CodeHash>,
    {
        use freenet_stdlib::prelude::{CodeHash, ContractInstanceId, ContractKey};
        use std::collections::HashSet;

        let metadata_entries = storage.load_all_hosting_metadata()?;
        let now_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64;

        let mut cache = self.hosting_cache.write();
        let mut loaded = 0;

        // Track which instance IDs we've loaded (for legacy detection)
        let mut loaded_instance_ids: HashSet<[u8; 32]> = HashSet::new();

        for (key_bytes, metadata) in metadata_entries {
            // Reconstruct ContractKey from instance ID bytes and code hash from metadata
            // key_bytes contains the ContractInstanceId (32 bytes)
            // metadata.code_hash contains the CodeHash (32 bytes)
            if key_bytes.len() == 32 {
                let mut instance_id_bytes = [0u8; 32];
                instance_id_bytes.copy_from_slice(&key_bytes);
                loaded_instance_ids.insert(instance_id_bytes);
                let instance_id = ContractInstanceId::new(instance_id_bytes);
                let code_hash = CodeHash::new(metadata.code_hash);
                let key = ContractKey::from_id_and_code(instance_id, code_hash);

                let access_type = match metadata.access_type {
                    1 => cache::AccessType::Put,
                    2 => cache::AccessType::Subscribe,
                    _ => cache::AccessType::Get,
                };

                // Calculate age from persisted timestamp
                let age_ms = now_ms.saturating_sub(metadata.last_access_ms);
                let age = std::time::Duration::from_millis(age_ms);

                cache.load_persisted_entry(
                    key,
                    metadata.size_bytes,
                    access_type,
                    age,
                    metadata.local_client_access,
                );
                loaded += 1;
            }
        }

        // Migrate legacy contracts: contracts in states table but without hosting metadata
        // This ensures the network doesn't "forget" contracts after upgrading
        let all_state_keys = storage.iter_all_state_keys().unwrap_or_default();
        let mut migrated = 0;
        let mut migration_failures = 0;

        for key_bytes in all_state_keys {
            if key_bytes.len() != 32 {
                continue;
            }

            let mut instance_id_bytes = [0u8; 32];
            instance_id_bytes.copy_from_slice(&key_bytes);

            // Skip if already loaded with metadata
            if loaded_instance_ids.contains(&instance_id_bytes) {
                continue;
            }

            // Legacy contract: has state but no hosting metadata
            let instance_id = ContractInstanceId::new(instance_id_bytes);

            // Look up code_hash from ContractStore
            if let Some(code_hash) = code_hash_lookup(&instance_id) {
                let key = ContractKey::from_id_and_code(instance_id, code_hash);

                // Get state size for the hosting cache
                let size_bytes = storage.get_state_size(&key).unwrap_or(Some(0)).unwrap_or(0);

                // Legacy contracts don't have local_client_access info
                cache.load_persisted_entry(
                    key,
                    size_bytes,
                    cache::AccessType::Get,
                    std::time::Duration::ZERO,
                    false,
                );

                // Persist hosting metadata so future restarts don't need migration
                let code_hash_bytes: [u8; 32] = *code_hash;
                let metadata = crate::contract::storages::HostingMetadata::new(
                    now_ms,
                    0, // GET access type
                    size_bytes,
                    code_hash_bytes,
                    false,
                );
                if let Err(e) = storage.store_hosting_metadata(&key, metadata) {
                    tracing::warn!(
                        contract = %key,
                        error = %e,
                        "Failed to persist hosting metadata for migrated legacy contract"
                    );
                }

                migrated += 1;
            } else {
                // ContractStore doesn't know about this contract
                // This shouldn't happen normally - means WASM code is missing
                migration_failures += 1;
                tracing::warn!(
                    instance_id = %instance_id,
                    "Legacy contract has state but no WASM code - cannot migrate"
                );
            }
        }

        // Sort LRU order by last_accessed time
        cache.finalize_loading();

        let total_loaded = loaded + migrated;

        if migrated > 0 || migration_failures > 0 {
            tracing::info!(
                loaded_with_metadata = loaded,
                migrated_legacy = migrated,
                migration_failures = migration_failures,
                total_contracts = total_loaded,
                total_bytes = cache.current_bytes(),
                "Loaded hosting cache from storage (with legacy migration)"
            );
        } else {
            tracing::info!(
                loaded_contracts = total_loaded,
                total_bytes = cache.current_bytes(),
                "Loaded hosting cache from storage"
            );
        }

        Ok(total_loaded)
    }

    /// Load hosting metadata from storage during startup (sqlite version).
    ///
    /// Also migrates legacy contracts that have state but no hosting metadata.
    #[cfg(all(feature = "sqlite", not(feature = "redb")))]
    pub async fn load_from_storage<F>(
        &self,
        storage: &crate::contract::storages::Storage,
        code_hash_lookup: F,
    ) -> Result<usize, crate::contract::storages::sqlite::SqlDbError>
    where
        F: Fn(&ContractInstanceId) -> Option<freenet_stdlib::prelude::CodeHash>,
    {
        use freenet_stdlib::prelude::{CodeHash, ContractInstanceId, ContractKey};
        use std::collections::HashSet;

        let metadata_entries = storage.load_all_hosting_metadata().await?;
        let now_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64;

        let mut cache = self.hosting_cache.write();
        let mut loaded = 0;

        // Track which instance IDs we've loaded (for legacy detection)
        let mut loaded_instance_ids: HashSet<[u8; 32]> = HashSet::new();

        for (key_bytes, metadata) in metadata_entries {
            // Reconstruct ContractKey from instance ID bytes and code hash from metadata
            // key_bytes contains the ContractInstanceId (32 bytes)
            // metadata.code_hash contains the CodeHash (32 bytes)
            if key_bytes.len() == 32 {
                let mut instance_id_bytes = [0u8; 32];
                instance_id_bytes.copy_from_slice(&key_bytes);
                loaded_instance_ids.insert(instance_id_bytes);
                let instance_id = ContractInstanceId::new(instance_id_bytes);
                let code_hash = CodeHash::new(metadata.code_hash);
                let key = ContractKey::from_id_and_code(instance_id, code_hash);

                let access_type = match metadata.access_type {
                    1 => cache::AccessType::Put,
                    2 => cache::AccessType::Subscribe,
                    _ => cache::AccessType::Get,
                };

                // Calculate age from persisted timestamp
                let age_ms = now_ms.saturating_sub(metadata.last_access_ms);
                let age = std::time::Duration::from_millis(age_ms);

                cache.load_persisted_entry(
                    key,
                    metadata.size_bytes,
                    access_type,
                    age,
                    metadata.local_client_access,
                );
                loaded += 1;
            }
        }

        // Migrate legacy contracts: contracts in states table but without hosting metadata
        let all_state_keys = storage.iter_all_state_keys().await.unwrap_or_default();
        let mut migrated = 0;
        let mut migration_failures = 0;

        for key_bytes in all_state_keys {
            if key_bytes.len() != 32 {
                continue;
            }

            let mut instance_id_bytes = [0u8; 32];
            instance_id_bytes.copy_from_slice(&key_bytes);

            // Skip if already loaded with metadata
            if loaded_instance_ids.contains(&instance_id_bytes) {
                continue;
            }

            // Legacy contract: has state but no hosting metadata
            let instance_id = ContractInstanceId::new(instance_id_bytes);

            // Look up code_hash from ContractStore
            if let Some(code_hash) = code_hash_lookup(&instance_id) {
                let key = ContractKey::from_id_and_code(instance_id, code_hash);

                // Get state size for the hosting cache
                let size_bytes = storage
                    .get_state_size(&key)
                    .await
                    .unwrap_or(Some(0))
                    .unwrap_or(0);

                cache.load_persisted_entry(
                    key,
                    size_bytes,
                    cache::AccessType::Get,
                    std::time::Duration::ZERO,
                    false,
                );

                // Persist hosting metadata so future restarts don't need migration
                let code_hash_bytes: [u8; 32] = *code_hash;
                let metadata = crate::contract::storages::sqlite::HostingMetadata::new(
                    now_ms,
                    0, // GET access type
                    size_bytes,
                    code_hash_bytes,
                    false,
                );
                if let Err(e) = storage.store_hosting_metadata(&key, metadata).await {
                    tracing::warn!(
                        contract = %key,
                        error = %e,
                        "Failed to persist hosting metadata for migrated legacy contract"
                    );
                }

                migrated += 1;
            } else {
                migration_failures += 1;
                tracing::warn!(
                    instance_id = %instance_id,
                    "Legacy contract has state but no WASM code - cannot migrate"
                );
            }
        }

        // Sort LRU order by last_accessed time
        cache.finalize_loading();

        let total_loaded = loaded + migrated;

        if migrated > 0 || migration_failures > 0 {
            tracing::info!(
                loaded_with_metadata = loaded,
                migrated_legacy = migrated,
                migration_failures = migration_failures,
                total_contracts = total_loaded,
                total_bytes = cache.current_bytes(),
                "Loaded hosting cache from storage (with legacy migration)"
            );
        } else {
            tracing::info!(
                loaded_contracts = total_loaded,
                total_bytes = cache.current_bytes(),
                "Loaded hosting cache from storage"
            );
        }

        Ok(total_loaded)
    }
}

impl Default for HostingManager {
    fn default() -> Self {
        Self::new()
    }
}

// =============================================================================
// Tests
// =============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use freenet_stdlib::prelude::CodeHash;

    fn make_contract_key(seed: u8) -> ContractKey {
        ContractKey::from_id_and_code(
            ContractInstanceId::new([seed; 32]),
            CodeHash::new([seed.wrapping_add(1); 32]),
        )
    }

    #[tokio::test]
    async fn test_subscribe_creates_new_subscription() {
        let manager = HostingManager::new();
        let contract = make_contract_key(1);

        let result = manager.subscribe(contract);

        assert!(result.is_new);
        assert!(manager.is_subscribed(&contract));
    }

    #[tokio::test]
    async fn test_subscribe_renews_existing() {
        let manager = HostingManager::new();
        let contract = make_contract_key(1);

        let first = manager.subscribe(contract);
        let second = manager.subscribe(contract);

        assert!(first.is_new);
        assert!(!second.is_new);
        assert!(second.expires_at >= first.expires_at);
    }

    #[tokio::test]
    async fn test_unsubscribe_removes_subscription() {
        let manager = HostingManager::new();
        let contract = make_contract_key(1);

        manager.subscribe(contract);
        assert!(manager.is_subscribed(&contract));

        manager.unsubscribe(&contract);
        assert!(!manager.is_subscribed(&contract));
    }

    #[tokio::test]
    async fn test_renew_subscription() {
        let manager = HostingManager::new();
        let contract = make_contract_key(1);

        // Renew non-existent subscription fails
        assert!(!manager.renew_subscription(&contract));

        // Subscribe then renew succeeds
        manager.subscribe(contract);
        assert!(manager.renew_subscription(&contract));
    }

    #[tokio::test]
    async fn test_get_subscribed_contracts() {
        let manager = HostingManager::new();
        let c1 = make_contract_key(1);
        let c2 = make_contract_key(2);
        let c3 = make_contract_key(3);

        manager.subscribe(c1);
        manager.subscribe(c2);
        manager.subscribe(c3);
        manager.unsubscribe(&c2);

        let subscribed = manager.get_subscribed_contracts();
        assert_eq!(subscribed.len(), 2);
        assert!(subscribed.contains(&c1));
        assert!(!subscribed.contains(&c2));
        assert!(subscribed.contains(&c3));
    }

    #[tokio::test]
    async fn test_active_subscription_count() {
        let manager = HostingManager::new();

        assert_eq!(manager.active_subscription_count(), 0);

        manager.subscribe(make_contract_key(1));
        manager.subscribe(make_contract_key(2));
        assert_eq!(manager.active_subscription_count(), 2);

        manager.unsubscribe(&make_contract_key(1));
        assert_eq!(manager.active_subscription_count(), 1);
    }

    #[test]
    fn test_client_subscription_basic() {
        let manager = HostingManager::new();
        let instance_id = ContractInstanceId::new([1; 32]);
        let client_id = crate::client_events::ClientId::next();

        let result = manager.add_client_subscription(&instance_id, client_id);
        assert!(result.is_first_client);
        assert!(manager.has_client_subscriptions(&instance_id));

        let is_last = manager.remove_client_subscription(&instance_id, client_id);
        assert!(is_last);
        assert!(!manager.has_client_subscriptions(&instance_id));
    }

    #[test]
    fn test_client_subscription_multiple_clients() {
        let manager = HostingManager::new();
        let instance_id = ContractInstanceId::new([1; 32]);
        let client1 = crate::client_events::ClientId::next();
        let client2 = crate::client_events::ClientId::next();

        let r1 = manager.add_client_subscription(&instance_id, client1);
        let r2 = manager.add_client_subscription(&instance_id, client2);

        assert!(r1.is_first_client);
        assert!(!r2.is_first_client);

        let is_last1 = manager.remove_client_subscription(&instance_id, client1);
        assert!(!is_last1); // client2 still subscribed

        let is_last2 = manager.remove_client_subscription(&instance_id, client2);
        assert!(is_last2);
    }

    #[test]
    fn test_hosting_cache_basic() {
        let manager = HostingManager::new();
        let key = make_contract_key(1);

        assert!(!manager.is_hosting_contract(&key));
        assert_eq!(manager.hosting_contracts_count(), 0);

        manager.record_contract_access(key, 1000, AccessType::Put);

        assert!(manager.is_hosting_contract(&key));
        assert_eq!(manager.hosting_contracts_count(), 1);
    }

    #[test]
    fn test_subscription_backoff() {
        let manager = HostingManager::new();
        let contract = make_contract_key(1);

        // Initially can request
        assert!(manager.can_request_subscription(&contract));

        // Mark pending
        assert!(manager.mark_subscription_pending(contract));

        // Can't request while pending
        assert!(!manager.can_request_subscription(&contract));

        // Complete with failure
        manager.complete_subscription_request(&contract, false);

        // Now in backoff - can't request immediately
        assert!(!manager.can_request_subscription(&contract));
    }

    #[test]
    fn test_should_host() {
        let manager = HostingManager::new();
        let contract = make_contract_key(1);

        // Not hosting initially
        assert!(!manager.should_host(&contract));

        // Add to hosting cache
        manager.record_contract_access(contract, 1000, AccessType::Put);
        assert!(manager.should_host(&contract));
    }

    /// Regression test for #3546: hosted-only contracts must NOT be in the
    /// renewal list. Including them caused subscription storms (#3763 incident).
    #[test]
    fn test_hosted_contract_not_in_renewal_after_restart() {
        let manager = HostingManager::new();
        let contract = make_contract_key(42);
        manager.record_contract_access(contract, 1000, AccessType::Get);
        assert!(manager.is_hosting_contract(&contract));
        assert!(
            manager.contracts_needing_renewal().is_empty(),
            "Hosted-only contract must NOT be in renewal list"
        );
    }

    /// Regression test for #3340: is_receiving_updates must return false when
    /// a contract is only in the hosting LRU cache (no active subscription).
    #[test]
    fn test_is_receiving_updates_excludes_hosting_cache_only() {
        let manager = HostingManager::new();
        let contract = make_contract_key(1);

        // Not receiving updates initially
        assert!(!manager.is_receiving_updates(&contract));

        // Add to hosting cache only — should_host true, is_receiving_updates false
        manager.record_contract_access(contract, 1000, AccessType::Put);
        assert!(manager.should_host(&contract));
        assert!(
            !manager.is_receiving_updates(&contract),
            "Hosting cache alone should NOT count as receiving updates"
        );

        // Add active subscription — now is_receiving_updates should be true
        manager.subscribe(contract);
        assert!(manager.is_receiving_updates(&contract));
    }

    /// Regression test for #3340: is_receiving_updates with client subscriptions.
    #[test]
    fn test_is_receiving_updates_with_client_subscription() {
        let manager = HostingManager::new();
        let contract = make_contract_key(1);
        let client_id = crate::client_events::ClientId::next();

        assert!(!manager.is_receiving_updates(&contract));

        manager.add_client_subscription(contract.id(), client_id);
        assert!(manager.is_receiving_updates(&contract));
    }

    #[test]
    fn test_contracts_needing_renewal_excludes_hosted_only() {
        let manager = HostingManager::new();
        let contract = make_contract_key(1);

        // Add to hosting cache (simulating GET operation)
        manager.record_contract_access(contract, 1000, AccessType::Get);

        // Hosted-only contracts should NOT be renewed -- subscribing to all
        // hosted contracts causes subscription storms (#3546). The local
        // cache shortcut (#3761) handles same-session freshness, and the
        // subscription piggyback (#3762) handles post-GET subscription.
        let needs_renewal = manager.contracts_needing_renewal();
        assert!(
            !needs_renewal.contains(&contract),
            "Hosted-only contract should NOT be in renewal list"
        );
    }

    // Removed: test_contracts_needing_renewal_includes_hosted was added in #3763
    // but caused subscription storms. Hosted-only contracts must NOT be renewed.
    // The exclusion test (test_contracts_needing_renewal_excludes_hosted_only)
    // covers the correct behavior.

    // Superseded: startup revalidation window removed in #3546 to prevent
    // subscription accumulation storms. Hosted-only contracts are no longer
    // proactively renewed at startup. Replaced by test_hosted_contracts_not_renewed_at_scale.
    #[ignore]
    #[test]
    fn test_hosted_contract_renewed_despite_no_interest() {
        let manager = HostingManager::new();
        let contract = make_contract_key(42);
        manager.record_contract_access(contract, 1000, AccessType::Get);
        assert!(manager.is_hosting_contract(&contract));
        // Before #3546: contracts_needing_renewal() included this during startup window
        // After #3546: hosted-only contracts are never included
        let renewals = manager.contracts_needing_renewal();
        assert!(
            !renewals.contains(&contract),
            "Hosted contract should NOT be in renewal list (startup window removed in #3546)"
        );
    }

    // Superseded: startup revalidation window removed in #3546.
    // Hosted contracts loaded from disk are no longer auto-subscribed on startup.
    #[ignore]
    #[test]
    fn test_startup_revalidation_includes_hosted_contracts() {
        let manager = HostingManager::new();
        let contract = make_contract_key(1);
        manager.record_contract_access(contract, 1000, AccessType::Get);
        // Before #3546: during startup window, this would be in renewal list
        // After #3546: hosted-only contracts are never renewed
        let needs_renewal = manager.contracts_needing_renewal();
        assert!(
            !needs_renewal.contains(&contract),
            "Hosted contract should NOT be in renewal list (startup window removed in #3546)"
        );
    }

    // Superseded: startup revalidation window removed in #3546.
    #[ignore]
    #[test]
    fn test_startup_revalidation_skips_already_subscribed() {
        let manager = HostingManager::new();
        let contract = make_contract_key(1);
        manager.record_contract_access(contract, 1000, AccessType::Get);
        manager.subscribe(contract);
        let needs_renewal = manager.contracts_needing_renewal();
        assert!(
            !needs_renewal.contains(&contract),
            "Already-subscribed contract should not be in renewal list"
        );
    }

    // Superseded: startup revalidation window removed in #3546.
    #[ignore]
    #[test]
    fn test_startup_revalidation_window_expires() {
        let manager = HostingManager::new();
        let contract = make_contract_key(1);
        manager.record_contract_access(contract, 1000, AccessType::Get);
        let needs_renewal = manager.contracts_needing_renewal();
        assert!(
            !needs_renewal.contains(&contract),
            "Hosted-only contract should NOT be in renewal list"
        );
    }

    // Superseded: startup revalidation window removed in #3546.
    #[ignore]
    #[test]
    fn test_startup_revalidation_multiple_contracts() {
        let manager = HostingManager::new();
        let contract_a = make_contract_key(1);
        let contract_b = make_contract_key(2);
        let contract_c = make_contract_key(3);
        manager.record_contract_access(contract_a, 1000, AccessType::Get);
        manager.record_contract_access(contract_b, 1000, AccessType::Get);
        manager.record_contract_access(contract_c, 1000, AccessType::Get);
        manager.subscribe(contract_b);
        let client_id = crate::client_events::ClientId::next();
        manager.add_client_subscription(contract_c.id(), client_id);
        let needs_renewal = manager.contracts_needing_renewal();
        // Before #3546: contract_a would be included by startup window
        // After #3546: only contract_c (client subscription) is included
        assert!(
            !needs_renewal.contains(&contract_a),
            "Hosted-only contract_a should NOT be included (startup window removed)"
        );
        assert!(
            !needs_renewal.contains(&contract_b),
            "Subscribed contract_b should be excluded (not expiring soon)"
        );
        assert!(
            needs_renewal.contains(&contract_c),
            "Client-subscribed contract_c should be included"
        );
    }

    /// Verify that hosted contracts are included in renewal and the renewal
    /// system handles scale (200 hosted contracts). The batch limit in
    /// renew_subscriptions_task (MAX_RECOVERY_ATTEMPTS_PER_INTERVAL = 10)
    /// prevents subscription storms by processing at most 10 per cycle.
    #[test]
    fn test_hosted_contracts_not_renewed_at_scale() {
        let manager = HostingManager::new();

        // Simulate 200 relay-cached contracts loaded from disk
        for i in 0..200u8 {
            let contract = make_contract_key(i);
            manager.record_contract_access(contract, 1000, AccessType::Get);
        }
        assert_eq!(manager.hosting_contracts_count(), 200);

        // None should appear in renewal list -- subscribing to all hosted
        // contracts causes subscription storms (#3546, confirmed in #3763).
        let needs_renewal = manager.contracts_needing_renewal();
        assert!(
            needs_renewal.is_empty(),
            "200 hosted-only contracts should NOT be in renewal list, found {}",
            needs_renewal.len()
        );

        // Subscribe to exactly 2 (simulating River client)
        let client_id = crate::client_events::ClientId::next();
        let contract_a = make_contract_key(42);
        let contract_b = make_contract_key(99);
        manager.add_client_subscription(contract_a.id(), client_id);
        manager.add_client_subscription(contract_b.id(), client_id);

        // Only those 2 should need renewal
        let needs_renewal = manager.contracts_needing_renewal();
        assert_eq!(
            needs_renewal.len(),
            2,
            "Only 2 client-subscribed contracts should need renewal, found {}",
            needs_renewal.len()
        );
        assert!(needs_renewal.contains(&contract_a));
        assert!(needs_renewal.contains(&contract_b));
    }

    /// Validates that backoff constants are internally consistent.
    ///
    /// MAX_SUBSCRIPTION_BACKOFF must be shorter than SUBSCRIPTION_LEASE_DURATION,
    /// otherwise a contract at maximum backoff will have its subscription expire
    /// before the next retry — causing permanent subscription loss that only
    /// recovers when the orphan recovery sweep picks it up (up to 30s later).
    ///
    /// This test would have caught the original bug where MAX_SUBSCRIPTION_BACKOFF
    /// was 600s (10 min) but SUBSCRIPTION_LEASE_DURATION was only 480s (8 min).
    #[test]
    fn test_backoff_shorter_than_lease() {
        assert!(
            MAX_SUBSCRIPTION_BACKOFF < SUBSCRIPTION_LEASE_DURATION,
            "MAX_SUBSCRIPTION_BACKOFF ({:?}) must be shorter than \
             SUBSCRIPTION_LEASE_DURATION ({:?}), otherwise subscriptions \
             expire before retry",
            MAX_SUBSCRIPTION_BACKOFF,
            SUBSCRIPTION_LEASE_DURATION
        );
    }

    /// Validates that the full backoff sequence never exceeds the lease duration.
    /// Even after many consecutive failures, no single backoff delay should be
    /// long enough to let the subscription expire.
    #[test]
    fn test_backoff_sequence_within_lease() {
        let backoff =
            ExponentialBackoff::new(INITIAL_SUBSCRIPTION_BACKOFF, MAX_SUBSCRIPTION_BACKOFF);
        // Check delays for up to 10 consecutive failures
        for failures in 1..=10 {
            let delay = backoff.delay_for_failures(failures);
            assert!(
                delay < SUBSCRIPTION_LEASE_DURATION,
                "Backoff delay after {} failures ({:?}) exceeds lease ({:?})",
                failures,
                delay,
                SUBSCRIPTION_LEASE_DURATION
            );
        }
    }

    fn make_peer_key(seed: u8) -> PeerKey {
        PeerKey(crate::transport::TransportPublicKey::from_bytes([seed; 32]))
    }

    /// Test that should_unsubscribe_upstream returns true when contract is not
    /// tracked (simulates "contract not found" early return in the Unsubscribe handler).
    #[test]
    fn test_should_unsubscribe_upstream_unknown_contract() {
        let manager = HostingManager::new();
        let unknown_contract = make_contract_key(99);

        // Contract never added to any tracking structure
        assert!(
            manager.should_unsubscribe_upstream(&unknown_contract),
            "Unknown contract with no clients and no downstream should return true"
        );
        assert!(!manager.has_downstream_subscribers(&unknown_contract));
        assert!(!manager.has_client_subscriptions(unknown_contract.id()));
    }

    #[test]
    fn test_should_unsubscribe_upstream() {
        let manager = HostingManager::new();
        let contract = make_contract_key(1);
        let peer = make_peer_key(10);
        let client_id = crate::client_events::ClientId::next();

        // No clients, no downstream -> should unsubscribe
        assert!(manager.should_unsubscribe_upstream(&contract));

        // Add downstream subscriber -> should NOT unsubscribe
        manager.add_downstream_subscriber(&contract, peer.clone());
        assert!(!manager.should_unsubscribe_upstream(&contract));

        // Remove downstream -> should unsubscribe again
        manager.remove_downstream_subscriber(&contract, &peer);
        assert!(manager.should_unsubscribe_upstream(&contract));

        // Add client subscription -> should NOT unsubscribe
        manager.add_client_subscription(contract.id(), client_id);
        assert!(!manager.should_unsubscribe_upstream(&contract));
    }

    // =========================================================================
    // Upstream Unsubscribe Decision Logic Tests
    // =========================================================================

    /// Simulate chain propagation: downstream peer unsubscribes, node checks
    /// whether it should propagate the unsubscribe upstream.
    ///
    /// Scenario: A -> B -> C (subscription tree). C unsubscribes from B.
    /// B has no other downstream subscribers and no local clients, so B
    /// should propagate the unsubscribe to A.
    #[test]
    fn test_chain_propagation_single_downstream() {
        let manager = HostingManager::new();
        let contract = make_contract_key(10);
        let downstream_c = make_peer_key(30);

        // B is hosting the contract with C as the only downstream subscriber
        manager.subscribe(contract);
        manager.add_downstream_subscriber(&contract, downstream_c.clone());

        // C unsubscribes from B
        assert!(manager.remove_downstream_subscriber(&contract, &downstream_c));

        // B has no local clients and no remaining downstream -> should propagate
        assert!(
            manager.should_unsubscribe_upstream(&contract),
            "Node with no clients and no downstream should propagate unsubscribe upstream"
        );
    }

    /// Scenario: A -> B, C -> B. C unsubscribes, but A is still subscribed.
    /// B should NOT propagate upstream because A remains as a downstream subscriber.
    #[test]
    fn test_no_propagation_with_remaining_downstream() {
        let manager = HostingManager::new();
        let contract = make_contract_key(10);
        let downstream_a = make_peer_key(10);
        let downstream_c = make_peer_key(30);

        // B hosts contract with both A and C as downstream subscribers
        manager.subscribe(contract);
        manager.add_downstream_subscriber(&contract, downstream_a.clone());
        manager.add_downstream_subscriber(&contract, downstream_c.clone());

        // C unsubscribes
        assert!(manager.remove_downstream_subscriber(&contract, &downstream_c));

        // A is still subscribed -> should NOT propagate
        assert!(
            !manager.should_unsubscribe_upstream(&contract),
            "Node with remaining downstream should NOT propagate unsubscribe"
        );
    }

    /// Scenario: Local client still interested even after all downstream peers leave.
    /// Node should NOT propagate upstream because a local WebSocket client is subscribed.
    #[test]
    fn test_no_propagation_with_local_client() {
        let manager = HostingManager::new();
        let contract = make_contract_key(10);
        let downstream_peer = make_peer_key(10);
        let client_id = crate::client_events::ClientId::next();

        // Node has both a downstream subscriber and a local client
        manager.subscribe(contract);
        manager.add_downstream_subscriber(&contract, downstream_peer.clone());
        manager.add_client_subscription(contract.id(), client_id);

        // Downstream peer unsubscribes
        assert!(manager.remove_downstream_subscriber(&contract, &downstream_peer));

        // Local client still subscribed -> should NOT propagate
        assert!(
            !manager.should_unsubscribe_upstream(&contract),
            "Node with local client should NOT propagate unsubscribe even if downstream is empty"
        );
    }

    /// Simulate client disconnect: when a WebSocket client disconnects, check
    /// that affected contracts can be identified and the unsubscribe decision
    /// is correct.
    #[test]
    fn test_client_disconnect_triggers_unsubscribe_decision() {
        let manager = HostingManager::new();
        let contract = make_contract_key(10);
        let client_id = crate::client_events::ClientId::next();

        // Client subscribes to a contract (no downstream peers)
        manager.subscribe(contract);
        manager.add_client_subscription(contract.id(), client_id);

        // Client should prevent unsubscribe
        assert!(!manager.should_unsubscribe_upstream(&contract));

        // Client disconnects
        let result = manager.remove_client_from_all_subscriptions(client_id);
        assert_eq!(
            result.affected_contracts.len(),
            1,
            "Disconnect should report the affected contract"
        );
        assert_eq!(result.affected_contracts[0], contract);

        // Now with no client and no downstream -> should unsubscribe
        assert!(
            manager.should_unsubscribe_upstream(&contract),
            "After client disconnect with no downstream, should propagate unsubscribe"
        );
    }

    /// Simulate client disconnect with multiple contracts: only contracts with
    /// no remaining interest should trigger the unsubscribe decision.
    #[test]
    fn test_client_disconnect_partial_unsubscribe() {
        let manager = HostingManager::new();
        let contract_a = make_contract_key(10);
        let contract_b = make_contract_key(20);
        let client_id = crate::client_events::ClientId::next();
        let downstream_peer = make_peer_key(50);

        // Client subscribes to both contracts
        manager.subscribe(contract_a);
        manager.subscribe(contract_b);
        manager.add_client_subscription(contract_a.id(), client_id);
        manager.add_client_subscription(contract_b.id(), client_id);

        // contract_b also has a downstream subscriber
        manager.add_downstream_subscriber(&contract_b, downstream_peer.clone());

        // Client disconnects
        let result = manager.remove_client_from_all_subscriptions(client_id);
        assert_eq!(result.affected_contracts.len(), 2);

        // contract_a: no client, no downstream -> should unsubscribe
        assert!(
            manager.should_unsubscribe_upstream(&contract_a),
            "Contract with no remaining interest should trigger unsubscribe"
        );

        // contract_b: no client, but has downstream -> should NOT unsubscribe
        assert!(
            !manager.should_unsubscribe_upstream(&contract_b),
            "Contract with downstream subscribers should NOT trigger unsubscribe"
        );
    }

    /// Simulate downstream subscriber expiry triggering unsubscribe decisions.
    /// Uses manual timestamp manipulation via DashMap to simulate time passing.
    #[test]
    fn test_expire_downstream_triggers_unsubscribe_decision() {
        let manager = HostingManager::new();
        let contract = make_contract_key(10);
        let peer = make_peer_key(10);

        // Add a downstream subscriber
        manager.subscribe(contract);
        manager.add_downstream_subscriber(&contract, peer.clone());

        // Not expired yet -> should NOT unsubscribe
        assert!(!manager.should_unsubscribe_upstream(&contract));

        // Manually set the subscriber's lease to the past
        if let Some(mut peers) = manager.downstream_subscribers.get_mut(&contract) {
            peers.insert(
                peer.clone(),
                Instant::now() - SUBSCRIPTION_LEASE_DURATION - Duration::from_secs(1),
            );
        }

        // Run expiry sweep
        let expired = manager.expire_stale_downstream_subscribers();
        assert_eq!(
            expired.len(),
            1,
            "Should detect one contract with expired downstream"
        );
        assert_eq!(expired[0].0, contract);
        assert_eq!(expired[0].1, 1, "One peer should have expired");

        // Now should unsubscribe (no client, no downstream)
        assert!(
            manager.should_unsubscribe_upstream(&contract),
            "After all downstream subscribers expire, should propagate unsubscribe"
        );
    }

    /// Partial expiry: some downstream subscribers expire but others remain.
    /// Should NOT trigger unsubscribe.
    #[test]
    fn test_partial_downstream_expiry_no_unsubscribe() {
        let manager = HostingManager::new();
        let contract = make_contract_key(10);
        let stale_peer = make_peer_key(10);
        let fresh_peer = make_peer_key(20);

        // Add two downstream subscribers
        manager.subscribe(contract);
        manager.add_downstream_subscriber(&contract, stale_peer.clone());
        manager.add_downstream_subscriber(&contract, fresh_peer.clone());

        // Make one subscriber stale
        if let Some(mut peers) = manager.downstream_subscribers.get_mut(&contract) {
            peers.insert(
                stale_peer,
                Instant::now() - SUBSCRIPTION_LEASE_DURATION - Duration::from_secs(1),
            );
        }

        // Run expiry sweep - one stale peer expired but fresh peer remains
        let expired = manager.expire_stale_downstream_subscribers();
        assert_eq!(expired.len(), 1, "One contract had expired peers");
        assert_eq!(expired[0].0, contract);
        assert_eq!(expired[0].1, 1, "One peer should have expired");

        // fresh_peer still present -> should NOT unsubscribe
        assert!(
            !manager.should_unsubscribe_upstream(&contract),
            "Contract with remaining downstream should NOT trigger unsubscribe"
        );
    }

    // =========================================================================
    // Unsubscribe Handler Logic Tests
    // =========================================================================

    fn make_interest_manager() -> crate::ring::interest::InterestManager<InstantTimeSrc> {
        crate::ring::interest::InterestManager::new(InstantTimeSrc::new())
    }

    /// Contract found + peer resolved → removes both tracking structures,
    /// triggers upstream unsubscribe propagation.
    #[test]
    fn test_unsubscribe_handler_contract_found_peer_resolved() {
        let manager = HostingManager::new();
        let interest = make_interest_manager();
        let contract = make_contract_key(1);
        let peer = make_peer_key(10);

        manager.add_downstream_subscriber(&contract, peer.clone());
        interest.register_peer_interest(&contract, peer.clone(), None, true);
        assert!(!manager.should_unsubscribe_upstream(&contract));

        manager.remove_downstream_subscriber(&contract, &peer);
        interest.remove_peer_interest(&contract, &peer);

        assert!(!manager.has_downstream_subscribers(&contract));
        assert!(manager.should_unsubscribe_upstream(&contract));
    }

    /// Removing an unknown peer is a noop; existing entries remain intact.
    #[test]
    fn test_unsubscribe_handler_unknown_peer_is_noop() {
        let manager = HostingManager::new();
        let contract = make_contract_key(2);
        let known_peer = make_peer_key(20);
        let unknown_peer = make_peer_key(99);

        manager.add_downstream_subscriber(&contract, known_peer.clone());

        assert!(!manager.remove_downstream_subscriber(&contract, &unknown_peer));
        assert!(manager.has_downstream_subscribers(&contract));
        assert!(!manager.should_unsubscribe_upstream(&contract));
    }

    /// Removing from an untracked contract is a noop; other contracts unaffected.
    #[test]
    fn test_unsubscribe_handler_unknown_contract_is_noop() {
        let manager = HostingManager::new();
        let known_contract = make_contract_key(3);
        let unknown_contract = make_contract_key(99);
        let peer = make_peer_key(30);

        manager.add_downstream_subscriber(&known_contract, peer.clone());

        assert!(!manager.remove_downstream_subscriber(&unknown_contract, &peer));
        assert!(manager.has_downstream_subscribers(&known_contract));
        assert!(!manager.has_downstream_subscribers(&unknown_contract));
    }

    /// `downstream_subscribers` is authoritative for unsubscribe decisions,
    /// independent of `InterestManager` state.
    #[test]
    fn test_unsubscribe_dual_tracking_authority() {
        let manager = HostingManager::new();
        let interest = make_interest_manager();
        let contract = make_contract_key(4);
        let peer = make_peer_key(40);

        manager.add_downstream_subscriber(&contract, peer.clone());
        interest.register_peer_interest(&contract, peer.clone(), None, true);

        manager.remove_downstream_subscriber(&contract, &peer);

        assert!(manager.should_unsubscribe_upstream(&contract));
        // InterestManager still tracks the peer — independent of unsubscribe decision
        assert!(interest.remove_peer_interest(&contract, &peer));
    }

    /// A hosted contract with downstream subscribers must NOT be evicted
    /// from the hosting cache even after TTL expires and cache is over budget.
    /// Without this, interior peers would drop hosting → stop renewal → lose
    /// upstream subscription → downstream subscribers lose their feed.
    ///
    /// This test operates at the HostingCache level with MockTimeSrc so we
    /// can actually advance time past TTL and verify the retain predicate.
    #[test]
    fn test_downstream_subscribers_protect_from_eviction() {
        use crate::ring::hosting::cache::HostingCache;
        use crate::util::time_source::SharedMockTimeSource;

        let time = SharedMockTimeSource::new();
        let min_ttl = Duration::from_secs(60);
        // Budget of 150 bytes with 2x100-byte entries = over budget
        let mut cache = HostingCache::new(150, min_ttl, time.clone());

        let protected = make_contract_key(1);
        let unprotected = make_contract_key(2);

        cache.record_access(protected, 100, AccessType::Get);
        cache.record_access(unprotected, 100, AccessType::Get);
        assert_eq!(cache.current_bytes(), 200); // over budget

        // Advance past TTL
        time.advance_time(Duration::from_secs(61));

        // Sweep with predicate that protects the first contract
        // (simulates has_downstream_subscribers returning true)
        let evicted = cache.sweep_expired(|k| *k == protected);

        assert!(
            !evicted.contains(&protected),
            "Contract with downstream subscribers must not be evicted"
        );
        assert!(
            evicted.contains(&unprotected),
            "Unprotected contract should be evicted when over budget + past TTL"
        );
        assert!(cache.contains(&protected));
    }

    /// A hosted contract with NO subscribers and NO clients SHOULD be
    /// evictable after TTL expires when the cache is over budget.
    ///
    /// Uses HostingCache with MockTimeSrc for time advancement.
    #[test]
    fn test_no_subscribers_allows_eviction() {
        use crate::ring::hosting::cache::HostingCache;
        use crate::util::time_source::SharedMockTimeSource;

        let time = SharedMockTimeSource::new();
        let min_ttl = Duration::from_secs(60);
        // Budget of 80 bytes, entry is 100 → over budget immediately
        let mut cache = HostingCache::new(80, min_ttl, time.clone());

        let contract = make_contract_key(100);
        cache.record_access(contract, 100, AccessType::Get);
        assert!(cache.contains(&contract));

        // Under TTL: should not be evicted even though over budget
        let evicted = cache.sweep_expired(|_| false);
        assert!(
            evicted.is_empty(),
            "Contract within TTL should not be evicted"
        );

        // Advance past TTL
        time.advance_time(Duration::from_secs(61));

        // Now should be evicted (over budget + past TTL + no retain predicate)
        let evicted = cache.sweep_expired(|_| false);
        assert!(
            evicted.contains(&contract),
            "Contract past TTL with no subscribers should be evicted"
        );
        assert!(!cache.contains(&contract));
    }

    // =========================================================================
    // Downstream Subscriber Limit Tests
    // =========================================================================

    #[test]
    fn test_downstream_subscriber_limit_enforced() {
        let manager = HostingManager::new();
        let contract = make_contract_key(50);

        // Use a small limit for testing to avoid issues with peer key generation.
        // We test the limit logic by adding peers up to the constant and verifying rejection.
        let limit = MAX_DOWNSTREAM_SUBSCRIBERS_PER_CONTRACT;

        // Add `limit` peers — all should succeed
        let mut peers = Vec::with_capacity(limit);
        for i in 0..limit {
            let peer = PeerKey(crate::transport::TransportPublicKey::from_bytes({
                let mut bytes = [0u8; 32];
                // Encode index across 3 bytes for safety
                bytes[0] = (i & 0xFF) as u8;
                bytes[1] = ((i >> 8) & 0xFF) as u8;
                bytes[2] = ((i >> 16) & 0xFF) as u8;
                bytes
            }));
            peers.push(peer.clone());
            let result = manager.add_downstream_subscriber(&contract, peer);
            assert!(
                result,
                "Downstream subscriber {i} should succeed within limit (count before: {i})"
            );
        }

        // Verify the actual count
        let actual_count = manager
            .downstream_subscribers
            .get(&contract)
            .map(|e| e.len())
            .unwrap_or(0);
        assert_eq!(
            actual_count, limit,
            "Should have exactly {limit} entries, got {actual_count}"
        );

        // The next new peer (with completely different bytes) should be rejected
        let extra_peer = PeerKey(crate::transport::TransportPublicKey::from_bytes([0xAA; 32]));
        // Verify it's not in the set
        let is_new = !manager
            .downstream_subscribers
            .get(&contract)
            .map(|e| e.contains_key(&extra_peer))
            .unwrap_or(false);
        assert!(is_new, "Extra peer should not already be in the set");

        let rejected = !manager.add_downstream_subscriber(&contract, extra_peer);
        assert!(
            rejected,
            "Downstream subscriber beyond limit should be rejected (count was {actual_count})"
        );
    }

    #[test]
    fn test_downstream_subscriber_existing_peer_can_renew_at_limit() {
        let manager = HostingManager::new();
        let contract = make_contract_key(51);

        // Fill up to the limit
        let first_peer = make_peer_key(1);
        manager.add_downstream_subscriber(&contract, first_peer.clone());

        for i in 1..MAX_DOWNSTREAM_SUBSCRIBERS_PER_CONTRACT {
            let peer = PeerKey(crate::transport::TransportPublicKey::from_bytes({
                let mut bytes = [0u8; 32];
                bytes[0] = (i & 0xFF) as u8;
                bytes[1] = ((i >> 8) & 0xFF) as u8;
                bytes
            }));
            manager.add_downstream_subscriber(&contract, peer);
        }

        // Existing peer can still renew (re-insert updates the timestamp)
        assert!(
            manager.add_downstream_subscriber(&contract, first_peer),
            "Existing peer should be able to renew at limit"
        );
    }

    // =========================================================================
    // Regression tests for #3469: downstream_subscriber_count leak
    // =========================================================================

    /// Regression test: expire_stale_downstream_subscribers must return the
    /// count of expired peers so the interest manager can be decremented.
    #[test]
    fn test_expire_returns_expired_count_for_interest_sync() {
        let manager = HostingManager::new();
        let interest = make_interest_manager();
        let contract = make_contract_key(90);
        let peer_a = make_peer_key(90);
        let peer_b = make_peer_key(91);

        // Register two downstream subscribers in both managers
        manager.add_downstream_subscriber(&contract, peer_a.clone());
        interest.add_downstream_subscriber(&contract);
        manager.add_downstream_subscriber(&contract, peer_b.clone());
        interest.add_downstream_subscriber(&contract);

        // Verify interest manager tracks 2 downstream
        let count = interest.with_local_interest(&contract, |li| li.downstream_subscriber_count);
        assert_eq!(count, 2);

        // Make both stale
        if let Some(mut peers) = manager.downstream_subscribers.get_mut(&contract) {
            let stale = Instant::now() - SUBSCRIPTION_LEASE_DURATION - Duration::from_secs(1);
            peers.insert(peer_a, stale);
            peers.insert(peer_b, stale);
        }

        // Expire and sync interest manager (mimics ring.rs TTL expiry path)
        let expired = manager.expire_stale_downstream_subscribers();
        assert_eq!(expired.len(), 1);
        let (expired_contract, expired_count) = &expired[0];
        assert_eq!(*expired_contract, contract);
        assert_eq!(*expired_count, 2);

        for _ in 0..*expired_count {
            interest.remove_downstream_subscriber(expired_contract);
        }

        // Interest manager should now show 0 downstream
        assert!(
            !interest.has_local_interest(&contract),
            "downstream_subscriber_count should be 0 after syncing with TTL expiry"
        );
    }

    // =========================================================================
    // Local Client Access Tests (#3769)
    // =========================================================================

    /// Core test for #3769: locally-accessed contracts should be included in
    /// renewal, but relay-cached contracts should NOT.
    #[test]
    fn test_local_client_access_enables_renewal() {
        let manager = HostingManager::new();
        let local_contract = make_contract_key(1);
        let relay_contract = make_contract_key(2);

        // Both contracts get hosted via GET
        manager.record_contract_access(local_contract, 1000, AccessType::Get);
        manager.record_contract_access(relay_contract, 1000, AccessType::Get);

        // Only the local one gets marked as locally accessed
        manager.mark_local_client_access(&local_contract);

        let needs_renewal = manager.contracts_needing_renewal();

        assert!(
            needs_renewal.contains(&local_contract),
            "Locally-accessed contract should be in renewal list"
        );
        assert!(
            !needs_renewal.contains(&relay_contract),
            "Relay-cached contract should NOT be in renewal list"
        );
    }

    /// Relay-only contracts at scale should not cause subscription storms.
    /// Regression test for #3763/#3765 (the subscription storm incident).
    #[test]
    fn test_relay_cached_contracts_not_renewed_at_scale() {
        let manager = HostingManager::new();

        // Simulate 200 relay-cached contracts (no local_client_access)
        for i in 0..200u8 {
            let contract = make_contract_key(i);
            manager.record_contract_access(contract, 1000, AccessType::Get);
        }

        // Mark only 2 as locally accessed (simulating River user)
        let local_a = make_contract_key(42);
        let local_b = make_contract_key(99);
        manager.mark_local_client_access(&local_a);
        manager.mark_local_client_access(&local_b);

        let needs_renewal = manager.contracts_needing_renewal();
        assert_eq!(
            needs_renewal.len(),
            2,
            "Only 2 locally-accessed contracts should need renewal, found {}",
            needs_renewal.len()
        );
        assert!(needs_renewal.contains(&local_a));
        assert!(needs_renewal.contains(&local_b));
    }

    /// Locally-accessed contracts with active subscriptions should not be
    /// double-counted in the renewal list.
    #[test]
    fn test_local_client_access_with_active_subscription_no_duplicate() {
        let manager = HostingManager::new();
        let contract = make_contract_key(1);

        manager.record_contract_access(contract, 1000, AccessType::Get);
        manager.mark_local_client_access(&contract);
        manager.subscribe(contract);

        let needs_renewal = manager.contracts_needing_renewal();
        // The contract has an active subscription that isn't expiring yet,
        // and local_client_access. It should not appear (subscription is fresh).
        assert!(
            !needs_renewal.contains(&contract),
            "Contract with fresh active subscription should not need renewal"
        );
    }

    /// Marking and querying unknown contracts should be no-ops (no panic).
    #[test]
    fn test_local_client_access_unknown_contract() {
        let manager = HostingManager::new();
        let contract = make_contract_key(1);

        assert!(!manager.has_local_client_access(&contract));
        manager.mark_local_client_access(&contract); // no-op, not in cache
        assert!(!manager.has_local_client_access(&contract));
    }

    /// The local_client_access flag should be sticky -- once set, it should
    /// persist even after the contract's access type changes.
    #[test]
    fn test_local_client_access_sticky_across_access_type_changes() {
        let manager = HostingManager::new();
        let contract = make_contract_key(1);

        manager.record_contract_access(contract, 1000, AccessType::Get);
        manager.mark_local_client_access(&contract);
        assert!(manager.has_local_client_access(&contract));

        // Refresh via a relay PUT -- should NOT clear the local flag
        manager.record_contract_access(contract, 1000, AccessType::Put);
        assert!(
            manager.has_local_client_access(&contract),
            "local_client_access should persist across access type changes"
        );
    }

    /// Simulate restart: contracts loaded from disk with local_client_access
    /// should appear in contracts_needing_renewal().
    #[test]
    fn test_local_client_access_survives_restart_via_load() {
        let manager = HostingManager::new();

        // Simulate loading from disk with local_client_access=true
        {
            let mut cache = manager.hosting_cache.write();
            cache.load_persisted_entry(
                make_contract_key(1),
                1000,
                cache::AccessType::Get,
                std::time::Duration::from_secs(10),
                true, // locally accessed before restart
            );
            cache.load_persisted_entry(
                make_contract_key(2),
                1000,
                cache::AccessType::Get,
                std::time::Duration::from_secs(10),
                false, // relay-cached
            );
            cache.finalize_loading();
        }

        let needs_renewal = manager.contracts_needing_renewal();
        assert!(
            needs_renewal.contains(&make_contract_key(1)),
            "Locally-accessed contract loaded from disk should be renewed"
        );
        assert!(
            !needs_renewal.contains(&make_contract_key(2)),
            "Relay-cached contract loaded from disk should NOT be renewed"
        );
    }

    /// When a locally-accessed contract is evicted and re-added via relay,
    /// the local_client_access flag should be cleared (relay doesn't set it).
    #[test]
    fn test_eviction_clears_local_client_access() {
        // Small budget to force eviction
        let manager = HostingManager::new();
        // Override with a tiny cache
        {
            let mut cache = manager.hosting_cache.write();
            *cache = cache::HostingCache::new(
                200,                       // tiny budget: room for ~2 contracts at 100 bytes
                std::time::Duration::ZERO, // no TTL protection
                crate::util::time_source::InstantTimeSrc::new(),
            );
        }

        let contract_a = make_contract_key(1);
        let contract_b = make_contract_key(2);
        let contract_c = make_contract_key(3);

        // Add A (locally accessed) and B
        manager.record_contract_access(contract_a, 100, AccessType::Get);
        manager.mark_local_client_access(&contract_a);
        manager.record_contract_access(contract_b, 100, AccessType::Get);

        assert!(manager.has_local_client_access(&contract_a));

        // Add C -- should evict A (oldest in LRU)
        manager.record_contract_access(contract_c, 100, AccessType::Get);
        assert!(
            !manager.is_hosting_contract(&contract_a),
            "contract_a should have been evicted"
        );

        // Re-add A via relay (no mark_local_client_access)
        manager.record_contract_access(contract_a, 100, AccessType::Get);
        assert!(
            !manager.has_local_client_access(&contract_a),
            "Re-added via relay should NOT have local_client_access"
        );

        // After local client re-accesses, flag is restored
        manager.mark_local_client_access(&contract_a);
        assert!(manager.has_local_client_access(&contract_a));
    }
}
