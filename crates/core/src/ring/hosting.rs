//! Unified hosting and subscription management.
//!
//! # Architecture Overview
//!
//! This module manages contract hosting (which contracts a peer keeps available) and
//! subscription state (which contracts a peer is actively interested in).
//!
//! ## Key Design (2026-01 Unified Hosting Refactor)
//!
//! This module unifies the previously separate "seeding" and "GET subscription" caches
//! into a single `HostingCache` that serves as the source of truth for which contracts
//! this peer is hosting.
//!
//! 1. **Hosting = subscription renewal**: All hosted contracts automatically get
//!    their subscriptions renewed. This fixes the bug where GET-triggered subscriptions
//!    would expire and never be renewed.
//!
//! 2. **Subscriptions are lease-based**: Active subscriptions have a lease that expires
//!    unless renewed. Clients must re-subscribe periodically (every ~2 minutes).
//!
//! 3. **Single cache**: One `HostingCache` with byte-budget LRU and TTL protection.
//!
//! ## Data Flow
//!
//! - GET/PUT/SUBSCRIBE operations add contracts to the hosting cache
//! - All hosted contracts get subscription renewal via `contracts_needing_renewal()`
//! - Active subscriptions prevent eviction from the hosting cache
//! - TTL protects recently accessed contracts from premature eviction

use super::hosting_cache::{
    AccessType, HostingCache, DEFAULT_HOSTING_BUDGET_BYTES, DEFAULT_MIN_TTL,
};
use crate::util::backoff::{ExponentialBackoff, TrackedBackoff};
use crate::util::time_source::InstantTimeSrc;
use dashmap::{DashMap, DashSet};
use freenet_stdlib::prelude::{ContractInstanceId, ContractKey};
use parking_lot::RwLock;
use std::collections::HashSet;
use std::time::Duration;
use tokio::time::Instant;
use tracing::{debug, info};

// =============================================================================
// Constants
// =============================================================================

/// Subscription lease duration.
/// Subscriptions automatically expire after this duration unless renewed.
pub const SUBSCRIPTION_LEASE_DURATION: Duration = Duration::from_secs(240); // 4 minutes

/// Recommended renewal interval for subscriptions.
/// Clients should renew subscriptions at this interval to prevent expiry.
pub const SUBSCRIPTION_RENEWAL_INTERVAL: Duration = Duration::from_secs(120); // 2 minutes

/// Initial backoff duration for subscription retries.
const INITIAL_SUBSCRIPTION_BACKOFF: Duration = Duration::from_secs(30);

/// Maximum backoff duration for subscription retries.
const MAX_SUBSCRIPTION_BACKOFF: Duration = Duration::from_secs(600); // 10 minutes

/// Maximum number of tracked subscription backoff entries.
const MAX_SUBSCRIPTION_BACKOFF_ENTRIES: usize = 4096;

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
/// - Subscriptions expire after `SUBSCRIPTION_LEASE_DURATION` (4 minutes)
/// - Clients must call `renew_subscription()` every `SUBSCRIPTION_RENEWAL_INTERVAL` (2 minutes)
/// - Expired subscriptions are removed by `expire_stale_subscriptions()`
///
/// # Hosting Model
///
/// Contracts are hosted based on access patterns:
/// - GET, PUT, SUBSCRIBE operations add contracts to the hosting cache
/// - **All hosted contracts get subscription renewal** (the key fix)
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
        let expires_at = Instant::now() + SUBSCRIPTION_LEASE_DURATION;
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
            *entry = Instant::now() + SUBSCRIPTION_LEASE_DURATION;
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
            debug!(%contract, "unsubscribe: removed active subscription");
        }
    }

    /// Check if we have an active (non-expired) subscription to a contract.
    pub fn is_subscribed(&self, contract: &ContractKey) -> bool {
        self.active_subscriptions
            .get(contract)
            .map(|expires_at| *expires_at > Instant::now())
            .unwrap_or(false)
    }

    /// Get all contracts with active subscriptions.
    pub fn get_subscribed_contracts(&self) -> Vec<ContractKey> {
        let now = Instant::now();
        self.active_subscriptions
            .iter()
            .filter(|entry| *entry.value() > now)
            .map(|entry| *entry.key())
            .collect()
    }

    /// Expire stale subscriptions and return the contracts that were expired.
    ///
    /// Should be called periodically by a background task.
    pub fn expire_stale_subscriptions(&self) -> Vec<ContractKey> {
        let now = Instant::now();
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
            info!(
                count = expired.len(),
                "expire_stale_subscriptions: expired {} subscriptions",
                expired.len()
            );
        }

        expired
    }

    /// Get the number of active subscriptions.
    #[allow(dead_code)] // Used in tests, may be used for metrics in future
    pub fn active_subscription_count(&self) -> usize {
        let now = Instant::now();
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
        let instance_ids_with_client: Vec<ContractInstanceId> = self
            .client_subscriptions
            .iter()
            .filter(|entry| entry.value().contains(&client_id))
            .map(|entry| *entry.key())
            .collect();

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
    // Hosting Cache Management
    // =========================================================================

    /// Record a contract access in the hosting cache.
    ///
    /// This is the main entry point for adding contracts to the hosting cache.
    /// ALL contracts in the hosting cache will have their subscriptions renewed.
    ///
    /// Returns contracts evicted from the cache (if any).
    /// Automatically persists hosting metadata for the accessed contract and
    /// removes persisted metadata for evicted contracts.
    pub fn record_contract_access(
        &self,
        key: ContractKey,
        size_bytes: u64,
        access_type: AccessType,
    ) -> Vec<ContractKey> {
        let evicted = self
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
                let metadata = HostingMetadata::new(now_ms, access_type_u8, size_bytes, code_hash);
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
            for evicted_key in &evicted {
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

        evicted
    }

    /// Check if a contract is in the hosting cache.
    pub fn is_hosting_contract(&self, key: &ContractKey) -> bool {
        self.hosting_cache.read().contains(key)
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
    pub fn should_host(&self, contract: &ContractKey) -> bool {
        self.is_subscribed(contract)
            || self.has_client_subscriptions(contract.id())
            || self.is_hosting_contract(contract)
    }

    /// Touch a contract in the hosting cache (refresh TTL without adding).
    ///
    /// Called when UPDATE is received for a hosted contract.
    pub fn touch_hosting(&self, key: &ContractKey) {
        self.hosting_cache.write().touch(key);
    }

    /// Sweep for expired entries in the hosting cache.
    ///
    /// Returns contracts evicted from this cache. Callers should check
    /// `has_client_subscriptions()` before removing subscription state.
    /// Automatically removes persisted metadata for expired contracts.
    pub fn sweep_expired_hosting(&self) -> Vec<ContractKey> {
        let expired = self.hosting_cache.write().sweep_expired();

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
        let now = Instant::now();
        self.active_subscriptions
            .iter()
            .map(|entry| {
                let contract = *entry.key();
                let expires_at = *entry.value();
                let is_active = expires_at > now;
                let has_client = self.has_client_subscriptions(contract.id());
                (contract, has_client, is_active, Some(expires_at))
            })
            .collect()
    }

    /// Get contracts that need subscription renewal.
    ///
    /// Returns contracts where:
    /// - We have an active subscription that will expire soon, OR
    /// - We have client subscriptions but no active network subscription, OR
    /// - We have hosted contracts without active subscriptions (THE FIX)
    ///
    /// This is the key fix: ALL hosted contracts get subscription renewal,
    /// not just those with client subscriptions or soon-to-expire subscriptions.
    pub fn contracts_needing_renewal(&self) -> Vec<ContractKey> {
        let now = Instant::now();
        let renewal_threshold = now + SUBSCRIPTION_RENEWAL_INTERVAL;

        let mut needs_renewal = Vec::new();

        // 1. Contracts with soon-to-expire subscriptions
        for entry in self.active_subscriptions.iter() {
            if *entry.value() <= renewal_threshold && *entry.value() > now {
                needs_renewal.push(*entry.key());
            }
        }

        // 2. Contracts with client subscriptions but no active network subscription
        for entry in self.client_subscriptions.iter() {
            let instance_id = entry.key();
            // Find if we have an active subscription for this contract
            let has_active = self
                .active_subscriptions
                .iter()
                .any(|sub| sub.key().id() == instance_id && *sub.value() > now);
            if !has_active {
                // Need to find the ContractKey - check hosting cache
                if let Some(contract) = self
                    .hosting_cache
                    .read()
                    .iter()
                    .find(|k| k.id() == instance_id)
                {
                    if !needs_renewal.contains(&contract) {
                        needs_renewal.push(contract);
                    }
                }
            }
        }

        // 3. THE FIX: All hosted contracts should have subscriptions renewed
        // This ensures GET-triggered subscriptions don't expire without renewal
        for contract in self.hosting_cache.read().iter() {
            // Skip if already in list
            if needs_renewal.contains(&contract) {
                continue;
            }
            // Skip if has active subscription that's not expiring soon
            if self
                .active_subscriptions
                .get(&contract)
                .map(|exp| *exp > renewal_threshold)
                .unwrap_or(false)
            {
                continue;
            }
            // This hosted contract needs subscription renewal
            needs_renewal.push(contract);
        }

        needs_renewal
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
        let now = tokio::time::Instant::now();

        // Add all hosted contracts
        let hosting_cache = self.hosting_cache.read();
        for contract_key in hosting_cache.iter() {
            let has_client_subscriptions =
                self.client_subscriptions.contains_key(contract_key.id());

            snapshot.set_contract(
                *contract_key.id(),
                ContractSubscription {
                    contract_key,
                    upstream: None,     // No upstream tracking in lease-based model
                    downstream: vec![], // No downstream tracking in lease-based model
                    is_seeding: true,   // TODO: Rename to is_hosting in topology_registry
                    has_client_subscriptions,
                },
            );
        }

        // Add subscribed contracts that might not be in hosting cache yet
        for entry in self.active_subscriptions.iter() {
            if *entry.value() > now {
                let contract_key = *entry.key();
                if !hosting_cache.contains(&contract_key) {
                    let has_client_subscriptions =
                        self.client_subscriptions.contains_key(contract_key.id());

                    snapshot.set_contract(
                        *contract_key.id(),
                        ContractSubscription {
                            contract_key,
                            upstream: None,
                            downstream: vec![],
                            is_seeding: false, // TODO: Rename to is_hosting
                            has_client_subscriptions,
                        },
                    );
                }
            }
        }

        snapshot.timestamp_nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos() as u64;

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
                    1 => super::hosting_cache::AccessType::Put,
                    2 => super::hosting_cache::AccessType::Subscribe,
                    _ => super::hosting_cache::AccessType::Get,
                };

                // Calculate age from persisted timestamp
                let age_ms = now_ms.saturating_sub(metadata.last_access_ms);
                let age = std::time::Duration::from_millis(age_ms);

                cache.load_persisted_entry(key, metadata.size_bytes, access_type, age);
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

                // Add to hosting cache as if it was just accessed via GET
                // Use current time as last_access since we don't have historical data
                cache.load_persisted_entry(
                    key,
                    size_bytes,
                    super::hosting_cache::AccessType::Get,
                    std::time::Duration::ZERO,
                );

                // Persist hosting metadata so future restarts don't need migration
                let code_hash_bytes: [u8; 32] = *code_hash;
                let metadata = crate::contract::storages::HostingMetadata::new(
                    now_ms,
                    0, // GET access type
                    size_bytes,
                    code_hash_bytes,
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
                    1 => super::hosting_cache::AccessType::Put,
                    2 => super::hosting_cache::AccessType::Subscribe,
                    _ => super::hosting_cache::AccessType::Get,
                };

                // Calculate age from persisted timestamp
                let age_ms = now_ms.saturating_sub(metadata.last_access_ms);
                let age = std::time::Duration::from_millis(age_ms);

                cache.load_persisted_entry(key, metadata.size_bytes, access_type, age);
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

                // Add to hosting cache as if it was just accessed via GET
                cache.load_persisted_entry(
                    key,
                    size_bytes,
                    super::hosting_cache::AccessType::Get,
                    std::time::Duration::ZERO,
                );

                // Persist hosting metadata so future restarts don't need migration
                let code_hash_bytes: [u8; 32] = *code_hash;
                let metadata = crate::contract::storages::sqlite::HostingMetadata::new(
                    now_ms,
                    0, // GET access type
                    size_bytes,
                    code_hash_bytes,
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

    #[test]
    fn test_contracts_needing_renewal_includes_hosted() {
        // This is the key test for the bug fix
        let manager = HostingManager::new();
        let contract = make_contract_key(1);

        // Add to hosting cache (simulating GET operation)
        manager.record_contract_access(contract, 1000, AccessType::Get);

        // Contract should need renewal even without active subscription
        // (this is the bug fix - previously hosted contracts weren't included)
        let needs_renewal = manager.contracts_needing_renewal();
        assert!(
            needs_renewal.contains(&contract),
            "Hosted contracts should need subscription renewal"
        );
    }
}
