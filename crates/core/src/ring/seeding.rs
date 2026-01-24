//! Simplified seeding and subscription management.
//!
//! # Architecture Overview
//!
//! This module manages contract seeding (which contracts a peer keeps available) and
//! subscription state (which contracts a peer is actively interested in).
//!
//! ## Key Simplification (2026-01 Refactor)
//!
//! Previously, this module maintained an explicit subscription tree with upstream/downstream
//! relationships. This was complex and prone to race conditions. The new architecture:
//!
//! 1. **Subscriptions are lease-based**: Active subscriptions have a lease that expires
//!    unless renewed. Clients must re-subscribe periodically (every ~2 minutes).
//!
//! 2. **Update propagation uses proximity cache only**: Updates propagate via the proximity
//!    cache mechanism, not an explicit subscription tree. Peers announce which contracts
//!    they're seeding, and updates are broadcast to nearby seeders.
//!
//! 3. **Tree structure is implicit**: The subscription tree emerges from routing behavior
//!    and seeding patterns, rather than being explicitly tracked.
//!
//! ## Data Flow
//!
//! - Subscribe request routes toward contract location
//! - Peers along the route cache the contract (seeding)
//! - Each seeding peer announces via proximity cache
//! - Updates broadcast to all proximity neighbors with the contract
//! - Subscriptions expire without renewal, triggering LRU-based eviction

use super::get_subscription_cache::{GetSubscriptionCache, DEFAULT_MAX_ENTRIES, DEFAULT_MIN_TTL};
use super::seeding_cache::{AccessType, SeedingCache};
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

/// Default seeding cache budget: 100MB
const DEFAULT_SEEDING_BUDGET_BYTES: u64 = 100 * 1024 * 1024;

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
// SeedingManager
// =============================================================================

/// Manages contract seeding and subscription state.
///
/// # Subscription Model
///
/// Subscriptions are lease-based with automatic expiry:
/// - `subscribe()` creates or renews a subscription with a lease
/// - Subscriptions expire after `SUBSCRIPTION_LEASE_DURATION` (4 minutes)
/// - Clients must call `renew_subscription()` every `SUBSCRIPTION_RENEWAL_INTERVAL` (2 minutes)
/// - Expired subscriptions are removed by `expire_stale_subscriptions()`
///
/// # Seeding Model
///
/// Contracts are seeded based on access patterns:
/// - GET, PUT, SUBSCRIBE operations add contracts to the seeding cache
/// - Active subscriptions prevent eviction from the seeding cache
/// - Inactive (expired) subscriptions allow LRU eviction
pub(crate) struct SeedingManager {
    /// Active subscriptions with lease expiry timestamps.
    /// Key: ContractKey, Value: expiry timestamp
    active_subscriptions: DashMap<ContractKey, Instant>,

    /// Contracts where a local client (WebSocket) is actively subscribed.
    /// Prevents seeding cache eviction while client subscriptions exist.
    client_subscriptions: DashMap<ContractInstanceId, HashSet<crate::client_events::ClientId>>,

    /// LRU cache of contracts this peer is seeding, with byte-budget awareness.
    seeding_cache: RwLock<SeedingCache<InstantTimeSrc>>,

    /// LRU+TTL cache of contracts we should auto-subscribe to based on GET access.
    get_subscription_cache: RwLock<GetSubscriptionCache<InstantTimeSrc>>,

    /// Contracts with subscription requests currently in-flight.
    pending_subscription_requests: DashSet<ContractKey>,

    /// Exponential backoff state for subscription retries.
    subscription_backoff: RwLock<TrackedBackoff<ContractKey>>,
}

impl SeedingManager {
    pub fn new() -> Self {
        let backoff_config =
            ExponentialBackoff::new(INITIAL_SUBSCRIPTION_BACKOFF, MAX_SUBSCRIPTION_BACKOFF);
        Self {
            active_subscriptions: DashMap::new(),
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
    /// Removes the active subscription. The contract may still be seeded
    /// (in the seeding cache) until evicted by LRU.
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
    // Seeding Cache Management
    // =========================================================================

    /// Record a contract access in the seeding cache.
    ///
    /// Returns contracts evicted from the cache (if any).
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

    /// Check if a contract is in the seeding cache.
    pub fn is_seeding_contract(&self, key: &ContractKey) -> bool {
        self.seeding_cache.read().contains(key)
    }

    /// Get the number of contracts in the seeding cache.
    pub fn seeding_contracts_count(&self) -> usize {
        self.seeding_cache.read().len()
    }

    /// Check if we should continue seeding a contract.
    ///
    /// Returns true if:
    /// - We have an active subscription, OR
    /// - We have client subscriptions, OR
    /// - The contract is in our seeding cache
    pub fn should_seed(&self, contract: &ContractKey) -> bool {
        self.is_subscribed(contract)
            || self.has_client_subscriptions(contract.id())
            || self.is_seeding_contract(contract)
    }

    // =========================================================================
    // GET Auto-Subscription Cache
    // =========================================================================

    /// Record a GET access for auto-subscription tracking.
    pub fn record_get_subscription(&self, key: ContractKey) -> Vec<ContractKey> {
        self.get_subscription_cache.write().record_access(key)
    }

    /// Refresh a contract's access time in the GET subscription cache.
    pub fn touch_get_subscription(&self, key: &ContractKey) {
        self.get_subscription_cache.write().touch(key);
    }

    /// Sweep for expired entries in the GET subscription cache.
    pub fn sweep_expired_get_subscriptions(&self) -> Vec<ContractKey> {
        self.get_subscription_cache.write().sweep_expired()
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
    /// - We have client subscriptions but no active network subscription
    ///
    /// BUG: This does NOT check GetSubscriptionCache for GET-triggered subscriptions
    /// that need renewal. GET subscriptions are stored in get_subscription_cache but
    /// aren't included in the renewal check.
    pub fn contracts_needing_renewal(&self) -> Vec<ContractKey> {
        let now = Instant::now();
        let renewal_threshold = now + SUBSCRIPTION_RENEWAL_INTERVAL;

        let mut needs_renewal = Vec::new();

        // Contracts with soon-to-expire subscriptions
        for entry in self.active_subscriptions.iter() {
            if *entry.value() <= renewal_threshold && *entry.value() > now {
                needs_renewal.push(*entry.key());
            }
        }

        // Contracts with client subscriptions but no active network subscription
        for entry in self.client_subscriptions.iter() {
            let instance_id = entry.key();
            // Find if we have an active subscription for this contract
            let has_active = self
                .active_subscriptions
                .iter()
                .any(|sub| sub.key().id() == instance_id && *sub.value() > now);
            if !has_active {
                // Need to find the ContractKey - check seeding cache
                if let Some(contract) = self
                    .seeding_cache
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

        // BUG: Missing check for get_subscription_cache contracts
        // Should iterate over get_subscription_cache.keys_lru_order() and add them to needs_renewal
        tracing::debug!(
            renewal_count = needs_renewal.len(),
            "contracts_needing_renewal: checked active_subscriptions and client_subscriptions (NOT GetSubscriptionCache)"
        );

        needs_renewal
    }

    // =========================================================================
    // Topology Snapshot (for telemetry/visualization)
    // =========================================================================

    /// Generate a topology snapshot for this peer.
    ///
    /// In the simplified lease-based model (2026-01 refactor), we don't track
    /// upstream/downstream relationships. The snapshot shows which contracts
    /// we're seeding and which have client subscriptions.
    #[allow(dead_code)] // Called from Ring methods that may be behind feature gates
    pub fn generate_topology_snapshot(
        &self,
        peer_addr: std::net::SocketAddr,
        location: f64,
    ) -> super::topology_registry::TopologySnapshot {
        use super::topology_registry::{ContractSubscription, TopologySnapshot};

        let mut snapshot = TopologySnapshot::new(peer_addr, location);
        let now = tokio::time::Instant::now();

        // Add all seeded contracts
        let seeding_cache = self.seeding_cache.read();
        for contract_key in seeding_cache.iter() {
            let is_subscribed = self
                .active_subscriptions
                .get(&contract_key)
                .map(|exp| *exp > now)
                .unwrap_or(false);

            let has_client_subscriptions =
                self.client_subscriptions.contains_key(contract_key.id());

            snapshot.set_contract(
                *contract_key.id(),
                ContractSubscription {
                    contract_key,
                    upstream: None,     // No upstream tracking in lease-based model
                    downstream: vec![], // No downstream tracking in lease-based model
                    is_seeding: true,
                    has_client_subscriptions,
                },
            );

            // If subscribed but not seeding, add that too
            if !is_subscribed && has_client_subscriptions {
                // Already added above
            }
        }

        // Add subscribed contracts that might not be in seeding cache yet
        for entry in self.active_subscriptions.iter() {
            if *entry.value() > now {
                let contract_key = *entry.key();
                if !seeding_cache.contains(&contract_key) {
                    let has_client_subscriptions =
                        self.client_subscriptions.contains_key(contract_key.id());

                    snapshot.set_contract(
                        *contract_key.id(),
                        ContractSubscription {
                            contract_key,
                            upstream: None,
                            downstream: vec![],
                            is_seeding: false,
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

impl Default for SeedingManager {
    fn default() -> Self {
        Self::new()
    }
}

// =============================================================================
// Test Accessors
// =============================================================================

#[cfg(test)]
impl SeedingManager {
    /// Test-only: Check if a contract is in the GET subscription cache.
    pub fn has_get_subscription(&self, key: &ContractKey) -> bool {
        self.get_subscription_cache.read().contains(key)
    }

    /// Test-only: Get all contracts in the GET subscription cache.
    pub fn get_subscription_cache_keys(&self) -> Vec<ContractKey> {
        self.get_subscription_cache.read().keys_lru_order()
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
        let manager = SeedingManager::new();
        let contract = make_contract_key(1);

        let result = manager.subscribe(contract);

        assert!(result.is_new);
        assert!(manager.is_subscribed(&contract));
    }

    #[tokio::test]
    async fn test_subscribe_renews_existing() {
        let manager = SeedingManager::new();
        let contract = make_contract_key(1);

        let first = manager.subscribe(contract);
        let second = manager.subscribe(contract);

        assert!(first.is_new);
        assert!(!second.is_new);
        assert!(second.expires_at >= first.expires_at);
    }

    #[tokio::test]
    async fn test_unsubscribe_removes_subscription() {
        let manager = SeedingManager::new();
        let contract = make_contract_key(1);

        manager.subscribe(contract);
        assert!(manager.is_subscribed(&contract));

        manager.unsubscribe(&contract);
        assert!(!manager.is_subscribed(&contract));
    }

    #[tokio::test]
    async fn test_renew_subscription() {
        let manager = SeedingManager::new();
        let contract = make_contract_key(1);

        // Renew non-existent subscription fails
        assert!(!manager.renew_subscription(&contract));

        // Subscribe then renew succeeds
        manager.subscribe(contract);
        assert!(manager.renew_subscription(&contract));
    }

    #[tokio::test]
    async fn test_get_subscribed_contracts() {
        let manager = SeedingManager::new();
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
        let manager = SeedingManager::new();

        assert_eq!(manager.active_subscription_count(), 0);

        manager.subscribe(make_contract_key(1));
        manager.subscribe(make_contract_key(2));
        assert_eq!(manager.active_subscription_count(), 2);

        manager.unsubscribe(&make_contract_key(1));
        assert_eq!(manager.active_subscription_count(), 1);
    }

    #[test]
    fn test_client_subscription_basic() {
        let manager = SeedingManager::new();
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
        let manager = SeedingManager::new();
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
    fn test_seeding_cache_basic() {
        let manager = SeedingManager::new();
        let key = make_contract_key(1);

        assert!(!manager.is_seeding_contract(&key));
        assert_eq!(manager.seeding_contracts_count(), 0);

        manager.record_contract_access(key, 1000, AccessType::Put);

        assert!(manager.is_seeding_contract(&key));
        assert_eq!(manager.seeding_contracts_count(), 1);
    }

    #[test]
    fn test_subscription_backoff() {
        let manager = SeedingManager::new();
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
    fn test_should_seed() {
        let manager = SeedingManager::new();
        let contract = make_contract_key(1);

        // Not seeding initially
        assert!(!manager.should_seed(&contract));

        // Add to seeding cache
        manager.record_contract_access(contract, 1000, AccessType::Put);
        assert!(manager.should_seed(&contract));
    }

    /// Regression test for PR #2804: GET subscriptions not included in renewal check.
    ///
    /// **The Bug**: contracts_needing_renewal() only checks active_subscriptions and
    /// client_subscriptions, but NOT GetSubscriptionCache. When a contract is retrieved
    /// via GET, it's stored in GetSubscriptionCache, but never renewed.
    ///
    /// This test directly verifies:
    /// 1. GET operations store subscriptions in GetSubscriptionCache
    /// 2. contracts_needing_renewal() does NOT return those contracts (the bug)
    #[test]
    #[should_panic(expected = "BUG REPRODUCED")]
    fn test_get_subscription_cache_not_checked_for_renewal() {
        let manager = SeedingManager::new();
        let contract = make_contract_key(42);

        // Simulate GET operation storing subscription in GetSubscriptionCache
        manager.record_get_subscription(contract);

        // Verify it's stored in GetSubscriptionCache
        assert!(
            manager.has_get_subscription(&contract),
            "Contract should be in GetSubscriptionCache after GET operation"
        );

        // BUG: contracts_needing_renewal() doesn't check GetSubscriptionCache
        let needs_renewal = manager.contracts_needing_renewal();

        // This assertion FAILS, demonstrating the bug
        assert!(
            needs_renewal.contains(&contract),
            "\n\n\
            ╔══════════════════════════════════════════════════════════════════════╗\n\
            ║ BUG REPRODUCED: GET subscriptions not included in renewal check!    ║\n\
            ╚══════════════════════════════════════════════════════════════════════╝\n\
            \n\
            Verified facts:\n\
            • Contract IS in GetSubscriptionCache: {}\n\
            • contracts_needing_renewal() returned: {} contracts\n\
            • Contract in renewal list: {} ← BUG: Should be TRUE\n\
            \n\
            Root cause:\n\
            • File: crates/core/src/ring/seeding.rs\n\
            • Function: contracts_needing_renewal() (line ~486)\n\
            • Bug: Only checks active_subscriptions and client_subscriptions\n\
            • Missing: Does NOT check get_subscription_cache\n\
            \n\
            Expected behavior:\n\
            When a contract is stored in GetSubscriptionCache (via GET operation),\n\
            it should be included in contracts_needing_renewal() so the renewal\n\
            task can renew the subscription.\n\
            \n\
            Actual behavior (BUG):\n\
            Contract is in GetSubscriptionCache but NOT in contracts_needing_renewal(),\n\
            so GET-triggered subscriptions are never renewed and expire after 240s.\n\
            \n\
            How to fix:\n\
            Add GetSubscriptionCache check to contracts_needing_renewal():\n\
            \n\
            ```rust\n\
            // In seeding.rs contracts_needing_renewal():\n\
            // Add after existing checks:\n\
            for key in self.get_subscription_cache.read().keys_lru_order() {{\n\
                if !needs_renewal.contains(&key) {{\n\
                    needs_renewal.push(key);\n\
                }}\n\
            }}\n\
            ```\n\
            \n\
            Related: PR #2804\n\
            ",
            manager.has_get_subscription(&contract),
            needs_renewal.len(),
            needs_renewal.contains(&contract)
        );
    }
}
