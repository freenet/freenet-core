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
/// Re-exported as the single source of truth for the default hosting storage
/// budget. `config::default_max_hosting_storage()` resolves to this constant so
/// the operator-facing default and the in-code fallback can never drift.
pub(crate) use cache::DEFAULT_HOSTING_BUDGET_BYTES;
pub use cache::{AccessType, RecordAccessResult};
use cache::{DEFAULT_MIN_TTL, HostingCache};
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

/// Lease-tracked active subscription state.
#[derive(Debug, Clone, Copy)]
pub(crate) struct SubscriptionLease {
    /// First successful subscribe — preserved across renewals so the
    /// dashboard can show continuous subscription duration.
    pub subscribed_since: Instant,
    /// When the lease expires unless renewed.
    pub expires_at: Instant,
    /// Most recent state update observed (for dashboard display).
    pub last_updated: Option<Instant>,
}

/// Public dashboard snapshot of one subscribed contract.
///
/// Durations are computed inside `HostingManager` so the dashboard does
/// not need to hold a `tokio::time::Instant` reference.
#[derive(Debug, Clone)]
pub struct SubscribedContractSnapshot {
    pub key: ContractKey,
    /// Seconds since the subscription was first established (preserved across renewals).
    pub subscribed_secs: u64,
    /// Seconds since the most recent observed state update, if any.
    pub last_updated_secs: Option<u64>,
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
    /// Active subscriptions with lease state and dashboard telemetry.
    /// Holds the lease expiry plus enough history (subscribed_since,
    /// last_updated) for the local-peer dashboard to render this map
    /// directly without a parallel mirror.
    active_subscriptions: DashMap<ContractKey, SubscriptionLease>,

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

    /// Monotonic per-contract state-write generation counter.
    ///
    /// Bumped at every persistent state write in the executor
    /// (`state_store.store` / `state_store.update`). Captured atomically
    /// when an `EvictContract` is enqueued (`HostedContract.write_generation`,
    /// recorded under the hosting-cache write lock) and re-checked at
    /// deletion time in `RuntimePool::remove_contract`. If the captured
    /// generation no longer matches the current value, a state write
    /// occurred between eviction and deletion (e.g. a PUT/UPDATE
    /// re-hosted the contract) and the disk reclamation must be skipped
    /// — the freshly-PUT state would otherwise be deleted.
    ///
    /// See `RuntimePool::remove_contract` and `EvictContract` for the
    /// race this token closes (the driver-side `host_contract` re-mark
    /// of a freshly-PUT contract runs after `PutQuery.await` returns,
    /// so the existing `is_hosting_contract` check is not sufficient).
    state_generation: DashMap<ContractKey, u64>,

    /// Retry queue for contracts whose `EvictContract` could not be
    /// completed at the original eviction time. Maps contract key →
    /// `expected_generation` captured when the original `EvictContract`
    /// event was emitted.
    ///
    /// Two skip points add entries here (both close narrow disk-leak
    /// edge cases — see PR #4212 review round 7):
    ///
    /// 1. **Queue-full drop**: when the per-contract fair queue rejects
    ///    an `EvictContract` event (queue-full), the hosting-cache
    ///    entry is already gone so no later sweep would re-emit. The
    ///    pending entry lets the periodic sweep retry.
    /// 2. **In-use-then-subscriber-expires**: when
    ///    `RuntimePool::remove_contract` skips reclamation because
    ///    `contract_in_use` is true (a subscriber appeared between
    ///    eviction and processing), the contract is gone from the
    ///    hosting cache. When that subscriber later expires no cache
    ///    entry remains to emit another eviction — the pending entry
    ///    lets the periodic sweep retry once `contract_in_use`
    ///    becomes false.
    ///
    /// Entries are removed by `pending_reclamation_remove` after a
    /// successful disk reclamation. The map is monotonically draining
    /// under steady load — bounded by the contracts the node has ever
    /// stored. The pending entries are a *retry queue* for
    /// reclamation, NOT a *block* on reclamation, so they do not
    /// constitute an unbounded cleanup exemption (AGENTS.md cleanup
    /// rule): the on-disk state stays until the retry succeeds.
    ///
    /// Behind an `Arc` so the periodic sweep snapshot can iterate
    /// without re-entering the `HostingManager` borrow.
    pending_reclamation: std::sync::Arc<DashMap<ContractKey, u64>>,
}

impl HostingManager {
    pub fn new(budget_bytes: u64) -> Self {
        let backoff_config =
            ExponentialBackoff::new(INITIAL_SUBSCRIPTION_BACKOFF, MAX_SUBSCRIPTION_BACKOFF);
        Self {
            active_subscriptions: DashMap::new(),
            client_subscriptions: DashMap::new(),
            hosting_cache: RwLock::new(HostingCache::new(
                budget_bytes,
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
            state_generation: DashMap::new(),
            pending_reclamation: std::sync::Arc::new(DashMap::new()),
        }
    }

    // =========================================================================
    // State-Write Generation Token
    // =========================================================================

    /// Atomically increment the state-write generation for `key` and return
    /// the new value. Called by the executor after every successful state
    /// write (`state_store.store` / `state_store.update`) — see the chokepoint
    /// comment in `Executor::commit_state_update` and the per-call-site
    /// callouts in `contract/executor/runtime.rs`.
    pub(crate) fn bump_state_generation(&self, key: &ContractKey) -> u64 {
        use dashmap::mapref::entry::Entry;
        match self.state_generation.entry(*key) {
            Entry::Occupied(mut e) => {
                let next = e.get().saturating_add(1);
                *e.get_mut() = next;
                next
            }
            Entry::Vacant(e) => {
                e.insert(1);
                1
            }
        }
    }

    /// Read the current state-write generation for `key` (0 if never written).
    pub(crate) fn state_generation(&self, key: &ContractKey) -> u64 {
        self.state_generation
            .get(key)
            .map(|v| *v.value())
            .unwrap_or(0)
    }

    /// Remove the generation entry for `key`. Called after a successful disk
    /// reclamation so the map does not grow unbounded.
    pub(crate) fn forget_state_generation(&self, key: &ContractKey) {
        self.state_generation.remove(key);
    }

    /// Update the hosting-cache snapshot of `key`'s state-write generation
    /// to `new_gen`. Paired with `bump_state_generation` at every state-write
    /// chokepoint (executor PUT/UPDATE and V2 delegate PUT/UPDATE) so a
    /// later eviction's snapshot reflects the current generation and the
    /// deletion-time guard in `RuntimePool::remove_contract` does not
    /// permanently skip reclamation after an UPDATE-then-evict. No-op when
    /// the entry is not currently cached. See
    /// `HostingCache::refresh_entry_generation`.
    pub(crate) fn refresh_cache_generation(&self, key: &ContractKey, new_gen: u64) {
        self.hosting_cache
            .write()
            .refresh_entry_generation(key, new_gen);
    }

    /// Set the storage reference for persisting hosting metadata.
    /// Must be called after executor creation.
    pub fn set_storage(&self, storage: crate::contract::storages::Storage) {
        *self.storage.write() = Some(storage);
    }

    // =========================================================================
    // Pending Reclamation Retry Queue
    // =========================================================================

    /// Add `key` to the pending-reclamation retry queue, recording the
    /// `expected_generation` captured at the original `EvictContract`
    /// emission time. If `key` is already present, replaces the entry —
    /// the most recent attempt's generation is the relevant one for the
    /// retry, and over-writing avoids unbounded growth from repeated
    /// add calls on the same key.
    ///
    /// See `pending_reclamation` field docs for the two skip points
    /// that feed this queue.
    pub(crate) fn pending_reclamation_add(&self, key: ContractKey, expected_generation: u64) {
        self.pending_reclamation.insert(key, expected_generation);
    }

    /// Remove `key` from the pending-reclamation queue. Called after a
    /// successful disk reclamation so the queue drains under steady
    /// load.
    pub(crate) fn pending_reclamation_remove(&self, key: &ContractKey) {
        self.pending_reclamation.remove(key);
    }

    /// Snapshot every pending reclamation entry as an owned vector so
    /// the periodic sweep can iterate without holding any DashMap
    /// shard guard.
    pub(crate) fn pending_reclamation_snapshot(&self) -> Vec<(ContractKey, u64)> {
        self.pending_reclamation
            .iter()
            .map(|entry| (*entry.key(), *entry.value()))
            .collect()
    }

    /// Number of contracts currently in the pending-reclamation queue
    /// (used in tests and diagnostics).
    #[cfg(test)]
    pub(crate) fn pending_reclamation_len(&self) -> usize {
        self.pending_reclamation.len()
    }

    // =========================================================================
    // Subscription Management (Lease-Based)
    // =========================================================================

    /// Subscribe to a contract with a lease.
    ///
    /// Creates a new subscription or renews an existing one. The subscription
    /// will expire after `SUBSCRIPTION_LEASE_DURATION` unless renewed.
    /// `subscribed_since` is preserved on renewal so the dashboard reports
    /// the continuous subscription duration, not the most recent renewal.
    pub fn subscribe(&self, contract: ContractKey) -> SubscribeResult {
        use dashmap::mapref::entry::Entry;
        let now = self.time_source.now();
        let expires_at = now + SUBSCRIPTION_LEASE_DURATION;
        let is_new = match self.active_subscriptions.entry(contract) {
            Entry::Occupied(mut e) => {
                // Renewal: advance the lease but DELIBERATELY preserve
                // `subscribed_since` (continuous duration) and
                // `last_updated` (most-recent UPDATE timestamp).
                e.get_mut().expires_at = expires_at;
                false
            }
            Entry::Vacant(e) => {
                e.insert(SubscriptionLease {
                    subscribed_since: now,
                    expires_at,
                    last_updated: None,
                });
                true
            }
        };

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
            entry.expires_at = self.time_source.now() + SUBSCRIPTION_LEASE_DURATION;
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
            .map(|lease| lease.expires_at > self.time_source.now())
            .unwrap_or(false)
    }

    /// Get all contracts with active subscriptions.
    pub fn get_subscribed_contracts(&self) -> Vec<ContractKey> {
        let now = self.time_source.now();
        let mut contracts: Vec<ContractKey> = self
            .active_subscriptions
            .iter()
            .filter(|entry| entry.value().expires_at > now)
            .map(|entry| *entry.key())
            .collect();
        // Sort for deterministic ordering (critical for simulation tests)
        contracts.sort_by(|a, b| a.id().as_bytes().cmp(b.id().as_bytes()));
        contracts
    }

    /// Snapshot of every active subscription for the local-peer dashboard.
    ///
    /// Reads directly from the canonical lease map (no parallel
    /// mirror). The earlier `network_status::subscribed_contracts`
    /// mirror silently drifted when SUBSCRIBE migrated to its driver
    /// and lost its recording hook.
    pub fn dashboard_subscription_snapshot(&self) -> Vec<SubscribedContractSnapshot> {
        let now = self.time_source.now();
        let mut snapshot: Vec<SubscribedContractSnapshot> = self
            .active_subscriptions
            .iter()
            .filter(|entry| entry.value().expires_at > now)
            .map(|entry| {
                let lease = *entry.value();
                SubscribedContractSnapshot {
                    key: *entry.key(),
                    subscribed_secs: now
                        .saturating_duration_since(lease.subscribed_since)
                        .as_secs(),
                    last_updated_secs: lease
                        .last_updated
                        .map(|t| now.saturating_duration_since(t).as_secs()),
                }
            })
            .collect();
        // Most recently updated first; never-updated entries fall to the
        // end. Ties on `last_updated_secs` (including the (None, None)
        // case) break by key bytes so the dashboard renders a stable
        // order across refreshes — DashMap iteration order would
        // otherwise leak through and reshuffle rows on every poll.
        snapshot.sort_by(|a, b| {
            let primary = match (a.last_updated_secs, b.last_updated_secs) {
                (Some(a_secs), Some(b_secs)) => a_secs.cmp(&b_secs),
                (Some(_), None) => std::cmp::Ordering::Less,
                (None, Some(_)) => std::cmp::Ordering::Greater,
                (None, None) => std::cmp::Ordering::Equal,
            };
            primary.then_with(|| a.key.id().as_bytes().cmp(b.key.id().as_bytes()))
        });
        snapshot
    }

    /// Record that a state update was observed for `contract`.
    ///
    /// Updates the dashboard "last seen update" timestamp. No-op if we
    /// are not currently subscribed.
    pub fn record_contract_update(&self, contract: &ContractKey) {
        if let Some(mut entry) = self.active_subscriptions.get_mut(contract) {
            entry.last_updated = Some(self.time_source.now());
        }
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
        self.active_subscriptions.retain(|contract, lease| {
            if lease.expires_at <= now {
                expired.push(*contract);
                false
            } else {
                true
            }
        });

        if !expired.is_empty() {
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
            .filter(|entry| entry.value().expires_at > now)
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

            // Find matching ContractKey in active_subscriptions.
            // Drop the DashMap iter() guard before `affected_contracts.push`
            // so an unrelated caller cannot deadlock on the same shard
            // (clippy: `significant_drop_in_scrutinee`).
            let contract = self
                .active_subscriptions
                .iter()
                .find(|entry| *entry.key().id() == instance_id)
                .map(|entry| *entry.key());
            if let Some(contract) = contract {
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
        let removed = if let Some(mut peers) = self.downstream_subscribers.get_mut(contract) {
            peers.remove(peer).is_some()
        } else {
            false
        };
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

    /// Whether something still depends on this node hosting `contract` — a
    /// live local client subscription or a downstream peer subscriber. Used
    /// to gate hosting-cache eviction reclamation: a contract that is in
    /// use must NOT have its on-disk state/code deleted.
    ///
    /// **Why `is_subscribed` (this node's own upstream subscription) is NOT
    /// included.** It would seem natural to also exempt contracts the node
    /// is actively subscribed to. The problem is `contracts_needing_renewal`
    /// section 1 renews ANY soon-to-expire active subscription
    /// unconditionally, with no gate on local interest. So an
    /// `is_subscribed`-only exemption is effectively unbounded — the renewal
    /// machinery would keep extending the lease forever, blocking
    /// reclamation indefinitely. That would violate the cleanup-exemption
    /// rule in `AGENTS.md` (exemptions must be time-bounded). Local-client
    /// subscriptions and downstream subscribers ARE both time-bounded:
    /// client subscriptions expire on disconnect; downstream subscribers
    /// expire via `expire_stale_downstream_subscribers` after
    /// `SUBSCRIPTION_LEASE_DURATION` without renewal.
    ///
    /// The narrow case "subscribed but no local interest" should be handled
    /// by tearing down the orphaned upstream subscription, not by carrying
    /// an unbounded GC exemption here.
    pub fn contract_in_use(&self, contract: &ContractKey) -> bool {
        self.has_client_subscriptions(contract.id()) || self.has_downstream_subscribers(contract)
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
    /// - `evicted`: `(ContractKey, write_generation)` pairs for contracts
    ///   evicted to make room — the generation snapshot is carried through
    ///   `EvictContract` and re-checked at deletion time.
    ///
    /// Automatically persists hosting metadata for the accessed contract and
    /// removes persisted metadata for evicted contracts.
    pub fn record_contract_access(
        &self,
        key: ContractKey,
        size_bytes: u64,
        access_type: AccessType,
    ) -> RecordAccessResult {
        // `contract_in_use` reads only the client_subscriptions /
        // downstream_subscribers / active_subscriptions DashMaps — never
        // `hosting_cache` — so calling it from inside the `hosting_cache`
        // write guard does not re-enter that lock. `sweep_expired_hosting`
        // uses this same pattern.
        //
        // Read the current state-write generation BEFORE taking the
        // hosting-cache write lock to avoid nested lock order against the
        // `state_generation` DashMap shards. The generation is monotonic so
        // a value read here is a valid lower bound; if a write races and
        // bumps it after this read, the cached entry will simply be
        // refreshed by the subsequent `record_contract_access` from that
        // write path.
        let current_generation = self.state_generation(&key);
        let result = self.hosting_cache.write().record_access(
            key,
            size_bytes,
            access_type,
            current_generation,
            |k: &ContractKey| self.contract_in_use(k),
        );

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
            for (evicted_key, _generation) in &result.evicted {
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

    /// Get the configured byte-budget of the hosting cache.
    #[cfg(test)]
    pub(crate) fn hosting_budget_bytes(&self) -> u64 {
        self.hosting_cache.read().budget_bytes()
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
    /// Contracts are protected from eviction while `contract_in_use` returns
    /// true — i.e. they have client subscriptions, downstream subscribers, or
    /// an active upstream network subscription. This is the same predicate
    /// used by `record_contract_access`, so all eviction paths agree.
    /// The downstream subscriber exemption is time-bounded: stale entries are
    /// removed by `expire_stale_downstream_subscribers()` (called periodically)
    /// after `SUBSCRIPTION_LEASE_DURATION` without renewal.
    /// Automatically removes persisted metadata for expired contracts.
    ///
    /// Returns `(ContractKey, write_generation)` pairs — the generation
    /// captured atomically under the hosting-cache write lock travels with
    /// the `EvictContract` event so the deletion-time guard can detect a
    /// re-host race.
    pub fn sweep_expired_hosting(&self) -> Vec<(ContractKey, u64)> {
        let expired = self
            .hosting_cache
            .write()
            .sweep_expired(|key| self.contract_in_use(key));

        // Clean up persisted metadata for expired contracts
        if !expired.is_empty() {
            if let Some(storage) = self.storage.read().as_ref() {
                for (expired_key, _generation) in &expired {
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
                let expires_at = entry.value().expires_at;
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
            .map(|entry| (*entry.key(), entry.value().expires_at))
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
                .any(|sub| sub.key().id() == &instance_id && sub.value().expires_at > now);
            if !has_active {
                // Need to find the ContractKey - check hosting cache.
                // Materialize the lookup into an owned value before the read
                // guard's scope ends so we don't hold the lock across the
                // hash insertion (clippy: `significant_drop_in_scrutinee`).
                let contract = self
                    .hosting_cache
                    .read()
                    .iter()
                    .find(|k| k.id() == &instance_id);
                if let Some(contract) = contract {
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
                        .map(|e| e.expires_at > now)
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

        // Record the raw set of keys that are in `active_subscriptions` right
        // now. This is used by regression tests to detect whether a peer
        // installed a subscription lease — e.g. the relay-pollution bug fixed
        // alongside this field where every forwarder on a SUBSCRIBE response
        // path was unconditionally adding itself to active_subscriptions,
        // causing feedback-loop renewal. Must be populated BEFORE the merged
        // `contracts` map below, which hides active_subscriptions entries
        // behind hosting cache presence when both exist.
        for entry in self.active_subscriptions.iter() {
            if entry.value().expires_at > now {
                snapshot.active_subscription_keys.insert(*entry.key().id());
            }
        }

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
            .map(|entry| (*entry.key(), entry.value().expires_at))
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
        Self::new(DEFAULT_HOSTING_BUDGET_BYTES)
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
        let manager = HostingManager::new(DEFAULT_HOSTING_BUDGET_BYTES);
        let contract = make_contract_key(1);

        let result = manager.subscribe(contract);

        assert!(result.is_new);
        assert!(manager.is_subscribed(&contract));
    }

    #[tokio::test]
    async fn test_new_uses_configured_budget() {
        let custom_budget = 256 * 1024 * 1024_u64;
        let manager = HostingManager::new(custom_budget);
        assert_eq!(
            manager.hosting_budget_bytes(),
            custom_budget,
            "HostingManager::new should pass the budget through to the cache"
        );

        // The default constructor still uses the in-code default.
        let default_manager = HostingManager::default();
        assert_eq!(
            default_manager.hosting_budget_bytes(),
            DEFAULT_HOSTING_BUDGET_BYTES
        );
    }

    #[tokio::test]
    async fn test_subscribe_renews_existing() {
        let manager = HostingManager::new(DEFAULT_HOSTING_BUDGET_BYTES);
        let contract = make_contract_key(1);

        let first = manager.subscribe(contract);
        let second = manager.subscribe(contract);

        assert!(first.is_new);
        assert!(!second.is_new);
        assert!(second.expires_at >= first.expires_at);
    }

    #[tokio::test]
    async fn test_unsubscribe_removes_subscription() {
        let manager = HostingManager::new(DEFAULT_HOSTING_BUDGET_BYTES);
        let contract = make_contract_key(1);

        manager.subscribe(contract);
        assert!(manager.is_subscribed(&contract));

        manager.unsubscribe(&contract);
        assert!(!manager.is_subscribed(&contract));
    }

    #[tokio::test]
    async fn test_renew_subscription() {
        let manager = HostingManager::new(DEFAULT_HOSTING_BUDGET_BYTES);
        let contract = make_contract_key(1);

        // Renew non-existent subscription fails
        assert!(!manager.renew_subscription(&contract));

        // Subscribe then renew succeeds
        manager.subscribe(contract);
        assert!(manager.renew_subscription(&contract));
    }

    #[tokio::test]
    async fn test_get_subscribed_contracts() {
        let manager = HostingManager::new(DEFAULT_HOSTING_BUDGET_BYTES);
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

    /// Pin: `subscribe(...)` must be visible in
    /// `dashboard_subscription_snapshot()` immediately. The
    /// previous parallel `network_status` mirror silently drifted
    /// after the SUBSCRIBE migration (PR #3806 → #3981).
    #[tokio::test]
    async fn dashboard_snapshot_reflects_active_subscriptions() {
        let manager = HostingManager::new(DEFAULT_HOSTING_BUDGET_BYTES);
        let c1 = make_contract_key(1);
        let c2 = make_contract_key(2);

        // Empty before any subscription.
        assert!(manager.dashboard_subscription_snapshot().is_empty());

        // Subscribing makes the contract visible to the dashboard.
        manager.subscribe(c1);
        let snap = manager.dashboard_subscription_snapshot();
        assert_eq!(snap.len(), 1);
        assert_eq!(snap[0].key, c1);
        assert!(snap[0].last_updated_secs.is_none());

        // record_contract_update populates last_updated_secs.
        manager.record_contract_update(&c1);
        let snap = manager.dashboard_subscription_snapshot();
        assert!(snap[0].last_updated_secs.is_some());

        // Multiple subscriptions are reflected.
        manager.subscribe(c2);
        let snap = manager.dashboard_subscription_snapshot();
        assert_eq!(snap.len(), 2);

        // Unsubscribe removes the entry.
        manager.unsubscribe(&c1);
        let snap = manager.dashboard_subscription_snapshot();
        assert_eq!(snap.len(), 1);
        assert_eq!(snap[0].key, c2);

        // record_contract_update on a non-subscribed contract is a no-op
        // (matches the legacy network_status::record_contract_updated
        // semantics, which silently dropped updates for unknown keys).
        manager.record_contract_update(&c1);
        let snap = manager.dashboard_subscription_snapshot();
        assert_eq!(snap.len(), 1);
        assert!(snap.iter().all(|s| s.key != c1));
    }

    /// Sort order of `dashboard_subscription_snapshot()` must be
    /// deterministic — DashMap iteration order would otherwise leak
    /// through to the rendered dashboard, reshuffling rows on every
    /// 5-second poll. Ties (including `None`/`None` for never-updated
    /// entries) must break by contract-key bytes.
    #[tokio::test]
    async fn dashboard_snapshot_sort_is_deterministic_on_ties() {
        let manager = HostingManager::new(DEFAULT_HOSTING_BUDGET_BYTES);
        // Three contracts with distinct, ordered key-byte prefixes
        // (`make_contract_key(seed)` writes `[seed; 32]` into the
        // ContractInstanceId, so seeds 0x10/0x40/0xF0 sort low/mid/high).
        let low = make_contract_key(0x10);
        let mid = make_contract_key(0x40);
        let high = make_contract_key(0xF0);

        // Subscribe all three, then drive `last_updated` to the same
        // wall-clock timestamp for `low` and `high`. `mid` stays
        // never-updated, so it must sort to the end.
        manager.subscribe(low);
        manager.subscribe(mid);
        manager.subscribe(high);
        manager.record_contract_update(&high);
        manager.record_contract_update(&low);

        let snap = manager.dashboard_subscription_snapshot();
        assert_eq!(snap.len(), 3);
        // `low` and `high` share `last_updated_secs` (both 0 immediately
        // after `record_contract_update`); the byte-key tie-break must
        // place `low` before `high`. `mid` (never updated) goes last.
        assert_eq!(
            snap.iter().map(|s| s.key).collect::<Vec<_>>(),
            vec![low, high, mid],
            "snapshot must be ordered (low, high, mid); got {:?}",
            snap.iter().map(|s| s.key).collect::<Vec<_>>()
        );

        // Re-poll: the order MUST be the same. (Pre-fix: DashMap
        // iteration order would shuffle on every call.)
        for _ in 0..5 {
            let again = manager.dashboard_subscription_snapshot();
            assert_eq!(
                again.iter().map(|s| s.key).collect::<Vec<_>>(),
                vec![low, high, mid],
                "repeated snapshots must be byte-stable"
            );
        }
    }

    /// Subscription renewal must not reset `subscribed_since`, otherwise
    /// the dashboard's "subscribed for X seconds" reading would flip back
    /// to ~0 every renewal interval (2 min) for every River user.
    #[tokio::test]
    async fn dashboard_snapshot_preserves_subscribed_since_across_renewals() {
        let manager = HostingManager::new(DEFAULT_HOSTING_BUDGET_BYTES);
        let c = make_contract_key(1);

        let read_lease = || {
            *manager
                .active_subscriptions
                .get(&c)
                .expect("subscription must exist")
        };

        manager.subscribe(c);
        let initial = read_lease();

        manager.subscribe(c);
        let renewed = read_lease();

        assert_eq!(
            renewed.subscribed_since, initial.subscribed_since,
            "subscribed_since must be preserved across renewals"
        );
        assert!(
            renewed.expires_at >= initial.expires_at,
            "expires_at must monotonically advance on renewal"
        );
    }

    #[tokio::test]
    async fn test_active_subscription_count() {
        let manager = HostingManager::new(DEFAULT_HOSTING_BUDGET_BYTES);

        assert_eq!(manager.active_subscription_count(), 0);

        manager.subscribe(make_contract_key(1));
        manager.subscribe(make_contract_key(2));
        assert_eq!(manager.active_subscription_count(), 2);

        manager.unsubscribe(&make_contract_key(1));
        assert_eq!(manager.active_subscription_count(), 1);
    }

    #[test]
    fn test_client_subscription_basic() {
        let manager = HostingManager::new(DEFAULT_HOSTING_BUDGET_BYTES);
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
        let manager = HostingManager::new(DEFAULT_HOSTING_BUDGET_BYTES);
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
        let manager = HostingManager::new(DEFAULT_HOSTING_BUDGET_BYTES);
        let key = make_contract_key(1);

        assert!(!manager.is_hosting_contract(&key));
        assert_eq!(manager.hosting_contracts_count(), 0);

        manager.record_contract_access(key, 1000, AccessType::Put);

        assert!(manager.is_hosting_contract(&key));
        assert_eq!(manager.hosting_contracts_count(), 1);
    }

    #[test]
    fn test_subscription_backoff() {
        let manager = HostingManager::new(DEFAULT_HOSTING_BUDGET_BYTES);
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
        let manager = HostingManager::new(DEFAULT_HOSTING_BUDGET_BYTES);
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
        let manager = HostingManager::new(DEFAULT_HOSTING_BUDGET_BYTES);
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
        let manager = HostingManager::new(DEFAULT_HOSTING_BUDGET_BYTES);
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
        let manager = HostingManager::new(DEFAULT_HOSTING_BUDGET_BYTES);
        let contract = make_contract_key(1);
        let client_id = crate::client_events::ClientId::next();

        assert!(!manager.is_receiving_updates(&contract));

        manager.add_client_subscription(contract.id(), client_id);
        assert!(manager.is_receiving_updates(&contract));
    }

    #[test]
    fn test_contracts_needing_renewal_excludes_hosted_only() {
        let manager = HostingManager::new(DEFAULT_HOSTING_BUDGET_BYTES);
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

    /// Regression: a node that merely relays a SUBSCRIBE response for some
    /// other peer must NOT end up with the contract in its own
    /// `active_subscriptions`, and consequently must NOT appear in
    /// `contracts_needing_renewal()`.
    ///
    /// Before the fix to `operations::subscribe::SubscribeMsgResult::Subscribed`,
    /// every relay on a SUBSCRIBE response path called `ring.subscribe(*key)`
    /// unconditionally. That installed a lease in `active_subscriptions`,
    /// which `contracts_needing_renewal()` path #1 would then pick up every
    /// ~2 minutes and spawn a fresh subscribe for — routing through new
    /// relays that *also* installed leases, compounding with each cycle.
    /// The feedback loop shows up as the 85+ phantom contracts observed on
    /// the `technic` peer's local dashboard (see commit message).
    ///
    /// This test models the post-fix relay state as "contract has a
    /// downstream subscriber registered, but no `subscribe()` lease", which
    /// is what the SUBSCRIBE Response relay branch now does. The assertion
    /// is that such a relay does not get recruited into the renewal cycle.
    #[test]
    fn test_relay_downstream_only_not_in_renewal() {
        let manager = HostingManager::new(DEFAULT_HOSTING_BUDGET_BYTES);
        let contract = make_contract_key(77);
        let downstream = make_peer_key(42);

        // Relay state: we've accepted a downstream subscriber for the
        // contract, but we have not called `subscribe()` on our own behalf
        // (we're just forwarding Updates for someone else) and we have no
        // local client expressing interest.
        assert!(manager.add_downstream_subscriber(&contract, downstream.clone()));

        // Invariant 1: we did not install a self-subscription lease.
        assert!(
            !manager.is_subscribed(&contract),
            "Relay must not have an active subscription lease just from \
             registering a downstream subscriber"
        );
        assert!(
            manager.get_subscribed_contracts().is_empty(),
            "active_subscriptions must be empty on a pure-relay peer"
        );

        // Invariant 2: the contract is not in the renewal set. This is the
        // load-bearing property: if the relay were in `active_subscriptions`,
        // `contracts_needing_renewal()` path #1 (expiring active leases)
        // would pick it up and spawn a new subscribe, recruiting more
        // relays. Pure downstream registration must NOT trigger renewal.
        let needs_renewal = manager.contracts_needing_renewal();
        assert!(
            !needs_renewal.contains(&contract),
            "Pure-relay peer must not appear in contracts_needing_renewal \
             (relay-subscription feedback loop regression, see \
             subscribe.rs::SubscribeMsgResult::Subscribed)"
        );

        // Invariant 3: downstream registration still works as intended —
        // the relay holds the downstream peer so UPDATE broadcasts can be
        // forwarded. This is the *correct* mechanism for a relay to receive
        // and propagate updates, without inflating subscription trees.
        assert!(manager.has_downstream_subscribers(&contract));
    }

    // Superseded: startup revalidation window removed in #3546 to prevent
    // subscription accumulation storms. Hosted-only contracts are no longer
    // proactively renewed at startup. Replaced by test_hosted_contracts_not_renewed_at_scale.
    #[ignore]
    #[test]
    fn test_hosted_contract_renewed_despite_no_interest() {
        let manager = HostingManager::new(DEFAULT_HOSTING_BUDGET_BYTES);
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
        let manager = HostingManager::new(DEFAULT_HOSTING_BUDGET_BYTES);
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
        let manager = HostingManager::new(DEFAULT_HOSTING_BUDGET_BYTES);
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
        let manager = HostingManager::new(DEFAULT_HOSTING_BUDGET_BYTES);
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
        let manager = HostingManager::new(DEFAULT_HOSTING_BUDGET_BYTES);
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
        let manager = HostingManager::new(DEFAULT_HOSTING_BUDGET_BYTES);

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
        let manager = HostingManager::new(DEFAULT_HOSTING_BUDGET_BYTES);
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
        let manager = HostingManager::new(DEFAULT_HOSTING_BUDGET_BYTES);
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
        let manager = HostingManager::new(DEFAULT_HOSTING_BUDGET_BYTES);
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
        let manager = HostingManager::new(DEFAULT_HOSTING_BUDGET_BYTES);
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
        let manager = HostingManager::new(DEFAULT_HOSTING_BUDGET_BYTES);
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
        let manager = HostingManager::new(DEFAULT_HOSTING_BUDGET_BYTES);
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
        let manager = HostingManager::new(DEFAULT_HOSTING_BUDGET_BYTES);
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
        let manager = HostingManager::new(DEFAULT_HOSTING_BUDGET_BYTES);
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
        let manager = HostingManager::new(DEFAULT_HOSTING_BUDGET_BYTES);
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
        let manager = HostingManager::new(DEFAULT_HOSTING_BUDGET_BYTES);
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
        let manager = HostingManager::new(DEFAULT_HOSTING_BUDGET_BYTES);
        let contract = make_contract_key(2);
        let known_peer = make_peer_key(20);
        let unknown_peer = make_peer_key(99);

        manager.add_downstream_subscriber(&contract, known_peer.clone());

        assert!(!manager.remove_downstream_subscriber(&contract, &unknown_peer));
        assert!(manager.has_downstream_subscribers(&contract));
        assert!(!manager.should_unsubscribe_upstream(&contract));
    }

    // ----------------------------------------------------------------------
    // contract_in_use — the eviction-reclamation gate.
    //
    // `operations::reclaim_evicted_contract` MUST NOT emit an EvictContract
    // event (which would delete the contract's state/code from disk) for a
    // contract that is still in use. `contract_in_use` is that gate.
    // ----------------------------------------------------------------------

    /// A freshly-evicted contract with no client or downstream subscribers is
    /// NOT in use — reclamation may proceed.
    #[test]
    fn test_contract_in_use_false_when_no_subscribers() {
        let manager = HostingManager::new(DEFAULT_HOSTING_BUDGET_BYTES);
        let contract = make_contract_key(1);
        assert!(
            !manager.contract_in_use(&contract),
            "a contract with no subscribers must not be considered in use"
        );
    }

    /// A contract with a live client subscription IS in use — the gate must
    /// keep its on-disk storage.
    #[test]
    fn test_contract_in_use_true_with_client_subscription() {
        let manager = HostingManager::new(DEFAULT_HOSTING_BUDGET_BYTES);
        let contract = make_contract_key(2);
        let client = crate::client_events::ClientId::next();

        manager.add_client_subscription(contract.id(), client);
        assert!(
            manager.contract_in_use(&contract),
            "a contract with a client subscription must be considered in use"
        );

        // After the last client unsubscribes, the contract is reclaimable.
        manager.remove_client_subscription(contract.id(), client);
        assert!(
            !manager.contract_in_use(&contract),
            "contract must become reclaimable once its last client unsubscribes"
        );
    }

    /// A contract with a downstream peer subscriber IS in use.
    #[test]
    fn test_contract_in_use_true_with_downstream_subscriber() {
        let manager = HostingManager::new(DEFAULT_HOSTING_BUDGET_BYTES);
        let contract = make_contract_key(3);
        let peer = make_peer_key(7);

        manager.add_downstream_subscriber(&contract, peer.clone());
        assert!(
            manager.contract_in_use(&contract),
            "a contract with a downstream subscriber must be considered in use"
        );

        manager.remove_downstream_subscriber(&contract, &peer);
        assert!(
            !manager.contract_in_use(&contract),
            "contract must become reclaimable once its last downstream subscriber leaves"
        );
    }

    /// A contract with ONLY an active upstream network subscription (no
    /// local client, no downstream subscriber) is NOT in use for
    /// reclamation purposes. Documented in `contract_in_use`'s rustdoc:
    /// `contracts_needing_renewal` renews active subscriptions
    /// unconditionally, so including `is_subscribed` here would be an
    /// effectively unbounded GC exemption (AGENTS.md cleanup-exemption
    /// rule). Local-client subscriptions and downstream-peer subscribers
    /// are both time-bounded and remain in the predicate.
    #[test]
    fn test_contract_in_use_excludes_network_subscription_only() {
        let manager = HostingManager::new(DEFAULT_HOSTING_BUDGET_BYTES);
        let contract = make_contract_key(4);

        assert!(!manager.has_client_subscriptions(contract.id()));
        assert!(!manager.has_downstream_subscribers(&contract));
        assert!(!manager.contract_in_use(&contract));

        // Establishing an upstream network subscription alone must NOT
        // make the contract appear in-use, because the renewal machinery
        // would keep extending the lease forever.
        manager.subscribe(contract);
        assert!(manager.is_subscribed(&contract));
        assert!(
            !manager.contract_in_use(&contract),
            "an active upstream network subscription alone must NOT block \
             reclamation (the renewal machinery would keep it alive \
             unboundedly — see contract_in_use rustdoc)"
        );

        manager.unsubscribe(&contract);
        assert!(!manager.contract_in_use(&contract));
    }

    /// Generation flow through `HostingManager`: bumping the state
    /// generation BEFORE `record_contract_access` makes the captured
    /// generation match; subsequently bumping the generation simulates
    /// a write that raced ahead of an `EvictContract`, and the captured
    /// snapshot on the evicted entry is now stale (less than current).
    ///
    /// This is the load-bearing flow the `RuntimePool::remove_contract`
    /// generation-mismatch guard relies on. PR #4212 review round C.
    #[test]
    fn test_record_contract_access_captures_then_diverges_from_current_generation() {
        let manager = HostingManager::new(DEFAULT_HOSTING_BUDGET_BYTES);
        // Tiny cache, zero TTL: any insert past the first immediately
        // evicts the previous entry.
        {
            let mut cache = manager.hosting_cache.write();
            *cache = cache::HostingCache::new(
                100,
                std::time::Duration::ZERO,
                crate::util::time_source::InstantTimeSrc::new(),
            );
        }

        let key = make_contract_key(1);
        let trigger = make_contract_key(2);

        // Simulate three state writes before the hosting record.
        assert_eq!(manager.bump_state_generation(&key), 1);
        assert_eq!(manager.bump_state_generation(&key), 2);
        assert_eq!(manager.bump_state_generation(&key), 3);
        assert_eq!(manager.state_generation(&key), 3);

        // Recording the access captures the current generation (3).
        manager.record_contract_access(key, 100, AccessType::Get);

        // Simulate a state write that races ahead of `EvictContract`.
        let new_generation = manager.bump_state_generation(&key);
        assert_eq!(new_generation, 4);

        // Now evict the entry by inserting `trigger`; the captured
        // generation on the evicted tuple must be the snapshot taken at
        // `record_contract_access` time (3), NOT the current value (4).
        // `RuntimePool::remove_contract` will compare this captured
        // value (3) against the current `state_generation` (4) and
        // SKIP the on-disk reclamation, closing the re-host race.
        let result = manager.record_contract_access(trigger, 100, AccessType::Get);
        assert_eq!(
            result.evicted,
            vec![(key, 3)],
            "evicted tuple must carry the generation captured atomically \
             when the entry was inserted, NOT the current generation"
        );
        assert_eq!(
            manager.state_generation(&key),
            4,
            "current generation must reflect the most recent write"
        );
        assert_ne!(
            result.evicted[0].1,
            manager.state_generation(&key),
            "the mismatch between captured and current is exactly what \
             `RuntimePool::remove_contract` keys off to skip reclamation"
        );
    }

    /// Without `refresh_cache_generation`, a hosted contract that receives
    /// a subsequent state write (UPDATE or re-PUT) has its `state_generation`
    /// advance while the cached `write_generation` snapshot stays at the
    /// `record_contract_access`-time value. Later, when this contract is
    /// evicted (LRU pressure, expiry sweep, etc.), the `EvictContract` event
    /// carries the stale snapshot. The deletion-time guard in
    /// `RuntimePool::remove_contract` compares the snapshot against the
    /// current generation and — seeing a mismatch — skips reclamation.
    /// Result: every UPDATE-then-evict leaks the on-disk state and code blob.
    ///
    /// The fix is to call `refresh_cache_generation` paired with every
    /// `bump_state_generation` so the snapshot tracks the counter. This
    /// test asserts the refresh updates the snapshot, so the
    /// subsequently-evicted entry carries the current generation rather
    /// than the stale one.
    ///
    /// Regression test for PR #4212 review round D (skeptical r3 #2).
    #[test]
    fn test_record_access_refresh_updates_write_generation() {
        let manager = HostingManager::new(DEFAULT_HOSTING_BUDGET_BYTES);
        // Tiny cache, zero TTL so the next insert evicts immediately.
        {
            let mut cache = manager.hosting_cache.write();
            *cache = cache::HostingCache::new(
                100,
                std::time::Duration::ZERO,
                crate::util::time_source::InstantTimeSrc::new(),
            );
        }

        let key = make_contract_key(1);
        let trigger = make_contract_key(2);

        // Initial write + hosting record: snapshot captures generation 1.
        let new_gen = manager.bump_state_generation(&key);
        assert_eq!(new_gen, 1);
        manager.refresh_cache_generation(&key, new_gen);
        manager.record_contract_access(key, 100, AccessType::Get);

        // Simulate an UPDATE that bumps the counter to 2 AND refreshes
        // the cached snapshot — this is the bump+refresh pair installed
        // at every state-write chokepoint.
        let new_gen = manager.bump_state_generation(&key);
        assert_eq!(new_gen, 2);
        manager.refresh_cache_generation(&key, new_gen);

        // Now force eviction. With the refresh, the evicted tuple should
        // carry the post-UPDATE generation (2). Without the refresh, it
        // would carry the stale snapshot (1), and `RuntimePool::remove_contract`
        // would see a mismatch against the current generation (2) and
        // SKIP reclamation — leaking the on-disk state forever.
        let result = manager.record_contract_access(trigger, 100, AccessType::Get);
        assert_eq!(
            result.evicted,
            vec![(key, 2)],
            "evicted tuple must carry the refreshed generation (post-UPDATE), \
             not the stale snapshot from initial record_contract_access"
        );
        assert_eq!(
            result.evicted[0].1,
            manager.state_generation(&key),
            "with bump+refresh in lock-step, the evicted snapshot matches \
             the current generation — deletion-time guard would proceed \
             with reclamation rather than skipping it"
        );
    }

    /// `refresh_cache_generation` is a no-op when the entry is not in the
    /// cache: if the contract was evicted between bump and refresh, the
    /// `EvictContract` already carried the pre-bump snapshot and the
    /// deletion-time guard will skip on that narrower mismatch. The
    /// no-op behavior is intentional — see the comment on
    /// `HostingCache::refresh_entry_generation`.
    #[test]
    fn test_refresh_cache_generation_is_noop_when_entry_absent() {
        let manager = HostingManager::new(DEFAULT_HOSTING_BUDGET_BYTES);
        let key = make_contract_key(99);

        // The contract is not in the hosting cache. The bump+refresh
        // pair runs from a state-write chokepoint, but the entry was
        // already evicted in a prior eviction wave. The refresh must
        // simply do nothing — not panic, not insert.
        let new_gen = manager.bump_state_generation(&key);
        manager.refresh_cache_generation(&key, new_gen);
        assert!(
            !manager.hosting_cache.read().contains(&key),
            "refresh must not insert a phantom entry for an absent contract"
        );
    }

    /// `bump_state_generation` is monotonic and starts at 1 on first
    /// bump (`state_generation` returns 0 for never-seen contracts).
    /// `forget_state_generation` returns the entry to the absent state
    /// so the next bump restarts at 1.
    #[test]
    fn test_state_generation_lifecycle() {
        let manager = HostingManager::new(DEFAULT_HOSTING_BUDGET_BYTES);
        let key = make_contract_key(42);

        assert_eq!(
            manager.state_generation(&key),
            0,
            "never-seen contract reads as generation 0"
        );

        assert_eq!(manager.bump_state_generation(&key), 1);
        assert_eq!(manager.bump_state_generation(&key), 2);
        assert_eq!(manager.bump_state_generation(&key), 3);
        assert_eq!(manager.state_generation(&key), 3);

        manager.forget_state_generation(&key);
        assert_eq!(
            manager.state_generation(&key),
            0,
            "after forget, generation reads as 0 again"
        );
        assert_eq!(
            manager.bump_state_generation(&key),
            1,
            "after forget, next bump restarts at 1"
        );
    }

    /// `record_contract_access` must not evict an in-use contract when the
    /// cache is over budget; once the in-use signal is removed the contract
    /// becomes evictable. The in-use signal here is a local client
    /// subscription — the time-bounded form `contract_in_use` actually
    /// checks (see its rustdoc for why an upstream network subscription
    /// alone is excluded).
    #[test]
    fn test_record_contract_access_skips_in_use_contract() {
        let manager = HostingManager::new(DEFAULT_HOSTING_BUDGET_BYTES);
        // Override with a tiny cache and ZERO TTL so every entry is instantly
        // eviction-eligible — `contract_in_use` is then the only protection.
        {
            let mut cache = manager.hosting_cache.write();
            *cache = cache::HostingCache::new(
                200, // room for ~2 contracts at 100 bytes
                std::time::Duration::ZERO,
                crate::util::time_source::InstantTimeSrc::new(),
            );
        }

        let in_use = make_contract_key(1);
        let filler = make_contract_key(2);
        let trigger = make_contract_key(3);

        // `in_use` is the oldest LRU entry but has a local client subscription.
        let client = crate::client_events::ClientId::next();
        manager.add_client_subscription(in_use.id(), client);
        assert!(manager.contract_in_use(&in_use));

        manager.record_contract_access(in_use, 100, AccessType::Get);
        manager.record_contract_access(filler, 100, AccessType::Get);

        // Inserting `trigger` puts the cache over budget. A naive LRU would
        // evict `in_use` (oldest) — `contract_in_use` must protect it, so
        // `filler` is evicted instead.
        let result = manager.record_contract_access(trigger, 100, AccessType::Get);
        assert_eq!(
            result.evicted,
            vec![(filler, 0)],
            "in-use (client-subscribed) contract must be skipped; the \
             unprotected contract must be evicted instead"
        );
        assert!(manager.is_hosting_contract(&in_use));
        assert!(!manager.is_hosting_contract(&filler));

        // Drop the client subscription: `in_use` is now evictable.
        manager.remove_client_subscription(in_use.id(), client);
        assert!(!manager.contract_in_use(&in_use));

        let result = manager.record_contract_access(filler, 100, AccessType::Get);
        assert!(
            result.evicted.iter().any(|(k, _)| *k == in_use),
            "once the subscription is removed the contract must become \
             evictable when the cache is over budget"
        );
        assert!(!manager.is_hosting_contract(&in_use));
    }

    /// Removing from an untracked contract is a noop; other contracts unaffected.
    #[test]
    fn test_unsubscribe_handler_unknown_contract_is_noop() {
        let manager = HostingManager::new(DEFAULT_HOSTING_BUDGET_BYTES);
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
        let manager = HostingManager::new(DEFAULT_HOSTING_BUDGET_BYTES);
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

        cache.record_access(protected, 100, AccessType::Get, 0, |_| false);
        cache.record_access(unprotected, 100, AccessType::Get, 0, |_| false);
        assert_eq!(cache.current_bytes(), 200); // over budget

        // Advance past TTL
        time.advance_time(Duration::from_secs(61));

        // Sweep with predicate that protects the first contract
        // (simulates has_downstream_subscribers returning true)
        let evicted = cache.sweep_expired(|k| *k == protected);

        assert!(
            !evicted.iter().any(|(k, _)| *k == protected),
            "Contract with downstream subscribers must not be evicted"
        );
        assert!(
            evicted.iter().any(|(k, _)| *k == unprotected),
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
        cache.record_access(contract, 100, AccessType::Get, 0, |_| false);
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
            evicted.iter().any(|(k, _)| *k == contract),
            "Contract past TTL with no subscribers should be evicted"
        );
        assert!(!cache.contains(&contract));
    }

    // =========================================================================
    // Downstream Subscriber Limit Tests
    // =========================================================================

    #[test]
    fn test_downstream_subscriber_limit_enforced() {
        let manager = HostingManager::new(DEFAULT_HOSTING_BUDGET_BYTES);
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
        let manager = HostingManager::new(DEFAULT_HOSTING_BUDGET_BYTES);
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
        let manager = HostingManager::new(DEFAULT_HOSTING_BUDGET_BYTES);
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
        let manager = HostingManager::new(DEFAULT_HOSTING_BUDGET_BYTES);
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
        let manager = HostingManager::new(DEFAULT_HOSTING_BUDGET_BYTES);

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
        let manager = HostingManager::new(DEFAULT_HOSTING_BUDGET_BYTES);
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
        let manager = HostingManager::new(DEFAULT_HOSTING_BUDGET_BYTES);
        let contract = make_contract_key(1);

        assert!(!manager.has_local_client_access(&contract));
        manager.mark_local_client_access(&contract); // no-op, not in cache
        assert!(!manager.has_local_client_access(&contract));
    }

    /// The local_client_access flag should be sticky -- once set, it should
    /// persist even after the contract's access type changes.
    #[test]
    fn test_local_client_access_sticky_across_access_type_changes() {
        let manager = HostingManager::new(DEFAULT_HOSTING_BUDGET_BYTES);
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
        let manager = HostingManager::new(DEFAULT_HOSTING_BUDGET_BYTES);

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
        let manager = HostingManager::new(DEFAULT_HOSTING_BUDGET_BYTES);
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

    // =========================================================================
    // Pending Reclamation Retry Queue (PR #4212 review round 7)
    //
    // The queue catches two narrow disk-leak edge cases — fair-queue
    // rejection of `EvictContract`, and the `contract_in_use` skip in
    // `RuntimePool::remove_contract` — where an `EvictContract` event
    // is dropped before reclamation runs but the hosting-cache entry is
    // already gone. The queue is drained by the periodic sweep, which
    // re-emits `EvictContract` via `reclaim_evicted_contract`.
    //
    // End-to-end coverage of the periodic sweep retry path (which
    // requires a wired `OpManager`) is intentionally deferred —
    // constructing a `RuntimePool` is too heavy for a unit test (see
    // the note on `remove_contract_tests` in
    // `contract/executor/runtime.rs`). These tests cover the manager-
    // level API the sweep relies on.
    // =========================================================================

    /// Basic API: add → snapshot reflects the entry; remove → snapshot
    /// becomes empty. The snapshot returns owned tuples (no lock held
    /// across iteration), which is the property the periodic sweep
    /// relies on.
    #[test]
    fn test_pending_reclamation_add_remove_snapshot() {
        let manager = HostingManager::new(DEFAULT_HOSTING_BUDGET_BYTES);
        let key_a = make_contract_key(1);
        let key_b = make_contract_key(2);

        assert_eq!(manager.pending_reclamation_len(), 0);
        assert!(manager.pending_reclamation_snapshot().is_empty());

        manager.pending_reclamation_add(key_a, 7);
        manager.pending_reclamation_add(key_b, 13);
        assert_eq!(manager.pending_reclamation_len(), 2);

        let mut snapshot = manager.pending_reclamation_snapshot();
        snapshot.sort_by(|a, b| a.0.id().as_bytes().cmp(b.0.id().as_bytes()));
        assert_eq!(snapshot, vec![(key_a, 7), (key_b, 13)]);

        // Re-adding the same key replaces the generation. This matters
        // for the queue-full skip point: if multiple eviction events
        // for the same key race the queue, the most recent generation
        // is the relevant one for the retry.
        manager.pending_reclamation_add(key_a, 99);
        let snapshot = manager.pending_reclamation_snapshot();
        let gen_a = snapshot
            .iter()
            .find(|(k, _)| *k == key_a)
            .map(|(_, g)| *g)
            .expect("key_a still present");
        assert_eq!(gen_a, 99, "re-add must replace the generation");

        manager.pending_reclamation_remove(&key_a);
        assert_eq!(manager.pending_reclamation_len(), 1);
        let remaining = manager.pending_reclamation_snapshot();
        assert_eq!(remaining, vec![(key_b, 13)]);

        manager.pending_reclamation_remove(&key_b);
        assert_eq!(manager.pending_reclamation_len(), 0);

        // Removing a key that is not present is a no-op (matters because
        // the success path in `RuntimePool::remove_contract` calls
        // `pending_reclamation_remove` unconditionally — the queue must
        // tolerate non-pending keys).
        manager.pending_reclamation_remove(&key_a);
        assert_eq!(manager.pending_reclamation_len(), 0);
    }

    /// Simulate the `contract_in_use` skip path: an EvictContract event
    /// could not complete because a subscriber appeared between
    /// eviction and processing. The pending entry survives subsequent
    /// snapshots so the periodic sweep can keep retrying; once the
    /// subscriber expires the snapshot still contains the entry and a
    /// successful retry would call `pending_reclamation_remove` to
    /// clear it.
    ///
    /// This is the manager-level invariant; end-to-end coverage of the
    /// sweep loop calling `reclaim_evicted_contract` for each entry
    /// (which requires a wired `OpManager`) is deferred — see the
    /// module-level test note.
    #[test]
    fn test_pending_reclamation_survives_in_use_skip_and_retries() {
        let manager = HostingManager::new(DEFAULT_HOSTING_BUDGET_BYTES);
        let contract = make_contract_key(42);
        let client = crate::client_events::ClientId::next();
        let captured_generation = 5u64;

        // Step 1: a client subscription means `contract_in_use` is true.
        // In production this is the state `RuntimePool::remove_contract`
        // observes when it hits the in-use skip and adds the key to the
        // pending queue.
        manager.add_client_subscription(contract.id(), client);
        assert!(manager.contract_in_use(&contract));
        manager.pending_reclamation_add(contract, captured_generation);
        assert_eq!(manager.pending_reclamation_len(), 1);

        // Step 2: the periodic sweep snapshots the queue. The entry is
        // returned with its captured generation intact, and the queue
        // state is unchanged (the sweep does not consume entries —
        // `reclaim_evicted_contract`'s `contract_in_use` gate filters
        // them, and successful retries call `pending_reclamation_remove`
        // explicitly).
        let snapshot = manager.pending_reclamation_snapshot();
        assert_eq!(snapshot, vec![(contract, captured_generation)]);
        assert_eq!(
            manager.pending_reclamation_len(),
            1,
            "snapshot must NOT drain the queue — entries stay until \
             explicit removal so the sweep can keep retrying until \
             `contract_in_use` becomes false"
        );

        // Step 3: subscriber leaves; `contract_in_use` becomes false.
        // The next sweep would route this through
        // `reclaim_evicted_contract`, which (with the gate now open)
        // emits a fresh `EvictContract`. On successful reclamation,
        // `RuntimePool::remove_contract` calls
        // `pending_reclamation_remove`. We model the successful retry
        // here by calling `pending_reclamation_remove` directly.
        manager.remove_client_subscription(contract.id(), client);
        assert!(!manager.contract_in_use(&contract));
        // The sweep would re-snapshot at this point and route through
        // reclaim_evicted_contract — model the successful path.
        manager.pending_reclamation_remove(&contract);
        assert_eq!(manager.pending_reclamation_len(), 0);
        assert!(manager.pending_reclamation_snapshot().is_empty());
    }
}
