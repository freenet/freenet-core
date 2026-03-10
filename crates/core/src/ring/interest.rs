//! Unified interest tracking for delta-based state synchronization.
//!
//! NOTE: This module provides foundation infrastructure for delta-based updates.
//! Many items are marked `#[allow(dead_code)]` because they will be used in
//! follow-up PRs that integrate the full delta sync workflow.
#![allow(dead_code)]
//!
//! This module provides the infrastructure for tracking which peers are interested
//! in which contracts, along with their state summaries. This enables delta-based
//! updates where we send only the changes rather than full contract state.
//!
//! # Core Concepts
//!
//! ## Interest vs Subscribe
//!
//! - **Interest** (neighbor-scoped): "Update me if you have it"
//!   - Exchanged between directly connected peers
//!   - No network propagation if peer doesn't have state
//!   - Used for proximity-style coordination
//!
//! - **Subscribe** (network-scoped): "Update me, and subscribe upstream if needed"
//!   - May propagate through the network
//!   - Establishes subscription tree
//!   - Used when client explicitly requests a contract
//!
//! Both result in summary exchange for delta computation. The update/delta mechanism
//! doesn't care WHY a peer is interested - only which peers want updates and their
//! current state summaries.
//!
//! ## Interest Lifecycle
//!
//! Interests expire after a TTL (20 minutes) unless refreshed. A background
//! heartbeat task sends `Interests { hashes }` to each connected peer every
//! 5 minutes, which refreshes the TTL. The TTL is 4x the heartbeat interval
//! to tolerate up to 3 consecutive missed heartbeats before expiry.
//!
//! Additional refresh triggers:
//! - Sending/receiving updates
//! - Summaries exchange
//! - Receiving `ChangeInterests { added }`
//!
//! This self-healing mechanism catches forgotten cleanup and prevents zombie interests.

use dashmap::DashMap;
use freenet_stdlib::prelude::{ContractKey, StateDelta, StateSummary};
use lru::LruCache;
use parking_lot::Mutex;
use std::collections::{HashMap, HashSet};
use std::num::NonZeroUsize;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;
use tokio::time::Instant;

use crate::transport::TransportPublicKey;
use crate::util::time_source::TimeSource;

/// Interval between interest heartbeat messages sent to each peer.
/// Each heartbeat sends a full `Interests { hashes }` message which refreshes
/// the peer's interest entries on the remote side.
pub const INTEREST_HEARTBEAT_INTERVAL: Duration = Duration::from_secs(300); // 5 minutes

/// TTL for peer interests. Set to 4x the heartbeat interval so that up to
/// 3 consecutive missed heartbeats are tolerated before expiry.
pub const INTEREST_TTL: Duration = Duration::from_secs(INTEREST_HEARTBEAT_INTERVAL.as_secs() * 4); // 20 minutes

/// Interval for background sweep to clean up expired interests.
pub const INTEREST_SWEEP_INTERVAL: Duration = Duration::from_secs(60); // 1 minute

/// Grace period before removing a disconnected peer's interests.
///
/// When a peer disconnects, we defer interest removal for this duration instead of
/// wiping immediately. If the peer reconnects within the grace period, the pending
/// removal is cancelled and interests are preserved. This prevents permanent interest
/// loss for peers with unstable connections (e.g., stale pending reservations causing
/// ~60s disconnect/reconnect cycles). Set to 90s to comfortably survive such cycles.
pub const INTEREST_DISCONNECT_GRACE_PERIOD: Duration = Duration::from_secs(90);

use crate::config::GlobalExecutor;
use crate::config::GlobalRng;

/// Maximum number of entries in the delta memoization cache.
const DELTA_CACHE_SIZE: usize = 1024;

/// Timeout for contract handler queries in the broadcast path (summary and
/// delta computation). Much shorter than the default 300s to prevent spawned
/// broadcast tasks from accumulating when the contract handler is slow.
const BROADCAST_CH_TIMEOUT: Duration = Duration::from_secs(10);

/// Identifies a peer for interest tracking purposes.
///
/// Uses the peer's public key rather than socket address, since addresses
/// can change (NAT, reconnection) but the key is stable.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct PeerKey(pub TransportPublicKey);

impl From<TransportPublicKey> for PeerKey {
    fn from(key: TransportPublicKey) -> Self {
        Self(key)
    }
}

/// Tracking information for a peer's interest in a specific contract.
#[derive(Clone, Debug)]
pub struct PeerInterest {
    /// The peer's current state summary. None if interested but has no state yet.
    pub summary: Option<StateSummary<'static>>,

    /// When this interest entry was last refreshed.
    /// Used for TTL-based expiration.
    pub last_refreshed: Instant,

    /// Whether this peer is our upstream in the subscription tree.
    /// Internal routing hint, not exposed to protocol.
    pub is_upstream: bool,
}

impl PeerInterest {
    /// Create a new peer interest entry with the given timestamp.
    pub fn new(summary: Option<StateSummary<'static>>, is_upstream: bool, now: Instant) -> Self {
        Self {
            summary,
            last_refreshed: now,
            is_upstream,
        }
    }

    /// Refresh the TTL timestamp with the given current time.
    pub fn refresh(&mut self, now: Instant) {
        self.last_refreshed = now;
    }

    /// Check if this interest has expired relative to the given current time.
    pub fn is_expired_at(&self, now: Instant) -> bool {
        now.saturating_duration_since(self.last_refreshed) > INTEREST_TTL
    }

    /// Update the peer's summary and refresh TTL.
    pub fn update_summary(&mut self, summary: Option<StateSummary<'static>>, now: Instant) {
        self.summary = summary;
        self.refresh(now);
    }
}

/// Tracks local reasons for interest in a contract.
///
/// A peer can be interested for multiple reasons. We only deregister interest
/// when ALL reasons are removed.
#[derive(Clone, Debug, Default)]
pub struct LocalInterest {
    /// Whether we're seeding this contract (in our local cache).
    pub seeding: bool,

    /// Number of local WebSocket clients subscribed to this contract.
    pub local_client_count: usize,

    /// Number of downstream peers subscribed through us.
    pub downstream_subscriber_count: usize,
}

impl LocalInterest {
    /// Check if we have any reason to be interested in this contract.
    pub fn is_interested(&self) -> bool {
        self.seeding || self.local_client_count > 0 || self.downstream_subscriber_count > 0
    }

    /// Increment the local client count and return whether this is the first client.
    pub fn add_client(&mut self) -> bool {
        let was_first = self.local_client_count == 0;
        self.local_client_count += 1;
        was_first && !self.seeding && self.downstream_subscriber_count == 0
    }

    /// Decrement the local client count and return whether interest was lost.
    pub fn remove_client(&mut self) -> bool {
        self.local_client_count = self.local_client_count.saturating_sub(1);
        !self.is_interested()
    }

    /// Increment the downstream subscriber count and return whether this is the first.
    pub fn add_downstream(&mut self) -> bool {
        let was_first =
            self.downstream_subscriber_count == 0 && self.local_client_count == 0 && !self.seeding;
        self.downstream_subscriber_count += 1;
        was_first
    }

    /// Decrement the downstream subscriber count and return whether interest was lost.
    pub fn remove_downstream(&mut self) -> bool {
        self.downstream_subscriber_count = self.downstream_subscriber_count.saturating_sub(1);
        !self.is_interested()
    }

    /// Set seeding status and return whether interest state changed.
    pub fn set_seeding(&mut self, seeding: bool) -> bool {
        let was_interested = self.is_interested();
        self.seeding = seeding;
        let is_interested = self.is_interested();
        was_interested != is_interested
    }
}

/// Key for delta cache using hashes to avoid allocation on every lookup.
///
/// Instead of storing full summary bytes, we hash them to u64. This makes
/// cache lookups O(1) without any heap allocation. Hash collisions are
/// extremely rare and only cause cache misses (not correctness issues).
#[derive(Clone, Copy, PartialEq, Eq, Hash)]
struct DeltaCacheKey {
    contract: ContractKey,
    peer_summary_hash: u64,
    our_summary_hash: u64,
}

/// Hash bytes to u64 for cache key construction.
/// Uses DefaultHasher for good distribution.
fn hash_bytes(bytes: &[u8]) -> u64 {
    use std::hash::Hasher;
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    hasher.write(bytes);
    hasher.finish()
}

/// Compute a fast hash of a contract key for connection-time discovery.
///
/// Uses FNV-1a for speed. Collisions are acceptable - they just mean we'll
/// check contracts that aren't actually shared.
///
/// This is a standalone function to avoid requiring type parameters when called.
pub fn contract_hash(contract: &ContractKey) -> u32 {
    // FNV-1a parameters
    const FNV_OFFSET: u32 = 2166136261;
    const FNV_PRIME: u32 = 16777619;

    let id_bytes = contract.id().as_bytes();
    let mut hash = FNV_OFFSET;
    for byte in id_bytes {
        hash ^= *byte as u32;
        hash = hash.wrapping_mul(FNV_PRIME);
    }
    hash
}

/// Check if a delta would be efficient compared to sending full state.
///
/// Returns true if summary size is less than 50% of state size.
///
/// This is a standalone function to avoid requiring type parameters when called.
pub fn is_delta_efficient(summary_size: usize, state_size: usize) -> bool {
    if state_size == 0 {
        return false;
    }
    summary_size * 2 < state_size
}

/// Manages interest tracking and delta computation for all contracts.
///
/// This is the central data structure for the delta-based synchronization system.
/// It unifies what was previously split between the subscription tree and proximity cache.
///
/// Generic over `T: TimeSource` to support deterministic simulation testing.
///
/// **Dual-tracking with `HostingManager::downstream_subscribers`:** both must be
/// kept in sync during register/remove operations. This manager drives UPDATE
/// broadcast targeting and upstream peer lookup; `downstream_subscribers` drives
/// unsubscribe-upstream decisions. See the Unsubscribe handler in
/// `operations/subscribe.rs` for the sync point.
pub struct InterestManager<T: TimeSource> {
    /// Track interested peers and their summaries for each contract.
    /// Key: ContractKey, Value: Map of PeerKey -> PeerInterest
    interested_peers: DashMap<ContractKey, HashMap<PeerKey, PeerInterest>>,

    /// Reverse index: which contracts is each peer interested in?
    /// Enables O(1) cleanup when a peer disconnects instead of O(contracts) scan.
    peer_contracts: DashMap<PeerKey, HashSet<ContractKey>>,

    /// Track our local interest reasons for each contract.
    local_interests: DashMap<ContractKey, LocalInterest>,

    /// Cache for memoizing delta computations.
    /// Avoids recomputing the same delta for multiple peers with identical summaries.
    delta_cache: Mutex<LruCache<DeltaCacheKey, StateDelta<'static>>>,

    /// Fast hash index for connection-time discovery.
    /// Maps u32 hash of contract ID -> list of ContractKeys (handles collisions).
    contract_hash_index: DashMap<u32, Vec<ContractKey>>,

    /// Time source for testability (DST-compatible).
    time_source: T,

    // === Delta Sync Metrics ===
    /// Number of times we sent a delta instead of full state.
    delta_sends: AtomicU64,

    /// Number of times we sent full state (no peer summary available or delta failed).
    full_state_sends: AtomicU64,

    /// Total bytes saved by sending deltas instead of full state.
    /// Calculated as: sum of (state_size - delta_size) for each delta send.
    delta_bytes_saved: AtomicU64,

    /// Number of ResyncRequests received (indicates delta application failures at remote peer).
    /// This counter helps detect incorrect summary caching issues (see PR #2763).
    resync_requests_received: AtomicU64,

    /// Throttle timestamps for proactive summary notifications.
    /// After applying a broadcast update, we notify interested peers of our new summary
    /// so they can skip sending us data we already have. This DashMap tracks the last
    /// notification time per contract to avoid flooding (minimum 100ms interval).
    summary_notify_timestamps: DashMap<ContractKey, Instant>,

    /// Deferred interest removals for disconnected peers.
    ///
    /// Instead of immediately wiping a peer's interests on disconnect, we record a
    /// deadline (now + INTEREST_DISCONNECT_GRACE_PERIOD). The sweep task executes
    /// the removal after the deadline passes. If the peer reconnects before the
    /// deadline, the entry is removed from this map and interests are preserved.
    pending_removals: DashMap<PeerKey, Instant>,
}

impl<T: TimeSource> InterestManager<T> {
    /// Create a new interest manager with the given time source.
    pub fn new(time_source: T) -> Self {
        Self {
            interested_peers: DashMap::new(),
            peer_contracts: DashMap::new(),
            local_interests: DashMap::new(),
            delta_cache: Mutex::new(LruCache::new(
                NonZeroUsize::new(DELTA_CACHE_SIZE).expect("DELTA_CACHE_SIZE must be > 0"),
            )),
            contract_hash_index: DashMap::new(),
            time_source,
            delta_sends: AtomicU64::new(0),
            full_state_sends: AtomicU64::new(0),
            delta_bytes_saved: AtomicU64::new(0),
            resync_requests_received: AtomicU64::new(0),
            summary_notify_timestamps: DashMap::new(),
            pending_removals: DashMap::new(),
        }
    }

    /// Record that a delta was sent instead of full state.
    ///
    /// Call this when successfully sending a delta to a peer.
    /// `state_size` is the full state size, `delta_size` is the delta size.
    pub fn record_delta_send(&self, state_size: usize, delta_size: usize) {
        self.delta_sends.fetch_add(1, Ordering::Relaxed);
        let bytes_saved = state_size.saturating_sub(delta_size);
        self.delta_bytes_saved
            .fetch_add(bytes_saved as u64, Ordering::Relaxed);
    }

    /// Record that full state was sent (no delta available).
    ///
    /// Call this when sending full state because no peer summary was available
    /// or delta computation failed.
    pub fn record_full_state_send(&self) {
        self.full_state_sends.fetch_add(1, Ordering::Relaxed);
    }

    /// Record that a ResyncRequest was received from a peer.
    ///
    /// This indicates the peer couldn't apply a delta we sent, likely because
    /// we had incorrect cached summary for them (the bug PR #2763 fixed).
    pub fn record_resync_request_received(&self) {
        self.resync_requests_received
            .fetch_add(1, Ordering::Relaxed);
    }

    /// Get the current time from the configured `TimeSource`.
    ///
    /// Use this to pass DST-compatible timestamps to components that need
    /// the current time (e.g., `BroadcastDedupCache`).
    pub fn now(&self) -> Instant {
        self.time_source.now()
    }

    /// Register a peer's interest in a contract.
    ///
    /// Returns true if this is a new interest (peer wasn't previously tracked).
    pub fn register_peer_interest(
        &self,
        contract: &ContractKey,
        peer: PeerKey,
        summary: Option<StateSummary<'static>>,
        is_upstream: bool,
    ) -> bool {
        let now = self.time_source.now();
        let mut entry = self.interested_peers.entry(*contract).or_default();
        let is_new = !entry.contains_key(&peer);

        entry.insert(peer.clone(), PeerInterest::new(summary, is_upstream, now));

        // Maintain reverse index for O(1) peer disconnect cleanup
        self.peer_contracts
            .entry(peer)
            .or_default()
            .insert(*contract);

        // Also index by hash for fast lookup
        self.index_contract_hash(contract);

        is_new
    }

    /// Remove a peer's interest in a contract.
    ///
    /// Returns true if the peer was actually removed.
    pub fn remove_peer_interest(&self, contract: &ContractKey, peer: &PeerKey) -> bool {
        if let Some(mut entry) = self.interested_peers.get_mut(contract) {
            let removed = entry.remove(peer).is_some();

            if removed {
                // Maintain reverse index
                if let Some(mut peer_entry) = self.peer_contracts.get_mut(peer) {
                    peer_entry.remove(contract);
                    if peer_entry.is_empty() {
                        drop(peer_entry);
                        self.peer_contracts.remove_if(peer, |_, v| v.is_empty());
                    }
                }
            }

            // Clean up empty entries using remove_if to avoid race condition
            // between dropping the entry guard and removing the contract.
            if entry.is_empty() {
                drop(entry);
                self.interested_peers
                    .remove_if(contract, |_, v| v.is_empty());
                // Clean up hash index if no interest remains
                self.cleanup_contract_if_no_interest(contract);
            }

            removed
        } else {
            false
        }
    }

    /// Update a peer's summary for a contract and refresh TTL.
    pub fn update_peer_summary(
        &self,
        contract: &ContractKey,
        peer: &PeerKey,
        summary: Option<StateSummary<'static>>,
    ) {
        let now = self.time_source.now();
        if let Some(mut entry) = self.interested_peers.get_mut(contract) {
            if let Some(interest) = entry.get_mut(peer) {
                interest.update_summary(summary, now);
            }
        }
    }

    /// Refresh the TTL for a peer's interest.
    pub fn refresh_peer_interest(&self, contract: &ContractKey, peer: &PeerKey) {
        let now = self.time_source.now();
        if let Some(mut entry) = self.interested_peers.get_mut(contract) {
            if let Some(interest) = entry.get_mut(peer) {
                interest.refresh(now);
            }
        }
    }

    /// Get all peers interested in a contract.
    pub fn get_interested_peers(&self, contract: &ContractKey) -> Vec<(PeerKey, PeerInterest)> {
        let mut peers: Vec<(PeerKey, PeerInterest)> = self
            .interested_peers
            .get(contract)
            .map(|entry| entry.iter().map(|(k, v)| (k.clone(), v.clone())).collect())
            .unwrap_or_default();
        // Sort by PeerKey bytes for deterministic ordering (critical for simulation tests)
        peers.sort_by(|(a, _), (b, _)| a.0.as_bytes().cmp(b.0.as_bytes()));
        peers
    }

    /// Get a specific peer's interest info for a contract.
    pub fn get_peer_interest(
        &self,
        contract: &ContractKey,
        peer: &PeerKey,
    ) -> Option<PeerInterest> {
        self.interested_peers
            .get(contract)
            .and_then(|entry| entry.get(peer).cloned())
    }

    /// Get all contracts a peer has interest entries for.
    ///
    /// Uses the `peer_contracts` reverse index for O(1) lookup.
    /// Used by the heartbeat handler to implement full-replace semantics.
    pub fn get_contracts_for_peer(&self, peer: &PeerKey) -> HashSet<ContractKey> {
        self.peer_contracts
            .get(peer)
            .map(|entry| entry.value().clone())
            .unwrap_or_default()
    }

    /// Get the peer's cached summary for a contract.
    pub fn get_peer_summary(
        &self,
        contract: &ContractKey,
        peer: &PeerKey,
    ) -> Option<StateSummary<'static>> {
        self.interested_peers
            .get(contract)
            .and_then(|entry| entry.get(peer).and_then(|i| i.summary.clone()))
    }

    /// Check if enough time has elapsed to send a proactive summary notification
    /// for this contract. Returns `true` if at least 100ms has passed since the last
    /// notification (or if no notification was ever sent). Updates the timestamp on success.
    ///
    /// This prevents flooding peers with summary notifications when multiple broadcasts
    /// are applied in rapid succession.
    pub fn should_send_summary_notification(&self, contract: &ContractKey) -> bool {
        let now = self.time_source.now();
        let min_interval = Duration::from_millis(100);

        let mut entry = self.summary_notify_timestamps.entry(*contract).or_insert(
            // Use a timestamp far in the past so the first check always succeeds
            now - min_interval - Duration::from_millis(1),
        );

        if now.duration_since(*entry.value()) >= min_interval {
            *entry.value_mut() = now;
            true
        } else {
            false
        }
    }

    /// Remove all interests for a peer (called on peer disconnect).
    ///
    /// Uses the reverse index for O(1) lookup instead of O(contracts) scan.
    /// Returns the number of contracts from which the peer was removed.
    pub fn remove_all_peer_interests(&self, peer: &PeerKey) -> usize {
        // Use the reverse index to find all contracts this peer is interested in
        let contracts: Vec<ContractKey> = self
            .peer_contracts
            .remove(peer)
            .map(|(_, set)| set.into_iter().collect())
            .unwrap_or_default();

        let removed_count = contracts.len();

        // Remove the peer from each contract's interest list
        for contract in &contracts {
            if let Some(mut entry) = self.interested_peers.get_mut(contract) {
                entry.remove(peer);

                // Clean up empty entries
                if entry.is_empty() {
                    drop(entry);
                    self.interested_peers
                        .remove_if(contract, |_, v| v.is_empty());
                    self.cleanup_contract_if_no_interest(contract);
                }
            }
        }

        if removed_count > 0 {
            tracing::debug!(removed_count, "Removed peer interests on disconnect");
        }

        removed_count
    }

    /// Schedule deferred removal of a peer's interests after a grace period.
    ///
    /// Instead of immediately wiping interests on disconnect, this records a deadline.
    /// The sweep task will execute the actual removal after the grace period expires.
    /// If the peer reconnects before the deadline (via `cancel_deferred_removal`),
    /// interests are preserved — avoiding permanent interest loss during connection blips.
    pub fn schedule_deferred_removal(&self, peer: &PeerKey) {
        let deadline = self.time_source.now() + INTEREST_DISCONNECT_GRACE_PERIOD;
        self.pending_removals.insert(peer.clone(), deadline);
        tracing::debug!(
            peer = %peer.0,
            grace_secs = INTEREST_DISCONNECT_GRACE_PERIOD.as_secs(),
            "Scheduled deferred interest removal"
        );
    }

    /// Cancel a pending deferred removal for a reconnecting peer.
    ///
    /// Returns true if a pending removal was cancelled (peer reconnected in time).
    pub fn cancel_deferred_removal(&self, peer: &PeerKey) -> bool {
        let cancelled = self.pending_removals.remove(peer).is_some();
        if cancelled {
            tracing::debug!(
                peer = %peer.0,
                "Cancelled deferred interest removal — peer reconnected"
            );
        }
        cancelled
    }

    /// Execute any deferred removals whose grace period has expired.
    ///
    /// Called by the sweep task alongside expired-interest cleanup.
    /// Returns the number of peers whose interests were removed.
    pub fn execute_pending_removals(&self) -> usize {
        let now = self.time_source.now();
        let expired_peers: Vec<PeerKey> = self
            .pending_removals
            .iter()
            .filter(|entry| now >= *entry.value())
            .map(|entry| entry.key().clone())
            .collect();

        let mut executed = 0;
        for peer in &expired_peers {
            // Atomically remove from pending_removals. If `cancel_deferred_removal`
            // already removed it (peer reconnected between collect and here), skip
            // the interest removal to avoid a TOCTOU race.
            if self.pending_removals.remove(peer).is_some() {
                let removed = self.remove_all_peer_interests(peer);
                tracing::info!(
                    peer = %peer.0,
                    removed_interests = removed,
                    "Executed deferred interest removal — peer did not reconnect"
                );
                executed += 1;
            }
        }
        executed
    }

    /// Register local interest in a contract (for tracking our reasons).
    pub fn register_local_interest(&self, contract: &ContractKey) -> &Self {
        self.local_interests.entry(*contract).or_default();
        self.index_contract_hash(contract);
        self
    }

    /// Register that we're seeding a contract locally.
    /// Returns true if this caused us to become interested (wasn't interested before).
    pub fn register_local_seeding(&self, contract: &ContractKey) -> bool {
        let mut entry = self.local_interests.entry(*contract).or_default();
        let was_interested = entry.is_interested();
        entry.seeding = true;
        self.index_contract_hash(contract);
        !was_interested
    }

    /// Unregister that we're seeding a contract locally.
    /// Returns true if this caused us to lose interest (no other reasons remain).
    pub fn unregister_local_seeding(&self, contract: &ContractKey) -> bool {
        if let Some(mut entry) = self.local_interests.get_mut(contract) {
            entry.seeding = false;
            let lost_interest = !entry.is_interested();
            if lost_interest {
                drop(entry);
                self.local_interests.remove(contract);
                // Clean up hash index if no interest remains
                self.cleanup_contract_if_no_interest(contract);
            }
            lost_interest
        } else {
            false
        }
    }

    /// Add a local client subscription.
    /// Returns true if this caused us to become interested.
    pub fn add_local_client(&self, contract: &ContractKey) -> bool {
        let mut entry = self.local_interests.entry(*contract).or_default();
        let became_interested = entry.add_client();
        self.index_contract_hash(contract);
        became_interested
    }

    /// Remove a local client subscription.
    /// Returns true if this caused us to lose interest.
    pub fn remove_local_client(&self, contract: &ContractKey) -> bool {
        if let Some(mut entry) = self.local_interests.get_mut(contract) {
            let lost_interest = entry.remove_client();
            if lost_interest {
                drop(entry);
                self.local_interests.remove(contract);
                // Clean up hash index if no interest remains
                self.cleanup_contract_if_no_interest(contract);
            }
            lost_interest
        } else {
            false
        }
    }

    /// Add a downstream subscriber.
    /// Returns true if this caused us to become interested.
    pub fn add_downstream_subscriber(&self, contract: &ContractKey) -> bool {
        let mut entry = self.local_interests.entry(*contract).or_default();
        let became_interested = entry.add_downstream();
        self.index_contract_hash(contract);
        became_interested
    }

    /// Remove a downstream subscriber.
    /// Returns true if this caused us to lose interest.
    pub fn remove_downstream_subscriber(&self, contract: &ContractKey) -> bool {
        if let Some(mut entry) = self.local_interests.get_mut(contract) {
            let lost_interest = entry.remove_downstream();
            if lost_interest {
                drop(entry);
                self.local_interests.remove(contract);
                // Clean up hash index if no interest remains
                self.cleanup_contract_if_no_interest(contract);
            }
            lost_interest
        } else {
            false
        }
    }

    /// Get or create local interest entry, returning mutable reference.
    pub fn with_local_interest<F, R>(&self, contract: &ContractKey, f: F) -> R
    where
        F: FnOnce(&mut LocalInterest) -> R,
    {
        let mut entry = self.local_interests.entry(*contract).or_default();
        f(entry.value_mut())
    }

    /// Check if we have any local interest in a contract.
    pub fn has_local_interest(&self, contract: &ContractKey) -> bool {
        self.local_interests
            .get(contract)
            .map(|entry| entry.is_interested())
            .unwrap_or(false)
    }

    /// Remove local interest entry if no longer interested.
    pub fn cleanup_local_interest(&self, contract: &ContractKey) {
        if let Some(entry) = self.local_interests.get(contract) {
            if !entry.is_interested() {
                drop(entry);
                self.local_interests.remove(contract);
            }
        }
    }

    /// Sweep expired peer interests.
    ///
    /// Returns list of (contract, peer) pairs that were removed.
    pub fn sweep_expired_interests(&self) -> Vec<(ContractKey, PeerKey)> {
        let now = self.time_source.now();
        let mut expired = Vec::new();

        // Collect and sort contracts for deterministic iteration order
        let mut contracts: Vec<_> = self
            .interested_peers
            .iter()
            .map(|entry| (*entry.key(), entry.value().clone()))
            .collect();
        contracts.sort_by(|(a, _), (b, _)| a.id().as_bytes().cmp(b.id().as_bytes()));

        for (contract, peers_map) in contracts {
            // Collect and sort peers for deterministic iteration order
            let mut peers_to_remove: Vec<PeerKey> = peers_map
                .iter()
                .filter(|(_, interest)| interest.is_expired_at(now))
                .map(|(peer, _)| peer.clone())
                .collect();
            peers_to_remove.sort_by(|a, b| a.0.as_bytes().cmp(b.0.as_bytes()));

            for peer in peers_to_remove {
                expired.push((contract, peer));
            }
        }

        // Remove expired entries
        for (contract, peer) in &expired {
            self.remove_peer_interest(contract, peer);
        }

        if !expired.is_empty() {
            tracing::debug!(
                expired_count = expired.len(),
                "Interest sweep: removed expired entries"
            );
        }

        expired
    }

    /// Start the background sweep task for expired peer interests.
    ///
    /// This spawns a task that runs periodically to clean up expired entries.
    /// Should be called once after the interest manager is set up.
    ///
    /// Note: The sweep interval uses real time (tokio::time) for scheduling,
    /// but expiration checking uses the TimeSource. In tests, manually call
    /// `sweep_expired_interests()` after advancing mock time.
    pub fn start_sweep_task(manager: std::sync::Arc<Self>)
    where
        T: Send + Sync + 'static,
    {
        GlobalExecutor::spawn(Self::sweep_task(manager));
    }

    /// Background task to sweep expired peer interests.
    async fn sweep_task(manager: std::sync::Arc<Self>)
    where
        T: Send + Sync + 'static,
    {
        // Add random initial delay to prevent synchronized sweeps across peers
        let initial_delay = Duration::from_secs(GlobalRng::random_range(10u64..=30u64));
        tokio::time::sleep(initial_delay).await;

        let mut interval = tokio::time::interval(INTEREST_SWEEP_INTERVAL);
        interval.tick().await; // Skip first immediate tick

        loop {
            interval.tick().await;

            // Execute any deferred removals whose grace period has expired
            manager.execute_pending_removals();

            // Capture stats before sweep for the health snapshot
            let stats = manager.stats();
            let expired = manager.sweep_expired_interests();

            if !expired.is_empty() {
                tracing::info!(
                    expired_count = expired.len(),
                    "Interest sweep: cleaned up expired peer interests"
                );

                // Emit per-entry expiration telemetry
                for (contract, peer) in &expired {
                    crate::tracing::telemetry::send_standalone_event(
                        "interest_expired",
                        serde_json::json!({
                            "contract": contract.to_string(),
                            "peer": peer.0.to_string(),
                        }),
                    );
                }
            }

            // Emit periodic health snapshot
            crate::tracing::telemetry::send_standalone_event(
                "subscription_health_snapshot",
                serde_json::json!({
                    "contracts_with_interests": stats.total_contracts,
                    "total_interest_entries": stats.total_peer_interests,
                    "expired_this_sweep": expired.len(),
                }),
            );
        }
    }

    /// Index a contract by its hash for fast lookup.
    fn index_contract_hash(&self, contract: &ContractKey) {
        let hash = contract_hash(contract);
        let mut entry = self.contract_hash_index.entry(hash).or_default();
        // Only add if not already present (dedup without Ord)
        if !entry.contains(contract) {
            entry.push(*contract);
        }
    }

    /// Remove a contract from the hash index.
    fn unindex_contract_hash(&self, contract: &ContractKey) {
        let hash = contract_hash(contract);
        if let Some(mut entry) = self.contract_hash_index.get_mut(&hash) {
            entry.retain(|c| c != contract);
            if entry.is_empty() {
                drop(entry);
                self.contract_hash_index.remove(&hash);
            }
        }
    }

    /// Clean up hash index for a contract if there's no remaining interest.
    /// Called after removing peer or local interest.
    fn cleanup_contract_if_no_interest(&self, contract: &ContractKey) {
        let has_peer_interest = self.interested_peers.contains_key(contract);
        let has_local_interest = self.has_local_interest(contract);

        if !has_peer_interest && !has_local_interest {
            self.unindex_contract_hash(contract);
            // Clean up summary notification timestamp when no interest remains
            self.summary_notify_timestamps.remove(contract);
        }
    }

    /// Look up contracts by hash. Returns all contracts that hash to this value
    /// (handles collisions by returning multiple candidates).
    pub fn lookup_by_hash(&self, hash: u32) -> Vec<ContractKey> {
        self.contract_hash_index
            .get(&hash)
            .as_deref()
            .cloned()
            .unwrap_or_default()
    }

    /// Get all contract hashes we're interested in.
    ///
    /// Uses the existing hash index for O(1) access - no rehashing needed.
    pub fn get_all_interest_hashes(&self) -> Vec<u32> {
        let mut hashes: Vec<u32> = self.contract_hash_index.iter().map(|e| *e.key()).collect();
        // Sort for deterministic ordering (critical for simulation tests)
        hashes.sort_unstable();
        hashes
    }

    /// Get contracts we're interested in that match the given hashes.
    pub fn get_matching_contracts(&self, hashes: &[u32]) -> Vec<ContractKey> {
        let hash_set: std::collections::HashSet<u32> = hashes.iter().copied().collect();

        let mut contracts: Vec<ContractKey> = self
            .contract_hash_index
            .iter()
            .filter(|entry| hash_set.contains(entry.key()))
            .flat_map(|entry| entry.value().clone())
            .collect();
        // Sort by contract ID bytes for deterministic ordering (critical for simulation tests)
        contracts.sort_by(|a, b| a.id().as_bytes().cmp(b.id().as_bytes()));
        contracts
    }

    /// Cache a computed delta for reuse.
    pub fn cache_delta(
        &self,
        contract: &ContractKey,
        peer_summary: &[u8],
        our_summary: &[u8],
        delta: StateDelta<'static>,
    ) {
        let key = DeltaCacheKey {
            contract: *contract,
            peer_summary_hash: hash_bytes(peer_summary),
            our_summary_hash: hash_bytes(our_summary),
        };
        self.delta_cache.lock().put(key, delta);
    }

    /// Look up a cached delta.
    pub fn get_cached_delta(
        &self,
        contract: &ContractKey,
        peer_summary: &[u8],
        our_summary: &[u8],
    ) -> Option<StateDelta<'static>> {
        let key = DeltaCacheKey {
            contract: *contract,
            peer_summary_hash: hash_bytes(peer_summary),
            our_summary_hash: hash_bytes(our_summary),
        };
        self.delta_cache.lock().get(&key).cloned()
    }

    /// Get the current state summary for a contract.
    ///
    /// Uses the contract handler to compute the summary via the contract's
    /// `summarize_state` method.
    pub async fn get_contract_summary(
        &self,
        op_manager: &crate::node::OpManager,
        key: &ContractKey,
    ) -> Option<StateSummary<'static>> {
        use crate::contract::ContractHandlerEvent;

        match op_manager
            .notify_contract_handler_with_timeout(
                ContractHandlerEvent::GetSummaryQuery { key: *key },
                BROADCAST_CH_TIMEOUT,
            )
            .await
        {
            Ok(ContractHandlerEvent::GetSummaryResponse { summary: Ok(s), .. }) => Some(s),
            Ok(ContractHandlerEvent::GetSummaryResponse {
                summary: Err(e), ..
            }) => {
                tracing::debug!(
                    contract = %key,
                    error = %e,
                    "Failed to get contract summary"
                );
                None
            }
            Ok(other) => {
                tracing::warn!(
                    contract = %key,
                    response = ?other,
                    "Unexpected response to GetSummaryQuery"
                );
                None
            }
            Err(e) => {
                tracing::debug!(
                    contract = %key,
                    error = %e,
                    "Error getting contract summary"
                );
                None
            }
        }
    }

    /// Compute a state delta for a peer given their cached summary.
    ///
    /// Uses the contract handler to compute the delta via the contract's
    /// `get_state_delta` method. Results are cached to avoid recomputation
    /// for peers with the same summary.
    ///
    /// Returns `Ok(None)` when the contract returns an empty delta (zero bytes),
    /// meaning the peer's state is logically equivalent to ours despite differing
    /// summary bytes (e.g., due to non-deterministic serialization order).
    ///
    /// # Arguments
    /// * `our_summary` - Our current state summary (used for cache key)
    /// * `our_state_size` - Size of our current state (for efficiency check)
    pub async fn compute_delta(
        &self,
        op_manager: &crate::node::OpManager,
        key: &ContractKey,
        their_summary: &StateSummary<'static>,
        our_summary: &StateSummary<'static>,
        our_state_size: usize,
    ) -> Result<Option<StateDelta<'static>>, String> {
        use crate::contract::ContractHandlerEvent;

        // Use slices directly - cache methods hash internally, no allocation needed
        let their_summary_bytes = their_summary.as_ref();
        let our_summary_bytes = our_summary.as_ref();

        // Check cache first (keyed by hash of contract + summaries)
        if let Some(cached) = self.get_cached_delta(key, their_summary_bytes, our_summary_bytes) {
            if cached.as_ref().is_empty() {
                tracing::trace!(contract = %key, "Cached empty delta (no change)");
                return Ok(None);
            }
            tracing::trace!(contract = %key, "Using cached delta");
            return Ok(Some(cached));
        }

        // Check if delta would be efficient
        // (summary > 50% of state size means delta probably won't help)
        if !is_delta_efficient(their_summary_bytes.len(), our_state_size) {
            return Err("Delta not efficient for this contract".to_string());
        }

        // Compute delta via contract handler (short timeout for broadcast path)
        match op_manager
            .notify_contract_handler_with_timeout(
                ContractHandlerEvent::GetDeltaQuery {
                    key: *key,
                    their_summary: their_summary.clone(),
                },
                BROADCAST_CH_TIMEOUT,
            )
            .await
        {
            Ok(ContractHandlerEvent::GetDeltaResponse { delta: Ok(d), .. }) => {
                if d.as_ref().is_empty() {
                    // Empty delta means no change needed — cache it so we don't
                    // re-invoke the contract on subsequent broadcast cycles
                    self.cache_delta(key, their_summary_bytes, our_summary_bytes, d);
                    tracing::trace!(
                        contract = %key,
                        "Contract returned empty delta (no change)"
                    );
                    Ok(None)
                } else {
                    // Cache the result (includes contract key to prevent cross-contract pollution)
                    self.cache_delta(key, their_summary_bytes, our_summary_bytes, d.clone());
                    Ok(Some(d))
                }
            }
            Ok(ContractHandlerEvent::GetDeltaResponse { delta: Err(e), .. }) => {
                Err(format!("Delta computation failed: {}", e))
            }
            Ok(other) => Err(format!("Unexpected response to GetDeltaQuery: {:?}", other)),
            Err(e) => Err(format!("Error computing delta: {}", e)),
        }
    }

    /// Get statistics about the interest manager state.
    pub fn stats(&self) -> InterestManagerStats {
        let total_contracts = self.interested_peers.len();
        let total_peer_interests: usize = self
            .interested_peers
            .iter()
            .map(|entry| entry.value().len())
            .sum();
        let local_interests = self.local_interests.len();
        let hash_index_size = self.contract_hash_index.len();

        InterestManagerStats {
            total_contracts,
            total_peer_interests,
            local_interests,
            hash_index_size,
            delta_sends: self.delta_sends.load(Ordering::Relaxed),
            full_state_sends: self.full_state_sends.load(Ordering::Relaxed),
            delta_bytes_saved: self.delta_bytes_saved.load(Ordering::Relaxed),
            resync_requests_received: self.resync_requests_received.load(Ordering::Relaxed),
        }
    }
}

/// Statistics about the interest manager state.
#[derive(Debug, Clone)]
pub struct InterestManagerStats {
    /// Number of contracts with at least one interested peer.
    pub total_contracts: usize,
    /// Total number of peer interest entries across all contracts.
    pub total_peer_interests: usize,
    /// Number of contracts with local interest.
    pub local_interests: usize,
    /// Size of the contract hash index.
    pub hash_index_size: usize,
    /// Number of times a delta was sent instead of full state.
    pub delta_sends: u64,
    /// Number of times full state was sent.
    pub full_state_sends: u64,
    /// Total bytes saved by sending deltas.
    pub delta_bytes_saved: u64,
    /// Number of ResyncRequests received (indicates delta failures at remote peers).
    /// With correct summary caching (PR #2763), this should be zero in normal operation.
    pub resync_requests_received: u64,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::util::time_source::SharedMockTimeSource;
    use freenet_stdlib::prelude::{CodeHash, ContractInstanceId};

    /// Type alias for tests using mock time
    type TestInterestManager = InterestManager<SharedMockTimeSource>;

    fn make_contract_key(seed: u8) -> ContractKey {
        ContractKey::from_id_and_code(
            ContractInstanceId::new([seed; 32]),
            CodeHash::new([seed.wrapping_add(1); 32]),
        )
    }

    fn make_peer_key(_seed: u8) -> PeerKey {
        use crate::transport::TransportKeypair;
        let keypair = TransportKeypair::new();
        PeerKey(keypair.public().clone())
    }

    fn make_manager() -> (TestInterestManager, SharedMockTimeSource) {
        let time_source = SharedMockTimeSource::new();
        let manager = InterestManager::new(time_source.clone());
        (manager, time_source)
    }

    #[test]
    fn test_register_and_remove_peer_interest() {
        let (manager, _time) = make_manager();
        let contract = make_contract_key(1);
        let peer = make_peer_key(1);

        // Register interest
        assert!(manager.register_peer_interest(&contract, peer.clone(), None, false));

        // Duplicate registration returns false
        assert!(!manager.register_peer_interest(&contract, peer.clone(), None, false));

        // Verify interest exists
        assert!(manager.get_peer_interest(&contract, &peer).is_some());

        // Remove interest
        assert!(manager.remove_peer_interest(&contract, &peer));

        // Verify removed
        assert!(manager.get_peer_interest(&contract, &peer).is_none());

        // Remove again returns false
        assert!(!manager.remove_peer_interest(&contract, &peer));
    }

    #[test]
    fn test_update_peer_summary() {
        let (manager, _time) = make_manager();
        let contract = make_contract_key(1);
        let peer = make_peer_key(1);

        // Register without summary
        manager.register_peer_interest(&contract, peer.clone(), None, false);
        assert!(manager.get_peer_summary(&contract, &peer).is_none());

        // Update with summary
        let summary = StateSummary::from(vec![1, 2, 3]);
        manager.update_peer_summary(&contract, &peer, Some(summary.clone()));

        let retrieved = manager.get_peer_summary(&contract, &peer);
        assert!(retrieved.is_some());
        assert_eq!(retrieved.unwrap().as_ref(), summary.as_ref());
    }

    #[test]
    fn test_local_interest_tracking() {
        let (manager, _time) = make_manager();
        let contract = make_contract_key(1);

        // Initially no interest
        assert!(!manager.has_local_interest(&contract));

        // Add seeding interest
        manager.with_local_interest(&contract, |interest| {
            interest.set_seeding(true);
        });
        assert!(manager.has_local_interest(&contract));

        // Add client interest
        manager.with_local_interest(&contract, |interest| {
            interest.add_client();
        });
        assert!(manager.has_local_interest(&contract));

        // Remove seeding - still interested due to client
        manager.with_local_interest(&contract, |interest| {
            interest.set_seeding(false);
        });
        assert!(manager.has_local_interest(&contract));

        // Remove client - no longer interested
        manager.with_local_interest(&contract, |interest| {
            interest.remove_client();
        });
        assert!(!manager.has_local_interest(&contract));
    }

    #[test]
    fn test_local_interest_transitions() {
        let mut interest = LocalInterest::default();

        // Initially not interested
        assert!(!interest.is_interested());

        // First client triggers interest
        assert!(interest.add_client()); // Returns true - gained interest
        assert!(interest.is_interested());

        // Second client doesn't change interest state
        assert!(!interest.add_client()); // Returns false - already interested
        assert!(interest.is_interested());

        // Remove one client - still interested
        assert!(!interest.remove_client()); // Returns false - still interested
        assert!(interest.is_interested());

        // Remove last client - interest lost
        assert!(interest.remove_client()); // Returns true - lost interest
        assert!(!interest.is_interested());
    }

    #[test]
    fn test_contract_hash_consistency() {
        let contract = make_contract_key(42);

        // Same contract should produce same hash
        let hash1 = contract_hash(&contract);
        let hash2 = contract_hash(&contract);
        assert_eq!(hash1, hash2);

        // Different contracts should (usually) produce different hashes
        let other_contract = make_contract_key(43);
        let other_hash = contract_hash(&other_contract);
        // Note: hash collision is theoretically possible but extremely unlikely
        // for these test values
        assert_ne!(hash1, other_hash);
    }

    #[test]
    fn test_contract_hash_index() {
        let (manager, _time) = make_manager();
        let contract = make_contract_key(1);
        let peer = make_peer_key(1);

        // Register interest (should also index the hash)
        manager.register_peer_interest(&contract, peer, None, false);

        // Look up by hash
        let hash = contract_hash(&contract);
        let retrieved = manager.lookup_by_hash(hash);
        assert_eq!(retrieved, vec![contract]);

        // Unknown hash returns empty vec
        assert!(manager.lookup_by_hash(12345).is_empty());
    }

    #[test]
    fn test_get_all_interest_hashes() {
        let (manager, _time) = make_manager();
        let contract1 = make_contract_key(1);
        let contract2 = make_contract_key(2);
        let peer = make_peer_key(1);

        // Register interests (use methods that properly index)
        manager.register_peer_interest(&contract1, peer.clone(), None, false);
        manager.register_local_seeding(&contract2);

        let hashes = manager.get_all_interest_hashes();
        assert_eq!(hashes.len(), 2);
        assert!(hashes.contains(&contract_hash(&contract1)));
        assert!(hashes.contains(&contract_hash(&contract2)));
    }

    #[test]
    fn test_delta_efficiency_check() {
        // Small summary relative to state - efficient
        assert!(is_delta_efficient(100, 1000));

        // Summary is 50% of state - not efficient
        assert!(!is_delta_efficient(500, 1000));

        // Summary larger than state - not efficient
        assert!(!is_delta_efficient(1500, 1000));

        // Zero state size - not efficient
        assert!(!is_delta_efficient(100, 0));
    }

    #[test]
    fn test_delta_cache() {
        let (manager, _time) = make_manager();
        let contract1 = make_contract_key(1);
        let contract2 = make_contract_key(2);

        let peer_summary = vec![1, 2, 3];
        let our_summary = vec![4, 5, 6];
        let delta = StateDelta::from(vec![7, 8, 9]);

        // Cache miss
        assert!(manager
            .get_cached_delta(&contract1, &peer_summary, &our_summary)
            .is_none());

        // Cache the delta for contract1
        manager.cache_delta(&contract1, &peer_summary, &our_summary, delta.clone());

        // Cache hit for contract1
        let cached = manager.get_cached_delta(&contract1, &peer_summary, &our_summary);
        assert!(cached.is_some());
        assert_eq!(cached.unwrap().as_ref(), delta.as_ref());

        // Cache miss for contract2 with same summaries (contract key isolates cache entries)
        assert!(manager
            .get_cached_delta(&contract2, &peer_summary, &our_summary)
            .is_none());
    }

    #[test]
    fn test_sweep_expired_interests() {
        let (manager, time) = make_manager();
        let contract = make_contract_key(1);
        let peer = make_peer_key(1);

        // Register interest
        manager.register_peer_interest(&contract, peer.clone(), None, false);

        // Advance time past TTL
        time.advance_time(INTEREST_TTL + Duration::from_secs(1));

        // Sweep should remove expired entry
        let expired = manager.sweep_expired_interests();
        assert_eq!(expired.len(), 1);
        assert_eq!(expired[0].0, contract);

        // Verify removed
        assert!(manager.get_peer_interest(&contract, &peer).is_none());
    }

    #[test]
    fn test_refresh_prevents_expiration() {
        let (manager, time) = make_manager();
        let contract = make_contract_key(1);
        let peer = make_peer_key(1);

        // Register interest
        manager.register_peer_interest(&contract, peer.clone(), None, false);

        // Advance time to nearly expired
        time.advance_time(INTEREST_TTL - Duration::from_secs(10));

        // Refresh the interest
        manager.refresh_peer_interest(&contract, &peer);

        // Advance time a bit more (past original registration but not past refresh)
        time.advance_time(Duration::from_secs(20));

        // Sweep should not remove it (refresh reset the TTL)
        let expired = manager.sweep_expired_interests();
        assert!(expired.is_empty());
        assert!(manager.get_peer_interest(&contract, &peer).is_some());
    }

    #[test]
    fn test_stats() {
        let (manager, _time) = make_manager();
        let contract1 = make_contract_key(1);
        let contract2 = make_contract_key(2);
        let peer1 = make_peer_key(1);
        let peer2 = make_peer_key(2);

        // Add various interests
        manager.register_peer_interest(&contract1, peer1.clone(), None, false);
        manager.register_peer_interest(&contract1, peer2.clone(), None, false);
        manager.register_peer_interest(&contract2, peer1, None, true);
        manager.with_local_interest(&contract1, |i| i.set_seeding(true));

        let stats = manager.stats();
        assert_eq!(stats.total_contracts, 2);
        assert_eq!(stats.total_peer_interests, 3);
        assert_eq!(stats.local_interests, 1);
        assert!(stats.hash_index_size >= 2);
    }

    #[test]
    fn test_delta_sync_metrics() {
        let (manager, _time) = make_manager();

        // Initially all metrics should be zero
        let stats = manager.stats();
        assert_eq!(stats.delta_sends, 0);
        assert_eq!(stats.full_state_sends, 0);
        assert_eq!(stats.delta_bytes_saved, 0);

        // Record some delta sends
        // state_size=1000, delta_size=100 -> 900 bytes saved
        manager.record_delta_send(1000, 100);
        manager.record_delta_send(2000, 200);

        // Record a full state send
        manager.record_full_state_send();
        manager.record_full_state_send();

        let stats = manager.stats();
        assert_eq!(stats.delta_sends, 2);
        assert_eq!(stats.full_state_sends, 2);
        // 900 + 1800 = 2700 bytes saved
        assert_eq!(stats.delta_bytes_saved, 2700);
    }

    #[test]
    fn test_get_matching_contracts() {
        let (manager, _time) = make_manager();
        let contract1 = make_contract_key(1);
        let contract2 = make_contract_key(2);
        let contract3 = make_contract_key(3);

        // Register local interest in contracts 1 and 2 (using set_seeding which indexes)
        manager.register_local_seeding(&contract1);
        manager.register_local_seeding(&contract2);

        // Get hashes
        let hash1 = contract_hash(&contract1);
        let hash2 = contract_hash(&contract2);
        let hash3 = contract_hash(&contract3);

        // Matching with partial overlap
        let matching = manager.get_matching_contracts(&[hash1, hash3]);
        assert_eq!(matching.len(), 1);
        assert!(matching.contains(&contract1));

        // Matching with full overlap
        let matching = manager.get_matching_contracts(&[hash1, hash2]);
        assert_eq!(matching.len(), 2);
        assert!(matching.contains(&contract1));
        assert!(matching.contains(&contract2));

        // No overlap
        let matching = manager.get_matching_contracts(&[hash3, 99999]);
        assert!(matching.is_empty());

        // Empty input
        let matching = manager.get_matching_contracts(&[]);
        assert!(matching.is_empty());
    }

    #[test]
    fn test_interest_sync_flow_simulation() {
        // Simulate the Interests -> Summaries flow that handle_interest_sync_message uses
        let (manager_a, _time_a) = make_manager();
        let (manager_b, _time_b) = make_manager();

        let contract1 = make_contract_key(1);
        let contract2 = make_contract_key(2);
        let contract3 = make_contract_key(3);

        let peer_a = make_peer_key(1);
        let peer_b = make_peer_key(2);

        let summary1 = StateSummary::from(vec![1, 1, 1]);
        let summary2 = StateSummary::from(vec![2, 2, 2]);

        // Setup: A is interested in contracts 1, 2 (using set_seeding which indexes)
        manager_a.register_local_seeding(&contract1);
        manager_a.register_local_seeding(&contract2);

        // Setup: B is interested in contracts 2, 3 (using set_seeding which indexes)
        manager_b.register_local_seeding(&contract2);
        manager_b.register_local_seeding(&contract3);

        // Step 1: A sends its interest hashes to B
        let a_hashes = manager_a.get_all_interest_hashes();
        assert_eq!(a_hashes.len(), 2);

        // Step 2: B finds matching contracts and registers A's interest
        let matching = manager_b.get_matching_contracts(&a_hashes);
        // Only contract2 is in both A and B's interests
        assert_eq!(matching.len(), 1);
        assert!(matching.contains(&contract2));

        // B registers A's interest in the matching contract
        for contract in &matching {
            manager_b.register_peer_interest(contract, peer_a.clone(), None, false);
        }

        // Verify B now tracks A's interest in contract2
        assert!(manager_b
            .get_interested_peers(&contract2)
            .iter()
            .any(|(pk, _)| pk == &peer_a));

        // Step 3: B sends summaries back for matching contracts
        // A receives and updates B's summary
        manager_a.register_peer_interest(&contract2, peer_b.clone(), Some(summary2.clone()), false);

        // Verify A has B's summary
        let cached_summary = manager_a.get_peer_summary(&contract2, &peer_b);
        assert!(cached_summary.is_some());
        assert_eq!(cached_summary.unwrap().as_ref(), summary2.as_ref());

        // Step 4: A sends its summary back
        manager_b.update_peer_summary(&contract2, &peer_a, Some(summary1.clone()));

        // Verify B has A's summary
        let cached_summary = manager_b.get_peer_summary(&contract2, &peer_a);
        assert!(cached_summary.is_some());
        assert_eq!(cached_summary.unwrap().as_ref(), summary1.as_ref());
    }

    #[test]
    fn test_change_interests_flow_simulation() {
        // Simulate the ChangeInterests flow
        let (manager, _time) = make_manager();
        let contract1 = make_contract_key(1);
        let contract2 = make_contract_key(2);
        let peer = make_peer_key(1);

        let hash1 = contract_hash(&contract1);
        let hash2 = contract_hash(&contract2);

        // Setup: local interest in contract1 (using set_seeding which indexes)
        manager.register_local_seeding(&contract1);

        // Peer declares interest in contract1 and contract2
        let added_hashes = vec![hash1, hash2];

        // For each added hash, lookup contracts and register if we have local interest
        for hash in &added_hashes {
            for contract in manager.lookup_by_hash(*hash) {
                if manager.has_local_interest(&contract) {
                    manager.register_peer_interest(&contract, peer.clone(), None, false);
                }
            }
        }

        // Only contract1 should have peer interest (we have local interest in it)
        assert!(manager
            .get_interested_peers(&contract1)
            .iter()
            .any(|(pk, _)| pk == &peer));
        // contract2 wasn't registered because we don't have local interest
        assert!(!manager
            .get_interested_peers(&contract2)
            .iter()
            .any(|(pk, _)| pk == &peer));

        // Later: peer removes interest in contract1
        let removed_hashes = vec![hash1];
        for hash in &removed_hashes {
            for contract in manager.lookup_by_hash(*hash) {
                manager.remove_peer_interest(&contract, &peer);
            }
        }

        // Verify peer is no longer interested
        assert!(!manager
            .get_interested_peers(&contract1)
            .iter()
            .any(|(pk, _)| pk == &peer));
    }

    #[test]
    fn test_resync_clears_summary() {
        // Simulate ResyncRequest clearing a peer's summary
        let (manager, _time) = make_manager();
        let contract = make_contract_key(1);
        let peer = make_peer_key(1);
        let summary = StateSummary::from(vec![1, 2, 3]);

        // Setup: register peer with summary
        manager.register_peer_interest(&contract, peer.clone(), Some(summary.clone()), false);

        // Verify summary is cached
        let cached = manager.get_peer_summary(&contract, &peer);
        assert!(cached.is_some());

        // Simulate ResyncRequest: clear the summary
        manager.update_peer_summary(&contract, &peer, None);

        // Verify summary is now None
        let cached = manager.get_peer_summary(&contract, &peer);
        assert!(cached.is_none());

        // Peer should still be interested (just no summary)
        assert!(manager
            .get_interested_peers(&contract)
            .iter()
            .any(|(pk, _)| pk == &peer));
    }

    #[test]
    fn test_resync_full_flow() {
        // Simulate the complete ResyncRequest -> ResyncResponse flow
        // Peer A has corrupted state and requests resync from Peer B
        let (manager_a, _time_a) = make_manager();
        let (manager_b, _time_b) = make_manager();

        let contract = make_contract_key(1);
        let peer_a = make_peer_key(1);
        let peer_b = make_peer_key(2);

        let old_summary = StateSummary::from(vec![1, 2, 3]); // A's corrupted summary
        let new_summary = StateSummary::from(vec![4, 5, 6]); // B's correct summary

        // Setup: both peers have interest in the contract
        manager_a.register_local_seeding(&contract);
        manager_b.register_local_seeding(&contract);

        // A tracks B's summary, B tracks A's summary
        manager_a.register_peer_interest(
            &contract,
            peer_b.clone(),
            Some(new_summary.clone()),
            false,
        );
        manager_b.register_peer_interest(
            &contract,
            peer_a.clone(),
            Some(old_summary.clone()),
            false,
        );

        // Step 1: A sends ResyncRequest
        // B receives it and clears A's cached summary
        manager_b.update_peer_summary(&contract, &peer_a, None);

        // Verify B cleared A's summary
        let cached = manager_b.get_peer_summary(&contract, &peer_a);
        assert!(cached.is_none(), "B should have cleared A's summary");

        // Step 2: B sends ResyncResponse with full state and summary
        // A receives it and updates B's summary
        manager_a.update_peer_summary(&contract, &peer_b, Some(new_summary.clone()));

        // Verify A has B's new summary
        let cached = manager_a.get_peer_summary(&contract, &peer_b);
        assert!(cached.is_some(), "A should have B's summary");
        assert_eq!(
            cached.unwrap().as_ref(),
            new_summary.as_ref(),
            "A should have B's correct summary"
        );

        // Both peers should still be interested
        assert!(manager_a
            .get_interested_peers(&contract)
            .iter()
            .any(|(pk, _)| pk == &peer_b));
        assert!(manager_b
            .get_interested_peers(&contract)
            .iter()
            .any(|(pk, _)| pk == &peer_a));
    }

    #[test]
    fn test_delta_vs_full_state_decision() {
        // This test verifies the decision logic for when to send delta vs full state.
        // The decision is based on:
        // 1. Whether we have peer's summary (None = full state)
        // 2. Whether delta is efficient (summary < 50% of state size)

        let (manager, _time) = make_manager();
        let contract = make_contract_key(1);
        let peer_with_summary = make_peer_key(1);
        let peer_without_summary = make_peer_key(2);

        // Register local seeding to index the contract
        manager.register_local_seeding(&contract);

        // Small summary (efficient for delta)
        let small_summary = StateSummary::from(vec![1; 100]); // 100 bytes
        let large_state_size = 1000; // 1000 bytes -> summary is 10%, delta efficient

        // Large summary (not efficient for delta)
        let large_summary = StateSummary::from(vec![1; 600]); // 600 bytes
                                                              // 600/1000 = 60% > 50%, delta NOT efficient

        // Register peer1 with small summary (delta should be efficient)
        manager.register_peer_interest(
            &contract,
            peer_with_summary.clone(),
            Some(small_summary.clone()),
            false,
        );

        // Register peer2 with no summary (should send full state)
        manager.register_peer_interest(&contract, peer_without_summary.clone(), None, false);

        // Test 1: Peer with summary - check if delta is efficient
        let peer_summary = manager.get_peer_summary(&contract, &peer_with_summary);
        assert!(peer_summary.is_some(), "peer should have summary");
        let summary = peer_summary.unwrap();
        assert!(
            is_delta_efficient(summary.as_ref().len(), large_state_size),
            "small summary should be efficient for delta"
        );

        // Test 2: Peer without summary - should send full state
        let peer_summary = manager.get_peer_summary(&contract, &peer_without_summary);
        assert!(
            peer_summary.is_none(),
            "peer without summary should trigger full state"
        );

        // Test 3: Large summary - delta not efficient
        assert!(
            !is_delta_efficient(large_summary.as_ref().len(), large_state_size),
            "large summary (>50% of state) should not be efficient for delta"
        );

        // Test 4: Edge case - summary exactly 50% of state size
        let half_summary = StateSummary::from(vec![1; 500]); // 500 bytes
                                                             // 500 * 2 = 1000, not < 1000, so not efficient
        assert!(
            !is_delta_efficient(half_summary.as_ref().len(), large_state_size),
            "summary at exactly 50% boundary should not be efficient"
        );

        // Test 5: Summary just under 50%
        let just_under_half = StateSummary::from(vec![1; 499]); // 499 bytes
                                                                // 499 * 2 = 998 < 1000, so efficient
        assert!(
            is_delta_efficient(just_under_half.as_ref().len(), large_state_size),
            "summary just under 50% should be efficient"
        );
    }

    #[test]
    fn test_broadcast_peer_selection() {
        // Test that we correctly identify which peers to broadcast to
        // and whether to use delta or full state for each

        let (manager, _time) = make_manager();
        let contract = make_contract_key(1);

        let peer1 = make_peer_key(1); // Has summary
        let peer2 = make_peer_key(2); // No summary
        let peer3 = make_peer_key(3); // Has summary

        let summary1 = StateSummary::from(vec![1, 2, 3]);
        let summary3 = StateSummary::from(vec![3, 2, 1]);

        // Setup: register all peers with interest
        manager.register_local_seeding(&contract);
        manager.register_peer_interest(&contract, peer1.clone(), Some(summary1.clone()), false);
        manager.register_peer_interest(&contract, peer2.clone(), None, false);
        manager.register_peer_interest(&contract, peer3.clone(), Some(summary3.clone()), false);

        // Get all interested peers
        let interested = manager.get_interested_peers(&contract);
        assert_eq!(interested.len(), 3);

        // For each peer, check what type of update they should receive
        let mut delta_peers = Vec::new();
        let mut full_state_peers = Vec::new();

        for (peer_key, _interest) in &interested {
            if let Some(_summary) = manager.get_peer_summary(&contract, peer_key) {
                delta_peers.push(peer_key.clone());
            } else {
                full_state_peers.push(peer_key.clone());
            }
        }

        // Verify classification
        assert_eq!(delta_peers.len(), 2);
        assert!(delta_peers.contains(&peer1));
        assert!(delta_peers.contains(&peer3));

        assert_eq!(full_state_peers.len(), 1);
        assert!(full_state_peers.contains(&peer2));
    }

    #[test]
    fn test_get_contracts_for_peer() {
        let (manager, _time) = make_manager();
        let contract1 = make_contract_key(1);
        let contract2 = make_contract_key(2);
        let contract3 = make_contract_key(3);
        let peer = make_peer_key(1);

        // Initially no contracts for peer
        let contracts = manager.get_contracts_for_peer(&peer);
        assert!(contracts.is_empty());

        // Register peer interest in contracts 1 and 2
        manager.register_peer_interest(&contract1, peer.clone(), None, false);
        manager.register_peer_interest(&contract2, peer.clone(), None, false);

        let contracts = manager.get_contracts_for_peer(&peer);
        assert_eq!(contracts.len(), 2);
        assert!(contracts.contains(&contract1));
        assert!(contracts.contains(&contract2));
        assert!(!contracts.contains(&contract3));

        // Remove interest in contract1
        manager.remove_peer_interest(&contract1, &peer);
        let contracts = manager.get_contracts_for_peer(&peer);
        assert_eq!(contracts.len(), 1);
        assert!(contracts.contains(&contract2));
    }

    #[test]
    fn test_full_replace_interest_sync() {
        // Simulate the full-replace semantics used by heartbeat handler:
        // receiving Interests { hashes } should add new entries, refresh shared
        // entries, and remove entries not in the incoming set.
        let (manager, time) = make_manager();
        let contract1 = make_contract_key(1);
        let contract2 = make_contract_key(2);
        let contract3 = make_contract_key(3);
        let peer = make_peer_key(1);

        // We have local interest in all three contracts
        manager.register_local_seeding(&contract1);
        manager.register_local_seeding(&contract2);
        manager.register_local_seeding(&contract3);

        // Initial state: peer is interested in contracts 1 and 2
        manager.register_peer_interest(&contract1, peer.clone(), None, false);
        manager.register_peer_interest(&contract2, peer.clone(), None, false);

        // Advance time so we can verify refresh
        time.advance_time(Duration::from_secs(60));

        // Simulate heartbeat: peer now sends hashes for contracts 2 and 3
        // (dropped 1, kept 2, added 3)
        let incoming_hashes: HashSet<u32> = [contract_hash(&contract2), contract_hash(&contract3)]
            .into_iter()
            .collect();

        // Step 1: Get peer's current interest set
        let current_contracts = manager.get_contracts_for_peer(&peer);
        assert_eq!(current_contracts.len(), 2);

        // Step 2: Remove entries whose hash is NOT in incoming set
        // (mirrors the handler's hash-domain comparison, not resolved keys)
        for contract in &current_contracts {
            let h = contract_hash(contract);
            if !incoming_hashes.contains(&h) {
                manager.remove_peer_interest(contract, &peer);
            }
        }

        // Step 3: Find matching contracts and register/refresh
        let matching =
            manager.get_matching_contracts(&incoming_hashes.iter().copied().collect::<Vec<_>>());
        for contract in &matching {
            if manager.get_peer_interest(contract, &peer).is_some() {
                // Existing entry: refresh TTL (preserves cached summary)
                manager.refresh_peer_interest(contract, &peer);
            } else {
                // New entry
                manager.register_peer_interest(contract, peer.clone(), None, false);
            }
        }

        // Verify: contract1 removed, contract2 refreshed, contract3 added
        assert!(
            manager.get_peer_interest(&contract1, &peer).is_none(),
            "contract1 should have been removed"
        );
        assert!(
            manager.get_peer_interest(&contract2, &peer).is_some(),
            "contract2 should still exist (refreshed)"
        );
        assert!(
            manager.get_peer_interest(&contract3, &peer).is_some(),
            "contract3 should have been added"
        );

        // Verify contract2 was refreshed (TTL reset)
        let interest2 = manager.get_peer_interest(&contract2, &peer).unwrap();
        assert!(
            !interest2.is_expired_at(time.now()),
            "contract2 interest should not be expired after refresh"
        );
    }

    #[test]
    fn test_refresh_preserves_summary() {
        // Verify that refresh_peer_interest preserves the cached summary,
        // unlike register_peer_interest which overwrites it.
        let (manager, time) = make_manager();
        let contract = make_contract_key(1);
        let peer = make_peer_key(1);
        let summary = StateSummary::from(vec![1, 2, 3]);

        // Register with a summary
        manager.register_peer_interest(&contract, peer.clone(), Some(summary.clone()), false);

        // Advance time
        time.advance_time(Duration::from_secs(60));

        // Refresh TTL (should preserve summary)
        manager.refresh_peer_interest(&contract, &peer);

        // Verify summary is still there
        let cached = manager.get_peer_summary(&contract, &peer);
        assert!(
            cached.is_some(),
            "summary should be preserved after refresh"
        );
        assert_eq!(cached.unwrap().as_ref(), summary.as_ref());

        // Verify TTL was reset
        let interest = manager.get_peer_interest(&contract, &peer).unwrap();
        assert!(
            !interest.is_expired_at(time.now()),
            "interest should not be expired after refresh"
        );
    }

    #[test]
    fn test_is_upstream_flag_registration() {
        let (manager, _time) = make_manager();
        let contract = make_contract_key(1);
        let upstream_peer = make_peer_key(1);
        let downstream_peer = make_peer_key(2);

        // Register upstream peer with is_upstream=true
        manager.register_peer_interest(&contract, upstream_peer.clone(), None, true);

        // Register downstream peer with is_upstream=false
        manager.register_peer_interest(&contract, downstream_peer.clone(), None, false);

        // Verify the is_upstream flag is preserved correctly
        let upstream_interest = manager
            .get_peer_interest(&contract, &upstream_peer)
            .unwrap();
        assert!(
            upstream_interest.is_upstream,
            "Peer registered with is_upstream=true should have is_upstream=true"
        );

        let downstream_interest = manager
            .get_peer_interest(&contract, &downstream_peer)
            .unwrap();
        assert!(
            !downstream_interest.is_upstream,
            "Peer registered with is_upstream=false should have is_upstream=false"
        );

        // Verify get_interested_peers returns both with correct flags
        let peers = manager.get_interested_peers(&contract);
        assert_eq!(peers.len(), 2);

        let upstream_entry = peers.iter().find(|(k, _)| k == &upstream_peer).unwrap();
        assert!(upstream_entry.1.is_upstream);

        let downstream_entry = peers.iter().find(|(k, _)| k == &downstream_peer).unwrap();
        assert!(!downstream_entry.1.is_upstream);
    }

    #[test]
    fn test_register_peer_interest_resets_ttl() {
        // Verify that register_peer_interest resets TTL for existing entries.
        // The heartbeat relies on this for new-entry registration.
        let (manager, time) = make_manager();
        let contract = make_contract_key(1);
        let peer = make_peer_key(1);

        // Register interest
        manager.register_peer_interest(&contract, peer.clone(), None, false);

        // Advance time to nearly expired
        time.advance_time(INTEREST_TTL - Duration::from_secs(10));

        // Re-register (as heartbeat would for a new entry)
        manager.register_peer_interest(&contract, peer.clone(), None, false);

        // Advance time past original registration but not past re-registration
        time.advance_time(Duration::from_secs(20));

        // Should not be expired
        let expired = manager.sweep_expired_interests();
        assert!(expired.is_empty(), "re-registration should have reset TTL");
        assert!(manager.get_peer_interest(&contract, &peer).is_some());
    }

    #[test]
    fn test_subscribe_registers_local_interest() {
        let (manager, _time) = make_manager();
        let contract = make_contract_key(1);

        assert!(!manager.has_local_interest(&contract));

        let became_interested = manager.add_local_client(&contract);
        assert!(became_interested);
        assert!(manager.has_local_interest(&contract));

        // Second call should not report "became interested" (already was)
        let became_interested_again = manager.add_local_client(&contract);
        assert!(!became_interested_again);
        assert!(manager.has_local_interest(&contract));
    }

    /// Regression test for #3467: relay nodes must have has_local_interest() = true
    /// when they have downstream subscribers, otherwise ChangeInterests processing
    /// is blocked and interest-based broadcast targeting breaks.
    #[test]
    fn test_downstream_subscriber_creates_local_interest() {
        let (manager, _time) = make_manager();
        let contract = make_contract_key(50);

        // Before adding downstream: no local interest
        assert!(!manager.has_local_interest(&contract));

        // Add downstream subscriber — should create local interest
        let became_interested = manager.add_downstream_subscriber(&contract);
        assert!(
            became_interested,
            "First downstream subscriber should create interest"
        );
        assert!(
            manager.has_local_interest(&contract),
            "Relay node with downstream subscriber must have local interest"
        );

        // Second downstream subscriber should not re-report "became interested"
        let became_interested_again = manager.add_downstream_subscriber(&contract);
        assert!(!became_interested_again);
        assert!(manager.has_local_interest(&contract));

        // Remove one downstream — still have one left
        let lost_interest = manager.remove_downstream_subscriber(&contract);
        assert!(!lost_interest, "Still have one downstream subscriber");
        assert!(manager.has_local_interest(&contract));

        // Remove last downstream — should lose interest
        let lost_interest = manager.remove_downstream_subscriber(&contract);
        assert!(lost_interest, "Last downstream subscriber removed");
        assert!(
            !manager.has_local_interest(&contract),
            "No downstream subscribers left — should lose interest"
        );
    }

    #[test]
    fn test_deferred_removal_executes_after_grace_period() {
        let (manager, time) = make_manager();
        let contract = make_contract_key(1);
        let peer = make_peer_key(1);

        // Register interest
        manager.register_peer_interest(&contract, peer.clone(), None, false);
        assert!(manager.get_peer_interest(&contract, &peer).is_some());

        // Schedule deferred removal
        manager.schedule_deferred_removal(&peer);

        // Before grace period expires, interests should still exist
        time.advance_time(INTEREST_DISCONNECT_GRACE_PERIOD - Duration::from_secs(1));
        let removed = manager.execute_pending_removals();
        assert_eq!(removed, 0);
        assert!(manager.get_peer_interest(&contract, &peer).is_some());

        // After grace period expires, interests should be removed
        time.advance_time(Duration::from_secs(2));
        let removed = manager.execute_pending_removals();
        assert_eq!(removed, 1);
        assert!(manager.get_peer_interest(&contract, &peer).is_none());
    }

    #[test]
    fn test_deferred_removal_cancelled_on_reconnect() {
        let (manager, time) = make_manager();
        let contract = make_contract_key(1);
        let peer = make_peer_key(1);

        // Register interest
        manager.register_peer_interest(&contract, peer.clone(), None, false);

        // Schedule deferred removal (peer disconnected)
        manager.schedule_deferred_removal(&peer);

        // Peer reconnects within grace period
        time.advance_time(Duration::from_secs(30));
        let cancelled = manager.cancel_deferred_removal(&peer);
        assert!(cancelled);

        // Even after grace period, interests should still exist
        time.advance_time(INTEREST_DISCONNECT_GRACE_PERIOD);
        let removed = manager.execute_pending_removals();
        assert_eq!(removed, 0);
        assert!(manager.get_peer_interest(&contract, &peer).is_some());
    }

    #[test]
    fn test_deferred_removal_replaces_on_repeated_disconnect() {
        let (manager, time) = make_manager();
        let contract = make_contract_key(1);
        let peer = make_peer_key(1);

        manager.register_peer_interest(&contract, peer.clone(), None, false);

        // First disconnect
        manager.schedule_deferred_removal(&peer);
        time.advance_time(Duration::from_secs(60));

        // Second disconnect before first grace period expires — resets deadline
        manager.schedule_deferred_removal(&peer);

        // Original deadline would have passed, but new one hasn't
        time.advance_time(Duration::from_secs(60));
        let removed = manager.execute_pending_removals();
        assert_eq!(removed, 0, "Second schedule should have reset the deadline");
        assert!(manager.get_peer_interest(&contract, &peer).is_some());

        // Now exceed the second deadline
        time.advance_time(Duration::from_secs(31));
        let removed = manager.execute_pending_removals();
        assert_eq!(removed, 1);
        assert!(manager.get_peer_interest(&contract, &peer).is_none());
    }

    #[test]
    fn test_cancel_deferred_removal_returns_false_when_none_pending() {
        let (manager, _time) = make_manager();
        let peer = make_peer_key(1);

        // No pending removal — cancel should return false
        assert!(!manager.cancel_deferred_removal(&peer));
    }

    /// Regression test: if cancel_deferred_removal runs between the collect phase
    /// and the removal phase of execute_pending_removals, the removal must be
    /// skipped (the peer reconnected). Without the guard on pending_removals.remove(),
    /// interests would be wiped even though the peer is back.
    #[test]
    fn test_execute_skips_removal_if_cancelled_between_collect_and_remove() {
        let (manager, time) = make_manager();
        let contract = make_contract_key(1);
        let peer = make_peer_key(1);

        manager.register_peer_interest(&contract, peer.clone(), None, false);
        manager.schedule_deferred_removal(&peer);

        // Advance past grace period
        time.advance_time(INTEREST_DISCONNECT_GRACE_PERIOD + Duration::from_secs(1));

        // Simulate reconnect cancelling the pending removal before sweep executes
        manager.cancel_deferred_removal(&peer);

        // execute_pending_removals should return 0 — the entry was already cancelled
        let removed = manager.execute_pending_removals();
        assert_eq!(removed, 0);
        assert!(
            manager.get_peer_interest(&contract, &peer).is_some(),
            "Interests must be preserved when peer reconnected before sweep executed"
        );
    }
}
