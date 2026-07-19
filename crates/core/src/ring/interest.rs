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
use std::net::SocketAddr;
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

/// Max distinct peers tracked as interested in a single contract.
/// Matches MAX_DOWNSTREAM_SUBSCRIBERS_PER_CONTRACT (hosting.rs) so the two
/// broadcast-target sources are symmetrically bounded (#3798 Gap 2).
pub(crate) const MAX_INTERESTED_PEERS_PER_CONTRACT: usize = 512;

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

/// Minimum interval between queue-full `ResyncRequest`s to the same peer for
/// the same contract (issue #4857).
///
/// A `ContractQueueFull` broadcast drop is silent: the receiver never applied
/// the delta, but the SENDER cached its own summary as ours on send-Ok
/// (`broadcast_queue.rs::record_delivery_to_interest`), so it believes we are
/// current and will never re-send the dropped change. Left unhealed, a
/// rarely-changing field diverges permanently until the ~5-min InterestSync
/// heartbeat happens to correct it. Emitting a `ResyncRequest` makes the sender
/// clear its cached summary of us and re-send full state — but issue #4251
/// showed that one request per dropped delta amplifies into a full-state storm
/// onto the same saturated queue. This interval bounds that amplification to at
/// most one request per (contract, peer) window while still healing far faster
/// than the heartbeat backstop.
const RESYNC_REQUEST_MIN_INTERVAL: Duration = Duration::from_secs(30);

/// Bound on the number of (contract, peer) entries in the queue-full
/// `ResyncRequest` throttle. The key is influenced by remote peers (any peer
/// can broadcast any contract to us), so it MUST be bounded — see the
/// per-key-collection rule in `.claude/rules/code-style.md`. LRU eviction fails
/// open: forgetting an entry merely permits one extra healing `ResyncRequest`,
/// which is safe.
const RESYNC_THROTTLE_CACHE_SIZE: usize = 4096;

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
    /// Whether we're hosting this contract (in our local cache).
    pub hosting: bool,

    /// Number of local WebSocket clients subscribed to this contract.
    pub local_client_count: usize,

    /// Number of downstream peers subscribed through us.
    pub downstream_subscriber_count: usize,
}

impl LocalInterest {
    /// Check if we have any reason to be interested in this contract.
    pub fn is_interested(&self) -> bool {
        self.hosting || self.local_client_count > 0 || self.downstream_subscriber_count > 0
    }

    /// Increment the local client count and return whether this is the first client.
    pub fn add_client(&mut self) -> bool {
        let was_first = self.local_client_count == 0;
        self.local_client_count += 1;
        was_first && !self.hosting && self.downstream_subscriber_count == 0
    }

    /// Decrement the local client count and return whether interest was lost.
    pub fn remove_client(&mut self) -> bool {
        self.local_client_count = self.local_client_count.saturating_sub(1);
        !self.is_interested()
    }

    /// Increment the downstream subscriber count and return whether this is the first.
    pub fn add_downstream(&mut self) -> bool {
        let was_first =
            self.downstream_subscriber_count == 0 && self.local_client_count == 0 && !self.hosting;
        self.downstream_subscriber_count += 1;
        was_first
    }

    /// Decrement the downstream subscriber count and return whether interest was lost.
    pub fn remove_downstream(&mut self) -> bool {
        self.downstream_subscriber_count = self.downstream_subscriber_count.saturating_sub(1);
        !self.is_interested()
    }

    /// Set hosting status and return whether interest state changed.
    pub fn set_hosting(&mut self, hosting: bool) -> bool {
        let was_interested = self.is_interested();
        self.hosting = hosting;
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

    /// Rate-limit gate for queue-full `ResyncRequest`s, keyed by
    /// (contract, target peer address). Bounded LRU so remote peers cannot grow
    /// it without bound. See [`InterestManager::should_send_resync_request`] and
    /// issue #4857.
    resync_request_throttle: Mutex<LruCache<(ContractKey, SocketAddr), Instant>>,
}

impl<T: TimeSource + Sync> InterestManager<T> {
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
            resync_request_throttle: Mutex::new(LruCache::new(
                NonZeroUsize::new(RESYNC_THROTTLE_CACHE_SIZE)
                    .expect("RESYNC_THROTTLE_CACHE_SIZE must be > 0"),
            )),
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
        // Hold the `interested_peers` shard guard across `peer_contracts`
        // insertion and `index_contract_hash` to keep the three writes
        // atomic against a concurrent `remove_peer_interest` (which would
        // otherwise observe a fully-removed peer and unindex a contract
        // we're about to re-index, leaving a zombie entry).
        // This intentionally undoes the PR #4129 `significant_drop_tightening`
        // change for these four sites — see PR notes.
        let mut entry = self.interested_peers.entry(*contract).or_default();
        let is_new = !entry.contains_key(&peer);

        // Cap distinct interested peers per contract to bound an adversarial
        // broadcast-amplification vector (#3798 Gap 2). Reject BEFORE the
        // reverse-index/hash writes below so a rejected peer leaves no zombie
        // `peer_contracts` / `contract_hash_index` entry. Only a NEW peer at
        // capacity is rejected — renewals of an already-tracked peer always
        // proceed so a legit at-capacity contract keeps serving its peers.
        // Returns `is_new = false` so a rejected adversary is not treated as a
        // new viable target and cannot trigger the #4359 pending-broadcast flush.
        if is_new && entry.len() >= MAX_INTERESTED_PEERS_PER_CONTRACT {
            drop(entry);
            tracing::warn!(
                contract = %contract,
                limit = MAX_INTERESTED_PEERS_PER_CONTRACT,
                "Interested-peer limit reached, rejecting peer"
            );
            return false;
        }

        entry.insert(peer.clone(), PeerInterest::new(summary, is_upstream, now));

        // Maintain reverse index for O(1) peer disconnect cleanup
        self.peer_contracts
            .entry(peer)
            .or_default()
            .insert(*contract);

        // Also index by hash for fast lookup
        self.index_contract_hash(contract);

        drop(entry);
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

    /// Rate-limit gate for queue-full `ResyncRequest`s to `target` for
    /// `contract`. Returns `true` (and records the send) when at least
    /// [`RESYNC_REQUEST_MIN_INTERVAL`] has elapsed since the last such request
    /// for this (contract, target) pair, or when none was ever sent.
    ///
    /// Issue #4857: a `ContractQueueFull` broadcast drop is silent — the
    /// receiver never applied the delta, but the SENDER cached its own summary
    /// as ours on send-Ok, so it believes we are current and will never re-send
    /// the dropped change. A `ResyncRequest` makes the sender clear that cached
    /// summary and re-send full state. Issue #4251 suppressed the request
    /// entirely because one-per-dropped-delta amplifies into a full-state storm;
    /// this gate keeps the healing signal but bounds it to one per window.
    pub fn should_send_resync_request(&self, contract: &ContractKey, target: SocketAddr) -> bool {
        let now = self.time_source.now();
        let key = (*contract, target);
        let mut throttle = self.resync_request_throttle.lock();
        if let Some(&last) = throttle.get(&key) {
            if now.duration_since(last) < RESYNC_REQUEST_MIN_INTERVAL {
                return false;
            }
        }
        throttle.put(key, now);
        true
    }

    /// PEEK the per-(contract, target) resync-request throttle WITHOUT recording a
    /// send: returns true when a send would be ALLOWED (the
    /// [`RESYNC_REQUEST_MIN_INTERVAL`] window has elapsed, or none was ever sent).
    ///
    /// Pair with [`Self::record_resync_request_sent`], which starts the window
    /// ONLY once the send actually happens (#4864 round-5 item 9). The queue-full
    /// heal is double-gated (this per-sender throttle AND the global per-contract
    /// emit cap); the combined check-AND-record `should_send_resync_request` would
    /// burn the 30s window for a send the global cap then suppressed, blocking
    /// this (contract, target) from retrying for up to 30s after the global cap's
    /// tokens refill.
    pub fn peek_should_send_resync_request(
        &self,
        contract: &ContractKey,
        target: SocketAddr,
    ) -> bool {
        let now = self.time_source.now();
        let key = (*contract, target);
        let mut throttle = self.resync_request_throttle.lock();
        // `get` bumps LRU recency but records no send timestamp.
        match throttle.get(&key) {
            Some(&last) => now.duration_since(last) >= RESYNC_REQUEST_MIN_INTERVAL,
            None => true,
        }
    }

    /// Record that a resync request was actually SENT for (contract, target),
    /// starting a fresh [`RESYNC_REQUEST_MIN_INTERVAL`] throttle window. Call only
    /// AFTER the send passes every gate and is emitted (#4864 round-5 item 9); see
    /// [`Self::peek_should_send_resync_request`].
    pub fn record_resync_request_sent(&self, contract: &ContractKey, target: SocketAddr) {
        let now = self.time_source.now();
        self.resync_request_throttle
            .lock()
            .put((*contract, target), now);
    }

    /// Remove all interests for a peer (called on peer disconnect).
    ///
    /// Uses the reverse index for O(1) lookup instead of O(contracts) scan.
    /// Returns the number of contracts from which the peer was actually removed.
    ///
    /// # Concurrency
    ///
    /// This is a *secondary-origin* remover: it starts from the
    /// `peer_contracts` reverse index. It does NOT remove the
    /// `peer_contracts` entry up front and then mutate `interested_peers`
    /// directly — that older shape had a bidirectional-consistency race
    /// (issue #4174): a concurrent `register_peer_interest(C, peer, ..)`
    /// running between the up-front `peer_contracts.remove` and the
    /// per-contract `interested_peers` mutation could re-insert `peer`
    /// into both maps, after which this method would strip `peer` from
    /// `interested_peers[C]` while leaving the reverse entry in
    /// `peer_contracts[peer]` behind.
    ///
    /// Instead it merely *snapshots* the contract set and delegates each
    /// per-contract cleanup to `remove_peer_interest`, which holds the
    /// `interested_peers[contract]` shard guard across the matching
    /// `peer_contracts` update — so every per-contract removal is
    /// atomic and the invariant
    /// `peer ∈ peer_contracts[peer] ⇔ peer ∈ interested_peers[contract]`
    /// is preserved even under a concurrent re-registration.
    pub fn remove_all_peer_interests(&self, peer: &PeerKey) -> usize {
        // Snapshot the contracts this peer is interested in WITHOUT
        // removing the reverse-index entry — `remove_peer_interest`
        // owns the `peer_contracts` update for each contract so the
        // two maps stay consistent (issue #4174).
        let contracts: Vec<ContractKey> = self
            .peer_contracts
            .get(peer)
            .map(|entry| entry.value().iter().cloned().collect())
            .unwrap_or_default();

        // Delegate each per-contract cleanup to `remove_peer_interest`,
        // which atomically updates both `interested_peers` and
        // `peer_contracts` under the contract's shard guard and also
        // runs `cleanup_contract_if_no_interest`. Count only the
        // contracts from which the peer was actually removed (a
        // concurrent `remove_peer_interest` for the same pair may have
        // already cleared an entry between the snapshot and here).
        let removed_count = contracts
            .iter()
            .filter(|contract| self.remove_peer_interest(contract, peer))
            .count();

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
    ///
    /// Currently unused inside the workspace but kept `pub` for external
    /// consumers; same lock-across-index discipline as
    /// [`Self::register_local_hosting`] applies so the method is not a
    /// PR #4129–shaped race footgun.
    pub fn register_local_interest(&self, contract: &ContractKey) -> &Self {
        let entry = self.local_interests.entry(*contract).or_default();
        self.index_contract_hash(contract);
        drop(entry);
        self
    }

    /// Register that we're hosting a contract locally.
    /// Returns true if this caused us to become interested (wasn't interested before).
    pub fn register_local_hosting(&self, contract: &ContractKey) -> bool {
        // Hold the `local_interests` shard guard across `index_contract_hash`
        // so a concurrent `remove_local_client` / `unregister_local_hosting`
        // for the last reason cannot run its cleanup (unindex no-op) before
        // we index, leaving a zombie entry in `contract_hash_index`.
        let mut entry = self.local_interests.entry(*contract).or_default();
        let was_interested = entry.is_interested();
        entry.hosting = true;
        self.index_contract_hash(contract);
        drop(entry);
        !was_interested
    }

    /// Unregister that we're hosting a contract locally.
    /// Returns true if this caused us to lose interest (no other reasons remain).
    pub fn unregister_local_hosting(&self, contract: &ContractKey) -> bool {
        if let Some(mut entry) = self.local_interests.get_mut(contract) {
            entry.hosting = false;
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
        // Same lock-across-index discipline as `register_local_hosting`:
        // hold the `local_interests` shard guard across
        // `index_contract_hash` to prevent a concurrent
        // `remove_local_client` from unindexing-before-we-index.
        let mut entry = self.local_interests.entry(*contract).or_default();
        let became_interested = entry.add_client();
        self.index_contract_hash(contract);
        drop(entry);
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
        // Same lock-across-index discipline as `register_local_hosting`.
        let mut entry = self.local_interests.entry(*contract).or_default();
        let became_interested = entry.add_downstream();
        self.index_contract_hash(contract);
        drop(entry);
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

    /// Mirror a subscriber-primary eviction that tore down a still-in-use
    /// contract's hosting subscription state (#4642 invariant 3, PR #4734).
    ///
    /// The `InterestManager` lives on `OpManager`, NOT on `HostingManager`, so
    /// when `HostingManager::teardown_evicted_in_use_contract` clears the
    /// hosting maps (`downstream_subscribers` + `client_subscriptions`) the
    /// eviction CONSUMER must replay the identical removals here or ghost
    /// `interested_peers` / `peer_contracts` / `local_client_count` entries
    /// survive. Those ghosts are load-bearing — they drive UPDATE broadcast
    /// targeting (`get_interested_peers`) and upstream interest counts — and do
    /// NOT self-heal, because the reconcilers iterate the very hosting maps the
    /// teardown just emptied.
    ///
    /// Mirrors, exactly:
    /// - `handle_unsubscribe_inbound` per downstream peer: `remove_peer_interest`
    ///   (clears `interested_peers` / `peer_contracts`) + `remove_downstream_subscriber`
    ///   (decrements the local `downstream_subscriber_count`).
    /// - the client-disconnect path per local client: `remove_local_client`
    ///   (decrements `local_client_count`).
    ///
    /// Idempotent and safe on an already-clean contract (each removal is a
    /// no-op when absent).
    pub fn remove_evicted_in_use(
        &self,
        contract: &ContractKey,
        downstream_peers: &[PeerKey],
        local_client_count: usize,
    ) {
        for peer in downstream_peers {
            self.remove_peer_interest(contract, peer);
            self.remove_downstream_subscriber(contract);
        }
        for _ in 0..local_client_count {
            self.remove_local_client(contract);
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

    /// Count contracts backed by *real demand*: a local client subscription or
    /// a downstream subscriber. This deliberately EXCLUDES the cache-only
    /// `hosting` reason, so it does not grow with the hosting cache.
    ///
    /// This is the denominator for the #3763 no-storm invariant: renewal /
    /// subscription volume must scale with active demand, not with cache size.
    /// `LocalInterest::is_interested()` (which folds in `hosting`) is the wrong
    /// signal for that check — see the sim assertions in
    /// `simulation_integration.rs` and the unit test
    /// `test_contracts_needing_renewal_bounded_by_active_interest`.
    ///
    /// Test/sim-only accessor (reached via `Ring::active_demand_count`).
    #[cfg(any(test, feature = "testing"))]
    pub fn active_demand_count(&self) -> usize {
        self.local_interests
            .iter()
            .filter(|entry| {
                let li = entry.value();
                li.local_client_count > 0 || li.downstream_subscriber_count > 0
            })
            .count()
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

    /// Get the size (in bytes) of the locally-stored state for a contract.
    ///
    /// Mirrors [`get_contract_summary`](Self::get_contract_summary): a bounded
    /// (`BROADCAST_CH_TIMEOUT`) `GetQuery` against the contract handler,
    /// returning the stored state's `size()` or `None` if it can't be read.
    /// Used by the summary-first PUT reverse leg to feed
    /// [`compute_delta`](Self::compute_delta)'s efficiency gate with the
    /// holder's own state size (the holder-side mirror of the originator's
    /// `merged_value.size()`).
    pub async fn get_contract_state_size(
        &self,
        op_manager: &crate::node::OpManager,
        key: &ContractKey,
    ) -> Option<usize> {
        use crate::contract::ContractHandlerEvent;

        match op_manager
            .notify_contract_handler_with_timeout(
                ContractHandlerEvent::GetQuery {
                    instance_id: *key.id(),
                    return_contract_code: false,
                },
                BROADCAST_CH_TIMEOUT,
            )
            .await
        {
            Ok(ContractHandlerEvent::GetResponse {
                response: Ok(store_response),
                ..
            }) => store_response.state.map(|state| state.size()),
            Ok(ContractHandlerEvent::GetResponse {
                response: Err(e), ..
            }) => {
                tracing::debug!(
                    contract = %key,
                    error = %e,
                    "Failed to get contract state size"
                );
                None
            }
            Ok(other) => {
                tracing::warn!(
                    contract = %key,
                    response = ?other,
                    "Unexpected response to GetQuery (state size)"
                );
                None
            }
            Err(e) => {
                tracing::debug!(
                    contract = %key,
                    error = %e,
                    "Error getting contract state size"
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

    /// Like `make_contract_key` but with a `u32` seed for tests that need
    /// many distinct contracts.
    fn make_unique_contract_key(seed: u32) -> ContractKey {
        let s = seed.to_le_bytes();
        let mut id = [0u8; 32];
        id[0..4].copy_from_slice(&s);
        let mut code = [0u8; 32];
        code[0..4].copy_from_slice(&s);
        code[4] = 0xAB;
        ContractKey::from_id_and_code(ContractInstanceId::new(id), CodeHash::new(code))
    }

    /// Build a deterministic peer key from a seed.
    ///
    /// Deterministic-and-distinct so tests never rely on RNG distinctness:
    /// distinct seeds always yield distinct keys, and the same seed always
    /// yields the same key (mirrors the sibling `hosting.rs` test helper).
    fn make_peer_key(seed: u8) -> PeerKey {
        make_unique_peer_key(seed as u32)
    }

    /// Like `make_peer_key` but with a `u32` seed for tests that need more
    /// than 256 pairwise-distinct peers (mirrors `make_unique_contract_key`).
    fn make_unique_peer_key(seed: u32) -> PeerKey {
        let mut bytes = [0u8; 32];
        bytes[0..4].copy_from_slice(&seed.to_le_bytes());
        PeerKey(crate::transport::TransportPublicKey::from_bytes(bytes))
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

    /// Regression for the InterestManager desync on subscribed eviction
    /// (PR #4734 Fix 1). When a subscriber-primary eviction shed + tore down a
    /// still-in-use contract, the hosting maps are cleared by
    /// `HostingManager::teardown_evicted_in_use_contract`, but the
    /// InterestManager lives on `OpManager` and must be synced separately by the
    /// consumer via `remove_evicted_in_use`. Before this fix, ghost
    /// `interested_peers` / `peer_contracts` / `local_client_count` entries
    /// survived (they drive UPDATE broadcast targeting + upstream interest
    /// counts) and did NOT self-heal. Assert every map is ZERO afterward — the
    /// gap the HostingManager-level `torn_down_...` test did not cover.
    #[test]
    fn remove_evicted_in_use_clears_all_interest_maps() {
        let (manager, _time) = make_manager();
        let contract = make_contract_key(1);
        let downstream_a = make_peer_key(1);
        let downstream_b = make_peer_key(2);

        // Mirror the real add path: per downstream peer,
        // register_peer_interest (interested_peers/peer_contracts) +
        // add_downstream_subscriber (downstream_subscriber_count). Plus two
        // local client subscriptions (local_client_count).
        manager.register_peer_interest(&contract, downstream_a.clone(), None, false);
        manager.add_downstream_subscriber(&contract);
        manager.register_peer_interest(&contract, downstream_b.clone(), None, false);
        manager.add_downstream_subscriber(&contract);
        manager.add_local_client(&contract);
        manager.add_local_client(&contract);

        // Sanity: interest present in all three maps before teardown.
        assert_eq!(manager.get_interested_peers(&contract).len(), 2);
        assert!(!manager.get_contracts_for_peer(&downstream_a).is_empty());
        assert!(!manager.get_contracts_for_peer(&downstream_b).is_empty());
        manager.with_local_interest(&contract, |li| {
            assert_eq!(li.local_client_count, 2);
            assert_eq!(li.downstream_subscriber_count, 2);
        });

        // Replay the hosting teardown against the InterestManager exactly as the
        // eviction consumers do.
        manager.remove_evicted_in_use(&contract, &[downstream_a.clone(), downstream_b.clone()], 2);

        // interested_peers / peer_contracts / local_client_count all ZERO — no
        // ghost survives to mis-target UPDATE broadcasts or inflate counts.
        assert!(
            manager.get_interested_peers(&contract).is_empty(),
            "interested_peers must be cleared for the evicted contract"
        );
        assert!(
            manager.get_contracts_for_peer(&downstream_a).is_empty(),
            "peer_contracts[downstream_a] must be cleared"
        );
        assert!(
            manager.get_contracts_for_peer(&downstream_b).is_empty(),
            "peer_contracts[downstream_b] must be cleared"
        );
        // has_local_interest reads via `.get` (no entry re-creation), so a false
        // result proves the local_interests entry — local_client_count and
        // downstream_subscriber_count — is fully gone.
        assert!(
            !manager.has_local_interest(&contract),
            "no local interest (client or downstream count) may remain"
        );
        let stats = manager.stats();
        assert_eq!(
            stats.total_contracts, 0,
            "no contract may retain interested peers"
        );
        assert_eq!(stats.total_peer_interests, 0);
        assert_eq!(
            stats.local_interests, 0,
            "no local_interests entry may survive"
        );
        assert_eq!(
            stats.hash_index_size, 0,
            "the contract hash index must be cleaned up once no interest remains"
        );

        // Idempotent: replaying on an already-clean contract is a no-op.
        manager.remove_evicted_in_use(&contract, &[downstream_a], 1);
        assert_eq!(manager.stats().total_contracts, 0);
        assert_eq!(manager.stats().local_interests, 0);
    }

    #[test]
    fn test_register_peer_interest_caps_at_max() {
        // #3798 Gap 2: a single contract's interested_peers map must be bounded
        // so a peer flooding distinct identities cannot amplify every broadcast.
        let (manager, _time) = make_manager();
        let contract = make_contract_key(1);

        // Fill to exactly MAX distinct peers — each is new and accepted.
        // Keys are deterministic AND pairwise-distinct (derived from a u32
        // counter), so the test never relies on RNG distinctness: a leaked
        // thread-local GlobalRng seed or a one-in-a-billion keypair collision
        // can no longer make the 513th registration spuriously non-new and
        // skip the cap branch (the cold-build flake this hardening fixes).
        let mut peers = Vec::with_capacity(MAX_INTERESTED_PEERS_PER_CONTRACT);
        for i in 0..MAX_INTERESTED_PEERS_PER_CONTRACT {
            let peer = make_unique_peer_key(i as u32);
            assert!(
                manager.register_peer_interest(&contract, peer.clone(), None, false),
                "registering a fresh peer below capacity must return is_new = true"
            );
            peers.push(peer);
        }
        assert_eq!(
            manager.get_interested_peers(&contract).len(),
            MAX_INTERESTED_PEERS_PER_CONTRACT
        );

        // One MORE distinct peer is rejected: returns is_new = false (so it does
        // NOT trigger the #4359 first-viable-target broadcast flush) and the map
        // length is unchanged. Its seed is past the fill range, so it is
        // guaranteed not already tracked.
        let overflow_peer = make_unique_peer_key(MAX_INTERESTED_PEERS_PER_CONTRACT as u32);
        assert!(
            !manager.register_peer_interest(&contract, overflow_peer.clone(), None, false),
            "a new peer at capacity must be rejected (is_new = false)"
        );
        assert_eq!(
            manager.get_interested_peers(&contract).len(),
            MAX_INTERESTED_PEERS_PER_CONTRACT,
            "capacity must not be exceeded"
        );

        // Invariant: the rejected peer left NO zombie reverse-index entry.
        assert!(
            manager.get_contracts_for_peer(&overflow_peer).is_empty(),
            "rejected peer must not appear in the peer_contracts reverse index"
        );

        // Renewals of an ALREADY-tracked peer are never rejected by capacity:
        // re-registering an existing peer with an updated summary returns false
        // (not new) but still refreshes the entry.
        let existing = peers[0].clone();
        let summary = StateSummary::from(vec![9, 9, 9]);
        assert!(
            !manager.register_peer_interest(
                &contract,
                existing.clone(),
                Some(summary.clone()),
                false
            ),
            "renewal of an existing peer must return is_new = false"
        );
        assert_eq!(
            manager.get_interested_peers(&contract).len(),
            MAX_INTERESTED_PEERS_PER_CONTRACT,
            "renewal must not change capacity"
        );
        let refreshed = manager
            .get_peer_summary(&contract, &existing)
            .expect("existing peer must still be present after renewal");
        assert_eq!(
            refreshed.as_ref(),
            summary.as_ref(),
            "renewal must update the existing peer's summary"
        );
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

    /// Issue #4857: `should_send_resync_request` must emit at most one
    /// `ResyncRequest` per (contract, peer) per `RESYNC_REQUEST_MIN_INTERVAL`.
    /// The first drop heals immediately; a burst of further drops within the
    /// window is throttled (bounding the #4251 amplification); after the window
    /// elapses a fresh request is allowed again.
    #[test]
    fn should_send_resync_request_rate_limits_per_contract_peer() {
        let (manager, time) = make_manager();
        let contract = make_contract_key(1);
        let addr: SocketAddr = "127.0.0.1:5001".parse().unwrap();

        // First drop for this (contract, peer) → allowed (immediate heal).
        assert!(
            manager.should_send_resync_request(&contract, addr),
            "first ResyncRequest for a fresh (contract, peer) must be allowed"
        );
        // Immediate repeat within the window → throttled.
        assert!(
            !manager.should_send_resync_request(&contract, addr),
            "a second ResyncRequest within RESYNC_REQUEST_MIN_INTERVAL must be throttled"
        );

        // A DIFFERENT peer for the same contract is an independent bucket.
        let other_addr: SocketAddr = "127.0.0.1:5002".parse().unwrap();
        assert!(
            manager.should_send_resync_request(&contract, other_addr),
            "a distinct peer must not share the first peer's throttle bucket"
        );
        // A DIFFERENT contract for the same peer is also independent.
        let other_contract = make_contract_key(2);
        assert!(
            manager.should_send_resync_request(&other_contract, addr),
            "a distinct contract must not share the first contract's throttle bucket"
        );

        // Just before the interval elapses → still throttled.
        time.advance_time(RESYNC_REQUEST_MIN_INTERVAL - Duration::from_millis(1));
        assert!(
            !manager.should_send_resync_request(&contract, addr),
            "ResyncRequest must stay throttled until the full interval elapses"
        );
        // After the interval elapses → allowed again.
        time.advance_time(Duration::from_millis(2));
        assert!(
            manager.should_send_resync_request(&contract, addr),
            "ResyncRequest must be allowed again once RESYNC_REQUEST_MIN_INTERVAL has elapsed"
        );
    }

    #[test]
    fn test_local_interest_tracking() {
        let (manager, _time) = make_manager();
        let contract = make_contract_key(1);

        // Initially no interest
        assert!(!manager.has_local_interest(&contract));

        // Add hosting interest
        manager.with_local_interest(&contract, |interest| {
            interest.set_hosting(true);
        });
        assert!(manager.has_local_interest(&contract));

        // Add client interest
        manager.with_local_interest(&contract, |interest| {
            interest.add_client();
        });
        assert!(manager.has_local_interest(&contract));

        // Remove hosting - still interested due to client
        manager.with_local_interest(&contract, |interest| {
            interest.set_hosting(false);
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
        manager.register_local_hosting(&contract2);

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
        assert!(
            manager
                .get_cached_delta(&contract1, &peer_summary, &our_summary)
                .is_none()
        );

        // Cache the delta for contract1
        manager.cache_delta(&contract1, &peer_summary, &our_summary, delta.clone());

        // Cache hit for contract1
        let cached = manager.get_cached_delta(&contract1, &peer_summary, &our_summary);
        assert!(cached.is_some());
        assert_eq!(cached.unwrap().as_ref(), delta.as_ref());

        // Cache miss for contract2 with same summaries (contract key isolates cache entries)
        assert!(
            manager
                .get_cached_delta(&contract2, &peer_summary, &our_summary)
                .is_none()
        );
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
        manager.with_local_interest(&contract1, |i| i.set_hosting(true));

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

        // Register local interest in contracts 1 and 2 (using set_hosting which indexes)
        manager.register_local_hosting(&contract1);
        manager.register_local_hosting(&contract2);

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

        // Setup: A is interested in contracts 1, 2 (using set_hosting which indexes)
        manager_a.register_local_hosting(&contract1);
        manager_a.register_local_hosting(&contract2);

        // Setup: B is interested in contracts 2, 3 (using set_hosting which indexes)
        manager_b.register_local_hosting(&contract2);
        manager_b.register_local_hosting(&contract3);

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
        assert!(
            manager_b
                .get_interested_peers(&contract2)
                .iter()
                .any(|(pk, _)| pk == &peer_a)
        );

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

        // Setup: local interest in contract1 (using set_hosting which indexes)
        manager.register_local_hosting(&contract1);

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
        assert!(
            manager
                .get_interested_peers(&contract1)
                .iter()
                .any(|(pk, _)| pk == &peer)
        );
        // contract2 wasn't registered because we don't have local interest
        assert!(
            !manager
                .get_interested_peers(&contract2)
                .iter()
                .any(|(pk, _)| pk == &peer)
        );

        // Later: peer removes interest in contract1
        let removed_hashes = vec![hash1];
        for hash in &removed_hashes {
            for contract in manager.lookup_by_hash(*hash) {
                manager.remove_peer_interest(&contract, &peer);
            }
        }

        // Verify peer is no longer interested
        assert!(
            !manager
                .get_interested_peers(&contract1)
                .iter()
                .any(|(pk, _)| pk == &peer)
        );
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
        assert!(
            manager
                .get_interested_peers(&contract)
                .iter()
                .any(|(pk, _)| pk == &peer)
        );
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
        manager_a.register_local_hosting(&contract);
        manager_b.register_local_hosting(&contract);

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
        assert!(
            manager_a
                .get_interested_peers(&contract)
                .iter()
                .any(|(pk, _)| pk == &peer_b)
        );
        assert!(
            manager_b
                .get_interested_peers(&contract)
                .iter()
                .any(|(pk, _)| pk == &peer_a)
        );
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

        // Register local hosting to index the contract
        manager.register_local_hosting(&contract);

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
        manager.register_local_hosting(&contract);
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
        manager.register_local_hosting(&contract1);
        manager.register_local_hosting(&contract2);
        manager.register_local_hosting(&contract3);

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

    /// Primitive-contract pin underpinning the D2 clobber fix in the
    /// `ChangeInterests` interest-sync handler (`node.rs`): a peer that is
    /// already our UPSTREAM host (`is_upstream = true`, set when we subscribed
    /// through it) must not be downgraded to a plain downstream interest when it
    /// re-advertises interest via a `ChangeInterests { added }` gossip. (That
    /// gossip is EVENT-DRIVEN — emitted only on a 0->1 interest transition, not
    /// on the ~5-min heartbeat, which sends the already-guarded `Interests`
    /// full-replace arm.)
    ///
    /// `register_peer_interest(.., is_upstream = false)` overwrites the whole
    /// `PeerInterest`, flipping `is_upstream` true -> false and wiping the
    /// cached delta-sync summary to `None`. The handler therefore guards the
    /// re-registration of an EXISTING entry with
    /// `get_peer_interest().is_some() -> refresh_peer_interest()`, which
    /// preserves both.
    ///
    /// SCOPE: this exercises the InterestManager PRIMITIVES directly, so it is a
    /// characterization of the contract the guard relies on — it PASSES on the
    /// pre-fix handler too (the bug was the handler calling the wrong
    /// primitive, not a primitive misbehaving). The handler-WIRING regression
    /// signal — the test that FAILS on the pre-fix unguarded arm — is
    /// `change_interests_arm_guards_register_with_refresh_pin` in `node.rs`.
    /// What this pins:
    ///   1. `refresh` PRESERVES `is_upstream` + summary, so
    ///      `send_unsubscribe_upstream`'s lookup
    ///      (`get_interested_peers().find(|i| i.is_upstream)`) still resolves,
    ///      keeping event-driven chain collapse working; and
    ///   2. a bare `register(false)` CLOBBERS both — the failure mode the
    ///      `ChangeInterests` handler exhibited before the guard was added.
    #[test]
    fn upstream_interest_survives_refresh_but_bare_register_clobbers_it() {
        let (manager, _time) = make_manager();
        let contract = make_contract_key(1);
        let upstream = make_peer_key(1);
        let summary = StateSummary::from(vec![1u8, 2, 3]);

        // We subscribed through `upstream`, so it is registered as our upstream
        // host with a cached delta-sync summary.
        manager.register_peer_interest(&contract, upstream.clone(), Some(summary.clone()), true);
        let before = manager.get_peer_interest(&contract, &upstream).unwrap();
        assert!(before.is_upstream);
        assert_eq!(before.summary.as_ref(), Some(&summary));

        // FIXED handler path: an existing entry is refreshed, not
        // re-registered. Refresh preserves is_upstream AND the cached summary.
        manager.refresh_peer_interest(&contract, &upstream);
        let after_refresh = manager.get_peer_interest(&contract, &upstream).unwrap();
        assert!(
            after_refresh.is_upstream,
            "refresh must preserve is_upstream so send_unsubscribe_upstream can \
             still find the upstream"
        );
        assert_eq!(
            after_refresh.summary.as_ref(),
            Some(&summary),
            "refresh must preserve the cached delta-sync summary"
        );

        // `send_unsubscribe_upstream` locates the upstream exactly this way;
        // assert it still resolves after the heartbeat refresh.
        let found = manager
            .get_interested_peers(&contract)
            .into_iter()
            .find(|(_, i)| i.is_upstream)
            .map(|(p, _)| p);
        assert_eq!(
            found,
            Some(upstream.clone()),
            "the upstream lookup used by send_unsubscribe_upstream must still \
             find the peer after a refresh"
        );

        // BUG path (the pre-fix unguarded `ChangeInterests` handler): a bare
        // register(false) overwrites the entry, clobbering BOTH fields.
        manager.register_peer_interest(&contract, upstream.clone(), None, false);
        let clobbered = manager.get_peer_interest(&contract, &upstream).unwrap();
        assert!(
            !clobbered.is_upstream,
            "documents the clobber: a bare register(false) flips is_upstream \
             true -> false"
        );
        assert!(
            clobbered.summary.is_none(),
            "documents the clobber: a bare register(false) wipes the cached summary"
        );
        assert!(
            manager
                .get_interested_peers(&contract)
                .into_iter()
                .all(|(_, i)| !i.is_upstream),
            "after the clobber, send_unsubscribe_upstream can no longer find any \
             upstream — the event-driven-collapse defeat this fix prevents"
        );
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

    /// `active_demand_count()` counts only contracts backed by REAL demand — a
    /// local client subscription or a downstream subscriber — and EXCLUDES
    /// cache-only `hosting` interest. This is the denominator for the #3763
    /// no-storm invariant, so the exclusion of hosting-only contracts is the
    /// load-bearing behavior and is asserted directly here (not just logged in
    /// the sim harness).
    #[test]
    fn test_active_demand_count_excludes_cache_only_hosting() {
        let (manager, _time) = make_manager();

        assert_eq!(manager.active_demand_count(), 0, "empty manager → 0 demand");

        // Cache-only hosting (no client, no downstream) is interest but NOT demand.
        let hosting_only = make_contract_key(1);
        manager.register_local_hosting(&hosting_only);
        assert!(
            manager.has_local_interest(&hosting_only),
            "register_local_hosting creates local interest"
        );
        assert_eq!(
            manager.active_demand_count(),
            0,
            "a hosting-only contract is interest but must NOT count as active demand"
        );

        // A local client subscription IS demand.
        let client = make_contract_key(2);
        manager.add_local_client(&client);
        assert_eq!(
            manager.active_demand_count(),
            1,
            "a local client subscription is active demand"
        );

        // A downstream subscriber IS demand.
        let downstream = make_contract_key(3);
        manager.add_downstream_subscriber(&downstream);
        assert_eq!(
            manager.active_demand_count(),
            2,
            "a downstream subscriber is active demand"
        );

        // Adding cache-only hosting on top of the client-demand contract must
        // neither double-count it nor change the total.
        manager.register_local_hosting(&client);
        assert_eq!(
            manager.active_demand_count(),
            2,
            "hosting layered on top of an already-demanded contract does not change the count"
        );

        // The hosting-only contract is genuinely tracked as interest — the
        // point is that interest (which includes hosting) and demand (which
        // does not) are distinct: it is interested but excluded from demand.
        assert!(
            manager.has_local_interest(&hosting_only),
            "the hosting-only contract is still tracked as local interest"
        );
        assert_eq!(
            manager.active_demand_count(),
            2,
            "...yet it is still excluded from the active-demand count"
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

    /// Verify that summary mismatch detection correctly identifies stale peers
    /// and that only the specific stale peer needs updating (not all subscribers).
    ///
    /// Regression test for #3791: summary mismatch triggered BroadcastStateChange
    /// to ALL subscribers instead of SyncStateToPeer to just the stale peer,
    /// causing O(peers^2) broadcast storms.
    #[test]
    fn test_summary_mismatch_targets_only_stale_peer() {
        let (manager, _time) = make_manager();

        let contract = make_contract_key(1);
        let peer_a = make_peer_key(1);
        let peer_b = make_peer_key(2);
        let peer_c = make_peer_key(3);

        manager.register_local_hosting(&contract);

        // Our state summary
        let our_summary = StateSummary::from(vec![1, 2, 3]);

        // Peer A and C have our current summary (up to date)
        manager.register_peer_interest(&contract, peer_a.clone(), Some(our_summary.clone()), false);
        manager.register_peer_interest(&contract, peer_c.clone(), Some(our_summary.clone()), false);

        // Peer B has an old summary (stale)
        let stale_summary = StateSummary::from(vec![0, 0, 0]);
        manager.register_peer_interest(
            &contract,
            peer_b.clone(),
            Some(stale_summary.clone()),
            false,
        );

        // Use the same stale-detection logic as production (node.rs):
        // zip both Option<StateSummary> and compare bytes.
        let peer_b_summary = manager.get_peer_summary(&contract, &peer_b);
        let is_stale = Some(&our_summary)
            .zip(peer_b_summary.as_ref())
            .is_some_and(|(ours, theirs)| ours.as_ref() != theirs.as_ref());
        assert!(is_stale, "Peer B should be detected as stale");

        // Peers A and C have our current summary and should NOT be stale
        for (label, peer) in [("A", &peer_a), ("C", &peer_c)] {
            let summary = manager.get_peer_summary(&contract, peer);
            let stale = Some(&our_summary)
                .zip(summary.as_ref())
                .is_some_and(|(ours, theirs)| ours.as_ref() != theirs.as_ref());
            assert!(!stale, "Peer {label} should NOT be stale");
        }

        // The fix (#3791): only peer B needs a state sync, not all 3 peers.
        // Before the fix, BroadcastStateChange would send to all 3 peers.
        // After the fix, SyncStateToPeer sends only to peer B.
        let interested_peers = manager.get_interested_peers(&contract);
        assert_eq!(
            interested_peers.len(),
            3,
            "All 3 peers should be interested"
        );

        // Count how many peers actually need syncing
        let stale_count = interested_peers
            .iter()
            .filter(|(pk, _)| {
                let summary = manager.get_peer_summary(&contract, pk);
                summary
                    .as_ref()
                    .map(|s| s.as_ref() != our_summary.as_ref())
                    .unwrap_or(false)
            })
            .count();
        assert_eq!(
            stale_count,
            1,
            "Only 1 peer (B) should need syncing, not all {}",
            interested_peers.len()
        );
    }

    /// Regression test for the PR #4129 add-then-index race in
    /// `InterestManager`.
    ///
    /// Before the fix, `add_local_client` / `register_local_hosting` /
    /// `add_downstream_subscriber` / `register_peer_interest` /
    /// `register_local_interest` released the `local_interests` (or
    /// `interested_peers`) shard guard before calling
    /// `index_contract_hash`. A concurrent `remove_*` for the same
    /// contract could then acquire the guard, decrement the last reason,
    /// run `cleanup_contract_if_no_interest` → `unindex_contract_hash`
    /// (a no-op because we haven't indexed yet), and the deferred index
    /// would leak a zombie entry into `contract_hash_index`.
    ///
    /// Two properties make this race awkward to test:
    ///
    /// 1. The zombie only PERSISTS if the contract sees no further
    ///    activity — a later add re-establishes backing interest, a
    ///    later remove's cleanup unindexes it. A stress test that
    ///    hammers one shared contract therefore continuously heals it.
    /// 2. The racy window (`local_interests` guard drop → deferred
    ///    `index_contract_hash`) is a handful of instructions wide.
    ///
    /// This test addresses both: each ROUND uses a fresh contract and
    /// runs exactly ONE add racing exactly ONE remove. A barrier
    /// releases the adder and remover simultaneously to maximize
    /// overlap. Because there is only one add and one remove, nothing
    /// can heal a zombie once created — it persists to the post-round
    /// check, which reads the three maps directly and calls no
    /// `remove_*` (which would trigger cleanup and heal it).
    ///
    /// Each of the four real add/remove PAIRS is exercised round-robin:
    /// `register_peer_interest`/`remove_peer_interest`,
    /// `register_local_hosting`/`unregister_local_hosting`,
    /// `add_local_client`/`remove_local_client`,
    /// `add_downstream_subscriber`/`remove_downstream_subscriber`. The
    /// fifth fixed site, `register_local_interest`, gets the same
    /// lock-across-index discipline but is NOT raced here: it is dead
    /// code (no workspace caller) with no symmetric remove operation, so
    /// there is no natural pair to race it against. It is structurally
    /// identical to the tested `register_local_hosting` and is guarded
    /// by code review plus the `.claude/rules/ring.md` rule entry.
    ///
    /// The fix holds the shard guard across `index_contract_hash`, so
    /// the racy interleaving cannot occur and no round produces a
    /// zombie.
    #[test]
    fn test_concurrent_add_remove_preserves_hash_index_invariant() {
        use std::sync::{Arc, Barrier, Mutex};
        use std::thread;

        let (manager, _time) = make_manager();
        let manager = Arc::new(manager);

        let rounds: u32 = 120_000;

        // Per-round spec shared with the two worker threads:
        // (contract, which-pair, stop-sentinel).
        let spec: Arc<Mutex<(ContractKey, u32, bool)>> =
            Arc::new(Mutex::new((make_unique_contract_key(0), 0, false)));
        // 3 parties: adder, remover, main.
        let round_start = Arc::new(Barrier::new(3));
        let round_end = Arc::new(Barrier::new(3));

        // Single shared peer key for the peer-interest pair. The remover
        // drains by enumerating `interested_peers` so it needs no key.
        let peer = make_peer_key(0);

        let adder = {
            let manager = Arc::clone(&manager);
            let spec = Arc::clone(&spec);
            let round_start = Arc::clone(&round_start);
            let round_end = Arc::clone(&round_end);
            let peer = peer.clone();
            thread::spawn(move || {
                loop {
                    round_start.wait();
                    let (contract, which, stop) = *spec.lock().unwrap();
                    if stop {
                        break;
                    }
                    match which {
                        0 => {
                            manager.register_peer_interest(&contract, peer.clone(), None, false);
                        }
                        1 => {
                            manager.register_local_hosting(&contract);
                        }
                        2 => {
                            manager.add_local_client(&contract);
                        }
                        _ => {
                            manager.add_downstream_subscriber(&contract);
                        }
                    }
                    round_end.wait();
                }
            })
        };

        let remover = {
            let manager = Arc::clone(&manager);
            let spec = Arc::clone(&spec);
            let round_start = Arc::clone(&round_start);
            let round_end = Arc::clone(&round_end);
            thread::spawn(move || {
                loop {
                    round_start.wait();
                    let (contract, which, stop) = *spec.lock().unwrap();
                    if stop {
                        break;
                    }
                    match which {
                        0 => {
                            let peers: Vec<PeerKey> = manager
                                .interested_peers
                                .get(&contract)
                                .map(|e| e.keys().cloned().collect())
                                .unwrap_or_default();
                            for p in peers {
                                manager.remove_peer_interest(&contract, &p);
                            }
                        }
                        1 => {
                            manager.unregister_local_hosting(&contract);
                        }
                        2 => {
                            manager.remove_local_client(&contract);
                        }
                        _ => {
                            manager.remove_downstream_subscriber(&contract);
                        }
                    }
                    round_end.wait();
                }
            })
        };

        let mut zombies: Vec<(u32, u32)> = Vec::new();
        for round in 0..rounds {
            let contract = make_unique_contract_key(round);
            let which = round % 4;
            *spec.lock().unwrap() = (contract, which, false);

            round_start.wait(); // release adder + remover simultaneously
            round_end.wait(); // both have completed their single op

            // Activity on `contract` has fully stopped — exactly one add
            // and one remove ran, nothing can heal a zombie now. Check
            // the shard-consistency invariant directly, calling no
            // `remove_*` (which would trigger cleanup). A zombie =
            // indexed in `contract_hash_index`, absent from BOTH
            // `local_interests` and `interested_peers`.
            // `lookup_by_hash` returns every contract sharing the 32-bit
            // hash, so check membership of THIS contract specifically —
            // `!is_empty()` would false-positive on a hash collision.
            let in_chi = manager
                .lookup_by_hash(contract_hash(&contract))
                .contains(&contract);
            let in_li = manager.local_interests.contains_key(&contract);
            let in_ip = manager.interested_peers.contains_key(&contract);
            if in_chi && !in_li && !in_ip {
                zombies.push((round, which));
            }
        }

        // Signal both workers to exit, then release them off round_start.
        *spec.lock().unwrap() = (make_unique_contract_key(0), 0, true);
        round_start.wait();
        adder.join().unwrap();
        remover.join().unwrap();

        assert!(
            zombies.is_empty(),
            "{} of {rounds} single-add/single-remove rounds leaked a \
             zombie entry into contract_hash_index (no backing \
             local_interests or interested_peers). This is the PR #4129 \
             race that PR #4171 fixes. First offenders (round, pair): \
             {:?}",
            zombies.len(),
            &zombies[..zombies.len().min(10)]
        );
    }

    /// Regression test for issue #4174: `remove_all_peer_interests` must
    /// preserve the bidirectional invariant
    /// `peer ∈ peer_contracts[peer] ⇔ peer ∈ interested_peers[contract]`
    /// when racing against a concurrent `register_peer_interest`.
    ///
    /// The bug: the old `remove_all_peer_interests` removed the
    /// `peer_contracts[peer]` entry up front, captured a snapshot of the
    /// contract set, then iterated that snapshot and mutated
    /// `interested_peers` directly. A concurrent
    /// `register_peer_interest(C, peer, ..)` for a contract `C` that is
    /// already in the snapshot — running in the window AFTER the up-front
    /// `peer_contracts.remove` but BEFORE the per-contract
    /// `interested_peers[C]` mutation — re-inserts `peer` into BOTH maps.
    /// `remove_all_peer_interests` then strips `peer` from
    /// `interested_peers[C]` (it still has `C` in its stale snapshot) but
    /// the freshly-re-created reverse entry in `peer_contracts[peer]`
    /// survives — leaving a one-sided "ghost": `peer ∈ peer_contracts`
    /// while `peer ∉ interested_peers[C]`.
    ///
    /// The fix delegates per-contract cleanup to `remove_peer_interest`,
    /// which holds the `interested_peers[contract]` shard guard across
    /// the `peer_contracts` update so each removal is atomic against a
    /// concurrent `register_peer_interest`.
    ///
    /// Test design (mirrors
    /// `test_concurrent_add_remove_preserves_hash_index_invariant`):
    /// barrier-synced rounds with a fresh contract per round. CRITICAL —
    /// to reproduce the race the contract must already be in the peer's
    /// `peer_contracts` set when `remove_all_peer_interests` snapshots
    /// it, so each round PRE-REGISTERS the contract on the main thread
    /// before opening the barrier. The two workers then race a
    /// re-`register_peer_interest` (refresh) of that already-registered
    /// contract against `remove_all_peer_interests`. After each round
    /// all activity on the contract has stopped, so the bidirectional
    /// invariant must hold regardless of interleaving — any violation is
    /// a real ghost left behind by the race.
    ///
    /// Sensitivity: with the fix reverted to the racy body, this test
    /// caught the race in 10/10 runs of 200_000 rounds each (the
    /// pre-registration is what makes it reliable — without it the
    /// snapshot never contains the raced contract and the test cannot
    /// see the bug). With the fix applied it passes 10/10.
    #[test]
    fn test_concurrent_remove_all_preserves_bidirectional_invariant() {
        use std::sync::{Arc, Barrier, Mutex};
        use std::thread;

        let (manager, _time) = make_manager();
        let manager = Arc::new(manager);

        let rounds: u32 = 200_000;

        // Per-round spec shared with the two worker threads:
        // (contract, stop-sentinel).
        let spec: Arc<Mutex<(ContractKey, bool)>> =
            Arc::new(Mutex::new((make_unique_contract_key(0), false)));
        // 3 parties: registrar, remover, main.
        let round_start = Arc::new(Barrier::new(3));
        let round_end = Arc::new(Barrier::new(3));

        // Single shared peer key raced across every round.
        let peer = make_peer_key(0);

        // Registrar: re-registers (refreshes) the peer's interest in the
        // round's contract — which the main thread has already
        // registered before the barrier opened.
        let registrar = {
            let manager = Arc::clone(&manager);
            let spec = Arc::clone(&spec);
            let round_start = Arc::clone(&round_start);
            let round_end = Arc::clone(&round_end);
            let peer = peer.clone();
            thread::spawn(move || {
                loop {
                    round_start.wait();
                    let (contract, stop) = *spec.lock().unwrap();
                    if stop {
                        break;
                    }
                    manager.register_peer_interest(&contract, peer.clone(), None, false);
                    round_end.wait();
                }
            })
        };

        // Remover: wipes ALL of the peer's interests, racing the
        // registrar above.
        let remover = {
            let manager = Arc::clone(&manager);
            let spec = Arc::clone(&spec);
            let round_start = Arc::clone(&round_start);
            let round_end = Arc::clone(&round_end);
            let peer = peer.clone();
            thread::spawn(move || {
                loop {
                    round_start.wait();
                    let (_contract, stop) = *spec.lock().unwrap();
                    if stop {
                        break;
                    }
                    manager.remove_all_peer_interests(&peer);
                    round_end.wait();
                }
            })
        };

        let mut ghosts: Vec<u32> = Vec::new();
        for round in 0..rounds {
            let contract = make_unique_contract_key(round);

            // Pre-register the contract BEFORE opening the barrier so it
            // is guaranteed to be in `remove_all_peer_interests`'s
            // snapshot — this is what makes the #4174 race observable.
            manager.register_peer_interest(&contract, peer.clone(), None, false);

            *spec.lock().unwrap() = (contract, false);

            round_start.wait(); // release registrar + remover simultaneously
            round_end.wait(); // both have completed their single op

            // Activity on `contract` has fully stopped. Check the
            // bidirectional invariant directly, without calling any
            // `remove_*` (which would trigger cleanup and mask a
            // ghost). A ghost = `peer` present on exactly one side:
            //   peer ∈ peer_contracts[peer]  XOR  peer ∈ interested_peers[contract]
            let in_peer_contracts = manager
                .peer_contracts
                .get(&peer)
                .map(|e| e.value().contains(&contract))
                .unwrap_or(false);
            let in_interested_peers = manager
                .interested_peers
                .get(&contract)
                .map(|e| e.contains_key(&peer))
                .unwrap_or(false);
            if in_peer_contracts != in_interested_peers {
                ghosts.push(round);
            }

            // Clean slate for the next round: if the registrar won the
            // race the contract may still be registered. Drop it so the
            // peer's contract set does not grow unboundedly (which would
            // slow every later `remove_all_peer_interests` snapshot).
            manager.remove_peer_interest(&contract, &peer);
        }

        // Signal both workers to exit, then release them off round_start.
        *spec.lock().unwrap() = (make_unique_contract_key(0), true);
        round_start.wait();
        registrar.join().unwrap();
        remover.join().unwrap();

        assert!(
            ghosts.is_empty(),
            "{} of {rounds} register/remove-all rounds left a one-sided \
             ghost: `peer` present in exactly one of peer_contracts / \
             interested_peers for the round's contract. This is the \
             issue #4174 bidirectional-consistency race. First offending \
             rounds: {:?}",
            ghosts.len(),
            &ghosts[..ghosts.len().min(10)]
        );
    }
}
