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
mod demand;
mod disk_usage;
// `pub(crate)` so `ring.rs` can re-export the reconcile controller to the node
// layer for the shadow-mode wiring (keystone step-2, #4642).
pub(crate) mod reconcile;

use crate::util::backoff::{ExponentialBackoff, TrackedBackoff};
use crate::util::time_source::{DynTimeSource, InstantTimeSrc, TimeSource};
pub(crate) use cache::HostingContractScore;
/// The pre-A2 flat 1 GiB budget, used as the upgrade-migration sentinel in
/// `config::ConfigArgs::build` (see the constant's docs).
pub(crate) use cache::LEGACY_FLAT_HOSTING_BUDGET_BYTES;
/// Upper clamp re-exported only for the config-default round-trip test, which
/// asserts the resolved default lands within [MIN, MAX] without hardcoding the
/// byte values. Gated to test builds so the re-export isn't an unused import
/// under `-D warnings` in release.
#[cfg(test)]
pub(crate) use cache::MAX_DEFAULT_HOSTING_BUDGET_BYTES;
/// The shared MIN budget floor (#4683 eviction floor uses it as the disk-budget
/// clamp lower bound; the config round-trip test asserts the RAM default lands
/// within [MIN, MAX]). `pub(crate)` so `ring` can re-export it for that test.
pub(crate) use cache::MIN_DEFAULT_HOSTING_BUDGET_BYTES;
/// Re-exported as the single source of truth for the default hosting storage
/// budget. `config::default_max_hosting_storage()` resolves to this function so
/// the operator-facing default and the in-code fallback can never drift. The
/// default is RAM-scaled (capability-relative, A2) rather than a flat constant.
pub(crate) use cache::default_hosting_budget_bytes;
pub use cache::{AccessType, EvictedInUseTeardown, RecordAccessResult};
/// Cost-pressure eviction inputs + day-one calibration constants (cost-aware
/// eviction, #4861). Re-exported so `Ring` (which reads the topology meter)
/// can build the per-axis snapshots the sweep consumes; the policy constants
/// and the shared axis assembly live beside the decision in `cache.rs`.
pub(crate) use cache::{COST_RATE_MIN_WINDOW, CostAxisPressure, build_cost_axes};
/// Aggregate disk-budget sizing defaults + pure clamp math (#4683). Re-exported
/// so `config` can resolve the persisted `hosting-disk-pct` / `max-hosting-disk`
/// defaults and `ring`/`HostingManager` can size the eviction floor.
pub(crate) use cache::{DEFAULT_HOSTING_DISK_PCT, DEFAULT_MAX_HOSTING_DISK_BYTES};
use cache::{HostingCache, HostingCacheStats, disk_budget_for_clamped};
use dashmap::{DashMap, DashSet};
use demand::ProximityPrior;
use disk_usage::DiskUsageTracker;
pub(crate) use disk_usage::{DiskBudgetExceeded, DiskUsageStats};
use freenet_stdlib::prelude::{ContractInstanceId, ContractKey};
use parking_lot::RwLock;
use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;
use tokio::time::Instant;
use tracing::{debug, info};

use super::Location;
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

/// Maximum one-shot repair-fetch attempts for a phantom in-use contract
/// (`contract_in_use` via downstream subscribers, but NO stored state — the
/// #4612 transit-relay leak) before the phantom registration is dropped
/// rather than kept as a persistent stateless "host".
const MAX_PHANTOM_REPAIR_ATTEMPTS: u32 = 3;

/// Minimum spacing between repair-fetch attempts for the same phantom
/// contract. Derived from [`crate::config::OPERATION_TTL`] (the sub-op GET's
/// own per-attempt ceiling) so a retry is never issued while the previous
/// fire-and-forget fetch could still be in flight.
const PHANTOM_REPAIR_COOLDOWN: Duration = crate::config::OPERATION_TTL;

/// Absolute-age bound for the exhausted-phantom exemption (review Fix C /
/// ring.md "Cleanup Exemptions Must Be Time-Bounded"). A phantom whose
/// downstream keeps renewing (so the Fix-6 downstream-expiry prune never fires)
/// but whose state stays unfetchable past this age is dropped, so it cannot be a
/// permanent GC blind spot + advertised dead-end. Four subscription leases (~32
/// min) is well past the 8-min lease — several renewal cycles of genuine
/// repair-fetch attempts must fail first. This drop is SAFE and NON-CYCLING
/// (unlike the original #4770 Drop): a re-registration goes through
/// `finalize_host_subscribe` (fetch-first), so if the state is still unfetchable
/// it registers NOTHING and the phantom cannot re-form.
const PHANTOM_ABSOLUTE_MAX_AGE: Duration =
    Duration::from_secs(SUBSCRIPTION_LEASE_DURATION.as_secs() * 4); // ~32 minutes

// =============================================================================
// Result Types
// =============================================================================

/// Per-contract repair-fetch attempt tracking for phantom in-use contracts.
/// See [`HostingManager::reconcile_phantom_in_use`].
#[derive(Debug, Clone, Copy)]
struct PhantomRepairEntry {
    attempts: u32,
    last_attempt: Instant,
    /// When this phantom was first observed (TimeSource). Drives the
    /// absolute-age drop (`PHANTOM_ABSOLUTE_MAX_AGE`, review Fix C) — an
    /// exhausted phantom whose downstream keeps renewing is dropped once its age
    /// exceeds the bound, so the exemption is time-bounded (ring.md).
    first_seen: Instant,
}

/// Action emitted by [`HostingManager::reconcile_phantom_in_use`] for one
/// phantom (in-use, stateless) contract. The driver in
/// `Ring::recover_orphaned_subscriptions` applies it: `Fetch` spawns a
/// fire-and-forget sub-op GET (`start_sub_op_get`); `Drop` removes the
/// downstream registration (via [`HostingManager::drop_phantom_downstream`]) and
/// collapses upstream.
///
/// The `Drop` variant here is NOT the #4770 cycling drop (that was neutralized in
/// step 10 §1c). It is the review-Fix-C absolute-age-bounded drop: it fires ONLY
/// after `PHANTOM_ABSOLUTE_MAX_AGE`, and it is non-cycling because a
/// re-registration must go through `finalize_host_subscribe` (fetch-first), which
/// registers NOTHING while the state stays unfetchable — so the phantom cannot
/// re-form. It re-satisfies the ring.md time-bounded-exemption rule that the
/// pure Fix-6 downstream-expiry prune leaves open when the downstream keeps
/// renewing.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum PhantomRepair {
    /// Launch a one-shot state repair fetch for the contract.
    Fetch(ContractKey),
    /// Repair attempts exhausted AND the phantom is older than
    /// `PHANTOM_ABSOLUTE_MAX_AGE` — drop the stale downstream registration.
    Drop(ContractKey),
}

/// Result of adding a client subscription.
#[derive(Debug)]
pub struct AddClientSubscriptionResult {
    /// Whether this was the first client for this contract.
    pub is_first_client: bool,
}

/// Outcome of an `add_downstream_subscriber` call. The variant
/// distinguishes "the peer was newly added" from "the peer was already
/// tracked and this call refreshed its lease timestamp".
///
/// Governance no longer reacts to these on a per-event basis: benefit
/// is a live snapshot read each reaper tick from
/// `downstream_subscriber_count`, so a renewal merely extends a lease
/// the snapshot already counts. The variant is retained because callers
/// still distinguish accept-vs-reject and new-vs-renewal for
/// subscription bookkeeping and telemetry.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AddSubscriberOutcome {
    /// Peer is now tracked for this contract; was not before.
    NewAdd,
    /// Peer was already tracked; this call refreshed the lease
    /// timestamp.
    Renewal,
    /// Per-contract subscriber cap reached; this is a new peer that
    /// was rejected. Equivalent to the old `false` return.
    Rejected,
}

impl AddSubscriberOutcome {
    /// True when the peer is now tracked (either newly added or
    /// renewed). Preserves the pre-Sybil-fix `bool` semantics for
    /// callers that only care "was the registration accepted".
    #[cfg_attr(not(test), allow(dead_code))]
    pub fn was_accepted(self) -> bool {
        matches!(self, Self::NewAdd | Self::Renewal)
    }
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

/// Result of a hosting-cache over-budget/expiry sweep
/// ([`HostingManager::sweep_expired_hosting`]).
#[derive(Debug, Default)]
pub struct HostingSweepResult {
    /// `(ContractKey, write_generation)` pairs the caller reclaims from disk.
    pub expired: Vec<(ContractKey, u64)>,
    /// Subscription state torn down for any still-in-use victim shed as a last
    /// resort, so the caller can replay the removals against the
    /// `InterestManager` (PR #4734 Fix 1). Empty in the common (zero-subscriber)
    /// sweep.
    pub evicted_in_use_teardown: Vec<EvictedInUseTeardown>,
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
    /// Whether this node is genuinely receiving updates for this contract —
    /// the real freshness signal ([`HostingManager::is_receiving_updates`]).
    /// `is_hosting` is NOT a freshness signal (a hosting-cache copy can go
    /// stale after its subscription lapses); only an active network/client
    /// subscription guarantees the cached state is kept current (PR #3699).
    pub is_receiving_updates: bool,
    /// Whether real demand pins this contract: a local client subscription or
    /// a registered downstream subscriber ([`HostingManager::contract_in_use`]).
    /// Distinguishes contracts held for actual demand from network-only
    /// subscriptions with no local/downstream reader.
    pub in_use: bool,
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

    /// Unified hosting cache with byte-budget demand-ordered eviction ("fuel
    /// gauge") and TTL protection. This is the single source of truth for which
    /// contracts we're hosting.
    ///
    /// The cache's clock is the same injectable [`DynTimeSource`] as
    /// `time_source` below, so subscription-lease time and cache-eviction TTL
    /// share one clock. Production installs `Arc<InstantTimeSrc>`; sims can
    /// inject a controllable clock (see `with_time_source`).
    hosting_cache: RwLock<HostingCache<DynTimeSource>>,

    /// Proximity-prior demand estimator (A3, freenet/freenet-core#4642). Maps a
    /// contract's ring distance from this peer to a predicted read rate, used as
    /// the `predicted_demand` term in the demand-ordered `keep_score`. Trained
    /// from this peer's own observed read rates; see [`demand::ProximityPrior`].
    demand_estimator: RwLock<ProximityPrior>,

    /// This peer's own ring location, pushed in by `Ring` on the snapshot
    /// cadence (`set_own_location`). `None` until the node has learned its
    /// location, in which case demand falls back to neutral (eviction degrades
    /// to Greedy-Dual floor + recency). The estimator needs it to turn a
    /// contract key into a distance.
    own_location: RwLock<Option<Location>>,

    /// Local-client GET hit-rate counters (#4642 A3 instrumentation). Driven by
    /// the actual serve-vs-forward DECISION in the client GET handler
    /// (`client_events`), not by cache membership: `local_get_serves` counts
    /// client GETs answered from local hosted state; `local_get_forwards` counts
    /// those routed to the network. The collector derives the hit-rate as
    /// `serves / (serves + forwards)`. Monotonic per-node scalars.
    local_get_serves: AtomicU64,
    local_get_forwards: AtomicU64,

    /// Downstream peers subscribed to contracts we host, with lease timestamps.
    /// Drives `should_unsubscribe_upstream()` decisions.
    ///
    /// Must be kept in sync with `InterestManager::interested_peers`
    /// (see `InterestManager` docs for the dual-tracking relationship).
    downstream_subscribers: DashMap<ContractKey, HashMap<PeerKey, Instant>>,

    /// Time source for downstream subscriber lease tracking.
    ///
    /// Injectable (see `with_time_source`): production uses
    /// `Arc<InstantTimeSrc>`; sims can inject a controllable clock so TTL /
    /// eviction is deterministic. Shared (same `Arc`) with `hosting_cache`.
    time_source: DynTimeSource,

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

    /// Repair-fetch attempt tracking for phantom in-use contracts (#4612):
    /// contract is `contract_in_use` via downstream subscribers but holds NO
    /// stored state (the transit-relay leak). Keyed per contract with the
    /// attempt count and the time of the last attempt; entries are removed as
    /// soon as state appears (repair succeeded) or when the phantom is
    /// dropped after [`MAX_PHANTOM_REPAIR_ATTEMPTS`]. Level-triggered against
    /// the state store by [`Self::reconcile_phantom_in_use`], so the map is
    /// bounded by the number of concurrently-phantom contracts.
    phantom_repair: DashMap<ContractKey, PhantomRepairEntry>,

    /// Aggregate on-disk usage accounting (#4683): hosted state + WASM blobs +
    /// wasmtime compile cache. `None` until [`Self::configure_disk_tracker`]
    /// installs it with the node's real paths (done once at startup alongside
    /// `set_storage`, since `with_time_source` has no path access). Now drives
    /// both the eviction floor (`min(ram, disk)`, via
    /// [`Self::recompute_effective_budget`]) and the pre-write admission gate
    /// ([`Self::admit_state_write`]), wired in #4702. Behind an
    /// `RwLock` for the same late-binding reason as `storage`.
    disk_tracker: RwLock<Option<DiskUsageTracker>>,

    /// The RAM-derived hosting budget the manager was constructed with (#4683).
    /// Preserved separately from the live `hosting_cache.budget_bytes` because
    /// the 60s recompute OVERWRITES the cache budget with the effective floor
    /// `min(ram_budget, disk_budget)`; keeping the original RAM budget here means
    /// a later recompute (after the disk budget grows) can restore the RAM floor
    /// instead of ratcheting only downward.
    ram_budget_bytes: AtomicU64,

    /// Fraction of Freenet-reachable disk capacity (`used + free`) the disk
    /// budget is sized at (#4683). Defaults to
    /// [`cache::DEFAULT_HOSTING_DISK_PCT`]; overridden from config at startup via
    /// [`Self::configure_disk_budget`]. Stored as bits so it lives in an
    /// `AtomicU64` (the recompute reads it off the sweep task without a lock).
    disk_pct_bits: AtomicU64,

    /// Hard upper clamp (bytes) for the disk budget (#4683), the `--max-hosting-disk`
    /// override. Defaults to [`cache::DEFAULT_MAX_HOSTING_DISK_BYTES`].
    max_hosting_disk_bytes: AtomicU64,

    /// Last aggregate disk budget computed by [`Self::recompute_effective_budget`]
    /// (#4683, admission gate live since #4702). This is the *aggregate* bound the pre-write
    /// gate checks projected disk against — distinct from the effective eviction
    /// floor `min(ram, disk)` that governs the state cache. Initialised to
    /// `u64::MAX` so the gate admits everything until the first 60s recompute
    /// installs a real value (the tracker is also unseeded before then, so the
    /// gate is a no-op regardless).
    disk_budget_bytes: AtomicU64,
}

impl HostingManager {
    /// Construct a `HostingManager` on the production wall-clock time source
    /// ([`InstantTimeSrc`]). Equivalent to
    /// `with_time_source(budget_bytes, Arc::new(InstantTimeSrc::new()))`.
    pub fn new(budget_bytes: u64) -> Self {
        Self::with_time_source(budget_bytes, std::sync::Arc::new(InstantTimeSrc::new()))
    }

    /// Construct a `HostingManager` on an explicit, injectable time source.
    ///
    /// Production calls [`new`](Self::new) (wall clock). Simulation tests inject
    /// a controllable clock (e.g. `SharedMockTimeSource`) so subscription-lease
    /// expiry and hosting-cache TTL/eviction advance deterministically under
    /// test control rather than wall time. The same `Arc` drives both the
    /// downstream-lease clock and the cache clock.
    pub fn with_time_source(budget_bytes: u64, time_source: DynTimeSource) -> Self {
        let backoff_config =
            ExponentialBackoff::new(INITIAL_SUBSCRIPTION_BACKOFF, MAX_SUBSCRIPTION_BACKOFF);
        Self {
            active_subscriptions: DashMap::new(),
            client_subscriptions: DashMap::new(),
            hosting_cache: RwLock::new(HostingCache::new(budget_bytes, time_source.clone())),
            demand_estimator: RwLock::new(ProximityPrior::new()),
            own_location: RwLock::new(None),
            local_get_serves: AtomicU64::new(0),
            local_get_forwards: AtomicU64::new(0),
            downstream_subscribers: DashMap::new(),
            time_source,
            pending_subscription_requests: DashSet::new(),
            subscription_backoff: RwLock::new(TrackedBackoff::new(
                backoff_config,
                MAX_SUBSCRIPTION_BACKOFF_ENTRIES,
            )),
            storage: RwLock::new(None),
            state_generation: DashMap::new(),
            pending_reclamation: std::sync::Arc::new(DashMap::new()),
            phantom_repair: DashMap::new(),
            disk_tracker: RwLock::new(None),
            ram_budget_bytes: AtomicU64::new(budget_bytes),
            disk_pct_bits: AtomicU64::new(DEFAULT_HOSTING_DISK_PCT.to_bits()),
            max_hosting_disk_bytes: AtomicU64::new(DEFAULT_MAX_HOSTING_DISK_BYTES),
            disk_budget_bytes: AtomicU64::new(u64::MAX),
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

    /// Drop the storage reference so its redb `Database` clone is released.
    /// Called on node shutdown to help free the on-disk file lock (issue #4401).
    pub(crate) fn clear_storage(&self) {
        *self.storage.write() = None;
    }

    // =========================================================================
    // Disk-usage accounting (#4683)
    // =========================================================================

    /// Install the aggregate disk-usage tracker with the node's real paths.
    /// Called once at startup (alongside `set_storage`), since `with_time_source`
    /// has no path access. The tracker starts unseeded; the first sweep tick
    /// seeds it lazily off the hot path.
    pub(crate) fn configure_disk_tracker(
        &self,
        contracts_dir: std::path::PathBuf,
        wasmtime_cache_dir: std::path::PathBuf,
    ) {
        *self.disk_tracker.write() = Some(DiskUsageTracker::new(contracts_dir, wasmtime_cache_dir));
    }

    /// Install the operator-configured disk-budget sizing knobs (#4683): the
    /// fraction of Freenet-reachable disk capacity and the hard cap. Called once
    /// at startup alongside [`Self::configure_disk_tracker`] (the config is only
    /// reachable there). If never called, the defaults set in the ctor apply.
    pub(crate) fn configure_disk_budget(&self, disk_pct: f64, max_hosting_disk: u64) {
        self.disk_pct_bits
            .store(disk_pct.to_bits(), Ordering::Relaxed);
        self.max_hosting_disk_bytes
            .store(max_hosting_disk, Ordering::Relaxed);
    }

    /// Recompute the effective hosting-cache budget as `min(ram_budget,
    /// disk_budget)` and install it via [`HostingCache::set_budget_bytes`] (#4683,
    /// eviction floor). Run on the 60s sweep AFTER the du-walks refresh, and
    /// crucially OUTSIDE any prior cache write lock: only the O(1)
    /// `set_budget_bytes` touches the cache lock here.
    ///
    /// `available` is the free bytes on the data-dir mount, injected as a
    /// parameter (prod passes `available_bytes(dir).unwrap_or(u64::MAX)`; tests
    /// pass a fixed value) — the determinism seam. When the tracker is absent or
    /// unseeded the aggregate `used` is not yet meaningful, so this is a no-op and
    /// the cache keeps its RAM budget until the first seed.
    ///
    /// Returns the effective budget it installed (for telemetry/tests), or `None`
    /// when it was a no-op.
    pub(crate) fn recompute_effective_budget(&self, available: u64) -> Option<u64> {
        let used = {
            let guard = self.disk_tracker.read();
            let tracker = guard.as_ref()?;
            if !tracker.is_seeded() {
                return None;
            }
            tracker.total_bytes()
        };
        let pct = f64::from_bits(self.disk_pct_bits.load(Ordering::Relaxed));
        let cap = self.max_hosting_disk_bytes.load(Ordering::Relaxed);
        let ram = self.ram_budget_bytes.load(Ordering::Relaxed);
        let disk_budget =
            disk_budget_for_clamped(used, available, pct, MIN_DEFAULT_HOSTING_BUDGET_BYTES, cap);
        // Publish the aggregate disk budget for the PR-3 admission gate. The gate
        // checks projected disk against THIS value (the aggregate bound), not the
        // effective floor installed on the cache below.
        self.disk_budget_bytes.store(disk_budget, Ordering::Relaxed);
        let effective = ram.min(disk_budget);
        // O(1) under the cache write lock; the expensive du-walk already ran
        // OUTSIDE any lock before this call.
        self.hosting_cache.write().set_budget_bytes(effective);
        Some(effective)
    }

    /// Pre-write admission gate for a state write (#4683, live since #4702): reject the write
    /// if replacing `key`'s tracked state size with `new_size` would push
    /// aggregate on-disk usage past the current disk budget. Read-only; the
    /// `+delta` is applied later by [`Self::record_state_write`] on the post-write
    /// success path.
    ///
    /// No-op admit (`Ok`) when the tracker is absent or unseeded: before the first
    /// 60s recompute the aggregate is not yet meaningful and `disk_budget_bytes`
    /// is `u64::MAX`, so early-startup writes are never spuriously rejected.
    pub(crate) fn admit_state_write(
        &self,
        key: &ContractKey,
        new_size: u64,
    ) -> Result<(), DiskBudgetExceeded> {
        let guard = self.disk_tracker.read();
        let Some(tracker) = guard.as_ref() else {
            return Ok(());
        };
        if !tracker.is_seeded() {
            return Ok(());
        }
        let budget = self.disk_budget_bytes.load(Ordering::Relaxed);
        tracker.admit_state_write(key, new_size, budget)
    }

    /// Pre-write admission gate for a state **UPDATE** to an already-hosted
    /// contract (#4683 growth-only rule, live since #4702). Unlike [`Self::admit_state_write`]
    /// (fresh PUT), a shrinking or size-holding UPDATE (`new_size <= old`) is
    /// admitted unconditionally — even when the aggregate is over budget —
    /// because an UPDATE mutates an already-counted footprint and rejecting it
    /// would stall CRDT convergence without freeing any bytes. Only genuine
    /// growth is subjected to the aggregate bound. No-op admit when the tracker
    /// is absent or unseeded, same as [`Self::admit_state_write`].
    pub(crate) fn admit_state_update(
        &self,
        key: &ContractKey,
        new_size: u64,
    ) -> Result<(), DiskBudgetExceeded> {
        let guard = self.disk_tracker.read();
        let Some(tracker) = guard.as_ref() else {
            return Ok(());
        };
        if !tracker.is_seeded() {
            return Ok(());
        }
        let budget = self.disk_budget_bytes.load(Ordering::Relaxed);
        tracker.admit_state_update(key, new_size, budget)
    }

    /// Pre-write admission gate for a newly-stored (deduped) WASM code blob
    /// (#4683, live since #4702): reject if charging `blob_len` on top of current aggregate
    /// usage would exceed the disk budget. Caller invokes this ONLY for a blob
    /// that is not already on disk (a re-PUT of existing code adds nothing).
    /// No-op admit when the tracker is absent or unseeded, same as
    /// [`Self::admit_state_write`].
    pub(crate) fn admit_wasm_write(&self, blob_len: u64) -> Result<(), DiskBudgetExceeded> {
        let guard = self.disk_tracker.read();
        let Some(tracker) = guard.as_ref() else {
            return Ok(());
        };
        if !tracker.is_seeded() {
            return Ok(());
        }
        let budget = self.disk_budget_bytes.load(Ordering::Relaxed);
        tracker.admit_wasm_write(blob_len, budget)
    }

    /// The current aggregate disk budget the admission gate checks against
    /// (#4683). `u64::MAX` until the first 60s recompute installs a real value.
    ///
    /// Also read by the dashboard's "Demand-driven eviction" panel
    /// (`Ring::dashboard_hosting_snapshot`, follow-up to #4683/#4702) to
    /// surface disk usage next to the RAM budget: `u64::MAX` maps to `None`
    /// there so an unseeded/not-yet-recomputed budget reads as "measuring…"
    /// instead of a nonsensical astronomical byte count.
    pub(crate) fn disk_budget_bytes(&self) -> u64 {
        self.disk_budget_bytes.load(Ordering::Relaxed)
    }

    /// Lazily seed the disk tracker on first use (like the hosting-cache
    /// `seed_if_absent`), summing the true on-disk state total from redb rows.
    /// No-op if the tracker is absent or already seeded. Runs OUTSIDE any cache
    /// write lock (the caller — the 60s sweep — invokes it off the hot path).
    ///
    /// The state seed is intentionally fail-loud on I/O error (a too-low seed
    /// would defeat the admission gate): a read failure leaves the
    /// tracker UNSEEDED so the next tick retries, rather than committing an
    /// under-count.
    #[cfg(feature = "redb")]
    pub(crate) fn seed_disk_tracker_if_absent(&self) {
        use freenet_stdlib::prelude::{CodeHash, ContractInstanceId, ContractKey};

        {
            let guard = self.disk_tracker.read();
            match guard.as_ref() {
                Some(t) if t.is_seeded() => return,
                Some(_) => {}
                None => return,
            }
        }

        // Read the exact per-contract state sizes from redb before taking the
        // tracker lock, so no storage I/O happens under it.
        let rows = {
            let storage = self.storage.read();
            let Some(storage) = storage.as_ref() else {
                return;
            };
            match storage.load_all_hosting_metadata() {
                Ok(entries) => entries,
                Err(e) => {
                    // Fail-loud: leave the tracker unseeded so the next sweep
                    // retries rather than seeding from an under-count.
                    tracing::warn!(error = %e, "disk tracker seed deferred: failed to read hosting metadata");
                    return;
                }
            }
        };

        let state_rows = rows.into_iter().filter_map(|(key_bytes, metadata)| {
            if key_bytes.len() != 32 {
                return None;
            }
            let mut id = [0u8; 32];
            id.copy_from_slice(&key_bytes);
            let key = ContractKey::from_id_and_code(
                ContractInstanceId::new(id),
                CodeHash::new(metadata.code_hash),
            );
            Some((key, metadata.size_bytes))
        });

        if let Some(tracker) = self.disk_tracker.read().as_ref() {
            tracker.seed(state_rows);
        }
    }

    /// Apply a state-write delta to the disk tracker at a state-write chokepoint.
    /// Wired from `Ring::commit_state_write` (the single infallible post-write
    /// funnel for all four executor chokepoints + the V2 delegate callback), so
    /// the counter only moves after the bytes actually landed.
    ///
    /// Calls through even when the tracker is not yet seeded (only skipping when
    /// absent). An unseeded write buffers its post-write size into the tracker's
    /// per-key map so the lazy seed picks it up instead of dropping it — this is
    /// what closes the seed/write TOCTOU (a write racing the first sweep tick's
    /// redb snapshot would otherwise permanently under-count that key). See
    /// [`DiskUsageTracker::record_state_write`].
    pub(crate) fn record_state_write(&self, key: &ContractKey, new_size: u64) {
        if let Some(tracker) = self.disk_tracker.read().as_ref() {
            tracker.record_state_write(key, new_size);
        }
    }

    /// Subtract a contract's state contribution from the disk tracker on
    /// eviction/reclamation. Wired from the reclaim path. No-op when the tracker
    /// is absent. Like [`Self::record_state_write`], calls through even while
    /// unseeded so the per-key buffer stays consistent with the seed.
    pub(crate) fn record_state_removed(&self, key: &ContractKey) {
        if let Some(tracker) = self.disk_tracker.read().as_ref() {
            tracker.record_state_removed(key);
        }
    }

    /// Charge a newly-written (deduped) WASM code blob to the disk tracker on the
    /// post-store success path (#4683). Live-accounts wasm between 60s du-walks
    /// so a burst of distinct-code PUTs can't overrun the budget within a sweep
    /// window, and so the state gate on the same PUT sees the wasm just stored.
    /// No-op when the tracker is absent. Caller charges exactly once per
    /// newly-written blob (dedup handled at the call site via `fetch_contract_code`).
    pub(crate) fn record_wasm_write(&self, blob_len: u64) {
        if let Some(tracker) = self.disk_tracker.read().as_ref() {
            tracker.record_wasm_write(blob_len);
        }
    }

    /// Subtract a WASM code blob's contribution from the disk tracker on contract
    /// removal (#4683). Mirror of [`Self::record_wasm_write`]. No-op when the
    /// tracker is absent.
    pub(crate) fn record_wasm_removed(&self, blob_len: u64) {
        if let Some(tracker) = self.disk_tracker.read().as_ref() {
            tracker.record_wasm_removed(blob_len);
        }
    }

    /// Free bytes on the data-dir mount (the `available` term the disk budget
    /// sizes against, #4683). `None` when the tracker is absent or the platform
    /// query fails; the sweep caller falls back to `u64::MAX`.
    pub(crate) fn disk_available_bytes(&self) -> Option<u64> {
        self.disk_tracker.read().as_ref()?.available_bytes()
    }

    /// Re-walk the WASM-blob and compile-cache totals. Called on the telemetry
    /// cadence (the 60s sweep) since both are `du`-measured, not delta-tracked.
    pub(crate) fn refresh_disk_usage(&self) {
        if let Some(tracker) = self.disk_tracker.read().as_ref() {
            tracker.refresh_wasm();
            tracker.refresh_compile_cache();
        }
    }

    /// Snapshot the disk-usage gauges for per-node telemetry, or `None` before
    /// the tracker is configured/seeded (early startup). Aggregate scalars only.
    pub(crate) fn disk_usage_stats(&self) -> Option<DiskUsageStats> {
        let guard = self.disk_tracker.read();
        let tracker = guard.as_ref()?;
        if !tracker.is_seeded() {
            return None;
        }
        Some(tracker.stats())
    }

    /// Test-only: install a disk tracker on nonexistent paths (so the du-walks
    /// contribute 0 and the free-space read returns `None`) and seed its state
    /// counter from `rows`. Lets `recompute_effective_budget` be driven with a
    /// known `used` and an injected `available`, no filesystem required.
    #[cfg(test)]
    pub(crate) fn seed_disk_tracker_for_test<I>(&self, rows: I)
    where
        I: IntoIterator<Item = (ContractKey, u64)>,
    {
        self.configure_disk_tracker(
            std::path::PathBuf::from("/nonexistent/contracts"),
            std::path::PathBuf::from("/nonexistent/cache"),
        );
        if let Some(tracker) = self.disk_tracker.read().as_ref() {
            tracker.seed(rows);
        }
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

    /// Up to `max_matches` currently-subscribed (non-expired) `ContractKey`s
    /// whose instance-id is in `wanted`, examining AT MOST `scan_cap` active
    /// subscription entries.
    ///
    /// Single pass, UNSORTED, no full-set materialization — unlike
    /// [`Self::get_subscribed_contracts`] (which allocates the whole set and
    /// sorts it, O(S log S)). The reconcile connection-drop shadow (keystone
    /// step-2, #4642) uses this so one co-host disconnect can't burst into an
    /// O(S log S) scan on a near-key gateway with a large subscribed set: BOTH
    /// the scan (`scan_cap`) and the match/build count (`max_matches`) are
    /// hard-bounded. A subscribed set larger than `scan_cap` is SAMPLED rather
    /// than fully enumerated — an acceptable slight undercount for a one-sided
    /// diagnostic counter (DashMap iteration order is unspecified, so the sample
    /// is arbitrary, not adversarially controllable). Returning the `ContractKey`
    /// directly also avoids reconstructing it from the reverse index's bare
    /// `ContractInstanceId` (no instance-id → `ContractKey` index exists in this
    /// crate — every resolver scans, as the comment at `ring.rs` ~895 notes).
    pub(crate) fn subscribed_keys_in(
        &self,
        wanted: &HashSet<ContractInstanceId>,
        scan_cap: usize,
        max_matches: usize,
    ) -> Vec<ContractKey> {
        let now = self.time_source.now();
        let mut out = Vec::new();
        // `.take(scan_cap)` bounds the entries EXAMINED (the scan); the inner
        // break bounds the MATCHES kept (the expensive build count downstream).
        for entry in self.active_subscriptions.iter().take(scan_cap) {
            if out.len() >= max_matches {
                break;
            }
            if entry.value().expires_at > now && wanted.contains(entry.key().id()) {
                out.push(*entry.key());
            }
        }
        out
    }

    /// Snapshot of every active subscription for the local-peer dashboard.
    ///
    /// Reads directly from the canonical lease map (no parallel
    /// mirror). The earlier `network_status::subscribed_contracts`
    /// mirror silently drifted when SUBSCRIBE migrated to its driver
    /// and lost its recording hook.
    pub fn dashboard_subscription_snapshot(&self) -> Vec<SubscribedContractSnapshot> {
        let now = self.time_source.now();
        // Phase 1: collect the lease data while iterating `active_subscriptions`.
        // Do NOT call `is_receiving_updates`/`is_subscribed` in here — they
        // re-`.get()` `active_subscriptions`, which would deadlock against the
        // shard guard this `.iter()` holds when the key hashes to the same
        // shard. Compute the freshness/demand booleans in phase 2, after the
        // iterator's guards are released.
        let leases: Vec<(ContractKey, u64, Option<u64>)> = self
            .active_subscriptions
            .iter()
            .filter(|entry| entry.value().expires_at > now)
            .map(|entry| {
                let lease = *entry.value();
                (
                    *entry.key(),
                    now.saturating_duration_since(lease.subscribed_since)
                        .as_secs(),
                    lease
                        .last_updated
                        .map(|t| now.saturating_duration_since(t).as_secs()),
                )
            })
            .collect();
        // Phase 2: iterator guards are dropped; the per-key lookups are now safe.
        let mut snapshot: Vec<SubscribedContractSnapshot> = leases
            .into_iter()
            .map(
                |(key, subscribed_secs, last_updated_secs)| SubscribedContractSnapshot {
                    key,
                    subscribed_secs,
                    last_updated_secs,
                    is_receiving_updates: self.is_receiving_updates(&key),
                    in_use: self.contract_in_use(&key),
                },
            )
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
        // Idempotent re-subscribe (same client + same contract) no
        // longer needs special handling for governance: benefit is a
        // live snapshot of `local_client_count`, so a duplicate insert
        // into the set is a no-op and cannot inflate the count.
        let is_new_for_client = entry.insert(client_id);
        debug!(
            contract = %instance_id,
            %client_id,
            is_first_client,
            is_new_for_client,
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
                // Client disconnect may have just transitioned the contract to
                // no-longer-in-use. Record abandonment so over-budget eviction
                // does NOT shed it for an old last-read accrued while it was
                // subscribed: `record_abandonment` resets its recency to the
                // current frontier at termination, so the formerly-subscribed
                // contract sorts LAST (freshest recency) and survives longest.
                self.maybe_record_abandonment(&contract);
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
    ///
    /// Returns `AddSubscriberOutcome` so callers can distinguish a
    /// genuine NewAdd (counts as fresh demand for governance scoring)
    /// from a Renewal (lease-extension only — must NOT count as fresh
    /// demand, otherwise a peer churning subscriptions every 2 minutes
    /// would pad a contract's `benefit_score` by `0.1 × 30 = 3.0` per
    /// hour, the Sybil-resistance equivalent of 30 distinct subscribers
    /// for a single rotating peer). See `Ring::add_downstream_subscriber`
    /// for the caller-side gating.
    pub fn add_downstream_subscriber(
        &self,
        contract: &ContractKey,
        peer: PeerKey,
    ) -> AddSubscriberOutcome {
        let mut entry = self.downstream_subscribers.entry(*contract).or_default();
        let is_new = !entry.contains_key(&peer);
        if is_new && entry.len() >= MAX_DOWNSTREAM_SUBSCRIBERS_PER_CONTRACT {
            tracing::warn!(
                contract = %contract,
                limit = MAX_DOWNSTREAM_SUBSCRIBERS_PER_CONTRACT,
                "Downstream subscriber limit reached, rejecting peer"
            );
            return AddSubscriberOutcome::Rejected;
        }
        entry.insert(peer, self.time_source.now());
        if is_new {
            AddSubscriberOutcome::NewAdd
        } else {
            AddSubscriberOutcome::Renewal
        }
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
            // If the contract has just transitioned to no-longer-in-use, record
            // abandonment: `record_abandonment` resets its recency to the current
            // frontier at termination, so the formerly-subscribed contract sorts
            // LAST (freshest recency) under over-budget eviction and survives
            // longest, rather than being shed for an old last-read accrued while
            // it was subscribed.
            self.maybe_record_abandonment(contract);
        }
        removed
    }

    /// Check whether any downstream peers are subscribed to this contract.
    pub fn has_downstream_subscribers(&self, contract: &ContractKey) -> bool {
        self.downstream_subscribers
            .get(contract)
            .is_some_and(|peers| !peers.is_empty())
    }

    /// Lease-valid downstream subscriber peer keys for `contract`.
    ///
    /// Returns the `PeerKey`s of downstream peers whose subscription lease is
    /// still active (renewed within `SUBSCRIPTION_LEASE_DURATION`), WITHOUT
    /// mutating the map — mirrors the read-only lease check in
    /// [`downstream_subscriber_count`](Self::downstream_subscriber_count) /
    /// `expire_stale_downstream_subscribers`, so a contract whose leases have all
    /// gone stale (but not yet been swept) reports none.
    ///
    /// Used by the reconcile input-builder (keystone step-2, #4642) to apply the
    /// piece-D **strictly-farther** filter: a downstream subscriber counts as
    /// live demand only if it is farther from the contract key than this peer
    /// (the closer/upstream one is excluded, so mutual co-hosts cannot perpetuate
    /// each other's leases). This accessor returns the raw lease-valid set; the
    /// caller resolves each peer's location and applies the distance filter.
    pub(crate) fn downstream_subscriber_peers(&self, contract: &ContractKey) -> Vec<PeerKey> {
        let now = self.time_source.now();
        self.downstream_subscribers
            .get(contract)
            .map(|peers| {
                peers
                    .iter()
                    .filter(|(_, last_renewed)| {
                        now.duration_since(**last_renewed) < SUBSCRIPTION_LEASE_DURATION
                    })
                    .map(|(peer, _)| peer.clone())
                    .collect()
            })
            .unwrap_or_default()
    }

    /// Number of local (WebSocket) clients currently subscribed to this
    /// contract. Used by governance to compute the live beneficiary
    /// snapshot (the strong, hard-to-fake demand signal). Reads
    /// `client_subscriptions`, which is keyed by `ContractInstanceId`
    /// and pruned on client disconnect, so the count is the
    /// currently-active set.
    ///
    /// The production reaper-tick path no longer calls this per-contract
    /// accessor (it uses the single-pass [`beneficiary_counts`] bulk
    /// builder instead); this remains as the per-contract reference used
    /// by the governance unit tests and the test-only `Ring` accessors.
    #[cfg(test)]
    pub(crate) fn local_client_count(&self, instance_id: &ContractInstanceId) -> usize {
        self.client_subscriptions
            .get(instance_id)
            .map(|clients| clients.len())
            .unwrap_or(0)
    }

    /// Number of downstream peers with a CURRENTLY-ACTIVE (non-expired)
    /// subscription lease for this contract. Used by governance to
    /// compute the live beneficiary snapshot (weak, attacker-rotatable
    /// demand signal).
    ///
    /// `downstream_subscribers` is keyed by `ContractKey`, while
    /// governance keys on `ContractInstanceId`. A `ContractInstanceId`
    /// does not uniquely determine the parameters half of a
    /// `ContractKey`, so we match by instance id and sum across any
    /// matching keys (in practice a node hosts a single key per
    /// instance id, but this is correct regardless).
    ///
    /// Lease expiry: this mirrors `expire_stale_downstream_subscribers`
    /// — a lease is active iff it was renewed within
    /// `SUBSCRIPTION_LEASE_DURATION`. We count only non-expired leases
    /// WITHOUT mutating the map (read-only), so a contract whose leases
    /// have all gone stale (but not yet been swept) correctly reports
    /// zero current beneficiaries this tick. The periodic
    /// `expire_stale_downstream_subscribers` sweep does the actual
    /// pruning; this count must not depend on that sweep having run.
    ///
    /// The production reaper-tick path no longer calls this per-contract
    /// accessor (it uses the single-pass [`beneficiary_counts`] bulk
    /// builder instead); this remains as the per-contract reference used
    /// by the governance unit tests and the test-only `Ring` accessors.
    #[cfg(test)]
    pub(crate) fn downstream_subscriber_count(&self, instance_id: &ContractInstanceId) -> usize {
        let now = self.time_source.now();
        self.downstream_subscribers
            .iter()
            .filter(|entry| entry.key().id() == instance_id)
            .map(|entry| {
                entry
                    .value()
                    .values()
                    .filter(|last_renewed| {
                        now.duration_since(**last_renewed) < SUBSCRIPTION_LEASE_DURATION
                    })
                    .count()
            })
            .sum()
    }

    /// Build the live beneficiary-weighted benefit value for every
    /// contract instance with at least one current beneficiary, in a
    /// SINGLE pass over `client_subscriptions` and
    /// `downstream_subscribers`. Returns
    /// `LOCAL_DEMAND_WEIGHT × active_local_clients +
    ///  FORWARDED_DEMAND_WEIGHT × active_downstream_subscribers` keyed
    /// by `ContractInstanceId`.
    ///
    /// This is the bulk equivalent of calling
    /// `local_client_count` + `downstream_subscriber_count` per
    /// contract, but avoids the O(N×M) cost of re-scanning the full
    /// `downstream_subscribers` map once per tracked contract on every
    /// reaper tick. The downstream lease-validity rule here MUST match
    /// `downstream_subscriber_count` exactly: a lease counts iff it was
    /// renewed within `SUBSCRIPTION_LEASE_DURATION` (read-only, no
    /// sweep). `downstream_subscribers` is keyed by `ContractKey` but
    /// governance keys on `ContractInstanceId`, so downstream counts are
    /// summed across all keys sharing an instance id.
    ///
    /// The caller is responsible for filtering the result to the set of
    /// contracts governance is actually tracking (a contract with
    /// beneficiaries but no ingested cost has no score and must not be
    /// added to the benefit map).
    pub(crate) fn beneficiary_counts(
        &self,
        local_weight: f64,
        forwarded_weight: f64,
    ) -> HashMap<ContractInstanceId, f64> {
        let now = self.time_source.now();
        let mut benefits: HashMap<ContractInstanceId, f64> = HashMap::new();

        // Pass 1: local-client beneficiaries.
        for entry in self.client_subscriptions.iter() {
            let count = entry.value().len();
            if count > 0 {
                *benefits.entry(*entry.key()).or_insert(0.0) += local_weight * count as f64;
            }
        }

        // Pass 2: downstream-peer beneficiaries, counting only
        // lease-valid entries exactly as `downstream_subscriber_count`
        // does, and grouping by instance id.
        for entry in self.downstream_subscribers.iter() {
            let active = entry
                .value()
                .values()
                .filter(|last_renewed| {
                    now.duration_since(**last_renewed) < SUBSCRIPTION_LEASE_DURATION
                })
                .count();
            if active > 0 {
                *benefits.entry(*entry.key().id()).or_insert(0.0) +=
                    forwarded_weight * active as f64;
            }
        }

        benefits
    }

    /// Whether something still depends on this node hosting `contract` — a
    /// live local client subscription or a downstream peer subscriber. Used
    /// to gate hosting-cache eviction reclamation: a contract that is in
    /// use must NOT have its on-disk state/code deleted.
    ///
    /// **Why `is_subscribed` (this node's own upstream subscription) is NOT
    /// included.** It would seem natural to also exempt contracts the node
    /// is actively subscribed to. But `contract_in_use` now *gates* renewal:
    /// `contracts_needing_renewal` section 1 renews a soon-to-expire lease
    /// only while `contract_in_use` is true. If `is_subscribed` counted as
    /// in-use, a subscribed contract would renew its own lease, which keeps
    /// the subscription alive, which keeps `is_subscribed` (and therefore
    /// `contract_in_use`) true — a self-renewing loop with no external
    /// demand. That is exactly the #3763 renewal storm the interest gate
    /// exists to stop, reintroduced through the back door; the exemption
    /// would also be effectively unbounded, blocking reclamation
    /// indefinitely, which violates the cleanup-exemption rule in `AGENTS.md`
    /// (exemptions must be time-bounded). Local-client subscriptions and
    /// downstream subscribers ARE both time-bounded and represent real
    /// external demand: client subscriptions expire on disconnect; downstream
    /// subscribers expire via `expire_stale_downstream_subscribers` after
    /// `SUBSCRIPTION_LEASE_DURATION` without renewal.
    ///
    /// The narrow case "subscribed but no local interest" should be handled
    /// by tearing down the orphaned upstream subscription, not by carrying
    /// an unbounded GC exemption here.
    pub fn contract_in_use(&self, contract: &ContractKey) -> bool {
        self.has_client_subscriptions(contract.id()) || self.has_downstream_subscribers(contract)
    }

    /// The SPLIT genuine-demand counts pinning `contract`:
    /// `(local_client_subscriptions, downstream_subscribers)`. This is the
    /// subscriber-primary eviction key (#4642, Ian's confirmed ordering): the
    /// cache orders victims ascending by `(local, downstream, recency_seq, key)`,
    /// so a contract THIS node's own client is subscribed to (local >= 1) is
    /// evicted LAST. Aligned with [`contract_in_use`](Self::contract_in_use) BY
    /// CONSTRUCTION — it counts the exact same two sources, so `local + downstream
    /// == 0` iff `!contract_in_use(k)`, keeping retention and the collapse/renewal
    /// decision in agreement (per the piece-D source). Counts ALL downstream
    /// entries (no lease filter, matching `has_downstream_subscribers`) so the
    /// count equals `contract_in_use` exactly; the periodic
    /// `expire_stale_downstream_subscribers` sweep keeps the map fresh.
    pub(crate) fn local_and_downstream_counts(&self, contract: &ContractKey) -> (usize, usize) {
        let local = self
            .client_subscriptions
            .get(contract.id())
            .map(|c| c.len())
            .unwrap_or(0);
        let downstream = self
            .downstream_subscribers
            .get(contract)
            .map(|peers| peers.len())
            .unwrap_or(0);
        (local, downstream)
    }

    /// Complete a subscriber-primary eviction that shed a still-in-use contract
    /// (#4642, invariant 3): drop every subscription record that keeps
    /// `contract_in_use(key)` true, so the disk-reclamation funnel
    /// (`reclaim_evicted_contract` → `RuntimePool::remove_contract`) actually
    /// frees the on-disk state instead of skipping it forever. Called ONLY for
    /// the `evicted_in_use` subset (victims that were subscribed at eviction-
    /// decision time), so a zero-subscriber eviction is untouched and — crucially
    /// — a contract that gained a fresh subscriber AFTER it was evicted as
    /// zero-subscriber is NOT torn down here (the re-host / re-subscribe guards in
    /// `RuntimePool::remove_contract` handle that race).
    ///
    /// Clears, in order:
    /// - `downstream_subscribers[key]` — the peer subscription leases. Downstream
    ///   re-home, when demand persists, happens through the interest-gated
    ///   renewal loop (ring-routed via `k_closest_potentially_hosting`), NOT from
    ///   here — this PR builds no proactive re-home signal.
    /// - `client_subscriptions[key.id()]` — local WebSocket client subscriptions.
    ///   Under the fewest-`(local, downstream)` ordering a contract with LOCAL
    ///   subscriptions is only ever a victim in the all-local-subscribed extreme
    ///   (every eligible contract carries a local subscriber and the peer is still
    ///   over budget), so this is a no-op in the common case and, in that extreme,
    ///   silently STRANDS the local client. That is the accepted last-resort
    ///   behavior — there is deliberately no client-notification surface (out of
    ///   scope; see hosting-invariants invariant 3).
    /// - the active upstream subscription lease (`unsubscribe`), so
    ///   `contracts_needing_renewal` section 1 (active-subscription renewal, gated
    ///   on `contract_in_use`) does not immediately re-drive the torn-down
    ///   contract; clearing `client_subscriptions` likewise stops section 2
    ///   (client-subscription re-subscribe) from re-driving it.
    ///
    /// Returns the removed subscription state so the eviction CONSUMER can
    /// replay the identical removals against the `InterestManager` (which lives
    /// on `OpManager`, not here) via
    /// [`InterestManager::remove_evicted_in_use`](crate::ring::interest::InterestManager::remove_evicted_in_use);
    /// otherwise ghost `interested_peers` / `peer_contracts` /
    /// `local_client_count` entries survive and mis-target UPDATE broadcasts /
    /// inflate upstream interest counts (PR #4734 Fix 1). See
    /// [`EvictedInUseTeardown`].
    ///
    /// Idempotent and safe to call on an already-clean key (each removal is a
    /// no-op when absent).
    fn teardown_evicted_in_use_contract(&self, key: &ContractKey) -> EvictedInUseTeardown {
        let downstream_peers: Vec<PeerKey> = self
            .downstream_subscribers
            .remove(key)
            .map(|(_, peers)| peers.into_keys().collect())
            .unwrap_or_default();
        let local_client_count = self
            .client_subscriptions
            .remove(key.id())
            .map(|(_, clients)| clients.len())
            .unwrap_or(0);
        // Drop our own upstream lease too (same as the sweep loop's belt-and-
        // suspenders `ring.unsubscribe`), so renewal section 1 won't re-drive it.
        self.unsubscribe(key);
        let had_downstream = !downstream_peers.is_empty();
        let had_client = local_client_count > 0;
        if had_downstream || had_client {
            debug!(
                contract = %key,
                had_downstream,
                had_client,
                "Tore down subscription state for a subscriber-primary eviction \
                 (#4642 invariant 3); disk reclamation can now proceed"
            );
        }
        EvictedInUseTeardown {
            key: *key,
            downstream_peers,
            local_client_count,
        }
    }

    /// Hook called from every code path that removes an "in-use" signal
    /// (client subscription, downstream subscriber, or stale-expiry of
    /// either). If the contract has just transitioned to no-longer-in-use,
    /// record abandonment in the hosting cache: `record_abandonment` resets the
    /// entry's recency to the current frontier at termination, so under the next
    /// over-budget sweep the formerly-subscribed contract sorts LAST (freshest
    /// recency) and survives longest — it is NOT evicted first. This stops it
    /// being shed for an old last-read it accrued while parked in the
    /// subscription tier (see `record_abandonment` /
    /// `record_abandonment_resets_recency_at_subscription_termination`).
    ///
    /// Idle persistence is preserved: this changes eviction *order*, not
    /// eviction *eligibility*. A contract with no budget pressure on it
    /// stays cached regardless.
    ///
    /// Lock-order note: `contract_in_use` reads only the subscription
    /// DashMaps and never touches `hosting_cache`, so calling it from
    /// here is safe even though we then take the `hosting_cache` write
    /// lock. Callers must invoke this AFTER any subscription-map guard
    /// they hold has been dropped (the guard is needed to mutate state,
    /// not to read `contract_in_use`).
    fn maybe_record_abandonment(&self, contract: &ContractKey) {
        if !self.contract_in_use(contract) {
            self.hosting_cache.write().record_abandonment(contract);
        }
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
            let became_empty = if let Some(mut peers) = self.downstream_subscribers.get_mut(&key) {
                let before = peers.len();
                peers.retain(|_, last_renewed| {
                    now.duration_since(*last_renewed) < SUBSCRIPTION_LEASE_DURATION
                });
                let expired = before - peers.len();
                if expired > 0 {
                    expired_counts.push((key, expired));
                }
                let empty = peers.is_empty();
                if empty {
                    drop(peers);
                    self.downstream_subscribers
                        .remove_if(&key, |_, peers| peers.is_empty());
                }
                empty
            } else {
                false
            };
            if became_empty {
                // Lease expiry may have just transitioned the contract to
                // no-longer-in-use — record abandonment so the next over-budget
                // sweep does NOT shed it for an old last-read: `record_abandonment`
                // resets its recency to the current frontier at termination, so it
                // sorts LAST (freshest recency) and survives longest.
                self.maybe_record_abandonment(&key);
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

    /// Count of phantom in-use contracts: contracts registered as in-use via a
    /// downstream subscriber whose state is NOT present on disk
    /// (`contract_in_use && !contract_state_present`). This is the step-10 §1d
    /// falsifier — after the register-after-state fix (a hop registers a
    /// downstream only once it holds state) it should read 0. A nonzero value
    /// means a hop registered demand it cannot serve (the #4404/#4612 phantom).
    ///
    /// redb-scoped by construction: `contract_state_present` is
    /// conservative-true on sqlite, before the storage handle is set, and on
    /// store errors, so this reads 0 on those backends regardless (matching the
    /// #4610/#4612 gate's own scoping).
    ///
    /// Scans only `downstream_subscribers` (the dominant relay phantom source,
    /// gen a/d, keyed by full `ContractKey`). Client-subscription-only phantoms
    /// (gen b/c) are keyed by instance id with no full-key index and cannot be
    /// state-checked here, so they are OUT OF SCOPE for this gauge; cleaning up a
    /// client-subscription-only phantom left by a dead-end GET/PUT is DEFERRED to
    /// a future PR (the up-front client subscription currently lingers until the
    /// client disconnects).
    pub(crate) fn phantom_in_use_count(&self) -> u64 {
        self.downstream_subscribers
            .iter()
            .filter(|entry| !self.contract_state_present(entry.key()))
            .count() as u64
    }

    /// Reconcile the downstream-driven in-use set against the state store
    /// (#4612 root fix, option b): enforce `in-use ⇒ has-state` as a repaired
    /// invariant rather than a persistent violation.
    ///
    /// The transit-relay SUBSCRIBE path registers downstream subscribers on a
    /// node that never fetched the contract's state (update forwarding needs
    /// the registration, so it cannot simply be skipped — see
    /// `register_downstream_subscriber`). That makes `contract_in_use` true
    /// with nothing on disk: a "phantom host" that is advertised to neighbors
    /// (GET dead-ends, #4404) and — before the #4610/#4611 gate — stormed the
    /// summarize loop. This sweep turns each phantom into one of:
    ///
    /// - [`PhantomRepair::Fetch`]: launch a one-shot sub-op GET so the state
    ///   is actually fetched/stored and the node becomes a real host. Bounded
    ///   by `max_fetches` per sweep, [`PHANTOM_REPAIR_COOLDOWN`] between
    ///   attempts per contract (never overlapping an in-flight fetch), and
    ///   [`MAX_PHANTOM_REPAIR_ATTEMPTS`] total (bound the producer — the
    ///   #4440/#4610 storms both came from unbounded producers).
    ///
    /// Once `MAX_PHANTOM_REPAIR_ATTEMPTS` fetches fail, the sweep STOPS fetching
    /// for that contract. The #4770 *cycling* Drop stays NEUTRALIZED (step 10
    /// §1c): with the register-after-state fix a genuine phantom is
    /// unrepresentable, so an immediate drop would only churn. But the exemption
    /// is time-bounded (ring.md): a phantom whose downstream keeps RENEWING never
    /// hits the Fix-6 downstream-expiry prune, so once it is older than
    /// [`PHANTOM_ABSOLUTE_MAX_AGE`] the sweep emits a [`PhantomRepair::Drop`]
    /// (review Fix C). That drop is safe and non-cycling because a
    /// re-registration must go through `finalize_host_subscribe` (fetch-first),
    /// which registers NOTHING while the state stays unfetchable. Below the age
    /// bound the sweep survives as a rollout net for phantoms left by pre-upgrade
    /// peers; the downstream peer's own lease expiry / renewal may re-root it.
    ///
    /// Decisions are level-triggered against the state store: a contract
    /// whose state appears (this sweep's fetch, an inbound UPDATE auto-fetch,
    /// a PUT...) has its tracking cleared on the next pass, whatever wrote it.
    /// Contracts with a subscription request in flight are skipped — that
    /// flow fetches the contract itself. Client-subscription-only phantoms
    /// are not scanned: `client_subscriptions` is keyed by instance id (no
    /// key index exists) and an active client subscribe already fetches
    /// state; the dominant phantom source is the relay's downstream map.
    ///
    /// Conservative by construction: `contract_state_present` returns `true`
    /// on the sqlite backend, pre-`set_storage`, and on store errors, so no
    /// phantom is flagged (and nothing is fetched or dropped) unless a real
    /// redb store answered "absent".
    pub(crate) fn reconcile_phantom_in_use(&self, max_fetches: usize) -> Vec<PhantomRepair> {
        let now = self.time_source.now();
        let mut actions = Vec::new();
        let mut fetches = 0usize;

        let keys: Vec<ContractKey> = self
            .downstream_subscribers
            .iter()
            .map(|entry| *entry.key())
            .collect();

        // Fix 6 (step 10 §1c follow-up): prune phantom_repair tracking for keys
        // that are no longer downstream-registered. This sweep only visits
        // downstream_subscribers, so once an exhausted phantom's downstream
        // subscription expires/unsubscribes the key is never revisited again —
        // without this its phantom_repair entry would leak forever (one permanent
        // per-node entry per exhausted phantom, violating the time-bounded
        // cleanup-exemption rule). Pruning against the current downstream snapshot
        // keeps the tracking existence-bounded. Uses the snapshot (not a live
        // downstream_subscribers lock), so no cross-DashMap guard is held during
        // the retain — a phantom_repair key is created only for a key in the
        // snapshot, so any key absent from it is genuinely orphaned.
        {
            let downstream: std::collections::HashSet<ContractKey> = keys.iter().copied().collect();
            self.phantom_repair
                .retain(|key, _| downstream.contains(key));
        }

        for key in keys {
            if self.contract_state_present(&key) {
                // Invariant holds (or repair succeeded) — clear any tracking.
                self.phantom_repair.remove(&key);
                continue;
            }
            if self.pending_subscription_requests.contains(&key) {
                continue;
            }

            use dashmap::mapref::entry::Entry;
            match self.phantom_repair.entry(key) {
                Entry::Occupied(mut e) => {
                    if e.get().attempts >= MAX_PHANTOM_REPAIR_ATTEMPTS {
                        // Repair attempts exhausted. The #4770 cycling Drop stays
                        // NEUTRALIZED for the common case (step 10 §1c): with the
                        // register-after-state fix a genuine phantom is
                        // unrepresentable, so an immediate drop would only churn.
                        // BUT the exemption must be time-bounded (ring.md): a
                        // phantom whose downstream keeps RENEWING never hits the
                        // Fix-6 downstream-expiry prune, so once it is older than
                        // PHANTOM_ABSOLUTE_MAX_AGE we DROP it (review Fix C). This
                        // is safe and non-cycling because a re-registration goes
                        // through finalize_host_subscribe (fetch-first) — while the
                        // state stays unfetchable it registers NOTHING, so the
                        // phantom cannot re-form.
                        if now.duration_since(e.get().first_seen) >= PHANTOM_ABSOLUTE_MAX_AGE {
                            e.remove();
                            actions.push(PhantomRepair::Drop(key));
                        }
                        // Under the absolute-age bound: STOP fetching, do not drop
                        // (the downstream's own lease expiry / renewal may re-root
                        // it, or a later fetch could still succeed).
                        continue;
                    }
                    let entry = e.get_mut();
                    if now.duration_since(entry.last_attempt) < PHANTOM_REPAIR_COOLDOWN {
                        continue;
                    }
                    if fetches < max_fetches {
                        entry.attempts += 1;
                        entry.last_attempt = now;
                        fetches += 1;
                        actions.push(PhantomRepair::Fetch(key));
                    }
                }
                Entry::Vacant(slot) => {
                    if fetches < max_fetches {
                        slot.insert(PhantomRepairEntry {
                            attempts: 1,
                            last_attempt: now,
                            first_seen: now,
                        });
                        fetches += 1;
                        actions.push(PhantomRepair::Fetch(key));
                    }
                }
            }
        }

        actions
    }

    /// Drop the downstream-subscriber registration for an
    /// absolute-age-exhausted phantom contract (review Fix C — the state stayed
    /// unfetchable past `PHANTOM_ABSOLUTE_MAX_AGE` while the downstream kept
    /// renewing). Returns the number of removed subscriber entries so the caller
    /// can decrement the interest manager symmetrically (one
    /// `remove_downstream_subscriber` per entry), then collapse upstream.
    /// Non-cycling: a re-registration must go through `finalize_host_subscribe`
    /// (fetch-first), which registers nothing while state is unfetchable.
    pub(crate) fn drop_phantom_downstream(&self, key: &ContractKey) -> usize {
        let removed = self
            .downstream_subscribers
            .remove(key)
            .map(|(_, peers)| peers.len())
            .unwrap_or(0);
        if removed > 0 {
            // Same as lease expiry: the contract may have just transitioned to
            // no-longer-in-use — reset recency so the next over-budget sweep does
            // not shed it for an old last-read.
            self.maybe_record_abandonment(key);
        }
        self.phantom_repair.remove(key);
        removed
    }

    // =========================================================================
    // Hosting Cache Management
    // =========================================================================

    /// Update this peer's own ring location, used to turn a contract key into a
    /// distance for the proximity-prior demand estimate. Pushed by `Ring` on the
    /// snapshot cadence (`own_location()` can be `None` early in a node's life,
    /// so callers should only push a known location).
    pub(crate) fn set_own_location(&self, location: Location) {
        *self.own_location.write() = Some(location);
    }

    /// Record that a local-client GET was answered from local hosted state (a
    /// hit). Counted at the actual serve decision in the client GET handler, not
    /// by cache membership, so the hit-rate metric reflects real serves. (#4642 A3)
    pub(crate) fn record_local_get_serve(&self) {
        self.local_get_serves.fetch_add(1, Ordering::Relaxed);
    }

    /// Record that a local-client GET was routed to the network (a miss/forward).
    /// Counterpart to [`record_local_get_serve`](Self::record_local_get_serve).
    pub(crate) fn record_local_get_forward(&self) {
        self.local_get_forwards.fetch_add(1, Ordering::Relaxed);
    }

    /// Monotonic count of local-client GETs served from local hosted state.
    pub(crate) fn local_get_serves(&self) -> u64 {
        self.local_get_serves.load(Ordering::Relaxed)
    }

    /// Monotonic count of local-client GETs routed to the network.
    pub(crate) fn local_get_forwards(&self) -> u64 {
        self.local_get_forwards.load(Ordering::Relaxed)
    }

    /// Ring distance from this peer to `key`, and the proximity-prior demand at
    /// that distance. Returns `(None, NEUTRAL_DEMAND)` when this peer's own
    /// location is not yet known — demand degrades to neutral, so eviction falls
    /// back to Greedy-Dual floor + recency ordering (still demand-aware via the
    /// read-refresh floor, just without distance weighting).
    fn distance_and_demand(&self, key: &ContractKey) -> (Option<f64>, f64) {
        let own = *self.own_location.read();
        let distance = own.map(|loc| loc.distance(Location::from(key)).as_f64());
        let demand = match distance {
            Some(d) => self.demand_estimator.read().predict(d),
            None => demand::NEUTRAL_DEMAND,
        };
        (distance, demand)
    }

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

        // Demand-ordered eviction (A3): predict this contract's read-demand from its
        // ring distance to us via the proximity prior. The read guard is dropped
        // before the hosting-cache write lock is taken (no nested lock order).
        let (distance, predicted_demand) = self.distance_and_demand(&key);

        let mut result = self.hosting_cache.write().record_access_with_demand(
            key,
            size_bytes,
            access_type,
            current_generation,
            predicted_demand,
            |k: &ContractKey| self.local_and_downstream_counts(k),
        );

        // Subscriber-primary eviction (#4642, invariant 3) can now shed a
        // still-in-use contract as a last resort. For each such victim tear down
        // its subscription state HERE — before the caller iterates `result.evicted`
        // and calls `reclaim_evicted_contract` — so `contract_in_use` is already
        // false when the reclaim gate checks it and the on-disk state is actually
        // freed (the memory-teardown the shipped #4720 code was missing). The
        // cache write lock is already dropped; the teardown touches only the
        // subscription DashMaps + active_subscriptions, never the hosting cache.
        //
        // Collect what each teardown removed so the CONSUMER can replay the same
        // removals against the `InterestManager` (which lives on `OpManager`, not
        // here) — otherwise ghost `interested_peers` / `peer_contracts` /
        // `local_client_count` entries survive (PR #4734 Fix 1).
        result.evicted_in_use_teardown = result
            .evicted_in_use
            .iter()
            .map(|evicted_key| self.teardown_evicted_in_use_contract(evicted_key))
            .collect();

        // Train the proximity prior from this peer's own observed read rate for
        // the accessed contract (only reads yield a sample; PUT is a seed). The
        // estimator is trained on the aggregate distance -> rate relationship;
        // the per-contract blend is A4. Cache lock already dropped.
        if let (Some(d), Some(rate)) = (distance, result.observed_read_rate) {
            self.demand_estimator.write().observe(d, rate);
        }

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

    /// Whether this node has STORED STATE for `key` in the contract state
    /// store (on disk), as opposed to merely tracking it in an in-memory
    /// cache or subscriber map.
    ///
    /// This is the load-bearing distinction for the #4610 summarize/broadcast
    /// gate. A contract can be marked hosted (`is_hosting_contract`) or be
    /// `contract_in_use` (a downstream subscriber renewing an inbound relay
    /// SUBSCRIBE) WITHOUT its state ever having been fetched and stored — the
    /// "phantom" (interested-but-stateless) contracts of #4440. Summarizing or
    /// broadcasting such a contract is pointless: there is nothing to
    /// summarize, every attempt fails with "Contract state not found in
    /// store", and at scale the periodic interest-sync/broadcast loops fire
    /// ~70-80 of these per second — the #4610 CPU/memory storm.
    ///
    /// Reads the state store directly so the answer is correct in the cases
    /// the in-memory caches get wrong:
    /// - a cache-hosted contract whose state was never stored → absent (skip);
    /// - an evicted-but-in-use contract whose state is still on disk
    ///   (`evict_over_budget` retains `contract_in_use` entries, and
    ///   `reclaim_evicted_contract` only deletes state once NOT in use) →
    ///   present, so it KEEPS summarizing/broadcasting (no regression). This
    ///   is exactly why the gate must NOT key on `is_hosting_contract`, which
    ///   reads only the in-memory hosting cache.
    ///
    /// Cheap: a single redb point lookup on the STATE table that does not
    /// deserialize the value (`get_state_size` reads only the value length).
    /// It runs per-hosted-contract per-heartbeat, but REPLACES a far more
    /// expensive failing round-trip (a `GetSummaryQuery` on the serial
    /// contract-handling loop → full fetch → `MissingContract`) for every
    /// phantom contract, so it is a net reduction in work.
    ///
    /// Conservative on uncertainty: an unset storage handle (pre-startup) or a
    /// transient store error returns `true` (treat as present) so a real
    /// hosted contract is never wrongly dropped from interest-sync repair. The
    /// sqlite backend has no cheap *synchronous* existence check, so it also
    /// returns `true`, preserving pre-#4610 behavior; the storm is a
    /// redb-production phenomenon.
    pub fn contract_state_present(&self, key: &ContractKey) -> bool {
        #[cfg(feature = "redb")]
        {
            if let Some(storage) = self.storage.read().as_ref() {
                return match storage.get_state_size(key) {
                    Ok(size) => size.is_some(),
                    Err(e) => {
                        // debug!, not warn!: this runs per-hosted-contract
                        // per-heartbeat, so a persistent store error would itself
                        // log-storm (~1000/heartbeat) — the same failure class
                        // #4610 fixes. Assume present so a real hosted contract is
                        // never dropped from interest-sync on a transient error.
                        tracing::debug!(
                            contract = %key,
                            error = %e,
                            "state-presence check failed; assuming present"
                        );
                        true
                    }
                };
            }
        }
        #[cfg(not(feature = "redb"))]
        {
            // No cheap synchronous existence check on this backend; preserve
            // pre-#4610 behavior. Touch `key` so it is not flagged unused.
            let _ = key;
        }
        true
    }

    /// Async existence probe for the ResyncResponse responder (#4864 round-5 P1).
    /// The sync [`Self::contract_state_present`] only has a redb fast-path, so on
    /// a SQLite build (`--no-default-features --features sqlite`) it returns
    /// `true` for every key — reopening the responder limiter-map key-flood hole
    /// there. This async variant keeps the redb SYNC fast-path (a cheap point
    /// lookup, no `.await`) and adds a REAL SQLite existence probe via the async
    /// `get_state_size` (`SELECT LENGTH(state) … WHERE contract = ?`), so a bogus
    /// key is rejected before it can consume a limiter slot on EITHER backend.
    /// Conservative on error / no-storage: returns `true` (assume present) so a
    /// genuinely-hosted contract is never wrongly refused a resync response.
    pub(crate) async fn contract_state_present_async(&self, key: &ContractKey) -> bool {
        #[cfg(feature = "redb")]
        {
            // redb: the synchronous point lookup is already cheap — no await.
            self.contract_state_present(key)
        }
        #[cfg(all(feature = "sqlite", not(feature = "redb")))]
        {
            // Clone the pool handle out of the (sync parking_lot) guard so the
            // `.await` below never holds a lock across an await point.
            let storage = self.storage.read().as_ref().cloned();
            match storage {
                Some(storage) => match storage.get_state_size(key).await {
                    Ok(size) => size.is_some(),
                    Err(e) => {
                        tracing::debug!(
                            contract = %key,
                            error = %e,
                            "sqlite state-presence check failed; assuming present"
                        );
                        true
                    }
                },
                None => true,
            }
        }
        #[cfg(not(any(feature = "redb", feature = "sqlite")))]
        {
            let _ = key;
            true
        }
    }

    /// The composed #4610 gate: summarize / broadcast a contract's state ONLY
    /// when we host or actively serve it AND we actually hold its state:
    /// `(is_hosting_contract || contract_in_use) && contract_state_present`.
    ///
    /// SINGLE SOURCE OF TRUTH for the gate, called by both
    /// `node::summary_if_hosted_or_in_use` (periodic interest-sync) and
    /// `broadcast_queue::should_broadcast_contract` (broadcast fan-out). Keeping
    /// the predicate in one place means the two paths cannot drift, and an
    /// `&&`→`||` miswire (which would let a phantom pass and re-open the storm)
    /// is caught by ONE behavioural test
    /// (`summarize_gate_skips_stateless_phantom_keeps_stateful_4610`) rather than
    /// needing one per call site.
    ///
    /// `contract_state_present` does a deliberate cheap SYNCHRONOUS redb point
    /// lookup on the state store. That is intentional and net-cheaper than the
    /// pre-#4610 behavior (it replaces a failing full-fetch round-trip on the
    /// serial contract-handling loop). Do NOT "optimize" it into an in-memory
    /// hosting-cache check: that would reintroduce the evicted-but-on-disk
    /// regression (a contract whose state is still on disk but no longer in the
    /// cache must keep summarizing/broadcasting).
    pub fn should_summarize_or_broadcast(&self, key: &ContractKey) -> bool {
        (self.is_hosting_contract(key) || self.contract_in_use(key))
            && self.contract_state_present(key)
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

    /// Snapshot the hosting cache's aggregate resource gauges (budget, current
    /// bytes, contract count, budget-triggered eviction count) under a single
    /// read lock, for the per-node `RouterSnapshot` telemetry (A2).
    pub(crate) fn hosting_cache_stats(&self) -> HostingCacheStats {
        self.hosting_cache.read().stats()
    }

    /// Per-contract Greedy-Dual eviction rows for the local-peer dashboard,
    /// in eviction order (next victim first). Reads the canonical hosting
    /// cache under a single lock — this is piece A's live demand-driven
    /// retention state (#4642), the mechanism that replaced the dormant MAD
    /// governance detector. See [`HostingContractScore`].
    pub(crate) fn dashboard_hosting_scores(&self) -> Vec<HostingContractScore> {
        self.hosting_cache.read().eviction_ordered_scores()
    }

    /// Whether the over-budget sweep would evict `score`'s contract in the
    /// COMMON case — NOT [`contract_in_use`](Self::contract_in_use).
    ///
    /// The dashboard uses this to badge the "next to evict" contract: the raw
    /// lowest-keep-score row can be an entry the sweep would not pick first
    /// (ordered last because a local client / downstream subscriber makes it
    /// in-use), so badging it unconditionally would mislabel a still-wanted
    /// contract. There is no longer a `min_ttl` age gate (dropped 2026-07-08), so
    /// eligibility reduces to "not in use".
    ///
    /// NOTE (subscriber-primary rework, #4642): under the split-ordering model an
    /// in-use contract is no longer hard-pinned — it is ordered LAST and IS shed
    /// as a last resort when nothing with fewer subscribers is eligible and the
    /// peer is still over budget. This badge deliberately reflects the common,
    /// non-last-resort case (in-use = not the next victim); it does not surface the
    /// all-subscribed-extreme last-resort shed. A dashboard that wants to show the
    /// true last-resort victim would drop the `!contract_in_use` term.
    ///
    /// Deadlock-safe by construction: `score` is already-collected owned data
    /// (the `hosting_cache` read guard was released when
    /// [`dashboard_hosting_scores`](Self::dashboard_hosting_scores) returned),
    /// and `contract_in_use` reads only the subscription maps — never the
    /// hosting cache — so there is no lock held across this call.
    pub(crate) fn is_eviction_eligible(&self, score: &HostingContractScore) -> bool {
        !self.contract_in_use(&score.key)
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
    ///
    /// Test-only since serve-DURING (#4642 R3 piece C) removed the production
    /// caller (the originator GET gate). The underlying flag is still maintained
    /// (`mark_local_client_access`) and consulted via the recency variant
    /// (`has_recent_local_client_access`), which the reconcile renewal path uses;
    /// this plain accessor survives only for the hosting unit tests.
    #[cfg(test)]
    pub fn has_local_client_access(&self, key: &ContractKey) -> bool {
        self.hosting_cache.read().has_local_client_access(key)
    }

    /// Whether a local client GET or PUT touched this contract within the renewal
    /// age gate (`SUBSCRIPTION_LEASE_DURATION`) — real, time-bounded local demand
    /// that is NOT a subscription. This is the exact signal
    /// `contracts_needing_renewal()` branch 3 uses to keep a read-only / PUT-only
    /// contract (River UI container, web/UI containers) renewed in the update mesh
    /// (`hosting-invariants.md` invariant 3: reads/PUTs are permanent demand). The
    /// reconcile input-builder ORs it into `contract_in_use` so the P6 renewal /
    /// collapse gate does not drop such a contract's lease.
    pub fn has_recent_local_client_access(&self, key: &ContractKey) -> bool {
        self.hosting_cache
            .read()
            .has_recent_local_client_access(key, SUBSCRIPTION_LEASE_DURATION)
    }

    /// Touch a contract in the hosting cache (refresh demand without adding).
    ///
    /// Called when a user GET serves a hosted contract from local cache — the
    /// dominant read path for hot contracts. Trains the proximity prior from the
    /// observed local-serve read rate (A3), the same way `record_contract_access`
    /// does for network GETs, so the prior is not blind to the reads it is meant
    /// to model. The cache write lock is dropped before the estimator write lock
    /// (no nested lock order), matching `record_contract_access`.
    pub fn touch_hosting(&self, key: &ContractKey) {
        // Predict this contract's read-demand from its ring distance BEFORE
        // touching, so a restart-loaded entry (seeded at neutral because our
        // location was unknown at load) picks up the distance prior on this
        // local-serve read — the same distance-prior path `record_contract_access`
        // takes for network refetches. Read guards are dropped before the cache
        // write lock (no nested lock order), matching `record_contract_access`.
        let (distance, predicted_demand) = self.distance_and_demand(key);
        let observed_read_rate = self
            .hosting_cache
            .write()
            .touch_with_demand(key, predicted_demand);
        // Cache lock dropped; train the prior from the observed local-serve rate.
        if let (Some(d), Some(rate)) = (distance, observed_read_rate) {
            self.demand_estimator.write().observe(d, rate);
        }
    }

    /// Sweep for over-budget entries in the hosting cache.
    ///
    /// Under normal (`AtCapacity`) pressure victims are chosen subscriber-primary
    /// (#4642, invariant 3): ascending `(local_subscription_count,
    /// downstream_subscriber_count, recency_seq, key)`, using the same split
    /// `local_and_downstream_counts` closure `record_contract_access` passes, so
    /// all eviction paths agree. A subscribed contract is ordered LAST and shed
    /// only as a last resort (when nothing with fewer subscribers is eligible and
    /// the peer is still over budget) — it is NOT hard-pinned, the change from the
    /// shipped #4720 code. When such a still-in-use victim IS shed, its
    /// subscription state is torn down here (via `teardown_evicted_in_use_contract`)
    /// so `contract_in_use` is false before the caller reclaims it and the on-disk
    /// state is actually freed. The `Overflow` pressure (which ALSO pierces the
    /// op-scoped backstop) is intentionally unwired in production (see
    /// `cache::MemoryPressure`).
    /// Downstream subscriber leases are otherwise time-bounded: stale entries are
    /// removed by `expire_stale_downstream_subscribers()` (called periodically)
    /// after `SUBSCRIPTION_LEASE_DURATION` without renewal.
    /// Automatically removes persisted metadata for expired contracts.
    ///
    /// Returns a [`HostingSweepResult`]: the `(ContractKey, write_generation)`
    /// reclaim pairs — the generation captured atomically under the hosting-cache
    /// write lock travels with the `EvictContract` event so the deletion-time
    /// guard can detect a re-host race — plus, for any still-in-use victim shed
    /// as a last resort, the subscription state torn down so the caller can sync
    /// the `InterestManager` (PR #4734 Fix 1).
    // No-axes convenience wrapper (the pure byte sweep). Production always
    // goes through `Ring::sweep_expired_hosting`, which supplies the cost
    // axes to `sweep_expired_hosting_with_cost`; this wrapper is retained for
    // tests exercising the no-cost degradation path.
    #[cfg_attr(not(test), allow(dead_code))]
    pub fn sweep_expired_hosting(&self) -> HostingSweepResult {
        self.sweep_expired_hosting_with_cost(&[])
    }

    /// [`Self::sweep_expired_hosting`] plus the cost-pressure decision
    /// (cost-aware eviction, #4861): after the byte-budget sweep, shed
    /// zero-demand contracts whose attributed update-work cost dominates the
    /// node's total on a cost axis — even while UNDER the byte budget. The
    /// caller (`Ring::sweep_expired_hosting`) builds `cost_axes` from the
    /// topology meter; an empty slice degrades to the pure byte sweep. Both
    /// phases run under ONE hosting-cache write guard so the combined
    /// decision is atomic against concurrent accesses; cost victims flow
    /// through the same teardown / metadata-cleanup / reclaim funnel as byte
    /// victims (their `was_in_use` is `false` by construction, so the in-use
    /// teardown is a no-op for them).
    pub fn sweep_expired_hosting_with_cost(
        &self,
        cost_axes: &[cache::CostAxisPressure],
    ) -> HostingSweepResult {
        // AtCapacity: the Overflow op-backstop-pierce trigger is intentionally
        // unwired in production (see cache::MemoryPressure). AtCapacity itself
        // CAN now shed a subscribed contract as a last resort — see below.
        //
        // `local_and_downstream_counts` reads only the subscription DashMaps —
        // never `hosting_cache` — so calling it from inside the write guard
        // does not re-enter the lock (same pattern as `record_contract_access`).
        let evicted = {
            let mut cache_guard = self.hosting_cache.write();
            let mut evicted = cache_guard.sweep_expired(
                |key| self.local_and_downstream_counts(key),
                cache::MemoryPressure::AtCapacity,
            );
            if !cost_axes.is_empty() {
                evicted.extend(cache_guard.evict_cost_pressure(
                    &|key: &ContractKey| self.local_and_downstream_counts(key),
                    cost_axes,
                ));
            }
            evicted
        };

        // Tear down subscription state for any still-in-use victim BEFORE the
        // caller reclaims it, so `contract_in_use` is false when the reclaim gate
        // checks it and the disk state is actually freed (the memory-teardown the
        // shipped #4720 code was missing). Then strip to the `(key, generation)`
        // reclaim list the caller (`ring.rs` maintenance loop) already consumes.
        // Collect each teardown so the caller can replay the removals against the
        // `InterestManager` (which lives on `OpManager`, not here).
        let mut evicted_in_use_teardown = Vec::new();
        let expired: Vec<(ContractKey, u64)> = evicted
            .iter()
            .map(|e| {
                if e.was_in_use {
                    evicted_in_use_teardown.push(self.teardown_evicted_in_use_contract(&e.key));
                }
                (e.key, e.write_generation)
            })
            .collect();

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

        HostingSweepResult {
            expired,
            evicted_in_use_teardown,
        }
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

    /// Mark a subscription request as in-flight, claiming the pending slot.
    /// Returns true if THIS call claimed the slot (the contract was not already
    /// pending), false if another caller already holds the claim.
    ///
    /// The claim is atomic: `DashSet::insert` returns true iff the value was
    /// newly inserted, so the check-and-claim is a single locked operation. A
    /// separate `contains()` then `insert()` would leave a TOCTOU window where
    /// two concurrent callers (the ~30s renewal loop and the connection-drop
    /// prompt re-root, #4642 piece F) could both observe "not pending" and each
    /// believe it owns the claim. Callers own the pending slot on a true return
    /// and are responsible for clearing it via `complete_subscription_request`.
    pub fn mark_subscription_pending(&self, contract: ContractKey) -> bool {
        self.pending_subscription_requests.insert(contract)
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

        // 1. Contracts with soon-to-expire subscriptions AND active demand.
        //
        //    The `contract_in_use` gate is load-bearing (demand-driven-hosting
        //    design §5a / §7). A subscribed host renews its lease only while
        //    something still depends on it hosting the contract: a local client,
        //    or a registered downstream subscriber. Both are time-bounded
        //    (clients disconnect, downstream registrations lease-expire), so when
        //    demand fades the peer stops renewing, its lease lapses, and the
        //    chain collapses inward toward the key. Without this gate every
        //    subscribed host self-renews forever regardless of interest, so live
        //    subscriptions grow with accumulated cache rather than active demand
        //    — the #3763 renewal storm. The eviction budget (piece A) does NOT
        //    bound this: an in-use, actively-subscribed contract is
        //    eviction-exempt AND self-renewing, so the storm set is exactly the
        //    set the budget may not touch.
        //
        // Collect and sort for deterministic iteration order
        let mut active_subs: Vec<_> = self
            .active_subscriptions
            .iter()
            .map(|entry| (*entry.key(), entry.value().expires_at))
            .collect();
        active_subs.sort_by(|(a, _), (b, _)| a.id().as_bytes().cmp(b.id().as_bytes()));

        for (key, expires_at) in active_subs {
            // `contract_in_use` reads only the client-subscription and
            // downstream-subscriber maps (never `hosting_cache`), and `active_subs`
            // is already materialized above, so no active_subscriptions shard guard
            // is held across this call.
            if expires_at <= renewal_threshold && expires_at > now && self.contract_in_use(&key) {
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

                // Seed the cold demand from the distance prior when our own
                // location is already known at load; `distance_and_demand`
                // returns NEUTRAL_DEMAND otherwise (the usual cold-restart case),
                // and the first live read applies the prior lazily.
                let predicted_demand = self.distance_and_demand(&key).1;
                cache.load_persisted_entry_with_demand(
                    key,
                    metadata.size_bytes,
                    access_type,
                    age,
                    metadata.local_client_access,
                    predicted_demand,
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
                // Distance prior when our location is known at load, else neutral
                // (applied lazily on first read). See the metadata-load branch.
                let predicted_demand = self.distance_and_demand(&key).1;
                cache.load_persisted_entry_with_demand(
                    key,
                    size_bytes,
                    cache::AccessType::Get,
                    std::time::Duration::ZERO,
                    false,
                    predicted_demand,
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

                // Seed the cold demand from the distance prior when our own
                // location is already known at load; `distance_and_demand`
                // returns NEUTRAL_DEMAND otherwise (the usual cold-restart case),
                // and the first live read applies the prior lazily.
                let predicted_demand = self.distance_and_demand(&key).1;
                cache.load_persisted_entry_with_demand(
                    key,
                    metadata.size_bytes,
                    access_type,
                    age,
                    metadata.local_client_access,
                    predicted_demand,
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

                // Distance prior when our location is known at load, else neutral
                // (applied lazily on first read). See the metadata-load branch.
                let predicted_demand = self.distance_and_demand(&key).1;
                cache.load_persisted_entry_with_demand(
                    key,
                    size_bytes,
                    cache::AccessType::Get,
                    std::time::Duration::ZERO,
                    false,
                    predicted_demand,
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
        Self::new(default_hosting_budget_bytes())
    }
}

// =============================================================================
// Tests
// =============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use freenet_stdlib::prelude::CodeHash;

    /// Fixed 1 GiB budget for the behavioral tests below. The production default
    /// is now RAM-scaled (capability-relative, #4642 A2); these tests
    /// deliberately pin a large, deterministic budget so eviction is driven only
    /// by the sizes they set, independent of the test host's real RAM.
    const DEFAULT_HOSTING_BUDGET_BYTES: u64 = 1024 * 1024 * 1024;

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

    /// THE cost-aware-eviction discriminator (#4861), at the manager level:
    /// under cost pressure — with the byte budget nowhere near exceeded (the
    /// exact shape of the production storm: a 121-byte zero-subscriber
    /// contract holding ~58% of the gateway's broadcast capacity, never
    /// evicted because the sweep was byte-gated) — the sweep sheds the
    /// zero-subscriber cost offender, while a SUBSCRIBED contract holding an
    /// equally dominant cost share is untouched. The victim flows through the
    /// standard [`HostingSweepResult::expired`] funnel (the same list the
    /// ring maintenance task feeds to `unregister_local_hosting` →
    /// advertisement retraction and `reclaim_evicted_contract` → disk
    /// reclamation), with no in-use teardown (cost victims are zero-demand by
    /// construction). Removing the subscriber then flips the protection: the
    /// same contract becomes evictable on the next cost sweep — proving the
    /// gate is the subscriber, not the key.
    #[tokio::test]
    async fn cost_pressure_sweep_evicts_zero_subscriber_offender_not_subscribed() {
        let clock = crate::util::time_source::SharedMockTimeSource::new();
        let manager = HostingManager::with_time_source(
            DEFAULT_HOSTING_BUDGET_BYTES,
            std::sync::Arc::new(clock.clone()),
        );
        let junk = make_contract_key(1);
        let hot_subscribed = make_contract_key(2);
        manager.record_contract_access(junk, 121, AccessType::Put);
        manager.record_contract_access(hot_subscribed, 121, AccessType::Put);
        manager.add_downstream_subscriber(&hot_subscribed, make_peer_key(10));
        // Age the PUT-seed genuine-access stamps past the cost window: the
        // storm has been running long past it with no genuine GET/PUT.
        clock.advance_time(cache::COST_RATE_MIN_WINDOW + Duration::from_secs(1));

        let axes = vec![CostAxisPressure {
            axis: "exec_cpu_micros_per_sec",
            total_rate: 100_000.0,
            floor: cache::EXEC_CPU_PRESSURE_FLOOR_MICROS_PER_SEC,
            rates: [(*junk.id(), 58_000.0), (*hot_subscribed.id(), 41_000.0)]
                .into_iter()
                .collect(),
        }];

        let result = manager.sweep_expired_hosting_with_cost(&axes);
        let expired_keys: Vec<ContractKey> = result.expired.iter().map(|(k, _)| *k).collect();
        assert_eq!(
            expired_keys,
            vec![junk],
            "the zero-subscriber cost offender is shed; the subscribed \
             contract is protected by candidacy, not ranking"
        );
        assert!(
            result.evicted_in_use_teardown.is_empty(),
            "cost victims are zero-demand — nothing to tear down"
        );
        assert!(!manager.is_hosting_contract(&junk));
        assert!(manager.is_hosting_contract(&hot_subscribed));

        // Idempotent under unchanged pressure: the offender is gone, the
        // subscribed contract stays protected.
        let again = manager.sweep_expired_hosting_with_cost(&axes);
        assert!(again.expired.is_empty());
        assert!(manager.is_hosting_contract(&hot_subscribed));

        // Protection follows the SUBSCRIBER: once the downstream subscriber
        // leaves — and the termination grace window (the recency reset at
        // subscription termination, invariant 3) has elapsed — the same
        // high-cost contract becomes a candidate.
        manager.remove_downstream_subscriber(&hot_subscribed, &make_peer_key(10));
        clock.advance_time(cache::COST_RATE_MIN_WINDOW + Duration::from_secs(1));
        let after_unsub = manager.sweep_expired_hosting_with_cost(&axes);
        let after_keys: Vec<ContractKey> = after_unsub.expired.iter().map(|(k, _)| *k).collect();
        assert_eq!(after_keys, vec![hot_subscribed]);
        assert!(!manager.is_hosting_contract(&hot_subscribed));
    }

    /// STORM-FREQUENCY integration test (#4861): drive the REAL production
    /// storm profile — 121-byte payload, ~58 co-host targets, sustained
    /// ~0.62 dispatches/s (~36 per-peer sends/s) for 10 minutes — through the
    /// ACTUAL `Meter` (production 100-sample window, real count-truncation),
    /// the real `contract_cost_rates` read, the shared `build_cost_axes`
    /// assembly, and the real sweep. This is the test the synthetic-axes
    /// tests above cannot substitute for: it proves the metering itself
    /// registers the storm (the reviewers' finding was that byte-denominated
    /// fanout reads ~4 KB/s vs a 131 KB/s floor — 30x under — and the
    /// 100-sample window truncated the rate further, so the trigger would
    /// NEVER fire for the real profile).
    ///
    /// Asserts, in order:
    /// 1. the byte axis alone indeed CANNOT see the storm (the bug);
    /// 2. the message-count axis reads the storm's TRUE ~36/s despite the
    ///    saturated 100-sample buffer (the window fix — the old
    ///    min-window-clamped math reads ~19/s and this assertion fails);
    /// 3. the storm contract IS shed through the real sweep;
    /// 4. an equally-storming but recently-GET-read contract is NOT shed
    ///    (recency-aware candidacy);
    /// 5. a single large-state fan-out burst (5 MB × 30 targets, first report
    ///    seconds before the sweep) is NOT shed (sustained-pressure gate), so
    ///    one burst can never mass-evict a large-state contract.
    #[tokio::test]
    async fn storm_frequency_profile_crosses_cost_trigger_through_real_meter() {
        use crate::topology::meter::{AttributionSource, Meter, ResourceType};

        // --- the real meter, production window size (TopologyManager::new). ---
        let meter = Meter::new_with_window_size(100);
        let t0 = Instant::now();
        let junk = make_contract_key(1);
        let read_hot = make_contract_key(2);
        let burst = make_contract_key(3);

        // 10 minutes of storm: one fan-out dispatch every 1.6s to 58 targets
        // of a 121-byte payload (the FX2j profile), for junk AND read_hot
        // (read_hot proves protection comes from recency, not a smaller cost).
        for i in 0..375u64 {
            let at = t0 + Duration::from_millis(1600 * i);
            for id in [junk.id(), read_hot.id()] {
                meter.report(
                    &AttributionSource::Contract(*id),
                    ResourceType::BroadcastMessagesSent,
                    58.0,
                    at,
                );
                meter.report(
                    &AttributionSource::Contract(*id),
                    ResourceType::BroadcastFanoutCost,
                    121.0 * 58.0,
                    at,
                );
            }
        }
        let now = t0 + Duration::from_secs(600);
        // ONE large-state fan-out burst just before the sweep: 5 MB × 30.
        let burst_bytes = 5.0 * 1024.0 * 1024.0 * 30.0;
        meter.report(
            &AttributionSource::Contract(*burst.id()),
            ResourceType::BroadcastMessagesSent,
            30.0,
            now - Duration::from_secs(1),
        );
        meter.report(
            &AttributionSource::Contract(*burst.id()),
            ResourceType::BroadcastFanoutCost,
            burst_bytes,
            now - Duration::from_secs(1),
        );

        // --- the real reads + the shared axis assembly. ---
        let cpu = meter.contract_cost_rates(
            &ResourceType::ExecCpuMicros,
            now,
            cache::COST_RATE_MIN_WINDOW,
        );
        let fanout = meter.contract_cost_rates(
            &ResourceType::BroadcastFanoutCost,
            now,
            cache::COST_RATE_MIN_WINDOW,
        );
        let msgs = meter.contract_cost_rates(
            &ResourceType::BroadcastMessagesSent,
            now,
            cache::COST_RATE_MIN_WINDOW,
        );

        // (1) The byte axis alone cannot see the storm: the junk contract's
        // byte rate is far below the byte floor (this is the production bug —
        // ~4 KB/s vs 131 KB/s).
        let junk_byte_rate = fanout.1.get(junk.id()).copied().unwrap_or(0.0);
        assert!(
            junk_byte_rate < cache::BROADCAST_FANOUT_PRESSURE_FLOOR_BYTES_PER_SEC / 20.0,
            "precondition: the tiny-payload storm must be invisible to the \
             byte axis (got {junk_byte_rate} B/s) — otherwise this test no \
             longer reproduces the #4861 blind spot"
        );
        // (2) The message-count axis reads the TRUE sustained rate despite
        // count-truncation: ~36/s. Under the pre-fix min-window-clamped math
        // the saturated 100-sample buffer reads 100×58/300s ≈ 19/s and this
        // fails.
        let junk_msg_rate = msgs.1.get(junk.id()).copied().unwrap_or(0.0);
        assert!(
            junk_msg_rate > 30.0,
            "saturated-buffer rate must reflect the true storm frequency \
             (~36 msgs/s), got {junk_msg_rate}/s — the count-truncation \
             under-read is back"
        );
        // The burst contract is in the TOTAL but excluded from candidacy
        // (sustained gate: first report is 1s old).
        assert!(!msgs.1.contains_key(burst.id()));
        assert!(!fanout.1.contains_key(burst.id()));

        let axes = cache::build_cost_axes(cpu, fanout, msgs);

        // --- the real sweep over real hosted entries. ---
        let clock = crate::util::time_source::SharedMockTimeSource::new();
        let manager = HostingManager::with_time_source(
            DEFAULT_HOSTING_BUDGET_BYTES,
            std::sync::Arc::new(clock.clone()),
        );
        manager.record_contract_access(junk, 121, AccessType::Put);
        manager.record_contract_access(read_hot, 121, AccessType::Put);
        manager.record_contract_access(burst, 5 * 1024 * 1024, AccessType::Put);
        // The storm has run long past the cost window with no genuine access…
        clock.advance_time(cache::COST_RATE_MIN_WINDOW + Duration::from_secs(1));
        // …but read_hot was genuinely GET-read just now.
        manager.record_contract_access(read_hot, 121, AccessType::Get);

        let result = manager.sweep_expired_hosting_with_cost(&axes);
        let expired_keys: Vec<ContractKey> = result.expired.iter().map(|(k, _)| *k).collect();
        // (3) the storm contract is shed; (4) the recently-read one is not;
        // (5) the single-burst large-state contract is not.
        assert_eq!(
            expired_keys,
            vec![junk],
            "the sustained zero-demand storm contract must be the ONLY victim"
        );
        assert!(result.evicted_in_use_teardown.is_empty());
        assert!(!manager.is_hosting_contract(&junk));
        assert!(
            manager.is_hosting_contract(&read_hot),
            "an equally-storming but recently-read contract must survive"
        );
        assert!(
            manager.is_hosting_contract(&burst),
            "a single large fan-out burst must not mass-evict (sustained gate)"
        );
    }

    /// The no-cost degradation path: `sweep_expired_hosting()` (no axes) is
    /// the pure byte sweep and never cost-evicts, so every pre-existing
    /// caller is behaviorally unchanged.
    #[tokio::test]
    async fn sweep_without_cost_axes_never_cost_evicts() {
        let manager = HostingManager::new(DEFAULT_HOSTING_BUDGET_BYTES);
        let junk = make_contract_key(1);
        manager.record_contract_access(junk, 121, AccessType::Put);
        let result = manager.sweep_expired_hosting();
        assert!(result.expired.is_empty());
        assert!(manager.is_hosting_contract(&junk));
        assert_eq!(manager.hosting_cache_stats().cost_evictions_total, 0);
    }

    /// `subscribed_keys_in` (the bounded connection-drop-shadow lookup, #4642)
    /// returns only subscribed matches, and both caps hard-bound the work.
    #[test]
    fn subscribed_keys_in_matches_and_bounds() {
        use std::collections::HashSet;
        let manager = HostingManager::new(DEFAULT_HOSTING_BUDGET_BYTES);
        for seed in 1..=6u8 {
            manager.subscribe(make_contract_key(seed));
        }
        // wanted = instance-ids of two subscribed contracts + one we never
        // subscribed to (seed 99).
        let wanted: HashSet<ContractInstanceId> = [
            *make_contract_key(2).id(),
            *make_contract_key(4).id(),
            *make_contract_key(99).id(),
        ]
        .into_iter()
        .collect();

        // Generous caps: exactly the two subscribed matches (99 excluded).
        let mut got = manager.subscribed_keys_in(&wanted, 1024, 32);
        got.sort_by(|a, b| a.id().as_bytes().cmp(b.id().as_bytes()));
        assert_eq!(got, vec![make_contract_key(2), make_contract_key(4)]);

        // `max_matches` caps the result (bounds the expensive build count).
        assert_eq!(manager.subscribed_keys_in(&wanted, 1024, 1).len(), 1);

        // `scan_cap` bounds the scan itself: examine zero entries → find none.
        assert!(manager.subscribed_keys_in(&wanted, 0, 32).is_empty());
    }

    /// `subscribed_keys_in` excludes expired leases (same freshness semantics as
    /// `get_subscribed_contracts`), driven purely by the injected clock.
    #[tokio::test]
    async fn subscribed_keys_in_excludes_expired_leases() {
        use std::collections::HashSet;
        let clock = crate::util::time_source::SharedMockTimeSource::new();
        let manager = HostingManager::with_time_source(
            DEFAULT_HOSTING_BUDGET_BYTES,
            std::sync::Arc::new(clock.clone()),
        );
        let contract = make_contract_key(7);
        manager.subscribe(contract);
        let wanted: HashSet<ContractInstanceId> = [*contract.id()].into_iter().collect();
        assert_eq!(
            manager.subscribed_keys_in(&wanted, 1024, 32),
            vec![contract],
            "a fresh lease is matched"
        );

        clock.advance_time(SUBSCRIPTION_LEASE_DURATION + Duration::from_secs(1));
        assert!(
            manager.subscribed_keys_in(&wanted, 1024, 32).is_empty(),
            "an expired lease must not be matched"
        );
    }

    /// The injected time source (`with_time_source`) drives the manager's
    /// clock: subscription-lease expiry crosses `SUBSCRIPTION_LEASE_DURATION`
    /// purely by advancing the injected clock, with no wall time passing. This
    /// is the manager-level primitive that unblocks deterministic eviction/TTL
    /// simulations (#4642 piece A) — production hardcoded `InstantTimeSrc`, so a
    /// sim could not fast-forward the 8-minute gate.
    #[tokio::test]
    async fn test_with_time_source_lease_expiry_follows_injected_clock() {
        use crate::util::time_source::SharedMockTimeSource;

        let clock = SharedMockTimeSource::new();
        let manager = HostingManager::with_time_source(
            DEFAULT_HOSTING_BUDGET_BYTES,
            std::sync::Arc::new(clock.clone()),
        );
        let contract = make_contract_key(1);

        manager.subscribe(contract);
        assert!(
            manager.is_subscribed(&contract),
            "a fresh lease should be active"
        );

        // Advance the injected clock to just before the lease boundary.
        clock.advance_time(SUBSCRIPTION_LEASE_DURATION - Duration::from_secs(1));
        assert!(
            manager.is_subscribed(&contract),
            "lease should still be active one second before expiry"
        );

        // Cross the lease boundary purely via the injected clock.
        clock.advance_time(Duration::from_secs(2));
        assert!(
            !manager.is_subscribed(&contract),
            "lease should expire once the injected clock passes SUBSCRIPTION_LEASE_DURATION"
        );
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

        // The default constructor uses the in-code default, which is now
        // RAM-scaled (#4642 A2) rather than the fixed test constant — assert
        // against the production default fn so this doesn't flake on a
        // low-memory / cgroup-limited CI host (Codex #4644 review).
        let default_manager = HostingManager::default();
        assert_eq!(
            default_manager.hosting_budget_bytes(),
            default_hosting_budget_bytes()
        );
    }

    /// The demand path (A3): once the manager knows its own ring location, a
    /// repeated read of a contract trains the proximity prior with a
    /// `(distance, rate)` sample, and `distance_and_demand` yields a distance +
    /// a finite, non-negative demand. Without a known location, demand is
    /// neutral and no distance is available (eviction degrades to floor + LRU).
    #[test]
    fn test_demand_estimator_trains_from_repeated_reads() {
        let manager = HostingManager::new(DEFAULT_HOSTING_BUDGET_BYTES);
        let key = make_contract_key(1);

        // No own-location yet: no distance, neutral demand, no training.
        let (dist, demand) = manager.distance_and_demand(&key);
        assert!(dist.is_none(), "distance unknown until own location is set");
        assert_eq!(demand, demand::NEUTRAL_DEMAND);
        assert_eq!(manager.demand_estimator.read().len(), 0);

        // Learn our location, then read the contract twice. The second read has
        // non-zero residency (floored at 1s), so it yields a training sample.
        manager.set_own_location(Location::new(0.5));
        manager.record_contract_access(key, 1_000, AccessType::Get);
        manager.record_contract_access(key, 1_000, AccessType::Get);

        assert_eq!(
            manager.demand_estimator.read().len(),
            1,
            "a repeated read must train the proximity prior with one sample"
        );

        let (dist, demand) = manager.distance_and_demand(&key);
        assert!(dist.is_some(), "distance is known once own location is set");
        assert!(
            demand.is_finite() && demand >= 0.0,
            "predicted demand must be finite and non-negative, got {demand}"
        );
    }

    /// A PUT is a SEED at the manager level too: it must NOT feed the proximity
    /// prior (only reads are demand). Regression guard against wiring PUT into
    /// the training path.
    #[test]
    fn test_put_does_not_train_demand_estimator() {
        let manager = HostingManager::new(DEFAULT_HOSTING_BUDGET_BYTES);
        let key = make_contract_key(1);
        manager.set_own_location(Location::new(0.5));

        manager.record_contract_access(key, 1_000, AccessType::Put);
        manager.record_contract_access(key, 1_000, AccessType::Put);

        assert_eq!(
            manager.demand_estimator.read().len(),
            0,
            "PUT is a seed, not read-demand -- it must not train the prior"
        );
    }

    /// Local-client GET hit-rate counters (#4642 A3) are driven by explicit
    /// serve/forward signals from the client GET handler, not by cache
    /// membership, and accumulate monotonically per node.
    #[test]
    fn test_local_get_hit_rate_counters() {
        let manager = HostingManager::new(DEFAULT_HOSTING_BUDGET_BYTES);
        assert_eq!(manager.local_get_serves(), 0);
        assert_eq!(manager.local_get_forwards(), 0);

        manager.record_local_get_serve();
        manager.record_local_get_serve();
        manager.record_local_get_forward();

        assert_eq!(manager.local_get_serves(), 2, "two local serves recorded");
        assert_eq!(manager.local_get_forwards(), 1, "one forward recorded");
    }

    /// The local-serve path (`touch_hosting`) also trains the proximity prior, so
    /// the dominant read path for hot contracts is not invisible to the estimator
    /// (A3 — addresses the Codex/adversarial review finding that only network
    /// refetches trained the prior).
    #[test]
    fn test_touch_hosting_trains_demand_estimator() {
        let manager = HostingManager::new(DEFAULT_HOSTING_BUDGET_BYTES);
        let key = make_contract_key(1);
        manager.set_own_location(Location::new(0.5));

        // Host the contract (a new-entry insert yields no rate sample), then
        // serve it locally: the touch has non-zero residency so it trains.
        manager.record_contract_access(key, 1_000, AccessType::Get);
        let before = manager.demand_estimator.read().len();
        manager.touch_hosting(&key);
        assert!(
            manager.demand_estimator.read().len() > before,
            "a local-serve touch must train the proximity prior"
        );

        // Touching an unhosted contract is a no-op and trains nothing.
        let absent = make_contract_key(2);
        let n = manager.demand_estimator.read().len();
        manager.touch_hosting(&absent);
        assert_eq!(
            manager.demand_estimator.read().len(),
            n,
            "touching an absent contract must not train the prior"
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

    /// Characterizes the dashboard subscription snapshot: it must carry the
    /// per-contract freshness (`is_receiving_updates`) and demand (`in_use`)
    /// signals with the correct values.
    ///
    /// NOTE: this asserts the two-phase snapshot's OUTPUT, not deadlock-safety.
    /// The same-shard DashMap re-lock the two-phase split avoids
    /// (`is_receiving_updates` re-`.get()`s `active_subscriptions`) is prevented
    /// BY CONSTRUCTION — phase 2 runs only after the iterator's shard guards
    /// drop. It does NOT manifest in this single-threaded, no-concurrent-writer
    /// test: a re-inlined same-shard read would still pass here. The guard
    /// against re-inlining is the load-bearing comment in
    /// `dashboard_subscription_snapshot`, not this test.
    #[test]
    fn dashboard_subscription_snapshot_reports_freshness_and_demand() {
        let manager = HostingManager::new(DEFAULT_HOSTING_BUDGET_BYTES);
        // Network subscription only: receiving updates, but no local/downstream demand.
        let network_only = make_contract_key(1);
        manager.subscribe(network_only);
        // Network + client subscription: receiving updates AND in use.
        let in_use = make_contract_key(2);
        manager.subscribe(in_use);
        manager.add_client_subscription(in_use.id(), crate::client_events::ClientId::next());

        let snap = manager.dashboard_subscription_snapshot();
        let net = snap
            .iter()
            .find(|c| c.key == network_only)
            .expect("network-only subscription present");
        assert!(
            net.is_receiving_updates,
            "an active network subscription is receiving updates"
        );
        assert!(
            !net.in_use,
            "a network-only subscription has no local/downstream demand"
        );
        let used = snap
            .iter()
            .find(|c| c.key == in_use)
            .expect("in-use subscription present");
        assert!(used.is_receiving_updates);
        assert!(used.in_use, "a client subscription is real demand → in_use");
    }

    /// `is_eviction_eligible` gates the dashboard's "next to evict" badge on the
    /// sweep skip filter, which since 2026-07-08 is `!contract_in_use` ONLY (the
    /// `min_ttl` age gate was dropped — invariant 3). A freshly-accessed, not-in-
    /// use contract is now eligible (the change: it is no longer protected by a
    /// cold-start floor), while an in-use contract reads as NOT eligible so the
    /// badge does not mislabel it.
    #[test]
    fn dashboard_is_eviction_eligible_matches_sweep_skip_filter() {
        let manager = HostingManager::new(DEFAULT_HOSTING_BUDGET_BYTES);

        // A freshly-accessed contract with nothing pinning it is ELIGIBLE — there
        // is no longer a `min_ttl` cold-start floor to protect it.
        let evictable = make_contract_key(2);
        let pinned = make_contract_key(3);
        manager.record_contract_access(evictable, 100, AccessType::Get);
        manager.record_contract_access(pinned, 100, AccessType::Get);
        // Pin `pinned` with a local client subscription → contract_in_use true.
        let client = crate::client_events::ClientId::next();
        manager.add_client_subscription(pinned.id(), client);
        assert!(manager.contract_in_use(&pinned));
        assert!(!manager.contract_in_use(&evictable));

        let scores = manager.dashboard_hosting_scores();
        let ev = scores
            .iter()
            .find(|s| s.key == evictable)
            .expect("evictable contract present");
        let pin = scores
            .iter()
            .find(|s| s.key == pinned)
            .expect("pinned contract present");
        assert!(
            manager.is_eviction_eligible(ev),
            "fresh and not in use → eligible (the real next victim; no TTL floor)"
        );
        assert!(
            !manager.is_eviction_eligible(pin),
            "in use → NOT eligible (sweep skips it)"
        );
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
        assert!(
            manager
                .add_downstream_subscriber(&contract, downstream.clone())
                .was_accepted()
        );

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
    // Governance Beneficiary-Count Accessor Tests
    // =========================================================================
    //
    // These tests pin `downstream_subscriber_count` and
    // `local_client_count`, the live-beneficiary accessors governance
    // reads each reaper tick. Time is controlled deterministically by
    // inserting explicit `tokio::time::Instant` lease timestamps into
    // `downstream_subscribers` (the same technique the expiry-sweep
    // tests above use) — under `#[tokio::test(start_paused = true)]`
    // tokio's `Instant` clock is frozen, so a timestamp computed as
    // `Instant::now() - D` is observed exactly `D` in the past by the
    // read inside the accessor.

    /// Active (recently-renewed) downstream leases are counted.
    #[tokio::test(start_paused = true)]
    async fn downstream_subscriber_count_counts_active_leases() {
        let manager = HostingManager::new(DEFAULT_HOSTING_BUDGET_BYTES);
        let contract = make_contract_key(1);
        let instance_id = *contract.id();

        assert_eq!(manager.downstream_subscriber_count(&instance_id), 0);

        manager.add_downstream_subscriber(&contract, make_peer_key(10));
        manager.add_downstream_subscriber(&contract, make_peer_key(20));

        assert_eq!(
            manager.downstream_subscriber_count(&instance_id),
            2,
            "two freshly-added downstream leases must be counted"
        );
    }

    /// A stale-but-unswept lease (renewed longer ago than
    /// `SUBSCRIPTION_LEASE_DURATION`) is NOT counted, even though the
    /// expiry sweep has not run to prune it. The accessor must compute
    /// liveness itself, not depend on the sweep.
    #[tokio::test(start_paused = true)]
    async fn downstream_subscriber_count_excludes_stale_unswept_lease() {
        let manager = HostingManager::new(DEFAULT_HOSTING_BUDGET_BYTES);
        let contract = make_contract_key(1);
        let instance_id = *contract.id();
        let peer = make_peer_key(10);

        manager.add_downstream_subscriber(&contract, peer.clone());
        assert_eq!(manager.downstream_subscriber_count(&instance_id), 1);

        // Backdate the lease past the lease duration WITHOUT calling
        // expire_stale_downstream_subscribers.
        if let Some(mut peers) = manager.downstream_subscribers.get_mut(&contract) {
            peers.insert(
                peer,
                Instant::now() - SUBSCRIPTION_LEASE_DURATION - Duration::from_secs(1),
            );
        }

        // The map entry still exists (no sweep ran)...
        assert!(manager.downstream_subscribers.contains_key(&contract));
        // ...but the stale lease must not be counted.
        assert_eq!(
            manager.downstream_subscriber_count(&instance_id),
            0,
            "a stale-but-unswept lease must not count as a live beneficiary"
        );
    }

    /// The exact `SUBSCRIPTION_LEASE_DURATION` boundary is NOT counted
    /// (the liveness check is strict `<`, so a lease aged exactly the
    /// lease duration has just expired).
    #[tokio::test(start_paused = true)]
    async fn downstream_subscriber_count_boundary_is_exclusive() {
        let manager = HostingManager::new(DEFAULT_HOSTING_BUDGET_BYTES);
        let contract = make_contract_key(1);
        let instance_id = *contract.id();
        let peer = make_peer_key(10);

        manager.add_downstream_subscriber(&contract, peer.clone());

        // Exactly at the boundary: age == SUBSCRIPTION_LEASE_DURATION.
        if let Some(mut peers) = manager.downstream_subscribers.get_mut(&contract) {
            peers.insert(peer.clone(), Instant::now() - SUBSCRIPTION_LEASE_DURATION);
        }
        assert_eq!(
            manager.downstream_subscriber_count(&instance_id),
            0,
            "a lease aged exactly SUBSCRIPTION_LEASE_DURATION is expired (strict <)"
        );

        // Just inside the boundary: still live.
        if let Some(mut peers) = manager.downstream_subscribers.get_mut(&contract) {
            peers.insert(
                peer,
                Instant::now() - SUBSCRIPTION_LEASE_DURATION + Duration::from_millis(1),
            );
        }
        assert_eq!(
            manager.downstream_subscriber_count(&instance_id),
            1,
            "a lease just inside SUBSCRIPTION_LEASE_DURATION is still live"
        );
    }

    /// Downstream counts are aggregated by `ContractInstanceId`
    /// regardless of the `ContractKey`'s code-hash half (governance keys
    /// on instance id, while `downstream_subscribers` keys on the full
    /// `ContractKey`). The accessor matches `entry.key().id()` and sums,
    /// so all peers under any key sharing the instance id are counted.
    #[tokio::test(start_paused = true)]
    async fn downstream_subscriber_count_sums_across_keys_sharing_instance_id() {
        let manager = HostingManager::new(DEFAULT_HOSTING_BUDGET_BYTES);
        let instance_id = ContractInstanceId::new([7; 32]);
        // Two ContractKeys with the SAME instance id but different code
        // hashes. (Note: ContractKey equality is by instance id, so these
        // share a `downstream_subscribers` map entry — the accessor's
        // instance-id matching must count every peer under that id.)
        let key_a = ContractKey::from_id_and_code(instance_id, CodeHash::new([1; 32]));
        let key_b = ContractKey::from_id_and_code(instance_id, CodeHash::new([2; 32]));
        assert_eq!(key_a.id(), key_b.id());

        manager.add_downstream_subscriber(&key_a, make_peer_key(10));
        manager.add_downstream_subscriber(&key_a, make_peer_key(20));
        manager.add_downstream_subscriber(&key_b, make_peer_key(30));

        assert_eq!(
            manager.downstream_subscriber_count(&instance_id),
            3,
            "downstream count must count all peers under any key sharing the instance id"
        );
    }

    /// `local_client_count`: 0 / N clients / after a client removal.
    #[tokio::test(start_paused = true)]
    async fn local_client_count_zero_n_and_after_removal() {
        use crate::client_events::ClientId;

        let manager = HostingManager::new(DEFAULT_HOSTING_BUDGET_BYTES);
        let instance_id = ContractInstanceId::new([5; 32]);

        // 0 clients.
        assert_eq!(manager.local_client_count(&instance_id), 0);

        // N clients.
        let c1 = ClientId::next();
        let c2 = ClientId::next();
        let c3 = ClientId::next();
        manager.add_client_subscription(&instance_id, c1);
        manager.add_client_subscription(&instance_id, c2);
        manager.add_client_subscription(&instance_id, c3);
        assert_eq!(manager.local_client_count(&instance_id), 3);

        // After a removal.
        manager.remove_client_subscription(&instance_id, c2);
        assert_eq!(
            manager.local_client_count(&instance_id),
            2,
            "removing one client must decrement the live local-client count"
        );
    }

    /// `beneficiary_counts` bulk accessor must produce the same value as
    /// the per-contract `LOCAL*locals + FORWARDED*downstreams`
    /// computation, including lease-validity filtering and summing
    /// across keys sharing an instance id.
    #[tokio::test(start_paused = true)]
    async fn beneficiary_counts_matches_per_contract_computation() {
        use crate::client_events::ClientId;
        const LOCAL: f64 = 1.0;
        const FORWARDED: f64 = 0.1;

        let manager = HostingManager::new(DEFAULT_HOSTING_BUDGET_BYTES);

        // Contract A: 2 local clients + 1 live downstream.
        let inst_a = ContractInstanceId::new([1; 32]);
        let key_a = ContractKey::from_id_and_code(inst_a, CodeHash::new([10; 32]));
        manager.add_client_subscription(&inst_a, ClientId::next());
        manager.add_client_subscription(&inst_a, ClientId::next());
        manager.add_downstream_subscriber(&key_a, make_peer_key(1));

        // Contract B: downstream across two keys sharing the instance id,
        // one of which is stale.
        let inst_b = ContractInstanceId::new([2; 32]);
        let key_b1 = ContractKey::from_id_and_code(inst_b, CodeHash::new([20; 32]));
        let key_b2 = ContractKey::from_id_and_code(inst_b, CodeHash::new([21; 32]));
        manager.add_downstream_subscriber(&key_b1, make_peer_key(2));
        let stale_peer = make_peer_key(3);
        manager.add_downstream_subscriber(&key_b2, stale_peer.clone());
        if let Some(mut peers) = manager.downstream_subscribers.get_mut(&key_b2) {
            peers.insert(
                stale_peer,
                Instant::now() - SUBSCRIPTION_LEASE_DURATION - Duration::from_secs(1),
            );
        }

        let bulk = manager.beneficiary_counts(LOCAL, FORWARDED);

        // Compare against the per-contract accessors for every instance
        // id present in the bulk map.
        for inst in [inst_a, inst_b] {
            let expected = LOCAL * manager.local_client_count(&inst) as f64
                + FORWARDED * manager.downstream_subscriber_count(&inst) as f64;
            let got = bulk.get(&inst).copied().unwrap_or(0.0);
            assert!(
                (got - expected).abs() < 1e-12,
                "instance {inst}: bulk {got} != per-contract {expected}"
            );
        }

        // Concrete values: A = 1*2 + 0.1*1 = 2.1; B = 0.1*1 = 0.1.
        assert!((bulk[&inst_a] - 2.1).abs() < 1e-12);
        assert!((bulk[&inst_b] - 0.1).abs() < 1e-12);
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
    /// `contracts_needing_renewal` section 1 now gates renewal on
    /// `contract_in_use`, so including `is_subscribed` here would make a
    /// subscribed contract renew its own lease indefinitely — a self-renewing
    /// loop and an effectively unbounded GC exemption (AGENTS.md
    /// cleanup-exemption rule). Local-client subscriptions and downstream-peer
    /// subscribers are both time-bounded and remain in the predicate.
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

    /// Behavioural regression for the #4473 UPDATE auto-fetch gate.
    ///
    /// On the `AutoFetchReason::InboundRelay` path,
    /// `OpManager::try_auto_fetch_contract` self-heal-fetches a contract only
    /// when `self.ring.contract_in_use(key)` — i.e. a local client or a
    /// downstream peer subscriber depends on it. (The `Originator` path is
    /// demand-driven and bypasses this gate; see `AutoFetchReason`.) Before the
    /// gate, an inbound UPDATE/broadcast for a phantom-interest contract (the
    /// #4404 placement-migration after-effect: stale interest with no
    /// subscriber) spawned a `fetch_contract` sub-op every time the 5-minute
    /// cooldown lapsed — the residual #4473 churn. This drives the predicate
    /// the gate keys on through real subscription registration/teardown and
    /// asserts the decision the gate makes at each step:
    ///   phantom (no subscriber)            → gate skips  (would NOT auto-fetch)
    ///   live local client                  → gate passes (WOULD auto-fetch)
    ///   live downstream subscriber         → gate passes (WOULD auto-fetch)
    ///   upstream network subscription only → gate skips  (would NOT auto-fetch)
    /// and that teardown returns the decision to "skip", so the gate re-arms
    /// only while a real subscriber exists.
    #[test]
    fn auto_fetch_gate_skips_phantom_contracts_and_passes_served_ones() {
        let manager = HostingManager::new(DEFAULT_HOSTING_BUDGET_BYTES);

        // Phantom: interest carried with no subscriber of any kind. The gate
        // (`!contract_in_use`) skips it — no auto-fetch sub-op spawned, which is
        // the whole point of the #4473 fix.
        let phantom = make_contract_key(40);
        assert!(
            !manager.contract_in_use(&phantom),
            "phantom contract (no subscriber) MUST be gated out of auto-fetch (#4473)"
        );

        // A live local client makes the contract genuinely needed → gate passes.
        let client_served = make_contract_key(41);
        let client = crate::client_events::ClientId::next();
        manager.add_client_subscription(client_served.id(), client);
        assert!(
            manager.contract_in_use(&client_served),
            "a contract with a live local-client subscription MUST pass the \
             auto-fetch gate so genuinely-needed state still self-heals"
        );
        // Teardown re-arms the skip: once the client leaves, the contract is
        // phantom again and must NOT keep auto-fetching.
        manager.remove_client_subscription(client_served.id(), client);
        assert!(
            !manager.contract_in_use(&client_served),
            "auto-fetch gate must re-close once the last client unsubscribes"
        );

        // A downstream peer subscriber likewise makes the fetch legitimate.
        let downstream_served = make_contract_key(42);
        let peer = make_peer_key(9);
        manager.add_downstream_subscriber(&downstream_served, peer.clone());
        assert!(
            manager.contract_in_use(&downstream_served),
            "a contract with a downstream subscriber MUST pass the auto-fetch gate"
        );
        manager.remove_downstream_subscriber(&downstream_served, &peer);
        assert!(
            !manager.contract_in_use(&downstream_served),
            "auto-fetch gate must re-close once the last downstream subscriber leaves"
        );

        // An upstream network subscription ALONE is deliberately excluded: it
        // renews its lease unboundedly (see `contract_in_use` rustdoc) and is
        // meant to be torn down, not kept alive by self-heal fetches. So a
        // contract we are merely network-subscribed to stays gated out.
        let upstream_only = make_contract_key(43);
        manager.subscribe(upstream_only);
        assert!(manager.is_subscribed(&upstream_only));
        assert!(
            !manager.contract_in_use(&upstream_only),
            "an upstream-network-subscription-only contract MUST stay gated out \
             of auto-fetch (#4473): keeping it would re-introduce the phantom churn"
        );
        manager.unsubscribe(&upstream_only);
    }

    /// Behavioural regression for the #4610 summarize/broadcast storm.
    ///
    /// The inbound relay-SUBSCRIBE / placement-migration path marks a contract
    /// `contract_in_use` (a downstream-subscriber renewal) WITHOUT its state
    /// ever being fetched/stored — a "phantom" (interested-but-stateless)
    /// contract (#4440). Before #4610 the summarize gate
    /// (`is_hosting_contract || contract_in_use`) and the broadcast gate
    /// `should_broadcast_contract` both passed such a contract, so the periodic
    /// interest-sync / broadcast loops called `summarize_contract_state` for it
    /// every heartbeat — a full fetch that always failed "Contract state not
    /// found in store" (~70-80/sec at scale; CPU pegged, memory to the 2G cap).
    ///
    /// The fix adds a `contract_state_present` (actual on-disk state) term,
    /// composed with the host-or-serve check in the single
    /// `should_summarize_or_broadcast` predicate that BOTH gate call sites use.
    /// This drives the exact storm signature through real subscriber
    /// registration + a real redb state store and asserts the COMPOSED gate
    /// decision (not just the leaf signals) for each case:
    ///   - a phantom (in_use, no stored state) → the OLD `(is_hosting||in_use)`
    ///     predicate is TRUE but the composed gate is FALSE → SKIPPED;
    ///   - a stateful in-use contract → composed gate TRUE → summarized/broadcast;
    ///   - an evicted-but-on-disk contract (state present, NOT in the hosting
    ///     cache, in_use) → composed gate TRUE → still summarized — the
    ///     regression the state-STORE check prevents (keying on
    ///     `is_hosting_contract`, i.e. cache membership, would wrongly drop it).
    ///
    /// Asserting the COMPOSED predicate (not the individual leaf signals) is what
    /// makes this test catch an `&&`→`||` miswire in
    /// `should_summarize_or_broadcast`: with `||`, the phantom (in_use but
    /// stateless) would pass and this test fails. Verified by flipping the
    /// operator. A leaf-only assertion would NOT catch that.
    #[cfg(feature = "redb")]
    #[tokio::test]
    async fn summarize_gate_skips_stateless_phantom_keeps_stateful_4610() {
        use freenet_stdlib::prelude::WrappedState;

        let manager = HostingManager::new(DEFAULT_HOSTING_BUDGET_BYTES);

        // Real redb state store, like production.
        let temp_dir = tempfile::TempDir::new().unwrap();
        let storage = crate::contract::storages::ReDb::new(temp_dir.path())
            .await
            .unwrap();

        let peer = make_peer_key(7);

        // Phantom: a remote peer relay-subscribed (downstream subscriber) but
        // we NEVER fetched/stored its state.
        let phantom = make_contract_key(70);
        manager.add_downstream_subscriber(&phantom, peer.clone());

        // Stateful: state actually stored AND in use.
        let stateful = make_contract_key(71);
        storage
            .store_state_sync(&stateful, WrappedState::new(vec![1, 2, 3, 4]))
            .unwrap();
        manager.add_downstream_subscriber(&stateful, peer.clone());

        // Evicted-but-on-disk: state stored, in use, but NOT in the hosting
        // cache (never `host_contract`-ed). The gate must NOT skip this one —
        // keying on `is_hosting_contract` (cache membership) would wrongly drop
        // it, so this is the regression the state-store check prevents.
        let evicted_on_disk = make_contract_key(72);
        storage
            .store_state_sync(&evicted_on_disk, WrappedState::new(vec![9, 9, 9]))
            .unwrap();
        manager.add_downstream_subscriber(&evicted_on_disk, peer.clone());

        manager.set_storage(storage);

        // Preconditions documenting the storm signature: the OLD predicate
        // (is_hosting || in_use) is TRUE for the phantom — that is exactly why
        // it stormed before the fix — yet it has no stored state.
        assert!(
            manager.contract_in_use(&phantom),
            "phantom must look in-use (downstream subscriber) — the pre-#4610 \
             gate would have summarized it every heartbeat"
        );
        assert!(
            !manager.contract_state_present(&phantom),
            "phantom precondition: no stored state"
        );
        assert!(
            !manager.is_hosting_contract(&evicted_on_disk),
            "evicted precondition: not in the hosting cache"
        );
        assert!(
            manager.contract_state_present(&evicted_on_disk),
            "evicted precondition: state still on disk"
        );

        // The actual gate decision (composed predicate, shared by both call
        // sites). Asserting THIS — not the leaf signals — is what catches an
        // `&&`→`||` miswire: with `||`, the phantom would pass.
        assert!(
            !manager.should_summarize_or_broadcast(&phantom),
            "#4610: a phantom (in_use but stateless) contract MUST be gated OUT \
             of summarize/broadcast. If this fails after an edit, check for an \
             `&&`→`||` miswire in should_summarize_or_broadcast."
        );
        assert!(
            manager.should_summarize_or_broadcast(&stateful),
            "a hosted/in-use contract WITH stored state MUST be summarized/broadcast"
        );
        assert!(
            manager.should_summarize_or_broadcast(&evicted_on_disk),
            "#4610 regression guard: an evicted-but-on-disk contract (state on \
             disk, in_use, NOT in the hosting cache) MUST stay summarized — the \
             gate reads the state STORE, not the hosting cache"
        );
    }

    /// #4610 fallback: with NO storage handle set (the pre-startup window, before
    /// `set_storage`), `contract_state_present` MUST return `true` (assume
    /// present). A refactor that returned `false` here would drop EVERY contract
    /// from summarize/broadcast during the startup window. So the composed gate
    /// reduces to the pre-#4610 `(is_hosting || in_use)` behavior until storage
    /// is attached.
    #[test]
    fn contract_state_present_assumes_present_without_storage_handle() {
        let manager = HostingManager::new(DEFAULT_HOSTING_BUDGET_BYTES);
        let key = make_contract_key(80);
        assert!(
            manager.contract_state_present(&key),
            "without a storage handle, contract_state_present must assume present \
             so startup-window contracts are not all dropped"
        );
        // And the composed gate still passes for an in-use contract pre-storage.
        let peer = make_peer_key(8);
        manager.add_downstream_subscriber(&key, peer);
        assert!(
            manager.should_summarize_or_broadcast(&key),
            "pre-storage, the gate must fall back to (is_hosting || in_use)"
        );
    }

    // NOTE: no sqlite-backend fallback test. The sqlite store has no cheap
    // *synchronous* existence check, so `contract_state_present` returns `true`
    // there (the `#[cfg(not(feature = "redb"))]` branch, preserving pre-#4610
    // behavior). A `#[cfg(all(feature = "sqlite", not(feature = "redb")))]` test
    // would never run: the sqlite backend does not compile on main (unrelated
    // pre-existing errors, e.g. missing `get_user_secrets_index`) and there is
    // no sqlite CI lane, so such a test would be unverifiable dead code.

    /// Behavioural regression for #4612 (phantom-hosting root cause) as
    /// AMENDED by step 10 §1c (#4770 Drop arm neutralized): the reconcile sweep
    /// enforces `in-use ⇒ has-state` by REPAIR-fetching a downstream-registered
    /// contract with no stored state (bounded one-shot per cooldown window), and
    /// after `MAX_PHANTOM_REPAIR_ATTEMPTS` failures it STOPS — it does NOT drop
    /// the registration (that cycling Drop arm was removed because
    /// register-after-state makes a genuine phantom unrepresentable, so dropping
    /// would only churn). Covers all transitions through a real redb store and
    /// the injected clock:
    ///   - phantom (downstream, stateless) → `Fetch`, exactly once per
    ///     cooldown window (a retry never overlaps the in-flight fetch);
    ///   - attempts exhausted → NO action, and the downstream registration is
    ///     retained (`contract_in_use` stays true — never dropped);
    ///   - repair success (state appears between passes, whatever wrote it)
    ///     → tracking cleared, no further fetches, contract keeps its
    ///     downstream subscribers;
    ///   - a stateful in-use contract is never flagged.
    #[cfg(feature = "redb")]
    #[tokio::test]
    async fn phantom_in_use_reconcile_fetches_then_stops_no_drop_4612() {
        use freenet_stdlib::prelude::WrappedState;

        let clock = crate::util::time_source::SharedMockTimeSource::new();
        let manager = HostingManager::with_time_source(
            DEFAULT_HOSTING_BUDGET_BYTES,
            std::sync::Arc::new(clock.clone()),
        );

        let temp_dir = tempfile::TempDir::new().unwrap();
        let storage = crate::contract::storages::ReDb::new(temp_dir.path())
            .await
            .unwrap();
        // Keep a handle to write state mid-test (repair-success case).
        let store_handle = storage.clone();

        let peer = make_peer_key(7);

        // Phantom: relay-registered downstream subscriber, state never fetched.
        let phantom = make_contract_key(70);
        manager.add_downstream_subscriber(&phantom, peer.clone());

        // Stateful in-use contract: must never be flagged.
        let stateful = make_contract_key(71);
        storage
            .store_state_sync(&stateful, WrappedState::new(vec![1, 2, 3]))
            .unwrap();
        manager.add_downstream_subscriber(&stateful, peer.clone());

        manager.set_storage(storage);

        assert!(
            manager.contract_in_use(&phantom) && !manager.contract_state_present(&phantom),
            "phantom precondition: in-use with no stored state (#4612)"
        );

        // Attempt 1, then nothing until the cooldown elapses.
        assert_eq!(
            manager.reconcile_phantom_in_use(8),
            vec![PhantomRepair::Fetch(phantom)],
            "a phantom gets a repair fetch; the stateful contract is not flagged"
        );
        assert!(
            manager.reconcile_phantom_in_use(8).is_empty(),
            "within the cooldown no second fetch is issued (never overlap the \
             in-flight sub-op GET)"
        );

        // Attempts 2 and 3, one per cooldown window.
        for attempt in 2..=MAX_PHANTOM_REPAIR_ATTEMPTS {
            clock.advance_time(PHANTOM_REPAIR_COOLDOWN + Duration::from_secs(1));
            assert_eq!(
                manager.reconcile_phantom_in_use(8),
                vec![PhantomRepair::Fetch(phantom)],
                "attempt {attempt} fires after the cooldown"
            );
        }

        // Attempts exhausted → the sweep STOPS (no Drop — step 10 §1c). The
        // downstream registration is RETAINED, so `contract_in_use` stays true;
        // the sweep never churns a registration it can't repair.
        clock.advance_time(PHANTOM_REPAIR_COOLDOWN + Duration::from_secs(1));
        assert!(
            manager.reconcile_phantom_in_use(8).is_empty(),
            "after MAX_PHANTOM_REPAIR_ATTEMPTS failed fetches the sweep emits \
             nothing — the #4770 cycling Drop arm is neutralized"
        );
        assert!(
            manager.contract_in_use(&phantom),
            "the downstream registration is retained (never dropped) — step 10 §1c"
        );
        assert!(
            manager.reconcile_phantom_in_use(8).is_empty(),
            "still nothing on the next pass — exhausted, but not dropped"
        );

        // Repair-success path: state appears (here via a direct store write,
        // in production via the sub-op GET / an UPDATE auto-fetch / a PUT)
        // after the first attempt → tracking cleared, no more fetches, and
        // the contract keeps its downstream subscribers.
        let repaired = make_contract_key(72);
        manager.add_downstream_subscriber(&repaired, peer.clone());
        assert_eq!(
            manager.reconcile_phantom_in_use(8),
            vec![PhantomRepair::Fetch(repaired)]
        );
        store_handle
            .store_state_sync(&repaired, WrappedState::new(vec![9, 9]))
            .unwrap();
        clock.advance_time(PHANTOM_REPAIR_COOLDOWN + Duration::from_secs(1));
        assert!(
            manager.reconcile_phantom_in_use(8).is_empty(),
            "once state is present the contract is no longer a phantom"
        );
        assert!(
            manager.contract_in_use(&repaired),
            "a repaired contract keeps its downstream subscribers (now a real host)"
        );

        // The stateful contract was never touched throughout.
        assert!(manager.contract_in_use(&stateful));
    }

    /// Fix 6 regression: neutralizing the #4770 Drop arm means an exhausted
    /// phantom's `phantom_repair` entry is retained after attempts run out. Since
    /// `reconcile_phantom_in_use` only visits `downstream_subscribers`, once that
    /// phantom's downstream subscription later expires the key would never be
    /// revisited and its tracking entry would leak permanently. The sweep must
    /// prune `phantom_repair` entries whose key is no longer downstream-registered.
    #[cfg(feature = "redb")]
    #[tokio::test]
    async fn exhausted_phantom_repair_pruned_when_downstream_expires() {
        let clock = crate::util::time_source::SharedMockTimeSource::new();
        let manager = HostingManager::with_time_source(
            DEFAULT_HOSTING_BUDGET_BYTES,
            std::sync::Arc::new(clock.clone()),
        );
        let temp_dir = tempfile::TempDir::new().unwrap();
        let storage = crate::contract::storages::ReDb::new(temp_dir.path())
            .await
            .unwrap();
        manager.set_storage(storage);

        let peer = make_peer_key(13);
        let phantom = make_contract_key(90);
        manager.add_downstream_subscriber(&phantom, peer.clone());

        // Exhaust the repair attempts (leaves a phantom_repair entry at MAX).
        assert_eq!(
            manager.reconcile_phantom_in_use(8),
            vec![PhantomRepair::Fetch(phantom)]
        );
        for attempt in 2..=MAX_PHANTOM_REPAIR_ATTEMPTS {
            clock.advance_time(PHANTOM_REPAIR_COOLDOWN + Duration::from_secs(1));
            assert_eq!(
                manager.reconcile_phantom_in_use(8),
                vec![PhantomRepair::Fetch(phantom)],
                "attempt {attempt}"
            );
        }
        clock.advance_time(PHANTOM_REPAIR_COOLDOWN + Duration::from_secs(1));
        assert!(
            manager.reconcile_phantom_in_use(8).is_empty(),
            "attempts exhausted → sweep stops"
        );
        assert!(
            manager.phantom_repair.get(&phantom).is_some(),
            "precondition: the exhausted phantom_repair entry is still tracked"
        );

        // The downstream subscription expires (peer unsubscribed / lease lapsed).
        manager.downstream_subscribers.remove(&phantom);
        assert!(!manager.contract_in_use(&phantom));

        // The next sweep must prune the now-orphaned phantom_repair entry.
        assert!(manager.reconcile_phantom_in_use(8).is_empty());
        assert!(
            manager.phantom_repair.get(&phantom).is_none(),
            "phantom_repair entry must be pruned once the downstream expired (Fix 6) — \
             no permanent per-node leak"
        );

        // Belt-and-suspenders: a later re-registration of the SAME key starts a
        // FRESH repair cycle (attempts=1 → Fetch), proving the exhausted entry
        // did not leak and leave the key stuck-exhausted.
        manager.add_downstream_subscriber(&phantom, peer);
        clock.advance_time(PHANTOM_REPAIR_COOLDOWN + Duration::from_secs(1));
        assert_eq!(
            manager.reconcile_phantom_in_use(8),
            vec![PhantomRepair::Fetch(phantom)],
            "a re-registered downstream starts a fresh repair cycle, not stuck-exhausted"
        );
    }

    /// Fix C regression: the Fix-6 prune only fires when the downstream EXPIRES.
    /// A phantom whose downstream keeps RENEWING (lease refreshable) would
    /// otherwise be a permanent GC blind spot + advertised dead-end. The
    /// exhausted-phantom exemption is now absolute-age-bounded: once the phantom
    /// exceeds `PHANTOM_ABSOLUTE_MAX_AGE` the sweep DROPS it, even though its
    /// downstream is still registered. Non-cycling because a re-registration goes
    /// through `finalize_host_subscribe` (fetch-first).
    #[cfg(feature = "redb")]
    #[tokio::test]
    async fn exhausted_phantom_dropped_after_absolute_age_bound() {
        let clock = crate::util::time_source::SharedMockTimeSource::new();
        let manager = HostingManager::with_time_source(
            DEFAULT_HOSTING_BUDGET_BYTES,
            std::sync::Arc::new(clock.clone()),
        );
        let temp_dir = tempfile::TempDir::new().unwrap();
        let storage = crate::contract::storages::ReDb::new(temp_dir.path())
            .await
            .unwrap();
        manager.set_storage(storage);

        let peer = make_peer_key(14);
        let phantom = make_contract_key(91);
        manager.add_downstream_subscriber(&phantom, peer.clone());

        // Exhaust the repair attempts (~4 × 60s, well under the 32-min bound).
        assert_eq!(
            manager.reconcile_phantom_in_use(8),
            vec![PhantomRepair::Fetch(phantom)]
        );
        for _ in 2..=MAX_PHANTOM_REPAIR_ATTEMPTS {
            clock.advance_time(PHANTOM_REPAIR_COOLDOWN + Duration::from_secs(1));
            assert_eq!(
                manager.reconcile_phantom_in_use(8),
                vec![PhantomRepair::Fetch(phantom)]
            );
        }
        clock.advance_time(PHANTOM_REPAIR_COOLDOWN + Duration::from_secs(1));
        // Exhausted but still YOUNG: NOT dropped (the downstream keeps renewing so
        // the Fix-6 downstream-expiry prune never fires).
        assert!(
            manager.reconcile_phantom_in_use(8).is_empty(),
            "a young exhausted phantom is not dropped (only fetching stops)"
        );
        assert!(manager.contract_in_use(&phantom));

        // The downstream keeps renewing (refresh its lease) so it never expires.
        manager.add_downstream_subscriber(&phantom, peer);
        // Age crosses the absolute bound.
        clock.advance_time(PHANTOM_ABSOLUTE_MAX_AGE + Duration::from_secs(1));
        assert_eq!(
            manager.reconcile_phantom_in_use(8),
            vec![PhantomRepair::Drop(phantom)],
            "an exhausted phantom older than PHANTOM_ABSOLUTE_MAX_AGE is dropped even while \
             its downstream keeps renewing (review Fix C — the exemption is time-bounded)"
        );
        // Applying the drop clears contract_in_use and removes the tracking.
        assert_eq!(manager.drop_phantom_downstream(&phantom), 1);
        assert!(
            !manager.contract_in_use(&phantom),
            "dropping the aged phantom clears contract_in_use"
        );
        assert!(manager.phantom_repair.get(&phantom).is_none());
    }

    /// Behavioural regression for the step-10 §1d phantom falsifier gauge:
    /// `phantom_in_use_count` reports the number of contracts registered as
    /// in-use via a downstream subscriber with NO state on disk
    /// (`contract_in_use && !contract_state_present`). This is the counter that
    /// should read 0 in production after the register-after-state fix.
    ///
    /// A stateless downstream registration (the pre-fix relay phantom) is
    /// counted; once state lands it is not; a stateful downstream host is never
    /// counted; and a client-subscription-only stateless contract is deliberately
    /// NOT counted (no full-key index — those phantoms are OUT OF SCOPE for this
    /// gauge; their cleanup is deferred to a future PR).
    #[cfg(feature = "redb")]
    #[tokio::test]
    async fn phantom_in_use_count_flags_stateless_downstream_only() {
        use freenet_stdlib::prelude::WrappedState;

        let manager = HostingManager::new(DEFAULT_HOSTING_BUDGET_BYTES);
        let temp_dir = tempfile::TempDir::new().unwrap();
        let storage = crate::contract::storages::ReDb::new(temp_dir.path())
            .await
            .unwrap();
        let store_handle = storage.clone();
        manager.set_storage(storage);

        let peer = make_peer_key(11);

        assert_eq!(
            manager.phantom_in_use_count(),
            0,
            "no registrations → no phantoms"
        );

        // Stateless downstream registration = the pre-fix relay phantom.
        let phantom = make_contract_key(80);
        manager.add_downstream_subscriber(&phantom, peer.clone());
        assert!(manager.contract_in_use(&phantom) && !manager.contract_state_present(&phantom));
        assert_eq!(
            manager.phantom_in_use_count(),
            1,
            "a downstream registration with no state on disk is a phantom"
        );

        // Stateful downstream host is never a phantom.
        let stateful = make_contract_key(81);
        store_handle
            .store_state_sync(&stateful, WrappedState::new(vec![1, 2, 3]))
            .unwrap();
        manager.add_downstream_subscriber(&stateful, peer.clone());
        assert_eq!(
            manager.phantom_in_use_count(),
            1,
            "a stateful downstream host is not counted; only the stateless one is"
        );

        // Once state lands for the phantom (repair fetch / UPDATE / PUT), the
        // falsifier drops back to 0 — this is what the fix must achieve in prod.
        store_handle
            .store_state_sync(&phantom, WrappedState::new(vec![4, 5]))
            .unwrap();
        assert_eq!(
            manager.phantom_in_use_count(),
            0,
            "once state is present the contract is no longer a phantom"
        );

        // A client-subscription-only stateless contract is in-use but is NOT
        // counted by this gauge (it is keyed by instance id with no full-key
        // index; those phantoms are out of scope and their cleanup is deferred).
        let client_only = make_contract_key(82);
        manager.add_client_subscription(client_only.id(), crate::client_events::ClientId::next());
        assert!(
            manager.contract_in_use(&client_only) && !manager.contract_state_present(&client_only)
        );
        assert_eq!(
            manager.phantom_in_use_count(),
            0,
            "client-subscription-only phantoms are out of scope for this gauge"
        );
    }

    /// #4612 producer bound: `max_fetches` caps the fetches emitted per sweep
    /// (the #4440/#4610 lesson — never an unbounded producer). Contracts left
    /// over are NOT counted as attempts and get picked up by a later sweep.
    #[cfg(feature = "redb")]
    #[tokio::test]
    async fn phantom_reconcile_bounds_fetches_per_sweep_4612() {
        let manager = HostingManager::new(DEFAULT_HOSTING_BUDGET_BYTES);
        let temp_dir = tempfile::TempDir::new().unwrap();
        let storage = crate::contract::storages::ReDb::new(temp_dir.path())
            .await
            .unwrap();
        manager.set_storage(storage);

        let peer = make_peer_key(9);
        for i in 0..5u8 {
            manager.add_downstream_subscriber(&make_contract_key(100 + i), peer.clone());
        }

        let first = manager.reconcile_phantom_in_use(2);
        assert_eq!(first.len(), 2, "sweep emits at most max_fetches fetches");
        // The remaining phantoms are untracked (no attempt burned) and the
        // next sweep picks them up immediately — no cooldown applies to a
        // contract that never got its fetch.
        let second = manager.reconcile_phantom_in_use(8);
        assert_eq!(
            second.len(),
            3,
            "later sweeps pick up the leftover phantoms without burning attempts"
        );
        assert!(
            first
                .iter()
                .chain(second.iter())
                .all(|a| matches!(a, PhantomRepair::Fetch(_))),
            "all emitted actions are fetches on the first pass over fresh phantoms"
        );
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
                std::sync::Arc::new(crate::util::time_source::InstantTimeSrc::new()),
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
                std::sync::Arc::new(crate::util::time_source::InstantTimeSrc::new()),
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

    /// `record_contract_access`: an in-use (locally-subscribed) contract is
    /// ordered LAST, so while a zero-subscriber contract is available to shed it
    /// is NOT the victim — the subscribed contract survives WITHOUT being torn
    /// down. Once its subscription is removed it becomes an ordinary zero-
    /// subscriber contract and is evicted normally. (The in-use signal here is a
    /// local client subscription, the LAST-evicted dimension of the
    /// subscriber-primary ordering; see `contract_in_use`'s rustdoc for why an
    /// upstream network subscription alone is excluded.) The complementary
    /// last-resort case — an in-use contract that IS shed and torn down — is
    /// covered by `record_contract_access_sheds_and_tears_down_only_subscribed`.
    #[test]
    fn test_record_contract_access_orders_in_use_last() {
        let manager = HostingManager::new(DEFAULT_HOSTING_BUDGET_BYTES);
        // Override with a tiny cache and ZERO TTL so every entry is instantly
        // eviction-eligible — ordering is then the only thing in play.
        {
            let mut cache = manager.hosting_cache.write();
            *cache = cache::HostingCache::new(
                200, // room for ~2 contracts at 100 bytes
                std::sync::Arc::new(crate::util::time_source::InstantTimeSrc::new()),
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

        // Inserting `trigger` puts the cache over budget. A naive LRU would evict
        // `in_use` (oldest) — but it is locally subscribed so it is ordered LAST,
        // and one eviction of the zero-subscriber `filler` is enough.
        let result = manager.record_contract_access(trigger, 100, AccessType::Get);
        assert_eq!(
            result.evicted,
            vec![(filler, 0)],
            "the zero-subscriber contract is shed; the locally-subscribed one is \
             ordered last and survives"
        );
        assert!(
            result.evicted_in_use.is_empty(),
            "no in-use contract was shed, so nothing was torn down"
        );
        assert!(manager.is_hosting_contract(&in_use));
        assert!(!manager.is_hosting_contract(&filler));
        // `in_use`'s subscription is intact — it was NOT torn down.
        assert!(manager.contract_in_use(&in_use));

        // Drop the client subscription: `in_use` is now an ordinary zero-
        // subscriber contract.
        manager.remove_client_subscription(in_use.id(), client);
        assert!(!manager.contract_in_use(&in_use));

        let result = manager.record_contract_access(filler, 100, AccessType::Get);
        assert!(
            result.evicted.iter().any(|(k, _)| *k == in_use),
            "once the subscription is removed the contract is evicted normally \
             when the cache is over budget"
        );
        assert!(!manager.is_hosting_contract(&in_use));
    }

    /// LOAD-BEARING (#4642, invariant 3): when the cache is over budget and EVERY
    /// hosted contract is subscribed, `record_contract_access` sheds the fewest-
    /// `(local, downstream)`-subscriber one AND tears down its subscription state
    /// so `contract_in_use` flips to false — the memory-teardown the shipped
    /// #4720 code was missing (there, the entry vanished from the cache but the
    /// subscription lingered, so `reclaim_evicted_contract` skipped the disk
    /// delete forever). Verifies fewest-downstream-first among equal-local
    /// contracts, and that ONLY the shed contract is torn down.
    #[test]
    fn record_contract_access_sheds_and_tears_down_only_subscribed() {
        let manager = HostingManager::new(DEFAULT_HOSTING_BUDGET_BYTES);
        // Tiny cache, ZERO TTL so every entry is instantly eligible (no min_ttl
        // grace masking the shed).
        {
            let mut cache = manager.hosting_cache.write();
            *cache = cache::HostingCache::new(
                200, // room for ~2 contracts at 100 bytes
                std::sync::Arc::new(crate::util::time_source::InstantTimeSrc::new()),
            );
        }

        let few = make_contract_key(1); // 1 downstream subscriber — shed first
        let many = make_contract_key(2); // 3 downstream subscribers — survives
        let trigger = make_contract_key(3); // 2 downstream subscribers

        // Every contract is subscribed (downstream peers); none is zero-subscriber.
        let p_few = make_peer_key(10);
        let p_many: Vec<_> = (0..3).map(|i| make_peer_key(20 + i)).collect();
        let p_trigger: Vec<_> = (0..2).map(|i| make_peer_key(30 + i)).collect();
        manager.add_downstream_subscriber(&few, p_few.clone());
        for p in &p_many {
            manager.add_downstream_subscriber(&many, p.clone());
        }
        for p in &p_trigger {
            manager.add_downstream_subscriber(&trigger, p.clone());
        }

        manager.record_contract_access(few, 100, AccessType::Get);
        manager.record_contract_access(many, 100, AccessType::Get);
        assert!(manager.contract_in_use(&few));
        assert!(manager.contract_in_use(&many));

        // Insert `trigger` over budget. Nothing is zero-subscriber, so the fewest-
        // downstream contract `few` (1 sub) is shed as the last resort ahead of
        // `many` (3 subs).
        let result = manager.record_contract_access(trigger, 100, AccessType::Get);
        assert_eq!(
            result.evicted,
            vec![(few, 0)],
            "with only subscribed contracts, the fewest-downstream one is shed"
        );
        assert_eq!(
            result.evicted_in_use,
            vec![few],
            "the shed subscribed contract is reported for teardown"
        );

        // THE LOAD-BEARING ASSERTION: the shed contract's subscription state was
        // torn down, so `contract_in_use` is now false and the reclaim gate
        // (`reclaim_evicted_contract` / `RuntimePool::remove_contract`) will
        // proceed to free the disk state instead of skipping it forever.
        assert!(
            !manager.contract_in_use(&few),
            "the shed contract must be torn down so contract_in_use is false and \
             disk reclamation proceeds (the memory-teardown #4720 lacked)"
        );
        assert!(!manager.has_downstream_subscribers(&few));
        assert!(!manager.is_hosting_contract(&few));

        // The surviving contract keeps its subscription intact — only the victim
        // was torn down.
        assert!(
            manager.contract_in_use(&many),
            "the surviving subscribed contract must NOT be torn down"
        );
        assert!(manager.has_downstream_subscribers(&many));
        assert!(manager.is_hosting_contract(&many));
    }

    /// A torn-down subscriber-primary eviction must NOT be immediately re-driven
    /// by `contracts_needing_renewal` (#4642 teardown correctness). In the all-
    /// local-subscribed extreme the victim's client subscription AND its upstream
    /// lease are cleared, so neither renewal section 1 (active-subscription, gated
    /// on `contract_in_use`) nor section 2 (client-subscription re-subscribe)
    /// re-selects it — otherwise the teardown would be undone one tick later and
    /// the contract would re-fetch/re-host, defeating the eviction.
    #[test]
    fn torn_down_eviction_not_redriven_by_contracts_needing_renewal() {
        let manager = HostingManager::new(DEFAULT_HOSTING_BUDGET_BYTES);
        // Tiny cache, ZERO TTL so every entry is instantly eligible.
        {
            let mut cache = manager.hosting_cache.write();
            *cache = cache::HostingCache::new(
                200,
                std::sync::Arc::new(crate::util::time_source::InstantTimeSrc::new()),
            );
        }

        let victim = make_contract_key(1);
        let keep = make_contract_key(2);
        let trigger = make_contract_key(3);

        // The all-LOCAL extreme: every contract has a local client subscription
        // AND an upstream subscription lease (so both renewal sections are live).
        let client = crate::client_events::ClientId::next();
        for k in [&victim, &keep, &trigger] {
            manager.add_client_subscription(k.id(), client);
            manager.subscribe(*k);
        }

        manager.record_contract_access(victim, 100, AccessType::Get);
        manager.record_contract_access(keep, 100, AccessType::Get);
        assert!(manager.contract_in_use(&victim));

        // Insert `trigger` over budget. Every contract is locally subscribed
        // (local = 1), so ties break by least-recent GET → `victim` (accessed
        // first) is shed as the last resort and torn down.
        let result = manager.record_contract_access(trigger, 100, AccessType::Get);
        assert_eq!(
            result.evicted_in_use,
            vec![victim],
            "the local victim is torn down"
        );
        assert!(
            !manager.contract_in_use(&victim),
            "victim's local subscription + lease were cleared"
        );
        assert!(
            !manager.is_subscribed(&victim),
            "victim's upstream lease was dropped"
        );

        // THE REGRESSION ASSERTION: renewal must not re-select the torn-down
        // victim. The still-subscribed `keep` (and `trigger`) may appear, but
        // `victim` must not — otherwise the next renewal tick would re-fetch it.
        let renewal = manager.contracts_needing_renewal();
        assert!(
            !renewal.contains(&victim),
            "a torn-down eviction must not be re-driven by contracts_needing_renewal, \
             got {renewal:?}"
        );
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

    /// A hosted contract with downstream subscribers is ordered LAST, so when a
    /// zero-subscriber contract is available to shed it is NOT evicted (one
    /// eviction of the zero-subscriber contract is enough to get back under
    /// budget). This is the common case; it is NOT a hard pin — see
    /// `evict_all_subscribed_sheds_fewest_as_last_resort` (cache tests) for the
    /// last-resort shed when EVERY contract is subscribed. Without ordering
    /// subscribed contracts last, interior peers would drop hosting → stop
    /// renewal → lose upstream subscription → downstream subscribers lose their feed.
    ///
    /// This test operates at the HostingCache level so it can seed both entries
    /// and verify the subscriber-primary ordering directly.
    #[test]
    fn test_zero_sub_evicted_before_downstream_subscribed() {
        use crate::ring::hosting::cache::HostingCache;
        use crate::util::time_source::SharedMockTimeSource;

        let time = SharedMockTimeSource::new();
        // Seed both under a generous budget (no auto-evict; distinct recency), then
        // tighten to 150 so 2x100-byte entries are over budget for the sweep. (No
        // `min_ttl` floor since 2026-07-08, so record_access sheds to budget
        // immediately — the "both resident over budget" state needs a budget shrink.)
        let mut cache = HostingCache::new(100_000, time.clone());

        let subscribed = make_contract_key(1);
        let zero_sub = make_contract_key(2);

        cache.record_access(subscribed, 100, AccessType::Get, 0, |_| (0, 0));
        cache.record_access(zero_sub, 100, AccessType::Get, 0, |_| (0, 0));
        cache.set_budget_for_test(150);
        assert_eq!(cache.current_bytes(), 200); // over budget

        // `subscribed` has a downstream subscriber; `zero_sub` has none. Under
        // subscriber-primary ordering the zero-subscriber one sheds first, and one
        // eviction is enough — so the subscribed one survives (ordered last).
        let evicted = cache.sweep_expired(
            |k| (0, (*k == subscribed) as usize),
            cache::MemoryPressure::AtCapacity,
        );

        assert!(
            !evicted.iter().any(|e| e.key == subscribed),
            "the subscribed contract is ordered last; it survives while a \
             zero-subscriber one can be shed"
        );
        assert!(
            evicted.iter().any(|e| e.key == zero_sub),
            "the zero-subscriber contract is evicted first when over budget"
        );
        assert!(cache.contains(&subscribed));
    }

    /// A hosted contract with NO subscribers and NO clients IS evictable when the
    /// cache is over budget — there is no `min_ttl` floor (dropped 2026-07-08), so
    /// the sweep sheds it immediately.
    ///
    /// Uses HostingCache with MockTimeSrc.
    #[test]
    fn test_no_subscribers_allows_eviction() {
        use crate::ring::hosting::cache::HostingCache;
        use crate::util::time_source::SharedMockTimeSource;

        let time = SharedMockTimeSource::new();
        // Budget of 80 bytes, entry is 100 → over budget immediately.
        let mut cache = HostingCache::new(80, time.clone());

        let contract = make_contract_key(100);
        // The insert can't self-evict the just-inserted contract (op-scoped
        // backstop), so it is resident but over budget.
        cache.record_access(contract, 100, AccessType::Get, 0, |_| (0, 0));
        assert!(cache.contains(&contract));

        // A sweep (no op-protected key) sheds the zero-subscriber contract at once
        // — no TTL wait.
        let evicted = cache.sweep_expired(|_| (0, 0), cache::MemoryPressure::AtCapacity);
        assert!(
            evicted.iter().any(|e| e.key == contract),
            "an over-budget contract with no subscribers is evicted (no min_ttl floor)"
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
            let result = manager
                .add_downstream_subscriber(&contract, peer)
                .was_accepted();
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

        let outcome = manager.add_downstream_subscriber(&contract, extra_peer);
        assert_eq!(
            outcome,
            AddSubscriberOutcome::Rejected,
            "Downstream subscriber beyond limit should return Rejected (count was {actual_count})"
        );
        assert!(
            !outcome.was_accepted(),
            "Rejected must NOT count as accepted"
        );
    }

    /// `add_downstream_subscriber` MUST distinguish a genuine new add
    /// from a renewal. Governance no longer consumes this distinction
    /// (benefit is a live snapshot of the current lease-valid subscriber
    /// set, so renewals cannot inflate it), but subscription bookkeeping
    /// and telemetry still rely on knowing whether a registration added
    /// a new peer or merely refreshed an existing lease.
    #[test]
    fn add_downstream_subscriber_distinguishes_new_add_from_renewal() {
        let manager = HostingManager::new(DEFAULT_HOSTING_BUDGET_BYTES);
        let contract = make_contract_key(1);
        let peer = make_peer_key(2);

        // First call: NewAdd.
        assert_eq!(
            manager.add_downstream_subscriber(&contract, peer.clone()),
            AddSubscriberOutcome::NewAdd,
            "first registration of a peer must be NewAdd"
        );

        // Second call with the same peer: Renewal, not NewAdd.
        assert_eq!(
            manager.add_downstream_subscriber(&contract, peer.clone()),
            AddSubscriberOutcome::Renewal,
            "repeated registration of the same peer must be Renewal, not NewAdd"
        );

        // Third call also Renewal — no escalation back to NewAdd.
        assert_eq!(
            manager.add_downstream_subscriber(&contract, peer),
            AddSubscriberOutcome::Renewal,
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

        // Existing peer can still renew (re-insert updates the timestamp).
        // Post-Sybil-fix: this returns Renewal, not NewAdd — pin both
        // facts so a regression flips the demand-ingest gate the wrong way.
        let outcome = manager.add_downstream_subscriber(&contract, first_peer);
        assert_eq!(
            outcome,
            AddSubscriberOutcome::Renewal,
            "Existing peer at limit should return Renewal (not NewAdd or Rejected)"
        );
        assert!(outcome.was_accepted(), "Renewal must count as accepted");
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
                200, // tiny budget: room for ~2 contracts at 100 bytes
                std::sync::Arc::new(crate::util::time_source::InstantTimeSrc::new()),
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

    /// Generation-mismatch + not-in-cache → keep pending and update its
    /// captured generation to the current one. Models the
    /// `RuntimePool::remove_contract` branch added in PR #4212 review
    /// round 8: an evicted-then-in-use-then-UPDATEd contract must keep
    /// its retry entry, otherwise the on-disk storage leaks once the
    /// subscriber later expires (UPDATE does not call `host_contract`,
    /// so the cache cannot emit another `EvictContract`).
    #[test]
    fn test_pending_reclamation_kept_on_generation_mismatch_when_not_hosted() {
        let manager = HostingManager::new(DEFAULT_HOSTING_BUDGET_BYTES);
        let contract = make_contract_key(0xA1);

        // Initial state: contract has been written 5 times (gen=5), was
        // evicted with `expected_generation=5`, and queued for retry
        // because a subscriber was still attached at the time.
        for _ in 0..5 {
            manager.bump_state_generation(&contract);
        }
        let captured_generation = manager.state_generation(&contract);
        assert_eq!(captured_generation, 5);
        manager.pending_reclamation_add(contract, captured_generation);

        // Simulate UPDATEs while the contract is still evicted (not in
        // cache): `state_generation` advances past the captured value.
        // UPDATE does not call `host_contract`, so the cache stays
        // empty.
        manager.bump_state_generation(&contract);
        manager.bump_state_generation(&contract);
        manager.bump_state_generation(&contract);
        let current_generation = manager.state_generation(&contract);
        assert_eq!(current_generation, captured_generation + 3);

        // Precondition: the contract is NOT in the hosting cache.
        assert!(!manager.is_hosting_contract(&contract));

        // The `RuntimePool::remove_contract` generation-mismatch +
        // not-hosted branch upserts the pending entry to the current
        // generation. Model that here via `pending_reclamation_add`.
        manager.pending_reclamation_add(contract, current_generation);

        let snapshot = manager.pending_reclamation_snapshot();
        assert_eq!(
            snapshot,
            vec![(contract, current_generation)],
            "pending entry must survive the generation mismatch AND its \
             expected_generation must advance to the current generation"
        );
    }

    /// Generation-mismatch + IS-in-cache → clear pending. Models the
    /// other half of the new branch: when the contract is back in the
    /// hosting cache (a PUT re-hosted it), the cache owns subsequent
    /// re-eviction and a stale pending entry would only produce
    /// spurious sweep retries.
    #[test]
    fn test_pending_reclamation_cleared_on_generation_mismatch_when_hosted() {
        let manager = HostingManager::new(DEFAULT_HOSTING_BUDGET_BYTES);
        let contract = make_contract_key(0xA2);

        // Queue a stale pending entry.
        let captured_generation = 7u64;
        manager.pending_reclamation_add(contract, captured_generation);
        assert_eq!(manager.pending_reclamation_len(), 1);

        // Simulate a PUT that re-hosted: bump generation AND add to
        // the hosting cache (record_contract_access ≈ what
        // `host_contract` does in production).
        manager.bump_state_generation(&contract);
        manager.record_contract_access(contract, 128, AccessType::Put);
        assert!(manager.is_hosting_contract(&contract));

        // The `RuntimePool::remove_contract` generation-mismatch +
        // is-hosting branch removes the pending entry. Model that here.
        manager.pending_reclamation_remove(&contract);
        assert_eq!(
            manager.pending_reclamation_len(),
            0,
            "pending entry must be cleared once the cache owns the contract \
             again — leaving it would let the sweep emit `EvictContract` \
             events that all bail at the `is_hosting_contract` check"
        );
    }

    // =========================================================================
    // Subscription-maintenance decision functions (#3367 Gap 2)
    //
    // Direct unit coverage for the three decision functions that drive
    // subscription maintenance. The named incidents in each test are the
    // failures these assertions would have caught: #3347 (hosting collapse),
    // #3360 (GET 94% fail / stale cache), and the #3363/#3763 subscription
    // storms. The existing tests above cover the happy paths; these lock the
    // boundary case that each incident actually hit — an *expired* lease that
    // still physically sits in `active_subscriptions` (the cache outlives the
    // lease), plus the hosted-without-clients framing of the storm.
    // =========================================================================

    /// `should_unsubscribe_upstream()` must return `false` for a contract we
    /// are hosting that still has a downstream subscriber, even though no
    /// *local* client is attached. The #3347 hosting collapse came from
    /// dropping the upstream lease for exactly this shape — a relay hosting a
    /// contract on behalf of downstream peers, with no local WebSocket client
    /// of its own. Tearing that down severs the only path keeping the relay's
    /// hosted state fresh for those downstream peers.
    #[test]
    fn test_should_unsubscribe_upstream_false_when_hosted_without_local_clients() {
        let manager = HostingManager::new(DEFAULT_HOSTING_BUDGET_BYTES);
        let contract = make_contract_key(0xC0);
        let downstream = make_peer_key(7);

        // We host the contract (in the LRU cache) and serve a downstream
        // subscriber, but no local client is subscribed.
        //
        // NOTE: the record_contract_access()/subscribe() calls model a
        // realistic hosting relay but are INCIDENTAL to this decision —
        // should_unsubscribe_upstream() reads only has_client_subscriptions()
        // + has_downstream_subscribers(), never the hosting cache or the lease
        // map. The decision keys solely off the downstream-subscriber map here.
        manager.record_contract_access(contract, 1000, AccessType::Get);
        manager.subscribe(contract);
        assert!(
            manager
                .add_downstream_subscriber(&contract, downstream.clone())
                .was_accepted()
        );

        // Precondition: hosted, with a downstream subscriber, no local client.
        assert!(manager.is_hosting_contract(&contract));
        assert!(manager.has_downstream_subscribers(&contract));
        assert!(
            !manager.has_client_subscriptions(contract.id()),
            "test precondition: no local client subscription"
        );

        // The downstream subscriber alone must hold the upstream lease open.
        assert!(
            !manager.should_unsubscribe_upstream(&contract),
            "hosted contract serving downstream peers must NOT unsubscribe \
             upstream just because no local client is attached (#3347 \
             hosting collapse)"
        );

        // Lock the other early-return branch: a local client subscription also
        // yields false, independent of the downstream map.
        let bare = make_contract_key(0xC1);
        let client_id = crate::client_events::ClientId::next();
        manager.add_client_subscription(bare.id(), client_id);
        assert!(
            !manager.has_downstream_subscribers(&bare),
            "test precondition: no downstream subscriber for the bare contract"
        );
        assert!(
            !manager.should_unsubscribe_upstream(&bare),
            "a local client subscription alone must hold the upstream lease \
             open (has_client_subscriptions early-return branch)"
        );
    }

    /// `is_receiving_updates()` must distinguish an *active* network
    /// subscription from a *stale* hosting-cache entry whose lease has already
    /// expired. An expired lease can still physically sit in
    /// `active_subscriptions` until `expire_stale_subscriptions()` sweeps it,
    /// and the contract typically remains in the hosting cache the whole time.
    /// If `is_receiving_updates()` keyed off mere map membership it would
    /// report a contract as fresh while no UPDATE stream is actually arriving
    /// — the #3360 "serving 94%-stale state" failure mode.
    #[test]
    fn test_is_receiving_updates_false_for_expired_lease_stale_cache() {
        let manager = HostingManager::new(DEFAULT_HOSTING_BUDGET_BYTES);
        let contract = make_contract_key(0x33);

        // Contract is in the hosting cache (durable fallback) ...
        manager.record_contract_access(contract, 1000, AccessType::Get);
        assert!(manager.is_hosting_contract(&contract));

        // ... and an active, non-expired subscription reads as receiving.
        manager.subscribe(contract);
        assert!(
            manager.is_receiving_updates(&contract),
            "an active lease must count as receiving updates"
        );

        // Force the lease into the past (same idiom as
        // test_expire_downstream_triggers_unsubscribe_decision: directly
        // backdate the private map to model time passing without a real
        // sleep). The entry is still present in `active_subscriptions`.
        if let Some(mut lease) = manager.active_subscriptions.get_mut(&contract) {
            lease.expires_at = Instant::now() - Duration::from_secs(1);
        }

        // Even though the cache entry survives and the lease row is still in
        // the map, an expired lease must NOT count as receiving updates.
        assert!(
            !manager.is_receiving_updates(&contract),
            "expired lease + warm cache must read as NOT receiving updates \
             (stale-cache distinction, #3360)"
        );
        // And the warm cache alone must never resurrect the signal.
        assert!(
            manager.is_hosting_contract(&contract),
            "test precondition: contract still hosted in the LRU cache"
        );
    }

    /// `contracts_needing_renewal()` must stay bounded by *active interest*,
    /// not by the size of the hosting cache. Purely-cached contracts — those
    /// with no live subscription, no client, and no recent local-client
    /// access — must NOT be counted, including the boundary case of a contract
    /// whose lease has already expired but still occupies `active_subscriptions`.
    ///
    /// This is the bound that the #3363/#3763 368-contract storm violated:
    /// renewing every cached contract makes the renewal set grow with the
    /// cache (unbounded) instead of with the handful of genuinely-subscribed
    /// contracts. It ties to the AGENTS.md GC rule — the only cache-derived
    /// entries that may be renewed are time-bounded by recent local-client
    /// access (`SUBSCRIPTION_LEASE_DURATION`); an expired lease grants no such
    /// exemption.
    #[test]
    fn test_contracts_needing_renewal_bounded_by_active_interest() {
        let manager = HostingManager::new(DEFAULT_HOSTING_BUDGET_BYTES);

        // 50 purely-cached contracts (e.g. relay-cached from GETs) — no
        // subscription, no client, no local-client access.
        for i in 0..50u8 {
            manager.record_contract_access(make_contract_key(i), 1000, AccessType::Get);
        }
        assert_eq!(manager.hosting_contracts_count(), 50);
        assert!(
            manager.contracts_needing_renewal().is_empty(),
            "purely-cached contracts must NOT need renewal — the set must be \
             bounded by active interest, not cache size (#3763 storm)"
        );

        // Branch-1 control set. All three carry a lease in `active_subscriptions`;
        // what distinguishes them is (a) which side of `now` the lease sits on and
        // (b) whether the contract has active demand (the demand-driven-hosting
        // §5a `contract_in_use` renewal gate: a live client OR downstream
        // subscriber).
        //
        // - `within_window`: lease expires inside the renewal window and in the
        //   future (now < expires_at <= now + SUBSCRIPTION_RENEWAL_INTERVAL), AND
        //   it has a downstream subscriber (active interest). Branch 1 returns it,
        //   so it MUST be counted — it pins the `expires_at > now` lower-bound
        //   guard AND the `contract_in_use` gate together. Without it the
        //   expired-lease assertion alone is inert: a fresh `subscribe()` lease is
        //   now + 8min (excluded by the window's upper bound) and 0xE0 isn't in
        //   the 0..50 cache for Branch 2, so the expired-lease guard could be
        //   deleted with the test still passing.
        // - `no_interest`: an identical within-window lease but with NO client and
        //   NO downstream subscriber. Under the interest gate it must NOT be
        //   counted: an un-demanded subscription LAPSES instead of self-renewing.
        //   This is the #3763 bound (renewal tracks active demand, not accumulated
        //   leases) and is what collapses a chain once demand ends.
        // - `expired`: lease is in the past. It must NOT be counted, otherwise the
        //   renewal set would refill itself from stale leases (the AGENTS.md
        //   time-bounded GC rule — an expired lease grants no renewal exemption).
        let within_window = make_contract_key(0xA0);
        manager.subscribe(within_window);
        manager.add_downstream_subscriber(&within_window, make_peer_key(0xA1));
        if let Some(mut lease) = manager.active_subscriptions.get_mut(&within_window) {
            lease.expires_at = Instant::now() + Duration::from_secs(30);
        }
        let no_interest = make_contract_key(0xB0);
        manager.subscribe(no_interest);
        if let Some(mut lease) = manager.active_subscriptions.get_mut(&no_interest) {
            lease.expires_at = Instant::now() + Duration::from_secs(30);
        }
        let expired = make_contract_key(0xE0);
        manager.record_contract_access(expired, 1000, AccessType::Get);
        manager.subscribe(expired);
        if let Some(mut lease) = manager.active_subscriptions.get_mut(&expired) {
            lease.expires_at = Instant::now() - Duration::from_secs(1);
        }
        let within_window_renewal = manager.contracts_needing_renewal();
        assert!(
            within_window_renewal.contains(&within_window),
            "a within-window lease WITH active downstream interest MUST be counted \
             (Branch 1 `expires_at > now` guard + `contract_in_use` gate)"
        );
        assert!(
            !within_window_renewal.contains(&no_interest),
            "a within-window lease with NO client and NO downstream subscriber must \
             NOT be counted — an un-demanded subscription lapses instead of \
             self-renewing (#3763 interest-gated renewal bound)"
        );
        assert!(
            !within_window_renewal.contains(&expired),
            "a contract with an expired lease must NOT be counted as needing \
             renewal (expired leases grant no renewal exemption — AGENTS.md \
             time-bounded GC rule)"
        );

        // Now add genuine interest: two client subscriptions. The renewal set
        // grows by exactly two and no more — it tracks interest, not cache.
        // want_a(3)/want_b(9) are intentionally chosen from the cached 0..50
        // set so Branch 2 can resolve their instance_id back to a ContractKey
        // via the hosting_cache lookup (line ~1390).
        let client_id = crate::client_events::ClientId::next();
        let want_a = make_contract_key(3);
        let want_b = make_contract_key(9);
        manager.add_client_subscription(want_a.id(), client_id);
        manager.add_client_subscription(want_b.id(), client_id);

        let needs_renewal = manager.contracts_needing_renewal();
        // Exactly three: the two client-subscribed contracts plus the
        // within-window lease — never the 51 cached-only / expired ones.
        assert_eq!(
            needs_renewal.len(),
            3,
            "renewal set must contain exactly the two client-subscribed \
             contracts and the within-window lease, not the 51 cached ones; \
             found {}",
            needs_renewal.len()
        );
        assert!(needs_renewal.contains(&want_a));
        assert!(needs_renewal.contains(&want_b));
        assert!(needs_renewal.contains(&within_window));
    }

    /// Regression for the interest-gated §1 renewal predicate (#3763): a live
    /// active lease is renewed **iff** the contract is `contract_in_use` — a
    /// live client subscription OR a downstream subscriber. This pins that the
    /// gate does NOT false-lapse a demanded contract, and that a demand-less
    /// lease correctly lapses:
    ///
    ///  - **No false-lapse for demand-backed contracts.** Every WS-client
    ///    subscribe entry point (a plain SUBSCRIBE and a GET-with-`subscribe:true`)
    ///    registers `client_subscriptions` via the listener-registration handler
    ///    (`ring.add_client_subscription`) — which is exactly the map
    ///    `contract_in_use` reads. So a genuinely client-subscribed contract's
    ///    lease is always selected by §1. A chain host is renewed via its
    ///    downstream subscriber.
    ///  - **A bare lease with no demand LAPSES, correctly.**
    ///    `run_executor_subscribe` installs an `active_subscriptions` lease but
    ///    registers interest only in the InterestManager (`add_local_client`),
    ///    NOT `client_subscriptions`. With no accompanying WS-client subscription
    ///    or downstream (a PUT/seed with no ongoing interest), the lease is NOT
    ///    renewed and lapses. Per design §1 a PUT only *seeds* a contract and is
    ///    not demand, so an idle seed must evaporate — this is the intended
    ///    behavior, not a false-lapse. Any real demand behind an executor
    ///    subscribe is the WS client that PUT/subscribed, tracked separately in
    ///    `client_subscriptions`.
    #[test]
    fn test_active_lease_renewed_iff_contract_in_use() {
        let manager = HostingManager::new(DEFAULT_HOSTING_BUDGET_BYTES);
        // Move a lease into the renewal window (§1 only considers soon-to-expire
        // leases; a fresh `subscribe()` lease is now+8min, outside the window).
        let into_window = |m: &HostingManager, key: &ContractKey| {
            if let Some(mut lease) = m.active_subscriptions.get_mut(key) {
                lease.expires_at = Instant::now() + Duration::from_secs(30);
            }
        };

        // (a) Bare lease — the `run_executor_subscribe` / PUT-seed shape: an
        //     active lease with no client subscription and no downstream. Must
        //     NOT be renewed → lapses.
        let seed = make_contract_key(1);
        manager.subscribe(seed);
        into_window(&manager, &seed);
        assert!(!manager.contract_in_use(&seed));
        assert!(
            !manager.contracts_needing_renewal().contains(&seed),
            "a bare active lease with no client subscription and no downstream (a \
             PUT / executor seed) must NOT be renewed — it lapses (design §1: a \
             seed is not demand)"
        );

        // (b) Demand-backed by a client subscription — the WS-subscribe entry
        //     point populates this via `add_client_subscription`. Must be renewed.
        let client_backed = make_contract_key(2);
        manager.subscribe(client_backed);
        into_window(&manager, &client_backed);
        let client_id = crate::client_events::ClientId::next();
        manager.add_client_subscription(client_backed.id(), client_id);
        assert!(manager.contract_in_use(&client_backed));
        assert!(
            manager.contracts_needing_renewal().contains(&client_backed),
            "a client-subscribed contract's lease MUST be renewed — no false-lapse \
             for a demand-backed subscription"
        );

        // (c) Demand-backed by a downstream subscriber — a chain host. Must be
        //     renewed.
        let downstream_backed = make_contract_key(3);
        manager.subscribe(downstream_backed);
        into_window(&manager, &downstream_backed);
        manager.add_downstream_subscriber(&downstream_backed, make_peer_key(7));
        assert!(manager.contract_in_use(&downstream_backed));
        assert!(
            manager
                .contracts_needing_renewal()
                .contains(&downstream_backed),
            "a chain host (downstream subscriber) lease MUST be renewed"
        );

        // (d) Removing the client subscription makes the once-demanded lease
        //     lapse — the collapse trigger.
        manager.remove_client_subscription(client_backed.id(), client_id);
        assert!(!manager.contract_in_use(&client_backed));
        assert!(
            !manager.contracts_needing_renewal().contains(&client_backed),
            "once the client subscription is gone the lease is no longer renewed \
             and lapses (chain collapse on interest loss)"
        );
    }

    // --- Eviction floor: effective = min(ram_budget, disk_budget) (#4683) ---

    /// When disk is the tighter resource, the recompute lowers the cache budget
    /// to the disk budget; when RAM is tighter, the RAM budget wins; when equal,
    /// either (the shared value) is installed. Drives `recompute_effective_budget`
    /// with a known `used` and an injected `available`, no filesystem.
    #[test]
    fn recompute_installs_min_of_ram_and_disk() {
        const MIB: u64 = 1024 * 1024;
        const GIB: u64 = 1024 * 1024 * 1024;

        // RAM budget = 4 GiB (ctor). pct = 0.5.
        let manager = HostingManager::new(4 * GIB);
        manager.configure_disk_budget(0.5, DEFAULT_MAX_HOSTING_DISK_BYTES);
        // Seed disk `used` = 2 GiB.
        manager.seed_disk_tracker_for_test([(make_contract_key(1), 2 * GIB)]);

        // Case DISK WINS: available small → disk_budget = 0.5*(2+2)=2 GiB < 4 GiB RAM.
        let eff = manager
            .recompute_effective_budget(2 * GIB)
            .expect("seeded → recompute runs");
        assert_eq!(eff, 2 * GIB, "disk is tighter → disk budget wins");
        assert_eq!(manager.hosting_budget_bytes(), 2 * GIB);

        // Case RAM WINS: available huge → disk_budget clamps to cap (32 GiB) > 4
        // GiB RAM, so RAM floor wins.
        let eff = manager
            .recompute_effective_budget(u64::MAX)
            .expect("seeded → recompute runs");
        assert_eq!(eff, 4 * GIB, "RAM is tighter → RAM budget wins");
        assert_eq!(manager.hosting_budget_bytes(), 4 * GIB);

        // Case EQUAL: choose available so disk_budget == RAM budget exactly.
        // 0.5*(used=2 GiB + available) = 4 GiB → available = 6 GiB.
        let eff = manager
            .recompute_effective_budget(6 * GIB)
            .expect("seeded → recompute runs");
        assert_eq!(eff, 4 * GIB, "equal budgets install the shared value");
        assert_eq!(manager.hosting_budget_bytes(), 4 * GIB);

        // Below the MIN floor: a tiny disk budget can never drop below the 128
        // MiB floor even when RAM is also small.
        let small = HostingManager::new(64 * MIB);
        small.configure_disk_budget(0.5, DEFAULT_MAX_HOSTING_DISK_BYTES);
        small.seed_disk_tracker_for_test(std::iter::empty());
        let eff = small.recompute_effective_budget(0).expect("seeded");
        assert_eq!(
            eff,
            64 * MIB,
            "RAM budget (64 MiB) is below the disk MIN floor and wins as the min"
        );
    }

    /// An unseeded (or absent) tracker makes the recompute a no-op: the cache
    /// keeps its RAM budget until the first seed, so early startup never installs
    /// a bogus zero/under-counted floor.
    #[test]
    fn recompute_is_noop_until_seeded() {
        const GIB: u64 = 1024 * 1024 * 1024;
        let manager = HostingManager::new(GIB);

        // No tracker configured at all → None, budget untouched.
        assert_eq!(manager.recompute_effective_budget(0), None);
        assert_eq!(manager.hosting_budget_bytes(), GIB);

        // Tracker configured but NOT seeded → still None.
        manager.configure_disk_tracker(
            std::path::PathBuf::from("/nonexistent/contracts"),
            std::path::PathBuf::from("/nonexistent/cache"),
        );
        assert_eq!(manager.recompute_effective_budget(0), None);
        assert_eq!(manager.hosting_budget_bytes(), GIB);
    }

    /// The recompute takes only the O(1) `set_budget_bytes` cache write lock, so
    /// it cannot deadlock against a concurrent `record_access_with_demand` (which
    /// also takes the cache write lock). Interleave many recomputes with many
    /// cache accesses from another thread and require completion.
    #[test]
    fn recompute_does_not_deadlock_against_cache_writes() {
        use std::sync::Arc;
        const GIB: u64 = 1024 * 1024 * 1024;

        let manager = Arc::new(HostingManager::new(4 * GIB));
        manager.configure_disk_budget(0.5, DEFAULT_MAX_HOSTING_DISK_BYTES);
        manager.seed_disk_tracker_for_test([(make_contract_key(1), GIB)]);

        let m2 = manager.clone();
        let writer = std::thread::spawn(move || {
            for i in 0..500u64 {
                // record_contract_access takes the hosting_cache write lock.
                m2.record_contract_access(
                    make_contract_key((i % 250) as u8),
                    i % 4096,
                    AccessType::Get,
                );
            }
        });

        for i in 0..500u64 {
            // Alternate the injected available so the installed floor varies,
            // exercising the set_budget_bytes path each iteration.
            let available = (i % 8) * GIB;
            manager.recompute_effective_budget(available);
        }
        writer
            .join()
            .expect("writer thread must not deadlock/panic");

        // Sanity: after the storm the cache budget is a valid min(ram, disk).
        let b = manager.hosting_budget_bytes();
        assert!(b <= 4 * GIB, "effective budget never exceeds the RAM floor");
    }

    // --- Admission gate: HostingManager level (#4683, live since #4702) --------

    /// Before the first recompute the aggregate disk budget is `u64::MAX` and the
    /// tracker may be unseeded, so the gate admits everything — early-startup
    /// writes are never spuriously rejected.
    #[test]
    fn admit_is_noop_before_recompute_and_seed() {
        let manager = HostingManager::new(1024 * 1024 * 1024);
        // No tracker at all → admit.
        assert!(
            manager
                .admit_state_write(&make_contract_key(1), 999)
                .is_ok()
        );
        assert!(manager.admit_wasm_write(999).is_ok());
        // Tracker seeded but no recompute yet → disk_budget_bytes is u64::MAX,
        // so still admit even a huge write.
        manager.seed_disk_tracker_for_test(std::iter::empty());
        assert_eq!(manager.disk_budget_bytes(), u64::MAX);
        assert!(
            manager
                .admit_state_write(&make_contract_key(1), u64::MAX / 2)
                .is_ok()
        );
    }

    // --- Dashboard disk-usage panel population (follow-up to #4683/#4702) ----
    //
    // `Ring::dashboard_hosting_snapshot` maps `disk_usage_stats()` and
    // `disk_budget_bytes()` straight into `HostingSnapshot`'s `Option<u64>`
    // disk fields (`None` while unseeded / not yet recomputed). These tests
    // exercise the two `HostingManager` accessors the mapping reads, so a
    // regression in the seeded/unseeded gate is caught at the source rather
    // than only through the heavier `Ring`-level plumbing.

    /// No tracker configured at all (mirrors a node with hosting disabled or
    /// very early startup before `configure_disk_tracker` runs): both
    /// accessors report the "not yet meaningful" values the dashboard maps to
    /// `None` — `disk_usage_stats()` is `None` and `disk_budget_bytes()` is
    /// `u64::MAX`.
    #[test]
    fn dashboard_disk_fields_absent_without_tracker() {
        let manager = HostingManager::new(1024 * 1024 * 1024);
        assert!(
            manager.disk_usage_stats().is_none(),
            "no tracker configured → disk_usage_stats must be None"
        );
        assert_eq!(
            manager.disk_budget_bytes(),
            u64::MAX,
            "no recompute has run → disk_budget_bytes stays u64::MAX"
        );
    }

    /// Tracker configured but not yet seeded (the window between
    /// `configure_disk_tracker` and the first successful `seed`): still
    /// `None` / `u64::MAX` — an in-flight seed must not leak a
    /// zero-initialized (misleadingly "empty") snapshot to the dashboard.
    #[test]
    fn dashboard_disk_fields_absent_while_unseeded() {
        let manager = HostingManager::new(1024 * 1024 * 1024);
        manager.configure_disk_tracker(
            std::path::PathBuf::from("/nonexistent/contracts"),
            std::path::PathBuf::from("/nonexistent/cache"),
        );
        assert!(
            manager.disk_usage_stats().is_none(),
            "configured-but-unseeded tracker → disk_usage_stats must stay None"
        );
        assert_eq!(manager.disk_budget_bytes(), u64::MAX);
    }

    /// Once seeded, `disk_usage_stats()` reports `Some` with the exact
    /// component breakdown (state/wasm/compile-cache/total) the dashboard
    /// renders. `disk_budget_bytes()` stays `u64::MAX` (→ `None`-mapped)
    /// until `recompute_effective_budget` has ALSO run — seeding alone does
    /// not install a budget.
    #[test]
    fn dashboard_disk_usage_populated_after_seed_budget_after_recompute() {
        const MIB: u64 = 1024 * 1024;
        let manager = HostingManager::new(64 * 1024 * MIB);
        manager.seed_disk_tracker_for_test([
            (make_contract_key(1), 100 * MIB),
            (make_contract_key(2), 50 * MIB),
        ]);

        let stats = manager
            .disk_usage_stats()
            .expect("seeded tracker → Some usage stats");
        assert_eq!(stats.state_bytes, 150 * MIB);
        assert_eq!(stats.total_bytes, 150 * MIB);
        // Seeding alone (no recompute) leaves the budget unset.
        assert_eq!(
            manager.disk_budget_bytes(),
            u64::MAX,
            "budget stays u64::MAX until recompute_effective_budget runs"
        );

        manager.configure_disk_budget(0.5, DEFAULT_MAX_HOSTING_DISK_BYTES);
        manager
            .recompute_effective_budget(150 * MIB)
            .expect("seeded → recompute installs a budget");
        let budget = manager.disk_budget_bytes();
        assert_ne!(
            budget,
            u64::MAX,
            "after recompute the dashboard must see a real (non-MAX) budget"
        );
    }

    /// After a recompute installs a real aggregate disk budget, the gate enforces
    /// it: a projected-over-budget write rejects, a projected-at-budget write
    /// admits. Drives a known `used` + injected `available` so the disk budget is
    /// deterministic (no filesystem).
    #[test]
    fn admit_enforces_recomputed_disk_budget() {
        const MIB: u64 = 1024 * 1024;
        const GIB: u64 = 1024 * 1024 * 1024;
        // RAM budget huge so the effective floor never masks the disk budget.
        let manager = HostingManager::new(64 * GIB);
        manager.configure_disk_budget(0.5, DEFAULT_MAX_HOSTING_DISK_BYTES);
        // Seed used = 200 MiB across one key.
        manager.seed_disk_tracker_for_test([(make_contract_key(1), 200 * MIB)]);
        // available = 200 MiB → disk_budget = 0.5*(200+200) MiB = 200 MiB, but
        // the MIN floor (128 MiB) is below that so the raw 200 MiB stands.
        let budget = manager
            .recompute_effective_budget(200 * MIB)
            .map(|_| manager.disk_budget_bytes())
            .expect("seeded → recompute runs");
        assert_eq!(budget, 200 * MIB, "disk budget = pct*(used+available)");

        // Current used = 200 MiB. A fresh PUT of key(2) sized so projected ==
        // budget admits: projected = 200 + new == 200 MiB → new = 0 admits; new
        // that makes projected exactly budget is the boundary. Use headroom = 0.
        // A write of 0 extra bytes trivially admits.
        assert!(manager.admit_state_write(&make_contract_key(2), 0).is_ok());
        // Any positive fresh write pushes projected over the (already-at) budget.
        let err = manager
            .admit_state_write(&make_contract_key(2), 1)
            .expect_err("projected 200MiB+1 > budget 200MiB rejects");
        assert_eq!(err.projected_bytes, 200 * MIB + 1);
        assert_eq!(err.budget_bytes, 200 * MIB);

        // An UPDATE that shrinks key(1) always admits (delta <= 0), even at the
        // budget ceiling.
        assert!(
            manager
                .admit_state_write(&make_contract_key(1), 100 * MIB)
                .is_ok()
        );
    }

    /// A rejected admit must not mutate the tracker (deferred-delta discipline):
    /// the counter only moves on the post-write success path via
    /// `record_state_write`, so a rejected write leaves no phantom bytes.
    #[test]
    fn rejected_admit_does_not_change_tracked_bytes() {
        const MIB: u64 = 1024 * 1024;
        let manager = HostingManager::new(64 * 1024 * MIB);
        manager.configure_disk_budget(0.5, DEFAULT_MAX_HOSTING_DISK_BYTES);
        manager.seed_disk_tracker_for_test([(make_contract_key(1), 200 * MIB)]);
        manager.recompute_effective_budget(200 * MIB);
        let before = manager
            .disk_usage_stats()
            .expect("tracker present")
            .state_bytes;
        assert!(
            manager.admit_state_write(&make_contract_key(2), 1).is_err(),
            "the write under test must be rejected for this assertion to be meaningful"
        );
        let after = manager
            .disk_usage_stats()
            .expect("tracker present")
            .state_bytes;
        assert_eq!(before, after, "rejected admit must not move state_bytes");
        assert_eq!(before, 200 * MIB);
    }

    /// #4683 finding #1 regression: on a disk-constrained node the aggregate can
    /// legitimately sit OVER the disk budget (disk_budget = clamp(0.5*(used+avail),
    /// MIN, MAX) can be < used when free space is scarce). In that state a
    /// shrinking or size-holding UPDATE must still be admitted — the hard PUT gate
    /// (`admit_state_write`) would reject it, stalling CRDT convergence, but the
    /// growth-only UPDATE gate (`admit_state_update`) admits it. Only genuine
    /// growth stays bounded.
    #[test]
    fn admit_state_update_shrinking_over_budget_admits() {
        const MIB: u64 = 1024 * 1024;
        const GIB: u64 = 1024 * 1024 * 1024;
        let manager = HostingManager::new(64 * GIB);
        manager.configure_disk_budget(0.5, DEFAULT_MAX_HOSTING_DISK_BYTES);
        // Seed used = 400 MiB across two keys.
        manager.seed_disk_tracker_for_test([
            (make_contract_key(1), 200 * MIB),
            (make_contract_key(2), 200 * MIB),
        ]);
        // available small so disk_budget = 0.5*(400+0) = 200 MiB but the MIN
        // floor (128 MiB) is below that → 200 MiB installed, while used = 400 MiB.
        // The node is now OVER budget (400 > 200).
        manager.recompute_effective_budget(0);
        let budget = manager.disk_budget_bytes();
        assert!(
            manager.disk_usage_stats().unwrap().total_bytes > budget,
            "test precondition: node must be over budget (used {} > budget {budget})",
            manager.disk_usage_stats().unwrap().total_bytes
        );

        // Hard PUT gate rejects a shrinking write of key(1) to 100 MiB
        // (projected = 400 − 200 + 100 = 300 MiB > 200 MiB budget)...
        assert!(
            manager
                .admit_state_write(&make_contract_key(1), 100 * MIB)
                .is_err(),
            "hard gate would reject the shrink and stall convergence"
        );
        // ...but the growth-only UPDATE gate admits it (delta = 100 − 200 <= 0).
        assert!(
            manager
                .admit_state_update(&make_contract_key(1), 100 * MIB)
                .is_ok(),
            "shrinking UPDATE must admit even over budget (convergence safety)"
        );
        // A size-holding UPDATE (delta == 0) also admits over budget.
        assert!(
            manager
                .admit_state_update(&make_contract_key(1), 200 * MIB)
                .is_ok()
        );
        // Genuine growth of key(1) beyond its current size is still bounded.
        assert!(
            manager
                .admit_state_update(&make_contract_key(1), 300 * MIB)
                .is_err(),
            "growth over budget must still reject"
        );
    }
}
