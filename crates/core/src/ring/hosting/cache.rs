//! Unified hosting cache for contract state caching.
//!
//! This module implements a byte-budget aware, **demand-ordered** ("fuel
//! gauge") cache with TTL protection for hosted contracts. It unifies the
//! previously separate `SeedingCache` (now called hosting cache) and
//! `GetSubscriptionCache` into a single source of truth for which contracts a
//! peer is hosting.
//!
//! # Design Principles
//!
//! 1. **Single source of truth**: All hosted contracts are tracked in one cache
//! 2. **Subscriber-primary eviction** (freenet/freenet-core#4642, subscriber-
//!    primary rework): when over budget the eviction victim is chosen by
//!    ascending `(local_subscription_count, downstream_subscriber_count,
//!    recency_seq, key_bytes)` — the FEWEST-subscriber contract first (local
//!    subscriptions weighed ahead of downstream), ties broken by least-recent
//!    **real GET or PUT access** (`recency_seq`), then a deterministic key-byte
//!    tiebreak. The split
//!    `(local, downstream)` counts are supplied by the caller (`HostingManager`)
//!    via the same closure that used to be `should_retain`; they count genuine
//!    demand (local client subscriptions + downstream subscribers) and sum to
//!    `0` exactly when the contract is NOT `contract_in_use`. Subscriber count
//!    is the ORDERING, NOT a hard pin: under NORMAL over-budget operation
//!    ([`MemoryPressure::AtCapacity`]) a subscribed contract is ordered LAST and
//!    survives while any contract with fewer subscribers is eligible, but when
//!    the peer is still over budget and nothing fewer-subscriber is eligible the
//!    fewest-subscriber contract IS shed as a last resort — including a
//!    subscribed one (its subscription state is then torn down; see
//!    [`RecordAccessResult::evicted_in_use_teardown`]). This is the change from
//!    the shipped #4720 code, which hard-pinned every subscribed contract. There
//!    is **no `min_ttl` cold-start floor** (dropped 2026-07-08): a real GET or
//!    PUT resets an entry's `recency_seq`, so a freshly-accessed contract is
//!    protected simply by being the most-recently-accessed under the recency
//!    ordering, and a fresh PUT protects itself at PUT time without waiting for a
//!    first read. The OOM valve ([`MemoryPressure::Overflow`], trigger unwired)
//!    now differs from `AtCapacity` only in that it pierces the op-scoped backstop
//!    (evicting even a contract with an in-flight op) to survive genuine RAM
//!    overflow.
//! 3. **Subscription renewal**: All hosted contracts get subscription renewal
//! 4. **Access type tracking**: Records how contract was accessed (GET/PUT/SUBSCRIBE).
//!    A real GET or PUT (genuine client access) resets `recency_seq`; SUBSCRIBE
//!    and automatic subscription-renewal traffic do NOT, so renewal cannot keep a
//!    contract artificially fresh (invariant 3, decision 2026-07-08).
//!
//! ## Demoted (telemetry-only) demand machinery
//!
//! `keep_score`, `eviction_floor`, `predicted_demand` and `record_abandonment`
//! (the Greedy-Dual + proximity-prior demand estimator, pieces A3/#4650/#4688)
//! are **retained but no longer drive eviction** — they are kept as the
//! dashboard/telemetry surface (`predicted_demand` is still trained and
//! displayed) for telemetry continuity this release, and are scheduled for
//! deletion in a follow-up once the subscriber-primary policy is field-
//! validated. Eviction reads NONE of them; it orders by subscriber count and
//! real GET/PUT recency (see principle 2). `predicted_demand` is still supplied by
//! the caller (`HostingManager`, which owns the proximity-prior estimator and
//! the peer's own ring location — see [`super::demand`]) purely so the
//! estimator keeps training and the dashboard column stays populated.

// NAMING LANDMINE: "cache" throughout this module means HOSTING, not a lesser
// cache tier. A peer stores a contract because a GET or PUT routed through it, and
// a routed GET/PUT is a demand signal, so the peer HOSTS the contract (holds
// WASM+state, kept fresh in the update mesh) until LRU eviction. There is no cache
// tier. This naming confuses humans and LLMs; slated to be renamed to hosting-* in
// a follow-up refactor AFTER 0.2.94 (deferred to avoid conflicting with the
// in-flight eviction rewrite of this file). See .claude/rules/hosting-invariants.md
// terminology + epic #4642.

use freenet_stdlib::prelude::ContractKey;
use std::collections::HashMap;
use std::time::Duration;
use tokio::time::Instant;

use super::demand::NEUTRAL_DEMAND;
use crate::ring::interest::PeerKey;
use crate::util::time_source::TimeSource;
use crate::wasm_runtime::read_total_ram_bytes;

/// Lower clamp for the RAM-scaled default hosting budget (128 MiB).
///
/// Even a genuinely tiny host should still be able to host a useful amount of
/// contract state. The budget tracks on-disk **state** bytes (not RSS), so this
/// floor is a disk allowance, not a memory reservation.
pub const MIN_DEFAULT_HOSTING_BUDGET_BYTES: u64 = 128 * 1024 * 1024;

/// Upper clamp for the RAM-scaled default hosting budget (1 GiB).
///
/// Equal to the historical flat default, on purpose: a host with ample RAM
/// (>= 8 GiB at the current divisor) resolves to exactly the previous 1 GiB
/// budget and sees NO behavior change. Only memory-constrained hosts get a
/// smaller, proportional budget. This makes the capability-relative default a
/// pure "small boxes get less" change (addresses #4565, gateway RSS -> OOM on
/// small boxes) rather than a budget increase for anyone.
pub const MAX_DEFAULT_HOSTING_BUDGET_BYTES: u64 = 1024 * 1024 * 1024;

/// The pre-A2 flat hosting budget (1 GiB) that older releases auto-persisted
/// into `config.toml` as `max-hosting-storage = 1073741824`.
///
/// Used ONLY as the one-time upgrade-migration sentinel in
/// `config::ConfigArgs::build`: a persisted value exactly equal to this is
/// treated as the old auto-derived default (NOT an explicit operator choice) and
/// re-derived from live RAM, so a small box that upgrades from a pre-A2 release
/// stops carrying the pinned 1 GiB (#4565). Deliberately a SEPARATE constant
/// from [`MAX_DEFAULT_HOSTING_BUDGET_BYTES`] even though they share a value
/// today: the migration sentinel must stay 1 GiB even if the clamp ceiling
/// later changes.
pub const LEGACY_FLAT_HOSTING_BUDGET_BYTES: u64 = 1024 * 1024 * 1024;

/// Fraction of total system RAM used to size the default hosting budget: 1/8.
///
/// Mirrors the module cache's `DEFAULT_MODULE_CACHE_RAM_DIVISOR`. The divisor —
/// NOT the `MAX` ceiling — is the small-box protection (#4441): the fix that
/// replaced the flat budget must itself never OOM a small box, so the budget
/// scales DOWN with RAM below the ceiling instead of being a fixed
/// count/constant. 1/8 leaves the other 7/8 of RAM for the rest of the node
/// (state store, module caches, transport buffers, WASM arenas, the OS).
const DEFAULT_HOSTING_BUDGET_RAM_DIVISOR: u64 = 8;

/// Fallback total-RAM estimate (1 GiB) when the OS query fails. Conservative:
/// at 1 GiB the divisor yields 128 MiB (the floor), so an unknown-RAM host gets
/// the smallest sane budget rather than the max.
const FALLBACK_TOTAL_RAM_BYTES: u64 = 1024 * 1024 * 1024;

/// Default hosting-storage budget, scaled to the memory the node may use.
///
/// Replaces the historical flat 1 GiB default. Returns
/// `clamp(total_ram / DEFAULT_HOSTING_BUDGET_RAM_DIVISOR,
/// MIN_DEFAULT_HOSTING_BUDGET_BYTES, MAX_DEFAULT_HOSTING_BUDGET_BYTES)`
/// (currently `clamp(total_ram / 8, 128 MiB, 1 GiB)`), where `total_ram` is
/// `min(host RAM, cgroup limit)` from the SAME single source
/// [`crate::wasm_runtime::read_total_ram_bytes`] that sizes the module cache and
/// the A1 resource-utilization telemetry — so the node has one notion of "how
/// much memory do I have".
///
/// This is the single source of truth for the default budget: the operator-
/// facing `config::default_max_hosting_storage()` resolves to it (re-exported
/// via `ring::hosting`). The operator override
/// (`--max-hosting-storage` / `max_hosting_storage` config / env) always wins
/// when explicitly set; only this DEFAULT is RAM-scaled.
///
/// The budget tracks hosted contract **state** bytes only. WASM code blobs and
/// ReDb/SQLite database overhead are additional and not counted against it, so
/// this is not a hard bound on total on-disk usage.
pub fn default_hosting_budget_bytes() -> u64 {
    let total_ram = read_total_ram_bytes()
        .map(|v| v as u64)
        .unwrap_or(FALLBACK_TOTAL_RAM_BYTES);
    budget_for_ram(total_ram)
}

/// Pure clamp math behind [`default_hosting_budget_bytes`], split out so the
/// small-box / large-box boundary behavior is unit-testable without depending
/// on the test host's real RAM. Returns the hosting-cache byte budget for a
/// host with `total_ram` bytes of usable memory.
fn budget_for_ram(total_ram: u64) -> u64 {
    (total_ram / DEFAULT_HOSTING_BUDGET_RAM_DIVISOR).clamp(
        MIN_DEFAULT_HOSTING_BUDGET_BYTES,
        MAX_DEFAULT_HOSTING_BUDGET_BYTES,
    )
}

/// Default fraction of the disk capacity *available to Freenet* used to size the
/// aggregate disk budget (#4683): 50%. Sized against `used + free` (capacity the
/// node can actually reach), NOT a naive live free-space read that shrinks as it
/// fills. Operator-overridable via `--hosting-disk-pct`.
pub const DEFAULT_HOSTING_DISK_PCT: f64 = 0.5;

/// Default hard upper clamp for the aggregate disk budget (#4683): 32 GiB.
///
/// Distinct from [`MAX_DEFAULT_HOSTING_BUDGET_BYTES`] (the 1 GiB RAM-scaled
/// ceiling) on purpose: a disk budget legitimately dwarfs the RAM budget on a
/// host with a large data disk, so this cap is far higher. Operator-overridable
/// via `--max-hosting-disk`; the shared MIN floor is
/// [`MIN_DEFAULT_HOSTING_BUDGET_BYTES`] so even a tiny disk still gets a usable
/// allowance.
pub const DEFAULT_MAX_HOSTING_DISK_BYTES: u64 = 32 * 1024 * 1024 * 1024;

/// Pure clamp math for the aggregate disk budget (#4683), the disk analogue of
/// [`budget_for_ram`]. Split out with no filesystem dependency so the
/// zero/huge/overflow boundary behavior is unit-testable without a real disk.
///
/// Returns `clamp(pct * (freenet_used + available), MIN, max_hosting_disk)`,
/// where the basis is the disk capacity *reachable by Freenet* (bytes it already
/// occupies plus bytes still free on the same mount), NOT a bare free-space read.
/// Anchoring on `used + available` keeps the budget stable as the disk fills
/// (a naive free-space basis would shrink the budget toward zero exactly when
/// the node needs the eviction floor most).
///
/// `pct` is clamped to `[0.0, 1.0]` and non-finite / negative values collapse to
/// `0.0`, so a mis-set config can only ever produce a valid budget. All fixed-
/// point conversion saturates rather than wrapping, so a huge `used + available`
/// cannot overflow into a small budget.
///
// Default-bound convenience entry: the live recompute path (#4702) uses
// `disk_budget_for_clamped` with the operator cap, so this constant-bound form
// stays test-only.
#[allow(dead_code)]
pub fn disk_budget_for(freenet_used: u64, available: u64, pct: f64) -> u64 {
    disk_budget_for_clamped(
        freenet_used,
        available,
        pct,
        MIN_DEFAULT_HOSTING_BUDGET_BYTES,
        DEFAULT_MAX_HOSTING_DISK_BYTES,
    )
}

/// [`disk_budget_for`] with explicit MIN/MAX clamp bounds, so the operator
/// override (`--max-hosting-disk`) can supply the ceiling. Kept separate from
/// the constant-bound wrapper so the pure math has one implementation.
pub fn disk_budget_for_clamped(
    freenet_used: u64,
    available: u64,
    pct: f64,
    min_bytes: u64,
    max_bytes: u64,
) -> u64 {
    // A non-finite or negative pct is meaningless — collapse to 0 (which the
    // clamp then lifts to the MIN floor), never NaN-propagate into the budget.
    let pct = if pct.is_finite() {
        pct.clamp(0.0, 1.0)
    } else {
        0.0
    };
    let basis = freenet_used.saturating_add(available);
    // f64 has 52 bits of mantissa; a basis above 2^52 loses precision but the
    // result is clamped to `max_bytes` anyway, so the loss is immaterial.
    let raw = (basis as f64) * pct;
    // Saturating fixed-point conversion: a raw value beyond u64::MAX (only
    // reachable via precision noise near the top of the range) clamps down
    // rather than wrapping. `as u64` on a non-finite/negative f64 is 0 in Rust,
    // but `raw` is already non-negative and finite here.
    let budget = if raw >= u64::MAX as f64 {
        u64::MAX
    } else {
        raw as u64
    };
    // If the operator set max below min, MIN wins (a usable floor beats an
    // inconsistent zero cap): clamp lower bound last.
    budget.min(max_bytes).max(min_bytes)
}

/// Point-in-time hosting-cache resource gauges for per-node telemetry.
///
/// Aggregate scalars only (never per-contract), emitted on the existing
/// `RouterSnapshot` cadence so the capability-relative budget's behavior is
/// observable in production (design principle: instrumentation is horizontal —
/// see `docs/design/hosting-eviction.md`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct HostingCacheStats {
    /// Configured byte budget (the RAM-scaled default, or the operator override).
    pub budget_bytes: u64,
    /// Current tracked contract-state bytes. The occupancy (utilization) ratio
    /// is `current_bytes / budget_bytes`; headroom is `1 - that`. The collector
    /// derives whichever it wants from these two raw scalars.
    pub current_bytes: u64,
    /// Number of contracts currently in the hosting cache.
    pub contract_count: u64,
    /// Monotonic count of contracts evicted specifically because the cache was
    /// over budget. The collector differences it across the cadence to derive a
    /// budget-triggered eviction rate.
    pub budget_evictions_total: u64,
    /// Monotonic count of over-budget evictions whose victim had been READ
    /// (GET/SUBSCRIBE) more than once during its residency (`read_count >= 2`,
    /// i.e. genuine repeat demand, not a one-off seed). Under a well-calibrated
    /// demand-ordered policy this stays near zero — evicting a repeatedly-requested
    /// contract is the #4338 miscalibration symptom (a real-demand contract
    /// evicted ahead of junk). A rising rate here (differenced against
    /// [`Self::budget_evictions_total`]) is the alarm that the demand estimate
    /// is mis-ordering the working set.
    pub evictions_of_recently_read_total: u64,
    /// Monotonic count of over-budget evictions whose victim was a genuine
    /// fresh-THIS-run PUT seed evicted before any read — `read_count == 0 &&
    /// seeded_this_run` (a live `record_access` insert, NOT a reload from
    /// persistence). This is the field falsifier for the "a PUT seeds a contract
    /// but does NOT count as read-demand" design decision
    /// (freenet/freenet-core#4642): if seeds are routinely evicted before their
    /// first reader, the demand-driven policy is discarding fresh content before
    /// it can be found. Differenced against [`Self::budget_evictions_total`] this
    /// gives the fraction of evictions that killed an unread seed. Disjoint from
    /// [`Self::evictions_of_recently_read_total`] (`read_count >= 2`): the
    /// `read_count == 1` band is counted by neither.
    ///
    /// The `seeded_this_run` restriction is load-bearing: a restart reloads the
    /// whole persisted hosted set with `read_count` reset to 0 and `inserted_at`
    /// reset to `now`, so a long-hosted, previously-read contract shed in the
    /// post-load over-budget burst would otherwise masquerade as an unread seed
    /// dying at age ≈ 0 — a false positive for this exact alarm on every restart.
    /// See [`HostedContract::seeded_this_run`].
    pub evicted_unread_total: u64,
    /// Running sum, in whole seconds, of the age-at-eviction (`now - inserted_at`)
    /// of every eviction counted by [`Self::evicted_unread_total`]. Mean unread-
    /// seed lifetime = this / `evicted_unread_total`; carried as a sum (not a
    /// pre-divided mean) so the collector can difference both across the snapshot
    /// cadence and recover the windowed mean. Answers "HOW YOUNG do seeds die?" —
    /// a small mean means seeds are evicted almost immediately after the PUT.
    pub evicted_unread_age_secs_sum: u64,
    /// Monotonic count of OOM-valve evictions — any eviction performed under
    /// [`MemoryPressure::Overflow`] (counted by pressure, so a concurrent
    /// subscription change cannot perturb it). Under Overflow the sweep sheds
    /// fewest-subscriber-first and pierces the in-use pin, so these MAY be
    /// subscribed contracts shed to avoid OOM. The Overflow trigger is
    /// intentionally unwired in production (see [`MemoryPressure`]), so this
    /// stays 0 in the field today; once the RSS trigger is plumbed, a nonzero
    /// differenced rate is the alarm that the node is shedding to avoid OOM.
    pub oom_valve_evictions_total: u64,
    /// Monotonic count of over-budget evictions whose victim still had ≥1
    /// subscriber (local client OR downstream peer) at eviction-decision time —
    /// i.e. a SUBSCRIBED contract shed by the subscriber-primary ordering
    /// (#4642, invariant 3). Counted by the captured `(local + downstream)`
    /// count, regardless of pressure, so it covers BOTH the normal AtCapacity
    /// last-resort shed (nothing zero-subscriber eligible) AND the Overflow
    /// valve. This is the FIELD FALSIFIER for the single riskiest new behavior
    /// in the subscriber-primary rework: shedding a subscribed contract. Unlike
    /// [`Self::oom_valve_evictions_total`] (which stays 0 until the Overflow
    /// trigger is wired), this can go nonzero the moment the eviction rework
    /// ships, so a rising differenced rate here is the signal to check whether
    /// budgets are too tight / demand is churning subscribed contracts.
    pub subscribed_evictions_total: u64,
    /// Monotonic count of COST-PRESSURE evictions (cost-aware eviction,
    /// #4861): zero-demand contracts shed because their attributed update-work
    /// cost (WASM CPU / broadcast fan-out) dominated the node's total on a
    /// cost axis, independently of the byte budget. Disjoint from
    /// [`Self::budget_evictions_total`] (byte-budget-triggered evictions
    /// only). The field falsifier for the cost trigger: a nonzero differenced
    /// rate means the node is actively shedding cost offenders; a runaway rate
    /// means the floors / share threshold are miscalibrated and churning cheap
    /// contracts.
    pub cost_evictions_total: u64,
}

/// Per-contract Greedy-Dual priority row for the local-peer dashboard.
///
/// This is what actually governs retention today (piece A of the
/// demand-driven hosting redesign, #4642): the over-budget walk evicts the
/// lowest `keep_score` first. Surfaced on the dashboard so an operator can
/// see the live demand-ordered eviction policy — the mechanism that
/// replaced the dormant MAD governance detector (#4296). Collected under a
/// single cache read lock by [`HostingCache::eviction_ordered_scores`].
#[derive(Debug, Clone, Copy)]
pub(crate) struct HostingContractScore {
    pub key: ContractKey,
    /// Greedy-Dual priority = `eviction_floor + predicted_demand` at the last
    /// refresh. Lowest evicts first.
    pub keep_score: f64,
    /// Stored per-contract read-demand estimate (reads/second).
    pub predicted_demand: f64,
    /// Per-contract memory cost (state bytes).
    pub size_bytes: u64,
    /// Read accesses (GET/SUBSCRIBE) observed over this entry's residency.
    pub read_count: u32,
    /// Monotonic access sequence at the entry's most recent real GET or PUT
    /// (genuine client access) — the eviction recency tiebreak (subscriber-
    /// primary rework, #4642). The cache orders zero-subscriber eviction
    /// candidates ascending by this. SUBSCRIBE / renewal traffic does NOT bump it.
    pub recency_seq: u64,
}

/// Type of access that adds/refreshes a contract in the hosting cache.
///
/// Only certain operations should refresh the LRU position to prevent manipulation:
/// - GET: User requesting the contract
/// - PUT: User writing new state
/// - SUBSCRIBE: User subscribing to updates
///
/// UPDATE is explicitly excluded because contract creators control when updates happen,
/// which could be abused to keep contracts cached indefinitely.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AccessType {
    Get,
    Put,
    /// Used in tests and reserved for future use when explicit SUBSCRIBE triggers hosting
    #[cfg_attr(not(test), allow(dead_code))]
    Subscribe,
}

/// Coarse memory-pressure state handed to the over-budget eviction sweep, which
/// implements invariant 3's ONE demand-ordered eviction decision. Kept minimal
/// because the eviction sweep only runs once
/// the byte budget is already exceeded, so it only needs to distinguish "at
/// capacity" from "genuine RAM overflow".
///
/// # There is ONE eviction ordering; the two variants only differ on the op-scoped backstop
///
/// Since the subscriber-primary eviction rework (#4642, Ian's confirmed model)
/// BOTH variants shed the fewest-`(local, downstream)`-subscriber contract using
/// the same [`victim_order`] — a subscribed contract is NOT hard-pinned under
/// either, and there is no `min_ttl` cold-start floor (dropped 2026-07-08). The
/// only difference is the op-scoped backstop:
///
/// - [`Self::AtCapacity`] never evicts the `protected` key (the contract whose
///   in-flight access triggered this sweep — the op-scoped backstop guard;
///   invariant 3, decision 2026-07-08). This is what production passes.
/// - [`Self::Overflow`] pierces that backstop, so a node at genuine RAM-overflow
///   can shed even a contract with an in-flight op to survive. This is "a corner
///   of the same eviction, not a separate valve" (hosting-invariants Resolved
///   decisions); it is NOT a second ranking.
///
/// The `Overflow` (backstop-pierce) TRIGGER is intentionally UNWIRED in
/// production: it needs a genuine RSS/A1 resource signal, which is NOT plumbed.
/// Production callers (`HostingManager::sweep_expired_hosting`,
/// `record_contract_access`) pass [`Self::AtCapacity`] unconditionally. Note
/// that — unlike the shipped #4720 code — AtCapacity CAN now shed a subscribed
/// contract (as a last resort), so shedding subscribed contracts is live in the
/// field; only the op-backstop PIERCE remains gated behind the unwired trigger.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MemoryPressure {
    /// Over the byte budget but not at genuine RAM-overflow risk. Every entry is
    /// eligible EXCEPT the op-scoped `protected` key; victims are chosen fewest-
    /// `(local, downstream)`-subscriber first, so a subscribed contract is ordered
    /// LAST and shed only as a last resort (when nothing with fewer subscribers is
    /// eligible and the peer is still over budget). This is the value production
    /// always passes.
    AtCapacity,
    /// Genuine RAM overflow: the same fewest-subscriber ordering, but ALSO
    /// pierces the op-scoped backstop so a contract with an in-flight op can be
    /// shed to survive OOM. NOTHING produces this value in production yet.
    ///
    /// `dead_code`-allowed in non-test builds ON PURPOSE: the backstop-pierce
    /// trigger (a genuine RSS/A1 resource signal) is intentionally unwired, so
    /// only the mechanism unit tests construct `Overflow`. Remove the allow when
    /// the trigger is plumbed in a later, separately-validated step.
    ///
    /// Subscription-state teardown for a shed subscribed contract is handled the
    /// SAME way as the AtCapacity last-resort shed: the `was_in_use` flag on the
    /// returned [`EvictedContract`] drives
    /// `HostingManager::teardown_evicted_in_use_contract`, so a valve eviction is
    /// not "undone in practice" by phantom demand. When the trigger is wired,
    /// prefer ranking by the LEASE-FILTERED downstream count
    /// (`HostingManager::downstream_subscriber_peers`) rather than the unfiltered
    /// map length, so stale-unswept leases don't mis-rank real demand.
    #[cfg_attr(not(test), allow(dead_code))]
    Overflow,
}

/// Result of recording a contract access in the hosting cache.
#[derive(Debug)]
pub struct RecordAccessResult {
    /// Whether this contract was newly added (vs. refreshed existing)
    pub is_new: bool,
    /// Contracts that were evicted to make room, paired with the
    /// state-write generation captured atomically under the hosting
    /// cache's write lock. Carried through `EvictContract` so the
    /// deletion-time guard in `RuntimePool::remove_contract` can detect
    /// a re-host that occurred between eviction and disk reclamation.
    pub evicted: Vec<(ContractKey, u64)>,
    /// Subset of [`Self::evicted`] whose victim still had ≥1 subscriber (a
    /// local client subscription OR a downstream peer subscriber) at the
    /// eviction-decision instant — i.e. `contract_in_use` was true for it.
    ///
    /// Under the subscriber-primary ordering (#4642, invariant 3) an
    /// over-budget peer sheds the fewest-`(local, downstream)`-subscriber
    /// contract, which — when nothing zero-subscriber is eligible — is a
    /// SUBSCRIBED contract. The `HostingManager` MUST tear down that
    /// contract's subscription state (see
    /// `HostingManager::teardown_evicted_in_use_contract`) so
    /// `contract_in_use` flips to false BEFORE `reclaim_evicted_contract`
    /// runs; otherwise the reclaim gate skips the deletion forever and the
    /// eviction frees the cache entry but not the on-disk state (the drift
    /// the shipped #4720 code carried). Captured atomically with the
    /// eviction decision to avoid a racy post-hoc `contract_in_use` re-read.
    pub evicted_in_use: Vec<ContractKey>,
    /// The subscription state that `HostingManager::teardown_evicted_in_use_contract`
    /// actually removed for each [`Self::evicted_in_use`] victim (one entry per
    /// key). Populated by the `HostingManager` AFTER `evicted_in_use` drives the
    /// teardown (empty at the cache layer, which decides the eviction but does
    /// not own the subscription maps or the peer identities). The eviction
    /// CONSUMER replays these removals against the `InterestManager` (which
    /// lives on `OpManager`, not `HostingManager`) via
    /// [`InterestManager::remove_evicted_in_use`](crate::ring::interest::InterestManager::remove_evicted_in_use)
    /// so no ghost `interested_peers` / `peer_contracts` / `local_client_count`
    /// entry survives to mis-target UPDATE broadcasts or inflate upstream
    /// interest counts (PR #4734 Fix 1).
    pub evicted_in_use_teardown: Vec<EvictedInUseTeardown>,
    /// Observed read-rate training sample (reads/second) for the accessed
    /// contract, or `None` when no meaningful rate is available (a seed/PUT, a
    /// brand-new entry, or a read with zero residency). `HostingManager` pairs
    /// this with the contract's ring distance and feeds it to the proximity
    /// prior (`super::demand::ProximityPrior`). Purely a training signal — it
    /// never affects THIS access's `keep_score`.
    pub observed_read_rate: Option<f64>,
}

/// The subscription state `HostingManager::teardown_evicted_in_use_contract`
/// removed from the hosting maps for a single subscriber-primary eviction that
/// shed a still-in-use contract. Carries exactly what the eviction consumer
/// needs to replay against the `InterestManager` (per-peer downstream removals +
/// a per-contract local-client count) so the two managers stay in sync. See
/// [`RecordAccessResult::evicted_in_use_teardown`] and
/// [`InterestManager::remove_evicted_in_use`](crate::ring::interest::InterestManager::remove_evicted_in_use).
#[derive(Debug, Clone)]
pub struct EvictedInUseTeardown {
    /// The evicted contract.
    pub key: ContractKey,
    /// Downstream peer leases removed from `downstream_subscribers[key]`. The
    /// consumer replays `remove_peer_interest(&key, peer)` +
    /// `remove_downstream_subscriber(&key)` per peer (the
    /// `handle_unsubscribe_inbound` pair).
    pub downstream_peers: Vec<PeerKey>,
    /// Number of local WebSocket client subscriptions removed from
    /// `client_subscriptions[key.id()]`. The consumer replays
    /// `remove_local_client(&key)` once per client (the client-disconnect path).
    pub local_client_count: usize,
}

/// A single contract evicted by the over-budget sweep, with the metadata the
/// `HostingManager` needs to (a) drive disk reclamation and (b) — for a
/// subscriber-primary eviction that shed a still-in-use contract — tear down
/// the subscription state pinning it. See [`RecordAccessResult::evicted_in_use`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct EvictedContract {
    /// The evicted contract's key.
    pub key: ContractKey,
    /// State-write generation snapshot captured atomically with the eviction
    /// decision; carried through `EvictContract` for the deletion-time re-host
    /// guard in `RuntimePool::remove_contract`.
    pub write_generation: u64,
    /// `true` iff the victim had ≥1 subscriber (local client OR downstream peer)
    /// at eviction-decision time — the flag the manager keys its subscription
    /// teardown on. Zero-subscriber evictions leave this `false` (nothing to
    /// tear down; `contract_in_use` is already false for them).
    pub was_in_use: bool,
}

/// Metadata about a hosted contract.
#[derive(Debug, Clone)]
pub struct HostedContract {
    /// Size of the contract state in bytes
    pub size_bytes: u64,
    /// Last time this contract was accessed (via GET/PUT/SUBSCRIBE). Used as the
    /// restart-reload recency tiebreak (`finalize_loading`) and to age-gate local-
    /// client renewal; it no longer gates eviction eligibility (the `min_ttl` floor
    /// was dropped 2026-07-08 — see [`Self::recency_seq`]).
    pub last_accessed: Instant,
    /// Monotonic access sequence number stamped on every access (insert / read /
    /// seed / touch).
    ///
    /// UNUSED, pending deletion: it was the old eviction tiebreak, which the
    /// subscriber-primary rework (#4642) replaced with [`Self::recency_seq`]
    /// (real-GET recency). No production reader remains (only a `#[cfg(test)]`
    /// assertion via the `get()` accessor keeps it from tripping dead-code). It
    /// is still maintained here to avoid churning the write sites in this PR;
    /// slated for removal with the rest of the demoted machinery once
    /// subscriber-primary is field-validated. Do NOT add new readers.
    pub last_access_seq: u64,
    /// Monotonic access sequence at this entry's most recent **real GET or PUT
    /// access** (genuine client access) — the eviction recency tiebreak among
    /// equal-`(local, downstream)`-subscriber candidates (subscriber-primary
    /// rework, #4642; GET-or-PUT extension 2026-07-08).
    ///
    /// Stamped (to the shared `access_seq` value) by an `AccessType::Get` or
    /// `AccessType::Put` access, by `touch_with_demand` (a local-cache GET serve),
    /// and — reset to the current frontier — at subscription TERMINATION
    /// (`record_abandonment`, so a formerly-subscribed contract is not instantly
    /// evicted for an old last-read accrued while it sat in the subscription tier).
    /// It is deliberately NOT bumped by SUBSCRIBE (a subscription, not a read/
    /// write), by subscription RENEWAL traffic, or by a restart reload — so that
    /// "recently accessed by a client" means exactly that. Without excluding
    /// renewal traffic, every subscribed contract would stay equally "fresh" and
    /// the tiebreak would be useless.
    ///
    /// A never-accessed entry (a fresh SUBSCRIBE-only seed, or a restart-reloaded
    /// entry) has `recency_seq == 0`, so among zero-subscriber candidates it sorts
    /// to evict first. A fresh PUT resets `recency_seq` at PUT time, so it is
    /// protected by being the most-recently-accessed without any `min_ttl` floor
    /// (dropped 2026-07-08).
    pub recency_seq: u64,
    /// Wall-clock instant of this entry's most recent **genuine GET/PUT
    /// access** — the same events that stamp [`Self::recency_seq`], recorded
    /// as a TIME so the cost-pressure eviction candidacy (cost-aware
    /// eviction, #4861) can ask "was this contract genuinely read or written
    /// within the cost window?" (`recency_seq` is an ordering, not a clock,
    /// so it cannot answer that). Stamped by GET/PUT in `record_access*`, by
    /// `touch_with_demand` (local-serve GET), and at subscription TERMINATION
    /// (`record_abandonment` — the recency clock starts there, invariant 3),
    /// mirroring `recency_seq` exactly. NOT stamped by SUBSCRIBE, renewal, or
    /// UPDATE traffic — a storm contract whose only activity is its own
    /// update churn stays cost-evictable, while a genuinely-read zero-
    /// subscriber contract (the River UI-container class invariant 3
    /// protects) is not a cost candidate. `None` = never genuinely accessed
    /// this run (SUBSCRIBE-only seeds, restart reloads).
    pub last_genuine_access: Option<Instant>,
    /// Demand-ordered (Greedy-Dual) priority: `eviction_floor + predicted_demand` captured at the
    /// last read-demand refresh (or at insert). The over-budget walk evicts the
    /// entry with the lowest `keep_score` (ties broken by `last_accessed`). A
    /// read (GET/SUBSCRIBE) recomputes this against the CURRENT `eviction_floor`,
    /// so recently-requested contracts sit at the frontier while never-requested
    /// ones fall behind as the floor ratchets up. See the module docs.
    pub keep_score: f64,
    /// Per-contract read-demand estimate (reads/second) from the proximity-prior
    /// estimator, supplied by the caller at insert / read-refresh. Stored so a
    /// `touch` (which has no estimator access) can recompute `keep_score` from
    /// the current floor without recomputing the prior. For A3 this is the
    /// proximity prior only; A4 blends it toward the contract's own observed
    /// read rate.
    pub predicted_demand: f64,
    /// Number of read accesses (GET/SUBSCRIBE, including `touch`) observed over
    /// this entry's residency. A PUT is a SEED, not a read, so it does not
    /// increment this. Drives (a) the observed-rate training sample fed back to
    /// the proximity prior (`read_count / residency`) and (b) the
    /// `evictions_of_recently_read` miscalibration counter (`>= 2` = genuine
    /// repeat demand).
    ///
    /// Note: a single client GET of a hosted-but-stale contract can bump this
    /// twice — once via `touch` (the pre-decision TTL refresh in the client GET
    /// handler) and once via the network refetch's `record_access(Get)`. This is
    /// a bounded (2x) over-count on the stale-refetch path only; it slightly
    /// inflates the trained rate and can make `evictions_of_recently_read` fire
    /// for a contract read just once. Both are soft, directional signals, so the
    /// inaccuracy is accepted rather than deduplicated (which would require
    /// threading GET-request identity through both paths).
    pub read_count: u32,
    /// When this entry was first inserted. Used with `read_count` to derive an
    /// observed read-rate training sample for the proximity prior.
    pub inserted_at: Instant,
    /// Whether this entry was inserted fresh during THIS process run (a live
    /// `record_access`), as opposed to reloaded from persisted state at startup
    /// (`load_persisted_entry_with_demand`).
    ///
    /// Sole purpose: keep the PUT-durability `evicted_unread_total` falsifier
    /// honest across restarts. A reload resets `read_count` to 0 and
    /// `inserted_at` to `now` for the ENTIRE persisted hosted set, so a
    /// long-hosted, previously-read contract looks identical to a brand-new
    /// unread seed (`read_count == 0`, age ≈ 0) the moment it is reloaded. Since
    /// load does not evict but the node is frequently over budget afterwards, the
    /// first `record_access` sheds a burst of the oldest reloaded entries — which
    /// would otherwise register as a false "unread seeds die instantly" spike on
    /// every restart (and restarts are frequent under the auto-update cadence).
    /// Gating the unread-seed counter on `read_count == 0 && seeded_this_run`
    /// excludes reloaded entries from the falsifier. Keying on
    /// `access_type == Put` is NOT sufficient: a reloaded entry whose last
    /// pre-restart access was a PUT still has `access_type == Put`.
    pub seeded_this_run: bool,
    /// Type of the last access
    pub access_type: AccessType,
    /// Whether a local client (HTTP/WebSocket) accessed this contract.
    /// Only flagged contracts get subscription renewal and local-cache serving.
    pub local_client_access: bool,
    /// When the local client last accessed this contract. Age-gates the renewal:
    /// contracts not accessed locally within SUBSCRIPTION_LEASE_DURATION stop
    /// being renewed (AGENTS.md cleanup exemption rule). None = never locally accessed.
    pub local_client_last_access: Option<Instant>,
    /// State-write generation observed when this entry was inserted or
    /// refreshed. Captured atomically under the hosting cache's write
    /// lock — passed in by `HostingManager::record_contract_access` from
    /// `Ring::state_generation`. Carried through `EvictContract` and
    /// re-checked at deletion time to close the re-host race. See
    /// `RecordAccessResult.evicted` and `RuntimePool::remove_contract`.
    pub write_generation: u64,
    /// When this contract transitioned from "in use" to "no longer in use"
    /// (last subscriber / client / downstream subscriber went away). `Some`
    /// means the entry is now a recently-abandoned eviction candidate;
    /// `None` means the entry has never been abandoned, or has been
    /// re-accessed since its last abandonment.
    ///
    /// The abandonment hook drops the entry's `keep_score` to the current
    /// `eviction_floor` (stripping its demand credit) so that under budget
    /// pressure it is evicted before still-active entries, whose `keep_score` is
    /// `floor + demand`. The proximity prior (`predict`) is strictly positive, so
    /// an active entry sorts strictly above an abandoned one at the floor; a
    /// zero-demand tie is unreachable through the estimator (it could only arise
    /// from an explicit zero handed in by a caller). Once past TTL,
    /// `evict_over_budget` picks the lowest `keep_score` first. Refreshing the
    /// entry via a read (`record_access` / `touch`) clears this field and
    /// restores its `keep_score` to `floor + predicted_demand`.
    ///
    /// Idle contracts with no subscribers but recent demand are NOT
    /// affected: they keep their earned `keep_score`. Only entries that
    /// were actively in use and then lost all in-use signals get their
    /// demand credit stripped, since their recency would otherwise keep
    /// them ahead despite no longer mattering.
    ///
    /// The timestamp itself is written but not read by eviction logic
    /// (eviction priority comes from `keep_score`, which `record_abandonment`
    /// adjusts as a side effect). It is retained
    /// for the governance dashboard (PR #4270) so an operator can see
    /// how long ago a flagged contract was last actively used. A future
    /// refactor that removes the field can do so only after the
    /// dashboard reader is also dropped — code-first reviewer of #4260
    /// flagged this as a "currently write-only" foot-gun worth pinning
    /// in the rustdoc.
    pub abandoned_at: Option<Instant>,
}

/// Unified hosting cache with a byte-budget and subscriber-primary eviction.
///
/// This cache maintains contracts that this peer is "hosting" - keeping available
/// for the network. The cache has:
/// - Byte budget: Large contracts consume more budget. The budget is measured
///   in tracked contract **state** bytes only; WASM code blobs and database
///   overhead are not counted against it.
/// - Subscriber-primary eviction: when over budget the victim is chosen by
///   ascending `(local_subscription_count, downstream_subscriber_count,
///   recency_seq, key_bytes)` (see [`victim_order`] and the module docs). There
///   is no `min_ttl` cold-start floor (dropped 2026-07-08); a fresh contract is
///   protected by being the most-recently-accessed under the recency ordering.
///
/// # Subscription Renewal
///
/// ALL contracts in this cache should have their subscriptions renewed automatically.
/// This is the key fix for the bug where GET-triggered subscriptions weren't being renewed.
// NAMING LANDMINE: "cache" here = HOSTING, not a lesser tier. Entries are contracts
// this peer HOSTS (WASM+state, kept fresh in the update mesh) because a GET/PUT
// routed through it, and a routed GET/PUT is demand. No cache tier exists; to be
// renamed hosting-* after 0.2.94. See .claude/rules/hosting-invariants.md + #4642.
pub struct HostingCache<T: TimeSource> {
    /// Maximum bytes to use for cached contracts
    budget_bytes: u64,
    /// Current total bytes used
    current_bytes: u64,
    /// Contract metadata indexed by key. Eviction order is derived from each
    /// entry's subscriber counts + `recency_seq` (via [`victim_order`]), not from
    /// a separate LRU list, so there is no second structure to keep in sync.
    contracts: HashMap<ContractKey, HostedContract>,
    /// Monotonic access counter. Incremented on every access (insert / read /
    /// seed / touch) and stamped onto the entry's `last_access_seq`, giving an
    /// exact least-recently-accessed tiebreak for eviction that is independent
    /// of mock-clock granularity.
    access_seq: u64,
    /// Greedy-Dual aging value. Monotonically non-decreasing:
    /// each over-budget eviction ratchets it up to the evicted contract's
    /// `keep_score`. A read refreshes the read contract's `keep_score` to
    /// `eviction_floor + predicted_demand`, so the floor is the moving frontier
    /// that separates still-wanted contracts from stale ones. Measured in
    /// cache-contention units, not wall-clock.
    eviction_floor: f64,
    /// Monotonic count of contracts evicted because the cache was over budget.
    /// Only `evict_over_budget` increments it, so it counts budget-triggered
    /// evictions specifically (not TTL sweeps that found nothing over budget).
    /// Exposed via [`HostingCache::stats`] for per-node telemetry.
    budget_evictions_total: u64,
    /// Monotonic count of over-budget evictions whose victim had `read_count >= 2`
    /// (genuine repeat demand). The #4338 miscalibration signal — see
    /// [`HostingCacheStats::evictions_of_recently_read_total`].
    evictions_of_recently_read_total: u64,
    /// Monotonic count of over-budget evictions whose victim had `read_count == 0`
    /// (an unread seed). The PUT-durability field falsifier — see
    /// [`HostingCacheStats::evicted_unread_total`].
    evicted_unread_total: u64,
    /// Running sum (whole seconds) of the age-at-eviction of unread-seed evictions.
    /// See [`HostingCacheStats::evicted_unread_age_secs_sum`].
    evicted_unread_age_secs_sum: u64,
    /// Monotonic count of evictions performed by the OOM valve — i.e. any
    /// eviction under [`MemoryPressure::Overflow`], counted by PRESSURE (not by
    /// the victim's subscriber count) so a concurrent subscription change can
    /// never perturb it. Under Overflow the sweep sheds fewest-subscriber-first
    /// and pierces the in-use pin, so these evictions MAY be subscribed
    /// contracts (real demand) shed to avoid OOM. Because the Overflow trigger is
    /// intentionally unwired in production (see [`MemoryPressure`]), this stays 0
    /// in the field today; it is exercised only by the valve-mechanism unit tests
    /// and, once the RSS trigger is plumbed, becomes the alarm for how often the
    /// node is shedding to avoid OOM.
    oom_valve_evictions_total: u64,
    /// Monotonic count of over-budget evictions whose victim was SUBSCRIBED
    /// (`local + downstream >= 1`) at eviction-decision time — the field
    /// falsifier for shedding a subscribed contract. Counted by the captured
    /// subscriber count regardless of pressure (covers the normal AtCapacity
    /// last-resort shed AND the Overflow valve). See
    /// [`HostingCacheStats::subscribed_evictions_total`].
    subscribed_evictions_total: u64,
    /// Monotonic count of cost-pressure evictions (cost-aware eviction,
    /// #4861). Only [`Self::evict_cost_pressure`] increments it. See
    /// [`HostingCacheStats::cost_evictions_total`].
    cost_evictions_total: u64,
    /// Time source for testability
    time_source: T,
}

/// The canonical eviction **victim ordering** (subscriber-primary, #4642).
///
/// Given the four ranking components of two hosted-contract candidates —
/// `local_subscription_count`, `downstream_subscriber_count`, `recency_seq`,
/// and the `ContractKey` — returns the [`Ordering`](std::cmp::Ordering) that
/// sorts them ascending by
/// `(local_subscription_count, downstream_subscriber_count, recency_seq, key_bytes)`:
///
/// 1. fewest **local client subscriptions** first — a contract THIS node's own
///    client is subscribed to is evicted LAST (Ian's confirmed ordering, #4642);
/// 2. then fewest **downstream subscribers** (forwarded demand);
/// 3. then least-recent real **GET** (`recency_seq`, a recency tiebreak — NOT a
///    primary key; subscription-renewal traffic must not refresh it);
/// 4. then contract-key bytes as a final deterministic tiebreak.
///
/// The candidate that sorts FIRST under this order is the one evicted first. A
/// locally-subscribed contract is ordered LAST but NOT absolutely pinned: in the
/// extreme where every eligible contract carries a local subscription and the
/// peer is still over budget, the least-recently-read local one IS the victim
/// (accepted last resort — see [`HostingCache::evict_over_budget`] and
/// hosting-invariants invariant 3).
///
/// This is the single reusable entry point for eviction ranking:
/// [`HostingCache::evict_over_budget`] sorts victims with it. Do NOT re-add
/// distance to this key — its causal pull on demand already flows through the
/// subscriber counts via keyward routing gravity, and byte-only / distance-based
/// eviction are the anti-patterns the hosting-invariants rule exists to keep out
/// (see `.claude/rules/hosting-invariants.md`).
pub(crate) fn victim_order(
    a: (usize, usize, u64, &ContractKey),
    b: (usize, usize, u64, &ContractKey),
) -> std::cmp::Ordering {
    let (a_local, a_down, a_seq, a_key) = a;
    let (b_local, b_down, b_seq, b_key) = b;
    a_local
        .cmp(&b_local)
        .then_with(|| a_down.cmp(&b_down))
        .then_with(|| a_seq.cmp(&b_seq))
        .then_with(|| a_key.as_bytes().cmp(b_key.as_bytes()))
}

// =============================================================================
// Cost-pressure eviction (cost-aware eviction, #4861)
// =============================================================================
//
// The byte budget above measures on-disk STATE bytes only, so a tiny contract
// that burns the node's update-work capacity (WASM CPU on every apply and
// every per-target send, per-message overhead on every broadcast) never
// creates eviction pressure: the #4861 storm contract was 121 bytes of state
// holding ~58% of a gateway's broadcast capacity with ZERO subscribers, and
// the byte-gated sweep never ran. Cost pressure closes that gap: the periodic
// sweep additionally reads the per-contract attributed cost rates from the
// topology meter — `ExecCpuMicros` (apply + probe + per-target
// summarize/delta WASM), `BroadcastFanoutCost` (payload × targets, the
// uplink-volume dimension), and `BroadcastMessagesSent` (per-peer send COUNT,
// the per-send-overhead dimension and the load-bearing storm signal: a
// tiny-payload high-frequency storm is invisible in bytes but reads its true
// message rate here) — and, when the node's total update-work on an axis is
// above a modest absolute floor AND some zero-demand contract holds more than
// [`COST_SHARE_THRESHOLD`] of it, sheds that contract, even while comfortably
// under the byte budget.
//
// The trigger is deliberately SCALE-FREE (a share of the node's own observed
// work, not an absolute capacity number) so day one needs no per-hardware
// calibration; the absolute floors only keep an idle node from evicting the
// one contract that does any work at all. Two guards keep it honest:
// SUSTAINED pressure (the meter read admits a contract into candidacy only
// once its reporting is at least half the cost window old, so a single large
// fan-out burst can never mass-evict — see `Meter::contract_cost_rates`) and
// the saturated-buffer true-rate fix (a full sample ring divides by its
// actual span, so a sustained high-frequency storm's real rate is
// representable instead of being count-truncated below the floor).
//
// Candidacy is restricted to ZERO-DEMAND contracts in invariant 3's full
// sense: `local_subs == 0 && downstream_subs == 0` AND no genuine GET/PUT
// within the cost window AND no local-client access within the reconciliation
// renewal lease (`SUBSCRIPTION_LEASE_DURATION` — so the cost sweep never sheds
// a contract the renewal loop still leases, including for one lease window
// after a restart) AND `attributed_cost > 0`. A SUBSCRIBED contract is
// NEVER a cost-eviction candidate, and neither is an actively-read one (the
// never-subscribed-but-read River UI-container class) — the subscriber-
// primary tiers of [`victim_order`] are not reordered by cost (cost breaks
// ties only WITHIN the zero-demand tier), and the #4296 "River room evicted
// for being popular" false positive is unreachable by construction, not
// merely unlikely. A storm contract stays evictable because its only
// activity is its own UPDATE churn, which never counts as genuine access.
// See `.claude/rules/hosting-invariants.md` invariant 3.

/// Share of the node's total attributed update-work on one cost axis above
/// which a single zero-demand contract is considered a cost offender. The
/// scale-free half of the cost-pressure trigger.
pub(crate) const COST_SHARE_THRESHOLD: f64 = 0.25;

/// Absolute floor for the node-total `ExecCpuMicros` rate (µs of WASM
/// `update_state` execution per second) below which the CPU axis never
/// triggers cost pressure. 50_000 µs/s = 5% of one core spent applying
/// contract updates, sustained across the meter window — a node doing less
/// update work than that is not under CPU pressure regardless of shares.
/// Day-one calibration; the scale-free share above is the primary signal.
pub(crate) const EXEC_CPU_PRESSURE_FLOOR_MICROS_PER_SEC: f64 = 50_000.0;

/// Absolute floor for the node-total `BroadcastFanoutCost` rate (payload
/// bytes × target count per second) below which the fan-out axis never
/// triggers cost pressure. 128 KiB/s of intended fan-out egress (~1 Mbit/s)
/// is a modest but real share of a small peer's uplink. Day-one calibration;
/// the scale-free share above is the primary signal.
pub(crate) const BROADCAST_FANOUT_PRESSURE_FLOOR_BYTES_PER_SEC: f64 = 128.0 * 1024.0;

/// Absolute floor for the node-total `BroadcastMessagesSent` rate (per-peer
/// broadcast messages per second) below which the message-COUNT axis never
/// triggers cost pressure.
///
/// This axis is the load-bearing storm signal (#4861): the observed
/// production profile — a 121-byte contract fanning to ~58 co-hosts at
/// ~36 per-peer sends/s, sustained — reads as only ~4 KB/s on the byte axis
/// (~30x under its floor) while dominating the gateway's real broadcast
/// capacity through per-send overhead (syscall/encryption/queue work +
/// per-target summarize WASM). Counted as MESSAGES it reads as its true
/// ~36/s, comfortably over this floor. Calibration: 10 msgs/s sustained
/// across the 5-min window = 3000 sends of overhead — real load on any
/// hardware — while a lightly-used legitimate contract (a chat room fanning
/// a few updates a minute to a handful of peers) stays well under 1/s.
pub(crate) const BROADCAST_MESSAGES_PRESSURE_FLOOR_PER_SEC: f64 = 10.0;

/// Minimum averaging window for the cost-rate reads feeding the trigger.
/// A lone burst (one big fan-out reported moments before the sweep) is
/// amortized over at least this window instead of the meter's 1-second
/// clamp, so only load SUSTAINED across the window registers as pressure.
/// See `RunningAverage::windowed_rate`.
pub(crate) const COST_RATE_MIN_WINDOW: Duration = Duration::from_secs(300);

/// Node-level attributed-cost snapshot for ONE cost axis, handed to the
/// sweep by the caller (`Ring` reads the topology meter — see
/// `Ring::hosting_cost_pressure_axes`; this module never touches the meter).
#[derive(Debug, Clone)]
pub(crate) struct CostAxisPressure {
    /// Axis name for logs/telemetry (e.g. `"exec_cpu_micros_per_sec"`).
    pub axis: &'static str,
    /// Node-total attributed rate across ALL contracts (axis units per
    /// second), the denominator of the share test.
    pub total_rate: f64,
    /// Absolute floor: at or below this total the axis never triggers.
    pub floor: f64,
    /// Per-contract attributed rates (axis units per second). Contracts
    /// absent from the map have zero attributed cost. Pre-filtered by the
    /// meter read to SUSTAINED sources only (first sample at least half the
    /// cost window old), so a single large burst is never a candidate; every
    /// positive-rate contract still counts in [`Self::total_rate`].
    pub rates: std::collections::HashMap<freenet_stdlib::prelude::ContractInstanceId, f64>,
}

/// Assemble the three cost-pressure axes from `(total_rate, per_contract)`
/// meter reads, binding each to its floor constant. Shared by
/// `Ring::hosting_cost_pressure_axes` (production) and the storm-frequency
/// integration test, so the test exercises the same axis assembly the sweep
/// uses.
pub(crate) type ContractCostRead = (
    f64,
    std::collections::HashMap<freenet_stdlib::prelude::ContractInstanceId, f64>,
);

pub(crate) fn build_cost_axes(
    exec_cpu: ContractCostRead,
    fanout_bytes: ContractCostRead,
    messages: ContractCostRead,
) -> Vec<CostAxisPressure> {
    vec![
        CostAxisPressure {
            axis: "exec_cpu_micros_per_sec",
            total_rate: exec_cpu.0,
            floor: EXEC_CPU_PRESSURE_FLOOR_MICROS_PER_SEC,
            rates: exec_cpu.1,
        },
        CostAxisPressure {
            axis: "broadcast_fanout_bytes_per_sec",
            total_rate: fanout_bytes.0,
            floor: BROADCAST_FANOUT_PRESSURE_FLOOR_BYTES_PER_SEC,
            rates: fanout_bytes.1,
        },
        CostAxisPressure {
            axis: "broadcast_messages_per_sec",
            total_rate: messages.0,
            floor: BROADCAST_MESSAGES_PRESSURE_FLOOR_PER_SEC,
            rates: messages.1,
        },
    ]
}

/// The cost-eviction candidacy predicate: only a contract with ZERO demand
/// may be shed by cost pressure, where zero demand means no local client
/// subscriptions AND no downstream subscribers AND no recent demand
/// (`recently_accessed == false` — the caller passes true for a genuine
/// GET/PUT within the cost window OR a local-client access within the
/// reconciliation renewal lease; see [`HostingCache::evict_cost_pressure`]) —
/// matching invariant 3's demand definition, which includes reads. A
/// subscribed contract is
/// NEVER a candidate, and neither is an actively-read/written one (the
/// never-subscribed-but-read River UI-container class invariant 3 protects) —
/// by construction, not by ranking — so cost pressure cannot reorder the
/// subscriber-primary tiers of [`victim_order`] and cannot touch read-hot
/// contracts. A storm contract stays evictable because its only activity is
/// its own UPDATE churn, which deliberately never counts as genuine access.
/// The rate must also be strictly positive (cost pressure is not a generic
/// zero-subscriber purge). NaN-safe: a NaN rate fails `> 0.0`.
pub(crate) fn cost_eviction_candidate(
    local_subs: usize,
    downstream_subs: usize,
    attributed_rate: f64,
    recently_accessed: bool,
) -> bool {
    local_subs == 0 && downstream_subs == 0 && !recently_accessed && attributed_rate > 0.0
}

impl<T: TimeSource> HostingCache<T> {
    /// Create a new hosting cache with the given byte budget.
    pub fn new(budget_bytes: u64, time_source: T) -> Self {
        Self {
            budget_bytes,
            current_bytes: 0,
            contracts: HashMap::new(),
            access_seq: 0,
            eviction_floor: 0.0,
            budget_evictions_total: 0,
            evictions_of_recently_read_total: 0,
            evicted_unread_total: 0,
            evicted_unread_age_secs_sum: 0,
            oom_valve_evictions_total: 0,
            subscribed_evictions_total: 0,
            cost_evictions_total: 0,
            time_source,
        }
    }

    /// Next monotonic access sequence number, stamped onto an entry on every
    /// access to give an exact least-recently-accessed eviction tiebreak.
    fn next_seq(&mut self) -> u64 {
        self.access_seq = self.access_seq.saturating_add(1);
        self.access_seq
    }

    /// Evict contracts while the cache is over budget, choosing victims
    /// **subscriber-primary**: ascending `(local_subscription_count,
    /// downstream_subscriber_count, recency_seq, key_bytes)` — fewest LOCAL
    /// client subscriptions first, then fewest downstream subscribers, then
    /// least-recent real GET, then a deterministic key-byte tiebreak (#4642
    /// subscriber-primary rework; Ian's confirmed ordering).
    ///
    /// `subscriber_counts(key)` is the caller's genuine-demand split
    /// `(local_client_subscriptions, downstream_subscribers)`; their sum is `0`
    /// exactly when the contract is NOT `contract_in_use`. There is NO `min_ttl`
    /// cold-start floor (dropped 2026-07-08); every entry is eligible, with a
    /// single op-scoped exception governed by `pressure`:
    ///
    /// - [`MemoryPressure::AtCapacity`] (the production value): every entry is
    ///   eligible EXCEPT `protected` — the op-scoped backstop guard. `protected`
    ///   is the contract whose in-flight access triggered this sweep (the just-
    ///   inserted key on the `record_access` path). Skipping it replaces the old
    ///   `min_ttl` age-0 guarantee that "the entry a call just inserted cannot be
    ///   evicted by that same call", so `host_contract` still guarantees the
    ///   contract is hosted on return. It is a backstop, not a pin: the just-
    ///   accessed key also has the highest `recency_seq`, so `victim_order` already
    ///   sorts it LAST — the skip only bites on an already-broken peer (invariant
    ///   3, decision 2026-07-08). Subscriber count is NOT a filter — it is the
    ///   ORDERING. A subscribed contract is ordered LAST but is NOT hard-pinned:
    ///   when nothing with fewer subscribers is eligible and the peer is still over
    ///   budget, the fewest-`(local, downstream)` subscribed contract IS evicted
    ///   (the accepted last resort). This is the change from the shipped #4720
    ///   code, which hard-filtered `subs == 0` (a hard pin).
    /// - [`MemoryPressure::Overflow`] (genuine-RAM-overflow corner — trigger
    ///   UNWIRED in production, see [`MemoryPressure`]): ALL entries are eligible,
    ///   INCLUDING `protected` (the op-scoped backstop is pierced), so the
    ///   fewest-subscriber contract is shed even out from under an in-flight op to
    ///   survive OOM. Not a separate ranking — the SAME `victim_order` — just a
    ///   wider eligibility set (hosting-invariants: "a corner of the same
    ///   eviction, not a separate valve").
    ///
    /// Any eviction whose victim was subscribed (`local + downstream >= 1` at
    /// decision time) increments [`Self::subscribed_evictions_total`] (the field
    /// falsifier) and is flagged `was_in_use` in the returned [`EvictedContract`]
    /// so the manager tears down its subscription state. Overflow-pressure
    /// evictions additionally increment [`Self::oom_valve_evictions_total`].
    ///
    /// Eviction stops as soon as `current_bytes <= budget_bytes`. Returns the
    /// evicted contracts; the generation is captured atomically under the cache's
    /// write lock so the `EvictContract` deletion-time guard can detect a re-host
    /// that races with this eviction (see `RuntimePool::remove_contract`).
    ///
    /// do NOT revert to byte-only / recency-only LRU as the PRIMARY key — see
    /// hosting-invariants (byte-only eviction anti-pattern). Subscriber count is
    /// the demand-authorization signal: a subscribed contract is real demand and
    /// is shed only as a last resort. Real GET/PUT recency is a tiebreak among
    /// equal-subscriber contracts, not the primary key.
    fn evict_over_budget<G>(
        &mut self,
        subscriber_counts: &G,
        pressure: MemoryPressure,
        protected: Option<&ContractKey>,
    ) -> Vec<EvictedContract>
    where
        G: Fn(&ContractKey) -> (usize, usize),
    {
        if self.current_bytes <= self.budget_bytes {
            return Vec::new();
        }

        // Collect eviction-eligible entries with their ordering keys, then evict
        // lowest-priority first until back under budget. O(n log n) per
        // over-budget event; n is the hosted-contract count (hundreds to ~1k),
        // and this only runs when actually over budget.
        //
        // Eligibility (no `min_ttl` floor since 2026-07-08 — every entry is
        // eligible) with one op-scoped exception:
        // - AtCapacity: skip the `protected` key (the op-scoped backstop guard).
        //   Subscriber count is the ORDERING, not a filter — a subscribed contract
        //   is ordered last but still eligible, so it is shed as a last resort when
        //   nothing zero-subscriber is eligible.
        // - Overflow (genuine-overflow corner): every entry, INCLUDING protected —
        //   pierce the op backstop to survive genuine RAM overflow.
        //
        // `subscriber_counts(key)` reads the unlocked subscription DashMaps, so
        // it is evaluated EXACTLY ONCE per entry here and reused for the (local,
        // downstream) sort key. Evaluating it once also fixes the captured
        // `was_in_use` flag atomically with the decision, so the manager's teardown
        // targets exactly the contracts this sweep decided to shed while in use (no
        // racy post-hoc `contract_in_use` re-read).
        let mut candidates: Vec<(ContractKey, usize, usize, u64)> = self
            .contracts
            .iter()
            .filter_map(|(key, entry)| {
                let eligible = match pressure {
                    // Op-scoped backstop: never evict the contract whose in-flight
                    // access triggered this sweep. Only trips on an already-broken
                    // peer because `protected` also has the highest recency_seq.
                    //
                    // KNOWN invariant-3 divergence (tracked in #4735, deferred): in
                    // the corner where the cache is at budget and EVERY incumbent is
                    // subscribed, excluding a zero-subscriber `protected` newcomer
                    // from the candidate set forces the victim to be a subscribed
                    // incumbent — the INVERSE of invariant 3's emergent refusal,
                    // which says the fewest-subscriber contract (the newcomer) is the
                    // one that should be refused, never displacing a 2+-subscriber
                    // incumbent. The correct fix needs `host_contract` to signal
                    // "served but not retained" so a zero-subscriber op doesn't have
                    // to be protected here; until then this is left as-is (comment
                    // only, no behavior change) — see #4735.
                    MemoryPressure::AtCapacity => protected != Some(key),
                    MemoryPressure::Overflow => true,
                };
                if !eligible {
                    return None;
                }
                let (local, downstream) = subscriber_counts(key);
                Some((*key, local, downstream, entry.recency_seq))
            })
            .collect();

        // Ascending by local subscriptions (fewest first), then downstream
        // subscribers, then recency_seq (least-recently real-GET first), then
        // contract-key bytes as a final deterministic tiebreak. `victim_order`
        // is the canonical ranking (Ian's confirmed ordering, #4642).
        candidates.sort_by(|a, b| victim_order((a.1, a.2, a.3, &a.0), (b.1, b.2, b.3, &b.0)));

        // Only used by the unread-seed age falsifier below.
        let now = self.time_source.now();
        let mut evicted = Vec::new();
        for (key, local, downstream, _seq) in candidates {
            if self.current_bytes <= self.budget_bytes {
                break; // back under budget, stop evicting
            }
            if let Some(entry) = self.contracts.remove(&key) {
                let was_in_use = local + downstream > 0;
                self.current_bytes = self.current_bytes.saturating_sub(entry.size_bytes);
                self.budget_evictions_total = self.budget_evictions_total.saturating_add(1);
                // Field falsifier for the single riskiest new behavior: shedding a
                // subscribed contract. Counted by the captured subscriber split
                // (not by pressure), so it covers the normal AtCapacity last-resort
                // shed AND the Overflow valve. The `was_in_use` flag on the
                // returned tuple drives the manager's subscription teardown.
                if was_in_use {
                    self.subscribed_evictions_total =
                        self.subscribed_evictions_total.saturating_add(1);
                }
                // Count OOM-valve evictions by PRESSURE (any eviction under
                // Overflow), not by the victim's subscriber count. Gating on
                // `pressure == Overflow` is airtight: it cannot be perturbed by a
                // concurrent subscription change, so the "stays 0 in the field
                // while the Overflow trigger is unwired" guarantee holds by
                // construction. (Overflow's only remaining special power is
                // piercing the op-scoped backstop; AtCapacity already sheds
                // subscribed contracts, tracked by `subscribed_evictions_total`.)
                if matches!(pressure, MemoryPressure::Overflow) {
                    self.oom_valve_evictions_total =
                        self.oom_valve_evictions_total.saturating_add(1);
                }
                if entry.read_count >= 2 {
                    self.evictions_of_recently_read_total =
                        self.evictions_of_recently_read_total.saturating_add(1);
                }
                if entry.read_count == 0 && entry.seeded_this_run {
                    // A genuine fresh-this-run PUT seed evicted before its first
                    // reader — the PUT-durability falsifier. Record it and its
                    // age (bounded by `budget_evictions_total`, the shared
                    // denominator) so the field can tell whether seeds die young.
                    // `seeded_this_run` excludes reloaded entries, whose restart
                    // reset of read_count/inserted_at would otherwise fake an
                    // "unread seeds die instantly" spike on every restart.
                    self.evicted_unread_total = self.evicted_unread_total.saturating_add(1);
                    let age_secs = now.saturating_duration_since(entry.inserted_at).as_secs();
                    self.evicted_unread_age_secs_sum =
                        self.evicted_unread_age_secs_sum.saturating_add(age_secs);
                }
                // Greedy-Dual aging: ratchet the demoted `eviction_floor`
                // telemetry up to the victim's `keep_score` (never down). This is
                // retained for telemetry continuity only — eviction no longer
                // orders by keep_score/floor (see the module + evict_over_budget
                // docs); it is scheduled for deletion with the rest of the demand
                // machinery once subscriber-primary is field-validated (#4642).
                if entry.keep_score > self.eviction_floor {
                    self.eviction_floor = entry.keep_score;
                }
                evicted.push(EvictedContract {
                    key,
                    write_generation: entry.write_generation,
                    was_in_use,
                });
            }
        }

        // Op-scoped backstop tripwire (invariant 3, decision 2026-07-08): under
        // AtCapacity we refused to evict `protected` (the op-scoped guard). If that
        // refusal leaves us still over budget with `protected` still resident, the
        // peer is in the corner the invariant calls out — a recency-reset contract
        // we would otherwise have to evict mid-op. On a healthy peer this never
        // fires: `protected` sorts LAST by recency, so everything else evicts first
        // and clears the budget before we would reach it. It CAN fire on an already-
        // over-capacity peer (e.g. a just-inserted contract larger than the whole
        // budget); genuine RAM overflow arrives as `Overflow`, which pierces the
        // backstop. Not a `debug_assert!`: the "contract larger than budget" corner
        // is legitimate (the old `min_ttl` age-0 gate left the same state silently),
        // so we surface it as a warning rather than panicking debug builds.
        if self.current_bytes > self.budget_bytes {
            if let Some(protected_key) = protected {
                if matches!(pressure, MemoryPressure::AtCapacity)
                    && self.contracts.contains_key(protected_key)
                {
                    tracing::warn!(
                        contract = %protected_key,
                        current_bytes = self.current_bytes,
                        budget_bytes = self.budget_bytes,
                        over_by = self.current_bytes - self.budget_bytes,
                        "op-scoped backstop kept an in-flight contract while still over \
                         budget (pathological — a genuine RAM-overflow signal would pierce \
                         this via Overflow)",
                    );
                }
            }
        }

        evicted
    }

    /// Record an access to a contract, adding or refreshing it in the cache.
    ///
    /// If the contract is already cached, this refreshes its LRU position and timestamp.
    /// If not cached, this adds it and evicts old contracts if necessary.
    ///
    /// Returns a `RecordAccessResult` containing:
    /// - `is_new`: Whether this contract was newly added (vs. refreshed existing)
    /// - `evicted`: `(ContractKey, write_generation)` pairs for contracts
    ///   evicted to make room. The generation is captured atomically under
    ///   the cache's write lock so the `EvictContract` deletion-time guard
    ///   can detect a re-host that occurred after this call returned.
    ///
    /// The entry this call just inserted is passed to `evict_over_budget` as the
    /// op-scoped `protected` key, so it can never be evicted by the same call
    /// (replacing the old `min_ttl` age-0 guarantee, now that `min_ttl` is gone).
    /// It also carries the highest `recency_seq`, so it sorts LAST regardless.
    /// Subscriber count ORDERS victims, it does not pin them: a contract with
    /// `subscriber_count(key) >= 1` (an active client subscription or downstream
    /// subscriber) is evicted LAST and survives while any contract with fewer
    /// subscribers is eligible, but when the cache is still over budget and nothing
    /// fewer-subscriber is eligible, the fewest-subscriber contract IS shed as a
    /// last resort — even a subscribed one. When that happens the returned
    /// `EvictedContract::was_in_use` flag is set so the caller tears down the
    /// subscription state and reclaims the on-disk state (see
    /// `HostingManager::teardown_evicted_in_use_contract`). This is the change
    /// from the shipped #4720 code, which hard-pinned subscribed contracts. The
    /// OOM valve ([`MemoryPressure::Overflow`], trigger unwired) adds only one
    /// extra power: it ALSO pierces the op-scoped backstop; a fresh insert always
    /// uses `AtCapacity`.
    ///
    /// `write_generation` is the current state-write generation for this
    /// contract (from `HostingManager::state_generation`). It is stored on
    /// the freshly-inserted/refreshed `HostedContract` so that when this
    /// entry is later evicted, its `write_generation` snapshot travels
    /// with the `EvictContract` event and is compared against the
    /// then-current generation at deletion time.
    // Neutral-demand convenience wrapper. Production always goes through
    // `record_access_with_demand` (the `HostingManager` supplies the
    // proximity-prior estimate), so in a non-test build this wrapper is unused;
    // it is retained for callers/tests that have no demand estimate.
    #[cfg_attr(not(test), allow(dead_code))]
    pub fn record_access<G>(
        &mut self,
        key: ContractKey,
        size_bytes: u64,
        access_type: AccessType,
        write_generation: u64,
        subscriber_counts: G,
    ) -> RecordAccessResult
    where
        G: Fn(&ContractKey) -> (usize, usize),
    {
        // Neutral demand: the demoted proximity-prior term (telemetry only). The
        // eviction order is subscriber-primary + real-GET recency regardless.
        // This wrapper is used by callers that have no demand estimate (and by
        // the cache's own unit tests); `HostingManager` uses
        // `record_access_with_demand` to supply the proximity-prior estimate.
        self.record_access_with_demand(
            key,
            size_bytes,
            access_type,
            write_generation,
            NEUTRAL_DEMAND,
            subscriber_counts,
        )
    }

    /// Like [`record_access`](Self::record_access) but with an explicit
    /// `predicted_demand` for the demoted (telemetry-only) demand estimate.
    ///
    /// Semantics by access kind:
    /// - **New entry (any kind):** inserted with the caller's `predicted_demand`
    ///   (telemetry). A GET/SUBSCRIBE counts as one read (`read_count = 1`); a
    ///   PUT is a SEED (`read_count = 0`). A GET or PUT (genuine client access)
    ///   stamps `recency_seq`; a SUBSCRIBE-only seed leaves it at 0.
    /// - **Existing entry, GET (real read):** stamp `recency_seq` to the
    ///   current frontier (this is the eviction recency refresh), bump
    ///   `read_count`, clear any abandonment, refresh the telemetry
    ///   `keep_score`. Returns an `observed_read_rate` training sample.
    /// - **Existing entry, SUBSCRIBE:** counts as a read for `read_count` /
    ///   telemetry, but does NOT stamp `recency_seq` — a subscription is not a
    ///   GET/PUT, so it must not refresh eviction recency (renewal traffic would
    ///   otherwise keep every subscribed contract equally fresh).
    /// - **Existing entry, PUT (seed only):** refresh recency-of-access/size/
    ///   generation, the stored `predicted_demand`, AND `recency_seq` (a PUT is
    ///   genuine client access — invariant 3, decision 2026-07-08), but no read
    ///   count and no `keep_score` refresh (a PUT is a write, not a read).
    pub fn record_access_with_demand<G>(
        &mut self,
        key: ContractKey,
        size_bytes: u64,
        access_type: AccessType,
        write_generation: u64,
        predicted_demand: f64,
        subscriber_counts: G,
    ) -> RecordAccessResult
    where
        G: Fn(&ContractKey) -> (usize, usize),
    {
        let now = self.time_source.now();
        let seq = self.next_seq();
        let is_read = matches!(access_type, AccessType::Get | AccessType::Subscribe);
        // A real GET or PUT (genuine client access) refreshes the eviction recency
        // key (invariant 3, decision 2026-07-08). SUBSCRIBE / renewal traffic does
        // NOT, so it can't keep a contract artificially fresh.
        let resets_recency = matches!(access_type, AccessType::Get | AccessType::Put);

        if let Some(existing) = self.contracts.get_mut(&key) {
            // Already cached - update size if changed
            if existing.size_bytes != size_bytes {
                // Adjust byte accounting: add new size, subtract old size
                self.current_bytes = self
                    .current_bytes
                    .saturating_add(size_bytes)
                    .saturating_sub(existing.size_bytes);
                existing.size_bytes = size_bytes;
            }
            existing.last_accessed = now;
            existing.last_access_seq = seq;
            existing.access_type = access_type;
            // Refresh the generation snapshot: a re-access is a fresh
            // "I'm hosting this state" assertion, so its captured generation
            // should track the current state-write generation.
            existing.write_generation = write_generation;
            // Keep the stored demand estimate current so a later `touch`
            // (which has no estimator access) refreshes against the latest prior.
            existing.predicted_demand = predicted_demand;

            // A real GET or PUT refreshes the eviction recency key (a PUT is
            // genuine client access; SUBSCRIBE/renewal does not). The
            // wall-clock twin (`last_genuine_access`) feeds the cost-eviction
            // recency guard (#4861).
            if resets_recency {
                existing.recency_seq = seq;
                existing.last_genuine_access = Some(now);
            }

            let observed_read_rate = if is_read {
                // Read (GET/SUBSCRIBE): count it, clear any abandoned marker,
                // and refresh the demoted telemetry keep_score.
                existing.read_count = existing.read_count.saturating_add(1);
                existing.abandoned_at = None;
                existing.keep_score = self.eviction_floor + predicted_demand;
                Self::rate_sample(existing.read_count, now, existing.inserted_at)
            } else {
                // Seed (PUT) on an existing entry: recency already refreshed above
                // (genuine client access), but no read count and no telemetry
                // keep_score refresh (a PUT is a write, not a read).
                None
            };

            RecordAccessResult {
                is_new: false,
                evicted: Vec::new(),
                evicted_in_use: Vec::new(),
                // Filled by `HostingManager::record_contract_access` after the
                // teardown loop; empty here (refresh path evicts nothing).
                evicted_in_use_teardown: Vec::new(),
                observed_read_rate,
            }
        } else {
            // Not cached - insert the new entry first, then evict over-budget
            // entries. The new entry is passed to `evict_over_budget` as the
            // op-scoped `protected` key, so it is never evicted by this call
            // (under AtCapacity, which a fresh insert always uses) — the backstop
            // that replaces the old `min_ttl` age-0 guarantee.
            let contract = HostedContract {
                size_bytes,
                last_accessed: now,
                last_access_seq: seq,
                // A real GET or PUT (genuine client access) seeds the eviction
                // recency key at the current frontier; a SUBSCRIBE-only seed starts
                // at 0 (never client-accessed), so it sorts to evict first among
                // zero-subscriber entries.
                recency_seq: if resets_recency { seq } else { 0 },
                last_genuine_access: resets_recency.then_some(now),
                keep_score: self.eviction_floor + predicted_demand,
                predicted_demand,
                read_count: if is_read { 1 } else { 0 },
                inserted_at: now,
                // Fresh insert this run: a genuine live PUT/GET seed, so it is
                // eligible for the unread-seed falsifier (gated on read_count==0).
                seeded_this_run: true,
                access_type,
                local_client_access: false,
                local_client_last_access: None,
                write_generation,
                abandoned_at: None,
            };
            self.contracts.insert(key, contract);
            self.current_bytes = self.current_bytes.saturating_add(size_bytes);

            // A fresh insert always evicts under AtCapacity: an insert never
            // constitutes genuine RAM overflow, so it must not pierce the op-scoped
            // backstop. The just-inserted `key` is the `protected` key, so this
            // call can never evict the contract it just added.
            //
            // KNOWN invariant-3 divergence (tracked in #4735, deferred): passing the
            // zero-subscriber newcomer as `protected` means that, when the cache is
            // at budget and every incumbent is subscribed, admitting this newcomer
            // sheds a subscribed incumbent — the inverse of invariant 3's emergent
            // refusal (the fewest-subscriber contract, i.e. the newcomer, should be
            // the one refused). The proper fix needs `host_contract` to distinguish
            // "served but not retained"; comment-only for now — see the matching note
            // on the `protected` skip in `evict_over_budget`.
            let evicted =
                self.evict_over_budget(&subscriber_counts, MemoryPressure::AtCapacity, Some(&key));

            // Split into the (key, generation) reclaim list (unchanged external
            // contract) and the in-use subset the manager must tear down. Under
            // the subscriber-primary ordering a fresh insert CAN now shed a
            // subscribed contract as a last resort, so `evicted_in_use` is no
            // longer always empty here.
            let evicted_in_use: Vec<ContractKey> = evicted
                .iter()
                .filter(|e| e.was_in_use)
                .map(|e| e.key)
                .collect();
            let evicted = evicted
                .into_iter()
                .map(|e| (e.key, e.write_generation))
                .collect();

            RecordAccessResult {
                is_new: true,
                evicted,
                evicted_in_use,
                // Filled by `HostingManager::record_contract_access` from the
                // teardown loop (this layer knows WHICH keys are in-use but not
                // the peer identities / client counts the manager tears down).
                evicted_in_use_teardown: Vec::new(),
                observed_read_rate: None,
            }
        }
    }

    /// Derive an observed read-rate (reads/second) sample from a contract's
    /// read count and residency. `None` when residency is non-positive (no
    /// elapsed time to divide by). Residency is floored at one second so a burst
    /// of quick reads can't produce an unbounded spike that would dominate the
    /// proximity-prior fit.
    fn rate_sample(read_count: u32, now: Instant, inserted_at: Instant) -> Option<f64> {
        let residency = now.saturating_duration_since(inserted_at);
        if residency.is_zero() {
            return None;
        }
        let residency_secs = residency.as_secs_f64().max(1.0);
        Some(read_count as f64 / residency_secs)
    }

    /// Mark a contract as accessed by a local client (HTTP/WebSocket).
    /// No-op if the contract is not in the cache.
    pub fn mark_local_client_access(&mut self, key: &ContractKey) {
        if let Some(existing) = self.contracts.get_mut(key) {
            existing.local_client_access = true;
            existing.local_client_last_access = Some(self.time_source.now());
        }
    }

    /// Check if a contract was accessed by a local client.
    pub fn has_local_client_access(&self, key: &ContractKey) -> bool {
        self.contracts
            .get(key)
            .map(|c| c.local_client_access)
            .unwrap_or(false)
    }

    /// Check if a contract was accessed by a local client within the given duration.
    /// Returns false if never locally accessed or if the access is too old.
    pub fn has_recent_local_client_access(
        &self,
        key: &ContractKey,
        max_age: std::time::Duration,
    ) -> bool {
        let now = self.time_source.now();
        self.contracts
            .get(key)
            .and_then(|c| c.local_client_last_access)
            .map(|t| now.saturating_duration_since(t) < max_age)
            .unwrap_or(false)
    }

    /// Touch/refresh a contract's demand without adding it if missing, using an
    /// explicit freshly-derived `predicted_demand` (the proximity-prior estimate
    /// for this contract's ring distance).
    ///
    /// Called when a user GET serves a hosted contract from local cache — the
    /// dominant read path for hot contracts. This is read-demand: it refreshes
    /// the TTL clock, updates the stored demand estimate to `predicted_demand`,
    /// floats the contract's `keep_score` back to the frontier
    /// (`eviction_floor + predicted_demand`), counts the read, and clears any
    /// abandonment — so actively requested contracts stay in the cache.
    ///
    /// Refreshing the stored estimate (rather than reusing the old one) is what
    /// lets a **restart-loaded entry** — inserted at `NEUTRAL_DEMAND` by
    /// `load_persisted_entry` because this peer's location was unknown at load —
    /// pick up the distance prior on its first local-serve read, exactly as
    /// `record_access_with_demand` already does for the network-refetch path.
    /// Without this, a persisted entry served only from local cache would sit at
    /// neutral demand indefinitely and the cold-start distance prior would never
    /// reach it (the restart-drift this fix closes).
    ///
    /// Returns the observed read-rate training sample (`read_count / residency`)
    /// the same way `record_access_with_demand` does, so the caller
    /// (`HostingManager::touch_hosting`) can feed the proximity prior from the
    /// local-serve path too — otherwise the prior would train only on network
    /// refetches and stay blind to the reads A3 is meant to model. `None` when
    /// the contract is absent or residency is zero.
    pub fn touch_with_demand(&mut self, key: &ContractKey, predicted_demand: f64) -> Option<f64> {
        let floor = self.eviction_floor;
        let seq = self.next_seq();
        let now = self.time_source.now();
        if let Some(existing) = self.contracts.get_mut(key) {
            existing.last_accessed = now;
            existing.last_access_seq = seq;
            // A touch is a local-cache GET serve — a real GET read, so it
            // refreshes the eviction recency key (unlike SUBSCRIBE/renewal)
            // and the wall-clock cost-eviction recency guard (#4861).
            existing.recency_seq = seq;
            existing.last_genuine_access = Some(now);
            existing.read_count = existing.read_count.saturating_add(1);
            // A fresh touch is evidence of demand — clear any abandoned marker,
            // refresh the stored demand estimate to the caller's freshly-derived
            // prior, and float the demoted telemetry keep_score to the frontier.
            existing.abandoned_at = None;
            existing.predicted_demand = predicted_demand;
            existing.keep_score = floor + predicted_demand;
            Self::rate_sample(existing.read_count, now, existing.inserted_at)
        } else {
            None
        }
    }

    /// Neutral-path convenience wrapper over
    /// [`touch_with_demand`](Self::touch_with_demand) that reuses the entry's
    /// STORED demand estimate (no fresh prior). Production goes through
    /// `touch_with_demand` (the `HostingManager` supplies the proximity-prior
    /// estimate); this wrapper serves the cache's own unit tests and any caller
    /// that has no estimator handy.
    #[cfg_attr(not(test), allow(dead_code))]
    pub fn touch(&mut self, key: &ContractKey) -> Option<f64> {
        let stored = self.contracts.get(key)?.predicted_demand;
        self.touch_with_demand(key, stored)
    }

    /// Mark `key` as recently abandoned and **reset its recency clock** as the
    /// contract enters the zero-subscriber tier.
    ///
    /// Called by `HostingManager` when a contract transitions from "in
    /// use" (has subscribers / clients / downstream peers) to "no longer
    /// in use" — the moment its `contract_in_use` predicate flips from
    /// `true` to `false`, i.e. subscription TERMINATION.
    ///
    /// # Recency reset at termination (invariant 3, decision 2026-07-08)
    ///
    /// While a contract is subscribed its eviction rank is dominated by its
    /// `(local, downstream)` subscriber counts, and its `recency_seq` (a GET/PUT
    /// tiebreak) is NOT refreshed by subscription-renewal traffic. So the moment
    /// the last subscription terminates and the contract drops into the zero-
    /// subscriber tier, its `recency_seq` could be arbitrarily stale (an old read
    /// from before it was subscribed), which would make it the instant eviction
    /// victim. To avoid that, termination resets `recency_seq` to the current
    /// frontier — the recency clock "starts" at termination, so a formerly-
    /// subscribed contract competes on equal footing with freshly-accessed
    /// zero-subscriber copies rather than being punished for an old last-read it
    /// accrued while parked in the subscription tier.
    ///
    /// The demoted telemetry `keep_score` is also dropped to the current
    /// `eviction_floor`; that field no longer drives eviction (see the module
    /// docs) and is retained only for the dashboard.
    ///
    /// No-op when `key` is absent (already evicted). Idempotent: only the FIRST
    /// abandonment (the actual termination) resets recency / marks the timestamp;
    /// a repeat call on an already-abandoned entry leaves it intact. A later
    /// re-subscription clears `abandoned_at` (via `record_access` / `touch`), so a
    /// second termination resets recency again — the correct behavior.
    ///
    /// Network forgetfulness is explicitly preserved: this method does **not**
    /// evict the contract. It only re-times the eviction *priority* used when the
    /// cache is genuinely over budget. A contract that's abandoned but under
    /// budget pressure stays cached.
    pub fn record_abandonment(&mut self, key: &ContractKey) {
        let floor = self.eviction_floor;
        // Only the first abandonment (the termination transition) acts. Decide
        // before taking the &mut borrow so we can allocate a fresh recency seq.
        let is_termination = self
            .contracts
            .get(key)
            .is_some_and(|e| e.abandoned_at.is_none());
        if !is_termination {
            return;
        }
        let seq = self.next_seq();
        let now = self.time_source.now();
        if let Some(existing) = self.contracts.get_mut(key) {
            existing.abandoned_at = Some(now);
            // Reset the recency clock at termination (see the doc above) so the
            // formerly-subscribed contract is not instantly evicted for an old
            // last-read accrued while it sat in the subscription tier. The
            // wall-clock twin gives the same grace against COST eviction
            // (#4861): a just-unsubscribed contract gets one cost window
            // before its update-work cost can make it a candidate.
            existing.recency_seq = seq;
            existing.last_genuine_access = Some(now);
            // Strip demand credit (demoted telemetry only): drop keep_score to
            // the frontier.
            existing.keep_score = floor;
        }
    }

    /// Update the cached `write_generation` snapshot for `key` to `new_gen`.
    ///
    /// Called paired with `HostingManager::bump_state_generation` from
    /// every state-write chokepoint (executor PUT/UPDATE and V2 delegate
    /// PUT/UPDATE). Without this refresh, an UPDATE (or re-PUT) to an
    /// already-hosted contract would leave the cached snapshot stuck at
    /// its `record_access`-time value while the `state_generation` counter
    /// kept advancing; a later eviction would carry the stale snapshot,
    /// the deletion-time generation guard in
    /// `RuntimePool::remove_contract` would see a mismatch, and disk
    /// reclamation would be permanently skipped — leaking the on-disk
    /// state and code blob.
    ///
    /// No-op when `key` is not in the cache: if the contract has been
    /// evicted between the bump and this refresh, the `EvictContract`
    /// already carried the pre-bump snapshot and the deletion-time guard
    /// will skip on the mismatch. That residual leak is narrower than
    /// the "permanent leak on every UPDATE" failure mode this method
    /// closes — see the call-site comment in `runtime.rs`.
    pub fn refresh_entry_generation(&mut self, key: &ContractKey, new_gen: u64) {
        if let Some(existing) = self.contracts.get_mut(key) {
            existing.write_generation = new_gen;
        }
    }

    /// Check if a contract is in the cache.
    pub fn contains(&self, key: &ContractKey) -> bool {
        self.contracts.contains_key(key)
    }

    /// Get metadata about a hosted contract.
    #[allow(dead_code)] // Public API for introspection
    pub fn get(&self, key: &ContractKey) -> Option<&HostedContract> {
        self.contracts.get(key)
    }

    /// Get the current number of hosted contracts.
    pub fn len(&self) -> usize {
        self.contracts.len()
    }

    /// Check if the cache is empty.
    #[allow(dead_code)] // Public API for introspection
    pub fn is_empty(&self) -> bool {
        self.contracts.is_empty()
    }

    /// Get the current bytes used.
    #[allow(dead_code)] // Public API for introspection
    pub fn current_bytes(&self) -> u64 {
        self.current_bytes
    }

    /// Get the budget in bytes.
    #[allow(dead_code)] // Public API for introspection
    pub fn budget_bytes(&self) -> u64 {
        self.budget_bytes
    }

    /// Test-only: shrink/grow the byte budget after construction WITHOUT running
    /// eviction. Lets a test seed several entries at a generous budget (so they
    /// get distinct `recency_seq` without the insert path auto-evicting), then
    /// tighten the budget and observe a subsequent sweep. Needed since dropping
    /// `min_ttl` (2026-07-08) means the insert path now sheds down to budget
    /// immediately, so "sit over budget with everything present" is only
    /// reachable by growing the resident set under a large budget then shrinking.
    #[cfg(test)]
    pub fn set_budget_for_test(&mut self, budget_bytes: u64) {
        self.budget_bytes = budget_bytes;
    }

    /// Overwrite the byte budget (#4683 eviction floor). The ONLY writer after
    /// the constructor: the 60s recompute sets this to
    /// `min(ram_budget, disk_budget)` so the existing Greedy-Dual over-budget
    /// walk sheds the least-valuable contracts down to the tighter of the two
    /// resource floors. O(1) and does NOT itself evict — the next
    /// `evict_over_budget` (on sweep or cache insert) enforces the new value.
    pub(crate) fn set_budget_bytes(&mut self, budget_bytes: u64) {
        self.budget_bytes = budget_bytes;
    }

    /// Snapshot the cache's aggregate resource gauges under a single read for
    /// per-node telemetry. See [`HostingCacheStats`].
    pub fn stats(&self) -> HostingCacheStats {
        HostingCacheStats {
            budget_bytes: self.budget_bytes,
            current_bytes: self.current_bytes,
            contract_count: self.contracts.len() as u64,
            budget_evictions_total: self.budget_evictions_total,
            subscribed_evictions_total: self.subscribed_evictions_total,
            cost_evictions_total: self.cost_evictions_total,
            evictions_of_recently_read_total: self.evictions_of_recently_read_total,
            evicted_unread_total: self.evicted_unread_total,
            evicted_unread_age_secs_sum: self.evicted_unread_age_secs_sum,
            oom_valve_evictions_total: self.oom_valve_evictions_total,
        }
    }

    /// Per-contract rows for the local dashboard, in the cache-side EVICTION
    /// order: ascending `(recency_seq, key)` — least-recent real GET/PUT first, the
    /// same order [`Self::keys_eviction_order`] uses. This reflects the order
    /// among the ZERO-subscriber candidate set the over-budget sweep would evict
    /// under `AtCapacity` (the production pressure); the subscriber-count pin is
    /// applied by the manager via `is_eviction_eligible`, so the front of the
    /// vec is the next zero-subscriber victim under budget pressure. The rows
    /// still carry the demoted telemetry score fields (`keep_score`,
    /// `predicted_demand`) the dashboard renders.
    pub(crate) fn eviction_ordered_scores(&self) -> Vec<HostingContractScore> {
        let mut rows: Vec<HostingContractScore> = self
            .contracts
            .iter()
            .map(|(k, v)| HostingContractScore {
                key: *k,
                keep_score: v.keep_score,
                predicted_demand: v.predicted_demand,
                size_bytes: v.size_bytes,
                read_count: v.read_count,
                recency_seq: v.recency_seq,
            })
            .collect();
        rows.sort_by(|a, b| {
            a.recency_seq
                .cmp(&b.recency_seq)
                .then_with(|| a.key.as_bytes().cmp(b.key.as_bytes()))
        });
        rows
    }

    /// Get all hosted contract keys in the cache-side EVICTION order — ascending
    /// `(recency_seq, key)`, i.e. the least-recently real-GET-read contract
    /// first. This is the order among the zero-subscriber candidate set under
    /// `AtCapacity`; the subscriber-count primary key and pin are applied by the
    /// caller (`HostingManager`), which the cache cannot see here.
    #[cfg(test)]
    pub fn keys_eviction_order(&self) -> Vec<ContractKey> {
        let mut entries: Vec<_> = self
            .contracts
            .iter()
            .map(|(k, v)| (*k, v.recency_seq))
            .collect();
        entries.sort_by(|a, b| {
            a.1.cmp(&b.1)
                .then_with(|| a.0.as_bytes().cmp(b.0.as_bytes()))
        });
        entries.into_iter().map(|(k, _)| k).collect()
    }

    /// The current Greedy-Dual aging value (the eviction floor). Test-only
    /// introspection for the eviction-floor ratchet.
    #[cfg(test)]
    pub fn eviction_floor(&self) -> f64 {
        self.eviction_floor
    }

    /// Iterate over all hosted contract keys.
    pub fn iter(&self) -> impl Iterator<Item = ContractKey> + '_ {
        self.contracts.keys().cloned()
    }

    /// Sweep for evictable contracts when the cache is over budget, at a given
    /// [`MemoryPressure`].
    ///
    /// Only evicts when `current_bytes > budget_bytes`. Victims are chosen
    /// subscriber-primary — ascending
    /// `(local_subscription_count, downstream_subscriber_count, recency_seq, key)` —
    /// with eligibility governed by `pressure`:
    /// - [`MemoryPressure::AtCapacity`] (production): every entry is eligible (no
    ///   `min_ttl` floor). Subscriber count is the ORDERING, not a filter — a
    ///   subscribed contract is ordered last but shed as a last resort when nothing
    ///   zero-subscriber is eligible.
    /// - [`MemoryPressure::Overflow`]: the genuine-overflow corner — ALSO pierces
    ///   the op-scoped backstop. The trigger is UNWIRED in production (see
    ///   [`MemoryPressure`]).
    ///
    /// The periodic sweep has no single triggering access, so it passes NO
    /// op-scoped `protected` key: in-flight-op contracts are protected only by
    /// their fresh `recency_seq` (they sort last), not by a per-sweep skip.
    ///
    /// `subscriber_counts(key)` is the caller's genuine-demand split
    /// `(local_client_subscriptions, downstream_subscribers)`; their sum is `0`
    /// iff not `contract_in_use`.
    ///
    /// Returns [`EvictedContract`]s: each carries the write-generation snapshot
    /// (carried through `EvictContract` for the deletion-time re-host guard) and
    /// the `was_in_use` flag the manager keys its subscription teardown on.
    pub fn sweep_expired<G>(
        &mut self,
        subscriber_counts: G,
        pressure: MemoryPressure,
    ) -> Vec<EvictedContract>
    where
        G: Fn(&ContractKey) -> (usize, usize),
    {
        // Over-budget eviction logic is shared with `record_access` via
        // `evict_over_budget`: it returns early when under budget, then evicts
        // subscriber-primary (fewest local, then fewest downstream, then
        // least-recent real GET/PUT). No `protected` key on the periodic sweep.
        self.evict_over_budget(&subscriber_counts, pressure, None)
    }

    /// Cost-pressure eviction (cost-aware eviction, #4861): shed zero-demand
    /// contracts whose attributed update-work cost dominates the node's total
    /// on a cost axis — even while UNDER the byte budget (this is what forces
    /// the sweep to act when byte pressure never materializes; see the module
    /// section "Cost-pressure eviction").
    ///
    /// Per axis, when `total_rate > floor`, the candidates are the hosted
    /// contracts satisfying [`cost_eviction_candidate`] (zero local
    /// subscriptions AND zero downstream subscribers AND no recent demand —
    /// neither a genuine GET/PUT within the cost window nor a local-client
    /// access within the reconciliation renewal lease — AND strictly positive
    /// attributed rate) whose rate exceeds [`COST_SHARE_THRESHOLD`] ×
    /// `total_rate`.
    /// Candidates are shed in DESCENDING attributed-rate order (contract-key
    /// bytes as a deterministic tiebreak); the deficit is covered exactly when
    /// no remaining zero-demand contract holds more than the threshold share
    /// of the axis total, i.e. every super-threshold offender is shed. Shares
    /// are computed against the PRE-eviction total, so the decision is a
    /// single deterministic pass, not a feedback loop. A contract shed by an
    /// earlier axis is naturally absent from later axes' candidate sets.
    ///
    /// The subscriber-primary tiers are untouched by construction: a contract
    /// with ANY subscriber fails candidacy outright, so `was_in_use` is
    /// `false` for every returned victim and the caller's in-use teardown is
    /// a no-op. Recency is READ here (the `last_genuine_access` /
    /// `local_client_last_access` candidacy guards) but never REFRESHED (an
    /// UPDATE-storm contract deliberately accrues no recency — cache.rs
    /// recency-stamping semantics unchanged); the in-flight-op races this
    /// opens are the same ones the periodic byte sweep already has, closed
    /// downstream by the `write_generation` re-host guard and the
    /// `contract_in_use` re-check in `RuntimePool::remove_contract`.
    ///
    /// `subscriber_counts(key)` is the caller's genuine-demand split, read
    /// once per candidate and captured atomically with the decision (same
    /// discipline as [`Self::evict_over_budget`]).
    pub fn evict_cost_pressure<G>(
        &mut self,
        subscriber_counts: &G,
        axes: &[CostAxisPressure],
    ) -> Vec<EvictedContract>
    where
        G: Fn(&ContractKey) -> (usize, usize),
    {
        let now = self.time_source.now();
        let mut evicted = Vec::new();
        for axis in axes {
            // NaN-safe: a NaN/zero/sub-floor total never triggers the axis.
            if axis.total_rate.is_nan() || axis.total_rate <= axis.floor {
                continue;
            }
            let share_cutoff = COST_SHARE_THRESHOLD * axis.total_rate;
            let mut offenders: Vec<(ContractKey, f64)> = self
                .contracts
                .iter()
                .filter_map(|(key, entry)| {
                    let rate = axis.rates.get(key.id()).copied().unwrap_or(0.0);
                    // Cheap, highly-selective gate FIRST (#4903 review perf):
                    // only a contract whose attributed rate exceeds the share
                    // cutoff can ever be a victim, so short-circuit BEFORE the
                    // two DashMap `subscriber_counts` gets and the recency
                    // check. This filter runs under the hosting write lock on
                    // every sweep where any axis is over its floor (a busy
                    // gateway: always), across every hosted contract, so paying
                    // the subscriber lookup only for the handful over the cutoff
                    // matters. `rate <= cutoff` rejects rate == 0 and negatives;
                    // a NaN rate (defensive: meter math) is not <= cutoff so it
                    // falls through here, but `cost_eviction_candidate` then
                    // rejects it (its `attributed_rate > 0.0` is false for NaN).
                    if rate <= share_cutoff {
                        return None;
                    }
                    let (local, downstream) = subscriber_counts(key);
                    // Recent demand by the SAME definition the rest of the
                    // system honors (review Fix 1 / invariant 3), so cost
                    // eviction can never shed a contract reconciliation
                    // still leases (an evict → re-lease churn loop):
                    //
                    // 1. A genuine GET/PUT within the cost window
                    //    (`last_genuine_access`) — read/write demand.
                    //    UPDATE churn never stamps it, so a storm contract
                    //    cannot protect itself.
                    // 2. A LOCAL-CLIENT access within the renewal lease
                    //    (`local_client_last_access` <
                    //    `SUBSCRIPTION_LEASE_DURATION`) — the exact signal
                    //    `contracts_needing_renewal` branch 3 renews on.
                    //    This covers both the 5–8-minute tail after a local
                    //    access (the lease outlives the cost window) and the
                    //    restart window: a reload stamps
                    //    `local_client_last_access = now` for locally-
                    //    accessed contracts while `last_genuine_access`
                    //    restarts at `None`, so without this clause the cost
                    //    sweep could evict a contract the reconcile loop was
                    //    still renewing.
                    let recently_accessed = entry
                        .last_genuine_access
                        .is_some_and(|at| now.saturating_duration_since(at) < COST_RATE_MIN_WINDOW)
                        || entry.local_client_last_access.is_some_and(|at| {
                            now.saturating_duration_since(at) < super::SUBSCRIPTION_LEASE_DURATION
                        });
                    cost_eviction_candidate(local, downstream, rate, recently_accessed)
                        .then_some((*key, rate))
                })
                .collect();
            // Descending by attributed rate; key bytes break exact-rate ties
            // deterministically.
            offenders.sort_by(|a, b| {
                b.1.partial_cmp(&a.1)
                    .unwrap_or(std::cmp::Ordering::Equal)
                    .then_with(|| a.0.as_bytes().cmp(b.0.as_bytes()))
            });
            for (key, rate) in offenders {
                if let Some(entry) = self.contracts.remove(&key) {
                    self.current_bytes = self.current_bytes.saturating_sub(entry.size_bytes);
                    self.cost_evictions_total = self.cost_evictions_total.saturating_add(1);
                    // Operator-facing: this is the storm diagnosis surfacing in
                    // the field. read_count is logged (not gated on) so the
                    // field can tell whether cost eviction is hitting contracts
                    // with genuine read demand.
                    tracing::warn!(
                        contract = %key,
                        axis = axis.axis,
                        attributed_rate = rate,
                        node_total_rate = axis.total_rate,
                        share = rate / axis.total_rate,
                        read_count = entry.read_count,
                        state_bytes = entry.size_bytes,
                        "cost-pressure eviction: shedding zero-subscriber contract \
                         dominating this node's attributed update-work (#4861)"
                    );
                    evicted.push(EvictedContract {
                        key,
                        write_generation: entry.write_generation,
                        // Candidacy requires (local, downstream) == (0, 0) at
                        // decision time, so the victim was NOT in use.
                        was_in_use: false,
                    });
                }
            }
        }
        evicted
    }

    /// Load a contract entry from persisted data during startup, using an
    /// explicit cold `predicted_demand`.
    ///
    /// Unlike `record_access`, this uses a pre-computed last_accessed time
    /// and doesn't evict other contracts (we may be over budget after loading).
    ///
    /// A restart-loaded entry has no per-contract read evidence for this run, so
    /// the **distance prior is its correct cold estimate** — the same estimate a
    /// fresh contract at that ring distance would receive. The caller
    /// (`HostingManager`) passes `predict(distance)` when this peer's own location
    /// is already known at load, and `NEUTRAL_DEMAND` otherwise. At a cold
    /// restart the location is typically not yet known, so this resolves to
    /// neutral and the prior is applied lazily on the entry's first read
    /// (`touch_with_demand` / `record_access_with_demand`); once the location is
    /// known it seeds the prior at load directly.
    ///
    /// # Arguments
    /// * `key` - The contract key
    /// * `size_bytes` - Size of the contract state
    /// * `access_type` - How the contract was last accessed (GET/PUT/SUBSCRIBE)
    /// * `last_access_age` - How long ago the contract was last accessed
    /// * `local_client_access` - Whether a local client accessed it pre-restart
    /// * `predicted_demand` - Cold demand estimate (the distance prior, or
    ///   `NEUTRAL_DEMAND` when this peer's location is unknown at load)
    pub fn load_persisted_entry_with_demand(
        &mut self,
        key: ContractKey,
        size_bytes: u64,
        access_type: AccessType,
        last_access_age: Duration,
        local_client_access: bool,
        predicted_demand: f64,
    ) {
        // Skip if already loaded (shouldn't happen, but defensive)
        if self.contracts.contains_key(&key) {
            return;
        }

        // Calculate the last_accessed time from age
        let now = self.time_source.now();
        let last_accessed = now.checked_sub(last_access_age).unwrap_or(now);

        // Locally-accessed contracts loaded from disk get one renewal window
        // (set to "now") so they can re-establish subscriptions after restart.
        // If the user doesn't access them again, the age gate expires naturally.
        let local_client_last_access = if local_client_access { Some(now) } else { None };

        let contract = HostedContract {
            size_bytes,
            last_accessed,
            // Assigned a proper order by `finalize_loading` once all entries are
            // loaded (sorted by persisted recency). Temporary 0 until then.
            last_access_seq: 0,
            // Reloaded entries stay at 0 ("never GET-read since restart") and are
            // NOT re-stamped by `finalize_loading`: persistence records only a
            // generic last-ACCESS time that can't distinguish a real GET from a
            // PUT/SUBSCRIBE, so stamping real-GET recency from it would fabricate
            // demand (a never-read PUT seed out-surviving a genuinely GET-read
            // entry). A live GET after restart re-stamps it to a true value. See
            // `finalize_loading`.
            recency_seq: 0,
            // Same reasoning as `recency_seq`: persistence can't distinguish a
            // genuine GET/PUT from a SUBSCRIBE, so a reload starts with no
            // genuine-access claim; a live GET/PUT re-stamps it (#4861).
            last_genuine_access: None,
            // Loaded entries start at the caller-supplied cold demand (the
            // distance prior when this peer's location is known at load, else
            // neutral). The first live read re-scores against the current prior.
            // `last_accessed` (from the persisted age) is the tiebreak, so among
            // equal scores the oldest evicts first — the same recency ordering
            // the old LRU load path produced.
            keep_score: self.eviction_floor + predicted_demand,
            predicted_demand,
            // Reads observed this run start at zero; the persisted access_type
            // reflects the last pre-restart access, not this run's read count.
            read_count: 0,
            inserted_at: now,
            // Reloaded from persistence, NOT seeded this run: excluded from the
            // unread-seed falsifier so a restart's reset read_count/inserted_at
            // cannot masquerade as fresh unread seeds dying young.
            seeded_this_run: false,
            access_type,
            local_client_access,
            local_client_last_access,
            // Generation 0 = "no observed writes since restart". The
            // executor will bump on the next state write, so a re-PUT
            // after restart will cleanly close the race window against
            // any concurrent eviction.
            write_generation: 0,
            // Persisted entries start un-abandoned; an abandonment
            // transition would be re-observed by the running node if
            // subscribers fail to re-attach after restart.
            abandoned_at: None,
        };

        self.contracts.insert(key, contract);
        self.current_bytes = self.current_bytes.saturating_add(size_bytes);
    }

    /// Neutral-demand convenience wrapper over
    /// [`load_persisted_entry_with_demand`](Self::load_persisted_entry_with_demand).
    /// Production goes through the `_with_demand` form (the `HostingManager`
    /// supplies the distance prior when its own location is known at load); this
    /// wrapper serves callers/tests that have no demand estimate.
    #[cfg_attr(not(test), allow(dead_code))]
    pub fn load_persisted_entry(
        &mut self,
        key: ContractKey,
        size_bytes: u64,
        access_type: AccessType,
        last_access_age: Duration,
        local_client_access: bool,
    ) {
        self.load_persisted_entry_with_demand(
            key,
            size_bytes,
            access_type,
            last_access_age,
            local_client_access,
            NEUTRAL_DEMAND,
        )
    }

    /// Finalize bulk loading: assign each loaded entry a general access sequence
    /// (`last_access_seq`) in persisted-recency order, advancing the running
    /// access counter past the assigned range so subsequent live accesses keep
    /// sorting after loaded entries.
    ///
    /// It deliberately does NOT stamp `recency_seq` (the eviction recency key):
    /// persistence records only a generic LAST-ACCESS time, which does not
    /// distinguish a real GET from a PUT/SUBSCRIBE, so stamping `recency_seq`
    /// from it would FABRICATE real-GET recency — a never-GET-read PUT seed
    /// persisted just before restart would then out-survive an entry that had
    /// genuine GET demand. Instead every reloaded entry keeps `recency_seq == 0`
    /// (treated as "never GET-read since restart"); a live GET after restart
    /// re-stamps it to a real value. Reloaded entries therefore tie at the front
    /// of the eviction order (0), broken by key bytes, until a real GET lifts
    /// them out — the conservative, non-fabricating choice.
    ///
    /// Call this after `load_persisted_entry` calls are complete.
    pub fn finalize_loading(&mut self) {
        let mut entries: Vec<_> = self
            .contracts
            .iter()
            .map(|(k, v)| (*k, v.last_accessed))
            .collect();
        entries.sort_by_key(|(_, last_accessed)| *last_accessed);

        for (key, _) in entries {
            let seq = self.next_seq();
            if let Some(entry) = self.contracts.get_mut(&key) {
                entry.last_access_seq = seq;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::util::time_source::SharedMockTimeSource;
    use freenet_stdlib::prelude::{CodeHash, ContractInstanceId};

    fn make_key(seed: u8) -> ContractKey {
        ContractKey::from_id_and_code(
            ContractInstanceId::new([seed; 32]),
            CodeHash::new([seed.wrapping_add(1); 32]),
        )
    }

    fn make_cache(budget: u64) -> (HostingCache<SharedMockTimeSource>, SharedMockTimeSource) {
        let time_source = SharedMockTimeSource::new();
        let cache = HostingCache::new(budget, time_source.clone());
        (cache, time_source)
    }

    /// Project the `(key, write_generation)` reclaim pairs out of a
    /// `sweep_expired` result so the ordering/eviction assertions can compare
    /// against a plain `Vec<(ContractKey, u64)>` (the `was_in_use` flag is
    /// asserted separately where it matters).
    fn evicted_pairs(evicted: &[EvictedContract]) -> Vec<(ContractKey, u64)> {
        evicted
            .iter()
            .map(|e| (e.key, e.write_generation))
            .collect()
    }

    #[test]
    fn victim_order_is_local_then_downstream_then_get_recency_then_key() {
        use std::cmp::Ordering;
        let k1 = make_key(1);
        let k2 = make_key(2);

        // 1. Fewer LOCAL client subscriptions ranks FIRST (evicted first), even
        //    against MORE downstream subscribers, a more-recent GET, and a smaller
        //    key on the other side. A locally-subscribed contract is evicted LAST.
        assert_eq!(
            victim_order((0, 99, 999, &k2), (1, 0, 0, &k1)),
            Ordering::Less,
            "fewer local subscriptions must rank ahead of everything else"
        );

        // 2. Equal local count: fewer DOWNSTREAM subscribers ranks first, even
        //    against a more-recent GET / smaller key on the other side.
        assert_eq!(
            victim_order((1, 0, 999, &k2), (1, 3, 0, &k1)),
            Ordering::Less,
            "with equal local count, fewer downstream subscribers ranks first"
        );

        // 3. Equal local AND downstream: least-recent real GET (smaller
        //    recency_seq) ranks first.
        assert_eq!(
            victim_order((2, 2, 5, &k1), (2, 2, 6, &k2)),
            Ordering::Less,
            "with equal subscriber counts, least-recent GET ranks first"
        );

        // 4. Equal subscribers AND GET recency: smaller key bytes break the tie
        //    deterministically.
        let (small, large) = if k1.as_bytes() < k2.as_bytes() {
            (&k1, &k2)
        } else {
            (&k2, &k1)
        };
        assert_eq!(
            victim_order((3, 1, 7, small), (3, 1, 7, large)),
            Ordering::Less,
            "equal subscriber counts + GET recency fall back to key-byte order"
        );

        // Reflexive: identical ranking components compare Equal.
        assert_eq!(
            victim_order((1, 1, 1, &k1), (1, 1, 1, &k1)),
            Ordering::Equal
        );
    }

    // =========================================================================
    // Cost-pressure eviction (cost-aware eviction, #4861)
    // =========================================================================

    /// The candidacy predicate: ONLY zero-demand contracts — no local
    /// subscriptions, no downstream subscribers, AND no genuine GET/PUT
    /// within the cost window (invariant 3 counts reads as demand) — with
    /// strictly positive attributed cost are cost-eviction candidates. Any
    /// subscriber OR recent genuine access makes a contract non-candidate by
    /// construction.
    #[test]
    fn cost_eviction_candidate_requires_zero_demand_and_nonzero_cost() {
        // The one shape that qualifies.
        assert!(cost_eviction_candidate(0, 0, 1.0, false));
        // A local client subscription protects, regardless of cost.
        assert!(!cost_eviction_candidate(1, 0, f64::MAX, false));
        // A downstream subscriber protects, regardless of cost.
        assert!(!cost_eviction_candidate(0, 1, f64::MAX, false));
        assert!(!cost_eviction_candidate(2, 3, 100.0, false));
        // A genuine GET/PUT within the cost window protects, regardless of
        // cost — the read-but-never-subscribed class (River UI container)
        // invariant 3 says must be retained.
        assert!(!cost_eviction_candidate(0, 0, f64::MAX, true));
        assert!(!cost_eviction_candidate(1, 1, f64::MAX, true));
        // Zero-demand but no attributed cost: not a candidate (nothing to
        // shed — cost pressure must never become a generic zero-sub purge).
        assert!(!cost_eviction_candidate(0, 0, 0.0, false));
        // Negative / NaN rates (defensive: meter math) are not cost.
        assert!(!cost_eviction_candidate(0, 0, -1.0, false));
        assert!(!cost_eviction_candidate(0, 0, f64::NAN, false));
    }

    /// Helper: one cost axis with the given total/floor and per-contract
    /// rates.
    fn cost_axis(total_rate: f64, floor: f64, rates: &[(&ContractKey, f64)]) -> CostAxisPressure {
        CostAxisPressure {
            axis: "test_axis",
            total_rate,
            floor,
            rates: rates.iter().map(|(k, r)| (*k.id(), *r)).collect(),
        }
    }

    /// The discriminator at the cache level: under cost pressure — while
    /// comfortably UNDER the byte budget — the zero-subscriber contract
    /// dominating the axis is shed, while a SUBSCRIBED contract holding an
    /// equally dominant share is untouched (candidacy, not ranking, protects
    /// it).
    #[test]
    fn evict_cost_pressure_sheds_zero_demand_offender_never_subscribed() {
        let (mut cache, clock) = make_cache(1024 * 1024);
        let junk = make_key(1);
        let subscribed = make_key(2);
        let quiet = make_key(3);
        // Tiny states: byte budget is nowhere near exceeded (the #4861 shape).
        cache.record_access(junk, 121, AccessType::Put, 7, |_| (0, 0));
        cache.record_access(subscribed, 121, AccessType::Put, 3, |_| (0, 0));
        cache.record_access(quiet, 121, AccessType::Put, 1, |_| (0, 0));
        assert!(cache.stats().current_bytes < cache.stats().budget_bytes);
        // Age the PUT-seed genuine-access stamps past the cost window: the
        // storm has run for a long time with no further genuine GET/PUT.
        clock.advance_time(COST_RATE_MIN_WINDOW + Duration::from_secs(1));

        // junk holds 58% of the axis total with zero subscribers; the
        // subscribed contract holds 40% with a downstream subscriber; quiet
        // does no attributable work.
        let axes = [cost_axis(
            100_000.0,
            50_000.0,
            &[(&junk, 58_000.0), (&subscribed, 40_000.0)],
        )];
        let counts = |key: &ContractKey| if *key == subscribed { (0, 1) } else { (0, 0) };
        let evicted = cache.evict_cost_pressure(&counts, &axes);

        assert_eq!(evicted.len(), 1, "exactly the dominant zero-sub offender");
        assert_eq!(evicted[0].key, junk);
        assert_eq!(
            evicted[0].write_generation, 7,
            "generation snapshot travels with the eviction for the re-host guard"
        );
        assert!(
            !evicted[0].was_in_use,
            "cost victims are zero-demand by construction"
        );
        assert!(cache.get(&subscribed).is_some(), "subscribed survives");
        assert!(cache.get(&quiet).is_some(), "no-cost contract survives");
        assert!(cache.get(&junk).is_none());
        let stats = cache.stats();
        assert_eq!(stats.cost_evictions_total, 1);
        assert_eq!(
            stats.budget_evictions_total, 0,
            "cost evictions must not masquerade as byte-budget evictions"
        );
        assert_eq!(
            stats.current_bytes,
            2 * 121,
            "byte accounting reflects the shed entry"
        );
    }

    /// A genuine GET/PUT within the cost window protects a ZERO-subscriber
    /// contract from cost eviction (invariant 3: reads are demand — the
    /// never-subscribed-but-read River UI-container class), and the
    /// protection expires once the access ages past the window. UPDATE churn
    /// gets no such protection: it never stamps `last_genuine_access`.
    #[test]
    fn evict_cost_pressure_recent_genuine_access_protects() {
        let (mut cache, clock) = make_cache(1024 * 1024);
        let read_hot = make_key(1);
        cache.record_access(read_hot, 121, AccessType::Put, 1, |_| (0, 0));
        clock.advance_time(COST_RATE_MIN_WINDOW + Duration::from_secs(1));
        // A genuine GET just now (the local-serve `touch` path — a real read).
        cache.touch(&read_hot);

        let axes = [cost_axis(100_000.0, 50_000.0, &[(&read_hot, 90_000.0)])];
        let evicted = cache.evict_cost_pressure(&|_: &ContractKey| (0, 0), &axes);
        assert!(
            evicted.is_empty(),
            "a genuinely-read zero-subscriber contract must not be a cost victim"
        );
        assert!(cache.get(&read_hot).is_some());

        // The same contract with the same cost IS shed once the read ages
        // past the cost window — proving the gate is the recent access.
        clock.advance_time(COST_RATE_MIN_WINDOW + Duration::from_secs(1));
        let evicted = cache.evict_cost_pressure(&|_: &ContractKey| (0, 0), &axes);
        assert_eq!(evicted.len(), 1);
        assert_eq!(evicted[0].key, read_hot);
    }

    /// #4903 review Fix 1 (repeat-PUT facet), cache level: a repeat PUT to an
    /// ALREADY-hosted contract refreshes `last_genuine_access`, so a
    /// write-only publisher PUTting continuously is never a cost-eviction
    /// candidate — and the protection still expires once the PUTs stop.
    /// (The production wiring — `relay_put_store_locally` calling
    /// `host_contract` outside its `!was_hosting` gate — is pinned by
    /// `relay_put_store_locally_stamps_recency_on_repeat_put` in
    /// `operations/put/op_ctx_task.rs`.)
    #[test]
    fn evict_cost_pressure_repeat_put_refreshes_protection() {
        let (mut cache, clock) = make_cache(1024 * 1024);
        let publisher = make_key(1);
        cache.record_access(publisher, 121, AccessType::Put, 1, |_| (0, 0));
        // Long past the cost window since first host…
        clock.advance_time(COST_RATE_MIN_WINDOW + Duration::from_secs(1));
        // …but the publisher keeps PUTting (the refresh path: the entry is
        // already hosted, so this is `record_access` on an existing entry).
        cache.record_access(publisher, 121, AccessType::Put, 2, |_| (0, 0));

        let axes = [cost_axis(100_000.0, 50_000.0, &[(&publisher, 90_000.0)])];
        let evicted = cache.evict_cost_pressure(&|_: &ContractKey| (0, 0), &axes);
        assert!(
            evicted.is_empty(),
            "a repeat PUT is genuine client access (invariant 3) — the \
             write-only publisher must not be cost-evicted into a churn loop"
        );
        assert!(cache.get(&publisher).is_some());

        // Once the PUTs stop for a full cost window the protection lapses.
        clock.advance_time(COST_RATE_MIN_WINDOW + Duration::from_secs(1));
        let evicted = cache.evict_cost_pressure(&|_: &ContractKey| (0, 0), &axes);
        assert_eq!(evicted.len(), 1);
        assert_eq!(evicted[0].key, publisher);
    }

    /// #4903 review Fix 1 (local-lease facet): a local-client access within
    /// the reconciliation renewal lease (`SUBSCRIPTION_LEASE_DURATION`, 8 min
    /// — LONGER than the 5-min cost window) protects from cost eviction, so
    /// the sweep can never shed a contract `contracts_needing_renewal` is
    /// still renewing (the minutes-5-to-8 churn window).
    #[test]
    fn evict_cost_pressure_local_client_lease_protects() {
        use crate::ring::hosting::SUBSCRIPTION_LEASE_DURATION;
        let (mut cache, clock) = make_cache(1024 * 1024);
        let local = make_key(1);
        cache.record_access(local, 121, AccessType::Put, 1, |_| (0, 0));
        clock.advance_time(COST_RATE_MIN_WINDOW + Duration::from_secs(1));
        // Local client touches it now (GET via the HTTP/WS gateway path).
        cache.mark_local_client_access(&local);
        // Advance PAST the cost window but WITHIN the renewal lease: the
        // genuine-access stamp is stale, the local lease is not.
        clock.advance_time(COST_RATE_MIN_WINDOW + Duration::from_secs(1));
        assert!(cache.has_recent_local_client_access(&local, SUBSCRIPTION_LEASE_DURATION));

        let axes = [cost_axis(100_000.0, 50_000.0, &[(&local, 90_000.0)])];
        let evicted = cache.evict_cost_pressure(&|_: &ContractKey| (0, 0), &axes);
        assert!(
            evicted.is_empty(),
            "a contract inside the local-client renewal lease must not be a \
             cost victim (reconciliation still leases it — invariant 3)"
        );

        // Once the lease lapses too, the contract is shed.
        clock.advance_time(SUBSCRIPTION_LEASE_DURATION);
        let evicted = cache.evict_cost_pressure(&|_: &ContractKey| (0, 0), &axes);
        assert_eq!(evicted.len(), 1);
        assert_eq!(evicted[0].key, local);
    }

    /// #4903 review Fix 1 (restart facet): a restart reload leaves
    /// `last_genuine_access = None` but seeds `local_client_last_access = now`
    /// for locally-accessed contracts (one renewal window to re-establish
    /// subscriptions). Cost candidacy must honor that lease — without it the
    /// sweep evicts a just-reloaded contract reconciliation is about to renew
    /// (~2.5 min churn after every restart). A reloaded contract with NO
    /// pre-restart local access gets no such grace.
    #[test]
    fn evict_cost_pressure_restart_local_lease_protects() {
        use crate::ring::hosting::SUBSCRIPTION_LEASE_DURATION;
        let (mut cache, clock) = make_cache(1024 * 1024);
        let local = make_key(1);
        let unloved = make_key(2);
        cache.load_persisted_entry(
            local,
            121,
            AccessType::Get,
            Duration::from_secs(3600),
            true, // locally accessed pre-restart → lease seeded at load
        );
        cache.load_persisted_entry(
            unloved,
            121,
            AccessType::Put,
            Duration::from_secs(3600),
            false,
        );
        cache.finalize_loading();

        let axes = [cost_axis(
            100_000.0,
            50_000.0,
            &[(&local, 40_000.0), (&unloved, 40_000.0)],
        )];
        let evicted = cache.evict_cost_pressure(&|_: &ContractKey| (0, 0), &axes);
        let keys: Vec<ContractKey> = evicted.iter().map(|e| e.key).collect();
        assert_eq!(
            keys,
            vec![unloved],
            "post-restart: the locally-leased reload survives, the never-\
             locally-accessed one is shed"
        );
        assert!(cache.get(&local).is_some());

        // The restart grace is time-bounded: one renewal lease, then the
        // reloaded contract competes like anything else.
        clock.advance_time(SUBSCRIPTION_LEASE_DURATION + Duration::from_secs(1));
        let evicted = cache.evict_cost_pressure(&|_: &ContractKey| (0, 0), &axes);
        assert_eq!(evicted.len(), 1);
        assert_eq!(evicted[0].key, local);
    }

    /// Exact-25%-share boundary: a contract holding EXACTLY the threshold
    /// share of the axis total is NOT shed (the share test is strict `>`);
    /// epsilon above it IS.
    #[test]
    fn evict_cost_pressure_exact_share_boundary_not_shed() {
        let (mut cache, clock) = make_cache(1024 * 1024);
        let boundary = make_key(1);
        cache.record_access(boundary, 121, AccessType::Put, 1, |_| (0, 0));
        clock.advance_time(COST_RATE_MIN_WINDOW + Duration::from_secs(1));

        // rate == 0.25 × total exactly: NOT an offender.
        let at_threshold = [cost_axis(100_000.0, 50_000.0, &[(&boundary, 25_000.0)])];
        assert!(
            cache
                .evict_cost_pressure(&|_: &ContractKey| (0, 0), &at_threshold)
                .is_empty(),
            "exactly the 25% share must NOT trigger (strict >)"
        );
        assert!(cache.get(&boundary).is_some());

        // Strictly above the threshold: shed.
        let above = [cost_axis(100_000.0, 50_000.0, &[(&boundary, 25_000.1)])];
        let evicted = cache.evict_cost_pressure(&|_: &ContractKey| (0, 0), &above);
        assert_eq!(evicted.len(), 1);
        assert_eq!(evicted[0].key, boundary);
    }

    /// A LOCAL client subscription protects exactly like a downstream one.
    #[test]
    fn evict_cost_pressure_local_subscription_protects() {
        let (mut cache, clock) = make_cache(1024 * 1024);
        let hot = make_key(1);
        cache.record_access(hot, 121, AccessType::Put, 1, |_| (0, 0));
        clock.advance_time(COST_RATE_MIN_WINDOW + Duration::from_secs(1));
        let axes = [cost_axis(100_000.0, 50_000.0, &[(&hot, 90_000.0)])];
        let evicted = cache.evict_cost_pressure(&|_: &ContractKey| (1, 0), &axes);
        assert!(
            evicted.is_empty(),
            "a locally-subscribed contract is never a cost victim"
        );
        assert!(cache.get(&hot).is_some());
    }

    /// The absolute floor gates the axis: a node doing little total update
    /// work never cost-evicts, even at a 100% share.
    #[test]
    fn evict_cost_pressure_respects_absolute_floor() {
        let (mut cache, clock) = make_cache(1024 * 1024);
        let only = make_key(1);
        cache.record_access(only, 121, AccessType::Put, 1, |_| (0, 0));
        clock.advance_time(COST_RATE_MIN_WINDOW + Duration::from_secs(1));
        // Total below the floor: the lone contract holds 100% of not-much.
        let axes = [cost_axis(10_000.0, 50_000.0, &[(&only, 10_000.0)])];
        let evicted = cache.evict_cost_pressure(&|_: &ContractKey| (0, 0), &axes);
        assert!(
            evicted.is_empty(),
            "sub-floor totals never trigger cost pressure"
        );
        // Exactly AT the floor also does not trigger (strict >).
        let axes = [cost_axis(50_000.0, 50_000.0, &[(&only, 50_000.0)])];
        assert!(
            cache
                .evict_cost_pressure(&|_: &ContractKey| (0, 0), &axes)
                .is_empty()
        );
        assert_eq!(cache.stats().cost_evictions_total, 0);
    }

    /// The share threshold gates candidacy: many small zero-sub contracts
    /// none of which dominates are all retained.
    #[test]
    fn evict_cost_pressure_respects_share_threshold() {
        let (mut cache, clock) = make_cache(1024 * 1024);
        let a = make_key(1);
        let b = make_key(2);
        cache.record_access(a, 121, AccessType::Put, 1, |_| (0, 0));
        cache.record_access(b, 121, AccessType::Put, 1, |_| (0, 0));
        clock.advance_time(COST_RATE_MIN_WINDOW + Duration::from_secs(1));
        // Each holds 20% — above the floor in total, but no single offender
        // exceeds the 25% share.
        let axes = [cost_axis(
            100_000.0,
            50_000.0,
            &[(&a, 20_000.0), (&b, 20_000.0)],
        )];
        let evicted = cache.evict_cost_pressure(&|_: &ContractKey| (0, 0), &axes);
        assert!(
            evicted.is_empty(),
            "no single contract dominates → no eviction"
        );
    }

    /// Multiple super-threshold offenders are shed in descending-rate order
    /// (the deficit is covered when every super-threshold offender is gone),
    /// and a second axis naturally skips contracts already shed by the first.
    #[test]
    fn evict_cost_pressure_sheds_all_super_threshold_offenders_descending() {
        let (mut cache, clock) = make_cache(1024 * 1024);
        let big = make_key(1);
        let bigger = make_key(2);
        let small = make_key(3);
        cache.record_access(big, 121, AccessType::Put, 1, |_| (0, 0));
        cache.record_access(bigger, 121, AccessType::Put, 2, |_| (0, 0));
        cache.record_access(small, 121, AccessType::Put, 3, |_| (0, 0));
        clock.advance_time(COST_RATE_MIN_WINDOW + Duration::from_secs(1));
        let axes = [
            cost_axis(
                100_000.0,
                50_000.0,
                &[(&big, 30_000.0), (&bigger, 45_000.0), (&small, 10_000.0)],
            ),
            // Second axis where an already-shed contract also dominates: it
            // is gone from the hosted set, so only itself would qualify and
            // nothing double-evicts.
            cost_axis(2_000_000.0, 131_072.0, &[(&bigger, 1_900_000.0)]),
        ];
        let evicted = cache.evict_cost_pressure(&|_: &ContractKey| (0, 0), &axes);
        let keys: Vec<ContractKey> = evicted.iter().map(|e| e.key).collect();
        assert_eq!(
            keys,
            vec![bigger, big],
            "descending attributed-rate order; sub-threshold contract retained; \
             no double-eviction across axes"
        );
        assert!(cache.get(&small).is_some());
        assert_eq!(cache.stats().cost_evictions_total, 2);
    }

    #[test]
    fn test_empty_cache() {
        let (cache, _) = make_cache(1000);
        assert!(cache.is_empty());
        assert_eq!(cache.len(), 0);
        assert_eq!(cache.current_bytes(), 0);
        assert!(!cache.contains(&make_key(1)));
    }

    #[test]
    fn test_add_single_contract() {
        let (mut cache, _) = make_cache(1000);
        let key = make_key(1);

        let result = cache.record_access(key, 100, AccessType::Get, 0, |_| (0, 0));

        assert!(result.is_new);
        assert!(result.evicted.is_empty());
        assert!(cache.contains(&key));
        assert_eq!(cache.len(), 1);
        assert_eq!(cache.current_bytes(), 100);

        let info = cache.get(&key).unwrap();
        assert_eq!(info.size_bytes, 100);
        assert_eq!(info.access_type, AccessType::Get);
    }

    #[test]
    fn test_refresh_existing_contract() {
        let (mut cache, time) = make_cache(1000);
        let key = make_key(1);

        // First access
        cache.record_access(key, 100, AccessType::Get, 0, |_| (0, 0));
        let first_access = cache.get(&key).unwrap().last_accessed;

        // Advance time and access again
        time.advance_time(Duration::from_secs(10));
        cache.record_access(key, 100, AccessType::Put, 0, |_| (0, 0));

        // Should still be one contract, but updated
        assert_eq!(cache.len(), 1);
        assert_eq!(cache.current_bytes(), 100);

        let info = cache.get(&key).unwrap();
        assert_eq!(info.access_type, AccessType::Put);
        assert!(info.last_accessed > first_access);
    }

    /// Over budget, eviction happens IMMEDIATELY — there is no `min_ttl` floor
    /// (dropped 2026-07-08). Adding a third entry that pushes over budget sheds the
    /// least-recently-accessed entry at once; the just-inserted entry is protected.
    /// (The former `test_lru_eviction_respects_ttl` / `test_lru_eviction_after_ttl`
    /// pair, which asserted min_ttl grace then post-TTL eviction, was collapsed into
    /// this single no-floor pin.)
    #[test]
    fn test_over_budget_evicts_least_recent_no_min_ttl_floor() {
        // Cache with max 200 bytes.
        let (mut cache, _time) = make_cache(200);
        let key1 = make_key(1);
        let key2 = make_key(2);
        let key3 = make_key(3);

        // Add two entries (fits exactly, no eviction).
        cache.record_access(key1, 100, AccessType::Get, 0, |_| (0, 0));
        cache.record_access(key2, 100, AccessType::Get, 0, |_| (0, 0));
        assert_eq!(cache.current_bytes(), 200);

        // Add third entry - immediately evicts key1 (oldest); no TTL wait.
        let result = cache.record_access(key3, 100, AccessType::Get, 0, |_| (0, 0));
        assert!(result.is_new);
        assert_eq!(result.evicted, vec![(key1, 0)]);
        assert_eq!(cache.len(), 2);
        assert!(!cache.contains(&key1));
        assert!(cache.contains(&key2));
        assert!(cache.contains(&key3));
    }

    #[test]
    fn test_access_refreshes_lru_position() {
        let (mut cache, time) = make_cache(200);
        let key1 = make_key(1);
        let key2 = make_key(2);
        let key3 = make_key(3);

        // Add two contracts
        cache.record_access(key1, 100, AccessType::Get, 0, |_| (0, 0));
        cache.record_access(key2, 100, AccessType::Get, 0, |_| (0, 0));

        // GET key1 again - a real GET refreshes the eviction recency position,
        // so it becomes the LAST eviction candidate; key2 is now first.
        cache.record_access(key1, 100, AccessType::Get, 0, |_| (0, 0));

        // Eviction order (least-worthy first) should now be [key2, key1]
        let order = cache.keys_eviction_order();
        assert_eq!(order, vec![key2, key1]);

        // Advance past TTL and add key3 - should evict key2 (now oldest)
        time.advance_time(Duration::from_secs(61));
        let result = cache.record_access(key3, 100, AccessType::Get, 0, |_| (0, 0));

        assert_eq!(result.evicted, vec![(key2, 0)]);
        assert!(cache.contains(&key1));
        assert!(!cache.contains(&key2));
        assert!(cache.contains(&key3));
    }

    #[test]
    fn test_touch_refreshes_ttl() {
        let (mut cache, time) = make_cache(200);
        let key1 = make_key(1);
        let key2 = make_key(2);
        let key3 = make_key(3);

        // Add two entries
        cache.record_access(key1, 100, AccessType::Get, 0, |_| (0, 0));
        cache.record_access(key2, 100, AccessType::Get, 0, |_| (0, 0));

        // Advance time by 50 seconds
        time.advance_time(Duration::from_secs(50));

        // Touch key1 (simulating UPDATE received)
        cache.touch(&key1);

        // Advance another 15 seconds (key1 now at 15s, key2 at 65s)
        time.advance_time(Duration::from_secs(15));

        // Add key3 - should evict key2 (past TTL), NOT key1 (recently touched)
        let result = cache.record_access(key3, 100, AccessType::Get, 0, |_| (0, 0));

        assert_eq!(
            result.evicted,
            vec![(key2, 0)],
            "Should evict key2 which is past TTL"
        );
        assert!(
            cache.contains(&key1),
            "key1 should remain (touched recently)"
        );
        assert!(cache.contains(&key3));
    }

    #[test]
    fn test_large_contract_evicts_multiple() {
        let (mut cache, time) = make_cache(300);

        let small1 = make_key(1);
        let small2 = make_key(2);
        let small3 = make_key(3);
        let large = make_key(4);

        // Add three small contracts
        cache.record_access(small1, 100, AccessType::Get, 0, |_| (0, 0));
        cache.record_access(small2, 100, AccessType::Get, 0, |_| (0, 0));
        cache.record_access(small3, 100, AccessType::Get, 0, |_| (0, 0));
        assert_eq!(cache.current_bytes(), 300);

        // Advance past TTL
        time.advance_time(Duration::from_secs(61));

        // Add one large contract - should evict two small ones
        let result = cache.record_access(large, 200, AccessType::Put, 0, |_| (0, 0));

        assert_eq!(result.evicted.len(), 2);
        assert_eq!(result.evicted[0], (small1, 0)); // Oldest first
        assert_eq!(result.evicted[1], (small2, 0));
        assert!(!cache.contains(&small1));
        assert!(!cache.contains(&small2));
        assert!(cache.contains(&small3));
        assert!(cache.contains(&large));
    }

    #[test]
    fn test_sweep_expired() {
        // Seed three entries under a generous budget (no auto-evict), then tighten
        // so the cache sits over budget for the sweep. (There is no `min_ttl` floor
        // since 2026-07-08, so record_access sheds to budget immediately — the
        // "3 resident over budget" state is only reachable via a budget shrink.)
        let (mut cache, _time) = make_cache(100_000);
        let key1 = make_key(1);
        let key2 = make_key(2);
        let key3 = make_key(3);

        cache.record_access(key1, 100, AccessType::Get, 0, |_| (0, 0)); // recency: oldest
        cache.record_access(key2, 100, AccessType::Get, 0, |_| (0, 0));
        cache.record_access(key3, 100, AccessType::Get, 0, |_| (0, 0));
        cache.set_budget_for_test(200);
        assert_eq!(cache.len(), 3);
        assert_eq!(cache.current_bytes(), 300);

        // Sweep evicts the least-recently-accessed entry (key1) to get back under
        // budget.
        let evicted = cache.sweep_expired(|_| (0, 0), MemoryPressure::AtCapacity);
        assert_eq!(evicted_pairs(&evicted), vec![(key1, 0)]);
        assert_eq!(cache.current_bytes(), 200);
    }

    #[test]
    fn test_sweep_orders_subscribed_last() {
        // Seed three entries under a generous budget (distinct recency), then
        // tighten so the sweep runs over budget.
        let (mut cache, _time) = make_cache(100_000);
        let key1 = make_key(1);
        let key2 = make_key(2);
        let key3 = make_key(3);

        cache.record_access(key1, 100, AccessType::Get, 0, |_| (0, 0));
        cache.record_access(key2, 100, AccessType::Get, 0, |_| (0, 0));
        cache.record_access(key3, 100, AccessType::Get, 0, |_| (0, 0));
        cache.set_budget_for_test(200);

        // Sweep with a subscriber count that makes key1 subscribed (downstream).
        // Under subscriber-primary ordering it sorts LAST, so the zero-subscriber
        // key2 is shed first — one eviction is enough to get back under budget, so
        // key1 survives (ordered last, not hard-pinned).
        let evicted =
            cache.sweep_expired(|k| (0, (*k == key1) as usize), MemoryPressure::AtCapacity);

        // key1 (subscribed) survives, key2 (zero-subscriber) evicted to get under budget
        assert_eq!(evicted_pairs(&evicted), vec![(key2, 0)]);
        assert!(cache.contains(&key1));
        assert!(!cache.contains(&key2));
        assert!(cache.contains(&key3));
        assert_eq!(cache.current_bytes(), 200);
    }

    /// `record_access` orders subscribed contracts LAST: over budget past TTL,
    /// the subscribed entry survives while a zero-subscriber past-TTL entry is
    /// shed instead (one eviction is enough to get back under budget). It is NOT
    /// a hard pin — see `evict_all_subscribed_sheds_fewest_as_last_resort` for
    /// the last-resort shed when EVERY contract is subscribed.
    #[test]
    fn test_record_access_orders_subscribed_last() {
        let (mut cache, time) = make_cache(200);
        let retained = make_key(1);
        let evictable = make_key(2);
        let trigger = make_key(3);

        // Fill the cache: `retained` (oldest) then `evictable`.
        cache.record_access(retained, 100, AccessType::Get, 0, |_| (0, 0));
        cache.record_access(evictable, 100, AccessType::Get, 0, |_| (0, 0));
        assert_eq!(cache.current_bytes(), 200);

        // Advance past TTL so both existing entries are eviction-eligible.
        time.advance_time(Duration::from_secs(61));

        // Insert a third entry, over budget. `retained` is the oldest, so a
        // naive LRU eviction would drop it first — but it is subscribed
        // (downstream), so under subscriber-primary ordering it sorts LAST and the
        // zero-subscriber `evictable` is shed first. One eviction is enough, so
        // `retained` survives (ordered last, not hard-pinned).
        let result = cache.record_access(trigger, 100, AccessType::Get, 0, |k| {
            (0, (*k == retained) as usize)
        });

        assert_eq!(
            result.evicted,
            vec![(evictable, 0)],
            "the subscribed contract is ordered last; the zero-subscriber \
             past-TTL contract must be evicted instead"
        );
        assert!(
            cache.contains(&retained),
            "subscribed contract survives while a zero-subscriber one can be shed"
        );
        assert!(!cache.contains(&evictable));
        assert!(cache.contains(&trigger));
    }

    /// The contract inserted by `record_access` is the op-scoped `protected` key,
    /// so it is never evicted by the same call even when the insert pushes over
    /// budget — the backstop that replaces the old `min_ttl` age-0 guarantee (now
    /// that `min_ttl` is dropped, 2026-07-08). It is also the most-recently-
    /// accessed, so it sorts last regardless.
    #[test]
    fn test_record_access_never_evicts_the_new_entry() {
        let (mut cache, _time) = make_cache(100);
        let first = make_key(1);
        let second = make_key(2);

        cache.record_access(first, 100, AccessType::Get, 0, |_| (0, 0));

        // Inserting `second` puts the cache over budget; `first` (older, zero-
        // subscriber) is evicted, but the just-inserted `second` (the op-protected
        // key) must survive.
        let result = cache.record_access(second, 100, AccessType::Get, 0, |_| (0, 0));
        assert_eq!(result.evicted, vec![(first, 0)]);
        assert!(cache.contains(&second));
        assert!(!cache.contains(&first));
    }

    #[test]
    fn test_touch_non_existent_is_no_op() {
        let (mut cache, _) = make_cache(1000);
        let key = make_key(1);

        // Touch a key that doesn't exist
        cache.touch(&key);

        // Should remain empty
        assert!(cache.is_empty());
        assert!(!cache.contains(&key));
    }

    #[test]
    fn test_access_types() {
        let (mut cache, _) = make_cache(1000);
        let key = make_key(1);

        // Test each access type is recorded correctly
        cache.record_access(key, 100, AccessType::Get, 0, |_| (0, 0));
        assert_eq!(cache.get(&key).unwrap().access_type, AccessType::Get);

        cache.record_access(key, 100, AccessType::Put, 0, |_| (0, 0));
        assert_eq!(cache.get(&key).unwrap().access_type, AccessType::Put);

        cache.record_access(key, 100, AccessType::Subscribe, 0, |_| (0, 0));
        assert_eq!(cache.get(&key).unwrap().access_type, AccessType::Subscribe);
    }

    #[test]
    fn test_contract_size_change() {
        let (mut cache, _) = make_cache(1000);
        let key = make_key(1);

        // Add contract with initial size
        cache.record_access(key, 100, AccessType::Get, 0, |_| (0, 0));
        assert_eq!(cache.current_bytes(), 100);
        assert_eq!(cache.get(&key).unwrap().size_bytes, 100);

        // Contract state grows
        cache.record_access(key, 200, AccessType::Put, 0, |_| (0, 0));
        assert_eq!(cache.current_bytes(), 200);
        assert_eq!(cache.get(&key).unwrap().size_bytes, 200);

        // Contract state shrinks
        cache.record_access(key, 150, AccessType::Put, 0, |_| (0, 0));
        assert_eq!(cache.current_bytes(), 150);
        assert_eq!(cache.get(&key).unwrap().size_bytes, 150);
    }

    #[test]
    fn test_iter_returns_all_hosted_keys() {
        let (mut cache, _) = make_cache(1000);

        // Empty cache yields no keys
        assert_eq!(cache.iter().count(), 0);

        let key1 = make_key(1);
        let key2 = make_key(2);
        let key3 = make_key(3);

        cache.record_access(key1, 100, AccessType::Get, 0, |_| (0, 0));
        cache.record_access(key2, 100, AccessType::Put, 0, |_| (0, 0));
        cache.record_access(key3, 100, AccessType::Subscribe, 0, |_| (0, 0));

        let keys: Vec<ContractKey> = cache.iter().collect();
        assert_eq!(keys.len(), 3);
        assert!(keys.contains(&key1));
        assert!(keys.contains(&key2));
        assert!(keys.contains(&key3));
    }

    /// `record_access` MUST capture the supplied `write_generation` on
    /// the inserted entry AND surface it on the `RecordAccessResult.evicted`
    /// tuple when that entry is later evicted by a subsequent
    /// `record_access`. This is the load-bearing flow for the
    /// EvictContract re-host race fix — `RuntimePool::remove_contract`
    /// compares this captured generation against the then-current
    /// generation to decide whether a write raced ahead of the eviction.
    /// Regression test for PR #4212 review round C.
    #[test]
    fn test_record_access_carries_write_generation_through_eviction() {
        let (mut cache, time) = make_cache(100);
        let evicted_key = make_key(1);
        let trigger_key = make_key(2);

        // Insert `evicted_key` with a specific captured generation.
        let captured_generation: u64 = 0xABCD_EF42;
        let first = cache.record_access(
            evicted_key,
            100,
            AccessType::Get,
            captured_generation,
            |_| (0, 0),
        );
        assert!(first.evicted.is_empty(), "first insert evicts nothing");
        assert_eq!(
            cache
                .get(&evicted_key)
                .expect("just inserted")
                .write_generation,
            captured_generation,
            "captured generation must be stored on the HostedContract entry"
        );

        // Advance past TTL and trigger an over-budget insert; this must
        // evict `evicted_key` and emit the captured generation alongside
        // it on the result tuple.
        time.advance_time(Duration::from_secs(61));
        let result = cache.record_access(
            trigger_key,
            100,
            AccessType::Get,
            999, // trigger's own generation is irrelevant to the eviction
            |_| (0, 0),
        );
        assert_eq!(
            result.evicted,
            vec![(evicted_key, captured_generation)],
            "evicted tuple must carry the generation captured atomically \
             when the evicted entry was inserted"
        );
    }

    /// Re-accessing an existing entry refreshes its captured generation.
    ///
    /// Rationale: a re-access (GET/PUT/SUBSCRIBE) is a fresh "I'm
    /// hosting this state" assertion. If a state write bumped the
    /// generation between the original insert and the refresh, the
    /// refresh must adopt the new generation so a later eviction
    /// captures the correct generation snapshot.
    #[test]
    fn test_record_access_refresh_updates_write_generation() {
        let (mut cache, _) = make_cache(1000);
        let key = make_key(7);

        cache.record_access(key, 100, AccessType::Get, 1, |_| (0, 0));
        assert_eq!(cache.get(&key).unwrap().write_generation, 1);

        // Refresh with a higher generation (simulates a state write that
        // bumped the generation between the original insert and a later
        // GET/PUT/SUBSCRIBE re-access).
        cache.record_access(key, 100, AccessType::Put, 5, |_| (0, 0));
        assert_eq!(
            cache.get(&key).unwrap().write_generation,
            5,
            "re-access must refresh the captured generation snapshot"
        );
    }

    /// Age gate: has_recent_local_client_access returns false after the
    /// max_age window expires. This is the TTL enforcement for the cleanup
    /// exemption rule (AGENTS.md).
    #[test]
    fn test_local_client_access_age_gate_expiry() {
        let lease = Duration::from_secs(480); // SUBSCRIPTION_LEASE_DURATION
        let (mut cache, time) = make_cache(10000);
        let key = make_key(1);

        cache.record_access(key, 100, AccessType::Get, 0, |_| (0, 0));
        cache.mark_local_client_access(&key);

        // Immediately after marking: recent access is true
        assert!(cache.has_local_client_access(&key));
        assert!(cache.has_recent_local_client_access(&key, lease));

        // Advance time just under the lease -- still recent
        time.advance_time(lease - Duration::from_secs(1));
        assert!(cache.has_recent_local_client_access(&key, lease));

        // Advance past the lease -- no longer recent
        time.advance_time(Duration::from_secs(2));
        assert!(
            !cache.has_recent_local_client_access(&key, lease),
            "Contract should exit renewal after lease expires"
        );

        // The flag itself is still set (sticky)
        assert!(
            cache.has_local_client_access(&key),
            "Flag should remain sticky even after age gate expires"
        );

        // Re-marking refreshes the timestamp
        cache.mark_local_client_access(&key);
        assert!(
            cache.has_recent_local_client_access(&key, lease),
            "Re-marking should refresh the age gate"
        );
    }

    #[test]
    fn eviction_ordered_scores_matches_key_order_and_carries_fields() {
        // The dashboard reads `eviction_ordered_scores` (production) to render
        // piece A's demand-driven eviction. It must yield the same order as the
        // test-only `keys_eviction_order` and carry each entry's score fields.
        let (mut cache, _) = make_cache(10_000);
        let key1 = make_key(1);
        let key2 = make_key(2);
        let key3 = make_key(3);
        cache.record_access(key1, 111, AccessType::Get, 0, |_| (0, 0));
        cache.record_access(key2, 222, AccessType::Get, 0, |_| (0, 0));
        cache.record_access(key3, 333, AccessType::Get, 0, |_| (0, 0));

        let scores = cache.eviction_ordered_scores();
        let score_keys: Vec<_> = scores.iter().map(|s| s.key).collect();
        assert_eq!(
            score_keys,
            cache.keys_eviction_order(),
            "eviction_ordered_scores must use the same order as keys_eviction_order"
        );
        for s in &scores {
            let entry = cache.get(&s.key).expect("scored key is in the cache");
            assert_eq!(s.keep_score, entry.keep_score);
            assert_eq!(s.size_bytes, entry.size_bytes);
            assert_eq!(s.read_count, entry.read_count);
            assert_eq!(s.predicted_demand, entry.predicted_demand);
            assert_eq!(s.recency_seq, entry.recency_seq);
        }
    }

    // The former `eviction_ordered_scores_flags_past_min_ttl_after_ttl_elapses`
    // test was DELETED: it exercised the `past_min_ttl` age gate, which no longer
    // exists (the `min_ttl` cold-start floor was dropped 2026-07-08 — invariant 3).
    // Dashboard eviction-eligibility is now "not in use" only; that half is
    // covered by `HostingManager::is_eviction_eligible` tests in hosting.rs.

    // --- Recently-abandoned priority (demand-credit strip) ---

    // Superseded by subscriber-primary eviction rework (#4642, PR-1): eviction
    // now orders by (subscriber_count, real-GET recency); demand/distance/
    // abandonment no longer drive eviction order. Retained as historical
    // documentation of the demoted demand machinery.
    #[ignore]
    #[test]
    fn test_record_abandonment_moves_entry_to_eviction_front() {
        let (mut cache, _) = make_cache(1000);
        let key1 = make_key(1);
        let key2 = make_key(2);
        let key3 = make_key(3);

        // Equal (neutral) demand -> equal keep_score; eviction order is by
        // access sequence: key1, key2, key3.
        cache.record_access(key1, 100, AccessType::Get, 0, |_| (0, 0));
        cache.record_access(key2, 100, AccessType::Get, 0, |_| (0, 0));
        cache.record_access(key3, 100, AccessType::Get, 0, |_| (0, 0));
        assert_eq!(cache.keys_eviction_order(), vec![key1, key2, key3]);

        // Abandon key3 — the most recently used — and its keep_score drops to
        // the floor (below the still-credited key1/key2), so it jumps to the
        // FRONT of the eviction order so budget pressure sees it first.
        cache.record_abandonment(&key3);
        assert_eq!(cache.keys_eviction_order(), vec![key3, key1, key2]);

        let info = cache.get(&key3).unwrap();
        assert!(
            info.abandoned_at.is_some(),
            "Abandonment must record a timestamp"
        );
        assert_eq!(
            info.keep_score,
            cache.eviction_floor(),
            "abandonment must strip demand credit to the floor"
        );
    }

    /// Decision 3 (invariant 3, 2026-07-08): the recency clock starts at
    /// subscription TERMINATION. `record_abandonment` (the termination hook) resets
    /// the contract's `recency_seq` to the current frontier, so a formerly-
    /// subscribed contract is NOT instantly evicted for an old last-read it accrued
    /// while parked in the subscription tier. Here `subscribed` has the OLDEST GET
    /// recency, but after termination it becomes the newest and survives; the
    /// genuinely-oldest zero-subscriber contract is the victim instead.
    #[test]
    fn record_abandonment_resets_recency_at_subscription_termination() {
        let (mut cache, _time) = make_cache(300); // 3x100-byte entries fit
        let subscribed = make_key(1);
        let older = make_key(2);
        let newer = make_key(3);
        let trigger = make_key(4);

        // Recency order after the GETs: subscribed < older < newer.
        cache.record_access(subscribed, 100, AccessType::Get, 0, |_| (0, 0));
        cache.record_access(older, 100, AccessType::Get, 0, |_| (0, 0));
        cache.record_access(newer, 100, AccessType::Get, 0, |_| (0, 0));

        // The subscription on `subscribed` terminates → recency reset to newest.
        cache.record_abandonment(&subscribed);
        assert!(
            cache.get(&subscribed).unwrap().recency_seq > cache.get(&newer).unwrap().recency_seq,
            "termination must reset recency to the frontier (newer than everything else)"
        );

        // Insert `trigger` over budget: the victim is the genuinely least-recent
        // zero-subscriber contract (`older`), NOT the formerly-subscribed one.
        let result = cache.record_access(trigger, 100, AccessType::Get, 0, |_| (0, 0));
        assert_eq!(
            result.evicted,
            vec![(older, 0)],
            "the least-recent zero-subscriber contract is shed; termination protected `subscribed`"
        );
        assert!(
            cache.contains(&subscribed),
            "the formerly-subscribed contract survived: termination reset its recency"
        );
        assert!(!cache.contains(&older));
    }

    #[test]
    fn test_record_abandonment_is_idempotent() {
        let (mut cache, time) = make_cache(1000);
        let key = make_key(1);
        cache.record_access(key, 100, AccessType::Get, 0, |_| (0, 0));

        cache.record_abandonment(&key);
        let first = cache.get(&key).unwrap().abandoned_at;
        assert!(first.is_some());

        time.advance_time(Duration::from_secs(5));
        cache.record_abandonment(&key);
        let second = cache.get(&key).unwrap().abandoned_at;

        // First abandonment timestamp wins — we want to time-from the
        // moment use ended, not from later re-marks.
        assert_eq!(first, second);
    }

    #[test]
    fn test_record_abandonment_missing_key_is_noop() {
        let (mut cache, _) = make_cache(1000);
        let key = make_key(42);
        // Should not panic, should not insert anything.
        cache.record_abandonment(&key);
        assert!(!cache.contains(&key));
        assert_eq!(cache.len(), 0);
    }

    #[test]
    fn test_record_access_clears_abandoned_marker() {
        let (mut cache, _) = make_cache(1000);
        let key = make_key(1);
        cache.record_access(key, 100, AccessType::Get, 0, |_| (0, 0));
        cache.record_abandonment(&key);
        assert!(cache.get(&key).unwrap().abandoned_at.is_some());

        // Fresh access — a re-subscribe or re-GET — clears the marker
        // and returns the entry to the LRU tail.
        cache.record_access(key, 100, AccessType::Get, 0, |_| (0, 0));
        assert!(
            cache.get(&key).unwrap().abandoned_at.is_none(),
            "record_access must clear abandoned_at"
        );
    }

    #[test]
    fn test_touch_clears_abandoned_marker() {
        let (mut cache, _) = make_cache(1000);
        let key = make_key(1);
        cache.record_access(key, 100, AccessType::Get, 0, |_| (0, 0));
        cache.record_abandonment(&key);

        cache.touch(&key);
        assert!(
            cache.get(&key).unwrap().abandoned_at.is_none(),
            "touch must clear abandoned_at"
        );
    }

    // Superseded by subscriber-primary eviction rework (#4642, PR-1): eviction
    // now orders by (subscriber_count, real-GET recency); demand/distance/
    // abandonment no longer drive eviction order. Retained as historical
    // documentation of the demoted demand machinery.
    #[ignore]
    #[test]
    fn test_abandoned_entry_evicted_before_active_under_pressure() {
        // Two entries, 100 bytes each, budget 200. Adding a third
        // pushes the cache 100 bytes over budget. Both existing entries
        // are past TTL. Without the abandonment bump key1 (oldest)
        // evicts; with the bump key2 (abandoned-and-bumped) evicts
        // even though key1 is older.
        let (mut cache, time) = make_cache(200);
        let key1 = make_key(1);
        let key2 = make_key(2);
        let key3 = make_key(3);

        cache.record_access(key1, 100, AccessType::Get, 0, |_| (0, 0));
        cache.record_access(key2, 100, AccessType::Get, 0, |_| (0, 0));

        // key2 is abandoned (latest in use, just lost its subscribers).
        cache.record_abandonment(&key2);

        // Both past TTL.
        time.advance_time(Duration::from_secs(61));

        // Insert key3: now over budget, must evict one. The abandoned
        // bucket means key2 goes first, even though key1 is the older
        // entry by `last_accessed`.
        let result = cache.record_access(key3, 100, AccessType::Get, 0, |_| (0, 0));
        assert!(result.is_new);
        assert_eq!(
            result.evicted,
            vec![(key2, 0)],
            "abandoned entry must evict first"
        );
        assert!(cache.contains(&key1));
        assert!(!cache.contains(&key2));
        assert!(cache.contains(&key3));
    }

    #[test]
    fn test_idle_persistence_preserved_when_under_pressure() {
        // Two entries, 100 bytes each, budget 1000. No pressure at all.
        // Abandoning an entry MUST NOT evict it — the network's
        // persistence-beyond-active-demand property only yields to
        // disk pressure, never to abandonment alone.
        let (mut cache, time) = make_cache(1000);
        let key1 = make_key(1);
        let key2 = make_key(2);

        cache.record_access(key1, 100, AccessType::Get, 0, |_| (0, 0));
        cache.record_access(key2, 100, AccessType::Get, 0, |_| (0, 0));

        cache.record_abandonment(&key2);
        time.advance_time(Duration::from_secs(3600));

        // No budget pressure → sweep_expired evicts nothing, abandoned
        // or otherwise.
        let evicted = cache.sweep_expired(|_| (0, 0), MemoryPressure::AtCapacity);
        assert!(evicted.is_empty(), "no pressure → no eviction");
        assert!(cache.contains(&key1));
        assert!(cache.contains(&key2));
    }

    // --- Capability-relative default budget + telemetry (A2) ---

    const MIB: u64 = 1024 * 1024;
    const GIB: u64 = 1024 * 1024 * 1024;

    /// The default budget scales with the memory the node may use, clamped to a
    /// sane floor/ceiling: `clamp(total_ram / 8, 128 MiB, 1 GiB)`.
    #[test]
    fn budget_for_ram_scales_and_clamps() {
        // Tiny box: total_ram / 8 is below the floor -> clamped up to MIN.
        assert_eq!(budget_for_ram(512 * MIB), MIN_DEFAULT_HOSTING_BUDGET_BYTES);
        assert_eq!(budget_for_ram(GIB), MIN_DEFAULT_HOSTING_BUDGET_BYTES);

        // Mid-range boxes scale linearly with RAM. The #4565 target: a 2 GiB VM
        // gets 256 MiB, far below the old flat 1 GiB, so it stops accumulating
        // hosted state toward OOM.
        assert_eq!(budget_for_ram(2 * GIB), 256 * MIB);
        assert_eq!(budget_for_ram(4 * GIB), 512 * MIB);

        // Ample-RAM box (>= 8 GiB) hits the ceiling, which equals the historical
        // flat default, so large gateways see NO change.
        assert_eq!(budget_for_ram(8 * GIB), MAX_DEFAULT_HOSTING_BUDGET_BYTES);
        assert_eq!(budget_for_ram(128 * GIB), MAX_DEFAULT_HOSTING_BUDGET_BYTES);
    }

    /// #4441 shape guard: the budget is RAM-SCALED, not a fixed count/constant.
    /// The old fixed >1024-contract cap was removed because it thrashed/OOM'd;
    /// the replacement budget must vary with RAM in the scaling band, and a
    /// memory-constrained box must get strictly LESS than a large one (and less
    /// than the old flat 1 GiB default).
    #[test]
    fn budget_for_ram_is_ram_scaled_not_fixed() {
        let small = budget_for_ram(2 * GIB);
        let large = budget_for_ram(6 * GIB);
        assert!(
            small < large,
            "budget must vary with RAM within the band, got small={small} large={large}"
        );
        assert!(
            small < MAX_DEFAULT_HOSTING_BUDGET_BYTES,
            "a memory-constrained box must get less than the old flat 1 GiB default"
        );
        // Genuinely a function of RAM, not a constant: distinct in-band inputs
        // give distinct budgets.
        assert_ne!(budget_for_ram(2 * GIB), budget_for_ram(3 * GIB));
        assert_ne!(budget_for_ram(3 * GIB), budget_for_ram(4 * GIB));
    }

    // --- Aggregate disk budget sizing (#4683) ---

    /// `disk_budget_for` clamps at both ends and scales linearly in between.
    #[test]
    fn disk_budget_for_scales_and_clamps() {
        // pct * (used + avail) below the MIN floor clamps up.
        assert_eq!(
            disk_budget_for(0, 100 * MIB, 0.5),
            MIN_DEFAULT_HOSTING_BUDGET_BYTES,
            "50 MiB budget is below the 128 MiB floor"
        );
        // Mid-range scales linearly: 0.5 * (used=4 GiB + avail=4 GiB) = 4 GiB.
        assert_eq!(disk_budget_for(4 * GIB, 4 * GIB, 0.5), 4 * GIB);
        // used counts toward the basis, not just free space.
        assert_eq!(disk_budget_for(2 * GIB, 0, 0.5), GIB);
        // Above the MAX cap clamps down.
        assert_eq!(
            disk_budget_for(200 * GIB, 200 * GIB, 0.5),
            DEFAULT_MAX_HOSTING_DISK_BYTES,
            "200 GiB budget is above the 32 GiB cap"
        );
    }

    /// pct=0 and pct=1 are the degenerate endpoints; both stay within the clamp.
    #[test]
    fn disk_budget_for_pct_endpoints() {
        // pct=0 → 0, clamped up to MIN.
        assert_eq!(
            disk_budget_for(10 * GIB, 10 * GIB, 0.0),
            MIN_DEFAULT_HOSTING_BUDGET_BYTES
        );
        // pct=1 → full basis, clamped down to MAX when the basis is large.
        assert_eq!(
            disk_budget_for(100 * GIB, 0, 1.0),
            DEFAULT_MAX_HOSTING_DISK_BYTES
        );
        // pct=1 with a small in-band basis passes through unclamped.
        assert_eq!(disk_budget_for(GIB, GIB, 1.0), 2 * GIB);
    }

    /// Zero free / zero used degenerate inputs never panic and honor the floor.
    #[test]
    fn disk_budget_for_zero_and_huge_free() {
        // Everything zero → MIN floor.
        assert_eq!(disk_budget_for(0, 0, 0.5), MIN_DEFAULT_HOSTING_BUDGET_BYTES);
        // Huge free (the `available_bytes` MAX-fallback case) does NOT overflow;
        // it clamps to the MAX cap.
        assert_eq!(
            disk_budget_for(0, u64::MAX, 0.5),
            DEFAULT_MAX_HOSTING_DISK_BYTES
        );
    }

    /// A non-finite / out-of-range pct collapses to a valid budget rather than
    /// propagating NaN or a nonsense value into the eviction floor.
    #[test]
    fn disk_budget_for_rejects_bad_pct() {
        // NaN / infinity → 0 → clamped up to MIN.
        assert_eq!(
            disk_budget_for(10 * GIB, 10 * GIB, f64::NAN),
            MIN_DEFAULT_HOSTING_BUDGET_BYTES
        );
        assert_eq!(
            disk_budget_for(10 * GIB, 10 * GIB, f64::INFINITY),
            MIN_DEFAULT_HOSTING_BUDGET_BYTES
        );
        // Negative pct clamps to 0 → MIN.
        assert_eq!(
            disk_budget_for(10 * GIB, 10 * GIB, -1.0),
            MIN_DEFAULT_HOSTING_BUDGET_BYTES
        );
        // pct > 1 clamps to 1.0 (full basis), then to MAX.
        assert_eq!(
            disk_budget_for(100 * GIB, 0, 5.0),
            DEFAULT_MAX_HOSTING_DISK_BYTES
        );
    }

    /// Saturating fixed-point: `used + available` overflow saturates the basis
    /// at `u64::MAX` (never wraps to a tiny basis), then the pct+clamp bound it.
    #[test]
    fn disk_budget_for_saturating_overflow() {
        // used + avail overflows u64 → basis saturates at u64::MAX, budget
        // clamps to the MAX cap rather than wrapping to something small.
        assert_eq!(
            disk_budget_for(u64::MAX, u64::MAX, 0.5),
            DEFAULT_MAX_HOSTING_DISK_BYTES
        );
    }

    /// Explicit clamp bounds (the operator `--max-hosting-disk` override) win,
    /// and an inconsistent max<min still yields the usable MIN floor (never 0).
    #[test]
    fn disk_budget_for_clamped_honors_explicit_bounds() {
        // Custom max below the computed value clamps down to it.
        assert_eq!(
            disk_budget_for_clamped(4 * GIB, 4 * GIB, 0.5, MIN_DEFAULT_HOSTING_BUDGET_BYTES, GIB),
            GIB
        );
        // max < min → MIN floor wins (no panic, no zero cap).
        assert_eq!(
            disk_budget_for_clamped(100 * GIB, 0, 0.5, 512 * MIB, 128 * MIB),
            512 * MIB
        );
    }

    /// The real resolved default always lands within the documented clamp,
    /// whatever RAM the test host has.
    #[test]
    fn default_hosting_budget_within_clamp() {
        let b = default_hosting_budget_bytes();
        assert!(
            (MIN_DEFAULT_HOSTING_BUDGET_BYTES..=MAX_DEFAULT_HOSTING_BUDGET_BYTES).contains(&b),
            "default {b} must be within [{}, {}]",
            MIN_DEFAULT_HOSTING_BUDGET_BYTES,
            MAX_DEFAULT_HOSTING_BUDGET_BYTES,
        );
    }

    /// The budget-triggered eviction counter increments once per over-budget
    /// eviction and is surfaced via `stats()`; a no-pressure (under-budget) cache
    /// leaves it untouched. This is the telemetry A2 ships to observe its own
    /// eviction rate. (There is no `min_ttl` floor since 2026-07-08, so an over-
    /// budget insert evicts — and counts — immediately.)
    #[test]
    fn test_budget_eviction_counter_tracks_over_budget_evictions() {
        let (mut cache, _time) = make_cache(200);
        let key1 = make_key(1);
        let key2 = make_key(2);
        let key3 = make_key(3);
        let key4 = make_key(4);

        // Under budget: no eviction, counter untouched.
        cache.record_access(key1, 100, AccessType::Get, 0, |_| (0, 0));
        cache.record_access(key2, 100, AccessType::Get, 0, |_| (0, 0));
        assert_eq!(cache.stats().budget_evictions_total, 0);

        // Over budget: the insert immediately sheds the oldest (key1) → counter 1.
        let result = cache.record_access(key3, 100, AccessType::Get, 0, |_| (0, 0));
        assert_eq!(result.evicted, vec![(key1, 0)]);
        assert_eq!(
            cache.stats().budget_evictions_total,
            1,
            "an over-budget insert counts one budget eviction (no min_ttl grace)"
        );

        // A further over-budget insert sheds key2 → counter 2.
        let result = cache.record_access(key4, 100, AccessType::Get, 0, |_| (0, 0));
        assert_eq!(result.evicted, vec![(key2, 0)]);
        let stats = cache.stats();
        assert_eq!(stats.budget_evictions_total, 2);
        assert_eq!(stats.current_bytes, 200);
        assert_eq!(stats.contract_count, 2);
        assert_eq!(stats.budget_bytes, 200);

        // No pressure now (at budget): the counter is unchanged.
        let evicted = cache.sweep_expired(|_| (0, 0), MemoryPressure::AtCapacity);
        assert!(evicted.is_empty(), "at budget -> no further eviction");
        assert_eq!(
            cache.stats().budget_evictions_total,
            2,
            "counter must not advance with no budget pressure"
        );
    }

    /// The unread-seed eviction counter (PUT-durability falsifier) increments
    /// only when a `read_count == 0` victim (a PUT seed never GET/SUBSCRIBEd) is
    /// evicted over budget, and records its age-at-eviction. A victim that was
    /// read even once must NOT count as an unread seed.
    #[test]
    fn test_evicted_unread_counts_only_never_read_seeds() {
        // Budget 100 bytes: only one 100-byte entry fits, so each insert past
        // TTL evicts the resident one.
        let (mut cache, time) = make_cache(100);
        let key1 = make_key(1);
        let key2 = make_key(2);
        let key3 = make_key(3);

        // Seed key1 via PUT (read_count == 0 — an unread seed).
        cache.record_access(key1, 100, AccessType::Put, 0, |_| (0, 0));
        assert_eq!(cache.stats().evicted_unread_total, 0);

        // Past TTL, insert key2 (also a seed). key1 is evicted: never read, so
        // it counts as an unread-seed eviction and its age (~61s) is recorded.
        time.advance_time(Duration::from_secs(61));
        let result = cache.record_access(key2, 100, AccessType::Put, 0, |_| (0, 0));
        assert_eq!(result.evicted, vec![(key1, 0)]);
        let stats = cache.stats();
        assert_eq!(stats.budget_evictions_total, 1);
        assert_eq!(stats.evicted_unread_total, 1, "an unread seed was evicted");
        assert_eq!(
            stats.evicted_unread_age_secs_sum, 61,
            "age-at-eviction of the unread seed is recorded"
        );

        // Read key2 once (GET) so read_count == 1 — no longer an unread seed.
        cache.record_access(key2, 100, AccessType::Get, 0, |_| (0, 0));

        // Past TTL, insert key3 to evict key2. key2 was read once, so it is
        // neither an unread seed nor a recently-read (>=2) victim: the unread
        // counter and age sum both hold.
        time.advance_time(Duration::from_secs(61));
        let result = cache.record_access(key3, 100, AccessType::Get, 0, |_| (0, 0));
        assert_eq!(result.evicted, vec![(key2, 0)]);
        let stats = cache.stats();
        assert_eq!(stats.budget_evictions_total, 2);
        assert_eq!(
            stats.evicted_unread_total, 1,
            "a victim read at least once must NOT count as an unread seed"
        );
        assert_eq!(
            stats.evicted_unread_age_secs_sum, 61,
            "age sum unchanged when the eviction is not an unread seed"
        );
    }

    /// A reloaded (persisted) entry with `read_count == 0` must NOT count as an
    /// unread seed when evicted. Restart pollution regression: a restart resets
    /// `read_count` to 0 and `inserted_at` to `now` for the WHOLE persisted set,
    /// then the post-load over-budget burst sheds the oldest reloaded entries —
    /// which, without the `seeded_this_run` gate, would fake an "unread seeds die
    /// instantly" spike (age ≈ 0) on every restart, a false positive for the very
    /// alarm this counter raises. Restarts are frequent under auto-update.
    #[test]
    fn test_reloaded_entry_eviction_does_not_count_as_unread_seed() {
        // Budget 100 bytes: one 100-byte entry fits.
        let (mut cache, _time) = make_cache(100);
        let key1 = make_key(1);
        let key2 = make_key(2);

        // Reload key1 from persistence with read_count 0 — the exact shape of a
        // long-hosted entry after a restart resets its counters. (A PUT was its
        // last pre-restart access, so access_type == Put, proving the counter
        // cannot key on access_type instead of the explicit marker.) A reloaded
        // entry has recency_seq 0, so it is the first eviction candidate.
        cache.load_persisted_entry(key1, 100, AccessType::Put, Duration::from_secs(1), false);
        cache.finalize_loading();
        assert!(cache.contains(&key1));
        assert_eq!(cache.stats().evicted_unread_total, 0);

        // A fresh insert (key2, the op-protected key) pushes over budget and sheds
        // the reloaded key1 (the post-load burst) — no `min_ttl` wait needed.
        let result = cache.record_access(key2, 100, AccessType::Get, 0, |_| (0, 0));
        assert_eq!(
            result.evicted,
            vec![(key1, 0)],
            "the reloaded entry is evicted in the post-load over-budget burst"
        );
        let stats = cache.stats();
        assert_eq!(stats.budget_evictions_total, 1, "it WAS evicted");
        assert_eq!(
            stats.evicted_unread_total, 0,
            "a reloaded entry (not seeded this run) must NOT count as an unread seed"
        );
        assert_eq!(
            stats.evicted_unread_age_secs_sum, 0,
            "no age is recorded for a reloaded (non-unread-seed) eviction"
        );
    }

    /// Restart must NOT fabricate real-GET recency (#4642 review fix): a reloaded
    /// entry keeps `recency_seq == 0` regardless of its persisted access type,
    /// because persistence records only a generic last-ACCESS time and cannot
    /// distinguish a real GET from a PUT/SUBSCRIBE. Stamping `recency_seq` from
    /// last-access order would let a never-GET-read PUT seed out-survive an entry
    /// with genuine GET demand after a restart.
    #[test]
    fn reloaded_entry_has_zero_get_recency_regardless_of_access_type() {
        let (mut cache, _) = make_cache(10_000);
        let put_seed = make_key(1);
        let subscribed = make_key(2);

        // Both reloaded from persistence with different pre-restart access types
        // and different persisted recency (so finalize_loading's last_access_seq
        // ordering differs) — neither may fabricate a nonzero recency_seq.
        cache.load_persisted_entry(
            put_seed,
            100,
            AccessType::Put,
            Duration::from_secs(30),
            false,
        );
        cache.load_persisted_entry(
            subscribed,
            100,
            AccessType::Subscribe,
            Duration::from_secs(10),
            false,
        );
        cache.finalize_loading();

        assert_eq!(
            cache.get(&put_seed).unwrap().recency_seq,
            0,
            "a reloaded PUT entry must have recency_seq == 0 (never GET-read since restart)"
        );
        assert_eq!(
            cache.get(&subscribed).unwrap().recency_seq,
            0,
            "a reloaded SUBSCRIBE entry must also have recency_seq == 0"
        );

        // A real GET after restart re-stamps it to a genuine (nonzero) value,
        // lifting it out of the never-GET-read front of the eviction order.
        cache.record_access(put_seed, 100, AccessType::Get, 0, |_| (0, 0));
        assert!(
            cache.get(&put_seed).unwrap().recency_seq > 0,
            "a real GET after restart must re-stamp recency_seq to a real value"
        );
    }

    // --- Demand-ordered (Greedy-Dual) eviction (A3) ---

    /// `keep_score` is `eviction_floor + predicted_demand` on insert, and a read
    /// refreshes it to the CURRENT floor + demand. Since the floor only rises,
    /// a repeatedly-read contract's keep_score climbs while an untouched one's
    /// stays put.
    #[test]
    fn fuel_gauge_keep_score_set_on_insert_and_refreshed_on_read() {
        let (mut cache, _) = make_cache(10_000);
        let key = make_key(1);

        // Insert at floor 0 with demand 3.0 -> keep_score 3.0.
        cache.record_access_with_demand(key, 100, AccessType::Get, 0, 3.0, |_| (0, 0));
        assert_eq!(cache.get(&key).unwrap().keep_score, 3.0);
        assert_eq!(cache.get(&key).unwrap().predicted_demand, 3.0);
        assert_eq!(cache.get(&key).unwrap().read_count, 1);

        // A read with a new demand estimate refreshes keep_score = floor + demand.
        cache.record_access_with_demand(key, 100, AccessType::Get, 0, 5.0, |_| (0, 0));
        assert_eq!(cache.get(&key).unwrap().keep_score, 5.0);
        assert_eq!(cache.get(&key).unwrap().read_count, 2);
    }

    /// Pure demand-ordering: a higher-demand contract survives even when a
    /// lower-demand one was inserted MORE recently — demand beats recency.
    // Superseded by subscriber-primary eviction rework (#4642, PR-1): eviction
    // now orders by (subscriber_count, real-GET recency); demand/distance/
    // abandonment no longer drive eviction order. Retained as historical
    // documentation of the demoted demand machinery.
    #[ignore]
    #[test]
    fn fuel_gauge_evicts_lowest_demand_not_lru() {
        let (mut cache, time) = make_cache(200);
        let high = make_key(1);
        let low = make_key(2);
        let trigger = make_key(3);

        // `high` inserted first (older) with strong demand; `low` inserted after
        // with weak demand. A recency-only (LRU) policy would evict `high`.
        cache.record_access_with_demand(high, 100, AccessType::Get, 0, 10.0, |_| (0, 0));
        cache.record_access_with_demand(low, 100, AccessType::Get, 0, 1.0, |_| (0, 0));
        assert_eq!(cache.current_bytes(), 200);

        time.advance_time(Duration::from_secs(61));

        // Over-budget insert must evict the LOWEST keep_score (`low`), not the
        // oldest (`high`).
        let result =
            cache.record_access_with_demand(trigger, 100, AccessType::Get, 0, 1.0, |_| (0, 0));
        assert_eq!(
            result.evicted,
            vec![(low, 0)],
            "lowest-demand contract must evict, not the least-recently-used one"
        );
        assert!(cache.contains(&high), "high-demand contract must survive");
        assert!(!cache.contains(&low));
    }

    /// Cold-start ordering: with ZERO observed samples, two contracts at
    /// different ring distances are separated purely by the analytic distance
    /// prior, so under budget pressure the NEAR-key contract out-survives the
    /// FAR-key one. This pins the intended cold-start eviction ordering that the
    /// distance prior exists to produce (the restart-drift / under-retention fix)
    /// AND documents the accepted demand-blind tradeoff behaviorally: because
    /// neither contract has any per-contract read evidence yet (A4 is deferred),
    /// a FAR-but-GET-hot contract could likewise be out-scored by an unread NEAR
    /// one once past `min_ttl` — bounded by `min_ttl`, resolved when A4 lands.
    // Superseded by subscriber-primary eviction rework (#4642, PR-1): eviction
    // now orders by (subscriber_count, real-GET recency); demand/distance/
    // abandonment no longer drive eviction order. Retained as historical
    // documentation of the demoted demand machinery.
    #[ignore]
    #[test]
    fn fuel_gauge_cold_start_keeps_near_key_over_far_key() {
        // The demand values come straight from the cold estimator (no samples),
        // so this exercises the real distance prior rather than hand-picked
        // magic numbers.
        let prior = super::super::demand::ProximityPrior::new();
        assert_eq!(prior.len(), 0, "cold estimator has no samples");
        let near_demand = prior.predict(0.02); // near this peer's key
        let far_demand = prior.predict(0.45); // far from the key
        assert!(
            near_demand > far_demand,
            "cold distance prior must slope down: near={near_demand}, far={far_demand}"
        );

        // Budget for exactly two 100-byte contracts.
        let (mut cache, time) = make_cache(200);
        let near = make_key(1);
        let far = make_key(2);
        let trigger = make_key(3);

        // Insert both cold (zero samples), scored only by the distance prior.
        cache.record_access_with_demand(near, 100, AccessType::Get, 0, near_demand, |_| (0, 0));
        cache.record_access_with_demand(far, 100, AccessType::Get, 0, far_demand, |_| (0, 0));
        assert_eq!(cache.current_bytes(), 200);
        assert!(
            cache.get(&near).unwrap().keep_score > cache.get(&far).unwrap().keep_score,
            "near-key contract must carry more cold demand credit than far-key"
        );

        // Age both past min_ttl so they are eviction-eligible; the trigger is
        // age-0 and cannot be evicted by its own insert.
        time.advance_time(Duration::from_secs(61));

        let result = cache.record_access_with_demand(
            trigger,
            100,
            AccessType::Get,
            0,
            NEUTRAL_DEMAND,
            |_| (0, 0),
        );
        assert_eq!(
            result.evicted,
            vec![(far, 0)],
            "cold start must evict the FAR-key contract (lower distance prior) first"
        );
        assert!(
            cache.contains(&near),
            "the NEAR-key contract out-survives the FAR-key one at cold start"
        );
        assert!(!cache.contains(&far));
    }

    /// The #4338 check at the cache level, written to actually DISCRIMINATE
    /// demand-ordering from recency: a repeatedly-read HIGH-demand contract (a
    /// River room) survives eviction against a NEWER low-demand junk contract
    /// even though the room is past TTL and is the *least-recently-accessed* of
    /// the two. Byte-LRU-with-TTL evicts the oldest past-TTL entry, so it would
    /// evict the room here — this test fails under that policy and passes only
    /// because eviction is ordered by `keep_score` (demand), not recency.
    ///
    /// Note the discrimination comes from NON-UNIFORM demand, not the floor
    /// alone: with uniform demand `keep_score = eviction_floor_at_last_refresh +
    /// constant` is monotone in refresh time (the floor only rises), i.e.
    /// identical to LRU ordering. The Greedy-Dual floor supplies aging, but the
    /// demand term is what lets an older contract outrank a newer one.
    // Superseded by subscriber-primary eviction rework (#4642, PR-1): eviction
    // now orders by (subscriber_count, real-GET recency); demand/distance/
    // abandonment no longer drive eviction order. Retained as historical
    // documentation of the demoted demand machinery.
    //
    // Specifically, the #4338 "a repeatedly-read contract survives junk" guard
    // is INTENTIONALLY removed, not accidentally dropped (Ian, 2026-07):
    // frequency protection for UNSUBSCRIBED contracts is gone — the tiebreak is
    // RECENCY-ONLY (time since the last real GET), with no per-contract
    // read-frequency signal. In the demand-driven model, real demand is
    // expressed by SUBSCRIBING, which pins the contract; a "heavily-read but
    // never-subscribed" contract is transient-by-design cruft, so it is not
    // protected against eviction by re-reads alone. See
    // docs/design/demand-driven-hosting.md §9 (Piece A).
    #[ignore]
    #[test]
    fn fuel_gauge_keeps_repeatedly_read_contract_over_junk() {
        // Budget for two 100-byte contracts.
        let (mut cache, time) = make_cache(200);
        let room = make_key(1); // the "River room": repeatedly read, high demand
        let junk = make_key(2); // never re-read, low demand, inserted LATER
        let trigger = make_key(3);

        // Seed the room with strong demand, then read it repeatedly (simulating
        // recurring GETs). These reads are the room's LAST accesses.
        cache.record_access_with_demand(room, 100, AccessType::Get, 0, 10.0, |_| (0, 0));
        for _ in 0..3 {
            time.advance_time(Duration::from_secs(5));
            cache.touch(&room);
        }

        // Junk arrives AFTER the room's last read, so junk is the more-recently-
        // accessed entry — and carries weak demand.
        time.advance_time(Duration::from_secs(5));
        cache.record_access_with_demand(junk, 100, AccessType::Get, 0, 1.0, |_| (0, 0));

        // Advance so BOTH are past TTL and eviction-eligible; the room, last read
        // 5s before junk, is the least-recently-accessed of the two.
        time.advance_time(Duration::from_secs(61));

        // Make the discrimination explicit and checked, not just asserted in
        // prose: the room is LRU-older yet carries more demand credit.
        let room_entry = cache.get(&room).expect("room hosted");
        let junk_entry = cache.get(&junk).expect("junk hosted");
        assert!(
            room_entry.last_access_seq < junk_entry.last_access_seq,
            "room must be the least-recently-accessed of the two, so recency \
             alone (byte-LRU) would evict it first"
        );
        assert!(
            room_entry.keep_score > junk_entry.keep_score,
            "room must carry more demand credit than junk ({} vs {})",
            room_entry.keep_score,
            junk_entry.keep_score,
        );

        // Over-budget insert: the fuel gauge evicts the lowest keep_score (junk).
        // Byte-LRU-with-TTL would instead evict the room (the oldest past-TTL
        // entry) — so this assertion is what fails under a recency-only policy.
        let result =
            cache.record_access_with_demand(trigger, 100, AccessType::Get, 0, 1.0, |_| (0, 0));
        assert_eq!(
            result.evicted,
            vec![(junk, 0)],
            "low-demand junk must evict, NOT the older-but-high-demand room"
        );
        assert!(
            cache.contains(&room),
            "the repeatedly-read high-demand room survives even though it is the \
             least-recently-accessed past-TTL entry (byte-LRU would have evicted it)"
        );
        assert!(!cache.contains(&junk));
        // The room (read_count >= 2) was never a victim, so no repeat-demand
        // eviction is recorded (junk, read once, does not count).
        assert_eq!(
            cache.stats().evictions_of_recently_read_total,
            0,
            "a repeatedly-read contract must never be evicted (no miscalibration)"
        );
    }

    /// The `eviction_floor` ratchets up to each evicted contract's `keep_score`
    /// and never decreases (Greedy-Dual aging).
    #[test]
    fn eviction_floor_ratchets_up_on_eviction() {
        let (mut cache, time) = make_cache(100);
        assert_eq!(cache.eviction_floor(), 0.0);

        // Insert a demand-7 contract, let it age, then evict it via an
        // over-budget insert; the floor must climb to 7.
        let a = make_key(1);
        cache.record_access_with_demand(a, 100, AccessType::Get, 0, 7.0, |_| (0, 0));
        time.advance_time(Duration::from_secs(61));
        let b = make_key(2);
        let r = cache.record_access_with_demand(b, 100, AccessType::Get, 0, 2.0, |_| (0, 0));
        assert_eq!(r.evicted, vec![(a, 0)]);
        assert_eq!(
            cache.eviction_floor(),
            7.0,
            "floor ratchets to evicted score"
        );

        // b's keep_score was floor(0)+2 = 2 at insert; a later eviction of a
        // lower-scored victim must not drop the floor below 7.
        time.advance_time(Duration::from_secs(61));
        let c = make_key(3);
        let r = cache.record_access_with_demand(c, 100, AccessType::Get, 0, 1.0, |_| (0, 0));
        assert_eq!(r.evicted, vec![(b, 0)], "b (keep_score 2) evicts");
        assert_eq!(
            cache.eviction_floor(),
            7.0,
            "floor must not drop below its high-water mark"
        );
    }

    /// A PUT is a SEED, not read-demand: on an existing entry it must NOT bump
    /// `read_count` or refresh `keep_score` to the frontier, so a repeatedly-PUT
    /// contract cannot outrank a repeatedly-GET one.
    #[test]
    fn put_seeds_but_earns_no_read_demand_credit() {
        let (mut cache, _) = make_cache(10_000);
        let key = make_key(1);

        // Seed via PUT: keep_score = floor(0) + demand, read_count 0.
        cache.record_access_with_demand(key, 100, AccessType::Put, 0, 2.0, |_| (0, 0));
        assert_eq!(cache.get(&key).unwrap().read_count, 0, "PUT is not a read");
        let seed_score = cache.get(&key).unwrap().keep_score;
        assert_eq!(seed_score, 2.0);

        // Manually raise the floor by evicting something else would be indirect;
        // instead assert that a re-PUT does NOT refresh keep_score or read_count.
        cache.record_access_with_demand(key, 100, AccessType::Put, 0, 9.0, |_| (0, 0));
        assert_eq!(
            cache.get(&key).unwrap().read_count,
            0,
            "re-PUT must not count as a read"
        );
        assert_eq!(
            cache.get(&key).unwrap().keep_score,
            seed_score,
            "re-PUT (seed) must not refresh keep_score to the frontier"
        );

        // A GET, by contrast, IS read-demand and refreshes both.
        cache.record_access_with_demand(key, 100, AccessType::Get, 0, 9.0, |_| (0, 0));
        assert_eq!(cache.get(&key).unwrap().read_count, 1);
        assert_eq!(cache.get(&key).unwrap().keep_score, 9.0);
    }

    /// The recently-read eviction counter fires only for victims with genuine
    /// repeat demand (`read_count >= 2`), not one-off seeds.
    #[test]
    fn evictions_of_recently_read_counts_only_repeat_demand() {
        let (mut cache, time) = make_cache(100);

        // A contract read twice (repeat demand), then forced out by junk.
        let read_twice = make_key(1);
        cache.record_access(read_twice, 100, AccessType::Get, 0, |_| (0, 0));
        cache.touch(&read_twice); // second read -> read_count 2
        time.advance_time(Duration::from_secs(61));
        // Evict it by inserting a higher-demand junk (so read_twice is the victim).
        let junk = make_key(2);
        let r = cache.record_access_with_demand(junk, 100, AccessType::Get, 0, 5.0, |_| (0, 0));
        assert_eq!(r.evicted, vec![(read_twice, 0)]);
        assert_eq!(
            cache.stats().evictions_of_recently_read_total,
            1,
            "evicting a twice-read contract is a miscalibration signal"
        );

        // Now evict the junk seed (read_count 1) -> must NOT increment.
        time.advance_time(Duration::from_secs(61));
        let seed = make_key(3);
        // junk currently has demand 5 (>seed's), so make the new one higher.
        let r = cache.record_access_with_demand(seed, 100, AccessType::Get, 0, 9.0, |_| (0, 0));
        assert_eq!(r.evicted, vec![(junk, 0)], "junk (read once) evicts");
        assert_eq!(
            cache.stats().evictions_of_recently_read_total,
            1,
            "a one-off seed eviction must not count as recently-read"
        );
    }

    /// A subscribed contract is ordered LAST, so when only ONE eviction is needed
    /// to get back under budget a zero-subscriber contract is shed and the
    /// subscribed one survives — even though its `keep_score` is the lowest and it
    /// is past TTL. (Subscriber-primary ordering dominates demand ordering; the
    /// subscribed contract is NOT hard-pinned — see
    /// `evict_all_subscribed_sheds_fewest_as_last_resort` for the last-resort shed.)
    #[test]
    fn subscribed_contract_ordered_last_survives_while_zero_sub_evicted() {
        let (mut cache, time) = make_cache(200);
        let subscribed = make_key(1); // lowest demand, but subscribed
        let other = make_key(2);
        let trigger = make_key(3);

        cache.record_access_with_demand(subscribed, 100, AccessType::Get, 0, 0.1, |_| (0, 0));
        cache.record_access_with_demand(other, 100, AccessType::Get, 0, 5.0, |_| (0, 0));
        time.advance_time(Duration::from_secs(61));

        // Over budget by one entry: `subscribed` has the lowest keep_score and a
        // naive demand order would evict it first, but it is downstream-subscribed
        // so it sorts LAST — the zero-subscriber `other` is shed instead.
        let result = cache.record_access_with_demand(trigger, 100, AccessType::Get, 0, 5.0, |k| {
            (0, (*k == subscribed) as usize)
        });
        assert_eq!(
            result.evicted,
            vec![(other, 0)],
            "the subscribed contract is ordered last; the zero-subscriber one sheds"
        );
        assert_eq!(
            result.evicted_in_use,
            Vec::<ContractKey>::new(),
            "no in-use contract was shed here, so nothing needs subscription teardown"
        );
        assert!(cache.contains(&subscribed));
        assert!(!cache.contains(&other));
    }

    // --- Subscriber-primary eviction (#4642, PR-1) ---

    /// Under normal (AtCapacity) over-budget pressure the zero-subscriber contract
    /// is shed before the subscribed one (subscriber-primary ordering). The
    /// subscribed contract is ordered LAST, not hard-pinned.
    #[test]
    fn evict_normal_drops_zero_sub_before_subscribed() {
        let (mut cache, time) = make_cache(200);
        let a = make_key(1);
        let b = make_key(2);
        let c = make_key(3);

        // A is subscribed (downstream); B is zero-subscriber.
        let subs = |k: &ContractKey| (0, usize::from(*k == a));
        cache.record_access(a, 100, AccessType::Get, 0, subs);
        cache.record_access(b, 100, AccessType::Get, 0, subs);
        assert_eq!(cache.current_bytes(), 200);

        // Past TTL, insert C over budget. Both A and B are eligible (past TTL),
        // but B (zero-subscriber) sorts ahead of A (subscribed), so B is the
        // victim and one eviction is enough — A survives, ordered last.
        time.advance_time(Duration::from_secs(61));
        let result = cache.record_access(c, 100, AccessType::Get, 0, subs);
        assert_eq!(
            result.evicted,
            vec![(b, 0)],
            "the zero-subscriber contract sheds before the subscribed one"
        );
        assert!(cache.contains(&a), "subscribed A survives (ordered last)");
        assert!(cache.contains(&c));
        assert!(!cache.contains(&b));
    }

    /// A real GET OR PUT (genuine client access) refreshes the eviction recency
    /// key (invariant 3, decision 2026-07-08); SUBSCRIBE / renewal does NOT. This
    /// discriminates both facts at once: `a` is re-PUT (must float to newest, so
    /// it survives) while `b` is only re-SUBSCRIBEd (must stay stale, so it is the
    /// victim). Without the PUT-refresh, `a` (the oldest GET) would be the victim
    /// instead — so this fails under a GET-only recency rule.
    #[test]
    fn real_get_and_put_refresh_recency_subscribe_does_not() {
        let (mut cache, _time) = make_cache(300);
        let a = make_key(1);
        let b = make_key(2);
        let c = make_key(3);
        let d = make_key(4);

        // Recency order after the initial GETs: A < B < C (A oldest).
        cache.record_access(a, 100, AccessType::Get, 0, |_| (0, 0));
        cache.record_access(b, 100, AccessType::Get, 0, |_| (0, 0));
        cache.record_access(c, 100, AccessType::Get, 0, |_| (0, 0));

        // A PUT on A refreshes A's recency (genuine client access) → A becomes
        // NEWEST. A SUBSCRIBE on B must NOT refresh B → B stays at its GET recency,
        // so B is now the least-recently-accessed entry.
        cache.record_access(a, 100, AccessType::Put, 0, |_| (0, 0));
        cache.record_access(b, 100, AccessType::Subscribe, 0, |_| (0, 0));

        let result = cache.record_access(d, 100, AccessType::Get, 0, |_| (0, 0));
        assert_eq!(
            result.evicted,
            vec![(b, 0)],
            "B is evicted: SUBSCRIBE did not refresh recency, but A's PUT did (A survives)"
        );
        assert!(cache.contains(&a), "A survives: its PUT refreshed recency");
        assert!(!cache.contains(&b));
        assert!(cache.contains(&c));
        assert!(cache.contains(&d));
    }

    /// A real GET refreshes the eviction recency key, floating the read entry to
    /// the newest position so a different (now least-recent) entry evicts.
    #[test]
    fn real_get_refreshes_recency() {
        let (mut cache, time) = make_cache(300);
        let a = make_key(1);
        let b = make_key(2);
        let c = make_key(3);
        let d = make_key(4);

        cache.record_access(a, 100, AccessType::Get, 0, |_| (0, 0));
        cache.record_access(b, 100, AccessType::Get, 0, |_| (0, 0));
        cache.record_access(c, 100, AccessType::Get, 0, |_| (0, 0));

        // A real GET on A makes A the newest, leaving B least-recently-GET.
        cache.record_access(a, 100, AccessType::Get, 0, |_| (0, 0));

        time.advance_time(Duration::from_secs(61));
        let result = cache.record_access(d, 100, AccessType::Get, 0, |_| (0, 0));
        assert_eq!(
            result.evicted,
            vec![(b, 0)],
            "B is evicted: A's real GET made A newest, leaving B least-recent"
        );
        assert!(cache.contains(&a));
        assert!(!cache.contains(&b));
        assert!(cache.contains(&c));
        assert!(cache.contains(&d));
    }

    /// The Overflow-pressure sweep sheds the FEWEST-subscriber contract first and
    /// bumps both the OOM-valve counter and the subscribed-eviction counter. (With
    /// `min_ttl` dropped 2026-07-08 there is no cold-start floor to pierce;
    /// Overflow's only remaining distinct power is piercing the op-scoped backstop,
    /// which a periodic sweep never sets — so here it behaves like AtCapacity
    /// except for the OOM-valve counter.)
    #[test]
    fn oom_valve_sheds_fewest_subscriber() {
        // Seed under a generous budget so the inserts don't auto-evict, then
        // tighten so both are resident and over budget for the sweep.
        let (mut cache, _time) = make_cache(100_000);
        let a = make_key(1);
        let b = make_key(2);

        // A has more downstream subscribers than B; both are subscribed.
        let subs = |k: &ContractKey| if *k == a { (0, 2) } else { (0, 1) };
        cache.record_access(a, 100, AccessType::Get, 0, subs);
        cache.record_access(b, 100, AccessType::Get, 0, subs);
        cache.set_budget_for_test(100);
        assert!(cache.contains(&a));
        assert!(cache.contains(&b));
        assert_eq!(cache.current_bytes(), 200, "200 bytes over the 100 budget");

        // The Overflow sweep sheds the FEWEST-subscriber contract (B, 1 sub) before
        // A (2 subs), bumping both counters.
        let evicted = cache.sweep_expired(subs, MemoryPressure::Overflow);
        assert_eq!(
            evicted_pairs(&evicted),
            vec![(b, 0)],
            "fewest-subscriber contract sheds first"
        );
        assert!(
            evicted.iter().all(|e| e.was_in_use),
            "the shed contract was subscribed, so it is flagged in-use for teardown"
        );
        assert!(cache.contains(&a), "the more-subscribed contract survives");
        assert!(!cache.contains(&b));
        assert_eq!(cache.stats().oom_valve_evictions_total, 1);
        assert_eq!(
            cache.stats().subscribed_evictions_total,
            1,
            "shedding a subscribed contract bumps the subscribed-eviction falsifier"
        );
    }

    /// With equal subscriber counts, the Overflow sweep tiebreaks by least-recent
    /// real GET/PUT.
    #[test]
    fn oom_valve_equal_subscribers_sheds_least_recent_get() {
        // Seed under a generous budget (distinct recency: A older than B), then
        // tighten so both are resident and over budget for the sweep.
        let (mut cache, _time) = make_cache(100_000);
        let a = make_key(1);
        let b = make_key(2);

        cache.record_access(a, 100, AccessType::Get, 0, |_| (0, 1)); // recency: A older
        cache.record_access(b, 100, AccessType::Get, 0, |_| (0, 1)); // recency: B newer
        cache.set_budget_for_test(100);
        assert_eq!(cache.current_bytes(), 200);

        let evicted = cache.sweep_expired(|_| (0, 1), MemoryPressure::Overflow);
        assert_eq!(
            evicted_pairs(&evicted),
            vec![(a, 0)],
            "with equal subscribers, the least-recently-accessed contract (A) sheds"
        );
        assert!(!cache.contains(&a));
        assert!(cache.contains(&b));
        assert_eq!(cache.stats().oom_valve_evictions_total, 1);
    }

    /// A normal AtCapacity eviction of a zero-subscriber contract must NOT touch
    /// EITHER the OOM-valve counter (Overflow-pressure only) or the
    /// subscribed-eviction counter (in-use victims only).
    #[test]
    fn oom_valve_counter_zero_under_normal_atcapacity() {
        let (mut cache, _time) = make_cache(100);
        let a = make_key(1);
        let b = make_key(2);

        cache.record_access(a, 100, AccessType::Get, 0, |_| (0, 0));
        // Inserting B (the op-protected key) over budget evicts the older, zero-
        // subscriber A under AtCapacity — no `min_ttl` wait needed.
        let result = cache.record_access(b, 100, AccessType::Get, 0, |_| (0, 0));
        assert_eq!(result.evicted, vec![(a, 0)]);
        assert!(
            result.evicted_in_use.is_empty(),
            "a zero-subscriber eviction needs no subscription teardown"
        );
        assert_eq!(
            cache.stats().oom_valve_evictions_total,
            0,
            "a normal zero-subscriber eviction must not touch the OOM-valve counter"
        );
        assert_eq!(
            cache.stats().subscribed_evictions_total,
            0,
            "a zero-subscriber eviction must not touch the subscribed-eviction counter"
        );
    }

    /// A fresh PUT resets `recency_seq` (invariant 3, decision 2026-07-08), so it
    /// is protected by being the most-recently-accessed — NO `min_ttl` floor
    /// needed. It out-survives an OLDER contract and is never evicted by its own
    /// insert (the op-scoped backstop). This is what `min_ttl`'s cold-start grace
    /// used to provide, now delivered by recency instead.
    #[test]
    fn fresh_put_protected_by_recency_not_min_ttl() {
        let (mut cache, _time) = make_cache(100);
        let old = make_key(1);
        let seed = make_key(2);

        // An older GET, then a fresh zero-subscriber PUT seed. Budget fits one.
        cache.record_access(old, 100, AccessType::Get, 0, |_| (0, 0)); // recency: older
        let result = cache.record_access(seed, 100, AccessType::Put, 0, |_| (0, 0));

        // The fresh PUT seed (newest recency + op-protected) survives; the OLDER
        // contract is shed. Under a GET-only recency rule the seed would have
        // recency 0 and be the victim instead — so this pins the PUT-refresh.
        assert_eq!(
            result.evicted,
            vec![(old, 0)],
            "the fresh PUT out-survives the older contract by recency"
        );
        assert!(
            cache.contains(&seed),
            "a fresh PUT is protected by its recency + the op-scoped backstop"
        );
        assert!(!cache.contains(&old));
    }

    /// LAST RESORT (Ian's confirmed ordering, #4642): when EVERY eligible contract
    /// is subscribed and the peer is still over budget, the fewest-`(local,
    /// downstream)`-subscriber one IS shed under normal AtCapacity pressure (no
    /// hard pin), and is flagged `was_in_use` so the manager tears down its
    /// subscription state. Also checks fewest-LOCAL-first: a downstream-only
    /// contract is shed before a local-client one.
    #[test]
    fn evict_all_subscribed_sheds_fewest_as_last_resort() {
        let (mut cache, time) = make_cache(200);
        let local_sub = make_key(1); // (local=1, downstream=0) — evicted LAST
        let downstream_sub = make_key(2); // (local=0, downstream=5) — evicted first
        let trigger = make_key(3);

        // Every contract is subscribed; NONE is zero-subscriber.
        let subs = |k: &ContractKey| {
            if *k == local_sub {
                (1, 0)
            } else if *k == downstream_sub {
                (0, 5)
            } else {
                (0, 1) // trigger: one downstream subscriber
            }
        };
        cache.record_access(local_sub, 100, AccessType::Get, 0, subs);
        cache.record_access(downstream_sub, 100, AccessType::Get, 0, subs);
        assert_eq!(cache.current_bytes(), 200);

        // Age both past min_ttl so they are eligible, then insert `trigger` over
        // budget. Nothing is zero-subscriber, so the fewest-`(local, downstream)`
        // contract — `downstream_sub` (0, 5) sorts ahead of `local_sub` (1, 0) —
        // is shed as the last resort, NOT hard-pinned.
        time.advance_time(Duration::from_secs(61));
        let result = cache.record_access(trigger, 100, AccessType::Get, 0, subs);
        assert_eq!(
            result.evicted,
            vec![(downstream_sub, 0)],
            "with only subscribed contracts, the fewest-LOCAL (then fewest-\
             downstream) one is shed — the local-client contract is evicted last"
        );
        assert_eq!(
            result.evicted_in_use,
            vec![downstream_sub],
            "the shed subscribed contract is flagged in-use for teardown"
        );
        assert!(
            cache.contains(&local_sub),
            "the local-client contract survives (ordered last)"
        );
        assert!(!cache.contains(&downstream_sub));
        assert_eq!(
            cache.stats().subscribed_evictions_total,
            1,
            "shedding a subscribed contract bumps the falsifier under AtCapacity"
        );
        assert_eq!(
            cache.stats().oom_valve_evictions_total,
            0,
            "AtCapacity last-resort shed is NOT an OOM-valve eviction"
        );
    }
}
