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
//!    ascending `(subscriber_count, last_get_seq, key_bytes)` — the
//!    FEWEST-subscriber contract first, ties broken by least-recent **real GET
//!    read**, then a deterministic key-byte tiebreak. `subscriber_count` is
//!    supplied by the caller (`HostingManager`) via the same closure that used
//!    to be `should_retain`; it is the count of genuine demand (local client
//!    subscriptions + downstream subscribers) and is `0` exactly when the
//!    contract is NOT `contract_in_use`. In NORMAL over-budget operation
//!    ([`MemoryPressure::AtCapacity`]) any subscribed contract (count >= 1) is
//!    PINNED — only zero-subscriber contracts are evicted, so among the
//!    candidate set `subscriber_count` is always 0 and the order reduces to
//!    least-recent-GET. The subscriber-count gradation only bites at ADMISSION
//!    (a later PR) and under the OOM valve ([`MemoryPressure::Overflow`]), where
//!    the pin is pierced and the fewest-subscriber contract is shed to survive
//!    genuine RAM overflow. `min_ttl` is preserved as the cold-start grace floor
//!    for fresh zero-subscriber contracts.
//! 3. **Subscription renewal**: All hosted contracts get subscription renewal
//! 4. **Access type tracking**: Records how contract was accessed (GET/PUT/SUBSCRIBE)
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
//! real-GET recency (see principle 2). `predicted_demand` is still supplied by
//! the caller (`HostingManager`, which owns the proximity-prior estimator and
//! the peer's own ring location — see [`super::demand`]) purely so the
//! estimator keeps training and the dashboard column stays populated.

use freenet_stdlib::prelude::ContractKey;
use std::collections::HashMap;
use std::time::Duration;
use tokio::time::Instant;

use super::demand::NEUTRAL_DEMAND;
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
    /// Monotonic access sequence at the entry's most recent real GET — the
    /// eviction recency tiebreak (subscriber-primary rework, #4642). The cache
    /// orders zero-subscriber eviction candidates ascending by this.
    pub last_get_seq: u64,
    /// Whether this entry is past its `min_ttl` age gate — the TTL half of the
    /// `evict_over_budget` eviction filter (`age >= min_ttl && !should_retain`).
    /// This is the only half the cache can decide on its own; the
    /// `!should_retain` (in-use) half is folded in by the `HostingManager`
    /// layer via [`super::HostingManager::is_eviction_eligible`]. Used by the
    /// dashboard so the "next to evict" badge only marks a contract the
    /// over-budget sweep would actually consider — never one it would skip.
    pub past_min_ttl: bool,
}

/// Multiplier for TTL relative to subscription renewal interval.
/// Gives this many renewal attempts before eviction if renewals keep failing.
pub const TTL_RENEWAL_MULTIPLIER: u32 = 4;

/// Default minimum TTL before a hosted contract can be evicted.
/// Computed as TTL_RENEWAL_MULTIPLIER × SUBSCRIPTION_RENEWAL_INTERVAL.
pub const DEFAULT_MIN_TTL: Duration = Duration::from_secs(
    super::SUBSCRIPTION_RENEWAL_INTERVAL.as_secs() * TTL_RENEWAL_MULTIPLIER as u64,
);

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

/// Coarse memory-pressure state handed to the over-budget eviction sweep — the
/// eviction half of invariant 3 ("admission and eviction are ONE demand-vs-
/// capacity decision"). Mirrors the shape of the admission core's
/// `MemoryPressure` (a later PR); kept minimal here because the eviction sweep
/// only runs once the byte budget is already exceeded, so it only needs to
/// distinguish "at capacity" from "genuine RAM overflow".
///
/// # The OOM-valve trigger is intentionally UNWIRED in production
///
/// [`Self::Overflow`] is the ONLY value that lets eviction pierce the in-use
/// pin (shed a subscribed contract). It is a genuine-RAM-overflow signal
/// (RSS near the ceiling), which is NOT plumbed yet: production callers
/// (`HostingManager::sweep_expired_hosting`, `record_contract_access`) pass
/// [`Self::AtCapacity`] unconditionally, so **nothing can shed a subscribed
/// contract in the field yet**. The valve MECHANISM lands + is unit-tested
/// here; wiring the real RSS/A1-resource trigger is a deliberately separate,
/// separately-validated step, because shedding a subscribed contract is the
/// single riskiest new behavior in the subscriber-primary rework (#4642).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MemoryPressure {
    /// Over the byte budget but not at genuine RAM-overflow risk. Subscribed
    /// contracts (subscriber_count >= 1) stay PINNED; only zero-subscriber
    /// contracts past `min_ttl` are evicted, by least-recent real GET. This is
    /// the value production always passes today (the Overflow trigger is
    /// unwired — see the type docs).
    AtCapacity,
    /// Genuine RAM overflow: the OOM valve may shed the FEWEST-subscriber
    /// contract, piercing the in-use pin AND bypassing `min_ttl`, to survive.
    /// NOTHING produces this value in production yet.
    ///
    /// `dead_code`-allowed in non-test builds ON PURPOSE: the OOM-valve trigger
    /// (a genuine RSS/A1 resource signal) is intentionally unwired for this PR,
    /// so only the valve-mechanism unit tests construct `Overflow`. Remove the
    /// allow when the trigger is plumbed in a later, separately-validated step.
    ///
    /// OWED when the trigger is wired (NOT done here — the mechanism is
    /// deliberately unwired this release):
    /// 1. **Rank the valve by the LEASE-FILTERED downstream count**
    ///    (`HostingManager::downstream_subscriber_peers`, which drops leases
    ///    older than `SUBSCRIPTION_LEASE_DURATION`), NOT the unfiltered
    ///    `subscriber_count` used for the normal-op pin. The unfiltered count
    ///    includes stale-unswept leases, which is fine for the pin (it must equal
    ///    `contract_in_use`) but would let the valve mis-rank "real demand" when
    ///    it is actually shedding under overflow.
    /// 2. **Clean up the shed contract's subscription state.** Valve-evicting a
    ///    subscribed contract MUST also remove its `client_subscriptions` /
    ///    `downstream_subscribers` entries, or `subscriber_count` immediately
    ///    reports it in-use again and the eviction is undone in practice
    ///    (the entry is gone from the cache but still pinned by phantom demand).
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
    /// Observed read-rate training sample (reads/second) for the accessed
    /// contract, or `None` when no meaningful rate is available (a seed/PUT, a
    /// brand-new entry, or a read with zero residency). `HostingManager` pairs
    /// this with the contract's ring distance and feeds it to the proximity
    /// prior (`super::demand::ProximityPrior`). Purely a training signal — it
    /// never affects THIS access's `keep_score`.
    pub observed_read_rate: Option<f64>,
}

/// Metadata about a hosted contract.
#[derive(Debug, Clone)]
pub struct HostedContract {
    /// Size of the contract state in bytes
    pub size_bytes: u64,
    /// Last time this contract was accessed (via GET/PUT/SUBSCRIBE). Drives the
    /// TTL gate (an entry is not eviction-eligible until `min_ttl` since this).
    pub last_accessed: Instant,
    /// Monotonic access sequence number stamped on every access (insert / read /
    /// seed / touch).
    ///
    /// UNUSED, pending deletion: it was the old eviction tiebreak, which the
    /// subscriber-primary rework (#4642) replaced with [`Self::last_get_seq`]
    /// (real-GET recency). No production reader remains (only a `#[cfg(test)]`
    /// assertion via the `get()` accessor keeps it from tripping dead-code). It
    /// is still maintained here to avoid churning the write sites in this PR;
    /// slated for removal with the rest of the demoted machinery once
    /// subscriber-primary is field-validated. Do NOT add new readers.
    pub last_access_seq: u64,
    /// Monotonic access sequence at this entry's most recent **real GET read**
    /// — the eviction recency tiebreak (subscriber-primary rework, #4642).
    ///
    /// Stamped (to the shared `access_seq` value) ONLY by an `AccessType::Get`
    /// access and by `touch_with_demand` (a local-cache GET serve). It is
    /// deliberately NOT bumped by PUT (a seed/write, not a read), by SUBSCRIBE
    /// (a subscription, not a read), by subscription RENEWAL traffic, or by a
    /// restart reload — so that "recently GET-read" means exactly that. Without
    /// this Get-only key, renewal traffic on every subscribed contract would
    /// keep them all equally "fresh" and the tiebreak would be useless.
    ///
    /// A never-GET-read entry (e.g. a fresh PUT seed) has `last_get_seq == 0`,
    /// so among zero-subscriber candidates it sorts to evict first once past
    /// `min_ttl` — the intended "unread seeds evaporate" behavior, with the
    /// `min_ttl` floor giving it cold-start grace to be found first.
    pub last_get_seq: u64,
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

/// Unified hosting cache that combines byte-budget LRU with TTL protection.
///
/// This cache maintains contracts that this peer is "hosting" - keeping available
/// for the network. The cache has:
/// - Byte budget: Large contracts consume more budget. The budget is measured
///   in tracked contract **state** bytes only; WASM code blobs and database
///   overhead are not counted against it.
/// - TTL protection: Contracts can't be evicted until min_ttl has passed
/// - Demand-ordered eviction (Greedy-Dual): when over budget the
///   contract with the lowest `keep_score` is evicted first (ties broken by
///   least-recently-read), NOT purely the oldest. See the module docs.
///
/// # Subscription Renewal
///
/// ALL contracts in this cache should have their subscriptions renewed automatically.
/// This is the key fix for the bug where GET-triggered subscriptions weren't being renewed.
pub struct HostingCache<T: TimeSource> {
    /// Maximum bytes to use for cached contracts
    budget_bytes: u64,
    /// Current total bytes used
    current_bytes: u64,
    /// Minimum time since last access before eviction is allowed
    min_ttl: Duration,
    /// Contract metadata indexed by key. Eviction order is derived from each
    /// entry's `keep_score` (then `last_accessed`), not from a separate LRU
    /// list, so there is no second structure to keep in sync.
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
    /// Time source for testability
    time_source: T,
}

impl<T: TimeSource> HostingCache<T> {
    /// Create a new hosting cache with the given byte budget and TTL.
    pub fn new(budget_bytes: u64, min_ttl: Duration, time_source: T) -> Self {
        Self {
            budget_bytes,
            current_bytes: 0,
            min_ttl,
            contracts: HashMap::new(),
            access_seq: 0,
            eviction_floor: 0.0,
            budget_evictions_total: 0,
            evictions_of_recently_read_total: 0,
            evicted_unread_total: 0,
            evicted_unread_age_secs_sum: 0,
            oom_valve_evictions_total: 0,
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
    /// **subscriber-primary**: ascending `(subscriber_count, last_get_seq,
    /// key_bytes)` — fewest subscribers first, then least-recent real GET, then
    /// a deterministic key-byte tiebreak (#4642 subscriber-primary rework).
    ///
    /// `subscriber_count(key)` is the caller's genuine-demand count (local
    /// client subscriptions + downstream subscribers); it is `0` exactly when
    /// the contract is NOT `contract_in_use`. Candidate eligibility depends on
    /// `pressure`:
    ///
    /// - [`MemoryPressure::AtCapacity`] (the production value today): an entry
    ///   is eligible iff `age >= min_ttl` AND `subscriber_count(key) == 0`.
    ///   Subscribed contracts are PINNED and never evicted; the `min_ttl` floor
    ///   gives fresh zero-subscriber contracts cold-start grace. Among the
    ///   eligible (all zero-subscriber) entries the order reduces to least-
    ///   recent real GET.
    /// - [`MemoryPressure::Overflow`] (the OOM valve — trigger UNWIRED in
    ///   production, see [`MemoryPressure`]): ALL entries are eligible; the pin
    ///   and `min_ttl` are BOTH pierced, and the fewest-subscriber (then least-
    ///   recent-GET) contract is shed to survive genuine RAM overflow. Shedding
    ///   a subscribed contract increments [`Self::oom_valve_evictions_total`].
    ///
    /// Eviction stops as soon as `current_bytes <= budget_bytes`. Returns the
    /// evicted `(key, write_generation)` pairs; the generation is captured
    /// atomically under the cache's write lock so the `EvictContract`
    /// deletion-time guard can detect a re-host that races with this eviction
    /// (see `RuntimePool::remove_contract`).
    ///
    /// do NOT revert to byte-only / recency-only LRU as the PRIMARY key — see
    /// hosting-invariants (byte-only eviction anti-pattern). Subscriber count is
    /// the demand-authorization signal: a subscribed contract is real demand and
    /// must be pinned in normal operation (only the OOM valve may pierce it).
    /// Real-GET recency is a SECONDARY tiebreak among equal-subscriber
    /// contracts, not the primary key.
    fn evict_over_budget<G>(
        &mut self,
        subscriber_count: &G,
        pressure: MemoryPressure,
    ) -> Vec<(ContractKey, u64)>
    where
        G: Fn(&ContractKey) -> usize,
    {
        if self.current_bytes <= self.budget_bytes {
            return Vec::new();
        }

        let now = self.time_source.now();

        // Collect eviction-eligible entries with their ordering keys, then evict
        // lowest-priority first until back under budget. O(n log n) per
        // over-budget event; n is the hosted-contract count (hundreds to ~1k),
        // and this only runs when actually over budget.
        //
        // Eligibility by pressure:
        // - AtCapacity: past-TTL AND zero-subscriber (subscribed = pinned).
        // - Overflow (OOM valve): every entry — pierce the in-use pin AND
        //   bypass min_ttl to survive genuine RAM overflow.
        //
        // `subscriber_count(key)` reads the unlocked subscription DashMaps, so it
        // is evaluated EXACTLY ONCE per entry here and reused for both the filter
        // decision and the sort key. Reading it twice (once to filter, once to
        // build the sort key) would open a TOCTOU window: a subscription
        // registered between the two reads could make an entry that passed the
        // `== 0` AtCapacity filter carry `subs >= 1` in the sort key. (The valve
        // counter is separately made airtight below by gating on `pressure`.)
        let mut candidates: Vec<(ContractKey, usize, u64)> = self
            .contracts
            .iter()
            .filter_map(|(key, entry)| {
                let subs = subscriber_count(key);
                let eligible = match pressure {
                    MemoryPressure::AtCapacity => {
                        let age = now.saturating_duration_since(entry.last_accessed);
                        age >= self.min_ttl && subs == 0
                    }
                    MemoryPressure::Overflow => true,
                };
                eligible.then_some((*key, subs, entry.last_get_seq))
            })
            .collect();

        // Ascending by subscriber_count (fewest first), then by last_get_seq
        // (least-recently real-GET first), then by contract-key bytes as a final
        // deterministic tiebreak. Under AtCapacity every candidate has
        // subscriber_count == 0, so the order reduces to real-GET recency.
        candidates.sort_by(|a, b| {
            a.1.cmp(&b.1)
                .then_with(|| a.2.cmp(&b.2))
                .then_with(|| a.0.as_bytes().cmp(b.0.as_bytes()))
        });

        let mut evicted = Vec::new();
        for (key, _subs, _) in candidates {
            if self.current_bytes <= self.budget_bytes {
                break; // back under budget, stop evicting
            }
            if let Some(entry) = self.contracts.remove(&key) {
                self.current_bytes = self.current_bytes.saturating_sub(entry.size_bytes);
                self.budget_evictions_total = self.budget_evictions_total.saturating_add(1);
                // Count OOM-valve evictions by PRESSURE, not by the entry's
                // subscriber count. Gating on `pressure == Overflow` is airtight:
                // it cannot be perturbed by a concurrent subscription change, so
                // the "stays 0 in the field while the Overflow trigger is unwired"
                // guarantee holds by construction. (Under Overflow the sweep sheds
                // fewest-subscriber-first, so these evictions MAY be subscribed
                // contracts — the valve piercing the in-use pin — which is exactly
                // the riskiest behavior this counter exists to surface.)
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
                evicted.push((key, entry.write_generation));
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
    /// Eviction respects TTL: age-0 entries are not eligible for eviction
    /// while `min_ttl > 0` (the production default — i.e. the new entry
    /// just inserted by this call cannot be evicted by the same call).
    /// It also honors `subscriber_count`: a contract with `subscriber_count(key)
    /// >= 1` (an active client subscription or downstream subscriber) is PINNED
    /// and never evicted under normal ([`MemoryPressure::AtCapacity`]) pressure,
    /// even past TTL. Evicting an in-use contract would orphan its on-disk state
    /// — the caller skips disk reclamation for it, but the contract is then gone
    /// from the cache and never revisited. (Only the OOM valve, wired via
    /// `sweep_expired` under [`MemoryPressure::Overflow`], may pierce this pin;
    /// a fresh insert always uses `AtCapacity`.)
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
        subscriber_count: G,
    ) -> RecordAccessResult
    where
        G: Fn(&ContractKey) -> usize,
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
            subscriber_count,
        )
    }

    /// Like [`record_access`](Self::record_access) but with an explicit
    /// `predicted_demand` for the demoted (telemetry-only) demand estimate.
    ///
    /// Semantics by access kind:
    /// - **New entry (any kind):** inserted with the caller's `predicted_demand`
    ///   (telemetry). A GET/SUBSCRIBE counts as one read (`read_count = 1`); a
    ///   PUT is a SEED (`read_count = 0`). Only a GET stamps `last_get_seq` (the
    ///   eviction recency key); a PUT/SUBSCRIBE leaves it at 0.
    /// - **Existing entry, GET (real read):** stamp `last_get_seq` to the
    ///   current frontier (this is the eviction recency refresh), bump
    ///   `read_count`, clear any abandonment, refresh the telemetry
    ///   `keep_score`. Returns an `observed_read_rate` training sample.
    /// - **Existing entry, SUBSCRIBE:** counts as a read for `read_count` /
    ///   telemetry, but does NOT stamp `last_get_seq` — a subscription is not a
    ///   GET, so it must not refresh eviction recency (renewal traffic would
    ///   otherwise keep every subscribed contract equally fresh).
    /// - **Existing entry, PUT (seed only):** refresh recency-of-access/size/
    ///   generation and the stored `predicted_demand`, but no read count, no
    ///   `last_get_seq` stamp, no `keep_score` refresh.
    pub fn record_access_with_demand<G>(
        &mut self,
        key: ContractKey,
        size_bytes: u64,
        access_type: AccessType,
        write_generation: u64,
        predicted_demand: f64,
        subscriber_count: G,
    ) -> RecordAccessResult
    where
        G: Fn(&ContractKey) -> usize,
    {
        let now = self.time_source.now();
        let seq = self.next_seq();
        let is_read = matches!(access_type, AccessType::Get | AccessType::Subscribe);
        // Only a real GET refreshes the eviction recency key. SUBSCRIBE and PUT
        // do not, so renewal/subscription traffic can't keep a contract fresh.
        let is_get = matches!(access_type, AccessType::Get);

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

            // A real GET refreshes the eviction recency key.
            if is_get {
                existing.last_get_seq = seq;
            }

            let observed_read_rate = if is_read {
                // Read (GET/SUBSCRIBE): count it, clear any abandoned marker,
                // and refresh the demoted telemetry keep_score.
                existing.read_count = existing.read_count.saturating_add(1);
                existing.abandoned_at = None;
                existing.keep_score = self.eviction_floor + predicted_demand;
                Self::rate_sample(existing.read_count, now, existing.inserted_at)
            } else {
                // Seed (PUT) on an existing entry: no read count, no recency
                // refresh, no telemetry keep_score refresh.
                None
            };

            RecordAccessResult {
                is_new: false,
                evicted: Vec::new(),
                observed_read_rate,
            }
        } else {
            // Not cached - insert the new entry first, then evict over-budget
            // entries. The new entry is age-0 so the eviction walk's
            // past-TTL check guarantees it is never evicted by this call
            // (under AtCapacity, which a fresh insert always uses).
            let contract = HostedContract {
                size_bytes,
                last_accessed: now,
                last_access_seq: seq,
                // Only a real GET seeds the eviction recency key; a fresh PUT
                // seed / SUBSCRIBE starts at 0 (never GET-read), so once past
                // min_ttl it sorts to evict first among zero-subscriber entries.
                last_get_seq: if is_get { seq } else { 0 },
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
            // constitutes genuine RAM overflow, so it must not pierce the pin.
            let evicted = self.evict_over_budget(&subscriber_count, MemoryPressure::AtCapacity);

            RecordAccessResult {
                is_new: true,
                evicted,
                // Brand-new entry has zero residency -> no rate sample yet.
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
            // refreshes the eviction recency key (unlike SUBSCRIBE/PUT/renewal).
            existing.last_get_seq = seq;
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

    /// Mark `key` as recently abandoned and strip its demand credit so it is the
    /// first candidate for eviction under budget pressure.
    ///
    /// Called by `HostingManager` when a contract transitions from "in
    /// use" (has subscribers / clients / downstream peers) to "no longer
    /// in use" — the moment its `contract_in_use` predicate flips from
    /// `true` to `false`. The entry's `keep_score` is dropped to the current
    /// `eviction_floor` (zero demand credit), so among the credited band it
    /// sorts first: a still-active entry keeps `floor + demand`, and since the
    /// proximity prior (`predict`) is strictly positive it sorts strictly above
    /// the abandoned entry at the floor. The TTL gate in `evict_over_budget`
    /// still applies: an abandoned entry within
    /// `min_ttl` of its last access is not yet eligible.
    ///
    /// No-op when `key` is absent (already evicted). Idempotent: calling
    /// it on an already-abandoned entry leaves the existing marker (and
    /// its earlier timestamp) intact, which is the right behavior — the
    /// first abandonment is what we want to time from.
    ///
    /// Network forgetfulness is explicitly preserved: this method does
    /// **not** evict the contract or shorten its TTL. It only changes the
    /// eviction *priority* used when the cache is genuinely over budget. A
    /// contract that's abandoned but under budget pressure stays cached.
    pub fn record_abandonment(&mut self, key: &ContractKey) {
        let floor = self.eviction_floor;
        if let Some(existing) = self.contracts.get_mut(key) {
            if existing.abandoned_at.is_none() {
                existing.abandoned_at = Some(self.time_source.now());
                // Strip demand credit: drop keep_score to the frontier so the
                // next over-budget walk evaluates this entry before any
                // still-credited (floor + demand) entry.
                existing.keep_score = floor;
            }
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

    /// Snapshot the cache's aggregate resource gauges under a single read for
    /// per-node telemetry. See [`HostingCacheStats`].
    pub fn stats(&self) -> HostingCacheStats {
        HostingCacheStats {
            budget_bytes: self.budget_bytes,
            current_bytes: self.current_bytes,
            contract_count: self.contracts.len() as u64,
            budget_evictions_total: self.budget_evictions_total,
            evictions_of_recently_read_total: self.evictions_of_recently_read_total,
            evicted_unread_total: self.evicted_unread_total,
            evicted_unread_age_secs_sum: self.evicted_unread_age_secs_sum,
            oom_valve_evictions_total: self.oom_valve_evictions_total,
        }
    }

    /// Per-contract rows for the local dashboard, in the cache-side EVICTION
    /// order: ascending `(last_get_seq, key)` — least-recent real GET first, the
    /// same order [`Self::keys_eviction_order`] uses. This reflects the order
    /// among the ZERO-subscriber candidate set the over-budget sweep would evict
    /// under `AtCapacity` (the production pressure); the subscriber-count pin is
    /// applied by the manager via `is_eviction_eligible`, so the front of the
    /// vec is the next zero-subscriber victim under budget pressure. The rows
    /// still carry the demoted telemetry score fields (`keep_score`,
    /// `predicted_demand`) the dashboard renders.
    pub(crate) fn eviction_ordered_scores(&self) -> Vec<HostingContractScore> {
        // `past_min_ttl` mirrors the `age >= self.min_ttl` term of the
        // `evict_over_budget` AtCapacity filter so the dashboard reflects real
        // sweep behavior (see `past_min_ttl` doc).
        let now = self.time_source.now();
        let mut rows: Vec<HostingContractScore> = self
            .contracts
            .iter()
            .map(|(k, v)| HostingContractScore {
                key: *k,
                keep_score: v.keep_score,
                predicted_demand: v.predicted_demand,
                size_bytes: v.size_bytes,
                read_count: v.read_count,
                last_get_seq: v.last_get_seq,
                past_min_ttl: now.saturating_duration_since(v.last_accessed) >= self.min_ttl,
            })
            .collect();
        rows.sort_by(|a, b| {
            a.last_get_seq
                .cmp(&b.last_get_seq)
                .then_with(|| a.key.as_bytes().cmp(b.key.as_bytes()))
        });
        rows
    }

    /// Get all hosted contract keys in the cache-side EVICTION order — ascending
    /// `(last_get_seq, key)`, i.e. the least-recently real-GET-read contract
    /// first. This is the order among the zero-subscriber candidate set under
    /// `AtCapacity`; the subscriber-count primary key and pin are applied by the
    /// caller (`HostingManager`), which the cache cannot see here.
    #[cfg(test)]
    pub fn keys_eviction_order(&self) -> Vec<ContractKey> {
        let mut entries: Vec<_> = self
            .contracts
            .iter()
            .map(|(k, v)| (*k, v.last_get_seq))
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
    /// subscriber-primary — ascending `(subscriber_count, last_get_seq, key)` —
    /// with eligibility governed by `pressure`:
    /// - [`MemoryPressure::AtCapacity`]: past-TTL, zero-subscriber entries only
    ///   (subscribed contracts pinned). This is the production value today.
    /// - [`MemoryPressure::Overflow`]: the OOM valve — pierces the pin and
    ///   `min_ttl` to shed the fewest-subscriber contract. The trigger is
    ///   UNWIRED in production (see [`MemoryPressure`]).
    ///
    /// `subscriber_count(key)` is the caller's genuine-demand count (client
    /// subscriptions + downstream subscribers), `0` iff not `contract_in_use`.
    ///
    /// Returns `(ContractKey, write_generation)` pairs for evicted contracts;
    /// the generation snapshot is carried through `EvictContract` so the
    /// deletion-time guard can detect a re-host race.
    pub fn sweep_expired<G>(
        &mut self,
        subscriber_count: G,
        pressure: MemoryPressure,
    ) -> Vec<(ContractKey, u64)>
    where
        G: Fn(&ContractKey) -> usize,
    {
        // Over-budget eviction logic is shared with `record_access` via
        // `evict_over_budget`: it returns early when under budget, then evicts
        // subscriber-primary (fewest-subscriber, then least-recent real GET).
        self.evict_over_budget(&subscriber_count, pressure)
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
            last_get_seq: 0,
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
    /// It deliberately does NOT stamp `last_get_seq` (the eviction recency key):
    /// persistence records only a generic LAST-ACCESS time, which does not
    /// distinguish a real GET from a PUT/SUBSCRIBE, so stamping `last_get_seq`
    /// from it would FABRICATE real-GET recency — a never-GET-read PUT seed
    /// persisted just before restart would then out-survive an entry that had
    /// genuine GET demand. Instead every reloaded entry keeps `last_get_seq == 0`
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

    fn make_cache(
        budget: u64,
        min_ttl: Duration,
    ) -> (HostingCache<SharedMockTimeSource>, SharedMockTimeSource) {
        let time_source = SharedMockTimeSource::new();
        let cache = HostingCache::new(budget, min_ttl, time_source.clone());
        (cache, time_source)
    }

    #[test]
    fn test_empty_cache() {
        let (cache, _) = make_cache(1000, Duration::from_secs(60));
        assert!(cache.is_empty());
        assert_eq!(cache.len(), 0);
        assert_eq!(cache.current_bytes(), 0);
        assert!(!cache.contains(&make_key(1)));
    }

    #[test]
    fn test_add_single_contract() {
        let (mut cache, _) = make_cache(1000, Duration::from_secs(60));
        let key = make_key(1);

        let result = cache.record_access(key, 100, AccessType::Get, 0, |_| 0);

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
        let (mut cache, time) = make_cache(1000, Duration::from_secs(60));
        let key = make_key(1);

        // First access
        cache.record_access(key, 100, AccessType::Get, 0, |_| 0);
        let first_access = cache.get(&key).unwrap().last_accessed;

        // Advance time and access again
        time.advance_time(Duration::from_secs(10));
        cache.record_access(key, 100, AccessType::Put, 0, |_| 0);

        // Should still be one contract, but updated
        assert_eq!(cache.len(), 1);
        assert_eq!(cache.current_bytes(), 100);

        let info = cache.get(&key).unwrap();
        assert_eq!(info.access_type, AccessType::Put);
        assert!(info.last_accessed > first_access);
    }

    #[test]
    fn test_lru_eviction_respects_ttl() {
        // Cache with max 200 bytes, 60 second TTL
        let (mut cache, time) = make_cache(200, Duration::from_secs(60));
        let key1 = make_key(1);
        let key2 = make_key(2);
        let key3 = make_key(3);

        // Add two entries
        cache.record_access(key1, 100, AccessType::Get, 0, |_| 0);
        cache.record_access(key2, 100, AccessType::Get, 0, |_| 0);
        assert_eq!(cache.current_bytes(), 200);

        // Advance time by 30 seconds (under TTL)
        time.advance_time(Duration::from_secs(30));

        // Add third entry - should NOT evict because all entries under TTL
        let result = cache.record_access(key3, 100, AccessType::Get, 0, |_| 0);
        assert!(
            result.evicted.is_empty(),
            "Should not evict entries under TTL"
        );
        assert_eq!(
            cache.len(),
            3,
            "Cache should exceed budget when all under TTL"
        );
        assert!(cache.contains(&key1));
        assert!(cache.contains(&key2));
        assert!(cache.contains(&key3));
    }

    #[test]
    fn test_lru_eviction_after_ttl() {
        // Cache with max 200 bytes, 60 second TTL
        let (mut cache, time) = make_cache(200, Duration::from_secs(60));
        let key1 = make_key(1);
        let key2 = make_key(2);
        let key3 = make_key(3);

        // Add two entries
        cache.record_access(key1, 100, AccessType::Get, 0, |_| 0);
        cache.record_access(key2, 100, AccessType::Get, 0, |_| 0);

        // Advance time past TTL
        time.advance_time(Duration::from_secs(61));

        // Add third entry - should evict key1 (oldest)
        let result = cache.record_access(key3, 100, AccessType::Get, 0, |_| 0);
        assert!(result.is_new);
        assert_eq!(result.evicted, vec![(key1, 0)]);
        assert_eq!(cache.len(), 2);
        assert!(!cache.contains(&key1));
        assert!(cache.contains(&key2));
        assert!(cache.contains(&key3));
    }

    #[test]
    fn test_access_refreshes_lru_position() {
        let (mut cache, time) = make_cache(200, Duration::from_secs(60));
        let key1 = make_key(1);
        let key2 = make_key(2);
        let key3 = make_key(3);

        // Add two contracts
        cache.record_access(key1, 100, AccessType::Get, 0, |_| 0);
        cache.record_access(key2, 100, AccessType::Get, 0, |_| 0);

        // GET key1 again - a real GET refreshes the eviction recency position,
        // so it becomes the LAST eviction candidate; key2 is now first.
        cache.record_access(key1, 100, AccessType::Get, 0, |_| 0);

        // Eviction order (least-worthy first) should now be [key2, key1]
        let order = cache.keys_eviction_order();
        assert_eq!(order, vec![key2, key1]);

        // Advance past TTL and add key3 - should evict key2 (now oldest)
        time.advance_time(Duration::from_secs(61));
        let result = cache.record_access(key3, 100, AccessType::Get, 0, |_| 0);

        assert_eq!(result.evicted, vec![(key2, 0)]);
        assert!(cache.contains(&key1));
        assert!(!cache.contains(&key2));
        assert!(cache.contains(&key3));
    }

    #[test]
    fn test_touch_refreshes_ttl() {
        let (mut cache, time) = make_cache(200, Duration::from_secs(60));
        let key1 = make_key(1);
        let key2 = make_key(2);
        let key3 = make_key(3);

        // Add two entries
        cache.record_access(key1, 100, AccessType::Get, 0, |_| 0);
        cache.record_access(key2, 100, AccessType::Get, 0, |_| 0);

        // Advance time by 50 seconds
        time.advance_time(Duration::from_secs(50));

        // Touch key1 (simulating UPDATE received)
        cache.touch(&key1);

        // Advance another 15 seconds (key1 now at 15s, key2 at 65s)
        time.advance_time(Duration::from_secs(15));

        // Add key3 - should evict key2 (past TTL), NOT key1 (recently touched)
        let result = cache.record_access(key3, 100, AccessType::Get, 0, |_| 0);

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
        let (mut cache, time) = make_cache(300, Duration::from_secs(60));

        let small1 = make_key(1);
        let small2 = make_key(2);
        let small3 = make_key(3);
        let large = make_key(4);

        // Add three small contracts
        cache.record_access(small1, 100, AccessType::Get, 0, |_| 0);
        cache.record_access(small2, 100, AccessType::Get, 0, |_| 0);
        cache.record_access(small3, 100, AccessType::Get, 0, |_| 0);
        assert_eq!(cache.current_bytes(), 300);

        // Advance past TTL
        time.advance_time(Duration::from_secs(61));

        // Add one large contract - should evict two small ones
        let result = cache.record_access(large, 200, AccessType::Put, 0, |_| 0);

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
        let (mut cache, time) = make_cache(200, Duration::from_secs(60));
        let key1 = make_key(1);
        let key2 = make_key(2);
        let key3 = make_key(3);

        // Add three entries (exceeds budget)
        cache.record_access(key1, 100, AccessType::Get, 0, |_| 0);
        cache.record_access(key2, 100, AccessType::Get, 0, |_| 0);
        cache.record_access(key3, 100, AccessType::Get, 0, |_| 0);
        assert_eq!(cache.len(), 3);
        assert_eq!(cache.current_bytes(), 300);

        // Sweep immediately - nothing should be evicted (all under TTL)
        let evicted = cache.sweep_expired(|_| 0, MemoryPressure::AtCapacity);
        assert!(evicted.is_empty());
        assert_eq!(cache.len(), 3);

        // Advance past TTL
        time.advance_time(Duration::from_secs(61));

        // Sweep should evict oldest entry to get back under budget
        let evicted = cache.sweep_expired(|_| 0, MemoryPressure::AtCapacity);
        assert_eq!(evicted, vec![(key1, 0)]);
        assert_eq!(cache.current_bytes(), 200);
    }

    #[test]
    fn test_sweep_respects_should_retain() {
        let (mut cache, time) = make_cache(200, Duration::from_secs(60));
        let key1 = make_key(1);
        let key2 = make_key(2);
        let key3 = make_key(3);

        // Add three entries (exceeds budget)
        cache.record_access(key1, 100, AccessType::Get, 0, |_| 0);
        cache.record_access(key2, 100, AccessType::Get, 0, |_| 0);
        cache.record_access(key3, 100, AccessType::Get, 0, |_| 0);

        // Advance past TTL
        time.advance_time(Duration::from_secs(61));

        // Sweep with predicate that retains key1
        let evicted = cache.sweep_expired(|k| (*k == key1) as usize, MemoryPressure::AtCapacity);

        // key1 should be retained, key2 evicted to get under budget
        assert_eq!(evicted, vec![(key2, 0)]);
        assert!(cache.contains(&key1));
        assert!(!cache.contains(&key2));
        assert!(cache.contains(&key3));
        assert_eq!(cache.current_bytes(), 200);
    }

    /// `record_access` must honor `should_retain`: an over-budget, past-TTL
    /// entry whose `should_retain` returns true is NOT evicted; an
    /// unretained past-TTL entry IS evicted.
    #[test]
    fn test_record_access_respects_should_retain() {
        let (mut cache, time) = make_cache(200, Duration::from_secs(60));
        let retained = make_key(1);
        let evictable = make_key(2);
        let trigger = make_key(3);

        // Fill the cache: `retained` (oldest) then `evictable`.
        cache.record_access(retained, 100, AccessType::Get, 0, |_| 0);
        cache.record_access(evictable, 100, AccessType::Get, 0, |_| 0);
        assert_eq!(cache.current_bytes(), 200);

        // Advance past TTL so both existing entries are eviction-eligible.
        time.advance_time(Duration::from_secs(61));

        // Insert a third entry, over budget. `retained` is the oldest, so a
        // naive LRU eviction would drop it first — but `should_retain`
        // protects it, so `evictable` (next oldest) must be evicted instead.
        let result = cache.record_access(trigger, 100, AccessType::Get, 0, |k| {
            (*k == retained) as usize
        });

        assert_eq!(
            result.evicted,
            vec![(evictable, 0)],
            "in-use (retained) contract must be skipped; the unretained \
             past-TTL contract must be evicted instead"
        );
        assert!(
            cache.contains(&retained),
            "retained contract must survive even past TTL and over budget"
        );
        assert!(!cache.contains(&evictable));
        assert!(cache.contains(&trigger));
    }

    /// The contract inserted by `record_access` is age-0, so while
    /// `min_ttl > 0` (the production default) it is never evicted by the
    /// same call even when the insert pushes over budget.
    #[test]
    fn test_record_access_never_evicts_the_new_entry() {
        let (mut cache, time) = make_cache(100, Duration::from_secs(60));
        let first = make_key(1);
        let second = make_key(2);

        cache.record_access(first, 100, AccessType::Get, 0, |_| 0);
        time.advance_time(Duration::from_secs(61));

        // Inserting `second` puts the cache over budget; `first` is past TTL
        // and unretained so it is evicted, but the just-inserted `second`
        // (age 0) must survive.
        let result = cache.record_access(second, 100, AccessType::Get, 0, |_| 0);
        assert_eq!(result.evicted, vec![(first, 0)]);
        assert!(cache.contains(&second));
        assert!(!cache.contains(&first));
    }

    #[test]
    fn test_touch_non_existent_is_no_op() {
        let (mut cache, _) = make_cache(1000, Duration::from_secs(60));
        let key = make_key(1);

        // Touch a key that doesn't exist
        cache.touch(&key);

        // Should remain empty
        assert!(cache.is_empty());
        assert!(!cache.contains(&key));
    }

    #[test]
    fn test_access_types() {
        let (mut cache, _) = make_cache(1000, Duration::from_secs(60));
        let key = make_key(1);

        // Test each access type is recorded correctly
        cache.record_access(key, 100, AccessType::Get, 0, |_| 0);
        assert_eq!(cache.get(&key).unwrap().access_type, AccessType::Get);

        cache.record_access(key, 100, AccessType::Put, 0, |_| 0);
        assert_eq!(cache.get(&key).unwrap().access_type, AccessType::Put);

        cache.record_access(key, 100, AccessType::Subscribe, 0, |_| 0);
        assert_eq!(cache.get(&key).unwrap().access_type, AccessType::Subscribe);
    }

    #[test]
    fn test_contract_size_change() {
        let (mut cache, _) = make_cache(1000, Duration::from_secs(60));
        let key = make_key(1);

        // Add contract with initial size
        cache.record_access(key, 100, AccessType::Get, 0, |_| 0);
        assert_eq!(cache.current_bytes(), 100);
        assert_eq!(cache.get(&key).unwrap().size_bytes, 100);

        // Contract state grows
        cache.record_access(key, 200, AccessType::Put, 0, |_| 0);
        assert_eq!(cache.current_bytes(), 200);
        assert_eq!(cache.get(&key).unwrap().size_bytes, 200);

        // Contract state shrinks
        cache.record_access(key, 150, AccessType::Put, 0, |_| 0);
        assert_eq!(cache.current_bytes(), 150);
        assert_eq!(cache.get(&key).unwrap().size_bytes, 150);
    }

    #[test]
    fn test_iter_returns_all_hosted_keys() {
        let (mut cache, _) = make_cache(1000, Duration::from_secs(60));

        // Empty cache yields no keys
        assert_eq!(cache.iter().count(), 0);

        let key1 = make_key(1);
        let key2 = make_key(2);
        let key3 = make_key(3);

        cache.record_access(key1, 100, AccessType::Get, 0, |_| 0);
        cache.record_access(key2, 100, AccessType::Put, 0, |_| 0);
        cache.record_access(key3, 100, AccessType::Subscribe, 0, |_| 0);

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
        let (mut cache, time) = make_cache(100, Duration::from_secs(60));
        let evicted_key = make_key(1);
        let trigger_key = make_key(2);

        // Insert `evicted_key` with a specific captured generation.
        let captured_generation: u64 = 0xABCD_EF42;
        let first = cache.record_access(
            evicted_key,
            100,
            AccessType::Get,
            captured_generation,
            |_| 0,
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
            |_| 0,
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
        let (mut cache, _) = make_cache(1000, Duration::from_secs(60));
        let key = make_key(7);

        cache.record_access(key, 100, AccessType::Get, 1, |_| 0);
        assert_eq!(cache.get(&key).unwrap().write_generation, 1);

        // Refresh with a higher generation (simulates a state write that
        // bumped the generation between the original insert and a later
        // GET/PUT/SUBSCRIBE re-access).
        cache.record_access(key, 100, AccessType::Put, 5, |_| 0);
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
        let (mut cache, time) = make_cache(10000, Duration::from_secs(60));
        let key = make_key(1);

        cache.record_access(key, 100, AccessType::Get, 0, |_| 0);
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
        let (mut cache, _) = make_cache(10_000, Duration::from_secs(60));
        let key1 = make_key(1);
        let key2 = make_key(2);
        let key3 = make_key(3);
        cache.record_access(key1, 111, AccessType::Get, 0, |_| 0);
        cache.record_access(key2, 222, AccessType::Get, 0, |_| 0);
        cache.record_access(key3, 333, AccessType::Get, 0, |_| 0);

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
            assert_eq!(s.last_get_seq, entry.last_get_seq);
            // Just accessed with a 60s min_ttl → not yet past the TTL gate.
            assert!(
                !s.past_min_ttl,
                "a freshly-accessed entry is within min_ttl, so it is not \
                 eviction-eligible yet"
            );
        }
    }

    #[test]
    fn eviction_ordered_scores_flags_past_min_ttl_after_ttl_elapses() {
        // `past_min_ttl` must mirror the `age >= min_ttl` term of the
        // `evict_over_budget` filter: false while within the TTL window, true
        // once the entry has aged past it. This is the cache half of the
        // dashboard "next to evict" eligibility (the in-use half is folded in
        // by `HostingManager::is_eviction_eligible`).
        let min_ttl = Duration::from_secs(60);
        let (mut cache, time_source) = make_cache(10_000, min_ttl);
        let key = make_key(1);
        cache.record_access(key, 111, AccessType::Get, 0, |_| 0);

        let before = cache.eviction_ordered_scores();
        assert!(
            !before[0].past_min_ttl,
            "within min_ttl → not past the TTL gate"
        );

        // Age the entry past min_ttl.
        time_source.advance_time(min_ttl + Duration::from_secs(1));
        let after = cache.eviction_ordered_scores();
        assert!(
            after[0].past_min_ttl,
            "past min_ttl → now eligible for the TTL gate"
        );
    }

    // --- Recently-abandoned priority (demand-credit strip) ---

    // Superseded by subscriber-primary eviction rework (#4642, PR-1): eviction
    // now orders by (subscriber_count, real-GET recency); demand/distance/
    // abandonment no longer drive eviction order. Retained as historical
    // documentation of the demoted demand machinery.
    #[ignore]
    #[test]
    fn test_record_abandonment_moves_entry_to_eviction_front() {
        let (mut cache, _) = make_cache(1000, Duration::from_secs(60));
        let key1 = make_key(1);
        let key2 = make_key(2);
        let key3 = make_key(3);

        // Equal (neutral) demand -> equal keep_score; eviction order is by
        // access sequence: key1, key2, key3.
        cache.record_access(key1, 100, AccessType::Get, 0, |_| 0);
        cache.record_access(key2, 100, AccessType::Get, 0, |_| 0);
        cache.record_access(key3, 100, AccessType::Get, 0, |_| 0);
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

    #[test]
    fn test_record_abandonment_is_idempotent() {
        let (mut cache, time) = make_cache(1000, Duration::from_secs(60));
        let key = make_key(1);
        cache.record_access(key, 100, AccessType::Get, 0, |_| 0);

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
        let (mut cache, _) = make_cache(1000, Duration::from_secs(60));
        let key = make_key(42);
        // Should not panic, should not insert anything.
        cache.record_abandonment(&key);
        assert!(!cache.contains(&key));
        assert_eq!(cache.len(), 0);
    }

    #[test]
    fn test_record_access_clears_abandoned_marker() {
        let (mut cache, _) = make_cache(1000, Duration::from_secs(60));
        let key = make_key(1);
        cache.record_access(key, 100, AccessType::Get, 0, |_| 0);
        cache.record_abandonment(&key);
        assert!(cache.get(&key).unwrap().abandoned_at.is_some());

        // Fresh access — a re-subscribe or re-GET — clears the marker
        // and returns the entry to the LRU tail.
        cache.record_access(key, 100, AccessType::Get, 0, |_| 0);
        assert!(
            cache.get(&key).unwrap().abandoned_at.is_none(),
            "record_access must clear abandoned_at"
        );
    }

    #[test]
    fn test_touch_clears_abandoned_marker() {
        let (mut cache, _) = make_cache(1000, Duration::from_secs(60));
        let key = make_key(1);
        cache.record_access(key, 100, AccessType::Get, 0, |_| 0);
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
        let (mut cache, time) = make_cache(200, Duration::from_secs(60));
        let key1 = make_key(1);
        let key2 = make_key(2);
        let key3 = make_key(3);

        cache.record_access(key1, 100, AccessType::Get, 0, |_| 0);
        cache.record_access(key2, 100, AccessType::Get, 0, |_| 0);

        // key2 is abandoned (latest in use, just lost its subscribers).
        cache.record_abandonment(&key2);

        // Both past TTL.
        time.advance_time(Duration::from_secs(61));

        // Insert key3: now over budget, must evict one. The abandoned
        // bucket means key2 goes first, even though key1 is the older
        // entry by `last_accessed`.
        let result = cache.record_access(key3, 100, AccessType::Get, 0, |_| 0);
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
        let (mut cache, time) = make_cache(1000, Duration::from_secs(60));
        let key1 = make_key(1);
        let key2 = make_key(2);

        cache.record_access(key1, 100, AccessType::Get, 0, |_| 0);
        cache.record_access(key2, 100, AccessType::Get, 0, |_| 0);

        cache.record_abandonment(&key2);
        time.advance_time(Duration::from_secs(3600));

        // No budget pressure → sweep_expired evicts nothing, abandoned
        // or otherwise.
        let evicted = cache.sweep_expired(|_| 0, MemoryPressure::AtCapacity);
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
    /// eviction and is surfaced via `stats()`; a TTL-protected or no-pressure
    /// cache leaves it untouched. This is the telemetry A2 ships to observe its
    /// own eviction rate.
    #[test]
    fn test_budget_eviction_counter_tracks_over_budget_evictions() {
        let (mut cache, time) = make_cache(200, Duration::from_secs(60));
        let key1 = make_key(1);
        let key2 = make_key(2);
        let key3 = make_key(3);

        cache.record_access(key1, 100, AccessType::Get, 0, |_| 0);
        cache.record_access(key2, 100, AccessType::Get, 0, |_| 0);
        assert_eq!(cache.stats().budget_evictions_total, 0);

        // Over budget but every entry is under TTL: no budget eviction yet.
        cache.record_access(key3, 100, AccessType::Get, 0, |_| 0);
        assert_eq!(
            cache.stats().budget_evictions_total,
            0,
            "TTL-protected entries must not count as budget evictions"
        );

        // Past TTL: a sweep now evicts the single over-budget entry.
        time.advance_time(Duration::from_secs(61));
        let evicted = cache.sweep_expired(|_| 0, MemoryPressure::AtCapacity);
        assert_eq!(
            evicted.len(),
            1,
            "one 100-byte entry brings 300 back to 200"
        );
        let stats = cache.stats();
        assert_eq!(stats.budget_evictions_total, 1);
        assert_eq!(stats.current_bytes, 200);
        assert_eq!(stats.contract_count, 2);
        assert_eq!(stats.budget_bytes, 200);

        // No pressure now (at budget): the counter is unchanged.
        let evicted = cache.sweep_expired(|_| 0, MemoryPressure::AtCapacity);
        assert!(evicted.is_empty(), "at budget -> no further eviction");
        assert_eq!(
            cache.stats().budget_evictions_total,
            1,
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
        let (mut cache, time) = make_cache(100, Duration::from_secs(60));
        let key1 = make_key(1);
        let key2 = make_key(2);
        let key3 = make_key(3);

        // Seed key1 via PUT (read_count == 0 — an unread seed).
        cache.record_access(key1, 100, AccessType::Put, 0, |_| 0);
        assert_eq!(cache.stats().evicted_unread_total, 0);

        // Past TTL, insert key2 (also a seed). key1 is evicted: never read, so
        // it counts as an unread-seed eviction and its age (~61s) is recorded.
        time.advance_time(Duration::from_secs(61));
        let result = cache.record_access(key2, 100, AccessType::Put, 0, |_| 0);
        assert_eq!(result.evicted, vec![(key1, 0)]);
        let stats = cache.stats();
        assert_eq!(stats.budget_evictions_total, 1);
        assert_eq!(stats.evicted_unread_total, 1, "an unread seed was evicted");
        assert_eq!(
            stats.evicted_unread_age_secs_sum, 61,
            "age-at-eviction of the unread seed is recorded"
        );

        // Read key2 once (GET) so read_count == 1 — no longer an unread seed.
        cache.record_access(key2, 100, AccessType::Get, 0, |_| 0);

        // Past TTL, insert key3 to evict key2. key2 was read once, so it is
        // neither an unread seed nor a recently-read (>=2) victim: the unread
        // counter and age sum both hold.
        time.advance_time(Duration::from_secs(61));
        let result = cache.record_access(key3, 100, AccessType::Get, 0, |_| 0);
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
        let (mut cache, time) = make_cache(100, Duration::from_secs(60));
        let key1 = make_key(1);
        let key2 = make_key(2);

        // Reload key1 from persistence with read_count 0 — the exact shape of a
        // long-hosted entry after a restart resets its counters. (A PUT was its
        // last pre-restart access, so access_type == Put, proving the counter
        // cannot key on access_type instead of the explicit marker.)
        cache.load_persisted_entry(key1, 100, AccessType::Put, Duration::from_secs(1), false);
        cache.finalize_loading();
        assert!(cache.contains(&key1));
        assert_eq!(cache.stats().evicted_unread_total, 0);

        // Advance past min_ttl so the reloaded entry is eviction-eligible, then a
        // fresh insert pushes over budget and sheds it (the post-load burst).
        time.advance_time(Duration::from_secs(61));
        let result = cache.record_access(key2, 100, AccessType::Get, 0, |_| 0);
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
    /// entry keeps `last_get_seq == 0` regardless of its persisted access type,
    /// because persistence records only a generic last-ACCESS time and cannot
    /// distinguish a real GET from a PUT/SUBSCRIBE. Stamping `last_get_seq` from
    /// last-access order would let a never-GET-read PUT seed out-survive an entry
    /// with genuine GET demand after a restart.
    #[test]
    fn reloaded_entry_has_zero_get_recency_regardless_of_access_type() {
        let (mut cache, _) = make_cache(10_000, Duration::from_secs(60));
        let put_seed = make_key(1);
        let subscribed = make_key(2);

        // Both reloaded from persistence with different pre-restart access types
        // and different persisted recency (so finalize_loading's last_access_seq
        // ordering differs) — neither may fabricate a nonzero last_get_seq.
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
            cache.get(&put_seed).unwrap().last_get_seq,
            0,
            "a reloaded PUT entry must have last_get_seq == 0 (never GET-read since restart)"
        );
        assert_eq!(
            cache.get(&subscribed).unwrap().last_get_seq,
            0,
            "a reloaded SUBSCRIBE entry must also have last_get_seq == 0"
        );

        // A real GET after restart re-stamps it to a genuine (nonzero) value,
        // lifting it out of the never-GET-read front of the eviction order.
        cache.record_access(put_seed, 100, AccessType::Get, 0, |_| 0);
        assert!(
            cache.get(&put_seed).unwrap().last_get_seq > 0,
            "a real GET after restart must re-stamp last_get_seq to a real value"
        );
    }

    // --- Demand-ordered (Greedy-Dual) eviction (A3) ---

    /// `keep_score` is `eviction_floor + predicted_demand` on insert, and a read
    /// refreshes it to the CURRENT floor + demand. Since the floor only rises,
    /// a repeatedly-read contract's keep_score climbs while an untouched one's
    /// stays put.
    #[test]
    fn fuel_gauge_keep_score_set_on_insert_and_refreshed_on_read() {
        let (mut cache, _) = make_cache(10_000, Duration::from_secs(60));
        let key = make_key(1);

        // Insert at floor 0 with demand 3.0 -> keep_score 3.0.
        cache.record_access_with_demand(key, 100, AccessType::Get, 0, 3.0, |_| 0);
        assert_eq!(cache.get(&key).unwrap().keep_score, 3.0);
        assert_eq!(cache.get(&key).unwrap().predicted_demand, 3.0);
        assert_eq!(cache.get(&key).unwrap().read_count, 1);

        // A read with a new demand estimate refreshes keep_score = floor + demand.
        cache.record_access_with_demand(key, 100, AccessType::Get, 0, 5.0, |_| 0);
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
        let (mut cache, time) = make_cache(200, Duration::from_secs(60));
        let high = make_key(1);
        let low = make_key(2);
        let trigger = make_key(3);

        // `high` inserted first (older) with strong demand; `low` inserted after
        // with weak demand. A recency-only (LRU) policy would evict `high`.
        cache.record_access_with_demand(high, 100, AccessType::Get, 0, 10.0, |_| 0);
        cache.record_access_with_demand(low, 100, AccessType::Get, 0, 1.0, |_| 0);
        assert_eq!(cache.current_bytes(), 200);

        time.advance_time(Duration::from_secs(61));

        // Over-budget insert must evict the LOWEST keep_score (`low`), not the
        // oldest (`high`).
        let result = cache.record_access_with_demand(trigger, 100, AccessType::Get, 0, 1.0, |_| 0);
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
        let (mut cache, time) = make_cache(200, Duration::from_secs(60));
        let near = make_key(1);
        let far = make_key(2);
        let trigger = make_key(3);

        // Insert both cold (zero samples), scored only by the distance prior.
        cache.record_access_with_demand(near, 100, AccessType::Get, 0, near_demand, |_| 0);
        cache.record_access_with_demand(far, 100, AccessType::Get, 0, far_demand, |_| 0);
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
            |_| 0,
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
    #[ignore]
    #[test]
    fn fuel_gauge_keeps_repeatedly_read_contract_over_junk() {
        // Budget for two 100-byte contracts.
        let (mut cache, time) = make_cache(200, Duration::from_secs(60));
        let room = make_key(1); // the "River room": repeatedly read, high demand
        let junk = make_key(2); // never re-read, low demand, inserted LATER
        let trigger = make_key(3);

        // Seed the room with strong demand, then read it repeatedly (simulating
        // recurring GETs). These reads are the room's LAST accesses.
        cache.record_access_with_demand(room, 100, AccessType::Get, 0, 10.0, |_| 0);
        for _ in 0..3 {
            time.advance_time(Duration::from_secs(5));
            cache.touch(&room);
        }

        // Junk arrives AFTER the room's last read, so junk is the more-recently-
        // accessed entry — and carries weak demand.
        time.advance_time(Duration::from_secs(5));
        cache.record_access_with_demand(junk, 100, AccessType::Get, 0, 1.0, |_| 0);

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
        let result = cache.record_access_with_demand(trigger, 100, AccessType::Get, 0, 1.0, |_| 0);
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
        let (mut cache, time) = make_cache(100, Duration::from_secs(60));
        assert_eq!(cache.eviction_floor(), 0.0);

        // Insert a demand-7 contract, let it age, then evict it via an
        // over-budget insert; the floor must climb to 7.
        let a = make_key(1);
        cache.record_access_with_demand(a, 100, AccessType::Get, 0, 7.0, |_| 0);
        time.advance_time(Duration::from_secs(61));
        let b = make_key(2);
        let r = cache.record_access_with_demand(b, 100, AccessType::Get, 0, 2.0, |_| 0);
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
        let r = cache.record_access_with_demand(c, 100, AccessType::Get, 0, 1.0, |_| 0);
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
        let (mut cache, _) = make_cache(10_000, Duration::from_secs(60));
        let key = make_key(1);

        // Seed via PUT: keep_score = floor(0) + demand, read_count 0.
        cache.record_access_with_demand(key, 100, AccessType::Put, 0, 2.0, |_| 0);
        assert_eq!(cache.get(&key).unwrap().read_count, 0, "PUT is not a read");
        let seed_score = cache.get(&key).unwrap().keep_score;
        assert_eq!(seed_score, 2.0);

        // Manually raise the floor by evicting something else would be indirect;
        // instead assert that a re-PUT does NOT refresh keep_score or read_count.
        cache.record_access_with_demand(key, 100, AccessType::Put, 0, 9.0, |_| 0);
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
        cache.record_access_with_demand(key, 100, AccessType::Get, 0, 9.0, |_| 0);
        assert_eq!(cache.get(&key).unwrap().read_count, 1);
        assert_eq!(cache.get(&key).unwrap().keep_score, 9.0);
    }

    /// The recently-read eviction counter fires only for victims with genuine
    /// repeat demand (`read_count >= 2`), not one-off seeds.
    #[test]
    fn evictions_of_recently_read_counts_only_repeat_demand() {
        let (mut cache, time) = make_cache(100, Duration::from_secs(60));

        // A contract read twice (repeat demand), then forced out by junk.
        let read_twice = make_key(1);
        cache.record_access(read_twice, 100, AccessType::Get, 0, |_| 0);
        cache.touch(&read_twice); // second read -> read_count 2
        time.advance_time(Duration::from_secs(61));
        // Evict it by inserting a higher-demand junk (so read_twice is the victim).
        let junk = make_key(2);
        let r = cache.record_access_with_demand(junk, 100, AccessType::Get, 0, 5.0, |_| 0);
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
        let r = cache.record_access_with_demand(seed, 100, AccessType::Get, 0, 9.0, |_| 0);
        assert_eq!(r.evicted, vec![(junk, 0)], "junk (read once) evicts");
        assert_eq!(
            cache.stats().evictions_of_recently_read_total,
            1,
            "a one-off seed eviction must not count as recently-read"
        );
    }

    /// A subscribed (retained) contract is exempt from eviction even when its
    /// `keep_score` is the lowest and it is past TTL — the pin dominates demand
    /// ordering.
    #[test]
    fn subscribe_pin_exempts_lowest_score_from_eviction() {
        let (mut cache, time) = make_cache(200, Duration::from_secs(60));
        let pinned = make_key(1); // lowest demand, but subscribed
        let other = make_key(2);
        let trigger = make_key(3);

        cache.record_access_with_demand(pinned, 100, AccessType::Get, 0, 0.1, |_| 0);
        cache.record_access_with_demand(other, 100, AccessType::Get, 0, 5.0, |_| 0);
        time.advance_time(Duration::from_secs(61));

        // Over budget: `pinned` has the lowest keep_score and would evict first,
        // but should_retain protects it, so `other` is evicted instead.
        let result = cache.record_access_with_demand(trigger, 100, AccessType::Get, 0, 5.0, |k| {
            (*k == pinned) as usize
        });
        assert_eq!(
            result.evicted,
            vec![(other, 0)],
            "an active subscription pins the contract even at the lowest keep_score"
        );
        assert!(cache.contains(&pinned));
        assert!(!cache.contains(&other));
    }

    // --- Subscriber-primary eviction (#4642, PR-1) ---

    /// Under normal (AtCapacity) over-budget pressure a subscribed contract is
    /// PINNED and only the zero-subscriber contract is evicted.
    #[test]
    fn evict_normal_pins_subscribed_drops_zero_sub() {
        let (mut cache, time) = make_cache(200, Duration::from_secs(60));
        let a = make_key(1);
        let b = make_key(2);
        let c = make_key(3);

        // A is subscribed (pinned); B is zero-subscriber.
        let subs = |k: &ContractKey| usize::from(*k == a);
        cache.record_access(a, 100, AccessType::Get, 0, subs);
        cache.record_access(b, 100, AccessType::Get, 0, subs);
        assert_eq!(cache.current_bytes(), 200);

        // Past TTL, insert C over budget. Under AtCapacity, A (subscribed) is
        // pinned and B (zero-subscriber, past TTL) is the only eligible victim.
        time.advance_time(Duration::from_secs(61));
        let result = cache.record_access(c, 100, AccessType::Get, 0, subs);
        assert_eq!(
            result.evicted,
            vec![(b, 0)],
            "a subscribed contract is pinned; only the zero-subscriber one is evicted"
        );
        assert!(cache.contains(&a), "subscribed A stays pinned");
        assert!(cache.contains(&c));
        assert!(!cache.contains(&b));
    }

    /// Neither SUBSCRIBE nor PUT refreshes the eviction recency key, so a
    /// contract subscribed/re-PUT after its last GET stays the least-recent
    /// real-GET entry and is evicted first.
    #[test]
    fn real_get_refreshes_recency_subscribe_and_put_do_not() {
        let (mut cache, time) = make_cache(300, Duration::from_secs(60));
        let a = make_key(1);
        let b = make_key(2);
        let c = make_key(3);
        let d = make_key(4);

        // last_get order A < B < C.
        cache.record_access(a, 100, AccessType::Get, 0, |_| 0);
        cache.record_access(b, 100, AccessType::Get, 0, |_| 0);
        cache.record_access(c, 100, AccessType::Get, 0, |_| 0);

        // A SUBSCRIBE on A and a PUT on B must NOT refresh their real-GET
        // recency, so A stays the least-recently-GET entry.
        cache.record_access(a, 100, AccessType::Subscribe, 0, |_| 0);
        cache.record_access(b, 100, AccessType::Put, 0, |_| 0);

        time.advance_time(Duration::from_secs(61));
        let result = cache.record_access(d, 100, AccessType::Get, 0, |_| 0);
        assert_eq!(
            result.evicted,
            vec![(a, 0)],
            "A is evicted: SUBSCRIBE/PUT did not refresh its real-GET recency"
        );
        assert!(!cache.contains(&a));
        assert!(cache.contains(&b));
        assert!(cache.contains(&c));
        assert!(cache.contains(&d));
    }

    /// A real GET refreshes the eviction recency key, floating the read entry to
    /// the newest position so a different (now least-recent) entry evicts.
    #[test]
    fn real_get_refreshes_recency() {
        let (mut cache, time) = make_cache(300, Duration::from_secs(60));
        let a = make_key(1);
        let b = make_key(2);
        let c = make_key(3);
        let d = make_key(4);

        cache.record_access(a, 100, AccessType::Get, 0, |_| 0);
        cache.record_access(b, 100, AccessType::Get, 0, |_| 0);
        cache.record_access(c, 100, AccessType::Get, 0, |_| 0);

        // A real GET on A makes A the newest, leaving B least-recently-GET.
        cache.record_access(a, 100, AccessType::Get, 0, |_| 0);

        time.advance_time(Duration::from_secs(61));
        let result = cache.record_access(d, 100, AccessType::Get, 0, |_| 0);
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

    /// The OOM valve (Overflow) pierces the in-use pin and sheds the
    /// FEWEST-subscriber contract first, bumping the valve counter.
    #[test]
    fn oom_valve_sheds_fewest_subscriber_piercing_pin() {
        let (mut cache, _time) = make_cache(100, Duration::from_secs(60));
        let a = make_key(1);
        let b = make_key(2);

        // A has more subscribers than B; both are subscribed (pinned under
        // AtCapacity).
        let subs = |k: &ContractKey| if *k == a { 2 } else { 1 };
        cache.record_access(a, 100, AccessType::Get, 0, subs);
        // Inserting B pushes over budget, but under AtCapacity both are pinned
        // (subscriber_count >= 1) and B is age-0, so nothing is evicted.
        let insert = cache.record_access(b, 100, AccessType::Get, 0, subs);
        assert!(
            insert.evicted.is_empty(),
            "AtCapacity pins both subscribed contracts"
        );
        assert!(cache.contains(&a));
        assert!(cache.contains(&b));
        assert_eq!(cache.current_bytes(), 200, "200 bytes over the 100 budget");

        // The OOM valve pierces the pin and sheds the FEWEST-subscriber contract
        // (B, 1 sub) before A (2 subs), bumping the valve counter.
        let evicted = cache.sweep_expired(subs, MemoryPressure::Overflow);
        assert_eq!(
            evicted,
            vec![(b, 0)],
            "fewest-subscriber contract sheds first"
        );
        assert!(cache.contains(&a), "the more-subscribed contract survives");
        assert!(!cache.contains(&b));
        assert_eq!(cache.stats().oom_valve_evictions_total, 1);
    }

    /// With equal subscriber counts, the OOM valve tiebreaks by least-recent
    /// real GET.
    #[test]
    fn oom_valve_equal_subscribers_sheds_least_recent_get() {
        let (mut cache, _time) = make_cache(100, Duration::from_secs(60));
        let a = make_key(1);
        let b = make_key(2);

        // Equal subscriber counts: the tiebreak is least-recent real GET.
        cache.record_access(a, 100, AccessType::Get, 0, |_| 1);
        cache.record_access(b, 100, AccessType::Get, 0, |_| 1);
        assert_eq!(cache.current_bytes(), 200);

        let evicted = cache.sweep_expired(|_| 1, MemoryPressure::Overflow);
        assert_eq!(
            evicted,
            vec![(a, 0)],
            "with equal subscribers, the least-recently-GET contract (A) sheds"
        );
        assert!(!cache.contains(&a));
        assert!(cache.contains(&b));
        assert_eq!(cache.stats().oom_valve_evictions_total, 1);
    }

    /// A normal AtCapacity eviction of a zero-subscriber contract must NOT
    /// touch the OOM-valve counter (that counter is only for pierced pins).
    #[test]
    fn oom_valve_counter_zero_under_normal_atcapacity() {
        let (mut cache, time) = make_cache(100, Duration::from_secs(60));
        let a = make_key(1);
        let b = make_key(2);

        cache.record_access(a, 100, AccessType::Get, 0, |_| 0);
        time.advance_time(Duration::from_secs(61));
        // Normal AtCapacity eviction of a zero-subscriber, past-TTL contract.
        let result = cache.record_access(b, 100, AccessType::Get, 0, |_| 0);
        assert_eq!(result.evicted, vec![(a, 0)]);
        assert_eq!(
            cache.stats().oom_valve_evictions_total,
            0,
            "a normal zero-subscriber eviction must not touch the OOM-valve counter"
        );
    }

    /// Cold-start grace: a fresh, never-read zero-subscriber PUT seed survives
    /// over-budget pressure within min_ttl, then becomes evictable past it.
    #[test]
    fn cold_start_grace_fresh_unread_seed_survives_min_ttl() {
        let (mut cache, time) = make_cache(100, Duration::from_secs(60));
        let seed = make_key(1);
        let other = make_key(2);

        // A fresh PUT seed (never GET-read, zero-subscriber, read_count 0).
        cache.record_access(seed, 100, AccessType::Put, 0, |_| 0);
        // Inserting `other` pushes over budget, but the seed is age-0 / within
        // min_ttl, so cold-start grace keeps it: nothing is evicted.
        let result = cache.record_access(other, 100, AccessType::Get, 0, |_| 0);
        assert!(
            result.evicted.is_empty(),
            "a fresh unread zero-subscriber seed is not evicted within min_ttl even over budget"
        );
        assert!(cache.contains(&seed));
        assert!(cache.contains(&other));

        // Past min_ttl the unread seed becomes eligible and sheds under budget.
        time.advance_time(Duration::from_secs(61));
        let evicted = cache.sweep_expired(|_| 0, MemoryPressure::AtCapacity);
        assert!(
            evicted.iter().any(|(k, _)| *k == seed),
            "past min_ttl the unread seed is evicted"
        );
        assert!(!cache.contains(&seed));
    }
}
