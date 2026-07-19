//! Merge-failure backoff — the load-bearing #4861 storm fix.
//!
//! ## What this does
//!
//! A "poison" contract is one whose delta/state merges reliably fail. The #4861
//! incident surfaced three distinct poison classes, all of which this backoff
//! must contain WITHOUT assuming the divergence ever heals (the fault is a
//! contract-layer bug; core's job is only to BOUND the cost):
//!
//! - **Compute poison** — every merge exceeds the execution budget (a runaway
//!   `update_state`). The [`MergeFailureClass::Timeout`] class targets this. A
//!   5s CPU burn is contract-intrinsic (whoever sent it, the node cannot afford
//!   probes), so Timeout suppression is **contract-wide**.
//! - **Semantic fork oscillation** — two stable divergent states, each side's
//!   deltas rejected by the other; every resync full-state apply just flips the
//!   node to the other fork forever (~1 cycle/min). Contained here because the
//!   backoff is reset ONLY by a genuine successful DELTA merge, never by a
//!   fork-flipping full-state/resync apply (see the note in `node.rs`).
//! - **Deserialization poison** — a malformed delta circulating that no holder
//!   can apply. Presents as [`MergeFailureClass::Invalid`].
//!
//! In all three, a handful of such contracts drove network-wide CPU churn:
//! every re-broadcast that slipped past the 60s [`BroadcastDedupCache`] re-ran
//! the (failing, expensive) WASM merge, and every delta failure emitted a
//! full-state `ResyncRequest`, which pulled a fresh full state that failed
//! again — a self-sustaining storm across many senders.
//!
//! ## Suppression scope: per-sender Invalid, contract-wide Timeout
//!
//! [`MergeFailureClass::Invalid`] failures are tracked and suppressed
//! **per-`(ContractInstanceId, sender)`**. [`MergeFailureClass::Timeout`]
//! failures and the failed-payload memo are **contract-wide**. This split is a
//! deliberate anti-abuse boundary (#4864 review P1):
//!
//! - **Single-attacker blackout is impossible.** A contract-wide Invalid gate
//!   keyed only on the contract id would let ONE connected peer send 3 distinct
//!   invalid deltas (spaced past the [`UpdateRateLimiter`]) and put a *healthy*
//!   contract into a contract-wide cooldown; during that cooldown every honest
//!   peer's valid delta is dropped unexecuted, so the success that would clear
//!   the entry can never land (self-sustaining blackout, re-poisoned each
//!   expiry toward the 30-min cap). Per-sender Invalid scoping means a bad
//!   sender only ever suppresses ITS OWN deltas; other peers merge normally and
//!   keep the contract live.
//! - **Fork storms stay contained per-channel.** The 45 diverged senders of the
//!   production fork each trip their own `(contract, sender)` channel (~3
//!   executions, then cooldown) while healthy senders flow — matching the
//!   observed evidence better than a single contract-wide gate.
//! - **Compute poison stays contract-wide.** A Timeout is a full ~5s CPU burn
//!   regardless of who sent it; the node cannot afford to keep probing it per
//!   sender, and an attacker who can make a healthy contract burn 5s per delta
//!   is exactly what a contract-wide quarantine must stop.
//!
//! The sender key is its `SocketAddr` (ip+port). Per the ring rules a raw
//! `SocketAddr` is normally a poor peer identifier because NAT'd peers can share
//! an IP — but ip+**port** distinguishes distinct NAT'd connections, the driver
//! has the addr in hand (no `PeerKeyLocation` lookup), and the entry is
//! ephemeral (TTL-swept, capped), so this matches the [`UpdateRateLimiter`]
//! precedent rather than warranting a heavier identity. The trade-off: a peer
//! that reconnects on a fresh port starts a fresh Invalid channel (a bounded
//! re-probe, not a correctness issue).
//!
//! ## The two suppression signals
//!
//! Both are cleared by a successful DELTA merge:
//!
//! - **Cooldown** ([`MergeDecision::InBackoff`]): time-based. The per-sender
//!   Invalid channel skips only that sender's deltas; the contract-wide Timeout
//!   channel skips all merges for the contract. Escalates per failure.
//! - **Known-failed-payload memoization** ([`MergeDecision::KnownFailedPayload`]):
//!   a specific `(is_delta, bytes)` payload that already failed cannot change
//!   outcome until the contract's state changes, so it is skipped even after the
//!   cooldown elapses (bounded by [`FAILED_PAYLOAD_TTL`]). The memo is
//!   **contract-wide and content-addressed**, and the skip is a HARD one: an
//!   identical bad payload replayed via ANY sender is skipped the instant it is
//!   memoized — no trip gate. Contract execution is deterministic, so the exact
//!   bad bytes fail identically for every sender; re-running is pure waste, and
//!   trip-gating would hand each fresh sender in the fork storm 3 free
//!   executions of a known-bad payload. The skip can never block a legitimate
//!   delta (new content = different bytes = memo miss), and it is ZERO-COST:
//!   the driver returns without running the merge, so the presenting sender's
//!   channel is NOT advanced and no `ResyncRequest` is emitted (the benign
//!   stale-reject penalises a channel only when the merge actually RUNS and
//!   fails). Any successful merge (a state-generation bump) drops the memo,
//!   re-admitting every payload.
//!
//! ## Failure classes
//!
//! [`MergeFailureClass::Timeout`] (a WASM execution timeout — see
//! `ExecutorError::is_wasm_timeout`) gets a much longer base/cap than
//! [`MergeFailureClass::Invalid`] (a cheap contract-side rejection or other exec
//! error): a runaway merge that pins the single contract-handling thread is far
//! more expensive to re-attempt than a merge that returns a rejection quickly.
//! The two classes are tracked in SEPARATE channels (per-sender Invalid vs
//! contract-wide Timeout), so there is no in-entry class escalation and hence no
//! escalation-inflation to correct: a contract's first Timeout cooldown is
//! always [`TIMEOUT_BASE`] no matter how many Invalid failures preceded it (the
//! #4864 review O1 concern is structurally avoided by the split).
//!
//! ## Trip threshold (why a single Invalid failure does not suppress)
//!
//! The two classes also differ in HOW MANY consecutive failures (with no
//! intervening success) must accrue before the FIRST cooldown suppresses:
//! `Invalid` requires [`INVALID_TRIP_THRESHOLD`] (3), `Timeout` requires
//! [`TIMEOUT_TRIP_THRESHOLD`] (1). The discriminator is whether the failure is
//! *cheap and interleaves with successes* or *consecutive-without-success*:
//!
//! - A benign stale-version `InvalidUpdate` reject (#3914) is cheap and normal —
//!   a busy contract hits it on every re-broadcast the 60s dedup missed, always
//!   interleaved with the successful newer-version merges. Tripping at 1 would
//!   blackout that sender's channel for 30s. Requiring 3 *consecutive* failures
//!   (per sender) means a benign reject (which a success always interrupts)
//!   never trips.
//! - A poison / fork sender fails consecutively with zero clean deltas, so its
//!   channel reaches 3 and trips (the fork storm at ~2 failures/min trips in
//!   ~90s).
//! - A `Timeout` is a full ~5s CPU burn every time; one is enough, so it trips
//!   at 1.
//!
//! ## Bounded growth
//!
//! Modeled on [`crate::ring::update_rate_limit::UpdateRateLimiter`]: each
//! [`DashMap`] (per-sender Invalid, contract-wide Timeout+memo) has a strict
//! [`AtomicUsize`] size cap ([`MAX_TRACKED_CONTRACTS`]) so an attacker churning
//! fresh contract ids (or `(contract, sender)` pairs) cannot grow the maps, plus
//! a periodic TTL sweep ([`Self::cleanup_expired`]) hooked into the Ring reaper.
//! Per-contract failed-payload history is a bounded [`VecDeque`]
//! ([`MAX_FAILED_PAYLOADS_PER_CONTRACT`]).
//!
//! ## What this is NOT
//!
//! - Not a ban. A poison contract self-heals the instant a merge succeeds
//!   (its state changed) or after the cooldown/TTL elapses.
//! - Not applied to queue-full backpressure (transient load, not poison) or to
//!   missing-contract failures (those are healed by auto-fetch, not by backing
//!   off) — the caller only records `is_contract_exec_rejection` failures.
//! - Not applied to client-local UPDATEs: a local client always gets a direct
//!   merge + error. (A successful client-local DELTA does clear the
//!   contract-wide Timeout + memo via [`Self::record_success_local`], but leaves
//!   per-sender Invalid channels intact — a local success says nothing about any
//!   specific remote sender's fork.)

use std::collections::VecDeque;
use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::time::Duration;

use dashmap::DashMap;
use freenet_stdlib::prelude::ContractInstanceId;
use tokio::time::Instant;

use crate::config::GlobalRng;
use crate::util::time_source::TimeSource;

/// Base cooldown for an `Invalid`-class failure (cheap contract rejection).
const INVALID_BASE: Duration = Duration::from_secs(30);
/// Cap for `Invalid`-class cooldown (30 min).
const INVALID_CAP: Duration = Duration::from_secs(30 * 60);
/// Base cooldown for a `Timeout`-class failure (runaway merge). Longer than
/// `Invalid` because re-attempting a merge that pins the contract thread for the
/// whole execution budget is far more costly than one that rejects quickly.
const TIMEOUT_BASE: Duration = Duration::from_secs(120);
/// Cap for `Timeout`-class cooldown (2h).
const TIMEOUT_CAP: Duration = Duration::from_secs(2 * 60 * 60);

/// Consecutive `Invalid`-class failures (with NO intervening success) required
/// before the FIRST cooldown suppresses merges for a given `(contract, sender)`
/// (#4864 review M1). Trip-at-1 would blackout a sender's channel on a single
/// benign stale-version `InvalidUpdate` reject (#3914: production hits these on
/// every re-broadcast missed by the 60s dedup). A benign reject interleaves with
/// successes, so it never reaches 3 consecutive; a genuine poison/fork sender
/// fails consecutively and trips. The fork storm (~2 failures/min, zero clean
/// deltas) still trips within ~90s.
const INVALID_TRIP_THRESHOLD: u32 = 3;
/// Consecutive `Timeout`-class failures required before suppression. One is
/// enough: every timeout is a full ~5s CPU burn on the single contract thread,
/// so there is no cheap-interleaved-with-success case to tolerate.
const TIMEOUT_TRIP_THRESHOLD: u32 = 1;

/// How long a failed `(is_delta, bytes)` payload hash is remembered (10 min).
const FAILED_PAYLOAD_TTL: Duration = Duration::from_secs(10 * 60);
/// Max remembered failed-payload hashes per contract. Bounds per-entry memory
/// and caps the memoization at the most-recent distinct failing payloads.
const MAX_FAILED_PAYLOADS_PER_CONTRACT: usize = 32;
/// Hard cap on tracked keys per map. At ~200 bytes/entry, 16 384 ≈ 3 MB. Bounds
/// the worst case where an attacker chooses fresh contract ids or sender ports.
pub(crate) const MAX_TRACKED_CONTRACTS: usize = 16_384;
/// Idle-entry cleanup grace period. An entry past its cooldown is retained until
/// it has been idle (no new failure) this long, preserving the failure counter
/// across consecutive failure→cooldown→retry cycles. Aligned with the longest
/// cooldown ([`TIMEOUT_CAP`]) so a contract still in a long cooldown is never
/// dropped early.
const CLEANUP_AGE: Duration = TIMEOUT_CAP;

/// Compute the memoization/dedup hash for a broadcast payload. Mirrors
/// [`crate::operations::update::BroadcastDedupCache`]: includes the `is_delta`
/// discriminant so a delta and a full state with identical bytes do not collide.
pub(crate) fn merge_payload_hash(is_delta: bool, payload_bytes: &[u8]) -> u64 {
    use ahash::AHasher;
    use std::hash::Hasher;
    let mut hasher = AHasher::default();
    hasher.write_u8(u8::from(is_delta));
    hasher.write(payload_bytes);
    hasher.finish()
}

/// `base * 2^(failures - trip_threshold)`, capped, with ±20% jitter (seeded
/// `GlobalRng`, so reproducible under simulation). The exponent is measured from
/// the TRIP point, so the FIRST suppressing cooldown (at
/// `failures == trip_threshold`) is the base, then escalates (#4864 review M1).
/// `saturating_sub` floors the pre-trip failures at the base too — those
/// cooldowns are never consulted (see [`MergeBackoff::check`]).
fn cooldown_for(class: MergeFailureClass, consecutive_failures: u32) -> Duration {
    let base = class.base();
    let cap = class.cap();
    let exponent = consecutive_failures
        .saturating_sub(class.trip_threshold())
        .min(20);
    let raw = base.saturating_mul(1u32 << exponent.min(30));
    let capped = raw.min(cap);
    let jitter: f64 = GlobalRng::random_range(0.8_f64..=1.2_f64);
    capped.mul_f64(jitter)
}

/// Failure class deciding the cooldown parameters (and the suppression scope:
/// `Invalid` is per-sender, `Timeout` is contract-wide).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum MergeFailureClass {
    /// Cheap contract-side rejection (`InvalidUpdate`) or any other non-timeout
    /// execution error (trap, OOG, deser). Base [`INVALID_BASE`]. Per-sender.
    Invalid,
    /// The WASM merge exceeded the execution time limit. Base [`TIMEOUT_BASE`].
    /// Contract-wide.
    Timeout,
}

impl MergeFailureClass {
    fn base(self) -> Duration {
        match self {
            MergeFailureClass::Invalid => INVALID_BASE,
            MergeFailureClass::Timeout => TIMEOUT_BASE,
        }
    }

    fn cap(self) -> Duration {
        match self {
            MergeFailureClass::Invalid => INVALID_CAP,
            MergeFailureClass::Timeout => TIMEOUT_CAP,
        }
    }

    /// Consecutive-failures-without-success required before this class starts
    /// suppressing merges. See [`INVALID_TRIP_THRESHOLD`] /
    /// [`TIMEOUT_TRIP_THRESHOLD`].
    fn trip_threshold(self) -> u32 {
        match self {
            MergeFailureClass::Invalid => INVALID_TRIP_THRESHOLD,
            MergeFailureClass::Timeout => TIMEOUT_TRIP_THRESHOLD,
        }
    }
}

/// Outcome of a pre-merge backoff check.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum MergeDecision {
    /// Not in backoff and payload not known-failed — run the merge.
    Allow,
    /// The sender's Invalid channel or the contract's Timeout is in a cooldown
    /// window — skip the merge (and any amplification, e.g. `ResyncRequest`).
    InBackoff,
    /// This exact payload already failed and no successful merge has cleared the
    /// entry since — skip; the outcome cannot change until the state changes.
    KnownFailedPayload,
}

impl MergeDecision {
    /// True when the merge should proceed.
    #[cfg_attr(not(test), allow(dead_code))]
    pub fn is_allowed(self) -> bool {
        matches!(self, MergeDecision::Allow)
    }
}

/// One exponential cooldown channel. Used per-`(contract, sender)` for the
/// `Invalid` class and once per contract (inside [`ContractState`]) for the
/// `Timeout` class. The class is fixed by WHERE the channel lives, so it is not
/// stored here. `consecutive_failures == 0` is the inert "no failure recorded"
/// state (never suppresses, since `0 < trip_threshold`).
struct Cooldown {
    consecutive_failures: u32,
    next_allowed: Instant,
    last_failure: Instant,
}

impl Cooldown {
    fn inert(now: Instant) -> Self {
        Self {
            consecutive_failures: 0,
            next_allowed: now,
            last_failure: now,
        }
    }

    /// Record one failure of `class`, escalating the cooldown.
    fn record(&mut self, class: MergeFailureClass, now: Instant) {
        self.consecutive_failures = self.consecutive_failures.saturating_add(1);
        self.last_failure = now;
        self.next_allowed = now + cooldown_for(class, self.consecutive_failures);
    }

    /// Has this channel reached its class trip threshold (poison, regardless of
    /// whether the current cooldown window is still open)? Gates the memo.
    fn tripped(&self, class: MergeFailureClass) -> bool {
        self.consecutive_failures >= class.trip_threshold()
    }

    /// Is this channel actively suppressing right now (tripped AND still inside
    /// the cooldown window)?
    fn suppressing(&self, class: MergeFailureClass, now: Instant) -> bool {
        self.tripped(class) && now < self.next_allowed
    }

    fn past_cooldown(&self, now: Instant) -> bool {
        now >= self.next_allowed
    }

    fn idle(&self, now: Instant) -> bool {
        now.saturating_duration_since(self.last_failure) > CLEANUP_AGE
    }
}

/// Contract-wide state: the `Timeout`-class cooldown plus the content-addressed
/// failed-payload memo (both apply to ALL senders).
struct ContractState {
    timeout: Cooldown,
    /// `(payload_hash, recorded_at)`, newest at back. Bounded + TTL-pruned.
    failed_payloads: VecDeque<(u64, Instant)>,
    /// Last time any failure touched this entry (drives the idle sweep).
    last_touch: Instant,
}

impl ContractState {
    fn new(now: Instant) -> Self {
        Self {
            timeout: Cooldown::inert(now),
            failed_payloads: VecDeque::new(),
            last_touch: now,
        }
    }

    /// Drop failed-payload records older than [`FAILED_PAYLOAD_TTL`].
    fn prune_payloads(&mut self, now: Instant) {
        while let Some((_, at)) = self.failed_payloads.front() {
            if now.saturating_duration_since(*at) > FAILED_PAYLOAD_TTL {
                self.failed_payloads.pop_front();
            } else {
                break;
            }
        }
    }

    /// Memoize a failing payload (dedup + bounded + TTL-pruned).
    fn memoize(&mut self, payload_hash: u64, now: Instant) {
        self.prune_payloads(now);
        if self.failed_payloads.iter().any(|(h, _)| *h == payload_hash) {
            return;
        }
        if self.failed_payloads.len() >= MAX_FAILED_PAYLOADS_PER_CONTRACT {
            self.failed_payloads.pop_front();
        }
        self.failed_payloads.push_back((payload_hash, now));
    }
}

/// Merge-failure backoff tracker. Two [`DashMap`]s: a per-`(contract, sender)`
/// `Invalid`-class map and a per-contract `Timeout`+memo map. Concurrency +
/// bounding mirror [`crate::ring::update_rate_limit::UpdateRateLimiter`].
pub(crate) struct MergeBackoff {
    /// Per-`(contract, sender)` `Invalid`-class cooldown.
    invalid_by_sender: DashMap<(ContractInstanceId, SocketAddr), Cooldown>,
    /// Authoritative size counter for the invalid map's strict cap.
    invalid_size: AtomicUsize,
    /// Per-contract `Timeout`-class cooldown + contract-wide failed-payload memo.
    contract: DashMap<ContractInstanceId, ContractState>,
    /// Authoritative size counter for the contract map's strict cap.
    contract_size: AtomicUsize,
    max_tracked: usize,
    time_source: Arc<dyn TimeSource + Send + Sync>,
    /// Total merges skipped (cooldown + known-payload) since creation.
    suppressed_total: AtomicU64,
}

impl MergeBackoff {
    pub fn new(time_source: Arc<dyn TimeSource + Send + Sync>) -> Self {
        Self::with_max(time_source, MAX_TRACKED_CONTRACTS)
    }

    pub fn with_max(time_source: Arc<dyn TimeSource + Send + Sync>, max_tracked: usize) -> Self {
        Self {
            invalid_by_sender: DashMap::new(),
            invalid_size: AtomicUsize::new(0),
            contract: DashMap::new(),
            contract_size: AtomicUsize::new(0),
            max_tracked,
            time_source,
            suppressed_total: AtomicU64::new(0),
        }
    }

    /// Decide whether a merge for `contract` presented by `sender` with the given
    /// payload should run.
    ///
    /// Suppression kicks in ONLY once the relevant channel has failed at least
    /// `class.trip_threshold()` times consecutively without an intervening
    /// success (#4864 review M1): the presenting sender's `Invalid` channel, or
    /// the contract's `Timeout` channel. `KnownFailedPayload` (contract-wide,
    /// content-addressed) takes precedence over `InBackoff`, but only once the
    /// contract is quarantined for this sender (sender Invalid tripped OR
    /// contract Timeout tripped) so a benign first reject is never suppressed.
    /// Bumps `suppressed_total` on any non-`Allow` outcome.
    ///
    /// Reads the two maps sequentially and never holds both shard guards at once
    /// (no cross-map lock order); the tiny TOCTOU window against a concurrent
    /// record is benign for a backoff heuristic.
    pub fn check(
        &self,
        contract: &ContractInstanceId,
        sender: SocketAddr,
        payload_hash: u64,
    ) -> MergeDecision {
        let now = self.time_source.now();

        // Per-sender Invalid channel — read the suppress flag, then release the
        // shard guard before touching the contract map.
        let sender_suppressing = self
            .invalid_by_sender
            .get(&(*contract, sender))
            .map(|cd| cd.suppressing(MergeFailureClass::Invalid, now))
            .unwrap_or(false);

        if let Some(mut cs) = self.contract.get_mut(contract) {
            cs.prune_payloads(now);
            // Content-addressed memo is a HARD skip for ANY sender the instant a
            // payload is memoized — no trip gate. Contract execution is
            // deterministic, so an exact payload that already failed against the
            // current state fails identically for every sender; re-running it is
            // pure waste (the production fork storm replays identical deltas via
            // ~45 senders — trip-gating the memo would hand each fresh sender 3
            // free executions of a known-bad payload). Skipping it can never
            // block a legitimate delta: new content = different bytes = memo
            // miss. The skip is ZERO-COST for the sender's channel: the driver
            // returns without running the merge, so `record_failure` is never
            // called (no failure-count increment) and no ResyncRequest is
            // emitted. Cleared when a success advances the state.
            if cs.failed_payloads.iter().any(|(h, _)| *h == payload_hash) {
                self.suppressed_total.fetch_add(1, Ordering::Relaxed);
                return MergeDecision::KnownFailedPayload;
            }
            if cs.timeout.suppressing(MergeFailureClass::Timeout, now) {
                self.suppressed_total.fetch_add(1, Ordering::Relaxed);
                return MergeDecision::InBackoff;
            }
        }

        if sender_suppressing {
            self.suppressed_total.fetch_add(1, Ordering::Relaxed);
            return MergeDecision::InBackoff;
        }
        MergeDecision::Allow
    }

    /// Record a failed merge. An `Invalid` failure escalates the presenting
    /// sender's per-`(contract, sender)` cooldown; a `Timeout` failure escalates
    /// the contract-wide `Timeout` cooldown. Either way the payload is memoized
    /// contract-wide. At a map's tracked cap, a failure for a *new* key is
    /// dropped (graceful degradation — bounded memory under id/port churn).
    pub fn record_failure(
        &self,
        contract: &ContractInstanceId,
        sender: SocketAddr,
        class: MergeFailureClass,
        payload_hash: u64,
    ) {
        let now = self.time_source.now();
        if class == MergeFailureClass::Invalid {
            self.record_invalid_sender(*contract, sender, now);
        }
        // Contract-wide side: memoize the payload always; escalate the Timeout
        // cooldown when this is a Timeout failure.
        self.with_contract_entry(*contract, now, |cs| {
            if class == MergeFailureClass::Timeout {
                cs.timeout.record(MergeFailureClass::Timeout, now);
            }
            cs.last_touch = now;
            cs.memoize(payload_hash, now);
        });
    }

    fn record_invalid_sender(
        &self,
        contract: ContractInstanceId,
        sender: SocketAddr,
        now: Instant,
    ) {
        use dashmap::mapref::entry::Entry as DEntry;
        match self.invalid_by_sender.entry((contract, sender)) {
            DEntry::Occupied(mut occ) => occ.get_mut().record(MergeFailureClass::Invalid, now),
            DEntry::Vacant(vac) => {
                // Reserve a slot before inserting (strict cap, see UpdateRateLimiter).
                let prev = self.invalid_size.fetch_add(1, Ordering::Relaxed);
                if prev >= self.max_tracked {
                    self.invalid_size.fetch_sub(1, Ordering::Relaxed);
                    return;
                }
                let mut cd = Cooldown::inert(now);
                cd.record(MergeFailureClass::Invalid, now);
                vac.insert(cd);
            }
        }
    }

    /// Get-or-create the contract-wide entry (respecting the strict cap) and run
    /// `f` on it. At the cap, a *new* contract's failure is dropped and `f` is
    /// not run.
    fn with_contract_entry(
        &self,
        contract: ContractInstanceId,
        now: Instant,
        f: impl FnOnce(&mut ContractState),
    ) {
        use dashmap::mapref::entry::Entry as DEntry;
        match self.contract.entry(contract) {
            DEntry::Occupied(mut occ) => f(occ.get_mut()),
            DEntry::Vacant(vac) => {
                let prev = self.contract_size.fetch_add(1, Ordering::Relaxed);
                if prev >= self.max_tracked {
                    self.contract_size.fetch_sub(1, Ordering::Relaxed);
                    return;
                }
                let mut cs = ContractState::new(now);
                f(&mut cs);
                vac.insert(cs);
            }
        }
    }

    /// A successful DELTA merge from `sender`: clears THIS sender's `Invalid`
    /// channel, plus the contract-wide `Timeout` cooldown and the failed-payload
    /// memo (the state advanced, so every remembered failure is stale). Other
    /// senders' `Invalid` channels are left intact — they may genuinely sit on
    /// the other fork.
    pub fn record_success_from_sender(&self, contract: &ContractInstanceId, sender: SocketAddr) {
        if self
            .invalid_by_sender
            .remove(&(*contract, sender))
            .is_some()
        {
            self.invalid_size.fetch_sub(1, Ordering::Relaxed);
        }
        self.clear_contract_side(contract);
    }

    /// A successful client-local DELTA merge: clears the contract-wide `Timeout`
    /// cooldown and the failed-payload memo ONLY. Per-sender `Invalid` channels
    /// are untouched — a local client's success proves the contract merges but
    /// says nothing about any specific remote sender's fork.
    pub fn record_success_local(&self, contract: &ContractInstanceId) {
        self.clear_contract_side(contract);
    }

    /// Drop the contract-wide entry (clears both the `Timeout` cooldown and the
    /// payload memo). Does not touch any per-sender `Invalid` channel.
    fn clear_contract_side(&self, contract: &ContractInstanceId) {
        if self.contract.remove(contract).is_some() {
            self.contract_size.fetch_sub(1, Ordering::Relaxed);
        }
    }

    /// Drop entries whose cooldown has elapsed AND that have been idle longer
    /// than [`CLEANUP_AGE`]; prune per-entry expired payload memos. Call from the
    /// Ring reaper. Bounds memory as idle contracts/senders roll off.
    pub fn cleanup_expired(&self) {
        let now = self.time_source.now();

        let mut removed_invalid = 0usize;
        self.invalid_by_sender.retain(|_, cd| {
            if cd.past_cooldown(now) && cd.idle(now) {
                removed_invalid += 1;
                return false;
            }
            true
        });
        if removed_invalid > 0 {
            self.invalid_size
                .fetch_sub(removed_invalid, Ordering::Relaxed);
        }

        let mut removed_contract = 0usize;
        self.contract.retain(|_, cs| {
            cs.prune_payloads(now);
            let past_cooldown = cs.timeout.past_cooldown(now);
            let idle = now.saturating_duration_since(cs.last_touch) > CLEANUP_AGE;
            if past_cooldown && cs.failed_payloads.is_empty() && idle {
                removed_contract += 1;
                return false;
            }
            true
        });
        if removed_contract > 0 {
            self.contract_size
                .fetch_sub(removed_contract, Ordering::Relaxed);
        }
    }

    /// Total merges skipped by this backoff since creation (dashboard / tests).
    /// Not yet surfaced on the live dashboard (a `RingStatsSnapshot` wire field
    /// is a clean follow-up); the sim-test signal is
    /// `GlobalTestMetrics::merges_suppressed_by_backoff`.
    #[cfg_attr(not(test), allow(dead_code))]
    pub fn suppressed_total(&self) -> u64 {
        self.suppressed_total.load(Ordering::Relaxed)
    }

    /// Number of distinct contracts currently SUPPRESSING at least one merge
    /// path (gauge): a contract counts if its `Timeout` channel is suppressing OR
    /// any of its per-sender `Invalid` channels is. Scans both maps (bounded by
    /// [`MAX_TRACKED_CONTRACTS`]).
    #[cfg_attr(not(test), allow(dead_code))]
    pub fn contracts_in_backoff(&self) -> usize {
        let now = self.time_source.now();
        let mut suppressing = std::collections::HashSet::new();
        for e in self.contract.iter() {
            if e.value()
                .timeout
                .suppressing(MergeFailureClass::Timeout, now)
            {
                suppressing.insert(*e.key());
            }
        }
        for e in self.invalid_by_sender.iter() {
            if e.value().suppressing(MergeFailureClass::Invalid, now) {
                suppressing.insert(e.key().0);
            }
        }
        suppressing.len()
    }

    /// Number of tracked per-sender `Invalid` channels (tests).
    #[cfg(test)]
    fn invalid_len(&self) -> usize {
        self.invalid_by_sender.len()
    }

    /// Number of tracked contract-wide entries (tests).
    #[cfg(test)]
    fn contract_len(&self) -> usize {
        self.contract.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::util::time_source::SharedMockTimeSource;

    fn mk_contract(byte: u8) -> ContractInstanceId {
        ContractInstanceId::new([byte; 32])
    }

    fn mk_addr(port: u16) -> SocketAddr {
        SocketAddr::from(([127, 0, 0, 1], port))
    }

    fn mk() -> (MergeBackoff, SharedMockTimeSource) {
        let ts = SharedMockTimeSource::new();
        (MergeBackoff::new(Arc::new(ts.clone())), ts)
    }

    #[test]
    fn untracked_contract_is_allowed() {
        let (b, _ts) = mk();
        assert_eq!(
            b.check(&mk_contract(1), mk_addr(1), 42),
            MergeDecision::Allow
        );
        assert_eq!(b.invalid_len(), 0, "check must not create an entry");
        assert_eq!(b.contract_len(), 0, "check must not create an entry");
    }

    /// #4864 review M1 test (b): sustained consecutive Invalid failures (no
    /// successes) from ONE sender do NOT suppress on failures 1-2, then trip at
    /// the 3rd.
    #[test]
    fn invalid_trips_only_after_three_consecutive_failures() {
        let (b, _ts) = mk();
        let c = mk_contract(1);
        let s = mk_addr(10);
        b.record_failure(&c, s, MergeFailureClass::Invalid, 1);
        assert_eq!(b.check(&c, s, 2), MergeDecision::Allow, "failure 1");
        b.record_failure(&c, s, MergeFailureClass::Invalid, 3);
        assert_eq!(b.check(&c, s, 4), MergeDecision::Allow, "failure 2");
        b.record_failure(&c, s, MergeFailureClass::Invalid, 5);
        assert_eq!(
            b.check(&c, s, 6),
            MergeDecision::InBackoff,
            "failure 3 trips"
        );
        assert_eq!(b.suppressed_total(), 1);
    }

    /// #4864 review P1 (the point of the per-sender rework): one sender tripping
    /// its Invalid channel must NOT suppress a DIFFERENT sender's delta — the
    /// second sender's merge still runs. A contract-wide gate would blackout the
    /// healthy sender here.
    #[test]
    fn invalid_backoff_is_scoped_per_sender() {
        let (b, _ts) = mk();
        let c = mk_contract(1);
        let poison = mk_addr(10);
        let healthy = mk_addr(20);
        // Poison sender trips its own channel (3 consecutive failures).
        for h in [1u64, 2, 3] {
            b.record_failure(&c, poison, MergeFailureClass::Invalid, h);
        }
        assert_eq!(
            b.check(&c, poison, 99),
            MergeDecision::InBackoff,
            "the poison sender is suppressed on its own channel"
        );
        // A different sender's delta is NOT suppressed — the contract stays live.
        assert_eq!(
            b.check(&c, healthy, 99),
            MergeDecision::Allow,
            "a healthy sender must still reach the merge (per-sender scoping)"
        );
    }

    /// #4864 review M1 test (a): two Invalid failures followed by a success from
    /// the same sender are never suppressed, and that sender's channel is cleared.
    #[test]
    fn invalid_two_failures_then_success_never_suppressed() {
        let (b, _ts) = mk();
        let c = mk_contract(1);
        let s = mk_addr(10);
        b.record_failure(&c, s, MergeFailureClass::Invalid, 1);
        assert!(b.check(&c, s, 2).is_allowed());
        b.record_failure(&c, s, MergeFailureClass::Invalid, 3);
        assert!(b.check(&c, s, 4).is_allowed());
        b.record_success_from_sender(&c, s);
        assert_eq!(b.invalid_len(), 0, "success clears the sender's channel");
        assert_eq!(b.suppressed_total(), 0, "nothing was ever suppressed");
        b.record_failure(&c, s, MergeFailureClass::Invalid, 5);
        assert_eq!(
            b.check(&c, s, 6),
            MergeDecision::Allow,
            "post-success failure 1 must not suppress"
        );
    }

    #[test]
    fn success_from_sender_clears_that_sender_and_contract_side() {
        let (b, _ts) = mk();
        let c = mk_contract(1);
        let s = mk_addr(10);
        for h in [1u64, 3, 5] {
            b.record_failure(&c, s, MergeFailureClass::Invalid, h);
        }
        assert!(!b.check(&c, s, 2).is_allowed());
        b.record_success_from_sender(&c, s);
        assert_eq!(b.check(&c, s, 2), MergeDecision::Allow);
        assert_eq!(
            b.check(&c, s, 1),
            MergeDecision::Allow,
            "memoized payload cleared too"
        );
        assert_eq!(b.invalid_len(), 0);
        assert_eq!(b.contract_len(), 0);
    }

    /// A success from sender S must NOT clear a DIFFERENT sender's Invalid
    /// channel (that sender may sit on the other fork). The contract-wide side
    /// (Timeout + memo) IS cleared.
    #[test]
    fn success_from_sender_leaves_other_senders_intact() {
        let (b, _ts) = mk();
        let c = mk_contract(1);
        let s1 = mk_addr(10);
        let s2 = mk_addr(20);
        for h in [1u64, 2, 3] {
            b.record_failure(&c, s1, MergeFailureClass::Invalid, h);
        }
        for h in [4u64, 5, 6] {
            b.record_failure(&c, s2, MergeFailureClass::Invalid, h);
        }
        // A success from s1 clears s1 but not s2.
        b.record_success_from_sender(&c, s1);
        assert_eq!(b.check(&c, s1, 100), MergeDecision::Allow, "s1 cleared");
        assert_eq!(
            b.check(&c, s2, 101),
            MergeDecision::InBackoff,
            "s2's independent channel is untouched"
        );
    }

    /// `record_success_local` (client-local success) clears the contract-wide
    /// Timeout + memo but leaves EVERY per-sender Invalid channel intact — a
    /// local client's success says nothing about any remote sender's fork.
    #[test]
    fn record_success_local_clears_contract_side_leaves_sender_channels() {
        let (b, _ts) = mk();
        let c = mk_contract(1);
        let s = mk_addr(10);
        b.record_failure(&c, s, MergeFailureClass::Timeout, 1);
        for h in [2u64, 3, 4] {
            b.record_failure(&c, s, MergeFailureClass::Invalid, h);
        }
        b.record_success_local(&c);
        // Contract-wide Timeout gone: a fresh sender with no channel is Allowed.
        assert_eq!(b.check(&c, mk_addr(99), 100), MergeDecision::Allow);
        // The sender's own Invalid channel is untouched — still suppressing.
        assert_eq!(b.check(&c, s, 100), MergeDecision::InBackoff);
    }

    #[test]
    fn known_failed_payload_skipped_even_after_cooldown() {
        let (b, ts) = mk();
        let c = mk_contract(1);
        let s = mk_addr(10);
        for _ in 0..3 {
            b.record_failure(&c, s, MergeFailureClass::Invalid, 99);
        }
        ts.advance_time(Duration::from_secs(60));
        assert_eq!(b.check(&c, s, 99), MergeDecision::KnownFailedPayload);
        assert_eq!(b.check(&c, s, 7), MergeDecision::Allow);
    }

    #[test]
    fn known_failed_payload_expires_after_ttl() {
        let (b, ts) = mk();
        let c = mk_contract(1);
        let s = mk_addr(10);
        for _ in 0..3 {
            b.record_failure(&c, s, MergeFailureClass::Invalid, 99);
        }
        ts.advance_time(FAILED_PAYLOAD_TTL + Duration::from_secs(1));
        assert_eq!(b.check(&c, s, 99), MergeDecision::Allow);
    }

    /// #4864 review M1 test (c): a Timeout trips at the FIRST failure and is
    /// contract-wide (suppresses regardless of which sender presents next).
    #[test]
    fn timeout_trips_at_first_failure_and_is_contract_wide() {
        let (b, _ts) = mk();
        let c = mk_contract(1);
        b.record_failure(&c, mk_addr(10), MergeFailureClass::Timeout, 1);
        assert_eq!(
            b.check(&c, mk_addr(10), 2),
            MergeDecision::InBackoff,
            "a single timeout (full ~5s CPU burn) must trip immediately"
        );
        // Contract-wide: a DIFFERENT sender is also suppressed.
        assert_eq!(
            b.check(&c, mk_addr(20), 3),
            MergeDecision::InBackoff,
            "Timeout suppression is contract-wide, not per-sender"
        );
    }

    #[test]
    fn timeout_class_gets_longer_cooldown_than_invalid() {
        let (b, ts) = mk();
        let invalid = mk_contract(1);
        let timeout = mk_contract(2);
        let s = mk_addr(10);
        for h in [1u64, 2, 3] {
            b.record_failure(&invalid, s, MergeFailureClass::Invalid, h);
        }
        b.record_failure(&timeout, s, MergeFailureClass::Timeout, 1);
        ts.advance_time(Duration::from_secs(60));
        assert_eq!(
            b.check(&invalid, s, 555),
            MergeDecision::Allow,
            "invalid-class cooldown (30s) should have elapsed by 60s"
        );
        assert_eq!(
            b.check(&timeout, s, 555),
            MergeDecision::InBackoff,
            "timeout-class cooldown (120s) should still be active at 60s"
        );
    }

    /// The Invalid (per-sender) and Timeout (contract-wide) channels are tracked
    /// independently: a Timeout persists and keeps suppressing even as later
    /// Invalid failures arrive, and its FIRST cooldown is TIMEOUT_BASE regardless
    /// of preceding Invalid failures (#4864 review O1 is structurally avoided —
    /// no shared counter to inflate).
    #[test]
    fn timeout_and_invalid_tracked_independently() {
        let (b, ts) = mk();
        let c = mk_contract(1);
        let s = mk_addr(10);
        // Two Invalid failures then a Timeout — the Timeout's first cooldown must
        // be ~120s (its base), not base shifted by the Invalid history.
        b.record_failure(&c, s, MergeFailureClass::Invalid, 1);
        b.record_failure(&c, s, MergeFailureClass::Invalid, 2);
        b.record_failure(&c, s, MergeFailureClass::Timeout, 3);
        let cs = b.contract.get(&c).unwrap();
        let cooldown = cs
            .timeout
            .next_allowed
            .saturating_duration_since(cs.timeout.last_failure);
        drop(cs);
        assert!(
            cooldown >= TIMEOUT_BASE.mul_f64(0.8) && cooldown <= TIMEOUT_BASE.mul_f64(1.2),
            "first Timeout cooldown {cooldown:?} must be TIMEOUT_BASE ±20%, not \
             inflated by the two preceding Invalid failures"
        );
        // A later Invalid does not downgrade / clear the still-active Timeout.
        ts.advance_time(Duration::from_secs(60));
        b.record_failure(&c, s, MergeFailureClass::Invalid, 4);
        assert_eq!(
            b.check(&c, mk_addr(99), 999),
            MergeDecision::InBackoff,
            "the contract-wide Timeout keeps suppressing (never downgrades)"
        );
    }

    #[test]
    fn cooldown_escalates_after_trip() {
        let (b, ts) = mk();
        let c = mk_contract(1);
        let s = mk_addr(10);
        for h in [1u64, 2, 3] {
            b.record_failure(&c, s, MergeFailureClass::Invalid, h);
        }
        ts.advance_time(Duration::from_secs(40));
        assert_eq!(
            b.check(&c, s, 10),
            MergeDecision::Allow,
            "first (base) cooldown should have elapsed by 40s"
        );
        b.record_failure(&c, s, MergeFailureClass::Invalid, 4);
        ts.advance_time(Duration::from_secs(40));
        assert_eq!(
            b.check(&c, s, 11),
            MergeDecision::InBackoff,
            "post-trip failure must escalate the cooldown past 40s"
        );
    }

    #[test]
    fn cooldown_jitter_stays_within_bounds() {
        for seed_byte in 0..32u8 {
            let (b, _ts) = mk();
            let c = mk_contract(seed_byte);
            let s = mk_addr(10);
            b.record_failure(&c, s, MergeFailureClass::Invalid, 1);
            let cd = b.invalid_by_sender.get(&(c, s)).unwrap();
            let cooldown = cd.next_allowed.saturating_duration_since(cd.last_failure);
            assert!(
                cooldown >= Duration::from_secs(24) && cooldown <= Duration::from_secs(36),
                "first-failure cooldown {cooldown:?} must be 30s ±20%"
            );
        }
    }

    #[test]
    fn cooldown_capped_at_class_max() {
        let (b, _ts) = mk();
        let c = mk_contract(1);
        let s = mk_addr(10);
        for i in 0..40u64 {
            b.record_failure(&c, s, MergeFailureClass::Invalid, i);
        }
        let cd = b.invalid_by_sender.get(&(c, s)).unwrap();
        let cooldown = cd.next_allowed.saturating_duration_since(cd.last_failure);
        assert!(
            cooldown <= INVALID_CAP.mul_f64(1.2),
            "escalating cooldown {cooldown:?} must stay capped near INVALID_CAP"
        );
        assert!(cooldown >= INVALID_CAP.mul_f64(0.8));
    }

    #[test]
    fn tracked_senders_are_capped() {
        let ts = SharedMockTimeSource::new();
        let b = MergeBackoff::with_max(Arc::new(ts.clone()), 4);
        let c = mk_contract(1);
        for port in 0..4u16 {
            b.record_failure(&c, mk_addr(port), MergeFailureClass::Invalid, 1);
        }
        assert_eq!(b.invalid_len(), 4);
        // 5th distinct sender is dropped (not tracked) — bounded memory.
        b.record_failure(&c, mk_addr(99), MergeFailureClass::Invalid, 1);
        assert_eq!(b.invalid_len(), 4, "cap must bound the per-sender channels");
        // But an already-tracked sender keeps escalating.
        b.record_failure(&c, mk_addr(0), MergeFailureClass::Invalid, 2);
        assert_eq!(b.invalid_len(), 4);
    }

    #[test]
    fn tracked_contracts_are_capped() {
        let ts = SharedMockTimeSource::new();
        let b = MergeBackoff::with_max(Arc::new(ts.clone()), 4);
        let s = mk_addr(10);
        for i in 0..4u8 {
            b.record_failure(&mk_contract(i), s, MergeFailureClass::Timeout, 1);
        }
        assert_eq!(b.contract_len(), 4);
        b.record_failure(&mk_contract(99), s, MergeFailureClass::Timeout, 1);
        assert_eq!(b.contract_len(), 4, "cap must bound the contract entries");
    }

    #[test]
    fn failed_payloads_are_bounded_per_contract() {
        let (b, _ts) = mk();
        let c = mk_contract(1);
        let s = mk_addr(10);
        for i in 0..(MAX_FAILED_PAYLOADS_PER_CONTRACT as u64 + 10) {
            b.record_failure(&c, s, MergeFailureClass::Invalid, i);
        }
        let cs = b.contract.get(&c).unwrap();
        assert!(
            cs.failed_payloads.len() <= MAX_FAILED_PAYLOADS_PER_CONTRACT,
            "per-contract failed-payload history must stay bounded"
        );
    }

    #[test]
    fn cleanup_removes_idle_expired_entries() {
        let (b, ts) = mk();
        let c = mk_contract(1);
        let s = mk_addr(10);
        b.record_failure(&c, s, MergeFailureClass::Invalid, 1);
        assert_eq!(b.invalid_len(), 1);
        assert_eq!(b.contract_len(), 1);
        ts.advance_time(CLEANUP_AGE + Duration::from_secs(1));
        b.cleanup_expired();
        assert_eq!(
            b.invalid_len(),
            0,
            "idle expired sender channel must be swept"
        );
        assert_eq!(
            b.contract_len(),
            0,
            "idle expired contract entry must be swept"
        );
        assert_eq!(b.invalid_size.load(Ordering::Relaxed), 0);
        assert_eq!(b.contract_size.load(Ordering::Relaxed), 0);
    }

    #[test]
    fn cleanup_preserves_active_cooldown() {
        let (b, ts) = mk();
        let c = mk_contract(1);
        b.record_failure(&c, mk_addr(10), MergeFailureClass::Timeout, 1);
        ts.advance_time(Duration::from_secs(60));
        b.cleanup_expired();
        assert_eq!(
            b.contract_len(),
            1,
            "an entry still in cooldown must not be swept"
        );
    }

    #[test]
    fn fork_oscillation_escalates_into_backoff_without_reset() {
        // Regression for the #4861 semantic fork-oscillation poison class: a
        // single fork sender's alternating deltas each fail, and every
        // ResyncResponse full-state apply flips the node to the other fork — a
        // "success" that proves nothing about convergence and does NOT reset the
        // backoff (node.rs). The per-sender failures accumulate and the sender's
        // channel enters backoff.
        let (b, ts) = mk();
        let c = mk_contract(1);
        let s = mk_addr(10);
        b.record_failure(&c, s, MergeFailureClass::Invalid, 0xAAAA);
        ts.advance_time(Duration::from_secs(40));
        b.record_failure(&c, s, MergeFailureClass::Invalid, 0xBBBB);
        ts.advance_time(Duration::from_secs(40));
        b.record_failure(&c, s, MergeFailureClass::Invalid, 0xCCCC);
        assert_eq!(
            b.check(&c, s, 0xDDDD),
            MergeDecision::InBackoff,
            "alternating fork deltas must trip the backoff (no resync reset)"
        );
    }

    #[test]
    fn contracts_in_backoff_gauge() {
        let (b, ts) = mk();
        let s = mk_addr(10);
        // Trip the Invalid contract (3 consecutive from one sender) and the
        // Timeout contract (1).
        for h in [1u64, 2, 3] {
            b.record_failure(&mk_contract(1), s, MergeFailureClass::Invalid, h);
        }
        b.record_failure(&mk_contract(2), s, MergeFailureClass::Timeout, 1);
        assert_eq!(b.contracts_in_backoff(), 2);
        ts.advance_time(Duration::from_secs(60));
        assert_eq!(
            b.contracts_in_backoff(),
            1,
            "only the still-cooling Timeout contract should count"
        );
    }

    /// The gauge must NOT count a contract still below its trip threshold.
    #[test]
    fn contracts_in_backoff_gauge_excludes_pre_trip_entries() {
        let (b, _ts) = mk();
        let c = mk_contract(1);
        let s = mk_addr(10);
        b.record_failure(&c, s, MergeFailureClass::Invalid, 1);
        b.record_failure(&c, s, MergeFailureClass::Invalid, 2);
        assert_eq!(b.contracts_in_backoff(), 0);
        assert!(b.check(&c, s, 3).is_allowed());
        b.record_failure(&c, s, MergeFailureClass::Invalid, 3);
        assert_eq!(b.contracts_in_backoff(), 1);
    }

    /// #4864 review item 7(a) (memo is a HARD content-addressed skip): a payload
    /// that failed is `KnownFailedPayload` IMMEDIATELY — even pre-trip, and even
    /// presented by a DIFFERENT sender (deterministic execution: the same bad
    /// bytes fail for everyone). But a sender's NOVEL payloads still run: the
    /// memo hard-skip never blocks a legitimate delta.
    #[test]
    fn memoized_payload_is_hard_skipped_for_any_sender_pre_trip() {
        let (b, _ts) = mk();
        let c = mk_contract(1);
        let s1 = mk_addr(10);
        let s2 = mk_addr(20);
        // s1 fails two DISTINCT payloads — its channel is at 2, still PRE-trip.
        b.record_failure(&c, s1, MergeFailureClass::Invalid, 0x11);
        b.record_failure(&c, s1, MergeFailureClass::Invalid, 0x22);
        // Both exact payloads are KnownFailedPayload immediately — pre-trip, and
        // even for s1 itself (no trip gate).
        assert_eq!(
            b.check(&c, s1, 0x11),
            MergeDecision::KnownFailedPayload,
            "a memoized payload is hard-skipped immediately, even pre-trip"
        );
        // ... and for a DIFFERENT sender that never saw it.
        assert_eq!(
            b.check(&c, s2, 0x22),
            MergeDecision::KnownFailedPayload,
            "the memo is content-addressed: any sender replaying it is hard-skipped"
        );
        // But s2's OWN novel payload still runs (memo miss, empty channel).
        assert_eq!(
            b.check(&c, s2, 0x33),
            MergeDecision::Allow,
            "a novel payload must still run — the memo never blocks new content"
        );
    }
}
