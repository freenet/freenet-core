//! Per-contract merge-failure backoff — the load-bearing #4861 storm fix.
//!
//! ## What this does
//!
//! A "poison" contract is one whose delta/state merges reliably fail. The #4861
//! incident surfaced three distinct poison classes, all of which this backoff
//! must contain WITHOUT assuming the divergence ever heals (the fault is a
//! contract-layer bug; core's job is only to BOUND the cost):
//!
//! - **Compute poison** — every merge exceeds the execution budget (a runaway
//!   `update_state`). The [`MergeFailureClass::Timeout`] class targets this.
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
//! This tracks, per [`ContractInstanceId`], a cooldown that grows exponentially
//! with consecutive merge failures. While a contract is in cooldown the driver
//! skips the WASM merge entirely (and skips the `ResyncRequest` amplification),
//! so a poison contract costs O(1) per inbound broadcast instead of O(WASM
//! merge). A successful merge clears the entry immediately.
//!
//! Two independent skip signals, both cleared by a successful merge:
//!
//! - **Cooldown** ([`MergeDecision::InBackoff`]): time-based, skips *all* merges
//!   for the contract until `next_allowed`. Escalates per failure.
//! - **Known-failed-payload memoization** ([`MergeDecision::KnownFailedPayload`]):
//!   a specific `(is_delta, bytes)` payload that already failed cannot change
//!   outcome until the contract's state changes, so it is skipped even after the
//!   cooldown elapses (bounded by [`FAILED_PAYLOAD_TTL`]). Any successful merge
//!   (a state-generation bump) drops the whole entry, re-admitting every
//!   payload.
//!
//! ## Failure classes
//!
//! [`MergeFailureClass::Timeout`] (a WASM execution timeout — see
//! `ExecutorError::is_wasm_timeout`) gets a much longer base/cap than
//! [`MergeFailureClass::Invalid`] (a cheap contract-side rejection or other exec
//! error): a runaway merge that pins the single contract-handling thread is far
//! more expensive to re-attempt than a merge that returns a rejection quickly.
//! A timeout escalates the entry's class to `Timeout` and never downgrades.
//!
//! ## Bounded growth
//!
//! Modeled on [`crate::ring::update_rate_limit::UpdateRateLimiter`]: a
//! [`DashMap`] with a strict [`AtomicUsize`] size cap ([`MAX_TRACKED_CONTRACTS`])
//! so an attacker churning fresh contract ids cannot grow the map, plus a
//! periodic TTL sweep ([`Self::cleanup_expired`]) hooked into the Ring reaper.
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
//!   merge + error.

use std::collections::VecDeque;
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

/// How long a failed `(is_delta, bytes)` payload hash is remembered (10 min).
const FAILED_PAYLOAD_TTL: Duration = Duration::from_secs(10 * 60);
/// Max remembered failed-payload hashes per contract. Bounds per-entry memory
/// and caps the memoization at the most-recent distinct failing payloads.
const MAX_FAILED_PAYLOADS_PER_CONTRACT: usize = 32;
/// Hard cap on tracked contracts. At ~200 bytes/entry, 16 384 ≈ 3 MB. Bounds the
/// worst case where an attacker chooses fresh contract ids.
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

/// Failure class deciding the cooldown parameters.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum MergeFailureClass {
    /// Cheap contract-side rejection (`InvalidUpdate`) or any other non-timeout
    /// execution error (trap, OOG, deser). Base [`INVALID_BASE`].
    Invalid,
    /// The WASM merge exceeded the execution time limit. Base [`TIMEOUT_BASE`].
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

    /// `Timeout` outranks `Invalid`: a timeout escalates an entry and never
    /// downgrades.
    fn rank(self) -> u8 {
        match self {
            MergeFailureClass::Invalid => 0,
            MergeFailureClass::Timeout => 1,
        }
    }
}

/// Outcome of a pre-merge backoff check.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum MergeDecision {
    /// Not in backoff and payload not known-failed — run the merge.
    Allow,
    /// The contract is in a cooldown window — skip the merge (and any
    /// amplification, e.g. `ResyncRequest`).
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

struct Entry {
    consecutive_failures: u32,
    class: MergeFailureClass,
    next_allowed: Instant,
    last_failure: Instant,
    /// `(payload_hash, recorded_at)`, newest at back. Bounded + TTL-pruned.
    failed_payloads: VecDeque<(u64, Instant)>,
}

impl Entry {
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
}

/// Per-contract merge-failure backoff tracker. All state lives in one
/// [`DashMap`]; concurrency + bounding mirror
/// [`crate::ring::update_rate_limit::UpdateRateLimiter`].
pub(crate) struct MergeBackoff {
    entries: DashMap<ContractInstanceId, Entry>,
    /// Authoritative size counter for the strict cap (see `UpdateRateLimiter`
    /// for why `len()` can't be used under a shard guard).
    size: AtomicUsize,
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
            entries: DashMap::new(),
            size: AtomicUsize::new(0),
            max_tracked,
            time_source,
            suppressed_total: AtomicU64::new(0),
        }
    }

    /// Decide whether a merge for `contract` with the given payload should run.
    ///
    /// `KnownFailedPayload` takes precedence over `InBackoff` (a specific failed
    /// payload is skipped even once the cooldown elapses). Bumps
    /// `suppressed_total` on any non-`Allow` outcome. Does NOT create an entry
    /// for an untracked contract (the common Allow path is a single shard read).
    pub fn check(&self, contract: &ContractInstanceId, payload_hash: u64) -> MergeDecision {
        let now = self.time_source.now();
        let Some(mut entry) = self.entries.get_mut(contract) else {
            return MergeDecision::Allow;
        };
        entry.prune_payloads(now);
        if entry
            .failed_payloads
            .iter()
            .any(|(h, _)| *h == payload_hash)
        {
            self.suppressed_total.fetch_add(1, Ordering::Relaxed);
            return MergeDecision::KnownFailedPayload;
        }
        if now < entry.next_allowed {
            self.suppressed_total.fetch_add(1, Ordering::Relaxed);
            return MergeDecision::InBackoff;
        }
        MergeDecision::Allow
    }

    /// Record a failed merge, escalating the contract's cooldown and memoizing
    /// the payload. A `Timeout` failure escalates the entry's class (and never
    /// downgrades). At the tracked-contracts cap, a failure for a *new* contract
    /// is dropped (graceful degradation — bounded memory under id churn).
    pub fn record_failure(
        &self,
        contract: &ContractInstanceId,
        class: MergeFailureClass,
        payload_hash: u64,
    ) {
        let now = self.time_source.now();
        use dashmap::mapref::entry::Entry as DEntry;
        match self.entries.entry(*contract) {
            DEntry::Occupied(mut occ) => {
                let entry = occ.get_mut();
                Self::apply_failure(entry, class, payload_hash, now);
            }
            DEntry::Vacant(vac) => {
                // Reserve a slot before inserting (strict cap, see UpdateRateLimiter).
                let prev = self.size.fetch_add(1, Ordering::Relaxed);
                if prev >= self.max_tracked {
                    self.size.fetch_sub(1, Ordering::Relaxed);
                    return;
                }
                let mut entry = Entry {
                    consecutive_failures: 0,
                    class,
                    next_allowed: now,
                    last_failure: now,
                    failed_payloads: VecDeque::new(),
                };
                Self::apply_failure(&mut entry, class, payload_hash, now);
                vac.insert(entry);
            }
        }
    }

    fn apply_failure(entry: &mut Entry, class: MergeFailureClass, payload_hash: u64, now: Instant) {
        // Escalate class upward only (Timeout dominates Invalid).
        if class.rank() > entry.class.rank() {
            entry.class = class;
        }
        entry.consecutive_failures = entry.consecutive_failures.saturating_add(1);
        entry.last_failure = now;

        let cooldown = Self::cooldown_for(entry.class, entry.consecutive_failures);
        entry.next_allowed = now + cooldown;

        // Memoize the failing payload (dedup + bounded + TTL-pruned).
        entry.prune_payloads(now);
        if !entry
            .failed_payloads
            .iter()
            .any(|(h, _)| *h == payload_hash)
        {
            if entry.failed_payloads.len() >= MAX_FAILED_PAYLOADS_PER_CONTRACT {
                entry.failed_payloads.pop_front();
            }
            entry.failed_payloads.push_back((payload_hash, now));
        }
    }

    /// `base * 2^(failures-1)`, capped, with ±20% jitter (seeded `GlobalRng`, so
    /// reproducible under simulation). Mirrors `ExponentialBackoff` + the
    /// code-style jitter rule.
    fn cooldown_for(class: MergeFailureClass, consecutive_failures: u32) -> Duration {
        let base = class.base();
        let cap = class.cap();
        let exponent = consecutive_failures.saturating_sub(1).min(20);
        let raw = base.saturating_mul(1u32 << exponent.min(30));
        let capped = raw.min(cap);
        let jitter: f64 = GlobalRng::random_range(0.8_f64..=1.2_f64);
        capped.mul_f64(jitter)
    }

    /// Record a successful merge: the contract's state advanced, so every
    /// remembered failure is stale. Drops the entry entirely (clears both the
    /// cooldown and the payload memoization).
    pub fn record_success(&self, contract: &ContractInstanceId) {
        if self.entries.remove(contract).is_some() {
            self.size.fetch_sub(1, Ordering::Relaxed);
        }
    }

    /// Drop entries whose cooldown has elapsed AND that have been idle longer
    /// than [`CLEANUP_AGE`]; prune per-entry expired payload memos. Call from the
    /// Ring reaper. Bounds memory as idle contracts roll off.
    pub fn cleanup_expired(&self) {
        let now = self.time_source.now();
        let mut removed = 0usize;
        self.entries.retain(|_, entry| {
            let past_cooldown = now >= entry.next_allowed;
            let idle = now.saturating_duration_since(entry.last_failure) > CLEANUP_AGE;
            if past_cooldown && idle {
                removed += 1;
                return false;
            }
            entry.prune_payloads(now);
            true
        });
        if removed > 0 {
            self.size.fetch_sub(removed, Ordering::Relaxed);
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

    /// Number of contracts currently in an active cooldown window (gauge).
    /// Scans the map (bounded by [`MAX_TRACKED_CONTRACTS`]).
    #[cfg_attr(not(test), allow(dead_code))]
    pub fn contracts_in_backoff(&self) -> usize {
        let now = self.time_source.now();
        self.entries
            .iter()
            .filter(|e| now < e.value().next_allowed)
            .count()
    }

    /// Number of tracked contracts (tests / dashboard).
    #[cfg_attr(not(test), allow(dead_code))]
    pub fn len(&self) -> usize {
        self.entries.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::util::time_source::SharedMockTimeSource;

    fn mk_contract(byte: u8) -> ContractInstanceId {
        ContractInstanceId::new([byte; 32])
    }

    fn mk() -> (MergeBackoff, SharedMockTimeSource) {
        let ts = SharedMockTimeSource::new();
        (MergeBackoff::new(Arc::new(ts.clone())), ts)
    }

    #[test]
    fn untracked_contract_is_allowed() {
        let (b, _ts) = mk();
        assert_eq!(b.check(&mk_contract(1), 42), MergeDecision::Allow);
        assert_eq!(b.len(), 0, "check must not create an entry");
    }

    #[test]
    fn failure_puts_contract_in_backoff() {
        let (b, _ts) = mk();
        let c = mk_contract(1);
        b.record_failure(&c, MergeFailureClass::Invalid, 1);
        // A DIFFERENT payload during cooldown is InBackoff (time-based skip).
        assert_eq!(b.check(&c, 2), MergeDecision::InBackoff);
        assert_eq!(b.suppressed_total(), 1);
    }

    #[test]
    fn success_clears_backoff() {
        let (b, _ts) = mk();
        let c = mk_contract(1);
        b.record_failure(&c, MergeFailureClass::Invalid, 1);
        assert!(!b.check(&c, 2).is_allowed());
        b.record_success(&c);
        assert_eq!(b.check(&c, 2), MergeDecision::Allow);
        assert_eq!(
            b.check(&c, 1),
            MergeDecision::Allow,
            "memoized payload cleared too"
        );
        assert_eq!(b.len(), 0);
    }

    #[test]
    fn known_failed_payload_skipped_even_after_cooldown() {
        let (b, ts) = mk();
        let c = mk_contract(1);
        b.record_failure(&c, MergeFailureClass::Invalid, 99);
        // Advance well past the Invalid cooldown (base 30s + jitter) so the
        // time-based InBackoff no longer applies.
        ts.advance_time(Duration::from_secs(60));
        // The specific failed payload is still skipped (memoization) ...
        assert_eq!(b.check(&c, 99), MergeDecision::KnownFailedPayload);
        // ... but a fresh payload is now allowed (cooldown elapsed).
        assert_eq!(b.check(&c, 7), MergeDecision::Allow);
    }

    #[test]
    fn known_failed_payload_expires_after_ttl() {
        let (b, ts) = mk();
        let c = mk_contract(1);
        b.record_failure(&c, MergeFailureClass::Invalid, 99);
        ts.advance_time(FAILED_PAYLOAD_TTL + Duration::from_secs(1));
        // Past the memo TTL the payload is no longer KnownFailedPayload; the
        // cooldown is also long past, so it's Allow.
        assert_eq!(b.check(&c, 99), MergeDecision::Allow);
    }

    #[test]
    fn timeout_class_gets_longer_cooldown_than_invalid() {
        let (b, ts) = mk();
        let invalid = mk_contract(1);
        let timeout = mk_contract(2);
        b.record_failure(&invalid, MergeFailureClass::Invalid, 1);
        b.record_failure(&timeout, MergeFailureClass::Timeout, 1);

        // After 60s (> Invalid base 30s+jitter, < Timeout base 120s), the
        // Invalid contract's cooldown has elapsed but the Timeout's has not.
        ts.advance_time(Duration::from_secs(60));
        assert_eq!(
            b.check(&invalid, 555),
            MergeDecision::Allow,
            "invalid-class cooldown (30s) should have elapsed by 60s"
        );
        assert_eq!(
            b.check(&timeout, 555),
            MergeDecision::InBackoff,
            "timeout-class cooldown (120s) should still be active at 60s"
        );
    }

    #[test]
    fn timeout_escalates_class_and_never_downgrades() {
        let (b, ts) = mk();
        let c = mk_contract(1);
        // First an Invalid failure, then a Timeout: the entry escalates to
        // Timeout params and stays there even if a later Invalid arrives.
        b.record_failure(&c, MergeFailureClass::Invalid, 1);
        b.record_failure(&c, MergeFailureClass::Timeout, 2);
        b.record_failure(&c, MergeFailureClass::Invalid, 3);
        // 60s < Timeout base — still in backoff, proving it kept Timeout params.
        ts.advance_time(Duration::from_secs(60));
        assert_eq!(b.check(&c, 999), MergeDecision::InBackoff);
    }

    #[test]
    fn cooldown_escalates_with_consecutive_failures() {
        let (b, ts) = mk();
        let c = mk_contract(1);
        // First failure: ~30s cooldown. Let it elapse.
        b.record_failure(&c, MergeFailureClass::Invalid, 1);
        ts.advance_time(Duration::from_secs(40));
        assert_eq!(b.check(&c, 2), MergeDecision::Allow);
        // Second failure: ~60s cooldown (base * 2). 40s is NOT enough now.
        b.record_failure(&c, MergeFailureClass::Invalid, 3);
        ts.advance_time(Duration::from_secs(40));
        assert_eq!(
            b.check(&c, 4),
            MergeDecision::InBackoff,
            "second consecutive failure must escalate the cooldown past 40s"
        );
    }

    #[test]
    fn cooldown_jitter_stays_within_bounds() {
        // ±20% jitter: a first Invalid failure must land in [24s, 36s].
        for seed_byte in 0..32u8 {
            let (b, _ts) = mk();
            let c = mk_contract(seed_byte);
            b.record_failure(&c, MergeFailureClass::Invalid, 1);
            let entry = b.entries.get(&c).unwrap();
            let cooldown = entry
                .next_allowed
                .saturating_duration_since(entry.last_failure);
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
        // Many consecutive failures must saturate at INVALID_CAP (+jitter),
        // never overflow.
        for i in 0..40u64 {
            b.record_failure(&c, MergeFailureClass::Invalid, i);
        }
        let entry = b.entries.get(&c).unwrap();
        let cooldown = entry
            .next_allowed
            .saturating_duration_since(entry.last_failure);
        // Capped at 30 min, +20% jitter ceiling = 36 min.
        assert!(
            cooldown <= INVALID_CAP.mul_f64(1.2),
            "escalating cooldown {cooldown:?} must stay capped near INVALID_CAP"
        );
        assert!(cooldown >= INVALID_CAP.mul_f64(0.8));
    }

    #[test]
    fn tracked_contracts_are_capped() {
        let ts = SharedMockTimeSource::new();
        let b = MergeBackoff::with_max(Arc::new(ts.clone()), 4);
        for i in 0..4u8 {
            b.record_failure(&mk_contract(i), MergeFailureClass::Invalid, 1);
        }
        assert_eq!(b.len(), 4);
        // 5th distinct contract is dropped (not tracked) — bounded memory.
        b.record_failure(&mk_contract(99), MergeFailureClass::Invalid, 1);
        assert_eq!(b.len(), 4, "cap must bound the tracked-contract count");
        // But an already-tracked contract keeps escalating.
        b.record_failure(&mk_contract(0), MergeFailureClass::Invalid, 2);
        assert_eq!(b.len(), 4);
    }

    #[test]
    fn failed_payloads_are_bounded_per_contract() {
        let (b, _ts) = mk();
        let c = mk_contract(1);
        for i in 0..(MAX_FAILED_PAYLOADS_PER_CONTRACT as u64 + 10) {
            b.record_failure(&c, MergeFailureClass::Invalid, i);
        }
        let entry = b.entries.get(&c).unwrap();
        assert!(
            entry.failed_payloads.len() <= MAX_FAILED_PAYLOADS_PER_CONTRACT,
            "per-contract failed-payload history must stay bounded"
        );
    }

    #[test]
    fn cleanup_removes_idle_expired_entries() {
        let (b, ts) = mk();
        let c = mk_contract(1);
        b.record_failure(&c, MergeFailureClass::Invalid, 1);
        assert_eq!(b.len(), 1);
        // Past cooldown AND past CLEANUP_AGE → removed.
        ts.advance_time(CLEANUP_AGE + Duration::from_secs(1));
        b.cleanup_expired();
        assert_eq!(b.len(), 0, "idle expired entry must be swept");
        assert_eq!(b.size.load(Ordering::Relaxed), 0);
    }

    #[test]
    fn cleanup_preserves_active_cooldown() {
        let (b, ts) = mk();
        let c = mk_contract(1);
        b.record_failure(&c, MergeFailureClass::Timeout, 1);
        // Well within the 2h Timeout cooldown.
        ts.advance_time(Duration::from_secs(60));
        b.cleanup_expired();
        assert_eq!(b.len(), 1, "an entry still in cooldown must not be swept");
    }

    #[test]
    fn fork_oscillation_escalates_into_backoff_without_reset() {
        // Regression for the #4861 semantic fork-oscillation poison class: two
        // divergent forks each reject the other's delta, and every ResyncResponse
        // full-state apply flips the node to the other fork — a "success" that
        // proves nothing about convergence. Because a resync apply does NOT reset
        // the backoff (node.rs) and a full-state apply doesn't reset it either
        // (only a successful DELTA merge does), the alternating delta failures
        // accumulate and the contract enters backoff, suppressing further merges
        // (and thus the ResyncRequest amplification). If a resync/full-state
        // apply reset the entry, the count would return to 1 every cycle and the
        // loop would never be contained.
        let (b, ts) = mk();
        let c = mk_contract(1);
        let fork_a_delta = 0xAAAA;
        let fork_b_delta = 0xBBBB;

        // Cycle 1: a fork-B delta fails to apply against fork-A state.
        b.record_failure(&c, MergeFailureClass::Invalid, fork_b_delta);
        // ... a resync flips us to fork B (NO record_success — that's the fix).
        // Cycle 2: a fork-A delta now fails against fork-B state. Let the first
        // (~30s) cooldown lapse so this models a genuine later re-attempt.
        ts.advance_time(Duration::from_secs(40));
        b.record_failure(&c, MergeFailureClass::Invalid, fork_a_delta);

        // Two consecutive failures without a reset ⇒ escalated (~60s) cooldown.
        // 40s later a fresh (non-memoized) delta is still suppressed, proving the
        // count escalated past a single-failure 30s cooldown rather than resetting
        // each cycle.
        ts.advance_time(Duration::from_secs(40));
        assert_eq!(
            b.check(&c, 0xCCCC),
            MergeDecision::InBackoff,
            "alternating fork deltas must escalate the backoff (no resync reset)"
        );
    }

    #[test]
    fn contracts_in_backoff_gauge() {
        let (b, ts) = mk();
        b.record_failure(&mk_contract(1), MergeFailureClass::Invalid, 1);
        b.record_failure(&mk_contract(2), MergeFailureClass::Timeout, 1);
        assert_eq!(b.contracts_in_backoff(), 2);
        // Advance past the Invalid cooldown only.
        ts.advance_time(Duration::from_secs(60));
        assert_eq!(
            b.contracts_in_backoff(),
            1,
            "only the still-cooling Timeout contract should count"
        );
    }
}
