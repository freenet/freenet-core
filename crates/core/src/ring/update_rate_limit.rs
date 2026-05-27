//! Per-(peer, contract) UPDATE rate limiter — front-line defense against
//! the May 21 incident pattern where one peer floods one contract with
//! many UPDATE/s.
//!
//! ## What this does
//!
//! Maintains a `(sender_addr, contract_instance_id) → last_accepted_at`
//! map. When a new UPDATE arrives, check the time since the last
//! accepted UPDATE for the same pair. If under [`MIN_UPDATE_INTERVAL`],
//! reject; otherwise stamp the new time and accept.
//!
//! ## What this is NOT
//!
//! - **Not the MAD outlier detector.** That's `crate::governance` and
//!   `crate::contract::governance` — those react in *minutes* via the
//!   reaper loop. This module reacts in *milliseconds* at the receive
//!   boundary.
//! - **Not a token-bucket smoothing layer.** Sustained bursts are
//!   rejected outright, not queued. A flood pattern hits a flat ceiling.
//! - **Not aware of intent.** A "renewing subscription that happened to
//!   trigger an UPDATE" looks identical to "DOS flood at the same rate";
//!   the rate is a flat statistical threshold, and we choose it
//!   generously enough that legitimate traffic doesn't hit it.
//!
//! ## Default rate
//!
//! [`MIN_UPDATE_INTERVAL`] defaults to 100ms (≈10 UPDATEs/sec from a
//! single peer for a single contract). Realistic human-driven contract
//! updates (chat, settings change, post) are orders of magnitude slower
//! than this. The 4PjqN5… incident was producing many UPDATEs per
//! second sustained — well above this ceiling.
//!
//! ## Bounded growth
//!
//! Two layers:
//!
//! 1. **Hard cap on entry count** ([`MAX_TRACKED_PAIRS`]). When the
//!    cap is hit, new entries are rejected — an attacker churning
//!    distinct `(sender, contract)` pairs cannot grow the map past
//!    this size. Critical because the address space is attacker-
//!    chosen (32-byte contract id × any source address).
//! 2. **Periodic TTL sweep** ([`UpdateRateLimiter::cleanup`]) drops
//!    entries idle for longer than [`CLEANUP_AGE`]. Hooked into the
//!    Ring's existing reaper tick.
//!
//! The cap alone is sufficient to bound memory; the TTL sweep
//! reclaims space from genuinely-idle pairs so the cap isn't
//! prematurely reached under normal traffic.
//!
//! ## Semantic note: `sender_addr` is the immediate upstream hop
//!
//! The key uses the SocketAddr of the peer that *sent us* the
//! message — not the originator of the UPDATE transaction. This is
//! deliberate: the limiter is a **receiver-side resource guard**.
//! "How much of my CPU/memory am I willing to spend processing
//! UPDATEs from a single immediate peer for a single contract?" is
//! the question this answers. If A floods through B to reach us, B
//! is who hits our limiter — and that's correct, because B is who
//! is consuming our resources.
//!
//! Originator-level protection is a different layer (Phase 7 ban
//! enforcement, where a banned originator's traffic is rejected
//! even if relayed). The two layers compose; neither subsumes the
//! other.
//!
//! ## Design doc reference
//!
//! `docs/design/contract-hardening.md` Phase 2: *"`TrackedBackoff<(PeerId,
//! ContractInstanceId)>`. Apply at `SyncStateToPeer` emit + originator
//! UPDATE entry. Reject with typed marker."*
//!
//! Divergences from the design doc, with rationale:
//!
//! - **Flat ceiling instead of `TrackedBackoff` exponential.** A flat
//!   100ms ceiling catches the May 21 pattern; exponential repeat-
//!   offender cooldown can land as a follow-up if observation shows
//!   it's needed.
//! - **Inbound relay receive-boundary, not `SyncStateToPeer` emit.**
//!   The doc's wording targets outbound emit. We instead gate at
//!   inbound receive (4 wire-variant sites: `RequestUpdate`,
//!   `BroadcastTo`, `RequestUpdateStreaming`, `BroadcastToStreaming`).
//!   Receive-side guards a peer's own resources directly; emit-side
//!   guards against being an amplifier. Receive is the higher-value
//!   first cut; emit is a follow-up.
//! - **No typed wire-level error.** Rejected UPDATEs are dropped
//!   silently. The sender's own retry / governance scoring will
//!   detect the flood pattern from its end.

use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use dashmap::DashMap;
use freenet_stdlib::prelude::ContractInstanceId;
use tokio::time::Instant;

use crate::util::time_source::TimeSource;

/// Minimum interval between accepted UPDATEs for the same
/// `(sender, contract)` pair. Default 100ms (≈10/s). UPDATEs arriving
/// faster than this are dropped.
///
/// Calibration rationale: legitimate contract updates are
/// human-cadence (seconds to minutes between writes). The 4PjqN5…
/// May 21 incident was producing many UPDATEs/s sustained. 100ms is
/// generous enough to never trigger on a real user while blocking the
/// flood pattern instantly.
pub(crate) const MIN_UPDATE_INTERVAL: Duration = Duration::from_millis(100);

/// How long an idle `(sender, contract)` entry stays in the map before
/// being cleared by the periodic cleanup pass. Bounds memory while
/// preserving the rate-limit signal across short-term retries.
pub(crate) const CLEANUP_AGE: Duration = Duration::from_secs(5 * 60);

/// Hard upper bound on the number of tracked `(sender, contract)`
/// pairs. When the cap is reached, new pairs are rejected outright —
/// this is the "no unbounded per-key collection for attacker-influenced
/// keys" rule from `.claude/rules/code-style.md`. At 64 bytes/entry,
/// 16 384 pairs ≈ 1 MB — tiny — but bounds the worst case where an
/// attacker chooses fresh contract ids.
///
/// On reaching the cap: the per-shard guard's existing entries
/// continue to track normally (so a legitimate peer who was already
/// tracked keeps working); only NEW pairs hit the cap. Combined with
/// the TTL sweep this gives smooth recovery as idle pairs roll off.
pub(crate) const MAX_TRACKED_PAIRS: usize = 16_384;

/// Outcome of an UPDATE rate-limit check. Callers must treat any
/// non-`Allowed` variant as "drop this message at the receive
/// boundary, do not spawn a relay driver."
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum RateLimitDecision {
    /// Allowed — proceed with normal UPDATE handling. The accepted
    /// timestamp has been stamped atomically.
    Allowed,
    /// Rejected — too soon after the previous accepted UPDATE from this
    /// `(sender, contract)` pair. Carries the elapsed time vs the
    /// configured minimum for diagnostic logging.
    Rejected {
        elapsed: Duration,
        min_interval: Duration,
    },
    /// Rejected because the tracking map has hit
    /// [`MAX_TRACKED_PAIRS`] and this is a new pair. Distinct from
    /// `Rejected` so the dashboard can tell "throttling working as
    /// intended" apart from "limiter overflowing" — the latter
    /// suggests an attack pattern with churned identities.
    CapacityExceeded,
}

impl RateLimitDecision {
    /// Convenience for the common branch shape `if !decision.is_allowed()`.
    pub fn is_allowed(self) -> bool {
        matches!(self, RateLimitDecision::Allowed)
    }
}

/// Per-(sender_addr, contract_instance_id) UPDATE rate limiter.
///
/// All state lives in a single [`DashMap`] keyed by the pair. Each
/// entry stores only the `Instant` of the most recent accepted UPDATE,
/// so per-entry memory is tiny.
pub(crate) struct UpdateRateLimiter {
    last_accepted: DashMap<(SocketAddr, ContractInstanceId), Instant>,
    min_interval: Duration,
    max_tracked_pairs: usize,
    time_source: Arc<dyn TimeSource + Send + Sync>,
    /// Total accepted UPDATEs since limiter creation. Surfaced on the
    /// dashboard so operators can see the limiter's signal-to-noise.
    accepted_total: AtomicU64,
    /// Total rejected UPDATEs since limiter creation (rate-limit hits).
    rejected_total: AtomicU64,
    /// Total UPDATEs rejected because the tracking map was at capacity
    /// when a new `(sender, contract)` pair tried to register. Surfaced
    /// separately because a non-zero value suggests an attacker is
    /// churning identities to exhaust the limiter's address space.
    capacity_rejected_total: AtomicU64,
}

impl UpdateRateLimiter {
    pub fn new(time_source: Arc<dyn TimeSource + Send + Sync>) -> Self {
        Self::with_config(time_source, MIN_UPDATE_INTERVAL, MAX_TRACKED_PAIRS)
    }

    pub fn with_config(
        time_source: Arc<dyn TimeSource + Send + Sync>,
        min_interval: Duration,
        max_tracked_pairs: usize,
    ) -> Self {
        Self {
            last_accepted: DashMap::new(),
            min_interval,
            max_tracked_pairs,
            time_source,
            accepted_total: AtomicU64::new(0),
            rejected_total: AtomicU64::new(0),
            capacity_rejected_total: AtomicU64::new(0),
        }
    }

    /// Check whether an UPDATE from `sender` for `contract` is allowed
    /// right now, and atomically stamp the accepted timestamp if so.
    ///
    /// Implementation note: uses [`DashMap::entry`] so the per-shard
    /// guard is held across the time-comparison + stamp. A previous
    /// probe-then-insert version was non-atomic — Codex review of
    /// PR #4285 caught that concurrent UPDATEs for the same pair
    /// could all read the same old timestamp and all be admitted.
    /// With the guard held, exactly one admitting caller wins per
    /// `min_interval` window.
    ///
    /// If rejected, no state change is made (so a barrage of rejected
    /// attempts doesn't itself extend the rate-limit window — the
    /// existing `last_accepted` timestamp stays put).
    pub fn check_and_record(
        &self,
        sender: SocketAddr,
        contract: ContractInstanceId,
    ) -> RateLimitDecision {
        let now = self.time_source.now();
        let key = (sender, contract);

        // Two-phase check:
        //   1. If the entry already exists, hold its shard guard
        //      across the time-comparison + stamp (atomic per pair).
        //   2. For new pairs we need to enforce MAX_TRACKED_PAIRS by
        //      reading `DashMap::len()` — but `len()` walks every
        //      shard, so we MUST NOT hold any shard guard when we
        //      call it (otherwise it deadlocks on the shard we hold).
        //      Check existence with a `contains_key` probe first; if
        //      it's a new pair, check `len()` against the cap and
        //      then call `insert`. `insert` is itself atomic per
        //      shard, so the worst-case race is a transient ±N
        //      overshoot of the cap (N = number of shards) — bounded
        //      and harmless.
        use dashmap::mapref::entry::Entry;
        if self.last_accepted.contains_key(&key) {
            // Existing pair: hold the shard guard for the
            // time-comparison + stamp.
            match self.last_accepted.entry(key) {
                Entry::Occupied(mut entry) => {
                    let last = *entry.get();
                    let elapsed = now.saturating_duration_since(last);
                    if elapsed < self.min_interval {
                        self.rejected_total.fetch_add(1, Ordering::Relaxed);
                        return RateLimitDecision::Rejected {
                            elapsed,
                            min_interval: self.min_interval,
                        };
                    }
                    *entry.get_mut() = now;
                    self.accepted_total.fetch_add(1, Ordering::Relaxed);
                    RateLimitDecision::Allowed
                }
                // Race: the entry was concurrently removed between
                // the contains_key probe and the entry() call. Treat
                // as a new pair, fall through.
                Entry::Vacant(entry) => self.insert_new_pair(entry, now),
            }
        } else {
            // New pair: check cap WITHOUT holding any shard guard.
            if self.last_accepted.len() >= self.max_tracked_pairs {
                self.capacity_rejected_total.fetch_add(1, Ordering::Relaxed);
                return RateLimitDecision::CapacityExceeded;
            }
            // Insert under the shard guard. If a concurrent insert
            // beat us to the same key, treat as a normal admission
            // (the other admit wins; we just bump our counter).
            match self.last_accepted.entry(key) {
                Entry::Vacant(entry) => self.insert_new_pair(entry, now),
                Entry::Occupied(mut entry) => {
                    let last = *entry.get();
                    let elapsed = now.saturating_duration_since(last);
                    if elapsed < self.min_interval {
                        self.rejected_total.fetch_add(1, Ordering::Relaxed);
                        return RateLimitDecision::Rejected {
                            elapsed,
                            min_interval: self.min_interval,
                        };
                    }
                    *entry.get_mut() = now;
                    self.accepted_total.fetch_add(1, Ordering::Relaxed);
                    RateLimitDecision::Allowed
                }
            }
        }
    }

    fn insert_new_pair(
        &self,
        entry: dashmap::mapref::entry::VacantEntry<'_, (SocketAddr, ContractInstanceId), Instant>,
        now: Instant,
    ) -> RateLimitDecision {
        entry.insert(now);
        self.accepted_total.fetch_add(1, Ordering::Relaxed);
        RateLimitDecision::Allowed
    }

    /// Drop entries whose timestamp is older than [`CLEANUP_AGE`].
    /// Call periodically from a reaper loop to bound memory.
    pub fn cleanup(&self) {
        let now = self.time_source.now();
        let cutoff = match now.checked_sub(CLEANUP_AGE) {
            Some(t) => t,
            None => return, // clock not advanced enough to bother
        };
        self.last_accepted.retain(|_, &mut last| last >= cutoff);
    }

    /// Total accepted UPDATEs since creation. Used by the dashboard
    /// snapshot (follow-up PR — surfaces "rate limit drops in last
    /// hour" on the governance card).
    #[cfg_attr(not(test), allow(dead_code))]
    pub fn accepted_total(&self) -> u64 {
        self.accepted_total.load(Ordering::Relaxed)
    }

    /// Total rejected UPDATEs since creation. Used by the dashboard
    /// snapshot (follow-up PR).
    #[cfg_attr(not(test), allow(dead_code))]
    pub fn rejected_total(&self) -> u64 {
        self.rejected_total.load(Ordering::Relaxed)
    }

    /// Total UPDATEs rejected because the tracking map was at capacity
    /// (a new `(sender, contract)` pair tried to register when the map
    /// already held [`MAX_TRACKED_PAIRS`] pairs). A non-zero value
    /// suggests an attacker is churning identities — surface separately
    /// from `rejected_total` on the dashboard for that reason.
    #[cfg_attr(not(test), allow(dead_code))]
    pub fn capacity_rejected_total(&self) -> u64 {
        self.capacity_rejected_total.load(Ordering::Relaxed)
    }

    /// Number of tracked `(sender, contract)` pairs. Used by tests and
    /// the dashboard for size visibility.
    #[cfg_attr(not(test), allow(dead_code))]
    pub fn len(&self) -> usize {
        self.last_accepted.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::util::time_source::SharedMockTimeSource;

    fn mk_sender(byte: u8) -> SocketAddr {
        SocketAddr::from(([10, 0, 0, byte], 30000 + byte as u16))
    }

    fn mk_contract(byte: u8) -> ContractInstanceId {
        ContractInstanceId::new([byte; 32])
    }

    fn mk_limiter() -> (UpdateRateLimiter, SharedMockTimeSource) {
        let ts = SharedMockTimeSource::new();
        let limiter = UpdateRateLimiter::new(Arc::new(ts.clone()));
        (limiter, ts)
    }

    // Local alias so test bodies can keep using `ts.advance(d)`
    trait Advance {
        fn advance(&self, d: Duration);
    }
    impl Advance for SharedMockTimeSource {
        fn advance(&self, d: Duration) {
            self.advance_time(d);
        }
    }

    #[test]
    fn first_update_for_pair_is_allowed() {
        let (l, _ts) = mk_limiter();
        let d = l.check_and_record(mk_sender(1), mk_contract(1));
        assert_eq!(d, RateLimitDecision::Allowed);
        assert_eq!(l.accepted_total(), 1);
        assert_eq!(l.rejected_total(), 0);
    }

    #[test]
    fn second_update_within_min_interval_is_rejected() {
        let (l, ts) = mk_limiter();
        assert!(
            l.check_and_record(mk_sender(1), mk_contract(1))
                .is_allowed()
        );
        // Only 10ms later — well under 100ms default.
        ts.advance(Duration::from_millis(10));
        let d = l.check_and_record(mk_sender(1), mk_contract(1));
        assert!(
            matches!(d, RateLimitDecision::Rejected { .. }),
            "second UPDATE 10ms after first must be rejected, got {d:?}"
        );
        assert_eq!(l.accepted_total(), 1);
        assert_eq!(l.rejected_total(), 1);
    }

    #[test]
    fn update_after_min_interval_is_allowed() {
        let (l, ts) = mk_limiter();
        assert!(
            l.check_and_record(mk_sender(1), mk_contract(1))
                .is_allowed()
        );
        // 200ms later — past the 100ms default.
        ts.advance(Duration::from_millis(200));
        let d = l.check_and_record(mk_sender(1), mk_contract(1));
        assert_eq!(d, RateLimitDecision::Allowed);
        assert_eq!(l.accepted_total(), 2);
        assert_eq!(l.rejected_total(), 0);
    }

    #[test]
    fn different_senders_same_contract_independent() {
        let (l, ts) = mk_limiter();
        // Sender 1 accepts.
        assert!(
            l.check_and_record(mk_sender(1), mk_contract(1))
                .is_allowed()
        );
        // Sender 2 immediately also accepts — different key.
        ts.advance(Duration::from_millis(1));
        assert!(
            l.check_and_record(mk_sender(2), mk_contract(1))
                .is_allowed()
        );
        // Sender 1 retry 1ms later still rejected.
        let d = l.check_and_record(mk_sender(1), mk_contract(1));
        assert!(matches!(d, RateLimitDecision::Rejected { .. }));
    }

    #[test]
    fn same_sender_different_contracts_independent() {
        let (l, _ts) = mk_limiter();
        assert!(
            l.check_and_record(mk_sender(1), mk_contract(1))
                .is_allowed()
        );
        // Same sender, different contract — independent rate limit.
        assert!(
            l.check_and_record(mk_sender(1), mk_contract(2))
                .is_allowed()
        );
        assert_eq!(l.accepted_total(), 2);
    }

    #[test]
    fn rejected_attempts_do_not_extend_window() {
        // If a flooding peer keeps trying every 10ms, we want the
        // first attempt past the 100ms window to succeed — i.e. the
        // rejected attempts MUST NOT push the last_accepted timestamp
        // forward. Otherwise a sustained flood would lock the pair out
        // indefinitely (the existing peer would never recover).
        let (l, ts) = mk_limiter();
        assert!(
            l.check_and_record(mk_sender(1), mk_contract(1))
                .is_allowed()
        );
        // 9 sub-100ms attempts, all rejected.
        for _ in 0..9 {
            ts.advance(Duration::from_millis(10));
            assert!(
                !l.check_and_record(mk_sender(1), mk_contract(1))
                    .is_allowed()
            );
        }
        // Now we're at 90ms — still rejected.
        ts.advance(Duration::from_millis(5));
        assert!(
            !l.check_and_record(mk_sender(1), mk_contract(1))
                .is_allowed()
        );
        // One more advance puts us past 100ms from the FIRST accept.
        ts.advance(Duration::from_millis(10));
        assert!(
            l.check_and_record(mk_sender(1), mk_contract(1))
                .is_allowed(),
            "after 105ms+ from original accept, next attempt MUST be allowed — \
             rejected attempts must not have moved the window forward"
        );
    }

    #[test]
    fn cleanup_removes_stale_entries() {
        let (l, ts) = mk_limiter();
        l.check_and_record(mk_sender(1), mk_contract(1));
        l.check_and_record(mk_sender(2), mk_contract(2));
        assert_eq!(l.len(), 2);

        // Advance past CLEANUP_AGE.
        ts.advance(CLEANUP_AGE + Duration::from_secs(1));
        l.cleanup();
        assert_eq!(l.len(), 0, "all stale entries must be cleared");
    }

    #[test]
    fn cleanup_preserves_fresh_entries() {
        let (l, ts) = mk_limiter();
        l.check_and_record(mk_sender(1), mk_contract(1));
        // Advance partway through the cleanup age.
        ts.advance(CLEANUP_AGE / 2);
        l.cleanup();
        assert_eq!(l.len(), 1, "fresh entry must be preserved");
    }

    #[test]
    fn counters_track_accepts_and_rejects() {
        let (l, ts) = mk_limiter();
        for i in 0..5 {
            // Five accepts: stagger by min_interval.
            ts.advance(MIN_UPDATE_INTERVAL + Duration::from_millis(1));
            assert!(
                l.check_and_record(mk_sender(1), mk_contract(1))
                    .is_allowed(),
                "iter {i}"
            );
        }
        // Three rejects: hammer with no advance.
        for _ in 0..3 {
            assert!(
                !l.check_and_record(mk_sender(1), mk_contract(1))
                    .is_allowed()
            );
        }
        assert_eq!(l.accepted_total(), 5);
        assert_eq!(l.rejected_total(), 3);
    }

    /// Pin test exhibiting the May 21 incident pattern: a single sender
    /// hammering a single contract at ~10 UPDATEs/s. With the limiter,
    /// at most ~10/s are accepted (one per MIN_UPDATE_INTERVAL window);
    /// the rest are dropped. The "real" 4PjqN5… incident was producing
    /// far more than this, so the rejection rate is overwhelming.
    #[test]
    fn may21_flood_pattern_is_throttled() {
        let (l, ts) = mk_limiter();
        let sender = mk_sender(1);
        let contract = mk_contract(1);

        // Simulate 1 second of flooding at 1ms per attempt (1000
        // attempts/s — 100× over the 10/s ceiling).
        for _ in 0..1000 {
            l.check_and_record(sender, contract);
            ts.advance(Duration::from_millis(1));
        }
        // Expected admits: floor(1000ms / 100ms) + 1 (first one is
        // unconditional) = ~11 admissions, ~989 rejections.
        let accepted = l.accepted_total();
        let rejected = l.rejected_total();
        assert!(
            (9..=12).contains(&accepted),
            "expected ~10 admits over 1s of flooding, got {accepted}"
        );
        assert_eq!(accepted + rejected, 1000);
        // The reject rate must be high — the flood is mostly dropped.
        assert!(
            rejected as f64 / 1000.0 > 0.95,
            "expected >95% rejection rate, got {}",
            rejected as f64 / 1000.0
        );
    }

    /// Pin: hard cap on tracked pairs prevents an attacker churning
    /// fresh contract identities from growing the map indefinitely.
    /// Once `MAX_TRACKED_PAIRS` is reached, new pairs get
    /// `CapacityExceeded`, never `Allowed`. Existing pairs keep
    /// working — graceful degradation, not total denial.
    #[test]
    fn capacity_exceeded_when_cap_reached() {
        // Small cap so the test is fast.
        let ts = SharedMockTimeSource::new();
        let limiter = UpdateRateLimiter::with_config(
            Arc::new(ts.clone()),
            MIN_UPDATE_INTERVAL,
            8, // tiny cap for test speed
        );

        // Fill the map with 8 distinct pairs — all should be Allowed.
        for i in 0..8 {
            let d = limiter.check_and_record(mk_sender(i + 1), mk_contract(i + 1));
            assert_eq!(d, RateLimitDecision::Allowed, "pair {i} should be allowed");
        }
        assert_eq!(limiter.len(), 8);

        // Pair #9 is a new key — must be CapacityExceeded.
        let d = limiter.check_and_record(mk_sender(99), mk_contract(99));
        assert_eq!(
            d,
            RateLimitDecision::CapacityExceeded,
            "new pair past the cap must be CapacityExceeded"
        );
        assert_eq!(limiter.capacity_rejected_total(), 1);

        // Pair #1 (already tracked) — should still work after
        // the min_interval elapses. Existing pairs aren't punished
        // by the cap.
        ts.advance(MIN_UPDATE_INTERVAL + Duration::from_millis(1));
        let d = limiter.check_and_record(mk_sender(1), mk_contract(1));
        assert_eq!(
            d,
            RateLimitDecision::Allowed,
            "existing pair must keep working at cap"
        );

        // After cleanup drops stale entries the cap recovers, but in
        // this test no time has passed so they're still fresh.
    }

    /// Pin the atomicity of `check_and_record` under concurrent
    /// callers. Without `DashMap::entry()` holding the shard guard
    /// across the time-comparison + stamp, two threads racing the
    /// same `(sender, contract)` pair could both decide `Allowed`,
    /// both increment `accepted_total`, and both spawn relay work.
    /// Codex review of PR #4285 caught this. The fixed implementation
    /// must serialize exactly one Allowed per `min_interval` window
    /// per pair.
    #[test]
    fn concurrent_check_and_record_admits_one_per_window() {
        use std::sync::{Arc as StdArc, Barrier};
        use std::thread;

        let ts = SharedMockTimeSource::new();
        let limiter = StdArc::new(UpdateRateLimiter::new(Arc::new(ts.clone())));
        let sender = mk_sender(1);
        let contract = mk_contract(1);

        const THREADS: usize = 16;
        let barrier = StdArc::new(Barrier::new(THREADS));
        let mut handles = Vec::with_capacity(THREADS);

        for _ in 0..THREADS {
            let l = limiter.clone();
            let b = barrier.clone();
            handles.push(thread::spawn(move || {
                b.wait();
                l.check_and_record(sender, contract)
            }));
        }

        let mut allowed = 0;
        let mut rejected = 0;
        for h in handles {
            match h.join().unwrap() {
                RateLimitDecision::Allowed => allowed += 1,
                RateLimitDecision::Rejected { .. } => rejected += 1,
                RateLimitDecision::CapacityExceeded => panic!("unexpected cap"),
            }
        }
        assert_eq!(
            allowed, 1,
            "exactly ONE concurrent caller must be admitted per window; \
             got {allowed} admits, {rejected} rejects"
        );
        assert_eq!(rejected, THREADS - 1);
        assert_eq!(limiter.accepted_total(), 1);
        assert_eq!(limiter.rejected_total(), (THREADS - 1) as u64);
    }

    /// Source-grep pin: the rate-limit gate at the UPDATE dispatch
    /// site in `node.rs` must cover ALL four UPDATE wire variants
    /// (`RequestUpdate`, `BroadcastTo`, `RequestUpdateStreaming`,
    /// `BroadcastToStreaming`). Codex review of #4285 caught a
    /// previous iteration that only gated `RequestUpdate`, letting
    /// a flooder bypass by switching opcode.
    ///
    /// Strategy: scrape `node.rs` for the UPDATE dispatch block and
    /// assert (a) the rate-limit call site appears, (b) it appears
    /// BEFORE any `start_relay_*` spawn, and (c) the four wire-variant
    /// names are mentioned in the same block (proving the dispatch
    /// matches on all of them).
    #[test]
    fn update_dispatch_gates_all_four_wire_variants() {
        const NODE_SRC: &str = include_str!("../node.rs");

        // Find the UPDATE handler block.
        let block_start = NODE_SRC
            .find("NetMessageV1::Update(ref op) =>")
            .expect("could not locate UPDATE dispatch block in node.rs");

        // Bound the search to a generous slice — the UPDATE block is
        // well under 4KB.
        let block_end = (block_start + 8192).min(NODE_SRC.len());
        let block = &NODE_SRC[block_start..block_end];

        // (a) the gate is invoked.
        let rate_limit_pos = block
            .find("update_rate_limiter")
            .expect("update_rate_limiter not invoked in UPDATE dispatch block");

        // (b) the gate fires BEFORE the first relay spawn — otherwise
        //     the spawn cost is paid even on rejection.
        let first_spawn_pos = block
            .find("start_relay_request_update(")
            .expect("start_relay_request_update spawn not found in block");
        assert!(
            rate_limit_pos < first_spawn_pos,
            "rate limit gate (offset {rate_limit_pos}) must appear BEFORE \
             the first relay spawn (offset {first_spawn_pos}) so rejected \
             messages don't pay the spawn cost"
        );

        // (c) all four wire variants are matched in the dispatch.
        for variant in [
            "UpdateMsg::RequestUpdate {",
            "UpdateMsg::BroadcastTo {",
            "UpdateMsg::RequestUpdateStreaming {",
            "UpdateMsg::BroadcastToStreaming {",
        ] {
            assert!(
                block.contains(variant),
                "UPDATE dispatch block missing wire variant: `{variant}`. \
                 If a new UPDATE wire variant was added, gate it through \
                 the rate limiter and update this list. If a variant was \
                 removed, update this list."
            );
        }
    }
}
