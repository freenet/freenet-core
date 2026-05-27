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
//! The `last_accepted` DashMap is bounded by a periodic cleanup task
//! that drops entries whose timestamp is older than [`CLEANUP_AGE`].
//! Caller is responsible for invoking [`UpdateRateLimiter::cleanup`]
//! periodically (the Ring's existing reaper loop is a natural place).
//!
//! ## Design doc reference
//!
//! `docs/design/contract-hardening.md` Phase 2: *"`TrackedBackoff<(PeerId,
//! ContractInstanceId)>`. Apply at `SyncStateToPeer` emit + originator
//! UPDATE entry. Reject with typed marker."*
//!
//! This MVP applies at INBOUND relay entry first (the highest-value
//! protection point: blocks a flooding peer at the door). Originator
//! and outbound-emit gates follow as separate PRs. The exponential
//! repeat-offender backoff via `TrackedBackoff` is also a follow-up;
//! the flat `MIN_UPDATE_INTERVAL` ceiling alone is sufficient for the
//! May 21 incident pattern.

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

/// Outcome of an UPDATE rate-limit check. Callers must treat
/// `Rejected` as "drop this message at the receive boundary, do not
/// spawn a relay driver."
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum RateLimitDecision {
    /// Allowed — proceed with normal UPDATE handling. The accepted
    /// timestamp has been stamped.
    Allowed,
    /// Rejected — too soon after the previous accepted UPDATE from this
    /// `(sender, contract)` pair. Carries the elapsed time vs the
    /// configured minimum for diagnostic logging.
    Rejected {
        elapsed: Duration,
        min_interval: Duration,
    },
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
    time_source: Arc<dyn TimeSource + Send + Sync>,
    /// Total accepted UPDATEs since limiter creation. Surfaced on the
    /// dashboard so operators can see the limiter's signal-to-noise.
    accepted_total: AtomicU64,
    /// Total rejected UPDATEs since limiter creation.
    rejected_total: AtomicU64,
}

impl UpdateRateLimiter {
    pub fn new(time_source: Arc<dyn TimeSource + Send + Sync>) -> Self {
        Self::with_min_interval(time_source, MIN_UPDATE_INTERVAL)
    }

    pub fn with_min_interval(
        time_source: Arc<dyn TimeSource + Send + Sync>,
        min_interval: Duration,
    ) -> Self {
        Self {
            last_accepted: DashMap::new(),
            min_interval,
            time_source,
            accepted_total: AtomicU64::new(0),
            rejected_total: AtomicU64::new(0),
        }
    }

    /// Check whether an UPDATE from `sender` for `contract` is allowed
    /// right now. If allowed, the accepted timestamp is recorded
    /// atomically; the caller can rely on this being the moment of
    /// admission for downstream rate-tracking. If rejected, no state
    /// change is made (so a barrage of rejected attempts doesn't itself
    /// extend the rate-limit window — the existing `last_accepted`
    /// timestamp stays put).
    pub fn check_and_record(
        &self,
        sender: SocketAddr,
        contract: ContractInstanceId,
    ) -> RateLimitDecision {
        let now = self.time_source.now();
        let key = (sender, contract);

        // Probe the existing timestamp without holding a guard across
        // the conditional update — avoids holding the DashMap shard
        // lock during the time-comparison + atomic-counter steps.
        let existing = self.last_accepted.get(&key).map(|r| *r);

        if let Some(last) = existing {
            let elapsed = now.saturating_duration_since(last);
            if elapsed < self.min_interval {
                self.rejected_total.fetch_add(1, Ordering::Relaxed);
                return RateLimitDecision::Rejected {
                    elapsed,
                    min_interval: self.min_interval,
                };
            }
        }

        // Allowed: stamp the new time. `insert` overwrites; if a
        // concurrent caller stamped a different time between our probe
        // and this insert, the later one wins, which is fine — the
        // rate limit is per-pair and roughly idempotent under
        // concurrent admission.
        self.last_accepted.insert(key, now);
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
}
