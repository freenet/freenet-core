//! Rate limiting for the ResyncRequest/ResyncResponse full-state protocol
//! (#4861).
//!
//! ## Why
//!
//! When a delta broadcast fails to apply, the UPDATE relay emits a
//! `ResyncRequest`, and the receiver replies with a full-state `ResyncResponse`
//! that is re-merged. For a "poison" contract whose deltas fail from many
//! senders this becomes a self-sustaining full-state storm (~16 resyncs/min per
//! contract in the incident): each failure pulls a fresh full state that fails
//! again. The per-contract merge backoff (`crate::ring::merge_backoff`) stops
//! re-running the merge, but two amplification edges remain and this module
//! caps both:
//!
//! - **Emit side** (per-contract): a hard ceiling on how often *this* node emits
//!   a `ResyncRequest` for a given contract, independent of the merge outcome.
//! - **Responder side** (per `(peer, contract)`): a hard ceiling on how often
//!   this node answers a *given peer's* `ResyncRequest` for a contract. This is
//!   the mixed-version-rollout guard: a not-yet-upgraded peer that still emits
//!   unlimited `ResyncRequest`s cannot make an upgraded peer full-state-reply in
//!   a loop.
//!
//! ## Mechanism: token bucket
//!
//! Each key owns a token bucket with capacity `burst` refilling one token per
//! `refill_interval`. A check consumes one token if available (allow) or is
//! denied. This gives a small burst for a genuinely-diverged contract to resync
//! promptly, then a flat steady-state ceiling of `1 / refill_interval`.
//!
//! ## Bounded growth
//!
//! Same discipline as [`crate::ring::update_rate_limit::UpdateRateLimiter`]: a
//! [`DashMap`] with a strict [`AtomicUsize`] size cap so an attacker churning
//! keys cannot grow the map, plus a periodic TTL sweep hooked into the Ring
//! reaper.

use std::hash::Hash;
use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::time::Duration;

use dashmap::DashMap;
use freenet_stdlib::prelude::ContractInstanceId;
use tokio::time::Instant;

use crate::util::time_source::TimeSource;

/// Per-contract minimum interval between emitted `ResyncRequest`s. A diverged
/// contract only needs an occasional full-state pull; a storm hits this ceiling.
pub(crate) const EMIT_REFILL_INTERVAL: Duration = Duration::from_secs(60);
/// Burst of emitted `ResyncRequest`s allowed before the steady-state ceiling.
pub(crate) const EMIT_BURST: f64 = 2.0;

/// Per-`(peer, contract)` minimum interval between sent `ResyncResponse`s.
pub(crate) const RESPOND_REFILL_INTERVAL: Duration = Duration::from_secs(30);
/// Burst of full-state responses to a single peer for a contract.
pub(crate) const RESPOND_BURST: f64 = 1.0;

/// Hard cap on tracked keys (per limiter). At ~64 bytes/entry, 16 384 ≈ 1 MB.
pub(crate) const MAX_TRACKED_KEYS: usize = 16_384;

/// Idle-key cleanup age. A fully-refilled key idle this long is dropped (a fresh
/// key gets a full bucket, so dropping a recovered one loses nothing).
const CLEANUP_AGE: Duration = Duration::from_secs(5 * 60);

struct Bucket {
    tokens: f64,
    last_refill: Instant,
}

impl Bucket {
    /// Refill by elapsed-time / interval, capped at `capacity`.
    fn refill(&mut self, now: Instant, capacity: f64, interval: Duration) {
        let elapsed = now.saturating_duration_since(self.last_refill);
        let gained = elapsed.as_secs_f64() / interval.as_secs_f64();
        self.tokens = (self.tokens + gained).min(capacity);
        self.last_refill = now;
    }
}

/// A token-bucket rate limiter keyed by `K`. One token per key is consumed per
/// allowed event; the bucket holds up to `capacity` and refills one token per
/// `refill_interval`.
pub(crate) struct TokenBucketLimiter<K: Eq + Hash + Clone> {
    buckets: DashMap<K, Bucket>,
    size: AtomicUsize,
    max_tracked: usize,
    capacity: f64,
    refill_interval: Duration,
    time_source: Arc<dyn TimeSource + Send + Sync>,
    allowed_total: AtomicU64,
    suppressed_total: AtomicU64,
}

impl<K: Eq + Hash + Clone> TokenBucketLimiter<K> {
    pub fn new(
        time_source: Arc<dyn TimeSource + Send + Sync>,
        capacity: f64,
        refill_interval: Duration,
        max_tracked: usize,
    ) -> Self {
        Self {
            buckets: DashMap::new(),
            size: AtomicUsize::new(0),
            max_tracked,
            capacity,
            refill_interval,
            time_source,
            allowed_total: AtomicU64::new(0),
            suppressed_total: AtomicU64::new(0),
        }
    }

    /// Check-and-consume one token for `key`. Returns `true` when the event is
    /// allowed (a token was consumed), `false` when rate-limited or the tracking
    /// map is at capacity for a new key.
    pub fn check_and_record(&self, key: K) -> bool {
        let now = self.time_source.now();
        use dashmap::mapref::entry::Entry;
        match self.buckets.entry(key) {
            Entry::Occupied(mut occ) => {
                let bucket = occ.get_mut();
                bucket.refill(now, self.capacity, self.refill_interval);
                if bucket.tokens >= 1.0 {
                    bucket.tokens -= 1.0;
                    self.allowed_total.fetch_add(1, Ordering::Relaxed);
                    true
                } else {
                    self.suppressed_total.fetch_add(1, Ordering::Relaxed);
                    false
                }
            }
            Entry::Vacant(vac) => {
                // Reserve a slot before inserting (strict cap, see UpdateRateLimiter).
                let prev = self.size.fetch_add(1, Ordering::Relaxed);
                if prev >= self.max_tracked {
                    self.size.fetch_sub(1, Ordering::Relaxed);
                    self.suppressed_total.fetch_add(1, Ordering::Relaxed);
                    return false;
                }
                // A brand-new key starts with a full bucket and spends one token.
                vac.insert(Bucket {
                    tokens: self.capacity - 1.0,
                    last_refill: now,
                });
                self.allowed_total.fetch_add(1, Ordering::Relaxed);
                true
            }
        }
    }

    /// Drop fully-refilled keys idle longer than [`CLEANUP_AGE`]. Call from the
    /// Ring reaper. A dropped key is indistinguishable from a fresh one (both get
    /// a full bucket), so this never changes behavior — only bounds memory.
    /// Retained entries are left unmutated (their `last_refill` stays the real
    /// last-use time, so the next check refills correctly).
    pub fn cleanup(&self) {
        let now = self.time_source.now();
        let capacity = self.capacity;
        let interval = self.refill_interval;
        let mut removed = 0usize;
        self.buckets.retain(|_, bucket| {
            let elapsed = now.saturating_duration_since(bucket.last_refill);
            let gained = elapsed.as_secs_f64() / interval.as_secs_f64();
            let tokens_now = (bucket.tokens + gained).min(capacity);
            let recovered = tokens_now >= capacity;
            let idle = elapsed > CLEANUP_AGE;
            if recovered && idle {
                removed += 1;
                false
            } else {
                true
            }
        });
        if removed > 0 {
            self.size.fetch_sub(removed, Ordering::Relaxed);
        }
    }

    /// Total events allowed since creation (dashboard / tests).
    #[cfg_attr(not(test), allow(dead_code))]
    pub fn allowed_total(&self) -> u64 {
        self.allowed_total.load(Ordering::Relaxed)
    }

    /// Total events suppressed (rate-limited or capacity) since creation.
    #[cfg_attr(not(test), allow(dead_code))]
    pub fn suppressed_total(&self) -> u64 {
        self.suppressed_total.load(Ordering::Relaxed)
    }

    /// Number of tracked keys (tests / dashboard).
    #[cfg_attr(not(test), allow(dead_code))]
    pub fn len(&self) -> usize {
        self.buckets.len()
    }
}

/// A per-contract `ResyncRequest` emit limiter.
pub(crate) type ResyncEmitLimiter = TokenBucketLimiter<ContractInstanceId>;
/// A per-`(peer, contract)` `ResyncResponse` responder limiter.
pub(crate) type ResyncResponseLimiter = TokenBucketLimiter<(SocketAddr, ContractInstanceId)>;

/// Build the emit-side limiter with the tuned defaults.
pub(crate) fn new_emit_limiter(
    time_source: Arc<dyn TimeSource + Send + Sync>,
) -> ResyncEmitLimiter {
    TokenBucketLimiter::new(time_source, EMIT_BURST, EMIT_REFILL_INTERVAL, MAX_TRACKED_KEYS)
}

/// Build the responder-side limiter with the tuned defaults.
pub(crate) fn new_response_limiter(
    time_source: Arc<dyn TimeSource + Send + Sync>,
) -> ResyncResponseLimiter {
    TokenBucketLimiter::new(
        time_source,
        RESPOND_BURST,
        RESPOND_REFILL_INTERVAL,
        MAX_TRACKED_KEYS,
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::util::time_source::SharedMockTimeSource;

    fn mk_contract(byte: u8) -> ContractInstanceId {
        ContractInstanceId::new([byte; 32])
    }

    fn mk_peer(byte: u8) -> SocketAddr {
        SocketAddr::from(([10, 0, 0, byte], 30000 + byte as u16))
    }

    #[test]
    fn emit_allows_burst_then_throttles() {
        let ts = SharedMockTimeSource::new();
        let l = new_emit_limiter(Arc::new(ts.clone()));
        let c = mk_contract(1);
        // Burst of 2 allowed immediately.
        assert!(l.check_and_record(c));
        assert!(l.check_and_record(c));
        // Third within the window is throttled.
        assert!(!l.check_and_record(c));
        assert_eq!(l.allowed_total(), 2);
        assert_eq!(l.suppressed_total(), 1);
    }

    #[test]
    fn emit_refills_one_per_interval() {
        let ts = SharedMockTimeSource::new();
        let l = new_emit_limiter(Arc::new(ts.clone()));
        let c = mk_contract(1);
        // Drain the burst.
        assert!(l.check_and_record(c));
        assert!(l.check_and_record(c));
        assert!(!l.check_and_record(c));
        // After one refill interval, exactly one more is allowed.
        ts.advance_time(EMIT_REFILL_INTERVAL + Duration::from_secs(1));
        assert!(l.check_and_record(c));
        assert!(!l.check_and_record(c), "only one token refilled");
    }

    #[test]
    fn emit_flood_is_bounded_to_ceil_window_over_interval() {
        // Model of `may21_flood_pattern_is_throttled`: a flood of delta
        // failures over a 5-minute window admits at most burst + window/interval
        // ResyncRequests.
        let ts = SharedMockTimeSource::new();
        let l = new_emit_limiter(Arc::new(ts.clone()));
        let c = mk_contract(1);
        // 300 attempts, one per second (5 minutes).
        for _ in 0..300 {
            l.check_and_record(c);
            ts.advance_time(Duration::from_secs(1));
        }
        // Expected admits ≈ burst(2) + 300s/60s = 2 + 5 = 7. Allow a small band.
        let allowed = l.allowed_total();
        assert!(
            (6..=8).contains(&allowed),
            "flood over 5 min must admit ~7 ResyncRequests, got {allowed}"
        );
        assert_eq!(allowed + l.suppressed_total(), 300);
    }

    #[test]
    fn emit_different_contracts_independent() {
        let ts = SharedMockTimeSource::new();
        let l = new_emit_limiter(Arc::new(ts.clone()));
        // Each contract gets its own bucket.
        assert!(l.check_and_record(mk_contract(1)));
        assert!(l.check_and_record(mk_contract(2)));
        assert!(l.check_and_record(mk_contract(1)));
        assert!(l.check_and_record(mk_contract(2)));
        // Now both are drained.
        assert!(!l.check_and_record(mk_contract(1)));
        assert!(!l.check_and_record(mk_contract(2)));
    }

    #[test]
    fn responder_burst_one_then_throttles() {
        let ts = SharedMockTimeSource::new();
        let l = new_response_limiter(Arc::new(ts.clone()));
        let key = (mk_peer(1), mk_contract(1));
        assert!(l.check_and_record(key));
        assert!(!l.check_and_record(key), "responder burst is 1");
        ts.advance_time(RESPOND_REFILL_INTERVAL + Duration::from_secs(1));
        assert!(l.check_and_record(key));
    }

    #[test]
    fn responder_flood_from_one_peer_is_bounded() {
        // A not-yet-upgraded peer floods ResyncRequests for one contract; the
        // upgraded responder answers at most ~window/interval times.
        let ts = SharedMockTimeSource::new();
        let l = new_response_limiter(Arc::new(ts.clone()));
        let key = (mk_peer(1), mk_contract(1));
        for _ in 0..300 {
            l.check_and_record(key);
            ts.advance_time(Duration::from_secs(1));
        }
        // burst(1) + 300s/30s = 1 + 10 = 11.
        let allowed = l.allowed_total();
        assert!(
            (10..=12).contains(&allowed),
            "responder flood over 5 min must send ~11 responses, got {allowed}"
        );
    }

    #[test]
    fn responder_different_peers_independent() {
        let ts = SharedMockTimeSource::new();
        let l = new_response_limiter(Arc::new(ts.clone()));
        let c = mk_contract(1);
        // Two peers requesting the same contract are independent.
        assert!(l.check_and_record((mk_peer(1), c)));
        assert!(l.check_and_record((mk_peer(2), c)));
        // Each is now throttled for its own pair.
        assert!(!l.check_and_record((mk_peer(1), c)));
        assert!(!l.check_and_record((mk_peer(2), c)));
    }

    #[test]
    fn tracked_keys_capped() {
        let ts = SharedMockTimeSource::new();
        let l = TokenBucketLimiter::new(Arc::new(ts.clone()), 1.0, RESPOND_REFILL_INTERVAL, 4);
        for i in 0..4u8 {
            assert!(l.check_and_record((mk_peer(i), mk_contract(i))));
        }
        assert_eq!(l.len(), 4);
        // 5th distinct key is dropped (capacity), suppressed.
        assert!(!l.check_and_record((mk_peer(99), mk_contract(99))));
        assert_eq!(l.len(), 4);
    }

    #[test]
    fn cleanup_removes_recovered_idle_keys() {
        let ts = SharedMockTimeSource::new();
        let l = new_emit_limiter(Arc::new(ts.clone()));
        l.check_and_record(mk_contract(1));
        l.check_and_record(mk_contract(2));
        assert_eq!(l.len(), 2);
        // Fully recover + idle past CLEANUP_AGE.
        ts.advance_time(CLEANUP_AGE + EMIT_REFILL_INTERVAL * 3);
        l.cleanup();
        assert_eq!(l.len(), 0, "recovered idle keys must be swept");
        assert_eq!(l.size.load(Ordering::Relaxed), 0);
    }

    #[test]
    fn cleanup_preserves_recently_used_keys() {
        let ts = SharedMockTimeSource::new();
        let l = new_emit_limiter(Arc::new(ts.clone()));
        l.check_and_record(mk_contract(1));
        // Only a short time passes — not idle enough to sweep.
        ts.advance_time(Duration::from_secs(10));
        l.cleanup();
        assert_eq!(l.len(), 1, "recently-used key must be preserved");
    }
}
