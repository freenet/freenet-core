//! Per-user operation rate limiting for HOSTED mode (#4561, P5 of #4381).
//!
//! Bounds how fast a single hosted user (one `userToken` ⇒ one [`UserId`]) can
//! issue contract operations (GET / PUT / UPDATE / SUBSCRIBE) so one visitor
//! cannot flood the node's executor and network. Over-rate requests are
//! REJECTED at the WebSocket boundary BEFORE the request is forwarded to the
//! node event loop, so a rejection costs no executor or network work; the
//! client is expected to retry shortly.
//!
//! # Why this is hosted-only
//!
//! The limiter is consulted ONLY when a connection carries a
//! [`UserSecretContext`](crate::wasm_runtime::UserSecretContext) — i.e. hosted
//! mode is on AND the connection presented a user token. Local / non-hosted /
//! tokenless connections derive no context, so they are NEVER rate-limited:
//! the single-user node behaves byte-for-byte as before (#4381's "never break
//! single-user default" invariant).
//!
//! # Server-level shared state, not a process global
//!
//! The limiter is owned as per-node server state (an `Arc` cloned into each
//! connection's request path), NOT a process-global `LazyLock`. The WS server
//! is per-node, so this naturally isolates nodes (a multi-node simulation in
//! one process never collides) while sharing one user's bucket across all of
//! that user's concurrent connections — the combined rate is what's bounded.
//!
//! # Concurrency
//!
//! `process_client_request` runs CONCURRENTLY across many WS connection tasks,
//! so the bucket must admit at most `capacity` tokens with NO check-then-act
//! race that lets two concurrent requests both pass when only one token is
//! left. Each bucket guards its `(tokens, last_refill)` pair behind a short
//! [`std::sync::Mutex`]; [`TokenBucket::try_acquire`] refills and decrements
//! inside the SAME lock, so the decision is atomic. The map itself is a
//! [`DashMap`], so distinct users never contend, and the per-bucket critical
//! section is a few arithmetic ops (no `.await`, no allocation).
//!
//! Token-bucket math (capacity = `burst`, refill = `rate` tokens/sec):
//! refill `elapsed * rate` tokens (capped at `capacity`) on each acquire, then
//! admit iff at least one whole token is available.

use std::sync::{Arc, Mutex};

use dashmap::DashMap;
use tokio::time::Instant;

use crate::wasm_runtime::UserId;

/// Default sustained per-user operation rate (requests/second) in hosted mode.
/// `0` disables operation rate limiting entirely.
pub const DEFAULT_PER_USER_OP_RATE_LIMIT: u64 = 10;

/// Default per-user burst capacity (max tokens that can accumulate). A user
/// who has been idle can issue up to this many ops back-to-back before being
/// throttled to the sustained [`DEFAULT_PER_USER_OP_RATE_LIMIT`].
pub const DEFAULT_PER_USER_OP_BURST: u64 = 50;

/// Minimum seconds between hosted-export downloads per user (export is far more
/// expensive than a single op, so it gets a separate, tighter limit). `0`
/// disables export rate limiting.
pub const DEFAULT_PER_USER_EXPORT_MIN_INTERVAL_SECS: u64 = 10;

/// A single user's token bucket. The `(tokens, last_refill)` pair is guarded by
/// a `Mutex` so refill+decrement is one atomic decision (no check-then-act race
/// across concurrent connection tasks sharing this user's bucket).
struct TokenBucket {
    /// `capacity` (= burst) and `refill_per_sec` (= sustained rate) are fixed at
    /// construction; only `state` mutates.
    capacity: f64,
    refill_per_sec: f64,
    state: Mutex<BucketState>,
}

struct BucketState {
    /// Fractional tokens currently available (≤ `capacity`).
    tokens: f64,
    /// When `tokens` was last refilled.
    last_refill: Instant,
}

impl TokenBucket {
    /// A fresh bucket starts FULL (`capacity` tokens) so a brand-new user can
    /// immediately issue a burst — the limiter only ever slows a user down, it
    /// never makes the first request wait.
    fn new(capacity: u64, refill_per_sec: u64, now: Instant) -> Self {
        let capacity = capacity as f64;
        Self {
            capacity,
            refill_per_sec: refill_per_sec as f64,
            state: Mutex::new(BucketState {
                tokens: capacity,
                last_refill: now,
            }),
        }
    }

    /// Try to spend one token. Returns `true` if admitted, `false` if the user
    /// is over-rate. Refill and decrement happen inside one lock so the
    /// decision is atomic across concurrent callers.
    fn try_acquire(&self, now: Instant) -> bool {
        let mut state = self.state.lock().unwrap_or_else(|p| p.into_inner());
        // Refill based on elapsed time since the last refill. `saturating_*`
        // isn't available on Instant arithmetic, but `now` is monotonic and
        // `>= last_refill` for any single bucket (Instants only move forward),
        // so `duration_since` is well-defined; guard anyway in case a paused
        // test clock hands back an equal instant (elapsed = 0, no refill).
        let elapsed = now.saturating_duration_since(state.last_refill);
        state.last_refill = now;
        if self.refill_per_sec > 0.0 {
            state.tokens =
                (state.tokens + elapsed.as_secs_f64() * self.refill_per_sec).min(self.capacity);
        }
        if state.tokens >= 1.0 {
            state.tokens -= 1.0;
            true
        } else {
            false
        }
    }
}

/// Per-user operation + export rate limiter, owned as per-node server state.
///
/// Cheap to clone (`Arc` inside): cloned once into each WS connection's request
/// path and once onto the HTTP export router. All clones share the same
/// underlying maps, so a user's multiple connections share one bucket.
#[derive(Clone)]
pub(crate) struct UserOpRateLimiter {
    inner: Arc<Inner>,
}

struct Inner {
    /// Sustained op rate (tokens/sec) and burst capacity. `op_rate == 0`
    /// disables op rate limiting (every op is admitted).
    op_rate: u64,
    op_burst: u64,
    /// Per-user op token buckets, created lazily on first op from a user.
    op_buckets: DashMap<UserId, TokenBucket>,
    /// Minimum interval between exports per user; `0` disables export limiting.
    export_min_interval_secs: u64,
    /// Last successful-export instant per user. Created on first export.
    last_export: DashMap<UserId, Instant>,
}

impl UserOpRateLimiter {
    /// Build a limiter from operator config. `op_rate == 0` disables op
    /// limiting; `export_min_interval_secs == 0` disables export limiting.
    pub(crate) fn new(op_rate: u64, op_burst: u64, export_min_interval_secs: u64) -> Self {
        Self {
            inner: Arc::new(Inner {
                op_rate,
                // A 0 burst with a non-zero rate would reject everything (the
                // bucket could never hold a whole token). Clamp burst up to at
                // least 1 so an operator who sets only the rate still gets a
                // working limiter rather than a deny-all.
                op_burst: op_burst.max(1),
                op_buckets: DashMap::new(),
                export_min_interval_secs,
                last_export: DashMap::new(),
            }),
        }
    }

    /// `true` if op rate limiting is active (non-zero configured rate).
    pub(crate) fn op_limiting_enabled(&self) -> bool {
        self.inner.op_rate > 0
    }

    /// Try to admit one operation for `user`. Returns `true` to allow, `false`
    /// to reject (over-rate). Always allows when op limiting is disabled.
    ///
    /// Concurrency-safe: the per-bucket `Mutex` makes refill+decrement atomic,
    /// so N concurrent calls with only M < N tokens available admit exactly M.
    pub(crate) fn try_acquire_op(&self, user: &UserId) -> bool {
        self.try_acquire_op_at(user, Instant::now())
    }

    /// `try_acquire_op` with an injected clock for deterministic tests.
    pub(crate) fn try_acquire_op_at(&self, user: &UserId, now: Instant) -> bool {
        if self.inner.op_rate == 0 {
            return true;
        }
        // `entry` so the lazy insert is atomic w.r.t. the map: two concurrent
        // first-requests from the same user share one bucket rather than racing
        // to create two. The created bucket starts full, then this same call
        // immediately spends its first token below.
        let bucket = self
            .inner
            .op_buckets
            .entry(*user)
            .or_insert_with(|| TokenBucket::new(self.inner.op_burst, self.inner.op_rate, now));
        bucket.try_acquire(now)
    }

    /// Try to admit one export for `user`. Returns `true` to allow (and records
    /// the export time), `false` to reject (too soon since the last one).
    /// Always allows when export limiting is disabled.
    pub(crate) fn try_acquire_export(&self, user: &UserId) -> bool {
        self.try_acquire_export_at(user, Instant::now())
    }

    /// `try_acquire_export` with an injected clock for deterministic tests.
    pub(crate) fn try_acquire_export_at(&self, user: &UserId, now: Instant) -> bool {
        let min_interval = self.inner.export_min_interval_secs;
        if min_interval == 0 {
            return true;
        }
        // Atomic test-and-set on the map entry so two concurrent exports from
        // one user can't both pass: whoever holds the entry guard decides, and
        // only an admitted export advances `last_export`.
        let mut entry = self.inner.last_export.entry(*user).or_insert(
            // Seed far enough in the past that the FIRST export is always
            // admitted (now - min_interval), then overwritten below.
            now - std::time::Duration::from_secs(min_interval),
        );
        let elapsed = now.saturating_duration_since(*entry);
        if elapsed.as_secs() >= min_interval {
            *entry = now;
            true
        } else {
            false
        }
    }

    /// Number of distinct users currently tracked (op buckets). Test-only
    /// visibility into map growth.
    #[cfg(test)]
    pub(crate) fn tracked_op_users(&self) -> usize {
        self.inner.op_buckets.len()
    }
}

/// A snapshot of the configured limits, used only for constructing the limiter
/// from `WebsocketApiConfig`. Keeps the three values bundled so the call sites
/// can't transpose them.
#[derive(Clone, Copy, Debug)]
pub(crate) struct UserOpRateLimitConfig {
    pub op_rate: u64,
    pub op_burst: u64,
    pub export_min_interval_secs: u64,
}

impl UserOpRateLimitConfig {
    pub(crate) fn build(self) -> UserOpRateLimiter {
        UserOpRateLimiter::new(self.op_rate, self.op_burst, self.export_min_interval_secs)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn uid(byte: u8) -> UserId {
        UserId::new([byte; 32])
    }

    #[tokio::test(start_paused = true)]
    async fn under_rate_always_passes() {
        // rate 10/s, burst 50. At ~1 req/sec we never exhaust the bucket.
        let limiter = UserOpRateLimiter::new(10, 50, 0);
        let user = uid(1);
        let mut now = Instant::now();
        for _ in 0..200 {
            assert!(
                limiter.try_acquire_op_at(&user, now),
                "under-rate must pass"
            );
            now += std::time::Duration::from_millis(1000);
        }
    }

    #[tokio::test(start_paused = true)]
    async fn burst_then_reject() {
        // burst = 5: 5 back-to-back ops pass, the 6th (same instant) is rejected.
        let limiter = UserOpRateLimiter::new(10, 5, 0);
        let user = uid(2);
        let now = Instant::now();
        for i in 0..5 {
            assert!(
                limiter.try_acquire_op_at(&user, now),
                "burst op {i} should pass"
            );
        }
        assert!(
            !limiter.try_acquire_op_at(&user, now),
            "op beyond burst capacity must be rejected"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn tokens_refill_over_time() {
        // burst = 5, rate = 10/s. Exhaust the burst, then advance 1s → 10 tokens
        // would refill but cap at burst (5), so 5 more pass and the 6th rejects.
        let limiter = UserOpRateLimiter::new(10, 5, 0);
        let user = uid(3);
        let t0 = Instant::now();
        for _ in 0..5 {
            assert!(limiter.try_acquire_op_at(&user, t0));
        }
        assert!(!limiter.try_acquire_op_at(&user, t0), "burst exhausted");

        // Advance 0.5s → 0.5 * 10 = 5 tokens refilled (== burst cap).
        let t1 = t0 + std::time::Duration::from_millis(500);
        for i in 0..5 {
            assert!(
                limiter.try_acquire_op_at(&user, t1),
                "refilled op {i} should pass"
            );
        }
        assert!(
            !limiter.try_acquire_op_at(&user, t1),
            "refill is capped at burst, so the 6th rejects"
        );

        // Partial refill: advance 0.1s → 1 token.
        let t2 = t1 + std::time::Duration::from_millis(100);
        assert!(limiter.try_acquire_op_at(&user, t2), "1 token refilled");
        assert!(!limiter.try_acquire_op_at(&user, t2), "only 1 refilled");
    }

    #[tokio::test(start_paused = true)]
    async fn disabled_never_limits() {
        // rate 0 disables op limiting: a flood always passes and no buckets
        // are even created.
        let limiter = UserOpRateLimiter::new(0, 50, 0);
        assert!(!limiter.op_limiting_enabled());
        let user = uid(4);
        let now = Instant::now();
        for _ in 0..10_000 {
            assert!(limiter.try_acquire_op_at(&user, now));
        }
        assert_eq!(
            limiter.tracked_op_users(),
            0,
            "disabled limiter must not allocate buckets"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn distinct_users_have_independent_buckets() {
        let limiter = UserOpRateLimiter::new(10, 3, 0);
        let a = uid(10);
        let b = uid(11);
        let now = Instant::now();
        // Exhaust A's burst.
        for _ in 0..3 {
            assert!(limiter.try_acquire_op_at(&a, now));
        }
        assert!(!limiter.try_acquire_op_at(&a, now), "A exhausted");
        // B is untouched — its bucket is full.
        for _ in 0..3 {
            assert!(limiter.try_acquire_op_at(&b, now), "B independent of A");
        }
        assert!(!limiter.try_acquire_op_at(&b, now), "B now exhausted too");
    }

    #[tokio::test(start_paused = true)]
    async fn same_user_shares_one_bucket() {
        // Two "connections" are just two call sites against the same UserId.
        // Combined they share one bucket: 3 total pass, then reject.
        let limiter = UserOpRateLimiter::new(10, 3, 0);
        let user = uid(20);
        let now = Instant::now();
        assert!(limiter.try_acquire_op_at(&user, now)); // conn 1
        assert!(limiter.try_acquire_op_at(&user, now)); // conn 2
        assert!(limiter.try_acquire_op_at(&user, now)); // conn 1
        assert!(
            !limiter.try_acquire_op_at(&user, now),
            "shared bucket: 4th rejected regardless of which connection"
        );
        assert_eq!(limiter.tracked_op_users(), 1, "one bucket for one user");
    }

    /// Concurrency: spawn N threads that hammer the SAME user's bucket at the
    /// same frozen instant; exactly `burst` may be admitted, never more. This
    /// is the over-admit race the per-bucket Mutex must prevent. Uses real
    /// threads (not the paused tokio clock) so the contention is genuine.
    #[test]
    fn concurrent_acquire_admits_at_most_burst() {
        use std::sync::Barrier;
        use std::sync::atomic::{AtomicUsize, Ordering};

        const THREADS: usize = 64;
        const BURST: u64 = 10;

        let limiter = UserOpRateLimiter::new(1_000_000, BURST, 0);
        let user = uid(99);
        // Freeze a single instant so NO refill happens during the race: the
        // only tokens available are the initial `BURST`.
        let now = Instant::now();
        let admitted = Arc::new(AtomicUsize::new(0));
        let barrier = Arc::new(Barrier::new(THREADS));

        std::thread::scope(|scope| {
            for _ in 0..THREADS {
                let limiter = limiter.clone();
                let admitted = admitted.clone();
                let barrier = barrier.clone();
                let user = user;
                scope.spawn(move || {
                    // Line everyone up so the acquires actually overlap.
                    barrier.wait();
                    // Each thread tries several times to maximize contention;
                    // the total admitted across ALL threads must still be BURST.
                    for _ in 0..4 {
                        if limiter.try_acquire_op_at(&user, now) {
                            admitted.fetch_add(1, Ordering::SeqCst);
                        }
                    }
                });
            }
        });

        assert_eq!(
            admitted.load(Ordering::SeqCst),
            BURST as usize,
            "with no refill, exactly `burst` tokens may be admitted across all \
             concurrent threads — more means a check-then-act over-admit race"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn export_min_interval_enforced() {
        // 10s min interval. First export passes; a second within 10s is rejected;
        // after 10s it passes again.
        let limiter = UserOpRateLimiter::new(10, 50, 10);
        let user = uid(30);
        let t0 = Instant::now();
        assert!(
            limiter.try_acquire_export_at(&user, t0),
            "first export passes"
        );
        assert!(
            !limiter.try_acquire_export_at(&user, t0),
            "immediate second export rejected"
        );
        let t_mid = t0 + std::time::Duration::from_secs(9);
        assert!(
            !limiter.try_acquire_export_at(&user, t_mid),
            "9s later still under the 10s interval"
        );
        let t_ok = t0 + std::time::Duration::from_secs(10);
        assert!(
            limiter.try_acquire_export_at(&user, t_ok),
            "10s later export is allowed"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn export_disabled_never_limits() {
        let limiter = UserOpRateLimiter::new(10, 50, 0);
        let user = uid(31);
        let now = Instant::now();
        for _ in 0..100 {
            assert!(
                limiter.try_acquire_export_at(&user, now),
                "export limiting disabled (0): every export passes"
            );
        }
    }

    #[tokio::test(start_paused = true)]
    async fn export_buckets_are_per_user() {
        let limiter = UserOpRateLimiter::new(10, 50, 10);
        let a = uid(40);
        let b = uid(41);
        let now = Instant::now();
        assert!(limiter.try_acquire_export_at(&a, now));
        assert!(!limiter.try_acquire_export_at(&a, now), "A throttled");
        assert!(
            limiter.try_acquire_export_at(&b, now),
            "B's export interval is independent of A's"
        );
    }
}
