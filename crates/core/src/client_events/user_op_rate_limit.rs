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
///
/// Set generously (100): a heavy-room hosted user's app-load — room list +
/// per-room state + member lists + delegate secret reads across many rooms —
/// can plausibly burst past 50 ops in the first second after page load. A
/// false-positive throttle there breaks the UX worse than allowing a slightly
/// larger ONE-TIME burst; the sustained [`DEFAULT_PER_USER_OP_RATE_LIMIT`]
/// (10/sec) is the real flood cap, the burst is just the initial allowance.
pub const DEFAULT_PER_USER_OP_BURST: u64 = 100;

/// Minimum seconds between hosted-export downloads per user (export is far more
/// expensive than a single op, so it gets a separate, tighter limit). `0`
/// disables export rate limiting.
pub const DEFAULT_PER_USER_EXPORT_MIN_INTERVAL_SECS: u64 = 10;

/// Hard cap on the number of distinct users tracked in EACH per-user map
/// (`op_buckets`, `last_export`). The `userToken` is client-chosen and
/// unvalidated (`UserSecretContext::from_token` just hashes arbitrary bytes —
/// there is no registry), so an actor past the hosted gate can present an
/// unbounded number of distinct tokens. Without a cap the maps would grow
/// permanently — the limiter would be its own memory-DoS, violating the
/// repo's "cleanup must be time-bounded / no unbounded per-key collection"
/// rules. The cap is generous (a real public proxy serves far fewer concurrent
/// users than this) so legitimate traffic never triggers eviction; see
/// [`Inner::evict_op_buckets_if_needed`] for the safety property (eviction
/// never resets an actively-throttling bucket).
const MAX_TRACKED_USERS: usize = 100_000;

/// Low-water-mark divisor for eviction hysteresis. When a map exceeds its cap we
/// evict down to `cap - cap/EVICTION_HYSTERESIS_DIVISOR` (≈90% of cap) rather
/// than just back to `cap`. This is what bounds the eviction COST under a
/// new-token-churn attack: without it, a map sitting exactly at cap would run a
/// full O(n) scan on EVERY new distinct token (each insert lands at cap+1 →
/// scan → back to cap → repeat), turning a cheap request into an ~O(cap) sweep
/// — the rate-limiter would amplify CPU instead of bounding it. With the
/// low-water-mark, one sweep frees ≈cap/10 slots, so the next sweep can't
/// re-trigger until ≈cap/10 more inserts arrive; every insert in between hits
/// the cheap `len <= cap` early-return. Net: amortized O(1) eviction work per
/// insert regardless of how fast fresh tokens churn.
const EVICTION_HYSTERESIS_DIVISOR: usize = 10;

/// Given a cap, the post-eviction target size (low-water-mark): `cap - cap/10`,
/// floored at 1. A single sweep evicts down to this, leaving headroom so the
/// next ≈cap/10 inserts skip the scan entirely.
fn eviction_low_water_mark(cap: usize) -> usize {
    cap.saturating_sub(cap / EVICTION_HYSTERESIS_DIVISOR).max(1)
}

/// A single user's token bucket. The bucket `state` is guarded by a `Mutex` so
/// refill+decrement is one atomic decision (no check-then-act race across
/// concurrent connection tasks sharing this user's bucket).
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
    /// When this bucket was last touched by an acquire. Drives LRU eviction.
    last_access: Instant,
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
                last_access: now,
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
        state.last_access = now;
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

    /// A bucket is SAFE TO EVICT only when it is essentially full at `now` —
    /// i.e. the user is NOT currently being throttled. Re-creating a full
    /// bucket yields an identical full bucket, so dropping it changes nothing.
    /// A depleted (actively-throttling) bucket must NEVER be evicted: dropping
    /// it would re-create a FULL bucket and hand the flooder a fresh burst.
    ///
    /// Returns `(safe_to_evict, last_access)` computed under one lock, applying
    /// the same time-based refill as `try_acquire` so "full" reflects `now`.
    fn evictability(&self, now: Instant) -> (bool, Instant) {
        let mut state = self.state.lock().unwrap_or_else(|p| p.into_inner());
        let elapsed = now.saturating_duration_since(state.last_refill);
        state.last_refill = now;
        if self.refill_per_sec > 0.0 {
            state.tokens =
                (state.tokens + elapsed.as_secs_f64() * self.refill_per_sec).min(self.capacity);
        }
        // "Essentially full" AND holding at least one whole token. Within one
        // token of capacity means re-creation is a no-op; the `>= 1.0` clause is
        // what makes that true even at capacity == 1, where `capacity - 1.0 == 0`
        // would otherwise classify a DEPLETED (tokens == 0), actively-throttling
        // bucket as evictable — dropping it would re-create a FULL bucket and let
        // a burst==1 flooder bypass the limit. `--per-user-op-burst` is operator-
        // configurable (floored at 1), so burst==1 is a reachable config. For
        // cap >= 2 the `>= 1.0` clause is already implied by `>= capacity - 1.0`,
        // so this is a no-op there.
        let safe = state.tokens >= self.capacity - 1.0 && state.tokens >= 1.0;
        (safe, state.last_access)
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
    /// Hard cap on entries in EACH per-user map. Defaults to
    /// [`MAX_TRACKED_USERS`]; lowered in tests via [`UserOpRateLimiter::with_cap`]
    /// so eviction can be exercised without inserting 100k entries.
    max_tracked_users: usize,
    /// Count of FULL eviction sweeps actually performed (past the cheap
    /// high-water early-return) across both maps. Used by the amortization test
    /// to assert hysteresis bounds the number of O(n) sweeps under token churn.
    /// Incremented on the rare sweep path only, so it's negligible in
    /// production.
    sweeps: std::sync::atomic::AtomicU64,
}

impl Inner {
    /// Bound `op_buckets` to [`MAX_TRACKED_USERS`]. Called only when a NEW user
    /// was just inserted (the map grew).
    ///
    /// COST / AMORTIZATION: the trigger is the high-water-mark (`len > cap`), but
    /// when it fires we evict down to the LOW-water-mark
    /// ([`eviction_low_water_mark`], ≈90% of cap), not just back to cap. So a
    /// single O(n) sweep frees ≈cap/10 slots and the next ≈cap/10 inserts all hit
    /// the cheap `len <= cap` early-return below. Without this hysteresis a map
    /// pinned at cap would run a full O(n) scan on EVERY new distinct token —
    /// the rate-limiter would become a CPU-amplification DoS under unvalidated
    /// `userToken` churn. Amortized cost is therefore O(1) per insert.
    ///
    /// SAFETY OF EVICTION (the load-bearing invariant): we only ever drop
    /// buckets that are essentially FULL at `now` — i.e. users who are NOT
    /// currently being throttled (see [`TokenBucket::evictability`]). Dropping a
    /// full bucket and lazily re-creating it later yields an identical full
    /// bucket, so eviction can never reset a depleted, actively-throttling
    /// flooder's bucket and hand them a fresh burst. Among the safe-to-evict
    /// (full) buckets we drop the LEAST-RECENTLY-USED first.
    ///
    /// If the map is over cap but too few buckets are safe to evict — i.e. most
    /// tracked users are currently being throttled, a genuine mass flood — we
    /// evict only what is safe and let the map sit over cap rather than weaken
    /// active limiting. That state is self-correcting: as soon as those buckets
    /// refill (the flood for that user pauses) they become evictable. A one-shot
    /// warning makes the (rare) degraded mode visible to operators. (That
    /// degraded state is also expensive for an attacker to MAINTAIN: a
    /// brand-new token's bucket starts full, so to keep a bucket un-evictable
    /// the attacker must spend its whole burst — all but the first of which are
    /// themselves rate-limited-rejected.)
    fn evict_op_buckets_if_needed(&self, now: Instant) {
        let len = self.op_buckets.len();
        if len <= self.max_tracked_users {
            return;
        }
        self.sweeps
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        // Hysteresis: aim for the low-water-mark, not just back to cap.
        let target_removals = len - eviction_low_water_mark(self.max_tracked_users);
        // Collect (key, last_access) for buckets that are SAFE to evict (full).
        // We never even consider a throttling bucket as a candidate.
        let mut candidates: Vec<(UserId, Instant)> = self
            .op_buckets
            .iter()
            .filter_map(|entry| {
                let (safe, last_access) = entry.value().evictability(now);
                safe.then(|| (*entry.key(), last_access))
            })
            .collect();
        // LRU first.
        candidates.sort_by_key(|(_, last_access)| *last_access);

        let mut removed = 0;
        for (key, _) in candidates.into_iter().take(target_removals) {
            // `remove_if` re-checks evictability under the shard write lock so a
            // bucket that started throttling between the scan and the remove is
            // NOT dropped (would otherwise reset it). This closes the
            // scan-then-remove race.
            if self
                .op_buckets
                .remove_if(&key, |_, bucket| bucket.evictability(now).0)
                .is_some()
            {
                removed += 1;
            }
        }
        // Warn only if we couldn't even get back under the HIGH-water-mark (cap)
        // — i.e. too few idle buckets to drop. Not reaching the low-water-mark is
        // expected and fine (it just means the next sweep comes sooner).
        if self.op_buckets.len() > self.max_tracked_users && removed < target_removals {
            // One-shot per process: this degraded mode (over cap, mostly active
            // throttling) is a posture signal worth surfacing once, but re-warning
            // on every subsequent over-cap sweep would itself be log spam under a
            // sustained flood. Intentionally not re-armed for a later distinct
            // episode — the metric to watch is the map size, not this line.
            static WARNED: std::sync::atomic::AtomicBool =
                std::sync::atomic::AtomicBool::new(false);
            if WARNED
                .compare_exchange(
                    false,
                    true,
                    std::sync::atomic::Ordering::SeqCst,
                    std::sync::atomic::Ordering::Relaxed,
                )
                .is_ok()
            {
                tracing::warn!(
                    tracked = self.op_buckets.len(),
                    cap = self.max_tracked_users,
                    "per-user op-rate map over cap with too few idle entries to \
                     evict (most tracked users are actively throttling). Holding \
                     over cap rather than resetting an active limiter; self-corrects \
                     as buckets refill. Possible distributed token flood."
                );
            }
        }
    }

    /// Bound `last_export` to [`MAX_TRACKED_USERS`]. Same high-water-trigger /
    /// low-water-target hysteresis as [`Self::evict_op_buckets_if_needed`] so the
    /// sweep is amortized O(1) per insert under token churn. An entry is safe to
    /// drop once it is older than `min_interval` (the next export would be
    /// admitted anyway, so re-creating it changes nothing — the same way a full
    /// op bucket is safe). We evict the OLDEST such stale entries first. If too
    /// few are stale we hold over cap rather than dropping an entry that is still
    /// actively throttling that user's next export.
    fn evict_last_export_if_needed(&self, now: Instant, min_interval: u64) {
        let len = self.last_export.len();
        if len <= self.max_tracked_users {
            return;
        }
        self.sweeps
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let target_removals = len - eviction_low_water_mark(self.max_tracked_users);
        let min = std::time::Duration::from_secs(min_interval);
        let mut candidates: Vec<(UserId, Instant)> = self
            .last_export
            .iter()
            .filter_map(|entry| {
                let last = *entry.value();
                (now.saturating_duration_since(last) >= min).then(|| (*entry.key(), last))
            })
            .collect();
        candidates.sort_by_key(|(_, last)| *last);
        for (key, _) in candidates.into_iter().take(target_removals) {
            // remove_if re-checks staleness under the write lock to avoid
            // dropping an entry a concurrent export just refreshed.
            self.last_export
                .remove_if(&key, |_, last| now.saturating_duration_since(*last) >= min);
        }
    }
}

impl UserOpRateLimiter {
    /// Build a limiter from operator config. `op_rate == 0` disables op
    /// limiting; `export_min_interval_secs == 0` disables export limiting.
    pub(crate) fn new(op_rate: u64, op_burst: u64, export_min_interval_secs: u64) -> Self {
        Self::with_cap(
            op_rate,
            op_burst,
            export_min_interval_secs,
            MAX_TRACKED_USERS,
        )
    }

    /// Like [`Self::new`] but with an explicit per-map entry cap. Production
    /// always uses [`MAX_TRACKED_USERS`] via `new`; tests use a small cap so the
    /// eviction path is exercised without inserting 100k entries.
    fn with_cap(
        op_rate: u64,
        op_burst: u64,
        export_min_interval_secs: u64,
        max_tracked_users: usize,
    ) -> Self {
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
                max_tracked_users: max_tracked_users.max(1),
                sweeps: std::sync::atomic::AtomicU64::new(0),
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
        //
        // `is_new` tells us whether THIS call created the bucket, so we only run
        // the (relatively expensive) eviction scan when the map actually grew —
        // not on every op of an existing user.
        let mut is_new = false;
        let admitted = {
            let bucket = self.inner.op_buckets.entry(*user).or_insert_with(|| {
                is_new = true;
                TokenBucket::new(self.inner.op_burst, self.inner.op_rate, now)
            });
            // Hold the entry guard ONLY for the acquire; it is dropped at the end
            // of this block, BEFORE eviction touches the map. Evicting while a
            // shard guard is held could deadlock DashMap.
            bucket.try_acquire(now)
        };
        if is_new {
            self.inner.evict_op_buckets_if_needed(now);
        }
        admitted
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
        let mut is_new = false;
        let admitted = {
            let mut entry = self.inner.last_export.entry(*user).or_insert_with(|| {
                is_new = true;
                // Seed far enough in the past that the FIRST export is always
                // admitted (now - min_interval), then overwritten below.
                now - std::time::Duration::from_secs(min_interval)
            });
            let elapsed = now.saturating_duration_since(*entry);
            if elapsed.as_secs() >= min_interval {
                *entry = now;
                true
            } else {
                false
            }
            // entry guard dropped here, before eviction touches the map.
        };
        if is_new {
            self.inner.evict_last_export_if_needed(now, min_interval);
        }
        admitted
    }

    /// Number of distinct users currently tracked (op buckets). Test-only
    /// visibility into map growth.
    #[cfg(test)]
    pub(crate) fn tracked_op_users(&self) -> usize {
        self.inner.op_buckets.len()
    }

    /// Number of distinct users currently tracked in the export-interval map.
    #[cfg(test)]
    pub(crate) fn tracked_export_users(&self) -> usize {
        self.inner.last_export.len()
    }

    /// Number of full eviction sweeps performed (past the cheap high-water
    /// early-return). The amortization test asserts hysteresis keeps this far
    /// below the number of inserts.
    #[cfg(test)]
    pub(crate) fn sweeps_performed(&self) -> u64 {
        self.inner.sweeps.load(std::sync::atomic::Ordering::Relaxed)
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

    /// `op_burst = 0` with a non-zero rate must NOT become a deny-all: it is
    /// clamped up to 1 so the first op of a fresh user still passes.
    #[tokio::test(start_paused = true)]
    async fn op_burst_zero_is_clamped_to_one() {
        let limiter = UserOpRateLimiter::new(10, 0, 0);
        let user = uid(50);
        let now = Instant::now();
        assert!(
            limiter.try_acquire_op_at(&user, now),
            "burst 0 must be clamped to 1 — first op passes, not deny-all"
        );
        assert!(
            !limiter.try_acquire_op_at(&user, now),
            "with clamped burst=1 the second same-instant op is rejected"
        );
    }

    /// Export interval uses whole-second granularity (`as_secs()`): 9.5s after
    /// an export is still under a 10s interval and must be rejected.
    #[tokio::test(start_paused = true)]
    async fn export_sub_second_boundary_still_rejects() {
        let limiter = UserOpRateLimiter::new(10, 50, 10);
        let user = uid(51);
        let t0 = Instant::now();
        assert!(limiter.try_acquire_export_at(&user, t0), "first export");
        let t_95 = t0 + std::time::Duration::from_millis(9_500);
        assert!(
            !limiter.try_acquire_export_at(&user, t_95),
            "9.5s < 10s interval → still rejected"
        );
        let t_10 = t0 + std::time::Duration::from_millis(10_000);
        assert!(
            limiter.try_acquire_export_at(&user, t_10),
            "exactly 10s → allowed"
        );
    }

    /// Map bound: admitting ops from many more distinct users than the cap keeps
    /// the op-bucket map at or below the cap. The evicted users are the IDLE
    /// (full) ones — so this also asserts that re-touching an old user works.
    #[tokio::test(start_paused = true)]
    async fn op_bucket_map_is_bounded() {
        // cap = 4, big burst so each user's single op leaves the bucket "full"
        // (essentially full → safe to evict). Advance time between users so
        // last_access strictly orders them for LRU.
        let limiter = UserOpRateLimiter::with_cap(1000, 1000, 0, 4);
        let mut now = Instant::now();
        for i in 0..50u8 {
            let user = uid(i);
            assert!(limiter.try_acquire_op_at(&user, now));
            now += std::time::Duration::from_millis(10);
            assert!(
                limiter.tracked_op_users() <= 4,
                "op-bucket map must never exceed the cap (was {} after user {i})",
                limiter.tracked_op_users()
            );
        }
    }

    /// Eviction MUST NOT reset an actively-throttling (depleted) bucket. With a
    /// tiny cap, a flooding user whose bucket is empty stays throttled even as
    /// many other distinct users arrive and force eviction scans — the evictor
    /// only drops the OTHER users' full/idle buckets, never the flooder's.
    #[tokio::test(start_paused = true)]
    async fn eviction_never_resets_active_flooder() {
        // cap = 2, burst = 3, rate tiny so refill is negligible over the test.
        let limiter = UserOpRateLimiter::with_cap(1, 3, 0, 2);
        let flooder = uid(200);
        let t0 = Instant::now();
        // Exhaust the flooder's bucket (burst 3), then confirm it's throttled.
        assert!(limiter.try_acquire_op_at(&flooder, t0));
        assert!(limiter.try_acquire_op_at(&flooder, t0));
        assert!(limiter.try_acquire_op_at(&flooder, t0));
        assert!(
            !limiter.try_acquire_op_at(&flooder, t0),
            "flooder is now depleted/throttled"
        );

        // Now a parade of OTHER users arrives, each forcing an eviction scan.
        // Their full buckets get evicted; the flooder's depleted bucket must not.
        let mut now = t0;
        for i in 0..40u8 {
            now += std::time::Duration::from_millis(1);
            let other = uid(i); // i never equals 200
            assert!(limiter.try_acquire_op_at(&other, now));
        }

        // The flooder, if its bucket had been evicted+recreated, would be FULL
        // again and admit. It must still be throttled (bucket preserved). Use a
        // time barely advanced so refill (rate=1/s) hasn't yielded a token.
        let t_check = now + std::time::Duration::from_millis(1);
        assert!(
            !limiter.try_acquire_op_at(&flooder, t_check),
            "eviction must NOT have reset the active flooder's depleted bucket"
        );
    }

    /// burst == 1 boundary (round-4 finding): at capacity == 1, a DEPLETED
    /// bucket has tokens == 0.0, and `tokens >= capacity - 1.0` is `0.0 >= 0.0`
    /// == true — so without the extra `tokens >= 1.0` clause in `evictability`,
    /// an actively-throttling burst==1 flooder would be classified evictable,
    /// dropped, and lazily recreated FULL → limit bypass. This test pins the fix:
    /// a depleted cap=1/burst=1 flooder stays throttled across other-user sweeps.
    ///
    /// All events share ONE frozen instant so the flooder sits at EXACTLY
    /// tokens == 0.0 (no refill) — the precise boundary the bug hit. Before the
    /// fix this FAILS (flooder evicted+recreated full → final acquire returns
    /// true); after the fix it PASSES.
    #[tokio::test(start_paused = true)]
    async fn eviction_never_resets_active_flooder_burst_one() {
        // cap = 2, burst = 1, rate 1/s (must be > 0 to enable op limiting).
        let limiter = UserOpRateLimiter::with_cap(1, 1, 0, 2);
        let flooder = uid(200);
        let t0 = Instant::now();
        // Spend the single burst token, then confirm throttled (tokens now 0.0).
        assert!(limiter.try_acquire_op_at(&flooder, t0));
        assert!(
            !limiter.try_acquire_op_at(&flooder, t0),
            "burst==1 flooder is now depleted/throttled (tokens == 0.0)"
        );

        // Parade of other users at the SAME instant t0 → zero refill, so the
        // flooder stays at exactly tokens == 0.0 throughout the eviction sweeps.
        for i in 0..40u8 {
            let other = uid(i); // never 200
            assert!(limiter.try_acquire_op_at(&other, t0));
        }

        // Still at t0 → no refill possible. If the depleted bucket had been
        // evicted+recreated it would now be FULL and admit. It must stay throttled.
        assert!(
            !limiter.try_acquire_op_at(&flooder, t0),
            "burst==1: eviction must NOT have reset the depleted flooder's bucket"
        );
    }

    /// Export map is bounded the same way: stale (older than the interval)
    /// entries are evicted, keeping the map at or below the cap.
    #[tokio::test(start_paused = true)]
    async fn export_map_is_bounded() {
        // cap = 3, interval = 1s. Advance > interval between users so prior
        // entries are stale (safe to evict).
        let limiter = UserOpRateLimiter::with_cap(10, 50, 1, 3);
        let mut now = Instant::now();
        for i in 0..30u8 {
            let user = uid(i);
            assert!(limiter.try_acquire_export_at(&user, now));
            now += std::time::Duration::from_millis(1100); // > 1s interval
            assert!(
                limiter.tracked_export_users() <= 3,
                "export map must never exceed the cap (was {} after user {i})",
                limiter.tracked_export_users()
            );
        }
    }

    /// A 32-byte UserId from a u32, so amortization tests can mint >256 users.
    fn uid32(n: u32) -> UserId {
        let mut b = [0u8; 32];
        b[..4].copy_from_slice(&n.to_le_bytes());
        UserId::new(b)
    }

    /// AMORTIZATION (review round 3): a sustained churn of fresh tokens at the
    /// cap must NOT trigger a full O(n) eviction sweep on every insert. With the
    /// low-water-mark hysteresis, one sweep frees ≈cap/10 slots, so sweeps occur
    /// at most ~once per cap/10 inserts. Here cap=100 (LWM=90, headroom 10) and
    /// we churn 2000 fresh users: without hysteresis that would be ~1900 sweeps
    /// (one per insert past cap); with it, ~(2000-100)/10 ≈ 190. We assert a
    /// generous bound well below the per-insert figure, AND that the map stays
    /// bounded throughout.
    #[tokio::test(start_paused = true)]
    async fn eviction_is_amortized_under_token_churn() {
        let cap = 100;
        let limiter = UserOpRateLimiter::with_cap(1000, 1000, 0, cap);
        let mut now = Instant::now();
        let inserts = 2000u32;
        for i in 0..inserts {
            let user = uid32(i);
            assert!(limiter.try_acquire_op_at(&user, now));
            now += std::time::Duration::from_millis(1);
            assert!(
                limiter.tracked_op_users() <= cap + 1,
                "map must stay bounded (≤ cap+1) during churn; was {} at insert {i}",
                limiter.tracked_op_users()
            );
        }
        let sweeps = limiter.sweeps_performed();
        // Per-insert sweeping would be ~inserts - cap ≈ 1900. Hysteresis caps it
        // near (inserts - cap) / (cap/10) ≈ 190. Assert comfortably below the
        // per-insert figure to prove amortization without pinning exact math.
        assert!(
            sweeps <= 400,
            "hysteresis must bound sweeps far below per-insert ({sweeps} sweeps \
             over {inserts} inserts; per-insert would be ~1900)"
        );
    }
}
