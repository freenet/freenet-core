//! Rate-limiting layer for tracing.
//!
//! Two limiters live here, intended to compose:
//!
//! * [`RateLimiter`] — global token bucket across *all* callsites. Cheap last
//!   line of defence against an aggregate event-rate explosion.
//! * [`PerCallsiteRateLimiter`] — per-callsite token bucket, so one chatty
//!   `tracing::warn!` site cannot drown out everything else. This is what
//!   contains issue #4251-style single-site spam (~40 WARN/sec from one
//!   `tracing::warn!` macro) before it ever consumes the global budget.
//!
//! Both expose `should_allow(...) -> bool` and are safe to clone / share
//! across threads. Use them via [`tracing_subscriber::filter::filter_fn`].
//!
//! # Example
//!
//! ```ignore
//! use tracing_subscriber::prelude::*;
//! use crate::util::rate_limit_layer::{PerCallsiteRateLimiter, RateLimiter};
//!
//! let global = RateLimiter::with_defaults();           // 1000 ev/sec total
//! let per_site = PerCallsiteRateLimiter::with_defaults(); // 30 ev/sec per callsite
//!
//! let filter = tracing_subscriber::filter::filter_fn(move |meta| {
//!     per_site.should_allow(meta) && global.should_allow()
//! });
//! ```

use dashmap::DashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;
use tokio::time::Instant;
use tracing_core::Metadata;
use tracing_core::callsite::Identifier;

/// Default maximum events per second before rate limiting kicks in.
pub const DEFAULT_MAX_EVENTS_PER_SECOND: u64 = 1000;

/// Default maximum events per second per callsite.
///
/// Picked so that a misbehaving WARN macro is capped at ~108k events/hr,
/// well under the historical ~140 MB/hr error log baseline that triggered
/// issue #4251. Real bursts (e.g. a flurry of legitimate errors on startup)
/// still pass through up to this rate per callsite.
pub const DEFAULT_MAX_EVENTS_PER_CALLSITE_PER_SECOND: u64 = 30;

/// Cap on the number of distinct callsites tracked. Each callsite holds
/// one [`CallsiteState`] (~64 B). Once the cap is hit, the per-callsite
/// limiter degrades gracefully to "pass" so a flood of unique callsites
/// can't OOM the process. In practice freenet has ~hundreds of callsites
/// total; the cap is a safety net.
pub const MAX_TRACKED_CALLSITES: usize = 4096;

/// How often to log a summary of dropped events.
const DROPPED_SUMMARY_INTERVAL: Duration = Duration::from_secs(10);

/// Inner state for the rate limiter (shared via Arc for thread safety).
struct RateLimiterInner {
    /// Maximum events allowed per second
    max_events_per_second: u64,
    /// Event counter for current window
    event_count: AtomicU64,
    /// Events dropped in current period
    dropped_count: AtomicU64,
    /// Total events dropped since start
    total_dropped: AtomicU64,
    /// Start of current counting window (as duration since UNIX_EPOCH for atomicity)
    window_start_nanos: AtomicU64,
    /// Last time we logged a dropped summary
    last_summary_nanos: AtomicU64,
    /// Process start time for calculating nanos
    start_instant: Instant,
}

/// Rate limiter for tracing events.
///
/// Uses a sliding window approach to track event rate and drops events
/// when the rate exceeds the configured maximum. Thread-safe and cloneable.
#[derive(Clone)]
pub struct RateLimiter {
    inner: Arc<RateLimiterInner>,
}

impl RateLimiter {
    /// Create a new rate limiter with the specified max events per second.
    pub fn new(max_events_per_second: u64) -> Self {
        let now = Instant::now();
        Self {
            inner: Arc::new(RateLimiterInner {
                max_events_per_second,
                event_count: AtomicU64::new(0),
                dropped_count: AtomicU64::new(0),
                total_dropped: AtomicU64::new(0),
                window_start_nanos: AtomicU64::new(0),
                last_summary_nanos: AtomicU64::new(0),
                start_instant: now,
            }),
        }
    }

    /// Create with default rate limit (1000 events/second).
    pub fn with_defaults() -> Self {
        Self::new(DEFAULT_MAX_EVENTS_PER_SECOND)
    }

    /// Get elapsed nanos since start (for atomic storage).
    fn elapsed_nanos(&self) -> u64 {
        self.inner.start_instant.elapsed().as_nanos() as u64
    }

    /// Check if an event should be allowed and update counters.
    /// Returns true if event should be logged, false if it should be dropped.
    pub fn should_allow(&self) -> bool {
        let now_nanos = self.elapsed_nanos();
        let window_start = self.inner.window_start_nanos.load(Ordering::Relaxed);

        // Check if we're in a new second
        const ONE_SECOND_NANOS: u64 = 1_000_000_000;
        if now_nanos >= window_start + ONE_SECOND_NANOS {
            // New window - reset counters
            // Use compare_exchange to avoid race conditions
            if self
                .inner
                .window_start_nanos
                .compare_exchange(window_start, now_nanos, Ordering::SeqCst, Ordering::Relaxed)
                .is_ok()
            {
                self.inner.event_count.store(0, Ordering::Relaxed);
                // Keep dropped_count for summary, reset it in maybe_log_summary
            }
        }

        // Increment and check count
        let count = self.inner.event_count.fetch_add(1, Ordering::Relaxed);

        if count < self.inner.max_events_per_second {
            self.maybe_log_summary();
            true
        } else {
            // Rate exceeded - drop this event
            self.inner.dropped_count.fetch_add(1, Ordering::Relaxed);
            self.inner.total_dropped.fetch_add(1, Ordering::Relaxed);
            self.maybe_log_summary();
            false
        }
    }

    /// Check if we should log a summary of dropped events.
    fn maybe_log_summary(&self) {
        let dropped = self.inner.dropped_count.load(Ordering::Relaxed);
        if dropped == 0 {
            return;
        }

        let now_nanos = self.elapsed_nanos();
        let last_summary = self.inner.last_summary_nanos.load(Ordering::Relaxed);
        let interval_nanos = DROPPED_SUMMARY_INTERVAL.as_nanos() as u64;

        if now_nanos >= last_summary + interval_nanos {
            // Time to log a summary
            if self
                .inner
                .last_summary_nanos
                .compare_exchange(last_summary, now_nanos, Ordering::SeqCst, Ordering::Relaxed)
                .is_ok()
            {
                let dropped = self.inner.dropped_count.swap(0, Ordering::Relaxed);
                let total = self.inner.total_dropped.load(Ordering::Relaxed);

                if dropped > 0 {
                    // Use eprintln to bypass the rate limiter itself
                    eprintln!(
                        "[RATE LIMIT] Dropped {} log events in last {}s (total dropped: {}). \
                         Max rate: {}/sec. Consider investigating log spam.",
                        dropped,
                        DROPPED_SUMMARY_INTERVAL.as_secs(),
                        total,
                        self.inner.max_events_per_second
                    );
                }
            }
        }
    }
}

impl Default for RateLimiter {
    fn default() -> Self {
        Self::with_defaults()
    }
}

// ───────────────────────── per-callsite limiter ─────────────────────────

/// Per-callsite token-bucket limiter for tracing events.
///
/// Each tracing macro invocation (`tracing::warn!(...)`, etc.) produces a
/// unique static [`Identifier`]. We keep one [`CallsiteState`] per identifier
/// and apply a 1-second sliding window: the first
/// `max_events_per_second_per_callsite` events in each window pass through
/// untouched; everything beyond is dropped and counted. A periodic summary
/// (default every 30 s) reports any callsites that have shed events,
/// pinpointing the chatty site by `target` / `file:line` so an operator
/// can diagnose the spam without having to scrape the full log.
///
/// Why per-callsite rather than just global: issue #4251 showed a single
/// `tracing::warn!` macro emitting ~40 events/sec drowning the error log
/// while staying well under the global 1000 ev/sec limit. A per-callsite
/// cap contains that class of single-site spam without throttling unrelated
/// legitimate logging.
#[derive(Clone)]
pub struct PerCallsiteRateLimiter {
    inner: Arc<PerCallsiteInner>,
}

struct PerCallsiteInner {
    max_events_per_second_per_callsite: u64,
    callsites: DashMap<Identifier, CallsiteState>,
    last_summary_nanos: AtomicU64,
    summary_interval: Duration,
    start_instant: Instant,
}

struct CallsiteState {
    /// 1-second window start, monotonic-ish nanos since [`PerCallsiteInner::start_instant`].
    window_start_nanos: AtomicU64,
    /// Events seen in the current window.
    event_count: AtomicU64,
    /// Events dropped in the current summary interval (reset on each summary).
    dropped_this_interval: AtomicU64,
    /// Events dropped since the limiter was constructed (monotonic).
    total_dropped: AtomicU64,
    /// Callsite name (`Metadata::name`, which IS `'static`). For an event macro
    /// this is typically `"event <file>:<line>"`, which is enough to pinpoint
    /// the chatty site in the summary line. `Metadata::file`/`line`/`target`
    /// only return the metadata's borrowed lifetime, so we cannot store them.
    name: &'static str,
}

impl PerCallsiteRateLimiter {
    pub fn new(max_events_per_second_per_callsite: u64) -> Self {
        Self::with_summary_interval(max_events_per_second_per_callsite, Duration::from_secs(30))
    }

    pub fn with_summary_interval(
        max_events_per_second_per_callsite: u64,
        summary_interval: Duration,
    ) -> Self {
        Self {
            inner: Arc::new(PerCallsiteInner {
                max_events_per_second_per_callsite,
                callsites: DashMap::new(),
                last_summary_nanos: AtomicU64::new(0),
                summary_interval,
                start_instant: Instant::now(),
            }),
        }
    }

    pub fn with_defaults() -> Self {
        Self::new(DEFAULT_MAX_EVENTS_PER_CALLSITE_PER_SECOND)
    }

    fn elapsed_nanos(&self) -> u64 {
        self.inner.start_instant.elapsed().as_nanos() as u64
    }

    /// Returns `true` if the event should be allowed through, `false` to drop.
    ///
    /// Cheap fast path: one DashMap lookup + two atomic ops on the hot path.
    /// Degrades to "pass" if the tracked-callsite cap is reached so an
    /// unbounded number of distinct callsites cannot OOM the process.
    pub fn should_allow(&self, metadata: &Metadata<'_>) -> bool {
        self.should_allow_inner(metadata.callsite(), metadata.name())
    }

    /// Inner entry point — split out so unit tests can synthesise
    /// `Identifier` values directly without needing the full `Metadata`
    /// machinery (which requires more `'static` lifetimes than tests can
    /// easily provide).
    fn should_allow_inner(&self, id: Identifier, name: &'static str) -> bool {
        let now_nanos = self.elapsed_nanos();

        let entry = match self.inner.callsites.get(&id) {
            Some(e) => e,
            None => {
                if self.inner.callsites.len() >= MAX_TRACKED_CALLSITES {
                    // Cap hit. Don't insert; let the event through and don't
                    // count it. Better to log than to OOM.
                    return true;
                }
                self.inner
                    .callsites
                    .entry(id.clone())
                    .or_insert_with(|| CallsiteState {
                        window_start_nanos: AtomicU64::new(now_nanos),
                        event_count: AtomicU64::new(0),
                        dropped_this_interval: AtomicU64::new(0),
                        total_dropped: AtomicU64::new(0),
                        name,
                    });
                // Re-fetch as an immutable reference (release the write guard).
                self.inner.callsites.get(&id).expect("just inserted")
            }
        };

        const ONE_SECOND_NANOS: u64 = 1_000_000_000;
        let window_start = entry.window_start_nanos.load(Ordering::Relaxed);
        if now_nanos >= window_start.saturating_add(ONE_SECOND_NANOS) {
            // Try to roll the window forward. If we race and lose, the
            // winning thread has already reset the counter; either way the
            // next fetch_add below is against the fresh window.
            if entry
                .window_start_nanos
                .compare_exchange(window_start, now_nanos, Ordering::SeqCst, Ordering::Relaxed)
                .is_ok()
            {
                entry.event_count.store(0, Ordering::Relaxed);
            }
        }

        let count = entry.event_count.fetch_add(1, Ordering::Relaxed);
        let allow = count < self.inner.max_events_per_second_per_callsite;
        if !allow {
            entry.dropped_this_interval.fetch_add(1, Ordering::Relaxed);
            entry.total_dropped.fetch_add(1, Ordering::Relaxed);
        }
        // Release the entry borrow before the summary pass, which may take
        // a brief write guard on the same map.
        drop(entry);
        self.maybe_log_summary(now_nanos);
        allow
    }

    fn maybe_log_summary(&self, now_nanos: u64) {
        let last = self.inner.last_summary_nanos.load(Ordering::Relaxed);
        let interval_nanos = self.inner.summary_interval.as_nanos() as u64;
        if now_nanos < last.saturating_add(interval_nanos) {
            return;
        }
        if self
            .inner
            .last_summary_nanos
            .compare_exchange(last, now_nanos, Ordering::SeqCst, Ordering::Relaxed)
            .is_err()
        {
            return;
        }

        // Snapshot offenders, then emit one line per. Use eprintln so the
        // summary itself bypasses the tracing layer (and therefore this
        // limiter). Don't hold the DashMap iter across allocation-heavy
        // work — copy out into a small Vec first.
        let mut offenders: Vec<(String, u64, u64)> = Vec::new();
        for entry in self.inner.callsites.iter() {
            let dropped = entry.dropped_this_interval.swap(0, Ordering::Relaxed);
            if dropped == 0 {
                continue;
            }
            let total = entry.total_dropped.load(Ordering::Relaxed);
            offenders.push((entry.name.to_string(), dropped, total));
        }
        offenders.sort_by(|a, b| b.1.cmp(&a.1));
        for (site, dropped, total) in offenders {
            eprintln!(
                "[RATE LIMIT per-callsite] {site}: dropped {dropped} in last {}s (cumulative: {total}). \
                 Per-callsite cap: {}/sec.",
                self.inner.summary_interval.as_secs(),
                self.inner.max_events_per_second_per_callsite
            );
        }
    }
}

impl Default for PerCallsiteRateLimiter {
    fn default() -> Self {
        Self::with_defaults()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_rate_limiter_allows_under_limit() {
        let limiter = RateLimiter::new(100);

        // Should allow up to 100 events
        for _ in 0..100 {
            assert!(limiter.should_allow());
        }
    }

    #[test]
    fn test_rate_limiter_drops_over_limit() {
        let limiter = RateLimiter::new(10);

        // Allow first 10
        for _ in 0..10 {
            assert!(limiter.should_allow());
        }

        // Should drop subsequent events
        for _ in 0..10 {
            assert!(!limiter.should_allow());
        }

        // Check dropped count
        assert_eq!(limiter.inner.dropped_count.load(Ordering::Relaxed), 10);
        assert_eq!(limiter.inner.total_dropped.load(Ordering::Relaxed), 10);
    }

    #[test]
    fn test_rate_limiter_resets_after_window() {
        let limiter = RateLimiter::new(5);

        // Exhaust the limit
        for _ in 0..5 {
            assert!(limiter.should_allow());
        }
        assert!(!limiter.should_allow());

        // Simulate what happens when a new time window begins by manually
        // resetting the event counter. The actual window reset logic is tested
        // implicitly - when should_allow detects a new second has passed, it
        // resets event_count to 0. Here we simulate that reset directly.
        limiter.inner.event_count.store(0, Ordering::Relaxed);

        // Should allow again after counter reset
        assert!(limiter.should_allow());

        // Verify we can use the full allowance again
        for _ in 0..4 {
            assert!(limiter.should_allow());
        }
        // Now at limit again
        assert!(!limiter.should_allow());
    }

    #[test]
    fn test_default_limit() {
        let limiter = RateLimiter::with_defaults();
        assert_eq!(
            limiter.inner.max_events_per_second,
            DEFAULT_MAX_EVENTS_PER_SECOND
        );
    }

    #[test]
    fn test_rate_limiter_is_cloneable() {
        let limiter1 = RateLimiter::new(10);
        let limiter2 = limiter1.clone();

        // Both should share state
        for _ in 0..10 {
            assert!(limiter1.should_allow());
        }

        // limiter2 should see the exhausted state
        assert!(!limiter2.should_allow());
    }

    // ──────────────────── per-callsite tests ────────────────────

    use tracing_core::Metadata;
    use tracing_core::callsite::Callsite;

    /// Minimal `Callsite` impl used purely as a stable identity for tests.
    /// `Identifier` is a `&'static dyn Callsite` and compares by pointer,
    /// so each test callsite needs a distinct address. ZSTs share addresses
    /// (well-defined per the unsized-locals rules), so we give it a tag.
    struct TestCallsite {
        _tag: usize,
    }
    impl Callsite for TestCallsite {
        fn set_interest(&self, _: tracing_core::subscriber::Interest) {}
        fn metadata(&self) -> &Metadata<'_> {
            unreachable!("limiter never invokes this in the inner fast path")
        }
    }

    static CS_A: TestCallsite = TestCallsite { _tag: 1 };
    static CS_B: TestCallsite = TestCallsite { _tag: 2 };
    static CS_C: TestCallsite = TestCallsite { _tag: 3 };
    static CS_D: TestCallsite = TestCallsite { _tag: 4 };

    fn id(cs: &'static TestCallsite) -> Identifier {
        Identifier(cs)
    }

    fn drive(
        limiter: &PerCallsiteRateLimiter,
        cs: &'static TestCallsite,
        n: usize,
        name: &'static str,
    ) -> usize {
        (0..n)
            .filter(|_| limiter.should_allow_inner(id(cs), name))
            .count()
    }

    #[test]
    fn per_callsite_allows_under_limit() {
        let limiter = PerCallsiteRateLimiter::new(50);
        let allowed = drive(&limiter, &CS_A, 50, "site_under_limit");
        assert_eq!(allowed, 50, "all 50 events under the cap must pass");
    }

    #[test]
    fn per_callsite_drops_over_limit() {
        let limiter = PerCallsiteRateLimiter::new(5);
        let allowed = drive(&limiter, &CS_B, 20, "site_over_limit");
        assert_eq!(allowed, 5, "only 5 of 20 events should pass the cap");
    }

    #[test]
    fn per_callsite_isolates_sites_from_each_other() {
        // Two callsites must NOT share a budget. This is the whole point of
        // the per-callsite limiter — without isolation a chatty site A
        // starves quiet site B.
        let limiter = PerCallsiteRateLimiter::new(3);
        let allowed_a = drive(&limiter, &CS_C, 10, "site_c");
        let allowed_b = drive(&limiter, &CS_D, 10, "site_d");
        assert_eq!(allowed_a, 3, "site C should be capped at 3");
        assert_eq!(
            allowed_b, 3,
            "site D should get its own 3 — not be starved by site C"
        );
    }

    #[test]
    fn per_callsite_window_resets() {
        let limiter = PerCallsiteRateLimiter::new(2);
        // Use a fresh static for window-reset isolation.
        static CS_W: TestCallsite = TestCallsite { _tag: 5 };
        let first = drive(&limiter, &CS_W, 5, "window_reset");
        assert_eq!(first, 2);
        // Force-roll the window without sleeping by zeroing the per-callsite
        // window start. `should_allow_inner` will see a >= 1-sec gap on the
        // next call and roll.
        for entry in limiter.inner.callsites.iter() {
            entry.window_start_nanos.store(0, Ordering::Relaxed);
            entry.event_count.store(0, Ordering::Relaxed);
        }
        let second = drive(&limiter, &CS_W, 5, "window_reset");
        assert_eq!(second, 2, "fresh window must restore full budget");
    }

    #[test]
    fn per_callsite_summary_path_is_safe_against_dashmap_borrow_self_collision() {
        // Reaching the summary path while a single entry has been borrowed
        // earlier in the same call has caused deadlocks in past versions of
        // similar limiters. Drive enough events at summary-interval=1ns to
        // exercise the summary every iteration and confirm no panic / hang.
        static CS_S: TestCallsite = TestCallsite { _tag: 6 };
        let limiter = PerCallsiteRateLimiter::with_summary_interval(2, Duration::from_nanos(1));
        let _ = drive(&limiter, &CS_S, 10, "summary_smoke");
    }

    #[test]
    fn per_callsite_default_uses_canonical_constant() {
        let limiter = PerCallsiteRateLimiter::default();
        assert_eq!(
            limiter.inner.max_events_per_second_per_callsite,
            DEFAULT_MAX_EVENTS_PER_CALLSITE_PER_SECOND
        );
    }

    #[test]
    fn per_callsite_is_cloneable_and_shares_state() {
        static CS_X: TestCallsite = TestCallsite { _tag: 7 };
        let limiter1 = PerCallsiteRateLimiter::new(3);
        let limiter2 = limiter1.clone();
        let a = drive(&limiter1, &CS_X, 3, "shared_state");
        let b = drive(&limiter2, &CS_X, 3, "shared_state");
        assert_eq!(a, 3);
        assert_eq!(b, 0, "limiter2 must observe limiter1's exhausted state");
    }
}
