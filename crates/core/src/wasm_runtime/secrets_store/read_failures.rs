//! Field visibility for secret reads that fail on a blob that IS present.
//!
//! A secret blob that exists but does not authenticate (a key-encryption-key
//! mismatch, or corruption) is reported to delegates as "absent" (#6323). The
//! only record used to be a `debug!` line, which release builds compile out,
//! so nothing in the field showed how many nodes were affected (#6368).
//!
//! This module counts such failures on the delegate-facing read paths only
//! (`SecretsStore::get_secret_for_delegate` and `list_secret_keys`), in two
//! process-wide counters (authentication vs I/O) that `resource_utilization`
//! telemetry reports, and logs a throttled release-level `warn!`. Write,
//! register, export, copy-forward and migration reads are not counted: a
//! failure there is not a delegate seeing a secret as absent. It changes no
//! read result.
//!
//! "Authentication" here also covers a key registry with a malformed header
//! (`InvalidData`) and a user-scope read with a wrong `dek_secret`. Counts are
//! reads: `list_secrets_len` plus `list_secrets` may count one corrupt
//! registry twice.
//!
//! Not-found never counts: an absent secret is normal.
//!
//! Privacy: the log carries the delegate key and the bs58 hash that names the
//! blob on disk, never the raw key name or any secret content. That log line
//! is in the normal log, which a user-initiated `freenet service report` can
//! upload. Telemetry carries aggregate counts only.

use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{LazyLock, Mutex, PoisonError};
use std::time::{Duration, Instant};

/// Minimum gap between two log lines for the same (delegate, blob).
const LOG_INTERVAL: Duration = Duration::from_secs(600);
/// Bound on tracked (delegate, blob) pairs. A pair past the bound is still
/// counted, and is logged under one shared overflow entry.
const MAX_TRACKED_BLOBS: usize = 256;
/// Bound on the distinct-delegate set; the reported count saturates here.
const MAX_TRACKED_DELEGATES: usize = 1024;
const OVERFLOW: &str = "<overflow>";

/// Which kind of failure was seen. Only used to word the log line.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum FailureKind {
    /// The blob was read but did not authenticate or decrypt.
    Auth,
    /// The read failed with an I/O error other than not-found.
    Io,
}

impl FailureKind {
    fn label(self) -> &'static str {
        match self {
            Self::Auth => {
                "blob present but failed to authenticate; the delegate sees it as absent \
                 (possible key-encryption-key mismatch or corruption)"
            }
            Self::Io => "blob present but read failed with an I/O error",
        }
    }
}

#[derive(Default)]
struct Throttle {
    /// (delegate, blob hash) -> (last log time, failures suppressed since).
    blobs: HashMap<(String, String), (Instant, u64)>,
    delegates: HashSet<String>,
}

/// Counts failed delegate-facing reads and throttles their log line.
pub struct SecretReadFailures {
    auth: AtomicU64,
    io: AtomicU64,
    state: Mutex<Throttle>,
}

impl SecretReadFailures {
    pub fn new() -> Self {
        Self {
            auth: AtomicU64::new(0),
            io: AtomicU64::new(0),
            state: Mutex::new(Throttle::default()),
        }
    }

    /// Lock the throttle state. A poisoned lock only means another thread
    /// panicked while holding it; the data is still usable, so recover it
    /// rather than disagree with the atomic counters.
    fn lock(&self) -> std::sync::MutexGuard<'_, Throttle> {
        self.state.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// Cumulative reads that failed to authenticate (key mismatch or
    /// corruption) since process start. Counts reads, not blobs.
    pub fn auth_total(&self) -> u64 {
        self.auth.load(Ordering::Relaxed)
    }

    /// Cumulative reads that failed with a non-not-found I/O error since
    /// process start. Counts reads, not blobs.
    pub fn io_total(&self) -> u64 {
        self.io.load(Ordering::Relaxed)
    }

    /// Distinct delegates with at least one failure (saturates at a bound).
    pub fn delegates_affected(&self) -> u32 {
        u32::try_from(self.lock().delegates.len()).unwrap_or(u32::MAX)
    }

    /// Record one failure and log it unless throttled. Returns `Some(n)` when
    /// a log line was emitted, where `n` is the number of failures for this
    /// blob suppressed since the previous line, and `None` when throttled.
    pub fn record(
        &self,
        now: Instant,
        delegate: &str,
        blob_hash: &str,
        kind: FailureKind,
    ) -> Option<u64> {
        match kind {
            FailureKind::Auth => &self.auth,
            FailureKind::Io => &self.io,
        }
        .fetch_add(1, Ordering::Relaxed);
        let suppressed = self.should_log(now, delegate, blob_hash)?;
        // Logged with the lock released, so a slow tracing layer cannot block
        // other failing reads or the telemetry sampler.
        tracing::warn!(
            delegate = %delegate,
            blob_hash = %blob_hash,
            suppressed_since_last_log = suppressed,
            "secret read failure: {}",
            kind.label()
        );
        Some(suppressed)
    }

    /// Update the throttle state; `Some(suppressed)` means log now.
    fn should_log(&self, now: Instant, delegate: &str, blob_hash: &str) -> Option<u64> {
        let mut state = self.lock();
        if state.delegates.len() < MAX_TRACKED_DELEGATES && !state.delegates.contains(delegate) {
            state.delegates.insert(delegate.to_owned());
        }
        let pair = (delegate.to_owned(), blob_hash.to_owned());
        let key = if state.blobs.contains_key(&pair) || state.blobs.len() < MAX_TRACKED_BLOBS {
            pair
        } else {
            (OVERFLOW.to_owned(), OVERFLOW.to_owned())
        };
        match state.blobs.get_mut(&key) {
            None => {
                state.blobs.insert(key, (now, 0));
                Some(0)
            }
            Some((last, suppressed)) => {
                if now.saturating_duration_since(*last) < LOG_INTERVAL {
                    *suppressed += 1;
                    return None;
                }
                *last = now;
                Some(std::mem::take(suppressed))
            }
        }
    }
}

impl Default for SecretReadFailures {
    fn default() -> Self {
        Self::new()
    }
}

/// The process-wide tracker reported by telemetry.
pub static SECRET_READ_FAILURES: LazyLock<SecretReadFailures> =
    LazyLock::new(SecretReadFailures::new);

#[cfg(test)]
thread_local! {
    /// Test-owned tracker for the current thread. Store reads run on the
    /// calling thread, so a test that installs one sees only its own
    /// failures, however many sibling tests corrupt blobs in parallel.
    static TEST_TRACKER: std::cell::RefCell<Option<std::sync::Arc<SecretReadFailures>>> =
        const { std::cell::RefCell::new(None) };
}

/// Route this thread's failures to a fresh tracker and return it.
#[cfg(test)]
pub(crate) fn install_test_tracker() -> std::sync::Arc<SecretReadFailures> {
    let t = std::sync::Arc::new(SecretReadFailures::new());
    TEST_TRACKER.with(|c| *c.borrow_mut() = Some(t.clone()));
    t
}

/// Record a failure on the process-wide tracker (or the thread's test one).
pub(super) fn record_failure(delegate: &str, blob_hash: &str, kind: FailureKind) {
    #[cfg(test)]
    {
        let installed = TEST_TRACKER.with(|c| c.borrow().clone());
        if let Some(t) = installed {
            t.record(Instant::now(), delegate, blob_hash, kind);
            return;
        }
    }
    SECRET_READ_FAILURES.record(Instant::now(), delegate, blob_hash, kind);
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn first_occurrence_logs_then_throttles_then_summarises() {
        let t = SecretReadFailures::new();
        let t0 = Instant::now();
        assert_eq!(t.record(t0, "d", "k", FailureKind::Auth), Some(0));
        assert_eq!(
            t.record(t0 + Duration::from_secs(1), "d", "k", FailureKind::Auth),
            None
        );
        assert_eq!(
            t.record(t0 + Duration::from_secs(2), "d", "k", FailureKind::Auth),
            None
        );
        assert_eq!(t.auth_total(), 3, "suppressed failures still count");
        assert_eq!(
            t.record(t0 + LOG_INTERVAL, "d", "k", FailureKind::Auth),
            Some(2),
            "the next line reports the two suppressed failures"
        );
        assert_eq!(
            t.record(t0 + LOG_INTERVAL * 2, "d", "k", FailureKind::Auth),
            Some(0),
            "the suppressed count resets after being reported"
        );
    }

    #[test]
    fn a_noisy_key_does_not_hide_another_key() {
        let t = SecretReadFailures::new();
        let t0 = Instant::now();
        assert!(t.record(t0, "d", "noisy", FailureKind::Auth).is_some());
        for i in 1..50 {
            assert!(
                t.record(t0 + Duration::from_secs(i), "d", "noisy", FailureKind::Auth)
                    .is_none()
            );
        }
        assert!(
            t.record(
                t0 + Duration::from_secs(60),
                "d",
                "other",
                FailureKind::Auth
            )
            .is_some()
        );
        assert!(
            t.record(t0 + Duration::from_secs(61), "d2", "noisy", FailureKind::Io)
                .is_some()
        );
        assert_eq!(t.delegates_affected(), 2);
    }

    #[test]
    fn kinds_count_separately() {
        let t = SecretReadFailures::new();
        let t0 = Instant::now();
        t.record(t0, "d", "a", FailureKind::Auth);
        t.record(t0, "d", "a", FailureKind::Auth);
        t.record(t0, "d", "b", FailureKind::Io);
        assert_eq!((t.auth_total(), t.io_total()), (2, 1));
    }

    #[test]
    fn tracking_is_bounded_but_counting_is_not() {
        let t = SecretReadFailures::new();
        let t0 = Instant::now();
        for i in 0..(MAX_TRACKED_BLOBS + 100) {
            t.record(t0, "d", &format!("k{i}"), FailureKind::Auth);
        }
        assert_eq!(t.auth_total(), (MAX_TRACKED_BLOBS + 100) as u64);
        let state = t.lock();
        assert!(state.blobs.len() <= MAX_TRACKED_BLOBS + 1);
    }

    #[test]
    fn a_poisoned_lock_does_not_make_the_two_readings_disagree() {
        let t = std::sync::Arc::new(SecretReadFailures::new());
        t.record(Instant::now(), "d", "k", FailureKind::Auth);
        let t2 = t.clone();
        let joined = std::thread::spawn(move || {
            let _guard = t2.state.lock().unwrap();
            panic!("poison the throttle lock");
        })
        .join();
        assert!(joined.is_err());
        assert!(t.state.is_poisoned());
        assert_eq!(t.delegates_affected(), 1);
        t.record(Instant::now(), "d2", "k", FailureKind::Io);
        assert_eq!((t.auth_total(), t.io_total()), (1, 1));
        assert_eq!(t.delegates_affected(), 2);
    }

    /// Without a thread-local test tracker, `record_failure` feeds the
    /// process-wide tracker that telemetry reads. The counter is shared with
    /// parallel tests, so assert a delta of at least what this test records.
    #[test]
    fn record_failure_without_a_test_tracker_feeds_the_global() {
        let (a0, i0) = (
            SECRET_READ_FAILURES.auth_total(),
            SECRET_READ_FAILURES.io_total(),
        );
        record_failure("global-fallback-delegate", "h1", FailureKind::Auth);
        record_failure("global-fallback-delegate", "h2", FailureKind::Io);
        assert!(SECRET_READ_FAILURES.auth_total() > a0);
        assert!(SECRET_READ_FAILURES.io_total() > i0);
        assert!(SECRET_READ_FAILURES.delegates_affected() >= 1);
    }
}
