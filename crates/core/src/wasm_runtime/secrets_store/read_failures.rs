//! Field visibility for secret reads that fail on a blob that IS present.
//!
//! A secret blob that exists but does not authenticate (a key-encryption-key
//! mismatch, or corruption) is reported to delegates as "absent" (#6323). The
//! only record used to be a `debug!` line, which release builds compile out,
//! so nothing in the field showed how many nodes were affected (#6368).
//!
//! This module counts such failures in one process-wide counter, which
//! `resource_utilization` telemetry reports, and logs a throttled release-level
//! `warn!`. It changes no read result.
//!
//! Not-found never counts: an absent secret is normal.
//!
//! Privacy: the log carries the delegate key and the bs58 hash that names the
//! blob on disk, never the raw key name or any secret content. Telemetry
//! carries aggregate counts only.

use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{LazyLock, Mutex};
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
            Self::Auth => "blob present but failed to authenticate",
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

/// Counts failed reads and throttles their log line.
pub struct SecretReadFailures {
    total: AtomicU64,
    state: Mutex<Throttle>,
}

impl SecretReadFailures {
    pub fn new() -> Self {
        Self {
            total: AtomicU64::new(0),
            state: Mutex::new(Throttle::default()),
        }
    }

    /// Cumulative failed reads since process start. Counts reads, not blobs:
    /// a delegate that reads the same bad blob twice counts twice.
    pub fn total(&self) -> u64 {
        self.total.load(Ordering::Relaxed)
    }

    /// Distinct delegates with at least one failure (saturates at a bound).
    pub fn delegates_affected(&self) -> u32 {
        let n = self.state.lock().map(|s| s.delegates.len()).unwrap_or(0);
        u32::try_from(n).unwrap_or(u32::MAX)
    }

    /// Record one failure and log it unless throttled. Returns whether a log
    /// line was emitted.
    pub fn record(&self, now: Instant, delegate: &str, blob_hash: &str, kind: FailureKind) -> bool {
        self.total.fetch_add(1, Ordering::Relaxed);
        // A poisoned lock only loses throttling state; counting continues.
        let Ok(mut state) = self.state.lock() else {
            return false;
        };
        if state.delegates.len() < MAX_TRACKED_DELEGATES {
            state.delegates.insert(delegate.to_owned());
        }
        let pair = (delegate.to_owned(), blob_hash.to_owned());
        let key = if state.blobs.contains_key(&pair) || state.blobs.len() < MAX_TRACKED_BLOBS {
            pair
        } else {
            (OVERFLOW.to_owned(), OVERFLOW.to_owned())
        };
        let suppressed = match state.blobs.get_mut(&key) {
            None => {
                state.blobs.insert(key, (now, 0));
                0
            }
            Some((last, suppressed)) => {
                if now.saturating_duration_since(*last) < LOG_INTERVAL {
                    *suppressed += 1;
                    return false;
                }
                *last = now;
                std::mem::take(suppressed)
            }
        };
        tracing::warn!(
            delegate = %delegate,
            blob_hash = %blob_hash,
            suppressed_since_last_log = suppressed,
            "secret read failure: {}; the delegate sees this secret as absent \
             (possible key-encryption-key mismatch or corruption)",
            kind.label()
        );
        true
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
pub(super) fn install_test_tracker() -> std::sync::Arc<SecretReadFailures> {
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
        assert!(t.record(t0, "d", "k", FailureKind::Auth));
        assert!(!t.record(t0 + Duration::from_secs(1), "d", "k", FailureKind::Auth));
        assert!(!t.record(t0 + Duration::from_secs(2), "d", "k", FailureKind::Auth));
        assert_eq!(t.total(), 3, "suppressed failures still count");
        assert!(t.record(t0 + LOG_INTERVAL, "d", "k", FailureKind::Auth));
    }

    #[test]
    fn a_noisy_key_does_not_hide_another_key() {
        let t = SecretReadFailures::new();
        let t0 = Instant::now();
        assert!(t.record(t0, "d", "noisy", FailureKind::Auth));
        for i in 1..50 {
            assert!(!t.record(t0 + Duration::from_secs(i), "d", "noisy", FailureKind::Auth));
        }
        assert!(t.record(
            t0 + Duration::from_secs(60),
            "d",
            "other",
            FailureKind::Auth
        ));
        assert!(t.record(t0 + Duration::from_secs(61), "d2", "noisy", FailureKind::Io));
        assert_eq!(t.delegates_affected(), 2);
    }

    #[test]
    fn tracking_is_bounded_but_counting_is_not() {
        let t = SecretReadFailures::new();
        let t0 = Instant::now();
        for i in 0..(MAX_TRACKED_BLOBS + 100) {
            t.record(t0, "d", &format!("k{i}"), FailureKind::Auth);
        }
        assert_eq!(t.total(), (MAX_TRACKED_BLOBS + 100) as u64);
        let state = t.state.lock().unwrap();
        assert!(state.blobs.len() <= MAX_TRACKED_BLOBS + 1);
    }
}
