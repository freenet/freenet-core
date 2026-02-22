//! Deadlock detection support using parking_lot's `deadlock_detection` feature.
//!
//! When the `deadlock_detection` feature is enabled on parking_lot (automatically
//! enabled in test builds via dev-dependencies and the `testing` feature flag),
//! this module provides a background thread that periodically checks for deadlocks
//! in parking_lot Mutex/RwLock usage.
//!
//! # How it works
//!
//! The `parking_lot` crate has an optional `deadlock_detection` feature that
//! instruments all Mutex/RwLock operations to track lock ordering. When enabled,
//! `parking_lot::deadlock::check_deadlock()` can detect cycles in lock acquisition.
//!
//! This feature is enabled automatically in test builds via:
//! - The `testing` feature flag: `testing = ["freenet-stdlib/testing", "parking_lot/deadlock_detection"]`
//! - Dev-dependencies: `parking_lot = { workspace = true, features = ["deadlock_detection"] }`
//!
//! In non-test builds, the feature is not enabled and there is zero overhead.
//!
//! # Usage
//!
//! Call [`init_deadlock_detector`] at the start of a test to spawn a background
//! thread that monitors for deadlocks. The returned [`DeadlockDetectorGuard`]
//! keeps the detector running until dropped.
//!
//! ```ignore
//! #[test]
//! fn my_test() {
//!     let _detector = freenet::deadlock_detection::init_deadlock_detector();
//!     // ... test code using parking_lot locks ...
//! }
//! ```

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

/// Guard that keeps the deadlock detector thread running.
///
/// The detector thread is stopped when this guard is dropped.
pub struct DeadlockDetectorGuard {
    shutdown: Arc<AtomicBool>,
}

impl Drop for DeadlockDetectorGuard {
    fn drop(&mut self) {
        self.shutdown.store(true, Ordering::Relaxed);
    }
}

/// Start a background thread that periodically checks for deadlocks.
///
/// Returns a guard that keeps the detector running. When the guard is dropped,
/// the detector thread will stop at its next check interval.
///
/// The detector checks every `interval` for deadlocked threads. When a deadlock
/// is detected, it logs the deadlock information via `eprintln!` and optionally
/// panics (controlled by the `panic_on_deadlock` parameter).
///
/// # Arguments
///
/// * `interval` - How often to check for deadlocks
/// * `panic_on_deadlock` - If true, panics when a deadlock is detected
///
/// # Note
///
/// This function works correctly whether or not the `deadlock_detection` feature
/// is enabled on parking_lot. Without the feature, `check_deadlock()` always
/// returns an empty vec, making this a harmless no-op.
pub fn init_deadlock_detector_with_config(
    interval: Duration,
    panic_on_deadlock: bool,
) -> DeadlockDetectorGuard {
    let shutdown = Arc::new(AtomicBool::new(false));
    let shutdown_clone = shutdown.clone();

    std::thread::Builder::new()
        .name("deadlock-detector".into())
        .spawn(move || {
            while !shutdown_clone.load(Ordering::Relaxed) {
                std::thread::sleep(interval);

                let deadlocks = parking_lot::deadlock::check_deadlock();
                if deadlocks.is_empty() {
                    continue;
                }

                let mut report = format!("{} deadlock(s) detected!\n", deadlocks.len());
                for (i, threads) in deadlocks.iter().enumerate() {
                    report.push_str(&format!("Deadlock #{i}:\n"));
                    for t in threads {
                        report.push_str(&format!(
                            "  Thread Id {:#?}\n  Backtrace:\n{:#?}\n",
                            t.thread_id(),
                            t.backtrace()
                        ));
                    }
                }

                eprintln!("{report}");

                if panic_on_deadlock {
                    panic!("Deadlock detected! See error output above for details.");
                }
            }
        })
        .expect("failed to spawn deadlock detector thread");

    DeadlockDetectorGuard { shutdown }
}

/// Start a deadlock detector with default settings.
///
/// Checks every 1 second and panics on deadlock detection. This is the
/// recommended configuration for tests -- catching deadlocks quickly and
/// failing fast.
pub fn init_deadlock_detector() -> DeadlockDetectorGuard {
    init_deadlock_detector_with_config(Duration::from_secs(1), true)
}

#[cfg(test)]
mod tests {
    use super::*;
    use parking_lot::Mutex;
    use std::sync::Arc;

    /// Verify that the deadlock detector can be initialized and dropped cleanly,
    /// and that normal lock usage does not produce false positives.
    #[test]
    fn test_detector_lifecycle_no_false_positives() {
        let guard = init_deadlock_detector_with_config(Duration::from_millis(100), false);

        let mutex = Arc::new(Mutex::new(0));
        let m = mutex.clone();

        // Normal lock usage should not trigger deadlock detection
        let handle = std::thread::spawn(move || {
            let mut val = m.lock();
            *val += 1;
        });
        handle.join().unwrap();

        assert_eq!(*mutex.lock(), 1);

        // Let the detector run a few check cycles to confirm no false positives
        std::thread::sleep(Duration::from_millis(350));
        drop(guard);
    }

    /// Verify that parking_lot's deadlock detection feature is active and can
    /// detect a real AB/BA deadlock.
    ///
    /// This test creates a classic deadlock pattern: Thread 1 holds lock A and
    /// waits for lock B, while Thread 2 holds lock B and waits for lock A.
    /// We then call `check_deadlock()` and verify it reports the cycle.
    ///
    /// **Important**: `check_deadlock()` clears detected deadlocks on each call
    /// (it returns deadlocks "since the last call"). This test must not share
    /// a process with other tests that call `check_deadlock()`, otherwise they
    /// can consume each other's results. With nextest (one process per test),
    /// this is guaranteed. With `cargo test`, use `--test-threads=1` or run
    /// this test in isolation.
    #[test]
    fn test_deadlock_is_detected() {
        use std::sync::Barrier;

        // Drain any pre-existing deadlock state from prior tests
        let _ = parking_lot::deadlock::check_deadlock();

        let lock_a = Arc::new(Mutex::new(()));
        let lock_b = Arc::new(Mutex::new(()));
        let barrier = Arc::new(Barrier::new(2));

        let la1 = lock_a.clone();
        let lb1 = lock_b.clone();
        let b1 = barrier.clone();

        // Thread 1: lock A, sync, then try to lock B (deadlock)
        let _t1 = std::thread::spawn(move || {
            let _a = la1.lock();
            b1.wait();
            let _b = lb1.lock();
        });

        let la2 = lock_a.clone();
        let lb2 = lock_b.clone();
        let b2 = barrier.clone();

        // Thread 2: lock B, sync, then try to lock A (deadlock)
        let _t2 = std::thread::spawn(move || {
            let _b = lb2.lock();
            b2.wait();
            let _a = la2.lock();
        });

        // Wait for threads to reach the deadlock state, then check.
        // Retry in case the lock operations haven't completed yet.
        let mut detected = false;
        for _ in 0..20 {
            std::thread::sleep(Duration::from_millis(100));
            let deadlocks = parking_lot::deadlock::check_deadlock();
            if !deadlocks.is_empty() {
                assert_eq!(deadlocks.len(), 1, "Expected exactly 1 deadlock cycle");
                assert_eq!(
                    deadlocks[0].len(),
                    2,
                    "Expected 2 threads in the deadlock cycle"
                );
                detected = true;
                break;
            }
        }

        assert!(
            detected,
            "Expected deadlock to be detected within 2 seconds. \
             This likely means the deadlock_detection feature is not enabled on parking_lot, \
             or another test consumed the deadlock state (use nextest or --test-threads=1)."
        );

        // Threads are permanently deadlocked; they will be cleaned up on process exit.
        // This is safe because nextest runs each test in its own process.
    }
}
