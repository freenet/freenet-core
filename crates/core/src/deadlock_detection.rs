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

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

/// Guard that keeps the deadlock detector thread running.
///
/// Dropping the guard stops the detector thread **and waits for it to fully
/// exit** before returning. This is a hard guarantee: once `drop` returns, the
/// detector thread has made its last [`parking_lot::deadlock::check_deadlock`]
/// call and is joined.
///
/// That join matters because `check_deadlock()` is *process-global* and
/// consuming: it returns each deadlock cycle exactly once. A detector thread
/// that lingered past the guard's lifetime could call `check_deadlock()` after
/// the guard was dropped and silently consume a deadlock cycle that a
/// subsequent, unrelated test expected to observe. That race made
/// `test_deadlock_is_detected` flaky under parallel execution (#3627): when the
/// detector-lifecycle test ran just before it (both share
/// `#[serial(deadlock_detection)]`), the lifecycle test's not-yet-joined
/// detector thread could steal the freshly-created deadlock report.
pub struct DeadlockDetectorGuard {
    shutdown: Arc<AtomicBool>,
    handle: Option<std::thread::JoinHandle<()>>,
}

impl Drop for DeadlockDetectorGuard {
    fn drop(&mut self) {
        self.shutdown.store(true, Ordering::Relaxed);
        if let Some(handle) = self.handle.take() {
            // Wait for the detector thread to observe the shutdown flag and
            // exit. Without this join the thread could perform one more
            // check_deadlock() after drop and consume another test's cycle.
            //
            // We deliberately ignore a join error (a panicked detector thread):
            // re-raising it here would turn drop into a double-panic. A detector
            // configured to panic on deadlock would already have surfaced that
            // panic on its own thread.
            if handle.join().is_err() {
                // Detector thread panicked; nothing actionable to do in drop.
            }
        }
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

    // Poll the shutdown flag at a fine granularity so that dropping the guard
    // stops the thread promptly, and — critically — so that `shutdown` is
    // re-checked immediately before every `check_deadlock()` call. That ordering
    // guarantees the thread performs no `check_deadlock()` after the guard has
    // signalled shutdown, which (together with the join in `Drop`) prevents the
    // detector from consuming another test's deadlock cycle (#3627).
    let poll_step = Duration::from_millis(10).min(interval);

    let handle = std::thread::Builder::new()
        .name("deadlock-detector".into())
        .spawn(move || {
            loop {
                // Sleep for `interval`, but in small steps so we notice shutdown
                // quickly instead of blocking for a whole interval.
                let mut waited = Duration::ZERO;
                while waited < interval {
                    if shutdown_clone.load(Ordering::Relaxed) {
                        return;
                    }
                    let step = poll_step.min(interval - waited);
                    std::thread::sleep(step);
                    waited += step;
                }

                // Re-check shutdown right before the consuming check_deadlock()
                // call so no cycle is consumed after the guard was dropped.
                if shutdown_clone.load(Ordering::Relaxed) {
                    return;
                }

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

    DeadlockDetectorGuard {
        shutdown,
        handle: Some(handle),
    }
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
    use serial_test::serial;
    use std::collections::HashSet;
    use std::sync::Arc;
    use std::thread::ThreadId;
    use std::time::Instant;

    /// Poll the process-global deadlock state until a cycle whose thread-id set
    /// equals `target` is reported, or `budget` elapses.
    ///
    /// Returns `true` if the target cycle was observed. This matches *only* the
    /// caller's own cycle by thread id, so it is robust to any other deadlock
    /// cycles that unrelated tests may have created concurrently (#3627).
    ///
    /// Callers must hold the `deadlock_detection` serial lock so that no other
    /// `check_deadlock()` caller consumes the target cycle first.
    fn wait_for_cycle(target: &HashSet<ThreadId>, budget: Duration) -> bool {
        let start = Instant::now();
        while start.elapsed() < budget {
            std::thread::sleep(Duration::from_millis(50));
            for cycle in parking_lot::deadlock::check_deadlock() {
                let cycle_threads: HashSet<ThreadId> =
                    cycle.iter().map(|t| t.thread_id()).collect();
                if &cycle_threads == target {
                    return true;
                }
            }
        }
        false
    }

    /// Verify that the deadlock detector can be initialized and dropped cleanly,
    /// and that normal lock usage does not produce false positives.
    #[test]
    #[serial(deadlock_detection)]
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
    /// We then call `check_deadlock()` and verify it reports *our* cycle.
    ///
    /// **Process-global state, and why this test used to be flaky (#3627):**
    /// `parking_lot::deadlock::check_deadlock()` inspects the *entire process*
    /// and returns *every* lock-wait cycle it finds (each cycle exactly once —
    /// once reported, a cycle is never reported again). Under `cargo test`
    /// (single process, many parallel test threads) any other test that forms
    /// a parking_lot lock-wait cycle while this one is polling would also show
    /// up in the returned vector. The previous implementation asserted
    /// `deadlocks.len() == 1` and `deadlocks[0].len() == 2`, so any such
    /// concurrent cycle broke those exact-count assertions even though *our*
    /// deadlock was detected perfectly well. `#[serial(deadlock_detection)]`
    /// did not help: it only serializes the deadlock-keyed tests against each
    /// other, not against the hundreds of unrelated tests running in parallel.
    ///
    /// The fix is to stop asserting on the global cycle count and instead
    /// identify *our own* cycle by the [`std::thread::ThreadId`]s of the two
    /// threads we spawned, asserting only that a 2-thread cycle containing
    /// exactly those two threads is present. That makes the assertion correct
    /// regardless of what any other test in the process is doing.
    ///
    /// `#[serial(deadlock_detection)]` is retained as defense-in-depth: it
    /// guarantees no *other* `check_deadlock()` caller (the sibling lifecycle
    /// test's detector thread) runs concurrently and consumes our cycle before
    /// we observe it.
    #[test]
    #[serial(deadlock_detection)]
    fn test_deadlock_is_detected() {
        use std::sync::Barrier;

        // Drain any pre-existing deadlock state from prior tests so the cycles
        // we observe below are reported fresh (a cycle is only ever reported
        // once).
        let _ = parking_lot::deadlock::check_deadlock();

        let lock_a = Arc::new(Mutex::new(()));
        let lock_b = Arc::new(Mutex::new(()));
        let barrier = Arc::new(Barrier::new(2));

        let la1 = lock_a.clone();
        let lb1 = lock_b.clone();
        let b1 = barrier.clone();

        // Thread 1: lock A, sync, then try to lock B (deadlock)
        let t1 = std::thread::spawn(move || {
            let _a = la1.lock();
            b1.wait();
            let _b = lb1.lock();
        });

        let la2 = lock_a.clone();
        let lb2 = lock_b.clone();
        let b2 = barrier.clone();

        // Thread 2: lock B, sync, then try to lock A (deadlock)
        let t2 = std::thread::spawn(move || {
            let _b = lb2.lock();
            b2.wait();
            let _a = la2.lock();
        });

        // The exact pair of thread ids that make up the cycle we expect.
        // `DeadlockedThread::thread_id()` returns `std::thread::ThreadId`, the
        // same type `JoinHandle::thread().id()` yields, so we can match our own
        // cycle out of whatever check_deadlock() reports process-wide.
        let our_threads: HashSet<ThreadId> = [t1.thread().id(), t2.thread().id()].into();

        // Wait for the two threads to reach the deadlocked state, then look for
        // OUR specific 2-thread cycle among everything check_deadlock() reports.
        //
        // We deliberately do NOT assert on the total number of cycles or on
        // other cycles' sizes: other tests in the same process may have their
        // own concurrent deadlocks (see #3627). We only require that our cycle
        // appears.
        //
        // The detection itself is deterministic once both threads have parked;
        // the only variable is scheduling latency, so we use a generous wall-
        // clock budget that resolves in milliseconds under normal load but
        // tolerates a heavily-oversubscribed CI runner. (This is NOT masking a
        // race — the consume-by-lingering-detector race that previously made
        // this flaky is fixed by joining the detector thread in
        // DeadlockDetectorGuard::drop.)
        let detected = wait_for_cycle(&our_threads, Duration::from_secs(10));

        assert!(
            detected,
            "Expected our 2-thread AB/BA deadlock to be detected within 10 seconds. \
             This likely means the deadlock_detection feature is not enabled on parking_lot, \
             or another check_deadlock() caller consumed our deadlock state \
             (all deadlock tests must share #[serial(deadlock_detection)], and the \
             detector guard must join its thread on drop)."
        );

        // Threads t1/t2 are permanently deadlocked by design; we intentionally
        // do not join them. They are cleaned up on process exit. This is safe
        // under nextest (a process per test) and, thanks to the per-cycle
        // matching above, also correct under single-process `cargo test`.
    }

    /// Spawn a self-contained AB/BA deadlock and return the two threads' ids.
    ///
    /// The locks are leaked (the threads block on them forever), mirroring the
    /// permanently-deadlocked threads in [`test_deadlock_is_detected`]. The
    /// caller is responsible for serializing access to the process-global
    /// deadlock state.
    fn spawn_deadlock_pair() -> HashSet<ThreadId> {
        use std::sync::Barrier;

        let lock_a = Arc::new(Mutex::new(()));
        let lock_b = Arc::new(Mutex::new(()));
        let barrier = Arc::new(Barrier::new(2));

        let la1 = lock_a.clone();
        let lb1 = lock_b.clone();
        let b1 = barrier.clone();
        let t1 = std::thread::spawn(move || {
            let _a = la1.lock();
            b1.wait();
            let _b = lb1.lock();
        });

        let la2 = lock_a;
        let lb2 = lock_b;
        let b2 = barrier;
        let t2 = std::thread::spawn(move || {
            let _b = lb2.lock();
            b2.wait();
            let _a = la2.lock();
        });

        [t1.thread().id(), t2.thread().id()].into()
    }

    /// Regression test for #3627: deadlock detection must remain correct when
    /// MORE THAN ONE deadlock cycle exists in the process at the same time.
    ///
    /// Before the fix, `test_deadlock_is_detected` asserted
    /// `deadlocks.len() == 1`. Under single-process `cargo test`, any other
    /// test that happened to form a parking_lot lock-wait cycle concurrently
    /// pushed `deadlocks.len()` above 1 and tripped that assertion, even though
    /// the test's own deadlock was detected fine. This test reproduces that
    /// exact condition deterministically by standing up two independent
    /// deadlocks and asserting that each is found by thread-id, and — crucially
    /// — that `check_deadlock()` does report both at once (so the old
    /// `len() == 1` assertion would have failed here).
    #[test]
    #[serial(deadlock_detection)]
    fn test_concurrent_deadlocks_are_each_detected() {
        // Drain pre-existing state so the cycles below are reported fresh.
        let _ = parking_lot::deadlock::check_deadlock();

        let pair_one = spawn_deadlock_pair();
        let pair_two = spawn_deadlock_pair();

        // Wait until BOTH cycles are simultaneously visible to a SINGLE
        // check_deadlock() call, retrying the *whole observation* if they are
        // not both parked yet.
        //
        // Subtlety: a cycle is reported only once, so if an early poll observes
        // just one parked pair, that call consumes it and it will never appear
        // again alongside the second pair. To get a clean both-at-once
        // observation we therefore spawn FRESH pairs and only count a poll that
        // returns both. We bound total attempts so a genuinely broken detector
        // still fails fast rather than hanging.
        let mut pairs = vec![pair_one, pair_two];
        let mut both_in_one_call = false;
        'attempts: for attempt in 0..20 {
            // Give the two freshly-spawned threads time to acquire their first
            // lock, cross the barrier, and park on the second lock before we
            // look. 300ms is comfortably longer than thread spawn + park even
            // on a loaded CI runner, while keeping the test fast.
            std::thread::sleep(Duration::from_millis(300));

            let deadlocks = parking_lot::deadlock::check_deadlock();
            let cycles: Vec<HashSet<ThreadId>> = deadlocks
                .iter()
                .map(|cycle| cycle.iter().map(|t| t.thread_id()).collect())
                .collect();

            if pairs.iter().all(|p| cycles.iter().any(|c| c == p)) {
                // A single call reported BOTH cycles at once. This is the heart
                // of #3627: with two live deadlocks, one check_deadlock() call
                // returns both cycles, so the pre-fix
                // `assert_eq!(deadlocks.len(), 1)` would have panicked here.
                both_in_one_call = true;
                break 'attempts;
            }

            // Not both visible at once (one pair parked late and was consumed,
            // or threads have not parked yet). Re-arm with fresh pairs and try
            // again so the next observation can catch both simultaneously.
            if attempt < 19 {
                pairs = vec![spawn_deadlock_pair(), spawn_deadlock_pair()];
            }
        }

        assert!(
            both_in_one_call,
            "expected a single check_deadlock() call to report both independent \
             deadlock cycles at once (this process-global contamination is what \
             broke test_deadlock_is_detected under parallel execution, #3627)"
        );

        // Threads remain deadlocked by design; cleaned up on process exit.
    }

    /// Regression test for the second #3627 root cause: a dropped
    /// [`DeadlockDetectorGuard`] must NOT leave a detector thread running that
    /// can consume a later test's deadlock cycle.
    ///
    /// Before the fix, `Drop` only set a shutdown flag and returned immediately,
    /// without joining the thread. The detector loop checked that flag only at
    /// the top of the loop, *after* sleeping a full interval, so after `drop`
    /// returned the thread could still perform one more consuming
    /// `check_deadlock()` call. When this happened between two
    /// `#[serial(deadlock_detection)]` tests, the lingering detector stole the
    /// next test's freshly-created deadlock report and `test_deadlock_is_detected`
    /// failed its "deadlock not detected" assertion.
    ///
    /// This test arranges exactly that timing: it runs a fast (10ms-interval)
    /// detector, drops it, then immediately creates a deadlock and asserts the
    /// cycle is still observable. With the old non-joining `Drop`, the lingering
    /// detector would race to consume the cycle; with the join, no detector
    /// activity can outlive the guard.
    #[test]
    #[serial(deadlock_detection)]
    fn test_dropped_detector_does_not_consume_later_deadlock() {
        // A short interval maximizes the chance the old, non-joining detector
        // would fire its post-drop check_deadlock() while we are setting up the
        // deadlock below.
        let guard = init_deadlock_detector_with_config(Duration::from_millis(10), false);

        // Drain any pre-existing state, then stop the detector. After this drop
        // returns, the join guarantees no detector check_deadlock() can run.
        let _ = parking_lot::deadlock::check_deadlock();
        drop(guard);

        // Now create our own deadlock and confirm WE can still observe it — i.e.
        // a lingering detector did not consume it out from under us.
        let our_threads = spawn_deadlock_pair();
        assert!(
            wait_for_cycle(&our_threads, Duration::from_secs(10)),
            "a dropped detector consumed our deadlock cycle — \
             DeadlockDetectorGuard::drop must join the detector thread (#3627)"
        );

        // Threads remain deadlocked by design; cleaned up on process exit.
    }
}
