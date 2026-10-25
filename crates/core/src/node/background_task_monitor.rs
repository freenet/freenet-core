//! Monitor for fire-and-forget background tasks.
//!
//! Background tasks spawned via `GlobalExecutor::spawn` that run for the entire
//! lifetime of a node are critical infrastructure. If any such task silently
//! exits (panic, cancellation, or unexpected return), the node runs in a
//! degraded state with no logging or detection.
//!
//! `BackgroundTaskMonitor` collects the `JoinHandle`s of these long-lived tasks
//! under human-readable names. The main event loop calls
//! [`BackgroundTaskMonitor::wait_for_any_exit`] inside its `select!` to detect
//! the first task that exits and propagate the failure.
//!
//! See issue #3121 and the incident described in #3120 for motivation.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use parking_lot::Mutex;
use tokio::task::JoinHandle;

/// A named background task whose JoinHandle is tracked for silent-death detection.
struct TrackedTask {
    name: &'static str,
    handle: JoinHandle<()>,
}

/// Registry of critical background tasks.
///
/// Tasks are registered with [`register`](BackgroundTaskMonitor::register) during
/// node construction. Once the node event loop starts, call
/// [`wait_for_any_exit`](BackgroundTaskMonitor::wait_for_any_exit) which resolves
/// with an error when the first tracked task exits.
///
/// Thread-safe: the inner state is behind `Arc<Mutex<_>>` so that subsystems
/// (Ring, OpManager, etc.) can register tasks during construction without
/// requiring mutable access to the monitor.
#[derive(Clone)]
pub(crate) struct BackgroundTaskMonitor {
    inner: Arc<Mutex<Vec<TrackedTask>>>,
    /// Set by `wait_for_any_exit`, after which a registration is not
    /// monitored.
    drained: Arc<AtomicBool>,
}

impl BackgroundTaskMonitor {
    pub fn new() -> Self {
        Self {
            inner: Arc::new(Mutex::new(Vec::new())),
            drained: Arc::new(AtomicBool::new(false)),
        }
    }

    /// Register a named background task for monitoring.
    ///
    /// The `name` is used in error messages when the task exits. It should be
    /// a short, human-readable identifier (e.g. "connection_maintenance",
    /// "garbage_cleanup").
    ///
    /// Only before [`Self::wait_for_any_exit`]: a task registered after it
    /// is not watched by anything, so it is logged at ERROR (the task keeps
    /// running, unmonitored) instead of failing silently.
    pub fn register(&self, name: &'static str, handle: JoinHandle<()>) {
        let mut inner = self.inner.lock();
        // Read under the lock `wait_for_any_exit` drains under, so a
        // registration is either drained or reported, never lost unseen.
        if self.drained.load(Ordering::Acquire) {
            tracing::error!(
                task = name,
                "Background task registered after the monitor started waiting; it is NOT monitored"
            );
            return;
        }
        inner.push(TrackedTask { name, handle });
    }

    /// Whether `wait_for_any_exit` has drained this monitor, so a new
    /// registration would not be monitored.
    #[cfg_attr(not(test), allow(dead_code))]
    pub fn is_drained(&self) -> bool {
        self.drained.load(Ordering::Acquire)
    }

    /// Consume the monitor and return a future that resolves with an error
    /// when the first tracked task exits (for any reason).
    ///
    /// This method drains all registered tasks out of the monitor. After
    /// calling this, the monitor is empty and should not be reused.
    pub fn wait_for_any_exit(self) -> impl std::future::Future<Output = anyhow::Error> + Send {
        let tasks: Vec<TrackedTask> = {
            let mut inner = self.inner.lock();
            self.drained.store(true, Ordering::Release);
            std::mem::take(&mut *inner)
        };

        async move {
            if tasks.is_empty() {
                // No tasks registered; park forever so the select! arm never fires.
                std::future::pending::<()>().await;
                unreachable!();
            }

            // Use a JoinSet to wait for the first task to exit.
            let mut join_set = tokio::task::JoinSet::new();
            let names: Vec<&'static str> = tasks.iter().map(|t| t.name).collect();

            for (idx, task) in tasks.into_iter().enumerate() {
                // Each entry in the JoinSet wraps one tracked JoinHandle.
                // When the underlying task exits, the wrapper completes.
                join_set.spawn(async move {
                    let result = task.handle.await;
                    (idx, result)
                });
            }

            // Wait for the first completion.
            let (idx, result) = join_set
                .join_next()
                .await
                .expect("JoinSet is non-empty")
                .expect("wrapper task should not panic");

            let name = names[idx];

            // Abort remaining wrapper tasks so they don't leak.
            join_set.abort_all();

            match result {
                Err(e) if e.is_panic() => {
                    tracing::error!(task = name, "Background task panicked: {e}");
                    anyhow::anyhow!("Background task '{name}' panicked: {e}")
                }
                Err(e) => {
                    tracing::error!(task = name, "Background task failed: {e}");
                    anyhow::anyhow!("Background task '{name}' failed: {e}")
                }
                Ok(()) => {
                    tracing::error!(
                        task = name,
                        "Background task exited unexpectedly (clean return)"
                    );
                    anyhow::anyhow!("Background task '{name}' exited unexpectedly")
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    /// When a tracked task panics, wait_for_any_exit resolves with a panic error.
    #[tokio::test]
    async fn panicking_task_is_detected() {
        let monitor = BackgroundTaskMonitor::new();

        // One task that sleeps forever, one that panics.
        let h1 = tokio::spawn(async {
            tokio::time::sleep(Duration::from_secs(60)).await;
        });
        let h2 = tokio::spawn(async {
            tokio::time::sleep(Duration::from_millis(10)).await;
            panic!("background task panic");
        });

        monitor.register("sleeper", h1);
        monitor.register("panicker", h2);

        let err = monitor.wait_for_any_exit().await;
        let msg = err.to_string();
        assert!(
            msg.contains("panicker") && msg.contains("panicked"),
            "Expected panic error for 'panicker', got: {msg}"
        );
    }

    /// When a tracked task returns cleanly (unexpected for long-lived tasks),
    /// wait_for_any_exit resolves with an "exited unexpectedly" error.
    #[tokio::test]
    async fn clean_exit_is_detected() {
        let monitor = BackgroundTaskMonitor::new();

        let h1 = tokio::spawn(async {
            tokio::time::sleep(Duration::from_secs(60)).await;
        });
        let h2 = tokio::spawn(async {
            // Exits immediately
        });

        monitor.register("sleeper", h1);
        monitor.register("quick_exit", h2);

        let err = monitor.wait_for_any_exit().await;
        let msg = err.to_string();
        assert!(
            msg.contains("quick_exit") && msg.contains("exited unexpectedly"),
            "Expected 'exited unexpectedly' error for 'quick_exit', got: {msg}"
        );
    }

    /// Registering zero tasks means wait_for_any_exit never resolves.
    #[tokio::test]
    async fn empty_monitor_never_resolves() {
        let monitor = BackgroundTaskMonitor::new();

        let result =
            tokio::time::timeout(Duration::from_millis(50), monitor.wait_for_any_exit()).await;

        assert!(result.is_err(), "Empty monitor should not resolve");
    }

    /// Multiple tasks: only the first to exit is reported.
    #[tokio::test]
    async fn first_exit_wins() {
        let monitor = BackgroundTaskMonitor::new();

        let h1 = tokio::spawn(async {
            tokio::time::sleep(Duration::from_millis(100)).await;
        });
        let h2 = tokio::spawn(async {
            tokio::time::sleep(Duration::from_millis(10)).await;
            // Returns early
        });
        let h3 = tokio::spawn(async {
            tokio::time::sleep(Duration::from_secs(60)).await;
        });

        monitor.register("slow_exit", h1);
        monitor.register("fast_exit", h2);
        monitor.register("sleeper", h3);

        let err = monitor.wait_for_any_exit().await;
        let msg = err.to_string();
        assert!(
            msg.contains("fast_exit"),
            "Expected 'fast_exit' to be detected first, got: {msg}"
        );
    }

    /// A task registered after `wait_for_any_exit` drained the monitor is
    /// not monitored, and the monitor says so: its exit does not resolve
    /// the wait (nothing could), and `is_drained` reports the state a
    /// late caller must check.
    #[tokio::test]
    async fn registration_after_wait_is_not_monitored() {
        let monitor = BackgroundTaskMonitor::new();
        let sleeper = tokio::spawn(async {
            tokio::time::sleep(Duration::from_secs(60)).await;
        });
        monitor.register("sleeper", sleeper);
        assert!(!monitor.is_drained());
        let wait = monitor.clone().wait_for_any_exit();
        assert!(monitor.is_drained());
        let (logs, guard) = crate::util::test_log_capture::install();
        monitor.register("late_exit", tokio::spawn(async {}));
        drop(guard);
        let logs = logs.lock().unwrap().clone();
        assert!(
            logs.iter()
                .any(|line| line.starts_with("ERROR") && line.contains("late_exit")),
            "the late registration is logged at ERROR: {logs:?}"
        );
        let fired = tokio::time::timeout(Duration::from_millis(100), wait).await;
        assert!(fired.is_err(), "a late registration is not watched");
    }
}
