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
}

impl BackgroundTaskMonitor {
    pub fn new() -> Self {
        Self {
            inner: Arc::new(Mutex::new(Vec::new())),
        }
    }

    /// Register a named background task for monitoring.
    ///
    /// The `name` is used in error messages when the task exits. It should be
    /// a short, human-readable identifier (e.g. "connection_maintenance",
    /// "garbage_cleanup").
    pub fn register(&self, name: &'static str, handle: JoinHandle<()>) {
        self.inner.lock().push(TrackedTask { name, handle });
    }

    /// Consume the monitor and return a future that resolves with an error
    /// when the first tracked task exits (for any reason).
    ///
    /// This method drains all registered tasks out of the monitor. After
    /// calling this, the monitor is empty and should not be reused.
    pub fn wait_for_any_exit(self) -> impl std::future::Future<Output = anyhow::Error> + Send {
        let tasks: Vec<TrackedTask> = {
            let mut inner = self.inner.lock();
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
}
