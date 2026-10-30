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
    /// Tasks registered through a [`LateRegistrar`], which the future
    /// returned by `wait_for_any_exit` picks up while it runs.
    late: Arc<LateTasks>,
}

/// Tasks handed to a [`LateRegistrar`] and not yet picked up by the wait.
#[derive(Default)]
struct LateTasks {
    pending: Mutex<Vec<TrackedTask>>,
    /// Names of every task registered, for tests.
    #[cfg(test)]
    registered: Mutex<Vec<&'static str>>,
    /// Signalled on each registration. `Notify` keeps one permit when no one
    /// is waiting, so a registration made between the wait taking `pending`
    /// and starting to wait again is not missed.
    added: tokio::sync::Notify,
    /// Abort handles of every task registered, and whether the wait future
    /// has been dropped (see [`AbortLateTasks`]). One lock, so a
    /// registration is either aborted by the drop or aborted on arrival.
    aborts: Mutex<(bool, Vec<tokio::task::AbortHandle>)>,
}

/// Held by the future `wait_for_any_exit` returns. When that future is
/// dropped (the node is stopping), aborts every task registered through a
/// [`LateRegistrar`], and any registered later is aborted at once. Tasks
/// registered before the wait stop on the ring's shutdown token; a late
/// task (the QUIC accept loop) has no such token, and dropping its
/// `JoinHandle` only detaches it.
struct AbortLateTasks(Arc<LateTasks>);

impl Drop for AbortLateTasks {
    fn drop(&mut self) {
        let mut aborts = self.0.aborts.lock();
        aborts.0 = true;
        for abort in aborts.1.drain(..) {
            abort.abort();
        }
    }
}

/// Registers tasks with a [`BackgroundTaskMonitor`] at any time, including
/// after [`BackgroundTaskMonitor::wait_for_any_exit`] has drained it: the
/// future that call returned also watches these tasks, and resolves when
/// one of them exits, as for a task registered before the wait.
///
/// For tasks started after the node's event loop begins. `run_node` starts
/// waiting on its monitor before the event listener opens the transport, so
/// a task started from the transport's open path (the QUIC accept loop) is
/// registered here; `BackgroundTaskMonitor::register` at that point would
/// leave it unmonitored.
///
/// A task registered after the wait future has resolved is not watched by
/// anything; the node is stopping by then. Every task registered here is
/// aborted when the wait future is dropped, or at once if it already has
/// been.
#[derive(Clone)]
pub(crate) struct LateRegistrar {
    late: Arc<LateTasks>,
}

impl LateRegistrar {
    /// Register a named background task, watched by the monitor's wait
    /// whether or not it has started (see [`LateRegistrar`]).
    pub fn register(&self, name: &'static str, handle: JoinHandle<()>) {
        #[cfg(test)]
        self.late.registered.lock().push(name);
        {
            let mut aborts = self.late.aborts.lock();
            if aborts.0 {
                tracing::debug!(
                    task = name,
                    "Node stopping; aborting a late-registered task"
                );
                handle.abort();
                return;
            }
            aborts.1.push(handle.abort_handle());
        }
        self.late.pending.lock().push(TrackedTask { name, handle });
        self.late.added.notify_one();
    }

    /// The names of the tasks registered through this monitor's late
    /// registrars, in order.
    #[cfg(test)]
    pub fn registered(&self) -> Vec<&'static str> {
        self.late.registered.lock().clone()
    }
}

impl BackgroundTaskMonitor {
    pub fn new() -> Self {
        Self {
            inner: Arc::new(Mutex::new(Vec::new())),
            drained: Arc::new(AtomicBool::new(false)),
            late: Arc::new(LateTasks::default()),
        }
    }

    /// A handle that registers tasks with this monitor at any time,
    /// including after `wait_for_any_exit` (see [`LateRegistrar`]).
    pub fn late_registrar(&self) -> LateRegistrar {
        LateRegistrar {
            late: self.late.clone(),
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
    /// calling this, the monitor is empty and should not be reused. The
    /// future also watches every task registered through a
    /// [`LateRegistrar`] of this monitor, before or after this call.
    pub fn wait_for_any_exit(self) -> impl std::future::Future<Output = anyhow::Error> + Send {
        let tasks: Vec<TrackedTask> = {
            let mut inner = self.inner.lock();
            self.drained.store(true, Ordering::Release);
            std::mem::take(&mut *inner)
        };
        let late = self.late;
        let abort_late = AbortLateTasks(late.clone());

        async move {
            let _abort_late = abort_late;
            // Each entry in the JoinSet wraps one tracked JoinHandle. When
            // the underlying task exits, the wrapper completes.
            let mut join_set = tokio::task::JoinSet::new();
            let mut names: Vec<&'static str> = Vec::new();
            let mut watch = |task: TrackedTask, join_set: &mut tokio::task::JoinSet<_>| {
                let idx = names.len();
                names.push(task.name);
                join_set.spawn(async move {
                    let result = task.handle.await;
                    (idx, result)
                });
            };
            for task in tasks {
                watch(task, &mut join_set);
            }

            // Wait for the first completion, picking up late registrations
            // as they arrive.
            let (idx, result) = loop {
                let added = std::mem::take(&mut *late.pending.lock());
                for task in added {
                    watch(task, &mut join_set);
                }
                if join_set.is_empty() {
                    // Nothing to watch yet; wait for a late registration.
                    late.added.notified().await;
                    continue;
                }
                // `biased`: an exit already reported wins over a new
                // registration. Both arms are cancel-safe: `join_next` keeps
                // unreported exits in the set, and a lost `notified` leaves
                // the registration in `pending` for the next pass.
                tokio::select! {
                    biased;
                    joined = join_set.join_next() => {
                        break joined
                            .expect("JoinSet is non-empty")
                            .expect("wrapper task should not panic");
                    }
                    () = late.added.notified() => {}
                }
            };

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

    /// A task registered through a late registrar AFTER the wait started is
    /// still watched: its exit resolves the wait. Also with nothing
    /// registered before the wait, the path the node takes when the
    /// transport's accept loop is the first late task.
    #[tokio::test]
    async fn late_registration_after_the_wait_is_watched() {
        for with_early_task in [true, false] {
            let monitor = BackgroundTaskMonitor::new();
            if with_early_task {
                monitor.register(
                    "sleeper",
                    tokio::spawn(async {
                        tokio::time::sleep(Duration::from_secs(60)).await;
                    }),
                );
            }
            let registrar = monitor.late_registrar();
            let wait = tokio::spawn(monitor.wait_for_any_exit());
            // Let the wait start and park before the registration.
            tokio::time::sleep(Duration::from_millis(20)).await;
            assert!(!wait.is_finished());
            let (stop, stopped) = tokio::sync::oneshot::channel::<()>();
            registrar.register(
                "late_task",
                tokio::spawn(async move {
                    // Ends when `stop` is dropped; the value is irrelevant.
                    stopped.await.ok();
                }),
            );
            tokio::time::sleep(Duration::from_millis(20)).await;
            assert!(
                !wait.is_finished(),
                "a running late task does not resolve the wait"
            );
            drop(stop);
            let err = tokio::time::timeout(Duration::from_secs(5), wait)
                .await
                .expect("the late task's exit resolves the wait")
                .expect("wait task");
            let msg = err.to_string();
            assert!(
                msg.contains("late_task") && msg.contains("exited unexpectedly"),
                "with_early_task={with_early_task}: {msg}"
            );
        }
    }

    /// A task registered through a late registrar BEFORE the wait started
    /// is watched too, and a late task that panics is reported as a panic.
    #[tokio::test]
    async fn late_registration_before_the_wait_is_watched() {
        let monitor = BackgroundTaskMonitor::new();
        monitor.late_registrar().register(
            "late_panicker",
            tokio::spawn(async {
                tokio::time::sleep(Duration::from_millis(10)).await;
                panic!("late task panic");
            }),
        );
        let err = tokio::time::timeout(Duration::from_secs(5), monitor.wait_for_any_exit())
            .await
            .expect("the late task's panic resolves the wait");
        let msg = err.to_string();
        assert!(
            msg.contains("late_panicker") && msg.contains("panicked"),
            "{msg}"
        );
    }
    /// Dropping the wait future (the node stopping) aborts the tasks
    /// registered through a late registrar, before or after the wait
    /// started, and a task registered after the drop is aborted at once.
    /// Dropping a `JoinHandle` only detaches its task, so without this a
    /// late task that never returns (the QUIC accept loop parks) would
    /// outlive the node.
    #[tokio::test]
    async fn dropping_the_wait_aborts_late_tasks() {
        // A task that never returns; the receiver sees the sender dropped
        // only when the task is aborted.
        let parked = || {
            let (tx, rx) = tokio::sync::oneshot::channel::<()>();
            let handle = tokio::spawn(async move {
                let _tx = tx;
                std::future::pending::<()>().await;
            });
            (handle, rx)
        };
        let monitor = BackgroundTaskMonitor::new();
        let registrar = monitor.late_registrar();
        let (before, before_rx) = parked();
        registrar.register("before_wait", before);
        let mut wait = Box::pin(monitor.wait_for_any_exit());
        let (after, mut after_rx) = parked();
        registrar.register("after_wait", after);
        // Poll the wait so it picks up both tasks, then drop it.
        assert!(
            tokio::time::timeout(Duration::from_millis(50), &mut wait)
                .await
                .is_err(),
            "parked tasks do not resolve the wait"
        );
        assert!(
            after_rx
                .try_recv()
                .is_err_and(|e| e == tokio::sync::oneshot::error::TryRecvError::Empty),
            "premise: the task runs while the wait is alive"
        );
        drop(wait);
        for (name, rx) in [("before_wait", before_rx), ("after_wait", after_rx)] {
            tokio::time::timeout(Duration::from_secs(5), rx)
                .await
                .unwrap_or_else(|_| panic!("{name} is aborted when the wait is dropped"))
                .expect_err("the task never sends");
        }
        let (late, late_rx) = parked();
        registrar.register("after_drop", late);
        tokio::time::timeout(Duration::from_secs(5), late_rx)
            .await
            .expect("a task registered after the drop is aborted at once")
            .expect_err("the task never sends");
    }
}
