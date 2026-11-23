//! Rebuilding the node's QUIC endpoint in process when it dies (#6272).
//!
//! Once the UDP transport is gone (#6151), a node whose QUIC endpoint died
//! (`liveness`, availability `Down`) has no transport at all. A node run
//! under a service supervisor (`FREENET_SUPERVISED`) exits through
//! `ListenerExit::QuicEndpointDown` and is restarted, and the crash breaker
//! counts those exits. A node with no supervisor has nobody to restart it,
//! so it rebuilds the endpoint itself, here:
//!
//! - It waits [`delay`] before each attempt: 1 s, doubling to a 5 minute cap
//!   ([`REBUILD_BACKOFF`]), each jittered by up to ±20% ([`REBUILD_JITTER`])
//!   so nodes that lost their endpoints together do not retry together. The
//!   wait is interruptible: the listener stopping ends it at once.
//! - It retries for as long as the node runs. An attempt that fails (the
//!   endpoint cannot be built) is logged and the next waits longer.
//! - An attempt builds the endpoint exactly as the transport did at bind,
//!   on the node's one socket, clock, settings and key (`QuicEndpoint::rebuild`),
//!   replacing the socket's QUIC ingress so the new endpoint starts with an
//!   empty allow-set and nothing the dead one queued. The transport then
//!   starts what runs on it (accept loop, upgrades, attempt router) and swaps
//!   all of it in for the dead endpoint's. New connections, native or
//!   upgraded, use the new endpoint; connections opened on the dead one were
//!   already closed locally by their watchers when it went `Down`, and the
//!   dead endpoint, its upgrades and their tasks are dropped by the swap.
//! - It owns the tasks serving the endpoint, the first one's included,
//!   rather than the node's task monitor: after a quinn driver panic, a task
//!   that still reaches quinn's poisoned lock (the accept loop, polled on
//!   another worker just before `Down` is reported) panics as the endpoint
//!   dies (#6184), and on this node that is the death to repair, not a
//!   failure that ends the node. (A node that exits for a death instead
//!   takes the monitor's backstop, `liveness::driver_death_restart`.) A task that ends while the endpoint stays up
//!   ([`TASK_END_DEATH_WAIT`]) still ends the node.
//! - The backoff starts again from 1 s only after a rebuilt endpoint has
//!   stayed up for [`REBUILD_STABLE`]. An endpoint that dies sooner keeps
//!   the backoff where it was, so a fault that kills every new endpoint is
//!   retried at most once per capped delay.
//!
//! Dark: [`ENDPOINT_REBUILD`] is off, so no node rebuilds in production.
//! Until the UDP removal an unsupervised node falls back to UDP when its
//! endpoint dies and needs no rebuild; the removal (D-8) turns this on
//! together with `breaker::UDP_FALLBACK = false`. A test turns it on for one
//! node through its simulation trace (`QuicUpgradeTrace::rebuild_endpoint`).
//!
//! The node sees the endpoint through one availability watch of its own
//! (`QuicAvailabilityWatch::settable`) that follows the current endpoint
//! and goes from `Down` back to `Up` after a rebuild; each endpoint's own
//! watch keeps its final `Down`.

use std::future::Future;
use std::net::SocketAddr;
use std::time::Duration;

use tokio::sync::{oneshot, watch};
use tokio::task::JoinHandle;

use super::liveness::{QuicAvailability, QuicAvailabilityWatch};
use crate::config::GlobalRng;
use crate::simulation::TimeSource;
use crate::util::backoff::ExponentialBackoff;

/// Whether a node with no supervisor rebuilds its QUIC endpoint when it dies.
/// Off until the UDP removal (D-8), which turns it on with
/// `breaker::UDP_FALLBACK = false`.
pub(crate) const ENDPOINT_REBUILD: bool = false;

/// The wait before each rebuild attempt, before jitter: 1 s, doubling, capped
/// at 5 minutes.
pub(crate) const REBUILD_BACKOFF: ExponentialBackoff =
    ExponentialBackoff::new(Duration::from_secs(1), Duration::from_secs(300));

/// How far each wait is jittered either way, as a fraction of it.
pub(crate) const REBUILD_JITTER: f64 = 0.2;

/// How long a rebuilt endpoint must stay up before the backoff starts again
/// from 1 s. Ten minutes, longer than the longest jittered wait (6 minutes):
/// a fault that kills each new endpoint after some time T then rebuilds at
/// most once per `max(T, 10 min)` if T is long enough to reset the backoff,
/// and at most once per capped wait plus T if it is not, so no T makes the
/// node rebuild faster than about once per 4 minutes.
pub(crate) const REBUILD_STABLE: Duration = Duration::from_secs(600);

/// How long [`run`] waits, after a task serving the endpoint ended, for the
/// endpoint's death to be reported before it takes the end for a failure:
/// the wait a node that exits for a death takes for the same report
/// (`liveness::driver_death_restart`). After a panic in a quinn driver
/// poisons quinn's lock, a task that still reaches it (the accept loop
/// polled on another worker in the moment before the watcher reports
/// `Down`) panics, and can do so just before the report, which follows the
/// panic within its unwind. The two never apply to one node: a node that
/// exits for a death never rebuilds ([`enabled_at`]).
pub(crate) const TASK_END_DEATH_WAIT: Duration = super::liveness::DRIVER_PANIC_REPORT_WAIT;

/// Whether the node bound at `bind_addr` rebuilds its endpoint when it dies.
/// Never one that exits for its supervisor to restart it (`supervised_exit`,
/// `liveness::gateway_quic_restarts`): the two would race for the same death.
pub(crate) fn enabled_at(supervised_exit: bool, bind_addr: SocketAddr) -> bool {
    enabled_with(
        supervised_exit || forced_supervised(bind_addr),
        ENDPOINT_REBUILD || forced_on(bind_addr),
    )
}

fn enabled_with(supervised_exit: bool, switch: bool) -> bool {
    switch && !supervised_exit
}

#[cfg(any(test, feature = "testing"))]
fn forced_on(bind_addr: SocketAddr) -> bool {
    crate::transport::in_memory_socket::quic_upgrade_trace_at(bind_addr)
        .is_some_and(|trace| trace.rebuilds_endpoint())
}

#[cfg(not(any(test, feature = "testing")))]
fn forced_on(_bind_addr: SocketAddr) -> bool {
    false
}

/// Whether a test treats the node at `bind_addr` as one that exits for its
/// supervisor (`QuicUpgradeTrace::supervise`), as `FREENET_SUPERVISED` does
/// in the binary, which a test cannot set for one node of a process.
#[cfg(any(test, feature = "testing"))]
fn forced_supervised(bind_addr: SocketAddr) -> bool {
    crate::transport::in_memory_socket::quic_upgrade_trace_at(bind_addr)
        .is_some_and(|trace| trace.supervised())
}

#[cfg(not(any(test, feature = "testing")))]
fn forced_supervised(_bind_addr: SocketAddr) -> bool {
    false
}

/// The wait before rebuild attempt `attempt` (0 for the first after a
/// death), scaled by `sample`, which is clamped to `1 ± REBUILD_JITTER`.
pub(crate) fn delay(attempt: u32, sample: f64) -> Duration {
    let sample = sample.clamp(1.0 - REBUILD_JITTER, 1.0 + REBUILD_JITTER);
    REBUILD_BACKOFF.delay(attempt).mul_f64(sample)
}

/// A jitter sample for [`delay`], from the run's RNG (deterministic in a
/// simulation).
pub(crate) fn jitter_sample() -> f64 {
    GlobalRng::random_range((1.0 - REBUILD_JITTER)..=(1.0 + REBUILD_JITTER))
}

/// Builds an endpoint in place of a dead one, on the node's socket
/// (`connection_handler::endpoint_rebuilder`); `None` when it cannot.
pub(crate) type EndpointFactory = Box<dyn FnMut() -> Option<super::QuicEndpoint> + Send>;

/// An endpoint built in place of a dead one, already swapped in: its
/// availability, and the tasks that serve it, which [`run`] owns from then
/// on and aborts when it replaces them or ends.
pub(crate) struct Rebuilt {
    pub(crate) availability: QuicAvailabilityWatch,
    pub(crate) tasks: Vec<(&'static str, JoinHandle<()>)>,
}

/// Why [`run`] returned.
#[derive(Debug, PartialEq, Eq)]
pub(crate) enum RebuildEnd {
    /// The listener stopped, or the node let its endpoint go (`Released`).
    Stopped,
    /// A task serving the endpoint ended while the endpoint stayed up. They
    /// never end on their own (each parks when it stops), so this is a
    /// failure, as the end of a registered task is to the node's monitor.
    TaskEnded(&'static str),
}

/// Rebuild the endpoint whenever it dies, until `stop` resolves (its sender
/// is held by the listener, so dropping the listener resolves it).
///
/// `current` is the availability of the endpoint the node holds now, and
/// `first_tasks` the tasks serving it, which this owns as it owns a rebuilt
/// endpoint's; `node` is the node's own view, which this sets: `Down` when
/// the endpoint dies, and the new endpoint's value once one is built.
/// `rebuild` builds and swaps in a new endpoint, or returns `None` when it
/// cannot. `jitter` gives each wait's sample for [`delay`]. Every wait is on
/// `time`.
///
/// A task that ends is taken for the endpoint's death when the endpoint goes
/// `Down` within [`TASK_END_DEATH_WAIT`], and for a failure
/// ([`RebuildEnd::TaskEnded`]) otherwise.
///
/// The `select!`s are `biased` so `stop` is always seen first; every arm is
/// cancel-safe (a oneshot receiver, `not_up`, a sleep, and polls of
/// `JoinHandle`s) and none is busy.
pub(crate) async fn run<T: TimeSource>(
    time: T,
    mut current: QuicAvailabilityWatch,
    first_tasks: Vec<(&'static str, JoinHandle<()>)>,
    node: watch::Sender<QuicAvailability>,
    mut rebuild: impl FnMut() -> Option<Rebuilt>,
    mut jitter: impl FnMut() -> f64,
    mut stop: oneshot::Receiver<()>,
) -> RebuildEnd {
    let mut tasks = OwnedTasks(first_tasks);
    let mut attempt: u32 = 0;
    let mut up_since = time.now_nanos();
    loop {
        let seen = tokio::select! {
            biased;
            _ = &mut stop => return RebuildEnd::Stopped,
            (name, how) = tasks.any_ended() => {
                // The ended task is not polled again: a rebuild replaces
                // it, and every other way out returns.
                tokio::select! {
                    biased;
                    _ = &mut stop => return RebuildEnd::Stopped,
                    seen = current.not_up() => {
                        tracing::warn!(
                            task = name,
                            ended = %how,
                            availability = ?seen,
                            "A task serving the QUIC endpoint ended as the endpoint went down"
                        );
                        seen
                    }
                    () = time.sleep(TASK_END_DEATH_WAIT) => {
                        tracing::error!(
                            task = name,
                            ended = %how,
                            "A task serving the QUIC endpoint ended while it stayed up"
                        );
                        return RebuildEnd::TaskEnded(name);
                    }
                }
            }
            seen = current.not_up() => seen,
        };
        if seen != QuicAvailability::Down {
            return RebuildEnd::Stopped;
        }
        node.send_replace(QuicAvailability::Down);
        let up_for = Duration::from_nanos(time.now_nanos().saturating_sub(up_since));
        if up_for >= REBUILD_STABLE {
            attempt = 0;
        }
        loop {
            let wait = delay(attempt, jitter());
            attempt = attempt.saturating_add(1);
            tracing::info!(
                attempt,
                wait_ms = wait.as_millis() as u64,
                "The QUIC endpoint is down; rebuilding it after the wait"
            );
            tokio::select! {
                biased;
                _ = &mut stop => return RebuildEnd::Stopped,
                () = time.sleep(wait) => {}
            }
            match rebuild() {
                Some(rebuilt) => {
                    tasks.replace(rebuilt.tasks);
                    current = rebuilt.availability;
                    up_since = time.now_nanos();
                    let now = current.get();
                    node.send_replace(now);
                    tracing::info!(attempt, availability = ?now, "QUIC endpoint rebuilt");
                    break;
                }
                None => {
                    // The factory logged why, at WARN.
                    tracing::debug!(attempt, "Could not rebuild the QUIC endpoint; retrying");
                }
            }
        }
    }
}

/// The tasks serving the current endpoint, aborted when replaced or
/// dropped (a task left to park would hold nothing, but would add one parked
/// task per rebuild for the life of the node).
struct OwnedTasks(Vec<(&'static str, JoinHandle<()>)>);

impl OwnedTasks {
    fn replace(&mut self, tasks: Vec<(&'static str, JoinHandle<()>)>) {
        for (_, task) in std::mem::replace(&mut self.0, tasks) {
            task.abort();
        }
    }

    /// Resolve with the name of the first task that ends; never while there
    /// is none. A task that ended is not polled again: the caller returns.
    async fn any_ended(&mut self) -> (&'static str, String) {
        std::future::poll_fn(|cx| {
            for (name, task) in &mut self.0 {
                if let std::task::Poll::Ready(outcome) = std::pin::Pin::new(task).poll(cx) {
                    let how = match outcome {
                        Ok(()) => "returned".to_string(),
                        // The task's panic or cancellation, as tokio words it.
                        Err(error) => error.to_string(),
                    };
                    return std::task::Poll::Ready((*name, how));
                }
            }
            std::task::Poll::Pending
        })
        .await
    }
}

impl Drop for OwnedTasks {
    fn drop(&mut self) {
        for (_, task) in &self.0 {
            task.abort();
        }
    }
}

#[cfg(test)]
mod tests;
