//! The rebuild loop on a virtual clock, with injected rebuilds and jitter.

use std::sync::Arc;
use std::time::Duration;

use parking_lot::Mutex;
use tokio::sync::{mpsc, oneshot, watch};

use super::*;
use crate::simulation::VirtualTime;
use crate::transport::quic::tests::{bounded, drive};

const LIMIT: Duration = Duration::from_secs(24 * 3600);

/// What a test sees of one [`run`]: when each rebuild was attempted, the
/// node's view, and the senders of the endpoints it built.
struct Loop {
    time: VirtualTime,
    /// The first endpoint's availability.
    first: watch::Sender<QuicAvailability>,
    node: QuicAvailabilityWatch,
    /// One per rebuild attempt: its time on `time`.
    attempts: mpsc::UnboundedReceiver<u64>,
    /// The availability of each endpoint a successful attempt built.
    built: Arc<Mutex<Vec<watch::Sender<QuicAvailability>>>>,
    stop: Option<oneshot::Sender<()>>,
    task: tokio::task::JoinHandle<RebuildEnd>,
}

/// Start [`run`]: attempt `i` fails when `outcome(i)` is `None`, and
/// otherwise builds an endpoint whose availability starts at that value,
/// with the tasks `tasks(i)` gives. Waits use the samples of `jitter`,
/// cycled.
fn start(
    outcome: impl Fn(usize) -> Option<QuicAvailability> + Send + 'static,
    tasks: impl Fn(usize) -> Vec<(&'static str, JoinHandle<()>)> + Send + 'static,
    jitter: Vec<f64>,
) -> Loop {
    start_serving(Vec::new(), outcome, tasks, jitter)
}

/// [`start`], with `first_tasks` serving the first endpoint.
fn start_serving(
    first_tasks: Vec<(&'static str, JoinHandle<()>)>,
    outcome: impl Fn(usize) -> Option<QuicAvailability> + Send + 'static,
    tasks: impl Fn(usize) -> Vec<(&'static str, JoinHandle<()>)> + Send + 'static,
    jitter: Vec<f64>,
) -> Loop {
    let time = VirtualTime::new();
    let (first, current) = QuicAvailabilityWatch::for_test(QuicAvailability::Up);
    let (node_sender, node) = QuicAvailabilityWatch::settable(QuicAvailability::Up);
    let (attempted, attempts) = mpsc::unbounded_channel();
    let built = Arc::new(Mutex::new(Vec::new()));
    let (stop, stopped) = oneshot::channel();
    let mut index = 0usize;
    let rebuild = {
        let time = time.clone();
        let built = built.clone();
        move || {
            let this = index;
            index += 1;
            attempted.send(time.now_nanos()).ok();
            let initial = outcome(this)?;
            let (sender, availability) = QuicAvailabilityWatch::for_test(initial);
            built.lock().push(sender);
            Some(Rebuilt {
                availability,
                tasks: tasks(this),
            })
        }
    };
    let mut samples = jitter.into_iter().cycle();
    let task = tokio::spawn(run(
        time.clone(),
        current,
        first_tasks,
        node_sender,
        rebuild,
        move || samples.next().expect("a sample"),
        stopped,
    ));
    Loop {
        time,
        first,
        node,
        attempts,
        built,
        stop: Some(stop),
        task,
    }
}

impl Loop {
    /// Move the clock until `n` more attempts were made; their times.
    async fn next_attempts(&mut self, n: usize) -> Vec<u64> {
        let (time, mut attempts) = (
            self.time.clone(),
            std::mem::replace(&mut self.attempts, mpsc::unbounded_channel().1),
        );
        let (seen, attempts) = drive(&time, LIMIT, async move {
            let mut seen = Vec::new();
            while seen.len() < n {
                seen.push(attempts.recv().await.expect("the loop runs"));
            }
            (seen, attempts)
        })
        .await;
        self.attempts = attempts;
        seen
    }

    /// Let every task run until it waits, without moving the clock.
    async fn settle() {
        for _ in 0..64 {
            tokio::task::yield_now().await;
        }
    }
}

/// After a death the loop waits 1 s, then twice as long before each later
/// attempt, up to 5 minutes, each wait scaled by its jitter sample, which is
/// kept within ±20%. The node reads `Down` throughout.
#[test]
fn failed_rebuilds_back_off_doubling_to_a_capped_jittered_wait() {
    bounded(|| async move {
        let samples = vec![0.8, 1.2, 1.0, 0.9, 1.1, 5.0, 0.0];
        let mut lp = start(|_| None, |_| Vec::new(), samples.clone());
        Loop::settle().await;
        assert_eq!(lp.node.get(), QuicAvailability::Up);
        let died_at = lp.time.now_nanos();
        lp.first.send(QuicAvailability::Down).unwrap();
        let times = lp.next_attempts(14).await;
        assert_eq!(lp.node.get(), QuicAvailability::Down);
        let bases = [1, 2, 4, 8, 16, 32, 64, 128, 256, 300, 300, 300, 300, 300];
        let mut previous = died_at;
        for (i, at) in times.iter().enumerate() {
            let sample = samples[i % samples.len()].clamp(0.8, 1.2);
            let expected = Duration::from_secs(bases[i]).mul_f64(sample);
            assert_eq!(
                Duration::from_nanos(at - previous),
                expected,
                "wait before attempt {i}"
            );
            assert!(expected <= Duration::from_secs(360));
            previous = *at;
        }
        drop(lp.stop.take());
        assert_eq!(lp.task.await.unwrap(), RebuildEnd::Stopped);
    })
}

/// Stopping the listener ends a wait at once: the clock does not move.
#[test]
fn stopping_interrupts_the_backoff() {
    bounded(|| async move {
        let mut lp = start(|_| None, |_| Vec::new(), vec![1.0]);
        lp.first.send(QuicAvailability::Down).unwrap();
        // Ten attempts in, the next wait is the 5 minute cap.
        lp.next_attempts(10).await;
        Loop::settle().await;
        let before = lp.time.now_nanos();
        assert!(!lp.task.is_finished(), "waiting for the next attempt");
        drop(lp.stop.take());
        Loop::settle().await;
        assert!(lp.task.is_finished(), "the stop ended the wait");
        assert_eq!(lp.time.now_nanos(), before, "without moving the clock");
        assert_eq!(lp.task.await.unwrap(), RebuildEnd::Stopped);
        assert!(lp.attempts.try_recv().is_err(), "no attempt after the stop");
    })
}

/// The loop stops on the stop before any death too, and when the node lets
/// the endpoint go (`Released`), without touching the node's view.
#[test]
fn the_loop_stops_on_a_stop_or_a_release() {
    bounded(|| async move {
        let mut lp = start(|_| Some(QuicAvailability::Up), |_| Vec::new(), vec![1.0]);
        drop(lp.stop.take());
        assert_eq!(lp.task.await.unwrap(), RebuildEnd::Stopped);

        let lp = start(|_| Some(QuicAvailability::Up), |_| Vec::new(), vec![1.0]);
        lp.first.send(QuicAvailability::Released).unwrap();
        assert_eq!(lp.task.await.unwrap(), RebuildEnd::Stopped);
        assert_eq!(lp.node.get(), QuicAvailability::Up);
        assert!(lp.built.lock().is_empty());
    })
}

/// A rebuilt endpoint turns the node's view `Up` again and is watched in
/// turn. If it dies before [`REBUILD_STABLE`], the next wait goes on
/// doubling; if it stayed up that long, the next wait is 1 s again.
#[test]
fn the_backoff_starts_again_only_after_a_rebuilt_endpoint_stayed_up() {
    bounded(|| async move {
        let mut lp = start(|_| Some(QuicAvailability::Up), |_| Vec::new(), vec![1.0]);
        let died = lp.time.now_nanos();
        lp.first.send(QuicAvailability::Down).unwrap();
        let first = lp.next_attempts(1).await[0];
        assert_eq!(Duration::from_nanos(first - died), Duration::from_secs(1));
        Loop::settle().await;
        assert_eq!(lp.node.get(), QuicAvailability::Up, "rebuilt");

        // Dies a minute later: the next wait is 2 s.
        lp.time.advance(Duration::from_secs(60));
        let died = lp.time.now_nanos();
        lp.built.lock()[0].send(QuicAvailability::Down).unwrap();
        let second = lp.next_attempts(1).await[0];
        assert_eq!(Duration::from_nanos(second - died), Duration::from_secs(2));
        Loop::settle().await;
        assert_eq!(lp.node.get(), QuicAvailability::Up);

        // Just short of the stable time: 4 s.
        lp.time.advance(REBUILD_STABLE - Duration::from_millis(1));
        let died = lp.time.now_nanos();
        lp.built.lock()[1].send(QuicAvailability::Down).unwrap();
        let third = lp.next_attempts(1).await[0];
        assert_eq!(Duration::from_nanos(third - died), Duration::from_secs(4));
        Loop::settle().await;

        // Up for the stable time: back to 1 s.
        lp.time.advance(REBUILD_STABLE);
        let died = lp.time.now_nanos();
        lp.built.lock()[2].send(QuicAvailability::Down).unwrap();
        Loop::settle().await;
        assert_eq!(lp.node.get(), QuicAvailability::Down);
        let fourth = lp.next_attempts(1).await[0];
        assert_eq!(Duration::from_nanos(fourth - died), Duration::from_secs(1));
        drop(lp.stop.take());
        assert_eq!(lp.task.await.unwrap(), RebuildEnd::Stopped);
    })
}

/// A rebuilt endpoint that is already down when the loop reads it is
/// rebuilt again after the next, longer wait; the node's view stays `Down`.
#[test]
fn an_endpoint_down_at_once_is_rebuilt_again() {
    bounded(|| async move {
        let mut lp = start(|_| Some(QuicAvailability::Down), |_| Vec::new(), vec![1.0]);
        let died = lp.time.now_nanos();
        lp.first.send(QuicAvailability::Down).unwrap();
        let times = lp.next_attempts(3).await;
        let gaps: Vec<_> = std::iter::once(died)
            .chain(times.iter().copied())
            .collect::<Vec<_>>()
            .windows(2)
            .map(|w| Duration::from_nanos(w[1] - w[0]))
            .collect();
        // Each attempt follows the one before it, with no reset.
        let expected = [1, 2, 4].map(Duration::from_secs);
        assert_eq!(gaps, expected);
        assert_eq!(lp.node.get(), QuicAvailability::Down);
        drop(lp.stop.take());
        assert_eq!(lp.task.await.unwrap(), RebuildEnd::Stopped);
    })
}

/// The loop owns the tasks of the endpoint it built: replacing them aborts
/// them, and one that ends while its endpoint stays up ends the loop, after
/// [`TASK_END_DEATH_WAIT`], as the end of a registered task would end the
/// node.
#[test]
fn the_loop_aborts_replaced_tasks_and_ends_when_one_ends() {
    bounded(|| async move {
        let (dropped_tx, mut dropped) = mpsc::unbounded_channel::<usize>();
        let (end_tx, end_rx) = oneshot::channel::<()>();
        let end_rx = Arc::new(Mutex::new(Some(end_rx)));
        let tasks = move |index: usize| {
            struct Guard(usize, mpsc::UnboundedSender<usize>);
            impl Drop for Guard {
                fn drop(&mut self) {
                    self.1.send(self.0).ok();
                }
            }
            let guard = Guard(index, dropped_tx.clone());
            let end = if index == 1 {
                end_rx.lock().take()
            } else {
                None
            };
            vec![(
                "served",
                tokio::spawn(async move {
                    let _guard = guard;
                    match end {
                        Some(end) => {
                            end.await.ok();
                        }
                        None => std::future::pending::<()>().await,
                    }
                }),
            )]
        };
        let mut lp = start(|_| Some(QuicAvailability::Up), tasks, vec![1.0]);
        lp.first.send(QuicAvailability::Down).unwrap();
        lp.next_attempts(1).await;
        Loop::settle().await;
        assert!(
            dropped.try_recv().is_err(),
            "the first endpoint's task runs"
        );
        lp.built.lock()[0].send(QuicAvailability::Down).unwrap();
        lp.next_attempts(1).await;
        Loop::settle().await;
        assert_eq!(dropped.try_recv(), Ok(0), "replaced, so aborted");
        // The second endpoint's task ends on its own, and the endpoint stays
        // up: a failure, once the wait for a death is over.
        end_tx.send(()).unwrap();
        Loop::settle().await;
        assert!(!lp.task.is_finished(), "waiting for a death");
        let ended_at = lp.time.now_nanos();
        let task = std::mem::replace(&mut lp.task, tokio::spawn(async { RebuildEnd::Stopped }));
        let end = drive(&lp.time, LIMIT, async move { task.await.unwrap() }).await;
        assert_eq!(end, RebuildEnd::TaskEnded("served"));
        assert_eq!(
            Duration::from_nanos(lp.time.now_nanos() - ended_at),
            TASK_END_DEATH_WAIT
        );
        assert_eq!(dropped.try_recv(), Ok(1));
        assert_eq!(lp.node.get(), QuicAvailability::Up, "not a death");
    })
}

/// A task serving the first endpoint (the rebuild loop owns those too) that
/// ends just before the endpoint is reported `Down`, as a task reaching
/// quinn's lock poisoned by a driver panic does (#6184), is that death: the
/// loop rebuilds the endpoint instead of ending. A task that ends with the
/// endpoint up ends the loop.
#[test]
fn a_task_ending_as_the_endpoint_dies_is_the_death() {
    bounded(|| async move {
        let (end_tx, end_rx) = oneshot::channel::<()>();
        let first = vec![(
            "quic_accept_loop",
            tokio::spawn(async move {
                end_rx.await.ok();
            }),
        )];
        let mut lp = start_serving(
            first,
            |_| Some(QuicAvailability::Up),
            |_| Vec::new(),
            vec![1.0],
        );
        Loop::settle().await;
        end_tx.send(()).unwrap();
        Loop::settle().await;
        assert!(!lp.task.is_finished(), "waiting for the death");
        lp.time.advance(TASK_END_DEATH_WAIT / 2);
        Loop::settle().await;
        lp.first.send(QuicAvailability::Down).unwrap();
        Loop::settle().await;
        assert_eq!(lp.node.get(), QuicAvailability::Down, "taken for the death");
        let died = lp.time.now_nanos();
        let at = lp.next_attempts(1).await[0];
        assert_eq!(Duration::from_nanos(at - died), Duration::from_secs(1));
        Loop::settle().await;
        assert_eq!(lp.node.get(), QuicAvailability::Up, "rebuilt");
        assert!(!lp.task.is_finished());
        drop(lp.stop.take());
        assert_eq!(lp.task.await.unwrap(), RebuildEnd::Stopped);

        // The same end with the endpoint left up: a failure.
        let first = vec![("quic_accept_loop", tokio::spawn(async {}))];
        let lp = start_serving(
            first,
            |_| Some(QuicAvailability::Up),
            |_| Vec::new(),
            vec![1.0],
        );
        let task = lp.task;
        let end = drive(&lp.time, LIMIT, async move { task.await.unwrap() }).await;
        assert_eq!(end, RebuildEnd::TaskEnded("quic_accept_loop"));
        assert!(lp.built.lock().is_empty());
    })
}

/// Off by default; never on a node that exits for its supervisor; and a test
/// turns it on only for the node whose trace asks.
#[test]
fn rebuilding_is_off_and_never_for_a_supervised_exit() {
    const { assert!(!ENDPOINT_REBUILD, "ships dark until the UDP removal") };
    assert!(enabled_with(false, true));
    assert!(!enabled_with(true, true), "a supervised node exits instead");
    assert!(!enabled_with(false, false));
    assert!(!enabled_with(true, false));
    let addr: SocketAddr = "10.62.72.1:4000".parse().unwrap();
    assert!(!enabled_at(false, addr), "no trace asks");
}

/// The wait for attempt `n` is the backoff's, scaled by the clamped sample.
#[test]
fn delay_scales_by_the_clamped_sample() {
    assert_eq!(delay(0, 1.0), Duration::from_secs(1));
    assert_eq!(delay(3, 1.0), Duration::from_secs(8));
    assert_eq!(delay(40, 1.0), Duration::from_secs(300));
    assert_eq!(delay(40, 9.0), Duration::from_secs(360));
    assert_eq!(delay(0, 0.0), Duration::from_millis(800));
    for _ in 0..1000 {
        let sample = jitter_sample();
        assert!((0.8..=1.2).contains(&sample), "{sample}");
    }
}

/// The backstop `liveness::driver_death_restart` reads a process-wide flag
/// that only a node that exits for an endpoint death can have set, so it is
/// sound only while no node both exits for a death and rebuilds: the switch
/// that makes the listener exit (`gateway_quic_restarts`, which also decides
/// `exit_for_endpoint_death`'s caller) turns rebuilding off, in every role
/// and configuration, and the listener passes exactly that switch.
#[test]
fn a_node_never_both_exits_for_a_death_and_rebuilds() {
    use crate::transport::quic::liveness::gateway_quic_restarts_with;
    for is_gateway in [false, true] {
        for enabled in [false, true] {
            for udp_fallback in [false, true] {
                let exits = gateway_quic_restarts_with(is_gateway, enabled, udp_fallback);
                for switch in [false, true] {
                    assert!(
                        !(exits && enabled_with(exits, switch)),
                        "gateway={is_gateway} enabled={enabled} udp_fallback={udp_fallback}"
                    );
                }
            }
        }
    }
    // Scoped to `new_listener`'s body with comments stripped, so neither a
    // call elsewhere in the file nor a commented-out one satisfies it.
    let body = crate::transport::peer_connection::inbound_budget::pin::fn_body(
        include_str!("../../connection_handler.rs"),
        "fn new_listener(",
    );
    let flat: String = body.split_whitespace().collect();
    assert!(
        flat.contains(
            "rebuild::enabled_at(super::quic::liveness::gateway_quic_restarts(is_gateway),"
        ),
        "the listener must feed enabled_at the switch that arms its exit"
    );
}
