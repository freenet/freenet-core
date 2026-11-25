//! The update check before a transport-fatal exit (#5940 phase D, W-S6).
//!
//! With no UDP to fall back to ([`super::breaker::udp_fallback`] is `false`),
//! a node whose QUIC endpoint cannot be built, or whose endpoint died, has no
//! transport, and the process exits for its supervisor. The binary's update
//! task runs beside the node and may not have asked GitHub yet (its first
//! check waits a random 0 to 60 s), so a node that exits within seconds of
//! start would never find the release that fixes it. Before such an exit the
//! library therefore runs the check the binary registered, bounded by
//! [`FATAL_EXIT_UPDATE_CHECK_TIMEOUT`] so a dead network cannot hold the
//! exit. It runs only under our own supervisor, whose `ExecStopPost` runs
//! `freenet update` after the exit and so can use what the check staged; an
//! unsupervised node has nothing that would install it and exits at once.
//!
//! Dark: while [`super::breaker::UDP_FALLBACK`] is `true` neither
//! [`bind_failure_is_fatal`] nor [`check_before_exit`] does anything, so the
//! exits keep their present shape. The constant is the only switch; there is
//! no runtime flag. A library user that registers no check never has its
//! process exited by a bind failure.

use std::future::Future;
use std::pin::Pin;
use std::sync::OnceLock;
use std::time::Duration;

/// How the registered check ended.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FatalExitCheckEnd {
    /// It ran to its end (or skipped itself); the exit proceeds.
    Finished,
    /// An operator asked the process to stop while it ran. The process then
    /// exits as for a stop (code 0), not with the code of the fault: a
    /// service manager's wrapper reads 42 as "update and relaunch", and the
    /// post-update rollback counts 45 as a crash.
    Stopped,
}

/// The check the binary registers: look for a newer release and stage it.
/// It reports through its own logs; the exit code is the caller's.
pub type FatalExitUpdateCheck = fn() -> Pin<Box<dyn Future<Output = FatalExitCheckEnd> + Send>>;

static CHECK: OnceLock<FatalExitUpdateCheck> = OnceLock::new();

/// How long a transport-fatal exit waits for the registered check. The
/// systemd unit's `TimeoutStopSec` is 45 s and a SIGKILL after it reaches
/// `ExecStopPost` as `KILL`, which the post-update rollback counts as a crash;
/// the check, the exit's 0.5 s log flush and a stop that arrives during the
/// check must all fit inside it, so the bound is well under half of it. 20 s
/// covers the release lookup and, usually, the staging download; when it
/// does not, `ExecStopPost`'s own `freenet update` downloads the rest.
pub const FATAL_EXIT_UPDATE_CHECK_TIMEOUT: Duration = Duration::from_secs(20);

/// Register the update check to run before a transport-fatal exit. Call once
/// from the binary, before the node is built; a second call is ignored.
pub fn register_fatal_exit_update_check(check: FatalExitUpdateCheck) {
    #[allow(clippy::let_underscore_must_use)]
    let _ = CHECK.set(check);
}

/// What [`run_check`] found.
#[derive(Debug, PartialEq, Eq)]
pub(crate) enum CheckOutcome {
    /// No check is registered.
    NotRegistered,
    /// The check returned.
    Completed,
    /// An operator's stop ended the check.
    Stopped,
    /// The check was still running at the timeout and was dropped.
    TimedOut,
    /// The check panicked. The exit still happens with its planned code: a
    /// panic unwinding out of here would turn the deliberate 42 into 101,
    /// which the post-update rollback counts as a crash.
    Panicked,
}

/// Run `check`, if there is one, for at most `timeout`.
pub(crate) async fn run_check(
    check: Option<FatalExitUpdateCheck>,
    timeout: Duration,
) -> CheckOutcome {
    let Some(check) = check else {
        return CheckOutcome::NotRegistered;
    };
    // `tokio::time::timeout`, not a `TimeSource`: the exit is process-level
    // and real-time (the service manager's stop timeout is wall-clock), and
    // only the binary registers a check, so simulated time never applies.
    use futures::FutureExt;
    let guarded = std::panic::AssertUnwindSafe(async { check().await }).catch_unwind();
    match tokio::time::timeout(timeout, guarded).await {
        Ok(Ok(FatalExitCheckEnd::Finished)) => CheckOutcome::Completed,
        Ok(Ok(FatalExitCheckEnd::Stopped)) => CheckOutcome::Stopped,
        Ok(Err(_)) => CheckOutcome::Panicked,
        Err(_) => CheckOutcome::TimedOut,
    }
}

/// Whether a QUIC endpoint that cannot be built ends the process: only with
/// no UDP to fall back to, and only in a process whose binary registered the
/// check, which is also the binary that owns the process exit.
pub(crate) fn bind_failure_is_fatal() -> bool {
    bind_failure_is_fatal_with(super::breaker::udp_fallback(), CHECK.get().is_some())
}

fn bind_failure_is_fatal_with(udp_fallback: bool, registered: bool) -> bool {
    !udp_fallback && registered
}

/// Test builds only: the check's bound in seconds, so a real-process test of
/// the timeout does not wait 20 s.
#[cfg(feature = "testing")]
const TEST_TIMEOUT_SECS_ENV: &str = "FREENET_TEST_FATAL_EXIT_CHECK_TIMEOUT_SECS";

fn timeout() -> Duration {
    #[cfg(feature = "testing")]
    if let Some(secs) = std::env::var(TEST_TIMEOUT_SECS_ENV)
        .ok()
        .and_then(|value| value.trim().parse::<u64>().ok())
    {
        return Duration::from_secs(secs);
    }
    FATAL_EXIT_UPDATE_CHECK_TIMEOUT
}

/// Whether the check runs before a transport-fatal exit: with no UDP to fall
/// back to, and under our own supervisor (whose `ExecStopPost` can use what
/// the check staged).
fn runs_before_exit(udp_fallback: bool, supervised: bool) -> bool {
    !udp_fallback && supervised
}

/// Run the registered check before a transport-fatal exit, when
/// [`runs_before_exit`]. A no-op otherwise, so today's exits (UDP present) are
/// unchanged, and it logs nothing before the gate.
pub(crate) async fn check_before_exit(reason: &str) {
    if !runs_before_exit(
        super::breaker::udp_fallback(),
        super::liveness::supervised_exits_enabled(),
    ) {
        return;
    }
    let timeout = timeout();
    // The root cause first: a stop during the check must not lose it.
    tracing::warn!(
        reason,
        timeout_secs = timeout.as_secs(),
        "Transport-fatal exit: running the update check first"
    );
    let outcome = run_check(CHECK.get().copied(), timeout).await;
    tracing::info!(
        reason,
        ?outcome,
        "Update check before a transport-fatal exit finished"
    );
    if outcome == CheckOutcome::Stopped {
        exit_as_stopped();
    }
}

/// An operator stopped the process during the check: exit as a stop. The QUIC
/// crash-breaker marker is left as it is. For an endpoint death it already
/// carries the noted exit, which is a real fault and still counts at the next
/// start; for a bind failure no marker was armed. Clearing it, as a clean
/// stop elsewhere does, would drop that note.
fn exit_as_stopped() -> ! {
    tracing::info!("Stopped during the update check before a transport-fatal exit; exiting 0");
    // The file log is written by a background worker that `process::exit` does
    // not wait for; see `node::p2p_impl::exit_for_gateway_quic_restart`.
    std::thread::sleep(Duration::from_millis(500));
    std::process::exit(0)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicU32, Ordering};

    static RAN: AtomicU32 = AtomicU32::new(0);

    fn finishing() -> Pin<Box<dyn Future<Output = FatalExitCheckEnd> + Send>> {
        Box::pin(async {
            RAN.fetch_add(1, Ordering::SeqCst);
            FatalExitCheckEnd::Finished
        })
    }

    fn stopping() -> Pin<Box<dyn Future<Output = FatalExitCheckEnd> + Send>> {
        Box::pin(async { FatalExitCheckEnd::Stopped })
    }

    fn panicking() -> Pin<Box<dyn Future<Output = FatalExitCheckEnd> + Send>> {
        Box::pin(async { panic!("test: the check panics") })
    }

    fn never_finishing() -> Pin<Box<dyn Future<Output = FatalExitCheckEnd> + Send>> {
        Box::pin(std::future::pending())
    }

    #[tokio::test]
    async fn run_check_reports_each_outcome() {
        let timeout = Duration::from_secs(5);
        assert_eq!(run_check(None, timeout).await, CheckOutcome::NotRegistered);
        let before = RAN.load(Ordering::SeqCst);
        assert_eq!(
            run_check(Some(finishing), timeout).await,
            CheckOutcome::Completed
        );
        assert_eq!(RAN.load(Ordering::SeqCst), before + 1, "the check ran");
        assert_eq!(
            run_check(Some(stopping), timeout).await,
            CheckOutcome::Stopped
        );
    }

    /// A panicking check is contained: the exit keeps its planned code.
    #[tokio::test]
    async fn a_panicking_check_is_contained() {
        let outcome = run_check(Some(panicking), Duration::from_secs(5)).await;
        assert_eq!(outcome, CheckOutcome::Panicked);
    }

    #[test]
    fn the_check_runs_only_with_no_udp_under_a_supervisor() {
        assert!(!runs_before_exit(true, true), "dark with UDP");
        assert!(!runs_before_exit(true, false));
        assert!(
            !runs_before_exit(false, false),
            "unsupervised: nothing installs it"
        );
        assert!(runs_before_exit(false, true));
    }

    /// A dead network must not hold the exit: a check that never ends is
    /// dropped at the timeout.
    #[tokio::test(start_paused = true)]
    async fn a_check_that_never_ends_is_dropped_at_the_timeout() {
        let started = tokio::time::Instant::now();
        // The outer bound makes a missing inner one a clean failure, not a hang.
        let outcome = tokio::time::timeout(
            FATAL_EXIT_UPDATE_CHECK_TIMEOUT * 2,
            run_check(Some(never_finishing), FATAL_EXIT_UPDATE_CHECK_TIMEOUT),
        )
        .await
        .expect("run_check must end at its own timeout");
        assert_eq!(outcome, CheckOutcome::TimedOut);
        assert_eq!(started.elapsed(), FATAL_EXIT_UPDATE_CHECK_TIMEOUT);
    }

    #[test]
    fn a_bind_failure_is_fatal_only_with_no_udp_and_a_registered_check() {
        assert!(!bind_failure_is_fatal_with(true, true), "dark with UDP");
        assert!(!bind_failure_is_fatal_with(true, false));
        assert!(!bind_failure_is_fatal_with(false, false), "no binary");
        assert!(bind_failure_is_fatal_with(false, true));
        assert!(!bind_failure_is_fatal(), "nothing registers in a unit test");
    }

    fn production(src: &str) -> String {
        let body = src
            .split_once("#[cfg(test)]\nmod tests {")
            .map_or(src, |(head, _)| head);
        body.lines()
            .filter(|line| !line.trim_start().starts_with("//"))
            .collect::<Vec<_>>()
            .join("\n")
            .split_whitespace()
            .collect()
    }

    /// The check is a no-op while UDP exists, and the three exit sites run it
    /// before they exit.
    #[test]
    fn the_exit_sites_run_the_check_before_exiting_and_it_is_dark_with_udp() {
        let own = production(include_str!("fatal_exit.rs"));
        let body = own
            .split_once("asyncfncheck_before_exit(")
            .expect("check_before_exit")
            .1;
        assert!(
            body.starts_with(
                "reason:&str){if!runs_before_exit(super::breaker::udp_fallback(),super::liveness::supervised_exits_enabled(),){return;}"
            ),
            "check_before_exit must return first while UDP exists: {body}"
        );

        let node = production(include_str!("../../node/p2p_impl.rs"));
        let restart = "exit_for_gateway_quic_restart(&";
        let check = "fatal_exit::check_before_exit(";
        let mut sites = 0;
        for (at, _) in node.match_indices(restart) {
            // Skip the definition's own parameter list.
            if node[..at].ends_with("fn") {
                continue;
            }
            sites += 1;
            assert!(
                node[..at].rfind(check).is_some_and(|c| at - c < 200),
                "a check_before_exit call must come right before the exit at {at}"
            );
        }
        assert_eq!(sites, 2, "the two gateway-restart exit sites");

        let bind = node
            .split_once("asyncfnexit_for_quic_bind_failure()->!{")
            .expect("exit_for_quic_bind_failure")
            .1;
        assert!(
            bind.find(check).expect("the check") < bind.find("process::exit(").expect("exit"),
            "the bind-failure exit must run the check before it exits"
        );

        let handler = production(include_str!("../connection_handler.rs"));
        assert!(
            handler.contains(
                "quic_endpoint.is_none()&&super::quic::fatal_exit::bind_failure_is_fatal()"
            ),
            "bind_udp_listener must exit through the fatal-exit path for a failed bind"
        );
    }
}
