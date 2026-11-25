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
//! exit.
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

/// The check the binary registers: look for a newer release and stage it.
/// It reports through its own logs; the exit code is the caller's.
pub type FatalExitUpdateCheck = fn() -> Pin<Box<dyn Future<Output = ()> + Send>>;

static CHECK: OnceLock<FatalExitUpdateCheck> = OnceLock::new();

/// How long a transport-fatal exit waits for the registered check: long
/// enough for the release lookup and the staging download on a slow link,
/// short enough that a dead network delays the supervisor's restart by under
/// a minute.
pub const FATAL_EXIT_UPDATE_CHECK_TIMEOUT: Duration = Duration::from_secs(45);

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
    /// The check was still running at the timeout and was dropped.
    TimedOut,
}

/// Run `check`, if there is one, for at most `timeout`.
pub(crate) async fn run_check(
    check: Option<FatalExitUpdateCheck>,
    timeout: Duration,
) -> CheckOutcome {
    let Some(check) = check else {
        return CheckOutcome::NotRegistered;
    };
    match tokio::time::timeout(timeout, check()).await {
        Ok(()) => CheckOutcome::Completed,
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

/// Run the registered check before a transport-fatal exit, when there is no
/// UDP to fall back to. A no-op otherwise, so today's exits are unchanged.
pub(crate) async fn check_before_exit(reason: &str) {
    if super::breaker::udp_fallback() {
        return;
    }
    let outcome = run_check(CHECK.get().copied(), FATAL_EXIT_UPDATE_CHECK_TIMEOUT).await;
    tracing::info!(
        reason,
        ?outcome,
        "Update check before a transport-fatal exit finished"
    );
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicU32, Ordering};

    static RAN: AtomicU32 = AtomicU32::new(0);

    fn finishing() -> Pin<Box<dyn Future<Output = ()> + Send>> {
        Box::pin(async {
            RAN.fetch_add(1, Ordering::SeqCst);
        })
    }

    fn never_finishing() -> Pin<Box<dyn Future<Output = ()> + Send>> {
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
    }

    /// A dead network must not hold the exit: a check that never ends is
    /// dropped at the timeout.
    #[tokio::test(start_paused = true)]
    async fn a_check_that_never_ends_is_dropped_at_the_timeout() {
        let started = tokio::time::Instant::now();
        let outcome = run_check(Some(never_finishing), FATAL_EXIT_UPDATE_CHECK_TIMEOUT).await;
        assert_eq!(outcome, CheckOutcome::TimedOut);
        assert_eq!(started.elapsed(), FATAL_EXIT_UPDATE_CHECK_TIMEOUT);
    }

    #[test]
    fn a_bind_failure_is_fatal_only_with_no_udp_and_a_registered_check() {
        assert!(!bind_failure_is_fatal_with(true, true), "dark with UDP");
        assert!(!bind_failure_is_fatal_with(true, false));
        assert!(!bind_failure_is_fatal_with(false, false), "no binary");
        assert!(bind_failure_is_fatal_with(false, true));
        assert!(
            super::super::breaker::UDP_FALLBACK,
            "this PR ships dark: the constant stays true until D"
        );
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
            body.starts_with("reason:&str){ifsuper::breaker::udp_fallback(){return;}"),
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
