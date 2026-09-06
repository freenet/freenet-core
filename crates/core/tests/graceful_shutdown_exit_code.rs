//! A graceful shutdown must make the `freenet` PROCESS exit 0 (#5227).
//!
//! Regression test for a production incident on a node running 0.2.121. A
//! deliberate `systemctl --user stop freenet` produced:
//!
//! ```text
//! CRITICAL: Network event listener exited: Graceful shutdown
//! Error: Graceful shutdown
//! systemd: freenet.service: Main process exited, code=exited, status=1/FAILURE
//! Freenet 0.2.121: crash 1/3 during post-update probation (node stop status 1);
//!   not updating, will auto-roll-back if it keeps crashing.
//! ```
//!
//! Nothing had crashed. `run_network_node` reports a clean SIGTERM stop as
//! `Err(EventLoopExitReason::GracefulShutdown)` — a typed sentinel, not a real
//! error — and `main` handed it to the `eprintln!("Error: …")` +
//! `std::process::exit(1)` fallback. `rollback::classify_stop` treats any status
//! outside {0, 2, 42, 43, 44} as a crash, so three clean stops of a freshly
//! updated node inside its probation window were enough to auto-roll-back a
//! perfectly healthy release (#4073).
//!
//! ## Why this test spawns a real process
//!
//! The classification predicate was ALREADY correct: `p2p_impl::
//! listener_exit_is_graceful` returned `true` for this exit, and
//! `listener_exit_graceful_classification` pinned that. The bug lived entirely
//! in the gap between that predicate and the process exit code, which no
//! in-process test can observe — `std::process::exit` only runs in the real
//! binary's `main`. So the assertion here is deliberately on
//! `ExitStatus::code()` of a spawned `freenet network`, not on any predicate.
//!
//! The decision itself — including the case a live node cannot easily be driven
//! into, an UNREQUESTED `GracefulShutdown` from a critical channel death, which
//! must still exit non-zero — is pinned by
//! `finish_run_maps_only_a_requested_graceful_stop_to_success` in
//! `src/bin/freenet.rs`.
//!
//! Unix only: the failure is about SIGTERM handling, which has no Windows
//! analogue (the Windows service stop path goes through `commands::service`).

#![cfg(unix)]

use std::{
    path::{Path, PathBuf},
    process::{Child, Command, Stdio},
    time::{Duration, Instant},
};

/// Log line emitted by `node::run_network_node`, which the binary only reaches
/// from INSIDE the `tokio::select!` of `run_network_node_with_signals` — i.e.
/// strictly after `signal::unix::signal(SignalKind::terminate())` has installed
/// the SIGTERM handler. Waiting for it is what makes the test deterministic: a
/// SIGTERM delivered before that point would kill the process with the signal's
/// default disposition (no exit code at all) rather than exercising the
/// graceful-shutdown path. The node's WS API is NOT a usable readiness signal —
/// `run_network` binds it several steps earlier, before the node is built.
const READY_MARKER: &str = "Starting node";

/// Emitted by the signal task in `run_network_node_with_signals` on SIGTERM /
/// SIGINT. Asserted after exit so a status of 0 cannot be credited to some other
/// clean-exit path (`run_disabled_idle`, an early return) — the test must prove
/// the node exited 0 *because it handled the signal*.
const SHUTDOWN_MARKER: &str = "Initiating graceful shutdown";

/// Path to the `freenet` binary built for THIS test run. Cargo sets
/// `CARGO_BIN_EXE_<name>` for integration tests and builds the binary first, so
/// unlike a hand-rolled `target/debug/…` lookup this can never assert against a
/// stale binary that predates the change under test.
fn freenet_bin() -> PathBuf {
    PathBuf::from(env!("CARGO_BIN_EXE_freenet"))
}

/// Reserve an ephemeral port by binding then immediately freeing it. The kernel
/// won't hand the same port straight back on the next `:0` request, so this
/// races far less than a fixed port.
fn reserve_port() -> std::io::Result<u16> {
    let listener = std::net::TcpListener::bind("127.0.0.1:0")?;
    Ok(listener.local_addr()?.port())
}

/// A spawned `freenet network` subprocess, SIGKILLed and reaped on drop so a
/// panicking assertion never leaks a node process.
struct NodeProcess {
    child: Child,
    log_dir: PathBuf,
}

impl Drop for NodeProcess {
    fn drop(&mut self) {
        if self.child.kill().is_ok() {
            let _reaped = self.child.wait().is_ok();
        }
    }
}

impl NodeProcess {
    /// Spawn a self-contained, fully offline gateway node rooted at `dir`.
    fn spawn(dir: &Path) -> anyhow::Result<Self> {
        let ws_port = reserve_port()?;
        let network_port = reserve_port()?;
        let child = Command::new(freenet_bin())
            .arg("network")
            // Same #4366 telemetry-contamination guard as persistence_roundtrip.rs:
            // this runs the real binary from target/<profile>/, so neither of the
            // in-process "under cargo test" signals fires and telemetry would
            // otherwise default ON and POST to the production OTLP collector.
            .env("FREENET_TELEMETRY_ENABLED", "false")
            // Pin the child's logging rather than inheriting it. CI's test_unit
            // job exports `RUST_LOG=error` (ci.yml), and the tracing layer builds
            // its file filter with `from_env_lossy()`, which would drop the INFO
            // readiness marker entirely and strand this test on its timeout.
            // Clear the log-destination overrides for the same reason: both
            // markers must land in a *file* in the log dir, because stdout/stderr
            // are /dev/null here and the console layer is off under `cargo test`
            // (it requires a terminal).
            .env("RUST_LOG", "info")
            .env_remove("FREENET_LOG_TO_STDERR")
            .env_remove("FREENET_DISABLE_LOGS")
            .env_remove("FREENET_LOG_FORMAT")
            .args(["--ws-api-address", "127.0.0.1"])
            .args(["--ws-api-port", &ws_port.to_string()])
            .args(["--network-address", "127.0.0.1"])
            .args(["--network-port", &network_port.to_string()])
            .args(["--public-network-address", "127.0.0.1"])
            .args(["--public-network-port", &network_port.to_string()])
            .arg("--is-gateway")
            // No gateway-index fetch, so the node boots with no internet.
            .arg("--skip-load-from-network")
            .arg("--ignore-protocol-checking")
            // Without this the node asks GitHub for a newer release at startup
            // and, on a clean (non-dirty) CI build, could exit 42 instead of 0 —
            // a real update path, but not the one under test. The disabled
            // branch is taken before the 0-60s startup jitter sleep, so this
            // also keeps the test fast.
            .arg("--disable-auto-update")
            .args(["--location", "0.5"])
            // Nothing is in flight, so there is nothing to drain; keep the stop
            // prompt rather than waiting out the 30s default.
            .args(["--shutdown-drain-secs", "1"])
            .args(["--config-dir", &dir.to_string_lossy()])
            .args(["--data-dir", &dir.to_string_lossy()])
            .args(["--log-dir", &dir.to_string_lossy()])
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()?;
        Ok(Self {
            child,
            log_dir: dir.to_path_buf(),
        })
    }

    fn pid(&self) -> i32 {
        self.child.id() as i32
    }

    /// Concatenated contents of the node's rotated log files. The tracing layer
    /// only mirrors to the console when stdout is a terminal (it is not under
    /// `cargo test`), so the log dir is the sole place the marker appears.
    fn logs(&self) -> String {
        let mut out = String::new();
        if let Ok(entries) = std::fs::read_dir(&self.log_dir) {
            for entry in entries.flatten() {
                let path = entry.path();
                if path.extension().is_some_and(|e| e == "log") {
                    out.push_str(&std::fs::read_to_string(&path).unwrap_or_default());
                }
            }
        }
        out
    }

    /// Block until the node has installed its SIGTERM handler and entered the
    /// event loop, or fail with the node's own logs attached.
    fn wait_until_ready(&mut self, timeout: Duration) -> anyhow::Result<()> {
        let deadline = Instant::now() + timeout;
        loop {
            if let Some(status) = self.child.try_wait()? {
                anyhow::bail!(
                    "freenet exited ({status:?}) before reaching {READY_MARKER:?}. Logs:\n{}",
                    self.logs()
                );
            }
            if self.logs().contains(READY_MARKER) {
                return Ok(());
            }
            if Instant::now() >= deadline {
                anyhow::bail!(
                    "freenet did not log {READY_MARKER:?} within {timeout:?}. Logs:\n{}",
                    self.logs()
                );
            }
            std::thread::sleep(Duration::from_millis(100));
        }
    }

    fn send_sigterm(&self) -> anyhow::Result<()> {
        // SAFETY: `kill(2)` with a pid we own and a valid signal number. The
        // child is still un-reaped (we have not called `wait`), so the pid
        // cannot have been recycled.
        if unsafe { libc::kill(self.pid(), libc::SIGTERM) } != 0 {
            anyhow::bail!("kill(SIGTERM) failed: {}", std::io::Error::last_os_error());
        }
        Ok(())
    }

    /// Wait for the process to exit, returning its raw `ExitStatus`.
    fn wait_for_exit(&mut self, timeout: Duration) -> anyhow::Result<std::process::ExitStatus> {
        let deadline = Instant::now() + timeout;
        loop {
            if let Some(status) = self.child.try_wait()? {
                return Ok(status);
            }
            if Instant::now() >= deadline {
                anyhow::bail!(
                    "freenet did not exit within {timeout:?} of SIGTERM. Logs:\n{}",
                    self.logs()
                );
            }
            std::thread::sleep(Duration::from_millis(100));
        }
    }
}

/// SIGTERM a running node and assert the PROCESS exits 0.
///
/// Before the fix this failed with `Some(1)`: `main` returned
/// `Err(EventLoopExitReason::GracefulShutdown)` and `Termination` turned the
/// sentinel into a failure status.
#[test]
fn graceful_shutdown_on_sigterm_exits_zero() -> anyhow::Result<()> {
    use std::os::unix::process::ExitStatusExt;

    let dir = tempfile::tempdir()?;
    let mut node = NodeProcess::spawn(dir.path())?;

    // A debug-profile node on a loaded self-hosted runner needs a moment to open
    // redb and build; it is seconds, not minutes. Kept well inside nextest's
    // per-test cap so a slow run still produces this test's own log-attached
    // diagnostic instead of being SIGKILLed by the harness.
    node.wait_until_ready(Duration::from_secs(120))?;

    node.send_sigterm()?;

    // The binary sleeps 2s after aborting its background tasks, on top of the
    // 1s drain window; systemd allows 45s (TimeoutStopSec) for the whole
    // teardown, so match that bound.
    let status = node.wait_for_exit(Duration::from_secs(45))?;

    // Distinguish the two failure shapes explicitly. `signal()` is set (and
    // `code()` is None) only if SIGTERM arrived before the handler was
    // installed, which would mean the readiness gate above is not doing its job
    // — a test bug, not the regression.
    assert!(
        status.signal().is_none(),
        "node was killed by signal {:?} instead of handling SIGTERM — the readiness \
         gate raced. Logs:\n{}",
        status.signal(),
        node.logs()
    );
    assert_eq!(
        status.code(),
        Some(0),
        "a graceful shutdown must exit 0 (#5227). Any other code is classified as a \
         CRASH by rollback::classify_stop, so systemd logs status=N/FAILURE for a clean \
         `systemctl stop` and the post-update probation counts it toward an \
         auto-rollback. Logs:\n{}",
        node.logs()
    );
    // Guards against crediting the 0 to some other clean-exit path: the node must
    // have exited 0 BECAUSE it handled the signal.
    let logs = node.logs();
    assert!(
        logs.contains(SHUTDOWN_MARKER),
        "exit 0 must come from the signal-handling path — {SHUTDOWN_MARKER:?} is absent, \
         so the node exited cleanly for some other reason and this test proves nothing. \
         Logs:\n{logs}"
    );

    Ok(())
}

/// The exposed predicate must key on the error TYPE, not its text.
///
/// `finish_run` only reaches a success exit for errors this returns `true` for,
/// so a string-shaped match here would let any error whose `Display` happens to
/// read "Graceful shutdown" (a peer-supplied or formatted message) be laundered
/// into exit 0.
#[test]
fn graceful_classification_is_by_type_not_by_message() {
    #[derive(Debug, thiserror::Error)]
    #[error("Graceful shutdown")]
    struct GracefulLookalike;

    assert!(freenet::listener_exit_is_graceful(
        &freenet::EventLoopExitReason::GracefulShutdown.into()
    ));
    assert!(!freenet::listener_exit_is_graceful(
        &freenet::EventLoopExitReason::UnexpectedStreamEnd.into()
    ));
    assert!(!freenet::listener_exit_is_graceful(&anyhow::Error::new(
        GracefulLookalike
    )));
    assert!(!freenet::listener_exit_is_graceful(&anyhow::anyhow!(
        "Graceful shutdown"
    )));
}
