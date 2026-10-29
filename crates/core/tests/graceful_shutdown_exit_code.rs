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
        Self::spawn_with(dir, dir, &[])
    }

    /// As [`Self::spawn`], logging to `log_dir` and passing `extra_args`.
    fn spawn_with(dir: &Path, log_dir: &Path, extra_args: &[&str]) -> anyhow::Result<Self> {
        Self::spawn_full(dir, log_dir, extra_args, None)
    }

    /// As [`Self::spawn_with`]. With `home`, the node gets no stop-note
    /// variable and `HOME` set to `home`, so it uses the note at its default
    /// place under `home`.
    fn spawn_full(
        dir: &Path,
        log_dir: &Path,
        extra_args: &[&str],
        home: Option<&Path>,
    ) -> anyhow::Result<Self> {
        let ws_port = reserve_port()?;
        let network_port = reserve_port()?;
        let mut command = Command::new(freenet_bin());
        command
            .arg("network")
            // Same #4366 telemetry-contamination guard as persistence_roundtrip.rs:
            // this runs the real binary from target/<profile>/, so neither of the
            // in-process "under cargo test" signals fires and telemetry would
            // otherwise default ON and POST to the production OTLP collector.
            .env("FREENET_TELEMETRY_ENABLED", "false")
            // The QUIC crash-loop breaker's stop note, in this test's own
            // directory: without it the node would use the default log
            // directory's note, which belongs to the machine's real node.
            .env("FREENET_QUIC_BREAKER_STOP_NOTE", stop_note(dir))
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
            .args(["--log-dir", &log_dir.to_string_lossy()])
            .args(extra_args)
            .stdout(Stdio::null())
            .stderr(Stdio::null());
        if let Some(home) = home {
            command
                .env_remove("FREENET_QUIC_BREAKER_STOP_NOTE")
                .env("HOME", home);
        }
        let child = command.spawn()?;
        Ok(Self {
            child,
            log_dir: log_dir.to_path_buf(),
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
        self.wait_for_logs(&[READY_MARKER], timeout)
    }

    /// Block until every one of `markers` is in the node's logs.
    fn wait_for_logs(&mut self, markers: &[&str], timeout: Duration) -> anyhow::Result<()> {
        let deadline = Instant::now() + timeout;
        loop {
            if let Some(status) = self.child.try_wait()? {
                anyhow::bail!(
                    "freenet exited ({status:?}) before logging {markers:?}. Logs:\n{}",
                    self.logs()
                );
            }
            let logs = self.logs();
            if markers.iter().all(|m| logs.contains(m)) {
                return Ok(());
            }
            if Instant::now() >= deadline {
                anyhow::bail!("freenet did not log {markers:?} within {timeout:?}. Logs:\n{logs}");
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

    /// Kill the node with SIGKILL, an abnormal exit, and reap it.
    fn kill_abnormally(&mut self) -> anyhow::Result<std::process::ExitStatus> {
        self.child.kill()?;
        Ok(self.child.wait()?)
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

// ---------------------------------------------------------------------------
// QUIC crash-loop breaker (#5940), driven through the real binary.
//
// The breaker's decisions are unit-tested in `transport::quic::breaker`; these
// tests cover the wiring no in-process test can see: that the node consults the
// breaker where it decides on QUIC (`p2p_protoc.rs`), and that `main` clears the
// marker on a clean exit.
// ---------------------------------------------------------------------------

/// The marker file the breaker keeps in the data directory.
const QUIC_MARKER_FILE: &str = "quic-crash-breaker.json";

/// Logged by `bind_udp_listener` right after it decides whether to build the
/// QUIC endpoint, so once it is present the QUIC decision has been acted on.
const TRANSPORT_BOUND_MARKER: &str = "UDP socket bound successfully";

/// Logged when the QUIC endpoint is built: QUIC is on.
const QUIC_BUILT_MARKER: &str = "QUIC endpoint built";

/// The breaker's WARN when it trips at startup.
const QUIC_TRIP_MARKER: &str = "The previous run with QUIC enabled ended abnormally";

/// The breaker's WARN on a later start inside the cooldown.
const QUIC_COOLING_MARKER: &str = "QUIC stays off: a run with QUIC enabled ended abnormally";

/// The stop note every test node is given (see `NodeProcess::spawn_with`).
fn stop_note(dir: &Path) -> PathBuf {
    dir.join("quic-breaker-stop-note")
}

/// Spawn a node with `transport-quic = prefer`, logging to `log_dir`, and wait
/// until it is ready for SIGTERM and has bound its transport.
fn spawn_quic_node(dir: &Path, log_dir: &Path) -> anyhow::Result<NodeProcess> {
    spawn_quic_node_with(dir, log_dir, &[])
}

/// As [`spawn_quic_node`], with `extra_args`.
fn spawn_quic_node_with(
    dir: &Path,
    log_dir: &Path,
    extra_args: &[&str],
) -> anyhow::Result<NodeProcess> {
    std::fs::create_dir_all(log_dir)?;
    spawn_quic_node_full(dir, log_dir, extra_args, None)
}

/// As [`spawn_quic_node_with`], with `home` as in `NodeProcess::spawn_full`.
fn spawn_quic_node_full(
    dir: &Path,
    log_dir: &Path,
    extra_args: &[&str],
    home: Option<&Path>,
) -> anyhow::Result<NodeProcess> {
    std::fs::create_dir_all(log_dir)?;
    let mut args = vec!["--transport-quic", "prefer"];
    args.extend_from_slice(extra_args);
    let mut node = NodeProcess::spawn_full(dir, log_dir, &args, home)?;
    node.wait_for_logs(
        &[READY_MARKER, TRANSPORT_BOUND_MARKER],
        Duration::from_secs(120),
    )?;
    Ok(node)
}

/// Wait until the breaker marker is Armed by `node`. The node arms it just
/// after binding its socket, so it can lag the bind log line slightly.
fn wait_for_armed_marker(dir: &Path, node: &NodeProcess) -> anyhow::Result<serde_json::Value> {
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        let marker = read_quic_marker(dir)?;
        let armed_by = marker.as_ref().and_then(|m| {
            (m.get("state")?.as_str()? == "armed").then_some(m.get("pid")?.as_i64()?)
        });
        if armed_by == Some(i64::from(node.pid())) {
            return Ok(marker.expect("checked above"));
        }
        if Instant::now() >= deadline {
            anyhow::bail!(
                "the marker was not armed by pid {} within 30 s; it is {marker:?}. Logs:\n{}",
                node.pid(),
                node.logs()
            );
        }
        std::thread::sleep(Duration::from_millis(50));
    }
}

/// The breaker marker's contents, or `None` when there is no marker.
fn read_quic_marker(dir: &Path) -> anyhow::Result<Option<serde_json::Value>> {
    match std::fs::read(dir.join(QUIC_MARKER_FILE)) {
        Ok(bytes) => Ok(Some(serde_json::from_slice(&bytes)?)),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(e) => Err(e.into()),
    }
}

/// (a) A node with QUIC enabled arms the marker with its own pid and runs
/// QUIC; (b) a SIGTERM stop exits 0 and clears it.
///
/// FALSIFY: delete the `quic_breaker_clean_exit()` call in `main`'s `Ok` arm;
/// the marker survives the SIGTERM and (b) fails.
#[test]
fn quic_breaker_marker_is_armed_and_cleared_by_a_clean_stop() -> anyhow::Result<()> {
    let dir = tempfile::tempdir()?;
    let mut node = spawn_quic_node(dir.path(), &dir.path().join("logs-1"))?;

    // (a)
    let marker = wait_for_armed_marker(dir.path(), &node)?;
    assert!(
        marker.get("boot_id").and_then(|b| b.as_str()).is_some(),
        "the marker records the boot identity: {marker}"
    );
    assert!(
        node.logs().contains(QUIC_BUILT_MARKER),
        "QUIC must be on in this run. Logs:\n{}",
        node.logs()
    );

    // (b)
    node.send_sigterm()?;
    let status = node.wait_for_exit(Duration::from_secs(45))?;
    assert_eq!(status.code(), Some(0), "Logs:\n{}", node.logs());
    assert_eq!(
        read_quic_marker(dir.path())?,
        None,
        "a clean stop must clear the marker, or the next start reads it as a \
         crash and runs without QUIC for the cooldown"
    );
    Ok(())
}

/// (c) A node with QUIC enabled that is SIGKILLed trips the breaker: the next
/// start records the trip, warns, and runs with QUIC off. A clean stop of that
/// run leaves the trip in place, the start after it is still inside the
/// cooldown and stays off without rewriting the marker, and a start with
/// `--quic-ignore-crash-breaker` runs QUIC, arms the marker, and clears it on
/// a clean stop.
///
/// FALSIFY: replace the `breaker::gate(..).await` call in
/// `P2pConnManager::build` with `breaker::Gated { settings:
/// config.quic_settings(), breaker: breaker::BreakerOutcome::NotConsulted }`;
/// no marker is written, the third run builds the QUIC endpoint, and this test
/// fails.
#[test]
fn quic_breaker_trips_after_an_abnormal_exit() -> anyhow::Result<()> {
    let dir = tempfile::tempdir()?;

    let mut first = spawn_quic_node(dir.path(), &dir.path().join("logs-1"))?;
    assert!(
        first.logs().contains(QUIC_BUILT_MARKER),
        "QUIC must be on before the crash. Logs:\n{}",
        first.logs()
    );
    // Killed only once it has armed, or there is nothing to trip on.
    wait_for_armed_marker(dir.path(), &first)?;
    first.kill_abnormally()?;
    drop(first);

    let mut second = spawn_quic_node(dir.path(), &dir.path().join("logs-2"))?;
    let marker = read_quic_marker(dir.path())?;
    assert_eq!(
        marker.as_ref().and_then(|m| m.get("state")?.as_str()),
        Some("tripped"),
        "after an abnormal exit the marker must record a trip; got {marker:?}. Logs:\n{}",
        second.logs()
    );
    let logs = second.logs();
    assert!(
        logs.contains(QUIC_TRIP_MARKER),
        "the trip must be logged. Logs:\n{logs}"
    );
    assert!(
        !logs.contains(QUIC_BUILT_MARKER),
        "QUIC must stay off after the trip, but the endpoint was built. Logs:\n{logs}"
    );

    second.send_sigterm()?;
    let status = second.wait_for_exit(Duration::from_secs(45))?;
    assert_eq!(status.code(), Some(0), "Logs:\n{}", second.logs());
    let tripped = read_quic_marker(dir.path())?;
    assert_eq!(
        tripped
            .as_ref()
            .and_then(|m| Some(m.get("state")?.as_str()?.to_owned())),
        Some("tripped".to_owned()),
        "a clean stop of a run the breaker kept off must not clear the trip"
    );

    // Cooling: still off, and the trip time is not moved.
    let mut third = spawn_quic_node(dir.path(), &dir.path().join("logs-3"))?;
    let logs = third.logs();
    assert!(
        logs.contains(QUIC_COOLING_MARKER) && !logs.contains(QUIC_BUILT_MARKER),
        "inside the cooldown QUIC must stay off, with the cooling WARN. Logs:\n{logs}"
    );
    assert_eq!(
        read_quic_marker(dir.path())?,
        tripped,
        "a start inside the cooldown must not rewrite the marker"
    );
    third.send_sigterm()?;
    assert_eq!(
        third.wait_for_exit(Duration::from_secs(45))?.code(),
        Some(0),
        "Logs:\n{}",
        third.logs()
    );

    // Override: QUIC runs, the marker is armed by this run and cleared by its
    // clean stop.
    let mut fourth = spawn_quic_node_with(
        dir.path(),
        &dir.path().join("logs-4"),
        &["--quic-ignore-crash-breaker"],
    )?;
    let logs = fourth.logs();
    assert!(
        logs.contains(QUIC_BUILT_MARKER),
        "the override must run QUIC inside the cooldown. Logs:\n{logs}"
    );
    // The override arms the marker like any QUIC start.
    wait_for_armed_marker(dir.path(), &fourth)?;
    fourth.send_sigterm()?;
    assert_eq!(
        fourth.wait_for_exit(Duration::from_secs(45))?.code(),
        Some(0),
        "Logs:\n{}",
        fourth.logs()
    );
    assert_eq!(read_quic_marker(dir.path())?, None);
    Ok(())
}

/// A SIGKILL whose pid is in the stop note (a deliberate hard kill, as the
/// macOS tray does) is not a trip, and the start consumes the note; a note
/// naming another pid does not excuse the kill. Drives `gate`'s reading of
/// the note from `FREENET_QUIC_BREAKER_STOP_NOTE`.
///
/// FALSIFY: make `gate` ignore `FREENET_QUIC_BREAKER_STOP_NOTE` (so it falls
/// back to the default path); the noted kill trips and the test's note is
/// left behind.
#[test]
fn quic_breaker_stop_note_excuses_only_the_noted_pid() -> anyhow::Result<()> {
    for noted_matches in [true, false] {
        let dir = tempfile::tempdir()?;
        let mut first = spawn_quic_node(dir.path(), &dir.path().join("logs-1"))?;
        wait_for_armed_marker(dir.path(), &first)?;
        let pid = u32::try_from(first.pid())?;
        first.kill_abnormally()?;
        drop(first);
        let noted = if noted_matches { pid } else { pid + 1 };
        std::fs::write(stop_note(dir.path()), format!("{noted}\n"))?;

        let mut second = spawn_quic_node(dir.path(), &dir.path().join("logs-2"))?;
        assert!(
            !stop_note(dir.path()).exists(),
            "the start must consume the note (noted pid matches: {noted_matches})"
        );
        let logs = second.logs();
        if noted_matches {
            assert!(
                !logs.contains(QUIC_TRIP_MARKER) && logs.contains(QUIC_BUILT_MARKER),
                "a noted kill is not a trip. Logs:\n{logs}"
            );
            wait_for_armed_marker(dir.path(), &second)?;
        } else {
            assert!(
                logs.contains(QUIC_TRIP_MARKER) && !logs.contains(QUIC_BUILT_MARKER),
                "a note for another pid does not excuse the kill. Logs:\n{logs}"
            );
            assert_eq!(
                read_quic_marker(dir.path())?
                    .and_then(|m| Some(m.get("state")?.as_str()?.to_owned())),
                Some("tripped".to_owned())
            );
        }
        second.send_sigterm()?;
        assert_eq!(
            second.wait_for_exit(Duration::from_secs(45))?.code(),
            Some(0),
            "Logs:\n{}",
            second.logs()
        );
    }
    Ok(())
}

/// A node started without `FREENET_QUIC_BREAKER_STOP_NOTE` (not by the tray
/// wrapper) reads the note at its default place, `$HOME/.local/state/freenet`
/// on Linux. That file may be meant for another node, so the start takes out
/// only its own marker's pid and leaves every other entry.
///
/// FALSIFY: treat the default-path note like the wrapper's (delete it whole);
/// the other node's entry is lost.
#[cfg(target_os = "linux")]
#[test]
fn quic_breaker_default_path_note_keeps_other_entries() -> anyhow::Result<()> {
    const OTHER: &str = "999999999";
    let dir = tempfile::tempdir()?;
    let home = dir.path().join("home");
    let note = home.join(".local/state/freenet/quic-breaker-stop-note");
    std::fs::create_dir_all(note.parent().expect("parent"))?;
    let entries = |note: &Path| -> anyhow::Result<Vec<String>> {
        Ok(std::fs::read_to_string(note)?
            .split_whitespace()
            .map(str::to_owned)
            .collect())
    };

    // A note for another node only: this start leaves it as it is.
    std::fs::write(&note, format!("{OTHER}\n"))?;
    let mut first = spawn_quic_node_full(dir.path(), &dir.path().join("logs-1"), &[], Some(&home))?;
    wait_for_armed_marker(dir.path(), &first)?;
    assert_eq!(entries(&note)?, vec![OTHER], "another node's entry is left");

    // Our pid beside it: no trip, and only our pid is taken out.
    let pid = first.pid();
    first.kill_abnormally()?;
    drop(first);
    std::fs::write(&note, format!("{OTHER}\n{pid}\n"))?;
    let mut second =
        spawn_quic_node_full(dir.path(), &dir.path().join("logs-2"), &[], Some(&home))?;
    let logs = second.logs();
    assert!(
        !logs.contains(QUIC_TRIP_MARKER) && logs.contains(QUIC_BUILT_MARKER),
        "a kill noted at the default path is not a trip. Logs:\n{logs}"
    );
    assert_eq!(entries(&note)?, vec![OTHER], "only our pid is taken out");
    wait_for_armed_marker(dir.path(), &second)?;
    second.send_sigterm()?;
    assert_eq!(
        second.wait_for_exit(Duration::from_secs(45))?.code(),
        Some(0),
        "Logs:\n{}",
        second.logs()
    );
    Ok(())
}
