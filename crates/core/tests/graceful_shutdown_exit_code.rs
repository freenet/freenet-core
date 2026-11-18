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

/// An `extra_args` entry that starts the node as a peer: it is not passed
/// on, and `--is-gateway` is left out.
const AS_PEER: &str = "<as-peer>";

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

/// A port free in both TCP and UDP right now, for the node to bind.
fn reserve_port() -> std::io::Result<u16> {
    use std::sync::atomic::{AtomicU32, Ordering};
    /// Below Linux's default ephemeral range (32768-60999), so a socket the OS
    /// assigns to another test cannot take the port before the node binds it.
    /// An OS-assigned port here lost that race: the node then exits 42.
    const FIRST: u32 = 20_000;
    const COUNT: u32 = 10_000;
    static NEXT: AtomicU32 = AtomicU32::new(0);
    // Each test under nextest is its own process, so the pid spreads them apart.
    let start = std::process::id().wrapping_mul(2_654_435_761);
    for _ in 0..COUNT {
        let offset = start.wrapping_add(NEXT.fetch_add(1, Ordering::Relaxed)) % COUNT;
        let port = (FIRST + offset) as u16;
        // The two ports serve TCP (WebSocket) and UDP (network), so a port must
        // be free in both.
        if std::net::TcpListener::bind(("127.0.0.1", port)).is_ok()
            && std::net::UdpSocket::bind(("127.0.0.1", port)).is_ok()
        {
            return Ok(port);
        }
    }
    Err(std::io::Error::new(
        std::io::ErrorKind::AddrInUse,
        "no free port below the ephemeral range",
    ))
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
        Self::spawn_full(dir, log_dir, extra_args, None, &[])
    }

    /// As [`Self::spawn_with`]. With `home`, the node gets no stop-note
    /// variable and `HOME` set to `home`, so it uses the note at its default
    /// place under `home`. `env` is set last; the node otherwise never looks
    /// supervised (the supervisor variables are removed).
    fn spawn_full(
        dir: &Path,
        log_dir: &Path,
        extra_args: &[&str],
        home: Option<&Path>,
        env: &[(&str, &str)],
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
            // The QUIC mode comes from the arguments alone: an inherited
            // FREENET_TRANSPORT_QUIC would decide it for the test that runs
            // with none.
            .env_remove("FREENET_TRANSPORT_QUIC")
            .env_remove("FREENET_LOG_FORMAT")
            .args(["--ws-api-address", "127.0.0.1"])
            .args(["--ws-api-port", &ws_port.to_string()])
            .args(["--network-address", "127.0.0.1"])
            .args(["--network-port", &network_port.to_string()])
            .args(["--public-network-address", "127.0.0.1"])
            .args(["--public-network-port", &network_port.to_string()])
            .args((!extra_args.contains(&AS_PEER)).then_some("--is-gateway"))
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
            .args(extra_args.iter().filter(|arg| **arg != AS_PEER))
            .stdout(Stdio::null())
            .stderr(Stdio::null());
        if let Some(home) = home {
            command
                .env_remove("FREENET_QUIC_BREAKER_STOP_NOTE")
                .env("HOME", home);
        }
        // A test runner under systemd would otherwise make every node look
        // supervised (`supervisor_status`).
        command
            .env_remove("FREENET_SUPERVISED")
            .env_remove("INVOCATION_ID")
            .env_remove("FREENET_TEST_QUIC_PANIC")
            .env_remove("FREENET_TEST_QUIC_ENDPOINT_DOWN_AFTER_SECS");
        command.envs(env.iter().copied());
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

/// The same WARN from the short-run rule, which every peer decides by
/// (#6151).
const QUIC_SHORT_RUNS_COOLING_MARKER: &str =
    "QUIC stays off: several runs in a row with QUIC enabled ended early";

/// The legacy exit-status rule's shadow marker on a unix peer (#6151):
/// `{"version", "boot_id", "marker"}`, the marker in the breaker's format.
const LEGACY_SHADOW_FILE: &str = "quic-crash-breaker-legacy.json";

/// The arguments that start a peer: it needs a gateway to start, and this
/// one is never reached.
fn peer_args(gateway: &str) -> [&str; 3] {
    [AS_PEER, "--gateway", gateway]
}

/// An unreachable gateway for [`peer_args`].
fn unreachable_gateway() -> String {
    format!("127.0.0.1:9,{}", "11".repeat(32))
}

/// The JSON file `name` in `dir`, or `None` when there is none.
fn read_json(dir: &Path, name: &str) -> anyhow::Result<Option<serde_json::Value>> {
    match std::fs::read(dir.join(name)) {
        Ok(bytes) => Ok(Some(serde_json::from_slice(&bytes)?)),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(e) => Err(e.into()),
    }
}

/// Wait until `node` has written both a peer's markers: the short-run marker
/// and the legacy shadow armed with its pid.
fn wait_for_peer_markers(dir: &Path, node: &NodeProcess) -> anyhow::Result<()> {
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        let marker = read_quic_marker(dir)?;
        let shadow = read_json(dir, LEGACY_SHADOW_FILE)?;
        let short_runs = marker
            .as_ref()
            .is_some_and(|m| m["state"] == "short_runs" && m["quic_panicked"] == false);
        let shadow_armed = shadow
            .as_ref()
            .is_some_and(|m| m["marker"]["state"] == "armed" && m["marker"]["pid"] == node.pid());
        if short_runs && shadow_armed {
            return Ok(());
        }
        if Instant::now() >= deadline {
            anyhow::bail!(
                "the peer's markers were not written within 30 s: {marker:?}, shadow \
                 {shadow:?}. Logs:\n{}",
                node.logs()
            );
        }
        std::thread::sleep(Duration::from_millis(50));
    }
}

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
    spawn_quic_node_full(dir, log_dir, extra_args, None, &[])
}

/// As [`spawn_quic_node_with`], with `home` as in `NodeProcess::spawn_full`.
fn spawn_quic_node_full(
    dir: &Path,
    log_dir: &Path,
    extra_args: &[&str],
    home: Option<&Path>,
    env: &[(&str, &str)],
) -> anyhow::Result<NodeProcess> {
    std::fs::create_dir_all(log_dir)?;
    let mut args = vec!["--transport-quic", "prefer"];
    args.extend_from_slice(extra_args);
    let mut node = NodeProcess::spawn_full(dir, log_dir, &args, home, env)?;
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

/// With no `transport-quic` flag, key or environment variable, the binary
/// builds a QUIC endpoint and arms the breaker: prefer is the default on Linux
/// and macOS. Every other test here sets the mode, so this is the one that sees
/// what a node that never set it runs.
///
/// FALSIFY: make `QuicMode::default_mode` return `Off`; no endpoint is built
/// and this test fails.
#[cfg(any(target_os = "linux", target_os = "macos"))]
#[test]
fn a_node_with_no_quic_setting_runs_prefer_and_arms_the_breaker() -> anyhow::Result<()> {
    let dir = tempfile::tempdir()?;
    let log_dir = dir.path().join("logs");
    std::fs::create_dir_all(&log_dir)?;
    let mut node = NodeProcess::spawn_full(dir.path(), &log_dir, &[], None, &[])?;
    node.wait_for_logs(
        &[READY_MARKER, TRANSPORT_BOUND_MARKER],
        Duration::from_secs(120),
    )?;
    assert!(
        node.logs().contains(QUIC_BUILT_MARKER),
        "an unset transport-quic must build the endpoint. Logs:\n{}",
        node.logs()
    );
    let logs = node.logs();
    let mode_line = logs
        .lines()
        .find(|l| l.contains("QUIC transport mode configured"))
        .unwrap_or_else(|| panic!("no QUIC mode line. Logs:\n{logs}"));
    assert!(
        mode_line.contains("Prefer") && mode_line.contains("default"),
        "the mode line must say prefer, from the default: {mode_line}"
    );
    wait_for_armed_marker(dir.path(), &node)?;
    node.send_sigterm()?;
    let status = node.wait_for_exit(Duration::from_secs(45))?;
    assert_eq!(status.code(), Some(0), "Logs:\n{}", node.logs());
    assert_eq!(
        read_quic_marker(dir.path())?,
        None,
        "a clean stop clears the marker"
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

/// A gateway takes the short gateway cooldown, never the peer's 24 h: a trip
/// 5 minutes old keeps QUIC off, one 20 minutes old does not. Every node these
/// tests spawn is a gateway (`--is-gateway`).
///
/// FALSIFY: pass `BreakerRole::Peer` to `breaker::gate` in
/// `P2pConnManager::build`; the 20-minute-old trip still holds and the second
/// start does not build the endpoint.
#[test]
fn quic_breaker_on_a_gateway_holds_only_for_the_gateway_cooldown() -> anyhow::Result<()> {
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)?
        .as_secs();
    for (age_min, quic_on) in [(5u64, false), (20, true)] {
        let dir = tempfile::tempdir()?;
        let marker = serde_json::json!({
            "state": "tripped",
            "tripped_at_secs": now - age_min * 60,
        });
        std::fs::write(
            dir.path().join(QUIC_MARKER_FILE),
            serde_json::to_vec(&marker)?,
        )?;
        let mut node = spawn_quic_node(dir.path(), &dir.path().join("logs"))?;
        let logs = node.logs();
        assert_eq!(
            logs.contains(QUIC_BUILT_MARKER),
            quic_on,
            "a gateway trip {age_min} min old: QUIC on must be {quic_on}. Logs:\n{logs}"
        );
        assert_eq!(
            logs.contains(QUIC_COOLING_MARKER),
            !quic_on,
            "the cooling WARN must match. Logs:\n{logs}"
        );
        node.send_sigterm()?;
        assert_eq!(
            node.wait_for_exit(Duration::from_secs(45))?.code(),
            Some(0),
            "Logs:\n{}",
            node.logs()
        );
    }
    Ok(())
}

/// Logged by a gateway that exits because its QUIC endpoint died.
#[cfg(feature = "testing")]
const ENDPOINT_DOWN_EXIT_MARKER: &str =
    "Gateway exiting for its service supervisor to restart it with QUIC on: its QUIC endpoint died";

/// Logged when the endpoint goes down after a caught adapter panic.
#[cfg(feature = "testing")]
const ENDPOINT_DOWN_MARKER: &str = "The QUIC endpoint is down";

/// Test builds (`testing`): fail the QUIC endpoint this many seconds after
/// the bind (`connection_handler::fail_endpoint_from_env`).
#[cfg(feature = "testing")]
const ENDPOINT_DOWN_ENV: &str = "FREENET_TEST_QUIC_ENDPOINT_DOWN_AFTER_SECS";

/// A supervised gateway whose QUIC endpoint dies exits through the fatal
/// listener path (exit 42), having noted the exit in the breaker marker, and
/// its next start runs QUIC again instead of tripping. The second such exit
/// within the window trips the next start (the gateway cooldown). An
/// unsupervised gateway keeps running over UDP.
///
/// Needs the `testing` feature, which CI's Linux test job builds with: the
/// endpoint is failed through a caught socket-adapter panic.
///
/// FALSIFY: drop the `note_endpoint_down_exit` call in
/// `connection_handler::listener_liveness`; the second start trips.
#[cfg(feature = "testing")]
#[test]
fn a_supervised_gateway_exits_on_endpoint_death_and_restarts_quic() -> anyhow::Result<()> {
    let dir = tempfile::tempdir()?;
    let down = [("FREENET_SUPERVISED", "1"), (ENDPOINT_DOWN_ENV, "2")];
    for run in 1..=freenet::ENDPOINT_DOWN_EXITS_TO_TRIP {
        let log_dir = dir.path().join(format!("logs-{run}"));
        let mut node = spawn_quic_node_full(dir.path(), &log_dir, &[], None, &down)?;
        let logs = node.logs();
        assert!(
            logs.contains(QUIC_BUILT_MARKER) && !logs.contains(QUIC_TRIP_MARKER),
            "run {run}: an earlier endpoint-down exit must not trip. Logs:\n{logs}"
        );
        let status = node.wait_for_exit(Duration::from_secs(60))?;
        let logs = node.logs();
        assert_eq!(
            status.code(),
            Some(42),
            "run {run}: the fatal-listener exit code. Logs:\n{logs}"
        );
        assert!(logs.contains(ENDPOINT_DOWN_EXIT_MARKER), "Logs:\n{logs}");
        let marker = read_quic_marker(dir.path())?.expect("marker");
        assert_eq!(
            marker["endpoint_down_secs"].as_array().map(Vec::len),
            Some(run),
            "run {run}: the exit is noted. Marker: {marker}"
        );
    }
    // The next start sees them in the window: a trip.
    let mut node = spawn_quic_node_full(
        dir.path(),
        &dir.path().join("logs-trip"),
        &[],
        None,
        &[("FREENET_SUPERVISED", "1")],
    )?;
    let logs = node.logs();
    assert!(
        logs.contains(QUIC_TRIP_MARKER) && !logs.contains(QUIC_BUILT_MARKER),
        "repeated endpoint-down exits in the window trip. Logs:\n{logs}"
    );
    node.send_sigterm()?;
    assert_eq!(node.wait_for_exit(Duration::from_secs(45))?.code(), Some(0));

    // Unsupervised: the endpoint dies, the node keeps running.
    let dir = tempfile::tempdir()?;
    let mut node = spawn_quic_node_full(
        dir.path(),
        &dir.path().join("logs"),
        &[],
        None,
        &[(ENDPOINT_DOWN_ENV, "1")],
    )?;
    node.wait_for_logs(&[ENDPOINT_DOWN_MARKER], Duration::from_secs(30))?;
    std::thread::sleep(Duration::from_secs(2));
    assert!(
        !node.logs().contains(ENDPOINT_DOWN_EXIT_MARKER),
        "an unsupervised gateway must not exit. Logs:\n{}",
        node.logs()
    );
    node.send_sigterm()?;
    assert_eq!(
        node.wait_for_exit(Duration::from_secs(45))?.code(),
        Some(0),
        "Logs:\n{}",
        node.logs()
    );
    Ok(())
}

/// A peer keeps the 24 h cooldown: a trip 20 minutes old, which a gateway
/// would be past, still keeps QUIC off.
///
/// FALSIFY: make `BreakerRole::from_is_gateway` return `Gateway` for every
/// node; the peer builds the endpoint.
#[test]
fn quic_breaker_on_a_peer_holds_for_the_peer_cooldown() -> anyhow::Result<()> {
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)?
        .as_secs();
    let dir = tempfile::tempdir()?;
    write_tripped_marker(dir.path(), now - 20 * 60)?;
    let gateway = unreachable_gateway();
    let mut node =
        spawn_quic_node_with(dir.path(), &dir.path().join("logs"), &peer_args(&gateway))?;
    let logs = node.logs();
    // A peer decides by short runs (#6151), which holds an earlier release's
    // trip for the peer cooldown too.
    assert!(
        logs.contains(QUIC_SHORT_RUNS_COOLING_MARKER) && !logs.contains(QUIC_BUILT_MARKER),
        "a peer trip 20 min old must keep QUIC off. Logs:\n{logs}"
    );
    node.send_sigterm()?;
    assert_eq!(
        node.wait_for_exit(Duration::from_secs(45))?.code(),
        Some(0),
        "Logs:\n{}",
        node.logs()
    );
    Ok(())
}

/// #6151: a unix peer decides by short runs that panicked in QUIC code, as
/// Windows does, so a SIGKILL, which records no QUIC panic, never trips it,
/// however often it repeats. The exit-status rule it used before runs
/// beside it as a shadow with its own marker: it still reads each kill as a
/// crash and trips (telemetry reports that as `legacy_verdict`), and holds
/// that trip on the next start, with QUIC on throughout. A clean stop clears
/// both markers. The first start also takes an earlier release's Armed
/// marker as the shadow's evidence.
///
/// FALSIFY: make `decides_by_short_runs` return `!unix` (peers back on the
/// exit-status rule): the second start trips and builds no endpoint. Drop
/// the `legacy_shadow` call in `breaker::gate`: no shadow marker is written.
#[test]
fn a_unix_peer_decides_by_short_runs_with_the_exit_status_rule_as_a_shadow() -> anyhow::Result<()> {
    let dir = tempfile::tempdir()?;
    let gateway = unreachable_gateway();
    // An earlier release's marker for a run that crashed on this boot: the
    // shadow's first evidence. Pid 1 is alive, so use one that is not.
    let boot_id = std::fs::read_to_string("/proc/sys/kernel/random/boot_id").ok();
    if let Some(boot_id) = boot_id.as_deref().map(str::trim) {
        let dead_pid = i32::MAX - 1;
        let armed = serde_json::json!({
            "state": "armed", "pid": dead_pid, "boot_id": boot_id, "nonce": 1,
            "armed_at_secs": 1,
        });
        std::fs::write(
            dir.path().join(QUIC_MARKER_FILE),
            serde_json::to_vec(&armed)?,
        )?;
    }

    for run in 0..3 {
        let mut node = spawn_quic_node_with(
            dir.path(),
            &dir.path().join(format!("logs-{run}")),
            &peer_args(&gateway),
        )?;
        let logs = node.logs();
        assert!(
            logs.contains(QUIC_BUILT_MARKER)
                && !logs.contains(QUIC_TRIP_MARKER)
                && !logs.contains("Several runs in a row"),
            "run {run}: a killed peer never trips. Logs:\n{logs}"
        );
        if run == 0 && boot_id.is_some() {
            // The earlier release's crash: the shadow tripped at this start
            // and holds, so it arms nothing this run.
            let shadow = read_json(dir.path(), LEGACY_SHADOW_FILE)?;
            assert_eq!(
                shadow.as_ref().map(|m| m["marker"]["state"].clone()),
                Some("tripped".into()),
                "{shadow:?}. Logs:\n{logs}"
            );
            node.send_sigterm()?;
            assert_eq!(node.wait_for_exit(Duration::from_secs(45))?.code(), Some(0));
            std::fs::remove_file(dir.path().join(LEGACY_SHADOW_FILE))?;
            continue;
        }
        wait_for_peer_markers(dir.path(), &node)?;
        node.kill_abnormally()?;
        drop(node);
        let after = spawn_quic_node_with(
            dir.path(),
            &dir.path().join(format!("logs-{run}-after")),
            &peer_args(&gateway),
        )?;
        let shadow = read_json(dir.path(), LEGACY_SHADOW_FILE)?;
        assert_eq!(
            shadow.as_ref().map(|m| m["marker"]["state"].clone()),
            Some("tripped".into()),
            "the exit-status rule reads a kill as a crash: {shadow:?}. Logs:\n{}",
            after.logs()
        );
        assert!(after.logs().contains(QUIC_BUILT_MARKER), "{}", after.logs());
        drop(after);
        std::fs::remove_file(dir.path().join(LEGACY_SHADOW_FILE))?;
    }

    // A clean stop clears both markers.
    let mut node = spawn_quic_node_with(
        dir.path(),
        &dir.path().join("logs-clean"),
        &peer_args(&gateway),
    )?;
    wait_for_peer_markers(dir.path(), &node)?;
    node.send_sigterm()?;
    assert_eq!(node.wait_for_exit(Duration::from_secs(45))?.code(), Some(0));
    assert_eq!(read_quic_marker(dir.path())?, None);
    assert_eq!(read_json(dir.path(), LEGACY_SHADOW_FILE)?, None);
    Ok(())
}

/// The breaker's start report, as logged with the `quic_breaker_start`
/// payload.
#[cfg(feature = "quic-panic-injection")]
const START_REPORT_MARKER: &str = "QUIC crash-loop breaker start report";

/// The `quic_breaker_start` payload a run logged, parsed.
#[cfg(feature = "quic-panic-injection")]
fn start_report(logs: &str) -> anyhow::Result<serde_json::Value> {
    let line = logs
        .lines()
        .find(|line| line.contains(START_REPORT_MARKER))
        .ok_or_else(|| anyhow::anyhow!("no start report. Logs:\n{logs}"))?;
    let json = line
        .split_once("report=")
        .map(|(_, rest)| rest)
        .and_then(|rest| rest.find('{').map(|at| &rest[at..]))
        .ok_or_else(|| anyhow::anyhow!("no payload in {line}"))?;
    let end = json
        .rfind('}')
        .ok_or_else(|| anyhow::anyhow!("no payload end in {line}"))?;
    Ok(serde_json::from_str(&json[..=end])?)
}

/// #6151: a unix peer that aborts on a QUIC panic two runs in a row (its
/// count while UDP exists, `PEER_SHORT_RUNS_TO_TRIP_WITH_UDP`, one below the
/// post-update rollback's three crashes; the binary's
/// `peer_breaker_trips_before_the_crash_loop_rollback` pins that order)
/// trips the breaker at the third start and runs with QUIC off; the fourth
/// stays off without moving the trip. The panic hook marks each run, with the
/// panic's place, before the abort. The exit-status rule's shadow reads the
/// first abort as a crash and holds that trip, which the start report gives
/// as `legacy_verdict`, while QUIC stays on until the short-run rule trips.
/// Linux CI builds this file with `quic-panic-injection`.
///
/// FALSIFY: return early from `on_panic` in `breaker/short_runs.rs` (no run
/// is marked, nothing counts, the third start runs QUIC); make
/// `decides_by_short_runs` return `!unix` (the second start trips on the
/// first abort, by exit status); or give a peer the gateway's three (the
/// third start runs QUIC).
#[cfg(feature = "quic-panic-injection")]
#[test]
fn a_unix_peer_trips_after_two_quic_panics_in_a_row() -> anyhow::Result<()> {
    let dir = tempfile::tempdir()?;
    let gateway = unreachable_gateway();
    let inject = [("FREENET_TEST_QUIC_PANIC", "abort:5")];
    let trip_at = u64::from(freenet::PEER_SHORT_RUNS_TO_TRIP_WITH_UDP);
    assert_eq!(trip_at, 2);
    for run in 0..trip_at {
        let logs_dir = dir.path().join(format!("logs-{run}"));
        std::fs::create_dir_all(&logs_dir)?;
        let mut node =
            spawn_quic_node_full(dir.path(), &logs_dir, &peer_args(&gateway), None, &inject)?;
        let logs = node.logs();
        assert!(logs.contains(QUIC_BUILT_MARKER), "run {run}. Logs:\n{logs}");
        let report = start_report(&logs)?;
        assert_eq!(report["rule"], "short_runs", "{report}");
        assert_eq!(report["verdict"], "arm", "{report}");
        assert_eq!(report["short_runs"], run, "{report}");
        // The shadow: no evidence at the first start; the first abort is a
        // crash to it, a trip it then holds.
        let legacy = if run == 0 { "arm" } else { "trip" };
        assert_eq!(report["legacy_verdict"], legacy, "run {run}: {report}");
        if run > 0 {
            let at = report["quic_panic_at"].as_str().unwrap_or_default();
            assert!(
                at.starts_with("core/src/transport/quic/panic_injection.rs:"),
                "run {run}: {report}"
            );
        }
        let status = node.wait_for_exit(Duration::from_secs(60))?;
        assert!(
            !status.success(),
            "an abort: {status:?}. Logs:\n{}",
            node.logs()
        );
        let marker = read_quic_marker(dir.path())?;
        assert_eq!(
            marker
                .as_ref()
                .map(|m| (m["state"].clone(), m["quic_panicked"].clone())),
            Some(("short_runs".into(), true.into())),
            "run {run}: the hook marked the run before the abort: {marker:?}"
        );
    }

    let mut third = spawn_quic_node_with(
        dir.path(),
        &dir.path().join("logs-trip"),
        &peer_args(&gateway),
    )?;
    let logs = third.logs();
    assert!(
        logs.contains("Several runs in a row with QUIC enabled ended early")
            && !logs.contains(QUIC_BUILT_MARKER),
        "two QUIC aborts in a row trip a peer. Logs:\n{logs}"
    );
    let report = start_report(&logs)?;
    assert_eq!(report["verdict"], "trip", "{report}");
    assert_eq!(report["quic_off"], true, "{report}");
    assert_eq!(report["short_runs"], trip_at, "{report}");
    assert_eq!(report["legacy_verdict"], "cool", "{report}");
    let tripped = read_quic_marker(dir.path())?;
    assert_eq!(
        tripped.as_ref().map(|m| m["state"].clone()),
        Some("tripped".into()),
        "{logs}"
    );
    third.send_sigterm()?;
    assert_eq!(
        third.wait_for_exit(Duration::from_secs(45))?.code(),
        Some(0)
    );

    let mut fourth = spawn_quic_node_with(
        dir.path(),
        &dir.path().join("logs-cool"),
        &peer_args(&gateway),
    )?;
    let logs = fourth.logs();
    assert!(
        logs.contains(QUIC_SHORT_RUNS_COOLING_MARKER) && !logs.contains(QUIC_BUILT_MARKER),
        "{logs}"
    );
    assert_eq!(
        read_quic_marker(dir.path())?,
        tripped,
        "the trip does not move"
    );
    fourth.send_sigterm()?;
    assert_eq!(
        fourth.wait_for_exit(Duration::from_secs(45))?.code(),
        Some(0)
    );
    Ok(())
}

/// The breaker's WARN when a peer with no UDP backs off (#6151).
#[cfg(feature = "quic-panic-injection")]
const QUIC_BACKOFF_MARKER: &str = "QUIC stays on, the node checks for an update";

/// #6151, the post-UDP path (`breaker::UDP_FALLBACK` false, switched on in
/// this test build by `FREENET_TEST_BREAKER_NO_UDP_FALLBACK`): a unix peer
/// whose three runs abort on a QUIC panic does not turn QUIC off at the
/// fourth start; it backs off (a 30 s first step here, by
/// `FREENET_TEST_BREAKER_BACKOFF_SECS`) with the transport not yet open and
/// the marker already recording the step and no QUIC panic. A SIGTERM during
/// the wait ends the node promptly with exit 0, and the next start reads the
/// marker as a run with no QUIC panic: QUIC on at once, no wait.
///
/// FALSIFY: drop the `wait_out_crash_loop_backoff` call before the transport
/// opens (the fourth start binds at once); write the backoff marker only at
/// arming (it is not on disk during the wait); or make a backoff marker
/// record a QUIC panic (the fifth start backs off again).
#[cfg(feature = "quic-panic-injection")]
#[test]
fn with_no_udp_a_unix_peer_backs_off_and_a_stop_during_the_wait_ends_it() -> anyhow::Result<()> {
    let dir = tempfile::tempdir()?;
    let gateway = unreachable_gateway();
    let no_udp = ("FREENET_TEST_BREAKER_NO_UDP_FALLBACK", "1");
    let backoff = ("FREENET_TEST_BREAKER_BACKOFF_SECS", "30");
    let inject = [no_udp, backoff, ("FREENET_TEST_QUIC_PANIC", "abort:5")];
    for run in 0..3u64 {
        let logs_dir = dir.path().join(format!("logs-{run}"));
        let mut node =
            spawn_quic_node_full(dir.path(), &logs_dir, &peer_args(&gateway), None, &inject)?;
        let report = start_report(&node.logs())?;
        assert_eq!(report["verdict"], "arm", "run {run}: {report}");
        assert_eq!(report["short_runs"], run, "{report}");
        let status = node.wait_for_exit(Duration::from_secs(60))?;
        assert!(
            !status.success(),
            "an abort: {status:?}. Logs:\n{}",
            node.logs()
        );
    }

    let logs_dir = dir.path().join("logs-backoff");
    std::fs::create_dir_all(&logs_dir)?;
    let mut args = vec!["--transport-quic", "prefer"];
    args.extend_from_slice(&peer_args(&gateway));
    let mut waiting =
        NodeProcess::spawn_full(dir.path(), &logs_dir, &args, None, &[no_udp, backoff])?;
    waiting.wait_for_logs(
        &[READY_MARKER, QUIC_BACKOFF_MARKER],
        Duration::from_secs(120),
    )?;
    let logs = waiting.logs();
    let report = start_report(&logs)?;
    assert_eq!(report["verdict"], "backoff", "{report}");
    assert_eq!(report["quic_off"], false, "{report}");
    assert_eq!(report["short_runs"], 3, "{report}");
    assert_eq!(report["backoff_step"], 1, "{report}");
    assert!(
        !logs.contains(TRANSPORT_BOUND_MARKER) && !logs.contains(QUIC_BUILT_MARKER),
        "the transport waits. Logs:\n{logs}"
    );
    let marker = read_quic_marker(dir.path())?;
    assert_eq!(
        marker.as_ref().map(|m| (
            m["state"].clone(),
            m["backoff_step"].clone(),
            m["quic_panicked"].clone()
        )),
        Some(("short_runs".into(), 1.into(), false.into())),
        "the step is on disk during the wait, with no QUIC panic: {marker:?}"
    );

    let stop_sent = Instant::now();
    waiting.send_sigterm()?;
    let status = waiting.wait_for_exit(Duration::from_secs(25))?;
    assert_eq!(status.code(), Some(0), "Logs:\n{}", waiting.logs());
    assert!(
        stop_sent.elapsed() < Duration::from_secs(20),
        "the stop did not wait out the backoff"
    );
    let after_stop = read_quic_marker(dir.path())?;
    assert!(
        after_stop
            .as_ref()
            .is_none_or(|m| m["state"] == "short_runs" && m["quic_panicked"] == false),
        "a stop during the wait leaves no QUIC panic recorded: {after_stop:?}"
    );

    let mut next = spawn_quic_node_full(
        dir.path(),
        &dir.path().join("logs-next"),
        &peer_args(&gateway),
        None,
        &[no_udp, backoff],
    )?;
    let logs = next.logs();
    assert!(
        logs.contains(QUIC_BUILT_MARKER) && !logs.contains(QUIC_BACKOFF_MARKER),
        "the start after a stop during the wait runs QUIC at once. Logs:\n{logs}"
    );
    assert_eq!(start_report(&logs)?["verdict"], "arm");
    next.send_sigterm()?;
    assert_eq!(next.wait_for_exit(Duration::from_secs(45))?.code(), Some(0));
    Ok(())
}

/// Write a Tripped breaker marker for `tripped_at_secs`.
fn write_tripped_marker(dir: &Path, tripped_at_secs: u64) -> anyhow::Result<()> {
    let marker = serde_json::json!({
        "state": "tripped",
        "tripped_at_secs": tripped_at_secs,
    });
    std::fs::write(dir.join(QUIC_MARKER_FILE), serde_json::to_vec(&marker)?)?;
    Ok(())
}

/// Logged by a gateway that exits for its supervisor to restore QUIC.
const GATEWAY_RESTART_MARKER: &str = "Gateway exiting for its service supervisor to restart it";

/// A supervised gateway that starts with QUIC kept off by a cooldown exits
/// when the cooldown ends (after at least `QUIC_RESTORE_MIN_UPTIME`, 90 s),
/// with exit 42, which the crash counters and the rollback treat as no
/// crash; its next start runs QUIC. An unsupervised one logs an ERROR and
/// keeps running.
///
/// FALSIFY: drop the `set_quic_restore_at` call in `breaker::gate`'s Cool
/// arm; the node never exits and the wait times out.
#[test]
fn a_supervised_gateway_restarts_when_its_breaker_cooldown_ends() -> anyhow::Result<()> {
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)?
        .as_secs();
    let dir = tempfile::tempdir()?;
    // The cooldown has about a minute left; the restart waits the 90 s floor.
    write_tripped_marker(dir.path(), now - 14 * 60)?;
    let supervised = [("FREENET_SUPERVISED", "1")];
    let mut node = spawn_quic_node_full(
        dir.path(),
        &dir.path().join("logs-1"),
        &[],
        None,
        &supervised,
    )?;
    let logs = node.logs();
    assert!(
        logs.contains(QUIC_COOLING_MARKER) && !logs.contains(QUIC_BUILT_MARKER),
        "QUIC off during the cooldown. Logs:\n{logs}"
    );
    let started = Instant::now();
    let status = node.wait_for_exit(Duration::from_secs(150))?;
    let logs = node.logs();
    assert_eq!(status.code(), Some(42), "Logs:\n{logs}");
    assert!(
        started.elapsed() >= Duration::from_secs(80),
        "not before the floor"
    );
    assert!(logs.contains(GATEWAY_RESTART_MARKER), "Logs:\n{logs}");
    assert!(
        !logs.contains("CRITICAL"),
        "a planned restart. Logs:\n{logs}"
    );

    let mut node = spawn_quic_node_full(
        dir.path(),
        &dir.path().join("logs-2"),
        &[],
        None,
        &supervised,
    )?;
    let logs = node.logs();
    assert!(
        logs.contains(QUIC_BUILT_MARKER),
        "QUIC on after the restart. Logs:\n{logs}"
    );
    node.send_sigterm()?;
    assert_eq!(node.wait_for_exit(Duration::from_secs(45))?.code(), Some(0));

    // Unsupervised: an ERROR, and no exit.
    let dir = tempfile::tempdir()?;
    write_tripped_marker(dir.path(), now - 5 * 60)?;
    let mut node = spawn_quic_node(dir.path(), &dir.path().join("logs"))?;
    let logs = node.logs();
    assert!(
        logs.contains("QUIC is off on this gateway until it restarts"),
        "Logs:\n{logs}"
    );
    node.send_sigterm()?;
    assert_eq!(node.wait_for_exit(Duration::from_secs(45))?.code(), Some(0));
    Ok(())
}

/// A gateway replaces a breaker marker it cannot read and runs QUIC armed;
/// one it cannot replace (here a directory) keeps QUIC off for the run.
///
/// FALSIFY: remove the gateway branch for `Verdict::Unreadable` in
/// `breaker::gate`; the unreadable file keeps QUIC off.
#[cfg(unix)]
#[test]
fn a_gateway_replaces_an_unreadable_breaker_marker() -> anyhow::Result<()> {
    use std::os::unix::fs::PermissionsExt;
    let dir = tempfile::tempdir()?;
    let path = dir.path().join(QUIC_MARKER_FILE);
    std::fs::write(&path, b"{}")?;
    std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o000))?;
    if std::fs::read(&path).is_ok() {
        // Root reads it anyway; nothing to test here.
        return Ok(());
    }
    let mut node = spawn_quic_node(dir.path(), &dir.path().join("logs"))?;
    let logs = node.logs();
    assert!(
        logs.contains(QUIC_BUILT_MARKER) && logs.contains("this gateway replaced it"),
        "Logs:\n{logs}"
    );
    wait_for_armed_marker(dir.path(), &node)?;
    node.send_sigterm()?;
    assert_eq!(node.wait_for_exit(Duration::from_secs(45))?.code(), Some(0));

    let dir = tempfile::tempdir()?;
    std::fs::create_dir(dir.path().join(QUIC_MARKER_FILE))?;
    let mut node = spawn_quic_node(dir.path(), &dir.path().join("logs"))?;
    let logs = node.logs();
    assert!(
        !logs.contains(QUIC_BUILT_MARKER)
            && logs.contains("Could not read or replace the QUIC crash-breaker marker"),
        "Logs:\n{logs}"
    );
    node.send_sigterm()?;
    assert_eq!(node.wait_for_exit(Duration::from_secs(45))?.code(), Some(0));
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
    let mut first = spawn_quic_node_full(
        dir.path(),
        &dir.path().join("logs-1"),
        &[],
        Some(&home),
        &[],
    )?;
    wait_for_armed_marker(dir.path(), &first)?;
    assert_eq!(entries(&note)?, vec![OTHER], "another node's entry is left");

    // Our pid beside it: no trip, and only our pid is taken out.
    let pid = first.pid();
    first.kill_abnormally()?;
    drop(first);
    std::fs::write(&note, format!("{OTHER}\n{pid}\n"))?;
    let mut second = spawn_quic_node_full(
        dir.path(),
        &dir.path().join("logs-2"),
        &[],
        Some(&home),
        &[],
    )?;
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

/// A deliberate endpoint-death exit takes 42 even in the first minute under a
/// unit that understands the fast-crash code (`FREENET_SYSTEMD_FAST_CRASH`):
/// 45 is counted by the post-update crash-loop rollback, so three early
/// endpoint deaths would roll a fresh release back.
///
/// FALSIFY: in `node::p2p_impl::listener_exit_code`, return
/// `fatal_listener_exit_code(..)` for a `GatewayQuicRestart` too; the exit is 45.
#[cfg(feature = "testing")]
#[test]
fn an_early_endpoint_death_exit_is_42_under_a_fast_crash_unit() -> anyhow::Result<()> {
    let dir = tempfile::tempdir()?;
    let env = [
        ("FREENET_SUPERVISED", "1"),
        ("FREENET_SYSTEMD_FAST_CRASH", "1"),
        (ENDPOINT_DOWN_ENV, "2"),
    ];
    let mut node = spawn_quic_node_full(dir.path(), &dir.path().join("logs"), &[], None, &env)?;
    let started = Instant::now();
    let status = node.wait_for_exit(Duration::from_secs(60))?;
    let logs = node.logs();
    assert!(
        started.elapsed() < Duration::from_secs(55),
        "the death must come inside the fast-crash window. Logs:\n{logs}"
    );
    assert!(logs.contains(ENDPOINT_DOWN_EXIT_MARKER), "Logs:\n{logs}");
    assert_eq!(status.code(), Some(42), "Logs:\n{logs}");
    Ok(())
}

/// Only our own unit's marker turns gateway restarts on. Under systemd alone
/// (`INVOCATION_ID`), a gateway logs a WARN and, when its endpoint dies,
/// keeps running over UDP; so does a supervised peer.
///
/// FALSIFY: map `SupervisorStatus::SupervisedUnverified` to
/// `GatewayQuicRestarts::Enabled` in `bin/freenet.rs`; the first node exits.
#[cfg(feature = "testing")]
#[test]
fn an_endpoint_death_without_our_supervisor_or_on_a_peer_does_not_exit() -> anyhow::Result<()> {
    let unverified_dir = tempfile::tempdir()?;
    let peer_dir = tempfile::tempdir()?;
    // A peer needs a gateway to start; this one is never reached.
    let gateway = format!("127.0.0.1:9,{}", "11".repeat(32));
    let unverified_env = [
        ("INVOCATION_ID", "0123456789abcdef"),
        (ENDPOINT_DOWN_ENV, "1"),
    ];
    let peer_args = [AS_PEER, "--gateway", &gateway];
    let peer_env = [("FREENET_SUPERVISED", "1"), (ENDPOINT_DOWN_ENV, "1")];
    let cases = [
        (unverified_dir.path(), &[][..], &unverified_env[..]),
        (peer_dir.path(), &peer_args[..], &peer_env[..]),
    ];
    for (index, (dir, args, env)) in cases.into_iter().enumerate() {
        let mut node = spawn_quic_node_full(dir, &dir.join("logs"), args, None, env)?;
        node.wait_for_logs(&[ENDPOINT_DOWN_MARKER], Duration::from_secs(30))?;
        std::thread::sleep(Duration::from_secs(3));
        let logs = node.logs();
        assert!(
            !logs.contains(GATEWAY_RESTART_MARKER),
            "case {index}: must not exit. Logs:\n{logs}"
        );
        if index == 0 {
            assert!(
                logs.contains("does not set FREENET_SUPERVISED=1"),
                "the unverified-supervisor WARN. Logs:\n{logs}"
            );
        }
        node.send_sigterm()?;
        assert_eq!(
            node.wait_for_exit(Duration::from_secs(45))?.code(),
            Some(0),
            "case {index}. Logs:\n{}",
            node.logs()
        );
    }
    Ok(())
}

/// A gateway's cooldown doubles on a repeat trip: a crash of a run whose
/// marker carries a trip from 20 minutes ago trips again with a 30-minute
/// cooldown, recorded in the marker, and a start inside it keeps QUIC off
/// where a first trip's 15 minutes would have ended.
///
/// FALSIFY: return 1 from `breaker::trip_number`; the marker records no
/// repeat and the third start runs QUIC.
#[test]
fn a_gateway_cooldown_grows_on_a_repeat_trip() -> anyhow::Result<()> {
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)?
        .as_secs();
    let dir = tempfile::tempdir()?;
    let mut first = spawn_quic_node(dir.path(), &dir.path().join("logs-1"))?;
    let mut armed = wait_for_armed_marker(dir.path(), &first)?;
    first.kill_abnormally()?;
    drop(first);
    // The crashed run had restarted after a first trip 20 minutes ago.
    armed["prior_trip"] = serde_json::json!({ "tripped_at_secs": now - 20 * 60, "trips": 1 });
    std::fs::write(
        dir.path().join(QUIC_MARKER_FILE),
        serde_json::to_vec(&armed)?,
    )?;

    let mut second = spawn_quic_node(dir.path(), &dir.path().join("logs-2"))?;
    let logs = second.logs();
    assert!(
        logs.contains(QUIC_TRIP_MARKER) && logs.contains("cooldown_minutes=30"),
        "a repeat trip doubles the cooldown. Logs:\n{logs}"
    );
    let marker = read_quic_marker(dir.path())?.expect("marker");
    assert_eq!(marker["trips"], 2, "Marker: {marker}");
    second.send_sigterm()?;
    assert_eq!(
        second.wait_for_exit(Duration::from_secs(45))?.code(),
        Some(0)
    );

    // That trip, 20 minutes on: a first trip would be over, a second is not.
    write_tripped_marker(dir.path(), now - 20 * 60)?;
    let mut marker = read_quic_marker(dir.path())?.expect("marker");
    marker["trips"] = 2.into();
    std::fs::write(
        dir.path().join(QUIC_MARKER_FILE),
        serde_json::to_vec(&marker)?,
    )?;
    let mut third = spawn_quic_node(dir.path(), &dir.path().join("logs-3"))?;
    let logs = third.logs();
    assert!(
        logs.contains(QUIC_COOLING_MARKER) && !logs.contains(QUIC_BUILT_MARKER),
        "a second trip holds for 30 minutes. Logs:\n{logs}"
    );
    third.send_sigterm()?;
    assert_eq!(
        third.wait_for_exit(Duration::from_secs(45))?.code(),
        Some(0)
    );
    Ok(())
}

/// Under the breaker override (`quic-ignore-crash-breaker`) a supervised
/// gateway does not exit when its endpoint dies: the next start would ignore
/// the record, so a death at every start would restart it forever (and stop
/// the unit at systemd's start limit). It keeps running over UDP, says why,
/// and records nothing.
///
/// FALSIFY: drop the `overridden` check in
/// `breaker::note_endpoint_down_exit_with`; the gateway exits 42.
#[cfg(feature = "testing")]
#[test]
fn under_the_breaker_override_an_endpoint_death_does_not_exit() -> anyhow::Result<()> {
    let dir = tempfile::tempdir()?;
    let env = [("FREENET_SUPERVISED", "1"), (ENDPOINT_DOWN_ENV, "1")];
    let mut node = spawn_quic_node_full(
        dir.path(),
        &dir.path().join("logs"),
        &["--quic-ignore-crash-breaker"],
        None,
        &env,
    )?;
    node.wait_for_logs(&[ENDPOINT_DOWN_MARKER], Duration::from_secs(30))?;
    std::thread::sleep(Duration::from_secs(3));
    let logs = node.logs();
    assert!(
        !logs.contains(GATEWAY_RESTART_MARKER),
        "must not exit under the override. Logs:\n{logs}"
    );
    assert!(
        logs.contains("The crash-loop breaker is overridden"),
        "the WARN names the override. Logs:\n{logs}"
    );
    let marker = read_quic_marker(dir.path())?;
    assert!(
        marker
            .as_ref()
            .and_then(|m| m.get("endpoint_down_secs"))
            .is_none(),
        "nothing recorded. Marker: {marker:?}"
    );
    node.send_sigterm()?;
    assert_eq!(
        node.wait_for_exit(Duration::from_secs(45))?.code(),
        Some(0),
        "Logs:\n{}",
        node.logs()
    );
    Ok(())
}
