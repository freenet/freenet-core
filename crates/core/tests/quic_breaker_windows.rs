//! The QUIC crash-loop breaker on Windows, through the real `freenet` process
//! (#6031).
//!
//! Three runs killed right after they start leave a marker that counts them,
//! the fourth start records a trip and runs with QUIC off, and the fifth stays
//! off without moving the trip. The decision itself is tested on every
//! platform in `quic/breaker/short_runs.rs`; this test covers what those tests
//! cannot reach, which is the wiring in the binary: `gate` taking the Windows
//! branch, and the marker being written with no SIGTERM handler to wait for.
//!
//! The `windows_unit` CI job runs it.
//!
//! Windows only: on unix the same process path is the crash-detecting breaker,
//! tested in `graceful_shutdown_exit_code.rs`.

#![cfg(windows)]

use std::{
    path::{Path, PathBuf},
    process::{Child, Command, Stdio},
    time::{Duration, Instant},
};

const MARKER_FILE: &str = "quic-crash-breaker.json";
/// Logged right after the node decides whether to build the QUIC endpoint.
const TRANSPORT_BOUND: &str = "UDP socket bound successfully";
const QUIC_BUILT: &str = "QUIC endpoint built";
const TRIP_WARN: &str = "Several runs in a row with QUIC enabled ended early";
const COOLING_WARN: &str = "QUIC stays off: several runs in a row with QUIC enabled ended early";
/// What a run logs when its marker gets no timer, so it could never clear.
const NO_TIMER_WARN: &str = "No task registrar";

/// How long one start may take, from spawn to the last thing a test waits for.
/// The `ci` nextest profile ends a test at 240 s, and the longest test here has
/// five starts, so a stuck start fails with its logs instead of being killed.
const START_LIMIT: Duration = Duration::from_secs(40);

/// What turns QUIC on for a test node.
const QUIC_ON: &[&str] = &["--transport-quic", "prefer"];

/// A free TCP port, also free for UDP right now.
fn free_port() -> anyhow::Result<u16> {
    for _ in 0..100 {
        let tcp = std::net::TcpListener::bind(("127.0.0.1", 0))?;
        let port = tcp.local_addr()?.port();
        if std::net::UdpSocket::bind(("127.0.0.1", port)).is_ok() {
            return Ok(port);
        }
    }
    anyhow::bail!("no port free for both TCP and UDP")
}

/// A `freenet network` process, killed on drop.
struct Node {
    child: Child,
    log_dir: PathBuf,
    deadline: Instant,
}

impl Drop for Node {
    fn drop(&mut self) {
        if self.child.kill().is_ok() {
            let _reaped = self.child.wait().is_ok();
        }
    }
}

impl Node {
    /// Start an offline gateway with `extra_args` and wait until it has acted
    /// on the QUIC decision.
    fn start(dir: &Path, log_dir: PathBuf, extra_args: &[&str]) -> anyhow::Result<Self> {
        std::fs::create_dir_all(&log_dir)?;
        let deadline = Instant::now() + START_LIMIT;
        let ws_port = free_port()?.to_string();
        let network_port = free_port()?.to_string();
        let child = Command::new(env!("CARGO_BIN_EXE_freenet"))
            .arg("network")
            // As in `graceful_shutdown_exit_code.rs`: no telemetry from a test,
            // and logs in a file, at a level that holds the lines read here.
            .env("FREENET_TELEMETRY_ENABLED", "false")
            .env("RUST_LOG", "info")
            .env_remove("FREENET_LOG_TO_STDERR")
            .env_remove("FREENET_DISABLE_LOGS")
            .env_remove("FREENET_LOG_FORMAT")
            .args(["--ws-api-address", "127.0.0.1"])
            .args(["--ws-api-port", &ws_port])
            .args(["--network-address", "127.0.0.1"])
            .args(["--network-port", &network_port])
            .args(["--public-network-address", "127.0.0.1"])
            .args(["--public-network-port", &network_port])
            .arg("--is-gateway")
            .arg("--skip-load-from-network")
            .arg("--ignore-protocol-checking")
            .arg("--disable-auto-update")
            .args(["--location", "0.5"])
            .args(extra_args)
            .args(["--config-dir", &dir.to_string_lossy()])
            .args(["--data-dir", &dir.to_string_lossy()])
            .args(["--log-dir", &log_dir.to_string_lossy()])
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()?;
        let mut node = Self {
            child,
            log_dir,
            deadline,
        };
        node.wait_for_log(TRANSPORT_BOUND)?;
        Ok(node)
    }

    fn logs(&self) -> String {
        let mut out = String::new();
        for entry in std::fs::read_dir(&self.log_dir).into_iter().flatten() {
            let path = entry.map(|e| e.path()).unwrap_or_default();
            if path.extension().is_some_and(|e| e == "log") {
                out.push_str(&std::fs::read_to_string(&path).unwrap_or_default());
            }
        }
        out
    }

    fn wait_for_log(&mut self, line: &str) -> anyhow::Result<()> {
        loop {
            if let Some(status) = self.child.try_wait()? {
                anyhow::bail!("exited ({status:?}) before {line:?}:\n{}", self.logs());
            }
            if self.logs().contains(line) {
                return Ok(());
            }
            if Instant::now() >= self.deadline {
                anyhow::bail!("no {line:?} within {START_LIMIT:?}:\n{}", self.logs());
            }
            std::thread::sleep(Duration::from_millis(100));
        }
    }

    /// Wait until this run's marker is on disk with `short_runs` counted. A
    /// failed read is retried: Windows refuses to open a file while the node
    /// renames it into place.
    fn wait_for_marker(&self, dir: &Path, short_runs: u64) -> anyhow::Result<()> {
        loop {
            let marker = read_marker(dir);
            let counted = marker.as_ref().ok().and_then(|marker| {
                let marker = marker.as_ref()?;
                (marker["state"] == "short_runs").then(|| marker["short_runs"].as_u64())?
            });
            if counted == Some(short_runs) {
                return Ok(());
            }
            if Instant::now() >= self.deadline {
                anyhow::bail!(
                    "the marker did not reach {short_runs}: {marker:?}\n{}",
                    self.logs()
                );
            }
            std::thread::sleep(Duration::from_millis(50));
        }
    }
}

fn read_marker(dir: &Path) -> anyhow::Result<Option<serde_json::Value>> {
    match std::fs::read(dir.join(MARKER_FILE)) {
        Ok(bytes) => Ok(Some(serde_json::from_slice(&bytes)?)),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(e) => Err(e.into()),
    }
}

/// Fails at the first `wait_for_marker` if `gate` does not take the Windows
/// branch, since no marker is then written.
#[test]
fn three_short_runs_keep_quic_off_for_the_cooldown() -> anyhow::Result<()> {
    let dir = tempfile::tempdir()?;
    for short_runs in 0..3 {
        let logs = dir.path().join(format!("logs-{short_runs}"));
        let mut node = Node::start(dir.path(), logs, QUIC_ON)?;
        node.wait_for_log(QUIC_BUILT)?;
        node.wait_for_marker(dir.path(), short_runs)?;
        // The warning comes just after the marker is written, and the log
        // writer runs on its own thread.
        std::thread::sleep(Duration::from_millis(500));
        assert!(!node.logs().contains(NO_TIMER_WARN), "{}", node.logs());
        node.child.kill()?;
        node.child.wait()?;
    }

    let fourth = Node::start(dir.path(), dir.path().join("logs-trip"), QUIC_ON)?;
    let logs = fourth.logs();
    assert!(logs.contains(TRIP_WARN), "{logs}");
    assert!(!logs.contains(QUIC_BUILT), "{logs}");
    let tripped = read_marker(dir.path())?;
    assert_eq!(
        tripped.as_ref().map(|marker| marker["state"].clone()),
        Some("tripped".into()),
        "{logs}"
    );
    drop(fourth);

    let fifth = Node::start(dir.path(), dir.path().join("logs-cool"), QUIC_ON)?;
    let logs = fifth.logs();
    assert!(logs.contains(COOLING_WARN), "{logs}");
    assert!(!logs.contains(QUIC_BUILT), "{logs}");
    assert_eq!(read_marker(dir.path())?, tripped, "the trip does not move");
    Ok(())
}

/// QUIC stays off by default on Windows until the breaker has been confirmed on
/// a real Windows machine (#6031). This fails if the default changes.
#[test]
fn quic_is_off_by_default_on_windows() -> anyhow::Result<()> {
    let dir = tempfile::tempdir()?;
    let node = Node::start(dir.path(), dir.path().join("logs"), &[])?;
    let logs = node.logs();
    assert!(!logs.contains(QUIC_BUILT), "{logs}");
    assert_eq!(read_marker(dir.path())?, None, "no marker with QUIC off");
    Ok(())
}
