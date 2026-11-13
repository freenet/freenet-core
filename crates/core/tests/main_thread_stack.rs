//! The node must not run on the process's main thread.
//!
//! The main thread's stack is fixed by the platform when the process starts:
//! 1 MiB on Windows, against 2 MiB for every thread std spawns (and more under
//! `RUST_MIN_STACK`, which CI sets, but which does not reach the main thread).
//! `run_node` used to `block_on` the whole node, so its startup futures were
//! polled on the main thread. In a debug build their nested poll frames used
//! about 900 KiB there by the first event-loop turn, a few KiB short of the
//! Windows limit. A branch that made those frames slightly larger (#6207, QUIC
//! `prefer` by default) overflowed in the Windows debug binary at startup
//! (exit 0xC00000FD, STATUS_STACK_OVERFLOW) before its transport was up. Linux
//! CI could not see it: the Linux main thread has 8 MiB.
//!
//! This test starts the real binary with a 640 KiB main-thread stack, set with
//! `ulimit -s` before `exec`, and requires the node to come up and keep running.
//! Before the fix the same build aborted with "thread 'main' has overflowed its
//! stack" at 640 KiB (and at every size up to about 900 KiB). With the node on
//! a worker, the main thread holds only command-line parsing and the wait on
//! the node task; clap's derived parser alone takes about 400 KiB in a debug
//! build, so the limit leaves room for it and none for the node.
//!
//! Linux only: `ulimit -s` sets the main thread's stack on exec there. The
//! Windows half is `tests/quic_breaker_windows.rs`, which runs the binary with
//! its real 1 MiB main stack.

#![cfg(target_os = "linux")]

use std::{
    path::{Path, PathBuf},
    process::{Child, Command, Stdio},
    time::{Duration, Instant},
};

/// Main-thread stack for the node, in KiB. See the module doc for the sizes
/// measured on either side of it.
const MAIN_STACK_KIB: u32 = 640;

/// Logged once the network listener is open, after the transport is bound:
/// the point the Windows run never reached.
const LISTENER_OPEN: &str = "Network listener opened";

/// How long the node must keep running after `LISTENER_OPEN`. Before the fix
/// the overflow came on the first event-loop turns, within milliseconds.
const KEEP_RUNNING: Duration = Duration::from_secs(3);

/// A port free in both TCP and UDP right now, below the ephemeral range.
fn reserve_port() -> std::io::Result<u16> {
    use std::sync::atomic::{AtomicU32, Ordering};
    const FIRST: u32 = 20_000;
    const COUNT: u32 = 10_000;
    static NEXT: AtomicU32 = AtomicU32::new(0);
    let start = std::process::id().wrapping_mul(2_654_435_761);
    for _ in 0..COUNT {
        let offset = start.wrapping_add(NEXT.fetch_add(1, Ordering::Relaxed)) % COUNT;
        let port = (FIRST + offset) as u16;
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

/// The spawned node, killed and reaped on drop.
struct Node {
    child: Child,
    log_dir: PathBuf,
}

impl Drop for Node {
    fn drop(&mut self) {
        if self.child.kill().is_ok() {
            let _reaped = self.child.wait().is_ok();
        }
    }
}

impl Node {
    fn spawn(dir: &Path, extra_args: &[&str]) -> anyhow::Result<Self> {
        let log_dir = dir.join("logs");
        std::fs::create_dir_all(&log_dir)?;
        let ws_port = reserve_port()?.to_string();
        let network_port = reserve_port()?.to_string();
        let dir_arg = dir.to_string_lossy().into_owned();
        let log_arg = log_dir.to_string_lossy().into_owned();
        let child = Command::new("/bin/sh")
            // `$0` is the binary and `$@` its arguments; `exec` keeps the
            // limit for the node's own main thread.
            .arg("-c")
            .arg(format!("ulimit -s {MAIN_STACK_KIB} && exec \"$0\" \"$@\""))
            .arg(env!("CARGO_BIN_EXE_freenet"))
            .arg("network")
            // Same guards as graceful_shutdown_exit_code.rs: no telemetry, an
            // INFO file log whatever CI exports, and the QUIC mode from the
            // arguments alone.
            .env("FREENET_TELEMETRY_ENABLED", "false")
            .env("FREENET_QUIC_BREAKER_STOP_NOTE", dir.join("stop-note"))
            .env("RUST_LOG", "info")
            .env_remove("FREENET_LOG_TO_STDERR")
            .env_remove("FREENET_DISABLE_LOGS")
            .env_remove("FREENET_LOG_FORMAT")
            .env_remove("FREENET_TRANSPORT_QUIC")
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
            .args(["--config-dir", &dir_arg])
            .args(["--data-dir", &dir_arg])
            .args(["--log-dir", &log_arg])
            .args(extra_args)
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::piped())
            .spawn()?;
        Ok(Self { child, log_dir })
    }

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

    /// The process's exit, with its stderr (where the overflow is reported).
    fn exited(&mut self) -> anyhow::Result<Option<String>> {
        let Some(status) = self.child.try_wait()? else {
            return Ok(None);
        };
        let mut stderr = String::new();
        if let Some(mut pipe) = self.child.stderr.take() {
            use std::io::Read;
            let _read = pipe.read_to_string(&mut stderr);
        }
        Ok(Some(format!(
            "{status:?}\nstderr:\n{stderr}\nlogs:\n{}",
            self.logs()
        )))
    }
}

fn starts_and_runs_with_a_small_main_stack(extra_args: &[&str]) -> anyhow::Result<()> {
    let dir = tempfile::tempdir()?;
    let mut node = Node::spawn(dir.path(), extra_args)?;
    let deadline = Instant::now() + Duration::from_secs(120);
    while !node.logs().contains(LISTENER_OPEN) {
        if let Some(exit) = node.exited()? {
            anyhow::bail!(
                "the node exited before {LISTENER_OPEN:?} with a {MAIN_STACK_KIB} KiB main \
                 stack; is node work running on the main thread? {exit}"
            );
        }
        if Instant::now() >= deadline {
            anyhow::bail!("no {LISTENER_OPEN:?} within 120 s. Logs:\n{}", node.logs());
        }
        std::thread::sleep(Duration::from_millis(100));
    }
    let running_until = Instant::now() + KEEP_RUNNING;
    while Instant::now() < running_until {
        if let Some(exit) = node.exited()? {
            anyhow::bail!(
                "the node exited after {LISTENER_OPEN:?} with a {MAIN_STACK_KIB} KiB main \
                 stack; is node work running on the main thread? {exit}"
            );
        }
        std::thread::sleep(Duration::from_millis(100));
    }
    Ok(())
}

/// The default transport.
#[test]
fn the_node_starts_with_a_small_main_thread_stack() -> anyhow::Result<()> {
    starts_and_runs_with_a_small_main_stack(&[])
}

/// QUIC `prefer`, the configuration that overflowed on Windows.
#[test]
fn the_node_starts_with_a_small_main_thread_stack_with_quic_prefer() -> anyhow::Result<()> {
    starts_and_runs_with_a_small_main_stack(&["--transport-quic", "prefer"])
}

/// QUIC off, so the UDP-only startup path is covered too.
#[test]
fn the_node_starts_with_a_small_main_thread_stack_with_quic_off() -> anyhow::Result<()> {
    starts_and_runs_with_a_small_main_stack(&["--transport-quic", "off"])
}
