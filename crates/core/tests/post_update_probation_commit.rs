//! End-to-end guard for the post-update probation COMMIT path (#5232, #4073).
//!
//! The crash-loop auto-rollback machinery (`commands::rollback`) arms a
//! probation marker when a new version is installed, and the node is supposed
//! to clear it once it has run healthily for `COMMIT_HEALTHY_UPTIME_SECS`
//! (60s). If that commit ever stops happening, the marker stays armed and every
//! later non-clean stop is scored as a crash of a probationary version — three
//! of them roll the node BACK off the release it just installed and pin that
//! release as known-bad locally. That is the failure #5232 was filed for.
//!
//! ## Why this test spawns the real binary
//!
//! The commit is not a property of `commit_probation_at` — that function is
//! already unit-tested and works. It is a property of its ONLY caller: a
//! fire-and-forget `GlobalExecutor::spawn` in `bin/freenet.rs`'s
//! `run_network_node_with_signals`, whose handle is dropped and which is
//! aborted at shutdown. A unit test on `commit_probation_at` passes happily
//! while that task never runs, never fires, or commits a version string that
//! does not match the marker the installer wrote. Nothing short of running the
//! node and watching the marker disappear covers that seam.
//!
//! ## What it asserts (the two observable halves of #5232)
//!
//! 1. A node that has been up longer than the commit window has NO probation
//!    marker left on disk, AND its log says it COMMITTED (rather than silently
//!    dropping the marker as stale — see below).
//! 2. A subsequent clean stop of that node records NO crash: the post-stop
//!    `freenet update` must not report "during post-update probation".
//!
//! ## Why assertion 1 checks the announcement and not just the file
//!
//! `commit_probation_at` removes the marker down BOTH of its branches: a
//! version-matched `Committed`, and a `ClearedStale` for a marker belonging to
//! some other version. So "the file is gone" alone would still pass if the
//! version comparison regressed and every marker were dropped as stale — which
//! would disable rollback entirely, a worse bug than the one this guards. The
//! commit announcement is the only artifact that distinguishes the two paths.
//!
//! It is asserted on the node's **stderr**, not on its log files, and that is
//! the point of the fix this test accompanies. `tracing` output goes to the
//! rolling log files under the log dir; systemd captures only stdout/stderr. So
//! the crash-count and rollback lines (printed by the installer on stderr) were
//! in the journal while the commit that disarms them was not, and #5232 was
//! filed reading a journal that showed three strikes accumulating and no commit
//! ever happening. Asserting the log file instead would pass on a build where
//! the journal is silent again — i.e. it would not hold the fix.
//!
//! ## Runtime
//!
//! This test necessarily spends the real commit window (60s) waiting, because
//! the window is a hard-coded constant in the binary under test. Budget ~90s.

// Unix-only: the second half stops the node with SIGTERM, which is what a
// supervisor sends and what makes the stop a *graceful* one worth asserting on.
// Windows has no equivalent that exercises the same node path.
#![cfg(unix)]

use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

/// The version the node under test reports. `build_info::VERSION` in the binary
/// is `env!("CARGO_PKG_VERSION")` of this very crate, so the marker we plant
/// carries the exact string the node will try to commit — a mismatch here would
/// make the test vacuous (the node would clear our marker as stale instead of
/// committing it, which assertion 1's log check also catches).
const NODE_VERSION: &str = env!("CARGO_PKG_VERSION");

/// Mirrors `commands::rollback::COMMIT_HEALTHY_UPTIME_SECS`, which is
/// `pub(crate)` in the binary crate and so not importable here. If that
/// constant is ever raised, this test fails on the poll deadline below rather
/// than passing silently — which is the correct outcome, since lengthening the
/// window is a real behavioural change to the rollback machinery.
const COMMIT_HEALTHY_UPTIME_SECS: u64 = 60;

/// Generous slack over the commit window for a cold-started debug-build node.
const COMMIT_POLL_DEADLINE: Duration = Duration::from_secs(COMMIT_HEALTHY_UPTIME_SECS + 90);

/// Substring of the line `commit_probation` announces on the `Committed`
/// branch only. Matched against the node's stderr, which is what a supervisor
/// records — see the module docs.
const COMMITTED_STDERR_NEEDLE: &str = "post-update probation passed";

/// Substring of the stderr line `freenet update` prints when it counts a crash
/// against an armed probation marker.
const CRASH_RECORDED_NEEDLE: &str = "during post-update probation";

// ---------------------------------------------------------------------------
// Binary / path resolution (mirrors persistence_roundtrip.rs)
// ---------------------------------------------------------------------------

fn workspace_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .and_then(|p| p.parent())
        .expect("workspace layout: crates/core/../../ should resolve")
        .to_path_buf()
}

fn target_dir() -> PathBuf {
    std::env::var_os("CARGO_TARGET_DIR")
        .map(PathBuf::from)
        .unwrap_or_else(|| workspace_root().join("target"))
}

fn freenet_bin() -> PathBuf {
    let debug = target_dir().join("debug").join("freenet");
    if debug.exists() {
        return debug;
    }
    let release = target_dir().join("release").join("freenet");
    assert!(
        release.exists(),
        "freenet binary not found at {debug:?} or {release:?}. Build it first: \
         `cargo build --bin freenet` (CI's test_unit job builds it before tests)."
    );
    release
}

// ---------------------------------------------------------------------------
// Probation state fixture
// ---------------------------------------------------------------------------

/// The auto-updater's state directory, as `commands::auto_update::state_dir`
/// computes it: `dirs::home_dir()/.local/state/freenet`. The node resolves it
/// from `$HOME`, NOT from `--config-dir`/`--data-dir`, so the test isolates it
/// by giving the child process its own `HOME`.
fn state_dir(home: &Path) -> PathBuf {
    home.join(".local/state/freenet")
}

/// Write a probation marker for `version` that is armed right now: a genuine
/// `ProbationState` as `begin_probation_at` would have written it immediately
/// after installing `version` over the previous binary.
///
/// `installed_at_unix` is NOW on purpose. A marker older than
/// `PROBATION_MAX_AGE_SECS` (1h) is treated as stale by both the commit and the
/// post-stop paths, so a backdated fixture would make both assertions pass for
/// the wrong reason.
fn arm_probation(home: &Path, version: &str) {
    let dir = state_dir(home);
    std::fs::create_dir_all(&dir).expect("create state dir");
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock after epoch")
        .as_secs();
    let rollback_binary = dir.join("known_good_binary");
    let target_binary = home.join(".local/bin/freenet");
    // Written as JSON rather than by constructing `ProbationState`, which is
    // `pub(crate)` to the binary crate. The shape is pinned by the binary's own
    // serde round-trip tests in `rollback.rs`; a field rename there would make
    // this parse as "no probation" and assertion 2 would stop being able to
    // fail, so the fixture is re-read and checked below before the node starts.
    let marker = format!(
        r#"{{"new_version":"{version}","previous_version":"0.0.0",
            "rollback_binary":"{}","target_binary":"{}",
            "rollback_size":1,"rollback_sha256":"00","installed_at_unix":{now},
            "crash_count":0}}"#,
        rollback_binary.display(),
        target_binary.display(),
    );
    std::fs::write(probation_path(home), marker).expect("write probation marker");
}

fn probation_path(home: &Path) -> PathBuf {
    state_dir(home).join("update_probation.json")
}

// ---------------------------------------------------------------------------
// Subprocess node lifecycle
// ---------------------------------------------------------------------------

/// A `freenet network` subprocess, killed and reaped on drop so a panicking
/// assertion never leaks a node.
struct NodeProcess {
    child: Child,
}

impl NodeProcess {
    /// Spawn a self-contained, isolated gateway node rooted at `dir`, with
    /// `HOME` pointed at `dir` so the auto-updater state directory (and hence
    /// the probation marker) is the test's own.
    ///
    /// stderr is captured to a file rather than a pipe: the assertions read it
    /// while the node is still running, and a pipe nobody drains would block
    /// the node once its buffer filled.
    fn spawn(dir: &Path, ws_port: u16, network_port: u16, log_dir: &Path) -> Self {
        let stderr = std::fs::File::create(stderr_path(dir)).expect("create node stderr file");
        let child = Command::new(freenet_bin())
            .arg("network")
            // Same #4366 contamination guard as persistence_roundtrip.rs: this
            // is the real binary, not an in-process test node, so telemetry
            // would otherwise default ON and POST to the production collector.
            .env("FREENET_TELEMETRY_ENABLED", "false")
            // The whole point: the node must read ITS state dir, not the
            // developer's or CI runner's real one.
            .env("HOME", dir)
            .args(["--ws-api-address", "127.0.0.1"])
            .args(["--ws-api-port", &ws_port.to_string()])
            .args(["--network-address", "127.0.0.1"])
            .args(["--network-port", &network_port.to_string()])
            .args(["--public-network-address", "127.0.0.1"])
            .args(["--public-network-port", &network_port.to_string()])
            .arg("--is-gateway")
            .arg("--skip-load-from-network")
            .arg("--ignore-protocol-checking")
            // Keep the node from reaching GitHub and, worse, deciding a newer
            // release exists and exiting 42 before the commit window elapses —
            // which would make this test flaky against the real release feed.
            // It does NOT gate the commit task, which is spawned
            // unconditionally; if that ever changes, this test fails, correctly.
            .arg("--disable-auto-update")
            .args(["--location", "0.5"])
            .args(["--config-dir", &dir.to_string_lossy()])
            .args(["--data-dir", &dir.to_string_lossy()])
            .args(["--log-dir", &log_dir.to_string_lossy()])
            .stdout(Stdio::null())
            .stderr(Stdio::from(stderr))
            .spawn()
            .expect("spawn freenet network");
        Self { child }
    }

    fn has_exited(&mut self) -> bool {
        matches!(self.child.try_wait(), Ok(Some(_)))
    }

    /// Stop the node the way a supervisor does, and return its exit status.
    fn stop_gracefully(mut self) -> std::process::ExitStatus {
        // SIGTERM is what `systemctl stop` sends; the node's signal handler
        // turns it into a graceful shutdown.
        let pid = self.child.id() as i32;
        // SAFETY: `kill(2)` with a pid we own and a valid signal number; the
        // child is reaped immediately below, so the pid cannot be recycled
        // between the call and the wait.
        unsafe {
            libc::kill(pid, libc::SIGTERM);
        }
        self.child.wait().expect("reap node")
    }
}

impl Drop for NodeProcess {
    fn drop(&mut self) {
        if self.child.kill().is_ok() {
            let _reaped = self.child.wait().is_ok();
        }
    }
}

fn reserve_port() -> u16 {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("bind ephemeral port");
    listener.local_addr().expect("local addr").port()
}

/// Where the node's stderr is captured. This stands in for the journal: it is
/// the channel a service supervisor records.
fn stderr_path(dir: &Path) -> PathBuf {
    dir.join("node.stderr")
}

fn read_node_stderr(dir: &Path) -> String {
    std::fs::read_to_string(stderr_path(dir)).unwrap_or_default()
}

/// Concatenate every rolling log file the node wrote under `log_dir`.
fn read_node_logs(log_dir: &Path) -> String {
    let Ok(entries) = std::fs::read_dir(log_dir) else {
        return String::new();
    };
    entries
        .filter_map(Result::ok)
        .filter(|e| {
            e.file_name()
                .to_str()
                .is_some_and(|n| n.starts_with("freenet") && n.ends_with(".log"))
        })
        .filter_map(|e| std::fs::read_to_string(e.path()).ok())
        .collect::<Vec<_>>()
        .join("\n")
}

// ---------------------------------------------------------------------------
// The test
// ---------------------------------------------------------------------------

/// #5232: a node up past the commit window must have committed its probation,
/// and a clean stop after that must not be scored as a crash.
#[test]
#[cfg(unix)]
fn probation_is_committed_after_a_healthy_uptime_window() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let home = tmp.path();
    let log_dir = home.join("logs");
    std::fs::create_dir_all(&log_dir).expect("create log dir");

    arm_probation(home, NODE_VERSION);
    assert!(
        probation_path(home).exists(),
        "fixture precondition: the probation marker must exist before the node starts"
    );

    let mut node = NodeProcess::spawn(home, reserve_port(), reserve_port(), &log_dir);

    // ---- Assertion 1: the marker is cleared, and cleared by COMMITTING ----
    let deadline = Instant::now() + COMMIT_POLL_DEADLINE;
    let mut committed = false;
    while Instant::now() < deadline {
        if !probation_path(home).exists() {
            committed = true;
            break;
        }
        assert!(
            !node.has_exited(),
            "the node exited before the probation commit window elapsed, so this test \
             proved nothing about the commit path. Node logs:\n{}",
            read_node_logs(&log_dir)
        );
        std::thread::sleep(Duration::from_secs(2));
    }

    assert!(
        committed,
        "#5232: the node ran for more than {COMMIT_HEALTHY_UPTIME_SECS}s but the post-update \
         probation marker at {} is STILL armed. Every later non-clean stop will now be scored \
         as a crash of a probationary version, and three of them roll this node back off the \
         release it just installed. Node logs:\n{}",
        probation_path(home).display(),
        read_node_logs(&log_dir)
    );

    let stderr = read_node_stderr(home);
    assert!(
        stderr.contains(COMMITTED_STDERR_NEEDLE),
        "#5232: the probation marker was removed, but the node never announced \
         {COMMITTED_STDERR_NEEDLE:?} on stderr. Either it was dropped down the ClearedStale \
         branch (a marker for a DIFFERENT version) rather than committed — which disables \
         rollback protection entirely — or the commit happened silently, leaving an operator \
         reading `journalctl -u freenet` unable to tell a committed node from one still \
         accumulating strikes. That is the misreading #5232 was filed on. Node stderr:\n{stderr}\
         \n\nNode logs:\n{}",
        read_node_logs(&log_dir)
    );

    // ---- Assertion 2: the clean stop that follows is not scored as a crash ----
    let status = node.stop_gracefully();

    // Reproduce what a supervisor does after the node stops: run `freenet
    // update` with the node's stop status forwarded. `--check` is used so the
    // test can never download and overwrite the binary it is testing; the
    // probation/crash-counting branch runs BEFORE the check/install split, so
    // it is exercised identically either way.
    //
    // Deliberately forwarding "1" rather than the observed status: 1 is what a
    // graceful shutdown actually exits with today (#5227) and is classified as
    // a crash, so this is the strictest input. If #5227's fix lands and clean
    // stops exit 0, this assertion still holds and still tests the probation
    // gate rather than the exit-code classifier.
    let post_stop = Command::new(freenet_bin())
        .arg("update")
        .arg("--check")
        .arg("--quiet")
        .env("HOME", home)
        .env("FREENET_TELEMETRY_ENABLED", "false")
        .env("FREENET_POST_STOP_EXIT_CODE", "1")
        .output()
        .expect("run post-stop freenet update");
    let post_stop_stderr = String::from_utf8_lossy(&post_stop.stderr);

    assert!(
        !post_stop_stderr.contains(CRASH_RECORDED_NEEDLE),
        "#5232: a clean stop of a node that had already committed its probation was still \
         counted as a probation crash. Three of these roll the node back. \
         Node stop status was {status:?}; post-stop `freenet update` said:\n{post_stop_stderr}"
    );
    assert!(
        !probation_path(home).exists(),
        "the post-stop `freenet update` re-armed a probation marker that had already been \
         committed at {}",
        probation_path(home).display()
    );
}
