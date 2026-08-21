//! End-to-end guard for the post-update probation lifecycle (#5232, #4073).
//!
//! The crash-loop auto-rollback machinery (`commands::rollback`) arms a
//! probation marker when a new version is installed. The node clears it once it
//! has run healthily for `COMMIT_HEALTHY_UPTIME_SECS` (60s); until then, each
//! non-clean stop counts a crash, and three of them roll the node BACK off the
//! release it just installed and pin that release as known-bad. #5232 was filed
//! believing the commit half never happened.
//!
//! ## Why these tests spawn the real binary
//!
//! The commit is not a property of `commit_probation_at` — that function is
//! already unit-tested and works. It is a property of its ONLY caller: a
//! `GlobalExecutor::spawn` in `bin/freenet.rs`'s `run_network_node_with_signals`
//! whose handle nothing awaits or monitors (it is retained only to `.abort()` it
//! at shutdown). A unit test on `commit_probation_at` passes happily while that
//! task never runs, never fires, or commits a version string that does not match
//! the marker the installer wrote. Nothing short of running the node and
//! watching the marker covers that seam, and `commands::rollback` lives in the
//! bin crate behind a `$HOME`-derived state dir, so no in-process harness
//! (`#[freenet_test]`, `SimNetwork`) can reach it either.
//!
//! ## The two halves, and why both are here
//!
//! * `probation_is_committed_after_a_healthy_uptime_window` — up past the
//!   window ⇒ marker gone, commit announced, and a following post-stop
//!   `freenet update` records NO crash.
//! * `probation_survives_a_stop_inside_the_commit_window` — stopped INSIDE the
//!   window ⇒ the marker is retained, and a crash of that still-uncommitted
//!   version counts `1/3`. Two separable claims, and the test asserts both: the
//!   retention is from a real graceful stop, the counting from a forwarded crash
//!   status (see `post_stop_update`).
//!
//! The second is not decoration. It is the positive control for the first: it
//! proves the fixture actually parses as a `ProbationState`, that
//! `FREENET_POST_STOP_EXIT_CODE` is spelled right, that `HOME` really relocates
//! the state dir, and that the crash line is producible in this harness at all.
//! Without it, the first test's "no crash was recorded" would also hold if the
//! post-stop command failed for some entirely unrelated reason — an assertion
//! that cannot fail. It is also the half the user-visible rollback ran through:
//! three stops landing inside the window, before anything could commit. (What
//! made those stops COUNT was #5227 — a graceful shutdown exited 1, which
//! classifies as a crash. Since #5230 it exits 0, so ordinary restarts no longer
//! accumulate strikes; the retention behaviour this test pins is what still
//! decides the outcome for a genuinely crashing release.) It costs ~15s rather
//! than ~65s because it never waits out the window.
//!
//! ## Why the commit assertion reads stderr, not the log files
//!
//! Under the shipped systemd deployment `tracing` output goes to the rolling log
//! files under the log dir and only stdout/stderr reach the journal, so the
//! crash-count and rollback lines (printed by the installer on stderr) were in
//! the journal while the commit that disarms them was not. #5232 was filed
//! reading a journal that showed strikes accumulating and no commit ever
//! happening. Asserting the log file would pass on a build where the journal is
//! silent again, i.e. it would not hold the fix. `crates/core/src/bin/commands/
//! rollback.rs::commit_announcement` carries the full rationale.
//!
//! Note that CI sets `RUST_LOG=error`, which filters the `tracing::info!` out
//! entirely — another reason the log files are the wrong thing to assert on.

// Unix-only: both tests stop the node with SIGTERM, which is what a supervisor
// sends and what makes the stop a *graceful* one worth asserting on. Windows has
// no equivalent that exercises the same node path.
#![cfg(unix)]

use std::path::{Path, PathBuf};
use std::process::{Child, Command, ExitStatus, Stdio};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

/// The version the node under test reports. `build_info::VERSION` in the binary
/// is `env!("CARGO_PKG_VERSION")` of this very crate, so the marker we plant
/// carries the exact string the node will try to commit.
const NODE_VERSION: &str = env!("CARGO_PKG_VERSION");

/// Mirrors `commands::rollback::COMMIT_HEALTHY_UPTIME_SECS`, which is
/// `pub(crate)` in the binary crate and so not importable here. Raising the real
/// constant past the deadline below fails these tests rather than passing
/// silently — the correct outcome, since lengthening the window is a real
/// behavioural change to the rollback machinery.
const COMMIT_HEALTHY_UPTIME_SECS: u64 = 60;

/// How long to wait for the commit AFTER the node is up (not after spawn):
/// readiness is waited for separately, so this budget covers only the window
/// itself plus scheduling slack.
const COMMIT_POLL_DEADLINE: Duration = Duration::from_secs(COMMIT_HEALTHY_UPTIME_SECS + 45);

/// How long a cold debug-build node may take to bind its WS port. Matches the
/// budget `persistence_roundtrip.rs` allows for the same thing.
const NODE_READY_DEADLINE: Duration = Duration::from_secs(60);

/// Substring of the line `commit_probation` announces on the `Committed` branch
/// ONLY.
///
/// Deliberately not the leading "post-update probation passed" phrase: the
/// ClearFailed branch — marker could not be removed, so rollback is still armed
/// — needs to say the version ran healthily too, and a needle both lines shared
/// would let this test report a commit while rollback was live.
/// `rollback.rs::tests::a_failed_clear_is_never_announced_as_a_pass` pins the
/// two apart in microseconds, rather than after this test's 60s window.
const COMMITTED_STDERR_NEEDLE: &str = "committed, auto-rollback disarmed";

/// Substring of the line `freenet update` prints when it counts a crash against
/// an armed probation marker. Asserted PRESENT by the in-window test and ABSENT
/// by the committed test — so a reword breaks the former loudly instead of
/// silently disarming the latter.
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

/// Fail early and unambiguously if the binary on disk is not the one this test
/// source describes. A stale `target/debug/freenet` from before a version bump
/// would take the ClearedStale branch instead of committing, and the failure
/// would read as a rollback regression rather than "rebuild your binary".
fn assert_binary_matches_crate_version() {
    let out = Command::new(freenet_bin())
        .arg("--version")
        .output()
        .expect("run freenet --version");
    let reported = String::from_utf8_lossy(&out.stdout);
    assert!(
        reported.contains(NODE_VERSION),
        "the freenet binary at {} reports {reported:?}, which does not contain this crate's \
         version {NODE_VERSION}. Rebuild it (`cargo build --bin freenet`) — a stale binary makes \
         these tests fail in a way that looks like a rollback bug.",
        freenet_bin().display()
    );
}

// ---------------------------------------------------------------------------
// Probation state fixture
// ---------------------------------------------------------------------------

/// The auto-updater's state directory, as `commands::auto_update::state_dir`
/// computes it: `dirs::home_dir()/.local/state/freenet`. The node resolves it
/// from `$HOME`, NOT from `--config-dir`/`--data-dir`, so the tests isolate it by
/// giving the child process its own `HOME`.
fn state_dir(home: &Path) -> PathBuf {
    home.join(".local/state/freenet")
}

fn probation_path(home: &Path) -> PathBuf {
    state_dir(home).join("update_probation.json")
}

/// Write a probation marker for `version`, armed right now — a `ProbationState`
/// as `begin_probation_at` would have written it immediately after installing
/// `version` over the previous binary.
///
/// `installed_at_unix` is NOW on purpose: a marker older than
/// `PROBATION_MAX_AGE_SECS` (1h) is treated as stale by both the commit and the
/// post-stop paths, so a backdated fixture would make the assertions pass for
/// the wrong reason.
///
/// This is hand-written JSON because `ProbationState` is `pub(crate)` to the bin
/// crate and cannot be constructed here. If a field is renamed in `rollback.rs`,
/// `read_probation_at` returns `None` and the node behaves as if there were no
/// probation at all. Nothing silently passes — but the failure surfaces in
/// `probation_survives_a_stop_inside_the_commit_window`, which takes ~15s and
/// reports "no crash was recorded", rather than in the 65s test. That is the
/// intended ordering: the cheap test is the fixture's canary.
fn arm_probation(home: &Path, version: &str) {
    let dir = state_dir(home);
    std::fs::create_dir_all(&dir).expect("create state dir");
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock after epoch")
        .as_secs();
    let marker = serde_json::json!({
        "new_version": version,
        "previous_version": "0.0.0",
        "rollback_binary": dir.join("known_good_binary"),
        "target_binary": home.join(".local/bin/freenet"),
        "rollback_size": 1,
        "rollback_sha256": "00",
        "installed_at_unix": now,
        "crash_count": 0,
    });
    std::fs::write(
        probation_path(home),
        serde_json::to_vec(&marker).expect("serialize probation fixture"),
    )
    .expect("write probation marker");
}

/// The marker's `crash_count`, or `None` if there is no marker.
fn probation_crash_count(home: &Path) -> Option<u64> {
    let raw = std::fs::read_to_string(probation_path(home)).ok()?;
    let parsed: serde_json::Value =
        serde_json::from_str(&raw).expect("marker must stay valid JSON");
    Some(
        parsed["crash_count"]
            .as_u64()
            .expect("marker must carry a numeric crash_count"),
    )
}

// ---------------------------------------------------------------------------
// Subprocess node lifecycle
// ---------------------------------------------------------------------------

/// Where the node's stderr is captured. This stands in for the journal: it is
/// the channel a service supervisor records.
fn stderr_path(dir: &Path) -> PathBuf {
    dir.join("node.stderr")
}

fn read_node_stderr(dir: &Path) -> String {
    std::fs::read_to_string(stderr_path(dir)).unwrap_or_default()
}

/// A `freenet network` subprocess, killed and reaped on drop so a panicking
/// assertion never leaks a node.
struct NodeProcess {
    child: Child,
    ws_port: u16,
}

impl NodeProcess {
    /// Spawn a self-contained, isolated gateway node rooted at `dir`, with
    /// `HOME` pointed at `dir` so the auto-updater state directory (and hence
    /// the probation marker) is the test's own.
    ///
    /// stderr goes to a file rather than a pipe: the assertions read it while
    /// the node is still running, and a pipe nobody drains would block the node
    /// once its buffer filled.
    fn spawn(dir: &Path, ws_port: u16, network_port: u16) -> Self {
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
            // Keeps the node from reaching GitHub and, worse, deciding a newer
            // release exists and exiting 42 before the window elapses — which
            // would make these tests flaky against the real release feed. It
            // does NOT gate the commit task, which is spawned unconditionally;
            // if that ever changes, these tests fail, correctly.
            .arg("--disable-auto-update")
            .args(["--location", "0.5"])
            .args(["--config-dir", &dir.to_string_lossy()])
            .args(["--data-dir", &dir.to_string_lossy()])
            // Deliberately the state dir, not a separate directory: on Linux the
            // log dir IS the auto-updater state dir in production, so the log
            // pruner and the probation marker share it. Pointing this elsewhere
            // would hide a pruner regression that started matching
            // `update_probation.json` (see `tracer.rs::rotating_log_family`).
            .args(["--log-dir", &state_dir(dir).to_string_lossy()])
            .stdout(Stdio::null())
            .stderr(Stdio::from(stderr))
            .spawn()
            .expect("spawn freenet network");
        Self { child, ws_port }
    }

    fn has_exited(&mut self) -> bool {
        matches!(self.child.try_wait(), Ok(Some(_)))
    }

    /// Block until the WS API accepts a TCP connection, so the commit window is
    /// timed from a node that is actually up rather than from `spawn`.
    fn wait_until_ready(&mut self, dir: &Path) {
        let deadline = Instant::now() + NODE_READY_DEADLINE;
        while Instant::now() < deadline {
            if std::net::TcpStream::connect(("127.0.0.1", self.ws_port)).is_ok() {
                return;
            }
            assert!(
                !self.has_exited(),
                "the node exited before its WS API came up.{}",
                self.diagnostics(dir)
            );
            std::thread::sleep(Duration::from_millis(500));
        }
        panic!(
            "the node's WS API never came up within {NODE_READY_DEADLINE:?}.{}",
            self.diagnostics(dir)
        );
    }

    /// Everything worth knowing when an assertion fails. The node's own stderr
    /// is included because the likeliest failures here — a port collision, a
    /// bind error, a rejected flag — report themselves there, possibly before
    /// `tracing` is even initialised.
    fn diagnostics(&mut self, dir: &Path) -> String {
        format!(
            "\n--- node exit status: {:?}\n--- node stderr:\n{}",
            self.child.try_wait(),
            read_node_stderr(dir)
        )
    }

    /// Stop the node the way a supervisor does, and return its exit status.
    /// Bounded: a shutdown hang is a real regression class here, and blocking
    /// forever would surface as an opaque harness timeout with no output.
    fn stop_gracefully(mut self, dir: &Path) -> ExitStatus {
        // SIGTERM is what `systemctl stop` sends; the node's signal handler
        // turns it into a graceful shutdown.
        let pid = self.child.id() as i32;
        // SAFETY: `kill(2)` with a pid we own and a valid signal number. The
        // child is only reaped below, so the pid cannot be recycled in between.
        unsafe {
            libc::kill(pid, libc::SIGTERM);
        }
        let deadline = Instant::now() + Duration::from_secs(60);
        while Instant::now() < deadline {
            if let Some(status) = self.child.try_wait().expect("poll node exit") {
                return status;
            }
            std::thread::sleep(Duration::from_millis(200));
        }
        let diagnostics = self.diagnostics(dir);
        let _kill = self.child.kill();
        let _reap = self.child.wait();
        panic!("the node did not exit within 60s of SIGTERM.{diagnostics}");
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

/// Run the post-stop `freenet update` a supervisor runs after the node stops,
/// and return its stderr.
///
/// `--check` is used so the tests can never download and overwrite the binary
/// they are testing; the probation/crash-counting branch runs BEFORE the
/// check/install split, so it is exercised identically either way.
///
/// The forwarded status is always `"1"`, never the node's observed status.
/// Since #5230 a graceful shutdown exits 0, and `classify_stop` short-circuits
/// `"0"` to `NotCrash` BEFORE `handle_post_stop_at` ever reads the marker — so
/// forwarding the real status would mean neither test touched the probation
/// logic at all, and the in-window test would lose its ability to fail. `"1"`
/// classifies as a crash, which keeps both tests aimed at the probation gate
/// rather than at the exit-code classifier that #5230 already owns.
fn post_stop_update(home: &Path) -> String {
    let out = Command::new(freenet_bin())
        .arg("update")
        .arg("--check")
        .arg("--quiet")
        .env("HOME", home)
        .env("FREENET_TELEMETRY_ENABLED", "false")
        .env("FREENET_POST_STOP_EXIT_CODE", "1")
        .output()
        .expect("run post-stop freenet update");
    String::from_utf8_lossy(&out.stderr).into_owned()
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

/// #5232: a node up past the commit window must have committed its probation —
/// visibly — and a clean stop after that must not be scored as a crash.
#[test]
fn probation_is_committed_after_a_healthy_uptime_window() {
    assert_binary_matches_crate_version();
    let tmp = tempfile::tempdir().expect("tempdir");
    let home = tmp.path();

    arm_probation(home, NODE_VERSION);
    assert_eq!(
        probation_crash_count(home),
        Some(0),
        "fixture precondition: an armed marker with no crashes yet"
    );

    let mut node = NodeProcess::spawn(home, reserve_port(), reserve_port());
    node.wait_until_ready(home);

    // ---- Assertion 1: committed, and said so ----
    //
    // Polls for the ANNOUNCEMENT rather than for the marker's absence.
    // `commit_probation_at` deletes the file before `commit_probation` prints,
    // so a poll on the file can observe "gone" microseconds before the line is
    // written — and the announcement is the specific artifact this guards.
    let deadline = Instant::now() + COMMIT_POLL_DEADLINE;
    let mut announced = false;
    while Instant::now() < deadline {
        if read_node_stderr(home).contains(COMMITTED_STDERR_NEEDLE) {
            announced = true;
            break;
        }
        assert!(
            !node.has_exited(),
            "the node exited before the probation commit window elapsed, so this test proved \
             nothing about the commit path.{}",
            node.diagnostics(home)
        );
        std::thread::sleep(Duration::from_secs(2));
    }

    let stderr = read_node_stderr(home);
    assert!(
        announced,
        "#5232: the node ran for more than {COMMIT_HEALTHY_UPTIME_SECS}s but never announced \
         {COMMITTED_STDERR_NEEDLE:?} on stderr. Either it did not commit — in which case the \
         marker is still armed, every later non-clean stop is scored as a crash of a \
         probationary version, and three of them roll this node back off the release it just \
         installed — or it committed silently, leaving an operator reading `journalctl -u \
         freenet` unable to tell a committed node from one accumulating strikes. That is the \
         misreading #5232 was filed on. Marker crash_count now: {:?}\n--- node stderr:\n{stderr}",
        probation_crash_count(home),
    );
    assert!(
        !stderr.contains("discarded a post-update probation marker"),
        "the marker was DISCARDED as belonging to another version rather than committed, so \
         rollback protection was dropped without the version ever proving itself. The planted \
         marker names {NODE_VERSION}, the same string the node commits, so this means the \
         version comparison regressed (the #5104 `v`-prefix class).\n--- node stderr:\n{stderr}"
    );
    assert_eq!(
        probation_crash_count(home),
        None,
        "the commit was announced but the marker is still on disk at {}",
        probation_path(home).display()
    );

    // ---- Assertion 2: the clean stop that follows is not scored as a crash ----
    let status = node.stop_gracefully(home);
    let post_stop_stderr = post_stop_update(home);
    assert!(
        !post_stop_stderr.contains(CRASH_RECORDED_NEEDLE),
        "#5232: once probation is committed, NOTHING may be counted against it — this forwards \
         a crash status (the strictest input) and it still must not register, because the marker \
         is gone. Three that do register roll the node back. Node stop status was {status:?}; \
         post-stop `freenet update` said:\n{post_stop_stderr}"
    );
    assert_eq!(
        probation_crash_count(home),
        None,
        "the post-stop `freenet update` re-armed a marker that had already been committed"
    );
}

/// The other half of the contract, and the positive control for the test above.
///
/// Two claims, both asserted here. First, a node stopped INSIDE the commit
/// window keeps its marker — that is what leaves a genuinely crash-looping
/// release rollback-eligible, and it is asserted against a REAL graceful stop.
/// Second, a crash of that still-uncommitted version is counted `1/3` and
/// persisted — asserted against a forwarded crash status, because since #5230 a
/// graceful stop is deliberately not a crash (see `post_stop_update`).
///
/// This is what makes the committed test's "no crash was recorded" meaningful.
/// That assertion is a negative on a subprocess's stderr, so on its own it would
/// also hold if the post-stop command never reached the probation branch at all
/// — a wrong env var name, a `HOME` that did not relocate the state dir, an
/// unparseable fixture, or a reworded needle. Here the same fixture, the same
/// env var and the same needle must produce a crash line, so all of it is
/// pinned. Runs in ~15s and makes no network call: the crash branch exits before
/// `freenet update` probes GitHub.
#[test]
fn probation_survives_a_stop_inside_the_commit_window() {
    assert_binary_matches_crate_version();
    let tmp = tempfile::tempdir().expect("tempdir");
    let home = tmp.path();

    arm_probation(home, NODE_VERSION);

    let mut node = NodeProcess::spawn(home, reserve_port(), reserve_port());
    node.wait_until_ready(home);
    // Stop well inside the window. `wait_until_ready` returns as soon as the WS
    // port answers, which is before the commit task's sleep elapses, so there is
    // no race to lose here — but assert it rather than trusting it.
    let stderr_at_stop = read_node_stderr(home);
    assert!(
        !stderr_at_stop.contains(COMMITTED_STDERR_NEEDLE),
        "the node committed before this test could stop it, so it is no longer testing an \
         in-window stop. Did COMMIT_HEALTHY_UPTIME_SECS shrink?\n{stderr_at_stop}"
    );
    let status = node.stop_gracefully(home);

    assert_eq!(
        probation_crash_count(home),
        Some(0),
        "a node stopped inside the commit window must keep its armed marker: that is what lets \
         a genuinely crash-looping release still be rolled back. This was a real graceful stop, \
         so it is the retention that is under test here, not the classification. Node stop \
         status: {status:?}"
    );

    let post_stop_stderr = post_stop_update(home);
    assert!(
        post_stop_stderr.contains(CRASH_RECORDED_NEEDLE),
        "a crash of an uncommitted probationary version must be counted, and reported on stderr \
         where the supervisor records it. If this fails, the committed test's \"no crash was \
         recorded\" assertion is no longer able to fail either — check the fixture parses, \
         `FREENET_POST_STOP_EXIT_CODE` is still the env var name, and the needle \
         {CRASH_RECORDED_NEEDLE:?} still matches what `freenet update` prints.\n\
         post-stop `freenet update` said:\n{post_stop_stderr}"
    );
    assert!(
        post_stop_stderr.contains("1/3"),
        "the first crash of a probationary version must count as 1 of 3, not something else:\n\
         {post_stop_stderr}"
    );
    assert_eq!(
        probation_crash_count(home),
        Some(1),
        "the crash must be PERSISTED, or crashes cannot accumulate toward the rollback threshold \
         across restarts and a genuinely bad release is never rolled back"
    );
}
