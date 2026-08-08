//! Guards for #5244: the `freenet update` process must not be mute.
//!
//! `set_logger` is installed only on the node path, so the `freenet update`
//! process ran with **no tracing subscriber at all** and every `tracing::warn!`
//! / `error!` in the installer was a no-op. The supervisor runs it as `freenet
//! update --quiet`, so sites whose only output was a `warn!` plus a
//! `!quiet`-gated `eprintln!` produced nothing at all in production — including
//! "installed the update but could not arm crash-loop rollback", which is
//! #4073's brick-safety machinery reporting that it is off.
//!
//! `crates/core/src/tracing/tracer.rs::init_cli_stderr_tracer` carries the
//! rationale for the shape of the fix, including the two traps that produce a
//! version of it that looks done and changes nothing.

#![cfg(unix)]

use std::path::{Path, PathBuf};
use std::process::Command;
use std::time::{SystemTime, UNIX_EPOCH};

/// The binary Cargo built for THIS test run.
///
/// Deliberately not a hand-rolled `target/debug/...` lookup: those prefer a
/// debug binary even under `--release` and silently assert against whatever was
/// left there by an earlier build. For a test whose entire job is to prove the
/// current binary is not mute, testing a stale one is the "verification that
/// cannot fail" failure mode. `tests/graceful_shutdown_exit_code.rs` uses this
/// same env var for the same reason.
const FREENET_BIN: &str = env!("CARGO_BIN_EXE_freenet");

fn workspace_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .and_then(|p| p.parent())
        .expect("workspace layout: crates/core/../../ should resolve")
        .to_path_buf()
}

/// Put the installer's GitHub cooldown far enough in the future that
/// `probe_latest_tag` defers, and near enough that it is not discarded as
/// implausible.
///
/// This is what makes the test below deterministic and offline. Without it,
/// `update --check` reaches api.github.com, so the test would depend on the
/// network being up AND GitHub not rate-limiting a shared CI egress IP — and it
/// would spend real quota on every run, which #5102 went to some trouble to
/// conserve. With it, the command takes the cooldown branch, emits exactly one
/// known WARN, and exits before any HTTP.
fn arm_github_cooldown(home: &Path) {
    let dir = home.join(".local/state/freenet");
    std::fs::create_dir_all(&dir).expect("create state dir");
    let until = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("clock after epoch")
        .as_secs()
        + 600;
    std::fs::write(dir.join("github_ratelimit_cooldown"), until.to_string())
        .expect("write cooldown");
}

fn run_update_check(home: &Path, rust_log: Option<&str>) -> (String, String) {
    let mut cmd = Command::new(FREENET_BIN);
    cmd.args(["update", "--check", "--quiet"])
        .env("HOME", home)
        .env("FREENET_TELEMETRY_ENABLED", "false")
        // A developer with any of these set must not get a vacuous pass.
        .env_remove("FREENET_DISABLE_LOGS")
        .env_remove("FREENET_LOG_FORMAT")
        .env_remove("FREENET_LOG_TO_STDERR")
        .env_remove("FREENET_POST_STOP_EXIT_CODE");
    match rust_log {
        Some(v) => cmd.env("RUST_LOG", v),
        None => cmd.env_remove("RUST_LOG"),
    };
    let out = cmd.output().expect("run freenet update --check");
    (
        String::from_utf8_lossy(&out.stdout).into_owned(),
        String::from_utf8_lossy(&out.stderr).into_owned(),
    )
}

/// A WARN from the update process must reach stderr.
///
/// The cooldown fixture guarantees exactly one WARN (`"Update check deferred"`,
/// `commands/update.rs`) on a path that makes no network call. Asserting on a
/// WARN rather than on TRACE matters: release builds set
/// `release_max_level_info`, which compiles `debug!`/`trace!` out entirely, so a
/// TRACE-based assertion would pass in CI's debug build and fail in the build we
/// actually ship.
///
/// Before the fix this produced zero bytes on both streams — there was no
/// subscriber to read the event.
#[test]
fn a_warning_from_the_update_process_reaches_stderr() {
    let home = tempfile::tempdir().expect("tempdir");
    arm_github_cooldown(home.path());

    let (stdout, stderr) = run_update_check(home.path(), None);

    assert!(
        stderr.contains("Update check deferred"),
        "#5244: the update process emitted no WARN, so it has no subscriber installed and every \
         warn!/error! in the installer is a no-op — including the ones reporting that crash-loop \
         rollback failed to arm.\nstderr was:\n{stderr}\nstdout was:\n{stdout}"
    );
    assert!(
        stderr.contains("WARN"),
        "the line reached stderr but without a level, so it came from somewhere other than the \
         subscriber:\n{stderr}"
    );
    // Diagnostics on stderr keeps the command's own output stream clean, and
    // systemd records both.
    assert!(
        stdout.trim().is_empty(),
        "tracing output must not land on stdout:\n{stdout}"
    );
}

/// The subscriber must not colour its output when stderr is not a terminal.
///
/// journald stores what it is given byte for byte, so ANSI escapes would end up
/// in the journal this change exists to reach. `Command::output` gives the child
/// a pipe, never a tty, so this is the non-interactive case.
#[test]
fn the_update_process_does_not_write_ansi_escapes_to_a_pipe() {
    let home = tempfile::tempdir().expect("tempdir");
    arm_github_cooldown(home.path());

    let (_stdout, stderr) = run_update_check(home.path(), None);

    assert!(
        !stderr.contains('\u{1b}'),
        "ANSI escape sequences must not reach a non-terminal stderr — they would be stored \
         verbatim in the journal:\n{stderr:?}"
    );
}

/// The `Update` arm must install the CLI logger, at WARN, and must NOT reach for
/// `set_logger`.
///
/// Three separate regressions, none of which the runtime tests above can see:
///
/// * No subscriber — they would catch that, but this fails faster and says why.
/// * `set_logger(None, None, Some(log_dir))`, the natural copy-paste from
///   `run_node`, sets `use_file_logging` and routes everything into the rolling
///   log files. systemd captures stdout/stderr, NOT those files — the asymmetry
///   that caused #5232 to be misdiagnosed. A terminal would still show output,
///   so the runtime tests would stay green while the journal stayed blind.
/// * A promotion to INFO or DEBUG. Nothing on the `--check` path logs at INFO
///   today, so a runtime test asserting "quiet at the default level" would pass
///   under that mutation and prove nothing. The level is pinned here, where it
///   is a fact about the code rather than about which paths happen to log.
///
/// Scraped ACROSS files (test in `tests/`, source in `src/bin/`) so it can never
/// be satisfied by its own assertion strings, and with comments stripped so it
/// cannot be satisfied by a comment ABOUT the call — both failure modes are
/// documented in `.claude/rules/bug-prevention-patterns.md`.
#[test]
fn the_update_arm_installs_the_cli_logger_at_warn_and_not_a_file_logger() {
    let src = std::fs::read_to_string(workspace_root().join("crates/core/src/bin/freenet.rs"))
        .expect("read bin/freenet.rs");

    let arm_start = src
        .find("Some(Command::Update(cmd)) =>")
        .expect("the Update dispatch arm must still exist");
    let arm = &src[arm_start..];
    let arm_end = arm
        .find("Some(Command::Uninstall(cmd))")
        .expect("the Uninstall arm must still follow Update; re-anchor this pin if it moved");
    let code: String = arm[..arm_end]
        .lines()
        .map(|l| match l.find("//") {
            Some(i) => &l[..i],
            None => l,
        })
        .collect::<Vec<_>>()
        .join("\n");

    assert!(
        code.contains("set_cli_logger"),
        "#5244: the Update arm must install a subscriber, or every warn!/error! in the \
         installer is a no-op. Arm code (comments stripped):\n{code}"
    );
    assert!(
        !code.contains("set_logger("),
        "the Update arm must use `set_cli_logger`, NOT `set_logger`: the latter's log-dir \
         argument routes output into the rolling log files, which systemd does not capture, so \
         the journal would stay exactly as blind while the fix looked done. Arm code:\n{code}"
    );
    assert!(
        code.contains("LevelFilter::WARN"),
        "the level must stay WARN: this runs on every non-clean stop, so INFO would flood the \
         journal of exactly the crash-looping node whose journal we need to read. Arm code:\n\
         {code}"
    );
}
