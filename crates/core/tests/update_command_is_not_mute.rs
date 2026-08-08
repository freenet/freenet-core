//! Guards for #5244: the `freenet update` process must not be mute.
//!
//! `set_logger` is installed only on the node path, so for a long time the
//! `freenet update` process ran with **no tracing subscriber at all** and every
//! `tracing::warn!` / `error!` in the installer was a no-op. The supervisor runs
//! it as `freenet update --quiet`, so sites whose only output was a `warn!` plus
//! a `!quiet`-gated `eprintln!` produced nothing at all in production —
//! including "installed the update but could not arm crash-loop rollback",
//! which is #4073's brick-safety machinery reporting that it is off.
//!
//! Two things need holding, and they fail in different ways, so they are tested
//! separately:
//!
//! 1. A subscriber exists in that process **and writes to stderr**. Tested by
//!    running the real binary, because a subscriber is process-global state that
//!    an in-process test cannot observe (the harness installs its own).
//! 2. It is wired via `set_cli_logger`, not via `set_logger` with a log dir.
//!    That distinction is invisible to the runtime tests but decides whether the
//!    output reaches the journal at all — see the pin's own docs.

#![cfg(unix)]

use std::path::PathBuf;
use std::process::Command;

fn workspace_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .and_then(|p| p.parent())
        .expect("workspace layout: crates/core/../../ should resolve")
        .to_path_buf()
}

fn freenet_bin() -> PathBuf {
    let target = std::env::var_os("CARGO_TARGET_DIR")
        .map(PathBuf::from)
        .unwrap_or_else(|| workspace_root().join("target"));
    let debug = target.join("debug").join("freenet");
    if debug.exists() {
        return debug;
    }
    let release = target.join("release").join("freenet");
    assert!(
        release.exists(),
        "freenet binary not found at {debug:?} or {release:?}. Build it first: \
         `cargo build --bin freenet`."
    );
    release
}

/// The update process must emit `tracing` output, and it must go to stderr.
///
/// Runs with `RUST_LOG=trace` so the assertion does not depend on the command
/// happening to log a warning: at TRACE the HTTP stack alone produces output on
/// any outcome, including an offline one (connection attempts are traced), so
/// this is not a network-dependent test even though `--check` does try to reach
/// GitHub. `--check` can never install, so it cannot overwrite the binary under
/// test.
///
/// Before the fix this produced zero bytes on both streams no matter what
/// `RUST_LOG` said, because there was no subscriber to read it.
#[test]
fn the_update_process_emits_tracing_to_stderr() {
    let home = tempfile::tempdir().expect("tempdir");

    let out = Command::new(freenet_bin())
        .args(["update", "--check", "--quiet"])
        .env("HOME", home.path())
        .env("RUST_LOG", "trace")
        .env("FREENET_TELEMETRY_ENABLED", "false")
        .output()
        .expect("run freenet update --check");

    let stderr = String::from_utf8_lossy(&out.stderr);
    let stdout = String::from_utf8_lossy(&out.stdout);

    assert!(
        !stderr.trim().is_empty(),
        "#5244: `freenet update` produced no tracing output at RUST_LOG=trace, so it has no \
         subscriber installed and every warn!/error! in the installer is a no-op — including \
         the ones that report crash-loop rollback failing to arm.\nstdout was:\n{stdout}"
    );
    assert!(
        stderr.contains("TRACE") || stderr.contains("DEBUG") || stderr.contains("INFO"),
        "stderr had content but none of it looks like a tracing line; is something else \
         writing here?\nstderr:\n{stderr}"
    );
    // Diagnostics on stderr keeps the command's human-facing `println!` output
    // on stdout parseable, and systemd records both.
    assert!(
        stdout.trim().is_empty(),
        "tracing output must not land on stdout, which is the command's own output stream:\n\
         {stdout}"
    );
}

/// At its default level the update process stays quiet, so installing a
/// subscriber does not turn every `ExecStopPost` run of a crash-looping node
/// into a wall of journal noise.
#[test]
fn the_update_process_is_quiet_when_nothing_is_wrong() {
    let home = tempfile::tempdir().expect("tempdir");

    let out = Command::new(freenet_bin())
        .args(["update", "--check", "--quiet"])
        .env("HOME", home.path())
        .env_remove("RUST_LOG")
        .env("FREENET_TELEMETRY_ENABLED", "false")
        .output()
        .expect("run freenet update --check");

    let stderr = String::from_utf8_lossy(&out.stderr);
    assert!(
        stderr.trim().is_empty(),
        "the default level must be WARN, not INFO: this runs on every non-clean stop, so an \
         INFO default would flood the journal of exactly the crash-looping node whose journal \
         we need to read.\nstderr:\n{stderr}"
    );
}

/// The `Update` arm must install the CLI logger, and must NOT reach for
/// `set_logger` with a log directory.
///
/// This is the trap that makes a fix here look done while changing nothing:
/// `set_logger(None, None, Some(log_dir))` — the natural copy-paste from
/// `run_node` — sets `use_file_logging`, which routes everything into the
/// rolling log files. systemd captures stdout/stderr, NOT those files. Every
/// `warn!` would start "working" and the journal would stay exactly as blind as
/// it was, which is the same asymmetry that caused #5232 to be misdiagnosed.
///
/// The runtime tests above cannot see that difference: with a log dir they
/// would still find output on stderr in a terminal, because `init_tracer` adds
/// a console layer when stdout is a TTY. Only the wiring tells them apart.
///
/// Scraped ACROSS files on purpose — this test lives in `tests/` and reads
/// `src/bin/freenet.rs`, so it can never be satisfied by its own assertion
/// strings, which is the self-matching failure mode documented in
/// `.claude/rules/bug-prevention-patterns.md`.
#[test]
fn the_update_arm_installs_the_cli_logger_and_not_a_file_logger() {
    let src = std::fs::read_to_string(workspace_root().join("crates/core/src/bin/freenet.rs"))
        .expect("read bin/freenet.rs");

    let arm_start = src
        .find("Some(Command::Update(cmd)) =>")
        .expect("the Update dispatch arm must still exist");
    let arm = &src[arm_start..];
    let arm_end = arm
        .find("Some(Command::Uninstall(cmd))")
        .expect("the Uninstall arm must still follow Update; re-anchor this pin if it moved");
    let arm = &arm[..arm_end];

    assert!(
        arm.contains("set_cli_logger"),
        "#5244: the Update arm must install a subscriber, or every warn!/error! in the \
         installer is a no-op. Arm body:\n{arm}"
    );
    assert!(
        !arm.contains("set_logger("),
        "the Update arm must use `set_cli_logger`, NOT `set_logger`: the latter's log-dir \
         argument routes output into the rolling log files, which systemd does not capture, so \
         the journal would stay exactly as blind while the fix looked done. Arm body:\n{arm}"
    );
}
