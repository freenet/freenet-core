//! System service management for Freenet.
//!
//! Supports:
//! - Linux: systemd user service (default) or system-wide service (--system)
//! - macOS: launchd user agent
//!
//! # Module layout
//!
//! This module is split into focused submodules:
//!
//! - `log_utils` – log file rotation helpers
//! - `single_instance` – macOS wrapper single-instance lock (flock-based)
//! - `launch_at_login` – macOS Launch at Login, plist helpers, legacy migration
//! - `wrapper` – process wrapper loop, state machine, log events
//! - `purge` – data purge, doctor, process-reaping
//! - `linux` – Linux/systemd service management
//! - `macos` – macOS/launchd service management
//! - `windows` – Windows registry/task service management

mod launch_at_login;
mod linux;
mod log_utils;
mod macos;
mod purge;
mod single_instance;
mod windows;
pub mod wrapper;

use anyhow::Result;
use clap::Subcommand;
use freenet::config::ConfigPaths;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use super::report::ReportCommand;

#[derive(Subcommand, Debug, Clone)]
pub enum ServiceCommand {
    /// Install Freenet as a system service
    Install {
        /// Install as a system-wide service instead of a user service.
        /// Requires root. Use this for containers (LXC/Docker), headless
        /// servers, or environments without a user session bus.
        #[arg(long)]
        system: bool,
    },
    /// Uninstall the Freenet system service
    Uninstall {
        /// Uninstall the system-wide service instead of the user service
        #[arg(long)]
        system: bool,
        /// Also remove all Freenet data, config, and logs
        #[arg(long, conflicts_with = "keep_data")]
        purge: bool,
        /// Only remove the service (skip interactive prompt)
        #[arg(long, conflicts_with = "purge")]
        keep_data: bool,
    },
    /// Check the status of the Freenet service
    Status {
        /// Check the system-wide service instead of the user service
        #[arg(long)]
        system: bool,
    },
    /// Start the Freenet service
    Start {
        /// Start the system-wide service instead of the user service
        #[arg(long)]
        system: bool,
    },
    /// Stop the Freenet service
    Stop {
        /// Stop the system-wide service instead of the user service
        #[arg(long)]
        system: bool,
    },
    /// Restart the Freenet service
    Restart {
        /// Restart the system-wide service instead of the user service
        #[arg(long)]
        system: bool,
    },
    /// Recover a wedged service install: re-template the wrapper/unit to the
    /// current binary, reap stale orphaned `freenet network` processes (PPID=1,
    /// holding the port on an old binary), and restart cleanly. Use when the
    /// node appears frozen on an old version (see issue #3967). Unlike
    /// `restart`, this kills detached orphans and refreshes the wrapper, so it
    /// closes the bootstrap gap that `restart` alone cannot.
    Doctor {
        /// Repair the system-wide service instead of the user service
        #[arg(long)]
        system: bool,
    },
    /// View service logs (follows new output)
    Logs {
        /// Show only error logs
        #[arg(long)]
        err: bool,
    },
    /// Generate and upload a diagnostic report for debugging
    Report(ReportCommand),
    /// Internal: process wrapper that manages the freenet node lifecycle.
    /// Handles auto-update (exit code 42), crash backoff, log capture,
    /// and on Windows shows a system tray icon.
    #[command(hide = true)]
    RunWrapper,
}

impl ServiceCommand {
    pub fn run(
        &self,
        version: &str,
        git_commit: &str,
        git_dirty: &str,
        build_timestamp: &str,
        config_dirs: Arc<ConfigPaths>,
    ) -> Result<()> {
        match self {
            ServiceCommand::Install { system } => install_service(*system),
            ServiceCommand::Uninstall {
                system,
                purge,
                keep_data,
            } => uninstall_service(*system, *purge, *keep_data),
            ServiceCommand::Status { system } => service_status(*system),
            ServiceCommand::Start { system } => start_service(*system),
            ServiceCommand::Stop { system } => stop_service(*system),
            ServiceCommand::Restart { system } => restart_service(*system),
            ServiceCommand::Doctor { system } => service_doctor(*system),
            ServiceCommand::Logs { err } => service_logs(*err),
            ServiceCommand::Report(cmd) => {
                cmd.run(version, git_commit, git_dirty, build_timestamp, config_dirs)
            }
            ServiceCommand::RunWrapper => run_wrapper(version),
        }
    }
}

// ── Run-wrapper: compiled process wrapper for auto-update + tray icon ──

/// Exit codes from the freenet binary that we handle specially.
const WRAPPER_EXIT_UPDATE_NEEDED: i32 = 42;
const WRAPPER_EXIT_ALREADY_RUNNING: i32 = 43;

/// Internal sentinel exit codes used by the wrapper loop (never from the child).
/// Restart: skip exit-code handling and relaunch immediately.
const SENTINEL_RESTART: i32 = -1;
/// Stop: enter stopped state, wait for tray Start/Quit action.
const SENTINEL_STOP: i32 = -2;

/// Initial backoff delay after a crash (seconds).
const WRAPPER_INITIAL_BACKOFF_SECS: u64 = 10;
/// Maximum backoff delay (seconds).
const WRAPPER_MAX_BACKOFF_SECS: u64 = 300;
/// Maximum port-conflict kill-and-retry attempts before giving up.
const WRAPPER_MAX_PORT_CONFLICT_KILLS: u32 = 3;
/// Maximum consecutive failures before the wrapper gives up.
const WRAPPER_MAX_CONSECUTIVE_FAILURES: u32 = 50;

/// Dashboard URL served by the local freenet node.
#[allow(dead_code)] // Used on Windows/macOS (tray + wrapper loop)
pub(crate) const DASHBOARD_URL: &str = "http://127.0.0.1:7509/";

/// host:port pair the dashboard HTTP server listens on. Kept in sync with
/// `DASHBOARD_URL`. Not cfg-gated even though only the tray wrappers use
/// it at runtime, so the `dashboard_url_and_addr_stay_in_sync` test can
/// assert their consistency on Linux CI.
#[allow(dead_code)]
const DASHBOARD_ADDR: &str = "127.0.0.1:7509";

/// Open a URL in the default browser (platform-specific).
#[cfg(any(target_os = "windows", target_os = "macos"))]
fn open_url_in_browser(url: &str) {
    super::open_url_in_browser(url);
}

// ── First-run onboarding ──
//
// On first launch of the tray wrapper we open the local dashboard in the
// user's default browser so a new user can actually see Freenet doing
// something. Subsequent launches are silent, matching Mac menu-bar
// conventions (Docker Desktop, Dropbox, Rectangle, etc.: guide the user
// on first install, stay out of the way afterwards).
//
// The marker-file helpers are platform-independent so they can be unit-
// tested on Linux CI; the dashboard-opening thread is tray-specific.

/// Path of the first-run marker file, if we can determine a data directory.
#[allow(dead_code)] // Used by the tray wrapper on Windows/macOS only
pub(super) fn first_run_marker_path() -> Option<PathBuf> {
    dirs::data_local_dir().map(|d| d.join("freenet").join(".first-run-complete"))
}

#[allow(dead_code)]
pub(super) fn is_first_run_at(marker: &Path) -> bool {
    !marker.exists()
}

#[allow(dead_code)]
pub(super) fn mark_first_run_complete_at(marker: &Path) -> std::io::Result<()> {
    if let Some(parent) = marker.parent() {
        std::fs::create_dir_all(parent)?;
    }
    std::fs::File::create(marker).map(|_| ())
}

/// Path of the legacy-install migration marker (issue #3943). DISTINCT from
/// the first-run/onboarding marker: a user who already launched a DMG before
/// this migration shipped has the first-run marker set but may still have the
/// racing legacy launchd agent, so the migration must NOT be gated on
/// onboarding state. The migration is its own one-shot, tracked by its own
/// marker, so it runs once for existing DMG users too.
#[allow(dead_code)]
pub(super) fn legacy_migration_marker_path() -> Option<PathBuf> {
    dirs::data_local_dir().map(|d| d.join("freenet").join(".legacy-migration-complete"))
}

#[cfg(any(target_os = "windows", target_os = "macos"))]
pub(super) fn dashboard_port_is_listening() -> bool {
    use std::net::TcpStream;
    use std::time::Duration;
    let Ok(addr) = DASHBOARD_ADDR.parse::<std::net::SocketAddr>() else {
        return false;
    };
    TcpStream::connect_timeout(&addr, Duration::from_millis(500)).is_ok()
}

// ── Platform dispatch functions ──────────────────────────────────────────────
//
// These thin wrappers dispatch to the platform-specific submodule (linux,
// macos, windows) or the purge/wrapper submodule. They live at this module
// level so `ServiceCommand::run` can call them without qualification, and
// so `use super::*` in the tests block pulls them all into scope.

#[cfg(target_os = "linux")]
fn install_service(system: bool) -> Result<()> {
    linux::install_service(system)
}
#[cfg(target_os = "macos")]
fn install_service(system: bool) -> Result<()> {
    macos::install_service(system)
}
#[cfg(target_os = "windows")]
fn install_service(system: bool) -> Result<()> {
    windows::install_service(system)
}

#[cfg(target_os = "linux")]
fn uninstall_service(system: bool, purge: bool, keep_data: bool) -> Result<()> {
    linux::uninstall_service(system, purge, keep_data)
}
#[cfg(target_os = "macos")]
fn uninstall_service(system: bool, purge: bool, keep_data: bool) -> Result<()> {
    macos::uninstall_service(system, purge, keep_data)
}
#[cfg(target_os = "windows")]
fn uninstall_service(system: bool, purge: bool, keep_data: bool) -> Result<()> {
    windows::uninstall_service(system, purge, keep_data)
}

#[cfg(target_os = "linux")]
fn service_status(system: bool) -> Result<()> {
    linux::service_status(system)
}
#[cfg(target_os = "macos")]
fn service_status(system: bool) -> Result<()> {
    macos::service_status(system)
}
#[cfg(target_os = "windows")]
fn service_status(system: bool) -> Result<()> {
    windows::service_status(system)
}

#[cfg(target_os = "linux")]
fn start_service(system: bool) -> Result<()> {
    linux::start_service(system)
}
#[cfg(target_os = "macos")]
fn start_service(system: bool) -> Result<()> {
    macos::start_service(system)
}
#[cfg(target_os = "windows")]
fn start_service(system: bool) -> Result<()> {
    windows::start_service(system)
}

#[cfg(target_os = "linux")]
fn stop_service(system: bool) -> Result<()> {
    linux::stop_service(system)
}
#[cfg(target_os = "macos")]
fn stop_service(system: bool) -> Result<()> {
    macos::stop_service(system)
}
#[cfg(target_os = "windows")]
fn stop_service(system: bool) -> Result<()> {
    windows::stop_service(system)
}

#[cfg(target_os = "linux")]
fn restart_service(system: bool) -> Result<()> {
    linux::restart_service(system)
}
#[cfg(target_os = "macos")]
fn restart_service(system: bool) -> Result<()> {
    macos::restart_service(system)
}
#[cfg(target_os = "windows")]
fn restart_service(system: bool) -> Result<()> {
    windows::restart_service(system)
}

#[cfg(target_os = "linux")]
fn service_logs(error_only: bool) -> Result<()> {
    linux::service_logs(error_only)
}
#[cfg(target_os = "macos")]
fn service_logs(error_only: bool) -> Result<()> {
    macos::service_logs(error_only)
}
#[cfg(target_os = "windows")]
fn service_logs(error_only: bool) -> Result<()> {
    windows::service_logs(error_only)
}

fn service_doctor(system: bool) -> Result<()> {
    purge::service_doctor(system)
}
fn run_wrapper(version: &str) -> Result<()> {
    wrapper::run_wrapper(version)
}

/// Public wrapper for use by the `uninstall` command.
pub fn purge_data(system_mode: bool) -> Result<()> {
    purge::purge_data(system_mode)
}

// Re-exports for the `update` command and other callers outside this module.
pub(crate) use log_utils::find_latest_log_file;
pub use purge::remove_dir_if_empty_pub;
pub use purge::should_purge;

#[cfg(target_os = "linux")]
pub use linux::generate_system_service_file;
#[cfg(target_os = "linux")]
pub use linux::generate_user_service_file;
#[cfg(target_os = "linux")]
pub use linux::stop_and_remove_service;

#[cfg(target_os = "macos")]
pub use macos::generate_wrapper_script;
#[cfg(target_os = "macos")]
pub use macos::stop_and_remove_service;

// Re-exports for tray.rs (Launch at Login toggle and state query).
#[cfg(target_os = "macos")]
pub(crate) use launch_at_login::{
    ToggleLaunchAtLoginOutcome, disable_launch_at_login, enable_launch_at_login,
    is_launch_at_login_enabled, macos_app_bundle_path, toggle_launch_at_login_outcome,
};

// Re-exports for update.rs (macOS plist generation).
#[cfg(target_os = "macos")]
pub(crate) use macos::generate_plist;

#[cfg(target_os = "windows")]
pub(crate) use windows::kill_freenet_service_processes;
#[cfg(target_os = "windows")]
pub use windows::stop_and_remove_service;

// Fallback for unsupported platforms
#[cfg(not(any(target_os = "linux", target_os = "macos", target_os = "windows")))]
fn install_service(_system: bool) -> Result<()> {
    anyhow::bail!("Service installation is not supported on this platform")
}

#[cfg(not(any(target_os = "linux", target_os = "macos", target_os = "windows")))]
pub fn stop_and_remove_service(_system: bool) -> Result<bool> {
    Ok(false)
}

#[cfg(not(any(target_os = "linux", target_os = "macos", target_os = "windows")))]
fn uninstall_service(_system: bool, _purge: bool, _keep_data: bool) -> Result<()> {
    anyhow::bail!("Service management is not supported on this platform")
}

#[cfg(not(any(target_os = "linux", target_os = "macos", target_os = "windows")))]
fn service_status(_system: bool) -> Result<()> {
    anyhow::bail!("Service management is not supported on this platform")
}

#[cfg(not(any(target_os = "linux", target_os = "macos", target_os = "windows")))]
fn start_service(_system: bool) -> Result<()> {
    anyhow::bail!("Service management is not supported on this platform")
}

#[cfg(not(any(target_os = "linux", target_os = "macos", target_os = "windows")))]
fn stop_service(_system: bool) -> Result<()> {
    anyhow::bail!("Service management is not supported on this platform")
}

#[cfg(not(any(target_os = "linux", target_os = "macos", target_os = "windows")))]
fn restart_service(_system: bool) -> Result<()> {
    anyhow::bail!("Service management is not supported on this platform")
}

#[cfg(not(any(target_os = "linux", target_os = "macos", target_os = "windows")))]
fn service_logs(_error_only: bool) -> Result<()> {
    anyhow::bail!("Service management is not supported on this platform")
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Regression guard for the service.rs submodule split: it dropped
    /// `spawn_first_run_dashboard_opener` entirely (only the call site
    /// survived) and left several Windows-only items unimported, breaking the
    /// Windows build. Linux CI compiles none of those `#[cfg(windows)]` paths,
    /// so the breakage shipped to `main` silently. These source-scrape pins run
    /// on every platform and fail fast if the Windows-critical wiring is
    /// dropped again.
    #[test]
    fn windows_first_run_wiring_is_present() {
        let wrapper = include_str!("service/wrapper.rs");
        assert!(
            wrapper.contains("fn spawn_first_run_dashboard_opener("),
            "wrapper.rs must define spawn_first_run_dashboard_opener — the split \
             dropped it, breaking the Windows build."
        );
        assert!(
            wrapper.contains("use super::single_instance::FIRST_RUN_OPENER_SPAWNED;"),
            "wrapper.rs must import FIRST_RUN_OPENER_SPAWNED from single_instance."
        );
        let windows = include_str!("service/windows.rs");
        assert!(
            windows.contains("use super::log_utils::find_latest_log_file;"),
            "windows.rs must import find_latest_log_file from log_utils."
        );
    }

    // Pull test-helper functions from submodules that are not re-exported at the
    // service.rs level (they are pub(super) in their submodule, making them
    // visible here via explicit `use`).
    #[allow(unused_imports)]
    use super::launch_at_login::*;
    #[allow(unused_imports)]
    use super::linux::*;
    #[allow(unused_imports)]
    use super::log_utils::*;
    #[allow(unused_imports)]
    use super::macos::*;
    #[allow(unused_imports)]
    use super::purge::*;
    #[allow(unused_imports)]
    use super::single_instance::*;
    #[allow(unused_imports)]
    use super::windows::*;
    #[allow(unused_imports)]
    use super::wrapper::*;
    use std::path::PathBuf;

    /// Return the section header (e.g. `[Unit]`, `[Service]`) that a given `key=`
    /// directive line appears under in a generated systemd unit, or `None` if the
    /// directive is absent. Comment lines (`# ...`) and the directive's value are
    /// ignored — only a line that *starts* with `{key}=` counts.
    ///
    /// Used to assert `StartLimitAction` lives in `[Unit]` (#4551): systemd SILENTLY
    /// IGNORES the `StartLimit*` directives when placed in `[Service]`, so a substring
    /// check that only proves the key *exists* would pass even with the limiter knob
    /// disabled. (The `[Unit]` placement of `StartLimitBurst`/`StartLimitIntervalSec`
    /// themselves is asserted by the linux.rs unit tests added in #4570.)
    #[cfg(target_os = "linux")]
    fn section_of_directive(unit: &str, key: &str) -> Option<String> {
        let prefix = format!("{key}=");
        let mut current: Option<String> = None;
        for line in unit.lines() {
            let trimmed = line.trim();
            if trimmed.starts_with('[') && trimmed.ends_with(']') && !trimmed.contains(' ') {
                current = Some(trimmed.to_string());
            } else if trimmed.starts_with(&prefix) {
                return current;
            }
        }
        None
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn section_of_directive_parses_sections() {
        let unit = "[Unit]\nDescription=x\nStartLimitBurst=5\n\n[Service]\nType=simple\n# StartLimitBurst=99 in a comment must be ignored\nExecStart=/bin/freenet network\n";
        assert_eq!(
            section_of_directive(unit, "Description").as_deref(),
            Some("[Unit]")
        );
        assert_eq!(
            section_of_directive(unit, "StartLimitBurst").as_deref(),
            Some("[Unit]")
        );
        assert_eq!(
            section_of_directive(unit, "Type").as_deref(),
            Some("[Service]")
        );
        assert_eq!(
            section_of_directive(unit, "ExecStart").as_deref(),
            Some("[Service]")
        );
        assert_eq!(section_of_directive(unit, "Nonexistent"), None);
    }

    #[test]
    fn command_line_args_strips_quoted_executable_path() {
        assert_eq!(
            command_line_args("\"C:\\Program Files\\Freenet\\freenet.exe\" service run-wrapper"),
            "service run-wrapper"
        );
        // Quoted path, no arguments — the GUI installer launched by Explorer.
        assert_eq!(
            command_line_args("\"C:\\Users\\me\\Downloads\\freenet.exe\""),
            ""
        );
    }

    #[test]
    fn command_line_args_strips_unquoted_executable_path() {
        assert_eq!(command_line_args("C:\\bin\\freenet.exe network"), "network");
        assert_eq!(command_line_args("freenet.exe"), "");
    }

    #[test]
    fn command_line_args_handles_degenerate_input() {
        // Empty and whitespace-only input.
        assert_eq!(command_line_args(""), "");
        assert_eq!(command_line_args("   "), "");
        // A leading quote with no closing quote — must not panic, yields "".
        assert_eq!(command_line_args("\"C:\\bin\\freenet.exe network"), "");
    }

    #[test]
    fn classify_freenet_process_identifies_wrapper_and_node() {
        assert_eq!(
            classify_freenet_process(
                "\"C:\\Users\\me\\AppData\\Local\\Freenet\\bin\\freenet.exe\" service run-wrapper"
            ),
            Some(FreenetServiceProcess::Wrapper)
        );
        assert_eq!(
            classify_freenet_process(
                "\"C:\\Users\\me\\AppData\\Local\\Freenet\\bin\\freenet.exe\" network"
            ),
            Some(FreenetServiceProcess::Node)
        );
    }

    /// Regression test for issue #4205: the Windows GUI installer is launched
    /// with no subcommand, so it must never be classified as a service
    /// process — otherwise the install-time `service stop` step kills the
    /// installer itself before anything is installed.
    #[test]
    fn classify_freenet_process_spares_installer_and_other_commands() {
        // GUI installer: double-clicked freenet.exe, no arguments.
        assert_eq!(
            classify_freenet_process("\"C:\\Users\\me\\Downloads\\freenet.exe\""),
            None
        );
        // The `service stop` child process that triggers the kill.
        assert_eq!(
            classify_freenet_process("\"C:\\Users\\me\\Downloads\\freenet.exe\" service stop"),
            None
        );
        // A concurrent self-update must not be killed.
        assert_eq!(
            classify_freenet_process("\"C:\\Program Files\\Freenet\\freenet.exe\" update"),
            None
        );
        // An install path that merely contains the word "network" — the
        // quoted exe path is stripped, so this must not match.
        assert_eq!(
            classify_freenet_process("\"C:\\Users\\me\\My Network Tools\\freenet.exe\""),
            None
        );
    }

    /// `network` / `run-wrapper` must only match as the subcommand (the first
    /// argument token), never as an option value or later argument —
    /// otherwise an unrelated `freenet` CLI invocation would be killed.
    #[test]
    fn classify_freenet_process_matches_subcommand_prefix_only() {
        // `network` appearing as an option value, not the subcommand.
        assert_eq!(
            classify_freenet_process("\"C:\\bin\\freenet.exe\" local --config-dir network"),
            None
        );
        // `run-wrapper` only counts immediately after the `service` subcommand.
        assert_eq!(
            classify_freenet_process("\"C:\\bin\\freenet.exe\" service stop run-wrapper"),
            None
        );
        // The real node spawn has `network` as the first token.
        assert_eq!(
            classify_freenet_process("\"C:\\bin\\freenet.exe\" network"),
            Some(FreenetServiceProcess::Node)
        );
    }

    #[test]
    fn parse_freenet_process_listing_extracts_pid_and_command_line() {
        let stdout = "1234\t\"C:\\bin\\freenet.exe\" service run-wrapper\r\n\
                      5678\t\"C:\\bin\\freenet.exe\" network\r\n\
                      9012\t\"C:\\Users\\me\\Downloads\\freenet.exe\"\r\n";
        assert_eq!(
            parse_freenet_process_listing(stdout),
            vec![
                (
                    1234u32,
                    "\"C:\\bin\\freenet.exe\" service run-wrapper".to_string()
                ),
                (5678u32, "\"C:\\bin\\freenet.exe\" network".to_string()),
                (
                    9012u32,
                    "\"C:\\Users\\me\\Downloads\\freenet.exe\"".to_string()
                ),
            ]
        );
    }

    #[test]
    fn parse_freenet_process_listing_skips_blank_and_malformed_lines() {
        // A process whose CommandLine could not be read prints just its PID;
        // blank lines and non-numeric lines must be ignored.
        assert_eq!(
            parse_freenet_process_listing("4242\t\r\n\r\nnot-a-pid\tsomething\r\n"),
            vec![(4242u32, String::new())]
        );
    }

    /// Flock-level regression test for the dup-tray bug observed in
    /// phase-1 smoke test 2026-04-22. Uses a dedicated path inside a
    /// temp directory instead of `~/Library/Caches/Freenet/wrapper.lock`
    /// so parallel tests (and CI) don't interfere. `flock` has
    /// identical semantics on Linux and macOS, so this test exercises
    /// the exact mechanism the production path uses even though the
    /// production path is macOS-only.
    #[cfg(unix)]
    fn try_acquire_flock_at(path: &Path) -> Option<std::fs::File> {
        use std::os::unix::io::AsRawFd;
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent).unwrap();
        }
        let file = std::fs::OpenOptions::new()
            .create(true)
            .truncate(false)
            .write(true)
            .open(path)
            .unwrap();
        // SAFETY: file is a valid open fd owned by `file`; flock is safe to call on any fd.
        let rc = unsafe { libc::flock(file.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) };
        if rc == 0 { Some(file) } else { None }
    }

    #[test]
    #[cfg(unix)]
    fn wrapper_lock_is_exclusive() {
        // Two acquirers of the same lockfile: first wins, second fails.
        // This is the invariant the dup-tray fix relies on. If a future
        // refactor turns the lock into a shared lock, or swaps LOCK_NB
        // for blocking, this test fails.
        let tmp = tempfile::tempdir().unwrap();
        let lock_path = tmp.path().join("wrapper.lock");

        let holder =
            try_acquire_flock_at(&lock_path).expect("first acquire must succeed on an unheld lock");
        assert!(
            try_acquire_flock_at(&lock_path).is_none(),
            "second acquire must fail while the first holder is alive"
        );

        // Dropping the holder releases the flock; a new acquirer
        // should now succeed. Exercises the kernel-released-on-close
        // semantics we rely on for crash recovery.
        drop(holder);
        assert!(
            try_acquire_flock_at(&lock_path).is_some(),
            "acquire must succeed once the previous holder has exited"
        );
    }

    #[test]
    fn wrapper_lock_path_is_under_cache_dir() {
        // If the platform can resolve a cache dir, the lock lives
        // under it in a Freenet subdirectory. Guards against a future
        // refactor that moves the lockfile somewhere user-global
        // (e.g. /tmp) where non-Freenet processes could collide.
        if let Some(p) = wrapper_lock_path() {
            assert!(
                p.ends_with("Freenet/wrapper.lock"),
                "unexpected wrapper lock path: {}",
                p.display()
            );
            let cache = dirs::cache_dir().expect("cache dir required for this assertion");
            assert!(
                p.starts_with(&cache),
                "wrapper lock {} must live under the user cache dir {}",
                p.display(),
                cache.display()
            );
        }
    }

    #[test]
    fn dashboard_url_and_addr_stay_in_sync() {
        // Guard against a future edit that changes DASHBOARD_URL (human-
        // readable form) without updating DASHBOARD_ADDR (parsed form), or
        // vice versa. They must agree on host:port.
        assert!(
            DASHBOARD_URL.contains(DASHBOARD_ADDR),
            "DASHBOARD_URL {DASHBOARD_URL:?} must contain DASHBOARD_ADDR {DASHBOARD_ADDR:?}"
        );
        DASHBOARD_ADDR
            .parse::<std::net::SocketAddr>()
            .expect("DASHBOARD_ADDR must parse as a SocketAddr");
    }

    #[test]
    fn launch_agent_plist_contents_embeds_all_program_arguments() {
        let args = vec![
            "/usr/local/bin/freenet".to_string(),
            "service".to_string(),
            "run-wrapper".to_string(),
        ];
        let xml = launch_agent_plist_contents(&args);
        assert!(xml.starts_with("<?xml version=\"1.0\""));
        assert!(xml.contains("<key>Label</key>"));
        assert!(xml.contains("org.freenet.Freenet"));
        assert!(xml.contains("<key>RunAtLoad</key>"));
        assert!(xml.contains("<true/>"));
        assert!(xml.contains("<string>/usr/local/bin/freenet</string>"));
        assert!(xml.contains("<string>service</string>"));
        assert!(xml.contains("<string>run-wrapper</string>"));
    }

    #[test]
    fn launch_agent_plist_contents_xml_escapes_unusual_paths() {
        // Unusual but legal filesystem path characters must be XML-escaped
        // or launchd fails to parse the plist at load time and the user's
        // Launch at Login setting silently does nothing.
        let args = vec!["/tmp/has <angles> & \"quotes\"/Freenet".to_string()];
        let xml = launch_agent_plist_contents(&args);
        assert!(xml.contains("&lt;angles&gt;"));
        assert!(xml.contains("&amp;"));
        assert!(xml.contains("&quot;quotes&quot;"));
        assert!(!xml.contains(" <angles> "));
        assert!(!xml.contains("& \""));
    }

    #[test]
    fn launch_agent_program_arguments_for_bundle_points_at_wrapper() {
        // Inside a .app bundle, ProgramArguments must be the CFBundleExecutable
        // shell wrapper (Contents/MacOS/Freenet). Launching it re-enters the
        // tray path via the `exec freenet-bin service run-wrapper` line in
        // scripts/package-macos.sh. If this regresses and ProgramArguments
        // becomes just `freenet-bin`, launchd runs bare freenet which
        // defaults to Network mode and the tray never appears: the exact
        // shipping bug that Codex P2 and the code-first/big-picture reviews
        // flagged on the first round of this commit.
        let exe = PathBuf::from("/Applications/Freenet.app/Contents/MacOS/freenet-bin");
        let args = launch_agent_program_arguments(&exe);
        assert_eq!(
            args,
            vec!["/Applications/Freenet.app/Contents/MacOS/Freenet".to_string()]
        );
    }

    #[test]
    fn launch_agent_program_arguments_for_nested_bundle_path() {
        // User dragged Freenet.app into a subfolder.
        let exe = PathBuf::from(
            "/Users/someone/Applications/Tools/Freenet.app/Contents/MacOS/freenet-bin",
        );
        let args = launch_agent_program_arguments(&exe);
        assert_eq!(
            args,
            vec![
                "/Users/someone/Applications/Tools/Freenet.app/Contents/MacOS/Freenet".to_string()
            ]
        );
    }

    #[test]
    fn launch_agent_program_arguments_for_non_bundle_binary_includes_subcommand() {
        // Non-bundle path (cargo run, raw binary install, dev build): the
        // plist must explicitly pass `service run-wrapper` so launchd
        // doesn't default the binary to Network mode.
        let exe = PathBuf::from("/usr/local/bin/freenet");
        let args = launch_agent_program_arguments(&exe);
        assert_eq!(
            args,
            vec![
                "/usr/local/bin/freenet".to_string(),
                "service".to_string(),
                "run-wrapper".to_string(),
            ]
        );
    }

    #[test]
    fn macos_app_bundle_path_finds_bundle_from_inner_binary() {
        let exe = PathBuf::from("/Applications/Freenet.app/Contents/MacOS/freenet-bin");
        assert_eq!(
            macos_app_bundle_path(&exe).as_deref(),
            Some(Path::new("/Applications/Freenet.app"))
        );
    }

    #[test]
    fn macos_app_bundle_path_returns_none_for_non_bundle_path() {
        assert_eq!(
            macos_app_bundle_path(Path::new("/usr/local/bin/freenet")),
            None
        );
        assert_eq!(macos_app_bundle_path(Path::new("/tmp/freenet")), None);
    }

    #[test]
    fn first_run_launch_at_login_action_covers_all_cases() {
        assert_eq!(
            first_run_launch_at_login_action(true, false),
            FirstRunLaunchAtLoginAction::Register
        );
        assert_eq!(
            first_run_launch_at_login_action(true, true),
            FirstRunLaunchAtLoginAction::AlreadyEnabled
        );
        assert_eq!(
            first_run_launch_at_login_action(false, false),
            FirstRunLaunchAtLoginAction::NotFirstRun
        );
        assert_eq!(
            first_run_launch_at_login_action(false, true),
            FirstRunLaunchAtLoginAction::NotFirstRun
        );
    }

    // ── Legacy install.sh → DMG migration (issue #3943) ──

    fn legacy_plist(exists: bool) -> Option<(PathBuf, bool)> {
        Some((
            PathBuf::from("/u/Library/LaunchAgents/org.freenet.node.plist"),
            exists,
        ))
    }

    #[test]
    fn legacy_install_migration_plan_skips_when_already_migrated() {
        // Migration marker present (we already migrated on a previous launch):
        // never re-migrate, even with a legacy plist + binaries still present.
        let resolved = ResolvedLegacyInstall {
            wrapper_script: Some(PathBuf::from("/u/.local/bin/freenet-service-wrapper.sh")),
            binaries: vec![PathBuf::from("/u/.local/bin/freenet")],
        };
        let plan = legacy_install_migration_plan(
            /* migration_already_done */ true,
            legacy_plist(true),
            &resolved,
        );
        assert_eq!(plan, LegacyMigrationPlan::NoMigration);
    }

    #[test]
    fn legacy_install_migration_plan_skips_when_no_legacy_plist() {
        // Clean (new-user) DMG install: no legacy plist, so there is nothing
        // to migrate even if a stray binary somehow exists. A lone binary with
        // no auto-start agent is harmless.
        let resolved = ResolvedLegacyInstall {
            wrapper_script: None,
            binaries: vec![PathBuf::from("/u/.local/bin/freenet")],
        };
        assert_eq!(
            legacy_install_migration_plan(false, None, &resolved),
            LegacyMigrationPlan::NoMigration
        );
        assert_eq!(
            legacy_install_migration_plan(false, legacy_plist(false), &resolved),
            LegacyMigrationPlan::NoMigration
        );
    }

    /// Regression test for issue #3943. Before the fix, a DMG launch with a
    /// pre-existing install.sh legacy install only LOGGED a warning; it did
    /// NOT remove the legacy plist, wrapper, or CLI binaries, so the two
    /// installs raced for port 7509 on every login. This asserts the plan
    /// boots out + removes the legacy plist first, then the wrapper script,
    /// then each legacy binary, in order. `migration_already_done = false`
    /// (covers existing DMG users, not just brand-new onboarding — Codex P1).
    #[test]
    fn legacy_install_migration_plan_removes_plist_wrapper_and_binaries() {
        let plist = PathBuf::from("/u/Library/LaunchAgents/org.freenet.node.plist");
        let resolved = ResolvedLegacyInstall {
            wrapper_script: Some(PathBuf::from("/u/.local/bin/freenet-service-wrapper.sh")),
            binaries: vec![
                PathBuf::from("/u/.local/bin/fdev"),
                PathBuf::from("/u/.local/bin/freenet"),
            ],
        };
        let plan = legacy_install_migration_plan(false, Some((plist.clone(), true)), &resolved);

        assert_eq!(
            plan,
            LegacyMigrationPlan::Migrate(vec![
                // Bootout + plist removal first so launchd stops respawning
                // the legacy backend before we remove anything else.
                LegacyMigrationStep::BootOutAndRemovePlist(plist),
                LegacyMigrationStep::RemoveLegacyWrapperScript(PathBuf::from(
                    "/u/.local/bin/freenet-service-wrapper.sh"
                )),
                LegacyMigrationStep::RemoveLegacyBinary(PathBuf::from("/u/.local/bin/fdev")),
                LegacyMigrationStep::RemoveLegacyBinary(PathBuf::from("/u/.local/bin/freenet")),
            ])
        );
    }

    #[test]
    fn legacy_install_migration_plan_handles_plist_only() {
        // Legacy plist present but the wrapper/binary couldn't be resolved
        // (e.g. wrapper already deleted, or plist hand-edited). We still boot
        // out + remove the plist — that is the thing racing for the port —
        // and the plan contains exactly that one step. Crucially we do NOT
        // fabricate binary-removal steps for guessed locations.
        let plist = PathBuf::from("/u/Library/LaunchAgents/org.freenet.node.plist");
        let plan = legacy_install_migration_plan(
            false,
            Some((plist.clone(), true)),
            &ResolvedLegacyInstall::default(),
        );
        assert_eq!(
            plan,
            LegacyMigrationPlan::Migrate(vec![LegacyMigrationStep::BootOutAndRemovePlist(plist)])
        );
    }

    /// Plist-path extractor against representative launchd-plist XML (the
    /// shape `generate_plist` emits). Runs on every platform — Linux CI
    /// exercises the parsing logic here; the macOS-only
    /// `legacy_extractors_match_generators` test below additionally pins the
    /// coupling to the generator's exact output.
    #[test]
    fn legacy_program_path_from_plist_parses_program_arguments() {
        let xml = "\
<plist version=\"1.0\"><dict>
  <key>Label</key><string>org.freenet.node</string>
  <key>ProgramArguments</key>
  <array>
    <string>/u/.local/bin/freenet-service-wrapper.sh</string>
  </array>
</dict></plist>";
        assert_eq!(
            legacy_program_path_from_plist(xml),
            Some(PathBuf::from("/u/.local/bin/freenet-service-wrapper.sh"))
        );

        // XML-escaped ampersand in the path round-trips through unescape.
        let xml = "<key>ProgramArguments</key><array><string>/u/A &amp; B/w.sh</string></array>";
        assert_eq!(
            legacy_program_path_from_plist(xml),
            Some(PathBuf::from("/u/A & B/w.sh"))
        );

        // No ProgramArguments, empty array, empty string → None (caller then
        // removes only the plist itself).
        assert_eq!(legacy_program_path_from_plist("<plist></plist>"), None);
        assert_eq!(
            legacy_program_path_from_plist("<key>ProgramArguments</key><array></array>"),
            None
        );
        assert_eq!(
            legacy_program_path_from_plist(
                "<key>ProgramArguments</key><array><string></string></array>"
            ),
            None
        );
    }

    /// Wrapper-binary extractor against representative wrapper-script content
    /// (the shape `generate_wrapper_script` emits), including a path with a
    /// space. Runs on every platform.
    #[test]
    fn legacy_binary_path_from_wrapper_parses_launch_line() {
        let sh = "#!/bin/bash\n# preamble\n    \"/u/.local/bin/freenet\" network 2>/dev/null &\n";
        assert_eq!(
            legacy_binary_path_from_wrapper(sh),
            Some(PathBuf::from("/u/.local/bin/freenet"))
        );

        // Quoted path with a space survives (the `-o command=` regression).
        let sh = "\"/Users/Some User/bin/freenet\" network &\n";
        assert_eq!(
            legacy_binary_path_from_wrapper(sh),
            Some(PathBuf::from("/Users/Some User/bin/freenet"))
        );

        // No recognizable launch line → None (no guessed binary removed).
        assert_eq!(
            legacy_binary_path_from_wrapper("#!/bin/bash\necho hi\n"),
            None
        );
    }

    /// The wrapper-ownership guard (Codex P2, round 4): we only delete a script
    /// that is BOTH named `freenet-service-wrapper.sh` AND carries the
    /// generated header. A hand-edited/repurposed `org.freenet.node` plist
    /// pointing at an unrelated script must not get that script deleted.
    #[test]
    fn is_legacy_freenet_wrapper_requires_name_and_signature() {
        let real = "#!/bin/bash\n# Freenet service wrapper for auto-update support.\n…";
        assert!(is_legacy_freenet_wrapper(
            Some("freenet-service-wrapper.sh"),
            real
        ));

        // Right content but wrong filename (repurposed plist target).
        assert!(!is_legacy_freenet_wrapper(Some("my-script.sh"), real));
        // Right filename but not our content (user overwrote it).
        assert!(!is_legacy_freenet_wrapper(
            Some("freenet-service-wrapper.sh"),
            "#!/bin/bash\nrm -rf ~\n"
        ));
        // No basename at all.
        assert!(!is_legacy_freenet_wrapper(None, real));
    }

    /// macOS-only: pin the extractors to `generate_plist` /
    /// `generate_wrapper_script`'s EXACT output so a format drift in either
    /// generator (which only compiles on macOS) trips CI. Also confirm the
    /// real generated wrapper passes the ownership guard.
    #[cfg(target_os = "macos")]
    #[test]
    fn legacy_extractors_match_generators() {
        let wrapper = PathBuf::from("/u/.local/bin/freenet-service-wrapper.sh");
        let log_dir = PathBuf::from("/u/.local/state/freenet");
        let xml = generate_plist(&wrapper, &log_dir);
        assert_eq!(legacy_program_path_from_plist(&xml), Some(wrapper));

        let bin = PathBuf::from("/u/.local/bin/freenet");
        let sh = generate_wrapper_script(&bin);
        assert_eq!(legacy_binary_path_from_wrapper(&sh), Some(bin));
        // The real generated wrapper must satisfy the ownership guard, or the
        // migration would never act on a genuine legacy install.
        assert!(is_legacy_freenet_wrapper(
            Some("freenet-service-wrapper.sh"),
            &sh
        ));

        // End-to-end: plist → wrapper path → wrapper script → binary path.
        let spacey = PathBuf::from("/Users/Some User/bin/freenet");
        let sh = generate_wrapper_script(&spacey);
        assert_eq!(legacy_binary_path_from_wrapper(&sh), Some(spacey));
    }

    #[test]
    fn toggle_launch_at_login_outcome_inverts_current_state() {
        assert_eq!(
            toggle_launch_at_login_outcome(false),
            ToggleLaunchAtLoginOutcome::Enable
        );
        assert_eq!(
            toggle_launch_at_login_outcome(true),
            ToggleLaunchAtLoginOutcome::Disable
        );
    }

    #[test]
    fn launch_at_login_plist_roundtrips() {
        let tmp = tempfile::tempdir().unwrap();
        let plist = tmp
            .path()
            .join("nested/Library/LaunchAgents/org.freenet.Freenet.plist");
        let args = vec!["/Applications/Freenet.app/Contents/MacOS/Freenet".to_string()];

        assert!(!is_launch_at_login_enabled_at(&plist));

        // Writing creates parent dirs and flips the state.
        write_launch_agent_plist_at(&plist, &args).unwrap();
        assert!(is_launch_at_login_enabled_at(&plist));
        let body = std::fs::read_to_string(&plist).unwrap();
        assert!(body.contains("/Applications/Freenet.app/Contents/MacOS/Freenet"));

        // Self-heal detection: happy-path match, then mismatch after a
        // subsequent write with a different leader (simulates the user
        // moving the .app).
        assert!(launch_agent_plist_has_expected_leader(
            &plist,
            "/Applications/Freenet.app/Contents/MacOS/Freenet"
        ));
        assert!(!launch_agent_plist_has_expected_leader(
            &plist,
            "/Users/someone/Applications/Freenet.app/Contents/MacOS/Freenet"
        ));

        let moved =
            vec!["/Users/someone/Applications/Freenet.app/Contents/MacOS/Freenet".to_string()];
        write_launch_agent_plist_at(&plist, &moved).unwrap();
        assert!(launch_agent_plist_has_expected_leader(
            &plist,
            "/Users/someone/Applications/Freenet.app/Contents/MacOS/Freenet"
        ));

        // Removing flips it back. Idempotent on repeat.
        remove_launch_agent_plist_at(&plist).unwrap();
        assert!(!is_launch_at_login_enabled_at(&plist));
        remove_launch_agent_plist_at(&plist).unwrap();
    }

    #[test]
    fn first_run_marker_roundtrips() {
        let tmp = tempfile::tempdir().unwrap();
        let marker = tmp.path().join("freenet").join(".first-run-complete");

        // Before any write, it should be a first run.
        assert!(is_first_run_at(&marker));
        assert!(!marker.exists());

        // Writing the marker creates parent dirs and flips the state.
        mark_first_run_complete_at(&marker).unwrap();
        assert!(marker.exists());
        assert!(!is_first_run_at(&marker));

        // Re-writing an existing marker must not error (subsequent launches
        // of a first-run flow that somehow re-ran shouldn't blow up).
        mark_first_run_complete_at(&marker).unwrap();
        assert!(!is_first_run_at(&marker));
    }

    #[test]
    #[cfg(target_os = "linux")]
    fn test_systemd_user_service_file_generation() {
        let binary_path = PathBuf::from("/usr/local/bin/freenet");
        let log_dir = PathBuf::from("/home/test/.local/state/freenet");
        let service_content = generate_user_service_file(&binary_path, &log_dir);

        // Verify the service file contains expected sections
        assert!(service_content.contains("[Unit]"));
        assert!(service_content.contains("[Service]"));
        assert!(service_content.contains("[Install]"));

        // Verify it references the correct binary
        assert!(service_content.contains("/usr/local/bin/freenet network"));

        // Logging routes to journal so journald handles rotation. The tracing
        // layer writes its own size-capped rolling files; routing systemd
        // stdout/stderr to a fixed freenet.log / freenet.error.log caused
        // unbounded growth (issue #4251 / log-spam fix).
        assert!(service_content.contains("StandardOutput=journal"));
        assert!(service_content.contains("StandardError=journal"));
        assert!(
            !service_content.contains("append:"),
            "regression: must not append systemd output to fixed unrotated file (#4251)"
        );

        // Verify resource limits are set
        assert!(service_content.contains("LimitNOFILE=65536"));
        assert!(service_content.contains("MemoryMax=2G"));
        assert!(service_content.contains("CPUQuota=200%"));

        // Verify restart configuration (always restart for auto-update support)
        assert!(service_content.contains("Restart=always"));
        assert!(service_content.contains("RestartSec=10"));

        // Verify restart loop prevention (limits consecutive failures).
        // StartLimitBurst/StartLimitIntervalSec are present and [Unit]-placed; their
        // section placement is asserted by the linux.rs unit tests added in #4570
        // (user_unit_places_start_limit_directives_in_unit_section). Here we only
        // assert this PR's additions: StartLimitAction placement + the exit-45 guard.
        assert!(service_content.contains("StartLimitBurst=5"));
        assert!(service_content.contains("StartLimitIntervalSec=120"));
        // #4551: StartLimitAction MUST also be in [Unit]; systemd SILENTLY IGNORES
        // it in [Service] (same trap as StartLimitBurst). StartLimitAction=none stops
        // (does not reboot) the host on a tripped loop.
        assert_eq!(
            section_of_directive(&service_content, "StartLimitAction").as_deref(),
            Some("[Unit]"),
            "StartLimitAction must be in [Unit] (#4551)"
        );
        assert!(
            service_content.contains("StartLimitAction=none"),
            "StartLimitAction=none keeps a tripped crash-loop as a stopped unit rather \
             than rebooting the host (#4551)"
        );
        // The 45 fast-crash code (crate::node::p2p_impl) must NOT be whitelisted by
        // SuccessExitStatus, or a boot-wedge loop would never count toward the burst.
        assert!(
            !service_content.contains("SuccessExitStatus=42 43 45")
                && !service_content.contains("SuccessExitStatus=42 45 43")
                && !service_content.contains("SuccessExitStatus=45"),
            "exit 45 (fast-crash) must stay OUT of SuccessExitStatus so it counts \
             toward StartLimitBurst (#4551)"
        );

        // Verify auto-update support via ExecStopPost. The `$EXIT_STATUS` systemd
        // sets in the ExecStopPost environment must reach /bin/sh as a literal
        // `$EXIT_STATUS`, so it is written doubled (`$$EXIT_STATUS`) in the unit —
        // systemd collapses `$$` -> `$`. A single-`$` revert silently breaks
        // auto-update (systemd eats the bare var before sh sees it) with no other
        // signal, so assert the doubled form, not just the directive's presence.
        assert!(
            service_content.contains("ExecStopPost=")
                && service_content.contains("\"$$EXIT_STATUS\""),
            "ExecStopPost must use the doubled $$EXIT_STATUS so systemd passes a \
             literal $EXIT_STATUS to sh (a single-$ revert silently breaks auto-update)"
        );
        // #4551: ExecStopPost must fire `freenet update` for BOTH exit 42 (healthy
        // node hit a fault/update) AND exit 45 (fast boot-crash) — firing on 45
        // preserves the #4549 self-heal for a boot-crash a newer release fixes, while
        // 45 staying out of SuccessExitStatus still counts it toward StartLimitBurst.
        // A `case` (not `&&`/`||`) avoids shell-precedence pitfalls.
        assert!(
            service_content.contains("case \"$$EXIT_STATUS\" in 42|45)")
                && service_content.contains("update --quiet"),
            "ExecStopPost must run `freenet update` for exit 42 OR 45 via a case \
             statement (boot-crash self-heal preserved; #4551)"
        );
        // #4551: the unit must advertise fast-crash (exit-45) support via the marker
        // env var the node gates on. Using the const (not a literal) pins template,
        // entry-point check, and test to one source of truth so they can't drift —
        // a drift would silently disable exit 45 (node never opts in).
        assert!(
            service_content.contains(&format!(
                "Environment={}=1",
                super::super::auto_update::SYSTEMD_FAST_CRASH_ENV_VAR
            )),
            "user unit must set the {} marker so the node opts in to exit 45 (#4551)",
            super::super::auto_update::SYSTEMD_FAST_CRASH_ENV_VAR
        );

        // Verify exit code 42 is treated as success (doesn't count against StartLimitBurst)
        assert!(service_content.contains("SuccessExitStatus=42 43"));

        // Verify exit code 43 prevents restart (another instance already running)
        assert!(service_content.contains("RestartPreventExitStatus=43"));

        // Regression for #3967: stale-orphan self-heal pre-flight. Because
        // RestartPreventExitStatus=43 blocks restart on exit 43, the
        // version-compare-and-kill must run as an ExecStartPre before the main
        // ExecStart so a stale orphan can't hold the port forever.
        assert!(
            service_content.contains("ExecStartPre=")
                && service_content.contains("Freenet version:")
                && service_content.contains("kill -TERM")
                && service_content.contains("kill -KILL"),
            "systemd unit must run a stale-orphan version-compare-and-kill pre-flight (#3967)"
        );

        // Verify graceful shutdown timeout is set. 45s = 30s drain
        // (default `shutdown-drain-secs`) + 15s peer-teardown headroom.
        assert!(service_content.contains("TimeoutStopSec=45"));

        // Verify user service targets default.target
        assert!(service_content.contains("WantedBy=default.target"));
        // User service should NOT have User= directive
        assert!(!service_content.contains("User="));
    }

    #[test]
    #[cfg(target_os = "linux")]
    fn test_systemd_system_service_file_generation() {
        let binary_path = PathBuf::from("/home/test/.local/bin/freenet");
        let log_dir = PathBuf::from("/home/test/.local/state/freenet");
        let username = "testuser";
        let home_dir = PathBuf::from("/home/test");
        let service_content =
            generate_system_service_file(&binary_path, &log_dir, username, &home_dir);

        // Verify sections
        assert!(service_content.contains("[Unit]"));
        assert!(service_content.contains("[Service]"));
        assert!(service_content.contains("[Install]"));

        // Verify User= directive is set
        assert!(service_content.contains("User=testuser"));

        // Verify HOME environment is set (needed for config/data paths)
        assert!(service_content.contains("Environment=HOME=/home/test"));

        // Verify system service targets multi-user.target
        assert!(service_content.contains("WantedBy=multi-user.target"));

        // Verify it still has all the standard settings
        assert!(service_content.contains("Restart=always"));
        assert!(service_content.contains("StartLimitBurst=5"));
        assert!(service_content.contains("StartLimitIntervalSec=120"));
        // StartLimitBurst/IntervalSec [Unit] placement is asserted by #4570's linux.rs
        // tests (system_unit_places_start_limit_directives_in_unit_section). Here we
        // assert this PR's additions: StartLimitAction placement + the exit-45 guard.
        assert_eq!(
            section_of_directive(&service_content, "StartLimitAction").as_deref(),
            Some("[Unit]"),
            "StartLimitAction must be in [Unit] (#4551)"
        );
        assert!(service_content.contains("StartLimitAction=none"));
        assert!(
            !service_content.contains("SuccessExitStatus=42 43 45")
                && !service_content.contains("SuccessExitStatus=45"),
            "exit 45 (fast-crash) must stay OUT of SuccessExitStatus (#4551)"
        );
        assert!(service_content.contains("LimitNOFILE=65536"));
        // ExecStopPost must double the env var (`$$EXIT_STATUS`) so systemd passes a
        // literal `$EXIT_STATUS` to sh; a single-`$` revert silently breaks auto-update.
        assert!(
            service_content.contains("ExecStopPost=")
                && service_content.contains("\"$$EXIT_STATUS\""),
            "ExecStopPost must use the doubled $$EXIT_STATUS (single-$ revert breaks auto-update)"
        );
        // #4551: ExecStopPost fires `freenet update` for exit 42 OR 45 (system unit too).
        assert!(
            service_content.contains("case \"$$EXIT_STATUS\" in 42|45)")
                && service_content.contains("update --quiet"),
            "ExecStopPost must run `freenet update` for exit 42 OR 45 via a case \
             statement (boot-crash self-heal preserved; #4551)"
        );
        // #4551: system unit must also set the fast-crash (exit-45) support marker.
        assert!(
            service_content.contains(&format!(
                "Environment={}=1",
                super::super::auto_update::SYSTEMD_FAST_CRASH_ENV_VAR
            )),
            "system unit must set the {} marker so the node opts in to exit 45 (#4551)",
            super::super::auto_update::SYSTEMD_FAST_CRASH_ENV_VAR
        );

        // Verify exit code 42 is treated as success (doesn't count against StartLimitBurst)
        assert!(service_content.contains("SuccessExitStatus=42 43"));

        // Verify exit code 43 prevents restart (another instance already running)
        assert!(service_content.contains("RestartPreventExitStatus=43"));

        // Regression for #3967: stale-orphan self-heal pre-flight (system unit).
        assert!(
            service_content.contains("ExecStartPre=")
                && service_content.contains("Freenet version:")
                && service_content.contains("kill -TERM")
                && service_content.contains("kill -KILL"),
            "system systemd unit must run a stale-orphan version-compare-and-kill pre-flight (#3967)"
        );

        // Logging routes to journal (same reasoning as the user-unit test).
        assert!(service_content.contains("StandardOutput=journal"));
        assert!(service_content.contains("StandardError=journal"));
        assert!(
            !service_content.contains("append:"),
            "regression: must not append systemd output to fixed unrotated file (#4251)"
        );
    }

    #[test]
    #[cfg(target_os = "macos")]
    fn test_macos_wrapper_script_generation() {
        let binary_path = PathBuf::from("/usr/local/bin/freenet");
        let script = generate_wrapper_script(&binary_path);

        // Regression for issue #4251: lifecycle messages MUST go through
        // `logger -t freenet` (auto-rotated by macOS unified logging),
        // not appended to a fixed ~/Library/Logs/freenet/freenet.log file
        // that grows without bound.
        assert!(
            script.contains("logger -t freenet"),
            "wrapper must route lifecycle messages through logger(1)"
        );
        assert!(
            !script.contains("Library/Logs/freenet/freenet.log\""),
            "regression: wrapper must NOT append to fixed unrotated freenet.log (#4251)"
        );
        assert!(
            !script.contains(">> \"$LOG\""),
            "regression: wrapper must not use the legacy LOG file variable (#4251)"
        );

        // Regression for #3301: startup stale-process cleanup, scoped to current user
        assert!(
            script.contains("pkill -f -u \"$(id -u)\" \"freenet network\""),
            "wrapper must kill stale processes on startup, scoped to current user"
        );
        assert!(
            script.contains("sleep 2"),
            "wrapper must wait after startup kill to let the OS release the port"
        );

        // Port-conflict detection: stderr captured for grep inspection
        assert!(
            script.contains("freenet.error.log.last"),
            "wrapper must capture stderr to a scratch file for port-conflict detection"
        );
        assert!(
            script.contains("already in use"),
            "wrapper must detect port-already-in-use errors from stderr"
        );

        // Port-conflict recovery: capped kill-and-retry loop
        assert!(
            script.contains("PORT_CONFLICT_KILLS"),
            "wrapper must track port-conflict kill attempts"
        );
        assert!(
            script.contains("MAX_PORT_CONFLICT_KILLS"),
            "wrapper must cap port-conflict kill attempts to prevent infinite loops"
        );

        // Correct binary path embedded (binary path is quoted in the generated script)
        assert!(
            script.contains("\"/usr/local/bin/freenet\" network"),
            "wrapper must invoke the correct binary"
        );

        // Normal shutdown path
        assert!(
            script.contains("exit 0"),
            "wrapper must exit cleanly on normal shutdown"
        );

        // Update path (exit code 42)
        assert!(
            script.contains("EXIT_CODE -eq 42"),
            "wrapper must handle exit code 42 for auto-update"
        );
    }

    /// Regression for issue #3967: on exit 43 the wrapper must self-heal a
    /// STALE ORPHAN holding the service port instead of unconditionally
    /// standing down. Standing down (`exit 0`) under launchd
    /// SuccessfulExit=false means launchd never respawns us, so an orphaned
    /// `freenet network` running an OLD binary would serve stale assets
    /// forever. The fix compares the holder's `Freenet version:` line against
    /// the on-disk binary and kills a stale orphan (SIGTERM→SIGKILL) before
    /// relaunching, while still deferring to a genuine current-version
    /// instance.
    #[test]
    #[cfg(target_os = "macos")]
    fn test_macos_wrapper_exit43_self_heals_stale_orphan() {
        let binary_path = PathBuf::from("/usr/local/bin/freenet");
        let script = generate_wrapper_script(&binary_path);

        // The exit-43 branch must invoke the self-heal routine BEFORE the
        // polite `exit 0`. Without the fix the branch was just a log + exit 0.
        assert!(
            script.contains("heal_stale_orphan_or_defer"),
            "exit-43 path must call the stale-orphan self-heal routine"
        );

        // It must detect the port holder. We reuse the existing user-scoped
        // `freenet network` detection rather than needing the port plumbed in.
        assert!(
            script.contains("holder_pids")
                && script.contains("pgrep -f -u \"$(id -u)\" \"freenet network\""),
            "self-heal must enumerate the port holder process(es)"
        );

        // It must compare versions: query the holder binary's --version and
        // the on-disk binary's --version, matching the `Freenet version:` line.
        assert!(
            script.contains("--version") && script.contains("Freenet version:"),
            "self-heal must compare holder vs on-disk `Freenet version:` lines"
        );
        assert!(
            script.contains("ondisk_version"),
            "self-heal must compute the on-disk binary version for comparison"
        );

        // MUST-FIX #3: the kill is gated on the holder being an init-adopted
        // ORPHAN (PPID==1). A user-run `freenet network` (PPID!=1) is never
        // killed even on a version mismatch. The decision is `[ "$ppid" = "1" ]
        // && { mismatch || no-holder-version }`, so PPID==1 is a REQUIRED
        // conjunct, not a mere fallback.
        assert!(
            script.contains("[ \"$ppid\" = \"1\" ] && {"),
            "self-heal must REQUIRE PPID==1 (orphan) before killing, not just as a fallback"
        );

        // MUST-FIX #4: an unreadable on-disk version ($ondisk empty) must NOT
        // make every holder look mismatched. version_mismatch is only set when
        // ondisk is non-empty AND holder_ver is non-empty AND they differ.
        assert!(
            script.contains(
                "[ -n \"$ondisk\" ] && [ -n \"$holder_ver\" ] && [ \"$holder_ver\" != \"$ondisk\" ]"
            ),
            "version-mismatch kill signal must require a readable on-disk version (#4 guard)"
        );

        // Escalation: SIGTERM, wait, then SIGKILL (the incident's orphan
        // ignored SIGTERM for >11s and needed -9).
        assert!(
            script.contains("kill -TERM") && script.contains("kill -KILL"),
            "self-heal must escalate SIGTERM -> SIGKILL"
        );
        assert!(
            script.contains("kill -0"),
            "self-heal must poll for liveness between SIGTERM and SIGKILL"
        );

        // On a detected stale orphan it must RELAUNCH (continue), not exit 0.
        assert!(
            script.contains("if heal_stale_orphan_or_defer; then"),
            "exit-43 path must branch on the self-heal result"
        );
        let exit43_idx = script
            .find("EXIT_CODE -eq 43")
            .expect("exit-43 branch must exist");
        let exit43_block = &script[exit43_idx..];
        let continue_idx = exit43_block
            .find("continue")
            .expect("exit-43 path must relaunch (continue) on a killed stale orphan");
        let exit0_idx = exit43_block
            .find("exit 0")
            .expect("exit-43 path must still defer (exit 0) for a current-version instance");
        assert!(
            continue_idx < exit0_idx,
            "relaunch (continue) must come before the polite exit 0 in the exit-43 branch"
        );

        // Negative assertion (polite path preserved): a current-version holder,
        // a user-supervised holder, or one whose version can't be determined
        // still defers. The routine logs a defer and the branch falls through
        // to `exit 0`.
        assert!(
            script.contains("deferring to port holder PID"),
            "a current-version / user-supervised holder must still be deferred to (polite path preserved)"
        );

        // The own-child guard prevents mistaking our just-exited child for an
        // orphan: the wrapper tracks WRAPPER_CHILD_PID and skips it.
        assert!(
            script.contains("WRAPPER_CHILD_PID"),
            "self-heal must skip the wrapper's own child to avoid false positives"
        );

        // Load-bearing: the generated script must be syntactically valid bash.
        // If our heredoc escaping is wrong this catches it.
        let tmp = tempfile::tempdir().unwrap();
        let script_path = tmp.path().join("wrapper.sh");
        std::fs::write(&script_path, &script).unwrap();
        let status = std::process::Command::new("bash")
            .arg("-n")
            .arg(&script_path)
            .status()
            .expect("failed to run bash -n");
        assert!(
            status.success(),
            "generated wrapper script must be syntactically valid bash"
        );
    }

    /// MUST-FIX #1 regression: systemd performs its OWN `$VAR`/`${VAR}`
    /// expansion on `Exec*` lines BEFORE handing the string to `/bin/sh`, so
    /// every dollar the SHELL must see has to be written as `$$` in the unit
    /// file (systemd collapses `$$` -> `$`). A single `$ident` is silently
    /// eaten to empty and the whole self-heal becomes a no-op that LOOKS
    /// installed (the `-` prefix swallows the failure). The old
    /// `.contains("Freenet version:")` smoke tests could not catch that. This
    /// test renders BOTH units and asserts the on-disk `ExecStartPre` text uses
    /// the `$$`-doubled form and contains NO bare single-`$` shell identifier
    /// that systemd would strip, then collapses `$$` -> `$` (emulating systemd)
    /// and runs `sh -n` on the resulting script body.
    #[test]
    #[cfg(target_os = "linux")]
    fn test_systemd_execstartpre_doubles_shell_dollars() {
        let tmp = tempfile::tempdir().unwrap();
        let binary_path = PathBuf::from("/usr/local/bin/freenet");
        let log_dir = PathBuf::from("/home/test/.local/state/freenet");
        let username = "test";
        let home_dir = PathBuf::from("/home/test");

        let units = [
            ("user", generate_user_service_file(&binary_path, &log_dir)),
            (
                "system",
                generate_system_service_file(&binary_path, &log_dir, username, &home_dir),
            ),
        ];

        for (which, unit) in units {
            let pre = unit
                .lines()
                .find(|l| l.starts_with("ExecStartPre=-/bin/sh -c "))
                .unwrap_or_else(|| panic!("{which} unit must have a self-heal ExecStartPre"));

            // Post-render the file on disk MUST carry the systemd-level `$$`
            // form for the shell identifiers the pre-flight relies on.
            for needle in ["$$pid", "$$exe", "$$ondisk", "$$self", "$$(id -u)"] {
                assert!(
                    pre.contains(needle),
                    "{which} ExecStartPre must double shell dollars (missing `{needle}`); \
                     a single `$` is eaten by systemd's own expansion and the self-heal \
                     silently no-ops (#3967 MUST-FIX 1)"
                );
            }

            // And it must NOT contain the bare single-`$` forms that systemd
            // would strip to empty. These are the exact identifiers from the
            // pre-flight; if any appears single-`$` the escaping regressed.
            // NB: we cannot blanket-assert absence of `$(id -u)` because the
            // VALID doubled form `$$(id -u)` contains it as a substring; the
            // `$$(id -u)` presence check above covers that identifier instead.
            for bad in [
                "/proc/$pid/",
                "\"$pid\"",
                "\"$exe\"",
                "\"$ondisk\"",
                "\"$self\"",
            ] {
                assert!(
                    !pre.contains(bad),
                    "{which} ExecStartPre contains bare single-`$` `{bad}` that systemd \
                     would strip; it must be doubled to `$$` (#3967 MUST-FIX 1)"
                );
            }

            // MUST-FIX #6: PPID is parsed after the FINAL ')' of
            // /proc/PID/stat, not as a fixed awk field of the raw line (a comm
            // with whitespace would shift fields).
            assert!(
                pre.contains("sed \"s/.*) //\""),
                "{which} ExecStartPre must parse PPID after the final ')' in \
                 /proc/PID/stat, not by fixed field index (#3967 MUST-FIX 6)"
            );

            // MUST-FIX #5: the pre-flight must exclude its own sh PID (`$$$$`
            // -> the sh's `$$`) and PID 1. The pre-flight's own argv contains
            // the literal "freenet network" (the substring `pgrep -f` matches),
            // so the self-PID + PID-1 skips are what keep it from killing
            // itself.
            assert!(
                pre.contains("self=$$$$") && pre.contains("[ \"$$pid\" = \"$$self\" ] && continue"),
                "{which} ExecStartPre must skip its own sh PID (#3967 MUST-FIX 5)"
            );
            assert!(
                pre.contains("[ \"$$pid\" = \"1\" ] && continue"),
                "{which} ExecStartPre must skip PID 1 (#3967 MUST-FIX 5)"
            );
            // MUST-FIX (round-2 RE-REVIEW): the pre-flight must NOT anchor on
            // the holder's exe equalling this unit's on-disk binary. A #3967
            // orphan is by definition running an OLD/DIFFERENT binary, so an
            // `exe == on-disk binary` guard skips exactly the process we must
            // kill — defeating the whole self-heal. The earlier round-1 form
            // `[ -n "$$bin" ] && [ "$$exe" != "$$bin" ] && continue` is the
            // broken guard and must never come back.
            assert!(
                !pre.contains("[ \"$$exe\" != \"$$bin\" ]"),
                "{which} ExecStartPre must NOT anchor on exe == current on-disk \
                 binary; that guard skips the old-binary orphan it must kill \
                 (#3967 round-2 RE-REVIEW)"
            );

            // MUST-FIX #3/#4: kill is gated on PPID==1 AND (readable-version
            // mismatch OR unreadable holder version). Render-level check.
            assert!(
                pre.contains("[ \"$$ppid\" = \"1\" ] &&"),
                "{which} ExecStartPre must require PPID==1 before killing (#3967 MUST-FIX 3)"
            );

            // Now emulate systemd's `$$` -> `$` collapse and confirm the body
            // is valid POSIX sh. This is the load-bearing end-to-end check that
            // the escaping produces a runnable script (the bug shipped because
            // nothing rendered + lexed the post-systemd form).
            let on_disk = pre
                .strip_prefix("ExecStartPre=-/bin/sh -c '")
                .and_then(|s| s.strip_suffix('\''))
                .unwrap_or_else(|| panic!("{which} ExecStartPre quoting unexpected"));
            let sh_body = on_disk.replace("$$", "$");

            let body_path = tmp.path().join(format!("{which}_execstartpre.sh"));
            std::fs::write(&body_path, &sh_body).unwrap();
            let status = std::process::Command::new("sh")
                .arg("-n")
                .arg(&body_path)
                .status()
                .expect("failed to run sh -n");
            assert!(
                status.success(),
                "{which} ExecStartPre body must be valid POSIX sh after systemd \
                 collapses `$$` -> `$` (#3967 MUST-FIX 1)"
            );
        }
    }

    /// Behavioral regression for the exit-43 self-heal DECISION (#3967). The
    /// render/`sh -n` tests prove the script PARSES; this proves it DECIDES
    /// correctly. We extract the heal routine from the generated wrapper, point
    /// it at a REAL on-disk fake binary (so `ondisk_version` is non-empty), and
    /// shim `pgrep`/`ps`/`kill`/`readlink`/`timeout` plus a separate holder
    /// binary via PATH. Cases:
    ///   (a) version-mismatched PPID==1 orphan      -> KILLED (relaunch),
    ///   (b) same-version PPID==1 holder            -> DEFERRED (polite path),
    ///   (c) version-mismatched but PPID!=1 holder  -> DEFERRED (MUST-FIX #3:
    ///       never SIGKILL a hand-run / supervised node),
    ///   (d) version-mismatched orphan but UNREADABLE on-disk version
    ///       -> DEFERRED (MUST-FIX #4: don't cull a healthy node mid-update),
    ///   (e) the holder pid == our own child       -> SKIPPED / DEFERRED.
    ///
    /// macOS-only because it extracts the heal helpers from the macOS wrapper
    /// (`generate_wrapper_script`); the systemd inline path is covered by the
    /// render + `sh -n` test above.
    #[test]
    #[cfg(target_os = "macos")]
    fn test_exit43_heal_decision_behavior() {
        #[cfg(unix)]
        use std::os::unix::fs::PermissionsExt;

        let tmp = tempfile::tempdir().unwrap();
        let dir = tmp.path();

        let chmod_x = |p: &std::path::Path| {
            std::fs::set_permissions(p, std::fs::Permissions::from_mode(0o755)).unwrap();
        };

        // Real on-disk binary baked into the wrapper. Prints a FIXED version.
        // Its readability is toggled per-case via the ONDISK_READABLE env that
        // the script consults (see wrapper-version override below); when the
        // file itself is non-executable, ondisk_version() returns empty.
        let ondisk_bin = dir.join("freenet");
        std::fs::write(
            &ondisk_bin,
            "#!/bin/sh\n[ \"$1\" = \"--version\" ] && echo \"Freenet version: 1.0.0 (current)\"\nexit 0\n",
        )
        .unwrap();
        chmod_x(&ondisk_bin);

        // Holder binary: a DIFFERENT path whose version is env-controlled, so
        // we can drive match vs mismatch independently of the on-disk binary.
        let holder_bin = dir.join("holder-freenet");
        std::fs::write(
            &holder_bin,
            "#!/bin/sh\n[ \"$1\" = \"--version\" ] && echo \"Freenet version: ${HOLDER_VER}\"\nexit 0\n",
        )
        .unwrap();
        chmod_x(&holder_bin);

        // Extract the heal helpers from the REAL generated wrapper so the test
        // exercises rendered logic, not a copy. The wrapper bakes in the
        // on-disk binary path, so ondisk_version() points at `ondisk_bin`.
        let wrapper = generate_wrapper_script(&ondisk_bin);
        let start = wrapper
            .find("version_line() {")
            .expect("wrapper must define version_line");
        let end_marker = "# Kill any stale freenet network processes before starting.";
        let end = wrapper.find(end_marker).expect("wrapper layout changed");
        let helpers = &wrapper[start..end];

        // `kill` and `sleep` are shell builtins, so a PATH shim wouldn't be
        // consulted. Override them as shell FUNCTIONS (functions beat builtins)
        // so we can observe the kill and skip the real up-to-12s wait. `kill`
        // records SIGTERM/SIGKILL to a marker; `kill -0` reports the holder
        // alive until that marker exists (so the TERM->poll->KILL loop ends).
        let killed_marker = dir.join("killed");
        let harness = format!(
            r#"#!/bin/sh
log_event() {{ :; }}
sleep() {{ :; }}
kill() {{
  sig=""
  case "$1" in -*) sig="$1"; shift;; esac
  case "$sig" in
    -0) [ -f "{marker}" ] && return 1; return 0;;
    -TERM|-15|-KILL|-9) : > "{marker}"; return 0;;
    *) return 0;;
  esac
}}
WRAPPER_CHILD_PID=999999
{helpers}
heal_stale_orphan_or_defer
echo "RC=$?"
"#,
            marker = killed_marker.display()
        );
        let harness_path = dir.join("harness.sh");
        std::fs::write(&harness_path, harness).unwrap();
        chmod_x(&harness_path);

        // PATH shims. Holder pid fixed at 4242 (unless overridden by PGREP_PID).
        let bin = dir.join("bin");
        std::fs::create_dir(&bin).unwrap();
        let write_shim = |name: &str, body: String| {
            let p = bin.join(name);
            std::fs::write(&p, format!("#!/bin/sh\n{body}")).unwrap();
            chmod_x(&p);
        };

        // pgrep: emit the holder pid (env-overridable to test self-exclusion).
        write_shim("pgrep", "echo \"${PGREP_PID:-4242}\"\n".to_string());
        // ps: -o ppid= -> $HOLDER_PPID ; -o command=/comm= -> the holder's full
        // argv, which (like the real wrapper) is `<binary> network`. The
        // $HOLDER_BIN env lets a case drive a spaced install path so the
        // holder_exe sed-strip (not awk whitespace-split) is exercised. We emit
        // the ` network` suffix so the strip has something to remove, mirroring
        // production argv.
        write_shim(
            "ps",
            "for a in \"$@\"; do case \"$a\" in \
             ppid=) echo \"$HOLDER_PPID\"; exit 0;; \
             command=|comm=) echo \"$HOLDER_BIN network\"; exit 0;; esac; done\nexit 0\n"
                .to_string(),
        );
        // readlink -f: map any /proc/PID/exe to the holder binary; passthrough.
        write_shim(
            "readlink",
            format!(
                "last=\"\"; for a in \"$@\"; do last=\"$a\"; done; \
                 case \"$last\" in */proc/*/exe) echo \"{}\";; *) echo \"$last\";; esac\n",
                holder_bin.display()
            ),
        );
        // timeout: drop the leading duration arg and exec the rest.
        write_shim("timeout", "shift\nexec \"$@\"\n".to_string());

        let path_env = format!("{}:{}", bin.display(), std::env::var("PATH").unwrap());

        // Returns (stdout, holder_was_killed). `holder_bin_override` lets a case
        // point the holder at a DIFFERENT (e.g. space-containing) install path
        // so we exercise `holder_exe`'s suffix-strip; defaults to `holder_bin`.
        let default_holder_bin = holder_bin.display().to_string();
        let run = |holder_ver: &str,
                   holder_ppid: &str,
                   pgrep_pid: Option<&str>,
                   holder_bin_override: Option<&str>|
         -> (String, bool) {
            std::fs::remove_file(&killed_marker).ok();
            let mut cmd = std::process::Command::new("sh");
            cmd.arg(&harness_path)
                .env("PATH", &path_env)
                .env("HOLDER_VER", holder_ver)
                .env("HOLDER_PPID", holder_ppid)
                .env(
                    "HOLDER_BIN",
                    holder_bin_override.unwrap_or(&default_holder_bin),
                );
            if let Some(p) = pgrep_pid {
                cmd.env("PGREP_PID", p);
            }
            let out = cmd.output().expect("failed to run heal harness");
            let stdout = String::from_utf8_lossy(&out.stdout).to_string();
            (stdout, killed_marker.exists())
        };

        // (a) orphan (PPID==1) on an OLD version vs the current on-disk 1.0.0.
        // NOTE: holder_bin is a DIFFERENT path from the on-disk binary, so this
        // also locks the round-2 RE-REVIEW invariant: a holder whose resolved
        // exe path differs from the unit's on-disk binary is STILL a kill target
        // when it is an old-version PPID==1 orphan (the systemd exe==bin anchor
        // wrongly skipped exactly this case).
        let (out_a, killed_a) = run("0.9.0 (old)", "1", None, None);
        assert!(
            out_a.contains("RC=0") && killed_a,
            "version-mismatched PPID==1 orphan must be KILLED + relaunch (RC=0), got: {out_a}"
        );

        // (b) orphan (PPID==1) running the SAME current version -> defer.
        let (out_b, killed_b) = run("1.0.0 (current)", "1", None, None);
        assert!(
            out_b.contains("RC=1") && !killed_b,
            "same-version holder must be DEFERRED, not killed (polite path), got: {out_b}"
        );

        // (c) MUST-FIX #3: version-mismatched but USER-parented (PPID!=1) -> defer.
        let (out_c, killed_c) = run("0.9.0 (old)", "4321", None, None);
        assert!(
            out_c.contains("RC=1") && !killed_c,
            "version-mismatched but user-parented (PPID!=1) holder must be DEFERRED (#3967 MUST-FIX 3), got: {out_c}"
        );

        // (d) MUST-FIX #4: same mismatch + orphan, but on-disk version UNREADABLE.
        // Make the baked-in binary non-executable so ondisk_version() is empty;
        // the mismatch signal must then be suppressed and a readable holder
        // deferred to (don't cull a healthy node mid-update).
        std::fs::set_permissions(&ondisk_bin, std::fs::Permissions::from_mode(0o644)).unwrap();
        let (out_d, killed_d) = run("0.9.0 (old)", "1", None, None);
        assert!(
            out_d.contains("RC=1") && !killed_d,
            "unreadable on-disk version must suppress the mismatch kill signal (#3967 MUST-FIX 4), got: {out_d}"
        );
        chmod_x(&ondisk_bin); // restore

        // (e) self-exclusion: the only holder pid IS our own child -> skip/defer.
        let (out_e, killed_e) = run("0.9.0 (old)", "1", Some("999999"), None);
        assert!(
            out_e.contains("RC=1") && !killed_e,
            "the routine must never target its own child PID (#3967), got: {out_e}"
        );

        // (f) NIT 2 regression: an install path that contains a SPACE must still
        // resolve cleanly so version comparison works. `holder_exe` strips the
        // trailing ` network` argv rather than awk-splitting on whitespace; the
        // old `awk '{print $1}'` would have truncated this to `/Some` and made
        // the holder version unreadable. We place a real old-version binary at a
        // spaced path and assert the orphan is still KILLED (its version is read
        // and seen to differ), proving the path was recovered intact.
        let spaced_dir = dir.join("Some User").join("bin");
        std::fs::create_dir_all(&spaced_dir).unwrap();
        let spaced_holder = spaced_dir.join("freenet");
        std::fs::write(
            &spaced_holder,
            "#!/bin/sh\n[ \"$1\" = \"--version\" ] && echo \"Freenet version: 0.9.0 (old)\"\nexit 0\n",
        )
        .unwrap();
        chmod_x(&spaced_holder);
        let (out_f, killed_f) = run(
            "ignored-uses-real-binary",
            "1",
            None,
            Some(&spaced_holder.display().to_string()),
        );
        assert!(
            out_f.contains("RC=0") && killed_f,
            "old-version PPID==1 orphan at a space-containing install path must be \
             KILLED — holder_exe must recover the full path, not truncate at the \
             first space (NIT 2), got: {out_f}"
        );
    }

    /// Behavioral regression for the SYSTEMD inline self-heal DECISION (#3967,
    /// round-2 RE-REVIEW). The render + `sh -n` test proves the inline body
    /// PARSES; this proves it DECIDES correctly. Crucially it locks the round-2
    /// fix: the round-1 unit anchored the holder loop on
    /// `[ "$exe" != "$bin" ] && continue`, which skipped every holder whose exe
    /// differed from the unit's on-disk binary — i.e. exactly the stale orphan
    /// (running an OLD binary) the self-heal exists to kill. Here the fake
    /// holder's exe path is DIFFERENT from the on-disk binary, and we assert it
    /// IS killed.
    ///
    /// We extract the `ExecStartPre` `/bin/sh -c '...'` body from the rendered
    /// unit, collapse systemd's `$$` -> `$`, and run it under `sh` with
    /// PATH-shimmed `pgrep`/`readlink`/`timeout` and a `kill` shell function
    /// (functions beat the builtin) that records targets to a marker.
    ///
    /// Linux-only because it exercises the `/proc`-based systemd inline path
    /// and the unit generators are `#[cfg(target_os = "linux")]`.
    #[test]
    #[cfg(target_os = "linux")]
    fn test_systemd_execstartpre_heal_decision_behavior() {
        use std::os::unix::fs::PermissionsExt;

        let tmp = tempfile::tempdir().unwrap();
        let dir = tmp.path();
        let chmod_x = |p: &std::path::Path| {
            std::fs::set_permissions(p, std::fs::Permissions::from_mode(0o755)).unwrap();
        };

        // On-disk binary baked into the unit (the binary the unit would launch).
        // Prints a FIXED current version. Lives at one path...
        let ondisk_bin = dir.join("freenet");
        std::fs::write(
            &ondisk_bin,
            "#!/bin/sh\n[ \"$1\" = \"--version\" ] && echo \"Freenet version: 1.0.0 (current)\"\nexit 0\n",
        )
        .unwrap();
        chmod_x(&ondisk_bin);

        // ...the holder binary lives at a DIFFERENT path (this is the whole
        // point: a #3967 orphan runs an old/different binary). Its version is
        // env-controlled so we can drive match vs mismatch.
        let holder_bin = dir.join("old").join("freenet");
        std::fs::create_dir_all(holder_bin.parent().unwrap()).unwrap();
        std::fs::write(
            &holder_bin,
            "#!/bin/sh\n[ \"$1\" = \"--version\" ] && echo \"Freenet version: ${HOLDER_VER}\"\nexit 0\n",
        )
        .unwrap();
        chmod_x(&holder_bin);

        // Render the user unit and carve out the ExecStartPre `/bin/sh -c '...'`
        // body, then collapse `$$` -> `$` exactly as systemd does before exec.
        let log_dir = dir.join("logs");
        let unit = generate_user_service_file(&ondisk_bin, &log_dir);
        let pre = unit
            .lines()
            .find(|l| l.starts_with("ExecStartPre=-/bin/sh -c "))
            .expect("unit must have a self-heal ExecStartPre");
        let body = pre
            .strip_prefix("ExecStartPre=-/bin/sh -c '")
            .and_then(|s| s.strip_suffix('\''))
            .expect("ExecStartPre quoting unexpected")
            .replace("$$", "$");

        // PATH shims. `pgrep` emits the holder pid; `readlink -f /proc/PID/exe`
        // maps to the holder binary; `timeout` drops the duration and execs the
        // rest. `kill`/`sleep` are overridden as shell FUNCTIONS in the harness.
        let bin = dir.join("bin");
        std::fs::create_dir(&bin).unwrap();
        let write_shim = |name: &str, src: String| {
            let p = bin.join(name);
            std::fs::write(&p, format!("#!/bin/sh\n{src}")).unwrap();
            chmod_x(&p);
        };
        write_shim("pgrep", "echo \"${PGREP_PID:-4242}\"\n".to_string());
        // readlink: -f <on-disk binary path> passes through (so `bin=` would
        // resolve if it existed); /proc/PID/exe -> the holder binary; otherwise
        // echo the last arg. The exe!=bin path is what the round-1 anchor broke.
        write_shim(
            "readlink",
            format!(
                "last=\"\"; for a in \"$@\"; do last=\"$a\"; done; \
                 case \"$last\" in */proc/*/exe) echo \"{}\";; *) echo \"$last\";; esac\n",
                holder_bin.display()
            ),
        );
        write_shim("timeout", "shift\nexec \"$@\"\n".to_string());

        // `sed`/`awk`/`grep`/`id` come from the real PATH; the /proc/PID/stat
        // read is shimmed via a fake stat file the body reads. Replace the
        // `/proc/$pid/stat` literal with our fixture so PPID is controllable.
        let stat_file = dir.join("stat");
        // Format mirrors /proc/PID/stat: "PID (comm) STATE PPID ...". The body
        // does `sed "s/.*) //"` then `awk '{print $2}'`, so PPID must be field 2
        // AFTER the final ')'. We park a comm WITH spaces to also keep the
        // round-1 MUST-FIX 6 (whitespace-comm) honest.
        let body = body.replace("/proc/$pid/stat", &stat_file.display().to_string());
        // The readlink call also references /proc/$pid/exe; leave it so the
        // shim's */proc/*/exe arm fires.

        let killed_marker = dir.join("killed");
        let path_env = format!("{}:{}", bin.display(), std::env::var("PATH").unwrap());

        // Returns (holder_was_killed). HOLDER_PPID drives the fake stat file's
        // PPID field; HOLDER_VER drives the holder binary's version.
        let run = |holder_ver: &str, holder_ppid: &str, pgrep_pid: Option<&str>| -> bool {
            std::fs::remove_file(&killed_marker).ok();
            std::fs::write(
                &stat_file,
                format!("4242 (freenet network) S {holder_ppid} 1 1 0 -1\n"),
            )
            .unwrap();
            let harness = format!(
                "#!/bin/sh\nkill() {{ sig=\"\"; case \"$1\" in -*) sig=\"$1\"; shift;; esac; \
                 case \"$sig\" in -0) [ -f \"{marker}\" ] && return 1; return 0;; \
                 -TERM|-15|-KILL|-9) : > \"{marker}\"; return 0;; *) return 0;; esac; }}\n\
                 sleep() {{ :; }}\n{body}\n",
                marker = killed_marker.display(),
                body = body,
            );
            let harness_path = dir.join("harness.sh");
            std::fs::write(&harness_path, harness).unwrap();
            let mut cmd = std::process::Command::new("sh");
            cmd.arg(&harness_path)
                .env("PATH", &path_env)
                .env("HOLDER_VER", holder_ver);
            if let Some(p) = pgrep_pid {
                cmd.env("PGREP_PID", p);
            }
            cmd.output().expect("failed to run systemd heal harness");
            killed_marker.exists()
        };

        // (a) ROUND-2 CORE: orphan (PPID==1) running an OLD version at a
        // DIFFERENT exe path than the on-disk binary -> MUST be KILLED. The
        // round-1 `exe != bin` anchor skipped exactly this case.
        assert!(
            run("0.9.0 (old)", "1", None),
            "systemd self-heal must KILL an old-version PPID==1 orphan whose exe \
             path differs from the on-disk binary (round-2 RE-REVIEW: the \
             exe==bin anchor wrongly skipped it)"
        );

        // (b) orphan (PPID==1) running the SAME current version -> DEFER.
        assert!(
            !run("1.0.0 (current)", "1", None),
            "systemd self-heal must DEFER to a same-version holder (polite path)"
        );

        // (c) version-mismatched but USER-parented (PPID!=1) -> DEFER.
        assert!(
            !run("0.9.0 (old)", "4321", None),
            "systemd self-heal must DEFER to a user-parented (PPID!=1) holder (#3967 MUST-FIX 3)"
        );

        // (d) on-disk binary version UNREADABLE (`$ondisk` empty) -> DEFER even for
        // a PPID==1 orphan whose own version IS readable. With no trustworthy
        // baseline we must not version-compare; killing here would reap a healthy
        // current node during an update window when the binary is momentarily
        // unreadable. Mirrors the macOS test's chmod-644 case (#3967 MUST-FIX 4),
        // which the systemd path's `[ -n "$$ondisk" ]` guard implements but had no
        // behavioral coverage on Linux (rule-review Info on #4408).
        std::fs::set_permissions(&ondisk_bin, std::fs::Permissions::from_mode(0o644)).unwrap();
        assert!(
            !run("0.9.0 (old)", "1", None),
            "systemd self-heal must DEFER when the on-disk version is unreadable, even \
             for a readable-version PPID==1 orphan (#3967 MUST-FIX 4: empty $ondisk \
             must not drive a kill)"
        );
        chmod_x(&ondisk_bin); // restore for any later use
    }

    #[test]
    #[cfg(target_os = "macos")]
    fn test_launchd_plist_generation() {
        let binary_path = PathBuf::from("/usr/local/bin/freenet");
        let log_dir = PathBuf::from("/Users/test/Library/Logs/freenet");
        let plist_content = generate_plist(&binary_path, &log_dir);

        // Verify it's valid plist structure
        assert!(plist_content.contains("<?xml version=\"1.0\""));
        assert!(plist_content.contains("<plist version=\"1.0\">"));
        assert!(plist_content.contains("</plist>"));

        // Verify the label is set
        assert!(plist_content.contains("<string>org.freenet.node</string>"));

        // Verify it references the correct binary
        assert!(plist_content.contains("/usr/local/bin/freenet"));

        // Stdout/stderr are discarded; the tracing layer writes its own
        // size-capped rolling logs and `freenet service report` collects
        // them. Writing launchd output to a fixed freenet.log /
        // freenet.error.log caused unbounded growth (issue #4251).
        assert!(plist_content.contains("<string>/dev/null</string>"));
        assert!(
            !plist_content.contains("/Users/test/Library/Logs/freenet/freenet.log"),
            "regression: launchd must not write stdout to a fixed unrotated log file (#4251)"
        );
        assert!(
            !plist_content.contains("/Users/test/Library/Logs/freenet/freenet.error.log"),
            "regression: launchd must not write stderr to a fixed unrotated log file (#4251)"
        );

        // Verify resource limits are set
        assert!(plist_content.contains("<key>NumberOfFiles</key>"));
        assert!(plist_content.contains("<integer>65536</integer>"));

        // Verify RunAtLoad is set
        assert!(plist_content.contains("<key>RunAtLoad</key>"));
        assert!(plist_content.contains("<true/>"));
    }

    #[test]
    fn test_should_purge_with_purge_flag() {
        assert!(should_purge(true, false).unwrap());
    }

    #[test]
    fn test_should_purge_with_keep_data_flag() {
        assert!(!should_purge(false, true).unwrap());
    }

    #[test]
    fn test_should_purge_no_flags_non_tty() {
        // In CI/test environments stdin is not a TTY, so this should default to false
        assert!(!should_purge(false, false).unwrap());
    }

    // ── remove_dir_if_empty (Windows uninstall leftover cleanup, #3904) ──

    #[test]
    fn test_remove_dir_if_empty_removes_empty_dir() {
        let tmp = tempfile::tempdir().unwrap();
        let empty = tmp.path().join("empty");
        std::fs::create_dir(&empty).unwrap();

        remove_dir_if_empty(&empty);

        assert!(!empty.exists(), "empty directory should have been removed");
    }

    #[test]
    fn test_remove_dir_if_empty_preserves_non_empty_dir() {
        // The whole point of this helper: if someone else dropped a file into
        // the parent (an unrelated app under `The Freenet Project Inc\`, or a
        // user-authored config file), we must NOT wipe it out. Any "looks
        // close to empty" heuristic would be catastrophic here.
        let tmp = tempfile::tempdir().unwrap();
        let nonempty = tmp.path().join("nonempty");
        std::fs::create_dir(&nonempty).unwrap();
        std::fs::write(nonempty.join("other_app.txt"), b"do not delete").unwrap();

        remove_dir_if_empty(&nonempty);

        assert!(nonempty.exists(), "non-empty directory must be preserved");
        assert!(
            nonempty.join("other_app.txt").exists(),
            "foreign files inside the parent must not be touched",
        );
    }

    #[test]
    fn test_remove_dir_if_empty_preserves_dir_with_subtree() {
        // Variant of the safety test above, but with a full subdirectory
        // tree instead of a single file. `read_dir().next().is_some()` must
        // treat a subdir as "non-empty" — we never want to pretend a parent
        // is empty just because all its direct children are directories.
        let tmp = tempfile::tempdir().unwrap();
        let project_parent = tmp.path().join("The Freenet Project Inc");
        let sibling_app = project_parent.join("OtherApp");
        let sibling_data = sibling_app.join("state");
        std::fs::create_dir_all(&sibling_data).unwrap();
        std::fs::write(sibling_data.join("state.bin"), b"unrelated").unwrap();

        remove_dir_if_empty(&project_parent);

        assert!(
            project_parent.exists(),
            "parent with a sibling app subtree must be preserved",
        );
        assert!(
            sibling_data.join("state.bin").exists(),
            "sibling app's data must not be touched",
        );
    }

    #[test]
    fn test_remove_dir_if_empty_noop_on_missing_path() {
        // The caller often passes a parent path that may or may not exist
        // (because ProjectDirs invents canonical locations even on systems
        // that have never installed Freenet). We must not error in that case.
        let tmp = tempfile::tempdir().unwrap();
        let missing = tmp.path().join("never-existed");

        // Must not panic and must not create the directory.
        remove_dir_if_empty(&missing);

        assert!(!missing.exists());
    }

    #[test]
    fn test_remove_dir_if_empty_noop_on_file() {
        // If someone's config lives at a path that happens to be a file and
        // not a directory, silently do nothing rather than erroring.
        let tmp = tempfile::tempdir().unwrap();
        let file = tmp.path().join("notadir");
        std::fs::write(&file, b"contents").unwrap();

        remove_dir_if_empty(&file);

        assert!(file.exists(), "regular file must be left alone");
    }

    #[test]
    fn test_remove_dir_if_empty_collapses_after_children_removed() {
        // Simulates the #3904 scenario: after `remove_if_exists` takes out
        // each of `data`, `config`, `cache` under a `Freenet\` parent, the
        // now-empty `Freenet\` parent must collapse.
        let tmp = tempfile::tempdir().unwrap();
        let parent = tmp.path().join("Freenet");
        let config = parent.join("config");
        std::fs::create_dir_all(&config).unwrap();
        std::fs::write(config.join("config.toml"), b"stuff").unwrap();

        // Recursive child removal then empty-parent cleanup, mirroring
        // purge_data_dirs' new structure.
        std::fs::remove_dir_all(&config).unwrap();
        remove_dir_if_empty(&parent);

        assert!(!parent.exists(), "parent should collapse once empty");
    }

    // ── purge_leaves_and_collapse integration (the call-site wiring) ──

    /// Build a `DataLeaves` under `base` that mirrors the Windows
    /// `%LOCALAPPDATA%` + `%APPDATA%` layout, except with tempdir-rooted
    /// paths so the tests run on any OS.
    fn seed_windows_like_layout(base: &Path) -> DataLeaves {
        let project_local = base.join("Local").join("The Freenet Project Inc");
        let project_roaming = base.join("Roaming").join("The Freenet Project Inc");
        let freenet_local = project_local.join("Freenet");
        let freenet_roaming = project_roaming.join("Freenet");

        let data_local = freenet_local.join("data");
        let data_roaming = freenet_roaming.join("data");
        let config = freenet_roaming.join("config");
        let config_local = freenet_local.join("config");
        let cache = freenet_local.join("cache");
        let log = base.join("Local").join("freenet").join("logs");

        for d in [
            &data_local,
            &data_roaming,
            &config,
            &config_local,
            &cache,
            &log,
        ] {
            std::fs::create_dir_all(d).unwrap();
            std::fs::write(d.join("placeholder.bin"), b"x").unwrap();
        }

        DataLeaves {
            data_local: Some(data_local),
            data_roaming: Some(data_roaming),
            config: Some(config),
            config_local: Some(config_local),
            cache: Some(cache),
            cache_lowercase: None,
            log: Some(log),
            collapse_parents: true,
        }
    }

    #[test]
    fn test_purge_leaves_and_collapse_cleans_windows_layout() {
        // This is the #3904 regression test proper: it exercises the full
        // call chain (DataLeaves → remove_if_exists per leaf → parent
        // collection → deepest-first collapse), not just the helper.
        // Reverting the parent collection or the `remove_dir_if_empty`
        // calls inside `purge_leaves_and_collapse` would cause this to fail.
        let tmp = tempfile::tempdir().unwrap();
        let leaves = seed_windows_like_layout(tmp.path());
        let freenet_local = leaves
            .data_local
            .as_ref()
            .unwrap()
            .parent()
            .unwrap()
            .to_path_buf();
        let freenet_roaming = leaves
            .config
            .as_ref()
            .unwrap()
            .parent()
            .unwrap()
            .to_path_buf();
        let log_parent = leaves.log.as_ref().unwrap().parent().unwrap().to_path_buf();

        purge_leaves_and_collapse(&leaves).unwrap();

        // Every leaf is gone.
        for leaf in [
            leaves.data_local.as_ref(),
            leaves.data_roaming.as_ref(),
            leaves.config.as_ref(),
            leaves.config_local.as_ref(),
            leaves.cache.as_ref(),
            leaves.log.as_ref(),
        ]
        .into_iter()
        .flatten()
        {
            assert!(!leaf.exists(), "leaf {} should be removed", leaf.display());
        }

        // The Freenet-owned parents collapsed...
        assert!(
            !freenet_local.exists(),
            "Local/.../Freenet/ should collapse"
        );
        assert!(
            !freenet_roaming.exists(),
            "Roaming/.../Freenet/ should collapse"
        );
        assert!(!log_parent.exists(), "Local/freenet/ should collapse");
    }

    #[test]
    fn test_purge_leaves_preserves_sibling_app_grandparent() {
        // Safety: `The Freenet Project Inc\` holds `Freenet\` (ours) and
        // could hold `OtherApp\` (unrelated). Only `Freenet\` is expendable.
        let tmp = tempfile::tempdir().unwrap();
        let leaves = seed_windows_like_layout(tmp.path());

        // Plant a sibling app next to our Freenet folder.
        let sibling = tmp
            .path()
            .join("Roaming")
            .join("The Freenet Project Inc")
            .join("OtherApp")
            .join("state.bin");
        std::fs::create_dir_all(sibling.parent().unwrap()).unwrap();
        std::fs::write(&sibling, b"dont touch").unwrap();

        purge_leaves_and_collapse(&leaves).unwrap();

        // Sibling and its grandparent both preserved.
        assert!(sibling.exists(), "sibling app data must survive");
        assert!(
            sibling.parent().unwrap().exists(),
            "sibling app directory must survive",
        );
        assert!(
            sibling.parent().unwrap().parent().unwrap().exists(),
            "`The Freenet Project Inc` grandparent must survive",
        );
    }

    #[test]
    fn test_purge_leaves_never_collapses_shared_parents_on_unix() {
        // Reproduces the codex-caught bug: on Linux `ProjectDirs` builds
        // `~/.local/share/freenet`, `~/.config/freenet`, `~/.cache/freenet`
        // (lowercase `freenet`; macOS uses its own `~/Library/...` scheme),
        // whose parents are shared across every app on the system. Collapsing
        // any of them, even when the `remove_dir_if_empty` non-empty check
        // makes it safe in practice on an active system, would wipe a shared
        // XDG root on a fresh account where Freenet was the only inhabitant.
        // The `collapse_parents: false` setting must suppress every collapse.
        // (The leaf basenames below are arbitrary — this test exercises the
        // parent-collapse suppression, not the leaf-name resolution.)
        let tmp = tempfile::tempdir().unwrap();

        // Each leaf sits in its own dedicated parent to verify each
        // collapse path independently.
        let shared_share = tmp.path().join("share");
        let shared_config = tmp.path().join("config");
        let shared_cache = tmp.path().join("cache");
        let shared_logs = tmp.path().join("state");

        let data = shared_share.join("Freenet");
        let config = shared_config.join("Freenet");
        let cache = shared_cache.join("Freenet");
        let log = shared_logs.join("freenet");

        for d in [&data, &config, &cache, &log] {
            std::fs::create_dir_all(d).unwrap();
            std::fs::write(d.join("a"), b"x").unwrap();
        }

        let leaves = DataLeaves {
            data_local: Some(data.clone()),
            data_roaming: None,
            config: Some(config.clone()),
            config_local: None,
            cache: Some(cache.clone()),
            cache_lowercase: None,
            log: Some(log.clone()),
            collapse_parents: false,
        };

        purge_leaves_and_collapse(&leaves).unwrap();

        // Every leaf removed.
        for leaf in [&data, &config, &cache, &log] {
            assert!(!leaf.exists(), "leaf {} should be removed", leaf.display());
        }

        // Every shared parent preserved — this is what protects
        // `~/.local/share/`, `~/.config/`, `~/.cache/`, `~/.local/state/`
        // on real systems.
        for parent in [&shared_share, &shared_config, &shared_cache, &shared_logs] {
            assert!(
                parent.exists(),
                "shared parent {} must NOT be collapsed on Unix",
                parent.display(),
            );
        }
    }

    #[test]
    fn test_purge_leaves_tolerates_missing_optional_leaves() {
        // Not every platform populates every leaf — an install that never
        // wrote to Roaming (legacy Windows) or never produced a
        // lowercase-cache variant still needs to clean up what IS there.
        let tmp = tempfile::tempdir().unwrap();
        let data_local = tmp.path().join("data");
        std::fs::create_dir_all(&data_local).unwrap();
        std::fs::write(data_local.join("a"), b"x").unwrap();

        let leaves = DataLeaves {
            data_local: Some(data_local.clone()),
            data_roaming: None,
            config: None,
            config_local: None,
            cache: None,
            cache_lowercase: None,
            log: None,
            collapse_parents: false,
        };

        purge_leaves_and_collapse(&leaves).unwrap();

        assert!(!data_local.exists(), "leaf should be removed");
    }

    #[test]
    fn test_purge_leaves_removes_config_in_local_app_data() {
        // Regression for the Windows-uninstall config-leftover bug
        // (#3904 follow-up, addressed in PR #3969): on Windows the
        // running node writes config to `config_local_dir()` (Local
        // AppData), not `config_dir()` (Roaming) — see `Config::build`
        // in config.rs which uses `defaults.config_local_dir()`. The
        // pre-fix `purge_leaves_and_collapse` only knew about the
        // Roaming `config` leaf, so it left
        // `%LOCALAPPDATA%\The Freenet Project Inc\Freenet\config\`
        // behind on every Windows uninstall.
        let tmp = tempfile::tempdir().unwrap();
        let freenet_local = tmp
            .path()
            .join("Local")
            .join("The Freenet Project Inc")
            .join("Freenet");
        let config_local = freenet_local.join("config");
        std::fs::create_dir_all(&config_local).unwrap();
        std::fs::write(config_local.join("config.toml"), b"x").unwrap();

        let leaves = DataLeaves {
            data_local: None,
            data_roaming: None,
            config: None,
            config_local: Some(config_local.clone()),
            cache: None,
            cache_lowercase: None,
            log: None,
            collapse_parents: true,
        };

        purge_leaves_and_collapse(&leaves).unwrap();

        assert!(
            !config_local.exists(),
            "config_local leaf must be removed (the #3904 follow-up regression)",
        );
        assert!(
            !freenet_local.exists(),
            "the now-empty Freenet parent should also collapse",
        );
    }

    /// On Linux/macOS the `directories` crate aliases `config_local_dir`
    /// to `config_dir`, so `DataLeaves::from_project_dirs()` must leave
    /// `config_local` empty after the dedup branches at
    /// `from_project_dirs` reject it. If a future refactor reorders the
    /// dedup conditions and lets `config_local` be `Some` on Linux, the
    /// purge code path would attempt to remove `~/.config/Freenet` a
    /// second time (harmless via `remove_if_exists`, but masks a real
    /// regression). This test pins the invariant.
    #[cfg(any(target_os = "linux", target_os = "macos"))]
    #[test]
    fn test_from_project_dirs_leaves_config_local_empty_on_unix() {
        let leaves = DataLeaves::from_project_dirs();
        assert!(
            leaves.config_local.is_none(),
            "config_local must alias config on Linux/macOS to avoid double removal; got {:?}",
            leaves.config_local,
        );
        assert!(
            !leaves.collapse_parents,
            "collapse_parents must stay false off Windows so shared XDG roots are never collapsed",
        );
    }

    /// Regression for #3907: `freenet uninstall --purge --system` on Linux
    /// resolves the service user's data via a hardcoded XDG layout. The leaf
    /// directories MUST be lowercase `freenet`, matching what the running node
    /// actually creates — `ProjectDirs::from("", "The Freenet Project Inc",
    /// "Freenet")` lowercases the application name on Linux, and
    /// `get_log_dir()` uses `~/.local/state/freenet`. The pre-fix code
    /// hardcoded uppercase `Freenet` for data/config/cache, so the purge
    /// matched nothing on disk and returned "success" while leaving every
    /// contract, delegate, and the database behind.
    #[cfg(target_os = "linux")]
    #[test]
    fn test_system_purge_uses_lowercase_freenet_paths() {
        let home = PathBuf::from("/home/svc");
        let dirs = linux_system_purge_dirs(&home);

        // Pin the exact leaves. Each entry must be lowercase `freenet`; an
        // uppercase `Freenet` (the #3907 bug) would never match the directories
        // the node created.
        assert_eq!(
            dirs,
            [
                ("data", home.join(".local/share/freenet")),
                ("config", home.join(".config/freenet")),
                ("cache", home.join(".cache/freenet")),
                ("logs", home.join(".local/state/freenet")),
            ],
            "system-mode purge must target lowercase `freenet` XDG dirs (#3907)",
        );

        // Defensive: assert no leaf contains the uppercase project name, which
        // is what the directories crate never produces on Linux.
        for (_label, dir) in &dirs {
            assert!(
                !dir.to_string_lossy().contains("/Freenet"),
                "leaf {} must not use uppercase `Freenet` (#3907)",
                dir.display(),
            );
        }
    }

    /// End-to-end check that the resolved leaves actually remove on-disk data
    /// seeded at the lowercase paths a real Linux install uses. This exercises
    /// the same `linux_system_purge_dirs` → `remove_if_exists` chain that
    /// `purge_data_dirs` runs in `--system` mode, but rooted at a tempdir so it
    /// never touches a real home. With the pre-fix uppercase paths the seeded
    /// lowercase directories would survive and this test would fail.
    #[cfg(target_os = "linux")]
    #[test]
    fn test_system_purge_removes_seeded_lowercase_data() {
        let tmp = tempfile::tempdir().unwrap();
        let home = tmp.path();

        let data = home.join(".local/share/freenet");
        let config = home.join(".config/freenet");
        let cache = home.join(".cache/freenet");
        let logs = home.join(".local/state/freenet");

        // Seed the directories with realistic contents the purge must remove.
        std::fs::create_dir_all(data.join("contracts")).unwrap();
        std::fs::create_dir_all(data.join("delegates")).unwrap();
        std::fs::create_dir_all(data.join("db")).unwrap();
        std::fs::write(data.join("db").join("freenet.redb"), b"x").unwrap();
        std::fs::create_dir_all(&config).unwrap();
        std::fs::write(config.join("config.toml"), b"x").unwrap();
        std::fs::create_dir_all(&cache).unwrap();
        std::fs::create_dir_all(&logs).unwrap();
        std::fs::write(logs.join("freenet.2026-06-02.log"), b"x").unwrap();

        for (label, dir) in linux_system_purge_dirs(home) {
            remove_if_exists(label, &dir).unwrap();
        }

        for leaf in [&data, &config, &cache, &logs] {
            assert!(
                !leaf.exists(),
                "system purge must remove lowercase leaf {} (#3907)",
                leaf.display(),
            );
        }
    }

    // ── Wrapper backoff state machine tests ──

    #[test]
    fn test_wrapper_exit_42_update_success_resets_state() {
        let mut state = WrapperState::new();
        state.consecutive_failures = 3;
        state.backoff_secs = 80;

        let action = next_wrapper_action(&mut state, 42, false, Some(true));
        assert_eq!(action, WrapperAction::Update);
        assert_eq!(state.consecutive_failures, 0);
        assert_eq!(state.backoff_secs, WRAPPER_INITIAL_BACKOFF_SECS);
        assert_eq!(state.port_conflict_kills, 0);
    }

    #[test]
    fn test_wrapper_exit_42_update_failure_backs_off() {
        let mut state = WrapperState::new();
        assert_eq!(state.backoff_secs, 10);

        let action = next_wrapper_action(&mut state, 42, false, Some(false));
        assert_eq!(action, WrapperAction::BackoffAndRelaunch { secs: 10 });
        assert_eq!(state.consecutive_failures, 1);
        assert_eq!(state.backoff_secs, 20); // doubled

        let action = next_wrapper_action(&mut state, 42, false, Some(false));
        assert_eq!(action, WrapperAction::BackoffAndRelaunch { secs: 20 });
        assert_eq!(state.backoff_secs, 40);
    }

    #[test]
    fn test_wrapper_exit_43_exits_immediately() {
        let mut state = WrapperState::new();
        let action = next_wrapper_action(&mut state, 43, false, None);
        assert_eq!(action, WrapperAction::Exit);
    }

    #[test]
    fn test_wrapper_exit_0_exits_cleanly() {
        let mut state = WrapperState::new();
        let action = next_wrapper_action(&mut state, 0, false, None);
        assert_eq!(action, WrapperAction::Exit);
    }

    #[test]
    fn test_wrapper_crash_exponential_backoff_with_cap() {
        let mut state = WrapperState::new();

        // First crash: 10s
        let action = next_wrapper_action(&mut state, 1, false, None);
        assert_eq!(action, WrapperAction::BackoffAndRelaunch { secs: 10 });
        assert_eq!(state.backoff_secs, 20);

        // Second: 20s
        let action = next_wrapper_action(&mut state, 1, false, None);
        assert_eq!(action, WrapperAction::BackoffAndRelaunch { secs: 20 });
        assert_eq!(state.backoff_secs, 40);

        // Keep doubling...
        let action = next_wrapper_action(&mut state, 1, false, None);
        assert_eq!(action, WrapperAction::BackoffAndRelaunch { secs: 40 });
        assert_eq!(state.backoff_secs, 80);

        let action = next_wrapper_action(&mut state, 1, false, None);
        assert_eq!(action, WrapperAction::BackoffAndRelaunch { secs: 80 });
        assert_eq!(state.backoff_secs, 160);

        let action = next_wrapper_action(&mut state, 1, false, None);
        assert_eq!(action, WrapperAction::BackoffAndRelaunch { secs: 160 });
        assert_eq!(state.backoff_secs, 300); // capped at MAX

        // Should stay capped
        let action = next_wrapper_action(&mut state, 1, false, None);
        assert_eq!(action, WrapperAction::BackoffAndRelaunch { secs: 300 });
        assert_eq!(state.backoff_secs, 300);

        assert_eq!(state.consecutive_failures, 6);
    }

    #[test]
    fn test_wrapper_port_conflict_kill_and_retry() {
        let mut state = WrapperState::new();

        // First port conflict: kill and retry
        let action = next_wrapper_action(&mut state, 1, true, None);
        assert_eq!(action, WrapperAction::KillAndRetry);
        assert_eq!(state.port_conflict_kills, 1);
        assert_eq!(state.backoff_secs, WRAPPER_INITIAL_BACKOFF_SECS);

        // Second: still kill and retry
        let action = next_wrapper_action(&mut state, 1, true, None);
        assert_eq!(action, WrapperAction::KillAndRetry);
        assert_eq!(state.port_conflict_kills, 2);

        // Third: still kill and retry (at the limit)
        let action = next_wrapper_action(&mut state, 1, true, None);
        assert_eq!(action, WrapperAction::KillAndRetry);
        assert_eq!(state.port_conflict_kills, 3);

        // Fourth: exceeds limit, falls through to backoff
        let action = next_wrapper_action(&mut state, 1, true, None);
        assert_eq!(action, WrapperAction::BackoffAndRelaunch { secs: 10 });
        assert_eq!(state.port_conflict_kills, 0); // reset on fallthrough
        assert_eq!(state.consecutive_failures, 1);
    }

    #[test]
    fn test_wrapper_port_conflict_resets_backoff() {
        let mut state = WrapperState::new();
        state.backoff_secs = 160; // simulate previous crashes

        let action = next_wrapper_action(&mut state, 1, true, None);
        assert_eq!(action, WrapperAction::KillAndRetry);
        assert_eq!(state.backoff_secs, WRAPPER_INITIAL_BACKOFF_SECS);
    }

    // ── Stuck-wrapper detection tests (#4382) ──

    #[test]
    fn test_stuck_fires_once_at_threshold_on_repeated_identical_failure() {
        let mut state = WrapperState::new();
        // First two identical crashes: below threshold, no stuck signal.
        for expected_streak in 1..WRAPPER_STUCK_NOTIFY_THRESHOLD {
            next_wrapper_action(&mut state, 1, false, None);
            assert!(
                !state.stuck_just_crossed,
                "should not fire at streak {expected_streak}"
            );
            assert_eq!(state.identical_failure_streak, expected_streak);
        }
        // The Nth identical crash crosses the threshold exactly once.
        next_wrapper_action(&mut state, 1, false, None);
        assert!(state.stuck_just_crossed, "must fire at threshold");
        assert_eq!(
            state.identical_failure_streak,
            WRAPPER_STUCK_NOTIFY_THRESHOLD
        );
        // Subsequent identical crashes must NOT re-fire (once per episode).
        next_wrapper_action(&mut state, 1, false, None);
        assert!(!state.stuck_just_crossed, "must not re-fire after crossing");
        assert_eq!(
            state.identical_failure_streak,
            WRAPPER_STUCK_NOTIFY_THRESHOLD + 1
        );
    }

    #[test]
    fn test_stuck_streak_resets_when_failure_cause_changes() {
        let mut state = WrapperState::new();
        // Two crashes with code 1, then a different code resets the streak.
        next_wrapper_action(&mut state, 1, false, None);
        next_wrapper_action(&mut state, 1, false, None);
        assert_eq!(state.identical_failure_streak, 2);
        next_wrapper_action(&mut state, 7, false, None);
        assert_eq!(state.identical_failure_streak, 1, "new cause resets streak");
        assert!(!state.stuck_just_crossed);
    }

    #[test]
    fn test_stuck_signature_distinguishes_port_conflict() {
        let mut state = WrapperState::new();
        // Same exit code but different port-conflict flag = different signature.
        next_wrapper_action(&mut state, 1, true, None);
        next_wrapper_action(&mut state, 1, true, None);
        assert_eq!(state.identical_failure_streak, 2);
        next_wrapper_action(&mut state, 1, false, None);
        assert_eq!(
            state.identical_failure_streak, 1,
            "port-conflict flag is part of the signature"
        );
    }

    #[test]
    fn test_stuck_port_conflict_loop_fires_at_threshold() {
        // Repeated exit-43-style port conflicts (the #3967 stale-orphan
        // signature) must trip the detector even though the early KillAndRetry
        // path returns before the generic backoff arm.
        let mut state = WrapperState::new();
        let mut fired = 0;
        for _ in 0..WRAPPER_STUCK_NOTIFY_THRESHOLD {
            next_wrapper_action(&mut state, 1, true, None);
            if state.stuck_just_crossed {
                fired += 1;
            }
        }
        assert_eq!(fired, 1, "stuck signal fires exactly once for port loop");
    }

    #[test]
    fn test_stuck_streak_cleared_by_clean_exit() {
        let mut state = WrapperState::new();
        next_wrapper_action(&mut state, 1, false, None);
        next_wrapper_action(&mut state, 1, false, None);
        assert_eq!(state.identical_failure_streak, 2);
        // Clean exit (code 0) resets the streak so a later failure starts fresh.
        next_wrapper_action(&mut state, 0, false, None);
        assert_eq!(state.identical_failure_streak, 0);
        assert!(state.last_failure_signature.is_none());
    }

    #[test]
    fn test_stuck_streak_cleared_by_successful_update() {
        let mut state = WrapperState::new();
        next_wrapper_action(&mut state, 1, false, None);
        next_wrapper_action(&mut state, 1, false, None);
        next_wrapper_action(&mut state, 42, false, Some(true));
        assert_eq!(state.identical_failure_streak, 0);
        assert!(state.last_failure_signature.is_none());
    }

    #[test]
    fn test_stuck_status_file_roundtrip_and_clear() {
        let tmp = tempfile::tempdir().unwrap();
        let status = StuckWrapperStatus::new(43, true, WRAPPER_STUCK_NOTIFY_THRESHOLD);
        write_stuck_status_file(tmp.path(), &status);

        let path = tmp.path().join(STUCK_STATUS_FILE_NAME);
        assert!(path.exists(), "status file must be written");
        let json = std::fs::read_to_string(&path).unwrap();
        let parsed: StuckWrapperStatus = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed, status);
        assert_eq!(parsed.failure_count, WRAPPER_STUCK_NOTIFY_THRESHOLD);
        assert!(parsed.is_port_conflict);
        assert!(
            parsed.recovery_hint.contains("freenet service stop"),
            "hint must give the recovery command"
        );

        // Clearing removes the file; clearing again (missing file) is a no-op.
        clear_stuck_status_file(tmp.path());
        assert!(!path.exists(), "status file must be cleared on recovery");
        clear_stuck_status_file(tmp.path());
    }

    #[test]
    fn test_stuck_status_messages_differ_by_cause() {
        let port = StuckWrapperStatus::new(43, true, 3);
        assert!(port.last_error.contains("port"));
        assert!(port.recovery_hint.contains("port"));

        let crash = StuckWrapperStatus::new(101, false, 3);
        assert!(!crash.is_port_conflict);
        assert!(crash.last_error.contains("exit 101"));
        assert!(crash.recovery_hint.contains("crashing"));
    }

    #[test]
    fn test_applescript_escape_quotes_and_backslashes() {
        assert_eq!(applescript_escape("plain"), "plain");
        assert_eq!(applescript_escape("a\"b"), "a\\\"b");
        assert_eq!(applescript_escape("a\\b"), "a\\\\b");
        // Backslash escaped before quote so an injected `\"` can't break out.
        assert_eq!(applescript_escape("\\\""), "\\\\\\\"");
    }

    // ── Exit-43 / cross-process persistence tests (#4382 review BLOCKING) ──

    #[test]
    fn test_exit_43_records_port_conflict_signature() {
        // The DOMINANT #3967 manifestation: the child sees a stale orphan on the
        // port and exits 43. Pre-fix this arm recorded NO signature; now it must
        // record a port-conflict signature so the detector can see it. Even
        // though the child stderr lacks "already in use" (is_port_conflict=false
        // from the caller), the recorded signature must force the port-conflict
        // flag for honest messaging.
        let mut state = WrapperState::new();
        let action = next_wrapper_action(&mut state, 43, false, None);
        assert_eq!(action, WrapperAction::Exit);
        assert_eq!(
            state.last_failure_signature,
            Some(FailureSignature {
                exit_code: 43,
                is_port_conflict: true,
            }),
            "exit 43 must record a forced port-conflict signature"
        );
        assert_eq!(state.identical_failure_streak, 1);
    }

    #[test]
    fn test_persistent_streak_accumulates_and_fires_once_at_threshold() {
        // Simulate N consecutive exit-43 *relaunches*, each a fresh process
        // (prior loaded from disk, in-memory state irrelevant). Only the
        // relaunch whose count reaches the threshold fires.
        let mut prior: Option<PersistentStuckStreak> = None;
        let mut fired = 0;
        for expected in 1..=(WRAPPER_STUCK_NOTIFY_THRESHOLD + 2) {
            let (record, just_crossed) =
                persistent_streak_action(prior.as_ref(), 43, true, "1.2.3", 1000 + expected as u64);
            assert_eq!(record.count, expected);
            if just_crossed {
                fired += 1;
                assert_eq!(expected, WRAPPER_STUCK_NOTIFY_THRESHOLD);
            }
            prior = Some(record);
        }
        assert_eq!(fired, 1, "fires exactly once across relaunches");
    }

    #[test]
    fn test_persistent_streak_resets_on_cause_change() {
        let prior = PersistentStuckStreak {
            exit_code: 43,
            is_port_conflict: true,
            version: "1.2.3".to_string(),
            count: 2,
            updated_unix_secs: 1000,
        };
        // Different exit code = different cause = fresh episode.
        let (record, just_crossed) =
            persistent_streak_action(Some(&prior), 101, false, "1.2.3", 1001);
        assert_eq!(record.count, 1, "cause change resets streak");
        assert!(!just_crossed);
    }

    #[test]
    fn test_persistent_streak_resets_on_version_change() {
        let prior = PersistentStuckStreak {
            exit_code: 43,
            is_port_conflict: true,
            version: "1.2.3".to_string(),
            count: WRAPPER_STUCK_NOTIFY_THRESHOLD - 1,
            updated_unix_secs: 1000,
        };
        // New binary version = different binary = fresh episode (must NOT cross
        // the threshold off a stale prior version's count).
        let (record, just_crossed) =
            persistent_streak_action(Some(&prior), 43, true, "1.2.4", 1001);
        assert_eq!(record.count, 1, "version change resets streak");
        assert!(!just_crossed);
    }

    #[test]
    fn test_persistent_streak_resets_on_ttl_expiry() {
        let prior = PersistentStuckStreak {
            exit_code: 43,
            is_port_conflict: true,
            version: "1.2.3".to_string(),
            count: WRAPPER_STUCK_NOTIFY_THRESHOLD - 1,
            updated_unix_secs: 1000,
        };
        // Same cause + version but the prior record is older than the TTL: a
        // stale file must NOT count toward the threshold (GC/TTL discipline).
        let now = 1000 + STUCK_STREAK_TTL_SECS + 1;
        let (record, just_crossed) = persistent_streak_action(Some(&prior), 43, true, "1.2.3", now);
        assert_eq!(record.count, 1, "stale streak (past TTL) resets");
        assert!(!just_crossed);

        // Exactly at the TTL boundary still counts as continuing.
        let at_boundary = 1000 + STUCK_STREAK_TTL_SECS;
        let (record, _) = persistent_streak_action(Some(&prior), 43, true, "1.2.3", at_boundary);
        assert_eq!(
            record.count, WRAPPER_STUCK_NOTIFY_THRESHOLD,
            "boundary continues"
        );
    }

    #[test]
    fn test_persistent_streak_file_roundtrip_and_clear() {
        let tmp = tempfile::tempdir().unwrap();
        assert!(
            read_stuck_streak_file(tmp.path()).is_none(),
            "missing file reads as no prior streak"
        );
        let record = PersistentStuckStreak {
            exit_code: 43,
            is_port_conflict: true,
            version: "1.2.3".to_string(),
            count: 2,
            updated_unix_secs: 1234,
        };
        write_stuck_streak_file(tmp.path(), &record);
        let read = read_stuck_streak_file(tmp.path()).expect("must read back");
        assert_eq!(read, record);

        clear_stuck_streak_file(tmp.path());
        assert!(
            read_stuck_streak_file(tmp.path()).is_none(),
            "cleared file reads as none"
        );
        // Clearing a missing file is a no-op.
        clear_stuck_streak_file(tmp.path());
    }

    #[test]
    fn test_read_stuck_streak_file_tolerates_malformed_json() {
        let tmp = tempfile::tempdir().unwrap();
        std::fs::write(tmp.path().join(STUCK_STREAK_FILE_NAME), "{not json").unwrap();
        assert!(
            read_stuck_streak_file(tmp.path()).is_none(),
            "malformed file must read as no prior streak, not panic"
        );
    }

    // ── stuck_loop_action pure-decision tests (#4382 review NIT) ──

    #[test]
    fn test_stuck_loop_action_writes_on_threshold_cross() {
        // Write takes precedence regardless of the other flags.
        assert_eq!(
            stuck_loop_action(true, false, WRAPPER_STUCK_NOTIFY_THRESHOLD),
            StuckLoopAction::Write
        );
        assert_eq!(stuck_loop_action(true, true, 0), StuckLoopAction::Write);
    }

    #[test]
    fn test_stuck_loop_action_clears_on_recovery() {
        // streak reset to 0 (clean exit / successful update) → clear.
        assert_eq!(stuck_loop_action(false, false, 0), StuckLoopAction::Clear);
    }

    #[test]
    fn test_stuck_loop_action_clears_on_cause_change_midloop() {
        // The #4382 review MINOR: cause changes mid-episode, streak==1, not a
        // recovery — the stale file for the prior cause must still be cleared.
        assert_eq!(stuck_loop_action(false, true, 1), StuckLoopAction::Clear);
    }

    #[test]
    fn test_stuck_loop_action_none_when_progressing_same_cause() {
        // Below threshold, same cause, no change: leave the file alone.
        assert_eq!(stuck_loop_action(false, false, 1), StuckLoopAction::None);
        assert_eq!(stuck_loop_action(false, false, 2), StuckLoopAction::None);
    }

    #[test]
    fn test_signature_change_flag_set_only_on_change_not_first_failure() {
        let mut state = WrapperState::new();
        // First failure: None -> Some, no stale file to clear.
        next_wrapper_action(&mut state, 1, false, None);
        assert!(!state.signature_changed, "first failure is not a change");
        // Same cause again: still no change.
        next_wrapper_action(&mut state, 1, false, None);
        assert!(!state.signature_changed);
        // Different cause: change flagged.
        next_wrapper_action(&mut state, 7, false, None);
        assert!(state.signature_changed, "cause change must flag");
        assert_eq!(state.identical_failure_streak, 1);
    }

    /// Regression test for #3716: tray actions were silently dropped during
    /// backoff sleep. Verify the sleep function maps each action correctly.
    #[test]
    #[cfg(any(target_os = "windows", target_os = "macos"))]
    fn test_backoff_sleep_handles_tray_actions() {
        use super::super::tray::TrayAction;
        use std::sync::mpsc;

        let send_and_check = |action: TrayAction| -> BackoffInterrupt {
            let (tx, rx) = mpsc::channel();
            tx.send(action).unwrap();
            sleep_with_jitter_interruptible(1, Some(&rx))
        };

        assert!(matches!(
            send_and_check(TrayAction::Start),
            BackoffInterrupt::Relaunch
        ));
        assert!(matches!(
            send_and_check(TrayAction::Restart),
            BackoffInterrupt::Relaunch
        ));
        assert!(matches!(
            send_and_check(TrayAction::CheckUpdate),
            BackoffInterrupt::CheckUpdate
        ));
        assert!(matches!(
            send_and_check(TrayAction::Quit),
            BackoffInterrupt::Quit
        ));

        // No action: sleep completes normally
        let (_tx, rx) = mpsc::channel::<TrayAction>();
        assert!(matches!(
            sleep_with_jitter_interruptible(1, Some(&rx)),
            BackoffInterrupt::Completed
        ));
    }

    /// Regression test for #3717: `freenet service install` used to call
    /// `config.build()` which triggered a remote gateway fetch. This fails
    /// on fresh installs when the network isn't ready. Verify that
    /// `ConfigPathsArgs::build()` succeeds independently without network.
    #[test]
    fn test_config_paths_build_succeeds_without_network() {
        // Provide an explicit temp data dir to avoid the debug_assertions temp
        // dir path (which hits an unreachable! in debug builds). In production
        // (release builds), default_dirs returns ProjectDirs and works fine.
        let tmp = tempfile::tempdir().unwrap();
        let args = freenet::config::ConfigPathsArgs {
            config_dir: Some(tmp.path().join("config")),
            data_dir: Some(tmp.path().join("data")),
            ..Default::default()
        };
        let result = args.build(None);
        assert!(
            result.is_ok(),
            "ConfigPathsArgs::build should succeed without network access"
        );
    }

    /// Regression test for #3716: `wait_for_network_ready` must be
    /// interruptible by a Quit action from the tray. Uses a non-resolvable
    /// probe address to force entry into the retry loop, then verifies the
    /// Quit action is consumed and returns `false`.
    #[test]
    #[cfg(any(target_os = "windows", target_os = "macos"))]
    fn test_network_ready_quit_during_wait() {
        use std::sync::mpsc;

        let tmp = tempfile::tempdir().unwrap();
        let (tx, rx) = mpsc::channel::<super::super::tray::TrayAction>();
        // Pre-load Quit so it's found on the first channel check
        tx.send(super::super::tray::TrayAction::Quit).unwrap();

        // Use a non-resolvable address to force the retry loop.
        // The function should find the Quit action after the first sleep
        // and return false without waiting for the full timeout.
        let result = wait_for_network_ready_inner(tmp.path(), "nonexistent.invalid:1", Some(&rx));
        assert!(
            !result,
            "Should return false when Quit is received during wait"
        );
    }

    #[test]
    fn test_find_latest_log_file_picks_newest() {
        use std::fs;
        use std::io::Write;

        let tmp = tempfile::tempdir().unwrap();

        // Create some log files with different modification times
        let old = tmp.path().join("freenet.2025-01-01.log");
        let new = tmp.path().join("freenet.2025-12-31.log");
        let unrelated = tmp.path().join("other.log");

        fs::write(&old, "old").unwrap();
        // Ensure different mtime
        std::thread::sleep(std::time::Duration::from_millis(50));
        let mut f = fs::File::create(&new).unwrap();
        f.write_all(b"new").unwrap();
        fs::write(&unrelated, "unrelated").unwrap();

        let result = find_latest_log_file(tmp.path(), "freenet");
        assert_eq!(result, Some(new));
    }

    #[test]
    fn test_find_latest_log_file_skips_empty_static_file() {
        use std::fs;

        let tmp = tempfile::tempdir().unwrap();

        // Empty static file should be skipped
        fs::write(tmp.path().join("freenet.log"), "").unwrap();
        // Rotated file with content should be found
        std::thread::sleep(std::time::Duration::from_millis(50));
        let rotated = tmp.path().join("freenet.2025-12-31.log");
        fs::write(&rotated, "content").unwrap();

        let result = find_latest_log_file(tmp.path(), "freenet");
        assert_eq!(result, Some(rotated));
    }

    #[test]
    fn test_find_latest_log_file_no_matching_files() {
        let tmp = tempfile::tempdir().unwrap();

        // No files at all
        assert_eq!(find_latest_log_file(tmp.path(), "freenet"), None);

        // Unrelated files only
        std::fs::write(tmp.path().join("other.log"), "data").unwrap();
        assert_eq!(find_latest_log_file(tmp.path(), "freenet"), None);
    }

    #[test]
    fn test_find_latest_log_file_static_wins_over_older_rotated() {
        use std::fs;

        let tmp = tempfile::tempdir().unwrap();

        // Old rotated file
        let rotated = tmp.path().join("freenet.2025-01-01.log");
        fs::write(&rotated, "old").unwrap();

        // Newer static file (e.g., from systemd)
        std::thread::sleep(std::time::Duration::from_millis(50));
        let static_file = tmp.path().join("freenet.log");
        fs::write(&static_file, "newer").unwrap();

        let result = find_latest_log_file(tmp.path(), "freenet");
        assert_eq!(result, Some(static_file));
    }

    #[test]
    fn test_find_latest_log_file_detects_rotation() {
        use std::fs;

        let tmp = tempfile::tempdir().unwrap();

        // Simulate hour 14 log file
        let hour14 = tmp.path().join("freenet.2025-12-31-14.log");
        fs::write(&hour14, "hour 14 data").unwrap();

        // Initially finds hour 14
        let result = find_latest_log_file(tmp.path(), "freenet");
        assert_eq!(result, Some(hour14.clone()));

        // Simulate rotation: hour 15 file appears with newer mtime
        std::thread::sleep(std::time::Duration::from_millis(50));
        let hour15 = tmp.path().join("freenet.2025-12-31-15.log");
        fs::write(&hour15, "hour 15 data").unwrap();

        // Now finds hour 15 — this is the rotation detection mechanism
        let result = find_latest_log_file(tmp.path(), "freenet");
        assert_eq!(result, Some(hour15));
    }

    /// Source-level regression pin for #3933 / #3934.
    ///
    /// The Windows-only failure mode — `Command::spawn()` returning
    /// "The handle is invalid" (os error 6) when the parent has called
    /// `FreeConsole()` and no explicit `Stdio::null()` is set — cannot
    /// be exercised in a portable unit test. This pin instead enforces
    /// that the guard remains present in the source: if a future
    /// refactor removes the `.stdin/.stdout/.stderr(Stdio::null())`
    /// lines from `spawn_update_command`, the test fails with a
    /// specific error message pointing at the relevant issues rather
    /// than silently shipping the regression to Windows users.
    #[test]
    fn spawn_update_command_must_null_all_three_standard_handles() {
        let src = include_str!("service/wrapper.rs");
        let (_, after_fn_start) = src
            .split_once("fn spawn_update_command(")
            .expect("spawn_update_command definition not found");
        // Limit to the function body — stop at the next top-level fn
        // so trailing source doesn't accidentally pass the check.
        let (body, _) = after_fn_start
            .split_once("\nfn ")
            .expect("could not locate end of spawn_update_command");
        let code_only: String = body
            .lines()
            .map(|line| line.split_once("//").map(|(c, _)| c).unwrap_or(line))
            .collect::<Vec<_>>()
            .join("\n");
        for handle in ["stdin", "stdout", "stderr"] {
            let pattern = format!(".{handle}(std::process::Stdio::null())");
            assert!(
                code_only.contains(&pattern),
                "spawn_update_command must call `{}` — without it, Windows \
                 autostart fires a silent spawn failure with os error 6 \
                 (#3933 / #3934). The fix pattern matches the network-child \
                 spawn in run_wrapper_loop.",
                pattern
            );
        }
    }

    /// #4073 crash-loop auto-rollback (cross-platform source-scrape pins).
    ///
    /// The supervisor → updater handoff that drives rollback is platform-gated
    /// (systemd unit / macOS shell wrapper / in-process wrapper), so these
    /// pins assert the wiring exists in the source on every CI platform rather
    /// than only where the cfg'd code compiles.
    #[test]
    fn macos_wrapper_passes_node_exit_code_to_post_stop_update() {
        let src = include_str!("service/macos.rs");
        // The wrapper script must run `freenet update` with the node's exit code
        // exported (only on the exit-42 update/crash branch), and bind the env
        // name from the rollback module's constant.
        assert!(
            src.contains("{post_stop_env}=$EXIT_CODE \"{binary}\" update --quiet"),
            "macOS wrapper must pass the node exit code to its post-stop \
             `freenet update` for crash-loop auto-rollback (#4073)"
        );
        assert!(
            src.contains("post_stop_env = super::super::rollback::POST_STOP_EXIT_CODE_ENV_VAR"),
            "macOS wrapper must source the post-stop env var name from the \
             rollback module so the two cannot drift (#4073)"
        );
    }

    #[test]
    fn in_process_wrapper_only_passes_exit_code_on_post_crash_update() {
        let src = include_str!("service/wrapper.rs");
        // The post-crash auto-update path passes the node's exit code.
        assert!(
            src.contains("spawn_update_command(&exe_path, Some(exit_code))"),
            "the wrapper's post-stop auto-update must pass Some(exit_code) so \
             `freenet update` can drive crash-loop auto-rollback (#4073)"
        );
        // Manual / tray "Check for Updates" paths must NOT pass an exit code —
        // they are not crashes and must never be counted toward a rollback.
        assert!(
            src.contains("spawn_update_command(&exe_path, None)"),
            "manual tray update paths must pass None so they are not counted as \
             probation crashes (#4073)"
        );
        assert!(
            src.contains("POST_STOP_EXIT_CODE_ENV_VAR"),
            "spawn_update_command must set the post-stop env var from the \
             rollback module constant (#4073)"
        );
    }

    /// Source-level regression pin for the `FreeConsole()` rule, mirroring
    /// `spawn_update_command_must_null_all_three_standard_handles`.
    ///
    /// `taskkill_pid` spawns a child via `Command::status()`, which inherits
    /// the parent's standard handles unless they are explicitly nulled. It is
    /// reachable from `kill_stale_freenet_processes` (run by the wrapper right
    /// after `FreeConsole()`) and from the auto-update restart path. If a
    /// future refactor drops the `.stdin/.stdout/.stderr(Stdio::null())`
    /// lines, this fails loudly instead of shipping a silent os-error-6 spawn
    /// failure to Windows.
    #[test]
    fn taskkill_pid_must_null_all_three_standard_handles() {
        let src = include_str!("service/windows.rs");
        let (_, after_fn_start) = src
            .split_once("fn taskkill_pid(")
            .expect("taskkill_pid definition not found");
        // Locate the end of taskkill_pid by finding the next top-level
        // function. In the submodule file the next fn may carry a pub
        // visibility modifier, so try all common patterns in order.
        let body = after_fn_start
            .split_once("\npub(super) fn ")
            .or_else(|| after_fn_start.split_once("\npub(crate) fn "))
            .or_else(|| after_fn_start.split_once("\npub fn "))
            .or_else(|| after_fn_start.split_once("\nfn "))
            .map(|(b, _)| b)
            .expect("could not locate end of taskkill_pid");
        let code_only: String = body
            .lines()
            .map(|line| line.split_once("//").map(|(c, _)| c).unwrap_or(line))
            .collect::<Vec<_>>()
            .join("\n");
        for handle in ["stdin", "stdout", "stderr"] {
            let pattern = format!(".{handle}(std::process::Stdio::null())");
            assert!(
                code_only.contains(&pattern),
                "taskkill_pid must call `{}` — without it, the `taskkill` \
                 spawn fails with os error 6 after the wrapper's \
                 FreeConsole() (#3933 / #3934).",
                pattern
            );
        }
    }

    /// Regression test for #4290: macOS uninstall must remove the wrapper
    /// script plus its `.sh.hash` sidecar (#4286) and any `.sh.bak` backup
    /// left by `freenet update`. Previously install wrote three files and
    /// uninstall removed zero, leaving stale artifacts in `~/.local/bin`.
    #[cfg(target_os = "macos")]
    #[test]
    fn remove_wrapper_files_removes_script_sidecar_and_backup() {
        use std::fs;

        let tmp = tempfile::TempDir::new().expect("create temp dir");
        let home = tmp.path();

        let wrapper_path = wrapper_script_path(home);
        let bin_dir = wrapper_path.parent().unwrap();
        fs::create_dir_all(bin_dir).expect("create .local/bin");

        let hash_path = wrapper_path.with_extension("sh.hash");
        let bak_path = wrapper_path.with_extension("sh.bak");

        // Stand-in for the launchd plist that the wrapper cleanup must NOT touch.
        let plist_path = home.join("org.freenet.node.plist");

        for path in [&wrapper_path, &hash_path, &bak_path, &plist_path] {
            fs::write(path, b"x" as &[u8]).expect("write fixture file");
            assert!(path.exists(), "fixture {} should exist", path.display());
        }

        remove_wrapper_files(&wrapper_path).expect("cleanup should succeed");

        assert!(!wrapper_path.exists(), "wrapper script must be removed");
        assert!(!hash_path.exists(), "hash sidecar must be removed");
        assert!(!bak_path.exists(), "bak backup must be removed");
        // The cleanup helper is scoped to wrapper artifacts only.
        assert!(
            plist_path.exists(),
            "unrelated files (plist) must be left untouched"
        );
    }

    /// Edge case for #4290: missing files are not an error — uninstalling
    /// twice, or uninstalling an install that never ran an update (no
    /// `.sh.bak`), must still succeed.
    #[cfg(target_os = "macos")]
    #[test]
    fn remove_wrapper_files_is_idempotent_when_files_absent() {
        use std::fs;

        let tmp = tempfile::TempDir::new().expect("create temp dir");
        let home = tmp.path();

        let wrapper_path = wrapper_script_path(home);
        let bin_dir = wrapper_path.parent().unwrap();
        fs::create_dir_all(bin_dir).expect("create .local/bin");

        // Only the script exists; no sidecar, no backup.
        fs::write(&wrapper_path, b"x").expect("write wrapper");

        // First removal clears the script.
        remove_wrapper_files(&wrapper_path).expect("first cleanup should succeed");
        assert!(!wrapper_path.exists());

        // Second removal with every target already gone is still Ok.
        remove_wrapper_files(&wrapper_path).expect("second cleanup should be a no-op");
    }

    /// Regression test for the #4290 bug *site* (not just the leaf helper):
    /// `stop_and_remove_service` must clean up the wrapper artifacts even when
    /// the launchd plist is already gone. The original bug was a missing call
    /// at the uninstall edge; a follow-up review found the cleanup was placed
    /// AFTER an early `return Ok(false)` taken when the plist is absent, so a
    /// partial or repeated uninstall still leaked the wrapper files. This test
    /// drives the actual uninstall entry point (via the home-parametrized core)
    /// with the plist absent and asserts the artifacts are removed anyway.
    #[cfg(target_os = "macos")]
    #[test]
    fn stop_and_remove_service_cleans_wrapper_when_plist_absent() {
        use std::fs;

        let tmp = tempfile::TempDir::new().expect("create temp dir");
        let home = tmp.path();

        let wrapper_path = wrapper_script_path(home);
        let bin_dir = wrapper_path.parent().unwrap();
        fs::create_dir_all(bin_dir).expect("create .local/bin");

        let hash_path = wrapper_path.with_extension("sh.hash");
        let bak_path = wrapper_path.with_extension("sh.bak");
        for path in [&wrapper_path, &hash_path, &bak_path] {
            fs::write(path, b"x").expect("write fixture file");
        }

        // No plist exists under this home: the uninstall hits the early-return
        // path. Pre-fix, that returned before wrapper cleanup ran.
        let plist_path = home.join("Library/LaunchAgents/org.freenet.node.plist");
        assert!(!plist_path.exists(), "plist must be absent for this case");

        let found = stop_and_remove_service_at(home).expect("uninstall should succeed");

        assert!(
            !found,
            "no service was present, so it must report not-found"
        );
        assert!(
            !wrapper_path.exists(),
            "wrapper script must be removed even when the plist is already gone"
        );
        assert!(!hash_path.exists(), "hash sidecar must be removed");
        assert!(!bak_path.exists(), "bak backup must be removed");
    }

    // Regression for #3967: `freenet service doctor` must exist as a parseable
    // subcommand (both user and --system forms). It is the one-shot recovery
    // for a wedged install that `restart` cannot perform.
    #[test]
    fn service_doctor_subcommand_parses() {
        use clap::Parser;

        #[derive(Parser)]
        struct TestCli {
            #[command(subcommand)]
            cmd: ServiceCommand,
        }

        let user = TestCli::try_parse_from(["freenet", "doctor"]).expect("`doctor` must parse");
        assert!(
            matches!(user.cmd, ServiceCommand::Doctor { system: false }),
            "bare `doctor` must be the user-mode variant"
        );

        let sys = TestCli::try_parse_from(["freenet", "doctor", "--system"])
            .expect("`doctor --system` must parse");
        assert!(
            matches!(sys.cmd, ServiceCommand::Doctor { system: true }),
            "`doctor --system` must set system=true"
        );
    }

    // Behavioral regression for the doctor reap step (#3967): the original
    // incident's orphan ignored SIGTERM for >11s and needed SIGKILL. This proves
    // `kill_pattern_escalating` actually escalates: a child that traps and
    // ignores SIGTERM must still be reaped (via SIGKILL) and counted as a
    // survivor. Uses a real child whose argv carries a unique marker so the
    // user-scoped `pgrep -f` matches exactly this process and nothing else.
    #[test]
    #[cfg(unix)]
    fn kill_pattern_escalating_force_kills_sigterm_ignorer() {
        use std::time::Duration;

        let uid = std::process::Command::new("id")
            .arg("-u")
            .output()
            .ok()
            .and_then(|o| String::from_utf8(o.stdout).ok())
            .unwrap_or_default();
        let uid = uid.trim().to_string();

        // Unique marker in the command line so pgrep -f matches only this child.
        let marker = format!("freenet-doctor-test-{}", std::process::id());
        // `trap '' TERM` makes the child ignore SIGTERM entirely; it only dies on
        // SIGKILL. The marker is embedded in the script text (a `:` no-op with the
        // marker as an argument) so it is unambiguously part of the command line
        // `pgrep -f` inspects, regardless of how the OS reports argv[0].
        // Trailing `; :` defeats the shell's exec-the-last-command optimization,
        // so the trap-installing `sh` stays the live process (an exec would
        // replace it with bare `sleep`, dropping both the TERM trap and the
        // marker from argv).
        let mut child = std::process::Command::new("sh")
            .arg("-c")
            .arg(format!("trap '' TERM; : {marker}; sleep 60; :"))
            .spawn()
            .expect("spawn sigterm-ignoring child");

        // Give it a moment to install the trap and be visible to pgrep.
        std::thread::sleep(Duration::from_millis(500));
        assert!(
            !pids_matching(&uid, &marker).is_empty(),
            "test child must be discoverable via pgrep -f before reaping"
        );

        // Short grace window (2s) keeps the test fast; the child ignores SIGTERM
        // so escalation to SIGKILL is the only thing that can reap it.
        let escalated = kill_pattern_escalating(&uid, &marker, 2);

        assert_eq!(
            escalated, 1,
            "the SIGTERM-ignoring child must be counted as a force-killed survivor"
        );
        assert!(
            pids_matching(&uid, &marker).is_empty(),
            "the SIGTERM-ignoring child must be gone after SIGKILL escalation"
        );
        // Reap the zombie so we don't leak it.
        child.wait().ok();
    }

    // Complementary case: a child that exits promptly on SIGTERM must be reaped
    // WITHOUT escalation (escalated count == 0), confirming we don't SIGKILL
    // (or miscount) a well-behaved process that drains on the polite signal.
    #[test]
    #[cfg(unix)]
    fn kill_pattern_escalating_no_escalation_for_graceful_exit() {
        use std::time::Duration;

        let uid = std::process::Command::new("id")
            .arg("-u")
            .output()
            .ok()
            .and_then(|o| String::from_utf8(o.stdout).ok())
            .unwrap_or_default();
        let uid = uid.trim().to_string();

        let marker = format!("freenet-doctor-graceful-{}", std::process::id());
        // No TERM trap: default disposition terminates the process on SIGTERM.
        let mut child = std::process::Command::new("sh")
            .arg("-c")
            .arg(format!(": {marker}; sleep 60; :"))
            .spawn()
            .expect("spawn graceful child");

        std::thread::sleep(Duration::from_millis(500));
        assert!(
            !pids_matching(&uid, &marker).is_empty(),
            "graceful test child must be discoverable before reaping"
        );

        let escalated = kill_pattern_escalating(&uid, &marker, 12);
        assert_eq!(
            escalated, 0,
            "a child that exits on SIGTERM must not need SIGKILL escalation"
        );
        assert!(
            pids_matching(&uid, &marker).is_empty(),
            "graceful child must be gone after SIGTERM"
        );
        child.wait().ok();
    }
}
