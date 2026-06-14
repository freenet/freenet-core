//! System service management for Freenet.
//!
//! Supports:
//! - Linux: systemd user service (default) or system-wide service (--system)
//! - macOS: launchd user agent

use anyhow::{Context, Result};
use clap::Subcommand;
use directories::ProjectDirs;
use freenet::config::ConfigPaths;
use freenet::tracing::tracer::get_log_dir;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use super::report::ReportCommand;

/// Tail log files with automatic rotation detection.
///
/// Spawns `tail -f` on the latest log file, then periodically checks (every 5s)
/// whether a newer log file has appeared. When rotation occurs (e.g., hourly
/// tracing-appender rotation), kills the old `tail` and starts a new one on
/// the new file.
#[cfg(any(target_os = "linux", target_os = "macos"))]
fn tail_with_rotation(log_dir: &Path, base_name: &str) -> Result<()> {
    use std::time::Duration;

    let mut current_log = find_latest_log_file(log_dir, base_name).ok_or_else(|| {
        anyhow::anyhow!(
            "No log files found in: {}\nMake sure the service has been installed and started.",
            log_dir.display()
        )
    })?;

    println!("Following logs from: {}", current_log.display());
    println!("Press Ctrl+C to stop.\n");

    loop {
        let mut child = std::process::Command::new("tail")
            .arg("-f")
            .arg(&current_log)
            .spawn()
            .context("Failed to spawn tail")?;

        // Poll for newer log files every 5 seconds
        loop {
            match child.try_wait() {
                Ok(Some(status)) => {
                    // tail exited (user Ctrl+C or error)
                    std::process::exit(status.code().unwrap_or(1));
                }
                Ok(None) => {
                    // tail still running, check for rotation
                }
                Err(e) => {
                    drop(child.kill());
                    drop(child.wait());
                    anyhow::bail!("Error waiting on tail process: {e}");
                }
            }

            std::thread::sleep(Duration::from_secs(5));

            if let Some(newer_log) = find_latest_log_file(log_dir, base_name) {
                if newer_log != current_log {
                    println!("\n--- Log rotated to: {} ---\n", newer_log.display());
                    drop(child.kill());
                    drop(child.wait());
                    current_log = newer_log;
                    break; // break inner loop to spawn new tail
                }
            }
        }
    }
}

/// Find the latest log file in the given directory.
/// Handles both static files (e.g., "freenet.log" from systemd) and
/// rotated files (e.g., "freenet.2025-12-27.log" from tracing-appender).
/// Returns the most recently modified file.
pub(super) fn find_latest_log_file(log_dir: &Path, base_name: &str) -> Option<std::path::PathBuf> {
    use std::fs;

    let mut candidates: Vec<(std::path::PathBuf, std::time::SystemTime)> = Vec::new();

    // Check for the static file (used by systemd StandardOutput)
    let static_file = log_dir.join(format!("{base_name}.log"));
    if static_file.exists() {
        if let Ok(metadata) = fs::metadata(&static_file) {
            if metadata.len() > 0 {
                if let Ok(modified) = metadata.modified() {
                    candidates.push((static_file, modified));
                }
            }
        }
    }

    // Look for rotated files (pattern: {base_name}.YYYY-MM-DD.log)
    if let Ok(entries) = fs::read_dir(log_dir) {
        for entry in entries.filter_map(|e| e.ok()) {
            let name = entry.file_name();
            let name_str = name.to_string_lossy();
            // Match pattern: {base_name}.YYYY-MM-DD.log
            if name_str.starts_with(&format!("{base_name}."))
                && name_str.ends_with(".log")
                && name_str.len() > format!("{base_name}..log").len()
            {
                if let Ok(metadata) = entry.metadata() {
                    if let Ok(modified) = metadata.modified() {
                        candidates.push((entry.path(), modified));
                    }
                }
            }
        }
    }

    // Return the most recently modified file
    candidates
        .into_iter()
        .max_by_key(|(_, modified)| *modified)
        .map(|(path, _)| path)
}

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
fn first_run_marker_path() -> Option<PathBuf> {
    dirs::data_local_dir().map(|d| d.join("freenet").join(".first-run-complete"))
}

#[allow(dead_code)]
fn is_first_run_at(marker: &Path) -> bool {
    !marker.exists()
}

#[allow(dead_code)]
fn mark_first_run_complete_at(marker: &Path) -> std::io::Result<()> {
    if let Some(parent) = marker.parent() {
        std::fs::create_dir_all(parent)?;
    }
    std::fs::File::create(marker).map(|_| ())
}

#[cfg(any(target_os = "windows", target_os = "macos"))]
fn dashboard_port_is_listening() -> bool {
    use std::net::TcpStream;
    use std::time::Duration;
    let Ok(addr) = DASHBOARD_ADDR.parse::<std::net::SocketAddr>() else {
        return false;
    };
    TcpStream::connect_timeout(&addr, Duration::from_millis(500)).is_ok()
}

// ── macOS wrapper single-instance lock ──
//
// Problem: launchd's LAL agent fires RunAtLoad while the user's
// open-launched wrapper is still alive, and spawns a second wrapper.
// The backend-level single-instance guard
// (EXIT_CODE_ALREADY_RUNNING = 43) catches the second wrapper's
// DAEMON CHILD but by the time that exit propagates, the second
// wrapper's tao event loop is already running on the main thread and
// owns an NSStatusItem. Two rabbits.
//
// Port-based detection of "another wrapper is running" is fragile:
//   - wrapper A's backend may be mid-crash or mid-backoff, in which
//     case port 7509 is briefly free but wrapper A's tray is still up
//   - `kill_stale_freenet_processes` runs early in wrapper startup
//     and happily pkills the OTHER wrapper's live backend child,
//     widening the race window it was meant to close
//   - a non-Freenet squatter on port 7509 triggers a false positive
//     with no diagnosable UX
//
// Solution: an advisory `flock` on a wrapper-scoped lockfile at
// `~/Library/Caches/Freenet/wrapper.lock`, acquired in `run_wrapper`
// BEFORE `kill_stale_freenet_processes` and BEFORE any user-visible
// state change. Held for the wrapper's lifetime; released on process
// exit by the kernel. If a second wrapper can't acquire, it exits
// silently. Robust to backend state, to pkill interference, and to
// third-party port squatters.

/// Advisory single-instance guard for the macOS wrapper process. Held
/// for the wrapper's lifetime and released automatically by the
/// kernel on process exit.
#[cfg(target_os = "macos")]
#[derive(Debug)]
#[allow(dead_code)]
pub(super) struct WrapperSingleInstanceLock {
    // The File keeps the fd alive; dropping it releases the flock.
    _file: std::fs::File,
}

/// Path of the wrapper lockfile if we can determine a cache directory.
#[allow(dead_code)]
pub(super) fn wrapper_lock_path() -> Option<PathBuf> {
    dirs::cache_dir().map(|d| d.join("Freenet").join("wrapper.lock"))
}

/// Outcome of attempting to acquire the wrapper single-instance lock.
/// Extracted as a pure type so the orchestration in `run_wrapper` stays
/// readable and the unit test can exercise every arm.
#[derive(Debug)]
#[allow(dead_code)]
pub(super) enum AcquireWrapperLockOutcome {
    /// We hold the lock; proceed with normal wrapper startup. The
    /// caller must hold the guard for the duration of the wrapper.
    Acquired(WrapperSingleInstanceLock),
    /// Another wrapper already holds the lock. Exit silently.
    AnotherWrapperRunning,
    /// We couldn't even try (no cache dir, can't create parent, etc.).
    /// Treat as "proceed without a lock" rather than silently exiting
    /// the user's only way to run Freenet. On platforms where the
    /// cache dir is always present, this is unreachable.
    UnavailableSoProceed,
}

/// Acquire the wrapper lockfile via `flock(LOCK_EX | LOCK_NB)`.
#[cfg(target_os = "macos")]
#[allow(dead_code)]
pub(super) fn acquire_wrapper_single_instance_lock() -> AcquireWrapperLockOutcome {
    use std::os::unix::io::AsRawFd;
    let Some(lock_path) = wrapper_lock_path() else {
        tracing::warn!("Wrapper lock: cache directory unresolvable; proceeding without lock");
        return AcquireWrapperLockOutcome::UnavailableSoProceed;
    };
    if let Some(parent) = lock_path.parent() {
        if let Err(e) = std::fs::create_dir_all(parent) {
            tracing::warn!(
                "Wrapper lock: failed to create {}: {}; proceeding without lock",
                parent.display(),
                e
            );
            return AcquireWrapperLockOutcome::UnavailableSoProceed;
        }
    }
    let file = match std::fs::OpenOptions::new()
        .create(true)
        .truncate(false)
        .write(true)
        .open(&lock_path)
    {
        Ok(f) => f,
        Err(e) => {
            tracing::warn!(
                "Wrapper lock: failed to open {}: {}; proceeding without lock",
                lock_path.display(),
                e
            );
            return AcquireWrapperLockOutcome::UnavailableSoProceed;
        }
    };
    // SAFETY: `file.as_raw_fd()` returns a valid fd owned by `file`,
    // and `libc::flock` only operates on that fd. LOCK_EX | LOCK_NB
    // is an exclusive non-blocking lock that fails fast if held by
    // another process. The lock is released automatically when the
    // fd closes on process exit (kernel-managed); we never need to
    // unlock explicitly.
    let rc = unsafe { libc::flock(file.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) };
    if rc != 0 {
        AcquireWrapperLockOutcome::AnotherWrapperRunning
    } else {
        AcquireWrapperLockOutcome::Acquired(WrapperSingleInstanceLock { _file: file })
    }
}

/// Non-macOS stub so `run_wrapper` compiles without cfg gates around
/// every mention. On Linux the backend-level port guard is sufficient
/// (no tray); on Windows the install-time wrapper detection is the
/// analogous mechanism.
#[cfg(not(target_os = "macos"))]
#[allow(dead_code)]
pub(super) fn acquire_wrapper_single_instance_lock() -> AcquireWrapperLockOutcome {
    AcquireWrapperLockOutcome::UnavailableSoProceed
}

#[cfg(not(target_os = "macos"))]
#[derive(Debug)]
#[allow(dead_code)]
pub(super) struct WrapperSingleInstanceLock;

/// At-most-once guard for spawning the first-run dashboard opener within a
/// single wrapper process. Without it, a wrapper that restarts its daemon
/// child several times before the HTTP server binds (e.g. during a crash
/// loop on initial startup) would accumulate one 30-second opener thread
/// per relaunch, each racing to open a browser tab and write the marker.
/// Checking a real wall-clock Instant plus polling a TCP port means we
/// also can't reuse the per-launch marker file itself as a latch.
#[cfg(any(target_os = "windows", target_os = "macos"))]
static FIRST_RUN_OPENER_SPAWNED: std::sync::atomic::AtomicBool =
    std::sync::atomic::AtomicBool::new(false);

// ── Launch at Login (macOS) ──
//
// On macOS, the DMG-installed Freenet.app does not auto-start on login by
// itself: the .app bundle is just a folder in /Applications. To match
// the Windows-installer experience (service registers with the OS and
// starts on boot) we write a user LaunchAgent plist that points at the
// .app bundle's CFBundleExecutable shell wrapper and sets RunAtLoad=true.
//
// The presence/absence of the plist file is the single source of truth
// for the user's preference: plist exists means launch at login enabled.
// The tray menu's Launch at Login check item reads this state.
//
// Implementation uses `launchctl bootstrap gui/$UID <plist>`, which loads
// the agent into the current GUI session and honours RunAtLoad on future
// logins. launchctl exit status is logged via tracing::warn for diagnosis
// but not propagated to callers; the plist is the authoritative preference.

/// Identifier that also serves as the plist filename and launchd label.
#[allow(dead_code)]
const LAUNCH_AT_LOGIN_LABEL: &str = "org.freenet.Freenet";

/// Legacy plist label written by the old `install.sh` / `freenet service
/// install` path. We log a warning when we detect it so users know to
/// clean up the duplicate before it races for port 7509.
#[allow(dead_code)]
const LEGACY_SERVICE_LAUNCHD_LABEL: &str = "org.freenet.node";

/// Absolute path of the user LaunchAgent plist, if `$HOME` is resolvable.
#[allow(dead_code)]
fn launch_agent_plist_path() -> Option<PathBuf> {
    launch_agent_plist_path_for(LAUNCH_AT_LOGIN_LABEL)
}

#[allow(dead_code)]
fn launch_agent_plist_path_for(label: &str) -> Option<PathBuf> {
    dirs::home_dir().map(|h| {
        h.join("Library")
            .join("LaunchAgents")
            .join(format!("{label}.plist"))
    })
}

#[allow(dead_code)]
fn is_launch_at_login_enabled_at(plist: &Path) -> bool {
    plist.exists()
}

/// If `exe` lives inside a macOS `.app` bundle, return that bundle's
/// absolute path. Walks up the parent directories until a path segment
/// ending in `.app` is found, or the filesystem root is reached.
///
/// Exposed at `pub(super)` so `update.rs` can detect bundle context for
/// its DMG-swap auto-update path.
#[allow(dead_code)]
pub(super) fn macos_app_bundle_path(exe: &Path) -> Option<PathBuf> {
    for ancestor in exe.ancestors() {
        if ancestor
            .file_name()
            .and_then(|n| n.to_str())
            .is_some_and(|n| n.ends_with(".app"))
        {
            return Some(ancestor.to_path_buf());
        }
    }
    None
}

/// The path to a `.app` bundle's CFBundleExecutable shell wrapper, by our
/// packaging convention. See `scripts/package-macos.sh` for how this
/// wrapper is produced: it execs the inner `freenet-bin` with the
/// `service run-wrapper` subcommand.
#[allow(dead_code)]
fn macos_app_bundle_wrapper(bundle: &Path) -> PathBuf {
    bundle.join("Contents").join("MacOS").join("Freenet")
}

/// Compute the ProgramArguments array for the user LaunchAgent. If we're
/// running from inside an `.app` bundle, launchd should relaunch the
/// bundle's CFBundleExecutable shell wrapper (which in turn execs
/// `freenet-bin service run-wrapper`, entering the tray path). Otherwise
/// (cargo-run, raw binary install, dev build) launchd needs to invoke
/// the binary with the explicit `service run-wrapper` subcommand or it
/// would default to `Network` mode and skip the tray entirely.
#[allow(dead_code)]
fn launch_agent_program_arguments(exe: &Path) -> Vec<String> {
    match macos_app_bundle_path(exe) {
        Some(bundle) => vec![
            macos_app_bundle_wrapper(&bundle)
                .to_string_lossy()
                .into_owned(),
        ],
        None => vec![
            exe.to_string_lossy().into_owned(),
            "service".to_string(),
            "run-wrapper".to_string(),
        ],
    }
}

fn xml_escape(s: &str) -> String {
    // XML 1.0 predefined entities. Filesystem paths very rarely contain
    // these, but launchd silently fails to load a malformed plist, so
    // defensive escaping avoids a hard-to-diagnose "Launch at Login
    // silently does nothing" for pathological paths.
    let mut out = String::with_capacity(s.len());
    for ch in s.chars() {
        match ch {
            '&' => out.push_str("&amp;"),
            '<' => out.push_str("&lt;"),
            '>' => out.push_str("&gt;"),
            '"' => out.push_str("&quot;"),
            '\'' => out.push_str("&apos;"),
            c => out.push(c),
        }
    }
    out
}

/// Build the plist XML for the user LaunchAgent given explicit
/// ProgramArguments. Pure function so the output format is unit-testable
/// without touching the filesystem or launchctl.
#[allow(dead_code)]
fn launch_agent_plist_contents(program_arguments: &[String]) -> String {
    let mut args_xml = String::new();
    for arg in program_arguments {
        args_xml.push_str("        <string>");
        args_xml.push_str(&xml_escape(arg));
        args_xml.push_str("</string>\n");
    }
    format!(
        r#"<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
    <key>Label</key>
    <string>{LAUNCH_AT_LOGIN_LABEL}</string>
    <key>ProgramArguments</key>
    <array>
{args_xml}    </array>
    <key>RunAtLoad</key>
    <true/>
    <key>KeepAlive</key>
    <false/>
    <key>ProcessType</key>
    <string>Interactive</string>
</dict>
</plist>
"#
    )
}

/// Write the plist to disk, creating `~/Library/LaunchAgents` if needed.
/// Does NOT activate the agent with launchctl: that's the caller's job
/// (separated so the filesystem half is unit-testable on Linux).
#[allow(dead_code)]
fn write_launch_agent_plist_at(plist: &Path, program_arguments: &[String]) -> std::io::Result<()> {
    if let Some(parent) = plist.parent() {
        std::fs::create_dir_all(parent)?;
    }
    std::fs::write(plist, launch_agent_plist_contents(program_arguments))
}

/// Remove the plist. Idempotent: returns Ok if the file was already gone.
#[allow(dead_code)]
fn remove_launch_agent_plist_at(plist: &Path) -> std::io::Result<()> {
    match std::fs::remove_file(plist) {
        Ok(()) => Ok(()),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(e) => Err(e),
    }
}

/// Returns true if the on-disk plist already contains the expected
/// leading ProgramArguments path, false otherwise (including on read
/// error). Used for the self-heal check at wrapper startup so the
/// plist gets rewritten after the user moves or replaces the .app.
#[allow(dead_code)]
fn launch_agent_plist_has_expected_leader(plist: &Path, expected_leader: &str) -> bool {
    let escaped = format!("<string>{}</string>", xml_escape(expected_leader));
    match std::fs::read_to_string(plist) {
        Ok(contents) => contents.contains(&escaped),
        Err(_) => false,
    }
}

/// Call `launchctl bootstrap gui/$UID <plist>`. Logs non-zero exit
/// (with stderr) via tracing::warn so silent "plist written but launchd
/// never loaded it" breakages are diagnosable after the fact.
#[cfg(target_os = "macos")]
fn launchctl_bootstrap(plist: &Path) {
    // SAFETY: libc::getuid is always safe; it returns the current user
    // ID without touching user-provided pointers.
    let uid = unsafe { libc::getuid() };
    let target = format!("gui/{uid}");
    match std::process::Command::new("launchctl")
        .args(["bootstrap", &target])
        .arg(plist)
        .output()
    {
        Ok(o) if !o.status.success() => {
            tracing::warn!(
                "launchctl bootstrap {} returned {}: {}",
                plist.display(),
                o.status,
                String::from_utf8_lossy(&o.stderr).trim()
            );
        }
        Err(e) => {
            tracing::warn!(
                "launchctl bootstrap {} failed to spawn: {}",
                plist.display(),
                e
            );
        }
        _ => {}
    }
}

#[cfg(target_os = "macos")]
fn launchctl_bootout(plist: &Path) {
    // SAFETY: libc::getuid is always safe; it returns the current user
    // ID without touching user-provided pointers.
    let uid = unsafe { libc::getuid() };
    let target = format!("gui/{uid}");
    match std::process::Command::new("launchctl")
        .args(["bootout", &target])
        .arg(plist)
        .output()
    {
        Ok(o) if !o.status.success() => {
            // bootout returns non-zero when the agent isn't currently
            // loaded, which is expected when we're disabling after a
            // previous session already exited. Log at debug, not warn.
            tracing::debug!(
                "launchctl bootout {} returned {}: {}",
                plist.display(),
                o.status,
                String::from_utf8_lossy(&o.stderr).trim()
            );
        }
        Err(e) => {
            tracing::warn!(
                "launchctl bootout {} failed to spawn: {}",
                plist.display(),
                e
            );
        }
        _ => {}
    }
}

#[cfg(not(target_os = "macos"))]
#[allow(dead_code)]
fn launchctl_bootstrap(_plist: &Path) {}

#[cfg(not(target_os = "macos"))]
#[allow(dead_code)]
fn launchctl_bootout(_plist: &Path) {}

/// Enable launch-at-login: write the plist and ask launchd to load it.
#[allow(dead_code)]
pub(super) fn enable_launch_at_login(executable: &Path) -> std::io::Result<()> {
    let Some(plist) = launch_agent_plist_path() else {
        return Err(std::io::Error::new(
            std::io::ErrorKind::NotFound,
            "could not resolve home directory for launch agent path",
        ));
    };
    let args = launch_agent_program_arguments(executable);
    // If an older plist is already loaded, bootout first so bootstrap
    // picks up any change to the executable path (e.g. user moved the
    // .app from /Applications to ~/Applications between launches).
    if plist.exists() {
        launchctl_bootout(&plist);
    }
    write_launch_agent_plist_at(&plist, &args)?;
    launchctl_bootstrap(&plist);
    Ok(())
}

#[allow(dead_code)]
pub(super) fn disable_launch_at_login() -> std::io::Result<()> {
    let Some(plist) = launch_agent_plist_path() else {
        return Ok(());
    };
    launchctl_bootout(&plist);
    remove_launch_agent_plist_at(&plist)
}

/// User-facing query: is launch-at-login currently on?
#[allow(dead_code)]
pub(super) fn is_launch_at_login_enabled() -> bool {
    launch_agent_plist_path()
        .map(|p| is_launch_at_login_enabled_at(&p))
        .unwrap_or(false)
}

/// Decision outcome for first-run Launch at Login registration. Pure so
/// the decision logic can be unit-tested independently of filesystem or
/// launchctl side-effects, per the deployment-rule pattern established
/// earlier in this PR.
#[derive(Debug, PartialEq, Eq)]
#[allow(dead_code)]
pub(super) enum FirstRunLaunchAtLoginAction {
    /// User hasn't launched before and Launch at Login is disabled: enable it.
    Register,
    /// User hasn't launched before but Launch at Login is already enabled
    /// (e.g. from the legacy install.sh path). Leave as-is.
    AlreadyEnabled,
    /// Not a first-run invocation.
    NotFirstRun,
}

#[allow(dead_code)]
pub(super) fn first_run_launch_at_login_action(
    is_first_run: bool,
    already_enabled: bool,
) -> FirstRunLaunchAtLoginAction {
    if !is_first_run {
        FirstRunLaunchAtLoginAction::NotFirstRun
    } else if already_enabled {
        FirstRunLaunchAtLoginAction::AlreadyEnabled
    } else {
        FirstRunLaunchAtLoginAction::Register
    }
}

/// Decision outcome for a user-initiated Launch at Login toggle (click
/// on the tray menu check item). Pure function, same testability
/// rationale as `first_run_launch_at_login_action`.
#[derive(Debug, PartialEq, Eq)]
#[allow(dead_code)]
pub(super) enum ToggleLaunchAtLoginOutcome {
    Enable,
    Disable,
}

#[allow(dead_code)]
pub(super) fn toggle_launch_at_login_outcome(
    currently_enabled: bool,
) -> ToggleLaunchAtLoginOutcome {
    if currently_enabled {
        ToggleLaunchAtLoginOutcome::Disable
    } else {
        ToggleLaunchAtLoginOutcome::Enable
    }
}

/// Startup self-heal: if Launch at Login is enabled but the plist points
/// somewhere other than where we'd write it now (user moved the .app,
/// upgraded via a new DMG install location, etc.), rewrite it. Idempotent:
/// no-op when the plist is already current, or when Launch at Login is
/// disabled, or when we can't figure out the expected location.
#[allow(dead_code)]
pub(super) fn refresh_launch_at_login_plist_if_stale() {
    if !is_launch_at_login_enabled() {
        return;
    }
    let Some(plist) = launch_agent_plist_path() else {
        return;
    };
    let Ok(exe) = std::env::current_exe() else {
        return;
    };
    let args = launch_agent_program_arguments(&exe);
    let Some(leader) = args.first() else {
        return;
    };
    if launch_agent_plist_has_expected_leader(&plist, leader) {
        return;
    }
    tracing::info!(
        "Refreshing stale Launch at Login plist (was pointing elsewhere, now {})",
        leader
    );
    if let Err(e) = enable_launch_at_login(&exe) {
        tracing::warn!("Failed to refresh Launch at Login plist: {}", e);
    }
}

/// Returns true if the legacy `org.freenet.node` LaunchAgent plist
/// exists. Used to warn users migrating from `install.sh` that they
/// have two auto-starts racing for the same ports.
#[allow(dead_code)]
pub(super) fn legacy_launchd_agent_present() -> bool {
    launch_agent_plist_path_for(LEGACY_SERVICE_LAUNCHD_LABEL)
        .map(|p| p.exists())
        .unwrap_or(false)
}

/// Run all the macOS-only Launch-at-Login housekeeping that must happen
/// before the tray is built. Specifically:
///
/// - On first wrapper launch, register a user LaunchAgent so Freenet
///   auto-starts on future logins (Windows-parity auto-start).
/// - On every subsequent launch, if the user had previously enabled
///   Launch at Login but the plist's embedded executable path no longer
///   matches the current one (user moved the .app, upgraded via a new
///   DMG installed elsewhere, etc.), rewrite it.
/// - If the legacy `org.freenet.node` LaunchAgent from the install.sh
///   era is still present, log a prominent warning so migrating users
///   know to clean it up before the two agents race for port 7509.
///
/// Called synchronously (not from the wrapper thread) so the tray's
/// Launch-at-Login check item reads the final filesystem state when
/// `TrayState::new` consults it. Previously the first-run registration
/// lived inside `run_wrapper_loop`, which ran on a background thread
/// AFTER the tray was built, so the menu item shipped stale-unchecked
/// on first launch even though Launch at Login had been enabled.
#[cfg(target_os = "macos")]
pub(super) fn macos_launch_at_login_startup(log_dir: &Path) {
    // Legacy-agent warning: fire once per startup regardless of first-run.
    // If users see this, they have an auto-start from the old install.sh
    // flow still configured; if we also write our own plist below, both
    // agents race for port 7509 on every login. To avoid that race we
    // treat the legacy plist's presence as "already enabled" for the
    // first-run decision, so we don't layer a second auto-start on top.
    // User is still instructed to clean up the legacy one manually.
    let legacy_present = legacy_launchd_agent_present();
    if legacy_present {
        log_wrapper_event(
            log_dir,
            "Legacy launchd agent ~/Library/LaunchAgents/org.freenet.node.plist \
             detected. Freenet is already configured to auto-start via that \
             agent; the new DMG install will NOT create a duplicate agent. \
             To clean up the legacy one when convenient: launchctl bootout \
             gui/$UID ~/Library/LaunchAgents/org.freenet.node.plist && rm \
             ~/Library/LaunchAgents/org.freenet.node.plist",
        );
    }

    // First-run auto-register. Treat the legacy plist as already-enabled
    // so migrating users don't end up with two plists (Codex P2).
    let Some(marker) = first_run_marker_path() else {
        return;
    };
    let already_enabled = is_launch_at_login_enabled() || legacy_present;
    match first_run_launch_at_login_action(is_first_run_at(&marker), already_enabled) {
        FirstRunLaunchAtLoginAction::Register => match std::env::current_exe() {
            Ok(exe) => match enable_launch_at_login(&exe) {
                Ok(()) => log_wrapper_event(log_dir, "First-run: registered Launch at Login agent"),
                Err(e) => log_wrapper_event(
                    log_dir,
                    &format!("First-run Launch at Login registration failed: {e}"),
                ),
            },
            Err(e) => log_wrapper_event(
                log_dir,
                &format!("First-run Launch at Login: could not resolve current exe: {e}"),
            ),
        },
        FirstRunLaunchAtLoginAction::AlreadyEnabled => {
            // Nothing to do for first-run, but the plist may still be stale.
            // Only refresh if our OWN plist is present — don't resurrect
            // a rewrite cycle on a legacy-only install.
            if is_launch_at_login_enabled() {
                refresh_launch_at_login_plist_if_stale();
            }
        }
        FirstRunLaunchAtLoginAction::NotFirstRun => {
            if is_launch_at_login_enabled() {
                refresh_launch_at_login_plist_if_stale();
            }
        }
    }
}

#[cfg(not(target_os = "macos"))]
#[allow(dead_code)]
pub(super) fn macos_launch_at_login_startup(_log_dir: &Path) {}

/// Spawn a short-lived thread that waits for the dashboard HTTP server to
/// come up, then opens it in the browser and writes the first-run marker.
/// The thread gives up after a generous timeout. If the daemon is crashing
/// repeatedly we'd rather skip first-run and retry next launch than open
/// a "connection refused" page in the user's browser.
///
/// Caller is responsible for gating on `FIRST_RUN_OPENER_SPAWNED` so we
/// don't start more than one opener per process lifetime.
#[cfg(any(target_os = "windows", target_os = "macos"))]
fn spawn_first_run_dashboard_opener(log_dir: &Path) {
    // Import locally so we use the unqualified `Instant::now()` form. Wall-
    // clock time is correct here: this is tray/CLI onboarding, not sim-
    // reachable core code, and the deadline is about what the user's
    // patience will tolerate. See `crates/core/src/bin/freenet.rs` for
    // similar bin-side wall-clock usage.
    use std::time::{Duration, Instant};

    let log_dir = log_dir.to_path_buf();
    std::thread::spawn(move || {
        let deadline = Instant::now() + Duration::from_secs(30);
        while Instant::now() < deadline {
            if dashboard_port_is_listening() {
                open_url_in_browser(DASHBOARD_URL);
                if let Some(marker) = first_run_marker_path() {
                    match mark_first_run_complete_at(&marker) {
                        Ok(()) => {
                            log_wrapper_event(&log_dir, "First-run onboarding: dashboard opened")
                        }
                        Err(e) => log_wrapper_event(
                            &log_dir,
                            &format!("Failed to write first-run marker: {e}"),
                        ),
                    }
                }
                return;
            }
            std::thread::sleep(Duration::from_millis(500));
        }
        log_wrapper_event(
            &log_dir,
            "First-run dashboard open skipped: dashboard never became reachable",
        );
    });
}

/// State for the wrapper backoff state machine.
#[derive(Debug, Clone)]
struct WrapperState {
    backoff_secs: u64,
    consecutive_failures: u32,
    port_conflict_kills: u32,
}

impl WrapperState {
    fn new() -> Self {
        Self {
            backoff_secs: WRAPPER_INITIAL_BACKOFF_SECS,
            consecutive_failures: 0,
            port_conflict_kills: 0,
        }
    }
}

/// Action the wrapper should take after the child process exits.
#[derive(Debug, PartialEq)]
enum WrapperAction {
    /// Run update, then relaunch.
    Update,
    /// Exit the wrapper cleanly.
    Exit,
    /// Kill stale processes and retry immediately.
    KillAndRetry,
    /// Wait (with jitter) then relaunch.
    BackoffAndRelaunch { secs: u64 },
}

/// Pure function: given current state and exit info, determine the next action
/// and update the state. Testable without spawning processes.
fn next_wrapper_action(
    state: &mut WrapperState,
    exit_code: i32,
    is_port_conflict: bool,
    update_succeeded: Option<bool>, // None if not an update exit code
) -> WrapperAction {
    match exit_code {
        code if code == WRAPPER_EXIT_UPDATE_NEEDED => {
            match update_succeeded {
                Some(true) => {
                    state.consecutive_failures = 0;
                    state.port_conflict_kills = 0;
                    state.backoff_secs = WRAPPER_INITIAL_BACKOFF_SECS;
                    WrapperAction::Update
                }
                Some(false) => {
                    state.consecutive_failures += 1;
                    let secs = state.backoff_secs;
                    state.backoff_secs = (state.backoff_secs * 2).min(WRAPPER_MAX_BACKOFF_SECS);
                    WrapperAction::BackoffAndRelaunch { secs }
                }
                None => WrapperAction::Update, // Will be called again with result
            }
        }
        code if code == WRAPPER_EXIT_ALREADY_RUNNING => WrapperAction::Exit,
        0 => WrapperAction::Exit,
        _ => {
            if is_port_conflict {
                state.port_conflict_kills += 1;
                if state.port_conflict_kills <= WRAPPER_MAX_PORT_CONFLICT_KILLS {
                    state.backoff_secs = WRAPPER_INITIAL_BACKOFF_SECS;
                    return WrapperAction::KillAndRetry;
                }
                // Fall through to normal crash backoff
            }
            state.consecutive_failures += 1;
            state.port_conflict_kills = 0;
            let secs = state.backoff_secs;
            state.backoff_secs = (state.backoff_secs * 2).min(WRAPPER_MAX_BACKOFF_SECS);
            WrapperAction::BackoffAndRelaunch { secs }
        }
    }
}

/// Compute a jittered duration: `secs` ±20%.
fn jitter_secs(secs: u64) -> u64 {
    use std::collections::hash_map::DefaultHasher;
    use std::hash::{Hash, Hasher};
    let mut hasher = DefaultHasher::new();
    std::time::SystemTime::now().hash(&mut hasher);
    let hash = hasher.finish();
    let factor = 0.8 + (hash % 1000) as f64 / 2500.0;
    (secs as f64 * factor) as u64
}

/// Result of a backoff sleep that was interrupted by a tray action.
#[allow(dead_code)] // Variants are constructed only on platforms with tray support
enum BackoffInterrupt {
    /// Sleep completed without interruption.
    Completed,
    /// User requested quit.
    Quit,
    /// User requested start/restart — break out of backoff and relaunch.
    Relaunch,
    /// User requested a check for updates.
    CheckUpdate,
}

/// Sleep for `secs` with ±20% jitter, interruptible via an optional action
/// channel. Handles `ViewLogs` and `OpenDashboard` inline. Returns the
/// action that interrupted the sleep so the caller can handle it.
/// Sleeps in 1-second chunks to remain responsive to tray actions.
fn sleep_with_jitter_interruptible(
    secs: u64,
    #[cfg(any(target_os = "windows", target_os = "macos"))] action_rx: Option<
        &std::sync::mpsc::Receiver<super::tray::TrayAction>,
    >,
    #[cfg(not(any(target_os = "windows", target_os = "macos")))] _action_rx: Option<
        &std::sync::mpsc::Receiver<super::tray::TrayAction>,
    >,
) -> BackoffInterrupt {
    let jittered = jitter_secs(secs).max(1);
    for _ in 0..jittered {
        std::thread::sleep(std::time::Duration::from_secs(1));

        #[cfg(any(target_os = "windows", target_os = "macos"))]
        if let Some(rx) = action_rx {
            if let Ok(action) = rx.try_recv() {
                match action {
                    super::tray::TrayAction::Quit => return BackoffInterrupt::Quit,
                    super::tray::TrayAction::ViewLogs => super::tray::open_log_file(),
                    super::tray::TrayAction::OpenDashboard => {
                        open_url_in_browser(DASHBOARD_URL);
                    }
                    super::tray::TrayAction::Start | super::tray::TrayAction::Restart => {
                        return BackoffInterrupt::Relaunch;
                    }
                    super::tray::TrayAction::CheckUpdate => {
                        return BackoffInterrupt::CheckUpdate;
                    }
                    super::tray::TrayAction::Stop => {} // already stopped/backing off
                }
            }
        }
    }
    BackoffInterrupt::Completed
}

/// Run the wrapper loop that manages a `freenet network` child process.
/// On Windows and macOS, shows a system tray / menu bar icon.
/// On Linux, runs the wrapper loop directly (no tray).
fn run_wrapper(version: &str) -> Result<()> {
    // On Windows, detach from the console so no terminal window is visible.
    // The wrapper runs as a background service — all output goes to log files.
    //
    // CRITICAL: after `FreeConsole()` (and in particular when this process
    // was launched by Windows autostart and never had a console at all),
    // the inherited standard handles are invalid. ANY `Command::spawn()`
    // reachable from this function MUST explicitly set
    // `.stdin/.stdout/.stderr(Stdio::null())` — otherwise `spawn()` fails
    // with "The handle is invalid" (os error 6) and the child silently
    // never starts. Known spawn sites downstream of here:
    //   - `spawn_update_command`                   (this file)
    //   - network child in `run_wrapper_loop`      (this file, ~line 1452)
    //   - `spawn_new_wrapper`                      (this file)
    //   - `open_log_file` notepad/open/xdg-open    (tray.rs)
    //   - `taskkill_pid` (nulls all three) and `list_freenet_processes`
    //     (`.output()` — fresh pipes, no stdin inherit), reached directly via
    //     `kill_stale_freenet_processes` (called below) and via
    //     `kill_freenet_service_processes` (the `update.rs` restart path)
    // Add any new child spawn in this module with null stdio by default.
    // See #3933 / #3934 and `.claude/rules/bug-prevention-patterns.md`
    // at the repo root for the rule + audit grep.
    #[cfg(target_os = "windows")]
    unsafe {
        winapi::um::wincon::FreeConsole();
    }

    use freenet::tracing::tracer::get_log_dir;
    use std::sync::mpsc;

    let log_dir = get_log_dir().unwrap_or_else(|| {
        let fallback = dirs::data_local_dir()
            .unwrap_or_else(|| std::path::PathBuf::from("."))
            .join("freenet")
            .join("logs");
        eprintln!(
            "Warning: could not determine log directory, using {}",
            fallback.display()
        );
        fallback
    });
    std::fs::create_dir_all(&log_dir)
        .with_context(|| format!("Failed to create log directory: {}", log_dir.display()))?;

    // macOS wrapper single-instance guard. MUST run before
    // `kill_stale_freenet_processes` and before anything user-visible.
    // The kill helper uses `pkill -f "freenet network"` which matches
    // the CURRENTLY-RUNNING backend child of a live wrapper, not just
    // stale orphans; if we were to kill_stale before the single-
    // instance check, a second wrapper would happily murder the first
    // wrapper's backend, then see a free port, then proceed to build a
    // duplicate tray. The flock-based guard is immune to that race
    // because it inspects wrapper-level state (the lockfile held by
    // the first wrapper process) not backend state.
    //
    // Phase-1 smoke test on 2026-04-22 reproduced the duplicate tray
    // whenever launchd's RunAtLoad fired while the user's open-
    // launched wrapper was still alive; this guard prevents that
    // overlap without changing the LAL agent's `RunAtLoad=true`
    // semantics (login-time auto-start still works; only the in-
    // session overlap is suppressed).
    //
    // Linux has no tray and Windows has install-time guards; the
    // overlap race only manifests on macOS where launchd can spawn a
    // concurrent wrapper via RunAtLoad.
    #[cfg(target_os = "macos")]
    let _wrapper_lock = match acquire_wrapper_single_instance_lock() {
        AcquireWrapperLockOutcome::Acquired(guard) => Some(guard),
        AcquireWrapperLockOutcome::AnotherWrapperRunning => {
            log_wrapper_event(
                &log_dir,
                &format!(
                    "Wrapper pid={} exiting: another Freenet wrapper is already \
                     running (lockfile at ~/Library/Caches/Freenet/wrapper.lock is \
                     held). Expected if launchd fired RunAtLoad while an existing \
                     wrapper is alive, or if Freenet.app was double-launched. \
                     If Freenet's menu bar icon is not visible, another process may \
                     be holding the lock; try `lsof ~/Library/Caches/Freenet/wrapper.lock`.",
                    std::process::id()
                ),
            );
            return Ok(());
        }
        AcquireWrapperLockOutcome::UnavailableSoProceed => {
            log_wrapper_event(
                &log_dir,
                "Wrapper single-instance lock unavailable; proceeding without guard. \
                 Dup-tray risk if RunAtLoad fires while another wrapper is alive.",
            );
            None
        }
    };

    // Kill stale freenet network processes from a previous wrapper instance.
    // Runs AFTER the single-instance lock so we only ever kill orphans:
    // any live peer wrapper holds the lock and would have blocked us
    // above. Concurrent overlap cases exit silently before reaching here.
    kill_stale_freenet_processes(&log_dir);

    #[cfg(any(target_os = "windows", target_os = "macos"))]
    {
        use super::tray::{TrayAction, WrapperStatus};

        let (action_tx, action_rx) = mpsc::channel::<TrayAction>();
        let (status_tx, status_rx) = mpsc::channel::<WrapperStatus>();
        // Wrapper → tray cleanup-done signal. On macOS, `tao::EventLoop::run`
        // diverges (calls `process::exit` on `ControlFlow::Exit`), so the tray
        // must block long enough for the wrapper thread to kill its child
        // daemon before the process exits. The signal fires in the wrapper's
        // spawn thunk regardless of which code path returned, so every exit
        // route (Quit, auto-update restart, network-wait abort) is covered.
        let (cleanup_tx, cleanup_rx) = mpsc::channel::<()>();
        let version_owned = version.to_string();
        let log_dir_clone = log_dir.clone();

        // macOS: handle Launch at Login state BEFORE building the tray so
        // the menu item's check state reads the final filesystem truth.
        // Previously this ran on the wrapper thread, after the tray was
        // already built, so first-run users saw a stale-unchecked box
        // despite the agent being registered. Also rewrites a stale plist
        // when the user moved the .app, and warns if the legacy install.sh
        // launchd agent is still present.
        #[cfg(target_os = "macos")]
        macos_launch_at_login_startup(&log_dir);

        // Wrapper loop runs on a background thread
        let loop_handle = std::thread::spawn(move || {
            let result = run_wrapper_loop(&log_dir_clone, Some((&action_rx, &status_tx)));
            cleanup_tx.send(()).ok();
            result
        });

        // Tray icon runs on the main thread (platform message pump)
        super::tray::run_tray_event_loop(action_tx, status_rx, cleanup_rx, &version_owned);

        // Tray loop exited (user clicked Quit) — join the wrapper thread
        match loop_handle.join() {
            Ok(result) => result,
            Err(_) => anyhow::bail!("Wrapper loop thread panicked"),
        }
    }

    #[cfg(not(any(target_os = "windows", target_os = "macos")))]
    {
        let _ = version;
        run_wrapper_loop(
            &log_dir,
            None::<(
                &mpsc::Receiver<super::tray::TrayAction>,
                &mpsc::Sender<super::tray::WrapperStatus>,
            )>,
        )
    }
}

/// Spawn `freenet update --quiet` and wait for it to complete.
/// On Windows, uses `CREATE_NO_WINDOW` to avoid flashing a console window
/// (the wrapper has already detached from the console via `FreeConsole`).
///
/// stdin/stdout/stderr are nulled on every platform. On Windows this is
/// load-bearing: the wrapper has already called `FreeConsole()` (and may
/// never have had a console at all when launched by autostart), so
/// inheriting the parent's invalid standard handles makes `spawn()` fail
/// with "The handle is invalid" (os error 6). Without these nulls, the
/// update subprocess silently fails to start and the wrapper falls into
/// the exit-42 / update-failed / backoff-relaunch loop documented in
/// #3934 (which was also the root cause of "Check for Updates" being
/// broken in #3933). Null stdio is harmless on macOS/Linux because
/// `--quiet` already suppresses all output.
fn spawn_update_command(exe_path: &Path) -> std::io::Result<std::process::ExitStatus> {
    let mut cmd = std::process::Command::new(exe_path);
    cmd.args(["update", "--quiet"])
        .stdin(std::process::Stdio::null())
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null());

    #[cfg(target_os = "windows")]
    {
        use std::os::windows::process::CommandExt;
        const CREATE_NO_WINDOW: u32 = 0x08000000;
        cmd.creation_flags(CREATE_NO_WINDOW);
    }

    cmd.status()
}

/// Spawn a new wrapper process from the (possibly updated) binary on disk.
/// Used after a successful tray-initiated update to re-exec the wrapper
/// so the tray displays the correct new version.
///
/// Returns `true` if the new wrapper was spawned successfully (caller should
/// exit to avoid two wrappers). Returns `false` on failure (caller should
/// fall through to relaunch the child with the current wrapper instead of
/// leaving the user with no running node).
fn spawn_new_wrapper(exe_path: &Path, log_dir: &Path) -> bool {
    let mut cmd = std::process::Command::new(exe_path);
    cmd.args(["service", "run-wrapper"])
        .stdin(std::process::Stdio::null())
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null());

    #[cfg(target_os = "windows")]
    {
        use std::os::windows::process::CommandExt;
        const DETACHED_PROCESS: u32 = 0x00000008;
        const CREATE_NEW_PROCESS_GROUP: u32 = 0x00000200;
        cmd.creation_flags(DETACHED_PROCESS | CREATE_NEW_PROCESS_GROUP);
    }

    match cmd.spawn() {
        Ok(_) => {
            log_wrapper_event(log_dir, "New wrapper process spawned");
            true
        }
        Err(e) => {
            log_wrapper_event(
                log_dir,
                &format!("Failed to spawn new wrapper: {e}. Continuing with current wrapper."),
            );
            false
        }
    }
}

/// Maximum time to wait for network readiness at startup (seconds).
const NETWORK_READINESS_TIMEOUT_SECS: u64 = 60;
/// Interval between network readiness checks (seconds).
const NETWORK_READINESS_CHECK_INTERVAL_SECS: u64 = 2;
/// DNS probe target for network readiness checks.
const NETWORK_PROBE_ADDR: &str = "freenet.org:443";

/// Wait for network connectivity before spawning the node.
///
/// On Windows, the registry Run key fires at user logon, which can be before
/// the network stack is fully operational. Without this check, the node starts
/// with no connectivity — gateway fetches fail, CONNECT handshakes timeout,
/// and the node gets stuck with zombie transient connections. See #3716.
///
/// Returns `true` if network is ready or timed out (we start the node
/// either way). Returns `false` if the user requested Quit via the tray
/// during the wait.
///
/// We probe our own domain because if it's unreachable, the gateway fetch
/// will also fail — there's no point starting the node before we can reach
/// the gateway index.
///
/// Note: On platforms with tray support, Quit actions are handled during
/// the wait. Other tray actions (Start, ViewLogs, etc.) are deferred until
/// the wrapper loop starts.
fn wait_for_network_ready(
    log_dir: &Path,
    #[cfg(any(target_os = "windows", target_os = "macos"))] action_rx: Option<
        &std::sync::mpsc::Receiver<super::tray::TrayAction>,
    >,
    #[cfg(not(any(target_os = "windows", target_os = "macos")))] _action_rx: Option<
        &std::sync::mpsc::Receiver<super::tray::TrayAction>,
    >,
) -> bool {
    wait_for_network_ready_inner(
        log_dir,
        NETWORK_PROBE_ADDR,
        #[cfg(any(target_os = "windows", target_os = "macos"))]
        action_rx,
        #[cfg(not(any(target_os = "windows", target_os = "macos")))]
        _action_rx,
    )
}

/// Inner implementation with configurable probe address for testing.
fn wait_for_network_ready_inner(
    log_dir: &Path,
    probe_addr: &str,
    #[cfg(any(target_os = "windows", target_os = "macos"))] action_rx: Option<
        &std::sync::mpsc::Receiver<super::tray::TrayAction>,
    >,
    #[cfg(not(any(target_os = "windows", target_os = "macos")))] _action_rx: Option<
        &std::sync::mpsc::Receiver<super::tray::TrayAction>,
    >,
) -> bool {
    use std::net::ToSocketAddrs;

    // Note: `to_socket_addrs()` is a blocking OS resolver call that can take
    // 15-30s on Windows when the network is down. The iteration count between
    // calls means the total wait may exceed NETWORK_READINESS_TIMEOUT_SECS
    // in pathological cases. This is acceptable for a pre-startup wait.

    // Quick check — if DNS works immediately, skip the wait
    if probe_addr.to_socket_addrs().is_ok() {
        return true;
    }

    log_wrapper_event(
        log_dir,
        "Network not ready yet, waiting for connectivity...",
    );

    let max_checks = NETWORK_READINESS_TIMEOUT_SECS / NETWORK_READINESS_CHECK_INTERVAL_SECS;

    for _ in 0..max_checks {
        let jittered = jitter_secs(NETWORK_READINESS_CHECK_INTERVAL_SECS);
        std::thread::sleep(std::time::Duration::from_secs(jittered.max(1)));

        // Allow the user to quit via the tray during the network wait
        #[cfg(any(target_os = "windows", target_os = "macos"))]
        if let Some(rx) = action_rx {
            if let Ok(super::tray::TrayAction::Quit) = rx.try_recv() {
                return false;
            }
        }

        if probe_addr.to_socket_addrs().is_ok() {
            log_wrapper_event(log_dir, "Network is ready");
            return true;
        }
    }

    log_wrapper_event(
        log_dir,
        "Network readiness timeout — starting node anyway (it will retry internally)",
    );
    true
}

/// The core wrapper loop. Spawns `freenet network`, handles exit codes,
/// and communicates with the tray icon (if present).
fn run_wrapper_loop(
    log_dir: &Path,
    #[cfg(any(target_os = "windows", target_os = "macos"))] tray: Option<(
        &std::sync::mpsc::Receiver<super::tray::TrayAction>,
        &std::sync::mpsc::Sender<super::tray::WrapperStatus>,
    )>,
    #[cfg(not(any(target_os = "windows", target_os = "macos")))] _tray: Option<(
        &std::sync::mpsc::Receiver<super::tray::TrayAction>,
        &std::sync::mpsc::Sender<super::tray::WrapperStatus>,
    )>,
) -> Result<()> {
    #[cfg(any(target_os = "windows", target_os = "macos"))]
    use super::tray::WrapperStatus;

    let exe_path = std::env::current_exe().context("Failed to get current executable")?;

    let mut state = WrapperState::new();

    // On first launch, wait for network connectivity before spawning the node.
    // This prevents the node from starting in a degraded state when the wrapper
    // is auto-started at login before the network stack is ready (see #3716).
    #[cfg(any(target_os = "windows", target_os = "macos"))]
    if !wait_for_network_ready(log_dir, tray.map(|(rx, _)| rx)) {
        return Ok(()); // User requested Quit during network wait
    }
    #[cfg(not(any(target_os = "windows", target_os = "macos")))]
    wait_for_network_ready(log_dir, None);

    // First-run onboarding: if this user has never launched the tray wrapper
    // before, open the dashboard in their browser once the HTTP server comes
    // up, and (on macOS) register the app for launch-at-login so Freenet
    // behaves like the Windows-installed service: always on, starts at
    // boot, no extra action required from the user. Runs the dashboard
    // opener in a background thread so it doesn't delay the main loop.
    // `FIRST_RUN_OPENER_SPAWNED` ensures at most one opener thread per
    // wrapper process lifetime so a crash-looping daemon doesn't accumulate
    // pending openers that would all race to open tabs when the port
    // eventually binds.
    #[cfg(any(target_os = "windows", target_os = "macos"))]
    if let Some(marker) = first_run_marker_path() {
        if is_first_run_at(&marker)
            && !FIRST_RUN_OPENER_SPAWNED.swap(true, std::sync::atomic::Ordering::SeqCst)
        {
            spawn_first_run_dashboard_opener(log_dir);
        }
    }

    loop {
        // Notify tray that we're starting
        #[cfg(any(target_os = "windows", target_os = "macos"))]
        if let Some((_, status_tx)) = tray {
            status_tx.send(WrapperStatus::Running).ok();
        }

        let stderr_path = log_dir.join("freenet.error.log.last");
        let stderr_file = std::fs::File::create(&stderr_path).ok();

        let mut cmd = std::process::Command::new(&exe_path);
        cmd.arg("network");

        // On Windows, prevent a console window from appearing for the child process.
        // The wrapper has already detached from the console via FreeConsole(),
        // which invalidates the standard handles. We must explicitly set
        // stdin/stdout to null to avoid inheriting the invalid handles
        // (otherwise spawn fails with "The handle is invalid" os error 6).
        #[cfg(target_os = "windows")]
        {
            use std::os::windows::process::CommandExt;
            const CREATE_NO_WINDOW: u32 = 0x08000000;
            cmd.creation_flags(CREATE_NO_WINDOW);
            cmd.stdin(std::process::Stdio::null());
            cmd.stdout(std::process::Stdio::null());
        }

        // Redirect stderr to a file for port-conflict detection.
        // On Windows, stderr must also be explicitly set to avoid
        // inheriting the invalid handle after FreeConsole().
        if let Some(stderr_file) = stderr_file {
            cmd.stderr(stderr_file);
        } else {
            #[cfg(target_os = "windows")]
            cmd.stderr(std::process::Stdio::null());
        }

        // Use spawn + polling so we can handle tray actions while child runs
        let mut child = match cmd.spawn() {
            Ok(c) => c,
            Err(e) => {
                log_wrapper_event(log_dir, &format!("Failed to spawn freenet network: {e}"));
                return Err(e).context("Failed to spawn freenet network");
            }
        };

        // Poll child + tray actions until child exits or a tray action kills it
        let exit_code = loop {
            // Check if child has exited
            match child.try_wait() {
                Ok(Some(status)) => break status.code().unwrap_or(1),
                Ok(None) => {} // still running
                Err(e) => {
                    log_wrapper_event(log_dir, &format!("Error waiting for child: {e}"));
                    break 1;
                }
            }

            // Process tray actions while child is running
            #[cfg(any(target_os = "windows", target_os = "macos"))]
            if let Some((action_rx, status_tx)) = tray {
                if let Ok(action) = action_rx.try_recv() {
                    match action {
                        super::tray::TrayAction::Quit => {
                            drop(child.kill());
                            drop(child.wait());
                            return Ok(());
                        }
                        super::tray::TrayAction::Restart => {
                            log_wrapper_event(log_dir, "Restart requested via tray");
                            drop(child.kill());
                            drop(child.wait());
                            break SENTINEL_RESTART;
                        }
                        super::tray::TrayAction::Stop => {
                            log_wrapper_event(log_dir, "Stop requested via tray");
                            drop(child.kill());
                            drop(child.wait());
                            break SENTINEL_STOP;
                        }
                        super::tray::TrayAction::Start => {
                            // Ignored while child is running — Start is only
                            // meaningful from the stopped state (handled below).
                        }
                        super::tray::TrayAction::ViewLogs => super::tray::open_log_file(),
                        super::tray::TrayAction::CheckUpdate => {
                            // Run the actual update (not just --check). If it
                            // succeeds (exit 0), kill the child and re-exec the
                            // wrapper so the tray shows the correct new version.
                            // Exit 2 means already up to date — no restart needed.
                            status_tx.send(WrapperStatus::Updating).ok();
                            let result = spawn_update_command(&exe_path);
                            match result {
                                Ok(s) if s.success() => {
                                    log_wrapper_event(
                                        log_dir,
                                        "Update installed via tray, restarting wrapper...",
                                    );
                                    status_tx.send(WrapperStatus::UpdatedRestarting).ok();
                                    drop(child.kill());
                                    drop(child.wait());
                                    if spawn_new_wrapper(&exe_path, log_dir) {
                                        return Ok(());
                                    }
                                    // Spawn failed. Fall through to relaunch child
                                    // with the current (old) wrapper rather than
                                    // leaving no node running.
                                    break SENTINEL_RESTART;
                                }
                                Ok(s)
                                    if s.code()
                                        == Some(super::update::EXIT_CODE_BUNDLE_UPDATE_STAGED) =>
                                {
                                    // macOS DMG-swap: the detached updater
                                    // will swap /Applications/Freenet.app
                                    // after this process exits and relaunch
                                    // the new bundle. Do NOT re-exec.
                                    log_wrapper_event(
                                        log_dir,
                                        "Bundle update staged; exiting for updater to take over",
                                    );
                                    status_tx.send(WrapperStatus::UpdatedRestarting).ok();
                                    drop(child.kill());
                                    drop(child.wait());
                                    return Ok(());
                                }
                                Ok(_) => {
                                    // Exit code 2 = already up to date, or other
                                    // non-zero = update failed. Either way, no restart.
                                    log_wrapper_event(log_dir, "No update available");
                                    status_tx.send(WrapperStatus::UpToDate).ok();
                                }
                                Err(e) => {
                                    log_wrapper_event(
                                        log_dir,
                                        &format!("Update check failed: {e}"),
                                    );
                                    status_tx.send(WrapperStatus::Running).ok();
                                }
                            }
                        }
                        super::tray::TrayAction::OpenDashboard => {
                            open_url_in_browser(DASHBOARD_URL);
                        }
                    }
                }
            }

            // Sleep briefly before polling again
            std::thread::sleep(std::time::Duration::from_millis(250));
        };

        // Restart sentinel from tray Restart action — skip exit code handling
        if exit_code == SENTINEL_RESTART {
            continue;
        }

        // Stop sentinel — enter stopped state, wait for Start/Quit/Restart
        if exit_code == SENTINEL_STOP {
            #[cfg(any(target_os = "windows", target_os = "macos"))]
            if let Some((action_rx, status_tx)) = tray {
                status_tx.send(WrapperStatus::Stopped).ok();
                loop {
                    if let Ok(action) = action_rx.try_recv() {
                        match action {
                            super::tray::TrayAction::Start | super::tray::TrayAction::Restart => {
                                log_wrapper_event(log_dir, "Start requested via tray");
                                break; // exit stopped loop, outer loop will relaunch
                            }
                            super::tray::TrayAction::Quit => {
                                return Ok(());
                            }
                            super::tray::TrayAction::ViewLogs => {
                                super::tray::open_log_file();
                            }
                            super::tray::TrayAction::CheckUpdate => {
                                // Allow checking for updates even while stopped
                                status_tx.send(WrapperStatus::Updating).ok();
                                let result = spawn_update_command(&exe_path);
                                match result {
                                    Ok(s) if s.success() => {
                                        log_wrapper_event(
                                            log_dir,
                                            "Update installed while stopped, restarting wrapper...",
                                        );
                                        status_tx.send(WrapperStatus::UpdatedRestarting).ok();
                                        if spawn_new_wrapper(&exe_path, log_dir) {
                                            return Ok(());
                                        }
                                        // Spawn failed. Exit stopped state and
                                        // relaunch child with the current wrapper.
                                        break;
                                    }
                                    Ok(s)
                                        if s.code()
                                            == Some(
                                                super::update::EXIT_CODE_BUNDLE_UPDATE_STAGED,
                                            ) =>
                                    {
                                        log_wrapper_event(
                                            log_dir,
                                            "Bundle update staged while stopped; exiting for updater",
                                        );
                                        status_tx.send(WrapperStatus::UpdatedRestarting).ok();
                                        return Ok(());
                                    }
                                    Ok(_) => {
                                        log_wrapper_event(log_dir, "No update available");
                                        status_tx.send(WrapperStatus::UpToDate).ok();
                                    }
                                    Err(e) => {
                                        log_wrapper_event(
                                            log_dir,
                                            &format!("Update check failed: {e}"),
                                        );
                                    }
                                }
                            }
                            // These actions are no-ops while the node is stopped.
                            super::tray::TrayAction::OpenDashboard
                            | super::tray::TrayAction::Stop => {}
                        }
                    }
                    std::thread::sleep(std::time::Duration::from_millis(250));
                }
            }
            continue;
        }

        // Determine if this is a port conflict (for the state machine)
        let is_port_conflict = stderr_path
            .exists()
            .then(|| std::fs::read_to_string(&stderr_path).unwrap_or_default())
            .map(|s| s.contains("already in use"))
            .unwrap_or(false);

        // For exit code 42, run the update first and pass result to state machine.
        // On Windows, this works because replace_binary() renames the running exe
        // (freenet.exe → freenet.exe.old) rather than deleting it. Windows allows
        // renaming a running executable. The new binary is placed at the original
        // path, so the next child launch uses it. The .old file is cleaned up on
        // the subsequent update.
        let update_succeeded = if exit_code == WRAPPER_EXIT_UPDATE_NEEDED {
            log_wrapper_event(log_dir, "Update needed, running freenet update...");

            #[cfg(any(target_os = "windows", target_os = "macos"))]
            if let Some((_, status_tx)) = tray {
                status_tx.send(WrapperStatus::Updating).ok();
            }

            let result = spawn_update_command(&exe_path);
            let outcome = super::update::classify_update_subprocess(&result);

            // Drive the persistent auto-update failure counter used by
            // `check_if_update_available` to break the exit-42 restart loop
            // (#3934). The split between `SpawnFailed` (records) and
            // `OtherFailure` (no change) is deliberate: transient GitHub/
            // network errors must NOT accumulate toward the lockout
            // (Codex P1 review on PR #3941), only environmental failures
            // (exe missing/locked) should. Install-stage failures (AV
            // holding freenet.exe) are recorded inside the subprocess
            // itself at `update.rs` on `replace_binary` error, so those
            // still lock out after MAX_UPDATE_FAILURES.
            match super::update::update_counter_action(outcome) {
                super::update::UpdateCounterAction::Clear => {
                    super::auto_update::clear_update_failures();
                }
                super::update::UpdateCounterAction::Record => {
                    super::auto_update::record_update_failure();
                }
                super::update::UpdateCounterAction::NoChange => {}
            }

            // macOS DMG-swap: if the update subprocess exits with the
            // bundle-staged code, the detached updater takes over after
            // this process exits. We must not re-exec or retry; just
            // return Ok so the wrapper exits cleanly and the updater can
            // swap /Applications/Freenet.app.
            if outcome == super::update::UpdateSubprocessOutcome::BundleUpdateStaged {
                log_wrapper_event(
                    log_dir,
                    "Bundle update staged during auto-update; exiting for updater",
                );
                #[cfg(any(target_os = "windows", target_os = "macos"))]
                if let Some((_, status_tx)) = tray {
                    status_tx.send(WrapperStatus::UpdatedRestarting).ok();
                }
                return Ok(());
            }

            let ok = outcome == super::update::UpdateSubprocessOutcome::BinaryReplaced;
            if ok {
                log_wrapper_event(log_dir, "Update successful, restarting...");
            } else {
                log_wrapper_event(log_dir, "Update failed");
            }
            Some(ok)
        } else {
            None
        };

        // Use the tested state machine to determine next action
        let action = next_wrapper_action(&mut state, exit_code, is_port_conflict, update_succeeded);

        match action {
            WrapperAction::Update => {
                // Update succeeded — re-exec the wrapper so the tray shows
                // the correct new version (compiled-in to the new binary).
                #[cfg(any(target_os = "windows", target_os = "macos"))]
                if let Some((_, status_tx)) = tray {
                    status_tx.send(WrapperStatus::UpdatedRestarting).ok();
                    // Brief pause so the tray can display the message
                    std::thread::sleep(std::time::Duration::from_secs(2));
                }
                if spawn_new_wrapper(&exe_path, log_dir) {
                    return Ok(());
                }
                // Spawn failed — fall through to relaunch child with current wrapper.
            }
            WrapperAction::Exit => {
                let reason = if exit_code == WRAPPER_EXIT_ALREADY_RUNNING {
                    "Another instance is already running, exiting cleanly"
                } else if exit_code == 0 {
                    "Normal shutdown"
                } else {
                    "Exiting"
                };
                log_wrapper_event(log_dir, reason);
                return Ok(());
            }
            WrapperAction::KillAndRetry => {
                log_wrapper_event(
                    log_dir,
                    &format!(
                        "Port conflict detected (attempt {}/{WRAPPER_MAX_PORT_CONFLICT_KILLS}) — killing stale process and retrying...",
                        state.port_conflict_kills,
                    ),
                );
                kill_stale_freenet_processes(log_dir);
                std::thread::sleep(std::time::Duration::from_secs(2));
            }
            WrapperAction::BackoffAndRelaunch { secs } => {
                if state.consecutive_failures >= WRAPPER_MAX_CONSECUTIVE_FAILURES {
                    log_wrapper_event(
                        log_dir,
                        &format!(
                            "Giving up after {} consecutive failures. \
                             Run 'freenet network' manually to diagnose.",
                            state.consecutive_failures,
                        ),
                    );
                    return Ok(());
                }

                #[cfg(any(target_os = "windows", target_os = "macos"))]
                if let Some((_, status_tx)) = tray {
                    status_tx.send(WrapperStatus::Stopped).ok();
                }

                log_wrapper_event(
                    log_dir,
                    &format!("Exited with code {exit_code}, restarting after {secs}s backoff..."),
                );
                // Loop: CheckUpdate resumes backoff; Relaunch/Quit/Completed break out.
                loop {
                    #[cfg(any(target_os = "windows", target_os = "macos"))]
                    let interrupt = sleep_with_jitter_interruptible(secs, tray.map(|(rx, _)| rx));
                    #[cfg(not(any(target_os = "windows", target_os = "macos")))]
                    let interrupt = sleep_with_jitter_interruptible(secs, None);
                    match interrupt {
                        BackoffInterrupt::Quit => return Ok(()),
                        BackoffInterrupt::Relaunch => {
                            log_wrapper_event(log_dir, "Relaunch requested during backoff");
                            state.consecutive_failures = 0;
                            break; // relaunch
                        }
                        BackoffInterrupt::CheckUpdate => {
                            // Run the update check. Only relaunch if an update
                            // was actually installed; otherwise resume backoff.
                            // This prevents users from defeating the crash
                            // backoff by repeatedly clicking "Check for Updates".
                            log_wrapper_event(log_dir, "Update check requested during backoff");
                            #[cfg(any(target_os = "windows", target_os = "macos"))]
                            if let Some((_, status_tx)) = tray {
                                status_tx.send(WrapperStatus::Updating).ok();
                                let result = spawn_update_command(&exe_path);
                                let outcome = super::update::classify_update_subprocess(&result);

                                // Drive the persistent failure counter for
                                // the user-initiated check path identically
                                // to the auto-update path above — see the
                                // long comment there for the SpawnFailed vs.
                                // OtherFailure rationale (#3934 / Codex P1).
                                match super::update::update_counter_action(outcome) {
                                    super::update::UpdateCounterAction::Clear => {
                                        super::auto_update::clear_update_failures();
                                    }
                                    super::update::UpdateCounterAction::Record => {
                                        super::auto_update::record_update_failure();
                                    }
                                    super::update::UpdateCounterAction::NoChange => {}
                                }

                                match outcome {
                                    super::update::UpdateSubprocessOutcome::BinaryReplaced => {
                                        log_wrapper_event(
                                            log_dir,
                                            "Update installed during backoff, restarting wrapper...",
                                        );
                                        status_tx.send(WrapperStatus::UpdatedRestarting).ok();
                                        if spawn_new_wrapper(&exe_path, log_dir) {
                                            return Ok(());
                                        }
                                        // Spawn failed. Relaunch with current wrapper.
                                        state.consecutive_failures = 0;
                                        break;
                                    }
                                    super::update::UpdateSubprocessOutcome::BundleUpdateStaged => {
                                        log_wrapper_event(
                                            log_dir,
                                            "Bundle update staged during backoff; exiting for updater",
                                        );
                                        status_tx.send(WrapperStatus::UpdatedRestarting).ok();
                                        return Ok(());
                                    }
                                    super::update::UpdateSubprocessOutcome::AlreadyUpToDate => {
                                        log_wrapper_event(log_dir, "No update available");
                                        status_tx.send(WrapperStatus::UpToDate).ok();
                                    }
                                    super::update::UpdateSubprocessOutcome::SpawnFailed => {
                                        let msg = match &result {
                                            Err(e) => {
                                                format!("Update subprocess failed to spawn: {e}")
                                            }
                                            Ok(_) => {
                                                // Unreachable: classify only returns
                                                // SpawnFailed for Err results.
                                                "Update subprocess failed to spawn".to_string()
                                            }
                                        };
                                        log_wrapper_event(log_dir, &msg);
                                        status_tx.send(WrapperStatus::Stopped).ok();
                                    }
                                    super::update::UpdateSubprocessOutcome::OtherFailure => {
                                        let msg = if let Ok(s) = &result {
                                            format!(
                                                "Update check failed with exit code {:?}",
                                                s.code()
                                            )
                                        } else {
                                            // Unreachable: classify returns
                                            // SpawnFailed for Err results.
                                            "Update check failed".to_string()
                                        };
                                        log_wrapper_event(log_dir, &msg);
                                        status_tx.send(WrapperStatus::Stopped).ok();
                                    }
                                }
                            }
                            // No update installed — resume backoff
                            continue;
                        }
                        BackoffInterrupt::Completed => break, // normal backoff done
                    }
                }
            }
        }
    }
}

/// Maximum number of wrapper log files to keep (one per day).
const WRAPPER_LOG_RETENTION_DAYS: usize = 7;

/// Append a timestamped message to a date-based wrapper log file.
/// Files are named `freenet-wrapper.YYYY-MM-DD.log` with automatic rotation.
/// Old files beyond `WRAPPER_LOG_RETENTION_DAYS` are deleted.
fn log_wrapper_event(log_dir: &Path, message: &str) {
    use std::io::Write;

    let now = chrono::Local::now();
    let date_str = now.format("%Y-%m-%d");
    let log_path = log_dir.join(format!("freenet-wrapper.{date_str}.log"));

    if let Ok(mut file) = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(&log_path)
    {
        let timestamp = now.format("%H:%M:%S");
        drop(writeln!(file, "{timestamp}: {message}"));
    }
    // Also print to stderr for debugging when run manually
    eprintln!("{message}");

    // Clean up old log files (best-effort, don't let cleanup failure block anything)
    cleanup_old_wrapper_logs(log_dir);
}

/// Delete wrapper log files older than the retention period.
fn cleanup_old_wrapper_logs(log_dir: &Path) {
    let entries = match std::fs::read_dir(log_dir) {
        Ok(e) => e,
        Err(_) => return,
    };

    let mut wrapper_logs: Vec<std::path::PathBuf> = entries
        .filter_map(|e| e.ok())
        .map(|e| e.path())
        .filter(|p| {
            p.file_name()
                .and_then(|n| n.to_str())
                .map(|n| n.starts_with("freenet-wrapper.") && n.ends_with(".log"))
                .unwrap_or(false)
        })
        .collect();

    if wrapper_logs.len() <= WRAPPER_LOG_RETENTION_DAYS {
        return;
    }

    // Sort by name (date is embedded, so lexicographic = chronological)
    wrapper_logs.sort();

    // Remove oldest files beyond retention
    let to_remove = wrapper_logs.len() - WRAPPER_LOG_RETENTION_DAYS;
    for path in &wrapper_logs[..to_remove] {
        drop(std::fs::remove_file(path));
    }
}

/// Kill stale `freenet network` processes from a previous wrapper instance.
fn kill_stale_freenet_processes(log_dir: &Path) {
    #[cfg(unix)]
    {
        // Scope to current user to avoid killing other users' processes on shared machines.
        // Mirrors the macOS wrapper script: pkill -f -u "$(id -u)" "freenet network"
        let uid = std::process::Command::new("id")
            .arg("-u")
            .output()
            .ok()
            .and_then(|o| String::from_utf8(o.stdout).ok())
            .unwrap_or_default();
        let uid = uid.trim();
        let status = std::process::Command::new("pkill")
            .args(["-f", "-u", uid, "freenet network"])
            .status();
        if let Ok(s) = status {
            if s.success() {
                log_wrapper_event(
                    log_dir,
                    "Killed stale freenet network process(es) on startup",
                );
                std::thread::sleep(std::time::Duration::from_secs(2));
            }
        }
    }

    #[cfg(target_os = "windows")]
    {
        // Reap orphaned `network` node children left by a previous wrapper
        // instance. Only nodes are targeted — never a wrapper or another
        // `freenet` subcommand. A single pass suffices here (unlike the loop
        // in `kill_freenet_service_processes`): this runs at wrapper startup
        // and on the port-conflict retry, when the only live wrapper is this
        // process, so no other wrapper exists to respawn a reaped node.
        // Shares the PowerShell-based enumeration used by
        // `kill_freenet_service_processes`; `wmic` (used here previously) is
        // deprecated and absent by default on recent Windows releases.
        if kill_freenet_processes_matching(FreenetServiceProcess::Node) > 0 {
            log_wrapper_event(
                log_dir,
                "Killed stale freenet network process(es) on startup",
            );
            std::thread::sleep(std::time::Duration::from_secs(2));
        }
    }
}

/// Determine whether the user wants to purge data directories.
///
/// - `--purge` → true
/// - `--keep-data` → false
/// - Neither → prompt interactively (defaults to false if stdin is not a TTY)
pub fn should_purge(purge: bool, keep_data: bool) -> Result<bool> {
    if purge {
        return Ok(true);
    }
    if keep_data {
        return Ok(false);
    }

    use std::io::{self, BufRead, IsTerminal, Write};

    // Check if stdin is a TTY for interactive prompting
    if io::stdin().is_terminal() {
        print!("Also remove all Freenet data, config, and logs? [y/N] ");
        io::stdout().flush()?;
        let mut line = String::new();
        io::stdin().lock().read_line(&mut line)?;
        let answer = line.trim().to_ascii_lowercase();
        Ok(answer == "y" || answer == "yes")
    } else {
        println!(
            "Non-interactive mode: keeping data. Use --purge to also remove data, config, and logs."
        );
        Ok(false)
    }
}

/// Remove a directory if it exists, printing what is being removed.
fn remove_if_exists(label: &str, path: &Path) -> Result<()> {
    if path.exists() {
        println!("Removing {label}: {}", path.display());
        std::fs::remove_dir_all(path)
            .with_context(|| format!("Failed to remove {label} directory: {}", path.display()))?;
    }
    Ok(())
}

/// Re-export for callers outside this module (e.g. `uninstall::run`) that
/// also need to collapse empty parent folders after their own cleanup
/// passes. Runtime use is Windows-only (see `uninstall::collapse_windows_bin_tree`)
/// but the function is exposed on every platform so that tests for the
/// caller-side helper can run under Linux CI too.
pub(super) fn remove_dir_if_empty_pub(path: &Path) {
    remove_dir_if_empty(path)
}

/// Remove a directory only if it is empty. Unlike `remove_if_exists`, this
/// never recursively deletes — it is intended for cleaning up empty parent
/// folders after their children have been removed (e.g. collapsing an empty
/// `%APPDATA%\The Freenet Project Inc\Freenet\` once its `config` subfolder
/// is gone). Any error other than "not empty" is swallowed; the parent is
/// expendable and we should not fail the uninstall over it.
fn remove_dir_if_empty(path: &Path) {
    if !path.is_dir() {
        return;
    }
    let Ok(mut entries) = std::fs::read_dir(path) else {
        return;
    };
    if entries.next().is_some() {
        return;
    }
    match std::fs::remove_dir(path) {
        Ok(()) => println!("Removing empty dir: {}", path.display()),
        Err(err) => {
            // Best-effort: the parent may have been re-populated by a
            // concurrent process, or the user lacks permission. Either
            // way, don't abort the uninstall.
            eprintln!("Note: could not remove empty dir {}: {err}", path.display());
        }
    }
}

/// Public wrapper for use by the `uninstall` command.
pub fn purge_data(system_mode: bool) -> Result<()> {
    purge_data_dirs(system_mode)
}

/// Remove Freenet data, config, cache, and log directories.
///
/// When `system_mode` is true on Linux, resolves directories for the service
/// user (via SUDO_USER) rather than root's home directory.
fn purge_data_dirs(#[allow(unused_variables)] system_mode: bool) -> Result<()> {
    // On Linux with --system, the service runs as the SUDO_USER, not root.
    // We need to resolve that user's directories, not root's.
    #[cfg(target_os = "linux")]
    let home_override: Option<std::path::PathBuf> = if system_mode {
        std::env::var("SUDO_USER")
            .ok()
            .map(|u| home_dir_for_user(&u))
    } else {
        None
    };
    #[cfg(not(target_os = "linux"))]
    let home_override: Option<std::path::PathBuf> = None;

    // If we have a home override (system mode on Linux), purge the service
    // user's XDG dirs via the manually-constructed paths. Otherwise use
    // ProjectDirs, which resolves from the current user.
    if let Some(ref home) = home_override {
        for (label, dir) in linux_system_purge_dirs(home) {
            remove_if_exists(label, &dir)?;
        }
    } else {
        let leaves = DataLeaves::from_project_dirs();
        purge_leaves_and_collapse(&leaves)?;
    }

    Ok(())
}

/// XDG leaf directories that `--system` mode must purge for the service user.
///
/// These MUST match the paths the running node actually creates. On Linux the
/// `directories` crate lowercases the application name when building
/// `ProjectDirs` (`ProjectDirs::from("", "The Freenet Project Inc", "Freenet")`
/// yields `~/.local/share/freenet`, not `~/.local/share/Freenet`), and
/// `get_log_dir` uses `~/.local/state/freenet`. The previous hardcoded
/// uppercase `Freenet` paths matched nothing on disk, so
/// `sudo freenet uninstall --purge --system` reported success while silently
/// leaving all of the user's contracts, delegates, and database behind (#3907).
///
/// Extracted as a pure function (parameterised on `home`) so the path logic is
/// unit-testable without mutating process-level `SUDO_USER`/home state. This is
/// only reached in `--system` mode, which only resolves a `home_override` on
/// Linux; the `cfg_attr` suppresses the dead-code lint on macOS/Windows, where
/// `home_override` is always `None` so the function is never called.
#[cfg_attr(not(target_os = "linux"), allow(dead_code))]
fn linux_system_purge_dirs(home: &Path) -> [(&'static str, PathBuf); 4] {
    [
        ("data", home.join(".local/share/freenet")),
        ("config", home.join(".config/freenet")),
        ("cache", home.join(".cache/freenet")),
        ("logs", home.join(".local/state/freenet")),
    ]
}

/// The full set of leaf directories `purge_data_dirs` needs to touch in
/// non-system mode. Grouping these into a value type makes the purge logic
/// testable without mocking `ProjectDirs` (a tricky proposition given it
/// reads process-level env vars).
#[derive(Debug, Default, Clone)]
struct DataLeaves {
    /// `data_local_dir` — on Windows this is `%LOCALAPPDATA%\...\data`.
    data_local: Option<PathBuf>,
    /// Pre-#3739 Roaming data path, only populated on Windows where it
    /// differs from `data_local`.
    data_roaming: Option<PathBuf>,
    /// `config_dir` (Roaming on Windows, e.g.
    /// `%APPDATA%\...\Freenet\config`). Only populated when it differs
    /// from both data paths (matches the macOS case where
    /// `config_dir == data_dir`). On Windows the running node does
    /// not write here — it writes to `config_local` — but several
    /// other call sites (`config.rs:1661` id-set fallback, `report.rs`
    /// config-file scan) still resolve through `config_dir()`, and an
    /// older install may have written to it before the live node
    /// switched to Local AppData. Cleaning both is correct.
    config: Option<PathBuf>,
    /// `config_local_dir` (Local on Windows, e.g.
    /// `%LOCALAPPDATA%\...\Freenet\config`). This is what the running
    /// node actually writes to (`Config::build` in config.rs uses
    /// `defaults.config_local_dir()`), which is *not* the same as
    /// `config_dir` on Windows. Without this, `freenet uninstall
    /// --purge` left the live config folder behind. Only populated
    /// when distinct from `data_local`, `data_roaming`, and `config`
    /// to avoid double removal — on Linux/macOS the `directories`
    /// crate aliases `config_local_dir` to `config_dir`, so this
    /// stays `None` and the existing leaves cover those platforms.
    config_local: Option<PathBuf>,
    /// `cache_dir` for the uppercase project bundle.
    cache: Option<PathBuf>,
    /// Lowercase-variant cache used by the webapp cache on case-sensitive
    /// filesystems.
    cache_lowercase: Option<PathBuf>,
    /// Log dir (as returned by `tracing::get_log_dir`).
    log: Option<PathBuf>,
    /// Whether the parents of our leaves are Freenet-owned and therefore
    /// safe to collapse if empty.
    ///
    /// True on Windows — `ProjectDirs` there builds
    /// `%LOCALAPPDATA%\The Freenet Project Inc\Freenet\{data,config,cache}`
    /// and `get_log_dir` returns `%LOCALAPPDATA%\freenet\logs`; the
    /// immediate parents (`...\Freenet`, `...\freenet`) exist only for us.
    ///
    /// False on Linux/macOS. On Linux the `directories` crate lowercases the
    /// application name, so `ProjectDirs` builds `~/.local/share/freenet`,
    /// `~/.config/freenet`, `~/.cache/freenet` (note the lowercase `freenet`,
    /// not `Freenet`); macOS uses its own `~/Library/...` scheme. The parents
    /// of those leaves are shared XDG/OS hierarchies used by every other app
    /// on the system — collapsing them would at best no-op (by luck) and at
    /// worst delete an otherwise-empty shared root on a fresh account. The
    /// safety must be enforced at the type level, not left to
    /// `remove_dir_if_empty`'s runtime check.
    collapse_parents: bool,
}

impl DataLeaves {
    fn from_project_dirs() -> Self {
        let mut leaves = DataLeaves::default();

        if let Some(dirs) = ProjectDirs::from("", "The Freenet Project Inc", "Freenet") {
            let data_dir = dirs.data_local_dir().to_path_buf();
            leaves.data_local = Some(data_dir.clone());

            let roaming = dirs.data_dir().to_path_buf();
            if roaming != data_dir {
                leaves.data_roaming = Some(roaming.clone());
            }

            let config_dir = dirs.config_dir().to_path_buf();
            if config_dir != data_dir && Some(&config_dir) != leaves.data_roaming.as_ref() {
                leaves.config = Some(config_dir);
            }

            let config_local_dir = dirs.config_local_dir().to_path_buf();
            if config_local_dir != data_dir
                && Some(&config_local_dir) != leaves.data_roaming.as_ref()
                && Some(&config_local_dir) != leaves.config.as_ref()
            {
                leaves.config_local = Some(config_local_dir);
            }

            leaves.cache = Some(dirs.cache_dir().to_path_buf());
        } else {
            eprintln!(
                "Warning: Could not determine Freenet directories. Data and config may not have been removed."
            );
        }

        if let Some(dirs) = ProjectDirs::from("", "The Freenet Project Inc", "freenet") {
            let cache_lower = dirs.cache_dir().to_path_buf();
            if cache_lower.exists() {
                leaves.cache_lowercase = Some(cache_lower);
            }
        }

        leaves.log = get_log_dir();
        leaves.collapse_parents = cfg!(target_os = "windows");

        leaves
    }
}

/// Remove every populated leaf directory and then collapse any Freenet-owned
/// parent folder that the removal left empty. This is the #3904 fix: on
/// Windows the per-leaf calls removed `...\Freenet\config` but not its now-
/// empty `...\Freenet\` parent. By collecting parents, deduping, and visiting
/// them deepest-first, we tidy up without ever attempting to remove a parent
/// that still holds a sibling app's data.
fn purge_leaves_and_collapse(leaves: &DataLeaves) -> Result<()> {
    let mut parents: Vec<PathBuf> = Vec::new();
    let collect = |leaf: &Path, acc: &mut Vec<PathBuf>| {
        if leaves.collapse_parents {
            push_parent(leaf, acc);
        }
    };

    if let Some(ref data_local) = leaves.data_local {
        remove_if_exists("data", data_local)?;
        collect(data_local, &mut parents);
    }
    if let Some(ref roaming) = leaves.data_roaming {
        remove_if_exists("data (legacy roaming)", roaming)?;
        collect(roaming, &mut parents);
    }
    if let Some(ref config) = leaves.config {
        remove_if_exists("config (legacy roaming)", config)?;
        collect(config, &mut parents);
    }
    if let Some(ref config_local) = leaves.config_local {
        remove_if_exists("config", config_local)?;
        collect(config_local, &mut parents);
    }
    if let Some(ref cache) = leaves.cache {
        remove_if_exists("cache", cache)?;
        collect(cache, &mut parents);
    }
    if let Some(ref cache_lower) = leaves.cache_lowercase {
        remove_if_exists("cache", cache_lower)?;
        collect(cache_lower, &mut parents);
    }
    if let Some(ref log) = leaves.log {
        remove_if_exists("logs", log)?;
        collect(log, &mut parents);
    }

    // Sort ascending, dedup consecutive duplicates, then reverse so that
    // deepest paths are processed first. If a hypothetical future caller
    // ever adds a grandparent alongside its parent, this ordering ensures
    // the grandparent is only evaluated after its child has been collapsed.
    parents.sort();
    parents.dedup();
    for parent in parents.into_iter().rev() {
        remove_dir_if_empty(&parent);
    }

    Ok(())
}

fn push_parent(leaf: &Path, acc: &mut Vec<PathBuf>) {
    if let Some(parent) = leaf.parent() {
        acc.push(parent.to_path_buf());
    }
}

/// Path to the system-wide systemd service file.
#[cfg(target_os = "linux")]
const SYSTEM_SERVICE_PATH: &str = "/etc/systemd/system/freenet.service";

/// Check if a system-wide Freenet service is installed.
#[cfg(target_os = "linux")]
fn has_system_service() -> bool {
    Path::new(SYSTEM_SERVICE_PATH).exists()
}

/// Check if a user-level Freenet service is installed.
#[cfg(target_os = "linux")]
fn has_user_service() -> bool {
    dirs::home_dir()
        .map(|h| h.join(".config/systemd/user/freenet.service").exists())
        .unwrap_or(false)
}

/// Recursively chown a directory to the given user (best-effort).
/// Used after creating directories with sudo so the service user can write to them.
#[cfg(target_os = "linux")]
fn chown_to_user(path: &Path, username: &str) {
    let _status = std::process::Command::new("chown")
        .args(["-R", username, &path.display().to_string()])
        .status();
}

/// Look up a user's home directory from /etc/passwd via `getent passwd`.
/// Falls back to `/home/{username}` if getent is unavailable.
#[cfg(target_os = "linux")]
fn home_dir_for_user(username: &str) -> std::path::PathBuf {
    // Try getent passwd which works with NSS (LDAP, NIS, etc.)
    if let Ok(output) = std::process::Command::new("getent")
        .args(["passwd", username])
        .output()
    {
        if output.status.success() {
            let line = String::from_utf8_lossy(&output.stdout);
            // Format: username:x:uid:gid:gecos:home:shell
            if let Some(home) = line.split(':').nth(5) {
                let home = home.trim();
                if !home.is_empty() {
                    return std::path::PathBuf::from(home);
                }
            }
        }
    }
    std::path::PathBuf::from(format!("/home/{username}"))
}

/// Resolve whether to use system or user mode.
/// If `--system` is passed, use system mode. Otherwise auto-detect based on
/// which service file exists, defaulting to user mode.
#[cfg(target_os = "linux")]
fn use_system_mode(system_flag: bool) -> bool {
    // Auto-detect: if only system service exists, use system mode
    system_flag || (has_system_service() && !has_user_service())
}

/// Run a systemctl command, using --user or not based on system mode.
#[cfg(target_os = "linux")]
fn systemctl(system_mode: bool, args: &[&str]) -> Result<std::process::ExitStatus> {
    let mut cmd = std::process::Command::new("systemctl");
    if !system_mode {
        cmd.arg("--user");
    }
    cmd.args(args);
    let status = cmd.status().context("Failed to run systemctl")?;
    Ok(status)
}

/// Run a systemctl command with helpful error on user-session failures.
#[cfg(target_os = "linux")]
fn systemctl_with_hint(system_mode: bool, args: &[&str], action: &str) -> Result<()> {
    let status = systemctl(system_mode, args)?;
    if status.success() {
        return Ok(());
    }

    if system_mode {
        anyhow::bail!("Failed to {action}");
    }

    // Check if this looks like a user session bus issue
    let hint = std::process::Command::new("systemctl")
        .args(["--user", "daemon-reload"])
        .stderr(std::process::Stdio::piped())
        .output()
        .ok()
        .and_then(|out| {
            let stderr = String::from_utf8_lossy(&out.stderr);
            if stderr.contains("bus")
                || stderr.contains("XDG_RUNTIME_DIR")
                || stderr.contains("Failed to connect")
            {
                Some(
                    "\n\nHint: User systemd session not available (common in containers/LXC).\n\
                     Try: sudo freenet service install --system",
                )
            } else {
                None
            }
        })
        .unwrap_or("");

    anyhow::bail!("Failed to {action}{hint}");
}

#[cfg(target_os = "linux")]
fn install_service(system: bool) -> Result<()> {
    if system {
        install_system_service()
    } else {
        install_user_service()
    }
}

#[cfg(target_os = "linux")]
fn install_user_service() -> Result<()> {
    use std::fs;

    let exe_path = std::env::current_exe().context("Failed to get current executable path")?;
    let home_dir = dirs::home_dir().context("Failed to get home directory")?;

    let service_dir = home_dir.join(".config/systemd/user");
    fs::create_dir_all(&service_dir).context("Failed to create systemd user directory")?;

    // Create log directory - use ~/.local/state/freenet for XDG compliance
    let log_dir = home_dir.join(".local/state/freenet");
    fs::create_dir_all(&log_dir).context("Failed to create log directory")?;

    let service_content = generate_user_service_file(&exe_path, &log_dir);
    let service_path = service_dir.join("freenet.service");

    fs::write(&service_path, &service_content).context("Failed to write service file")?;

    // Sidecar records the unit's SHA-256 so a later `freenet update` can
    // distinguish "Freenet's unit" from a hand-edited one before
    // overwriting (#4287). A failed sidecar write only weakens future
    // user-modification protection — warn and continue.
    let hash_path = service_path.with_extension("service.hash");
    let unit_hash = super::update::wrapper_content_hash(&service_content);
    if let Err(e) = super::update::write_wrapper_hash_sidecar(&hash_path, &unit_hash) {
        eprintln!(
            "Warning: failed to write service hash sidecar at {}: {}.",
            hash_path.display(),
            e
        );
    }

    // Reload systemd user daemon
    systemctl_with_hint(false, &["daemon-reload"], "reload systemd daemon")?;

    // Enable the service
    systemctl_with_hint(false, &["enable", "freenet"], "enable service")?;

    println!("Freenet user service installed successfully.");
    println!();
    println!("To start the service now:");
    println!("  freenet service start");
    println!();
    println!("The service will start automatically on login.");
    println!("Logs will be written to: {}", log_dir.display());

    Ok(())
}

#[cfg(target_os = "linux")]
fn install_system_service() -> Result<()> {
    use std::fs;

    let exe_path = std::env::current_exe().context("Failed to get current executable path")?;

    // Get the user to run the service as.
    // When running with sudo, SUDO_USER has the original (non-root) user.
    let username = std::env::var("SUDO_USER")
        .or_else(|_| std::env::var("USER"))
        .or_else(|_| std::env::var("LOGNAME"))
        .context(
            "Could not determine username. Set the USER environment variable \
             or run with sudo (which sets SUDO_USER).",
        )?;

    if username == "root" {
        anyhow::bail!(
            "Refusing to install system service running as root.\n\
             Run with sudo from a non-root user account so SUDO_USER is set,\n\
             or set the USER environment variable to the desired service user."
        );
    }

    // Look up the user's home directory from /etc/passwd.
    // When running with sudo, dirs::home_dir() returns /root which is wrong.
    let home_dir = home_dir_for_user(&username);

    // Create log directory and fix ownership (we're running as root via sudo,
    // but the service will run as the target user).
    let log_dir = home_dir.join(".local/state/freenet");
    fs::create_dir_all(&log_dir).context("Failed to create log directory")?;
    chown_to_user(&log_dir, &username);

    let service_content = generate_system_service_file(&exe_path, &log_dir, &username, &home_dir);

    fs::write(SYSTEM_SERVICE_PATH, &service_content).with_context(|| {
        format!(
            "Failed to write service file to {SYSTEM_SERVICE_PATH}. \
             Are you running as root? Try: sudo freenet service install --system"
        )
    })?;

    // Sidecar records the unit's SHA-256 so a later `freenet update` can
    // distinguish "Freenet's unit" from a hand-edited one before
    // overwriting (#4287). Lives next to the root-owned unit and is
    // written by the same root process, so root owns it too. A failed
    // sidecar write only weakens future user-modification protection —
    // warn and continue.
    let system_hash_path = Path::new(SYSTEM_SERVICE_PATH).with_extension("service.hash");
    let unit_hash = super::update::wrapper_content_hash(&service_content);
    if let Err(e) = super::update::write_wrapper_hash_sidecar(&system_hash_path, &unit_hash) {
        eprintln!(
            "Warning: failed to write service hash sidecar at {}: {}.",
            system_hash_path.display(),
            e
        );
    }

    // Reload systemd daemon (system-level, no --user)
    let status = systemctl(true, &["daemon-reload"])?;
    if !status.success() {
        anyhow::bail!("Failed to reload systemd daemon");
    }

    // Enable the service
    let status = systemctl(true, &["enable", "freenet"])?;
    if !status.success() {
        anyhow::bail!("Failed to enable service");
    }

    println!("Freenet system service installed successfully.");
    println!("  Service runs as user: {username}");
    println!();
    println!("To start the service now:");
    println!("  sudo freenet service start --system");
    println!();
    println!("The service will start automatically on boot.");
    println!("Logs will be written to: {}", log_dir.display());

    Ok(())
}

#[cfg(target_os = "linux")]
pub fn generate_user_service_file(binary_path: &Path, log_dir: &Path) -> String {
    format!(
        r#"[Unit]
Description=Freenet Node
Documentation=https://freenet.org
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
# Stale-orphan self-heal (issue #3967): RestartPreventExitStatus=43 below
# means an exit 43 ("another instance already running") never restarts the
# unit. That is correct for a legitimate second instance, but if the port
# holder is an ORPHANED `freenet network` (PPID=1) still running an OLD
# binary, the unit would stand down and the orphan would serve stale assets
# forever. This pre-flight runs before every start: it finds the port
# holder, and kills it ONLY when it is an init-adopted orphan (PPID==1) whose
# `Freenet version:` line differs from the binary this unit would launch (or
# whose version can't be read). A user-run `freenet network` (parented by a
# shell, PPID!=1) is always left alone, as is a current-version orphan.
#
# systemd performs its own $VAR/${{VAR}} expansion on Exec* lines BEFORE handing
# the string to /bin/sh, so every dollar the SHELL must see is written as $$
# here (systemd collapses $$ -> a single $ for sh). Self-match guards: the
# pre-flight sh excludes its own PID ($$$$ -> the sh's $$) and PID 1, and only
# considers holders whose resolved exe equals this unit's freenet binary, so
# `pgrep -f "freenet network"` never targets the pre-flight's own sh. PPID is
# read after the final ')' in /proc/PID/stat (comm is parenthesized) so a comm
# containing whitespace can't shift the field. The '-' prefix means a failure
# here never blocks the start.
ExecStartPre=-/bin/sh -c 'self=$$$$; bin=$$(readlink -f {binary} 2>/dev/null); ondisk=$$(timeout 5 {binary} --version 2>/dev/null | grep "^Freenet version:"); for pid in $$(pgrep -f -u "$$(id -u)" "freenet network" 2>/dev/null); do [ "$$pid" = "$$self" ] && continue; [ "$$pid" = "1" ] && continue; exe=$$(readlink -f /proc/$$pid/exe 2>/dev/null); [ -n "$$bin" ] && [ "$$exe" != "$$bin" ] && continue; hv=""; [ -x "$$exe" ] && hv=$$(timeout 5 "$$exe" --version 2>/dev/null | grep "^Freenet version:"); ppid=$$(sed "s/.*) //" /proc/$$pid/stat 2>/dev/null | awk "{{print \$$2}}"); mismatch=1; [ -n "$$ondisk" ] && [ -n "$$hv" ] && [ "$$hv" != "$$ondisk" ] && mismatch=0; if [ "$$ppid" = "1" ] && {{ [ "$$mismatch" = "0" ] || [ -z "$$hv" ]; }}; then kill -TERM "$$pid" 2>/dev/null || true; w=0; while kill -0 "$$pid" 2>/dev/null && [ $$w -lt 12 ]; do sleep 1; w=$$((w+1)); done; kill -0 "$$pid" 2>/dev/null && kill -KILL "$$pid" 2>/dev/null || true; fi; done'
ExecStart={binary} network
Restart=always
# Wait 10 seconds before restart to avoid rapid restart loops
RestartSec=10
# Stop restart loop after 5 failures in 2 minutes (e.g., port conflict with
# a stale process). Without this, systemd restarts indefinitely.
# SuccessExitStatus=42 ensures auto-update exits don't count as failures.
StartLimitBurst=5
StartLimitIntervalSec=120
# Allow 45 seconds for graceful shutdown before SIGKILL.
# The node handles SIGTERM by (1) waiting up to `shutdown-drain-secs`
# (default 30s) for in-flight client PUT/GET/UPDATE/SUBSCRIBE drivers
# to finish, then (2) closing peer connections. The 15s headroom over
# the default drain covers peer-connection teardown + spawn-task
# cleanup. If you raise `shutdown-drain-secs`, raise this in lockstep.
TimeoutStopSec=45

# Auto-update: if peer exits with code 42 (version mismatch with gateway),
# run update before systemd restarts the service. The '-' prefix means
# ExecStopPost failure won't affect service restart. $$EXIT_STATUS is doubled
# so systemd passes a literal $EXIT_STATUS through to sh (which systemd itself
# sets in the ExecStopPost environment).
ExecStopPost=-/bin/sh -c '[ "$$EXIT_STATUS" = "42" ] && {binary} update --quiet || true'
# Treat exit code 42 as success so it doesn't count against StartLimitBurst.
# Without this, rapid update cycles (exit 42 → ExecStopPost → restart) can
# exhaust the burst limit and permanently kill the service.
SuccessExitStatus=42 43
# Exit code 43 = another instance is already running on the port.
# Do NOT restart — the existing instance is healthy.
RestartPreventExitStatus=43

# Logging
# - The node's tracing layer writes its own size-capped, hourly-rotated
#   logs to {log_dir}/freenet.YYYY-MM-DD-HH.log (LOG_RETENTION_HOURS +
#   LOG_DIR_MAX_BYTES; see crates/core/src/tracing.rs).
# - systemd's StandardOutput/StandardError previously appended to a fixed
#   freenet.log / freenet.error.log that the time-based cleanup never
#   pruned (mtime stayed fresh while the file was being written), so they
#   grew without bound on long-running nodes (issue #4251).
# - Routing both to the journal lets journald handle rotation, and panics
#   or pre-tracing-init output remain queryable via
#   `journalctl --user-unit freenet`.
StandardOutput=journal
StandardError=journal
SyslogIdentifier=freenet

# Resource limits to prevent runaway resource consumption
# File descriptors needed for network connections
LimitNOFILE=65536
# Memory limit (2GB soft limit for user service)
MemoryMax=2G
# CPU quota (200% = 2 cores max)
CPUQuota=200%

[Install]
WantedBy=default.target
"#,
        binary = binary_path.display(),
        log_dir = log_dir.display()
    )
}

#[cfg(target_os = "linux")]
pub fn generate_system_service_file(
    binary_path: &Path,
    log_dir: &Path,
    username: &str,
    home_dir: &Path,
) -> String {
    format!(
        r#"[Unit]
Description=Freenet Node
Documentation=https://freenet.org
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
User={username}
Environment=HOME={home}
# Stale-orphan self-heal (issue #3967): see the matching comment in the user
# unit (including the systemd $$-escaping, PPID-after-final-')' parse, and the
# self-PID / PID-1 / exe-path self-match guards). RestartPreventExitStatus=43
# means an exit 43 never restarts the unit, so an init-adopted orphan (PPID==1)
# running an OLD binary would hold the port forever. This pre-flight kills the
# holder ONLY when it is such an orphan whose `Freenet version:` differs from
# (or can't be read against) the binary this unit launches; a user-run instance
# (PPID!=1) is always left alone. The '-' prefix means a failure here never
# blocks the start.
ExecStartPre=-/bin/sh -c 'self=$$$$; bin=$$(readlink -f {binary} 2>/dev/null); ondisk=$$(timeout 5 {binary} --version 2>/dev/null | grep "^Freenet version:"); for pid in $$(pgrep -f -u "$$(id -u)" "freenet network" 2>/dev/null); do [ "$$pid" = "$$self" ] && continue; [ "$$pid" = "1" ] && continue; exe=$$(readlink -f /proc/$$pid/exe 2>/dev/null); [ -n "$$bin" ] && [ "$$exe" != "$$bin" ] && continue; hv=""; [ -x "$$exe" ] && hv=$$(timeout 5 "$$exe" --version 2>/dev/null | grep "^Freenet version:"); ppid=$$(sed "s/.*) //" /proc/$$pid/stat 2>/dev/null | awk "{{print \$$2}}"); mismatch=1; [ -n "$$ondisk" ] && [ -n "$$hv" ] && [ "$$hv" != "$$ondisk" ] && mismatch=0; if [ "$$ppid" = "1" ] && {{ [ "$$mismatch" = "0" ] || [ -z "$$hv" ]; }}; then kill -TERM "$$pid" 2>/dev/null || true; w=0; while kill -0 "$$pid" 2>/dev/null && [ $$w -lt 12 ]; do sleep 1; w=$$((w+1)); done; kill -0 "$$pid" 2>/dev/null && kill -KILL "$$pid" 2>/dev/null || true; fi; done'
ExecStart={binary} network
Restart=always
# Wait 10 seconds before restart to avoid rapid restart loops
RestartSec=10
# Stop restart loop after 5 failures in 2 minutes (e.g., port conflict with
# a stale process). Without this, systemd restarts indefinitely.
# SuccessExitStatus=42 ensures auto-update exits don't count as failures.
StartLimitBurst=5
StartLimitIntervalSec=120
# Allow 45 seconds for graceful shutdown before SIGKILL.
# The node handles SIGTERM by (1) waiting up to `shutdown-drain-secs`
# (default 30s) for in-flight client PUT/GET/UPDATE/SUBSCRIBE drivers
# to finish, then (2) closing peer connections. The 15s headroom over
# the default drain covers peer-connection teardown + spawn-task
# cleanup. If you raise `shutdown-drain-secs`, raise this in lockstep.
TimeoutStopSec=45

# Auto-update: if peer exits with code 42 (version mismatch with gateway),
# run update before systemd restarts the service. The '-' prefix means
# ExecStopPost failure won't affect service restart. $$EXIT_STATUS is doubled
# so systemd passes a literal $EXIT_STATUS through to sh (which systemd itself
# sets in the ExecStopPost environment).
ExecStopPost=-/bin/sh -c '[ "$$EXIT_STATUS" = "42" ] && {binary} update --quiet || true'
# Treat exit code 42 as success so it doesn't count against StartLimitBurst.
# Without this, rapid update cycles (exit 42 → ExecStopPost → restart) can
# exhaust the burst limit and permanently kill the service.
SuccessExitStatus=42 43
# Exit code 43 = another instance is already running on the port.
# Do NOT restart — the existing instance is healthy.
RestartPreventExitStatus=43

# Logging
# - The node's tracing layer writes its own size-capped, hourly-rotated
#   logs to {log_dir}/freenet.YYYY-MM-DD-HH.log (LOG_RETENTION_HOURS +
#   LOG_DIR_MAX_BYTES; see crates/core/src/tracing.rs).
# - systemd's StandardOutput/StandardError previously appended to a fixed
#   freenet.log / freenet.error.log that the time-based cleanup never
#   pruned (mtime stayed fresh while the file was being written), so they
#   grew without bound on long-running nodes (issue #4251).
# - Routing both to the journal lets journald handle rotation, and panics
#   or pre-tracing-init output remain queryable via
#   `journalctl -u freenet`.
StandardOutput=journal
StandardError=journal
SyslogIdentifier=freenet

# Resource limits to prevent runaway resource consumption
# File descriptors needed for network connections
LimitNOFILE=65536
# Memory limit (2GB soft limit)
MemoryMax=2G
# CPU quota (200% = 2 cores max)
CPUQuota=200%

[Install]
WantedBy=multi-user.target
"#,
        binary = binary_path.display(),
        log_dir = log_dir.display(),
        username = username,
        home = home_dir.display()
    )
}

/// Stop, disable, and remove the Freenet service file. Does not purge data.
/// Returns true if a service was found and removed.
#[cfg(target_os = "linux")]
pub fn stop_and_remove_service(system: bool) -> Result<bool> {
    use std::fs;

    let system_mode = use_system_mode(system);

    let service_path = if system_mode {
        std::path::PathBuf::from(SYSTEM_SERVICE_PATH)
    } else {
        dirs::home_dir()
            .context("Failed to get home directory")?
            .join(".config/systemd/user/freenet.service")
    };

    if !service_path.exists() {
        return Ok(false);
    }

    // Stop the service if running (best-effort, may already be stopped)
    let _stop = systemctl(system_mode, &["stop", "freenet"]);

    // Disable the service (best-effort, may already be disabled)
    let _disable = systemctl(system_mode, &["disable", "freenet"]);

    fs::remove_file(&service_path).context("Failed to remove service file")?;

    // Reload systemd (best-effort, failure is non-fatal during uninstall)
    drop(systemctl(system_mode, &["daemon-reload"]));

    Ok(true)
}

#[cfg(target_os = "linux")]
fn uninstall_service(system: bool, purge: bool, keep_data: bool) -> Result<()> {
    stop_and_remove_service(system)?;

    println!("Freenet service uninstalled.");

    if should_purge(purge, keep_data)? {
        let system_mode = use_system_mode(system);
        purge_data_dirs(system_mode)?;
        println!("All Freenet data, config, and logs removed.");
    }

    Ok(())
}

#[cfg(target_os = "linux")]
fn service_status(system: bool) -> Result<()> {
    let system_mode = use_system_mode(system);
    let status = systemctl(system_mode, &["status", "freenet"])?;
    std::process::exit(status.code().unwrap_or(1));
}

#[cfg(target_os = "linux")]
fn start_service(system: bool) -> Result<()> {
    let system_mode = use_system_mode(system);
    systemctl_with_hint(system_mode, &["start", "freenet"], "start service")?;
    println!("Freenet service started.");
    println!("Open http://127.0.0.1:7509/ in your browser to view your Freenet dashboard.");
    Ok(())
}

#[cfg(target_os = "linux")]
fn stop_service(system: bool) -> Result<()> {
    let system_mode = use_system_mode(system);
    systemctl_with_hint(system_mode, &["stop", "freenet"], "stop service")?;
    println!("Freenet service stopped.");
    Ok(())
}

#[cfg(target_os = "linux")]
fn restart_service(system: bool) -> Result<()> {
    let system_mode = use_system_mode(system);
    systemctl_with_hint(system_mode, &["restart", "freenet"], "restart service")?;
    println!("Freenet service restarted.");
    println!("Open http://127.0.0.1:7509/ in your browser to view your Freenet dashboard.");
    Ok(())
}

#[cfg(target_os = "linux")]
fn service_logs(error_only: bool) -> Result<()> {
    let log_dir = dirs::home_dir()
        .context("Failed to get home directory")?
        .join(".local/state/freenet");

    let base_name = if error_only {
        "freenet.error"
    } else {
        "freenet"
    };

    tail_with_rotation(&log_dir, base_name)
}

// macOS implementation using launchd
// --system flag is not supported on macOS (launchd daemons need different setup)
#[cfg(target_os = "macos")]
fn install_service(system: bool) -> Result<()> {
    if system {
        anyhow::bail!(
            "The --system flag is only supported on Linux.\n\
             On macOS, use the default user agent: freenet service install"
        );
    }
    install_macos_service()
}

/// Derive the path of the macOS auto-update wrapper script from a home
/// directory. Single source of truth shared by the install path (which
/// writes the script), the update path, and the uninstall path (which
/// removes it). Keep this in sync with `update.rs`'s wrapper derivation.
#[cfg(target_os = "macos")]
fn wrapper_script_path(home_dir: &Path) -> PathBuf {
    home_dir.join(".local/bin/freenet-service-wrapper.sh")
}

/// Remove the wrapper script and its sidecar/backup files written by the
/// install (`*.sh`, `*.sh.hash`) and update (`*.sh.bak`) paths.
///
/// Idempotent: a missing file is not an error, so uninstalling twice (or
/// uninstalling an install that never ran an update) succeeds cleanly.
/// Returns an error only if a present file cannot be removed.
///
/// Regression target for #4290: install wrote three files, uninstall
/// removed zero, leaving stale wrapper artifacts in `~/.local/bin`.
#[cfg(target_os = "macos")]
fn remove_wrapper_files(wrapper_path: &Path) -> Result<()> {
    use std::fs;

    // `.sh` itself, plus the `.sh.hash` sidecar (#4286) and any `.sh.bak`
    // backup left by `freenet update`. Derived via `with_extension` so they
    // track the wrapper path rather than being independently hardcoded.
    let targets = [
        wrapper_path.to_path_buf(),
        wrapper_path.with_extension("sh.hash"),
        wrapper_path.with_extension("sh.bak"),
    ];

    for target in &targets {
        match fs::remove_file(target) {
            Ok(()) => {}
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
            Err(e) => {
                return Err(e).with_context(|| {
                    format!("Failed to remove wrapper file {}", target.display())
                });
            }
        }
    }

    Ok(())
}

#[cfg(target_os = "macos")]
fn install_macos_service() -> Result<()> {
    use std::fs;
    use std::os::unix::fs::PermissionsExt;

    let exe_path = std::env::current_exe().context("Failed to get current executable path")?;
    let home_dir = dirs::home_dir().context("Failed to get home directory")?;

    let launch_agents_dir = home_dir.join("Library/LaunchAgents");
    fs::create_dir_all(&launch_agents_dir).context("Failed to create LaunchAgents directory")?;

    // Create log directory in proper macOS location
    let log_dir = home_dir.join("Library/Logs/freenet");
    fs::create_dir_all(&log_dir).context("Failed to create log directory")?;

    // Create wrapper script for auto-update support.
    // launchd doesn't have ExecStopPost like systemd, so we use a wrapper
    // that checks exit code 42 (update needed) and runs update before restart.
    let wrapper_path = wrapper_script_path(&home_dir);
    let wrapper_dir = wrapper_path
        .parent()
        .context("wrapper path has no parent directory")?;
    fs::create_dir_all(wrapper_dir).context("Failed to create wrapper directory")?;
    let wrapper_content = generate_wrapper_script(&exe_path);
    fs::write(&wrapper_path, &wrapper_content).context("Failed to write wrapper script")?;
    fs::set_permissions(&wrapper_path, fs::Permissions::from_mode(0o755))
        .context("Failed to make wrapper script executable")?;

    // Sidecar records the wrapper's SHA-256 so a later `freenet update`
    // can distinguish "Freenet's wrapper" from a hand-edited one before
    // overwriting (#3967). A failed sidecar write only weakens future
    // user-modification protection — warn and continue.
    let wrapper_hash_path = wrapper_path.with_extension("sh.hash");
    let wrapper_hash = super::update::wrapper_content_hash(&wrapper_content);
    if let Err(e) = super::update::write_wrapper_hash_sidecar(&wrapper_hash_path, &wrapper_hash) {
        eprintln!(
            "Warning: failed to write wrapper hash sidecar at {}: {}.",
            wrapper_hash_path.display(),
            e
        );
    }

    let plist_content = generate_plist(&wrapper_path, &log_dir);
    let plist_path = launch_agents_dir.join("org.freenet.node.plist");

    fs::write(&plist_path, plist_content).context("Failed to write plist file")?;

    println!("Freenet service installed successfully.");
    println!();
    println!("To start the service now:");
    println!("  freenet service start");
    println!();
    println!("The service will start automatically on login.");
    println!("Logs will be written to: {}", log_dir.display());

    Ok(())
}

#[cfg(target_os = "macos")]
pub fn generate_wrapper_script(binary_path: &Path) -> String {
    format!(
        r#"#!/bin/bash
# Freenet service wrapper for auto-update support.
# This wrapper monitors exit code 42 (update needed) and runs update before restart.
# Includes exponential backoff to prevent rapid restart loops on repeated failures.
# On startup, kills any stale 'freenet network' processes to avoid port conflicts.

BACKOFF=10       # Initial backoff in seconds
MAX_BACKOFF=300  # Maximum backoff (5 minutes)
CONSECUTIVE_FAILURES=0
PORT_CONFLICT_KILLS=0
MAX_PORT_CONFLICT_KILLS=3  # Give up after this many kill attempts

# Lifecycle messages route through `logger`, which writes to macOS unified
# logging (auto-rotated, queryable via `log show --predicate 'process ==
# "logger"' --info`). Previously these were appended to a fixed
# ~/Library/Logs/freenet/freenet.log that the cleanup pass never touched
# (its mtime stayed fresh while being written), so the file grew without
# bound on long-running nodes (issue #4251). The transient
# freenet.error.log.last scratch file below is overwritten on every launch
# and so does not accumulate.
log_event() {{
    logger -t freenet "$1"
}}

# Print a binary's full version identity line, e.g.
#   Freenet version: 0.2.71 (abc1234)
# Bounded by a 5s timeout so a wedged binary can't stall the whole self-heal
# before ExecStart. `timeout` may be absent on a bare macOS box, so fall back
# to invoking the binary directly when it isn't on PATH.
version_line() {{
    if command -v timeout >/dev/null 2>&1; then
        timeout 5 "$1" --version 2>/dev/null | grep '^Freenet version:'
    else
        "$1" --version 2>/dev/null | grep '^Freenet version:'
    fi
}}

# Print the on-disk binary's full version identity line. Used on the exit-43
# self-heal path to tell a stale orphan apart from a legitimate second
# instance running the SAME binary we would launch.
ondisk_version() {{
    version_line "{binary}"
}}

# Enumerate PIDs of `freenet network` processes owned by the current user.
holder_pids() {{
    pgrep -f -u "$(id -u)" "freenet network" 2>/dev/null
}}

# Resolve a PID's executable path on macOS. `ps -o command=` yields the full
# argv (first field = the running image path); we take that first field. This
# avoids the `ps -o comm=` truncation that silently degrades a long install
# path to PPID-only detection. Empty if the process is gone.
holder_exe() {{
    ps -o command= -p "$1" 2>/dev/null | awk '{{print $1; exit}}'
}}

# Stale-orphan self-heal for exit 43.
#
# WHY this exists: the binary returns exit 43 ("another instance is already
# running") whenever it cleanly detects something already holding the
# service port. launchd's plist sets KeepAlive.SuccessfulExit=false, so if
# this wrapper responds with `exit 0`, launchd treats it as an intentional
# stop and NEVER respawns us. (systemd has the same trap via
# RestartPreventExitStatus=43.) That is correct for a real second instance,
# but catastrophic when the port holder is an ORPHANED `freenet network`
# (PPID=1, detached from any wrapper) still running a STALE OLD binary: every
# new spawn detects it, exits 43, we stand down, and the orphan serves stale
# assets forever (issue #3967).
#
# So before deferring, decide per holder. We ONLY kill a process that is an
# ORPHAN (PPID==1, adopted by launchd/init and not this wrapper's own child) —
# a deliberately hand-run `freenet network` is parented by a user shell, so its
# PPID != 1 and we always defer to it (never SIGKILL a developer's instance or
# truncate a supervised upgrade's drain). Among orphans we kill when EITHER:
#   * its version line differs from the binary we would launch (stale binary),
#     but only when we can actually read our own on-disk version (a non-empty
#     ondisk); if ondisk is unreadable mid-update we must NOT treat "differs"
#     as a kill signal, or we'd cull a healthy current node, OR
#   * we cannot read the holder's own version at all (binary gone/unreadable),
#     in which case an init-adopted orphan is presumed stale.
# Kill is SIGTERM, then SIGKILL (the original incident's orphan ignored SIGTERM
# for >11s). Returns 0 = killed a stale orphan, relaunch; 1 = defer.
heal_stale_orphan_or_defer() {{
    local ondisk pid exe holder_ver ppid killed=1
    ondisk="$(ondisk_version)"
    for pid in $(holder_pids); do
        # Never touch our own child (the instance we just ran in this loop).
        [ "$pid" = "$WRAPPER_CHILD_PID" ] && continue
        ppid="$(ps -o ppid= -p "$pid" 2>/dev/null | tr -d ' ')"
        exe="$(holder_exe "$pid")"
        holder_ver=""
        if [ -n "$exe" ] && [ -x "$exe" ]; then
            holder_ver="$(version_line "$exe")"
        fi
        # Orphan-only: a non-init-adopted holder is a real second instance (or a
        # user-run node) and is always deferred to, regardless of version.
        version_mismatch=1
        if [ -n "$ondisk" ] && [ -n "$holder_ver" ] && [ "$holder_ver" != "$ondisk" ]; then
            version_mismatch=0
        fi
        if [ "$ppid" = "1" ] && {{ [ "$version_mismatch" = "0" ] || [ -z "$holder_ver" ]; }}; then
            log_event "Exit 43: port holder PID $pid is a STALE orphan (holder='$holder_ver' ondisk='$ondisk' ppid=$ppid). Killing and relaunching."
            kill -TERM "$pid" 2>/dev/null || true
            # Wait up to ~12s for graceful exit, then escalate to SIGKILL.
            local waited=0
            while kill -0 "$pid" 2>/dev/null && [ $waited -lt 12 ]; do
                sleep 1
                waited=$((waited + 1))
            done
            if kill -0 "$pid" 2>/dev/null; then
                log_event "Exit 43: stale orphan PID $pid ignored SIGTERM after ${{waited}}s, sending SIGKILL"
                kill -KILL "$pid" 2>/dev/null || true
                sleep 1
            fi
            killed=0
        else
            log_event "Exit 43: deferring to port holder PID $pid (holder='$holder_ver' ondisk='$ondisk' ppid=$ppid) — current version, user-supervised, or version undeterminable"
        fi
    done
    return $killed
}}

# Kill any stale freenet network processes before starting.
# This handles the case where a previous launch daemon restart left a child
# process still holding the port (e.g. port 7509).
# Scoped to the current user to avoid killing processes owned by other users.
if pkill -f -u "$(id -u)" "freenet network" 2>/dev/null; then
    log_event "Killed stale freenet network process(es) on startup"
    sleep 2
fi

# Forward a SIGTERM (sent by launchd on stop / restart) to the node so it can
# run its graceful drain, rather than relying solely on launchd group-signaling
# the whole process group. The node handles SIGTERM by draining in-flight
# client drivers before closing peer connections. Harmless if launchd already
# group-signals — the child just receives a (deduplicated) TERM either way.
forward_term() {{
    [ -n "$WRAPPER_CHILD_PID" ] && kill -TERM "$WRAPPER_CHILD_PID" 2>/dev/null || true
}}
trap forward_term TERM

while true; do
    # Launch in the background so we know our own child's PID. This lets the
    # exit-43 self-heal path avoid mistaking our just-exited child for a
    # stale orphan still holding the port, and lets the TERM trap above forward
    # launchd's stop signal to the node for a graceful drain.
    "{binary}" network 2>"$HOME/Library/Logs/freenet/freenet.error.log.last" &
    WRAPPER_CHILD_PID=$!
    # `wait` is interrupted by the trapped TERM; re-wait so we collect the
    # child's real exit status after it finishes draining.
    wait $WRAPPER_CHILD_PID
    EXIT_CODE=$?
    while kill -0 "$WRAPPER_CHILD_PID" 2>/dev/null; do
        wait $WRAPPER_CHILD_PID
        EXIT_CODE=$?
    done

    if [ $EXIT_CODE -eq 42 ]; then
        log_event "Update needed, running freenet update..."
        if "{binary}" update --quiet; then
            log_event "Update successful, restarting..."
            CONSECUTIVE_FAILURES=0
            PORT_CONFLICT_KILLS=0
            BACKOFF=10
            sleep 2
        else
            CONSECUTIVE_FAILURES=$((CONSECUTIVE_FAILURES + 1))
            log_event "Update failed (attempt $CONSECUTIVE_FAILURES), backing off $BACKOFF seconds..."
            sleep $BACKOFF
            BACKOFF=$((BACKOFF * 2))
            [ $BACKOFF -gt $MAX_BACKOFF ] && BACKOFF=$MAX_BACKOFF
        fi
        continue
    elif [ $EXIT_CODE -eq 43 ]; then
        # Another instance holds the port. Before standing down (which under
        # launchd SuccessfulExit=false would stop us forever), check whether
        # the holder is a stale orphan running an old binary and, if so, kill
        # it and relaunch instead of deferring. See heal_stale_orphan_or_defer.
        if heal_stale_orphan_or_defer; then
            log_event "Killed stale orphan holding the port on exit 43, relaunching"
            CONSECUTIVE_FAILURES=0
            PORT_CONFLICT_KILLS=0
            BACKOFF=10
            sleep 2
            continue
        fi
        log_event "Another instance (current version) is already running, exiting cleanly"
        exit 0
    elif [ $EXIT_CODE -eq 0 ]; then
        log_event "Normal shutdown"
        exit 0
    else
        # Check if this looks like a port-already-in-use failure.
        if grep -q "already in use" "$HOME/Library/Logs/freenet/freenet.error.log.last" 2>/dev/null; then
            PORT_CONFLICT_KILLS=$((PORT_CONFLICT_KILLS + 1))
            if [ $PORT_CONFLICT_KILLS -le $MAX_PORT_CONFLICT_KILLS ]; then
                log_event "Port conflict detected (attempt $PORT_CONFLICT_KILLS/$MAX_PORT_CONFLICT_KILLS) — killing stale freenet process and retrying..."
                pkill -f -u "$(id -u)" "freenet network" 2>/dev/null || true
                sleep 2
                BACKOFF=10
                continue
            else
                log_event "Port conflict persists after $MAX_PORT_CONFLICT_KILLS kill attempts. Manual intervention may be required ('pkill freenet'). Backing off..."
            fi
        fi
        CONSECUTIVE_FAILURES=$((CONSECUTIVE_FAILURES + 1))
        PORT_CONFLICT_KILLS=0
        log_event "Exited with code $EXIT_CODE, restarting after backoff..."
        sleep $BACKOFF
        BACKOFF=$((BACKOFF * 2))
        [ $BACKOFF -gt $MAX_BACKOFF ] && BACKOFF=$MAX_BACKOFF
    fi
done
"#,
        binary = binary_path.display()
    )
}

#[cfg(target_os = "macos")]
pub(super) fn generate_plist(wrapper_path: &Path, log_dir: &Path) -> String {
    // Note: wrapper_path is the auto-update wrapper script, not the freenet binary directly.
    // The wrapper handles the loop: run freenet, check exit code, update if needed.
    format!(
        r#"<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
    <key>Label</key>
    <string>org.freenet.node</string>
    <key>ProgramArguments</key>
    <array>
        <string>{wrapper}</string>
    </array>
    <key>RunAtLoad</key>
    <true/>
    <key>KeepAlive</key>
    <dict>
        <key>SuccessfulExit</key>
        <false/>
    </dict>
    <!--
        Logging
        - The node's tracing layer writes its own size-capped, hourly-
          rotated logs to {log_dir}/freenet.YYYY-MM-DD-HH.log
          (LOG_RETENTION_HOURS + LOG_DIR_MAX_BYTES; see
          crates/core/src/tracing.rs).
        - launchd previously appended to fixed freenet.log / freenet.error.log
          that the time-based cleanup never pruned, so they grew without
          bound (issue #4251). macOS does not offer a journal target for
          launchd, so the cleanest option is /dev/null — diagnostics
          remain available via `freenet service report`, which collects
          the rotated tracing logs.
    -->
    <key>StandardOutPath</key>
    <string>/dev/null</string>
    <key>StandardErrorPath</key>
    <string>/dev/null</string>
    <key>SoftResourceLimits</key>
    <dict>
        <key>NumberOfFiles</key>
        <integer>65536</integer>
    </dict>
    <key>HardResourceLimits</key>
    <dict>
        <key>NumberOfFiles</key>
        <integer>65536</integer>
    </dict>
</dict>
</plist>
"#,
        wrapper = wrapper_path.display(),
        log_dir = log_dir.display()
    )
}

/// Bail with "--system not supported on macOS" for commands that don't apply.
#[cfg(target_os = "macos")]
fn check_no_system_flag(system: bool) -> Result<()> {
    if system {
        anyhow::bail!(
            "The --system flag is only supported on Linux.\n\
             On macOS, use the default user agent commands without --system."
        );
    }
    Ok(())
}

/// Stop and remove the Freenet launchd agent. Does not purge data.
/// Returns true if a service was found and removed.
#[cfg(target_os = "macos")]
pub fn stop_and_remove_service(_system: bool) -> Result<bool> {
    let home_dir = dirs::home_dir().context("Failed to get home directory")?;
    stop_and_remove_service_at(&home_dir)
}

/// Core of macOS uninstall, parametrized on the home directory so the
/// cleanup-ordering invariant is unit-testable without touching the real
/// `$HOME` or shelling out to `launchctl` (which only runs when the plist is
/// present).
#[cfg(target_os = "macos")]
fn stop_and_remove_service_at(home_dir: &Path) -> Result<bool> {
    use std::fs;

    let plist_path = home_dir.join("Library/LaunchAgents/org.freenet.node.plist");

    // Remove the auto-update wrapper script and its sidecar/backup files
    // (#4290). install writes `*.sh` + `*.sh.hash`; `freenet update` may
    // leave a `*.sh.bak`. Missing files are not an error (idempotent), so we
    // run this BEFORE the plist early-return: a partial or repeated uninstall
    // can leave the plist already gone while the wrapper artifacts remain, and
    // those must still be cleaned up.
    remove_wrapper_files(&wrapper_script_path(home_dir))?;

    if !plist_path.exists() {
        return Ok(false);
    }

    if let Some(plist_path_str) = plist_path.to_str() {
        // Unload the service if loaded (ignore errors as it may not be loaded)
        let unload_status = std::process::Command::new("launchctl")
            .args(["unload", plist_path_str])
            .status();
        if let Err(e) = unload_status {
            eprintln!("Warning: Failed to unload service: {}", e);
        }
    }

    // Remove the plist file
    fs::remove_file(&plist_path).context("Failed to remove plist file")?;

    Ok(true)
}

#[cfg(target_os = "macos")]
fn uninstall_service(system: bool, purge: bool, keep_data: bool) -> Result<()> {
    check_no_system_flag(system)?;

    stop_and_remove_service(system)?;

    println!("Freenet service uninstalled.");

    if should_purge(purge, keep_data)? {
        purge_data_dirs(false)?;
        println!("All Freenet data, config, and logs removed.");
    }

    Ok(())
}

#[cfg(target_os = "macos")]
fn service_status(system: bool) -> Result<()> {
    check_no_system_flag(system)?;

    let output = std::process::Command::new("launchctl")
        .args(["list", "org.freenet.node"])
        .output()
        .context("Failed to check service status")?;

    if output.status.success() {
        println!("Freenet service is running.");
        if !output.stdout.is_empty() {
            println!("{}", String::from_utf8_lossy(&output.stdout));
        }
    } else {
        println!("Freenet service is not running.");
        std::process::exit(3); // Standard exit code for "not running"
    }

    Ok(())
}

#[cfg(target_os = "macos")]
fn start_service(system: bool) -> Result<()> {
    check_no_system_flag(system)?;

    let plist_path = dirs::home_dir()
        .context("Failed to get home directory")?
        .join("Library/LaunchAgents/org.freenet.node.plist");

    if !plist_path.exists() {
        anyhow::bail!("Service not installed. Run 'freenet service install' first.");
    }

    let plist_path_str = plist_path
        .to_str()
        .context("Plist path contains invalid UTF-8")?;

    let status = std::process::Command::new("launchctl")
        .args(["load", plist_path_str])
        .status()
        .context("Failed to start service")?;

    if status.success() {
        println!("Freenet service started.");
        println!("Open http://127.0.0.1:7509/ in your browser to view your Freenet dashboard.");
    } else {
        anyhow::bail!("Failed to start service");
    }

    Ok(())
}

#[cfg(target_os = "macos")]
fn stop_service(system: bool) -> Result<()> {
    check_no_system_flag(system)?;

    let plist_path = dirs::home_dir()
        .context("Failed to get home directory")?
        .join("Library/LaunchAgents/org.freenet.node.plist");

    let plist_path_str = plist_path
        .to_str()
        .context("Plist path contains invalid UTF-8")?;

    let status = std::process::Command::new("launchctl")
        .args(["unload", plist_path_str])
        .status()
        .context("Failed to stop service")?;

    if status.success() {
        println!("Freenet service stopped.");
    } else {
        anyhow::bail!("Failed to stop service");
    }

    Ok(())
}

#[cfg(target_os = "macos")]
fn restart_service(system: bool) -> Result<()> {
    check_no_system_flag(system)?;
    stop_service(false)?;
    start_service(false)
}

#[cfg(target_os = "macos")]
fn service_logs(error_only: bool) -> Result<()> {
    let log_dir = dirs::home_dir()
        .context("Failed to get home directory")?
        .join("Library/Logs/freenet");

    let base_name = if error_only {
        "freenet.error"
    } else {
        "freenet"
    };

    tail_with_rotation(&log_dir, base_name)
}

// Windows implementation
// Note: Windows service management requires either:
// 1. Running as a Windows Service (requires service registration)
// 2. Using Task Scheduler for user-level autostart
// For now, we provide Task Scheduler-based autostart

#[cfg(target_os = "windows")]
fn install_service(system: bool) -> Result<()> {
    if system {
        anyhow::bail!(
            "The --system flag is only supported on Linux.\n\
             On Windows, use the default scheduled task: freenet service install"
        );
    }

    let exe_path = std::env::current_exe().context("Failed to get current executable path")?;
    let exe_path_str = exe_path
        .to_str()
        .context("Executable path contains invalid UTF-8")?;

    // Register Freenet to start at logon via the registry Run key.
    // This requires no admin privileges — HKCU is user-writable.
    // The wrapper manages the freenet network child process, handles
    // auto-update (exit code 42), crash backoff, log capture, and
    // shows a system tray icon.
    let run_command = format!("\"{}\" service run-wrapper", exe_path_str);
    let hkcu = winreg::RegKey::predef(winreg::enums::HKEY_CURRENT_USER);
    let (run_key, _) = hkcu
        .create_subkey(r"Software\Microsoft\Windows\CurrentVersion\Run")
        .context("Failed to open registry Run key")?;
    run_key
        .set_value("Freenet", &run_command)
        .context("Failed to write Freenet registry entry")?;

    // Also clean up any legacy scheduled task from older installs
    drop(
        std::process::Command::new("schtasks")
            .args(["/delete", "/tn", "Freenet", "/f"])
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null())
            .status(),
    );

    println!("Freenet autostart registered successfully.");
    println!();
    println!("To start Freenet now:");
    println!("  freenet service start");
    println!();
    println!("Freenet will start automatically when you log in.");
    println!("A system tray icon will appear with status and controls.");

    Ok(())
}

#[cfg(target_os = "windows")]
fn check_no_system_flag_windows(system: bool) -> Result<()> {
    if system {
        anyhow::bail!(
            "The --system flag is only supported on Linux.\n\
             On Windows, use the default service commands without --system."
        );
    }
    Ok(())
}

/// Stop and remove Freenet autostart. Kills running process, removes registry
/// Run key, and cleans up any legacy scheduled task. Does not purge data.
/// Returns true if Freenet was registered.
#[cfg(target_os = "windows")]
pub fn stop_and_remove_service(_system: bool) -> Result<bool> {
    let hkcu = winreg::RegKey::predef(winreg::enums::HKEY_CURRENT_USER);
    let run_key = hkcu
        .open_subkey_with_flags(
            r"Software\Microsoft\Windows\CurrentVersion\Run",
            winreg::enums::KEY_READ | winreg::enums::KEY_WRITE,
        )
        .context("Failed to open registry Run key")?;

    let had_registry = run_key.delete_value("Freenet").is_ok();

    // Kill the running Freenet service processes (wrapper + node child).
    // Targets only the service's own processes by command line so an
    // unrelated freenet.exe — e.g. the GUI installer — is never killed
    // (issue #4205).
    kill_freenet_service_processes();

    // Also clean up any legacy scheduled task from older installs
    let had_task = std::process::Command::new("schtasks")
        .args(["/query", "/tn", "Freenet"])
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .status()
        .map(|s| s.success())
        .unwrap_or(false);
    if had_task {
        drop(
            std::process::Command::new("schtasks")
                .args(["/delete", "/tn", "Freenet", "/f"])
                .stdout(std::process::Stdio::null())
                .stderr(std::process::Stdio::null())
                .status(),
        );
    }

    Ok(had_registry || had_task)
}

#[cfg(target_os = "windows")]
fn uninstall_service(system: bool, purge: bool, keep_data: bool) -> Result<()> {
    check_no_system_flag_windows(system)?;

    stop_and_remove_service(system)?;

    println!("Freenet autostart uninstalled.");

    if should_purge(purge, keep_data)? {
        purge_data_dirs(false)?;
        println!("All Freenet data, config, and logs removed.");
    }

    Ok(())
}

#[cfg(target_os = "windows")]
fn service_status(system: bool) -> Result<()> {
    check_no_system_flag_windows(system)?;

    let hkcu = winreg::RegKey::predef(winreg::enums::HKEY_CURRENT_USER);
    let registered = hkcu
        .open_subkey(r"Software\Microsoft\Windows\CurrentVersion\Run")
        .ok()
        .and_then(|k| k.get_value::<String, _>("Freenet").ok())
        .is_some();

    if registered {
        println!("Freenet autostart is registered.");
        // Check if actually running
        let running = std::process::Command::new("tasklist")
            .args(["/fi", "imagename eq freenet.exe", "/fo", "csv", "/nh"])
            .output()
            .map(|o| {
                let stdout = String::from_utf8_lossy(&o.stdout);
                stdout.contains("freenet.exe")
            })
            .unwrap_or(false);
        if running {
            println!("Freenet is currently running.");
        } else {
            println!("Freenet is not currently running.");
        }
    } else {
        println!("Freenet autostart is not registered.");
        std::process::exit(3);
    }

    Ok(())
}

#[cfg(target_os = "windows")]
fn start_service(system: bool) -> Result<()> {
    check_no_system_flag_windows(system)?;

    let exe_path = std::env::current_exe().context("Failed to get current executable path")?;

    // Spawn the wrapper as a detached process that survives parent exit.
    // CREATE_NEW_PROCESS_GROUP (0x200) + DETACHED_PROCESS (0x08) ensures
    // the child is not killed when the parent's console or job object closes.
    use std::os::windows::process::CommandExt;
    const DETACHED_PROCESS: u32 = 0x00000008;
    const CREATE_NEW_PROCESS_GROUP: u32 = 0x00000200;
    std::process::Command::new(&exe_path)
        .args(["service", "run-wrapper"])
        .stdin(std::process::Stdio::null())
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .creation_flags(DETACHED_PROCESS | CREATE_NEW_PROCESS_GROUP)
        .spawn()
        .context("Failed to start Freenet")?;

    println!("Freenet started.");
    println!("Open http://127.0.0.1:7509/ in your browser to view your Freenet dashboard.");

    Ok(())
}

/// Strip the leading executable path from a Windows process command line,
/// returning just the argument portion (everything after the program name).
///
/// The program path is normally quoted — both Rust's `Command` and the
/// registry `Run` key quote it — in which case it can be stripped exactly.
/// For an unquoted path we fall back to splitting on the first whitespace;
/// that is only ambiguous for unquoted paths containing spaces, which
/// Windows avoids for the processes Freenet itself spawns.
#[cfg_attr(not(target_os = "windows"), allow(dead_code))]
fn command_line_args(cmdline: &str) -> &str {
    let trimmed = cmdline.trim_start();
    if let Some(after_open_quote) = trimmed.strip_prefix('"') {
        after_open_quote
            .find('"')
            .map_or("", |close| after_open_quote[close + 1..].trim_start())
    } else {
        trimmed
            .find(char::is_whitespace)
            .map_or("", |space| trimmed[space..].trim_start())
    }
}

/// Which Freenet service process a `freenet.exe` command line represents.
#[cfg_attr(not(target_os = "windows"), allow(dead_code))]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum FreenetServiceProcess {
    /// The `service run-wrapper` supervisor (also the Windows tray process).
    Wrapper,
    /// The `network` node child the wrapper supervises.
    Node,
}

/// Classify a `freenet.exe` process from its full command line.
///
/// Matching is by the **subcommand prefix** — the first argument token after
/// the executable path — not by scanning every token. A blanket token scan
/// would misfire on a command line where `network` or `run-wrapper` appears
/// as an option value or path component rather than the subcommand, e.g.
/// `freenet local --config-dir network`.
///
/// Returns `None` for every other `freenet.exe` invocation, which must never
/// be killed: the GUI installer (launched with no subcommand — issue #4205),
/// `service stop`, `update`, or any directly-run `freenet` CLI command. Note
/// that a node started manually via the top-level `freenet network`
/// subcommand IS classified as `Node` — `service stop` is meant to stop a
/// running node regardless of how it was launched.
#[cfg_attr(not(target_os = "windows"), allow(dead_code))]
fn classify_freenet_process(cmdline: &str) -> Option<FreenetServiceProcess> {
    let mut args = command_line_args(cmdline).split_whitespace();
    match args.next()? {
        "network" => Some(FreenetServiceProcess::Node),
        "service" if args.next() == Some("run-wrapper") => Some(FreenetServiceProcess::Wrapper),
        _ => None,
    }
}

/// Parse the `<pid>\t<command line>` lines produced by the PowerShell process
/// listing in `list_freenet_processes` into `(pid, command_line)` pairs.
/// Lines that do not begin with a numeric PID are ignored.
#[cfg_attr(not(target_os = "windows"), allow(dead_code))]
fn parse_freenet_process_listing(stdout: &str) -> Vec<(u32, String)> {
    stdout
        .lines()
        .filter_map(|line| {
            let line = line.trim_end_matches('\r');
            let (pid, cmdline) = line.split_once('\t').unwrap_or((line, ""));
            Some((pid.trim().parse::<u32>().ok()?, cmdline.to_string()))
        })
        .collect()
}

/// Enumerate `freenet.exe` processes and their command lines via PowerShell.
///
/// Uses `Get-CimInstance` rather than `wmic`, which is deprecated and no
/// longer installed by default on recent Windows releases. Returns `None` if
/// the process listing could not be obtained.
///
/// A process whose `CommandLine` cannot be read (owned by another user) is
/// listed with an empty command line and so is never classified as a service
/// process. The wrapper, node, and installer all run as the same user, so
/// this never hides one of our own processes.
///
/// `.output()` captures stdout/stderr through fresh pipes and does not inherit
/// stdin, so this spawn is safe even after `FreeConsole()` (see the spawn-site
/// audit note on `run_wrapper`).
#[cfg(target_os = "windows")]
fn list_freenet_processes() -> Option<Vec<(u32, String)>> {
    let output = std::process::Command::new("powershell")
        .args([
            "-NoProfile",
            "-NonInteractive",
            "-Command",
            "Get-CimInstance Win32_Process -Filter \"Name='freenet.exe'\" | \
             ForEach-Object { \"$($_.ProcessId)`t$($_.CommandLine)\" }",
        ])
        .output()
        .ok()?;
    if !output.status.success() {
        return None;
    }
    Some(parse_freenet_process_listing(&String::from_utf8_lossy(
        &output.stdout,
    )))
}

/// Force-terminate a process by PID via `taskkill`. Returns whether `taskkill`
/// reported success.
///
/// All three standard handles are nulled so the spawn succeeds even when the
/// caller has detached from its console via `FreeConsole()` — the auto-update
/// restart path reaches here from the wrapper. See
/// `.claude/rules/bug-prevention-patterns.md`.
#[cfg(target_os = "windows")]
fn taskkill_pid(pid: u32) -> bool {
    std::process::Command::new("taskkill")
        .args(["/f", "/pid", &pid.to_string()])
        .stdin(std::process::Stdio::null())
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .status()
        .is_ok_and(|status| status.success())
}

/// Enumerate `freenet.exe` processes and `taskkill` every one classified as
/// `kind`, excluding the current process. Returns the number terminated.
#[cfg(target_os = "windows")]
fn kill_freenet_processes_matching(kind: FreenetServiceProcess) -> usize {
    let our_pid = std::process::id();
    let Some(processes) = list_freenet_processes() else {
        return 0;
    };
    let mut killed = 0;
    for (pid, cmdline) in processes {
        if pid != our_pid && classify_freenet_process(&cmdline) == Some(kind) && taskkill_pid(pid) {
            killed += 1;
        }
    }
    killed
}

/// Stop the running Freenet service — the `service run-wrapper` supervisor and
/// the `network` node child it manages.
///
/// Unlike a blanket `taskkill /im freenet.exe`, this targets only the
/// service's own processes (classified by command line), so it never kills an
/// unrelated `freenet.exe` that merely shares the image name: the GUI
/// installer (issue #4205), a concurrent `freenet` CLI command, or the caller
/// itself.
///
/// Wrappers are killed first, then nodes in a short re-enumerating loop. Only
/// a live wrapper spawns `network` children, and `taskkill /f` merely
/// *requests* termination (`TerminateProcess` is asynchronous), so a wrapper
/// can briefly outlive the wrapper pass — long enough to spawn a node the
/// first node enumeration missed. Each loop pass re-enumerates; once no
/// wrapper this sweep terminated is still alive, the node set is stable. The
/// loop stops as soon as a pass kills nothing — everything is gone, or the
/// remainder is unkillable and retrying will not help — and is bounded to 3
/// passes (200ms apart, comfortably over normal `TerminateProcess` latency)
/// so an unkillable process cannot hang the caller.
///
/// Known limitation: a wrapper mid-self-update can `spawn_new_wrapper` inside
/// its own async-termination window; that successor is not re-enumerated and
/// may survive. This matches the previous blanket `taskkill`'s exposure and
/// requires a self-update to coincide with a stop to the millisecond.
///
/// Returns the count of successful `taskkill` requests — which counts a node
/// re-killed across passes more than once, so callers only test `> 0`. A `0`
/// result means nothing was running, the process listing was unavailable, or
/// every `taskkill` failed; callers treat all three as "nothing was stopped".
#[cfg(target_os = "windows")]
pub(crate) fn kill_freenet_service_processes() -> usize {
    let mut killed = kill_freenet_processes_matching(FreenetServiceProcess::Wrapper);
    for _ in 0..3 {
        let nodes = kill_freenet_processes_matching(FreenetServiceProcess::Node);
        killed += nodes;
        if nodes == 0 {
            break;
        }
        // Give a still-dying wrapper time to finish so a node it respawns is
        // visible to the next enumeration.
        std::thread::sleep(std::time::Duration::from_millis(200));
    }
    killed
}

#[cfg(target_os = "windows")]
fn stop_service(system: bool) -> Result<()> {
    check_no_system_flag_windows(system)?;

    if kill_freenet_service_processes() > 0 {
        println!("Freenet stopped.");
        Ok(())
    } else {
        anyhow::bail!("Failed to stop Freenet. It may not be running.")
    }
}

#[cfg(target_os = "windows")]
fn restart_service(system: bool) -> Result<()> {
    check_no_system_flag_windows(system)?;
    drop(stop_service(false));
    // Give it a moment to stop
    std::thread::sleep(std::time::Duration::from_secs(2));
    start_service(false)
}

#[cfg(target_os = "windows")]
fn service_logs(error_only: bool) -> Result<()> {
    use freenet::tracing::tracer::get_log_dir;
    use std::time::Duration;

    let log_dir = get_log_dir().context(
        "Could not determine log directory. \
         Ensure Freenet has been run at least once via 'freenet service run-wrapper'.",
    )?;

    let base_name = if error_only {
        "freenet.error"
    } else {
        "freenet"
    };

    // Also check the wrapper log (now date-rotated: freenet-wrapper.YYYY-MM-DD.log)
    let wrapper_log = find_latest_log_file(&log_dir, "freenet-wrapper");

    let mut current_log = match find_latest_log_file(&log_dir, base_name) {
        Some(log_path) => {
            println!("Log file: {}", log_path.display());
            if let Some(ref wl) = wrapper_log {
                println!("Wrapper log: {}", wl.display());
            }
            log_path
        }
        None => {
            if let Some(ref wl) = wrapper_log {
                println!("No node logs found, showing wrapper log:");
                let status = std::process::Command::new("powershell")
                    .args([
                        "-Command",
                        &format!("Get-Content -Path '{}' -Tail 50 -Wait", wl.display()),
                    ])
                    .status()
                    .context("Failed to open wrapper log")?;
                std::process::exit(status.code().unwrap_or(1));
            } else {
                anyhow::bail!(
                    "No log files found in {}.\n\
                     Ensure Freenet has been run at least once.",
                    log_dir.display()
                );
            }
        }
    };

    println!("Press Ctrl+C to stop.\n");

    loop {
        let mut child = std::process::Command::new("powershell")
            .args([
                "-Command",
                &format!(
                    "Get-Content -Path '{}' -Tail 50 -Wait",
                    current_log.display()
                ),
            ])
            .spawn()
            .context("Failed to spawn PowerShell for log tailing")?;

        loop {
            match child.try_wait() {
                Ok(Some(status)) => {
                    if !status.success() {
                        // Fallback: open in notepad
                        drop(
                            std::process::Command::new("notepad")
                                .arg(&current_log)
                                .spawn(),
                        );
                    }
                    std::process::exit(status.code().unwrap_or(1));
                }
                Ok(None) => {}
                Err(e) => {
                    drop(child.kill());
                    drop(child.wait());
                    anyhow::bail!("Error waiting on PowerShell process: {e}");
                }
            }

            std::thread::sleep(Duration::from_secs(5));

            if let Some(newer_log) = find_latest_log_file(&log_dir, base_name) {
                if newer_log != current_log {
                    println!("\n--- Log rotated to: {} ---\n", newer_log.display());
                    drop(child.kill());
                    drop(child.wait());
                    current_log = newer_log;
                    break;
                }
            }
        }
    }
}

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
    use std::path::PathBuf;

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

        // Verify restart loop prevention (limits consecutive failures)
        assert!(service_content.contains("StartLimitBurst=5"));
        assert!(service_content.contains("StartLimitIntervalSec=120"));

        // Verify auto-update support via ExecStopPost
        assert!(service_content.contains("ExecStopPost="));

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
        assert!(service_content.contains("LimitNOFILE=65536"));
        assert!(service_content.contains("ExecStopPost="));

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
            for needle in ["$$pid", "$$exe", "$$ondisk", "$$self", "$$bin", "$$(id -u)"] {
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
            // -> the sh's `$$`) and PID 1, and anchor holders on the resolved
            // exe path so `pgrep -f "freenet network"` can't match its own sh.
            assert!(
                pre.contains("self=$$$$") && pre.contains("[ \"$$pid\" = \"$$self\" ] && continue"),
                "{which} ExecStartPre must skip its own sh PID (#3967 MUST-FIX 5)"
            );
            assert!(
                pre.contains("[ \"$$pid\" = \"1\" ] && continue"),
                "{which} ExecStartPre must skip PID 1 (#3967 MUST-FIX 5)"
            );
            assert!(
                pre.contains("[ \"$$exe\" != \"$$bin\" ] && continue"),
                "{which} ExecStartPre must only consider holders whose resolved \
                 exe is this unit's freenet binary (#3967 MUST-FIX 5)"
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
        // ps: -o ppid= -> $HOLDER_PPID ; -o command=/comm= -> holder exe path.
        write_shim(
            "ps",
            format!(
                "for a in \"$@\"; do case \"$a\" in \
                 ppid=) echo \"$HOLDER_PPID\"; exit 0;; \
                 command=|comm=) echo \"{}\"; exit 0;; esac; done\nexit 0\n",
                holder_bin.display()
            ),
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

        // Returns (stdout, holder_was_killed).
        let run =
            |holder_ver: &str, holder_ppid: &str, pgrep_pid: Option<&str>| -> (String, bool) {
                std::fs::remove_file(&killed_marker).ok();
                let mut cmd = std::process::Command::new("sh");
                cmd.arg(&harness_path)
                    .env("PATH", &path_env)
                    .env("HOLDER_VER", holder_ver)
                    .env("HOLDER_PPID", holder_ppid);
                if let Some(p) = pgrep_pid {
                    cmd.env("PGREP_PID", p);
                }
                let out = cmd.output().expect("failed to run heal harness");
                let stdout = String::from_utf8_lossy(&out.stdout).to_string();
                (stdout, killed_marker.exists())
            };

        // (a) orphan (PPID==1) on an OLD version vs the current on-disk 1.0.0.
        let (out_a, killed_a) = run("0.9.0 (old)", "1", None);
        assert!(
            out_a.contains("RC=0") && killed_a,
            "version-mismatched PPID==1 orphan must be KILLED + relaunch (RC=0), got: {out_a}"
        );

        // (b) orphan (PPID==1) running the SAME current version -> defer.
        let (out_b, killed_b) = run("1.0.0 (current)", "1", None);
        assert!(
            out_b.contains("RC=1") && !killed_b,
            "same-version holder must be DEFERRED, not killed (polite path), got: {out_b}"
        );

        // (c) MUST-FIX #3: version-mismatched but USER-parented (PPID!=1) -> defer.
        let (out_c, killed_c) = run("0.9.0 (old)", "4321", None);
        assert!(
            out_c.contains("RC=1") && !killed_c,
            "version-mismatched but user-parented (PPID!=1) holder must be DEFERRED (#3967 MUST-FIX 3), got: {out_c}"
        );

        // (d) MUST-FIX #4: same mismatch + orphan, but on-disk version UNREADABLE.
        // Make the baked-in binary non-executable so ondisk_version() is empty;
        // the mismatch signal must then be suppressed and a readable holder
        // deferred to (don't cull a healthy node mid-update).
        std::fs::set_permissions(&ondisk_bin, std::fs::Permissions::from_mode(0o644)).unwrap();
        let (out_d, killed_d) = run("0.9.0 (old)", "1", None);
        assert!(
            out_d.contains("RC=1") && !killed_d,
            "unreadable on-disk version must suppress the mismatch kill signal (#3967 MUST-FIX 4), got: {out_d}"
        );
        chmod_x(&ondisk_bin); // restore

        // (e) self-exclusion: the only holder pid IS our own child -> skip/defer.
        let (out_e, killed_e) = run("0.9.0 (old)", "1", Some("999999"));
        assert!(
            out_e.contains("RC=1") && !killed_e,
            "the routine must never target its own child PID (#3967), got: {out_e}"
        );
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
        let src = include_str!("service.rs");
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
        let src = include_str!("service.rs");
        let (_, after_fn_start) = src
            .split_once("fn taskkill_pid(")
            .expect("taskkill_pid definition not found");
        let (body, _) = after_fn_start
            .split_once("\nfn ")
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
            fs::write(path, b"x").expect("write fixture file");
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
}
