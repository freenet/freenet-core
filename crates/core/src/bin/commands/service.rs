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
use std::path::Path;
use std::sync::Arc;

use super::report::ReportCommand;

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
pub(super) const DASHBOARD_URL: &str = "http://127.0.0.1:7509/";

/// Open a URL in the default browser (platform-specific).
#[cfg(any(target_os = "windows", target_os = "macos"))]
fn open_url_in_browser(url: &str) {
    #[cfg(target_os = "windows")]
    drop(
        std::process::Command::new("cmd")
            .args(["/c", "start", "", url])
            .spawn(),
    );
    #[cfg(target_os = "macos")]
    drop(std::process::Command::new("open").arg(url).spawn());
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

    // Kill stale freenet network processes from a previous wrapper instance
    kill_stale_freenet_processes(&log_dir);

    #[cfg(any(target_os = "windows", target_os = "macos"))]
    {
        use super::tray::{TrayAction, WrapperStatus};

        let (action_tx, action_rx) = mpsc::channel::<TrayAction>();
        let (status_tx, status_rx) = mpsc::channel::<WrapperStatus>();
        let version_owned = version.to_string();
        let log_dir_clone = log_dir.clone();

        // Wrapper loop runs on a background thread
        let loop_handle = std::thread::spawn(move || {
            run_wrapper_loop(&log_dir_clone, Some((&action_rx, &status_tx)))
        });

        // Tray icon runs on the main thread (platform message pump)
        super::tray::run_tray_event_loop(action_tx, status_rx, &version_owned);

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
fn spawn_update_command(exe_path: &Path) -> std::io::Result<std::process::ExitStatus> {
    let mut cmd = std::process::Command::new(exe_path);
    cmd.args(["update", "--quiet"]);

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
    use std::net::ToSocketAddrs;

    // Note: `to_socket_addrs()` is a blocking OS resolver call that can take
    // 15-30s on Windows when the network is down. The deadline check between
    // calls means the total wait may exceed NETWORK_READINESS_TIMEOUT_SECS
    // in pathological cases. This is acceptable for a pre-startup wait.

    // Quick check — if DNS works immediately, skip the wait
    if NETWORK_PROBE_ADDR.to_socket_addrs().is_ok() {
        return true;
    }

    log_wrapper_event(
        log_dir,
        "Network not ready yet, waiting for connectivity...",
    );

    let max_checks = NETWORK_READINESS_TIMEOUT_SECS / NETWORK_READINESS_CHECK_INTERVAL_SECS;

    for _ in 0..max_checks {
        std::thread::sleep(std::time::Duration::from_secs(
            NETWORK_READINESS_CHECK_INTERVAL_SECS,
        ));

        // Allow the user to quit via the tray during the network wait
        #[cfg(any(target_os = "windows", target_os = "macos"))]
        if let Some(rx) = action_rx {
            if let Ok(super::tray::TrayAction::Quit) = rx.try_recv() {
                return false;
            }
        }

        if NETWORK_PROBE_ADDR.to_socket_addrs().is_ok() {
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
                                    // Spawn failed — fall through to relaunch child
                                    // with the current (old) wrapper rather than
                                    // leaving no node running.
                                    break SENTINEL_RESTART;
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
                                        // Spawn failed — exit stopped state and
                                        // relaunch child with the current wrapper.
                                        break;
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

            let ok = spawn_update_command(&exe_path)
                .map(|s| s.success())
                .unwrap_or(false);

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
                                match result {
                                    Ok(s) if s.success() => {
                                        log_wrapper_event(
                                            log_dir,
                                            "Update installed during backoff, restarting wrapper...",
                                        );
                                        status_tx.send(WrapperStatus::UpdatedRestarting).ok();
                                        if spawn_new_wrapper(&exe_path, log_dir) {
                                            return Ok(());
                                        }
                                        // Spawn failed — relaunch with current wrapper
                                        state.consecutive_failures = 0;
                                        break;
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
        // Use WMIC to find freenet.exe processes with "network" in the command line,
        // then kill them by PID. This avoids killing the wrapper itself or other
        // freenet subcommands (update, service, etc.).
        let output = std::process::Command::new("wmic")
            .args([
                "process",
                "where",
                "name='freenet.exe' and commandline like '%network%'",
                "get",
                "processid",
                "/format:list",
            ])
            .output();
        if let Ok(output) = output {
            let stdout = String::from_utf8_lossy(&output.stdout);
            let mut killed = false;
            for line in stdout.lines() {
                if let Some(pid_str) = line.strip_prefix("ProcessId=") {
                    let pid = pid_str.trim();
                    if !pid.is_empty() {
                        drop(
                            std::process::Command::new("taskkill")
                                .args(["/F", "/PID", pid])
                                .stdout(std::process::Stdio::null())
                                .stderr(std::process::Stdio::null())
                                .status(),
                        );
                        killed = true;
                    }
                }
            }
            if killed {
                log_wrapper_event(
                    log_dir,
                    "Killed stale freenet network process(es) on startup",
                );
                std::thread::sleep(std::time::Duration::from_secs(2));
            }
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

    // If we have a home override (system mode), construct paths manually using
    // XDG defaults. Otherwise use ProjectDirs which resolves from the current user.
    if let Some(ref home) = home_override {
        // XDG defaults for the service user
        remove_if_exists("data", &home.join(".local/share/Freenet"))?;
        remove_if_exists("config", &home.join(".config/Freenet"))?;
        remove_if_exists("cache", &home.join(".cache/Freenet"))?;
        // Also remove lowercase cache dir used by webapp cache
        remove_if_exists("cache", &home.join(".cache/freenet"))?;
        remove_if_exists("logs", &home.join(".local/state/freenet"))?;
    } else {
        match ProjectDirs::from("", "The Freenet Project Inc", "Freenet") {
            Some(ref dirs) => {
                let data_dir = dirs.data_dir();
                remove_if_exists("data", data_dir)?;

                let config_dir = dirs.config_dir();
                // On macOS, config_dir == data_dir; skip if already removed
                if config_dir != data_dir {
                    remove_if_exists("config", config_dir)?;
                }

                remove_if_exists("cache", dirs.cache_dir())?;
            }
            None => {
                eprintln!(
                    "Warning: Could not determine Freenet directories. Data and config may not have been removed."
                );
            }
        }

        // Also remove lowercase "freenet" cache dir used by webapp cache
        // (may differ from uppercase "Freenet" on case-sensitive filesystems)
        if let Some(ref dirs) = ProjectDirs::from("", "The Freenet Project Inc", "freenet") {
            let cache_dir = dirs.cache_dir();
            if cache_dir.exists() {
                remove_if_exists("cache", cache_dir)?;
            }
        }

        if let Some(log_dir) = get_log_dir() {
            remove_if_exists("logs", &log_dir)?;
        }
    }

    Ok(())
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

    fs::write(&service_path, service_content).context("Failed to write service file")?;

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
ExecStart={binary} network
Restart=always
# Wait 10 seconds before restart to avoid rapid restart loops
RestartSec=10
# Stop restart loop after 5 failures in 2 minutes (e.g., port conflict with
# a stale process). Without this, systemd restarts indefinitely.
# SuccessExitStatus=42 ensures auto-update exits don't count as failures.
StartLimitBurst=5
StartLimitIntervalSec=120
# Allow 15 seconds for graceful shutdown before SIGKILL
# The node handles SIGTERM to properly close peer connections
TimeoutStopSec=15

# Auto-update: if peer exits with code 42 (version mismatch with gateway),
# run update before systemd restarts the service. The '-' prefix means
# ExecStopPost failure won't affect service restart.
ExecStopPost=-/bin/sh -c '[ "$EXIT_STATUS" = "42" ] && {binary} update --quiet || true'
# Treat exit code 42 as success so it doesn't count against StartLimitBurst.
# Without this, rapid update cycles (exit 42 → ExecStopPost → restart) can
# exhaust the burst limit and permanently kill the service.
SuccessExitStatus=42 43
# Exit code 43 = another instance is already running on the port.
# Do NOT restart — the existing instance is healthy.
RestartPreventExitStatus=43

# Logging - write to files for systems without active user journald
# (headless servers, systems without lingering enabled, etc.)
StandardOutput=append:{log_dir}/freenet.log
StandardError=append:{log_dir}/freenet.error.log
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
ExecStart={binary} network
Restart=always
# Wait 10 seconds before restart to avoid rapid restart loops
RestartSec=10
# Stop restart loop after 5 failures in 2 minutes (e.g., port conflict with
# a stale process). Without this, systemd restarts indefinitely.
# SuccessExitStatus=42 ensures auto-update exits don't count as failures.
StartLimitBurst=5
StartLimitIntervalSec=120
# Allow 15 seconds for graceful shutdown before SIGKILL
# The node handles SIGTERM to properly close peer connections
TimeoutStopSec=15

# Auto-update: if peer exits with code 42 (version mismatch with gateway),
# run update before systemd restarts the service. The '-' prefix means
# ExecStopPost failure won't affect service restart.
ExecStopPost=-/bin/sh -c '[ "$EXIT_STATUS" = "42" ] && {binary} update --quiet || true'
# Treat exit code 42 as success so it doesn't count against StartLimitBurst.
# Without this, rapid update cycles (exit 42 → ExecStopPost → restart) can
# exhaust the burst limit and permanently kill the service.
SuccessExitStatus=42 43
# Exit code 43 = another instance is already running on the port.
# Do NOT restart — the existing instance is healthy.
RestartPreventExitStatus=43

# Logging - write to files for systems without active user journald
StandardOutput=append:{log_dir}/freenet.log
StandardError=append:{log_dir}/freenet.error.log
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

    let log_file = find_latest_log_file(&log_dir, base_name).ok_or_else(|| {
        anyhow::anyhow!(
            "No log files found in: {}\nMake sure the service has been installed and started.",
            log_dir.display()
        )
    })?;

    println!("Following logs from: {}", log_file.display());
    println!("Press Ctrl+C to stop.\n");

    let status = std::process::Command::new("tail")
        .arg("-f")
        .arg(&log_file)
        .status()
        .context("Failed to tail log file")?;

    std::process::exit(status.code().unwrap_or(1));
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
    let wrapper_dir = home_dir.join(".local/bin");
    fs::create_dir_all(&wrapper_dir).context("Failed to create wrapper directory")?;
    let wrapper_path = wrapper_dir.join("freenet-service-wrapper.sh");
    let wrapper_content = generate_wrapper_script(&exe_path);
    fs::write(&wrapper_path, wrapper_content).context("Failed to write wrapper script")?;
    fs::set_permissions(&wrapper_path, fs::Permissions::from_mode(0o755))
        .context("Failed to make wrapper script executable")?;

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

LOG="$HOME/Library/Logs/freenet/freenet.log"

# Kill any stale freenet network processes before starting.
# This handles the case where a previous launch daemon restart left a child
# process still holding the port (e.g. port 7509).
# Scoped to the current user to avoid killing processes owned by other users.
if pkill -f -u "$(id -u)" "freenet network" 2>/dev/null; then
    echo "$(date): Killed stale freenet network process(es) on startup" >> "$LOG"
    sleep 2
fi

while true; do
    "{binary}" network 2>"$HOME/Library/Logs/freenet/freenet.error.log.last"
    EXIT_CODE=$?

    if [ $EXIT_CODE -eq 42 ]; then
        echo "$(date): Update needed, running freenet update..." >> "$LOG"
        if "{binary}" update --quiet; then
            echo "$(date): Update successful, restarting..." >> "$LOG"
            CONSECUTIVE_FAILURES=0
            PORT_CONFLICT_KILLS=0
            BACKOFF=10
            sleep 2
        else
            CONSECUTIVE_FAILURES=$((CONSECUTIVE_FAILURES + 1))
            echo "$(date): Update failed (attempt $CONSECUTIVE_FAILURES), backing off $BACKOFF seconds..." >> "$LOG"
            sleep $BACKOFF
            BACKOFF=$((BACKOFF * 2))
            [ $BACKOFF -gt $MAX_BACKOFF ] && BACKOFF=$MAX_BACKOFF
        fi
        continue
    elif [ $EXIT_CODE -eq 43 ]; then
        echo "$(date): Another instance is already running, exiting cleanly" >> "$LOG"
        exit 0
    elif [ $EXIT_CODE -eq 0 ]; then
        echo "$(date): Normal shutdown" >> "$LOG"
        exit 0
    else
        # Check if this looks like a port-already-in-use failure.
        if grep -q "already in use" "$HOME/Library/Logs/freenet/freenet.error.log.last" 2>/dev/null; then
            PORT_CONFLICT_KILLS=$((PORT_CONFLICT_KILLS + 1))
            if [ $PORT_CONFLICT_KILLS -le $MAX_PORT_CONFLICT_KILLS ]; then
                echo "$(date): Port conflict detected (attempt $PORT_CONFLICT_KILLS/$MAX_PORT_CONFLICT_KILLS) — killing stale freenet process and retrying..." >> "$LOG"
                pkill -f -u "$(id -u)" "freenet network" 2>/dev/null || true
                sleep 2
                BACKOFF=10
                continue
            else
                echo "$(date): Port conflict persists after $MAX_PORT_CONFLICT_KILLS kill attempts. Manual intervention may be required ('pkill freenet'). Backing off..." >> "$LOG"
            fi
        fi
        CONSECUTIVE_FAILURES=$((CONSECUTIVE_FAILURES + 1))
        PORT_CONFLICT_KILLS=0
        echo "$(date): Exited with code $EXIT_CODE, restarting after backoff..." >> "$LOG"
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
fn generate_plist(wrapper_path: &Path, log_dir: &Path) -> String {
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
    <key>StandardOutPath</key>
    <string>{log_dir}/freenet.log</string>
    <key>StandardErrorPath</key>
    <string>{log_dir}/freenet.error.log</string>
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
    use std::fs;

    let plist_path = dirs::home_dir()
        .context("Failed to get home directory")?
        .join("Library/LaunchAgents/org.freenet.node.plist");

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

    let log_file = find_latest_log_file(&log_dir, base_name).ok_or_else(|| {
        anyhow::anyhow!(
            "No log files found in: {}\nMake sure the service has been installed and started.",
            log_dir.display()
        )
    })?;

    println!("Following logs from: {}", log_file.display());
    println!("Press Ctrl+C to stop.\n");

    let status = std::process::Command::new("tail")
        .arg("-f")
        .arg(&log_file)
        .status()
        .context("Failed to tail log file")?;

    std::process::exit(status.code().unwrap_or(1));
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

    // Kill any running freenet processes (wrapper + child), excluding ourselves
    let our_pid = std::process::id().to_string();
    drop(
        std::process::Command::new("taskkill")
            .args([
                "/f",
                "/im",
                "freenet.exe",
                "/fi",
                &format!("PID ne {}", our_pid),
            ])
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null())
            .status(),
    );

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

#[cfg(target_os = "windows")]
fn stop_service(system: bool) -> Result<()> {
    check_no_system_flag_windows(system)?;

    // Kill freenet processes, excluding the current one (which IS freenet.exe)
    let our_pid = std::process::id().to_string();
    let status = std::process::Command::new("taskkill")
        .args([
            "/f",
            "/im",
            "freenet.exe",
            "/fi",
            &format!("PID ne {}", our_pid),
        ])
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .status()
        .context("Failed to stop Freenet")?;

    if status.success() {
        println!("Freenet stopped.");
    } else {
        anyhow::bail!("Failed to stop Freenet. It may not be running.");
    }

    Ok(())
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

    match find_latest_log_file(&log_dir, base_name) {
        Some(log_path) => {
            println!("Log file: {}", log_path.display());
            if let Some(ref wl) = wrapper_log {
                println!("Wrapper log: {}", wl.display());
            }
            // Use PowerShell Get-Content -Wait to follow the log (like tail -f)
            let status = std::process::Command::new("powershell")
                .args([
                    "-Command",
                    &format!("Get-Content -Path '{}' -Tail 50 -Wait", log_path.display()),
                ])
                .status()
                .context("Failed to open log file")?;
            if !status.success() {
                // Fallback: just open in notepad
                drop(std::process::Command::new("notepad").arg(&log_path).spawn());
            }
        }
        None => {
            if let Some(ref wl) = wrapper_log {
                println!("No node logs found, showing wrapper log:");
                drop(
                    std::process::Command::new("powershell")
                        .args([
                            "-Command",
                            &format!("Get-Content -Path '{}' -Tail 50 -Wait", wl.display()),
                        ])
                        .status(),
                );
            } else {
                anyhow::bail!(
                    "No log files found in {}.\n\
                     Ensure Freenet has been run at least once.",
                    log_dir.display()
                );
            }
        }
    }

    Ok(())
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

        // Verify log paths are set correctly (file-based logging for headless systems)
        assert!(service_content.contains("/home/test/.local/state/freenet/freenet.log"));
        assert!(service_content.contains("/home/test/.local/state/freenet/freenet.error.log"));

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

        // Verify graceful shutdown timeout is set
        assert!(service_content.contains("TimeoutStopSec=15"));

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
    }

    #[test]
    #[cfg(target_os = "macos")]
    fn test_macos_wrapper_script_generation() {
        let binary_path = PathBuf::from("/usr/local/bin/freenet");
        let script = generate_wrapper_script(&binary_path);

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

        // Verify log paths are set correctly
        assert!(plist_content.contains("/Users/test/Library/Logs/freenet/freenet.log"));
        assert!(plist_content.contains("/Users/test/Library/Logs/freenet/freenet.error.log"));

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
}
