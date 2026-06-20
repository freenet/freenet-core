use anyhow::{Context, Result};
use std::path::Path;

#[cfg(target_os = "macos")]
use super::launch_at_login::macos_launch_at_login_startup;
#[cfg(target_os = "macos")]
use super::single_instance::{AcquireWrapperLockOutcome, acquire_wrapper_single_instance_lock};

#[cfg(any(target_os = "windows", target_os = "macos"))]
use super::DASHBOARD_URL;
#[cfg(any(target_os = "windows", target_os = "macos"))]
use super::open_url_in_browser;
use super::{
    SENTINEL_RESTART, SENTINEL_STOP, WRAPPER_EXIT_ALREADY_RUNNING, WRAPPER_EXIT_UPDATE_NEEDED,
    WRAPPER_INITIAL_BACKOFF_SECS, WRAPPER_MAX_BACKOFF_SECS, WRAPPER_MAX_CONSECUTIVE_FAILURES,
    WRAPPER_MAX_PORT_CONFLICT_KILLS,
};

/// State for the wrapper backoff state machine.
#[derive(Debug, Clone)]
pub(super) struct WrapperState {
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
pub(super) enum WrapperAction {
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
pub(super) fn next_wrapper_action(
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
pub(super) enum BackoffInterrupt {
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
pub(super) fn sleep_with_jitter_interruptible(
    secs: u64,
    #[cfg(any(target_os = "windows", target_os = "macos"))] action_rx: Option<
        &std::sync::mpsc::Receiver<super::super::tray::TrayAction>,
    >,
    #[cfg(not(any(target_os = "windows", target_os = "macos")))] _action_rx: Option<
        &std::sync::mpsc::Receiver<super::super::tray::TrayAction>,
    >,
) -> BackoffInterrupt {
    let jittered = jitter_secs(secs).max(1);
    for _ in 0..jittered {
        std::thread::sleep(std::time::Duration::from_secs(1));

        #[cfg(any(target_os = "windows", target_os = "macos"))]
        if let Some(rx) = action_rx {
            if let Ok(action) = rx.try_recv() {
                match action {
                    super::super::tray::TrayAction::Quit => return BackoffInterrupt::Quit,
                    super::super::tray::TrayAction::ViewLogs => super::super::tray::open_log_file(),
                    super::super::tray::TrayAction::OpenDashboard => {
                        open_url_in_browser(DASHBOARD_URL);
                    }
                    super::super::tray::TrayAction::Start
                    | super::super::tray::TrayAction::Restart => {
                        return BackoffInterrupt::Relaunch;
                    }
                    super::super::tray::TrayAction::CheckUpdate => {
                        return BackoffInterrupt::CheckUpdate;
                    }
                    super::super::tray::TrayAction::Stop => {} // already stopped/backing off
                }
            }
        }
    }
    BackoffInterrupt::Completed
}

/// Run the wrapper loop that manages a `freenet network` child process.
/// On Windows and macOS, shows a system tray / menu bar icon.
/// On Linux, runs the wrapper loop directly (no tray).
pub(super) fn run_wrapper(version: &str) -> Result<()> {
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
        use super::super::tray::{TrayAction, WrapperStatus};

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
        super::super::tray::run_tray_event_loop(action_tx, status_rx, cleanup_rx, &version_owned);

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
                &mpsc::Receiver<super::super::tray::TrayAction>,
                &mpsc::Sender<super::super::tray::WrapperStatus>,
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
pub(super) fn spawn_update_command(exe_path: &Path) -> std::io::Result<std::process::ExitStatus> {
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
        &std::sync::mpsc::Receiver<super::super::tray::TrayAction>,
    >,
    #[cfg(not(any(target_os = "windows", target_os = "macos")))] _action_rx: Option<
        &std::sync::mpsc::Receiver<super::super::tray::TrayAction>,
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
pub(super) fn wait_for_network_ready_inner(
    log_dir: &Path,
    probe_addr: &str,
    #[cfg(any(target_os = "windows", target_os = "macos"))] action_rx: Option<
        &std::sync::mpsc::Receiver<super::super::tray::TrayAction>,
    >,
    #[cfg(not(any(target_os = "windows", target_os = "macos")))] _action_rx: Option<
        &std::sync::mpsc::Receiver<super::super::tray::TrayAction>,
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
            if let Ok(super::super::tray::TrayAction::Quit) = rx.try_recv() {
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
        &std::sync::mpsc::Receiver<super::super::tray::TrayAction>,
        &std::sync::mpsc::Sender<super::super::tray::WrapperStatus>,
    )>,
    #[cfg(not(any(target_os = "windows", target_os = "macos")))] _tray: Option<(
        &std::sync::mpsc::Receiver<super::super::tray::TrayAction>,
        &std::sync::mpsc::Sender<super::super::tray::WrapperStatus>,
    )>,
) -> Result<()> {
    #[cfg(any(target_os = "windows", target_os = "macos"))]
    use super::super::tray::WrapperStatus;

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
                        super::super::tray::TrayAction::Quit => {
                            drop(child.kill());
                            drop(child.wait());
                            return Ok(());
                        }
                        super::super::tray::TrayAction::Restart => {
                            log_wrapper_event(log_dir, "Restart requested via tray");
                            drop(child.kill());
                            drop(child.wait());
                            break SENTINEL_RESTART;
                        }
                        super::super::tray::TrayAction::Stop => {
                            log_wrapper_event(log_dir, "Stop requested via tray");
                            drop(child.kill());
                            drop(child.wait());
                            break SENTINEL_STOP;
                        }
                        super::super::tray::TrayAction::Start => {
                            // Ignored while child is running — Start is only
                            // meaningful from the stopped state (handled below).
                        }
                        super::super::tray::TrayAction::ViewLogs => {
                            super::super::tray::open_log_file()
                        }
                        super::super::tray::TrayAction::CheckUpdate => {
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
                                        == Some(
                                            super::super::update::EXIT_CODE_BUNDLE_UPDATE_STAGED,
                                        ) =>
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
                        super::super::tray::TrayAction::OpenDashboard => {
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
                            super::super::tray::TrayAction::Start
                            | super::super::tray::TrayAction::Restart => {
                                log_wrapper_event(log_dir, "Start requested via tray");
                                break; // exit stopped loop, outer loop will relaunch
                            }
                            super::super::tray::TrayAction::Quit => {
                                return Ok(());
                            }
                            super::super::tray::TrayAction::ViewLogs => {
                                super::super::tray::open_log_file();
                            }
                            super::super::tray::TrayAction::CheckUpdate => {
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
                                                super::super::update::EXIT_CODE_BUNDLE_UPDATE_STAGED,
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
                            super::super::tray::TrayAction::OpenDashboard
                            | super::super::tray::TrayAction::Stop => {}
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
            let outcome = super::super::update::classify_update_subprocess(&result);

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
            match super::super::update::update_counter_action(outcome) {
                super::super::update::UpdateCounterAction::Clear => {
                    super::super::auto_update::clear_update_failures();
                }
                super::super::update::UpdateCounterAction::Record => {
                    super::super::auto_update::record_update_failure();
                }
                super::super::update::UpdateCounterAction::NoChange => {}
            }

            // macOS DMG-swap: if the update subprocess exits with the
            // bundle-staged code, the detached updater takes over after
            // this process exits. We must not re-exec or retry; just
            // return Ok so the wrapper exits cleanly and the updater can
            // swap /Applications/Freenet.app.
            if outcome == super::super::update::UpdateSubprocessOutcome::BundleUpdateStaged {
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

            let ok = outcome == super::super::update::UpdateSubprocessOutcome::BinaryReplaced;
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
                                let outcome =
                                    super::super::update::classify_update_subprocess(&result);

                                // Drive the persistent failure counter for
                                // the user-initiated check path identically
                                // to the auto-update path above — see the
                                // long comment there for the SpawnFailed vs.
                                // OtherFailure rationale (#3934 / Codex P1).
                                match super::super::update::update_counter_action(outcome) {
                                    super::super::update::UpdateCounterAction::Clear => {
                                        super::super::auto_update::clear_update_failures();
                                    }
                                    super::super::update::UpdateCounterAction::Record => {
                                        super::super::auto_update::record_update_failure();
                                    }
                                    super::super::update::UpdateCounterAction::NoChange => {}
                                }

                                match outcome {
                                    super::super::update::UpdateSubprocessOutcome::BinaryReplaced => {
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
                                    super::super::update::UpdateSubprocessOutcome::BundleUpdateStaged => {
                                        log_wrapper_event(
                                            log_dir,
                                            "Bundle update staged during backoff; exiting for updater",
                                        );
                                        status_tx.send(WrapperStatus::UpdatedRestarting).ok();
                                        return Ok(());
                                    }
                                    super::super::update::UpdateSubprocessOutcome::AlreadyUpToDate => {
                                        log_wrapper_event(log_dir, "No update available");
                                        status_tx.send(WrapperStatus::UpToDate).ok();
                                    }
                                    super::super::update::UpdateSubprocessOutcome::SpawnFailed => {
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
                                    super::super::update::UpdateSubprocessOutcome::OtherFailure => {
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
pub(super) fn log_wrapper_event(log_dir: &Path, message: &str) {
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
/// Called at wrapper startup and on port-conflict retry.
pub(super) fn kill_stale_freenet_processes(log_dir: &Path) {
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
        use super::windows::{FreenetServiceProcess, kill_freenet_processes_matching};
        if kill_freenet_processes_matching(FreenetServiceProcess::Node) > 0 {
            log_wrapper_event(
                log_dir,
                "Killed stale freenet network process(es) on startup",
            );
            std::thread::sleep(std::time::Duration::from_secs(2));
        }
    }
}
