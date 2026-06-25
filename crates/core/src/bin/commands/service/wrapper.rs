use anyhow::{Context, Result};
use std::path::Path;

#[cfg(target_os = "macos")]
use super::launch_at_login::macos_launch_at_login_startup;
#[cfg(target_os = "macos")]
use super::single_instance::{AcquireWrapperLockOutcome, acquire_wrapper_single_instance_lock};

#[cfg(any(target_os = "windows", target_os = "macos"))]
use super::single_instance::FIRST_RUN_OPENER_SPAWNED;

#[cfg(any(target_os = "windows", target_os = "macos"))]
use super::DASHBOARD_URL;
#[cfg(any(target_os = "windows", target_os = "macos"))]
use super::open_url_in_browser;
// First-run marker helpers + dashboard reachability probe live in the parent
// `service` module; the first-run onboarding opener below references them.
use super::{
    SENTINEL_RESTART, SENTINEL_STOP, WRAPPER_EXIT_ALREADY_RUNNING, WRAPPER_EXIT_UPDATE_NEEDED,
    WRAPPER_INITIAL_BACKOFF_SECS, WRAPPER_MAX_BACKOFF_SECS, WRAPPER_MAX_CONSECUTIVE_FAILURES,
    WRAPPER_MAX_PORT_CONFLICT_KILLS,
};
#[cfg(any(target_os = "windows", target_os = "macos"))]
use super::{
    dashboard_port_is_listening, first_run_marker_path, is_first_run_at, mark_first_run_complete_at,
};

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

/// Number of consecutive *identical* failures (same exit code + same detected
/// cause, e.g. repeated exit 43 against a held port) before the wrapper
/// surfaces a "stuck" signal to the operator (#4382).
///
/// Rationale for N = 3: a single failure is noise (a transient port race, a
/// one-off crash), and two could still be an unlucky double. By the third
/// identical failure the wrapper is demonstrably looping on the *same* cause
/// rather than making progress — the signature of the #3967 stale-orphan wedge,
/// where the wrapper repeatedly loses the port to a detached old binary and
/// exits 43 every cycle. Three is low enough to surface the problem within a
/// couple of backoff intervals (tens of seconds, not the ~50-failure give-up
/// horizon) yet high enough to avoid crying wolf on transient blips.
pub(super) const WRAPPER_STUCK_NOTIFY_THRESHOLD: u32 = 3;

/// File name (under the log directory) where the wrapper records a stuck-loop
/// status (failure count + last error). This is groundwork: the homepage
/// banner reader is not yet wired (tracked as a follow-up to #4382), so the
/// operator-facing signals delivered today are the cross-platform log line and
/// the macOS osascript notification. The file lets a future banner surface the
/// stuck state even when the served node is a stale orphan (dovetails with the
/// #4289 version-mismatch banner).
pub(super) const STUCK_STATUS_FILE_NAME: &str = "wrapper-stuck-status.json";

/// File name (under the log directory) where the wrapper persists a
/// *cross-process* identical-failure streak (#4382). The dominant #3967
/// manifestation — a stale orphan answering on the dashboard port — makes the
/// child exit 43 (`WRAPPER_EXIT_ALREADY_RUNNING`); the wrapper then exits and
/// (on macOS) launchd relaunches `service run-wrapper` fresh, resetting the
/// in-memory streak to 0 every cycle. Without disk persistence the in-process
/// `WRAPPER_STUCK_NOTIFY_THRESHOLD` is never reached across relaunches and the
/// signal never fires. This file lets consecutive exit-43 relaunches accumulate.
pub(super) const STUCK_STREAK_FILE_NAME: &str = "wrapper-stuck-streak.json";

/// TTL for the persisted cross-process streak file (#4382). If the last update
/// is older than this, the streak is considered stale and reset to a fresh
/// episode rather than counting an ancient failure toward the threshold. This
/// honours the GC/TTL discipline (AGENTS.md "WHEN writing cleanup/GC logic"):
/// the persisted counter must not be able to trigger forever off a stale file.
/// One hour comfortably spans the exit-43 relaunch cadence (launchd relaunches
/// within seconds) while preventing a days-old file from instantly tripping the
/// detector after an unrelated later failure.
pub(super) const STUCK_STREAK_TTL_SECS: u64 = 3600;

/// Distinguishing signature of a wrapper failure: the child exit code plus
/// whether it was detected as a port conflict. Two failures with the same
/// signature are "identical" for the purpose of the stuck-loop detector
/// (#4382) — the wrapper is looping on the same cause rather than making
/// progress.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct FailureSignature {
    pub(super) exit_code: i32,
    pub(super) is_port_conflict: bool,
}

/// State for the wrapper backoff state machine.
#[derive(Debug, Clone)]
pub(super) struct WrapperState {
    pub(super) backoff_secs: u64,
    pub(super) consecutive_failures: u32,
    pub(super) port_conflict_kills: u32,
    /// Signature of the most recent failure, used to count *identical*
    /// repeats (#4382). `None` before the first failure or after a success
    /// resets the streak.
    pub(super) last_failure_signature: Option<FailureSignature>,
    /// How many times in a row the same `last_failure_signature` has
    /// occurred (1 on first occurrence of a signature).
    pub(super) identical_failure_streak: u32,
    /// Set once the stuck notification has fired for the current streak so
    /// the wrapper surfaces the problem once per stuck episode, not on every
    /// subsequent identical failure.
    pub(super) stuck_notified: bool,
    /// Transient flag set by the most recent `next_wrapper_action` call when
    /// that call's failure pushed the identical-failure streak across
    /// `WRAPPER_STUCK_NOTIFY_THRESHOLD`. The loop reads it to fire the
    /// stuck notification + status-file write exactly once per episode.
    pub(super) stuck_just_crossed: bool,
    /// Transient flag set by the most recent `next_wrapper_action` call when
    /// that call's failure had a *different* signature than the previous one
    /// (a fresh episode started). The loop reads it to clear a stale status
    /// file written for the prior cause, so the homepage banner doesn't show
    /// the wrong recovery hint when the failure cause changes mid-loop (#4382
    /// review MINOR).
    pub(super) signature_changed: bool,
}

impl WrapperState {
    pub(super) fn new() -> Self {
        Self {
            backoff_secs: WRAPPER_INITIAL_BACKOFF_SECS,
            consecutive_failures: 0,
            port_conflict_kills: 0,
            last_failure_signature: None,
            identical_failure_streak: 0,
            stuck_notified: false,
            stuck_just_crossed: false,
            signature_changed: false,
        }
    }

    /// Record a failure with the given signature and update the
    /// identical-failure streak. Returns `true` exactly once per stuck
    /// episode: on the failure that first reaches
    /// `WRAPPER_STUCK_NOTIFY_THRESHOLD` consecutive identical failures, so the
    /// caller can surface the stuck signal a single time rather than on every
    /// repeat.
    pub(super) fn record_failure_signature(&mut self, sig: FailureSignature) -> bool {
        if self.last_failure_signature == Some(sig) {
            self.identical_failure_streak = self.identical_failure_streak.saturating_add(1);
        } else {
            // A different failure cause starts a fresh episode. Flag it so the
            // loop clears any stale status file written for the prior cause
            // (#4382 review MINOR); only flag a *change*, not the very first
            // failure (None -> Some), where there is no stale file to clear.
            if self.last_failure_signature.is_some() {
                self.signature_changed = true;
            }
            self.last_failure_signature = Some(sig);
            self.identical_failure_streak = 1;
            self.stuck_notified = false;
        }
        if self.identical_failure_streak >= WRAPPER_STUCK_NOTIFY_THRESHOLD && !self.stuck_notified {
            self.stuck_notified = true;
            return true;
        }
        false
    }

    /// Reset the identical-failure streak after a clean run / successful
    /// update so a later failure starts a fresh episode.
    pub(super) fn reset_failure_streak(&mut self) {
        self.last_failure_signature = None;
        self.identical_failure_streak = 0;
        self.stuck_notified = false;
        self.stuck_just_crossed = false;
        self.signature_changed = false;
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
    // Reset the per-call transients before recording this outcome; only a
    // failure that crosses the stuck threshold / changes signature sets them
    // back to true.
    state.stuck_just_crossed = false;
    state.signature_changed = false;
    let sig = FailureSignature {
        exit_code,
        is_port_conflict,
    };
    match exit_code {
        code if code == WRAPPER_EXIT_UPDATE_NEEDED => {
            match update_succeeded {
                Some(true) => {
                    state.consecutive_failures = 0;
                    state.port_conflict_kills = 0;
                    state.backoff_secs = WRAPPER_INITIAL_BACKOFF_SECS;
                    state.reset_failure_streak();
                    WrapperAction::Update
                }
                Some(false) => {
                    state.consecutive_failures += 1;
                    state.stuck_just_crossed = state.record_failure_signature(sig);
                    let secs = state.backoff_secs;
                    state.backoff_secs = (state.backoff_secs * 2).min(WRAPPER_MAX_BACKOFF_SECS);
                    WrapperAction::BackoffAndRelaunch { secs }
                }
                None => WrapperAction::Update, // Will be called again with result
            }
        }
        code if code == WRAPPER_EXIT_ALREADY_RUNNING => {
            // The child's pre-flight check_for_existing_process() saw a live
            // node already answering on the WS/dashboard port and exited 43
            // (#3967 stale-orphan signature). The child returns AlreadyRunning
            // *without* printing "already in use", so the stderr-derived
            // `is_port_conflict` is false here — but conceptually this IS a
            // port conflict (a stale orphan holding the port), so force the
            // flag in the recorded signature for honest messaging.
            //
            // The in-process streak alone never reaches the threshold here:
            // exit 43 returns Exit and the process terminates, so on macOS
            // launchd relaunches a fresh wrapper with a reset in-memory streak.
            // The loop therefore ALSO persists this signature to disk (see
            // persistent_streak_action) so consecutive exit-43 *relaunches*
            // across processes are what actually surface the signal. We still
            // record in-process for the rare same-process repeat.
            let sig = FailureSignature {
                exit_code,
                is_port_conflict: true,
            };
            state.stuck_just_crossed = state.record_failure_signature(sig);
            WrapperAction::Exit
        }
        0 => {
            // Clean exit — the wrapper is making progress, so a later failure
            // starts a fresh stuck episode.
            state.reset_failure_streak();
            WrapperAction::Exit
        }
        _ => {
            // Record the failure signature on EVERY crash path (including the
            // port-conflict kill-and-retry path): repeated identical port
            // conflicts are precisely the #3967 stale-orphan signature, so the
            // detector must see them even while we're still retrying.
            state.stuck_just_crossed = state.record_failure_signature(sig);
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

/// What the wrapper loop should do with the in-process stuck-status file after
/// a `next_wrapper_action` call (#4382).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum StuckLoopAction {
    /// Threshold just crossed this iteration: write the status file + notify.
    Write,
    /// The wrapper recovered (clean exit / successful update) or the failure
    /// cause changed mid-episode: clear any stale status file so the homepage
    /// banner doesn't show the wrong recovery hint.
    Clear,
    /// Neither — leave the status file untouched.
    None,
}

/// Pure decision for the in-process stuck-status file based on the transient
/// flags `next_wrapper_action` set on the state (#4382). Extracted so the
/// three branches are unit-testable without spawning processes or touching the
/// filesystem (mirrors the pure-core + platform-binding pattern in this file).
///
/// - `stuck_just_crossed`: this iteration's failure first reached the
///   threshold → write the file (takes precedence).
/// - `streak` reset to 0: clean exit / successful update → clear.
/// - `signature_changed`: a different cause started mid-loop → clear the stale
///   file written for the prior cause (the #4382 review MINOR; without this the
///   streak==1 case fell through both old branches and left the banner stale).
pub(super) fn stuck_loop_action(
    stuck_just_crossed: bool,
    signature_changed: bool,
    streak: u32,
) -> StuckLoopAction {
    if stuck_just_crossed {
        StuckLoopAction::Write
    } else if streak == 0 || signature_changed {
        StuckLoopAction::Clear
    } else {
        StuckLoopAction::None
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
        // Mark the child as supervised so it knows exit code 42 will actually be
        // caught and applied (issue #4580). Without this, the node can only fall
        // back to systemd's INVOCATION_ID, which the in-process wrapper path does
        // not set.
        cmd.env(super::super::auto_update::SUPERVISED_ENV_VAR, "1");

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

        // #4382 (cross-process): an exit-43 stale-orphan relaunch — the DOMINANT
        // #3967 manifestation — terminates this process, so the in-memory streak
        // never accumulates (launchd relaunches `service run-wrapper` fresh each
        // cycle). Persist the streak to disk so consecutive exit-43 relaunches
        // surface the signal. This is the *only* path that reaches the threshold
        // for the stable-orphan-on-port case.
        if exit_code == WRAPPER_EXIT_ALREADY_RUNNING {
            handle_persistent_stuck_relaunch(log_dir, exit_code, /* is_port_conflict */ true);
        }

        // #4382 (in-process): when the same failure has repeated
        // WRAPPER_STUCK_NOTIFY_THRESHOLD times in a row within a single wrapper
        // process (repeated non-43 crashes, or the bind-conflict KillAndRetry
        // window), surface it once. The pure `stuck_loop_action` decides Write
        // (threshold crossed), Clear (recovered, or the cause changed mid-loop
        // leaving a stale file), or None.
        match stuck_loop_action(
            state.stuck_just_crossed,
            state.signature_changed,
            state.identical_failure_streak,
        ) {
            StuckLoopAction::Write => {
                let status = StuckWrapperStatus::new(
                    exit_code,
                    is_port_conflict,
                    state.identical_failure_streak,
                );
                log_wrapper_event(log_dir, &status.log_line());
                write_stuck_status_file(log_dir, &status);
                notify_stuck_wrapper(&status);
            }
            StuckLoopAction::Clear => {
                // The wrapper is making progress again (or switched cause), so
                // clear any stale stuck-status file so the homepage banner
                // doesn't linger / show the wrong recovery hint. On a clean
                // exit / successful update also clear the persisted
                // cross-process streak so a later unrelated failure starts from
                // zero (GC discipline).
                clear_stuck_status_file(log_dir);
                if state.identical_failure_streak == 0 {
                    clear_stuck_streak_file(log_dir);
                }
            }
            StuckLoopAction::None => {}
        }

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

/// Status the wrapper writes to `STUCK_STATUS_FILE_NAME` once a repeated,
/// identical failure has crossed `WRAPPER_STUCK_NOTIFY_THRESHOLD` (#4382).
///
/// Serialized as JSON so a future homepage reader can render an operator
/// banner even when the served node is a stale orphan answering on the port.
/// The reader is not yet wired (follow-up to #4382); today's delivered signals
/// are the log line and the macOS osascript notification.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub(super) struct StuckWrapperStatus {
    /// Child exit code that keeps repeating.
    pub(super) exit_code: i32,
    /// Whether the repeated failure was detected as a port conflict.
    pub(super) is_port_conflict: bool,
    /// How many consecutive identical failures have occurred.
    pub(super) failure_count: u32,
    /// Human-readable last-error summary (exit code + cause).
    pub(super) last_error: String,
    /// One-line recovery hint for the operator.
    pub(super) recovery_hint: String,
}

impl StuckWrapperStatus {
    pub(super) fn new(exit_code: i32, is_port_conflict: bool, failure_count: u32) -> Self {
        let cause = if is_port_conflict {
            // exit 43 / "address already in use" against a held port is the
            // #3967 stale-orphan signature.
            format!("exit {exit_code} — the dashboard port is held by another process")
        } else {
            format!("exit {exit_code}")
        };
        let last_error =
            format!("Node failed {failure_count} times in a row with the same cause: {cause}.");
        let recovery_hint = if is_port_conflict {
            "The background service looks stuck on an old process holding the port. \
             Recover with: freenet service stop && freenet service start \
             (or kill the stale 'freenet network' process holding the dashboard port)."
                .to_string()
        } else {
            "The background service is repeatedly crashing on startup. \
             Recover with: freenet service stop && freenet service start, \
             then check the wrapper logs for the underlying error."
                .to_string()
        };
        Self {
            exit_code,
            is_port_conflict,
            failure_count,
            last_error,
            recovery_hint,
        }
    }

    /// Single log line summarizing the stuck condition.
    pub(super) fn log_line(&self) -> String {
        format!(
            "Wrapper appears stuck: {} {}",
            self.last_error, self.recovery_hint
        )
    }
}

/// Write the stuck status to `STUCK_STATUS_FILE_NAME` under the log directory.
/// Best-effort: a write failure must never block the wrapper loop.
pub(super) fn write_stuck_status_file(log_dir: &Path, status: &StuckWrapperStatus) {
    let path = log_dir.join(STUCK_STATUS_FILE_NAME);
    match serde_json::to_string_pretty(status) {
        Ok(json) => {
            if let Err(e) = std::fs::write(&path, json) {
                eprintln!("Failed to write stuck-status file {}: {e}", path.display());
            }
        }
        Err(e) => eprintln!("Failed to serialize stuck-status: {e}"),
    }
}

/// Remove the stuck status file (best-effort) once the wrapper recovers, so a
/// stale banner doesn't linger. A missing file is not an error.
pub(super) fn clear_stuck_status_file(log_dir: &Path) {
    let path = log_dir.join(STUCK_STATUS_FILE_NAME);
    if path.exists() {
        drop(std::fs::remove_file(&path));
    }
}

/// Persisted, *cross-process* identical-failure streak (#4382).
///
/// The dominant #3967 wedge makes the child exit 43 against a stale orphan on
/// the port; the wrapper then exits and launchd relaunches it fresh, so the
/// in-memory streak resets every cycle and the in-process detector never fires.
/// This record, written to `STUCK_STREAK_FILE_NAME` under the log dir, lets the
/// count survive across relaunches.
///
/// GC/TTL discipline (AGENTS.md "WHEN writing cleanup/GC logic"): the streak is
/// reset (not accumulated) whenever the cause changes, the binary `version`
/// changes, or the record is older than `STUCK_STREAK_TTL_SECS`. A stale file
/// can therefore never trip the detector forever.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub(super) struct PersistentStuckStreak {
    pub(super) exit_code: i32,
    pub(super) is_port_conflict: bool,
    /// Compiled binary version that observed this streak. A version change
    /// means a different binary — a fresh episode, not a continuation.
    pub(super) version: String,
    /// Consecutive identical cross-process failures so far.
    pub(super) count: u32,
    /// Unix seconds of the last update, for TTL-based staleness reset.
    pub(super) updated_unix_secs: u64,
}

/// Pure decision for the cross-process streak (#4382): given the previously
/// persisted record (if any), the new failure signature, the current binary
/// `version`, and the current time, compute the record to persist and whether
/// this update crosses `WRAPPER_STUCK_NOTIFY_THRESHOLD` for the first time.
///
/// The streak resets to 1 (a fresh episode) when the signature differs, the
/// version differs, or the prior record is older than `STUCK_STREAK_TTL_SECS`.
/// It fires exactly once — on the relaunch whose `count` first reaches the
/// threshold — so a long stale loop doesn't re-notify on every cycle.
///
/// Kept pure (no filesystem/clock access) so the three branches — accumulate,
/// reset-on-cause/version-change, reset-on-TTL — are unit-testable.
pub(super) fn persistent_streak_action(
    prior: Option<&PersistentStuckStreak>,
    exit_code: i32,
    is_port_conflict: bool,
    version: &str,
    now_unix_secs: u64,
) -> (PersistentStuckStreak, bool) {
    let continues = prior.is_some_and(|p| {
        p.exit_code == exit_code
            && p.is_port_conflict == is_port_conflict
            && p.version == version
            && now_unix_secs.saturating_sub(p.updated_unix_secs) <= STUCK_STREAK_TTL_SECS
    });
    let count = if continues {
        // Safe: `continues` implies `prior` is Some.
        prior.map(|p| p.count).unwrap_or(0).saturating_add(1)
    } else {
        1
    };
    // Fire exactly on the transition into the threshold, not on every later
    // identical relaunch.
    let just_crossed = count == WRAPPER_STUCK_NOTIFY_THRESHOLD;
    let record = PersistentStuckStreak {
        exit_code,
        is_port_conflict,
        version: version.to_string(),
        count,
        updated_unix_secs: now_unix_secs,
    };
    (record, just_crossed)
}

/// Read the persisted cross-process streak record, if present and parseable.
/// A missing or malformed file is treated as "no prior streak".
pub(super) fn read_stuck_streak_file(log_dir: &Path) -> Option<PersistentStuckStreak> {
    let path = log_dir.join(STUCK_STREAK_FILE_NAME);
    let json = std::fs::read_to_string(&path).ok()?;
    serde_json::from_str(&json).ok()
}

/// Write the persisted cross-process streak record (best-effort).
pub(super) fn write_stuck_streak_file(log_dir: &Path, record: &PersistentStuckStreak) {
    let path = log_dir.join(STUCK_STREAK_FILE_NAME);
    match serde_json::to_string_pretty(record) {
        Ok(json) => {
            if let Err(e) = std::fs::write(&path, json) {
                eprintln!("Failed to write stuck-streak file {}: {e}", path.display());
            }
        }
        Err(e) => eprintln!("Failed to serialize stuck-streak: {e}"),
    }
}

/// Remove the persisted cross-process streak file (best-effort) once the
/// wrapper makes progress, so a later unrelated failure starts from zero.
pub(super) fn clear_stuck_streak_file(log_dir: &Path) {
    let path = log_dir.join(STUCK_STREAK_FILE_NAME);
    if path.exists() {
        drop(std::fs::remove_file(&path));
    }
}

/// Current wall-clock time in Unix seconds (real time is correct here: this is
/// operator-facing staleness bookkeeping on disk across OS process restarts,
/// not simulated node logic, and lives in the `src/bin` CLI rather than the
/// `crates/core` library subject to the `TimeSource` rule).
fn now_unix_secs() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

/// Record an exit-43-style relaunch (stale orphan on the port) against the
/// persisted cross-process streak and, if it first crosses the threshold,
/// surface the stuck signal. Called on the `WRAPPER_EXIT_ALREADY_RUNNING` path
/// in the loop, since that path exits the process before the in-memory streak
/// can ever accumulate.
fn handle_persistent_stuck_relaunch(log_dir: &Path, exit_code: i32, is_port_conflict: bool) {
    let prior = read_stuck_streak_file(log_dir);
    let (record, just_crossed) = persistent_streak_action(
        prior.as_ref(),
        exit_code,
        is_port_conflict,
        env!("CARGO_PKG_VERSION"),
        now_unix_secs(),
    );
    write_stuck_streak_file(log_dir, &record);
    if just_crossed {
        let status = StuckWrapperStatus::new(exit_code, is_port_conflict, record.count);
        log_wrapper_event(log_dir, &status.log_line());
        write_stuck_status_file(log_dir, &status);
        notify_stuck_wrapper(&status);
    }
}

/// Surface the stuck condition as a desktop notification. macOS uses
/// `osascript display notification`; other platforms rely on the status file +
/// log line (the banner half of #4382), so this is a no-op there.
fn notify_stuck_wrapper(status: &StuckWrapperStatus) {
    #[cfg(target_os = "macos")]
    {
        // AppleScript string-literal escaping: backslash and double-quote.
        let body = applescript_escape(&status.last_error);
        let title = applescript_escape("Freenet service may be stuck");
        let script = format!("display notification \"{body}\" with title \"{title}\"");
        // Best-effort: a notification failure must never block the wrapper.
        if let Err(e) = std::process::Command::new("osascript")
            .arg("-e")
            .arg(&script)
            // Null all three handles: the wrapper has detached from its
            // console (see the FreeConsole cross-reference in run_wrapper).
            .stdin(std::process::Stdio::null())
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null())
            .spawn()
        {
            eprintln!("Failed to post stuck-wrapper notification: {e}");
        }
    }
    #[cfg(not(target_os = "macos"))]
    {
        let _ = status;
    }
}

/// Escape a string for safe interpolation into an AppleScript string literal
/// (backslash first, then double-quote). Kept as a pure function so it can be
/// unit-tested on non-macOS CI.
#[allow(dead_code)] // Only called on macOS, but tested everywhere.
pub(super) fn applescript_escape(s: &str) -> String {
    s.replace('\\', "\\\\").replace('"', "\\\"")
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
