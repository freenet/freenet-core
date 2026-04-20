//! System tray/menu bar icon for the Freenet run-wrapper.
//!
//! Displays a tray icon with a right-click menu while the Freenet node
//! is running. The menu mirrors `freenet service` CLI commands plus
//! extras like "Open Dashboard" and "View Logs".
//!
//! Supported on Windows (system tray) and macOS (menu bar).
//! On other platforms, this module provides no-op stubs.

// Types are used on Windows/macOS only; suppress warnings on Linux.
#[allow(unused_imports)]
use std::sync::mpsc;

/// Actions the tray icon can send to the wrapper loop.
#[derive(Debug, Clone, PartialEq, Eq)]
#[allow(dead_code)]
pub enum TrayAction {
    /// Open the local dashboard in the default browser.
    OpenDashboard,
    /// Kill the child process; the wrapper loop will relaunch it.
    Restart,
    /// Stop the node but keep the tray running.
    Stop,
    /// Start the node (after a Stop).
    Start,
    /// Run `freenet update --check` and report the result.
    CheckUpdate,
    /// Open the latest log file in the system viewer.
    ViewLogs,
    /// Kill the child process and exit the wrapper.
    Quit,
}

/// Status updates from the wrapper loop to the tray icon.
#[derive(Debug, Clone)]
#[allow(dead_code)]
pub enum WrapperStatus {
    /// The freenet node is running normally.
    Running,
    /// An auto-update is in progress.
    Updating,
    /// The node has stopped (wrapper is in backoff or exiting).
    Stopped,
    /// Update check completed — already on the latest version.
    UpToDate,
    /// Update installed — wrapper will re-exec with the new binary.
    /// The tray loop should exit so the process can terminate.
    UpdatedRestarting,
}

// ── Menu item IDs and dispatch logic (platform-independent) ──
//
// The string IDs are explicit so that menu events can be dispatched by
// comparing the event's id to known constants. Keeping these outside the
// platform module lets them compile and be unit-tested on any target,
// including Linux CI where muda/tray-icon aren't available.

const MENU_ID_OPEN_DASHBOARD: &str = "freenet.tray.open_dashboard";
// Status and version items are display-only and never dispatched, but we still
// give them stable IDs for parity with the interactive items.
#[allow(dead_code)]
const MENU_ID_STATUS: &str = "freenet.tray.status";
#[allow(dead_code)]
const MENU_ID_VERSION: &str = "freenet.tray.version";
const MENU_ID_STOP: &str = "freenet.tray.stop";
const MENU_ID_START: &str = "freenet.tray.start";
const MENU_ID_RESTART: &str = "freenet.tray.restart";
const MENU_ID_CHECK_UPDATE: &str = "freenet.tray.check_update";
const MENU_ID_VIEW_LOGS: &str = "freenet.tray.view_logs";
const MENU_ID_QUIT: &str = "freenet.tray.quit";

/// What should happen in response to a menu event. Side-effects (opening
/// URLs, sending actions through channels, or exiting the loop) are the
/// caller's responsibility so this decision is pure and unit-testable.
#[derive(Debug, PartialEq, Eq)]
#[allow(dead_code)]
enum MenuDispatch {
    OpenDashboard,
    Action(TrayAction),
    Quit,
    Unknown,
}

#[allow(dead_code)]
fn dispatch_menu_event(event_id: &str) -> MenuDispatch {
    match event_id {
        MENU_ID_OPEN_DASHBOARD => MenuDispatch::OpenDashboard,
        MENU_ID_STOP => MenuDispatch::Action(TrayAction::Stop),
        MENU_ID_START => MenuDispatch::Action(TrayAction::Start),
        MENU_ID_RESTART => MenuDispatch::Action(TrayAction::Restart),
        MENU_ID_CHECK_UPDATE => MenuDispatch::Action(TrayAction::CheckUpdate),
        MENU_ID_VIEW_LOGS => MenuDispatch::Action(TrayAction::ViewLogs),
        MENU_ID_QUIT => MenuDispatch::Quit,
        _ => MenuDispatch::Unknown,
    }
}

/// Derived UI state for a given `WrapperStatus`. Kept as a pure value so the
/// status → UI projection is unit-testable on every platform (including Linux
/// CI), independent of the AppKit / Win32 widget plumbing in the platform
/// module.
#[derive(Debug, PartialEq, Eq)]
#[allow(dead_code)]
struct MenuState {
    status_text: &'static str,
    stop_enabled: bool,
    start_enabled: bool,
    restart_enabled: bool,
    is_terminal: bool,
}

#[allow(dead_code)]
fn compute_menu_state(status: &WrapperStatus) -> MenuState {
    let status_text = match status {
        WrapperStatus::Running => "Running",
        WrapperStatus::Updating => "Checking for updates...",
        WrapperStatus::Stopped => "Stopped",
        WrapperStatus::UpToDate => "Up to date",
        WrapperStatus::UpdatedRestarting => "Updated! Restarting...",
    };
    let is_running = matches!(
        status,
        WrapperStatus::Running | WrapperStatus::UpToDate | WrapperStatus::Updating
    );
    MenuState {
        status_text,
        stop_enabled: is_running,
        start_enabled: !is_running,
        restart_enabled: is_running,
        is_terminal: matches!(status, WrapperStatus::UpdatedRestarting),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn dispatch_maps_known_menu_items_to_their_actions() {
        assert_eq!(
            dispatch_menu_event(MENU_ID_OPEN_DASHBOARD),
            MenuDispatch::OpenDashboard
        );
        assert_eq!(
            dispatch_menu_event(MENU_ID_STOP),
            MenuDispatch::Action(TrayAction::Stop)
        );
        assert_eq!(
            dispatch_menu_event(MENU_ID_START),
            MenuDispatch::Action(TrayAction::Start)
        );
        assert_eq!(
            dispatch_menu_event(MENU_ID_RESTART),
            MenuDispatch::Action(TrayAction::Restart)
        );
        assert_eq!(
            dispatch_menu_event(MENU_ID_CHECK_UPDATE),
            MenuDispatch::Action(TrayAction::CheckUpdate)
        );
        assert_eq!(
            dispatch_menu_event(MENU_ID_VIEW_LOGS),
            MenuDispatch::Action(TrayAction::ViewLogs)
        );
        assert_eq!(dispatch_menu_event(MENU_ID_QUIT), MenuDispatch::Quit);
    }

    #[test]
    fn dispatch_returns_unknown_for_foreign_ids() {
        assert_eq!(dispatch_menu_event(""), MenuDispatch::Unknown);
        assert_eq!(dispatch_menu_event("stop"), MenuDispatch::Unknown);
        assert_eq!(
            dispatch_menu_event("freenet.tray.nonexistent"),
            MenuDispatch::Unknown
        );
    }

    #[test]
    fn menu_state_enables_stop_while_running() {
        for status in [
            WrapperStatus::Running,
            WrapperStatus::UpToDate,
            WrapperStatus::Updating,
        ] {
            let s = compute_menu_state(&status);
            assert!(s.stop_enabled, "Stop should be enabled for {status:?}");
            assert!(!s.start_enabled, "Start should be disabled for {status:?}");
            assert!(
                s.restart_enabled,
                "Restart should be enabled for {status:?}"
            );
            assert!(!s.is_terminal, "{status:?} is not terminal");
        }
    }

    #[test]
    fn menu_state_enables_start_while_stopped() {
        let s = compute_menu_state(&WrapperStatus::Stopped);
        assert!(!s.stop_enabled);
        assert!(s.start_enabled);
        assert!(!s.restart_enabled);
        assert!(!s.is_terminal);
        assert_eq!(s.status_text, "Stopped");
    }

    #[test]
    fn menu_state_flags_terminal_on_updated_restarting() {
        let s = compute_menu_state(&WrapperStatus::UpdatedRestarting);
        assert!(s.is_terminal);
        assert_eq!(s.status_text, "Updated! Restarting...");
    }

    #[test]
    fn menu_state_status_texts_are_distinct() {
        let texts: Vec<&'static str> = [
            WrapperStatus::Running,
            WrapperStatus::Updating,
            WrapperStatus::Stopped,
            WrapperStatus::UpToDate,
            WrapperStatus::UpdatedRestarting,
        ]
        .iter()
        .map(|s| compute_menu_state(s).status_text)
        .collect();
        let mut sorted = texts.clone();
        sorted.sort();
        sorted.dedup();
        assert_eq!(
            texts.len(),
            sorted.len(),
            "each WrapperStatus should map to a distinct status_text"
        );
    }
}

// ── Windows + macOS implementation ──────────────────────────────────

#[cfg(any(target_os = "windows", target_os = "macos"))]
mod platform {
    use super::*;
    use muda::{Menu, MenuEvent, MenuId, MenuItem, PredefinedMenuItem};
    use std::sync::mpsc as std_mpsc;
    use tray_icon::{Icon, TrayIcon, TrayIconBuilder};

    const DASHBOARD_URL: &str = super::super::service::DASHBOARD_URL;

    /// Pre-rendered 256x256 RGBA icon of the Freenet logo.
    /// 256x256 gives the OS full detail to downscale from at any DPI.
    fn build_icon() -> Result<Icon, tray_icon::BadIcon> {
        let rgba = include_bytes!("assets/freenet_256x256.rgba").to_vec();
        Icon::from_rgba(rgba, 256, 256)
    }

    /// Open a URL in the default browser, platform-appropriately.
    fn open_url(url: &str) {
        super::super::open_url_in_browser(url);
    }

    /// Construct a menu item with an explicit string ID. We set IDs
    /// explicitly so `dispatch_menu_event` can match on stable constants
    /// rather than auto-generated numeric IDs (which would also change
    /// between runs, defeating unit tests of the dispatch logic).
    fn menu_item(id: &'static str, text: &str, enabled: bool) -> MenuItem {
        MenuItem::with_id(MenuId::new(id), text, enabled, None)
    }

    /// Owns the tray icon and the mutable menu items whose state changes at
    /// runtime (status text, enable/disable toggles).
    struct TrayState {
        // Held only to keep the tray icon alive; dropped on shutdown.
        _tray: TrayIcon,
        version: String,
        status_item: MenuItem,
        stop_item: MenuItem,
        start_item: MenuItem,
        restart_item: MenuItem,
    }

    impl TrayState {
        fn new(version: String) -> Result<Self, String> {
            let menu = Menu::new();

            // App header — disabled item at the top of the menu that
            // identifies the app and shows its version. Serves the same
            // role as the bold app-name entry at the top of standard
            // macOS app menus; keeps the menu bar status item itself
            // compact (icon only) rather than eating horizontal space
            // with a text label.
            let app_header = menu_item(MENU_ID_VERSION, &format!("Freenet {version}"), false);
            let open_dashboard = menu_item(MENU_ID_OPEN_DASHBOARD, "Open Dashboard", true);
            let status_item = menu_item(MENU_ID_STATUS, "Status: Starting...", false);
            let stop_item = menu_item(MENU_ID_STOP, "Stop", true);
            let start_item = menu_item(MENU_ID_START, "Start", false);
            let restart_item = menu_item(MENU_ID_RESTART, "Restart", true);
            let check_update = menu_item(MENU_ID_CHECK_UPDATE, "Check for Updates", true);
            let view_logs = menu_item(MENU_ID_VIEW_LOGS, "View Logs", true);
            let quit_item = menu_item(MENU_ID_QUIT, "Quit", true);

            menu.append(&app_header).ok();
            menu.append(&PredefinedMenuItem::separator()).ok();
            menu.append(&open_dashboard).ok();
            menu.append(&PredefinedMenuItem::separator()).ok();
            menu.append(&status_item).ok();
            menu.append(&PredefinedMenuItem::separator()).ok();
            menu.append(&stop_item).ok();
            menu.append(&start_item).ok();
            menu.append(&restart_item).ok();
            menu.append(&check_update).ok();
            menu.append(&view_logs).ok();
            menu.append(&PredefinedMenuItem::separator()).ok();
            menu.append(&quit_item).ok();

            let icon = build_icon().map_err(|e| format!("Failed to build tray icon: {e}"))?;

            let tray = TrayIconBuilder::new()
                .with_menu(Box::new(menu))
                .with_tooltip(format!("Freenet {version} - Starting..."))
                .with_icon(icon)
                .build()
                .map_err(|e| format!("Failed to create tray icon: {e}"))?;

            Ok(Self {
                _tray: tray,
                version,
                status_item,
                stop_item,
                start_item,
                restart_item,
            })
        }

        /// Applies a status update to the tray UI. Returns `true` if the
        /// status is terminal (tray should exit shortly afterwards).
        ///
        /// All derived state (status text, enabled flags, terminal flag)
        /// comes from `compute_menu_state`, which is pure and unit-tested
        /// on every platform. This method is the thin AppKit/Win32 binding
        /// that applies the computed state to the real widgets.
        fn apply_status(&self, status: &WrapperStatus) -> bool {
            let s = compute_menu_state(status);
            self.status_item
                .set_text(format!("Status: {}", s.status_text));
            let _ = self._tray.set_tooltip(Some(format!(
                "Freenet {} - {}",
                self.version, s.status_text
            )));
            self.stop_item.set_enabled(s.stop_enabled);
            self.start_item.set_enabled(s.start_enabled);
            self.restart_item.set_enabled(s.restart_enabled);
            s.is_terminal
        }
    }

    // ── Windows event loop ──
    //
    // Windows drives the tray via a hidden HWND; we pump Win32 messages
    // manually on the main thread and poll muda's event channel.

    #[cfg(target_os = "windows")]
    fn pump_win32_messages() -> bool {
        use winapi::um::winuser::{
            DispatchMessageW, PM_REMOVE, PeekMessageW, TranslateMessage, WM_QUIT,
        };
        // Safety: standard Win32 message pump on the thread that created the HWND.
        unsafe {
            let mut msg = std::mem::zeroed();
            while PeekMessageW(&mut msg, std::ptr::null_mut(), 0, 0, PM_REMOVE) != 0 {
                if msg.message == WM_QUIT {
                    return true;
                }
                TranslateMessage(&msg);
                DispatchMessageW(&msg);
            }
        }
        false
    }

    #[cfg(target_os = "windows")]
    fn run_event_loop(
        state: TrayState,
        action_tx: std_mpsc::Sender<TrayAction>,
        status_rx: std_mpsc::Receiver<WrapperStatus>,
        _cleanup_done_rx: std_mpsc::Receiver<()>,
    ) {
        // Windows doesn't need the cleanup signal — the Win32 message pump
        // returns control to the caller normally, so `run_wrapper` can join
        // the wrapper thread afterwards. The parameter exists for signature
        // parity with the macOS path.
        let menu_rx = MenuEvent::receiver();

        loop {
            if pump_win32_messages() {
                action_tx.send(TrayAction::Quit).ok();
                break;
            }

            if let Ok(event) = menu_rx.try_recv() {
                match dispatch_menu_event(event.id.as_ref()) {
                    MenuDispatch::OpenDashboard => open_url(DASHBOARD_URL),
                    MenuDispatch::Action(action) => {
                        action_tx.send(action).ok();
                    }
                    MenuDispatch::Quit => {
                        action_tx.send(TrayAction::Quit).ok();
                        break;
                    }
                    MenuDispatch::Unknown => {}
                }
            }

            if let Ok(status) = status_rx.try_recv() {
                let is_terminal = state.apply_status(&status);
                if is_terminal {
                    std::thread::sleep(std::time::Duration::from_secs(1));
                    action_tx.send(TrayAction::Quit).ok();
                    break;
                }
            }

            std::thread::sleep(std::time::Duration::from_millis(100));
        }
    }

    // ── macOS event loop ──
    //
    // On macOS a menu bar status item is only rendered and interactive
    // while an NSApplication event loop is pumping on the main thread.
    // The `tray-icon` crate registers the `NSStatusItem` but does *not*
    // drive the run loop — the host application has to. We use
    // `tao::EventLoop`, which wraps `NSApplication.run()` correctly and
    // matches tray-icon's own published examples. Status updates from the
    // wrapper thread are forwarded in as user events so they land on the
    // main thread, where AppKit state mutation is allowed.
    //
    // Historical note: a previous version of this module had a no-op
    // `pump_platform_messages` on macOS with a comment claiming that
    // `tray-icon` drove the NSRunLoop internally. That was not true, and
    // the menu bar icon never appeared in practice.

    #[cfg(target_os = "macos")]
    fn run_event_loop(
        state: TrayState,
        action_tx: std_mpsc::Sender<TrayAction>,
        status_rx: std_mpsc::Receiver<WrapperStatus>,
        cleanup_done_rx: std_mpsc::Receiver<()>,
    ) {
        use std::time::{Duration, Instant};
        use tao::event::Event;
        use tao::event_loop::{ControlFlow, EventLoopBuilder};

        let event_loop = EventLoopBuilder::<WrapperStatus>::with_user_event().build();
        let proxy = event_loop.create_proxy();

        // Forward status updates into the tao event loop so they're
        // delivered on the main thread (AppKit forbids mutating
        // NSMenuItem off-main).
        std::thread::spawn(move || {
            while let Ok(status) = status_rx.recv() {
                let is_terminal = matches!(&status, WrapperStatus::UpdatedRestarting);
                if proxy.send_event(status).is_err() {
                    break;
                }
                if is_terminal {
                    break;
                }
            }
        });

        let menu_rx = MenuEvent::receiver();

        event_loop.run(move |event, _window, control_flow| {
            // Wake at least every 100ms to poll muda's menu event channel
            // — muda delivers menu events on its own channel rather than
            // via tao's event stream, so we can't rely on Poll/Wait alone.
            *control_flow = ControlFlow::WaitUntil(Instant::now() + Duration::from_millis(100));

            // Drain menu events.
            while let Ok(menu_event) = menu_rx.try_recv() {
                match dispatch_menu_event(menu_event.id.as_ref()) {
                    MenuDispatch::OpenDashboard => open_url(DASHBOARD_URL),
                    MenuDispatch::Action(action) => {
                        action_tx.send(action).ok();
                    }
                    MenuDispatch::Quit => {
                        action_tx.send(TrayAction::Quit).ok();
                        // Block until the wrapper thread confirms it has killed
                        // the daemon child. On macOS, tao's run is divergent:
                        // when we set ControlFlow::Exit the process exits
                        // without unwinding other threads, so we must wait
                        // until the wrapper has finished cleanup or we risk
                        // orphaning the daemon. The wrapper's spawn thunk
                        // signals `cleanup_done_rx` on every exit path of
                        // `run_wrapper_loop`, so this waits forever only if
                        // the wrapper itself is genuinely stuck (e.g. in a
                        // long `freenet update` that the user triggered via
                        // "Check for Updates"). Waiting is the right trade
                        // because the alternative is a guaranteed orphan.
                        let _ = cleanup_done_rx.recv();
                        *control_flow = ControlFlow::Exit;
                        return;
                    }
                    MenuDispatch::Unknown => {}
                }
            }

            // Apply status updates forwarded from the wrapper thread.
            if let Event::UserEvent(status) = event {
                let is_terminal = state.apply_status(&status);
                if is_terminal {
                    std::thread::sleep(Duration::from_secs(1));
                    action_tx.send(TrayAction::Quit).ok();
                    // Same rationale as the Quit handler above.
                    let _ = cleanup_done_rx.recv();
                    *control_flow = ControlFlow::Exit;
                }
            }
        });
    }

    /// Run the tray icon event loop on the current thread (must be the main
    /// thread — both Windows and macOS require UI work on the main thread).
    ///
    /// `action_tx` sends user menu actions to the wrapper loop running on
    /// another thread. `status_rx` receives status updates from the wrapper
    /// loop to update the tooltip. `cleanup_done_rx` receives a signal once
    /// the wrapper thread has finished its shutdown work (killing the daemon
    /// child); the macOS event loop waits briefly on it before exiting so
    /// the daemon doesn't get orphaned when the process exits. `version` is
    /// the current freenet version string for display.
    pub fn run_tray_event_loop(
        action_tx: std_mpsc::Sender<TrayAction>,
        status_rx: std_mpsc::Receiver<WrapperStatus>,
        cleanup_done_rx: std_mpsc::Receiver<()>,
        version: &str,
    ) {
        let state = match TrayState::new(version.to_string()) {
            Ok(s) => s,
            Err(e) => {
                eprintln!("{e}. Running without tray.");
                return;
            }
        };
        run_event_loop(state, action_tx, status_rx, cleanup_done_rx);
    }
}

// ── Stub for platforms without tray support (Linux, etc.) ───────────

#[cfg(not(any(target_os = "windows", target_os = "macos")))]
mod platform {
    use super::*;
    use std::sync::mpsc as std_mpsc;

    /// No-op on unsupported platforms. Returns immediately.
    pub fn run_tray_event_loop(
        _action_tx: std_mpsc::Sender<TrayAction>,
        _status_rx: std_mpsc::Receiver<WrapperStatus>,
        _cleanup_done_rx: std_mpsc::Receiver<()>,
        _version: &str,
    ) {
        // Tray icon not supported. The wrapper loop runs directly on the
        // main thread without a tray.
    }
}

#[allow(unused_imports, dead_code)]
pub use platform::run_tray_event_loop;

/// Open the latest log file in the platform's default viewer.
#[allow(dead_code)]
pub fn open_log_file() {
    use freenet::tracing::tracer::get_log_dir;

    let Some(log_dir) = get_log_dir() else {
        eprintln!("Could not determine log directory");
        return;
    };

    let latest = super::service::find_latest_log_file(&log_dir, "freenet");

    match latest {
        Some(path) => {
            #[cfg(target_os = "windows")]
            {
                std::process::Command::new("notepad")
                    .arg(&path)
                    .spawn()
                    .ok();
            }
            #[cfg(target_os = "macos")]
            {
                std::process::Command::new("open").arg(&path).spawn().ok();
            }
            #[cfg(not(any(target_os = "windows", target_os = "macos")))]
            {
                drop(std::process::Command::new("xdg-open").arg(&path).spawn());
            }
        }
        None => {
            eprintln!("No log files found in {}", log_dir.display());
        }
    }
}
