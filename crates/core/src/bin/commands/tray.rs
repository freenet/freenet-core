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
#[derive(Debug)]
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

// ── Windows + macOS implementation ──────────────────────────────────

#[cfg(any(target_os = "windows", target_os = "macos"))]
mod platform {
    use super::*;
    use muda::{Menu, MenuEvent, MenuItem, PredefinedMenuItem};
    use std::sync::mpsc as std_mpsc;
    use tray_icon::{Icon, TrayIconBuilder};

    const DASHBOARD_URL: &str = super::super::service::DASHBOARD_URL;

    /// Dispatch pending Win32 messages so the tray-icon crate's hidden HWND
    /// receives user interaction events (WM_USER_TRAYICON → right-click menu).
    /// On macOS, tray-icon uses a Cocoa run-loop internally, so this is a no-op.
    /// Returns `true` if `WM_QUIT` was received (caller should exit the loop).
    #[cfg(target_os = "windows")]
    fn pump_platform_messages() -> bool {
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

    #[cfg(target_os = "macos")]
    fn pump_platform_messages() -> bool {
        // macOS: tray-icon drives the NSRunLoop internally; nothing to pump.
        false
    }

    /// Build the tray icon from embedded RGBA pixel data of the Freenet logo.
    /// Pre-rendered from the SVG at freenet/web with the blue gradient preserved.
    /// Uses 256x256 as the source so the OS has full detail to downscale from
    /// at any DPI (100%-300%). Adds ~256KB to the binary.
    fn build_icon() -> Result<Icon, tray_icon::BadIcon> {
        let rgba = include_bytes!("assets/freenet_256x256.rgba").to_vec();
        Icon::from_rgba(rgba, 256, 256)
    }

    /// Open a URL in the default browser, platform-appropriately.
    fn open_url(url: &str) {
        #[cfg(target_os = "windows")]
        {
            std::process::Command::new("cmd")
                .args(["/c", "start", url])
                .spawn()
                .ok();
        }
        #[cfg(target_os = "macos")]
        {
            std::process::Command::new("open").arg(url).spawn().ok();
        }
    }

    /// Run the tray icon event loop on the current thread (must be the main thread
    /// for the platform message pump to work — required by both Windows and macOS).
    ///
    /// `action_tx` sends user menu actions to the wrapper loop running on another thread.
    /// `status_rx` receives status updates from the wrapper loop to update the tooltip.
    /// `version` is the current freenet version string for display.
    pub fn run_tray_event_loop(
        action_tx: std_mpsc::Sender<TrayAction>,
        status_rx: std_mpsc::Receiver<WrapperStatus>,
        version: &str,
    ) {
        let menu = Menu::new();

        let open_dashboard = MenuItem::new("Open Dashboard", true, None);
        let separator1 = PredefinedMenuItem::separator();
        let status_item = MenuItem::new("Status: Starting...", false, None);
        let version_item = MenuItem::new(format!("Version: {version}"), false, None);
        let separator2 = PredefinedMenuItem::separator();
        // Stop is enabled when running, Start is enabled when stopped
        let stop_item = MenuItem::new("Stop", true, None);
        let start_item = MenuItem::new("Start", false, None);
        let restart_item = MenuItem::new("Restart", true, None);
        let check_update = MenuItem::new("Check for Updates", true, None);
        let view_logs = MenuItem::new("View Logs", true, None);
        let separator3 = PredefinedMenuItem::separator();
        let quit_item = MenuItem::new("Quit", true, None);

        menu.append(&open_dashboard).ok();
        menu.append(&separator1).ok();
        menu.append(&status_item).ok();
        menu.append(&version_item).ok();
        menu.append(&separator2).ok();
        menu.append(&stop_item).ok();
        menu.append(&start_item).ok();
        menu.append(&restart_item).ok();
        menu.append(&check_update).ok();
        menu.append(&view_logs).ok();
        menu.append(&separator3).ok();
        menu.append(&quit_item).ok();

        let icon = match build_icon() {
            Ok(i) => i,
            Err(e) => {
                eprintln!("Failed to build tray icon: {e}. Running without tray.");
                return;
            }
        };

        let _tray = match TrayIconBuilder::new()
            .with_menu(Box::new(menu))
            .with_tooltip(format!("Freenet {version} - Starting..."))
            .with_icon(icon)
            .build()
        {
            Ok(t) => t,
            Err(e) => {
                eprintln!("Failed to create tray icon: {e}. Running without tray.");
                return;
            }
        };

        let menu_rx = MenuEvent::receiver();

        // Capture menu item IDs for matching
        let open_dashboard_id = open_dashboard.id().clone();
        let stop_id = stop_item.id().clone();
        let start_id = start_item.id().clone();
        let restart_id = restart_item.id().clone();
        let check_update_id = check_update.id().clone();
        let view_logs_id = view_logs.id().clone();
        let quit_id = quit_item.id().clone();

        loop {
            // Pump platform messages so the tray-icon crate's hidden HWND
            // receives user interaction events (right-click, etc.).
            if pump_platform_messages() {
                action_tx.send(TrayAction::Quit).ok();
                break;
            }

            // Process menu events (non-blocking peek)
            if let Ok(event) = menu_rx.try_recv() {
                let action = if event.id == open_dashboard_id {
                    open_url(DASHBOARD_URL);
                    None
                } else if event.id == stop_id {
                    Some(TrayAction::Stop)
                } else if event.id == start_id {
                    Some(TrayAction::Start)
                } else if event.id == restart_id {
                    Some(TrayAction::Restart)
                } else if event.id == check_update_id {
                    Some(TrayAction::CheckUpdate)
                } else if event.id == view_logs_id {
                    Some(TrayAction::ViewLogs)
                } else if event.id == quit_id {
                    action_tx.send(TrayAction::Quit).ok();
                    break;
                } else {
                    None
                };

                if let Some(action) = action {
                    action_tx.send(action).ok();
                }
            }

            // Check for status updates from the wrapper loop
            if let Ok(status) = status_rx.try_recv() {
                let status_text = match &status {
                    WrapperStatus::Running => "Running",
                    WrapperStatus::Updating => "Checking for updates...",
                    WrapperStatus::Stopped => "Stopped",
                    WrapperStatus::UpToDate => "Up to date",
                    WrapperStatus::UpdatedRestarting => "Updated! Restarting...",
                };
                status_item.set_text(format!("Status: {status_text}"));
                _tray
                    .set_tooltip(Some(format!("Freenet {version} - {status_text}")))
                    .ok();

                // Toggle Start/Stop/Restart enabled state based on running status
                let is_running = matches!(status, WrapperStatus::Running | WrapperStatus::UpToDate);
                stop_item.set_enabled(is_running);
                start_item.set_enabled(!is_running);
                restart_item.set_enabled(is_running);

                // Exit the tray loop so the wrapper can re-exec with the new binary
                if matches!(status, WrapperStatus::UpdatedRestarting) {
                    // Brief pause so the user can see the "Updated!" message
                    std::thread::sleep(std::time::Duration::from_secs(1));
                    action_tx.send(TrayAction::Quit).ok();
                    break;
                }
            }

            // Yield to avoid busy-spinning between iterations.
            std::thread::sleep(std::time::Duration::from_millis(100));
        }
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
