//! Windows system tray icon for the Freenet run-wrapper.
//!
//! Displays a tray icon with a right-click menu while the Freenet node
//! is running. The menu mirrors `freenet service` CLI commands plus
//! extras like "Open Dashboard" and "View Logs".
//!
//! On non-Windows platforms, this module provides no-op stubs.

// Types and functions are only used on Windows; suppress warnings on other platforms.
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
}

// ── Windows implementation ──────────────────────────────────────────

#[cfg(target_os = "windows")]
mod platform {
    use super::*;
    use muda::{Menu, MenuEvent, MenuItem, PredefinedMenuItem};
    use std::sync::mpsc as std_mpsc;
    use tray_icon::{icon::Icon, TrayIcon, TrayIconBuilder};

    const DASHBOARD_URL: &str = super::service::DASHBOARD_URL;

    /// Build the tray icon from embedded 32x32 RGBA pixel data of the Freenet logo.
    /// Pre-rendered from the SVG logo at freenet/web with the blue gradient preserved.
    fn build_icon() -> Result<Icon, tray_icon::BadIcon> {
        let rgba = include_bytes!("assets/freenet_32x32.rgba").to_vec();
        Icon::from_rgba(rgba, 32, 32)
    }

    /// Run the tray icon event loop on the current thread (must be the main thread
    /// on Windows for the message pump to work).
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
        let restart_item = MenuItem::new("Restart", true, None);
        let check_update = MenuItem::new("Check for Updates", true, None);
        let view_logs = MenuItem::new("View Logs", true, None);
        let separator3 = PredefinedMenuItem::separator();
        let quit_item = MenuItem::new("Quit", true, None);

        let _ = menu.append(&open_dashboard);
        let _ = menu.append(&separator1);
        let _ = menu.append(&status_item);
        let _ = menu.append(&version_item);
        let _ = menu.append(&separator2);
        let _ = menu.append(&restart_item);
        let _ = menu.append(&check_update);
        let _ = menu.append(&view_logs);
        let _ = menu.append(&separator3);
        let _ = menu.append(&quit_item);

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
        let restart_id = restart_item.id().clone();
        let check_update_id = check_update.id().clone();
        let view_logs_id = view_logs.id().clone();
        let quit_id = quit_item.id().clone();

        loop {
            // Process menu events (non-blocking peek with timeout)
            if let Ok(event) = menu_rx.try_recv() {
                let action = if event.id == open_dashboard_id {
                    // Open dashboard directly — no need to go through the wrapper loop
                    let _ = std::process::Command::new("cmd")
                        .args(["/c", "start", DASHBOARD_URL])
                        .spawn();
                    None
                } else if event.id == restart_id {
                    Some(TrayAction::Restart)
                } else if event.id == check_update_id {
                    Some(TrayAction::CheckUpdate)
                } else if event.id == view_logs_id {
                    Some(TrayAction::ViewLogs)
                } else if event.id == quit_id {
                    let _ = action_tx.send(TrayAction::Quit);
                    break;
                } else {
                    None
                };

                if let Some(action) = action {
                    let _ = action_tx.send(action);
                }
            }

            // Check for status updates from the wrapper loop
            if let Ok(status) = status_rx.try_recv() {
                let status_text = match &status {
                    WrapperStatus::Running => "Running",
                    WrapperStatus::Updating => "Updating...",
                    WrapperStatus::Stopped => "Stopped",
                };
                let _ = status_item.set_text(format!("Status: {status_text}"));
                let _ = _tray.set_tooltip(Some(format!("Freenet {version} - {status_text}")));
            }

            // Yield to avoid busy-spinning. The Windows message pump is driven
            // by tray-icon internally; we just need to process events periodically.
            std::thread::sleep(std::time::Duration::from_millis(100));
        }
    }
}

// ── Non-Windows stub ────────────────────────────────────────────────

#[cfg(not(target_os = "windows"))]
mod platform {
    use super::*;
    use std::sync::mpsc as std_mpsc;

    /// No-op on non-Windows platforms. Returns immediately.
    pub fn run_tray_event_loop(
        _action_tx: std_mpsc::Sender<TrayAction>,
        _status_rx: std_mpsc::Receiver<WrapperStatus>,
        _version: &str,
    ) {
        // Tray icon is Windows-only. On other platforms the wrapper loop
        // runs directly on the main thread without a tray.
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

    // Find the latest log file using the same logic as service_logs
    let latest = super::service::find_latest_log_file(&log_dir, "freenet");

    match latest {
        Some(path) => {
            #[cfg(target_os = "windows")]
            {
                let _ = std::process::Command::new("notepad").arg(&path).spawn();
            }
            #[cfg(not(target_os = "windows"))]
            {
                drop(std::process::Command::new("xdg-open").arg(&path).spawn());
            }
        }
        None => {
            eprintln!("No log files found in {}", log_dir.display());
        }
    }
}
