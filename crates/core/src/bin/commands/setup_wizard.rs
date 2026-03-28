//! Windows setup wizard for first-run installation.
//!
//! When `freenet.exe` is run with no arguments and is NOT in the canonical
//! install location (`%LOCALAPPDATA%\Freenet\bin\`), this module shows a
//! native Windows dialog offering to install Freenet or run without installing.
//!
//! On non-Windows platforms, `maybe_show_setup_wizard()` is a no-op.

#[cfg(target_os = "windows")]
pub mod detection;
#[cfg(target_os = "windows")]
pub mod installer;
#[cfg(target_os = "windows")]
pub mod ui;

// Re-export stubs on non-Windows so the entry point compiles everywhere.
#[cfg(not(target_os = "windows"))]
pub mod ui {
    #[derive(Debug, Clone, PartialEq)]
    #[allow(dead_code)]
    pub enum SetupResult {
        Installed,
        RunWithout,
        Cancelled,
    }
}

/// Check if Freenet needs to be installed and show the setup wizard if so.
///
/// Returns `Ok(true)` if the caller should exit (wizard shown, or service started).
/// Returns `Ok(false)` if the caller should proceed with normal startup
/// (non-Windows, or user chose "Run without installing").
///
/// On Windows, when Freenet is already installed, this starts the background
/// service (with tray icon) instead of returning false — users expect that
/// running `freenet.exe` with no args launches the tray app, not console mode.
pub fn maybe_show_setup_wizard() -> anyhow::Result<bool> {
    #[cfg(target_os = "windows")]
    {
        if detection::is_installed() {
            // Already installed: start the service (tray icon + background node)
            // rather than dropping into raw console mode.
            return start_installed_service();
        }

        // Show the setup dialog. The dialog creates its own window, so the
        // console from Explorer may briefly flash. We detach it AFTER the user
        // makes their choice so that "Run without installing" preserves console
        // output for CLI users who happen to run from a non-install location.
        let result = ui::show_setup_dialog()?;

        match result {
            ui::SetupResult::Installed => Ok(true),
            ui::SetupResult::RunWithout => Ok(false), // Caller runs node with console intact
            ui::SetupResult::Cancelled => Ok(true),
        }
    }

    #[cfg(not(target_os = "windows"))]
    {
        Ok(false)
    }
}

/// Start the background service on an already-installed Windows system.
///
/// Spawns `freenet service start` (which launches the run-wrapper with tray icon
/// as a detached process) and exits. If starting fails, falls back to console mode
/// so the user isn't left with nothing.
#[cfg(target_os = "windows")]
fn start_installed_service() -> anyhow::Result<bool> {
    let exe = std::env::current_exe().unwrap_or_default();

    let status = std::process::Command::new(&exe)
        .args(["service", "start"])
        .status();

    match status {
        Ok(s) if s.success() => Ok(true), // Service started, caller should exit
        _ => Ok(false),                   // Failed — fall back to console mode
    }
}
