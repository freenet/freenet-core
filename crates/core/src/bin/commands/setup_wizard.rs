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
/// Returns `Ok(true)` if the wizard was shown and the caller should exit
/// (either installation completed or user cancelled).
/// Returns `Ok(false)` if Freenet is already installed or the user chose
/// to run without installing — the caller should proceed with normal startup.
pub fn maybe_show_setup_wizard() -> anyhow::Result<bool> {
    #[cfg(target_os = "windows")]
    {
        if detection::is_installed() {
            return Ok(false);
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
