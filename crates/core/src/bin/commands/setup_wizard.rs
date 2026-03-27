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

        // Detach from console before showing the GUI window.
        // This hides the brief console flash that would otherwise appear
        // when the user double-clicks freenet.exe from Explorer.
        //
        // Note: if the user chooses "Run without installing", the node
        // will run without console output since we already detached.
        // This is acceptable for double-click-from-Explorer scenarios
        // where there's no terminal to show output in anyway. CLI users
        // who run `freenet network` explicitly don't hit this path.
        unsafe {
            winapi::um::wincon::FreeConsole();
        }

        match ui::show_setup_dialog()? {
            ui::SetupResult::Installed => Ok(true),
            ui::SetupResult::RunWithout => Ok(false),
            ui::SetupResult::Cancelled => Ok(true),
        }
    }

    #[cfg(not(target_os = "windows"))]
    {
        Ok(false)
    }
}
