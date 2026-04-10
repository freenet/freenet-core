pub mod auto_update;
pub mod prompt;
pub mod report;
pub mod service;
pub mod setup_wizard;
pub mod tray;
pub mod uninstall;
pub mod update;

/// Open a URL in the default browser (platform-specific).
///
/// On Windows, uses `ShellExecuteW` instead of `cmd /c start` because the
/// wrapper/tray detach from the console via `FreeConsole()` at startup.
/// After that, `cmd.exe` has no console context and fails silently.
#[cfg(any(target_os = "windows", target_os = "macos"))]
pub(crate) fn open_url_in_browser(url: &str) {
    #[cfg(target_os = "windows")]
    {
        use std::ffi::OsStr;
        use std::os::windows::ffi::OsStrExt;

        let operation: Vec<u16> = OsStr::new("open").encode_wide().chain(Some(0)).collect();
        let url_wide: Vec<u16> = OsStr::new(url).encode_wide().chain(Some(0)).collect();

        unsafe {
            winapi::um::shellapi::ShellExecuteW(
                std::ptr::null_mut(),
                operation.as_ptr(),
                url_wide.as_ptr(),
                std::ptr::null(),
                std::ptr::null(),
                winapi::um::winuser::SW_SHOWNORMAL,
            );
        }
    }
    #[cfg(target_os = "macos")]
    drop(std::process::Command::new("open").arg(url).spawn());
}
