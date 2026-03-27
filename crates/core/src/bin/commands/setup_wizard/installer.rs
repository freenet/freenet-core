//! Background installation logic for the Freenet setup wizard.
//!
//! Runs on a background thread and reports progress via a callback.
//! Each step is independent — failures in optional steps (like downloading
//! fdev) don't block the installation.

use anyhow::{Context, Result};
use std::path::{Path, PathBuf};

use super::detection::get_install_dir;

/// Progress updates sent from the installer thread to the UI.
#[derive(Debug, Clone)]
pub enum InstallProgress {
    StoppingExisting,
    CopyingBinary,
    DownloadingFdev,
    FdevSkipped(String),
    AddingToPath,
    InstallingService,
    LaunchingService,
    OpeningDashboard,
    Complete,
    Error(String),
}

/// Dashboard URL served by the local freenet node.
const DASHBOARD_URL: &str = "http://127.0.0.1:7509/";

/// Run the full installation, reporting progress via the callback.
///
/// Steps:
/// 1. Stop any running Freenet service
/// 2. Copy freenet.exe to install directory
/// 3. Download fdev.exe (optional, non-blocking on failure)
/// 4. Add install directory to user PATH
/// 5. Install service (Task Scheduler)
/// 6. Start service (launches run-wrapper + tray icon)
/// 7. Open dashboard in browser
pub fn run_install(progress: impl Fn(InstallProgress) + Send) -> Result<()> {
    let install_dir =
        get_install_dir().context("Could not determine install directory (%LOCALAPPDATA%)")?;

    let current_exe =
        std::env::current_exe().context("Could not determine current executable path")?;

    let installed_exe = install_dir.join("freenet.exe");

    // Step 1: Stop existing service if running
    progress(InstallProgress::StoppingExisting);
    if installed_exe.exists() {
        // Try to stop gracefully — ignore errors (may not be running)
        drop(
            std::process::Command::new(&installed_exe)
                .args(["service", "stop"])
                .stdout(std::process::Stdio::null())
                .stderr(std::process::Stdio::null())
                .status(),
        );
        std::thread::sleep(std::time::Duration::from_secs(2));
    }

    // Step 2: Copy binary to install directory
    progress(InstallProgress::CopyingBinary);
    std::fs::create_dir_all(&install_dir)
        .with_context(|| format!("Failed to create directory: {}", install_dir.display()))?;
    std::fs::copy(&current_exe, &installed_exe)
        .with_context(|| format!("Failed to copy binary to {}", installed_exe.display()))?;

    // Step 3: Download fdev.exe (optional)
    progress(InstallProgress::DownloadingFdev);
    match download_fdev(&install_dir) {
        Ok(()) => {}
        Err(e) => {
            progress(InstallProgress::FdevSkipped(format!("{e:#}")));
        }
    }

    // Step 4: Add to user PATH
    progress(InstallProgress::AddingToPath);
    if let Err(e) = add_to_user_path(&install_dir) {
        // Non-fatal — warn but continue
        eprintln!("Warning: could not add to PATH: {e:#}");
    }

    // Step 5: Install service
    progress(InstallProgress::InstallingService);
    let status = std::process::Command::new(&installed_exe)
        .args(["service", "install"])
        .status()
        .context("Failed to run freenet service install")?;
    if !status.success() {
        return Err(anyhow::anyhow!(
            "Service installation failed (exit code {}). You may need to run as Administrator.",
            status.code().unwrap_or(-1)
        ));
    }

    // Step 6: Start service
    progress(InstallProgress::LaunchingService);
    let status = std::process::Command::new(&installed_exe)
        .args(["service", "start"])
        .status()
        .context("Failed to start freenet service")?;
    if !status.success() {
        eprintln!(
            "Warning: service start returned exit code {}",
            status.code().unwrap_or(-1)
        );
    }

    // Step 7: Open dashboard
    progress(InstallProgress::OpeningDashboard);
    drop(
        std::process::Command::new("cmd")
            .args(["/c", "start", DASHBOARD_URL])
            .spawn(),
    );

    progress(InstallProgress::Complete);
    Ok(())
}

/// Download fdev.exe from the GitHub release matching the current version.
fn download_fdev(install_dir: &Path) -> Result<()> {
    let version = env!("CARGO_PKG_VERSION");
    let url = format!(
        "https://github.com/freenet/freenet-core/releases/download/v{version}/fdev-x86_64-pc-windows-msvc.zip"
    );

    let zip_path = install_dir.join("fdev-download.zip");

    // Use Win32 URLDownloadToFile for zero additional dependencies
    download_url_to_file(&url, &zip_path)?;

    // Extract fdev.exe from the zip
    let zip_file = std::fs::File::open(&zip_path).context("Failed to open downloaded fdev zip")?;
    let mut archive = zip::ZipArchive::new(zip_file).context("Failed to read fdev zip")?;

    for i in 0..archive.len() {
        let mut file = archive.by_index(i)?;
        let name = file.name().to_string();
        if name.ends_with("fdev.exe") {
            let dest = install_dir.join("fdev.exe");
            let mut out = std::fs::File::create(&dest).context("Failed to create fdev.exe")?;
            std::io::copy(&mut file, &mut out)?;
            break;
        }
    }

    // Clean up zip
    drop(std::fs::remove_file(&zip_path));

    Ok(())
}

/// Download a URL to a file using Win32 URLDownloadToFileW.
#[cfg(target_os = "windows")]
fn download_url_to_file(url: &str, dest: &Path) -> Result<()> {
    use std::ffi::OsStr;
    use std::os::windows::ffi::OsStrExt;

    let url_wide: Vec<u16> = OsStr::new(url).encode_wide().chain(Some(0)).collect();
    let dest_wide: Vec<u16> = dest.as_os_str().encode_wide().chain(Some(0)).collect();

    // SAFETY: FFI call to urlmon.dll. Both strings are null-terminated wide strings.
    let hr = unsafe {
        winapi::um::urlmon::URLDownloadToFileW(
            std::ptr::null_mut(),
            url_wide.as_ptr(),
            dest_wide.as_ptr(),
            0,
            std::ptr::null_mut(),
        )
    };

    if hr != 0 {
        anyhow::bail!("URLDownloadToFileW failed with HRESULT 0x{hr:08x}");
    }

    Ok(())
}

#[cfg(not(target_os = "windows"))]
fn download_url_to_file(_url: &str, _dest: &Path) -> Result<()> {
    anyhow::bail!("URLDownloadToFile is only available on Windows")
}

/// Add a directory to the user's PATH environment variable via the registry.
#[cfg(target_os = "windows")]
fn add_to_user_path(dir: &Path) -> Result<()> {
    use winreg::enums::*;
    use winreg::RegKey;

    let hkcu = RegKey::predef(HKEY_CURRENT_USER);
    let env = hkcu.open_subkey_with_flags("Environment", KEY_READ | KEY_WRITE)?;

    let current_path: String = env.get_value("Path").unwrap_or_default();
    let dir_str = dir.to_string_lossy();

    // Check if already in PATH (case-insensitive)
    if current_path
        .to_lowercase()
        .contains(&dir_str.to_lowercase())
    {
        return Ok(());
    }

    // Append to PATH
    let new_path = if current_path.is_empty() {
        dir_str.to_string()
    } else {
        format!("{current_path};{dir_str}")
    };

    env.set_value("Path", &new_path)?;

    // Broadcast WM_SETTINGCHANGE so running Explorer picks up the change
    broadcast_environment_change();

    Ok(())
}

#[cfg(not(target_os = "windows"))]
fn add_to_user_path(_dir: &Path) -> Result<()> {
    Ok(()) // No-op on non-Windows
}

/// Broadcast WM_SETTINGCHANGE so other processes pick up the PATH change.
#[cfg(target_os = "windows")]
fn broadcast_environment_change() {
    use std::ffi::OsStr;
    use std::os::windows::ffi::OsStrExt;

    let environment: Vec<u16> = OsStr::new("Environment")
        .encode_wide()
        .chain(Some(0))
        .collect();

    unsafe {
        winapi::um::winuser::SendMessageTimeoutW(
            winapi::um::winuser::HWND_BROADCAST,
            winapi::um::winuser::WM_SETTINGCHANGE,
            0,
            environment.as_ptr() as isize,
            winapi::um::winuser::SMTO_ABORTIFHUNG,
            5000,
            std::ptr::null_mut(),
        );
    }
}
