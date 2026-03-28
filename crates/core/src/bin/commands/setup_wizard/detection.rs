//! Detects whether Freenet is installed in the canonical location.
//!
//! The wizard should only appear when the user runs `freenet.exe` from
//! a non-install location (e.g., Downloads). If the binary is already
//! in `%LOCALAPPDATA%\Freenet\bin\` or a service is registered, skip
//! the wizard and run the node normally.

/// Check if the current executable is installed.
///
/// Returns `true` if either:
/// 1. The binary is running from `%LOCALAPPDATA%\Freenet\bin\`, OR
/// 2. Freenet autostart is registered (registry Run key or legacy scheduled task)
pub fn is_installed() -> bool {
    is_in_install_dir() || is_service_registered()
}

/// Check if the current exe is in the canonical install directory.
fn is_in_install_dir() -> bool {
    let current_exe = match std::env::current_exe() {
        Ok(p) => p,
        Err(_) => return false,
    };

    let install_dir = match get_install_dir() {
        Some(d) => d,
        None => return false,
    };

    // Get the parent directory of the current exe
    let exe_dir = match current_exe.parent() {
        Some(d) => d,
        None => return false,
    };

    // Windows paths are case-insensitive
    paths_equivalent(exe_dir, &install_dir)
}

/// Check if Freenet autostart is registered (registry Run key or legacy scheduled task).
fn is_service_registered() -> bool {
    // Check registry Run key (new mechanism)
    let has_registry = winreg::RegKey::predef(winreg::enums::HKEY_CURRENT_USER)
        .open_subkey(r"Software\Microsoft\Windows\CurrentVersion\Run")
        .ok()
        .and_then(|k| k.get_value::<String, _>("Freenet").ok())
        .is_some();
    if has_registry {
        return true;
    }

    // Fallback: check legacy scheduled task
    std::process::Command::new("schtasks")
        .args(["/query", "/tn", "Freenet"])
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .status()
        .map(|s| s.success())
        .unwrap_or(false)
}

/// Get the canonical install directory: `%LOCALAPPDATA%\Freenet\bin\`
pub fn get_install_dir() -> Option<std::path::PathBuf> {
    dirs::data_local_dir().map(|d| d.join("Freenet").join("bin"))
}

/// Compare two paths case-insensitively (Windows convention).
/// Attempts to canonicalize both paths first to resolve symlinks and `..\`.
fn paths_equivalent(a: &std::path::Path, b: &std::path::Path) -> bool {
    let a_canon = a.canonicalize().unwrap_or_else(|_| a.to_path_buf());
    let b_canon = b.canonicalize().unwrap_or_else(|_| b.to_path_buf());

    let a_str = a_canon.to_string_lossy().to_lowercase();
    let b_str = b_canon.to_string_lossy().to_lowercase();

    // Normalize trailing separators
    let a_trimmed = a_str.trim_end_matches(['/', '\\']);
    let b_trimmed = b_str.trim_end_matches(['/', '\\']);

    a_trimmed == b_trimmed
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_paths_equivalent_same_path() {
        let a = std::path::Path::new("/tmp/test");
        assert!(paths_equivalent(a, a));
    }

    #[test]
    fn test_paths_equivalent_trailing_separator() {
        let a = std::path::Path::new("/tmp/test");
        let b = std::path::Path::new("/tmp/test/");
        assert!(paths_equivalent(a, b));
    }

    #[test]
    fn test_paths_not_equivalent() {
        let a = std::path::Path::new("/tmp/test");
        let b = std::path::Path::new("/tmp/other");
        assert!(!paths_equivalent(a, b));
    }

    #[test]
    fn test_get_install_dir_returns_something() {
        // On any platform, dirs::data_local_dir() should return Some
        let dir = get_install_dir();
        assert!(dir.is_some());
        let dir = dir.unwrap();
        assert!(dir.to_string_lossy().contains("Freenet"));
    }
}
