//! Full uninstallation of Freenet: service, data, and binaries.

use anyhow::{Context, Result};
use clap::Args;
use std::path::{Path, PathBuf};

#[derive(Args, Debug, Clone)]
pub struct UninstallCommand {
    /// Also remove all Freenet data, config, and logs
    #[arg(long, conflicts_with = "keep_data")]
    pub purge: bool,
    /// Only remove service and binaries (skip interactive prompt about data)
    #[arg(long, conflicts_with = "purge")]
    pub keep_data: bool,
    /// Operate on the system-wide service instead of user service (Linux only)
    #[arg(long)]
    pub system: bool,
}

impl UninstallCommand {
    pub fn run(&self) -> Result<()> {
        // Step 1: Uninstall the service if installed (reuses service.rs logic)
        let service_removed = super::service::stop_and_remove_service(self.system)?;
        if service_removed {
            println!("Freenet service removed.");
        }

        // Step 2: Determine whether to purge data
        let do_purge = super::service::should_purge(self.purge, self.keep_data)?;
        if do_purge {
            super::service::purge_data(self.system)?;
            println!("All Freenet data, config, and logs removed.");
        }

        // Step 3: Remove binaries from every known install location.
        // Users often have multiple installations at once (e.g. `curl | sh` put
        // the binary in ~/.local/bin and they later ran `cargo install freenet`
        // which also dropped one in ~/.cargo/bin). Visiting only the directory
        // of the running executable leaves stale copies behind.
        let install_dirs = get_install_dirs(None);
        let mut removed_any = false;
        for dir in &install_dirs {
            if !dir.exists() {
                continue;
            }
            let removed = remove_binaries(dir)?;
            if !removed.is_empty() {
                removed_any = true;
            }
        }

        // On Windows the PowerShell installer creates %LOCALAPPDATA%\Freenet\bin
        // just for our binaries, so if it's now empty we can collapse the
        // enclosing %LOCALAPPDATA%\Freenet\ folder as well. Other platforms
        // drop binaries into shared dirs (~/.local/bin, ~/.cargo/bin) whose
        // parents belong to unrelated tools — don't touch those.
        #[cfg(target_os = "windows")]
        if let Ok(local_app_data) = std::env::var("LOCALAPPDATA") {
            let bin_dir = PathBuf::from(local_app_data).join("Freenet").join("bin");
            super::service::remove_dir_if_empty_pub(&bin_dir);
            if let Some(parent) = bin_dir.parent() {
                super::service::remove_dir_if_empty_pub(parent);
            }
        }

        // Summary
        if !service_removed && !removed_any && !do_purge {
            println!("Freenet does not appear to be installed.");
        } else {
            println!();
            println!("Freenet has been completely uninstalled.");
        }

        Ok(())
    }
}

/// Remove Freenet binaries from the given install dir. Returns the list of
/// files that were removed.
fn remove_binaries(install_dir: &Path) -> Result<Vec<PathBuf>> {
    let mut removed = Vec::new();

    for name in binary_names() {
        let path = install_dir.join(name);
        if path.exists() {
            // On Windows, a running executable cannot delete itself. Skip with a hint.
            #[cfg(target_os = "windows")]
            if is_current_exe(&path) {
                println!(
                    "Cannot remove running binary: {}\n\
                     Please delete it manually after this command exits.",
                    path.display()
                );
                continue;
            }

            std::fs::remove_file(&path)
                .with_context(|| format!("Failed to remove {}", path.display()))?;
            println!("Removed {}", path.display());
            removed.push(path);
        }
    }

    // Also remove the macOS wrapper script if present
    let wrapper = install_dir.join("freenet-service-wrapper.sh");
    if wrapper.exists() {
        std::fs::remove_file(&wrapper)
            .with_context(|| format!("Failed to remove {}", wrapper.display()))?;
        println!("Removed {}", wrapper.display());
        removed.push(wrapper);
    }

    Ok(removed)
}

/// Check if the given path is the currently running executable.
#[cfg(target_os = "windows")]
fn is_current_exe(path: &Path) -> bool {
    std::env::current_exe()
        .ok()
        .and_then(|exe| std::fs::canonicalize(&exe).ok())
        .zip(std::fs::canonicalize(path).ok())
        .is_some_and(|(a, b)| a == b)
}

/// Get every directory that Freenet binaries may have been installed into,
/// in no particular order (duplicates are removed).
///
/// If `override_dir` or `FREENET_INSTALL_DIR` is set, only that location is
/// returned — the caller has explicitly told us where to look. Otherwise we
/// return the union of the install.sh default (`~/.local/bin`), the Cargo
/// default (`~/.cargo/bin`), the Windows PowerShell installer default
/// (`%LOCALAPPDATA%\Freenet\bin`), and the parent directory of the currently
/// running executable.
fn get_install_dirs(override_dir: Option<&Path>) -> Vec<PathBuf> {
    if let Some(dir) = override_dir {
        return vec![dir.to_path_buf()];
    }

    if let Ok(dir) = std::env::var("FREENET_INSTALL_DIR") {
        return vec![PathBuf::from(dir)];
    }

    let mut dirs: Vec<PathBuf> = Vec::new();

    if let Ok(exe) = std::env::current_exe() {
        if let Some(parent) = exe.parent() {
            dirs.push(parent.to_path_buf());
        }
    }

    if let Some(home) = dirs::home_dir() {
        dirs.push(home.join(".local/bin")); // install.sh default
        dirs.push(home.join(".cargo/bin")); // `cargo install freenet` default
    }

    #[cfg(target_os = "windows")]
    if let Ok(local_app_data) = std::env::var("LOCALAPPDATA") {
        // install.ps1 default — not normally on PATH, so exe parent detection
        // misses it when the user ran a different `freenet.exe` (e.g. from a
        // crate install).
        dirs.push(PathBuf::from(local_app_data).join("Freenet").join("bin"));
    }

    dirs.sort();
    dirs.dedup();
    dirs
}

/// Get the names of binaries to remove.
fn binary_names() -> &'static [&'static str] {
    #[cfg(target_os = "windows")]
    {
        &["freenet.exe", "fdev.exe"]
    }
    #[cfg(not(target_os = "windows"))]
    {
        &["freenet", "fdev"]
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_get_install_dirs_with_override() {
        let dirs = get_install_dirs(Some(Path::new("/tmp/test-freenet")));
        assert_eq!(dirs, vec![PathBuf::from("/tmp/test-freenet")]);
    }

    #[test]
    fn test_get_install_dirs_without_override_includes_defaults() {
        // Ensure the env var isn't leaking from another test.
        // Safe: tests in this module run single-threaded via cargo test.
        unsafe {
            std::env::remove_var("FREENET_INSTALL_DIR");
        }

        let dirs = get_install_dirs(None);
        assert!(!dirs.is_empty(), "should always return at least exe parent");

        // When $HOME is set, both user-local install dirs should appear.
        if let Some(home) = dirs::home_dir() {
            assert!(
                dirs.contains(&home.join(".local/bin")),
                "expected ~/.local/bin (install.sh default) in {dirs:?}",
            );
            assert!(
                dirs.contains(&home.join(".cargo/bin")),
                "expected ~/.cargo/bin (cargo install default) in {dirs:?}",
            );
        }
    }

    #[test]
    fn test_get_install_dirs_env_var_takes_precedence() {
        // Safe: tests in this module run single-threaded via cargo test.
        unsafe {
            std::env::set_var("FREENET_INSTALL_DIR", "/custom/path");
        }
        let dirs = get_install_dirs(None);
        unsafe {
            std::env::remove_var("FREENET_INSTALL_DIR");
        }
        assert_eq!(dirs, vec![PathBuf::from("/custom/path")]);
    }

    #[test]
    fn test_get_install_dirs_deduplicates() {
        let dirs = get_install_dirs(None);
        let mut sorted = dirs.clone();
        sorted.sort();
        sorted.dedup();
        assert_eq!(dirs.len(), sorted.len(), "expected no duplicate entries");
    }

    #[test]
    fn test_binary_names() {
        let names = binary_names();
        assert!(names.len() >= 2);
        #[cfg(not(target_os = "windows"))]
        {
            assert!(names.contains(&"freenet"));
            assert!(names.contains(&"fdev"));
        }
    }

    #[test]
    fn test_remove_binaries_in_empty_dir() {
        let tmp = tempfile::tempdir().unwrap();
        let removed = remove_binaries(tmp.path()).unwrap();
        assert!(removed.is_empty());
    }

    #[test]
    fn test_remove_binaries_removes_files() {
        let tmp = tempfile::tempdir().unwrap();

        // Create fake binaries
        std::fs::write(tmp.path().join("freenet"), b"fake").unwrap();
        std::fs::write(tmp.path().join("fdev"), b"fake").unwrap();
        std::fs::write(tmp.path().join("freenet-service-wrapper.sh"), b"fake").unwrap();

        let removed = remove_binaries(tmp.path()).unwrap();
        #[cfg(not(target_os = "windows"))]
        assert_eq!(removed.len(), 3);

        // Verify files are gone
        assert!(!tmp.path().join("freenet").exists());
        assert!(!tmp.path().join("fdev").exists());
        assert!(!tmp.path().join("freenet-service-wrapper.sh").exists());
    }

    #[test]
    fn test_remove_binaries_handles_partial_install() {
        // A directory that contains freenet but not fdev (or vice versa)
        // must not error — we're scanning multiple locations and may find
        // only a subset of the binaries in any one of them.
        let tmp = tempfile::tempdir().unwrap();
        #[cfg(not(target_os = "windows"))]
        std::fs::write(tmp.path().join("freenet"), b"fake").unwrap();
        #[cfg(target_os = "windows")]
        std::fs::write(tmp.path().join("freenet.exe"), b"fake").unwrap();

        let removed = remove_binaries(tmp.path()).unwrap();
        assert_eq!(removed.len(), 1);
    }
}
