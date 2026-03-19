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

        // Step 3: Remove binaries
        let install_dir = get_install_dir(None);
        let removed = remove_binaries(&install_dir)?;

        // Summary
        if !service_removed && removed.is_empty() && !do_purge {
            println!("Freenet does not appear to be installed.");
        } else {
            println!();
            println!("Freenet has been completely uninstalled.");
        }

        Ok(())
    }
}

/// Remove Freenet binaries. Returns the list of files that were removed.
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

    if removed.is_empty() {
        println!("No Freenet binaries found in {}", install_dir.display());
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

/// Get the directory where Freenet binaries are installed.
///
/// Priority: `override_dir` > `FREENET_INSTALL_DIR` env > current exe parent > `~/.local/bin`
fn get_install_dir(override_dir: Option<&Path>) -> PathBuf {
    if let Some(dir) = override_dir {
        return dir.to_path_buf();
    }

    if let Ok(dir) = std::env::var("FREENET_INSTALL_DIR") {
        return PathBuf::from(dir);
    }

    // Detect from current executable location
    if let Ok(exe) = std::env::current_exe() {
        if let Some(parent) = exe.parent() {
            return parent.to_path_buf();
        }
    }

    // Fallback to default install location
    dirs::home_dir()
        .map(|h| h.join(".local/bin"))
        .unwrap_or_else(|| PathBuf::from("/usr/local/bin"))
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
    fn test_get_install_dir_with_override() {
        let dir = get_install_dir(Some(Path::new("/tmp/test-freenet")));
        assert_eq!(dir, PathBuf::from("/tmp/test-freenet"));
    }

    #[test]
    fn test_get_install_dir_without_override() {
        // Without override, should return a non-empty path (env var or exe parent)
        let dir = get_install_dir(None);
        assert!(!dir.as_os_str().is_empty());
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
}
