//! Full uninstallation of Freenet: service, data, and binaries.

use anyhow::{Context, Result};
use clap::Args;
use std::io::{self, BufRead, IsTerminal, Write};
use std::path::PathBuf;

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
        // Step 1: Uninstall the service if installed
        let service_was_installed = uninstall_service_if_present(self.system)?;

        // Step 2: Determine whether to purge data
        let do_purge = should_purge(self.purge, self.keep_data)?;
        if do_purge {
            // Reuse the purge logic from service.rs
            super::service::purge_data(self.system)?;
            println!("All Freenet data, config, and logs removed.");
        }

        // Step 3: Remove binaries
        let removed = remove_binaries()?;

        // Summary
        if !service_was_installed && removed.is_empty() && !do_purge {
            println!("Freenet does not appear to be installed.");
        } else {
            println!();
            println!("Freenet has been completely uninstalled.");
        }

        Ok(())
    }
}

/// Determine whether the user wants to purge data directories.
fn should_purge(purge: bool, keep_data: bool) -> Result<bool> {
    if purge {
        return Ok(true);
    }
    if keep_data {
        return Ok(false);
    }

    if io::stdin().is_terminal() {
        print!("Also remove all Freenet data, config, and logs? [y/N] ");
        io::stdout().flush()?;
        let mut line = String::new();
        io::stdin().lock().read_line(&mut line)?;
        let answer = line.trim().to_ascii_lowercase();
        Ok(answer == "y" || answer == "yes")
    } else {
        println!("Non-interactive mode: keeping data. Use --purge to also remove data.");
        Ok(false)
    }
}

/// Try to uninstall the service if one is installed. Returns true if a service was found.
fn uninstall_service_if_present(system: bool) -> Result<bool> {
    if is_service_installed(system) {
        println!("Stopping and removing Freenet service...");
        do_uninstall_service(system)?;
        println!("Freenet service removed.");
        Ok(true)
    } else {
        Ok(false)
    }
}

/// Check if a Freenet service is currently installed.
fn is_service_installed(#[allow(unused_variables)] system: bool) -> bool {
    #[cfg(target_os = "linux")]
    {
        if system {
            std::path::Path::new("/etc/systemd/system/freenet.service").exists()
        } else {
            dirs::home_dir()
                .map(|h| h.join(".config/systemd/user/freenet.service").exists())
                .unwrap_or(false)
                || std::path::Path::new("/etc/systemd/system/freenet.service").exists()
        }
    }

    #[cfg(target_os = "macos")]
    {
        dirs::home_dir()
            .map(|h| {
                h.join("Library/LaunchAgents/org.freenet.node.plist")
                    .exists()
            })
            .unwrap_or(false)
    }

    #[cfg(target_os = "windows")]
    {
        // Check if scheduled task exists
        std::process::Command::new("schtasks")
            .args(["/query", "/tn", "Freenet"])
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null())
            .status()
            .map(|s| s.success())
            .unwrap_or(false)
    }

    #[cfg(not(any(target_os = "linux", target_os = "macos", target_os = "windows")))]
    {
        false
    }
}

/// Uninstall the service (stop, disable, remove service file).
fn do_uninstall_service(#[allow(unused_variables)] system: bool) -> Result<()> {
    #[cfg(target_os = "linux")]
    {
        // Auto-detect system vs user mode
        let system_mode = if system {
            true
        } else {
            // If system service exists but user service doesn't, use system mode
            std::path::Path::new("/etc/systemd/system/freenet.service").exists()
                && !dirs::home_dir()
                    .map(|h| h.join(".config/systemd/user/freenet.service").exists())
                    .unwrap_or(false)
        };

        let systemctl = |args: &[&str]| -> Result<()> {
            let mut cmd = std::process::Command::new("systemctl");
            if !system_mode {
                cmd.arg("--user");
            }
            cmd.args(args);
            // Best-effort: service may already be stopped/disabled
            drop(cmd.status());
            Ok(())
        };

        systemctl(&["stop", "freenet"])?;
        systemctl(&["disable", "freenet"])?;

        let service_path = if system_mode {
            PathBuf::from("/etc/systemd/system/freenet.service")
        } else {
            dirs::home_dir()
                .context("Failed to get home directory")?
                .join(".config/systemd/user/freenet.service")
        };

        if service_path.exists() {
            std::fs::remove_file(&service_path).context("Failed to remove service file")?;
        }

        // Reload daemon
        systemctl(&["daemon-reload"])?;
    }

    #[cfg(target_os = "macos")]
    {
        if let Some(home) = dirs::home_dir() {
            let plist_path = home.join("Library/LaunchAgents/org.freenet.node.plist");
            if let Some(path_str) = plist_path.to_str() {
                let _ = std::process::Command::new("launchctl")
                    .args(["unload", path_str])
                    .status();
            }
            if plist_path.exists() {
                std::fs::remove_file(&plist_path).context("Failed to remove plist file")?;
            }
        }
    }

    #[cfg(target_os = "windows")]
    {
        let _ = std::process::Command::new("schtasks")
            .args(["/end", "/tn", "Freenet"])
            .status();
        let _ = std::process::Command::new("schtasks")
            .args(["/delete", "/tn", "Freenet", "/f"])
            .status();
    }

    Ok(())
}

/// Remove Freenet binaries. Returns the list of files that were removed.
fn remove_binaries() -> Result<Vec<PathBuf>> {
    let install_dir = get_install_dir();
    let mut removed = Vec::new();

    let binaries = get_binary_names();

    for name in &binaries {
        let path = install_dir.join(name);
        if path.exists() {
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

/// Get the directory where Freenet binaries are installed.
fn get_install_dir() -> PathBuf {
    // The install script uses FREENET_INSTALL_DIR or ~/.local/bin
    if let Ok(dir) = std::env::var("FREENET_INSTALL_DIR") {
        return PathBuf::from(dir);
    }

    // Try to detect from current executable location
    if let Ok(exe) = std::env::current_exe() {
        if let Some(parent) = exe.parent() {
            return parent.to_path_buf();
        }
    }

    // Fallback to default
    dirs::home_dir()
        .map(|h| h.join(".local/bin"))
        .unwrap_or_else(|| PathBuf::from("/usr/local/bin"))
}

/// Get the names of binaries to remove.
fn get_binary_names() -> Vec<&'static str> {
    #[cfg(target_os = "windows")]
    {
        vec!["freenet.exe", "fdev.exe"]
    }
    #[cfg(not(target_os = "windows"))]
    {
        vec!["freenet", "fdev"]
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_should_purge_with_purge_flag() {
        assert!(should_purge(true, false).unwrap());
    }

    #[test]
    fn test_should_purge_with_keep_data_flag() {
        assert!(!should_purge(false, true).unwrap());
    }

    #[test]
    fn test_should_purge_no_flags_non_tty() {
        // In CI/test environments stdin is not a TTY, so this should default to false
        assert!(!should_purge(false, false).unwrap());
    }

    #[test]
    fn test_get_binary_names() {
        let names = get_binary_names();
        assert!(names.len() >= 2);
        #[cfg(not(target_os = "windows"))]
        {
            assert!(names.contains(&"freenet"));
            assert!(names.contains(&"fdev"));
        }
        #[cfg(target_os = "windows")]
        {
            assert!(names.contains(&"freenet.exe"));
            assert!(names.contains(&"fdev.exe"));
        }
    }

    #[test]
    fn test_get_install_dir_with_env_override() {
        // Save and restore env var
        let original = std::env::var("FREENET_INSTALL_DIR").ok();
        std::env::set_var("FREENET_INSTALL_DIR", "/tmp/test-freenet-install");
        let dir = get_install_dir();
        assert_eq!(dir, PathBuf::from("/tmp/test-freenet-install"));
        match original {
            Some(val) => std::env::set_var("FREENET_INSTALL_DIR", val),
            None => std::env::remove_var("FREENET_INSTALL_DIR"),
        }
    }

    #[test]
    fn test_remove_binaries_in_empty_dir() {
        let tmp = tempfile::tempdir().unwrap();
        let original = std::env::var("FREENET_INSTALL_DIR").ok();
        std::env::set_var("FREENET_INSTALL_DIR", tmp.path().to_str().unwrap());

        let removed = remove_binaries().unwrap();
        assert!(removed.is_empty());

        match original {
            Some(val) => std::env::set_var("FREENET_INSTALL_DIR", val),
            None => std::env::remove_var("FREENET_INSTALL_DIR"),
        }
    }

    #[test]
    fn test_remove_binaries_removes_files() {
        let tmp = tempfile::tempdir().unwrap();
        let original = std::env::var("FREENET_INSTALL_DIR").ok();
        std::env::set_var("FREENET_INSTALL_DIR", tmp.path().to_str().unwrap());

        // Create fake binaries
        std::fs::write(tmp.path().join("freenet"), b"fake").unwrap();
        std::fs::write(tmp.path().join("fdev"), b"fake").unwrap();
        std::fs::write(tmp.path().join("freenet-service-wrapper.sh"), b"fake").unwrap();

        let removed = remove_binaries().unwrap();
        #[cfg(not(target_os = "windows"))]
        assert_eq!(removed.len(), 3);

        // Verify files are gone
        assert!(!tmp.path().join("freenet").exists());
        assert!(!tmp.path().join("fdev").exists());
        assert!(!tmp.path().join("freenet-service-wrapper.sh").exists());

        match original {
            Some(val) => std::env::set_var("FREENET_INSTALL_DIR", val),
            None => std::env::remove_var("FREENET_INSTALL_DIR"),
        }
    }
}
