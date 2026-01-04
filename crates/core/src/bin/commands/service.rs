//! System service management for Freenet.
//!
//! Supports:
//! - Linux: systemd user service
//! - macOS: launchd user agent

use anyhow::{Context, Result};
use clap::Subcommand;
use std::path::Path;

use super::report::ReportCommand;

/// Find the latest log file in the given directory.
/// Handles both static files (e.g., "freenet.log" from systemd) and
/// rotated files (e.g., "freenet.2025-12-27.log" from tracing-appender).
/// Returns the most recently modified file.
fn find_latest_log_file(log_dir: &Path, base_name: &str) -> Option<std::path::PathBuf> {
    use std::fs;

    let mut candidates: Vec<(std::path::PathBuf, std::time::SystemTime)> = Vec::new();

    // Check for the static file (used by systemd StandardOutput)
    let static_file = log_dir.join(format!("{base_name}.log"));
    if static_file.exists() {
        if let Ok(metadata) = fs::metadata(&static_file) {
            if metadata.len() > 0 {
                if let Ok(modified) = metadata.modified() {
                    candidates.push((static_file, modified));
                }
            }
        }
    }

    // Look for rotated files (pattern: {base_name}.YYYY-MM-DD.log)
    if let Ok(entries) = fs::read_dir(log_dir) {
        for entry in entries.filter_map(|e| e.ok()) {
            let name = entry.file_name();
            let name_str = name.to_string_lossy();
            // Match pattern: {base_name}.YYYY-MM-DD.log
            if name_str.starts_with(&format!("{base_name}."))
                && name_str.ends_with(".log")
                && name_str.len() > format!("{base_name}..log").len()
            {
                if let Ok(metadata) = entry.metadata() {
                    if let Ok(modified) = metadata.modified() {
                        candidates.push((entry.path(), modified));
                    }
                }
            }
        }
    }

    // Return the most recently modified file
    candidates
        .into_iter()
        .max_by_key(|(_, modified)| *modified)
        .map(|(path, _)| path)
}

#[derive(Subcommand, Debug, Clone)]
pub enum ServiceCommand {
    /// Install Freenet as a system service
    Install,
    /// Uninstall the Freenet system service
    Uninstall,
    /// Check the status of the Freenet service
    Status,
    /// Start the Freenet service
    Start,
    /// Stop the Freenet service
    Stop,
    /// Restart the Freenet service
    Restart,
    /// View service logs (follows new output)
    Logs {
        /// Show only error logs
        #[arg(long)]
        err: bool,
    },
    /// Generate and upload a diagnostic report for debugging
    Report(ReportCommand),
}

impl ServiceCommand {
    pub fn run(
        &self,
        version: &str,
        git_commit: &str,
        git_dirty: &str,
        build_timestamp: &str,
    ) -> Result<()> {
        match self {
            ServiceCommand::Install => install_service(),
            ServiceCommand::Uninstall => uninstall_service(),
            ServiceCommand::Status => service_status(),
            ServiceCommand::Start => start_service(),
            ServiceCommand::Stop => stop_service(),
            ServiceCommand::Restart => restart_service(),
            ServiceCommand::Logs { err } => service_logs(*err),
            ServiceCommand::Report(cmd) => cmd.run(version, git_commit, git_dirty, build_timestamp),
        }
    }
}

#[cfg(target_os = "linux")]
fn install_service() -> Result<()> {
    use std::fs;

    let exe_path = std::env::current_exe().context("Failed to get current executable path")?;
    let home_dir = dirs::home_dir().context("Failed to get home directory")?;

    let service_dir = home_dir.join(".config/systemd/user");
    fs::create_dir_all(&service_dir).context("Failed to create systemd user directory")?;

    // Create log directory - use ~/.local/state/freenet for XDG compliance
    let log_dir = home_dir.join(".local/state/freenet");
    fs::create_dir_all(&log_dir).context("Failed to create log directory")?;

    let service_content = generate_service_file(&exe_path, &log_dir);
    let service_path = service_dir.join("freenet.service");

    fs::write(&service_path, service_content).context("Failed to write service file")?;

    // Reload systemd user daemon
    let status = std::process::Command::new("systemctl")
        .args(["--user", "daemon-reload"])
        .status()
        .context("Failed to reload systemd")?;

    if !status.success() {
        anyhow::bail!("Failed to reload systemd daemon");
    }

    // Enable the service
    let status = std::process::Command::new("systemctl")
        .args(["--user", "enable", "freenet"])
        .status()
        .context("Failed to enable service")?;

    if !status.success() {
        anyhow::bail!("Failed to enable service");
    }

    println!("Freenet service installed successfully.");
    println!();
    println!("To start the service now:");
    println!("  freenet service start");
    println!();
    println!("The service will start automatically on login.");
    println!("Logs will be written to: {}", log_dir.display());

    Ok(())
}

#[cfg(target_os = "linux")]
pub fn generate_service_file(binary_path: &Path, log_dir: &Path) -> String {
    format!(
        r#"[Unit]
Description=Freenet Node
Documentation=https://freenet.org
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
ExecStart={binary} network
Restart=always
# Wait 10 seconds before restart to avoid rapid restart loops
RestartSec=10
# Allow 15 seconds for graceful shutdown before SIGKILL
# The node handles SIGTERM to properly close peer connections
TimeoutStopSec=15

# Auto-update: if peer exits with code 42 (version mismatch with gateway),
# run update before systemd restarts the service. The '-' prefix means
# ExecStopPost failure won't affect service restart.
ExecStopPost=-/bin/sh -c '[ "$EXIT_STATUS" = "42" ] && {binary} update --quiet || true'

# Logging - write to files for systems without active user journald
# (headless servers, systems without lingering enabled, etc.)
StandardOutput=append:{log_dir}/freenet.log
StandardError=append:{log_dir}/freenet.error.log
SyslogIdentifier=freenet

# Resource limits to prevent runaway resource consumption
# File descriptors needed for network connections
LimitNOFILE=65536
# Memory limit (2GB soft limit for user service)
MemoryMax=2G
# CPU quota (200% = 2 cores max)
CPUQuota=200%

[Install]
WantedBy=default.target
"#,
        binary = binary_path.display(),
        log_dir = log_dir.display()
    )
}

#[cfg(target_os = "linux")]
fn uninstall_service() -> Result<()> {
    use std::fs;

    // Stop the service if running
    let _ = std::process::Command::new("systemctl")
        .args(["--user", "stop", "freenet"])
        .status();

    // Disable the service
    let _ = std::process::Command::new("systemctl")
        .args(["--user", "disable", "freenet"])
        .status();

    // Remove the service file
    let service_path = dirs::home_dir()
        .context("Failed to get home directory")?
        .join(".config/systemd/user/freenet.service");

    if service_path.exists() {
        fs::remove_file(&service_path).context("Failed to remove service file")?;
    }

    // Reload systemd
    let _ = std::process::Command::new("systemctl")
        .args(["--user", "daemon-reload"])
        .status();

    println!("Freenet service uninstalled.");

    Ok(())
}

#[cfg(target_os = "linux")]
fn service_status() -> Result<()> {
    let status = std::process::Command::new("systemctl")
        .args(["--user", "status", "freenet"])
        .status()
        .context("Failed to check service status")?;

    std::process::exit(status.code().unwrap_or(1));
}

#[cfg(target_os = "linux")]
fn start_service() -> Result<()> {
    let status = std::process::Command::new("systemctl")
        .args(["--user", "start", "freenet"])
        .status()
        .context("Failed to start service")?;

    if status.success() {
        println!("Freenet service started.");
    } else {
        anyhow::bail!("Failed to start service");
    }

    Ok(())
}

#[cfg(target_os = "linux")]
fn stop_service() -> Result<()> {
    let status = std::process::Command::new("systemctl")
        .args(["--user", "stop", "freenet"])
        .status()
        .context("Failed to stop service")?;

    if status.success() {
        println!("Freenet service stopped.");
    } else {
        anyhow::bail!("Failed to stop service");
    }

    Ok(())
}

#[cfg(target_os = "linux")]
fn restart_service() -> Result<()> {
    let status = std::process::Command::new("systemctl")
        .args(["--user", "restart", "freenet"])
        .status()
        .context("Failed to restart service")?;

    if status.success() {
        println!("Freenet service restarted.");
    } else {
        anyhow::bail!("Failed to restart service");
    }

    Ok(())
}

#[cfg(target_os = "linux")]
fn service_logs(error_only: bool) -> Result<()> {
    let log_dir = dirs::home_dir()
        .context("Failed to get home directory")?
        .join(".local/state/freenet");

    let base_name = if error_only {
        "freenet.error"
    } else {
        "freenet"
    };

    let log_file = find_latest_log_file(&log_dir, base_name).ok_or_else(|| {
        anyhow::anyhow!(
            "No log files found in: {}\nMake sure the service has been installed and started.",
            log_dir.display()
        )
    })?;

    println!("Following logs from: {}", log_file.display());
    println!("Press Ctrl+C to stop.\n");

    let status = std::process::Command::new("tail")
        .arg("-f")
        .arg(&log_file)
        .status()
        .context("Failed to tail log file")?;

    std::process::exit(status.code().unwrap_or(1));
}

// macOS implementation using launchd
#[cfg(target_os = "macos")]
fn install_service() -> Result<()> {
    use std::fs;
    use std::os::unix::fs::PermissionsExt;

    let exe_path = std::env::current_exe().context("Failed to get current executable path")?;
    let home_dir = dirs::home_dir().context("Failed to get home directory")?;

    let launch_agents_dir = home_dir.join("Library/LaunchAgents");
    fs::create_dir_all(&launch_agents_dir).context("Failed to create LaunchAgents directory")?;

    // Create log directory in proper macOS location
    let log_dir = home_dir.join("Library/Logs/freenet");
    fs::create_dir_all(&log_dir).context("Failed to create log directory")?;

    // Create wrapper script for auto-update support.
    // launchd doesn't have ExecStopPost like systemd, so we use a wrapper
    // that checks exit code 42 (update needed) and runs update before restart.
    let wrapper_dir = home_dir.join(".local/bin");
    fs::create_dir_all(&wrapper_dir).context("Failed to create wrapper directory")?;
    let wrapper_path = wrapper_dir.join("freenet-service-wrapper.sh");
    let wrapper_content = generate_wrapper_script(&exe_path);
    fs::write(&wrapper_path, wrapper_content).context("Failed to write wrapper script")?;
    fs::set_permissions(&wrapper_path, fs::Permissions::from_mode(0o755))
        .context("Failed to make wrapper script executable")?;

    let plist_content = generate_plist(&wrapper_path, &log_dir);
    let plist_path = launch_agents_dir.join("org.freenet.node.plist");

    fs::write(&plist_path, plist_content).context("Failed to write plist file")?;

    println!("Freenet service installed successfully.");
    println!();
    println!("To start the service now:");
    println!("  freenet service start");
    println!();
    println!("The service will start automatically on login.");
    println!("Logs will be written to: {}", log_dir.display());

    Ok(())
}

#[cfg(target_os = "macos")]
pub fn generate_wrapper_script(binary_path: &Path) -> String {
    format!(
        r#"#!/bin/bash
# Freenet service wrapper for auto-update support.
# This wrapper monitors exit code 42 (update needed) and runs update before restart.
# Includes exponential backoff to prevent rapid restart loops on repeated failures.

BACKOFF=10       # Initial backoff in seconds
MAX_BACKOFF=300  # Maximum backoff (5 minutes)
CONSECUTIVE_FAILURES=0

while true; do
    "{binary}" network
    EXIT_CODE=$?

    if [ $EXIT_CODE -eq 42 ]; then
        echo "$(date): Update needed, running freenet update..." >> "$HOME/Library/Logs/freenet/freenet.log"
        if "{binary}" update --quiet; then
            echo "$(date): Update successful, restarting..." >> "$HOME/Library/Logs/freenet/freenet.log"
            CONSECUTIVE_FAILURES=0
            BACKOFF=10
            sleep 2
        else
            CONSECUTIVE_FAILURES=$((CONSECUTIVE_FAILURES + 1))
            echo "$(date): Update failed (attempt $CONSECUTIVE_FAILURES), backing off $BACKOFF seconds..." >> "$HOME/Library/Logs/freenet/freenet.log"
            sleep $BACKOFF
            BACKOFF=$((BACKOFF * 2))
            [ $BACKOFF -gt $MAX_BACKOFF ] && BACKOFF=$MAX_BACKOFF
        fi
        continue
    elif [ $EXIT_CODE -eq 0 ]; then
        echo "$(date): Normal shutdown" >> "$HOME/Library/Logs/freenet/freenet.log"
        exit 0
    else
        echo "$(date): Exited with code $EXIT_CODE, restarting after backoff..." >> "$HOME/Library/Logs/freenet/freenet.log"
        sleep $BACKOFF
        BACKOFF=$((BACKOFF * 2))
        [ $BACKOFF -gt $MAX_BACKOFF ] && BACKOFF=$MAX_BACKOFF
    fi
done
"#,
        binary = binary_path.display()
    )
}

#[cfg(target_os = "macos")]
fn generate_plist(wrapper_path: &Path, log_dir: &Path) -> String {
    // Note: wrapper_path is the auto-update wrapper script, not the freenet binary directly.
    // The wrapper handles the loop: run freenet, check exit code, update if needed.
    format!(
        r#"<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
    <key>Label</key>
    <string>org.freenet.node</string>
    <key>ProgramArguments</key>
    <array>
        <string>{wrapper}</string>
    </array>
    <key>RunAtLoad</key>
    <true/>
    <key>KeepAlive</key>
    <dict>
        <key>SuccessfulExit</key>
        <false/>
    </dict>
    <key>StandardOutPath</key>
    <string>{log_dir}/freenet.log</string>
    <key>StandardErrorPath</key>
    <string>{log_dir}/freenet.error.log</string>
    <key>SoftResourceLimits</key>
    <dict>
        <key>NumberOfFiles</key>
        <integer>65536</integer>
    </dict>
    <key>HardResourceLimits</key>
    <dict>
        <key>NumberOfFiles</key>
        <integer>65536</integer>
    </dict>
</dict>
</plist>
"#,
        wrapper = wrapper_path.display(),
        log_dir = log_dir.display()
    )
}

#[cfg(target_os = "macos")]
fn uninstall_service() -> Result<()> {
    use std::fs;

    let plist_path = dirs::home_dir()
        .context("Failed to get home directory")?
        .join("Library/LaunchAgents/org.freenet.node.plist");

    let plist_path_str = plist_path
        .to_str()
        .context("Plist path contains invalid UTF-8")?;

    // Unload the service if loaded (ignore errors as it may not be loaded)
    let unload_status = std::process::Command::new("launchctl")
        .args(["unload", plist_path_str])
        .status();
    if let Err(e) = unload_status {
        eprintln!("Warning: Failed to unload service: {}", e);
    }

    // Remove the plist file
    if plist_path.exists() {
        fs::remove_file(&plist_path).context("Failed to remove plist file")?;
    }

    println!("Freenet service uninstalled.");

    Ok(())
}

#[cfg(target_os = "macos")]
fn service_status() -> Result<()> {
    let output = std::process::Command::new("launchctl")
        .args(["list", "org.freenet.node"])
        .output()
        .context("Failed to check service status")?;

    if output.status.success() {
        println!("Freenet service is running.");
        if !output.stdout.is_empty() {
            println!("{}", String::from_utf8_lossy(&output.stdout));
        }
    } else {
        println!("Freenet service is not running.");
        std::process::exit(3); // Standard exit code for "not running"
    }

    Ok(())
}

#[cfg(target_os = "macos")]
fn start_service() -> Result<()> {
    let plist_path = dirs::home_dir()
        .context("Failed to get home directory")?
        .join("Library/LaunchAgents/org.freenet.node.plist");

    if !plist_path.exists() {
        anyhow::bail!("Service not installed. Run 'freenet service install' first.");
    }

    let plist_path_str = plist_path
        .to_str()
        .context("Plist path contains invalid UTF-8")?;

    let status = std::process::Command::new("launchctl")
        .args(["load", plist_path_str])
        .status()
        .context("Failed to start service")?;

    if status.success() {
        println!("Freenet service started.");
    } else {
        anyhow::bail!("Failed to start service");
    }

    Ok(())
}

#[cfg(target_os = "macos")]
fn stop_service() -> Result<()> {
    let plist_path = dirs::home_dir()
        .context("Failed to get home directory")?
        .join("Library/LaunchAgents/org.freenet.node.plist");

    let plist_path_str = plist_path
        .to_str()
        .context("Plist path contains invalid UTF-8")?;

    let status = std::process::Command::new("launchctl")
        .args(["unload", plist_path_str])
        .status()
        .context("Failed to stop service")?;

    if status.success() {
        println!("Freenet service stopped.");
    } else {
        anyhow::bail!("Failed to stop service");
    }

    Ok(())
}

#[cfg(target_os = "macos")]
fn restart_service() -> Result<()> {
    stop_service()?;
    start_service()
}

#[cfg(target_os = "macos")]
fn service_logs(error_only: bool) -> Result<()> {
    let log_dir = dirs::home_dir()
        .context("Failed to get home directory")?
        .join("Library/Logs/freenet");

    let base_name = if error_only {
        "freenet.error"
    } else {
        "freenet"
    };

    let log_file = find_latest_log_file(&log_dir, base_name).ok_or_else(|| {
        anyhow::anyhow!(
            "No log files found in: {}\nMake sure the service has been installed and started.",
            log_dir.display()
        )
    })?;

    println!("Following logs from: {}", log_file.display());
    println!("Press Ctrl+C to stop.\n");

    let status = std::process::Command::new("tail")
        .arg("-f")
        .arg(&log_file)
        .status()
        .context("Failed to tail log file")?;

    std::process::exit(status.code().unwrap_or(1));
}

// Windows implementation
// Note: Windows service management requires either:
// 1. Running as a Windows Service (requires service registration)
// 2. Using Task Scheduler for user-level autostart
// For now, we provide Task Scheduler-based autostart

#[cfg(target_os = "windows")]
fn install_service() -> Result<()> {
    let exe_path = std::env::current_exe().context("Failed to get current executable path")?;
    let exe_path_str = exe_path
        .to_str()
        .context("Executable path contains invalid UTF-8")?;

    // Use Task Scheduler to run at login
    // schtasks /create /tn "Freenet" /tr "path\to\freenet.exe network" /sc onlogon /rl highest
    let status = std::process::Command::new("schtasks")
        .args([
            "/create",
            "/tn",
            "Freenet",
            "/tr",
            &format!("\"{}\" network", exe_path_str),
            "/sc",
            "onlogon",
            "/rl",
            "highest",
            "/f", // Force overwrite if exists
        ])
        .status()
        .context("Failed to create scheduled task")?;

    if !status.success() {
        anyhow::bail!("Failed to create scheduled task. You may need to run as Administrator.");
    }

    println!("Freenet scheduled task installed successfully.");
    println!();
    println!("To start Freenet now:");
    println!("  freenet service start");
    println!();
    println!("Freenet will start automatically when you log in.");
    println!();
    println!("Note: For production use, consider running Freenet as a Windows Service.");
    println!("See: https://freenet.org/docs/windows-service");

    Ok(())
}

#[cfg(target_os = "windows")]
fn uninstall_service() -> Result<()> {
    // Stop if running
    let _ = std::process::Command::new("schtasks")
        .args(["/end", "/tn", "Freenet"])
        .status();

    // Delete the task
    let status = std::process::Command::new("schtasks")
        .args(["/delete", "/tn", "Freenet", "/f"])
        .status()
        .context("Failed to delete scheduled task")?;

    if !status.success() {
        anyhow::bail!("Failed to delete scheduled task. It may not exist or you may need Administrator privileges.");
    }

    println!("Freenet scheduled task uninstalled.");

    Ok(())
}

#[cfg(target_os = "windows")]
fn service_status() -> Result<()> {
    let output = std::process::Command::new("schtasks")
        .args(["/query", "/tn", "Freenet", "/v", "/fo", "list"])
        .output()
        .context("Failed to query scheduled task")?;

    if output.status.success() {
        println!("Freenet scheduled task is installed.");
        println!("{}", String::from_utf8_lossy(&output.stdout));
    } else {
        println!("Freenet scheduled task is not installed.");
        std::process::exit(3);
    }

    Ok(())
}

#[cfg(target_os = "windows")]
fn start_service() -> Result<()> {
    let status = std::process::Command::new("schtasks")
        .args(["/run", "/tn", "Freenet"])
        .status()
        .context("Failed to start scheduled task")?;

    if status.success() {
        println!("Freenet started.");
    } else {
        anyhow::bail!("Failed to start Freenet. Make sure the scheduled task is installed.");
    }

    Ok(())
}

#[cfg(target_os = "windows")]
fn stop_service() -> Result<()> {
    let status = std::process::Command::new("schtasks")
        .args(["/end", "/tn", "Freenet"])
        .status()
        .context("Failed to stop scheduled task")?;

    if status.success() {
        println!("Freenet stopped.");
    } else {
        anyhow::bail!("Failed to stop Freenet. It may not be running.");
    }

    Ok(())
}

#[cfg(target_os = "windows")]
fn restart_service() -> Result<()> {
    let _ = stop_service();
    // Give it a moment to stop
    std::thread::sleep(std::time::Duration::from_secs(2));
    start_service()
}

#[cfg(target_os = "windows")]
fn service_logs(_error_only: bool) -> Result<()> {
    // Windows Task Scheduler doesn't capture stdout/stderr to files by default.
    // Users can check Event Viewer or run Freenet manually to see output.
    anyhow::bail!(
        "Log viewing is not yet supported on Windows.\n\
         To view logs, run 'freenet network' manually in a terminal,\n\
         or check Windows Event Viewer for application errors."
    )
}

// Fallback for unsupported platforms
#[cfg(not(any(target_os = "linux", target_os = "macos", target_os = "windows")))]
fn install_service() -> Result<()> {
    anyhow::bail!("Service installation is not supported on this platform")
}

#[cfg(not(any(target_os = "linux", target_os = "macos", target_os = "windows")))]
fn uninstall_service() -> Result<()> {
    anyhow::bail!("Service management is not supported on this platform")
}

#[cfg(not(any(target_os = "linux", target_os = "macos", target_os = "windows")))]
fn service_status() -> Result<()> {
    anyhow::bail!("Service management is not supported on this platform")
}

#[cfg(not(any(target_os = "linux", target_os = "macos", target_os = "windows")))]
fn start_service() -> Result<()> {
    anyhow::bail!("Service management is not supported on this platform")
}

#[cfg(not(any(target_os = "linux", target_os = "macos", target_os = "windows")))]
fn stop_service() -> Result<()> {
    anyhow::bail!("Service management is not supported on this platform")
}

#[cfg(not(any(target_os = "linux", target_os = "macos", target_os = "windows")))]
fn restart_service() -> Result<()> {
    anyhow::bail!("Service management is not supported on this platform")
}

#[cfg(not(any(target_os = "linux", target_os = "macos", target_os = "windows")))]
fn service_logs(_error_only: bool) -> Result<()> {
    anyhow::bail!("Service management is not supported on this platform")
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;

    #[test]
    #[cfg(target_os = "linux")]
    fn test_systemd_service_file_generation() {
        let binary_path = PathBuf::from("/usr/local/bin/freenet");
        let log_dir = PathBuf::from("/home/test/.local/state/freenet");
        let service_content = generate_service_file(&binary_path, &log_dir);

        // Verify the service file contains expected sections
        assert!(service_content.contains("[Unit]"));
        assert!(service_content.contains("[Service]"));
        assert!(service_content.contains("[Install]"));

        // Verify it references the correct binary
        assert!(service_content.contains("/usr/local/bin/freenet network"));

        // Verify log paths are set correctly (file-based logging for headless systems)
        assert!(service_content.contains("/home/test/.local/state/freenet/freenet.log"));
        assert!(service_content.contains("/home/test/.local/state/freenet/freenet.error.log"));

        // Verify resource limits are set
        assert!(service_content.contains("LimitNOFILE=65536"));
        assert!(service_content.contains("MemoryMax=2G"));
        assert!(service_content.contains("CPUQuota=200%"));

        // Verify restart configuration (always restart for auto-update support)
        assert!(service_content.contains("Restart=always"));
        assert!(service_content.contains("RestartSec=10"));

        // Verify auto-update support via ExecStopPost
        assert!(service_content.contains("ExecStopPost="));

        // Verify graceful shutdown timeout is set
        assert!(service_content.contains("TimeoutStopSec=15"));
    }

    #[test]
    #[cfg(target_os = "macos")]
    fn test_launchd_plist_generation() {
        let binary_path = PathBuf::from("/usr/local/bin/freenet");
        let log_dir = PathBuf::from("/Users/test/Library/Logs/freenet");
        let plist_content = generate_plist(&binary_path, &log_dir);

        // Verify it's valid plist structure
        assert!(plist_content.contains("<?xml version=\"1.0\""));
        assert!(plist_content.contains("<plist version=\"1.0\">"));
        assert!(plist_content.contains("</plist>"));

        // Verify the label is set
        assert!(plist_content.contains("<string>org.freenet.node</string>"));

        // Verify it references the correct binary
        assert!(plist_content.contains("/usr/local/bin/freenet"));

        // Verify log paths are set correctly
        assert!(plist_content.contains("/Users/test/Library/Logs/freenet/freenet.log"));
        assert!(plist_content.contains("/Users/test/Library/Logs/freenet/freenet.error.log"));

        // Verify resource limits are set
        assert!(plist_content.contains("<key>NumberOfFiles</key>"));
        assert!(plist_content.contains("<integer>65536</integer>"));

        // Verify RunAtLoad is set
        assert!(plist_content.contains("<key>RunAtLoad</key>"));
        assert!(plist_content.contains("<true/>"));
    }
}
