//! User service management for Freenet.
//!
//! Supports:
//! - Linux: systemd user service (~/.config/systemd/user/)
//! - macOS: launchd user agent (~/Library/LaunchAgents/)

use anyhow::{Context, Result};
use clap::Subcommand;
use std::path::Path;

#[derive(Subcommand, Debug, Clone)]
pub enum ServiceCommand {
    /// Install Freenet as a user service (auto-starts on login)
    Install,
    /// Uninstall the Freenet user service
    Uninstall,
    /// Check the status of the Freenet service
    Status,
    /// Start the Freenet service
    Start,
    /// Stop the Freenet service
    Stop,
    /// Restart the Freenet service
    Restart,
}

impl ServiceCommand {
    pub fn run(&self) -> Result<()> {
        match self {
            ServiceCommand::Install => install_service(),
            ServiceCommand::Uninstall => uninstall_service(),
            ServiceCommand::Status => service_status(),
            ServiceCommand::Start => start_service(),
            ServiceCommand::Stop => stop_service(),
            ServiceCommand::Restart => restart_service(),
        }
    }
}

#[cfg(target_os = "linux")]
fn install_service() -> Result<()> {
    use std::fs;

    let exe_path = std::env::current_exe().context("Failed to get current executable path")?;
    let service_dir = dirs::home_dir()
        .context("Failed to get home directory")?
        .join(".config/systemd/user");

    fs::create_dir_all(&service_dir).context("Failed to create systemd user directory")?;

    let service_content = generate_service_file(&exe_path);
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

    Ok(())
}

#[cfg(target_os = "linux")]
fn generate_service_file(binary_path: &Path) -> String {
    format!(
        r#"[Unit]
Description=Freenet Node
Documentation=https://freenet.org
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
ExecStart={} network
Restart=on-failure
# Wait 10 seconds before restart to avoid rapid restart loops
RestartSec=10

# Logging
StandardOutput=journal
StandardError=journal
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
        binary_path.display()
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

// macOS implementation using launchd
#[cfg(target_os = "macos")]
fn install_service() -> Result<()> {
    use std::fs;

    let exe_path = std::env::current_exe().context("Failed to get current executable path")?;
    let home_dir = dirs::home_dir().context("Failed to get home directory")?;

    let launch_agents_dir = home_dir.join("Library/LaunchAgents");
    fs::create_dir_all(&launch_agents_dir).context("Failed to create LaunchAgents directory")?;

    // Create log directory in proper macOS location
    let log_dir = home_dir.join("Library/Logs/freenet");
    fs::create_dir_all(&log_dir).context("Failed to create log directory")?;

    let plist_content = generate_plist(&exe_path, &log_dir);
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
fn generate_plist(binary_path: &Path, log_dir: &Path) -> String {
    format!(
        r#"<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
    <key>Label</key>
    <string>org.freenet.node</string>
    <key>ProgramArguments</key>
    <array>
        <string>{binary}</string>
        <string>network</string>
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
        binary = binary_path.display(),
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

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;

    #[test]
    #[cfg(target_os = "linux")]
    fn test_systemd_service_file_generation() {
        let binary_path = PathBuf::from("/usr/local/bin/freenet");
        let service_content = generate_service_file(&binary_path);

        // Verify the service file contains expected sections
        assert!(service_content.contains("[Unit]"));
        assert!(service_content.contains("[Service]"));
        assert!(service_content.contains("[Install]"));

        // Verify it references the correct binary
        assert!(service_content.contains("/usr/local/bin/freenet network"));

        // Verify resource limits are set
        assert!(service_content.contains("LimitNOFILE=65536"));
        assert!(service_content.contains("MemoryMax=2G"));
        assert!(service_content.contains("CPUQuota=200%"));

        // Verify restart configuration
        assert!(service_content.contains("Restart=on-failure"));
        assert!(service_content.contains("RestartSec=10"));
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
