//! System service management for Freenet.
//!
//! Supports:
//! - Linux: systemd user service
//! - macOS: launchd user agent

use anyhow::{Context, Result};
use clap::Subcommand;
use std::path::Path;

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
RestartSec=10

# Logging
StandardOutput=journal
StandardError=journal
SyslogIdentifier=freenet

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
    let launch_agents_dir = dirs::home_dir()
        .context("Failed to get home directory")?
        .join("Library/LaunchAgents");

    fs::create_dir_all(&launch_agents_dir).context("Failed to create LaunchAgents directory")?;

    let plist_content = generate_plist(&exe_path);
    let plist_path = launch_agents_dir.join("org.freenet.node.plist");

    fs::write(&plist_path, plist_content).context("Failed to write plist file")?;

    println!("Freenet service installed successfully.");
    println!();
    println!("To start the service now:");
    println!("  freenet service start");
    println!();
    println!("The service will start automatically on login.");

    Ok(())
}

#[cfg(target_os = "macos")]
fn generate_plist(binary_path: &Path) -> String {
    format!(
        r#"<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
    <key>Label</key>
    <string>org.freenet.node</string>
    <key>ProgramArguments</key>
    <array>
        <string>{}</string>
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
    <string>/tmp/freenet.log</string>
    <key>StandardErrorPath</key>
    <string>/tmp/freenet.error.log</string>
</dict>
</plist>
"#,
        binary_path.display()
    )
}

#[cfg(target_os = "macos")]
fn uninstall_service() -> Result<()> {
    use std::fs;

    let plist_path = dirs::home_dir()
        .context("Failed to get home directory")?
        .join("Library/LaunchAgents/org.freenet.node.plist");

    // Unload the service if loaded
    let _ = std::process::Command::new("launchctl")
        .args(["unload", plist_path.to_str().unwrap_or_default()])
        .status();

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

    let status = std::process::Command::new("launchctl")
        .args(["load", plist_path.to_str().unwrap_or_default()])
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

    let status = std::process::Command::new("launchctl")
        .args(["unload", plist_path.to_str().unwrap_or_default()])
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

// Fallback for unsupported platforms
#[cfg(not(any(target_os = "linux", target_os = "macos")))]
fn install_service() -> Result<()> {
    anyhow::bail!("Service installation is not supported on this platform")
}

#[cfg(not(any(target_os = "linux", target_os = "macos")))]
fn uninstall_service() -> Result<()> {
    anyhow::bail!("Service management is not supported on this platform")
}

#[cfg(not(any(target_os = "linux", target_os = "macos")))]
fn service_status() -> Result<()> {
    anyhow::bail!("Service management is not supported on this platform")
}

#[cfg(not(any(target_os = "linux", target_os = "macos")))]
fn start_service() -> Result<()> {
    anyhow::bail!("Service management is not supported on this platform")
}

#[cfg(not(any(target_os = "linux", target_os = "macos")))]
fn stop_service() -> Result<()> {
    anyhow::bail!("Service management is not supported on this platform")
}

#[cfg(not(any(target_os = "linux", target_os = "macos")))]
fn restart_service() -> Result<()> {
    anyhow::bail!("Service management is not supported on this platform")
}
