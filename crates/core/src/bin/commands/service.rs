//! System service management for Freenet.
//!
//! Supports:
//! - Linux: systemd user service (default) or system-wide service (--system)
//! - macOS: launchd user agent

use anyhow::{Context, Result};
use clap::Subcommand;
use freenet::config::ConfigPaths;
use std::path::Path;
use std::sync::Arc;

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
    Install {
        /// Install as a system-wide service instead of a user service.
        /// Requires root. Use this for containers (LXC/Docker), headless
        /// servers, or environments without a user session bus.
        #[arg(long)]
        system: bool,
    },
    /// Uninstall the Freenet system service
    Uninstall {
        /// Uninstall the system-wide service instead of the user service
        #[arg(long)]
        system: bool,
    },
    /// Check the status of the Freenet service
    Status {
        /// Check the system-wide service instead of the user service
        #[arg(long)]
        system: bool,
    },
    /// Start the Freenet service
    Start {
        /// Start the system-wide service instead of the user service
        #[arg(long)]
        system: bool,
    },
    /// Stop the Freenet service
    Stop {
        /// Stop the system-wide service instead of the user service
        #[arg(long)]
        system: bool,
    },
    /// Restart the Freenet service
    Restart {
        /// Restart the system-wide service instead of the user service
        #[arg(long)]
        system: bool,
    },
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
        config_dirs: Arc<ConfigPaths>,
    ) -> Result<()> {
        match self {
            ServiceCommand::Install { system } => install_service(*system),
            ServiceCommand::Uninstall { system } => uninstall_service(*system),
            ServiceCommand::Status { system } => service_status(*system),
            ServiceCommand::Start { system } => start_service(*system),
            ServiceCommand::Stop { system } => stop_service(*system),
            ServiceCommand::Restart { system } => restart_service(*system),
            ServiceCommand::Logs { err } => service_logs(*err),
            ServiceCommand::Report(cmd) => {
                cmd.run(version, git_commit, git_dirty, build_timestamp, config_dirs)
            }
        }
    }
}

/// Path to the system-wide systemd service file.
#[cfg(target_os = "linux")]
const SYSTEM_SERVICE_PATH: &str = "/etc/systemd/system/freenet.service";

/// Check if a system-wide Freenet service is installed.
#[cfg(target_os = "linux")]
fn has_system_service() -> bool {
    Path::new(SYSTEM_SERVICE_PATH).exists()
}

/// Check if a user-level Freenet service is installed.
#[cfg(target_os = "linux")]
fn has_user_service() -> bool {
    dirs::home_dir()
        .map(|h| h.join(".config/systemd/user/freenet.service").exists())
        .unwrap_or(false)
}

/// Recursively chown a directory to the given user (best-effort).
/// Used after creating directories with sudo so the service user can write to them.
#[cfg(target_os = "linux")]
fn chown_to_user(path: &Path, username: &str) {
    let _ = std::process::Command::new("chown")
        .args(["-R", username, &path.display().to_string()])
        .status();
}

/// Look up a user's home directory from /etc/passwd via `getent passwd`.
/// Falls back to `/home/{username}` if getent is unavailable.
#[cfg(target_os = "linux")]
fn home_dir_for_user(username: &str) -> std::path::PathBuf {
    // Try getent passwd which works with NSS (LDAP, NIS, etc.)
    if let Ok(output) = std::process::Command::new("getent")
        .args(["passwd", username])
        .output()
    {
        if output.status.success() {
            let line = String::from_utf8_lossy(&output.stdout);
            // Format: username:x:uid:gid:gecos:home:shell
            if let Some(home) = line.split(':').nth(5) {
                let home = home.trim();
                if !home.is_empty() {
                    return std::path::PathBuf::from(home);
                }
            }
        }
    }
    std::path::PathBuf::from(format!("/home/{username}"))
}

/// Resolve whether to use system or user mode.
/// If `--system` is passed, use system mode. Otherwise auto-detect based on
/// which service file exists, defaulting to user mode.
#[cfg(target_os = "linux")]
fn use_system_mode(system_flag: bool) -> bool {
    // Auto-detect: if only system service exists, use system mode
    system_flag || (has_system_service() && !has_user_service())
}

/// Run a systemctl command, using --user or not based on system mode.
#[cfg(target_os = "linux")]
fn systemctl(system_mode: bool, args: &[&str]) -> Result<std::process::ExitStatus> {
    let mut cmd = std::process::Command::new("systemctl");
    if !system_mode {
        cmd.arg("--user");
    }
    cmd.args(args);
    let status = cmd.status().context("Failed to run systemctl")?;
    Ok(status)
}

/// Run a systemctl command with helpful error on user-session failures.
#[cfg(target_os = "linux")]
fn systemctl_with_hint(system_mode: bool, args: &[&str], action: &str) -> Result<()> {
    let status = systemctl(system_mode, args)?;
    if status.success() {
        return Ok(());
    }

    if system_mode {
        anyhow::bail!("Failed to {action}");
    }

    // Check if this looks like a user session bus issue
    let hint = std::process::Command::new("systemctl")
        .args(["--user", "daemon-reload"])
        .stderr(std::process::Stdio::piped())
        .output()
        .ok()
        .and_then(|out| {
            let stderr = String::from_utf8_lossy(&out.stderr);
            if stderr.contains("bus")
                || stderr.contains("XDG_RUNTIME_DIR")
                || stderr.contains("Failed to connect")
            {
                Some(
                    "\n\nHint: User systemd session not available (common in containers/LXC).\n\
                     Try: sudo freenet service install --system",
                )
            } else {
                None
            }
        })
        .unwrap_or("");

    anyhow::bail!("Failed to {action}{hint}");
}

#[cfg(target_os = "linux")]
fn install_service(system: bool) -> Result<()> {
    if system {
        install_system_service()
    } else {
        install_user_service()
    }
}

#[cfg(target_os = "linux")]
fn install_user_service() -> Result<()> {
    use std::fs;

    let exe_path = std::env::current_exe().context("Failed to get current executable path")?;
    let home_dir = dirs::home_dir().context("Failed to get home directory")?;

    let service_dir = home_dir.join(".config/systemd/user");
    fs::create_dir_all(&service_dir).context("Failed to create systemd user directory")?;

    // Create log directory - use ~/.local/state/freenet for XDG compliance
    let log_dir = home_dir.join(".local/state/freenet");
    fs::create_dir_all(&log_dir).context("Failed to create log directory")?;

    let service_content = generate_user_service_file(&exe_path, &log_dir);
    let service_path = service_dir.join("freenet.service");

    fs::write(&service_path, service_content).context("Failed to write service file")?;

    // Reload systemd user daemon
    systemctl_with_hint(false, &["daemon-reload"], "reload systemd daemon")?;

    // Enable the service
    systemctl_with_hint(false, &["enable", "freenet"], "enable service")?;

    println!("Freenet user service installed successfully.");
    println!();
    println!("To start the service now:");
    println!("  freenet service start");
    println!();
    println!("The service will start automatically on login.");
    println!("Logs will be written to: {}", log_dir.display());

    Ok(())
}

#[cfg(target_os = "linux")]
fn install_system_service() -> Result<()> {
    use std::fs;

    let exe_path = std::env::current_exe().context("Failed to get current executable path")?;

    // Get the user to run the service as.
    // When running with sudo, SUDO_USER has the original (non-root) user.
    let username = std::env::var("SUDO_USER")
        .or_else(|_| std::env::var("USER"))
        .or_else(|_| std::env::var("LOGNAME"))
        .context(
            "Could not determine username. Set the USER environment variable \
             or run with sudo (which sets SUDO_USER).",
        )?;

    if username == "root" {
        anyhow::bail!(
            "Refusing to install system service running as root.\n\
             Run with sudo from a non-root user account so SUDO_USER is set,\n\
             or set the USER environment variable to the desired service user."
        );
    }

    // Look up the user's home directory from /etc/passwd.
    // When running with sudo, dirs::home_dir() returns /root which is wrong.
    let home_dir = home_dir_for_user(&username);

    // Create log directory and fix ownership (we're running as root via sudo,
    // but the service will run as the target user).
    let log_dir = home_dir.join(".local/state/freenet");
    fs::create_dir_all(&log_dir).context("Failed to create log directory")?;
    chown_to_user(&log_dir, &username);

    let service_content = generate_system_service_file(&exe_path, &log_dir, &username, &home_dir);

    fs::write(SYSTEM_SERVICE_PATH, &service_content).with_context(|| {
        format!(
            "Failed to write service file to {SYSTEM_SERVICE_PATH}. \
             Are you running as root? Try: sudo freenet service install --system"
        )
    })?;

    // Reload systemd daemon (system-level, no --user)
    let status = systemctl(true, &["daemon-reload"])?;
    if !status.success() {
        anyhow::bail!("Failed to reload systemd daemon");
    }

    // Enable the service
    let status = systemctl(true, &["enable", "freenet"])?;
    if !status.success() {
        anyhow::bail!("Failed to enable service");
    }

    println!("Freenet system service installed successfully.");
    println!("  Service runs as user: {username}");
    println!();
    println!("To start the service now:");
    println!("  sudo freenet service start --system");
    println!();
    println!("The service will start automatically on boot.");
    println!("Logs will be written to: {}", log_dir.display());

    Ok(())
}

#[cfg(target_os = "linux")]
pub fn generate_user_service_file(binary_path: &Path, log_dir: &Path) -> String {
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
pub fn generate_system_service_file(
    binary_path: &Path,
    log_dir: &Path,
    username: &str,
    home_dir: &Path,
) -> String {
    format!(
        r#"[Unit]
Description=Freenet Node
Documentation=https://freenet.org
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
User={username}
Environment=HOME={home}
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
StandardOutput=append:{log_dir}/freenet.log
StandardError=append:{log_dir}/freenet.error.log
SyslogIdentifier=freenet

# Resource limits to prevent runaway resource consumption
# File descriptors needed for network connections
LimitNOFILE=65536
# Memory limit (2GB soft limit)
MemoryMax=2G
# CPU quota (200% = 2 cores max)
CPUQuota=200%

[Install]
WantedBy=multi-user.target
"#,
        binary = binary_path.display(),
        log_dir = log_dir.display(),
        username = username,
        home = home_dir.display()
    )
}

#[cfg(target_os = "linux")]
fn uninstall_service(system: bool) -> Result<()> {
    use std::fs;

    let system_mode = use_system_mode(system);

    // Stop the service if running
    let _ = systemctl(system_mode, &["stop", "freenet"]);

    // Disable the service
    let _ = systemctl(system_mode, &["disable", "freenet"]);

    // Remove the service file
    let service_path = if system_mode {
        std::path::PathBuf::from(SYSTEM_SERVICE_PATH)
    } else {
        dirs::home_dir()
            .context("Failed to get home directory")?
            .join(".config/systemd/user/freenet.service")
    };

    if service_path.exists() {
        fs::remove_file(&service_path).context("Failed to remove service file")?;
    }

    // Reload systemd
    let _ = systemctl(system_mode, &["daemon-reload"]);

    println!("Freenet service uninstalled.");

    Ok(())
}

#[cfg(target_os = "linux")]
fn service_status(system: bool) -> Result<()> {
    let system_mode = use_system_mode(system);
    let status = systemctl(system_mode, &["status", "freenet"])?;
    std::process::exit(status.code().unwrap_or(1));
}

#[cfg(target_os = "linux")]
fn start_service(system: bool) -> Result<()> {
    let system_mode = use_system_mode(system);
    systemctl_with_hint(system_mode, &["start", "freenet"], "start service")?;
    println!("Freenet service started.");
    Ok(())
}

#[cfg(target_os = "linux")]
fn stop_service(system: bool) -> Result<()> {
    let system_mode = use_system_mode(system);
    systemctl_with_hint(system_mode, &["stop", "freenet"], "stop service")?;
    println!("Freenet service stopped.");
    Ok(())
}

#[cfg(target_os = "linux")]
fn restart_service(system: bool) -> Result<()> {
    let system_mode = use_system_mode(system);
    systemctl_with_hint(system_mode, &["restart", "freenet"], "restart service")?;
    println!("Freenet service restarted.");
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
// --system flag is not supported on macOS (launchd daemons need different setup)
#[cfg(target_os = "macos")]
fn install_service(system: bool) -> Result<()> {
    if system {
        anyhow::bail!(
            "The --system flag is only supported on Linux.\n\
             On macOS, use the default user agent: freenet service install"
        );
    }
    install_macos_service()
}

#[cfg(target_os = "macos")]
fn install_macos_service() -> Result<()> {
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

/// Bail with "--system not supported on macOS" for commands that don't apply.
#[cfg(target_os = "macos")]
fn check_no_system_flag(system: bool) -> Result<()> {
    if system {
        anyhow::bail!(
            "The --system flag is only supported on Linux.\n\
             On macOS, use the default user agent commands without --system."
        );
    }
    Ok(())
}

#[cfg(target_os = "macos")]
fn uninstall_service(system: bool) -> Result<()> {
    use std::fs;

    check_no_system_flag(system)?;

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
fn service_status(system: bool) -> Result<()> {
    check_no_system_flag(system)?;

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
fn start_service(system: bool) -> Result<()> {
    check_no_system_flag(system)?;

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
fn stop_service(system: bool) -> Result<()> {
    check_no_system_flag(system)?;

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
fn restart_service(system: bool) -> Result<()> {
    check_no_system_flag(system)?;
    stop_service(false)?;
    start_service(false)
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
fn install_service(system: bool) -> Result<()> {
    if system {
        anyhow::bail!(
            "The --system flag is only supported on Linux.\n\
             On Windows, use the default scheduled task: freenet service install"
        );
    }

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
fn check_no_system_flag_windows(system: bool) -> Result<()> {
    if system {
        anyhow::bail!(
            "The --system flag is only supported on Linux.\n\
             On Windows, use the default scheduled task commands without --system."
        );
    }
    Ok(())
}

#[cfg(target_os = "windows")]
fn uninstall_service(system: bool) -> Result<()> {
    check_no_system_flag_windows(system)?;

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
fn service_status(system: bool) -> Result<()> {
    check_no_system_flag_windows(system)?;

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
fn start_service(system: bool) -> Result<()> {
    check_no_system_flag_windows(system)?;

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
fn stop_service(system: bool) -> Result<()> {
    check_no_system_flag_windows(system)?;

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
fn restart_service(system: bool) -> Result<()> {
    check_no_system_flag_windows(system)?;
    let _ = stop_service(false);
    // Give it a moment to stop
    std::thread::sleep(std::time::Duration::from_secs(2));
    start_service(false)
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
fn install_service(_system: bool) -> Result<()> {
    anyhow::bail!("Service installation is not supported on this platform")
}

#[cfg(not(any(target_os = "linux", target_os = "macos", target_os = "windows")))]
fn uninstall_service(_system: bool) -> Result<()> {
    anyhow::bail!("Service management is not supported on this platform")
}

#[cfg(not(any(target_os = "linux", target_os = "macos", target_os = "windows")))]
fn service_status(_system: bool) -> Result<()> {
    anyhow::bail!("Service management is not supported on this platform")
}

#[cfg(not(any(target_os = "linux", target_os = "macos", target_os = "windows")))]
fn start_service(_system: bool) -> Result<()> {
    anyhow::bail!("Service management is not supported on this platform")
}

#[cfg(not(any(target_os = "linux", target_os = "macos", target_os = "windows")))]
fn stop_service(_system: bool) -> Result<()> {
    anyhow::bail!("Service management is not supported on this platform")
}

#[cfg(not(any(target_os = "linux", target_os = "macos", target_os = "windows")))]
fn restart_service(_system: bool) -> Result<()> {
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
    fn test_systemd_user_service_file_generation() {
        let binary_path = PathBuf::from("/usr/local/bin/freenet");
        let log_dir = PathBuf::from("/home/test/.local/state/freenet");
        let service_content = generate_user_service_file(&binary_path, &log_dir);

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

        // Verify user service targets default.target
        assert!(service_content.contains("WantedBy=default.target"));
        // User service should NOT have User= directive
        assert!(!service_content.contains("User="));
    }

    #[test]
    #[cfg(target_os = "linux")]
    fn test_systemd_system_service_file_generation() {
        let binary_path = PathBuf::from("/home/test/.local/bin/freenet");
        let log_dir = PathBuf::from("/home/test/.local/state/freenet");
        let username = "testuser";
        let home_dir = PathBuf::from("/home/test");
        let service_content =
            generate_system_service_file(&binary_path, &log_dir, username, &home_dir);

        // Verify sections
        assert!(service_content.contains("[Unit]"));
        assert!(service_content.contains("[Service]"));
        assert!(service_content.contains("[Install]"));

        // Verify User= directive is set
        assert!(service_content.contains("User=testuser"));

        // Verify HOME environment is set (needed for config/data paths)
        assert!(service_content.contains("Environment=HOME=/home/test"));

        // Verify system service targets multi-user.target
        assert!(service_content.contains("WantedBy=multi-user.target"));

        // Verify it still has all the standard settings
        assert!(service_content.contains("Restart=always"));
        assert!(service_content.contains("LimitNOFILE=65536"));
        assert!(service_content.contains("ExecStopPost="));
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
