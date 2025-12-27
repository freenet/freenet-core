//! Diagnostic report generation and upload for debugging.
//!
//! Collects system info, logs, config, and optional problem description,
//! then uploads to the Freenet report server for debugging.

use anyhow::{Context, Result};
use clap::Args;
use flate2::write::GzEncoder;
use flate2::Compression;
use serde::{Deserialize, Serialize};
use std::fs;
use std::io::{self, BufRead, Write};
use std::path::PathBuf;

const DEFAULT_REPORT_SERVER: &str = "https://nova.locut.us/api/reports";

#[derive(Args, Debug, Clone)]
pub struct ReportCommand {
    /// Save report locally instead of uploading
    #[arg(long, value_name = "PATH")]
    pub local: Option<PathBuf>,

    /// Problem description (skips interactive prompt)
    #[arg(long, short = 'm')]
    pub message: Option<String>,

    /// Skip problem description prompt
    #[arg(long)]
    pub no_message: bool,

    /// Override upload server URL
    #[arg(long, default_value = DEFAULT_REPORT_SERVER)]
    pub server: String,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct DiagnosticReport {
    /// Client timestamp for clock skew detection
    pub client_timestamp: String,
    /// System information
    pub system_info: SystemInfo,
    /// Version and build info
    pub version_info: VersionInfo,
    /// Log file contents
    pub logs: LogContents,
    /// Config file contents (if available)
    pub config: Option<String>,
    /// Network status (if node is running)
    pub network_status: Option<String>,
    /// User's problem description
    pub user_message: Option<String>,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct SystemInfo {
    pub os: String,
    pub arch: String,
    pub hostname: String,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct VersionInfo {
    pub version: String,
    pub git_commit: String,
    pub git_dirty: bool,
    pub build_timestamp: String,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct LogContents {
    pub main_log: Option<String>,
    pub error_log: Option<String>,
    pub main_log_size_bytes: u64,
    pub error_log_size_bytes: u64,
}

#[derive(Deserialize, Debug)]
struct UploadResponse {
    code: String,
}

impl ReportCommand {
    pub fn run(
        &self,
        version: &str,
        git_commit: &str,
        git_dirty: &str,
        build_timestamp: &str,
    ) -> Result<()> {
        println!("Collecting diagnostic info...");

        // Collect all diagnostic data
        let report = self.collect_report(version, git_commit, git_dirty, build_timestamp)?;

        // Print summary
        self.print_summary(&report);

        // Handle local save or upload
        if let Some(ref path) = self.local {
            self.save_local(&report, path)?;
        } else {
            let rt = tokio::runtime::Runtime::new()?;
            rt.block_on(self.upload_report(&report))?;
        }

        Ok(())
    }

    fn collect_report(
        &self,
        version: &str,
        git_commit: &str,
        git_dirty: &str,
        build_timestamp: &str,
    ) -> Result<DiagnosticReport> {
        let system_info = SystemInfo {
            os: std::env::consts::OS.to_string(),
            arch: std::env::consts::ARCH.to_string(),
            hostname: hostname::get()
                .map(|h| h.to_string_lossy().to_string())
                .unwrap_or_else(|_| "unknown".to_string()),
        };

        let version_info = VersionInfo {
            version: version.to_string(),
            git_commit: git_commit.to_string(),
            git_dirty: git_dirty == " (dirty)",
            build_timestamp: build_timestamp.to_string(),
        };

        let logs = self.collect_logs()?;
        let config = self.collect_config();
        let network_status = self.collect_network_status();
        let user_message = self.get_user_message()?;

        let client_timestamp = chrono::Utc::now().to_rfc3339();

        Ok(DiagnosticReport {
            client_timestamp,
            system_info,
            version_info,
            logs,
            config,
            network_status,
            user_message,
        })
    }

    fn collect_logs(&self) -> Result<LogContents> {
        let log_dir = get_log_dir()?;

        let main_log_path = log_dir.join("freenet.log");
        let error_log_path = log_dir.join("freenet.error.log");

        let (main_log, main_log_size) = read_log_file(&main_log_path);
        let (error_log, error_log_size) = read_log_file(&error_log_path);

        Ok(LogContents {
            main_log,
            error_log,
            main_log_size_bytes: main_log_size,
            error_log_size_bytes: error_log_size,
        })
    }

    fn collect_config(&self) -> Option<String> {
        // Try standard config locations
        let config_paths = [
            dirs::config_dir().map(|p| p.join("freenet").join("config.toml")),
            dirs::home_dir().map(|p| p.join(".config").join("freenet").join("config.toml")),
        ];

        for path in config_paths.into_iter().flatten() {
            if path.exists() {
                if let Ok(content) = fs::read_to_string(&path) {
                    return Some(content);
                }
            }
        }

        None
    }

    fn collect_network_status(&self) -> Option<String> {
        // Try to query the local node's WebSocket API
        // This is optional - if the node isn't running, we just skip this
        // For now, we'll leave this as None and implement it when we have
        // the WebSocket client available
        // TODO: Query ws://127.0.0.1:50509 for node diagnostics
        None
    }

    fn get_user_message(&self) -> Result<Option<String>> {
        // Check for --message flag
        if let Some(ref msg) = self.message {
            return Ok(Some(msg.clone()));
        }

        // Check for --no-message flag
        if self.no_message {
            return Ok(None);
        }

        // Interactive prompt
        println!();
        println!("What issue are you experiencing? (Enter on empty line to finish, or just Enter to skip)");
        print!("> ");
        io::stdout().flush()?;

        let stdin = io::stdin();
        let mut lines = Vec::new();

        for line in stdin.lock().lines() {
            let line = line.context("Failed to read input")?;

            if line.is_empty() {
                // Empty line = done
                break;
            }

            lines.push(line);
            print!("> ");
            io::stdout().flush()?;
        }

        if lines.is_empty() {
            Ok(None)
        } else {
            Ok(Some(lines.join("\n")))
        }
    }

    fn print_summary(&self, report: &DiagnosticReport) {
        println!(
            "  - Version: {} ({}{})",
            report.version_info.version,
            report.version_info.git_commit,
            if report.version_info.git_dirty {
                " dirty"
            } else {
                ""
            }
        );
        println!(
            "  - OS: {} {}",
            report.system_info.os, report.system_info.arch
        );

        let total_log_size = report.logs.main_log_size_bytes + report.logs.error_log_size_bytes;
        println!("  - Logs: {}", format_bytes(total_log_size));

        println!(
            "  - Config: {}",
            if report.config.is_some() {
                "found"
            } else {
                "not found"
            }
        );
        println!(
            "  - Node status: {}",
            if report.network_status.is_some() {
                "running"
            } else {
                "not running or unreachable"
            }
        );
    }

    fn save_local(&self, report: &DiagnosticReport, path: &PathBuf) -> Result<()> {
        let json = serde_json::to_string_pretty(report)?;
        fs::write(path, &json).context("Failed to write report to file")?;
        println!();
        println!("Report saved to: {}", path.display());
        Ok(())
    }

    async fn upload_report(&self, report: &DiagnosticReport) -> Result<()> {
        println!();
        print!("Uploading report...");
        io::stdout().flush()?;

        // Serialize to JSON
        let json = serde_json::to_vec(report)?;

        // Gzip compress
        let mut encoder = GzEncoder::new(Vec::new(), Compression::default());
        encoder.write_all(&json)?;
        let compressed = encoder.finish()?;

        // Upload
        let client = reqwest::Client::builder()
            .user_agent("freenet-report")
            .build()?;

        let response = client
            .post(&self.server)
            .header("Content-Type", "application/json")
            .header("Content-Encoding", "gzip")
            .body(compressed)
            .send()
            .await
            .context("Failed to upload report")?;

        if !response.status().is_success() {
            let status = response.status();
            let body = response.text().await.unwrap_or_default();
            anyhow::bail!("Upload failed: {} - {}", status, body);
        }

        let upload_response: UploadResponse = response
            .json()
            .await
            .context("Failed to parse upload response")?;

        println!(" done");
        println!();
        println!("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");
        println!("  Report code: {}", upload_response.code);
        println!("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");
        println!();
        println!("Share this code with the Freenet team on Matrix.");

        Ok(())
    }
}

fn get_log_dir() -> Result<PathBuf> {
    #[cfg(target_os = "linux")]
    {
        Ok(dirs::home_dir()
            .context("Failed to get home directory")?
            .join(".local/state/freenet"))
    }

    #[cfg(target_os = "macos")]
    {
        Ok(dirs::home_dir()
            .context("Failed to get home directory")?
            .join("Library/Logs/freenet"))
    }

    #[cfg(target_os = "windows")]
    {
        Ok(dirs::data_local_dir()
            .context("Failed to get local app data directory")?
            .join("freenet")
            .join("logs"))
    }

    #[cfg(not(any(target_os = "linux", target_os = "macos", target_os = "windows")))]
    {
        anyhow::bail!("Unsupported platform for log collection")
    }
}

fn read_log_file(path: &PathBuf) -> (Option<String>, u64) {
    match fs::metadata(path) {
        Ok(metadata) => {
            let size = metadata.len();
            match fs::read_to_string(path) {
                Ok(content) => (Some(content), size),
                Err(_) => (None, size),
            }
        }
        Err(_) => (None, 0),
    }
}

fn format_bytes(bytes: u64) -> String {
    const KB: u64 = 1024;
    const MB: u64 = KB * 1024;

    if bytes >= MB {
        format!("{:.1} MB", bytes as f64 / MB as f64)
    } else if bytes >= KB {
        format!("{:.1} KB", bytes as f64 / KB as f64)
    } else {
        format!("{} bytes", bytes)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_format_bytes() {
        assert_eq!(format_bytes(0), "0 bytes");
        assert_eq!(format_bytes(500), "500 bytes");
        assert_eq!(format_bytes(1024), "1.0 KB");
        assert_eq!(format_bytes(1536), "1.5 KB");
        assert_eq!(format_bytes(1048576), "1.0 MB");
        assert_eq!(format_bytes(1572864), "1.5 MB");
    }

    #[test]
    fn test_system_info() {
        let info = SystemInfo {
            os: std::env::consts::OS.to_string(),
            arch: std::env::consts::ARCH.to_string(),
            hostname: "test".to_string(),
        };
        assert!(!info.os.is_empty());
        assert!(!info.arch.is_empty());
    }

    #[test]
    fn test_report_serialization() {
        let report = DiagnosticReport {
            client_timestamp: "2025-01-01T00:00:00Z".to_string(),
            system_info: SystemInfo {
                os: "linux".to_string(),
                arch: "x86_64".to_string(),
                hostname: "test".to_string(),
            },
            version_info: VersionInfo {
                version: "0.1.0".to_string(),
                git_commit: "abc123".to_string(),
                git_dirty: false,
                build_timestamp: "2025-01-01".to_string(),
            },
            logs: LogContents {
                main_log: Some("test log".to_string()),
                error_log: None,
                main_log_size_bytes: 8,
                error_log_size_bytes: 0,
            },
            config: None,
            network_status: None,
            user_message: Some("Test message".to_string()),
        };

        let json = serde_json::to_string(&report).unwrap();
        assert!(json.contains("linux"));
        assert!(json.contains("test log"));
    }
}
