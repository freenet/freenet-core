//! Diagnostic report generation and upload for debugging.
//!
//! Collects system info, logs, config, and optional problem description,
//! then uploads to the Freenet report server for debugging.

use anyhow::{Context, Result};
use chrono::{DateTime, Duration, Utc};
use clap::Args;
use flate2::write::GzEncoder;
use flate2::Compression;
use freenet::tracing::tracer::get_log_dir;
use freenet_stdlib::client_api::{
    ClientRequest, HostResponse, NodeDiagnosticsConfig, NodeQuery, QueryResponse, WebApi,
};
use serde::{Deserialize, Serialize};
use std::fs;
use std::io::{self, BufRead, BufReader, Write};
use std::path::PathBuf;
use std::time::Duration as StdDuration;
use tokio_tungstenite::connect_async;

const DEFAULT_REPORT_SERVER: &str = "https://nova.locut.us/api/reports";
/// Only include log entries from the last 30 minutes
const LOG_RETENTION_MINUTES: i64 = 30;
/// Maximum size for all logs combined in the report (2 MB)
const MAX_TOTAL_LOG_SIZE: usize = 2 * 1024 * 1024;
/// Maximum length for a single log line (10 KB) - longer lines are truncated
const MAX_LINE_LENGTH: usize = 10 * 1024;
/// Default WebSocket API port
const DEFAULT_WS_API_PORT: u16 = 7509;
/// Timeout for WebSocket connection and queries
const WS_TIMEOUT_SECS: u64 = 5;

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
    /// Size of the filtered log content included in the report
    pub main_log_size_bytes: u64,
    /// Size of the filtered error log content included in the report
    pub error_log_size_bytes: u64,
    /// Original size of the main log file on disk
    #[serde(default)]
    pub main_log_original_size_bytes: u64,
    /// Original size of the error log file on disk
    #[serde(default)]
    pub error_log_original_size_bytes: u64,
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
        let network_status = self.collect_network_status(&config);
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
        let log_dir = get_log_dir().context("Unsupported platform for log collection")?;

        // Find log files - support both legacy names and rolling log patterns
        let main_log_files = find_log_files(&log_dir, "freenet");
        let error_log_files = find_log_files(&log_dir, "freenet.error");

        let (main_log, main_log_original_size) = read_and_merge_log_files(&main_log_files);
        let (error_log, error_log_original_size) = read_and_merge_log_files(&error_log_files);

        // Calculate filtered content sizes
        let main_log_size = main_log.as_ref().map(|s| s.len() as u64).unwrap_or(0);
        let error_log_size = error_log.as_ref().map(|s| s.len() as u64).unwrap_or(0);

        Ok(LogContents {
            main_log,
            error_log,
            main_log_size_bytes: main_log_size,
            error_log_size_bytes: error_log_size,
            main_log_original_size_bytes: main_log_original_size,
            error_log_original_size_bytes: error_log_original_size,
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

    fn collect_network_status(&self, config_content: &Option<String>) -> Option<String> {
        // Try to query the local node's WebSocket API
        // This is optional - if the node isn't running, we just skip this

        // Parse the WebSocket port from config, or use default
        let ws_port = config_content
            .as_ref()
            .and_then(|c| parse_ws_port_from_config(c))
            .unwrap_or(DEFAULT_WS_API_PORT);

        // Create a runtime for the async WebSocket query
        let rt = match tokio::runtime::Runtime::new() {
            Ok(rt) => rt,
            Err(_) => return None,
        };

        rt.block_on(async {
            match tokio::time::timeout(
                StdDuration::from_secs(WS_TIMEOUT_SECS),
                query_node_diagnostics(ws_port),
            )
            .await
            {
                Ok(Ok(diagnostics)) => Some(diagnostics),
                Ok(Err(_)) | Err(_) => None,
            }
        })
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

        let filtered_size = report.logs.main_log_size_bytes + report.logs.error_log_size_bytes;
        let original_size =
            report.logs.main_log_original_size_bytes + report.logs.error_log_original_size_bytes;
        if original_size > filtered_size && filtered_size > 0 {
            println!(
                "  - Logs: {} (last {} min, {} total on disk)",
                format_bytes(filtered_size),
                LOG_RETENTION_MINUTES,
                format_bytes(original_size)
            );
        } else {
            println!("  - Logs: {}", format_bytes(original_size));
        }

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

/// Find log files matching the given prefix.
/// Supports both legacy format (freenet.log) and rolling format (freenet.YYYY-MM-DD.log).
/// Returns files sorted by modification time (newest first).
fn find_log_files(log_dir: &PathBuf, prefix: &str) -> Vec<PathBuf> {
    let mut files = Vec::new();

    // Check for legacy file first
    let legacy_path = log_dir.join(format!("{}.log", prefix));
    if legacy_path.exists() {
        files.push(legacy_path);
    }

    // Look for rolling log files (freenet.YYYY-MM-DD.log or freenet.YYYY-MM-DD-HH.log pattern)
    if let Ok(entries) = fs::read_dir(log_dir) {
        for entry in entries.flatten() {
            let path = entry.path();
            if let Some(name) = path.file_name().and_then(|n| n.to_str()) {
                // Match pattern: prefix.YYYY-MM-DD.log (daily) or prefix.YYYY-MM-DD-HH.log (hourly)
                if name.starts_with(prefix)
                    && name.ends_with(".log")
                    && name.len() > prefix.len() + 5
                {
                    // Check if it has a date pattern (daily: 11 chars, hourly: 14 chars)
                    let middle = &name[prefix.len()..name.len() - 4];
                    if middle.starts_with('.') && (middle.len() == 11 || middle.len() == 14) {
                        // .YYYY-MM-DD or .YYYY-MM-DD-HH
                        files.push(path);
                    }
                }
            }
        }
    }

    // Sort by modification time, newest first
    files.sort_by(|a, b| {
        let a_time = fs::metadata(a).and_then(|m| m.modified()).ok();
        let b_time = fs::metadata(b).and_then(|m| m.modified()).ok();
        b_time.cmp(&a_time)
    });

    files
}

/// Read and merge multiple log files, filtering to the last 30 minutes.
/// Returns (merged_content, total_original_size).
/// Applies MAX_TOTAL_LOG_SIZE limit, keeping most recent entries if exceeded.
fn read_and_merge_log_files(files: &[PathBuf]) -> (Option<String>, u64) {
    if files.is_empty() {
        return (None, 0);
    }

    let mut total_original_size = 0u64;
    let mut all_content = Vec::new();

    // Process files in reverse order (oldest first) so merged string is chronological.
    // This way, most recent entries are at the end, and truncating from the beginning
    // preserves the most recent (most relevant) logs.
    for file in files.iter().rev() {
        let (content, size) = read_log_file(file);
        total_original_size += size;
        if let Some(content) = content {
            all_content.push(content);
        }
    }

    if all_content.is_empty() {
        return (None, total_original_size);
    }

    let merged = all_content.join("\n");

    // If total size exceeds limit, truncate from beginning to keep most recent logs
    let result = if merged.len() > MAX_TOTAL_LOG_SIZE {
        let skip_bytes = merged.len() - MAX_TOTAL_LOG_SIZE;
        // Find a safe UTF-8 boundary, then find the next newline to avoid cutting mid-line
        let safe_skip = merged
            .char_indices()
            .take_while(|(i, _)| *i <= skip_bytes)
            .last()
            .map(|(i, _)| i)
            .unwrap_or(0);
        let truncate_at = merged[safe_skip..]
            .find('\n')
            .map(|pos| safe_skip + pos + 1)
            .unwrap_or(safe_skip);
        format!(
            "[... {} bytes truncated to fit size limit ...]\n{}",
            truncate_at,
            &merged[truncate_at..]
        )
    } else {
        merged
    };

    (Some(result), total_original_size)
}

/// Read log file, filtering to entries from the last 30 minutes.
/// Returns (filtered_content, original_file_size).
fn read_log_file(path: &PathBuf) -> (Option<String>, u64) {
    let metadata = match fs::metadata(path) {
        Ok(m) => m,
        Err(_) => return (None, 0),
    };
    let original_size = metadata.len();

    let file = match fs::File::open(path) {
        Ok(f) => f,
        Err(_) => return (None, original_size),
    };

    let cutoff = Utc::now() - Duration::minutes(LOG_RETENTION_MINUTES);

    let reader = BufReader::new(file);
    let mut filtered_lines = Vec::new();
    let mut include_line = false;

    for line in reader.lines() {
        let line = match line {
            Ok(l) => l,
            Err(_) => continue,
        };

        // Try to extract timestamp from this line
        // Log format: optional ANSI codes, then ISO 8601 timestamp like 2025-12-26T17:28:28.636476Z
        if let Some(ts) = extract_timestamp(&line) {
            if let Ok(parsed) = DateTime::parse_from_rfc3339(&ts)
                .map(|dt| dt.with_timezone(&Utc))
                .or_else(|_| ts.parse::<DateTime<Utc>>())
            {
                include_line = parsed >= cutoff;
            }
        }

        // Include this line if we're within the time window
        // (lines without timestamps inherit the state from the previous timestamped line)
        if include_line {
            // Truncate very long lines (e.g., delegates logging full state as byte arrays)
            let line = if line.len() > MAX_LINE_LENGTH {
                // Find a safe UTF-8 character boundary for truncation
                let truncate_at = line
                    .char_indices()
                    .take_while(|(i, _)| *i < MAX_LINE_LENGTH)
                    .last()
                    .map(|(i, c)| i + c.len_utf8())
                    .unwrap_or(0);
                format!(
                    "{}... [truncated, {} total bytes]",
                    &line[..truncate_at],
                    line.len()
                )
            } else {
                line
            };
            filtered_lines.push(line);
        }
    }

    if filtered_lines.is_empty() {
        (None, original_size)
    } else {
        (Some(filtered_lines.join("\n")), original_size)
    }
}

/// Extract ISO 8601 timestamp from a log line.
/// Handles ANSI escape codes that may surround the timestamp.
fn extract_timestamp(line: &str) -> Option<String> {
    // Skip any leading ANSI escape sequences
    let mut chars = line.chars().peekable();
    while chars.peek() == Some(&'\x1b') {
        // Skip escape sequence: ESC [ ... m
        chars.next(); // ESC
        if chars.next() != Some('[') {
            break;
        }
        for c in chars.by_ref() {
            if c == 'm' {
                break;
            }
        }
    }

    // Collect remaining string and look for timestamp pattern
    let remaining: String = chars.collect();

    // Look for YYYY-MM-DDTHH:MM:SS pattern
    if remaining.len() < 19 {
        return None;
    }

    // Check if it starts with a valid timestamp format
    let potential = &remaining[..std::cmp::min(30, remaining.len())];
    if potential.len() >= 19
        && potential.chars().nth(4) == Some('-')
        && potential.chars().nth(7) == Some('-')
        && potential.chars().nth(10) == Some('T')
        && potential.chars().nth(13) == Some(':')
        && potential.chars().nth(16) == Some(':')
    {
        // Find the end of the timestamp (up to Z or space or ANSI escape)
        let end = potential.find([' ', '\x1b']).unwrap_or(potential.len());
        let ts = &potential[..end];
        // Ensure it ends with Z for RFC3339 compatibility
        if ts.ends_with('Z') {
            return Some(ts.to_string());
        } else {
            // Add Z if missing (some formats omit it)
            return Some(format!("{}Z", ts));
        }
    }

    None
}

/// Parse the WebSocket API port from config TOML content.
fn parse_ws_port_from_config(config: &str) -> Option<u16> {
    // Look for [ws_api] section with port
    // Format: [ws_api]\n...\nws-api-port = 7509
    // or just: ws-api-port = 7509
    for line in config.lines() {
        let line = line.trim();
        if line.starts_with("ws-api-port") || line.starts_with("ws_api_port") {
            if let Some(value) = line.split('=').nth(1) {
                if let Ok(port) = value.trim().parse::<u16>() {
                    return Some(port);
                }
            }
        }
    }
    None
}

/// Query the node for diagnostics via WebSocket API.
async fn query_node_diagnostics(port: u16) -> Result<String> {
    let url = format!("ws://127.0.0.1:{port}/v1/contract/command?encodingProtocol=native");

    let (stream, _) = connect_async(&url)
        .await
        .context("Failed to connect to node WebSocket API")?;

    let mut client = WebApi::start(stream);

    // Query for full node diagnostics
    let config = NodeDiagnosticsConfig {
        include_node_info: true,
        include_network_info: true,
        include_subscriptions: true,
        contract_keys: vec![],
        include_system_metrics: true,
        include_detailed_peer_info: true,
        include_subscriber_peer_ids: false,
    };

    client
        .send(ClientRequest::NodeQueries(NodeQuery::NodeDiagnostics {
            config,
        }))
        .await
        .context("Failed to send diagnostics query")?;

    let response = client
        .recv()
        .await
        .context("Failed to receive diagnostics response")?;

    // Close connection gracefully
    let _ = client.send(ClientRequest::Disconnect { cause: None }).await;

    match response {
        HostResponse::QueryResponse(QueryResponse::NodeDiagnostics(diag)) => {
            // Serialize the diagnostics to JSON for the report
            serde_json::to_string_pretty(&diag).context("Failed to serialize diagnostics")
        }
        _ => anyhow::bail!("Unexpected response from node"),
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
                main_log_original_size_bytes: 100,
                error_log_original_size_bytes: 0,
            },
            config: None,
            network_status: None,
            user_message: Some("Test message".to_string()),
        };

        let json = serde_json::to_string(&report).unwrap();
        assert!(json.contains("linux"));
        assert!(json.contains("test log"));
    }

    #[test]
    fn test_extract_timestamp_plain() {
        // Plain timestamp without ANSI codes
        let line = "2025-12-26T17:28:28.636476Z INFO freenet: Starting";
        assert_eq!(
            extract_timestamp(line),
            Some("2025-12-26T17:28:28.636476Z".to_string())
        );
    }

    #[test]
    fn test_extract_timestamp_with_ansi() {
        // Timestamp surrounded by ANSI escape codes (as in actual logs)
        let line = "\x1b[2m2025-12-26T17:28:28.636476Z\x1b[0m \x1b[32m INFO\x1b[0m freenet";
        assert_eq!(
            extract_timestamp(line),
            Some("2025-12-26T17:28:28.636476Z".to_string())
        );
    }

    #[test]
    fn test_extract_timestamp_no_timestamp() {
        // Line without timestamp
        let line = "    at crates/core/src/bin/freenet.rs:136";
        assert_eq!(extract_timestamp(line), None);
    }

    #[test]
    fn test_extract_timestamp_adds_z_if_missing() {
        // Timestamp without trailing Z
        let line = "2025-12-26T17:28:28.636476 INFO";
        let result = extract_timestamp(line);
        assert!(result.is_some());
        assert!(result.unwrap().ends_with('Z'));
    }

    #[test]
    fn test_parse_ws_port_from_config() {
        // Test with ws-api-port
        let config = r#"
mode = "network"
[ws_api]
ws-api-port = 8080
"#;
        assert_eq!(parse_ws_port_from_config(config), Some(8080));

        // Test with underscore variant
        let config = "ws_api_port = 9000";
        assert_eq!(parse_ws_port_from_config(config), Some(9000));

        // Test with no port
        let config = "mode = \"network\"";
        assert_eq!(parse_ws_port_from_config(config), None);
    }

    #[test]
    fn test_find_log_files_patterns() {
        use std::fs;
        use tempfile::TempDir;

        let temp_dir = TempDir::new().unwrap();
        let log_dir = temp_dir.path().to_path_buf();

        // Create test files matching different log patterns
        fs::write(log_dir.join("freenet.log"), "legacy").unwrap();
        fs::write(log_dir.join("freenet.2025-12-26.log"), "daily").unwrap();
        fs::write(log_dir.join("freenet.2025-12-26-14.log"), "hourly").unwrap();
        fs::write(log_dir.join("freenet.error.log"), "error legacy").unwrap();
        fs::write(
            log_dir.join("freenet.error.2025-12-26-14.log"),
            "error hourly",
        )
        .unwrap();
        fs::write(log_dir.join("other.log"), "unrelated").unwrap();

        // Test finding main log files
        let main_files = find_log_files(&log_dir, "freenet");
        let main_names: Vec<_> = main_files
            .iter()
            .filter_map(|p| p.file_name())
            .filter_map(|n| n.to_str())
            .collect();

        assert!(
            main_names.contains(&"freenet.log"),
            "Should find legacy format"
        );
        assert!(
            main_names.contains(&"freenet.2025-12-26.log"),
            "Should find daily format"
        );
        assert!(
            main_names.contains(&"freenet.2025-12-26-14.log"),
            "Should find hourly format"
        );
        assert!(
            !main_names.iter().any(|n| n.contains("error")),
            "Should not match error logs with 'freenet' prefix"
        );
        assert!(
            !main_names.contains(&"other.log"),
            "Should not match unrelated files"
        );

        // Test finding error log files
        let error_files = find_log_files(&log_dir, "freenet.error");
        let error_names: Vec<_> = error_files
            .iter()
            .filter_map(|p| p.file_name())
            .filter_map(|n| n.to_str())
            .collect();

        assert!(
            error_names.contains(&"freenet.error.log"),
            "Should find error legacy format"
        );
        assert!(
            error_names.contains(&"freenet.error.2025-12-26-14.log"),
            "Should find error hourly format"
        );
    }
}
