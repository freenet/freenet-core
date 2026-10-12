//! Diagnostic report generation and upload for debugging.
//!
//! Collects system info, logs, config, and optional problem description,
//! then uploads to the Freenet report server for debugging.

use anyhow::{Context, Result};
use chrono::{DateTime, Duration, Utc};
use clap::Args;
use flate2::Compression;
use flate2::write::GzEncoder;
use freenet::config::ConfigPaths;
use freenet::tracing::tracer::get_log_dir;
use freenet::util::os_trust::{OsTrustSummary, add_os_root_certificates};
use freenet_stdlib::client_api::{
    ClientRequest, HostResponse, NodeDiagnosticsConfig, NodeDiagnosticsResponse, NodeQuery,
    QueryResponse, WebApi,
};
use serde::{Deserialize, Serialize};
use std::fs;
use std::io::{self, BufRead, BufReader, Write};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration as StdDuration;
use tokio_tungstenite::connect_async;

/// Older releases upload to `https://nova.locut.us/api/reports`; the server
/// keeps that route for them.
const DEFAULT_REPORT_SERVER: &str = "https://telemetry.freenet.org/api/reports";
/// Only include log entries from the last 30 minutes
const LOG_RETENTION_MINUTES: i64 = 30;
/// Maximum size of each log stream in the report (2 MB for the main log and
/// 2 MB for the error log)
const MAX_TOTAL_LOG_SIZE: usize = 2 * 1024 * 1024;
/// Maximum length for a single log line (10 KB) - longer lines are truncated
const MAX_LINE_LENGTH: usize = 10 * 1024;
/// Default WebSocket API port
const DEFAULT_WS_API_PORT: u16 = 7509;
/// Timeout for a single WebSocket connect+query attempt. A struggling node
/// is exactly the case we most need diagnostics for, so err on the side of
/// waiting longer rather than dropping the field silently.
const WS_TIMEOUT_SECS: u64 = 15;
/// Number of additional retry attempts after the initial query times out.
/// Connection refused is not retried: that signals the node isn't running.
const WS_RETRY_ATTEMPTS: u32 = 1;
/// Loopback hosts to try, in order. Both are attempted so that
/// `ws-api-address = "::"` combined with `IPV6_V6ONLY=1` (or a v4-only
/// bind) doesn't silently drop the diagnostics.
const LOOPBACK_HOSTS: &[&str] = &["127.0.0.1", "[::1]"];
/// Appended to upload failures. Private, not the public Matrix room: the saved
/// report holds the node's config and recent logs (peer addresses included).
const SEND_LOCALLY_HINT: &str = "To send it another way, re-run with `--local <PATH>` \
     to save it to a file and send that file privately to a Freenet developer, \
     e.g. as a direct message on Matrix (it contains your config and recent logs)";

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
    /// Network status (if node is running and responded successfully)
    pub network_status: Option<String>,
    /// Reason `network_status` is missing (connection refused, timeout, etc.).
    /// Populated whenever `network_status` is `None`, so the report never
    /// silently drops the field without telling us why.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub network_status_error: Option<String>,
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
        config_dirs: Arc<ConfigPaths>,
    ) -> Result<()> {
        println!("Collecting diagnostic info...");

        // Collect all diagnostic data
        let report = self.collect_report(
            version,
            git_commit,
            git_dirty,
            build_timestamp,
            &config_dirs,
        )?;

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
        config_dirs: &ConfigPaths,
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

        let logs = self.collect_logs(config_dirs.log_dir().map(Path::to_path_buf))?;
        let config = self.collect_config(&config_dirs.config_dir());
        let (network_status, network_status_error) = match self.collect_network_status(&config) {
            Ok(diag) => (Some(diag), None),
            Err(e) => (None, Some(e)),
        };
        let user_message = self.get_user_message()?;

        let client_timestamp = chrono::Utc::now().to_rfc3339();

        Ok(DiagnosticReport {
            client_timestamp,
            system_info,
            version_info,
            logs,
            config,
            network_status,
            network_status_error,
            user_message,
        })
    }

    fn collect_logs(&self, log_dir: Option<PathBuf>) -> Result<LogContents> {
        let log_dir = log_dir
            .or_else(get_log_dir)
            .context("Unsupported platform for log collection")?;

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

    fn collect_config(&self, config_dir: &Path) -> Option<String> {
        // Try standard config locations
        let config_paths = [
            Some(config_dir.join("config.toml")),
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

    /// Query the local node for live diagnostics. Returns the serialized
    /// diagnostics on success, or a human-readable error string on failure
    /// so the report can record *why* the field is missing rather than
    /// silently dropping it.
    ///
    /// Worst-case duration is `LOOPBACK_HOSTS.len() * (WS_RETRY_ATTEMPTS + 1) * WS_TIMEOUT_SECS`
    /// seconds. Prints progress so the user isn't left staring at a blank
    /// terminal during a degraded-node query.
    fn collect_network_status(&self, config_content: &Option<String>) -> Result<String, String> {
        let ws_port = config_content
            .as_ref()
            .and_then(|c| parse_ws_port_from_config(c))
            .unwrap_or(DEFAULT_WS_API_PORT);

        let rt = tokio::runtime::Runtime::new()
            .map_err(|e| format!("failed to create tokio runtime: {e}"))?;

        let worst_case_secs =
            LOOPBACK_HOSTS.len() as u64 * (WS_RETRY_ATTEMPTS as u64 + 1) * WS_TIMEOUT_SECS;
        print!("  Querying local node diagnostics (up to {worst_case_secs}s)... ");
        io::stdout().flush().ok();

        let result = rt.block_on(query_with_fallback(
            ws_port,
            WS_RETRY_ATTEMPTS,
            StdDuration::from_secs(WS_TIMEOUT_SECS),
        ));
        match &result {
            Ok(_) => println!("ok"),
            Err(e) => println!("unreachable ({e})"),
        }
        result
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
        println!(
            "What issue are you experiencing? (Enter on empty line to finish, or just Enter to skip)"
        );
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
        match (&report.network_status, &report.network_status_error) {
            (Some(_), _) => println!("  - Node status: running"),
            (None, Some(err)) => println!("  - Node status: unreachable ({err})"),
            (None, None) => println!("  - Node status: not running or unreachable"),
        }
    }

    fn save_local(&self, report: &DiagnosticReport, path: &PathBuf) -> Result<()> {
        let json = serde_json::to_string_pretty(report)?;
        fs::write(path, &json).context("Failed to write report to file")?;
        println!();
        println!("Report saved to: {}", path.display());
        Ok(())
    }

    async fn upload_report(&self, report: &DiagnosticReport) -> Result<()> {
        self.upload_report_with(report, reqwest::Client::builder())
            .await
    }

    /// `base` is a fresh `reqwest::Client::builder()` in production; tests pass
    /// one that ignores proxy settings in the environment.
    async fn upload_report_with(
        &self,
        report: &DiagnosticReport,
        base: reqwest::ClientBuilder,
    ) -> Result<()> {
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
        // OS roots so this works behind TLS-intercepting proxies, whose CA
        // lives in the OS store (see util::os_trust for why only here).
        let (builder, os_trust) = add_os_root_certificates(
            base.user_agent("freenet-report")
                .connect_timeout(StdDuration::from_secs(30))
                .timeout(StdDuration::from_secs(300)),
        );
        let client = builder
            .build()
            .with_context(|| format!("Failed to set up the upload. {SEND_LOCALLY_HINT}"))?;

        let mut response = client
            .post(&self.server)
            .header("Content-Type", "application/json")
            .header("Content-Encoding", "gzip")
            .body(compressed)
            .send()
            .await
            .map_err(|error| upload_send_error(error, &os_trust))?;

        // Capped whatever the status: a captive portal or proxy page can be
        // large, and this upload trusts more CAs than any other client.
        let status = response.status();
        let (body, read_error) = read_capped(&mut response).await;
        if !status.is_success() {
            // Whatever arrived is worth showing, even if the read broke off.
            let shown = printable_error_body(&String::from_utf8_lossy(&body));
            anyhow::bail!("Upload failed: {status} - {shown}. {SEND_LOCALLY_HINT}");
        }
        if let Some(error) = read_error {
            return Err(anyhow::Error::new(error).context(format!(
                "Failed to read upload response. {SEND_LOCALLY_HINT}"
            )));
        }

        // A 200 that is not our JSON is usually a captive portal or proxy page.
        let upload_response: UploadResponse = serde_json::from_slice(&body)
            .with_context(|| format!("Failed to parse upload response. {SEND_LOCALLY_HINT}"))?;
        // The response may come through an interceptor this upload trusts, so
        // never print a code that could drive the terminal.
        let code = validated_report_code(&upload_response.code)
            .with_context(|| format!("Unexpected upload response. {SEND_LOCALLY_HINT}"))?;

        println!(" done");
        println!();
        println!("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");
        println!("  Report code: {code}");
        println!("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");
        println!();
        println!("Share this code with the Freenet team on Matrix.");

        Ok(())
    }
}

/// Context for a failed send, with the trust summary only when it explains the
/// failure (see `OsTrustSummary::explaining`).
fn upload_send_error(error: reqwest::Error, os_trust: &OsTrustSummary) -> anyhow::Error {
    let message = upload_failure_message(os_trust.explaining(&error));
    anyhow::Error::new(error).context(message)
}

fn upload_failure_message(os_trust: Option<&OsTrustSummary>) -> String {
    match os_trust {
        Some(summary) => format!("Failed to upload report ({summary}). {SEND_LOCALLY_HINT}"),
        None => format!("Failed to upload report. {SEND_LOCALLY_HINT}"),
    }
}

/// Upper bound on how much of a response body is read. The real reply is a few
/// bytes of JSON; only the first [`MAX_ERROR_BODY_CHARS`] of an error are shown.
const MAX_RESPONSE_BYTES: usize = 64 * 1024;

/// Reads at most [`MAX_RESPONSE_BYTES`] of the body, returning what arrived
/// and the error that ended the read early, if one did.
async fn read_capped(response: &mut reqwest::Response) -> (Vec<u8>, Option<reqwest::Error>) {
    let mut raw = Vec::new();
    while raw.len() < MAX_RESPONSE_BYTES {
        match response.chunk().await {
            Ok(Some(chunk)) => {
                let room = MAX_RESPONSE_BYTES - raw.len();
                raw.extend_from_slice(&chunk[..chunk.len().min(room)]);
            }
            Ok(None) => break,
            Err(error) => return (raw, Some(error)),
        }
    }
    (raw, None)
}

/// Report codes are short ASCII tokens (six characters from the server).
fn validated_report_code(code: &str) -> Result<&str> {
    anyhow::ensure!(
        (4..=32).contains(&code.len()) && code.bytes().all(|b| b.is_ascii_alphanumeric()),
        "the server returned an unexpected report code"
    );
    Ok(code)
}

/// Maximum characters of an HTTP error body shown to the user.
const MAX_ERROR_BODY_CHARS: usize = 500;

/// Makes a server- or proxy-sent error body safe and short enough to print: a
/// block page can be a whole HTML document, and its bytes must not drive the
/// terminal (escape sequences, line breaks that fake extra output, bidi
/// overrides), so it is shown as one plain line with runs of spaces collapsed.
fn printable_error_body(body: &str) -> String {
    let mut after_space = false;
    // Bounded: the body comes from `read_capped` (at most 64 KiB of bytes).
    let cleaned: String = body
        .chars()
        .filter_map(|c| match c {
            '\n' | '\t' | '\r' | '\u{2028}' | '\u{2029}' | ' ' => Some(' '),
            // Format characters (category Cf) that reorder or hide text, which
            // `is_control` (Cc only) lets through.
            '\u{00AD}'
            | '\u{061C}'
            | '\u{200B}'..='\u{200F}'
            | '\u{202A}'..='\u{202E}'
            | '\u{2060}'..='\u{206F}'
            | '\u{FEFF}'
            | '\u{E0000}'..='\u{E007F}' => None,
            c if c.is_control() => None,
            c => Some(c),
        })
        .filter(|&c| {
            let repeated = c == ' ' && after_space;
            after_space = c == ' ';
            !repeated
        })
        .collect();
    // Trimmed after stripping, so a dropped character next to the edge cannot
    // leave a stray space, nor count as text that was cut off.
    let mut chars = cleaned.trim().chars();
    let mut shown: String = chars.by_ref().take(MAX_ERROR_BODY_CHARS).collect();
    if chars.next().is_some() {
        shown.push('…');
    }
    shown
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
///
/// If the file contains no parseable timestamps (e.g., panic backtraces,
/// stderr output), the entire file is included so that crash diagnostics
/// are never silently discarded.
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
    let mut all_lines = Vec::new();
    let mut include_line = false;
    let mut any_timestamp_found = false;

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
                any_timestamp_found = true;
                include_line = parsed >= cutoff;
            }
        }

        // Truncate very long lines (e.g., delegates logging full state as byte arrays)
        let line = if line.len() > MAX_LINE_LENGTH {
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

        // Track all lines in case we need to fall back to unfiltered content
        all_lines.push(line.clone());

        // Include this line if we're within the time window
        // (lines without timestamps inherit the state from the previous timestamped line)
        if include_line {
            filtered_lines.push(line);
        }
    }

    // If no timestamps were found, include all lines — the file likely contains
    // panic backtraces or other non-timestamped output that is critical for debugging.
    let result_lines = if !any_timestamp_found && !all_lines.is_empty() {
        all_lines
    } else {
        filtered_lines
    };

    if result_lines.is_empty() {
        (None, original_size)
    } else {
        (Some(result_lines.join("\n")), original_size)
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

/// Attempt the diagnostics query against each loopback host in order, with
/// a bounded number of retries on timeout. Returns the serialized
/// diagnostics on success, or a concatenated error string describing every
/// attempt on failure.
///
/// Retries are timeout-only: connection-refused errors are reported
/// immediately since they indicate the node is not running on that host.
///
/// The `per_attempt_timeout` parameter exists for tests; production callers
/// pass `Duration::from_secs(WS_TIMEOUT_SECS)`. Worst-case duration is
/// `LOOPBACK_HOSTS.len() * (retry_attempts + 1) * per_attempt_timeout`.
async fn query_with_fallback(
    port: u16,
    retry_attempts: u32,
    per_attempt_timeout: StdDuration,
) -> Result<String, String> {
    let mut errors: Vec<String> = Vec::new();
    let total_attempts = retry_attempts.saturating_add(1);

    for host in LOOPBACK_HOSTS {
        for attempt in 0..total_attempts {
            match tokio::time::timeout(per_attempt_timeout, query_node_diagnostics(host, port))
                .await
            {
                Ok(Ok(diag)) => return Ok(diag),
                Ok(Err(e)) => {
                    // Connection-level failure (refused, no route, etc.).
                    // Don't retry the same host: the error won't change.
                    errors.push(format!("{host}:{port}: {e:#}"));
                    break;
                }
                Err(_) => {
                    errors.push(format!(
                        "{host}:{port}: timed out after {:?} (attempt {}/{})",
                        per_attempt_timeout,
                        attempt + 1,
                        total_attempts
                    ));
                }
            }
        }
    }

    Err(errors.join("; "))
}

/// Query the node for diagnostics via WebSocket API.
async fn query_node_diagnostics(host: &str, port: u16) -> Result<String> {
    let url = format!("ws://{host}:{port}/v1/contract/command?encodingProtocol=native");

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

    // Close connection gracefully; ignore errors since we're done
    let _disconnect = client.send(ClientRequest::Disconnect { cause: None }).await;

    match response {
        HostResponse::QueryResponse(QueryResponse::NodeDiagnostics(diag)) => {
            diagnostics_to_json(&diag)
        }
        HostResponse::ContractResponse(_)
        | HostResponse::DelegateResponse { .. }
        | HostResponse::QueryResponse(_)
        | HostResponse::Ok
        | _ => anyhow::bail!("Unexpected response from node"),
    }
}

/// Serialize `NodeDiagnosticsResponse` to JSON for the report payload.
///
/// Since freenet-stdlib 0.8.0 (`fix!: stringify
/// NodeDiagnosticsResponse.contract_states keys`, freenet-stdlib#70),
/// `contract_states` is keyed by `String`, so the whole struct serializes
/// natively with `serde_json` — no manual key stringification is needed.
/// This previously had to rebuild `contract_states` by hand because the map
/// was keyed by `ContractKey`, whose derived `Serialize` emits a struct and
/// `serde_json` rejects non-string JSON map keys (freenet/freenet-core#3987).
fn diagnostics_to_json(diag: &NodeDiagnosticsResponse) -> Result<String> {
    serde_json::to_string_pretty(diag).context("Failed to serialize diagnostics")
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

    /// tests/tls_native_roots.rs proves `add_os_root_certificates` makes a
    /// client trust a TLS-intercepting proxy's CA, and that the real rejection
    /// is what `OsTrustSummary::explaining` recognises. This pins the wiring in
    /// the binary, which no test can reach over TLS: without it, uploads fail
    /// behind such proxies with `UnknownIssuer` and those tests stay green.
    #[test]
    fn upload_client_is_built_with_os_root_certificates() {
        let src = include_str!("report.rs");
        // Production hands the upload a fresh builder (tests pass one that
        // ignores proxy settings)...
        let entry = item_code(src, "    async fn upload_report(", "\n    }\n");
        assert!(
            entry.contains("self.upload_report_with(report,reqwest::Client::builder())"),
            "upload_report must start from a fresh client builder"
        );
        let code = item_code(src, "    async fn upload_report_with(", "\n    }\n");
        // ...which gets the OS roots. Built with concat! so this file's own
        // text never contains the needle.
        let helper_call = concat!("let(builder,os_trust)=add_os_root", "_certificates(base.");
        assert!(
            code.contains(helper_call),
            "the upload must build its client with add_os_root_certificates"
        );
        // ...and that builder must be the one that POSTs: one client, built
        // from it, with no second client or builder to replace it.
        assert!(code.contains("letclient=builder.build().with_context("));
        assert_eq!(code.matches("letclient").count(), 1, "a second client");
        assert_eq!(code.matches("Client::").count(), 0, "a second client");
        assert_eq!(
            code.matches("ClientBuilder").count(),
            1,
            "a second builder (the one expected is the `base` parameter)"
        );
        assert!(!code.contains("builder="), "the builder replaced");
        assert!(code.contains("client.post("));
        assert!(
            code.contains("upload_send_error(error,&os_trust)"),
            "send failures lose the trust summary"
        );
        let send_error = item_code(src, "fn upload_send_error(", "\n}\n");
        assert!(
            send_error.contains("upload_failure_message(os_trust.explaining(&error))"),
            "the summary is no longer chosen by OsTrustSummary::explaining"
        );
    }

    /// Code-only text of the item starting at `signature` (outside the test
    /// module): comment lines dropped and whitespace removed, so a rustfmt
    /// reflow cannot break or satisfy a pin.
    fn item_code(src: &str, signature: &str, end: &str) -> String {
        let tests_at = src.find("#[cfg(test)]").expect("test module marker");
        let start = src
            .find(signature)
            .unwrap_or_else(|| panic!("`{signature}` moved; update this pin"));
        assert!(start < tests_at, "`{signature}` matched inside the tests");
        let len = src[start..].find(end).expect("end of item");
        src[start..start + len]
            .lines()
            .filter(|line| !line.trim_start().starts_with("//"))
            .flat_map(str::chars)
            .filter(|c| !c.is_whitespace())
            .collect()
    }

    fn sample_report() -> DiagnosticReport {
        DiagnosticReport {
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
                main_log: None,
                error_log: None,
                main_log_size_bytes: 0,
                error_log_size_bytes: 0,
                main_log_original_size_bytes: 0,
                error_log_original_size_bytes: 0,
            },
            config: None,
            network_status: None,
            network_status_error: None,
            user_message: None,
        }
    }

    /// Runs the real upload against a local server answering with `status` and
    /// `body`. Plain HTTP, so it covers everything after the TLS handshake;
    /// proxy settings in the environment are ignored so they cannot intercept.
    async fn upload_against(status: u16, body: String) -> Result<()> {
        use httptest::{Expectation, Server, matchers::request, responders::status_code};
        let server = Server::run();
        server.expect(
            Expectation::matching(request::method_path("POST", "/api/reports"))
                .respond_with(status_code(status).body(body)),
        );
        let command = ReportCommand {
            local: None,
            message: None,
            no_message: true,
            server: server.url("/api/reports").to_string(),
        };
        command
            .upload_report_with(&sample_report(), reqwest::Client::builder().no_proxy())
            .await
    }

    /// The upload reads its reply through the cap. Leading whitespace is valid
    /// JSON, so only a capped read can fail on this reply.
    #[tokio::test]
    async fn upload_reads_the_reply_through_the_cap() {
        let padded = format!("{}{{\"code\":\"Q69UF2\"}}", " ".repeat(MAX_RESPONSE_BYTES));
        let error = upload_against(200, padded)
            .await
            .expect_err("the reply lies beyond the cap");
        assert_eq!(
            error.to_string(),
            format!("Failed to parse upload response. {SEND_LOCALLY_HINT}")
        );
    }

    /// Serves one reply whose body stops short of its Content-Length, then
    /// hangs up, so reading the reply fails part-way.
    async fn truncated_reply_server(status_line: &'static str, partial: &'static str) -> String {
        use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt};
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}/api/reports", listener.local_addr().unwrap());
        tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            let mut stream = tokio::io::BufReader::new(stream);
            // Take the whole request first, so the client is reading the reply
            // (not still sending) when the connection drops.
            let mut length = 0;
            loop {
                let mut line = String::new();
                stream.read_line(&mut line).await.unwrap();
                if line == "\r\n" {
                    break;
                }
                if let Some(value) = line.to_ascii_lowercase().strip_prefix("content-length:") {
                    length = value.trim().parse().unwrap();
                }
            }
            let mut request_body = vec![0; length];
            stream.read_exact(&mut request_body).await.unwrap();
            let reply = format!(
                "{status_line}\r\ncontent-length: {}\r\n\r\n{partial}",
                partial.len() + 100
            );
            stream.get_mut().write_all(reply.as_bytes()).await.unwrap();
        });
        url
    }

    async fn upload_to(url: String) -> Result<()> {
        let command = ReportCommand {
            local: None,
            message: None,
            no_message: true,
            server: url,
        };
        command
            .upload_report_with(&sample_report(), reqwest::Client::builder().no_proxy())
            .await
    }

    /// Even with the whole code received, a reply that breaks off is reported
    /// as a failed read, not accepted or blamed on the reply's format.
    #[tokio::test]
    async fn upload_reports_a_reply_that_breaks_off() {
        let url = truncated_reply_server("HTTP/1.1 200 OK", r#"{"code":"Q69UF2"}"#).await;
        let error = upload_to(url)
            .await
            .expect_err("a broken-off reply is not a success");
        assert_eq!(
            error.to_string(),
            format!("Failed to read upload response. {SEND_LOCALLY_HINT}")
        );
    }

    /// For an error status, whatever arrived is still shown.
    #[tokio::test]
    async fn upload_shows_what_arrived_of_an_error_page_that_breaks_off() {
        let url = truncated_reply_server("HTTP/1.1 403 Forbidden", "Blocked by policy").await;
        let error = upload_to(url).await.expect_err("a 403 fails the upload");
        assert_eq!(
            error.to_string(),
            format!("Upload failed: 403 Forbidden - Blocked by policy. {SEND_LOCALLY_HINT}")
        );
    }

    #[tokio::test]
    async fn upload_accepts_a_valid_code() {
        upload_against(200, r#"{"code":"Q69UF2"}"#.into())
            .await
            .expect("a well-formed reply is accepted");
    }

    #[tokio::test]
    async fn upload_failure_shows_a_capped_sanitised_body_and_the_hint() {
        let page = format!(
            "\u{1b}[2J<html>\n  <body>{}</body>",
            "x".repeat(MAX_RESPONSE_BYTES * 2)
        );
        let error = upload_against(403, page)
            .await
            .expect_err("a 403 fails the upload")
            .to_string();
        assert!(!error.contains('\u{1b}'), "{error}");
        assert!(
            error.starts_with("Upload failed: 403 Forbidden - [2J<html> <body>xxx"),
            "{error}"
        );
        assert!(error.contains("…."), "truncation not marked: {error}");
        assert!(error.ends_with(SEND_LOCALLY_HINT), "{error}");
    }

    #[tokio::test]
    async fn upload_rejects_a_code_that_could_drive_the_terminal() {
        let error = upload_against(200, r#"{"code":"\u001b]0;x\u0007"}"#.into())
            .await
            .expect_err("an escape sequence is not a report code");
        assert_eq!(
            error.to_string(),
            format!("Unexpected upload response. {SEND_LOCALLY_HINT}")
        );
    }

    #[tokio::test]
    async fn upload_rejects_a_success_page_that_is_not_the_reply() {
        let error = upload_against(200, "<html>Sign in to the network</html>".into())
            .await
            .expect_err("a captive portal page is not a reply");
        assert_eq!(
            error.to_string(),
            format!("Failed to parse upload response. {SEND_LOCALLY_HINT}")
        );
    }

    #[tokio::test]
    async fn response_read_is_capped() {
        use httptest::{Expectation, Server, matchers::request, responders::status_code};
        let server = Server::run();
        server.expect(
            Expectation::matching(request::method_path("GET", "/"))
                .respond_with(status_code(200).body("x".repeat(MAX_RESPONSE_BYTES * 2 + 7))),
        );
        let mut response = reqwest::Client::builder()
            .no_proxy()
            .build()
            .unwrap()
            .get(server.url("/").to_string())
            .send()
            .await
            .unwrap();
        let (body, read_error) = read_capped(&mut response).await;
        assert!(read_error.is_none());
        assert_eq!(body.len(), MAX_RESPONSE_BYTES);
    }

    #[test]
    fn upload_failure_shows_the_trust_summary_only_when_given() {
        let summary = OsTrustSummary {
            found: 3,
            added: 2,
            ..OsTrustSummary::default()
        };
        assert_eq!(
            upload_failure_message(Some(&summary)),
            format!(
                "Failed to upload report (2 of 3 OS trust store certificates added). \
                 {SEND_LOCALLY_HINT}"
            )
        );
        assert_eq!(
            upload_failure_message(None),
            format!("Failed to upload report. {SEND_LOCALLY_HINT}")
        );
    }

    /// A refused connection is a connect error (`is_connect()`), like a TLS
    /// rejection, but the trust summary would mislead there.
    #[tokio::test]
    async fn refused_connection_does_not_show_the_trust_summary() {
        // Bound but never listening: the port stays ours, and connecting to
        // it is refused, with no window for another socket to take it.
        let reserved = tokio::net::TcpSocket::new_v4().unwrap();
        reserved.bind("127.0.0.1:0".parse().unwrap()).unwrap();
        let port = reserved.local_addr().unwrap().port();
        let error = reqwest::Client::builder()
            .no_proxy()
            .timeout(StdDuration::from_secs(10))
            .build()
            .unwrap()
            .get(format!("http://127.0.0.1:{port}/"))
            .send()
            .await
            .expect_err("nothing listens on the reserved port");
        assert!(error.is_connect());
        let summary = OsTrustSummary {
            found: 3,
            added: 2,
            ..OsTrustSummary::default()
        };
        assert_eq!(
            upload_send_error(error, &summary).to_string(),
            upload_failure_message(None)
        );
    }

    #[test]
    fn report_codes_are_short_ascii_tokens() {
        assert_eq!(validated_report_code("Q69UF2").unwrap(), "Q69UF2");
        assert!(validated_report_code("ABCD").is_ok());
        assert!(validated_report_code(&"A".repeat(32)).is_ok());
        for bad in [
            "",
            "ABC",
            "\u{1b}]0;pwned\u{7}",
            "ABC DEF",
            "ABC\nDEF",
            "ÄBCDEF",
        ] {
            assert!(validated_report_code(bad).is_err(), "{bad:?} accepted");
        }
        assert!(validated_report_code(&"A".repeat(33)).is_err());
    }

    #[test]
    fn printable_error_body_keeps_terminal_control_out_and_marks_truncation() {
        assert_eq!(printable_error_body(""), "");
        // Each character that could drive or fake terminal output, including
        // both ends of every stripped range and the bidi isolates inside one.
        for c in [
            '\u{1b}',
            '\u{7f}',
            '\u{9b}',
            '\u{ad}',
            '\u{61c}',
            '\u{200b}',
            '\u{200f}',
            '\u{202a}',
            '\u{202b}',
            '\u{202e}',
            '\u{2060}',
            '\u{2066}',
            '\u{2069}',
            '\u{206f}',
            '\u{feff}',
            '\u{e0000}',
            '\u{e007f}',
        ] {
            assert_eq!(printable_error_body(&format!("a{c}b")), "ab", "{c:?} kept");
        }
        // Line breaks and tabs become spaces, so the body cannot fake lines,
        // and runs of them collapse so indentation does not use up the budget.
        for c in ['\n', '\r', '\t', '\u{2028}', '\u{2029}'] {
            assert_eq!(printable_error_body(&format!("a{c}b")), "a b", "{c:?}");
        }
        assert_eq!(printable_error_body("a \n\t  \r\n b"), "a b");
        // Leading and trailing whitespace (an error page's final newline) is
        // dropped rather than shown before the hint.
        assert_eq!(printable_error_body("\n  Forbidden\n"), "Forbidden");
        assert_eq!(
            printable_error_body("\u{1b} Forbidden \u{200b}"),
            "Forbidden"
        );
        let exactly = "x".repeat(MAX_ERROR_BODY_CHARS);
        assert_eq!(
            printable_error_body(&format!("{exactly} \u{200b}")),
            exactly,
            "only stripped characters followed: nothing was cut off"
        );
        // Ordinary text, including non-ASCII, passes through untouched.
        let text = "日本語 é\u{a0}x\u{ae}";
        assert_eq!(printable_error_body(text), text);
        let exactly = "é".repeat(MAX_ERROR_BODY_CHARS);
        assert_eq!(printable_error_body(&exactly), exactly);
        let over = "é".repeat(MAX_ERROR_BODY_CHARS + 1);
        assert_eq!(printable_error_body(&over), format!("{exactly}…"));
        // Stripped characters do not count toward the limit.
        let padded = format!("{}{exactly}", "\u{1b}".repeat(10));
        assert_eq!(printable_error_body(&padded), exactly);
    }

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
            network_status_error: None,
            user_message: Some("Test message".to_string()),
        };

        let json = serde_json::to_string(&report).unwrap();
        assert!(json.contains("linux"));
        assert!(json.contains("test log"));
    }

    #[test]
    fn test_diagnostics_to_json_with_populated_contract_states() {
        // Regression for #3987: serde_json::to_string_pretty(&NodeDiagnosticsResponse)
        // panicked with "key must be a string" when contract_states had any entries,
        // because ContractKey derives Serialize as a struct (not a string) and
        // serde_json forbids non-string JSON object keys. Every report from a node
        // hosting at least one contract was silently uploaded with empty
        // network_status. The fix builds JSON manually with stringified keys.
        use freenet_stdlib::client_api::ContractState;
        use freenet_stdlib::prelude::{CodeHash, ContractInstanceId, ContractKey};
        use std::collections::HashMap;

        let key1 = ContractKey::from_id_and_code(
            ContractInstanceId::new([1u8; 32]),
            CodeHash::new([2u8; 32]),
        );
        let key2 = ContractKey::from_id_and_code(
            ContractInstanceId::new([3u8; 32]),
            CodeHash::new([4u8; 32]),
        );
        let mut contract_states = HashMap::new();
        contract_states.insert(
            key1.to_string(),
            ContractState {
                subscribers: 2,
                subscriber_peer_ids: vec!["peer-a".to_string(), "peer-b".to_string()],
                size_bytes: 1234,
            },
        );
        contract_states.insert(
            key2.to_string(),
            ContractState {
                subscribers: 0,
                subscriber_peer_ids: vec![],
                size_bytes: 0,
            },
        );

        let diag = NodeDiagnosticsResponse {
            node_info: None,
            network_info: None,
            subscriptions: vec![],
            contract_states,
            system_metrics: None,
            connected_peers_detailed: vec![],
        };

        let json = diagnostics_to_json(&diag).expect("serialization must not fail");
        let parsed: serde_json::Value =
            serde_json::from_str(&json).expect("output must be valid JSON");

        // Field is preserved.
        let states = parsed["contract_states"]
            .as_object()
            .expect("contract_states must be a JSON object");
        assert_eq!(states.len(), 2);

        // Keys are the Base58 contract id (matches ContractKey's Display impl,
        // which is what the report viewer expects), not the struct form
        // {"instance":..., "code":...} that the broken derive produced.
        assert!(states.contains_key(&key1.to_string()));
        assert!(states.contains_key(&key2.to_string()));

        // Values round-trip.
        assert_eq!(states[&key1.to_string()]["subscribers"], 2);
        assert_eq!(states[&key1.to_string()]["size_bytes"], 1234);
    }

    #[test]
    fn test_native_serde_json_on_string_keyed_contract_states_succeeds() {
        // Regression guard for freenet/freenet-core#3993: since
        // freenet-stdlib 0.8.0 stringified `contract_states` keys
        // (freenet-stdlib#70), the pre-0.8 tripwire that asserted native
        // `serde_json::to_string` *fails* on a ContractKey-keyed map no
        // longer applies. Its inversion — native serialization must now
        // *succeed* and preserve the stringified key — is what lets
        // `diagnostics_to_json` be a thin `serde_json::to_string_pretty`
        // wrapper. If a future stdlib bump regresses the key type back to a
        // non-string, this test trips before the report path silently breaks.
        use freenet_stdlib::client_api::ContractState;
        use freenet_stdlib::prelude::{CodeHash, ContractInstanceId, ContractKey};
        use std::collections::HashMap;

        let key = ContractKey::from_id_and_code(
            ContractInstanceId::new([1u8; 32]),
            CodeHash::new([2u8; 32]),
        );
        let mut contract_states = HashMap::new();
        contract_states.insert(
            key.to_string(),
            ContractState {
                subscribers: 0,
                subscriber_peer_ids: vec![],
                size_bytes: 0,
            },
        );
        let diag = NodeDiagnosticsResponse {
            node_info: None,
            network_info: None,
            subscriptions: vec![],
            contract_states,
            system_metrics: None,
            connected_peers_detailed: vec![],
        };

        let json = serde_json::to_string(&diag)
            .expect("native serde_json must accept String-keyed contract_states");
        let parsed: serde_json::Value =
            serde_json::from_str(&json).expect("output must be valid JSON");
        assert!(
            parsed["contract_states"]
                .as_object()
                .expect("contract_states must be a JSON object")
                .contains_key(&key.to_string()),
            "stringified contract key must survive native serialization, got: {json}",
        );
    }

    #[test]
    fn test_diagnostics_to_json_with_empty_contract_states() {
        // The bug only triggered when contract_states had >=1 entry, but the
        // empty case is the path users with no hosted contracts hit. Asserting
        // both keeps either case from regressing.
        let diag = NodeDiagnosticsResponse {
            node_info: None,
            network_info: None,
            subscriptions: vec![],
            contract_states: std::collections::HashMap::new(),
            system_metrics: None,
            connected_peers_detailed: vec![],
        };

        let json = diagnostics_to_json(&diag).expect("serialization must not fail");
        let parsed: serde_json::Value =
            serde_json::from_str(&json).expect("output must be valid JSON");
        assert!(parsed["contract_states"].as_object().unwrap().is_empty());
    }

    #[test]
    fn test_diagnostics_to_json_all_fields_populated_round_trip() {
        // Guard against the derived serialization dropping or mangling a
        // field: every Option must be Some, every Vec must be non-empty, and
        // every top-level key must round-trip with a distinguishable value.
        // If a field is renamed, dropped, or gains a serde attribute that
        // changes the wire shape, the missing/renamed field surfaces here
        // instead of in production reports.
        use freenet_stdlib::client_api::{
            ConnectedPeerInfo, ContractState, NetworkInfo, NodeInfo, SubscriptionInfo,
            SystemMetrics,
        };
        use freenet_stdlib::prelude::{CodeHash, ContractInstanceId, ContractKey};
        use std::collections::HashMap;

        let key = ContractKey::from_id_and_code(
            ContractInstanceId::new([7u8; 32]),
            CodeHash::new([8u8; 32]),
        );
        let mut contract_states = HashMap::new();
        contract_states.insert(
            key.to_string(),
            ContractState {
                subscribers: 5,
                subscriber_peer_ids: vec!["peer-z".to_string()],
                size_bytes: 9999,
            },
        );

        let diag = NodeDiagnosticsResponse {
            node_info: Some(NodeInfo {
                peer_id: "peer-self".to_string(),
                is_gateway: true,
                location: Some("0.5".to_string()),
                listening_address: Some("0.0.0.0:31337".to_string()),
                uptime_seconds: 3600,
            }),
            network_info: Some(NetworkInfo {
                connected_peers: vec![("peer-x".to_string(), "10.0.0.1:31337".to_string())],
                active_connections: 1,
            }),
            subscriptions: vec![SubscriptionInfo {
                contract_key: ContractInstanceId::new([7u8; 32]),
                client_id: 42,
            }],
            contract_states,
            system_metrics: Some(SystemMetrics {
                active_connections: 1,
                hosting_contracts: 1,
            }),
            connected_peers_detailed: vec![ConnectedPeerInfo {
                peer_id: "peer-x".to_string(),
                address: "10.0.0.1:31337".to_string(),
            }],
        };

        let json = diagnostics_to_json(&diag).expect("serialization must not fail");
        let parsed: serde_json::Value =
            serde_json::from_str(&json).expect("output must be valid JSON");

        // All six top-level keys present and distinguishable.
        let obj = parsed.as_object().expect("top-level must be object");
        assert_eq!(obj.len(), 6, "expected six top-level fields, got {obj:?}");
        assert_eq!(parsed["node_info"]["peer_id"], "peer-self");
        assert_eq!(parsed["network_info"]["active_connections"], 1);
        assert_eq!(parsed["subscriptions"][0]["client_id"], 42);
        assert_eq!(parsed["system_metrics"]["hosting_contracts"], 1);
        assert_eq!(parsed["connected_peers_detailed"][0]["peer_id"], "peer-x");
        let states = parsed["contract_states"]
            .as_object()
            .expect("contract_states must be a JSON object");
        assert_eq!(states.len(), 1);
        assert_eq!(states[&key.to_string()]["size_bytes"], 9999);
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

    #[test]
    fn test_read_log_file_no_timestamps_includes_all_content() {
        use tempfile::TempDir;

        let temp_dir = TempDir::new().unwrap();
        let log_path = temp_dir.path().join("freenet.error.log");

        // Simulate panic backtrace output (no timestamps)
        let panic_content = "\
thread 'main' panicked at 'called `Result::unwrap()` on an `Err` value: AddrInUse', src/main.rs:42
stack backtrace:
   0: std::panicking::begin_panic_handler
   1: core::panicking::panic_fmt
   2: core::result::unwrap_failed
   3: freenet::main";
        fs::write(&log_path, panic_content).unwrap();

        let (content, original_size) = read_log_file(&log_path);

        assert!(
            content.is_some(),
            "Files with no timestamps should still be included"
        );
        let content = content.unwrap();
        assert!(
            content.contains("panicked"),
            "Panic message should be preserved"
        );
        assert!(
            content.contains("stack backtrace"),
            "Backtrace should be preserved"
        );
        assert_eq!(original_size, panic_content.len() as u64);
    }

    /// Short per-attempt timeout for tests that exercise the timeout path.
    /// 200ms is long enough to avoid CI flakes but short enough that a
    /// retry test can run multiple attempts in under a second.
    const TEST_TIMEOUT: StdDuration = StdDuration::from_millis(200);

    /// Regression for issue #3897: when no node is listening, the report must
    /// surface a concrete error instead of silently setting `network_status`
    /// to `None`. Uses a port that nothing is bound to so `connect_async`
    /// fails with connection-refused.
    #[tokio::test]
    async fn test_query_with_fallback_connection_refused_reports_error() {
        // Pick a port unlikely to have anything listening on loopback.
        // If this ever flakes, a real listener on that port is the only
        // explanation and the test is still telling us something useful.
        let port = 59111;

        let result = query_with_fallback(port, 0, TEST_TIMEOUT).await;

        let err = result.expect_err("no listener → must return Err, not silent None");
        assert!(
            err.contains("127.0.0.1") && err.contains("[::1]"),
            "error should mention BOTH loopback hosts, got: {err}"
        );
        assert!(
            err.contains(&port.to_string()),
            "error should include the port number, got: {err}"
        );
        // Must NOT be empty: the whole point is no silent failure.
        assert!(!err.is_empty());
    }

    /// Regression for issue #3897: when the connect step hangs forever, the
    /// query must time out and the error must identify timeout (not empty,
    /// not connection-refused wording). Uses a TCP listener that accepts
    /// the connection but never sends any WebSocket handshake bytes, so
    /// tokio-tungstenite's `connect_async` blocks on the handshake until
    /// our timeout fires.
    #[tokio::test]
    async fn test_query_with_fallback_timeout_reports_error() {
        use tokio::net::TcpListener;

        // Bind on IPv4 loopback only. The [::1] attempt will fail with
        // connection-refused immediately; we only need one of the two
        // attempts to hit the timeout path.
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();

        // Accept the connection but never write any handshake bytes, so
        // `connect_async` blocks until the per-attempt timeout fires.
        // Holding `_handle` keeps the accept task alive.
        let _handle = tokio::spawn(async move {
            loop {
                let (_stream, _) = match listener.accept().await {
                    Ok(v) => v,
                    Err(_) => break,
                };
                // Keep the stream alive so the client's handshake hangs.
                std::mem::forget(_stream);
            }
        });

        let result = query_with_fallback(port, 0, TEST_TIMEOUT).await;

        let err = result.expect_err("handshake never completes → must return Err");
        assert!(
            err.contains("timed out") || err.contains("timeout"),
            "error should identify timeout, got: {err}"
        );
        assert!(
            err.contains(&port.to_string()),
            "error should include the port number, got: {err}"
        );
    }

    /// Regression for issue #3897: verify `retry_attempts > 0` actually
    /// causes a second attempt when the first times out. The production
    /// config passes `WS_RETRY_ATTEMPTS=1`, so this path must be exercised
    /// by the test suite.
    #[tokio::test]
    async fn test_query_with_fallback_retries_on_timeout() {
        use std::sync::Arc;
        use std::sync::atomic::{AtomicUsize, Ordering};
        use tokio::net::TcpListener;

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();

        let accepts = Arc::new(AtomicUsize::new(0));
        let accepts_clone = accepts.clone();

        // Accept every connection and hang (never complete handshake).
        let _handle = tokio::spawn(async move {
            loop {
                let (_stream, _) = match listener.accept().await {
                    Ok(v) => v,
                    Err(_) => break,
                };
                accepts_clone.fetch_add(1, Ordering::SeqCst);
                std::mem::forget(_stream);
            }
        });

        // retry_attempts=1 → 2 attempts per host × 1 host that accepts = 2 timeouts.
        // ([::1] gets connection-refused immediately, not counted in accepts.)
        let result = query_with_fallback(port, 1, TEST_TIMEOUT).await;
        assert!(result.is_err());

        // Give the accept task a moment to register both connections.
        tokio::time::sleep(StdDuration::from_millis(50)).await;

        let total_accepts = accepts.load(Ordering::SeqCst);
        assert_eq!(
            total_accepts, 2,
            "retry_attempts=1 must produce 2 v4 accepts (initial + 1 retry); got {total_accepts}"
        );

        // Error string must mention both attempts so operators can see the
        // retry actually happened.
        let err = result.unwrap_err();
        assert!(
            err.contains("attempt 1/2") && err.contains("attempt 2/2"),
            "error should identify both attempts, got: {err}"
        );
    }

    /// Serialization must round-trip the new `network_status_error` field
    /// and omit it from JSON when `None` so existing report schemas stay
    /// unchanged on the server side.
    #[test]
    fn test_network_status_error_serialization() {
        let base_report = || DiagnosticReport {
            client_timestamp: "2026-04-17T00:00:00Z".to_string(),
            system_info: SystemInfo {
                os: "linux".into(),
                arch: "x86_64".into(),
                hostname: "test".into(),
            },
            version_info: VersionInfo {
                version: "0.2.46".into(),
                git_commit: "abc".into(),
                git_dirty: false,
                build_timestamp: "".into(),
            },
            logs: LogContents {
                main_log: None,
                error_log: None,
                main_log_size_bytes: 0,
                error_log_size_bytes: 0,
                main_log_original_size_bytes: 0,
                error_log_original_size_bytes: 0,
            },
            config: None,
            network_status: None,
            network_status_error: None,
            user_message: None,
        };

        // None case: field must be omitted from JSON so older server
        // deserializers don't see an unexpected key.
        let report = base_report();
        let json = serde_json::to_string(&report).unwrap();
        assert!(
            !json.contains("network_status_error"),
            "None should be skipped; got {json}"
        );

        // Some case: field must appear in JSON.
        let mut report = base_report();
        report.network_status_error = Some("timed out after 15s".to_string());
        let json = serde_json::to_string(&report).unwrap();
        assert!(
            json.contains("network_status_error"),
            "Some should appear in JSON; got {json}"
        );
        assert!(json.contains("timed out after 15s"));

        // Round-trip deserialization preserves the error string.
        let restored: DiagnosticReport = serde_json::from_str(&json).unwrap();
        assert_eq!(
            restored.network_status_error.as_deref(),
            Some("timed out after 15s")
        );
    }

    #[test]
    fn test_read_log_file_old_timestamps_excluded() {
        use tempfile::TempDir;

        let temp_dir = TempDir::new().unwrap();
        let log_path = temp_dir.path().join("freenet.log");

        // All timestamps are old (> 30 minutes ago) — should be filtered out
        let old_content = "\
2020-01-01T00:00:00.000000Z  INFO freenet: Old log entry 1
2020-01-01T00:00:01.000000Z  INFO freenet: Old log entry 2";
        fs::write(&log_path, old_content).unwrap();

        let (content, original_size) = read_log_file(&log_path);

        assert!(
            content.is_none(),
            "Old timestamped entries should be filtered out"
        );
        assert_eq!(original_size, old_content.len() as u64);
    }
}
