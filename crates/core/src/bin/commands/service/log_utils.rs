// `anyhow::{Context, Result}` is only used by the cfg-gated `tail_with_rotation`;
// `Path` is used by the unconditional `find_latest_log_file`.
#[cfg(any(target_os = "linux", target_os = "macos"))]
use anyhow::{Context, Result};
use std::path::Path;

/// Tail log files with automatic rotation detection.
///
/// Spawns `tail -f` on the latest log file, then periodically checks (every 5s)
/// whether a newer log file has appeared. When rotation occurs (e.g., hourly
/// tracing-appender rotation), kills the old `tail` and starts a new one on
/// the new file.
#[cfg(any(target_os = "linux", target_os = "macos"))]
pub(crate) fn tail_with_rotation(log_dir: &Path, base_name: &str) -> Result<()> {
    use std::time::Duration;

    let mut current_log = find_latest_log_file(log_dir, base_name).ok_or_else(|| {
        anyhow::anyhow!(
            "No log files found in: {}\nMake sure the service has been installed and started.",
            log_dir.display()
        )
    })?;

    println!("Following logs from: {}", current_log.display());
    println!("Press Ctrl+C to stop.\n");

    loop {
        let mut child = std::process::Command::new("tail")
            .arg("-f")
            .arg(&current_log)
            .spawn()
            .context("Failed to spawn tail")?;

        // Poll for newer log files every 5 seconds
        loop {
            match child.try_wait() {
                Ok(Some(status)) => {
                    // tail exited (user Ctrl+C or error)
                    std::process::exit(status.code().unwrap_or(1_i32));
                }
                Ok(None) => {
                    // tail still running, check for rotation
                }
                Err(e) => {
                    drop(child.kill());
                    drop(child.wait());
                    anyhow::bail!("Error waiting on tail process: {e}");
                }
            }

            std::thread::sleep(Duration::from_secs(5));

            if let Some(newer_log) = find_latest_log_file(log_dir, base_name) {
                if newer_log != current_log {
                    println!("\n--- Log rotated to: {} ---\n", newer_log.display());
                    drop(child.kill());
                    drop(child.wait());
                    current_log = newer_log;
                    break; // break inner loop to spawn new tail
                }
            }
        }
    }
}

/// Find the latest log file in the given directory.
/// Handles both static files (e.g., "freenet.log" from systemd) and
/// rotated files (e.g., "freenet.2025-12-27.log" from tracing-appender).
/// Returns the most recently modified file.
pub(crate) fn find_latest_log_file(log_dir: &Path, base_name: &str) -> Option<std::path::PathBuf> {
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
