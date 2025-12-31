//! Auto-update detection for Freenet peers.
//!
//! When a peer detects a version mismatch with another peer (typically the gateway),
//! it checks GitHub to verify a newer version exists before exiting with a special
//! exit code. This prevents malicious peers from triggering exits by claiming
//! fake version numbers.
//!
//! This is temporary alpha-testing infrastructure to reduce the burden of
//! frequent updates during rapid development.

use anyhow::Result;
use semver::Version;
use std::fs;
use std::path::PathBuf;
use std::time::{Duration, SystemTime};

// Re-export version mismatch detection from transport layer
pub use freenet::transport::{clear_version_mismatch, has_version_mismatch};

/// Exit code that signals "update needed and verified against GitHub".
/// The service wrapper catches this and runs `freenet update` before restarting.
pub const EXIT_CODE_UPDATE_NEEDED: i32 = 42;

/// Minimum interval between update checks (1 hour).
const UPDATE_CHECK_INTERVAL: Duration = Duration::from_secs(3600);

/// Maximum consecutive update failures before disabling auto-update.
const MAX_UPDATE_FAILURES: u32 = 3;

/// GitHub API URL for latest release.
const GITHUB_API_URL: &str = "https://api.github.com/repos/freenet/freenet-core/releases/latest";

/// Error returned when an update is needed.
/// The main function catches this and exits with EXIT_CODE_UPDATE_NEEDED.
#[derive(Debug)]
pub struct UpdateNeededError {
    pub new_version: String,
}

impl std::fmt::Display for UpdateNeededError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "Update available: version {} is available on GitHub. Exiting for auto-update.",
            self.new_version
        )
    }
}

impl std::error::Error for UpdateNeededError {}

/// Result of an update check attempt.
#[derive(Debug, PartialEq)]
pub enum UpdateCheckResult {
    /// Rate limited or too many failures - didn't actually check GitHub.
    /// The caller should NOT clear the version mismatch flag (preserve it for later).
    Skipped,
    /// Checked GitHub, no newer version available.
    /// The caller should clear the version mismatch flag.
    NoUpdateAvailable,
    /// Checked GitHub, newer version confirmed.
    /// The caller should clear the version mismatch flag.
    UpdateAvailable(String),
}

/// Check if an update is available, respecting rate limits and failure counts.
///
/// Returns an `UpdateCheckResult` indicating:
/// - `Skipped` if rate limited or too many failures (didn't check GitHub)
/// - `NoUpdateAvailable` if checked but no newer version exists
/// - `UpdateAvailable(version)` if a newer version is confirmed on GitHub
///
/// IMPORTANT: The caller should only clear the version mismatch flag if the
/// result is NOT `Skipped`. This prevents losing mismatches when rate-limited.
///
/// Security: This function verifies against GitHub, so a malicious peer
/// claiming a fake version won't trigger an exit.
pub async fn check_if_update_available(current_version: &str) -> UpdateCheckResult {
    // Don't check if we've failed too many times
    if !should_attempt_update() {
        tracing::debug!(
            failures = get_update_failure_count(),
            max = MAX_UPDATE_FAILURES,
            "Skipping update check - too many previous failures"
        );
        return UpdateCheckResult::Skipped;
    }

    // Rate limit checks
    if !should_check_for_update() {
        tracing::debug!("Skipping update check - checked recently");
        return UpdateCheckResult::Skipped;
    }

    // Record that we're checking now
    record_check_time();

    // Fetch latest version from GitHub
    match get_latest_version().await {
        Ok(latest) => {
            let current = match Version::parse(current_version) {
                Ok(v) => v,
                Err(e) => {
                    tracing::warn!(
                        "Failed to parse current version '{}': {}",
                        current_version,
                        e
                    );
                    return UpdateCheckResult::NoUpdateAvailable;
                }
            };

            let latest_ver = match Version::parse(&latest) {
                Ok(v) => v,
                Err(e) => {
                    tracing::warn!("Failed to parse latest version '{}': {}", latest, e);
                    return UpdateCheckResult::NoUpdateAvailable;
                }
            };

            if latest_ver > current {
                tracing::info!(
                    current = %current_version,
                    latest = %latest,
                    "Newer version confirmed on GitHub"
                );
                // Clear failure count since we successfully checked
                clear_update_failures();
                UpdateCheckResult::UpdateAvailable(latest)
            } else {
                tracing::debug!(
                    current = %current_version,
                    latest = %latest,
                    "No newer version available (peer may have claimed fake version)"
                );
                UpdateCheckResult::NoUpdateAvailable
            }
        }
        Err(e) => {
            tracing::warn!(
                "Failed to check GitHub for updates: {}. Continuing to run.",
                e
            );
            // Treat network errors as "checked but no update" - don't preserve the flag
            // forever just because GitHub was temporarily unreachable
            UpdateCheckResult::NoUpdateAvailable
        }
    }
}

/// Fetch the latest version string from GitHub releases API.
async fn get_latest_version() -> Result<String> {
    let client = reqwest::Client::builder()
        .user_agent("freenet-updater")
        .timeout(Duration::from_secs(10))
        .build()?;

    let response = client.get(GITHUB_API_URL).send().await?;

    if !response.status().is_success() {
        anyhow::bail!("GitHub API returned {}", response.status());
    }

    #[derive(serde::Deserialize)]
    struct Release {
        tag_name: String,
    }

    let release: Release = response.json().await?;
    Ok(release.tag_name.trim_start_matches('v').to_string())
}

/// Get the state directory for update tracking files.
fn state_dir() -> Option<PathBuf> {
    dirs::home_dir().map(|h| h.join(".local/state/freenet"))
}

/// Get the last time we checked for updates.
fn get_last_check_time() -> Option<SystemTime> {
    let marker = state_dir()?.join("last_update_check");
    fs::metadata(&marker).ok()?.modified().ok()
}

/// Record that we just checked for updates.
fn record_check_time() {
    if let Some(dir) = state_dir() {
        let _ = fs::create_dir_all(&dir);
        let marker = dir.join("last_update_check");
        let _ = fs::write(&marker, "");
    }
}

/// Check if enough time has passed since the last update check.
fn should_check_for_update() -> bool {
    match get_last_check_time() {
        Some(last) => last
            .elapsed()
            .map(|e| e > UPDATE_CHECK_INTERVAL)
            .unwrap_or(true),
        None => true,
    }
}

/// Get the number of consecutive update failures.
fn get_update_failure_count() -> u32 {
    let marker = match state_dir() {
        Some(d) => d.join("update_failures"),
        None => return 0,
    };

    fs::read_to_string(marker)
        .ok()
        .and_then(|s| s.trim().parse().ok())
        .unwrap_or(0)
}

/// Record an update failure.
/// This should be called by the update command when an update fails.
/// After MAX_UPDATE_FAILURES consecutive failures, auto-update is disabled
/// until a successful manual update clears the counter.
#[allow(dead_code)] // Will be wired up to update command in follow-up
pub fn record_update_failure() {
    if let Some(dir) = state_dir() {
        let _ = fs::create_dir_all(&dir);
        let count = get_update_failure_count() + 1;
        let _ = fs::write(dir.join("update_failures"), count.to_string());
    }
}

/// Clear the update failure count (called on successful update check).
pub fn clear_update_failures() {
    if let Some(dir) = state_dir() {
        let _ = fs::remove_file(dir.join("update_failures"));
    }
}

/// Check if we should attempt an update based on failure history.
fn should_attempt_update() -> bool {
    get_update_failure_count() < MAX_UPDATE_FAILURES
}

#[cfg(test)]
mod tests {
    use super::*;
    use freenet::transport::signal_version_mismatch;

    #[test]
    fn test_version_mismatch_flag() {
        // Clear any previous state
        clear_version_mismatch();
        assert!(!has_version_mismatch());

        // Signal a mismatch
        signal_version_mismatch();
        assert!(has_version_mismatch());

        // Clear it
        clear_version_mismatch();
        assert!(!has_version_mismatch());
    }

    #[test]
    fn test_update_needed_error_display() {
        let err = UpdateNeededError {
            new_version: "0.1.74".to_string(),
        };
        let msg = format!("{}", err);
        assert!(msg.contains("0.1.74"));
        assert!(msg.contains("auto-update"));
    }
}
