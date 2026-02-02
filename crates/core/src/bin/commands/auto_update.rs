//! Auto-update detection for Freenet peers.
//!
//! When a peer detects a version mismatch with another peer (typically the gateway),
//! it checks GitHub to verify a newer version exists before exiting with a special
//! exit code. This prevents malicious peers from triggering exits by claiming
//! fake version numbers.
//!
//! Uses exponential backoff for GitHub API checks: starts at 1 minute after first
//! mismatch detection, doubles after each check that finds no update, up to 1 hour max.
//! This ensures peers update promptly when a release is published without spamming
//! the GitHub API.
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

/// Initial backoff interval for update checks (1 minute).
const INITIAL_BACKOFF: Duration = Duration::from_secs(60);

/// Maximum backoff interval for update checks (1 hour).
const MAX_BACKOFF: Duration = Duration::from_secs(3600);

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
    /// Rate limited, too many failures, or no update available yet - will retry later.
    /// The caller should NOT clear the version mismatch flag (preserve it for retry).
    Skipped,
    /// Checked GitHub, newer version confirmed.
    /// The caller should clear the version mismatch flag.
    UpdateAvailable(String),
}

/// Check if an update is available, respecting rate limits and failure counts.
///
/// Returns an `UpdateCheckResult` indicating:
/// - `Skipped` if rate limited, too many failures, or no update available yet (will retry)
/// - `UpdateAvailable(version)` if a newer version is confirmed on GitHub
///
/// Uses exponential backoff: after each check that finds no update, the backoff
/// interval doubles (starting at 1 minute, max 1 hour). This handles the case where
/// a gateway is running a pre-release version before the GitHub release is published.
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

    // Check if enough time has passed according to current backoff
    let current_backoff = get_current_backoff();
    if !should_check_for_update(current_backoff) {
        tracing::debug!(
            backoff_secs = current_backoff.as_secs(),
            "Skipping update check - backoff not elapsed"
        );
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
                    // Increase backoff and retry later
                    increase_backoff();
                    return UpdateCheckResult::Skipped;
                }
            };

            let latest_ver = match Version::parse(&latest) {
                Ok(v) => v,
                Err(e) => {
                    tracing::warn!("Failed to parse latest version '{}': {}", latest, e);
                    // Increase backoff and retry later
                    increase_backoff();
                    return UpdateCheckResult::Skipped;
                }
            };

            if latest_ver > current {
                tracing::info!(
                    current = %current_version,
                    latest = %latest,
                    "Newer version confirmed on GitHub"
                );
                // Clear failure count and backoff since we found an update
                clear_update_failures();
                reset_backoff();
                UpdateCheckResult::UpdateAvailable(latest)
            } else {
                tracing::debug!(
                    current = %current_version,
                    latest = %latest,
                    backoff_secs = current_backoff.as_secs(),
                    "No newer version on GitHub yet, will retry with increased backoff"
                );
                // No update yet - increase backoff and keep the mismatch flag for retry
                increase_backoff();
                UpdateCheckResult::Skipped
            }
        }
        Err(e) => {
            tracing::warn!(
                "Failed to check GitHub for updates: {}. Will retry with increased backoff.",
                e
            );
            // Network error - increase backoff and retry later
            increase_backoff();
            UpdateCheckResult::Skipped
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

/// Get the current backoff interval from file, defaulting to INITIAL_BACKOFF.
fn get_current_backoff() -> Duration {
    let marker = match state_dir() {
        Some(d) => d.join("update_backoff_secs"),
        None => return INITIAL_BACKOFF,
    };

    fs::read_to_string(marker)
        .ok()
        .and_then(|s| s.trim().parse::<u64>().ok())
        .map(Duration::from_secs)
        .unwrap_or(INITIAL_BACKOFF)
}

/// Increase the backoff interval (double it, up to MAX_BACKOFF).
fn increase_backoff() {
    if let Some(dir) = state_dir() {
        let _ = fs::create_dir_all(&dir);
        let current = get_current_backoff();
        let new_backoff = std::cmp::min(current * 2, MAX_BACKOFF);
        let _ = fs::write(
            dir.join("update_backoff_secs"),
            new_backoff.as_secs().to_string(),
        );
    }
}

/// Reset backoff to initial value (called when update is found).
pub fn reset_backoff() {
    if let Some(dir) = state_dir() {
        let _ = fs::remove_file(dir.join("update_backoff_secs"));
    }
}

/// Check if enough time has passed since the last update check.
fn should_check_for_update(backoff: Duration) -> bool {
    match get_last_check_time() {
        Some(last) => last.elapsed().map(|e| e > backoff).unwrap_or(true),
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

    #[test]
    fn test_backoff_constants() {
        // Verify backoff progression: 1m -> 2m -> 4m -> 8m -> 16m -> 32m -> 64m (capped to 60m)
        assert_eq!(INITIAL_BACKOFF, Duration::from_secs(60));
        assert_eq!(MAX_BACKOFF, Duration::from_secs(3600));

        // Doubling 60 six times: 60 -> 120 -> 240 -> 480 -> 960 -> 1920 -> 3840 (capped to 3600)
        let mut backoff = INITIAL_BACKOFF;
        for _ in 0..6 {
            backoff = std::cmp::min(backoff * 2, MAX_BACKOFF);
        }
        assert_eq!(backoff, MAX_BACKOFF);
    }
}
