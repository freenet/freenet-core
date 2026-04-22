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

pub use freenet::transport::{
    clear_version_mismatch, get_open_connection_count, has_version_mismatch,
    version_mismatch_generation,
};

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
                // Reset the GitHub-check backoff so the next version bump is
                // noticed promptly. Deliberately do NOT clear the update
                // failure count here: that must only be reset by an actual
                // successful install (see `record_update_failure` /
                // `clear_update_failures` call sites in `commands::update`),
                // otherwise every peer-mismatch check would wipe the
                // failure tally and the `MAX_UPDATE_FAILURES` gate could
                // never trigger — which is what let #3934's exit-42 loop
                // run unbounded.
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
        let _mkdir = fs::create_dir_all(&dir);
        let marker = dir.join("last_update_check");
        let _write = fs::write(&marker, "");
    }
}

/// Get the current backoff interval from file, defaulting to INITIAL_BACKOFF.
fn get_current_backoff() -> Duration {
    let path = state_dir().map(|d| d.join("update_backoff_secs"));
    path.and_then(|p| fs::read_to_string(p).ok())
        .and_then(|s| s.trim().parse::<u64>().ok())
        .map(Duration::from_secs)
        .unwrap_or(INITIAL_BACKOFF)
}

/// Increase the backoff interval (double it, up to MAX_BACKOFF).
fn increase_backoff() {
    if let Some(dir) = state_dir() {
        let _mkdir = fs::create_dir_all(&dir);
        let current = get_current_backoff();
        let new_backoff = std::cmp::min(current * 2, MAX_BACKOFF);
        let _write = fs::write(
            dir.join("update_backoff_secs"),
            new_backoff.as_secs().to_string(),
        );
    }
}

/// Reset backoff to initial value (called when update is found).
pub fn reset_backoff() {
    if let Some(dir) = state_dir() {
        let _rm = fs::remove_file(dir.join("update_backoff_secs"));
    }
}

/// Check if enough time has passed since the last update check.
fn should_check_for_update(backoff: Duration) -> bool {
    get_last_check_time()
        .and_then(|last| last.elapsed().ok())
        .is_none_or(|elapsed| elapsed > backoff)
}

/// Get the number of consecutive update failures.
fn get_update_failure_count() -> u32 {
    state_dir()
        .map(|d| get_update_failure_count_at(&d))
        .unwrap_or(0)
}

/// Testable variant of [`get_update_failure_count`] that reads from an explicit
/// directory. Missing / unparseable files are treated as zero.
pub(crate) fn get_update_failure_count_at(dir: &std::path::Path) -> u32 {
    fs::read_to_string(dir.join("update_failures"))
        .ok()
        .and_then(|s| s.trim().parse().ok())
        .unwrap_or(0)
}

/// Record an update failure. Called by the update command when the install
/// step fails (see `commands::update`). After `MAX_UPDATE_FAILURES`
/// consecutive failures, [`should_attempt_update`] returns false and the
/// version-mismatch update loop is disabled until a successful install
/// clears the counter — this is what prevents the exit-42 restart loop
/// reported in #3934 when `replace_binary` fails persistently (e.g. AV
/// locks, read-only install dir).
pub fn record_update_failure() {
    if let Some(dir) = state_dir() {
        record_update_failure_at(&dir);
    }
}

/// Testable variant of [`record_update_failure`] that writes into an
/// explicit directory. Missing directories are created on demand.
pub(crate) fn record_update_failure_at(dir: &std::path::Path) {
    let _mkdir = fs::create_dir_all(dir);
    let count = get_update_failure_count_at(dir) + 1;
    let _write = fs::write(dir.join("update_failures"), count.to_string());
}

/// Clear the update failure count. Called from the update command after a
/// successful binary install so the counter resets automatically once the
/// underlying problem is resolved (and so manual `freenet update` recovers
/// from a locked-out auto-update state).
pub fn clear_update_failures() {
    if let Some(dir) = state_dir() {
        clear_update_failures_at(&dir);
    }
}

/// Testable variant of [`clear_update_failures`] that operates on an
/// explicit directory.
pub(crate) fn clear_update_failures_at(dir: &std::path::Path) {
    let _rm = fs::remove_file(dir.join("update_failures"));
}

/// Check if we should attempt an update based on failure history.
pub fn should_attempt_update() -> bool {
    state_dir()
        .map(|d| should_attempt_update_at(&d))
        .unwrap_or(true)
}

/// Testable variant of [`should_attempt_update`] that reads from an explicit
/// directory. Used by the regression tests for the #3934 lockout invariant.
pub(crate) fn should_attempt_update_at(dir: &std::path::Path) -> bool {
    get_update_failure_count_at(dir) < MAX_UPDATE_FAILURES
}

/// Returns true if the update check backoff has reached the maximum (1 hour).
/// At that point, we've checked GitHub multiple times with no update found,
/// so the version mismatch flag should be cleared to stop log spam.
pub fn has_reached_max_backoff() -> bool {
    get_current_backoff() >= MAX_BACKOFF
}

/// One-shot GitHub check performed at node startup, independent of peer signals.
///
/// Addresses the "offline-for-days transient peer" gap: a node that has been
/// offline long enough to fall out of the compatible-version window cannot rely
/// on a peer handshake to tell it to update, because handshakes with an
/// incompatible peer may never complete successfully. The normal peer-signal
/// driven update loop therefore never triggers.
///
/// This function asks GitHub directly whether a newer release exists. It is
/// intentionally decoupled from the backoff / failure-count state used by the
/// peer-signal loop: startup is a distinct one-shot event and should not
/// interact with running-state backoff.
///
/// Fail-open: any error (GitHub unreachable, parse failure, etc.) returns
/// `None` so the caller falls through to the normal update loop.
///
/// Returns `Some(latest_version_string)` only when GitHub confirms a strictly
/// newer release than `current_version`. Never returns a downgrade.
pub async fn startup_update_check(current_version: &str) -> Option<String> {
    startup_update_check_with_fetcher(current_version, get_latest_version).await
}

/// Testable core of [`startup_update_check`]. The `fetcher` argument returns
/// the latest version string as reported by the release source; tests inject a
/// fake fetcher to avoid hitting GitHub.
pub(crate) async fn startup_update_check_with_fetcher<F, Fut>(
    current_version: &str,
    fetcher: F,
) -> Option<String>
where
    F: FnOnce() -> Fut,
    Fut: std::future::Future<Output = Result<String>>,
{
    let latest = match fetcher().await {
        Ok(s) => s,
        Err(e) => {
            tracing::warn!(
                "Startup update check: failed to fetch latest version: {}. \
                 Continuing with current binary.",
                e
            );
            return None;
        }
    };
    compare_versions_for_startup(current_version, &latest)
}

/// Pure version comparison for the startup check.
///
/// Returns `Some(latest)` iff `latest` parses as semver strictly greater than
/// `current`. Returns `None` on any parse failure (fail-open) or when the
/// current binary is already at or ahead of the reported release.
pub(crate) fn compare_versions_for_startup(current: &str, latest: &str) -> Option<String> {
    let current_ver = match Version::parse(current) {
        Ok(v) => v,
        Err(e) => {
            tracing::warn!(
                "Startup update check: failed to parse current version '{}': {}",
                current,
                e
            );
            return None;
        }
    };
    let latest_ver = match Version::parse(latest) {
        Ok(v) => v,
        Err(e) => {
            tracing::warn!(
                "Startup update check: failed to parse latest version '{}': {}",
                latest,
                e
            );
            return None;
        }
    };
    if latest_ver > current_ver {
        Some(latest.to_string())
    } else {
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use freenet::transport::{
        set_open_connection_count, signal_version_mismatch, version_mismatch_generation,
    };

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
    fn test_mismatch_generation_increments() {
        let gen_before = version_mismatch_generation();
        signal_version_mismatch();
        let gen_after = version_mismatch_generation();
        assert!(
            gen_after > gen_before,
            "generation should increment on each signal"
        );

        // Multiple signals keep incrementing
        signal_version_mismatch();
        assert!(version_mismatch_generation() > gen_after);
    }

    #[test]
    fn test_open_connection_count() {
        set_open_connection_count(0);
        assert_eq!(get_open_connection_count(), 0);

        set_open_connection_count(5);
        assert_eq!(get_open_connection_count(), 5);

        set_open_connection_count(0);
        assert_eq!(get_open_connection_count(), 0);
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
    fn test_compare_versions_newer_available() {
        assert_eq!(
            compare_versions_for_startup("0.1.74", "0.1.75"),
            Some("0.1.75".to_string())
        );
        assert_eq!(
            compare_versions_for_startup("0.1.74", "0.2.0"),
            Some("0.2.0".to_string())
        );
        assert_eq!(
            compare_versions_for_startup("0.1.74", "1.0.0"),
            Some("1.0.0".to_string())
        );
    }

    #[test]
    fn test_compare_versions_already_current() {
        assert_eq!(compare_versions_for_startup("0.1.75", "0.1.75"), None);
    }

    #[test]
    fn test_compare_versions_never_downgrades() {
        // GitHub reports an older version (e.g. tag rollback) — never downgrade.
        assert_eq!(compare_versions_for_startup("0.2.0", "0.1.99"), None);
        assert_eq!(compare_versions_for_startup("1.0.0", "0.9.99"), None);
    }

    #[test]
    fn test_compare_versions_unparseable_fails_open() {
        assert_eq!(
            compare_versions_for_startup("not-a-version", "0.1.75"),
            None
        );
        assert_eq!(compare_versions_for_startup("0.1.74", "also-garbage"), None);
        assert_eq!(compare_versions_for_startup("", "0.1.75"), None);
    }

    #[test]
    fn test_compare_versions_prerelease_semver_semantics() {
        // semver: 0.1.75-alpha < 0.1.75, 0.1.75 > 0.1.75-alpha
        assert_eq!(
            compare_versions_for_startup("0.1.75-alpha", "0.1.75"),
            Some("0.1.75".to_string())
        );
        assert_eq!(compare_versions_for_startup("0.1.75", "0.1.75-alpha"), None);
    }

    #[tokio::test]
    async fn test_startup_check_fetcher_error_returns_none() {
        // Fetcher failure must not propagate — startup check is fail-open so
        // the node always boots even when GitHub is unreachable.
        let result = startup_update_check_with_fetcher("0.1.74", || async {
            anyhow::bail!("simulated network failure")
        })
        .await;
        assert_eq!(result, None);
    }

    #[tokio::test]
    async fn test_startup_check_finds_newer_version() {
        let result =
            startup_update_check_with_fetcher("0.1.74", || async { Ok("0.1.75".to_string()) })
                .await;
        assert_eq!(result, Some("0.1.75".to_string()));
    }

    #[tokio::test]
    async fn test_startup_check_no_update_when_current() {
        let result =
            startup_update_check_with_fetcher("0.1.75", || async { Ok("0.1.75".to_string()) })
                .await;
        assert_eq!(result, None);
    }

    #[tokio::test]
    async fn test_startup_check_refuses_downgrade() {
        // A node running a newer (possibly pre-release) build must never be
        // downgraded by the startup check, even if GitHub reports an older tag.
        let result =
            startup_update_check_with_fetcher("0.2.0", || async { Ok("0.1.99".to_string()) }).await;
        assert_eq!(result, None);
    }

    #[test]
    fn test_update_failure_counter_roundtrip() {
        // Invariant #3934 relies on: record → get observes increments,
        // clear → get returns zero again. If this regresses, the auto-
        // update lockout cannot accumulate and the exit-42 restart loop
        // becomes unbounded again.
        let tmp = tempfile::tempdir().unwrap();
        let dir = tmp.path();

        assert_eq!(get_update_failure_count_at(dir), 0);

        record_update_failure_at(dir);
        assert_eq!(get_update_failure_count_at(dir), 1);

        record_update_failure_at(dir);
        record_update_failure_at(dir);
        assert_eq!(get_update_failure_count_at(dir), 3);

        clear_update_failures_at(dir);
        assert_eq!(get_update_failure_count_at(dir), 0);

        // Clearing an already-clear counter is idempotent.
        clear_update_failures_at(dir);
        assert_eq!(get_update_failure_count_at(dir), 0);
    }

    #[test]
    fn test_should_attempt_update_locks_out_after_max_failures() {
        // Core regression test for #3934: once MAX_UPDATE_FAILURES
        // consecutive failures accumulate, should_attempt_update must
        // return false so the child stops exiting 42 and the
        // spawn-update / exit-42 / backoff loop terminates.
        let tmp = tempfile::tempdir().unwrap();
        let dir = tmp.path();

        assert!(should_attempt_update_at(dir), "fresh state: no lockout");

        for _ in 0..MAX_UPDATE_FAILURES - 1 {
            record_update_failure_at(dir);
            assert!(
                should_attempt_update_at(dir),
                "below threshold still allowed"
            );
        }
        record_update_failure_at(dir);
        assert!(
            !should_attempt_update_at(dir),
            "MAX_UPDATE_FAILURES reached: auto-update must be disabled"
        );

        // A successful install clears the counter and re-enables updates.
        clear_update_failures_at(dir);
        assert!(
            should_attempt_update_at(dir),
            "after clear: updates re-enabled (manual install recovery)"
        );
    }

    #[test]
    fn test_update_failure_counter_persists_on_disk() {
        // The counter must survive process restarts: the child records a
        // failure via the wrapper's spawn_update_command result, then the
        // wrapper relaunches a fresh child. If the counter lived only in
        // memory the lockout would never fire.
        let tmp = tempfile::tempdir().unwrap();
        let dir = tmp.path();

        record_update_failure_at(dir);
        record_update_failure_at(dir);

        let on_disk = std::fs::read_to_string(dir.join("update_failures"))
            .expect("failure counter file should exist after recording");
        assert_eq!(on_disk.trim(), "2");
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
