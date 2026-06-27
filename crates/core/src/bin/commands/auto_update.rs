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
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, SystemTime};

pub use freenet::transport::{
    clear_version_mismatch, get_open_connection_count, has_version_mismatch,
    version_mismatch_generation,
};

/// Exit code that signals "update needed and verified against GitHub".
/// The service wrapper catches this and runs `freenet update` before restarting.
pub const EXIT_CODE_UPDATE_NEEDED: i32 = 42;

/// Environment variable set by every Freenet supervisor (systemd unit, macOS
/// launchd wrapper script, and the Windows/Linux in-process `service
/// run-wrapper` loop) on the `freenet network` child it spawns. Its presence is
/// positive evidence that *something* will catch exit code 42 and run
/// `freenet update` before restarting the node.
///
/// Auto-update is structurally supervised-install-only: the running node never
/// replaces its own binary — it exits 42 and relies on its supervisor to apply
/// the update and restart it. A bare `freenet network` run has no supervisor, so
/// it detects the update, exits 42, dies, and is never restarted (issue #4580).
/// We use this marker (and `INVOCATION_ID` as a systemd fallback) to tell the
/// operator, loudly, when an update was detected but will not be applied.
pub const SUPERVISED_ENV_VAR: &str = "FREENET_SUPERVISED";

/// Environment variable set ONLY by the freshly-generated Freenet **systemd** units
/// (see `service/linux.rs`) on the `freenet network` child. Its presence is positive
/// evidence that the supervising unit understands the distinct fast-crash exit code
/// 45 (#4551): the unit keeps 45 OUT of `SuccessExitStatus` (so it counts toward
/// `StartLimitBurst`), sets `StartLimitAction=none`, and fires `ExecStopPost`
/// `freenet update` on exit 42 OR 45.
///
/// The node entry point gates [`freenet::enable_fast_crash_exit_code`] on this
/// marker. Unlike [`SUPERVISED_ENV_VAR`], the macOS/Windows run-wrapper does NOT set
/// it (the wrapper only understands exit 42), and an OLD systemd unit (e.g. a node
/// auto-updated to a 45-aware binary but whose unit file was never regenerated) won't
/// have it either — so in those cases the node keeps emitting the burst-exempt
/// self-healing exit 42 rather than a 45 the supervisor would mishandle.
pub const SYSTEMD_FAST_CRASH_ENV_VAR: &str = "FREENET_SYSTEMD_FAST_CRASH";

/// Whether the running node appears to be under a supervisor that will catch
/// exit code 42 and apply the update.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SupervisorStatus {
    /// The authoritative [`SUPERVISED_ENV_VAR`] marker is set — a Freenet
    /// supervisor spawned us and *will* catch exit 42 and run `freenet update`.
    /// Nothing to warn about; exit 42 is expected to be applied.
    Supervised,
    /// We are under systemd (`INVOCATION_ID` is set) but our authoritative
    /// marker is absent. systemd alone does not prove the unit has the
    /// exit-42 → `freenet update` `ExecStopPost` hook: a custom unit,
    /// `systemd-run`, or a hand-written service running `freenet network`
    /// without that hook is also `INVOCATION_ID`-bearing. We therefore can't
    /// confirm the update will be applied — warn, but more softly, since this
    /// also covers older Freenet units installed before the marker existed.
    SupervisedUnverified,
    /// No evidence of any supervisor. Exit 42 will most likely kill the node
    /// without the update ever being applied — the operator must run
    /// `freenet update` manually (or install Freenet as a service).
    Unsupervised,
}

/// Detect whether a supervisor is present, using an injectable env lookup so the
/// logic is unit-testable without mutating the process environment.
///
/// Honest by construction: we only claim the fully-quiet [`Supervised`] state on
/// *our own* marker. systemd's generic `INVOCATION_ID` is reported as
/// [`SupervisedUnverified`] (still warned about, softly) because it does not
/// prove the unit carries the exit-42 → `freenet update` hook. Absence of both
/// is [`Unsupervised`], which drives the loud error.
///
/// [`Supervised`]: SupervisorStatus::Supervised
/// [`SupervisedUnverified`]: SupervisorStatus::SupervisedUnverified
/// [`Unsupervised`]: SupervisorStatus::Unsupervised
pub fn detect_supervisor_status<F>(env_get: F) -> SupervisorStatus
where
    F: Fn(&str) -> Option<String>,
{
    let present = |key: &str| env_get(key).is_some_and(|v| !v.trim().is_empty());
    if present(SUPERVISED_ENV_VAR) {
        SupervisorStatus::Supervised
    } else if present("INVOCATION_ID") {
        SupervisorStatus::SupervisedUnverified
    } else {
        SupervisorStatus::Unsupervised
    }
}

/// Read the live supervisor status from the process environment.
pub fn supervisor_status() -> SupervisorStatus {
    detect_supervisor_status(|key| std::env::var(key).ok())
}

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

/// Sentinel error returned by [`get_latest_version`] when the global GitHub-poll
/// token bucket is empty. Distinct from a network/parse error so the caller can
/// tell "we deliberately skipped polling to bound load" apart from "GitHub was
/// unreachable" — the two must drive different behaviour (the latter may fall
/// back to a gateway-trust exit 42; the former must NOT, or local rate-limiting
/// would itself trigger the restart loop the limiter exists to prevent).
#[derive(Debug)]
struct RateLimitedError;

impl std::fmt::Display for RateLimitedError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "GitHub update poll rate-limited (token bucket empty)")
    }
}

impl std::error::Error for RateLimitedError {}

/// Result of an update check attempt.
#[derive(Debug, PartialEq)]
pub enum UpdateCheckResult {
    /// Rate limited, too many failures, or no update available yet - will retry later.
    /// The caller should NOT clear the version mismatch flag (preserve it for retry).
    Skipped,
    /// The global GitHub-poll rate limiter (#4073) denied this check: we did NOT
    /// reach GitHub at all. Distinct from [`Skipped`] because the caller must NOT
    /// treat it as a "GitHub says no update / unreachable" signal — in
    /// particular it must NOT feed the legacy "max-backoff + 0 connections ->
    /// exit 42" gateway-trust fallback, since the supervisor-side `freenet
    /// update` shares the same empty bucket and would just exit "already up to
    /// date", turning local rate-limiting into a restart loop. The caller should
    /// do nothing and retry once the bucket refills (the version-mismatch flag is
    /// preserved).
    ///
    /// [`Skipped`]: UpdateCheckResult::Skipped
    RateLimited,
    /// Checked GitHub, newer version confirmed.
    /// The caller should clear the version mismatch flag.
    UpdateAvailable(String),
    /// The newer version on GitHub is pinned known-bad on this node by a prior
    /// crash-loop rollback (#4073). Distinct from [`Skipped`] so the caller does
    /// NOT fall through to the legacy "max-backoff + 0 connections -> exit 42"
    /// fallback: there is nothing safe to update to, so the node must stay put.
    /// The caller should CLEAR the driving signal (version-mismatch / urgent) so
    /// it stops trying to exit for this update; the signal re-arms on the next
    /// peer handshake, which will pick up a later, strictly-newer fix.
    ///
    /// [`Skipped`]: UpdateCheckResult::Skipped
    PinnedKnownBad,
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
/// Set the first time this process logs the auto-update lockout at warn! level,
/// so the permanent locked-out state is surfaced loudly once rather than on
/// every 60s update-loop tick (it can be hit from multiple triggers per tick).
static LOCKOUT_WARNED: AtomicBool = AtomicBool::new(false);

pub async fn check_if_update_available(current_version: &str) -> UpdateCheckResult {
    // Don't check if we've failed too many times
    if !should_attempt_update() {
        // Loud, operator-visible (issue #4580): a persistent lockout means this
        // peer has stopped trying to auto-update entirely and will silently stay
        // behind. A common cause is a non-writable binary path (e.g. a
        // hand-installed binary the service account can't replace), which is a
        // *permanent* lockout until an operator intervenes. Warn LOUDLY the first
        // time we observe it this process, then drop to debug! so the permanent
        // state does not spam the log every minute.
        let failures = get_update_failure_count();
        if !LOCKOUT_WARNED.swap(true, Ordering::Relaxed) {
            tracing::warn!(
                failures,
                max = MAX_UPDATE_FAILURES,
                "Auto-update is LOCKED OUT after {MAX_UPDATE_FAILURES} consecutive failed update \
                 attempts and will NOT retry. This peer will stay on the current version until an \
                 operator intervenes. Run `freenet update` manually to update and reset the \
                 lockout; if updates keep failing, the binary path is likely not writable by this \
                 account.",
            );
        } else {
            tracing::debug!(
                failures,
                max = MAX_UPDATE_FAILURES,
                "Skipping update check - auto-update locked out (already warned this process)"
            );
        }
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
                // #4073: never trigger an exit-42 update to a version that is
                // locally BLOCKED — either pinned known-bad by a prior crash-loop
                // rollback, OR gated after repeatedly failing to install (checksum
                // / signature / download / extract). In both cases the installer
                // would refuse it anyway, so emitting exit 42 only produces a
                // pointless restart cycle. The mismatch flag is kept (handled like
                // the pin) so a later, strictly-newer fixed release is still
                // picked up.
                if super::rollback::is_version_pinned_bad(&latest)
                    || super::rollback::is_version_install_gated(&latest)
                {
                    tracing::warn!(
                        version = %latest,
                        "Newer version is locally blocked (crash-loop known-bad pin or repeated \
                         install failures); not triggering auto-update to it (#4073)"
                    );
                    // Distinct result (NOT Skipped) so the caller clears the
                    // driving signal and does not fall through to the legacy
                    // max-backoff exit-42 fallback. GROW the GitHub-check backoff
                    // (do NOT reset it): a node that has already rolled back to a
                    // good version is in no hurry to find the fix, so it should
                    // poll at most hourly rather than at the 60s floor while
                    // peers keep advertising the pinned-bad version. An hourly
                    // check still catches a later strictly-newer release.
                    increase_backoff();
                    return UpdateCheckResult::PinnedKnownBad;
                }
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
        Err(e) if e.downcast_ref::<RateLimitedError>().is_some() => {
            // Our OWN rate limiter denied the poll — NOT a GitHub failure. Do not
            // grow the backoff and (critically) return the distinct RateLimited
            // result so the caller does not fall through to the gateway-trust
            // exit-42 fallback. We simply retry once the bucket refills.
            tracing::debug!(
                "Update check skipped: GitHub poll rate-limited; will retry when the token bucket refills"
            );
            UpdateCheckResult::RateLimited
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
    // Global persistent rate limit (#4073): refuse to hit GitHub when the shared
    // token bucket is empty. This is the in-node choke point (the node's startup
    // check, the #4589 re-poll, and the peer-signal loop all reach GitHub through
    // here); the supervisor-side `freenet update` is bounded by the same bucket
    // at `get_latest_release`. A denied poll returns Err so the caller
    // (`check_if_update_available`) treats it as `Skipped` and retries later —
    // the node keeps running, it just does not poll GitHub this tick.
    if !try_consume_node_poll() {
        return Err(RateLimitedError.into());
    }

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
///
/// `pub(crate)` so the crash-loop auto-rollback module (`commands::rollback`)
/// persists its probation / known-bad markers in the SAME directory as the
/// auto-update failure counter and backoff state, ensuring both the node and
/// the supervisor-invoked `freenet update` agree on a single state location.
pub(crate) fn state_dir() -> Option<PathBuf> {
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
/// directory.
///
/// * Missing file → `0` (legitimate "no failures yet").
/// * Present but unparseable → `MAX_UPDATE_FAILURES` (defensive: if the
///   counter file has been truncated or corrupted we must NOT silently
///   reset the lockout — that would be an amplification vector for any
///   process that can partially overwrite the file, defeating the
///   #3934 fix. Users can recover by explicitly deleting the file).
pub(crate) fn get_update_failure_count_at(dir: &std::path::Path) -> u32 {
    match fs::read_to_string(dir.join("update_failures")) {
        Ok(s) => s.trim().parse().unwrap_or(MAX_UPDATE_FAILURES),
        Err(_) => 0,
    }
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

// ── Persistent GitHub-poll rate limit (token buckets) ──────────────────────
//
// Issue #4073 aggregate-load bounding: every GitHub release poll is gated by an
// on-disk token bucket so that no combination of restarts, peer signals, or
// repeated failed installs can make one node hammer the GitHub REST API. The
// buckets are persisted in `state_dir()` (not in memory) precisely so the fresh,
// short-lived `freenet update` process honours the limit across restarts: an
// in-memory limiter would reset to full on every relaunch and not bound a
// restart loop at all.
//
// There are TWO independent buckets, one per fetch path:
//   * the NODE bucket gates the in-process `get_latest_version` (startup check,
//     #4589 re-poll, peer-signal loop);
//   * the INSTALL bucket gates the supervisor-invoked `get_latest_release`
//     (`freenet update`).
//
// They are SEPARATE on purpose. The node always runs before the supervisor's
// `freenet update` in a restart cycle (it detects, exits 42, THEN the installer
// runs), so a single shared bucket would let the node win every token race and
// starve the installer — a low/refilling shared bucket could leave a legitimate
// update unable to actually fetch+install while the node keeps spending the lone
// refill token to re-confirm it. Two buckets bound each path independently and
// remove that ordering bias.
//
// Capacity / refill are tuned so NORMAL operation never trips a limit while a
// runaway loop is firmly capped:
//   * Normal load is tiny: ~1 boot poll + the staggered re-poll (a few times a
//     day) + the occasional real install — far below capacity, and each consumed
//     token refills within ~10 min.
//   * A runaway loop is bounded to ~1 token per [`GITHUB_POLL_REFILL_SECS`] once
//     the initial capacity drains, i.e. ~6 GitHub calls/hour/node PER PATH
//     regardless of restart rate. (The per-target-version install-failure gate
//     normally stops the failed-install loop well before then.)

/// Maximum number of GitHub release polls (per path) in a burst before the
/// refill rate takes over. Comfortably above a real detect→install burst and any
/// normal daily activity, yet far below any rate that would matter to GitHub.
pub(crate) const GITHUB_POLL_BUCKET_CAPACITY: f64 = 8.0;

/// Refill interval: one token returns every ~10 minutes, so the sustained poll
/// rate of any loop is capped at ~6 GitHub REST calls/hour/node per path.
pub(crate) const GITHUB_POLL_REFILL_SECS: f64 = 600.0;

/// On-disk bucket file for the in-process node poll path (`get_latest_version`).
const NODE_POLL_BUCKET_FILE: &str = "github_poll_bucket_node";

/// On-disk bucket file for the supervisor-side installer (`get_latest_release`).
/// Independent from the node bucket so the node cannot starve the installer.
const INSTALL_POLL_BUCKET_FILE: &str = "github_poll_bucket_install";

/// Token-bucket state persisted across processes.
#[derive(Debug, Clone, Copy, PartialEq)]
pub(crate) struct GithubPollBucket {
    pub tokens: f64,
    pub updated_unix: u64,
}

/// How an on-disk bucket read resolved. Distinguishes a legitimately-absent
/// bucket (first boot) from a corrupt/torn one so they can be handled
/// differently: missing initialises to a full bucket (so the first real poll is
/// never blocked), whereas corrupt is treated conservatively (deny) so a torn
/// write cannot be used to reset the limiter to full.
#[derive(Debug, Clone, Copy, PartialEq)]
enum BucketRead {
    Missing,
    Corrupt,
    Present(GithubPollBucket),
}

/// Seconds since the Unix epoch (wall clock). The bucket only needs elapsed
/// real time between polls; tests inject `now_unix` directly into the pure
/// helpers below rather than relying on this.
fn now_unix() -> u64 {
    SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

/// Pure token-bucket step. Refills `prev` for the elapsed time, then attempts to
/// spend one token. `prev == None` means "no prior state" and starts from a full
/// bucket so a fresh node's first poll is always allowed. Returns the new state
/// and whether a token was spent (the poll is allowed).
fn github_poll_bucket_step(
    prev: Option<GithubPollBucket>,
    now_unix: u64,
    capacity: f64,
    refill_secs: f64,
) -> (GithubPollBucket, bool) {
    // The stored timestamp must never move BACKWARDS. On a backwards clock
    // (suspend/resume, NTP step) `saturating_sub` already prevents a negative
    // refill THIS step — but if we then persisted the earlier `now_unix`, a later
    // forward step back to the original time would measure elapsed from the
    // rewound timestamp and grant refill credit for time that already elapsed
    // before the rewind. Anchoring on `max(now, prev)` makes the bucket measure
    // elapsed only from the highest timestamp ever seen, so a clock that dips and
    // recovers yields zero net credit.
    let stored_unix = match prev {
        Some(s) => now_unix.max(s.updated_unix),
        None => now_unix,
    };
    let mut tokens = match prev {
        Some(s) => {
            let elapsed = now_unix.saturating_sub(s.updated_unix) as f64;
            (s.tokens + elapsed / refill_secs).min(capacity)
        }
        None => capacity,
    };
    let allowed = tokens >= 1.0;
    if allowed {
        tokens -= 1.0;
    }
    (
        GithubPollBucket {
            tokens,
            updated_unix: stored_unix,
        },
        allowed,
    )
}

fn read_github_poll_bucket_at(dir: &std::path::Path, file: &str) -> BucketRead {
    match fs::read_to_string(dir.join(file)) {
        Ok(raw) => {
            let mut it = raw.split_whitespace();
            match (
                it.next().and_then(|t| t.parse::<f64>().ok()),
                it.next().and_then(|t| t.parse::<u64>().ok()),
            ) {
                // Reject non-finite / negative token counts as corrupt: a NaN
                // would make every comparison false and could be abused to
                // bypass the limiter.
                (Some(tokens), Some(updated_unix)) if tokens.is_finite() && tokens >= 0.0 => {
                    BucketRead::Present(GithubPollBucket {
                        tokens,
                        updated_unix,
                    })
                }
                _ => BucketRead::Corrupt,
            }
        }
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => BucketRead::Missing,
        // Any other read error (permissions, etc.) is treated as corrupt =>
        // deny, conservatively, rather than granting a free poll.
        Err(_) => BucketRead::Corrupt,
    }
}

fn write_github_poll_bucket_at(
    dir: &std::path::Path,
    file: &str,
    bucket: &GithubPollBucket,
) -> std::io::Result<()> {
    fs::create_dir_all(dir)?;
    fs::write(
        dir.join(file),
        format!("{} {}", bucket.tokens, bucket.updated_unix),
    )
}

/// Try to consume one token from the named on-disk bucket in `dir`.
/// Returns `true` if a poll is permitted (token spent), `false` if rate-limited.
pub(crate) fn try_consume_github_poll_at(dir: &std::path::Path, file: &str, now_unix: u64) -> bool {
    let prev = match read_github_poll_bucket_at(dir, file) {
        BucketRead::Present(s) => Some(s),
        BucketRead::Missing => None,
        BucketRead::Corrupt => {
            // Deny this poll, and (best-effort) rewrite an empty bucket dated now
            // so the limiter self-heals (a token trickles back after the refill
            // interval) without ever granting a free token from the corrupt
            // state. This makes a corrupt/torn bucket fail closed; if the rewrite
            // itself fails the next read stays corrupt and keeps denying.
            if write_github_poll_bucket_at(
                dir,
                file,
                &GithubPollBucket {
                    tokens: 0.0,
                    updated_unix: now_unix,
                },
            )
            .is_err()
            {
                tracing::debug!("Could not reset corrupt GitHub-poll bucket; staying denied");
            }
            return false;
        }
    };
    let (next, allowed) = github_poll_bucket_step(
        prev,
        now_unix,
        GITHUB_POLL_BUCKET_CAPACITY,
        GITHUB_POLL_REFILL_SECS,
    );
    // FAIL CLOSED: only allow the poll if we BOTH had a token AND persisted the
    // post-consume state. If the write fails (read-only / full state dir), a
    // missing bucket would otherwise read as full on every restart and grant an
    // unbounded burst — so a non-persistable consume must deny instead.
    let persisted = write_github_poll_bucket_at(dir, file, &next).is_ok();
    allowed && persisted
}

fn try_consume_poll_bucket(file: &str) -> bool {
    // Deny when the state directory cannot be resolved — with nowhere to persist
    // the bucket we cannot bound a loop, so denying is the safe choice (mirrors
    // [`should_attempt_update`]'s `unwrap_or(false)`).
    match state_dir() {
        Some(dir) => try_consume_github_poll_at(&dir, file, now_unix()),
        None => false,
    }
}

/// Try to consume one token from the NODE poll bucket (`get_latest_version`).
pub(crate) fn try_consume_node_poll() -> bool {
    try_consume_poll_bucket(NODE_POLL_BUCKET_FILE)
}

/// Try to consume one token from the INSTALL poll bucket (`get_latest_release`).
/// Independent of the node bucket so the node can never starve the installer.
pub(crate) fn try_consume_install_poll() -> bool {
    try_consume_poll_bucket(INSTALL_POLL_BUCKET_FILE)
}

/// Check if we should attempt an update based on failure history.
///
/// If the state directory cannot be resolved (e.g. Windows service
/// account with no `USERPROFILE`), returns `false`: with no place to
/// persist the failure counter we cannot distinguish a fresh session
/// from one that has been looping for hours, so the safest choice is
/// to skip auto-update entirely rather than risk an unbounded exit-42
/// loop (skeptical-review H2 on PR #3941). Users in that situation
/// still receive updates via whatever external packaging mechanism
/// installed them.
pub fn should_attempt_update() -> bool {
    state_dir()
        .map(|d| should_attempt_update_at(&d))
        .unwrap_or(false)
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
    fn test_corrupt_counter_file_is_treated_as_max() {
        // Defensive invariant: a present-but-unparseable counter file
        // must be treated as MAX, not silently reset to 0. Otherwise an
        // AV tool (or any process) that truncates/corrupts the file
        // mid-write silently defeats the auto-update lockout and the
        // exit-42 loop becomes unbounded again (testing-review point
        // #5 on PR #3941).
        let tmp = tempfile::tempdir().unwrap();
        let dir = tmp.path();

        // Non-numeric content: simulates corruption.
        std::fs::write(dir.join("update_failures"), "garbage").unwrap();
        assert_eq!(get_update_failure_count_at(dir), MAX_UPDATE_FAILURES);
        assert!(!should_attempt_update_at(dir));

        // Empty content: simulates truncated write.
        std::fs::write(dir.join("update_failures"), "").unwrap();
        assert_eq!(get_update_failure_count_at(dir), MAX_UPDATE_FAILURES);
        assert!(!should_attempt_update_at(dir));

        // Negative/overflow: parse failure → MAX.
        std::fs::write(dir.join("update_failures"), "-1").unwrap();
        assert_eq!(get_update_failure_count_at(dir), MAX_UPDATE_FAILURES);

        // Deleting the file is the explicit user recovery path.
        clear_update_failures_at(dir);
        assert_eq!(get_update_failure_count_at(dir), 0);
        assert!(should_attempt_update_at(dir));
    }

    #[test]
    fn test_check_if_update_available_does_not_clear_failure_counter() {
        // Regression test for the #3934 invariant that the PR fixed:
        // `check_if_update_available` MUST NOT call
        // `clear_update_failures()` in its `UpdateAvailable` arm. Before
        // the fix, that call wiped the counter on every peer-mismatch
        // GitHub check, so accumulated failures from previous install
        // attempts were erased before the MAX_UPDATE_FAILURES gate
        // could ever trigger, leaving the exit-42 loop unbounded.
        //
        // We look for call-syntax (`clear_update_failures(`) rather
        // than any textual occurrence, because the replacement comment
        // explaining why the call was removed legitimately mentions
        // the function by name. Strip line comments first so a comment
        // using the call-syntax form in example code would not trip us.
        let src = include_str!("auto_update.rs");
        let (_, after_fn_start) = src
            .split_once("pub async fn check_if_update_available(")
            .expect("check_if_update_available definition not found");
        let (body, _) = after_fn_start
            .split_once("\n}\n")
            .expect("could not locate end of check_if_update_available");
        let code_only: String = body
            .lines()
            .map(|line| line.split_once("//").map(|(c, _)| c).unwrap_or(line))
            .collect::<Vec<_>>()
            .join("\n");
        assert!(
            !code_only.contains("clear_update_failures("),
            "check_if_update_available must not call clear_update_failures() — \
             doing so wipes the #3934 lockout counter on every peer-mismatch \
             GitHub check. Only a successful install (update.rs) or a verified \
             AlreadyUpToDate exit should clear failures."
        );
    }

    #[test]
    fn test_should_attempt_update_conservative_when_state_dir_missing() {
        // skeptical-review H2 on PR #3941: when state_dir() returns
        // None (e.g. Windows service account with no USERPROFILE), we
        // cannot persist failure state, so attempting auto-update
        // risks the same unbounded exit-42 loop that the per-file
        // counter is supposed to prevent. `should_attempt_update`
        // must return false in that case, not true.
        //
        // We exercise `should_attempt_update_at` against a path that
        // genuinely cannot be read (a file path used as if it were a
        // directory). `get_update_failure_count_at` treats
        // `read_to_string`-error as 0 (missing file), but
        // `should_attempt_update` at the public wrapper level uses
        // `unwrap_or(false)` for `state_dir() → None`. We pin that
        // source-level choice too.
        let src = include_str!("auto_update.rs");
        let (_, after_fn_start) = src
            .split_once("pub fn should_attempt_update() -> bool {")
            .expect("should_attempt_update definition not found");
        let (body, _) = after_fn_start
            .split_once('}')
            .expect("could not locate end of should_attempt_update");
        assert!(
            body.contains("unwrap_or(false)"),
            "should_attempt_update must fall back to `false` when state_dir \
             is unavailable — falling back to `true` allows the exit-42 loop \
             to run unbounded on Windows service accounts without USERPROFILE."
        );
    }

    #[test]
    fn test_detect_supervisor_status_marker_present() {
        // Issue #4580: the explicit FREENET_SUPERVISED marker (set by all three
        // supervisors on the network child) is positive evidence of a supervisor.
        let env = |key: &str| {
            if key == SUPERVISED_ENV_VAR {
                Some("1".to_string())
            } else {
                None
            }
        };
        assert_eq!(detect_supervisor_status(env), SupervisorStatus::Supervised);
    }

    #[test]
    fn test_detect_supervisor_status_invocation_id_is_unverified() {
        // systemd sets INVOCATION_ID for EVERY service instance, including custom
        // units / systemd-run that lack our exit-42 → `freenet update` hook. So a
        // bare INVOCATION_ID is only *unverified* supervision: we still warn (more
        // softly), rather than going fully quiet as if the update were guaranteed
        // to apply. This is the false-positive guard from the Codex review.
        let env = |key: &str| {
            if key == "INVOCATION_ID" {
                Some("a1b2c3d4".to_string())
            } else {
                None
            }
        };
        assert_eq!(
            detect_supervisor_status(env),
            SupervisorStatus::SupervisedUnverified
        );
    }

    #[test]
    fn test_detect_supervisor_status_marker_beats_invocation_id() {
        // When BOTH our authoritative marker and systemd's INVOCATION_ID are set
        // (the normal Freenet systemd-unit case), the marker wins and we report
        // the fully-quiet Supervised state.
        let env = |key: &str| match key {
            SUPERVISED_ENV_VAR => Some("1".to_string()),
            "INVOCATION_ID" => Some("a1b2c3d4".to_string()),
            _ => None,
        };
        assert_eq!(detect_supervisor_status(env), SupervisorStatus::Supervised);
    }

    #[test]
    fn test_detect_supervisor_status_unsupervised_when_absent() {
        // The whole point of #4580: a bare `freenet network` run has neither
        // signal and must be reported as Unsupervised so the loud warning fires
        // instead of silently exiting 42 with nothing to apply the update.
        let env = |_key: &str| None;
        assert_eq!(
            detect_supervisor_status(env),
            SupervisorStatus::Unsupervised
        );
    }

    #[test]
    fn test_detect_supervisor_status_empty_and_whitespace_are_not_evidence() {
        // An empty or whitespace-only value (e.g. `FREENET_SUPERVISED=`) must NOT
        // count as a supervisor — it would suppress the warning while leaving the
        // update unapplied, which is exactly the silent failure we are fixing.
        for value in ["", "   ", "\t", "\n"] {
            let env = move |key: &str| {
                if key == SUPERVISED_ENV_VAR || key == "INVOCATION_ID" {
                    Some(value.to_string())
                } else {
                    None
                }
            };
            assert_eq!(
                detect_supervisor_status(env),
                SupervisorStatus::Unsupervised,
                "value {value:?} must not count as supervisor evidence"
            );
        }
    }

    #[test]
    fn test_locked_out_update_check_is_loud() {
        // Source-scrape pin for #4580: when the failure lockout disables
        // auto-update, the skip MUST be operator-visible (warn!), not a silent
        // debug! line. A regression to debug! would re-hide the permanent
        // lockout (e.g. non-writable binary path) the issue calls out.
        let src = include_str!("auto_update.rs");
        let (_, after_fn_start) = src
            .split_once("pub async fn check_if_update_available(")
            .expect("check_if_update_available definition not found");
        let (_, after_guard) = after_fn_start
            .split_once("if !should_attempt_update() {")
            .expect("lockout guard not found");
        let (guard_body, _) = after_guard
            .split_once("return UpdateCheckResult::Skipped;")
            .expect("could not locate end of lockout guard");
        // Strip line comments so the explanatory comment (which itself mentions
        // "warn!"/"debug!") cannot satisfy the assertion — match only on code.
        let code_only: String = guard_body
            .lines()
            .map(|line| line.split_once("//").map(|(c, _)| c).unwrap_or(line))
            .collect::<Vec<_>>()
            .join("\n");
        assert!(
            code_only.contains("tracing::warn!"),
            "the auto-update lockout skip must log at warn! level so a permanent \
             lockout is diagnosable (#4580), not silently dropped at debug!"
        );
    }

    #[test]
    fn test_github_poll_bucket_allows_initial_burst_then_caps() {
        // Fresh (missing) bucket starts full: the first CAPACITY polls at the
        // same instant are allowed, the next is denied (token-bucket cap).
        let tmp = tempfile::tempdir().unwrap();
        let dir = tmp.path();
        let now = 1_000_000u64;

        let cap = GITHUB_POLL_BUCKET_CAPACITY as u64;
        for i in 0..cap {
            assert!(
                try_consume_github_poll_at(dir, NODE_POLL_BUCKET_FILE, now),
                "poll {i} within capacity must be allowed"
            );
        }
        assert!(
            !try_consume_github_poll_at(dir, NODE_POLL_BUCKET_FILE, now),
            "poll beyond capacity at the same instant must be denied"
        );
    }

    #[test]
    fn test_github_poll_bucket_refills_over_time() {
        // After draining, one token returns per refill interval (and no more).
        let tmp = tempfile::tempdir().unwrap();
        let dir = tmp.path();
        let mut now = 2_000_000u64;

        let cap = GITHUB_POLL_BUCKET_CAPACITY as u64;
        for _ in 0..cap {
            assert!(try_consume_github_poll_at(dir, NODE_POLL_BUCKET_FILE, now));
        }
        assert!(
            !try_consume_github_poll_at(dir, NODE_POLL_BUCKET_FILE, now),
            "drained"
        );

        // Half a refill interval: still not enough for a whole token.
        now += (GITHUB_POLL_REFILL_SECS as u64) / 2;
        assert!(
            !try_consume_github_poll_at(dir, NODE_POLL_BUCKET_FILE, now),
            "half a refill interval is < 1 token"
        );

        // A full refill interval from the last write: exactly one token back.
        now += GITHUB_POLL_REFILL_SECS as u64;
        assert!(
            try_consume_github_poll_at(dir, NODE_POLL_BUCKET_FILE, now),
            "one refill interval should grant exactly one token"
        );
        assert!(
            !try_consume_github_poll_at(dir, NODE_POLL_BUCKET_FILE, now),
            "only one token should have refilled"
        );
    }

    #[test]
    fn test_github_poll_bucket_refill_is_capped_at_capacity() {
        // A long idle period cannot accumulate more than CAPACITY tokens (no
        // unbounded credit that would let a later burst exceed the cap).
        let tmp = tempfile::tempdir().unwrap();
        let dir = tmp.path();
        let now = 3_000_000u64;

        // Seed an empty bucket, then jump far into the future.
        write_github_poll_bucket_at(
            dir,
            NODE_POLL_BUCKET_FILE,
            &GithubPollBucket {
                tokens: 0.0,
                updated_unix: now,
            },
        )
        .unwrap();
        let far_future = now + (GITHUB_POLL_REFILL_SECS as u64) * 10_000;

        let cap = GITHUB_POLL_BUCKET_CAPACITY as u64;
        for _ in 0..cap {
            assert!(try_consume_github_poll_at(
                dir,
                NODE_POLL_BUCKET_FILE,
                far_future
            ));
        }
        assert!(
            !try_consume_github_poll_at(dir, NODE_POLL_BUCKET_FILE, far_future),
            "refill must be capped at CAPACITY regardless of idle time"
        );
    }

    #[test]
    fn test_github_poll_bucket_denies_on_corrupt_file() {
        // A corrupt/unparseable bucket must FAIL CLOSED (deny) so a torn write
        // cannot be used to reset the limiter to full and bypass it.
        let tmp = tempfile::tempdir().unwrap();
        let dir = tmp.path();
        std::fs::write(dir.join(NODE_POLL_BUCKET_FILE), "not-a-bucket").unwrap();

        assert!(
            !try_consume_github_poll_at(dir, NODE_POLL_BUCKET_FILE, 4_000_000),
            "corrupt bucket must deny the poll"
        );

        // And it self-heals to an empty-but-valid bucket: a poll one refill
        // interval later is allowed again (never a free token from the corrupt
        // state).
        assert!(matches!(
            read_github_poll_bucket_at(dir, NODE_POLL_BUCKET_FILE),
            BucketRead::Present(_)
        ));
        assert!(try_consume_github_poll_at(
            dir,
            NODE_POLL_BUCKET_FILE,
            4_000_000 + GITHUB_POLL_REFILL_SECS as u64
        ));
    }

    #[test]
    fn test_github_poll_bucket_nan_and_negative_are_corrupt() {
        // Non-finite / negative token counts must be rejected as corrupt rather
        // than trusted (a NaN compares false everywhere and could bypass the cap).
        let tmp = tempfile::tempdir().unwrap();
        let dir = tmp.path();
        for bad in ["NaN 100", "inf 100", "-1 100", "5", "5 notanint"] {
            std::fs::write(dir.join(NODE_POLL_BUCKET_FILE), bad).unwrap();
            assert_eq!(
                read_github_poll_bucket_at(dir, NODE_POLL_BUCKET_FILE),
                BucketRead::Corrupt,
                "{bad:?} must read as corrupt"
            );
        }
    }

    #[test]
    fn test_github_poll_bucket_backwards_clock_gives_no_credit() {
        // A backwards clock (suspend/resume, NTP step) must never credit tokens.
        let prev = GithubPollBucket {
            tokens: 0.0,
            updated_unix: 5_000_000,
        };
        let (next, allowed) = github_poll_bucket_step(
            Some(prev),
            4_000_000, // earlier than updated_unix
            GITHUB_POLL_BUCKET_CAPACITY,
            GITHUB_POLL_REFILL_SECS,
        );
        assert!(
            !allowed,
            "no token should be available after a backwards clock"
        );
        assert_eq!(next.tokens, 0.0, "no negative-time credit");
        // Critically: the stored timestamp must NOT rewind, or a later forward
        // step would grant credit for already-elapsed time (Codex finding).
        assert_eq!(
            next.updated_unix, 5_000_000,
            "stored timestamp must not move backwards"
        );
    }

    #[test]
    fn test_github_poll_bucket_dip_then_recover_grants_no_credit() {
        // End-to-end: drain the bucket at T, dip the clock back by a full refill
        // interval (denied, no credit), then return to T. The recovery must NOT
        // grant a refill token for the window that elapsed before the dip.
        let tmp = tempfile::tempdir().unwrap();
        let dir = tmp.path();
        let t = 10_000_000u64;

        let cap = GITHUB_POLL_BUCKET_CAPACITY as u64;
        for _ in 0..cap {
            assert!(try_consume_github_poll_at(dir, NODE_POLL_BUCKET_FILE, t));
        }
        assert!(
            !try_consume_github_poll_at(dir, NODE_POLL_BUCKET_FILE, t),
            "drained at T"
        );

        // Clock dips back a full refill interval.
        assert!(
            !try_consume_github_poll_at(
                dir,
                NODE_POLL_BUCKET_FILE,
                t - GITHUB_POLL_REFILL_SECS as u64
            ),
            "still empty during the backwards dip"
        );
        // Clock returns to T: must NOT have been credited a token by the dip.
        assert!(
            !try_consume_github_poll_at(dir, NODE_POLL_BUCKET_FILE, t),
            "returning to T must not grant credit for pre-dip time"
        );
        // A genuine refill interval PAST the high-water mark does grant one token.
        assert!(try_consume_github_poll_at(
            dir,
            NODE_POLL_BUCKET_FILE,
            t + GITHUB_POLL_REFILL_SECS as u64
        ));
    }

    #[test]
    fn test_github_poll_bucket_missing_is_full_not_denied() {
        // Regression guard: a legitimately-absent bucket (first boot) must NOT be
        // treated like a corrupt one — the first real poll has to go through, or
        // a fresh node could never detect an update.
        let tmp = tempfile::tempdir().unwrap();
        let dir = tmp.path();
        assert_eq!(
            read_github_poll_bucket_at(dir, NODE_POLL_BUCKET_FILE),
            BucketRead::Missing
        );
        assert!(
            try_consume_github_poll_at(dir, NODE_POLL_BUCKET_FILE, 6_000_000),
            "first poll on a fresh node must be allowed"
        );
    }

    #[cfg(unix)]
    #[test]
    fn test_github_poll_bucket_fails_closed_when_unpersistable() {
        // Codex P2: if the post-consume state cannot be persisted (read-only /
        // full state dir), the poll must be DENIED — otherwise a missing bucket
        // would read as full on every restart and grant an unbounded burst,
        // failing open. Make the dir read-only so the write fails.
        use std::os::unix::fs::PermissionsExt;
        let tmp = tempfile::tempdir().unwrap();
        let dir = tmp.path();
        let orig = std::fs::metadata(dir).unwrap().permissions();
        std::fs::set_permissions(dir, std::fs::Permissions::from_mode(0o555)).unwrap();

        let allowed = try_consume_github_poll_at(dir, NODE_POLL_BUCKET_FILE, 8_000_000);

        // Restore perms before asserting so tempdir cleanup always works.
        std::fs::set_permissions(dir, orig).unwrap();
        assert!(
            !allowed,
            "an unpersistable consume must deny (fail closed), not allow"
        );
    }

    #[test]
    fn test_get_latest_version_consults_rate_limit_bucket() {
        // Source pin: the in-node GitHub fetch MUST gate on the persistent token
        // bucket at its top, or the loop's GitHub spam is unbounded again (#4073).
        let src = include_str!("auto_update.rs");
        let (_, body) = src
            .split_once("async fn get_latest_version() -> Result<String> {")
            .expect("get_latest_version definition not found");
        let (head, _) = body
            .split_once("reqwest::Client::builder()")
            .expect("client builder not found");
        assert!(
            head.contains("try_consume_node_poll()"),
            "get_latest_version must consume a rate-limit token before hitting GitHub"
        );
    }

    #[test]
    fn test_node_and_install_buckets_are_independent() {
        // Codex P1: the node and installer must NOT share a bucket. Draining the
        // node bucket completely must leave the install bucket untouched, so the
        // node (which always polls first in a restart cycle) can never starve the
        // supervisor-side installer of the token it needs to actually update.
        let tmp = tempfile::tempdir().unwrap();
        let dir = tmp.path();
        let now = 7_500_000u64;

        // Drain the node bucket to empty.
        let cap = GITHUB_POLL_BUCKET_CAPACITY as u64;
        for _ in 0..cap {
            assert!(try_consume_github_poll_at(dir, NODE_POLL_BUCKET_FILE, now));
        }
        assert!(
            !try_consume_github_poll_at(dir, NODE_POLL_BUCKET_FILE, now),
            "node bucket drained"
        );

        // The install bucket is still full — the installer is not starved.
        for _ in 0..cap {
            assert!(
                try_consume_github_poll_at(dir, INSTALL_POLL_BUCKET_FILE, now),
                "install bucket must be unaffected by node-bucket drain"
            );
        }
    }

    #[test]
    fn test_get_latest_release_consults_install_bucket() {
        // Source pin: the supervisor-side installer fetch must gate on the
        // SEPARATE install bucket (not the node bucket), so the node cannot
        // starve it (#4073 Codex P1).
        let src = include_str!("update.rs");
        let (_, body) = src
            .split_once("async fn get_latest_release(")
            .expect("get_latest_release definition not found");
        let (head, _) = body
            .split_once("reqwest::Client::builder()")
            .expect("client builder not found");
        assert!(
            head.contains("try_consume_install_poll()"),
            "get_latest_release must consume an INSTALL-bucket token before hitting GitHub"
        );
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
