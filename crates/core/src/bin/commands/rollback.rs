//! Crash-loop auto-rollback for the Freenet auto-updater (#4073).
//!
//! Brick-safety: when a node auto-updates to a release that then crash-loops on
//! startup, this module reverts the node to the immediately-previous,
//! known-good binary and records the bad version locally so it is NOT
//! auto-re-applied. Without it, a bad release that wedges at boot leaves the
//! node down until an operator intervenes (systemd `StartLimitBurst` /
//! `StartLimitAction=none` stops the unit, the run-wrapper gives up after its
//! consecutive-failure cap).
//!
//! ## How it hooks the EXISTING crash detection (no parallel detector)
//!
//! Every Freenet supervisor that applies an auto-update already runs
//! `freenet update` immediately after the node process stops:
//!   * **systemd** (Linux): `ExecStopPost` runs `freenet update` on exit 42
//!     (healthy-then-died / update-needed) OR 45 (fast crash / boot-wedge,
//!     #4551).
//!   * **macOS launchd wrapper script**: runs `freenet update` on exit 42.
//!   * **in-process run-wrapper** (`service/wrapper.rs`, Windows + macOS tray):
//!     runs `freenet update` on exit 42.
//!
//! Each of those supervisors now sets [`POST_STOP_EXIT_CODE_ENV_VAR`] on that
//! post-stop `freenet update` invocation, carrying the exit code the node just
//! died with. `freenet update` reads it ([`post_stop_exit_code_from_env`]) and,
//! when the node is in post-update **probation** (see below), counts the stop as
//! a crash of the probationary version and rolls back once the crash count
//! crosses [`ROLLBACK_CRASH_THRESHOLD`]. This reuses the existing exit-code /
//! restart machinery rather than inventing a second watchdog.
//!
//! ## Lifecycle
//!
//! 1. **Install** (`commands::update`): before overwriting the running binary,
//!    snapshot it as the known-good rollback target ([`capture_known_good`]),
//!    then mark the freshly-installed version as on-probation
//!    ([`begin_probation`]).
//! 2. **Commit**: once the new version has run healthily for
//!    [`COMMIT_HEALTHY_UPTIME_SECS`] the node clears the probation marker
//!    ([`commit_probation`]). After that, ordinary later crashes never trigger a
//!    rollback. The window mirrors `MIN_HEALTHY_UPTIME_FOR_UPDATE_EXIT` (the
//!    same boundary that classifies a fatal exit as a fast crash).
//! 3. **Detect + revert**: a crash during probation increments the marker's
//!    crash counter ([`handle_post_stop`]); on the
//!    [`ROLLBACK_CRASH_THRESHOLD`]th crash the previous binary is restored, the
//!    bad version is pinned ([`pin_known_bad_at`]), and the marker is cleared.
//! 4. **Pin**: a pinned version is refused by both the installer
//!    (`commands::update`) and the node's update checks
//!    (`commands::auto_update`) so we never loop update → crash → revert →
//!    re-update. A later, strictly-newer release (a fix) is NOT pinned and is
//!    applied normally.
//!
//! ## Bounded by construction (no new flap/loop)
//!
//! * Rollback only ever restores the **known-good** target captured before the
//!   probationary install. We never forward-chase a newer version while in
//!   probation, so the rollback target is always a version that previously ran
//!   (one generation back). [`ROLLBACK_CRASH_THRESHOLD`] is `< 5`
//!   (`StartLimitBurst`) so a Linux rollback fires before systemd gives up.
//! * If the reverted version ALSO cannot stay up there is no longer a probation
//!   marker, so it is NOT rolled back again — the supervisor's existing bound
//!   (systemd `StartLimit` / the wrapper's consecutive-failure cap) takes over
//!   and the node is left for the operator.
//! * If the known-good binary is missing we refuse to "roll back" rather than
//!   risk deleting the only working binary.

use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

/// Environment variable a supervisor sets on the post-stop `freenet update`
/// invocation, carrying the exit code (`$EXIT_STATUS`) the node just stopped
/// with. Its presence tells `freenet update` it is being run as part of the
/// restart cycle (as opposed to a manual / tray-initiated update), which is the
/// only context in which a stop should be counted as a crash for rollback
/// purposes.
///
/// Forward/backward compatible by design (an env var, not a CLI flag): an OLD
/// `freenet` binary — e.g. the one we just rolled back TO — silently ignores it
/// instead of erroring on an unknown argument, and a NEW binary under an OLD
/// supervisor simply never sees it (and behaves exactly as before).
pub const POST_STOP_EXIT_CODE_ENV_VAR: &str = "FREENET_POST_STOP_EXIT_CODE";

/// Consecutive crashes of a probationary version before we roll back.
///
/// MUST stay strictly below the systemd unit's `StartLimitBurst` (5, see
/// `service/linux.rs`) so the rollback fires while systemd is still willing to
/// restart the unit — if we waited until the 5th crash, systemd's
/// `StartLimitAction=none` would already have stopped the unit and the
/// supervisor would never run the `ExecStopPost` that performs the rollback.
/// Three confirmed crashes is enough signal: a single crash is noise, two could
/// be unlucky, by the third the new version is demonstrably not staying up.
pub(crate) const ROLLBACK_CRASH_THRESHOLD: u32 = 3;

/// Brick-safety invariant, enforced at compile time: a Linux rollback fires
/// from the `ExecStopPost` of the Nth probation crash, so it must happen BEFORE
/// systemd's `StartLimitBurst` (5, see `service/linux.rs`) stops the unit and
/// removes the chance to run `ExecStopPost` at all.
const _: () = assert!(
    ROLLBACK_CRASH_THRESHOLD < 5,
    "ROLLBACK_CRASH_THRESHOLD must be < systemd StartLimitBurst (5) in service/linux.rs"
);

/// How long a freshly-installed version must run before it is considered
/// committed (probation cleared). Mirrors
/// `node::p2p_impl::MIN_HEALTHY_UPTIME_FOR_UPDATE_EXIT` (60s): a fatal exit
/// before this boundary is the fast-crash / boot-wedge we guard against, so a
/// node that survives past it has cleared the same bar.
pub(crate) const COMMIT_HEALTHY_UPTIME_SECS: u64 = 60;

/// Probation marker: JSON describing the on-probation version and its
/// known-good rollback target.
const PROBATION_FILE: &str = "update_probation.json";

/// Pinned known-bad version (plain text, a single version string). The
/// installer and the node's update checks both refuse to (re-)apply this exact
/// version.
const KNOWN_BAD_FILE: &str = "known_bad_version";

/// Snapshot of the previous, known-good binary kept as the rollback target.
const KNOWN_GOOD_BINARY_FILE: &str = "known_good_binary";

/// Persisted post-update probation record.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct ProbationState {
    /// The version that was just installed and is on probation.
    pub new_version: String,
    /// The previous, known-good version we would roll back to.
    pub previous_version: String,
    /// Path to the retained known-good binary blob ([`known_good_binary_path`]).
    pub rollback_binary: PathBuf,
    /// Path to the live binary to restore over on rollback.
    pub target_binary: PathBuf,
    /// Unix seconds when the probationary version was installed (diagnostics).
    pub installed_at_unix: u64,
    /// Number of crashes observed during this probation so far.
    pub crash_count: u32,
}

/// Outcome of [`handle_post_stop`], telling the caller (`commands::update`) what
/// to do next.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum PostStopOutcome {
    /// Not a probation crash (no probation, or a stale marker): the caller
    /// should run the normal update flow.
    Proceed,
    /// A probation crash was recorded but the rollback threshold is not yet
    /// reached. The caller must NOT perform a normal update this cycle; the
    /// supervisor will restart the same (probationary) binary.
    CrashRecorded { crash_count: u32 },
    /// The previous binary was restored. The caller should exit "success" so
    /// the supervisor restarts the now-rolled-back binary.
    RolledBack {
        restored_version: String,
        bad_version: String,
    },
    /// A rollback was warranted but could not be performed (no retained
    /// known-good binary, or the restore failed). The caller must NOT perform a
    /// normal update; the node is left for the supervisor's own bound /
    /// operator intervention.
    RollbackUnavailable { reason: String },
}

// ── State directory plumbing ───────────────────────────────────────────────

fn state_dir() -> Option<PathBuf> {
    super::auto_update::state_dir()
}

/// Path to the retained known-good binary blob, if the state dir resolves.
pub(crate) fn known_good_binary_path() -> Option<PathBuf> {
    state_dir().map(|d| d.join(KNOWN_GOOD_BINARY_FILE))
}

// ── Known-good capture ─────────────────────────────────────────────────────

/// Copy `current_binary` to the known-good snapshot, atomically (copy to a
/// temp sibling, then rename). Called BEFORE the install overwrites the live
/// binary, and ONLY when transitioning from a committed/healthy version (not
/// while already on probation), so the snapshot is always a version that
/// previously ran.
pub(crate) fn capture_known_good(current_binary: &Path) -> Result<()> {
    let dir = state_dir().context("could not resolve state directory")?;
    capture_known_good_at(&dir, current_binary)
}

pub(crate) fn capture_known_good_at(dir: &Path, current_binary: &Path) -> Result<()> {
    std::fs::create_dir_all(dir).context("failed to create state directory")?;
    let dest = dir.join(KNOWN_GOOD_BINARY_FILE);
    let tmp = dir.join(format!("{KNOWN_GOOD_BINARY_FILE}.tmp"));
    std::fs::copy(current_binary, &tmp).context("failed to snapshot known-good binary")?;
    set_executable(&tmp);
    if let Err(e) = std::fs::rename(&tmp, &dest) {
        let _rm = std::fs::remove_file(&tmp);
        return Err(e).context("failed to install known-good binary snapshot");
    }
    Ok(())
}

// ── Probation marker ───────────────────────────────────────────────────────

/// Begin (or refresh) probation for `new_version`, just installed over the live
/// binary at `target_binary`. `version_being_replaced` is the version of the
/// binary that was overwritten.
///
/// Carries the known-good rollback target forward: if a probation marker
/// already exists (a chained update WHILE on probation — e.g. a manual
/// `freenet update`), we keep the existing `previous_version` /
/// `rollback_binary` rather than treating the unproven probationary binary as
/// known-good. Clears any known-bad pin, since a successful forward install
/// means we have moved on.
pub(crate) fn begin_probation(
    new_version: &str,
    version_being_replaced: &str,
    target_binary: &Path,
) {
    if let Some(dir) = state_dir() {
        begin_probation_at(
            dir.as_path(),
            new_version,
            version_being_replaced,
            target_binary,
        );
    }
}

pub(crate) fn begin_probation_at(
    dir: &Path,
    new_version: &str,
    version_being_replaced: &str,
    target_binary: &Path,
) {
    let _mkdir = std::fs::create_dir_all(dir);
    let rollback_binary = dir.join(KNOWN_GOOD_BINARY_FILE);
    // Preserve the known-good baseline across a chained in-probation install.
    let previous_version = match read_probation_at(dir) {
        Some(existing) => existing.previous_version,
        None => version_being_replaced.to_string(),
    };
    let state = ProbationState {
        new_version: new_version.to_string(),
        previous_version,
        rollback_binary,
        target_binary: target_binary.to_path_buf(),
        installed_at_unix: now_unix(),
        crash_count: 0,
    };
    let _write = write_probation_at(dir, &state);
    // Moving forward to a new version supersedes any prior known-bad pin.
    clear_known_bad_at(dir);
}

/// Read the current probation marker, if any. A missing or unparseable marker
/// is reported as "no probation" (we cannot safely act on a corrupt marker).
pub(crate) fn read_probation() -> Option<ProbationState> {
    read_probation_at(state_dir()?.as_path())
}

pub(crate) fn read_probation_at(dir: &Path) -> Option<ProbationState> {
    let raw = std::fs::read_to_string(dir.join(PROBATION_FILE)).ok()?;
    serde_json::from_str(&raw).ok()
}

fn write_probation_at(dir: &Path, state: &ProbationState) -> Result<()> {
    let raw = serde_json::to_string(state).context("failed to serialize probation marker")?;
    std::fs::write(dir.join(PROBATION_FILE), raw).context("failed to write probation marker")
}

fn remove_probation_at(dir: &Path) {
    let _rm = std::fs::remove_file(dir.join(PROBATION_FILE));
}

/// Clear probation if the running version matches the marker (it has proven
/// healthy). A marker for a DIFFERENT version is stale (e.g. left over after a
/// rollback or external install) and is also removed. Returns `Some(version)`
/// when an in-probation version was committed (for logging).
pub fn commit_probation(current_version: &str) {
    if let Some(dir) = state_dir() {
        match commit_probation_at(dir.as_path(), current_version) {
            CommitOutcome::Committed => tracing::info!(
                version = current_version,
                "Auto-update probation passed: new version ran healthily for \
                 {COMMIT_HEALTHY_UPTIME_SECS}s; committing (rollback disarmed)."
            ),
            CommitOutcome::ClearedStale { marker_version } => tracing::debug!(
                running = current_version,
                marker = %marker_version,
                "Cleared stale auto-update probation marker for a different version."
            ),
            CommitOutcome::Nothing => {}
        }
    }
}

#[derive(Debug, PartialEq, Eq)]
pub(crate) enum CommitOutcome {
    /// The running version matched the probation marker and was committed.
    Committed,
    /// A marker for a different version was found and removed as stale.
    ClearedStale { marker_version: String },
    /// No probation marker present.
    Nothing,
}

pub(crate) fn commit_probation_at(dir: &Path, current_version: &str) -> CommitOutcome {
    match read_probation_at(dir) {
        Some(state) if state.new_version == current_version => {
            remove_probation_at(dir);
            CommitOutcome::Committed
        }
        Some(state) => {
            // Running a different version healthily than the marker describes:
            // the marker can no longer protect anything, so drop it.
            remove_probation_at(dir);
            CommitOutcome::ClearedStale {
                marker_version: state.new_version,
            }
        }
        None => CommitOutcome::Nothing,
    }
}

// ── Known-bad pin ──────────────────────────────────────────────────────────

pub(crate) fn pin_known_bad_at(dir: &Path, version: &str) -> Result<()> {
    let _mkdir = std::fs::create_dir_all(dir);
    std::fs::write(dir.join(KNOWN_BAD_FILE), format!("{version}\n"))
        .context("failed to write known-bad pin")
}

pub(crate) fn read_known_bad_at(dir: &Path) -> Option<String> {
    let raw = std::fs::read_to_string(dir.join(KNOWN_BAD_FILE)).ok()?;
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        None
    } else {
        Some(trimmed.to_string())
    }
}

fn clear_known_bad_at(dir: &Path) {
    let _rm = std::fs::remove_file(dir.join(KNOWN_BAD_FILE));
}

/// Whether `version` is the locally pinned known-bad version that must not be
/// auto-(re)applied. Used by both the installer and the node's update checks.
pub fn is_version_pinned_bad(version: &str) -> bool {
    state_dir()
        .map(|d| is_version_pinned_bad_at(d.as_path(), version))
        .unwrap_or(false)
}

pub(crate) fn is_version_pinned_bad_at(dir: &Path, version: &str) -> bool {
    read_known_bad_at(dir).as_deref() == Some(version)
}

// ── Post-stop crash handling / rollback ────────────────────────────────────

/// Parse the [`POST_STOP_EXIT_CODE_ENV_VAR`] env var into the node's exit code,
/// if set by a supervisor.
pub fn post_stop_exit_code_from_env() -> Option<i32> {
    std::env::var(POST_STOP_EXIT_CODE_ENV_VAR)
        .ok()?
        .trim()
        .parse()
        .ok()
}

/// Handle a post-stop `freenet update` invocation for crash-loop rollback.
///
/// `current_version` is the version of the binary now running (the one the node
/// just stopped on). The decision is intentionally independent of whatever
/// release is "latest" on GitHub: while in probation we never forward-chase a
/// newer version (that would make the rollback target an unproven binary), so a
/// stop with no commit yet is treated as a crash of the probationary version.
pub(crate) fn handle_post_stop(current_version: &str) -> PostStopOutcome {
    match state_dir() {
        Some(dir) => handle_post_stop_at(dir.as_path(), current_version),
        None => PostStopOutcome::Proceed,
    }
}

pub(crate) fn handle_post_stop_at(dir: &Path, current_version: &str) -> PostStopOutcome {
    let Some(mut state) = read_probation_at(dir) else {
        // Not on probation: a normal supervised restart (e.g. a long-running
        // version that hit a transient fault, or a genuine update-needed exit).
        // Let the caller run the ordinary update flow.
        return PostStopOutcome::Proceed;
    };

    if state.new_version != current_version {
        // The marker describes a different version than the one that just
        // stopped — stale (e.g. an external install changed the binary). Drop
        // it and fall through to the normal flow rather than acting on it.
        remove_probation_at(dir);
        return PostStopOutcome::Proceed;
    }

    // In probation and the probationary version stopped without committing →
    // count it as a crash of that version.
    state.crash_count = state.crash_count.saturating_add(1);
    let _write = write_probation_at(dir, &state);

    if !should_rollback(state.crash_count) {
        return PostStopOutcome::CrashRecorded {
            crash_count: state.crash_count,
        };
    }

    // Threshold reached: roll back to the known-good binary.
    if !state.rollback_binary.exists() {
        // Never delete the only working binary: with no retained known-good we
        // cannot restore. Stop counting (the supervisor's own bound takes over)
        // and surface the situation for the operator.
        remove_probation_at(dir);
        return PostStopOutcome::RollbackUnavailable {
            reason: format!(
                "no retained known-good binary at {}",
                state.rollback_binary.display()
            ),
        };
    }

    // Pin BEFORE restoring so that even if the restore is interrupted we never
    // re-apply the bad version (loop-safety). Pin write failure is best-effort:
    // it lives in the same directory as the marker we just wrote.
    if let Err(e) = pin_known_bad_at(dir, &state.new_version) {
        tracing::warn!(error = %e, version = %state.new_version,
            "Failed to pin known-bad version during rollback (continuing)");
    }

    match restore_binary(&state.rollback_binary, &state.target_binary) {
        Ok(()) => {
            remove_probation_at(dir);
            PostStopOutcome::RolledBack {
                restored_version: state.previous_version.clone(),
                bad_version: state.new_version.clone(),
            }
        }
        Err(e) => {
            // Leave the marker in place so a subsequent crash retries the
            // restore (bounded by the supervisor's own crash limiter). The pin
            // is already set, so we will not re-apply the bad version meanwhile.
            PostStopOutcome::RollbackUnavailable {
                reason: format!("restore failed: {e:#}"),
            }
        }
    }
}

/// Pure rollback decision: roll back once probation crashes reach the threshold.
pub(crate) fn should_rollback(crash_count: u32) -> bool {
    crash_count >= ROLLBACK_CRASH_THRESHOLD
}

/// Restore `src` (the retained known-good binary) over `target` (the live
/// binary), without ever deleting `src`. Copies `src` to a same-directory temp,
/// moves the current (bad) binary aside, then renames the temp into place;
/// on failure the displaced binary is moved back so we never end up with no
/// binary at `target`. Moving the current binary aside first is required on
/// Windows, where a running `.exe` cannot be replaced by renaming over it but
/// CAN be renamed away — and is harmless on Unix.
fn restore_binary(src: &Path, target: &Path) -> Result<()> {
    let parent = target
        .parent()
        .context("target binary has no parent directory")?;
    let stem = target.file_name().unwrap_or_default().to_string_lossy();
    let temp = parent.join(format!(".{stem}.rollback.tmp"));
    let displaced = parent.join(format!(".{stem}.badver"));

    std::fs::copy(src, &temp).context("failed to copy known-good binary into place")?;
    set_executable(&temp);

    if displaced.exists() {
        let _rm = std::fs::remove_file(&displaced);
    }
    if target.exists() {
        std::fs::rename(target, &displaced).context("failed to move current binary aside")?;
    }
    if let Err(e) = std::fs::rename(&temp, target) {
        // Put the bad binary back so the node still has *a* binary to run.
        let _restore = std::fs::rename(&displaced, target);
        let _rm = std::fs::remove_file(&temp);
        return Err(e).context("failed to install rolled-back binary");
    }
    // Best-effort removal of the displaced bad binary (not running anymore).
    let _rm = std::fs::remove_file(&displaced);
    Ok(())
}

fn set_executable(path: &Path) {
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        if let Ok(meta) = std::fs::metadata(path) {
            let mut perms = meta.permissions();
            perms.set_mode(0o755);
            let _chmod = std::fs::set_permissions(path, perms);
        }
    }
    #[cfg(not(unix))]
    {
        let _ = path;
    }
}

fn now_unix() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn write_dummy_binary(path: &Path, contents: &[u8]) {
        std::fs::write(path, contents).unwrap();
        set_executable(path);
    }

    /// Build a probation marker in `dir` for `new_version` over a fake live
    /// binary, having captured a fake known-good binary first. Returns the live
    /// binary path.
    fn setup_probation(dir: &Path, new_version: &str, prev_version: &str) -> PathBuf {
        let bin_dir = dir.join("bin");
        std::fs::create_dir_all(&bin_dir).unwrap();
        let live = bin_dir.join("freenet");
        // The "previous" (known-good) binary content we will roll back to.
        write_dummy_binary(&live, b"GOOD-BINARY");
        capture_known_good_at(dir, &live).unwrap();
        // Now the live binary is "replaced" by the new (bad) version.
        write_dummy_binary(&live, b"BAD-BINARY");
        begin_probation_at(dir, new_version, prev_version, &live);
        live
    }

    #[test]
    fn probation_roundtrip_and_commit() {
        let tmp = tempfile::tempdir().unwrap();
        let dir = tmp.path();
        let live = setup_probation(dir, "0.2.84", "0.2.83");

        let state = read_probation_at(dir).expect("probation present");
        assert_eq!(state.new_version, "0.2.84");
        assert_eq!(state.previous_version, "0.2.83");
        assert_eq!(state.crash_count, 0);
        assert_eq!(state.target_binary, live);

        // Committing the matching version clears the marker.
        assert_eq!(commit_probation_at(dir, "0.2.84"), CommitOutcome::Committed);
        assert!(read_probation_at(dir).is_none());
        // Idempotent.
        assert_eq!(commit_probation_at(dir, "0.2.84"), CommitOutcome::Nothing);
    }

    #[test]
    fn commit_clears_stale_marker_for_other_version() {
        let tmp = tempfile::tempdir().unwrap();
        let dir = tmp.path();
        setup_probation(dir, "0.2.84", "0.2.83");
        // A different version is running healthily → stale marker removed.
        let CommitOutcome::ClearedStale { marker_version } = commit_probation_at(dir, "0.2.85")
        else {
            panic!("expected ClearedStale");
        };
        assert_eq!(marker_version, "0.2.84");
        assert!(read_probation_at(dir).is_none());
    }

    #[test]
    fn crash_during_probation_records_then_rolls_back_and_pins() {
        let tmp = tempfile::tempdir().unwrap();
        let dir = tmp.path();
        let live = setup_probation(dir, "0.2.84", "0.2.83");

        // First crashes below threshold are recorded, no rollback.
        for expected in 1..ROLLBACK_CRASH_THRESHOLD {
            let PostStopOutcome::CrashRecorded { crash_count } = handle_post_stop_at(dir, "0.2.84")
            else {
                panic!("expected CrashRecorded({expected})");
            };
            assert_eq!(crash_count, expected);
            // Still the bad binary, still pinned to nothing.
            assert_eq!(std::fs::read(&live).unwrap(), b"BAD-BINARY");
            assert!(read_known_bad_at(dir).is_none());
            assert!(read_probation_at(dir).is_some());
        }

        // Threshold crash → rollback.
        let PostStopOutcome::RolledBack {
            restored_version,
            bad_version,
        } = handle_post_stop_at(dir, "0.2.84")
        else {
            panic!("expected RolledBack");
        };
        assert_eq!(restored_version, "0.2.83");
        assert_eq!(bad_version, "0.2.84");

        // Live binary is the known-good content again.
        assert_eq!(std::fs::read(&live).unwrap(), b"GOOD-BINARY");
        // Bad version pinned; probation cleared.
        assert!(is_version_pinned_bad_at(dir, "0.2.84"));
        assert!(read_probation_at(dir).is_none());
        // The retained known-good blob is NOT consumed by the restore.
        assert!(known_good_path(dir).exists());
    }

    fn known_good_path(dir: &Path) -> PathBuf {
        dir.join(KNOWN_GOOD_BINARY_FILE)
    }

    #[test]
    fn pinned_version_is_not_reapplied_but_newer_is_allowed() {
        let tmp = tempfile::tempdir().unwrap();
        let dir = tmp.path();
        pin_known_bad_at(dir, "0.2.84").unwrap();

        assert!(is_version_pinned_bad_at(dir, "0.2.84"));
        // A different (newer fix) version is not pinned.
        assert!(!is_version_pinned_bad_at(dir, "0.2.85"));
        // The previous good version is not pinned.
        assert!(!is_version_pinned_bad_at(dir, "0.2.83"));

        // A successful forward install supersedes the pin.
        let live = dir.join("freenet");
        write_dummy_binary(&live, b"NEWER");
        begin_probation_at(dir, "0.2.85", "0.2.83", &live);
        assert!(!is_version_pinned_bad_at(dir, "0.2.84"));
    }

    #[test]
    fn rollback_is_bounded_to_one_generation() {
        // After a rollback there is no probation marker, so a further crash of
        // the rolled-back version does NOT trigger a second rollback.
        let tmp = tempfile::tempdir().unwrap();
        let dir = tmp.path();
        setup_probation(dir, "0.2.84", "0.2.83");
        for _ in 0..ROLLBACK_CRASH_THRESHOLD {
            handle_post_stop_at(dir, "0.2.84");
        }
        assert!(read_probation_at(dir).is_none());
        // Now running the rolled-back version; another stop is a no-op Proceed.
        assert_eq!(handle_post_stop_at(dir, "0.2.83"), PostStopOutcome::Proceed);
    }

    #[test]
    fn no_probation_means_proceed() {
        let tmp = tempfile::tempdir().unwrap();
        assert_eq!(
            handle_post_stop_at(tmp.path(), "0.2.84"),
            PostStopOutcome::Proceed
        );
    }

    #[test]
    fn stale_marker_for_other_version_proceeds_and_is_cleared() {
        let tmp = tempfile::tempdir().unwrap();
        let dir = tmp.path();
        setup_probation(dir, "0.2.84", "0.2.83");
        // A DIFFERENT version stopped than the marker describes.
        assert_eq!(handle_post_stop_at(dir, "0.2.99"), PostStopOutcome::Proceed);
        assert!(read_probation_at(dir).is_none());
    }

    #[test]
    fn rollback_unavailable_when_known_good_missing() {
        let tmp = tempfile::tempdir().unwrap();
        let dir = tmp.path();
        let live = setup_probation(dir, "0.2.84", "0.2.83");
        // Remove the retained known-good blob to simulate a missing target.
        std::fs::remove_file(known_good_path(dir)).unwrap();

        for _ in 0..ROLLBACK_CRASH_THRESHOLD - 1 {
            handle_post_stop_at(dir, "0.2.84");
        }
        assert!(matches!(
            handle_post_stop_at(dir, "0.2.84"),
            PostStopOutcome::RollbackUnavailable { .. }
        ));
        // Bad binary still in place (we never deleted the only binary).
        assert_eq!(std::fs::read(&live).unwrap(), b"BAD-BINARY");
        // Marker removed so we stop futile rollback attempts.
        assert!(read_probation_at(dir).is_none());
    }

    #[test]
    fn begin_probation_preserves_known_good_across_chained_install() {
        // A chained install WHILE on probation must not adopt the unproven
        // probationary version as the rollback target.
        let tmp = tempfile::tempdir().unwrap();
        let dir = tmp.path();
        let live = setup_probation(dir, "0.2.84", "0.2.83");

        // Chained forward install to 0.2.85 while still on probation for 0.2.84.
        write_dummy_binary(&live, b"NEWEST");
        begin_probation_at(dir, "0.2.85", "0.2.84", &live);

        let state = read_probation_at(dir).unwrap();
        assert_eq!(state.new_version, "0.2.85");
        // Previous version is still the original known-good, not 0.2.84.
        assert_eq!(state.previous_version, "0.2.83");
    }

    #[test]
    fn restore_binary_preserves_source_and_replaces_target() {
        let tmp = tempfile::tempdir().unwrap();
        let dir = tmp.path();
        let src = dir.join("good");
        let target = dir.join("live");
        write_dummy_binary(&src, b"SRC-GOOD");
        write_dummy_binary(&target, b"TARGET-BAD");

        restore_binary(&src, &target).unwrap();
        assert_eq!(std::fs::read(&target).unwrap(), b"SRC-GOOD");
        // Source binary is preserved (never deleted).
        assert_eq!(std::fs::read(&src).unwrap(), b"SRC-GOOD");
    }

    #[test]
    fn should_rollback_threshold() {
        assert!(!should_rollback(0));
        assert!(!should_rollback(ROLLBACK_CRASH_THRESHOLD - 1));
        assert!(should_rollback(ROLLBACK_CRASH_THRESHOLD));
        assert!(should_rollback(ROLLBACK_CRASH_THRESHOLD + 1));
    }

    #[test]
    fn corrupt_probation_marker_reads_as_none() {
        let tmp = tempfile::tempdir().unwrap();
        let dir = tmp.path();
        std::fs::write(dir.join(PROBATION_FILE), "{not valid json").unwrap();
        assert!(read_probation_at(dir).is_none());
        // And a post-stop with a corrupt marker proceeds normally.
        assert_eq!(handle_post_stop_at(dir, "0.2.84"), PostStopOutcome::Proceed);
    }
}
