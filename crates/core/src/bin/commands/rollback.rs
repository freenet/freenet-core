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
//! ## What counts as a crash (coverage)
//!
//! A "crash" for rollback purposes is ANY non-graceful stop of the
//! probationary version — not just the network-listener watchdog exits
//! (42/45). It covers:
//!   * panics (exit 101 with the default unwinding runtime, SIGABRT/134 under
//!     `panic=abort`),
//!   * early-startup failures (exit 1 from config / `node.build()` / a
//!     non-listener task dying before the watchdog arms),
//!   * signal deaths — SIGSEGV (139), SIGKILL / OOM-kill (137), SIGABRT (134),
//!     which systemd surfaces as numeric codes or signal *names*
//!     (`SEGV`/`KILL`/`ABRT`), both of which classify as a crash,
//!   * the fast-crash watchdog exit 45 (sub-60s fatal listener exit, #4551).
//!
//! DEPLOY WATCH (accepted tradeoff): an OOM-kill (SIGKILL/137) of the
//! probationary version during the probation window counts toward rollback.
//! This is intentional — a real memory-regression OOM is exactly the kind of
//! bad release we want to roll back. But the production peer cap is 2 GB
//! (`MemoryMax=2G`), so a legitimately memory-heavy-but-fine release could be
//! OOM-killed on a capped peer and rolled back. After deploying a release that
//! changes the memory profile, watch the 2 GB-capped peers for rollback churn
//! and bump the cap (or fix the regression) rather than letting good releases
//! bounce. The probation window is short (until first 60s-healthy boot), so a
//! release that survives its first minute on a capped peer is not affected.
//!
//! It deliberately does NOT count as a crash:
//!   * exit 0 (graceful shutdown), 43 (another instance already running),
//!   * exit 42. On a systemd unit (which sets [`SYSTEMD_FAST_CRASH_ENV_VAR`])
//!     a real sub-60s crash uses 45, so a 42 is the node *voluntarily*
//!     stepping forward to a newer release — counting it would cause a
//!     spurious backward rollback during a release+hotfix cascade. On the
//!     macOS/in-process wrappers (no fast-crash marker) the node emits 42 for
//!     *both* a fatal-listener boot-wedge and a voluntary update, so 42 is
//!     irreducibly ambiguous there; we treat it as NOT-a-crash to avoid the
//!     spurious rollback, accepting that a fatal-listener boot-wedge under
//!     those wrappers is bounded by the wrapper's own consecutive-failure cap
//!     rather than auto-rolled-back (unchanged from pre-#4073). Panics /
//!     signals / early errors under those wrappers DO roll back, since they
//!     use unambiguous crash codes.
//!
//! Every supervisor that runs `freenet update` after the node stops forwards
//! the node's exit status via [`POST_STOP_EXIT_CODE_ENV_VAR`]; `freenet update`
//! classifies it ([`classify_stop`]) and, when in probation, counts a crash —
//! **before** any GitHub call, so the count is recorded even if the network is
//! down.
//!
//! ## Lifecycle
//!
//! 1. **Install** (`commands::update`): before overwriting the running binary,
//!    snapshot it as the known-good rollback target ([`capture_known_good`],
//!    fsync'd, with size+SHA-256 recorded), then mark the freshly-installed
//!    version as on-probation ([`begin_probation`]).
//! 2. **Commit**: once the new version has run healthily for
//!    [`COMMIT_HEALTHY_UPTIME_SECS`] the node clears the probation marker
//!    ([`commit_probation`]). After that, ordinary later crashes never trigger
//!    a rollback.
//! 3. **Detect + revert**: a crash during probation increments the marker's
//!    crash counter ([`handle_post_stop`]); on the
//!    [`ROLLBACK_CRASH_THRESHOLD`]th crash the previous binary is restored
//!    (after verifying its integrity), the bad version is pinned
//!    ([`pin_known_bad_at`]), and the marker is cleared.
//! 4. **Pin**: a pinned version is refused by both the installer
//!    (`commands::update`) and the node's update checks
//!    (`commands::auto_update`) so we never loop update -> crash -> revert ->
//!    re-update. A later, strictly-newer release (a fix) is NOT pinned and is
//!    applied normally.
//!
//! ## Bounded by construction (no new flap/loop)
//!
//! * Rollback only ever restores the **known-good** target captured before the
//!   probationary install, so the rollback target is always a version that
//!   previously ran (one generation back). [`ROLLBACK_CRASH_THRESHOLD`] is
//!   `< 5` (`StartLimitBurst`) so a Linux rollback fires before systemd gives
//!   up, and `< 50` (`WRAPPER_MAX_CONSECUTIVE_FAILURES`) for the wrapper.
//! * After a rollback there is no probation marker, so a still-crashing
//!   reverted version is NOT rolled back again — the supervisor's existing
//!   bound takes over and the node is left for the operator.
//! * The known-good blob is integrity-checked (size + SHA-256) before any
//!   restore; a corrupt/truncated blob yields [`PostStopOutcome::RollbackUnavailable`]
//!   and the running binary is left untouched (never deletes the only working
//!   binary). The known-bad pin is persisted BEFORE the restore; if it can't
//!   be persisted the restore is abandoned (no restore-then-loop oscillation).
//! * Marker is age-bounded: a probation older than [`PROBATION_MAX_AGE_SECS`]
//!   is treated as stale (committed) so a never-cleared marker cannot roll back
//!   a long-healthy version much later (GC discipline, AGENTS.md).

use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

/// Environment variable a supervisor sets on the post-stop `freenet update`
/// invocation, carrying the status (`$EXIT_STATUS`) the node just stopped with.
/// Its value may be a numeric exit code OR a signal name (systemd passes e.g.
/// `SEGV`/`ABRT`/`KILL` for signal deaths), so it is forwarded and classified
/// as an opaque string ([`classify_stop`]).
///
/// Its presence tells `freenet update` it is being run as part of the restart
/// cycle (as opposed to a manual / tray-initiated update), which is the only
/// context in which a stop should be counted as a crash.
///
/// Forward/backward compatible by design (an env var, not a CLI flag): an OLD
/// `freenet` binary — e.g. the one we just rolled back TO — silently ignores it
/// instead of erroring on an unknown argument, and a NEW binary under an OLD
/// supervisor simply never sees it (and behaves exactly as before).
pub const POST_STOP_EXIT_CODE_ENV_VAR: &str = "FREENET_POST_STOP_EXIT_CODE";

/// Consecutive crashes of a probationary version before we roll back.
///
/// MUST stay strictly below the systemd unit's `StartLimitBurst` (5) AND the
/// run-wrapper's `WRAPPER_MAX_CONSECUTIVE_FAILURES` (50) so the rollback fires
/// while the supervisor is still willing to restart — see the compile-time
/// asserts below. Three confirmed crashes is enough signal: one is noise, two
/// could be unlucky, by the third the new version is demonstrably not staying
/// up.
pub(crate) const ROLLBACK_CRASH_THRESHOLD: u32 = 3;

/// Brick-safety invariant, enforced at compile time: a Linux rollback fires
/// from the `ExecStopPost` of the Nth probation crash, so it must happen BEFORE
/// systemd's `StartLimitBurst` (5, see `service/linux.rs`) stops the unit and
/// removes the chance to run `ExecStopPost` at all.
const _: () = assert!(
    ROLLBACK_CRASH_THRESHOLD < 5,
    "ROLLBACK_CRASH_THRESHOLD must be < systemd StartLimitBurst (5) in service/linux.rs"
);

/// Same invariant for the in-process run-wrapper, whose
/// `WRAPPER_MAX_CONSECUTIVE_FAILURES` (50, see `service.rs`) bounds its retry
/// loop: the rollback must fire well before the wrapper gives up.
const _: () = assert!(
    ROLLBACK_CRASH_THRESHOLD < 50,
    "ROLLBACK_CRASH_THRESHOLD must be < WRAPPER_MAX_CONSECUTIVE_FAILURES (50) in service.rs"
);

/// How long a freshly-installed version must run before it is considered
/// committed (probation cleared). Mirrors
/// `node::p2p_impl::MIN_HEALTHY_UPTIME_FOR_UPDATE_EXIT` (60s): a fatal exit
/// before this boundary is the fast-crash / boot-wedge we guard against.
///
/// The commit timer starts at process main (`run_network_node_with_signals`),
/// which is marginally EARLIER than the listener-start origin the fast-crash
/// classifier (`fatal_listener_exit_code`) uses. That skew is fail-safe: the
/// marker is cleared a beat early, which can only make us roll back LESS, never
/// roll back a node that has clearly passed the fast-crash window.
pub(crate) const COMMIT_HEALTHY_UPTIME_SECS: u64 = 60;

/// Absolute age after which a probation marker is treated as stale and cleared
/// (GC discipline: cleanup exemptions must be time-bounded — AGENTS.md). If the
/// commit timer ever fails to clear the marker, this stops a long-healthy
/// version from being rolled back by a much-later transient crash. One hour is
/// far longer than any real crash-loop (which trips the
/// [`ROLLBACK_CRASH_THRESHOLD`] within minutes) yet short enough that a marker
/// surviving it is unambiguously stale.
pub(crate) const PROBATION_MAX_AGE_SECS: u64 = 3600;

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
    /// Path to the retained known-good binary blob (the `known_good_binary`
    /// file in the state dir).
    pub rollback_binary: PathBuf,
    /// Path to the live binary to restore over on rollback.
    pub target_binary: PathBuf,
    /// Size in bytes of the known-good blob at capture (integrity check).
    pub rollback_size: u64,
    /// Lowercase hex SHA-256 of the known-good blob at capture (integrity).
    pub rollback_sha256: String,
    /// Unix seconds when the probationary version was installed.
    pub installed_at_unix: u64,
    /// Number of crashes observed during this probation so far.
    pub crash_count: u32,
}

/// Size + SHA-256 of the captured known-good binary, returned by
/// [`capture_known_good`] and stored in the probation marker for the
/// pre-restore integrity check.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct KnownGoodMeta {
    pub size: u64,
    pub sha256: String,
}

/// Classification of a post-stop status for rollback purposes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum StopClass {
    /// A crash: count toward rollback (when in probation).
    Crash,
    /// A clean / voluntary-update stop: do NOT count.
    NotCrash,
}

/// Outcome of [`handle_post_stop`], telling the caller (`commands::update`) what
/// to do next.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum PostStopOutcome {
    /// Not a probation crash (clean/voluntary stop, no probation, or a stale
    /// marker): the caller should run the normal update flow.
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
    /// A rollback was warranted but could not be performed safely (no retained
    /// known-good binary, a failed integrity check, an unpersistable pin, or a
    /// failed restore). The caller must NOT perform a normal update; the
    /// running binary is left untouched for the supervisor's own bound /
    /// operator intervention.
    RollbackUnavailable { reason: String },
}

// ── State directory plumbing ───────────────────────────────────────────────

fn state_dir() -> Option<PathBuf> {
    super::auto_update::state_dir()
}

// ── Durable write helpers (S7: atomic marker/pin; S3: fsync) ───────────────

/// Best-effort directory fsync so a preceding rename is durable. A no-op where
/// the platform won't open a directory for sync (e.g. Windows).
fn fsync_dir(dir: &Path) {
    if let Ok(f) = std::fs::File::open(dir) {
        let _sync = f.sync_all();
    }
}

/// Write `bytes` to `path` atomically and durably: write a sibling temp,
/// fsync it, rename into place, then fsync the directory. Used for the
/// probation marker and the known-bad pin so a torn write can never leave a
/// half-written marker/pin that mis-drives the rollback decision.
fn atomic_write(path: &Path, bytes: &[u8]) -> Result<()> {
    use std::io::Write;
    let dir = path.parent().context("path has no parent directory")?;
    std::fs::create_dir_all(dir).ok();
    let file_name = path
        .file_name()
        .context("path has no file name")?
        .to_string_lossy();
    let tmp = dir.join(format!(".{file_name}.tmp"));
    {
        let mut f = std::fs::File::create(&tmp).context("create temp for atomic write")?;
        f.write_all(bytes).context("write temp for atomic write")?;
        f.sync_all().context("fsync temp for atomic write")?;
    }
    if let Err(e) = std::fs::rename(&tmp, path) {
        let _rm = std::fs::remove_file(&tmp);
        return Err(e).context("rename temp into place");
    }
    fsync_dir(dir);
    Ok(())
}

/// SHA-256 + byte length of a file, streamed in chunks.
fn sha256_file(path: &Path) -> Result<(u64, String)> {
    use sha2::{Digest, Sha256};
    use std::io::Read;
    let mut file = std::fs::File::open(path).context("open file for hashing")?;
    let mut hasher = Sha256::new();
    let mut buf = [0u8; 64 * 1024];
    let mut size: u64 = 0;
    loop {
        let n = file.read(&mut buf).context("read file for hashing")?;
        if n == 0 {
            break;
        }
        size += n as u64;
        hasher.update(&buf[..n]);
    }
    let hex = hasher
        .finalize()
        .iter()
        .fold(String::with_capacity(64), |mut s, b| {
            use std::fmt::Write;
            write!(s, "{b:02x}").expect("writing to String is infallible");
            s
        });
    Ok((size, hex))
}

// ── Known-good capture ─────────────────────────────────────────────────────

/// Copy `current_binary` to the known-good snapshot, durably (copy to a temp
/// sibling, fsync, rename, fsync dir), and return its size + SHA-256. Called by
/// [`prepare_known_good_for_install_at`] BEFORE the install overwrites the live
/// binary, so the snapshot is always a version that previously ran.
pub(crate) fn capture_known_good_at(dir: &Path, current_binary: &Path) -> Result<KnownGoodMeta> {
    use std::io::Write;
    std::fs::create_dir_all(dir).context("failed to create state directory")?;
    let dest = dir.join(KNOWN_GOOD_BINARY_FILE);
    let tmp = dir.join(format!("{KNOWN_GOOD_BINARY_FILE}.tmp"));

    // Copy + fsync the blob so a power loss can't leave a truncated snapshot
    // that would later be restored over a working binary (S3).
    {
        let mut src = std::fs::File::open(current_binary).context("open current binary")?;
        let mut dst = std::fs::File::create(&tmp).context("create known-good temp")?;
        std::io::copy(&mut src, &mut dst).context("copy known-good binary")?;
        dst.flush().ok();
        dst.sync_all().context("fsync known-good temp")?;
    }
    set_executable(&tmp);
    if let Err(e) = std::fs::rename(&tmp, &dest) {
        let _rm = std::fs::remove_file(&tmp);
        return Err(e).context("install known-good binary snapshot");
    }
    fsync_dir(dir);

    let (size, sha256) = sha256_file(&dest).context("hash known-good snapshot")?;
    Ok(KnownGoodMeta { size, sha256 })
}

// ── Probation marker ───────────────────────────────────────────────────────

/// Decide the rollback target for an install and snapshot it if needed,
/// returning `(known_good_meta, previous_version)` — the integrity metadata of
/// the rollback blob AND the version label that MUST accompany it.
///
/// The blob and the label are computed together here, in one place, so they can
/// never diverge (the bug this fixes: capturing fresh in one site while
/// carrying a stale label forward in another, which would later restore the
/// wrong binary and report the wrong version).
///
/// - **Genuine chained in-probation install** — the existing marker is for the
///   version we are replacing AND the known-good blob is still present: keep the
///   existing blob (its recorded integrity meta) and carry its
///   `previous_version` forward. The live binary is the unproven new version, so
///   it must NOT become the known-good.
/// - **Otherwise** — no marker, a stale marker for a different version, OR the
///   blob was externally deleted: snapshot the about-to-be-replaced
///   `current_binary` fresh and label it `current_version`, so the blob and the
///   recorded `previous_version` always agree.
///
/// `Err` only if a needed fresh capture fails (full/unwritable state dir); the
/// caller treats that as "no rollback protection this cycle".
pub(crate) fn prepare_known_good_for_install(
    current_version: &str,
    current_binary: &Path,
) -> Result<(KnownGoodMeta, String)> {
    let dir = state_dir().context("could not resolve state directory")?;
    prepare_known_good_for_install_at(&dir, current_version, current_binary)
}

pub(crate) fn prepare_known_good_for_install_at(
    dir: &Path,
    current_version: &str,
    current_binary: &Path,
) -> Result<(KnownGoodMeta, String)> {
    if let Some(existing) = read_probation_at(dir) {
        if existing.new_version == current_version && dir.join(KNOWN_GOOD_BINARY_FILE).exists() {
            return Ok((
                KnownGoodMeta {
                    size: existing.rollback_size,
                    sha256: existing.rollback_sha256,
                },
                existing.previous_version,
            ));
        }
    }
    let meta = capture_known_good_at(dir, current_binary)?;
    Ok((meta, current_version.to_string()))
}

/// Begin (or refresh) probation for `new_version`, just installed over the live
/// binary at `target_binary`. `previous_version` and `meta` are the label and
/// integrity metadata of the rollback target, computed together by
/// [`prepare_known_good_for_install`] so they cannot diverge. Clears any
/// known-bad pin, since a successful forward install means we have moved on.
///
/// Returns `Err` if the probation marker could not be persisted (full /
/// unwritable state dir). In that case the update still succeeded but the new
/// version has NO crash-loop rollback protection, so the caller MUST surface
/// the error rather than discard it.
pub(crate) fn begin_probation(
    new_version: &str,
    previous_version: &str,
    target_binary: &Path,
    meta: &KnownGoodMeta,
) -> Result<()> {
    match state_dir() {
        Some(dir) => begin_probation_at(
            dir.as_path(),
            new_version,
            previous_version,
            target_binary,
            meta,
        ),
        // No state dir means the earlier known-good capture would also have
        // failed (so this is unreachable in practice); nothing to arm.
        None => Ok(()),
    }
}

pub(crate) fn begin_probation_at(
    dir: &Path,
    new_version: &str,
    previous_version: &str,
    target_binary: &Path,
    meta: &KnownGoodMeta,
) -> Result<()> {
    let _mkdir = std::fs::create_dir_all(dir);
    let state = ProbationState {
        new_version: new_version.to_string(),
        // Stored verbatim — the caller (prepare_known_good_for_install) is the
        // single source of truth for the blob/label pairing.
        previous_version: previous_version.to_string(),
        rollback_binary: dir.join(KNOWN_GOOD_BINARY_FILE),
        target_binary: target_binary.to_path_buf(),
        rollback_size: meta.size,
        rollback_sha256: meta.sha256.clone(),
        installed_at_unix: now_unix(),
        crash_count: 0,
    };
    // Moving forward to a new (non-pinned) version supersedes any prior
    // known-bad pin; best-effort, independent of the marker write.
    clear_known_bad_at(dir);
    write_probation_at(dir, &state)
        .context("failed to write crash-loop probation marker; the installed version has no rollback protection")
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
    let raw = serde_json::to_vec(state).context("serialize probation marker")?;
    atomic_write(&dir.join(PROBATION_FILE), &raw)
}

fn remove_probation_at(dir: &Path) {
    let _rm = std::fs::remove_file(dir.join(PROBATION_FILE));
}

/// Clear probation if the running version matches the marker (it has proven
/// healthy). A marker for a DIFFERENT version is stale (e.g. left over after a
/// rollback or external install) and is also removed.
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
    atomic_write(&dir.join(KNOWN_BAD_FILE), format!("{version}\n").as_bytes())
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

// ── Post-stop crash classification / handling / rollback ───────────────────

/// Read the raw post-stop status string the supervisor forwarded, if any. May
/// be a numeric exit code or a signal name; classified by [`classify_stop`].
pub fn post_stop_status_from_env() -> Option<String> {
    let raw = std::env::var(POST_STOP_EXIT_CODE_ENV_VAR).ok()?;
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        None
    } else {
        Some(trimmed.to_string())
    }
}

/// Classify a forwarded post-stop status as a crash or not. See the module-level
/// "What counts as a crash" section for the full rationale, including why exit
/// 42 is never counted.
pub(crate) fn classify_stop(status: &str) -> StopClass {
    match status.trim() {
        // Clean / non-crash node exits:
        //   0  graceful shutdown
        //   2  EXIT_CODE_ALREADY_UP_TO_DATE (an updater code, not a node crash)
        //   43 EXIT_CODE_ALREADY_RUNNING (another instance holds the port)
        //   44 EXIT_CODE_BUNDLE_UPDATE_STAGED (internal updater code)
        "0" | "2" | "43" | "44" => StopClass::NotCrash,
        // 42 = FATAL_LISTENER_EXIT_CODE / update-needed. Treated as NOT a crash
        // everywhere: under the systemd fast-crash marker a real sub-60s crash
        // would be 45, so 42 is a voluntary forward-update; under the wrappers
        // 42 is ambiguous and we choose not-a-crash to avoid a spurious
        // backward rollback (see module docs).
        "42" => StopClass::NotCrash,
        // 45 (fast crash), 1, 101, 134/137/139, signal names (SEGV/ABRT/KILL),
        // or any other/unrecognized status -> a crash.
        _ => StopClass::Crash,
    }
}

/// Handle a post-stop `freenet update` invocation for crash-loop rollback.
///
/// `status` is the forwarded `$EXIT_STATUS`; `current_version` is the version of
/// the binary that just stopped. No network call is made, so the crash count is
/// recorded even when GitHub is unreachable.
pub(crate) fn handle_post_stop(status: &str, current_version: &str) -> PostStopOutcome {
    match state_dir() {
        Some(dir) => handle_post_stop_at(dir.as_path(), status, current_version),
        None => PostStopOutcome::Proceed,
    }
}

pub(crate) fn handle_post_stop_at(
    dir: &Path,
    status: &str,
    current_version: &str,
) -> PostStopOutcome {
    // A clean or voluntary-update stop is never a crash: run the normal flow
    // (which, for a real update-needed 42, performs the forward update).
    if classify_stop(status) == StopClass::NotCrash {
        return PostStopOutcome::Proceed;
    }

    let Some(mut state) = read_probation_at(dir) else {
        // Crash of a version that is NOT on probation (e.g. a long-running,
        // committed version hit a transient fault): let the caller run the
        // ordinary update flow (#4549 self-heal), don't roll back.
        return PostStopOutcome::Proceed;
    };

    if state.new_version != current_version {
        // The marker describes a different version than the one that just
        // stopped — stale (e.g. an external install changed the binary). Drop
        // it and fall through to the normal flow rather than acting on it.
        remove_probation_at(dir);
        return PostStopOutcome::Proceed;
    }

    // GC discipline (S5): a marker older than the max age means the version has
    // been around far longer than any boot-wedge would survive. Treat as
    // committed/stale so a much-later transient crash can't trigger a rollback.
    if now_unix().saturating_sub(state.installed_at_unix) > PROBATION_MAX_AGE_SECS {
        remove_probation_at(dir);
        return PostStopOutcome::Proceed;
    }

    // In probation and the probationary version crashed without committing →
    // count it. Persist FIRST (local, no network) so the count survives even if
    // everything below is skipped.
    state.crash_count = state.crash_count.saturating_add(1);
    let persisted = write_probation_at(dir, &state).is_ok();

    if !should_rollback(state.crash_count) {
        // Below threshold we rely on the persisted count to accumulate across
        // restarts. If the write failed (full / unwritable state dir) the count
        // is lost and we would re-read the old value forever, never reaching the
        // threshold before the supervisor gives up. Surface that as
        // RollbackUnavailable instead of silently under-counting.
        if !persisted {
            return PostStopOutcome::RollbackUnavailable {
                reason: "cannot persist probation crash count (state dir full/unwritable); \
                         crashes cannot accumulate toward rollback"
                    .to_string(),
            };
        }
        return PostStopOutcome::CrashRecorded {
            crash_count: state.crash_count,
        };
    }
    // At/above threshold we proceed to roll back using the in-memory count even
    // if the persist failed — the marker is removed on a successful rollback
    // anyway, and a genuinely broken state dir will surface at the pin step (S6).

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

    // S3: verify the retained blob's integrity BEFORE touching the live binary.
    // A truncated/corrupt snapshot must never be restored over a (bad but
    // running) binary — that would brick with no recovery.
    match sha256_file(&state.rollback_binary) {
        Ok((size, hash)) if size == state.rollback_size && hash == state.rollback_sha256 => {}
        Ok((size, _hash)) => {
            // Definitively corrupt: this rollback target is unusable. Drop the
            // marker so we stop re-attempting it; the next crash falls through
            // to the normal update flow (which may self-heal forward).
            remove_probation_at(dir);
            return PostStopOutcome::RollbackUnavailable {
                reason: format!(
                    "known-good integrity check failed (size {size} vs expected {}, or SHA-256 \
                     mismatch); leaving the running binary in place",
                    state.rollback_size
                ),
            };
        }
        Err(e) => {
            // Transient read error: leave the marker so a later crash retries
            // (bounded by the supervisor's own crash limiter).
            return PostStopOutcome::RollbackUnavailable {
                reason: format!("cannot read known-good binary to verify: {e:#}"),
            };
        }
    }

    // S6: persist the known-bad pin BEFORE restoring. If it can't be persisted
    // (read-only / full state dir), DON'T restore — restoring without a durable
    // pin would let the good version re-fetch and re-install the bad version,
    // oscillating. Leave the marker so a later crash retries.
    if let Err(e) = pin_known_bad_at(dir, &state.new_version) {
        return PostStopOutcome::RollbackUnavailable {
            reason: format!("cannot persist known-bad pin (not restoring to avoid a loop): {e:#}"),
        };
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
            // Leave the marker so a subsequent crash retries the restore
            // (bounded by the supervisor's own crash limiter). The pin is
            // already persisted, so we won't re-apply the bad version meanwhile.
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

/// Restore `src` (the integrity-verified known-good binary) over `target` (the
/// live binary), without ever deleting `src`.
///
/// On Unix `rename(2)` atomically replaces the target, and a running executable
/// keeps its open inode, so we copy → fsync → rename directly with no window
/// where `target` is absent (S4). On Windows a running `.exe` cannot be
/// replaced by renaming over it but CAN be renamed away, so we move the current
/// binary aside first and restore it on failure so we never end up with no
/// binary at `target`.
fn restore_binary(src: &Path, target: &Path) -> Result<()> {
    let parent = target
        .parent()
        .context("target binary has no parent directory")?;
    let stem = target.file_name().unwrap_or_default().to_string_lossy();
    let temp = parent.join(format!(".{stem}.rollback.tmp"));

    // Copy the known-good binary to a same-directory temp and fsync it.
    {
        use std::io::Write;
        let mut s = std::fs::File::open(src).context("open known-good binary")?;
        let mut t = std::fs::File::create(&temp).context("create rollback temp")?;
        std::io::copy(&mut s, &mut t).context("copy known-good binary into place")?;
        t.flush().ok();
        t.sync_all().context("fsync rollback temp")?;
    }
    set_executable(&temp);

    #[cfg(not(windows))]
    {
        // Atomic replace; the running process keeps its open inode.
        if let Err(e) = std::fs::rename(&temp, target) {
            let _rm = std::fs::remove_file(&temp);
            return Err(e).context("atomically install rolled-back binary");
        }
        fsync_dir(parent);
        Ok(())
    }

    #[cfg(windows)]
    {
        let displaced = parent.join(format!(".{stem}.badver"));
        if displaced.exists() {
            let _rm = std::fs::remove_file(&displaced);
        }
        if target.exists() {
            std::fs::rename(target, &displaced).context("move current binary aside")?;
        }
        if let Err(e) = std::fs::rename(&temp, target) {
            // Put the bad binary back so the node still has *a* binary to run.
            if std::fs::rename(&displaced, target).is_err() {
                tracing::error!(
                    target = %target.display(),
                    displaced = %displaced.display(),
                    "CRITICAL: rollback failed AND could not restore the displaced binary; \
                     the previous binary remains at the displaced path for manual recovery"
                );
            }
            let _rm = std::fs::remove_file(&temp);
            return Err(e).context("install rolled-back binary");
        }
        // Best-effort removal of the displaced bad binary (not running anymore).
        let _rm = std::fs::remove_file(&displaced);
        Ok(())
    }
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

    fn known_good_path(dir: &Path) -> PathBuf {
        dir.join(KNOWN_GOOD_BINARY_FILE)
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
        let meta = capture_known_good_at(dir, &live).unwrap();
        // Now the live binary is "replaced" by the new (bad) version.
        write_dummy_binary(&live, b"BAD-BINARY");
        begin_probation_at(dir, new_version, prev_version, &live, &meta).unwrap();
        live
    }

    #[test]
    fn classify_stop_crash_vs_not() {
        // Clean / voluntary-update / already-running: never a crash.
        for s in ["0", "2", "42", "43", "44", " 42 ", "0\n"] {
            assert_eq!(classify_stop(s), StopClass::NotCrash, "status {s:?}");
        }
        // Fast crash, panics, signals (numeric and name), early errors, unknown.
        for s in [
            "45", "1", "101", "134", "137", "139", "SEGV", "ABRT", "KILL", "garbage",
        ] {
            assert_eq!(classify_stop(s), StopClass::Crash, "status {s:?}");
        }
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
        // Integrity metadata recorded for the known-good blob.
        assert_eq!(state.rollback_size, "GOOD-BINARY".len() as u64);
        assert_eq!(state.rollback_sha256.len(), 64);

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

        // First crashes below threshold are recorded, no rollback. Use a panic
        // exit code (101) to confirm the broadened crash detection (M1).
        for expected in 1..ROLLBACK_CRASH_THRESHOLD {
            let PostStopOutcome::CrashRecorded { crash_count } =
                handle_post_stop_at(dir, "101", "0.2.84")
            else {
                panic!("expected CrashRecorded({expected})");
            };
            assert_eq!(crash_count, expected);
            assert_eq!(std::fs::read(&live).unwrap(), b"BAD-BINARY");
            assert!(read_known_bad_at(dir).is_none());
            assert!(read_probation_at(dir).is_some());
        }

        // Threshold crash (a SIGSEGV, surfaced as a signal name) → rollback.
        let PostStopOutcome::RolledBack {
            restored_version,
            bad_version,
        } = handle_post_stop_at(dir, "SEGV", "0.2.84")
        else {
            panic!("expected RolledBack");
        };
        assert_eq!(restored_version, "0.2.83");
        assert_eq!(bad_version, "0.2.84");

        // Live binary is the known-good content again.
        assert_eq!(std::fs::read(&live).unwrap(), b"GOOD-BINARY");
        // Bad version pinned; probation cleared; known-good blob preserved.
        assert!(is_version_pinned_bad_at(dir, "0.2.84"));
        assert!(read_probation_at(dir).is_none());
        assert!(known_good_path(dir).exists());
    }

    #[test]
    fn voluntary_update_exit_42_during_probation_is_not_a_crash() {
        // M2 regression: a healthy node stepping forward to a newer release with
        // exit 42 inside its probation window must NOT be counted as a crash
        // (which would cause a spurious backward rollback during a release
        // cascade). It must Proceed and leave crash_count untouched.
        let tmp = tempfile::tempdir().unwrap();
        let dir = tmp.path();
        setup_probation(dir, "0.2.84", "0.2.83");

        for _ in 0..ROLLBACK_CRASH_THRESHOLD + 2 {
            assert_eq!(
                handle_post_stop_at(dir, "42", "0.2.84"),
                PostStopOutcome::Proceed
            );
            let state = read_probation_at(dir).expect("marker preserved");
            assert_eq!(
                state.crash_count, 0,
                "exit 42 must not increment crash_count"
            );
        }
    }

    #[test]
    fn clean_exit_codes_during_probation_are_not_crashes() {
        let tmp = tempfile::tempdir().unwrap();
        let dir = tmp.path();
        setup_probation(dir, "0.2.84", "0.2.83");
        for s in ["0", "43"] {
            assert_eq!(
                handle_post_stop_at(dir, s, "0.2.84"),
                PostStopOutcome::Proceed
            );
            assert_eq!(read_probation_at(dir).unwrap().crash_count, 0);
        }
    }

    #[test]
    fn pinned_version_is_not_reapplied_but_newer_is_allowed() {
        let tmp = tempfile::tempdir().unwrap();
        let dir = tmp.path();
        pin_known_bad_at(dir, "0.2.84").unwrap();

        assert!(is_version_pinned_bad_at(dir, "0.2.84"));
        assert!(!is_version_pinned_bad_at(dir, "0.2.85"));
        assert!(!is_version_pinned_bad_at(dir, "0.2.83"));

        // A successful forward install supersedes the pin.
        let live = dir.join("freenet");
        write_dummy_binary(&live, b"NEWER");
        let meta = KnownGoodMeta {
            size: 5,
            sha256: "x".repeat(64),
        };
        begin_probation_at(dir, "0.2.85", "0.2.83", &live, &meta).unwrap();
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
            handle_post_stop_at(dir, "101", "0.2.84");
        }
        assert!(read_probation_at(dir).is_none());
        // Now running the rolled-back version; another crash is a no-op Proceed.
        assert_eq!(
            handle_post_stop_at(dir, "101", "0.2.83"),
            PostStopOutcome::Proceed
        );
    }

    #[test]
    fn no_probation_means_proceed() {
        let tmp = tempfile::tempdir().unwrap();
        assert_eq!(
            handle_post_stop_at(tmp.path(), "101", "0.2.84"),
            PostStopOutcome::Proceed
        );
    }

    #[test]
    fn stale_marker_for_other_version_proceeds_and_is_cleared() {
        let tmp = tempfile::tempdir().unwrap();
        let dir = tmp.path();
        setup_probation(dir, "0.2.84", "0.2.83");
        // A DIFFERENT version crashed than the marker describes.
        assert_eq!(
            handle_post_stop_at(dir, "101", "0.2.99"),
            PostStopOutcome::Proceed
        );
        assert!(read_probation_at(dir).is_none());
    }

    #[test]
    fn aged_out_marker_is_treated_as_committed() {
        // S5: a probation marker older than PROBATION_MAX_AGE_SECS is stale; a
        // crash then must NOT roll back a long-healthy version.
        let tmp = tempfile::tempdir().unwrap();
        let dir = tmp.path();
        let live = setup_probation(dir, "0.2.84", "0.2.83");

        // Backdate installed_at beyond the TTL.
        let mut state = read_probation_at(dir).unwrap();
        state.installed_at_unix = now_unix().saturating_sub(PROBATION_MAX_AGE_SECS + 60);
        write_probation_at(dir, &state).unwrap();

        assert_eq!(
            handle_post_stop_at(dir, "101", "0.2.84"),
            PostStopOutcome::Proceed
        );
        assert!(read_probation_at(dir).is_none(), "stale marker cleared");
        assert_eq!(std::fs::read(&live).unwrap(), b"BAD-BINARY", "no rollback");
    }

    #[test]
    fn corrupt_known_good_blob_does_not_brick() {
        // S3: if the retained known-good blob is truncated/corrupt, the
        // integrity check must fail, leave the running binary untouched, and
        // NOT pin (we never restored).
        let tmp = tempfile::tempdir().unwrap();
        let dir = tmp.path();
        let live = setup_probation(dir, "0.2.84", "0.2.83");

        // Corrupt the snapshot (truncate to different content/size).
        std::fs::write(known_good_path(dir), b"TRUNC").unwrap();

        for _ in 0..ROLLBACK_CRASH_THRESHOLD - 1 {
            handle_post_stop_at(dir, "101", "0.2.84");
        }
        let PostStopOutcome::RollbackUnavailable { reason } =
            handle_post_stop_at(dir, "101", "0.2.84")
        else {
            panic!("expected RollbackUnavailable");
        };
        assert!(reason.contains("integrity"), "reason: {reason}");
        // Running binary untouched (still the bad version), not pinned.
        assert_eq!(std::fs::read(&live).unwrap(), b"BAD-BINARY");
        assert!(!is_version_pinned_bad_at(dir, "0.2.84"));
        // Marker dropped so we stop re-attempting the unusable target.
        assert!(read_probation_at(dir).is_none());
    }

    #[test]
    fn pin_write_failure_aborts_rollback_without_restoring() {
        // S6: if the known-bad pin can't be persisted, do NOT restore — that
        // would let the good version re-install the bad one and oscillate.
        let tmp = tempfile::tempdir().unwrap();
        let dir = tmp.path();
        let live = setup_probation(dir, "0.2.84", "0.2.83");

        // Make the pin path un-writable by occupying it with a directory, so
        // the final rename in atomic_write fails.
        std::fs::create_dir(dir.join(KNOWN_BAD_FILE)).unwrap();

        for _ in 0..ROLLBACK_CRASH_THRESHOLD - 1 {
            handle_post_stop_at(dir, "101", "0.2.84");
        }
        let PostStopOutcome::RollbackUnavailable { reason } =
            handle_post_stop_at(dir, "101", "0.2.84")
        else {
            panic!("expected RollbackUnavailable");
        };
        assert!(reason.contains("pin"), "reason: {reason}");
        // Did NOT restore (still the bad binary), marker left for retry.
        assert_eq!(std::fs::read(&live).unwrap(), b"BAD-BINARY");
        assert!(read_probation_at(dir).is_some());
    }

    #[test]
    fn rollback_unavailable_when_known_good_missing() {
        let tmp = tempfile::tempdir().unwrap();
        let dir = tmp.path();
        let live = setup_probation(dir, "0.2.84", "0.2.83");
        std::fs::remove_file(known_good_path(dir)).unwrap();

        for _ in 0..ROLLBACK_CRASH_THRESHOLD - 1 {
            handle_post_stop_at(dir, "101", "0.2.84");
        }
        assert!(matches!(
            handle_post_stop_at(dir, "101", "0.2.84"),
            PostStopOutcome::RollbackUnavailable { .. }
        ));
        assert_eq!(std::fs::read(&live).unwrap(), b"BAD-BINARY");
        assert!(read_probation_at(dir).is_none());
    }

    #[test]
    fn prepare_known_good_preserves_blob_and_label_for_genuine_chained_install() {
        // Genuine chained install (existing marker is for the running version,
        // blob present): keep the existing blob and carry its label forward.
        let tmp = tempfile::tempdir().unwrap();
        let dir = tmp.path();
        setup_probation(dir, "0.2.84", "0.2.83"); // blob = the 0.2.83 (GOOD) binary
        let blob_before = std::fs::read(known_good_path(dir)).unwrap();
        let existing = read_probation_at(dir).unwrap();
        // The running binary is the 0.2.84 (BAD) content from setup_probation.
        let live = dir.join("bin").join("freenet");

        let (meta, previous) = prepare_known_good_for_install_at(dir, "0.2.84", &live).unwrap();

        // Carried forward: previous == 0.2.83, blob unchanged, meta matches marker.
        assert_eq!(previous, "0.2.83");
        assert_eq!(std::fs::read(known_good_path(dir)).unwrap(), blob_before);
        assert_eq!(meta.size, existing.rollback_size);
        assert_eq!(meta.sha256, existing.rollback_sha256);
    }

    #[test]
    fn prepare_known_good_recaptures_when_blob_externally_deleted() {
        // #2 regression: if the known-good blob is externally deleted while a
        // marker persists, a chained install must re-capture the CURRENT binary
        // and label it the CURRENT version — never carry a stale label forward
        // onto a freshly-captured blob (which would restore the wrong binary and
        // report the wrong version on a later rollback).
        let tmp = tempfile::tempdir().unwrap();
        let dir = tmp.path();
        let live = setup_probation(dir, "0.2.84", "0.2.83");
        // External deletion of the blob while the marker (new=0.2.84) remains.
        std::fs::remove_file(known_good_path(dir)).unwrap();

        let (meta, previous) = prepare_known_good_for_install_at(dir, "0.2.84", &live).unwrap();

        // Re-captured fresh: label is the current version, blob == current binary,
        // and the recorded meta matches the (re-captured) blob — they agree.
        assert_eq!(previous, "0.2.84");
        assert!(known_good_path(dir).exists());
        assert_eq!(
            std::fs::read(known_good_path(dir)).unwrap(),
            std::fs::read(&live).unwrap()
        );
        let (size, hash) = sha256_file(&known_good_path(dir)).unwrap();
        assert_eq!(meta.size, size);
        assert_eq!(meta.sha256, hash);
    }

    #[test]
    fn prepare_known_good_recaptures_for_stale_marker_other_version() {
        // A leftover marker for a DIFFERENT version than the one being replaced
        // must be ignored: capture fresh + label the current version.
        let tmp = tempfile::tempdir().unwrap();
        let dir = tmp.path();
        setup_probation(dir, "0.2.84", "0.2.83"); // stale marker (new=0.2.84)
        let live = dir.join("freenet2");
        write_dummy_binary(&live, b"V88-CONTENT");

        // Replacing version 0.2.88 (NOT the stale marker's 0.2.84).
        let (meta, previous) = prepare_known_good_for_install_at(dir, "0.2.88", &live).unwrap();

        assert_eq!(previous, "0.2.88");
        assert_eq!(std::fs::read(known_good_path(dir)).unwrap(), b"V88-CONTENT");
        let (size, hash) = sha256_file(&known_good_path(dir)).unwrap();
        assert_eq!(meta.size, size);
        assert_eq!(meta.sha256, hash);
    }

    #[cfg(unix)]
    #[test]
    fn crash_count_write_failure_is_rollback_unavailable() {
        // If the probation crash count can't be persisted (full/unwritable state
        // dir), we must surface RollbackUnavailable rather than silently
        // under-counting forever (a lost increment would never reach threshold).
        use std::os::unix::fs::PermissionsExt;
        let tmp = tempfile::tempdir().unwrap();
        let dir = tmp.path();
        setup_probation(dir, "0.2.84", "0.2.83");

        // Make the state dir read-only so the marker write (temp + rename) fails,
        // while the existing marker + blob remain readable.
        let orig = std::fs::metadata(dir).unwrap().permissions();
        std::fs::set_permissions(dir, std::fs::Permissions::from_mode(0o555)).unwrap();

        let outcome = handle_post_stop_at(dir, "101", "0.2.84");

        // Restore perms before any assertion so tempdir cleanup always works.
        std::fs::set_permissions(dir, orig).unwrap();

        let PostStopOutcome::RollbackUnavailable { reason } = outcome else {
            panic!("expected RollbackUnavailable, got {outcome:?}");
        };
        assert!(reason.contains("persist"), "reason: {reason}");
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
        assert_eq!(
            handle_post_stop_at(dir, "101", "0.2.84"),
            PostStopOutcome::Proceed
        );
    }

    #[test]
    fn capture_known_good_records_real_hash() {
        let tmp = tempfile::tempdir().unwrap();
        let dir = tmp.path();
        let bin = dir.join("freenet");
        write_dummy_binary(&bin, b"hello world");
        let meta = capture_known_good_at(dir, &bin).unwrap();
        assert_eq!(meta.size, 11);
        // SHA-256 of "hello world".
        assert_eq!(
            meta.sha256,
            "b94d27b9934d3e08a52e52d7da7dabfac484efe37a5380ee9088f7ace2efcde9"
        );
        // The blob matches and verifies against the recorded meta.
        let (size, hash) = sha256_file(&known_good_path(dir)).unwrap();
        assert_eq!(size, meta.size);
        assert_eq!(hash, meta.sha256);
    }
}
