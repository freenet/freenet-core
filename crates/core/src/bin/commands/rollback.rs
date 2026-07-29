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
//!    ([`commit_probation`]) and reclaims the known-good snapshot
//!    ([`discard_known_good_at`]). After that, ordinary later crashes never
//!    trigger a rollback.
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
//! ## The known-good blob lives exactly as long as the marker
//!
//! The retained snapshot is an UNCOMPRESSED copy of the executable (~58 MB
//! measured on a real peer). No AUTOMATED reader can reach it without a
//! probation marker: [`handle_post_stop_at`] returns
//! [`PostStopOutcome::Proceed`] before looking at it when [`read_probation_at`]
//! is `None`, and [`prepare_known_good_for_install_at`] re-captures fresh from
//! the live binary rather than reading it in that case. Keeping it past the
//! commit therefore bought this module nothing — it was dead weight for the
//! days-to-weeks until the next update — so the commit reclaims it.
//!
//! The tradeoff this accepts, in two parts:
//!
//! * **Automated.** Once probation has committed, a node that degrades LATER
//!   has no local binary to revert to and recovers by re-installing from
//!   GitHub. That is the same position as a node whose known-good capture
//!   failed (already a supported, non-fatal state — see `commands::update`),
//!   and the crash-loop class this module exists for (a bad release that wedges
//!   at boot) is unaffected, because it fires inside the probation window while
//!   the blob is still present.
//! * **Manual.** An OPERATOR with shell access could always `cp` the blob back
//!   by hand, so it doubled as an offline recovery artifact for the post-commit
//!   degradation class — a use no code path expresses and this module never
//!   promised, but a real one. Reclaiming the blob removes it. Note also that
//!   the commit trigger is a bare 60 s sleep (`bin/freenet.rs`) with no health
//!   check beyond "the process is still alive", so "committed" means less than
//!   it sounds like. Whether that warrants keeping an offline copy (or a
//!   stronger commit condition) is an open question for the maintainer, NOT
//!   something the code below tries to answer.
//!
//! Not compression: adding a decode step to the one path whose failure bricks a
//! node would weaken the integrity guarantee (the recorded SHA would cover the
//! stored bytes, not the installed ones) for a saving that deleting the file
//! beats outright.
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
///
/// Captured by [`capture_known_good_at`] just before an install and reclaimed by
/// [`discard_known_good_at`] when the probation marker that made it reachable is
/// retired — its lifetime is exactly the marker's.
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
        // Only preserve the existing rollback target for a GENUINE chained
        // in-probation install: the marker is for the version we're replacing,
        // the blob is present, AND the marker hasn't aged out. The age check
        // mirrors handle_post_stop's TTL — a marker that survived past
        // PROBATION_MAX_AGE_SECS means the current version has actually been
        // running fine for a long time (commit just failed to clear the marker),
        // so the CURRENT binary is the real known-good. Without this, the next
        // update would carry forward the stale older blob/label and a later
        // rollback would skip over the actual immediately-previous good version.
        let fresh_enough =
            now_unix().saturating_sub(existing.installed_at_unix) <= PROBATION_MAX_AGE_SECS;
        if existing.new_version == current_version
            && fresh_enough
            && dir.join(KNOWN_GOOD_BINARY_FILE).exists()
        {
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

// ── Known-good reclamation ─────────────────────────────────────────────────

/// Reclaim the retained known-good snapshot once the marker that made it
/// reachable has been removed (see the module docs: the blob's lifetime is
/// exactly the marker's, and it is ~58 MB).
///
/// `expected` is the integrity metadata recorded in the marker being retired.
///
/// This is brick-recovery state, so it only ever unlinks a file it can
/// positively identify as the now-dead rollback target:
///
/// * It hashes the candidate and deletes it only when size AND SHA-256 match
///   `expected`, so a snapshot a concurrent install captured for a different
///   generation is left alone.
/// * It re-checks for a probation marker in the SAME statement as the unlink,
///   so the marker is the last thing read before the file goes away.
/// * It only ever considers `dir/known_good_binary`, never the
///   `rollback_binary` path carried in the marker JSON, so a corrupt or
///   tampered marker cannot make us unlink an arbitrary file.
///
/// # What the marker re-check does and does not cover
///
/// It covers SEQUENTIAL misuse completely: a caller that reclaims before
/// removing the marker gets a no-op rather than an armed marker pointing at a
/// deleted binary, which is why `commit_probation_at` can rely on it instead of
/// on a comment.
///
/// It does NOT make this safe against a truly CONCURRENT `freenet update`. Both
/// sides are check-then-act on the same state directory with no lock, so an
/// installer that writes its marker in the window between the re-check and the
/// `remove_file` is still not seen. The window is now one syscall wide rather
/// than a ~58 MB hash wide, and `begin_probation_at` closes the same race from
/// the other end by disarming when the blob it just recorded has vanished — but
/// a genuine sub-syscall interleaving remains open, and only a lock on the state
/// directory would close it. Do not read this guard as "the concurrent installer
/// case is handled".
///
/// Best-effort by construction: it returns `()`, and every failure is logged
/// and swallowed. Reclaiming disk must never fail a probation commit or an
/// update — a blob we fail to delete is simply overwritten by the next
/// install's capture, which is exactly the pre-existing steady state.
fn discard_known_good_at(dir: &Path, expected: &KnownGoodMeta) {
    if read_probation_at(dir).is_some() {
        // Still armed (or re-armed): the blob may be a live rollback target.
        // Cheap early-out before the hash; re-checked below, because a hash of
        // a ~58 MB file is a very wide window to have decided anything in.
        return;
    }
    let blob = dir.join(KNOWN_GOOD_BINARY_FILE);
    if !blob.exists() {
        return;
    }
    match sha256_file(&blob) {
        Ok((size, sha256)) if size == expected.size && sha256 == expected.sha256 => {
            // Re-read the marker AFTER the hash, immediately before the unlink.
            // The hash above is a cold ~58 MB read (seconds on slow storage);
            // a marker armed during it must not be missed, so the decision to
            // delete is made against the freshest possible read.
            if read_probation_at(dir).is_some() {
                tracing::debug!(
                    path = %blob.display(),
                    "A probation marker was armed while verifying the retained rollback \
                     binary; leaving it in place."
                );
                return;
            }
            match std::fs::remove_file(&blob) {
                Ok(()) => {
                    fsync_dir(dir);
                    tracing::info!(
                        bytes = size,
                        path = %blob.display(),
                        "Reclaimed the retained known-good rollback binary (probation is over, \
                         nothing can read it)."
                    );
                }
                Err(e) => tracing::warn!(
                    error = %e,
                    path = %blob.display(),
                    "Could not reclaim the retained known-good rollback binary; it is dead \
                     weight until the next update's capture overwrites it."
                ),
            }
        }
        Ok((size, _sha256)) => tracing::debug!(
            size,
            expected_size = expected.size,
            path = %blob.display(),
            "Retained binary is not the rollback target being retired (a concurrent install \
             most likely re-captured it); leaving it in place."
        ),
        Err(e) => tracing::warn!(
            error = %e,
            path = %blob.display(),
            "Could not verify the retained known-good binary before reclaiming it; leaving it \
             in place."
        ),
    }
}

/// Begin (or refresh) probation for `new_version`, just installed over the live
/// binary at `target_binary`. `previous_version` and `meta` are the label and
/// integrity metadata of the rollback target, computed together by
/// [`prepare_known_good_for_install`] so they cannot diverge. Clears any
/// known-bad pin, since a successful forward install means we have moved on.
///
/// Returns `Err` if the probation marker could not be persisted (full /
/// unwritable state dir), or if the known-good blob it names disappeared while
/// the marker was being written (see [`discard_known_good_at`] — a concurrent
/// probation commit can reclaim it). In both cases the update still succeeded
/// but the new version has NO crash-loop rollback protection, so the caller
/// MUST surface the error rather than discard it.
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
    let blob = state.rollback_binary.clone();
    write_probation_at(dir, &state)
        .context("failed to write crash-loop probation marker; the installed version has no rollback protection")?;

    // Verify the rollback target still exists AFTER arming, and disarm if it
    // does not. A concurrently-running node's probation commit can reclaim the
    // blob (`discard_known_good_at`) in the window between the caller's
    // `prepare_known_good_for_install` and this write — reachable in practice
    // because the tray's "Check for Updates" runs a real install while the
    // `freenet network` child is still up (`service/wrapper.rs`). Checking
    // after the write, not before, is what makes this useful: it catches a
    // reclaim that raced the write itself.
    //
    // An armed marker naming a binary that is not there is strictly worse than
    // no marker: it burns the crash budget to `RollbackUnavailable` instead of
    // restoring, and it suppresses the ordinary update flow (`handle_post_stop`
    // returns `Proceed` with no marker, which is the #4549 forward self-heal).
    // So we would rather ship this cycle with NO rollback protection — the same
    // state as a failed capture, which the caller already surfaces — than with
    // a marker that lies.
    if !blob.exists() {
        remove_probation_at(dir);
        anyhow::bail!(
            "the known-good rollback binary at {} disappeared while arming probation \
             (most likely reclaimed by a concurrent probation commit); the installed \
             version has no rollback protection",
            blob.display()
        );
    }
    Ok(())
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
///
/// # Blocking
///
/// This performs blocking file I/O, including a cold read + SHA-256 of the
/// ~58 MB known-good blob when there is a marker to retire
/// ([`discard_known_good_at`]). Callers on a tokio runtime MUST run it via
/// `spawn_blocking` — `bin/freenet.rs`'s commit task does, and
/// `commit_probation_runs_on_a_blocking_thread` pins that.
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
            // Ordering is load-bearing and fail-safe in this direction only:
            // drop the marker FIRST, then reclaim the blob it pointed at. A
            // crash in between leaves an orphaned blob that the next install's
            // capture overwrites (today's steady state, harmless); the reverse
            // order would leave an ARMED marker advertising a rollback target
            // that no longer exists. `discard_known_good_at`'s marker re-check
            // enforces that ordering for THIS process — it does not make the
            // reclaim safe against a concurrent `freenet update`; see its docs.
            remove_probation_at(dir);
            discard_known_good_at(dir, &retired_meta(&state));
            CommitOutcome::Committed
        }
        Some(state) => {
            // Running a different version healthily than the marker describes:
            // the marker can no longer protect anything, so drop it (and the
            // blob it was the only reader of). Same ordering rationale as above.
            remove_probation_at(dir);
            discard_known_good_at(dir, &retired_meta(&state));
            CommitOutcome::ClearedStale {
                marker_version: state.new_version,
            }
        }
        None => {
            // No marker means no blob we can positively identify as dead: an
            // orphan (e.g. left by a post-stop path that removed the marker
            // itself) is left for the next install's capture to overwrite
            // rather than deleted on a guess.
            CommitOutcome::Nothing
        }
    }
}

/// Integrity metadata of the rollback blob described by a marker we are
/// retiring, used to identify the blob before reclaiming it.
fn retired_meta(state: &ProbationState) -> KnownGoodMeta {
    KnownGoodMeta {
        size: state.rollback_size,
        sha256: state.rollback_sha256.clone(),
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

// ── Per-target-version install-failure gate ────────────────────────────────
//
// Distinct from the crash-loop known-bad pin above: the pin fires when an
// already-INSTALLED version crash-loops, whereas this gate fires when a version
// repeatedly FAILS TO INSTALL (checksum / signature / download / extract). The
// #4586 fail-closed checksum gate turned a bad-manifest install from a
// self-terminating failure into a NON-counting one (it is classified
// `OtherFailure` => `NoChange`, so it never trips the #3934 lockout), which left
// the node free to loop: detect newer X → exit 42 → `freenet update` → install
// fails the gate → no install → re-detect → exit 42 → … forever.
//
// This gate breaks that loop. Each failed install of a target version increments
// a persisted per-version counter; once the SAME version has failed
// [`INSTALL_FAILURE_GATE_THRESHOLD`] times it is gated, and both the node's
// update detection and the installer stop acting on it until a STRICTLY-NEWER
// version appears. Like the pin, the gate is keyed by exact version string, so a
// newer release (a fix) never matches and installs normally.
//
// Degrade-safe (NOT fail-closed like the rate-limit bucket): a missing or
// corrupt gate file reads as "not gated". Treating a corrupt file as "gate
// everything" could brick auto-update entirely (we would not know which version
// to exempt), so the conservative choice here is the opposite of the bucket's —
// the GitHub-spam dimension is already bounded by the rate-limit bucket, and
// atomic tmp+rename writes make corruption unlikely in the first place.

/// Consecutive failed installs of the SAME target version before that version is
/// gated out of the node's update detection and the installer. Mirrors the spirit
/// of [`ROLLBACK_CRASH_THRESHOLD`] (three confirmations: one is noise, two could
/// be unlucky, by the third the version is demonstrably not installable here).
pub(crate) const INSTALL_FAILURE_GATE_THRESHOLD: u32 = 3;

/// Per-target-version install-failure record (JSON: version + consecutive count).
const INSTALL_FAILURES_FILE: &str = "install_failures.json";

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct InstallFailureState {
    /// The target version whose install has been failing.
    pub version: String,
    /// Consecutive failed installs of that version.
    pub count: u32,
}

pub(crate) fn read_install_failures_at(dir: &Path) -> Option<InstallFailureState> {
    let raw = std::fs::read_to_string(dir.join(INSTALL_FAILURES_FILE)).ok()?;
    serde_json::from_str(&raw).ok()
}

fn write_install_failures_at(dir: &Path, state: &InstallFailureState) -> Result<()> {
    let raw = serde_json::to_vec(state).context("serialize install-failure state")?;
    atomic_write(&dir.join(INSTALL_FAILURES_FILE), &raw)
}

fn clear_install_failures_at(dir: &Path) {
    let _rm = std::fs::remove_file(dir.join(INSTALL_FAILURES_FILE));
}

/// Record one failed install of `version`. If the record is for a different
/// version (e.g. a newer release became the target), it RESETS to track the new
/// version with a count of 1 — so an old gated version never blocks a new one,
/// and a transient failure of a new version starts its own count.
pub(crate) fn record_install_failure_at(dir: &Path, version: &str) {
    let next = match read_install_failures_at(dir) {
        Some(prev) if prev.version == version => InstallFailureState {
            version: version.to_string(),
            count: prev.count.saturating_add(1),
        },
        _ => InstallFailureState {
            version: version.to_string(),
            count: 1,
        },
    };
    if let Err(e) = write_install_failures_at(dir, &next) {
        // Best-effort: if we cannot persist the counter the gate simply will not
        // engage for this version, and the rate-limit bucket still bounds the
        // GitHub load. Surface it for diagnosis rather than failing the update.
        tracing::warn!(
            version,
            error = %e,
            "Failed to persist per-version install-failure counter (#4073)"
        );
    }
}

/// Record one failed install of `version` against the shared state directory.
pub fn record_install_failure(version: &str) {
    if let Some(dir) = state_dir() {
        record_install_failure_at(&dir, version);
    }
}

/// Clear the install-failure counter (called after a successful install / when
/// the node is confirmed already up to date — we have moved forward).
pub fn clear_install_failures() {
    if let Some(dir) = state_dir() {
        clear_install_failures_at(&dir);
    }
}

/// Whether `version` is currently gated by repeated install failures: the stored
/// record is for this exact version AND has reached the threshold. A
/// strictly-newer version never matches (different string), so a fix is never
/// blocked. Degrade-safe: missing/corrupt record => not gated.
pub(crate) fn is_version_install_gated_at(dir: &Path, version: &str) -> bool {
    match read_install_failures_at(dir) {
        Some(state) => state.version == version && state.count >= INSTALL_FAILURE_GATE_THRESHOLD,
        None => false,
    }
}

/// Whether `version` is install-gated against the shared state directory.
pub fn is_version_install_gated(version: &str) -> bool {
    state_dir()
        .map(|d| is_version_install_gated_at(d.as_path(), version))
        .unwrap_or(false)
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
        // The stale marker was the blob's only reader, so it is reclaimed too.
        assert!(!known_good_path(dir).exists());
    }

    #[test]
    fn commit_reclaims_the_known_good_blob() {
        // The ~58 MB snapshot is reachable ONLY through the probation marker,
        // so committing (which removes the marker) must reclaim it instead of
        // carrying it for the days-to-weeks until the next update.
        let tmp = tempfile::tempdir().unwrap();
        let dir = tmp.path();
        setup_probation(dir, "0.2.84", "0.2.83");
        assert!(known_good_path(dir).exists(), "precondition: blob captured");

        assert_eq!(commit_probation_at(dir, "0.2.84"), CommitOutcome::Committed);

        assert!(read_probation_at(dir).is_none());
        assert!(
            !known_good_path(dir).exists(),
            "known-good blob must be reclaimed once probation commits"
        );
    }

    #[test]
    fn commit_with_no_marker_leaves_an_unidentifiable_blob() {
        // Without a marker we have no recorded size/SHA to identify the blob
        // against, so `Nothing` must not delete on a guess. (Orphans are
        // overwritten by the next install's capture.)
        let tmp = tempfile::tempdir().unwrap();
        let dir = tmp.path();
        let live = dir.join("freenet");
        write_dummy_binary(&live, b"ORPHAN-BLOB");
        capture_known_good_at(dir, &live).unwrap();

        assert_eq!(commit_probation_at(dir, "0.2.84"), CommitOutcome::Nothing);

        assert!(known_good_path(dir).exists());
    }

    #[test]
    fn commit_leaves_a_blob_that_is_not_the_retired_rollback_target() {
        // Race guard: a concurrent `freenet update` can capture a FRESH
        // known-good over the same path between our marker read and our
        // reclaim. That blob belongs to the next probation generation, so its
        // content will not match the retired marker's meta and it must survive
        // — deleting it would leave the next marker pointing at nothing.
        let tmp = tempfile::tempdir().unwrap();
        let dir = tmp.path();
        setup_probation(dir, "0.2.84", "0.2.83");
        // Stand in for the concurrent install's fresh capture.
        std::fs::write(
            known_good_path(dir),
            b"FRESHLY-CAPTURED-BY-CONCURRENT-INSTALL",
        )
        .unwrap();

        assert_eq!(commit_probation_at(dir, "0.2.84"), CommitOutcome::Committed);

        assert_eq!(
            std::fs::read(known_good_path(dir)).unwrap(),
            b"FRESHLY-CAPTURED-BY-CONCURRENT-INSTALL"
        );
    }

    #[test]
    fn commit_leaves_a_same_size_blob_whose_content_differs() {
        // The size check alone is not enough: a concurrent install can capture
        // a DIFFERENT binary that happens to be the same length. Identity has
        // to come from the SHA-256, so this case must survive too. (Without
        // this, mutating the guard down to a size-only comparison stays green.)
        let tmp = tempfile::tempdir().unwrap();
        let dir = tmp.path();
        setup_probation(dir, "0.2.84", "0.2.83");
        let recorded = read_probation_at(dir).unwrap();
        // Same length as b"GOOD-BINARY", one byte different.
        let impostor = b"GOOD-BINARZ";
        assert_eq!(impostor.len() as u64, recorded.rollback_size);
        std::fs::write(known_good_path(dir), impostor).unwrap();

        assert_eq!(commit_probation_at(dir, "0.2.84"), CommitOutcome::Committed);

        assert_eq!(std::fs::read(known_good_path(dir)).unwrap(), impostor);
    }

    #[test]
    fn begin_probation_disarms_when_the_known_good_blob_is_missing() {
        // The concurrent-install race from the other end: if the blob a marker
        // names has been reclaimed by a node's probation commit while we were
        // arming, we must NOT leave an armed marker pointing at nothing — that
        // burns the crash budget to RollbackUnavailable and suppresses the
        // ordinary forward-update self-heal. Disarm and surface instead.
        let tmp = tempfile::tempdir().unwrap();
        let dir = tmp.path();
        let live = dir.join("freenet");
        write_dummy_binary(&live, b"GOOD-BINARY");
        let meta = capture_known_good_at(dir, &live).unwrap();
        // Stand in for a concurrent commit reclaiming the blob.
        std::fs::remove_file(known_good_path(dir)).unwrap();

        let err = begin_probation_at(dir, "0.2.84", "0.2.83", &live, &meta)
            .expect_err("must not arm a marker whose rollback target is gone");

        assert!(
            format!("{err:#}").contains("no rollback protection"),
            "error must say the version is unprotected: {err:#}"
        );
        assert!(
            read_probation_at(dir).is_none(),
            "the lying marker must be removed, not left armed"
        );
        // And with no marker, a later crash falls through to the normal update
        // flow (the #4549 forward self-heal) rather than to RollbackUnavailable.
        assert_eq!(
            handle_post_stop_at(dir, "101", "0.2.84"),
            PostStopOutcome::Proceed
        );
    }

    #[test]
    fn begin_probation_arms_normally_when_the_blob_is_present() {
        // Guard against the check above being over-eager: the ordinary install
        // path (blob captured, then probation armed) must still arm.
        let tmp = tempfile::tempdir().unwrap();
        let dir = tmp.path();
        let live = dir.join("freenet");
        write_dummy_binary(&live, b"GOOD-BINARY");
        let meta = capture_known_good_at(dir, &live).unwrap();

        begin_probation_at(dir, "0.2.84", "0.2.83", &live, &meta).unwrap();

        let state = read_probation_at(dir).expect("armed");
        assert_eq!(state.new_version, "0.2.84");
        assert_eq!(state.previous_version, "0.2.83");
    }

    #[test]
    fn discard_known_good_refuses_while_a_probation_marker_exists() {
        // The fail-safe ordering (remove the marker, THEN reclaim) is enforced
        // by this guard, not merely documented: reclaiming while armed would
        // leave a marker advertising a rollback target that no longer exists.
        let tmp = tempfile::tempdir().unwrap();
        let dir = tmp.path();
        setup_probation(dir, "0.2.84", "0.2.83");
        let armed = read_probation_at(dir).expect("armed");

        discard_known_good_at(dir, &retired_meta(&armed));

        assert!(
            known_good_path(dir).exists(),
            "must never delete the rollback target while probation is armed"
        );
        // And the still-armed marker can still roll back.
        for _ in 0..ROLLBACK_CRASH_THRESHOLD - 1 {
            handle_post_stop_at(dir, "101", "0.2.84");
        }
        assert!(matches!(
            handle_post_stop_at(dir, "101", "0.2.84"),
            PostStopOutcome::RolledBack { .. }
        ));
    }

    /// Source pin: the marker re-check must sit BETWEEN the hash and the
    /// unlink. A behavioural test cannot cover this — the difference is purely
    /// when the read happens relative to a multi-second hash of a cold ~58 MB
    /// file, and staging that would mean a sleep-based race, i.e. a flaky test.
    /// So pin the ordering in the source instead. Needles are split with
    /// `concat!` so this test cannot match its own text.
    #[test]
    fn discard_rechecks_the_marker_after_hashing_and_before_unlinking() {
        let src = include_str!("rollback.rs");
        let fn_start = src
            .find(concat!("fn discard_", "known_good_at("))
            .expect("discard fn present");
        let body = &src[fn_start..];
        let body = &body[..body.find("\n}\n").expect("fn body terminates")];

        let hash_at = body
            .find(concat!("sha256", "_file("))
            .expect("the blob is hashed");
        let unlink_at = body
            .find(concat!("remove", "_file("))
            .expect("the blob is unlinked");
        assert!(hash_at < unlink_at, "hash must precede the unlink");
        assert!(
            body[hash_at..unlink_at].contains(concat!("read_", "probation_at(")),
            "the probation marker MUST be re-read between hashing the blob and \
             unlinking it — a marker armed during the hash would otherwise be \
             missed, leaving an armed marker pointing at a deleted binary"
        );
    }

    /// Source pin: the probation commit does a cold ~58 MB read + SHA-256, so
    /// it must never run on a tokio worker thread. It fires at T+60s of the
    /// first boot after every auto-update, on every peer, during ring
    /// bootstrap — blocking a worker there is a fleet-wide regression.
    /// Whitespace-stripped so rustfmt reflowing the call cannot break it.
    #[test]
    fn commit_probation_runs_on_a_blocking_thread() {
        let src = include_str!("../freenet.rs");
        let stripped: String = src.chars().filter(|c| !c.is_whitespace()).collect();
        assert!(
            stripped.contains(concat!(
                "spawn_blocking(move||commands::rollback::",
                "commit_probation(&version))"
            )),
            "bin/freenet.rs must call rollback::commit_probation via \
             spawn_blocking; it performs a cold ~58 MB read + SHA-256 and would \
             stall a runtime worker during ring bootstrap"
        );
    }

    #[test]
    fn commit_succeeds_even_when_the_blob_cannot_be_verified() {
        // Reclamation is best-effort: an unreadable blob must be left alone and
        // must not change (or fail) the commit outcome.
        let tmp = tempfile::tempdir().unwrap();
        let dir = tmp.path();
        setup_probation(dir, "0.2.84", "0.2.83");
        // A directory at the blob path: exists, but hashing it fails.
        std::fs::remove_file(known_good_path(dir)).unwrap();
        std::fs::create_dir(known_good_path(dir)).unwrap();

        assert_eq!(commit_probation_at(dir, "0.2.84"), CommitOutcome::Committed);

        assert!(read_probation_at(dir).is_none(), "commit still happened");
        assert!(
            known_good_path(dir).is_dir(),
            "an unverifiable blob is left in place, never removed on a guess"
        );
    }

    #[test]
    fn next_install_recaptures_a_fresh_blob_after_commit_reclaimed_it() {
        // End-to-end: the reclaim must not break the NEXT update cycle. From a
        // state where the blob is ABSENT (not merely stale),
        // prepare_known_good_for_install_at has to capture the live binary
        // fresh, label it the version being replaced, and produce a rollback
        // target that actually restores.
        let tmp = tempfile::tempdir().unwrap();
        let dir = tmp.path();
        let live = setup_probation(dir, "0.2.84", "0.2.83");
        assert_eq!(commit_probation_at(dir, "0.2.84"), CommitOutcome::Committed);
        assert!(!known_good_path(dir).exists(), "blob reclaimed at commit");
        assert!(read_probation_at(dir).is_none());

        // The next install snapshots the (now proven-good) 0.2.84 binary.
        let (meta, previous) = prepare_known_good_for_install_at(dir, "0.2.84", &live).unwrap();
        assert_eq!(previous, "0.2.84", "fresh capture is labelled 0.2.84");
        assert_eq!(std::fs::read(known_good_path(dir)).unwrap(), b"BAD-BINARY");
        let (size, hash) = sha256_file(&known_good_path(dir)).unwrap();
        assert_eq!(meta.size, size);
        assert_eq!(meta.sha256, hash);

        // Arm 0.2.85 over it and confirm the fresh target really restores.
        write_dummy_binary(&live, b"WORSE-BINARY");
        begin_probation_at(dir, "0.2.85", &previous, &live, &meta).unwrap();
        for _ in 0..ROLLBACK_CRASH_THRESHOLD - 1 {
            handle_post_stop_at(dir, "101", "0.2.85");
        }
        let PostStopOutcome::RolledBack {
            restored_version,
            bad_version,
        } = handle_post_stop_at(dir, "101", "0.2.85")
        else {
            panic!("expected RolledBack");
        };
        assert_eq!(restored_version, "0.2.84");
        assert_eq!(bad_version, "0.2.85");
        assert_eq!(std::fs::read(&live).unwrap(), b"BAD-BINARY");
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

        // A successful forward install supersedes the pin. Capture a real
        // known-good blob first, as every production caller does — arming
        // probation now verifies its rollback target exists and refuses to
        // record one that does not (see
        // begin_probation_disarms_when_the_known_good_blob_is_missing).
        let live = dir.join("freenet");
        write_dummy_binary(&live, b"NEWER");
        let meta = capture_known_good_at(dir, &live).unwrap();
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
    fn prepare_known_good_recaptures_when_marker_aged_out() {
        // A marker for the CURRENT version that survived past the TTL means the
        // current version has actually run fine for a long time (commit failed
        // to clear it). The next install must NOT carry the stale older blob
        // forward — it must re-capture the current binary as the known-good.
        let tmp = tempfile::tempdir().unwrap();
        let dir = tmp.path();
        let live = setup_probation(dir, "0.2.84", "0.2.83");
        // Backdate the marker beyond the TTL.
        let mut state = read_probation_at(dir).unwrap();
        state.installed_at_unix = now_unix().saturating_sub(PROBATION_MAX_AGE_SECS + 60);
        write_probation_at(dir, &state).unwrap();

        let (meta, previous) = prepare_known_good_for_install_at(dir, "0.2.84", &live).unwrap();

        // Re-captured: label is the current version (not the stale 0.2.83), and
        // the blob is the current binary, so blob and label agree.
        assert_eq!(previous, "0.2.84");
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
    fn install_gate_engages_after_threshold_failures_of_same_version() {
        // Core #4073 regression: repeated failed installs of the SAME version
        // must, after the threshold, gate that version out of update detection —
        // this is what bounds the detect → exit 42 → failed install → restart
        // loop a bad manifest/checksum would otherwise sustain forever.
        let tmp = tempfile::tempdir().unwrap();
        let dir = tmp.path();

        for n in 1..INSTALL_FAILURE_GATE_THRESHOLD {
            record_install_failure_at(dir, "0.2.90");
            assert!(
                !is_version_install_gated_at(dir, "0.2.90"),
                "below threshold ({n}) must not gate yet"
            );
        }
        record_install_failure_at(dir, "0.2.90");
        assert!(
            is_version_install_gated_at(dir, "0.2.90"),
            "threshold reached: version must be gated"
        );
    }

    #[test]
    fn install_gate_allows_strictly_newer_version() {
        // A gated version must NOT block a different (newer) release — the fix.
        let tmp = tempfile::tempdir().unwrap();
        let dir = tmp.path();
        for _ in 0..INSTALL_FAILURE_GATE_THRESHOLD {
            record_install_failure_at(dir, "0.2.90");
        }
        assert!(is_version_install_gated_at(dir, "0.2.90"));
        assert!(
            !is_version_install_gated_at(dir, "0.2.91"),
            "a newer version must not be gated by an older version's failures"
        );
    }

    #[test]
    fn install_gate_resets_when_target_version_changes() {
        // If a newer release becomes the failing target, the counter resets to
        // track it (count 1), so the old version's accumulated failures don't
        // instantly gate the new one.
        let tmp = tempfile::tempdir().unwrap();
        let dir = tmp.path();
        for _ in 0..INSTALL_FAILURE_GATE_THRESHOLD {
            record_install_failure_at(dir, "0.2.90");
        }
        assert!(is_version_install_gated_at(dir, "0.2.90"));

        record_install_failure_at(dir, "0.2.91");
        let state = read_install_failures_at(dir).unwrap();
        assert_eq!(state.version, "0.2.91");
        assert_eq!(state.count, 1, "new target starts a fresh count");
        assert!(!is_version_install_gated_at(dir, "0.2.91"));
        // The old version is no longer tracked, so it is no longer gated either.
        assert!(!is_version_install_gated_at(dir, "0.2.90"));
    }

    #[test]
    fn install_gate_cleared_on_success() {
        let tmp = tempfile::tempdir().unwrap();
        let dir = tmp.path();
        for _ in 0..INSTALL_FAILURE_GATE_THRESHOLD {
            record_install_failure_at(dir, "0.2.90");
        }
        assert!(is_version_install_gated_at(dir, "0.2.90"));

        clear_install_failures_at(dir);
        assert!(read_install_failures_at(dir).is_none());
        assert!(!is_version_install_gated_at(dir, "0.2.90"));
        // Clearing an already-clear counter is idempotent.
        clear_install_failures_at(dir);
        assert!(!is_version_install_gated_at(dir, "0.2.90"));
    }

    #[test]
    fn install_gate_degrades_safe_on_missing_or_corrupt() {
        // Degrade-safe (NOT fail-closed): a missing or corrupt record reads as
        // "not gated" so a torn file can never brick auto-update entirely.
        let tmp = tempfile::tempdir().unwrap();
        let dir = tmp.path();

        // Missing.
        assert!(!is_version_install_gated_at(dir, "0.2.90"));
        assert!(read_install_failures_at(dir).is_none());

        // Corrupt.
        std::fs::write(dir.join(INSTALL_FAILURES_FILE), "{not valid json").unwrap();
        assert!(read_install_failures_at(dir).is_none());
        assert!(!is_version_install_gated_at(dir, "0.2.90"));
    }

    #[test]
    fn install_gate_uses_atomic_write_no_temp_left_behind() {
        // Writes go through atomic_write (tmp + rename); no stray temp remains.
        let tmp = tempfile::tempdir().unwrap();
        let dir = tmp.path();
        record_install_failure_at(dir, "0.2.90");
        assert!(read_install_failures_at(dir).is_some());
        let leftover_tmp = dir.join(format!(".{INSTALL_FAILURES_FILE}.tmp"));
        assert!(
            !leftover_tmp.exists(),
            "atomic_write must not leave a temp file behind"
        );
    }

    #[test]
    fn install_failure_loop_is_bounded_by_the_gate() {
        // End-to-end bound: simulate the failed-install loop. Each cycle the node
        // would detect X and the supervisor's `freenet update` would fail to
        // install X (recording a failure). After at most
        // INSTALL_FAILURE_GATE_THRESHOLD cycles the node's detection is gated and
        // stops emitting exit 42 for X — so the loop cannot run unbounded.
        let tmp = tempfile::tempdir().unwrap();
        let dir = tmp.path();

        let mut emitted_exit_42 = 0u32;
        for _cycle in 0..1000 {
            // Node detection: would it emit exit 42 for X this cycle?
            if is_version_install_gated_at(dir, "0.2.90") {
                break; // gated -> node stays put, loop is broken
            }
            emitted_exit_42 += 1;
            // Supervisor runs `freenet update`, install fails -> record.
            record_install_failure_at(dir, "0.2.90");
        }

        assert!(
            is_version_install_gated_at(dir, "0.2.90"),
            "the loop must end with the version gated"
        );
        assert_eq!(
            emitted_exit_42, INSTALL_FAILURE_GATE_THRESHOLD,
            "node must stop emitting exit 42 after exactly the threshold cycles"
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
