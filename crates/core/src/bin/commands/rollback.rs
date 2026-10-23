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
//!    a rollback. Clearing can FAIL (an unwritable or full state directory —
//!    #5244); the marker then stays armed, nothing retries within the process,
//!    and [`commit_announcement`] says so rather than reporting a disarm that
//!    did not happen. A marker naming a DIFFERENT version is also removed here,
//!    and that removal can fail the same way — but it is a materially different
//!    situation (that marker could never have rolled this node back, see step
//!    3), so the two failures are distinct outcomes with distinct wording.
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
use std::io::Write;
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
/// `pub(crate)` because the Nix supervisor (`nix/freenet-node.sh`) must read
/// this same file before it re-seeds a binary, and a pin in `bin/freenet.rs`
/// asserts the shell looks for THIS name. A private constant with a literal
/// copy in the pin would be rewritten by the same rename it is meant to catch.
pub(crate) const KNOWN_BAD_FILE: &str = "known_bad_version";

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

/// [`atomic_write`] for the update records (#5799), which the node, the
/// updater and a tray "Check for Updates" can write at once: each write uses
/// its own temporary name, so one never renames another's half-written bytes
/// into place. The crash-loop rollback files keep [`atomic_write`]'s fixed
/// name, which their writers never share.
fn atomic_write_record(path: &Path, bytes: &[u8]) -> Result<()> {
    use std::io::Write;
    atomic_write_record_with(path, bytes, |file, bytes| file.write_all(bytes))
}

/// [`atomic_write_record`] with the write injected, so the cleanup is
/// testable.
fn atomic_write_record_with(
    path: &Path,
    bytes: &[u8],
    write: impl FnOnce(&mut std::fs::File, &[u8]) -> std::io::Result<()>,
) -> Result<()> {
    let dir = path.parent().context("path has no parent directory")?;
    std::fs::create_dir_all(dir).ok();
    let file_name = path
        .file_name()
        .context("path has no file name")?
        .to_string_lossy();
    static SEQ: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
    let seq = SEQ.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    let tmp = dir.join(format!(".{file_name}.{}.{seq}.tmp", std::process::id()));
    let written = std::fs::File::create(&tmp)
        .context("create temp for atomic write")
        .and_then(|mut f| {
            write(&mut f, bytes).context("write temp for atomic write")?;
            f.sync_all().context("fsync temp for atomic write")
        });
    if let Err(e) = written {
        // A unique name is never reused, so a temp left here would stay.
        let _rm = std::fs::remove_file(&tmp);
        return Err(e);
    }
    if let Err(e) = std::fs::rename(&tmp, path) {
        let _rm = std::fs::remove_file(&tmp);
        return Err(e).context("rename temp into place");
    }
    fsync_dir(dir);
    Ok(())
}

/// Every file [`atomic_write_record`] writes in the state directory. Only
/// their temporaries are swept; `record_writes_name_only_swept_files` keeps
/// this list complete.
const RECORD_FILES: [&str; 2] = [UPDATE_COOLDOWN_FILE, DOWNLOAD_BUDGET_FILE];
/// How old a temporary must be before the sweep removes it: far longer than
/// any write takes, so a write in progress in another process is never hit.
const STALE_TEMP_AGE: std::time::Duration = std::time::Duration::from_secs(3600);

/// Whether `name` is a temporary [`atomic_write_record`] makes for one of
/// [`RECORD_FILES`]: `.{file}.{pid}.{seq}.tmp`.
fn is_record_temp(name: &str) -> bool {
    RECORD_FILES.iter().any(|file| {
        name.strip_prefix('.')
            .and_then(|rest| rest.strip_prefix(*file))
            .and_then(|rest| rest.strip_suffix(".tmp"))
            .and_then(|middle| middle.strip_prefix('.'))
            .and_then(|ids| ids.split_once('.'))
            .is_some_and(|(pid, seq)| {
                [pid, seq]
                    .iter()
                    .all(|n| !n.is_empty() && n.bytes().all(|b| b.is_ascii_digit()))
            })
    })
}

/// Remove [`atomic_write_record`] temporaries in `dir` older than
/// [`STALE_TEMP_AGE`] and return how many. A unique name is never reused, so
/// a temporary left by a SIGKILL or a power loss mid-write would otherwise
/// stay for good. Nothing else in the directory is touched.
pub(crate) fn sweep_stale_atomic_temps_at(dir: &Path, now: SystemTime) -> usize {
    let Ok(entries) = std::fs::read_dir(dir) else {
        return 0;
    };
    entries
        .flatten()
        .filter(|entry| entry.file_name().to_str().is_some_and(is_record_temp))
        .filter(|entry| {
            entry
                .metadata()
                .and_then(|m| m.modified())
                .is_ok_and(|mtime| {
                    now.duration_since(mtime)
                        .is_ok_and(|age| age >= STALE_TEMP_AGE)
                })
        })
        .filter(|entry| std::fs::remove_file(entry.path()).is_ok())
        .count()
}

/// [`sweep_stale_atomic_temps_at`] on the shared state directory, at startup.
pub fn sweep_stale_atomic_temps() {
    if let Some(dir) = state_dir() {
        let removed = sweep_stale_atomic_temps_at(&dir, SystemTime::now());
        if removed > 0 {
            tracing::info!(
                removed,
                "Removed stale temporary files from the state directory"
            );
        }
    }
}

/// Try to take an exclusive POSIX record lock (`fcntl(F_SETLK)`) on the whole
/// of `file` without blocking: `Ok(false)` when another process holds it.
/// Record locks belong to the process, are not inherited across fork, and do
/// NOT exclude other callers in the same process; closing any descriptor of
/// the file in this process releases them. Callers add their own in-process
/// exclusion. Off unix this always succeeds.
pub(crate) fn try_lock_file(file: &std::fs::File) -> std::io::Result<bool> {
    #[cfg(unix)]
    {
        use std::os::unix::io::AsRawFd;
        // SAFETY: an all-zero `flock` is a valid value; the fields that matter
        // are set below. Whole file (`l_start` = `l_len` = 0).
        let mut request: libc::flock = unsafe { std::mem::zeroed() };
        request.l_type = libc::F_WRLCK as _;
        request.l_whence = libc::SEEK_SET as _;
        // SAFETY: `file` owns an open descriptor for the duration of the call,
        // and `request` is a valid `flock` that fcntl only reads.
        if unsafe { libc::fcntl(file.as_raw_fd(), libc::F_SETLK, &request) } != 0 {
            let e = std::io::Error::last_os_error();
            // POSIX allows either errno for "held by another process".
            return if matches!(e.raw_os_error(), Some(libc::EAGAIN | libc::EACCES)) {
                Ok(false)
            } else {
                Err(e)
            };
        }
    }
    #[cfg(not(unix))]
    let _ = file;
    Ok(true)
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

/// Remove the probation marker, reporting whether it is actually gone.
///
/// The result is NOT decorative: every caller has just decided that rollback
/// should no longer fire, and a failed unlink leaves the marker ARMED. A caller
/// that announces the decision must not announce it when this failed (#5232).
///
/// "Already absent" is success — the post-condition is "no marker", not "I
/// deleted something".
fn remove_probation_at(dir: &Path) -> std::io::Result<()> {
    match std::fs::remove_file(dir.join(PROBATION_FILE)) {
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
        other => other,
    }
}

/// The operator-facing line for a marker-clearing outcome, or `None` when
/// nothing happened. Split out from [`commit_probation`] so every branch is
/// unit-testable: `commit_probation` itself reads the process-global `$HOME`
/// (or `$STATE_DIRECTORY`) through [`state_dir`], cached for the process, and
/// so cannot be driven from an in-process test.
///
/// Every outcome gets its OWN wording. The four are reached from four different
/// arms of [`commit_probation_at`] and differ in both facts an operator acts on
/// — whether this version passed probation, and whether rollback is still live
/// — so a line shared between two of them is wrong for at least one. That is
/// the whole point of #5232: never announce a state the node is not in.
///
/// ## Why these go to stderr and not only to `tracing` (#5232)
///
/// **This is the canonical explanation; the other comments point here.** Under
/// the shipped systemd deployment the node's `tracing` output goes to the
/// rolling log files under the log dir (`tracing::tracer::init_tracer` adds the
/// console layer only when stdout is a terminal, which it is not under a service
/// manager), while the unit records `StandardOutput=journal` /
/// `StandardError=journal`. So `tracing` does not reach the journal and
/// stdout/stderr does. The three other decisions in this flow — the crash count,
/// the rollback, and rollback-unavailable — are all `eprintln!` in
/// [`super::update`], so `journalctl -u freenet` showed strikes accumulating
/// against a probationary version and never once showed the commit that disarms
/// them. #5232 was filed reading exactly that asymmetry. This puts the
/// marker-clearing decisions on the same channel as the decisions they cancel.
///
/// (The fallback tracer — used when no log dir resolves, or when
/// `FREENET_LOG_TO_STDERR` is set — does write to stdout, so on those paths the
/// `tracing` line reaches the journal too and the operator sees both.)
///
/// Volume is bounded: at most one line per node start, and only when a probation
/// marker actually exists.
pub(crate) fn commit_announcement(
    outcome: &CommitOutcome,
    current_version: &str,
) -> Option<String> {
    match outcome {
        CommitOutcome::Committed => Some(format!(
            "Freenet {current_version}: post-update probation passed (ran healthily for \
             {COMMIT_HEALTHY_UPTIME_SECS}s); committed, auto-rollback disarmed."
        )),
        // Deliberately as loud as the commit. This branch drops rollback
        // protection for `marker_version` WITHOUT committing anything, it is the
        // branch a version-comparison regression would take, and it is
        // indistinguishable from a healthy commit by the state directory alone
        // (both just delete the marker).
        CommitOutcome::ClearedStale { marker_version } => Some(format!(
            "Freenet {current_version}: discarded a post-update probation marker belonging to \
             {marker_version} (not the running version); auto-rollback protection for \
             {marker_version} is gone."
        )),
        // The state change did NOT happen, so this must not read like the two
        // above. The marker names the RUNNING version, is still on disk and
        // still inside its TTL, so an ordinary crash in the next hour can still
        // roll this node back off a version that just proved itself — and the
        // commit timer fires once per process, so nothing retries.
        CommitOutcome::CommitFailed {
            marker_version,
            error,
        } => Some(format!(
            "Freenet {current_version}: ran healthily for {COMMIT_HEALTHY_UPTIME_SECS}s, but the \
             post-update probation marker for {marker_version} could NOT be removed ({error}); \
             auto-rollback is STILL ARMED and a crash within the next hour can still roll this \
             node back. Check the permissions and free space on the Freenet state directory."
        )),
        // Neither half of the CommitFailed line is true here, which is why the
        // two are separate variants:
        //
        //  - The running version was never on probation (the marker names
        //    another version), so it has not "passed" anything and saying it
        //    "ran healthily for {window}s" would describe a test that was not
        //    performed.
        //  - Rollback is NOT armed. `handle_post_stop_at` checks
        //    `state.new_version != current_version` before it counts anything,
        //    removes the marker and proceeds, so no crash of THIS version is
        //    ever scored against this marker. Claiming otherwise would tell the
        //    operator brick-safety equipment is live when it is not.
        //
        // What IS actionable is the condition both branches share and only this
        // one has left unsaid: the state directory could not be written, which
        // is the same condition that breaks the rest of the update machinery
        // (#5244) — including capturing a rollback target for the NEXT update.
        CommitOutcome::StaleClearFailed {
            marker_version,
            error,
        } => Some(format!(
            "Freenet {current_version}: a leftover post-update probation marker belonging to \
             {marker_version} (not the running version) could NOT be removed ({error}). It does \
             not put this node at risk of rollback — a crash is only counted against a marker \
             naming the running version — but the Freenet state directory could not be written, \
             which will also block capturing a rollback target for the next update. Check its \
             permissions and free space."
        )),
        CommitOutcome::Nothing => None,
    }
}

/// Clear probation if the running version matches the marker (it has proven
/// healthy). A marker for a DIFFERENT version is stale (e.g. an external install
/// replaced the binary, or two binaries share a `$HOME`) and is also removed.
///
/// Both outcomes DISARM rollback for the marker's version, so both are announced
/// on stderr as well as through `tracing` — see [`commit_announcement`] for why
/// stderr specifically.
///
/// Reached only from the node's commit timer, so "survived the window ⇒
/// committed" holds for a process that actually ran the network. A node parked
/// by `freenet service disable` (`run_disabled_idle`) never reaches it and keeps
/// its marker until the [`PROBATION_MAX_AGE_SECS`] TTL retires it. That is
/// deliberate: a process that never joined the network has not demonstrated the
/// health this probation tests for, so committing there would disarm rollback on
/// evidence the mechanism does not have.
pub fn commit_probation(current_version: &str) {
    if let Some(dir) = state_dir() {
        // Ordering: transition, then log, then announce. The stderr write goes
        // LAST because it is the only step that can fail or block.
        //
        // The write itself no longer panics (see the `writeln!` below), but it
        // still shares a process-global lock and can block on a stalled reader,
        // and a future edit that reintroduces a panicking macro must not be able
        // to break the two steps above it:
        //
        //  - Before the removal, a failure would leave a healthy node's marker
        //    armed — an observability line turned into a rollback bug.
        //  - Before the `tracing` record, it would take that record with it,
        //    leaving the operator with NEITHER channel on exactly the node whose
        //    stderr is already broken. That is strictly worse than before this
        //    announcement existed, when the record always got written.
        //
        // The message is built up front because the `match` below consumes
        // `outcome`; building a string cannot fail, so doing it early costs
        // nothing.
        let outcome = commit_probation_at(dir.as_path(), current_version);
        let announcement = commit_announcement(&outcome, current_version);
        match outcome {
            CommitOutcome::Committed => tracing::info!(
                version = current_version,
                "Auto-update probation passed: new version ran healthily for \
                 {COMMIT_HEALTHY_UPTIME_SECS}s; committing (rollback disarmed)."
            ),
            // Was `debug!`, which a release build compiles out entirely
            // (`release_max_level_info`), so this branch was completely silent
            // in the field. See `commit_announcement` for why that matters.
            CommitOutcome::ClearedStale { marker_version } => tracing::warn!(
                running = current_version,
                marker = %marker_version,
                "Cleared stale auto-update probation marker for a different version."
            ),
            CommitOutcome::CommitFailed {
                marker_version,
                error,
            } => tracing::error!(
                running = current_version,
                marker = %marker_version,
                error = %error,
                "Probation passed but the marker could not be removed; rollback is STILL armed."
            ),
            CommitOutcome::StaleClearFailed {
                marker_version,
                error,
            } => tracing::error!(
                running = current_version,
                marker = %marker_version,
                error = %error,
                "A stale probation marker for another version could not be removed; rollback is \
                 not armed for the running version, but the state directory is not writable."
            ),
            CommitOutcome::Nothing => {}
        }
        if let Some(line) = announcement {
            announce(&mut std::io::stderr(), &line);
        }
    }
}

/// Write one announcement line, swallowing a write error.
///
/// **Must not panic.** `eprintln!` — which this replaced — panics on a write
/// error (EPIPE once the supervisor's reader is gone; a Windows child that
/// inherited invalid standard handles), and [`commit_probation`] runs on a
/// tokio worker. An observability line must never be able to take the process
/// down: `panic = "abort"` sits commented out in `crates/core/Cargo.toml`, and
/// under it an EPIPE at t=60s would abort the node, which exits non-zero, which
/// the post-stop handler scores as a probation CRASH. Three of those roll the
/// node back, so the announcement of a successful commit would itself have
/// caused the rollback it reports disarming.
///
/// Taken as `impl Write` rather than writing to `std::io::stderr()` inline so
/// that property is testable at all — a test cannot break the real stderr. See
/// `tests::a_broken_stderr_does_not_panic_the_announcement`, which drives a
/// writer that fails every call and goes RED against either panicking form
/// (`eprintln!`, or a `.unwrap()`/`.expect()` on this `writeln!`).
fn announce(out: &mut impl Write, line: &str) {
    let _broken_stderr = writeln!(out, "{line}");
}

#[derive(Debug, PartialEq, Eq)]
pub(crate) enum CommitOutcome {
    /// The running version matched the probation marker and was committed.
    Committed,
    /// A marker for a different version was found and removed as stale.
    ClearedStale { marker_version: String },
    /// The marker named the RUNNING version and could NOT be removed, so
    /// rollback is still ARMED even though this version has proven healthy.
    /// Kept distinct from the two success outcomes so the announcement cannot
    /// claim a disarm that did not happen.
    CommitFailed {
        marker_version: String,
        error: String,
    },
    /// A marker for a DIFFERENT version could not be removed.
    ///
    /// Split from [`CommitOutcome::CommitFailed`] because the two are reached
    /// from different arms of [`commit_probation_at`] and almost nothing true of
    /// one is true of the other. Here the running version was never on probation
    /// (so it has not "passed" anything), and the stuck marker cannot roll this
    /// node back either: [`handle_post_stop_at`] tests
    /// `state.new_version != current_version` before counting anything, drops
    /// the marker and returns [`PostStopOutcome::Proceed`]. Announcing this with
    /// the `CommitFailed` wording would tell the operator that brick-safety
    /// equipment is live when it is not, and would omit the condition that
    /// actually needs attention: an unwritable state directory.
    StaleClearFailed {
        marker_version: String,
        error: String,
    },
    /// No probation marker present.
    Nothing,
}

pub(crate) fn commit_probation_at(dir: &Path, current_version: &str) -> CommitOutcome {
    match read_probation_at(dir) {
        Some(state) if state.new_version == current_version => match remove_probation_at(dir) {
            Ok(()) => CommitOutcome::Committed,
            Err(e) => CommitOutcome::CommitFailed {
                marker_version: state.new_version,
                error: e.to_string(),
            },
        },
        Some(state) => {
            // Running a different version healthily than the marker describes:
            // the marker can no longer protect anything, so drop it.
            match remove_probation_at(dir) {
                Ok(()) => CommitOutcome::ClearedStale {
                    marker_version: state.new_version,
                },
                // NOT `CommitFailed`: the running version was never on
                // probation, and a marker naming another version cannot roll
                // this node back. Different facts, so a different line.
                Err(e) => CommitOutcome::StaleClearFailed {
                    marker_version: state.new_version,
                    error: e.to_string(),
                },
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

// ── Per-target-version update cooldown (#5799) ─────────────────────────────
//
// The install gate above deliberately ignores TRANSIENT failures, because
// gating on them would permanently suppress a good release after a brief
// outage. Ignoring them entirely left nothing to stop the loop on a network
// where GitHub downloads keep failing: detect X, exit 42, the download fails,
// restart on the old binary, detect X, exit 42, every few minutes (#5799).
//
// This record is the time-bounded answer. A failure to fetch X, by the node or
// by the updater, records X with a retry time; until then nothing asks GitHub
// about X or exits for it. The wait is 1 h, then 2 h, then 4 h at most, each
// ±20%. It always expires, a failure of a different release replaces it with
// a fresh count, and a successful install clears it. A missing, corrupt or implausible record reads
// as no cooldown: the worst that costs is one attempt, the behaviour before.

/// The wait after the first failure to fetch a release.
pub(crate) const UPDATE_COOLDOWN_BASE: std::time::Duration = std::time::Duration::from_secs(3600);
/// The longest wait, from the third consecutive failure on.
pub(crate) const UPDATE_COOLDOWN_MAX: std::time::Duration =
    std::time::Duration::from_secs(4 * 3600);
/// Fraction by which each wait is randomly shortened or lengthened.
const UPDATE_COOLDOWN_JITTER: f64 = 0.2;
/// How long after its retry time a record's failures stop counting toward
/// the fallback. A failure that old says nothing about the network now, and
/// counting it could send a node that tries only now and then straight to
/// the fallback on its first new failure. Two days, not one: a #3934
/// locked-out node attempts about once a day plus the re-poll's lag, so a
/// one-day expiry would rarely let its failures add up to the fallback.
const UPDATE_FAILURES_EXPIRE_AFTER: std::time::Duration = std::time::Duration::from_secs(48 * 3600);
const UPDATE_COOLDOWN_FILE: &str = "update_cooldown.json";
/// Longest error text kept, so the record `freenet service report` reads
/// stays small.
const UPDATE_COOLDOWN_ERROR_MAX_CHARS: usize = 500;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct UpdateCooldownState {
    /// The release that could not be fetched.
    pub version: String,
    /// Consecutive failures of that release.
    pub failures: u32,
    /// Unix seconds of the last failure.
    pub failed_at_unix: u64,
    /// Unix seconds before which nothing asks GitHub about, or exits for,
    /// `version`. Stored rather than derived so its jitter is stable.
    pub retry_at_unix: u64,
    /// The last failure, for `freenet service report`. A hold keeps it.
    pub last_error: String,
    /// Why the release was last held without a failure being counted (an
    /// updater run that attempted no download, an empty poll bucket), if the
    /// latest write was such a hold.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub hold_reason: Option<String>,
    /// Written by the node as it exits for the updater: the attempt is
    /// counted in case the updater never reports, and the updater's own
    /// outcome for the same attempt replaces it rather than counting again.
    /// Every other write clears it, a hold included: a hold is a report on
    /// the attempt, so the next failure is a new attempt and counts.
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub provisional: bool,
}

/// The wait after the `failures`-th consecutive failure, given a uniform
/// sample `rand_unit` in `[0, 1]`: 1 h, 2 h, then 4 h, each ±20%.
pub(crate) fn update_cooldown_delay(failures: u32, rand_unit: f64) -> std::time::Duration {
    let doublings = failures.saturating_sub(1).min(8);
    let base = UPDATE_COOLDOWN_BASE
        .saturating_mul(1u32 << doublings)
        .min(UPDATE_COOLDOWN_MAX);
    let factor =
        1.0 - UPDATE_COOLDOWN_JITTER + 2.0 * UPDATE_COOLDOWN_JITTER * rand_unit.clamp(0.0, 1.0);
    base.mul_f64(factor)
}

pub(crate) fn read_update_cooldown_at(dir: &Path) -> Option<UpdateCooldownState> {
    let raw = std::fs::read_to_string(dir.join(UPDATE_COOLDOWN_FILE)).ok()?;
    serde_json::from_str(&raw).ok()
}

fn unix_secs(time: SystemTime) -> u64 {
    time.duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

/// The failures of `prev` that still count at `now`: none once its retry time
/// is more than [`UPDATE_FAILURES_EXPIRE_AFTER`] in the past.
fn live_failures(prev: &UpdateCooldownState, now: SystemTime) -> u32 {
    let expired = unix_secs(now)
        > prev
            .retry_at_unix
            .saturating_add(UPDATE_FAILURES_EXPIRE_AFTER.as_secs());
    if expired { 0 } else { prev.failures }
}

/// Write a record for `version` at `failures`, waiting from `now`. `false`
/// when it could not be written.
fn write_update_cooldown_at(
    dir: &Path,
    version: &str,
    failures: u32,
    (error, hold_reason): (&str, Option<&str>),
    (now, rand_unit): (SystemTime, f64),
    provisional: bool,
) -> bool {
    let now_unix = unix_secs(now);
    let bounded =
        |text: &str| -> String { text.chars().take(UPDATE_COOLDOWN_ERROR_MAX_CHARS).collect() };
    store_update_cooldown_at(
        dir,
        &UpdateCooldownState {
            version: version.to_string(),
            failures,
            failed_at_unix: now_unix,
            retry_at_unix: now_unix
                .saturating_add(update_cooldown_delay(failures, rand_unit).as_secs()),
            last_error: bounded(error),
            hold_reason: hold_reason.map(bounded),
            provisional,
        },
    )
}

/// Write `state` as the record. `false` when it could not be written.
fn store_update_cooldown_at(dir: &Path, state: &UpdateCooldownState) -> bool {
    let written = serde_json::to_vec(state)
        .context("serialize update cooldown")
        .and_then(|raw| atomic_write_record(&dir.join(UPDATE_COOLDOWN_FILE), &raw));
    if let Err(e) = &written {
        tracing::warn!(version = %state.version, error = %e, "Failed to persist the update cooldown (#5799)");
    }
    written.is_ok()
}

/// Record a failure to fetch `version` and return the consecutive failure
/// count, this one included (failures whose retry time is more than two
/// days past no longer count). Every failure writes a cooldown for the release
/// that actually failed: a record for any other release (or the
/// [`UNKNOWN_RELEASE`] sentinel) is replaced, starting the count at 1. Keeping
/// another release's record instead would leave the failing one with no
/// cooldown at all: after a recorded release is withdrawn, the older latest
/// release would be retried back to back, and with the withdrawn release's
/// count at the fallback every failure would exit (#5799).
///
/// `None` when it could not be written. Callers then exit for the update (fail
/// open): an unrecorded failure would never count toward the fallback, and the
/// node could stay on its old binary for good.
///
/// A failure for the release of a provisional record (see
/// [`record_provisional_failure_at`]) is that same attempt's outcome: it
/// replaces the record at its count instead of counting again.
pub(crate) fn record_update_failure_at(
    dir: &Path,
    version: &str,
    error: &str,
    now: SystemTime,
    rand_unit: f64,
) -> Option<u32> {
    record_failure_with(dir, version, error, (now, rand_unit), false)
}

/// [`record_update_failure_at`] for the node exiting for the updater: the
/// attempt counts now, in case the updater is killed before it reports, and
/// the record is marked provisional so the updater's outcome replaces it.
pub(crate) fn record_provisional_failure_at(
    dir: &Path,
    version: &str,
    error: &str,
    now: SystemTime,
    rand_unit: f64,
) -> Option<u32> {
    record_failure_with(dir, version, error, (now, rand_unit), true)
}

fn record_failure_with(
    dir: &Path,
    version: &str,
    error: &str,
    (now, rand_unit): (SystemTime, f64),
    provisional: bool,
) -> Option<u32> {
    with_record_lock(dir, || {
        let failures = match read_update_cooldown_at(dir) {
            // The updater reporting on the attempt the node already counted.
            Some(prev) if prev.version == version && prev.provisional && !provisional => {
                live_failures(&prev, now).max(1)
            }
            Some(prev) if prev.version == version => live_failures(&prev, now).saturating_add(1),
            _ => 1,
        };
        // One write, flag included: a second write setting the flag could fail
        // after the first succeeded, leaving a counted record the updater's
        // outcome would count again.
        write_update_cooldown_at(
            dir,
            version,
            failures,
            (error, None),
            (now, rand_unit),
            provisional,
        )
        .then_some(failures)
    })
}

/// Where the record lock lives, beside the records it guards.
const RECORD_LOCK_FILE: &str = "update_records.lock";
/// How long a writer waits for the record lock before going ahead without
/// it. Holders only read and rewrite a small file, so this is only reached if
/// one is wedged; losing one update to a race then costs one attempt. Short,
/// because the wait blocks its thread and staging writes from async code.
const RECORD_LOCK_WAIT: std::time::Duration = std::time::Duration::from_millis(100);

/// Run `f`, a read-modify-write of the records in `dir`, with them locked
/// against every other writer: other threads by a mutex, other processes (the
/// node, the updater, a tray "Check for Updates") by a POSIX record lock on
/// [`RECORD_LOCK_FILE`]. Unlocked, concurrent writers lose each other's
/// increments. Fails open: if the lock cannot be taken within
/// [`RECORD_LOCK_WAIT`], or at all, `f` runs anyway.
fn with_record_lock<T>(dir: &Path, f: impl FnOnce() -> T) -> T {
    // Serialises this process's writers, which a record lock cannot, and
    // keeps at most one descriptor of the lock file open here: closing a
    // second one would release the lock.
    static IN_PROCESS: std::sync::Mutex<()> = std::sync::Mutex::new(());
    let _thread = IN_PROCESS.lock().unwrap_or_else(|e| e.into_inner());
    let _process = lock_records(dir);
    #[cfg(test)]
    LAST_RECORD_LOCK_TAKEN.with(|taken| taken.set(Some(_process.is_some())));
    f()
}

#[cfg(test)]
thread_local! {
    /// Run on this thread each time a record writer finds the lock busy, so a
    /// test can release the holder at that moment, whatever else delayed
    /// the writer.
    static ON_BUSY_RECORD_LOCK: std::cell::RefCell<Option<Box<dyn FnMut()>>> =
        std::cell::RefCell::new(None);
    /// Whether this thread's last record writer got the lock.
    static LAST_RECORD_LOCK_TAKEN: std::cell::Cell<Option<bool>> =
        const { std::cell::Cell::new(None) };
}

fn lock_records(dir: &Path) -> Option<std::fs::File> {
    std::fs::create_dir_all(dir).ok()?;
    let file = std::fs::OpenOptions::new()
        .create(true)
        .truncate(false)
        .write(true)
        .open(dir.join(RECORD_LOCK_FILE))
        .ok()?;
    // Bounded by time slept rather than a clock reading: crates/core may not
    // read `Instant::now()` outside a TimeSource (DST rule lint), and this
    // bound needs no precision.
    const POLL: std::time::Duration = std::time::Duration::from_millis(10);
    let mut slept = std::time::Duration::ZERO;
    loop {
        match try_lock_file(&file) {
            Ok(true) => return Some(file),
            Ok(false) if slept < RECORD_LOCK_WAIT => {
                #[cfg(test)]
                ON_BUSY_RECORD_LOCK.with(|hook| {
                    if let Some(hook) = hook.borrow_mut().as_mut() {
                        hook();
                    }
                });
                std::thread::sleep(POLL);
                slept += POLL;
            }
            Ok(false) => {
                tracing::warn!("Update records stayed locked by another process; writing anyway");
                return None;
            }
            Err(e) => {
                tracing::warn!(error = %e, "Cannot lock the update records; writing anyway");
                return None;
            }
        }
    }
}

/// Make sure `version` is held WITHOUT counting a failure, for updater runs
/// that gave up before attempting a download (a cached GitHub cooldown, an
/// empty install bucket, a rate limit, a failed tag probe). The supervisor runs
/// `freenet update` after every non-clean exit, crashes included, so counting
/// these would walk a crash-looping node to the fallback without a single
/// download. A live record keeps its retry time and count and takes the new
/// reason, and stops being provisional; an expired one gets a fresh wait at
/// its count (zero once its failures no longer count), keeping its last real
/// error; with none, or one for another
/// release, a hold at zero failures is written for `version`, so the release
/// held is always the one on record.
pub(crate) fn hold_update_without_counting_at(
    dir: &Path,
    version: &str,
    reason: &str,
    now: SystemTime,
    rand_unit: f64,
) {
    with_record_lock(dir, || {
        let (failures, last_error) = match read_update_cooldown_at(dir) {
            Some(prev) if prev.version == version => {
                if update_cooldown_remaining_at(dir, version, now).is_some() {
                    // Live: keep its retry time and count, but say why it
                    // is held now (the reason a run's first hold gave is
                    // replaced by the one its outcome gives). A hold is a
                    // report on the attempt, so a provisional record is
                    // provisional no longer: the next failure is a new
                    // attempt and counts.
                    store_update_cooldown_at(
                        dir,
                        &UpdateCooldownState {
                            provisional: false,
                            hold_reason: Some(
                                reason
                                    .chars()
                                    .take(UPDATE_COOLDOWN_ERROR_MAX_CHARS)
                                    .collect(),
                            ),
                            ..prev
                        },
                    );
                    return;
                }
                (live_failures(&prev, now), prev.last_error)
            }
            _ => (0, String::new()),
        };
        write_update_cooldown_at(
            dir,
            version,
            failures,
            (&last_error, Some(reason)),
            (now, rand_unit),
            false,
        );
    });
}

/// Hold `version` without counting a failure until `until` (when the node's
/// download budget is whole again), though never more than the longest
/// cooldown from `now`: a retry time beyond that reads as a bogus record. A
/// trigger that wakes before `until` finds the budget still spent and holds
/// again, without a request. A live record for the release keeps the later
/// retry time; its count and last real error are kept.
pub(crate) fn hold_update_until_at(
    dir: &Path,
    version: &str,
    reason: &str,
    until: SystemTime,
    now: SystemTime,
) {
    with_record_lock(dir, || {
        let latest = now.checked_add(UPDATE_COOLDOWN_MAX).unwrap_or(now);
        let mut retry_at_unix = unix_secs(until.clamp(now, latest));
        let (failures, last_error) = match read_update_cooldown_at(dir) {
            Some(prev) if prev.version == version => {
                if update_cooldown_remaining_at(dir, version, now).is_some() {
                    retry_at_unix = retry_at_unix.max(prev.retry_at_unix);
                }
                (live_failures(&prev, now), prev.last_error)
            }
            _ => (0, String::new()),
        };
        store_update_cooldown_at(
            dir,
            &UpdateCooldownState {
                version: version.to_string(),
                failures,
                failed_at_unix: unix_secs(now),
                retry_at_unix,
                last_error,
                hold_reason: Some(
                    reason
                        .chars()
                        .take(UPDATE_COOLDOWN_ERROR_MAX_CHARS)
                        .collect(),
                ),
                provisional: false,
            },
        );
    });
}

/// [`hold_update_without_counting_at`] against the shared state directory.
pub fn hold_update_without_counting(version: &str, error: &str) {
    let rand_unit = freenet::config::GlobalRng::random_range(0.0_f64..1.0);
    if let Some(dir) = state_dir() {
        hold_update_without_counting_at(&dir, version, error, SystemTime::now(), rand_unit);
    }
}

/// How long until `version` may be tried again, or `None` when it is not
/// cooling down. A retry time further out than the longest possible wait is a
/// bogus record (a clock that jumped, a hand edit), and is ignored.
pub(crate) fn update_cooldown_remaining_at(
    dir: &Path,
    version: &str,
    now: SystemTime,
) -> Option<std::time::Duration> {
    let state = read_update_cooldown_at(dir).filter(|s| s.version == version)?;
    let retry_at = UNIX_EPOCH.checked_add(std::time::Duration::from_secs(state.retry_at_unix))?;
    let remaining = retry_at.duration_since(now).ok()?;
    let longest = UPDATE_COOLDOWN_MAX.mul_f64(1.0 + UPDATE_COOLDOWN_JITTER);
    (!remaining.is_zero() && remaining <= longest).then_some(remaining)
}

/// The release cooling down, and for how long, if it is newer than
/// `current_version`. This is what the update triggers check before they ask
/// GitHub anything: they would find that same release. A release this node
/// has blocked (#4073: pinned known-bad, or install-gated) holds nothing:
/// no trigger acts on it, and a fixed release must be found promptly.
pub(crate) fn update_cooldown_pending_at(
    dir: &Path,
    current_version: &str,
    now: SystemTime,
) -> Option<(String, std::time::Duration)> {
    let state = read_update_cooldown_at(dir)?;
    if is_not_newer(&state.version, current_version)
        || is_version_pinned_bad_at(dir, &state.version)
        || is_version_install_gated_at(dir, &state.version)
    {
        return None;
    }
    let remaining = update_cooldown_remaining_at(dir, &state.version, now)?;
    Some((state.version, remaining))
}

/// The release a failed tag lookup that named no version counts against when
/// nothing newer is on record (#5799). Not a version, so it counts as newer
/// than any (see [`is_not_newer`]): every trigger holds while it cools down.
/// Any real release's record replaces it.
pub(crate) const UNKNOWN_RELEASE: &str = "the latest release";

/// Remove the record if `stale` says its release can no longer be installed:
/// a tag lookup has just succeeded, so there is no unknown release
/// ([`UNKNOWN_RELEASE`]) left to wait for, and a release at or below the
/// current version, blocked by #4073, or newer than the latest (withdrawn) is
/// not worth waiting for either.
pub(crate) fn clear_stale_update_record_at(dir: &Path, stale: impl Fn(&str) -> bool) {
    with_record_lock(dir, || {
        if read_update_cooldown_at(dir).is_some_and(|state| stale(&state.version)) {
            let _rm = std::fs::remove_file(dir.join(UPDATE_COOLDOWN_FILE));
        }
    });
}

/// The release on record, cooling down or not, if it is newer than
/// `current_version` (the sentinel [`UNKNOWN_RELEASE`] included) and not
/// `blocked`.
pub(crate) fn recorded_newer_release_at(
    dir: &Path,
    current_version: &str,
    blocked: impl Fn(&str) -> bool,
) -> Option<String> {
    read_update_cooldown_at(dir)
        .filter(|state| !is_not_newer(&state.version, current_version) && !blocked(&state.version))
        .map(|state| state.version)
}

/// [`update_cooldown_pending_at`] against the shared state directory.
pub fn update_cooldown_pending(current_version: &str) -> Option<(String, std::time::Duration)> {
    state_dir().and_then(|dir| update_cooldown_pending_at(&dir, current_version, SystemTime::now()))
}

/// Whether `version` is not newer than `current` (semver). Unparseable
/// versions count as newer, so a record is never silently dropped.
fn is_not_newer(version: &str, current: &str) -> bool {
    semver::Version::parse(version)
        .ok()
        .zip(semver::Version::parse(current).ok())
        .is_some_and(|(version, current)| version <= current)
}

pub(crate) fn clear_update_cooldown_at(dir: &Path) {
    with_record_lock(dir, || {
        let _rm = std::fs::remove_file(dir.join(UPDATE_COOLDOWN_FILE));
    });
}

/// Clear the cooldown (after a successful install, or when already up to date).
pub fn clear_update_cooldown() {
    if let Some(dir) = state_dir() {
        clear_update_cooldown_at(&dir);
    }
}

/// One line for `freenet service report`, or `None` when nothing is pending.
pub(crate) fn update_cooldown_summary_at(
    dir: &Path,
    current_version: &str,
    now: SystemTime,
) -> Option<String> {
    let state = read_update_cooldown_at(dir)?;
    if is_not_newer(&state.version, current_version) {
        return None;
    }
    let wait = if is_version_install_gated_at(dir, &state.version)
        || is_version_pinned_bad_at(dir, &state.version)
    {
        "auto-update will not retry it: it is blocked by repeated install failures or a \
         crash-loop rollback (#4073); run `freenet update --force` to override"
            .to_string()
    } else {
        match update_cooldown_remaining_at(dir, &state.version, now) {
            Some(left) => format!("next attempt in {} min", left.as_secs().div_ceil(60)),
            None => "cooldown over, the next update check may try again".to_string(),
        }
    };
    let held = state
        .hold_reason
        .as_deref()
        .map(|reason| format!(" Then held without counting: {reason}"))
        .unwrap_or_default();
    Some(if state.failures == 0 {
        format!(
            "Update to {} is on hold, with no failed download yet ({wait}).{held}",
            state.version
        )
    } else {
        format!(
            "Update to {} failed {} time(s) in a row ({wait}). Last error: {}{held}",
            state.version, state.failures, state.last_error
        )
    })
}

/// [`update_cooldown_summary_at`] against the shared state directory.
pub fn update_cooldown_summary(current_version: &str) -> Option<String> {
    state_dir().and_then(|dir| update_cooldown_summary_at(&dir, current_version, SystemTime::now()))
}

// ── Hard ceiling on release downloads (#5799) ──────────────────────────────
//
// Independent of the cooldown above, so a bug in it cannot turn the fleet's
// downloads into a flood that GitHub throttles. Staging and the updater
// together receive at most `DOWNLOAD_BYTES_PER_DAY` bytes of release archives
// per node in a fixed window of `DOWNLOAD_WINDOW` from the first byte charged,
// whichever releases they are for: a per-release allowance would start afresh
// each time the target release changed. It counts bytes received, not runs:
// an attempt that fails early costs next to nothing, and staging resumes a
// partial download rather than fetching it again, so the normal schedule
// (runs at 0, 1 h and 3 h, then about every 4 h) stays far below it, however
// long an outage lasts. It binds only when downloads keep starting over: a
// server ignoring the resume, or connections dropping after most of an
// archive, including the updater's downloads, which always start from zero.
// Every staging run also spends one token from the node's GitHub poll bucket
// (#4073), which bounds the requests.
//
// Both reads and writes fail OPEN: an unreadable record counts as nothing
// spent, and a spend that cannot be written is lost. The poll bucket still
// bounds the requests, and a budget that failed closed could hold an update
// back indefinitely.
//
// Accounting caveats, all bounded:
// - Each download is charged its expected size before its body, then
//   reconciled to the bytes received. A killed run keeps its pre-charge, and
//   a resumed download is charged again for what it re-fetches, so the
//   ledger over-counts rather than under-counts.
// - A download of unknown size is charged at most `MAX_UNKNOWN_DOWNLOAD`.
// - A refund that lands after the window rolled over is taken from the new
//   window, under-counting it by at most one download.
// - Manual and `--force` updater runs are charged but never refused.
// - The record lock is cross-process on unix only; on Windows a concurrent
//   tray and node write can lose one adjustment.

/// Bytes of release archives staging and the updater together may receive
/// per node per window: room for the macOS app bundle's DMG, the largest
/// download, several times over, and for about eight of the ~41 MiB freenet
/// and fdev archive sets.
pub(crate) const DOWNLOAD_BYTES_PER_DAY: u64 = 320 * 1024 * 1024;
const DOWNLOAD_WINDOW: std::time::Duration = std::time::Duration::from_secs(24 * 3600);
const DOWNLOAD_BUDGET_FILE: &str = "download_budget.json";

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct DownloadBudget {
    window_start_unix: u64,
    bytes: u64,
}

/// The current window's record, if there is a readable one.
fn download_budget_at(dir: &Path, now_unix: u64) -> Option<DownloadBudget> {
    std::fs::read_to_string(dir.join(DOWNLOAD_BUDGET_FILE))
        .ok()
        .and_then(|raw| serde_json::from_str::<DownloadBudget>(&raw).ok())
        .filter(|d| {
            d.window_start_unix <= now_unix
                && now_unix - d.window_start_unix < DOWNLOAD_WINDOW.as_secs()
        })
}

/// Bytes of release archives charged in the current window. Zero when the
/// record is missing, unreadable, or from another window (including one dated
/// in the future, after a clock step).
pub(crate) fn download_bytes_at(dir: &Path, now: SystemTime) -> u64 {
    download_budget_at(dir, unix_secs(now)).map_or(0, |d| d.bytes)
}

/// When the current window ends and the allowance is whole again, if bytes
/// have been charged in it.
pub(crate) fn download_window_end_at(dir: &Path, now: SystemTime) -> Option<SystemTime> {
    download_budget_at(dir, unix_secs(now))
        .map(|d| UNIX_EPOCH + std::time::Duration::from_secs(d.window_start_unix) + DOWNLOAD_WINDOW)
}

/// Charge `bytes` to the current window, starting a window when there is
/// none. A failure to write is logged and otherwise ignored (fail open).
pub(crate) fn charge_download_bytes_at(dir: &Path, bytes: u64, now: SystemTime) {
    if bytes > 0 {
        with_record_lock(dir, || adjust_download_bytes_locked(dir, bytes, 0, now));
    }
}

/// Give back `bytes` charged earlier in the current window, never below
/// zero: a download that received less than was charged for it.
pub(crate) fn refund_download_bytes_at(dir: &Path, bytes: u64, now: SystemTime) {
    if bytes > 0 {
        with_record_lock(dir, || adjust_download_bytes_locked(dir, 0, bytes, now));
    }
}

fn adjust_download_bytes_locked(dir: &Path, add: u64, remove: u64, now: SystemTime) {
    let now_unix = unix_secs(now);
    let next = match download_budget_at(dir, now_unix) {
        Some(d) => DownloadBudget {
            bytes: d.bytes.saturating_add(add).saturating_sub(remove),
            ..d
        },
        // A refund with no window open has nothing to give back to.
        None if add == 0 => return,
        None => DownloadBudget {
            window_start_unix: now_unix,
            bytes: add,
        },
    };
    let written = serde_json::to_vec(&next)
        .context("serialize the download budget")
        .and_then(|raw| atomic_write_record(&dir.join(DOWNLOAD_BUDGET_FILE), &raw));
    if let Err(e) = written {
        tracing::warn!(error = %e, "Failed to record downloaded bytes (#5799)");
    }
}

// ── Post-stop crash classification / handling / rollback ───────────────────

/// Read the raw post-stop status string the supervisor forwarded, if any. May
/// be a numeric exit code or a signal name; classified by [`classify_stop`].
pub fn post_stop_status_from_env() -> Option<String> {
    post_stop_status_from(std::env::var(POST_STOP_EXIT_CODE_ENV_VAR).ok().as_deref())
}

/// [`post_stop_status_from_env`] over the variable's raw value: an empty or
/// blank status is no status, so it never reads as a crash (#4073).
pub(crate) fn post_stop_status_from(raw: Option<&str>) -> Option<String> {
    let trimmed = raw?.trim();
    (!trimmed.is_empty()).then(|| trimmed.to_string())
}

/// Whether the supervisor passed the node's stop status at all, even an empty
/// one: systemd forwards an empty `$EXIT_STATUS` after a start timeout or when
/// there was no main process. Presence, not content, marks a `freenet update`
/// the supervisor started (#5799); [`post_stop_status_from_env`] still reads
/// an empty status as no crash.
pub fn post_stop_status_present() -> bool {
    post_stop_status_present_in(std::env::var_os(POST_STOP_EXIT_CODE_ENV_VAR).as_deref())
}

pub(crate) fn post_stop_status_present_in(raw: Option<&std::ffi::OsStr>) -> bool {
    raw.is_some()
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
        //
        // A failed unlink is tolerable HERE, unlike in `commit_probation_at`:
        // nothing is announced, and the next stop re-reads the marker and takes
        // this same branch again, so the removal effectively retries. Reporting
        // it would need an output channel this process does not have — see #5244.
        let _retried_next_stop = remove_probation_at(dir);
        return PostStopOutcome::Proceed;
    }

    // GC discipline (S5): a marker older than the max age means the version has
    // been around far longer than any boot-wedge would survive. Treat as
    // committed/stale so a much-later transient crash can't trigger a rollback.
    if now_unix().saturating_sub(state.installed_at_unix) > PROBATION_MAX_AGE_SECS {
        // Same as above: not announced, and re-attempted on the next stop.
        let _retried_next_stop = remove_probation_at(dir);
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
        let _retried_next_stop = remove_probation_at(dir);
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
            let _retried_next_stop = remove_probation_at(dir);
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
            // If this fails the marker names the version we just replaced, so
            // the restored binary's next stop sees a version mismatch and takes
            // the stale branch above. Self-healing, hence not surfaced here.
            let _cleared_by_version_mismatch = remove_probation_at(dir);
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
pub(crate) mod tests {
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

    /// Both marker-clearing outcomes MUST announce themselves, and `Nothing`
    /// must stay silent.
    ///
    /// The end-to-end test `tests/post_update_probation_commit.rs` greps the
    /// node's stderr for a substring of the Committed line; these assertions are
    /// what make a reword of either line fail in microseconds instead of after
    /// that test's 60s window (and they are the only coverage the ClearedStale
    /// line has, since reaching it end-to-end needs a second 60s node boot).
    /// A commit announcement must never claim a disarm that did not happen.
    ///
    /// `remove_probation_at` can fail (read-only or full state dir — the same
    /// condition that breaks the rest of the update machinery, see #5244). The
    /// marker then stays on disk and stays inside its TTL, so a crash within the
    /// hour can still roll the node back. Announcing "rollback disarmed" there
    /// would reintroduce, at the moment of fixing it, exactly the misleading-
    /// journal problem this whole change exists to remove (#5232).
    #[test]
    fn a_failed_clear_is_never_announced_as_a_pass() {
        let line = commit_announcement(
            &CommitOutcome::CommitFailed {
                marker_version: "0.2.123".to_string(),
                error: "Permission denied (os error 13)".to_string(),
            },
            "0.2.123",
        )
        .expect("a marker that could not be cleared must be reported");

        assert!(
            line.contains("STILL ARMED"),
            "the operator must be told rollback is still live: {line}"
        );
        assert!(
            line.contains("Permission denied (os error 13)"),
            "the cause is what makes this actionable: {line}"
        );
        assert!(
            !line.contains("disarmed"),
            "MUST NOT claim a disarm that did not happen: {line}"
        );
        // `tests/post_update_probation_commit.rs` greps stderr for the success
        // line's needle to conclude the node committed. If a FAILED clear also
        // contained that substring, the end-to-end guard would pass while
        // rollback was still armed — the exact false-green this PR exists to
        // prevent. Assert the two are disjoint, in both directions.
        let committed = commit_announcement(&CommitOutcome::Committed, "0.2.123")
            .expect("a commit must be announced");
        let needle = "committed, auto-rollback disarmed";
        assert!(committed.contains(needle), "{committed}");
        assert!(
            !line.contains(needle),
            "a failed clear must not contain the committed-branch needle: {line}"
        );
    }

    /// The sibling of the test above, for the OTHER arm that cannot remove the
    /// marker — and the reason `ClearFailed` was split in two.
    ///
    /// Scenario: the node runs 0.2.134, the state dir is unwritable (disk-full
    /// recovery, wrong ownership, an immutable bind-mount), and a marker for
    /// 0.2.133 is left over from a failed-restore path. Under the single
    /// `ClearFailed` variant this reached the CommitFailed wording, and BOTH of
    /// its claims were false:
    ///
    ///  - "ran healthily for {window}s" describes a probation 0.2.134 was never
    ///    on. Nothing about the running version was under test.
    ///  - "auto-rollback is STILL ARMED and a crash within the next hour can
    ///    still roll this node back" is impossible.
    ///    `handle_post_stop_at` tests `state.new_version != current_version`
    ///    before it counts anything, drops the marker and returns `Proceed`, so
    ///    no crash is ever scored against a foreign marker — asserted below
    ///    against the real function rather than taken on trust, because the
    ///    wording is only correct for as long as that stays true.
    ///
    /// So the operator was told brick-safety equipment was live when it was
    /// not, and was NOT told the condition that actually needs attention. That
    /// is the #5232 misleading-journal failure reproduced inside the branch
    /// added to fix it.
    #[test]
    fn a_failed_stale_clear_claims_neither_a_pass_nor_an_armed_rollback() {
        let line = commit_announcement(
            &CommitOutcome::StaleClearFailed {
                marker_version: "0.2.133".to_string(),
                error: "Read-only file system (os error 30)".to_string(),
            },
            "0.2.134",
        )
        .expect("a marker that could not be cleared must be reported");

        assert!(
            !line.contains("STILL ARMED"),
            "a marker for ANOTHER version cannot roll this node back, so claiming rollback is \
             armed tells the operator brick-safety equipment is live when it is not: {line}"
        );
        assert!(
            !line.contains("ran healthily for"),
            "the running version was never on probation, so it has not passed anything and this \
             must not describe a test that was never performed: {line}"
        );
        assert!(
            !line.contains("disarmed"),
            "nothing was disarmed — the marker is still on disk: {line}"
        );
        assert!(
            !line.contains("committed, auto-rollback disarmed"),
            "must not carry the needle the e2e test reads as a successful commit: {line}"
        );
        // What the operator CAN act on: which marker is stuck, why, and that
        // the state directory is unwritable.
        assert!(line.contains("0.2.133"), "{line}");
        assert!(line.contains("0.2.134"), "{line}");
        assert!(
            line.contains("Read-only file system (os error 30)"),
            "the cause is what makes this actionable: {line}"
        );
        assert!(
            line.contains("state directory"),
            "the actionable condition is the unwritable state dir, not the marker: {line}"
        );

        // The premise the wording rests on, checked against the real code path
        // rather than asserted in prose: a crash of a version the marker does
        // NOT name is never counted, so rollback genuinely is not armed.
        let dir = tempfile::tempdir().unwrap();
        let live = dir.path().join("freenet");
        write_dummy_binary(&live, b"NEWER");
        let meta = KnownGoodMeta {
            size: 5,
            sha256: "x".repeat(64),
        };
        begin_probation_at(dir.path(), "0.2.133", "0.2.132", &live, &meta).unwrap();
        assert_eq!(
            handle_post_stop_at(dir.path(), "101", "0.2.134"),
            PostStopOutcome::Proceed,
            "a crash of a version the marker does not name must not be counted; if this ever \
             changes, the StaleClearFailed wording above becomes the lie it was written to remove"
        );
        assert_eq!(
            read_probation_at(dir.path()).map(|s| s.crash_count),
            None,
            "the foreign marker is dropped, not incremented"
        );
    }

    /// A stderr that fails every write must NOT panic the announcement.
    ///
    /// `commit_probation` runs on a tokio worker from `bin/freenet.rs`'s commit
    /// timer, and the panicking form this replaced (`eprintln!`) would unwind it
    /// on EPIPE — which happens once a supervisor's reader is gone, and on a
    /// Windows child that inherited invalid standard handles. With
    /// `panic = "abort"` (commented out one line away in `crates/core/
    /// Cargo.toml`) that unwind becomes a process abort at t=60s: a non-zero
    /// exit, scored by `handle_post_stop_at` as a probation crash, three of
    /// which roll the node back. The line announcing a successful commit would
    /// have caused the rollback it reports disarming.
    ///
    /// The point of `announce` taking `impl Write` is that this is testable at
    /// all: a test cannot break the real `std::io::stderr()`.
    #[test]
    fn a_broken_stderr_does_not_panic_the_announcement() {
        /// Fails every call, the way a pipe whose reader has gone does.
        struct BrokenPipe {
            writes: usize,
        }
        impl Write for BrokenPipe {
            fn write(&mut self, _buf: &[u8]) -> std::io::Result<usize> {
                self.writes += 1;
                Err(std::io::Error::from(std::io::ErrorKind::BrokenPipe))
            }
            fn flush(&mut self) -> std::io::Result<()> {
                Err(std::io::Error::from(std::io::ErrorKind::BrokenPipe))
            }
        }

        let mut out = BrokenPipe { writes: 0 };
        // Every announced outcome, so a future branch that reaches for a
        // panicking macro of its own is covered too.
        for outcome in [
            CommitOutcome::Committed,
            CommitOutcome::ClearedStale {
                marker_version: "0.2.122".to_string(),
            },
            CommitOutcome::CommitFailed {
                marker_version: "0.2.123".to_string(),
                error: "Permission denied (os error 13)".to_string(),
            },
            CommitOutcome::StaleClearFailed {
                marker_version: "0.2.122".to_string(),
                error: "Read-only file system (os error 30)".to_string(),
            },
        ] {
            let line = commit_announcement(&outcome, "0.2.123").expect("announced");
            announce(&mut out, &line);
        }

        // Premise check: the writer really was exercised, so a future refactor
        // that stops calling it cannot leave this passing vacuously.
        assert!(
            out.writes >= 4,
            "the failing writer was not actually written to, so this test proved nothing about \
             panicking on a write error; got {} write(s)",
            out.writes
        );
    }

    /// The commit must not do its blocking work on a tokio worker.
    ///
    /// `commit_probation` does synchronous filesystem I/O and then takes the
    /// process-global stderr lock — which under the shipped systemd unit is a
    /// journald socket, so a stopped or backed-up journald parks whatever thread
    /// holds it, and every later `eprintln!` in the process queues behind it.
    /// Parking a runtime worker there is a different failure from the panic
    /// above and is not fixed by it, so it gets its own guard.
    ///
    /// A source scrape because there is nothing else to assert against: the call
    /// site is one `GlobalExecutor::spawn` in another file's `main` path, with no
    /// seam a test can reach. Scoped to that binding's block and loud on a moved
    /// anchor (per `.claude/rules/bug-prevention-patterns.md`) rather than
    /// widening to the rest of a 1000-line file, and it scrapes a DIFFERENT file
    /// from the one it lives in, so it cannot be satisfied by its own assertion
    /// strings.
    #[test]
    fn the_commit_runs_off_the_async_worker() {
        const FREENET_MAIN: &str = include_str!("../freenet.rs");
        const ANCHOR: &str = "let commit_probation_task = {";

        assert_eq!(
            FREENET_MAIN.matches(ANCHOR).count(),
            1,
            "`{ANCHOR}` no longer appears exactly once in bin/freenet.rs, so this pin cannot \
             locate the commit timer. Re-anchor it rather than deleting it."
        );
        let after = FREENET_MAIN
            .split_once(ANCHOR)
            .expect("anchor counted above")
            .1;
        let block = after
            .split_once("\n    };")
            .unwrap_or_else(|| {
                panic!("could not find the end of the `{ANCHOR}` block; re-anchor this pin")
            })
            .0;

        let spawn_blocking = block.find("spawn_blocking(").unwrap_or_else(|| {
            panic!(
                "the probation commit no longer goes through `spawn_blocking`, so its \
                 synchronous file I/O and its blocking stderr write run on a tokio worker — a \
                 stalled journald then parks that worker and every later `eprintln!` behind it. \
                 Block was:\n{block}"
            )
        });
        let commit = block.find("commit_probation(").unwrap_or_else(|| {
            panic!("the commit timer no longer calls `commit_probation`. Block was:\n{block}")
        });
        assert!(
            spawn_blocking < commit,
            "`commit_probation` must be called INSIDE `spawn_blocking`, not before it. \
             Block was:\n{block}"
        );
    }

    /// "Already absent" is the post-condition we want, so it is success — not a
    /// spurious CommitFailed warning on every idempotent re-commit.
    #[test]
    fn removing_an_absent_marker_succeeds() {
        let dir = tempfile::tempdir().unwrap();
        assert!(
            remove_probation_at(dir.path()).is_ok(),
            "a missing marker means the post-condition already holds"
        );
        assert_eq!(
            commit_probation_at(dir.path(), "0.2.123"),
            CommitOutcome::Nothing
        );
    }

    /// End-to-end through `commit_probation_at`: an unremovable marker yields a
    /// failure outcome rather than a false Committed — and the arm it came from
    /// decides WHICH failure outcome, since the two are announced differently.
    ///
    /// Both arms are driven against the same locked directory, so a regression
    /// that collapses them back into one variant fails here rather than only in
    /// the wording tests.
    ///
    /// Unlink permission lives on the DIRECTORY, so the marker is made
    /// unremovable by dropping write permission on its parent. Root bypasses
    /// that check entirely, so the test skips rather than failing when it would
    /// be testing nothing (containers commonly run tests as root).
    #[test]
    #[cfg(unix)]
    fn an_unremovable_marker_reports_clear_failed_not_committed() {
        use std::os::unix::fs::PermissionsExt;

        // SAFETY: `geteuid` reads the calling process's effective uid. It takes
        // no arguments, touches no memory, and cannot fail.
        if unsafe { libc::geteuid() } == 0 {
            eprintln!("skipping: running as root, which bypasses directory write permissions");
            return;
        }

        let dir = tempfile::tempdir().unwrap();
        let live = dir.path().join("freenet");
        write_dummy_binary(&live, b"NEWER");
        let meta = KnownGoodMeta {
            size: 5,
            sha256: "x".repeat(64),
        };
        begin_probation_at(dir.path(), "0.2.123", "0.2.122", &live, &meta).unwrap();

        let mut perms = std::fs::metadata(dir.path()).unwrap().permissions();
        let original = perms.mode();
        perms.set_mode(0o555);
        std::fs::set_permissions(dir.path(), perms).unwrap();

        let outcome = commit_probation_at(dir.path(), "0.2.123");
        // Same locked directory, same unremovable marker, but now the marker
        // names a version we are NOT running — the stale arm.
        let stale_outcome = commit_probation_at(dir.path(), "0.2.124");

        // Restore before asserting so a failure cannot leave an undeletable
        // tempdir behind.
        let mut restore = std::fs::metadata(dir.path()).unwrap().permissions();
        restore.set_mode(original);
        std::fs::set_permissions(dir.path(), restore).unwrap();

        match stale_outcome {
            CommitOutcome::StaleClearFailed { marker_version, .. } => {
                assert_eq!(marker_version, "0.2.123");
            }
            other @ (CommitOutcome::Committed
            | CommitOutcome::ClearedStale { .. }
            | CommitOutcome::CommitFailed { .. }
            | CommitOutcome::Nothing) => panic!(
                "an unremovable marker for ANOTHER version must report as StaleClearFailed: \
                 reporting it as CommitFailed announces a probation this version never sat and \
                 an armed rollback that cannot fire. Got: {other:?}"
            ),
        }

        match outcome {
            CommitOutcome::CommitFailed { marker_version, .. } => {
                assert_eq!(marker_version, "0.2.123");
            }
            // Enumerated, not `other =>`: `wildcard_enum_match_arm` exists so a
            // new `CommitOutcome` variant breaks every match site rather than
            // silently falling through here — and this test's own PR is what
            // added the failure variants, so a wildcard would have swallowed
            // exactly the kind of variant it is meant to catch. It earned that
            // keep immediately: splitting `ClearFailed` into `CommitFailed` and
            // `StaleClearFailed` broke this arm, which is the point.
            other @ (CommitOutcome::Committed
            | CommitOutcome::ClearedStale { .. }
            | CommitOutcome::StaleClearFailed { .. }
            | CommitOutcome::Nothing) => panic!(
                "an unremovable marker must NOT report as committed — rollback is still armed. \
                 Got: {other:?}"
            ),
        }
        assert!(
            read_probation_at(dir.path()).is_some(),
            "premise check: the marker really did survive"
        );
    }

    #[test]
    fn both_marker_clearing_outcomes_are_announced() {
        let committed = commit_announcement(&CommitOutcome::Committed, "0.2.123")
            .expect("a commit must be announced");
        assert!(
            committed.contains("post-update probation passed"),
            "the e2e test greps for this substring; keep them in step: {committed}"
        );
        assert!(committed.contains("0.2.123"), "{committed}");

        let stale = commit_announcement(
            &CommitOutcome::ClearedStale {
                marker_version: "0.2.122".to_string(),
            },
            "0.2.123",
        )
        .expect("discarding a marker disarms rollback and must be announced");
        assert!(
            stale.contains("discarded a post-update probation marker"),
            "{stale}"
        );
        // Both versions have to appear: which marker was dropped is the whole
        // diagnostic value, and the running version is what distinguishes this
        // from a healthy commit.
        assert!(stale.contains("0.2.122"), "{stale}");
        assert!(stale.contains("0.2.123"), "{stale}");
        assert!(
            !stale.contains("post-update probation passed"),
            "a discarded marker must NOT read as a passed probation: {stale}"
        );

        assert_eq!(
            commit_announcement(&CommitOutcome::Nothing, "0.2.123"),
            None,
            "no marker means no decision was made, so there is nothing to report"
        );
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
        assert_eq!(temp_files(dir), Vec::<String>::new());
    }

    fn temp_files(dir: &Path) -> Vec<String> {
        std::fs::read_dir(dir)
            .unwrap()
            .map(|e| e.unwrap().file_name().to_string_lossy().into_owned())
            .filter(|name| name.ends_with(".tmp"))
            .collect()
    }

    #[test]
    fn a_failed_atomic_write_leaves_no_temp_file() {
        let tmp = tempfile::tempdir().unwrap();
        let dir = tmp.path();
        let failed = atomic_write_record_with(&dir.join(UPDATE_COOLDOWN_FILE), b"x", |_, _| {
            Err(std::io::Error::other("disk full"))
        });
        assert!(failed.is_err());
        assert_eq!(temp_files(dir), Vec::<String>::new());
        // A failed rename (onto a non-empty directory) too.
        let target = dir.join("occupied");
        std::fs::create_dir(&target).unwrap();
        std::fs::write(target.join("x"), b"x").unwrap();
        assert!(atomic_write_record(&target, b"x").is_err());
        assert_eq!(temp_files(dir), Vec::<String>::new());
        assert!(atomic_write(&target, b"x").is_err());
        assert_eq!(temp_files(dir), Vec::<String>::new());
    }

    /// Concurrent writers (the node, the updater, a tray "Check for Updates")
    /// never rename each other's half-written bytes into place.
    #[test]
    fn concurrent_cooldown_writes_leave_a_valid_record() {
        let tmp = tempfile::tempdir().unwrap();
        let dir = tmp.path().to_path_buf();
        let now = SystemTime::now();
        let writers: Vec<_> = (0..8)
            .map(|i| {
                let dir = dir.clone();
                std::thread::spawn(move || {
                    // Few rounds: each write fsyncs under the process-wide
                    // mutex, which other tests' writers queue behind.
                    for _ in 0..10 {
                        record_update_failure_at(&dir, "0.2.141", &"e".repeat(i * 40), now, 0.5)
                            .expect("write");
                    }
                })
            })
            .collect();
        for w in writers {
            w.join().unwrap();
        }
        let record = read_update_cooldown_at(&dir).unwrap();
        assert_eq!(record.version, "0.2.141");
        // Every increment survived: the read-modify-write is locked (#5799).
        assert_eq!(record.failures, 8 * 10);
        assert_eq!(temp_files(&dir), Vec::<String>::new());
    }

    /// Turns [`lock_helper_subprocess`] from a no-op test into a helper
    /// process: `probe:<path>` or `hold:<ms>:<path>` (`ms` empty: until
    /// stdin closes).
    const LOCK_HELPER_ENV: &str = "FREENET_TEST_LOCK_HELPER";

    /// Not a test of its own: a no-op unless [`LockHelper`] re-executes this
    /// binary filtered to it, with [`LOCK_HELPER_ENV`] set. A re-executed
    /// helper, unlike a fork, inherits none of this process's descriptors
    /// (all are close-on-exec), so it can never keep another test's lock
    /// alive the way a forked child did.
    #[test]
    #[cfg(unix)]
    fn lock_helper_subprocess() {
        use std::io::{Read, Write};
        let Ok(spec) = std::env::var(LOCK_HELPER_ENV) else {
            return;
        };
        let say = |line: &str| {
            let mut out = std::io::stdout();
            #[allow(clippy::let_underscore_must_use)]
            let _ = writeln!(out, "{line}").and_then(|()| out.flush());
        };
        let open = |path: &str| {
            std::fs::OpenOptions::new()
                .create(true)
                .truncate(false)
                .write(true)
                .open(path)
                .expect("open the lock file")
        };
        if let Some(path) = spec.strip_prefix("probe:") {
            let got = try_lock_file(&open(path)).unwrap_or(false);
            say(if got { "LOCK=1" } else { "LOCK=0" });
            std::process::exit(0);
        }
        let (ms, path) = spec
            .strip_prefix("hold:")
            .and_then(|rest| rest.split_once(':'))
            .expect("hold:<ms>:<path>");
        let file = open(path);
        if !try_lock_file(&file).unwrap_or(false) {
            say("LOCK=0");
            std::process::exit(0);
        }
        say("LOCKED");
        match ms.parse::<u64>() {
            Ok(ms) => std::thread::sleep(std::time::Duration::from_millis(ms)),
            Err(_) => {
                let mut sink = Vec::new();
                #[allow(clippy::let_underscore_must_use)]
                let _ = std::io::stdin().read_to_end(&mut sink);
            }
        }
        // Written BEFORE the lock is released (at exit), so a writer that
        // finds it once it holds the lock waited for this holder.
        say("RELEASING");
        std::process::exit(0);
    }

    /// This test binary re-executed as [`lock_helper_subprocess`].
    #[cfg(unix)]
    pub(crate) struct LockHelper {
        child: std::process::Child,
        out: std::process::ChildStdout,
        seen: String,
    }

    #[cfg(unix)]
    impl LockHelper {
        pub(crate) fn spawn(spec: &str) -> Self {
            let mut child = std::process::Command::new(std::env::current_exe().unwrap())
                .args([
                    "commands::rollback::tests::lock_helper_subprocess",
                    "--exact",
                    "--nocapture",
                    "--test-threads",
                    "1",
                ])
                .env(LOCK_HELPER_ENV, spec)
                .stdin(std::process::Stdio::piped())
                .stdout(std::process::Stdio::piped())
                .stderr(std::process::Stdio::null())
                .spawn()
                .expect("re-execute the test binary");
            let out = child.stdout.take().unwrap();
            Self {
                child,
                out,
                seen: String::new(),
            }
        }

        /// Read until `marker` appears; `false` at end of output first.
        pub(crate) fn wait_for(&mut self, marker: &str) -> bool {
            use std::io::Read;
            let mut buf = [0u8; 256];
            while !self.seen.contains(marker) {
                match self.out.read(&mut buf) {
                    Ok(0) | Err(_) => return false,
                    Ok(n) => self.seen.push_str(&String::from_utf8_lossy(&buf[..n])),
                }
            }
            true
        }

        /// Hold `path`'s lock for `hold_ms` (until [`Self::release`] when
        /// `None`); returns once the helper holds it.
        pub(crate) fn hold(path: &Path, hold_ms: Option<u64>) -> Self {
            let ms = hold_ms.map_or_else(String::new, |ms| ms.to_string());
            let mut helper = Self::spawn(&format!("hold:{ms}:{}", path.display()));
            assert!(
                helper.wait_for("LOCKED"),
                "the helper did not take the lock: {}",
                helper.seen
            );
            helper
        }

        /// Whether the helper has said it is releasing, without blocking.
        pub(crate) fn released(&mut self) -> bool {
            use std::io::Read;
            use std::os::unix::io::AsRawFd;
            let fd = self.out.as_raw_fd();
            // SAFETY: flag changes on a descriptor this struct owns.
            unsafe {
                let flags = libc::fcntl(fd, libc::F_GETFL);
                libc::fcntl(fd, libc::F_SETFL, flags | libc::O_NONBLOCK);
            }
            let mut buf = [0u8; 256];
            while let Ok(n) = self.out.read(&mut buf) {
                if n == 0 {
                    break;
                }
                self.seen.push_str(&String::from_utf8_lossy(&buf[..n]));
            }
            self.seen.contains("RELEASING")
        }

        /// Let a helper started with no hold time go, and wait for it.
        pub(crate) fn release(mut self) {
            drop(self.child.stdin.take());
            #[allow(clippy::let_underscore_must_use)]
            let _ = self.child.wait();
        }
    }

    #[cfg(unix)]
    impl Drop for LockHelper {
        fn drop(&mut self) {
            #[allow(clippy::let_underscore_must_use)]
            let _ = self.child.kill();
            #[allow(clippy::let_underscore_must_use)]
            let _ = self.child.wait();
        }
    }

    /// Whether another process can take the lock on `path` right now.
    #[cfg(unix)]
    pub(crate) fn another_process_can_lock_file(path: &Path) -> bool {
        let mut helper = LockHelper::spawn(&format!("probe:{}", path.display()));
        assert!(
            helper.wait_for("LOCK="),
            "the helper exited without reporting"
        );
        helper.seen.contains("LOCK=1")
    }

    /// Whether another process can take the record lock in `dir` right now.
    #[cfg(unix)]
    fn another_process_can_lock(dir: &Path) -> bool {
        another_process_can_lock_file(&dir.join(RECORD_LOCK_FILE))
    }

    /// #5799: while a record is being rewritten, another
    /// process (the updater, a tray "Check for Updates") cannot take the
    /// record lock, so it waits instead of losing the increment. Checked from
    /// another process at that moment, not by timing, which contention from
    /// other tests in this process could satisfy by itself.
    #[test]
    #[cfg(unix)]
    fn the_record_lock_excludes_another_process() {
        let tmp = tempfile::tempdir().unwrap();
        let dir = tmp.path();
        let mut probes = Vec::new();
        with_record_lock(dir, || probes.push(another_process_can_lock(dir)));
        probes.push(another_process_can_lock(dir));
        assert_eq!(probes, [false, true], "locked while held, free after");
        // The wait for the lock blocks its thread, and staging writes from
        // async code, so it stays short.
        assert!(RECORD_LOCK_WAIT <= std::time::Duration::from_millis(100));
    }

    /// Concurrent byte counts all land: the read-modify-write of the budget
    /// record is locked like the cooldown's.
    #[test]
    fn concurrent_byte_counts_all_land() {
        let tmp = tempfile::tempdir().unwrap();
        let dir = tmp.path().to_path_buf();
        let now = SystemTime::now();
        let writers: Vec<_> = (0..8)
            .map(|_| {
                let dir = dir.clone();
                std::thread::spawn(move || {
                    for _ in 0..10 {
                        charge_download_bytes_at(&dir, 1, now);
                    }
                })
            })
            .collect();
        for w in writers {
            w.join().unwrap();
        }
        assert_eq!(download_bytes_at(&dir, now), 80);
    }

    /// Concurrent record writes, each through its own temporary, never
    /// publish a torn file and leave no temporary behind. (The rollback
    /// files' `atomic_write` keeps its fixed temporary name; their writers
    /// never run at once.)
    #[test]
    fn concurrent_record_writes_never_publish_a_torn_file() {
        let tmp = tempfile::tempdir().unwrap();
        let path = tmp.path().join(UPDATE_COOLDOWN_FILE);
        let writers: Vec<_> = (0..8u8)
            .map(|i| {
                let path = path.clone();
                std::thread::spawn(move || {
                    let body = vec![b'a' + i; 64 * 1024];
                    for _ in 0..5 {
                        atomic_write_record(&path, &body).unwrap();
                    }
                })
            })
            .collect();
        for w in writers {
            w.join().unwrap();
        }
        let got = std::fs::read(&path).unwrap();
        assert_eq!(got.len(), 64 * 1024);
        assert!(got.iter().all(|b| *b == got[0]), "torn file");
        assert_eq!(temp_files(tmp.path()), Vec::<String>::new());
    }

    /// Every record writer waits for a lock another process holds. The holder
    /// keeps it until the writer has found it busy, then a hook on the
    /// writer's thread lets it go and waits for it to exit, so the writer
    /// must then get the lock. No timing is involved: under plain `cargo
    /// test` a writer can wait on the in-process mutex for any length of
    /// time first, and a writer that never took the lock never fires the
    /// hook.
    #[test]
    #[cfg(unix)]
    fn every_record_writer_waits_for_a_lock_another_process_holds() {
        let tmp = tempfile::tempdir().unwrap();
        let dir = tmp.path();
        let now = SystemTime::now();
        let writers: [(&str, &dyn Fn()); 5] = [
            ("failure", &|| {
                record_update_failure_at(dir, "0.2.141", "x", now, 0.5);
            }),
            ("hold", &|| {
                hold_update_without_counting_at(dir, "0.2.142", "held", now, 0.5)
            }),
            ("clear", &|| clear_update_cooldown_at(dir)),
            ("clear stale", &|| {
                clear_stale_update_record_at(dir, |_| true)
            }),
            ("bytes", &|| charge_download_bytes_at(dir, 1, now)),
        ];
        for (what, write) in writers {
            let holder = std::rc::Rc::new(std::cell::RefCell::new(Some(LockHelper::hold(
                &dir.join(RECORD_LOCK_FILE),
                None,
            ))));
            let fired = std::rc::Rc::new(std::cell::Cell::new(false));
            let (hook_holder, hook_fired) = (holder.clone(), fired.clone());
            ON_BUSY_RECORD_LOCK.with(|hook| {
                *hook.borrow_mut() = Some(Box::new(move || {
                    if let Some(holder) = hook_holder.borrow_mut().take() {
                        holder.release();
                        hook_fired.set(true);
                    }
                }))
            });
            LAST_RECORD_LOCK_TAKEN.with(|taken| taken.set(None));
            write();
            ON_BUSY_RECORD_LOCK.with(|hook| *hook.borrow_mut() = None);
            assert!(fired.get(), "the {what} writer found the lock busy");
            assert_eq!(
                LAST_RECORD_LOCK_TAKEN.with(|taken| taken.get()),
                Some(true),
                "the {what} writer got the lock once it was free"
            );
            drop(holder);
        }
    }

    /// A holder that never lets go does not block a writer for good: it
    /// gives up after RECORD_LOCK_WAIT's worth of polls (counted, not timed,
    /// so CPU starvation cannot fail it) and writes anyway (fail open) while
    /// the holder still holds the lock. The wall-clock bound is a generous
    /// backstop against a wait that never ends.
    #[test]
    #[cfg(unix)]
    fn a_wedged_lock_holder_does_not_block_a_writer() {
        let tmp = tempfile::tempdir().unwrap();
        let dir = tmp.path();
        // Held until this test kills it.
        let mut holder = LockHelper::hold(&dir.join(RECORD_LOCK_FILE), None);
        let polls = std::rc::Rc::new(std::cell::Cell::new(0u32));
        let counted = polls.clone();
        ON_BUSY_RECORD_LOCK.with(|hook| {
            *hook.borrow_mut() = Some(Box::new(move || counted.set(counted.get() + 1)))
        });
        let started = std::time::Instant::now();
        let gave_up = lock_records(dir).is_none();
        ON_BUSY_RECORD_LOCK.with(|hook| *hook.borrow_mut() = None);
        assert!(gave_up, "the lock stayed busy");
        assert_eq!(
            u128::from(polls.get()),
            RECORD_LOCK_WAIT.as_millis() / 10,
            "one poll per 10 ms of RECORD_LOCK_WAIT"
        );
        assert!(started.elapsed() < std::time::Duration::from_secs(30));
        let now = SystemTime::now();
        let written = record_update_failure_at(dir, "0.2.141", "x", now, 0.5);
        assert_eq!(written, Some(1));
        assert!(
            !holder.released(),
            "written while the holder still held the lock"
        );
    }

    /// The wrappers over the shared state directory act on it (here a
    /// per-thread temporary one, never the developer's real directory).
    #[test]
    fn the_state_dir_wrappers_act_on_the_state_dir() {
        let state = super::super::auto_update::TestStateDir::new();
        let dir = state.path();
        assert_eq!(update_cooldown_pending("0.2.140"), None);
        record_update_failure_at(dir, "0.2.141", "offline", SystemTime::now(), 0.5);
        assert_eq!(read_update_cooldown_at(dir).unwrap().last_error, "offline");
        let (version, _) = update_cooldown_pending("0.2.140").expect("pending");
        assert_eq!(version, "0.2.141");
        assert!(
            update_cooldown_summary("0.2.140")
                .unwrap()
                .contains("offline")
        );
        hold_update_without_counting("0.2.142", "held");
        assert_eq!(read_update_cooldown_at(dir).unwrap().version, "0.2.142");
        clear_update_cooldown();
        assert!(read_update_cooldown_at(dir).is_none());
        // The startup sweep removes a stale record temporary there.
        let stale = dir.join(format!(".{UPDATE_COOLDOWN_FILE}.1.2.tmp"));
        std::fs::write(&stale, b"x").unwrap();
        std::fs::File::options()
            .write(true)
            .open(&stale)
            .unwrap()
            .set_modified(SystemTime::now() - STALE_TEMP_AGE * 2)
            .unwrap();
        sweep_stale_atomic_temps();
        assert!(!stale.exists());
        // Never so short that a write in progress elsewhere could be hit.
        assert!(STALE_TEMP_AGE >= std::time::Duration::from_secs(3600));
    }

    /// One attempt is one failure: the updater's outcome for an attempt the
    /// node counted as it exited replaces that provisional record, while
    /// separate attempts still add up toward the fallback.
    #[test]
    fn the_updater_s_outcome_replaces_the_node_s_provisional_count() {
        let tmp = tempfile::tempdir().unwrap();
        let dir = tmp.path();
        let now = SystemTime::now();
        let failures = |dir: &Path| read_update_cooldown_at(dir).unwrap();
        assert_eq!(
            record_provisional_failure_at(dir, "0.2.141", "exited", now, 0.5),
            Some(1)
        );
        assert!(failures(dir).provisional);
        assert_eq!(
            record_update_failure_at(dir, "0.2.141", "updater failed", now, 0.5),
            Some(1),
            "the same attempt"
        );
        let record = failures(dir);
        assert!(!record.provisional);
        assert_eq!(record.last_error, "updater failed");
        // The next attempt counts.
        assert_eq!(
            record_provisional_failure_at(dir, "0.2.141", "exited", now, 0.5),
            Some(2)
        );
        assert_eq!(
            record_update_failure_at(dir, "0.2.141", "updater failed", now, 0.5),
            Some(2)
        );
        // A failure with no provisional record before it counts as usual.
        assert_eq!(
            record_update_failure_at(dir, "0.2.141", "again", now, 0.5),
            Some(3)
        );
        // A provisional record for another release is not this attempt.
        record_provisional_failure_at(dir, "0.2.142", "exited", now, 0.5);
        assert_eq!(
            record_update_failure_at(dir, "0.2.141", "updater failed", now, 0.5),
            Some(1)
        );
    }

    /// A hold is a report on the attempt the node exited for: it ends the
    /// record's provisional status, so the next failure is a new attempt and
    /// counts, whether the hold met a live record or an expired one.
    #[test]
    fn a_hold_ends_a_provisional_record() {
        let tmp = tempfile::tempdir().unwrap();
        let dir = tmp.path();
        let now = SystemTime::now();
        record_provisional_failure_at(dir, "0.2.141", "exited", now, 0.5);
        hold_update_without_counting_at(dir, "0.2.141", "rate limited", now, 0.5);
        let record = read_update_cooldown_at(dir).unwrap();
        assert!(!record.provisional, "a live hold ends it");
        assert_eq!(record.failures, 1);
        assert_eq!(
            record_update_failure_at(dir, "0.2.141", "updater failed", now, 0.5),
            Some(2),
            "the next failure counts"
        );

        let later = now + UPDATE_COOLDOWN_MAX * 2;
        record_provisional_failure_at(dir, "0.2.141", "exited", later, 0.5);
        let expired = later + UPDATE_COOLDOWN_MAX * 2;
        hold_update_without_counting_at(dir, "0.2.141", "rate limited", expired, 0.5);
        assert!(
            !read_update_cooldown_at(dir).unwrap().provisional,
            "an expired-record hold ends it"
        );
    }

    /// A cooldown for a release this node has blocked (#4073) holds no
    /// trigger, so a fixed release is found promptly.
    #[test]
    fn a_blocked_release_s_cooldown_holds_nothing() {
        let now = SystemTime::now();
        for block in ["pin", "gate"] {
            let tmp = tempfile::tempdir().unwrap();
            let dir = tmp.path();
            record_update_failure_at(dir, "0.2.141", "x", now, 0.5);
            assert!(update_cooldown_pending_at(dir, "0.2.140", now).is_some());
            match block {
                "pin" => pin_known_bad_at(dir, "0.2.141").unwrap(),
                _ => {
                    for _ in 0..INSTALL_FAILURE_GATE_THRESHOLD {
                        record_install_failure_at(dir, "0.2.141");
                    }
                }
            }
            assert_eq!(
                update_cooldown_pending_at(dir, "0.2.140", now),
                None,
                "{block}"
            );
            // Another release's block changes nothing.
            record_update_failure_at(dir, "0.2.142", "x", now, 0.5);
            assert!(
                update_cooldown_pending_at(dir, "0.2.140", now).is_some(),
                "{block}"
            );
        }
    }

    /// A supervisor that passes an empty stop status (systemd after a start
    /// timeout, or with no main process) still marks the run as one it
    /// started, while the empty status itself is no crash.
    #[test]
    fn an_empty_stop_status_is_supervised_but_not_a_crash() {
        use std::ffi::OsStr;
        assert!(post_stop_status_present_in(Some(OsStr::new(""))));
        assert!(post_stop_status_present_in(Some(OsStr::new("1"))));
        assert!(!post_stop_status_present_in(None));
        assert_eq!(post_stop_status_from(Some("")), None);
        assert_eq!(post_stop_status_from(Some("  ")), None);
        assert_eq!(post_stop_status_from(None), None);
        assert_eq!(post_stop_status_from(Some(" 9 ")).as_deref(), Some("9"));
    }

    /// Every failure writes a cooldown for the release that failed, whatever
    /// release was on record, starting its count afresh; the sentinel for an
    /// unknown release is replaced by any real one; and a hold writes for the
    /// release it holds.
    #[test]
    fn every_failure_cools_down_the_release_that_failed() {
        let tmp = tempfile::tempdir().unwrap();
        let dir = tmp.path();
        let now = SystemTime::now();
        let hour = std::time::Duration::from_secs(3600);
        for _ in 0..3 {
            record_update_failure_at(dir, "0.2.142", "newer", now - hour * 9, 0.5);
        }
        // The newer release is withdrawn; the older latest one fails.
        assert_eq!(
            record_update_failure_at(dir, "0.2.141", "older", now, 0.5),
            Some(1),
            "a fresh count for the release that failed"
        );
        let record = read_update_cooldown_at(dir).unwrap();
        assert_eq!(
            (record.version.as_str(), record.last_error.as_str()),
            ("0.2.141", "older")
        );
        assert!(update_cooldown_remaining_at(dir, "0.2.141", now).is_some());
        // Back to the newer one: it starts afresh too.
        assert_eq!(
            record_update_failure_at(dir, "0.2.142", "newer", now, 0.5),
            Some(1)
        );
        // A hold writes for the release it holds, at zero failures.
        hold_update_without_counting_at(dir, "0.2.141", "held", now, 0.5);
        let record = read_update_cooldown_at(dir).unwrap();
        assert_eq!((record.version.as_str(), record.failures), ("0.2.141", 0));
        assert!(update_cooldown_remaining_at(dir, "0.2.141", now).is_some());
        // The sentinel gives way to a real release.
        let tmp = tempfile::tempdir().unwrap();
        let dir = tmp.path();
        record_update_failure_at(dir, UNKNOWN_RELEASE, "offline", now, 0.5);
        assert_eq!(
            record_update_failure_at(dir, "0.2.141", "download", now, 0.5),
            Some(1)
        );
        assert_eq!(read_update_cooldown_at(dir).unwrap().version, "0.2.141");
    }

    /// Failures whose retry time is more than two days past no longer
    /// count, for a failure or a hold; two days or less past, they still do.
    #[test]
    fn failures_two_days_past_their_retry_time_stop_counting() {
        assert_eq!(
            UPDATE_FAILURES_EXPIRE_AFTER,
            std::time::Duration::from_secs(48 * 3600)
        );
        let hour = std::time::Duration::from_secs(3600);
        let now = UNIX_EPOCH + std::time::Duration::from_secs(unix_secs(SystemTime::now()));
        let tmp = tempfile::tempdir().unwrap();
        let dir = tmp.path();
        let mut record = UpdateCooldownState {
            version: "0.2.141".to_string(),
            failures: 2,
            failed_at_unix: 0,
            retry_at_unix: 0,
            last_error: "earlier".to_string(),
            hold_reason: None,
            provisional: false,
        };
        let write = |record: &UpdateCooldownState| {
            std::fs::write(
                dir.join(UPDATE_COOLDOWN_FILE),
                serde_json::to_vec(record).unwrap(),
            )
            .unwrap()
        };
        // Exactly two days past the retry time: still counted.
        record.retry_at_unix = unix_secs(now - hour * 48);
        write(&record);
        hold_update_without_counting_at(dir, "0.2.141", "held", now, 0.5);
        assert_eq!(read_update_cooldown_at(dir).unwrap().failures, 2);
        write(&record);
        assert_eq!(
            record_update_failure_at(dir, "0.2.141", "now", now, 0.5),
            Some(3)
        );
        // A second more: a hold keeps none, a failure starts at 1.
        record.retry_at_unix -= 1;
        write(&record);
        hold_update_without_counting_at(dir, "0.2.141", "held", now, 0.5);
        let held = read_update_cooldown_at(dir).unwrap();
        assert_eq!((held.failures, held.last_error.as_str()), (0, "earlier"));
        write(&record);
        assert_eq!(
            record_update_failure_at(dir, "0.2.141", "now", now, 0.5),
            Some(1)
        );
        // Between one and two days past still counts (a locked-out node
        // attempting about daily).
        record.retry_at_unix = unix_secs(now - hour * 30);
        write(&record);
        assert_eq!(
            record_update_failure_at(dir, "0.2.141", "now", now, 0.5),
            Some(3)
        );
    }

    /// Two writers for different releases, interleaved as the node and the
    /// updater can be when they see different latest releases: neither's
    /// failure is ever dropped. After every write the record is a cooldown in
    /// force for the release that writer failed. (The lock that keeps truly
    /// simultaneous writes whole is `concurrent_cooldown_writes_leave_a_valid_record`.)
    #[test]
    fn interleaved_writers_for_different_releases_never_lose_a_cooldown() {
        let tmp = tempfile::tempdir().unwrap();
        let dir = tmp.path();
        let now = SystemTime::now();
        for round in 0..10 {
            for version in ["0.2.142", "0.2.141"] {
                assert_eq!(
                    record_update_failure_at(dir, version, version, now, 0.5),
                    Some(1),
                    "round {round}"
                );
                let record = read_update_cooldown_at(dir).unwrap();
                assert_eq!(record.version, version, "round {round}");
                assert!(update_cooldown_remaining_at(dir, version, now).is_some());
            }
        }
    }

    #[test]
    fn the_cooldown_is_an_hour_then_two_then_four_with_jitter() {
        let hour = std::time::Duration::from_secs(3600);
        for (failures, hours) in [(0, 1), (1, 1), (2, 2), (3, 4), (4, 4), (u32::MAX, 4)] {
            assert_eq!(
                update_cooldown_delay(failures, 0.5),
                hour * hours,
                "{failures}"
            );
        }
        assert_eq!(update_cooldown_delay(1, 0.0), hour.mul_f64(0.8));
        assert_eq!(update_cooldown_delay(1, 1.0), hour.mul_f64(1.2));
        assert_eq!(
            update_cooldown_delay(3, 9.0),
            UPDATE_COOLDOWN_MAX.mul_f64(1.2)
        );
    }

    #[test]
    fn the_cooldown_holds_a_release_until_it_expires() {
        let tmp = tempfile::tempdir().unwrap();
        let dir = tmp.path();
        let now = SystemTime::now();
        let hour = std::time::Duration::from_secs(3600);
        assert!(update_cooldown_remaining_at(dir, "0.2.141", now).is_none());
        assert_eq!(
            record_update_failure_at(dir, "0.2.141", "reset", now, 0.5),
            Some(1)
        );
        let left = update_cooldown_remaining_at(dir, "0.2.141", now).unwrap();
        assert!(left <= hour && left > hour - std::time::Duration::from_secs(2));
        assert!(
            update_cooldown_remaining_at(dir, "0.2.142", now).is_none(),
            "per release"
        );
        assert!(
            update_cooldown_remaining_at(dir, "0.2.141", now + hour).is_none(),
            "expires"
        );
        assert_eq!(
            record_update_failure_at(dir, "0.2.141", "reset", now, 0.5),
            Some(2)
        );
        assert!(update_cooldown_remaining_at(dir, "0.2.141", now + hour).is_some());
        assert_eq!(
            update_cooldown_pending_at(dir, "0.2.140", now)
                .map(|(v, _)| v)
                .as_deref(),
            Some("0.2.141")
        );
        assert!(
            update_cooldown_pending_at(dir, "0.2.141", now).is_none(),
            "not newer"
        );
        assert_eq!(
            record_update_failure_at(dir, "0.2.142", "x", now, 0.5),
            Some(1),
            "afresh"
        );
        clear_update_cooldown_at(dir);
        assert!(read_update_cooldown_at(dir).is_none());
    }

    /// A cooldown that fails closed could hold an update back for good;
    /// failing open costs one attempt, the behaviour before it existed.
    #[test]
    fn a_damaged_or_implausible_record_is_no_cooldown() {
        let tmp = tempfile::tempdir().unwrap();
        let dir = tmp.path();
        let now = SystemTime::now();
        std::fs::write(dir.join(UPDATE_COOLDOWN_FILE), "{not json").unwrap();
        assert!(update_cooldown_remaining_at(dir, "0.2.141", now).is_none());
        assert!(update_cooldown_pending_at(dir, "0.2.140", now).is_none());
        assert_eq!(
            record_update_failure_at(dir, "0.2.141", "x", now, 0.5),
            Some(1)
        );
        let mut state = read_update_cooldown_at(dir).unwrap();
        state.retry_at_unix = unix_secs(now) + UPDATE_COOLDOWN_MAX.as_secs() * 2;
        std::fs::write(
            dir.join(UPDATE_COOLDOWN_FILE),
            serde_json::to_vec(&state).unwrap(),
        )
        .unwrap();
        assert!(
            update_cooldown_remaining_at(dir, "0.2.141", now).is_none(),
            "clock skew"
        );
    }

    #[test]
    #[cfg(unix)]
    fn an_unwritable_record_is_reported_so_the_caller_fails_open() {
        let tmp = tempfile::tempdir().unwrap();
        // A directory where the record goes: neither readable nor replaceable.
        std::fs::create_dir(tmp.path().join(UPDATE_COOLDOWN_FILE)).unwrap();
        let now = SystemTime::now();
        assert_eq!(
            record_update_failure_at(tmp.path(), "0.2.141", "x", now, 0.5),
            None
        );
    }

    /// Updater runs that made no download attempt hold the release without
    /// counting a failure.
    #[test]
    fn a_hold_without_counting_never_advances_the_count() {
        let tmp = tempfile::tempdir().unwrap();
        let dir = tmp.path();
        let now = SystemTime::now();
        let hour = std::time::Duration::from_secs(3600);
        hold_update_without_counting_at(dir, "0.2.141", "skipped", now, 0.5);
        let first = read_update_cooldown_at(dir).unwrap();
        // #5799: a hold on a fresh record counts nothing.
        assert_eq!((first.version.as_str(), first.failures), ("0.2.141", 0));
        assert_eq!(first.hold_reason.as_deref(), Some("skipped"));
        assert!(update_cooldown_remaining_at(dir, "0.2.141", now).is_some());
        assert_eq!(
            record_update_failure_at(dir, "0.2.141", "real", now, 0.5),
            Some(1),
            "the first real failure is the first counted"
        );
        record_update_failure_at(dir, "0.2.141", "real", now, 0.5);
        let live = read_update_cooldown_at(dir).unwrap();
        for _ in 0..10 {
            hold_update_without_counting_at(dir, "0.2.141", "skipped", now, 0.5);
        }
        assert_eq!(
            read_update_cooldown_at(dir).unwrap(),
            UpdateCooldownState {
                hold_reason: Some("skipped".to_string()),
                ..live.clone()
            },
            "a live record keeps its retry time, count and error, and takes the reason"
        );
        let later = now + hour * 3;
        hold_update_without_counting_at(dir, "0.2.141", "skipped", later, 0.5);
        let held = read_update_cooldown_at(dir).unwrap();
        assert_eq!(held.failures, live.failures);
        // #5799: the real error survives the hold, for the report.
        assert_eq!(held.last_error, "real");
        assert_eq!(held.hold_reason.as_deref(), Some("skipped"));
        assert!(update_cooldown_remaining_at(dir, "0.2.141", later).is_some());
        let summary = update_cooldown_summary_at(dir, "0.2.140", later).unwrap();
        assert!(
            summary.contains("Last error: real")
                && summary.contains("held without counting: skipped"),
            "{summary}"
        );
        // A real failure clears the hold reason.
        record_update_failure_at(dir, "0.2.141", "again", later, 0.5);
        assert_eq!(read_update_cooldown_at(dir).unwrap().hold_reason, None);
        // A hold for another release replaces the record with its own.
        record_update_failure_at(dir, "0.2.150", "newer", now, 0.5);
        hold_update_without_counting_at(dir, "0.2.141", "held", now, 0.5);
        let held = read_update_cooldown_at(dir).unwrap();
        assert_eq!((held.version.as_str(), held.failures), ("0.2.141", 0));
    }

    /// A hold with nothing failed yet says so rather than "failed 0 times".
    #[test]
    fn the_summary_of_a_hold_says_nothing_has_failed() {
        let tmp = tempfile::tempdir().unwrap();
        let dir = tmp.path();
        let now = SystemTime::now();
        hold_update_without_counting_at(dir, "0.2.141", "the install bucket is empty", now, 0.5);
        let summary = update_cooldown_summary_at(dir, "0.2.140", now).unwrap();
        assert!(
            summary.starts_with("Update to 0.2.141 is on hold, with no failed download yet")
                && summary.contains("the install bucket is empty")
                && !summary.contains("0 time(s)"),
            "{summary}"
        );
    }

    /// #5799: temporaries a killed write left behind are swept at
    /// startup once an hour old, and only `atomic_write`'s own.
    /// The startup sweep removes only stale temporaries of the update
    /// records; the crash-loop rollback files' temporaries, and everything
    /// else in the state directory, are left alone.
    #[test]
    fn stale_record_temporaries_are_swept_and_nothing_else() {
        let tmp = tempfile::tempdir().unwrap();
        let dir = tmp.path();
        let swept = [
            format!(".{UPDATE_COOLDOWN_FILE}.123.0.tmp"),
            format!(".{DOWNLOAD_BUDGET_FILE}.9.41.tmp"),
        ];
        let kept = [
            ".unrelated.json.123.0.tmp".to_string(),
            format!(".{UPDATE_COOLDOWN_FILE}.12x.0.tmp"),
            format!(".{UPDATE_COOLDOWN_FILE}.1.2.3.tmp"),
            format!(".{UPDATE_COOLDOWN_FILE}.tmp"),
            format!(".{INSTALL_FAILURES_FILE}.9.41.tmp"),
            format!(".{INSTALL_FAILURES_FILE}.tmp"),
            format!(".{KNOWN_BAD_FILE}.tmp"),
            format!(".{PROBATION_FILE}.tmp"),
            UPDATE_COOLDOWN_FILE.to_string(),
        ];
        for name in swept.iter().chain(&kept) {
            std::fs::write(dir.join(name), b"x").unwrap();
        }
        let now = SystemTime::now();
        assert_eq!(sweep_stale_atomic_temps_at(dir, now), 0, "too recent");
        assert_eq!(
            sweep_stale_atomic_temps_at(
                dir,
                now + STALE_TEMP_AGE + std::time::Duration::from_secs(5)
            ),
            swept.len()
        );
        let mut left: Vec<String> = std::fs::read_dir(dir)
            .unwrap()
            .map(|e| e.unwrap().file_name().into_string().unwrap())
            .collect();
        left.sort();
        let mut expected = kept.to_vec();
        expected.sort();
        assert_eq!(left, expected);
    }

    /// Which writer each file uses is discovered from the source, not
    /// trusted: the update records go through `atomic_write_record`, whose
    /// temporaries the sweep knows, and the crash-loop rollback files through
    /// the original `atomic_write`, which the sweep never touches.
    #[test]
    fn record_writes_name_only_swept_files() {
        let src = include_str!("rollback.rs").replace("\r\n", "\n");
        let production = src
            .split_once(concat!("#[cfg(test)]\n", "pub(crate) mod tests {"))
            .unwrap()
            .0;
        let flat: String = production.split_whitespace().collect();
        let written = |writer: &str| -> Vec<String> {
            let needle = format!("{writer}(&dir.join(");
            flat.match_indices(&needle)
                .map(|(at, _)| {
                    let name = &flat[at + needle.len()..];
                    name[..name.find(')').unwrap()].to_string()
                })
                .collect()
        };
        let mut records = written(concat!("atomic_write", "_record"));
        records.sort();
        assert_eq!(records, ["DOWNLOAD_BUDGET_FILE", "UPDATE_COOLDOWN_FILE"]);
        assert_eq!(RECORD_FILES, [UPDATE_COOLDOWN_FILE, DOWNLOAD_BUDGET_FILE]);
        let mut rollback_files = written(concat!("atomic", "_write"));
        rollback_files.sort();
        assert_eq!(
            rollback_files,
            ["INSTALL_FAILURES_FILE", "KNOWN_BAD_FILE", "PROBATION_FILE"]
        );
    }

    #[test]
    fn the_summary_names_the_release_and_says_when() {
        let tmp = tempfile::tempdir().unwrap();
        let dir = tmp.path();
        let now = SystemTime::now();
        assert!(update_cooldown_summary_at(dir, "0.2.140", now).is_none());
        let long_error = format!("error sending request: {}", "x".repeat(2000));
        record_update_failure_at(dir, "0.2.141", &long_error, now, 0.5);
        let summary = update_cooldown_summary_at(dir, "0.2.140", now).unwrap();
        assert!(
            summary.contains("0.2.141") && summary.contains("1 time(s)"),
            "{summary}"
        );
        assert!(summary.contains("next attempt in 60 min"), "{summary}");
        assert_eq!(
            read_update_cooldown_at(dir)
                .unwrap()
                .last_error
                .chars()
                .count(),
            UPDATE_COOLDOWN_ERROR_MAX_CHARS
        );
        let later = now + std::time::Duration::from_secs(2 * 3600);
        assert!(
            update_cooldown_summary_at(dir, "0.2.140", later)
                .unwrap()
                .contains("cooldown over")
        );
        assert!(
            update_cooldown_summary_at(dir, "0.2.141", now).is_none(),
            "installed"
        );
        pin_known_bad_at(dir, "0.2.141").unwrap();
        assert!(
            update_cooldown_summary_at(dir, "0.2.140", now)
                .unwrap()
                .contains("will not retry")
        );
        record_update_failure_at(dir, "not-a-version", "x", now, 0.5);
        assert!(
            update_cooldown_summary_at(dir, "0.2.140", now).is_some(),
            "never dropped"
        );
    }

    /// The longest wait (4 h + 20%) is honoured to its end, and
    /// only a retry time beyond it reads as a bogus record. The window is
    /// pinned too.
    #[test]
    fn the_longest_cooldown_is_honoured_to_its_edge() {
        let tmp = tempfile::tempdir().unwrap();
        let dir = tmp.path();
        // A whole second, so the first moment's remaining wait is exactly the
        // longest one.
        let now = UNIX_EPOCH + std::time::Duration::from_secs(unix_secs(SystemTime::now()));
        let minutes = |m: u64| std::time::Duration::from_secs(m * 60);
        for _ in 0..3 {
            record_update_failure_at(dir, "0.2.141", "x", now, 1.0);
        }
        assert_eq!(read_update_cooldown_at(dir).unwrap().failures, 3);
        assert!(
            update_cooldown_remaining_at(dir, "0.2.141", now).is_some(),
            "the full 4.8 h is honoured from the first moment"
        );
        assert!(update_cooldown_remaining_at(dir, "0.2.141", now + minutes(282)).is_some());
        assert!(update_cooldown_remaining_at(dir, "0.2.141", now + minutes(289)).is_none());
        assert_eq!(DOWNLOAD_WINDOW, std::time::Duration::from_secs(24 * 3600));
    }

    /// A budget hold, record by record: a live record for the release keeps
    /// the later retry time, its count and its error; an expired or
    /// implausible one keeps its error and its still-live count but not its
    /// retry time; another release's record is replaced and lends nothing.
    /// The retry time is never more than the longest cooldown from now,
    /// unless a live record already waits longer, and the failure time is
    /// always now.
    #[test]
    fn a_budget_hold_record_by_record() {
        let now = UNIX_EPOCH + std::time::Duration::from_secs(1_800_000_000);
        let at = |offset_min: i64| {
            let base = unix_secs(now);
            if offset_min >= 0 {
                base + offset_min.unsigned_abs() * 60
            } else {
                base - offset_min.unsigned_abs() * 60
            }
        };
        let record = |version: &str, retry_min: i64| UpdateCooldownState {
            version: version.to_string(),
            failures: 2,
            failed_at_unix: at(-60),
            retry_at_unix: at(retry_min),
            last_error: "boom".to_string(),
            hold_reason: None,
            provisional: true,
        };
        let (h, cap) = (60, 4 * 60);
        // (case, record on file, `until`, expected retry, count, error)
        for (case, prev, until, retry, failures, error) in [
            ("no record, until below the cap", None, 2 * h, 2 * h, 0, ""),
            ("no record, until above the cap", None, 10 * h, cap, 0, ""),
            ("no record, until already past", None, -h, 0, 0, ""),
            (
                "live, its later retry kept",
                Some(record("9.9.9", 3 * h)),
                h,
                3 * h,
                2,
                "boom",
            ),
            (
                "live, the later until wins",
                Some(record("9.9.9", h)),
                2 * h,
                2 * h,
                2,
                "boom",
            ),
            (
                "live, until above the cap",
                Some(record("9.9.9", 3 * h)),
                10 * h,
                cap,
                2,
                "boom",
            ),
            (
                "live past the cap (jitter), kept",
                Some(record("9.9.9", 270)),
                10 * h,
                270,
                2,
                "boom",
            ),
            (
                "expired cooldown, failures still live",
                Some(record("9.9.9", -h)),
                2 * h,
                2 * h,
                2,
                "boom",
            ),
            (
                "expired cooldown, until above the cap",
                Some(record("9.9.9", -h)),
                10 * h,
                cap,
                2,
                "boom",
            ),
            (
                "failures expired too",
                Some(record("9.9.9", -49 * h)),
                2 * h,
                2 * h,
                0,
                "boom",
            ),
            (
                "implausible retry time, ignored",
                Some(record("9.9.9", 10 * h)),
                h,
                h,
                2,
                "boom",
            ),
            (
                "another release, live",
                Some(record("9.9.10", 3 * h)),
                h,
                h,
                0,
                "",
            ),
            (
                "another release, until above the cap",
                Some(record("9.9.10", 3 * h)),
                10 * h,
                cap,
                0,
                "",
            ),
            (
                "the unknown-release sentinel",
                Some(record(UNKNOWN_RELEASE, 3 * h)),
                h,
                h,
                0,
                "",
            ),
        ] {
            let tmp = tempfile::tempdir().unwrap();
            let dir = tmp.path();
            if let Some(prev) = &prev {
                assert!(store_update_cooldown_at(dir, prev), "{case}");
            }
            let until = UNIX_EPOCH + std::time::Duration::from_secs(at(until));
            hold_update_until_at(dir, "9.9.9", "the budget is spent", until, now);
            assert_eq!(
                read_update_cooldown_at(dir),
                Some(UpdateCooldownState {
                    version: "9.9.9".to_string(),
                    failures,
                    failed_at_unix: unix_secs(now),
                    retry_at_unix: at(retry),
                    last_error: error.to_string(),
                    hold_reason: Some("the budget is spent".to_string()),
                    provisional: false,
                }),
                "{case}"
            );
        }
    }

    /// The release on record counts for a failed lookup only when it is
    /// newer than the current version and not blocked (#4073). The sentinel
    /// and an unparseable version count as newer.
    #[test]
    fn only_a_newer_unblocked_release_on_record_is_returned() {
        let now = SystemTime::now();
        let none = |_: &str| false;
        let tmp = tempfile::tempdir().unwrap();
        assert_eq!(recorded_newer_release_at(tmp.path(), "9.9.8", none), None);
        for (recorded, blocked, found) in [
            ("9.9.7", false, false),
            ("9.9.8", false, false),
            ("9.9.9", false, true),
            ("9.9.9", true, false),
            (UNKNOWN_RELEASE, false, true),
            ("not-a-version", false, true),
        ] {
            let tmp = tempfile::tempdir().unwrap();
            let dir = tmp.path();
            record_update_failure_at(dir, recorded, "x", now, 0.5);
            assert_eq!(
                recorded_newer_release_at(dir, "9.9.8", |v: &str| blocked && v == recorded),
                found.then(|| recorded.to_string()),
                "{recorded}, blocked: {blocked}"
            );
        }
    }

    /// The download budget counts a node's bytes in a fixed window from the
    /// first byte charged, refunds never go below zero, and it fails open
    /// both ways: an unreadable record is nothing spent, an unwritable one
    /// loses the spend rather than blocking downloads.
    #[test]
    fn download_bytes_are_counted_per_node_in_a_fixed_window() {
        let tmp = tempfile::tempdir().unwrap();
        let dir = tmp.path();
        let now = SystemTime::now();
        let hour = std::time::Duration::from_secs(3600);
        assert_eq!(download_bytes_at(dir, now), 0);
        assert_eq!(download_window_end_at(dir, now), None);
        refund_download_bytes_at(dir, 10, now);
        assert!(
            !dir.join(DOWNLOAD_BUDGET_FILE).exists(),
            "a refund opens no window"
        );
        charge_download_bytes_at(dir, 1000, now);
        charge_download_bytes_at(dir, 0, now);
        // Later charges do not move the window.
        charge_download_bytes_at(dir, 500, now + hour * 23);
        assert_eq!(download_bytes_at(dir, now + hour * 23), 1500);
        let end = download_window_end_at(dir, now).unwrap();
        assert_eq!(
            end.duration_since(UNIX_EPOCH).unwrap().as_secs(),
            unix_secs(now) + DOWNLOAD_WINDOW.as_secs()
        );
        refund_download_bytes_at(dir, 400, now);
        assert_eq!(download_bytes_at(dir, now), 1100);
        refund_download_bytes_at(dir, u64::MAX, now);
        assert_eq!(download_bytes_at(dir, now), 0, "never below zero");
        charge_download_bytes_at(dir, 1500, now);
        let next_day = now + DOWNLOAD_WINDOW;
        assert_eq!(download_bytes_at(dir, next_day), 0, "a new window");
        charge_download_bytes_at(dir, 7, next_day);
        assert_eq!(download_bytes_at(dir, next_day), 7);
        // A record from the future (a clock stepped back) is a new window
        // rather than one that blocks until then.
        assert_eq!(download_bytes_at(dir, now), 0);
        // Corrupt: nothing spent.
        std::fs::write(dir.join(DOWNLOAD_BUDGET_FILE), "{bad").unwrap();
        assert_eq!(download_bytes_at(dir, now), 0);
        charge_download_bytes_at(dir, 3, now);
        assert_eq!(download_bytes_at(dir, now), 3);
        // Unwritable (a directory in the way): the spend is lost, no panic,
        // and reads still say nothing is spent.
        std::fs::remove_file(dir.join(DOWNLOAD_BUDGET_FILE)).unwrap();
        std::fs::create_dir(dir.join(DOWNLOAD_BUDGET_FILE)).unwrap();
        charge_download_bytes_at(dir, u64::MAX, now);
        assert_eq!(download_bytes_at(dir, now), 0);
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
