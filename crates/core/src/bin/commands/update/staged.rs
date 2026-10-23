//! Download a release while the node is still running, so the post-stop
//! updater only has to install it (#5790).
//!
//! On systemd the node's exit-42 hands off to `freenet update` in
//! `ExecStopPost`, which runs under the unit's `TimeoutStopSec`. Downloading
//! the release archives (two of ~20 MB each) inside that window fails on a
//! slow link: systemd kills the updater, `Restart=always` starts the old
//! binary again, it detects the same update, exits 42, and the cycle repeats
//! forever. The fix is to move the download out of the stop phase. Before it
//! exits 42 the node fetches the release into a cache under the state
//! directory, bounded per read by a stall timeout and overall by
//! [`STAGE_DEADLINE`], resuming a partial file left by an earlier attempt. The
//! updater then installs from the cache, which takes seconds.
//!
//! The cache is never trusted. [`load`] re-runs the same signature and
//! checksum verification over the cached bytes that the installer runs over a
//! fresh download, checks the cached manifest against the live one when it can
//! reach it, and on any problem discards the cache and falls back to
//! downloading, exactly as before this existed. So the worst a damaged or
//! stale cache can do is cost the download it was meant to save.
//!
//! Only nodes already running a release that contains this module benefit: the
//! node that stages and the `freenet update` that installs are both the binary
//! on disk, so a node on an older release still downloads in `ExecStopPost`.
//!
//! Staging also decides whether to exit at all (#5799). When GitHub cannot be
//! reached for the release, exiting would hand the same download to an updater
//! that fails it too, and the supervisor would restart the old binary into the
//! same decision. So the node stays on its current binary
//! ([`UpdatePreparation::Stay`]) and records a cooldown for the release
//! (`rollback::record_update_failure_at`: 1 h, 2 h, then 4 h, ±20%), which
//! every update trigger checks before it asks GitHub anything. Every exit
//! records first too, including the exit to install a staged release (a
//! successful install clears it), because systemd may kill the updater before
//! it records anything. A lookup that finds nothing newer to install never
//! exits: the updater would find nothing either. Three failures of one release
//! in a row, or one that cannot be recorded, exit anyway: a fault here can
//! delay an update but never prevent one.
//!
//! Staging's GitHub traffic has a hard ceiling that does not depend on any of
//! that: every staging run spends one token from the node's persisted poll
//! bucket (#4073), and staging and the updater together receive at most
//! `rollback::DOWNLOAD_BYTES_PER_DAY` bytes of release archives per node per
//! day (the updater charges the same record through [`ByteMeter`]). While the
//! budget is spent, staging and the updater both hold the release without
//! counting a failure: an exit would only reach an updater the same budget
//! refuses.

use std::fs;
use std::io::Write as _;
use std::path::{Path, PathBuf};
use std::time::{Duration, SystemTime};

use anyhow::{Context, Result};

use super::super::auto_update::GithubRateLimitedError;
use super::{Checksums, Release, ReleaseVerificationError};

/// Directory under the auto-update state directory holding at most one
/// release, in a subdirectory named after its tag.
const CACHE_DIR: &str = "staged_update";
const MANIFEST: &str = "SHA256SUMS.txt";
const SIGNATURE: &str = "SHA256SUMS.txt.sig";
/// The asset-download path GitHub serves for a tag. Unlike the asset list this
/// is not an `api.github.com` request, so staging spends no REST quota (#5102).
const RELEASE_DOWNLOAD_PREFIX: &str = "https://github.com/freenet/freenet-core/releases/download/";

/// How long one staging run may take, retries included. The node keeps
/// serving on its current version meanwhile, including after an urgent or
/// isolation trigger that used to exit at once. A run that runs out of time
/// counts as a failed attempt like any other (#5799): the node stays and
/// retries after a cooldown, and only the third such failure in a row exits
/// for the updater. A partial download survives in the cache, so the next
/// run resumes it.
const STAGE_DEADLINE: Duration = Duration::from_secs(2 * 3600);
/// Consecutive failures of one release after which a failed attempt exits 42
/// anyway, so the updater tries its own download (#5799).
const FALLBACK_AFTER_FAILURES: u32 = 3;
/// What an exit to install a staged release records. The install clears it;
/// if the updater fails or is killed before it can, the restart holds instead
/// of exiting for the same release again at once.
const INSTALL_PENDING: &str =
    "exited to install the downloaded update; the install did not report success";
/// Waits between failed attempts. The number of entries is the number of
/// retries. Only network failures are retried; see [`worth_retrying`].
const STAGE_RETRY_DELAYS: [Duration; 5] = [
    Duration::from_secs(30),
    Duration::from_secs(60),
    Duration::from_secs(120),
    Duration::from_secs(300),
    Duration::from_secs(600),
];
/// Waits between failed tag lookups within one staging run, so a blip of a
/// few seconds does not cost a whole cooldown. A rate limit is never retried
/// (see [`worth_retrying`]).
const LOOKUP_RETRY_DELAYS: [Duration; 2] = [Duration::from_secs(5), Duration::from_secs(15)];
/// Free space the state directory must have before the node stages. The
/// archives take ~45 MB, and at install time the crash-loop rollback snapshot of
/// the running binary is written to the same directory (#4073). Staging on a
/// nearly full disk could cost that snapshot, so below this the node leaves the
/// download to the updater's temporary directory, as before.
const MIN_FREE_BYTES: u64 = 512 * 1024 * 1024;
/// How long the updater waits for the live manifest when checking the cache is
/// still current. Short, because it runs inside `TimeoutStopSec`; on timeout
/// the (already verified) cache is used.
const LIVE_MANIFEST_TIMEOUT: Duration = Duration::from_secs(10);

/// Authenticates a manifest against its optional detached signature.
/// Production passes [`super::verify_release_manifest_signature`]; tests pass a
/// throwaway key.
type ManifestVerifier<'a> = &'a (dyn Fn(&[u8], Option<&[u8]>) -> Result<()> + Sync);

/// The wall clock staging records against. Production passes
/// `SystemTime::now`; tests pass a fixed time, so they can walk a whole day of
/// cooldowns without waiting for it.
pub(super) type Clock<'a> = &'a (dyn Fn() -> SystemTime + Sync);

/// A failure to read or write the cache itself, not the network. Another
/// attempt would fail the same way, so it is not retried.
#[derive(Debug)]
struct CacheIoError {
    what: String,
    source: std::io::Error,
}

impl std::fmt::Display for CacheIoError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}: {}", self.what, self.source)
    }
}

impl std::error::Error for CacheIoError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(&self.source)
    }
}

fn cache_io(what: impl Into<String>) -> impl FnOnce(std::io::Error) -> anyhow::Error {
    let what = what.into();
    move |source| CacheIoError { what, source }.into()
}

/// The release does not publish this asset (HTTP 404). Retried like a network
/// failure, because a release's assets can lag its tag; for fdev it means the
/// release has none, and the updater is left to it.
#[derive(Debug)]
struct AssetMissing(String);

impl std::fmt::Display for AssetMissing {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "release asset not found: {}", self.0)
    }
}

impl std::error::Error for AssetMissing {}

/// Today's download budget for this node is spent (#5799). Not retried: the
/// budget is what bounds the release's download bytes.
#[derive(Debug)]
pub(super) struct BudgetSpent;

impl std::fmt::Display for BudgetSpent {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("today's download budget is spent (#5799)")
    }
}

impl std::error::Error for BudgetSpent {}

/// A staging run did not finish within its deadline ([`STAGE_DEADLINE`]). A
/// partial download survives it, so the next attempt resumes.
#[derive(Debug)]
struct StageTimedOut(Duration);

impl std::fmt::Display for StageTimedOut {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "the download did not finish within {} minutes",
            self.0.as_secs() / 60
        )
    }
}

impl std::error::Error for StageTimedOut {}

/// How many received bytes beyond what a [`ByteMeter`] has charged it lets
/// accumulate before it writes them to the budget record (a download longer
/// than its Content-Length said).
const METER_PERSIST_EVERY: u64 = 16 * 1024 * 1024;

/// What a download of unknown size is charged for before its body arrives:
/// more than the largest release asset, the macOS DMG (about 78 MiB for
/// 0.2.148), with room to spare.
const MAX_UNKNOWN_DOWNLOAD: u64 = 128 * 1024 * 1024;

/// Counts the archive bytes a run receives (staging's or the updater's)
/// against what is left of the node's daily budget
/// (`rollback::DOWNLOAD_BYTES_PER_DAY`), and, when it has a ledger, charges
/// that budget's record: BEFORE each download's body, for the bytes it is
/// expected to bring, then reconciled to what actually arrived when it ends.
/// A run killed mid-download (systemd's `TimeoutStopSec` on the updater) has
/// therefore already paid for it.
pub(super) struct ByteMeter<'a> {
    allowance: u64,
    received: std::sync::atomic::AtomicU64,
    /// Bytes charged to the ledger for this meter: `received`, plus what has
    /// been charged ahead of a download in progress.
    recorded: std::sync::atomic::AtomicU64,
    ledger: Option<Ledger<'a>>,
}

/// Where a [`ByteMeter`] writes what it counts: the state directory's
/// per-node budget record.
struct Ledger<'a> {
    dir: PathBuf,
    clock: Clock<'a>,
}

impl<'a> ByteMeter<'a> {
    pub(super) fn new(allowance: u64) -> Self {
        Self {
            allowance,
            received: std::sync::atomic::AtomicU64::new(0),
            recorded: std::sync::atomic::AtomicU64::new(0),
            ledger: None,
        }
    }

    /// A meter allowing `allowance` bytes that charges them to the budget
    /// record in `dir` (see [`ByteMeter::precharge`] and
    /// [`ByteMeter::flush`]).
    pub(super) fn charged_to(allowance: u64, dir: &Path, clock: Clock<'a>) -> Self {
        Self {
            ledger: Some(Ledger {
                dir: dir.to_path_buf(),
                clock,
            }),
            ..Self::new(allowance)
        }
    }

    #[cfg(test)]
    fn unlimited() -> Self {
        Self::new(u64::MAX)
    }

    fn received(&self) -> u64 {
        self.received.load(std::sync::atomic::Ordering::Relaxed)
    }

    /// Refuse before a request when nothing is left.
    pub(super) fn check(&self) -> Result<()> {
        if self.received() >= self.allowance {
            return Err(BudgetSpent.into());
        }
        Ok(())
    }

    /// Charge the ledger, before a download's body arrives, for the
    /// `expected` bytes it will bring (its Content-Length), or
    /// [`MAX_UNKNOWN_DOWNLOAD`] when the size is unknown; never more than is
    /// left of the allowance, nor more than a whole day's budget (an
    /// unlimited meter's allowance is unbounded).
    pub(super) fn precharge(&self, expected: Option<u64>) {
        let received = self.received();
        let left = self.allowance.saturating_sub(received);
        let charge = expected
            .unwrap_or(MAX_UNKNOWN_DOWNLOAD)
            .min(left)
            .min(super::super::rollback::DOWNLOAD_BYTES_PER_DAY);
        self.record(received.saturating_add(charge));
    }

    /// Count `n` bytes just received; an error once they pass the allowance.
    pub(super) fn spend(&self, n: usize) -> Result<()> {
        use std::sync::atomic::Ordering::Relaxed;
        let n = u64::try_from(n).unwrap_or(u64::MAX);
        let total = self.received.fetch_add(n, Relaxed).saturating_add(n);
        if total.saturating_sub(self.recorded.load(Relaxed)) >= METER_PERSIST_EVERY {
            self.record(total);
        }
        if total > self.allowance {
            return Err(BudgetSpent.into());
        }
        Ok(())
    }

    /// Reconcile the ledger to the bytes actually received: refund what was
    /// charged ahead and did not arrive, charge what arrived uncharged.
    pub(super) fn flush(&self) {
        self.record(self.received());
    }

    /// Bring this meter's charge on the ledger to `total`. Fails open like
    /// the record itself.
    fn record(&self, total: u64) {
        use std::sync::atomic::Ordering::Relaxed;
        let Some(ledger) = &self.ledger else {
            return;
        };
        let before = self.recorded.swap(total, Relaxed);
        let now = (ledger.clock)();
        if total > before {
            super::super::rollback::charge_download_bytes_at(&ledger.dir, total - before, now);
        } else {
            super::super::rollback::refund_download_bytes_at(&ledger.dir, before - total, now);
        }
    }
}

/// Whether another attempt could succeed. Everything is retried except four
/// failures that come back the same every time: a rate limit (retrying into it
/// is what escalates it), a release that fails verification, a cache that
/// cannot be written, and a spent download budget.
fn worth_retrying(e: &anyhow::Error) -> bool {
    e.downcast_ref::<BudgetSpent>().is_none()
        && e.downcast_ref::<GithubRateLimitedError>().is_none()
        && e.downcast_ref::<ReleaseVerificationError>().is_none()
        && e.downcast_ref::<CacheIoError>().is_none()
}

pub(super) fn freenet_asset_name() -> String {
    format!(
        "freenet-{}.{}",
        super::get_target_triple(),
        super::get_archive_extension()
    )
}

pub(super) fn fdev_asset_name() -> String {
    format!(
        "fdev-{}.{}",
        super::get_target_triple(),
        super::get_archive_extension()
    )
}

fn cache_root() -> Option<PathBuf> {
    super::super::auto_update::state_dir().map(|d| d.join(CACHE_DIR))
}

/// The cache directory for `tag`, refusing any tag that is not a plain path
/// component. The tag comes from GitHub's redirect, so it is not trusted to
/// stay inside the cache.
fn tag_dir(root: &Path, tag: &str) -> Result<PathBuf> {
    let plain = !tag.is_empty()
        && !tag.starts_with('.')
        && tag
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || matches!(c, '.' | '-' | '_' | '+'));
    anyhow::ensure!(plain, "refusing to stage release tag {tag:?}");
    Ok(root.join(tag))
}

/// Exclusive, non-blocking lock on the cache, so two processes sharing a state
/// directory (two nodes under one account) never write the same partial file.
/// Kept beside the cache rather than in it, because discarding the cache
/// removes the directory. `Ok(None)` when another process holds it.
///
/// A POSIX record lock (`fcntl`, `rollback::try_lock_file`), not `flock`: a
/// `flock` is shared with every
/// child forked while it is held, until that child execs, so a process that
/// spawns children could keep it held after releasing it. Record locks belong
/// to the process and are never inherited. They do not exclude other callers
/// in the same process, and closing ANY descriptor of the lock file in this
/// process releases the lock, so [`HELD_LOCKS`] refuses a second in-process
/// holder before it can open one.
///
/// Unix only. Elsewhere this always succeeds; the write order (archive moved
/// into place before the manifest is written) and the installer's checksums
/// still keep a cache that two processes wrote from being installed wrong.
struct CacheLock {
    /// `Option` so `drop` can close it while still holding [`HELD_LOCKS`].
    file: Option<fs::File>,
    path: PathBuf,
}

/// Lock files this process holds. A record lock does not exclude this
/// process's own other callers, and closing a second descriptor for the file
/// would silently release it, so in-process exclusion is enforced here.
static HELD_LOCKS: std::sync::Mutex<Vec<PathBuf>> = std::sync::Mutex::new(Vec::new());

impl Drop for CacheLock {
    fn drop(&mut self) {
        // Poisoning cannot leave this list wrong: every critical section is a
        // single push or retain.
        let mut held = HELD_LOCKS.lock().unwrap_or_else(|e| e.into_inner());
        // Release the lock before the registry entry. In the other order a
        // second in-process caller could open the file in between, and this
        // close would then release the lock it believes it holds.
        drop(self.file.take());
        held.retain(|p| p != &self.path);
    }
}

fn try_lock(root: &Path) -> std::io::Result<Option<CacheLock>> {
    if let Some(parent) = root.parent() {
        fs::create_dir_all(parent)?;
    }
    let path = root.with_extension("lock");
    let mut held = HELD_LOCKS.lock().unwrap_or_else(|e| e.into_inner());
    if held.contains(&path) {
        return Ok(None);
    }
    let file = fs::OpenOptions::new()
        .create(true)
        .truncate(false)
        .write(true)
        .open(&path)?;
    if !super::super::rollback::try_lock_file(&file)? {
        return Ok(None);
    }
    held.push(path.clone());
    Ok(Some(CacheLock {
        file: Some(file),
        path,
    }))
}

/// Remove every staged release except `keep`, so the cache never holds more
/// than the one release that is about to be installed.
fn remove_other_tags(root: &Path, keep: &str) {
    let Ok(entries) = fs::read_dir(root) else {
        return;
    };
    for entry in entries.flatten() {
        if entry.file_name() != keep {
            #[allow(clippy::let_underscore_must_use)]
            let _ = fs::remove_dir_all(entry.path());
        }
    }
}

/// Delete the whole cache. Called once an update has been installed (the
/// files have served their purpose) and when the installer finds nothing to
/// install. Skipped while another process is staging into it.
pub(super) fn discard() {
    if let Some(root) = cache_root() {
        if let Ok(Some(_lock)) = try_lock(&root) {
            discard_at(&root);
        }
    }
}

fn discard_at(root: &Path) {
    match fs::remove_dir_all(root) {
        Ok(()) => {}
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
        Err(e) => {
            tracing::warn!(error = %e, path = %root.display(), "Failed to remove staged update")
        }
    }
}

/// Remove staged releases this binary already is (or is newer than), and ones
/// it has since blocked (#4073). Run when the node starts, so a cache left
/// behind by an update applied some other way (a package manager, a manual
/// install, an updater killed before its cleanup) does not sit in the state
/// directory until the next release.
pub(crate) fn discard_stale(current_version: &str) {
    let Some(root) = cache_root() else {
        return;
    };
    let Ok(Some(_lock)) = try_lock(&root) else {
        return;
    };
    discard_stale_at(&root, current_version, version_blocked);
}

fn discard_stale_at(root: &Path, current_version: &str, blocked: impl Fn(&str) -> bool) {
    let Ok(entries) = fs::read_dir(root) else {
        return;
    };
    for entry in entries.flatten() {
        let keep = entry
            .file_name()
            .to_str()
            .is_some_and(|tag| should_stage(tag, current_version, &blocked));
        if !keep {
            #[allow(clippy::let_underscore_must_use)]
            let _ = fs::remove_dir_all(entry.path());
        }
    }
}

/// Hold the staged release for a cooldown without counting a failure, for an
/// updater run that gave up before attempting a download (see
/// `rollback::hold_update_without_counting_at`). Nothing staged, nothing held.
pub(super) fn hold_staged_release(reason: &str) {
    if let Some(root) = cache_root() {
        let rand_unit = freenet::config::GlobalRng::random_range(0.0_f64..1.0);
        hold_staged_release_at(&root, reason, SystemTime::now(), rand_unit);
    }
}

fn hold_staged_release_at(root: &Path, reason: &str, now: SystemTime, rand_unit: f64) {
    if let (Some(tag), Some(state_dir)) = (staged_tag_at(root), root.parent()) {
        super::super::rollback::hold_update_without_counting_at(
            state_dir,
            super::super::auto_update::version_from_tag(&tag),
            reason,
            now,
            rand_unit,
        );
    }
}

/// Whether a complete copy of `tag` is staged, so installing it needs no
/// download of the freenet archive.
pub(super) fn is_staged(tag: &str) -> bool {
    cache_root().is_some_and(|root| staged_tag_at(&root).as_deref() == Some(tag))
}

/// The tag of the release staged under `root`, if a complete one is there (its
/// manifest is written only after the freenet archive verified).
fn staged_tag_at(root: &Path) -> Option<String> {
    fs::read_dir(root)
        .ok()?
        .flatten()
        .filter_map(|entry| entry.file_name().into_string().ok())
        .find(|tag| tag_dir(root, tag).is_ok_and(|dir| dir.join(MANIFEST).is_file()))
}

/// A version this node refuses to install (#4073): pinned known-bad after a
/// crash-loop rollback, or gated after repeated install failures.
fn version_blocked(version: &str) -> bool {
    super::super::rollback::is_version_pinned_bad(version)
        || super::super::rollback::is_version_install_gated(version)
}

/// Bytes available to this user on the filesystem holding `dir`, if known.
#[cfg(unix)]
#[allow(clippy::useless_conversion)] // statvfs field widths differ by platform
fn free_bytes(dir: &Path) -> Option<u64> {
    use std::os::unix::ffi::OsStrExt;
    let path = std::ffi::CString::new(dir.as_os_str().as_bytes()).ok()?;
    let mut stat = std::mem::MaybeUninit::<libc::statvfs>::uninit();
    // SAFETY: `path` is NUL-terminated and outlives the call; statvfs writes
    // a full `statvfs` into `stat` when it returns 0, and only then is it read.
    if unsafe { libc::statvfs(path.as_ptr(), stat.as_mut_ptr()) } != 0 {
        return None;
    }
    // SAFETY: initialised by the successful call above.
    let stat = unsafe { stat.assume_init() };
    Some(u64::from(stat.f_bavail).saturating_mul(u64::from(stat.f_frsize)))
}

#[cfg(not(unix))]
fn free_bytes(_dir: &Path) -> Option<u64> {
    None
}

// ── Node side: download before exiting ──────────────────────────────────────

/// What the node does once [`stage_latest_release`] returns.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[must_use]
pub(crate) enum UpdatePreparation {
    /// Exit 42: the updater installs, from the cache when staging succeeded
    /// and from the network otherwise.
    Exit,
    /// Keep running on the current binary (#5799). The release could not be
    /// fetched, or is cooling down; the triggers check its record before they
    /// try again.
    Stay,
}

impl UpdatePreparation {
    pub(crate) fn should_exit(self) -> bool {
        self == Self::Exit
    }
}

/// Download and verify the latest release into the cache before the node
/// exits for an update, and decide whether to exit at all. Never fails and
/// never panics. `target` is the release the trigger detected, when it knows
/// one: a failed tag lookup is then recorded against it.
pub(crate) async fn stage_latest_release(
    current_version: &str,
    target: Option<&str>,
) -> UpdatePreparation {
    // Before anything can decide to skip: the release canary (Gate B) arms its
    // staging check on this line, so a later regression in any skip condition
    // shows up as "prepared but never downloaded" instead of as a binary that
    // predates staging.
    tracing::info!("Preparing the update before exiting (#5790)");
    contain_panic(stage_latest_release_inner(current_version, target), || {
        record_panic(target)
    })
    .await
}

/// Run `fut`, turning a panic into a log line and an exit. The update-check
/// task sends on `update_tx` only after staging returns; a panic unwinding
/// through it would drop the sender, and the node would exit 0 without ever
/// running the updater, on every attempt. `on_panic` records the failure.
async fn contain_panic(
    fut: impl std::future::Future<Output = UpdatePreparation>,
    on_panic: impl FnOnce(),
) -> UpdatePreparation {
    use futures::FutureExt;
    match std::panic::AssertUnwindSafe(fut).catch_unwind().await {
        Ok(preparation) => preparation,
        Err(_) => {
            tracing::error!(
                "Downloading the update in advance panicked; exiting for the update anyway"
            );
            on_panic();
            UpdatePreparation::Exit
        }
    }
}

fn record_panic(target: Option<&str>) {
    if let Some(root) = cache_root() {
        record_panic_at(&root, target);
    }
}

fn record_panic_at(root: &Path, target: Option<&str>) {
    if let Some(version) = target {
        record_failure(
            root,
            version,
            "preparing the update panicked",
            SystemTime::now(),
        );
    }
}

async fn stage_latest_release_inner(
    current_version: &str,
    target: Option<&str>,
) -> UpdatePreparation {
    let Some(root) = cache_root() else {
        tracing::warn!("No usable state directory; the update will be downloaded after exit");
        return UpdatePreparation::Exit;
    };
    // The tag the updater will install. Resolved through the same quota-free
    // redirect the updater uses, so both normally see the same tag; if a newer
    // release lands in between, the updater finds no cache for it and
    // downloads as before.
    let lookup = || super::super::auto_update::fetch_latest_release_tag(false);
    #[cfg(target_os = "macos")]
    {
        // An app-bundle install updates from the DMG, not from these archives,
        // so nothing is staged; the lookup, cooldown and records still apply.
        let in_bundle = std::env::current_exe()
            .ok()
            .and_then(|exe| super::super::service::macos_app_bundle_path(&exe))
            .is_some();
        if in_bundle {
            return decide_without_staging(
                &root,
                current_version,
                target,
                lookup,
                (
                    &LOOKUP_RETRY_DELAYS,
                    super::super::rollback::DOWNLOAD_BYTES_PER_DAY,
                ),
                &SystemTime::now,
            )
            .await
            .preparation();
        }
    }
    stage_into(
        &root,
        current_version,
        lookup,
        target,
        version_blocked,
        &production_env(&root),
    )
    .await
    .preparation()
}

/// The release signature check staging runs over a downloaded manifest: the
/// installer's own, quietly.
fn verify_production(manifest: &[u8], signature: Option<&[u8]>) -> Result<()> {
    super::verify_release_manifest_signature(manifest, signature, true)
}

/// The environment the node stages in, for the cache at `root`.
fn production_env(root: &Path) -> StageEnv<'static> {
    StageEnv {
        download_prefix: RELEASE_DOWNLOAD_PREFIX,
        // Unknown off unix, where the check is skipped.
        free: root.parent().and_then(free_bytes),
        verify: &verify_production,
        delays: &STAGE_RETRY_DELAYS,
        lookup_delays: &LOOKUP_RETRY_DELAYS,
        deadline: STAGE_DEADLINE,
        clock: &SystemTime::now,
        byte_budget: super::super::rollback::DOWNLOAD_BYTES_PER_DAY,
    }
}

/// What [`stage_into`] did. Tests assert on it; production acts on
/// [`Staging::preparation`].
#[derive(Debug, PartialEq)]
enum Staging {
    /// Nothing downloaded, for a reason that leaves the download to the
    /// updater (low disk, cache locked, an install that stages nothing).
    /// Recorded before the exit, like every exit.
    Skipped,
    Staged,
    /// Failed in a way another attempt would repeat: the release fails
    /// verification, or the cache cannot be written. The updater, which counts
    /// verification failures toward the #4073 install gate, takes over.
    Failed,
    /// GitHub could not be reached for the release. The node stays.
    Deferred,
    /// The release is cooling down; nothing was attempted. The node stays.
    CoolingDown,
    /// The tag lookup found nothing newer to install, or only a release this
    /// node has blocked (#4073). The node stays: the updater would find
    /// nothing to do. A release the trigger named is held without counting.
    NothingToInstall,
    /// The node's poll bucket was empty, so the run never reached the network.
    /// The node stays, holding the release without counting a failure.
    Throttled,
    /// A failure after `FALLBACK_AFTER_FAILURES` in a row, or one that could
    /// not be recorded: exit so the updater tries its own download.
    FallingBack,
}

impl Staging {
    fn preparation(&self) -> UpdatePreparation {
        match self {
            Staging::Deferred
            | Staging::CoolingDown
            | Staging::NothingToInstall
            | Staging::Throttled => UpdatePreparation::Stay,
            Staging::Skipped | Staging::Staged | Staging::Failed | Staging::FallingBack => {
                UpdatePreparation::Exit
            }
        }
    }
}

/// Whether a staging failure would come back the same on the next attempt.
fn is_permanent(e: &anyhow::Error) -> bool {
    e.downcast_ref::<ReleaseVerificationError>().is_some()
        || e.downcast_ref::<CacheIoError>().is_some()
}

/// Record a failure of `version` in the state directory beside `root`. `None`
/// when it could not be recorded.
fn record_failure(root: &Path, version: &str, error: &str, now: SystemTime) -> Option<u32> {
    let rand_unit = freenet::config::GlobalRng::random_range(0.0_f64..1.0);
    super::super::rollback::record_update_failure_at(root.parent()?, version, error, now, rand_unit)
}

/// Record a failure before an exit that leaves the download (or the install)
/// to the updater, which systemd may kill before it records anything. The
/// record is provisional: the updater's outcome for this attempt replaces it
/// rather than counting the attempt twice.
fn exit_after_recording(
    root: &Path,
    version: &str,
    reason: &str,
    staging: Staging,
    now: SystemTime,
) -> Staging {
    if let Some(dir) = root.parent() {
        let rand_unit = freenet::config::GlobalRng::random_range(0.0_f64..1.0);
        super::super::rollback::record_provisional_failure_at(dir, version, reason, now, rand_unit);
    }
    staging
}

/// Record a failure to fetch `version` and decide what the node does next:
/// stay, unless this is the third in a row or it could not be recorded.
fn after_transient_failure(root: &Path, version: &str, error: &str, now: SystemTime) -> Staging {
    match record_failure(root, version, error, now) {
        Some(failures) if failures < FALLBACK_AFTER_FAILURES => {
            tracing::warn!(
                version,
                failures,
                "Staying on the current version and trying the update again after a cooldown (#5799)"
            );
            Staging::Deferred
        }
        failures => {
            tracing::warn!(
                version,
                ?failures,
                "Could not fetch the update {FALLBACK_AFTER_FAILURES} times in a row, or could not \
                 record the failure; exiting so the updater tries its own download (#5799)"
            );
            Staging::FallingBack
        }
    }
}

/// Hold `version` without counting a failure until the node's download
/// budget is whole again, and stay. Exiting would hand the download to an
/// updater the same budget refuses: a restart for nothing. Nothing counts
/// toward the fallback while the budget is spent.
fn budget_spent(root: &Path, version: &str, now: SystemTime) -> Staging {
    let Some(dir) = root.parent() else {
        return Staging::Throttled;
    };
    let until = super::super::rollback::download_window_end_at(dir, now).unwrap_or(now);
    tracing::warn!(
        version,
        resets_in_min = until
            .duration_since(now)
            .unwrap_or_default()
            .as_secs()
            .div_ceil(60),
        "Today's download budget is spent; holding the update until it resets (#5799)"
    );
    super::super::rollback::hold_update_until_at(
        dir,
        version,
        "today's download budget is spent (#5799)",
        until,
        now,
    );
    Staging::Throttled
}

/// Hold `version` without counting a failure (the poll bucket or a GitHub
/// rate limit said no, so nothing was attempted) and stay.
fn throttled(root: &Path, version: Option<&str>, why: &str, now: SystemTime) -> Staging {
    tracing::info!(why, "Not downloading the update in advance now (#5799)");
    if let (Some(version), Some(state_dir)) = (version, root.parent()) {
        let rand_unit = freenet::config::GlobalRng::random_range(0.0_f64..1.0);
        super::super::rollback::hold_update_without_counting_at(
            state_dir, version, why, now, rand_unit,
        );
    }
    Staging::Throttled
}

/// Spend a token from the node's GitHub poll bucket beside `root`. Without a
/// state directory there is nothing to spend from, and staging could not
/// record anything either; allowed, as before #5799.
fn spend_poll_token(root: &Path, now: SystemTime) -> bool {
    let now_unix = now
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |d| d.as_secs());
    root.parent()
        .is_none_or(|dir| super::super::auto_update::try_consume_node_poll_at(dir, now_unix))
}

/// Everything staging decides before it downloads: the poll budget, the tag
/// lookup (retried after each of `lookup_delays`), the #4073 block list and
/// the cooldown. `Ok` carries the tag.
async fn decide_before_download<F, Fut>(
    root: &Path,
    current_version: &str,
    lookup: F,
    target: Option<&str>,
    blocked: impl Fn(&str) -> bool,
    (lookup_delays, byte_budget, clock): (&[Duration], u64, Clock<'_>),
) -> std::result::Result<String, Staging>
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = Result<String>>,
{
    // Checked before the poll token and the lookup (#5799): while any newer
    // release cools down the triggers wait, and the lookup would only find it.
    if let Some((version, left)) = root.parent().and_then(|dir| {
        super::super::rollback::update_cooldown_pending_at(dir, current_version, clock())
    }) {
        tracing::info!(version = %version, retry_in_min = left.as_secs().div_ceil(60), "Update is cooling down after failed attempts; staying on the current version (#5799)");
        return Err(Staging::CoolingDown);
    }
    // A spent download budget holds before any request too, unless a
    // complete release is already staged and could be installed without
    // downloading it. A manifest is written only after its archive
    // verifies, so one whose archive no longer verifies (truncated or
    // corrupted since, e.g. by a power loss) is not.
    if let Some(dir) = root.parent() {
        if super::super::rollback::download_bytes_at(dir, clock()) >= byte_budget {
            let staged = match staged_tag_at(root) {
                Some(tag) => freenet_archive_staged(root, &tag).await,
                None => false,
            };
            if !staged {
                let version = lookup_failure_target(root, current_version, target, &blocked);
                return Err(budget_spent(root, &version, clock()));
            }
        }
    }
    if !spend_poll_token(root, clock()) {
        return Err(throttled(
            root,
            target,
            "the node's GitHub poll budget is spent (#4073)",
            clock(),
        ));
    }
    // Before the run first reaches the network: a run killed from here on (a
    // crash, a stop mid-download) leaves a hold, so the restart waits out a
    // cooldown instead of starting over at once. Every outcome below replaces
    // it with its own record.
    if let Some(dir) = root.parent() {
        super::super::rollback::hold_update_without_counting_at(
            dir,
            &lookup_failure_target(root, current_version, target, &blocked),
            "a download in advance started and did not report how it ended",
            clock(),
            freenet::config::GlobalRng::random_range(0.0_f64..1.0),
        );
    }
    // Resolved first: the updater's first step is the same lookup, so when it
    // fails here an exit would only restart the node on the same binary.
    let tag = match stage_with_retries(lookup, lookup_delays, "Looking up the release to download")
        .await
    {
        Ok(tag) => tag,
        Err(e) => {
            tracing::warn!(error = %e, "Could not resolve the release to download in advance; staying on the current version (#5799)");
            // Counted even when the trigger knew no version, so a node that
            // cannot reach GitHub still walks to the fallback.
            let version = lookup_failure_target(root, current_version, target, &blocked);
            // A rate limit (a cooldown GitHub asked for, cached or live) is
            // not a failure of the release: hold it without counting, as the
            // updater does.
            if e.downcast_ref::<GithubRateLimitedError>().is_some() {
                return Err(throttled(
                    root,
                    Some(&version),
                    "GitHub rate-limited the release lookup",
                    clock(),
                ));
            }
            return Err(after_transient_failure(
                root,
                &version,
                &format!("{e:#}"),
                clock(),
            ));
        }
    };
    // A tag that passes this is a semver version, so `tag_dir` will accept it;
    // `stage_release_at` checks again regardless.
    // The lookup answered: forget a record for a release it shows cannot be
    // installed, which would otherwise hold every trigger for its cooldown.
    if let Some(dir) = root.parent() {
        let latest = super::super::auto_update::version_from_tag(&tag);
        super::super::rollback::clear_stale_update_record_at(dir, |recorded| {
            is_stale_record(recorded, current_version, latest, &blocked)
        });
    }
    if !should_stage(&tag, current_version, &blocked) {
        tracing::info!(tag = %tag, "No newer installable release to download in advance; staying on the current version");
        // An exit would only reach an updater that finds nothing to install,
        // so the node stays (#5799). A trigger that named a release holds it
        // without counting: when the lookup and the trigger's check disagree
        // (the API and the redirect can briefly report different releases),
        // the next trigger would otherwise name it again at once. The two
        // version-less triggers restart their own wait on a stay. (A stale
        // record, the sentinel included, was cleared above; the hold for a
        // named release is written after that on purpose, since it is what
        // spaces the next trigger.)
        if let (Some(version), Some(dir)) = (target, root.parent()) {
            super::super::rollback::hold_update_without_counting_at(
                dir,
                version,
                "the release lookup found no newer installable release",
                clock(),
                freenet::config::GlobalRng::random_range(0.0_f64..1.0),
            );
        }
        return Err(Staging::NothingToInstall);
    }
    // The clear above can remove the run's own hold (the sentinel's, or a
    // withdrawn named release's), so hold the release about to be downloaded:
    // a run killed mid-download then restarts into a cooldown.
    if let Some(dir) = root.parent() {
        super::super::rollback::hold_update_without_counting_at(
            dir,
            super::super::auto_update::version_from_tag(&tag),
            "a download in advance started and did not report how it ended",
            clock(),
            freenet::config::GlobalRng::random_range(0.0_f64..1.0),
        );
    }
    Ok(tag)
}

/// Whether a record for `recorded` is stale once a lookup has found `latest`:
/// the unknown release has become known, or the release is at or below the
/// current version, blocked by #4073, or newer than the latest (withdrawn).
/// An unparseable record is kept, as everywhere else.
fn is_stale_record(
    recorded: &str,
    current_version: &str,
    latest: &str,
    blocked: &impl Fn(&str) -> bool,
) -> bool {
    if recorded == super::super::rollback::UNKNOWN_RELEASE {
        return true;
    }
    let (Ok(release), Ok(current)) = (
        semver::Version::parse(recorded),
        semver::Version::parse(current_version),
    ) else {
        return false;
    };
    release <= current
        || blocked(recorded)
        || semver::Version::parse(latest).is_ok_and(|latest| release > latest)
}

/// The release a failed tag lookup counts against: the one the trigger named,
/// else the newer release already on record unless it is blocked (#4073),
/// else a sentinel standing for
/// "whatever is latest" (`rollback::UNKNOWN_RELEASE`), which every trigger
/// treats as pending like any newer release.
fn lookup_failure_target(
    root: &Path,
    current_version: &str,
    target: Option<&str>,
    blocked: &impl Fn(&str) -> bool,
) -> String {
    target
        .map(str::to_string)
        .or_else(|| {
            root.parent().and_then(|dir| {
                super::super::rollback::recorded_newer_release_at(dir, current_version, blocked)
            })
        })
        .unwrap_or_else(|| super::super::rollback::UNKNOWN_RELEASE.to_string())
}

/// The decision for an install that stages nothing (a macOS app bundle, which
/// updates from the DMG): the same checks as [`stage_into`], then an exit for
/// the updater, recorded first. Platform-independent so it is tested
/// everywhere.
#[cfg_attr(not(target_os = "macos"), allow(dead_code))]
async fn decide_without_staging<F, Fut>(
    root: &Path,
    current_version: &str,
    target: Option<&str>,
    lookup: F,
    (lookup_delays, byte_budget): (&[Duration], u64),
    clock: Clock<'_>,
) -> Staging
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = Result<String>>,
{
    match decide_before_download(
        root,
        current_version,
        lookup,
        target,
        version_blocked,
        (lookup_delays, byte_budget, clock),
    )
    .await
    {
        Ok(tag) => exit_after_recording(
            root,
            super::super::auto_update::version_from_tag(&tag),
            INSTALL_PENDING,
            Staging::Skipped,
            clock(),
        ),
        Err(staging) => staging,
    }
}

/// Whether the freenet archive of `tag` is in the cache and matches the
/// checksum its cached manifest lists. Existence is not enough: a power loss
/// can leave a zero-length or truncated file behind a rename that reached the
/// disk before its data did. (The installer still verifies the manifest's
/// signature before it installs anything; this decides only whether to skip
/// the download budget and whether an fdev failure may still exit to install.)
async fn freenet_archive_staged(root: &Path, tag: &str) -> bool {
    let (root, tag) = (root.to_path_buf(), tag.to_string());
    // Off the async runtime: hashing ~20 MB must not stall a serving node.
    tokio::task::spawn_blocking(move || freenet_archive_staged_blocking(&root, &tag))
        .await
        .unwrap_or(false)
}

/// [`freenet_archive_staged`] on the calling thread.
fn freenet_archive_staged_blocking(root: &Path, tag: &str) -> bool {
    let Ok(dir) = tag_dir(root, tag) else {
        return false;
    };
    let Ok(manifest) = fs::read(dir.join(MANIFEST)) else {
        return false;
    };
    let checksums = Checksums::parse(&String::from_utf8_lossy(&manifest));
    let freenet = freenet_asset_name();
    let Ok(expected) = super::required_checksum(Some(&checksums), &freenet) else {
        return false;
    };
    super::verify_checksum(&dir.join(&freenet), expected).is_ok()
}

/// Whether the node's download budget in `state_dir` is spent and no staged
/// release could be installed without downloading: whether an attempt now
/// could only be refused. Lets a #3934 locked-out node keep its daily retry
/// rather than spend it on such an attempt.
///
/// The claim runs on the async runtime, up to once a minute, so this checks
/// only that a staged archive is present and non-empty, never its checksum.
/// A non-empty archive that no longer verifies, or a staged release older
/// than the one about to be fetched, lets the claim through; staging then
/// verifies the archive and holds. The cost is one daily retry, plus the one
/// tag lookup a claiming trigger (startup, the re-poll) makes before staging,
/// never a download.
pub(crate) fn download_budget_blocks_at(state_dir: &Path, now: SystemTime) -> bool {
    let root = state_dir.join(CACHE_DIR);
    super::super::rollback::download_bytes_at(state_dir, now)
        >= super::super::rollback::DOWNLOAD_BYTES_PER_DAY
        && !staged_tag_at(&root).is_some_and(|tag| {
            tag_dir(&root, &tag).is_ok_and(|dir| {
                fs::metadata(dir.join(freenet_asset_name())).is_ok_and(|m| m.len() > 0)
            })
        })
}

/// Everything environmental [`stage_into`] depends on besides the cache root
/// (and the state directory beside it), the tag lookup and the #4073 block
/// list, so tests can drive every branch without the real state directory,
/// GitHub, or the wall clock.
struct StageEnv<'a> {
    download_prefix: &'a str,
    free: Option<u64>,
    verify: ManifestVerifier<'a>,
    delays: &'a [Duration],
    /// Waits between failed tag lookups; production passes
    /// [`LOOKUP_RETRY_DELAYS`].
    lookup_delays: &'a [Duration],
    deadline: Duration,
    clock: Clock<'a>,
    /// Archive bytes per node per day; production passes
    /// `rollback::DOWNLOAD_BYTES_PER_DAY`.
    byte_budget: u64,
}

/// Decide whether to stage and do it.
async fn stage_into<F, Fut>(
    root: &Path,
    current_version: &str,
    lookup: F,
    target: Option<&str>,
    blocked: impl Fn(&str) -> bool,
    env: &StageEnv<'_>,
) -> Staging
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = Result<String>>,
{
    let clock = env.clock;
    let tag = match decide_before_download(
        root,
        current_version,
        lookup,
        target,
        blocked,
        (env.lookup_delays, env.byte_budget, clock),
    )
    .await
    {
        Ok(tag) => tag,
        Err(staging) => return staging,
    };
    let version = super::super::auto_update::version_from_tag(&tag);
    if let Some(free) = env.free {
        if free < MIN_FREE_BYTES {
            tracing::warn!(
                free_mib = free / (1024 * 1024),
                "Too little free space to download the update in advance; it will be downloaded after exit"
            );
            return exit_after_recording(
                root,
                version,
                "too little free space",
                Staging::Skipped,
                clock(),
            );
        }
    }
    let _lock = match try_lock(root) {
        Ok(Some(lock)) => lock,
        Ok(None) => {
            tracing::info!(
                "Another process is downloading the update in advance; leaving it to that"
            );
            return exit_after_recording(root, version, "cache busy", Staging::Skipped, clock());
        }
        Err(e) => {
            tracing::warn!(error = %e, "Cannot lock the update cache; the update will be downloaded after exit");
            return exit_after_recording(
                root,
                version,
                "cache lock failed",
                Staging::Skipped,
                clock(),
            );
        }
    };
    // The hard ceiling on bytes (#5799), whatever the cooldown decided; a run
    // it refuses holds (see `budget_spent`). The poll token this run spent
    // above bounds its requests, retries included.
    let spent = root.parent().map_or(0, |dir| {
        super::super::rollback::download_bytes_at(dir, clock())
    });
    let allowance = env.byte_budget.saturating_sub(spent);
    let meter = match root.parent() {
        Some(dir) => ByteMeter::charged_to(allowance, dir, clock),
        None => ByteMeter::new(allowance),
    };
    if meter.check().is_err() && !freenet_archive_staged(root, &tag).await {
        tracing::warn!(
            tag = %tag,
            spent_mib = spent / (1024 * 1024),
            "Not downloading the update in advance: today's download budget is spent (#5799)"
        );
        return budget_spent(root, version, clock());
    }

    tracing::info!(tag = %tag, "Downloading the update before exiting, so the restart only has to install it");
    let started = tokio::time::Instant::now();
    let outcome = stage_within(
        || stage_release_at(root, env.download_prefix, &tag, env.verify, &meter),
        env.delays,
        env.deadline,
    )
    .await;
    // Whatever the outcome. Each download was charged its expected size
    // before its body arrived, and this flush reconciles that charge to the
    // bytes actually received. A run killed before this line keeps its
    // pre-charge, so it over-counts rather than under-counts.
    meter.flush();
    let error = match outcome {
        StageOutcome::Staged => {
            tracing::info!(
                "Update downloaded and verified; exiting to install it ({tag}, {}s)",
                started.elapsed().as_secs()
            );
            return exit_after_recording(root, version, INSTALL_PENDING, Staging::Staged, clock());
        }
        StageOutcome::Failed(e) if is_permanent(&e) => {
            tracing::warn!(
                tag = %tag,
                error = %format!("{e:#}"),
                "Could not download the update in advance; exiting anyway, the updater will download what is missing"
            );
            return exit_after_recording(
                root,
                version,
                &format!("{e:#}"),
                Staging::Failed,
                clock(),
            );
        }
        // A rate limit on the asset downloads is NOT recorded as a GitHub
        // cooldown: the updater would honour that and refuse to install at
        // all. It is a failure of THIS release instead, which only the node's
        // own triggers consult.
        StageOutcome::Failed(e) => e,
        StageOutcome::TimedOut => StageTimedOut(env.deadline).into(),
    };
    // #5791's rule: an fdev failure never costs the freenet archive. When
    // freenet is staged, exit to install it; the updater downloads fdev.
    // Checked before anything is logged, so this path never reports a
    // download failure for a release it is about to install.
    if freenet_archive_staged(root, &tag).await {
        tracing::info!(tag = %tag, error = %format!("{error:#}"), "The freenet archive is downloaded and verified; exiting to install it, fdev will be downloaded by the updater");
        return exit_after_recording(root, version, INSTALL_PENDING, Staging::Staged, clock());
    }
    log_transient_failure(&tag, &error);
    if failure_cause(&error) == FailureCause::BudgetSpent {
        return budget_spent(root, version, clock());
    }
    after_transient_failure(root, version, &format!("{error:#}"), clock())
}

/// Why a staging run that stays failed, for its log line.
#[derive(Debug, PartialEq, Eq)]
enum FailureCause {
    TimedOut(Duration),
    BudgetSpent,
    RateLimited,
    Unreachable,
}

fn failure_cause(e: &anyhow::Error) -> FailureCause {
    if let Some(StageTimedOut(deadline)) = e.downcast_ref::<StageTimedOut>() {
        FailureCause::TimedOut(*deadline)
    } else if e.downcast_ref::<BudgetSpent>().is_some() {
        FailureCause::BudgetSpent
    } else if e.downcast_ref::<GithubRateLimitedError>().is_some() {
        FailureCause::RateLimited
    } else {
        FailureCause::Unreachable
    }
}

/// Say why a staging run that stays failed. The download lines start with the
/// phrase the release canary (Gate B) reads, `Could not download the update
/// in advance`, and name their cause after it; the timeout's line instead
/// starts `Downloading the update in advance timed out`; the canary catches
/// a timeout through its "download started but never finished" check
/// instead. The canary's source pins check each literal.
fn log_transient_failure(tag: &str, e: &anyhow::Error) {
    match failure_cause(e) {
        FailureCause::TimedOut(deadline) => tracing::warn!(
            tag = %tag,
            deadline_secs = deadline.as_secs(),
            "Downloading the update in advance timed out; a later attempt resumes the partial download (#5799)"
        ),
        FailureCause::BudgetSpent => tracing::warn!(
            tag = %tag,
            error = %format!("{e:#}"),
            "Could not download the update in advance; today's download budget is spent (#5799)"
        ),
        FailureCause::RateLimited => tracing::warn!(
            tag = %tag,
            error = %format!("{e:#}"),
            "Could not download the update in advance; GitHub rate-limited the download (#5799)"
        ),
        FailureCause::Unreachable => tracing::warn!(
            tag = %tag,
            error = %format!("{e:#}"),
            "Could not download the update in advance; GitHub could not be reached for it (#5799)"
        ),
    }
}

/// Whether `tag` is a release worth downloading: newer than this binary, and
/// not a version this node has blocked (#4073), which the updater would refuse.
fn should_stage(tag: &str, current_version: &str, blocked: impl Fn(&str) -> bool) -> bool {
    let latest = super::super::auto_update::version_from_tag(tag);
    let newer = semver::Version::parse(latest)
        .ok()
        .zip(semver::Version::parse(current_version).ok())
        .is_some_and(|(latest, current)| latest > current);
    newer && !blocked(latest)
}

#[derive(Debug)]
enum StageOutcome {
    Staged,
    Failed(anyhow::Error),
    TimedOut,
}

async fn stage_within<F, Fut>(attempt: F, delays: &[Duration], deadline: Duration) -> StageOutcome
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = Result<()>>,
{
    match tokio::time::timeout(
        deadline,
        stage_with_retries(attempt, delays, "Downloading the update in advance"),
    )
    .await
    {
        Ok(Ok(())) => StageOutcome::Staged,
        Ok(Err(e)) => StageOutcome::Failed(e),
        Err(_) => StageOutcome::TimedOut,
    }
}

/// Run `attempt` until it succeeds, waiting about `delays[i]` (±20%) after the
/// i-th failure, and stopping early on a failure that is not
/// [`worth_retrying`]. Returns the last error when it gives up. `what` names
/// the attempt in the retry line.
async fn stage_with_retries<T, F, Fut>(mut attempt: F, delays: &[Duration], what: &str) -> Result<T>
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = Result<T>>,
{
    let mut delays = delays.iter();
    loop {
        let e = match attempt().await {
            Ok(value) => return Ok(value),
            Err(e) => e,
        };
        if !worth_retrying(&e) {
            return Err(e);
        }
        let Some(delay) = delays.next() else {
            return Err(e);
        };
        // ±20% so nodes that failed together do not retry together.
        let delay = delay.mul_f64(freenet::config::GlobalRng::random_range(0.8_f64..1.2));
        tracing::warn!(
            error = %format!("{e:#}"),
            retry_in_secs = delay.as_secs(),
            "{what} failed; will retry"
        );
        // Cancelled with the update-check task when the node shuts down.
        tokio::time::sleep(delay).await;
    }
}

/// Download `tag`'s manifest, signature and archives from `download_prefix`
/// into `root/<tag>/`, verifying the manifest signature and each archive's
/// checksum. An archive already in the cache with the right checksum is kept;
/// a partial one is resumed.
///
/// The manifest is written as soon as the freenet archive verifies, so an
/// attempt that later fails on fdev still leaves the updater a usable cache
/// for the binary that matters.
async fn stage_release_at(
    root: &Path,
    download_prefix: &str,
    tag: &str,
    verify: ManifestVerifier<'_>,
    meter: &ByteMeter<'_>,
) -> Result<()> {
    let dir = tag_dir(root, tag)?;
    remove_other_tags(root, tag);
    fs::create_dir_all(&dir).map_err(cache_io(format!("creating {}", dir.display())))?;
    let url = |name: &str| format!("{download_prefix}{tag}/{name}");

    let manifest = super::download_optional_bytes(&url(MANIFEST))
        .await
        .context("Failed to download SHA256SUMS.txt")?
        .ok_or_else(|| AssetMissing(url(MANIFEST)))?;
    let signature = super::download_optional_bytes(&url(SIGNATURE))
        .await
        .context("Failed to download SHA256SUMS.txt.sig")?;
    verify(&manifest, signature.as_deref())?;
    let checksums = Checksums::parse(&String::from_utf8_lossy(&manifest));

    let freenet = freenet_asset_name();
    fetch_verified(&checksums, &url(&freenet), &dir, &freenet, meter).await?;

    write_atomic(&dir.join(MANIFEST), &manifest)?;
    match signature {
        Some(sig) => write_atomic(&dir.join(SIGNATURE), &sig)?,
        None => match fs::remove_file(dir.join(SIGNATURE)) {
            Ok(()) => {}
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
            Err(e) => return Err(cache_io("removing a stale staged signature")(e)),
        },
    }

    // fdev is best-effort for the installer, so a release without it, or a
    // fault that will not go away, just leaves it to the updater. A network
    // failure is retried like the freenet archive's: on the links this exists
    // for, the updater's own fdev download can be killed by `TimeoutStopSec`.
    let fdev = fdev_asset_name();
    if checksums.get(&fdev).is_some() {
        if let Err(e) = fetch_verified(&checksums, &url(&fdev), &dir, &fdev, meter).await {
            if worth_retrying(&e) && e.downcast_ref::<AssetMissing>().is_none() {
                return Err(e.context("Failed to download fdev"));
            }
            tracing::warn!(error = %format!("{e:#}"), "fdev will be downloaded by the updater");
        }
    }
    Ok(())
}

async fn fetch_verified(
    checksums: &Checksums,
    url: &str,
    dir: &Path,
    name: &str,
    meter: &ByteMeter<'_>,
) -> Result<()> {
    let expected = super::required_checksum(Some(checksums), name)?.to_string();
    let dest = dir.join(name);
    if dest.exists() && verify_file(&dest, &expected).await.is_ok() {
        return Ok(());
    }
    let part = dir.join(format!("{name}.part"));
    let resumed = download_resumable(url, &part, meter).await?;
    if let Err(e) = verify_file(&part, &expected).await {
        // Never keep bytes that cannot be right.
        remove_quietly(&part);
        if !resumed {
            return Err(e);
        }
        // The bytes kept from an earlier attempt may belong to a different
        // upload of the asset. Start over once before calling the release bad.
        download_resumable(url, &part, meter).await?;
        if let Err(e) = verify_file(&part, &expected).await {
            remove_quietly(&part);
            return Err(e);
        }
    }
    fs::rename(&part, &dest).map_err(cache_io(format!("moving {name} into place")))?;
    Ok(())
}

/// [`super::verify_checksum`] off the async runtime: hashing ~20 MB must not
/// stall a node that is still serving.
async fn verify_file(path: &Path, expected: &str) -> Result<()> {
    let (path, expected) = (path.to_path_buf(), expected.to_string());
    tokio::task::spawn_blocking(move || super::verify_checksum(&path, &expected))
        .await
        .context("checksum task failed")?
}

fn remove_quietly(path: &Path) {
    #[allow(clippy::let_underscore_must_use)]
    let _ = fs::remove_file(path);
}

/// Download `url` into `part`, continuing from the bytes already there when
/// the server honours a range request. Returns whether the result builds on
/// bytes from an earlier attempt. Bounded by a stall timeout and by `meter`,
/// the node's download budget: a slow transfer that keeps making progress
/// is allowed to finish, one that passes the budget is stopped (the bytes
/// kept, so a later attempt resumes them).
async fn download_resumable(url: &str, part: &Path, meter: &ByteMeter<'_>) -> Result<bool> {
    use reqwest::StatusCode;
    use reqwest::header::{CONTENT_RANGE, RANGE};

    meter.check()?;
    let have = fs::metadata(part).map(|m| m.len()).unwrap_or(0);
    let client = reqwest::Client::builder()
        .user_agent(super::super::auto_update::GITHUB_USER_AGENT)
        .read_timeout(super::STALLED_TRANSFER_TIMEOUT)
        .build()?;
    let mut request = client.get(url);
    if have > 0 {
        request = request.header(RANGE, format!("bytes={have}-"));
    }
    let response = request.send().await.context("Failed to download file")?;
    let status = response.status();
    if super::super::auto_update::is_rate_limited_status(status) {
        return Err(super::rate_limited(&response).into());
    }

    let resumed;
    let file = if have > 0 && status == StatusCode::PARTIAL_CONTENT {
        // Append only if the server is continuing from exactly where we are.
        let resumes_here = response
            .headers()
            .get(CONTENT_RANGE)
            .and_then(|v| v.to_str().ok())
            .is_some_and(|v| v.starts_with(&format!("bytes {have}-")));
        if !resumes_here {
            remove_quietly(part);
            anyhow::bail!("server resumed the download at an unexpected offset");
        }
        resumed = true;
        tokio::fs::OpenOptions::new()
            .append(true)
            .open(part)
            .await
            .map_err(cache_io("reopening the partial download"))?
    } else if have > 0 && status == StatusCode::RANGE_NOT_SATISFIABLE {
        // The partial file already holds the whole asset; the caller's checksum
        // decides whether it is right.
        return Ok(true);
    } else if status.is_success() {
        resumed = false;
        tokio::fs::File::create(part)
            .await
            .map_err(cache_io("creating the download file"))?
    } else if status == StatusCode::NOT_FOUND {
        return Err(AssetMissing(url.to_string()).into());
    } else {
        anyhow::bail!("Download failed: {status}");
    };

    // Charged before the body, so a run killed mid-download has paid for it;
    // reconciled to what arrived once the download ends, however it ends.
    meter.precharge(response.content_length());
    let received = receive_into(response, file, part, meter).await;
    meter.flush();
    received.map(|()| resumed)
}

/// Write `response`'s body to `file` (the partial download at `part`),
/// counting it against `meter`. A local write failure (most likely a full
/// disk) gives the space back rather than keep a partial file nobody can
/// finish. A network failure keeps it, so the next attempt resumes.
async fn receive_into(
    response: reqwest::Response,
    mut file: tokio::fs::File,
    part: &Path,
    meter: &ByteMeter<'_>,
) -> Result<()> {
    use futures::StreamExt;
    use tokio::io::AsyncWriteExt;
    let mut stream = response.bytes_stream();
    let mut write_failure = None;
    let mut over_budget = None;
    while let Some(chunk) = stream.next().await {
        let chunk = chunk.context("Error while downloading")?;
        if let Err(e) = file.write_all(&chunk).await {
            write_failure = Some(("writing the download", e));
            break;
        }
        if let Err(e) = meter.spend(chunk.len()) {
            over_budget = Some(e);
            break;
        }
    }
    if write_failure.is_none() {
        if let Err(e) = file.flush().await {
            write_failure = Some(("flushing the download", e));
        }
    }
    if write_failure.is_none() {
        if let Err(e) = file.sync_all().await {
            write_failure = Some(("syncing the download", e));
        }
    }
    drop(file);
    if let Some((what, e)) = write_failure {
        remove_quietly(part);
        return Err(cache_io(what)(e));
    }
    if let Some(e) = over_budget {
        return Err(e);
    }
    Ok(())
}

fn write_atomic(path: &Path, bytes: &[u8]) -> Result<()> {
    let tmp = path.with_extension("tmp");
    fs::write(&tmp, bytes).map_err(cache_io(format!("writing {}", tmp.display())))?;
    fs::rename(&tmp, path).map_err(cache_io(format!("writing {}", path.display())))?;
    Ok(())
}

// ── Updater side: install from the cache ────────────────────────────────────

/// A release found in the cache, already verified against the signed manifest.
pub(super) struct StagedRelease {
    pub(super) checksums: Checksums,
    pub(super) freenet_archive: PathBuf,
    /// `None` when fdev was not staged or does not verify; the updater then
    /// downloads it as before.
    pub(super) fdev_archive: Option<PathBuf>,
    manifest: Vec<u8>,
}

/// The cached copy of `release`, if there is a complete one that passes the
/// same verification a fresh download must pass and still matches the
/// release's live manifest. Anything else discards the cache and returns
/// `None`, so the updater downloads instead.
pub(super) async fn load(release: &Release, quiet: bool) -> Option<StagedRelease> {
    let root = cache_root()?;
    let verify = |manifest: &[u8], signature: Option<&[u8]>| {
        super::verify_release_manifest_signature(manifest, signature, quiet)
    };
    load_at(&root, release, &verify).await
}

async fn load_at(
    root: &Path,
    release: &Release,
    verify: ManifestVerifier<'_>,
) -> Option<StagedRelease> {
    let dir = tag_dir(root, &release.tag_name).ok()?;
    if !dir.join(MANIFEST).exists() {
        return None;
    }
    let lock = match try_lock(root) {
        Ok(Some(lock)) => lock,
        Ok(None) => {
            tracing::info!(
                "The update cache is busy (another process is staging); downloading instead"
            );
            return None;
        }
        Err(e) => {
            tracing::warn!(error = %e, "Cannot lock the update cache; downloading instead");
            return None;
        }
    };
    let rejected = match verify_staged(&dir, release, verify) {
        Ok(staged) => {
            if !live_manifest_differs(release, &staged.manifest, LIVE_MANIFEST_TIMEOUT).await {
                return Some(staged);
            }
            anyhow::anyhow!("the release's manifest changed since it was downloaded")
        }
        Err(e) => e,
    };
    tracing::warn!(error = %format!("{rejected:#}"), tag = %release.tag_name, "Discarding the staged update; downloading instead");
    // Not gated on --quiet: this install now takes the slow path #5790 exists
    // to avoid, which an operator (and the release canary) should see. Not
    // `eprintln!`, which panics if stderr is closed.
    #[allow(clippy::let_underscore_must_use)]
    let _ = writeln!(
        std::io::stderr(),
        "Discarding the update downloaded in advance ({rejected:#}); downloading it again."
    );
    discard_at(root);
    drop(lock);
    None
}

/// Whether the release now serves a different manifest from the cached one,
/// which happens when its assets are re-uploaded under the same tag. Only a
/// manifest actually fetched counts: when it cannot be fetched quickly, the
/// verified cache is used, as the network path would have failed anyway.
async fn live_manifest_differs(release: &Release, cached: &[u8], timeout: Duration) -> bool {
    let Some(asset) = release.assets.iter().find(|a| a.name == MANIFEST) else {
        return false;
    };
    match super::download_optional_bytes_within(&asset.browser_download_url, timeout).await {
        Ok(Some(live)) => live != cached,
        Ok(None) => false,
        Err(e) => {
            tracing::info!(error = %e, "Could not re-check the release manifest; using the verified staged copy");
            false
        }
    }
}

fn verify_staged(
    dir: &Path,
    release: &Release,
    verify: ManifestVerifier<'_>,
) -> Result<StagedRelease> {
    let listed = |name: &str| release.assets.iter().any(|a| a.name == name);
    let freenet = freenet_asset_name();
    // Mirror the network path exactly: it would refuse a release without a
    // manifest or without our archive, so the cache must not install one.
    anyhow::ensure!(listed(MANIFEST), "release does not publish {MANIFEST}");
    anyhow::ensure!(listed(&freenet), "release does not publish {freenet}");

    let manifest = fs::read(dir.join(MANIFEST)).context("Failed to read staged manifest")?;
    // Whether a signature is required is decided by what the release
    // publishes, not by what happens to be in the cache: deleting the cached
    // signature must not downgrade a signed release to an unsigned install.
    let signature = if listed(SIGNATURE) {
        Some(
            fs::read(dir.join(SIGNATURE))
                .context("release is signed but no signature was staged")?,
        )
    } else {
        None
    };
    verify(&manifest, signature.as_deref())?;
    let checksums = Checksums::parse(&String::from_utf8_lossy(&manifest));

    let freenet_archive = dir.join(&freenet);
    super::verify_checksum(
        &freenet_archive,
        super::required_checksum(Some(&checksums), &freenet)?,
    )?;

    let fdev = fdev_asset_name();
    let fdev_archive = dir.join(&fdev);
    let fdev_ok = listed(&fdev)
        && super::required_checksum(Some(&checksums), &fdev)
            .and_then(|hash| super::verify_checksum(&fdev_archive, hash))
            .is_ok();

    Ok(StagedRelease {
        checksums,
        freenet_archive,
        fdev_archive: fdev_ok.then_some(fdev_archive),
        manifest,
    })
}

#[cfg(test)]
mod tests {
    use super::super::Asset;
    use super::*;
    use httptest::{Expectation, Server, matchers::*, responders::*};
    use sha2::{Digest, Sha256};

    const TAG: &str = "v9.9.9";

    fn sha256_hex(bytes: &[u8]) -> String {
        hex::encode(Sha256::digest(bytes))
    }

    fn manifest_for(files: &[(&str, &[u8])]) -> String {
        files
            .iter()
            .map(|(name, bytes)| format!("{}  {name}\n", sha256_hex(bytes)))
            .collect()
    }

    /// Verifier for an unsigned test release under a policy that does not
    /// require signatures, as `REQUIRE_RELEASE_SIGNATURE` currently is.
    fn unsigned(manifest: &[u8], signature: Option<&[u8]>) -> Result<()> {
        super::super::verify_manifest_signature_with(manifest, signature, &[0u8; 32], false, true)
    }

    fn test_key() -> (ed25519_dalek::SigningKey, [u8; 32]) {
        let sk = ed25519_dalek::SigningKey::from_bytes(&[42u8; 32]);
        let vk = sk.verifying_key().to_bytes();
        (sk, vk)
    }

    /// A fresh cache root inside its own temporary directory, so the lock
    /// file `try_lock` puts beside it is cleaned up with it.
    fn test_root() -> (tempfile::TempDir, PathBuf) {
        let tmp = tempfile::tempdir().unwrap();
        let root = tmp.path().join(CACHE_DIR);
        (tmp, root)
    }

    /// Port 0 cannot be connected to, so the live-manifest re-check fails at
    /// once and offline, and can never reach another test's mock server.
    fn refused_url() -> String {
        "http://127.0.0.1:0/".to_string()
    }

    fn release_listing(names: &[&str]) -> Release {
        release_listing_at(&refused_url(), names)
    }

    fn release_listing_at(url_prefix: &str, names: &[&str]) -> Release {
        Release {
            tag_name: TAG.to_string(),
            assets: names
                .iter()
                .map(|name| Asset {
                    name: name.to_string(),
                    browser_download_url: format!("{url_prefix}{name}"),
                })
                .collect(),
        }
    }

    fn asset_path(name: &str) -> String {
        format!("/{TAG}/{name}")
    }

    /// Serve `TAG`'s manifest, an optional signature (404 when `None`), and
    /// each archive exactly once.
    fn serve_release(
        manifest: &[u8],
        signature: Option<&[u8]>,
        archives: &[(&str, &[u8])],
    ) -> Server {
        let server = Server::run();
        server.expect(
            Expectation::matching(request::method_path("GET", asset_path(MANIFEST)))
                .times(..)
                .respond_with(status_code(200).body(manifest.to_vec())),
        );
        let sig =
            Expectation::matching(request::method_path("GET", asset_path(SIGNATURE))).times(..);
        server.expect(match signature {
            Some(sig_bytes) => sig.respond_with(status_code(200).body(sig_bytes.to_vec())),
            None => sig.respond_with(status_code(404)),
        });
        for (name, bytes) in archives {
            server.expect(
                Expectation::matching(request::method_path("GET", asset_path(name)))
                    .times(1)
                    .respond_with(status_code(200).body(bytes.to_vec())),
            );
        }
        server
    }

    #[tokio::test]
    async fn signed_release_round_trips_through_the_cache() {
        // The production case: every real release is signed.
        use ed25519_dalek::Signer;
        let (sk, vk) = test_key();
        let verify = |m: &[u8], s: Option<&[u8]>| {
            super::super::verify_manifest_signature_with(m, s, &vk, true, true)
        };
        let freenet = freenet_asset_name();
        let fdev = fdev_asset_name();
        let manifest = manifest_for(&[(&freenet, b"freenet bytes"), (&fdev, b"fdev bytes")]);
        let sig = sk.sign(manifest.as_bytes()).to_bytes();
        let server = serve_release(
            manifest.as_bytes(),
            Some(&sig),
            &[(&freenet, b"freenet bytes"), (&fdev, b"fdev bytes")],
        );

        let (_tmp, root) = test_root();
        // A previously staged release must not linger next to the new one.
        fs::create_dir_all(root.as_path().join("v0.0.1")).unwrap();
        stage_release_at(
            root.as_path(),
            &server.url_str("/"),
            TAG,
            &verify,
            &ByteMeter::unlimited(),
        )
        .await
        .expect("staging should succeed");
        assert!(!root.as_path().join("v0.0.1").exists());

        let signed = release_listing(&[MANIFEST, SIGNATURE, &freenet, &fdev]);
        let staged = load_at(root.as_path(), &signed, &verify)
            .await
            .expect("a signed cache should verify");
        assert_eq!(fs::read(&staged.freenet_archive).unwrap(), b"freenet bytes");
        assert_eq!(
            fs::read(staged.fdev_archive.unwrap()).unwrap(),
            b"fdev bytes"
        );
        assert_eq!(
            staged.checksums.get(&freenet),
            Some(sha256_hex(b"freenet bytes").as_str())
        );

        // Deleting the staged signature must not downgrade to unsigned.
        fs::remove_file(root.as_path().join(TAG).join(SIGNATURE)).unwrap();
        assert!(load_at(root.as_path(), &signed, &verify).await.is_none());
    }

    #[tokio::test]
    async fn restaging_a_staged_release_downloads_nothing_again() {
        // A failed install leaves the cache; the node re-stages after the
        // restart and must not fetch the archive again (`.times(1)` above).
        let freenet = freenet_asset_name();
        let manifest = manifest_for(&[(&freenet, b"archive")]);
        let server = serve_release(manifest.as_bytes(), None, &[(&freenet, b"archive")]);
        let (_tmp, root) = test_root();
        for _ in 0..2 {
            stage_release_at(
                root.as_path(),
                &server.url_str("/"),
                TAG,
                &unsigned,
                &ByteMeter::unlimited(),
            )
            .await
            .unwrap();
        }
    }

    #[tokio::test]
    async fn release_without_fdev_still_stages() {
        let freenet = freenet_asset_name();
        let manifest = manifest_for(&[(&freenet, b"archive")]);
        let server = serve_release(manifest.as_bytes(), None, &[(&freenet, b"archive")]);
        let (_tmp, root) = test_root();
        stage_release_at(
            root.as_path(),
            &server.url_str("/"),
            TAG,
            &unsigned,
            &ByteMeter::unlimited(),
        )
        .await
        .unwrap();
        let release = release_listing(&[MANIFEST, &freenet]);
        let staged = load_at(root.as_path(), &release, &unsigned).await.unwrap();
        assert!(staged.fdev_archive.is_none());
    }

    #[tokio::test]
    async fn fdev_network_failure_is_retried_but_keeps_freenet_usable() {
        let freenet = freenet_asset_name();
        let fdev = fdev_asset_name();
        let manifest = manifest_for(&[(&freenet, b"archive"), (&fdev, b"fdev")]);
        let server = serve_release(manifest.as_bytes(), None, &[(&freenet, b"archive")]);
        server.expect(
            Expectation::matching(request::method_path("GET", asset_path(&fdev)))
                .respond_with(status_code(503)),
        );
        let (_tmp, root) = test_root();
        let err = stage_release_at(
            root.as_path(),
            &server.url_str("/"),
            TAG,
            &unsigned,
            &ByteMeter::unlimited(),
        )
        .await
        .expect_err("a transient fdev failure should fail the attempt");
        assert!(worth_retrying(&err), "a 503 should be retried, got {err:#}");
        // ...but the freenet archive is already usable by the updater.
        let release = release_listing(&[MANIFEST, &freenet]);
        let staged = load_at(root.as_path(), &release, &unsigned)
            .await
            .expect("the manifest is written once freenet verifies");
        assert!(staged.fdev_archive.is_none());
    }

    #[tokio::test]
    async fn partial_download_is_resumed_not_restarted() {
        let body: &'static [u8] = b"0123456789abcdefghijklmnopqrstuvwxyz";
        let server = Server::run();
        // Only a correctly-ranged request is answered with the remainder; a
        // request that restarts from zero would get a 500 and fail the test.
        server.expect(
            Expectation::matching(all_of![
                request::method_path("GET", "/asset"),
                request::headers(contains(("range", "bytes=10-"))),
            ])
            .times(1)
            .respond_with(
                status_code(206)
                    .append_header(
                        "content-range",
                        format!("bytes 10-{}/{}", body.len() - 1, body.len()),
                    )
                    .body(&body[10..]),
            ),
        );
        let dir = tempfile::tempdir().unwrap();
        let part = dir.path().join("asset.part");
        fs::write(&part, &body[..10]).unwrap();

        let resumed = download_resumable(&server.url_str("/asset"), &part, &ByteMeter::unlimited())
            .await
            .unwrap();
        assert!(resumed);
        assert_eq!(fs::read(&part).unwrap(), body);
    }

    #[tokio::test]
    async fn resume_at_the_wrong_offset_discards_the_partial() {
        let server = Server::run();
        server.expect(
            Expectation::matching(request::method_path("GET", "/asset")).respond_with(
                status_code(206)
                    .append_header("content-range", "bytes 0-9/20")
                    .body("0123456789"),
            ),
        );
        let dir = tempfile::tempdir().unwrap();
        let part = dir.path().join("asset.part");
        fs::write(&part, b"0123456789").unwrap();

        assert!(
            download_resumable(&server.url_str("/asset"), &part, &ByteMeter::unlimited())
                .await
                .is_err()
        );
        assert!(
            !part.exists(),
            "a misaligned partial must not be appended to or kept"
        );
    }

    #[tokio::test]
    async fn range_not_satisfiable_means_the_partial_is_complete() {
        let server = Server::run();
        server.expect(
            Expectation::matching(request::method_path("GET", "/asset"))
                .respond_with(status_code(416)),
        );
        let dir = tempfile::tempdir().unwrap();
        let part = dir.path().join("asset.part");
        fs::write(&part, b"whole file").unwrap();

        let resumed = download_resumable(&server.url_str("/asset"), &part, &ByteMeter::unlimited())
            .await
            .unwrap();
        assert!(resumed);
        assert_eq!(fs::read(&part).unwrap(), b"whole file");
    }

    #[tokio::test]
    async fn server_ignoring_the_range_restarts_cleanly() {
        let body: &'static [u8] = b"the whole file";
        let server = Server::run();
        server.expect(
            Expectation::matching(request::method_path("GET", "/asset"))
                .respond_with(status_code(200).body(body)),
        );
        let dir = tempfile::tempdir().unwrap();
        let part = dir.path().join("asset.part");
        fs::write(&part, b"stale prefix that must not survive").unwrap();

        let resumed = download_resumable(&server.url_str("/asset"), &part, &ByteMeter::unlimited())
            .await
            .unwrap();
        assert!(!resumed);
        assert_eq!(fs::read(&part).unwrap(), body);
    }

    #[tokio::test]
    async fn download_rate_limit_is_typed_and_not_retried() {
        let server = Server::run();
        server.expect(
            Expectation::matching(request::method_path("GET", asset_path(MANIFEST)))
                .respond_with(status_code(429).append_header("retry-after", "120")),
        );
        let (_tmp, root) = test_root();
        let err = stage_release_at(
            root.as_path(),
            &server.url_str("/"),
            TAG,
            &unsigned,
            &ByteMeter::unlimited(),
        )
        .await
        .unwrap_err();
        let limited = err
            .downcast_ref::<GithubRateLimitedError>()
            .unwrap_or_else(|| panic!("a 429 must surface as a rate limit, got {err:#}"));
        assert_eq!(limited.retry_after, Some(Duration::from_secs(120)));
        assert!(!worth_retrying(&err));

        let archive = Server::run();
        archive.expect(
            Expectation::matching(request::method_path("GET", "/asset"))
                .respond_with(status_code(403)),
        );
        let err = download_resumable(
            &archive.url_str("/asset"),
            &root.as_path().join("a.part"),
            &ByteMeter::unlimited(),
        )
        .await
        .unwrap_err();
        assert!(err.downcast_ref::<GithubRateLimitedError>().is_some());
    }

    #[tokio::test]
    async fn corrupt_download_is_not_kept_or_retried() {
        let freenet = freenet_asset_name();
        let manifest = manifest_for(&[(&freenet, b"good bytes")]);
        let server = serve_release(manifest.as_bytes(), None, &[(&freenet, b"tampered!!")]);

        let (_tmp, root) = test_root();
        let err = stage_release_at(
            root.as_path(),
            &server.url_str("/"),
            TAG,
            &unsigned,
            &ByteMeter::unlimited(),
        )
        .await
        .expect_err("a checksum mismatch must fail staging");
        assert!(
            err.downcast_ref::<ReleaseVerificationError>().is_some(),
            "got: {err:#}"
        );
        assert!(
            !worth_retrying(&err),
            "a bad release must not be downloaded again and again"
        );
        let dir = root.as_path().join(TAG);
        assert!(!dir.join(format!("{freenet}.part")).exists());
        assert!(!dir.join(&freenet).exists());
        // No manifest either, so the updater sees no staged release at all.
        let release = release_listing(&[MANIFEST, &freenet]);
        assert!(load_at(root.as_path(), &release, &unsigned).await.is_none());
    }

    #[tokio::test]
    async fn mismatch_after_resume_restarts_once_from_zero() {
        // Bytes kept from an earlier attempt can belong to an older upload of
        // the asset; one clean download decides.
        let freenet = freenet_asset_name();
        let manifest = manifest_for(&[(&freenet, b"current upload")]);
        let server = serve_release(manifest.as_bytes(), None, &[]);
        server.expect(
            Expectation::matching(all_of![
                request::method_path("GET", asset_path(&freenet)),
                request::headers(contains(key("range"))),
            ])
            .times(1)
            .respond_with(
                status_code(206)
                    .append_header("content-range", "bytes 3-13/14")
                    .body("rent upload"),
            ),
        );
        server.expect(
            Expectation::matching(all_of![
                request::method_path("GET", asset_path(&freenet)),
                not(request::headers(contains(key("range")))),
            ])
            .times(1)
            .respond_with(status_code(200).body("current upload")),
        );
        let (_tmp, root) = test_root();
        let dir = root.as_path().join(TAG);
        fs::create_dir_all(&dir).unwrap();
        fs::write(dir.join(format!("{freenet}.part")), b"old").unwrap();

        stage_release_at(
            root.as_path(),
            &server.url_str("/"),
            TAG,
            &unsigned,
            &ByteMeter::unlimited(),
        )
        .await
        .unwrap();
        assert_eq!(fs::read(dir.join(&freenet)).unwrap(), b"current upload");
    }

    #[tokio::test]
    async fn unwritable_cache_is_not_retried() {
        let freenet = freenet_asset_name();
        let manifest = manifest_for(&[(&freenet, b"archive")]);
        let server = serve_release(manifest.as_bytes(), None, &[]);
        let (_tmp, root) = test_root();
        // A file where the tag directory should be.
        fs::create_dir_all(&root).unwrap();
        fs::write(root.as_path().join(TAG), b"").unwrap();
        let err = stage_release_at(
            root.as_path(),
            &server.url_str("/"),
            TAG,
            &unsigned,
            &ByteMeter::unlimited(),
        )
        .await
        .unwrap_err();
        assert!(err.downcast_ref::<CacheIoError>().is_some(), "got: {err:#}");
        assert!(!worth_retrying(&err));
    }

    #[tokio::test]
    async fn missing_manifest_is_retried_as_propagation_lag() {
        let server = Server::run();
        server.expect(
            Expectation::matching(request::method_path("GET", asset_path(MANIFEST)))
                .respond_with(status_code(404)),
        );
        let (_tmp, root) = test_root();
        let err = stage_release_at(
            root.as_path(),
            &server.url_str("/"),
            TAG,
            &unsigned,
            &ByteMeter::unlimited(),
        )
        .await
        .unwrap_err();
        assert!(err.downcast_ref::<AssetMissing>().is_some(), "got: {err:#}");
        assert!(worth_retrying(&err), "a release's assets can lag its tag");
    }

    #[tokio::test]
    async fn missing_or_bad_fdev_does_not_fail_staging() {
        let freenet = freenet_asset_name();
        let fdev = fdev_asset_name();
        let manifest = manifest_for(&[(&freenet, b"archive"), (&fdev, b"fdev")]);
        for fdev_response in [status_code(404), status_code(200).body("not fdev")] {
            let server = serve_release(manifest.as_bytes(), None, &[(&freenet, b"archive")]);
            server.expect(
                Expectation::matching(request::method_path("GET", asset_path(&fdev)))
                    .respond_with(fdev_response),
            );
            let (_tmp, root) = test_root();
            stage_release_at(
                root.as_path(),
                &server.url_str("/"),
                TAG,
                &unsigned,
                &ByteMeter::unlimited(),
            )
            .await
            .expect("fdev that cannot be staged is left to the updater");
        }
    }

    /// Lay out a staged release by hand, as `stage_release_at` would.
    fn write_staged(root: &Path, files: &[(&str, &[u8])]) -> PathBuf {
        let dir = root.join(TAG);
        fs::create_dir_all(&dir).unwrap();
        for (name, bytes) in files {
            fs::write(dir.join(name), bytes).unwrap();
        }
        dir
    }

    #[tokio::test]
    async fn tampered_cache_is_discarded_and_not_installed() {
        let freenet = freenet_asset_name();
        let (_tmp, root) = test_root();
        let cache = root.as_path().join(CACHE_DIR);
        let manifest = manifest_for(&[(&freenet, b"genuine")]);
        write_staged(
            &cache,
            &[(MANIFEST, manifest.as_bytes()), (&freenet, b"swapped")],
        );

        let release = release_listing(&[MANIFEST, &freenet]);
        assert!(load_at(&cache, &release, &unsigned).await.is_none());
        assert!(
            !cache.exists(),
            "a cache that fails verification must be removed"
        );
    }

    #[tokio::test]
    async fn invalid_signature_is_rejected() {
        let (_, vk) = test_key();
        let verify = |m: &[u8], s: Option<&[u8]>| {
            super::super::verify_manifest_signature_with(m, s, &vk, false, true)
        };
        let freenet = freenet_asset_name();
        let (_tmp, root) = test_root();
        let manifest = manifest_for(&[(&freenet, b"archive")]);
        write_staged(
            root.as_path(),
            &[
                (MANIFEST, manifest.as_bytes()),
                (SIGNATURE, &[7u8; 64]),
                (&freenet, b"archive"),
            ],
        );
        let signed = release_listing(&[MANIFEST, SIGNATURE, &freenet]);
        assert!(load_at(root.as_path(), &signed, &verify).await.is_none());
    }

    #[tokio::test]
    async fn cache_must_match_what_the_release_publishes() {
        let freenet = freenet_asset_name();
        let manifest = manifest_for(&[(&freenet, b"archive")]);
        let files = [
            (MANIFEST, manifest.as_bytes()),
            (freenet.as_str(), &b"archive"[..]),
        ];

        // Another tag's cache is never used...
        let (_tmp, root) = test_root();
        write_staged(root.as_path(), &files);
        let mut newer = release_listing(&[MANIFEST, &freenet]);
        newer.tag_name = "v10.0.0".to_string();
        assert!(load_at(root.as_path(), &newer, &unsigned).await.is_none());
        // ...while the matching tag loads, so the miss above is the tag.
        let matching = release_listing(&[MANIFEST, &freenet]);
        assert!(
            load_at(root.as_path(), &matching, &unsigned)
                .await
                .is_some()
        );

        // A release the network path would refuse, the cache refuses too.
        for listing in [vec![freenet.as_str()], vec![MANIFEST]] {
            let (_tmp, root) = test_root();
            write_staged(root.as_path(), &files);
            let release = release_listing(&listing);
            assert!(
                load_at(root.as_path(), &release, &unsigned).await.is_none(),
                "listing {listing:?} must not install from the cache"
            );
        }
    }

    #[tokio::test]
    async fn reuploaded_release_is_not_installed_from_an_older_cache() {
        let freenet = freenet_asset_name();
        let staged_manifest = manifest_for(&[(&freenet, b"first upload")]);
        let live_manifest = manifest_for(&[(&freenet, b"second upload")]);
        let server = Server::run();
        server.expect(
            Expectation::matching(request::method_path("GET", "/m/SHA256SUMS.txt"))
                .respond_with(status_code(200).body(live_manifest)),
        );
        let (_tmp, root) = test_root();
        write_staged(
            root.as_path(),
            &[
                (MANIFEST, staged_manifest.as_bytes()),
                (&freenet, b"first upload"),
            ],
        );
        let release = release_listing_at(&server.url_str("/m/"), &[MANIFEST, &freenet]);
        assert!(load_at(root.as_path(), &release, &unsigned).await.is_none());
        assert!(!root.as_path().join(TAG).exists());
    }

    #[tokio::test]
    async fn unreachable_live_manifest_falls_back_to_the_verified_cache() {
        let freenet = freenet_asset_name();
        let manifest = manifest_for(&[(&freenet, b"archive")]);
        let server = Server::run();
        server.expect(
            Expectation::matching(request::method_path("GET", "/m/SHA256SUMS.txt"))
                .respond_with(status_code(502)),
        );
        let (_tmp, root) = test_root();
        write_staged(
            root.as_path(),
            &[(MANIFEST, manifest.as_bytes()), (&freenet, b"archive")],
        );
        let release = release_listing_at(&server.url_str("/m/"), &[MANIFEST, &freenet]);
        assert!(load_at(root.as_path(), &release, &unsigned).await.is_some());
    }

    #[tokio::test]
    async fn unverifiable_fdev_is_left_to_the_network() {
        let freenet = freenet_asset_name();
        let fdev = fdev_asset_name();
        let (_tmp, root) = test_root();
        let manifest = manifest_for(&[(&freenet, b"archive"), (&fdev, b"fdev")]);
        write_staged(
            root.as_path(),
            &[
                (MANIFEST, manifest.as_bytes()),
                (&freenet, b"archive"),
                (&fdev, b"wrong"),
            ],
        );
        let release = release_listing(&[MANIFEST, &freenet, &fdev]);
        let staged = load_at(root.as_path(), &release, &unsigned)
            .await
            .expect("a bad fdev must not reject the freenet archive");
        assert!(staged.fdev_archive.is_none());
    }

    #[tokio::test]
    async fn live_manifest_check_is_short_and_tolerates_absence() {
        let freenet = freenet_asset_name();
        let manifest = manifest_for(&[(&freenet, b"archive")]);
        let server = Server::run();
        server.expect(
            Expectation::matching(request::method_path("GET", "/gone/SHA256SUMS.txt"))
                .respond_with(status_code(404)),
        );
        server.expect(
            Expectation::matching(request::method_path("GET", "/slow/SHA256SUMS.txt"))
                .respond_with(delay_and_then(
                    Duration::from_secs(5),
                    status_code(200).body("a different manifest"),
                )),
        );
        let gone = release_listing_at(&server.url_str("/gone/"), &[MANIFEST]);
        assert!(!live_manifest_differs(&gone, manifest.as_bytes(), LIVE_MANIFEST_TIMEOUT).await);

        // A live manifest that does not arrive in time leaves the verified cache
        // in use rather than eating the stop phase's budget.
        let slow = release_listing_at(&server.url_str("/slow/"), &[MANIFEST]);
        let started = std::time::Instant::now();
        assert!(
            !live_manifest_differs(&slow, manifest.as_bytes(), Duration::from_millis(200)).await
        );
        assert!(started.elapsed() < Duration::from_secs(4));
        // It runs inside TimeoutStopSec=45, beside a 10s probe and a 10s
        // asset-list fetch.
        assert!(LIVE_MANIFEST_TIMEOUT <= Duration::from_secs(10));
    }

    /// The environment the tests stage in: `prefix` as the download host, no
    /// free-space figure, unsigned releases, no retries, the real deadline
    /// and the real clock.
    fn test_env(prefix: &str) -> StageEnv<'_> {
        StageEnv {
            download_prefix: prefix,
            free: None,
            verify: &unsigned,
            delays: &[],
            lookup_delays: &[],
            deadline: STAGE_DEADLINE,
            clock: &SystemTime::now,
            byte_budget: super::super::super::rollback::DOWNLOAD_BYTES_PER_DAY,
        }
    }

    /// Drive `stage_into` with everything environmental fixed by the test.
    async fn stage_into_with(
        root: &Path,
        server: &Server,
        tag: Result<String>,
        free: Option<u64>,
        blocked: &dyn Fn(&str) -> bool,
    ) -> Staging {
        let prefix = server.url_str("/");
        let env = StageEnv {
            free,
            ..test_env(&prefix)
        };
        let tag = tag.map_err(|e| format!("{e:#}"));
        stage_into(
            root,
            "9.9.8",
            move || {
                let tag = tag.clone();
                async move { tag.map_err(anyhow::Error::msg) }
            },
            None,
            blocked,
            &env,
        )
        .await
    }

    /// Drive `stage_into` for `TAG` against `prefix`, no retries.
    async fn stage_once(root: &Path, prefix: &str, deadline: Duration) -> Staging {
        let env = StageEnv {
            deadline,
            ..test_env(prefix)
        };
        stage_into(
            root,
            "9.9.8",
            || async { Ok(TAG.to_string()) },
            None,
            |_: &str| false,
            &env,
        )
        .await
    }

    /// The cooldown record beside `root`, if any.
    fn cooldown(root: &Path) -> Option<super::super::super::rollback::UpdateCooldownState> {
        super::super::super::rollback::read_update_cooldown_at(root.parent().unwrap())
    }

    /// A server whose release archive always answers 500, as a network where
    /// GitHub's asset downloads fail does. Counts the archive requests.
    fn failing_release() -> Server {
        let freenet = freenet_asset_name();
        let manifest = manifest_for(&[(&freenet, b"archive")]);
        let server = serve_release(manifest.as_bytes(), None, &[]);
        server.expect(
            Expectation::matching(request::method_path("GET", asset_path(&freenet)))
                .times(..)
                .respond_with(status_code(500)),
        );
        server
    }

    #[tokio::test]
    async fn stage_into_stages_a_newer_release() {
        let freenet = freenet_asset_name();
        let manifest = manifest_for(&[(&freenet, b"archive")]);
        let server = serve_release(manifest.as_bytes(), None, &[(&freenet, b"archive")]);
        let (_tmp, root) = test_root();
        let outcome = stage_into_with(
            root.as_path(),
            &server,
            Ok(TAG.into()),
            // Exactly the threshold is enough.
            Some(MIN_FREE_BYTES),
            &|_| false,
        )
        .await;
        assert_eq!(outcome, Staging::Staged);
        assert!(outcome.preparation().should_exit());
        let release = release_listing(&[MANIFEST, &freenet]);
        assert!(load_at(root.as_path(), &release, &unsigned).await.is_some());
        // Recorded before the exit, provisionally; the install clears it.
        let record = cooldown(&root).unwrap();
        assert_eq!(
            (record.last_error.as_str(), record.failures),
            (INSTALL_PENDING, 1)
        );
        assert!(record.provisional);
        // An updater that then fails this install replaces the record at the
        // same count: one attempt, one failure.
        let mut out = Vec::new();
        super::super::note_install_failure_at(
            root.parent(),
            "9.9.9",
            "the install failed",
            SystemTime::now(),
            &mut out,
        );
        let record = cooldown(&root).unwrap();
        assert_eq!(
            (record.last_error.as_str(), record.failures),
            ("the install failed", 1)
        );
    }

    #[tokio::test]
    async fn stage_into_skips_without_downloading() {
        // No expectations: any request to this server fails the test.
        let server = Server::run();
        let (_tmp, root) = test_root();
        let none = |_: &str| false;
        type Case<'a> = (
            &'a str,
            Result<String>,
            Option<u64>,
            &'a dyn Fn(&str) -> bool,
        );
        // `stage_into_with` names no target, as the version-less triggers do:
        // with nothing to install they stay (#5799), since the updater
        // would find nothing to do.
        let cases: [(Case<'_>, Staging); 3] = [
            (
                ("not newer", Ok("v9.9.8".into()), None, &none),
                Staging::NothingToInstall,
            ),
            (
                ("blocked", Ok(TAG.into()), None, &|v| v == "9.9.9"),
                Staging::NothingToInstall,
            ),
            (
                ("low disk", Ok(TAG.into()), Some(MIN_FREE_BYTES - 1), &none),
                Staging::Skipped,
            ),
        ];
        for ((case, tag, free, blocked), expected) in cases {
            let outcome = stage_into_with(root.as_path(), &server, tag, free, blocked).await;
            assert_eq!(outcome, expected, "{case}");
            assert_eq!(
                outcome.preparation().should_exit(),
                expected == Staging::Skipped,
                "{case}"
            );
        }
        // Only the low-disk exit had a release to install, and it records
        // first, in case the updater is killed before it can (#5799).
        assert_eq!(cooldown(&root).map(|r| r.failures), Some(1));
        // A trigger that named a release stays too, and holds that release
        // without counting, so the next trigger does not name it again at
        // once.
        let (_tmp, root) = test_root();
        let prefix = server.url_str("/");
        let outcome = stage_into(
            &root,
            "9.9.8",
            || async { Ok("v9.9.8".to_string()) },
            Some("9.9.9"),
            |_: &str| false,
            &test_env(&prefix),
        )
        .await;
        assert_eq!(outcome, Staging::NothingToInstall);
        assert!(!outcome.preparation().should_exit());
        let record = cooldown(&root).expect("held");
        assert_eq!((record.version.as_str(), record.failures), ("9.9.9", 0));
        assert!(record.hold_reason.is_some());
        assert!(
            super::super::super::rollback::update_cooldown_pending_at(
                root.parent().unwrap(),
                "9.9.8",
                SystemTime::now()
            )
            .is_some(),
            "the triggers hold"
        );
    }

    /// `stage_latest_release` is the only entry point the node calls; the
    /// marker and the panic containment must be in it, not just exist.
    #[test]
    fn entry_point_logs_the_canary_marker_and_contains_panics() {
        let src = include_str!("staged.rs").replace("\r\n", "\n");
        let production = src
            .split_once(concat!("#[cfg(test)]\n", "mod tests {"))
            .expect("test module not found")
            .0;
        let start = production
            .find("pub(crate) async fn stage_latest_release(")
            .expect("entry point not found");
        let body = &production[start..];
        let body = &body[body.find(" {\n").expect("body")..];
        let body = &body[..body.find("\n}\n").expect("end of entry point")];
        let statements: Vec<&str> = body
            .lines()
            .map(str::trim)
            .filter(|l| !l.is_empty() && !l.starts_with("//"))
            .collect();
        assert_eq!(
            statements[1..],
            [
                "tracing::info!(\"Preparing the update before exiting (#5790)\");",
                "contain_panic(stage_latest_release_inner(current_version, target), || {",
                "record_panic(target)",
                "})",
                ".await",
            ],
            "stage_latest_release must log the marker, then run staging inside contain_panic"
        );
    }

    /// The environment the node really stages in, field by field, and the
    /// wrappers that find the cache through the shared state directory (here
    /// a per-thread temporary one, never the developer's real directory).
    #[test]
    fn the_node_stages_in_the_production_environment() {
        use super::super::super::auto_update::TestStateDir;
        use super::super::super::rollback::{
            DOWNLOAD_BYTES_PER_DAY, pin_known_bad_at, read_update_cooldown_at,
        };
        let state = TestStateDir::new();
        let root = cache_root().expect("the cache sits in the state directory");
        assert_eq!(root, state.path().join(CACHE_DIR));
        let env = production_env(&root);
        assert_eq!(env.download_prefix, RELEASE_DOWNLOAD_PREFIX);
        // The state directory's free space. Other tests write to the same
        // filesystem meanwhile, so the two readings are compared loosely.
        let close = |a: Option<u64>, b: Option<u64>| match (a, b) {
            (Some(a), Some(b)) => a.abs_diff(b) < 1 << 30,
            (a, b) => a == b,
        };
        assert!(close(env.free, free_bytes(state.path())));
        assert_eq!(env.free.is_some(), cfg!(unix));
        assert_eq!(env.delays, STAGE_RETRY_DELAYS);
        assert_eq!(env.lookup_delays, LOOKUP_RETRY_DELAYS);
        assert_eq!(env.deadline, STAGE_DEADLINE);
        assert_eq!(env.byte_budget, DOWNLOAD_BYTES_PER_DAY);
        let wall = SystemTime::now();
        let clock = (env.clock)();
        let skew = clock.duration_since(wall).unwrap_or_else(|e| e.duration());
        assert!(skew < Duration::from_secs(60), "the wall clock");
        // The real signature check: a forged signature is refused.
        assert!((env.verify)(b"manifest", Some(&[0u8; 64])).is_err());
        // The #4073 block list the node passes reads the state directory.
        assert!(!version_blocked("9.9.9"));
        pin_known_bad_at(state.path(), "9.9.9").unwrap();
        assert!(version_blocked("9.9.9"));
        // A contained panic records against the state directory.
        record_panic(Some("9.9.10"));
        let record = read_update_cooldown_at(state.path()).unwrap();
        assert!(record.last_error.contains("panicked"));
        // An updater skip holds the staged release there.
        write_staged(&root, &[(MANIFEST, b"m")]);
        assert!(is_staged(TAG));
        assert!(!is_staged("v9.9.10"));
        hold_staged_release("skipped");
        let record = read_update_cooldown_at(state.path()).unwrap();
        assert_eq!((record.version.as_str(), record.failures), ("9.9.9", 0));
    }

    /// The node passes its real #4073 block list to both staging paths: the
    /// archives (`stage_into`) and the app bundle (`decide_without_staging`).
    /// `version_blocked` itself is checked against the state directory above.
    #[test]
    fn the_node_stages_with_its_block_list() {
        let src = include_str!("staged.rs").replace("\r\n", "\n");
        let production = src
            .split_once(concat!("#[cfg(test)]\n", "mod tests {"))
            .unwrap()
            .0;
        let body = |signature: &str| -> String {
            let start = production
                .find(signature)
                .unwrap_or_else(|| panic!("{signature}"));
            let rest = &production[start..];
            rest[..rest.find("\n}\n").unwrap()]
                .lines()
                .filter(|l| !l.trim_start().starts_with("//"))
                .collect::<String>()
                .split_whitespace()
                .collect()
        };
        let inner = body("\nasync fn stage_latest_release_inner(");
        assert!(
            inner.contains(
                "stage_into(&root,current_version,lookup,target,version_blocked,&production_env(&root),)"
            ),
            "{inner}"
        );
        let bundle = body("\nasync fn decide_without_staging<");
        assert!(bundle.contains("target,version_blocked,("), "{bundle}");
    }

    /// The byte budget covers every download in a staging run: fdev's, and
    /// the from-zero re-download after a resumed archive fails its checksum.
    #[tokio::test]
    async fn the_byte_budget_covers_fdev_and_the_restarted_download() {
        use super::super::super::rollback::download_bytes_at;
        let freenet = freenet_asset_name();
        let fdev = fdev_asset_name();
        let good = b"the freenet archive".to_vec();
        let fdev_body = vec![b'f'; 5000];
        // fdev's listed checksum never matches, so it is fetched once, counted
        // and left to the updater.
        let manifest = manifest_for(&[(&freenet, &good), (&fdev, b"the real fdev")]);
        let server = serve_release(manifest.as_bytes(), None, &[(&fdev, &fdev_body)]);
        // The freenet archive: a partial from an earlier upload is resumed
        // (wrong bytes), then downloaded again from zero.
        server.expect(
            Expectation::matching(all_of![
                request::method_path("GET", asset_path(&freenet)),
                request::headers(contains(("range", "bytes=4-"))),
            ])
            .times(1)
            .respond_with(
                status_code(206)
                    .append_header(
                        "content-range",
                        format!("bytes 4-{}/{}", good.len() - 1, good.len()),
                    )
                    .body(vec![b'x'; good.len() - 4]),
            ),
        );
        server.expect(
            Expectation::matching(all_of![
                request::method_path("GET", asset_path(&freenet)),
                not(request::headers(contains(key("range")))),
            ])
            .times(1)
            .respond_with(status_code(200).body(good.clone())),
        );
        let (_tmp, root) = test_root();
        let dir = root.parent().unwrap();
        fs::create_dir_all(root.join(TAG)).unwrap();
        fs::write(root.join(TAG).join(format!("{freenet}.part")), b"old!").unwrap();
        let now = SystemTime::now();
        let clock = move || now;
        let meter = ByteMeter::charged_to(u64::MAX, dir, &clock);
        stage_release_at(&root, &server.url_str("/"), TAG, &unsigned, &meter)
            .await
            .expect("staged, fdev left to the updater");
        meter.flush();
        let resumed = (good.len() - 4) as u64;
        assert_eq!(
            download_bytes_at(dir, now),
            resumed + good.len() as u64 + fdev_body.len() as u64
        );
    }

    /// With today's budget spent and the freenet archive already staged, the
    /// run exits to install it without requesting fdev either.
    #[tokio::test]
    async fn a_spent_budget_requests_no_fdev_and_still_installs() {
        use super::super::super::rollback::charge_download_bytes_at;
        let freenet = freenet_asset_name();
        let fdev = fdev_asset_name();
        let manifest = manifest_for(&[(&freenet, b"archive"), (&fdev, b"fdev")]);
        // Manifest and signature only: an archive request fails the test.
        let server = serve_release(manifest.as_bytes(), None, &[]);
        let (_tmp, root) = test_root();
        write_staged(
            &root,
            &[(MANIFEST, manifest.as_bytes()), (&freenet, b"archive")],
        );
        let now = SystemTime::now();
        charge_download_bytes_at(root.parent().unwrap(), u64::MAX, now);
        assert_eq!(
            stage_at(&root, &server.url_str("/"), now).await,
            Staging::Staged
        );
    }

    #[test]
    fn tags_that_are_not_plain_path_components_are_refused() {
        let root = Path::new("/cache");
        for bad in ["", ".", "..", "../etc", "v1/../../x", "v1\\x", ".hidden"] {
            assert!(tag_dir(root, bad).is_err(), "{bad:?} must be refused");
        }
        assert_eq!(tag_dir(root, "v0.2.141").unwrap(), root.join("v0.2.141"));
    }

    #[test]
    fn only_newer_unblocked_releases_are_staged() {
        let none = |_: &str| false;
        assert!(should_stage("v0.2.142", "0.2.141", none));
        assert!(!should_stage("v0.2.141", "0.2.141", none));
        assert!(!should_stage("v0.2.140", "0.2.141", none));
        assert!(!should_stage("not-a-version", "0.2.141", none));
        assert!(!should_stage("v0.2.142", "0.2.141", |v| v == "0.2.142"));
    }

    #[test]
    fn stale_staged_releases_are_removed_at_startup() {
        let (_tmp, root) = test_root();
        for tag in ["v0.2.140", "v0.2.141", "v0.2.142", "junk"] {
            fs::create_dir_all(root.as_path().join(tag)).unwrap();
        }
        fs::create_dir_all(root.as_path().join("v0.2.143")).unwrap();
        discard_stale_at(root.as_path(), "0.2.141", |v| v == "0.2.143");
        let mut left: Vec<_> = fs::read_dir(root.as_path())
            .unwrap()
            .map(|e| e.unwrap().file_name().into_string().unwrap())
            .collect();
        left.sort();
        assert_eq!(left, ["v0.2.142"]);
    }

    /// Another process holds the cache lock: staging must skip without
    /// downloading, and the lock must be free once that process exits. The
    /// holder is this test binary re-executed (see
    /// `rollback::tests::LockHelper`), which inherits none of this process's
    /// descriptors, unlike a forked child.
    #[tokio::test]
    #[cfg(unix)]
    async fn cache_lock_excludes_another_process() {
        use super::super::super::rollback::tests::LockHelper;
        let (_tmp, root) = test_root();
        fs::create_dir_all(root.parent().unwrap()).unwrap();
        let child = LockHelper::hold(&root.with_extension("lock"), None);

        assert!(
            try_lock(&root).unwrap().is_none(),
            "a lock held by another process must be refused"
        );
        let server = Server::run(); // no expectations: staging must not download
        let outcome = stage_into_with(&root, &server, Ok(TAG.into()), None, &|_| false).await;
        assert_eq!(outcome, Staging::Skipped);

        child.release();
        assert!(
            try_lock(&root).unwrap().is_some(),
            "the lock must be free once its holder exits"
        );
    }

    #[test]
    fn cache_lock_excludes_this_process_too() {
        let (_tmp, root) = test_root();
        let held = try_lock(&root).unwrap().expect("first lock");
        assert!(
            try_lock(&root).unwrap().is_none(),
            "a second holder in the same process must be refused"
        );
        drop(held);
        assert!(try_lock(&root).unwrap().is_some());
    }

    /// `stage_into` must hold the lock for the whole download, not just take
    /// it: binding it to `_` would release it at once.
    #[test]
    fn stage_into_holds_the_lock_while_downloading() {
        let src = include_str!("staged.rs").replace("\r\n", "\n");
        let production = src
            .split_once(concat!("#[cfg(test)]\n", "mod tests {"))
            .unwrap()
            .0;
        let start = production.find("async fn stage_into<").unwrap();
        let body = &production[start..];
        let body = &body[..body.find("\n}\n").unwrap()];
        assert!(
            body.lines()
                .any(|l| l.trim().starts_with("let _lock = match try_lock(root) {")),
            "stage_into must keep its lock guard alive"
        );
        assert!(
            !production.contains(concat!("record_github", "_cooldown")),
            "staging must not record a cooldown the updater would then obey"
        );
    }

    // Real time: these talk to local sockets, which a paused clock would
    // time out before they answer.
    /// #5799 regression. A download that fails because GitHub cannot be
    /// reached used to end in "exiting anyway": exit 42, the updater fails the
    /// same download, the supervisor restarts the old binary, which detects the
    /// same release and exits again, for as long as the network stays bad. Now
    /// the node stays, and the release cools down so nothing tries it again
    /// meanwhile; the third failure in a row exits for the updater.
    #[tokio::test]
    async fn a_network_failure_stays_cools_down_and_falls_back_on_the_third() {
        let (_tmp, root) = test_root();
        let server = failing_release();
        let outcome = stage_once(&root, &server.url_str("/"), STAGE_DEADLINE).await;
        assert_eq!(outcome, Staging::Deferred);
        assert!(!outcome.preparation().should_exit());
        let record = cooldown(&root).expect("recorded");
        assert_eq!((record.version.as_str(), record.failures), ("9.9.9", 1));
        assert!(record.last_error.contains("500"), "{}", record.last_error);

        // The next attempt holds without a single request.
        let quiet = Server::run();
        let outcome = stage_once(&root, &quiet.url_str("/"), STAGE_DEADLINE).await;
        assert_eq!(outcome, Staging::CoolingDown);
        assert!(!outcome.preparation().should_exit());

        // Expire it and fail again: still staying at two.
        let state_dir = root.parent().unwrap();
        super::super::super::rollback::clear_update_cooldown_at(state_dir);
        let long_ago = SystemTime::now() - Duration::from_secs(5 * 3600);
        for _ in 0..FALLBACK_AFTER_FAILURES - 2 {
            super::super::super::rollback::record_update_failure_at(
                state_dir, "9.9.9", "earlier", long_ago, 0.5,
            );
        }
        let outcome = stage_once(&root, &server.url_str("/"), STAGE_DEADLINE).await;
        assert_eq!(outcome, Staging::Deferred);
        // And the third exits so the updater tries its own download.
        super::super::super::rollback::record_update_failure_at(
            state_dir, "9.9.9", "earlier", long_ago, 0.5,
        );
        let outcome = stage_once(&root, &server.url_str("/"), STAGE_DEADLINE).await;
        assert_eq!(outcome, Staging::FallingBack);
        assert!(outcome.preparation().should_exit());
        assert_eq!(FALLBACK_AFTER_FAILURES, 3);
    }

    #[tokio::test]
    async fn a_timeout_stays_and_records() {
        let never = Server::run();
        never.expect(
            Expectation::matching(any())
                .times(..)
                .respond_with(delay_and_then(Duration::from_secs(3600), status_code(200))),
        );
        let (_tmp, root) = test_root();
        let outcome = stage_once(&root, &never.url_str("/"), Duration::from_millis(1)).await;
        assert_eq!(outcome, Staging::Deferred);
        assert_eq!(cooldown(&root).map(|r| r.failures), Some(1));
        let error = cooldown(&root).unwrap().last_error;
        assert!(error.contains("did not finish"), "{error}");
    }

    /// A failure that cannot be recorded would never reach the fallback, so
    /// the node fails open and exits.
    #[tokio::test]
    async fn an_unrecordable_failure_exits() {
        let (tmp, root) = test_root();
        fs::create_dir_all(tmp.path().join("update_cooldown.json")).unwrap();
        let outcome = stage_once(&root, &failing_release().url_str("/"), STAGE_DEADLINE).await;
        assert_eq!(outcome, Staging::FallingBack);
    }

    /// A release that fails verification, or a cache that cannot be written,
    /// is not a network problem: exit for the updater (which counts
    /// verification failures toward the #4073 gate), recording first.
    #[tokio::test]
    async fn permanent_failures_exit_and_record() {
        let freenet = freenet_asset_name();
        let manifest = manifest_for(&[(&freenet, b"good bytes")]);
        let server = serve_release(manifest.as_bytes(), None, &[(&freenet, b"tampered!!")]);
        let (_tmp, root) = test_root();
        let outcome = stage_once(&root, &server.url_str("/"), STAGE_DEADLINE).await;
        assert_eq!(outcome, Staging::Failed);
        assert!(outcome.preparation().should_exit());
        assert_eq!(cooldown(&root).map(|r| r.failures), Some(1));

        let tmp = tempfile::tempdir().unwrap();
        let file_root = tmp.path().join(CACHE_DIR);
        fs::write(&file_root, b"not a directory").unwrap();
        let outcome = stage_once(&file_root, &failing_release().url_str("/"), STAGE_DEADLINE).await;
        assert_eq!(outcome, Staging::Failed);
        assert_eq!(cooldown(&file_root).map(|r| r.failures), Some(1));
    }

    /// A restart after a staged exit whose install never reported back holds
    /// instead of exiting again within seconds.
    #[tokio::test]
    async fn a_restart_after_a_staged_exit_holds() {
        let freenet = freenet_asset_name();
        let manifest = manifest_for(&[(&freenet, b"archive")]);
        let server = serve_release(manifest.as_bytes(), None, &[(&freenet, b"archive")]);
        let (_tmp, root) = test_root();
        assert_eq!(
            stage_once(&root, &server.url_str("/"), STAGE_DEADLINE).await,
            Staging::Staged
        );
        let quiet = Server::run();
        assert_eq!(
            stage_once(&root, &quiet.url_str("/"), STAGE_DEADLINE).await,
            Staging::CoolingDown
        );
    }

    /// #5791's rule: an fdev failure never costs the freenet archive.
    #[tokio::test]
    async fn an_fdev_only_failure_still_installs_the_staged_freenet() {
        let freenet = freenet_asset_name();
        let fdev = fdev_asset_name();
        let manifest = manifest_for(&[(&freenet, b"archive"), (&fdev, b"fdev")]);
        let server = serve_release(manifest.as_bytes(), None, &[(&freenet, b"archive")]);
        server.expect(
            Expectation::matching(request::method_path("GET", asset_path(&fdev)))
                .times(..)
                .respond_with(status_code(500)),
        );
        let (_tmp, root) = test_root();
        let outcome = stage_once(&root, &server.url_str("/"), STAGE_DEADLINE).await;
        assert_eq!(outcome, Staging::Staged);
        assert_eq!(cooldown(&root).unwrap().last_error, INSTALL_PENDING);
    }

    /// A failed tag lookup stays: the updater's first step is the same lookup.
    /// It counts toward the fallback whether or not the trigger knew a
    /// release (#5799): against the named one, else the newer one on
    /// record, else a sentinel that every trigger treats as pending.
    #[tokio::test]
    async fn a_failed_tag_lookup_stays_and_counts_toward_the_fallback() {
        use super::super::super::rollback::{UNKNOWN_RELEASE, record_update_failure_at};
        let server = Server::run();
        let prefix = server.url_str("/");
        let run = |root: PathBuf, target: Option<&'static str>, now: SystemTime| {
            let prefix = prefix.clone();
            async move {
                let clock = move || now;
                let env = StageEnv {
                    clock: &clock,
                    ..test_env(&prefix)
                };
                stage_into(
                    &root,
                    "9.9.8",
                    || async { Err::<String, _>(anyhow::anyhow!("offline")) },
                    target,
                    |_: &str| false,
                    &env,
                )
                .await
            }
        };
        let (_tmp, root) = test_root();
        let t0 = SystemTime::now();
        let hour = Duration::from_secs(3600);
        assert_eq!(run(root.clone(), None, t0).await, Staging::Deferred);
        let record = cooldown(&root).unwrap();
        assert_eq!(
            (record.version.as_str(), record.failures),
            (UNKNOWN_RELEASE, 1)
        );
        // Pending like any newer release, so the next trigger holds.
        assert_eq!(run(root.clone(), None, t0).await, Staging::CoolingDown);
        assert_eq!(
            run(root.clone(), None, t0 + hour * 2).await,
            Staging::Deferred
        );
        assert_eq!(
            run(root.clone(), None, t0 + hour * 5).await,
            Staging::FallingBack
        );
        // A newer release already on record is the one it counts against.
        let (_tmp, root) = test_root();
        let state_dir = root.parent().unwrap();
        fs::create_dir_all(state_dir).unwrap();
        record_update_failure_at(state_dir, "9.9.9", "earlier", t0 - hour * 5, 0.5);
        assert_eq!(run(root.clone(), None, t0).await, Staging::Deferred);
        let record = cooldown(&root).unwrap();
        assert_eq!((record.version.as_str(), record.failures), ("9.9.9", 2));
        // A named release is counted directly.
        let (_tmp, root) = test_root();
        assert_eq!(
            run(root.clone(), Some("9.9.9"), t0).await,
            Staging::Deferred
        );
        assert_eq!(cooldown(&root).map(|r| r.failures), Some(1));
    }

    /// A locked-out node (#3934) whose version-less trigger may claim, and
    /// whose staging then decides to exit, can spend its claim. Staging
    /// records the release before it says to exit, so a claim that consulted
    /// the release cooldown would be refused by that very record: the node
    /// would download, record an extra failure, and stay.
    #[tokio::test]
    async fn a_locked_out_node_can_claim_the_exit_staging_decided_on() {
        use super::super::super::auto_update as au;
        let freenet = freenet_asset_name();
        let manifest = manifest_for(&[(&freenet, b"archive")]);
        let server = serve_release(manifest.as_bytes(), None, &[(&freenet, b"archive")]);
        let (_tmp, root) = test_root();
        let state_dir = root.parent().unwrap();
        fs::create_dir_all(state_dir).unwrap();
        // MAX_UPDATE_FAILURES failed installs lock the node out; the claimable
        // check below fails if that number changes.
        for _ in 0..3 {
            au::record_update_failure_at(state_dir);
        }
        let stamp = fs::metadata(state_dir.join("update_failures"))
            .unwrap()
            .modified()
            .unwrap();
        let now = stamp + Duration::from_secs(24 * 3600);
        assert!(au::update_attempt_claimable_at(state_dir, "9.9.8", now));
        let clock = move || now;
        let prefix = server.url_str("/");
        let env = StageEnv {
            clock: &clock,
            ..test_env(&prefix)
        };
        let outcome = stage_into(
            &root,
            "9.9.8",
            || async { Ok(TAG.to_string()) },
            None,
            |_: &str| false,
            &env,
        )
        .await;
        assert_eq!(outcome, Staging::Staged);
        assert!(outcome.preparation().should_exit());
        assert_eq!(
            au::claim_update_attempt_unless_cooling_at(state_dir, "9.9.8", now).err(),
            Some(au::ClaimRefused::ReleaseCoolingDown),
            "the record staging wrote starts a cooldown"
        );
        assert!(au::claim_update_attempt_at(state_dir, now).is_ok());
        assert!(
            au::claim_update_attempt_at(state_dir, now).is_err(),
            "the retry is spent"
        );
        // The node's post-staging claim is that one.
        let src = include_str!("../auto_update.rs").replace("\r\n", "\n");
        let start = src
            .find("pub fn claim_update_attempt_after_staging(")
            .unwrap();
        let body = &src[start..];
        let flat: String = body[..body.find("\n}\n").unwrap()]
            .split_whitespace()
            .collect();
        assert!(flat.contains("Some(dir)=>claim_update_attempt_at(&dir,SystemTime::now()),"));
    }

    /// #5799: while any newer release cools down, staging stops
    /// before it spends a poll token or looks the tag up, whether or not the
    /// trigger named a release.
    #[tokio::test]
    async fn a_cooling_release_spends_no_token_and_no_lookup() {
        let server = Server::run();
        let prefix = server.url_str("/");
        let (_tmp, root) = test_root();
        let state_dir = root.parent().unwrap();
        fs::create_dir_all(state_dir).unwrap();
        let now = SystemTime::now();
        super::super::super::rollback::record_update_failure_at(state_dir, "9.9.9", "x", now, 0.5);
        for target in [None, Some("9.9.9"), Some("9.9.10")] {
            let outcome = stage_into(
                &root,
                "9.9.8",
                || async { panic!("looked the tag up while a release cools down") },
                target,
                |_: &str| false,
                &test_env(&prefix),
            )
            .await;
            assert_eq!(outcome, Staging::CoolingDown, "{target:?}");
        }
        assert!(
            !state_dir.join("github_poll_bucket_node").exists(),
            "no poll token was spent"
        );
    }

    /// Stage `TAG` at the fixed time `now` against `prefix`, for a trigger
    /// that knew the release, as the update loop's triggers do.
    async fn stage_at(root: &Path, prefix: &str, now: SystemTime) -> Staging {
        let clock = move || now;
        let env = StageEnv {
            clock: &clock,
            ..test_env(prefix)
        };
        stage_into(
            root,
            "9.9.8",
            || async { Ok(TAG.to_string()) },
            Some("9.9.9"),
            |_: &str| false,
            &env,
        )
        .await
    }

    /// A release on record is withdrawn after three failures, and the latest
    /// is an older one that fails too. Its failure writes its own cooldown,
    /// with a fresh count, instead of reusing the withdrawn release's count:
    /// otherwise nothing would space the attempts, and every one would exit.
    #[tokio::test]
    async fn a_withdrawn_release_on_record_does_not_suppress_the_cooldown() {
        use super::super::super::rollback::{
            record_update_failure_at, update_cooldown_remaining_at,
        };
        let (_tmp, root) = test_root();
        let state_dir = root.parent().unwrap();
        fs::create_dir_all(state_dir).unwrap();
        let t0 = SystemTime::now();
        let hour = Duration::from_secs(3600);
        for _ in 0..FALLBACK_AFTER_FAILURES {
            record_update_failure_at(state_dir, "9.9.10", "earlier", t0 - hour * 6, 0.5);
        }
        let server = failing_release();
        assert_eq!(
            stage_at(&root, &server.url_str("/"), t0).await,
            Staging::Deferred
        );
        let record = cooldown(&root).unwrap();
        assert_eq!((record.version.as_str(), record.failures), ("9.9.9", 1));
        assert!(update_cooldown_remaining_at(state_dir, "9.9.9", t0).is_some());
        let quiet = Server::run();
        assert_eq!(
            stage_at(&root, &quiet.url_str("/"), t0).await,
            Staging::CoolingDown
        );
    }

    /// A run writes a hold before it first reaches the network, so a node
    /// killed mid-run (a crash, a stop mid-download) restarts into a cooldown
    /// instead of starting the download over at once. The hold counts nothing.
    #[tokio::test]
    async fn a_run_holds_the_release_before_it_reaches_the_network() {
        use super::super::super::rollback::{UNKNOWN_RELEASE, read_update_cooldown_at};
        let server = Server::run(); // no expectations: nothing is downloaded
        let prefix = server.url_str("/");
        for (target, held) in [(Some("9.9.9"), "9.9.9"), (None, UNKNOWN_RELEASE)] {
            let (_tmp, root) = test_root();
            let dir = root.parent().unwrap().to_path_buf();
            let mut seen = None;
            let outcome = stage_into(
                &root,
                "9.9.8",
                || {
                    seen = Some(read_update_cooldown_at(&dir));
                    async { Err::<String, _>(anyhow::anyhow!("killed here")) }
                },
                target,
                |_: &str| false,
                &test_env(&prefix),
            )
            .await;
            assert_eq!(outcome, Staging::Deferred, "{target:?}");
            let record = seen.flatten().expect("held before the lookup");
            assert_eq!(
                (record.version.as_str(), record.failures),
                (held, 0),
                "{target:?}"
            );
            assert!(record.hold_reason.is_some());
        }
    }

    /// While a download is in progress the release being downloaded is held,
    /// even when the lookup's clear removed the run's first hold (the
    /// sentinel's, or a withdrawn named release's), so a run killed then
    /// restarts into a cooldown.
    #[tokio::test]
    async fn a_download_in_progress_is_held_for_the_resolved_release() {
        use super::super::tests::{partial_server, run_until};
        for target in [None, Some("9.9.10")] {
            // Small, so the manifest download (the first request) stalls
            // rather than being refused for its size.
            let server = partial_server(Some(2000), 100, true).await;
            let (_tmp, root) = test_root();
            let env = test_env(&server.prefix);
            let requests = server.requests.clone();
            run_until(
                stage_into(
                    &root,
                    "9.9.8",
                    || async { Ok(TAG.to_string()) },
                    target,
                    |_: &str| false,
                    &env,
                ),
                || requests.load(std::sync::atomic::Ordering::SeqCst) > 0,
            )
            .await;
            let record = cooldown(&root).expect("held while downloading");
            assert_eq!(
                (record.version.as_str(), record.failures),
                ("9.9.9", 0),
                "{target:?}"
            );
            assert!(record.hold_reason.is_some());
        }
    }

    /// A version-less lookup that succeeds and finds nothing to install
    /// clears a record the triggers would otherwise wait out: the sentinel for
    /// an unknown release, or a release newer than the latest (withdrawn). An
    /// unparseable record is kept.
    #[tokio::test]
    async fn a_lookup_that_finds_nothing_clears_stale_records() {
        use super::super::super::rollback::{UNKNOWN_RELEASE, record_update_failure_at};
        let server = Server::run();
        let t0 = SystemTime::now();
        let long_ago = t0 - Duration::from_secs(5 * 3600);
        for (recorded, kept) in [
            (UNKNOWN_RELEASE, false),
            ("9.9.9", false),
            ("not-a-version", true),
        ] {
            let (_tmp, root) = test_root();
            let dir = root.parent().unwrap();
            fs::create_dir_all(dir).unwrap();
            record_update_failure_at(dir, recorded, "offline", long_ago, 0.5);
            let outcome =
                stage_into_with(&root, &server, Ok("v9.9.8".into()), None, &|_| false).await;
            assert_eq!(outcome, Staging::NothingToInstall);
            assert_eq!(cooldown(&root).is_some(), kept, "{recorded}");
        }
    }

    /// What a failed lookup counts against: the release the trigger named,
    /// whatever is on record; else a newer, unblocked release on record;
    /// else the sentinel.
    #[test]
    fn a_named_target_takes_precedence_over_the_record() {
        use super::super::super::rollback::{UNKNOWN_RELEASE, record_update_failure_at};
        let now = SystemTime::now();
        let blocked = |v: &str| v == "9.9.12";
        for (recorded, target, expected) in [
            (Some("9.9.10"), Some("9.9.11"), "9.9.11"),
            (None, Some("9.9.11"), "9.9.11"),
            (Some("9.9.10"), None, "9.9.10"),
            (Some("9.9.8"), None, UNKNOWN_RELEASE),
            (Some("9.9.12"), None, UNKNOWN_RELEASE),
            (None, None, UNKNOWN_RELEASE),
        ] {
            let (_tmp, root) = test_root();
            let dir = root.parent().unwrap();
            fs::create_dir_all(dir).unwrap();
            if let Some(recorded) = recorded {
                record_update_failure_at(dir, recorded, "earlier", now, 0.5);
            }
            assert_eq!(
                lookup_failure_target(&root, "9.9.8", target, &blocked),
                expected,
                "{recorded:?} on record, {target:?} named"
            );
        }
    }

    /// A trigger names a release with two failures on record, and the lookup
    /// finds nothing newer than this binary (the release was withdrawn). The
    /// record is cleared as stale, then the named release is held again from
    /// a count of zero: the hold spaces the next trigger, and the withdrawn
    /// release's count must not carry into it.
    #[tokio::test]
    async fn a_withdrawn_named_release_is_cleared_then_held_from_zero() {
        use super::super::super::rollback::record_update_failure_at;
        let server = Server::run(); // no expectations: nothing is downloaded
        let (_tmp, root) = test_root();
        let dir = root.parent().unwrap();
        fs::create_dir_all(dir).unwrap();
        let long_ago = SystemTime::now() - Duration::from_secs(5 * 3600);
        for _ in 0..2 {
            record_update_failure_at(dir, "9.9.9", "earlier", long_ago, 0.5);
        }
        assert_eq!(cooldown(&root).unwrap().failures, 2);
        let prefix = server.url_str("/");
        let env = test_env(&prefix);
        let outcome = stage_into(
            &root,
            "9.9.8",
            || async { Ok("v9.9.8".to_string()) },
            Some("9.9.9"),
            |_: &str| false,
            &env,
        )
        .await;
        assert_eq!(outcome, Staging::NothingToInstall);
        let record = cooldown(&root).expect("the named release is held");
        assert_eq!((record.version.as_str(), record.failures), ("9.9.9", 0));
        assert_eq!(record.last_error, "", "nothing carried from the old record");
        assert_eq!(
            record.hold_reason.as_deref(),
            Some("the release lookup found no newer installable release")
        );
    }

    /// What a successful lookup counts as stale, clause by clause.
    #[test]
    fn stale_records_after_a_successful_lookup() {
        use super::super::super::rollback::UNKNOWN_RELEASE;
        let none = |_: &str| false;
        let blocked = |v: &str| v == "9.9.9";
        for (recorded, latest, blocked, stale) in [
            (
                UNKNOWN_RELEASE,
                "9.9.9",
                &none as &dyn Fn(&str) -> bool,
                true,
            ),
            ("9.9.7", "9.9.9", &none, true),
            ("9.9.8", "9.9.9", &none, true),
            ("9.9.9", "9.9.9", &blocked, true),
            ("9.9.10", "9.9.9", &none, true),
            ("9.9.9", "9.9.9", &none, false),
            ("9.9.9", "9.9.10", &none, false),
            ("not-a-version", "9.9.9", &none, false),
        ] {
            assert_eq!(
                is_stale_record(recorded, "9.9.8", latest, &blocked),
                stale,
                "{recorded} with {latest} latest"
            );
        }
    }

    /// A failed lookup never counts against a release this node has blocked
    /// (#4073); it counts against the sentinel instead.
    #[tokio::test]
    async fn a_failed_lookup_skips_a_blocked_release_on_record() {
        use super::super::super::rollback::{UNKNOWN_RELEASE, record_update_failure_at};
        let (_tmp, root) = test_root();
        let dir = root.parent().unwrap();
        fs::create_dir_all(dir).unwrap();
        let long_ago = SystemTime::now() - Duration::from_secs(5 * 3600);
        record_update_failure_at(dir, "9.9.9", "earlier", long_ago, 0.5);
        let server = Server::run();
        let outcome = stage_into_with(
            &root,
            &server,
            Err(anyhow::anyhow!("offline")),
            None,
            &|v| v == "9.9.9",
        )
        .await;
        assert_eq!(outcome, Staging::Deferred);
        let record = cooldown(&root).unwrap();
        assert_eq!(
            (record.version.as_str(), record.failures),
            (UNKNOWN_RELEASE, 1)
        );
    }

    /// The hold a run writes before it reaches the network says why it is
    /// held once the run knows: a rate-limited lookup replaces the reason,
    /// keeping the retry time and the count.
    #[tokio::test]
    async fn a_live_hold_takes_the_reason_its_run_ends_with() {
        let server = Server::run();
        let prefix = server.url_str("/");
        let (_tmp, root) = test_root();
        let dir = root.parent().unwrap().to_path_buf();
        let mut first = None;
        let outcome = stage_into(
            &root,
            "9.9.8",
            || {
                first = super::super::super::rollback::read_update_cooldown_at(&dir);
                async { Err::<String, _>(GithubRateLimitedError { retry_after: None }.into()) }
            },
            Some("9.9.9"),
            |_: &str| false,
            &test_env(&prefix),
        )
        .await;
        assert_eq!(outcome, Staging::Throttled);
        let first = first.expect("the hold before the lookup");
        let last = cooldown(&root).unwrap();
        assert_eq!(
            (last.retry_at_unix, last.failures),
            (first.retry_at_unix, first.failures)
        );
        assert_ne!(first.hold_reason, last.hold_reason);
        assert_eq!(
            last.hold_reason.as_deref(),
            Some("GitHub rate-limited the release lookup")
        );
    }

    /// A download of unknown size (chunked) killed mid-way leaves at most
    /// MAX_UNKNOWN_DOWNLOAD charged, or what is left of a limited allowance,
    /// and a huge declared size is capped at a day's budget: a killed
    /// unlimited meter (a manual run, `--force`) charges at most a day's
    /// budget, never `u64::MAX`, and never erases what was charged before it.
    #[tokio::test]
    async fn an_unknown_size_precharge_is_capped() {
        use super::super::super::rollback::{
            DOWNLOAD_BYTES_PER_DAY, charge_download_bytes_at, download_bytes_at,
        };
        use super::super::tests::{partial_server, run_until};
        const MIB: u64 = 1024 * 1024;
        assert_eq!(MAX_UNKNOWN_DOWNLOAD, 128 * MIB);
        let chunked = partial_server(None, 100, true).await;
        let huge = partial_server(Some(1 << 40), 100, true).await;
        let now = SystemTime::now();
        let clock = move || now;
        for (case, prefix, allowance, expected) in [
            ("unlimited, chunked", &chunked.prefix, u64::MAX, 128 * MIB),
            ("limited, chunked", &chunked.prefix, 10 * MIB, 10 * MIB),
            (
                "unlimited, huge",
                &huge.prefix,
                u64::MAX,
                DOWNLOAD_BYTES_PER_DAY,
            ),
        ] {
            let (_tmp, root) = test_root();
            let dir = root.parent().unwrap();
            fs::create_dir_all(&root).unwrap();
            charge_download_bytes_at(dir, 1000, now);
            let meter = ByteMeter::charged_to(allowance, dir, &clock);
            let part = root.join("asset.part");
            run_until(
                download_resumable(&format!("{prefix}asset"), &part, &meter),
                || download_bytes_at(dir, now) > 1000,
            )
            .await;
            assert_eq!(download_bytes_at(dir, now), 1000 + expected, "{case}");
        }
    }

    /// A staging download is charged for its Content-Length before its body
    /// arrives, so one abandoned mid-way (the run killed) leaves that charge
    /// on the ledger; one that ends, here cut short by the server, is
    /// reconciled to the bytes that actually arrived.
    #[tokio::test]
    async fn a_download_is_charged_before_its_body_and_reconciled_after() {
        use super::super::super::rollback::download_bytes_at;
        use super::super::tests::{fixed_now, run_until, stalling_server};
        const MIB: u64 = 1024 * 1024;
        let now = fixed_now();
        let clock = move || now;
        // Abandoned: the server sends 100 bytes of 5 MiB and stalls.
        let (_tmp, root) = test_root();
        let dir = root.parent().unwrap();
        fs::create_dir_all(&root).unwrap();
        let prefix = stalling_server(5 << 20, 100).await;
        let meter = ByteMeter::charged_to(u64::MAX, dir, &clock);
        let part = root.join("asset.part");
        run_until(
            download_resumable(&format!("{prefix}asset"), &part, &meter),
            || download_bytes_at(dir, now) > 0,
        )
        .await;
        assert_eq!(download_bytes_at(dir, now), 5 * MIB, "charged up front");
        // Never more than the allowance left.
        let (_tmp, root) = test_root();
        let dir = root.parent().unwrap();
        fs::create_dir_all(&root).unwrap();
        let meter = ByteMeter::charged_to(MIB, dir, &clock);
        let part = root.join("asset.part");
        run_until(
            download_resumable(&format!("{prefix}asset"), &part, &meter),
            || download_bytes_at(dir, now) > 0,
        )
        .await;
        assert_eq!(download_bytes_at(dir, now), MIB);
        // Ended: 40 MiB declared, 20 MiB sent, then the connection closes.
        let (prefix, _, _) = restarting_server(Vec::new(), 40 << 20, 20 << 20).await;
        let (_tmp, root) = test_root();
        let dir = root.parent().unwrap();
        fs::create_dir_all(&root).unwrap();
        let meter = ByteMeter::charged_to(u64::MAX, dir, &clock);
        let part = root.join("asset.part");
        assert!(
            download_resumable(&format!("{prefix}asset"), &part, &meter)
                .await
                .is_err(),
            "the server stops half way"
        );
        assert_eq!(download_bytes_at(dir, now), 20 * MIB, "reconciled");
        meter.flush();
        assert_eq!(
            download_bytes_at(dir, now),
            20 * MIB,
            "a second reconcile changes nothing"
        );
    }

    /// When the node stages next after a run at `after`: once the record's
    /// cooldown ends, on the next 60 s tick. The re-poll is pulled in to the
    /// cooldown's end (#5799), so a node no peer prompts follows the same
    /// schedule.
    fn next_run(root: &Path, after: SystemTime) -> SystemTime {
        let record = cooldown(root).expect("every outcome here records");
        let retry = std::time::UNIX_EPOCH + Duration::from_secs(record.retry_at_unix);
        retry.max(after) + Duration::from_secs(60)
    }

    fn hours(from: SystemTime, to: SystemTime) -> f64 {
        to.duration_since(from).unwrap().as_secs_f64() / 3600.0
    }

    /// #5799: a day of persistent failures on the real schedule. The
    /// waits are 1 h and 2 h, the third failure exits for the updater, and
    /// every attempt after that exits again, about 4 h apart (the updater fails
    /// the same download and records it too). A download ceiling that counted
    /// runs used to stop the exits after the third run, leaving the update
    /// waiting until the next day.
    #[tokio::test]
    async fn a_day_of_persistent_failures_keeps_falling_back_every_four_hours() {
        let (_tmp, root) = test_root();
        let state_dir = root.parent().unwrap().to_path_buf();
        let server = failing_release();
        let t0 = SystemTime::now();
        let mut now = t0;
        let mut exits = Vec::new();
        while now < t0 + Duration::from_secs(24 * 3600) {
            let outcome = stage_at(&root, &server.url_str("/"), now).await;
            match outcome {
                Staging::FallingBack => {
                    exits.push(now);
                    super::super::super::rollback::record_update_failure_at(
                        &state_dir,
                        "9.9.9",
                        "the updater could not download it either",
                        now,
                        0.5,
                    );
                }
                Staging::Deferred => {}
                Staging::Skipped
                | Staging::Staged
                | Staging::Failed
                | Staging::CoolingDown
                | Staging::NothingToInstall
                | Staging::Throttled => panic!("{outcome:?} at t0+{:.1} h", hours(t0, now)),
            }
            now = next_run(&root, now);
        }
        let first = hours(t0, exits[0]);
        assert!((2.4..=3.7).contains(&first), "first exit at {first:.1} h");
        assert!(exits.len() >= 5, "{} exits in a day", exits.len());
        for pair in exits.windows(2) {
            let gap = hours(pair[0], pair[1]);
            assert!((3.2..=4.9).contains(&gap), "exits {gap:.1} h apart");
        }
    }

    /// #5799: the network heals at t0+4 h, after the third failure
    /// exited. The next scheduled run stages the release; nothing left over
    /// from the outage holds it back.
    #[tokio::test]
    async fn after_a_long_outage_the_next_scheduled_run_stages() {
        let (_tmp, root) = test_root();
        let state_dir = root.parent().unwrap().to_path_buf();
        let failing = failing_release();
        let freenet = freenet_asset_name();
        let manifest = manifest_for(&[(&freenet, b"archive")]);
        let healed = serve_release(manifest.as_bytes(), None, &[(&freenet, b"archive")]);
        let t0 = SystemTime::now();
        let heal = t0 + Duration::from_secs(4 * 3600);
        let mut now = t0;
        while now < heal {
            if stage_at(&root, &failing.url_str("/"), now).await == Staging::FallingBack {
                super::super::super::rollback::record_update_failure_at(
                    &state_dir, "9.9.9", "updater", now, 0.5,
                );
            }
            now = next_run(&root, now);
        }
        assert!(
            hours(heal, now) < 4.9,
            "next run {:.1} h after",
            hours(heal, now)
        );
        assert_eq!(
            stage_at(&root, &healed.url_str("/"), now).await,
            Staging::Staged
        );
    }

    /// A server that answers the manifest in full and every archive request
    /// with a 200 that ignores any Range and stops after `sent` of its
    /// declared `declared` bytes: a download that starts over every time and
    /// never finishes, the pathological case the byte budget exists for.
    /// Returns its URL prefix, the number of manifest requests (one per
    /// attempt that reached it) and the archive bytes it sent.
    async fn restarting_server(
        manifest: Vec<u8>,
        declared: usize,
        sent: usize,
    ) -> (
        String,
        std::sync::Arc<std::sync::atomic::AtomicUsize>,
        std::sync::Arc<std::sync::atomic::AtomicUsize>,
    ) {
        use std::sync::atomic::Ordering::SeqCst;
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let prefix = format!("http://{}/", listener.local_addr().unwrap());
        let runs = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let bytes = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let (counted_runs, counted_bytes) = (runs.clone(), bytes.clone());
        tokio::spawn(async move {
            loop {
                let Ok((mut conn, _)) = listener.accept().await else {
                    return;
                };
                let (manifest, runs, bytes) = (
                    manifest.clone(),
                    counted_runs.clone(),
                    counted_bytes.clone(),
                );
                tokio::spawn(async move {
                    let mut request = Vec::new();
                    let mut buf = [0u8; 1024];
                    while !request.windows(4).any(|w| w == b"\r\n\r\n") {
                        match conn.read(&mut buf).await {
                            Ok(0) | Err(_) => return,
                            Ok(n) => request.extend_from_slice(&buf[..n]),
                        }
                    }
                    let line = String::from_utf8_lossy(&request);
                    let path = line.split_whitespace().nth(1).unwrap_or("").to_string();
                    let reply = if path.ends_with(&format!("/{MANIFEST}")) {
                        runs.fetch_add(1, SeqCst);
                        let mut r = format!(
                            "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                            manifest.len()
                        )
                        .into_bytes();
                        r.extend_from_slice(&manifest);
                        r
                    } else if path.ends_with(&format!("/{SIGNATURE}")) {
                        b"HTTP/1.1 404 Not Found\r\nContent-Length: 0\r\nConnection: close\r\n\r\n"
                            .to_vec()
                    } else {
                        bytes.fetch_add(sent, SeqCst);
                        let mut r = format!(
                            "HTTP/1.1 200 OK\r\nContent-Length: {declared}\r\nConnection: close\r\n\r\n"
                        )
                        .into_bytes();
                        r.extend(std::iter::repeat_n(b'x', sent));
                        r
                    };
                    let _sent = conn.write_all(&reply).await;
                    let _done = conn.shutdown().await;
                });
            }
        });
        (prefix, runs, bytes)
    }

    /// One route of [`routed_server`]: a body declaring `declared` bytes
    /// (chunked, with no size, when `None`) of which `body` is sent, then the
    /// connection is held open (`stall`) or the body ends and it is closed.
    struct Route {
        suffix: String,
        declared: Option<usize>,
        body: Vec<u8>,
        stall: bool,
    }

    impl Route {
        /// The whole of `body` under its Content-Length.
        fn whole(suffix: &str, body: Vec<u8>) -> Self {
            Self {
                suffix: suffix.to_string(),
                declared: Some(body.len()),
                body,
                stall: false,
            }
        }
    }

    /// A server answering each request with the first route whose suffix
    /// ends the request path, and a 404 for any other path. Returns its URL
    /// prefix.
    async fn routed_server(routes: Vec<Route>) -> String {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let prefix = format!("http://{}/", listener.local_addr().unwrap());
        let routes = std::sync::Arc::new(routes);
        tokio::spawn(async move {
            while let Ok((mut conn, _)) = listener.accept().await {
                let routes = routes.clone();
                tokio::spawn(async move {
                    let mut request = Vec::new();
                    let mut buf = [0u8; 1024];
                    while !request.windows(4).any(|w| w == b"\r\n\r\n") {
                        match conn.read(&mut buf).await {
                            Ok(0) | Err(_) => return,
                            Ok(n) => request.extend_from_slice(&buf[..n]),
                        }
                    }
                    let line = String::from_utf8_lossy(&request);
                    let path = line.split_whitespace().nth(1).unwrap_or("").to_string();
                    let Some(route) = routes.iter().find(|r| path.ends_with(&r.suffix)) else {
                        let _sent = conn
                            .write_all(
                                b"HTTP/1.1 404 Not Found\r\nContent-Length: 0\r\nConnection: close\r\n\r\n",
                            )
                            .await;
                        return;
                    };
                    let mut reply = match route.declared {
                        Some(n) => format!(
                            "HTTP/1.1 200 OK\r\nContent-Length: {n}\r\nConnection: close\r\n\r\n"
                        )
                        .into_bytes(),
                        None => b"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n".to_vec(),
                    };
                    if route.declared.is_none() {
                        reply.extend(format!("{:x}\r\n", route.body.len()).into_bytes());
                        reply.extend(&route.body);
                        reply.extend(b"\r\n");
                        if !route.stall {
                            reply.extend(b"0\r\n\r\n");
                        }
                    } else {
                        reply.extend(&route.body);
                    }
                    let _sent = conn.write_all(&reply).await;
                    if route.stall {
                        tokio::time::sleep(Duration::from_secs(3600)).await;
                    } else {
                        let _done = conn.shutdown().await;
                    }
                });
            }
        });
        prefix
    }

    /// #5799: a staging attempt that runs out its deadline is dropped
    /// mid-download, so the download's own reconcile never runs and its
    /// pre-charge (the whole Content-Length) is left on the ledger. The
    /// flush in `stage_into` after the deadline reconciles it to the bytes
    /// that arrived. The clock is paused and advanced past the deadline only
    /// once the pre-charge and the bytes are in; until then the second
    /// branch keeps the runtime busy, so the paused clock cannot jump ahead.
    #[tokio::test(start_paused = true)]
    async fn a_timed_out_attempt_is_reconciled_by_stage_into() {
        use super::super::super::rollback::download_bytes_at;
        use super::super::tests::fixed_now;
        const MIB: u64 = 1024 * 1024;
        let freenet = freenet_asset_name();
        let manifest = manifest_for(&[(&freenet, b"the real archive")]);
        let prefix = routed_server(vec![
            Route::whole(MANIFEST, manifest.into_bytes()),
            Route {
                suffix: freenet.clone(),
                declared: Some(5 << 20),
                body: vec![b'x'; 100],
                stall: true,
            },
        ])
        .await;
        let (_tmp, root) = test_root();
        let dir = root.parent().unwrap();
        let now = fixed_now();
        let clock = move || now;
        let deadline = Duration::from_secs(10);
        let env = StageEnv {
            deadline,
            clock: &clock,
            ..test_env(&prefix)
        };
        let part = root.join(TAG).join(format!("{freenet}.part"));
        let run = stage_into(
            &root,
            "9.9.8",
            || async { Ok(TAG.to_string()) },
            Some("9.9.9"),
            |_: &str| false,
            &env,
        );
        let arrived = async {
            let cap = std::time::Instant::now();
            while download_bytes_at(dir, now) == 0
                || fs::metadata(&part).map_or(0, |m| m.len()) < 100
            {
                assert!(
                    cap.elapsed() < Duration::from_secs(60),
                    "the download never got going"
                );
                tokio::task::yield_now().await;
            }
            assert_eq!(download_bytes_at(dir, now), 5 * MIB, "pre-charged");
            tokio::time::advance(deadline).await;
        };
        let (outcome, ()) = tokio::join!(run, arrived);
        assert_eq!(outcome, Staging::Deferred);
        assert_eq!(download_bytes_at(dir, now), 100, "reconciled");
        let error = cooldown(&root).unwrap().last_error;
        assert!(error.contains("did not finish"), "{error}");
    }

    /// #5799: two downloads on one meter, the freenet archive (of unknown
    /// size) then fdev, with the run killed during fdev. The ledger then
    /// holds the archive's actual bytes plus fdev's pre-charge: a pre-charge
    /// adds to the bytes already received rather than replacing them, and
    /// is the download's own size, not all the allowance left.
    #[tokio::test]
    async fn a_run_killed_in_its_second_download_keeps_both_charges() {
        use super::super::super::rollback::download_bytes_at;
        use super::super::tests::{fixed_now, run_until};
        const MIB: u64 = 1024 * 1024;
        let freenet = freenet_asset_name();
        let fdev = fdev_asset_name();
        let archive = vec![b'a'; 1000];
        let manifest = manifest_for(&[(&freenet, &archive), (&fdev, b"the real fdev")]);
        let prefix = routed_server(vec![
            Route::whole(MANIFEST, manifest.into_bytes()),
            Route {
                suffix: freenet.clone(),
                declared: None,
                body: archive,
                stall: false,
            },
            Route {
                suffix: fdev.clone(),
                declared: Some(5 << 20),
                body: vec![b'f'; 100],
                stall: true,
            },
        ])
        .await;
        let (_tmp, root) = test_root();
        let dir = root.parent().unwrap();
        fs::create_dir_all(&root).unwrap();
        let now = fixed_now();
        let clock = move || now;
        let meter = ByteMeter::charged_to(50 * MIB, dir, &clock);
        let fdev_part = root.join(TAG).join(format!("{fdev}.part"));
        // Killed once fdev's pre-charge has moved the ledger off the
        // archive's reconciled 1000 bytes.
        run_until(
            stage_release_at(&root, &prefix, TAG, &unsigned, &meter),
            || fdev_part.exists() && download_bytes_at(dir, now) != 1000,
        )
        .await;
        assert!(
            root.join(TAG).join(&freenet).exists(),
            "the archive completed"
        );
        assert_eq!(download_bytes_at(dir, now), 1000 + 5 * MIB);
    }

    /// #5799: the byte budget binds when downloads keep starting over, and
    /// stops the run that reaches it mid-download. From then on the node
    /// holds, without counting a failure and without a request, until the
    /// budget's window resets: an exit would hand the download to an updater
    /// the same budget refuses, so no fallback exit happens meanwhile.
    #[tokio::test]
    async fn the_byte_budget_stops_downloads_that_keep_starting_over() {
        use std::sync::atomic::Ordering::SeqCst;
        let freenet = freenet_asset_name();
        let manifest = manifest_for(&[(&freenet, b"the real archive")]);
        let (prefix, runs, bytes) =
            restarting_server(manifest.into_bytes(), 1_000_000, 10_000).await;
        let (_tmp, root) = test_root();
        let delays = [Duration::from_millis(1); 5];
        let env = StageEnv {
            delays: &delays,
            byte_budget: 25_000,
            ..test_env(&prefix)
        };
        let run = || {
            stage_into(
                &root,
                "9.9.8",
                || async { Ok(TAG.to_string()) },
                Some("9.9.9"),
                |_: &str| false,
                &env,
            )
        };
        // Two attempts restart from zero, the third passes the budget and is
        // stopped: no more retries, and the node holds.
        let outcome = run().await;
        assert_eq!(outcome, Staging::Throttled);
        assert!(!outcome.preparation().should_exit());
        assert_eq!(bytes.load(SeqCst), 30_000);
        let requests = runs.load(SeqCst);
        let state_dir = root.parent().unwrap();
        let spent = super::super::super::rollback::download_bytes_at(state_dir, SystemTime::now());
        assert!((25_000..=30_000).contains(&spent), "{spent}");
        let held = cooldown(&root).unwrap();
        assert_eq!(held.failures, 0);
        assert!(held.hold_reason.unwrap().contains("budget"));
        // Even with two failures on record and the cooldown out of the way,
        // later runs hold without a request and never fall back.
        let long_ago = SystemTime::now() - Duration::from_secs(5 * 3600);
        super::super::super::rollback::clear_update_cooldown_at(state_dir);
        for _ in 0..2 {
            super::super::super::rollback::record_update_failure_at(
                state_dir, "9.9.9", "earlier", long_ago, 0.5,
            );
        }
        for _ in 0..3 {
            let outcome = run().await;
            assert_eq!(outcome, Staging::Throttled);
            assert!(!outcome.preparation().should_exit());
            assert_eq!(cooldown(&root).unwrap().failures, 2);
            // Out of the way again, as if each hold had ended.
            let mut record = cooldown(&root).unwrap();
            record.retry_at_unix = SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_secs()
                - 1;
            fs::write(
                state_dir.join("update_cooldown.json"),
                serde_json::to_vec(&record).unwrap(),
            )
            .unwrap();
        }
        assert_eq!(runs.load(SeqCst), requests, "a held run makes no request");
    }

    /// A staged manifest whose archive no longer verifies (truncated or
    /// corrupted after it was staged) does not get past a spent budget: the
    /// run holds, without counting, before the poll token and the lookup. A
    /// complete staged copy does.
    #[tokio::test]
    async fn a_spent_budget_holds_when_the_staged_copy_is_incomplete() {
        let freenet = freenet_asset_name();
        let manifest = manifest_for(&[(&freenet, b"archive")]);
        let server = Server::run(); // no expectations: no request at all
        let (_tmp, root) = test_root();
        let dir = root.parent().unwrap().to_path_buf();
        write_staged(
            &root,
            &[(MANIFEST, manifest.as_bytes()), (&freenet, b"arch")],
        );
        let now = SystemTime::now();
        super::super::super::rollback::charge_download_bytes_at(&dir, u64::MAX, now);
        // The claim's cheap check sees a non-empty archive and lets a
        // locked-out node's claim through; staging below still holds.
        assert!(!download_budget_blocks_at(&dir, now));
        let clock = move || now;
        let prefix = server.url_str("/");
        let env = StageEnv {
            clock: &clock,
            ..test_env(&prefix)
        };
        let outcome = stage_into(
            &root,
            "9.9.8",
            || async { panic!("looked the tag up with the budget spent") },
            Some("9.9.9"),
            |_: &str| false,
            &env,
        )
        .await;
        assert_eq!(outcome, Staging::Throttled);
        assert_eq!(cooldown(&root).unwrap().failures, 0);
        assert!(
            !dir.join("github_poll_bucket_node").exists(),
            "no poll token was spent"
        );
        // An empty archive (a torn write) blocks the claim; a complete one
        // does not.
        write_staged(&root, &[(MANIFEST, manifest.as_bytes()), (&freenet, b"")]);
        assert!(download_budget_blocks_at(&dir, now));
        write_staged(
            &root,
            &[(MANIFEST, manifest.as_bytes()), (&freenet, b"archive")],
        );
        assert!(!download_budget_blocks_at(&dir, now));
    }

    /// A spent budget holds an install that stages nothing (a macOS app
    /// bundle) too, before the lookup.
    #[tokio::test]
    async fn a_spent_budget_holds_an_install_that_stages_nothing() {
        let (_tmp, root) = test_root();
        let dir = root.parent().unwrap();
        fs::create_dir_all(dir).unwrap();
        // A whole second, read once, so the expected retry time is exact.
        let now = std::time::UNIX_EPOCH
            + Duration::from_secs(
                SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap()
                    .as_secs(),
            );
        let clock = move || now;
        super::super::super::rollback::charge_download_bytes_at(dir, 1000, now);
        let lookup = || async { panic!("looked the tag up with the budget spent") };
        let outcome =
            decide_without_staging(&root, "9.9.8", Some("9.9.9"), lookup, (&[], 1000), &clock)
                .await;
        assert_eq!(outcome, Staging::Throttled);
        assert!(!outcome.preparation().should_exit());
        let held = cooldown(&root).unwrap();
        assert_eq!((held.version.as_str(), held.failures), ("9.9.9", 0));
        // Held until the window resets, within the longest plausible wait.
        let retry = std::time::UNIX_EPOCH + Duration::from_secs(held.retry_at_unix);
        assert_eq!(
            retry.duration_since(now).unwrap(),
            Duration::from_secs(4 * 3600)
        );
    }

    /// The budget is the node's, not a release's: after one release has
    /// spent it, another release gets no fresh allowance.
    #[tokio::test]
    async fn alternating_releases_share_one_allowance() {
        use std::sync::atomic::Ordering::SeqCst;
        let freenet = freenet_asset_name();
        let manifest = manifest_for(&[(&freenet, b"the real archive")]);
        let (prefix, _, bytes) = restarting_server(manifest.into_bytes(), 1_000_000, 10_000).await;
        let (_tmp, root) = test_root();
        let state_dir = root.parent().unwrap().to_path_buf();
        let delays = [Duration::from_millis(1); 5];
        let env = StageEnv {
            delays: &delays,
            byte_budget: 25_000,
            ..test_env(&prefix)
        };
        for (tag, version) in [
            ("v9.9.9", "9.9.9"),
            ("v9.9.10", "9.9.10"),
            ("v9.9.9", "9.9.9"),
        ] {
            super::super::super::rollback::clear_update_cooldown_at(&state_dir);
            fs::remove_file(state_dir.join("github_poll_bucket_node")).ok();
            stage_into(
                &root,
                "9.9.8",
                || async move { Ok(tag.to_string()) },
                Some(version),
                |_: &str| false,
                &env,
            )
            .await;
        }
        assert_eq!(
            bytes.load(SeqCst),
            30_000,
            "only the first release's run downloaded anything"
        );
    }

    /// #5799: with the cooldown taken out of the picture before every
    /// run, the poll bucket still bounds staging's requests (one token per run,
    /// 8 then one per 10 minutes) and the byte budget its bytes.
    #[tokio::test]
    async fn with_the_cooldown_gone_the_bucket_and_budget_still_bound_staging() {
        use std::sync::atomic::Ordering::SeqCst;
        let freenet = freenet_asset_name();
        let manifest = manifest_for(&[(&freenet, b"the real archive")]);
        let (prefix, runs, bytes) =
            restarting_server(manifest.into_bytes(), 1_000_000, 10_000).await;
        let (_tmp, root) = test_root();
        let state_dir = root.parent().unwrap().to_path_buf();
        let t0 = SystemTime::now();
        // Requests: a run a minute for 50 minutes (one attempt each, no
        // retries here) reaches GitHub at most 8 + 5 times.
        for minute in 0..50 {
            super::super::super::rollback::clear_update_cooldown_at(&state_dir);
            let now = t0 + Duration::from_secs(60 * minute);
            let clock = move || now;
            let env = StageEnv {
                clock: &clock,
                ..test_env(&prefix)
            };
            stage_into(
                &root,
                "9.9.8",
                || async { Ok(TAG.to_string()) },
                Some("9.9.9"),
                |_: &str| false,
                &env,
            )
            .await;
        }
        let reached = runs.load(SeqCst);
        assert!((8..=13).contains(&reached), "{reached} runs reached GitHub");
        // Bytes: with the bucket refilled too, only the budget is left, and it
        // holds the archive bytes to the budget plus at most one response.
        runs.store(0, SeqCst);
        bytes.store(0, SeqCst);
        fs::remove_file(state_dir.join("download_budget.json")).unwrap();
        let delays = [Duration::from_millis(1); 5];
        for _ in 0..20 {
            super::super::super::rollback::clear_update_cooldown_at(&state_dir);
            fs::remove_file(state_dir.join("github_poll_bucket_node")).ok();
            let env = StageEnv {
                delays: &delays,
                byte_budget: 45_000,
                ..test_env(&prefix)
            };
            stage_into(
                &root,
                "9.9.8",
                || async { Ok(TAG.to_string()) },
                Some("9.9.9"),
                |_: &str| false,
                &env,
            )
            .await;
        }
        // One run, stopped on its fifth attempt; every later run refused.
        assert_eq!(bytes.load(SeqCst), 50_000);
        assert_eq!(runs.load(SeqCst), 5);
    }

    /// #5799: a run spends ONE token from the poll bucket, however many
    /// attempts it makes, so a fully failing run cannot drain the bucket that
    /// detection shares, and running short mid-run never turns a real network
    /// failure into a "throttled" run that counts nothing.
    #[tokio::test]
    async fn a_failing_run_spends_one_poll_token_and_counts_its_failure() {
        let freenet = freenet_asset_name();
        let manifest = manifest_for(&[(&freenet, b"archive")]);
        let server = serve_release(manifest.as_bytes(), None, &[]);
        server.expect(
            Expectation::matching(request::method_path("GET", asset_path(&freenet)))
                .times(6)
                .respond_with(status_code(500)),
        );
        let (_tmp, root) = test_root();
        let state_dir = root.parent().unwrap();
        fs::create_dir_all(state_dir).unwrap();
        let now = SystemTime::now();
        let now_unix = now.duration_since(std::time::UNIX_EPOCH).unwrap().as_secs();
        fs::write(
            state_dir.join("github_poll_bucket_node"),
            format!("1 {now_unix}"),
        )
        .unwrap();
        let clock = move || now;
        let delays = [Duration::from_millis(1); 5];
        let prefix = server.url_str("/");
        let env = StageEnv {
            clock: &clock,
            delays: &delays,
            ..test_env(&prefix)
        };
        let outcome = stage_into(
            &root,
            "9.9.8",
            || async { Ok(TAG.to_string()) },
            Some("9.9.9"),
            |_: &str| false,
            &env,
        )
        .await;
        assert_eq!(outcome, Staging::Deferred);
        assert_eq!(cooldown(&root).unwrap().failures, 1);
        let bucket = fs::read_to_string(state_dir.join("github_poll_bucket_node")).unwrap();
        assert!(bucket.starts_with("0 "), "{bucket}");
    }

    /// The budget fails open: a record that cannot be written does not stop
    /// a download.
    #[tokio::test]
    async fn an_unwritable_budget_record_still_stages() {
        let freenet = freenet_asset_name();
        let manifest = manifest_for(&[(&freenet, b"archive")]);
        let server = serve_release(manifest.as_bytes(), None, &[(&freenet, b"archive")]);
        let (tmp, root) = test_root();
        fs::create_dir_all(tmp.path().join("download_budget.json")).unwrap();
        assert_eq!(
            stage_once(&root, &server.url_str("/"), STAGE_DEADLINE).await,
            Staging::Staged
        );
    }

    /// #5799: a staging run that stays names the real cause, not
    /// "GitHub could not be reached" for all of them, and the fdev-only path
    /// (which exits to install) checks for the staged freenet archive before
    /// anything is logged, so it never reports a failure.
    #[test]
    fn a_failed_run_names_its_cause() {
        let hour = Duration::from_secs(3600);
        let limited = anyhow::Error::new(GithubRateLimitedError { retry_after: None })
            .context("Failed to download file");
        for (e, cause) in [
            (StageTimedOut(hour).into(), FailureCause::TimedOut(hour)),
            (
                anyhow::Error::new(BudgetSpent).context("Failed to download fdev"),
                FailureCause::BudgetSpent,
            ),
            (limited, FailureCause::RateLimited),
            (
                anyhow::anyhow!("Download failed: 500"),
                FailureCause::Unreachable,
            ),
        ] {
            assert_eq!(failure_cause(&e), cause, "{e:#}");
        }
        let src = include_str!("staged.rs").replace("\r\n", "\n");
        let production = src
            .split_once(concat!("#[cfg(test)]\n", "mod tests {"))
            .unwrap()
            .0;
        // Each cause logs its own line. Behavioural log capture is avoided
        // here: tracing caches callsite interest process-wide (#5314), so it
        // can be blinded by other tests. Each arm is bounded by the next one.
        let log = &production[production.find("\nfn log_transient_failure(").unwrap()..];
        let log: String = log[..log.find("\n}\n").unwrap()]
            .split_whitespace()
            .collect();
        let arms: Vec<&str> = log.split("FailureCause::").skip(1).collect();
        for (arm, line) in [
            (
                "TimedOut(deadline)=>",
                "timedout;alaterattemptresumesthepartialdownload",
            ),
            ("BudgetSpent=>", "today'sdownloadbudgetisspent"),
            ("RateLimited=>", "GitHubrate-limitedthedownload"),
            ("Unreachable=>", "GitHubcouldnotbereachedforit"),
        ] {
            let body = arms
                .iter()
                .find(|a| a.starts_with(arm))
                .unwrap_or_else(|| panic!("no `{arm}` arm"));
            assert!(body.contains(line), "`{arm}` must log `{line}`: {body}");
        }
        let start = production.find("\nasync fn stage_into<").unwrap();
        let body = &production[start..];
        let body = &body[..body.find("\n}\n").unwrap()];
        let staged = body
            .rfind("if freenet_archive_staged(root, &tag).await {")
            .expect("the fdev-only exit");
        let logged = body
            .find("log_transient_failure(&tag, &error);")
            .expect("the failure line");
        assert!(
            staged < logged,
            "checked for a staged archive before logging"
        );
    }

    /// #5799: a staged archive is one whose bytes match the cached
    /// manifest, not merely a file that exists.
    #[tokio::test]
    async fn a_staged_archive_must_match_its_manifest() {
        let freenet = freenet_asset_name();
        let manifest = manifest_for(&[(&freenet, b"archive")]);
        let (_tmp, root) = test_root();
        for (case, bytes, manifest, staged) in [
            ("zero-length", &b""[..], manifest.as_bytes(), false),
            ("truncated", &b"arch"[..], manifest.as_bytes(), false),
            ("empty manifest", &b"archive"[..], &b""[..], false),
            ("complete", &b"archive"[..], manifest.as_bytes(), true),
        ] {
            write_staged(&root, &[(MANIFEST, manifest), (&freenet, bytes)]);
            assert_eq!(freenet_archive_staged(&root, TAG).await, staged, "{case}");
        }
    }

    /// The fallback fires at exactly the third failure in a row, not the
    /// fourth, when the failures are recent. Failures whose retry time is more
    /// than two days past no longer count: a node that tries only now and
    /// then starts afresh rather than falling back on its first new failure.
    #[tokio::test]
    async fn old_failures_stop_counting_toward_the_fallback() {
        use super::super::super::rollback::record_update_failure_at;
        let server = failing_release();
        let now = SystemTime::now();
        let hour = Duration::from_secs(3600);
        let ago = |hours: u32| now - hour * hours;
        for (case, times, expected, failures) in [
            ("recent", [ago(9), ago(5)], Staging::FallingBack, 3),
            ("old", [ago(9 * 24), ago(9 * 24 - 1)], Staging::Deferred, 1),
            // The second retry time (+2 h) is just over two days past.
            ("two days past", [ago(52), ago(51)], Staging::Deferred, 1),
            (
                "under two days past",
                [ago(50), ago(49)],
                Staging::FallingBack,
                3,
            ),
            (
                "a day and more past",
                [ago(30), ago(29)],
                Staging::FallingBack,
                3,
            ),
        ] {
            let (_tmp, root) = test_root();
            let state_dir = root.parent().unwrap();
            fs::create_dir_all(state_dir).unwrap();
            for at in times {
                record_update_failure_at(state_dir, "9.9.9", "earlier", at, 0.5);
            }
            assert_eq!(cooldown(&root).unwrap().failures, 2, "{case}");
            assert_eq!(
                stage_at(&root, &server.url_str("/"), now).await,
                expected,
                "{case}"
            );
            assert_eq!(cooldown(&root).unwrap().failures, failures, "{case}");
        }
    }

    /// A tag lookup refused by a GitHub rate limit holds the release it would
    /// count against, without counting, whether or not the trigger named one.
    #[tokio::test]
    async fn a_rate_limited_lookup_holds_without_counting() {
        use super::super::super::rollback::UNKNOWN_RELEASE;
        let server = Server::run();
        let prefix = server.url_str("/");
        for (target, held) in [(None, UNKNOWN_RELEASE), (Some("9.9.9"), "9.9.9")] {
            let (_tmp, root) = test_root();
            let outcome = stage_into(
                &root,
                "9.9.8",
                || async {
                    Err::<String, _>(
                        GithubRateLimitedError {
                            retry_after: Some(Duration::from_secs(600)),
                        }
                        .into(),
                    )
                },
                target,
                |_: &str| false,
                &test_env(&prefix),
            )
            .await;
            assert_eq!(outcome, Staging::Throttled, "{target:?}");
            assert!(!outcome.preparation().should_exit());
            let record = cooldown(&root).unwrap();
            assert_eq!(
                (record.version.as_str(), record.failures),
                (held, 0),
                "{target:?}"
            );
        }
    }

    /// The exits that leave the download to the updater
    /// because the cache is busy or cannot be locked record first, like every
    /// other exit.
    #[tokio::test]
    async fn a_busy_or_unlockable_cache_records_before_exiting() {
        let server = Server::run(); // no expectations: nothing is downloaded
        let (_tmp, root) = test_root();
        let held = try_lock(&root).unwrap().expect("lock");
        let outcome = stage_into_with(&root, &server, Ok(TAG.into()), None, &|_| false).await;
        drop(held);
        assert_eq!(outcome, Staging::Skipped);
        let record = cooldown(&root).unwrap();
        assert_eq!(
            (record.failures, record.last_error.as_str()),
            (1, "cache busy")
        );

        let (_tmp, root) = test_root();
        fs::create_dir_all(root.with_extension("lock")).unwrap();
        let outcome = stage_into_with(&root, &server, Ok(TAG.into()), None, &|_| false).await;
        assert_eq!(outcome, Staging::Skipped);
        let record = cooldown(&root).unwrap();
        assert_eq!(
            (record.failures, record.last_error.as_str()),
            (1, "cache lock failed")
        );
    }

    /// A run the empty poll bucket stops holds the release and
    /// counts nothing, however often it happens; only a run that reached the
    /// network (or that the byte budget refused) counts.
    #[tokio::test]
    async fn runs_the_empty_bucket_stops_never_count() {
        let server = Server::run(); // no expectations: nothing is requested
        let prefix = server.url_str("/");
        let (_tmp, root) = test_root();
        let state_dir = root.parent().unwrap().to_path_buf();
        fs::create_dir_all(&state_dir).unwrap();
        let t0 = SystemTime::now();
        // Two hours apart, past each hold's (at most 1.2 h) wait.
        for hour in (0..10u64).step_by(2) {
            let now = t0 + Duration::from_secs(hour * 3600 + 60);
            let now_unix = now.duration_since(std::time::UNIX_EPOCH).unwrap().as_secs();
            fs::write(
                state_dir.join("github_poll_bucket_node"),
                format!("0 {now_unix}"),
            )
            .unwrap();
            let clock = move || now;
            let env = StageEnv {
                clock: &clock,
                ..test_env(&prefix)
            };
            let outcome = stage_into(
                &root,
                "9.9.8",
                || async { panic!("looked the tag up with an empty bucket") },
                Some("9.9.9"),
                |_: &str| false,
                &env,
            )
            .await;
            assert_eq!(outcome, Staging::Throttled, "hour {hour}");
            assert!(!outcome.preparation().should_exit(), "hour {hour}");
            assert_eq!(cooldown(&root).unwrap().failures, 0, "hour {hour}");
        }
        // A trigger that names no release stays too, and has nothing to hold.
        let (_tmp, root) = test_root();
        let state_dir = root.parent().unwrap();
        fs::create_dir_all(state_dir).unwrap();
        let now = SystemTime::now();
        let now_unix = now.duration_since(std::time::UNIX_EPOCH).unwrap().as_secs();
        fs::write(
            state_dir.join("github_poll_bucket_node"),
            format!("0 {now_unix}"),
        )
        .unwrap();
        let outcome = stage_into(
            &root,
            "9.9.8",
            || async { panic!("looked the tag up with an empty bucket") },
            None,
            |_: &str| false,
            &test_env(&prefix),
        )
        .await;
        assert_eq!(outcome, Staging::Throttled);
        assert!(!outcome.preparation().should_exit());
        assert!(cooldown(&root).is_none());
    }

    /// The budget's size and window are pinned, and a release
    /// whose freenet archive is already staged is not refused by a spent
    /// budget: there is nothing left to download, so it exits to install.
    #[tokio::test]
    async fn a_spent_budget_still_installs_a_staged_release() {
        use super::super::super::rollback::{DOWNLOAD_BYTES_PER_DAY, charge_download_bytes_at};
        assert_eq!(DOWNLOAD_BYTES_PER_DAY, 320 * 1024 * 1024);
        let freenet = freenet_asset_name();
        let manifest = manifest_for(&[(&freenet, b"archive")]);
        // The manifest is fetched again; the archive is not.
        let server = serve_release(manifest.as_bytes(), None, &[]);
        let (_tmp, root) = test_root();
        write_staged(
            &root,
            &[(MANIFEST, manifest.as_bytes()), (&freenet, b"archive")],
        );
        let now = SystemTime::now();
        charge_download_bytes_at(root.parent().unwrap(), u64::MAX, now);
        assert_eq!(
            stage_at(&root, &server.url_str("/"), now).await,
            Staging::Staged
        );
    }

    /// The install that stages nothing (a macOS app bundle) records before its
    /// exit and honours the cooldown. Platform-independent, so tested here.
    #[tokio::test]
    async fn an_install_that_stages_nothing_records_and_honours_the_cooldown() {
        let (_tmp, root) = test_root();
        let ok = || async { Ok(TAG.to_string()) };
        assert_eq!(
            decide_without_staging(&root, "9.9.8", None, ok, (&[], u64::MAX), &SystemTime::now)
                .await,
            Staging::Skipped
        );
        assert_eq!(cooldown(&root).unwrap().last_error, INSTALL_PENDING);
        assert_eq!(
            decide_without_staging(&root, "9.9.8", None, ok, (&[], u64::MAX), &SystemTime::now)
                .await,
            Staging::CoolingDown
        );
    }

    #[test]
    fn an_updater_skip_holds_the_staged_release_without_counting() {
        let (_tmp, root) = test_root();
        let now = SystemTime::now();
        hold_staged_release_at(&root, "skipped", now, 0.5);
        assert!(cooldown(&root).is_none(), "nothing staged, nothing held");
        write_staged(&root, &[(MANIFEST, b"m")]);
        for _ in 0..5 {
            hold_staged_release_at(&root, "skipped", now, 0.5);
        }
        let record = cooldown(&root).unwrap();
        assert_eq!((record.version.as_str(), record.failures), ("9.9.9", 0));
    }

    #[test]
    fn the_staged_tag_is_the_one_with_a_manifest() {
        let (_tmp, root) = test_root();
        assert_eq!(staged_tag_at(&root), None);
        fs::create_dir_all(root.join("v9.9.8")).unwrap();
        assert_eq!(
            staged_tag_at(&root),
            None,
            "a partial download is not staged"
        );
        write_staged(&root, &[(MANIFEST, b"m")]);
        assert_eq!(staged_tag_at(&root).as_deref(), Some(TAG));
    }

    #[test]
    fn a_contained_panic_records_against_a_known_release() {
        let (_tmp, root) = test_root();
        record_panic_at(&root, None);
        assert!(cooldown(&root).is_none());
        record_panic_at(&root, Some("9.9.9"));
        assert!(cooldown(&root).unwrap().last_error.contains("panicked"));
    }

    #[tokio::test]
    #[cfg(target_os = "linux")]
    async fn failed_write_removes_the_partial_and_is_not_retried() {
        let server = Server::run();
        server.expect(
            Expectation::matching(request::method_path("GET", "/asset"))
                .respond_with(status_code(200).body(vec![7u8; 64 * 1024])),
        );
        let (_tmp, root) = test_root();
        fs::create_dir_all(&root).unwrap();
        let part = root.join("asset.part");
        // Every write to /dev/full fails with ENOSPC, as a full disk would.
        std::os::unix::fs::symlink("/dev/full", &part).unwrap();
        let err = download_resumable(&server.url_str("/asset"), &part, &ByteMeter::unlimited())
            .await
            .unwrap_err();
        assert!(err.downcast_ref::<CacheIoError>().is_some(), "got: {err:#}");
        assert!(!worth_retrying(&err));
        assert!(
            part.symlink_metadata().is_err(),
            "the partial must be removed"
        );
    }

    #[tokio::test]
    async fn resume_survives_a_redirect_to_the_asset_host() {
        // Real downloads are a 302 from github.com to the asset CDN; the range
        // request must reach the server that has the bytes.
        let body: &'static [u8] = b"0123456789abcdefghij";
        let cdn = Server::run();
        cdn.expect(
            Expectation::matching(all_of![
                request::method_path("GET", "/blob"),
                request::headers(contains(("range", "bytes=10-"))),
            ])
            .respond_with(
                status_code(206)
                    .append_header("content-range", "bytes 10-19/20")
                    .body(&body[10..]),
            ),
        );
        let github = Server::run();
        github.expect(
            Expectation::matching(request::method_path("GET", "/asset"))
                .respond_with(status_code(302).append_header("location", cdn.url_str("/blob"))),
        );
        let (_tmp, root) = test_root();
        fs::create_dir_all(&root).unwrap();
        let part = root.join("asset.part");
        fs::write(&part, &body[..10]).unwrap();
        assert!(
            download_resumable(&github.url_str("/asset"), &part, &ByteMeter::unlimited())
                .await
                .unwrap()
        );
        assert_eq!(fs::read(&part).unwrap(), body);
    }

    #[test]
    #[cfg(unix)]
    fn free_space_is_known_on_unix() {
        let (tmp, _) = test_root();
        assert!(free_bytes(tmp.path()).is_some_and(|b| b > 0));
    }

    #[tokio::test]
    async fn a_panic_while_staging_still_lets_the_update_proceed() {
        let mut recorded = false;
        assert_eq!(
            contain_panic(async { panic!("staging bug") }, || recorded = true).await,
            UpdatePreparation::Exit
        );
        assert!(recorded);
        assert_eq!(
            contain_panic(async { UpdatePreparation::Stay }, || panic!("no hook")).await,
            UpdatePreparation::Stay
        );
    }

    /// A tag lookup that fails is retried twice within the run, about 5 s and
    /// 15 s later, so a blip of a few seconds does not cost a whole cooldown;
    /// a run whose lookups all fail counts ONE failure. A rate limit is never
    /// retried. Paused clock: the waits are asserted, not slept.
    #[tokio::test(start_paused = true)]
    async fn a_failed_tag_lookup_is_retried_twice_within_the_run() {
        assert_eq!(
            LOOKUP_RETRY_DELAYS,
            [Duration::from_secs(5), Duration::from_secs(15)]
        );
        let run = |root: PathBuf, error: fn() -> anyhow::Error, succeed_on: usize| async move {
            let mut calls = 0;
            let env = StageEnv {
                lookup_delays: &LOOKUP_RETRY_DELAYS,
                ..test_env("http://127.0.0.1:0/")
            };
            let started = tokio::time::Instant::now();
            let outcome = stage_into(
                &root,
                "9.9.8",
                || {
                    calls += 1;
                    let ok = calls == succeed_on;
                    async move {
                        if ok {
                            Ok("v9.9.8".to_string())
                        } else {
                            Err(error())
                        }
                    }
                },
                Some("9.9.9"),
                |_: &str| false,
                &env,
            )
            .await;
            (outcome, calls, started.elapsed())
        };
        let secs = |d: Duration| d.as_secs_f64();
        // Every lookup fails: three of them, 5 s and 15 s apart (±20%), one
        // failure counted.
        let (_tmp, root) = test_root();
        let (outcome, calls, took) = run(root.clone(), || anyhow::anyhow!("offline"), 0).await;
        assert_eq!((outcome, calls), (Staging::Deferred, 3));
        assert!((16.0..=24.0).contains(&secs(took)), "{took:?}");
        assert_eq!(cooldown(&root).map(|r| r.failures), Some(1));
        // The second lookup answers: the run goes on, 5 s (±20%) later.
        let (_tmp, root) = test_root();
        let (outcome, calls, took) = run(root.clone(), || anyhow::anyhow!("offline"), 2).await;
        assert_eq!((outcome, calls), (Staging::NothingToInstall, 2));
        assert!((4.0..=6.0).contains(&secs(took)), "{took:?}");
        // A rate limit is never retried into.
        let (_tmp, root) = test_root();
        let limited = || GithubRateLimitedError { retry_after: None }.into();
        let (_, calls, took) = run(root.clone(), limited, 0).await;
        assert_eq!((calls, took), (1, Duration::ZERO));
    }

    #[tokio::test(start_paused = true)]
    async fn retries_until_success_then_stops() {
        let mut calls = 0;
        let result = stage_with_retries(
            || {
                calls += 1;
                let n = calls;
                async move {
                    if n < 3 {
                        anyhow::bail!("transient")
                    } else {
                        Ok(())
                    }
                }
            },
            &STAGE_RETRY_DELAYS,
            "test",
        )
        .await;
        assert!(result.is_ok());
        assert_eq!(calls, 3);
    }

    #[tokio::test(start_paused = true)]
    async fn gives_up_after_the_last_retry_with_jittered_waits() {
        let mut attempts = Vec::new();
        let result: Result<()> = stage_with_retries(
            || {
                attempts.push(tokio::time::Instant::now());
                async { anyhow::bail!("down") }
            },
            &STAGE_RETRY_DELAYS,
            "test",
        )
        .await;
        assert!(result.is_err());
        assert_eq!(attempts.len(), STAGE_RETRY_DELAYS.len() + 1);
        let gaps: Vec<Duration> = attempts.windows(2).map(|w| w[1] - w[0]).collect();
        for (gap, nominal) in gaps.iter().zip(STAGE_RETRY_DELAYS) {
            assert!(
                *gap >= nominal.mul_f64(0.8) && *gap <= nominal.mul_f64(1.2),
                "a wait must stay within ±20% of {nominal:?}, took {gap:?}"
            );
        }
        assert!(
            gaps.iter().zip(STAGE_RETRY_DELAYS).any(|(g, n)| *g != n),
            "retry waits must be jittered, got exactly {gaps:?}"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn permanent_failures_end_the_retries_at_once() {
        let permanent: [fn() -> anyhow::Error; 3] = [
            || GithubRateLimitedError { retry_after: None }.into(),
            || ReleaseVerificationError("bad checksum".into()).into(),
            || cache_io("writing")(std::io::Error::other("disk full")).context("wrapped"),
        ];
        for make in permanent {
            let mut calls = 0;
            let result = stage_with_retries(
                || {
                    calls += 1;
                    async move { Err::<(), _>(make()) }
                },
                &STAGE_RETRY_DELAYS,
                "test",
            )
            .await;
            assert!(result.is_err());
            assert_eq!(calls, 1, "{:#} must not be retried", make());
        }
    }

    #[tokio::test(start_paused = true)]
    async fn deadline_bounds_a_download_that_never_ends() {
        let outcome = stage_within(
            std::future::pending::<Result<()>>,
            &STAGE_RETRY_DELAYS,
            STAGE_DEADLINE,
        )
        .await;
        assert!(matches!(outcome, StageOutcome::TimedOut), "got {outcome:?}");
    }

    #[test]
    fn retry_budget_leaves_most_of_the_deadline_for_downloading() {
        let waits: Duration = STAGE_RETRY_DELAYS.iter().sum();
        assert!(waits.mul_f64(1.2) < STAGE_DEADLINE / 2);
    }

    /// The updater's half of #5790 lives in `update.rs`; a staging that the
    /// installer never consults is inert while every test above stays green.
    /// Cross-file, so these needles cannot match their own literals.
    #[test]
    fn installer_installs_from_and_cleans_up_the_cache() {
        let src = include_str!("../update.rs");
        let production = src
            .split_once("\n#[cfg(test)]\nmod tests {")
            .expect("update.rs test module not found")
            .0;
        let method = |sig: &str| -> &str {
            let start = production
                .find(sig)
                .unwrap_or_else(|| panic!("`{sig}` not found in update.rs"));
            let rest = &production[start..];
            let end = rest[sig.len()..]
                .find("\n    }\n")
                .map(|i| i + sig.len())
                .unwrap_or_else(|| panic!("end of `{sig}` not found"));
            &rest[..end]
        };
        // At statement position, so a commented-out call does not count.
        let calls = |body: &str, call: &str| {
            body.lines()
                .map(str::trim)
                .any(|line| line.starts_with(call))
        };

        let install = method("    async fn download_and_install(");
        assert!(
            install.lines().map(str::trim).any(|l| l
                .contains("match staged::load(release, self.quiet).await")
                && !l.starts_with("//")),
            "download_and_install must try the staged release first"
        );
        assert!(
            calls(install, "staged.fdev_archive,"),
            "download_and_install must hand the staged fdev to try_update_fdev"
        );
        let service_file = install
            .find("if let Err(e) = ensure_service_file_updated(&current_exe")
            .expect("service-file refresh not found");
        let fdev = install
            .find("self.try_update_fdev(")
            .expect("fdev update not found");
        assert!(
            service_file < fdev,
            "the service-file refresh must run before the fdev download, which can be \
             killed by TimeoutStopSec on a slow link"
        );

        let run = method("    async fn run_async(");
        let (_, installed) = run
            .split_once("InstallRecord::Clear => {")
            .expect("Installed arm not found");
        let installed = &installed[..installed
            .find("\n            }")
            .expect("end of Installed arm not found")];
        for call in [
            "super::rollback::clear_install_failures();",
            "staged::discard();",
        ] {
            assert!(
                calls(installed, call),
                "the Installed arm must call `{call}`"
            );
        }
        let branch = run
            .find("if !self.force && latest_ver <= current_ver")
            .expect("up-to-date branch not found");
        let (up_to_date, _) = run[branch..]
            .split_once("std::process::exit(EXIT_CODE_ALREADY_UP_TO_DATE);")
            .expect("up-to-date exit not found");
        assert!(
            calls(up_to_date, "staged::discard();"),
            "the already-up-to-date path must discard the cache"
        );
    }
}
