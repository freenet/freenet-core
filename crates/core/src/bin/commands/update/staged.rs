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

use std::fs;
use std::path::{Path, PathBuf};
use std::time::Duration;

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

/// How long the node keeps trying before it gives up and exits 42 anyway,
/// leaving the download to the post-stop updater as before. Until then the
/// node keeps serving on its current version, including after an urgent or
/// isolation trigger that used to exit at once. A partial download survives in
/// the cache, so the next attempt (after the restart) resumes it.
const STAGE_DEADLINE: Duration = Duration::from_secs(2 * 3600);
/// Waits between failed attempts. The number of entries is the number of
/// retries. Only network failures are retried; see [`worth_retrying`].
const STAGE_RETRY_DELAYS: [Duration; 5] = [
    Duration::from_secs(30),
    Duration::from_secs(60),
    Duration::from_secs(120),
    Duration::from_secs(300),
    Duration::from_secs(600),
];
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

/// Whether another attempt could succeed. Everything is retried except three
/// failures that come back the same every time: a rate limit (retrying into it
/// is what escalates it), a release that fails verification, and a cache that
/// cannot be written.
fn worth_retrying(e: &anyhow::Error) -> bool {
    e.downcast_ref::<GithubRateLimitedError>().is_none()
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
/// A POSIX record lock (`fcntl`), not `flock`: a `flock` is shared with every
/// child forked while it is held, until that child execs, so a process that
/// spawns children could keep it held after releasing it. Record locks belong
/// to the process and are never inherited. Two consequences: they do not
/// exclude other threads of the same process (each process stages from one
/// task, so nothing needs that), and closing ANY descriptor of the lock file
/// in this process releases the lock, so nothing else may open it.
///
/// Unix only. Elsewhere this always succeeds; the write order (archive moved
/// into place before the manifest is written) and the installer's checksums
/// still keep a cache that two processes wrote from being installed wrong.
struct CacheLock {
    _file: fs::File,
}

fn try_lock(root: &Path) -> std::io::Result<Option<CacheLock>> {
    if let Some(parent) = root.parent() {
        fs::create_dir_all(parent)?;
    }
    let file = fs::OpenOptions::new()
        .create(true)
        .truncate(false)
        .write(true)
        .open(root.with_extension("lock"))?;
    #[cfg(unix)]
    {
        use std::os::unix::io::AsRawFd;
        // SAFETY: an all-zero `flock` is a valid value; the fields that
        // matter are set below. Whole file (`l_start` = `l_len` = 0).
        let mut request: libc::flock = unsafe { std::mem::zeroed() };
        request.l_type = libc::F_WRLCK as _;
        request.l_whence = libc::SEEK_SET as _;
        // SAFETY: `file` owns an open descriptor for the duration of the call,
        // and `request` is a valid `flock` that fcntl only reads.
        if unsafe { libc::fcntl(file.as_raw_fd(), libc::F_SETLK, &request) } != 0 {
            let e = std::io::Error::last_os_error();
            // POSIX allows either errno for "held by another process".
            return if matches!(e.raw_os_error(), Some(libc::EAGAIN | libc::EACCES)) {
                Ok(None)
            } else {
                Err(e)
            };
        }
    }
    Ok(Some(CacheLock { _file: file }))
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

/// Download and verify the latest release into the cache before the node
/// exits for an update. Never fails and never panics: whatever happens, the
/// caller goes on to exit 42 and the post-stop updater installs, from the cache
/// when this succeeded and from the network otherwise.
///
/// Runs while the node is still serving, so a slow download costs nothing but
/// time. Gives up after [`STAGE_DEADLINE`] so that a problem here can delay an
/// update but never prevent one.
pub(crate) async fn stage_latest_release(current_version: &str) {
    // Before anything can decide to skip: the release canary (Gate B) arms its
    // staging check on this line, so a later regression in any skip condition
    // shows up as "prepared but never downloaded" instead of as a binary that
    // predates staging.
    tracing::info!("Preparing the update before exiting (#5790)");
    contain_panic(stage_latest_release_inner(current_version)).await;
}

/// Run `fut`, turning a panic into a log line. The update-check task sends on
/// `update_tx` only after staging returns; a panic unwinding through it would
/// drop the sender, and the node would exit 0 without ever running the
/// updater, on every attempt.
async fn contain_panic(fut: impl std::future::Future<Output = ()>) {
    use futures::FutureExt;
    if std::panic::AssertUnwindSafe(fut)
        .catch_unwind()
        .await
        .is_err()
    {
        tracing::error!(
            "Downloading the update in advance panicked; exiting for the update anyway"
        );
    }
}

async fn stage_latest_release_inner(current_version: &str) {
    #[cfg(target_os = "macos")]
    {
        // An app-bundle install updates from the DMG, not from these archives.
        let in_bundle = std::env::current_exe()
            .ok()
            .and_then(|exe| super::super::service::macos_app_bundle_path(&exe))
            .is_some();
        if in_bundle {
            return;
        }
    }
    let Some(root) = cache_root() else {
        tracing::warn!("No usable state directory; the update will be downloaded after exit");
        return;
    };
    let verify = |manifest: &[u8], signature: Option<&[u8]>| {
        super::verify_release_manifest_signature(manifest, signature, true)
    };
    stage_into(
        &root,
        RELEASE_DOWNLOAD_PREFIX,
        current_version,
        // Unknown off unix, where the check is skipped.
        root.parent().and_then(free_bytes),
        // The tag the updater will install. Resolved through the same
        // quota-free redirect the updater uses, so both normally see the same
        // tag; if a newer release lands in between, the updater finds no cache
        // for it and downloads as before.
        super::super::auto_update::fetch_latest_release_tag(false),
        version_blocked,
        &verify,
        &STAGE_RETRY_DELAYS,
        STAGE_DEADLINE,
    )
    .await;
}

/// What [`stage_into`] did. Only logged in production; tests assert on it.
#[derive(Debug, PartialEq)]
enum Staging {
    Skipped,
    Staged,
    Failed,
    TimedOut,
}

/// Decide whether to stage and do it. Everything environmental (cache root,
/// download host, free space, tag lookup, the #4073 block list, signature key,
/// retry schedule) is a parameter, so tests can drive every branch without the
/// real state directory or GitHub.
#[allow(clippy::too_many_arguments)]
async fn stage_into(
    root: &Path,
    download_prefix: &str,
    current_version: &str,
    free: Option<u64>,
    resolve_tag: impl std::future::Future<Output = Result<String>>,
    blocked: impl Fn(&str) -> bool,
    verify: ManifestVerifier<'_>,
    delays: &[Duration],
    deadline: Duration,
) -> Staging {
    if let Some(free) = free {
        if free < MIN_FREE_BYTES {
            tracing::warn!(
                free_mib = free / (1024 * 1024),
                "Too little free space to download the update in advance; it will be downloaded after exit"
            );
            return Staging::Skipped;
        }
    }
    let tag = match resolve_tag.await {
        Ok(tag) => tag,
        Err(e) => {
            tracing::warn!(error = %e, "Could not resolve the release to download in advance; the update will be downloaded after exit");
            return Staging::Skipped;
        }
    };
    if !should_stage(&tag, current_version, blocked) {
        tracing::info!(tag = %tag, "No newer installable release to download in advance");
        return Staging::Skipped;
    }
    if tag_dir(root, &tag).is_err() {
        tracing::warn!(tag = %tag, "Release tag is not a plain name; the update will be downloaded after exit");
        return Staging::Skipped;
    }
    let _lock = match try_lock(root) {
        Ok(Some(lock)) => lock,
        Ok(None) => {
            tracing::info!(
                "Another process is downloading the update in advance; leaving it to that"
            );
            return Staging::Skipped;
        }
        Err(e) => {
            tracing::warn!(error = %e, "Cannot lock the update cache; the update will be downloaded after exit");
            return Staging::Skipped;
        }
    };

    tracing::info!(tag = %tag, "Downloading the update before exiting, so the restart only has to install it");
    let started = tokio::time::Instant::now();
    let outcome = stage_within(
        || stage_release_at(root, download_prefix, &tag, verify),
        delays,
        deadline,
    )
    .await;
    match outcome {
        StageOutcome::Staged => {
            tracing::info!(
                "Update downloaded and verified; exiting to install it ({tag}, {}s)",
                started.elapsed().as_secs()
            );
            Staging::Staged
        }
        StageOutcome::Failed(e) => {
            // A rate limit is NOT recorded as a cooldown here: the updater
            // that runs next would honour it and refuse to install at all, so
            // the exit would buy nothing. Left alone, it can still try.
            tracing::warn!(
                tag = %tag,
                error = %format!("{e:#}"),
                "Could not download the update in advance; exiting anyway, the updater will download what is missing"
            );
            Staging::Failed
        }
        StageOutcome::TimedOut => {
            tracing::warn!(
                tag = %tag,
                deadline_secs = deadline.as_secs(),
                "Downloading the update in advance timed out; exiting anyway, a later attempt resumes the partial download"
            );
            Staging::TimedOut
        }
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
    match tokio::time::timeout(deadline, stage_with_retries(attempt, delays)).await {
        Ok(Ok(())) => StageOutcome::Staged,
        Ok(Err(e)) => StageOutcome::Failed(e),
        Err(_) => StageOutcome::TimedOut,
    }
}

/// Run `attempt` until it succeeds, waiting about `delays[i]` (±20%) after the
/// i-th failure, and stopping early on a failure that is not
/// [`worth_retrying`]. Returns the last error when it gives up.
async fn stage_with_retries<F, Fut>(mut attempt: F, delays: &[Duration]) -> Result<()>
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = Result<()>>,
{
    let mut delays = delays.iter();
    loop {
        let e = match attempt().await {
            Ok(()) => return Ok(()),
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
            "Downloading the update in advance failed; will retry"
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
    fetch_verified(&checksums, &url(&freenet), &dir, &freenet).await?;

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
        if let Err(e) = fetch_verified(&checksums, &url(&fdev), &dir, &fdev).await {
            if worth_retrying(&e) && e.downcast_ref::<AssetMissing>().is_none() {
                return Err(e.context("Failed to download fdev"));
            }
            tracing::warn!(error = %format!("{e:#}"), "fdev will be downloaded by the updater");
        }
    }
    Ok(())
}

async fn fetch_verified(checksums: &Checksums, url: &str, dir: &Path, name: &str) -> Result<()> {
    let expected = super::required_checksum(Some(checksums), name)?.to_string();
    let dest = dir.join(name);
    if dest.exists() && verify_file(&dest, &expected).await.is_ok() {
        return Ok(());
    }
    let part = dir.join(format!("{name}.part"));
    let resumed = download_resumable(url, &part).await?;
    if let Err(e) = verify_file(&part, &expected).await {
        // Never keep bytes that cannot be right.
        remove_quietly(&part);
        if !resumed {
            return Err(e);
        }
        // The bytes kept from an earlier attempt may belong to a different
        // upload of the asset. Start over once before calling the release bad.
        download_resumable(url, &part).await?;
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
/// bytes from an earlier attempt. Bounded by a stall timeout only: a slow
/// transfer that keeps making progress is allowed to finish.
async fn download_resumable(url: &str, part: &Path) -> Result<bool> {
    use futures::StreamExt;
    use reqwest::StatusCode;
    use reqwest::header::{CONTENT_RANGE, RANGE};
    use tokio::io::AsyncWriteExt;

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
    let mut file = if have > 0 && status == StatusCode::PARTIAL_CONTENT {
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

    // A local write failure (most likely a full disk) gives the space back
    // rather than keep a partial file nobody can finish. A network failure
    // keeps it, so the next attempt resumes.
    let mut stream = response.bytes_stream();
    let mut write_failure = None;
    while let Some(chunk) = stream.next().await {
        let chunk = chunk.context("Error while downloading")?;
        if let Err(e) = file.write_all(&chunk).await {
            write_failure = Some(("writing the download", e));
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
    Ok(resumed)
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
    // to avoid, which an operator (and the release canary) should see.
    eprintln!("Discarding the update downloaded in advance ({rejected:#}); downloading it again.");
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

    fn release_listing(names: &[&str]) -> Release {
        // Port 9 (discard) refuses at once, so the live-manifest re-check
        // fails fast and offline instead of resolving a real host.
        release_listing_at("http://127.0.0.1:9/", names)
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

        let root = tempfile::tempdir().unwrap();
        // A previously staged release must not linger next to the new one.
        fs::create_dir_all(root.path().join("v0.0.1")).unwrap();
        stage_release_at(root.path(), &server.url_str("/"), TAG, &verify)
            .await
            .expect("staging should succeed");
        assert!(!root.path().join("v0.0.1").exists());

        let signed = release_listing(&[MANIFEST, SIGNATURE, &freenet, &fdev]);
        let staged = load_at(root.path(), &signed, &verify)
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
        fs::remove_file(root.path().join(TAG).join(SIGNATURE)).unwrap();
        assert!(load_at(root.path(), &signed, &verify).await.is_none());
    }

    #[tokio::test]
    async fn restaging_a_staged_release_downloads_nothing_again() {
        // A failed install leaves the cache; the node re-stages after the
        // restart and must not fetch the archive again (`.times(1)` above).
        let freenet = freenet_asset_name();
        let manifest = manifest_for(&[(&freenet, b"archive")]);
        let server = serve_release(manifest.as_bytes(), None, &[(&freenet, b"archive")]);
        let root = tempfile::tempdir().unwrap();
        for _ in 0..2 {
            stage_release_at(root.path(), &server.url_str("/"), TAG, &unsigned)
                .await
                .unwrap();
        }
    }

    #[tokio::test]
    async fn release_without_fdev_still_stages() {
        let freenet = freenet_asset_name();
        let manifest = manifest_for(&[(&freenet, b"archive")]);
        let server = serve_release(manifest.as_bytes(), None, &[(&freenet, b"archive")]);
        let root = tempfile::tempdir().unwrap();
        stage_release_at(root.path(), &server.url_str("/"), TAG, &unsigned)
            .await
            .unwrap();
        let release = release_listing(&[MANIFEST, &freenet]);
        let staged = load_at(root.path(), &release, &unsigned).await.unwrap();
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
        let root = tempfile::tempdir().unwrap();
        let err = stage_release_at(root.path(), &server.url_str("/"), TAG, &unsigned)
            .await
            .expect_err("a transient fdev failure should fail the attempt");
        assert!(worth_retrying(&err), "a 503 should be retried, got {err:#}");
        // ...but the freenet archive is already usable by the updater.
        let release = release_listing(&[MANIFEST, &freenet]);
        let staged = load_at(root.path(), &release, &unsigned)
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

        let resumed = download_resumable(&server.url_str("/asset"), &part)
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
            download_resumable(&server.url_str("/asset"), &part)
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

        let resumed = download_resumable(&server.url_str("/asset"), &part)
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

        let resumed = download_resumable(&server.url_str("/asset"), &part)
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
        let root = tempfile::tempdir().unwrap();
        let err = stage_release_at(root.path(), &server.url_str("/"), TAG, &unsigned)
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
        let err = download_resumable(&archive.url_str("/asset"), &root.path().join("a.part"))
            .await
            .unwrap_err();
        assert!(err.downcast_ref::<GithubRateLimitedError>().is_some());
    }

    #[tokio::test]
    async fn corrupt_download_is_not_kept_or_retried() {
        let freenet = freenet_asset_name();
        let manifest = manifest_for(&[(&freenet, b"good bytes")]);
        let server = serve_release(manifest.as_bytes(), None, &[(&freenet, b"tampered!!")]);

        let root = tempfile::tempdir().unwrap();
        let err = stage_release_at(root.path(), &server.url_str("/"), TAG, &unsigned)
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
        let dir = root.path().join(TAG);
        assert!(!dir.join(format!("{freenet}.part")).exists());
        assert!(!dir.join(&freenet).exists());
        // No manifest either, so the updater sees no staged release at all.
        let release = release_listing(&[MANIFEST, &freenet]);
        assert!(load_at(root.path(), &release, &unsigned).await.is_none());
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
        let root = tempfile::tempdir().unwrap();
        let dir = root.path().join(TAG);
        fs::create_dir_all(&dir).unwrap();
        fs::write(dir.join(format!("{freenet}.part")), b"old").unwrap();

        stage_release_at(root.path(), &server.url_str("/"), TAG, &unsigned)
            .await
            .unwrap();
        assert_eq!(fs::read(dir.join(&freenet)).unwrap(), b"current upload");
    }

    #[tokio::test]
    async fn unwritable_cache_is_not_retried() {
        let freenet = freenet_asset_name();
        let manifest = manifest_for(&[(&freenet, b"archive")]);
        let server = serve_release(manifest.as_bytes(), None, &[]);
        let root = tempfile::tempdir().unwrap();
        // A file where the tag directory should be.
        fs::write(root.path().join(TAG), b"").unwrap();
        let err = stage_release_at(root.path(), &server.url_str("/"), TAG, &unsigned)
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
        let root = tempfile::tempdir().unwrap();
        let err = stage_release_at(root.path(), &server.url_str("/"), TAG, &unsigned)
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
            let root = tempfile::tempdir().unwrap();
            stage_release_at(root.path(), &server.url_str("/"), TAG, &unsigned)
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
        let root = tempfile::tempdir().unwrap();
        let cache = root.path().join(CACHE_DIR);
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
        let root = tempfile::tempdir().unwrap();
        let manifest = manifest_for(&[(&freenet, b"archive")]);
        write_staged(
            root.path(),
            &[
                (MANIFEST, manifest.as_bytes()),
                (SIGNATURE, &[7u8; 64]),
                (&freenet, b"archive"),
            ],
        );
        let signed = release_listing(&[MANIFEST, SIGNATURE, &freenet]);
        assert!(load_at(root.path(), &signed, &verify).await.is_none());
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
        let root = tempfile::tempdir().unwrap();
        write_staged(root.path(), &files);
        let mut newer = release_listing(&[MANIFEST, &freenet]);
        newer.tag_name = "v10.0.0".to_string();
        assert!(load_at(root.path(), &newer, &unsigned).await.is_none());
        // ...while the matching tag loads, so the miss above is the tag.
        let matching = release_listing(&[MANIFEST, &freenet]);
        assert!(load_at(root.path(), &matching, &unsigned).await.is_some());

        // A release the network path would refuse, the cache refuses too.
        for listing in [vec![freenet.as_str()], vec![MANIFEST]] {
            let root = tempfile::tempdir().unwrap();
            write_staged(root.path(), &files);
            let release = release_listing(&listing);
            assert!(
                load_at(root.path(), &release, &unsigned).await.is_none(),
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
        let root = tempfile::tempdir().unwrap();
        write_staged(
            root.path(),
            &[
                (MANIFEST, staged_manifest.as_bytes()),
                (&freenet, b"first upload"),
            ],
        );
        let release = release_listing_at(&server.url_str("/m/"), &[MANIFEST, &freenet]);
        assert!(load_at(root.path(), &release, &unsigned).await.is_none());
        assert!(!root.path().join(TAG).exists());
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
        let root = tempfile::tempdir().unwrap();
        write_staged(
            root.path(),
            &[(MANIFEST, manifest.as_bytes()), (&freenet, b"archive")],
        );
        let release = release_listing_at(&server.url_str("/m/"), &[MANIFEST, &freenet]);
        assert!(load_at(root.path(), &release, &unsigned).await.is_some());
    }

    #[tokio::test]
    async fn unverifiable_fdev_is_left_to_the_network() {
        let freenet = freenet_asset_name();
        let fdev = fdev_asset_name();
        let root = tempfile::tempdir().unwrap();
        let manifest = manifest_for(&[(&freenet, b"archive"), (&fdev, b"fdev")]);
        write_staged(
            root.path(),
            &[
                (MANIFEST, manifest.as_bytes()),
                (&freenet, b"archive"),
                (&fdev, b"wrong"),
            ],
        );
        let release = release_listing(&[MANIFEST, &freenet, &fdev]);
        let staged = load_at(root.path(), &release, &unsigned)
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

    /// Drive `stage_into` with everything environmental fixed by the test.
    async fn stage_into_with(
        root: &Path,
        server: &Server,
        tag: Result<String>,
        free: Option<u64>,
        blocked: &dyn Fn(&str) -> bool,
    ) -> Staging {
        stage_into(
            root,
            &server.url_str("/"),
            "9.9.8",
            free,
            async move { tag },
            blocked,
            &unsigned,
            &[],
            STAGE_DEADLINE,
        )
        .await
    }

    #[tokio::test]
    async fn stage_into_stages_a_newer_release() {
        let freenet = freenet_asset_name();
        let manifest = manifest_for(&[(&freenet, b"archive")]);
        let server = serve_release(manifest.as_bytes(), None, &[(&freenet, b"archive")]);
        let root = tempfile::tempdir().unwrap();
        let outcome = stage_into_with(
            root.path(),
            &server,
            Ok(TAG.into()),
            Some(u64::MAX),
            &|_| false,
        )
        .await;
        assert_eq!(outcome, Staging::Staged);
        let release = release_listing(&[MANIFEST, &freenet]);
        assert!(load_at(root.path(), &release, &unsigned).await.is_some());
    }

    #[tokio::test]
    async fn stage_into_skips_without_downloading() {
        // No expectations: any request to this server fails the test.
        let server = Server::run();
        let root = tempfile::tempdir().unwrap();
        let none = |_: &str| false;
        type Case<'a> = (
            &'a str,
            Result<String>,
            Option<u64>,
            &'a dyn Fn(&str) -> bool,
        );
        let cases: [Case<'_>; 5] = [
            ("low disk", Ok(TAG.into()), Some(MIN_FREE_BYTES - 1), &none),
            (
                "tag lookup failed",
                Err(anyhow::anyhow!("offline")),
                None,
                &none,
            ),
            ("not newer", Ok("v9.9.8".into()), None, &none),
            ("blocked", Ok(TAG.into()), None, &|v| v == "9.9.9"),
            ("unsafe tag", Ok("v9.9.9/../x".into()), None, &none),
        ];
        for (case, tag, free, blocked) in cases {
            let outcome = stage_into_with(root.path(), &server, tag, free, blocked).await;
            assert_eq!(outcome, Staging::Skipped, "{case}");
        }
    }

    /// `stage_latest_release` is the only entry point the node calls; the
    /// marker and the panic containment must be in it, not just exist.
    #[test]
    fn entry_point_logs_the_canary_marker_and_contains_panics() {
        let src = include_str!("staged.rs");
        let production = src
            .split_once(concat!("#[cfg(test)]\n", "mod tests {"))
            .expect("test module not found")
            .0;
        let start = production
            .find("pub(crate) async fn stage_latest_release(")
            .expect("entry point not found");
        let body = &production[start..];
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
                "contain_panic(stage_latest_release_inner(current_version)).await;",
            ],
            "stage_latest_release must log the marker, then run staging inside contain_panic"
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
        let root = tempfile::tempdir().unwrap();
        for tag in ["v0.2.140", "v0.2.141", "v0.2.142", "junk"] {
            fs::create_dir_all(root.path().join(tag)).unwrap();
        }
        fs::create_dir_all(root.path().join("v0.2.143")).unwrap();
        discard_stale_at(root.path(), "0.2.141", |v| v == "0.2.143");
        let mut left: Vec<_> = fs::read_dir(root.path())
            .unwrap()
            .map(|e| e.unwrap().file_name().into_string().unwrap())
            .collect();
        left.sort();
        assert_eq!(left, ["v0.2.142"]);
    }

    /// A record lock only excludes OTHER processes, so the holder here is a
    /// forked child. Between fork and _exit it makes only async-signal-safe
    /// calls (open, fcntl, read, write), as a fork of a threaded process must.
    #[tokio::test]
    #[cfg(unix)]
    async fn cache_lock_excludes_another_process() {
        use std::os::unix::ffi::OsStrExt;
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path().join(CACHE_DIR);
        let path =
            std::ffi::CString::new(root.with_extension("lock").as_os_str().as_bytes()).unwrap();
        let (mut ready, mut release) = ([0i32; 2], [0i32; 2]);
        // SAFETY: a valid two-element array for pipe(2) to fill.
        assert_eq!(unsafe { libc::pipe(ready.as_mut_ptr()) }, 0);
        // SAFETY: as above.
        assert_eq!(unsafe { libc::pipe(release.as_mut_ptr()) }, 0);

        // SAFETY: the child only calls async-signal-safe functions on data
        // prepared before the fork, then _exits without unwinding.
        let pid = unsafe { libc::fork() };
        assert!(pid >= 0, "fork failed");
        if pid == 0 {
            // SAFETY: async-signal-safe calls only, on a NUL-terminated path
            // and descriptors created before the fork; `_exit` never returns.
            unsafe {
                let fd = libc::open(path.as_ptr(), libc::O_RDWR | libc::O_CREAT, 0o600);
                let mut request: libc::flock = std::mem::zeroed();
                request.l_type = libc::F_WRLCK as _;
                request.l_whence = libc::SEEK_SET as _;
                let locked = [u8::from(
                    fd >= 0 && libc::fcntl(fd, libc::F_SETLK, &request) == 0,
                )];
                libc::write(ready[1], locked.as_ptr().cast(), 1);
                let mut go = [0u8];
                libc::read(release[0], go.as_mut_ptr().cast(), 1);
                libc::_exit(0);
            }
        }
        let mut locked = [0u8];
        assert_eq!(
            // SAFETY: reading one byte into a one-byte buffer.
            unsafe { libc::read(ready[0], locked.as_mut_ptr().cast(), 1) },
            1
        );
        assert_eq!(locked[0], 1, "the child could not take the lock");

        assert!(
            try_lock(&root).unwrap().is_none(),
            "a lock held by another process must be refused"
        );
        let server = Server::run(); // no expectations: staging must not download
        let outcome = stage_into_with(&root, &server, Ok(TAG.into()), None, &|_| false).await;
        assert_eq!(outcome, Staging::Skipped);

        // SAFETY: writing one byte, then reaping our own child.
        unsafe {
            libc::write(release[1], [1u8].as_ptr().cast(), 1);
            let mut status = 0;
            assert_eq!(libc::waitpid(pid, &mut status, 0), pid);
            for fd in ready.into_iter().chain(release) {
                libc::close(fd);
            }
        }
        assert!(
            try_lock(&root).unwrap().is_some(),
            "the lock must be free once its holder exits"
        );
    }

    #[tokio::test]
    async fn a_panic_while_staging_still_lets_the_update_proceed() {
        contain_panic(async { panic!("staging bug") }).await;
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
        )
        .await;
        assert!(result.is_ok());
        assert_eq!(calls, 3);
    }

    #[tokio::test(start_paused = true)]
    async fn gives_up_after_the_last_retry_with_jittered_waits() {
        let mut calls = 0;
        let started = tokio::time::Instant::now();
        let result = stage_with_retries(
            || {
                calls += 1;
                async { anyhow::bail!("down") }
            },
            &STAGE_RETRY_DELAYS,
        )
        .await;
        assert!(result.is_err());
        assert_eq!(calls, STAGE_RETRY_DELAYS.len() + 1);
        let nominal: Duration = STAGE_RETRY_DELAYS.iter().sum();
        let waited = started.elapsed();
        assert!(
            waited >= nominal.mul_f64(0.8) && waited <= nominal.mul_f64(1.2),
            "waits must stay within ±20% of {nominal:?}, took {waited:?}"
        );
        // Five independent uniform draws never land exactly on the nominal sum,
        // so equality means the jitter is gone.
        assert_ne!(waited, nominal, "retry waits must be jittered");
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
                    async move { Err(make()) }
                },
                &STAGE_RETRY_DELAYS,
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
            .split_once("Ok(InstallOutcome::Installed) => {")
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
