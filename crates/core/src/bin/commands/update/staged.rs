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
//! directory, with no deadline beyond a stall timeout and resuming a partial
//! file left by an earlier attempt. The updater then installs from the cache,
//! which takes seconds.
//!
//! The cache is never trusted. [`load`] re-runs the same signature and
//! checksum verification over the cached bytes that the installer runs over a
//! fresh download, and any problem (missing file, bad signature, wrong hash)
//! discards the cache and falls back to downloading, exactly as before this
//! existed. So the worst a damaged or stale cache can do is cost the download
//! it was meant to save.

use std::fs::{self, File, OpenOptions};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::time::Duration;

use anyhow::{Context, Result};

use super::{Checksums, Release};

/// Directory under the auto-update state directory holding at most one
/// release, in a subdirectory named after its tag.
const CACHE_DIR: &str = "staged_update";
const MANIFEST: &str = "SHA256SUMS.txt";
const SIGNATURE: &str = "SHA256SUMS.txt.sig";
/// The asset-download path GitHub serves for a tag. Unlike the asset list this
/// is not an `api.github.com` request, so staging spends no REST quota (#5102).
const RELEASE_DOWNLOAD_PREFIX: &str = "https://github.com/freenet/freenet-core/releases/download/";

/// How long the node keeps retrying the download before it gives up and exits
/// 42 anyway, leaving the download to the post-stop updater as before. A
/// partial download survives in the cache, so the next attempt (after the
/// restart) resumes it rather than starting over.
const STAGE_DEADLINE: Duration = Duration::from_secs(2 * 3600);
/// Waits between failed attempts. The number of entries is the number of
/// retries; a failed attempt usually leaves a partial file to resume.
const STAGE_RETRY_DELAYS: [Duration; 5] = [
    Duration::from_secs(30),
    Duration::from_secs(60),
    Duration::from_secs(120),
    Duration::from_secs(300),
    Duration::from_secs(600),
];

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
/// install.
pub(super) fn discard() {
    if let Some(root) = cache_root() {
        discard_at(&root);
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

// ── Node side: download before exiting ──────────────────────────────────────

/// Download and verify the latest release into the cache before the node
/// exits for an update. Never fails: whatever happens, the caller goes on to
/// exit 42 and the post-stop updater installs, from the cache when this
/// succeeded and from the network otherwise.
///
/// Runs while the node is still serving, so a slow download costs nothing but
/// time. Gives up after [`STAGE_DEADLINE`] so that a problem here can delay an
/// update but never prevent one.
pub(crate) async fn stage_latest_release(current_version: &str) {
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
    // The tag the updater will install. Resolved through the same quota-free
    // redirect the updater uses, so both normally see the same tag; if a newer
    // release lands in between, the updater finds no cache for it and
    // downloads as before.
    let tag = match super::super::auto_update::fetch_latest_release_tag(false).await {
        Ok(tag) => tag,
        Err(e) => {
            tracing::warn!(error = %e, "Could not resolve the release to download in advance; the update will be downloaded after exit");
            return;
        }
    };
    let newer = semver::Version::parse(super::super::auto_update::version_from_tag(&tag))
        .ok()
        .zip(semver::Version::parse(current_version).ok())
        .is_some_and(|(latest, current)| latest > current);
    if !newer {
        // Nothing to download: the exit is a fallback (e.g. the gateway-trust
        // path) and the updater will decide for itself.
        return;
    }

    tracing::info!(tag = %tag, "Downloading the update before exiting, so the restart only has to install it");
    let started = std::time::Instant::now();
    let staged = tokio::time::timeout(
        STAGE_DEADLINE,
        stage_with_retries(
            || stage_release_at(&root, RELEASE_DOWNLOAD_PREFIX, &tag),
            &STAGE_RETRY_DELAYS,
        ),
    )
    .await;
    match staged {
        Ok(true) => tracing::info!(
            tag = %tag,
            elapsed_secs = started.elapsed().as_secs(),
            "Update downloaded and verified; exiting to install it"
        ),
        Ok(false) => tracing::warn!(
            tag = %tag,
            "Could not download the update in advance; exiting anyway, the updater will download it"
        ),
        Err(_) => tracing::warn!(
            tag = %tag,
            deadline_secs = STAGE_DEADLINE.as_secs(),
            "Downloading the update in advance timed out; exiting anyway, a later attempt resumes the partial download"
        ),
    }
}

/// Run `attempt` until it succeeds, waiting `delays[i]` after the i-th
/// failure. Returns whether an attempt succeeded. A GitHub rate limit ends the
/// retries at once: retrying into it is what escalates it.
async fn stage_with_retries<F, Fut>(mut attempt: F, delays: &[Duration]) -> bool
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = Result<()>>,
{
    let mut delays = delays.iter();
    loop {
        match attempt().await {
            Ok(()) => return true,
            Err(e) => {
                if e.downcast_ref::<super::super::auto_update::GithubRateLimitedError>()
                    .is_some()
                {
                    tracing::warn!(error = %e, "Downloading the update in advance was rate-limited");
                    return false;
                }
                let Some(delay) = delays.next() else {
                    tracing::warn!(error = %e, "Downloading the update in advance failed; giving up");
                    return false;
                };
                // ±20% so nodes that failed together do not retry together.
                let delay = delay.mul_f64(freenet::config::GlobalRng::random_range(0.8_f64..1.2));
                tracing::warn!(
                    error = %e,
                    retry_in_secs = delay.as_secs(),
                    "Downloading the update in advance failed; will retry"
                );
                // Interrupted by the caller's task being aborted at shutdown.
                tokio::time::sleep(delay).await;
            }
        }
    }
}

/// Download `tag`'s manifest, signature and archives from `download_prefix`
/// into `root/<tag>/`, verifying the manifest signature and each archive's
/// checksum. An archive already in the cache with the right checksum is kept;
/// a partial one is resumed.
async fn stage_release_at(root: &Path, download_prefix: &str, tag: &str) -> Result<()> {
    let dir = tag_dir(root, tag)?;
    remove_other_tags(root, tag);
    fs::create_dir_all(&dir).with_context(|| format!("Failed to create {}", dir.display()))?;
    let url = |name: &str| format!("{download_prefix}{tag}/{name}");

    let manifest = super::download_bytes(&url(MANIFEST))
        .await
        .context("Failed to download SHA256SUMS.txt")?;
    let signature = super::download_optional_bytes(&url(SIGNATURE))
        .await
        .context("Failed to download SHA256SUMS.txt.sig")?;
    super::verify_release_manifest_signature(&manifest, signature.as_deref(), true)?;
    let checksums = Checksums::parse(&String::from_utf8_lossy(&manifest));

    let freenet = freenet_asset_name();
    fetch_verified(&checksums, &url(&freenet), &dir, &freenet).await?;
    // fdev is best-effort for the installer, so it is here too: without it the
    // updater downloads fdev itself, after the freenet binary is installed.
    let fdev = fdev_asset_name();
    if let Err(e) = fetch_verified(&checksums, &url(&fdev), &dir, &fdev).await {
        tracing::warn!(error = %e, "Could not download fdev in advance");
    }

    // Written last, so a cache that has a manifest also has the archive.
    write_atomic(&dir.join(MANIFEST), &manifest)?;
    match signature {
        Some(sig) => write_atomic(&dir.join(SIGNATURE), &sig)?,
        None => match fs::remove_file(dir.join(SIGNATURE)) {
            Ok(()) => {}
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
            Err(e) => return Err(e).context("Failed to remove a stale staged signature"),
        },
    }
    Ok(())
}

async fn fetch_verified(checksums: &Checksums, url: &str, dir: &Path, name: &str) -> Result<()> {
    let expected = super::required_checksum(Some(checksums), name)?;
    let dest = dir.join(name);
    if dest.exists() && super::verify_checksum(&dest, expected).is_ok() {
        return Ok(());
    }
    let part = dir.join(format!("{name}.part"));
    download_resumable(url, &part).await?;
    if let Err(e) = super::verify_checksum(&part, expected) {
        // Never resume from bytes that cannot be right.
        #[allow(clippy::let_underscore_must_use)]
        let _ = fs::remove_file(&part);
        return Err(e);
    }
    fs::rename(&part, &dest).with_context(|| format!("Failed to move {name} into place"))?;
    Ok(())
}

/// Download `url` into `part`, continuing from the bytes already there when
/// the server honours a range request. Bounded by a stall timeout only: a
/// slow transfer that keeps making progress is allowed to finish.
async fn download_resumable(url: &str, part: &Path) -> Result<()> {
    use futures::StreamExt;
    use reqwest::StatusCode;
    use reqwest::header::{CONTENT_RANGE, RANGE};

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

    let mut file = if have > 0 && status == StatusCode::PARTIAL_CONTENT {
        // Append only if the server is continuing from exactly where we are.
        let resumes_here = response
            .headers()
            .get(CONTENT_RANGE)
            .and_then(|v| v.to_str().ok())
            .is_some_and(|v| v.starts_with(&format!("bytes {have}-")));
        if !resumes_here {
            #[allow(clippy::let_underscore_must_use)]
            let _ = fs::remove_file(part);
            anyhow::bail!("server resumed the download at an unexpected offset");
        }
        OpenOptions::new()
            .append(true)
            .open(part)
            .context("Failed to reopen partial download")?
    } else if have > 0 && status == StatusCode::RANGE_NOT_SATISFIABLE {
        // The partial file already holds the whole asset; the caller's checksum
        // decides whether it is right.
        return Ok(());
    } else if status.is_success() {
        File::create(part).context("Failed to create download file")?
    } else {
        anyhow::bail!("Download failed: {status}");
    };

    let mut stream = response.bytes_stream();
    while let Some(chunk) = stream.next().await {
        let chunk = chunk.context("Error while downloading")?;
        file.write_all(&chunk)?;
    }
    file.sync_all()?;
    Ok(())
}

fn write_atomic(path: &Path, bytes: &[u8]) -> Result<()> {
    let tmp = path.with_extension("tmp");
    fs::write(&tmp, bytes).with_context(|| format!("Failed to write {}", tmp.display()))?;
    fs::rename(&tmp, path).with_context(|| format!("Failed to write {}", path.display()))?;
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
}

/// The cached copy of `release`, if there is a complete one that passes the
/// same verification a fresh download must pass. Anything else discards the
/// cache and returns `None`, so the updater downloads instead.
pub(super) fn load(release: &Release, quiet: bool) -> Option<StagedRelease> {
    load_at(&cache_root()?, release, quiet)
}

fn load_at(root: &Path, release: &Release, quiet: bool) -> Option<StagedRelease> {
    let dir = tag_dir(root, &release.tag_name).ok()?;
    if !dir.join(MANIFEST).exists() {
        return None;
    }
    match verify_staged(&dir, release, quiet) {
        Ok(staged) => Some(staged),
        Err(e) => {
            tracing::warn!(error = %e, tag = %release.tag_name, "Discarding the staged update; downloading instead");
            if !quiet {
                eprintln!(
                    "Discarding the update downloaded in advance ({e}); downloading it again."
                );
            }
            discard_at(root);
            None
        }
    }
}

fn verify_staged(dir: &Path, release: &Release, quiet: bool) -> Result<StagedRelease> {
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
    super::verify_release_manifest_signature(&manifest, signature.as_deref(), quiet)?;
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

    fn release_listing(names: &[&str]) -> Release {
        Release {
            tag_name: TAG.to_string(),
            assets: names
                .iter()
                .map(|name| Asset {
                    name: name.to_string(),
                    browser_download_url: format!("https://unused.invalid/{name}"),
                })
                .collect(),
        }
    }

    fn path(tag_and_name: &str) -> String {
        format!("/{tag_and_name}")
    }

    /// A server publishing `TAG` with the given manifest, no signature, and
    /// the given archives.
    fn serve_release(manifest: &str, archives: &[(&str, &'static [u8])]) -> Server {
        let server = Server::run();
        server.expect(
            Expectation::matching(request::method_path(
                "GET",
                path(&format!("{TAG}/{MANIFEST}")),
            ))
            .times(..)
            .respond_with(status_code(200).body(manifest.to_string())),
        );
        server.expect(
            Expectation::matching(request::method_path(
                "GET",
                path(&format!("{TAG}/{SIGNATURE}")),
            ))
            .times(..)
            .respond_with(status_code(404)),
        );
        for (name, bytes) in archives {
            server.expect(
                Expectation::matching(request::method_path("GET", path(&format!("{TAG}/{name}"))))
                    .times(..)
                    .respond_with(status_code(200).body(*bytes)),
            );
        }
        server
    }

    #[tokio::test]
    async fn staged_release_is_installed_from_the_cache() {
        static FREENET: &[u8] = b"freenet archive bytes";
        static FDEV: &[u8] = b"fdev archive bytes";
        let freenet = freenet_asset_name();
        let fdev = fdev_asset_name();
        let manifest = manifest_for(&[(&freenet, FREENET), (&fdev, FDEV)]);
        let freenet_static: &'static str = Box::leak(freenet.clone().into_boxed_str());
        let fdev_static: &'static str = Box::leak(fdev.clone().into_boxed_str());
        let server = serve_release(&manifest, &[(freenet_static, FREENET), (fdev_static, FDEV)]);

        let root = tempfile::tempdir().unwrap();
        // A previously staged release must not linger next to the new one.
        fs::create_dir_all(root.path().join("v0.0.1")).unwrap();
        stage_release_at(root.path(), &server.url_str("/"), TAG)
            .await
            .expect("staging should succeed");
        assert!(!root.path().join("v0.0.1").exists());

        let release = release_listing(&[MANIFEST, &freenet, &fdev]);
        let staged = load_at(root.path(), &release, true).expect("cache should verify");
        assert_eq!(fs::read(&staged.freenet_archive).unwrap(), FREENET);
        assert_eq!(fs::read(staged.fdev_archive.unwrap()).unwrap(), FDEV);
        assert_eq!(
            staged.checksums.get(&freenet),
            Some(sha256_hex(FREENET).as_str())
        );
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

        download_resumable(&server.url_str("/asset"), &part)
            .await
            .unwrap();
        assert_eq!(fs::read(&part).unwrap(), body);
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

        download_resumable(&server.url_str("/asset"), &part)
            .await
            .unwrap();
        assert_eq!(fs::read(&part).unwrap(), body);
    }

    #[tokio::test]
    async fn corrupt_download_is_not_kept_for_resuming() {
        static GOOD: &[u8] = b"good bytes";
        static BAD: &[u8] = b"tampered!!";
        let freenet = freenet_asset_name();
        let manifest = manifest_for(&[(&freenet, GOOD)]);
        let freenet_static: &'static str = Box::leak(freenet.clone().into_boxed_str());
        let server = serve_release(&manifest, &[(freenet_static, BAD)]);

        let root = tempfile::tempdir().unwrap();
        let err = stage_release_at(root.path(), &server.url_str("/"), TAG)
            .await
            .expect_err("a checksum mismatch must fail staging");
        assert!(err.to_string().contains("Checksum"), "got: {err:#}");
        let dir = root.path().join(TAG);
        assert!(!dir.join(format!("{freenet}.part")).exists());
        assert!(!dir.join(&freenet).exists());
        // No manifest either, so the updater sees no staged release at all.
        assert!(load_at(root.path(), &release_listing(&[MANIFEST, &freenet]), true).is_none());
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

    #[test]
    fn tampered_cache_is_discarded_and_not_installed() {
        let freenet = freenet_asset_name();
        let root = tempfile::tempdir().unwrap();
        let cache = root.path().join(CACHE_DIR);
        let manifest = manifest_for(&[(&freenet, b"genuine")]);
        write_staged(
            &cache,
            &[(MANIFEST, manifest.as_bytes()), (&freenet, b"swapped")],
        );

        let release = release_listing(&[MANIFEST, &freenet]);
        assert!(load_at(&cache, &release, true).is_none());
        assert!(
            !cache.exists(),
            "a cache that fails verification must be removed"
        );
    }

    #[test]
    fn missing_signature_does_not_downgrade_a_signed_release() {
        // The release publishes a signature; the cache lacks it. Accepting
        // the cache would install from an unauthenticated manifest that the
        // network path would have authenticated.
        let freenet = freenet_asset_name();
        let root = tempfile::tempdir().unwrap();
        let manifest = manifest_for(&[(&freenet, b"archive")]);
        write_staged(
            root.path(),
            &[(MANIFEST, manifest.as_bytes()), (&freenet, b"archive")],
        );

        let signed = release_listing(&[MANIFEST, SIGNATURE, &freenet]);
        assert!(load_at(root.path(), &signed, true).is_none());
    }

    #[test]
    fn invalid_signature_is_rejected() {
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
        assert!(load_at(root.path(), &signed, true).is_none());
    }

    #[test]
    fn cache_for_another_tag_is_not_used() {
        let freenet = freenet_asset_name();
        let root = tempfile::tempdir().unwrap();
        let manifest = manifest_for(&[(&freenet, b"archive")]);
        write_staged(
            root.path(),
            &[(MANIFEST, manifest.as_bytes()), (&freenet, b"archive")],
        );

        let mut newer = release_listing(&[MANIFEST, &freenet]);
        newer.tag_name = "v10.0.0".to_string();
        assert!(load_at(root.path(), &newer, true).is_none());
        // ...while the matching tag still loads, so the miss above is the tag.
        assert!(load_at(root.path(), &release_listing(&[MANIFEST, &freenet]), true).is_some());
    }

    #[test]
    fn unverifiable_fdev_is_left_to_the_network() {
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
        let staged = load_at(
            root.path(),
            &release_listing(&[MANIFEST, &freenet, &fdev]),
            true,
        )
        .expect("a bad fdev must not reject the freenet archive");
        assert!(staged.fdev_archive.is_none());
    }

    #[test]
    fn tags_that_are_not_plain_path_components_are_refused() {
        let root = Path::new("/cache");
        for bad in ["", ".", "..", "../etc", "v1/../../x", "v1\\x", ".hidden"] {
            assert!(tag_dir(root, bad).is_err(), "{bad:?} must be refused");
        }
        assert_eq!(tag_dir(root, "v0.2.141").unwrap(), root.join("v0.2.141"));
    }

    #[tokio::test(start_paused = true)]
    async fn retries_until_success_then_stops() {
        let mut calls = 0;
        let ok = stage_with_retries(
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
        assert!(ok);
        assert_eq!(calls, 3);
    }

    #[tokio::test(start_paused = true)]
    async fn gives_up_after_the_last_retry() {
        let mut calls = 0;
        let ok = stage_with_retries(
            || {
                calls += 1;
                async { anyhow::bail!("down") }
            },
            &STAGE_RETRY_DELAYS,
        )
        .await;
        assert!(!ok);
        assert_eq!(calls, STAGE_RETRY_DELAYS.len() + 1);
    }

    #[tokio::test(start_paused = true)]
    async fn rate_limit_ends_the_retries_at_once() {
        let mut calls = 0;
        let ok = stage_with_retries(
            || {
                calls += 1;
                async {
                    Err(super::super::super::auto_update::GithubRateLimitedError {
                        retry_after: None,
                    }
                    .into())
                }
            },
            &STAGE_RETRY_DELAYS,
        )
        .await;
        assert!(!ok);
        assert_eq!(calls, 1);
    }

    #[test]
    fn retry_budget_fits_inside_the_deadline() {
        // The deadline is the backstop; the retries are meant to run out first
        // on a link that keeps failing quickly, leaving the rest for downloads.
        let waits: Duration = STAGE_RETRY_DELAYS.iter().sum();
        assert!(waits < STAGE_DEADLINE / 2);
    }
}
