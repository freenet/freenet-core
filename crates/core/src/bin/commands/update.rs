//! Self-update functionality for Freenet binary.

use anyhow::{Context, Result};
use clap::Args;
use semver::Version;
use std::fs::{self, File};
use std::io::{self, Write};
use std::path::{Path, PathBuf};
use std::process::Command;

#[cfg(target_os = "macos")]
use super::service::generate_wrapper_script;
#[cfg(target_os = "linux")]
use super::service::{generate_system_service_file, generate_user_service_file};

const GITHUB_API_URL: &str = "https://api.github.com/repos/freenet/freenet-core/releases/latest";

/// Exit code returned when the binary is already up to date (no update performed).
/// Used by the service wrapper to avoid unnecessary restarts.
pub const EXIT_CODE_ALREADY_UP_TO_DATE: i32 = 2;

/// Exit code returned when a macOS DMG-swap update has been staged and the
/// detached updater script is ready to take over. The wrapper MUST NOT
/// re-exec itself on this code (the current .app bundle is about to be
/// replaced out from under us); instead it should exit cleanly so the
/// updater can complete the swap and relaunch.
///
/// Value 44 chosen to follow the existing WRAPPER_EXIT_* convention in
/// service.rs (42 = update needed, 43 = already running) and to avoid
/// collision with the exit-3 signal that `freenet service status` uses
/// for "service not running" (which a caller might legitimately
/// disambiguate from "update staged" by exit code alone).
pub const EXIT_CODE_BUNDLE_UPDATE_STAGED: i32 = 44;

/// Classified outcome of a `freenet update` subprocess invocation, from
/// the wrapper's point of view. Pure function so the four existing
/// dispatch sites in `service.rs` stay in sync: a typo like forgetting
/// the `Some(...)` arm at one of them would route the wrapper into
/// re-exec'ing a bundle that's about to be replaced out from under it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[allow(dead_code)]
pub enum UpdateSubprocessOutcome {
    /// In-place binary replacement succeeded. Wrapper should re-exec via
    /// `spawn_new_wrapper` so the tray reflects the new version.
    BinaryReplaced,
    /// macOS DMG-swap: the detached updater will finish the update and
    /// relaunch the new bundle after this process exits. Wrapper MUST
    /// exit cleanly without re-exec.
    BundleUpdateStaged,
    /// Update check ran; already on the latest version.
    AlreadyUpToDate,
    /// Update attempted but failed (spawn error, network error, or non-
    /// zero exit other than `ALREADY_UP_TO_DATE` / `BUNDLE_UPDATE_STAGED`).
    Failed,
}

/// Classify the exit status of a `freenet update` subprocess invocation.
/// Pure over just the `io::Result<ExitStatus>` so it can be unit-tested
/// with synthetic inputs.
#[allow(dead_code)]
pub fn classify_update_subprocess(
    result: &std::io::Result<std::process::ExitStatus>,
) -> UpdateSubprocessOutcome {
    match result {
        Ok(s) if s.success() => UpdateSubprocessOutcome::BinaryReplaced,
        Ok(s) if s.code() == Some(EXIT_CODE_BUNDLE_UPDATE_STAGED) => {
            UpdateSubprocessOutcome::BundleUpdateStaged
        }
        Ok(s) if s.code() == Some(EXIT_CODE_ALREADY_UP_TO_DATE) => {
            UpdateSubprocessOutcome::AlreadyUpToDate
        }
        Ok(_) => UpdateSubprocessOutcome::Failed,
        Err(_) => UpdateSubprocessOutcome::Failed,
    }
}

/// Compute the macOS DMG asset filename for a given release tag. Strips
/// a leading `v` if present (releases are tagged `v0.2.49` but the DMG
/// filename embeds the bare version, per the release pipeline in
/// `.github/workflows/cross-compile.yml`). Pure so the derivation is
/// testable without a real `Release` fixture.
#[allow(dead_code)]
pub fn macos_dmg_asset_name(tag_name: &str) -> String {
    // `strip_prefix` removes at most one leading `v`, unlike
    // `trim_start_matches` which would eat multiple (e.g. a typo tag
    // like `vv0.2.49` would silently double-strip and produce the wrong
    // DMG name, then fail the asset lookup with a confusing "not
    // found" rather than surfacing the bad tag).
    let version = tag_name.strip_prefix('v').unwrap_or(tag_name);
    format!("Freenet-{}.dmg", version)
}

#[derive(Args, Debug, Clone)]
pub struct UpdateCommand {
    /// Only check if an update is available without installing
    #[arg(long)]
    pub check: bool,

    /// Force update even if already on latest version
    #[arg(long)]
    pub force: bool,

    /// Suppress interactive output (for automated updates)
    #[arg(long)]
    pub quiet: bool,
}

impl UpdateCommand {
    pub fn run(&self, current_version: &str) -> Result<()> {
        let rt = tokio::runtime::Runtime::new()?;
        rt.block_on(self.run_async(current_version))
    }

    async fn run_async(&self, current_version: &str) -> Result<()> {
        if !self.quiet {
            println!("Current version: {}", current_version);
            println!("Checking for updates...");
        }

        let latest = get_latest_release().await?;

        let latest_version = latest.tag_name.trim_start_matches('v');
        if !self.quiet {
            println!("Latest version: {}", latest_version);
        }

        // Use semver for proper version comparison
        let current_ver =
            Version::parse(current_version).context("Failed to parse current version as semver")?;
        let latest_ver =
            Version::parse(latest_version).context("Failed to parse latest version as semver")?;

        if !self.force && latest_ver <= current_ver {
            if !self.quiet {
                println!("You are already running the latest version.");
            }
            // Exit with a distinct code so the service wrapper knows no update
            // was performed and can skip the unnecessary restart.
            std::process::exit(EXIT_CODE_ALREADY_UP_TO_DATE);
        }

        if self.check {
            if latest_ver > current_ver && !self.quiet {
                println!(
                    "Update available: {} -> {}",
                    current_version, latest_version
                );
            }
            return Ok(());
        }

        if !self.quiet {
            println!("Downloading update...");
        }
        self.download_and_install(&latest).await
    }

    async fn download_and_install(&self, release: &Release) -> Result<()> {
        // macOS DMG-swap path: when running from inside a .app bundle,
        // in-place binary replacement would invalidate the bundle's code
        // signature and Gatekeeper would refuse to launch the result on
        // next login. Instead we download the signed+notarized DMG and
        // hand off to a detached updater script that swaps the whole
        // bundle once this process exits. See fn maybe_perform_bundle_update.
        #[cfg(target_os = "macos")]
        {
            // Detect the bundle context once up front so the error arm
            // can tell the difference between "not in a bundle, fall
            // through to binary-replace" and "in a bundle, DMG-swap
            // failed, abort to protect the signature". Falling through
            // on error would overwrite Contents/MacOS/freenet-bin and
            // invalidate the bundle's code signature, bricking the app
            // on next Gatekeeper check (per Codex P1 and code-first
            // review on this PR).
            let running_in_bundle = std::env::current_exe()
                .ok()
                .and_then(|exe| super::service::macos_app_bundle_path(&exe))
                .is_some();

            match self.maybe_perform_bundle_update(release).await {
                Ok(true) => {
                    tracing::info!("DMG-swap update staged for release {}", release.tag_name);
                    if !self.quiet {
                        println!("Bundle update staged. Freenet will relaunch shortly.");
                    }
                    std::process::exit(EXIT_CODE_BUNDLE_UPDATE_STAGED);
                }
                Ok(false) => {
                    // Not running inside a .app bundle (cargo run, raw
                    // binary install, dev build): fall through to the
                    // standard in-place binary replacement path below.
                }
                Err(e) if running_in_bundle => {
                    // Running in a bundle AND the DMG-swap failed. Do
                    // NOT fall through: binary-replace would corrupt the
                    // signature and break Gatekeeper on next boot. Log
                    // and return so the user stays on the working old
                    // version; the next auto-update cycle can retry.
                    tracing::warn!(
                        "DMG-swap bundle update failed for {}: {}. Skipping update to preserve code signature.",
                        release.tag_name,
                        e
                    );
                    if !self.quiet {
                        eprintln!(
                            "Bundle update failed: {e}. Skipping update to avoid corrupting the signed bundle. Next attempt will retry."
                        );
                    }
                    return Ok(());
                }
                Err(e) => {
                    // Not in a bundle: safe to fall through to binary-
                    // replace. Log the bundle-attempt failure for
                    // diagnosability; continue with the standard path.
                    if !self.quiet {
                        eprintln!(
                            "Bundle update check failed ({e}); continuing with in-place binary replacement."
                        );
                    }
                }
            }
        }

        let target = get_target_triple();
        let extension = get_archive_extension();
        let freenet_asset_name = format!("freenet-{}.{}", target, extension);
        let fdev_asset_name = format!("fdev-{}.{}", target, extension);

        let freenet_asset = release
            .assets
            .iter()
            .find(|a| a.name == freenet_asset_name)
            .ok_or_else(|| {
                anyhow::anyhow!(
                    "No binary available for your platform ({}). Available assets: {}",
                    target,
                    release
                        .assets
                        .iter()
                        .map(|a| a.name.as_str())
                        .collect::<Vec<_>>()
                        .join(", ")
                )
            })?;

        let fdev_asset = release.assets.iter().find(|a| a.name == fdev_asset_name);

        // Try to get checksums
        let checksums = if let Some(checksums_asset) =
            release.assets.iter().find(|a| a.name == "SHA256SUMS.txt")
        {
            if !self.quiet {
                println!("Downloading checksums...");
            }
            match download_checksums(&checksums_asset.browser_download_url).await {
                Ok(c) => Some(c),
                Err(e) => {
                    if !self.quiet {
                        eprintln!(
                            "Warning: Failed to download checksums: {}. Continuing without verification.",
                            e
                        );
                    }
                    None
                }
            }
        } else {
            if !self.quiet {
                eprintln!(
                    "Warning: SHA256SUMS.txt not found in release. Continuing without checksum verification."
                );
            }
            None
        };

        let temp_dir = tempfile::tempdir().context("Failed to create temp directory")?;

        // Download and install freenet
        let freenet_archive_path = temp_dir.path().join(&freenet_asset_name);
        download_file(
            &freenet_asset.browser_download_url,
            &freenet_archive_path,
            self.quiet,
        )
        .await?;

        if let Some(checksums) = &checksums {
            if let Some(expected_hash) = checksums.get(&freenet_asset_name) {
                if !self.quiet {
                    println!("Verifying freenet checksum...");
                }
                verify_checksum(&freenet_archive_path, expected_hash)?;
            } else if !self.quiet {
                eprintln!(
                    "Warning: Checksum not found for {}. Continuing without verification.",
                    freenet_asset_name
                );
            }
        }

        // Use a subdirectory per archive to avoid filename collisions
        let freenet_extract_dir = temp_dir.path().join("freenet");
        fs::create_dir_all(&freenet_extract_dir)?;
        let extracted_freenet =
            extract_binary(&freenet_archive_path, &freenet_extract_dir, "freenet")?;

        let current_exe = std::env::current_exe().context("Failed to get current executable")?;
        replace_binary(&extracted_freenet, &current_exe)?;

        if !self.quiet {
            println!(
                "Successfully updated freenet to version {}",
                release.tag_name.trim_start_matches('v')
            );
        }

        // Download and install fdev alongside freenet.
        // All fdev failures are non-fatal — a failed fdev update must never
        // prevent the service file update or service restart that follows.
        if let Some(fdev_asset) = fdev_asset {
            if !self.quiet {
                println!("Downloading fdev...");
            }
            self.try_update_fdev(
                fdev_asset,
                &fdev_asset_name,
                &checksums,
                temp_dir.path(),
                &current_exe,
            )
            .await;
        } else if !self.quiet {
            eprintln!("Warning: fdev not found in release assets. Skipping fdev update.");
        }

        // Check if service file needs updating (for users who installed before v0.1.75)
        if let Err(e) = ensure_service_file_updated(&current_exe, self.quiet) {
            if !self.quiet {
                eprintln!(
                    "Warning: Failed to update service file: {}. \
                     Run 'freenet service install' to update manually.",
                    e
                );
            }
        }

        // Automatically restart service if running
        // Note: When called from ExecStopPost or wrapper script, we should NOT restart
        // because systemd/launchd will restart the service automatically.
        // The quiet flag indicates automated context where we skip manual restart.
        if !self.quiet {
            #[cfg(target_os = "linux")]
            {
                if is_systemd_service_active() {
                    println!("Restarting Freenet service...");
                    let status = Command::new("systemctl")
                        .args(["--user", "restart", "freenet"])
                        .status();
                    match status {
                        Ok(s) if s.success() => println!("Service restarted successfully."),
                        Ok(_) => eprintln!(
                            "Warning: Failed to restart service. Run 'freenet service restart' manually."
                        ),
                        Err(e) => eprintln!(
                            "Warning: Failed to restart service: {}. Run 'freenet service restart' manually.",
                            e
                        ),
                    }
                }
            }

            #[cfg(target_os = "macos")]
            {
                if is_launchd_service_active() {
                    println!("Restarting Freenet service...");
                    // launchctl doesn't have a restart command, so stop + start
                    if let Err(e) = Command::new("launchctl")
                        .args(["stop", "org.freenet.node"])
                        .status()
                    {
                        eprintln!("Warning: failed to stop service: {e}");
                    }
                    let status = Command::new("launchctl")
                        .args(["start", "org.freenet.node"])
                        .status();
                    match status {
                        Ok(s) if s.success() => println!("Service restarted successfully."),
                        Ok(_) => eprintln!(
                            "Warning: Failed to restart service. Run 'freenet service restart' manually."
                        ),
                        Err(e) => eprintln!(
                            "Warning: Failed to restart service: {}. Run 'freenet service restart' manually.",
                            e
                        ),
                    }
                }
            }

            #[cfg(target_os = "windows")]
            {
                if is_windows_wrapper_running() {
                    println!("Restarting Freenet service...");
                    // Kill old wrapper + child processes (excluding ourselves),
                    // then start a new wrapper with the updated binary.
                    let our_pid = std::process::id().to_string();
                    Command::new("taskkill")
                        .args([
                            "/f",
                            "/im",
                            "freenet.exe",
                            "/fi",
                            &format!("PID ne {}", our_pid),
                        ])
                        .stdout(std::process::Stdio::null())
                        .stderr(std::process::Stdio::null())
                        .status()
                        .ok();
                    // Brief pause to ensure the old process is fully stopped
                    std::thread::sleep(std::time::Duration::from_secs(2));
                    let status = Command::new(&current_exe)
                        .args(["service", "start"])
                        .status();
                    match status {
                        Ok(s) if s.success() => println!("Service restarted successfully."),
                        Ok(_) => eprintln!(
                            "Warning: Failed to restart service. Run 'freenet service start' manually."
                        ),
                        Err(e) => eprintln!(
                            "Warning: Failed to restart service: {}. Run 'freenet service start' manually.",
                            e
                        ),
                    }
                }
            }
        }

        Ok(())
    }

    /// Try to update fdev, printing warnings on failure. Never returns an error —
    /// fdev update failures must not prevent the caller from continuing with
    /// service file updates and service restarts.
    async fn try_update_fdev(
        &self,
        asset: &Asset,
        asset_name: &str,
        checksums: &Option<Checksums>,
        temp_dir: &Path,
        freenet_exe: &Path,
    ) {
        let archive_path = temp_dir.join(asset_name);
        if let Err(e) = download_file(&asset.browser_download_url, &archive_path, self.quiet).await
        {
            if !self.quiet {
                eprintln!(
                    "Warning: Failed to download fdev: {}. Skipping fdev update.",
                    e
                );
            }
            return;
        }

        if let Some(checksums) = &checksums {
            if let Some(expected_hash) = checksums.get(asset_name) {
                if !self.quiet {
                    println!("Verifying fdev checksum...");
                }
                if let Err(e) = verify_checksum(&archive_path, expected_hash) {
                    if !self.quiet {
                        eprintln!(
                            "Warning: fdev checksum verification failed: {}. Skipping fdev update.",
                            e
                        );
                    }
                    return;
                }
            }
        }

        let extract_dir = temp_dir.join("fdev");
        if let Err(e) = fs::create_dir_all(&extract_dir) {
            if !self.quiet {
                eprintln!(
                    "Warning: Failed to create fdev extract directory: {}. Skipping fdev update.",
                    e
                );
            }
            return;
        }

        let extracted_fdev = match extract_binary(&archive_path, &extract_dir, "fdev") {
            Ok(path) => path,
            Err(e) => {
                if !self.quiet {
                    eprintln!(
                        "Warning: Failed to extract fdev: {}. Skipping fdev update.",
                        e
                    );
                }
                return;
            }
        };

        // Install fdev next to the freenet binary
        let Some(install_dir) = freenet_exe.parent() else {
            if !self.quiet {
                eprintln!("Warning: Cannot determine install directory. Skipping fdev update.");
            }
            return;
        };

        #[cfg(target_os = "windows")]
        let fdev_dest = install_dir.join("fdev.exe");
        #[cfg(not(target_os = "windows"))]
        let fdev_dest = install_dir.join("fdev");

        if let Err(e) = replace_binary(&extracted_fdev, &fdev_dest) {
            if !self.quiet {
                eprintln!(
                    "Warning: Failed to update fdev: {}. You can update it manually with: curl -fsSL https://freenet.org/install.sh | sh",
                    e
                );
            }
        } else if !self.quiet {
            println!("Successfully updated fdev.");
        }
    }

    /// macOS DMG-swap update. Invoked from `download_and_install` when we
    /// detect we're running inside a .app bundle. Downloads the new
    /// `Freenet-<version>.dmg`, verifies its checksum against
    /// SHA256SUMS.txt, mounts it, stages the new bundle in
    /// `~/Library/Caches/Freenet/staging/Freenet.app`, and spawns the
    /// detached `macos-bundle-updater.sh` script which waits for the
    /// running process to exit and then swaps the bundle.
    ///
    /// Returns:
    ///   Ok(true)  update was staged; caller should exit with
    ///             EXIT_CODE_BUNDLE_UPDATE_STAGED
    ///   Ok(false) not running in a bundle; caller should use the
    ///             standard binary-replacement flow instead
    ///   Err       failed; caller may fall back to binary replacement
    #[cfg(target_os = "macos")]
    async fn maybe_perform_bundle_update(&self, release: &Release) -> Result<bool> {
        let current_exe = std::env::current_exe()
            .context("Failed to resolve current executable for bundle-update check")?;
        let Some(bundle_path) = super::service::macos_app_bundle_path(&current_exe) else {
            return Ok(false);
        };

        let dmg_asset_name = macos_dmg_asset_name(&release.tag_name);
        let dmg_asset = release
            .assets
            .iter()
            .find(|a| a.name == dmg_asset_name)
            .ok_or_else(|| anyhow::anyhow!("No DMG asset named {} in release", dmg_asset_name))?;

        // Acquire a flock on a lockfile in the cache dir so two concurrent
        // updates (e.g. tray "Check for Updates" racing with auto-update)
        // can't stomp each other's staging directories and leave the user
        // with no /Applications/Freenet.app (skeptical review H1).
        let _update_lock = acquire_update_lock()?;

        let checksums = match release.assets.iter().find(|a| a.name == "SHA256SUMS.txt") {
            Some(a) => download_checksums(&a.browser_download_url).await.ok(),
            None => None,
        };

        let scratch = tempfile::tempdir().context("Failed to create temp directory")?;
        let dmg_path = scratch.path().join(&dmg_asset_name);
        download_file(&dmg_asset.browser_download_url, &dmg_path, self.quiet).await?;

        // Require checksum verification for bundle updates. Unverified
        // DMG install is a supply-chain risk that macOS Gatekeeper alone
        // should not have to mitigate. If SHA256SUMS.txt doesn't list
        // the DMG, abort rather than silently proceed (code-first,
        // skeptical M7).
        match checksums.as_ref().and_then(|c| c.get(&dmg_asset_name)) {
            Some(expected) => verify_checksum(&dmg_path, expected)?,
            None => anyhow::bail!(
                "No SHA256 checksum listed for {}; refusing unverified DMG install. \
                 Check that the release uploaded SHA256SUMS.txt covering the DMG asset.",
                dmg_asset_name
            ),
        }

        let mount_point = scratch.path().join("mount");
        std::fs::create_dir_all(&mount_point)?;
        hdiutil_attach(&dmg_path, &mount_point)?;
        // Scope guard: detach in every control-flow path below, including
        // `?` early returns from `ditto` spawn failure. Without this, the
        // volume stays mounted after the function returns and the tempdir
        // drop tries to remove_dir_all under a live mount, which at best
        // fails and at worst (if hdiutil is confused) could touch the
        // mounted contents.
        let detach_guard = MountDetachOnDrop {
            mount_point: mount_point.clone(),
        };

        let mounted_app = mount_point.join("Freenet.app");
        if !mounted_app.exists() {
            drop(detach_guard);
            anyhow::bail!(
                "DMG mounted at {} does not contain Freenet.app",
                mount_point.display()
            );
        }

        // Stage as a SIBLING of the target bundle, not in
        // ~/Library/Caches. The final `rename(2)` the updater script
        // performs must stay on the same APFS volume or the fallback
        // cp+rm is not crash-safe (skeptical review H2). Using a
        // hidden sibling also keeps the staging dir cleaned up even
        // if the updater crashes mid-flight — any `/Applications/.Freenet.app.staging.*`
        // leftover is obviously unrelated to the real install.
        let staged_app = bundle_staging_path(&bundle_path)?;
        if staged_app.exists() {
            std::fs::remove_dir_all(&staged_app)
                .context("Failed to clean previous staging directory")?;
        }
        if let Some(parent) = staged_app.parent() {
            std::fs::create_dir_all(parent)?;
        }

        // `ditto` preserves HFS+/APFS metadata and extended attributes
        // (including the code-signature bits and stapled notarization).
        // A plain `cp -R` would strip quarantine/notarization attrs and
        // Gatekeeper would then reject the bundle on next launch.
        let ditto = std::process::Command::new("/usr/bin/ditto")
            .arg(&mounted_app)
            .arg(&staged_app)
            .output()
            .context("Failed to spawn /usr/bin/ditto")?;
        drop(detach_guard);
        if !ditto.status.success() {
            anyhow::bail!(
                "ditto copy failed ({}): {}",
                ditto.status,
                String::from_utf8_lossy(&ditto.stderr).trim()
            );
        }

        let script_path = write_updater_script()?;
        spawn_detached_updater(&script_path, &bundle_path, &staged_app)?;

        Ok(true)
    }
}

#[derive(serde::Deserialize, Debug)]
struct Release {
    tag_name: String,
    assets: Vec<Asset>,
}

#[derive(serde::Deserialize, Debug)]
struct Asset {
    name: String,
    browser_download_url: String,
}

/// SHA256 checksums parsed from SHA256SUMS.txt
struct Checksums {
    entries: std::collections::HashMap<String, String>,
}

impl Checksums {
    fn parse(content: &str) -> Self {
        let mut entries = std::collections::HashMap::new();
        for line in content.lines() {
            // Format: "hash  filename" or "hash filename"
            let parts: Vec<&str> = line.split_whitespace().collect();
            if parts.len() >= 2 {
                let hash = parts[0].to_string();
                let filename = parts[1].to_string();
                entries.insert(filename, hash);
            }
        }
        Self { entries }
    }

    fn get(&self, filename: &str) -> Option<&str> {
        self.entries.get(filename).map(|s| s.as_str())
    }
}

async fn get_latest_release() -> Result<Release> {
    let client = reqwest::Client::builder()
        .user_agent("freenet-updater")
        .build()?;

    let response = client
        .get(GITHUB_API_URL)
        .send()
        .await
        .context("Failed to fetch release info")?;

    if !response.status().is_success() {
        anyhow::bail!(
            "GitHub API returned error: {} {}",
            response.status(),
            response.text().await.unwrap_or_default()
        );
    }

    response
        .json::<Release>()
        .await
        .context("Failed to parse release info")
}

async fn download_checksums(url: &str) -> Result<Checksums> {
    let client = reqwest::Client::builder()
        .user_agent("freenet-updater")
        .build()?;

    let response = client
        .get(url)
        .send()
        .await
        .context("Failed to download checksums")?;

    if !response.status().is_success() {
        anyhow::bail!("Failed to download checksums: {}", response.status());
    }

    let content = response.text().await.context("Failed to read checksums")?;
    Ok(Checksums::parse(&content))
}

fn verify_checksum(file_path: &Path, expected_hash: &str) -> Result<()> {
    use sha2::{Digest, Sha256};
    use std::io::Read;

    let mut file = File::open(file_path).context("Failed to open file for checksum")?;
    let mut hasher = Sha256::new();
    let mut buf = [0u8; 8192];
    loop {
        let n = file
            .read(&mut buf)
            .context("Failed to read file for checksum")?;
        if n == 0 {
            break;
        }
        hasher.update(&buf[..n]);
    }
    let result = hasher.finalize();
    let actual_hash = result.iter().fold(String::with_capacity(64), |mut s, b| {
        use std::fmt::Write;
        write!(s, "{:02x}", b).expect("writing to String is infallible");
        s
    });

    if actual_hash != expected_hash {
        anyhow::bail!(
            "Checksum verification failed!\nExpected: {}\nGot:      {}\n\
             The download may be corrupted or tampered with.",
            expected_hash,
            actual_hash
        );
    }

    Ok(())
}

fn get_target_triple() -> &'static str {
    // Note: We always request musl binaries for Linux because that's what
    // the release workflow builds. musl binaries work on both musl (Alpine)
    // and glibc systems, so this is the simplest approach.
    #[cfg(all(target_arch = "x86_64", target_os = "linux"))]
    {
        "x86_64-unknown-linux-musl"
    }
    #[cfg(all(target_arch = "aarch64", target_os = "linux"))]
    {
        "aarch64-unknown-linux-musl"
    }
    #[cfg(all(target_arch = "x86_64", target_os = "macos"))]
    {
        "x86_64-apple-darwin"
    }
    #[cfg(all(target_arch = "aarch64", target_os = "macos"))]
    {
        "aarch64-apple-darwin"
    }
    #[cfg(all(target_arch = "x86_64", target_os = "windows"))]
    {
        "x86_64-pc-windows-msvc"
    }
    #[cfg(not(any(
        all(target_arch = "x86_64", target_os = "linux"),
        all(target_arch = "aarch64", target_os = "linux"),
        all(target_arch = "x86_64", target_os = "macos"),
        all(target_arch = "aarch64", target_os = "macos"),
        all(target_arch = "x86_64", target_os = "windows"),
    )))]
    {
        "unknown"
    }
}

/// Get the archive extension for the current platform
fn get_archive_extension() -> &'static str {
    #[cfg(target_os = "windows")]
    {
        "zip"
    }
    #[cfg(not(target_os = "windows"))]
    {
        "tar.gz"
    }
}

async fn download_file(url: &str, dest: &Path, quiet: bool) -> Result<()> {
    let client = reqwest::Client::builder()
        .user_agent("freenet-updater")
        .build()?;

    let response = client
        .get(url)
        .send()
        .await
        .context("Failed to download file")?;

    if !response.status().is_success() {
        anyhow::bail!("Download failed: {}", response.status());
    }

    let total_size = response.content_length().unwrap_or(0);
    let mut downloaded: u64 = 0;
    let mut file = File::create(dest).context("Failed to create temp file")?;

    let mut stream = response.bytes_stream();
    use futures::StreamExt;

    while let Some(chunk) = stream.next().await {
        let chunk = chunk.context("Error while downloading")?;
        file.write_all(&chunk)?;
        downloaded += chunk.len() as u64;

        if !quiet && total_size > 0 {
            let progress = (downloaded as f64 / total_size as f64 * 100.0) as u32;
            // Use ANSI escape to clear to end of line for clean output
            print!("\rDownloading... {}%\x1b[K", progress);
            io::stdout().flush()?;
        }
    }

    if !quiet {
        println!("\rDownload complete.\x1b[K");
    }
    Ok(())
}

fn extract_binary(archive_path: &Path, dest_dir: &Path, name: &str) -> Result<PathBuf> {
    // Extract with path traversal protection
    let dest_dir_canonical = dest_dir
        .canonicalize()
        .context("Failed to canonicalize dest dir")?;

    // Determine archive type from extension
    let is_zip = archive_path
        .extension()
        .map(|ext| ext == "zip")
        .unwrap_or(false);

    if is_zip {
        extract_zip(archive_path, dest_dir, &dest_dir_canonical)?;
    } else {
        extract_tar_gz(archive_path, dest_dir, &dest_dir_canonical)?;
    }

    // Binary name differs on Windows
    #[cfg(target_os = "windows")]
    let binary_name = format!("{name}.exe");
    #[cfg(not(target_os = "windows"))]
    let binary_name = name.to_string();

    let binary_path = dest_dir.join(&binary_name);
    if !binary_path.exists() {
        anyhow::bail!("{name} binary not found in archive");
    }

    // Verify the binary is executable and works
    verify_binary(&binary_path)?;

    Ok(binary_path)
}

fn extract_tar_gz(archive_path: &Path, dest_dir: &Path, dest_dir_canonical: &Path) -> Result<()> {
    let file = File::open(archive_path).context("Failed to open archive")?;
    let decoder = flate2::read::GzDecoder::new(file);
    let mut archive = tar::Archive::new(decoder);

    for entry in archive
        .entries()
        .context("Failed to read archive entries")?
    {
        let mut entry = entry.context("Failed to read archive entry")?;
        let path = entry.path().context("Failed to get entry path")?;

        // Security: Prevent path traversal attacks
        validate_extract_path(dest_dir, dest_dir_canonical, &path)?;

        entry
            .unpack_in(dest_dir)
            .context("Failed to extract entry")?;
    }

    Ok(())
}

#[cfg(target_os = "windows")]
fn extract_zip(archive_path: &Path, dest_dir: &Path, dest_dir_canonical: &Path) -> Result<()> {
    use std::io::Read;

    let file = File::open(archive_path).context("Failed to open archive")?;
    let mut archive = zip::ZipArchive::new(file).context("Failed to read zip archive")?;

    for i in 0..archive.len() {
        let mut file = archive.by_index(i).context("Failed to read zip entry")?;
        let outpath = match file.enclosed_name() {
            Some(path) => dest_dir.join(path),
            None => continue,
        };

        // Security: Prevent path traversal attacks
        let outpath_str = outpath.to_string_lossy();
        validate_extract_path(dest_dir, dest_dir_canonical, Path::new(&*outpath_str))?;

        if file.name().ends_with('/') {
            fs::create_dir_all(&outpath)?;
        } else {
            if let Some(p) = outpath.parent() {
                if !p.exists() {
                    fs::create_dir_all(p)?;
                }
            }
            let mut outfile = File::create(&outpath)?;
            std::io::copy(&mut file, &mut outfile)?;
        }
    }

    Ok(())
}

#[cfg(not(target_os = "windows"))]
fn extract_zip(_archive_path: &Path, _dest_dir: &Path, _dest_dir_canonical: &Path) -> Result<()> {
    anyhow::bail!("Zip extraction is only supported on Windows")
}

fn validate_extract_path(dest_dir: &Path, dest_dir_canonical: &Path, path: &Path) -> Result<()> {
    let entry_dest = dest_dir.join(path);
    let entry_canonical = entry_dest
        .canonicalize()
        .unwrap_or_else(|_| entry_dest.clone());

    // For new files, check parent directory is within dest_dir
    let check_path = if entry_canonical.exists() {
        entry_canonical
    } else {
        entry_dest
            .parent()
            .and_then(|p| p.canonicalize().ok())
            .unwrap_or_else(|| dest_dir_canonical.to_path_buf())
    };

    if !check_path.starts_with(dest_dir_canonical) {
        anyhow::bail!(
            "Security error: archive contains path traversal attempt: {}",
            path.display()
        );
    }

    Ok(())
}

fn verify_binary(binary_path: &Path) -> Result<()> {
    // Set executable permissions first
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let mut perms = fs::metadata(binary_path)?.permissions();
        perms.set_mode(0o755);
        fs::set_permissions(binary_path, perms)?;
    }

    // Run --version to verify the binary works
    let output = Command::new(binary_path)
        .arg("--version")
        .output()
        .context("Failed to execute downloaded binary for verification")?;

    if !output.status.success() {
        anyhow::bail!(
            "Downloaded binary failed verification (--version check failed). \
             This could indicate a corrupted download or wrong architecture."
        );
    }

    // Verify output contains expected version format
    let stdout = String::from_utf8_lossy(&output.stdout);
    if !stdout.contains("Freenet") && !stdout.contains("freenet") {
        anyhow::bail!(
            "Downloaded binary doesn't appear to be Freenet. \
             Got: {}",
            stdout.trim()
        );
    }

    Ok(())
}

/// Check if the service file needs updating (missing auto-update hook) and update if so.
/// This ensures users who installed before v0.1.75 get the ExecStopPost hook.
#[cfg(target_os = "linux")]
fn ensure_service_file_updated(binary_path: &Path, quiet: bool) -> Result<()> {
    // Try user service first
    let home_dir = dirs::home_dir();
    let user_service_path = home_dir
        .as_ref()
        .map(|h| h.join(".config/systemd/user/freenet.service"));

    if let Some(ref service_path) = user_service_path {
        if service_path.exists() {
            return update_service_file(binary_path, service_path, false, quiet);
        }
    }

    // Try system service
    let system_service_path = Path::new("/etc/systemd/system/freenet.service");
    if system_service_path.exists() {
        return update_service_file(binary_path, system_service_path, true, quiet);
    }

    Ok(())
}

/// Update a specific service file if it's missing the auto-update hook.
#[cfg(target_os = "linux")]
fn update_service_file(
    binary_path: &Path,
    service_path: &Path,
    system_mode: bool,
    quiet: bool,
) -> Result<()> {
    let content = fs::read_to_string(service_path).context("Failed to read service file")?;

    // Check if the service file has all required directives.
    // RestartPreventExitStatus=43 prevents restart loops when another instance
    // is already running (added in 0.2.5).
    if content.contains("ExecStopPost=")
        && content.contains("SuccessExitStatus=42")
        && content.contains("RestartPreventExitStatus=43")
    {
        return Ok(()); // Already up to date
    }

    if !quiet {
        println!("Updating service file to add auto-update support...");
    }

    // Create backup of existing service file in case user had customizations
    let backup_path = service_path.with_extension("service.bak");
    if let Err(e) = fs::copy(service_path, &backup_path) {
        if !quiet {
            eprintln!(
                "Warning: Failed to backup service file: {}. Continuing anyway.",
                e
            );
        }
    } else if !quiet {
        println!("Backed up existing service file to {:?}", backup_path);
    }

    // Generate new service file content
    let new_content = if system_mode {
        // Extract User= and Environment=HOME= from existing file
        let username = content
            .lines()
            .find_map(|l| l.strip_prefix("User="))
            .unwrap_or("freenet");
        let home_dir = content
            .lines()
            .find_map(|l| l.strip_prefix("Environment=HOME="))
            .map(PathBuf::from)
            .unwrap_or_else(|| PathBuf::from(format!("/home/{username}")));
        let log_dir = home_dir.join(".local/state/freenet");
        generate_system_service_file(binary_path, &log_dir, username, &home_dir)
    } else {
        let home_dir = dirs::home_dir().context("Failed to get home directory")?;
        let log_dir = home_dir.join(".local/state/freenet");
        generate_user_service_file(binary_path, &log_dir)
    };

    // Write the updated service file
    fs::write(service_path, new_content).context("Failed to write updated service file")?;

    // Reload systemd daemon
    let mut cmd = Command::new("systemctl");
    if !system_mode {
        cmd.arg("--user");
    }
    cmd.arg("daemon-reload");
    let status = cmd.status().context("Failed to reload systemd")?;

    if !status.success() && !quiet {
        if system_mode {
            eprintln!(
                "Warning: Failed to reload systemd daemon. Run 'systemctl daemon-reload' manually."
            );
        } else {
            eprintln!(
                "Warning: Failed to reload systemd daemon. Run 'systemctl --user daemon-reload' manually."
            );
        }
    } else if !quiet {
        println!("Service file updated with auto-update hook.");
    }

    Ok(())
}

/// Check if the wrapper script needs updating (missing or outdated) and update if so.
#[cfg(target_os = "macos")]
fn ensure_service_file_updated(binary_path: &Path, quiet: bool) -> Result<()> {
    let home_dir = match dirs::home_dir() {
        Some(dir) => dir,
        None => return Ok(()), // Can't update wrapper without home dir
    };

    let plist_path = home_dir.join("Library/LaunchAgents/org.freenet.node.plist");

    // Only update if plist exists (user has installed as service)
    if !plist_path.exists() {
        return Ok(());
    }

    let wrapper_path = home_dir.join(".local/bin/freenet-service-wrapper.sh");

    // Check if wrapper script exists and has the update logic
    let needs_update = if wrapper_path.exists() {
        let content = fs::read_to_string(&wrapper_path).context("Failed to read wrapper script")?;
        // Check for key auto-update markers
        !content.contains("EXIT_CODE=$?") || !content.contains("freenet update")
    } else {
        true
    };

    if !needs_update {
        return Ok(());
    }

    if !quiet {
        println!("Updating service wrapper to add auto-update support...");
    }

    // Ensure wrapper directory exists
    let wrapper_dir = wrapper_path
        .parent()
        .context("Wrapper path has no parent directory")?;
    fs::create_dir_all(wrapper_dir).context("Failed to create wrapper directory")?;

    // Create backup of existing wrapper script if it exists
    if wrapper_path.exists() {
        let backup_path = wrapper_path.with_extension("sh.bak");
        if let Err(e) = fs::copy(&wrapper_path, &backup_path) {
            if !quiet {
                eprintln!(
                    "Warning: Failed to backup wrapper script: {}. Continuing anyway.",
                    e
                );
            }
        } else if !quiet {
            println!("Backed up existing wrapper script to {:?}", backup_path);
        }
    }

    // Generate and write new wrapper script
    let wrapper_content = generate_wrapper_script(binary_path);
    fs::write(&wrapper_path, &wrapper_content).context("Failed to write wrapper script")?;

    // Make executable
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let mut perms = fs::metadata(&wrapper_path)?.permissions();
        perms.set_mode(0o755);
        fs::set_permissions(&wrapper_path, perms)?;
    }

    if !quiet {
        println!("Service wrapper updated with auto-update hook.");
    }

    Ok(())
}

/// Migrate old-style Windows Task Scheduler entries to registry Run key,
/// and ensure the run-wrapper command is used for auto-update and tray support.
#[cfg(target_os = "windows")]
fn ensure_service_file_updated(binary_path: &Path, quiet: bool) -> Result<()> {
    let exe_path_str = binary_path
        .to_str()
        .context("Executable path contains invalid UTF-8")?;
    let run_command = format!("\"{}\" service run-wrapper", exe_path_str);

    let hkcu = winreg::RegKey::predef(winreg::enums::HKEY_CURRENT_USER);

    // Check if registry Run key already has the correct command
    let current_value = hkcu
        .open_subkey(r"Software\Microsoft\Windows\CurrentVersion\Run")
        .ok()
        .and_then(|k| k.get_value::<String, _>("Freenet").ok());

    let needs_update = match &current_value {
        None => {
            // No registry entry — check if there's a legacy scheduled task to migrate
            let has_legacy_task = std::process::Command::new("schtasks")
                .args(["/query", "/tn", "Freenet"])
                .stdout(std::process::Stdio::null())
                .stderr(std::process::Stdio::null())
                .status()
                .map(|s| s.success())
                .unwrap_or(false);
            has_legacy_task
        }
        Some(val) => !val.contains("run-wrapper"),
    };

    if !needs_update {
        return Ok(());
    }

    if !quiet {
        println!("Migrating Freenet autostart to registry Run key...");
    }

    // Write registry Run key
    let (run_key, _) = hkcu
        .create_subkey(r"Software\Microsoft\Windows\CurrentVersion\Run")
        .context("Failed to open registry Run key")?;
    run_key
        .set_value("Freenet", &run_command)
        .context("Failed to write Freenet registry entry")?;

    // Clean up legacy scheduled task (best-effort, may need admin)
    drop(
        std::process::Command::new("schtasks")
            .args(["/delete", "/tn", "Freenet", "/f"])
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null())
            .status(),
    );

    if !quiet {
        println!("Freenet autostart migrated successfully.");
    }

    Ok(())
}

/// No-op on other platforms
#[cfg(not(any(target_os = "linux", target_os = "macos", target_os = "windows")))]
fn ensure_service_file_updated(_binary_path: &Path, _quiet: bool) -> Result<()> {
    Ok(())
}

fn replace_binary(new_binary: &Path, dest: &Path) -> Result<()> {
    let backup_path = dest.with_extension("old");
    let parent_dir = dest
        .parent()
        .context("Destination path has no parent directory")?;

    // Remove old backup if it exists
    if backup_path.exists() {
        fs::remove_file(&backup_path).context("Failed to remove old backup")?;
    }

    // First, copy new binary to a temp location in the same directory
    // This ensures the rename will be atomic (same filesystem)
    let file_stem = dest.file_name().unwrap_or_default().to_string_lossy();
    let temp_new = parent_dir.join(format!(".{file_stem}.new.tmp"));
    fs::copy(new_binary, &temp_new).context("Failed to copy new binary to target directory")?;

    // Set executable permissions on temp file
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let mut perms = fs::metadata(&temp_new)?.permissions();
        perms.set_mode(0o755);
        fs::set_permissions(&temp_new, perms)?;
    }

    if dest.exists() {
        // Rename current to backup (atomic on Unix)
        fs::rename(dest, &backup_path)
            .context("Failed to backup current binary. You may need to run with sudo.")?;
    }

    // Rename temp to dest (atomic on Unix)
    if let Err(e) = fs::rename(&temp_new, dest) {
        // Try to restore backup
        if backup_path.exists() {
            if let Err(restore_err) = fs::rename(&backup_path, dest) {
                eprintln!(
                    "CRITICAL: Failed to restore backup after update failure. \
                     Original binary may be at: {}",
                    backup_path.display()
                );
                eprintln!("Restore error: {}", restore_err);
            }
        }
        // Clean up temp file (best-effort, failure is non-fatal during error recovery)
        drop(fs::remove_file(&temp_new));
        return Err(e).context("Failed to install new binary");
    }

    // Remove backup on success
    if backup_path.exists() {
        if let Err(e) = fs::remove_file(&backup_path) {
            // Non-fatal: warn but don't fail
            eprintln!(
                "Warning: Failed to remove backup file {}: {}",
                backup_path.display(),
                e
            );
        }
    }

    Ok(())
}

#[cfg(target_os = "linux")]
fn is_systemd_service_active() -> bool {
    std::process::Command::new("systemctl")
        .args(["--user", "is-active", "--quiet", "freenet"])
        .status()
        .map(|s| s.success())
        .unwrap_or(false)
}

#[cfg(target_os = "macos")]
fn is_launchd_service_active() -> bool {
    std::process::Command::new("launchctl")
        .args(["list", "org.freenet.node"])
        .status()
        .map(|s| s.success())
        .unwrap_or(false)
}

#[cfg(target_os = "windows")]
fn is_windows_wrapper_running() -> bool {
    std::process::Command::new("tasklist")
        .args(["/fi", "imagename eq freenet.exe", "/fo", "csv", "/nh"])
        .output()
        .map(|o| {
            let stdout = String::from_utf8_lossy(&o.stdout);
            // Check that there's another freenet.exe besides ourselves
            stdout.matches("freenet.exe").count() > 1
        })
        .unwrap_or(false)
}

// ── macOS DMG-swap update support ──

/// Cache directory for the updater script runtime + log. Staged bundles
/// live beside the target (see `bundle_staging_path`) so `rename(2)`
/// stays same-volume. Uses `dirs::cache_dir()` which on macOS resolves
/// to `~/Library/Caches`.
#[cfg(target_os = "macos")]
fn bundle_updater_cache_root() -> Result<PathBuf> {
    dirs::cache_dir()
        .map(|d| d.join("Freenet"))
        .context("Could not resolve user cache directory")
}

/// Path where we stage the new .app during an update. Must be on the
/// same APFS volume as the target bundle so the final `rename(2)` the
/// updater script performs is atomic (skeptical H2). We use a hidden
/// sibling of the target named with our PID to avoid clashing with
/// concurrent updates and to keep filesystem cleanup obvious.
#[cfg(target_os = "macos")]
fn bundle_staging_path(target_bundle: &Path) -> Result<PathBuf> {
    let parent = target_bundle
        .parent()
        .context("Target bundle has no parent directory")?;
    let pid = std::process::id();
    Ok(parent.join(format!(".Freenet.app.staging.{pid}")))
}

#[cfg(target_os = "macos")]
fn updater_runtime_dir() -> Result<PathBuf> {
    Ok(bundle_updater_cache_root()?.join("updater"))
}

/// Lockfile-backed exclusion for concurrent update attempts. Returns a
/// guard that holds the flock until dropped. Concurrent `freenet update`
/// invocations (e.g. tray-triggered racing with auto-update-on-exit-42)
/// must be serialized or they can stomp each other's staging and leave
/// the user with no installed bundle (skeptical H1).
#[cfg(target_os = "macos")]
fn acquire_update_lock() -> Result<UpdateLock> {
    use std::os::unix::io::AsRawFd;
    let dir = updater_runtime_dir()?;
    std::fs::create_dir_all(&dir)?;
    let lock_path = dir.join("update.lock");
    let file = std::fs::OpenOptions::new()
        .create(true)
        .truncate(false)
        .write(true)
        .open(&lock_path)
        .context("Failed to open update lockfile")?;
    // flock with LOCK_EX | LOCK_NB: fail fast instead of queueing. A
    // parallel updater should bail rather than serialize behind us.
    let rc = unsafe { libc::flock(file.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) };
    if rc != 0 {
        anyhow::bail!(
            "Another Freenet update is already in progress (lockfile held: {})",
            lock_path.display()
        );
    }
    Ok(UpdateLock { _file: file })
}

#[cfg(target_os = "macos")]
struct UpdateLock {
    // Held only to keep the flock alive; dropped releases the lock.
    _file: std::fs::File,
}

/// Scope guard that detaches an hdiutil-mounted volume when it goes out
/// of scope. Ensures the mount is cleaned up on every exit path from
/// `maybe_perform_bundle_update`, including `?` early-returns.
#[cfg(target_os = "macos")]
struct MountDetachOnDrop {
    mount_point: PathBuf,
}

#[cfg(target_os = "macos")]
impl Drop for MountDetachOnDrop {
    fn drop(&mut self) {
        let _ = hdiutil_detach(&self.mount_point);
    }
}

#[cfg(target_os = "macos")]
fn hdiutil_attach(dmg: &Path, mount_point: &Path) -> Result<()> {
    let output = std::process::Command::new("hdiutil")
        .args(["attach", "-nobrowse", "-readonly", "-mountpoint"])
        .arg(mount_point)
        .arg(dmg)
        .output()
        .context("Failed to spawn hdiutil")?;
    if !output.status.success() {
        anyhow::bail!(
            "hdiutil attach failed ({}): {}",
            output.status,
            String::from_utf8_lossy(&output.stderr).trim()
        );
    }
    Ok(())
}

#[cfg(target_os = "macos")]
fn hdiutil_detach(mount_point: &Path) -> Result<()> {
    // `-force` so a stuck unmount from a prior crashed run doesn't pin
    // us indefinitely. Attach is read-only, so there's no data at risk.
    let output = std::process::Command::new("hdiutil")
        .args(["detach", "-force"])
        .arg(mount_point)
        .output()
        .context("Failed to spawn hdiutil")?;
    if !output.status.success() {
        anyhow::bail!(
            "hdiutil detach failed ({}): {}",
            output.status,
            String::from_utf8_lossy(&output.stderr).trim()
        );
    }
    Ok(())
}

/// Materialize the macOS bundle updater script onto disk. We ship it as
/// an embedded `include_str!` so the Freenet binary is self-contained
/// even when installed from crates.io without the `scripts/` directory.
#[cfg(target_os = "macos")]
fn write_updater_script() -> Result<PathBuf> {
    const SCRIPT: &str = include_str!("../../../../../scripts/macos-bundle-updater.sh");
    let dir = updater_runtime_dir()?;
    std::fs::create_dir_all(&dir)?;
    let path = dir.join("macos-bundle-updater.sh");
    std::fs::write(&path, SCRIPT)?;
    use std::os::unix::fs::PermissionsExt;
    let mut perms = std::fs::metadata(&path)?.permissions();
    perms.set_mode(0o755);
    std::fs::set_permissions(&path, perms)?;
    Ok(path)
}

/// Spawn the bundle updater in a detached process group so it survives
/// when the current process exits (which it is about to, so the bundle
/// can be swapped).
#[cfg(target_os = "macos")]
fn spawn_detached_updater(script: &Path, current_app: &Path, staged_app: &Path) -> Result<()> {
    use std::os::unix::process::CommandExt;
    use std::process::{Command, Stdio};
    let log_path = updater_runtime_dir()?.join("updater.log");
    let mut cmd = Command::new("/bin/bash");
    cmd.arg(script)
        .arg(current_app)
        .arg(staged_app)
        .arg(&log_path)
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null());
    // Detach the updater from the parent's session AND process group so
    // it survives SIGHUP if the user closes a parent Terminal, and isn't
    // swept up as our own child on exit. `setsid` makes a new session
    // (which implicitly makes a new process group); the subsequent
    // `setpgid(0, 0)` is redundant but cheap belt-and-braces. Without
    // `setsid`, a terminal-parent session delivers SIGHUP to all
    // descendants on close, killing the updater mid-swap (skeptical M5).
    unsafe {
        cmd.pre_exec(|| {
            if libc::setsid() == -1 {
                // setsid fails if we're already a session leader.
                // That's fine; the setpgid below still detaches us
                // from the parent's process group.
            }
            libc::setpgid(0, 0);
            Ok(())
        });
    }
    cmd.spawn().context("Failed to spawn detached updater")?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    // Synthesize an ExitStatus with a specific numeric code. Uses the
    // Unix-specific `ExitStatusExt::from_raw` so this test module is
    // implicitly unix-only; that matches where the production classifier
    // actually matters (Linux + macOS wrapper flow; Windows also hits
    // these tests since it compiles the classifier).
    #[cfg(unix)]
    fn exit_with(code: i32) -> std::process::ExitStatus {
        use std::os::unix::process::ExitStatusExt;
        // wait()-style status bits: (code << 8) for a normal exit.
        std::process::ExitStatus::from_raw(code << 8)
    }

    #[cfg(unix)]
    #[test]
    fn classify_update_subprocess_success() {
        let result: std::io::Result<std::process::ExitStatus> = Ok(exit_with(0));
        assert_eq!(
            classify_update_subprocess(&result),
            UpdateSubprocessOutcome::BinaryReplaced
        );
    }

    #[cfg(unix)]
    #[test]
    fn classify_update_subprocess_already_up_to_date() {
        let result = Ok(exit_with(EXIT_CODE_ALREADY_UP_TO_DATE));
        assert_eq!(
            classify_update_subprocess(&result),
            UpdateSubprocessOutcome::AlreadyUpToDate
        );
    }

    #[cfg(unix)]
    #[test]
    fn classify_update_subprocess_bundle_update_staged() {
        let result = Ok(exit_with(EXIT_CODE_BUNDLE_UPDATE_STAGED));
        assert_eq!(
            classify_update_subprocess(&result),
            UpdateSubprocessOutcome::BundleUpdateStaged
        );
    }

    #[cfg(unix)]
    #[test]
    fn classify_update_subprocess_arbitrary_failure() {
        let result = Ok(exit_with(1));
        assert_eq!(
            classify_update_subprocess(&result),
            UpdateSubprocessOutcome::Failed
        );
        let result = Ok(exit_with(42));
        assert_eq!(
            classify_update_subprocess(&result),
            UpdateSubprocessOutcome::Failed
        );
    }

    #[test]
    fn classify_update_subprocess_spawn_error() {
        let result: std::io::Result<std::process::ExitStatus> =
            Err(std::io::Error::new(std::io::ErrorKind::NotFound, "boom"));
        assert_eq!(
            classify_update_subprocess(&result),
            UpdateSubprocessOutcome::Failed
        );
    }

    #[test]
    fn macos_dmg_asset_name_strips_leading_v() {
        assert_eq!(macos_dmg_asset_name("v0.2.49"), "Freenet-0.2.49.dmg");
        assert_eq!(
            macos_dmg_asset_name("v0.2.49-rc.1"),
            "Freenet-0.2.49-rc.1.dmg"
        );
    }

    #[test]
    fn macos_dmg_asset_name_passes_through_bare_version() {
        assert_eq!(macos_dmg_asset_name("0.2.49"), "Freenet-0.2.49.dmg");
    }

    #[test]
    fn macos_dmg_asset_name_only_strips_one_leading_v() {
        // A hypothetical tag like `vv0.2.49` (broken, but should not
        // silently eat both leading chars): verify we strip just one.
        assert_eq!(macos_dmg_asset_name("vv0.2.49"), "Freenet-v0.2.49.dmg");
    }
}
