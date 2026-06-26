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

/// Ed25519 public key (raw 32-byte, little-endian compressed point) used to
/// authenticate the release `SHA256SUMS.txt` manifest before any binary is
/// installed. The matching private key lives only in the
/// `FREENET_RELEASE_SIGNING_KEY` CI secret; the release workflow signs the
/// raw manifest bytes with it and uploads `SHA256SUMS.txt.sig` alongside the
/// manifest (see `.github/workflows/cross-compile.yml`).
///
/// Authenticating the manifest is what makes the existing fail-closed
/// checksum gate trustworthy: the checksums verify the binary against the
/// manifest, and this signature verifies the manifest against a key the
/// attacker does not hold.
///
/// The hex form (pinned by `release_pubkey_matches_published_hex`) is:
/// `eb2986604e34a3f985279a7e70852f3764d3347fe82074d92b1e4bc6336f8664`.
const FREENET_RELEASE_PUBKEY: [u8; 32] = [
    0xeb, 0x29, 0x86, 0x60, 0x4e, 0x34, 0xa3, 0xf9, 0x85, 0x27, 0x9a, 0x7e, 0x70, 0x85, 0x2f, 0x37,
    0x64, 0xd3, 0x34, 0x7f, 0xe8, 0x20, 0x74, 0xd9, 0x2b, 0x1e, 0x4b, 0xc6, 0x33, 0x6f, 0x86, 0x64,
];

/// Whether a release MUST carry a valid `SHA256SUMS.txt.sig` for the install
/// to proceed.
///
/// Two-release transition (do NOT flip this in the same release that first
/// ships signing):
///   * **false (now):** when the signature asset is PRESENT it is always
///     verified and an invalid/mismatched signature refuses the install
///     (fail-closed); when it is ABSENT the install is still allowed (with a
///     warning) so a node can keep updating to/from older, unsigned release
///     lineages that predate signing.
///   * **true (a later release):** an absent signature ALSO refuses the
///     install. Only flip this once every release a live node could be
///     updating from publishes a signature — i.e. after the signed floor is
///     established — otherwise nodes on the unsigned lineage brick their own
///     auto-update.
const REQUIRE_RELEASE_SIGNATURE: bool = false;

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
    /// The update subprocess could not be launched at all (`Err` from
    /// `Command::status`). This is an environmental failure that will
    /// not resolve on its own (e.g. exe missing / locked, OS resource
    /// exhaustion) — it DOES count toward the auto-update lockout so
    /// the exit-42 loop terminates.
    SpawnFailed,
    /// The update subprocess ran but exited non-zero with a code we
    /// don't specifically recognize. This bucket covers transient
    /// failures like GitHub API errors, download aborts, and checksum
    /// mismatches — those must NOT count toward the lockout (three
    /// flaky GitHub calls should not permanently disable auto-update).
    /// Install-stage failures (`replace_binary`) are recorded inside
    /// the subprocess itself via `record_update_failure`, so they will
    /// still lock out after MAX_UPDATE_FAILURES.
    OtherFailure,
}

/// What the wrapper should do to the persistent auto-update failure
/// counter for a given subprocess outcome. Pure so the glue between
/// [`UpdateSubprocessOutcome`] and the `record`/`clear` calls in
/// `service.rs` is directly unit-testable — a typo here reopens the
/// #3934 exit-42 restart loop with no CI signal.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum UpdateCounterAction {
    /// Clear the failure counter (install succeeded or detached
    /// updater committed to completing the swap).
    Clear,
    /// Increment the failure counter toward `MAX_UPDATE_FAILURES`.
    Record,
    /// Leave the counter alone (transient failure or benign outcome).
    NoChange,
}

/// Map a subprocess outcome to the correct counter action. The split
/// between `SpawnFailed` (records) and `OtherFailure` (no change) is
/// the anti-regression for Codex's P1 finding on PR #3941: lumping
/// transient network errors into the lockout counter would
/// permanently disable auto-update after a brief GitHub outage.
/// Install-stage failures are still counted because
/// `UpdateCommand::download_and_install` calls `record_update_failure`
/// directly on `replace_binary` error before propagating.
pub fn update_counter_action(outcome: UpdateSubprocessOutcome) -> UpdateCounterAction {
    match outcome {
        UpdateSubprocessOutcome::BinaryReplaced | UpdateSubprocessOutcome::BundleUpdateStaged => {
            UpdateCounterAction::Clear
        }
        UpdateSubprocessOutcome::SpawnFailed => UpdateCounterAction::Record,
        UpdateSubprocessOutcome::AlreadyUpToDate | UpdateSubprocessOutcome::OtherFailure => {
            UpdateCounterAction::NoChange
        }
    }
}

/// Classify the exit status of a `freenet update` subprocess invocation.
/// Pure over just the `io::Result<ExitStatus>` so it can be unit-tested
/// with synthetic inputs.
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
        Ok(_) => UpdateSubprocessOutcome::OtherFailure,
        Err(_) => UpdateSubprocessOutcome::SpawnFailed,
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
            // Confirming the binary is already current is as strong a
            // recovery signal as a successful install: any accumulated
            // failure counter from prior doomed attempts should clear
            // here, otherwise a user who hit the MAX_UPDATE_FAILURES
            // lockout and then updated via an external channel (apt,
            // homebrew, Windows installer) would stay permanently
            // locked out, since subsequent `freenet update` invocations
            // take this `AlreadyUpToDate` branch without ever touching
            // the counter (skeptical-review H1 on PR #3941).
            super::auto_update::clear_update_failures();
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

        // Download the SHA256SUMS.txt manifest AND authenticate it with the
        // baked-in release public key before trusting any hash inside it.
        //
        // Order matters: the signature authenticates the *manifest*, then the
        // fail-closed `required_checksum` gate below verifies the *binary*
        // against that now-trusted manifest. Without the signature step the
        // checksum gate only proves the binary matches whatever manifest was
        // served — which an attacker who can serve a manifest could forge.
        //
        // `download_and_verify_checksums` returns `None` when the manifest is
        // absent or its download failed (the fail-closed `required_checksum`
        // gate then refuses the install); it returns `Err` only for a
        // signature that is present-but-invalid (or absent when signatures
        // are required). That refusal, like the checksum gate, propagates out
        // of `download_and_install` BEFORE the `replace_binary` install step
        // (the only site that calls `record_update_failure`), so it is a
        // retryable `OtherFailure` (-> `NoChange`), never a
        // `MAX_UPDATE_FAILURES` lockout.
        let checksums = self.download_and_verify_checksums(release).await?;

        let temp_dir = tempfile::tempdir().context("Failed to create temp directory")?;

        // Download and install freenet
        let freenet_archive_path = temp_dir.path().join(&freenet_asset_name);
        download_file(
            &freenet_asset.browser_download_url,
            &freenet_archive_path,
            self.quiet,
        )
        .await?;

        // Fail-closed checksum gate. A missing manifest, a missing entry
        // for our asset, or a hash mismatch all REFUSE the install. This
        // matches the macOS DMG path (`maybe_perform_bundle_update`), which
        // already refuses unverified installs. The error propagates out of
        // `download_and_install` BEFORE the `replace_binary` install step
        // below — the only site that calls `record_update_failure` — so a
        // verification failure is classified as a retryable `OtherFailure`
        // (-> `NoChange`), NOT counted toward the `MAX_UPDATE_FAILURES`
        // lockout. A transiently-missing manifest therefore retries under
        // the existing exponential backoff rather than permanently
        // disabling auto-update.
        let expected_hash = required_checksum(checksums.as_ref(), &freenet_asset_name)?;
        if !self.quiet {
            println!("Verifying freenet checksum...");
        }
        verify_checksum(&freenet_archive_path, expected_hash)?;

        // Use a subdirectory per archive to avoid filename collisions
        let freenet_extract_dir = temp_dir.path().join("freenet");
        fs::create_dir_all(&freenet_extract_dir)?;
        let extracted_freenet =
            extract_binary(&freenet_archive_path, &freenet_extract_dir, "freenet")?;

        let current_exe = std::env::current_exe().context("Failed to get current executable")?;
        // The failure-counter record/clear is scoped to the install step
        // specifically, NOT to any earlier download / checksum / extract
        // failure. Those are transient (transient GitHub outage, flaky
        // connection, bad mirror) and are already retried under
        // `check_if_update_available`'s own exponential backoff. Counting
        // them toward the MAX_UPDATE_FAILURES lockout would permanently
        // disable auto-update after a brief network hiccup (Codex P1 on
        // PR #3941). Only genuine install-stage failures (AV lock,
        // read-only install dir, permission-denied) should accumulate.
        match replace_binary(&extracted_freenet, &current_exe) {
            Ok(()) => {
                super::auto_update::clear_update_failures();
            }
            Err(e) => {
                super::auto_update::record_update_failure();
                return Err(e);
            }
        }

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
                    // Kill the old wrapper + node child, then start a new
                    // wrapper with the updated binary. Targets only the
                    // service's own processes by command line so an unrelated
                    // freenet.exe is never killed (issue #4205).
                    super::service::kill_freenet_service_processes();
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

    /// Download `SHA256SUMS.txt` and, when the release published it,
    /// `SHA256SUMS.txt.sig`, returning the parsed checksums only after the
    /// manifest has been authenticated against [`FREENET_RELEASE_PUBKEY`].
    ///
    /// Returns:
    ///   * `Ok(Some(checksums))` — manifest downloaded and (per the
    ///     transition policy) authenticated; callers feed this into the
    ///     fail-closed `required_checksum` gate.
    ///   * `Ok(None)` — the manifest asset is absent, or its download failed.
    ///     The caller's `required_checksum` gate turns this into a fail-closed
    ///     refusal for the freenet binary (and a skip for best-effort fdev).
    ///   * `Err` — a signature was present but INVALID (tampering / wrong
    ///     key), the signature asset existed but could not be downloaded, or
    ///     a signature was required ([`REQUIRE_RELEASE_SIGNATURE`]) but
    ///     absent. All are fail-closed refusals; because this runs before
    ///     `replace_binary`, they are retryable `OtherFailure`s, not lockouts.
    ///
    /// The signature is verified over the EXACT bytes the checksums are parsed
    /// from (same in-memory buffer), so there is no time-of-check/time-of-use
    /// gap between "what was authenticated" and "what is trusted".
    async fn download_and_verify_checksums(&self, release: &Release) -> Result<Option<Checksums>> {
        let Some(checksums_asset) = release.assets.iter().find(|a| a.name == "SHA256SUMS.txt")
        else {
            // No manifest at all. Leave the fail-closed `required_checksum`
            // gate to refuse the binary install; nothing to authenticate.
            return Ok(None);
        };

        if !self.quiet {
            println!("Downloading checksums...");
        }
        let manifest_bytes = match download_bytes(&checksums_asset.browser_download_url).await {
            Ok(bytes) => bytes,
            Err(e) => {
                if !self.quiet {
                    eprintln!("Warning: Failed to download checksums: {}.", e);
                }
                return Ok(None);
            }
        };

        // Fetch the detached signature when the release published one. If the
        // asset exists but cannot be fetched, refuse rather than silently
        // downgrading to an unverified install — a signed manifest whose
        // signature we merely failed to retrieve must not be trusted.
        let signature = match release
            .assets
            .iter()
            .find(|a| a.name == "SHA256SUMS.txt.sig")
        {
            Some(sig_asset) => Some(
                download_bytes(&sig_asset.browser_download_url)
                    .await
                    .context("Failed to download release signature SHA256SUMS.txt.sig")?,
            ),
            None => None,
        };

        verify_release_manifest_signature(&manifest_bytes, signature.as_deref(), self.quiet)?;

        Ok(Some(Checksums::parse(&String::from_utf8_lossy(
            &manifest_bytes,
        ))))
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

        // Fail-closed, like the freenet binary: never install fdev
        // unverified. Because fdev updates are best-effort (they must never
        // block the service-file update or restart that follows the freenet
        // install), a missing/failed checksum here SKIPS the fdev update
        // rather than aborting the whole run.
        match required_checksum(checksums.as_ref(), asset_name) {
            Ok(expected_hash) => {
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
            Err(e) => {
                if !self.quiet {
                    eprintln!("Warning: {} Skipping fdev update.", e);
                }
                return;
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

// macOS DMG path only: the Linux/Windows binary path now goes through
// `download_and_verify_checksums`, which fetches the raw manifest bytes via
// `download_bytes` so the same buffer can be both signature-verified and
// parsed. The macOS DMG is independently Apple-signed + notarized, so its
// checksum check stays as-is for now.
#[cfg(target_os = "macos")]
async fn download_checksums(url: &str) -> Result<Checksums> {
    let bytes = download_bytes(url).await?;
    Ok(Checksums::parse(&String::from_utf8_lossy(&bytes)))
}

/// Download a small release asset fully into memory. Used for the
/// `SHA256SUMS.txt` manifest and its `.sig` detached signature, where the
/// caller needs the exact bytes (to verify a signature over them, or to feed
/// to `Signature::from_bytes`) rather than streaming to a file like
/// [`download_file`].
async fn download_bytes(url: &str) -> Result<Vec<u8>> {
    let client = reqwest::Client::builder()
        .user_agent("freenet-updater")
        .build()?;

    let response = client
        .get(url)
        .send()
        .await
        .context("Failed to download release asset")?;

    if !response.status().is_success() {
        anyhow::bail!("Download failed: {}", response.status());
    }

    Ok(response
        .bytes()
        .await
        .context("Failed to read release asset body")?
        .to_vec())
}

/// Authenticate the raw bytes of `SHA256SUMS.txt` against the baked-in
/// [`FREENET_RELEASE_PUBKEY`], applying the [`REQUIRE_RELEASE_SIGNATURE`]
/// transition policy. Thin wrapper over [`verify_manifest_signature_with`] so
/// the policy core (pubkey + require flag injectable) is unit-testable without
/// the real signing key and on every target OS.
fn verify_release_manifest_signature(
    manifest_bytes: &[u8],
    signature: Option<&[u8]>,
    quiet: bool,
) -> Result<()> {
    verify_manifest_signature_with(
        manifest_bytes,
        signature,
        &FREENET_RELEASE_PUBKEY,
        REQUIRE_RELEASE_SIGNATURE,
        quiet,
    )
}

/// Policy core for release-manifest signature verification. Pure (no I/O) and
/// fully parameterised so tests can exercise every branch — valid signature,
/// tampered manifest, wrong key, absent signature under both policies —
/// without ever touching the real private key.
///
/// Fail-closed contract:
///   * signature PRESENT  -> `verify_strict`; any failure (tamper, wrong key,
///     malformed length) returns `Err` (refuse the install).
///   * signature ABSENT   -> `Err` iff `require_signature`, else `Ok` with a
///     warning (the transition allowance for unsigned older lineages).
fn verify_manifest_signature_with(
    manifest_bytes: &[u8],
    signature: Option<&[u8]>,
    pubkey: &[u8; 32],
    require_signature: bool,
    quiet: bool,
) -> Result<()> {
    use ed25519_dalek::{Signature, VerifyingKey};

    let Some(sig_bytes) = signature else {
        if require_signature {
            anyhow::bail!(
                "Release is missing SHA256SUMS.txt.sig but a release signature is required; \
                 refusing to install. The release must publish a signed checksum manifest."
            );
        }
        if !quiet {
            eprintln!(
                "Warning: release did not publish SHA256SUMS.txt.sig; \
                 installing without release-signature verification."
            );
        }
        return Ok(());
    };

    // ed25519 signatures are exactly 64 bytes. A wrong length means the asset
    // is truncated or not a raw signature — refuse rather than guess.
    let sig_array: [u8; 64] = sig_bytes.try_into().map_err(|_| {
        anyhow::anyhow!(
            "Release signature has wrong length ({} bytes, expected 64); refusing to install.",
            sig_bytes.len()
        )
    })?;
    let parsed_signature = Signature::from_bytes(&sig_array);

    let verifying_key = VerifyingKey::from_bytes(pubkey)
        .context("Baked-in release public key is invalid; refusing to install")?;

    // `verify_strict` rejects non-canonical / small-order points (the same
    // hardening the website contract uses). A failure here means the manifest
    // was tampered with or signed by a key we don't trust: fail closed.
    verifying_key
        .verify_strict(manifest_bytes, &parsed_signature)
        .map_err(|e| {
            anyhow::anyhow!(
                "Release signature verification failed: {e}. The checksum manifest may be \
                 corrupted or tampered with; refusing to install."
            )
        })?;

    if !quiet {
        println!("Verified release signature.");
    }
    Ok(())
}

/// Resolve the expected SHA-256 for a REQUIRED install artifact,
/// **failing closed**. Returns the hex digest to compare against, or an
/// error when no trustworthy checksum is available: a missing manifest
/// (`checksums` is `None`) and a missing entry for `asset_name` both mean
/// "refuse to install this unverified artifact". This is the single gate
/// that gives Linux/Windows the same strictness the macOS DMG path already
/// has (`maybe_perform_bundle_update`).
///
/// The returned value is a plain `anyhow::Error`. Callers in the freenet
/// install path propagate it out of `download_and_install` BEFORE the
/// `replace_binary` step (the only place that calls `record_update_failure`),
/// so a refusal is classified as a retryable `OtherFailure` -> `NoChange`
/// rather than counting toward the `MAX_UPDATE_FAILURES` lockout. A
/// transiently-missing manifest therefore retries under the existing
/// exponential backoff instead of permanently disabling auto-update.
///
/// Pure (no I/O) so the fail-closed contract is unit-testable on every CI
/// runner regardless of target OS.
fn required_checksum<'a>(checksums: Option<&'a Checksums>, asset_name: &str) -> Result<&'a str> {
    checksums.and_then(|c| c.get(asset_name)).ok_or_else(|| {
        anyhow::anyhow!(
            "No SHA256 checksum available for {asset_name}; refusing to install an \
                 unverified binary. The release must publish a SHA256SUMS.txt that lists \
                 {asset_name}.",
        )
    })
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

/// Check if the on-disk systemd unit has drifted from the current
/// template and rewrite it if so. Locates the user unit first, then the
/// system unit, and delegates the staleness decision to
/// `update_service_file` (equality-based since #4287).
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

/// Build the systemd unit text that `freenet` would write *right now*
/// for `service_path`. For system-mode units the `User=` /
/// `Environment=HOME=` values are inherited from the on-disk unit
/// (`existing`) so that the regenerated template only differs from
/// what's on disk when the *template itself* has drifted — not merely
/// because the operator chose a different service user at install time.
/// Returning the same bytes the install path produced is what makes the
/// equality-based staleness check (`decide_wrapper_action`) stable
/// across runs (#4287).
#[cfg(target_os = "linux")]
fn render_current_systemd_unit(
    binary_path: &Path,
    existing: &str,
    system_mode: bool,
) -> Result<String> {
    if system_mode {
        // Extract User= and Environment=HOME= from existing file
        let username = existing
            .lines()
            .find_map(|l| l.strip_prefix("User="))
            .unwrap_or("freenet");
        let home_dir = existing
            .lines()
            .find_map(|l| l.strip_prefix("Environment=HOME="))
            .map(PathBuf::from)
            .unwrap_or_else(|| PathBuf::from(format!("/home/{username}")));
        let log_dir = home_dir.join(".local/state/freenet");
        Ok(generate_system_service_file(
            binary_path,
            &log_dir,
            username,
            &home_dir,
        ))
    } else {
        let home_dir = dirs::home_dir().context("Failed to get home directory")?;
        let log_dir = home_dir.join(".local/state/freenet");
        Ok(generate_user_service_file(binary_path, &log_dir))
    }
}

/// Update a specific service file if its on-disk content has drifted
/// from the current template.
///
/// Staleness is decided by byte-for-byte equality against what
/// `generate_{user,system}_service_file` produces now, NOT by grepping
/// for a fixed set of marker substrings (issue #4287, mirroring the
/// macOS wrapper fix #4286/#3967). The old marker check returned
/// "up to date" for any unit that happened to contain the three
/// markers (`ExecStopPost=`, `SuccessExitStatus=42`,
/// `RestartPreventExitStatus=43`), so a future template change
/// introducing a new directive (a new `ExecStartPre=`, a changed
/// `Restart=` policy, an extra `SuccessExitStatus` value, etc.) would
/// silently fail to propagate to existing Linux installs.
///
/// A `freenet.service.hash` sidecar records the SHA-256 of the unit we
/// last wrote so a later update can tell "Freenet's previous unit" from
/// an operator hand-edit and avoid clobbering customizations. Existing
/// installs have no sidecar; that case is treated as "ours" so the next
/// update fixes the unit and records the hash (migration). The shared
/// `decide_wrapper_action` decision matrix is artifact-agnostic — it
/// reasons about (template, on-disk content, sidecar) regardless of
/// whether the artifact is a macOS wrapper or a systemd unit — so the
/// Linux path reuses it rather than duplicating the logic.
#[cfg(target_os = "linux")]
fn update_service_file(
    binary_path: &Path,
    service_path: &Path,
    system_mode: bool,
    quiet: bool,
) -> Result<()> {
    let content = fs::read_to_string(service_path).context("Failed to read service file")?;
    let hash_path = service_path.with_extension("service.hash");

    let new_content = render_current_systemd_unit(binary_path, &content, system_mode)?;

    // Read the sidecar non-fatally: a missing/unreadable sidecar means
    // "no recorded hash" (migration shape for every pre-#4287 install).
    let sidecar_raw = fs::read_to_string(&hash_path).ok();
    let action = decide_wrapper_action(&new_content, Some(&content), sidecar_raw.as_deref());

    match &action {
        WrapperAction::UserModified { on_disk_hash } => {
            if !quiet {
                eprintln!(
                    "Warning: systemd unit at {} differs from the current Freenet \
                     template AND from the hash recorded in {}, suggesting it has \
                     been hand-edited. Leaving it alone. To accept the bundled \
                     unit, delete the .service.hash sidecar or reinstall via \
                     `freenet service install`. (on-disk hash: {})",
                    service_path.display(),
                    hash_path.display(),
                    on_disk_hash,
                );
            }
            return Ok(());
        }
        WrapperAction::UpToDate {
            backfill_sidecar: Some(h),
        } => {
            // Unit already matches the template but the sidecar is
            // missing/malformed — backfill it so a future hand-edit is
            // detected instead of silently clobbered. No daemon-reload:
            // the unit text didn't change.
            if let Err(e) = write_wrapper_hash_sidecar(&hash_path, h) {
                if !quiet {
                    eprintln!(
                        "Warning: failed to backfill service hash sidecar at {}: {}. \
                         User-modification detection on the next `freenet update` \
                         will fall back to treating the unit as ours.",
                        hash_path.display(),
                        e
                    );
                }
            }
            return Ok(());
        }
        WrapperAction::UpToDate {
            backfill_sidecar: None,
        } => {
            return Ok(()); // Already up to date, sidecar already correct.
        }
        WrapperAction::WriteOurs { .. } => {
            // Fall through to the rewrite path below.
        }
    }

    let WrapperAction::WriteOurs { new_hash } = &action else {
        unreachable!("non-WriteOurs actions returned above");
    };

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

    // Write the updated service file
    fs::write(service_path, &new_content).context("Failed to write updated service file")?;

    // Record the hash of what we just wrote. A failed sidecar write only
    // weakens future user-modification detection (it falls back to
    // treating the unit as ours), so warn but continue.
    if let Err(e) = write_wrapper_hash_sidecar(&hash_path, new_hash) {
        if !quiet {
            eprintln!(
                "Warning: failed to write service hash sidecar at {}: {}. \
                 User-modification detection on the next `freenet update` \
                 will fall back to treating the unit as ours.",
                hash_path.display(),
                e
            );
        }
    }

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
/// Pure helper: decide whether a launchd plist needs regeneration. Returns
/// true when the plist still references either legacy unrotated log path
/// (`Library/Logs/freenet/freenet.log` for StandardOutPath, or
/// `Library/Logs/freenet/freenet.error.log` for StandardErrorPath — issue
/// #4251). Newer plists send both standard streams to `/dev/null`.
///
/// Both substrings are checked independently: the second is NOT a
/// substring of the first (`.error.log` does not contain `.log` as a
/// contiguous tail — the dot precedes `error`). A partially-migrated
/// plist (stdout already `/dev/null` but stderr still legacy) must still
/// be detected; round-3 review caught the regression from collapsing
/// these into one check.
///
/// Cross-platform-compiled (no `#[cfg(target_os = "macos")]`) so non-macOS
/// CI runners can exercise the predicate per code-style.md / deployment.md
/// preference for pure decision helpers. The `#[allow(dead_code)]`
/// suppresses the dead-code lint on non-macOS builds where the only
/// non-test caller (`ensure_service_file_updated`) is cfg'd out.
#[allow(dead_code)]
pub(super) fn launchd_plist_needs_regen(content: &str) -> bool {
    content.contains("Library/Logs/freenet/freenet.log")
        || content.contains("Library/Logs/freenet/freenet.error.log")
}

/// Pure helper: macOS launchd wrapper needs regen iff the on-disk
/// content differs from what `generate_wrapper_script` produces now.
///
/// Replaces the older marker-substring check (issue #3967), which
/// returned `false` for the Jan 2026 wrapper that had the auto-update
/// markers but lacked the later-added orphan-killing
/// `pkill -f -u "$(id -u)" "freenet network"` block on startup. Hosts
/// installed with that wrapper sat in exit-43 loops for weeks because
/// the staleness check believed they were already current.
/// User-modification protection is layered on at the caller via the
/// `freenet-service-wrapper.sh.hash` sidecar (see
/// `ensure_service_file_updated`). `dead_code` allow + cross-platform
/// compile so non-macOS CI exercises the predicate.
#[allow(dead_code)]
pub(super) fn wrapper_needs_regen(current_template: &str, on_disk: &str) -> bool {
    current_template != on_disk
}

/// Pure helper: validate a wrapper-sidecar hash string. SHA-256 hex is
/// exactly 64 lowercase ASCII hex chars; anything else means the sidecar
/// is truncated, corrupted, or was never a hash. Treating malformed
/// as "no sidecar" (rather than as permanent UserModified) lets the
/// host recover from a partial prior write — without this, a single
/// interrupted update could lock the auto-update path forever.
/// Code-first-reviewer flagged on PR #4286.
#[allow(dead_code)]
pub(super) fn is_valid_wrapper_hash(s: &str) -> bool {
    s.len() == 64
        && s.bytes()
            .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
}

/// Pure helper: SHA-256 hex digest of `content`. Used to fingerprint the
/// wrapper we last wrote so a later `freenet update` can distinguish
/// "Freenet's previous wrapper" from "a user-edited wrapper" (#3967).
#[allow(dead_code)]
pub(super) fn wrapper_content_hash(content: &str) -> String {
    use sha2::{Digest, Sha256};
    use std::fmt::Write;
    let mut hasher = Sha256::new();
    hasher.update(content.as_bytes());
    hasher
        .finalize()
        .iter()
        .fold(String::with_capacity(64), |mut s, b| {
            write!(s, "{b:02x}").expect("writing to String is infallible");
            s
        })
}

/// The decision the auto-update path takes for the wrapper script,
/// derived purely from the current template, the on-disk wrapper, and
/// the on-disk sidecar — no I/O. Extracting this lets non-macOS CI
/// runners exercise the full decision matrix (skeptical-reviewer
/// flagged on PR #4286 that the macOS-only end-to-end tests don't run
/// in CI today).
#[derive(Debug, PartialEq, Eq)]
#[allow(dead_code)]
pub(super) enum WrapperAction {
    /// On-disk wrapper matches the current template byte-for-byte.
    /// No rewrite needed. If `backfill_sidecar` is `Some`, the caller
    /// should write that hash to the sidecar — covers the case where
    /// the wrapper is up-to-date but the sidecar is missing or
    /// malformed (self-heal so user-modification protection is in
    /// force on the next update).
    UpToDate { backfill_sidecar: Option<String> },
    /// On-disk wrapper differs from the template and we have grounds
    /// to overwrite it: no on-disk wrapper, or the sidecar shows the
    /// wrapper is the one we wrote previously. The caller should
    /// write `template` plus a sidecar containing `new_hash`.
    WriteOurs { new_hash: String },
    /// On-disk wrapper differs AND the sidecar records a different
    /// hash, indicating the user (or another tool) edited it. The
    /// caller should leave the wrapper alone and warn.
    /// `on_disk_hash` is the hash of the current on-disk content,
    /// provided so the warning message can include it.
    UserModified { on_disk_hash: String },
}

/// Pure helper: decide what to do with the macOS launchd wrapper given
/// the inputs the caller has gathered from disk. Cross-platform-compiled
/// so every CI runner exercises this decision matrix — closes the gap
/// the skeptical reviewer flagged on PR #4286 about the macOS-only E2E
/// tests not running in CI.
///
/// - `current_template`: what `generate_wrapper_script(binary)` would
///   produce now.
/// - `on_disk_wrapper`: `Some(content)` if the wrapper exists and was
///   read successfully (`None` if missing or unreadable).
/// - `sidecar_raw`: `Some(raw)` if the sidecar exists and was read
///   successfully (`None` if missing or unreadable). The raw bytes
///   are trimmed and validated as a 64-char hex string by this
///   helper, so the caller doesn't need to pre-process.
#[allow(dead_code)]
pub(super) fn decide_wrapper_action(
    current_template: &str,
    on_disk_wrapper: Option<&str>,
    sidecar_raw: Option<&str>,
) -> WrapperAction {
    let stored_hash = sidecar_raw
        .map(|s| s.trim())
        .filter(|s| is_valid_wrapper_hash(s));
    let template_hash = wrapper_content_hash(current_template);

    let Some(on_disk) = on_disk_wrapper else {
        return WrapperAction::WriteOurs {
            new_hash: template_hash,
        };
    };
    if !wrapper_needs_regen(current_template, on_disk) {
        // Wrapper byte-matches template. Backfill the sidecar if it's
        // missing or malformed, so a future hand-edit triggers the
        // user-modification path instead of silently being clobbered.
        let backfill_sidecar = match stored_hash {
            Some(s) if s == template_hash => None,
            _ => Some(template_hash),
        };
        return WrapperAction::UpToDate { backfill_sidecar };
    }
    let on_disk_hash = wrapper_content_hash(on_disk);
    match stored_hash {
        Some(s) if s != on_disk_hash => WrapperAction::UserModified { on_disk_hash },
        _ => WrapperAction::WriteOurs {
            new_hash: template_hash,
        },
    }
}

/// Write the wrapper hash sidecar in the canonical on-disk format
/// (single line of 64 lowercase hex chars + trailing newline). Single
/// source of truth so `install_macos_service` and
/// `ensure_service_file_updated` cannot drift apart on the format
/// — a divergence would loop-rewrite the wrapper on every update.
/// Testing-reviewer flagged on PR #4286.
#[allow(dead_code)]
pub(super) fn write_wrapper_hash_sidecar(path: &Path, hash: &str) -> std::io::Result<()> {
    fs::write(path, format!("{hash}\n"))
}

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
    let wrapper_hash_path = wrapper_path.with_extension("sh.hash");

    let new_wrapper_content = generate_wrapper_script(binary_path);

    // A non-UTF-8 wrapper on disk (very unusual for a shell script,
    // but possible if corruption snuck in) is treated the same as a
    // missing wrapper: we'll overwrite. Otherwise the whole update
    // path would abort with a misleading "Failed to read wrapper
    // script" error on an obviously-recoverable state.
    // Skeptical-reviewer flagged on PR #4286.
    let on_disk_wrapper = fs::read_to_string(&wrapper_path).ok();
    let sidecar_raw = fs::read_to_string(&wrapper_hash_path).ok();
    let action = decide_wrapper_action(
        &new_wrapper_content,
        on_disk_wrapper.as_deref(),
        sidecar_raw.as_deref(),
    );

    // Also force regeneration of the plist if it still points
    // StandardOutPath/StandardErrorPath at the unrotated freenet.log
    // paths (issue #4251). Read fails non-fatally: a transient FS
    // error should not kill the whole auto-update path. If we cannot
    // read the plist we conservatively skip the plist-regen step and
    // log a warning; the wrapper-side update still gets a chance.
    let plist_needs_update = match fs::read_to_string(&plist_path) {
        Ok(content) => launchd_plist_needs_regen(&content),
        Err(e) => {
            if !quiet {
                eprintln!(
                    "Warning: failed to read launchd plist at {} for migration check: {}. \
                     Skipping plist regen.",
                    plist_path.display(),
                    e
                );
            }
            false
        }
    };

    // Step 1: act on the wrapper FIRST. The plist's ProgramArguments
    // points at this wrapper path, so the wrapper must exist and be
    // executable before we point launchd at it. If wrapper regen
    // fails we exit BEFORE rewriting the plist so the system stays
    // in its previous-working state.
    match &action {
        WrapperAction::UserModified { on_disk_hash } => {
            if !quiet {
                eprintln!(
                    "Warning: wrapper script at {} differs from the current Freenet \
                     template AND from the hash recorded in {}, suggesting it has \
                     been hand-edited. Leaving it alone. To accept the bundled \
                     wrapper, delete the .sh.hash sidecar or reinstall via \
                     `freenet service install`. (on-disk hash: {})",
                    wrapper_path.display(),
                    wrapper_hash_path.display(),
                    on_disk_hash,
                );
            }
        }
        WrapperAction::UpToDate {
            backfill_sidecar: Some(h),
        } => {
            if let Err(e) = write_wrapper_hash_sidecar(&wrapper_hash_path, h) {
                if !quiet {
                    eprintln!(
                        "Warning: failed to backfill wrapper hash sidecar at {}: {}.",
                        wrapper_hash_path.display(),
                        e
                    );
                }
            }
        }
        WrapperAction::UpToDate {
            backfill_sidecar: None,
        } => {}
        WrapperAction::WriteOurs { new_hash } => {
            if !quiet {
                println!("Updating service wrapper to add auto-update support...");
            }
            let wrapper_dir = wrapper_path
                .parent()
                .context("Wrapper path has no parent directory")?;
            fs::create_dir_all(wrapper_dir).context("Failed to create wrapper directory")?;

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

            fs::write(&wrapper_path, &new_wrapper_content)
                .context("Failed to write wrapper script")?;

            // Failed sidecar write only weakens future user-mod
            // protection; it does not invalidate the wrapper we
            // just wrote, so warn but continue.
            if let Err(e) = write_wrapper_hash_sidecar(&wrapper_hash_path, new_hash) {
                if !quiet {
                    eprintln!(
                        "Warning: failed to write wrapper hash sidecar at {}: {}. \
                         User-modification detection on the next `freenet update` \
                         will fall back to treating the wrapper as ours.",
                        wrapper_hash_path.display(),
                        e
                    );
                }
            }

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
        }
    }

    // Step 2: regenerate the plist AFTER the wrapper is in place,
    // then reload launchd so the new config takes effect immediately
    // (rather than waiting for the next user logout / reboot — see
    // issue #4251 re-review #1).
    if plist_needs_update {
        if !quiet {
            println!("Updating launchd plist to drop legacy log paths (issue #4251)...");
        }
        let log_dir = home_dir.join("Library/Logs/freenet");
        let plist_content = super::service::generate_plist(&wrapper_path, &log_dir);
        // Best-effort backup so a botched rewrite is recoverable.
        let backup_path = plist_path.with_extension("plist.bak");
        if let Err(e) = fs::copy(&plist_path, &backup_path) {
            if !quiet {
                eprintln!(
                    "Warning: failed to back up plist: {}. Continuing anyway.",
                    e
                );
            }
        }
        fs::write(&plist_path, plist_content).context("Failed to write updated plist")?;

        // No automatic launchctl reload: when auto-update is driven by
        // the launchd-managed wrapper, `freenet update` runs as a child
        // of the wrapper's job tree. Apple documents `launchctl bootout`
        // as terminating the ENTIRE process tree of the agent — which
        // would kill the in-flight `freenet update` mid-call before the
        // subsequent `bootstrap` can run, silently failing in exactly
        // the auto-update path this code exists to support. (Detached
        // helpers race the bootout; `kickstart` does not re-read the
        // plist.) The new plist therefore takes effect at the next
        // user-driven reload or reboot. The wrapper-script rewrite
        // (issue #4251) DOES take effect at the next wrapper iteration
        // — that's where the dominant unbounded freenet.log growth was
        // coming from. Once the OS reads the new plist, the smaller
        // launchd-captured stdout/stderr also stops accumulating.
        if !quiet {
            println!(
                "launchd plist updated. To apply immediately without rebooting, run:\n  \
                 launchctl bootout gui/$(id -u) {plist}\n  \
                 launchctl bootstrap gui/$(id -u) {plist}",
                plist = plist_path.display()
            );
        }
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
    // SAFETY: `file.as_raw_fd()` returns an owned, open file descriptor
    // for the duration of this function; libc::flock only consults the
    // fd and the constant flag bits, with no pointer arguments.
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
        hdiutil_detach(&self.mount_point).ok();
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
    const SCRIPT: &str = include_str!("../../../scripts/macos-bundle-updater.sh");
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
    //
    // SAFETY: `pre_exec` is unsafe because the closure runs between
    // fork and exec, where only async-signal-safe functions are
    // allowed. `libc::setsid` and `libc::setpgid(0, 0)` are both
    // async-signal-safe per POSIX and do not allocate or touch
    // user-provided pointers.
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

    // ---- Issue #4287: systemd-unit-content-equality staleness detection. ----
    //
    // Before #4287 the Linux unit-staleness check (`systemd_unit_needs_regen`)
    // grepped for a fixed set of marker substrings (`ExecStopPost=`,
    // `SuccessExitStatus=42`, `RestartPreventExitStatus=43`, and the absence
    // of the legacy `StandardOutput=append:` target). Any future template
    // change introducing a NEW directive (a new `ExecStartPre=`, a changed
    // `Restart=` policy, an extra `SuccessExitStatus` value, etc.) would be
    // invisible to the check: a stale unit still containing the three markers
    // returned "up to date" and the update silently failed to propagate to
    // existing Linux installs. This is the exact anti-pattern #3967/#4286
    // surfaced on macOS.
    //
    // The fix reuses the cross-platform `decide_wrapper_action` decision
    // matrix (equality against the current template + `.service.hash`
    // sidecar for user-modification protection). The tests below exercise
    // that matrix with synthetic systemd-unit text so they run on EVERY CI
    // runner, not just Linux — closing the same "platform-gated path not
    // tested in CI" gap #4286 closed for macOS.

    /// Synthetic "current template" systemd unit + its hash, for the
    /// decision-matrix tests below. The exact text is irrelevant; what
    /// matters is whether the on-disk unit we feed in equals it.
    fn fixture_unit() -> (String, String) {
        let t = "[Service]\nExecStart=/usr/local/bin/freenet network\nSuccessExitStatus=42 43\n"
            .to_string();
        let h = wrapper_content_hash(&t);
        (t, h)
    }

    #[test]
    fn systemd_unit_new_directive_drift_is_detected_4287() {
        // The motivating scenario: a unit that still satisfies the OLD
        // marker check (has ExecStopPost / SuccessExitStatus=42 /
        // RestartPreventExitStatus=43) but is missing a hypothetical
        // future directive present in the current template. The old
        // marker grep returned "up to date"; equality detects the drift.
        let template = "[Service]\nExecStartPre=/usr/local/bin/freenet preflight\nExecStopPost=-/bin/sh -c '...'\nSuccessExitStatus=42 43\nRestartPreventExitStatus=43\n";
        let stale_but_markers_present = "[Service]\nExecStopPost=-/bin/sh -c '...'\nSuccessExitStatus=42 43\nRestartPreventExitStatus=43\n";
        // No sidecar (migration shape for existing installs) → WriteOurs.
        assert_eq!(
            decide_wrapper_action(template, Some(stale_but_markers_present), None),
            WrapperAction::WriteOurs {
                new_hash: wrapper_content_hash(template)
            },
            "a unit missing a new template directive must be detected as \
             drifted even though the legacy markers are all present (#4287)"
        );
    }

    #[test]
    fn systemd_unit_no_sidecar_stale_migrates_to_ours_4287() {
        // Pre-#4287 install: unit differs from current template (e.g.
        // legacy append:freenet.log target) and no sidecar exists.
        // Treat as ours so the host migrates on the next update.
        let (template, template_hash) = fixture_unit();
        let legacy = "[Service]\nExecStart=/usr/local/bin/freenet network\nStandardOutput=append:/home/u/.local/state/freenet/freenet.log\n";
        assert_eq!(
            decide_wrapper_action(&template, Some(legacy), None),
            WrapperAction::WriteOurs {
                new_hash: template_hash
            }
        );
    }

    #[test]
    fn systemd_unit_identical_to_template_with_sidecar_is_noop_4287() {
        // The post-#4287 fresh-install shape: unit byte-matches the
        // template AND a matching sidecar exists → no rewrite, no
        // daemon-reload loop.
        let (template, template_hash) = fixture_unit();
        assert_eq!(
            decide_wrapper_action(&template, Some(&template), Some(&template_hash)),
            WrapperAction::UpToDate {
                backfill_sidecar: None
            }
        );
    }

    #[test]
    fn systemd_unit_identical_to_template_missing_sidecar_backfills_4287() {
        // Migration self-heal: a pre-#4287 unit that happens to already
        // match today's template gets a sidecar backfilled so a future
        // hand-edit is detected instead of silently clobbered.
        let (template, template_hash) = fixture_unit();
        assert_eq!(
            decide_wrapper_action(&template, Some(&template), None),
            WrapperAction::UpToDate {
                backfill_sidecar: Some(template_hash)
            }
        );
    }

    #[test]
    fn systemd_unit_user_modified_is_preserved_4287() {
        // Unit differs from the template AND the sidecar records a hash
        // that doesn't match the on-disk content → operator hand-edited
        // it. Leave it alone.
        let (template, _) = fixture_unit();
        let edited = "[Service]\nExecStart=/usr/local/bin/freenet network --custom-flag\n";
        let edited_hash = wrapper_content_hash(edited);
        let prior_freenet_hash =
            wrapper_content_hash("[Service]\nExecStart=/usr/local/bin/freenet network\n");
        assert_eq!(
            decide_wrapper_action(&template, Some(edited), Some(&prior_freenet_hash)),
            WrapperAction::UserModified {
                on_disk_hash: edited_hash
            }
        );
    }

    #[test]
    fn systemd_unit_malformed_sidecar_treated_as_missing_4287() {
        // A truncated/corrupted `.service.hash` (e.g. from an interrupted
        // prior write) must NOT lock a stale unit into permanent
        // UserModified — fall through to WriteOurs so the host recovers.
        let (template, template_hash) = fixture_unit();
        let stale = "[Service]\nExecStart=/usr/local/bin/freenet network\n# old\n";
        assert_eq!(
            decide_wrapper_action(&template, Some(stale), Some("deadbe")),
            WrapperAction::WriteOurs {
                new_hash: template_hash
            }
        );
    }

    /// Linux-only: the REAL generators must be deterministic, otherwise
    /// the equality check would flag the freshly-installed unit as stale
    /// on every `freenet update` and loop-rewrite it forever. Mirrors
    /// `freshly_generated_wrapper_does_not_trigger_regen_loop` for macOS.
    #[test]
    #[cfg(target_os = "linux")]
    fn freshly_generated_systemd_unit_does_not_trigger_regen_loop_4287() {
        use std::path::PathBuf;
        let binary = PathBuf::from("/usr/local/bin/freenet");
        let log_dir = PathBuf::from("/home/u/.local/state/freenet");

        // User unit: install shape (unit + matching sidecar) → no-op.
        let user_unit = generate_user_service_file(&binary, &log_dir);
        let user_hash = wrapper_content_hash(&user_unit);
        assert_eq!(
            decide_wrapper_action(&user_unit, Some(&user_unit), Some(&user_hash)),
            WrapperAction::UpToDate {
                backfill_sidecar: None
            },
            "a freshly generated user unit with a matching sidecar must be \
             a no-op — otherwise every `freenet update` loops rewriting it"
        );

        // System unit: same determinism contract, plus the User=/HOME=
        // round-trip that `render_current_systemd_unit` relies on.
        let home = PathBuf::from("/home/svc");
        let system_unit = generate_system_service_file(&binary, &log_dir, "svc", &home);
        let system_hash = wrapper_content_hash(&system_unit);
        assert_eq!(
            decide_wrapper_action(&system_unit, Some(&system_unit), Some(&system_hash)),
            WrapperAction::UpToDate {
                backfill_sidecar: None
            },
        );
    }

    /// Linux-only: `render_current_systemd_unit` must reproduce the
    /// exact bytes of an installed system unit when fed that unit back
    /// as `existing` (it re-extracts `User=` / `Environment=HOME=`). If
    /// this round-trip drifted, every update would treat a fine unit as
    /// stale. Covers the issue's explicit "User=/Environment= override"
    /// edge case.
    #[test]
    #[cfg(target_os = "linux")]
    fn render_current_systemd_unit_round_trips_user_and_home_4287() {
        use std::path::PathBuf;
        let binary = PathBuf::from("/usr/local/bin/freenet");
        let home = PathBuf::from("/home/customsvc");
        let log_dir = home.join(".local/state/freenet");
        let installed = generate_system_service_file(&binary, &log_dir, "customsvc", &home);

        let rendered = render_current_systemd_unit(&binary, &installed, true)
            .expect("render must succeed for a well-formed system unit");
        assert_eq!(
            rendered, installed,
            "re-rendering an installed system unit must reproduce it \
             byte-for-byte so the equality check is a no-op (#4287)"
        );
    }

    /// Linux-only edge case (Gemini review): a system unit hand-edited
    /// to keep `User=` but DROP `Environment=HOME=` falls back to the
    /// inferred `/home/{user}` home. Every Freenet-generated unit always
    /// embeds `Environment=HOME=`, so this path is only reachable when an
    /// operator removed that line — in which case the unit already
    /// differs from the template and the fallback's exact value can't
    /// cause a spurious rewrite of an otherwise-pristine unit. This test
    /// pins the inferred-home behavior so the equality check stays
    /// predictable rather than depending on ambient state.
    #[test]
    #[cfg(target_os = "linux")]
    fn render_current_systemd_unit_infers_home_when_env_home_absent_4287() {
        use std::path::PathBuf;
        let binary = PathBuf::from("/usr/local/bin/freenet");
        // A unit with User= but no Environment=HOME= line.
        let existing = "[Service]\nUser=alice\nExecStart=/usr/local/bin/freenet network\n";

        let rendered = render_current_systemd_unit(&binary, existing, true)
            .expect("render must succeed even without Environment=HOME=");

        // The fallback infers /home/alice; the regenerated unit therefore
        // equals what the template produces for that inferred home.
        let inferred_home = PathBuf::from("/home/alice");
        let log_dir = inferred_home.join(".local/state/freenet");
        let expected = generate_system_service_file(&binary, &log_dir, "alice", &inferred_home);
        assert_eq!(
            rendered, expected,
            "missing Environment=HOME= must infer /home/{{user}} deterministically (#4287)"
        );
    }

    #[test]
    fn launchd_plist_with_legacy_log_paths_needs_regen() {
        let legacy = r#"<plist version="1.0"><dict>
    <key>StandardOutPath</key>
    <string>/Users/u/Library/Logs/freenet/freenet.log</string>
    <key>StandardErrorPath</key>
    <string>/Users/u/Library/Logs/freenet/freenet.error.log</string>
</dict></plist>"#;
        assert!(
            launchd_plist_needs_regen(legacy),
            "legacy plist must be detected as needing regen (#4251)"
        );
    }

    #[test]
    fn launchd_plist_with_stderr_only_legacy_path_still_needs_regen() {
        // Regression for round-3 review: a partially-migrated plist
        // (stdout already /dev/null, stderr still pointing at the
        // legacy freenet.error.log) must still be detected. The
        // single-substring shortcut for `freenet.log` was wrong
        // because `.error.log` does NOT contain `.log` as a
        // contiguous substring (`.` precedes `error`).
        let partial = r#"<plist version="1.0"><dict>
    <key>StandardOutPath</key>
    <string>/dev/null</string>
    <key>StandardErrorPath</key>
    <string>/Users/u/Library/Logs/freenet/freenet.error.log</string>
</dict></plist>"#;
        assert!(
            launchd_plist_needs_regen(partial),
            "plist with legacy stderr-only path must still need regen (round-3 review)"
        );
    }

    /// Round-3 review L2: assert the freshly-generated plist template
    /// does NOT contain the legacy-path substrings, otherwise every
    /// auto-update would loop rewriting it. Calls the real generator
    /// with synthetic inputs so a future edit to `generate_plist` that
    /// accidentally adds the legacy path is caught at test time.
    #[test]
    #[cfg(target_os = "macos")]
    fn freshly_generated_plist_does_not_trigger_regen_loop() {
        use std::path::PathBuf;
        let wrapper = PathBuf::from("/Users/test/.local/bin/freenet-service-wrapper.sh");
        let log_dir = PathBuf::from("/Users/test/Library/Logs/freenet");
        let generated = super::super::service::generate_plist(&wrapper, &log_dir);
        assert!(
            !launchd_plist_needs_regen(&generated),
            "freshly-generated plist must not match the legacy-path predicate \
             — otherwise every auto-update would loop rewriting it.\n\nGenerated:\n{generated}"
        );
    }

    #[test]
    fn launchd_plist_with_dev_null_is_up_to_date() {
        let current = r#"<plist version="1.0"><dict>
    <key>StandardOutPath</key>
    <string>/dev/null</string>
    <key>StandardErrorPath</key>
    <string>/dev/null</string>
</dict></plist>"#;
        assert!(
            !launchd_plist_needs_regen(current),
            "current /dev/null plist must NOT be flagged for regen"
        );
    }

    // ---- Issue #3967: wrapper-content-equality staleness detection. ----
    // Pin: ANY drift between the on-disk wrapper and what
    // `generate_wrapper_script` produces now must trigger regen,
    // regardless of which markers are present. See `wrapper_needs_regen`
    // docs for the Jan-2026 incident this guards against.

    #[test]
    fn wrapper_with_auto_update_hook_but_no_pkill_block_needs_regen_3967() {
        // Jan 2026 shape: has auto-update markers but no pkill-on-startup
        // block. The old marker check returned false here.
        let jan_2026_wrapper = r#"#!/bin/bash
BACKOFF=10
while true; do
    /usr/local/bin/freenet network
    EXIT_CODE=$?
    if [ $EXIT_CODE -eq 42 ]; then
        /usr/local/bin/freenet update --quiet
        continue
    fi
    sleep $BACKOFF
done
"#;
        let current_template = r#"#!/bin/bash
pkill -f -u "$(id -u)" "freenet network" 2>/dev/null || true
BACKOFF=10
while true; do
    /usr/local/bin/freenet network
    EXIT_CODE=$?
    if [ $EXIT_CODE -eq 42 ]; then
        /usr/local/bin/freenet update --quiet
        continue
    fi
    sleep $BACKOFF
done
"#;
        assert!(
            wrapper_needs_regen(current_template, jan_2026_wrapper),
            "Jan 2026 wrapper missing the pkill-on-startup block must \
             be detected as needing regen (#3967)."
        );
    }

    #[test]
    fn wrapper_identical_to_template_does_not_need_regen() {
        let template = "#!/bin/bash\necho hi\n";
        assert!(
            !wrapper_needs_regen(template, template),
            "byte-identical wrapper must NOT be flagged for regen — \
             otherwise every `freenet update` would loop rewriting it"
        );
    }

    #[test]
    fn wrapper_needs_regen_detects_single_char_drift() {
        assert!(
            wrapper_needs_regen("BACKOFF=10\n", "BACKOFF=11\n"),
            "any drift between template and on-disk wrapper must \
             trigger regen (#3967)"
        );
    }

    /// Calls the real macOS wrapper generator with synthetic inputs
    /// so a future edit to `generate_wrapper_script` that introduces
    /// nondeterminism (timestamps, random IDs, environment-dependent
    /// strings) is caught at test time — otherwise every auto-update
    /// would loop rewriting an "always-stale" wrapper.
    #[test]
    #[cfg(target_os = "macos")]
    fn freshly_generated_wrapper_does_not_trigger_regen_loop() {
        use std::path::PathBuf;
        let binary = PathBuf::from("/Users/test/.local/bin/freenet");
        let a = super::super::service::generate_wrapper_script(&binary);
        let b = super::super::service::generate_wrapper_script(&binary);
        assert!(
            !wrapper_needs_regen(&a, &b),
            "two calls to generate_wrapper_script with the same \
             binary path must produce identical output — \
             otherwise every auto-update would loop rewriting the \
             wrapper"
        );
    }

    #[test]
    fn is_valid_wrapper_hash_accepts_real_sha256_hex_only() {
        // 64 lowercase hex chars: the actual SHA-256 shape.
        let good = wrapper_content_hash("anything");
        assert!(is_valid_wrapper_hash(&good));
        assert!(is_valid_wrapper_hash(&"a".repeat(64)));
        assert!(is_valid_wrapper_hash(&"0123456789abcdef".repeat(4)));

        // Reject everything else — these are all "malformed sidecar"
        // shapes that, before the carve-out, would have locked the
        // host into permanent UserModified.
        assert!(!is_valid_wrapper_hash(""), "empty");
        assert!(!is_valid_wrapper_hash("not a hash"), "free text");
        assert!(!is_valid_wrapper_hash(&"a".repeat(63)), "truncated");
        assert!(!is_valid_wrapper_hash(&"a".repeat(65)), "too long");
        assert!(
            !is_valid_wrapper_hash(&"A".repeat(64)),
            "uppercase rejected — wrapper_content_hash always emits lowercase"
        );
        assert!(
            !is_valid_wrapper_hash(&"g".repeat(64)),
            "non-hex char rejected"
        );
    }

    // ---- decide_wrapper_action: pure decision matrix. ----
    //
    // Skeptical-reviewer flagged that the macOS-only end-to-end tests
    // for `ensure_service_file_updated` don't run in CI (`ci.yml` has
    // no macOS `cargo test` job). Extracting the decision logic into
    // this pure helper closes that gap: every CI runner exercises the
    // matrix below regardless of target OS, so a future regression on
    // any branch is caught at PR time.

    /// Build the "current" template + its hash for use in the
    /// decision-matrix tests below. The exact content is irrelevant;
    /// what matters is that on-disk wrappers we feed in differ from
    /// it for the regen cases and match it for the up-to-date cases.
    fn fixture_template() -> (String, String) {
        let t = "#!/bin/bash\necho fixture\n".to_string();
        let h = wrapper_content_hash(&t);
        (t, h)
    }

    #[test]
    fn decide_wrapper_action_no_wrapper_writes_ours() {
        let (template, hash) = fixture_template();
        assert_eq!(
            decide_wrapper_action(&template, None, None),
            WrapperAction::WriteOurs { new_hash: hash }
        );
    }

    #[test]
    fn decide_wrapper_action_stale_wrapper_no_sidecar_writes_ours_3967() {
        // The migration shape for every pre-#3967 install: wrapper
        // differs from current template (e.g. missing the pkill
        // block) and no sidecar exists. Treat as ours so the host
        // can recover.
        let (template, hash) = fixture_template();
        let stale = "#!/bin/bash\n# stale Jan 2026 wrapper\n";
        assert_eq!(
            decide_wrapper_action(&template, Some(stale), None),
            WrapperAction::WriteOurs { new_hash: hash }
        );
    }

    #[test]
    fn decide_wrapper_action_stale_wrapper_matching_sidecar_writes_ours() {
        // Sidecar present and matches the on-disk hash: it's ours,
        // safe to overwrite (e.g. template drifted between releases).
        let (template, template_hash) = fixture_template();
        let stale = "#!/bin/bash\n# previous Freenet wrapper\n";
        let stale_hash = wrapper_content_hash(stale);
        assert_eq!(
            decide_wrapper_action(&template, Some(stale), Some(&stale_hash)),
            WrapperAction::WriteOurs {
                new_hash: template_hash
            }
        );
    }

    #[test]
    fn decide_wrapper_action_stale_wrapper_with_trailing_newline_sidecar() {
        // Sidecar with `\n` (the canonical on-disk form) must trim
        // and validate as ours. Pins the wire-format contract between
        // `write_wrapper_hash_sidecar` and `decide_wrapper_action` —
        // testing-reviewer flagged on PR #4286 that a future
        // refactor dropping the trim would silently break.
        let (template, template_hash) = fixture_template();
        let stale = "#!/bin/bash\n# previous Freenet wrapper\n";
        let sidecar_with_newline = format!("{}\n", wrapper_content_hash(stale));
        assert_eq!(
            decide_wrapper_action(&template, Some(stale), Some(&sidecar_with_newline)),
            WrapperAction::WriteOurs {
                new_hash: template_hash
            },
            "matching sidecar with trailing newline must validate the same as one without",
        );
    }

    #[test]
    fn decide_wrapper_action_user_modified_wrapper_is_preserved() {
        let (template, _) = fixture_template();
        let edited = "#!/bin/bash\n# user customized this\n";
        let edited_hash = wrapper_content_hash(edited);
        // Sidecar records what Freenet wrote previously (different
        // from what's currently on disk → user edited it).
        let prior_freenet_hash = wrapper_content_hash("#!/bin/bash\n# prior Freenet\n");
        assert_eq!(
            decide_wrapper_action(&template, Some(edited), Some(&prior_freenet_hash)),
            WrapperAction::UserModified {
                on_disk_hash: edited_hash
            }
        );
    }

    #[test]
    fn decide_wrapper_action_malformed_sidecar_treated_as_missing_3967() {
        // Truncated / corrupted sidecar must NOT lock the host into
        // permanent UserModified — fall through to WriteOurs so the
        // auto-update path can recover from a partial prior write.
        let (template, hash) = fixture_template();
        let stale = "#!/bin/bash\n# stale\n";
        // Truncated hex (length 6 — not 64).
        assert_eq!(
            decide_wrapper_action(&template, Some(stale), Some("deadbe")),
            WrapperAction::WriteOurs {
                new_hash: hash.clone()
            }
        );
        // Free text.
        assert_eq!(
            decide_wrapper_action(&template, Some(stale), Some("not a hash at all")),
            WrapperAction::WriteOurs {
                new_hash: hash.clone()
            }
        );
        // Empty file.
        assert_eq!(
            decide_wrapper_action(&template, Some(stale), Some("")),
            WrapperAction::WriteOurs { new_hash: hash }
        );
    }

    #[test]
    fn decide_wrapper_action_up_to_date_with_matching_sidecar_is_noop() {
        let (template, template_hash) = fixture_template();
        assert_eq!(
            decide_wrapper_action(&template, Some(&template), Some(&template_hash)),
            WrapperAction::UpToDate {
                backfill_sidecar: None
            }
        );
    }

    #[test]
    fn decide_wrapper_action_up_to_date_with_missing_sidecar_backfills() {
        // Pre-#3967 install with a wrapper that happens to be
        // byte-identical to today's template (rare but possible)
        // — backfill the sidecar so future user-mod protection
        // works. Code-first-reviewer flagged on PR #4286.
        let (template, template_hash) = fixture_template();
        assert_eq!(
            decide_wrapper_action(&template, Some(&template), None),
            WrapperAction::UpToDate {
                backfill_sidecar: Some(template_hash)
            }
        );
    }

    #[test]
    fn decide_wrapper_action_up_to_date_with_malformed_sidecar_backfills() {
        // Same self-heal as the missing-sidecar case: a corrupted
        // sidecar (truncated, free text, etc.) gets overwritten with
        // a valid one when the wrapper is already up to date.
        let (template, template_hash) = fixture_template();
        assert_eq!(
            decide_wrapper_action(&template, Some(&template), Some("garbage")),
            WrapperAction::UpToDate {
                backfill_sidecar: Some(template_hash)
            }
        );
    }

    #[test]
    fn wrapper_content_hash_is_stable_and_distinct() {
        let a = wrapper_content_hash("hello");
        let b = wrapper_content_hash("hello");
        let c = wrapper_content_hash("hello\n"); // trailing newline differs
        assert_eq!(a, b, "hash must be deterministic for the same input");
        assert_ne!(
            a, c,
            "hash must distinguish inputs differing by a single byte — \
             otherwise the user-modification sidecar can't tell a \
             hand-edited wrapper from Freenet's"
        );
        assert_eq!(
            a.len(),
            64,
            "SHA-256 hex digest must be exactly 64 chars; the wrapper \
             sidecar's trim() comparison relies on a stable encoding"
        );
    }

    /// Shared mutex for tests that mutate `HOME`. Cargo runs unit
    /// tests in parallel, so HOME-touching tests must serialize on
    /// this lock or they'd see each other's tempdirs and the assertions
    /// would become flaky.
    #[cfg(target_os = "macos")]
    static HOME_ENV_LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());

    /// RAII guard that restores the prior `HOME` on drop. Tests use it
    /// to point `ensure_service_file_updated` at a tempdir without
    /// leaking that override to sibling tests.
    #[cfg(target_os = "macos")]
    struct ScopedHome {
        prior: Option<std::ffi::OsString>,
        _lock: std::sync::MutexGuard<'static, ()>,
    }
    #[cfg(target_os = "macos")]
    impl ScopedHome {
        fn new(path: &std::path::Path) -> Self {
            let lock = HOME_ENV_LOCK
                .lock()
                .unwrap_or_else(|poison| poison.into_inner());
            let prior = std::env::var_os("HOME");
            // SAFETY: Rust 2024 marks `set_var`/`remove_var` unsafe
            // because other threads could be reading the environment
            // concurrently. The static `HOME_ENV_LOCK` mutex
            // serializes ALL test code in this module that touches
            // `HOME`, and nothing else in this crate's test binary
            // reads `HOME` while a `ScopedHome` is alive. The
            // serialization is local to the cargo-test process, which
            // is what matters: cargo runs each binary in its own
            // process, so HOME-touching tests in other binaries (the
            // lib tests, integration tests, etc.) don't share state
            // with this one.
            unsafe {
                std::env::set_var("HOME", path);
            }
            Self { prior, _lock: lock }
        }
    }
    #[cfg(target_os = "macos")]
    impl Drop for ScopedHome {
        fn drop(&mut self) {
            // SAFETY: same lock-guarded reasoning as `ScopedHome::new`
            // above; the `_lock` field keeps the mutex held until
            // after this drop runs, so no sibling test sees a
            // mid-restore HOME value.
            unsafe {
                match self.prior.take() {
                    Some(v) => std::env::set_var("HOME", v),
                    None => std::env::remove_var("HOME"),
                }
            }
        }
    }

    /// End-to-end regression for issue #3967: a host with a stale
    /// pre-pkill wrapper but no hash sidecar (the migration shape for
    /// every existing install before this PR landed) must have its
    /// wrapper rewritten by `ensure_service_file_updated`, and a hash
    /// sidecar must be left behind so the NEXT update has
    /// user-modification protection.
    #[test]
    #[cfg(target_os = "macos")]
    fn ensure_service_file_updated_rewrites_pre_pkill_wrapper_and_records_hash() {
        use std::fs;
        use tempfile::TempDir;

        let tmp = TempDir::new().expect("create tempdir");
        let fake_home = tmp.path();
        let _home = ScopedHome::new(fake_home);

        // The macOS branch only acts when the plist exists.
        let launch_agents = fake_home.join("Library/LaunchAgents");
        fs::create_dir_all(&launch_agents).unwrap();
        fs::write(launch_agents.join("org.freenet.node.plist"), "<plist/>").unwrap();

        // Stale Jan-2026-shape wrapper with no sidecar (pre-#3967 install).
        let wrapper_dir = fake_home.join(".local/bin");
        fs::create_dir_all(&wrapper_dir).unwrap();
        let wrapper = wrapper_dir.join("freenet-service-wrapper.sh");
        let stale = "#!/bin/bash\n# EXIT_CODE=$?\n# freenet update\n# (no pkill block!)\n";
        fs::write(&wrapper, stale).unwrap();
        let hash_sidecar = wrapper.with_extension("sh.hash");
        assert!(!hash_sidecar.exists(), "test precondition: no sidecar yet");

        let binary = wrapper_dir.join("freenet");
        ensure_service_file_updated(&binary, /* quiet */ true)
            .expect("ensure_service_file_updated should succeed");

        let new_content = fs::read_to_string(&wrapper).unwrap();
        let expected = super::super::service::generate_wrapper_script(&binary);
        assert_eq!(
            new_content, expected,
            "stale Jan-2026 wrapper must be replaced byte-for-byte by the current template (#3967)"
        );
        let recorded = fs::read_to_string(&hash_sidecar)
            .expect("hash sidecar must be written after a successful regen");
        assert_eq!(
            recorded.trim(),
            wrapper_content_hash(&new_content),
            "recorded sidecar hash must match the wrapper we just wrote"
        );
    }

    /// Pin the install -> update no-op contract: when the wrapper is
    /// byte-identical to the current template AND a matching sidecar
    /// exists (the post-#3967 fresh-install shape), the next
    /// `freenet update` must be a no-op. If either writer (`install`
    /// in service.rs, the regen path here) ever changes the sidecar
    /// format without the other matching, this test fails by
    /// detecting the resulting regen loop. Testing reviewer flagged
    /// this gap on PR #4286.
    #[test]
    #[cfg(target_os = "macos")]
    fn ensure_service_file_updated_is_noop_after_fresh_install() {
        use std::fs;
        use tempfile::TempDir;

        let tmp = TempDir::new().unwrap();
        let fake_home = tmp.path();
        let _home = ScopedHome::new(fake_home);

        let launch_agents = fake_home.join("Library/LaunchAgents");
        fs::create_dir_all(&launch_agents).unwrap();
        fs::write(launch_agents.join("org.freenet.node.plist"), "<plist/>").unwrap();

        let wrapper_dir = fake_home.join(".local/bin");
        fs::create_dir_all(&wrapper_dir).unwrap();
        let binary = wrapper_dir.join("freenet");

        // Simulate what `install_macos_service` writes: wrapper text
        // equal to what `generate_wrapper_script` produces now, plus
        // a sidecar containing its SHA-256.
        let wrapper = wrapper_dir.join("freenet-service-wrapper.sh");
        let template = super::super::service::generate_wrapper_script(&binary);
        fs::write(&wrapper, &template).unwrap();
        let sidecar = wrapper.with_extension("sh.hash");
        fs::write(&sidecar, format!("{}\n", wrapper_content_hash(&template))).unwrap();

        // Snapshot mtimes so we can prove neither file was rewritten.
        let wrapper_before = fs::metadata(&wrapper).unwrap().modified().unwrap();
        let sidecar_before = fs::metadata(&sidecar).unwrap().modified().unwrap();

        ensure_service_file_updated(&binary, /* quiet */ true).unwrap();

        // Both files must still hold the same content AND have
        // unchanged mtimes (no needless rewrite).
        assert_eq!(fs::read_to_string(&wrapper).unwrap(), template);
        assert_eq!(
            fs::read_to_string(&sidecar).unwrap().trim(),
            wrapper_content_hash(&template),
        );
        assert_eq!(
            fs::metadata(&wrapper).unwrap().modified().unwrap(),
            wrapper_before,
            "wrapper must not be rewritten when already up to date \
             — otherwise every `freenet update` loops on it"
        );
        assert_eq!(
            fs::metadata(&sidecar).unwrap().modified().unwrap(),
            sidecar_before,
            "sidecar must not be rewritten when wrapper is already up to date"
        );
    }

    /// Self-heal: if the wrapper is up-to-date but the sidecar is
    /// missing (a prior wrapper write succeeded but the sidecar write
    /// failed, or a pre-#3967 install that happened to land on the
    /// current template), the next `freenet update` must backfill
    /// the sidecar so the NEXT user-modification protection works.
    /// Without this, the host stays in the "no sidecar = treat as
    /// ours" migration state forever and a later hand-edit would be
    /// silently clobbered on the next template change.
    /// Code-first-reviewer flagged this on PR #4286.
    #[test]
    #[cfg(target_os = "macos")]
    fn ensure_service_file_updated_backfills_missing_sidecar() {
        use std::fs;
        use tempfile::TempDir;

        let tmp = TempDir::new().unwrap();
        let fake_home = tmp.path();
        let _home = ScopedHome::new(fake_home);

        let launch_agents = fake_home.join("Library/LaunchAgents");
        fs::create_dir_all(&launch_agents).unwrap();
        fs::write(launch_agents.join("org.freenet.node.plist"), "<plist/>").unwrap();

        let wrapper_dir = fake_home.join(".local/bin");
        fs::create_dir_all(&wrapper_dir).unwrap();
        let binary = wrapper_dir.join("freenet");

        // Wrapper is up-to-date but no sidecar exists (the gap the
        // self-heal closes).
        let wrapper = wrapper_dir.join("freenet-service-wrapper.sh");
        let template = super::super::service::generate_wrapper_script(&binary);
        fs::write(&wrapper, &template).unwrap();
        let sidecar = wrapper.with_extension("sh.hash");
        assert!(!sidecar.exists(), "precondition: no sidecar");

        ensure_service_file_updated(&binary, /* quiet */ true).unwrap();

        // Wrapper unchanged.
        assert_eq!(fs::read_to_string(&wrapper).unwrap(), template);
        // Sidecar now present with the right hash.
        assert_eq!(
            fs::read_to_string(&sidecar).unwrap().trim(),
            wrapper_content_hash(&template),
            "sidecar must be backfilled when wrapper is up-to-date \
             but sidecar was missing"
        );
    }

    /// Malformed (truncated / corrupted) sidecar must NOT lock the
    /// host into permanent `UserModified`: treating a non-64-hex
    /// sidecar as "no sidecar" lets the auto-update path recover
    /// after an interrupted prior write. The wrapper here is stale,
    /// so the malformed sidecar should fall through to WriteOurs
    /// rather than UserModified. Code-first-reviewer flagged this
    /// on PR #4286.
    #[test]
    #[cfg(target_os = "macos")]
    fn ensure_service_file_updated_treats_malformed_sidecar_as_missing() {
        use std::fs;
        use tempfile::TempDir;

        let tmp = TempDir::new().unwrap();
        let fake_home = tmp.path();
        let _home = ScopedHome::new(fake_home);

        let launch_agents = fake_home.join("Library/LaunchAgents");
        fs::create_dir_all(&launch_agents).unwrap();
        fs::write(launch_agents.join("org.freenet.node.plist"), "<plist/>").unwrap();

        let wrapper_dir = fake_home.join(".local/bin");
        fs::create_dir_all(&wrapper_dir).unwrap();
        let wrapper = wrapper_dir.join("freenet-service-wrapper.sh");
        let stale = "#!/bin/bash\n# stale Jan 2026 wrapper, no pkill block\n";
        fs::write(&wrapper, stale).unwrap();
        // Corrupted / truncated sidecar — what a partial prior write
        // could leave behind.
        fs::write(wrapper.with_extension("sh.hash"), "deadbe").unwrap();

        let binary = wrapper_dir.join("freenet");
        ensure_service_file_updated(&binary, /* quiet */ true).unwrap();

        // Wrapper must have been rewritten — the malformed sidecar
        // didn't latch the host into UserModified.
        let new_content = fs::read_to_string(&wrapper).unwrap();
        assert_ne!(new_content, stale, "stale wrapper must be replaced");
        let expected = super::super::service::generate_wrapper_script(&binary);
        assert_eq!(new_content, expected);
        // Sidecar replaced with a real hash.
        let recorded = fs::read_to_string(wrapper.with_extension("sh.hash")).unwrap();
        assert!(
            is_valid_wrapper_hash(recorded.trim()),
            "sidecar must now hold a valid SHA-256, got {recorded:?}"
        );
    }

    /// User-modification protection: when the on-disk wrapper differs
    /// from the current template AND the recorded sidecar hash
    /// doesn't match the on-disk content, `ensure_service_file_updated`
    /// must leave the wrapper alone (preserving the user's edits).
    #[test]
    #[cfg(target_os = "macos")]
    fn ensure_service_file_updated_preserves_user_modified_wrapper() {
        use std::fs;
        use tempfile::TempDir;

        let tmp = TempDir::new().expect("create tempdir");
        let fake_home = tmp.path();
        let _home = ScopedHome::new(fake_home);

        let launch_agents = fake_home.join("Library/LaunchAgents");
        fs::create_dir_all(&launch_agents).unwrap();
        fs::write(launch_agents.join("org.freenet.node.plist"), "<plist/>").unwrap();

        let wrapper_dir = fake_home.join(".local/bin");
        fs::create_dir_all(&wrapper_dir).unwrap();
        let wrapper = wrapper_dir.join("freenet-service-wrapper.sh");
        let user_edited = "#!/bin/bash\n# I have customized this myself\necho user\n";
        fs::write(&wrapper, user_edited).unwrap();
        // Sidecar hash that doesn't match the on-disk file: simulates
        // Freenet writing the wrapper, then the user editing it.
        let stale_hash = wrapper_content_hash("something-different");
        fs::write(wrapper.with_extension("sh.hash"), format!("{stale_hash}\n")).unwrap();

        let binary = wrapper_dir.join("freenet");
        ensure_service_file_updated(&binary, /* quiet */ true)
            .expect("ensure_service_file_updated should succeed even when skipping");

        let still_on_disk = fs::read_to_string(&wrapper).unwrap();
        assert_eq!(
            still_on_disk, user_edited,
            "user-modified wrapper must be preserved (sidecar hash mismatch)"
        );
    }

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
    fn classify_update_subprocess_other_failure() {
        // Arbitrary non-zero exit codes that aren't one of the specific
        // known codes fall into `OtherFailure`. This variant covers
        // transient network / GitHub / checksum errors that must NOT
        // count toward the lockout — that's the Codex P1 anti-regression
        // (see the split from a single `Failed` variant on PR #3941).
        let result = Ok(exit_with(1));
        assert_eq!(
            classify_update_subprocess(&result),
            UpdateSubprocessOutcome::OtherFailure
        );
        let result = Ok(exit_with(42));
        assert_eq!(
            classify_update_subprocess(&result),
            UpdateSubprocessOutcome::OtherFailure
        );
    }

    #[test]
    fn classify_update_subprocess_spawn_error() {
        // Spawn failure is distinct from runtime failure: the subprocess
        // never started (exe missing, AV-locked binary, OS resource
        // exhaustion). These are environmental and persistent, so they
        // MUST count toward the lockout — the #3934 null-stdio fix
        // addresses one major cause of spawn failures, but any future
        // cause should still lock out rather than loop forever.
        let result: std::io::Result<std::process::ExitStatus> =
            Err(std::io::Error::new(std::io::ErrorKind::NotFound, "boom"));
        assert_eq!(
            classify_update_subprocess(&result),
            UpdateSubprocessOutcome::SpawnFailed
        );
    }

    #[test]
    fn update_counter_action_matches_outcome_semantics() {
        // Pin the glue between subprocess outcomes and the persistent
        // failure counter. A typo flipping any of these arms reopens
        // #3934 (exit-42 loop unbounded) or Codex's P1 regression
        // (transient network errors permanently disabling auto-update).
        //
        // This is the testing-reviewer-requested "glue test" that
        // directly exercises the action the wrapper takes. Both call
        // sites of `update_counter_action` in `service.rs` must agree
        // with this mapping.
        assert_eq!(
            update_counter_action(UpdateSubprocessOutcome::BinaryReplaced),
            UpdateCounterAction::Clear,
            "successful install must clear accumulated failures"
        );
        assert_eq!(
            update_counter_action(UpdateSubprocessOutcome::BundleUpdateStaged),
            UpdateCounterAction::Clear,
            "macOS DMG-swap commits to the update — clear failures"
        );
        assert_eq!(
            update_counter_action(UpdateSubprocessOutcome::AlreadyUpToDate),
            UpdateCounterAction::NoChange,
            "already-up-to-date is not an install attempt — don't touch counter"
        );
        assert_eq!(
            update_counter_action(UpdateSubprocessOutcome::SpawnFailed),
            UpdateCounterAction::Record,
            "spawn failure is environmental and persistent — must lock out"
        );
        assert_eq!(
            update_counter_action(UpdateSubprocessOutcome::OtherFailure),
            UpdateCounterAction::NoChange,
            "transient network/checksum errors must NOT accumulate (Codex P1)"
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

    #[test]
    fn embedded_updater_script_resolves_inside_freenet_crate() {
        // Regression for #4240 — the include_str! path used to walk
        // outside the crate, so cargo publish dropped the script and
        // `cargo install freenet` failed at build time. Re-included
        // here so non-macOS CI also exercises the path. The packaging
        // half is asserted in .github/workflows/ci.yml.
        const SCRIPT: &str = include_str!("../../../scripts/macos-bundle-updater.sh");
        assert!(
            SCRIPT.contains("macOS DMG-swap updater"),
            "include_str! resolved to the wrong file"
        );
    }

    // ---- Fail-closed checksum verification (auto-update security hardening). ----
    //
    // Before this change the Linux/Windows install path was fail-OPEN: a
    // missing SHA256SUMS.txt, or a manifest with no entry for the asset,
    // printed "Continuing without checksum verification" and installed the
    // binary anyway. These tests pin the new fail-CLOSED contract: no
    // trustworthy checksum -> refuse to install. The macOS DMG path already
    // had this strictness (`maybe_perform_bundle_update`).

    #[test]
    fn required_checksum_missing_manifest_is_fail_closed() {
        // SHA256SUMS.txt absent from the release, or its download failed,
        // surfaces as `checksums == None`. The gate must refuse rather
        // than proceed with an unverified binary.
        let err = required_checksum(None, "freenet-x86_64-unknown-linux-musl.tar.gz")
            .expect_err("a missing manifest must refuse to install (fail-closed)");
        assert!(
            err.to_string().contains("refusing to install"),
            "error must state the install was refused, got: {err}"
        );
    }

    #[test]
    fn required_checksum_missing_entry_is_fail_closed() {
        // Manifest present, but it lists no entry for our asset -> still
        // refuse. (The asset could have been published without its hash, or
        // the manifest could be for a different artifact set.)
        let manifest = Checksums::parse("abc123  some-other-asset.tar.gz\n");
        let err = required_checksum(Some(&manifest), "freenet-x86_64-unknown-linux-musl.tar.gz")
            .expect_err("a missing entry must refuse to install (fail-closed)");
        assert!(
            err.to_string().contains("refusing to install"),
            "error must state the install was refused, got: {err}"
        );
    }

    #[test]
    fn required_checksum_present_entry_returns_hash() {
        // Happy path: a listed entry resolves to its hash so verification
        // can proceed. Guards against the gate refusing valid installs.
        let manifest = Checksums::parse(
            "deadbeefdeadbeefdeadbeefdeadbeefdeadbeefdeadbeefdeadbeefdeadbeef  \
             freenet-x86_64-unknown-linux-musl.tar.gz\n",
        );
        assert_eq!(
            required_checksum(Some(&manifest), "freenet-x86_64-unknown-linux-musl.tar.gz").unwrap(),
            "deadbeefdeadbeefdeadbeefdeadbeefdeadbeefdeadbeefdeadbeefdeadbeef",
        );
    }

    #[test]
    fn verify_checksum_accepts_match_and_rejects_mismatch() {
        use std::io::Write as _;
        let dir = tempfile::tempdir().expect("create tempdir");
        let path = dir.path().join("artifact.bin");
        let contents = b"freenet release artifact";
        std::fs::File::create(&path)
            .unwrap()
            .write_all(contents)
            .unwrap();

        // Compute the correct digest the same way verify_checksum does.
        use sha2::{Digest, Sha256};
        let mut hasher = Sha256::new();
        hasher.update(contents);
        let correct = hasher.finalize().iter().fold(String::new(), |mut s, b| {
            use std::fmt::Write as _;
            write!(s, "{:02x}", b).unwrap();
            s
        });

        verify_checksum(&path, &correct).expect("matching checksum must pass");
        assert!(
            verify_checksum(&path, &"0".repeat(64)).is_err(),
            "a mismatched checksum must refuse (return Err), never silently pass"
        );
    }

    #[cfg(unix)]
    #[test]
    fn checksum_refusal_is_retryable_not_a_lockout() {
        // A fail-closed checksum refusal returns Err from
        // `download_and_install` BEFORE `replace_binary`, so
        // `record_update_failure` is never called and the process exits 1
        // (anyhow error in `main`). The supervisor classifies exit 1 as
        // OtherFailure -> NoChange: retry under backoff, NOT a
        // MAX_UPDATE_FAILURES lockout. This pins the retry-not-lockout
        // classification for the checksum-refusal path.
        let exit = Ok(exit_with(1));
        assert_eq!(
            classify_update_subprocess(&exit),
            UpdateSubprocessOutcome::OtherFailure,
            "a checksum refusal exits 1, which must classify as OtherFailure"
        );
        assert_eq!(
            update_counter_action(UpdateSubprocessOutcome::OtherFailure),
            UpdateCounterAction::NoChange,
            "OtherFailure must NOT increment the lockout counter (retry, don't lock out)"
        );
    }

    #[test]
    fn checksum_gate_precedes_install_and_records_no_failure() {
        // Structural pin for the retry-not-lockout contract that the pure
        // tests above can't reach. The fail-closed checksum gate
        // (`required_checksum` + `verify_checksum` on the freenet archive)
        // MUST run before `replace_binary` — the only site that calls
        // `record_update_failure` — and no `record_update_failure` may sit
        // between the gate and the install. If it did, a transiently
        // missing checksum would count toward MAX_UPDATE_FAILURES and
        // could permanently disable auto-update (the regression this PR
        // guards against). The repo uses source-scrape pins like this for
        // exactly this kind of ordering invariant (see
        // bug-prevention-patterns.md).
        const SRC: &str = include_str!("update.rs");

        let gate = SRC
            .find("required_checksum(checksums.as_ref(), &freenet_asset_name)")
            .expect("freenet checksum gate must exist in download_and_install");
        let verify = SRC
            .find("verify_checksum(&freenet_archive_path")
            .expect("freenet archive verify_checksum call must exist");
        let install = SRC
            .find("replace_binary(&extracted_freenet")
            .expect("replace_binary install call must exist");

        assert!(
            gate < verify && verify < install,
            "checksum gate + verify must precede the replace_binary install"
        );
        assert!(
            !SRC[gate..install].contains("record_update_failure"),
            "no record_update_failure may run between the checksum gate and \
             replace_binary — a checksum refusal must stay a retryable \
             OtherFailure, not a MAX_UPDATE_FAILURES lockout"
        );
    }

    // ---- Ed25519 release-manifest signing + verification (auto-update). ----
    //
    // The release workflow signs the raw bytes of SHA256SUMS.txt with the
    // private half of FREENET_RELEASE_PUBKEY (openssl, raw ed25519) and
    // uploads SHA256SUMS.txt.sig. The binary verifies that signature over the
    // exact manifest bytes before trusting any checksum inside it. These tests
    // pin: (a) the baked-in key matches the published hex, (b) the
    // CI-openssl-sign <-> binary-dalek-verify interop is real, and (c) the
    // fail-closed + transition policy of `verify_manifest_signature_with`.
    //
    // None of these tests use the real private key — they sign with throwaway
    // keys (openssl-generated or dalek `from_bytes` with a fixed test seed).

    /// A deterministic dalek signing key built from a fixed test seed. Never
    /// the real release key. Returns (signing_key, raw 32-byte verifying key).
    fn test_signing_key(seed: u8) -> (ed25519_dalek::SigningKey, [u8; 32]) {
        let sk = ed25519_dalek::SigningKey::from_bytes(&[seed; 32]);
        let vk = sk.verifying_key().to_bytes();
        (sk, vk)
    }

    fn dalek_sign(sk: &ed25519_dalek::SigningKey, msg: &[u8]) -> [u8; 64] {
        use ed25519_dalek::Signer;
        sk.sign(msg).to_bytes()
    }

    const SAMPLE_MANIFEST: &[u8] =
        b"abc123  freenet-x86_64-unknown-linux-musl.tar.gz\ndef456  fdev-x86_64-unknown-linux-musl.tar.gz\n";

    #[test]
    fn release_pubkey_matches_published_hex() {
        // Pins the baked-in key to the documented hex. A copy-paste slip in
        // the byte array would otherwise silently make every signature fail
        // (or, worse, trust the wrong key).
        let expected =
            hex::decode("eb2986604e34a3f985279a7e70852f3764d3347fe82074d92b1e4bc6336f8664")
                .unwrap();
        assert_eq!(FREENET_RELEASE_PUBKEY.as_slice(), expected.as_slice());
        // Must be a valid ed25519 point, or verification could never succeed.
        ed25519_dalek::VerifyingKey::from_bytes(&FREENET_RELEASE_PUBKEY)
            .expect("baked-in release public key must be a valid ed25519 point");
    }

    #[test]
    fn transition_flag_is_false_until_signed_floor_established() {
        // Tripwire for the two-release rollout. Flipping this to `true` makes
        // an ABSENT signature refuse the install, which bricks auto-update for
        // any node still updating from an unsigned older release. Only flip it
        // (and then update this test) once every release a live node could be
        // updating from publishes SHA256SUMS.txt.sig. See REQUIRE_RELEASE_SIGNATURE.
        assert!(
            !REQUIRE_RELEASE_SIGNATURE,
            "do not require signatures until the signed floor is established; \
             see the two-release transition note on REQUIRE_RELEASE_SIGNATURE"
        );
    }

    #[test]
    fn valid_signature_accepted() {
        let (sk, vk) = test_signing_key(7);
        let sig = dalek_sign(&sk, SAMPLE_MANIFEST);
        verify_manifest_signature_with(SAMPLE_MANIFEST, Some(&sig), &vk, false, true)
            .expect("a valid signature over the manifest must be accepted");
        // Also accepted when signatures are required.
        verify_manifest_signature_with(SAMPLE_MANIFEST, Some(&sig), &vk, true, true)
            .expect("a valid signature must be accepted even when required");
    }

    #[test]
    fn tampered_manifest_rejected() {
        let (sk, vk) = test_signing_key(7);
        let sig = dalek_sign(&sk, SAMPLE_MANIFEST);
        let mut tampered = SAMPLE_MANIFEST.to_vec();
        tampered[0] ^= 0x01; // flip one bit of the first checksum
        let err = verify_manifest_signature_with(&tampered, Some(&sig), &vk, false, true)
            .expect_err("a signature over different bytes must be refused (fail-closed)");
        assert!(
            err.to_string().contains("refusing to install"),
            "tamper rejection must refuse the install, got: {err}"
        );
    }

    #[test]
    fn wrong_key_signature_rejected() {
        // Signed by key A, verified against key B's public key.
        let (sk_a, _vk_a) = test_signing_key(1);
        let (_sk_b, vk_b) = test_signing_key(2);
        let sig = dalek_sign(&sk_a, SAMPLE_MANIFEST);
        let err = verify_manifest_signature_with(SAMPLE_MANIFEST, Some(&sig), &vk_b, false, true)
            .expect_err("a signature from an untrusted key must be refused (fail-closed)");
        assert!(err.to_string().contains("refusing to install"));
    }

    #[test]
    fn malformed_signature_length_rejected() {
        let (_sk, vk) = test_signing_key(3);
        let short = [0u8; 63];
        let err = verify_manifest_signature_with(SAMPLE_MANIFEST, Some(&short), &vk, false, true)
            .expect_err("a wrong-length signature must be refused");
        assert!(err.to_string().contains("wrong length"));
    }

    #[test]
    fn absent_signature_allowed_when_not_required() {
        // Transition behaviour: no .sig published -> allowed (with a warning).
        verify_manifest_signature_with(SAMPLE_MANIFEST, None, &FREENET_RELEASE_PUBKEY, false, true)
            .expect("absent signature must be allowed while not required");
    }

    #[test]
    fn absent_signature_refused_when_required() {
        // Post-transition behaviour: no .sig published -> refuse.
        let err = verify_manifest_signature_with(
            SAMPLE_MANIFEST,
            None,
            &FREENET_RELEASE_PUBKEY,
            true,
            true,
        )
        .expect_err("absent signature must be refused once signatures are required");
        assert!(err.to_string().contains("refusing to install"));
    }

    /// Committed openssl-produced fixture: a raw ed25519 signature generated by
    /// `openssl pkeyutl -sign -rawin` (the exact command CI uses) over a fixed
    /// message, with the raw public key extracted from the openssl DER SPKI.
    /// Verifying it through the binary's dalek path proves openssl <-> dalek
    /// interop CONCRETELY and HERMETICALLY — no openssl needed at test time, so
    /// CI always exercises the cross-implementation guarantee.
    ///
    /// Regenerate with:
    ///   openssl genpkey -algorithm ed25519 -out k.pem
    ///   printf 'freenet-release-interop-fixture-v1\n' > m.txt
    ///   openssl pkeyutl -sign -inkey k.pem -rawin -in m.txt -out m.sig
    ///   openssl pkey -in k.pem -pubout -outform DER | tail -c 32 | xxd -p -c64  # pubkey
    ///   xxd -p -c64 m.sig                                                       # signature
    #[test]
    fn openssl_fixture_verifies_via_dalek() {
        const FIXTURE_MSG: &[u8] = b"freenet-release-interop-fixture-v1\n";
        let pubkey: [u8; 32] =
            hex::decode("998007f01e8bd34d736017398822ce214bf78b81a2015e2e5e8de94d9c0cd2ae")
                .unwrap()
                .try_into()
                .unwrap();
        let sig =
            hex::decode("062a3ae0db73cc41fcd73f2cbb29eb0cf0cdd9420bb49db7d204ddec8803e81a5aa653b68c13105fd12ee996e8a9ba5eb553ec310133c6913756dbef437b1f0a")
                .unwrap();

        verify_manifest_signature_with(FIXTURE_MSG, Some(&sig), &pubkey, true, true)
            .expect("openssl-produced raw ed25519 signature must verify via the dalek path");

        // And the fixture must fail closed if the message is altered, proving
        // the verification is actually binding the openssl signature to the
        // message bytes (not trivially passing).
        let mut tampered = FIXTURE_MSG.to_vec();
        tampered.push(b'!');
        assert!(
            verify_manifest_signature_with(&tampered, Some(&sig), &pubkey, true, true).is_err(),
            "altering the fixture message must refuse (fail-closed)"
        );
    }

    /// Live interop: generate a throwaway ed25519 key with openssl, sign a
    /// sample SHA256SUMS the SAME way CI will (`pkeyutl -sign -rawin`), then
    /// verify the resulting 64-byte signature through the binary's exact dalek
    /// path. This is the proof the user most needs: CI-openssl-sign <->
    /// binary-dalek-verify actually interoperate on this machine.
    ///
    /// Skips (does not fail) only when openssl is absent or too old for raw
    /// ed25519 signing — the hermetic `openssl_fixture_verifies_via_dalek`
    /// test still guarantees the interop in that case.
    #[test]
    fn interop_openssl_sign_dalek_verify() {
        use std::process::Command;

        // Environmental availability check: a missing/old openssl is a skip,
        // not a failure (the committed fixture covers interop unconditionally).
        let openssl_ok = Command::new("openssl")
            .arg("version")
            .output()
            .ok()
            .map(|o| o.status.success())
            .unwrap_or(false);
        if !openssl_ok {
            eprintln!("skipping interop_openssl_sign_dalek_verify: openssl not available");
            return;
        }

        let dir = tempfile::tempdir().expect("tempdir");
        let key_pem = dir.path().join("key.pem");
        let msg = dir.path().join("SHA256SUMS.txt");
        let sig = dir.path().join("SHA256SUMS.txt.sig");
        let pub_der = dir.path().join("pub.der");

        std::fs::write(&msg, SAMPLE_MANIFEST).unwrap();

        let genkey = Command::new("openssl")
            .args(["genpkey", "-algorithm", "ed25519", "-out"])
            .arg(&key_pem)
            .output()
            .expect("spawn openssl genpkey");
        if !genkey.status.success() {
            eprintln!(
                "skipping interop_openssl_sign_dalek_verify: openssl genpkey ed25519 unsupported"
            );
            return;
        }

        // The exact signing invocation the CI workflow uses.
        let sign = Command::new("openssl")
            .arg("pkeyutl")
            .arg("-sign")
            .arg("-inkey")
            .arg(&key_pem)
            .arg("-rawin")
            .arg("-in")
            .arg(&msg)
            .arg("-out")
            .arg(&sig)
            .output()
            .expect("spawn openssl pkeyutl sign");
        if !sign.status.success() {
            eprintln!(
                "skipping interop_openssl_sign_dalek_verify: openssl raw ed25519 sign unsupported \
                 (needs openssl >= 3.0)"
            );
            return;
        }

        // Extract the raw 32-byte public key: the ed25519 DER SubjectPublicKeyInfo
        // is a fixed 12-byte prefix followed by the 32-byte key.
        let pubout = Command::new("openssl")
            .args(["pkey", "-pubout", "-outform", "DER", "-in"])
            .arg(&key_pem)
            .arg("-out")
            .arg(&pub_der)
            .output()
            .expect("spawn openssl pkey pubout");
        assert!(pubout.status.success(), "openssl pkey -pubout must succeed");

        let der = std::fs::read(&pub_der).unwrap();
        assert!(der.len() >= 32, "DER SPKI must contain a 32-byte key");
        let pubkey: [u8; 32] = der[der.len() - 32..].try_into().unwrap();

        let sig_bytes = std::fs::read(&sig).unwrap();
        assert_eq!(
            sig_bytes.len(),
            64,
            "openssl raw ed25519 signature must be exactly 64 bytes"
        );

        // The real interop assertion: openssl-signed, dalek-verified.
        verify_manifest_signature_with(SAMPLE_MANIFEST, Some(&sig_bytes), &pubkey, true, true)
            .expect("openssl-signed manifest must verify through the binary's dalek path");

        // Fail-closed under tampering, end to end.
        let mut tampered = SAMPLE_MANIFEST.to_vec();
        tampered[0] ^= 0x01;
        assert!(
            verify_manifest_signature_with(&tampered, Some(&sig_bytes), &pubkey, true, true)
                .is_err(),
            "a tampered manifest must be refused even with a valid openssl signature"
        );
    }
}
