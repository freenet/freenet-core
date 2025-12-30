//! Self-update functionality for Freenet binary.

use anyhow::{Context, Result};
use clap::Args;
use semver::Version;
use std::fs::{self, File};
use std::io::{self, Write};
use std::path::{Path, PathBuf};
use std::process::Command;

const GITHUB_API_URL: &str = "https://api.github.com/repos/freenet/freenet-core/releases/latest";

#[derive(Args, Debug, Clone)]
pub struct UpdateCommand {
    /// Only check if an update is available without installing
    #[arg(long)]
    pub check: bool,

    /// Force update even if already on latest version
    #[arg(long)]
    pub force: bool,
}

impl UpdateCommand {
    pub fn run(&self, current_version: &str) -> Result<()> {
        let rt = tokio::runtime::Runtime::new()?;
        rt.block_on(self.run_async(current_version))
    }

    async fn run_async(&self, current_version: &str) -> Result<()> {
        println!("Current version: {}", current_version);
        println!("Checking for updates...");

        let latest = get_latest_release().await?;

        let latest_version = latest.tag_name.trim_start_matches('v');
        println!("Latest version: {}", latest_version);

        // Use semver for proper version comparison
        let current_ver =
            Version::parse(current_version).context("Failed to parse current version as semver")?;
        let latest_ver =
            Version::parse(latest_version).context("Failed to parse latest version as semver")?;

        if !self.force && latest_ver <= current_ver {
            println!("You are already running the latest version.");
            return Ok(());
        }

        if self.check {
            if latest_ver > current_ver {
                println!(
                    "Update available: {} -> {}",
                    current_version, latest_version
                );
            }
            return Ok(());
        }

        println!("Downloading update...");
        self.download_and_install(&latest).await
    }

    async fn download_and_install(&self, release: &Release) -> Result<()> {
        let target = get_target_triple();
        let extension = get_archive_extension();
        let asset_name = format!("freenet-{}.{}", target, extension);

        let asset = release
            .assets
            .iter()
            .find(|a| a.name == asset_name)
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

        // Try to get checksums
        let checksums = if let Some(checksums_asset) =
            release.assets.iter().find(|a| a.name == "SHA256SUMS.txt")
        {
            println!("Downloading checksums...");
            match download_checksums(&checksums_asset.browser_download_url).await {
                Ok(c) => Some(c),
                Err(e) => {
                    eprintln!("Warning: Failed to download checksums: {}. Continuing without verification.", e);
                    None
                }
            }
        } else {
            eprintln!("Warning: SHA256SUMS.txt not found in release. Continuing without checksum verification.");
            None
        };

        // Download to temp file
        let temp_dir = tempfile::tempdir().context("Failed to create temp directory")?;
        let archive_path = temp_dir.path().join(&asset_name);

        download_file(&asset.browser_download_url, &archive_path).await?;

        // Verify checksum if available
        if let Some(ref checksums) = checksums {
            if let Some(expected_hash) = checksums.get(&asset_name) {
                println!("Verifying checksum...");
                verify_checksum(&archive_path, expected_hash)?;
            } else {
                eprintln!(
                    "Warning: Checksum not found for {}. Continuing without verification.",
                    asset_name
                );
            }
        }

        // Extract archive
        let extracted_binary = extract_binary(&archive_path, temp_dir.path())?;

        // Replace current binary
        let current_exe = std::env::current_exe().context("Failed to get current executable")?;
        replace_binary(&extracted_binary, &current_exe)?;

        println!(
            "Successfully updated to version {}",
            release.tag_name.trim_start_matches('v')
        );

        // Automatically restart service if running
        #[cfg(target_os = "linux")]
        {
            if is_systemd_service_active() {
                println!("Restarting Freenet service...");
                let status = Command::new("systemctl")
                    .args(["--user", "restart", "freenet"])
                    .status();
                match status {
                    Ok(s) if s.success() => println!("Service restarted successfully."),
                    Ok(_) => eprintln!("Warning: Failed to restart service. Run 'freenet service restart' manually."),
                    Err(e) => eprintln!("Warning: Failed to restart service: {}. Run 'freenet service restart' manually.", e),
                }
            }
        }

        #[cfg(target_os = "macos")]
        {
            if is_launchd_service_active() {
                println!("Restarting Freenet service...");
                // launchctl doesn't have a restart command, so stop + start
                let _ = Command::new("launchctl")
                    .args(["stop", "org.freenet.node"])
                    .status();
                let status = Command::new("launchctl")
                    .args(["start", "org.freenet.node"])
                    .status();
                match status {
                    Ok(s) if s.success() => println!("Service restarted successfully."),
                    Ok(_) => eprintln!("Warning: Failed to restart service. Run 'freenet service restart' manually."),
                    Err(e) => eprintln!("Warning: Failed to restart service: {}. Run 'freenet service restart' manually.", e),
                }
            }
        }

        Ok(())
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

    let mut file = File::open(file_path).context("Failed to open file for checksum")?;
    let mut hasher = Sha256::new();
    std::io::copy(&mut file, &mut hasher).context("Failed to read file for checksum")?;
    let result = hasher.finalize();
    let actual_hash = format!("{:x}", result);

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

async fn download_file(url: &str, dest: &Path) -> Result<()> {
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

        if total_size > 0 {
            let progress = (downloaded as f64 / total_size as f64 * 100.0) as u32;
            // Use ANSI escape to clear to end of line for clean output
            print!("\rDownloading... {}%\x1b[K", progress);
            io::stdout().flush()?;
        }
    }

    println!("\rDownload complete.\x1b[K");
    Ok(())
}

fn extract_binary(archive_path: &Path, dest_dir: &Path) -> Result<PathBuf> {
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
    let binary_name = "freenet.exe";
    #[cfg(not(target_os = "windows"))]
    let binary_name = "freenet";

    let binary_path = dest_dir.join(binary_name);
    if !binary_path.exists() {
        anyhow::bail!("Binary not found in archive");
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

fn replace_binary(new_binary: &Path, current_exe: &Path) -> Result<()> {
    // On Unix, we can't directly replace a running binary, but we can rename it
    let backup_path = current_exe.with_extension("old");
    let parent_dir = current_exe
        .parent()
        .context("Current executable has no parent directory")?;

    // Remove old backup if it exists
    if backup_path.exists() {
        fs::remove_file(&backup_path).context("Failed to remove old backup")?;
    }

    // First, copy new binary to a temp location in the same directory
    // This ensures the rename will be atomic (same filesystem)
    let temp_new = parent_dir.join(".freenet.new.tmp");
    fs::copy(new_binary, &temp_new).context("Failed to copy new binary to target directory")?;

    // Set executable permissions on temp file
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let mut perms = fs::metadata(&temp_new)?.permissions();
        perms.set_mode(0o755);
        fs::set_permissions(&temp_new, perms)?;
    }

    // Rename current to backup (atomic on Unix)
    fs::rename(current_exe, &backup_path)
        .context("Failed to backup current binary. You may need to run with sudo.")?;

    // Rename temp to current (atomic on Unix)
    if let Err(e) = fs::rename(&temp_new, current_exe) {
        // Try to restore backup
        if let Err(restore_err) = fs::rename(&backup_path, current_exe) {
            // Critical: rollback failed, system may be in broken state
            eprintln!(
                "CRITICAL: Failed to restore backup after update failure. \
                 Original binary may be at: {}",
                backup_path.display()
            );
            eprintln!("Restore error: {}", restore_err);
        }
        // Clean up temp file
        let _ = fs::remove_file(&temp_new);
        return Err(e).context("Failed to install new binary");
    }

    // Remove backup on success
    if let Err(e) = fs::remove_file(&backup_path) {
        // Non-fatal: warn but don't fail
        eprintln!(
            "Warning: Failed to remove backup file {}: {}",
            backup_path.display(),
            e
        );
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
