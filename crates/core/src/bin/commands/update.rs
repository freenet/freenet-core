//! Self-update functionality for Freenet binary.

use anyhow::{Context, Result};
use clap::Args;
use std::fs::{self, File};
use std::io::{self, Write};
use std::path::{Path, PathBuf};

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

        if !self.force && latest_version == current_version {
            println!("You are already running the latest version.");
            return Ok(());
        }

        if self.check {
            if latest_version != current_version {
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
        let asset_name = format!("freenet-{}.tar.gz", target);

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

        // Download to temp file
        let temp_dir = tempfile::tempdir().context("Failed to create temp directory")?;
        let archive_path = temp_dir.path().join(&asset_name);

        download_file(&asset.browser_download_url, &archive_path).await?;

        // Extract archive
        let extracted_binary = extract_binary(&archive_path, temp_dir.path())?;

        // Replace current binary
        let current_exe = std::env::current_exe().context("Failed to get current executable")?;
        replace_binary(&extracted_binary, &current_exe)?;

        println!(
            "Successfully updated to version {}",
            release.tag_name.trim_start_matches('v')
        );

        // Check if service is running and offer to restart
        #[cfg(target_os = "linux")]
        {
            if is_systemd_service_active() {
                println!();
                println!("The Freenet service is running. Restart it to use the new version:");
                println!("  freenet service restart");
            }
        }

        #[cfg(target_os = "macos")]
        {
            if is_launchd_service_active() {
                println!();
                println!("The Freenet service is running. Restart it to use the new version:");
                println!("  freenet service restart");
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

fn get_target_triple() -> &'static str {
    #[cfg(all(target_arch = "x86_64", target_os = "linux"))]
    {
        "x86_64-unknown-linux-gnu"
    }
    #[cfg(all(target_arch = "aarch64", target_os = "linux"))]
    {
        "aarch64-unknown-linux-gnu"
    }
    #[cfg(all(target_arch = "x86_64", target_os = "macos"))]
    {
        "x86_64-apple-darwin"
    }
    #[cfg(all(target_arch = "aarch64", target_os = "macos"))]
    {
        "aarch64-apple-darwin"
    }
    #[cfg(not(any(
        all(target_arch = "x86_64", target_os = "linux"),
        all(target_arch = "aarch64", target_os = "linux"),
        all(target_arch = "x86_64", target_os = "macos"),
        all(target_arch = "aarch64", target_os = "macos"),
    )))]
    {
        "unknown"
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
            print!("\rDownloading... {}%", progress);
            io::stdout().flush()?;
        }
    }

    println!("\rDownload complete.     ");
    Ok(())
}

fn extract_binary(archive_path: &Path, dest_dir: &Path) -> Result<PathBuf> {
    let file = File::open(archive_path).context("Failed to open archive")?;
    let decoder = flate2::read::GzDecoder::new(file);
    let mut archive = tar::Archive::new(decoder);

    archive
        .unpack(dest_dir)
        .context("Failed to extract archive")?;

    let binary_path = dest_dir.join("freenet");
    if !binary_path.exists() {
        anyhow::bail!("Binary not found in archive");
    }

    Ok(binary_path)
}

fn replace_binary(new_binary: &Path, current_exe: &Path) -> Result<()> {
    // On Unix, we can't directly replace a running binary, but we can rename it
    let backup_path = current_exe.with_extension("old");

    // Remove old backup if it exists
    let _ = fs::remove_file(&backup_path);

    // Rename current to backup
    fs::rename(current_exe, &backup_path)
        .context("Failed to backup current binary. You may need to run with sudo.")?;

    // Copy new binary to location
    if let Err(e) = fs::copy(new_binary, current_exe) {
        // Try to restore backup
        let _ = fs::rename(&backup_path, current_exe);
        return Err(e).context("Failed to install new binary");
    }

    // Set executable permissions
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let mut perms = fs::metadata(current_exe)?.permissions();
        perms.set_mode(0o755);
        fs::set_permissions(current_exe, perms)?;
    }

    // Remove backup
    let _ = fs::remove_file(&backup_path);

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
