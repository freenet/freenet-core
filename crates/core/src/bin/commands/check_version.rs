use anyhow::{Context, Result};
use clap::Args;
use semver::Version;

const GITHUB_API_URL: &str = "https://api.github.com/repos/freenet/freenet-core/releases/latest";

#[derive(Args, Debug, Clone)]
pub struct CheckVersionCommand {}

impl CheckVersionCommand {
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

        let current_ver = Version::parse(current_version)
            .context("Failed to parse current version as semver")?;
        let latest_ver = Version::parse(latest_version)
            .context("Failed to parse latest version as semver")?;

        if latest_ver > current_ver {
            println!(
                "Update available: {} -> {}",
                current_version, latest_version
            );
            println!("Run 'freenet update' to install the latest version.");
        } else {
            println!("You are already running the latest version.");
        }

        Ok(())
    }
}

#[derive(serde::Deserialize, Debug)]
struct Release {
    tag_name: String,
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
