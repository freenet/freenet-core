use anyhow::{Context, Result};
use std::time::Duration;

pub const GITHUB_API_URL: &str =
    "https://api.github.com/repos/freenet/freenet-core/releases/latest";

#[derive(serde::Deserialize, Debug)]
pub struct Release {
    pub tag_name: String,
    pub assets: Vec<Asset>,
}

#[derive(serde::Deserialize, Debug)]
pub struct Asset {
    pub name: String,
    pub browser_download_url: String,
}

pub async fn get_latest_release() -> Result<Release> {
    let client = reqwest::Client::builder()
        .user_agent("freenet-updater")
        .timeout(Duration::from_secs(10))
        .build()?;

    let response = client
        .get(GITHUB_API_URL)
        .send()
        .await
        .context("Failed to fetch release info from GitHub")?;

    if !response.status().is_success() {
        let status = response.status();
        let text = response.text().await.unwrap_or_default();
        let error_msg = if text.len() > 200 {
            format!("{} (body truncated): {}...", status, &text[..200])
        } else {
            format!("{}: {}", status, text)
        };
        anyhow::bail!("GitHub API returned error: {}", error_msg);
    }

    response
        .json::<Release>()
        .await
        .context("Failed to parse release info from GitHub")
}
