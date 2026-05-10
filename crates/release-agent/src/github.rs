use std::time::Duration;

use anyhow::{Context, Result};
use reqwest::Client;
use semver::Version;
use serde::Deserialize;

#[derive(Deserialize)]
struct ReleaseInfo {
    tag_name: String,
}

pub async fn fetch_latest_version(client: &Client, repo: &str) -> Result<Version> {
    let url = format!("https://api.github.com/repos/{repo}/releases/latest");
    let info: ReleaseInfo = client
        .get(&url)
        .header("User-Agent", "freenet-release-agent")
        .header("Accept", "application/vnd.github+json")
        .timeout(Duration::from_secs(10))
        .send()
        .await
        .with_context(|| format!("GET {url}"))?
        .error_for_status()
        .with_context(|| format!("non-2xx from {url}"))?
        .json()
        .await
        .context("decode release JSON")?;

    let tag = info.tag_name.strip_prefix('v').unwrap_or(&info.tag_name);
    Version::parse(tag).with_context(|| format!("parse tag {tag:?} as semver"))
}
