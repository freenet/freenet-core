use std::time::Duration;

use anyhow::{Context, Result};
use reqwest::Client;
use semver::Version;
use serde::Deserialize;

/// Source of truth for "the latest released version on GitHub". Stored on
/// `AppState` as `Arc<dyn LatestSource>` so integration tests can inject a
/// fake without making real network calls — and so the security-critical
/// `target == latest` check in `update_handler` is automatically tested.
#[async_trait::async_trait]
pub trait LatestSource: Send + Sync {
    async fn fetch_latest(&self) -> Result<Version>;
}

/// Live source backed by `GET /repos/{repo}/releases/latest`.
///
/// **Caveat**: GitHub's `/releases/latest` endpoint excludes pre-releases
/// (releases marked with `prerelease=true`). A request for an `-rc1` /
/// `-beta` tag will be refused by `update_handler` even if such a release
/// exists, because it will never equal `latest`. This is intentional — the
/// release agent is for stable rollouts only.
pub struct GitHubLatest {
    pub client: Client,
    pub repo: String,
}

#[derive(Deserialize)]
struct ReleaseInfo {
    tag_name: String,
}

#[async_trait::async_trait]
impl LatestSource for GitHubLatest {
    async fn fetch_latest(&self) -> Result<Version> {
        let url = format!("https://api.github.com/repos/{}/releases/latest", self.repo);
        let info: ReleaseInfo = self
            .client
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
}

/// Test double — returns a pinned Version. Lives outside `#[cfg(test)]` so
/// integration tests under `tests/` (which compile as separate crates) can
/// also use it.
#[doc(hidden)]
pub struct StaticLatest(pub Version);

#[async_trait::async_trait]
impl LatestSource for StaticLatest {
    async fn fetch_latest(&self) -> Result<Version> {
        Ok(self.0.clone())
    }
}
