use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, SystemTime};

use anyhow::{Context, Result};
use semver::Version;
use tokio::process::Command;
use tokio::sync::Mutex;

/// Cached `freenet --version` result. Without caching the agent would fork
/// the freenet binary on every request, which made `GET /version` a cheap
/// self-DoS vector and added latency on the hot path of `POST /update`.
///
/// Cache key includes mtime AND size: an atomic-rename install that uses
/// `cp -p` would preserve mtime, and an mtime-only key would return the
/// stale pre-upgrade version forever. The combination of (path, mtime,
/// size) is the cheapest correctness-preserving signature available
/// without re-reading the file contents.
#[derive(Default, Clone)]
pub struct VersionCache {
    inner: Arc<Mutex<Option<CachedVersion>>>,
}

struct CachedVersion {
    path: PathBuf,
    mtime: SystemTime,
    size: u64,
    version: Version,
}

impl VersionCache {
    pub fn new() -> Self {
        Self::default()
    }

    pub async fn current(&self, binary_path: &Path) -> Result<Version> {
        let meta = tokio::fs::metadata(binary_path)
            .await
            .with_context(|| format!("stat {}", binary_path.display()))?;
        let mtime = meta
            .modified()
            .context("binary mtime not available on this filesystem")?;
        let size = meta.len();

        {
            let guard = self.inner.lock().await;
            if let Some(c) = guard.as_ref() {
                if c.path == binary_path && c.mtime == mtime && c.size == size {
                    return Ok(c.version.clone());
                }
            }
        }

        let version = read_version(binary_path).await?;
        let mut guard = self.inner.lock().await;
        *guard = Some(CachedVersion {
            path: binary_path.to_path_buf(),
            mtime,
            size,
            version: version.clone(),
        });
        Ok(version)
    }
}

async fn read_version(binary_path: &Path) -> Result<Version> {
    let output = tokio::time::timeout(
        Duration::from_secs(5),
        Command::new(binary_path).arg("--version").output(),
    )
    .await
    .context("freenet --version timed out")?
    .with_context(|| format!("failed to spawn {}", binary_path.display()))?;

    if !output.status.success() {
        anyhow::bail!(
            "freenet --version exited {}: stderr={}",
            output.status,
            String::from_utf8_lossy(&output.stderr)
        );
    }

    parse_version_output(&String::from_utf8_lossy(&output.stdout))
}

fn parse_version_output(out: &str) -> Result<Version> {
    out.split_whitespace()
        .find_map(|t| Version::parse(t.trim_start_matches('v')).ok())
        .with_context(|| format!("could not parse version from {out:?}"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;
    use std::os::unix::fs::PermissionsExt;

    #[test]
    fn parses_plain_output() {
        let v = parse_version_output("freenet 0.2.56\n").unwrap();
        assert_eq!(v, Version::new(0, 2, 56));
    }

    #[test]
    fn parses_v_prefixed_output() {
        let v = parse_version_output("freenet v0.2.56").unwrap();
        assert_eq!(v, Version::new(0, 2, 56));
    }

    #[test]
    fn parses_pre_release() {
        let v = parse_version_output("freenet 0.2.56-rc1\n").unwrap();
        assert_eq!(v.major, 0);
        assert_eq!(v.minor, 2);
        assert_eq!(v.patch, 56);
        assert!(!v.pre.is_empty());
    }

    #[test]
    fn rejects_garbage() {
        assert!(parse_version_output("no version here").is_err());
    }

    #[tokio::test]
    async fn cache_returns_same_result_without_respawn() {
        // Stub binary that records each invocation by appending to a
        // counter file. Cache should mean second call doesn't increment.
        let dir = tempfile::tempdir().unwrap();
        let counter_path = dir.path().join("count");
        let bin_path = dir.path().join("fake-freenet");
        {
            let mut f = std::fs::File::create(&bin_path).unwrap();
            writeln!(
                f,
                "#!/bin/sh\necho stub-tool 0.2.56\necho x >> {}",
                counter_path.display()
            )
            .unwrap();
        }
        let mut perms = std::fs::metadata(&bin_path).unwrap().permissions();
        perms.set_mode(0o755);
        std::fs::set_permissions(&bin_path, perms).unwrap();

        let cache = VersionCache::new();
        let v1 = cache.current(&bin_path).await.unwrap();
        let v2 = cache.current(&bin_path).await.unwrap();
        assert_eq!(v1, Version::new(0, 2, 56));
        assert_eq!(v1, v2);

        let invocations = std::fs::read_to_string(&counter_path).unwrap_or_default();
        assert_eq!(
            invocations.lines().count(),
            1,
            "second call should hit the cache, not respawn"
        );
    }

    #[tokio::test]
    async fn cache_invalidates_when_binary_changes() {
        // Simulates the post-update path: the agent's `/version` endpoint
        // must reflect the new binary immediately. mtime alone isn't
        // sufficient (an atomic-rename with `cp -p` would preserve it);
        // size is the cheap belt-and-braces.
        let dir = tempfile::tempdir().unwrap();
        let bin_path = dir.path().join("fake-freenet");

        let write_stub = |contents: &str| {
            let mut f = std::fs::File::create(&bin_path).unwrap();
            writeln!(f, "{contents}").unwrap();
            let mut perms = std::fs::metadata(&bin_path).unwrap().permissions();
            perms.set_mode(0o755);
            std::fs::set_permissions(&bin_path, perms).unwrap();
        };

        write_stub("#!/bin/sh\necho stub-tool 0.2.55");
        let cache = VersionCache::new();
        let v1 = cache.current(&bin_path).await.unwrap();
        assert_eq!(v1, Version::new(0, 2, 55));

        // Swap the binary with a forced size change AND pin the old mtime
        // back onto the new file. Even with identical mtime the size
        // difference must invalidate the cache.
        let old_mtime = std::fs::metadata(&bin_path).unwrap().modified().unwrap();
        write_stub("#!/bin/sh\necho stub-tool 0.2.56  # padding to change size");
        let new_size_handle = std::fs::OpenOptions::new()
            .write(true)
            .open(&bin_path)
            .unwrap();
        new_size_handle.set_modified(old_mtime).unwrap();
        drop(new_size_handle);

        let v2 = cache.current(&bin_path).await.unwrap();
        assert_eq!(
            v2,
            Version::new(0, 2, 56),
            "cache must re-stat when binary content changes, even if mtime is preserved"
        );
    }
}
