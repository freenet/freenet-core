use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime};

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

/// TTL for the [`ServiceHealthCache`]. The release workflow polls `/version`
/// every 5s; a 2s TTL means at most one `systemctl` fork per poll while still
/// being far fresher than the poll cadence, so the workflow never acts on a
/// stale "active" reading for more than one cycle. The cache exists for the
/// same reason as [`VersionCache`]: `/version` is UNAUTHENTICATED, so forking a
/// subprocess on every hit is a cheap self-DoS vector.
const SERVICE_HEALTH_TTL: Duration = Duration::from_secs(2);

/// Caches the result of `systemctl is-active <service>` for a short TTL.
///
/// The `/version` endpoint reports the ON-DISK binary version, which is NOT the
/// same question as "is the gateway actually running it": during the v0.2.71
/// release the vega gateway's binary was swapped to 0.2.71 but the service
/// failed to restart (old process hung on shutdown → SIGKILL → unit left
/// `failed`). `/version` still reported 0.2.71, so the release workflow reported
/// success while the gateway was DOWN for ~5 minutes. Reporting the service
/// state lets the workflow tell those two cases apart.
///
/// # Why a TTL cache
///
/// `/version` is unauthenticated. Without caching, every request would fork
/// `systemctl` — the exact self-DoS vector [`VersionCache`] was built to
/// eliminate for `freenet --version`. The TTL ([`SERVICE_HEALTH_TTL`]) is short
/// relative to the workflow's 5s poll so a freshly-restarted (or freshly-failed)
/// service is reflected within one poll cycle.
///
/// # Reliance on the update-script ordering
///
/// The workflow treats `installed == VERSION && service_active == true` as a
/// successful update. That is only sound because `deploy-local-gateway.sh`
/// (what `/update` ultimately spawns via `gateway-auto-update.sh`) performs
/// **stop → swap binary → start**: the service is DOWN while the on-disk binary
/// is replaced, so the binary only reads as "new" once the service has been
/// restarted onto it. There is therefore no window where the OLD process is
/// still `active` while the binary already reads as new. If that script is ever
/// changed to swap-then-restart, this check would need to additionally verify
/// the running process is the new binary (e.g. via `ActiveEnterTimestamp`); see
/// the comment in `.github/workflows/gateway-update.yml`.
#[derive(Default, Clone)]
pub struct ServiceHealthCache {
    inner: Arc<Mutex<Option<CachedServiceHealth>>>,
}

struct CachedServiceHealth {
    service: String,
    active: bool,
    checked_at: Instant,
}

impl ServiceHealthCache {
    pub fn new() -> Self {
        Self::default()
    }

    /// Return whether `service` is `active`, using the cached value if it is
    /// younger than [`SERVICE_HEALTH_TTL`] and for the same service name.
    /// Resolves `systemctl` via PATH.
    pub async fn is_active(&self, service: &str) -> bool {
        self.is_active_with(Path::new("systemctl"), service).await
    }

    /// [`Self::is_active`] with the `systemctl` binary path injected so tests
    /// can point at a stub without real systemd. Production callers use
    /// [`Self::is_active`].
    pub async fn is_active_with(&self, systemctl: &Path, service: &str) -> bool {
        {
            let guard = self.inner.lock().await;
            if let Some(c) = guard.as_ref() {
                if c.service == service && c.checked_at.elapsed() < SERVICE_HEALTH_TTL {
                    return c.active;
                }
            }
        }

        let active = query_service_active(systemctl, service).await;
        let mut guard = self.inner.lock().await;
        *guard = Some(CachedServiceHealth {
            service: service.to_string(),
            active,
            checked_at: Instant::now(),
        });
        active
    }
}

/// Run `systemctl is-active <service>` once and interpret the result.
///
/// Returns `true` only when systemd reports the unit `active`. `inactive`,
/// `failed`, `activating`, an unknown unit, or a missing/erroring `systemctl`
/// all map to `false` — never an error and never a panic. `systemctl is-active`
/// is a read-only query that does not require root, so no sudo is involved.
///
/// Note: `is-active` exits non-zero for any non-active state, so we key on the
/// printed state word (stdout) rather than the exit status.
async fn query_service_active(systemctl: &Path, service: &str) -> bool {
    let unit = if service.ends_with(".service") {
        service.to_string()
    } else {
        format!("{service}.service")
    };

    let output = match tokio::time::timeout(
        Duration::from_secs(5),
        Command::new(systemctl).arg("is-active").arg(&unit).output(),
    )
    .await
    {
        Ok(Ok(output)) => output,
        Ok(Err(e)) => {
            // systemctl missing (non-systemd host) or otherwise unspawnable.
            tracing::warn!(error = %e, unit, "could not spawn `systemctl is-active`");
            return false;
        }
        Err(_) => {
            tracing::warn!(unit, "`systemctl is-active` timed out");
            return false;
        }
    };

    // `is-active` prints exactly the active state on stdout (e.g. `active`,
    // `inactive`, `failed`, `activating`). Match `active` precisely so
    // `activating`/`inactive` are not counted as up.
    let state = String::from_utf8_lossy(&output.stdout);
    state.trim() == "active"
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

    /// Write a stub `systemctl` that prints `state` on stdout and exits 0 for
    /// `active`, 3 otherwise — matching real `systemctl is-active` behaviour
    /// (non-active states exit non-zero but still print the state word). Each
    /// invocation also appends a line to `counter_path` so a test can assert
    /// how many times the stub was forked (cache coverage).
    fn write_stub_systemctl_counting(dir: &Path, state: &str, counter_path: &Path) -> PathBuf {
        let bin = dir.join("fake-systemctl");
        let exit = if state == "active" { 0 } else { 3 };
        let script = format!(
            "#!/bin/sh\necho x >> {counter}\necho {state}\nexit {exit}\n",
            counter = counter_path.display()
        );
        let mut f = std::fs::File::create(&bin).unwrap();
        f.write_all(script.as_bytes()).unwrap();
        let mut perms = std::fs::metadata(&bin).unwrap().permissions();
        perms.set_mode(0o755);
        std::fs::set_permissions(&bin, perms).unwrap();
        bin
    }

    /// Convenience: stub `systemctl` without a fork counter.
    fn write_stub_systemctl(dir: &Path, state: &str) -> PathBuf {
        write_stub_systemctl_counting(dir, state, &dir.join("ignored-count"))
    }

    #[tokio::test]
    async fn service_active_true_when_systemctl_reports_active() {
        let dir = tempfile::tempdir().unwrap();
        let systemctl = write_stub_systemctl(dir.path(), "active");
        assert!(query_service_active(&systemctl, "freenet-gateway").await);
    }

    #[tokio::test]
    async fn service_active_false_when_failed() {
        // The vega v0.2.71 case: binary swapped, but the unit is `failed`.
        let dir = tempfile::tempdir().unwrap();
        let systemctl = write_stub_systemctl(dir.path(), "failed");
        assert!(!query_service_active(&systemctl, "freenet-gateway").await);
    }

    #[tokio::test]
    async fn service_active_false_when_inactive() {
        let dir = tempfile::tempdir().unwrap();
        let systemctl = write_stub_systemctl(dir.path(), "inactive");
        assert!(!query_service_active(&systemctl, "freenet-gateway").await);
    }

    #[tokio::test]
    async fn service_active_false_when_activating() {
        // `activating` is not yet up; must not be counted as active.
        let dir = tempfile::tempdir().unwrap();
        let systemctl = write_stub_systemctl(dir.path(), "activating");
        assert!(!query_service_active(&systemctl, "freenet-gateway").await);
    }

    #[tokio::test]
    async fn service_active_false_when_systemctl_missing() {
        // Non-systemd host (or systemctl not installed): never panic, never
        // error — just report the service as not active.
        let dir = tempfile::tempdir().unwrap();
        let missing = dir.path().join("does-not-exist-systemctl");
        assert!(!query_service_active(&missing, "freenet-gateway").await);
    }

    #[tokio::test]
    async fn service_health_cache_does_not_refork_within_ttl() {
        // The unauthenticated /version endpoint must NOT fork systemctl on
        // every hit (self-DoS vector). A second lookup within the TTL must be
        // served from cache, so the stub is forked exactly once.
        let dir = tempfile::tempdir().unwrap();
        let counter = dir.path().join("count");
        let systemctl = write_stub_systemctl_counting(dir.path(), "active", &counter);

        let cache = ServiceHealthCache::new();
        assert!(cache.is_active_with(&systemctl, "freenet-gateway").await);
        assert!(cache.is_active_with(&systemctl, "freenet-gateway").await);

        let forks = std::fs::read_to_string(&counter).unwrap_or_default();
        assert_eq!(
            forks.lines().count(),
            1,
            "second is_active within TTL must hit the cache, not refork systemctl"
        );
    }

    #[tokio::test]
    async fn service_health_cache_reforks_for_different_service() {
        // A different service name is a different question — must not be served
        // from a cache entry keyed on the previous name.
        let dir = tempfile::tempdir().unwrap();
        let counter = dir.path().join("count");
        let systemctl = write_stub_systemctl_counting(dir.path(), "active", &counter);

        let cache = ServiceHealthCache::new();
        cache.is_active_with(&systemctl, "freenet-gateway").await;
        cache
            .is_active_with(&systemctl, "freenet-gateway-hector")
            .await;

        let forks = std::fs::read_to_string(&counter).unwrap_or_default();
        assert_eq!(
            forks.lines().count(),
            2,
            "different service name must re-query, not reuse the cached entry"
        );
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
