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
///
/// # Concurrency
///
/// Concurrent cache-misses each fork `systemctl` before the cache fills (no
/// single-flight). This matches [`VersionCache`]'s accepted tradeoff and is
/// bounded by the short TTL; the `/version` poll is sequential in practice, so
/// adding single-flight would diverge from the established pattern for no real
/// gain.
#[derive(Clone)]
pub struct ServiceHealthCache {
    inner: Arc<Mutex<Option<CachedServiceHealth>>>,
    /// Entries older than this are re-queried. Defaults to
    /// [`SERVICE_HEALTH_TTL`]; injectable so tests can force expiry instantly
    /// instead of sleeping the real 2s.
    ttl: Duration,
}

impl Default for ServiceHealthCache {
    fn default() -> Self {
        Self {
            inner: Arc::new(Mutex::new(None)),
            ttl: SERVICE_HEALTH_TTL,
        }
    }
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

    /// Construct a cache with a custom TTL. Production uses [`Self::new`] (2s);
    /// this exists so tests can pin both sides of the TTL boundary without a
    /// real `sleep`.
    pub fn with_ttl(ttl: Duration) -> Self {
        Self {
            ttl,
            ..Self::default()
        }
    }

    /// Return whether `service` is `active`, using the cached value if it is
    /// younger than the cache's TTL and for the same service name. Resolves
    /// `systemctl` via PATH.
    pub async fn is_active(&self, service: &str) -> bool {
        self.is_active_with(Path::new("systemctl"), service).await
    }

    /// [`Self::is_active`] with the `systemctl` binary path injected so tests
    /// can point at a stub without real systemd. Production callers use
    /// [`Self::is_active`].
    pub async fn is_active_with(&self, systemctl: &Path, service: &str) -> bool {
        self.is_active_inner(service, || query_service_active(systemctl, service))
            .await
    }

    /// Cache logic (TTL check + store) with the underlying query injected as a
    /// closure. Production passes [`query_service_active`] (which forks
    /// `systemctl`); cache-behaviour tests pass an in-process closure so they
    /// can assert query counts deterministically without forking a subprocess
    /// per cached call (which is flaky under plain `cargo test` — see
    /// `write_stub_systemctl` for why). The real fork-and-interpret path is
    /// covered separately by the `query_service_active` tests.
    async fn is_active_inner<F, Fut>(&self, service: &str, query: F) -> bool
    where
        F: FnOnce() -> Fut,
        Fut: std::future::Future<Output = bool>,
    {
        {
            let guard = self.inner.lock().await;
            if let Some(c) = guard.as_ref() {
                if c.service == service && c.checked_at.elapsed() < self.ttl {
                    return c.active;
                }
            }
        }

        let active = query().await;
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
    /// (non-active states exit non-zero but still print the state word). Used
    /// by the `query_service_active` interpretation tests and the
    /// `is_active_with` production-wiring smoke test. Cache-behaviour tests do
    /// NOT fork a stub (see the `is_active_inner` closure tests below): a real
    /// fork per cached call made those assertions flaky under plain
    /// `cargo test`, because each `#[tokio::test]` is its own current-thread
    /// runtime and many of them spawning+reaping children concurrently in one
    /// process races tokio's Unix child reaper, so `child.wait()` occasionally
    /// fails. CI runs these under `cargo nextest`, which isolates each test in
    /// its own process and avoids the race; the in-process `is_active_inner`
    /// closure tests keep the cache logic deterministic under either runner.
    ///
    /// The write handle is flushed and explicitly closed before chmod/exec —
    /// good hygiene against ETXTBSY, independent of the reaper race above.
    fn write_stub_systemctl(dir: &Path, state: &str) -> PathBuf {
        let bin = dir.join("fake-systemctl");
        let exit = if state == "active" { 0 } else { 3 };
        let script = format!("#!/bin/sh\necho {state}\nexit {exit}\n");
        {
            let mut f = std::fs::File::create(&bin).unwrap();
            f.write_all(script.as_bytes()).unwrap();
            f.flush().unwrap();
        } // f dropped/closed here, so the file is not open-for-write at exec.
        let mut perms = std::fs::metadata(&bin).unwrap().permissions();
        perms.set_mode(0o755);
        std::fs::set_permissions(&bin, perms).unwrap();
        bin
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

    // Cache-behaviour tests drive the cache through `is_active_inner` with an
    // in-process counting closure rather than forking a real stub. The
    // subprocess fork-and-interpret path is covered by the
    // `service_active_*` tests above; mixing a per-call fork into the
    // cache-behaviour assertions made them flaky under the parallel test
    // runner (ETXTBSY when exec'ing freshly-written stub scripts). Counting an
    // AtomicUsize is deterministic and has no shared OS state.
    use std::sync::atomic::{AtomicUsize, Ordering};

    #[tokio::test]
    async fn service_health_cache_does_not_refork_within_ttl() {
        // The unauthenticated /version endpoint must NOT re-query systemctl on
        // every hit (self-DoS vector). A second lookup within the TTL must be
        // served from cache, so the query closure runs exactly once.
        let queries = AtomicUsize::new(0);
        let q = || {
            queries.fetch_add(1, Ordering::SeqCst);
            std::future::ready(true)
        };

        let cache = ServiceHealthCache::new(); // 2s TTL: both calls are within it.
        assert!(cache.is_active_inner("freenet-gateway", q).await);
        assert!(cache.is_active_inner("freenet-gateway", q).await);

        assert_eq!(
            queries.load(Ordering::SeqCst),
            1,
            "second is_active within TTL must hit the cache, not re-query"
        );
    }

    #[tokio::test]
    async fn service_health_cache_reforks_for_different_service() {
        // A different service name is a different question — must not be served
        // from a cache entry keyed on the previous name.
        let queries = AtomicUsize::new(0);
        let q = || {
            queries.fetch_add(1, Ordering::SeqCst);
            std::future::ready(true)
        };

        let cache = ServiceHealthCache::new();
        cache.is_active_inner("freenet-gateway", q).await;
        cache.is_active_inner("freenet-gateway-hector", q).await;

        assert_eq!(
            queries.load(Ordering::SeqCst),
            2,
            "different service name must re-query, not reuse the cached entry"
        );
    }

    #[tokio::test]
    async fn service_health_cache_reforks_after_ttl_expiry() {
        // The cache's whole purpose is bounding staleness, so pin the expiry
        // side of the TTL boundary: once an entry is older than the TTL, the
        // next lookup must re-query AND return the fresh state.
        //
        // TTL = 0 makes every cached entry instantly expired (elapsed() < 0 is
        // never true), so the second lookup deterministically re-queries — no
        // real `sleep` needed, no flakiness. The within-TTL no-refork case is
        // covered by `service_health_cache_does_not_refork_within_ttl`.
        let queries = AtomicUsize::new(0);
        // Reports `active` on the first query, `failed` on every later one, so
        // the test can observe that the re-query returned the FRESH state.
        let q = || {
            let n = queries.fetch_add(1, Ordering::SeqCst);
            std::future::ready(n == 0)
        };

        let cache = ServiceHealthCache::with_ttl(Duration::ZERO);

        assert!(
            cache.is_active_inner("freenet-gateway", q).await,
            "first lookup reflects the active state"
        );
        // Entry is already expired (TTL = 0): the next lookup must re-query
        // rather than serve the stale `true`, and pick up the fresh `failed`
        // state (the vega case).
        assert!(
            !cache.is_active_inner("freenet-gateway", q).await,
            "after TTL expiry the cache must re-query and return the fresh (failed) state"
        );

        assert_eq!(
            queries.load(Ordering::SeqCst),
            2,
            "an expired entry must trigger a re-query"
        );
    }

    #[tokio::test]
    async fn service_health_cache_is_active_with_forks_real_query() {
        // Smoke-test the production wiring: `is_active_with` must actually call
        // through to the real `systemctl` fork path (a stub here). The cache-
        // behaviour assertions use the in-process closure above; this one keeps
        // a single fork so the closure-vs-real seam can't silently diverge.
        let dir = tempfile::tempdir().unwrap();
        let systemctl = write_stub_systemctl(dir.path(), "active");
        let cache = ServiceHealthCache::new();
        assert!(cache.is_active_with(&systemctl, "freenet-gateway").await);
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
            {
                let mut f = std::fs::File::create(&bin_path).unwrap();
                writeln!(f, "{contents}").unwrap();
                f.flush().unwrap();
            } // close before chmod/exec — see write_stub_systemctl re: ETXTBSY.
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
