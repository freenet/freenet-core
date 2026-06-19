//! End-to-end coverage of the `/update` pipeline. Exercises HMAC, replay,
//! version-pin (GitHub-latest cross-check via the `LatestSource` trait
//! seam), no-op, downgrade refusal, rate-limit, and the dry-run happy
//! path. Catches regressions in the validation step ordering — e.g. a
//! refactor that accidentally puts JSON parse before HMAC verify, or that
//! consumes the rate-limit window before the GitHub check.

use std::os::unix::fs::PermissionsExt;
use std::path::Path;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use freenet_release_agent::{
    auth::{HEADER_SIGNATURE, sign},
    config::Config,
    github::{LatestSource, StaticLatest},
    server::{AppState, UpdateRequest, build_router},
    updater::Updater,
    version::{ServiceHealthCache, VersionCache},
};
use semver::Version;
use serde_json::json;
use tokio::sync::Mutex;

fn now_secs() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs() as i64
}

/// Poll `path` until it exists and is non-empty, returning its contents. The
/// fake-sudo wrappers record argv from the SPAWNED child, which races the HTTP
/// response — a single read can land before the child has written under
/// parallel load. Bounded deadline so a genuinely missing write still fails the
/// test rather than hanging.
async fn poll_file_nonempty(path: &Path) -> String {
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
    loop {
        if let Ok(contents) = std::fs::read_to_string(path) {
            if !contents.trim().is_empty() {
                return contents;
            }
        }
        assert!(
            std::time::Instant::now() < deadline,
            "expected {} to be recorded within the deadline",
            path.display()
        );
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    }
}

/// Poll `path` until it contains at least `expected` lines. Used to drain
/// lingering stub children before a test drops its Harness (and the TempDir
/// holding the stub script): each spawned stub appends one line to a shared
/// marker file on EXIT, so once the line count reaches the number of children
/// we spawned, every child has finished and teardown won't delete a script out
/// from under a still-running process. Bounded deadline so a genuinely stuck
/// child fails the test rather than hanging.
async fn poll_done_count(path: &Path, expected: usize) {
    if expected == 0 {
        return;
    }
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
    loop {
        let count = std::fs::read_to_string(path)
            .map(|c| c.lines().filter(|l| !l.trim().is_empty()).count())
            .unwrap_or(0);
        if count >= expected {
            return;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "expected {expected} stub child(ren) to finish (found {count}) within the deadline"
        );
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    }
}

fn write_stub_freenet(dir: &Path, version: &str) -> std::path::PathBuf {
    let bin = dir.join("fake-freenet");
    let script = format!("#!/bin/sh\necho freenet {version}\n");
    std::fs::write(&bin, script).unwrap();
    let mut perms = std::fs::metadata(&bin).unwrap().permissions();
    perms.set_mode(0o755);
    std::fs::set_permissions(&bin, perms).unwrap();
    bin
}

/// Write a stub `systemctl` that prints `state` and exits 0 for `active`, 3
/// otherwise — matching real `systemctl is-active`. Lets the `/version`
/// service-health reporting be exercised over HTTP without real systemd.
fn write_stub_systemctl(dir: &Path, state: &str) -> std::path::PathBuf {
    let bin = dir.join("fake-systemctl");
    let exit = if state == "active" { 0 } else { 3 };
    let script = format!("#!/bin/sh\necho {state}\nexit {exit}\n");
    std::fs::write(&bin, script).unwrap();
    let mut perms = std::fs::metadata(&bin).unwrap().permissions();
    perms.set_mode(0o755);
    std::fs::set_permissions(&bin, perms).unwrap();
    bin
}

struct Harness {
    base: String,
    secret: Vec<u8>,
    _tmp: tempfile::TempDir,
}

impl Harness {
    async fn build(
        current_binary_version: &str,
        latest_on_github: Version,
        rate_limit_seconds: u64,
    ) -> Self {
        let tmp = tempfile::tempdir().unwrap();
        let binary_path = write_stub_freenet(tmp.path(), current_binary_version);

        let secret: Vec<u8> = (0..32).map(|i| i as u8).collect();
        let secret_path = tmp.path().join("hmac.key");
        std::fs::write(&secret_path, hex::encode(&secret)).unwrap();

        let config = Config {
            listen_addr: "127.0.0.1:0".parse().unwrap(),
            binary_path: binary_path.clone(),
            update_command: tmp.path().join("never-invoked.sh"),
            hmac_secret_path: secret_path,
            github_repo: "freenet/freenet-core".into(),
            // dry_run = true so the updater never actually shells out;
            // we only care about the request-validation pipeline here.
            // A separate live-spawn test would require sudoers / root.
            dry_run: true,
            rate_limit_seconds,
            clock_skew_tolerance_seconds: 300,
            river_announce_command: std::path::PathBuf::new(),
            river_announce_user: String::new(),
            managed_service: "freenet-gateway".into(),
        };

        // Default stub: report the service active so the /version handler
        // returns a deterministic `service_active: true` regardless of the
        // host's real systemd state. Tests that care about the false case use
        // `build_with_service_state`.
        let systemctl_path = write_stub_systemctl(tmp.path(), "active");

        let latest_source: Arc<dyn LatestSource> = Arc::new(StaticLatest(latest_on_github));
        let state = AppState {
            config: Arc::new(config.clone()),
            secret: Arc::new(secret.clone()),
            latest_source,
            updater: Updater::new_with_sudo(config.update_command.clone(), true),
            announcer: freenet_release_agent::announcer::Announcer::new_with_sudo(
                std::path::PathBuf::new(),
                true,
                String::new(),
            ),
            version_cache: VersionCache::new(),
            service_health_cache: ServiceHealthCache::new(),
            systemctl_path,
            last_update_attempt: Arc::new(Mutex::new(None)),
            last_announce_attempt: Arc::new(Mutex::new(None)),
            update_in_flight: Arc::new(std::sync::atomic::AtomicBool::new(false)),
        };

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let router = build_router(state);
        tokio::spawn(async move {
            axum::serve(listener, router).await.unwrap();
        });

        Self {
            base: format!("http://{addr}"),
            secret,
            _tmp: tmp,
        }
    }

    /// Like [`Harness::build`] but with the gateway service-state stub set to
    /// `service_state` (e.g. `active`, `failed`), so the `/version`
    /// service-health field can be pinned over the HTTP layer. Always reports
    /// the binary as already on `binary_version` (dry-run, no spawn).
    async fn build_with_service_state(binary_version: &str, service_state: &str) -> Self {
        let tmp = tempfile::tempdir().unwrap();
        let binary_path = write_stub_freenet(tmp.path(), binary_version);
        let systemctl_path = write_stub_systemctl(tmp.path(), service_state);

        let secret: Vec<u8> = (0..32).map(|i| i as u8).collect();
        let secret_path = tmp.path().join("hmac.key");
        std::fs::write(&secret_path, hex::encode(&secret)).unwrap();

        let config = Config {
            listen_addr: "127.0.0.1:0".parse().unwrap(),
            binary_path: binary_path.clone(),
            update_command: tmp.path().join("never-invoked.sh"),
            hmac_secret_path: secret_path,
            github_repo: "freenet/freenet-core".into(),
            dry_run: true,
            rate_limit_seconds: 600,
            clock_skew_tolerance_seconds: 300,
            river_announce_command: std::path::PathBuf::new(),
            river_announce_user: String::new(),
            managed_service: "freenet-gateway".into(),
        };

        let latest_source: Arc<dyn LatestSource> =
            Arc::new(StaticLatest(Version::parse(binary_version).unwrap()));
        let state = AppState {
            config: Arc::new(config.clone()),
            secret: Arc::new(secret.clone()),
            latest_source,
            updater: Updater::new_with_sudo(config.update_command.clone(), true),
            announcer: freenet_release_agent::announcer::Announcer::new_with_sudo(
                std::path::PathBuf::new(),
                true,
                String::new(),
            ),
            version_cache: VersionCache::new(),
            service_health_cache: ServiceHealthCache::new(),
            systemctl_path,
            last_update_attempt: Arc::new(Mutex::new(None)),
            last_announce_attempt: Arc::new(Mutex::new(None)),
            update_in_flight: Arc::new(std::sync::atomic::AtomicBool::new(false)),
        };

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let router = build_router(state);
        tokio::spawn(async move {
            axum::serve(listener, router).await.unwrap();
        });

        Self {
            base: format!("http://{addr}"),
            secret,
            _tmp: tmp,
        }
    }

    /// Live-spawn variant: `dry_run = false`, fake-sudo script captures
    /// argv to `record_file`, real `update_command` is the user-supplied
    /// `update_script`. Lets tests exercise the spawn pipeline without
    /// needing real root/sudoers.
    async fn build_live(
        current_binary_version: &str,
        latest_on_github: Version,
        rate_limit_seconds: u64,
        update_script: &str,
        record_file: &Path,
    ) -> Self {
        let tmp = tempfile::tempdir().unwrap();
        let binary_path = write_stub_freenet(tmp.path(), current_binary_version);

        let secret: Vec<u8> = (0..32).map(|i| i as u8).collect();
        let secret_path = tmp.path().join("hmac.key");
        std::fs::write(&secret_path, hex::encode(&secret)).unwrap();

        // The "real" update script the agent is told to invoke.
        let update_command = tmp.path().join("update.sh");
        std::fs::write(&update_command, update_script).unwrap();
        let mut perms = std::fs::metadata(&update_command).unwrap().permissions();
        perms.set_mode(0o755);
        std::fs::set_permissions(&update_command, perms).unwrap();

        // Fake-sudo: records exact argv (after the --non-interactive that
        // the agent always prepends), then either passes through to the
        // update script or short-circuits — depends on the script's exit.
        let fake_sudo = tmp.path().join("fake-sudo");
        let sudo_script = format!(
            "#!/bin/sh\n\
             # First arg is --non-interactive, then the real command + its argv.\n\
             shift\n\
             echo \"$@\" > {record}\n\
             # Exec the rest so the test stub's exit code surfaces.\n\
             exec \"$@\"\n",
            record = record_file.display()
        );
        std::fs::write(&fake_sudo, sudo_script).unwrap();
        let mut perms = std::fs::metadata(&fake_sudo).unwrap().permissions();
        perms.set_mode(0o755);
        std::fs::set_permissions(&fake_sudo, perms).unwrap();

        let config = Config {
            listen_addr: "127.0.0.1:0".parse().unwrap(),
            binary_path: binary_path.clone(),
            update_command: update_command.clone(),
            hmac_secret_path: secret_path,
            github_repo: "freenet/freenet-core".into(),
            dry_run: false,
            rate_limit_seconds,
            clock_skew_tolerance_seconds: 300,
            river_announce_command: std::path::PathBuf::new(),
            river_announce_user: String::new(),
            managed_service: "freenet-gateway".into(),
        };

        let systemctl_path = write_stub_systemctl(tmp.path(), "active");
        let latest_source: Arc<dyn LatestSource> = Arc::new(StaticLatest(latest_on_github));
        let updater = freenet_release_agent::updater::Updater {
            command: update_command,
            dry_run: false,
            sudo_command: fake_sudo,
        };
        let state = AppState {
            config: Arc::new(config),
            secret: Arc::new(secret.clone()),
            latest_source,
            updater,
            announcer: freenet_release_agent::announcer::Announcer::new_with_sudo(
                std::path::PathBuf::new(),
                true,
                String::new(),
            ),
            version_cache: VersionCache::new(),
            service_health_cache: ServiceHealthCache::new(),
            systemctl_path,
            last_update_attempt: Arc::new(Mutex::new(None)),
            last_announce_attempt: Arc::new(Mutex::new(None)),
            update_in_flight: Arc::new(std::sync::atomic::AtomicBool::new(false)),
        };

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let router = build_router(state);
        tokio::spawn(async move {
            axum::serve(listener, router).await.unwrap();
        });

        Self {
            base: format!("http://{addr}"),
            secret,
            _tmp: tmp,
        }
    }

    fn signed_body(&self, version: &str, issued_at: i64) -> (String, String) {
        let req = UpdateRequest {
            version: version.into(),
            issued_at,
        };
        let body = serde_json::to_string(&req).unwrap();
        let sig = sign(&self.secret, body.as_bytes());
        (body, sig)
    }
}

#[tokio::test]
async fn happy_path_dry_run() {
    let h = Harness::build("0.2.55", Version::new(0, 2, 56), 600).await;
    let (body, sig) = h.signed_body("0.2.56", now_secs());
    let resp = reqwest::Client::new()
        .post(format!("{}/update", h.base))
        .header(HEADER_SIGNATURE.as_str(), &sig)
        .body(body)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    let j: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(j["accepted"], true);
    assert_eq!(j["dry_run"], true);
    assert_eq!(j["no_op"], false);
    assert_eq!(j["target_version"], "0.2.56");
}

#[tokio::test]
async fn missing_signature_is_401() {
    let h = Harness::build("0.2.55", Version::new(0, 2, 56), 600).await;
    let (body, _) = h.signed_body("0.2.56", now_secs());
    let resp = reqwest::Client::new()
        .post(format!("{}/update", h.base))
        .body(body)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 401);
}

#[tokio::test]
async fn tampered_body_is_401() {
    let h = Harness::build("0.2.55", Version::new(0, 2, 56), 600).await;
    let (body, sig) = h.signed_body("0.2.56", now_secs());
    let tampered = body.replace("0.2.56", "0.2.99");
    let resp = reqwest::Client::new()
        .post(format!("{}/update", h.base))
        .header(HEADER_SIGNATURE.as_str(), &sig)
        .body(tampered)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 401);
}

#[tokio::test]
async fn stale_issued_at_is_401() {
    let h = Harness::build("0.2.55", Version::new(0, 2, 56), 600).await;
    let (body, sig) = h.signed_body("0.2.56", now_secs() - 3600);
    let resp = reqwest::Client::new()
        .post(format!("{}/update", h.base))
        .header(HEADER_SIGNATURE.as_str(), &sig)
        .body(body)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 401);
}

#[tokio::test]
async fn malformed_body_is_400() {
    let h = Harness::build("0.2.55", Version::new(0, 2, 56), 600).await;
    let body = json!({"not": "a valid request"}).to_string();
    let sig = sign(&h.secret, body.as_bytes());
    let resp = reqwest::Client::new()
        .post(format!("{}/update", h.base))
        .header(HEADER_SIGNATURE.as_str(), &sig)
        .body(body)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 400);
}

#[tokio::test]
async fn downgrade_is_403() {
    let h = Harness::build("0.2.56", Version::new(0, 2, 55), 600).await;
    let (body, sig) = h.signed_body("0.2.55", now_secs());
    let resp = reqwest::Client::new()
        .post(format!("{}/update", h.base))
        .header(HEADER_SIGNATURE.as_str(), &sig)
        .body(body)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 403);
}

#[tokio::test]
async fn version_pin_mismatch_is_403() {
    // Security-critical: a valid HMAC signature for a version that doesn't
    // match GitHub `latest` must be refused. This is the defense against a
    // leaked HMAC token.
    let h = Harness::build("0.2.55", Version::new(0, 2, 56), 600).await;
    let (body, sig) = h.signed_body("0.2.99", now_secs());
    let resp = reqwest::Client::new()
        .post(format!("{}/update", h.base))
        .header(HEADER_SIGNATURE.as_str(), &sig)
        .body(body)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 403);
}

#[tokio::test]
async fn no_op_returns_200_with_flag() {
    let h = Harness::build("0.2.56", Version::new(0, 2, 56), 600).await;
    let (body, sig) = h.signed_body("0.2.56", now_secs());
    let resp = reqwest::Client::new()
        .post(format!("{}/update", h.base))
        .header(HEADER_SIGNATURE.as_str(), &sig)
        .body(body)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    let j: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(j["no_op"], true);
}

#[tokio::test]
async fn rate_limit_kicks_in_after_first_success() {
    // 10-minute window. Second request immediately after first must be
    // refused with 429.
    let h = Harness::build("0.2.55", Version::new(0, 2, 56), 600).await;
    let now = now_secs();

    // First request — succeeds
    let (body1, sig1) = h.signed_body("0.2.56", now);
    let r1 = reqwest::Client::new()
        .post(format!("{}/update", h.base))
        .header(HEADER_SIGNATURE.as_str(), &sig1)
        .body(body1)
        .send()
        .await
        .unwrap();
    assert_eq!(r1.status(), 200);

    // Second request, distinct payload so the HMAC differs — should be 429
    let (body2, sig2) = h.signed_body("0.2.56", now + 1);
    let r2 = reqwest::Client::new()
        .post(format!("{}/update", h.base))
        .header(HEADER_SIGNATURE.as_str(), &sig2)
        .body(body2)
        .send()
        .await
        .unwrap();
    assert_eq!(r2.status(), 429);
}

#[tokio::test]
async fn version_endpoint_reads_stub_binary() {
    let h = Harness::build("0.2.55", Version::new(0, 2, 56), 600).await;
    let resp = reqwest::Client::new()
        .get(format!("{}/version", h.base))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    let j: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(j["version"], "0.2.55");

    // Service-health fields must be present so the release workflow can
    // distinguish a successful binary swap from a gateway whose service
    // failed to restart (vega v0.2.71). The default harness stub reports the
    // service `active`, so this asserts the field is wired through as `true`.
    assert_eq!(j["managed_service"], "freenet-gateway");
    assert_eq!(j["service_active"], true);
}

#[tokio::test]
async fn version_endpoint_reports_service_active_true() {
    // Stub systemctl reports `active` → /version must report service_active: true.
    let h = Harness::build_with_service_state("0.2.71", "active").await;
    let resp = reqwest::Client::new()
        .get(format!("{}/version", h.base))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    let j: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(j["version"], "0.2.71");
    assert_eq!(j["service_active"], true);
}

#[tokio::test]
async fn version_endpoint_reports_service_active_false_when_failed() {
    // The exact vega v0.2.71 shape: binary swapped to the new version on disk
    // but the service is `failed`. /version must report service_active: false
    // so the workflow does NOT treat the update as successful.
    let h = Harness::build_with_service_state("0.2.71", "failed").await;
    let resp = reqwest::Client::new()
        .get(format!("{}/version", h.base))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    let j: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(
        j["version"], "0.2.71",
        "binary reads as the new version (swap succeeded)"
    );
    assert_eq!(
        j["service_active"], false,
        "service is failed → must NOT look like a successful update"
    );
}

#[tokio::test]
async fn healthz_is_ok() {
    let h = Harness::build("0.2.55", Version::new(0, 2, 56), 600).await;
    let resp = reqwest::Client::new()
        .get(format!("{}/healthz", h.base))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
}

#[tokio::test]
async fn spawn_failure_does_not_consume_rate_limit_window() {
    // Security-critical contract: an immediate sudo rejection or script
    // failure that is caught within the 1s probe window (surfaced as 500) must
    // NOT mark the rate-limit window consumed — otherwise an attacker (or a
    // transient sudoers misconfig) could lock out legitimate retries for the
    // full window.
    //
    // Determinism note: whether the `exit 1` child is *reaped* within the 1s
    // wall-clock probe is scheduler-dependent. Under heavy parallel load the
    // child can miss the probe deadline and be (legitimately) classified as
    // "still running" → 202, in which case production DOES consume the window
    // for that classification — so we cannot assert "never 429" against a
    // single attempt. Instead we retry the scenario on a FRESH harness until
    // the probe deterministically catches the fast exit (500), then assert the
    // window-not-consumed contract on that attempt. In isolation the 500 path
    // is reliable; the retry only absorbs rare load-induced misses.
    let mut attempt = 0;
    loop {
        attempt += 1;
        let tmp = tempfile::tempdir().unwrap();
        let argv_record = tmp.path().join("argv.log");
        let done_log = tmp.path().join("done.log");
        // Update script records its exit then exits 1 immediately (surfaced as
        // 500 to the caller). The `echo done` lets us drain every spawned child
        // before this iteration's TempDir drops — under load the fast `exit 1`
        // can miss the 1s probe and be handed to the background wait task, whose
        // `Command::spawn` of fake-sudo would otherwise race the TempDir rmdir
        // and log `No such file or directory` to stderr.
        let update_script = format!("#!/bin/sh\necho done >> {}\nexit 1\n", done_log.display());
        let h = Harness::build_live(
            "0.2.55",
            Version::new(0, 2, 56),
            600, // 10 min window — long enough that a second successful spawn
            // within seconds proves the window WAS NOT consumed.
            &update_script,
            &argv_record,
        )
        .await;

        // Count spawned children so we can drain all of them before dropping
        // this iteration's TempDir. Any POST that passes validation spawns the
        // update child (500 = caught fast exit, 202 = probe-miss handed to the
        // background task); both append a `done` line on exit.
        let mut spawns = 0usize;

        let (body, sig) = h.signed_body("0.2.56", now_secs());
        let r1 = reqwest::Client::new()
            .post(format!("{}/update", h.base))
            .header(HEADER_SIGNATURE.as_str(), &sig)
            .body(body)
            .send()
            .await
            .unwrap();
        spawns += 1;
        if r1.status() != 500 {
            // Probe missed the fast exit under load (got 202); retry on a fresh
            // harness so this stays deterministic. Bounded to avoid a hang if
            // the 500 path were genuinely broken. Drain the spawned child first
            // so its fake-sudo exec doesn't race this TempDir's deletion.
            poll_done_count(&done_log, spawns).await;
            assert!(
                attempt < 20,
                "spawn failure never surfaced as 500 across {attempt} attempts; got {}",
                r1.status()
            );
            continue;
        }

        // Immediately retry. Should NOT be 429 — window must not have
        // been consumed by the failed spawn.
        let (body2, sig2) = h.signed_body("0.2.56", now_secs());
        let r2 = reqwest::Client::new()
            .post(format!("{}/update", h.base))
            .header(HEADER_SIGNATURE.as_str(), &sig2)
            .body(body2)
            .send()
            .await
            .unwrap();
        spawns += 1;
        assert_ne!(
            r2.status(),
            429,
            "rate-limit window must not be consumed by a failed spawn"
        );
        // Also exclude 409: a failed spawn must release the InFlightGuard (Rust
        // move semantics drop it when `run` returns Err), so the retry must not
        // see the slot still held. Without this, a future leak of the guard on
        // the Err path would surface as 409 (not 429) and slip past the check above.
        assert_ne!(
            r2.status(),
            409,
            "failed spawn must release the in-flight guard, so the retry is not 409"
        );
        // Drain both spawned children before the TempDir drops.
        poll_done_count(&done_log, spawns).await;
        break;
    }
}

#[tokio::test]
async fn concurrent_updates_only_spawn_once() {
    // Regression for #4271 (nova 0.2.65 outage): two update POSTs arriving
    // while the first update's restart is still in flight must NOT both spawn
    // a `systemctl stop/restart` — the second restart SIGKILLed the freshly
    // started process and took the gateway DOWN. The in-flight guard makes the
    // agent idempotent: the first POST spawns, the second is rejected with 409
    // while the first is still running.
    //
    // The update script appends one line per invocation to a shared counter
    // file and sleeps well past the 1s early-exit probe, so the first update
    // is provably "in flight" when the second POST lands. Without the guard,
    // BOTH POSTs spawn and the counter file has two lines — the assertion below
    // is load-bearing.
    let tmp = tempfile::tempdir().unwrap();
    let argv_record = tmp.path().join("argv.log");
    let spawn_count = tmp.path().join("spawns.log");
    let done_marker = tmp.path().join("done.log");
    // Each spawn appends a line, then sleeps just past the 1s early-exit probe
    // so it is still running when the second POST arrives (1s probe < 1.5s
    // sleep). Kept as short as possible so the stub child doesn't linger after
    // the HTTP responses land and add scheduler contention to sibling tests
    // running in parallel. The final `echo done` lets the test drain the
    // in-flight child before dropping the Harness (and its TempDir) — otherwise
    // the still-sleeping child's script would vanish out from under it and emit
    // a `No such file or directory` to stderr during teardown.
    let update_script = format!(
        "#!/bin/sh\necho spawned >> {}\nsleep 1.5\necho done > {}\n",
        spawn_count.display(),
        done_marker.display(),
    );
    // rate_limit_seconds = 0 so the rate-limiter never fires: this test
    // isolates the in-flight guard (429 vs 409 are different mechanisms, and
    // the first request holds the rate-limit mutex across its 1s probe, so a
    // nonzero window would mask the overlap with a 429 instead of the 409 we
    // are asserting).
    let h = Harness::build_live(
        "0.2.55",
        Version::new(0, 2, 56),
        0,
        &update_script,
        &argv_record,
    )
    .await;

    // Fire both POSTs concurrently. Distinct issued_at so the HMACs differ,
    // mirroring two genuinely separate update commands.
    let now = now_secs();
    let (body1, sig1) = h.signed_body("0.2.56", now);
    let (body2, sig2) = h.signed_body("0.2.56", now + 1);
    let base = h.base.clone();
    let url = format!("{base}/update");

    let url1 = url.clone();
    let f1 = tokio::spawn(async move {
        reqwest::Client::new()
            .post(&url1)
            .header(HEADER_SIGNATURE.as_str(), &sig1)
            .body(body1)
            .send()
            .await
            .unwrap()
            .status()
            .as_u16()
    });
    // Small skew so f1 reliably claims the slot first, then f2 races into the
    // in-flight window while f1's stub is still sleeping.
    tokio::time::sleep(std::time::Duration::from_millis(200)).await;
    let f2 = tokio::spawn(async move {
        reqwest::Client::new()
            .post(&url)
            .header(HEADER_SIGNATURE.as_str(), &sig2)
            .body(body2)
            .send()
            .await
            .unwrap()
            .status()
            .as_u16()
    });

    let s1 = f1.await.unwrap();
    let s2 = f2.await.unwrap();

    // Exactly one acceptance (200/202) and exactly one 409 rejection, in
    // either order.
    let mut statuses = [s1, s2];
    statuses.sort_unstable();
    assert_eq!(
        statuses[1], 409,
        "the second, overlapping update must be rejected with 409 Conflict (got {s1} and {s2})"
    );
    assert!(
        statuses[0] == 200 || statuses[0] == 202,
        "the first update must be accepted (got {s1} and {s2})"
    );

    // Load-bearing: only ONE spawn reached the update script. Without the
    // in-flight guard the second POST would also spawn and this file would
    // have two lines — the exact double-stop that downed nova.
    //
    // The spawned stub writes its marker at the top of the script, but it runs
    // asynchronously; under load it may not have flushed by the time the HTTP
    // responses land. Poll briefly for the first marker to appear, then assert
    // it never grows to two — if a second spawn occurred it would already be
    // present (both POSTs were dispatched and resolved above).
    let spawns = {
        let mut contents = String::new();
        for _ in 0..50 {
            contents = std::fs::read_to_string(&spawn_count).unwrap_or_default();
            if !contents.trim().is_empty() {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        }
        contents
    };
    assert_eq!(
        spawns.lines().count(),
        1,
        "exactly one update may spawn while another is in flight, got: {spawns:?}"
    );

    // Drain the in-flight stub child before the Harness (and its TempDir) drops.
    // The accepted update's stub sleeps 1.5s; if we returned now the TempDir
    // would be deleted out from under the still-running script, which then logs
    // `No such file or directory` to stderr. Polling its `done` marker makes
    // teardown deterministic and noise-free.
    poll_file_nonempty(&done_marker).await;
}

#[tokio::test]
async fn update_after_in_flight_completes_is_accepted() {
    // The in-flight guard must be RELEASED when the update completes, so a
    // legitimate follow-up update is not permanently blocked. Uses a short
    // sleep (just past the 1s probe) then exit, and a 0s rate-limit window so
    // the rate-limiter doesn't mask the in-flight behaviour being tested.
    let tmp = tempfile::tempdir().unwrap();
    let argv_record = tmp.path().join("argv.log");
    let done_log = tmp.path().join("done.log");
    // Sleep just past the 1s early-exit probe; kept minimal so the stub child
    // doesn't linger and add scheduler contention to parallel tests. Each
    // invocation appends a line to `done.log` on EXIT, so the test can drain
    // every accepted-and-spawned child before dropping the Harness (and its
    // TempDir) — otherwise a still-sleeping child's script would vanish out
    // from under it and emit `No such file or directory` to stderr at teardown.
    let update_script = format!(
        "#!/bin/sh\nsleep 1.5\necho done >> {}\n",
        done_log.display()
    );
    let h = Harness::build_live(
        "0.2.55",
        Version::new(0, 2, 56),
        0, // no rate-limit, so we isolate the in-flight guard
        &update_script,
        &argv_record,
    )
    .await;

    // Count the children we actually spawn so teardown can wait for all of them
    // to exit (the first accepted update plus the second accepted follow-up).
    let mut spawned = 0u32;

    let (body1, sig1) = h.signed_body("0.2.56", now_secs());
    let r1 = reqwest::Client::new()
        .post(format!("{}/update", h.base))
        .header(HEADER_SIGNATURE.as_str(), &sig1)
        .body(body1)
        .send()
        .await
        .unwrap();
    assert!(r1.status() == 200 || r1.status() == 202);
    spawned += 1;

    // While still in flight, a second update is 409.
    let (body2, sig2) = h.signed_body("0.2.56", now_secs());
    let r2 = reqwest::Client::new()
        .post(format!("{}/update", h.base))
        .header(HEADER_SIGNATURE.as_str(), &sig2)
        .body(body2)
        .send()
        .await
        .unwrap();
    assert_eq!(r2.status(), 409, "overlapping update must be 409");

    // POLL for the guard to release rather than asserting on a fixed sleep that
    // happens to match the stub's runtime. The background wait task frees the
    // slot when the stub child exits (~1.5s) — under parallel load that can
    // slip well past the nominal time, so a fixed wait would flake. Retry POST 3
    // until it is accepted, with a bounded deadline. Each rejected attempt is a
    // 409 (slot still held); we tolerate those and only fail if the slot never
    // frees within the deadline. rate_limit_seconds = 0 means an accepted POST
    // never turns into a 429, so retries are safe.
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(15);
    let last_status = loop {
        let (body3, sig3) = h.signed_body("0.2.56", now_secs());
        let r3 = reqwest::Client::new()
            .post(format!("{}/update", h.base))
            .header(HEADER_SIGNATURE.as_str(), &sig3)
            .body(body3)
            .send()
            .await
            .unwrap();
        let last_status = r3.status().as_u16();
        if last_status == 200 || last_status == 202 {
            spawned += 1;
            break last_status;
        }
        assert_eq!(
            last_status, 409,
            "while the slot is still held the follow-up must be 409, got {last_status}"
        );
        if std::time::Instant::now() >= deadline {
            break last_status;
        }
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    };
    assert!(
        last_status == 200 || last_status == 202,
        "guard must be released after the update completes; got {last_status}"
    );

    // Drain every child we spawned (the initial accepted update and the final
    // accepted follow-up) before the Harness/TempDir drops. Each child appends
    // one line to `done.log` on exit; wait until all of them have.
    poll_done_count(&done_log, spawned as usize).await;
}

#[tokio::test]
async fn sudoers_argv_matches_allowlist() {
    // Pins the exact argv shape the sudoers entry must match:
    //   /usr/local/bin/gateway-auto-update.sh --force --target-version *
    // A refactor that reorders --force/--target-version or adds another
    // flag would silently break every production invocation when sudoers
    // rejects it. This test records the actual argv via a fake-sudo
    // wrapper and asserts it lines up with the sudoers allowlist.
    let tmp = tempfile::tempdir().unwrap();
    let argv_record = tmp.path().join("argv.log");
    let done_marker = tmp.path().join("done.log");
    // Update script does nothing; we only care about the argv sudo sees. Sleep
    // just past the 1s early-exit probe (so the handler returns 202, not a
    // probe-window failure) and no longer — a lingering stub child only adds
    // scheduler contention to the parallel suite. The final `echo done` lets the
    // test drain the child before the TempDir drops, so the still-sleeping
    // script isn't deleted out from under it (which logs `No such file or
    // directory` to stderr at teardown).
    let update_script = format!(
        "#!/bin/sh\n# noop — wait long enough that the 1s probe times out\nsleep 1.5\necho done > {}\n",
        done_marker.display(),
    );
    let h = Harness::build_live(
        "0.2.55",
        Version::new(0, 2, 56),
        600,
        &update_script,
        &argv_record,
    )
    .await;

    let (body, sig) = h.signed_body("0.2.56", now_secs());
    let r = reqwest::Client::new()
        .post(format!("{}/update", h.base))
        .header(HEADER_SIGNATURE.as_str(), &sig)
        .body(body)
        .send()
        .await
        .unwrap();
    assert!(
        r.status() == 202 || r.status() == 200,
        "expected 200/202, got {}",
        r.status()
    );

    // POLL for the fake-sudo wrapper to record argv. The wrapper runs in the
    // spawned child, which races the HTTP response — under parallel load the
    // file may not exist yet when the response lands. A single read here was
    // green only by luck; poll with a bounded deadline instead.
    let recorded = poll_file_nonempty(&argv_record).await;
    let argv = recorded.trim();
    // Expected: <update_script_path> --force --target-version v0.2.56
    assert!(
        argv.ends_with("--force --target-version v0.2.56"),
        "argv suffix must match sudoers allowlist (got: {argv:?})"
    );

    // Drain the in-flight stub child before the Harness/TempDir drops, so the
    // still-sleeping script isn't deleted out from under it at teardown.
    poll_file_nonempty(&done_marker).await;
}

/// Build a Harness whose announcer points at a stub announce script.
/// Used by the /announce/river tests below.
async fn build_with_announcer(announce_script: &str, record_file: &Path) -> Harness {
    let tmp = tempfile::tempdir().unwrap();
    let binary_path = write_stub_freenet(tmp.path(), "0.2.56");
    let secret: Vec<u8> = (0..32).map(|i| i as u8).collect();
    let secret_path = tmp.path().join("hmac.key");
    std::fs::write(&secret_path, hex::encode(&secret)).unwrap();

    let announce_command = tmp.path().join("announce.sh");
    std::fs::write(&announce_command, announce_script).unwrap();
    let mut perms = std::fs::metadata(&announce_command).unwrap().permissions();
    perms.set_mode(0o755);
    std::fs::set_permissions(&announce_command, perms).unwrap();

    let fake_sudo = tmp.path().join("fake-sudo");
    // The agent invokes `sudo -n -u <user> <command> <message>`.
    // Record TWICE: full pre-shift argv to `<record>.full` (so tests
    // can pin the `-u <user>` shape against the sudoers allowlist),
    // and post-shift argv to `<record>` for legacy "argv contains
    // message" assertions.
    let record_full_path = format!("{}.full", record_file.display());
    let sudo_script = format!(
        "#!/bin/sh\n\
         echo \"$@\" > {record_full}\n\
         shift # --non-interactive\n\
         shift # -u\n\
         shift # <user>\n\
         echo \"$@\" > {record}\n\
         exec \"$@\"\n",
        record = record_file.display(),
        record_full = record_full_path,
    );
    std::fs::write(&fake_sudo, sudo_script).unwrap();
    let mut perms = std::fs::metadata(&fake_sudo).unwrap().permissions();
    perms.set_mode(0o755);
    std::fs::set_permissions(&fake_sudo, perms).unwrap();

    let config = Config {
        listen_addr: "127.0.0.1:0".parse().unwrap(),
        binary_path,
        update_command: tmp.path().join("unused.sh"),
        hmac_secret_path: secret_path,
        github_repo: "freenet/freenet-core".into(),
        dry_run: false,
        rate_limit_seconds: 600,
        clock_skew_tolerance_seconds: 300,
        river_announce_command: announce_command.clone(),
        river_announce_user: "nobody".into(),
        managed_service: "freenet-gateway".into(),
    };
    let latest_source: Arc<dyn LatestSource> = Arc::new(StaticLatest(Version::new(0, 2, 56)));
    let announcer = freenet_release_agent::announcer::Announcer {
        command: announce_command,
        dry_run: false,
        sudo_command: fake_sudo,
        run_as_user: "nobody".into(),
    };
    let systemctl_path = write_stub_systemctl(tmp.path(), "active");
    let state = AppState {
        config: Arc::new(config),
        secret: Arc::new(secret.clone()),
        latest_source,
        updater: Updater::new_with_sudo(std::path::PathBuf::from("/bin/true"), true),
        announcer,
        version_cache: VersionCache::new(),
        service_health_cache: ServiceHealthCache::new(),
        systemctl_path,
        last_update_attempt: Arc::new(Mutex::new(None)),
        last_announce_attempt: Arc::new(Mutex::new(None)),
        update_in_flight: Arc::new(std::sync::atomic::AtomicBool::new(false)),
    };

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let router = build_router(state);
    tokio::spawn(async move {
        axum::serve(listener, router).await.unwrap();
    });

    Harness {
        base: format!("http://{addr}"),
        secret,
        _tmp: tmp,
    }
}

fn signed_announce(secret: &[u8], message: &str, issued_at: i64) -> (String, String) {
    let body = serde_json::to_string(&json!({
        "message": message,
        "issued_at": issued_at,
    }))
    .unwrap();
    let sig = sign(secret, body.as_bytes());
    (body, sig)
}

#[tokio::test]
async fn announce_disabled_returns_503_with_valid_signature() {
    // The default-built Harness has an unconfigured announcer (empty
    // command path) — that gateway can't post to River.
    let h = Harness::build("0.2.55", Version::new(0, 2, 56), 600).await;
    let (body, sig) = signed_announce(&h.secret, "hello", now_secs());
    let resp = reqwest::Client::new()
        .post(format!("{}/announce/river", h.base))
        .header(HEADER_SIGNATURE.as_str(), &sig)
        .body(body)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 503);
}

#[tokio::test]
async fn announce_disabled_still_requires_signature() {
    // Pins the validation order: HMAC verify runs BEFORE the
    // is_configured check, so an unauthenticated probe can't
    // distinguish "endpoint disabled on this gateway" from
    // "endpoint enabled, wrong secret". Both return 401 today.
    // A future refactor that moves the 503 above the signature
    // verify would re-enable the fingerprint and fail this test.
    let h = Harness::build("0.2.55", Version::new(0, 2, 56), 600).await;
    let body = serde_json::to_string(&json!({
        "message": "hello",
        "issued_at": now_secs(),
    }))
    .unwrap();
    let resp = reqwest::Client::new()
        .post(format!("{}/announce/river", h.base))
        // No X-Signature header at all.
        .body(body)
        .send()
        .await
        .unwrap();
    assert_eq!(
        resp.status(),
        401,
        "unsigned request must 401 even when announce is unconfigured (no fingerprint leak)"
    );
}

#[tokio::test]
async fn announce_happy_path() {
    let tmp = tempfile::tempdir().unwrap();
    let record = tmp.path().join("argv.log");
    let done_marker = tmp.path().join("done.log");
    // Sleep just past the 1s probe so the handler returns 202; kept short so
    // the stub child doesn't linger and contend with the parallel suite. The
    // final `echo done` lets the test drain the child before its TempDir drops,
    // so the still-sleeping script isn't deleted out from under it (which logs
    // `No such file or directory` to stderr at teardown).
    let announce_script = format!(
        "#!/bin/sh\nsleep 1.5\necho done > {}\n",
        done_marker.display()
    );
    let h = build_with_announcer(&announce_script, &record).await;
    let (body, sig) = signed_announce(&h.secret, "Freenet v0.2.57 released", now_secs());
    let resp = reqwest::Client::new()
        .post(format!("{}/announce/river", h.base))
        .header(HEADER_SIGNATURE.as_str(), &sig)
        .body(body)
        .send()
        .await
        .unwrap();
    assert!(
        resp.status() == 200 || resp.status() == 202,
        "got {}",
        resp.status()
    );
    // POLL for the fake-sudo wrapper to record argv. It runs in the spawned
    // child, which races the HTTP response — under parallel load the file may
    // not exist yet when the response lands. The wrapper writes `record.full`
    // first, then `record`, so a non-empty `record` implies both are present.
    let recorded = poll_file_nonempty(&record).await;
    assert!(
        recorded.contains("Freenet v0.2.57 released"),
        "argv should contain the message, got: {recorded:?}"
    );

    // Pin the FULL pre-shift argv against the sudoers allowlist:
    //   sudo --non-interactive -u <user> <command> <message>
    // A refactor that drops --non-interactive or -u would otherwise
    // silently work in tests but fail at production sudoers.
    let recorded_full = std::fs::read_to_string(format!("{}.full", record.display()))
        .expect("fake-sudo recorded full argv");
    let full = recorded_full.trim();
    assert!(
        full.starts_with("--non-interactive -u nobody "),
        "full argv must start with `--non-interactive -u nobody`, got: {full:?}"
    );
    assert!(
        full.ends_with(" Freenet v0.2.57 released"),
        "full argv must end with the message, got: {full:?}"
    );

    // Drain the in-flight stub child before the Harness/TempDir drops, so the
    // still-sleeping announce script isn't deleted out from under it.
    poll_file_nonempty(&done_marker).await;
}

#[tokio::test]
async fn announce_missing_signature_is_401() {
    let tmp = tempfile::tempdir().unwrap();
    let record = tmp.path().join("argv.log");
    let h = build_with_announcer("#!/bin/sh\nexit 0\n", &record).await;
    let (body, _) = signed_announce(&h.secret, "x", now_secs());
    let resp = reqwest::Client::new()
        .post(format!("{}/announce/river", h.base))
        .body(body)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 401);
}

#[tokio::test]
async fn announce_empty_message_is_400() {
    let tmp = tempfile::tempdir().unwrap();
    let record = tmp.path().join("argv.log");
    let h = build_with_announcer("#!/bin/sh\nexit 0\n", &record).await;
    let (body, sig) = signed_announce(&h.secret, "", now_secs());
    let resp = reqwest::Client::new()
        .post(format!("{}/announce/river", h.base))
        .header(HEADER_SIGNATURE.as_str(), &sig)
        .body(body)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 400);
}

#[tokio::test]
async fn announce_oversize_message_is_413() {
    let tmp = tempfile::tempdir().unwrap();
    let record = tmp.path().join("argv.log");
    let h = build_with_announcer("#!/bin/sh\nexit 0\n", &record).await;
    // Just over the 4 KiB message cap — but under the 8 KiB HTTP body cap
    // so we hit the message-len check, not the body-limit middleware.
    let big = "x".repeat(4 * 1024 + 50);
    let (body, sig) = signed_announce(&h.secret, &big, now_secs());
    let resp = reqwest::Client::new()
        .post(format!("{}/announce/river", h.base))
        .header(HEADER_SIGNATURE.as_str(), &sig)
        .body(body)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 413);
}

#[tokio::test]
async fn announce_spawn_failure_does_not_consume_rate_limit() {
    // Mirrors `spawn_failure_does_not_consume_rate_limit_window`: the
    // announce path uses the same 1s wall-clock early-exit probe, so whether
    // the `exit 1` child is reaped within it is scheduler-dependent. Retry on a
    // FRESH harness until the probe deterministically catches the fast exit
    // (500), then assert the window-not-consumed contract on that attempt.
    let mut attempt = 0;
    loop {
        attempt += 1;
        let tmp = tempfile::tempdir().unwrap();
        let record = tmp.path().join("argv.log");
        let done_log = tmp.path().join("done.log");
        // Script records its exit then exits 1 immediately → 500 + window NOT
        // consumed. The `echo done` lets us drain every spawned child before
        // this iteration's TempDir drops — a probe-miss (202) hands the fast
        // `exit 1` child to the background wait task, whose fake-sudo exec would
        // otherwise race the TempDir rmdir and log `No such file or directory`.
        let announce_script = format!("#!/bin/sh\necho done >> {}\nexit 1\n", done_log.display());
        let h = build_with_announcer(&announce_script, &record).await;

        // Every POST that passes validation spawns the announce child (500 =
        // caught fast exit, 202 = probe-miss handed to the background task);
        // both append a `done` line on exit. Count them so we can drain all.
        let mut spawns = 0usize;

        let (body, sig) = signed_announce(&h.secret, "first", now_secs());
        let r1 = reqwest::Client::new()
            .post(format!("{}/announce/river", h.base))
            .header(HEADER_SIGNATURE.as_str(), &sig)
            .body(body)
            .send()
            .await
            .unwrap();
        spawns += 1;
        if r1.status() != 500 {
            // Probe missed the fast exit under load (got 202); retry on a fresh
            // harness. Bounded to avoid hanging if the 500 path were broken.
            // Drain the spawned child first so its fake-sudo exec doesn't race
            // this TempDir's deletion.
            poll_done_count(&done_log, spawns).await;
            assert!(
                attempt < 20,
                "announce spawn failure never surfaced as 500 across {attempt} attempts; got {}",
                r1.status()
            );
            continue;
        }

        let (body2, sig2) = signed_announce(&h.secret, "second", now_secs());
        let r2 = reqwest::Client::new()
            .post(format!("{}/announce/river", h.base))
            .header(HEADER_SIGNATURE.as_str(), &sig2)
            .body(body2)
            .send()
            .await
            .unwrap();
        spawns += 1;
        assert_ne!(
            r2.status(),
            429,
            "failed spawn must not consume the announce rate-limit window"
        );
        // Drain both spawned children before the TempDir drops.
        poll_done_count(&done_log, spawns).await;
        break;
    }
}

#[tokio::test]
async fn announce_tampered_body_is_401() {
    // Pins HMAC-verify-before-parse on the announce path. If the parse
    // ran first, a tampered body would 400 instead of 401.
    let tmp = tempfile::tempdir().unwrap();
    let record = tmp.path().join("argv.log");
    let h = build_with_announcer("#!/bin/sh\nexit 0\n", &record).await;
    let (body, sig) = signed_announce(&h.secret, "real message", now_secs());
    let tampered = body.replace("real message", "tampered!!!!");
    let resp = reqwest::Client::new()
        .post(format!("{}/announce/river", h.base))
        .header(HEADER_SIGNATURE.as_str(), &sig)
        .body(tampered)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 401);
}

#[tokio::test]
async fn announce_stale_issued_at_is_401() {
    let tmp = tempfile::tempdir().unwrap();
    let record = tmp.path().join("argv.log");
    let h = build_with_announcer("#!/bin/sh\nexit 0\n", &record).await;
    let (body, sig) = signed_announce(&h.secret, "hello", now_secs() - 3600);
    let resp = reqwest::Client::new()
        .post(format!("{}/announce/river", h.base))
        .header(HEADER_SIGNATURE.as_str(), &sig)
        .body(body)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 401);
}

#[tokio::test]
async fn announce_nul_byte_is_400() {
    // serde_json decodes \u{0} into a real NUL char; the handler
    // rejects them defensively before they reach argv.
    let tmp = tempfile::tempdir().unwrap();
    let record = tmp.path().join("argv.log");
    let h = build_with_announcer("#!/bin/sh\nexit 0\n", &record).await;
    let body = serde_json::to_string(&json!({
        "message": "hello\u{0}world",
        "issued_at": now_secs(),
    }))
    .unwrap();
    let sig = sign(&h.secret, body.as_bytes());
    let resp = reqwest::Client::new()
        .post(format!("{}/announce/river", h.base))
        .header(HEADER_SIGNATURE.as_str(), &sig)
        .body(body)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 400);
}

#[tokio::test]
async fn announce_exactly_at_message_limit_is_accepted() {
    // 4096 bytes (ANNOUNCE_MESSAGE_MAX_LEN). Handler check is `> MAX`,
    // so 4096 passes. One-over (4097) goes 413 — see next test.
    let tmp = tempfile::tempdir().unwrap();
    let record = tmp.path().join("argv.log");
    let done_marker = tmp.path().join("done.log");
    // Sleep just past the 1s probe so the handler returns 202; kept short so
    // the stub child doesn't linger and contend with the parallel suite. The
    // final `echo done` lets the test drain the child before its TempDir drops,
    // so the still-sleeping script isn't deleted out from under it (which logs
    // `No such file or directory` to stderr at teardown).
    let announce_script = format!(
        "#!/bin/sh\nsleep 1.5\necho done > {}\n",
        done_marker.display()
    );
    let h = build_with_announcer(&announce_script, &record).await;
    let exact = "x".repeat(4096);
    let (body, sig) = signed_announce(&h.secret, &exact, now_secs());
    let resp = reqwest::Client::new()
        .post(format!("{}/announce/river", h.base))
        .header(HEADER_SIGNATURE.as_str(), &sig)
        .body(body)
        .send()
        .await
        .unwrap();
    assert!(
        resp.status() == 200 || resp.status() == 202,
        "exactly-at-limit must pass; got {}",
        resp.status()
    );

    // Drain the in-flight stub child before the Harness/TempDir drops, so the
    // still-sleeping announce script isn't deleted out from under it.
    poll_file_nonempty(&done_marker).await;
}

#[tokio::test]
async fn announce_one_over_message_limit_is_413() {
    let tmp = tempfile::tempdir().unwrap();
    let record = tmp.path().join("argv.log");
    let h = build_with_announcer("#!/bin/sh\nexit 0\n", &record).await;
    let over = "x".repeat(4097);
    let (body, sig) = signed_announce(&h.secret, &over, now_secs());
    let resp = reqwest::Client::new()
        .post(format!("{}/announce/river", h.base))
        .header(HEADER_SIGNATURE.as_str(), &sig)
        .body(body)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 413);
}

#[tokio::test]
async fn announce_rate_limit_kicks_in_after_first_success() {
    // 1-minute window: two back-to-back announces, second must 429.
    let tmp = tempfile::tempdir().unwrap();
    let record = tmp.path().join("argv.log");
    let done_log = tmp.path().join("done.log");
    // The first announce spawns a child; record its exit so we can drain it
    // before the TempDir drops. Under load the fast `exit 0` can miss the 1s
    // probe and be handed to the background wait task, whose fake-sudo exec
    // would otherwise race the TempDir rmdir and log `No such file or directory`.
    let announce_script = format!("#!/bin/sh\necho done >> {}\nexit 0\n", done_log.display());
    let h = build_with_announcer(&announce_script, &record).await;

    let (b1, s1) = signed_announce(&h.secret, "first", now_secs());
    let r1 = reqwest::Client::new()
        .post(format!("{}/announce/river", h.base))
        .header(HEADER_SIGNATURE.as_str(), &s1)
        .body(b1)
        .send()
        .await
        .unwrap();
    assert!(r1.status() == 200 || r1.status() == 202);

    let (b2, s2) = signed_announce(&h.secret, "second", now_secs());
    let r2 = reqwest::Client::new()
        .post(format!("{}/announce/river", h.base))
        .header(HEADER_SIGNATURE.as_str(), &s2)
        .body(b2)
        .send()
        .await
        .unwrap();
    assert_eq!(r2.status(), 429);

    // Only the first announce spawned (the second was rate-limited). Drain it
    // before the TempDir drops so its fake-sudo exec doesn't race the deletion.
    poll_done_count(&done_log, 1).await;
}

#[tokio::test]
async fn oversized_body_rejected() {
    // Defense against a flood of authenticated megabyte bodies — the
    // 4 KiB ceiling rejects them before HMAC verify allocates.
    let h = Harness::build("0.2.55", Version::new(0, 2, 56), 600).await;
    let big = "x".repeat(5 * 1024);
    let body = json!({"version": "0.2.56", "issued_at": now_secs(), "pad": big}).to_string();
    let sig = sign(&h.secret, body.as_bytes());
    let resp = reqwest::Client::new()
        .post(format!("{}/update", h.base))
        .header(HEADER_SIGNATURE.as_str(), &sig)
        .body(body)
        .send()
        .await
        .unwrap();
    assert!(
        resp.status() == 413 || resp.status() == 400,
        "oversized body must be rejected before HMAC, got {}",
        resp.status()
    );
}
