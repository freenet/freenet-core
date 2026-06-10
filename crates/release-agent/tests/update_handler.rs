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
    // failure within the 1s probe window must NOT mark the rate-limit
    // window consumed — otherwise an attacker (or a transient sudoers
    // misconfig) could lock out legitimate retries for the full window.
    let tmp = tempfile::tempdir().unwrap();
    let argv_record = tmp.path().join("argv.log");
    // Update script exits 1 immediately. Surface as 500 to the caller.
    let h = Harness::build_live(
        "0.2.55",
        Version::new(0, 2, 56),
        600, // 10 min window — long enough that a second successful spawn
        // within seconds proves the window WAS NOT consumed.
        "#!/bin/sh\nexit 1\n",
        &argv_record,
    )
    .await;

    let (body, sig) = h.signed_body("0.2.56", now_secs());
    let r1 = reqwest::Client::new()
        .post(format!("{}/update", h.base))
        .header(HEADER_SIGNATURE.as_str(), &sig)
        .body(body)
        .send()
        .await
        .unwrap();
    assert_eq!(r1.status(), 500, "spawn failure should surface as 500");

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
    assert_ne!(
        r2.status(),
        429,
        "rate-limit window must not be consumed by a failed spawn"
    );
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
    // Update script does nothing; we only care about the argv sudo sees.
    let h = Harness::build_live(
        "0.2.55",
        Version::new(0, 2, 56),
        600,
        "#!/bin/sh\n# noop — wait long enough that the 1s probe times out\nsleep 5\n",
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

    let recorded =
        std::fs::read_to_string(&argv_record).expect("fake-sudo should have recorded argv");
    let argv = recorded.trim();
    // Expected: <update_script_path> --force --target-version v0.2.56
    assert!(
        argv.ends_with("--force --target-version v0.2.56"),
        "argv suffix must match sudoers allowlist (got: {argv:?})"
    );
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
    let h = build_with_announcer("#!/bin/sh\nsleep 5\n", &record).await;
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
    let recorded = std::fs::read_to_string(&record).expect("fake-sudo recorded argv");
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
    let tmp = tempfile::tempdir().unwrap();
    let record = tmp.path().join("argv.log");
    // Script exits 1 immediately → 500 + window NOT consumed.
    let h = build_with_announcer("#!/bin/sh\nexit 1\n", &record).await;
    let (body, sig) = signed_announce(&h.secret, "first", now_secs());
    let r1 = reqwest::Client::new()
        .post(format!("{}/announce/river", h.base))
        .header(HEADER_SIGNATURE.as_str(), &sig)
        .body(body)
        .send()
        .await
        .unwrap();
    assert_eq!(r1.status(), 500);

    let (body2, sig2) = signed_announce(&h.secret, "second", now_secs());
    let r2 = reqwest::Client::new()
        .post(format!("{}/announce/river", h.base))
        .header(HEADER_SIGNATURE.as_str(), &sig2)
        .body(body2)
        .send()
        .await
        .unwrap();
    assert_ne!(
        r2.status(),
        429,
        "failed spawn must not consume the announce rate-limit window"
    );
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
    let h = build_with_announcer("#!/bin/sh\nsleep 5\n", &record).await;
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
    let h = build_with_announcer("#!/bin/sh\nexit 0\n", &record).await;

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
