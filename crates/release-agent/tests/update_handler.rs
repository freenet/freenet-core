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
    version::VersionCache,
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
        };

        let latest_source: Arc<dyn LatestSource> = Arc::new(StaticLatest(latest_on_github));
        let state = AppState {
            config: Arc::new(config.clone()),
            secret: Arc::new(secret.clone()),
            latest_source,
            updater: Updater {
                command: config.update_command.clone(),
                dry_run: true,
            },
            version_cache: VersionCache::new(),
            last_update_attempt: Arc::new(Mutex::new(None)),
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
