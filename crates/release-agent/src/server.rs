use std::net::SocketAddr;
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::Result;
use axum::{
    Json, Router,
    body::Bytes,
    extract::State,
    http::{HeaderMap, StatusCode},
    response::{IntoResponse, Response},
    routing::{get, post},
};
use reqwest::Client as HttpClient;
use semver::Version;
use serde::{Deserialize, Serialize};
use tokio::sync::Mutex;

use crate::auth::{HEADER_SIGNATURE, check_clock_skew, verify_signature};
use crate::config::Config;
use crate::github::fetch_latest_version;
use crate::updater::Updater;
use crate::version::current_version;

#[derive(Clone)]
pub struct AppState {
    pub config: Arc<Config>,
    pub secret: Arc<Vec<u8>>,
    pub http: HttpClient,
    pub updater: Updater,
    pub last_update_attempt: Arc<Mutex<Option<Instant>>>,
}

#[derive(Serialize)]
pub struct VersionResponse {
    pub version: String,
    pub binary_path: String,
}

#[derive(Deserialize, Serialize)]
pub struct UpdateRequest {
    pub version: String,
    pub issued_at: i64,
}

#[derive(Serialize)]
pub struct UpdateResponse {
    pub accepted: bool,
    pub dry_run: bool,
    pub current_version: String,
    pub target_version: String,
    pub no_op: bool,
}

pub fn build_router(state: AppState) -> Router {
    Router::new()
        .route("/healthz", get(|| async { "ok" }))
        .route("/version", get(version_handler))
        .route("/update", post(update_handler))
        .with_state(state)
}

async fn version_handler(State(state): State<AppState>) -> Response {
    match current_version(&state.config.binary_path).await {
        Ok(v) => Json(VersionResponse {
            version: v.to_string(),
            binary_path: state.config.binary_path.display().to_string(),
        })
        .into_response(),
        Err(e) => {
            tracing::warn!(error = %e, "failed to read freenet --version");
            (
                StatusCode::SERVICE_UNAVAILABLE,
                format!("version unavailable: {e}"),
            )
                .into_response()
        }
    }
}

async fn update_handler(
    State(state): State<AppState>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    // 1. HMAC over raw body — never deserialize untrusted input first.
    let sig = match headers.get(HEADER_SIGNATURE).and_then(|v| v.to_str().ok()) {
        Some(s) => s,
        None => return (StatusCode::UNAUTHORIZED, "missing X-Signature").into_response(),
    };
    if let Err(e) = verify_signature(&state.secret, &body, sig) {
        tracing::warn!(error = %e, "signature verification failed");
        return (StatusCode::UNAUTHORIZED, "invalid signature").into_response();
    }

    // 2. Parse body
    let req: UpdateRequest = match serde_json::from_slice(&body) {
        Ok(r) => r,
        Err(e) => return (StatusCode::BAD_REQUEST, format!("bad body: {e}")).into_response(),
    };

    // 3. Replay protection
    if let Err(e) = check_clock_skew(req.issued_at, state.config.clock_skew_tolerance_seconds) {
        return (StatusCode::UNAUTHORIZED, format!("{e}")).into_response();
    }

    // 4. Parse target
    let target = match Version::parse(&req.version) {
        Ok(v) => v,
        Err(e) => return (StatusCode::BAD_REQUEST, format!("bad version: {e}")).into_response(),
    };

    // 5. Read current
    let current = match current_version(&state.config.binary_path).await {
        Ok(v) => v,
        Err(e) => {
            tracing::error!(error = %e, "current version read failed");
            return (
                StatusCode::SERVICE_UNAVAILABLE,
                "cannot read current version",
            )
                .into_response();
        }
    };

    // 6. No-op
    if target == current {
        return Json(UpdateResponse {
            accepted: true,
            dry_run: state.config.dry_run,
            current_version: current.to_string(),
            target_version: target.to_string(),
            no_op: true,
        })
        .into_response();
    }

    // 7. Refuse downgrade
    if target < current {
        return (
            StatusCode::FORBIDDEN,
            format!("refusing to downgrade from {current} to {target}"),
        )
            .into_response();
    }

    // 8. Rate limit (cheaper than the GitHub round-trip below; check first)
    {
        let mut guard = state.last_update_attempt.lock().await;
        if let Some(prev) = *guard {
            let since = prev.elapsed();
            let limit = Duration::from_secs(state.config.rate_limit_seconds);
            if since < limit {
                let remaining = (limit - since).as_secs();
                return (
                    StatusCode::TOO_MANY_REQUESTS,
                    format!("rate-limited; retry in {remaining}s"),
                )
                    .into_response();
            }
        }
        *guard = Some(Instant::now());
    }

    // 9. Independent verification: target must equal GitHub `latest`. A
    //    leaked HMAC token cannot install an arbitrary version this way.
    let latest = match fetch_latest_version(&state.http, &state.config.github_repo).await {
        Ok(v) => v,
        Err(e) => {
            tracing::error!(error = %e, "GitHub latest lookup failed");
            return (StatusCode::BAD_GATEWAY, "GitHub lookup failed").into_response();
        }
    };
    if target != latest {
        return (
            StatusCode::FORBIDDEN,
            format!("requested version {target} does not match GitHub latest {latest}"),
        )
            .into_response();
    }

    // 10. Run (or pretend to, in dry-run)
    if let Err(e) = state.updater.run().await {
        tracing::error!(error = %e, "updater spawn failed");
        return (
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("spawn failed: {e}"),
        )
            .into_response();
    }

    tracing::info!(
        target = %target,
        current = %current,
        dry_run = state.config.dry_run,
        "update accepted"
    );

    let status = if state.config.dry_run {
        StatusCode::OK
    } else {
        StatusCode::ACCEPTED
    };
    (
        status,
        Json(UpdateResponse {
            accepted: true,
            dry_run: state.config.dry_run,
            current_version: current.to_string(),
            target_version: target.to_string(),
            no_op: false,
        }),
    )
        .into_response()
}

pub async fn serve(addr: SocketAddr, router: Router) -> Result<()> {
    let listener = tokio::net::TcpListener::bind(addr).await?;
    tracing::info!(%addr, "release-agent listening");
    axum::serve(listener, router).await?;
    Ok(())
}
