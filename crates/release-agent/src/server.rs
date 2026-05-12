use std::net::SocketAddr;
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::Result;
use axum::{
    Json, Router,
    body::Bytes,
    extract::{DefaultBodyLimit, State},
    http::{HeaderMap, StatusCode},
    response::{IntoResponse, Response},
    routing::{get, post},
};
use semver::Version;
use serde::{Deserialize, Serialize};
use tokio::sync::Mutex;

use crate::auth::{HEADER_SIGNATURE, check_clock_skew, verify_signature};
use crate::config::Config;
use crate::github::LatestSource;
use crate::updater::Updater;
use crate::version::VersionCache;

/// Cap on the request body for `POST /update`. The legitimate body is ~60
/// bytes (a small JSON object). 4 KiB is generous and rejects megabyte
/// floods before they reach HMAC verification.
const UPDATE_BODY_LIMIT_BYTES: usize = 4 * 1024;

#[derive(Clone)]
pub struct AppState {
    pub config: Arc<Config>,
    pub secret: Arc<Vec<u8>>,
    pub latest_source: Arc<dyn LatestSource>,
    pub updater: Updater,
    pub version_cache: VersionCache,
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

/// Successful response from `POST /update`.
///
/// **Caller contract**: a 200/202 response with `no_op == true` means the
/// gateway was already on `target_version`; the agent did NOT spawn the
/// update script. Callers that poll `/version` to confirm the upgrade must
/// inspect `no_op` to distinguish "already there" from "just kicked off".
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
        .route(
            "/update",
            post(update_handler).layer(DefaultBodyLimit::max(UPDATE_BODY_LIMIT_BYTES)),
        )
        .with_state(state)
}

async fn version_handler(State(state): State<AppState>) -> Response {
    match state.version_cache.current(&state.config.binary_path).await {
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
    let sig = match headers.get(&HEADER_SIGNATURE).and_then(|v| v.to_str().ok()) {
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

    // 3. Replay protection (rejects non-positive issued_at as well)
    if let Err(e) = check_clock_skew(req.issued_at, state.config.clock_skew_tolerance_seconds) {
        return (StatusCode::UNAUTHORIZED, format!("{e}")).into_response();
    }

    // 4. Parse target
    let target = match Version::parse(&req.version) {
        Ok(v) => v,
        Err(e) => return (StatusCode::BAD_REQUEST, format!("bad version: {e}")).into_response(),
    };

    // 5. Read current
    let current = match state.version_cache.current(&state.config.binary_path).await {
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

    // 6. No-op (already on the requested version)
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

    // 8. Independent verification: target must equal GitHub `latest`. A
    //    leaked HMAC token cannot install an arbitrary version this way.
    let latest = match state.latest_source.fetch_latest().await {
        Ok(v) => v,
        Err(e) => {
            tracing::error!(error = %e, "GitHub latest lookup failed");
            // Note: rate-limit window is NOT touched here, so a transient
            // GitHub outage does not lock out the legitimate retry.
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

    // 9. Rate limit + spawn under a single lock. Holding the mutex across
    //    the spawn serialises concurrent requests, and we only mark the
    //    window consumed on actual spawn-success — so a transient GitHub
    //    5xx or an immediate sudo failure does NOT consume the window.
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

    if let Err(e) = state.updater.run(&target).await {
        tracing::error!(error = %e, "updater spawn failed");
        // Deliberately do NOT update last_update_attempt — the caller
        // should be able to retry once the underlying problem is fixed,
        // without waiting out the full rate-limit window.
        return (
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("update command failed: {e}"),
        )
            .into_response();
    }
    *guard = Some(Instant::now());
    drop(guard);

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
