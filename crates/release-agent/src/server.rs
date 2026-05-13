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

use crate::announcer::Announcer;
use crate::auth::{HEADER_SIGNATURE, check_clock_skew, verify_signature};
use crate::config::Config;
use crate::github::LatestSource;
use crate::updater::Updater;
use crate::version::VersionCache;

/// Cap on the request body for `POST /update`. The legitimate body is ~60
/// bytes (a small JSON object). 4 KiB is generous and rejects megabyte
/// floods before they reach HMAC verification.
const UPDATE_BODY_LIMIT_BYTES: usize = 4 * 1024;

/// Cap on `POST /announce/river`. River chat messages are typically
/// 100-500 bytes; 8 KiB leaves room for multi-line release notes while
/// still rejecting outright floods.
const ANNOUNCE_BODY_LIMIT_BYTES: usize = 8 * 1024;

/// Cap on announcement payload text. Prevents an authenticated client
/// from posting megabyte-sized "messages" that would still pass the
/// body limit at the HTTP layer.
const ANNOUNCE_MESSAGE_MAX_LEN: usize = 4 * 1024;

#[derive(Clone)]
pub struct AppState {
    pub config: Arc<Config>,
    pub secret: Arc<Vec<u8>>,
    pub latest_source: Arc<dyn LatestSource>,
    pub updater: Updater,
    pub announcer: Announcer,
    pub version_cache: VersionCache,
    pub last_update_attempt: Arc<Mutex<Option<Instant>>>,
    pub last_announce_attempt: Arc<Mutex<Option<Instant>>>,
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

/// Request body for `POST /announce/river`.
#[derive(Deserialize, Serialize)]
pub struct AnnounceRequest {
    pub message: String,
    pub issued_at: i64,
}

/// Successful response from `POST /announce/river`.
#[derive(Serialize)]
pub struct AnnounceResponse {
    pub accepted: bool,
    pub dry_run: bool,
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
        .route(
            "/announce/river",
            post(announce_river_handler).layer(DefaultBodyLimit::max(ANNOUNCE_BODY_LIMIT_BYTES)),
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
    //
    //    Acquire with a timeout so a stuck spawn doesn't deadlock all
    //    future requests forever. The probe duration in Updater::run is
    //    1s; allowing 5s here gives 5× headroom while still bounding the
    //    blast radius of a flood of authenticated requests.
    let mut guard = match tokio::time::timeout(
        Duration::from_secs(5),
        state.last_update_attempt.lock(),
    )
    .await
    {
        Ok(g) => g,
        Err(_) => {
            tracing::warn!("update mutex acquire timed out; spawn likely in progress");
            return (
                StatusCode::SERVICE_UNAVAILABLE,
                "update in progress; retry shortly",
            )
                .into_response();
        }
    };
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

async fn announce_river_handler(
    State(state): State<AppState>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    // Endpoint is opt-in via config. Off-by-default means a vega-style
    // install (no Freenet node, no signing key) returns 503 instead of
    // pretending to succeed.
    if !state.announcer.is_configured() {
        return (
            StatusCode::SERVICE_UNAVAILABLE,
            "river announcements not configured on this gateway",
        )
            .into_response();
    }

    // 1. HMAC over raw body (same secret as /update for now; separation
    //    can come later if the operator wants per-endpoint secrets).
    let sig = match headers.get(&HEADER_SIGNATURE).and_then(|v| v.to_str().ok()) {
        Some(s) => s,
        None => return (StatusCode::UNAUTHORIZED, "missing X-Signature").into_response(),
    };
    if let Err(e) = verify_signature(&state.secret, &body, sig) {
        tracing::warn!(error = %e, "announce signature verification failed");
        return (StatusCode::UNAUTHORIZED, "invalid signature").into_response();
    }

    // 2. Parse body
    let req: AnnounceRequest = match serde_json::from_slice(&body) {
        Ok(r) => r,
        Err(e) => return (StatusCode::BAD_REQUEST, format!("bad body: {e}")).into_response(),
    };

    // 3. Replay protection
    if let Err(e) = check_clock_skew(req.issued_at, state.config.clock_skew_tolerance_seconds) {
        return (StatusCode::UNAUTHORIZED, format!("{e}")).into_response();
    }

    // 4. Sanity-check the message itself. Reject empty, oversize, or
    //    control-character payloads before paying for the sudo spawn.
    if req.message.is_empty() {
        return (StatusCode::BAD_REQUEST, "message must not be empty").into_response();
    }
    if req.message.len() > ANNOUNCE_MESSAGE_MAX_LEN {
        return (
            StatusCode::PAYLOAD_TOO_LARGE,
            format!(
                "message too long ({} bytes; max {})",
                req.message.len(),
                ANNOUNCE_MESSAGE_MAX_LEN
            ),
        )
            .into_response();
    }
    if req.message.chars().any(|c| c == '\0') {
        return (
            StatusCode::BAD_REQUEST,
            "message must not contain NUL bytes",
        )
            .into_response();
    }

    // 5. Rate-limit + spawn. 1-minute window: announcements are
    //    idempotent-ish from the caller's view (CI re-running would
    //    just re-announce), but rate-limiting prevents accidental
    //    storms from a stuck workflow.
    let mut guard = match tokio::time::timeout(
        Duration::from_secs(5),
        state.last_announce_attempt.lock(),
    )
    .await
    {
        Ok(g) => g,
        Err(_) => {
            return (StatusCode::SERVICE_UNAVAILABLE, "announce in progress").into_response();
        }
    };
    if let Some(prev) = *guard {
        if prev.elapsed() < Duration::from_secs(60) {
            return (
                StatusCode::TOO_MANY_REQUESTS,
                "rate-limited; announcements throttled to 1/min",
            )
                .into_response();
        }
    }

    if let Err(e) = state.announcer.run(&req.message).await {
        tracing::error!(error = %e, "announcer spawn failed");
        // Same contract as /update: failure does NOT consume the window.
        return (
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("announce command failed: {e}"),
        )
            .into_response();
    }
    *guard = Some(Instant::now());
    drop(guard);

    let status = if state.config.dry_run {
        StatusCode::OK
    } else {
        StatusCode::ACCEPTED
    };
    (
        status,
        Json(AnnounceResponse {
            accepted: true,
            dry_run: state.config.dry_run,
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
