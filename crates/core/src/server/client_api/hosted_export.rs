//! Hosted-mode "export my data" HTTP endpoint (P3-live of #4381).
//!
//! `GET /v{1,2}/hosted/export` lets a hosted user download their per-user
//! delegate secrets — live, from the running node — as a single encrypted P3
//! [`crate::wasm_runtime::secret_export`] bundle (`FNSX` format). The user
//! re-imports it on their own peer with the token they already hold
//! (`freenet secrets import --use-token-key`), completing the
//! hosted → self-host migration.
//!
//! # Why an HTTP-only operation (no new wire variant)
//!
//! The bundle is raw bytes, not a `HostResponse`. Routing it through the
//! WebSocket `ClientRequest`/`HostResponse` path would force a new
//! freenet-stdlib wire-format variant (and the stdlib-first release dance).
//! Instead this is a self-contained freenet-core HTTP operation: the handler
//! runs the export on the node's executor (which owns the `SecretsStore`) via
//! [`crate::contract::ContractHandlerEvent::ExportUserSecrets`] and streams the
//! bytes straight back as an `application/octet-stream` download.
//!
//! # Security
//!
//! The endpoint returns a user's private secrets, so it applies the SAME gate
//! as the WebSocket `userToken` (the refuse-plaintext-token invariant): hosted
//! mode ON + a loopback source + `X-Forwarded-Proto: https`. The token is read
//! from the `X-Freenet-User-Token` HEADER (never a query param, so it cannot
//! land in an access log or the URL). The gate primitives
//! ([`decide_user_token`], [`is_loopback_source`], [`derive_user_context`]) are
//! shared verbatim with the WS middleware — this module only adds the
//! export-specific twist that a no-token / `Local` outcome is REJECTED (an
//! export of "no user scope" is meaningless here), whereas the WS path lets a
//! tokenless connection fall through to `Local`.
//!
//! ## Threat model: the token is the entire secret
//!
//! The per-user DEK and the bundle key are derived SOLELY from the user token
//! and are node-KEK-independent BY DESIGN (export portability — a self-hosting
//! user can decrypt their bundle on a fresh peer that never had the operator's
//! KEK). There is therefore NO node-side second factor: anyone who presents a
//! valid token over a secure connection gets that user's data. This is the
//! intended hosted model (the token already names the per-user namespace and
//! derives the storage DEK), but it means token confidentiality is the whole
//! ballgame — hence the strict refuse-plaintext-token gate above.
//!
//! ## DoS hardening — off-loop execution + per-user bound (#4381 P5)
//!
//! An export enumerates AND AEAD-decrypts EVERY secret in the user's scope. To
//! keep an AUTHENTICATED token-holder (valid token + secure connection) from
//! using a large or repeated export to wedge the node, two guards are in place:
//!
//! 1. **Off-loop execution.** The export no longer runs synchronously on the
//!    single-threaded contract-handling loop. The handler arm
//!    (`ContractHandlerEvent::ExportUserSecrets`) hands off to
//!    `RuntimePool::export_user_secrets`, which runs the synchronous
//!    enumerate+decrypt+re-encrypt on a blocking thread (`spawn_blocking`,
//!    runtime-flavor-gated like the WASM-compile offload of #4441). The loop is
//!    free to drain other PUT/GET/UPDATE/delegate ops while an export runs, so a
//!    single user's export can no longer block the node's contract loop.
//!
//! 2. **Per-user bound.** `secret_export::export_bundle` rejects — BEFORE the
//!    heavy work — any export exceeding `MAX_EXPORT_SECRET_COUNT` or
//!    `MAX_EXPORT_TOTAL_PLAINTEXT_BYTES`, bounding the worst-case work of a
//!    single export. The rejection surfaces here as HTTP 413.
//!
//! What remains for the broader P5 abuse work (#4381): per-user RATE limiting
//! across many requests over time (e.g. N exports/minute) and node-wide export
//! concurrency caps. The two guards above remove the head-of-line blocking and
//! bound a single request; a full quota/rate-limiting system is tracked
//! separately and is not required for the endpoint behind the default-off flag.

use axum::{
    Router,
    extract::ConnectInfo,
    http::{HeaderMap, StatusCode},
    response::{IntoResponse, Response},
    routing::get,
};
use std::net::SocketAddr;
use std::sync::{Arc, OnceLock, Weak};

use crate::client_events::websocket::{UserTokenDecision, decide_user_token, derive_user_context};
use crate::node::OpManager;
use crate::wasm_runtime::UserSecretContext;

use super::ApiVersion;

/// Header carrying the hosted user's token. A HEADER (not a query param) so the
/// high-value credential never lands in the request URI / access logs. Mirrors
/// the WS path, which also accepts `X-Freenet-User-Token` (taking precedence
/// over the `userToken` query form).
const USER_TOKEN_HEADER: &str = "x-freenet-user-token";

/// Download filename + bundle extension. `.fnsx` matches the P3 bundle's `FNSX`
/// magic and the offline `freenet secrets export` default.
const DOWNLOAD_FILENAME: &str = "freenet-data.fnsx";

/// Per-node handle the export HTTP handler uses to reach the executor (which
/// owns the `SecretsStore`) through the contract handler.
///
/// Created and injected into the HTTP router as an `Extension` in
/// [`HttpClientApi::as_router_with_origin_contracts`](super::HttpClientApi)
/// (`client_api.rs`), with the SAME `Arc` also stored on the returned
/// `HttpClientApi`. The node fills it ONCE at startup via
/// [`ClientEventsProxy::set_op_manager`], called from `node::p2p_impl` on each
/// boxed client just before the `ClientEventsCombinator` consumes them — the
/// first place the live `op_manager` meets the client proxies.
///
/// `Weak` so a torn-down node does not keep its `OpManager` alive (and a stale
/// handle self-heals to a 503 rather than a use-after-free). Because each node
/// owns its OWN handle (it is an Extension on that node's router, not a
/// process-global), multiple hosted nodes in one process — e.g. parallel
/// integration tests — never clobber each other. The `OnceLock` makes the wiring
/// write-once (a second `set_op_manager` is ignored).
#[derive(Clone, Default)]
pub(crate) struct ExportOpManagerHandle(Arc<OnceLock<Weak<OpManager>>>);

impl ExportOpManagerHandle {
    /// Wire this handle to the running node's `OpManager`. Idempotent
    /// (write-once): a repeat call is ignored, so node startup can call it
    /// freely without racing.
    pub(crate) fn set(&self, op_manager: &Arc<OpManager>) {
        // OnceLock::set returns Err if already set; the first writer wins. A
        // repeat is benign (same node), so the duplicate is intentionally
        // ignored.
        if self.0.set(Arc::downgrade(op_manager)).is_err() {
            tracing::debug!("export op_manager handle already set; ignoring repeat wiring");
        }
    }

    /// Resolve the live `OpManager`, if the node is up and not torn down.
    fn current(&self) -> Option<Arc<OpManager>> {
        self.0.get().and_then(Weak::upgrade)
    }
}

/// Registers the hosted-export route for `version`. The handler reaches the node
/// through the per-node [`ExportOpManagerHandle`] carried as a request
/// `Extension` (injected in `HttpClientApi::as_router_with_origin_contracts`).
pub(super) fn routes(version: ApiVersion) -> Router {
    let path = format!("/{}/hosted/export", version.prefix());
    Router::new().route(&path, get(export_handler))
}

/// Apply the hosted-mode token gate to an export request and, on success,
/// return the per-user secret context PLUS the raw token bytes (the bundle-key
/// material).
///
/// This reuses the WS gate verbatim ([`decide_user_token`] /
/// [`is_loopback_source`] / [`derive_user_context`]) so the export endpoint and
/// the WebSocket `userToken` honor IDENTICAL security rules. The one
/// difference, intrinsic to export: `Local` (no/empty token) is REJECTED here,
/// because exporting "no user scope" is meaningless — the user must present
/// their token. The WS path instead lets `Local` through (a tokenless
/// connection legitimately uses the single-user namespace).
///
/// On rejection returns `(StatusCode::FORBIDDEN, reason)`; the reason is a
/// fixed, non-secret string (never echoes the token).
pub(crate) fn export_user_context_or_reject(
    req_headers: &HeaderMap,
    source_ip: Option<std::net::IpAddr>,
    hosted_mode: bool,
) -> Result<(UserSecretContext, Vec<u8>), (StatusCode, &'static str)> {
    // Hosted mode is the master switch. Off ⇒ no per-user namespaces exist, so
    // there is nothing to export under a user scope. Reject (do NOT export the
    // node-local secrets to an unauthenticated caller).
    if !hosted_mode {
        return Err((
            StatusCode::FORBIDDEN,
            "hosted-mode export is disabled on this node",
        ));
    }

    // Token from the HEADER only — never a query param (keeps the credential
    // out of the URI / access logs).
    let token = req_headers
        .get(USER_TOKEN_HEADER)
        .and_then(|v| v.to_str().ok())
        .map(str::to_owned);
    let xfp_https = req_headers
        .get("x-forwarded-proto")
        .and_then(|v| v.to_str().ok())
        .is_some_and(|v| v.eq_ignore_ascii_case("https"));

    // `has_token` is a NON-EMPTY check, matching `derive_user_context`'s
    // empty-is-absent rule (an empty header is treated as no token).
    let has_token = token.as_deref().is_some_and(|t| !t.is_empty());

    match decide_user_token(hosted_mode, has_token, source_ip, xfp_https) {
        UserTokenDecision::Honor => {
            // `derive_user_context` returns `Some` here (Honor implies hosted +
            // non-empty token); the token is the bundle-key material.
            let token = token.expect("Honor decision implies a non-empty token");
            let ctx = derive_user_context(hosted_mode, Some(token.as_str()))
                .expect("derive_user_context returns Some for a honored hosted non-empty token");
            Ok((ctx, token.into_bytes()))
        }
        UserTokenDecision::RejectInsecure => Err((
            StatusCode::FORBIDDEN,
            "hosted user token requires a secure (TLS/loopback) connection",
        )),
        UserTokenDecision::Local => Err((
            StatusCode::FORBIDDEN,
            "hosted export requires a user token (X-Freenet-User-Token header)",
        )),
    }
}

/// `GET /v{1,2}/hosted/export` — export this hosted user's per-user delegate
/// secrets as an encrypted bundle download.
///
/// Takes the whole `Request` (rather than typed `Extension`/`ConnectInfo`
/// extractors) so it can read `HostedMode` and the `ConnectInfo<SocketAddr>`
/// source from `extensions()` directly — the same approach `connection_info`
/// uses, and necessary because this crate builds `axum` without the feature
/// that provides the `ConnectInfo` extractor.
///
/// PERFORMANCE / DoS (#4381 P5): the export is bounded (per-user secret-count /
/// byte cap, returned here as 413) AND runs OFF the contract-handling loop on a
/// blocking thread, so a single authenticated token-holder's export can neither
/// do unbounded work nor stall other contract ops. See the module-level
/// "DoS hardening" section for what remains (cross-request rate limiting).
async fn export_handler(req: axum::extract::Request) -> Response {
    // Tolerant: the standalone `as_router` composition path has no `HostedMode`
    // layer, so a missing extension means hosted-off ⇒ the gate below 403s (it
    // never silently exports). The WS gate stays strict (fail-loud) precisely
    // because there a dropped flag could put users on a shared namespace; here a
    // dropped flag only ever DENIES, so failing safe-to-off is correct.
    let hosted_mode = req
        .extensions()
        .get::<crate::server::HostedMode>()
        .map(|hm| hm.0)
        .unwrap_or(false);

    // Source IP from the `ConnectInfo<SocketAddr>` request extension that
    // `into_make_service_with_connect_info::<SocketAddr>()` injects for every
    // connection (kernel-set from the accepted socket; not spoofable off-host).
    // Read from `extensions()` rather than as a typed extractor — same as
    // `connection_info` — so a missing ConnectInfo (only in unit tests that omit
    // it) yields `None` and the gate fails closed (cannot prove loopback).
    let source_ip = req
        .extensions()
        .get::<ConnectInfo<SocketAddr>>()
        .map(|ci| ci.0.ip());

    let req_headers = req.headers();

    let (user_context, token) =
        match export_user_context_or_reject(req_headers, source_ip, hosted_mode) {
            Ok(v) => v,
            Err((status, reason)) => {
                // Do NOT log the token. Log only the non-secret source IP and
                // the rejection reason.
                tracing::warn!(
                    source_ip = ?source_ip,
                    reason,
                    "Rejected hosted export request"
                );
                return (status, reason).into_response();
            }
        };

    // Per-node route to the executor, carried as an Extension and filled by the
    // node at startup. Absent (standalone `as_router` composition with no node)
    // or not-yet-filled (startup race) ⇒ 503.
    let op_manager = req
        .extensions()
        .get::<ExportOpManagerHandle>()
        .and_then(ExportOpManagerHandle::current);
    let Some(op_manager) = op_manager else {
        tracing::warn!("Hosted export requested but no running node is registered");
        return (
            StatusCode::SERVICE_UNAVAILABLE,
            "node is not ready to serve exports",
        )
            .into_response();
    };

    match run_export(&op_manager, user_context, token).await {
        Ok(bundle) => {
            // `Content-Length` is set by axum from the body. Force a download
            // with a stable filename; octet-stream so browsers don't sniff.
            (
                StatusCode::OK,
                [
                    (axum::http::header::CONTENT_TYPE, "application/octet-stream"),
                    (
                        axum::http::header::CONTENT_DISPOSITION,
                        // Static filename, no user input interpolated, so no
                        // header-injection surface.
                        &format!("attachment; filename=\"{DOWNLOAD_FILENAME}\""),
                    ),
                ],
                bundle,
            )
                .into_response()
        }
        Err(status_and_msg) => status_and_msg.into_response(),
    }
}

/// Drive the export on the node's executor and return the bundle bytes.
///
/// The `token` is moved into a redacted, zeroizing wrapper on the handler event
/// so it is wiped after the bundle key is derived and never reaches a log.
async fn run_export(
    op_manager: &OpManager,
    user_context: UserSecretContext,
    token: Vec<u8>,
) -> Result<Vec<u8>, (StatusCode, &'static str)> {
    use crate::contract::ContractHandlerEvent;

    let event = ContractHandlerEvent::ExportUserSecrets {
        user_context,
        token: crate::contract::RedactedToken::new(token),
    };

    // Hosted export is initiated by a local HTTP client → ClientLocal lane (#4534).
    match op_manager
        .notify_contract_handler_prioritized(event, crate::contract::Priority::ClientLocal)
        .await
    {
        Ok(ContractHandlerEvent::ExportUserSecretsResponse(Ok(bundle))) => Ok(bundle),
        Ok(ContractHandlerEvent::ExportUserSecretsResponse(Err(e))) => {
            // An over-limit export is a client condition, not a node fault:
            // surface it as 413 (Payload Too Large) so the caller can tell it
            // apart from a genuine 500. The message is non-secret (sizes only).
            // See the per-user export bound in `secret_export` (#4381 P5).
            if e.is_export_too_large() {
                tracing::warn!(error = %e, "Rejected hosted export: over per-user limit");
                return Err((
                    StatusCode::PAYLOAD_TOO_LARGE,
                    "export exceeds the per-user size limit",
                ));
            }
            // Executor-side failure (e.g. a secret failed to decrypt). Do not
            // leak internals to the client; log the detail, return a generic 500.
            tracing::error!(error = %e, "Hosted export failed on the executor");
            Err((
                StatusCode::INTERNAL_SERVER_ERROR,
                "export failed on the node",
            ))
        }
        Ok(other) => {
            tracing::error!(
                response = %other,
                "Unexpected contract-handler response to ExportUserSecrets"
            );
            Err((
                StatusCode::INTERNAL_SERVER_ERROR,
                "export failed on the node",
            ))
        }
        Err(e) => {
            tracing::error!(error = %e, "Contract handler unavailable for hosted export");
            Err((
                StatusCode::SERVICE_UNAVAILABLE,
                "node is not ready to serve exports",
            ))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::client_events::websocket::is_loopback_source;
    use axum::http::HeaderValue;
    use std::net::{IpAddr, Ipv4Addr, Ipv6Addr};

    fn headers_with(token: Option<&str>, xfp: Option<&str>) -> HeaderMap {
        let mut h = HeaderMap::new();
        if let Some(t) = token {
            h.insert(USER_TOKEN_HEADER, HeaderValue::from_str(t).unwrap());
        }
        if let Some(x) = xfp {
            h.insert("x-forwarded-proto", HeaderValue::from_str(x).unwrap());
        }
        h
    }

    const LOOPBACK: Option<IpAddr> = Some(IpAddr::V4(Ipv4Addr::LOCALHOST));

    #[test]
    fn gate_honors_secure_request_and_returns_token_bytes() {
        let headers = headers_with(Some("tok-abc"), Some("https"));
        let (ctx, token) =
            export_user_context_or_reject(&headers, LOOPBACK, true).expect("must honor");
        // The returned token bytes are the raw header value (the bundle key).
        assert_eq!(token, b"tok-abc");
        // The context scopes to the same token's derived user id.
        let expected = UserSecretContext::from_token(b"tok-abc");
        assert_eq!(ctx.user_id(), expected.user_id());
    }

    #[test]
    fn gate_rejects_when_hosted_mode_off() {
        let headers = headers_with(Some("tok-abc"), Some("https"));
        let err = export_user_context_or_reject(&headers, LOOPBACK, false).unwrap_err();
        assert_eq!(err.0, StatusCode::FORBIDDEN);
    }

    #[test]
    fn gate_rejects_missing_token() {
        let headers = headers_with(None, Some("https"));
        let err = export_user_context_or_reject(&headers, LOOPBACK, true).unwrap_err();
        assert_eq!(err.0, StatusCode::FORBIDDEN);
    }

    #[test]
    fn gate_rejects_empty_token() {
        // An empty header is treated as no token (matches derive_user_context).
        let headers = headers_with(Some(""), Some("https"));
        let err = export_user_context_or_reject(&headers, LOOPBACK, true).unwrap_err();
        assert_eq!(err.0, StatusCode::FORBIDDEN);
    }

    #[test]
    fn gate_rejects_token_without_https() {
        // Loopback but no X-Forwarded-Proto: https ⇒ RejectInsecure.
        let headers = headers_with(Some("tok-abc"), None);
        let err = export_user_context_or_reject(&headers, LOOPBACK, true).unwrap_err();
        assert_eq!(err.0, StatusCode::FORBIDDEN);
    }

    #[test]
    fn gate_rejects_token_from_non_loopback_source() {
        // A token arriving from off-host is never honored, even with XFP:https
        // (the header is client-spoofable from a non-loopback source).
        let headers = headers_with(Some("tok-abc"), Some("https"));
        let public = Some(IpAddr::V4(Ipv4Addr::new(203, 0, 113, 7)));
        let err = export_user_context_or_reject(&headers, public, true).unwrap_err();
        assert_eq!(err.0, StatusCode::FORBIDDEN);
    }

    #[test]
    fn gate_rejects_missing_source_ip() {
        // No ConnectInfo ⇒ cannot prove loopback ⇒ fail closed.
        let headers = headers_with(Some("tok-abc"), Some("https"));
        let err = export_user_context_or_reject(&headers, None, true).unwrap_err();
        assert_eq!(err.0, StatusCode::FORBIDDEN);
    }

    #[test]
    fn gate_honors_ipv4_mapped_loopback() {
        // `::ffff:127.0.0.1` from a dual-stack socket normalizes to loopback.
        let headers = headers_with(Some("tok-abc"), Some("https"));
        let addr = IpAddr::V6(Ipv4Addr::LOCALHOST.to_ipv6_mapped());
        let mapped = Some(addr);
        assert!(is_loopback_source(addr));
        let (_, token) =
            export_user_context_or_reject(&headers, mapped, true).expect("mapped loopback honored");
        assert_eq!(token, b"tok-abc");
    }

    #[test]
    fn gate_rejects_ipv6_non_loopback() {
        let headers = headers_with(Some("tok-abc"), Some("https"));
        let public6 = Some(IpAddr::V6(Ipv6Addr::new(0x2001, 0xdb8, 0, 0, 0, 0, 0, 1)));
        let err = export_user_context_or_reject(&headers, public6, true).unwrap_err();
        assert_eq!(err.0, StatusCode::FORBIDDEN);
    }
}
