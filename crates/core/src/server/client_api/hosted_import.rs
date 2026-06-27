//! Live "import my data" HTTP endpoint (P3-live of #4592, CHUNK 1).
//!
//! `POST /v{1,2}/import` writes delegate secrets from an encrypted P3
//! [`crate::wasm_runtime::secret_export`] bundle (`FNSX` format) into THIS
//! node's local single-user [`crate::wasm_runtime::secrets_store::SecretsStore`]
//! — LIVE, while the node keeps running. It is the durable counterpart of the
//! live EXPORT (`hosted_export`) and the backend for both the magic-link pull
//! and the file-upload fallback of #4592.
//!
//! # Why route through the running node (not open ReDb directly)
//!
//! The offline `freenet secrets import` CLI opens the secrets ReDb directly,
//! which is single-writer — so it requires the node to be STOPPED. This endpoint
//! instead submits a [`crate::contract::ContractHandlerEvent::ImportSecrets`]
//! event to the running node's contract loop, which routes it to the ONE
//! legitimate `SecretsStore` (owned by a pooled executor). Unlike the read-only
//! export (which runs off-loop), the import runs ON the contract loop (see
//! `RuntimePool::import_secrets`): it WRITES, and the store write path assumes
//! node-wide write serialization, so on-loop keeps every secret write in one
//! serialization domain (with delegate `store_secret`). The brief loop-block is
//! acceptable because this endpoint is loopback + dashboard-gated (a one-shot
//! operator migration), not the authenticated-remote DoS surface of the export.
//!
//! # Security — the import GATE (fail-closed)
//!
//! This endpoint WRITES secrets, so it is the highest-value write surface in the
//! HTTP API. The receiving node is a NORMAL single-user node (hosted mode OFF)
//! importing into [`crate::wasm_runtime::secrets_store::SecretScope::Local`], so
//! it does NOT reuse the hosted-export token gate (which needs hosted ON + a
//! per-user token). Instead it must be reachable ONLY from the node's own
//! dashboard/shell origin over loopback — NEVER from a per-contract `AuthToken`
//! origin or a sandboxed contract iframe. The gate ([`import_gate_or_reject`])
//! enforces, in order:
//!
//! 1. **Loopback source IP**, read from the kernel-set `ConnectInfo<SocketAddr>`
//!    (not spoofable off-host). A MISSING `ConnectInfo` (only in unit tests that
//!    omit it) fails CLOSED — we cannot prove loopback, so we reject. Mirrors
//!    the export handler's source-IP read.
//! 2. **Trusted dashboard/shell origin**, via the SAME
//!    [`super::permission_prompts::is_origin_trusted`] decision the
//!    permission-prompt CSRF guard uses: the `Origin` header must be present and
//!    match a loopback / LAN-CIDR / allowed-host origin. That helper REJECTS
//!    `Origin: null`, which is exactly what a sandboxed contract iframe presents
//!    (the sandbox omits `allow-same-origin`), so the per-contract iframe is
//!    excluded here.
//! 3. **No per-contract `AuthToken`** (best-effort defense-in-depth, NOT the
//!    primary iframe exclusion). A per-contract webapp's shell page is
//!    same-origin with the node, so it would pass check #2; it carries a
//!    per-contract `AuthToken` registered in the [`OriginContractMap`], while the
//!    node's own first-party dashboard carries none. So a request presenting a
//!    registered origin-contract token — via the `Authorization` bearer header,
//!    the `authToken` query param, or the `Authorization` cookie — is rejected.
//!    But this only CLASSIFIES a *well-behaved* client that actually presents its
//!    token: a hostile script could simply omit the token to evade this check, so
//!    #3 cannot be relied on to exclude a malicious contract web app on its own.
//!
//! The load-bearing exclusion of sandboxed contract iframes is **check #2**: the
//! sandbox omits `allow-same-origin`, so the iframe presents `Origin: null`,
//! which `is_origin_trusted` rejects. Check #3 is an extra layer that catches a
//! cooperative same-origin shell page; it does NOT backstop #2. A future
//! maintainer MUST NOT weaken check #2 (e.g. start admitting `Origin: null` or a
//! cross-site origin) on the belief that #3 will still keep contract web apps
//! out — it will not.
//!
//! The bundle's decryption key is the real authorization for the DATA: a caller
//! who passes the gate still cannot import anything they cannot decrypt. The
//! decrypt is all-or-nothing and happens BEFORE any write
//! ([`crate::wasm_runtime::secret_export::import_bundle`] → `open_bundle`), so a
//! wrong key surfaces as a 4xx with NOTHING written — never a 500, never a
//! partial write. The key material rides in a HEADER (never the URL/logs), in a
//! redacted/zeroizing wrapper, and is never logged.

use axum::{
    Json, Router,
    extract::{ConnectInfo, DefaultBodyLimit},
    http::{HeaderMap, StatusCode},
    response::{IntoResponse, Response},
    routing::post,
};
use std::net::{IpAddr, SocketAddr};

use crate::client_events::AuthToken;
use crate::client_events::websocket::is_loopback_source;
use crate::contract::{
    BundleKeyKind, ContractHandlerEvent, ImportBundle, ImportTargetScope, Priority, RedactedToken,
};
use crate::node::OpManager;
use crate::server::{AllowedHosts, AllowedSourceCidrs};
use crate::wasm_runtime::secret_export::MAX_EXPORT_TOTAL_PLAINTEXT_BYTES;

use super::hosted_export::ExportOpManagerHandle;
use super::permission_prompts::is_origin_trusted;
use super::{ApiVersion, OriginContractMap};

/// Header carrying the bundle decryption key (the token, or passphrase). A
/// HEADER (not a query param) so this high-value secret never lands in the
/// request URI / access logs — same discipline as the export's
/// `X-Freenet-User-Token`.
const BUNDLE_KEY_HEADER: &str = "x-freenet-bundle-key";

/// Header selecting how to interpret [`BUNDLE_KEY_HEADER`]: `token` (default,
/// the hosted-export form) or `passphrase` (the offline `--passphrase` form).
const BUNDLE_KEY_KIND_HEADER: &str = "x-freenet-bundle-key-kind";

/// Header controlling collision handling: `true` overwrites an existing secret,
/// anything else (default) skips + reports it. Off unless explicitly opted in.
const OVERWRITE_HEADER: &str = "x-freenet-import-overwrite";

/// `authToken` cookie / bearer name used by the per-contract web shell (see
/// `client_api.rs` — the cookie is set under the `Authorization` header name,
/// lowercased on the wire).
const AUTHORIZATION_NAME: &str = "authorization";

/// Request-body cap for the uploaded bundle, sized to accept ANY bundle the
/// existing `export_bundle` can produce (so a valid max-size export is never
/// rejected with a spurious 413), while still bounding what one import buffers.
///
/// The export limit ([`MAX_EXPORT_TOTAL_PLAINTEXT_BYTES`]) counts DECRYPTED
/// plaintext bytes, but the bundle body is CBOR-serialized BEFORE encryption and
/// `secret_export` encodes each secret's `Vec<u8>` as a CBOR ARRAY OF INTEGERS,
/// not a byte string (a deliberate choice — see its `BundleEntry` note), so a
/// plaintext byte costs up to 2 bytes on the wire. A legitimate export can thus
/// serialize to ~2x its plaintext, plus per-entry delegate-key/code-hash/
/// secret-hash CBOR arrays (bounded by `MAX_EXPORT_SECRET_COUNT`) and the small
/// header + AEAD tag. We size the cap to `2x plaintext + 64 MiB` — the doubling
/// covers the CBOR integer-array overhead and the 64 MiB margin generously
/// covers the per-entry key/hash overhead (~200 B x up to 10k entries) plus
/// framing. Over the cap → HTTP 413.
const MAX_IMPORT_BUNDLE_BYTES: usize = 2 * MAX_EXPORT_TOTAL_PLAINTEXT_BYTES + 64 * 1024 * 1024;

/// Registers the live-import route for `version`. The handler reaches the node
/// through the per-node [`ExportOpManagerHandle`] carried as a request
/// `Extension` (the SAME handle the export endpoint uses, injected in
/// `HttpClientApi::as_router_with_origin_contracts`). A [`DefaultBodyLimit`] of
/// [`MAX_IMPORT_BUNDLE_BYTES`] bounds the uploaded bundle.
pub(super) fn routes(version: ApiVersion) -> Router {
    let path = format!("/{}/import", version.prefix());
    Router::new()
        .route(&path, post(import_handler))
        .layer(DefaultBodyLimit::max(MAX_IMPORT_BUNDLE_BYTES))
}

/// Apply the live-import gate. Pure + unit-testable: takes the request's headers,
/// the kernel-set source IP, the host/CIDR allowlists, the (raw) query string,
/// and the origin-contract map, and decides whether the request may import.
///
/// On rejection returns `(StatusCode::FORBIDDEN, reason)` with a fixed,
/// non-secret reason string (never echoes the key or any token). See the module
/// docs for the three layered checks. FAIL-CLOSED throughout.
fn import_gate_or_reject(
    headers: &HeaderMap,
    source_ip: Option<IpAddr>,
    allowed_hosts: Option<&AllowedHosts>,
    allowed_source_cidrs: Option<&AllowedSourceCidrs>,
    query: Option<&str>,
    origin_contracts: &OriginContractMap,
) -> Result<(), (StatusCode, &'static str)> {
    // (1) Loopback source. A missing ConnectInfo cannot prove loopback ⇒ reject
    // (fail closed), mirroring the export handler.
    let Some(ip) = source_ip else {
        return Err((
            StatusCode::FORBIDDEN,
            "import requires a loopback connection",
        ));
    };
    if !is_loopback_source(ip) {
        return Err((
            StatusCode::FORBIDDEN,
            "import requires a loopback connection",
        ));
    }

    // (2) Trusted dashboard/shell origin. The Origin header must be present and
    // pass the shared `is_origin_trusted` decision, which rejects `Origin: null`
    // (the sandboxed contract iframe) and cross-site origins, and requires a
    // loopback / LAN-CIDR / allowed-host origin.
    let Some(origin) = headers.get("origin").and_then(|v| v.to_str().ok()) else {
        return Err((
            StatusCode::FORBIDDEN,
            "import requires a trusted same-origin request (missing Origin)",
        ));
    };
    if !is_origin_trusted(headers, origin, allowed_hosts, allowed_source_cidrs) {
        return Err((
            StatusCode::FORBIDDEN,
            "import is only available from the node's own dashboard origin",
        ));
    }

    // (3) Exclude per-contract AuthToken origins. The node's first-party
    // dashboard carries no per-contract token; a per-contract web shell does.
    // Reject if ANY presented credential (bearer header / authToken query /
    // Authorization cookie) is a registered origin-contract token.
    if presented_tokens(headers, query).any(|tok| origin_contracts.contains_key(&tok)) {
        return Err((
            StatusCode::FORBIDDEN,
            "import is not available to contract web apps",
        ));
    }

    // DEFERRED (Sec-Fetch-Site hardening, #4592 follow-up): browsers attach
    // `Sec-Fetch-Site: same-origin` to a first-party fetch and `cross-site` to a
    // cross-origin one. Checking it would add a fetch-metadata CSRF layer on top
    // of the Origin gate (#2). We do NOT enforce it here on purpose: the header
    // is browser-only, so a legitimate non-browser client (a future
    // `freenet secrets import --live` CLI, curl in a migration script) omits it
    // entirely. "Reject when missing" would break those clients; "enforce only
    // when present" is weaker and needs a deliberate call. Left as a follow-up
    // decision rather than a silent default. Until then the Origin gate (#2) is
    // the CSRF boundary.

    Ok(())
}

/// Collect every `AuthToken` candidate the request presents, across the three
/// channels a per-contract web shell could carry one on: the `Authorization`
/// bearer header, the `authToken` query param, and the `Authorization` cookie
/// (value `Bearer <token>`). Pure (no map lookup) so it is unit-testable on its
/// own; the gate checks each against the origin-contract map.
fn presented_tokens<'a>(
    headers: &'a HeaderMap,
    query: Option<&'a str>,
) -> impl Iterator<Item = AuthToken> + 'a {
    let mut tokens: Vec<AuthToken> = Vec::new();

    // (a) Authorization: Bearer <token>
    if let Some(v) = headers
        .get(axum::http::header::AUTHORIZATION)
        .and_then(|v| v.to_str().ok())
        && let Some(tok) = strip_bearer(v)
    {
        tokens.push(AuthToken::from(tok.to_owned()));
    }

    // (b) ?authToken=<token>
    if let Some(q) = query {
        for (k, val) in url_query_pairs(q) {
            if k == "authToken" && !val.is_empty() {
                tokens.push(AuthToken::from(val));
            }
        }
    }

    // (c) Cookie: authorization=Bearer <token> (the per-contract shell cookie).
    if let Some(cookies) = headers
        .get(axum::http::header::COOKIE)
        .and_then(|v| v.to_str().ok())
    {
        for part in cookies.split(';') {
            let part = part.trim();
            if let Some((name, value)) = part.split_once('=')
                && name.trim().eq_ignore_ascii_case(AUTHORIZATION_NAME)
            {
                // The value may be `Bearer <tok>` (possibly quoted / %20-encoded
                // for the space). Strip a Bearer prefix in any of those forms;
                // otherwise treat the whole value as the token.
                let value = value.trim().trim_matches('"');
                let tok = strip_bearer(value)
                    .or_else(|| value.strip_prefix("Bearer%20"))
                    .unwrap_or(value);
                if !tok.is_empty() {
                    tokens.push(AuthToken::from(tok.to_owned()));
                }
            }
        }
    }

    tokens.into_iter()
}

/// Strip a `Bearer ` (case-insensitive, single ASCII space) prefix from an
/// `Authorization` value, returning the bare token. `None` if the value is not a
/// bearer credential.
fn strip_bearer(value: &str) -> Option<&str> {
    let rest = value.strip_prefix("Bearer ").or_else(|| {
        value
            .get(..7)
            .filter(|p| p.eq_ignore_ascii_case("Bearer "))
            .map(|_| &value[7..])
    })?;
    Some(rest.trim())
}

/// Minimal `application/x-www-form-urlencoded` query splitter for the `authToken`
/// lookup. Does NOT percent-decode (the gate only needs to MATCH a token, and a
/// percent-encoded token would simply not match a registered one — fail safe).
fn url_query_pairs(query: &str) -> impl Iterator<Item = (&str, String)> {
    query.split('&').filter_map(|pair| {
        let (k, v) = pair.split_once('=')?;
        Some((k, v.to_owned()))
    })
}

/// Parse the bundle-key-kind header. Default [`BundleKeyKind::Token`] (the
/// hosted-export form). An unrecognized value is a client error (400).
fn parse_key_kind(headers: &HeaderMap) -> Result<BundleKeyKind, (StatusCode, &'static str)> {
    match headers
        .get(BUNDLE_KEY_KIND_HEADER)
        .and_then(|v| v.to_str().ok())
    {
        None => Ok(BundleKeyKind::Token),
        Some(s) if s.eq_ignore_ascii_case("token") => Ok(BundleKeyKind::Token),
        Some(s) if s.eq_ignore_ascii_case("passphrase") => Ok(BundleKeyKind::Passphrase),
        Some(_) => Err((
            StatusCode::BAD_REQUEST,
            "X-Freenet-Bundle-Key-Kind must be 'token' or 'passphrase'",
        )),
    }
}

/// `POST /v{1,2}/import` — import delegate secrets from an encrypted bundle into
/// this node's Local scope, live.
///
/// Takes the whole `Request` (rather than typed extractors) so it can read the
/// `ConnectInfo<SocketAddr>` source and the host/CIDR/origin-contract extensions
/// from `extensions()` directly — the same approach the export handler uses, and
/// necessary because this crate builds `axum` without the `ConnectInfo`
/// extractor feature. The body is read AFTER the gate + readiness checks so an
/// unauthorized or not-ready request never makes the node buffer a large bundle.
async fn import_handler(req: axum::extract::Request) -> Response {
    let (parts, body) = req.into_parts();
    let headers = &parts.headers;

    // Source IP from the kernel-set ConnectInfo extension; missing ⇒ fail closed.
    let source_ip = parts
        .extensions
        .get::<ConnectInfo<SocketAddr>>()
        .map(|ci| ci.0.ip());

    let allowed_hosts = parts.extensions.get::<AllowedHosts>();
    let allowed_source_cidrs = parts.extensions.get::<AllowedSourceCidrs>();
    // Origin-contract map is always present (layered in
    // `as_router_with_origin_contracts`); absent only in a standalone test
    // composition, in which case there are no per-contract tokens to exclude.
    let empty_origin_contracts: OriginContractMap = std::sync::Arc::new(dashmap::DashMap::new());
    let origin_contracts = parts
        .extensions
        .get::<OriginContractMap>()
        .unwrap_or(&empty_origin_contracts);

    if let Err((status, reason)) = import_gate_or_reject(
        headers,
        source_ip,
        allowed_hosts,
        allowed_source_cidrs,
        parts.uri.query(),
        origin_contracts,
    ) {
        // Never log the key. Log only the non-secret source IP + reason.
        tracing::warn!(source_ip = ?source_ip, reason, "Rejected live import request");
        return (status, reason).into_response();
    }

    // Bundle key material (required, non-empty) from the HEADER only.
    let key = match headers.get(BUNDLE_KEY_HEADER).and_then(|v| v.to_str().ok()) {
        Some(k) if !k.is_empty() => k.to_owned(),
        _ => {
            return (
                StatusCode::BAD_REQUEST,
                "import requires a bundle key (X-Freenet-Bundle-Key header)",
            )
                .into_response();
        }
    };
    let key_kind = match parse_key_kind(headers) {
        Ok(k) => k,
        Err((status, reason)) => return (status, reason).into_response(),
    };
    let overwrite = headers
        .get(OVERWRITE_HEADER)
        .and_then(|v| v.to_str().ok())
        .is_some_and(|v| v.eq_ignore_ascii_case("true"));

    // Per-node route to the executor (the SAME handle the export uses). Absent /
    // not-yet-filled ⇒ 503.
    let op_manager = parts
        .extensions
        .get::<ExportOpManagerHandle>()
        .and_then(ExportOpManagerHandle::current);
    let Some(op_manager) = op_manager else {
        tracing::warn!("Live import requested but no running node is registered");
        return (
            StatusCode::SERVICE_UNAVAILABLE,
            "node is not ready to serve imports",
        )
            .into_response();
    };

    // Read the body only now that the request is authorized and the node is
    // ready. `to_bytes` enforces the same cap as the DefaultBodyLimit layer; an
    // over-cap body surfaces as 413.
    let bundle = match axum::body::to_bytes(body, MAX_IMPORT_BUNDLE_BYTES).await {
        Ok(b) => b.to_vec(),
        Err(_) => {
            return (
                StatusCode::PAYLOAD_TOO_LARGE,
                "import bundle exceeds the maximum allowed size",
            )
                .into_response();
        }
    };
    if bundle.is_empty() {
        return (StatusCode::BAD_REQUEST, "import bundle is empty").into_response();
    }

    match run_import(&op_manager, bundle, key, key_kind, overwrite).await {
        Ok(report) => (
            StatusCode::OK,
            Json(serde_json::json!({
                "imported": report.imported,
                "skipped": report.skipped,
            })),
        )
            .into_response(),
        Err(status_and_msg) => status_and_msg.into_response(),
    }
}

/// Drive the import on the node's executor and return the [`ImportReport`].
///
/// The bundle bytes and the key are moved into the (redacted/zeroizing) event
/// wrappers so the high-value key is wiped after the bundle is decrypted and
/// never reaches a log; the bundle's `Debug` prints only its length.
async fn run_import(
    op_manager: &OpManager,
    bundle: Vec<u8>,
    key: String,
    key_kind: BundleKeyKind,
    overwrite: bool,
) -> Result<crate::wasm_runtime::secret_export::ImportReport, (StatusCode, &'static str)> {
    let event = ContractHandlerEvent::ImportSecrets {
        // v1 always targets the node-local single-user scope.
        target_scope: ImportTargetScope::Local,
        bundle: ImportBundle::new(bundle),
        key_material: RedactedToken::new(key.into_bytes()),
        key_kind,
        overwrite,
    };

    // Initiated by a local HTTP client → ClientLocal lane (#4534).
    match op_manager
        .notify_contract_handler_prioritized(event, Priority::ClientLocal)
        .await
    {
        Ok(ContractHandlerEvent::ImportSecretsResponse(Ok(report))) => Ok(report),
        Ok(ContractHandlerEvent::ImportSecretsResponse(Err(e))) => {
            // A wrong key / corrupt bundle is a CLIENT-input fault: surface 4xx
            // (with NOTHING written — the decrypt fails before any write), so the
            // caller can tell it apart from a genuine 500. The message is
            // non-secret (never echoes the key or any plaintext).
            if e.is_import_bad_bundle() {
                tracing::warn!(error = %e, "Rejected live import: bad bundle/key");
                return Err((
                    StatusCode::UNPROCESSABLE_ENTITY,
                    "could not decrypt the bundle: wrong key or corrupt bundle",
                ));
            }
            // The contract loop's fair queue is full (#4534): a transient
            // back-pressure condition, not a real failure. 503 so the caller can
            // distinguish "retry later" from a genuine error.
            if e.is_contract_queue_full() {
                tracing::warn!(error = %e, "Rejected live import: contract queue full");
                return Err((
                    StatusCode::SERVICE_UNAVAILABLE,
                    "node is busy; retry the import shortly",
                ));
            }
            // Executor-side failure (store/IO). Do not leak internals; log the
            // detail, return a generic 500.
            tracing::error!(error = %e, "Live import failed on the executor");
            Err((
                StatusCode::INTERNAL_SERVER_ERROR,
                "import failed on the node",
            ))
        }
        Ok(other) => {
            tracing::error!(
                response = %other,
                "Unexpected contract-handler response to ImportSecrets"
            );
            Err((
                StatusCode::INTERNAL_SERVER_ERROR,
                "import failed on the node",
            ))
        }
        Err(e) => {
            tracing::error!(error = %e, "Contract handler unavailable for live import");
            Err((
                StatusCode::SERVICE_UNAVAILABLE,
                "node is not ready to serve imports",
            ))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::http::HeaderValue;
    use std::net::{Ipv4Addr, Ipv6Addr};

    const LOOPBACK: Option<IpAddr> = Some(IpAddr::V4(Ipv4Addr::LOCALHOST));
    const LOCAL_ORIGIN: &str = "http://127.0.0.1:50509";

    fn headers_with(origin: Option<&str>) -> HeaderMap {
        let mut h = HeaderMap::new();
        if let Some(o) = origin {
            h.insert("origin", HeaderValue::from_str(o).unwrap());
        }
        h
    }

    fn empty_map() -> OriginContractMap {
        std::sync::Arc::new(dashmap::DashMap::new())
    }

    /// Headline: loopback source + a localhost dashboard origin + no
    /// per-contract token ⇒ the gate admits.
    #[test]
    fn gate_admits_loopback_dashboard_origin() {
        let headers = headers_with(Some(LOCAL_ORIGIN));
        import_gate_or_reject(&headers, LOOPBACK, None, None, None, &empty_map())
            .expect("loopback dashboard request must be admitted");
    }

    /// A missing ConnectInfo cannot prove loopback ⇒ fail closed (403).
    #[test]
    fn gate_rejects_missing_source_ip() {
        let headers = headers_with(Some(LOCAL_ORIGIN));
        let err =
            import_gate_or_reject(&headers, None, None, None, None, &empty_map()).unwrap_err();
        assert_eq!(err.0, StatusCode::FORBIDDEN);
    }

    /// A non-loopback source IP is never honored (403), even with a good origin.
    #[test]
    fn gate_rejects_non_loopback_source() {
        let headers = headers_with(Some(LOCAL_ORIGIN));
        let public = Some(IpAddr::V4(Ipv4Addr::new(203, 0, 113, 7)));
        let err =
            import_gate_or_reject(&headers, public, None, None, None, &empty_map()).unwrap_err();
        assert_eq!(err.0, StatusCode::FORBIDDEN);
    }

    /// `::ffff:127.0.0.1` from a dual-stack socket normalizes to loopback and is
    /// admitted.
    #[test]
    fn gate_admits_ipv4_mapped_loopback() {
        let headers = headers_with(Some(LOCAL_ORIGIN));
        let mapped = Some(IpAddr::V6(Ipv4Addr::LOCALHOST.to_ipv6_mapped()));
        import_gate_or_reject(&headers, mapped, None, None, None, &empty_map())
            .expect("ipv4-mapped loopback must be admitted");
    }

    /// A missing Origin header is rejected (CSRF: a state-changing write needs a
    /// trusted origin).
    #[test]
    fn gate_rejects_missing_origin() {
        let headers = headers_with(None);
        let err =
            import_gate_or_reject(&headers, LOOPBACK, None, None, None, &empty_map()).unwrap_err();
        assert_eq!(err.0, StatusCode::FORBIDDEN);
    }

    /// `Origin: null` (the sandboxed contract iframe) is rejected — the
    /// per-contract iframe exclusion.
    #[test]
    fn gate_rejects_null_origin_sandbox_iframe() {
        let headers = headers_with(Some("null"));
        let err =
            import_gate_or_reject(&headers, LOOPBACK, None, None, None, &empty_map()).unwrap_err();
        assert_eq!(err.0, StatusCode::FORBIDDEN);
    }

    /// A cross-site origin (not a loopback/allowed host) is rejected.
    #[test]
    fn gate_rejects_cross_site_origin() {
        let headers = headers_with(Some("https://evil.example.com"));
        let err =
            import_gate_or_reject(&headers, LOOPBACK, None, None, None, &empty_map()).unwrap_err();
        assert_eq!(err.0, StatusCode::FORBIDDEN);
    }

    /// A request presenting a registered per-contract AuthToken (bearer header)
    /// is rejected even from a loopback dashboard origin — the AuthToken-origin
    /// exclusion.
    #[test]
    fn gate_rejects_registered_origin_contract_bearer() {
        let map: OriginContractMap = std::sync::Arc::new(dashmap::DashMap::new());
        let tok = AuthToken::from("contract-token-xyz".to_owned());
        map.insert(tok.clone(), dummy_origin_contract());

        let mut headers = headers_with(Some(LOCAL_ORIGIN));
        headers.insert(
            axum::http::header::AUTHORIZATION,
            HeaderValue::from_str("Bearer contract-token-xyz").unwrap(),
        );
        let err = import_gate_or_reject(&headers, LOOPBACK, None, None, None, &map).unwrap_err();
        assert_eq!(err.0, StatusCode::FORBIDDEN);
    }

    /// A request presenting a registered per-contract AuthToken via the
    /// `Authorization` cookie is likewise rejected.
    #[test]
    fn gate_rejects_registered_origin_contract_cookie() {
        let map: OriginContractMap = std::sync::Arc::new(dashmap::DashMap::new());
        let tok = AuthToken::from("cookie-token-abc".to_owned());
        map.insert(tok.clone(), dummy_origin_contract());

        let mut headers = headers_with(Some(LOCAL_ORIGIN));
        headers.insert(
            axum::http::header::COOKIE,
            HeaderValue::from_str("authorization=Bearer cookie-token-abc").unwrap(),
        );
        let err = import_gate_or_reject(&headers, LOOPBACK, None, None, None, &map).unwrap_err();
        assert_eq!(err.0, StatusCode::FORBIDDEN);
    }

    /// A bearer token NOT registered as an origin contract does not trip the
    /// exclusion (it is not a per-contract origin).
    #[test]
    fn gate_admits_unregistered_bearer() {
        let mut headers = headers_with(Some(LOCAL_ORIGIN));
        headers.insert(
            axum::http::header::AUTHORIZATION,
            HeaderValue::from_str("Bearer some-unrelated-token").unwrap(),
        );
        import_gate_or_reject(&headers, LOOPBACK, None, None, None, &empty_map())
            .expect("an unregistered bearer is not a contract origin");
    }

    #[test]
    fn presented_tokens_reads_header_query_and_cookie() {
        let mut headers = HeaderMap::new();
        headers.insert(
            axum::http::header::AUTHORIZATION,
            HeaderValue::from_str("Bearer header-tok").unwrap(),
        );
        headers.insert(
            axum::http::header::COOKIE,
            HeaderValue::from_str("foo=bar; authorization=Bearer cookie-tok").unwrap(),
        );
        let toks: Vec<String> = presented_tokens(&headers, Some("a=b&authToken=query-tok"))
            .map(|t| t.as_str().to_owned())
            .collect();
        assert!(toks.contains(&"header-tok".to_owned()));
        assert!(toks.contains(&"query-tok".to_owned()));
        assert!(toks.contains(&"cookie-tok".to_owned()));
    }

    #[test]
    fn parse_key_kind_defaults_and_variants() {
        let mut h = HeaderMap::new();
        assert_eq!(parse_key_kind(&h).unwrap(), BundleKeyKind::Token);
        h.insert(
            BUNDLE_KEY_KIND_HEADER,
            HeaderValue::from_static("passphrase"),
        );
        assert_eq!(parse_key_kind(&h).unwrap(), BundleKeyKind::Passphrase);
        h.insert(BUNDLE_KEY_KIND_HEADER, HeaderValue::from_static("TOKEN"));
        assert_eq!(parse_key_kind(&h).unwrap(), BundleKeyKind::Token);
        h.insert(BUNDLE_KEY_KIND_HEADER, HeaderValue::from_static("bogus"));
        assert_eq!(parse_key_kind(&h).unwrap_err().0, StatusCode::BAD_REQUEST);
    }

    #[test]
    fn strip_bearer_handles_case_and_absence() {
        assert_eq!(strip_bearer("Bearer abc"), Some("abc"));
        assert_eq!(strip_bearer("bearer abc"), Some("abc"));
        assert_eq!(strip_bearer("Token abc"), None);
    }

    #[test]
    fn gate_rejects_ipv6_non_loopback() {
        let headers = headers_with(Some(LOCAL_ORIGIN));
        let public6 = Some(IpAddr::V6(Ipv6Addr::new(0x2001, 0xdb8, 0, 0, 0, 0, 0, 1)));
        let err =
            import_gate_or_reject(&headers, public6, None, None, None, &empty_map()).unwrap_err();
        assert_eq!(err.0, StatusCode::FORBIDDEN);
    }

    fn dummy_origin_contract() -> super::super::OriginContract {
        use freenet_stdlib::prelude::ContractInstanceId;
        // Any contract id works; the gate only checks token membership.
        let id = ContractInstanceId::new([0u8; 32]);
        super::super::OriginContract::new(id, crate::client_events::ClientId::FIRST)
    }
}
