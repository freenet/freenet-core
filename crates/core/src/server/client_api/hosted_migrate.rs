//! Zero-friction "magic-link" secrets migration (#4592, the primary path).
//!
//! Turns the hosted → self-host handoff into a link the user clicks on their
//! own peer. Three cooperating HTTP surfaces, on the SAME binary (a node can be
//! either side), plus a small confirmation page:
//!
//! 1. **Hosted MINT** — `POST /v{1,2}/hosted/migrate/mint` (on try.freenet.org).
//!    Gated exactly like the live EXPORT (hosted ON + loopback + `XFP:https` +
//!    the durable `X-Freenet-User-Token`). Exports the user's secrets under a
//!    FRESH EPHEMERAL key, stores the ephemeral-keyed bundle + key server-side
//!    under a single-use, short-TTL, per-user-bounded pull token, and returns
//!    the token. The frontend builds the link
//!    `http://127.0.0.1:7509/hosted/import?source=<origin>&pt=<token>`.
//!
//! 2. **Hosted PULL** — `GET /v{1,2}/hosted/migrate/pull?pt=<token>` (public,
//!    reachable over the internet through the hosting node's reverse proxy).
//!    The ONLY auth is the pull token: a 256-bit unguessable, single-use,
//!    short-TTL capability. Returns the ephemeral bundle bytes + the ephemeral
//!    key (in headers, over TLS). NOT gated on the durable token — the pulling
//!    peer never has it.
//!
//! 3. **Local IMPORT PAGE** — `GET /hosted/import` (on the user's own peer).
//!    A first-party confirmation page ("Import your data from <source>?"). It
//!    does NOT auto-pull: a state-changing import requires an explicit click,
//!    defeating drive-by / CSRF navigation to the link.
//!
//! 4. **Local PULL-IMPORT** — `POST /v{1,2}/hosted/pull-import` (on the user's
//!    own peer). Gated by the SAME loopback + trusted-origin gate as the live
//!    import (`hosted_import`), so only the node's own dashboard/confirmation
//!    page can drive it. Fetches the bundle over HTTPS from an ALLOWLISTED
//!    source (`try.freenet.org` only, https, default port) — a strict SSRF
//!    guard — then imports it into `Local` with `overwrite=false`
//!    (NON-DESTRUCTIVE: a secret already present locally is kept and reported
//!    in `skipped`, never clobbered).
//!
//! # Why the durable token never leaves the hosting node
//!
//! The bundle key and the user scope are DECOUPLED at the crypto layer
//! (`export_bundle(store, scope, material)` — independent args). The mint
//! exports the real user's scope under a random ephemeral key, so the pulling
//! peer only ever handles the ephemeral key (delivered once, over TLS, then the
//! token is burned). The durable `X-Freenet-User-Token` stays on the hosting
//! node.
//!
//! # Pull-token lifecycle (the load-bearing auth object)
//!
//! - **Minted** only behind the authenticated mint gate.
//! - **256-bit** from `OsRng` (unguessable capability — brute force infeasible).
//! - **Single-use**: `take` removes it; a replay gets a uniform 404.
//! - **Short-TTL** ([`PULL_TOKEN_TTL`]): expired tokens are pruned and read as
//!   absent (same 404), so a leaked-but-stale link is inert.
//! - **Bounded**: per-user ([`MAX_PULL_TOKENS_PER_USER`]) and global (count +
//!   bytes), so an authenticated token-holder cannot exhaust node memory by
//!   minting repeatedly; minting also shares the per-user EXPORT rate limit.

use axum::{
    Json, Router,
    extract::{ConnectInfo, DefaultBodyLimit},
    http::{HeaderMap, HeaderName, HeaderValue, StatusCode, header::CONTENT_TYPE},
    response::{Html, IntoResponse, Response},
    routing::{get, post},
};
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;
use tokio::time::Instant;

use dashmap::DashMap;
use serde::Deserialize;
use serde_json::json;
use zeroize::Zeroizing;

use crate::client_events::user_op_rate_limit::UserOpRateLimiter;
use crate::contract::BundleKeyKind;
use crate::server::{AllowedHosts, AllowedSourceCidrs, HostedMode};
use crate::wasm_runtime::UserId;

use super::hosted_export::{
    ExportOpManagerHandle, export_rate_limited, export_user_context_or_reject, run_export,
};
use super::hosted_import::{
    BUNDLE_KEY_HEADER, BUNDLE_KEY_KIND_HEADER, MAX_IMPORT_BUNDLE_BYTES, import_gate_or_reject,
    run_import,
};
use super::{ApiVersion, OriginContractMap};

/// How long a minted migration pull token stays valid. A focused "do it now"
/// window: long enough to open the link on your own peer, short enough that a
/// leaked-but-stale link is inert. Expired tokens read as absent (404).
const PULL_TOKEN_TTL: Duration = Duration::from_secs(600);

/// Max live pull tokens a single user may hold. Minting past the cap evicts the
/// user's OLDEST token. Bounds a single authenticated token-holder's footprint
/// (minting also shares the per-user export rate limit).
const MAX_PULL_TOKENS_PER_USER: usize = 5;

/// Global cap on live pull tokens across all users. With the per-user cap and
/// the byte budget, bounds total store memory. Minting past it evicts the
/// globally-oldest token first.
const MAX_PULL_TOKENS_TOTAL: usize = 512;

/// Global byte budget for buffered ephemeral bundles. Sized to comfortably hold
/// at least one maximal export (`MAX_IMPORT_BUNDLE_BYTES`) plus headroom; real
/// bundles are kilobytes. Minting evicts oldest tokens until the new bundle
/// fits; a single bundle larger than the whole budget is refused (can't happen
/// for a valid export, which is capped below this).
const MAX_PULL_STORE_BYTES: usize = MAX_IMPORT_BUNDLE_BYTES + 512 * 1024 * 1024;

/// Upper bound on the JSON body of a `pull-import` request (`{source, pt}` is a
/// few hundred bytes; this is generous). Over ⇒ 413.
const MAX_PULL_IMPORT_REQUEST_BYTES: usize = 64 * 1024;

/// Timeout for the local peer's outbound HTTPS pull from the hosting node.
const MIGRATION_PULL_TIMEOUT_SECS: u64 = 30;

/// The ONLY hosts a local peer will pull a migration bundle FROM. A strict SSRF
/// allowlist: a caller-supplied `source` must be `https`, on the default port,
/// with a host in this list. Kept a small constant list so it is trivial to
/// extend, but shipped locked to the one production hosting origin.
const ALLOWED_MIGRATION_HOSTS: &[&str] = &["try.freenet.org"];

// ---------------------------------------------------------------------------
// Pull-token store
// ---------------------------------------------------------------------------

/// One minted, pre-computed migration bundle awaiting a single pull.
struct PullEntry {
    /// The ephemeral-keyed FNSX bundle bytes (encrypted under `key`).
    bundle: Vec<u8>,
    /// The ephemeral bundle key (bs58). Delivered to the pulling peer once, over
    /// TLS; zeroized when the entry drops.
    key: Zeroizing<String>,
    /// The user this bundle was minted for — used ONLY for the per-user cap.
    user_id: UserId,
    /// Monotonic expiry. A token at or past this reads as absent.
    expires_at: Instant,
}

/// Per-node handle to the migration pull-token store. Cheap to clone (an `Arc`),
/// injected as a router `Extension` in
/// [`HttpClientApi::as_router_with_origin_contracts`](super::HttpClientApi).
/// Each node owns its own store, so parallel in-process nodes never collide.
#[derive(Clone, Default)]
pub(crate) struct MigratePullStore(Arc<DashMap<String, PullEntry>>);

impl MigratePullStore {
    /// Store a freshly-minted ephemeral bundle under a new random single-use
    /// token, returning the token. Prunes expired entries, enforces the per-user
    /// cap (evicting that user's oldest), then the global count + byte budget
    /// (evicting the globally-oldest until the new bundle fits). Returns `None`
    /// only if the new bundle alone exceeds the whole byte budget (a valid
    /// export never does), which the caller maps to a 503.
    fn insert(&self, user_id: UserId, bundle: Vec<u8>, key: String) -> Option<String> {
        let now = Instant::now();
        let new_len = bundle.len();
        if new_len > MAX_PULL_STORE_BYTES {
            return None;
        }

        // (1) Drop expired entries so caps count only live tokens.
        self.0.retain(|_, e| e.expires_at > now);

        // (2) Per-user cap: keep at most MAX_PULL_TOKENS_PER_USER - 1 of this
        // user's existing tokens, evicting oldest, so this insert lands at the
        // cap.
        let mut mine: Vec<(String, Instant)> = self
            .0
            .iter()
            .filter(|r| r.value().user_id == user_id)
            .map(|r| (r.key().clone(), r.value().expires_at))
            .collect();
        if mine.len() >= MAX_PULL_TOKENS_PER_USER {
            mine.sort_by_key(|(_, exp)| *exp);
            let excess = mine.len() - (MAX_PULL_TOKENS_PER_USER - 1);
            for (id, _) in mine.into_iter().take(excess) {
                self.0.remove(&id);
            }
        }

        // (3) Global count + byte budget: evict globally-oldest until the new
        // bundle fits.
        loop {
            let count = self.0.len();
            let total: usize = self.0.iter().map(|r| r.value().bundle.len()).sum();
            if count < MAX_PULL_TOKENS_TOTAL && total + new_len <= MAX_PULL_STORE_BYTES {
                break;
            }
            let oldest = self
                .0
                .iter()
                .min_by_key(|r| r.value().expires_at)
                .map(|r| r.key().clone());
            match oldest {
                Some(id) => {
                    self.0.remove(&id);
                }
                None => break, // store empty but new bundle still didn't fit — impossible given the guard above
            }
        }

        let token = random_token();
        self.0.insert(
            token.clone(),
            PullEntry {
                bundle,
                key: Zeroizing::new(key),
                user_id,
                expires_at: now + PULL_TOKEN_TTL,
            },
        );
        Some(token)
    }

    /// Consume the entry for `token` (single-use). Returns `None` — uniformly,
    /// with no distinction an attacker could probe — if the token is unknown,
    /// already used, or expired.
    fn take(&self, token: &str) -> Option<PullEntry> {
        let (_, entry) = self.0.remove(token)?;
        if entry.expires_at <= Instant::now() {
            return None;
        }
        Some(entry)
    }
}

/// A cryptographically-random, unguessable 256-bit capability token, bs58
/// encoded. Used for BOTH the pull token (a download-authorizing bearer
/// capability) and the ephemeral bundle key (the decryption secret) — both must
/// be unguessable, so `OsRng` (the documented crypto-material exception to
/// `GlobalRng`) is correct: a `GlobalRng` seed leak would let an attacker
/// predict tokens and either steal a pending migration or forge a valid link.
fn random_token() -> String {
    // OsRng: cryptographic capability/key material — see the module + code-style
    // `OsRng` exception. Never `GlobalRng` here.
    use chacha20poly1305::aead::OsRng;
    use chacha20poly1305::aead::rand_core::RngCore;
    let mut buf = [0u8; 32];
    OsRng.fill_bytes(&mut buf);
    bs58::encode(buf)
        .with_alphabet(bs58::Alphabet::BITCOIN)
        .into_string()
}

// ---------------------------------------------------------------------------
// Routes
// ---------------------------------------------------------------------------

/// Registers the migration HTTP endpoints for `version`: the hosted `mint` +
/// `pull` and the local `pull-import`. The confirmation PAGE (`/hosted/import`)
/// is unversioned and registered separately in `client_api.rs` (like `/`).
pub(super) fn routes(version: ApiVersion) -> Router {
    let prefix = version.prefix();
    Router::new()
        .route(
            &format!("/{prefix}/hosted/migrate/mint"),
            post(mint_handler),
        )
        .route(&format!("/{prefix}/hosted/migrate/pull"), get(pull_handler))
        .route(
            &format!("/{prefix}/hosted/pull-import"),
            post(pull_import_handler).layer(DefaultBodyLimit::max(MAX_PULL_IMPORT_REQUEST_BYTES)),
        )
}

// ---------------------------------------------------------------------------
// Hosted side: mint + pull
// ---------------------------------------------------------------------------

/// `POST /v{1,2}/hosted/migrate/mint` — mint a one-time migration link for the
/// authenticated hosted user. Gated identically to the live export; the durable
/// token authenticates the user but is NOT used as the bundle key.
async fn mint_handler(req: axum::extract::Request) -> Response {
    let hosted_mode = req
        .extensions()
        .get::<HostedMode>()
        .map(|hm| hm.0)
        .unwrap_or(false);
    let source_ip = req
        .extensions()
        .get::<ConnectInfo<SocketAddr>>()
        .map(|ci| ci.0.ip());
    let headers = req.headers();

    // Same gate as the live export (hosted + loopback + XFP:https + valid
    // token). We take the user CONTEXT (the scope) and DISCARD the durable token
    // — the bundle is sealed under a fresh ephemeral key instead.
    let (user_context, durable_token) =
        match export_user_context_or_reject(headers, source_ip, hosted_mode) {
            Ok(v) => v,
            Err((status, reason)) => {
                tracing::warn!(source_ip = ?source_ip, reason, "Rejected migration mint");
                return (status, reason).into_response();
            }
        };
    // The durable token is only used to authenticate + scope; it is NEVER the
    // bundle key here (the mint seals under a fresh ephemeral key). Wipe it now
    // so it does not linger in freed memory — it must not leave this node.
    drop(zeroize::Zeroizing::new(durable_token));

    // Minting runs a full export, so it shares the per-user EXPORT rate limit.
    let limiter = req.extensions().get::<UserOpRateLimiter>();
    if export_rate_limited(limiter, &user_context) {
        tracing::warn!(
            user_id = ?user_context.user_id(),
            "Rejected migration mint: per-user export rate limit exceeded"
        );
        return (
            StatusCode::TOO_MANY_REQUESTS,
            "migration rate limit exceeded; retry shortly",
        )
            .into_response();
    }

    let op_manager = req
        .extensions()
        .get::<ExportOpManagerHandle>()
        .and_then(ExportOpManagerHandle::current);
    let Some(op_manager) = op_manager else {
        tracing::warn!("Migration mint requested but no running node is registered");
        return (
            StatusCode::SERVICE_UNAVAILABLE,
            "node is not ready to serve migrations",
        )
            .into_response();
    };

    let Some(store) = req.extensions().get::<MigratePullStore>().cloned() else {
        return (
            StatusCode::SERVICE_UNAVAILABLE,
            "migration is not available on this node",
        )
            .into_response();
    };

    // Fresh ephemeral bundle key (never derived from the durable token). The
    // user id is only for the store's per-user cap.
    let ephemeral_key = random_token();
    let user_id = *user_context.user_id();

    // Export the user's scope under the ephemeral key (same executor round-trip
    // as the live export). Over-limit / busy / failure map to 413 / 503 / 500.
    let bundle = match run_export(
        &op_manager,
        user_context,
        ephemeral_key.clone().into_bytes(),
    )
    .await
    {
        Ok(b) => b,
        Err((status, reason)) => return (status, reason).into_response(),
    };

    let Some(pull_token) = store.insert(user_id, bundle, ephemeral_key) else {
        return (
            StatusCode::SERVICE_UNAVAILABLE,
            "too many pending migrations; retry shortly",
        )
            .into_response();
    };

    (
        StatusCode::OK,
        Json(json!({
            "pull_token": pull_token,
            "expires_in_secs": PULL_TOKEN_TTL.as_secs(),
        })),
    )
        .into_response()
}

/// `GET /v{1,2}/hosted/migrate/pull?pt=<token>` — hand the ephemeral bundle to
/// the pulling peer. PUBLIC: the single-use pull token is the entire auth. A
/// missing / expired / already-used token gets a uniform 404.
async fn pull_handler(req: axum::extract::Request) -> Response {
    let Some(store) = req.extensions().get::<MigratePullStore>().cloned() else {
        return pull_not_found();
    };
    let Some(pt) = pt_from_query(req.uri().query()) else {
        return pull_not_found();
    };
    let Some(entry) = store.take(&pt) else {
        return pull_not_found();
    };

    let mut headers = HeaderMap::new();
    headers.insert(
        CONTENT_TYPE,
        HeaderValue::from_static("application/octet-stream"),
    );
    // The ephemeral key rides in a header (never the URL) — the same discipline
    // and header names the local pull-import forwards into the import path.
    match HeaderValue::from_str(entry.key.as_str()) {
        Ok(v) => {
            headers.insert(HeaderName::from_static(BUNDLE_KEY_HEADER), v);
        }
        Err(_) => {
            // A bs58 key is always a valid header value; this cannot happen. Fail
            // closed rather than serve a keyless bundle.
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                "migration key encoding error",
            )
                .into_response();
        }
    }
    headers.insert(
        HeaderName::from_static(BUNDLE_KEY_KIND_HEADER),
        HeaderValue::from_static("token"),
    );

    (StatusCode::OK, headers, entry.bundle).into_response()
}

/// Uniform "no such pull" response — identical for unknown / expired / used, so
/// it is not an oracle for token existence.
fn pull_not_found() -> Response {
    (
        StatusCode::NOT_FOUND,
        "migration link not found, expired, or already used",
    )
        .into_response()
}

/// Extract and validate the `pt` query parameter. A valid pull token is
/// non-empty, bounded, and bs58 (ASCII alphanumeric); anything else is treated
/// as absent so a malformed `pt` can never reach the store as a lookup key.
fn pt_from_query(query: Option<&str>) -> Option<String> {
    let q = query?;
    for pair in q.split('&') {
        if let Some(val) = pair.strip_prefix("pt=")
            && is_valid_pull_token(val)
        {
            return Some(val.to_owned());
        }
    }
    None
}

/// A pull token is bs58 of 32 bytes: non-empty, at most 64 chars, ASCII
/// alphanumeric. Rejecting anything else keeps injection/oversized values out
/// of both the store lookup and the outbound pull URL.
fn is_valid_pull_token(pt: &str) -> bool {
    !pt.is_empty() && pt.len() <= 64 && pt.chars().all(|c| c.is_ascii_alphanumeric())
}

// ---------------------------------------------------------------------------
// Local side: confirmation page + pull-import
// ---------------------------------------------------------------------------

/// `GET /hosted/import` — the local peer's confirmation page. Served from the
/// node's own first-party origin. It reads `source`/`pt` from the URL
/// CLIENT-side (so no untrusted input is interpolated server-side) and requires
/// an explicit click before POSTing to `pull-import` — a state-changing import
/// is never triggered by mere navigation to the link.
pub(super) async fn import_page() -> impl IntoResponse {
    Html(format!(
        include_str!("assets/hosted_import_page.html"),
        style = HOSTED_IMPORT_PAGE_CSS,
        script = HOSTED_IMPORT_PAGE_JS,
    ))
}

const HOSTED_IMPORT_PAGE_CSS: &str = include_str!("assets/hosted_import_page.css");
const HOSTED_IMPORT_PAGE_JS: &str = include_str!("assets/hosted_import_page.js");

#[derive(Deserialize)]
struct PullImportRequest {
    source: String,
    pt: String,
}

/// `POST /v{1,2}/hosted/pull-import` — the local peer fetches the bundle from
/// the allowlisted hosting node and imports it (non-destructively). Gated by the
/// SAME loopback + trusted-origin gate as the live import, so only the node's
/// own confirmation page can drive it.
async fn pull_import_handler(req: axum::extract::Request) -> Response {
    let (parts, body) = req.into_parts();
    let headers = &parts.headers;

    let source_ip = parts
        .extensions
        .get::<ConnectInfo<SocketAddr>>()
        .map(|ci| ci.0.ip());
    let allowed_hosts = parts.extensions.get::<AllowedHosts>();
    let allowed_source_cidrs = parts.extensions.get::<AllowedSourceCidrs>();
    let empty_origin_contracts: OriginContractMap = Arc::new(DashMap::new());
    let origin_contracts = parts
        .extensions
        .get::<OriginContractMap>()
        .unwrap_or(&empty_origin_contracts);

    // Reuse the live-import gate verbatim: loopback + trusted first-party origin
    // + no per-contract token. This is what makes the pull-import CSRF-safe (a
    // sandboxed contract iframe presents Origin: null and is rejected).
    if let Err((status, reason)) = import_gate_or_reject(
        headers,
        source_ip,
        allowed_hosts,
        allowed_source_cidrs,
        parts.uri.query(),
        origin_contracts,
    ) {
        tracing::warn!(source_ip = ?source_ip, reason, "Rejected migration pull-import");
        return (status, reason).into_response();
    }

    let op_manager = parts
        .extensions
        .get::<ExportOpManagerHandle>()
        .and_then(ExportOpManagerHandle::current);
    let Some(op_manager) = op_manager else {
        return (
            StatusCode::SERVICE_UNAVAILABLE,
            "node is not ready to serve imports",
        )
            .into_response();
    };

    let body_bytes = match axum::body::to_bytes(body, MAX_PULL_IMPORT_REQUEST_BYTES).await {
        Ok(b) => b,
        Err(_) => {
            return (StatusCode::PAYLOAD_TOO_LARGE, "request body too large").into_response();
        }
    };
    let request: PullImportRequest = match serde_json::from_slice(&body_bytes) {
        Ok(v) => v,
        Err(_) => {
            return (StatusCode::BAD_REQUEST, "invalid pull-import request body").into_response();
        }
    };

    // SSRF guard: the source MUST be https, default port, allowlisted host. We
    // then build the pull URL from the trusted constant host (not the raw
    // source), so a path/port/userinfo in `source` cannot redirect the fetch.
    let Some(host) = allowed_migration_host(&request.source) else {
        tracing::warn!(source = %request.source, "Rejected migration pull-import: source not allowed");
        return (
            StatusCode::FORBIDDEN,
            "migration source is not an allowed origin",
        )
            .into_response();
    };
    if !is_valid_pull_token(&request.pt) {
        return (StatusCode::BAD_REQUEST, "invalid migration token").into_response();
    }

    let (bundle, key, key_kind) = match pull_bundle_from_source(host, &request.pt).await {
        Ok(v) => v,
        Err((status, reason)) => return (status, reason).into_response(),
    };

    // Non-destructive: overwrite=false. A secret already present locally is kept
    // and reported in `skipped`, never clobbered. (TODO(#4592): an optional
    // pre-import snapshot could add belt-and-suspenders, deferred for v1 since
    // import is already non-destructive.)
    match run_import(&op_manager, bundle, key, key_kind, false).await {
        Ok(report) => (
            StatusCode::OK,
            Json(json!({
                "imported": report.imported,
                "skipped": report.skipped,
            })),
        )
            .into_response(),
        Err((status, reason)) => (status, reason).into_response(),
    }
}

/// Validate a caller-supplied migration `source` against the SSRF allowlist,
/// returning the matched trusted host on success. Requires `https`, no explicit
/// (non-default) port, and an exact (case-insensitive) host match. Returns the
/// CONSTANT host string so callers build the pull URL from trusted data.
fn allowed_migration_host(source: &str) -> Option<&'static str> {
    // `reqwest::Url` re-exports the `url` crate's `Url`; use it rather than a
    // direct `url` dep (reqwest is already a lib dependency).
    let url = reqwest::Url::parse(source).ok()?;
    if url.scheme() != "https" {
        return None;
    }
    // Only the default https port (443). An explicit port is rejected so a
    // crafted `https://try.freenet.org:1234/` cannot redirect the fetch.
    if url.port().is_some() {
        return None;
    }
    let host = url.host_str()?;
    ALLOWED_MIGRATION_HOSTS
        .iter()
        .copied()
        .find(|allowed| allowed.eq_ignore_ascii_case(host))
}

/// Fetch the ephemeral bundle from the hosting node's public pull endpoint over
/// HTTPS (TLS validated). The URL is built from the ALREADY-VALIDATED constant
/// host and validated token. The body is read with a hard cap so a hostile /
/// oversized response cannot exhaust memory.
async fn pull_bundle_from_source(
    host: &str,
    pt: &str,
) -> Result<(Vec<u8>, String, BundleKeyKind), (StatusCode, &'static str)> {
    let url = format!("https://{host}/v1/hosted/migrate/pull?pt={pt}");
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(MIGRATION_PULL_TIMEOUT_SECS))
        .https_only(true)
        // Do NOT follow redirects: the SSRF allowlist only vets the FIRST URL's
        // host, so a redirect from the allowlisted origin to an internal address
        // would defeat it. The pull endpoint answers 200/404 directly, never a
        // redirect, so this only closes the SSRF-via-redirect hole.
        .redirect(reqwest::redirect::Policy::none())
        .build()
        .map_err(|_| {
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                "migration HTTP client init failed",
            )
        })?;

    let mut resp = client.get(&url).send().await.map_err(|_| {
        (
            StatusCode::BAD_GATEWAY,
            "could not reach the migration source",
        )
    })?;
    if !resp.status().is_success() {
        return Err((
            StatusCode::BAD_GATEWAY,
            "migration source rejected the link (expired or already used)",
        ));
    }

    // Extract the key + kind (owned) BEFORE the streaming body read.
    let key = resp
        .headers()
        .get(BUNDLE_KEY_HEADER)
        .and_then(|v| v.to_str().ok())
        .map(str::to_owned)
        .filter(|k| !k.is_empty());
    let Some(key) = key else {
        return Err((
            StatusCode::BAD_GATEWAY,
            "migration source returned no bundle key",
        ));
    };
    let key_kind = match resp
        .headers()
        .get(BUNDLE_KEY_KIND_HEADER)
        .and_then(|v| v.to_str().ok())
    {
        Some(s) if s.eq_ignore_ascii_case("passphrase") => BundleKeyKind::Passphrase,
        _ => BundleKeyKind::Token,
    };

    let mut bundle = Vec::new();
    loop {
        match resp.chunk().await {
            Ok(Some(chunk)) => {
                if bundle.len() + chunk.len() > MAX_IMPORT_BUNDLE_BYTES {
                    return Err((StatusCode::BAD_GATEWAY, "migration bundle too large"));
                }
                bundle.extend_from_slice(&chunk);
            }
            Ok(None) => break,
            Err(_) => {
                return Err((
                    StatusCode::BAD_GATEWAY,
                    "error reading the migration bundle",
                ));
            }
        }
    }
    if bundle.is_empty() {
        return Err((
            StatusCode::BAD_GATEWAY,
            "migration source returned an empty bundle",
        ));
    }
    Ok((bundle, key, key_kind))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn user(tag: u8) -> UserId {
        UserId::new([tag; 32])
    }

    /// A minted token round-trips exactly once: the first pull returns the
    /// bundle + key, and a second pull of the same token gets nothing
    /// (single-use).
    #[tokio::test(start_paused = true)]
    async fn pull_token_is_single_use() {
        let store = MigratePullStore::default();
        let token = store
            .insert(
                user(1),
                b"bundle-bytes".to_vec(),
                "ephemeral-key".to_owned(),
            )
            .expect("mint must store the bundle");

        let first = store.take(&token).expect("first pull returns the entry");
        assert_eq!(first.bundle, b"bundle-bytes");
        assert_eq!(first.key.as_str(), "ephemeral-key");

        assert!(
            store.take(&token).is_none(),
            "a second pull of the same token must find nothing (single-use)"
        );
    }

    /// An unknown token is a miss (no panic, uniform None).
    #[tokio::test(start_paused = true)]
    async fn pull_unknown_token_is_none() {
        let store = MigratePullStore::default();
        assert!(store.take("never-minted").is_none());
    }

    /// A token past its TTL reads as absent, even though it was never pulled.
    #[tokio::test(start_paused = true)]
    async fn pull_token_expires() {
        let store = MigratePullStore::default();
        let token = store
            .insert(user(1), b"b".to_vec(), "k".to_owned())
            .expect("mint");
        tokio::time::advance(PULL_TOKEN_TTL + Duration::from_secs(1)).await;
        assert!(
            store.take(&token).is_none(),
            "an expired token must read as absent"
        );
    }

    /// Minting past the per-user cap evicts that user's OLDEST token while
    /// leaving other users untouched. Distinct users get distinct tokens/scopes.
    #[tokio::test(start_paused = true)]
    async fn per_user_cap_evicts_oldest() {
        let store = MigratePullStore::default();
        let mut tokens = Vec::new();
        for i in 0..MAX_PULL_TOKENS_PER_USER {
            let t = store
                .insert(user(1), vec![i as u8], format!("k{i}"))
                .expect("mint");
            tokens.push(t);
            // Space entries in time so "oldest" is well-defined.
            tokio::time::advance(Duration::from_secs(1)).await;
        }
        // A user-2 token is independent and must survive user-1 churn.
        let other = store
            .insert(user(2), b"other".to_vec(), "ok".to_owned())
            .expect("mint");

        // One more user-1 mint pushes over the cap, evicting user-1's oldest.
        let newest = store
            .insert(user(1), b"newest".to_vec(), "kn".to_owned())
            .expect("mint");

        assert!(
            store.take(&tokens[0]).is_none(),
            "user-1's oldest token must have been evicted at the cap"
        );
        assert!(
            store.take(&newest).is_some(),
            "the newest user-1 token must be present"
        );
        assert!(
            store.take(&other).is_some(),
            "another user's token must be unaffected by user-1's cap"
        );
    }

    #[test]
    fn ssrf_allowlist_accepts_only_https_default_port_trusted_host() {
        assert_eq!(
            allowed_migration_host("https://try.freenet.org"),
            Some("try.freenet.org")
        );
        assert_eq!(
            allowed_migration_host("https://try.freenet.org/"),
            Some("try.freenet.org")
        );
        // Case-insensitive host match.
        assert_eq!(
            allowed_migration_host("https://TRY.freenet.org"),
            Some("try.freenet.org")
        );
    }

    #[test]
    fn ssrf_allowlist_rejects_non_https_and_untrusted() {
        // Non-https (plaintext) is rejected.
        assert!(allowed_migration_host("http://try.freenet.org").is_none());
        // A different host is rejected.
        assert!(allowed_migration_host("https://evil.example.com").is_none());
        // A look-alike subdomain is rejected (exact host match).
        assert!(allowed_migration_host("https://try.freenet.org.evil.com").is_none());
        // An explicit port is rejected (only default 443).
        assert!(allowed_migration_host("https://try.freenet.org:8443").is_none());
        // An embedded-credential / host-confusion attempt is rejected.
        assert!(allowed_migration_host("https://try.freenet.org@evil.com").is_none());
        // Loopback (the classic SSRF target) is rejected.
        assert!(allowed_migration_host("https://127.0.0.1").is_none());
        // Garbage is rejected.
        assert!(allowed_migration_host("not a url").is_none());
    }

    #[test]
    fn pull_token_validation() {
        assert!(is_valid_pull_token("abcDEF123"));
        assert!(!is_valid_pull_token(""));
        assert!(!is_valid_pull_token("has-dash"));
        assert!(!is_valid_pull_token("has space"));
        assert!(!is_valid_pull_token("has/slash"));
        assert!(!is_valid_pull_token(&"a".repeat(65)));
    }

    #[test]
    fn pt_extracted_and_validated_from_query() {
        assert_eq!(pt_from_query(Some("pt=abc123")).as_deref(), Some("abc123"));
        assert_eq!(
            pt_from_query(Some("x=1&pt=tok9&y=2")).as_deref(),
            Some("tok9")
        );
        // A malformed pt value is treated as absent.
        assert!(pt_from_query(Some("pt=bad-token")).is_none());
        assert!(pt_from_query(Some("nopt=here")).is_none());
        assert!(pt_from_query(None).is_none());
    }

    /// The confirmation page must render the confirm control and wire the POST
    /// to the local pull-import endpoint — and must NOT auto-run it (a
    /// state-changing import needs an explicit click).
    #[test]
    fn import_page_wiring() {
        // The page is a format-template; render it and scan the served bytes.
        let html = format!(
            include_str!("assets/hosted_import_page.html"),
            style = HOSTED_IMPORT_PAGE_CSS,
            script = HOSTED_IMPORT_PAGE_JS,
        );
        assert!(
            html.contains("/v1/hosted/pull-import"),
            "the page JS must POST to the local pull-import endpoint"
        );
        assert!(
            HOSTED_IMPORT_PAGE_JS.contains("addEventListener"),
            "the import must be behind an explicit user gesture, not auto-run"
        );
    }
}
