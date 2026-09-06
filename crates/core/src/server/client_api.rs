use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::time::Instant;

use dashmap::DashMap;

use axum::extract::Path;
use axum::response::IntoResponse;
use axum::{Extension, Router};
use freenet_stdlib::client_api::{ClientError, ErrorKind, HostResponse};
use freenet_stdlib::prelude::ContractInstanceId;
use futures::FutureExt;
use futures::future::BoxFuture;
use tokio::sync::mpsc;
use tracing::instrument;

use crate::client_events::{ClientEventsProxy, ClientId, OpenRequest};
use crate::server::HostCallbackResult;

use super::{
    ApiVersion, AuthToken, ClientConnection, errors::WebSocketApiError, home_page, path_handlers,
};

/// Content-Security-Policy served with the shell (outer) page of any Freenet
/// webapp. The shell runs an inline postMessage bridge and embeds the real
/// webapp in a sandboxed iframe.
///
/// `connect-src` must include BOTH of:
///   - `'self'`: same-origin HTTP fetches (e.g. the `/permission/pending`
///     poller injected by `path_handlers`). Missing this was
///     freenet/freenet-core#3842 — every webapp logged a repeating CSP
///     violation every few seconds and permission prompts never surfaced.
///   - `ws:` / `wss:`: the bridge opens the real WebSocket on behalf of the
///     sandboxed iframe.
///
/// `worker-src 'self'` allows the shell to register the same-origin
/// notification service worker (`/freenet-notify-sw.js`). Without an explicit
/// `worker-src`, the worker source falls back through `child-src` (absent here)
/// to `script-src 'unsafe-inline'` — which permits inline scripts but NOT an
/// external script URL — so `navigator.serviceWorker.register` is CSP-blocked
/// and notifications can never show on mobile (where the page-level
/// `Notification` constructor is unsupported). `worker-src 'self'` is the
/// minimal directive that permits the same-origin worker.
const SHELL_PAGE_CSP: &str = "default-src 'none'; script-src 'unsafe-inline'; frame-src 'self'; style-src 'unsafe-inline'; img-src data:; connect-src 'self' ws: wss:; worker-src 'self'";

/// The notification service worker, served at `/freenet-notify-sw.js`. See the
/// file header for why a service worker is required: mobile browsers reject the
/// page-level `new Notification()` constructor, so the shell must show
/// notifications via `ServiceWorkerRegistration.showNotification()` instead.
const NOTIFY_SW_JS: &str = include_str!("path_handlers/assets/notify_sw.js");

/// The `sandbox` CSP directive served with EVERY response that carries
/// contract-authored bytes, so their opaque origin is decided here rather than
/// by whichever browsing context happens to embed them.
///
/// # Why this is not redundant with the iframe `sandbox` attribute
///
/// The attribute only constrains the frame *the shell creates*. Since the shell
/// iframe carries `allow-popups-to-escape-sandbox` (needed so `target="_blank"`
/// opens a real tab in every browser — see `navigation_interceptor.js`), a
/// contract can obtain a browsing context that the attribute does not reach:
///
/// 1. from a click, `window.open('about:blank')` — the popup escapes the
///    sandbox, so it is a top-level context with NO sandboxing flags, and its
///    `about:blank` document inherits the opener's origin, so the contract can
///    script it;
/// 2. in that popup, `document.write` an `<iframe src="…/contract/web/KEY/…">`.
///    That is a *nested* navigable, not a top-level document, so it carries
///    `Sec-Fetch-Dest: iframe` — and because its parent has no sandboxing
///    flags, it inherits none;
/// 3. the contract's own bytes therefore execute at the node's REAL origin:
///    `localStorage` (the hosted per-user access key), same-origin `fetch` of
///    any node route including another app's shell page and its auth token.
///
/// Confirmed reproducible in chromium, firefox and webkit before this header
/// existed; blocked in all three after. Step 2 works with any contract asset —
/// a scriptable `image/svg+xml`, or plain HTML the contract wrote — so gating
/// on `Sec-Fetch-Dest: document` alone does not close it. That is exactly the
/// escape #3818 removed `allow-popups-to-escape-sandbox` to prevent, and this
/// header is what allows the flag back.
///
/// The token list mirrors the iframe's `sandbox` attribute so in-frame
/// behaviour is unchanged: the effective policy is the intersection of the two,
/// and an app that works framed keeps working. Keep them in sync — the pin is
/// `shell_page_iframe_sandbox_matches_contract_content_csp` in
/// `path_handlers.rs`.
pub(super) const CONTRACT_CONTENT_SANDBOX_CSP: &str = "sandbox allow-scripts allow-forms allow-popups \
     allow-popups-to-escape-sandbox allow-downloads allow-modals";

/// The stricter variant for a contract asset loaded as a TOP-LEVEL document.
///
/// Nothing legitimate lands there — HTML sub-paths are routed to the shell and
/// `?__sandbox=1` top-level loads are redirected, so what remains is a URL a
/// contract navigated a tab to. An opaque origin alone would already deny it
/// the node's data; withholding `allow-scripts` additionally denies it a
/// scripted full-page UI displayed under the node's own address.
const CONTRACT_DOCUMENT_SANDBOX_CSP: &str = "sandbox";

/// Content-Security-Policy served with the sandboxed iframe that actually
/// runs a webapp. The iframe has an opaque (null) origin because the
/// sandbox attribute omits `allow-same-origin`, so CSP `'self'` would not
/// match the local API server's origin. We therefore interpolate the
/// concrete origin derived from the request Host header.
///
/// Prefixed with `CONTRACT_CONTENT_SANDBOX_CSP` so the opaque origin survives
/// being embedded somewhere other than the shell's own iframe — see that
/// constant for the attack it closes.
fn sandbox_csp_for_origin(origin: &str) -> String {
    format!(
        "{CONTRACT_CONTENT_SANDBOX_CSP}; default-src {origin} 'unsafe-inline' 'unsafe-eval' blob: data:; connect-src {origin} blob: data:"
    )
}

/// Derive the browser-facing origin to interpolate into the sandbox CSP from
/// the request headers, honoring a TLS-terminating reverse proxy's forwarded
/// scheme and host.
///
/// The CSP origin MUST match the origin the *browser* actually used, or the
/// CSP blocks the webapp's own assets. Behind a TLS proxy (the supported
/// hosted-mode deployment) the browser's origin is `https://<public-host>`,
/// while the node itself is reached over loopback `http`. We therefore:
///
/// - use `https` when `X-Forwarded-Proto: https` is present (set by the
///   trusted proxy — same forwarded-header trust model as the hosted-mode
///   token gate, which already requires the operator's proxy to set/strip
///   `X-Forwarded-*`); otherwise `http`.
/// - prefer `X-Forwarded-Host` over `Host` for the host:port, since a proxy
///   may rewrite the upstream `Host` to its loopback target (nginx does this
///   by default) while preserving the original in `X-Forwarded-Host`.
///
/// A direct connection carries neither forwarded header and falls back to the
/// previous behavior: `http://<Host>`. `'self'` only as a last resort when no
/// host is available at all.
///
/// Note: the forwarded headers are client-spoofable through a careless proxy,
/// but the CSP is defence-in-depth, not a trust boundary — a mismatched origin
/// only *breaks* the sandboxed app (too-strict CSP), it cannot widen what the
/// opaque-origin iframe may reach. So honoring them here is safe.
fn sandbox_origin_from_headers(headers: &axum::http::HeaderMap) -> String {
    let scheme = headers
        .get("x-forwarded-proto")
        .and_then(|v| v.to_str().ok())
        .map(|v| v.split(',').next().unwrap_or(v).trim())
        .filter(|v| v.eq_ignore_ascii_case("https"))
        .map(|_| "https")
        .unwrap_or("http");
    headers
        .get("x-forwarded-host")
        .or_else(|| headers.get(axum::http::header::HOST))
        .and_then(|h| h.to_str().ok())
        // `X-Forwarded-Host`, like `X-Forwarded-Proto`, may be a comma-separated
        // list when the request traverses multiple proxies (or a proxy appends
        // rather than overwrites). The first entry is the original client-facing
        // host; using the whole list would yield an invalid CSP origin like
        // `https://public.example, proxy.internal` and re-break the app.
        .map(|host| host.split(',').next().unwrap_or(host).trim())
        .filter(|host| !host.is_empty())
        .map(|host| format!("{scheme}://{host}"))
        .unwrap_or_else(|| "'self'".to_string())
}

pub(crate) mod hosted_export;
pub(crate) mod hosted_import;
pub(crate) mod hosted_migrate;
mod permission_prompts;
mod v1;
mod v2;

#[derive(Clone)]
pub(super) struct HttpClientApiRequest(mpsc::Sender<ClientConnection>);

impl std::ops::Deref for HttpClientApiRequest {
    type Target = mpsc::Sender<ClientConnection>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl HttpClientApiRequest {
    /// Constructs a request wrapper around an existing sender. Used by
    /// sibling-module tests that need to feed in a mocked channel.
    #[cfg(test)]
    pub(super) fn from_sender(sender: mpsc::Sender<ClientConnection>) -> Self {
        Self(sender)
    }
}

/// Represents an origin contract entry with metadata for token expiration.
#[derive(Clone, Debug)]
pub struct OriginContract {
    /// The contract instance ID
    pub contract_id: ContractInstanceId,
    /// The client ID associated with this token
    pub client_id: ClientId,
    /// Timestamp of when the token was last accessed (for expiration tracking)
    pub last_accessed: Instant,
}

impl OriginContract {
    /// Create a new origin contract entry
    pub fn new(contract_id: ContractInstanceId, client_id: ClientId) -> Self {
        Self {
            contract_id,
            client_id,
            last_accessed: Instant::now(),
        }
    }
}

/// Maps authentication tokens to origin contract metadata.
pub type OriginContractMap = Arc<DashMap<AuthToken, OriginContract>>;

/// Handles HTTP client requests for contract access and interaction.
pub struct HttpClientApi {
    pub(crate) origin_contracts: OriginContractMap,
    proxy_server_request: mpsc::Receiver<ClientConnection>,
    response_channels: HashMap<ClientId, mpsc::UnboundedSender<HostCallbackResult>>,
    /// Per-node route to the executor for the hosted-mode export endpoint
    /// (P3-live of #4381). Shared (same `Arc`) with the router's `Extension`;
    /// the node fills it at startup via `set_op_manager`.
    export_op_manager: hosted_export::ExportOpManagerHandle,
}

impl HttpClientApi {
    /// Returns the uninitialized axum router to compose with other routing handling or websockets.
    pub fn as_router(socket: &SocketAddr) -> (Self, Router) {
        let origin_contracts = Arc::new(DashMap::new());
        Self::as_router_with_origin_contracts(
            socket,
            origin_contracts,
            crate::contract::user_input::pending_prompts(),
            // Standalone composition with no node config behind it (local-node
            // mode, tests). Falls back to the same directory `build()` would
            // have derived.
            crate::config::default_webapp_cache_dir(),
        )
    }

    /// Returns the uninitialized axum router with a provided origin_contracts map.
    ///
    /// Merges V1 and V2 HTTP routes; both currently share the same handler logic.
    pub(crate) fn as_router_with_origin_contracts(
        socket: &SocketAddr,
        origin_contracts: OriginContractMap,
        pending_prompts: crate::contract::user_input::PendingPrompts,
        webapp_cache_dir: std::path::PathBuf,
    ) -> (Self, Router) {
        // Controls the cookie Secure flag: when true, cookies are sent over HTTP
        // (no HTTPS required). Includes is_unspecified() so that 0.0.0.0 bindings
        // (network mode) allow HTTP cookies — most home users lack TLS.
        let localhost = socket.ip().is_loopback() || socket.ip().is_unspecified();

        // NOTE: do NOT re-add a `create_dir_all` here. Until #5291 this function
        // created `$TMPDIR/freenet/webs` and PANICKED the node if it could not —
        // a directory that stopped being read in April 2025 when the real
        // webapp cache moved to `webapp_cache` (and later to the XDG cache dir).
        // The only surviving effect was aborting startup: it took down the
        // v0.2.124 release canary, which stages its binary at `$TMPDIR/freenet`,
        // so the mkdir hit ENOTDIR. Unpacked web contracts live under the
        // config-driven `webapp_cache_dir` below, and `WebappCache::with_root`
        // creates it — warning rather than aborting, which is the right
        // response to a directory the node can serve everything else without.
        let (proxy_request_sender, request_to_server) = mpsc::channel(1);

        let config = Config::new(localhost, webapp_cache_dir);

        // Per-node route to the executor for the hosted-mode export endpoint.
        // The SAME handle is injected as a request `Extension` (read by the
        // export handler) and stored in the returned `HttpClientApi` (filled by
        // the node via `set_op_manager`).
        let export_op_manager = hosted_export::ExportOpManagerHandle::default();

        // Per-node store of one-time migration pull tokens (#4592 magic-link
        // path). Injected as an Extension below; the hosted mint fills it and
        // the public pull drains it.
        let migrate_store = hosted_migrate::MigratePullStore::default();

        let router = Router::new()
            .route("/", axum::routing::get(home_page::homepage))
            .route(
                "/peer/{address}",
                axum::routing::get(home_page::peer_detail),
            )
            .route(
                "/contract/{key}",
                axum::routing::get(home_page::contract_detail),
            )
            // Notification service worker, served at the origin root so its
            // default scope (`/`) covers every contract shell page. The shell
            // registers it to show notifications via showNotification() — the
            // only path that works on mobile browsers (see notify_sw.js).
            .route(
                "/freenet-notify-sw.js",
                axum::routing::get(notify_service_worker),
            )
            // Local peer's migration confirmation page (#4592). First-party
            // origin so its POST to `pull-import` passes the import gate; it
            // never auto-pulls (an explicit click is required).
            .route(
                "/hosted/import",
                axum::routing::get(hosted_migrate::import_page),
            )
            .merge(v1::routes(config.clone()))
            .merge(v2::routes(config))
            // Hosted-mode "export my data" endpoint (P3-live of #4381). Gated
            // by the same refuse-plaintext-token check as the WS userToken; see
            // `hosted_export`. Reaches the node via the per-node
            // `ExportOpManagerHandle` Extension below.
            .merge(hosted_export::routes(ApiVersion::V1))
            .merge(hosted_export::routes(ApiVersion::V2))
            // Live "import my data" endpoint (P3-live of #4592). Writes secrets,
            // so it is gated to the node's own dashboard origin over loopback
            // (see `hosted_import`). Reuses the SAME per-node
            // `ExportOpManagerHandle` Extension below to reach the executor.
            .merge(hosted_import::routes(ApiVersion::V1))
            .merge(hosted_import::routes(ApiVersion::V2))
            // Zero-friction magic-link migration (#4592): hosted mint + public
            // pull + local pull-import. Reuses the export/import gates and the
            // per-node `ExportOpManagerHandle`; the pull-token store below.
            .merge(hosted_migrate::routes(ApiVersion::V1))
            .merge(hosted_migrate::routes(ApiVersion::V2))
            .merge(permission_prompts::routes())
            .layer(Extension(origin_contracts.clone()))
            .layer(Extension(pending_prompts))
            .layer(Extension(export_op_manager.clone()))
            .layer(Extension(migrate_store))
            .layer(Extension(HttpClientApiRequest(proxy_request_sender)));

        (
            Self {
                proxy_server_request: request_to_server,
                origin_contracts,
                response_channels: HashMap::new(),
                export_op_manager,
            },
            router,
        )
    }

    /// Returns a reference to the origin contracts map (for integration testing).
    /// This allows tests to verify token expiration behavior.
    pub fn origin_contracts(&self) -> &OriginContractMap {
        &self.origin_contracts
    }
}

#[derive(Clone, Debug)]
struct Config {
    localhost: bool,
    /// This node's unpacked-webapp cache. Built once per server from
    /// `WebsocketApiConfig::webapp_cache_dir` and shared by every request, so
    /// the LRU sweep's debounce and in-progress flag are per-node rather than
    /// per-request. Threaded rather than read from a global because the sweep
    /// DELETES — see [`path_handlers::WebappCache`].
    webapp_cache: path_handlers::WebappCache,
}

impl Config {
    fn new(localhost: bool, webapp_cache_dir: std::path::PathBuf) -> Self {
        Self {
            localhost,
            webapp_cache: path_handlers::WebappCache::with_root(webapp_cache_dir),
        }
    }

    #[cfg(test)]
    fn webapp_cache_root(&self) -> &std::path::Path {
        self.webapp_cache.root()
    }
}

#[instrument(level = "debug")]
async fn home() -> axum::response::Response {
    axum::response::Response::default()
}

/// `GET /freenet-notify-sw.js` — serves the notification service worker.
///
/// The gateway shell registers this worker so it can call
/// `ServiceWorkerRegistration.showNotification()`, which is the ONLY way to
/// display a web notification on mobile browsers (they reject the page-level
/// `new Notification()` constructor). It is served at the ORIGIN ROOT on
/// purpose: a service worker's default scope is the directory of its script, so
/// `/freenet-notify-sw.js` gets scope `/`, which covers every
/// `/v{1,2}/contract/web/<key>/` shell page with a single registration. The
/// worker has no `fetch` handler, so it never intercepts or alters any request.
async fn notify_service_worker() -> impl IntoResponse {
    (
        [
            (
                axum::http::header::CONTENT_TYPE,
                "text/javascript; charset=utf-8",
            ),
            // Modest cache; a new binary rolls out an updated worker within the
            // hour, and the browser also revalidates the worker script on
            // navigation regardless of this header.
            (axum::http::header::CACHE_CONTROL, "max-age=3600"),
            (axum::http::header::X_CONTENT_TYPE_OPTIONS, "nosniff"),
            // Defense-in-depth: the worker makes NO network requests (no fetch,
            // no importScripts), so lock its own execution context to nothing.
            // showNotification()/clients/postMessage are JS API calls, not
            // CSP-governed resource loads, so this does not affect it.
            (
                axum::http::header::CONTENT_SECURITY_POLICY,
                "default-src 'none'",
            ),
        ],
        NOTIFY_SW_JS,
    )
}

#[allow(clippy::too_many_arguments)]
async fn web_home(
    Path(key): Path<String>,
    Extension(rs): Extension<HttpClientApiRequest>,
    axum::extract::State(config): axum::extract::State<Config>,
    req_headers: axum::http::HeaderMap,
    api_version: ApiVersion,
    query_string: Option<String>,
    hosted_mode: bool,
    source_addr: Option<std::net::SocketAddr>,
) -> Result<axum::response::Response, WebSocketApiError> {
    // Check if this is the sandboxed iframe requesting its content
    let is_sandbox = query_string
        .as_ref()
        .map(|qs| qs.split('&').any(|p| p == "__sandbox=1"))
        .unwrap_or(false);

    if is_sandbox {
        return serve_sandbox_response(
            key,
            api_version,
            None,
            query_string.as_deref(),
            &req_headers,
            rs,
            &config.webapp_cache,
        )
        .await;
    }

    // Root document load: render the shell that wraps the contract root.
    render_shell_response(
        key,
        &config,
        api_version,
        query_string,
        None,
        rs,
        hosted_mode,
        source_addr,
    )
    .await
}

/// Generates a shell page response: a fresh auth token + secure cookie,
/// the contract fetched/unpacked into the cache, and the iframe wrapper
/// with the shell CSP and anti-framing headers.
///
/// Shared by `web_home` (root document loads, `sub_path = None`) and the
/// deep-link branch of `web_subpages` (`sub_path = Some(path)` so a
/// reloaded/bookmarked sub-page URL renders the shell pointed at that
/// page instead of 404ing or being flattened to the contract root — see
/// freenet/freenet-core#3841). Both must issue the SAME credentials and
/// headers; factoring it here keeps the deep-link path from drifting out
/// of sync with the root path (e.g. forgetting the cookie or the CSP).
#[allow(clippy::too_many_arguments)]
async fn render_shell_response(
    key: String,
    config: &Config,
    api_version: ApiVersion,
    query_string: Option<String>,
    sub_path: Option<&str>,
    rs: HttpClientApiRequest,
    hosted_mode: bool,
    // Peer address of the requesting connection, for the issuance audit log.
    // `None` only where no `ConnectInfo` is installed (standalone test routers).
    source_addr: Option<std::net::SocketAddr>,
) -> Result<axum::response::Response, WebSocketApiError> {
    use headers::{Header, HeaderMapExt};

    // Shell page: generate auth token, serve iframe wrapper with CSP
    let token = AuthToken::generate();

    // AUDIT (GHSA-824h-7x5x-wfmf): this is THE issuance point for an
    // app-identity auth token — the node mints one on request for ANY contract
    // id, and whoever holds it is thereafter attested as that app. Record every
    // issuance so the trail exists after the fact.
    //
    // `info!` deliberately, not `debug!`: the crate builds with
    // `release_max_level_info`, so a `debug!` here would be compiled out of
    // every shipped binary and the audit log would exist only in development.
    //
    // Ids and addresses ONLY. The token itself is never logged — it IS the
    // credential — and neither is any key material.
    //
    // LOG THE PARSED ID, NEVER THE RAW PATH. `key` is an unvalidated
    // `Path<String>` that axum has already percent-DECODED, and this route is
    // exposed publicly through the hosted proxy, so logging it raw would let any
    // internet user inject newlines (`%0A`) and forge entries inside the very
    // audit trail this line exists to produce. Parsing first bounds the value to
    // 32 base58-encoded bytes with no control characters. An unparseable key is
    // not logged here at all — it cannot yield a token, and the request fails
    // below.
    if let Ok(instance_id) = ContractInstanceId::from_base58(&key) {
        tracing::info!(
            contract_id = %instance_id,
            peer_addr = ?source_addr,
            api_version = %api_version.prefix(),
            "Issued app-identity auth token for a contract shell page"
        );
    }

    let auth_header = headers::Authorization::<headers::authorization::Bearer>::name().to_string();
    let version_prefix = api_version.prefix();
    // Don't set a cookie domain — the browser will default to the request's origin host,
    // which works for both localhost and remote access.
    let cookie = cookie::Cookie::build((auth_header, format!("Bearer {}", token.as_str())))
        .path(format!("/{version_prefix}/contract/web/{key}"))
        .same_site(cookie::SameSite::Strict)
        .max_age(cookie::time::Duration::days(1))
        .secure(!config.localhost)
        .http_only(false)
        .build();

    let token_header = headers::Authorization::bearer(token.as_str()).unwrap();
    let contract_response = path_handlers::contract_home(
        key,
        rs,
        token.clone(),
        api_version,
        query_string,
        sub_path,
        hosted_mode,
        &config.webapp_cache,
    )
    .await?;

    let mut response = contract_response.into_response();
    response.headers_mut().typed_insert(token_header);
    response.headers_mut().insert(
        headers::SetCookie::name(),
        headers::HeaderValue::from_str(&cookie.to_string()).unwrap(),
    );
    // CSP: shell page runs the inline postMessage bridge script, embeds a sandboxed
    // iframe, and opens the real WebSocket on behalf of the iframe. connect-src must
    // allow:
    //   - 'self' so the shell can fetch same-origin endpoints (e.g. the
    //     /permission/pending poller injected by path_handlers). Without it
    //     every Freenet webapp logs a repeating CSP violation every few
    //     seconds and the permission-prompt overlay never appears.
    //   - ws: / wss: so the bridge can open the real WebSocket on behalf of
    //     the sandboxed iframe.
    response.headers_mut().insert(
        axum::http::header::CONTENT_SECURITY_POLICY,
        axum::http::HeaderValue::from_static(SHELL_PAGE_CSP),
    );
    // Shell page must not be framed itself
    response.headers_mut().insert(
        axum::http::header::X_FRAME_OPTIONS,
        axum::http::HeaderValue::from_static("DENY"),
    );
    response.headers_mut().insert(
        axum::http::header::X_CONTENT_TYPE_OPTIONS,
        axum::http::HeaderValue::from_static("nosniff"),
    );

    Ok(response)
}

/// Reads the `HostedMode` flag for the contract-web (shell) route, TOLERANTLY.
///
/// The shell route is the per-user-token *minting/UX* point, not the security
/// gate. Unlike the WebSocket `connection_info` middleware — where a missing
/// `Extension<HostedMode>` MUST fail loud (a dropped flag there could silently
/// put public users on a shared Local namespace) — this route must fail SAFE to
/// off: absent extension ⇒ `HostedMode(false)` ⇒ the shell mints no userToken ⇒
/// unchanged (non-hosted) behavior.
///
/// Why tolerant: only `serve_client_api_in_impl` installs the `Extension`. The
/// public `HttpClientApi::as_router` composition path returns a router WITHOUT
/// it, so a required extractor here would make `/v{1,2}/contract/web/...` reject
/// with a missing-extension 500 even for plain sandbox requests — a regression
/// for that supported standalone composition mode. The production serve path
/// still installs the real `Extension`, so hosted mode works there.
fn hosted_mode_or_default(ext: Option<Extension<crate::server::HostedMode>>) -> bool {
    ext.map(|Extension(hm)| hm.0).unwrap_or(false)
}

#[allow(clippy::too_many_arguments)]
async fn web_subpages(
    key: String,
    last_path: String,
    api_version: ApiVersion,
    query_string: Option<String>,
    req_headers: axum::http::HeaderMap,
    config: &Config,
    request_sender: HttpClientApiRequest,
    hosted_mode: bool,
    source_addr: Option<std::net::SocketAddr>,
) -> Result<axum::response::Response, WebSocketApiError> {
    let is_sandbox = query_string
        .as_ref()
        .map(|qs| qs.split('&').any(|p| p == "__sandbox=1"))
        .unwrap_or(false);

    // For sandbox sub-page requests to HTML files, serve through the sandbox
    // content pipeline (with WebSocket shim + navigation interceptor injected).
    if is_sandbox && is_html_page(&last_path) {
        return serve_sandbox_response(
            key,
            api_version,
            Some(&last_path),
            query_string.as_deref(),
            &req_headers,
            request_sender,
            &config.webapp_cache,
        )
        .await;
    }

    // Top-level document loads of contract HTML sub-paths (pasted URLs,
    // bookmarks, cross-contract link clicks, deep-link reloads) must be
    // served the shell page — NOT the raw contract HTML — so the response
    // carries a fresh auth token + cookie and wraps the contract in the
    // sandbox iframe. Otherwise `variable_content` serves raw HTML with no
    // `authToken`, breaking webapps that read credentials from
    // `location.search` (freenet/river#208: Delta links hit "Connection
    // failed" on the destination).
    //
    // The sub-path is threaded into shell generation so the iframe's
    // `data-src` points at `/{prefix}/contract/web/{key}/{sub_path}?__sandbox=1`,
    // landing the in-iframe webapp on the requested route instead of the
    // contract root. This is the fuller fix for path-based deep-link reload
    // (#3841): previously this branch issued a 303 redirect to the shell
    // root, which preserved URL fragments (enough for hash-routed apps like
    // Delta) but flattened path-routed deep links to `/`. Rendering the
    // shell in place preserves the path AND avoids the extra round-trip.
    //
    // Clients without `Sec-Fetch-Dest` (curl, older browsers) intentionally
    // fall through to `variable_content` — matches pre-PR behaviour and
    // avoids changing the response shape for non-browser clients.
    let fetch_dest = req_headers
        .get("sec-fetch-dest")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("");
    if should_serve_shell_for_subpage(is_sandbox, &last_path, fetch_dest) {
        return render_shell_response(
            key,
            config,
            api_version,
            query_string,
            Some(&last_path),
            request_sender,
            hosted_mode,
            source_addr,
        )
        .await;
    }

    let version_prefix = api_version.prefix();
    let full_path: String = format!("/{version_prefix}/contract/web/{key}/{last_path}");
    let result = path_handlers::variable_content(
        key,
        full_path,
        api_version,
        request_sender,
        &config.webapp_cache,
    )
    .await
    .map_err(|e| *e);
    // Attach the sandbox CORS headers to BOTH success and error responses. The
    // sandboxed iframe has a null origin, so a subresource response WITHOUT
    // `Access-Control-Allow-Origin` is reported by the browser as an opaque
    // "CORS error" that masks the real status — a plain 404 for a missing asset
    // (or a 400 for a rejected path) looked like a CORS failure to the app
    // (user report: SUB0PT1MAL / cirro, 2026-07-29). Building the response from
    // the error here, and then adding the headers, lets the app see the true
    // status with CORS allowed.
    //
    // SECURITY: error bodies on this route are now cross-origin-readable by a
    // malicious contract's iframe JS. Keep them non-sensitive — reflected
    // request path and generic io/parse messages only. Any new error path here
    // MUST NOT embed internal filesystem paths, config, or secrets.
    let mut response = match result {
        Ok(r) => r.into_response(),
        Err(e) => e.into_response(),
    };
    add_sandbox_cors_headers(&mut response);
    // Everything served from here is contract-authored, so it is sandboxed
    // unconditionally: the node decides its origin, not whichever context
    // embeds it. See `CONTRACT_CONTENT_SANDBOX_CSP` for why the iframe's
    // `sandbox` attribute is not sufficient on its own, and why keying this on
    // `Sec-Fetch-Dest: document` would leave the hole open — a contract that
    // escapes to an unsandboxed popup embeds these bytes as an `iframe` dest,
    // not a `document` one.
    //
    // Two shapes, because a top-level document is the one case where nothing
    // legitimate arrives:
    //   - `document`: no `allow-scripts`. A contract-authored `evil.svg`, served
    //     as `image/svg+xml`, executes script when it IS the document, and
    //     `nosniff` does not help — the type is genuinely scriptable.
    //   - anything else: the full token list, matching the app iframe, so the
    //     app's own subresources and any HTML it frames itself behave exactly
    //     as before. On a non-document response the `sandbox` directive has no
    //     effect at all (it applies to documents and workers), so this is inert
    //     for scripts, styles and images; it is the `iframe`/`embed`/`object`
    //     and header-less cases it is there for.
    let sandbox_csp = if fetch_dest == "document" {
        CONTRACT_DOCUMENT_SANDBOX_CSP
    } else {
        CONTRACT_CONTENT_SANDBOX_CSP
    };
    response.headers_mut().insert(
        axum::http::header::CONTENT_SECURITY_POLICY,
        axum::http::HeaderValue::from_static(sandbox_csp),
    );
    Ok(response)
}

/// Builds a 303 redirect to the contract's shell root, preserving
/// inbound query parameters (minus sensitive ones that must never be
/// attacker-controlled across the redirect).
///
/// Validates `key` as a `ContractInstanceId` before interpolating into
/// the `Location` header. A crafted path containing e.g. percent-encoded
/// CRLF would otherwise reach `HeaderValue::try_from` inside
/// `Redirect::to`, which panics on invalid header values. Returning a
/// structured `InvalidParam` error instead keeps the handler panic-free
/// and lets axum serialise a normal 4xx response.
///
/// `__sandbox=1` is stripped so a crafted deep link cannot land the
/// victim inside `web_home`'s sandbox branch and bypass shell
/// generation entirely. `authToken` is stripped so a malicious
/// cross-contract link cannot inject a token into the destination
/// shell's `location.search`; the shell generates its own token via
/// `AuthToken::generate()` and any forwarded value would only mislead
/// webapps that read credentials from the URL.
fn redirect_to_shell_root(
    key: &str,
    api_version: ApiVersion,
    query_string: Option<&str>,
) -> Result<axum::response::Response, WebSocketApiError> {
    let shell_url = build_canonical_shell_url(key, api_version, None, query_string)?;
    Ok(axum::response::Redirect::to(&shell_url).into_response())
}

/// Validate `key`, filter sensitive query params, and return the
/// canonical `/{prefix}/contract/web/{key}/[?query]` URL. Shared
/// between `redirect_to_shell_root` (303 See Other for cross-contract
/// HTML subpath redirects) and the no-trailing-slash redirect handlers
/// in `v1.rs`/`v2.rs` (308 Permanent Redirect for canonical URL form,
/// freenet/freenet-core#4019).
///
/// `key` is interpolated into a `Location` header by the caller, so
/// validation MUST reject CRLF-bearing input here before
/// `HeaderValue::try_from` ever sees it. The check via
/// `ContractInstanceId::from_base58` also rejects path-traversal-style
/// inputs like `../../etc/passwd` that would point the redirect at an
/// attacker-chosen URL on the reader's gateway.
///
/// Sensitive query params (`__sandbox`, `authToken`) are stripped:
/// `__sandbox` would otherwise drop the redirect victim straight into
/// `web_home`'s sandbox branch, bypassing shell generation; `authToken`
/// is the shell's auth credential, which must only come from
/// `AuthToken::generate()` and never from an attacker-supplied URL.
pub(super) fn build_canonical_shell_url(
    key: &str,
    api_version: ApiVersion,
    sub_path: Option<&str>,
    query_string: Option<&str>,
) -> Result<String, WebSocketApiError> {
    if key.is_empty() {
        return Err(WebSocketApiError::InvalidParam {
            error_cause: "empty contract key in redirect target".into(),
        });
    }
    let _instance_id =
        ContractInstanceId::from_base58(key).map_err(|err| WebSocketApiError::InvalidParam {
            error_cause: format!("invalid contract key in redirect target: {err}"),
        })?;

    let filtered_query = query_string
        .map(|qs| {
            qs.split('&')
                .filter(|p| !p.is_empty())
                .filter(|p| !is_sensitive_query_param(p))
                .collect::<Vec<_>>()
                .join("&")
        })
        .filter(|s| !s.is_empty());

    // A sub-path lands the shell on the page that was actually requested
    // instead of the contract root. Sanitized with the SAME check `shell_page`
    // applies before interpolating it into the iframe `data-src`, so a crafted
    // path cannot break out of the URL's path component and into the `Location`
    // header (`?`, `#`, control chars, CRLF, `.`/`..` segments are all
    // rejected).
    let sub_path = sub_path
        .filter(|sp| !sp.is_empty())
        .map(path_handlers::sanitize_shell_sub_path)
        .transpose()?
        .unwrap_or_default();

    let prefix = api_version.prefix();
    Ok(match filtered_query {
        Some(qs) => format!("/{prefix}/contract/web/{key}/{sub_path}?{qs}"),
        None => format!("/{prefix}/contract/web/{key}/{sub_path}"),
    })
}

/// Builds a 303 redirect to the shell for a specific contract SUB-PAGE,
/// preserving the inbound query minus the sensitive params.
///
/// Used when a `?__sandbox=1` URL is loaded as a top-level document. Redirecting
/// to the contract ROOT there is lossy in a way users notice: an app that opens
/// its own current page in a new tab (`window.open(location.href)`, or a
/// hash-only open that inherits `__sandbox=1` from the base) lands on the
/// contract root with its query dropped — losing, for example, an invitation
/// parameter. Before #5100 the interceptor's `window.open` override hid this by
/// stripping `__sandbox` and forwarding the clean URL; the override is gone, so
/// the server has to land the redirect on the right page itself.
fn redirect_to_shell_sub_page(
    key: &str,
    api_version: ApiVersion,
    sub_path: Option<&str>,
    query_string: Option<&str>,
) -> Result<axum::response::Response, WebSocketApiError> {
    let shell_url = build_canonical_shell_url(key, api_version, sub_path, query_string)?;
    Ok(axum::response::Redirect::to(&shell_url).into_response())
}

/// Query parameters that must be stripped before forwarding a user URL
/// into the shell. `__sandbox` is a server-interpreted routing flag;
/// `authToken` is the shell's auth credential and must only come from
/// `AuthToken::generate()`, never from an attacker-controlled URL.
///
/// The NAME is percent-decoded before the check. A raw prefix match is
/// bypassable by encoding one character of the name — `authT%6Fken=evil`
/// survives it, and `new URLSearchParams(...).get("authToken")` in the iframe
/// then returns `evil`, which is exactly the webapp-reads-its-credential-from-
/// `location.search` case this exists to prevent. Only the name is decoded; the
/// value is forwarded byte-for-byte, since re-encoding it could break a signed
/// or opaque app parameter.
pub(super) fn is_sensitive_query_param(param: &str) -> bool {
    // Prefix-match so variants like `__sandbox_debug` or `authTokenExtra`
    // (from a future refactor or an adversarial URL) are also stripped.
    // Shared with `path_handlers::shell_page`, which forwards query params into
    // the iframe — the two filters used to be separate copies of this rule.
    let name = param.split('=').next().unwrap_or(param);
    let decoded = percent_decode_ascii(name);
    decoded.starts_with("__sandbox") || decoded.starts_with("authToken")
}

/// Percent-decodes the ASCII escapes in a query-parameter NAME so it can be
/// compared against a literal. Deliberately minimal: invalid escapes are left
/// as-is (they cannot form the names we are looking for), and non-ASCII bytes
/// are passed through, because the only decision this feeds is a prefix match
/// against two ASCII literals.
fn percent_decode_ascii(s: &str) -> String {
    let bytes = s.as_bytes();
    let mut out = String::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        // Read the two hex digits as BYTES. Slicing `&s[i + 1..i + 3]` would
        // panic whenever `%` is followed by a multi-byte character (`%é`), and
        // a query string is attacker-controlled on every request.
        if let (Some(b'%'), Some(hi), Some(lo)) = (
            bytes.get(i).copied(),
            bytes.get(i + 1).copied(),
            bytes.get(i + 2).copied(),
        ) {
            if let (Some(hi), Some(lo)) = ((hi as char).to_digit(16), (lo as char).to_digit(16)) {
                out.push((hi as u8 * 16 + lo as u8) as char);
                i += 3;
                continue;
            }
        }
        // Byte-wise passthrough: a non-ASCII byte becomes its Latin-1 char.
        // That mangles multi-byte text, which is fine and deliberate — the only
        // consumer prefix-compares the result against two ASCII literals, and a
        // mangled non-ASCII name cannot equal either.
        out.push(bytes[i] as char);
        i += 1;
    }
    out
}

/// Returns true if a contract sub-path request is a top-level HTML
/// document load that must be served the shell page (with the sub-path
/// threaded into the iframe), rather than raw contract HTML.
///
/// Only top-level document loads (`Sec-Fetch-Dest: document`) of HTML
/// sub-paths are ambiguous with pasted URLs, bookmarks, cross-contract
/// link clicks, and deep-link reloads; serving them the shell guarantees
/// a fresh auth token + cookie and the sandbox wrapper while preserving
/// the requested path in the iframe's `data-src` (#3841). Sandbox iframe
/// requests (`__sandbox=1`) already flow through the sandbox pipeline and
/// are never handled here. Sub-resource fetches (`Sec-Fetch-Dest` =
/// `script`/`style`/`image`/`empty`/...) and non-HTML asset paths fall
/// through to `variable_content` so real assets still 404 normally.
fn should_serve_shell_for_subpage(is_sandbox: bool, last_path: &str, fetch_dest: &str) -> bool {
    !is_sandbox && is_html_page(last_path) && fetch_dest == "document"
}

/// Returns true if the path looks like an HTML page request.
///
/// Matches `.html`/`.htm` extensions, directory-style paths (`news/`),
/// and extensionless paths (`about/team`) that likely resolve to `index.html`.
fn is_html_page(path: &str) -> bool {
    let lower = path.to_lowercase();
    lower.ends_with(".html")
        || lower.ends_with(".htm")
        || lower.ends_with('/')
        || !lower.contains('.')
}

/// Serves sandbox content (contract HTML + WS shim) inside the iframe and adds
/// the appropriate CORS and CSP headers.
///
/// Shared by `web_home` (for the root page) and `web_subpages` (for sub-pages).
/// No auth token or cookie -- the shell page handles auth via postMessage.
///
/// Includes `Sec-Fetch-Dest` check: if a sandbox URL is loaded as a top-level
/// document (e.g. pasted in the address bar), redirect to the shell page instead
/// of serving raw sandbox content outside the iframe.
#[allow(clippy::too_many_arguments)]
async fn serve_sandbox_response(
    key: String,
    api_version: ApiVersion,
    sub_path: Option<&str>,
    query_string: Option<&str>,
    req_headers: &axum::http::HeaderMap,
    request_sender: HttpClientApiRequest,
    webapp_cache: &path_handlers::WebappCache,
) -> Result<axum::response::Response, WebSocketApiError> {
    // Block top-level navigation to sandbox URLs. Sec-Fetch-Dest: iframe is set
    // by the browser automatically and cannot be spoofed by scripts.
    let fetch_dest = req_headers
        .get("sec-fetch-dest")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("");
    if fetch_dest == "document" {
        // Land on the requested page, not the contract root — see
        // `redirect_to_shell_sub_page`. A sub-path the sanitizer rejects falls
        // back to the root rather than erroring: this branch exists to keep raw
        // sandbox content off a top-level document, and that job is done by
        // redirecting at all, whatever the target.
        return redirect_to_shell_sub_page(&key, api_version, sub_path, query_string)
            .or_else(|_| redirect_to_shell_root(&key, api_version, query_string));
    }

    let contract_response = match path_handlers::serve_sandbox_content(
        key,
        api_version,
        sub_path,
        request_sender,
        webapp_cache,
    )
    .await
    {
        Ok(r) => r.into_response(),
        Err(e) => {
            // Same null-origin reasoning as `web_subpages`: an error subresource
            // response without CORS is surfaced by the browser as an opaque
            // "CORS error" inside the iframe, masking the real status. Attach the
            // sandbox CORS headers so the app sees the true 4xx/5xx. (CSP is only
            // meaningful on served content, so it is skipped for the error.)
            //
            // SECURITY: as in `web_subpages`, these error bodies are now
            // cross-origin-readable by a malicious contract's iframe JS. Keep
            // them non-sensitive — any new error path here MUST NOT embed
            // internal filesystem paths, config, or secrets.
            let mut response = e.into_response();
            add_sandbox_cors_headers(&mut response);
            // The body reflects the request path, so sandbox it too rather than
            // reasoning about whether the current error renderer can be coaxed
            // into emitting markup. Origin-CSP is skipped (there is no contract
            // content to load subresources for); the sandbox directive is the
            // part that matters.
            response.headers_mut().insert(
                axum::http::header::CONTENT_SECURITY_POLICY,
                axum::http::HeaderValue::from_static(CONTRACT_CONTENT_SANDBOX_CSP),
            );
            return Ok(response);
        }
    };
    let mut response = contract_response;
    add_sandbox_cors_headers(&mut response);
    // See `sandbox_csp_for_origin` for why we interpolate a concrete origin
    // rather than using `'self'`, and `sandbox_origin_from_headers` for why we
    // honor the proxy's forwarded scheme/host (so the CSP matches the browser's
    // real `https://` origin behind a TLS-terminating reverse proxy).
    let local_api_origin = sandbox_origin_from_headers(req_headers);
    let csp = sandbox_csp_for_origin(&local_api_origin);
    if let Ok(csp_value) = axum::http::HeaderValue::from_str(&csp) {
        response
            .headers_mut()
            .insert(axum::http::header::CONTENT_SECURITY_POLICY, csp_value);
    }
    Ok(response)
}

/// Adds CORS and security headers needed for sandbox iframe responses.
///
/// Sandboxed iframes have a null origin, so sub-resource requests require
/// `Access-Control-Allow-Origin: *` to load correctly.
fn add_sandbox_cors_headers(response: &mut axum::response::Response) {
    response.headers_mut().insert(
        axum::http::header::ACCESS_CONTROL_ALLOW_ORIGIN,
        axum::http::HeaderValue::from_static("*"),
    );
    response.headers_mut().insert(
        axum::http::header::X_CONTENT_TYPE_OPTIONS,
        axum::http::HeaderValue::from_static("nosniff"),
    );
}

impl ClientEventsProxy for HttpClientApi {
    #[instrument(level = "debug", skip(self))]
    fn recv(&mut self) -> BoxFuture<'_, Result<OpenRequest<'static>, ClientError>> {
        async move {
            while let Some(msg) = self.proxy_server_request.recv().await {
                match msg {
                    ClientConnection::NewConnection {
                        callbacks,
                        assigned_token,
                    } => {
                        let cli_id = ClientId::next();
                        callbacks
                            .send(HostCallbackResult::NewId { id: cli_id })
                            .map_err(|_e| ErrorKind::NodeUnavailable)?;
                        if let Some((assigned_token, contract)) = assigned_token {
                            let origin = OriginContract::new(contract, cli_id);
                            self.origin_contracts.insert(assigned_token.clone(), origin);
                            tracing::debug!(
                                ?assigned_token,
                                ?contract,
                                ?cli_id,
                                "Stored assigned token in origin_contracts map"
                            );
                        }
                        self.response_channels.insert(cli_id, callbacks);
                        continue;
                    }
                    ClientConnection::Request {
                        client_id,
                        req,
                        auth_token,
                        origin_contract,
                        // Forwarded explicitly rather than swallowed by `..`.
                        // Every request on THIS proxy is node-internal today
                        // (webapp-cache fetches, which carry no origin), so
                        // dropping it would be inert — but the producers set it
                        // deliberately, and a future client-facing request here
                        // would otherwise lose its attestation silently.
                        connection_scope,
                        user_context,
                        ..
                    } => {
                        return Ok(OpenRequest::new(client_id, req)
                            .with_token(auth_token)
                            .with_origin_contract(origin_contract)
                            .with_connection_scope(connection_scope)
                            .with_user_context(user_context));
                    }
                }
            }
            tracing::warn!("Shutting down HTTP client API receiver");
            Err(ErrorKind::Disconnect.into())
        }
        .boxed()
    }

    #[instrument(level = "debug", skip(self))]
    fn send(
        &mut self,
        id: ClientId,
        result: Result<HostResponse, ClientError>,
    ) -> BoxFuture<'_, Result<(), ClientError>> {
        async move {
            if let Some(ch) = self.response_channels.remove(&id) {
                let should_rm = result
                    .as_ref()
                    .map_err(|err| matches!(err.kind(), ErrorKind::Disconnect))
                    .err()
                    .unwrap_or(false);
                if ch.send(HostCallbackResult::Result { id, result }).is_ok() && !should_rm {
                    // still alive connection, keep it
                    self.response_channels.insert(id, ch);
                } else {
                    tracing::info!("dropped connection to client #{id}");
                }
            } else {
                tracing::warn!("client: {id} not found");
            }
            Ok(())
        }
        .boxed()
    }

    fn set_op_manager(&self, op_manager: &dyn std::any::Any) {
        // Wire the export endpoint's per-node handle (shared with the router
        // Extension) to the live node. See `hosted_export`. The caller passes an
        // `Arc<OpManager>` behind `&dyn Any` to keep the `pub(crate)` `OpManager`
        // out of the public `ClientEventsProxy` signature; downcast it here.
        if let Some(op_manager) = op_manager.downcast_ref::<Arc<crate::node::OpManager>>() {
            self.export_op_manager.set(op_manager);
        } else {
            tracing::error!(
                "HttpClientApi::set_op_manager called with a non-OpManager argument; \
                 hosted export will be unavailable on this node"
            );
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Builds an `HttpClientApiRequest` whose receiver is dropped immediately,
    /// so any attempt to send on it fails fast with a closed-channel error.
    /// Suitable for redirect-branch tests that must never reach the full
    /// fetch/unpack pipeline — the handler short-circuits to a redirect before
    /// the sender is used, and if it doesn't the send error surfaces as a
    /// bounded test failure instead of a 30-second timeout.
    fn dead_request_sender() -> HttpClientApiRequest {
        let (tx, _rx) = mpsc::channel(1);
        HttpClientApiRequest::from_sender(tx)
    }

    /// Regression for #5291: composing the router must not abort the process
    /// because something in the system temp directory is in the way.
    ///
    /// `as_router_with_origin_contracts` used to `create_dir_all` a vestigial
    /// `$TMPDIR/freenet/webs` and `panic!` on failure. Nothing had read that
    /// directory since the real webapp cache was renamed in April 2025, so its
    /// only remaining effect was to kill the node at startup when `$TMPDIR`
    /// happened to hold a non-directory (or another user's) `freenet` entry.
    /// That is exactly what blocked release v0.2.124: the auto-update canary
    /// stages the binary it gates AT `$TMPDIR/freenet`, so the mkdir hit
    /// ENOTDIR and the node exited 101 before the update check could run.
    ///
    /// Run in a **child process** (a re-exec of this test binary filtered to
    /// this one test) for two reasons: the hostile condition is a process-wide
    /// environment variable, which `set_var` makes unsound to install from a
    /// test thread in edition 2024, and the failure mode is a `panic!` in a
    /// non-async constructor, which a child's exit status observes directly.
    /// The child fails every time without the fix and passes every time with
    /// it.
    #[test]
    fn router_construction_survives_a_hostile_temp_dir() {
        const CHILD_ENV: &str = "FREENET_TEST_5291_CHILD";
        const CHILD_TEST: &str =
            "server::client_api::tests::router_construction_survives_a_hostile_temp_dir";

        if std::env::var_os(CHILD_ENV).is_some() {
            // Premise: the environment really is the hostile one the parent
            // built. If `temp_dir()` ignored our variables the child would pass
            // vacuously, so assert the collision exists before relying on it.
            let clash = std::env::temp_dir().join("freenet");
            assert!(
                clash.is_file(),
                "premise: {} must exist as a FILE for this child to reproduce \
                 the #5291 condition; got metadata {:?}",
                clash.display(),
                std::fs::metadata(&clash)
            );

            let (api, _router) = HttpClientApi::as_router(&"127.0.0.1:0".parse().unwrap());
            drop(api);
            return;
        }

        let tmp = crate::util::tests::get_temp_dir();
        // The obstruction: `$TMPDIR/freenet` exists, as a file.
        std::fs::write(tmp.path().join("freenet"), b"not a directory")
            .expect("stage the blocking file");

        // Two cache roots, because the deleted mkdir was not the only
        // `create_dir_all` this function reaches.
        //
        // `benign` keeps the child's REAL webapp cache off the developer's home
        // cache while the obstruction sits where the DELETED mkdir used to
        // point. That is the #5291 case proper.
        //
        // `obstructed` puts the cache root itself under the blocking file, so
        // the surviving `create_dir_all` in `WebappCache::with_root` fails too.
        // `with_root` is documented to warn and carry on, and
        // `with_root_tolerates_a_root_that_is_not_a_directory` pins that
        // directly — but a guard on `with_root` says nothing about the caller,
        // and it is this function's job not to turn that warning back into a
        // dead node. Without this case, making `with_root` fatal would
        // reinstate the whole #5291 class with the test still green.
        let benign = tmp.path().join("webapp_cache");
        let obstructed = tmp.path().join("freenet").join("webapp_cache");

        for (label, cache) in [("benign", &benign), ("obstructed", &obstructed)] {
            let exe = std::env::current_exe().expect("test binary path");
            let output = std::process::Command::new(exe)
                .args(["--exact", "--test-threads=1", "--nocapture", CHILD_TEST])
                .env(CHILD_ENV, "1")
                // `temp_dir()` reads TMPDIR on unix and TMP/TEMP on Windows.
                .env("TMPDIR", tmp.path())
                .env("TMP", tmp.path())
                .env("TEMP", tmp.path())
                .env("FREENET_WEBAPP_CACHE_DIR", cache)
                .output()
                .expect("re-exec the test binary");
            let stdout = String::from_utf8_lossy(&output.stdout);
            let stderr = String::from_utf8_lossy(&output.stderr);
            assert!(
                output.status.success(),
                "[{label}] composing the router must not panic when \
                 $TMPDIR/freenet is not a directory (#5291).\nstdout:\n{stdout}\n\
                 stderr:\n{stderr}",
            );
            // Fail CLOSED on a rename: libtest exits 0 when its filter matches
            // nothing, so without this the check goes vacuous the moment this
            // function moves or is renamed.
            assert!(
                stdout.contains("1 passed"),
                "[{label}] the child must actually have run {CHILD_TEST} — if \
                 this function was renamed or moved, update CHILD_TEST.\n\
                 stdout:\n{stdout}\nstderr:\n{stderr}",
            );
        }

        // The other half of the premise. The child asserts the TMPDIR knob took
        // effect; this asserts the CACHE-ROOT knob did. If
        // `FREENET_WEBAPP_CACHE_DIR` ever stopped being read, the obstructed
        // case would quietly resolve to the ProjectDirs default, create it
        // successfully, and pass while testing nothing.
        assert!(
            benign.is_dir(),
            "premise: the child must honour FREENET_WEBAPP_CACHE_DIR — {} was \
             never created, so the obstructed case did not exercise the \
             surviving create_dir_all",
            benign.display()
        );
    }

    #[test]
    fn is_html_page_detects_html_extensions() {
        assert!(is_html_page("page.html"));
        assert!(is_html_page("page.HTML"));
        assert!(is_html_page("page.htm"));
        assert!(is_html_page("dir/page.html"));
    }

    /// The shell-route HostedMode read must fail SAFE to off: an absent
    /// extension maps to `false` (no userToken minted), a present one to its
    /// real value. This is the inverse of the WS gate, which fails loud.
    #[test]
    fn hosted_mode_or_default_absent_is_off() {
        assert!(
            !hosted_mode_or_default(None),
            "absent HostedMode extension must map to hosted-off"
        );
        assert!(
            !hosted_mode_or_default(Some(Extension(crate::server::HostedMode(false)))),
            "present HostedMode(false) must stay off"
        );
        assert!(
            hosted_mode_or_default(Some(Extension(crate::server::HostedMode(true)))),
            "present HostedMode(true) must be honored"
        );
    }

    /// Regression (Codex re-review of #4513): the contract-web shell route must
    /// NOT require an `Extension<HostedMode>`. Only `serve_client_api_in_impl`
    /// installs that layer; the public `HttpClientApi::as_router` composition
    /// path does not. A required extractor here made `/v{1,2}/contract/web/...`
    /// reject with axum's missing-extension 500 even for plain requests — a
    /// regression for that supported standalone composition mode.
    ///
    /// We drive the REAL `as_router` router (the one lacking the layer). The
    /// returned `HttpClientApi` owns the proxy receiver; we drop it so the
    /// shell render's `NewConnection` send fails fast (closed channel) instead
    /// of blocking. The point is only that the request reaches the handler body
    /// — proving the extractor tolerated the absent extension — so we assert the
    /// response body is NOT axum's "Missing request extension" rejection.
    #[tokio::test]
    async fn contract_web_route_does_not_require_hosted_mode_extension() {
        use axum::body::to_bytes;
        use tower::ServiceExt;

        // A syntactically valid contract key so routing + key parsing succeed
        // and execution reaches the shell-render body.
        let key = "EqJ5YpEEV3XLqEvKWLQHFhGAac2qXzSUoE6k2zbdnXBr";

        for uri in [
            format!("/v1/contract/web/{key}/"),
            format!("/v2/contract/web/{key}/"),
        ] {
            // `as_router` is the standalone composition path with NO HostedMode
            // layer. EXPLICITLY drop the returned `HttpClientApi` (it owns the
            // proxy receiver) so the handler's `NewConnection` send hits a
            // closed channel and fails fast — otherwise the shell render would
            // block awaiting a `NewId` reply that nothing services. A bare
            // `_api` binding would keep the receiver alive to end-of-scope, so
            // we `drop` it by name.
            let (api, router) = HttpClientApi::as_router(&"127.0.0.1:0".parse().unwrap());
            drop(api);

            let req = axum::http::Request::builder()
                .uri(&uri)
                .body(axum::body::Body::empty())
                .unwrap();
            // Timeout guard: if a future change makes this route block (e.g. the
            // send no longer fails fast), surface it as a clear assertion rather
            // than a CI hang.
            let resp =
                tokio::time::timeout(std::time::Duration::from_secs(10), router.oneshot(req))
                    .await
                    .unwrap_or_else(|_| panic!("{uri} shell route hung instead of returning"))
                    .unwrap();

            // Whatever the handler does downstream (here: a fast NodeError from
            // the closed channel), it must NOT be axum's extractor rejection.
            let status = resp.status();
            let body = to_bytes(resp.into_body(), usize::MAX).await.unwrap();
            let body = String::from_utf8_lossy(&body);
            assert!(
                !body.contains("Missing request extension"),
                "{uri} must tolerate an absent HostedMode extension, not 500 on \
                 the missing extractor; got status {status}, body: {body}"
            );
        }
    }

    #[test]
    fn is_html_page_detects_directory_style_paths() {
        assert!(is_html_page("news/"));
        assert!(is_html_page("about/team/"));
    }

    #[test]
    fn is_html_page_detects_extensionless_paths() {
        // Extensionless paths are likely directory-style navigation
        assert!(is_html_page("news"));
        assert!(is_html_page("about/team"));
    }

    #[test]
    fn is_html_page_rejects_non_html_files() {
        assert!(!is_html_page("app.js"));
        assert!(!is_html_page("style.css"));
        assert!(!is_html_page("image.png"));
        assert!(!is_html_page("assets/app.wasm"));
    }

    /// Regression test for freenet/freenet-core#3842.
    ///
    /// The shell page CSP must allow same-origin HTTP fetches (`'self'` in
    /// `connect-src`) so the JavaScript that `path_handlers` injects can
    /// poll `/permission/pending` without tripping a CSP violation. Before
    /// the fix, `connect-src` was `ws: wss:` only, and every Freenet webapp
    /// logged a repeating "Content-Security-Policy: blocked the loading of
    /// a resource (connect-src) at http://.../permission/pending" error and
    /// permission prompts never surfaced.
    ///
    /// Also verifies the WebSocket schemes stay allowed so the bridge can
    /// still open ws/wss connections for the sandboxed iframe.
    #[test]
    fn shell_page_csp_allows_same_origin_and_websocket_connect() {
        let csp = SHELL_PAGE_CSP;
        let connect_src = csp
            .split(';')
            .map(str::trim)
            .find(|d| d.starts_with("connect-src"))
            .expect("connect-src directive present");
        // Present: 'self' for same-origin fetches (#3842 fix), plus ws:/wss:
        // for the WebSocket bridge.
        assert!(
            connect_src.contains("'self'"),
            "connect-src must include 'self' (regression #3842); got: {connect_src}"
        );
        assert!(
            connect_src.contains("ws:"),
            "connect-src must include ws:; got: {connect_src}"
        );
        assert!(
            connect_src.contains("wss:"),
            "connect-src must include wss:; got: {connect_src}"
        );
        // default-src 'none' doesn't affect connect-src (which is set
        // explicitly), but it guards unset directives (font-src, media-src,
        // object-src, ...) — defence in depth. Pinned so it's not silently
        // relaxed in a future refactor.
        assert!(
            csp.contains("default-src 'none'"),
            "default-src should stay 'none' for defence in depth; got: {csp}"
        );
    }

    /// The shell registers a same-origin notification service worker (the only
    /// way to show notifications on mobile). With `default-src 'none'` and no
    /// `worker-src`, the registration would fall back to `script-src`
    /// (`'unsafe-inline'`, which forbids a script URL) and be CSP-blocked. So
    /// the shell CSP must grant `worker-src 'self'`.
    #[test]
    fn shell_page_csp_allows_service_worker() {
        let csp = SHELL_PAGE_CSP;
        let worker_src = csp
            .split(';')
            .map(str::trim)
            .find(|d| d.starts_with("worker-src"))
            .expect("worker-src directive present so the SW registration isn't CSP-blocked");
        assert!(
            worker_src.contains("'self'"),
            "worker-src must include 'self' so /freenet-notify-sw.js can register; got: {worker_src}"
        );
    }

    /// `GET /freenet-notify-sw.js` must serve the notification service worker as
    /// JavaScript. The worker is the only way to show notifications on mobile
    /// (the page-level `Notification` constructor is unsupported there), and it
    /// MUST NOT carry a `fetch` handler — that would silently intercept every
    /// request on the origin.
    #[tokio::test]
    async fn notify_service_worker_route_serves_js() {
        use axum::body::to_bytes;

        let response = notify_service_worker().await.into_response();
        assert_eq!(response.status(), axum::http::StatusCode::OK);

        let content_type = response
            .headers()
            .get(axum::http::header::CONTENT_TYPE)
            .expect("service worker must set a Content-Type")
            .to_str()
            .unwrap();
        assert!(
            content_type.contains("javascript"),
            "service worker must be served as JavaScript so the browser accepts it; got: {content_type}"
        );
        // The worker makes no network requests, so its own execution context is
        // locked to `default-src 'none'` (defense-in-depth). Pin the header so a
        // refactor can't silently drop it. Read before consuming the body below.
        assert_eq!(
            response
                .headers()
                .get(axum::http::header::CONTENT_SECURITY_POLICY)
                .and_then(|v| v.to_str().ok()),
            Some("default-src 'none'"),
            "the served worker must carry a locked-down CSP"
        );

        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let src = std::str::from_utf8(&body).unwrap();
        assert!(
            src.contains("notificationclick"),
            "worker must route notification clicks"
        );
        assert!(
            src.contains("skipWaiting") && src.contains("clients.claim"),
            "worker must activate and claim clients so the first notification shows without a reload"
        );
        assert!(
            src.contains("__freenet_notify_click__"),
            "worker must post clicks back to the shell for iframe routing"
        );
        // A fetch handler would make this root-scoped worker intercept every
        // request on the origin. Its ABSENCE is a hard invariant — the worker
        // exists only to own showNotification() and click routing. Catch every
        // form: addEventListener('fetch'/"fetch") AND self.onfetch. Also forbid
        // importScripts, so it stays a self-contained, no-network worker.
        assert!(
            !src.contains("'fetch'")
                && !src.contains("\"fetch\"")
                && !src.contains("onfetch")
                && !src.contains("importScripts"),
            "worker must NOT register a fetch handler in any form, nor importScripts (it must not intercept or make requests)"
        );
        // Click routing reads the tag + originating URL from notification data
        // and routes only to the originating contract's window. Pin the read
        // side of the shell<->worker contract and the per-contract routing.
        assert!(
            src.contains("data.fnTag") && src.contains("data.fnUrl"),
            "worker must read the routing tag and originating URL from notification data"
        );
        assert!(
            src.contains("pickNotifyClient"),
            "worker must route the click only to the originating contract's window"
        );
    }

    /// The worker must be REACHABLE at the origin root `/freenet-notify-sw.js`.
    /// Root path matters: a service worker's scope defaults to its script's
    /// directory, so serving it at `/` gives it scope `/`, covering every
    /// `/v{1,2}/contract/web/<key>/` shell page with one registration. This
    /// drives the real `as_router` router so a mis-registered or shadowed route
    /// fails the test, not just production.
    #[tokio::test]
    async fn notify_service_worker_route_is_wired() {
        use axum::body::to_bytes;
        use tower::ServiceExt;

        let (api, router) = HttpClientApi::as_router(&"127.0.0.1:0".parse().unwrap());
        drop(api);

        let req = axum::http::Request::builder()
            .uri("/freenet-notify-sw.js")
            .body(axum::body::Body::empty())
            .unwrap();
        let resp = router.oneshot(req).await.unwrap();

        assert_eq!(
            resp.status(),
            axum::http::StatusCode::OK,
            "GET /freenet-notify-sw.js must route to the service worker handler"
        );
        let content_type = resp
            .headers()
            .get(axum::http::header::CONTENT_TYPE)
            .expect("service worker must set a Content-Type")
            .to_str()
            .unwrap()
            .to_string();
        assert!(
            content_type.contains("javascript"),
            "must be served as JavaScript so the browser accepts it as a worker; got: {content_type}"
        );
        let body = to_bytes(resp.into_body(), usize::MAX).await.unwrap();
        assert!(
            std::str::from_utf8(&body)
                .unwrap()
                .contains("notificationclick"),
            "the served script must be the notification worker"
        );
    }

    /// The contract HTML served into the app frame must carry the `sandbox`
    /// directive itself, not merely inherit the iframe attribute.
    ///
    /// The attribute governs only the frame the shell creates. A contract that
    /// escapes to a popup it controls (possible since the iframe regained
    /// `allow-popups-to-escape-sandbox`) can re-embed this very response in an
    /// unsandboxed context; without the header the app's own HTML then runs at
    /// the node's real origin, with `localStorage` and same-origin `fetch`.
    /// That is the #3818 escape, and it needs no SVG or other exotic type —
    /// the contract's ordinary index page is enough.
    ///
    /// Pin the directive first, and the token list second: the tokens must
    /// match the iframe's `sandbox` attribute, because the effective policy is
    /// the INTERSECTION of the two. A token missing here silently withdraws a
    /// capability from every contract app (dropping `allow-forms` breaks every
    /// form; dropping `allow-popups` breaks the new-tab fix this shipped with).
    #[test]
    fn sandbox_csp_sandboxes_the_contract_document_itself() {
        let csp = sandbox_csp_for_origin("http://127.0.0.1:7509");
        let sandbox = csp
            .split(';')
            .map(str::trim)
            .find(|d| d.starts_with("sandbox"))
            .unwrap_or_else(|| {
                panic!(
                    "contract content must be served a `sandbox` CSP directive so its \
                     opaque origin does not depend on who embeds it (#3818); got: {csp}"
                )
            });
        assert_eq!(
            sandbox, CONTRACT_CONTENT_SANDBOX_CSP,
            "the contract document's sandbox tokens must match the app iframe's \
             `sandbox` attribute exactly — the effective policy is the intersection, \
             so any token dropped here is withdrawn from every contract app"
        );
        // No `allow-same-origin`: that single token would hand the contract the
        // node's real origin directly and undo the whole isolation model.
        assert!(
            !sandbox.contains("allow-same-origin"),
            "allow-same-origin would give contract content the node's own origin"
        );

        // The top-level-document policy is STRICTER, and must be pinned against
        // something other than itself. `web_subpages_sandboxes_contract_assets`
        // compares the served header to this constant, so widening the constant
        // moves both sides together and nothing goes red — verified by mutation:
        // setting it equal to CONTRACT_CONTENT_SANDBOX_CSP left the whole suite
        // green. What it uniquely buys is that a contract-authored `evil.svg`
        // navigated to directly cannot run script, and cannot paint a scripted
        // full-page UI under the node's own address.
        assert!(
            !CONTRACT_DOCUMENT_SANDBOX_CSP.contains("allow-scripts"),
            "a contract asset loaded as a TOP-LEVEL document must not be allowed \
             to run script: nothing legitimate arrives there, and the opaque \
             origin alone would still leave a scripted page under the node's URL"
        );
        assert!(
            !CONTRACT_DOCUMENT_SANDBOX_CSP.contains("allow-same-origin"),
            "allow-same-origin would give a navigated-to contract asset the \
             node's own origin"
        );
    }

    /// The sandbox iframe has an opaque (null) origin because the sandbox
    /// attribute omits `allow-same-origin`, so CSP `'self'` wouldn't match
    /// the local API server. `sandbox_csp_for_origin` must interpolate the
    /// explicit origin into BOTH `default-src` and `connect-src`, otherwise
    /// the webapp inside the sandbox can't load its own resources or talk
    /// to the local API — the same class of bug as #3842, different page.
    #[test]
    fn sandbox_csp_includes_explicit_origin_in_connect_src() {
        let csp = sandbox_csp_for_origin("http://127.0.0.1:7509");
        let connect_src = csp
            .split(';')
            .map(str::trim)
            .find(|d| d.starts_with("connect-src"))
            .expect("connect-src directive present");
        assert!(
            connect_src.contains("http://127.0.0.1:7509"),
            "sandbox connect-src must include the explicit local API origin; got: {connect_src}"
        );
        let default_src = csp
            .split(';')
            .map(str::trim)
            .find(|d| d.starts_with("default-src"))
            .expect("default-src directive present");
        assert!(
            default_src.contains("http://127.0.0.1:7509"),
            "sandbox default-src must include the explicit local API origin; got: {default_src}"
        );
        // blob: and data: must remain allowed for client-side WASM/Blob
        // workflows (e.g. spawning Web Workers from a Blob URL).
        assert!(connect_src.contains("blob:"));
        assert!(connect_src.contains("data:"));
    }

    /// Regression test for cross-contract link handling (Delta report,
    /// freenet/river#208 follow-up) and deep-link reload (#3841). Top-level
    /// HTML sub-path document loads must be served the shell page so the
    /// response carries a fresh auth token; before the original fix these
    /// loads served raw HTML with no `authToken`.
    #[test]
    fn subpage_serves_shell_for_top_level_html_document_load() {
        assert!(should_serve_shell_for_subpage(false, "page2", "document"));
        assert!(should_serve_shell_for_subpage(
            false,
            "about/team",
            "document"
        ));
        assert!(should_serve_shell_for_subpage(false, "news/", "document"));
        assert!(should_serve_shell_for_subpage(
            false,
            "index.html",
            "document"
        ));
    }

    #[test]
    fn subpage_does_not_serve_shell_for_sub_resource_fetches() {
        // Non-HTML assets must be served so the page can load resources.
        // These still fall through to `variable_content`, so a genuinely
        // missing asset 404s as before (the fix must not mask real 404s).
        for path in ["app.js", "style.css", "app.wasm", "logo.png"] {
            assert!(
                !should_serve_shell_for_subpage(false, path, "document"),
                "{path} must not be served the shell"
            );
        }
        // Only top-level document loads are ambiguous with pasted URLs and
        // cross-contract clicks; iframe/xhr/fetch must never be intercepted.
        for dest in ["iframe", "empty", "script", "style", "image", ""] {
            assert!(
                !should_serve_shell_for_subpage(false, "page2", dest),
                "Sec-Fetch-Dest={dest} must not be served the shell"
            );
        }
    }

    #[test]
    fn subpage_does_not_serve_shell_for_sandbox_requests() {
        // Sandbox iframe requests flow through the sandbox content pipeline;
        // intercepting would break multi-page navigation inside the sandbox.
        assert!(!should_serve_shell_for_subpage(true, "page2", "iframe"));
        assert!(!should_serve_shell_for_subpage(true, "page2", "document"));
    }

    /// A valid contract key used across redirect tests. Constructed from
    /// 32 zero bytes so `ContractInstanceId::from_base58` accepts it.
    fn valid_contract_key_b58() -> String {
        use freenet_stdlib::prelude::ContractInstanceId;
        let bytes = [0u8; 32];
        ContractInstanceId::new(bytes).to_string()
    }

    #[test]
    fn redirect_to_shell_root_drops_sensitive_params_and_preserves_others() {
        let key = valid_contract_key_b58();
        // Mixed query with sensitive + harmless params.
        let query = Some("authToken=attacker&invite=abc&__sandbox=1&room=42");
        let resp = redirect_to_shell_root(&key, ApiVersion::V1, query)
            .expect("valid key should not fail validation");
        let loc = resp
            .headers()
            .get(axum::http::header::LOCATION)
            .expect("redirect must set Location")
            .to_str()
            .unwrap()
            .to_string();

        // Pin the full shape: contract sub-path, preserved query (order
        // preserved, sensitive params stripped).
        assert_eq!(
            loc,
            format!("/v1/contract/web/{key}/?invite=abc&room=42"),
            "sensitive params must be stripped, harmless ones preserved in order"
        );

        // Defensive: ensure attacker token never appears anywhere in the
        // redirect URL. If a future refactor reintroduces it, this fails.
        assert!(
            !loc.contains("authToken"),
            "authToken must never appear in redirect target"
        );
        assert!(
            !loc.contains("__sandbox"),
            "__sandbox must never appear in redirect target"
        );
    }

    #[test]
    fn redirect_to_shell_root_omits_query_when_empty() {
        let key = valid_contract_key_b58();
        let resp = redirect_to_shell_root(&key, ApiVersion::V1, None).unwrap();
        let loc = resp
            .headers()
            .get(axum::http::header::LOCATION)
            .unwrap()
            .to_str()
            .unwrap()
            .to_string();
        assert_eq!(loc, format!("/v1/contract/web/{key}/"));
        assert!(!loc.contains('?'));

        // A query that is entirely sensitive params must also produce no
        // trailing `?` — keeps the URL canonical so browsers don't show
        // a bare `?` in the address bar.
        let resp =
            redirect_to_shell_root(&key, ApiVersion::V1, Some("authToken=x&__sandbox=1")).unwrap();
        let loc = resp
            .headers()
            .get(axum::http::header::LOCATION)
            .unwrap()
            .to_str()
            .unwrap()
            .to_string();
        assert_eq!(loc, format!("/v1/contract/web/{key}/"));
    }

    #[test]
    fn redirect_to_shell_root_uses_303_see_other_for_fragment_preservation() {
        // A 303 See Other (axum's `Redirect::to` default) has RFC 7231
        // SHOULD semantics for URL-fragment preservation across the
        // redirect, and converts any method to GET. Pin so that a future
        // refactor to `Redirect::temporary` (307) or `Redirect::permanent`
        // (308) doesn't silently change fragment semantics for hash-routed
        // webapps like Delta.
        let key = valid_contract_key_b58();
        let resp = redirect_to_shell_root(&key, ApiVersion::V1, None).unwrap();
        assert_eq!(resp.status(), axum::http::StatusCode::SEE_OTHER);
    }

    /// Regression test for the L3 panic surface raised during review:
    /// without key validation, a crafted path containing percent-encoded
    /// CRLF would reach `HeaderValue::try_from` inside `Redirect::to`,
    /// which panics on invalid header values. Validating via
    /// `ContractInstanceId::from_base58` first converts this into a
    /// structured 4xx response.
    #[test]
    fn redirect_to_shell_root_rejects_invalid_key_instead_of_panicking() {
        // Obvious garbage.
        assert!(matches!(
            redirect_to_shell_root("not-a-real-contract-key", ApiVersion::V1, None),
            Err(WebSocketApiError::InvalidParam { .. })
        ));
        // CRLF-bearing key: exactly the panic surface L3 flagged. Must
        // return an error, not panic inside the handler.
        assert!(matches!(
            redirect_to_shell_root("AAAA\r\nInjected: x", ApiVersion::V1, None),
            Err(WebSocketApiError::InvalidParam { .. })
        ));
        // Empty key.
        assert!(matches!(
            redirect_to_shell_root("", ApiVersion::V1, None),
            Err(WebSocketApiError::InvalidParam { .. })
        ));
    }

    /// The router state must be rooted at the directory the node's config
    /// names, not at a process-wide default.
    ///
    /// This is the half of the isolation that lives in core; the other half is
    /// the `#[freenet_test]` harness setting `webapp_cache_dir` (pinned in
    /// `freenet-macros`). The cache is LRU-EVICTED, so a builder that fell back
    /// to the default would put every integration test back to deleting from
    /// the developer's real `~/.cache/freenet/webapp_cache` — the bug a
    /// `#[cfg(test)]`-gated redirect missed, because `cfg(test)` is false when
    /// an integration test links the lib as an ordinary dependency.
    ///
    /// Scope, stated plainly: this pins `Config::new`, the one constructor the
    /// router uses, against ignoring its argument. It does not re-prove the
    /// call chain above it — that is the compiler's job, since the root is a
    /// required parameter with no default anywhere between here and
    /// `WebsocketApiConfig`.
    #[test]
    fn router_config_is_rooted_at_the_configured_webapp_cache_dir() {
        let root = tempfile::tempdir().expect("tempdir");
        let configured = root.path().join("webapp_cache");
        assert_ne!(
            configured,
            crate::config::default_webapp_cache_dir(),
            "premise: the configured dir must differ from the default, or this \
             test would pass even if the argument were ignored"
        );

        let config = Config::new(true, configured.clone());

        assert_eq!(
            config.webapp_cache_root(),
            configured.as_path(),
            "the router's cache must be rooted where the node's config says"
        );
    }

    /// A minimal localhost `Config` for handler tests. The webapp cache is
    /// rooted in a per-process temp dir: it is LRU-size-bounded, so a handler
    /// test that reached the real directory would DELETE from the developer's
    /// cache.
    fn localhost_config() -> Config {
        static TEST_CACHE_ROOT: std::sync::LazyLock<tempfile::TempDir> =
            std::sync::LazyLock::new(|| tempfile::tempdir().expect("test webapp cache root"));
        Config {
            localhost: true,
            webapp_cache: path_handlers::WebappCache::with_root(
                TEST_CACHE_ROOT.path().to_path_buf(),
            ),
        }
    }

    /// End-to-end regression for #3841: a top-level document load of an
    /// HTML sub-path must be served the SHELL page (rendered in place with
    /// the sub-path threaded into the iframe), NOT a raw redirect to the
    /// contract root and NOT raw contract HTML. Routing into the shell is
    /// observable on the client-connection channel: `render_shell_response`
    /// → `contract_home` → `ensure_contract_cached` emits a `NewConnection`
    /// before any response is produced, whereas the old behaviour returned a
    /// synchronous 303 without touching the channel.
    ///
    /// Pins the wiring the predicate-only tests do not reach: the
    /// `Sec-Fetch-Dest` header lookup, the `RawQuery` interaction, and the
    /// branch selection in `web_subpages`. The full shell HTML (iframe
    /// `data-src` carrying the sub-path) is asserted at the `contract_home`
    /// layer in path_handlers.rs, which can prime the webapp cache.
    #[tokio::test]
    async fn web_subpages_serves_shell_for_top_level_document_load() {
        let key = valid_contract_key_b58();

        // Case 1: top-level HTML document load of an HTML sub-path
        // (non-sandbox — a pasted/bookmarked/reloaded deep link). Must
        // route into the shell-render path. We observe the `NewConnection`
        // the render emits on the channel, then abort before delivering a
        // GetResponse to keep the test bounded.
        let mut headers = axum::http::HeaderMap::new();
        headers.insert("sec-fetch-dest", "document".parse().unwrap());
        let (tx, mut rx) = mpsc::channel(4);
        let sender = HttpClientApiRequest::from_sender(tx);
        let handler = {
            let key = key.clone();
            tokio::spawn(async move {
                web_subpages(
                    key,
                    "page2".to_string(),
                    ApiVersion::V1,
                    Some("authToken=attacker&invite=abc".to_string()),
                    headers,
                    &localhost_config(),
                    sender,
                    false,
                    None,
                )
                .await
                .map(|_| ())
            })
        };
        let msg = tokio::time::timeout(std::time::Duration::from_secs(5), rx.recv())
            .await
            .expect("shell render must emit on the channel (not a synchronous redirect)")
            .expect("channel must stay open for the send");
        assert!(
            matches!(msg, ClientConnection::NewConnection { .. }),
            "top-level document load must route into the shell render path \
             (NewConnection), not a 303 redirect; got: {msg:?}"
        );
        handler.abort();

        // Case 1b: a sandbox sub-page request with `Sec-Fetch-Dest:
        // document` must still redirect to the shell root (via the
        // sandbox branch's existing top-level-load guard) rather than
        // serving raw sandbox content in the top frame. This guards the
        // sandbox short-circuit that runs *before* the deep-link block,
        // which is the path a pasted `?__sandbox=1` URL hits. Unchanged
        // by #3841 — it is a security guard, not a deep-link.
        let mut headers = axum::http::HeaderMap::new();
        headers.insert("sec-fetch-dest", "document".parse().unwrap());
        let resp = web_subpages(
            key.clone(),
            "page2".to_string(),
            ApiVersion::V1,
            Some("__sandbox=1".to_string()),
            headers,
            &localhost_config(),
            dead_request_sender(),
            false,
            None,
        )
        .await
        .expect("sandbox document load must redirect, not error");
        assert_eq!(resp.status(), axum::http::StatusCode::SEE_OTHER);

        // Case 2: missing Sec-Fetch-Dest (curl, older browsers) falls
        // through to variable_content — pre-PR behaviour preserved, no
        // change in response shape for non-browser clients. With a dead
        // request sender the cache-miss fetch fails fast with a
        // closed-channel error (see `dead_request_sender`). We assert
        // only that the response is NOT a shell-root redirect.
        let res = web_subpages(
            key.clone(),
            "page2".to_string(),
            ApiVersion::V1,
            None,
            axum::http::HeaderMap::new(),
            &localhost_config(),
            dead_request_sender(),
            false,
            None,
        )
        .await;
        match res {
            Ok(resp) => assert_ne!(resp.status(), axum::http::StatusCode::SEE_OTHER),
            Err(_) => {
                // variable_content returned an error for the cache-miss
                // fetch attempt — that's fine, the point is that we did
                // not take a redirect branch.
            }
        }
    }

    /// Regression for the SUB0PT1MAL/cirro CORS report (2026-07-29): an ERROR
    /// subresource response from `web_subpages` MUST still carry
    /// `Access-Control-Allow-Origin: *`.
    ///
    /// The sandboxed iframe has a null origin, so a subresource fetch whose
    /// response lacks the CORS header is reported by the browser as an opaque
    /// "CORS error" that masks the real status. Previously only the SUCCESS
    /// branch of `web_subpages` added the header; a `variable_content` error
    /// (e.g. a rejected path, which returns `Err(InvalidParam)` → 400) returned
    /// a bare response, so the app saw an opaque CORS failure instead of the
    /// true 400. We drive the error branch with a traversal path (a clean
    /// `Err`) and assert the response is both 400 AND CORS-allowed.
    #[tokio::test]
    async fn web_subpages_error_response_carries_cors_header() {
        // A UNIQUE non-zero key so this test can't collide on the process-global
        // webapp cache with another test that might warm the all-zeros key (which
        // would flip the guard's 400 into a cache-fetch 500). See the three
        // path_handlers traversal tests, which use the same unique-seed idiom.
        let key = {
            use freenet_stdlib::prelude::ContractInstanceId;
            let mut bytes = [0u8; 32];
            bytes[0] = 0x3a;
            bytes[1] = 0x54;
            ContractInstanceId::new(bytes).to_string()
        };

        // A `..` traversal makes `variable_content` return Err(InvalidParam).
        // Non-HTML + no `Sec-Fetch-Dest` so it falls through to
        // `variable_content` rather than the shell/sandbox branches.
        let resp = web_subpages(
            key,
            "../../../etc/hostname".to_string(),
            ApiVersion::V1,
            None,
            axum::http::HeaderMap::new(),
            &localhost_config(),
            dead_request_sender(),
            false,
            None,
        )
        .await
        .expect("web_subpages must convert the inner error into a response, not propagate it");

        assert_eq!(
            resp.status(),
            axum::http::StatusCode::BAD_REQUEST,
            "a rejected traversal path must surface as 400"
        );
        assert_eq!(
            resp.headers()
                .get(axum::http::header::ACCESS_CONTROL_ALLOW_ORIGIN)
                .map(|v| v.to_str().unwrap_or("")),
            Some("*"),
            "even an error subresource response must carry the sandbox CORS header, \
             otherwise the null-origin iframe surfaces it as an opaque CORS error"
        );
        // The Err arm of the route gets the sandbox directive too. Its sibling
        // test drives only Ok responses, so without this a header attached to
        // just one arm would go unnoticed — mutation-confirmed.
        assert_eq!(
            resp.headers()
                .get(axum::http::header::CONTENT_SECURITY_POLICY)
                .map(|v| v.to_str().unwrap_or("")),
            Some(CONTRACT_CONTENT_SANDBOX_CSP),
            "the error arm must be sandboxed as well: the body reflects the \
             request path and the response is reachable from a context we do \
             not control (#3818)"
        );
    }

    /// Guard for the companion fix that lets `allow-popups-to-escape-sandbox`
    /// back onto the app iframe (#3818).
    ///
    /// The flag is what makes `target="_blank"` open a real tab in Firefox as
    /// well as Chrome/Safari, and it costs the iframe `sandbox` attribute its
    /// standing as the thing that keeps contract bytes off the node's origin: a
    /// contract can escape to a popup it fully controls and re-embed its own
    /// assets there, unsandboxed. So the sandbox is served as a HEADER on every
    /// response carrying contract bytes — see `CONTRACT_CONTENT_SANDBOX_CSP`
    /// for the full three-step attack and the cross-engine reproduction.
    ///
    /// The cases below are the ones a narrower guard gets wrong. Keying on
    /// `Sec-Fetch-Dest: document`, which is where this started, covers only the
    /// pasted-URL shape and misses the nested-navigable shape the escape
    /// actually uses. `document` keeps the stricter no-`allow-scripts` policy
    /// because nothing legitimate arrives there; everything else gets the app
    /// iframe's own token list so in-frame behaviour is untouched.
    #[tokio::test]
    async fn web_subpages_sandboxes_contract_assets() {
        // Unique non-zero key so the cold-cache error path can't collide with
        // another test on the process-global webapp cache.
        let key = {
            use freenet_stdlib::prelude::ContractInstanceId;
            let mut bytes = [0u8; 32];
            bytes[0] = 0x3a;
            bytes[1] = 0x56;
            ContractInstanceId::new(bytes).to_string()
        };

        // Non-HTML, so `should_serve_shell_for_subpage` is false and every case
        // below reaches `variable_content`. An empty `dest` means "no
        // `Sec-Fetch-Dest` header at all", which is its own case.
        let subpage_dest = |dest: &'static str| {
            let key = key.clone();
            async move {
                let mut headers = axum::http::HeaderMap::new();
                if !dest.is_empty() {
                    headers.insert("sec-fetch-dest", dest.parse().unwrap());
                }
                web_subpages(
                    key,
                    "evil.svg".to_string(),
                    ApiVersion::V1,
                    None,
                    headers,
                    &localhost_config(),
                    dead_request_sender(),
                    false,
                    None,
                )
                .await
                .expect("web_subpages must respond, not propagate")
            }
        };

        let csp = |resp: &axum::response::Response| {
            resp.headers()
                .get(axum::http::header::CONTENT_SECURITY_POLICY)
                .map(|v| v.to_str().unwrap_or("").to_string())
        };

        // Top-level document load of a scriptable asset: opaque origin, no script.
        assert_eq!(
            csp(&subpage_dest("document").await).as_deref(),
            Some(CONTRACT_DOCUMENT_SANDBOX_CSP),
            "a contract asset loaded as a top-level document must be sandboxed, \
             or a contract-authored SVG runs script at the node's own origin"
        );

        // A NESTED navigable is the one the escaped-popup attack uses: the
        // contract writes `<iframe src=…>` into an unsandboxed popup it owns,
        // and the request carries `Sec-Fetch-Dest: iframe`, not `document`.
        // Gating on `document` alone left contract bytes executing at the
        // node's real origin — reproduced in all three engines. It must be
        // sandboxed, but WITH `allow-scripts`, because this is also how an app
        // frames its own HTML sub-page from inside the shell.
        for dest in ["iframe", "frame", "embed", "object"] {
            assert_eq!(
                csp(&subpage_dest(dest).await).as_deref(),
                Some(CONTRACT_CONTENT_SANDBOX_CSP),
                "a contract asset loaded as a `{dest}` navigable must still be \
                 sandboxed: an escaped popup embeds it exactly this way, and \
                 without the header it runs at the node's own origin (#3818)"
            );
        }

        // A client that sends no `Sec-Fetch-Dest` at all (curl, and browsers
        // predating Fetch Metadata) must not be the way around it either. CSP
        // is inert for non-browsers, so this costs them nothing.
        assert_eq!(
            csp(&subpage_dest("").await).as_deref(),
            Some(CONTRACT_CONTENT_SANDBOX_CSP),
            "a request without `Sec-Fetch-Dest` must fail CLOSED: the header is \
             the only signal, and an older browser that omits it would otherwise \
             be served unsandboxed contract bytes"
        );

        // Subresource fetches are sandboxed too, which is INERT for them — the
        // `sandbox` directive applies to documents and workers, not to an image
        // or a stylesheet. Asserting it here keeps the rule "every response on
        // this route carries the header" simple enough to hold, rather than an
        // allow-list of destinations that a new dest name would silently escape.
        assert_eq!(
            csp(&subpage_dest("image").await).as_deref(),
            Some(CONTRACT_CONTENT_SANDBOX_CSP),
            "the header is unconditional on this route"
        );
    }

    /// A percent-encoded parameter NAME must not slip the sensitive-param
    /// filter. `authT%6Fken=evil` reads back as `authToken` from
    /// `new URLSearchParams(location.search)` inside the iframe, which is
    /// precisely the "webapp reads its credential from `location.search`" case
    /// the filter exists for; a raw `starts_with` never sees it.
    #[test]
    fn sensitive_query_params_are_matched_after_decoding_the_name() {
        for evil in [
            "authT%6Fken=evil",
            "%61uthToken=evil",
            "%5F%5Fsandbox=1",
            "__sandbo%78=1",
            "%61uthTokenExtra=evil",
        ] {
            assert!(
                is_sensitive_query_param(evil),
                "{evil} must be stripped: the browser decodes the name before a \
                 webapp reads it back"
            );
        }
        // Ordinary app params are untouched, including ones that merely
        // CONTAIN an escape in their value.
        for ok in [
            "invitation=abc",
            "room=%2Fpath",
            "q=authToken",
            "myauthToken=x",
        ] {
            assert!(!is_sensitive_query_param(ok), "{ok} must be preserved");
        }
        // A malformed escape is not a decode, and must not become one.
        assert!(!is_sensitive_query_param("auth%zzToken=x"));
        // Multi-byte characters around a `%` must not panic: a query string is
        // attacker-controlled on every request, and byte-slicing the two hex
        // digits would land mid-character.
        for odd in [
            // 3-byte char after `%`: byte index i+3 lands INSIDE it, so a
            // `&str` slice of the two hex digits panics. Reachable with one
            // unauthenticated GET carrying raw non-ASCII in the query — hyper
            // accepts it, browsers just never send it.
            "%\u{20ac}=1",
            "a%\u{20ac}b=1",
            // 4-byte char.
            "%\u{1f600}=1",
            "%é=1",
            "auth%é=1",
            "%",
            "%2",
            "a%",
            "%e2%82%ac=1",
            "authToken%=1",
            "π=1",
        ] {
            let _ = is_sensitive_query_param(odd);
        }
        // …and one that must still be caught despite the neighbouring escape.
        assert!(is_sensitive_query_param("authT%6Fken%é=1"));
    }

    /// Regression test for the sub-page loss that removing the `window.open`
    /// override exposed (#5100 review).
    ///
    /// An app that opens its own current page in a new tab — `window.open(
    /// location.href)`, or a hash-only open that inherits `__sandbox=1` from
    /// the base — now reaches the server as a TOP-LEVEL document load of a
    /// `?__sandbox=1` URL. That must not be served raw (it is contract HTML
    /// outside its iframe), so it redirects; the bug is redirecting to the
    /// contract ROOT, which silently drops both the page and the app's own
    /// query params. The deleted override used to hide this by stripping
    /// `__sandbox` client-side and forwarding the clean URL.
    ///
    /// `__sandbox` and `authToken` must still be stripped from the target:
    /// this URL is attacker-reachable (a pasted deep link), and the shell must
    /// mint its own token rather than adopt one from the URL.
    #[tokio::test]
    async fn top_level_sandbox_url_redirects_to_the_same_page_not_the_root() {
        let key = {
            use freenet_stdlib::prelude::ContractInstanceId;
            let mut bytes = [0u8; 32];
            bytes[0] = 0x3a;
            bytes[1] = 0x57;
            ContractInstanceId::new(bytes).to_string()
        };
        let mut headers = axum::http::HeaderMap::new();
        headers.insert("sec-fetch-dest", "document".parse().unwrap());
        let config = localhost_config();

        let resp = serve_sandbox_response(
            key.clone(),
            ApiVersion::V1,
            Some("rooms/index.html"),
            Some("__sandbox=1&invitation=abc&authToken=stolen"),
            &headers,
            dead_request_sender(),
            &config.webapp_cache,
        )
        .await
        .expect("a top-level sandbox URL must redirect, not error");

        let location = resp
            .headers()
            .get(axum::http::header::LOCATION)
            .and_then(|v| v.to_str().ok())
            .expect("redirect carries a Location")
            .to_string();

        assert_eq!(
            location,
            format!("/v1/contract/web/{key}/rooms/index.html?invitation=abc"),
            "the redirect must land on the requested page with the app's own \
             query preserved, and must strip `__sandbox` and `authToken`"
        );
    }

    /// A sub-path the shell sanitizer rejects must still redirect — the point
    /// of this branch is that raw sandbox content never becomes a top-level
    /// document, and that holds whatever the redirect target is. Falling back
    /// to the contract root is the safe answer; erroring would turn a hostile
    /// URL into a 400 that leaks nothing but also serves the user nothing.
    #[tokio::test]
    async fn top_level_sandbox_url_with_an_unusable_sub_path_falls_back_to_root() {
        let key = {
            use freenet_stdlib::prelude::ContractInstanceId;
            let mut bytes = [0u8; 32];
            bytes[0] = 0x3a;
            bytes[1] = 0x58;
            ContractInstanceId::new(bytes).to_string()
        };
        let mut headers = axum::http::HeaderMap::new();
        headers.insert("sec-fetch-dest", "document".parse().unwrap());
        let config = localhost_config();

        for bad in ["../escape/index.html", "a\rb/index.html"] {
            let resp = serve_sandbox_response(
                key.clone(),
                ApiVersion::V1,
                Some(bad),
                Some("__sandbox=1"),
                &headers,
                dead_request_sender(),
                &config.webapp_cache,
            )
            .await
            .unwrap_or_else(|e| panic!("`{bad}` must redirect, not error: {e:?}"));

            let location = resp
                .headers()
                .get(axum::http::header::LOCATION)
                .and_then(|v| v.to_str().ok())
                .unwrap_or_default()
                .to_string();
            assert_eq!(
                location,
                format!("/v1/contract/web/{key}/"),
                "`{bad}` must fall back to the contract root"
            );
        }
    }

    /// Companion regression for the OTHER symmetric CORS-on-error branch: an
    /// HTML sandbox subresource that errors (`serve_sandbox_response` →
    /// `serve_sandbox_content`) MUST also carry the sandbox CORS header, or the
    /// null-origin iframe surfaces it as an opaque CORS failure. Drives the
    /// uncached-contract error (`serve_sandbox_content` returns
    /// `NodeError("Contract not cached yet")`) via a cold cache + dead sender;
    /// no `Sec-Fetch-Dest: document`, so it does NOT take the redirect branch.
    #[tokio::test]
    async fn serve_sandbox_response_error_carries_cors_and_sandbox_headers() {
        // Unique non-zero key so this cold-cache assertion can't collide with
        // another test on the process-global webapp cache.
        let key = {
            use freenet_stdlib::prelude::ContractInstanceId;
            let mut bytes = [0u8; 32];
            bytes[0] = 0x3a;
            bytes[1] = 0x55;
            ContractInstanceId::new(bytes).to_string()
        };
        let config = localhost_config();
        let headers = axum::http::HeaderMap::new();

        let resp = serve_sandbox_response(
            key,
            ApiVersion::V1,
            Some("page.html"),
            None,
            &headers,
            dead_request_sender(),
            &config.webapp_cache,
        )
        .await
        .expect(
            "serve_sandbox_response must convert the inner error into a response, not propagate it",
        );

        assert!(
            !resp.status().is_success(),
            "an uncached sandbox HTML subresource must be an error status, got {}",
            resp.status()
        );
        assert_eq!(
            resp.headers()
                .get(axum::http::header::ACCESS_CONTROL_ALLOW_ORIGIN)
                .map(|v| v.to_str().unwrap_or("")),
            Some("*"),
            "the sandbox-HTML error branch must carry the CORS header too, \
             otherwise the null-origin iframe surfaces it as an opaque CORS error"
        );
        // …and the sandbox directive, for the same reason the success branch
        // carries it: the body reflects the request path, and this response is
        // reachable from a context we do not control once popups can escape the
        // sandbox. Cheaper to sandbox every response on the route than to keep
        // re-deriving whether the current error renderer can emit markup.
        assert_eq!(
            resp.headers()
                .get(axum::http::header::CONTENT_SECURITY_POLICY)
                .map(|v| v.to_str().unwrap_or("")),
            Some(CONTRACT_CONTENT_SANDBOX_CSP),
            "the sandbox-HTML error branch must also be sandboxed (#3818)"
        );
    }

    /// Regression test pinning the ordering inside `web_subpages`: a
    /// sandbox iframe request for an HTML sub-path MUST hit the sandbox
    /// branch, not the top-level-document shell-render branch, even if a
    /// future reorder moves the shell block earlier.
    ///
    /// We cannot exercise the full sandbox pipeline in a unit test
    /// (requires a real contract state in the cache), but we can
    /// assert the predicate-level invariant — `should_serve_shell_for_subpage`
    /// returns false for any sandbox request — and check the source
    /// ordering via byte-offset comparison so that a reorder is caught
    /// mechanically.
    #[test]
    fn web_subpages_sandbox_branch_runs_before_shell_branch() {
        let src = include_str!("client_api.rs");
        // The sandbox short-circuit must appear before the
        // top-level-document shell render inside `web_subpages`. Both
        // markers are unique to that function.
        let sandbox_idx = src
            .find("if is_sandbox && is_html_page(&last_path) {")
            .expect("sandbox short-circuit marker present in web_subpages");
        let shell_idx = src
            .find("should_serve_shell_for_subpage(is_sandbox")
            .expect("subpage shell-render marker present in web_subpages");
        assert!(
            sandbox_idx < shell_idx,
            "sandbox branch must run before the top-level-document shell render: \
             reordering would break sandbox iframe sub-page loads"
        );

        // Predicate-level invariant: sandbox requests are never
        // served the shell-render branch regardless of other inputs.
        assert!(!should_serve_shell_for_subpage(true, "page2", "document"));
        assert!(!should_serve_shell_for_subpage(true, "news/", "document"));
        assert!(!should_serve_shell_for_subpage(
            true,
            "index.html",
            "iframe"
        ));
    }

    #[test]
    fn sandbox_csp_adapts_to_remote_host_origin() {
        // When the gateway is accessed over the LAN, the Host header gives
        // the LAN address; the CSP must still match rather than being
        // pinned to localhost.
        let csp = sandbox_csp_for_origin("http://192.168.1.42:7509");
        assert!(csp.contains("http://192.168.1.42:7509"));
        assert!(!csp.contains("127.0.0.1"));
    }

    fn hdrs(pairs: &[(&str, &str)]) -> axum::http::HeaderMap {
        let mut h = axum::http::HeaderMap::new();
        for (k, v) in pairs {
            h.insert(
                axum::http::HeaderName::from_bytes(k.as_bytes()).unwrap(),
                axum::http::HeaderValue::from_str(v).unwrap(),
            );
        }
        h
    }

    /// Regression for the live-deploy bug: behind a TLS-terminating reverse
    /// proxy the browser's origin is `https://<public-host>`, but the sandbox
    /// CSP origin was hardcoded to `http://<Host>` — so the CSP blocked the
    /// webapp's own `https://` assets and the app never loaded. The origin must
    /// honor `X-Forwarded-Proto: https`.
    #[test]
    fn sandbox_origin_honors_forwarded_https_scheme() {
        let origin = sandbox_origin_from_headers(&hdrs(&[
            ("host", "127.0.0.1:7509"),
            ("x-forwarded-proto", "https"),
            ("x-forwarded-host", "localhost:8443"),
        ]));
        // https scheme + the public host the browser actually used.
        assert_eq!(origin, "https://localhost:8443");
        // And the resulting CSP allows that exact (https) origin.
        let csp = sandbox_csp_for_origin(&origin);
        assert!(csp.contains("https://localhost:8443"));
        assert!(!csp.contains("http://localhost:8443"));
    }

    /// A proxy that preserves Host (e.g. Caddy) needs no `X-Forwarded-Host`:
    /// the scheme still upgrades from `X-Forwarded-Proto`.
    #[test]
    fn sandbox_origin_forwarded_proto_without_forwarded_host_uses_host() {
        let origin = sandbox_origin_from_headers(&hdrs(&[
            ("host", "try.example.org"),
            ("x-forwarded-proto", "https"),
        ]));
        assert_eq!(origin, "https://try.example.org");
    }

    /// A direct (no-proxy) connection is unchanged: `http://<Host>`.
    #[test]
    fn sandbox_origin_direct_connection_is_http_host() {
        assert_eq!(
            sandbox_origin_from_headers(&hdrs(&[("host", "127.0.0.1:7509")])),
            "http://127.0.0.1:7509"
        );
        // Explicit `X-Forwarded-Proto: http` must NOT upgrade to https.
        assert_eq!(
            sandbox_origin_from_headers(&hdrs(&[
                ("host", "127.0.0.1:7509"),
                ("x-forwarded-proto", "http"),
            ])),
            "http://127.0.0.1:7509"
        );
    }

    /// No host at all → `'self'` fallback (unchanged).
    #[test]
    fn sandbox_origin_no_host_falls_back_to_self() {
        assert_eq!(sandbox_origin_from_headers(&hdrs(&[])), "'self'");
    }

    /// Multi-proxy: comma-separated `X-Forwarded-Host`/`-Proto` must use the
    /// first (client-facing) entry, not the whole list (which is an invalid
    /// CSP origin that would re-break the app).
    #[test]
    fn sandbox_origin_uses_first_of_comma_separated_forwarded_values() {
        let origin = sandbox_origin_from_headers(&hdrs(&[
            ("host", "127.0.0.1:7509"),
            ("x-forwarded-proto", "https, http"),
            ("x-forwarded-host", "public.example, proxy.internal"),
        ]));
        assert_eq!(origin, "https://public.example");
    }
}
