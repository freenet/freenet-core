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
const SHELL_PAGE_CSP: &str = "default-src 'none'; script-src 'unsafe-inline'; frame-src 'self'; style-src 'unsafe-inline'; img-src data:; connect-src 'self' ws: wss:";

/// Content-Security-Policy served with the sandboxed iframe that actually
/// runs a webapp. The iframe has an opaque (null) origin because the
/// sandbox attribute omits `allow-same-origin`, so CSP `'self'` would not
/// match the local API server's origin. We therefore interpolate the
/// concrete origin derived from the request Host header.
fn sandbox_csp_for_origin(origin: &str) -> String {
    format!(
        "default-src {origin} 'unsafe-inline' 'unsafe-eval' blob: data:; connect-src {origin} blob: data:"
    )
}

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
}

impl HttpClientApi {
    /// Returns the uninitialized axum router to compose with other routing handling or websockets.
    pub fn as_router(socket: &SocketAddr) -> (Self, Router) {
        let origin_contracts = Arc::new(DashMap::new());
        Self::as_router_with_origin_contracts(
            socket,
            origin_contracts,
            crate::contract::user_input::pending_prompts(),
        )
    }

    /// Returns the uninitialized axum router with a provided origin_contracts map.
    ///
    /// Merges V1 and V2 HTTP routes; both currently share the same handler logic.
    pub(crate) fn as_router_with_origin_contracts(
        socket: &SocketAddr,
        origin_contracts: OriginContractMap,
        pending_prompts: crate::contract::user_input::PendingPrompts,
    ) -> (Self, Router) {
        // Controls the cookie Secure flag: when true, cookies are sent over HTTP
        // (no HTTPS required). Includes is_unspecified() so that 0.0.0.0 bindings
        // (network mode) allow HTTP cookies — most home users lack TLS.
        let localhost = socket.ip().is_loopback() || socket.ip().is_unspecified();
        let contract_web_path = std::env::temp_dir().join("freenet").join("webs");
        std::fs::create_dir_all(&contract_web_path).unwrap_or_else(|e| {
            panic!(
                "Failed to create contract web directory at {}: {}. \
                 This may happen if {} was created by another user. \
                 Try: sudo rm -rf {}",
                contract_web_path.display(),
                e,
                std::env::temp_dir().join("freenet").display(),
                std::env::temp_dir().join("freenet").display(),
            )
        });

        let (proxy_request_sender, request_to_server) = mpsc::channel(1);

        let config = Config { localhost };

        let router = Router::new()
            .route("/", axum::routing::get(home_page::homepage))
            .route(
                "/peer/{address}",
                axum::routing::get(home_page::peer_detail),
            )
            .merge(v1::routes(config.clone()))
            .merge(v2::routes(config))
            .merge(permission_prompts::routes())
            .layer(Extension(origin_contracts.clone()))
            .layer(Extension(pending_prompts))
            .layer(Extension(HttpClientApiRequest(proxy_request_sender)));

        (
            Self {
                proxy_server_request: request_to_server,
                origin_contracts,
                response_channels: HashMap::new(),
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
}

#[instrument(level = "debug")]
async fn home() -> axum::response::Response {
    axum::response::Response::default()
}

async fn web_home(
    Path(key): Path<String>,
    Extension(rs): Extension<HttpClientApiRequest>,
    axum::extract::State(config): axum::extract::State<Config>,
    req_headers: axum::http::HeaderMap,
    api_version: ApiVersion,
    query_string: Option<String>,
) -> Result<axum::response::Response, WebSocketApiError> {
    use headers::{Header, HeaderMapExt};

    // Check if this is the sandboxed iframe requesting its content
    let is_sandbox = query_string
        .as_ref()
        .map(|qs| qs.split('&').any(|p| p == "__sandbox=1"))
        .unwrap_or(false);

    if is_sandbox {
        return serve_sandbox_response(key, api_version, None, &req_headers).await;
    }

    // Shell page: generate auth token, serve iframe wrapper with CSP
    let token = AuthToken::generate();

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
    let contract_response =
        path_handlers::contract_home(key, rs, token.clone(), api_version, query_string).await?;

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

async fn web_subpages(
    key: String,
    last_path: String,
    api_version: ApiVersion,
    query_string: Option<String>,
    req_headers: axum::http::HeaderMap,
) -> Result<axum::response::Response, WebSocketApiError> {
    let is_sandbox = query_string
        .as_ref()
        .map(|qs| qs.split('&').any(|p| p == "__sandbox=1"))
        .unwrap_or(false);

    // For sandbox sub-page requests to HTML files, serve through the sandbox
    // content pipeline (with WebSocket shim + navigation interceptor injected).
    if is_sandbox && is_html_page(&last_path) {
        return serve_sandbox_response(key, api_version, Some(&last_path), &req_headers).await;
    }

    // Top-level document load for a contract sub-path HTML page (paste,
    // bookmark, or cross-contract link click via window.location.assign).
    // These requests would otherwise be served raw by `variable_content`
    // without generating an auth token or wrapping in the shell iframe,
    // leaving the page with no WebSocket credentials and breaking every
    // webapp that reads `authToken` from `location.search` (freenet/river#208
    // follow-up: Delta cross-contract links produced "Connection failed"
    // on the destination site). Redirect to the shell root instead so the
    // shell route (`web_home`) issues a fresh auth token. Any URL fragment
    // is preserved client-side by the browser across the redirect, which
    // is sufficient for hash-routed webapps (e.g. Delta). Path-based
    // multi-page apps lose the sub-path component here — a fuller fix
    // would thread the sub-path into shell generation so the initial
    // iframe load targets the requested page, but the root redirect
    // restores the baseline guarantee that every top-level contract
    // load gets a working shell.
    let fetch_dest = req_headers
        .get("sec-fetch-dest")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("");
    if should_redirect_subpage_to_shell(is_sandbox, &last_path, fetch_dest) {
        let shell_url = format!("/{}/contract/web/{key}/", api_version.prefix());
        return Ok(axum::response::Redirect::to(&shell_url).into_response());
    }

    let version_prefix = api_version.prefix();
    let full_path: String = format!("/{version_prefix}/contract/web/{key}/{last_path}");
    path_handlers::variable_content(key, full_path, api_version)
        .await
        .map_err(|e| *e)
        .map(|r| {
            let mut response = r.into_response();
            add_sandbox_cors_headers(&mut response);
            response
        })
}

/// Decides whether a sub-path HTTP request should be redirected to the
/// contract's shell root.
///
/// A top-level document load (`Sec-Fetch-Dest: document`) for a contract
/// HTML sub-path would otherwise be served raw by `variable_content` with
/// no auth token and no shell iframe, breaking any webapp that reads its
/// credentials from `location.search`. Redirecting to the shell root
/// route (`web_home`) forces a fresh auth token + shell wrapper. The
/// browser preserves any URL fragment client-side across the redirect,
/// which is sufficient for hash-routed webapps (e.g. Delta).
///
/// Sandbox iframe requests (`__sandbox=1`) are not redirected — those go
/// through the sandbox pipeline.
fn should_redirect_subpage_to_shell(is_sandbox: bool, last_path: &str, fetch_dest: &str) -> bool {
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
async fn serve_sandbox_response(
    key: String,
    api_version: ApiVersion,
    sub_path: Option<&str>,
    req_headers: &axum::http::HeaderMap,
) -> Result<axum::response::Response, WebSocketApiError> {
    // Block top-level navigation to sandbox URLs. Sec-Fetch-Dest: iframe is set
    // by the browser automatically and cannot be spoofed by scripts.
    let fetch_dest = req_headers
        .get("sec-fetch-dest")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("");
    if fetch_dest == "document" {
        let shell_url = format!("/{}/contract/web/{key}/", api_version.prefix());
        return Ok(axum::response::Redirect::to(&shell_url).into_response());
    }

    let contract_response =
        path_handlers::serve_sandbox_content(key, api_version, sub_path).await?;
    let mut response = contract_response.into_response();
    add_sandbox_cors_headers(&mut response);
    // See `sandbox_csp_for_origin` for why we interpolate a concrete origin
    // rather than using `'self'`.
    let local_api_origin = req_headers
        .get(axum::http::header::HOST)
        .and_then(|h| h.to_str().ok())
        .map(|host| format!("http://{host}"))
        .unwrap_or_else(|| "'self'".to_string());
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
                        ..
                    } => {
                        return Ok(OpenRequest::new(client_id, req)
                            .with_token(auth_token)
                            .with_origin_contract(origin_contract));
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
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn is_html_page_detects_html_extensions() {
        assert!(is_html_page("page.html"));
        assert!(is_html_page("page.HTML"));
        assert!(is_html_page("page.htm"));
        assert!(is_html_page("dir/page.html"));
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
    /// freenet/river#208 follow-up).
    ///
    /// A top-level document load of a contract HTML sub-path (e.g. a
    /// cross-contract link click via `window.location.assign`, a pasted
    /// bookmark, or a shared deep link) must be redirected to the shell
    /// root so `web_home` can issue a fresh auth token and wrap the
    /// content in the sandboxed iframe. Before this fix, such loads hit
    /// `variable_content` directly and served raw HTML with no shell,
    /// leaving webapps (Delta in particular) with no `authToken` query
    /// parameter and "Connection failed" WebSocket state.
    #[test]
    fn subpage_redirects_top_level_html_document_load_to_shell() {
        // Top-level document load of an HTML sub-path → redirect.
        assert!(should_redirect_subpage_to_shell(false, "page2", "document"));
        assert!(should_redirect_subpage_to_shell(
            false,
            "about/team",
            "document"
        ));
        assert!(should_redirect_subpage_to_shell(false, "news/", "document"));
        assert!(should_redirect_subpage_to_shell(
            false,
            "index.html",
            "document"
        ));
    }

    #[test]
    fn subpage_does_not_redirect_sub_resource_fetches() {
        // Non-HTML assets (JS, CSS, WASM, images) must be served, not
        // redirected, or the page would never load its resources.
        for path in ["app.js", "style.css", "app.wasm", "logo.png"] {
            assert!(
                !should_redirect_subpage_to_shell(false, path, "document"),
                "{path} must not be redirected"
            );
        }
        // iframe / xhr / fetch destinations must never be redirected,
        // even for HTML paths — only top-level document loads are
        // ambiguous with pasted URLs and cross-contract clicks.
        for dest in ["iframe", "empty", "script", "style", "image", ""] {
            assert!(
                !should_redirect_subpage_to_shell(false, "page2", dest),
                "Sec-Fetch-Dest={dest} must not be redirected"
            );
        }
    }

    #[test]
    fn subpage_does_not_redirect_sandbox_requests() {
        // Sandbox iframe requests already carry the correct path and
        // must flow through the sandbox content pipeline, not be
        // redirected to the shell root (which would defeat multi-page
        // navigation inside the sandbox).
        assert!(!should_redirect_subpage_to_shell(true, "page2", "iframe"));
        assert!(!should_redirect_subpage_to_shell(true, "page2", "document"));
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
}
