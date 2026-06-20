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
        .map(|host| format!("{scheme}://{host}"))
        .unwrap_or_else(|| "'self'".to_string())
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
    hosted_mode: bool,
) -> Result<axum::response::Response, WebSocketApiError> {
    // Check if this is the sandboxed iframe requesting its content
    let is_sandbox = query_string
        .as_ref()
        .map(|qs| qs.split('&').any(|p| p == "__sandbox=1"))
        .unwrap_or(false);

    if is_sandbox {
        return serve_sandbox_response(key, api_version, None, &req_headers, rs).await;
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
async fn render_shell_response(
    key: String,
    config: &Config,
    api_version: ApiVersion,
    query_string: Option<String>,
    sub_path: Option<&str>,
    rs: HttpClientApiRequest,
    hosted_mode: bool,
) -> Result<axum::response::Response, WebSocketApiError> {
    use headers::{Header, HeaderMapExt};

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
    let contract_response = path_handlers::contract_home(
        key,
        rs,
        token.clone(),
        api_version,
        query_string,
        sub_path,
        hosted_mode,
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
            &req_headers,
            request_sender,
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
        )
        .await;
    }

    let version_prefix = api_version.prefix();
    let full_path: String = format!("/{version_prefix}/contract/web/{key}/{last_path}");
    path_handlers::variable_content(key, full_path, api_version, request_sender)
        .await
        .map_err(|e| *e)
        .map(|r| {
            let mut response = r.into_response();
            add_sandbox_cors_headers(&mut response);
            response
        })
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
    let shell_url = build_canonical_shell_url(key, api_version, query_string)?;
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
/// `ContractInstanceId::from_bytes` also rejects path-traversal-style
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
    query_string: Option<&str>,
) -> Result<String, WebSocketApiError> {
    if key.is_empty() {
        return Err(WebSocketApiError::InvalidParam {
            error_cause: "empty contract key in redirect target".into(),
        });
    }
    let _instance_id =
        ContractInstanceId::from_bytes(key).map_err(|err| WebSocketApiError::InvalidParam {
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

    let prefix = api_version.prefix();
    Ok(match filtered_query {
        Some(qs) => format!("/{prefix}/contract/web/{key}/?{qs}"),
        None => format!("/{prefix}/contract/web/{key}/"),
    })
}

/// Query parameters that must be stripped before forwarding a user URL
/// into the shell. `__sandbox` is a server-interpreted routing flag;
/// `authToken` is the shell's auth credential and must only come from
/// `AuthToken::generate()`, never from an attacker-controlled URL.
fn is_sensitive_query_param(param: &str) -> bool {
    // Prefix-match so variants like `__sandbox_debug` or `authTokenExtra`
    // (from a future refactor or an adversarial URL) are also stripped.
    // Matches the filter in `path_handlers::shell_page` that forwards
    // query params into the iframe.
    param.starts_with("__sandbox") || param.starts_with("authToken")
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
async fn serve_sandbox_response(
    key: String,
    api_version: ApiVersion,
    sub_path: Option<&str>,
    req_headers: &axum::http::HeaderMap,
    request_sender: HttpClientApiRequest,
) -> Result<axum::response::Response, WebSocketApiError> {
    // Block top-level navigation to sandbox URLs. Sec-Fetch-Dest: iframe is set
    // by the browser automatically and cannot be spoofed by scripts.
    let fetch_dest = req_headers
        .get("sec-fetch-dest")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("");
    if fetch_dest == "document" {
        return redirect_to_shell_root(&key, api_version, None);
    }

    let contract_response =
        path_handlers::serve_sandbox_content(key, api_version, sub_path, request_sender).await?;
    let mut response = contract_response.into_response();
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
                        user_context,
                        ..
                    } => {
                        return Ok(OpenRequest::new(client_id, req)
                            .with_token(auth_token)
                            .with_origin_contract(origin_contract)
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
    /// 32 zero bytes so `ContractInstanceId::from_bytes` accepts it.
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
    /// `ContractInstanceId::from_bytes` first converts this into a
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

    /// A minimal localhost `Config` for handler tests (controls only the
    /// cookie Secure flag).
    fn localhost_config() -> Config {
        Config { localhost: true }
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
}
