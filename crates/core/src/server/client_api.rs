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
        // Block top-level navigation to sandbox URLs. With allow-popups-to-escape-sandbox
        // on the iframe, a malicious contract could window.open() its own URL to escape
        // the sandbox and gain same-origin access to the API. Sec-Fetch-Dest: iframe
        // is set by the browser automatically and cannot be spoofed by scripts.
        let fetch_dest = req_headers
            .get("sec-fetch-dest")
            .and_then(|v| v.to_str().ok())
            .unwrap_or("");
        if fetch_dest == "document" {
            // Top-level navigation to a sandbox URL — redirect to the shell page instead
            let shell_url = format!("/{}/contract/web/{key}/", api_version.prefix());
            return Ok(axum::response::Redirect::to(&shell_url).into_response());
        }

        // Serve sandbox content (contract HTML + WS shim) inside the iframe.
        // No auth token or cookie — the shell page handles auth via postMessage.
        let contract_response =
            path_handlers::serve_sandbox_content(key, api_version, None).await?;
        let mut response = contract_response.into_response();
        add_sandbox_cors_headers(&mut response);
        // CSP for sandbox content: the iframe has an opaque origin (null) because the
        // sandbox attribute omits allow-same-origin. This means CSP 'self' won't match
        // the local API server's actual origin, so we must use the explicit local API
        // origin derived from the Host header. This allows the iframe to load scripts,
        // styles, images, and WASM from the local API server while blocking access
        // to other origins.
        let local_api_origin = req_headers
            .get(axum::http::header::HOST)
            .and_then(|h| h.to_str().ok())
            .map(|host| format!("http://{host}"))
            .unwrap_or_else(|| "'self'".to_string());
        let csp = format!(
            "default-src {local_api_origin} 'unsafe-inline' 'unsafe-eval' blob: data:; connect-src {local_api_origin} blob: data:"
        );
        if let Ok(csp_value) = axum::http::HeaderValue::from_str(&csp) {
            response
                .headers_mut()
                .insert(axum::http::header::CONTENT_SECURITY_POLICY, csp_value);
        }
        return Ok(response);
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
    // allow ws:/wss: so the bridge can establish the WebSocket connection.
    response.headers_mut().insert(
        axum::http::header::CONTENT_SECURITY_POLICY,
        axum::http::HeaderValue::from_static(
            "default-src 'none'; script-src 'unsafe-inline'; frame-src 'self'; style-src 'unsafe-inline'; img-src data:; connect-src ws: wss:",
        ),
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
        // Block top-level navigation to sandbox URLs (same protection as web_home)
        let fetch_dest = req_headers
            .get("sec-fetch-dest")
            .and_then(|v| v.to_str().ok())
            .unwrap_or("");
        if fetch_dest == "document" {
            let shell_url = format!("/{}/contract/web/{key}/", api_version.prefix());
            return Ok(axum::response::Redirect::to(&shell_url).into_response());
        }

        let contract_response =
            path_handlers::serve_sandbox_content(key, api_version, Some(&last_path)).await?;
        let mut response = contract_response.into_response();
        add_sandbox_cors_headers(&mut response);
        let local_api_origin = req_headers
            .get(axum::http::header::HOST)
            .and_then(|h| h.to_str().ok())
            .map(|host| format!("http://{host}"))
            .unwrap_or_else(|| "'self'".to_string());
        let csp = format!(
            "default-src {local_api_origin} 'unsafe-inline' 'unsafe-eval' blob: data:; connect-src {local_api_origin} blob: data:"
        );
        if let Ok(csp_value) = axum::http::HeaderValue::from_str(&csp) {
            response
                .headers_mut()
                .insert(axum::http::header::CONTENT_SECURITY_POLICY, csp_value);
        }
        return Ok(response);
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

/// Returns true if the path looks like an HTML page request.
fn is_html_page(path: &str) -> bool {
    let lower = path.to_lowercase();
    // Explicit .html or .htm extension
    if lower.ends_with(".html") || lower.ends_with(".htm") {
        return true;
    }
    // Directory-style paths (e.g., "news/") where the server would serve index.html
    if lower.ends_with('/') || !lower.contains('.') {
        return true;
    }
    false
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
}
