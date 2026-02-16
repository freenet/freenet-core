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
use futures::future::BoxFuture;
use futures::FutureExt;
use tokio::sync::mpsc;
use tracing::instrument;

use crate::client_events::{ClientEventsProxy, ClientId, OpenRequest};
use crate::server::HostCallbackResult;

use super::{errors::WebSocketApiError, path_handlers, ApiVersion, AuthToken, ClientConnection};

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

/// Represents an attested contract entry with metadata for token expiration.
#[derive(Clone, Debug)]
pub struct AttestedContract {
    /// The contract instance ID
    pub contract_id: ContractInstanceId,
    /// The client ID associated with this token
    pub client_id: ClientId,
    /// Timestamp of when the token was last accessed (for expiration tracking)
    pub last_accessed: Instant,
}

impl AttestedContract {
    /// Create a new attested contract entry
    pub fn new(contract_id: ContractInstanceId, client_id: ClientId) -> Self {
        Self {
            contract_id,
            client_id,
            last_accessed: Instant::now(),
        }
    }
}

/// Maps authentication tokens to attested contract metadata.
pub type AttestedContractMap = Arc<DashMap<AuthToken, AttestedContract>>;

/// Handles HTTP client requests for contract access and interaction.
pub struct HttpClientApi {
    pub(crate) attested_contracts: AttestedContractMap,
    proxy_server_request: mpsc::Receiver<ClientConnection>,
    response_channels: HashMap<ClientId, mpsc::UnboundedSender<HostCallbackResult>>,
}

impl HttpClientApi {
    /// Returns the uninitialized axum router to compose with other routing handling or websockets.
    pub fn as_router(socket: &SocketAddr) -> (Self, Router) {
        let attested_contracts = Arc::new(DashMap::new());
        Self::as_router_with_attested_contracts(socket, attested_contracts)
    }

    /// Returns the uninitialized axum router with a provided attested_contracts map.
    ///
    /// Merges V1 and V2 HTTP routes; both currently share the same handler logic.
    pub fn as_router_with_attested_contracts(
        socket: &SocketAddr,
        attested_contracts: AttestedContractMap,
    ) -> (Self, Router) {
        // Controls the cookie Secure flag: when true, cookies are sent over HTTP
        // (no HTTPS required). Includes is_unspecified() so that 0.0.0.0 bindings
        // (network mode) allow HTTP cookies — most home users lack TLS.
        // Note: this is intentionally different from `localhost_only` in mod.rs,
        // which uses only is_loopback() to control WebSocket origin restrictions.
        let localhost = socket.ip().is_loopback() || socket.ip().is_unspecified();
        let contract_web_path = std::env::temp_dir().join("freenet").join("webs");
        std::fs::create_dir_all(contract_web_path).unwrap();

        let (proxy_request_sender, request_to_server) = mpsc::channel(1);

        let config = Config { localhost };

        let router = v1::routes(config.clone())
            .merge(v2::routes(config))
            .layer(Extension(attested_contracts.clone()))
            .layer(Extension(HttpClientApiRequest(proxy_request_sender)));

        (
            Self {
                proxy_server_request: request_to_server,
                attested_contracts,
                response_channels: HashMap::new(),
            },
            router,
        )
    }

    /// Returns a reference to the attested contracts map (for integration testing).
    /// This allows tests to verify token expiration behavior.
    pub fn attested_contracts(&self) -> &AttestedContractMap {
        &self.attested_contracts
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
    api_version: ApiVersion,
) -> Result<axum::response::Response, WebSocketApiError> {
    use headers::{Header, HeaderMapExt};

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
        path_handlers::contract_home(key, rs, token.clone(), api_version).await?;

    let mut response = contract_response.into_response();
    response.headers_mut().typed_insert(token_header);
    response.headers_mut().insert(
        headers::SetCookie::name(),
        headers::HeaderValue::from_str(&cookie.to_string()).unwrap(),
    );

    Ok(response)
}

async fn web_subpages(
    key: String,
    last_path: String,
    api_version: ApiVersion,
) -> Result<axum::response::Response, WebSocketApiError> {
    let version_prefix = api_version.prefix();
    let full_path: String = format!("/{version_prefix}/contract/web/{key}/{last_path}");
    path_handlers::variable_content(key, full_path, api_version)
        .await
        .map_err(|e| *e)
        .map(|r| r.into_response())
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
                            let attested = AttestedContract::new(contract, cli_id);
                            self.attested_contracts
                                .insert(assigned_token.clone(), attested);
                            tracing::debug!(
                                ?assigned_token,
                                ?contract,
                                ?cli_id,
                                "Stored assigned token in attested_contracts map"
                            );
                        }
                        self.response_channels.insert(cli_id, callbacks);
                        continue;
                    }
                    ClientConnection::Request {
                        client_id,
                        req,
                        auth_token,
                        attested_contract,
                        ..
                    } => {
                        return Ok(OpenRequest::new(client_id, req)
                            .with_token(auth_token)
                            .with_attested_contract(attested_contract))
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
