use std::{
    collections::{HashMap, HashSet, VecDeque},
    sync::{Arc, Mutex as StdMutex, OnceLock},
    time::Duration,
};

use dashmap::DashMap;

/// Tracks auth tokens that have already been warned about to avoid log spam.
/// When a client repeatedly tries to use an invalid token (e.g., after node restart),
/// we only want to log the warning once per token rather than flooding the logs.
static WARNED_INVALID_TOKENS: OnceLock<StdMutex<HashSet<String>>> = OnceLock::new();

/// Maximum number of warned tokens to track before clearing the set.
/// This prevents unbounded memory growth if many invalid tokens are tried.
const MAX_WARNED_TOKENS: usize = 1000;

/// Special error message prefix that clients can detect to know they need to refresh
/// and get a new auth token. This is sent when a client connects with a stale token
/// that is no longer valid (e.g., after node restart).
pub const AUTH_TOKEN_INVALID_ERROR: &str = "AUTH_TOKEN_INVALID";

/// Interval for sending WebSocket ping frames to keep connection alive.
/// Idle TCP connections may be closed by the OS, browser, or intermediate
/// proxies after extended periods of inactivity. 30 seconds is a safe
/// interval that prevents most idle timeouts while not creating excessive
/// overhead.
const WEBSOCKET_PING_INTERVAL: Duration = Duration::from_secs(30);

/// Per-client rate limiter for delegate operations.
///
/// Wraps `TrackedBackoff` to throttle clients that repeatedly send requests
/// for missing/failing delegates. Without this, a single misbehaving client
/// retrying at ~3 Hz can flood the event loop and cause "node not available"
/// errors for all clients. See #3305, #3332.
///
/// Tracking approach:
/// - On outgoing requests: checks for existing backoff and rejects immediately
///   if in backoff (returns the remaining duration so the caller can send an
///   error response without forwarding to the node or blocking the event loop)
/// - On incoming responses: extracts the delegate key directly from the error
///   or success response to record backoff state (no correlation needed)
struct DelegateRateLimiter {
    backoff: TrackedBackoff<[u8; 32]>,
}

impl DelegateRateLimiter {
    fn new() -> Self {
        let config = ExponentialBackoff::new(
            Duration::from_millis(100), // base: 100ms after first failure
            Duration::from_secs(5),     // max: 5s cap
        );
        Self {
            backoff: TrackedBackoff::new(config, 64),
        }
    }

    /// Check if a delegate key is currently in backoff.
    /// Returns the remaining backoff duration, or `None` if the request can proceed.
    fn check_backoff(&self, delegate_key: &[u8]) -> Option<Duration> {
        let key = to_key_array(delegate_key)?;
        self.backoff.remaining_backoff(&key)
    }

    /// Record a delegate error for a specific key (extracted from the error response).
    fn record_error(&mut self, delegate_key: &[u8]) {
        if let Some(key) = to_key_array(delegate_key) {
            self.backoff.record_failure(key);
        }
    }

    /// Record a successful delegate response. Clears backoff for the key.
    fn record_success(&mut self, delegate_key: &[u8]) {
        if let Some(key) = to_key_array(delegate_key) {
            self.backoff.record_success(&key);
        }
    }
}

/// Convert a delegate key byte slice to a fixed-size array, or `None` if wrong length.
fn to_key_array(key: &[u8]) -> Option<[u8; 32]> {
    key.try_into().ok()
}

/// Extract the delegate key bytes from a `DelegateError`, if available.
///
/// Most `DelegateError` variants carry a `DelegateKey`; `ExecutionError` and
/// `ForbiddenSecretAccess` do not, so we return `None` for those.
fn delegate_error_key(err: &DelegateError) -> Option<&[u8]> {
    match err {
        DelegateError::RegisterError(key) => Some(key.bytes()),
        DelegateError::Missing(key) => Some(key.bytes()),
        DelegateError::MissingSecret { key, .. } => Some(key.bytes()),
        DelegateError::ExecutionError(_) | DelegateError::ForbiddenSecretAccess(_) => None,
        // non_exhaustive: future variants without a key get no tracking
        _ => None,
    }
}

use axum::{
    extract::{
        ws::{Message, WebSocket},
        Query, WebSocketUpgrade,
    },
    http::StatusCode,
    response::{IntoResponse, Response},
    routing::get,
    Extension, Router,
};
use freenet_stdlib::{
    client_api::{
        ClientRequest, ContractRequest, ContractResponse, DelegateError, ErrorKind, HostResponse,
        RequestError,
    },
    prelude::*,
};
use futures::{future::BoxFuture, stream::SplitSink, FutureExt, SinkExt, StreamExt};
use headers::Header;
use serde::Deserialize;
use tokio::sync::{mpsc, Mutex};

use crate::{
    client_events::AuthToken,
    server::{ApiVersion, ClientConnection, HostCallbackResult},
    util::{
        backoff::{ExponentialBackoff, TrackedBackoff},
        EncodingProtocol,
    },
};

use super::{ClientError, ClientEventsProxy, ClientId, HostResult, OpenRequest};
use crate::server::client_api::OriginContractMap;

/// Checks if a WebSocket Origin header value refers to localhost.
///
/// Delimiter-aware: rejects origins like `http://localhost.evil.com` by requiring
/// the hostname to be followed by `:`, `/`, or end-of-string.
fn is_localhost_origin(origin: &str) -> bool {
    let prefixes = [
        "http://localhost:",
        "http://localhost/",
        "https://localhost:",
        "https://localhost/",
        "http://127.0.0.1:",
        "http://127.0.0.1/",
        "https://127.0.0.1:",
        "https://127.0.0.1/",
        "http://[::1]:",
        "http://[::1]/",
        "https://[::1]:",
        "https://[::1]/",
    ];
    let exact = [
        "http://localhost",
        "https://localhost",
        "http://127.0.0.1",
        "https://127.0.0.1",
        "http://[::1]",
        "https://[::1]",
    ];
    prefixes.iter().any(|p| origin.starts_with(p)) || exact.contains(&origin)
}

/// Returns `true` if the request's `Host` header is in the allowed set.
fn is_allowed_host(headers: &axum::http::HeaderMap, allowed_hosts: &HashSet<String>) -> bool {
    let Some(host_header) = headers
        .get(axum::http::header::HOST)
        .and_then(|h| h.to_str().ok())
    else {
        return false;
    };
    allowed_hosts.contains(&host_header.to_lowercase())
}

/// Checks if an error represents a client-side disconnect rather than a server error.
///
/// Client disconnects are expected behavior when clients timeout, crash, or close
/// connections without proper WebSocket close handshake. These should be logged
/// as warnings rather than errors since they don't indicate server problems.
fn is_client_disconnect_error(error: &anyhow::Error) -> bool {
    let error_msg = error.to_string().to_lowercase();

    // WebSocket protocol errors from tungstenite
    if error_msg.contains("connection reset without closing handshake")
        || error_msg.contains("connection closed without")
    {
        return true;
    }

    // IO errors indicating client-side disconnects
    if error_msg.contains("connection reset")
        || error_msg.contains("connection aborted")
        || error_msg.contains("broken pipe")
        || error_msg.contains("connection refused")
        || error_msg.contains("not connected")
    {
        return true;
    }

    false
}

#[derive(Clone)]
struct WebSocketRequest(mpsc::Sender<ClientConnection>);

impl std::ops::Deref for WebSocketRequest {
    type Target = mpsc::Sender<ClientConnection>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

pub struct WebSocketProxy {
    proxy_server_request: mpsc::Receiver<ClientConnection>,
    response_channels: HashMap<ClientId, mpsc::UnboundedSender<HostCallbackResult>>,
}

/// Channel capacity for WebSocket client connections to the node event loop.
///
/// This must be large enough to handle bursts of concurrent client connections
/// and requests without filling up. When this channel is full, new client
/// connections fail with `NodeUnavailable`, which cascades to ALL connecting
/// clients — not just the ones causing the load. See #3332.
///
/// Previous value of 10 was far too small: a single misbehaving client retrying
/// at 3 Hz could saturate it within seconds, causing all other clients to get
/// "node not available" errors.
const PARALLELISM: usize = 256;

impl WebSocketProxy {
    pub fn create_router(server_routing: Router) -> (Self, Router) {
        // Create a default empty origin contracts map
        let origin_contracts = Arc::new(DashMap::new());
        Self::create_router_with_origin_contracts(server_routing, origin_contracts)
    }

    pub fn create_router_with_origin_contracts(
        server_routing: Router,
        origin_contracts: OriginContractMap,
    ) -> (Self, Router) {
        let (proxy_request_sender, proxy_server_request) = mpsc::channel(PARALLELISM);

        let ws_request = WebSocketRequest(proxy_request_sender);

        // Each version route gets its own Extension<ApiVersion> via a nested router,
        // eliminating the need for per-version wrapper functions.
        let v1_route = Router::new()
            .route("/v1/contract/command", get(websocket_commands))
            .layer(Extension(ApiVersion::V1));
        let v2_route = Router::new()
            .route("/v2/contract/command", get(websocket_commands))
            .layer(Extension(ApiVersion::V2));

        let router = server_routing
            .merge(v1_route)
            .merge(v2_route)
            .layer(Extension(origin_contracts))
            .layer(Extension(ws_request))
            .layer(axum::middleware::from_fn(connection_info));

        (
            WebSocketProxy {
                proxy_server_request,
                response_channels: HashMap::new(),
            },
            router,
        )
    }

    /// Sets up a subscription notification channel and builds the `OpenRequest`.
    ///
    /// Returns `None` if the client has already disconnected (response channel
    /// closed or missing). Returning `None` instead of an error prevents
    /// transient disconnects from killing `client_fn` (#3479).
    fn setup_subscription(
        &self,
        client_id: ClientId,
        key: ContractInstanceId,
        req: Box<ClientRequest<'static>>,
        auth_token: Option<AuthToken>,
        origin_contract: Option<ContractInstanceId>,
    ) -> Option<OpenRequest<'static>> {
        let (tx, rx) =
            tokio::sync::mpsc::channel(crate::contract::SUBSCRIBER_NOTIFICATION_CHANNEL_SIZE);
        let ch = self.response_channels.get(&client_id)?;
        if ch
            .send(HostCallbackResult::SubscriptionChannel {
                key,
                id: client_id,
                callback: rx,
            })
            .is_err()
        {
            tracing::debug!(%client_id, "Client channel closed during subscription setup");
            return None;
        }
        Some(
            OpenRequest::new(client_id, req)
                .with_notification(tx)
                .with_token(auth_token)
                .with_origin_contract(origin_contract),
        )
    }

    async fn internal_proxy_recv(
        &mut self,
        msg: ClientConnection,
    ) -> Result<Option<OpenRequest<'_>>, ClientError> {
        match msg {
            ClientConnection::NewConnection { callbacks, .. } => {
                let cli_id = ClientId::next();
                if callbacks
                    .send(HostCallbackResult::NewId { id: cli_id })
                    .is_err()
                {
                    // Client disconnected before ID assignment -- normal under load (#3479).
                    tracing::debug!(
                        %cli_id,
                        "NewConnection callback closed -- client disconnected before ID assignment"
                    );
                    return Ok(None);
                }
                self.response_channels.insert(cli_id, callbacks);
                Ok(None)
            }
            ClientConnection::Request {
                client_id,
                req,
                auth_token,
                origin_contract,
                ..
            } => {
                // Extract the subscription key if this request needs a notification channel.
                let sub_key = match &*req {
                    ClientRequest::ContractOp(ContractRequest::Subscribe { key, .. }) => Some(*key),
                    ClientRequest::ContractOp(ContractRequest::Get {
                        key,
                        subscribe: true,
                        ..
                    }) => Some(*key),
                    ClientRequest::ContractOp(ContractRequest::Put {
                        contract,
                        subscribe: true,
                        ..
                    }) => Some(*contract.key().id()),
                    ClientRequest::DelegateOp(_)
                    | ClientRequest::ContractOp(_)
                    | ClientRequest::Disconnect { .. }
                    | ClientRequest::Authenticate { .. }
                    | ClientRequest::NodeQueries(_)
                    | ClientRequest::Close
                    | _ => None,
                };

                let open_req = if let Some(key) = sub_key {
                    tracing::debug!(%client_id, contract = %key, "setting up subscription channel");
                    match self.setup_subscription(client_id, key, req, auth_token, origin_contract)
                    {
                        Some(r) => r,
                        None => return Ok(None),
                    }
                } else {
                    OpenRequest::new(client_id, req)
                        .with_token(auth_token)
                        .with_origin_contract(origin_contract)
                };
                Ok(Some(open_req))
            }
        }
    }
}

struct EncodingProtocolExt(EncodingProtocol);

impl headers::Header for EncodingProtocolExt {
    fn name() -> &'static axum::http::HeaderName {
        static HEADER: OnceLock<axum::http::HeaderName> = OnceLock::new();
        HEADER.get_or_init(|| axum::http::HeaderName::from_static("encoding-protocol"))
    }

    fn decode<'i, I>(values: &mut I) -> Result<Self, headers::Error>
    where
        Self: Sized,
        I: Iterator<Item = &'i axum::http::HeaderValue>,
    {
        values
            .next()
            .and_then(|val| match val.to_str().ok()? {
                "native" => Some(EncodingProtocolExt(EncodingProtocol::Native)),
                "flatbuffers" => Some(EncodingProtocolExt(EncodingProtocol::Flatbuffers)),
                _ => None,
            })
            .ok_or_else(headers::Error::invalid)
    }

    fn encode<E: Extend<axum::http::HeaderValue>>(&self, values: &mut E) {
        let header = match self.0 {
            EncodingProtocol::Native => axum::http::HeaderValue::from_static("native"),
            EncodingProtocol::Flatbuffers => axum::http::HeaderValue::from_static("flatbuffers"),
        };
        values.extend([header]);
    }
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct ConnectionInfo {
    auth_token: Option<AuthToken>,
    encoding_protocol: Option<EncodingProtocol>,
    /// Accepted for backward compatibility but ignored — streaming is always enabled.
    #[allow(dead_code)]
    streaming: Option<bool>,
}

async fn connection_info(
    Query(ConnectionInfo {
        auth_token: auth_token_q,
        encoding_protocol,
        streaming: _,
    }): Query<ConnectionInfo>,
    Extension(allowed_hosts): Extension<crate::server::AllowedHosts>,
    mut req: axum::extract::Request,
    next: axum::middleware::Next,
) -> Response {
    use headers::{
        authorization::{Authorization, Bearer},
        HeaderMapExt,
    };
    // tracing::info!(
    //     "headers: {:?}",
    //     req.headers()
    //         .iter()
    //         .flat_map(|(k, v)| v.to_str().ok().map(|v| format!("{k}: {v}")))
    //         .collect::<Vec<_>>()
    // );

    let encoding_protoc = match req.headers().typed_try_get::<EncodingProtocolExt>() {
        Ok(Some(protoc)) => protoc.0,
        Ok(None) => encoding_protocol.unwrap_or(EncodingProtocol::Flatbuffers),
        Err(_error) => {
            return (
                StatusCode::BAD_REQUEST,
                format!(
                    "Incorrect `{header}` header specification",
                    header = EncodingProtocolExt::name()
                ),
            )
                .into_response()
        }
    };

    let auth_token = match req.headers().typed_try_get::<Authorization<Bearer>>() {
        Ok(Some(value)) => Some(AuthToken::from(value.token().to_owned())),
        Ok(None) => auth_token_q.clone(),
        Err(_error) => {
            return (
                StatusCode::BAD_REQUEST,
                format!(
                    "Incorrect Bearer `{header}` header specification",
                    header = Authorization::<Bearer>::name()
                ),
            )
                .into_response()
        }
    };

    // Check Origin header for WebSocket upgrades only.
    // This blocks cross-site WebSocket hijacking (evil.com connecting to our gateway)
    // while allowing both localhost and legitimate remote access.
    //
    // Only applied to WebSocket upgrade requests — regular HTTP requests (e.g., asset
    // loads from sandboxed iframes with Origin: null) must pass through without origin
    // validation, since iframe sub-resources legitimately have opaque origins.
    let is_ws_upgrade = req
        .headers()
        .get(axum::http::header::UPGRADE)
        .and_then(|v| v.to_str().ok())
        .is_some_and(|v| v.eq_ignore_ascii_case("websocket"));

    if is_ws_upgrade {
        if let Some(origin) = req.headers().get(axum::http::header::ORIGIN) {
            if let Ok(origin_str) = origin.to_str() {
                if !is_localhost_origin(origin_str)
                    && !is_allowed_host(req.headers(), &allowed_hosts)
                {
                    let host_header = req
                        .headers()
                        .get(axum::http::header::HOST)
                        .and_then(|h| h.to_str().ok());
                    tracing::warn!(
                        origin = origin_str,
                        host = ?host_header,
                        "Rejected WebSocket connection: Host header not in allowed hosts \
                         (possible DNS rebinding attack)"
                    );
                    return (
                        StatusCode::FORBIDDEN,
                        "WebSocket connections from this origin are not allowed",
                    )
                        .into_response();
                }
            } else {
                return (StatusCode::BAD_REQUEST, "Invalid Origin header").into_response();
            }
        }
    }

    tracing::debug!(
        ?auth_token_q, ?auth_token, request_uri = ?req.uri(), "connection_info middleware extracting auth token and encoding protocol",
    );
    req.extensions_mut().insert(encoding_protoc);
    req.extensions_mut().insert(auth_token);
    // `streaming` query parameter is accepted but ignored — streaming is now
    // always enabled for payloads exceeding CHUNK_THRESHOLD (512 KiB).

    next.run(req).await
}

async fn websocket_commands(
    ws: WebSocketUpgrade,
    Extension(auth_token): Extension<Option<AuthToken>>,
    Extension(encoding_protoc): Extension<EncodingProtocol>,
    Extension(rs): Extension<WebSocketRequest>,
    Extension(origin_contracts): Extension<OriginContractMap>,
    Extension(api_version): Extension<ApiVersion>,
) -> Response {
    let on_upgrade = move |ws: WebSocket| async move {
        // Get the data we need from the DashMap
        // Track whether a token was provided but is invalid (stale/expired)
        let (auth_and_instance, token_is_invalid) = if let Some(token) = auth_token.as_ref() {
            // Only collect and log map contents when trace is enabled
            if tracing::enabled!(tracing::Level::TRACE) {
                let map_contents: Vec<_> =
                    origin_contracts.iter().map(|e| e.key().clone()).collect();
                tracing::trace!(?token, "origin_contracts map keys: {:?}", map_contents);
            }

            if let Some(entry) = origin_contracts.get(token) {
                let origin = entry.value();
                tracing::trace!(?token, contract_id = ?origin.contract_id, "Found token in origin_contracts map");
                (Some((token.clone(), origin.contract_id)), false)
            } else {
                // Rate-limit warnings: only log once per unique token to avoid log spam
                // when clients repeatedly retry with the same stale token
                let warned_tokens =
                    WARNED_INVALID_TOKENS.get_or_init(|| StdMutex::new(HashSet::new()));
                let token_str = token.as_str().to_string();
                let mut warned = warned_tokens.lock().unwrap();

                // Clear set if it gets too large to prevent unbounded memory growth
                if warned.len() >= MAX_WARNED_TOKENS {
                    warned.clear();
                }

                if warned.insert(token_str) {
                    // First time seeing this invalid token - log it
                    tracing::warn!(
                        ?token,
                        "Auth token not found in origin_contracts map. \
                         This usually means the node was restarted and the client has a stale token. \
                         Client should refresh the page to get a new token."
                    );
                } else {
                    // Already warned about this token - use debug level to reduce noise
                    tracing::debug!(?token, "Auth token still not found (already warned)");
                }
                (None, true) // Token was provided but is invalid
            }
        } else {
            tracing::trace!("No auth token provided in WebSocket request");
            (None, false) // No token provided - not an error
        };

        // Only evaluate auth_and_instance for trace when trace is enabled
        if tracing::enabled!(tracing::Level::TRACE) {
            tracing::trace!(protoc = ?ws.protocol(), ?auth_and_instance, "websocket connection established");
        } else {
            tracing::trace!(protoc = ?ws.protocol(), "websocket connection established");
        }
        if let Err(error) = websocket_interface(
            rs.clone(),
            auth_and_instance,
            token_is_invalid,
            encoding_protoc,
            api_version,
            ws,
        )
        .await
        {
            // Client-side disconnects (e.g., closing without handshake) are expected
            // and should not be logged as errors. These occur when clients timeout,
            // crash, or close connections abruptly.
            if is_client_disconnect_error(&error) {
                tracing::warn!("WebSocket client disconnect: {error}");
            } else {
                tracing::error!("WebSocket protocol error: {error}");
            }
        }
    };

    // 100MB limit: WASM contract uploads can be very large and the default
    // ~64KB would reject them. Streaming chunks individual responses but the
    // initial PUT still arrives as a single WebSocket message.
    ws.max_message_size(100 * 1024 * 1024)
        .on_upgrade(on_upgrade)
}

/// Send a synthetic Disconnect to the node so subscription cleanup always runs.
async fn notify_disconnect(
    request_sender: &WebSocketRequest,
    client_id: ClientId,
    auth_token: &Option<(AuthToken, ContractInstanceId)>,
    api_version: ApiVersion,
) {
    tracing::debug!(%client_id, "Notifying node of disconnect for subscription cleanup");
    if let Err(e) = request_sender
        .send(ClientConnection::Request {
            client_id,
            req: Box::new(ClientRequest::Disconnect { cause: None }),
            auth_token: auth_token.as_ref().map(|t| t.0.clone()),
            origin_contract: auth_token.as_ref().map(|t| t.1),
            api_version,
        })
        .await
    {
        tracing::debug!(%client_id, error = %e, "Failed to send disconnect notification");
    }
}

async fn websocket_interface(
    request_sender: WebSocketRequest,
    mut auth_token: Option<(AuthToken, ContractInstanceId)>,
    token_is_invalid: bool,
    encoding_protoc: EncodingProtocol,
    api_version: ApiVersion,
    ws: WebSocket,
) -> anyhow::Result<()> {
    let (mut response_rx, client_id) =
        new_client_connection(&request_sender, auth_token.clone()).await?;
    let (mut server_sink, mut client_stream) = ws.split();
    let contract_updates: Arc<Mutex<VecDeque<(_, mpsc::Receiver<HostResult>)>>> =
        Arc::new(Mutex::new(VecDeque::new()));

    // ReassemblyBuffer evicts incomplete streams after STREAM_TTL (60s) via
    // evict_stale(), called on every receive_chunk(). Concurrent streams are
    // capped at MAX_CONCURRENT_STREAMS (8).
    let mut conn_state = ConnectionState {
        encoding_protoc,
        reassembly: freenet_stdlib::client_api::streaming::ReassemblyBuffer::new(),
        next_stream_id: 0,
    };

    // If a token was provided but is invalid (stale/expired), immediately send an error
    // to the client so it can refresh and get a new token. This prevents endless retry loops.
    if token_is_invalid {
        let error_msg = format!(
            "{}: The auth token is no longer valid. This usually happens after a node restart. \
             Please refresh the page to get a new token.",
            AUTH_TOKEN_INVALID_ERROR
        );
        let error: ClientError = ErrorKind::Unhandled {
            cause: std::borrow::Cow::Owned(error_msg),
        }
        .into();

        let serialized_error = match encoding_protoc {
            EncodingProtocol::Flatbuffers => error
                .into_fbs_bytes()
                .map_err(|e| anyhow::anyhow!("Failed to serialize error: {:?}", e))?,
            EncodingProtocol::Native => {
                bincode::serialize(&Err::<HostResponse, ClientError>(error))?
            }
        };

        send_response_message(&mut server_sink, serialized_error, &mut conn_state, None).await?;

        tracing::debug!("Sent AUTH_TOKEN_INVALID error to client, closing connection");
        // Close the connection after sending the error. Keeping it open causes delegate
        // failures: all subsequent messages would be processed with origin_contract=None
        // (since auth_token=None), so the delegate receives origin=None and fails with
        // "missing message origin". The client handles AUTH_TOKEN_INVALID by reloading
        // the page to get a fresh token, so closing is the correct behavior.
        notify_disconnect(&request_sender, client_id, &auth_token, api_version).await;
        if let Err(e) = server_sink.send(Message::Close(None)).await {
            tracing::debug!(error = %e, "Failed to send WebSocket close frame after auth error");
        }
        return Ok(());
    }

    // Per-connection rate limiter for delegate operations (#3305, #3332)
    let mut delegate_rate_limiter = DelegateRateLimiter::new();

    // Create ping interval to keep connection alive and prevent idle timeout
    let mut ping_interval = tokio::time::interval(WEBSOCKET_PING_INTERVAL);
    // Don't send ping immediately on connection, wait for first interval
    ping_interval.tick().await;

    loop {
        let contract_updates_cp = contract_updates.clone();
        let listeners_task = async move {
            loop {
                let mut lock = contract_updates_cp.lock().await;
                let active_listeners = &mut *lock;
                for _ in 0..active_listeners.len() {
                    if let Some((key, mut listener)) = active_listeners.pop_front() {
                        match listener.try_recv() {
                            Ok(r) => {
                                active_listeners.push_back((key, listener));
                                return Ok::<_, anyhow::Error>(r);
                            }
                            Err(mpsc::error::TryRecvError::Empty) => {
                                active_listeners.push_back((key, listener));
                            }
                            Err(mpsc::error::TryRecvError::Disconnected) => {
                                tracing::debug!(contract = %key, "listener removed");
                            }
                        }
                    }
                }
                std::mem::drop(lock);
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        };

        // IMPORTANT: Only cancellation-safe futures (recv, next, tick) go inside the
        // select futures below. Processing functions (process_client_request,
        // process_host_response) run in branch handlers AFTER the select resolves,
        // so they cannot be cancelled by other branches.
        //
        // NOTE: Do NOT add `biased;` here. Biased select polls branches in declaration
        // order, which starves host responses, subscription notifications, and pings
        // when client messages arrive in bursts.
        tokio::select! {
            next_msg = client_stream.next() => {
                let next_msg = match next_msg
                    .ok_or_else::<ClientError, _>(|| ErrorKind::Disconnect.into())
                {
                    Err(err) => {
                        tracing::debug!(err = %err, "client channel closed");
                        notify_disconnect(&request_sender, client_id, &auth_token, api_version).await;
                        if let Err(e) = server_sink.send(Message::Close(None)).await {
                            tracing::debug!(error = %e, "Failed to send WebSocket close frame");
                        }
                        return Ok(());
                    }
                    Ok(v) => v,
                };
                // Process the request outside the select — runs to completion
                match process_client_request(
                    client_id,
                    next_msg,
                    &request_sender,
                    &mut auth_token.as_mut().map(|t| t.0.clone()),
                    auth_token.as_mut().map(|t| t.1),
                    api_version,
                    &mut delegate_rate_limiter,
                    &mut conn_state,
                )
                .await
                {
                    Ok(Some(error)) => {
                        if let Err(err) = server_sink.send(error).await {
                            tracing::debug!(err = %err, "error sending error response to client");
                            notify_disconnect(&request_sender, client_id, &auth_token, api_version).await;
                            return Err(err.into());
                        }
                    }
                    Ok(None) => continue,
                    Err(None) => {
                        tracing::debug!(%client_id, "client channel closed during request processing");
                        notify_disconnect(&request_sender, client_id, &auth_token, api_version).await;
                        if let Err(e) = server_sink.send(Message::Close(None)).await {
                            tracing::debug!(error = %e, "Failed to send WebSocket close frame");
                        }
                        return Ok(())
                    },
                    Err(Some(err)) => {
                        tracing::debug!(%client_id, err = %err, "client request error");
                        notify_disconnect(&request_sender, client_id, &auth_token, api_version).await;
                        return Err(err)
                    },
                }
            }
            msg = response_rx.recv() => {
                // process_host_response runs in the branch handler (not the select future)
                // so it cannot be cancelled by other branches resolving first.
                let msg = match process_host_response(msg, client_id, &mut server_sink, &mut delegate_rate_limiter, &mut conn_state).await {
                    Ok(msg) => msg,
                    Err(err) => {
                        notify_disconnect(&request_sender, client_id, &auth_token, api_version).await;
                        return Err(err);
                    }
                };
                if let Some(NewSubscription { key, callback }) = msg {
                    tracing::debug!(cli_id = %client_id, contract = %key, "added new notification listener");
                    let active_listeners = &mut *contract_updates.lock().await;
                    active_listeners.push_back((key, callback));
                }
            }
            response = listeners_task => {
                let response = match response {
                    Ok(r) => r,
                    Err(err) => {
                        notify_disconnect(&request_sender, client_id, &auth_token, api_version).await;
                        return Err(err);
                    }
                };
                match &response {
                    Ok(res) => tracing::debug!(response = %res, cli_id = %client_id, "sending notification"),
                    Err(err) => tracing::debug!(response = %err, cli_id = %client_id, "sending notification error"),
                }
                let stream_content = extract_stream_content(&response);
                let serialized_res = match conn_state.encoding_protoc {
                    EncodingProtocol::Flatbuffers => match response {
                        Ok(res) => match res.into_fbs_bytes() {
                            Ok(b) => b,
                            Err(err) => {
                                notify_disconnect(&request_sender, client_id, &auth_token, api_version).await;
                                return Err(err.into());
                            }
                        },
                        Err(err) => match err.into_fbs_bytes() {
                            Ok(b) => b,
                            Err(err) => {
                                notify_disconnect(&request_sender, client_id, &auth_token, api_version).await;
                                return Err(err.into());
                            }
                        },
                    },
                    EncodingProtocol::Native => match bincode::serialize(&response) {
                        Ok(b) => b,
                        Err(err) => {
                            notify_disconnect(&request_sender, client_id, &auth_token, api_version).await;
                            return Err(err.into());
                        }
                    },
                };
                if let Err(err) = send_response_message(&mut server_sink, serialized_res, &mut conn_state, stream_content).await {
                    tracing::debug!(err = %err, "error sending notification to client");
                    notify_disconnect(&request_sender, client_id, &auth_token, api_version).await;
                    return Err(err.into());
                }
            }
            // Send periodic pings to keep connection alive and prevent idle timeout
            _ = ping_interval.tick() => {
                tracing::trace!(%client_id, "sending WebSocket ping to keep connection alive");
                if let Err(err) = server_sink.send(Message::Ping(vec![].into())).await {
                    tracing::debug!(%client_id, %err, "ping failed, connection dead");
                    notify_disconnect(&request_sender, client_id, &auth_token, api_version).await;
                    return Err(err.into());
                }
            }
        }
    }
}

/// Maximum chunks to send before yielding to the tokio runtime.
/// Prevents starving pings, subscriptions, and other select arms when
/// sending large chunked responses (e.g., 200 chunks for a 50 MiB payload).
const MAX_CHUNKS_PER_BATCH: usize = 64;

/// Prepare response messages for non-async contexts (unit tests).
/// For the actual send path, use `send_response_message` which serializes
/// and sends one chunk at a time to avoid doubling peak memory.
#[cfg(test)]
fn prepare_response_messages(
    payload: Vec<u8>,
    conn_state: &mut ConnectionState,
    stream_content: Option<freenet_stdlib::client_api::StreamContent>,
) -> Result<Vec<Vec<u8>>, Box<dyn std::error::Error + Send + Sync>> {
    use freenet_stdlib::client_api::streaming::{chunk_response, CHUNK_THRESHOLD};

    if payload.len() > CHUNK_THRESHOLD {
        let stream_id = conn_state.next_stream_id;
        conn_state.next_stream_id = conn_state.next_stream_id.wrapping_add(1);

        let mut messages = Vec::new();

        if let Some(content) = stream_content {
            if conn_state.encoding_protoc == EncodingProtocol::Native {
                let header: HostResponse = HostResponse::StreamHeader {
                    stream_id,
                    total_bytes: payload.len() as u64,
                    content,
                };
                messages.push(bincode::serialize(&Ok::<_, ClientError>(header))?);
            }
        }

        let chunks = chunk_response(payload, stream_id);
        match conn_state.encoding_protoc {
            EncodingProtocol::Flatbuffers => {
                for chunk in chunks {
                    messages.push(chunk.into_fbs_bytes()?);
                }
            }
            EncodingProtocol::Native => {
                for chunk in chunks {
                    messages.push(bincode::serialize(&Ok::<_, ClientError>(chunk))?);
                }
            }
        }
        Ok(messages)
    } else {
        Ok(vec![payload])
    }
}

async fn send_response_message(
    tx: &mut futures::stream::SplitSink<WebSocket, Message>,
    serialized: Vec<u8>,
    conn_state: &mut ConnectionState,
    stream_content: Option<freenet_stdlib::client_api::StreamContent>,
) -> Result<(), axum::Error> {
    use freenet_stdlib::client_api::streaming::{chunk_response, CHUNK_THRESHOLD};

    if serialized.len() > CHUNK_THRESHOLD {
        let stream_id = conn_state.next_stream_id;
        conn_state.next_stream_id = conn_state.next_stream_id.wrapping_add(1);

        // Send StreamHeader before chunks if content type is known.
        // StreamHeader is only supported over Native encoding (bincode), since
        // flatbuffers clients use transparent reassembly via StreamChunk only.
        if let Some(content) = stream_content {
            if conn_state.encoding_protoc == EncodingProtocol::Native {
                let header: HostResponse = HostResponse::StreamHeader {
                    stream_id,
                    total_bytes: serialized.len() as u64,
                    content,
                };
                let header_bytes =
                    bincode::serialize(&Ok::<_, ClientError>(header)).map_err(axum::Error::new)?;
                tx.send(Message::Binary(header_bytes.into())).await?;
            }
        }

        // Serialize and send each chunk individually to avoid materializing
        // all chunks in memory at once (which would roughly double peak memory
        // for large payloads).
        let chunks = chunk_response(serialized, stream_id);
        for (i, chunk) in chunks.into_iter().enumerate() {
            let b = match conn_state.encoding_protoc {
                EncodingProtocol::Flatbuffers => {
                    chunk.into_fbs_bytes().map_err(axum::Error::new)?
                }
                EncodingProtocol::Native => {
                    bincode::serialize(&Ok::<_, ClientError>(chunk)).map_err(axum::Error::new)?
                }
            };
            tx.send(Message::Binary(b.into())).await?;

            // Yield to the tokio runtime periodically so pings, subscriptions,
            // and other control flow in the connection's select loop can proceed.
            if (i + 1) % MAX_CHUNKS_PER_BATCH == 0 {
                tokio::task::yield_now().await;
            }
        }
        Ok(())
    } else {
        tx.send(Message::Binary(serialized.into())).await
    }
}

async fn new_client_connection(
    request_sender: &WebSocketRequest,
    assigned_token: Option<(AuthToken, ContractInstanceId)>,
) -> Result<(mpsc::UnboundedReceiver<HostCallbackResult>, ClientId), ClientError> {
    let (response_sender, mut response_recv) = mpsc::unbounded_channel();
    tracing::debug!(?assigned_token, "sending new client connection request");
    request_sender
        .send(ClientConnection::NewConnection {
            callbacks: response_sender,
            assigned_token,
        })
        .await
        .map_err(|_| ErrorKind::NodeUnavailable)?;
    match response_recv.recv().await {
        Some(HostCallbackResult::NewId { id: client_id, .. }) => Ok((response_recv, client_id)),
        None => Err(ErrorKind::NodeUnavailable.into()),
        other => unreachable!("received unexpected message after NewConnection: {other:?}"),
    }
}

struct NewSubscription {
    key: ContractInstanceId,
    callback: mpsc::Receiver<HostResult>,
}

struct ConnectionState {
    encoding_protoc: EncodingProtocol,
    reassembly: freenet_stdlib::client_api::streaming::ReassemblyBuffer,
    next_stream_id: u32,
}

/// Extract `StreamContent` metadata from a host response for `StreamHeader`.
/// Returns `Some` for response types where incremental consumption is useful
/// (currently `GetResponse`), `None` for all others.
fn extract_stream_content(
    result: &Result<HostResponse, ClientError>,
) -> Option<freenet_stdlib::client_api::StreamContent> {
    match result {
        Ok(HostResponse::ContractResponse(ContractResponse::GetResponse {
            key, contract, ..
        })) => Some(freenet_stdlib::client_api::StreamContent::GetResponse {
            key: *key,
            includes_contract: contract.is_some(),
        }),
        _ => None,
    }
}

#[allow(clippy::too_many_arguments)]
async fn process_client_request(
    client_id: ClientId,
    msg: Result<Message, axum::Error>,
    request_sender: &mpsc::Sender<ClientConnection>,
    auth_token: &mut Option<AuthToken>,
    origin_contract: Option<ContractInstanceId>,
    api_version: ApiVersion,
    rate_limiter: &mut DelegateRateLimiter,
    conn_state: &mut ConnectionState,
) -> Result<Option<Message>, Option<anyhow::Error>> {
    let raw_msg = match msg {
        Ok(Message::Binary(data)) => data.to_vec(),
        Ok(Message::Text(data)) => data.as_bytes().to_vec(),
        Ok(Message::Close(_)) => return Err(None),
        Ok(Message::Ping(ping)) => return Ok(Some(Message::Pong(ping))),
        Ok(m) => {
            tracing::debug!(msg = ?m, "received random message");
            return Ok(None);
        }
        Err(err) => return Err(Some(err.into())),
    };

    // Decode raw bytes into a ClientRequest based on the encoding protocol.
    // Returns the decoded request or a WebSocket error message to send back.
    fn decode_client_request(
        bytes: &[u8],
        encoding: EncodingProtocol,
    ) -> Result<ClientRequest<'static>, Option<Message>> {
        match encoding {
            EncodingProtocol::Flatbuffers => ClientRequest::try_decode_fbs(bytes)
                .map(|d| d.into_owned())
                .map_err(|err| Some(Message::Binary(err.into_fbs_bytes().into()))),
            EncodingProtocol::Native => bincode::deserialize::<ClientRequest>(bytes)
                .map(|d| d.into_owned())
                .map_err(|err| {
                    match bincode::serialize(&Err::<HostResponse, ClientError>(
                        ErrorKind::DeserializationError {
                            cause: format!("{err}").into(),
                        }
                        .into(),
                    )) {
                        Ok(b) => Some(Message::Binary(b.into())),
                        Err(ser_err) => {
                            tracing::error!("failed to serialize error response: {ser_err}");
                            None
                        }
                    }
                }),
        }
    }

    let req = match decode_client_request(&raw_msg, conn_state.encoding_protoc) {
        Ok(req) => req,
        Err(msg) => return Ok(msg),
    };

    // Handle StreamChunk: reassemble chunked requests from any client.
    // freenet-stdlib 0.2.2+ automatically chunks large ClientRequest messages
    // (>512 KiB), and the server always reassembles them. Both directions
    // (client→server and server→client) chunk transparently above the threshold.
    let req = if let ClientRequest::StreamChunk {
        stream_id,
        index,
        total,
        data,
    } = req
    {
        match conn_state
            .reassembly
            .receive_chunk(stream_id, index, total, data)
        {
            Ok(Some(complete)) => {
                match decode_client_request(&complete, conn_state.encoding_protoc) {
                    Ok(req) => req,
                    Err(msg) => return Ok(msg),
                }
            }
            Ok(None) => return Ok(None),
            Err(e) => {
                tracing::warn!(%client_id, error = %e, "streaming reassembly error");
                // Send error response to client instead of killing the connection.
                // A single malformed chunk (duplicate, out-of-range, etc.) should not
                // terminate the entire session.
                let error_msg = match bincode::serialize(&Err::<HostResponse, ClientError>(
                    ErrorKind::Unhandled {
                        cause: format!("stream reassembly error: {e}").into(),
                    }
                    .into(),
                )) {
                    Ok(b) => Some(Message::Binary(b.into())),
                    Err(ser_err) => {
                        tracing::error!("failed to serialize reassembly error: {ser_err}");
                        None
                    }
                };
                return Ok(error_msg);
            }
        }
    } else {
        req
    };

    // Rate-limit delegate requests that have been failing repeatedly (#3305).
    // If the delegate key is in backoff, reject the request immediately without
    // forwarding it to the node. This prevents a single misbehaving client from
    // flooding the event loop (which caused "node not available" for all clients,
    // #3332) and avoids blocking the connection's event loop with a sleep (which
    // would stall pings, subscriptions, and other responses).
    if let ClientRequest::DelegateOp(ref delegate_req) = req {
        let key_bytes: &[u8] = delegate_req.key().bytes();
        if let Some(remaining) = rate_limiter.check_backoff(key_bytes) {
            tracing::warn!(
                %client_id,
                delegate_key = %delegate_req.key(),
                backoff_ms = remaining.as_millis(),
                "Rejecting delegate request due to repeated failures (retry after backoff)"
            );
            let error: ClientError = ErrorKind::RequestError(RequestError::DelegateError(
                DelegateError::Missing(delegate_req.key().clone()),
            ))
            .into();
            let serialized = match conn_state.encoding_protoc {
                EncodingProtocol::Flatbuffers => error
                    .into_fbs_bytes()
                    .map_err(|e| Some(anyhow::anyhow!("serialize error: {:?}", e)))?,
                EncodingProtocol::Native => {
                    bincode::serialize(&Err::<HostResponse, ClientError>(error))
                        .map_err(|e| Some(anyhow::anyhow!("serialize error: {:?}", e)))?
                }
            };
            return Ok(Some(Message::Binary(serialized.into())));
        }
    }

    // Scope check: contract web apps (identified by origin_contract) cannot use NodeQueries.
    // This prevents malicious contracts from exfiltrating peer topology data.
    if origin_contract.is_some() {
        if let ClientRequest::NodeQueries(_) = &req {
            tracing::warn!(
                %client_id,
                contract = ?origin_contract,
                "Blocked NodeQueries from contract web app"
            );
            let error: ClientError = ErrorKind::Unhandled {
                cause: std::borrow::Cow::Borrowed(
                    "NodeQueries is not available to contract web applications",
                ),
            }
            .into();
            let serialized = match conn_state.encoding_protoc {
                EncodingProtocol::Flatbuffers => error
                    .into_fbs_bytes()
                    .map_err(|e| Some(anyhow::anyhow!("serialize error: {:?}", e)))?,
                EncodingProtocol::Native => {
                    bincode::serialize(&Err::<HostResponse, ClientError>(error))
                        .map_err(|e| Some(anyhow::anyhow!("serialize error: {:?}", e)))?
                }
            };
            return Ok(Some(Message::Binary(serialized.into())));
        }
    }

    // Intercept explicit disconnect requests sent by the client as data messages
    if matches!(req, ClientRequest::Disconnect { .. }) {
        // Treat this like a WebSocket close message
        tracing::debug!("Client explicitly sent a Disconnect request, closing connection.");
        return Err(None); // Signal graceful closure to websocket_interface
    }

    if let ClientRequest::Authenticate { token } = &req {
        *auth_token = Some(AuthToken::from(token.clone()));
    }

    tracing::debug!(req = %req, "received client request");
    request_sender
        .send(ClientConnection::Request {
            client_id,
            req: Box::new(req),
            auth_token: auth_token.clone(),
            origin_contract,
            api_version,
        })
        .await
        .map_err(|err| Some(err.into()))?;
    Ok(None)
}

async fn process_host_response(
    msg: Option<HostCallbackResult>,
    client_id: ClientId,
    tx: &mut SplitSink<WebSocket, Message>,
    rate_limiter: &mut DelegateRateLimiter,
    conn_state: &mut ConnectionState,
) -> anyhow::Result<Option<NewSubscription>> {
    let encoding_protoc = conn_state.encoding_protoc;
    match msg {
        Some(HostCallbackResult::Result { id, result }) => {
            debug_assert_eq!(id, client_id);

            // Update delegate rate limiter based on response (#3305).
            // On success: clear backoff so the client can resume normal speed.
            // On delegate error: extract the delegate key from the error and
            // record the failure. We match only DelegateError (not ContractError,
            // Disconnect, or Timeout) to avoid penalizing delegate keys for
            // unrelated failures.
            match &result {
                Ok(HostResponse::DelegateResponse { key, .. }) => {
                    rate_limiter.record_success(key.bytes());
                }
                Err(err)
                    if matches!(
                        err.kind(),
                        ErrorKind::RequestError(RequestError::DelegateError(_))
                    ) =>
                {
                    if let ErrorKind::RequestError(RequestError::DelegateError(delegate_err)) =
                        err.kind()
                    {
                        if let Some(key_bytes) = delegate_error_key(delegate_err) {
                            rate_limiter.record_error(key_bytes);
                        }
                    }
                }
                _ => {}
            }

            let result = match result {
                Ok(res) => {
                    let response_type = match res {
                        HostResponse::ContractResponse { .. } => "ContractResponse",
                        HostResponse::DelegateResponse { .. } => "DelegateResponse",
                        HostResponse::QueryResponse(_) => "QueryResponse",
                        HostResponse::Ok => "HostResponse::Ok",
                        HostResponse::StreamChunk { .. } => "StreamChunk",
                        HostResponse::StreamHeader { .. } => "StreamHeader",
                        _ => "Unknown",
                    };

                    // Enhanced logging for UPDATE responses
                    match &res {
                        HostResponse::ContractResponse(ContractResponse::UpdateResponse {
                            key,
                            summary,
                        }) => {
                            tracing::debug!(
                                client_id = %id,
                                contract = %key,
                                summary_size = summary.size(),
                                phase = "update_response",
                                "Processing UpdateResponse for WebSocket delivery"
                            );
                        }
                        HostResponse::ContractResponse(_)
                        | HostResponse::DelegateResponse { .. }
                        | HostResponse::QueryResponse(_)
                        | HostResponse::Ok
                        | _ => {
                            tracing::debug!(response = %res, response_type, cli_id = %id, "sending response");
                        }
                    }

                    match res {
                        HostResponse::ContractResponse(ContractResponse::GetResponse {
                            key,
                            contract,
                            state,
                        }) => Ok(ContractResponse::GetResponse {
                            key,
                            contract,
                            state,
                        }
                        .into()),
                        other @ (HostResponse::ContractResponse(_)
                        | HostResponse::DelegateResponse { .. }
                        | HostResponse::QueryResponse(_)
                        | HostResponse::Ok
                        | _) => Ok(other),
                    }
                }
                Err(err) => {
                    tracing::debug!(response = %err, cli_id = %id, "sending response error");
                    Err(err)
                }
            };
            // Log when UPDATE response is about to be sent over WebSocket
            let is_update_response = match &result {
                Ok(HostResponse::ContractResponse(ContractResponse::UpdateResponse {
                    key,
                    ..
                })) => {
                    tracing::debug!(
                        client_id = %client_id,
                        contract = %key,
                        phase = "serializing",
                        "Serializing UpdateResponse for WebSocket delivery"
                    );
                    Some(*key)
                }
                _ => None,
            };

            let stream_content = extract_stream_content(&result);

            let serialized_res = match encoding_protoc {
                EncodingProtocol::Flatbuffers => match result {
                    Ok(res) => res.into_fbs_bytes()?,
                    Err(err) => err.into_fbs_bytes()?,
                },
                EncodingProtocol::Native => bincode::serialize(&result)?,
            };

            // Log serialization completion for UPDATE responses
            if let Some(key) = is_update_response {
                tracing::debug!(
                    client_id = %client_id,
                    contract = %key,
                    size_bytes = serialized_res.len(),
                    phase = "serialized",
                    "Serialized UpdateResponse for WebSocket delivery"
                );
            }

            let send_result =
                send_response_message(tx, serialized_res, conn_state, stream_content).await;

            // Log WebSocket send result for UPDATE responses
            if let Some(key) = is_update_response {
                match &send_result {
                    Ok(()) => {
                        tracing::debug!(
                            client_id = %client_id,
                            contract = %key,
                            phase = "sent",
                            "Successfully sent UpdateResponse over WebSocket"
                        );
                    }
                    Err(err) => {
                        tracing::error!(
                            client_id = %client_id,
                            contract = %key,
                            error = ?err,
                            phase = "error",
                            "Failed to send UpdateResponse over WebSocket"
                        );
                    }
                }
            }

            send_result?;
            Ok(None)
        }
        Some(HostCallbackResult::SubscriptionChannel { key, id, callback }) => {
            debug_assert_eq!(id, client_id);
            Ok(Some(NewSubscription { key, callback }))
        }
        Some(HostCallbackResult::NewId { id: cli_id }) => {
            tracing::debug!(%cli_id, "new client registered");
            Ok(None)
        }
        None => {
            let error: ClientError = ErrorKind::NodeUnavailable.into();
            let result_error = match encoding_protoc {
                EncodingProtocol::Flatbuffers => error.into_fbs_bytes()?,
                EncodingProtocol::Native => {
                    bincode::serialize(&Err::<HostResponse, ClientError>(error))?
                }
            };
            send_response_message(tx, result_error, conn_state, None).await?;
            tx.send(Message::Close(None)).await?;
            tracing::warn!(
                client_id = %client_id,
                "Node shut down while handling responses"
            );
            Err(anyhow::anyhow!(
                "node shut down while handling responses for client {}",
                client_id
            ))
        }
    }
}

impl ClientEventsProxy for WebSocketProxy {
    fn recv(&mut self) -> BoxFuture<'_, Result<OpenRequest<'static>, ClientError>> {
        async move {
            loop {
                let msg = self.proxy_server_request.recv().await;
                if let Some(msg) = msg {
                    if let Some(reply) = self.internal_proxy_recv(msg).await? {
                        break Ok(reply.into_owned());
                    }
                } else {
                    break Err(ClientError::from(ErrorKind::ChannelClosed));
                }
            }
        }
        .boxed()
    }

    fn send(
        &mut self,
        id: ClientId,
        result: Result<HostResponse, ClientError>,
    ) -> BoxFuture<'_, Result<(), ClientError>> {
        async move {
            // Log UPDATE responses specifically
            match &result {
                Ok(HostResponse::ContractResponse(
                    freenet_stdlib::client_api::ContractResponse::UpdateResponse { key, summary },
                )) => {
                    tracing::debug!(
                        client_id = %id,
                        contract = %key,
                        summary_size = summary.size(),
                        "WebSocket send() called with UpdateResponse"
                    );
                }
                Ok(other_response) => {
                    tracing::debug!(
                        client_id = %id,
                        response = ?other_response,
                        "WebSocket send() called with response"
                    );
                }
                Err(error) => {
                    tracing::debug!(
                        client_id = %id,
                        error = ?error,
                        "WebSocket send() called with error"
                    );
                }
            }

            if let Some(ch) = self.response_channels.remove(&id) {
                // Log success/failure of sending UPDATE responses
                if let Ok(HostResponse::ContractResponse(
                    freenet_stdlib::client_api::ContractResponse::UpdateResponse { key, .. },
                )) = &result
                {
                    tracing::debug!(
                        client_id = %id,
                        contract = %key,
                        "Found WebSocket channel, sending UpdateResponse"
                    );
                }

                // Check if this is an UPDATE response and extract key before moving result
                let update_key = match &result {
                    Ok(HostResponse::ContractResponse(
                        freenet_stdlib::client_api::ContractResponse::UpdateResponse {
                            key, ..
                        },
                    )) => Some(*key),
                    _ => None,
                };

                let should_rm = result
                    .as_ref()
                    .map_err(|err| matches!(err.kind(), ErrorKind::Disconnect))
                    .err()
                    .unwrap_or(false);

                let send_result = ch.send(HostCallbackResult::Result { id, result });

                // Log UPDATE response send result
                if let Some(key) = update_key {
                    match send_result.is_ok() {
                        true => {
                            tracing::debug!(
                                client_id = %id,
                                contract = %key,
                                phase = "sent",
                                "Successfully sent UpdateResponse to client"
                            );
                        }
                        false => {
                            tracing::error!(
                                client_id = %id,
                                contract = %key,
                                phase = "error",
                                "Failed to send UpdateResponse - channel send failed"
                            );
                        }
                    }
                }

                if send_result.is_ok() && !should_rm {
                    // still alive connection, keep it
                    self.response_channels.insert(id, ch);
                } else {
                    tracing::info!(
                        client_id = %id,
                        "Dropped connection to client"
                    );
                }
            } else {
                // Log when client is not found for UPDATE responses
                match &result {
                    Ok(HostResponse::ContractResponse(
                        freenet_stdlib::client_api::ContractResponse::UpdateResponse {
                            key, ..
                        },
                    )) => {
                        tracing::error!(
                            client_id = %id,
                            contract = %key,
                            "Client not found in WebSocket response channels for UpdateResponse"
                        );
                    }
                    _ => {
                        tracing::warn!(
                            client_id = %id,
                            "Client not found in response channels"
                        );
                    }
                }
            }
            Ok(())
        }
        .boxed()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use freenet_stdlib::client_api::streaming::{
        chunk_request, ReassemblyBuffer, CHUNK_SIZE, CHUNK_THRESHOLD,
    };

    fn test_conn_state(encoding: EncodingProtocol) -> ConnectionState {
        ConnectionState {
            encoding_protoc: encoding,
            reassembly: ReassemblyBuffer::new(),
            next_stream_id: 0,
        }
    }

    #[test]
    fn test_is_client_disconnect_error() {
        // WebSocket protocol errors
        assert!(is_client_disconnect_error(&anyhow::anyhow!(
            "Connection reset without closing handshake"
        )));
        assert!(is_client_disconnect_error(&anyhow::anyhow!(
            "WebSocket protocol error: Connection reset without closing handshake"
        )));
        assert!(is_client_disconnect_error(&anyhow::anyhow!(
            "Connection closed without performing the closing handshake"
        )));

        // IO errors
        assert!(is_client_disconnect_error(&anyhow::anyhow!(
            "IO error: connection reset by peer"
        )));
        assert!(is_client_disconnect_error(&anyhow::anyhow!(
            "IO error: connection aborted"
        )));
        assert!(is_client_disconnect_error(&anyhow::anyhow!("broken pipe")));
        assert!(is_client_disconnect_error(&anyhow::anyhow!(
            "connection refused"
        )));
        assert!(is_client_disconnect_error(&anyhow::anyhow!(
            "not connected"
        )));

        // Case insensitivity
        assert!(is_client_disconnect_error(&anyhow::anyhow!(
            "CONNECTION RESET BY PEER"
        )));

        // Non-disconnect errors should return false
        assert!(!is_client_disconnect_error(&anyhow::anyhow!(
            "invalid WebSocket handshake"
        )));
        assert!(!is_client_disconnect_error(&anyhow::anyhow!(
            "failed to parse message"
        )));
        assert!(!is_client_disconnect_error(&anyhow::anyhow!(
            "permission denied"
        )));
    }

    #[test]
    fn test_is_localhost_origin() {
        // Valid localhost origins
        assert!(is_localhost_origin("http://localhost"));
        assert!(is_localhost_origin("https://localhost"));
        assert!(is_localhost_origin("http://localhost:3000"));
        assert!(is_localhost_origin("https://localhost:8080"));
        assert!(is_localhost_origin("http://localhost/path"));

        // Valid 127.0.0.1 origins
        assert!(is_localhost_origin("http://127.0.0.1"));
        assert!(is_localhost_origin("https://127.0.0.1"));
        assert!(is_localhost_origin("http://127.0.0.1:50509"));
        assert!(is_localhost_origin("http://127.0.0.1/path"));

        // Valid IPv6 loopback origins
        assert!(is_localhost_origin("http://[::1]"));
        assert!(is_localhost_origin("https://[::1]"));
        assert!(is_localhost_origin("http://[::1]:3000"));
        assert!(is_localhost_origin("http://[::1]/path"));

        // Reject external origins
        assert!(!is_localhost_origin("http://evil.com"));
        assert!(!is_localhost_origin("https://attacker.io:8080"));

        // Reject hostname spoofing (delimiter-aware)
        assert!(!is_localhost_origin("http://localhost.evil.com"));
        assert!(!is_localhost_origin("http://127.0.0.1.evil.com"));

        // Reject empty/garbage
        assert!(!is_localhost_origin(""));
        assert!(!is_localhost_origin("localhost"));
    }

    #[test]
    fn test_is_allowed_host() {
        let allowed: HashSet<String> = [
            "localhost",
            "localhost:7509",
            "127.0.0.1",
            "127.0.0.1:7509",
            "[::1]",
            "[::1]:7509",
            "myhost",
            "myhost:7509",
            "192.168.1.50",
            "192.168.1.50:7509",
            "mynode.example.com",
            "mynode.example.com:7509",
        ]
        .iter()
        .map(|s| s.to_string())
        .collect();

        fn headers_with_host(host: &str) -> axum::http::HeaderMap {
            let mut map = axum::http::HeaderMap::new();
            map.insert(
                axum::http::header::HOST,
                axum::http::HeaderValue::from_str(host).unwrap(),
            );
            map
        }

        // Allowed: host with port
        assert!(is_allowed_host(
            &headers_with_host("192.168.1.50:7509"),
            &allowed
        ));
        assert!(is_allowed_host(
            &headers_with_host("mynode.example.com:7509"),
            &allowed
        ));
        assert!(is_allowed_host(
            &headers_with_host("localhost:7509"),
            &allowed
        ));

        // Allowed: host without port
        assert!(is_allowed_host(&headers_with_host("localhost"), &allowed));
        assert!(is_allowed_host(
            &headers_with_host("192.168.1.50"),
            &allowed
        ));

        // Rejected: DNS rebinding
        assert!(!is_allowed_host(
            &headers_with_host("evil.com:7509"),
            &allowed
        ));
        assert!(!is_allowed_host(&headers_with_host("evil.com"), &allowed));

        // Rejected: no Host header
        assert!(!is_allowed_host(&axum::http::HeaderMap::new(), &allowed));

        // Allowed: case insensitive
        assert!(is_allowed_host(&headers_with_host("MyHost:7509"), &allowed));
        assert!(is_allowed_host(
            &headers_with_host("LOCALHOST:7509"),
            &allowed
        ));
    }

    #[test]
    fn test_to_key_array_valid() {
        let key = [42u8; 32];
        assert_eq!(to_key_array(&key), Some(key));
    }

    #[test]
    fn test_to_key_array_wrong_length() {
        assert_eq!(to_key_array(&[0u8; 31]), None);
        assert_eq!(to_key_array(&[0u8; 33]), None);
        assert_eq!(to_key_array(&[]), None);
    }

    #[test]
    fn test_rate_limiter_no_backoff_initially() {
        let limiter = DelegateRateLimiter::new();
        let key = [1u8; 32];
        assert!(limiter.check_backoff(&key).is_none());
    }

    #[test]
    fn test_rate_limiter_backoff_after_error() {
        let mut limiter = DelegateRateLimiter::new();
        let key = [1u8; 32];

        // No backoff before any errors
        assert!(limiter.check_backoff(&key).is_none());

        // Record a failure
        limiter.record_error(&key);

        // Now should be in backoff
        let remaining = limiter.check_backoff(&key);
        assert!(remaining.is_some());
        assert!(remaining.unwrap() > Duration::ZERO);
    }

    #[test]
    fn test_rate_limiter_success_clears_backoff() {
        let mut limiter = DelegateRateLimiter::new();
        let key = [1u8; 32];

        limiter.record_error(&key);
        assert!(limiter.check_backoff(&key).is_some());

        limiter.record_success(&key);
        assert!(limiter.check_backoff(&key).is_none());
    }

    #[test]
    fn test_rate_limiter_independent_keys() {
        let mut limiter = DelegateRateLimiter::new();
        let key_a = [1u8; 32];
        let key_b = [2u8; 32];

        limiter.record_error(&key_a);

        // key_a is in backoff, key_b is not
        assert!(limiter.check_backoff(&key_a).is_some());
        assert!(limiter.check_backoff(&key_b).is_none());

        // Success on key_a doesn't affect key_b
        limiter.record_error(&key_b);
        limiter.record_success(&key_a);
        assert!(limiter.check_backoff(&key_a).is_none());
        assert!(limiter.check_backoff(&key_b).is_some());
    }

    #[test]
    fn test_rate_limiter_escalating_backoff() {
        let mut limiter = DelegateRateLimiter::new();
        let key = [1u8; 32];

        // First failure
        limiter.record_error(&key);
        let first_backoff = limiter.check_backoff(&key).unwrap();

        // Record success then failure again to reset and measure independently
        limiter.record_success(&key);

        // Two consecutive failures should produce longer backoff
        limiter.record_error(&key);
        limiter.record_error(&key);
        let second_backoff = limiter.check_backoff(&key).unwrap();

        // Second backoff should be longer (accounting for jitter)
        // With base=100ms: 1 failure ≈ 100ms, 2 failures ≈ 200ms
        // Even with ±20% jitter, 200ms*0.8 > 100ms*1.2 doesn't always hold,
        // but the underlying failure count should increase
        assert!(second_backoff > Duration::ZERO);
        assert!(first_backoff > Duration::ZERO);
    }

    #[test]
    fn test_delegate_error_key_extraction() {
        use freenet_stdlib::prelude::DelegateKey;

        let code_hash = freenet_stdlib::prelude::CodeHash::new([0u8; 32]);
        let delegate_key = DelegateKey::new([42u8; 32], code_hash);

        // Missing variant carries the key
        let err = DelegateError::Missing(delegate_key.clone());
        let extracted = delegate_error_key(&err);
        assert!(extracted.is_some());
        assert_eq!(extracted.unwrap(), &[42u8; 32]);

        // RegisterError variant carries the key
        let err = DelegateError::RegisterError(delegate_key.clone());
        assert!(delegate_error_key(&err).is_some());

        // MissingSecret variant carries the key
        let err = DelegateError::MissingSecret {
            key: delegate_key,
            secret: freenet_stdlib::prelude::SecretsId::new(b"test".to_vec()),
        };
        assert!(delegate_error_key(&err).is_some());

        // ExecutionError does NOT carry a key
        let err = DelegateError::ExecutionError("test error".into());
        assert!(delegate_error_key(&err).is_none());
    }

    // =========================================================================
    // WebSocket streaming unit tests
    // =========================================================================

    #[test]
    fn above_threshold_always_chunked() {
        // Streaming is always enabled — any payload above CHUNK_THRESHOLD
        // must be chunked regardless of client request parameters.
        let payload = vec![0xAB; CHUNK_THRESHOLD + 100];
        let mut conn = test_conn_state(EncodingProtocol::Native);

        let messages = prepare_response_messages(payload.clone(), &mut conn, None).unwrap();

        assert!(
            messages.len() > 1,
            "payload above threshold must be chunked"
        );
        assert_eq!(conn.next_stream_id, 1, "stream_id must advance");

        // Verify round-trip reassembly
        let mut buf = ReassemblyBuffer::new();
        let mut result = None;
        for msg_bytes in &messages {
            let resp: Result<HostResponse, ClientError> = bincode::deserialize(msg_bytes).unwrap();
            if let Ok(HostResponse::StreamChunk {
                stream_id,
                index,
                total,
                data,
            }) = resp
            {
                if let Ok(Some(complete)) = buf.receive_chunk(stream_id, index, total, data) {
                    result = Some(complete);
                }
            }
        }
        assert_eq!(&result.unwrap()[..], &payload[..]);
    }

    #[test]
    fn payload_at_threshold_not_chunked() {
        let payload = vec![0xCC; CHUNK_THRESHOLD];
        let mut conn = test_conn_state(EncodingProtocol::Native);

        let messages = prepare_response_messages(payload.clone(), &mut conn, None).unwrap();
        assert_eq!(
            messages.len(),
            1,
            "payload exactly at CHUNK_THRESHOLD must not be chunked"
        );
        assert_eq!(conn.next_stream_id, 0);
    }

    #[test]
    fn payload_below_threshold_not_chunked() {
        let payload = vec![0xCC; CHUNK_THRESHOLD - 1];
        let mut conn = test_conn_state(EncodingProtocol::Native);

        let messages = prepare_response_messages(payload, &mut conn, None).unwrap();
        assert_eq!(messages.len(), 1);
    }

    #[test]
    fn payload_above_threshold_is_chunked_and_reassembles() {
        let payload = vec![0xCC; CHUNK_THRESHOLD + 1];
        let mut conn = test_conn_state(EncodingProtocol::Native);

        let messages = prepare_response_messages(payload.clone(), &mut conn, None).unwrap();
        assert!(messages.len() > 1, "must produce multiple chunks");
        assert_eq!(conn.next_stream_id, 1);

        // Reassemble and verify round-trip
        let mut buf = ReassemblyBuffer::new();
        let mut result = None;
        for msg_bytes in &messages {
            let resp: Result<HostResponse, ClientError> = bincode::deserialize(msg_bytes).unwrap();
            if let Ok(HostResponse::StreamChunk {
                stream_id,
                index,
                total,
                data,
            }) = resp
            {
                if let Some(r) = buf.receive_chunk(stream_id, index, total, data).unwrap() {
                    result = Some(r);
                }
            }
        }
        assert_eq!(result.unwrap(), payload);
    }

    // --- Test 3: Flatbuffers encoding ---

    #[test]
    fn flatbuffers_chunked_serialization_succeeds() {
        let payload = vec![0xDD; CHUNK_THRESHOLD + 100];
        let mut conn = test_conn_state(EncodingProtocol::Flatbuffers);

        let messages = prepare_response_messages(payload.clone(), &mut conn, None).unwrap();
        assert!(
            messages.len() > 1,
            "must produce multiple FBS-encoded chunks"
        );
        assert_eq!(conn.next_stream_id, 1);

        // Each message is the output of HostResponse::StreamChunk::into_fbs_bytes()
        // which succeeded (no panic). Verify the total data across all chunks
        // accounts for the original payload.
        let total_fbs_bytes: usize = messages.iter().map(|m| m.len()).sum();
        assert!(
            total_fbs_bytes > payload.len(),
            "FBS-encoded chunks must contain at least as much data as the payload \
                 (plus FBS overhead)"
        );
    }

    #[test]
    fn flatbuffers_no_stream_header_even_with_content() {
        // When stream_content is Some, Flatbuffers path should still NOT
        // produce a StreamHeader (it's Native-only).
        let payload = vec![0xDD; CHUNK_THRESHOLD + 100];
        let mut conn = test_conn_state(EncodingProtocol::Flatbuffers);
        let content = freenet_stdlib::client_api::StreamContent::Raw;

        let messages_with_content =
            prepare_response_messages(payload.clone(), &mut conn, Some(content)).unwrap();

        let mut conn2 = test_conn_state(EncodingProtocol::Flatbuffers);
        let messages_without_content =
            prepare_response_messages(payload, &mut conn2, None).unwrap();

        // Same number of messages: no extra StreamHeader was added
        assert_eq!(
            messages_with_content.len(),
            messages_without_content.len(),
            "Flatbuffers must not add StreamHeader regardless of stream_content"
        );
    }

    #[test]
    fn server_reassembles_chunked_request() {
        let payload = vec![0xFF; CHUNK_SIZE * 3];
        let chunks = chunk_request(payload.clone(), 42);
        assert_eq!(chunks.len(), 3);

        let mut buf = ReassemblyBuffer::new();
        let mut result = None;
        for chunk in &chunks {
            if let ClientRequest::StreamChunk {
                stream_id,
                index,
                total,
                data,
            } = chunk
            {
                if let Some(r) = buf
                    .receive_chunk(*stream_id, *index, *total, data.clone())
                    .unwrap()
                {
                    result = Some(r);
                }
            }
        }
        assert_eq!(result.unwrap(), payload);
    }

    #[test]
    fn server_reassembly_out_of_order() {
        let payload: Vec<u8> = (0..CHUNK_SIZE * 3).map(|i| (i % 256) as u8).collect();
        let chunks = chunk_request(payload.clone(), 7);

        let mut buf = ReassemblyBuffer::new();
        let order = [2, 0, 1];
        let mut result = None;
        for &i in &order {
            if let ClientRequest::StreamChunk {
                stream_id,
                index,
                total,
                data,
            } = &chunks[i]
            {
                if let Some(r) = buf
                    .receive_chunk(*stream_id, *index, *total, data.clone())
                    .unwrap()
                {
                    result = Some(r);
                }
            }
        }
        assert_eq!(result.unwrap(), payload);
    }

    #[test]
    fn server_reassembly_interleaved_streams() {
        let payload_a = vec![0xAA; CHUNK_SIZE * 2];
        let payload_b = vec![0xBB; CHUNK_SIZE * 3];
        let chunks_a = chunk_request(payload_a.clone(), 1);
        let chunks_b = chunk_request(payload_b.clone(), 2);

        let mut buf = ReassemblyBuffer::new();
        let mut result_a = None;
        let mut result_b = None;

        // Interleave: A[0], B[0], B[1], A[1], B[2]
        let interleaved = [
            &chunks_a[0],
            &chunks_b[0],
            &chunks_b[1],
            &chunks_a[1],
            &chunks_b[2],
        ];
        for chunk in interleaved {
            if let ClientRequest::StreamChunk {
                stream_id,
                index,
                total,
                data,
            } = chunk
            {
                if let Some(r) = buf
                    .receive_chunk(*stream_id, *index, *total, data.clone())
                    .unwrap()
                {
                    match *stream_id {
                        1 => result_a = Some(r),
                        _ => result_b = Some(r),
                    }
                }
            }
        }
        assert_eq!(result_a.unwrap(), payload_a);
        assert_eq!(result_b.unwrap(), payload_b);
    }

    // --- Test 5: ReassemblyBuffer bounds ---

    #[test]
    fn reassembly_rejects_too_many_concurrent_streams() {
        use freenet_stdlib::client_api::streaming::{StreamError, MAX_CONCURRENT_STREAMS};

        let mut buf = ReassemblyBuffer::new();
        for i in 0..MAX_CONCURRENT_STREAMS as u32 {
            buf.receive_chunk(i, 0, 2, bytes::Bytes::from_static(&[0xAA]))
                .expect("within limit");
        }
        let err = buf
            .receive_chunk(
                MAX_CONCURRENT_STREAMS as u32,
                0,
                2,
                bytes::Bytes::from_static(&[0xBB]),
            )
            .unwrap_err();
        assert!(matches!(err, StreamError::TooManyConcurrentStreams { .. }));
    }

    #[test]
    fn reassembly_rejects_oversized_total_chunks() {
        use freenet_stdlib::client_api::streaming::{StreamError, MAX_TOTAL_CHUNKS};

        let mut buf = ReassemblyBuffer::new();
        let err = buf
            .receive_chunk(
                1,
                0,
                MAX_TOTAL_CHUNKS + 1,
                bytes::Bytes::from_static(&[0xCC]),
            )
            .unwrap_err();
        assert!(matches!(err, StreamError::TotalChunksTooLarge { .. }));
    }

    #[test]
    fn reassembly_rejects_duplicate_chunk() {
        use freenet_stdlib::client_api::streaming::StreamError;

        let mut buf = ReassemblyBuffer::new();
        buf.receive_chunk(1, 0, 3, bytes::Bytes::from_static(&[0xDD]))
            .unwrap();
        let err = buf
            .receive_chunk(1, 0, 3, bytes::Bytes::from_static(&[0xEE]))
            .unwrap_err();
        assert!(matches!(
            err,
            StreamError::DuplicateChunk {
                stream_id: 1,
                index: 0
            }
        ));
    }

    // --- Bonus: StreamHeader and stream_id behavior ---

    #[test]
    fn native_sends_stream_header_before_chunks() {
        let payload = vec![0xEE; CHUNK_THRESHOLD + 100];
        let payload_len = payload.len();
        let mut conn = test_conn_state(EncodingProtocol::Native);
        let content = freenet_stdlib::client_api::StreamContent::Raw;

        let messages = prepare_response_messages(payload, &mut conn, Some(content)).unwrap();
        assert!(messages.len() >= 2);

        let first: Result<HostResponse, ClientError> = bincode::deserialize(&messages[0]).unwrap();
        match first {
            Ok(HostResponse::StreamHeader {
                stream_id,
                total_bytes,
                ..
            }) => {
                assert_eq!(stream_id, 0);
                assert_eq!(total_bytes, payload_len as u64);
            }
            other => panic!("expected StreamHeader, got: {other:?}"),
        }

        for msg in &messages[1..] {
            let resp: Result<HostResponse, ClientError> = bincode::deserialize(msg).unwrap();
            assert!(matches!(resp, Ok(HostResponse::StreamChunk { .. })));
        }
    }

    #[test]
    fn stream_id_advances_only_for_chunked_sends() {
        let large = vec![0xFF; CHUNK_THRESHOLD + 1];
        let small = vec![0x00; 100];
        let mut conn = test_conn_state(EncodingProtocol::Native);

        assert_eq!(conn.next_stream_id, 0);
        prepare_response_messages(large.clone(), &mut conn, None).unwrap();
        assert_eq!(conn.next_stream_id, 1);
        prepare_response_messages(large, &mut conn, None).unwrap();
        assert_eq!(conn.next_stream_id, 2);
        prepare_response_messages(small, &mut conn, None).unwrap();
        assert_eq!(
            conn.next_stream_id, 2,
            "non-chunked must not advance stream_id"
        );
    }

    // --- NodeUnavailable encoding bugfix test ---

    #[test]
    fn node_unavailable_native_serializes_as_bincode() {
        let error: ClientError = ErrorKind::NodeUnavailable.into();
        let bytes = bincode::serialize(&Err::<HostResponse, ClientError>(error)).unwrap();
        // Must round-trip through bincode
        let result: Result<HostResponse, ClientError> = bincode::deserialize(&bytes).unwrap();
        assert!(result.is_err());
        assert!(
            matches!(result.unwrap_err().kind(), ErrorKind::NodeUnavailable),
            "expected NodeUnavailable error kind"
        );
    }

    #[test]
    fn node_unavailable_flatbuffers_serializes_as_fbs() {
        let error: ClientError = ErrorKind::NodeUnavailable.into();
        let fbs_bytes = error
            .into_fbs_bytes()
            .expect("FBS serialization must succeed");
        // Verify it's valid FBS by attempting to decode
        assert!(!fbs_bytes.is_empty(), "FBS bytes must not be empty");
        // bincode deserialization must fail — this ensures the fix actually
        // dispatches to FBS encoding rather than bincode
        let bincode_result: Result<Result<HostResponse, ClientError>, _> =
            bincode::deserialize(&fbs_bytes);
        assert!(
            bincode_result.is_err(),
            "FBS-encoded error must not be valid bincode"
        );
    }

    // --- Inbound StreamChunk reassembly test ---

    #[test]
    fn server_reassembles_inbound_stream_chunks() {
        // The server must reassemble inbound StreamChunks from clients because
        // freenet-stdlib 0.2.2+ always chunks large requests client-side.
        let payload = vec![0xFF; CHUNK_SIZE * 3];
        let chunks = chunk_request(payload.clone(), 42);
        assert_eq!(chunks.len(), 3);

        let mut conn = test_conn_state(EncodingProtocol::Native);
        let mut result = None;
        for chunk in &chunks {
            if let ClientRequest::StreamChunk {
                stream_id,
                index,
                total,
                data,
            } = chunk
            {
                if let Some(r) = conn
                    .reassembly
                    .receive_chunk(*stream_id, *index, *total, data.clone())
                    .unwrap()
                {
                    result = Some(r);
                }
            }
        }
        assert_eq!(
            result.unwrap(),
            payload,
            "server must reassemble inbound StreamChunks"
        );
    }

    // --- FBS chunked serialization validation ---

    #[test]
    fn flatbuffers_chunked_produces_valid_distinct_messages() {
        // Verify that FBS-encoded chunks produce distinct, non-empty messages
        // with total size exceeding the original payload (FBS overhead).
        // Full round-trip deserialization of HostResponse FBS is not available
        // server-side (FBS decoding is implemented on the client/browser side).
        let payload = vec![0xDD; CHUNK_THRESHOLD + 100];
        let mut conn = test_conn_state(EncodingProtocol::Flatbuffers);

        let messages = prepare_response_messages(payload.clone(), &mut conn, None).unwrap();
        assert!(
            messages.len() > 1,
            "must produce multiple FBS-encoded chunks"
        );

        for msg in &messages {
            assert!(!msg.is_empty(), "FBS chunk must not be empty");
        }

        let total_fbs_bytes: usize = messages.iter().map(|m| m.len()).sum();
        assert!(
            total_fbs_bytes > payload.len(),
            "FBS chunks ({total_fbs_bytes}) must exceed raw payload ({})",
            payload.len()
        );

        // Verify the messages are NOT valid bincode (proves FBS encoding was used)
        let bincode_result: Result<Result<HostResponse, ClientError>, _> =
            bincode::deserialize(&messages[0]);
        assert!(
            bincode_result.is_err(),
            "FBS-encoded message must not be valid bincode"
        );
    }

    // --- stream_id wrapping test ---

    #[test]
    fn stream_id_wraps_at_u32_max() {
        let large = vec![0xFF; CHUNK_THRESHOLD + 1];
        let mut conn = test_conn_state(EncodingProtocol::Native);
        conn.next_stream_id = u32::MAX;

        let messages = prepare_response_messages(large, &mut conn, None).unwrap();
        assert!(messages.len() > 1, "must produce chunks");
        assert_eq!(
            conn.next_stream_id, 0,
            "stream_id must wrap from u32::MAX to 0"
        );
    }

    // --- extract_stream_content tests ---

    #[test]
    fn extract_stream_content_returns_some_for_get_response() {
        use freenet_stdlib::prelude::{ContractCode, Parameters, WrappedState};

        let code = ContractCode::from(vec![1, 2, 3]);
        let key = freenet_stdlib::prelude::ContractKey::from_params_and_code(
            Parameters::from(vec![]),
            &code,
        );
        let result: Result<HostResponse, ClientError> = Ok(HostResponse::ContractResponse(
            ContractResponse::GetResponse {
                key,
                contract: None,
                state: WrappedState::new(vec![0; 100]),
            },
        ));
        let content = extract_stream_content(&result);
        assert!(content.is_some(), "GetResponse must produce StreamContent");
    }

    #[test]
    fn extract_stream_content_returns_none_for_other_responses() {
        let result: Result<HostResponse, ClientError> = Ok(HostResponse::Ok);
        assert!(extract_stream_content(&result).is_none());

        let result: Result<HostResponse, ClientError> = Err(ErrorKind::NodeUnavailable.into());
        assert!(extract_stream_content(&result).is_none());
    }
}
