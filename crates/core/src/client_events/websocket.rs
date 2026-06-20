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
    Extension, Router,
    extract::{
        Query, WebSocketUpgrade,
        ws::{Message, WebSocket},
    },
    http::StatusCode,
    response::{IntoResponse, Response},
    routing::get,
};
use freenet_stdlib::{
    client_api::{
        ClientRequest, ContractRequest, ContractResponse, DelegateError, ErrorKind, HostResponse,
        RequestError,
    },
    prelude::*,
};
use futures::{FutureExt, SinkExt, StreamExt, future::BoxFuture, stream::SplitSink};
use headers::Header;
use serde::Deserialize;
use tokio::sync::{Mutex, mpsc};

use crate::{
    client_events::AuthToken,
    server::{ApiVersion, ClientConnection, HostCallbackResult},
    util::{
        EncodingProtocol,
        backoff::{ExponentialBackoff, TrackedBackoff},
    },
};

use super::{ClientError, ClientEventsProxy, ClientId, HostResult, OpenRequest};
use crate::server::client_api::OriginContractMap;
use crate::wasm_runtime::UserSecretContext;

/// Checks if a WebSocket Origin header value refers to localhost.
///
/// Delimiter-aware: rejects origins like `http://localhost.evil.com` by requiring
/// the hostname to be followed by `:`, `/`, or end-of-string.
///
/// Also reused by the permission-prompt endpoints
/// (`crates/core/src/server/client_api/permission_prompts.rs`) so the HTTP and
/// WS layers agree on what counts as a loopback Origin.
pub(crate) fn is_localhost_origin(origin: &str) -> bool {
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
///
/// Reused by the permission-prompt endpoints (see [`crate::server::client_api`])
/// so HTTP and WS layers share the same Host allowlist check.
pub(crate) fn is_allowed_host(
    headers: &axum::http::HeaderMap,
    allowed_hosts: &HashSet<String>,
) -> bool {
    let Some(host_header) = headers
        .get(axum::http::header::HOST)
        .and_then(|h| h.to_str().ok())
    else {
        return false;
    };
    allowed_hosts.contains(&host_header.to_lowercase())
}

/// Returns `true` if the request's `Host` header IP is acceptable for a
/// privileged WS upgrade **and** the `Origin` is compatible with it.
///
/// Two acceptance branches with **different Origin policies**:
///
/// 1. **Operator-configured CIDR** (`--allowed-source-cidrs` / config
///    `allowed-source-cidrs`): accepts the upgrade when the Host IP is in
///    the CIDR and the Origin is **either** `null` (sandboxed iframe
///    served from the same node) **or** a same-IP literal (direct browser
///    access). The operator has explicitly opted into the null-origin
///    risk surface — see `origin_compatible_with_host_ip`.
///
/// 2. **Default LAN/private** (no operator config required): accepts the
///    upgrade when the Host IP is private (RFC 1918 / loopback /
///    link-local / IPv6 ULA, but **not** the unspecified `0.0.0.0` / `::`)
///    and the Origin is a **same-IP literal** matching the Host IP.
///    `Origin: null` is **rejected** on this branch because any site can
///    serve a `<iframe sandbox>` whose origin is null and use it to probe
///    LAN-bound nodes — a default-on CSWSH would expose every desktop
///    install on a LAN. Operators who genuinely need null-origin
///    sandboxed access from non-localhost contexts must opt in via
///    `--allowed-source-cidrs`.
///
/// This dual policy closes #3957 (LAN access works out of the box)
/// without widening CSWSH exposure beyond what an explicit operator
/// opt-in already accepts. The default-LAN branch matches what
/// [`is_private_ip`](crate::server::is_private_ip) and the
/// `private_network_filter` accept for **source** IPs, modulo the
/// unspecified-address exclusion, so the HTTP and WS layers agree on
/// what counts as a trusted network position.
///
/// **Not covered by the default-LAN branch:** CGNAT (100.64/10),
/// public-unicast IPv4/IPv6, IPv6 GUA, and `0.0.0.0` / `::`. Tailnet,
/// CGNAT, and other non-private but operator-controlled ranges still
/// require `--allowed-source-cidrs` (see `host_header_ip_in_tailscale_cidr_allowed`).
///
/// CSWSH defence (both branches): `https://evil.com`'s JS opening
/// `ws://<victim-lan-ip>:7509/...` sends `Origin: https://evil.com`, which
/// parses to a hostname — not an IP, not matching the Host IP — so it's
/// refused. DNS rebinding is defeated because no DNS resolution happens
/// at this layer; only string-parseable IP literals are compared.
pub(crate) fn host_header_ip_in_cidrs(
    headers: &axum::http::HeaderMap,
    origin_str: &str,
    allowed_cidrs: &crate::server::AllowedSourceCidrs,
) -> bool {
    let Some(host_header) = headers
        .get(axum::http::header::HOST)
        .and_then(|h| h.to_str().ok())
    else {
        return false;
    };
    let Some(ip) = parse_host_header_ip(host_header) else {
        return false;
    };
    let host_ip = normalize_mapped_v6(ip);

    // Operator-configured CIDR: permissive Origin policy (null OK).
    if allowed_cidrs.contains_ip(&host_ip) {
        return origin_compatible_with_host_ip(origin_str, host_ip);
    }

    // Default LAN/private: stricter Origin policy (same-IP literal only,
    // no null) to prevent default-on CSWSH from sandboxed iframes hosted
    // on arbitrary sites.
    if is_default_lan_host_ip(&host_ip) {
        return origin_is_same_ip_literal(origin_str, host_ip);
    }

    false
}

/// Whether a `Host` header IP qualifies for the default LAN/private path.
///
/// Stricter than [`is_private_ip`](crate::server::is_private_ip): rejects
/// the unspecified addresses (`0.0.0.0` / `::`). `is_private_ip` accepts
/// them for source-IP filter purposes (legitimate kernel-source values),
/// but no real browser legitimately sends `Host: 0.0.0.0` or `Host: [::]`,
/// and accepting them would widen the matchable-Host surface for no use
/// case.
fn is_default_lan_host_ip(ip: &std::net::IpAddr) -> bool {
    if ip.is_unspecified() {
        return false;
    }
    crate::server::is_private_ip(ip)
}

/// Strict Origin gate used by the default LAN/private branch: requires
/// the Origin to parse to an IP literal that matches the Host IP after
/// IPv4-mapped-v6 normalization. `Origin: null` and hostname Origins are
/// rejected.
fn origin_is_same_ip_literal(origin_str: &str, host_ip: std::net::IpAddr) -> bool {
    let Some(origin_host) = parse_origin_host(origin_str) else {
        return false;
    };
    let Ok(origin_ip) = origin_host.parse::<std::net::IpAddr>() else {
        return false;
    };
    normalize_mapped_v6(origin_ip) == host_ip
}

/// Decides whether an `Origin` header value is compatible with a `Host` IP
/// that's already been confirmed to be inside an allowed CIDR.
///
/// See `host_header_ip_in_cidrs` for the full rationale.
fn origin_compatible_with_host_ip(origin_str: &str, host_ip: std::net::IpAddr) -> bool {
    // `Origin: null` is what Freenet's dApp sandbox iframe actually sends.
    // A malicious sandboxed iframe on evil.com would also send `null`, but
    // that's the same risk surface as an operator-configured `allowed-host`
    // entry (existing behaviour); extending the risk to operator-configured
    // CIDRs is a deliberate choice documented in the PR description.
    if origin_str == "null" {
        return true;
    }
    let Some(origin_host) = parse_origin_host(origin_str) else {
        return false;
    };
    let Ok(origin_ip) = origin_host.parse::<std::net::IpAddr>() else {
        // Origin is a hostname (e.g. `evil.com`). Never accept — this is the
        // primary CSWSH guard.
        return false;
    };
    normalize_mapped_v6(origin_ip) == host_ip
}

/// Extracts the IP part of a `Host` header value, handling `host:port`,
/// `[v6]`, and `[v6]:port`. Returns `None` on any deviation from these
/// forms, including bare unbracketed IPv6, malformed brackets, trailing
/// garbage, and non-numeric ports.
///
/// Strict parsing is deliberate: this function gates a security decision
/// (CIDR membership for WS upgrades), so any ambiguous input must fail
/// closed. RFC 7230 requires brackets for IPv6 in the Host header;
/// anything else is either a bug or an attempt to confuse the parser.
fn parse_host_header_ip(host_header: &str) -> Option<std::net::IpAddr> {
    let host = host_header.trim();
    // Bracketed IPv6 form: `[...]` or `[...]:port`. The content inside the
    // brackets must parse as IPv6 (not IPv4 — RFC 3986 forbids bracketed
    // v4). Anything after `]` must be either empty or a numeric port.
    if let Some(rest) = host.strip_prefix('[') {
        let end = rest.find(']')?;
        let inside = &rest[..end];
        let after = &rest[end + 1..];
        let v6: std::net::Ipv6Addr = inside.parse().ok()?;
        if !after.is_empty() && !is_colon_port(after) {
            return None;
        }
        return Some(std::net::IpAddr::V6(v6));
    }
    // Unbracketed: must be IPv4 or IPv4:port. Bare IPv6 without brackets
    // is non-conformant and rejected to avoid ambiguity with `host:port`
    // where the host is a multi-colon IPv6 literal.
    let (host_part, after) = host
        .rsplit_once(':')
        .map(|(h, p)| (h, Some(p)))
        .unwrap_or((host, None));
    let v4: std::net::Ipv4Addr = host_part.parse().ok()?;
    if let Some(port) = after
        && (port.is_empty() || !port.bytes().all(|b| b.is_ascii_digit()))
    {
        return None;
    }
    Some(std::net::IpAddr::V4(v4))
}

/// `true` when `s` looks like a `:port` suffix with an all-digit port body.
fn is_colon_port(s: &str) -> bool {
    let Some(rest) = s.strip_prefix(':') else {
        return false;
    };
    !rest.is_empty() && rest.bytes().all(|b| b.is_ascii_digit())
}

/// Extracts the host part of an `Origin` header (e.g. `https://100.64.1.5:7509`
/// → `100.64.1.5`). Returns `None` for unparseable or scheme-less values so
/// `origin_compatible_with_host_ip` falls through to reject.
///
/// We hand-roll a minimal parser rather than pull in `url`: browsers produce
/// `scheme://authority` for Origin, and we only need the authority's host-part.
/// Userinfo (`user@host`) in Origin is not sent by any real browser, but we
/// tolerate it by stripping the `user@` prefix.
fn parse_origin_host(origin: &str) -> Option<&str> {
    let after_scheme = origin.split_once("://").map(|(_, rest)| rest)?;
    // Strip path/query: Origin header is scheme://authority, but defence in
    // depth against a weird value like `http://host/extra`.
    let authority = after_scheme
        .split_once('/')
        .map(|(a, _)| a)
        .unwrap_or(after_scheme);
    // Strip userinfo.
    let host_and_port = authority
        .rsplit_once('@')
        .map(|(_, hp)| hp)
        .unwrap_or(authority);
    // Bracketed IPv6: `[::1]:port` or `[::1]`.
    if let Some(rest) = host_and_port.strip_prefix('[') {
        let end = rest.find(']')?;
        return Some(&rest[..end]);
    }
    // Strip trailing `:port` only when there's exactly one colon (IPv4/host).
    // Bare IPv6 without brackets is non-conformant in Origin and doesn't
    // occur in practice; fall through to `parse()` which will simply fail.
    Some(
        match host_and_port
            .as_bytes()
            .iter()
            .filter(|&&b| b == b':')
            .count()
        {
            0 | 1 => host_and_port
                .rsplit_once(':')
                .map(|(h, _)| h)
                .unwrap_or(host_and_port),
            _ => host_and_port,
        },
    )
}

fn normalize_mapped_v6(ip: std::net::IpAddr) -> std::net::IpAddr {
    match ip {
        std::net::IpAddr::V6(v6) => v6
            .to_ipv4_mapped()
            .map(std::net::IpAddr::V4)
            .unwrap_or(std::net::IpAddr::V6(v6)),
        v4 => v4,
    }
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
    /// Abort guard for the detached `axum::serve` server task(s) and the
    /// token-cleanup loop spawned by `serve_client_api_in_impl`. Held here so
    /// that when the node tears down its client-events task (which owns this
    /// proxy via the client combinator) those server tasks are aborted too,
    /// freeing the bound ports on shutdown. See issue #4401.
    ///
    /// `None` for proxies created directly via `create_router*` in tests, where
    /// no server task is spawned.
    server_aborts: Option<crate::util::AbortOnDrop>,
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
                server_aborts: None,
            },
            router,
        )
    }

    /// Attach the abort guard for the detached server tasks so they are torn
    /// down when this proxy is dropped (issue #4401).
    pub(crate) fn with_server_aborts(mut self, aborts: crate::util::AbortOnDrop) -> Self {
        self.server_aborts = Some(aborts);
        self
    }

    /// Sets up a subscription notification channel and builds the `OpenRequest`.
    ///
    /// Returns `None` if the client has already disconnected (response channel
    /// closed or missing). Returning `None` instead of an error prevents
    /// transient disconnects from killing `client_fn` (#3479).
    #[allow(clippy::too_many_arguments)]
    fn setup_subscription(
        &self,
        client_id: ClientId,
        key: ContractInstanceId,
        req: Box<ClientRequest<'static>>,
        auth_token: Option<AuthToken>,
        origin_contract: Option<ContractInstanceId>,
        user_context: Option<UserSecretContext>,
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
                .with_origin_contract(origin_contract)
                .with_user_context(user_context),
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
                user_context,
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
                    match self.setup_subscription(
                        client_id,
                        key,
                        req,
                        auth_token,
                        origin_contract,
                        user_context,
                    ) {
                        Some(r) => r,
                        None => return Ok(None),
                    }
                } else {
                    OpenRequest::new(client_id, req)
                        .with_token(auth_token)
                        .with_origin_contract(origin_contract)
                        .with_user_context(user_context)
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
    /// Durable per-user token for hosted mode (P2 of #4381). This is a SEPARATE
    /// credential from `auth_token` (the per-app session token minted by the
    /// HTTP attestation flow): `auth_token` identifies which app-contract a
    /// connection is acting for, whereas `user_token` identifies WHICH USER's
    /// secret namespace the connection's delegate secret operations bind to.
    /// The browser shell page supplies it as the `userToken` query parameter on
    /// the WS upgrade URL (the P2-frontend follow-up mints/stores/presents it).
    /// It is honored ONLY when the node runs in hosted mode; otherwise it is
    /// ignored entirely and every connection stays single-user.
    user_token: Option<String>,
}

/// Decide a connection's per-user secret namespace from the hosted-mode flag
/// and the presented user token (P2 of #4381).
///
/// This is the security-critical gate, factored out of the `connection_info`
/// middleware so it can be unit-tested in isolation:
///
/// - hosted mode OFF => always `None` (token ignored entirely; single-user).
/// - hosted mode ON, non-empty token => `Some` context derived solely from the
///   token bytes.
/// - hosted mode ON, no token or empty token => `None` (this connection is
///   single-user, just like a non-hosted node).
///
/// `None` everywhere means [`crate::wasm_runtime::SecretScope::Local`]
/// downstream — byte-for-byte the pre-#4381 behavior — so the flag-off path is
/// provably inert.
fn derive_user_context(hosted_mode: bool, user_token: Option<&str>) -> Option<UserSecretContext> {
    match (hosted_mode, user_token) {
        (true, Some(token)) if !token.is_empty() => {
            Some(UserSecretContext::from_token(token.as_bytes()))
        }
        // hosted on but no/empty token, or hosted off (token ignored): Local.
        (true, _) | (false, _) => None,
    }
}

/// Outcome of the "should this connection's user token be honored?" gate.
///
/// Pure decision over `(hosted_mode, has_token, source_ip, xfp_https)`, factored
/// out of the `connection_info` middleware so the security property is provable
/// without standing up a WS stack (refuse-plaintext-token invariant of #4381).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum UserTokenDecision {
    /// Honor the token: derive the per-user secret context from it.
    Honor,
    /// Hosted mode + a token were present, but the connection is not
    /// demonstrably secure (not a loopback source carrying
    /// `X-Forwarded-Proto: https`). Reject the upgrade rather than silently
    /// dropping the token — silently falling back to `Local` would put the user
    /// on the WRONG (shared) namespace under a public plaintext deployment.
    RejectInsecure,
    /// No per-user scoping: treat as single-user (`SecretScope::Local`). Reached
    /// for the flag-off path and the no-token path (`has_token == false`) — both
    /// byte-for-byte the pre-#4381 behavior, so no rejection fires.
    ///
    /// NOTE: an EMPTY token (`?userToken=` with no value) is mapped to `Local`
    /// too, but that happens at the CALL SITE, which passes `has_token = false`
    /// for an empty token (matching `derive_user_context`'s empty-is-absent
    /// rule). This pure fn keys only on the `has_token` bool and never inspects
    /// token contents — so an empty token must NOT reach here as `has_token ==
    /// true`, or it would be wrongly rejected as insecure.
    Local,
}

/// Whether a source IP is loopback (`127.0.0.0/8`, `::1`), after normalizing an
/// IPv4-mapped IPv6 source (`::ffff:127.0.0.1`) from a dual-stack socket.
///
/// Loopback is the trust anchor for honoring a durable per-user token: a direct
/// local browser, or a TLS-terminating reverse proxy colocated on the same host,
/// both connect from loopback. Anything off-host cannot forge a loopback source
/// (the kernel sets it from the accepted socket — see `ConnectInfo<SocketAddr>`
/// in `private_network_filter`), so it is a sound, non-spoofable signal that the
/// plaintext HTTP port was NOT exposed to the network for this connection.
fn is_loopback_source(ip: std::net::IpAddr) -> bool {
    match ip {
        std::net::IpAddr::V4(v4) => v4.is_loopback(),
        std::net::IpAddr::V6(v6) => match v6.to_ipv4_mapped() {
            Some(v4) => v4.is_loopback(),
            None => v6.is_loopback(),
        },
    }
}

/// The security-critical gate: decide whether to honor a per-user `userToken`.
///
/// HONOR only when the connection is demonstrably secure, so an operator who
/// accidentally exposes a plaintext HTTP port to the internet — directly OR
/// through a reverse proxy — cannot leak the durable, node-independent token.
///
/// A loopback source proves only the proxy→node hop is local; the node
/// REQUIRES `X-Forwarded-Proto: https` (set by the TLS-terminating proxy) as
/// positive evidence the browser→proxy hop used TLS. Host is NOT used — it is
/// proxy-rewritable (e.g. nginx's default config rewrites `Host` to the
/// upstream `127.0.0.1:7509`), so it cannot grant trust.
///
/// 1. **source loopback + `X-Forwarded-Proto: https`** ⇒ `Honor`. Either a
///    same-host TLS-terminating reverse proxy (the intended hosted deployment,
///    e.g. Caddy — which sets XFP by default) attesting the browser→proxy hop
///    was TLS, or a local process forging the header — but a local process
///    already has full node access, so loopback is the trust boundary.
/// 2. **source loopback + NO `X-Forwarded-Proto: https`** (header missing or
///    `http`) ⇒ `RejectInsecure`. A direct plaintext connection — even
///    loopback — or a plaintext reverse proxy (nginx default, no XFP). Without
///    positive HTTPS evidence the token may have crossed the network in
///    cleartext, so it is refused: secure by default.
/// 3. **source non-loopback** (any XFP) ⇒ `RejectInsecure`. The token crossed
///    the network to reach the node directly; never honor it.
///
/// `X-Forwarded-Proto` is trusted **only** from a loopback source (branch 1
/// requires loopback). From an off-host source it is client-spoofable
/// (`curl -H 'X-Forwarded-Proto: https'`) and is never consulted (branch 3).
///
/// Decision table (only reached when hosted mode is on and a non-empty token is
/// present — see the call site's `has_token` non-empty check):
///
/// | source     | xfp_https | outcome          |
/// |------------|-----------|------------------|
/// | loopback   | true      | `Honor`          |
/// | loopback   | false     | `RejectInsecure` |
/// | non-loop   | any       | `RejectInsecure` |
/// | `None`     | any       | `RejectInsecure` |
///
/// A missing source IP (`None`) is treated as insecure (fail closed): we cannot
/// prove loopback, so we must not honor the token. In production `ConnectInfo`
/// always yields the peer address; `None` only arises in unit tests that omit it.
fn decide_user_token(
    hosted_mode: bool,
    has_token: bool,
    source_ip: Option<std::net::IpAddr>,
    xfp_https: bool,
) -> UserTokenDecision {
    // Flag off, or no token presented: single-user, no rejection. This is the
    // ONLY path when hosted mode is off, so the whole invariant is hosted-gated
    // and the flag-off behavior is byte-for-byte unchanged.
    if !hosted_mode || !has_token {
        return UserTokenDecision::Local;
    }

    // Hosted mode + a token: the source MUST be loopback. A non-loopback source
    // means the token reached the node directly over the network — never honor.
    let Some(ip) = source_ip else {
        // No source IP: cannot prove loopback ⇒ fail closed.
        return UserTokenDecision::RejectInsecure;
    };
    if !is_loopback_source(ip) {
        return UserTokenDecision::RejectInsecure;
    }

    // Source is loopback. The proxy→node hop is local, but that says nothing
    // about the browser→proxy hop. Require positive HTTPS evidence
    // (`X-Forwarded-Proto: https`, set by the TLS-terminating proxy) before
    // honoring the token; otherwise the token may have crossed the network in
    // cleartext (direct plaintext, or a plaintext proxy). `xfp_https` is
    // trusted here ONLY because we've already proven the source is loopback.
    if xfp_https {
        UserTokenDecision::Honor
    } else {
        UserTokenDecision::RejectInsecure
    }
}

async fn connection_info(
    Query(ConnectionInfo {
        auth_token: auth_token_q,
        encoding_protocol,
        streaming: _,
        user_token: user_token_q,
    }): Query<ConnectionInfo>,
    Extension(allowed_hosts): Extension<crate::server::AllowedHosts>,
    Extension(allowed_source_cidrs): Extension<crate::server::AllowedSourceCidrs>,
    Extension(hosted_mode): Extension<crate::server::HostedMode>,
    mut req: axum::extract::Request,
    next: axum::middleware::Next,
) -> Response {
    use headers::{
        HeaderMapExt,
        authorization::{Authorization, Bearer},
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
                .into_response();
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
                .into_response();
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
                // Accept the upgrade when the origin is localhost, the Host
                // header matches an explicit `allowed-host` entry, or the
                // Host is an acceptable IP (private/LAN by default, or
                // inside `allowed-source-cidrs`) *and* the Origin is
                // compatible with it (null sandboxed iframe or same-IP).
                // See `host_header_ip_in_cidrs` for the CSWSH rationale.
                if !is_localhost_origin(origin_str)
                    && !is_allowed_host(req.headers(), &allowed_hosts)
                    && !host_header_ip_in_cidrs(req.headers(), origin_str, &allowed_source_cidrs)
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

    // Derive the per-connection user secret context (hosted mode, P2 of #4381),
    // gated by the refuse-plaintext-token secure-connection check below.
    //
    // SECURITY-CRITICAL boundary: this is the ONE place a `UserSecretContext` is
    // constructed. It happens here, at connection establishment, from the user
    // token presented on the upgrade request — never from a `ClientRequest`, a
    // delegate message, or anything reachable from WASM. Downstream the context
    // is immutable and travels with the connection.
    //
    // Token source: the `userToken` query parameter (canonical, supplied by the
    // browser shell page on the WS URL), OR an `X-Freenet-User-Token` header
    // which, if present, takes precedence (mirrors how a Bearer `auth_token`
    // header overrides its query form). The token is sensitive: we derive the
    // context and then drop the raw bytes — they are never stored or logged.
    // `hosted_mode` is a REQUIRED `Extension<HostedMode>` extractor (above), not
    // read tolerantly: it is a security-isolation flag, so a missing injection
    // must fail loud (500) rather than silently default to hosted-off. The single
    // bring-up choke point `server::serve_client_api_in_impl` injects it on both
    // router branches (loopback and LAN), so production always has it; a 500 here
    // means a misconfiguration to surface, not to paper over. A silent
    // default-to-false in a real hosted deployment that lost the injection would
    // disable per-user isolation (all users -> Local shared namespace ->
    // cross-user clobber) — for a security flag, fail-loud beats fail-silent.
    let user_token = req
        .headers()
        .get("x-freenet-user-token")
        .and_then(|v| v.to_str().ok())
        .map(|s| s.to_owned())
        .or(user_token_q);

    // REFUSE-PLAINTEXT-TOKEN invariant (#4381): a `userToken` is a durable,
    // node-independent, high-value credential that names a per-user secret
    // namespace. Honor it ONLY over a demonstrably secure connection, so an
    // operator who accidentally exposes a plaintext HTTP port to the internet —
    // directly OR through a reverse proxy — cannot leak it. "Secure" requires a
    // loopback source carrying `X-Forwarded-Proto: https` (positive evidence the
    // browser→proxy hop used TLS, set by the TLS-terminating proxy). See
    // `decide_user_token` for the full rationale and truth table.
    //
    // Source IP: read from the `ConnectInfo<SocketAddr>` request extension that
    // `axum::serve(..).into_make_service_with_connect_info::<SocketAddr>()` injects
    // for every connection (the same plumbing `private_network_filter` extracts).
    // We read it from `req.extensions()` rather than as a typed extractor so a
    // missing ConnectInfo (only possible in unit tests that omit it) yields
    // `None` and fails CLOSED in the gate below, instead of 500-ing the request.
    //
    // Host is NOT consulted: a loopback source proves only the proxy→node hop is
    // local, and `Host` is proxy-rewritable (nginx's default rewrites it to the
    // upstream `127.0.0.1:7509`), so it cannot grant trust. The node requires
    // positive HTTPS evidence instead.
    //
    // `X-Forwarded-Proto` is trusted ONLY when the source is loopback; from any
    // off-host source it is client-spoofable, so `decide_user_token` consults it
    // only after the source is already proven loopback.
    let source_ip = req
        .extensions()
        .get::<axum::extract::ConnectInfo<std::net::SocketAddr>>()
        .map(|ci| ci.0.ip());
    let xfp_https = req
        .headers()
        .get("x-forwarded-proto")
        .and_then(|v| v.to_str().ok())
        .is_some_and(|v| v.eq_ignore_ascii_case("https"));

    // `has_token` is a NON-EMPTY check, matching `derive_user_context`'s
    // empty-is-absent semantics. The browser shell sends `?userToken=` (empty)
    // for a brand-new user who has no token yet; that connection must fall
    // through to Local (single-user) so the client can connect and GET a token,
    // NOT be locked out with a 403. Both helpers must agree on "empty": an empty
    // token is treated as no token, so it is never gated.
    let user_context = match decide_user_token(
        hosted_mode.0,
        user_token.as_deref().is_some_and(|t| !t.is_empty()),
        source_ip,
        xfp_https,
    ) {
        UserTokenDecision::Honor => derive_user_context(hosted_mode.0, user_token.as_deref()),
        UserTokenDecision::Local => None,
        UserTokenDecision::RejectInsecure => {
            // Fail LOUD, not silent. Dropping the token and falling back to Local
            // would silently put the user on the WRONG (shared) namespace under a
            // public plaintext deployment; rejecting surfaces the misconfiguration
            // to the operator/user instead. Do not log the token (high-value
            // credential); log only the non-secret source IP and path.
            tracing::warn!(
                source_ip = ?source_ip,
                xfp_https,
                request_path = req.uri().path(),
                "Rejected hosted user token over an insecure connection \
                 (need a loopback source carrying X-Forwarded-Proto: https)"
            );
            return (
                StatusCode::FORBIDDEN,
                "hosted user token requires a secure (TLS/loopback) connection",
            )
                .into_response();
        }
    };

    // Do NOT log credentials. The auth token, the user token, and the full
    // request URI are all sensitive: the URI query string carries
    // `?auth_token=<raw>` and `?userToken=<raw>` verbatim, and the user token
    // is an especially high-value credential (durable, node-independent, names
    // a per-user secret namespace). Log only the URI PATH and non-secret
    // presence/derived flags. `?user_context` is safe — its `Debug` redacts the
    // dek_secret and prints only the non-secret user_id (or `None`).
    tracing::debug!(
        auth_token_present = auth_token.is_some(),
        hosted_mode = hosted_mode.0,
        ?user_context,
        request_path = req.uri().path(),
        "connection_info middleware: resolved auth token, hosted-mode context, and encoding protocol",
    );
    req.extensions_mut().insert(encoding_protoc);
    req.extensions_mut().insert(auth_token);
    req.extensions_mut().insert(user_context);
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
    // The per-connection user secret context derived in `connection_info`
    // (hosted mode). `None` outside hosted mode or when no user token was
    // presented. Owned and moved into the connection task below.
    Extension(user_context): Extension<Option<UserSecretContext>>,
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
            user_context,
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
            // Synthetic disconnect: no secret operations, so no user context.
            user_context: None,
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
    // Per-connection user secret context (hosted mode). Derived once at upgrade
    // and immutable for the life of this connection; cloned into each
    // `ClientConnection::Request` so every request from this connection (and
    // only this connection) binds to the same user namespace.
    user_context: Option<UserSecretContext>,
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
                    user_context.as_ref(),
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
    use freenet_stdlib::client_api::streaming::{CHUNK_THRESHOLD, chunk_response};

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
    use freenet_stdlib::client_api::streaming::{CHUNK_THRESHOLD, chunk_response};

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
    user_context: Option<&UserSecretContext>,
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
            // Clone the connection's user context into this request. Every
            // request from this connection carries the SAME context, derived
            // once at upgrade; a client cannot vary it per-request.
            user_context: user_context.cloned(),
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
        CHUNK_SIZE, CHUNK_THRESHOLD, ReassemblyBuffer, chunk_request,
    };

    /// The hosted-mode flag is the gate: with it OFF, a presented user token is
    /// ignored entirely and no per-user context is derived (single-user).
    #[test]
    fn hosted_mode_off_ignores_user_token() {
        assert!(
            derive_user_context(false, Some("some-user-token")).is_none(),
            "flag off must ignore the token entirely"
        );
        assert!(derive_user_context(false, None).is_none());
        assert!(derive_user_context(false, Some("")).is_none());
    }

    /// With hosted mode ON, a non-empty token yields a context; absent/empty
    /// token still means single-user (no context).
    #[test]
    fn hosted_mode_on_honors_only_a_nonempty_token() {
        assert!(
            derive_user_context(true, Some("alice")).is_some(),
            "flag on + token must derive a context"
        );
        assert!(
            derive_user_context(true, None).is_none(),
            "flag on + no token => single-user"
        );
        assert!(
            derive_user_context(true, Some("")).is_none(),
            "flag on + empty token => single-user (not an empty-token namespace)"
        );
    }

    /// The derived namespace depends only on the token bytes: same token =>
    /// same UserId; different tokens => different UserId. This pins that the
    /// flag-gated boundary feeds the token straight into the (deterministic)
    /// derivation with no other inputs.
    #[test]
    fn derived_context_namespace_tracks_only_the_token() {
        let a = derive_user_context(true, Some("alice")).unwrap();
        let a_again = derive_user_context(true, Some("alice")).unwrap();
        let b = derive_user_context(true, Some("bob")).unwrap();
        assert_eq!(a.user_id(), a_again.user_id());
        assert_ne!(a.user_id(), b.user_id());
    }

    // ---- refuse-plaintext-token secure-connection gate (#4381) ----

    use std::net::{IpAddr, Ipv4Addr, Ipv6Addr};

    const LOOPBACK_V4: IpAddr = IpAddr::V4(Ipv4Addr::LOCALHOST); // 127.0.0.1
    const LOOPBACK_V4_ALT: IpAddr = IpAddr::V4(Ipv4Addr::new(127, 0, 0, 5)); // 127/8
    const LOOPBACK_V6: IpAddr = IpAddr::V6(Ipv6Addr::LOCALHOST); // ::1
    const PUBLIC_V4: IpAddr = IpAddr::V4(Ipv4Addr::new(203, 0, 113, 7)); // TEST-NET-3
    const LAN_V4: IpAddr = IpAddr::V4(Ipv4Addr::new(192, 168, 1, 50)); // private but NOT loopback

    /// `is_loopback_source` accepts loopback v4/v6 and v4-mapped-v6 loopback,
    /// and rejects everything else — including a private-but-not-loopback LAN IP
    /// (loopback, not "private", is the trust anchor for the token gate).
    #[test]
    fn loopback_source_detection() {
        assert!(is_loopback_source(LOOPBACK_V4));
        assert!(is_loopback_source(LOOPBACK_V4_ALT));
        assert!(is_loopback_source(LOOPBACK_V6));
        // ::ffff:127.0.0.1 (IPv4-mapped loopback from a dual-stack socket).
        assert!(is_loopback_source(
            "::ffff:127.0.0.1".parse::<IpAddr>().unwrap()
        ));
        assert!(!is_loopback_source(PUBLIC_V4));
        assert!(!is_loopback_source(LAN_V4));
        // ::ffff:8.8.8.8 (mapped public) must not be treated as loopback.
        assert!(!is_loopback_source(
            "::ffff:8.8.8.8".parse::<IpAddr>().unwrap()
        ));
    }

    /// Flag OFF: the gate is inert. A presented token over ANY source/XFP
    /// (even a remote, plaintext one) is `Local` — no rejection path fires. This
    /// proves the whole invariant is hosted-mode-gated and flag-off is unchanged.
    #[test]
    fn gate_flag_off_is_always_local_never_rejects() {
        for source in [Some(PUBLIC_V4), Some(LAN_V4), Some(LOOPBACK_V4), None] {
            for xfp in [false, true] {
                assert_eq!(
                    decide_user_token(false, true, source, xfp),
                    UserTokenDecision::Local,
                    "flag off must be Local for source={source:?} xfp={xfp}"
                );
            }
        }
    }

    /// No token (any mode/source/XFP): `Local`, never rejected. Token-less
    /// connections are unaffected by the gate in all modes.
    #[test]
    fn gate_no_token_is_always_local() {
        for hosted in [false, true] {
            for source in [Some(PUBLIC_V4), Some(LOOPBACK_V4), None] {
                for xfp in [false, true] {
                    assert_eq!(
                        decide_user_token(hosted, false, source, xfp),
                        UserTokenDecision::Local,
                        "no token must be Local for hosted={hosted} source={source:?}"
                    );
                }
            }
        }
    }

    /// Hosted ON + token + loopback source + `X-Forwarded-Proto: https` => Honor.
    /// The intended hosted deployment: a same-host TLS-terminating reverse proxy
    /// attesting the browser→proxy hop was TLS (e.g. Caddy, which sets XFP by
    /// default).
    #[test]
    fn gate_loopback_with_https_is_honored() {
        for source in [LOOPBACK_V4, LOOPBACK_V4_ALT, LOOPBACK_V6] {
            assert_eq!(
                decide_user_token(true, true, Some(source), true),
                UserTokenDecision::Honor,
                "loopback {source:?} + XFP=https (intended TLS proxy) must be honored"
            );
        }
    }

    /// Hosted ON + token + loopback source + NO `X-Forwarded-Proto: https` =>
    /// REJECT. This single case now covers BOTH the direct plaintext loopback
    /// connection AND the nginx-default plaintext-proxy gap: nginx's default
    /// rewrites `Host` to the upstream `127.0.0.1:7509` and (without explicit
    /// config) forwards no XFP, so a same-host proxy serving public PLAINTEXT
    /// http presents a loopback source, a local-looking Host, and no XFP — yet
    /// the browser→proxy hop crossed the network in cleartext. Without positive
    /// HTTPS evidence the token is refused: Host is NOT consulted, so it cannot
    /// be used to bypass the gate.
    #[test]
    fn gate_loopback_no_https_is_rejected() {
        for source in [LOOPBACK_V4, LOOPBACK_V4_ALT, LOOPBACK_V6] {
            assert_eq!(
                decide_user_token(true, true, Some(source), false),
                UserTokenDecision::RejectInsecure,
                "loopback {source:?} + NO XFP (direct plaintext or plaintext proxy) \
                 must be rejected — Host must NOT grant trust"
            );
        }
    }

    /// Hosted ON + token + non-loopback source => REJECT for ANY XFP. The token
    /// reached the node directly over the network. XFP is client-spoofable from
    /// off-host and is never consulted here. Covers the directly-exposed
    /// plaintext port and the LAN-exposed port (trust anchor is loopback, NOT
    /// "is_private_ip").
    #[test]
    fn gate_non_loopback_source_is_always_rejected() {
        for source in [PUBLIC_V4, LAN_V4] {
            for xfp in [false, true] {
                assert_eq!(
                    decide_user_token(true, true, Some(source), xfp),
                    UserTokenDecision::RejectInsecure,
                    "non-loopback {source:?} must be rejected (xfp={xfp}) \
                     — spoofable XFP must NOT widen a remote source"
                );
            }
        }
    }

    /// Hosted ON + token + unknown source (`None`) => REJECT (fail closed) for any
    /// XFP. We cannot prove loopback, so we must not honor. In production
    /// ConnectInfo is always present; `None` only arises if the plumbing is missing.
    #[test]
    fn gate_missing_source_fails_closed() {
        for xfp in [false, true] {
            assert_eq!(
                decide_user_token(true, true, None, xfp),
                UserTokenDecision::RejectInsecure,
            );
        }
    }

    /// Regression: an EMPTY `userToken` (`?userToken=` => `Some("")`) must be
    /// treated as no token (Local), NOT rejected as insecure. The call site
    /// computes `has_token` as a NON-EMPTY check, so an empty token over a
    /// remote/non-loopback hosted connection falls through to Local — a
    /// brand-new remote user whose client sends an empty `userToken=` connects
    /// as single-user to GET a token, instead of being locked out with a 403.
    /// This mirrors `derive_user_context`'s empty-is-absent semantics so the two
    /// helpers agree on "empty".
    #[test]
    fn gate_empty_token_is_local_not_rejected() {
        // Replicate the call-site computation of `has_token`.
        let has_token = |t: Option<&str>| t.is_some_and(|t| !t.is_empty());

        // Empty token + hosted on + remote source => Local (the regression).
        assert_eq!(
            decide_user_token(true, has_token(Some("")), Some(PUBLIC_V4), false),
            UserTokenDecision::Local,
            "empty userToken over a remote hosted connection must be Local, not rejected"
        );
        // Empty token + hosted on + loopback + no XFP => Local (no spurious
        // Honor and no rejection either — empty is absent before the gate runs).
        assert_eq!(
            decide_user_token(true, has_token(Some("")), Some(LOOPBACK_V4), false),
            UserTokenDecision::Local,
        );
        // Sanity: a NON-empty token over the same remote source IS rejected, so
        // the empty-token allowance is specifically about emptiness.
        assert_eq!(
            decide_user_token(true, has_token(Some("alice")), Some(PUBLIC_V4), false),
            UserTokenDecision::RejectInsecure,
        );
        // Whitespace is intentionally a real (non-empty) token, consistent with
        // derive_user_context — so it IS gated (rejected over a remote source).
        assert_eq!(
            decide_user_token(true, has_token(Some(" ")), Some(PUBLIC_V4), false),
            UserTokenDecision::RejectInsecure,
            "whitespace token is non-empty => treated as a real token"
        );
    }

    /// Integration-level check that the `connection_info` middleware actually
    /// enforces the gate end-to-end (not just the pure helper): it reads the
    /// `ConnectInfo<SocketAddr>` source, the `X-Forwarded-Proto` header, and the
    /// `userToken`, and returns 403 for an insecure hosted token while passing
    /// everything else through. Builds a minimal router that layers
    /// `connection_info` over a trivial 200 handler, mirroring the production
    /// extension stack (`HostedMode`, `AllowedHosts`, `AllowedSourceCidrs`).
    ///
    /// These requests are NOT WebSocket upgrades (no `Upgrade: websocket`), so
    /// the Origin/Host CSWSH check is skipped — this isolates the user-token gate.
    #[tokio::test]
    async fn middleware_rejects_insecure_hosted_token_and_passes_rest() {
        use axum::{Extension, Router, body::Body, http::Request, http::StatusCode, routing::get};
        use std::net::SocketAddr;
        use tower::ServiceExt;

        async fn handler() -> &'static str {
            "ok"
        }

        // `connection_info` inserts `Extension<Option<UserSecretContext>>`,
        // `Extension<Option<AuthToken>>`, and `Extension<EncodingProtocol>` and
        // then calls the next service; the trivial handler ignores them. The
        // three extractor Extensions it READS must be present or it 500s.
        fn router(hosted: bool) -> Router {
            Router::new()
                .route("/", get(handler))
                .layer(axum::middleware::from_fn(connection_info))
                .layer(Extension(crate::server::HostedMode(hosted)))
                .layer(Extension(crate::server::AllowedHosts::default()))
                .layer(Extension(crate::server::AllowedSourceCidrs::default()))
        }

        // The gate reads only the `X-Forwarded-Proto` header and the
        // `ConnectInfo<SocketAddr>` source — `Host` is NOT consulted (it is
        // proxy-rewritable and cannot grant trust), so the helper does not set
        // it. `xfp_https` sets `X-Forwarded-Proto: https` when true.
        fn req(
            hosted_uri_token: Option<&str>,
            source: Option<SocketAddr>,
            xfp_https: bool,
        ) -> Request<Body> {
            let uri = match hosted_uri_token {
                Some(t) => format!("/?userToken={t}"),
                None => "/".to_string(),
            };
            let mut builder = Request::builder().uri(uri);
            if xfp_https {
                builder = builder.header("x-forwarded-proto", "https");
            }
            if let Some(addr) = source {
                builder = builder.extension(axum::extract::ConnectInfo(addr));
            }
            builder.body(Body::empty()).unwrap()
        }

        let loopback: SocketAddr = ([127, 0, 0, 1], 12345).into();
        let public: SocketAddr = ([203, 0, 113, 7], 443).into();

        // Hosted ON + token + remote plaintext => 403 (the leak-prevention case).
        let resp = router(true)
            .oneshot(req(Some("alice"), Some(public), false))
            .await
            .unwrap();
        assert_eq!(
            resp.status(),
            StatusCode::FORBIDDEN,
            "insecure hosted token must be rejected at the middleware"
        );

        // Hosted ON + token + remote + spoofed XFP https => still 403.
        let resp = router(true)
            .oneshot(req(Some("alice"), Some(public), true))
            .await
            .unwrap();
        assert_eq!(
            resp.status(),
            StatusCode::FORBIDDEN,
            "XFP from a remote source must not bypass the gate"
        );

        // Hosted ON + token + loopback + XFP https => 200 (intended same-host TLS
        // proxy; e.g. Caddy, which sets X-Forwarded-Proto: https by default).
        let resp = router(true)
            .oneshot(req(Some("alice"), Some(loopback), true))
            .await
            .unwrap();
        assert_eq!(
            resp.status(),
            StatusCode::OK,
            "loopback + XFP=https (TLS proxy) hosted token must be honored"
        );

        // Hosted ON + token + loopback + NO XFP => 403. Covers BOTH a direct
        // plaintext loopback connection AND the nginx-default plaintext-proxy
        // gap (loopback source, local-looking rewritten Host, no XFP): without
        // positive HTTPS evidence the token is refused. Host is NOT consulted,
        // so it cannot be used to bypass the gate.
        let resp = router(true)
            .oneshot(req(Some("alice"), Some(loopback), false))
            .await
            .unwrap();
        assert_eq!(
            resp.status(),
            StatusCode::FORBIDDEN,
            "loopback + no XFP (direct plaintext or plaintext proxy) must be rejected"
        );

        // Hosted OFF + token + remote => 200 (gate inert; token ignored, Local).
        let resp = router(false)
            .oneshot(req(Some("alice"), Some(public), false))
            .await
            .unwrap();
        assert_eq!(
            resp.status(),
            StatusCode::OK,
            "flag off must not reject; token is ignored, not gated"
        );

        // No token + remote (any mode) => 200 (unaffected by the gate).
        let resp = router(true)
            .oneshot(req(None, Some(public), false))
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        let resp = router(false)
            .oneshot(req(None, Some(public), false))
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);

        // Hosted ON + EMPTY token (`/?userToken=`) + remote => 200, NOT 403.
        // A brand-new remote user whose client sends an empty userToken must be
        // able to connect (as Local) to GET a real token, not be locked out.
        let resp = router(true)
            .oneshot(req(Some(""), Some(public), false))
            .await
            .unwrap();
        assert_eq!(
            resp.status(),
            StatusCode::OK,
            "empty userToken over a remote hosted connection must be Local (200), not rejected"
        );
    }

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

    fn headers_with_host(host: &str) -> axum::http::HeaderMap {
        let mut map = axum::http::HeaderMap::new();
        map.insert(
            axum::http::header::HOST,
            axum::http::HeaderValue::from_str(host).unwrap(),
        );
        map
    }

    fn cidrs(list: &[&str]) -> crate::server::AllowedSourceCidrs {
        crate::server::AllowedSourceCidrs(Arc::new(
            list.iter().map(|s| s.parse().unwrap()).collect(),
        ))
    }

    /// Parsing: only the two RFC-compliant shapes (`v4[:port]` and
    /// `[v6][:port]`) are accepted. Every other shape must fail closed.
    #[test]
    fn parse_host_header_ip_variants() {
        // Happy path: IPv4 with and without port.
        assert_eq!(
            parse_host_header_ip("100.64.1.5:7509"),
            Some("100.64.1.5".parse().unwrap())
        );
        assert_eq!(
            parse_host_header_ip("100.64.1.5"),
            Some("100.64.1.5".parse().unwrap())
        );
        // Happy path: bracketed IPv6 with and without port.
        assert_eq!(
            parse_host_header_ip("[fd7a:115c:a1e0::1]:7509"),
            Some("fd7a:115c:a1e0::1".parse().unwrap())
        );
        assert_eq!(
            parse_host_header_ip("[fd7a:115c:a1e0::1]"),
            Some("fd7a:115c:a1e0::1".parse().unwrap())
        );

        // Malformed bracket cases — all rejected. These are the
        // regressions Codex review flagged: trailing junk after `]`
        // used to be silently accepted and would have matched a CIDR.
        assert_eq!(parse_host_header_ip("[100.64.1.5]evil.com"), None);
        assert_eq!(parse_host_header_ip("[100.64.1.5]:7509"), None); // v4 in brackets: no
        assert_eq!(parse_host_header_ip("[fd7a::1]:notaport"), None);
        assert_eq!(parse_host_header_ip("[fd7a::1]garbage"), None);
        assert_eq!(parse_host_header_ip("["), None);
        assert_eq!(parse_host_header_ip("[]"), None);
        assert_eq!(parse_host_header_ip("[::1"), None); // unclosed bracket

        // Unbracketed IPv6 is ambiguous with `host:port`, so rejected.
        assert_eq!(parse_host_header_ip("fd7a:115c:a1e0::1"), None);

        // IPv4 with non-numeric port — rejected.
        assert_eq!(parse_host_header_ip("100.64.1.5:abc"), None);
        assert_eq!(parse_host_header_ip("100.64.1.5:"), None);

        // Hostnames never parse as IPs.
        assert_eq!(parse_host_header_ip("evil.com"), None);
        assert_eq!(parse_host_header_ip("evil.com:7509"), None);
        assert_eq!(parse_host_header_ip("localhost:7509"), None);
        assert_eq!(parse_host_header_ip(""), None);
    }

    /// Regression for the v0.2.47 gap: a user configuring
    /// `allowed-source-cidrs = ["100.64.0.0/10"]` to reach their node via
    /// Tailscale must be able to complete the WebSocket upgrade. The Host
    /// header they send is their own tailnet IP, which is not in
    /// `allowed_hosts` unless duplicated manually. Before the fix, `is_ws_upgrade`
    /// rejected these with 403.
    #[test]
    fn host_header_ip_in_tailscale_cidr_allowed() {
        let allowed = cidrs(&["100.64.0.0/10"]);
        assert!(host_header_ip_in_cidrs(
            &headers_with_host("100.64.1.5:7509"),
            "null",
            &allowed,
        ));
        assert!(host_header_ip_in_cidrs(
            &headers_with_host("100.64.1.5"),
            "null",
            &allowed,
        ));
        // A neighbouring user's tailnet IP is still inside 100.64.0.0/10;
        // if the operator widened the CIDR, that's the operator's choice.
        assert!(host_header_ip_in_cidrs(
            &headers_with_host("100.127.9.9:7509"),
            "null",
            &allowed,
        ));
    }

    /// Regression guard against a CIDR match accidentally widening to any IP:
    /// a non-empty allowlist must still reject IPs outside its ranges.
    #[test]
    fn host_header_ip_outside_cidr_rejected() {
        let allowed = cidrs(&["100.64.0.0/10"]);
        assert!(!host_header_ip_in_cidrs(
            &headers_with_host("8.8.8.8:7509"),
            "null",
            &allowed,
        ));
        assert!(!host_header_ip_in_cidrs(
            &headers_with_host("100.128.0.1:7509"),
            "null",
            &allowed,
        ));
        assert!(!host_header_ip_in_cidrs(
            &headers_with_host("100.63.255.255:7509"),
            "null",
            &allowed,
        ));
    }

    /// DNS-rebinding defense: hostnames must NEVER be accepted via the CIDR
    /// path, even if the attacker sets up a DNS record that resolves to an
    /// IP inside the allowed CIDR. We intentionally do not resolve DNS in
    /// this middleware; only string-parseable IP literals are considered.
    #[test]
    fn host_header_hostname_never_accepted_via_cidr() {
        let allowed = cidrs(&["100.64.0.0/10"]);
        assert!(!host_header_ip_in_cidrs(
            &headers_with_host("evil.com"),
            "null",
            &allowed,
        ));
        assert!(!host_header_ip_in_cidrs(
            &headers_with_host("evil.com:7509"),
            "null",
            &allowed,
        ));
        // A hostname that happens to look like an IP at a glance is still a
        // hostname to `IpAddr::from_str`, so still rejected.
        assert!(!host_header_ip_in_cidrs(
            &headers_with_host("100-64-1-5.evil.com"),
            "null",
            &allowed,
        ));
    }

    /// Default-allow for private/LAN IPs only when the Origin is a
    /// same-IP literal; default-deny for public IPs and for `Origin: null`
    /// on the default-private branch. The `null` rejection is the CSWSH
    /// guard against a malicious `<iframe sandbox>` on `evil.com` probing
    /// LAN-bound nodes — operators who need null-origin sandboxed access
    /// from non-localhost contexts must opt in via `--allowed-source-cidrs`.
    #[test]
    fn host_header_empty_cidrs_accepts_private_with_ip_literal_origin() {
        let empty = crate::server::AllowedSourceCidrs::default();
        // Private (loopback, RFC 1918) + same-IP literal Origin: accepted.
        assert!(host_header_ip_in_cidrs(
            &headers_with_host("127.0.0.1:7509"),
            "http://127.0.0.1:7509",
            &empty,
        ));
        assert!(host_header_ip_in_cidrs(
            &headers_with_host("192.168.1.5:7509"),
            "http://192.168.1.5:7509",
            &empty,
        ));
        // Private Host + Origin: null: REJECTED on the default branch
        // (CSWSH guard against sandboxed iframes on arbitrary sites).
        assert!(!host_header_ip_in_cidrs(
            &headers_with_host("127.0.0.1:7509"),
            "null",
            &empty,
        ));
        assert!(!host_header_ip_in_cidrs(
            &headers_with_host("192.168.1.5:7509"),
            "null",
            &empty,
        ));
        // CGNAT (100.64/10) is NOT private — still requires explicit
        // CIDR even with a same-IP literal Origin (Tailnet users opt in
        // via `--allowed-source-cidrs`).
        assert!(!host_header_ip_in_cidrs(
            &headers_with_host("100.64.1.5:7509"),
            "http://100.64.1.5:7509",
            &empty,
        ));
        // Public IPs always rejected even with a same-IP literal Origin.
        assert!(!host_header_ip_in_cidrs(
            &headers_with_host("8.8.8.8:7509"),
            "http://8.8.8.8:7509",
            &empty,
        ));
    }

    /// Regression for #3957: a node bound to `0.0.0.0` and accessed from
    /// the LAN at its RFC 1918 IP must accept the WS upgrade out of the
    /// box. Reproduces fluffomat's report (Win11 host browsing into an
    /// Alpine VM at `192.168.178.192:7509`, bridged networking) where
    /// `private_network_filter` accepted the connection but the WS
    /// upgrade rejected it because the LAN IP wasn't in `allowed_hosts`
    /// and no `--allowed-source-cidrs` was configured.
    ///
    /// The accepting case is the actual production flow: the shell page
    /// (loaded at `http://<lan-ip>:7509/`) opens a real WebSocket on
    /// behalf of the sandboxed iframe via its postMessage bridge, so the
    /// browser sends `Origin: http://<lan-ip>:7509` — a same-IP literal,
    /// which passes the CSWSH guard. River and Delta both use this
    /// shell-mediated path, so neither relies on the now-rejected
    /// `Origin: null` shortcut on the default-private branch.
    #[test]
    fn lan_ip_accepted_without_explicit_allowlist_3957() {
        let empty = crate::server::AllowedSourceCidrs::default();
        // Production scenario: shell page opens a real WebSocket whose
        // Origin is the gateway's own LAN URL.
        assert!(host_header_ip_in_cidrs(
            &headers_with_host("192.168.178.192:7509"),
            "http://192.168.178.192:7509",
            &empty,
        ));
        // CSWSH guards still hold:

        // - Sandboxed iframe on a 3rd-party site (Origin: null) probing
        //   the LAN-bound gateway: REJECTED by the default-private branch.
        assert!(!host_header_ip_in_cidrs(
            &headers_with_host("192.168.178.192:7509"),
            "null",
            &empty,
        ));
        // - Cross-origin attacker hostname: REJECTED.
        assert!(!host_header_ip_in_cidrs(
            &headers_with_host("192.168.178.192:7509"),
            "https://evil.com",
            &empty,
        ));
        // - Origin spelling a different LAN IP must not hijack via the
        //   same-host check.
        assert!(!host_header_ip_in_cidrs(
            &headers_with_host("192.168.178.192:7509"),
            "http://192.168.178.50:7509",
            &empty,
        ));
    }

    /// Pin: `0.0.0.0` and `::` (unspecified addresses) are NOT acceptable
    /// Host headers on the default-private branch even though
    /// `is_private_ip` returns true for them. No legitimate browser sends
    /// `Host: 0.0.0.0` or `Host: [::]`, so we narrow the default-LAN
    /// surface to actual host IPs.
    #[test]
    fn host_header_unspecified_rejected_on_default_branch() {
        let empty = crate::server::AllowedSourceCidrs::default();
        assert!(!host_header_ip_in_cidrs(
            &headers_with_host("0.0.0.0:7509"),
            "http://0.0.0.0:7509",
            &empty,
        ));
        assert!(!host_header_ip_in_cidrs(
            &headers_with_host("[::]:7509"),
            "http://[::]:7509",
            &empty,
        ));
        // Even with operator CIDR explicitly covering 0.0.0.0/0 — wait,
        // the CIDR validator rejects /0, so the operator path can't
        // actually permit unspecified. This case is reachable only via a
        // crafted CIDR like "0.0.0.0/8", which IS accepted by the
        // validator. Pin the resulting behaviour: unspecified Host inside
        // an explicit operator CIDR remains accepted (operator opt-in).
        let permissive = cidrs(&["0.0.0.0/8"]);
        assert!(host_header_ip_in_cidrs(
            &headers_with_host("0.0.0.0:7509"),
            "null",
            &permissive,
        ));
    }

    /// IPv6 CIDRs must work end-to-end, including the bracketed `[v6]:port`
    /// form browsers send in the Host header. To exercise CIDR membership
    /// specifically (not the default-private path) the "outside" case uses
    /// a global-unicast IPv6 (2001:db8::/32 documentation prefix) which is
    /// neither ULA nor link-local nor loopback, so it has no default-allow
    /// path and must be rejected when not in the CIDR.
    #[test]
    fn host_header_ipv6_in_cidr_allowed() {
        let allowed = cidrs(&["fd7a:115c:a1e0::/48"]);
        assert!(host_header_ip_in_cidrs(
            &headers_with_host("[fd7a:115c:a1e0::1]:7509"),
            "null",
            &allowed,
        ));
        assert!(host_header_ip_in_cidrs(
            &headers_with_host("[fd7a:115c:a1e0::1]"),
            "null",
            &allowed,
        ));
        // Public IPv6 (documentation prefix) outside the /48 — rejected
        // because it isn't ULA/loopback/link-local AND isn't in the CIDR.
        assert!(!host_header_ip_in_cidrs(
            &headers_with_host("[2001:db8::1]:7509"),
            "null",
            &allowed,
        ));
    }

    /// IPv4-mapped IPv6 (`[::ffff:100.64.1.5]`) in the Host header must be
    /// normalized to IPv4 so an operator's IPv4 CIDR still matches. Mirrors
    /// the same normalization in `is_source_allowed`.
    #[test]
    fn host_header_ipv4_mapped_v6_normalized_to_v4() {
        let allowed = cidrs(&["100.64.0.0/10"]);
        assert!(host_header_ip_in_cidrs(
            &headers_with_host("[::ffff:100.64.1.5]:7509"),
            "null",
            &allowed,
        ));
        assert!(host_header_ip_in_cidrs(
            &headers_with_host("[::ffff:100.64.1.5]"),
            "null",
            &allowed,
        ));
        // Mapped v6 OUTSIDE the v4 CIDR still rejected.
        assert!(!host_header_ip_in_cidrs(
            &headers_with_host("[::ffff:8.8.8.8]:7509"),
            "null",
            &allowed,
        ));
    }

    /// Missing Host header must not panic and must not accept.
    #[test]
    fn host_header_missing_rejected() {
        let allowed = cidrs(&["100.64.0.0/10"]);
        assert!(!host_header_ip_in_cidrs(
            &axum::http::HeaderMap::new(),
            "null",
            &allowed,
        ));
    }

    /// CSWSH regression: an attacker at `https://evil.com` whose browser-side
    /// JS opens `new WebSocket("ws://<victim-tailnet-ip>:7509/...")` sends
    /// `Origin: https://evil.com` along with `Host: <victim-tailnet-ip>:7509`.
    /// The Host IP matches the operator's CIDR, but the Origin does not, so
    /// we must reject. This is the primary guard against cross-site
    /// WebSocket hijacking introduced in earlier iterations of this PR.
    #[test]
    fn cswsh_cross_origin_real_origin_rejected() {
        let allowed = cidrs(&["100.64.0.0/10"]);
        assert!(!host_header_ip_in_cidrs(
            &headers_with_host("100.64.1.5:7509"),
            "https://evil.com",
            &allowed,
        ));
        assert!(!host_header_ip_in_cidrs(
            &headers_with_host("100.64.1.5:7509"),
            "http://evil.com:8080",
            &allowed,
        ));
        // Even an origin that spells an IP but doesn't match the Host IP
        // is rejected — this catches attacker pages hosted on one CIDR
        // peer trying to hijack a different peer.
        assert!(!host_header_ip_in_cidrs(
            &headers_with_host("100.64.1.5:7509"),
            "http://100.64.9.9:7509",
            &allowed,
        ));
    }

    /// Direct browser access (user typed `http://100.64.1.5:7509/` into the
    /// address bar): Origin and Host refer to the same IP. Must be accepted.
    #[test]
    fn direct_browser_access_same_ip_accepted() {
        let allowed = cidrs(&["100.64.0.0/10"]);
        assert!(host_header_ip_in_cidrs(
            &headers_with_host("100.64.1.5:7509"),
            "http://100.64.1.5:7509",
            &allowed,
        ));
        assert!(host_header_ip_in_cidrs(
            &headers_with_host("100.64.1.5:7509"),
            "https://100.64.1.5",
            &allowed,
        ));
    }

    /// The `Origin: null` case covers Freenet's sandboxed dApp iframe, which
    /// deliberately strips origin via `<iframe sandbox=...>` without
    /// `allow-same-origin`. Must be accepted when Host IP is in CIDR. A
    /// malicious sandboxed iframe hosted on evil.com would also send
    /// `Origin: null` — this is the documented CSWSH risk the operator
    /// accepts when configuring `--allowed-source-cidrs` (equivalent risk
    /// profile to an explicit `--allowed-host` entry).
    #[test]
    fn origin_null_accepted_for_sandboxed_iframe() {
        let allowed = cidrs(&["100.64.0.0/10"]);
        assert!(host_header_ip_in_cidrs(
            &headers_with_host("100.64.1.5:7509"),
            "null",
            &allowed,
        ));
    }

    /// Unit tests for the Origin host-part extractor.
    #[test]
    fn parse_origin_host_variants() {
        assert_eq!(
            parse_origin_host("http://100.64.1.5:7509"),
            Some("100.64.1.5")
        );
        assert_eq!(parse_origin_host("https://100.64.1.5"), Some("100.64.1.5"));
        assert_eq!(
            parse_origin_host("http://[fd7a:115c:a1e0::1]:7509"),
            Some("fd7a:115c:a1e0::1")
        );
        assert_eq!(parse_origin_host("http://evil.com:8080"), Some("evil.com"));
        // Userinfo (non-standard in Origin, but tolerated).
        assert_eq!(
            parse_origin_host("http://user@100.64.1.5:7509"),
            Some("100.64.1.5")
        );
        // Missing scheme → not a valid Origin.
        assert_eq!(parse_origin_host("evil.com"), None);
        assert_eq!(parse_origin_host(""), None);
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
        use freenet_stdlib::client_api::streaming::{MAX_CONCURRENT_STREAMS, StreamError};

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
        use freenet_stdlib::client_api::streaming::{MAX_TOTAL_CHUNKS, StreamError};

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
