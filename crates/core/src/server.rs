//! Handles external client connections (HTTP/WebSocket).
//!
//! This module acts as the bridge between external clients and the Freenet node's core logic.
//! It parses `ClientRequest`s, sends them to the main node event loop (`node::Node`) via an
//! internal channel, and forwards `HostResponse`s back to the clients.
//!
//! See [`../architecture.md`](../architecture.md) for its place in the overall architecture.

pub(crate) mod app_packaging;
pub(crate) mod client_api;
pub(crate) mod errors;
mod home_page;
pub(crate) mod path_handlers;

use std::collections::HashSet;
use std::net::{IpAddr, SocketAddr};
use std::sync::Arc;
use std::time::Duration;
use tokio::time::Instant;

use dashmap::DashMap;

use freenet_stdlib::{
    client_api::{ClientError, ClientRequest, HostResponse},
    prelude::*,
};

use axum::{response::IntoResponse, Extension};
use client_api::HttpClientApi;
use tower_http::trace::TraceLayer;

use crate::{
    client_events::{websocket::WebSocketProxy, AuthToken, BoxedClient, ClientId, HostResult},
    config::{GlobalExecutor, WebsocketApiConfig},
};

pub use app_packaging::WebApp;

// Export types needed for integration testing
pub use client_api::{OriginContract, OriginContractMap};

/// API version for websocket and HTTP client API **routing**.
///
/// This controls URL path prefixes (`/v1/...` vs `/v2/...`) and is used by
/// the HTTP client API and WebSocket proxy to version client-facing endpoints.
///
/// **Not to be confused with [`crate::wasm_runtime::delegate_api::DelegateApiVersion`]**,
/// which governs WASM-level delegate host function availability and is
/// auto-detected from the delegate module's imports. The two version axes are
/// independent: a V1 HTTP client can invoke a V2 delegate, and vice versa.
///
/// V1 is the default for backwards compatibility. V2 currently behaves
/// identically but provides a routing seam for future protocol changes.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub(crate) enum ApiVersion {
    #[default]
    V1,
    V2,
}

impl ApiVersion {
    /// Returns the URL path prefix for this version (e.g. `"v1"` or `"v2"`).
    pub fn prefix(self) -> &'static str {
        match self {
            Self::V1 => "v1",
            Self::V2 => "v2",
        }
    }
}

#[derive(Debug)]
#[allow(clippy::large_enum_variant)]
pub(crate) enum ClientConnection {
    NewConnection {
        callbacks: tokio::sync::mpsc::UnboundedSender<HostCallbackResult>,
        assigned_token: Option<(AuthToken, ContractInstanceId)>,
    },
    Request {
        client_id: ClientId,
        req: Box<ClientRequest<'static>>,
        auth_token: Option<AuthToken>,
        origin_contract: Option<ContractInstanceId>,
        /// Plumbing for future V2-specific dispatch; not yet read.
        #[allow(dead_code)]
        api_version: ApiVersion,
    },
}

#[derive(Debug)]
pub(crate) enum HostCallbackResult {
    NewId {
        id: ClientId,
    },
    Result {
        id: ClientId,
        result: Result<HostResponse, ClientError>,
    },
    SubscriptionChannel {
        id: ClientId,
        /// The contract being subscribed to (identified by instance_id since full key may not be known yet)
        key: ContractInstanceId,
        callback: tokio::sync::mpsc::Receiver<HostResult>,
    },
}

async fn serve(socket: SocketAddr, router: axum::Router) -> std::io::Result<()> {
    serve_with_listener(socket, router, None).await
}

async fn serve_with_listener(
    socket: SocketAddr,
    router: axum::Router,
    pre_bound: Option<std::net::TcpListener>,
) -> std::io::Result<()> {
    let listener = match pre_bound {
        Some(std_listener) => {
            std_listener.set_nonblocking(true)?;
            tokio::net::TcpListener::from_std(std_listener)?
        }
        None => {
            // Use SO_REUSEADDR so we can rebind immediately if the previous
            // process exited but the socket is still in TIME_WAIT.
            let std_listener = {
                let is_ipv6 = socket.is_ipv6();
                let sock = socket2::Socket::new(
                    if is_ipv6 {
                        socket2::Domain::IPV6
                    } else {
                        socket2::Domain::IPV4
                    },
                    socket2::Type::STREAM,
                    Some(socket2::Protocol::TCP),
                )
                .map_err(|e| {
                    std::io::Error::new(e.kind(), format!("Failed to create socket: {e}"))
                })?;
                // Enable dual-stack: accept both IPv4 and IPv6 on a single socket.
                // IPv4 clients connect via IPv4-mapped addresses (::ffff:x.x.x.x).
                if is_ipv6 {
                    sock.set_only_v6(false)?;
                }
                sock.set_reuse_address(true)?;
                sock.set_nonblocking(true)?;
                sock.bind(&socket.into()).map_err(|e| {
                    if e.kind() == std::io::ErrorKind::AddrInUse {
                        std::io::Error::new(
                            std::io::ErrorKind::AddrInUse,
                            format!(
                                "Port {} is already in use. Another freenet process may be running. \
                                 Use 'pkill freenet' to stop it, or specify a different port with --ws-api-port.",
                                socket.port()
                            ),
                        )
                    } else {
                        e
                    }
                })?;
                sock.listen(128)?;
                std::net::TcpListener::from(sock)
            };
            tokio::net::TcpListener::from_std(std_listener)?
        }
    };
    tracing::info!("HTTP client API listening on {}", socket);
    GlobalExecutor::spawn(async move {
        axum::serve(
            listener,
            router.into_make_service_with_connect_info::<SocketAddr>(),
        )
        .await
        .map_err(|e| {
            tracing::error!("Error while running HTTP client API server: {e}");
        })
    });
    Ok(())
}

/// Returns `true` if the IP is a private/local address suitable for LAN access.
///
/// Accepted ranges:
/// - **IPv4**: loopback (127/8), RFC 1918 (10/8, 172.16/12, 192.168/16),
///   link-local (169.254/16), unspecified (0.0.0.0)
/// - **IPv6**: loopback (::1), link-local (fe80::/10), ULA (fc00::/7),
///   unspecified (::), IPv4-mapped (::ffff:x.x.x.x) delegated to IPv4 checks
pub fn is_private_ip(ip: &IpAddr) -> bool {
    match ip {
        IpAddr::V4(v4) => {
            v4.is_loopback() || v4.is_private() || v4.is_link_local() || v4.is_unspecified()
        }
        IpAddr::V6(v6) => {
            // IPv4-mapped addresses (::ffff:x.x.x.x) from dual-stack sockets
            if let Some(v4) = v6.to_ipv4_mapped() {
                return is_private_ip(&IpAddr::V4(v4));
            }
            v6.is_loopback() || v6.is_unspecified() || is_ipv6_link_local(v6) || is_ipv6_ula(v6)
        }
    }
}

/// fe80::/10 — IPv6 link-local
fn is_ipv6_link_local(addr: &std::net::Ipv6Addr) -> bool {
    (addr.segments()[0] & 0xffc0) == 0xfe80
}

/// fc00::/7 — IPv6 Unique Local Address (ULA)
fn is_ipv6_ula(addr: &std::net::Ipv6Addr) -> bool {
    (addr.segments()[0] & 0xfe00) == 0xfc00
}

pub mod local_node {
    use freenet_stdlib::client_api::{ClientRequest, ErrorKind};
    use std::net::SocketAddr;
    use tower_http::trace::TraceLayer;

    use crate::{
        client_events::{websocket::WebSocketProxy, ClientEventsProxy, OpenRequest},
        contract::{Executor, ExecutorError},
    };

    use super::{client_api::HttpClientApi, serve};

    pub async fn run_local_node(mut executor: Executor, socket: SocketAddr) -> anyhow::Result<()> {
        if !super::is_private_ip(&socket.ip()) {
            anyhow::bail!(
                "invalid ip: {}, only loopback and private network addresses are allowed",
                socket.ip()
            )
        }
        let (mut gw, gw_router) = HttpClientApi::as_router(&socket);
        let (mut ws_proxy, ws_router) = WebSocketProxy::create_router(gw_router);

        serve(socket, ws_router.layer(TraceLayer::new_for_http())).await?;

        // TODO: use combinator instead
        // let mut all_clients =
        //    ClientEventsCombinator::new([Box::new(ws_handle), Box::new(http_handle)]);
        enum Receiver {
            Ws,
            Gw,
        }
        let mut receiver;
        loop {
            let req = crate::deterministic_select! {
                req = ws_proxy.recv() => {
                    receiver = Receiver::Ws;
                    req?
                },
                req = gw.recv() => {
                    receiver = Receiver::Gw;
                    req?
                },
            };
            let OpenRequest {
                client_id: id,
                request,
                notification_channel,
                token,
                ..
            } = req;
            tracing::trace!(cli_id = %id, "got request -> {request}");

            let res = match *request {
                ClientRequest::ContractOp(op) => {
                    executor
                        .contract_requests(op, id, notification_channel)
                        .await
                }
                ClientRequest::DelegateOp(op) => {
                    let origin_contract = token.and_then(|token| {
                        gw.origin_contracts
                            .get(&token)
                            .map(|entry| entry.contract_id)
                    });
                    executor.delegate_request(op, origin_contract.as_ref())
                }
                ClientRequest::Disconnect { cause } => {
                    if let Some(cause) = cause {
                        tracing::info!("disconnecting cause: {cause}");
                    }
                    // fixme: token must live for a bit to allow reconnections
                    if let Some(rm_token) = gw.origin_contracts.iter().find_map(|entry| {
                        let (k, origin) = entry.pair();
                        (origin.client_id == id).then(|| k.clone())
                    }) {
                        gw.origin_contracts.remove(&rm_token);
                    }
                    continue;
                }
                ClientRequest::Authenticate { .. }
                | ClientRequest::NodeQueries(_)
                | ClientRequest::Close
                | _ => Err(ExecutorError::other(anyhow::anyhow!("not supported"))),
            };

            match res {
                Ok(res) => {
                    match receiver {
                        Receiver::Ws => ws_proxy.send(id, Ok(res)).await?,
                        Receiver::Gw => gw.send(id, Ok(res)).await?,
                    };
                }
                Err(err) if err.is_request() => {
                    let err = ErrorKind::RequestError(err.unwrap_request());
                    match receiver {
                        Receiver::Ws => {
                            ws_proxy.send(id, Err(err.into())).await?;
                        }
                        Receiver::Gw => {
                            gw.send(id, Err(err.into())).await?;
                        }
                    };
                }
                Err(err) => {
                    tracing::error!("{err}");
                    let err = Err(ErrorKind::Unhandled {
                        cause: format!("{err}").into(),
                    }
                    .into());
                    match receiver {
                        Receiver::Ws => {
                            ws_proxy.send(id, err).await?;
                        }
                        Receiver::Gw => {
                            gw.send(id, err).await?;
                        }
                    };
                }
            }
        }
    }
}

pub async fn serve_client_api(config: WebsocketApiConfig) -> std::io::Result<[BoxedClient; 2]> {
    let (gw, ws_proxy) = serve_client_api_in_impl(config, None).await?;
    Ok([Box::new(gw), Box::new(ws_proxy)])
}

/// Like [`serve_client_api`] but reuses a pre-bound TCP listener, avoiding the
/// release-then-rebind race window that causes port conflicts in parallel tests.
pub async fn serve_client_api_with_listener(
    config: WebsocketApiConfig,
    listener: std::net::TcpListener,
) -> std::io::Result<[BoxedClient; 2]> {
    let (gw, ws_proxy) = serve_client_api_in_impl(config, Some(listener)).await?;
    Ok([Box::new(gw), Box::new(ws_proxy)])
}

/// Like [`serve_client_api_with_listener`] but also returns the `OriginContractMap`.
///
/// Use this in integration tests that need to pre-populate auth token → contract
/// mappings in order to test delegate attestation behaviour (issue #1523).
pub async fn serve_client_api_with_listener_and_contracts(
    config: WebsocketApiConfig,
    listener: std::net::TcpListener,
) -> std::io::Result<([BoxedClient; 2], OriginContractMap)> {
    let (gw, ws_proxy) = serve_client_api_in_impl(config, Some(listener)).await?;
    let origin_contracts = gw.origin_contracts.clone();
    Ok(([Box::new(gw), Box::new(ws_proxy)], origin_contracts))
}

/// Serves the client API and returns the concrete types (for integration testing).
/// This allows tests to access internal state like the origin_contracts map.
pub async fn serve_client_api_for_test(
    config: WebsocketApiConfig,
) -> std::io::Result<(
    client_api::HttpClientApi,
    crate::client_events::websocket::WebSocketProxy,
)> {
    serve_client_api_in_impl(config, None).await
}

pub(crate) async fn serve_client_api_in(
    config: WebsocketApiConfig,
) -> std::io::Result<(HttpClientApi, WebSocketProxy)> {
    serve_client_api_in_impl(config, None).await
}

/// Hostnames and IPs accepted in the HTTP `Host` header for WebSocket connections.
pub(crate) type AllowedHosts = Arc<HashSet<String>>;

/// Builds the allowlist of hostnames/IPs for WebSocket `Host` header validation.
///
/// Each entry is stored with and without the port suffix so both
/// `Host: myhost` and `Host: myhost:7509` are accepted.
fn build_allowed_hosts(
    bind_addr: IpAddr,
    port: u16,
    extra_allowed_hosts: &[String],
) -> HashSet<String> {
    let mut hosts = HostAllowlistBuilder::new(port);

    hosts.add_localhost();
    hosts.add_machine_hostname();

    if !bind_addr.is_unspecified() {
        hosts.add(&bind_addr.to_string());
    }

    for host in extra_allowed_hosts {
        hosts.add(host);
    }

    hosts.build()
}

struct HostAllowlistBuilder {
    hosts: HashSet<String>,
    port: u16,
}

impl HostAllowlistBuilder {
    fn new(port: u16) -> Self {
        Self {
            hosts: HashSet::new(),
            port,
        }
    }

    fn add(&mut self, host: &str) {
        let host_lower = host.to_lowercase();
        self.hosts.insert(format!("{host_lower}:{}", self.port));
        self.hosts.insert(host_lower);
    }

    fn add_localhost(&mut self) {
        self.add("localhost");
        self.add("127.0.0.1");
        self.add("[::1]");
    }

    fn add_machine_hostname(&mut self) {
        let Ok(name) = hostname::get() else { return };
        let Some(name_str) = name.to_str() else {
            return;
        };

        self.add(name_str);
        self.resolve_hostname_ips(name_str);
    }

    fn resolve_hostname_ips(&mut self, hostname: &str) {
        let Ok(addrs) = std::net::ToSocketAddrs::to_socket_addrs(&(hostname, self.port)) else {
            return;
        };
        for addr in addrs {
            self.add(&addr.ip().to_string());
        }
    }

    fn build(self) -> HashSet<String> {
        self.hosts
    }
}

async fn serve_client_api_in_impl(
    config: WebsocketApiConfig,
    pre_bound: Option<std::net::TcpListener>,
) -> std::io::Result<(HttpClientApi, WebSocketProxy)> {
    let ws_socket = (config.address, config.port).into();

    // Create a shared origin_contracts map with token expiration support
    let origin_contracts: OriginContractMap = Arc::new(DashMap::new());

    // Spawn background task to clean up expired tokens
    spawn_token_cleanup_task(
        origin_contracts.clone(),
        config.token_ttl_seconds,
        config.token_cleanup_interval_seconds,
    );

    // Pass the shared map to both the HTTP client API and WebSocketProxy
    let (gw, gw_router) =
        HttpClientApi::as_router_with_origin_contracts(&ws_socket, origin_contracts.clone());
    let (ws_proxy, ws_router) =
        WebSocketProxy::create_router_with_origin_contracts(gw_router, origin_contracts);

    let allowed_hosts: AllowedHosts = Arc::new(build_allowed_hosts(
        config.address,
        config.port,
        &config.allowed_hosts,
    ));
    tracing::info!(?allowed_hosts, "WebSocket Host header allowlist built");

    // When bound to a non-loopback address, reject connections from non-private
    // source IPs. This is sufficient security: only LAN clients can connect.
    let needs_lan_filter = !config.address.is_loopback();
    let router = if needs_lan_filter {
        ws_router
            .layer(Extension(allowed_hosts))
            .layer(axum::middleware::from_fn(private_network_filter))
            .layer(TraceLayer::new_for_http())
    } else {
        ws_router
            .layer(Extension(allowed_hosts))
            .layer(TraceLayer::new_for_http())
    };

    serve_with_listener(ws_socket, router, pre_bound).await?;
    Ok((gw, ws_proxy))
}

/// Middleware that rejects requests from non-private IP addresses.
async fn private_network_filter(
    connect_info: axum::extract::ConnectInfo<SocketAddr>,
    req: axum::http::Request<axum::body::Body>,
    next: axum::middleware::Next,
) -> axum::response::Response {
    if !is_private_ip(&connect_info.0.ip()) {
        tracing::warn!(
            remote_ip = %connect_info.0.ip(),
            "Rejected connection from non-private IP"
        );
        return (
            axum::http::StatusCode::FORBIDDEN,
            "Only local network connections are allowed",
        )
            .into_response();
    }
    next.run(req).await
}

/// Spawns a background task that periodically removes expired authentication tokens.
///
/// Tokens that haven't been used for the specified TTL duration will be removed from the map.
/// This prevents memory leaks and ensures old tokens don't remain valid indefinitely.
///
/// # Arguments
/// * `origin_contracts` - The shared map of authentication tokens
/// * `token_ttl_seconds` - How long tokens remain valid without activity (in seconds)
/// * `cleanup_interval_seconds` - How often to run the cleanup task (in seconds)
fn spawn_token_cleanup_task(
    origin_contracts: OriginContractMap,
    token_ttl_seconds: u64,
    cleanup_interval_seconds: u64,
) {
    let token_ttl = Duration::from_secs(token_ttl_seconds);
    let cleanup_interval = Duration::from_secs(cleanup_interval_seconds);

    GlobalExecutor::spawn(async move {
        let mut interval = tokio::time::interval(cleanup_interval);
        interval.tick().await; // Skip the first immediate tick

        loop {
            interval.tick().await;

            // Clean up expired tokens
            let now = Instant::now();
            let initial_count = origin_contracts.len();

            // Remove tokens that haven't been accessed in token_ttl
            origin_contracts.retain(|token, origin| {
                let elapsed = now.duration_since(origin.last_accessed);
                let should_keep = elapsed < token_ttl;

                if !should_keep {
                    tracing::info!(
                        ?token,
                        contract_id = ?origin.contract_id,
                        client_id = ?origin.client_id,
                        elapsed_hours = elapsed.as_secs() / 3600,
                        "Removing expired authentication token"
                    );
                }

                should_keep
            });

            let removed_count = initial_count - origin_contracts.len();
            if removed_count > 0 {
                tracing::debug!(
                    removed_count,
                    remaining_count = origin_contracts.len(),
                    "Token cleanup completed"
                );
            }
        }
    });
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::{Ipv4Addr, Ipv6Addr};

    #[test]
    fn test_is_private_ip_v4() {
        // Loopback
        assert!(is_private_ip(&IpAddr::V4(Ipv4Addr::LOCALHOST)));
        assert!(is_private_ip(&IpAddr::V4(Ipv4Addr::new(127, 0, 0, 2))));

        // RFC 1918
        assert!(is_private_ip(&IpAddr::V4(Ipv4Addr::new(10, 0, 0, 1))));
        assert!(is_private_ip(&IpAddr::V4(Ipv4Addr::new(172, 16, 0, 1))));
        assert!(is_private_ip(&IpAddr::V4(Ipv4Addr::new(172, 31, 255, 255))));
        assert!(is_private_ip(&IpAddr::V4(Ipv4Addr::new(192, 168, 1, 2))));

        // Link-local
        assert!(is_private_ip(&IpAddr::V4(Ipv4Addr::new(169, 254, 1, 1))));

        // Unspecified (bind address)
        assert!(is_private_ip(&IpAddr::V4(Ipv4Addr::UNSPECIFIED)));

        // Public IPs — must be rejected
        assert!(!is_private_ip(&IpAddr::V4(Ipv4Addr::new(8, 8, 8, 8))));
        assert!(!is_private_ip(&IpAddr::V4(Ipv4Addr::new(1, 1, 1, 1))));
        assert!(!is_private_ip(&IpAddr::V4(Ipv4Addr::new(172, 32, 0, 1))));
        assert!(!is_private_ip(&IpAddr::V4(Ipv4Addr::new(192, 169, 0, 1))));
    }

    #[test]
    fn test_is_private_ip_v6() {
        // Loopback
        assert!(is_private_ip(&IpAddr::V6(Ipv6Addr::LOCALHOST)));

        // Unspecified
        assert!(is_private_ip(&IpAddr::V6(Ipv6Addr::UNSPECIFIED)));

        // Link-local (fe80::/10)
        assert!(is_private_ip(&IpAddr::V6(Ipv6Addr::new(
            0xfe80, 0, 0, 0, 0, 0, 0, 1
        ))));
        assert!(is_private_ip(&IpAddr::V6(Ipv6Addr::new(
            0xfebf, 0xffff, 0, 0, 0, 0, 0, 1
        ))));
        // fe40:: is NOT link-local
        assert!(!is_private_ip(&IpAddr::V6(Ipv6Addr::new(
            0xfe40, 0, 0, 0, 0, 0, 0, 1
        ))));

        // ULA (fc00::/7 — includes fd00::/8)
        assert!(is_private_ip(&IpAddr::V6(Ipv6Addr::new(
            0xfd00, 0, 0, 0, 0, 0, 0, 1
        ))));
        assert!(is_private_ip(&IpAddr::V6(Ipv6Addr::new(
            0xfc00, 0, 0, 0, 0, 0, 0, 1
        ))));
        assert!(is_private_ip(&IpAddr::V6(Ipv6Addr::new(
            0xfdff, 0xffff, 0, 0, 0, 0, 0, 1
        ))));

        // Public IPv6 — must be rejected
        assert!(!is_private_ip(&IpAddr::V6(Ipv6Addr::new(
            0x2001, 0xdb8, 0, 0, 0, 0, 0, 1
        ))));
        assert!(!is_private_ip(&IpAddr::V6(Ipv6Addr::new(
            0x2607, 0xf8b0, 0, 0, 0, 0, 0, 1
        ))));

        // IPv4-mapped IPv6 addresses (::ffff:x.x.x.x) — appear when IPv4 clients
        // connect to dual-stack sockets. Must delegate to IPv4 private checks.
        assert!(is_private_ip(
            &"::ffff:127.0.0.1".parse::<IpAddr>().unwrap()
        )); // loopback
        assert!(is_private_ip(&"::ffff:10.0.0.1".parse::<IpAddr>().unwrap())); // RFC 1918
        assert!(is_private_ip(
            &"::ffff:192.168.1.1".parse::<IpAddr>().unwrap()
        )); // RFC 1918
        assert!(!is_private_ip(&"::ffff:8.8.8.8".parse::<IpAddr>().unwrap())); // public
    }

    #[test]
    fn test_build_allowed_hosts_always_includes_localhost() {
        let hosts = build_allowed_hosts(IpAddr::V4(Ipv4Addr::LOCALHOST), 7509, &[]);
        assert!(hosts.contains("localhost"));
        assert!(hosts.contains("localhost:7509"));
        assert!(hosts.contains("127.0.0.1"));
        assert!(hosts.contains("127.0.0.1:7509"));
        assert!(hosts.contains("[::1]"));
        assert!(hosts.contains("[::1]:7509"));
    }

    #[test]
    fn test_build_allowed_hosts_includes_machine_hostname() {
        let hosts = build_allowed_hosts(IpAddr::V4(Ipv4Addr::UNSPECIFIED), 7509, &[]);
        if let Ok(name) = hostname::get() {
            if let Some(name_str) = name.to_str() {
                assert!(hosts.contains(&name_str.to_lowercase()));
            }
        }
    }

    #[test]
    fn test_build_allowed_hosts_custom_hostname() {
        let hosts = build_allowed_hosts(
            IpAddr::V4(Ipv4Addr::UNSPECIFIED),
            7509,
            &["mynode.example.com".to_string()],
        );
        assert!(hosts.contains("mynode.example.com"));
        assert!(hosts.contains("mynode.example.com:7509"));
        assert!(hosts.contains("localhost"));
    }

    #[test]
    fn test_build_allowed_hosts_specific_bind_addr() {
        let hosts = build_allowed_hosts(IpAddr::V4(Ipv4Addr::new(192, 168, 1, 50)), 7509, &[]);
        assert!(hosts.contains("192.168.1.50"));
        assert!(hosts.contains("192.168.1.50:7509"));
        assert!(hosts.contains("localhost"));
    }

    #[test]
    fn test_build_allowed_hosts_excludes_unspecified() {
        let hosts = build_allowed_hosts(IpAddr::V4(Ipv4Addr::UNSPECIFIED), 7509, &[]);
        assert!(!hosts.contains("0.0.0.0"));
        assert!(!hosts.contains("0.0.0.0:7509"));
    }

    #[test]
    fn test_build_allowed_hosts_excludes_ipv6_unspecified() {
        // :: is the new default bind address; verify it's excluded from allowlist
        // but localhost variants are still included
        let hosts = build_allowed_hosts(IpAddr::V6(Ipv6Addr::UNSPECIFIED), 7509, &[]);
        assert!(!hosts.contains("::"));
        assert!(!hosts.contains("[::]:7509"));
        assert!(hosts.contains("localhost"));
        assert!(hosts.contains("[::1]"));
        assert!(hosts.contains("127.0.0.1"));
    }
}
