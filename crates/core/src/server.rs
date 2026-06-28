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
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr};
use std::sync::Arc;
use std::time::Duration;
use tokio::time::Instant;

use dashmap::DashMap;

use freenet_stdlib::{
    client_api::{ClientError, ClientRequest, HostResponse},
    prelude::*,
};

use axum::{Extension, response::IntoResponse};
use client_api::HttpClientApi;
use tower_http::trace::TraceLayer;

use crate::{
    client_events::{AuthToken, BoxedClient, ClientId, HostResult, websocket::WebSocketProxy},
    config::{GlobalExecutor, WebsocketApiConfig},
    wasm_runtime::UserSecretContext,
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
        /// Per-connection per-user secret namespace (hosted mode, P2 of #4381),
        /// derived once at WS upgrade from the connection's user token. `None`
        /// outside hosted mode. Carried on the connection, never from the
        /// request body, so a client cannot forge another user's namespace.
        user_context: Option<UserSecretContext>,
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

/// For a wildcard or loopback bind address, returns the equivalent address in
/// the other IP family so the client API can be served on a second, explicit
/// socket and thus be reachable over both IPv4 and IPv6.
///
/// This is what makes `http://127.0.0.1:7509/` AND `http://[::1]:7509/` (and
/// `localhost`, which resolves to either) work on every OS. A single IPv6
/// socket is not enough: IPv4-mapped acceptance (`IPV6_V6ONLY=false`) applies
/// only to the `::` wildcard — never to a specific address like `::1` — and is
/// unreliable on Windows even for `::`, so the literal IPv4 loopback is refused
/// there. See issue #4330.
///
/// Returns `None` for any specific (non-wildcard, non-loopback) address: an
/// operator who binds the API to a particular interface IP gets exactly that
/// one socket, with no companion in the other family.
fn companion_bind_addr(addr: IpAddr) -> Option<IpAddr> {
    match addr {
        IpAddr::V6(v6) if v6 == Ipv6Addr::UNSPECIFIED => Some(IpAddr::V4(Ipv4Addr::UNSPECIFIED)),
        IpAddr::V6(v6) if v6 == Ipv6Addr::LOCALHOST => Some(IpAddr::V4(Ipv4Addr::LOCALHOST)),
        IpAddr::V4(v4) if v4 == Ipv4Addr::UNSPECIFIED => Some(IpAddr::V6(Ipv6Addr::UNSPECIFIED)),
        IpAddr::V4(v4) if v4 == Ipv4Addr::LOCALHOST => Some(IpAddr::V6(Ipv6Addr::LOCALHOST)),
        // A specific (non-wildcard, non-loopback) interface address is bound
        // as-is, with no companion. Enumerate variants rather than `_` to
        // satisfy `clippy::wildcard_enum_match_arm`.
        IpAddr::V4(_) | IpAddr::V6(_) => None,
    }
}

async fn serve_with_listener(
    socket: SocketAddr,
    router: axum::Router,
    pre_bound: Option<std::net::TcpListener>,
) -> std::io::Result<tokio::task::AbortHandle> {
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
                // Bind IPv6 sockets v6-only. We do NOT rely on single-socket
                // dual-stack (IPV6_V6ONLY=false / IPv4-mapped addresses) for the
                // client API: that only delivers IPv4 to a socket bound to the
                // `::` wildcard (never to a specific address like `::1`), and is
                // unreliable on Windows even for `::`. Instead, `serve_client_api`
                // binds an explicit companion socket in the other family (see
                // `companion_bind_addr`) so literal `127.0.0.1` AND `::1` /
                // `localhost` are reachable on every OS. v6-only here keeps the
                // two sockets from contending for the IPv4 space (a v4-mapped
                // `::` socket would make the companion `0.0.0.0` bind fail with
                // AddrInUse on Linux).
                if is_ipv6 {
                    sock.set_only_v6(true)?;
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
    // Retain the spawn's AbortHandle so the node can tear the server down on
    // graceful shutdown (issue #4401). Dropping the JoinHandle alone does NOT
    // abort the task, so without this the server keeps serving on the bound
    // port after the node has shut down.
    let handle = GlobalExecutor::spawn(async move {
        axum::serve(
            listener,
            router.into_make_service_with_connect_info::<SocketAddr>(),
        )
        .await
        .map_err(|e| {
            tracing::error!("Error while running HTTP client API server: {e}");
        })
    });
    Ok(handle.abort_handle())
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
        client_events::{ClientEventsProxy, OpenRequest, websocket::WebSocketProxy},
        contract::{Executor, ExecutorError},
    };

    use super::{client_api::HttpClientApi, serve_dual_stack};

    pub async fn run_local_node(mut executor: Executor, socket: SocketAddr) -> anyhow::Result<()> {
        if !super::is_private_ip(&socket.ip()) {
            anyhow::bail!(
                "invalid ip: {}, only loopback and private network addresses are allowed",
                socket.ip()
            )
        }
        let (mut gw, gw_router) = HttpClientApi::as_router(&socket);
        let (mut ws_proxy, ws_router) = WebSocketProxy::create_router(gw_router);

        // Route through serve_dual_stack (not the bare single-socket bind) so
        // this path also binds an IPv4+IPv6 companion pair for loopback/wildcard
        // addresses — keeping every client-API serve path reachable over both
        // families (#4330).
        //
        // `run_local_node` owns the server for the lifetime of the process (it
        // loops forever below), so the abort guard is leaked deliberately: there
        // is no graceful-shutdown hook on this path to hand it to.
        let mut aborts = crate::util::AbortOnDrop::new();
        serve_dual_stack(
            socket,
            ws_router.layer(TraceLayer::new_for_http()),
            None,
            &mut aborts,
        )
        .await?;
        std::mem::forget(aborts);

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
                user_context,
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
                    // `user_context` is `Some` only in hosted mode with a user
                    // token; otherwise `None` keeps secrets `SecretScope::Local`.
                    executor.delegate_request(
                        op,
                        origin_contract.as_ref(),
                        None,
                        user_context.as_ref(),
                    )
                }
                ClientRequest::Disconnect { cause } => {
                    if let Some(cause) = cause {
                        tracing::info!("disconnecting cause: {cause}");
                    }
                    // fixme: token must live for a bit to allow reconnections
                    // Drop the iter() guard before remove() to avoid holding a
                    // DashMap shard guard across a mutating call on the same map
                    // (clippy: `significant_drop_in_scrutinee`).
                    // Safe: the outer caller serialises by `id`, so no concurrent
                    // iter+remove on this shard is possible.
                    let rm_token = gw.origin_contracts.iter().find_map(|entry| {
                        let (k, origin) = entry.pair();
                        (origin.client_id == id).then(|| k.clone())
                    });
                    if let Some(rm_token) = rm_token {
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

/// Whether this node runs in hosted mode (P2 of #4381). Injected as an axum
/// `Extension` so the `connection_info` middleware can decide whether to honor
/// a connection's `userToken` and derive a per-user secret namespace. Defaults
/// to `false`; only `--hosted-mode` / `hosted-mode = true` turns it on.
#[derive(Clone, Copy, Default)]
pub(crate) struct HostedMode(pub bool);

/// The node's secrets dir, injected as an axum `Extension` so the WS hook can
/// stamp per-user last-activity markers (#4561, P5 of #4381, inactive-user
/// TTL). `Arc<PathBuf>` so cloning it onto every connection is cheap. An EMPTY
/// path means "no secrets tree to mark" (the standalone / non-hosted test
/// composition) and disables stamping; the real node serve path carries the
/// resolved `config.secrets_dir`. Stamping is additionally gated on
/// `user_context.is_some()` in the hook, so a Local connection never writes a
/// marker even if a path is present.
#[derive(Clone, Default)]
pub(crate) struct ActivitySecretsDir(pub Arc<std::path::PathBuf>);

/// User-supplied source CIDRs that extend the built-in private-IP allowlist.
///
/// The filter accepts a request if the source IP is private (loopback / RFC1918 /
/// link-local / IPv6 ULA) **or** matches any of these ranges. Empty by default;
/// populated via `--allowed-source-cidrs` or `allowed-source-cidrs` in config.toml.
#[derive(Clone, Default)]
pub(crate) struct AllowedSourceCidrs(pub Arc<Vec<ipnet::IpNet>>);

impl AllowedSourceCidrs {
    pub(crate) fn contains_ip(&self, ip: &IpAddr) -> bool {
        self.0.iter().any(|net| net.contains(ip))
    }
}

/// Minimum IPv4 CIDR prefix length the operator allowlist will accept.
///
/// `/8` is the broadest reasonable range: it still covers `10.0.0.0/8`
/// (RFC1918) and `100.64.0.0/10` (CGNAT / Tailscale). Anything shorter
/// (`/7` through `/0`) would span enormous public space — `0.0.0.0/0`
/// would expose the fully-privileged client API to the entire internet
/// — so we refuse to load such configs at all rather than trust the
/// operator typed what they meant.
const MIN_IPV4_PREFIX_LEN: u8 = 8;

/// Minimum IPv6 CIDR prefix length accepted for the same reason.
/// `/16` still accommodates a /48 tailnet, /56 Hurricane Electric subnet,
/// or /64 home LAN while refusing `::/0`-sized footguns.
const MIN_IPV6_PREFIX_LEN: u8 = 16;

/// Validates a single operator-supplied CIDR for the local-API allowlist.
///
/// Returns an error if the prefix length is shorter than
/// [`MIN_IPV4_PREFIX_LEN`] / [`MIN_IPV6_PREFIX_LEN`], which would widen the
/// trust boundary past anything the operator could plausibly own. This is a
/// safety net, not a substitute for good judgment: a user can still pass
/// `8.8.0.0/16` and reach the public internet on purpose. We just refuse to
/// silently accept whole-internet footguns like `0.0.0.0/0`.
pub fn validate_source_cidr(net: &ipnet::IpNet) -> Result<(), String> {
    let (prefix, min) = match net {
        ipnet::IpNet::V4(v4) => (v4.prefix_len(), MIN_IPV4_PREFIX_LEN),
        ipnet::IpNet::V6(v6) => (v6.prefix_len(), MIN_IPV6_PREFIX_LEN),
    };
    if prefix < min {
        return Err(format!(
            "CIDR `{net}` has prefix /{prefix}; minimum accepted is /{min}. \
             Shorter prefixes would trust too large a range for a \
             fully-privileged local API."
        ));
    }
    Ok(())
}

/// Pure decision function for the source-IP filter.
///
/// Extracted from [`private_network_filter`] so the boolean composition can
/// be unit-tested directly — catching inverted-operator regressions that
/// would turn a security bypass into a silent test pass.
///
/// The filter accepts traffic when the source IP is private (loopback,
/// RFC1918, IPv6 ULA / link-local) **or** when it matches an operator-
/// supplied CIDR. IPv4-mapped IPv6 sources (`::ffff:a.b.c.d`) are normalized
/// to IPv4 before the CIDR check so operators can write CIDRs in natural v4
/// notation even on a dual-stack socket. (`is_private_ip` already handles
/// mapped-v6 internally.)
pub(crate) fn is_source_allowed(ip: IpAddr, allowed: &AllowedSourceCidrs) -> bool {
    if is_private_ip(&ip) {
        return true;
    }
    let match_ip = match ip {
        IpAddr::V6(v6) => v6
            .to_ipv4_mapped()
            .map(IpAddr::V4)
            .unwrap_or(IpAddr::V6(v6)),
        v4 => v4,
    };
    allowed.contains_ip(&match_ip)
}

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

    // Collects the AbortHandles of every detached task this function spawns
    // (token cleanup + the `axum::serve` server socket(s)) so they are torn
    // down when the returned `WebSocketProxy` is dropped. The proxy is one of
    // the node's `BoxedClient`s, so aborting the node's client-events task on
    // shutdown drops the proxy and, with it, these server tasks — releasing the
    // bound ports instead of leaving a stale server answering requests meant
    // for a restarted node. See issue #4401.
    let mut server_aborts = crate::util::AbortOnDrop::new();

    // Create a shared origin_contracts map with token expiration support
    let origin_contracts: OriginContractMap = Arc::new(DashMap::new());

    // Spawn background task to clean up expired tokens
    server_aborts.push(spawn_token_cleanup_task(
        origin_contracts.clone(),
        config.token_ttl_seconds,
        config.token_cleanup_interval_seconds,
    ));

    // Pass the shared map to both the HTTP client API and WebSocketProxy
    let (gw, gw_router) = HttpClientApi::as_router_with_origin_contracts(
        &ws_socket,
        origin_contracts.clone(),
        crate::contract::user_input::pending_prompts(),
    );
    let (ws_proxy, ws_router) =
        WebSocketProxy::create_router_with_origin_contracts(gw_router, origin_contracts);

    let allowed_hosts: AllowedHosts = Arc::new(build_allowed_hosts(
        config.address,
        config.port,
        &config.allowed_hosts,
    ));
    tracing::info!(?allowed_hosts, "WebSocket Host header allowlist built");

    let allowed_source_cidrs = AllowedSourceCidrs(Arc::new(config.allowed_source_cidrs.clone()));
    // Log each range on its own line so ops journald/grep output stays
    // legible. `warn!` because granting non-private access to a fully-
    // privileged API is a posture change the operator should see on every
    // boot, not a silent tweak buried in info.
    for cidr in allowed_source_cidrs.0.iter() {
        tracing::warn!(
            %cidr,
            "Local API source CIDR enabled: ensure this range is fully under \
             your control. Anything reachable in it can access contract \
             state and keys."
        );
    }

    // When bound to a non-loopback address, reject connections from non-private
    // source IPs. Users may extend the allowlist with --allowed-source-cidrs
    // (e.g. for a Tailscale tailnet range).
    let hosted_mode = HostedMode(config.hosted_mode);
    if hosted_mode.0 {
        // Posture change the operator should see on every boot: hosted mode
        // lets untrusted connections claim per-user secret namespaces via the
        // `userToken` parameter. warn! (not info!) so it stands out in logs.
        tracing::warn!(
            "Hosted mode ENABLED: WebSocket connections presenting a `userToken` \
             get a per-user delegate-secret namespace. Only run this on a node \
             intended as a shared public proxy."
        );
    }

    // Per-user operation + export rate limiter (#4561, P5 of #4381). Built once
    // here (the only place the operator config is in scope) and injected as an
    // `Extension` on BOTH the WS path (`process_client_request` consults it for
    // contract ops) and the HTTP export handler (per-user export throttle). The
    // SAME `Arc` is shared across every connection, so one user's many
    // connections share their bucket; nodes don't collide because each node
    // owns its own router Extension. Op rate limiting only ever fires for a
    // connection that derives a `UserSecretContext` (hosted mode), so the
    // single-user / non-hosted path is never throttled even though the
    // Extension is present.
    let op_rate_limiter = crate::client_events::user_op_rate_limit::UserOpRateLimitConfig {
        op_rate: config.per_user_op_rate_limit,
        op_burst: config.per_user_op_burst,
        export_min_interval_secs: config.per_user_export_min_interval_secs,
    }
    .build();
    if hosted_mode.0 && op_rate_limiter.op_limiting_enabled() {
        tracing::info!(
            op_rate = config.per_user_op_rate_limit,
            op_burst = config.per_user_op_burst,
            export_min_interval_secs = config.per_user_export_min_interval_secs,
            "Hosted per-user operation rate limiting enabled"
        );
    }

    // Per-user last-activity marker root (#4561, P5 of #4381, inactive-user
    // TTL). Injected as an `Extension` so the WS hook can stamp
    // `<secrets_dir>/users/<user_id>/.last_seen` on connect + each request,
    // keeping the activity record the reclaim sweep reads up to date. An empty
    // path (the standalone/test composition default) disables stamping; only
    // the real node serve path carries a resolved `secrets_dir`. Stamping is
    // ALSO gated on `user_context.is_some()` in the hook, so a non-hosted
    // connection never writes a marker even if a path is present.
    let activity_secrets_dir = ActivitySecretsDir(Arc::new(config.secrets_dir.clone()));

    let needs_lan_filter = !config.address.is_loopback();
    let router = if needs_lan_filter {
        // Layer ordering matters: axum executes layers bottom-to-top per
        // `Router::layer` docs, so the LAST `.layer(..)` call is the
        // OUTERMOST and runs first in request flow. `private_network_filter`
        // uses an `Extension<AllowedSourceCidrs>` extractor, so that
        // Extension layer must be applied AFTER the `from_fn` layer here —
        // otherwise the extension is injected after the filter runs and
        // every request 500s because the extractor finds nothing. Docker
        // NAT tests caught this (see regression in this module's `tests`
        // submodule, `middleware_has_extension_injected`).
        //
        // `connection_info` (inside the WebSocket router) also extracts
        // `Extension<AllowedSourceCidrs>` for the Host-header CIDR check.
        ws_router
            .layer(Extension(hosted_mode))
            .layer(Extension(op_rate_limiter))
            .layer(Extension(activity_secrets_dir))
            .layer(Extension(allowed_hosts))
            .layer(axum::middleware::from_fn(private_network_filter))
            .layer(Extension(allowed_source_cidrs))
            .layer(TraceLayer::new_for_http())
    } else {
        // Loopback binds still inject an (empty) `AllowedSourceCidrs` so
        // `connection_info` always finds the extractor; missing it would
        // 500 every WS upgrade.
        ws_router
            .layer(Extension(hosted_mode))
            .layer(Extension(op_rate_limiter))
            .layer(Extension(activity_secrets_dir))
            .layer(Extension(allowed_hosts))
            .layer(Extension(allowed_source_cidrs))
            .layer(TraceLayer::new_for_http())
    };

    serve_dual_stack(ws_socket, router, pre_bound, &mut server_aborts).await?;

    // Hand ownership of the server tasks' abort guard to the proxy so they are
    // torn down when the proxy is dropped on node shutdown (issue #4401).
    let ws_proxy = ws_proxy.with_server_aborts(server_aborts);
    Ok((gw, ws_proxy))
}

/// Serve `router` on `primary`, and — when `primary` is a wildcard/loopback
/// address whose bind we own — also on a companion socket in the other IP
/// family (see [`companion_bind_addr`]). This is what makes the client API
/// reachable over both `127.0.0.1` and `::1`/`localhost` on every OS (#4330).
///
/// A caller-provided `pre_bound` listener is used as-is, with no companion: the
/// caller chose that exact socket. The companion bind is best-effort — if the
/// other family is unavailable on this host (e.g. IPv4 or IPv6 disabled), the
/// node keeps serving on the primary rather than failing to start. A failure to
/// bind the primary is still fatal.
async fn serve_dual_stack(
    primary: SocketAddr,
    router: axum::Router,
    pre_bound: Option<std::net::TcpListener>,
    aborts: &mut crate::util::AbortOnDrop,
) -> std::io::Result<()> {
    let companion = if pre_bound.is_none() {
        companion_bind_addr(primary.ip()).map(|ip| SocketAddr::from((ip, primary.port())))
    } else {
        None
    };
    match companion {
        Some(companion) => {
            aborts.push(serve_with_listener(primary, router.clone(), pre_bound).await?);
            match serve_with_listener(companion, router, None).await {
                Ok(handle) => aborts.push(handle),
                Err(e) => tracing::warn!(
                    %companion,
                    primary = %primary,
                    error = %e,
                    "Could not bind companion {} listener for the client API; \
                     continuing with the primary listener only",
                    if companion.is_ipv4() { "IPv4" } else { "IPv6" },
                ),
            }
        }
        None => {
            aborts.push(serve_with_listener(primary, router, pre_bound).await?);
        }
    }
    // The primary client-API socket is now bound (a primary bind failure is
    // fatal above, so reaching here means we own the port). That is the
    // authoritative "node recovered the port" signal: clear any stuck-wrapper
    // banner a now-dead supervising wrapper left behind so a freshly-recovered
    // node does not keep showing it (#4288).
    crate::service_status::clear_stuck_status_on_startup();
    Ok(())
}

/// Middleware that rejects requests from non-private IP addresses unless the
/// source IP matches an operator-supplied CIDR in [`AllowedSourceCidrs`].
async fn private_network_filter(
    connect_info: axum::extract::ConnectInfo<SocketAddr>,
    Extension(allowed_source_cidrs): Extension<AllowedSourceCidrs>,
    req: axum::http::Request<axum::body::Body>,
    next: axum::middleware::Next,
) -> axum::response::Response {
    let ip = connect_info.0.ip();
    if !is_source_allowed(ip, &allowed_source_cidrs) {
        tracing::warn!(
            remote_ip = %ip,
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
) -> tokio::task::AbortHandle {
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
    })
    .abort_handle()
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

    fn cidrs(list: &[&str]) -> AllowedSourceCidrs {
        AllowedSourceCidrs(Arc::new(list.iter().map(|s| s.parse().unwrap()).collect()))
    }

    /// Regression test for a layer-ordering bug that was only caught by the
    /// Docker NAT CI job: axum executes `.layer(..)` calls bottom-to-top, so
    /// the LAST `.layer(..)` call is the OUTERMOST and runs FIRST. If
    /// `Extension(AllowedSourceCidrs)` is applied before `from_fn(filter)`
    /// in the builder chain, the filter's `Extension<AllowedSourceCidrs>`
    /// extractor runs before the extension is injected and every request
    /// 500s — silently breaking every HTTP client including the in-test
    /// connectivity probe. The test below builds the exact layer stack from
    /// `serve_client_api_in_impl` and drives it with a synthetic request
    /// from both an allowlisted source and a public one; if someone
    /// reorders the layers again, the allowlisted request will stop
    /// returning 200 and this test fails.
    #[tokio::test]
    async fn middleware_layer_stack_allows_configured_source() {
        use axum::{Router, routing::get};
        use tower::ServiceExt;

        async fn handler() -> &'static str {
            "ok"
        }

        let allowed = cidrs(&["100.64.0.0/10"]);
        let app: Router = Router::new()
            .route("/", get(handler))
            .layer(axum::middleware::from_fn(private_network_filter))
            .layer(Extension(allowed));

        // A request with a ConnectInfo from inside the allowlist must
        // pass through the middleware and reach the handler.
        let req = axum::http::Request::builder()
            .uri("/")
            .extension(axum::extract::ConnectInfo(SocketAddr::from((
                [100, 64, 0, 1],
                12345,
            ))))
            .body(axum::body::Body::empty())
            .unwrap();
        let resp = app.clone().oneshot(req).await.unwrap();
        assert_eq!(
            resp.status(),
            axum::http::StatusCode::OK,
            "allowlisted CGNAT source must reach the handler; \
             500 here means the middleware failed to extract the Extension \
             (check layer ordering in serve_client_api_in_impl)"
        );

        // Public source outside the allowlist must be rejected with 403.
        let req = axum::http::Request::builder()
            .uri("/")
            .extension(axum::extract::ConnectInfo(SocketAddr::from((
                [8, 8, 8, 8],
                12345,
            ))))
            .body(axum::body::Body::empty())
            .unwrap();
        let resp = app.clone().oneshot(req).await.unwrap();
        assert_eq!(resp.status(), axum::http::StatusCode::FORBIDDEN);

        // Loopback with empty allowlist (separate router) must still pass
        // via the private-IP branch — the OR composition guard.
        let empty_app: Router = Router::new()
            .route("/", get(handler))
            .layer(axum::middleware::from_fn(private_network_filter))
            .layer(Extension(AllowedSourceCidrs::default()));
        let req = axum::http::Request::builder()
            .uri("/")
            .extension(axum::extract::ConnectInfo(SocketAddr::from((
                [127, 0, 0, 1],
                12345,
            ))))
            .body(axum::body::Body::empty())
            .unwrap();
        let resp = empty_app.oneshot(req).await.unwrap();
        assert_eq!(resp.status(), axum::http::StatusCode::OK);
    }

    #[test]
    fn allowed_source_cidrs_empty_rejects_public() {
        let allow = AllowedSourceCidrs::default();
        assert!(!allow.contains_ip(&IpAddr::V4(Ipv4Addr::new(8, 8, 8, 8))));
        // Empty list means default-deny. Private IPs still pass via is_private_ip
        // at the call site, so we only assert the CIDR layer here.
        assert!(!allow.contains_ip(&IpAddr::V4(Ipv4Addr::LOCALHOST)));
    }

    #[test]
    fn allowed_source_cidrs_tailscale_cgnat() {
        let allow = cidrs(&["100.64.0.0/10"]);
        // Tailscale tailnet IPs
        assert!(allow.contains_ip(&IpAddr::V4(Ipv4Addr::new(100, 64, 0, 1))));
        assert!(allow.contains_ip(&IpAddr::V4(Ipv4Addr::new(100, 100, 50, 1))));
        assert!(allow.contains_ip(&IpAddr::V4(Ipv4Addr::new(100, 127, 255, 254))));
        // Outside CGNAT
        assert!(!allow.contains_ip(&IpAddr::V4(Ipv4Addr::new(100, 128, 0, 1))));
        assert!(!allow.contains_ip(&IpAddr::V4(Ipv4Addr::new(100, 63, 255, 255))));
        assert!(!allow.contains_ip(&IpAddr::V4(Ipv4Addr::new(8, 8, 8, 8))));
    }

    #[test]
    fn allowed_source_cidrs_narrow_tailnet() {
        // A user pinning to their assigned tailnet subnet only
        let allow = cidrs(&["100.64.1.0/24"]);
        assert!(allow.contains_ip(&IpAddr::V4(Ipv4Addr::new(100, 64, 1, 5))));
        assert!(!allow.contains_ip(&IpAddr::V4(Ipv4Addr::new(100, 64, 2, 5))));
    }

    #[test]
    fn allowed_source_cidrs_ipv6() {
        let allow = cidrs(&["fd7a:115c:a1e0::/48"]);
        assert!(allow.contains_ip(&IpAddr::V6(Ipv6Addr::new(
            0xfd7a, 0x115c, 0xa1e0, 0, 0, 0, 0, 1
        ))));
        assert!(!allow.contains_ip(&IpAddr::V6(Ipv6Addr::new(
            0xfd7a, 0x115c, 0xa1e1, 0, 0, 0, 0, 1
        ))));
    }

    // Middleware decision tests: exercise the full boolean composition of
    // `is_source_allowed`, not just `AllowedSourceCidrs::contains`. An
    // inverted operator in the middleware would still pass the bare
    // `contains` tests above, so these are the regression guards that
    // actually prevent a security bypass.

    #[test]
    fn is_source_allowed_accepts_private_with_empty_allowlist() {
        let empty = AllowedSourceCidrs::default();
        assert!(is_source_allowed(IpAddr::V4(Ipv4Addr::LOCALHOST), &empty));
        assert!(is_source_allowed(
            IpAddr::V4(Ipv4Addr::new(10, 0, 0, 1)),
            &empty
        ));
        assert!(is_source_allowed(
            IpAddr::V4(Ipv4Addr::new(192, 168, 1, 5)),
            &empty
        ));
        assert!(is_source_allowed(IpAddr::V6(Ipv6Addr::LOCALHOST), &empty));
    }

    #[test]
    fn is_source_allowed_rejects_public_with_empty_allowlist() {
        let empty = AllowedSourceCidrs::default();
        assert!(!is_source_allowed(
            IpAddr::V4(Ipv4Addr::new(8, 8, 8, 8)),
            &empty
        ));
        assert!(!is_source_allowed(
            IpAddr::V4(Ipv4Addr::new(100, 64, 0, 1)),
            &empty
        ));
        assert!(!is_source_allowed(
            IpAddr::V4(Ipv4Addr::new(203, 0, 113, 42)),
            &empty
        ));
    }

    #[test]
    fn is_source_allowed_accepts_configured_tailscale_range() {
        let tailnet = cidrs(&["100.64.0.0/10"]);
        assert!(is_source_allowed(
            IpAddr::V4(Ipv4Addr::new(100, 64, 0, 1)),
            &tailnet
        ));
        assert!(is_source_allowed(
            IpAddr::V4(Ipv4Addr::new(100, 127, 0, 1)),
            &tailnet
        ));
        // Outside the tailnet range, not private: must still reject.
        assert!(!is_source_allowed(
            IpAddr::V4(Ipv4Addr::new(100, 128, 0, 1)),
            &tailnet
        ));
        assert!(!is_source_allowed(
            IpAddr::V4(Ipv4Addr::new(8, 8, 8, 8)),
            &tailnet
        ));
        // Private IPs still pass alongside the CIDR.
        assert!(is_source_allowed(
            IpAddr::V4(Ipv4Addr::new(192, 168, 1, 5)),
            &tailnet
        ));
    }

    #[test]
    fn is_source_allowed_normalizes_ipv4_mapped_ipv6_for_cidr_match() {
        // An IPv4 client arriving on a dual-stack socket presents as
        // ::ffff:a.b.c.d. Operators expect their v4 CIDRs to still match.
        let tailnet = cidrs(&["100.64.0.0/10"]);
        let mapped = IpAddr::V6(Ipv4Addr::new(100, 64, 0, 1).to_ipv6_mapped());
        assert!(is_source_allowed(mapped, &tailnet));

        // And the same normalization must NOT accidentally promote a
        // public mapped v4 into the private set: ::ffff:8.8.8.8 stays
        // rejected with an empty allowlist, and public with an unrelated
        // allowlist.
        let public_mapped = IpAddr::V6(Ipv4Addr::new(8, 8, 8, 8).to_ipv6_mapped());
        assert!(!is_source_allowed(
            public_mapped,
            &AllowedSourceCidrs::default()
        ));
        assert!(!is_source_allowed(public_mapped, &tailnet));
    }

    #[test]
    fn is_source_allowed_accepts_configured_ipv6_range() {
        let tailnet_v6 = cidrs(&["fd7a:115c:a1e0::/48"]);
        // fd7a:115c:a1e0::/48 is inside fc00::/7 (ULA) so is_private_ip
        // already accepts it — the allowlist is a no-op for this case but
        // the test documents the intent and guards against a future change
        // that narrows `is_private_ip` and breaks this invariant.
        let inside = IpAddr::V6(Ipv6Addr::new(0xfd7a, 0x115c, 0xa1e0, 0x0001, 0, 0, 0, 1));
        assert!(is_source_allowed(inside, &tailnet_v6));
    }

    #[test]
    fn validate_source_cidr_rejects_overly_broad_ipv4() {
        // Whole-internet footguns must be rejected at parse time.
        assert!(validate_source_cidr(&"0.0.0.0/0".parse().unwrap()).is_err());
        assert!(validate_source_cidr(&"0.0.0.0/7".parse().unwrap()).is_err());
        // /8 is the minimum accepted (covers 10.0.0.0/8 RFC1918).
        assert!(validate_source_cidr(&"10.0.0.0/8".parse().unwrap()).is_ok());
        // /10 allows Tailscale CGNAT.
        assert!(validate_source_cidr(&"100.64.0.0/10".parse().unwrap()).is_ok());
        // /32 (single host) is fine.
        assert!(validate_source_cidr(&"203.0.113.5/32".parse().unwrap()).is_ok());
    }

    #[test]
    fn validate_source_cidr_rejects_overly_broad_ipv6() {
        assert!(validate_source_cidr(&"::/0".parse().unwrap()).is_err());
        assert!(validate_source_cidr(&"::/15".parse().unwrap()).is_err());
        assert!(validate_source_cidr(&"::/16".parse().unwrap()).is_ok());
        assert!(validate_source_cidr(&"fd7a:115c:a1e0::/48".parse().unwrap()).is_ok());
        assert!(validate_source_cidr(&"::1/128".parse().unwrap()).is_ok());
    }

    #[test]
    fn validate_source_cidr_error_message_is_actionable() {
        let err = validate_source_cidr(&"0.0.0.0/0".parse().unwrap()).unwrap_err();
        assert!(err.contains("0.0.0.0/0"), "should quote the offending CIDR");
        assert!(err.contains("/0"), "should state the offending prefix");
        assert!(err.contains("/8"), "should state the minimum accepted");
    }

    #[test]
    fn allowed_source_cidrs_multiple_ranges() {
        let allow = cidrs(&["100.64.0.0/10", "10.100.0.0/16"]);
        assert!(allow.contains_ip(&IpAddr::V4(Ipv4Addr::new(100, 64, 1, 1))));
        assert!(allow.contains_ip(&IpAddr::V4(Ipv4Addr::new(10, 100, 5, 5))));
        assert!(!allow.contains_ip(&IpAddr::V4(Ipv4Addr::new(10, 101, 0, 1))));
    }

    #[test]
    fn allowed_source_cidrs_does_not_accept_public_by_default() {
        // Regression guard: the default (no CIDRs configured) MUST NOT
        // trust CGNAT or any public space. Users must opt in explicitly.
        let allow = AllowedSourceCidrs::default();
        for ip in [
            Ipv4Addr::new(100, 64, 0, 1),   // CGNAT
            Ipv4Addr::new(8, 8, 8, 8),      // Public
            Ipv4Addr::new(203, 0, 113, 42), // Public (TEST-NET-3)
        ] {
            assert!(
                !allow.contains_ip(&IpAddr::V4(ip)),
                "{ip} must not be trusted by default",
            );
        }
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

    #[test]
    fn companion_bind_addr_maps_loopback_and_wildcard_across_families() {
        // Loopback and wildcard get a companion in the other family so the API
        // is reachable over both IPv4 and IPv6.
        assert_eq!(
            companion_bind_addr(IpAddr::V6(Ipv6Addr::LOCALHOST)),
            Some(IpAddr::V4(Ipv4Addr::LOCALHOST)),
        );
        assert_eq!(
            companion_bind_addr(IpAddr::V6(Ipv6Addr::UNSPECIFIED)),
            Some(IpAddr::V4(Ipv4Addr::UNSPECIFIED)),
        );
        assert_eq!(
            companion_bind_addr(IpAddr::V4(Ipv4Addr::LOCALHOST)),
            Some(IpAddr::V6(Ipv6Addr::LOCALHOST)),
        );
        assert_eq!(
            companion_bind_addr(IpAddr::V4(Ipv4Addr::UNSPECIFIED)),
            Some(IpAddr::V6(Ipv6Addr::UNSPECIFIED)),
        );

        // A specific interface address gets no companion: the operator chose
        // exactly that socket.
        assert_eq!(
            companion_bind_addr(IpAddr::V4(Ipv4Addr::new(192, 168, 1, 50))),
            None,
        );
        assert_eq!(
            companion_bind_addr(IpAddr::V6(Ipv6Addr::new(
                0xfd7a, 0x115c, 0xa1e0, 0, 0, 0, 0, 1
            ))),
            None,
        );
    }

    /// Regression test for #4330: when the client API binds the local-mode
    /// default (`::1`), the literal IPv4 loopback `127.0.0.1` must ALSO be
    /// reachable. A single IPv6 socket bound to the *specific* address `::1`
    /// never accepts IPv4 (IPv4-mapped acceptance is wildcard-only), so before
    /// the dual-family bind a Windows browser opening `http://127.0.0.1:7509/`
    /// got "can't access this site". This asserts both families connect on the
    /// same port; it fails if the companion bind is dropped OR if the IPv6
    /// socket reverts to relying on `IPV6_V6ONLY=false`.
    #[tokio::test]
    async fn client_api_reachable_on_both_ipv4_and_ipv6_loopback() {
        use axum::{Router, routing::get};

        // Distinctive marker so the assertion confirms OUR server answered, not
        // some unrelated process that happens to hold the port.
        const MARKER: &str = "dual-stack-marker-4330";
        async fn handler() -> &'static str {
            MARKER
        }

        // Grab an OS-assigned port on the IPv6 loopback, then reuse it for the
        // dual-family bind (serve_with_listener sets SO_REUSEADDR).
        let probe = std::net::TcpListener::bind((Ipv6Addr::LOCALHOST, 0)).unwrap();
        let port = probe.local_addr().unwrap().port();
        drop(probe);

        let primary = SocketAddr::from((Ipv6Addr::LOCALHOST, port));
        let router = Router::new().route("/", get(handler));
        let mut aborts = crate::util::AbortOnDrop::new();
        serve_dual_stack(primary, router, None, &mut aborts)
            .await
            .expect("binding the IPv6 loopback primary must succeed");
        // Keep the servers alive for the duration of the assertions below; the
        // guard would otherwise abort them as soon as this scope's locals drop.
        std::mem::forget(aborts);

        // GET over BOTH families and assert OUR handler answered. Checking the
        // body (not just a TCP connect) rules out a false pass where an
        // unrelated process holds 127.0.0.1:port and the best-effort companion
        // bind silently failed: only our companion socket returns MARKER.
        // `127.0.0.1` is the #4330 regression surface — connection refused (or a
        // wrong body) here means the dual-family bind regressed.
        for url in [
            format!("http://127.0.0.1:{port}/"),
            format!("http://[::1]:{port}/"),
        ] {
            let body = reqwest::get(&url)
                .await
                .unwrap_or_else(|e| panic!("GET {url} must connect ({e}); see #4330"))
                .text()
                .await
                .unwrap();
            assert_eq!(body, MARKER, "wrong server answered {url}");
        }
    }
}
