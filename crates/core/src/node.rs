//! The main node data type which encapsulates all the behaviour for maintaining a connection
//! and performing operations within the network.
//!
//! This module contains the primary event loop (`NodeP2P::run_node`) that orchestrates
//! interactions between different components like the network, operations, contracts, and clients.
//! It receives events and dispatches actions via channels.
//!
//! # Implementations
//! Node comes with different underlying implementations that can be used upon construction.
//! Those implementations are:
//! - libp2p: all the connection is handled by libp2p.
//! - in-memory: a simplifying node used for emulation purposes mainly.
//! - inter-process: similar to in-memory, but can be rana cross multiple processes, closer to the real p2p impl
//!
//! The main node data structure and execution loop.
//! See [`../../architecture.md`](../../architecture.md) for a high-level overview of the node's role and the event loop interactions.

use anyhow::Context;
use freenet_stdlib::{
    client_api::{ClientRequest, ErrorKind},
    prelude::ContractInstanceId,
};
use std::{
    borrow::Cow,
    fs::File,
    io::Read,
    net::{IpAddr, SocketAddr, ToSocketAddrs},
    sync::Arc,
    time::Duration,
};
use std::{collections::HashSet, convert::Infallible};

use self::p2p_impl::NodeP2P;
use crate::{
    client_events::{BoxedClient, ClientEventsProxy, ClientId, OpenRequest},
    config::{Address, GatewayConfig, WebsocketApiConfig},
    contract::{ExecutorError, NetworkContractHandler},
    local_node::Executor,
    message::{InnerMessage, NetMessage, NodeEvent, Transaction, TransactionType},
    operations::{OpError, connect, get, put, subscribe, update},
    ring::{Location, PeerKeyLocation},
    tracing::{EventRegister, NetEventLog, NetEventRegister},
};
use crate::{
    config::Config,
    message::{MessageStats, NetMessageV1},
};
use freenet_stdlib::client_api::DelegateRequest;
use serde::{Deserialize, Serialize};

pub(crate) use network_bridge::{
    ConnectionError, EventLoopNotificationsSender, NetworkBridge, OpExecutionPayload,
};
#[cfg(test)]
pub(crate) use network_bridge::{EventLoopNotificationsReceiver, event_loop_notification_channel};
// Re-export types for dev_tool and testing
pub use network_bridge::{EventLoopExitReason, NetworkStats, reset_channel_id_counter};

use crate::topology::rate::Rate;
use crate::transport::{TransportKeypair, TransportPublicKey};
pub(crate) use op_state_manager::OpManager;

mod network_bridge;

// Re-export fault injection types for test infrastructure.
// No cfg gate: underlying items are unconditionally compiled and integration
// tests compile the lib without cfg(test).
pub use network_bridge::in_memory::{FaultInjectorState, get_fault_injector, set_fault_injector};
pub(crate) mod background_task_monitor;
pub(crate) mod neighbor_hosting;
pub(crate) mod network_status;
mod op_state_manager;
mod p2p_impl;
mod request_router;
pub(crate) mod testing_impl;

pub use request_router::{DeduplicatedRequest, RequestRouter};

/// Handle to trigger graceful shutdown of the node.
#[derive(Clone)]
pub struct ShutdownHandle {
    tx: tokio::sync::mpsc::Sender<NodeEvent>,
    /// Counter of currently-running client-originated driver tasks
    /// (`run_client_put` / `_get` / `_update` / `_subscribe`). Read by
    /// `shutdown` to wait for those tasks to finish before triggering
    /// the Disconnect.
    inflight_client_ops: Arc<std::sync::atomic::AtomicUsize>,
    /// Admission gate flipped by `shutdown` *before* the drain begins,
    /// so `start_client_*` can fail fast with `OpError::NodeShuttingDown`
    /// instead of slipping a new op into the post-drain race window.
    /// Same `Arc` is held by `OpManager::shutting_down` so the gate is
    /// visible to the spawn sites without a separate channel.
    shutting_down: Arc<std::sync::atomic::AtomicBool>,
    /// Maximum time to wait for `inflight_client_ops` to reach zero
    /// before forcing the disconnect anyway. `Duration::ZERO` disables
    /// the drain (legacy immediate-disconnect behaviour).
    drain_timeout: std::time::Duration,
}

impl ShutdownHandle {
    /// Trigger a graceful shutdown of the node.
    ///
    /// Three-phase shutdown — order matters:
    ///
    /// 1. **Close admission**: flip `OpManager::shutting_down` so
    ///    `start_client_{put,get,update,subscribe}` immediately
    ///    refuse new work with `OpError::NodeShuttingDown`. Without
    ///    this, a new client op could spawn between the drain
    ///    observing `counter == 0` and Disconnect being sent — that
    ///    op would bump the counter (now unobserved) and then get
    ///    cut off by the Disconnect. (Codex reviewer call-out
    ///    2026-05.)
    /// 2. **Drain**: wait up to `drain_timeout` for the in-flight
    ///    client-op counter to reach zero. Without this wait, a
    ///    SIGTERM arriving mid-PUT (e.g. release-driven auto-update
    ///    on the nova gateway) drops the client's WebSocket
    ///    mid-operation. See the rationale on
    ///    `Config::shutdown_drain_secs`.
    /// 3. **Disconnect**: send `NodeEvent::Disconnect`, which closes
    ///    peer connections and exits the event loop.
    ///
    /// Scope limitation: the drain covers **client-originated**
    /// drivers only. In-flight *relay* operations (peer-to-peer
    /// PUT/GET this node is forwarding) are NOT drained — those are
    /// short-lived per-message work and the peer can re-attempt. The
    /// targeted failure mode is user-facing WS client requests (the
    /// `freenet-git` mirror), not relay traffic.
    pub async fn shutdown(&self) {
        use std::sync::atomic::Ordering;

        // Phase 1: close admission BEFORE the drain. Subsequent
        // start_client_* calls fail fast.
        self.shutting_down.store(true, Ordering::Relaxed);

        // Phase 2: drain.
        self.wait_for_drain().await;

        // Phase 3: trigger event-loop teardown.
        if let Err(err) = self
            .tx
            .send(NodeEvent::Disconnect {
                cause: Some("graceful shutdown".into()),
            })
            .await
        {
            tracing::debug!(
                error = %err,
                "failed to send graceful shutdown signal; shutdown channel may already be closed"
            );
        }
    }

    /// Poll-loop the in-flight client-op counter until it hits zero or
    /// `drain_timeout` expires. Cap each individual sleep at 200ms so
    /// the drain can react promptly when the counter clears.
    async fn wait_for_drain(&self) {
        use std::sync::atomic::Ordering;

        if self.drain_timeout.is_zero() {
            return;
        }
        let initial = self.inflight_client_ops.load(Ordering::Relaxed);
        if initial == 0 {
            return;
        }
        tracing::info!(
            initial,
            drain_timeout_secs = self.drain_timeout.as_secs(),
            "Shutdown drain: waiting for in-flight client ops to finish"
        );

        // `tokio::time` is appropriate here even under the
        // `TimeSource`-or-bust rule for crates/core: shutdown drain is
        // a process-exit code path that wall-clock blocks on real
        // tokio sleeps, has no analogue in simulation tests, and is
        // explicitly bounded by `drain_timeout`.
        let drained = tokio::time::timeout(self.drain_timeout, async {
            let mut tick = tokio::time::interval(std::time::Duration::from_millis(200));
            tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            // First `tick` fires immediately; advance past it so the
            // loop body actually sleeps between checks.
            tick.tick().await;
            loop {
                if self.inflight_client_ops.load(Ordering::Relaxed) == 0 {
                    return;
                }
                tick.tick().await;
            }
        })
        .await;

        let remaining = self.inflight_client_ops.load(Ordering::Relaxed);
        match drained {
            Ok(()) => tracing::info!(initial, "Shutdown drain complete (all client ops finished)"),
            Err(_) => tracing::warn!(
                initial,
                remaining,
                drain_timeout_secs = self.drain_timeout.as_secs(),
                "Shutdown drain timed out; proceeding with disconnect"
            ),
        }
    }
}

pub struct Node {
    inner: NodeP2P,
    shutdown_handle: ShutdownHandle,
}

impl Node {
    pub fn update_location(&mut self, location: Location) {
        self.inner
            .op_manager
            .ring
            .connection_manager
            .update_location(Some(location));
    }

    /// Get a handle that can be used to trigger graceful shutdown.
    pub fn shutdown_handle(&self) -> ShutdownHandle {
        self.shutdown_handle.clone()
    }

    pub async fn run(self) -> anyhow::Result<Infallible> {
        self.inner.run_node().await
    }
}

/// When instancing a node you can either join an existing network or bootstrap a new network with a listener
/// which will act as the initial provider. This initial peer will be listening at the provided port and assigned IP.
/// If those are not free the instancing process will return an error.
///
/// In order to bootstrap a new network the following arguments are required to be provided to the builder:
/// - ip: IP associated to the initial node.
/// - port: listening port of the initial node.
///
/// If both are provided but also additional peers are added via the [`Self::add_gateway()`] method, this node will
/// be listening but also try to connect to an existing peer.
#[derive(Serialize, Deserialize, Clone, Debug)]
#[non_exhaustive] // avoid directly instantiating this struct
pub struct NodeConfig {
    /// Determines if an initial connection should be attempted.
    /// Only true for an initial gateway/node. If false, the gateway will be disconnected unless other peers connect through it.
    pub should_connect: bool,
    pub is_gateway: bool,
    /// If not specified, a key is generated and used when creating the node.
    pub key_pair: TransportKeypair,
    // optional local info, in case this is an initial bootstrap node
    /// IP to bind to the network listener.
    pub network_listener_ip: IpAddr,
    /// socket port to bind to the network listener.
    pub network_listener_port: u16,
    /// Our own external socket address, if known (set for gateways, learned for peers).
    pub(crate) own_addr: Option<SocketAddr>,
    pub(crate) config: Arc<Config>,
    /// At least one gateway is required for joining the network.
    /// Not necessary if this is an initial node.
    pub(crate) gateways: Vec<InitPeerNode>,
    /// the location of this node, used for gateways.
    pub(crate) location: Option<Location>,
    pub(crate) max_hops_to_live: Option<usize>,
    pub(crate) rnd_if_htl_above: Option<usize>,
    pub(crate) max_number_conn: Option<usize>,
    pub(crate) min_number_conn: Option<usize>,
    pub(crate) max_upstream_bandwidth: Option<Rate>,
    pub(crate) max_downstream_bandwidth: Option<Rate>,
    pub(crate) blocked_addresses: Option<HashSet<SocketAddr>>,
    pub(crate) transient_budget: usize,
    pub(crate) transient_ttl: Duration,
    /// Minimum ring connections before this peer advertises readiness
    /// to accept non-CONNECT operations. `None` or `Some(0)` disables the gate.
    /// Default: `Some(3)` in production.
    #[serde(default)]
    pub(crate) relay_ready_connections: Option<usize>,
}

impl NodeConfig {
    pub async fn new(config: Config) -> anyhow::Result<NodeConfig> {
        tracing::info!("Loading node configuration for mode {}", config.mode);

        // Get our own public key to filter out self-connections
        let own_pub_key = config.transport_keypair().public();

        let mut gateways = Vec::with_capacity(config.gateways.len());
        for gw in &config.gateways {
            let GatewayConfig {
                address,
                public_key_path,
                location,
            } = gw;

            // Wait for the public key file to be in X25519 hex format.
            // The gateway may still be initializing and converting from RSA PEM.
            let mut key_bytes = None;
            for attempt in 0..10 {
                let mut key_file = File::open(public_key_path).with_context(|| {
                    format!("failed loading gateway pubkey from {public_key_path:?}")
                })?;
                let mut buf = String::new();
                key_file.read_to_string(&mut buf)?;
                let buf = buf.trim();

                // Check for legacy RSA PEM format - gateway may still be initializing
                if buf.starts_with("-----BEGIN") {
                    if attempt < 9 {
                        tracing::debug!(
                            public_key_path = ?public_key_path,
                            attempt = attempt + 1,
                            "Gateway public key is still RSA PEM format, waiting for X25519 conversion..."
                        );
                        tokio::time::sleep(std::time::Duration::from_millis(500)).await;
                        continue;
                    } else {
                        tracing::warn!(
                            public_key_path = ?public_key_path,
                            "Gateway public key still in RSA PEM format after 5s. Skipping this gateway."
                        );
                        break;
                    }
                }

                match hex::decode(buf) {
                    Ok(bytes) if bytes.len() == 32 => {
                        key_bytes = Some(bytes);
                        break;
                    }
                    Ok(bytes) => {
                        anyhow::bail!(
                            "invalid gateway pubkey length {} (expected 32) from {public_key_path:?}",
                            bytes.len()
                        );
                    }
                    Err(e) => {
                        anyhow::bail!(
                            "failed to decode gateway pubkey hex from {public_key_path:?}: {e}"
                        );
                    }
                }
            }

            let key_bytes = match key_bytes {
                Some(bytes) => bytes,
                None => continue, // Skip this gateway
            };
            let mut key_arr = [0u8; 32];
            key_arr.copy_from_slice(&key_bytes);
            let transport_pub_key = TransportPublicKey::from_bytes(key_arr);

            // Skip if this gateway's public key matches our own
            if &transport_pub_key == own_pub_key {
                tracing::warn!(
                    "Skipping gateway with same public key as self: {:?}",
                    public_key_path
                );
                continue;
            }

            let address = Self::parse_socket_addr(address).await?;
            let peer_key_location = PeerKeyLocation::new(transport_pub_key, address);
            let location = location
                .map(Location::new)
                .unwrap_or_else(|| Location::from_address(&address));
            gateways.push(InitPeerNode::new(peer_key_location, location));
        }
        tracing::info!(
            "Node will be listening at {}:{} internal address",
            config.network_api.address,
            config.network_api.port
        );
        if let Some(own_addr) = &config.peer_id {
            tracing::info!("Node external address: {}", own_addr.socket_addr());
        }
        Ok(NodeConfig {
            should_connect: true,
            is_gateway: config.is_gateway,
            key_pair: config.transport_keypair().clone(),
            gateways,
            own_addr: config.peer_id.clone().map(|p| p.socket_addr()),
            network_listener_ip: config.network_api.address,
            network_listener_port: config.network_api.port,
            location: config.location.map(Location::new),
            config: Arc::new(config.clone()),
            max_hops_to_live: None,
            rnd_if_htl_above: None,
            max_number_conn: Some(config.network_api.max_connections),
            min_number_conn: Some(config.network_api.min_connections),
            max_upstream_bandwidth: None,
            max_downstream_bandwidth: None,
            blocked_addresses: config.network_api.blocked_addresses.clone(),
            transient_budget: config.network_api.transient_budget,
            transient_ttl: Duration::from_secs(config.network_api.transient_ttl_secs),
            relay_ready_connections: if config.network_api.skip_load_from_network {
                Some(0) // Local/test networks: disable relay gate
            } else {
                Some(3) // Production: require 3 relay-ready upstream peers
            },
        })
    }

    pub(crate) async fn parse_socket_addr(address: &Address) -> anyhow::Result<SocketAddr> {
        let (hostname, port) = match address {
            crate::config::Address::Hostname(hostname) => {
                match hostname.rsplit_once(':') {
                    None => {
                        // no port found, use default
                        let hostname_with_port =
                            format!("{}:{}", hostname, crate::config::default_network_api_port());

                        if let Ok(mut addrs) = hostname_with_port.to_socket_addrs() {
                            if let Some(addr) = addrs.next() {
                                return Ok(addr);
                            }
                        }

                        (Cow::Borrowed(hostname.as_str()), None)
                    }
                    Some((host, port)) => match port.parse::<u16>() {
                        Ok(port) => {
                            if let Ok(mut addrs) = hostname.to_socket_addrs() {
                                if let Some(addr) = addrs.next() {
                                    return Ok(addr);
                                }
                            }

                            (Cow::Borrowed(host), Some(port))
                        }
                        Err(_) => return Err(anyhow::anyhow!("Invalid port number: {port}")),
                    },
                }
            }
            Address::HostAddress(addr) => return Ok(*addr),
        };

        let resolver = hickory_resolver::TokioResolver::builder_tokio()?.build()?;

        // only issue one query with .
        let hostname = if hostname.ends_with('.') {
            hostname
        } else {
            Cow::Owned(format!("{hostname}."))
        };

        let ips = resolver.lookup_ip(hostname.as_ref()).await?;
        match ips.iter().next() {
            Some(ip) => Ok(SocketAddr::new(
                ip,
                port.unwrap_or_else(crate::config::default_network_api_port),
            )),
            None => Err(anyhow::anyhow!("Fail to resolve IP address of {hostname}")),
        }
    }

    pub fn config(&self) -> &Config {
        &self.config
    }

    pub fn is_gateway(&mut self) -> &mut Self {
        self.is_gateway = true;
        self
    }

    pub fn first_gateway(&mut self) {
        self.should_connect = false;
    }

    pub fn with_should_connect(&mut self, should_connect: bool) -> &mut Self {
        self.should_connect = should_connect;
        self
    }

    pub fn max_hops_to_live(&mut self, num_hops: usize) -> &mut Self {
        self.max_hops_to_live = Some(num_hops);
        self
    }

    pub fn rnd_if_htl_above(&mut self, num_hops: usize) -> &mut Self {
        self.rnd_if_htl_above = Some(num_hops);
        self
    }

    pub fn max_number_of_connections(&mut self, num: usize) -> &mut Self {
        self.max_number_conn = Some(num);
        self
    }

    pub fn min_number_of_connections(&mut self, num: usize) -> &mut Self {
        self.min_number_conn = Some(num);
        self
    }

    pub fn relay_ready_connections(&mut self, num: Option<usize>) -> &mut Self {
        self.relay_ready_connections = num;
        self
    }

    pub fn with_own_addr(&mut self, addr: SocketAddr) -> &mut Self {
        self.own_addr = Some(addr);
        self
    }

    pub fn with_location(&mut self, loc: Location) -> &mut Self {
        self.location = Some(loc);
        self
    }

    /// Connection info for an already existing peer. Required in case this is not a gateway node.
    pub fn add_gateway(&mut self, peer: InitPeerNode) -> &mut Self {
        self.gateways.push(peer);
        self
    }

    /// Builds a node using the default backend connection manager.
    pub async fn build<const CLIENTS: usize>(
        self,
        clients: [BoxedClient; CLIENTS],
    ) -> anyhow::Result<Node> {
        let (node, _flush_handle) = self.build_with_flush_handle(clients).await?;
        Ok(node)
    }

    /// Builds a node and returns flush handle for event aggregation (for testing).
    pub async fn build_with_flush_handle<const CLIENTS: usize>(
        self,
        clients: [BoxedClient; CLIENTS],
    ) -> anyhow::Result<(Node, crate::tracing::EventFlushHandle)> {
        let (event_register, flush_handle) = {
            use super::tracing::{DynamicRegister, TelemetryReporter};

            let event_reg = EventRegister::new(self.config.event_log());
            let flush_handle = event_reg.flush_handle();

            let mut registers: Vec<Box<dyn NetEventRegister>> = vec![Box::new(event_reg)];

            // Add OpenTelemetry register if feature enabled
            #[cfg(feature = "trace-ot")]
            {
                use super::tracing::OTEventRegister;
                registers.push(Box::new(OTEventRegister::new()));
            }

            // Add telemetry reporter if enabled in config
            if let Some(telemetry) = TelemetryReporter::new(&self.config.telemetry) {
                registers.push(Box::new(telemetry));
            }

            (DynamicRegister::new(registers), flush_handle)
        };
        let cfg = self.config.clone();
        let drain_timeout = std::time::Duration::from_secs(cfg.shutdown_drain_secs);
        let (node_inner, shutdown_tx) = NodeP2P::build::<NetworkContractHandler, CLIENTS, _>(
            self,
            clients,
            event_register,
            cfg,
        )
        .await?;
        let shutdown_handle = ShutdownHandle {
            tx: shutdown_tx,
            inflight_client_ops: node_inner.op_manager.inflight_client_ops_handle(),
            shutting_down: node_inner.op_manager.shutting_down_handle(),
            drain_timeout,
        };
        Ok((
            Node {
                inner: node_inner,
                shutdown_handle,
            },
            flush_handle,
        ))
    }

    pub fn get_own_addr(&self) -> Option<SocketAddr> {
        self.own_addr
    }

    /// Returns all specified gateways for this peer. Returns an error if the peer is not a gateway
    /// and no gateways are specified.
    fn get_gateways(&self) -> anyhow::Result<Vec<PeerKeyLocation>> {
        let gateways: Vec<PeerKeyLocation> = self
            .gateways
            .iter()
            .map(|node| node.peer_key_location.clone())
            .collect();

        if !self.is_gateway && gateways.is_empty() {
            anyhow::bail!(
                "At least one remote gateway is required to join an existing network for non-gateway nodes."
            )
        } else {
            Ok(gateways)
        }
    }
}

/// Gateway node to use for joining the network.
#[derive(Clone, Serialize, Deserialize, Debug)]
pub struct InitPeerNode {
    peer_key_location: PeerKeyLocation,
    location: Location,
}

impl InitPeerNode {
    pub fn new(peer_key_location: PeerKeyLocation, location: Location) -> Self {
        Self {
            peer_key_location,
            location,
        }
    }
}

async fn report_result(
    tx: Option<Transaction>,
    op_result: Result<(), OpError>,
    op_manager: &OpManager,
    _event_listener: &mut dyn NetEventRegister,
) {
    // Add UPDATE-specific debug logging at the start
    if let Some(tx_id) = tx {
        if matches!(tx_id.transaction_type(), TransactionType::Update) {
            tracing::debug!("report_result called for UPDATE transaction {}", tx_id);
        }
    }

    match op_result {
        Ok(()) => {
            // No legacy `OpEnum` to report. Task-per-tx drivers publish
            // their own `HostResult` via `result_router_tx`, record
            // route events through `record_relay_route_event` /
            // `record_acceptor_outcome`, and handle dashboard
            // classification inline. Nothing remains for this branch
            // to do beyond the dispatch site's own logging.
            tracing::debug!(?tx, "Network message dispatch finished");
        }
        Err(err) => {
            // Mark operation as completed and notify waiting clients of the error
            if let Some(tx) = tx {
                // Sub-operations (e.g., Subscribe spawned by PUT) have no client
                // registered — sending errors for them would pollute the
                // SessionActor's pending_results cache.
                if !tx.is_sub_operation() {
                    let client_error = freenet_stdlib::client_api::ClientError::from(
                        freenet_stdlib::client_api::ErrorKind::OperationError {
                            cause: err.to_string().into(),
                        },
                    );
                    op_manager.send_client_result(tx, Err(client_error));
                }

                op_manager.completed(tx);
            }
            #[cfg(any(debug_assertions, test))]
            {
                use std::io::Write;
                #[cfg(debug_assertions)]
                let OpError::InvalidStateTransition { tx, state, trace } = err else {
                    tracing::error!("Finished transaction with error: {err}");
                    return;
                };
                #[cfg(not(debug_assertions))]
                let OpError::InvalidStateTransition { tx } = err else {
                    tracing::error!("Finished transaction with error: {err}");
                    return;
                };
                // todo: this can be improved once std::backtrace::Backtrace::frames is stabilized
                #[cfg(debug_assertions)]
                let trace = format!("{trace}");
                #[cfg(debug_assertions)]
                {
                    let mut tr_lines = trace.lines();
                    let trace = tr_lines
                        .nth(2)
                        .map(|second_trace| {
                            let second_trace_lines =
                                [second_trace, tr_lines.next().unwrap_or_default()];
                            second_trace_lines.join("\n")
                        })
                        .unwrap_or_default();
                    let peer = op_manager.ring.connection_manager.own_location();
                    let log = format!(
                        "Transaction ({tx} @ {peer}) error trace:\n {trace} \nstate:\n {state:?}\n"
                    );
                    std::io::stderr().write_all(log.as_bytes()).unwrap();
                }
                #[cfg(not(debug_assertions))]
                {
                    let peer = op_manager.ring.connection_manager.own_location();
                    let log = format!("Transaction ({tx} @ {peer}) error\n");
                    std::io::stderr().write_all(log.as_bytes()).unwrap();
                }
            }
            #[cfg(not(any(debug_assertions, test)))]
            {
                tracing::debug!("Finished transaction with error: {err}");
            }
        }
    }
}

/// Process a network message and deliver results to clients via the canonical
/// path: report_result → send_client_result → ResultRouter → SessionActor.
pub(crate) async fn process_message_decoupled<CB>(
    msg: NetMessage,
    source_addr: Option<std::net::SocketAddr>,
    op_manager: Arc<OpManager>,
    conn_manager: CB,
    mut event_listener: Box<dyn NetEventRegister>,
    pending_op_result: Option<tokio::sync::mpsc::Sender<NetMessage>>,
) where
    CB: NetworkBridge + Clone + 'static,
{
    let tx = *msg.id();

    let op_result = handle_pure_network_message(
        msg,
        source_addr,
        op_manager.clone(),
        conn_manager,
        event_listener.as_mut(),
        pending_op_result,
    )
    .await;

    // Report result and deliver to clients via the single canonical path:
    // report_result → send_client_result → ResultRouter → SessionActor
    report_result(Some(tx), op_result, &op_manager, &mut *event_listener).await;
}

/// Pure network message handling (no client concerns)
#[allow(clippy::too_many_arguments)]
async fn handle_pure_network_message<CB>(
    msg: NetMessage,
    source_addr: Option<std::net::SocketAddr>,
    op_manager: Arc<OpManager>,
    conn_manager: CB,
    event_listener: &mut dyn NetEventRegister,
    pending_op_result: Option<tokio::sync::mpsc::Sender<NetMessage>>,
) -> Result<(), crate::node::OpError>
where
    CB: NetworkBridge + Clone + 'static,
{
    match msg {
        NetMessage::V1(msg_v1) => {
            handle_pure_network_message_v1(
                msg_v1,
                source_addr,
                op_manager,
                conn_manager,
                event_listener,
                pending_op_result,
            )
            .await
        }
    }
}

/// Forward an inbound reply directly to the awaiting
/// [`OpCtx::send_and_await`][ocxawait] caller.
///
/// Returns `true` if a callback was registered (message forwarded or
/// dropped on a closed receiver — either way the caller must not fall
/// through to other handling). Returns `false` if no callback is
/// registered.
///
/// # Safety argument
///
/// `p2p_protoc::pending_op_results` is only populated via
/// `p2p_protoc::handle_op_execution`, driven by `op_execution_sender`.
/// The only way to obtain a clone of that sender is through
/// [`crate::node::OpManager::op_ctx`], whose round-trip method is
/// [`OpCtx::send_and_await`][ocxawait]. This is a **structural
/// invariant**: the sender field is `pub(crate)` and there is no other
/// `pub` accessor on `EventLoopNotificationsSender`.
///
/// [ocxawait]: crate::operations::OpCtx::send_and_await
///
/// # Channel safety
///
/// Uses `try_send` on the bounded capacity-1 channel created by
/// `OpCtx::send_and_await`. On a closed receiver (caller cancelled or
/// timed out) the send fails and is logged; the handler still makes
/// progress. See `.claude/rules/channel-safety.md`.
fn try_forward_driver_reply(
    pending_op_result: Option<&tokio::sync::mpsc::Sender<NetMessage>>,
    reply: NetMessage,
    op_label: &'static str,
) -> bool {
    let Some(callback) = pending_op_result else {
        return false;
    };
    let tx_id = *reply.id();
    if let Err(err) = callback.try_send(reply) {
        tracing::error!(
            %err,
            %tx_id,
            op = op_label,
            "Failed to forward driver reply to OpCtx task"
        );
    }
    true
}

/// Fill in an acceptor's external address from `source_addr` when the
/// `ConnectMsg::Response` arrives with `acceptor.peer_addr = Unknown`.
///
/// An acceptor behind NAT does not know its own external address; the
/// inbound transport's `source_addr` is used to backstop the missing
/// value before the driver reads it. The driver itself does not see
/// `source_addr`, so the rewrite must happen at the dispatch site.
///
/// Non-`Response` variants and `Response` with an already-`Known`
/// acceptor address pass through unchanged.
fn fill_connect_response_acceptor_addr(
    op: connect::ConnectMsg,
    source_addr: Option<std::net::SocketAddr>,
) -> connect::ConnectMsg {
    #[allow(clippy::wildcard_enum_match_arm)]
    match op {
        connect::ConnectMsg::Response { id, mut payload } => {
            if payload.acceptor.peer_addr.is_unknown() {
                if let Some(addr) = source_addr {
                    payload.acceptor.peer_addr = crate::ring::PeerAddr::Known(addr);
                    tracing::debug!(
                        acceptor_pub_key = %payload.acceptor.pub_key(),
                        acceptor_addr = %addr,
                        "connect bypass: filled acceptor address from source_addr"
                    );
                } else {
                    tracing::warn!(
                        acceptor_pub_key = %payload.acceptor.pub_key(),
                        "connect bypass: response received without source_addr, cannot fill acceptor address"
                    );
                }
            }
            connect::ConnectMsg::Response { id, payload }
        }
        other => other,
    }
}

/// Pure network message processing for V1 messages (no client concerns)
#[allow(clippy::too_many_arguments, clippy::needless_return)]
async fn handle_pure_network_message_v1<CB>(
    msg: NetMessageV1,
    source_addr: Option<std::net::SocketAddr>,
    op_manager: Arc<OpManager>,
    conn_manager: CB,
    event_listener: &mut dyn NetEventRegister,
    pending_op_result: Option<tokio::sync::mpsc::Sender<NetMessage>>,
) -> Result<(), crate::node::OpError>
where
    CB: NetworkBridge + Clone + 'static,
{
    // Register network events (pure network concern)
    event_listener
        .register_events(NetEventLog::from_inbound_msg_v1(
            &msg,
            &op_manager,
            source_addr,
        ))
        .await;

    let tx = Some(*msg.id());
    tracing::debug!(?tx, "Processing pure network operation");

    match msg {
        NetMessageV1::Connect(ref op) => {
            // CONNECT reply forwarding: the joiner expects fan-in
            // (up to `target_connections` `Response`s over time).
            // The waiter (`OpCtx::send_and_collect_replies`) has a
            // multi-reply receiver, so this bypass forwards every
            // non-`Request` variant without short-circuiting after
            // the first hit.
            //
            // `Request` is NEVER forwarded here: it spawns a relay
            // driver via the dispatch gate below.
            if matches!(
                op,
                connect::ConnectMsg::Response { .. }
                    | connect::ConnectMsg::Rejected { .. }
                    | connect::ConnectMsg::ObservedAddress { .. }
                    | connect::ConnectMsg::ConnectFailed { .. }
            ) {
                let forwarded_op = fill_connect_response_acceptor_addr(op.clone(), source_addr);
                if try_forward_driver_reply(
                    pending_op_result.as_ref(),
                    NetMessage::V1(NetMessageV1::Connect(forwarded_op)),
                    "connect",
                ) {
                    return Ok(());
                }
            }

            // Relay-CONNECT dispatch: fresh inbound `Request` with
            // a real upstream address and no running relay driver
            // spawns `start_relay_connect`. The driver owns the
            // entire transaction lifetime in task locals. The
            // `active_relay_connect_txs` check dedups against
            // GC-spawned retries and duplicate Requests.
            //
            // `source_addr.is_none()` (originator loopback) cannot
            // reach this branch — the reply bypass above handles
            // joiner-side replies first. Originator state lives in
            // `start_client_connect` task locals; there is no
            // joiner-side legacy state machine.
            if let connect::ConnectMsg::Request { id, payload } = op {
                if let Some(upstream_addr) = source_addr {
                    if !op_manager.active_relay_connect_txs.contains(id) {
                        if let Err(err) = connect::op_ctx_task::start_relay_connect(
                            op_manager.clone(),
                            *id,
                            payload.clone(),
                            upstream_addr,
                        )
                        .await
                        {
                            tracing::error!(
                                tx = %id,
                                %upstream_addr,
                                error = %err,
                                "CONNECT relay dispatch: start_relay_connect failed"
                            );
                        }
                    } else {
                        tracing::debug!(
                            tx = %id,
                            %upstream_addr,
                            "CONNECT: duplicate Request, relay driver already running"
                        );
                    }
                } else {
                    tracing::debug!(
                        tx = %id,
                        "CONNECT: Request without source_addr ignored (no legacy joiner path)"
                    );
                }
            } else {
                tracing::debug!(
                    tx = %op.id(),
                    ?op,
                    "CONNECT: non-Request variant ignored \
                     (Response/Rejected/ObservedAddress/ConnectFailed already handled by bypass)"
                );
            }
            return Ok(());
        }
        NetMessageV1::Put(ref op) => {
            // Forward only **terminal** Response/ResponseStreaming
            // messages to the originator's awaiting task via the
            // bypass. Non-terminal messages (Request,
            // RequestStreaming, ForwardingAck) must NOT be
            // forwarded: they would fill the capacity-1 reply
            // channel and cause `classify_reply` to fail.
            if matches!(
                op,
                put::PutMsg::Response { .. } | put::PutMsg::ResponseStreaming { .. }
            ) && try_forward_driver_reply(
                pending_op_result.as_ref(),
                NetMessage::V1(NetMessageV1::Put((*op).clone())),
                "put",
            ) {
                return Ok(());
            }

            // Relay PUT dispatch. `start_relay_put` handles
            // non-streaming Request (with upgrade-on-forward to
            // streaming when payload > threshold);
            // `start_relay_put_streaming` handles direct
            // `RequestStreaming` inbound. `ForwardingAck` is a
            // no-op kept for backward compatibility.
            //
            // Originator loopback: `start_client_put`'s
            // `send_and_await(target=None)` arrives with
            // `source_addr=None`. Map to `upstream_addr=own_addr`
            // so the same driver handles both relay hops and
            // originator loopback.
            let effective_upstream =
                source_addr.or_else(|| op_manager.ring.connection_manager.get_own_addr());
            if let Some(upstream_addr) = effective_upstream {
                #[allow(clippy::wildcard_enum_match_arm)]
                match op {
                    put::PutMsg::Request {
                        id,
                        contract,
                        related_contracts,
                        value,
                        htl,
                        skip_list,
                    } => {
                        if let Err(err) = put::op_ctx_task::start_relay_put(
                            op_manager.clone(),
                            conn_manager.clone(),
                            *id,
                            contract.clone(),
                            related_contracts.clone(),
                            value.clone(),
                            *htl,
                            skip_list.clone(),
                            upstream_addr,
                        )
                        .await
                        {
                            tracing::error!(
                                tx = %id,
                                contract = %contract.key(),
                                error = %err,
                                "PUT relay dispatch: start_relay_put failed"
                            );
                        }
                    }
                    put::PutMsg::RequestStreaming {
                        id,
                        stream_id,
                        contract_key,
                        total_size,
                        htl,
                        skip_list,
                        subscribe,
                    } => {
                        if let Err(err) = put::op_ctx_task::start_relay_put_streaming(
                            op_manager.clone(),
                            conn_manager.clone(),
                            *id,
                            *stream_id,
                            *contract_key,
                            *total_size,
                            *htl,
                            skip_list.clone(),
                            *subscribe,
                            upstream_addr,
                        )
                        .await
                        {
                            tracing::error!(
                                tx = %id,
                                contract = %contract_key,
                                error = %err,
                                "PUT relay dispatch: start_relay_put_streaming failed"
                            );
                        }
                    }
                    _ => {
                        tracing::debug!(
                            tx = %op.id(),
                            ?op,
                            "PUT: non-dispatch variant ignored \
                             (Response/ResponseStreaming already handled \
                             by bypass; ForwardingAck is no-op)"
                        );
                    }
                }
            } else {
                tracing::debug!(
                    tx = %op.id(),
                    ?op,
                    "PUT: no own_addr available — pre-handshake \
                     message ignored"
                );
            }
            return Ok(());
        }
        NetMessageV1::Get(ref op) => {
            // Forward only **terminal** Response/ResponseStreaming
            // messages to the originator's awaiting task. Other
            // variants must NOT be forwarded — they would fill the
            // capacity-1 reply channel.
            if matches!(
                op,
                get::GetMsg::Response { .. } | get::GetMsg::ResponseStreaming { .. }
            ) && try_forward_driver_reply(
                pending_op_result.as_ref(),
                NetMessage::V1(NetMessageV1::Get((*op).clone())),
                "get",
            ) {
                return Ok(());
            }

            // Relay GET dispatch. Originator loopback
            // (`source_addr=None`) is mapped to
            // `upstream_addr=own_addr` so the same `start_relay_get`
            // driver handles both relay hops and loopback.
            let effective_upstream =
                source_addr.or_else(|| op_manager.ring.connection_manager.get_own_addr());
            if let Some(upstream_addr) = effective_upstream {
                #[allow(clippy::wildcard_enum_match_arm)]
                match op {
                    get::GetMsg::Request {
                        id,
                        instance_id,
                        fetch_contract,
                        htl,
                        visited,
                        subscribe,
                    } => {
                        if let Err(err) = get::op_ctx_task::start_relay_get(
                            op_manager.clone(),
                            *id,
                            *instance_id,
                            *htl,
                            upstream_addr,
                            visited.clone(),
                            *fetch_contract,
                            *subscribe,
                        )
                        .await
                        {
                            tracing::error!(
                                tx = %id,
                                %instance_id,
                                error = %err,
                                "GET relay dispatch: start_relay_get failed"
                            );
                        }
                    }
                    _ => {
                        tracing::debug!(
                            tx = %op.id(),
                            ?op,
                            "GET: non-dispatch variant ignored \
                             (Response/ResponseStreaming already handled \
                             by bypass; ForwardingAck is no-op; \
                             ResponseStreamingAck handled by stream layer)"
                        );
                    }
                }
            } else {
                tracing::debug!(
                    tx = %op.id(),
                    ?op,
                    "GET: no own_addr available — pre-handshake \
                     message ignored"
                );
            }
            return Ok(());
        }
        NetMessageV1::Update(ref op) => {
            // UPDATE is fire-and-forget end-to-end — no upstream
            // reply to await. For relay hops
            // (`source_addr.is_some()`) dispatch the matching
            // driver and return. `source_addr.is_none()` would
            // mean an internal caller; there are none, so the else
            // branch logs and drops.
            if let Some(sender_addr) = source_addr {
                #[allow(clippy::wildcard_enum_match_arm)]
                match op {
                    update::UpdateMsg::RequestUpdate {
                        id,
                        key,
                        related_contracts,
                        value,
                    } => {
                        if let Err(err) = update::op_ctx_task::start_relay_request_update(
                            op_manager.clone(),
                            *id,
                            *key,
                            related_contracts.clone(),
                            value.clone(),
                            sender_addr,
                        )
                        .await
                        {
                            tracing::error!(
                                tx = %id,
                                %key,
                                error = %err,
                                "UPDATE relay dispatch: start_relay_request_update failed"
                            );
                        }
                        return Ok(());
                    }
                    update::UpdateMsg::BroadcastTo {
                        id,
                        key,
                        payload,
                        sender_summary_bytes,
                    } => {
                        if let Err(err) = update::op_ctx_task::start_relay_broadcast_to(
                            op_manager.clone(),
                            *id,
                            *key,
                            payload.clone(),
                            sender_summary_bytes.clone(),
                            sender_addr,
                        )
                        .await
                        {
                            tracing::error!(
                                tx = %id,
                                %key,
                                error = %err,
                                "UPDATE relay dispatch: start_relay_broadcast_to failed"
                            );
                        }
                        return Ok(());
                    }
                    // Streaming relay UPDATE: claim stream → assemble
                    // → apply → BroadcastStateChange fans out.
                    update::UpdateMsg::RequestUpdateStreaming {
                        id,
                        key,
                        stream_id,
                        total_size,
                    } => {
                        if let Err(err) = update::op_ctx_task::start_relay_request_update_streaming(
                            op_manager.clone(),
                            *id,
                            *key,
                            *stream_id,
                            *total_size,
                            sender_addr,
                        )
                        .await
                        {
                            tracing::error!(
                                tx = %id,
                                %key,
                                error = %err,
                                "UPDATE relay dispatch: start_relay_request_update_streaming failed"
                            );
                        }
                        return Ok(());
                    }
                    update::UpdateMsg::BroadcastToStreaming {
                        id,
                        key,
                        stream_id,
                        total_size,
                    } => {
                        if let Err(err) = update::op_ctx_task::start_relay_broadcast_to_streaming(
                            op_manager.clone(),
                            *id,
                            *key,
                            *stream_id,
                            *total_size,
                            sender_addr,
                        )
                        .await
                        {
                            tracing::error!(
                                tx = %id,
                                %key,
                                error = %err,
                                "UPDATE relay dispatch: start_relay_broadcast_to_streaming failed"
                            );
                        }
                        return Ok(());
                    }
                }
            } else {
                tracing::debug!(
                    tx = %op.id(),
                    ?op,
                    "UPDATE: internal-source variant ignored"
                );
            }
            return Ok(());
        }
        NetMessageV1::Subscribe(ref op) => {
            // Forward only **terminal** Response messages to the
            // originator's awaiting task. Other variants must NOT
            // be forwarded — they would fill the capacity-1 reply
            // channel.
            if matches!(op, subscribe::SubscribeMsg::Response { .. })
                && try_forward_driver_reply(
                    pending_op_result.as_ref(),
                    NetMessage::V1(NetMessageV1::Subscribe((*op).clone())),
                    "subscribe",
                )
            {
                return Ok(());
            }

            // Relay SUBSCRIBE dispatch. Originator loopback
            // (`source_addr=None`) is mapped to
            // `upstream_addr=own_addr` so the same
            // `start_relay_subscribe` driver handles both.
            let effective_upstream =
                source_addr.or_else(|| op_manager.ring.connection_manager.get_own_addr());
            if let Some(upstream_addr) = effective_upstream {
                #[allow(clippy::wildcard_enum_match_arm)]
                match op {
                    subscribe::SubscribeMsg::Request {
                        id,
                        instance_id,
                        htl,
                        visited,
                        is_renewal,
                    } => {
                        if let Err(err) = subscribe::op_ctx_task::start_relay_subscribe(
                            op_manager.clone(),
                            *id,
                            *instance_id,
                            *htl,
                            visited.clone(),
                            *is_renewal,
                            upstream_addr,
                        )
                        .await
                        {
                            tracing::error!(
                                tx = %id,
                                %instance_id,
                                error = %err,
                                "SUBSCRIBE relay dispatch: start_relay_subscribe failed"
                            );
                        }
                    }
                    subscribe::SubscribeMsg::Unsubscribe { id, instance_id } => {
                        subscribe::handle_unsubscribe_inbound(
                            &op_manager,
                            *id,
                            *instance_id,
                            source_addr,
                        )
                        .await;
                    }
                    _ => {
                        // Response handled by bypass above;
                        // ForwardingAck is a wire-only telemetry
                        // hook (#3570) with no state mutation.
                        tracing::debug!(
                            tx = %op.id(),
                            ?op,
                            "SUBSCRIBE: non-dispatch variant ignored \
                             (Response already handled by bypass; \
                             ForwardingAck is no-op)"
                        );
                    }
                }
            } else {
                tracing::debug!(
                    tx = %op.id(),
                    ?op,
                    "SUBSCRIBE: no own_addr available — pre-handshake \
                     message ignored"
                );
            }
            return Ok(());
        }
        // Non-transactional message types: process once and return immediately.
        // These must NOT fall through to the post-loop "Dropping message" warning,
        // which is only meant for operation retry exhaustion.
        NetMessageV1::NeighborHosting { ref message } => {
            let Some(source) = source_addr else {
                tracing::warn!(
                    "Received NeighborHosting message without source address (pure network)"
                );
                return Ok(());
            };
            tracing::debug!(
                from = %source,
                "Processing NeighborHosting message (pure network)"
            );

            // Note: In the simplified architecture (2026-01 refactor), we no longer
            // attempt to establish subscriptions based on HostingAnnounce messages.
            // Update propagation uses the neighbor hosting manager directly, and subscriptions
            // are lease-based with automatic expiry.

            // Resolve source SocketAddr to TransportPublicKey for neighbor hosting
            let source_pub_key = op_manager
                .ring
                .connection_manager
                .get_peer_by_addr(source)
                .map(|pkl| pkl.pub_key().clone());
            let Some(source_pub_key) = source_pub_key else {
                tracing::debug!(
                    %source,
                    "NeighborHosting: could not resolve source addr to pub_key, skipping"
                );
                return Ok(());
            };
            let result = op_manager
                .neighbor_hosting
                .handle_message(&source_pub_key, message.clone());
            if let Some(response) = result.response {
                // Send response back to sender
                let response_msg =
                    NetMessage::V1(NetMessageV1::NeighborHosting { message: response });
                if let Err(err) = conn_manager.send(source, response_msg).await {
                    tracing::error!(%err, %source, "Failed to send NeighborHosting response");
                }
            }
            // Proactive state sync: broadcast our state for shared contracts
            // so the neighbor gets current state if they're stale after restart.
            // Only sync contracts we're actively interested in (receiving updates
            // or have downstream subscribers) — skip cached-only contracts.
            for instance_id in result.overlapping_contracts {
                if let Some((key, state)) =
                    get_contract_state_by_id(&op_manager, &instance_id).await
                {
                    if !op_manager.ring.is_receiving_updates(&key)
                        && !op_manager.ring.has_downstream_subscribers(&key)
                    {
                        continue;
                    }
                    tracing::debug!(
                        contract = %key,
                        peer = %source_pub_key,
                        "Proximity cache overlap — syncing state to neighbor"
                    );
                    // Non-blocking emit: SyncStateToPeer is best-effort
                    // gossip — if dropped, the next interest-sync round
                    // or a subsequent summary mismatch will catch it. A
                    // blocking 30 s `.await` here would itself stack on
                    // the same notification channel that the executor's
                    // try_notify path is trying to keep responsive
                    // (#4145 / #4234).
                    if let Err(e) = op_manager.try_notify_node_event(NodeEvent::SyncStateToPeer {
                        key,
                        new_state: state,
                        target: source,
                    }) {
                        // Best-effort by design (see comment above);
                        // log at debug to keep the caller layer in
                        // step with the helper-internal downgrade
                        // (#4238).
                        tracing::debug!(
                            contract = %instance_id,
                            error = %e,
                            "Failed to emit SyncStateToPeer for proximity sync (best-effort)"
                        );
                    }
                }
            }
            return Ok(());
        }
        NetMessageV1::InterestSync { ref message } => {
            let Some(source) = source_addr else {
                tracing::warn!("Received InterestSync message without source address");
                return Ok(());
            };
            tracing::debug!(
                from = %source,
                "Processing InterestSync message"
            );

            // Handle interest synchronization for delta-based updates
            if let Some(response) =
                handle_interest_sync_message(&op_manager, source, message.clone()).await
            {
                let response_msg = NetMessage::V1(NetMessageV1::InterestSync { message: response });
                if let Err(err) = conn_manager.send(source, response_msg).await {
                    tracing::error!(%err, %source, "Failed to send InterestSync response");
                }
            }
            return Ok(());
        }
        NetMessageV1::ReadyState { ready } => {
            let Some(source) = source_addr else {
                tracing::warn!("Received ReadyState message without source address");
                return Ok(());
            };
            if ready {
                op_manager.ring.connection_manager.mark_peer_ready(source);
            } else {
                op_manager
                    .ring
                    .connection_manager
                    .mark_peer_not_ready(source);
            }
            tracing::debug!(
                from = %source,
                ready,
                "Processed ReadyState from peer"
            );
            return Ok(());
        }
        NetMessageV1::Aborted(tx) => {
            // Drivers own their own cancellation; `Aborted` senders are
            // drivers themselves and the bypass handles in-driver delivery.
            tracing::debug!(
                %tx,
                tx_type = ?tx.transaction_type(),
                "Received Aborted message — driver owns cancellation, ignoring"
            );
            Ok(())
        }
    }
}

/// Handle incoming InterestSync messages for delta-based state synchronization.
///
/// This function processes the interest exchange protocol:
/// - `Interests`: Connection-time discovery of shared contract interests
/// - `Summaries`: State summaries for shared contracts
/// - `ChangeInterests`: Incremental interest changes
/// - `ResyncRequest`: Request full state when delta application fails
async fn handle_interest_sync_message(
    op_manager: &Arc<OpManager>,
    source: std::net::SocketAddr,
    message: crate::message::InterestMessage,
) -> Option<crate::message::InterestMessage> {
    use crate::message::{InterestMessage, NodeEvent, SummaryEntry};
    use crate::ring::interest::contract_hash;

    match message {
        InterestMessage::Interests { hashes } => {
            tracing::debug!(
                from = %source,
                hash_count = hashes.len(),
                "Received Interests message"
            );

            let peer_key = get_peer_key_from_addr(op_manager, source);

            // Full-replace semantics: the incoming hashes represent the peer's
            // complete interest set. Remove entries for contracts whose hash is
            // NOT in the incoming set, then register/refresh the rest.
            if let Some(ref pk) = peer_key {
                let incoming_hashes: std::collections::HashSet<u32> =
                    hashes.iter().copied().collect();
                let current_contracts = op_manager.interest_manager.get_contracts_for_peer(pk);

                // Hash collisions (FNV-1a u32) can cause a stale entry to
                // survive if its hash collides with a live one. This is the
                // safe direction — false negatives on removal, not false
                // positives — and extremely rare in practice.
                let mut removed = 0usize;
                for contract in &current_contracts {
                    let h = contract_hash(contract);
                    if !incoming_hashes.contains(&h) {
                        op_manager
                            .interest_manager
                            .remove_peer_interest(contract, pk);
                        removed += 1;
                    }
                }
                if removed > 0 {
                    tracing::debug!(
                        from = %source,
                        removed,
                        "Full-replace: removed stale interest entries"
                    );
                }
            }

            // Find contracts we share interest in
            let matching = op_manager.interest_manager.get_matching_contracts(&hashes);

            // Build summaries for shared contracts and register/refresh peer interest
            let mut entries = Vec::with_capacity(matching.len());
            for contract in matching {
                let hash = contract_hash(&contract);
                let summary = get_contract_summary(op_manager, &contract).await;
                entries.push(SummaryEntry::from_summary(hash, summary.as_ref()));

                if let Some(ref pk) = peer_key {
                    // Refresh TTL for existing entries (preserves cached summary).
                    // Only register new interest if this is a genuinely new entry;
                    // otherwise register_peer_interest would overwrite the cached
                    // summary with None, defeating delta optimization.
                    if op_manager
                        .interest_manager
                        .get_peer_interest(&contract, pk)
                        .is_some()
                    {
                        op_manager
                            .interest_manager
                            .refresh_peer_interest(&contract, pk);
                    } else {
                        op_manager.interest_manager.register_peer_interest(
                            &contract,
                            pk.clone(),
                            None, // New entry; summary arrives in their Summaries response
                            false,
                        );
                    }
                }
            }

            if entries.is_empty() {
                None
            } else {
                Some(InterestMessage::Summaries { entries })
            }
        }

        InterestMessage::Summaries { entries } => {
            tracing::debug!(
                from = %source,
                entry_count = entries.len(),
                "Received Summaries message"
            );

            // Update peer summaries and detect stale peers (#3221).
            //
            // Compare each peer summary with our own before storing it. If they
            // differ, the peer missed an earlier broadcast. We send state only
            // to the specific peer that reported the stale summary via
            // SyncStateToPeer (not BroadcastStateChange which fans out to ALL
            // subscribers). This avoids O(peers^2) broadcast storms where N
            // peers each trigger a full fan-out broadcast. See #3791.
            //
            // Both sides may detect the same mismatch (A sees B is stale, B sees
            // A is stale). This is safe: the contract's merge semantics (CRDTs
            // etc.) ensure the newer/correct state wins regardless of push order.
            //
            // When either summary is None, we skip the comparison. A peer with
            // no summary has no state yet and should receive it via the normal
            // subscription/GET flow, not via broadcast.
            let peer_key = get_peer_key_from_addr(op_manager, source);
            let mut stale_contracts = Vec::new();
            // Collect (contract, state_hash) for deferred StateConfirmed telemetry.
            // Only emitted in direct-runner mode to avoid .await points that change
            // turmoil task scheduling.
            let emit_confirmed = crate::config::SimulationIdleTimeout::is_enabled();
            let mut confirmed_states: Vec<(freenet_stdlib::prelude::ContractKey, String)> =
                Vec::new();

            if let Some(pk) = peer_key {
                for entry in entries {
                    for contract in op_manager.interest_manager.lookup_by_hash(entry.hash) {
                        if !op_manager.interest_manager.has_local_interest(&contract) {
                            continue;
                        }

                        let their_summary = entry.to_summary();
                        let our_summary = get_contract_summary(op_manager, &contract).await;

                        if emit_confirmed {
                            if let Some(ref summary) = our_summary {
                                confirmed_states.push((contract, hex::encode(summary.as_ref())));
                            }
                        }

                        let is_stale = our_summary
                            .as_ref()
                            .zip(their_summary.as_ref())
                            .is_some_and(|(ours, theirs)| ours.as_ref() != theirs.as_ref());

                        op_manager.interest_manager.update_peer_summary(
                            &contract,
                            &pk,
                            their_summary,
                        );

                        if is_stale && !stale_contracts.contains(&contract) {
                            stale_contracts.push(contract);
                        }
                    }
                }
            }

            // Send current state only to the specific peer that reported a stale
            // summary. Previously this emitted BroadcastStateChange which fanned
            // out to ALL subscribers (~28 peers), causing O(peers^2) traffic when
            // many peers reported mismatches within the same heartbeat cycle.
            for contract in stale_contracts {
                let Some(state) = get_contract_state(op_manager, &contract).await else {
                    tracing::trace!(
                        contract = %contract,
                        "Skipping stale-peer sync — no local state available"
                    );
                    continue;
                };
                // Fires per stale-peer detection during interest sync, which
                // is dominant on hot contracts. Diagnostic-grade rather than
                // user-actionable; keep accessible via RUST_LOG=…=debug.
                tracing::debug!(
                    contract = %contract,
                    stale_peer = %source,
                    "Summary mismatch in interest sync — syncing state to stale peer"
                );
                // Non-blocking emit: SyncStateToPeer is best-effort
                // gossip — if dropped, the next interest-sync round
                // will retry. Blocking here would stack the heal
                // path on the same notification channel the executor
                // is trying to keep responsive (#4145 / #4234).
                if let Err(e) = op_manager.try_notify_node_event(NodeEvent::SyncStateToPeer {
                    key: contract,
                    new_state: state,
                    target: source,
                }) {
                    // Best-effort by design (see comment above); log
                    // at debug to keep the caller layer in step with
                    // the helper-internal downgrade (#4238).
                    tracing::debug!(
                        contract = %contract,
                        error = %e,
                        "Failed to emit SyncStateToPeer for stale peer correction (best-effort)"
                    );
                }
            }

            // Emit deferred StateConfirmed telemetry so the convergence
            // checker has up-to-date state hashes for CRDT-merged state.
            for (key, state_hash) in confirmed_states {
                if let Some(event) =
                    crate::tracing::NetEventLog::state_confirmed(&op_manager.ring, key, state_hash)
                {
                    op_manager
                        .ring
                        .register_events(either::Either::Left(event))
                        .await;
                }
            }

            // No response needed for Summaries
            None
        }

        InterestMessage::ChangeInterests { added, removed } => {
            tracing::debug!(
                from = %source,
                added_count = added.len(),
                removed_count = removed.len(),
                "Received ChangeInterests message"
            );

            let peer_key = get_peer_key_from_addr(op_manager, source);

            // Handle removals
            if let Some(ref pk) = peer_key {
                for hash in removed {
                    // Handle hash collisions - remove interest from all matching contracts
                    for contract in op_manager.interest_manager.lookup_by_hash(hash) {
                        op_manager
                            .interest_manager
                            .remove_peer_interest(&contract, pk);
                    }
                }
            }

            // Handle additions - respond with summaries for newly shared contracts
            let mut entries = Vec::new();
            if let Some(ref pk) = peer_key {
                for hash in added {
                    // Handle hash collisions - process all matching contracts
                    for contract in op_manager.interest_manager.lookup_by_hash(hash) {
                        // Only process if we have local interest in this contract
                        if !op_manager.interest_manager.has_local_interest(&contract) {
                            continue;
                        }

                        // Register their interest
                        op_manager.interest_manager.register_peer_interest(
                            &contract,
                            pk.clone(),
                            None,
                            false,
                        );

                        // Get our summary to send back
                        let summary = get_contract_summary(op_manager, &contract).await;
                        entries.push(SummaryEntry::from_summary(hash, summary.as_ref()));
                    }
                }
            }

            if entries.is_empty() {
                None
            } else {
                Some(InterestMessage::Summaries { entries })
            }
        }

        InterestMessage::ResyncRequest { key } => {
            tracing::info!(
                from = %source,
                contract = %key,
                event = "resync_request_received",
                "Received ResyncRequest - peer needs full state"
            );

            // Track this for testing - high counts indicate incorrect summary caching (PR #2763)
            op_manager.interest_manager.record_resync_request_received();
            crate::config::GlobalTestMetrics::record_resync_request();

            // Clear cached summary for this peer
            let peer_key = get_peer_key_from_addr(op_manager, source);
            if let Some(ref pk) = peer_key {
                op_manager
                    .interest_manager
                    .update_peer_summary(&key, pk, None);
            }

            // Get PeerKeyLocation for telemetry
            let from_peer = op_manager.ring.connection_manager.get_peer_by_addr(source);

            // Emit telemetry for ResyncRequest received
            if let Some(ref from_pkl) = from_peer {
                if let Some(event) = crate::tracing::NetEventLog::resync_request_received(
                    &op_manager.ring,
                    key,
                    from_pkl.clone(),
                ) {
                    op_manager
                        .ring
                        .register_events(either::Either::Left(event))
                        .await;
                }
            } else {
                tracing::debug!(
                    contract = %key,
                    source = %source,
                    "ResyncRequest telemetry skipped: peer lookup failed"
                );
            }

            // Fetch current state from store
            let state = get_contract_state(op_manager, &key).await;
            let Some(state) = state else {
                tracing::warn!(
                    contract = %key,
                    "ResyncRequest for contract we don't have state for"
                );
                return None;
            };

            // Fetch our summary
            let summary = get_contract_summary(op_manager, &key).await;
            let Some(summary) = summary else {
                tracing::warn!(
                    contract = %key,
                    "ResyncRequest for contract we can't compute summary for"
                );
                return None;
            };

            tracing::info!(
                to = %source,
                contract = %key,
                state_size = state.as_ref().len(),
                summary_size = summary.as_ref().len(),
                event = "resync_response_sent",
                "Sending ResyncResponse with full state"
            );

            // Emit telemetry for ResyncResponse sent
            if let Some(ref to_pkl) = from_peer {
                if let Some(event) = crate::tracing::NetEventLog::resync_response_sent(
                    &op_manager.ring,
                    key,
                    to_pkl.clone(),
                    state.as_ref().len(),
                ) {
                    op_manager
                        .ring
                        .register_events(either::Either::Left(event))
                        .await;
                }
            }

            Some(InterestMessage::ResyncResponse {
                key,
                state_bytes: state.as_ref().to_vec(),
                summary_bytes: summary.as_ref().to_vec(),
            })
        }

        InterestMessage::ResyncResponse {
            key,
            state_bytes,
            summary_bytes,
        } => {
            tracing::info!(
                from = %source,
                contract = %key,
                state_size = state_bytes.len(),
                event = "resync_response_received",
                "Received ResyncResponse with full state"
            );

            // Apply the full state using an update
            let state = freenet_stdlib::prelude::State::from(state_bytes.clone());
            let update_data = freenet_stdlib::prelude::UpdateData::State(state);

            // Send to contract handler
            use crate::contract::ContractHandlerEvent;
            match op_manager
                .notify_contract_handler(ContractHandlerEvent::UpdateQuery {
                    key,
                    data: update_data,
                    related_contracts: Default::default(),
                })
                .await
            {
                Ok(ContractHandlerEvent::UpdateResponse {
                    new_value: Ok(_), ..
                }) => {
                    tracing::info!(
                        from = %source,
                        contract = %key,
                        event = "resync_applied",
                        changed = true,
                        "ResyncResponse state applied successfully"
                    );
                }
                Ok(ContractHandlerEvent::UpdateNoChange { .. }) => {
                    tracing::info!(
                        from = %source,
                        contract = %key,
                        event = "resync_applied",
                        changed = false,
                        "ResyncResponse state unchanged (already had this state)"
                    );
                }
                Ok(other) => {
                    // Display, not Debug, for `other` — `?other` Debug-prints
                    // the full UpdateResponse, expands the inner
                    // anyhow::Error, and emits a ~15-line backtrace per call
                    // under queue saturation (issue #4251).
                    // ContractHandlerEvent's hand-written Display
                    // (`contract/handler.rs:706`) gives a single-line variant
                    // summary without expanding nested anyhow chains.
                    tracing::debug!(
                        from = %source,
                        contract = %key,
                        event = "resync_failed",
                        response = %other,
                        "Unexpected response to resync update"
                    );
                }
                Err(e) => {
                    tracing::error!(
                        from = %source,
                        contract = %key,
                        event = "resync_failed",
                        error = %e,
                        "Failed to apply resync state"
                    );
                }
            }

            // Update the peer's summary in our interest tracker
            let peer_key = get_peer_key_from_addr(op_manager, source);
            if let Some(pk) = peer_key {
                let summary = freenet_stdlib::prelude::StateSummary::from(summary_bytes);
                op_manager
                    .interest_manager
                    .update_peer_summary(&key, &pk, Some(summary));
            }

            // No response needed
            None
        }
    }
}

/// Get the contract state from the state store.
async fn get_contract_state(
    op_manager: &Arc<OpManager>,
    key: &freenet_stdlib::prelude::ContractKey,
) -> Option<freenet_stdlib::prelude::WrappedState> {
    get_contract_state_by_id(op_manager, key.id())
        .await
        .map(|(_, state)| state)
}

/// Get the contract state by instance ID, returning both the full `ContractKey` and state.
///
/// Used for proactive state sync when proximity cache discovers overlapping contracts,
/// where we only have a `ContractInstanceId` (not a full `ContractKey`).
async fn get_contract_state_by_id(
    op_manager: &Arc<OpManager>,
    instance_id: &freenet_stdlib::prelude::ContractInstanceId,
) -> Option<(
    freenet_stdlib::prelude::ContractKey,
    freenet_stdlib::prelude::WrappedState,
)> {
    use crate::contract::ContractHandlerEvent;

    match op_manager
        .notify_contract_handler(ContractHandlerEvent::GetQuery {
            instance_id: *instance_id,
            return_contract_code: false,
        })
        .await
    {
        Ok(ContractHandlerEvent::GetResponse {
            key: Some(key),
            response: Ok(store_response),
        }) => store_response.state.map(|state| (key, state)),
        Ok(ContractHandlerEvent::GetResponse {
            response: Err(e), ..
        }) => {
            tracing::warn!(
                contract = %instance_id,
                error = %e,
                "Failed to get contract state by instance id"
            );
            None
        }
        _ => None,
    }
}

/// Get the contract state summary using the contract's summarize_state method.
async fn get_contract_summary(
    op_manager: &Arc<OpManager>,
    key: &freenet_stdlib::prelude::ContractKey,
) -> Option<freenet_stdlib::prelude::StateSummary<'static>> {
    use crate::contract::ContractHandlerEvent;

    match op_manager
        .notify_contract_handler(ContractHandlerEvent::GetSummaryQuery { key: *key })
        .await
    {
        Ok(ContractHandlerEvent::GetSummaryResponse {
            summary: Ok(summary),
            ..
        }) => Some(summary),
        Ok(ContractHandlerEvent::GetSummaryResponse {
            summary: Err(e), ..
        }) => {
            // Fires repeatedly when the executor queue is saturated for a hot
            // contract (issue #4251). Demoted to debug because the actionable
            // signal is the queue saturation itself, not the per-summary
            // failure.
            tracing::debug!(
                contract = %key,
                error = %e,
                "Failed to get contract summary"
            );
            None
        }
        _ => None,
    }
}

/// Get the PeerKey for a socket address.
fn get_peer_key_from_addr(
    op_manager: &Arc<OpManager>,
    addr: std::net::SocketAddr,
) -> Option<crate::ring::interest::PeerKey> {
    op_manager
        .ring
        .connection_manager
        .get_peer_by_addr(addr)
        .map(|pkl| crate::ring::interest::PeerKey::from(pkl.pub_key.clone()))
}

/// Attempts to subscribe to a contract. Thin wrapper around
/// [`subscribe_with_id`] that allocates a fresh transaction.
#[allow(dead_code)]
pub async fn subscribe(
    op_manager: Arc<OpManager>,
    instance_id: ContractInstanceId,
    client_id: Option<ClientId>,
) -> Result<Transaction, OpError> {
    subscribe_with_id(op_manager, instance_id, client_id, None).await
}

/// Subscribe to a contract with a specific transaction ID (for
/// deduplication).
///
/// Entry point for **client-initiated** SUBSCRIBE only. Other callers
/// (executor auto-subscribe, ring renewals, PUT/GET sub-op fallback)
/// invoke their own drivers directly — `run_executor_subscribe`,
/// `run_renewal_subscribe`, `run_client_subscribe`. `is_renewal` is
/// accepted only by `run_renewal_subscribe`, so renewal misrouting is
/// a compile error.
///
/// # Parameters
///
/// - `client_id`: If set, registers a subscription-result waiter via
///   `ch_outbound.waiting_for_subscription_result`. Both WS call sites
///   in `client_events.rs` leave this `None` because they pre-register
///   a transaction-result waiter via `waiting_for_transaction_result`.
/// - `transaction_id`: Client-visible tx id. If `None`, a fresh one is
///   allocated — currently only the dead-code wrapper `subscribe()`
///   does this.
pub async fn subscribe_with_id(
    op_manager: Arc<OpManager>,
    instance_id: ContractInstanceId,
    client_id: Option<ClientId>,
    transaction_id: Option<Transaction>,
) -> Result<Transaction, OpError> {
    let client_tx = match transaction_id {
        Some(id) => id,
        None => Transaction::new::<subscribe::SubscribeMsg>(),
    };

    if let Some(client_id) = client_id {
        use crate::client_events::RequestId;
        // Generate a default RequestId for internal subscription operations.
        // Legacy behaviour preserved: callers that pass a `client_id` expect
        // the subscription-result waiter to be registered here. The WS path
        // does not hit this branch (it pre-registers its own waiter).
        let request_id = RequestId::new();
        if let Err(e) = op_manager
            .ch_outbound
            .waiting_for_subscription_result(client_tx, instance_id, client_id, request_id)
            .await
        {
            tracing::warn!(tx = %client_tx, error = %e, "failed to register subscription result waiter");
        }
    }

    // Spawn the driver and return the client-visible tx immediately.
    // The driver owns retries, peer selection, local completion, and
    // result delivery via `result_router_tx`.
    subscribe::start_client_subscribe(op_manager, instance_id, client_tx).await
}

/// The identifier of a peer in the network: a known public key and socket address.
///
/// This is a type alias for [`ring::KnownPeerKeyLocation`], which bundles a peer's
/// cryptographic identity (public key) with its guaranteed-known network address.
///
/// Use `KnownPeerKeyLocation` directly when you need the full type name for clarity.
/// Use `PeerKeyLocation` when the address may be unknown (e.g., during NAT traversal).
pub type PeerId = crate::ring::KnownPeerKeyLocation;

pub async fn run_local_node(
    mut executor: Executor,
    socket: WebsocketApiConfig,
) -> anyhow::Result<()> {
    if !crate::server::is_private_ip(&socket.address) {
        anyhow::bail!(
            "invalid ip: {}, only loopback and private network addresses are allowed",
            socket.address
        )
    }

    let (mut gw, mut ws_proxy) = crate::server::serve_client_api_in(socket).await?;

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
            origin_contract,
            ..
        } = req;
        tracing::debug!(client_id = %id, ?token, "Received OpenRequest -> {request}");

        let res = match *request {
            ClientRequest::ContractOp(op) => {
                executor
                    .contract_requests(op, id, notification_channel)
                    .await
            }
            ClientRequest::DelegateOp(op) => {
                // Use the origin_contract already resolved by the WebSocket/HTTP client API
                // instead of re-looking up from gw.origin_contracts (which could fail
                // if the token expired between WebSocket connect and this request)
                let op_name = match op {
                    DelegateRequest::RegisterDelegate { .. } => "RegisterDelegate",
                    DelegateRequest::ApplicationMessages { .. } => "ApplicationMessages",
                    DelegateRequest::UnregisterDelegate(_) => "UnregisterDelegate",
                    _ => "Unknown",
                };
                tracing::debug!(
                    op_name = ?op_name,
                    ?origin_contract,
                    "Handling ClientRequest::DelegateOp"
                );
                executor.delegate_request(op, origin_contract.as_ref(), None)
            }
            ClientRequest::Disconnect { cause } => {
                if let Some(cause) = cause {
                    tracing::info!("disconnecting cause: {cause}");
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

pub async fn run_network_node(mut node: Node) -> anyhow::Result<()> {
    tracing::info!("Starting node");

    let is_gateway = node.inner.is_gateway;
    let location = if let Some(loc) = node.inner.location {
        Some(loc)
    } else {
        is_gateway
            .then(|| {
                node.inner
                    .peer_id
                    .as_ref()
                    .map(|id| Location::from_address(&id.socket_addr()))
            })
            .flatten()
    };

    if let Some(location) = location {
        tracing::info!("Setting initial location: {location}");
        node.update_location(location);
    }

    match node.run().await {
        Ok(_) => {
            if is_gateway {
                tracing::info!("Gateway finished");
            } else {
                tracing::info!("Node finished");
            }

            Ok(())
        }
        Err(e) => {
            tracing::error!("{e}");
            Err(e)
        }
    }
}

#[cfg(test)]
mod tests {
    use std::net::{Ipv4Addr, Ipv6Addr};

    use super::*;
    use rstest::rstest;

    /// Source-level pins for the three log sites in this file that were
    /// demoted / format-fixed in PR #4252 for issue #4251. Each pin
    /// asserts the macro family of the call site by scanning a 400-byte
    /// window before the anchor message. Same shape as the
    /// `bug-prevention-patterns.md` FreeConsole pins in `service.rs`.
    /// Window widened from 240 to 400 in re-review #3 to absorb future
    /// added structured fields without false-breaking the pin.
    fn assert_log_site_pin(needle: &str, must_contain: &[&str], must_not_contain: &[&str]) {
        let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("src/node.rs");
        let source = std::fs::read_to_string(&path)
            .unwrap_or_else(|e| panic!("must read own source at {}: {e}", path.display()));
        let idx = source
            .find(needle)
            .unwrap_or_else(|| panic!("log message `{needle}` must still exist in source"));
        let start = idx.saturating_sub(400);
        let window = &source[start..idx];
        for needle in must_contain {
            assert!(
                window.contains(needle),
                "site `{needle}` must appear in the 240-byte window before message:\n{window}",
                needle = needle
            );
        }
        for forbidden in must_not_contain {
            assert!(
                !window.contains(forbidden),
                "site must NOT contain `{forbidden}` (would restore an issue #4251 regression):\n{window}",
                forbidden = forbidden
            );
        }
    }

    #[test]
    fn summary_mismatch_in_interest_sync_logs_at_debug_pin_test() {
        // Demoted from INFO to DEBUG to stop dominating peer logs on
        // hot contracts. Per #4251 review (testing reviewer #1).
        assert_log_site_pin(
            "Summary mismatch in interest sync \u{2014} syncing state to stale peer",
            &["tracing::debug!"],
            &["tracing::info!", "tracing::warn!"],
        );
    }

    #[test]
    fn unexpected_resync_response_uses_display_not_debug_pin_test() {
        // Switched from `response = ?other` (Debug-expanded UpdateResponse
        // → anyhow chain → ~15-line backtrace per call) to `response =
        // %other` (single-line Display via ContractHandlerEvent's
        // hand-written impl). Per #4251 review (code-first + Codex).
        assert_log_site_pin(
            "Unexpected response to resync update",
            &["tracing::debug!", "response = %other"],
            &["response = ?other", "tracing::warn!", "tracing::info!"],
        );
    }

    #[test]
    fn failed_to_get_contract_summary_logs_at_debug_pin_test() {
        // Demoted from WARN to DEBUG: this site fires repeatedly when
        // the executor queue is saturated for a hot contract (#4251).
        // The actionable signal is the queue saturation itself, not
        // the per-summary failure. Caught by rule-review on PR #4252.
        assert_log_site_pin(
            "Failed to get contract summary",
            &["tracing::debug!"],
            &["tracing::warn!", "tracing::info!"],
        );
    }

    // Hostname resolution tests
    #[tokio::test]
    async fn test_hostname_resolution_localhost() {
        let addr = Address::Hostname("localhost".to_string());
        let socket_addr = NodeConfig::parse_socket_addr(&addr).await.unwrap();
        assert!(
            socket_addr.ip() == IpAddr::V4(Ipv4Addr::LOCALHOST)
                || socket_addr.ip() == IpAddr::V6(Ipv6Addr::LOCALHOST)
        );
        assert!(socket_addr.port() > 1024);
    }

    #[tokio::test]
    async fn test_hostname_resolution_with_port() {
        let addr = Address::Hostname("google.com:8080".to_string());
        let socket_addr = NodeConfig::parse_socket_addr(&addr).await.unwrap();
        assert_eq!(socket_addr.port(), 8080);
    }

    #[tokio::test]
    async fn test_hostname_resolution_with_trailing_dot() {
        // DNS names with trailing dot should be handled
        let addr = Address::Hostname("localhost.".to_string());
        let result = NodeConfig::parse_socket_addr(&addr).await;
        // This should either succeed or fail gracefully
        if let Ok(socket_addr) = result {
            assert!(
                socket_addr.ip() == IpAddr::V4(Ipv4Addr::LOCALHOST)
                    || socket_addr.ip() == IpAddr::V6(Ipv6Addr::LOCALHOST)
            );
        }
    }

    #[tokio::test]
    async fn test_hostname_resolution_direct_socket_addr() {
        let socket = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(192, 168, 1, 1)), 8080);
        let addr = Address::HostAddress(socket);
        let resolved = NodeConfig::parse_socket_addr(&addr).await.unwrap();
        assert_eq!(resolved, socket);
    }

    #[tokio::test]
    async fn test_hostname_resolution_invalid_port() {
        let addr = Address::Hostname("localhost:not_a_port".to_string());
        let result = NodeConfig::parse_socket_addr(&addr).await;
        assert!(result.is_err());
    }

    // Superseded: Old addr-only equality (same_addr_different_keys → equal) was replaced
    // with full-field equality (addr + pub_key) in #3616. Kept as historical documentation
    // of the old behavior.
    #[ignore]
    #[rstest]
    #[case::same_addr_different_keys(8080, 8080, true)]
    #[case::different_addr_same_key(8080, 8081, false)]
    fn test_peer_id_equality(#[case] port1: u16, #[case] port2: u16, #[case] expected_equal: bool) {
        let keypair1 = TransportKeypair::new();
        let keypair2 = TransportKeypair::new();
        let addr1 = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), port1);
        let addr2 = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), port2);
        // Old behavior: PeerId equality was addr-only, so same_addr_different_keys was true.
        // New behavior: equality uses full fields, so same_addr_different_keys is false.
        let peer1 = PeerId::new(keypair1.public().clone(), addr1);
        let peer2 = PeerId::new(keypair2.public().clone(), addr2);
        assert_eq!(peer1 == peer2, expected_equal);
    }

    // PeerId (KnownPeerKeyLocation) equality tests
    // PeerId now uses full-field equality (both addr and pub_key), matching identity semantics.
    #[test]
    fn test_peer_id_equality_same_key_same_addr() {
        let keypair = TransportKeypair::new();
        let addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 8080);
        let peer1 = PeerId::new(keypair.public().clone(), addr);
        let peer2 = PeerId::new(keypair.public().clone(), addr);
        assert_eq!(peer1, peer2);
    }

    #[test]
    fn test_peer_id_equality_different_key_same_addr() {
        let keypair1 = TransportKeypair::new();
        let keypair2 = TransportKeypair::new();
        let addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 8080);
        // Different keys at same addr are different peers (key is identity)
        let peer1 = PeerId::new(keypair1.public().clone(), addr);
        let peer2 = PeerId::new(keypair2.public().clone(), addr);
        assert_ne!(peer1, peer2);
    }

    #[test]
    fn test_peer_id_equality_different_addr() {
        let keypair = TransportKeypair::new();
        let addr1 = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 8080);
        let addr2 = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 8081);
        let peer1 = PeerId::new(keypair.public().clone(), addr1);
        let peer2 = PeerId::new(keypair.public().clone(), addr2);
        assert_ne!(peer1, peer2);
    }

    #[rstest]
    #[case::lower_port_first(8080, 8081)]
    #[case::high_port_diff(1024, 65535)]
    fn test_peer_id_ordering(#[case] lower_port: u16, #[case] higher_port: u16) {
        let keypair = TransportKeypair::new();
        let addr1 = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), lower_port);
        let addr2 = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), higher_port);

        let peer1 = PeerId::new(keypair.public().clone(), addr1);
        let peer2 = PeerId::new(keypair.public().clone(), addr2);

        assert!(peer1 < peer2);
        assert!(peer2 > peer1);
    }

    #[test]
    fn test_peer_id_hash_consistency() {
        use std::collections::hash_map::DefaultHasher;
        use std::hash::{Hash, Hasher};

        let keypair = TransportKeypair::new();
        let addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 8080);

        let peer1 = PeerId::new(keypair.public().clone(), addr);
        let peer2 = PeerId::new(keypair.public().clone(), addr);

        let mut hasher1 = DefaultHasher::new();
        let mut hasher2 = DefaultHasher::new();
        peer1.hash(&mut hasher1);
        peer2.hash(&mut hasher2);

        // Same key + same address should produce same hash
        assert_eq!(hasher1.finish(), hasher2.finish());
    }

    #[test]
    fn test_peer_id_random_produces_unique() {
        let peer1 = PeerId::random();
        let peer2 = PeerId::random();

        // Random peers should have different addresses (with high probability)
        assert_ne!(peer1.socket_addr(), peer2.socket_addr());
    }

    #[test]
    fn test_peer_id_serialization() {
        let peer = PeerId::random();
        let bytes = peer.to_bytes();
        assert!(!bytes.is_empty());

        // Should be deserializable
        let deserialized: PeerId = bincode::deserialize(&bytes).unwrap();
        assert_eq!(peer.socket_addr(), deserialized.socket_addr());
    }

    #[test]
    fn test_peer_id_display() {
        let peer = PeerId::random();
        let display = format!("{}", peer);
        let debug = format!("{:?}", peer);

        // Display and Debug should produce the same output
        assert_eq!(display, debug);
        // Should not be empty
        assert!(!display.is_empty());
    }

    // InitPeerNode tests
    #[test]
    fn test_init_peer_node_construction() {
        let keypair = TransportKeypair::new();
        let addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(192, 168, 1, 1)), 8080);
        let peer_key_location = PeerKeyLocation::new(keypair.public().clone(), addr);
        let location = Location::new(0.5);

        let init_peer = InitPeerNode::new(peer_key_location.clone(), location);

        assert_eq!(init_peer.peer_key_location, peer_key_location);
        assert_eq!(init_peer.location, location);
    }

    // Tests for `try_forward_driver_reply`.
    //
    // The bypass routes a reply directly to an awaiting
    // `OpCtx::send_and_await` caller. These tests cover the
    // helper's contract; end-to-end branch coverage lives in the
    // per-driver tests.
    mod callback_forward_tests {
        use super::super::try_forward_driver_reply;
        use crate::message::{MessageStats, NetMessage, NetMessageV1, Transaction};
        use crate::operations::connect::ConnectMsg;

        fn dummy_reply() -> NetMessage {
            NetMessage::V1(NetMessageV1::Aborted(Transaction::new::<ConnectMsg>()))
        }

        // ───────────────────────────────────────────────────────────
        // Tests for `try_forward_driver_reply`.
        //
        // The bypass routes a reply directly to an awaiting
        // `OpCtx::send_and_await` caller. These tests cover the
        // helper's contract; end-to-end branch coverage lives in the
        // per-driver tests.
        // ───────────────────────────────────────────────────────────

        #[tokio::test]
        async fn bypass_forwards_when_callback_registered() {
            let (tx, mut rx) = tokio::sync::mpsc::channel::<NetMessage>(1);
            let reply = dummy_reply();
            let expected_id = *reply.id();

            let taken = try_forward_driver_reply(Some(&tx), reply, "subscribe");
            assert!(taken, "callback present → bypass must be taken");

            let received = rx
                .try_recv()
                .expect("helper should forward the reply to the callback");
            assert_eq!(*received.id(), expected_id);
        }

        #[tokio::test]
        async fn bypass_returns_false_when_no_callback() {
            // No callback registered → caller must fall through to legacy
            // `handle_op_request`. The helper must not panic and must
            // return `false`.
            let taken = try_forward_driver_reply(None, dummy_reply(), "subscribe");
            assert!(!taken, "no callback → bypass must not be taken");
        }

        #[tokio::test]
        async fn bypass_returns_true_even_when_receiver_dropped() {
            // Structural rule: once a callback is registered, the bypass
            // is taken — the legacy path must NOT run regardless of
            // whether the task-side receiver is still alive. If the task
            // was cancelled and dropped its receiver, `try_send` fails
            // with `Closed` and we log, but we still return `true` so
            // the caller returns `Ok(None)` from the pipeline.
            //
            // Running `handle_op_request` in this case would call
            // `load_or_init` on an empty DashMap and return
            // `OpNotPresent`, which is meaningless for a tx owned by a
            // (now-dead) task and pointlessly wastes a pipeline
            // iteration.
            let (tx, rx) = tokio::sync::mpsc::channel::<NetMessage>(1);
            drop(rx);

            let taken = try_forward_driver_reply(Some(&tx), dummy_reply(), "subscribe");
            assert!(
                taken,
                "callback present but receiver dropped → bypass still taken"
            );
        }

        /// Pin the bypass call site. Without this regression guard a
        /// future refactor could delete the
        /// `try_forward_driver_reply` invocation in the SUBSCRIBE
        /// branch of `handle_pure_network_message_v1` and the unit tests
        /// on the helper itself would still pass — because unit coverage
        /// on the helper only proves the helper works, not that it's
        /// wired in. Integration (simulation) failures would catch it
        /// eventually but as end-to-end hangs, which is a noisy signal.
        ///
        /// This test reads the `node.rs` source at compile time via
        /// `include_str!` and asserts that the SUBSCRIBE branch of
        /// `handle_pure_network_message_v1` invokes
        /// `try_forward_driver_reply` before running
        /// `handle_op_request`. A refactor that deletes the bypass call
        /// will fail this test at the unit-test level (review finding
        /// Testing #1).
        ///
        /// If the match arm structure changes (e.g. SUBSCRIBE branch
        /// moves or is renamed), the string patterns below need to be
        /// updated to match. That's a load-bearing but intentional
        /// coupling — the whole point is to fail loudly when the wiring
        /// changes so the change is noticed.
        #[test]
        fn bypass_is_wired_into_subscribe_branch_regression_guard() {
            // Full file text, read at compile time.
            const SOURCE: &str = include_str!("node.rs");

            // Locate the SUBSCRIBE branch of handle_pure_network_message_v1.
            // Use a runtime-built needle so this test cannot self-match
            // its own anchor string in the test source below.
            let subscribe_branch_anchor: String =
                ["NetMessageV1::", "Subscribe(ref op)", " => {"].concat();
            let branch_start = SOURCE.find(&subscribe_branch_anchor).expect(
                "SUBSCRIBE branch of handle_pure_network_message_v1 not found; \
                         the match arm has been renamed or moved — update this regression guard",
            );

            // Bound the window at the end-of-SUBSCRIBE-arm sentinel
            // ("Non-transactional message types:" header that precedes
            // the next match arm).
            let next_variant_anchor: String = ["// Non-transactional", " message types:"].concat();
            let window_end = SOURCE[branch_start..]
                .find(&next_variant_anchor)
                .expect("end of SUBSCRIBE branch not found — update guard")
                + branch_start;
            let window = &SOURCE[branch_start..window_end];

            // The bypass helper MUST be invoked in the SUBSCRIBE branch.
            // If this assertion fails, either:
            //   (a) the bypass was removed (regression — re-add it), or
            //   (b) the branch was restructured (update this guard).
            assert!(
                window.contains("try_forward_driver_reply("),
                "SUBSCRIBE branch no longer calls \
                 try_forward_driver_reply before relay dispatch. \
                 Either restore the bypass or update this regression \
                 guard if the branch was legitimately refactored."
            );

            // The bypass MUST be gated on Response-only. Without this
            // filter, non-terminal messages like ForwardingAck fill the
            // capacity-1 reply channel and cause UnexpectedOpState
            // (commit 5cb6f37c).
            let response_gate: String = [
                "matches!(op, ",
                "subscribe::SubscribeMsg::Response { .. }",
                ")",
            ]
            .concat();
            assert!(
                window.contains(&response_gate),
                "SUBSCRIBE branch bypass is not gated on Response-only. \
                 Non-terminal messages (ForwardingAck, Unsubscribe) must NOT \
                 be forwarded to the driver channel — they would fill \
                 the capacity-1 reply slot and block the real Response."
            );
        }

        #[tokio::test]
        async fn bypass_does_not_block_when_channel_already_full() {
            // Pin the non-blocking contract: `try_send` on a full
            // channel must fail without blocking the handler. Future
            // refactors must not switch to `.send().await` (see
            // `.claude/rules/channel-safety.md`).
            let (tx, _rx) = tokio::sync::mpsc::channel::<NetMessage>(1);
            // Pre-fill the capacity-1 channel.
            tx.try_send(dummy_reply())
                .expect("capacity-1 channel should accept first message");

            let taken = try_forward_driver_reply(Some(&tx), dummy_reply(), "subscribe");
            assert!(
                taken,
                "callback present but channel full → bypass still taken"
            );
            // The test would hang on regression: blocking `send().await`
            // on a full channel whose receiver is still alive would
            // stall the `#[tokio::test]` runtime indefinitely.
        }

        // Note on per-variant coverage: Phase 1's point is that every op
        // variant of `handle_pure_network_message_v1` can terminate an
        // `OpCtx::send_and_await` round-trip. The helper tested above is
        // variant-agnostic once the `is_operation_completed` guard passes,
        // and each op's own `is_completed` impl is covered by unit tests in
        // `crates/core/src/operations/{connect,put,get,subscribe,update}.rs`.
        // The remaining "do the five branches of `handle_pure_network_message_v1`
        // actually invoke the helper with the matching reply variant?"
        // question is enforced by the compiler — each branch binds `ref op`
        // for the concrete op type and reconstructs the same variant before
        // handing it to `forward_pending_op_result_if_completed`. An
        // end-to-end integration test that spins up a node and exercises
        // `OpCtx::send_and_await` for each op kind belongs alongside
        // the per-op driver suites.

        // ───────────────────────────────────────────────────────────
        // Regression tests for the subscribe-branch message-type
        // filter added in the ForwardingAck fix (5cb6f37c).
        //
        // The bug: `try_forward_driver_reply` was called for ALL
        // subscribe message types (including ForwardingAck). A relay
        // peer's ForwardingAck would fill the capacity-1 reply
        // channel, causing the task to receive it instead of the
        // real Response and fail with UnexpectedOpState.
        //
        // These tests verify the filtering logic that
        // `handle_pure_network_message_v1` applies BEFORE calling the
        // bypass helper: only `SubscribeMsg::Response` is forwarded.
        // ───────────────────────────────────────────────────────────

        use crate::operations::VisitedPeers;
        use crate::operations::subscribe::{SubscribeMsg, SubscribeMsgResult};

        /// Helper: simulate the filtering logic from the SUBSCRIBE
        /// branch of `handle_pure_network_message_v1`. Returns
        /// `true` if the message would be forwarded to the
        /// driver channel (and the branch would return early).
        fn subscribe_branch_would_forward(
            op: &SubscribeMsg,
            callback: Option<&tokio::sync::mpsc::Sender<NetMessage>>,
        ) -> bool {
            matches!(op, SubscribeMsg::Response { .. })
                && try_forward_driver_reply(
                    callback,
                    NetMessage::V1(NetMessageV1::Subscribe(op.clone())),
                    "subscribe",
                )
        }

        #[tokio::test]
        async fn subscribe_response_is_forwarded_to_task() {
            let (tx, mut rx) = tokio::sync::mpsc::channel::<NetMessage>(1);
            let sub_tx = Transaction::new::<SubscribeMsg>();
            let instance_id = freenet_stdlib::prelude::ContractInstanceId::new([1u8; 32]);
            let key = freenet_stdlib::prelude::ContractKey::from_id_and_code(
                instance_id,
                freenet_stdlib::prelude::CodeHash::new([2u8; 32]),
            );
            let op = SubscribeMsg::Response {
                id: sub_tx,
                instance_id,
                result: SubscribeMsgResult::Subscribed { key },
                hop_count: 0,
            };

            let taken = subscribe_branch_would_forward(&op, Some(&tx));
            assert!(taken, "Response with callback → must be forwarded");

            let received = rx.try_recv().expect("Response should be in channel");
            assert_eq!(*received.id(), sub_tx);
        }

        #[tokio::test]
        async fn forwarding_ack_is_not_forwarded_to_task() {
            // ForwardingAck is non-terminal: relay peers send it to
            // signal "I'm working on it". Forwarding it would fill
            // the capacity-1 channel and block the real Response.
            let (tx, mut rx) = tokio::sync::mpsc::channel::<NetMessage>(1);
            let sub_tx = Transaction::new::<SubscribeMsg>();
            let instance_id = freenet_stdlib::prelude::ContractInstanceId::new([3u8; 32]);
            let op = SubscribeMsg::ForwardingAck {
                id: sub_tx,
                instance_id,
            };

            let taken = subscribe_branch_would_forward(&op, Some(&tx));
            assert!(
                !taken,
                "ForwardingAck must NOT be forwarded to task channel"
            );
            assert!(
                rx.try_recv().is_err(),
                "channel must remain empty after ForwardingAck"
            );
        }

        #[tokio::test]
        async fn unsubscribe_is_not_forwarded_to_task() {
            let (tx, mut rx) = tokio::sync::mpsc::channel::<NetMessage>(1);
            let sub_tx = Transaction::new::<SubscribeMsg>();
            let instance_id = freenet_stdlib::prelude::ContractInstanceId::new([4u8; 32]);
            let op = SubscribeMsg::Unsubscribe {
                id: sub_tx,
                instance_id,
            };

            let taken = subscribe_branch_would_forward(&op, Some(&tx));
            assert!(!taken, "Unsubscribe must NOT be forwarded to task channel");
            assert!(rx.try_recv().is_err(), "channel must remain empty");
        }

        #[tokio::test]
        async fn request_is_not_forwarded_to_task() {
            let (tx, mut rx) = tokio::sync::mpsc::channel::<NetMessage>(1);
            let sub_tx = Transaction::new::<SubscribeMsg>();
            let instance_id = freenet_stdlib::prelude::ContractInstanceId::new([5u8; 32]);
            let op = SubscribeMsg::Request {
                id: sub_tx,
                instance_id,
                htl: 5,
                visited: VisitedPeers::new(&sub_tx),
                is_renewal: false,
            };

            let taken = subscribe_branch_would_forward(&op, Some(&tx));
            assert!(!taken, "Request must NOT be forwarded to task channel");
            assert!(rx.try_recv().is_err(), "channel must remain empty");
        }

        #[tokio::test]
        async fn response_without_callback_falls_through() {
            // No callback registered (legacy path) — filter must
            // return false so handle_op_request runs.
            let sub_tx = Transaction::new::<SubscribeMsg>();
            let instance_id = freenet_stdlib::prelude::ContractInstanceId::new([6u8; 32]);
            let op = SubscribeMsg::Response {
                id: sub_tx,
                instance_id,
                result: SubscribeMsgResult::NotFound,
                hop_count: 0,
            };

            let taken = subscribe_branch_would_forward(&op, None);
            assert!(
                !taken,
                "Response without callback → must fall through to legacy path"
            );
        }

        // ───────────────────────────────────────────────────────────
        // Regression guard: PUT branch of handle_pure_network_message_v1
        // must call try_forward_driver_reply before relay dispatch,
        // gated on Response|ResponseStreaming only.
        // ───────────────────────────────────────────────────────────

        #[test]
        fn bypass_is_wired_into_put_branch_regression_guard() {
            const SOURCE: &str = include_str!("node.rs");

            let put_branch_anchor = "NetMessageV1::Put(ref op) => {";
            let branch_start = SOURCE.find(put_branch_anchor).expect(
                "PUT branch of handle_pure_network_message_v1 not found; \
                 the match arm has been renamed or moved — update this regression guard",
            );

            // End the window at the next NetMessageV1 variant to bound
            // the search to the PUT arm only.
            let next_variant = "NetMessageV1::Get(ref op) => {";
            let window_end = SOURCE[branch_start..]
                .find(next_variant)
                .expect("could not find end of PUT arm")
                + branch_start;
            let window = &SOURCE[branch_start..window_end];

            assert!(
                window.contains("try_forward_driver_reply("),
                "PUT branch no longer calls try_forward_driver_reply. \
                 Restore the bypass or update this regression guard."
            );

            assert!(
                window.contains("put::PutMsg::Response { .. }"),
                "PUT branch bypass is not gated on Response. \
                 Non-terminal messages must NOT be forwarded to the driver channel."
            );

            assert!(
                window.contains("put::PutMsg::ResponseStreaming { .. }"),
                "PUT branch bypass is not gated on ResponseStreaming. \
                 Both terminal variants must be forwarded."
            );

            // The legacy fallthrough must NOT return. Compose needles
            // at runtime so the assert source itself does not contain
            // them.
            let legacy_dispatch_needle = format!("handle{}::<put::PutOp, _>", "_op_request");
            assert!(
                !window.contains(&legacy_dispatch_needle),
                "PUT branch must not call legacy state-machine dispatch"
            );
            let dashmap_gate_needle = format!("has{}_op", "_put");
            assert!(
                !window.contains(&dashmap_gate_needle),
                "PUT branch must not gate dispatch on per-op DashMap existence"
            );
        }

        // ───────────────────────────────────────────────────────────
        // Per-variant filter tests for the PUT branch bypass.
        // Only Response and ResponseStreaming may be forwarded; all
        // other variants must fall through to relay dispatch.
        // ───────────────────────────────────────────────────────────

        use crate::operations::put::PutMsg;
        use freenet_stdlib::prelude::*;

        fn dummy_put_key(a: u8, b: u8) -> ContractKey {
            ContractKey::from_id_and_code(ContractInstanceId::new([a; 32]), CodeHash::new([b; 32]))
        }

        fn put_branch_would_forward(
            op: &PutMsg,
            callback: Option<&tokio::sync::mpsc::Sender<NetMessage>>,
        ) -> bool {
            matches!(
                op,
                PutMsg::Response { .. } | PutMsg::ResponseStreaming { .. }
            ) && try_forward_driver_reply(
                callback,
                NetMessage::V1(NetMessageV1::Put(op.clone())),
                "put",
            )
        }

        #[tokio::test]
        async fn put_response_is_forwarded_to_task() {
            let (tx, mut rx) = tokio::sync::mpsc::channel::<NetMessage>(1);
            let put_tx = Transaction::new::<PutMsg>();
            let key = dummy_put_key(10, 11);
            let op = PutMsg::Response {
                id: put_tx,
                key,
                hop_count: 0,
            };

            let taken = put_branch_would_forward(&op, Some(&tx));
            assert!(taken, "Response with callback → must be forwarded");

            let received = rx.try_recv().expect("Response should be in channel");
            assert_eq!(*received.id(), put_tx);
        }

        #[tokio::test]
        async fn put_response_streaming_is_forwarded_to_task() {
            let (tx, mut rx) = tokio::sync::mpsc::channel::<NetMessage>(1);
            let put_tx = Transaction::new::<PutMsg>();
            let key = dummy_put_key(12, 13);
            let op = PutMsg::ResponseStreaming {
                id: put_tx,
                key,
                continue_forwarding: false,
                hop_count: 0,
            };

            let taken = put_branch_would_forward(&op, Some(&tx));
            assert!(taken, "ResponseStreaming with callback → must be forwarded");

            let received = rx
                .try_recv()
                .expect("ResponseStreaming should be in channel");
            assert_eq!(*received.id(), put_tx);
        }

        #[tokio::test]
        async fn put_forwarding_ack_is_not_forwarded_to_task() {
            let (tx, mut rx) = tokio::sync::mpsc::channel::<NetMessage>(1);
            let put_tx = Transaction::new::<PutMsg>();
            let key = dummy_put_key(14, 15);
            let op = PutMsg::ForwardingAck {
                id: put_tx,
                contract_key: key,
            };

            let taken = put_branch_would_forward(&op, Some(&tx));
            assert!(
                !taken,
                "ForwardingAck must NOT be forwarded to task channel"
            );
            assert!(
                rx.try_recv().is_err(),
                "channel must remain empty after ForwardingAck"
            );
        }

        #[tokio::test]
        async fn put_request_is_not_forwarded_to_task() {
            let (tx, mut rx) = tokio::sync::mpsc::channel::<NetMessage>(1);
            let put_tx = Transaction::new::<PutMsg>();
            let op = PutMsg::Request {
                id: put_tx,
                contract: ContractContainer::Wasm(ContractWasmAPIVersion::V1(
                    WrappedContract::new(
                        std::sync::Arc::new(ContractCode::from(vec![0u8])),
                        Parameters::from(vec![]),
                    ),
                )),
                related_contracts: RelatedContracts::default(),
                value: WrappedState::new(vec![1u8]),
                htl: 5,
                skip_list: std::collections::HashSet::new(),
            };

            let taken = put_branch_would_forward(&op, Some(&tx));
            assert!(!taken, "Request must NOT be forwarded to task channel");
            assert!(rx.try_recv().is_err(), "channel must remain empty");
        }

        #[tokio::test]
        async fn put_response_without_callback_falls_through() {
            let put_tx = Transaction::new::<PutMsg>();
            let key = dummy_put_key(16, 17);
            let op = PutMsg::Response {
                id: put_tx,
                key,
                hop_count: 0,
            };

            let taken = put_branch_would_forward(&op, None);
            assert!(
                !taken,
                "Response without callback → must fall through to legacy path"
            );
        }

        // ───────────────────────────────────────────────────────────
        // Regression guards for the GET branch.
        //
        // Two dispatch layers:
        //   1. Reply bypass: terminal Response/ResponseStreaming for
        //      an active client driver → `try_forward_driver_reply`.
        //   2. Relay dispatch: `GetMsg::Request` →  `start_relay_get`,
        //      with originator loopback mapped to `upstream=own_addr`.
        // ───────────────────────────────────────────────────────────

        #[test]
        fn get_branch_dispatches_relay_driver() {
            const SOURCE: &str = include_str!("node.rs");
            let anchor = "NetMessageV1::Get(ref op) => {";
            let branch_start = SOURCE.find(anchor).expect(
                "GET branch of handle_pure_network_message_v1 not found; \
                 the match arm has been renamed or moved — update this guard",
            );
            // End the window at the next NetMessageV1 variant to bound
            // the search to the GET arm only.
            let next_variant = "NetMessageV1::Update(ref op) => {";
            let window_end = SOURCE[branch_start..]
                .find(next_variant)
                .expect("could not find end of GET arm")
                + branch_start;
            let window = &SOURCE[branch_start..window_end];

            // Reply bypass must precede relay dispatch.
            assert!(
                window.contains("try_forward_driver_reply("),
                "GET branch no longer calls try_forward_driver_reply \
                 before relay dispatch. Restore the bypass."
            );
            assert!(
                window.contains("get::GetMsg::Response { .. }"),
                "GET branch bypass is not gated on Response. \
                 Non-terminal messages must NOT be forwarded to the driver channel."
            );
            assert!(
                window.contains("get::GetMsg::ResponseStreaming { .. }"),
                "GET branch bypass is not gated on ResponseStreaming. \
                 Both terminal variants must be forwarded."
            );

            // Relay dispatch must call start_relay_get.
            assert!(
                window.contains("start_relay_get("),
                "GET branch no longer calls start_relay_get for relay dispatch."
            );

            // Originator loopback (source_addr=None) is mapped to
            // upstream=own_addr, so dispatch is conditional on an
            // effective upstream rather than `source_addr.is_some()`.
            assert!(
                window.contains("effective_upstream") || window.contains("upstream_addr"),
                "GET relay dispatch must thread an effective upstream address \
                 (source_addr or own_addr loopback) into the relay driver."
            );

            // Legacy fallthrough and gate must stay deleted. Compose
            // needles at runtime so the assert source itself does not
            // contain them.
            let legacy_dispatch_needle = format!("handle{}::<get::GetOp, _>", "_op_request");
            assert!(
                !window.contains(&legacy_dispatch_needle),
                "GET branch must NOT call legacy state-machine dispatch"
            );
            let dashmap_gate_needle = format!("has{}_op", "_get");
            assert!(
                !window.contains(&dashmap_gate_needle),
                "GET branch must NOT gate on per-op DashMap existence"
            );

            // Bypass must precede relay dispatch in source order
            // (terminal-reply fast path has priority).
            let bypass_pos = window
                .find("try_forward_driver_reply(")
                .expect("try_forward_driver_reply not found in GET branch");
            let relay_pos = window
                .find("start_relay_get(")
                .expect("start_relay_get not found in GET branch");
            assert!(
                bypass_pos < relay_pos,
                "Reply bypass (try_forward_driver_reply) must \
                 appear BEFORE relay dispatch (start_relay_get) — \
                 swapping order would break the terminal-reply fast \
                 path."
            );
        }

        // ───────────────────────────────────────────────────────────
        // Regression guards for the UPDATE branch.
        //
        // UPDATE is fire-and-forget end-to-end — no upstream reply
        // to await, so no reply bypass exists. Only relay dispatch
        // is wired here.
        // ───────────────────────────────────────────────────────────

        /// Pin: every UPDATE wire variant dispatches to a relay
        /// driver. No legacy fallthrough remains — every reachable
        /// arm spawns a driver and returns `Ok(None)`.
        #[test]
        fn update_branch_dispatches_all_relay_drivers() {
            const SOURCE: &str = include_str!("node.rs");

            let anchor = "NetMessageV1::Update(ref op) => {";
            let branch_start = SOURCE.find(anchor).expect(
                "UPDATE branch of handle_pure_network_message_v1 not found; \
                 the match arm has been renamed or moved — update this regression guard",
            );

            // End the window at the next NetMessageV1 variant to bound
            // the search to the UPDATE arm only.
            let next_variant = "NetMessageV1::Subscribe(ref op) => {";
            let window_end = SOURCE[branch_start..]
                .find(next_variant)
                .expect("could not find end of UPDATE arm")
                + branch_start;
            let window = &SOURCE[branch_start..window_end];

            for driver in [
                "start_relay_request_update(",
                "start_relay_broadcast_to(",
                "start_relay_request_update_streaming(",
                "start_relay_broadcast_to_streaming(",
            ] {
                assert!(
                    window.contains(driver),
                    "UPDATE branch must call {driver} for relay dispatch."
                );
            }

            // Negative pins for the fallthrough: composing
            // needles at runtime so this test's source doesn't trip its
            // own assertion.
            let legacy_call = ["handle_op_request::<update::", "UpdateOp", ", _>"].concat();
            assert!(
                !window.contains(&legacy_call),
                "UPDATE branch must NOT call handle_op_request"
            );
            let dispatch_gate = ["has_", "update_op"].concat();
            assert!(
                !window.contains(&dispatch_gate),
                "UPDATE relay dispatch must NOT consult has_update_op"
            );
        }

        /// Pin: relay UPDATE dispatch is gated on
        /// `source_addr.is_some()`; internal callers must not spawn
        /// drivers.
        #[test]
        fn update_branch_dispatch_gates_on_source_addr() {
            const SOURCE: &str = include_str!("node.rs");

            let anchor = "NetMessageV1::Update(ref op) => {";
            let branch_start = SOURCE.find(anchor).expect("UPDATE branch not found");
            let next_variant = "NetMessageV1::Subscribe(ref op) => {";
            let window_end = SOURCE[branch_start..]
                .find(next_variant)
                .expect("could not find end of UPDATE arm")
                + branch_start;
            let window = &SOURCE[branch_start..window_end];

            assert!(
                window.contains("if let Some(sender_addr) = source_addr"),
                "UPDATE relay dispatch must be gated on source_addr.is_some() — \
                 internal callers must NOT spawn relay drivers."
            );
        }

        // ── Relay PUT dispatch structural pin tests.

        #[test]
        fn put_branch_dispatches_relay_drivers() {
            const SOURCE: &str = include_str!("node.rs");
            let anchor = "NetMessageV1::Put(ref op) => {";
            let branch_start = SOURCE.find(anchor).expect(
                "PUT branch of handle_pure_network_message_v1 not found; \
                 the match arm has been renamed or moved — update this guard",
            );
            // End the window at the next NetMessageV1 variant to bound
            // the search to the PUT arm only.
            let next_variant = "NetMessageV1::Get(ref op) => {";
            let window_end = SOURCE[branch_start..]
                .find(next_variant)
                .expect("could not find end of PUT arm")
                + branch_start;
            let window = &SOURCE[branch_start..window_end];
            assert!(
                window.contains("start_relay_put("),
                "PUT branch no longer calls start_relay_put for relay dispatch."
            );
            assert!(
                window.contains("start_relay_put_streaming("),
                "PUT branch must call start_relay_put_streaming for streaming relay hops."
            );
            // Originator loopback (source_addr=None) is mapped to
            // upstream=own_addr, so dispatch is conditional on an
            // effective upstream rather than `source_addr.is_some()`.
            assert!(
                window.contains("effective_upstream") || window.contains("upstream_addr"),
                "PUT relay dispatch must thread an effective upstream address \
                 (source_addr or own_addr loopback) into the relay drivers."
            );
            // Legacy fallthrough and gate must stay deleted. Compose
            // needles at runtime so the assert source itself does not
            // contain them.
            let legacy_dispatch_needle = format!("handle{}::<put::PutOp, _>", "_op_request");
            assert!(
                !window.contains(&legacy_dispatch_needle),
                "PUT branch must NOT call legacy state-machine dispatch"
            );
            let dashmap_gate_needle = format!("has{}_op", "_put");
            assert!(
                !window.contains(&dashmap_gate_needle),
                "PUT branch must NOT gate on per-op DashMap existence"
            );
        }

        /// Pin: `start_relay_put` (slice A driver) MUST itself perform
        /// the upgrade-on-forward decision. The dispatch gate in
        /// node.rs no longer pre-checks `should_use_streaming` —
        /// the driver re-serializes the merged payload after
        /// `relay_put_store_locally` and conditionally builds either
        /// `PutMsg::Request` or `PutMsg::RequestStreaming` +
        /// `send_stream`.
        #[test]
        fn start_relay_put_handles_upgrade_on_forward() {
            const SOURCE: &str = include_str!("operations/put/op_ctx_task.rs");
            let anchor = "async fn drive_relay_put<CB>(";
            let driver_start = SOURCE
                .find(anchor)
                .expect("drive_relay_put fn not found — has the signature changed?");
            // Bound the search to the function body. End at the next
            // top-level `async fn` declaration in the module.
            let driver_end = SOURCE[driver_start + anchor.len()..]
                .find("\nasync fn ")
                .map(|idx| idx + driver_start + anchor.len())
                .unwrap_or(SOURCE.len());
            let body = &SOURCE[driver_start..driver_end];
            assert!(
                body.contains("should_use_streaming("),
                "drive_relay_put must call should_use_streaming on the merged \
                 payload to decide between non-streaming Request and streaming \
                 upgrade on forward."
            );
            assert!(
                body.contains("PutMsg::RequestStreaming {"),
                "drive_relay_put must build PutMsg::RequestStreaming when the \
                 forwarded payload would exceed streaming_threshold."
            );
            assert!(
                body.contains("send_stream("),
                "drive_relay_put must call NetworkBridge::send_stream for the \
                 raw fragments after the RequestStreaming metadata send."
            );
        }

        // ── Relay SUBSCRIBE dispatch structural pin tests.
        //
        // Every SUBSCRIBE wire variant routes either to the relay
        // driver (Request) or to a dedicated inbound handler
        // (Unsubscribe). Response is forwarded via the reply bypass.

        #[test]
        fn subscribe_branch_dispatches_relay_driver() {
            const SOURCE: &str = include_str!("node.rs");
            let anchor = "NetMessageV1::Subscribe(ref op) => {";
            let branch_start = SOURCE.find(anchor).expect(
                "SUBSCRIBE branch of handle_pure_network_message_v1 not found; \
                 the match arm has been renamed or moved — update this guard",
            );
            // End the window at the next NetMessageV1 variant to bound
            // the search to the SUBSCRIBE arm only.
            let next_variant = "// Non-transactional message types:";
            let window_end = SOURCE[branch_start..]
                .find(next_variant)
                .expect("could not find end of SUBSCRIBE arm")
                + branch_start;
            let window = &SOURCE[branch_start..window_end];

            // Terminal-reply bypass must still be present (gated on
            // SubscribeMsg::Response).
            assert!(
                window.contains("try_forward_driver_reply("),
                "SUBSCRIBE branch no longer calls try_forward_driver_reply \
                 before relay dispatch — restore it."
            );
            assert!(
                window.contains("subscribe::SubscribeMsg::Response { .. }"),
                "SUBSCRIBE branch bypass is not gated on Response. \
                 Non-terminal messages must NOT be forwarded to the driver channel."
            );

            // Relay dispatch must call start_relay_subscribe and route
            // the Unsubscribe variant through the inbound handler.
            assert!(
                window.contains("start_relay_subscribe("),
                "SUBSCRIBE branch no longer calls start_relay_subscribe for relay \
                 dispatch — restore it."
            );
            assert!(
                window.contains("handle_unsubscribe_inbound("),
                "SUBSCRIBE branch must call handle_unsubscribe_inbound \
                 for Unsubscribe wire messages."
            );

            // Originator loopback (source_addr=None) is mapped to
            // upstream=own_addr.
            assert!(
                window.contains("effective_upstream") || window.contains("upstream_addr"),
                "SUBSCRIBE relay dispatch must thread an effective upstream address \
                 (source_addr or own_addr loopback) into the relay driver."
            );

            // Legacy fallthrough and gate must stay deleted. Compose
            // needles at runtime so the assert source itself does not
            // contain them.
            let legacy_dispatch_needle =
                format!("handle{}::<subscribe::SubscribeOp, _>", "_op_request");
            assert!(
                !window.contains(&legacy_dispatch_needle),
                "SUBSCRIBE branch must NOT call legacy state-machine dispatch"
            );
            let dashmap_gate_needle = format!("has{}_op", "_subscribe");
            assert!(
                !window.contains(&dashmap_gate_needle),
                "SUBSCRIBE branch must NOT gate on per-op DashMap existence"
            );

            // Bypass must precede relay dispatch in source order
            // (terminal-reply fast path has priority).
            let bypass_pos = window
                .find("try_forward_driver_reply(")
                .expect("try_forward_driver_reply not found in SUBSCRIBE branch");
            let relay_pos = window
                .find("start_relay_subscribe(")
                .expect("start_relay_subscribe not found in SUBSCRIBE branch");
            assert!(
                bypass_pos < relay_pos,
                "SUBSCRIBE bypass (try_forward_driver_reply) must appear \
                 BEFORE relay dispatch (start_relay_subscribe). Swapping order \
                 would break the client-driver terminal-reply fast path."
            );
        }
    }

    /// Tests for `fill_connect_response_acceptor_addr`. The driver
    /// does not see `source_addr`, so the dispatch site must rewrite
    /// the payload before forwarding.
    mod fill_connect_response_acceptor_addr_tests {
        use std::net::{IpAddr, Ipv4Addr, SocketAddr};

        use super::super::fill_connect_response_acceptor_addr;
        use crate::message::Transaction;
        use crate::operations::connect::{ConnectMsg, ConnectResponse};
        use crate::ring::{PeerAddr, PeerKeyLocation};

        fn dummy_unknown_pkl() -> PeerKeyLocation {
            let pkl = PeerKeyLocation::random();
            PeerKeyLocation {
                pub_key: pkl.pub_key,
                peer_addr: PeerAddr::Unknown,
            }
        }

        fn known_addr() -> SocketAddr {
            SocketAddr::new(IpAddr::V4(Ipv4Addr::new(203, 0, 113, 5)), 50051)
        }

        #[test]
        fn fills_unknown_acceptor_addr_from_source_addr() {
            let id = Transaction::new::<ConnectMsg>();
            let payload = ConnectResponse {
                acceptor: dummy_unknown_pkl(),
            };
            let msg = ConnectMsg::Response { id, payload };

            let source = known_addr();
            let filled = fill_connect_response_acceptor_addr(msg, Some(source));

            #[allow(clippy::wildcard_enum_match_arm)]
            match filled {
                ConnectMsg::Response { payload, .. } => {
                    assert_eq!(payload.acceptor.socket_addr(), Some(source));
                }
                other => panic!("expected Response, got {other:?}"),
            }
        }

        #[test]
        fn leaves_known_acceptor_addr_unchanged() {
            let id = Transaction::new::<ConnectMsg>();
            let original_addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(192, 0, 2, 7)), 12345);
            let pkl = PeerKeyLocation::random();
            let payload = ConnectResponse {
                acceptor: PeerKeyLocation {
                    pub_key: pkl.pub_key,
                    peer_addr: PeerAddr::Known(original_addr),
                },
            };
            let msg = ConnectMsg::Response { id, payload };

            let filled = fill_connect_response_acceptor_addr(msg, Some(known_addr()));

            #[allow(clippy::wildcard_enum_match_arm)]
            match filled {
                ConnectMsg::Response { payload, .. } => {
                    assert_eq!(
                        payload.acceptor.socket_addr(),
                        Some(original_addr),
                        "fill must NOT overwrite a known acceptor address"
                    );
                }
                other => panic!("expected Response, got {other:?}"),
            }
        }

        #[test]
        fn unknown_acceptor_without_source_addr_passes_through() {
            // No source_addr available (e.g. inbound delivery dropped it).
            // The helper must not panic; the unknown address survives so
            // the driver's downstream `socket_addr()` check logs+drops.
            let id = Transaction::new::<ConnectMsg>();
            let payload = ConnectResponse {
                acceptor: dummy_unknown_pkl(),
            };
            let msg = ConnectMsg::Response { id, payload };

            let filled = fill_connect_response_acceptor_addr(msg, None);

            #[allow(clippy::wildcard_enum_match_arm)]
            match filled {
                ConnectMsg::Response { payload, .. } => {
                    assert!(
                        payload.acceptor.peer_addr.is_unknown(),
                        "fill must remain Unknown when source_addr is None"
                    );
                }
                other => panic!("expected Response, got {other:?}"),
            }
        }

        #[test]
        fn rejected_variant_passes_through_untouched() {
            // The bypass forwards both Response and Rejected; only Response
            // carries an acceptor. The helper must leave Rejected alone.
            use crate::ring::Location;
            let id = Transaction::new::<ConnectMsg>();
            let dl = Location::new(0.42);
            let msg = ConnectMsg::Rejected {
                id,
                desired_location: dl,
            };

            let filled = fill_connect_response_acceptor_addr(msg, Some(known_addr()));

            #[allow(clippy::wildcard_enum_match_arm)]
            match filled {
                ConnectMsg::Rejected {
                    id: rid,
                    desired_location,
                } => {
                    assert_eq!(rid, id);
                    assert_eq!(desired_location, dl);
                }
                other => panic!("expected Rejected, got {other:?}"),
            }
        }
    }

    /// Regression guards for the CONNECT bypass `matches!` predicate.
    ///
    /// The relay-CONNECT driver owns the entire tx lifetime in task
    /// locals, so all four non-`Request` `ConnectMsg` variants
    /// (Response, Rejected, ObservedAddress, ConnectFailed) must
    /// reach the per-tx waiter receiver. `Request` is the spawn
    /// signal and is handled by the dispatch gate.
    mod connect_bypass_coverage_guards {
        const SOURCE: &str = include_str!("node.rs");

        fn connect_branch_window() -> &'static str {
            let branch_anchor = "NetMessageV1::Connect(ref op) => {";
            let branch_start = SOURCE.find(branch_anchor).expect(
                "Connect branch of handle_pure_network_message_v1 not found; \
                 the match arm has been renamed or moved — update this guard",
            );

            let next_variant_anchor = "NetMessageV1::Put(ref op) => {";
            let window_end = SOURCE[branch_start..]
                .find(next_variant_anchor)
                .expect("end of Connect branch not found — update guard")
                + branch_start;

            &SOURCE[branch_start..window_end]
        }

        #[test]
        fn connect_branch_bypass_forwards_response() {
            assert!(
                connect_branch_window().contains("connect::ConnectMsg::Response { .. }"),
                "Connect bypass `matches!` no longer forwards Response. \
                 Response is the joiner-fan-in terminal variant and MUST \
                 reach the per-tx multi-reply receiver."
            );
        }

        #[test]
        fn connect_branch_bypass_forwards_rejected() {
            assert!(
                connect_branch_window().contains("connect::ConnectMsg::Rejected { .. }"),
                "Connect bypass `matches!` no longer forwards Rejected. \
                 Relay drivers and the joiner driver both observe Rejected \
                 to record connection failure / record_connection_failure."
            );
        }

        #[test]
        fn connect_branch_bypass_forwards_observed_address() {
            assert!(
                connect_branch_window().contains("connect::ConnectMsg::ObservedAddress { .. }"),
                "Connect bypass `matches!` no longer forwards \
                 ObservedAddress. The joiner driver inbox owns the \
                 set_own_addr / update_location side effect; dropping \
                 ObservedAddress here breaks NAT discovery."
            );
        }

        #[test]
        fn connect_branch_bypass_forwards_connect_failed() {
            assert!(
                connect_branch_window().contains("connect::ConnectMsg::ConnectFailed { .. }"),
                "Connect bypass `matches!` no longer forwards \
                 ConnectFailed. The relay driver inbox owns hole-punch \
                 failure re-route; dropping ConnectFailed here strands \
                 the re-route on legacy `process_message`."
            );
        }

        #[test]
        fn connect_branch_bypass_does_not_forward_request() {
            // `Request` is the spawn signal handled by the dispatch
            // gate (commit 3); forwarding it via the bypass would
            // route fresh Requests into a multi-reply receiver that
            // doesn't exist yet, dropping them silently.
            let window = connect_branch_window();
            // Locate the bypass `matches!` block specifically — the
            // dispatch gate further down does destructure
            // `ConnectMsg::Request { id, payload }`, which is fine.
            let bypass_anchor = "if matches!(\n                op,";
            let bypass_start = window
                .find(bypass_anchor)
                .expect("bypass `matches!` block not found in Connect branch — guard outdated");
            let bypass_end = window[bypass_start..]
                .find(") {")
                .expect("bypass `matches!` block has no closing `) {`")
                + bypass_start;
            let bypass_block = &window[bypass_start..bypass_end];

            assert!(
                !bypass_block.contains("connect::ConnectMsg::Request"),
                "Connect bypass `matches!` MUST NOT forward Request. \
                 Request is the spawn signal for start_relay_connect; \
                 forwarding it would route fresh Requests into a multi-reply \
                 receiver that doesn't exist yet."
            );
        }

        /// The Connect branch MUST dispatch to `start_relay_connect`
        /// for fresh inbound Requests.
        #[test]
        fn connect_branch_dispatches_start_relay_connect_for_fresh_request() {
            let window = connect_branch_window();
            assert!(
                window.contains("start_relay_connect("),
                "Connect branch no longer calls start_relay_connect — \
                 removing it strands relay CONNECT on legacy."
            );
        }

        /// Relay dispatch must be gated on `source_addr.is_some()` so
        /// originator loop-back from `start_client_connect` (which
        /// cannot happen for CONNECT, but the guard documents the
        /// invariant) is dropped rather than spawning a self-loop.
        #[test]
        fn connect_relay_dispatch_gated_on_source_addr() {
            let window = connect_branch_window();
            let dispatch_anchor = "start_relay_connect(";
            let dispatch_pos = window
                .find(dispatch_anchor)
                .expect("start_relay_connect not found in Connect branch");
            let gate_start = dispatch_pos.saturating_sub(500);
            let gate_window = &window[gate_start..dispatch_pos];
            assert!(
                gate_window.contains("source_addr"),
                "CONNECT relay dispatch is not gated on source_addr — \
                 originator loop-back must NOT spawn a relay driver."
            );
        }

        /// Relay dispatch must also check `!active_relay_connect_txs.contains(id)`
        /// to avoid re-spawning a driver while a previous one is still running
        /// (e.g. duplicate Request retransmission while the driver is mid-
        /// handle_request).
        #[test]
        fn connect_relay_dispatch_guarded_by_active_relay_set() {
            let window = connect_branch_window();
            let dispatch_pos = window
                .find("start_relay_connect(")
                .expect("start_relay_connect not found in Connect branch");
            let gate_start = dispatch_pos.saturating_sub(500);
            let gate_window = &window[gate_start..dispatch_pos];
            assert!(
                gate_window.contains("active_relay_connect_txs"),
                "CONNECT relay dispatch is not guarded by \
                 active_relay_connect_txs.contains(id). Without it, a \
                 duplicate Request retransmission could spawn a second \
                 driver before the first inserts into the dedup set."
            );
        }
    }

    /// Tests for `ShutdownHandle::shutdown`'s drain behaviour. The
    /// drain stops in-flight client PUT/GET/UPDATE/SUBSCRIBE drivers
    /// from being torn down mid-operation when the gateway is stopped
    /// for an auto-update (motivating incident: `freenet-git` mirror
    /// failures on the nova gateway).
    mod shutdown_drain {
        use super::*;
        use std::sync::atomic::{AtomicUsize, Ordering};
        use std::time::Duration;

        /// Construct a ShutdownHandle wired to a fresh channel,
        /// counter, and admission gate, mirroring the production
        /// wire-up in `NodeBuilder::build`. The receiver is returned
        /// so tests can assert what (if anything) was sent; the gate
        /// is returned so tests can observe Phase 1 flipping it.
        fn make_handle(
            initial_count: usize,
            drain_timeout: Duration,
        ) -> (
            ShutdownHandle,
            Arc<AtomicUsize>,
            Arc<std::sync::atomic::AtomicBool>,
            tokio::sync::mpsc::Receiver<NodeEvent>,
        ) {
            let (tx, rx) = tokio::sync::mpsc::channel(1);
            let counter = Arc::new(AtomicUsize::new(initial_count));
            let gate = Arc::new(std::sync::atomic::AtomicBool::new(false));
            let handle = ShutdownHandle {
                tx,
                inflight_client_ops: counter.clone(),
                shutting_down: gate.clone(),
                drain_timeout,
            };
            (handle, counter, gate, rx)
        }

        #[tokio::test]
        async fn shutdown_with_zero_ops_returns_immediately() {
            let (handle, _counter, _gate, mut rx) = make_handle(0, Duration::from_secs(60));
            let start = std::time::Instant::now();
            handle.shutdown().await;
            assert!(
                start.elapsed() < Duration::from_millis(100),
                "shutdown with zero in-flight ops should not sleep"
            );
            // Disconnect must still be sent.
            assert!(matches!(
                rx.recv().await.expect("Disconnect must be sent"),
                NodeEvent::Disconnect { .. }
            ));
        }

        #[tokio::test]
        async fn shutdown_waits_then_proceeds_on_timeout() {
            // 1 op in flight that never decrements; drain capped at 200ms.
            let (handle, _counter, _gate, mut rx) = make_handle(1, Duration::from_millis(200));
            let start = std::time::Instant::now();
            handle.shutdown().await;
            let elapsed = start.elapsed();
            assert!(
                elapsed >= Duration::from_millis(180),
                "shutdown should wait the full drain timeout when ops \
                 never finish (elapsed: {elapsed:?})"
            );
            // Disconnect must still be sent so the node can exit.
            assert!(matches!(
                rx.recv()
                    .await
                    .expect("Disconnect must be sent even on drain timeout"),
                NodeEvent::Disconnect { .. }
            ));
        }

        #[tokio::test]
        async fn shutdown_proceeds_as_soon_as_counter_clears() {
            // 1 op in flight; another task decrements after 100ms.
            let (handle, counter, _gate, mut rx) = make_handle(1, Duration::from_secs(5));
            let counter_clone = counter.clone();
            tokio::spawn(async move {
                tokio::time::sleep(Duration::from_millis(100)).await;
                counter_clone.fetch_sub(1, Ordering::Relaxed);
            });
            let start = std::time::Instant::now();
            handle.shutdown().await;
            let elapsed = start.elapsed();
            assert!(
                elapsed >= Duration::from_millis(80) && elapsed < Duration::from_secs(2),
                "shutdown should return shortly after the counter clears, \
                 not wait the full drain timeout (elapsed: {elapsed:?})"
            );
            assert!(matches!(
                rx.recv().await.expect("Disconnect must be sent"),
                NodeEvent::Disconnect { .. }
            ));
        }

        #[tokio::test]
        async fn drain_disabled_skips_wait_even_with_ops_in_flight() {
            // Tests opt out of the drain via Duration::ZERO so a
            // SimNetwork teardown doesn't block on the 30s production
            // default. Verify the zero-timeout path bypasses the wait
            // entirely even when ops are "in flight".
            let (handle, _counter, _gate, mut rx) = make_handle(5, Duration::ZERO);
            let start = std::time::Instant::now();
            handle.shutdown().await;
            assert!(
                start.elapsed() < Duration::from_millis(50),
                "drain_timeout=0 must skip the wait"
            );
            assert!(matches!(
                rx.recv().await.expect("Disconnect must be sent"),
                NodeEvent::Disconnect { .. }
            ));
        }

        /// Phase 1 of the three-phase shutdown: admission gate MUST be
        /// flipped before the drain begins, so `start_client_*` calls
        /// arriving during the drain wait fail fast and don't slip
        /// through the post-drain race window. Codex reviewer call-out
        /// 2026-05 — re-opening this race re-opens the
        /// gateway-restart-kills-mirror-PUT failure for any op spawned
        /// in the window between drain-complete and Disconnect-send.
        #[tokio::test]
        async fn shutdown_closes_admission_gate_before_drain() {
            // 1 op in flight; drain caps at 500ms so we have time to
            // observe the gate during the wait.
            let (handle, counter, gate, mut rx) = make_handle(1, Duration::from_millis(500));
            assert!(
                !gate.load(Ordering::Relaxed),
                "admission gate must start closed"
            );

            let counter_clone = counter.clone();
            let gate_clone = gate.clone();
            let observed_during_drain = tokio::spawn(async move {
                // Wait briefly so shutdown's Phase 1 fires first.
                tokio::time::sleep(Duration::from_millis(50)).await;
                let g = gate_clone.load(Ordering::Relaxed);
                // Release the op so drain can complete.
                counter_clone.fetch_sub(1, Ordering::Relaxed);
                g
            });

            handle.shutdown().await;

            let gate_was_set_during_drain = observed_during_drain
                .await
                .expect("observer task must not panic");
            assert!(
                gate_was_set_during_drain,
                "shutdown() must flip the admission gate BEFORE the \
                 drain wait, not after. Otherwise a new client op \
                 spawned during the drain bypasses the gate, bumps \
                 the counter (now unobserved), and gets cut off."
            );
            assert!(matches!(
                rx.recv().await.expect("Disconnect must be sent"),
                NodeEvent::Disconnect { .. }
            ));
        }
    }
}
