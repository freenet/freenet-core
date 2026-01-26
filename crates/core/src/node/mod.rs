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
use either::Either;
use freenet_stdlib::{
    client_api::{ClientRequest, ErrorKind},
    prelude::ContractInstanceId,
};
use std::{
    borrow::Cow,
    fmt::Display,
    fs::File,
    hash::Hash,
    io::Read,
    net::{IpAddr, SocketAddr, ToSocketAddrs},
    sync::Arc,
    time::Duration,
};
use std::{collections::HashSet, convert::Infallible};

use self::p2p_impl::NodeP2P;
use crate::{
    client_events::{BoxedClient, ClientEventsProxy, ClientId, OpenRequest},
    config::{Address, GatewayConfig, GlobalRng, WebsocketApiConfig},
    contract::{Callback, ExecutorError, ExecutorToEventLoopChannel, NetworkContractHandler},
    local_node::Executor,
    message::{InnerMessage, NetMessage, NodeEvent, Transaction, TransactionType},
    operations::{
        connect::{self, ConnectOp},
        get, put, subscribe, update, OpEnum, OpError, OpOutcome,
    },
    ring::{Location, PeerKeyLocation},
    router::{RouteEvent, RouteOutcome},
    tracing::{EventRegister, NetEventLog, NetEventRegister},
};
use crate::{
    config::Config,
    message::{MessageStats, NetMessageV1},
};
use freenet_stdlib::client_api::DelegateRequest;
use serde::{Deserialize, Serialize};
use tracing::Instrument;

use crate::operations::handle_op_request;
pub(crate) use network_bridge::{ConnectionError, EventLoopNotificationsSender, NetworkBridge};
// Re-export types for dev_tool and testing
pub use network_bridge::{
    clear_all_fault_injectors, reset_channel_id_counter, EventLoopExitReason, NetworkStats,
};

use crate::topology::rate::Rate;
use crate::transport::{TransportKeypair, TransportPublicKey};
pub(crate) use op_state_manager::{OpManager, OpNotAvailable};

mod message_processor;
mod network_bridge;
mod op_state_manager;
mod p2p_impl;
pub(crate) mod proximity_cache;
mod request_router;
pub(crate) mod testing_impl;

pub use message_processor::MessageProcessor;
pub use request_router::{DeduplicatedRequest, RequestRouter};

/// Handle to trigger graceful shutdown of the node.
#[derive(Clone)]
pub struct ShutdownHandle {
    tx: tokio::sync::mpsc::Sender<NodeEvent>,
}

impl ShutdownHandle {
    /// Trigger a graceful shutdown of the node.
    ///
    /// This will:
    /// 1. Close all peer connections gracefully
    /// 2. Stop accepting new connections
    /// 3. Exit the event loop
    pub async fn shutdown(&self) {
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
            tracing::info!("Node external address: {}", own_addr.addr);
        }
        Ok(NodeConfig {
            should_connect: true,
            is_gateway: config.is_gateway,
            key_pair: config.transport_keypair().clone(),
            gateways,
            own_addr: config.peer_id.clone().map(|p| p.addr),
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

        let (conf, opts) = hickory_resolver::system_conf::read_system_conf()?;
        let resolver = hickory_resolver::TokioAsyncResolver::new(
            conf,
            opts,
            hickory_resolver::name_server::GenericConnector::new(
                hickory_resolver::name_server::TokioRuntimeProvider::new(),
            ),
        );

        // only issue one query with .
        let hostname = if hostname.ends_with('.') {
            hostname
        } else {
            Cow::Owned(format!("{hostname}."))
        };

        let ips = resolver.lookup_ip(hostname.as_ref()).await?;
        match ips.into_iter().next() {
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
        let (node_inner, shutdown_tx) = NodeP2P::build::<NetworkContractHandler, CLIENTS, _>(
            self,
            clients,
            event_register,
            cfg,
        )
        .await?;
        let shutdown_handle = ShutdownHandle { tx: shutdown_tx };
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
    op_result: Result<Option<OpEnum>, OpError>,
    op_manager: &OpManager,
    executor_callback: Option<ExecutorToEventLoopChannel<Callback>>,
    event_listener: &mut dyn NetEventRegister,
) {
    // Add UPDATE-specific debug logging at the start
    if let Some(tx_id) = tx {
        if matches!(tx_id.transaction_type(), TransactionType::Update) {
            tracing::debug!("report_result called for UPDATE transaction {}", tx_id);
        }
    }

    match op_result {
        Ok(Some(op_res)) => {
            // Log specifically for UPDATE operations
            if let crate::operations::OpEnum::Update(ref update_op) = op_res {
                tracing::debug!(
                    "UPDATE operation {} completed, finalized: {}",
                    update_op.id,
                    update_op.finalized()
                );
            }

            // Send to result router (skip for sub-operations - parent handles notification)
            if let Some(transaction) = tx {
                // Sub-operations (e.g., Subscribe spawned by PUT) don't notify clients directly;
                // the parent operation handles the client response.
                if op_manager.is_sub_operation(transaction) {
                    tracing::debug!(
                        tx = %transaction,
                        "Skipping client notification for sub-operation"
                    );
                } else {
                    let host_result = op_res.to_host_result();
                    // Await result delivery to ensure the client receives the response
                    // before the operation is considered complete. This prevents timeout
                    // issues where the operation completes but the response hasn't been
                    // delivered through the channel chain yet.
                    op_manager
                        .send_client_result(transaction, host_result)
                        .await;
                }
            }

            // check operations.rs:handle_op_result to see what's the meaning of each state
            // in case more cases want to be handled when feeding information to the OpManager

            match op_res.outcome() {
                OpOutcome::ContractOpSuccess {
                    target_peer,
                    contract_location,
                    first_response_time,
                    payload_size,
                    payload_transfer_time,
                } => {
                    let event = RouteEvent {
                        peer: target_peer.clone(),
                        contract_location,
                        outcome: RouteOutcome::Success {
                            time_to_response_start: first_response_time,
                            payload_size,
                            payload_transfer_time,
                        },
                    };
                    if let Some(log_event) =
                        NetEventLog::route_event(op_res.id(), &op_manager.ring, &event)
                    {
                        event_listener
                            .register_events(Either::Left(log_event))
                            .await;
                    }
                    op_manager.ring.routing_finished(event);
                }
                // todo: handle failures, need to track timeouts and other potential failures
                // OpOutcome::ContractOpFailure {
                //     target_peer: Some(target_peer),
                //     contract_location,
                // } => {
                //     op_manager.ring.routing_finished(RouteEvent {
                //         peer: *target_peer,
                //         contract_location,
                //         outcome: RouteOutcome::Failure,
                //     });
                // }
                OpOutcome::Incomplete | OpOutcome::Irrelevant => {}
            }
            if let Some(mut cb) = executor_callback {
                cb.response(op_res).await;
            }
        }
        Ok(None) => {
            tracing::debug!(?tx, "No operation result found");
        }
        Err(err) => {
            // just mark the operation as completed so no redundant messages are processed for this transaction anymore
            if let Some(tx) = tx {
                op_manager.completed(tx);
            }
            #[cfg(any(debug_assertions, test))]
            {
                use std::io::Write;
                #[cfg(debug_assertions)]
                let OpError::InvalidStateTransition { tx, state, trace } = err
                else {
                    tracing::error!("Finished transaction with error: {err}");
                    return;
                };
                #[cfg(not(debug_assertions))]
                let OpError::InvalidStateTransition { tx } = err
                else {
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

/// Pure network message processing using MessageProcessor for client handling
#[allow(clippy::too_many_arguments)]
pub(crate) async fn process_message_decoupled<CB>(
    msg: NetMessage,
    source_addr: Option<std::net::SocketAddr>,
    op_manager: Arc<OpManager>,
    conn_manager: CB,
    mut event_listener: Box<dyn NetEventRegister>,
    executor_callback: Option<ExecutorToEventLoopChannel<crate::contract::Callback>>,
    message_processor: std::sync::Arc<MessageProcessor>,
    pending_op_result: Option<tokio::sync::mpsc::Sender<NetMessage>>,
) where
    CB: NetworkBridge,
{
    let tx = *msg.id();

    // Pure network message processing - no client types involved
    let op_result = handle_pure_network_message(
        msg,
        source_addr,
        op_manager.clone(),
        conn_manager,
        event_listener.as_mut(),
        pending_op_result,
    )
    .await;

    // Prepare host result for session actor routing
    let host_result = match &op_result {
        Ok(Some(op_res)) => Some(op_res.to_host_result()),
        Ok(None) => None,
        Err(e) => Some(Err(freenet_stdlib::client_api::ClientError::from(
            freenet_stdlib::client_api::ErrorKind::OperationError {
                cause: e.to_string().into(),
            },
        ))),
    };

    // Report operation completion for telemetry/result routing
    report_result(
        Some(tx),
        op_result,
        &op_manager,
        executor_callback,
        &mut *event_listener,
    )
    .await;

    // Delegate to MessageProcessor - it handles all client concerns internally
    if let Err(e) = message_processor
        .handle_network_result(tx, host_result)
        .await
    {
        tracing::error!(
            "Failed to handle network result for transaction {}: {}",
            tx,
            e
        );
    }
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
) -> Result<Option<crate::operations::OpEnum>, crate::node::OpError>
where
    CB: NetworkBridge,
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

/// Pure network message processing for V1 messages (no client concerns)
#[allow(clippy::too_many_arguments)]
async fn handle_pure_network_message_v1<CB>(
    msg: NetMessageV1,
    source_addr: Option<std::net::SocketAddr>,
    op_manager: Arc<OpManager>,
    mut conn_manager: CB,
    event_listener: &mut dyn NetEventRegister,
    pending_op_result: Option<tokio::sync::mpsc::Sender<NetMessage>>,
) -> Result<Option<crate::operations::OpEnum>, crate::node::OpError>
where
    CB: NetworkBridge,
{
    // Register network events (pure network concern)
    event_listener
        .register_events(NetEventLog::from_inbound_msg_v1(&msg, &op_manager))
        .await;

    const MAX_RETRIES: usize = 10usize;
    for i in 0..MAX_RETRIES {
        let tx = Some(*msg.id());
        tracing::debug!(?tx, "Processing pure network operation, iteration: {i}");

        match msg {
            NetMessageV1::Connect(ref op) => {
                let parent_span = tracing::Span::current();
                let span = tracing::info_span!(
                    parent: parent_span,
                    "handle_connect_op_request",
                    transaction = %msg.id(),
                    tx_type = %msg.id().transaction_type()
                );
                let op_result = handle_op_request::<ConnectOp, _>(
                    &op_manager,
                    &mut conn_manager,
                    op,
                    source_addr,
                )
                .instrument(span)
                .await;

                if let Err(OpError::OpNotAvailable(state)) = &op_result {
                    match state {
                        OpNotAvailable::Running => {
                            tracing::debug!("Pure network: Operation still running");
                            tokio::time::sleep(Duration::from_micros(1_000)).await;
                            continue;
                        }
                        OpNotAvailable::Completed => {
                            tracing::debug!("Pure network: Operation already completed");
                            return Ok(None);
                        }
                    }
                }

                return handle_pure_network_result(
                    tx,
                    op_result,
                    &op_manager,
                    &mut *event_listener,
                )
                .await;
            }
            NetMessageV1::Put(ref op) => {
                tracing::debug!(
                    tx = %op.id(),
                    "handle_pure_network_message_v1: Processing PUT message"
                );
                let op_result = handle_op_request::<put::PutOp, _>(
                    &op_manager,
                    &mut conn_manager,
                    op,
                    source_addr,
                )
                .await;
                tracing::debug!(
                    tx = %op.id(),
                    op_result_ok = op_result.is_ok(),
                    "handle_pure_network_message_v1: PUT handle_op_request completed"
                );

                // Handle pending operation results (network concern)
                if is_operation_completed(&op_result) {
                    if let Some(ref op_execution_callback) = pending_op_result {
                        let tx_id = *op.id();
                        let _ = op_execution_callback
                            .send(NetMessage::V1(NetMessageV1::Put((*op).clone())))
                            .await
                            .inspect_err(|err| tracing::error!(%err, %tx_id, "Failed to send message to executor"));
                    }
                }

                if let Err(OpError::OpNotAvailable(state)) = &op_result {
                    match state {
                        OpNotAvailable::Running => {
                            tracing::debug!("Pure network: Operation still running");
                            tokio::time::sleep(Duration::from_micros(1_000)).await;
                            continue;
                        }
                        OpNotAvailable::Completed => {
                            tracing::debug!("Pure network: Operation already completed");
                            return Ok(None);
                        }
                    }
                }

                return handle_pure_network_result(
                    tx,
                    op_result,
                    &op_manager,
                    &mut *event_listener,
                )
                .await;
            }
            NetMessageV1::Get(ref op) => {
                let op_result = handle_op_request::<get::GetOp, _>(
                    &op_manager,
                    &mut conn_manager,
                    op,
                    source_addr,
                )
                .await;

                // Handle pending operation results (network concern)
                if is_operation_completed(&op_result) {
                    if let Some(ref op_execution_callback) = pending_op_result {
                        let tx_id = *op.id();
                        let _ = op_execution_callback
                            .send(NetMessage::V1(NetMessageV1::Get((*op).clone())))
                            .await
                            .inspect_err(|err| tracing::error!(%err, %tx_id, "Failed to send message to executor"));
                    }
                }

                if let Err(OpError::OpNotAvailable(state)) = &op_result {
                    match state {
                        OpNotAvailable::Running => {
                            tracing::debug!("Pure network: Operation still running");
                            tokio::time::sleep(Duration::from_micros(1_000)).await;
                            continue;
                        }
                        OpNotAvailable::Completed => {
                            tracing::debug!("Pure network: Operation already completed");
                            return Ok(None);
                        }
                    }
                }

                return handle_pure_network_result(
                    tx,
                    op_result,
                    &op_manager,
                    &mut *event_listener,
                )
                .await;
            }
            NetMessageV1::Update(ref op) => {
                let op_result = handle_op_request::<update::UpdateOp, _>(
                    &op_manager,
                    &mut conn_manager,
                    op,
                    source_addr,
                )
                .await;

                if let Err(OpError::OpNotAvailable(state)) = &op_result {
                    match state {
                        OpNotAvailable::Running => {
                            tracing::debug!("Pure network: Operation still running");
                            tokio::time::sleep(Duration::from_micros(1_000)).await;
                            continue;
                        }
                        OpNotAvailable::Completed => {
                            tracing::debug!("Pure network: Operation already completed");
                            return Ok(None);
                        }
                    }
                }

                return handle_pure_network_result(
                    tx,
                    op_result,
                    &op_manager,
                    &mut *event_listener,
                )
                .await;
            }
            NetMessageV1::Subscribe(ref op) => {
                let op_result = handle_op_request::<subscribe::SubscribeOp, _>(
                    &op_manager,
                    &mut conn_manager,
                    op,
                    source_addr,
                )
                .await;

                if let Err(OpError::OpNotAvailable(state)) = &op_result {
                    match state {
                        OpNotAvailable::Running => {
                            tracing::debug!("Pure network: Operation still running");
                            tokio::time::sleep(Duration::from_micros(1_000)).await;
                            continue;
                        }
                        OpNotAvailable::Completed => {
                            tracing::debug!("Pure network: Operation already completed");
                            return Ok(None);
                        }
                    }
                }

                return handle_pure_network_result(
                    tx,
                    op_result,
                    &op_manager,
                    &mut *event_listener,
                )
                .await;
            }
            NetMessageV1::ProximityCache { ref message } => {
                let Some(source) = source_addr else {
                    tracing::warn!(
                        "Received ProximityCache message without source address (pure network)"
                    );
                    break;
                };
                tracing::debug!(
                    from = %source,
                    "Processing ProximityCache message (pure network)"
                );

                // Note: In the simplified architecture (2026-01 refactor), we no longer
                // attempt to establish subscriptions based on CacheAnnounce messages.
                // Update propagation uses the proximity cache directly, and subscriptions
                // are lease-based with automatic expiry.

                if let Some(response) = op_manager
                    .proximity_cache
                    .handle_message(source, message.clone())
                {
                    // Send response back to sender
                    let response_msg =
                        NetMessage::V1(NetMessageV1::ProximityCache { message: response });
                    if let Err(err) = conn_manager.send(source, response_msg).await {
                        tracing::error!(%err, %source, "Failed to send ProximityCache response");
                    }
                }
                break;
            }
            NetMessageV1::InterestSync { ref message } => {
                let Some(source) = source_addr else {
                    tracing::warn!("Received InterestSync message without source address");
                    break;
                };
                tracing::debug!(
                    from = %source,
                    "Processing InterestSync message"
                );

                // Handle interest synchronization for delta-based updates
                if let Some(response) =
                    handle_interest_sync_message(&op_manager, source, message.clone()).await
                {
                    let response_msg =
                        NetMessage::V1(NetMessageV1::InterestSync { message: response });
                    if let Err(err) = conn_manager.send(source, response_msg).await {
                        tracing::error!(%err, %source, "Failed to send InterestSync response");
                    }
                }
                break;
            }
            _ => break, // Exit the loop if no applicable message type is found
        }
    }

    // If we reach here, no operation was processed
    Ok(None)
}

/// Pure network result handling - no client notification logic
async fn handle_pure_network_result(
    tx: Option<Transaction>,
    op_result: Result<Option<crate::operations::OpEnum>, OpError>,
    _op_manager: &Arc<OpManager>,
    _event_listener: &mut dyn NetEventRegister,
) -> Result<Option<crate::operations::OpEnum>, crate::node::OpError> {
    tracing::debug!("Pure network result handling for transaction: {:?}", tx);

    match &op_result {
        Ok(Some(_op_res)) => {
            // Log network operation completion
            tracing::debug!(
                "Network operation completed successfully for transaction: {:?}",
                tx
            );

            // Register completion event (pure network concern)
            if let Some(tx_id) = tx {
                // TODO: Register completion event properly
                tracing::debug!("Network operation completed for transaction: {}", tx_id);
            }

            // TODO: Handle executor callbacks (network concern)
        }
        Ok(None) => {
            tracing::debug!("Network operation returned no result");
        }
        Err(OpError::StatePushed) => {
            return Ok(None);
        }
        Err(OpError::OpNotPresent(tx_id)) => {
            // OpNotPresent means a response arrived for an operation that no longer exists.
            // This is benign - it happens when:
            // 1. An operation timed out before the response arrived
            // 2. A late response arrives after a peer restart
            // 3. The operation was already completed via another path
            //
            // We log at debug level and return Ok(None) to avoid propagating
            // confusing "op not present" errors to clients.
            tracing::debug!(
                tx = %tx_id,
                "Network response arrived for non-existent operation (likely timed out or already completed)"
            );
            return Ok(None);
        }
        Err(e) => {
            tracing::error!("Network operation failed: {}", e);
            // TODO: Register error event properly
            if let Some(tx_id) = tx {
                tracing::debug!(
                    "Network operation failed for transaction: {} with error: {}",
                    tx_id,
                    e
                );
            }
        }
    }

    op_result
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
    use crate::message::{InterestMessage, SummaryEntry};
    use crate::ring::interest::contract_hash;

    match message {
        InterestMessage::Interests { hashes } => {
            tracing::debug!(
                from = %source,
                hash_count = hashes.len(),
                "Received Interests message"
            );

            // Find contracts we share interest in
            let matching = op_manager.interest_manager.get_matching_contracts(&hashes);

            // Build summaries for shared contracts
            let mut entries = Vec::with_capacity(matching.len());
            for contract in matching {
                let hash = contract_hash(&contract);
                // Get our state summary for this contract
                let summary = get_contract_summary(op_manager, &contract).await;
                entries.push(SummaryEntry::from_summary(hash, summary.as_ref()));

                // Register peer's interest
                let peer_key = get_peer_key_from_addr(op_manager, source);
                if let Some(pk) = peer_key {
                    op_manager.interest_manager.register_peer_interest(
                        &contract, pk,
                        None, // We'll get their summary in their Summaries response
                        false,
                    );
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

            // Update peer summaries in our interest tracker
            let peer_key = get_peer_key_from_addr(op_manager, source);
            if let Some(pk) = peer_key {
                for entry in entries {
                    // Handle hash collisions - lookup returns all contracts with this hash
                    for contract in op_manager.interest_manager.lookup_by_hash(entry.hash) {
                        // Only update if we have local interest in this contract
                        if op_manager.interest_manager.has_local_interest(&contract) {
                            let summary = entry.to_summary();
                            op_manager
                                .interest_manager
                                .update_peer_summary(&contract, &pk, summary);
                        }
                    }
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
                    tracing::warn!(
                        from = %source,
                        contract = %key,
                        event = "resync_failed",
                        response = ?other,
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
    use crate::contract::ContractHandlerEvent;

    match op_manager
        .notify_contract_handler(ContractHandlerEvent::GetQuery {
            instance_id: *key.id(),
            return_contract_code: false,
        })
        .await
    {
        Ok(ContractHandlerEvent::GetResponse {
            response: Ok(store_response),
            ..
        }) => store_response.state,
        Ok(ContractHandlerEvent::GetResponse {
            response: Err(e), ..
        }) => {
            tracing::warn!(
                contract = %key,
                error = %e,
                "Failed to get contract state"
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
            tracing::warn!(
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

/// Attempts to subscribe to a contract
#[allow(dead_code)]
pub async fn subscribe(
    op_manager: Arc<OpManager>,
    instance_id: ContractInstanceId,
    client_id: Option<ClientId>,
) -> Result<Transaction, OpError> {
    // Client-initiated subscriptions are never renewals
    subscribe_with_id(op_manager, instance_id, client_id, None, false).await
}

/// Attempts to subscribe to a contract with a specific transaction ID (for deduplication)
///
/// `is_renewal` indicates whether this is a renewal (requester already has the contract).
/// If true, the responder will skip sending state to save bandwidth.
pub async fn subscribe_with_id(
    op_manager: Arc<OpManager>,
    instance_id: ContractInstanceId,
    client_id: Option<ClientId>,
    transaction_id: Option<Transaction>,
    is_renewal: bool,
) -> Result<Transaction, OpError> {
    let op = match transaction_id {
        Some(id) => subscribe::start_op_with_id(instance_id, id, is_renewal),
        None => subscribe::start_op(instance_id, is_renewal),
    };
    let id = op.id;
    if let Some(client_id) = client_id {
        use crate::client_events::RequestId;
        // Generate a default RequestId for internal subscription operations
        let request_id = RequestId::new();
        let _ = op_manager
            .ch_outbound
            .waiting_for_subscription_result(id, instance_id, client_id, request_id)
            .await;
    }
    // Initialize a subscribe op.
    match subscribe::request_subscribe(&op_manager, op).await {
        Err(err) => {
            tracing::error!("{}", err);
            Err(err)
        }
        Ok(()) => Ok(id),
    }
}

async fn handle_aborted_op(
    tx: Transaction,
    op_manager: &OpManager,
    gateways: &[PeerKeyLocation],
) -> Result<(), OpError> {
    use crate::util::IterExt;
    match tx.transaction_type() {
        TransactionType::Connect => {
            // attempt to establish a connection failed, this could be a fatal error since the node
            // is useless without connecting to the network, we will retry with exponential backoff
            match op_manager.pop(&tx) {
                Ok(Some(OpEnum::Connect(op)))
                    if op_manager.ring.open_connections()
                        < op_manager.ring.connection_manager.min_connections =>
                {
                    let gateway = op.gateway().cloned();
                    if let Some(gateway) = gateway {
                        // Record failure and apply backoff if we know the address
                        if let Some(peer_addr) = gateway.peer_addr.as_known() {
                            let backoff_duration = {
                                let mut backoff = op_manager.gateway_backoff.lock();
                                backoff.record_failure(*peer_addr);
                                backoff.remaining_backoff(*peer_addr)
                            };

                            if let Some(duration) = backoff_duration {
                                tracing::info!(
                                    gateway = %gateway,
                                    backoff_secs = duration.as_secs(),
                                    "Gateway connection failed, waiting before retry"
                                );
                                tokio::time::sleep(duration).await;
                            }
                        }

                        tracing::debug!("Retrying connection to gateway {}", gateway);
                        connect::join_ring_request(&gateway, op_manager).await?;
                    }
                }
                Ok(Some(OpEnum::Connect(_))) => {
                    if op_manager.ring.open_connections() == 0 && op_manager.ring.is_gateway() {
                        tracing::warn!("Retrying joining the ring with an other gateway");
                        if let Some(gateway) = gateways.iter().shuffle().next() {
                            connect::join_ring_request(gateway, op_manager).await?
                        }
                    }
                }
                Ok(Some(other)) => {
                    op_manager.push(tx, other).await?;
                }
                _ => {}
            }
        }
        TransactionType::Get => match op_manager.pop(&tx) {
            Ok(Some(OpEnum::Get(op))) => {
                if let Err(err) = op.handle_abort(op_manager).await {
                    if !matches!(err, OpError::StatePushed) {
                        return Err(err);
                    }
                }
            }
            Ok(Some(other)) => {
                op_manager.push(tx, other).await?;
            }
            _ => {}
        },
        TransactionType::Subscribe => match op_manager.pop(&tx) {
            Ok(Some(OpEnum::Subscribe(op))) => {
                if let Err(err) = op.handle_abort(op_manager).await {
                    if !matches!(err, OpError::StatePushed) {
                        return Err(err);
                    }
                }
            }
            Ok(Some(other)) => {
                op_manager.push(tx, other).await?;
            }
            _ => {}
        },
        TransactionType::Put => match op_manager.pop(&tx) {
            Ok(Some(OpEnum::Put(op))) => {
                if let Err(err) = op.handle_abort(op_manager).await {
                    if !matches!(err, OpError::StatePushed) {
                        return Err(err);
                    }
                }
            }
            Ok(Some(other)) => {
                op_manager.push(tx, other).await?;
            }
            _ => {}
        },
        TransactionType::Update => match op_manager.pop(&tx) {
            Ok(Some(OpEnum::Update(op))) => {
                if let Err(err) = op.handle_abort(op_manager).await {
                    if !matches!(err, OpError::StatePushed) {
                        return Err(err);
                    }
                }
            }
            Ok(Some(other)) => {
                op_manager.push(tx, other).await?;
            }
            _ => {}
        },
    }
    Ok(())
}

/// The identifier of a peer in the network is composed of its address and public key.
///
/// A regular peer will have its `PeerId` set when it connects to a gateway as it get's
/// its external address from the gateway.
///
/// A gateway will have its `PeerId` set when it is created since it will know its own address
/// from the start.
#[derive(Serialize, Deserialize, Eq, Clone)]
pub struct PeerId {
    pub addr: SocketAddr,
    pub pub_key: TransportPublicKey,
}

impl Hash for PeerId {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.addr.hash(state);
    }
}

impl PartialEq<PeerId> for PeerId {
    fn eq(&self, other: &PeerId) -> bool {
        self.addr == other.addr
    }
}

impl Ord for PeerId {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.addr.cmp(&other.addr)
    }
}

impl PartialOrd for PeerId {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl PeerId {
    pub fn new(addr: SocketAddr, pub_key: TransportPublicKey) -> Self {
        Self { addr, pub_key }
    }
}

thread_local! {
    static PEER_ID: std::cell::RefCell<Option<TransportPublicKey>> = const { std::cell::RefCell::new(None) };
}

#[cfg(test)]
impl<'a> arbitrary::Arbitrary<'a> for PeerId {
    fn arbitrary(u: &mut arbitrary::Unstructured<'a>) -> arbitrary::Result<Self> {
        let addr: ([u8; 4], u16) = u.arbitrary()?;

        let pub_key = PEER_ID.with(|peer_id| {
            let mut peer_id = peer_id.borrow_mut();
            match &*peer_id {
                Some(k) => k.clone(),
                None => {
                    let key = TransportKeypair::new().public().clone();
                    peer_id.replace(key.clone());
                    key
                }
            }
        });

        Ok(Self {
            addr: addr.into(),
            pub_key,
        })
    }
}

impl PeerId {
    pub fn random() -> Self {
        let mut addr = [0; 4];
        GlobalRng::fill_bytes(&mut addr[..]);
        // Use random port instead of get_free_port() for speed - tests don't actually bind
        let port: u16 = GlobalRng::random_range(1024u16..65535u16);

        let pub_key = PEER_ID.with(|peer_id| {
            let mut peer_id = peer_id.borrow_mut();
            match &*peer_id {
                Some(k) => k.clone(),
                None => {
                    let key = TransportKeypair::new().public().clone();
                    peer_id.replace(key.clone());
                    key
                }
            }
        });

        Self {
            addr: (addr, port).into(),
            pub_key,
        }
    }

    #[cfg(test)]
    pub fn to_bytes(self) -> Vec<u8> {
        bincode::serialize(&self).unwrap()
    }
}

impl std::fmt::Debug for PeerId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        <Self as Display>::fmt(self, f)
    }
}

impl Display for PeerId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?}", self.pub_key)
    }
}

pub async fn run_local_node(
    mut executor: Executor,
    socket: WebsocketApiConfig,
) -> anyhow::Result<()> {
    match socket.address {
        IpAddr::V4(ip) if !ip.is_loopback() => {
            anyhow::bail!("invalid ip: {ip}, expecting localhost")
        }
        IpAddr::V6(ip) if !ip.is_loopback() => {
            anyhow::bail!("invalid ip: {ip}, expecting localhost")
        }
        _ => {}
    }

    let (mut gw, mut ws_proxy) = crate::server::serve_gateway_in(socket).await?;

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
            attested_contract,
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
                // Use the attested_contract already resolved by WebSocket/HttpGateway
                // instead of re-looking up from gw.attested_contracts (which could fail
                // if the token expired between WebSocket connect and this request)
                let op_name = match op {
                    DelegateRequest::RegisterDelegate { .. } => "RegisterDelegate",
                    DelegateRequest::ApplicationMessages { .. } => "ApplicationMessages",
                    DelegateRequest::GetSecretRequest { .. } => "GetSecretRequest",
                    DelegateRequest::UnregisterDelegate(_) => "UnregisterDelegate",
                    _ => "Unknown",
                };
                tracing::debug!(
                    op_name = ?op_name,
                    ?attested_contract,
                    "Handling ClientRequest::DelegateOp"
                );
                executor.delegate_request(op, attested_contract.as_ref())
            }
            ClientRequest::Disconnect { cause } => {
                if let Some(cause) = cause {
                    tracing::info!("disconnecting cause: {cause}");
                }
                continue;
            }
            _ => Err(ExecutorError::other(anyhow::anyhow!("not supported"))),
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
                    .map(|id| Location::from_address(&id.addr))
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

/// Trait to determine if an operation has completed, regardless of its specific type.
pub trait IsOperationCompleted {
    /// Returns true if the operation has completed (successfully or with error)
    fn is_completed(&self) -> bool;
}

impl IsOperationCompleted for OpEnum {
    fn is_completed(&self) -> bool {
        match self {
            OpEnum::Connect(op) => op.is_completed(),
            OpEnum::Put(op) => op.is_completed(),
            OpEnum::Get(op) => op.is_completed(),
            OpEnum::Subscribe(op) => op.is_completed(),
            OpEnum::Update(op) => op.is_completed(),
        }
    }
}

/// Check if an operation result indicates completion
pub fn is_operation_completed(op_result: &Result<Option<OpEnum>, OpError>) -> bool {
    match op_result {
        // If we got an OpEnum, check its specific completion status using the trait
        Ok(Some(op)) => op.is_completed(),
        _ => false,
    }
}

#[cfg(test)]
mod tests {
    use std::net::{Ipv4Addr, Ipv6Addr};

    use super::*;
    use crate::operations::OpError;
    use rstest::rstest;

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

    // PeerId equality tests
    #[rstest]
    #[case::same_addr_different_keys(8080, 8080, true)]
    #[case::different_addr_same_key(8080, 8081, false)]
    fn test_peer_id_equality(#[case] port1: u16, #[case] port2: u16, #[case] expected_equal: bool) {
        let keypair1 = TransportKeypair::new();
        let keypair2 = TransportKeypair::new();
        let addr1 = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), port1);
        let addr2 = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), port2);

        let peer1 = PeerId::new(addr1, keypair1.public().clone());
        let peer2 = PeerId::new(addr2, keypair2.public().clone());

        assert_eq!(peer1 == peer2, expected_equal);
    }

    #[rstest]
    #[case::lower_port_first(8080, 8081)]
    #[case::high_port_diff(1024, 65535)]
    fn test_peer_id_ordering(#[case] lower_port: u16, #[case] higher_port: u16) {
        let keypair = TransportKeypair::new();
        let addr1 = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), lower_port);
        let addr2 = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), higher_port);

        let peer1 = PeerId::new(addr1, keypair.public().clone());
        let peer2 = PeerId::new(addr2, keypair.public().clone());

        assert!(peer1 < peer2);
        assert!(peer2 > peer1);
    }

    #[test]
    fn test_peer_id_hash_consistency() {
        use std::collections::hash_map::DefaultHasher;
        use std::hash::{Hash, Hasher};

        let keypair1 = TransportKeypair::new();
        let keypair2 = TransportKeypair::new();
        let addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 8080);

        let peer1 = PeerId::new(addr, keypair1.public().clone());
        let peer2 = PeerId::new(addr, keypair2.public().clone());

        let mut hasher1 = DefaultHasher::new();
        let mut hasher2 = DefaultHasher::new();
        peer1.hash(&mut hasher1);
        peer2.hash(&mut hasher2);

        // Same address should produce same hash
        assert_eq!(hasher1.finish(), hasher2.finish());
    }

    #[test]
    fn test_peer_id_random_produces_unique() {
        let peer1 = PeerId::random();
        let peer2 = PeerId::random();

        // Random peers should have different addresses (with high probability)
        assert_ne!(peer1.addr, peer2.addr);
    }

    #[test]
    fn test_peer_id_serialization() {
        let peer = PeerId::random();
        let bytes = peer.clone().to_bytes();
        assert!(!bytes.is_empty());

        // Should be deserializable
        let deserialized: PeerId = bincode::deserialize(&bytes).unwrap();
        assert_eq!(peer.addr, deserialized.addr);
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

    // is_operation_completed tests - parametrized
    #[rstest]
    #[case::with_none(Ok(None), false)]
    #[case::with_running_error(Err(OpError::OpNotAvailable(super::OpNotAvailable::Running)), false)]
    #[case::with_state_pushed_error(Err(OpError::StatePushed), false)]
    fn test_is_operation_completed(
        #[case] result: Result<Option<OpEnum>, OpError>,
        #[case] expected: bool,
    ) {
        assert_eq!(is_operation_completed(&result), expected);
    }
}
