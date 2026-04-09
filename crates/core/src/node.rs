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
    contract::{Callback, ExecutorError, ExecutorToEventLoopChannel, NetworkContractHandler},
    local_node::Executor,
    message::{InnerMessage, NetMessage, NodeEvent, Transaction, TransactionType},
    operations::{
        OpEnum, OpError, OpOutcome,
        connect::{self, ConnectOp},
        get, put, subscribe, update,
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
#[cfg(test)]
pub(crate) use network_bridge::{EventLoopNotificationsReceiver, event_loop_notification_channel};
// Re-export types for dev_tool and testing
pub use network_bridge::{EventLoopExitReason, NetworkStats, reset_channel_id_counter};

use crate::topology::rate::Rate;
use crate::transport::{TransportKeypair, TransportPublicKey};
pub(crate) use op_state_manager::{OpManager, OpNotAvailable};

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

            // Send to result router (skip for sub-operations and subscription renewals)
            if let Some(transaction) = tx {
                // Sub-operations (e.g., Subscribe spawned by PUT) don't notify clients directly;
                // the parent operation handles the client response.
                if op_manager.is_sub_operation(transaction) {
                    tracing::debug!(
                        tx = %transaction,
                        "Skipping client notification for sub-operation"
                    );
                } else if op_res.is_subscription_renewal() {
                    // Subscription renewals are node-internal operations spawned by the
                    // renewal manager (ring.rs). No client registered a transaction
                    // for these, so sending to the session actor would just produce
                    // "registered transactions: 0" noise. See #2891.
                    tracing::debug!(
                        tx = %transaction,
                        "Skipping client notification for subscription renewal"
                    );
                } else {
                    let host_result = op_res.to_host_result();
                    // Await result delivery to ensure the client receives the response
                    // before the operation is considered complete. This prevents timeout
                    // issues where the operation completes but the response hasn't been
                    // delivered through the channel chain yet.
                    op_manager.send_client_result(transaction, host_result);
                }
            }

            // check operations.rs:handle_op_result to see what's the meaning of each state
            // in case more cases want to be handled when feeding information to the OpManager

            // Record operation result for dashboard stats
            let (classified_op_type, classified_success) =
                classify_op_outcome(op_res.id().transaction_type(), op_res.outcome());
            if let Some(op_type) = classified_op_type {
                network_status::record_op_result(op_type, classified_success);
            }

            let route_event = match op_res.outcome() {
                OpOutcome::ContractOpSuccess {
                    target_peer,
                    contract_location,
                    first_response_time,
                    payload_size,
                    payload_transfer_time,
                } => Some(RouteEvent {
                    peer: target_peer.clone(),
                    contract_location,
                    outcome: RouteOutcome::Success {
                        time_to_response_start: first_response_time,
                        payload_size,
                        payload_transfer_time,
                    },
                    op_type: classified_op_type,
                }),
                OpOutcome::ContractOpSuccessUntimed {
                    target_peer,
                    contract_location,
                } => Some(RouteEvent {
                    peer: target_peer.clone(),
                    contract_location,
                    outcome: RouteOutcome::SuccessUntimed,
                    op_type: classified_op_type,
                }),
                OpOutcome::ContractOpFailure {
                    target_peer,
                    contract_location,
                } => Some(RouteEvent {
                    peer: target_peer.clone(),
                    contract_location,
                    outcome: RouteOutcome::Failure,
                    op_type: classified_op_type,
                }),
                OpOutcome::Incomplete | OpOutcome::Irrelevant => None,
            };
            if let Some(event) = route_event {
                if let Some(log_event) =
                    NetEventLog::route_event(op_res.id(), &op_manager.ring, &event)
                {
                    event_listener
                        .register_events(Either::Left(log_event))
                        .await;
                }
                op_manager.ring.routing_finished(event);
            }
            if let Some(mut cb) = executor_callback {
                cb.response(op_res).await;
            }
        }
        Ok(None) => {
            tracing::debug!(?tx, "No operation result found");
        }
        Err(err) => {
            // Mark operation as completed and notify waiting clients of the error
            if let Some(tx) = tx {
                // Sub-operations (e.g., Subscribe spawned by PUT) have no client
                // registered — sending errors for them would pollute the
                // SessionActor's pending_results cache.
                if !op_manager.is_sub_operation(tx) {
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
    executor_callback: Option<ExecutorToEventLoopChannel<crate::contract::Callback>>,
    pending_op_result: Option<tokio::sync::mpsc::Sender<NetMessage>>,
) where
    CB: NetworkBridge,
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
    report_result(
        Some(tx),
        op_result,
        &op_manager,
        executor_callback,
        &mut *event_listener,
    )
    .await;
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

/// Returns the exponential backoff delay for the given retry attempt.
///
/// Starts at 5ms and doubles each attempt, capped at 1000ms.
fn op_retry_backoff(attempt: usize) -> Duration {
    Duration::from_millis((5u64 << attempt.min(8)).min(1_000))
}

/// Route an inbound task-per-tx reply directly to an awaiting
/// [`OpCtx::send_and_await`][ocxawait] caller, bypassing the legacy op
/// state machine entirely.
///
/// Returns `true` if a callback was registered and the message was forwarded
/// (or dropped due to a closed receiver, which is also a successful
/// "bypass taken" from the pipeline's point of view — the legacy path must
/// not run). Returns `false` if no callback is registered; the caller then
/// falls through to the legacy `handle_op_request` path.
///
/// # Why this exists
///
/// Phase 1 (#3802) wired [`forward_pending_op_result_if_completed`] to fire
/// the callback after the legacy state machine classified the reply as
/// completed. That only works when a [`crate::operations::OpEnum`] was
/// previously pushed into [`crate::node::OpManager`]'s per-op DashMap:
/// `load_or_init` pops it, `process_message` produces
/// [`crate::operations::OperationResult::SendAndComplete`], and
/// `is_operation_completed` returns `true`.
///
/// Phase 2b's task-per-tx callers (starting with client-initiated SUBSCRIBE,
/// #1454) never push an op into that DashMap: state lives in the task's
/// locals. When a reply arrives, `load_or_init` sees an empty slot and
/// returns [`crate::operations::OpError::OpNotPresent`] (this is the
/// legacy guard against stale responses arriving after GC cleanup, e.g.
/// `operations/subscribe.rs:1181-1192`). `handle_op_result` swallows
/// `OpNotPresent` as benign and returns `Ok(None)`, so
/// `forward_pending_op_result_if_completed` short-circuits and the
/// awaiting task hangs forever.
///
/// This helper is the fix: any branch of
/// [`handle_pure_network_message_v1`] that wants to support a task-per-tx
/// caller checks this first, and on a hit returns without ever touching
/// `handle_op_request`. Phase 2b adds this guard to the SUBSCRIBE branch
/// only; Phases 2c/3/4 will add it to their own branches when they
/// introduce their first task-per-tx callers.
///
/// # Safety argument for the bypass
///
/// `p2p_protoc::pending_op_results` is only populated via
/// `p2p_protoc::handle_op_execution`, which is only driven by the
/// `op_execution_sender` channel. The only way to obtain a clone of that
/// sender is through [`crate::node::OpManager::op_ctx`] (production
/// factory) or the in-module `OpCtx` unit tests — both of which construct
/// an [`OpCtx`][ocx] whose only round-trip method is
/// [`OpCtx::send_and_await`][ocxawait]. This is a **structural
/// invariant**, not a convention: the sender field is `pub(crate)` and
/// there is no other `pub` accessor on `EventLoopNotificationsSender`.
///
/// Consequence: legacy paths (SUBSCRIBE renewals, PUT sub-op subscribes,
/// contract-executor-initiated subscribes, intermediate-peer forwarding)
/// never appear in `pending_op_results` for their own txs, so the bypass
/// never triggers for them and their behavior is unchanged.
///
/// [ocx]: crate::operations::OpCtx
/// [ocxawait]: crate::operations::OpCtx::send_and_await
///
/// # Channel safety
///
/// Uses `try_send` on the bounded capacity-1 channel created by
/// [`OpCtx::send_and_await`][ocxawait]. On a closed receiver (e.g., caller
/// timed out or was cancelled) the send fails and is logged; the
/// pure-network-message handler still makes progress. See
/// `.claude/rules/channel-safety.md`.
fn try_forward_task_per_tx_reply(
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
            "Failed to forward task-per-tx reply to OpCtx task"
        );
    }
    true
}

/// If `op_result` indicates the operation completed and a `pending_op_result`
/// callback is wired, forward `reply` to the awaiting caller of
/// [`crate::operations::OpCtx::send_and_await`].
///
/// This is the reply side of the async sub-transaction round-trip primitive
/// introduced by #1454. The caller side — [`OpCtx::send_and_await`][ocx] —
/// installs a one-shot bounded [`tokio::sync::mpsc::Sender`] into
/// `p2p_protoc::pending_op_results` keyed by its `Transaction`, and each
/// branch of `handle_pure_network_message_v1` calls this helper after
/// `handle_op_request` so the caller can `await` a single reply keyed by
/// the same `Transaction`.
///
/// Wired for PUT and GET historically; extended to SUBSCRIBE, CONNECT, and
/// UPDATE in Phase 1 of the async-transaction refactor (#1454) so every op
/// kind can terminate an `OpCtx::send_and_await` round-trip without hanging.
/// (Phase 1 predates `OpCtx`; the caller side used to live directly on
/// `OpManager::notify_op_execution`, now deleted. Phase 2a moved the caller
/// side into `OpCtx` behind an `OpManager::op_ctx` factory.)
///
/// [ocx]: crate::operations::OpCtx::send_and_await
///
/// # Channel safety
///
/// Uses `try_send` rather than `.send().await` on the bounded capacity-1
/// mpsc channel created by `OpCtx::send_and_await`. This is sound because
/// the callback is fired **at most once per transaction**: the
/// `is_operation_completed` guard above combined with the `completed` /
/// `under_progress` dedup sets in `OpManager` ensures that subsequent
/// messages for the same tx short-circuit before reaching this code. So
/// `try_send` on an empty capacity-1 channel cannot fail with `Full` —
/// it can only fail with `Closed` when the `OpCtx` owner has dropped its
/// receiver. Using `try_send` eliminates any risk of blocking the
/// pure-network-message handler if a future consumer ever ends up unable
/// to drain the reply, satisfying the preference for non-blocking sends
/// in `.claude/rules/channel-safety.md`.
///
/// As of Phase 2a (#1454) this path is still dormant: `pending_op_result`
/// is always `None` because no production caller of `OpCtx::send_and_await`
/// exists yet. Phase 2b will introduce the first real caller (SUBSCRIBE
/// client-initiated path); the `try_send` choice here means Phase 2b does
/// not need to touch this function.
fn forward_pending_op_result_if_completed(
    op_result: &Result<Option<OpEnum>, OpError>,
    pending_op_result: Option<&tokio::sync::mpsc::Sender<NetMessage>>,
    reply: NetMessage,
) {
    if !is_operation_completed(op_result) {
        return;
    }
    let Some(callback) = pending_op_result else {
        return;
    };
    let tx_id = *reply.id();
    if let Err(err) = callback.try_send(reply) {
        tracing::error!(%err, %tx_id, "Failed to send message to executor");
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
        .register_events(NetEventLog::from_inbound_msg_v1(
            &msg,
            &op_manager,
            source_addr,
        ))
        .await;

    const MAX_RETRIES: usize = 15usize;
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

                // Handle pending operation results (network concern)
                forward_pending_op_result_if_completed(
                    &op_result,
                    pending_op_result.as_ref(),
                    NetMessage::V1(NetMessageV1::Connect((*op).clone())),
                );

                if let Err(OpError::OpNotAvailable(state)) = &op_result {
                    match state {
                        OpNotAvailable::Running => {
                            let delay = op_retry_backoff(i);
                            tracing::debug!(
                                delay_ms = delay.as_millis() as u64,
                                attempt = i,
                                "Pure network: Operation still running, backing off"
                            );
                            tokio::time::sleep(delay).await;
                            continue;
                        }
                        OpNotAvailable::Completed => {
                            tracing::debug!(
                                tx = %msg.id(),
                                tx_type = ?msg.id().transaction_type(),
                                "Pure network: Operation already completed"
                            );
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
                forward_pending_op_result_if_completed(
                    &op_result,
                    pending_op_result.as_ref(),
                    NetMessage::V1(NetMessageV1::Put((*op).clone())),
                );

                if let Err(OpError::OpNotAvailable(state)) = &op_result {
                    match state {
                        OpNotAvailable::Running => {
                            let delay = op_retry_backoff(i);
                            tracing::debug!(
                                delay_ms = delay.as_millis() as u64,
                                attempt = i,
                                "Pure network: Operation still running, backing off"
                            );
                            tokio::time::sleep(delay).await;
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
                forward_pending_op_result_if_completed(
                    &op_result,
                    pending_op_result.as_ref(),
                    NetMessage::V1(NetMessageV1::Get((*op).clone())),
                );

                if let Err(OpError::OpNotAvailable(state)) = &op_result {
                    match state {
                        OpNotAvailable::Running => {
                            let delay = op_retry_backoff(i);
                            tracing::debug!(
                                delay_ms = delay.as_millis() as u64,
                                attempt = i,
                                "Pure network: Operation still running, backing off"
                            );
                            tokio::time::sleep(delay).await;
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

                // Handle pending operation results (network concern)
                forward_pending_op_result_if_completed(
                    &op_result,
                    pending_op_result.as_ref(),
                    NetMessage::V1(NetMessageV1::Update((*op).clone())),
                );

                if let Err(OpError::OpNotAvailable(state)) = &op_result {
                    match state {
                        OpNotAvailable::Running => {
                            let delay = op_retry_backoff(i);
                            tracing::debug!(
                                delay_ms = delay.as_millis() as u64,
                                attempt = i,
                                "Pure network: Operation still running, backing off"
                            );
                            tokio::time::sleep(delay).await;
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
                // Phase 2b (#1454): task-per-tx bypass for client-initiated
                // SUBSCRIBE. See `try_forward_task_per_tx_reply` for the full
                // reasoning (reply-side structural gap between Phase 1's
                // forwarding hook and task-per-tx callers who never push an
                // op into the OpManager DashMap).
                if try_forward_task_per_tx_reply(
                    pending_op_result.as_ref(),
                    NetMessage::V1(NetMessageV1::Subscribe((*op).clone())),
                    "subscribe",
                ) {
                    return Ok(None);
                }

                let op_result = handle_op_request::<subscribe::SubscribeOp, _>(
                    &op_manager,
                    &mut conn_manager,
                    op,
                    source_addr,
                )
                .await;

                // Legacy-path forwarding: no-op for SUBSCRIBE now that the
                // task-per-tx bypass above handles every case where a
                // callback is registered. Kept here for structural parity
                // with the other op branches and to catch any future
                // case where a legacy-path caller registers a callback
                // without going through `OpCtx`.
                forward_pending_op_result_if_completed(
                    &op_result,
                    pending_op_result.as_ref(),
                    NetMessage::V1(NetMessageV1::Subscribe((*op).clone())),
                );

                if let Err(OpError::OpNotAvailable(state)) = &op_result {
                    match state {
                        OpNotAvailable::Running => {
                            let delay = op_retry_backoff(i);
                            tracing::debug!(
                                delay_ms = delay.as_millis() as u64,
                                attempt = i,
                                "Pure network: Operation still running, backing off"
                            );
                            tokio::time::sleep(delay).await;
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
            // Non-transactional message types: process once and return immediately.
            // These must NOT fall through to the post-loop "Dropping message" warning,
            // which is only meant for operation retry exhaustion.
            NetMessageV1::NeighborHosting { ref message } => {
                let Some(source) = source_addr else {
                    tracing::warn!(
                        "Received NeighborHosting message without source address (pure network)"
                    );
                    return Ok(None);
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
                    return Ok(None);
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
                        if let Err(e) = op_manager
                            .notify_node_event(NodeEvent::SyncStateToPeer {
                                key,
                                new_state: state,
                                target: source,
                            })
                            .await
                        {
                            tracing::warn!(
                                contract = %instance_id,
                                error = %e,
                                "Failed to emit SyncStateToPeer for proximity sync"
                            );
                        }
                    }
                }
                return Ok(None);
            }
            NetMessageV1::InterestSync { ref message } => {
                let Some(source) = source_addr else {
                    tracing::warn!("Received InterestSync message without source address");
                    return Ok(None);
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
                return Ok(None);
            }
            NetMessageV1::ReadyState { ready } => {
                let Some(source) = source_addr else {
                    tracing::warn!("Received ReadyState message without source address");
                    return Ok(None);
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
                return Ok(None);
            }
            NetMessageV1::Aborted(tx) => {
                tracing::debug!(
                    %tx,
                    tx_type = ?tx.transaction_type(),
                    "Received Aborted message, delegating to handle_aborted_op"
                );
                // Empty gateways: Aborted messages arrive over p2p connections, not
                // from the gateway join path. The gateways list is only used by
                // Connect retries; other operation types ignore it entirely.
                if let Err(err) = handle_aborted_op(tx, &op_manager, &[]).await {
                    if !matches!(err, OpError::StatePushed) {
                        tracing::error!(
                            %tx,
                            error = %err,
                            "Error handling aborted operation"
                        );
                    }
                }
                return Ok(None);
            }
        }
    }

    // If we reach here, retries were exhausted waiting for a concurrent operation to finish
    tracing::warn!(
        tx = %msg.id(),
        tx_type = ?msg.id().transaction_type(),
        "Dropping message after {MAX_RETRIES} retry attempts (operation busy)"
    );
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
                tracing::info!(
                    contract = %contract,
                    stale_peer = %source,
                    "Summary mismatch in interest sync — syncing state to stale peer"
                );
                if let Err(e) = op_manager
                    .notify_node_event(NodeEvent::SyncStateToPeer {
                        key: contract,
                        new_state: state,
                        target: source,
                    })
                    .await
                {
                    tracing::warn!(
                        contract = %contract,
                        error = %e,
                        "Failed to emit SyncStateToPeer for stale peer correction"
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

/// Attempts to subscribe to a contract with a specific transaction ID (for deduplication).
///
/// Since #1454 Phase 2b, this function is the entry point for
/// **client-initiated** SUBSCRIBE only — it spawns a task-per-tx driver via
/// [`crate::operations::subscribe::start_client_subscribe`] rather than going
/// through the legacy `request_subscribe` + `handle_op_result` re-entry loop.
///
/// The renewal-initiated path (`ring::connection_maintenance`), the PUT
/// sub-op path (`operations::start_subscription_request_internal`), and the
/// executor/WASM-initiated path (`contract::executor::SubscribeContract::resume_op`)
/// all call `subscribe::request_subscribe` directly and bypass this function,
/// so they continue on the legacy path unchanged.
///
/// The legacy `is_renewal` parameter has been removed in Phase 2b: no live
/// caller passes `true`, and the task-per-tx path does not carry renewal
/// jitter / spam-prevention semantics (those are owned by
/// `ring::connection_maintenance` and are load-bearing there). Accepting
/// `is_renewal=true` here would silently route a renewal through the wrong
/// code path — removing the parameter makes the misuse a compile error
/// instead of a runtime footgun (review finding L1).
///
/// # Parameters
///
/// - `client_id`: If set, registers a legacy subscription-result waiter via
///   `ch_outbound.waiting_for_subscription_result`. Both WS call sites in
///   `client_events.rs` leave this `None` because they pre-register a
///   transaction-result waiter via `waiting_for_transaction_result`.
/// - `transaction_id`: The client-visible transaction id. If `None`, a fresh
///   one is allocated — currently only the dead-code wrapper `subscribe()`
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

    // Task-per-tx: spawn the driver and return the client-visible tx
    // immediately. The spawned task owns retries, peer selection, local
    // completion, and result delivery via `result_router_tx`.
    subscribe::start_client_subscribe(op_manager, instance_id, client_tx).await
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
                        // Clean up phantom location_for_peer entry left by should_accept's
                        // record_pending_location (#3088). Without this, the gateway appears
                        // permanently connected and initial_join_procedure never retries it.
                        if let Some(peer_addr) = gateway.peer_addr.as_known() {
                            op_manager
                                .ring
                                .connection_manager
                                .prune_in_transit_connection(*peer_addr);

                            let backoff_duration = {
                                let mut backoff = op_manager.gateway_backoff.lock();
                                backoff.record_failure(*peer_addr);
                                backoff.remaining_backoff(*peer_addr)
                            };

                            if let Some(duration) = backoff_duration {
                                // Cap the wait at GATEWAY_BACKOFF_POLL_CAP when the
                                // node already has ring connections, matching the
                                // policy in initial_join_procedure (issue #3304).
                                // ±20% jitter to prevent thundering herd.
                                let open_conns = op_manager.ring.open_connections();
                                let effective = if open_conns > 0 {
                                    let jitter_ms = crate::config::GlobalRng::random_range(
                                        0u64..(connect::GATEWAY_BACKOFF_POLL_CAP.as_millis() / 5)
                                            as u64,
                                    );
                                    let cap = connect::GATEWAY_BACKOFF_POLL_CAP.mul_f64(0.8)
                                        + Duration::from_millis(jitter_ms);
                                    duration.min(cap)
                                } else {
                                    duration
                                };
                                tracing::info!(
                                    gateway = %gateway,
                                    backoff_secs = duration.as_secs(),
                                    effective_wait_secs = effective.as_secs(),
                                    open_connections = open_conns,
                                    "Gateway connection failed, waiting before retry"
                                );
                                // Use select! so suspend/isolation recovery can
                                // wake us immediately via gateway_backoff_cleared,
                                // matching the pattern in initial_join_procedure.
                                tokio::select! {
                                    _ = tokio::time::sleep(effective) => {},
                                    _ = op_manager.gateway_backoff_cleared.notified() => {
                                        tracing::info!(
                                            gateway = %gateway,
                                            "Gateway backoff cleared externally, retrying immediately"
                                        );
                                    },
                                }
                            }
                        }

                        tracing::debug!("Retrying connection to gateway {}", gateway);
                        connect::join_ring_request(&gateway, op_manager).await?;
                    }
                }
                Ok(Some(OpEnum::Connect(op))) => {
                    // Clean up phantom location_for_peer entry (#3088)
                    if let Some(peer_addr) = op.get_next_hop_addr() {
                        op_manager
                            .ring
                            .connection_manager
                            .prune_in_transit_connection(peer_addr);
                    }
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
                executor.delegate_request(op, origin_contract.as_ref())
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

/// Classify a `(TransactionType, OpOutcome)` pair into an optional `OpType` and success flag
/// for dashboard recording. Returns `(None, false)` for CONNECT transactions (not contract ops).
///
/// - `Irrelevant`: operation completed but without routing stats for this peer → success
/// - `Incomplete`: operation never finalized → failure
fn classify_op_outcome(
    tx_type: TransactionType,
    outcome: OpOutcome<'_>,
) -> (Option<network_status::OpType>, bool) {
    use network_status::OpType;
    match (tx_type, outcome) {
        (
            TransactionType::Get,
            OpOutcome::ContractOpSuccess { .. } | OpOutcome::ContractOpSuccessUntimed { .. },
        ) => (Some(OpType::Get), true),
        (TransactionType::Get, OpOutcome::ContractOpFailure { .. }) => (Some(OpType::Get), false),
        (
            TransactionType::Put,
            OpOutcome::ContractOpSuccess { .. } | OpOutcome::ContractOpSuccessUntimed { .. },
        ) => (Some(OpType::Put), true),
        (TransactionType::Put, OpOutcome::ContractOpFailure { .. }) => (Some(OpType::Put), false),
        (
            TransactionType::Update,
            OpOutcome::ContractOpSuccess { .. } | OpOutcome::ContractOpSuccessUntimed { .. },
        ) => (Some(OpType::Update), true),
        (TransactionType::Update, OpOutcome::ContractOpFailure { .. }) => {
            (Some(OpType::Update), false)
        }
        (
            TransactionType::Subscribe,
            OpOutcome::ContractOpSuccess { .. } | OpOutcome::ContractOpSuccessUntimed { .. },
        ) => (Some(OpType::Subscribe), true),
        (TransactionType::Subscribe, OpOutcome::ContractOpFailure { .. }) => {
            (Some(OpType::Subscribe), false)
        }
        // Irrelevant = completed successfully but without routing stats
        // (e.g., UPDATE when stats.target is None, SUBSCRIBE when stats is None)
        (TransactionType::Get, OpOutcome::Irrelevant) => (Some(OpType::Get), true),
        (TransactionType::Put, OpOutcome::Irrelevant) => (Some(OpType::Put), true),
        (TransactionType::Update, OpOutcome::Irrelevant) => (Some(OpType::Update), true),
        (TransactionType::Subscribe, OpOutcome::Irrelevant) => (Some(OpType::Subscribe), true),
        // Incomplete = operation never finalized
        (TransactionType::Get, OpOutcome::Incomplete) => (Some(OpType::Get), false),
        (TransactionType::Put, OpOutcome::Incomplete) => (Some(OpType::Put), false),
        (TransactionType::Update, OpOutcome::Incomplete) => (Some(OpType::Update), false),
        (TransactionType::Subscribe, OpOutcome::Incomplete) => (Some(OpType::Subscribe), false),
        // CONNECT is not a contract operation
        _ => (None, false),
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

    // classify_op_outcome tests
    mod classify_op_outcome_tests {
        use super::super::{classify_op_outcome, network_status::OpType};
        use crate::message::TransactionType;
        use crate::operations::OpOutcome;

        #[test]
        fn irrelevant_counted_as_success() {
            let (op_type, success) =
                classify_op_outcome(TransactionType::Update, OpOutcome::Irrelevant);
            assert!(matches!(op_type, Some(OpType::Update)));
            assert!(success);
        }

        #[test]
        fn incomplete_counted_as_failure() {
            let (op_type, success) =
                classify_op_outcome(TransactionType::Get, OpOutcome::Incomplete);
            assert!(matches!(op_type, Some(OpType::Get)));
            assert!(!success);
        }

        #[test]
        fn connect_skipped() {
            let (op_type, _) = classify_op_outcome(TransactionType::Connect, OpOutcome::Irrelevant);
            assert!(op_type.is_none());

            let (op_type, _) = classify_op_outcome(TransactionType::Connect, OpOutcome::Incomplete);
            assert!(op_type.is_none());
        }

        #[test]
        fn subscribe_irrelevant_is_success() {
            let (op_type, success) =
                classify_op_outcome(TransactionType::Subscribe, OpOutcome::Irrelevant);
            assert!(matches!(op_type, Some(OpType::Subscribe)));
            assert!(success);
        }

        #[test]
        fn put_incomplete_is_failure() {
            let (op_type, success) =
                classify_op_outcome(TransactionType::Put, OpOutcome::Incomplete);
            assert!(matches!(op_type, Some(OpType::Put)));
            assert!(!success);
        }
    }

    // Phase 1 (#1454) tests for forward_pending_op_result_if_completed.
    //
    // These exercise the callback-forwarding helper used by every branch of
    // `handle_pure_network_message_v1`. The helper is the only place that
    // drives the `pending_op_result` oneshot channel from a completed op
    // result back to a caller of `OpCtx::send_and_await`. Phase 1 extended
    // the hook from PUT/GET only to cover SUBSCRIBE/CONNECT/UPDATE as well,
    // so these tests verify the helper forwards correctly for every op
    // variant and short-circuits in the negative cases. (The caller side
    // used to live on `OpManager::notify_op_execution`, which Phase 2a
    // replaced with `OpCtx::send_and_await` — see #1454.)
    mod callback_forward_tests {
        use super::super::{
            OpError, OpNotAvailable, forward_pending_op_result_if_completed,
            try_forward_task_per_tx_reply,
        };
        use crate::message::{MessageStats, NetMessage, NetMessageV1, Transaction};
        use crate::operations::OpEnum;
        use crate::operations::connect::{ConnectMsg, ConnectOp, ConnectState};

        fn completed_connect_op() -> ConnectOp {
            ConnectOp::with_state(ConnectState::Completed)
        }

        fn dummy_reply() -> NetMessage {
            // We don't care about the payload — the helper only looks at
            // `NetMessage::id()` for logging. Use the tx-only `Aborted`
            // variant to avoid building an entire ConnectMsg payload.
            NetMessage::V1(NetMessageV1::Aborted(Transaction::new::<ConnectMsg>()))
        }

        #[tokio::test]
        async fn forwards_reply_when_completed_and_sender_present() {
            let op = completed_connect_op();
            let op_result = Ok(Some(OpEnum::Connect(Box::new(op))));

            let (tx, mut rx) = tokio::sync::mpsc::channel::<NetMessage>(1);
            let reply = dummy_reply();
            let expected_id = *reply.id();

            forward_pending_op_result_if_completed(&op_result, Some(&tx), reply);

            let received = rx.try_recv().expect("helper should forward the reply");
            assert_eq!(*received.id(), expected_id);
        }

        #[tokio::test]
        async fn no_forward_when_sender_absent() {
            // Helper must not panic / block when no pending_op_result sender is wired.
            let op = completed_connect_op();
            let op_result = Ok(Some(OpEnum::Connect(Box::new(op))));

            forward_pending_op_result_if_completed(&op_result, None, dummy_reply());
            // Nothing to assert beyond "did not panic".
        }

        #[tokio::test]
        async fn no_forward_when_op_not_completed() {
            // `Ok(None)` and OpError variants should not trigger a send even if
            // a sender is present. This is the guard that keeps in-progress
            // ops (e.g. `SendAndContinue`) from prematurely firing the callback.
            let (tx, mut rx) = tokio::sync::mpsc::channel::<NetMessage>(1);

            let ok_none: Result<Option<OpEnum>, OpError> = Ok(None);
            forward_pending_op_result_if_completed(&ok_none, Some(&tx), dummy_reply());
            assert!(rx.try_recv().is_err(), "Ok(None) must not forward");

            let err_running: Result<Option<OpEnum>, OpError> =
                Err(OpError::OpNotAvailable(OpNotAvailable::Running));
            forward_pending_op_result_if_completed(&err_running, Some(&tx), dummy_reply());
            assert!(
                rx.try_recv().is_err(),
                "OpNotAvailable::Running must not forward"
            );

            let err_completed: Result<Option<OpEnum>, OpError> =
                Err(OpError::OpNotAvailable(OpNotAvailable::Completed));
            forward_pending_op_result_if_completed(&err_completed, Some(&tx), dummy_reply());
            assert!(
                rx.try_recv().is_err(),
                "OpNotAvailable::Completed must not forward (no OpEnum payload)"
            );
        }

        #[tokio::test]
        async fn no_forward_when_op_in_progress() {
            // A non-completed op state (WaitingForResponses) must not trigger
            // the callback even though the op exists — this is the core guard
            // that keeps mid-flight operations from prematurely terminating
            // an `OpCtx::send_and_await` round-trip.
            use crate::operations::connect::JoinerState;
            use std::collections::HashSet;
            use tokio::time::Instant;

            let waiting = ConnectState::WaitingForResponses(JoinerState {
                target_connections: 1,
                observed_address: None,
                accepted: HashSet::new(),
                last_progress: Instant::now(),
                started_without_address: true,
            });
            let op = ConnectOp::with_state(waiting);
            assert!(
                !op.is_completed(),
                "precondition: WaitingForResponses must not be completed"
            );
            let op_result = Ok(Some(OpEnum::Connect(Box::new(op))));

            let (tx, mut rx) = tokio::sync::mpsc::channel::<NetMessage>(1);
            forward_pending_op_result_if_completed(&op_result, Some(&tx), dummy_reply());
            assert!(
                rx.try_recv().is_err(),
                "in-progress op must not forward to pending_op_result"
            );
        }

        #[tokio::test]
        async fn no_hang_when_receiver_dropped() {
            // Regression guard for the `try_send` channel-safety choice:
            // if the `OpCtx::send_and_await` caller drops its receiver
            // (e.g. cancelled, timed out) before the op completes, the
            // reply side must not block the pure-network-message handler.
            // With `try_send` the send fails with `Closed` and we log;
            // with `.send().await` it would have succeeded but stranded
            // the message. Either way the handler must make progress —
            // the test asserts the helper returns promptly (the
            // `#[tokio::test]` runtime would hang the whole test process
            // on regression).
            let op = completed_connect_op();
            let op_result = Ok(Some(OpEnum::Connect(Box::new(op))));

            let (tx, rx) = tokio::sync::mpsc::channel::<NetMessage>(1);
            drop(rx);

            forward_pending_op_result_if_completed(&op_result, Some(&tx), dummy_reply());
            // Returning at all is the assertion.
        }

        // ───────────────────────────────────────────────────────────
        // Phase 2b (#1454) task-per-tx bypass tests for
        // `try_forward_task_per_tx_reply`.
        //
        // The bypass routes a reply directly to an awaiting
        // `OpCtx::send_and_await` caller, skipping the legacy
        // `handle_op_request` path entirely. These tests cover the
        // helper's contract; end-to-end "the SUBSCRIBE branch of
        // `handle_pure_network_message_v1` actually invokes the helper"
        // coverage comes from the `run_client_subscribe` tests added
        // alongside the Phase 2b migration.
        // ───────────────────────────────────────────────────────────

        #[tokio::test]
        async fn bypass_forwards_when_callback_registered() {
            let (tx, mut rx) = tokio::sync::mpsc::channel::<NetMessage>(1);
            let reply = dummy_reply();
            let expected_id = *reply.id();

            let taken = try_forward_task_per_tx_reply(Some(&tx), reply, "subscribe");
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
            let taken = try_forward_task_per_tx_reply(None, dummy_reply(), "subscribe");
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

            let taken = try_forward_task_per_tx_reply(Some(&tx), dummy_reply(), "subscribe");
            assert!(
                taken,
                "callback present but receiver dropped → bypass still taken"
            );
        }

        /// Pin the bypass call site. Without this regression guard a
        /// future refactor could delete the
        /// `try_forward_task_per_tx_reply` invocation in the SUBSCRIBE
        /// branch of `handle_pure_network_message_v1` and the unit tests
        /// on the helper itself would still pass — because unit coverage
        /// on the helper only proves the helper works, not that it's
        /// wired in. Integration (simulation) failures would catch it
        /// eventually but as end-to-end hangs, which is a noisy signal.
        ///
        /// This test reads the `node.rs` source at compile time via
        /// `include_str!` and asserts that the SUBSCRIBE branch of
        /// `handle_pure_network_message_v1` invokes
        /// `try_forward_task_per_tx_reply` before running
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
            let subscribe_branch_anchor = "NetMessageV1::Subscribe(ref op) => {";
            let branch_start = SOURCE.find(subscribe_branch_anchor).expect(
                "SUBSCRIBE branch of handle_pure_network_message_v1 not found; \
                         the match arm has been renamed or moved — update this regression guard",
            );

            // Slice a window large enough to contain the branch body up
            // to (and including) the first `handle_op_request` call.
            let window_end = SOURCE[branch_start..]
                .find("handle_op_request::<subscribe::SubscribeOp, _>")
                .expect("SUBSCRIBE branch no longer calls handle_op_request — update guard")
                + branch_start;
            let window = &SOURCE[branch_start..window_end];

            // The bypass helper MUST be invoked BEFORE the legacy
            // handle_op_request call. If this assertion fails, either:
            //   (a) the bypass was removed (regression — re-add it), or
            //   (b) the branch was restructured (update this guard).
            assert!(
                window.contains("try_forward_task_per_tx_reply("),
                "SUBSCRIBE branch no longer calls try_forward_task_per_tx_reply \
                 before handle_op_request. This is the bypass Phase 2b (#1454) \
                 added to prevent task-per-tx callers from hanging on replies \
                 that load_or_init would drop as OpNotPresent. Either restore \
                 the bypass invocation or update this regression guard if the \
                 branch has been legitimately refactored."
            );
        }

        #[tokio::test]
        async fn bypass_does_not_block_when_channel_already_full() {
            // Defensive regression: `try_send` on a full channel must
            // fail without blocking the pure-network-message handler.
            // Although Phase 1's dedup guarantees the callback fires at
            // most once per tx (so a "full" capacity-1 channel
            // shouldn't happen in practice), this test pins the
            // non-blocking contract so future refactors can't
            // accidentally switch to `.send().await` and reintroduce
            // the class of bug documented in
            // `.claude/rules/channel-safety.md`.
            let (tx, _rx) = tokio::sync::mpsc::channel::<NetMessage>(1);
            // Pre-fill the capacity-1 channel.
            tx.try_send(dummy_reply())
                .expect("capacity-1 channel should accept first message");

            let taken = try_forward_task_per_tx_reply(Some(&tx), dummy_reply(), "subscribe");
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
        // `OpCtx::send_and_await` for each op kind belongs in Phase 2b,
        // where the first real production caller is added.
    }
}
