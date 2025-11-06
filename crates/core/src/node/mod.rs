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
    prelude::ContractKey,
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
    config::{Address, GatewayConfig, WebsocketApiConfig},
    contract::{Callback, ExecutorError, ExecutorToEventLoopChannel, NetworkContractHandler},
    local_node::Executor,
    message::{InnerMessage, NetMessage, Transaction, TransactionType},
    operations::{
        connect::{self, ConnectOp},
        connect_v2::ConnectOpV2,
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
use rsa::pkcs8::DecodePublicKey;
use serde::{Deserialize, Serialize};
use tracing::Instrument;

use crate::operations::handle_op_request;
pub(crate) use network_bridge::{ConnectionError, EventLoopNotificationsSender, NetworkBridge};

use crate::topology::rate::Rate;
use crate::transport::{TransportKeypair, TransportPublicKey};
pub(crate) use op_state_manager::{OpManager, OpNotAvailable};

mod message_processor;
mod network_bridge;
mod op_state_manager;
mod p2p_impl;
mod request_router;
pub(crate) mod testing_impl;

pub use message_processor::MessageProcessor;
pub use request_router::{DeduplicatedRequest, RequestRouter};

pub struct Node(NodeP2P);

impl Node {
    pub fn update_location(&mut self, location: Location) {
        self.0
            .op_manager
            .ring
            .connection_manager
            .update_location(Some(location));
    }

    pub async fn run(self) -> anyhow::Result<Infallible> {
        self.0.run_node().await
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
    pub(crate) peer_id: Option<PeerId>,
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

            let mut key_file = File::open(public_key_path).with_context(|| {
                format!("failed loading gateway pubkey from {public_key_path:?}")
            })?;
            let mut buf = String::new();
            key_file.read_to_string(&mut buf)?;

            let pub_key = rsa::RsaPublicKey::from_public_key_pem(&buf)?;
            let transport_pub_key = TransportPublicKey::from(pub_key);

            // Skip if this gateway's public key matches our own
            if &transport_pub_key == own_pub_key {
                tracing::warn!(
                    "Skipping gateway with same public key as self: {:?}",
                    public_key_path
                );
                continue;
            }

            let address = Self::parse_socket_addr(address).await?;
            let peer_id = PeerId::new(address, transport_pub_key);
            let location = location
                .map(Location::new)
                .unwrap_or_else(|| Location::from_address(&address));
            gateways.push(InitPeerNode::new(peer_id, location));
        }
        tracing::info!(
            "Node will be listening at {}:{} internal address",
            config.network_api.address,
            config.network_api.port
        );
        if let Some(peer_id) = &config.peer_id {
            tracing::info!("Node external address: {}", peer_id.addr);
        }
        Ok(NodeConfig {
            should_connect: true,
            is_gateway: config.is_gateway,
            key_pair: config.transport_keypair().clone(),
            gateways,
            peer_id: config.peer_id.clone(),
            network_listener_ip: config.network_api.address,
            network_listener_port: config.network_api.port,
            location: config.location.map(Location::new),
            config: Arc::new(config.clone()),
            max_hops_to_live: None,
            rnd_if_htl_above: None,
            max_number_conn: None,
            min_number_conn: None,
            max_upstream_bandwidth: None,
            max_downstream_bandwidth: None,
            blocked_addresses: config.network_api.blocked_addresses.clone(),
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

    pub fn with_peer_id(&mut self, peer_id: PeerId) -> &mut Self {
        self.peer_id = Some(peer_id);
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
            #[cfg(feature = "trace-ot")]
            {
                use super::tracing::{CombinedRegister, OTEventRegister};
                let event_reg = EventRegister::new(self.config.event_log());
                let flush_handle = event_reg.flush_handle();
                (
                    CombinedRegister::new([Box::new(event_reg), Box::new(OTEventRegister::new())]),
                    flush_handle,
                )
            }
            #[cfg(not(feature = "trace-ot"))]
            {
                let event_reg = EventRegister::new(self.config.event_log());
                let flush_handle = event_reg.flush_handle();
                (event_reg, flush_handle)
            }
        };
        let cfg = self.config.clone();
        let node = NodeP2P::build::<NetworkContractHandler, CLIENTS, _>(
            self,
            clients,
            event_register,
            cfg,
        )
        .await?;
        Ok((Node(node), flush_handle))
    }

    pub fn get_peer_id(&self) -> Option<PeerId> {
        self.peer_id.clone()
    }

    /// Returns all specified gateways for this peer. Returns an error if the peer is not a gateway
    /// and no gateways are specified.
    fn get_gateways(&self) -> anyhow::Result<Vec<PeerKeyLocation>> {
        let gateways: Vec<PeerKeyLocation> = self
            .gateways
            .iter()
            .map(|node| PeerKeyLocation {
                peer: node.peer_id.clone(),
                location: Some(node.location),
            })
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
    peer_id: PeerId,
    location: Location,
}

impl InitPeerNode {
    pub fn new(peer_id: PeerId, location: Location) -> Self {
        Self { peer_id, location }
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

            // Send to result router
            if let Some(transaction) = tx {
                let host_result = op_res.to_host_result();
                let router_tx_clone = op_manager.result_router_tx.clone();
                let event_notifier = op_manager.to_event_listener.clone();

                // Spawn fire-and-forget task to avoid blocking report_result()
                // while still guaranteeing message delivery
                tokio::spawn(async move {
                    if let Err(e) = router_tx_clone.send((transaction, host_result)).await {
                        tracing::error!(
                            "CRITICAL: Result router channel closed - dual-path delivery broken. \
                             Router or session actor has crashed. Transaction: {}. Error: {}. \
                             Consider restarting node.",
                            transaction,
                            e
                        );
                        // TODO: Consider implementing circuit breaker or automatic recovery
                    } else {
                        // Transaction completed successfully, notify to clean up subscriptions
                        use crate::message::NodeEvent;
                        use either::Either;
                        let _ = event_notifier
                            .notifications_sender
                            .send(Either::Right(NodeEvent::TransactionCompleted(transaction)))
                            .await;
                    }
                });
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
                    event_listener
                        .register_events(Either::Left(NetEventLog::route_event(
                            op_res.id(),
                            &op_manager.ring,
                            &event,
                        )))
                        .await;
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
                    let peer = &op_manager
                        .ring
                        .connection_manager
                        .get_peer_key()
                        .expect("Peer key not found");
                    let log = format!(
                        "Transaction ({tx} @ {peer}) error trace:\n {trace} \nstate:\n {state:?}\n"
                    );
                    std::io::stderr().write_all(log.as_bytes()).unwrap();
                }
                #[cfg(not(debug_assertions))]
                {
                    let peer = &op_manager
                        .ring
                        .connection_manager
                        .get_peer_key()
                        .expect("Peer key not found");
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

macro_rules! handle_op_not_available {
    ($op_result:ident) => {
        if let Err(OpError::OpNotAvailable(state)) = &$op_result {
            match state {
                OpNotAvailable::Running => {
                    tracing::debug!("Operation still running");
                    // TODO: do exponential backoff
                    tokio::time::sleep(Duration::from_micros(1_000)).await;
                    continue;
                }
                OpNotAvailable::Completed => {
                    tracing::debug!("Operation already completed");
                    return;
                }
            }
        }
    };
}

/// Legacy process_message - only kept for testing_impl
/// Production code uses process_message_decoupled instead
pub(super) async fn process_message<CB>(
    msg: NetMessage,
    op_manager: Arc<OpManager>,
    conn_manager: CB,
    event_listener: Box<dyn NetEventRegister>,
    executor_callback: Option<ExecutorToEventLoopChannel<crate::contract::Callback>>,
    pending_op_result: Option<tokio::sync::mpsc::Sender<NetMessage>>,
) where
    CB: NetworkBridge,
{
    let tx = Some(*msg.id());
    match msg {
        NetMessage::V1(msg_v1) => {
            process_message_v1(
                tx,
                msg_v1,
                op_manager,
                conn_manager,
                event_listener,
                executor_callback,
                pending_op_result,
            )
            .await
        }
    }
}

/// NEW: Pure network message processing using MessageProcessor for client handling
#[allow(clippy::too_many_arguments)]
pub(crate) async fn process_message_decoupled<CB>(
    msg: NetMessage,
    op_manager: Arc<OpManager>,
    conn_manager: CB,
    event_listener: Box<dyn NetEventRegister>,
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
        op_manager.clone(),
        conn_manager,
        event_listener,
        executor_callback,
        pending_op_result,
    )
    .await;

    // Delegate to MessageProcessor - it handles all client concerns internally
    if let Err(e) = message_processor.handle_network_result(tx, op_result).await {
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
    op_manager: Arc<OpManager>,
    conn_manager: CB,
    event_listener: Box<dyn NetEventRegister>,
    executor_callback: Option<ExecutorToEventLoopChannel<crate::contract::Callback>>,
    pending_op_result: Option<tokio::sync::mpsc::Sender<NetMessage>>,
) -> Result<Option<crate::operations::OpEnum>, crate::node::OpError>
where
    CB: NetworkBridge,
{
    match msg {
        NetMessage::V1(msg_v1) => {
            handle_pure_network_message_v1(
                msg_v1,
                op_manager,
                conn_manager,
                event_listener,
                executor_callback,
                pending_op_result,
            )
            .await
        }
    }
}

async fn process_message_v1<CB>(
    tx: Option<Transaction>,
    msg: NetMessageV1,
    op_manager: Arc<OpManager>,
    mut conn_manager: CB,
    mut event_listener: Box<dyn NetEventRegister>,
    executor_callback: Option<ExecutorToEventLoopChannel<crate::contract::Callback>>,
    pending_op_result: Option<tokio::sync::mpsc::Sender<NetMessage>>,
) where
    CB: NetworkBridge,
{
    event_listener
        .register_events(NetEventLog::from_inbound_msg_v1(&msg, &op_manager))
        .await;

    const MAX_RETRIES: usize = 10usize;
    for i in 0..MAX_RETRIES {
        tracing::debug!(?tx, "Processing operation, iteration: {i}");
        match msg {
            NetMessageV1::Connect(ref op) => {
                let parent_span = tracing::Span::current();
                let span = tracing::info_span!(
                    parent: parent_span,
                    "handle_connect_op_request",
                    transaction = %msg.id(),
                    tx_type = %msg.id().transaction_type()
                );
                let op_result =
                    handle_op_request::<connect::ConnectOp, _>(&op_manager, &mut conn_manager, op)
                        .instrument(span)
                        .await;

                handle_op_not_available!(op_result);
                return report_result(
                    tx,
                    op_result,
                    &op_manager,
                    executor_callback,
                    &mut *event_listener,
                )
                .await;
            }
            NetMessageV1::ConnectV2(ref op) => {
                let parent_span = tracing::Span::current();
                let span = tracing::info_span!(
                    parent: parent_span,
                    "handle_connect_v2_op_request",
                    transaction = %msg.id(),
                    tx_type = %msg.id().transaction_type()
                );
                let op_result =
                    handle_op_request::<ConnectOpV2, _>(&op_manager, &mut conn_manager, op)
                        .instrument(span)
                        .await;

                handle_op_not_available!(op_result);
                return report_result(
                    tx,
                    op_result,
                    &op_manager,
                    executor_callback,
                    &mut *event_listener,
                )
                .await;
            }
            NetMessageV1::Put(ref op) => {
                let op_result =
                    handle_op_request::<put::PutOp, _>(&op_manager, &mut conn_manager, op).await;

                if is_operation_completed(&op_result) {
                    if let Some(ref op_execution_callback) = pending_op_result {
                        let tx_id = *op.id();
                        let _ = op_execution_callback
                            .send(NetMessage::V1(NetMessageV1::Put((*op).clone())))
                            .await
                            .inspect_err(
                                |err| tracing::error!(%err, %tx_id, "Failed to send message to client"),
                            );
                    }
                }

                handle_op_not_available!(op_result);
                return report_result(
                    tx,
                    op_result,
                    &op_manager,
                    executor_callback,
                    &mut *event_listener,
                )
                .await;
            }
            NetMessageV1::Get(ref op) => {
                let op_result =
                    handle_op_request::<get::GetOp, _>(&op_manager, &mut conn_manager, op).await;
                if is_operation_completed(&op_result) {
                    if let Some(ref op_execution_callback) = pending_op_result {
                        let tx_id = *op.id();
                        let _ = op_execution_callback
                            .send(NetMessage::V1(NetMessageV1::Get((*op).clone()))).await.inspect_err(|err|
                                tracing::error!(%err, %tx_id, "Failed to send message to client")
                            );
                    }
                }
                handle_op_not_available!(op_result);
                return report_result(
                    tx,
                    op_result,
                    &op_manager,
                    executor_callback,
                    &mut *event_listener,
                )
                .await;
            }
            NetMessageV1::Subscribe(ref op) => {
                let op_result = handle_op_request::<subscribe::SubscribeOp, _>(
                    &op_manager,
                    &mut conn_manager,
                    op,
                )
                .await;
                if is_operation_completed(&op_result) {
                    if let Some(ref op_execution_callback) = pending_op_result {
                        let tx_id = *op.id();
                        let _ = op_execution_callback
                            .send(NetMessage::V1(NetMessageV1::Subscribe((*op).clone()))).await.inspect_err(|err|
                                tracing::error!(%err, %tx_id, "Failed to send message to client")
                            );
                    }
                }
                handle_op_not_available!(op_result);
                return report_result(
                    tx,
                    op_result,
                    &op_manager,
                    executor_callback,
                    &mut *event_listener,
                )
                .await;
            }
            NetMessageV1::Update(ref op) => {
                let op_result =
                    handle_op_request::<update::UpdateOp, _>(&op_manager, &mut conn_manager, op)
                        .await;
                if is_operation_completed(&op_result) {
                    if let Some(ref op_execution_callback) = pending_op_result {
                        let tx_id = *op.id();
                        let _ = op_execution_callback
                                .send(NetMessage::V1(NetMessageV1::Update((*op).clone()))).await.inspect_err(|err|
                                    tracing::error!(%err, %tx_id, "Failed to send message to client")
                                );
                    }
                }
                handle_op_not_available!(op_result);
                return report_result(
                    tx,
                    op_result,
                    &op_manager,
                    executor_callback,
                    &mut *event_listener,
                )
                .await;
            }
            NetMessageV1::Unsubscribed {
                ref key, ref from, ..
            } => {
                tracing::debug!(
                    "Received Unsubscribed message for contract {} from peer {}",
                    key,
                    from
                );
                op_manager.ring.remove_subscriber(key, from);
                break;
            }
            _ => break, // Exit the loop if no applicable message type is found
        }
    }
}

/// Pure network message processing for V1 messages (no client concerns)
#[allow(clippy::too_many_arguments)]
async fn handle_pure_network_message_v1<CB>(
    msg: NetMessageV1,
    op_manager: Arc<OpManager>,
    mut conn_manager: CB,
    mut event_listener: Box<dyn NetEventRegister>,
    executor_callback: Option<ExecutorToEventLoopChannel<crate::contract::Callback>>,
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
                let op_result =
                    handle_op_request::<connect::ConnectOp, _>(&op_manager, &mut conn_manager, op)
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

                // Pure network result processing - no client handling
                return handle_pure_network_result(
                    tx,
                    op_result,
                    &op_manager,
                    executor_callback,
                    &mut *event_listener,
                )
                .await;
            }
            NetMessageV1::ConnectV2(ref op) => {
                let parent_span = tracing::Span::current();
                let span = tracing::info_span!(
                    parent: parent_span,
                    "handle_connect_v2_op_request",
                    transaction = %msg.id(),
                    tx_type = %msg.id().transaction_type()
                );
                let op_result =
                    handle_op_request::<ConnectOpV2, _>(&op_manager, &mut conn_manager, op)
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
                    executor_callback,
                    &mut *event_listener,
                )
                .await;
            }
            NetMessageV1::Put(ref op) => {
                tracing::debug!(
                    tx = %op.id(),
                    "handle_pure_network_message_v1: Processing PUT message"
                );
                let op_result =
                    handle_op_request::<put::PutOp, _>(&op_manager, &mut conn_manager, op).await;
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
                    executor_callback,
                    &mut *event_listener,
                )
                .await;
            }
            NetMessageV1::Get(ref op) => {
                let op_result =
                    handle_op_request::<get::GetOp, _>(&op_manager, &mut conn_manager, op).await;

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
                    executor_callback,
                    &mut *event_listener,
                )
                .await;
            }
            NetMessageV1::Update(ref op) => {
                let op_result =
                    handle_op_request::<update::UpdateOp, _>(&op_manager, &mut conn_manager, op)
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
                    executor_callback,
                    &mut *event_listener,
                )
                .await;
            }
            NetMessageV1::Subscribe(ref op) => {
                let op_result = handle_op_request::<subscribe::SubscribeOp, _>(
                    &op_manager,
                    &mut conn_manager,
                    op,
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
                    executor_callback,
                    &mut *event_listener,
                )
                .await;
            }
            NetMessageV1::Unsubscribed {
                ref key, ref from, ..
            } => {
                tracing::debug!(
                    "Received Unsubscribed message for contract {} from peer {}",
                    key,
                    from
                );
                op_manager.ring.remove_subscriber(key, from);
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
    _executor_callback: Option<ExecutorToEventLoopChannel<crate::contract::Callback>>,
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
            // Executor callback functionality needs to be restored with proper types
            if let Some(_callback) = _executor_callback {
                tracing::debug!("Executor callback available for transaction {:?} but not implemented in pure network processing", tx);
            }
        }
        Ok(None) => {
            tracing::debug!("Network operation returned no result");
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

/// Attempts to subscribe to a contract
#[allow(dead_code)]
pub async fn subscribe(
    op_manager: Arc<OpManager>,
    key: ContractKey,
    client_id: Option<ClientId>,
) -> Result<Transaction, OpError> {
    subscribe_with_id(op_manager, key, client_id, None).await
}

/// Attempts to subscribe to a contract with a specific transaction ID (for deduplication)
pub async fn subscribe_with_id(
    op_manager: Arc<OpManager>,
    key: ContractKey,
    client_id: Option<ClientId>,
    transaction_id: Option<Transaction>,
) -> Result<Transaction, OpError> {
    let op = match transaction_id {
        Some(id) => subscribe::start_op_with_id(key, id),
        None => subscribe::start_op(key),
    };
    let id = op.id;
    if let Some(client_id) = client_id {
        use crate::client_events::RequestId;
        // Generate a default RequestId for internal subscription operations
        let request_id = RequestId::new();
        let _ = op_manager
            .ch_outbound
            .waiting_for_subscription_result(id, *key.id(), client_id, request_id)
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
    if let TransactionType::Connect = tx.transaction_type() {
        // attempt to establish a connection failed, this could be a fatal error since the node
        // is useless without connecting to the network, we will retry with exponential backoff
        // if necessary
        match op_manager.pop(&tx) {
            // only keep attempting to connect if the node hasn't got enough connections yet
            Ok(Some(OpEnum::Connect(op)))
                if op.has_backoff()
                    && op_manager.ring.open_connections()
                        < op_manager.ring.connection_manager.min_connections =>
            {
                let ConnectOp {
                    gateway, backoff, ..
                } = *op;
                if let Some(gateway) = gateway {
                    tracing::warn!("Retry connecting to gateway {}", gateway.peer);
                    connect::join_ring_request(backoff, &gateway, op_manager).await?;
                }
            }
            Ok(Some(OpEnum::ConnectV2(op)))
                if op.has_backoff()
                    && op_manager.ring.open_connections()
                        < op_manager.ring.connection_manager.min_connections =>
            {
                let gateway = op.gateway().cloned();
                if let Some(gateway) = gateway {
                    tracing::warn!("Retry connecting to gateway {}", gateway.peer);
                    connect::join_ring_request(None, &gateway, op_manager).await?;
                }
            }
            Ok(Some(OpEnum::Connect(_))) => {
                // if no connections were achieved just fail
                if op_manager.ring.open_connections() == 0 && op_manager.ring.is_gateway() {
                    tracing::warn!("Retrying joining the ring with an other gateway");
                    if let Some(gateway) = gateways.iter().shuffle().next() {
                        connect::join_ring_request(None, gateway, op_manager).await?
                    }
                }
            }
            Ok(Some(OpEnum::ConnectV2(_))) => {
                if op_manager.ring.open_connections() == 0 && op_manager.ring.is_gateway() {
                    tracing::warn!("Retrying joining the ring with an other gateway");
                    if let Some(gateway) = gateways.iter().shuffle().next() {
                        connect::join_ring_request(None, gateway, op_manager).await?
                    }
                }
            }
            _ => {}
        }
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
        use rand::Rng;
        let mut addr = [0; 4];
        rand::rng().fill(&mut addr[..]);
        let port = crate::util::get_free_port().unwrap();

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

    let (mut gw, mut ws_proxy) = crate::server::serve_gateway_in(socket).await;

    // TODO: use combinator instead
    // let mut all_clients =
    //    ClientEventsCombinator::new([Box::new(ws_handle), Box::new(http_handle)]);
    enum Receiver {
        Ws,
        Gw,
    }
    let mut receiver;
    loop {
        let req = tokio::select! {
            req = ws_proxy.recv() => {
                receiver = Receiver::Ws;
                req?
            }
            req = gw.recv() => {
                receiver = Receiver::Gw;
                req?
            }
        };
        let OpenRequest {
            client_id: id,
            request,
            notification_channel,
            token,
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
                let attested_contract = token.and_then(|token| {
                    gw.attested_contracts
                        .get(&token)
                        .map(|entry| entry.value().contract_id)
                });
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

    let is_gateway = node.0.is_gateway;
    let location = if let Some(loc) = node.0.location {
        Some(loc)
    } else {
        is_gateway
            .then(|| {
                node.0
                    .peer_id
                    .clone()
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
            OpEnum::ConnectV2(op) => op.is_completed(),
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

    #[tokio::test]
    async fn test_hostname_resolution() {
        let addr = Address::Hostname("localhost".to_string());
        let socket_addr = NodeConfig::parse_socket_addr(&addr).await.unwrap();
        assert!(
            socket_addr.ip() == IpAddr::V4(Ipv4Addr::LOCALHOST)
                || socket_addr.ip() == IpAddr::V6(Ipv6Addr::LOCALHOST)
        );
        // Port should be in valid range
        assert!(socket_addr.port() > 1024); // Ensure we're using unprivileged ports

        let addr = Address::Hostname("google.com".to_string());
        let socket_addr = NodeConfig::parse_socket_addr(&addr).await.unwrap();
        // Port should be in valid range
        assert!(socket_addr.port() > 1024); // Ensure we're using unprivileged ports

        let addr = Address::Hostname("google.com:8080".to_string());
        let socket_addr = NodeConfig::parse_socket_addr(&addr).await.unwrap();
        assert_eq!(socket_addr.port(), 8080);
    }
}
