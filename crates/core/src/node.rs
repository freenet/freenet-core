//! The main node data type which encapsulates all the behaviour for maintaining a connection
//! and performing operations within the network.
//!
//! # Implementations
//! Node comes with different underlying implementations that can be used upon construction.
//! Those implementations are:
//! - libp2p: all the connection is handled by libp2p.
//! - in-memory: a simplifying node used for emulation purposes mainly.
//! - inter-process: similar to in-memory, but can be rana cross multiple processes, closer to the real p2p impl

use std::{
    fmt::Display,
    io::Write,
    net::{IpAddr, SocketAddr},
    sync::Arc,
    time::Duration,
};

use either::Either;
use freenet_stdlib::{
    client_api::{ClientRequest, ContractRequest, ErrorKind},
    prelude::{ContractKey, RelatedContracts, WrappedState},
};

use serde::{Deserialize, Serialize};
use tracing::Instrument;

use self::p2p_impl::NodeP2P;
use crate::{
    client_events::{BoxedClient, ClientEventsProxy, ClientId, OpenRequest},
    config::GlobalExecutor,
    contract::{
        Callback, ClientResponsesReceiver, ClientResponsesSender, ContractError,
        ExecutorToEventLoopChannel, NetworkContractHandler,
    },
    message::{NetMessage, NodeEvent, Transaction, TransactionType},
    operations::{
        connect::{self, ConnectOp},
        get, put, subscribe, update, OpEnum, OpError, OpOutcome,
    },
    ring::{Location, PeerKeyLocation},
    router::{RouteEvent, RouteOutcome},
    tracing::{EventRegister, NetEventLog, NetEventRegister},
    DynError,
};
use crate::{
    config::Config,
    message::{MessageStats, NetMessageV1},
};

use crate::operations::handle_op_request;
pub use network_bridge::inter_process::InterProcessConnManager;
pub(crate) use network_bridge::{ConnectionError, EventLoopNotificationsSender, NetworkBridge};

use crate::topology::rate::Rate;
use crate::transport::{TransportKeypair, TransportPublicKey};
pub(crate) use op_state_manager::{OpManager, OpNotAvailable};

mod network_bridge;
mod op_state_manager;
mod p2p_impl;
pub(crate) mod testing_impl;

pub struct Node(NodeP2P);

impl Node {
    pub async fn run(self) -> Result<(), DynError> {
        self.0.run_node().await?;
        Ok(())
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
pub struct NodeConfig {
    /// Determines if an initial connection should be attempted.
    /// Only true for an initial gateway/node. If false, the gateway will be disconnected unless other peers connect through it.
    pub should_connect: bool,
    pub is_gateway: bool,
    /// If not specified, a key is generated and used when creating the node.
    pub key_pair: TransportKeypair,
    // optional local info, in case this is an initial bootstrap node
    /// IP to bind to the network listener.
    pub local_ip: Option<IpAddr>,
    /// socket port to bind to the network listener.
    pub local_port: Option<u16>,
    pub(crate) config: Arc<Config>,
    /// IP dialers should connect to
    pub(crate) public_ip: Option<IpAddr>,
    /// socket port dialers should connect to, only set on gateways
    pub(crate) public_port: Option<u16>,
    /// At least an other running listener node is required for joining the network.
    /// Not necessary if this is an initial node.
    pub(crate) remote_nodes: Vec<InitPeerNode>,
    /// the location of this node, used for gateways.
    pub(crate) location: Option<Location>,
    pub(crate) max_hops_to_live: Option<usize>,
    pub(crate) rnd_if_htl_above: Option<usize>,
    pub(crate) max_number_conn: Option<usize>,
    pub(crate) min_number_conn: Option<usize>,
    pub(crate) max_upstream_bandwidth: Option<Rate>,
    pub(crate) max_downstream_bandwidth: Option<Rate>,
}

impl NodeConfig {
    pub fn new(config: Config) -> NodeConfig {
        NodeConfig {
            should_connect: true,
            is_gateway: false,
            key_pair: config.transport_keypair.clone(),
            remote_nodes: Vec::with_capacity(1),
            config: Arc::new(config),
            local_ip: None,
            local_port: None,
            public_ip: None,
            public_port: None,
            location: None,
            max_hops_to_live: None,
            rnd_if_htl_above: None,
            max_number_conn: None,
            min_number_conn: None,
            max_upstream_bandwidth: None,
            max_downstream_bandwidth: None,
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

    pub fn with_port(&mut self, port: u16) -> &mut Self {
        self.local_port = Some(port);
        self
    }

    pub fn with_ip<T: Into<IpAddr>>(&mut self, ip: T) -> &mut Self {
        self.local_ip = Some(ip.into());
        self
    }

    pub fn with_location(&mut self, loc: Location) -> &mut Self {
        self.location = Some(loc);
        self
    }

    /// Connection info for an already existing peer. Required in case this is not a gateway node.
    pub fn add_gateway(&mut self, peer: InitPeerNode) -> &mut Self {
        self.remote_nodes.push(peer);
        self
    }

    /// Builds a node using the default backend connection manager.
    pub async fn build<const CLIENTS: usize>(
        self,
        clients: [BoxedClient; CLIENTS],
    ) -> Result<Node, anyhow::Error> {
        let event_register = {
            #[cfg(feature = "trace-ot")]
            {
                use super::tracing::{CombinedRegister, OTEventRegister};
                CombinedRegister::new([
                    Box::new(EventRegister::new(self.config.event_log())),
                    Box::new(OTEventRegister::new()),
                ])
            }
            #[cfg(not(feature = "trace-ot"))]
            {
                EventRegister::new(self.config.event_log())
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
        Ok(Node(node))
    }

    pub fn get_peer_id(&self) -> Option<PeerId> {
        match (self.public_ip, self.public_port) {
            (Some(ip), Some(port)) => Some(PeerId::new(
                SocketAddr::new(ip, port),
                self.key_pair.public().clone(),
            )),
            _ => None,
        }
    }

    /// Returns all specified gateways for this peer. Returns an error if the peer is not a gateway
    /// and no gateways are specified.
    fn get_gateways(&self) -> Result<Vec<PeerKeyLocation>, anyhow::Error> {
        let gateways: Vec<PeerKeyLocation> = self
            .remote_nodes
            .iter()
            .map(|node| PeerKeyLocation {
                peer: node.peer_id.clone(),
                location: Some(node.location),
            })
            .collect();

        if (self.local_ip.is_none() || self.local_port.is_none()) && gateways.is_empty() {
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

/// Process client events.
async fn client_event_handling<ClientEv>(
    op_manager: Arc<OpManager>,
    mut client_events: ClientEv,
    mut client_responses: ClientResponsesReceiver,
    node_controller: tokio::sync::mpsc::Sender<NodeEvent>,
) where
    ClientEv: ClientEventsProxy + Send + 'static,
{
    loop {
        tokio::select! {
            client_request = client_events.recv() => {
                let req = match client_request {
                    Ok(request) => {
                        tracing::debug!(%request, "got client request event");
                        request
                    }
                    Err(error) if matches!(error.kind(), ErrorKind::Shutdown) => {
                        node_controller.send(NodeEvent::Disconnect { cause: None }).await.ok();
                        break;
                    }
                    Err(error) => {
                        tracing::debug!(%error, "client error");
                        continue;
                    }
                };
                // fixme: only allow in certain modes (e.g. while testing)
                if let ClientRequest::Disconnect { cause } = &*req.request {
                    node_controller.send(NodeEvent::Disconnect { cause: cause.clone() }).await.ok();
                    break;
                }
                process_open_request(req, op_manager.clone()).await;
            }
            res = client_responses.recv() => {
                if let Some((cli_id, res)) = res {
                    if let Ok(result) = &res {
                        tracing::debug!(%result, "sending client response");
                    }
                    if let Err(err) = client_events.send(cli_id, res).await {
                        tracing::debug!("channel closed: {err}");
                        break;
                    }
                }
            }
        }
    }
}

#[inline]
async fn process_open_request(request: OpenRequest<'static>, op_manager: Arc<OpManager>) {
    // this will indirectly start actions on the local contract executor
    let fut = async move {
        let client_id = request.client_id;

        // fixme: communicate back errors in this loop to the client somehow
        match *request.request {
            ClientRequest::ContractOp(ops) => match ops {
                ContractRequest::Put {
                    state,
                    contract,
                    related_contracts,
                } => {
                    let peer_id = op_manager
                        .ring
                        .get_peer_key()
                        .expect("Peer id not found at put op, it should be set");
                    // Initialize a put op.
                    tracing::debug!(
                        this_peer = %peer_id,
                        "Received put from user event",
                    );
                    let op = put::start_op(
                        contract,
                        related_contracts,
                        state,
                        op_manager.ring.max_hops_to_live,
                    );
                    let _ = op_manager
                        .ch_outbound
                        .waiting_for_transaction_result(op.id, client_id)
                        .await;
                    if let Err(err) = put::request_put(&op_manager, op).await {
                        tracing::error!("{}", err);
                    }
                }
                ContractRequest::Update { key, data } => {
                    let peer_id = op_manager
                        .ring
                        .get_peer_key()
                        .expect("Peer id not found at update op, it should be set");
                    tracing::debug!(
                        this_peer = %peer_id,
                        "Received update from user event",
                    );
                    let state = match data {
                        freenet_stdlib::prelude::UpdateData::State(s) => s,
                        _ => {
                            unreachable!();
                        }
                    };

                    let wrapped_state = WrappedState::from(state.into_bytes());

                    let related_contracts = RelatedContracts::default();

                    let op = update::start_op(key, wrapped_state, related_contracts);

                    let _ = op_manager
                        .ch_outbound
                        .waiting_for_transaction_result(op.id, client_id)
                        .await;

                    if let Err(err) = update::request_update(&op_manager, op).await {
                        tracing::error!("request update error {}", err)
                    }
                }
                ContractRequest::Get {
                    key,
                    fetch_contract: contract,
                } => {
                    let peer_id = op_manager
                        .ring
                        .get_peer_key()
                        .expect("Peer id not found at get op, it should be set");
                    // Initialize a get op.
                    tracing::debug!(
                        this_peer = %peer_id,
                        "Received get from user event",
                    );
                    let op = get::start_op(key, contract);
                    let _ = op_manager
                        .ch_outbound
                        .waiting_for_transaction_result(op.id, client_id)
                        .await;
                    if let Err(err) = get::request_get(&op_manager, op).await {
                        tracing::error!("{}", err);
                    }
                }
                ContractRequest::Subscribe { key, .. } => {
                    subscribe(op_manager, key, Some(client_id)).await;
                }
                _ => {
                    tracing::error!("Op not supported");
                }
            },
            ClientRequest::DelegateOp(_op) => todo!("FIXME: delegate op"),
            ClientRequest::Disconnect { .. } => unreachable!(),
            _ => {
                tracing::error!("Op not supported");
            }
        }
    };
    GlobalExecutor::spawn(fut.instrument(
        tracing::info_span!(parent: tracing::Span::current(), "process_client_request"),
    ));
}

#[allow(unused)]
macro_rules! log_handling_msg {
    ($op:expr, $id:expr, $op_manager:ident) => {
        tracing::debug!(
            tx = %$id,
            this_peer = %$op_manager.ring.peer_key,
            concat!("Handling ", $op, " request"),
        );
    };
}

async fn report_result(
    tx: Option<Transaction>,
    op_result: Result<Option<OpEnum>, OpError>,
    op_manager: &OpManager,
    executor_callback: Option<ExecutorToEventLoopChannel<Callback>>,
    client_req_handler_callback: Option<(ClientId, ClientResponsesSender)>,
    event_listener: &mut dyn NetEventRegister,
) {
    match op_result {
        Ok(Some(op_res)) => {
            if let Some((client_id, cb)) = client_req_handler_callback {
                let _ = cb.send((client_id, op_res.to_host_result()));
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
            tracing::debug!(?tx, "No operation result found, not sending response");
        }
        Err(err) => {
            // just mark the operation as completed so no redundant messages are processed for this transaction anymore
            if let Some(tx) = tx {
                op_manager.completed(tx);
            }
            #[cfg(any(debug_assertions, test))]
            {
                let OpError::InvalidStateTransition { tx, state, trace } = err else {
                    tracing::error!("Finished transaction with error: {err}");
                    return;
                };
                // todo: this can be improved once std::backtrace::Backtrace::frames is stabilized
                let trace = format!("{trace}");
                let mut tr_lines = trace.lines();
                let trace = tr_lines
                    .nth(2)
                    .map(|second_trace| {
                        let second_trace_lines =
                            [second_trace, tr_lines.next().unwrap_or_default()];
                        second_trace_lines.join("\n")
                    })
                    .unwrap_or_default();
                let peer = &op_manager.ring.get_peer_key().expect("Peer key not found");
                let log = format!(
                    "Transaction ({tx} @ {peer}) error trace:\n {trace} \nstate:\n {state:?}\n"
                );
                std::io::stderr().write_all(log.as_bytes()).unwrap();
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

async fn process_message<CB>(
    msg: NetMessage,
    op_manager: Arc<OpManager>,
    conn_manager: CB,
    event_listener: Box<dyn NetEventRegister>,
    executor_callback: Option<ExecutorToEventLoopChannel<crate::contract::Callback>>,
    client_req_handler_callback: Option<ClientResponsesSender>,
    client_id: Option<ClientId>,
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
                client_req_handler_callback,
                client_id,
            )
            .await
        }
    }
}

#[allow(clippy::too_many_arguments)]
async fn process_message_v1<CB>(
    tx: Option<Transaction>,
    msg: NetMessageV1,
    op_manager: Arc<OpManager>,
    mut conn_manager: CB,
    mut event_listener: Box<dyn NetEventRegister>,
    executor_callback: Option<ExecutorToEventLoopChannel<crate::contract::Callback>>,
    client_req_handler_callback: Option<ClientResponsesSender>,
    client_id: Option<ClientId>,
) where
    CB: NetworkBridge,
{
    let cli_req = client_id.zip(client_req_handler_callback);
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
                    peer = ?op_manager.ring.get_peer_key(),
                    transaction = %msg.id(),
                    tx_type = %msg.id().transaction_type()
                );
                let op_result =
                    handle_op_request::<connect::ConnectOp, _>(&op_manager, &mut conn_manager, op)
                        .instrument(span)
                        .await;
                handle_op_not_available!(op_result);
                match &op_result {
                    Ok(Some(OpEnum::Connect(op))) => {
                        tracing::info!(?op, "Connect operation started");
                    }
                    Ok(None) => {
                        tracing::info!("Connect operation not started");
                    }
                    Err(err) => {
                        tracing::error!(%err, "Connect operation failed");
                    }
                    _ => {}
                }
                return report_result(
                    tx,
                    op_result,
                    &op_manager,
                    executor_callback,
                    cli_req,
                    &mut *event_listener,
                )
                .await;
            }
            NetMessageV1::Put(ref op) => {
                let op_result =
                    handle_op_request::<put::PutOp, _>(&op_manager, &mut conn_manager, op).await;
                handle_op_not_available!(op_result);
                return report_result(
                    tx,
                    op_result,
                    &op_manager,
                    executor_callback,
                    cli_req,
                    &mut *event_listener,
                )
                .await;
            }
            NetMessageV1::Get(ref op) => {
                let op_result =
                    handle_op_request::<get::GetOp, _>(&op_manager, &mut conn_manager, op).await;
                handle_op_not_available!(op_result);
                return report_result(
                    tx,
                    op_result,
                    &op_manager,
                    executor_callback,
                    cli_req,
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
                handle_op_not_available!(op_result);
                return report_result(
                    tx,
                    op_result,
                    &op_manager,
                    executor_callback,
                    cli_req,
                    &mut *event_listener,
                )
                .await;
            }
            NetMessageV1::Update(ref op) => {
                let op_result =
                    handle_op_request::<update::UpdateOp, _>(&op_manager, &mut conn_manager, op)
                        .await;
                handle_op_not_available!(op_result);
                return report_result(
                    tx,
                    op_result,
                    &op_manager,
                    executor_callback,
                    cli_req,
                    &mut *event_listener,
                )
                .await;
            }
            NetMessageV1::Unsubscribed { ref key, .. } => {
                subscribe(op_manager, key.clone(), None).await;
                break;
            }
            _ => break, // Exit the loop if no applicable message type is found
        }
    }
}

/// Attempts to subscribe to a contract
async fn subscribe(op_manager: Arc<OpManager>, key: ContractKey, client_id: Option<ClientId>) {
    const TIMEOUT: Duration = Duration::from_secs(30);
    let mut missing_contract = false;
    let timeout = tokio::time::timeout(TIMEOUT, async {
        // Initialize a subscribe op.
        loop {
            let op = subscribe::start_op(key.clone());
            if let Some(client_id) = client_id {
                let _ = op_manager
                    .ch_outbound
                    .waiting_for_transaction_result(op.id, client_id)
                    .await;
            }
            match subscribe::request_subscribe(&op_manager, op).await {
                Err(OpError::ContractError(ContractError::ContractNotFound(key)))
                    if !missing_contract =>
                {
                    tracing::info!(%key, "Trying to subscribe to a contract not present, requesting it first");
                    missing_contract = true;
                    let get_op = get::start_op(key.clone(), true);
                    if let Err(error) = get::request_get(&op_manager, get_op).await {
                        tracing::error!(%key, %error, "Failed getting the contract while previously trying to subscribe; bailing");
                        break Err(error);
                    }
                    continue;
                }
                Err(OpError::ContractError(ContractError::ContractNotFound(_))) => {
                    tracing::warn!("Still waiting for {key} contract");
                    tokio::time::sleep(Duration::from_secs(2)).await
                }
                Err(err) => {
                    tracing::error!("{}", err);
                    break Err(err);
                }
                Ok(()) => {
                    if missing_contract {
                        tracing::debug!(%key,
                            "Got back the missing contract while subscribing"
                        );
                    }
                    tracing::debug!(%key, "Starting subscribe request");
                    break Ok(());
                }
            }
        }
    });
    match timeout.await {
        Err(_) => {
            tracing::error!(%key, "Timeout while waiting for contract to start subscription");
        }
        Ok(Err(error)) => {
            tracing::error!(%key, %error, "Error while subscribing to contract");
        }
        Ok(Ok(_)) => {
            tracing::debug!(%key, "Started subscription to contract");
        }
    }
}

async fn handle_aborted_op<CM>(
    tx: Transaction,
    this_peer_pub_key: TransportPublicKey,
    op_manager: &OpManager,
    conn_manager: &mut CM,
    gateways: &[PeerKeyLocation],
) -> Result<(), OpError>
where
    CM: NetworkBridge + Send,
{
    use crate::util::IterExt;
    if let TransactionType::Connect = tx.transaction_type() {
        // attempt to establish a connection failed, this could be a fatal error since the node
        // is useless without connecting to the network, we will retry with exponential backoff
        // if necessary
        match op_manager.pop(&tx) {
            // only keep attempting to connect if the node hasn't got enough connections yet
            Ok(Some(OpEnum::Connect(op)))
                if op.has_backoff()
                    && op_manager.ring.open_connections() < op_manager.ring.min_connections =>
            {
                let ConnectOp {
                    gateway, backoff, ..
                } = *op;
                if let Some(gateway) = gateway {
                    tracing::warn!("Retry connecting to gateway {}", gateway.peer);
                    connect::join_ring_request(
                        backoff,
                        this_peer_pub_key,
                        &gateway,
                        op_manager,
                        conn_manager,
                    )
                    .await?;
                }
            }
            Ok(Some(OpEnum::Connect(_))) => {
                // if no connections were achieved just fail
                if op_manager.ring.open_connections() == 0 && op_manager.ring.is_gateway() {
                    tracing::warn!("Retrying joining the ring with an other gateway");
                    if let Some(gateway) = gateways.iter().shuffle().next() {
                        connect::join_ring_request(
                            None,
                            this_peer_pub_key,
                            gateway,
                            op_manager,
                            conn_manager,
                        )
                        .await?
                    }
                }
            }
            _ => {}
        }
    }
    Ok(())
}

/*
- Cuando es un gateway: se define desde el inicio del nodo
- Cuando es un peer regular: se define en el momento de la conexión con el gateway
*/
#[derive(Serialize, Deserialize, PartialEq, Eq, Hash, Clone)]
pub struct PeerId {
    pub addr: SocketAddr,
    pub pub_key: TransportPublicKey,
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

// impl FromStr for PeerId {
//     type Err = anyhow::Error;
//     fn from_str(s: &str) -> Result<Self, Self::Err> {
//         Ok(Self {
//             addr: s.parse()?,
//             pub_key: TransportKeypair::new().public,
//         })
//     }
// }

impl PeerId {
    pub fn new(addr: SocketAddr, pub_key: TransportPublicKey) -> Self {
        Self { addr, pub_key }
    }

    pub fn addr(&self) -> SocketAddr {
        self.addr
    }

    pub fn pub_key(&self) -> &TransportPublicKey {
        &self.pub_key
    }
}

#[cfg(test)]
impl<'a> arbitrary::Arbitrary<'a> for PeerId {
    fn arbitrary(u: &mut arbitrary::Unstructured<'a>) -> arbitrary::Result<Self> {
        let addr: ([u8; 4], u16) = u.arbitrary()?;
        let pub_key = TransportKeypair::new().public().clone(); // TODO: impl arbitrary for TransportPublicKey
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
        rand::thread_rng().fill(&mut addr[..]);
        let port = crate::util::get_free_port().unwrap();
        let pub_key = TransportKeypair::new().public().clone();
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
        write!(f, "{:?}", self.addr)
    }
}
