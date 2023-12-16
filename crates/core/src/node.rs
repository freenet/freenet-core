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
    net::{IpAddr, Ipv4Addr},
    path::PathBuf,
    str::FromStr,
    sync::Arc,
    time::Duration,
};

use either::Either;
use freenet_stdlib::client_api::{ClientRequest, ContractRequest, ErrorKind};
use libp2p::{identity, multiaddr::Protocol, Multiaddr, PeerId as Libp2pPeerId};
use serde::{Deserialize, Serialize};
use tracing::Instrument;

use self::p2p_impl::NodeP2P;
use crate::{
    client_events::{BoxedClient, ClientEventsProxy, ClientId, OpenRequest},
    config::Config,
    config::GlobalExecutor,
    contract::{
        Callback, ClientResponsesReceiver, ClientResponsesSender, ContractError,
        ExecutorToEventLoopChannel, NetworkContractHandler, OperationMode,
    },
    message::{NetMessage, NodeEvent, Transaction, TransactionType},
    operations::{
        connect::{self, ConnectOp},
        get, put, subscribe, OpEnum, OpError, OpOutcome,
    },
    ring::{Location, PeerKeyLocation},
    router::{RouteEvent, RouteOutcome},
    tracing::{EventRegister, NetEventLog, NetEventRegister},
    DynError,
};

use crate::operations::handle_op_request;
pub use network_bridge::inter_process::InterProcessConnManager;
pub(crate) use network_bridge::{ConnectionError, EventLoopNotificationsSender, NetworkBridge};

pub(crate) use op_state_manager::{OpManager, OpNotAvailable};

mod network_bridge;
mod op_state_manager;
mod p2p_impl;
pub(crate) mod testing_impl;

#[derive(clap::Parser, Clone, Debug)]
pub struct PeerCliConfig {
    /// Node operation mode.
    #[clap(value_enum, default_value_t=OperationMode::Local)]
    pub mode: OperationMode,
    /// Overrides the default data directory where Freenet contract files are stored.
    pub node_data_dir: Option<PathBuf>,

    /// Address to bind to
    #[arg(long, short, default_value_t = IpAddr::V4(Ipv4Addr::LOCALHOST))]
    pub address: IpAddr,

    /// Port to expose api on
    #[arg(long, short, default_value_t = 50509)]
    pub port: u16,
}

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
#[derive(Serialize, Deserialize)]
pub struct NodeConfig {
    /// public identifier for the peer
    pub peer_id: PeerId,
    // optional local info, in case this is an initial bootstrap node
    /// IP to bind to the listener
    pub local_ip: Option<IpAddr>,
    /// socket port to bind to the listener
    pub local_port: Option<u16>,
    /// IP dialers should connect to
    pub(crate) public_ip: Option<IpAddr>,
    /// socket port dialers should connect to
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
}

impl NodeConfig {
    pub fn new() -> NodeConfig {
        let local_key = Config::conf().local_peer_keypair.public().into();
        NodeConfig {
            peer_id: local_key,
            remote_nodes: Vec::with_capacity(1),
            local_ip: None,
            local_port: None,
            public_ip: None,
            public_port: None,
            location: None,
            max_hops_to_live: None,
            rnd_if_htl_above: None,
            max_number_conn: None,
            min_number_conn: None,
        }
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

    /// Optional identity key of this node.
    /// If not provided it will be either obtained from the configuration or freshly generated.
    pub fn with_key(&mut self, key: PeerId) -> &mut Self {
        self.peer_id = key;
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
        config: PeerCliConfig,
        clients: [BoxedClient; CLIENTS],
        private_key: identity::Keypair,
    ) -> Result<Node, anyhow::Error> {
        let event_register = {
            #[cfg(feature = "trace-ot")]
            {
                use super::tracing::{CombinedRegister, OTEventRegister};
                CombinedRegister::new([
                    Box::new(EventRegister::new()),
                    Box::new(OTEventRegister::new()),
                ])
            }
            #[cfg(not(feature = "trace-ot"))]
            {
                EventRegister::new()
            }
        };
        let node = NodeP2P::build::<NetworkContractHandler, CLIENTS, _>(
            self,
            private_key,
            clients,
            event_register,
            config,
        )
        .await?;
        Ok(Node(node))
    }

    pub fn is_gateway(&self) -> bool {
        self.local_ip.is_some() && self.local_port.is_some() && self.location.is_some()
    }

    /// Returns all specified gateways for this peer. Returns an error if the peer is not a gateway
    /// and no gateways are specified.
    fn get_gateways(&self) -> Result<Vec<PeerKeyLocation>, anyhow::Error> {
        let peer = self.peer_id;
        let gateways: Vec<_> = self
            .remote_nodes
            .iter()
            .filter_map(|node| {
                if node.addr.is_some() {
                    Some(PeerKeyLocation {
                        peer: node.identifier,
                        location: Some(node.location),
                    })
                } else {
                    None
                }
            })
            .filter(|pkloc| pkloc.peer != peer)
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

impl Default for NodeConfig {
    fn default() -> Self {
        Self::new()
    }
}

/// Gateway node to bootstrap the network.
#[derive(Clone, Serialize, Deserialize)]
pub struct InitPeerNode {
    addr: Option<Multiaddr>,
    identifier: PeerId,
    location: Location,
}

impl InitPeerNode {
    pub fn new(identifier: Libp2pPeerId, location: Location) -> Self {
        Self {
            addr: None,
            identifier: PeerId(identifier),
            location,
        }
    }

    /// Given a byte array decode into a PeerId data type.
    ///
    /// # Panic
    /// Will panic if is not a valid representation.
    pub fn decode_peer_id<T: AsMut<[u8]>>(mut bytes: T) -> Libp2pPeerId {
        Libp2pPeerId::from_public_key(
            &identity::Keypair::try_from(
                identity::ed25519::Keypair::try_from_bytes(bytes.as_mut()).unwrap(),
            )
            .unwrap()
            .public(),
        )
    }

    /// IP which will be assigned to this node.
    pub fn listening_ip<T: Into<IpAddr>>(mut self, ip: T) -> Self {
        if let Some(addr) = &mut self.addr {
            addr.push(Protocol::from(ip.into()));
        } else {
            self.addr = Some(Multiaddr::from(ip.into()));
        }
        self
    }

    /// TCP listening port (only required in case of using TCP as transport).
    /// If not specified port 7800 will be used as default.
    pub fn listening_port(mut self, port: u16) -> Self {
        if let Some(addr) = &mut self.addr {
            addr.push(Protocol::Tcp(port));
        } else {
            self.addr = Some(Multiaddr::from(Protocol::Tcp(port)));
        }
        self
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

        let mut missing_contract = false;
        match *request.request {
            ClientRequest::ContractOp(ops) => match ops {
                ContractRequest::Put {
                    state,
                    contract,
                    related_contracts,
                } => {
                    // Initialize a put op.
                    tracing::debug!(
                        this_peer = %op_manager.ring.peer_key,
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
                ContractRequest::Update {
                    key: _key,
                    data: _delta,
                } => {
                    // FIXME: perform updates
                    tracing::debug!(
                        this_peer = %op_manager.ring.peer_key,
                        "Received update from user event",
                    );
                }
                ContractRequest::Get {
                    key,
                    fetch_contract: contract,
                } => {
                    // Initialize a get op.
                    tracing::debug!(
                        this_peer = %op_manager.ring.peer_key,
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
                    const TIMEOUT: Duration = Duration::from_secs(10);
                    let timeout = tokio::time::timeout(TIMEOUT, async {
                        // Initialize a subscribe op.
                        loop {
                            let op = subscribe::start_op(key.clone());
                            let _ = op_manager
                                .ch_outbound
                                .waiting_for_transaction_result(op.id, client_id)
                                .await;
                            match subscribe::request_subscribe(&op_manager, op).await {
                                Err(OpError::ContractError(ContractError::ContractNotFound(
                                    key,
                                ))) if !missing_contract => {
                                    tracing::info!(%key, "Trying to subscribe to a contract not present, requesting it first");
                                    missing_contract = true;
                                    let get_op = get::start_op(key.clone(), true);
                                    if let Err(error) = get::request_get(&op_manager, get_op).await
                                    {
                                        tracing::error!(%key, %error, "Failed getting the contract while previously trying to subscribe; bailing");
                                        break;
                                    }
                                }
                                Err(OpError::ContractError(ContractError::ContractNotFound(_))) => {
                                    tracing::warn!("Still waiting for {key} contract");
                                    tokio::time::sleep(Duration::from_secs(2)).await
                                }
                                Err(err) => {
                                    tracing::error!("{}", err);
                                    break;
                                }
                                Ok(()) => {
                                    if missing_contract {
                                        tracing::debug!(%key,
                                            "Got back the missing contract while subscribing"
                                        );
                                    }
                                    tracing::debug!(%key, "Starting subscribe request");
                                    break;
                                }
                            }
                        }
                    });
                    if timeout.await.is_err() {
                        tracing::error!(%key, "Timeout while waiting for contract");
                    }
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
                        peer: *target_peer,
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
        Ok(None) => {}
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
                let log =
                    format!("Transaction ({tx}) error trace:\n {trace} \nstate:\n {state:?}\n");
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
                    tokio::time::sleep(Duration::from_micros(1_000)).await;
                    continue;
                }
                OpNotAvailable::Completed => return,
            }
        }
    };
}

async fn process_message<CB>(
    msg: NetMessage,
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

    let tx = Some(*msg.id());
    event_listener
        .register_events(NetEventLog::from_inbound_msg(&msg, &op_manager))
        .await;
    loop {
        match &msg {
            NetMessage::Connect(op) => {
                // log_handling_msg!("join", op.id(), op_manager);
                let op_result =
                    handle_op_request::<connect::ConnectOp, _>(&op_manager, &mut conn_manager, op)
                        .await;
                handle_op_not_available!(op_result);
                break report_result(
                    tx,
                    op_result,
                    &op_manager,
                    executor_callback,
                    cli_req,
                    &mut *event_listener,
                )
                .await;
            }
            NetMessage::Put(op) => {
                // log_handling_msg!("put", *op.id(), op_manager);
                let op_result =
                    handle_op_request::<put::PutOp, _>(&op_manager, &mut conn_manager, op).await;
                handle_op_not_available!(op_result);
                break report_result(
                    tx,
                    op_result,
                    &op_manager,
                    executor_callback,
                    cli_req,
                    &mut *event_listener,
                )
                .await;
            }
            NetMessage::Get(op) => {
                // log_handling_msg!("get", op.id(), op_manager);
                let op_result =
                    handle_op_request::<get::GetOp, _>(&op_manager, &mut conn_manager, op).await;
                handle_op_not_available!(op_result);
                break report_result(
                    tx,
                    op_result,
                    &op_manager,
                    executor_callback,
                    cli_req,
                    &mut *event_listener,
                )
                .await;
            }
            NetMessage::Subscribe(op) => {
                // log_handling_msg!("subscribe", op.id(), op_manager);
                let op_result = handle_op_request::<subscribe::SubscribeOp, _>(
                    &op_manager,
                    &mut conn_manager,
                    op,
                )
                .await;
                handle_op_not_available!(op_result);
                break report_result(
                    tx,
                    op_result,
                    &op_manager,
                    executor_callback,
                    cli_req,
                    &mut *event_listener,
                )
                .await;
            }
            _ => break,
        }
    }
}

async fn handle_aborted_op<CM>(
    tx: Transaction,
    this_peer: PeerId,
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
                        this_peer,
                        &gateway,
                        op_manager,
                        conn_manager,
                    )
                    .await?;
                }
            }
            Ok(Some(OpEnum::Connect(_))) => {
                // if no connections were achieved just fail
                if op_manager.ring.open_connections() == 0 {
                    tracing::warn!("Retrying joining the ring with an other gateway");
                    if let Some(gateway) = gateways
                        .iter()
                        .shuffle()
                        .next()
                        .filter(|p| p.peer != this_peer)
                    {
                        connect::join_ring_request(
                            None,
                            this_peer,
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

#[derive(PartialEq, Eq, Hash, Clone, Copy, PartialOrd, Ord)]
pub struct PeerId(Libp2pPeerId);

#[cfg(test)]
impl<'a> arbitrary::Arbitrary<'a> for PeerId {
    fn arbitrary(u: &mut arbitrary::Unstructured<'a>) -> arbitrary::Result<Self> {
        let data: [u8; 32] = u.arbitrary()?;
        let id = Libp2pPeerId::from_multihash(
            libp2p::multihash::Multihash::wrap(0, data.as_slice()).unwrap(),
        )
        .unwrap();
        Ok(Self(id))
    }
}

impl PeerId {
    pub fn random() -> Self {
        use libp2p::identity::Keypair;
        PeerId::from(Keypair::generate_ed25519().public())
    }

    #[cfg(test)]
    pub fn to_bytes(self) -> Vec<u8> {
        self.0.to_bytes()
    }
}

impl std::fmt::Debug for PeerId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        <Self as Display>::fmt(self, f)
    }
}

impl Display for PeerId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl From<identity::PublicKey> for PeerId {
    fn from(val: identity::PublicKey) -> Self {
        PeerId(Libp2pPeerId::from(val))
    }
}

impl From<Libp2pPeerId> for PeerId {
    fn from(val: Libp2pPeerId) -> Self {
        PeerId(val)
    }
}

impl FromStr for PeerId {
    type Err = libp2p_identity::ParseError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Ok(Self(Libp2pPeerId::from_str(s)?))
    }
}

mod serialization {
    use libp2p::PeerId as Libp2pPeerId;
    use serde::{Deserialize, Deserializer, Serialize, Serializer};

    use super::PeerId;

    impl Serialize for PeerId {
        fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
        where
            S: Serializer,
        {
            serializer.serialize_bytes(&self.0.to_bytes())
        }
    }

    impl<'de> Deserialize<'de> for PeerId {
        fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
        where
            D: Deserializer<'de>,
        {
            let bytes: Vec<u8> = Deserialize::deserialize(deserializer)?;
            Ok(PeerId(
                Libp2pPeerId::from_bytes(&bytes).expect("failed deserialization of PeerKey"),
            ))
        }
    }
}
