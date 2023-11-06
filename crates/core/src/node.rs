//! The main node data type which encapsulates all the behaviour for maintaining a connection
//! and performing operations within the network.
//!
//! # Implementations
//! Node comes with different underlying implementations that can be used upon construction.
//! Those implementations are:
//! - libp2p: all the connection is handled by libp2p.
//! - In memory: a simplifying node used for emulation purposes mainly.

use std::{
    fmt::Display,
    net::{IpAddr, Ipv4Addr},
    path::PathBuf,
    sync::Arc,
    time::Duration,
};

use either::Either;
use freenet_stdlib::client_api::{ClientRequest, ContractRequest};
use libp2p::{identity, multiaddr::Protocol, Multiaddr, PeerId};
use tracing::Instrument;

#[cfg(test)]
use self::in_memory_impl::NodeInMemory;
use self::{event_log::EventLog, p2p_impl::NodeP2P};
use crate::{
    client_events::{BoxedClient, ClientEventsProxy, ClientId, OpenRequest},
    config::Config,
    config::GlobalExecutor,
    contract::{
        ClientResponses, ClientResponsesSender, ContractError, ExecutorToEventLoopChannel,
        NetworkContractHandler, NetworkEventListenerHalve, OperationMode,
    },
    message::{Message, Transaction, TransactionType},
    operations::{
        connect::{self, ConnectMsg, ConnectOp},
        get, put, subscribe, OpEnum, OpError, OpOutcome,
    },
    ring::{Location, PeerKeyLocation},
    router::{RouteEvent, RouteOutcome},
    util::ExponentialBackoff,
};

use crate::operations::handle_op_request;
pub(crate) use event_log::{EventLogRegister, EventRegister};
pub(crate) use network_bridge::{ConnectionError, EventLoopNotificationsSender, NetworkBridge};
pub(crate) use op_state_manager::{OpManager, OpNotAvailable};

mod event_log;
#[cfg(test)]
mod in_memory_impl;
mod network_bridge;
mod op_state_manager;
mod p2p_impl;
#[cfg(test)]
pub(crate) mod tests;

#[derive(clap::Parser, Clone, Debug)]
pub struct NodeConfig {
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
    pub async fn run(self) -> Result<(), Box<dyn std::error::Error + Send + Sync + 'static>> {
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
pub struct NodeBuilder<const CLIENTS: usize> {
    /// local peer private key in
    pub(crate) local_key: identity::Keypair,
    // optional local info, in case this is an initial bootstrap node
    /// IP to bind to the listener
    pub(crate) local_ip: Option<IpAddr>,
    /// socket port to bind to the listener
    pub(crate) local_port: Option<u16>,
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
    pub(crate) clients: [BoxedClient; CLIENTS],
}

impl<const CLIENTS: usize> NodeBuilder<CLIENTS> {
    pub fn new(clients: [BoxedClient; CLIENTS]) -> NodeBuilder<CLIENTS> {
        let local_key = if let Some(key) = &Config::conf().local_peer_keypair {
            key.clone()
        } else {
            identity::Keypair::generate_ed25519()
        };
        NodeBuilder {
            local_key,
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
            clients,
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
    pub fn with_key(&mut self, key: identity::Keypair) -> &mut Self {
        self.local_key = key;
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
    pub async fn build(self, config: NodeConfig) -> Result<Node, anyhow::Error> {
        let event_log = event_log::EventRegister::new();
        let node = NodeP2P::build::<NetworkContractHandler, CLIENTS, event_log::EventRegister>(
            self, event_log, config,
        )
        .await?;
        Ok(Node(node))
    }

    /// Returns all specified gateways for this peer. Returns an error if the peer is not a gateway
    /// and no gateways are specified.
    fn get_gateways(&self) -> Result<Vec<PeerKeyLocation>, anyhow::Error> {
        let peer = PeerKey::from(self.local_key.public());
        let gateways: Vec<_> = self
            .remote_nodes
            .iter()
            .filter_map(|node| {
                if node.addr.is_some() {
                    Some(PeerKeyLocation {
                        peer: PeerKey::from(node.identifier),
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

/// Gateway node to bootstrap the network.
#[derive(Clone)]
pub struct InitPeerNode {
    addr: Option<Multiaddr>,
    identifier: PeerId,
    location: Location,
}

impl InitPeerNode {
    pub fn new(identifier: PeerId, location: Location) -> Self {
        Self {
            addr: None,
            identifier,
            location,
        }
    }

    /// Given a byte array decode into a PeerId data type.
    ///
    /// # Panic
    /// Will panic if is not a valid representation.
    pub fn decode_peer_id<T: AsMut<[u8]>>(mut bytes: T) -> PeerId {
        PeerId::from_public_key(
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

async fn join_ring_request<CM>(
    backoff: Option<ExponentialBackoff>,
    peer_key: PeerKey,
    gateway: &PeerKeyLocation,
    op_storage: &OpManager,
    conn_manager: &mut CM,
) -> Result<(), OpError>
where
    CM: NetworkBridge + Send + Sync,
{
    let tx_id = Transaction::new::<ConnectMsg>();
    let mut op =
        connect::initial_request(peer_key, *gateway, op_storage.ring.max_hops_to_live, tx_id);
    if let Some(mut backoff) = backoff {
        // backoff to retry later in case it failed
        tracing::warn!("Performing a new join, attempt {}", backoff.retries() + 1);
        if backoff.sleep().await.is_none() {
            tracing::error!("Max number of retries reached");
            return Err(OpError::MaxRetriesExceeded(tx_id, tx_id.transaction_type()));
        }
        op.backoff = Some(backoff);
    }
    connect::connect_request(tx_id, op_storage, conn_manager, op).await?;
    Ok(())
}

/// Process client events.
#[tracing::instrument(skip_all)]
async fn client_event_handling<ClientEv>(
    op_storage: Arc<OpManager>,
    mut client_events: ClientEv,
    mut client_responses: ClientResponses,
) where
    ClientEv: ClientEventsProxy + Send + Sync + 'static,
{
    loop {
        tokio::select! {
            client_request = client_events.recv() => {
                let req = match client_request {
                    Ok(req) => {
                        tracing::debug!(%req, "got client request event");
                        req
                    }
                    Err(err) => {
                        tracing::debug!(error = %err, "client error");
                        continue;
                    }
                };
                if let ClientRequest::Disconnect { .. } = &*req.request {
                    // todo: notify executor of disconnect
                    continue;
                }
                process_open_request(req, op_storage.clone()).await;
            }
            res = client_responses.recv() => {
                if let Some((cli_id, res)) = res {
                    if let Ok(res) = &res {
                        tracing::debug!(%res, "sending client response");
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
async fn process_open_request(request: OpenRequest<'static>, op_storage: Arc<OpManager>) {
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
                        "Received put from user event @ {}",
                        &op_storage.ring.peer_key
                    );
                    let op = put::start_op(
                        contract,
                        related_contracts,
                        state,
                        op_storage.ring.max_hops_to_live,
                    );
                    if let Err(err) = put::request_put(&op_storage, op, Some(client_id)).await {
                        tracing::error!("{}", err);
                    }
                }
                ContractRequest::Update {
                    key: _key,
                    data: _delta,
                } => {
                    todo!()
                }
                ContractRequest::Get {
                    key,
                    fetch_contract: contract,
                } => {
                    // Initialize a get op.
                    tracing::debug!(
                        "Received get from user event @ {}",
                        &op_storage.ring.peer_key
                    );
                    let op = get::start_op(key, contract);
                    if let Err(err) = get::request_get(&op_storage, op, Some(client_id)).await {
                        tracing::error!("{}", err);
                    }
                }
                ContractRequest::Subscribe { key, .. } => {
                    // Initialize a subscribe op.
                    loop {
                        let op = subscribe::start_op(key.clone());
                        match subscribe::request_subscribe(&op_storage, op, Some(client_id)).await {
                            Err(OpError::ContractError(ContractError::ContractNotFound(key)))
                                if !missing_contract =>
                            {
                                tracing::info!("Trying to subscribe to a contract not present: {key}, requesting it first");
                                missing_contract = true;
                                let get_op = get::start_op(key.clone(), true);
                                if let Err(err) =
                                    get::request_get(&op_storage, get_op, Some(client_id)).await
                                {
                                    tracing::error!("Failed getting the contract `{key}` while previously trying to subscribe; bailing: {err}");
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
                                    tracing::debug!(
                                        "Got back the missing contract ({key}) while subscribing"
                                    );
                                }
                                tracing::debug!("Starting subscribe request to {key}");
                                break;
                            }
                        }
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
    GlobalExecutor::spawn(fut.instrument(tracing::span!(
        tracing::Level::INFO,
        "process_client_request"
    )));
}

#[allow(unused)]
macro_rules! log_handling_msg {
    ($op:expr, $id:expr, $op_storage:ident) => {
        tracing::debug!(
            tx = %$id,
            concat!("Handling ", $op, " request @ {}"),
            $op_storage.ring.peer_key,
        );
    };
}

async fn report_result(
    tx: Option<Transaction>,
    op_result: Result<Option<OpEnum>, OpError>,
    op_storage: &OpManager,
    executor_callback: Option<ExecutorToEventLoopChannel<NetworkEventListenerHalve>>,
    client_req_handler_callback: Option<(ClientId, ClientResponsesSender)>,
    event_listener: &mut Box<dyn EventLogRegister>,
) {
    match op_result {
        Ok(Some(op_res)) => {
            if let Some((client_id, cb)) = client_req_handler_callback {
                let _ = cb.send((client_id, op_res.to_host_result(client_id)));
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
                        .register_events(Either::Left(EventLog::route_event(
                            op_res.id(),
                            op_storage,
                            &event,
                        )))
                        .await;
                    op_storage.ring.routing_finished(event);
                }
                // todo: handle failures, need to track timeouts and other potential failures
                // OpOutcome::ContractOpFailure {
                //     target_peer: Some(target_peer),
                //     contract_location,
                // } => {
                //     op_storage.ring.routing_finished(RouteEvent {
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
                op_storage.completed(tx);
            }
            #[cfg(debug_assertions)]
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
                tracing::error!(%tx, ?state, "Wrong state");
                eprintln!("{trace}");
            }
            #[cfg(not(debug_assertions))]
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

#[tracing::instrument(name = "process_network_message", skip_all)]
async fn process_message<CB>(
    msg: Result<Message, ConnectionError>,
    op_storage: Arc<OpManager>,
    mut conn_manager: CB,
    mut event_listener: Box<dyn EventLogRegister>,
    executor_callback: Option<ExecutorToEventLoopChannel<NetworkEventListenerHalve>>,
    client_req_handler_callback: Option<ClientResponsesSender>,
    client_id: Option<ClientId>,
) where
    CB: NetworkBridge,
{
    let cli_req = client_id.zip(client_req_handler_callback);
    match msg {
        Ok(msg) => {
            let tx = Some(*msg.id());
            event_listener
                .register_events(EventLog::from_inbound_msg(&msg, &op_storage))
                .await;
            loop {
                match &msg {
                    Message::Connect(op) => {
                        // log_handling_msg!("join", op.id(), op_storage);
                        let op_result = handle_op_request::<connect::ConnectOp, _>(
                            &op_storage,
                            &mut conn_manager,
                            op,
                            client_id,
                        )
                        .await;
                        handle_op_not_available!(op_result);
                        break report_result(
                            tx,
                            op_result,
                            &op_storage,
                            executor_callback,
                            cli_req,
                            &mut event_listener,
                        )
                        .await;
                    }
                    Message::Put(op) => {
                        // log_handling_msg!("put", *op.id(), op_storage);
                        let op_result = handle_op_request::<put::PutOp, _>(
                            &op_storage,
                            &mut conn_manager,
                            op,
                            client_id,
                        )
                        .await;
                        handle_op_not_available!(op_result);
                        break report_result(
                            tx,
                            op_result,
                            &op_storage,
                            executor_callback,
                            cli_req,
                            &mut event_listener,
                        )
                        .await;
                    }
                    Message::Get(op) => {
                        // log_handling_msg!("get", op.id(), op_storage);
                        let op_result = handle_op_request::<get::GetOp, _>(
                            &op_storage,
                            &mut conn_manager,
                            op,
                            client_id,
                        )
                        .await;
                        handle_op_not_available!(op_result);
                        break report_result(
                            tx,
                            op_result,
                            &op_storage,
                            executor_callback,
                            cli_req,
                            &mut event_listener,
                        )
                        .await;
                    }
                    Message::Subscribe(op) => {
                        // log_handling_msg!("subscribe", op.id(), op_storage);
                        let op_result = handle_op_request::<subscribe::SubscribeOp, _>(
                            &op_storage,
                            &mut conn_manager,
                            op,
                            client_id,
                        )
                        .await;
                        handle_op_not_available!(op_result);
                        break report_result(
                            tx,
                            op_result,
                            &op_storage,
                            executor_callback,
                            cli_req,
                            &mut event_listener,
                        )
                        .await;
                    }
                    _ => break,
                }
            }
        }
        Err(err) => {
            report_result(
                None,
                Err(err.into()),
                &op_storage,
                executor_callback,
                cli_req,
                &mut event_listener,
            )
            .await;
        }
    }
}

async fn handle_cancelled_op<CM>(
    tx: Transaction,
    peer_key: PeerKey,
    op_storage: &OpManager,
    conn_manager: &mut CM,
) -> Result<(), OpError>
where
    CM: NetworkBridge + Send + Sync,
{
    if let TransactionType::Connect = tx.transaction_type() {
        // the attempt to join the network failed, this could be a fatal error since the node
        // is useless without connecting to the network, we will retry with exponential backoff
        match op_storage.pop(&tx) {
            Ok(Some(OpEnum::Connect(op))) if op.has_backoff() => {
                let ConnectOp {
                    gateway, backoff, ..
                } = *op;
                if let Some(gateway) = gateway {
                    let backoff = backoff.expect("infallible");
                    tracing::warn!("Retry connecting to gateway {}", gateway.peer);
                    join_ring_request(Some(backoff), peer_key, &gateway, op_storage, conn_manager)
                        .await?;
                }
            }
            Ok(Some(OpEnum::Connect(_))) => {
                return Err(OpError::MaxRetriesExceeded(tx, tx.transaction_type()));
            }
            _ => {}
        }
    }
    Ok(())
}

#[derive(PartialEq, Eq, Hash, Clone, Copy, PartialOrd, Ord)]
pub struct PeerKey(PeerId);

#[cfg(test)]
impl<'a> arbitrary::Arbitrary<'a> for PeerKey {
    fn arbitrary(u: &mut arbitrary::Unstructured<'a>) -> arbitrary::Result<Self> {
        let data: [u8; 32] = u.arbitrary()?;
        let id =
            PeerId::from_multihash(libp2p::multihash::Multihash::wrap(0, data.as_slice()).unwrap())
                .unwrap();
        Ok(Self(id))
    }
}

impl PeerKey {
    #[cfg(test)]
    pub fn random() -> Self {
        use libp2p::identity::Keypair;
        PeerKey::from(Keypair::generate_ed25519().public())
    }

    #[cfg(test)]
    pub fn to_bytes(self) -> Vec<u8> {
        self.0.to_bytes()
    }
}

impl std::fmt::Debug for PeerKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        <Self as Display>::fmt(self, f)
    }
}

impl Display for PeerKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl From<identity::PublicKey> for PeerKey {
    fn from(val: identity::PublicKey) -> Self {
        PeerKey(PeerId::from(val))
    }
}

impl From<PeerId> for PeerKey {
    fn from(val: PeerId) -> Self {
        PeerKey(val)
    }
}

mod serialization {
    use libp2p::PeerId;
    use serde::{Deserialize, Deserializer, Serialize, Serializer};

    use super::PeerKey;

    impl Serialize for PeerKey {
        fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
        where
            S: Serializer,
        {
            serializer.serialize_bytes(&self.0.to_bytes())
        }
    }

    impl<'de> Deserialize<'de> for PeerKey {
        fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
        where
            D: Deserializer<'de>,
        {
            let bytes: Vec<u8> = Deserialize::deserialize(deserializer)?;
            Ok(PeerKey(
                PeerId::from_bytes(&bytes).expect("failed deserialization of PeerKey"),
            ))
        }
    }
}
