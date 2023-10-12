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

use freenet_stdlib::client_api::{ClientRequest, ContractRequest};
use libp2p::{identity, multiaddr::Protocol, Multiaddr, PeerId};

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
    message::{InnerMessage, Message, Transaction, TransactionType, TxType},
    operations::{
        get,
        join_ring::{self, JoinRingMsg, JoinRingOp},
        put, subscribe, OpEnum, OpError, OpOutcome,
    },
    ring::{Location, PeerKeyLocation},
    router::{RouteEvent, RouteOutcome},
    util::{ExponentialBackoff, IterExt},
};

use crate::operations::handle_op_request;
pub(crate) use conn_manager::{ConnectionBridge, ConnectionError};
#[cfg(test)]
pub(crate) use event_log::test_utils::TestEventListener;
pub(crate) use event_log::{EventLogRegister, EventRegister};
pub(crate) use op_state::OpManager;

mod conn_manager;
mod event_log;
#[cfg(test)]
mod in_memory_impl;
mod op_state;
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
        //TODO: Initialize tracer
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
        let local_key = if let Some(key) = &Config::get_static_conf().local_peer_keypair {
            key.clone()
        } else {
            identity::Keypair::generate_ed25519()
        };
        NodeBuilder {
            local_key,
            remote_nodes: Vec::with_capacity(1),
            local_ip: None,
            local_port: None,
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
    CM: ConnectionBridge + Send + Sync,
{
    let tx_id = Transaction::new(<JoinRingMsg as TxType>::tx_type_id(), &peer_key);
    let mut op =
        join_ring::initial_request(peer_key, *gateway, op_storage.ring.max_hops_to_live, tx_id);
    if let Some(mut backoff) = backoff {
        // backoff to retry later in case it failed
        tracing::warn!(
            "Performing a new join attempt, attempt number: {}",
            backoff.retries()
        );
        if backoff.sleep_async().await.is_none() {
            tracing::error!("Max number of retries reached");
            return Err(OpError::MaxRetriesExceeded(
                tx_id,
                format!("{:?}", tx_id.tx_type()),
            ));
        }
        op.backoff = Some(backoff);
    }
    join_ring::join_ring_request(tx_id, op_storage, conn_manager, op).await?;
    Ok(())
}

/// Process client events.
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
                    Ok(req) => req,
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
                    if let Err(err) = client_events.send(cli_id, res).await {
                        tracing::error!("channel closed: {err}");
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
    let op_storage_cp = op_storage.clone();
    let fut = async move {
        let client_id = request.client_id;
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
                        &op_storage_cp.ring.peer_key
                    );
                    let op = put::start_op(
                        contract,
                        state,
                        op_storage_cp.ring.max_hops_to_live,
                        &op_storage_cp.ring.peer_key,
                    );
                    if let Err(err) = put::request_put(&op_storage_cp, op, Some(client_id)).await {
                        tracing::error!("{}", err);
                    }
                    todo!("use `related_contracts`: {related_contracts:?}")
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
                        &op_storage_cp.ring.peer_key
                    );
                    let op = get::start_op(key, contract, &op_storage_cp.ring.peer_key);
                    if let Err(err) = get::request_get(&op_storage_cp, op, Some(client_id)).await {
                        tracing::error!("{}", err);
                    }
                }
                ContractRequest::Subscribe { key, .. } => {
                    // Initialize a subscribe op.
                    loop {
                        // FIXME: this will block the event loop until the subscribe op succeeds
                        //        instead the op should be deferred for later execution
                        let op = subscribe::start_op(key.clone(), &op_storage_cp.ring.peer_key);
                        match subscribe::request_subscribe(&op_storage_cp, op, Some(client_id))
                            .await
                        {
                            Err(OpError::ContractError(ContractError::ContractNotFound(key))) => {
                                tracing::warn!("Trying to subscribe to a contract not present: {}, requesting it first", key);
                                let get_op =
                                    get::start_op(key.clone(), true, &op_storage_cp.ring.peer_key);
                                if let Err(err) =
                                    get::request_get(&op_storage_cp, get_op, Some(client_id)).await
                                {
                                    tracing::error!("Failed getting the contract `{}` while previously trying to subscribe; bailing: {}", key, err);
                                    tokio::time::sleep(Duration::from_secs(5)).await;
                                }
                            }
                            Err(err) => {
                                tracing::error!("{}", err);
                                break;
                            }
                            Ok(()) => break,
                        }
                    }
                    todo!()
                }
                _ => {
                    tracing::error!("op not supported");
                }
            },
            ClientRequest::DelegateOp(_op) => todo!("FIXME: delegate op"),
            ClientRequest::Disconnect { .. } => unreachable!(),
            _ => {
                tracing::error!("op not supported");
            }
        }
    };

    GlobalExecutor::spawn(fut);
}

macro_rules! log_handling_msg {
    ($op:expr, $id:expr, $op_storage:ident) => {
        tracing::debug!(
            concat!("Handling ", $op, " get request @ {} (tx: {})"),
            $op_storage.ring.peer_key,
            $id
        );
    };
}

async fn report_result(
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
                    if let Err(err) = event_listener
                        .event_received(EventLog::route_event(op_res.id(), op_storage, &event))
                        .await
                    {
                        tracing::warn!("failed logging event: {err}");
                    }
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
            tracing::debug!("Finished tx w/ error: {}", err)
        }
    }
}

async fn process_message<CB>(
    msg: Result<Message, ConnectionError>,
    op_storage: Arc<OpManager>,
    mut conn_manager: CB,
    mut event_listener: Box<dyn EventLogRegister>,
    executor_callback: Option<ExecutorToEventLoopChannel<NetworkEventListenerHalve>>,
    client_req_handler_callback: Option<ClientResponsesSender>,
    client_id: Option<ClientId>,
) where
    CB: ConnectionBridge,
{
    let cli_req = client_id.zip(client_req_handler_callback);
    match msg {
        Ok(msg) => {
            if let Err(err) = event_listener
                .event_received(EventLog::from_msg(&msg, &op_storage))
                .await
            {
                tracing::warn!("failed logging event: {err}");
            }
            match msg {
                Message::JoinRing(op) => {
                    log_handling_msg!("join", op.id(), op_storage);
                    let op_result = handle_op_request::<join_ring::JoinRingOp, _>(
                        &op_storage,
                        &mut conn_manager,
                        op,
                        client_id,
                    )
                    .await;
                    report_result(
                        op_result,
                        &op_storage,
                        executor_callback,
                        cli_req,
                        &mut event_listener,
                    )
                    .await;
                }
                Message::Put(op) => {
                    log_handling_msg!("put", *op.id(), op_storage);
                    let op_result = handle_op_request::<put::PutOp, _>(
                        &op_storage,
                        &mut conn_manager,
                        op,
                        client_id,
                    )
                    .await;
                    report_result(
                        op_result,
                        &op_storage,
                        executor_callback,
                        cli_req,
                        &mut event_listener,
                    )
                    .await;
                }
                Message::Get(op) => {
                    log_handling_msg!("get", op.id(), op_storage);
                    let op_result = handle_op_request::<get::GetOp, _>(
                        &op_storage,
                        &mut conn_manager,
                        op,
                        client_id,
                    )
                    .await;
                    report_result(
                        op_result,
                        &op_storage,
                        executor_callback,
                        cli_req,
                        &mut event_listener,
                    )
                    .await;
                }
                Message::Subscribe(op) => {
                    log_handling_msg!("subscribe", op.id(), op_storage);
                    let op_result = handle_op_request::<subscribe::SubscribeOp, _>(
                        &op_storage,
                        &mut conn_manager,
                        op,
                        client_id,
                    )
                    .await;
                    report_result(
                        op_result,
                        &op_storage,
                        executor_callback,
                        cli_req,
                        &mut event_listener,
                    )
                    .await;
                }
                _ => {}
            }
        }
        Err(err) => {
            report_result(
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
    gateways: impl Iterator<Item = &PeerKeyLocation>,
    op_storage: &OpManager,
    conn_manager: &mut CM,
) -> Result<(), OpError>
where
    CM: ConnectionBridge + Send + Sync,
{
    tracing::warn!("Failed tx `{}`, potentially attempting a retry", tx);
    match tx.tx_type() {
        TransactionType::JoinRing => {
            const MSG: &str = "Fatal error: unable to connect to the network";
            // the attempt to join the network failed, this could be a fatal error since the node
            // is useless without connecting to the network, we will retry with exponential backoff
            match op_storage.pop(&tx) {
                Some(OpEnum::JoinRing(op)) if op.has_backoff() => {
                    if let JoinRingOp {
                        backoff: Some(backoff),
                        gateway,
                        ..
                    } = *op
                    {
                        if cfg!(test) {
                            join_ring_request(None, peer_key, &gateway, op_storage, conn_manager)
                                .await?;
                        } else {
                            join_ring_request(
                                Some(backoff),
                                peer_key,
                                &gateway,
                                op_storage,
                                conn_manager,
                            )
                            .await?;
                        }
                    }
                }
                None | Some(OpEnum::JoinRing(_)) => {
                    let rand_gw = gateways
                        .shuffle()
                        .take(1)
                        .next()
                        .expect("at least one gateway");
                    if !cfg!(test) {
                        tracing::error!("{}", MSG);
                    } else {
                        tracing::debug!("{}", MSG);
                    }
                    join_ring_request(None, peer_key, rand_gw, op_storage, conn_manager).await?;
                }
                _ => {}
            }
        }
        _ => unreachable!(),
    }
    Ok(())
}

#[derive(Debug, PartialEq, Eq, Hash, Clone, Copy, PartialOrd, Ord)]
pub struct PeerKey(PeerId);

impl PeerKey {
    #[cfg(test)]
    pub fn random() -> Self {
        use libp2p::identity::Keypair;
        PeerKey::from(Keypair::generate_ed25519().public())
    }

    pub fn to_bytes(self) -> Vec<u8> {
        self.0.to_bytes()
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
