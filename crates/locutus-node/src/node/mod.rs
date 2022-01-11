//! The main node data type which encapsulates all the behaviour for maintaining a connection
//! and performing operations within the network.
//!
//! # Implementations
//! Node comes with different underlying implementations that can be used upon construction.
//! Those implementations are:
//! - libp2p: all the connection is handled by libp2p.
//! - In memory: a simplifying node used for emulation pourpouses mainly.

use std::{net::IpAddr, sync::Arc};

use libp2p::{identity, multiaddr::Protocol, Multiaddr, PeerId};

use self::libp2p_impl::NodeLibP2P;
use crate::{
    config::CONF,
    conn_manager::PeerKey,
    contract::{CHandlerImpl, ContractError, ContractStoreError},
    operations::{get, put, subscribe, OpError},
    ring::{Location, PeerKeyLocation},
    user_events::{UserEvent, UserEventsProxy},
};
#[cfg(test)]
use crate::{contract::SimStoreError, user_events::test_utils::MemoryEventsGen};

#[cfg(test)]
pub(crate) use in_memory::NodeInMemory;
pub(crate) use op_state::OpManager;

mod event_listener;
#[cfg(test)]
mod in_memory;
mod libp2p_impl;
mod op_state;
#[cfg(test)]
pub(crate) mod test_utils;

pub struct Node<CErr>(NodeImpl<CErr>);

#[cfg(test)]
impl<CErr> Node<CErr>
where
    CErr: std::error::Error + Send + Sync + 'static,
{
    pub async fn listen_on(&mut self) -> Result<(), anyhow::Error> {
        match self.0 {
            NodeImpl::LibP2P(ref mut node) => node.listen_on().await,
            NodeImpl::InMemory(ref mut node) => {
                let (_usr_ev_controller, rcv_copy) =
                    tokio::sync::watch::channel((0, PeerKey::random()));
                let user_events = MemoryEventsGen::new(rcv_copy, node.peer_key);
                node.listen_on(user_events).await
            }
        }
    }
}

#[cfg(not(test))]
impl<CErr> Node<CErr>
where
    CErr: std::error::Error + Send + Sync + 'static,
{
    pub async fn listen_on(&mut self) -> Result<(), anyhow::Error> {
        match self.0 {
            NodeImpl::LibP2P(ref mut node) => node.listen_on().await,
        }
    }
}

#[cfg(test)]
enum NodeImpl<CErr> {
    LibP2P(Box<NodeLibP2P<CErr>>),
    InMemory(Box<NodeInMemory<CErr>>),
}

#[cfg(not(test))]
enum NodeImpl<CErr> {
    LibP2P(Box<NodeLibP2P<CErr>>),
}

/// When instancing a node you can either join an existing network or bootstrap a new network with a listener
/// which will act as the initial provider. This initial peer will be listening at the provided port and assigned IP.
/// If those are not free the instancing process will return an error.
///
/// In order to bootstrap a new network the following arguments are required to be provided to the builder:
/// - ip: IP associated to the initial node.
/// - port: listening port of the initial node.
///
/// If both are provided but also additional peers are added via the [`Self::add_provider()`] method, this node will
/// be listening but also try to connect to an existing peer.
#[derive(Clone)]
pub struct NodeConfig {
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
}

impl NodeConfig {
    pub fn new() -> NodeConfig {
        let local_key = if let Some(key) = &CONF.local_peer_keypair {
            key.clone()
        } else {
            identity::Keypair::generate_ed25519()
        };
        NodeConfig {
            local_key,
            remote_nodes: Vec::with_capacity(1),
            local_ip: None,
            local_port: None,
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

    /// Builds a node using libp2p as backend connection manager.
    pub fn build_libp2p(self) -> Result<Node<ContractStoreError>, anyhow::Error> {
        let node = NodeLibP2P::<ContractStoreError>::build::<CHandlerImpl>(self)?;
        Ok(Node(NodeImpl::LibP2P(Box::new(node))))
    }

    #[cfg(test)]
    /// Builds a node using in-memory transport. Used for testing pourpouses.
    pub(crate) fn build_in_memory(self) -> Result<Node<SimStoreError>, anyhow::Error> {
        use self::event_listener::TestEventListener;
        use crate::contract::MemoryContractHandler;

        let listener = Box::new(TestEventListener::new());
        let in_mem =
            NodeInMemory::<SimStoreError>::build::<MemoryContractHandler>(self, Some(listener))?;
        Ok(Node(NodeImpl::InMemory(Box::new(in_mem))))
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

impl Default for NodeConfig {
    fn default() -> Self {
        Self::new()
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
            &identity::Keypair::Ed25519(
                identity::ed25519::Keypair::decode(bytes.as_mut()).unwrap(),
            )
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

/// Small helper function to convert a tuple composed of an IP address and a port
/// to a libp2p Multiaddr type.
fn multiaddr_from_connection(conn: (IpAddr, u16)) -> Multiaddr {
    let mut addr = Multiaddr::with_capacity(2);
    addr.push(Protocol::from(conn.0));
    addr.push(Protocol::Tcp(conn.1));
    addr
}

/// Process user events.
async fn user_event_handling<UsrEv, CErr>(op_storage: Arc<OpManager<CErr>>, mut user_events: UsrEv)
where
    UsrEv: UserEventsProxy + Send + Sync + 'static,
    CErr: std::error::Error + Send + Sync + 'static,
{
    loop {
        let ev = user_events.recv().await;
        let op_storage_cp = op_storage.clone();
        tokio::spawn(async move {
            match ev {
                UserEvent::Put { value, contract } => {
                    // Initialize a put op.
                    log::debug!(
                        "Received put from user event @ {}",
                        &op_storage_cp.ring.peer_key
                    );
                    let op = put::PutOp::start_op(
                        contract,
                        value,
                        op_storage_cp.ring.max_hops_to_live,
                        &op_storage_cp.ring.peer_key,
                    );
                    if let Err(err) = put::request_put(&op_storage_cp, op).await {
                        log::error!("{}", err);
                    }
                }
                UserEvent::Get { key, contract } => {
                    // Initialize a get op.
                    log::debug!(
                        "Received get from user event @ {}",
                        &op_storage_cp.ring.peer_key
                    );
                    let op = get::GetOp::start_op(key, contract, &op_storage_cp.ring.peer_key);
                    if let Err(err) = get::request_get(&op_storage_cp, op).await {
                        log::error!("{}", err);
                    }
                }
                UserEvent::Subscribe { key } => {
                    // Initialize a subscribe op.
                    loop {
                        let op =
                            subscribe::SubscribeOp::start_op(key, &op_storage_cp.ring.peer_key);
                        match subscribe::request_subscribe(&op_storage_cp, op).await {
                            Err(OpError::ContractError(ContractError::ContractNotFound(key))) => {
                                log::warn!("Trying to subscribe to a contract not present: {}, requesting it first", key);
                                let get_op =
                                    get::GetOp::start_op(key, true, &op_storage_cp.ring.peer_key);
                                if let Err(err) = get::request_get(&op_storage_cp, get_op).await {
                                    log::error!("Failed getting the contract `{}` while previously trying to subscribe; bailing: {}", key, err);
                                }
                            }
                            Err(err) => {
                                log::error!("{}", err);
                                break;
                            }
                            Ok(()) => break,
                        }
                    }
                }
            }
        });
    }
}
