use std::net::IpAddr;

use libp2p::{identity, multiaddr::Protocol, Multiaddr, PeerId};

use crate::config::CONF;

use self::{in_memory::InMemory, libp2p_impl::NodeLibP2P};
pub(crate) use op_state::{OpExecutionError, OpStateStorage};

mod in_memory;
mod libp2p_impl;
mod op_state;

pub struct Node(NodeImpl);

impl Node {
    pub async fn listen_on(&mut self) -> Result<(), ()> {
        match self.0 {
            NodeImpl::LibP2P(ref mut node) => node.listen_on().await,
            NodeImpl::InMemory(ref mut node) => node.listen_on().await,
        }
    }
}

enum NodeImpl {
    LibP2P(Box<NodeLibP2P>),
    InMemory(Box<InMemory>),
}

pub struct NodeConfig {
    /// local peer private key in
    local_key: identity::Keypair,

    // optional local info, in case this is an initial bootstrap node
    /// IP to bind to the listener
    local_ip: Option<IpAddr>,
    /// socket port to bind to the listener
    local_port: Option<u16>,

    /// At least an other running listener node is required for joining the network.
    /// Not necessary if this is an initial node.
    remote_nodes: Vec<InitPeerNode>,
}

impl NodeConfig {
    /// When instancing a node you can either join an existing network or bootstrap a new network with a listener
    /// which will act as the initial provider. This initial peer will be listening at the provided port and assigned IP.
    /// If those are not free the instancing process will return an error.
    ///
    /// In order to bootstrap a new network the following arguments are required to be provided to the builder:
    /// - ip: IP associated to the initial node.
    /// - port: listening port of the initial node.
    ///
    /// If both are provided but also additional peers are added via the [add_provider] method, this node will
    /// be listening but also try to connect to an existing peer.
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
        }
    }

    pub fn with_port(mut self, port: u16) -> Self {
        self.local_port = Some(port);
        self
    }

    pub fn with_ip<T: Into<IpAddr>>(mut self, ip: T) -> Self {
        self.local_ip = Some(ip.into());
        self
    }

    /// Optional identity key of this node.
    /// If not provided it will be either obtained from the configuration or freshly generated.
    pub fn with_key(mut self, key: identity::Keypair) -> Self {
        self.local_key = key;
        self
    }

    /// Connection info for an already existing peer. Required in case this is not a bootstrapping node.
    pub fn add_provider(mut self, peer: InitPeerNode) -> Self {
        self.remote_nodes.push(peer);
        self
    }

    /// Builds a node using libp2p as backend connection manager.
    pub fn build_libp2p(self) -> std::io::Result<Node> {
        Ok(Node(NodeImpl::LibP2P(Box::new(NodeLibP2P::build(self)?))))
    }

    /// Builds a node using in-memory transport. Used for testing pourpouses.
    pub fn build_in_memory(self) -> Result<Node, &'static str> {
        let inmem = InMemory::build(self)?;
        Ok(Node(NodeImpl::InMemory(Box::new(inmem))))
    }
}

impl Default for NodeConfig {
    fn default() -> Self {
        Self::new()
    }
}

/// Initial listening peer node to bootstrap the network.
#[derive(Clone)]
pub struct InitPeerNode {
    addr: Option<Multiaddr>,
    identifier: Option<PeerId>,
}

impl InitPeerNode {
    pub fn new() -> Self {
        Self {
            addr: None,
            identifier: None,
        }
    }

    /// Given a byte array decode into a PeerId data type.
    ///
    /// # Panic
    /// Will panic if is not a valid representation.
    pub fn decode_peer_id<T: AsMut<[u8]>>(mut bytes: T) -> PeerId {
        PeerId::from_public_key(
            identity::Keypair::Ed25519(identity::ed25519::Keypair::decode(bytes.as_mut()).unwrap())
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

    pub fn with_identifier(mut self, id: PeerId) -> Self {
        self.identifier = Some(id);
        self
    }
}

impl std::default::Default for InitPeerNode {
    fn default() -> Self {
        let conf = &CONF;
        let identifier = conf.bootstrap_id
        .expect("At least one public identifier is required to bootstrap the connection to the network.");
        let multi_addr = multiaddr_from_connection((conf.bootstrap_ip, conf.bootstrap_port));
        Self {
            addr: Some(multi_addr),
            identifier: Some(identifier),
        }
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
