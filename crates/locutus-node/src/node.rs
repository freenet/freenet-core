//! The main node data type which encapsulates all the behaviour for maintaining a connection
//! and performing operations within the network.
//!
//! # Implementations
//! Node comes with different underlying implementations that can be used upon construction.
//! Those implementations are:
//! - libp2p: all the connection is handled by libp2p.
//! - In memory: a simplifying node used for emulation pourpouses mainly.

use std::net::IpAddr;

use libp2p::{identity, multiaddr::Protocol, Multiaddr, PeerId};

use crate::{config::CONF, ring::Location};

use self::libp2p_impl::NodeLibP2P;
pub(crate) use in_memory::NodeInMemory;
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
    InMemory(Box<NodeInMemory>),
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

    /// the location of this node, used for gateways.
    location: Option<Location>,

    max_hops_to_live: Option<usize>,
    rnd_if_htl_above: Option<usize>,
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
        }
    }

    pub fn max_hops_to_live(mut self, num_hops: usize) -> Self {
        self.max_hops_to_live = Some(num_hops);
        self
    }

    pub fn rnd_if_htl_above(mut self, num_hops: usize) -> Self {
        self.rnd_if_htl_above = Some(num_hops);
        self
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

    pub fn with_location(mut self, loc: Location) -> Self {
        self.location = Some(loc);
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
        let inmem = NodeInMemory::build(self)?;
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
    location: Option<Location>,
}

impl InitPeerNode {
    pub fn new() -> Self {
        Self {
            addr: None,
            identifier: None,
            location: None,
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

    pub fn with_location(mut self, loc: Location) -> Self {
        self.location = Some(loc);
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
            location: None,
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

#[cfg(test)]
pub mod test_utils {
    use std::net::{Ipv4Addr, Ipv6Addr, SocketAddr, TcpListener};

    use libp2p::{identity, PeerId};
    use rand::Rng;
    use tokio::sync::mpsc;

    use crate::{
        conn_manager::{ConnectionBridge, Transport},
        message::Message,
        node::{InitPeerNode, NodeInMemory},
        operations::{
            join_ring::{handle_join_ring, JoinRingMsg},
            OpError,
        },
        ring::{Distance, Location},
        NodeConfig,
    };

    pub fn get_free_port() -> Result<u16, ()> {
        let mut port;
        for _ in 0..100 {
            port = get_dynamic_port();
            let bind_addr = SocketAddr::from((Ipv4Addr::LOCALHOST, port));
            if let Ok(conn) = TcpListener::bind(bind_addr) {
                std::mem::drop(conn);
                return Ok(port);
            }
        }
        Err(())
    }

    pub fn get_dynamic_port() -> u16 {
        const FIRST_DYNAMIC_PORT: u16 = 49152;
        const LAST_DYNAMIC_PORT: u16 = 65535;
        rand::thread_rng().gen_range(FIRST_DYNAMIC_PORT..LAST_DYNAMIC_PORT)
    }

    pub(crate) struct SimNetwork {
        // gateways: HashMap<String, InMemory>,
        // peers: HashMap<String, InMemory>,
        meta_info_tx: mpsc::Sender<Result<NetEvent, OpError>>,
        meta_info_rx: mpsc::Receiver<Result<NetEvent, OpError>>,
    }

    pub(crate) struct NetEvent {
        pub(crate) event: EventType,
    }

    pub(crate) enum EventType {
        /// A peer joined the network through some gateway.
        JoinSuccess { peer: PeerId },
    }

    impl SimNetwork {
        pub fn build(
            network_size: usize,
            ring_max_htl: usize,
            rnd_if_htl_above: usize,
        ) -> SimNetwork {
            let sim = SimNetwork::new();

            // build gateway node
            // let probe_protocol = Some(ProbeProtocol::new(ring_protocol.clone(), loc));
            let gateway_pair = identity::Keypair::generate_ed25519();
            let gateway_peer_id = gateway_pair.public().into_peer_id();
            let gateway_port = get_free_port().unwrap();
            let gateway_loc = Location::random();
            let config = NodeConfig::new()
                .with_ip(Ipv6Addr::LOCALHOST)
                .with_port(gateway_port)
                .with_key(gateway_pair)
                .with_location(gateway_loc)
                .max_hops_to_live(ring_max_htl)
                .rnd_if_htl_above(rnd_if_htl_above);
            let gateway = NodeInMemory::build(config).unwrap();
            sim.initialize_gateway(gateway, "gateway".to_owned());

            // add other nodes to the simulation
            for node_no in 0..network_size {
                let label = format!("node-{}", node_no);
                let config = NodeConfig::new()
                    .add_provider(
                        InitPeerNode::new()
                            .listening_ip(Ipv6Addr::LOCALHOST)
                            .listening_port(gateway_port)
                            .with_identifier(gateway_peer_id)
                            .with_location(gateway_loc),
                    )
                    .max_hops_to_live(ring_max_htl)
                    .rnd_if_htl_above(rnd_if_htl_above);
                sim.initialize_peer(NodeInMemory::build(config).unwrap(), label);
            }
            sim
        }

        pub async fn recv_net_events(&mut self) -> Option<Result<NetEvent, OpError>> {
            self.meta_info_rx.recv().await
        }

        fn new() -> Self {
            let (meta_info_tx, meta_info_rx) = mpsc::channel(100);
            Self {
                meta_info_rx,
                meta_info_tx,
            }
        }

        fn initialize_gateway(&self, gateway: NodeInMemory, sender_label: String) {
            let info_ch = self.meta_info_tx.clone();
            tokio::spawn(Self::listen(gateway, info_ch, sender_label));
        }

        fn initialize_peer(&self, mut peer: NodeInMemory, sender_label: String) {
            let info_ch = self.meta_info_tx.clone();
            tokio::spawn(async move {
                if peer.join_ring().await.is_err() {
                    let _ = info_ch.send(Err(OpError::IllegalStateTransition)).await;
                    return Err(());
                }
                Self::listen(peer, info_ch, sender_label).await
            });
        }

        async fn listen(
            mut gateway: NodeInMemory,
            info_ch: mpsc::Sender<Result<NetEvent, OpError>>,
            _sender: String,
        ) -> Result<(), ()> {
            while let Ok(msg) = gateway.conn_manager.recv().await {
                if let Message::JoinRing(msg) = msg {
                    if let JoinRingMsg::Connected { target, .. } = msg {
                        let _ = info_ch
                            .send(Ok(NetEvent {
                                event: EventType::JoinSuccess {
                                    peer: target.peer.0,
                                },
                            }))
                            .await;
                        break;
                    }
                    match handle_join_ring(&mut gateway.op_storage, &mut gateway.conn_manager, msg)
                        .await
                    {
                        Err(err) => {
                            let _ = info_ch.send(Err(err)).await;
                        }
                        Ok(()) => {}
                    }
                } else {
                    return Err(());
                }
            }
            Ok(())
        }
    }

    /// Builds an histogram of the distribution in the ring of each node relative to each other.
    fn _ring_distribution<'a>(
        nodes: impl Iterator<Item = &'a NodeInMemory> + 'a,
    ) -> impl Iterator<Item = Distance> + 'a {
        // TODO: groupby  certain intervals
        // e.g. grouping func: (it * 200.0).roundToInt().toDouble() / 200.0
        nodes
            .map(|node| {
                let node_ring = &node.op_storage.ring;
                let self_loc = node.conn_manager.transport.location().unwrap();
                node_ring
                    .connections_by_location
                    .read()
                    .keys()
                    .map(|d| self_loc.distance(d))
                    .collect::<Vec<_>>()
            })
            .flatten()
    }
}
