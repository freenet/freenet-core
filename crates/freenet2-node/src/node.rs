use std::net::IpAddr;

use libp2p::{
    core::{muxing, transport, upgrade},
    dns::TokioDnsConfig,
    identify, identity,
    multiaddr::Protocol,
    noise, ping,
    swarm::SwarmBuilder,
    tcp::TokioTcpConfig,
    yamux, Multiaddr, PeerId, Swarm, Transport,
};

use crate::config::{self, GlobalExecutor, CONF};

const CURRENT_AGENT_VER: &str = "freenet2/0.1.0";
const CURRENT_IDENTIFY_PROTOC_VER: &str = "id/1.0.0";

pub struct Node {
    swarm: Swarm<NetBehaviour>,
    listen_on: Option<(IpAddr, u16)>,
}

impl Node {
    fn listen_on(&mut self) -> Result<(), ()> {
        if let Some(conn) = self.listen_on {
            let listening_addr = multiaddr_from_connection(conn);
            self.swarm.listen_on(listening_addr).unwrap();
            Ok(())
        } else {
            Err(())
        }
    }
}

/// The network behaviour implements the following capabilities:
///
/// - [Identify](https://github.com/libp2p/specs/tree/master/identify) libp2p protocol.
/// - Pinging between peers.
// TODO: - freenet2 routing and messaging protocol
#[derive(libp2p::NetworkBehaviour)]
#[behaviour(event_process = false)]
#[behaviour(out_event = "NetEvent")]
struct NetBehaviour {
    identify: identify::Identify,
    ping: ping::Ping,
}

#[derive(Debug)]
pub(crate) enum NetEvent {
    Identify(identify::IdentifyEvent),
    Ping(ping::PingEvent),
}

impl From<identify::IdentifyEvent> for NetEvent {
    fn from(event: identify::IdentifyEvent) -> NetEvent {
        Self::Identify(event)
    }
}

impl From<ping::PingEvent> for NetEvent {
    fn from(event: ping::PingEvent) -> NetEvent {
        Self::Ping(event)
    }
}

pub struct NodeConfig {
    /// ED25519 local peer private key.
    local_ed25519_key: identity::ed25519::Keypair,
    /// ED25519 local peer private key in generic format.
    local_key: identity::Keypair,
    /// The peer ID of this machine.
    local_peer_id: PeerId,

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
        let local_ed25519_key = if let Some(key) = &CONF.local_peer_keypair {
            key.clone()
        } else {
            identity::ed25519::Keypair::generate()
        };
        let local_key = identity::Keypair::Ed25519(local_ed25519_key.clone());
        let local_peer_id = PeerId::from(local_key.public());
        NodeConfig {
            local_ed25519_key,
            local_key,
            local_peer_id,
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
    pub fn with_key(mut self, key: identity::ed25519::Keypair) -> Self {
        self.local_key = identity::Keypair::Ed25519(key.clone());
        self.local_ed25519_key = key;
        self.local_peer_id = PeerId::from(self.local_key.public());
        self
    }

    /// Connection info for an already existing peer. Required in case this is not a bootstrapping node.
    pub fn add_provider(mut self, peer: InitPeerNode) -> Self {
        self.remote_nodes.push(peer);
        self
    }

    /// Builds the default implementation of a node.
    pub fn build(self) -> std::io::Result<Node> {
        if (self.local_ip.is_none() || self.local_port.is_none()) && self.remote_nodes.is_empty() {
            // This is not an initial provider. At least one remote provider is required to join an existing network.
            // return Err();
            // todo!()
        }

        let transport = self.config_transport()?;
        let behaviour = self.config_behaviour();

        let swarm = {
            // We set a global executor which is virtually the Tokio multi-threaded executor
            // to reuse it's thread pool and scheduler in order to drive futures.
            let global_executor = Box::new(GlobalExecutor);
            let builder = SwarmBuilder::new(transport, behaviour, self.local_peer_id)
                .executor(global_executor);
            builder.build()
        };

        Ok(Node {
            swarm,
            listen_on: self.local_ip.zip(self.local_port),
        })
    }

    /// Capabilities built into the transport by default:
    ///
    /// - TCP/IP handling over Tokio streams.
    /// - DNS when dialing peers.
    /// - Authentication and encryption via [Noise protocol](https://github.com/libp2p/specs/tree/master/noise)
    /// - Compression using Deflate (disabled right now due to a bug).
    /// - Multiplexing, preferentially using [Yamux](https://github.com/hashicorp/yamux/blob/master/spec.md).
    fn config_transport(
        &self,
    ) -> std::io::Result<transport::Boxed<(PeerId, muxing::StreamMuxerBox)>> {
        let noise_keys = noise::Keypair::<noise::X25519Spec>::new()
            .into_authentic(&self.local_key)
            .expect("signing libp2p-noise static DH keypair failed");

        let tcp = TokioTcpConfig::new()
            .nodelay(true)
            // FIXME: there seems to be a problem with the deflate upgrade 
            // that repeteadly allocates more space on the heap until OOM
            // .and_then(|conn, endpoint| {
            //     upgrade::apply(
            //         conn,
            //         DeflateConfig::default(),
            //         endpoint,
            //         upgrade::Version::V1,
            //     )
            // });
            ;
        Ok(TokioDnsConfig::system(tcp)?
            .upgrade(upgrade::Version::V1)
            .authenticate(noise::NoiseConfig::xx(noise_keys).into_authenticated())
            .multiplex(yamux::YamuxConfig::default())
            .timeout(std::time::Duration::from_secs(config::PEER_TIMEOUT_SECS))
            .map(|(peer, muxer), _| (peer, muxing::StreamMuxerBox::new(muxer)))
            .boxed())
    }

    fn config_behaviour(&self) -> NetBehaviour {
        let ident_config = identify::IdentifyConfig::new(
            CURRENT_IDENTIFY_PROTOC_VER.to_string(),
            self.local_key.public(),
        )
        .with_agent_version(CURRENT_AGENT_VER.to_string());

        let ping = if cfg!(debug_assertions) {
            ping::Ping::new(ping::PingConfig::new().with_keep_alive(true))
        } else {
            ping::Ping::default()
        };
        NetBehaviour {
            identify: identify::Identify::new(ident_config),
            ping,
        }
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

#[cfg(test)]
mod tests {
    use std::{
        net::{Ipv4Addr, SocketAddr, TcpListener},
        time::Duration,
    };

    use crate::config::tracing::Logger;

    use super::*;
    use libp2p::{futures::StreamExt, swarm::SwarmEvent};
    use rand::Rng;

    fn get_free_port() -> Result<u16, ()> {
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

    fn get_dynamic_port() -> u16 {
        const FIRST_DYNAMIC_PORT: u16 = 49152;
        const LAST_DYNAMIC_PORT: u16 = 65535;
        rand::thread_rng().gen_range(FIRST_DYNAMIC_PORT..LAST_DYNAMIC_PORT)
    }

    async fn ping_ev_loop(peer: &mut Node) -> Result<(), ()> {
        loop {
            let ev = tokio::time::timeout(Duration::from_secs(1), peer.swarm.select_next_some());
            match ev.await {
                Ok(SwarmEvent::Behaviour(NetEvent::Ping(ping))) => {
                    if ping.result.is_ok() {
                        return Ok(());
                    }
                }
                Ok(other) => {
                    log::debug!("{:?}", other)
                }
                Err(_) => {
                    return Err(());
                }
            }
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn ping() -> Result<(), ()> {
        Logger::get_logger();

        let peer1_key = identity::ed25519::Keypair::generate();
        let peer1_id: PeerId = identity::Keypair::Ed25519(peer1_key.clone())
            .public()
            .into();
        let peer1_port = get_free_port().unwrap();
        let peer1_config = InitPeerNode::new()
            .listening_ip(Ipv4Addr::LOCALHOST)
            .listening_port(peer1_port)
            .with_identifier(peer1_id);

        // Start up the initial node.
        GlobalExecutor::spawn(async move {
            log::info!("initial peer port: {}", peer1_port);
            let mut peer1 = NodeConfig::default()
                .with_ip(Ipv4Addr::LOCALHOST)
                .with_port(peer1_port)
                .with_key(peer1_key)
                .build()
                .unwrap();
            peer1.listen_on().unwrap();
            // kill_rx.await.map_err(|_| ())
            ping_ev_loop(&mut peer1).await
        });

        // Start up the dialing node
        let dialer = GlobalExecutor::spawn(async move {
            let mut peer2 = NodeConfig::default().build().unwrap();
            let port = get_free_port().unwrap();
            log::info!("second peer port: {}", port);
            peer2
                .swarm
                .dial_addr(peer1_config.addr.unwrap())
                .map_err(|_| ())?;
            let res = ping_ev_loop(&mut peer2).await;
            res
        });

        dialer.await.map_err(|_| ())?
    }
}
