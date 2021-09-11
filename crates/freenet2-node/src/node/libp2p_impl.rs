#![allow(unused)] // FIXME: remove this attr
use std::net::IpAddr;

use libp2p::{
    core::{muxing, transport, upgrade},
    dns::TokioDnsConfig,
    identify,
    identity::{self, Keypair},
    multiaddr::Protocol,
    noise, ping,
    swarm::SwarmBuilder,
    tcp::TokioTcpConfig,
    yamux, Multiaddr, PeerId, Swarm, Transport,
};

use crate::{
    config::{self, GlobalExecutor, CONF},
    NodeConfig,
};

const CURRENT_AGENT_VER: &str = "freenet2/0.1.0";
const CURRENT_IDENTIFY_PROTOC_VER: &str = "id/1.0.0";

pub struct NodeLibP2P {
    swarm: Swarm<NetBehaviour>,
    listen_on: Option<(IpAddr, u16)>,
}

impl NodeLibP2P {
    pub(super) fn listen_on(&mut self) -> Result<(), ()> {
        if let Some(conn) = self.listen_on {
            let listening_addr = super::multiaddr_from_connection(conn);
            self.swarm.listen_on(listening_addr).unwrap();
            Ok(())
        } else {
            Err(())
        }
    }

    pub(super) fn build(config: NodeConfig) -> std::io::Result<Self> {
        if (config.local_ip.is_none() || config.local_port.is_none())
            && config.remote_nodes.is_empty()
        {
            // This is not an initial provider. At least one remote provider is required to join an existing network.
            // return Err();
            // todo!()
        }

        let transport = Self::config_transport(&config.local_key)?;
        let behaviour = Self::config_behaviour(&config.local_key);

        let swarm = {
            // We set a global executor which is virtually the Tokio multi-threaded executor
            // to reuse it's thread pool and scheduler in order to drive futures.
            let global_executor = Box::new(GlobalExecutor);
            let builder = SwarmBuilder::new(transport, behaviour, config.local_peer_id)
                .executor(global_executor);
            builder.build()
        };

        Ok(Self {
            swarm,
            listen_on: config.local_ip.zip(config.local_port),
        })
    }

    /// Capabilities built into the transport by default:
    ///
    /// - TCP/IP handling over Tokio streams.
    /// - DNS when dialing peers.
    /// - Authentication and encryption via [Noise](https://github.com/libp2p/specs/tree/master/noise) protocol.
    /// - Compression using Deflate (disabled right now due to a bug).
    /// - Multiplexing using [Yamux](https://github.com/hashicorp/yamux/blob/master/spec.md).
    fn config_transport(
        local_key: &Keypair,
    ) -> std::io::Result<transport::Boxed<(PeerId, muxing::StreamMuxerBox)>> {
        let noise_keys = noise::Keypair::<noise::X25519Spec>::new()
            .into_authentic(local_key)
            .expect("signing libp2p-noise static DH keypair failed");

        let tcp = TokioTcpConfig::new()
            .nodelay(true)
            .port_reuse(true)
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

    fn config_behaviour(local_key: &Keypair) -> NetBehaviour {
        let ident_config = identify::IdentifyConfig::new(
            CURRENT_IDENTIFY_PROTOC_VER.to_string(),
            local_key.public(),
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

#[cfg(test)]
mod tests {
    use std::{
        net::{Ipv4Addr, SocketAddr, TcpListener},
        time::Duration,
    };

    use crate::{
        config::tracing::Logger,
        node::{InitPeerNode, Node},
    };

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

    /// Ping test event loop
    async fn ping_ev_loop(peer: &mut NodeLibP2P) -> Result<(), ()> {
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
        Logger::init_logger();

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
            log::debug!("Initial peer port: {}", peer1_port);
            let config = NodeConfig::default()
                .with_ip(Ipv4Addr::LOCALHOST)
                .with_port(peer1_port)
                .with_key(peer1_key);
            let mut peer1 = NodeLibP2P::build(config).unwrap();
            peer1.listen_on().unwrap();
            ping_ev_loop(&mut peer1).await
        });

        // Start up the dialing node
        let dialer = GlobalExecutor::spawn(async move {
            let mut peer2 = NodeLibP2P::build(NodeConfig::default()).unwrap();
            // wait a bit to make sure the first peer is up and listening
            tokio::time::sleep(Duration::from_millis(10)).await;
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
