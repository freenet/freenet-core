use std::{net::IpAddr, sync::Arc};

use libp2p::{
    core::{muxing, transport, upgrade},
    dns::TokioDnsConfig,
    identify,
    identity::Keypair,
    noise, ping,
    swarm::SwarmBuilder,
    tcp::TokioTcpConfig,
    yamux, PeerId, Swarm, Transport,
};
use tokio::sync::mpsc::{self, Receiver};

use crate::{
    config::{self, GlobalExecutor},
    conn_manager::PeerKey,
    contract::{self, ContractHandler, ContractStoreError},
    message::Message,
    ring::{PeerKeyLocation, Ring},
    NodeConfig,
};

use super::OpManager;

const CURRENT_AGENT_VER: &str = "locutus/0.1.0";
const CURRENT_IDENTIFY_PROTOC_VER: &str = "id/1.0.0";

pub struct NodeLibP2P<CErr = ContractStoreError> {
    listen_on: Option<(IpAddr, u16)>,
    pub(crate) peer_key: PeerKey,
    gateways: Vec<PeerKeyLocation>,
    notification_channel: Receiver<Message>,
    pub(crate) conn_manager: P2pConnectionManager,
    pub(crate) op_storage: Arc<OpManager<CErr>>,
    // event_listener: Option<Box<dyn EventListener + Send + Sync + 'static>>,
    is_gateway: bool,
}

pub(crate) struct P2pConnectionManager {
    swarm: Swarm<NetBehaviour>,
}

impl<CErr> NodeLibP2P<CErr>
where
    CErr: std::error::Error,
{
    pub(super) async fn listen_on(&mut self) -> Result<(), anyhow::Error> {
        if let Some(conn) = self.listen_on {
            let listening_addr = super::multiaddr_from_connection(conn);
            self.conn_manager.swarm.listen_on(listening_addr).unwrap();
            Ok(())
        } else {
            anyhow::bail!("failed listening to connections")
        }
    }

    pub(super) fn build<CH>(
        config: NodeConfig,
    ) -> Result<NodeLibP2P<<CH as ContractHandler>::Error>, anyhow::Error>
    where
        CH: ContractHandler + Send + Sync + 'static,
        <CH as ContractHandler>::Error: std::error::Error + Send + Sync + 'static,
    {
        let peer_key = PeerKey::from(config.local_key.public());
        let gateways = config.get_gateways()?;

        let conn_manager = {
            let transport = Self::config_transport(&config.local_key)?;
            let behaviour = Self::config_behaviour(&config.local_key);
            // We set a global executor which is virtually the Tokio multi-threaded executor
            // to reuse it's thread pool and scheduler in order to drive futures.
            let global_executor = Box::new(GlobalExecutor);
            let builder = SwarmBuilder::new(
                transport,
                behaviour,
                PeerId::from(config.local_key.public()),
            )
            .executor(global_executor);
            let swarm = builder.build();
            P2pConnectionManager { swarm }
        };

        let ring = Ring::new(&config, &gateways)?;
        let (notification_tx, notification_channel) = mpsc::channel(100);
        let (ops_ch_channel, ch_channel) = contract::contract_handler_channel();
        let op_storage = Arc::new(OpManager::new(ring, notification_tx, ops_ch_channel));
        let contract_handler = CH::from(ch_channel);

        tokio::spawn(contract::contract_handling(contract_handler));

        Ok(NodeLibP2P {
            peer_key,
            conn_manager,
            gateways,
            notification_channel,
            op_storage,
            listen_on: config.local_ip.zip(config.local_port),
            is_gateway: config.location.is_some(),
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
            .timeout(config::PEER_TIMEOUT)
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
// TODO: - locutus routing and messaging protocol
#[derive(libp2p::NetworkBehaviour)]
#[behaviour(event_process = false)]
#[behaviour(out_event = "NetEvent")]
struct NetBehaviour {
    identify: identify::Identify,
    ping: ping::Ping,
}

#[derive(Debug)]
pub(crate) enum NetEvent {
    Identify(Box<identify::IdentifyEvent>),
    Ping(ping::PingEvent),
}

impl From<identify::IdentifyEvent> for NetEvent {
    fn from(event: identify::IdentifyEvent) -> NetEvent {
        Self::Identify(Box::new(event))
    }
}

impl From<ping::PingEvent> for NetEvent {
    fn from(event: ping::PingEvent) -> NetEvent {
        Self::Ping(event)
    }
}

#[cfg(test)]
mod test {
    use std::{net::Ipv4Addr, time::Duration};

    use super::*;
    use crate::{
        config::tracing::Logger,
        contract::CHandlerImpl,
        node::{test_utils::get_free_port, InitPeerNode},
        ring::Location,
    };

    use futures::StreamExt;
    use libp2p::swarm::SwarmEvent;

    /// Ping test event loop
    async fn ping_ev_loop<CErr>(peer: &mut NodeLibP2P<CErr>) -> Result<(), ()>
    where
        CErr: std::error::Error,
    {
        loop {
            let ev = tokio::time::timeout(
                Duration::from_secs(1),
                peer.conn_manager.swarm.select_next_some(),
            );
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

        let peer1_key = Keypair::generate_ed25519();
        let peer1_id: PeerId = peer1_key.public().into();
        let peer1_port = get_free_port().unwrap();
        let peer1_config = InitPeerNode::new(peer1_id, Location::random())
            .listening_ip(Ipv4Addr::LOCALHOST)
            .listening_port(peer1_port);

        // Start up the initial node.
        GlobalExecutor::spawn(async move {
            log::debug!("Initial peer port: {}", peer1_port);
            let mut config = NodeConfig::default();
            config
                .with_ip(Ipv4Addr::LOCALHOST)
                .with_port(peer1_port)
                .with_key(peer1_key);
            let mut peer1 =
                NodeLibP2P::<ContractStoreError>::build::<CHandlerImpl>(config).unwrap();
            peer1.listen_on().await.unwrap();
            ping_ev_loop(&mut peer1).await
        });

        // Start up the dialing node
        let dialer = GlobalExecutor::spawn(async move {
            let mut peer2 =
                NodeLibP2P::<ContractStoreError>::build::<CHandlerImpl>(NodeConfig::default())
                    .unwrap();
            // wait a bit to make sure the first peer is up and listening
            tokio::time::sleep(Duration::from_millis(10)).await;
            peer2
                .conn_manager
                .swarm
                .dial(peer1_config.addr.unwrap())
                .map_err(|_| ())?;
            let res = ping_ev_loop(&mut peer2).await;
            res
        });

        dialer.await.map_err(|_| ())?
    }
}
