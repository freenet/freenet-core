use std::sync::Arc;

use either::Either;
use libp2p::{
    core::{muxing, transport, upgrade},
    dns::TokioDnsConfig,
    identity::Keypair,
    noise,
    tcp::TokioTcpConfig,
    yamux, PeerId, Transport,
};
use tokio::sync::mpsc::{self, Receiver};

use super::{
    conn_manager::p2p_protoc::P2pConnManager, join_ring_request, user_event_handling, PeerKey,
};
use crate::{
    config::{self, GlobalExecutor},
    contract::{self, ContractHandler, ContractStoreError},
    message::{Message, NodeEvent},
    ring::Ring,
    NodeConfig, UserEventsProxy,
};

use super::OpManager;

pub(super) struct NodeP2P<CErr = ContractStoreError> {
    pub(crate) peer_key: PeerKey,
    pub(crate) op_storage: Arc<OpManager<CErr>>,
    notification_channel: Receiver<Either<Message, NodeEvent>>,
    pub(super) conn_manager: P2pConnManager,
    // event_listener: Option<Box<dyn EventListener + Send + Sync + 'static>>,
    is_gateway: bool,
}

impl<CErr> NodeP2P<CErr>
where
    CErr: std::error::Error + Send + Sync + 'static,
{
    pub(super) async fn run_node<UsrEv>(mut self, user_events: UsrEv) -> Result<(), anyhow::Error>
    where
        UsrEv: UserEventsProxy + Send + Sync + 'static,
    {
        // 1. start listening in case this is a listening node (gateway) and join the ring
        if self.is_gateway {
            self.conn_manager.listen_on()?;
        }
        let gateways_to_join =
            super::initial_join_requests(self.is_gateway, self.conn_manager.gateways.iter());
        for gateway in gateways_to_join {
            join_ring_request(
                None,
                self.peer_key,
                gateway,
                &self.op_storage,
                &mut self.conn_manager.bridge,
            )
            .await?;
        }

        // 2. start the user event handler loop
        GlobalExecutor::spawn(user_event_handling(self.op_storage.clone(), user_events));

        // 3. start the p2p event loop
        self.conn_manager
            .run_event_listener(self.op_storage.clone(), self.notification_channel)
            .await
    }

    pub fn build<CH>(
        config: NodeConfig,
    ) -> Result<NodeP2P<<CH as ContractHandler>::Error>, anyhow::Error>
    where
        CH: ContractHandler + Send + Sync + 'static,
        <CH as ContractHandler>::Error: std::error::Error + Send + Sync + 'static,
    {
        let peer_key = PeerKey::from(config.local_key.public());
        let gateways = config.get_gateways()?;

        let conn_manager = {
            let transport = Self::config_transport(&config.local_key)?;
            P2pConnManager::build(transport, &config)?
        };

        let ring = Ring::new(&config, &gateways)?;
        let (notification_tx, notification_channel) = mpsc::channel(100);
        let (ops_ch_channel, ch_channel) = contract::contract_handler_channel();
        let op_storage = Arc::new(OpManager::new(ring, notification_tx, ops_ch_channel));
        let contract_handler = CH::from(ch_channel);

        GlobalExecutor::spawn(contract::contract_handling(contract_handler));

        Ok(NodeP2P {
            peer_key,
            conn_manager,
            notification_channel,
            op_storage,
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

        let tcp = TokioTcpConfig::new().nodelay(true).port_reuse(true);
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
        Ok(TokioDnsConfig::system(tcp)?
            .upgrade(upgrade::Version::V1)
            .authenticate(noise::NoiseConfig::xx(noise_keys).into_authenticated())
            .multiplex(yamux::YamuxConfig::default())
            .timeout(config::PEER_TIMEOUT)
            .map(|(peer, muxer), _| (peer, muxing::StreamMuxerBox::new(muxer)))
            .boxed())
    }
}

#[cfg(test)]
mod test {
    use std::{net::Ipv4Addr, time::Duration};

    use super::super::conn_manager::p2p_protoc::NetEvent;
    use super::*;
    use crate::{
        config::{tracing::Logger, GlobalExecutor},
        contract::TestContractHandler,
        node::{test::get_free_port, InitPeerNode},
        ring::Location,
    };

    use futures::StreamExt;
    use libp2p::swarm::SwarmEvent;

    /// Ping test event loop
    async fn ping_ev_loop<CErr>(peer: &mut NodeP2P<CErr>) -> Result<(), ()>
    where
        CErr: std::error::Error,
    {
        loop {
            let ev = tokio::time::timeout(
                Duration::from_secs(30),
                peer.conn_manager.swarm.select_next_some(),
            );
            match ev.await {
                Ok(SwarmEvent::Behaviour(NetEvent::Ping(ping))) => {
                    if ping.result.is_ok() {
                        log::info!("ping done @ {}", peer.peer_key);
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
        let peer1_port = get_free_port().unwrap();
        let peer1_key = Keypair::generate_ed25519();
        let peer1_id: PeerId = peer1_key.public().into();
        let peer1_config = InitPeerNode::new(peer1_id, Location::random())
            .listening_ip(Ipv4Addr::LOCALHOST)
            .listening_port(peer1_port);

        // Start up the initial node.
        GlobalExecutor::spawn(async move {
            let mut config = NodeConfig::default();
            config
                .with_ip(Ipv4Addr::LOCALHOST)
                .with_port(peer1_port)
                .with_key(peer1_key);
            let mut peer1 = Box::new(NodeP2P::<ContractStoreError>::build::<TestContractHandler>(
                config,
            )?);
            peer1.conn_manager.listen_on()?;
            ping_ev_loop(&mut peer1).await.unwrap();
            Ok::<_, anyhow::Error>(())
        });

        // Start up the dialing node
        let dialer = GlobalExecutor::spawn(async move {
            let mut config = NodeConfig::default();
            config.add_gateway(peer1_config.clone());
            let mut peer2 =
                NodeP2P::<ContractStoreError>::build::<TestContractHandler>(config).unwrap();
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
