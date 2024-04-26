use std::sync::Arc;

use libp2p::{
    core::{
        muxing,
        transport::{self, upgrade},
    },
    dns,
    identity::Keypair,
    noise, tcp, yamux, PeerId as Libp2pPeerId, Transport,
};
use tracing::Instrument;

use super::{
    client_event_handling,
    network_bridge::{
        event_loop_notification_channel, p2p_protoc::P2pConnManager, EventLoopNotificationsReceiver,
    },
    NetEventRegister, PeerId as FreenetPeerId,
};
use crate::{
    client_events::{combinator::ClientEventsCombinator, BoxedClient},
    config::{self, Config, GlobalExecutor},
    contract::{
        self, ClientResponsesSender, ContractHandler, ContractHandlerChannel,
        ExecutorToEventLoopChannel, NetworkEventListenerHalve, WaitingResolution,
    },
    message::NodeEvent,
    node::NodeConfig,
    operations::connect,
};

use super::OpManager;

pub(super) struct NodeP2P {
    pub(crate) peer_key: FreenetPeerId,
    pub(crate) op_manager: Arc<OpManager>,
    notification_channel: EventLoopNotificationsReceiver,
    client_wait_for_transaction: ContractHandlerChannel<WaitingResolution>,
    pub(super) conn_manager: P2pConnManager,
    executor_listener: ExecutorToEventLoopChannel<NetworkEventListenerHalve>,
    cli_response_sender: ClientResponsesSender,
    node_controller: tokio::sync::mpsc::Receiver<NodeEvent>,
    is_gateway: bool,
    pub(crate) config: Arc<Config>,
}

impl NodeP2P {
    pub(super) async fn run_node(mut self) -> Result<(), anyhow::Error> {
        // start listening in case this is a listening node (gateway) and join the ring
        if self.is_gateway {
            self.conn_manager.listen_on()?;
        }

        connect::initial_join_procedure(
            &self.op_manager,
            &mut self.conn_manager.bridge,
            self.peer_key,
            &self.conn_manager.gateways,
        )
        .await?;

        // start the p2p event loop
        self.conn_manager
            .run_event_listener(
                &self.config,
                self.op_manager.clone(),
                self.client_wait_for_transaction,
                self.notification_channel,
                self.executor_listener,
                self.cli_response_sender,
                self.node_controller,
            )
            .await
    }

    pub(crate) async fn build<CH, const CLIENTS: usize, ER>(
        config: NodeConfig,
        private_key: Keypair,
        clients: [BoxedClient; CLIENTS],
        event_register: ER,
        ch_builder: CH::Builder,
    ) -> Result<NodeP2P, anyhow::Error>
    where
        CH: ContractHandler + Send + 'static,
        ER: NetEventRegister + Clone,
    {
        let peer_key = config.peer_id;

        let (notification_channel, notification_tx) = event_loop_notification_channel();
        let (ch_outbound, ch_inbound, wait_for_event) = contract::contract_handler_channel();
        let (client_responses, cli_response_sender) = contract::client_responses_channel();

        let op_manager = Arc::new(OpManager::new(
            notification_tx,
            ch_outbound,
            &config,
            event_register.clone(),
        )?);
        let (executor_listener, executor_sender) = contract::executor_channel(op_manager.clone());
        let contract_handler = CH::build(ch_inbound, executor_sender, ch_builder)
            .await
            .map_err(|e| anyhow::anyhow!(e))?;

        let conn_manager = {
            let transport = Self::config_transport(&private_key)?;
            P2pConnManager::build(
                transport,
                &config,
                op_manager.clone(),
                event_register,
                private_key,
            )?
        };

        let parent_span = tracing::Span::current();
        GlobalExecutor::spawn(
            contract::contract_handling(contract_handler)
                .instrument(tracing::info_span!(parent: parent_span.clone(), "contract_handling")),
        );
        let clients = ClientEventsCombinator::new(clients);
        let (node_controller_tx, node_controller_rx) = tokio::sync::mpsc::channel(1);
        GlobalExecutor::spawn(
            client_event_handling(
                op_manager.clone(),
                clients,
                client_responses,
                node_controller_tx,
            )
            .instrument(tracing::info_span!(parent: parent_span, "client_event_handling")),
        );

        Ok(NodeP2P {
            peer_key,
            conn_manager,
            notification_channel,
            client_wait_for_transaction: wait_for_event,
            op_manager,
            executor_listener,
            cli_response_sender,
            node_controller: node_controller_rx,
            is_gateway: config.is_gateway(),
            config: config.config.clone(),
        })
    }

    /// Capabilities built into the transport by default:
    ///
    /// - TCP/IP handling over Tokio streams.
    /// - DNS when dialing peers.
    /// - Authentication and encryption via [Noise](https://github.com/libp2p/specs/tree/master/noise) protocol.
    /// - Compression using Deflate.
    /// - Multiplexing using [Yamux](https://github.com/hashicorp/yamux/blob/master/spec.md).
    fn config_transport(
        local_key: &Keypair,
    ) -> std::io::Result<transport::Boxed<(Libp2pPeerId, muxing::StreamMuxerBox)>> {
        let tcp = tcp::tokio::Transport::new(tcp::Config::new().nodelay(true).port_reuse(true));
        let with_dns = dns::tokio::Transport::system(tcp)?;
        Ok(with_dns
            .upgrade(upgrade::Version::V1)
            .authenticate(noise::Config::new(local_key).unwrap())
            .multiplex(yamux::Config::default())
            .timeout(config::PEER_TIMEOUT)
            .map(|(peer, muxer), _| (peer, muxing::StreamMuxerBox::new(muxer)))
            .boxed())
    }
}

#[cfg(test)]
mod test {
    use std::{net::Ipv4Addr, time::Duration};

    use super::*;
    use crate::{
        client_events::test::MemoryEventsGen,
        contract::MemoryContractHandler,
        node::{testing_impl::get_free_port, InitPeerNode},
        ring::Location,
    };

    use futures::StreamExt;
    use tokio::sync::watch::channel;

    /// Ping test event loop
    async fn ping_ev_loop(peer: &mut NodeP2P) -> Result<(), ()> {
        loop {
            let ev = tokio::time::timeout(
                Duration::from_secs(30),
                peer.conn_manager.swarm.select_next_some(),
            );
            match ev.await {
                Ok(other) => {
                    tracing::debug!("{:?}", other)
                }
                Err(_) => {
                    return Err(());
                }
            }
        }
    }

    #[ignore]
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn ping() -> Result<(), ()> {
        let peer1_port = get_free_port().unwrap();
        let peer1_key = Keypair::generate_ed25519();
        let peer1_id: Libp2pPeerId = peer1_key.public().into();
        let peer1_config = InitPeerNode::new(peer1_id, Location::random())
            .listening_ip(Ipv4Addr::LOCALHOST)
            .listening_port(peer1_port);

        let peer2_key = Keypair::generate_ed25519();
        let peer2_id: Libp2pPeerId = peer2_key.public().into();

        let (_, receiver1) = channel((0, FreenetPeerId::from(peer1_id)));
        let (_, receiver2) = channel((0, FreenetPeerId::from(peer2_id)));

        // Start up the initial node.
        GlobalExecutor::spawn(async move {
            let user_events = MemoryEventsGen::new(receiver1, FreenetPeerId::from(peer1_id));
            let mut config = NodeConfig::new();
            config
                .with_ip(Ipv4Addr::LOCALHOST)
                .with_port(peer1_port)
                .with_key(peer1_key.public().into());
            let mut peer1 = Box::new(
                NodeP2P::build::<MemoryContractHandler, 1, _>(
                    config,
                    peer1_key,
                    [Box::new(user_events)],
                    crate::tracing::TestEventListener::new().await,
                    "ping-listener".into(),
                )
                .await?,
            );
            peer1.conn_manager.listen_on()?;
            ping_ev_loop(&mut peer1).await.unwrap();
            Ok::<_, anyhow::Error>(())
        });

        // Start up the dialing node
        let dialer = GlobalExecutor::spawn(async move {
            let user_events = MemoryEventsGen::new(receiver2, FreenetPeerId::from(peer2_id));
            let mut config = NodeConfig::new();
            config
                .add_gateway(peer1_config.clone())
                .with_key(peer2_key.public().into());
            let mut peer2 = NodeP2P::build::<MemoryContractHandler, 1, _>(
                config,
                peer2_key,
                [Box::new(user_events)],
                crate::tracing::TestEventListener::new().await,
                "ping-dialer".into(),
            )
            .await
            .unwrap();
            // wait a bit to make sure the first peer is up and listening
            tokio::time::sleep(Duration::from_millis(10)).await;
            peer2
                .conn_manager
                .swarm
                .dial(peer1_config.addr.unwrap())
                .map_err(|_| ())?;
            ping_ev_loop(&mut peer2).await
        });

        dialer.await.map_err(|_| ())?
    }
}
