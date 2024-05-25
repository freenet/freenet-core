use std::sync::Arc;

use tracing::Instrument;

use super::{
    client_event_handling,
    network_bridge::{
        event_loop_notification_channel, p2p_protoc::P2pConnManager, EventLoopNotificationsReceiver,
    },
    NetEventRegister,
};
use crate::transport::TransportPublicKey;
use crate::{
    client_events::{combinator::ClientEventsCombinator, BoxedClient},
    config::GlobalExecutor,
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
    pub(crate) peer_pub_key: TransportPublicKey,
    pub(crate) op_manager: Arc<OpManager>,
    notification_channel: EventLoopNotificationsReceiver,
    client_wait_for_transaction: ContractHandlerChannel<WaitingResolution>,
    pub(super) conn_manager: P2pConnManager,
    executor_listener: ExecutorToEventLoopChannel<NetworkEventListenerHalve>,
    cli_response_sender: ClientResponsesSender,
    node_controller: tokio::sync::mpsc::Receiver<NodeEvent>,
    should_try_connect: bool,
}

impl NodeP2P {
    pub(super) async fn run_node(self) -> Result<(), anyhow::Error> {
        if self.should_try_connect {
            connect::initial_join_procedure(
                self.op_manager.clone(),
                self.conn_manager.bridge.clone(),
                self.peer_pub_key,
                &self.conn_manager.gateways,
            )
            .await?;
        }

        // start the p2p event loop
        self.conn_manager
            .run_event_listener(
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
        clients: [BoxedClient; CLIENTS],
        event_register: ER,
        ch_builder: CH::Builder,
    ) -> Result<NodeP2P, anyhow::Error>
    where
        CH: ContractHandler + Send + 'static,
        ER: NetEventRegister + Clone,
    {
        let peer_pub_key = config.key_pair.public().clone();

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

        let conn_manager =
            P2pConnManager::build(&config, op_manager.clone(), event_register).await?;

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
            peer_pub_key,
            conn_manager,
            notification_channel,
            client_wait_for_transaction: wait_for_event,
            op_manager,
            executor_listener,
            cli_response_sender,
            node_controller: node_controller_rx,
            should_try_connect: config.should_connect,
        })
    }
}
