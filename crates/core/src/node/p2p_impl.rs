use std::{convert::Infallible, sync::Arc, time::Duration};

use futures::{future::BoxFuture, FutureExt};
use tracing::Instrument;

use super::{
    network_bridge::{
        event_loop_notification_channel, p2p_protoc::P2pConnManager, EventLoopNotificationsReceiver,
    },
    NetEventRegister, PeerId,
};
use crate::{
    client_events::client_event_handling,
    ring::{ConnectionManager, Location},
};
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

pub(crate) struct NodeP2P {
    pub(crate) op_manager: Arc<OpManager>,
    pub(super) conn_manager: P2pConnManager,
    pub(super) peer_id: Option<PeerId>,
    pub(super) is_gateway: bool,
    /// used for testing with deterministic location
    pub(super) location: Option<Location>,
    notification_channel: EventLoopNotificationsReceiver,
    client_wait_for_transaction: ContractHandlerChannel<WaitingResolution>,
    executor_listener: ExecutorToEventLoopChannel<NetworkEventListenerHalve>,
    cli_response_sender: ClientResponsesSender,
    node_controller: tokio::sync::mpsc::Receiver<NodeEvent>,
    should_try_connect: bool,
    client_events_task: BoxFuture<'static, anyhow::Error>,
    contract_executor_task: BoxFuture<'static, anyhow::Error>,
}

impl NodeP2P {
    /// Aggressively establish connections during startup to avoid on-demand delays
    async fn aggressive_initial_connections(&self) {
        let min_connections = self.op_manager
            .ring
            .connection_manager
            .min_connections;
            
        tracing::info!(
            "Starting aggressive connection acquisition phase (target: {} connections)",
            min_connections
        );
        
        // Try for up to 10 seconds to establish minimum connections
        let start = std::time::Instant::now();
        let max_duration = Duration::from_secs(10);
        
        while start.elapsed() < max_duration {
            let current_connections = self.op_manager.ring.open_connections();
            
            if current_connections >= min_connections {
                tracing::info!(
                    "Reached minimum connections target: {}/{}",
                    current_connections,
                    min_connections
                );
                break;
            }
            
            tracing::debug!(
                "Current connections: {}/{}, waiting for connection maintenance to acquire more",
                current_connections,
                min_connections
            );
            
            // The connection maintenance task will handle establishing connections
            // We just wait and monitor progress
            tokio::time::sleep(Duration::from_secs(2)).await;
        }
        
        let final_connections = self.op_manager.ring.open_connections();
        tracing::info!(
            "Aggressive connection phase complete. Final connections: {}/{}",
            final_connections,
            min_connections
        );
    }
    pub(super) async fn run_node(self) -> anyhow::Result<Infallible> {
        if self.should_try_connect {
            connect::initial_join_procedure(self.op_manager.clone(), &self.conn_manager.gateways)
                .await?;
            
            // After connecting to gateways, aggressively try to reach min_connections
            // This is important for fast startup and avoiding on-demand connection delays
            self.aggressive_initial_connections().await;
        }

        let f = self.conn_manager.run_event_listener(
            self.op_manager.clone(),
            self.client_wait_for_transaction,
            self.notification_channel,
            self.executor_listener,
            self.cli_response_sender,
            self.node_controller,
        );

        tokio::select!(
            r = f => {
               let Err(e) = r;
               Err(e)
            }
            e = self.client_events_task => {
                Err(e)
            }
            e = self.contract_executor_task => {
                Err(e)
            }
        )
    }

    pub(crate) async fn build<CH, const CLIENTS: usize, ER>(
        config: NodeConfig,
        clients: [BoxedClient; CLIENTS],
        event_register: ER,
        ch_builder: CH::Builder,
    ) -> anyhow::Result<Self>
    where
        CH: ContractHandler + Send + 'static,
        ER: NetEventRegister + Clone,
    {
        let (notification_channel, notification_tx) = event_loop_notification_channel();
        let (ch_outbound, ch_inbound, wait_for_event) = contract::contract_handler_channel();
        let (client_responses, cli_response_sender) = contract::client_responses_channel();

        let connection_manager = ConnectionManager::new(&config);
        let op_manager = Arc::new(OpManager::new(
            notification_tx,
            ch_outbound,
            &config,
            event_register.clone(),
            connection_manager,
        )?);
        let (executor_listener, executor_sender) = contract::executor_channel(op_manager.clone());
        let contract_handler = CH::build(ch_inbound, executor_sender, ch_builder)
            .await
            .map_err(|e| anyhow::anyhow!(e))?;

        let conn_manager =
            P2pConnManager::build(&config, op_manager.clone(), event_register).await?;

        let parent_span = tracing::Span::current();
        let contract_executor_task = GlobalExecutor::spawn(
            contract::contract_handling(contract_handler)
                .instrument(tracing::info_span!(parent: parent_span.clone(), "contract_handling")),
        )
        .map(|r| match r {
            Ok(Err(e)) => anyhow::anyhow!("Error in contract handling task: {e}"),
            Ok(Ok(_)) => anyhow::anyhow!("Contract handling task exited unexpectedly"),
            Err(e) => anyhow::anyhow!(e),
        })
        .boxed();
        let clients = ClientEventsCombinator::new(clients);
        let (node_controller_tx, node_controller_rx) = tokio::sync::mpsc::channel(1);
        let client_events_task = GlobalExecutor::spawn(
            client_event_handling(
                op_manager.clone(),
                clients,
                client_responses,
                node_controller_tx,
            )
            .instrument(tracing::info_span!(parent: parent_span, "client_event_handling")),
        )
        .map(|r| match r {
            Ok(_) => anyhow::anyhow!("Client event handling task exited unexpectedly"),
            Err(e) => anyhow::anyhow!(e),
        })
        .boxed();

        Ok(NodeP2P {
            conn_manager,
            notification_channel,
            client_wait_for_transaction: wait_for_event,
            op_manager,
            executor_listener,
            cli_response_sender,
            node_controller: node_controller_rx,
            should_try_connect: config.should_connect,
            peer_id: config.peer_id,
            is_gateway: config.is_gateway,
            location: config.location,
            client_events_task,
            contract_executor_task,
        })
    }
}
