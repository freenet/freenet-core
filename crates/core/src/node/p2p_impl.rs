use std::{convert::Infallible, sync::Arc};

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
    pub(super) async fn run_node(self) -> anyhow::Result<Infallible> {
        // Save what we need for connections before moving self
        let should_try_connect = self.should_try_connect;
        let gateways = self.conn_manager.gateways.clone();
        let op_manager_for_connect = self.op_manager.clone();

        // Create a channel to signal when the event listener is ready
        let (ready_tx, ready_rx) = tokio::sync::oneshot::channel();

        // Start event listener in background with ready signal
        let event_listener_handle = {
            let op_manager = self.op_manager.clone();
            let conn_manager = self.conn_manager;
            let client_wait_for_transaction = self.client_wait_for_transaction;
            let notification_channel = self.notification_channel;
            let executor_listener = self.executor_listener;
            let cli_response_sender = self.cli_response_sender;
            let node_controller = self.node_controller;

            GlobalExecutor::spawn(async move {
                conn_manager
                    .run_event_listener(
                        op_manager,
                        client_wait_for_transaction,
                        notification_channel,
                        executor_listener,
                        cli_response_sender,
                        node_controller,
                        Some(ready_tx),
                    )
                    .await
            })
        };

        // Wait for event listener to signal it's ready
        if ready_rx.await.is_ok() {
            // Now it's safe to initiate connections
            if should_try_connect {
                GlobalExecutor::spawn(async move {
                    if let Err(e) =
                        connect::initial_join_procedure(op_manager_for_connect.clone(), &gateways)
                            .await
                    {
                        tracing::error!("Initial join procedure failed: {}", e);
                    }

                    // After initial join, trigger more aggressive connection attempts
                    // The ring's connection maintenance task will handle this, but we can
                    // also trigger it manually for faster startup
                    tracing::info!(
                        "Initial join complete, connection maintenance will find more peers"
                    );

                    // For now, use the connection maintenance to avoid overwhelming the network
                    // aggressive_initial_connections would be better but needs access to self
                });
            }
        }

        tokio::select!(
            r = event_listener_handle => {
               match r {
                   Ok(Err(e)) => Err(e),
                   Ok(Ok(_)) => unreachable!("Event listener should not return Ok"),
                   Err(e) => Err(e.into()),
               }
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
