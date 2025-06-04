use std::{collections::HashSet, convert::Infallible, sync::Arc, time::Duration};

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
    message::{NetMessage, NodeEvent, Transaction},
    node::NodeConfig,
    operations::{connect, OpEnum},
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
        let min_connections = self.op_manager.ring.connection_manager.min_connections;

        tracing::info!(
            "Starting aggressive connection acquisition phase (target: {} connections)",
            min_connections
        );

        // For small networks, we want to ensure all nodes discover each other quickly
        // to avoid the 10+ second delays on first GET operations
        let start = std::time::Instant::now();
        let max_duration = Duration::from_secs(10);
        let mut last_connection_count = 0;
        let mut stable_rounds = 0;

        while start.elapsed() < max_duration {
            let current_connections = self.op_manager.ring.open_connections();

            // If we've reached our target, we're done
            if current_connections >= min_connections {
                tracing::info!(
                    "Reached minimum connections target: {}/{}",
                    current_connections,
                    min_connections
                );
                break;
            }

            // If connection count is stable for 3 rounds, actively trigger more connections
            if current_connections == last_connection_count {
                stable_rounds += 1;
                if stable_rounds >= 3 && current_connections > 0 {
                    tracing::info!(
                        "Connection count stable at {}, triggering active peer discovery",
                        current_connections
                    );

                    // Trigger the connection maintenance task to actively look for more peers
                    // In small networks, we want to be more aggressive
                    for _ in 0..3 {
                        if let Err(e) = self.trigger_connection_maintenance().await {
                            tracing::warn!("Failed to trigger connection maintenance: {}", e);
                        }
                        tokio::time::sleep(Duration::from_millis(100)).await;
                    }
                    stable_rounds = 0;
                }
            } else {
                stable_rounds = 0;
                last_connection_count = current_connections;
            }

            tracing::debug!(
                "Current connections: {}/{}, waiting for more peers (elapsed: {}s)",
                current_connections,
                min_connections,
                start.elapsed().as_secs()
            );

            // Check more frequently at the beginning
            let sleep_duration = if start.elapsed() < Duration::from_secs(3) {
                Duration::from_millis(500)
            } else {
                Duration::from_secs(1)
            };
            tokio::time::sleep(sleep_duration).await;
        }

        let final_connections = self.op_manager.ring.open_connections();
        tracing::info!(
            "Aggressive connection phase complete. Final connections: {}/{} (took {}s)",
            final_connections,
            min_connections,
            start.elapsed().as_secs()
        );
    }

    /// Trigger the connection maintenance task to actively look for more peers
    async fn trigger_connection_maintenance(&self) -> anyhow::Result<()> {
        // Send a connect request to find more peers
        use crate::operations::connect;
        let ideal_location = Location::random();
        let tx = Transaction::new::<connect::ConnectMsg>();

        // Find a connected peer to query
        let query_target = {
            let router = self.op_manager.ring.router.read();
            self.op_manager.ring.connection_manager.routing(
                ideal_location,
                None,
                &HashSet::<PeerId>::new(),
                &router,
            )
        };

        if let Some(query_target) = query_target {
            let joiner = self.op_manager.ring.connection_manager.own_location();
            let msg = connect::ConnectMsg::Request {
                id: tx,
                target: query_target.clone(),
                msg: connect::ConnectRequest::FindOptimalPeer {
                    query_target,
                    ideal_location,
                    joiner,
                    max_hops_to_live: self.op_manager.ring.max_hops_to_live,
                    skip_connections: HashSet::new(),
                    skip_forwards: HashSet::new(),
                },
            };

            self.op_manager
                .notify_op_change(
                    NetMessage::from(msg),
                    OpEnum::Connect(Box::new(connect::ConnectOp::new(tx, None, None, None))),
                )
                .await?;
        }

        Ok(())
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
