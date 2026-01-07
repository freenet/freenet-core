use std::{convert::Infallible, sync::Arc, time::Duration};

use futures::{future::BoxFuture, FutureExt};
use tokio::task::JoinHandle;
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
        self, mediator_channels, run_op_request_mediator, ContractHandler, ContractHandlerChannel,
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
    node_controller: tokio::sync::mpsc::Receiver<NodeEvent>,
    should_try_connect: bool,
    client_events_task: BoxFuture<'static, anyhow::Error>,
    contract_executor_task: BoxFuture<'static, anyhow::Error>,
    initial_join_task: Option<JoinHandle<()>>,
}

impl NodeP2P {
    /// Aggressively wait for connections during startup to avoid on-demand delays.
    /// This is an associated function that can be spawned as a task to run concurrently
    /// with the event listener. Without the event listener running, connection
    /// handshakes won't be processed.
    async fn aggressive_initial_connections_impl(
        op_manager: &Arc<OpManager>,
        min_connections: usize,
    ) {
        tracing::info!(
            "Starting aggressive connection acquisition phase (target: {} connections)",
            min_connections
        );

        // For small networks, we want to ensure all nodes discover each other quickly
        // to avoid the 10+ second delays on first GET operations
        let start = tokio::time::Instant::now();
        let max_duration = Duration::from_secs(10);
        let mut last_connection_count = 0;

        while start.elapsed() < max_duration {
            // Cooperative yielding for CI environments with limited CPU cores
            // This is critical - the event listener needs CPU time to process handshakes
            tokio::task::yield_now().await;

            let current_connections = op_manager.ring.open_connections();

            // If we've reached our target, we're done
            if current_connections >= min_connections {
                tracing::info!(
                    "Reached minimum connections target: {}/{}",
                    current_connections,
                    min_connections
                );
                break;
            }

            // Log progress when connection count changes
            if current_connections != last_connection_count {
                tracing::info!(
                    "Connection progress: {}/{} (elapsed: {}s)",
                    current_connections,
                    min_connections,
                    start.elapsed().as_secs()
                );
                last_connection_count = current_connections;
            } else {
                tracing::debug!(
                    "Current connections: {}/{}, waiting for more peers (elapsed: {}s)",
                    current_connections,
                    min_connections,
                    start.elapsed().as_secs()
                );
            }

            // Check more frequently at the beginning to detect quick connections
            let sleep_duration = if start.elapsed() < Duration::from_secs(3) {
                Duration::from_millis(250)
            } else {
                Duration::from_millis(500)
            };
            tokio::time::sleep(sleep_duration).await;
        }

        let final_connections = op_manager.ring.open_connections();
        tracing::info!(
            "Aggressive connection phase complete. Final connections: {}/{} (took {}s)",
            final_connections,
            min_connections,
            start.elapsed().as_secs()
        );
    }

    pub(super) async fn run_node(mut self) -> anyhow::Result<Infallible> {
        // Record the start time for uptime tracking in shutdown event
        let start_time = tokio::time::Instant::now();

        // Emit peer startup event
        if let Some(event) = crate::tracing::NetEventLog::peer_startup(
            &self.op_manager.ring,
            crate::config::PCK_VERSION.to_string(),
            None, // git_commit - not available in library, only in binary
            None, // git_dirty - not available in library, only in binary
        ) {
            use either::Either;
            self.op_manager
                .ring
                .register_events(Either::Left(event))
                .await;
            tracing::info!(
                version = crate::config::PCK_VERSION,
                is_gateway = self.op_manager.ring.is_gateway(),
                "Peer startup event emitted"
            );
        }

        if self.should_try_connect {
            let join_handle = connect::initial_join_procedure(
                self.op_manager.clone(),
                &self.conn_manager.gateways,
            )
            .await?;
            self.initial_join_task = Some(join_handle);

            // Note: We don't run aggressive_initial_connections here because
            // the event listener hasn't started yet. The connect requests from
            // initial_join_procedure are queued but won't be processed until
            // the event listener runs. Instead, we'll run the aggressive
            // connection phase concurrently with the event listener below.
        }

        // Spawn aggressive connection task to run concurrently with event listener.
        // This is needed because connection handshakes are processed by the event
        // listener, so we can't block waiting for connections before it starts.
        let aggressive_conn_task = if self.should_try_connect {
            let op_manager = self.op_manager.clone();
            let min_connections = op_manager.ring.connection_manager.min_connections;
            Some(GlobalExecutor::spawn(async move {
                Self::aggressive_initial_connections_impl(&op_manager, min_connections).await;
            }))
        } else {
            None
        };

        let f = self.conn_manager.run_event_listener(
            self.op_manager.clone(),
            self.client_wait_for_transaction,
            self.notification_channel,
            self.executor_listener,
            self.node_controller,
        );

        let join_task = self.initial_join_task.take();
        let result = tokio::select! {
            biased;
            r = f => {
               let Err(e) = r;
               tracing::error!("Network event listener exited: {}", e);
               Err(e)
            }
            e = self.client_events_task => {
                tracing::error!("Client events task exited: {:?}", e);
                Err(e)
            }
            e = self.contract_executor_task => {
                tracing::error!("Contract executor task exited: {:?}", e);
                Err(e)
            }
        };

        if let Some(handle) = join_task {
            handle.abort();
        }
        if let Some(handle) = aggressive_conn_task {
            handle.abort();
        }

        // Emit peer shutdown event
        let (graceful, reason) = match &result {
            Ok(_) => (true, None),
            Err(e) => (false, Some(e.to_string())),
        };
        if let Some(event) = crate::tracing::NetEventLog::peer_shutdown(
            &self.op_manager.ring,
            graceful,
            reason.clone(),
            start_time,
        ) {
            use either::Either;
            self.op_manager
                .ring
                .register_events(Either::Left(event))
                .await;
            tracing::info!(
                graceful,
                reason = reason.as_deref().unwrap_or("clean exit"),
                uptime_secs = start_time.elapsed().as_secs(),
                "Peer shutdown event emitted"
            );
        }

        result
    }

    /// Build a new node and return it along with a shutdown sender.
    ///
    /// The shutdown sender can be used to trigger graceful shutdown by sending
    /// `NodeEvent::Disconnect`.
    pub(crate) async fn build<CH, const CLIENTS: usize, ER>(
        config: NodeConfig,
        clients: [BoxedClient; CLIENTS],
        event_register: ER,
        ch_builder: CH::Builder,
    ) -> anyhow::Result<(Self, tokio::sync::mpsc::Sender<NodeEvent>)>
    where
        CH: ContractHandler + Send + 'static,
        ER: NetEventRegister + Clone,
    {
        let (notification_channel, notification_tx) = event_loop_notification_channel();
        let (mut ch_outbound, ch_inbound, wait_for_event) = contract::contract_handler_channel();
        let (client_responses, cli_response_sender) = contract::client_responses_channel();

        // Prepare session adapter channel for actor-based client management
        let (session_tx, session_rx) = tokio::sync::mpsc::channel(1000);

        // Install session adapter in contract handler
        ch_outbound.with_session_adapter(session_tx.clone());

        // Create result router channel for dual-path result delivery
        let (result_router_tx, result_router_rx) = tokio::sync::mpsc::channel(1000);

        // Spawn Session Actor
        use crate::client_events::session_actor::SessionActor;
        let session_actor = SessionActor::new(session_rx, cli_response_sender.clone());
        GlobalExecutor::spawn(async move {
            tracing::info!("Session actor starting");
            session_actor.run().await;
            tracing::warn!("Session actor stopped");
        });

        // Spawn ResultRouter task
        use crate::client_events::result_router::ResultRouter;
        let router = ResultRouter::new(result_router_rx, session_tx.clone());
        GlobalExecutor::spawn(async move {
            tracing::info!("Result router starting");
            router.run().await;
            tracing::warn!("Result router stopped");
        });

        tracing::info!("Actor-based client management infrastructure installed with result router");

        let connection_manager = ConnectionManager::new(&config);
        let op_manager = Arc::new(OpManager::new(
            notification_tx,
            ch_outbound,
            &config,
            event_register.clone(),
            connection_manager,
            result_router_tx,
        )?);
        op_manager.ring.attach_op_manager(&op_manager);

        // Create channels for the mediator pattern:
        // - op_request_channel: executors send (Transaction, oneshot::Sender) to mediator
        // - mediator_channels: mediator forwards Transaction to event loop and routes responses back
        let (op_request_receiver, op_sender) = contract::op_request_channel();
        let (executor_listener, to_event_loop_tx, from_event_loop_rx) =
            mediator_channels(op_manager.clone());

        // Spawn the mediator task that bridges between the pooled executors and the event loop
        GlobalExecutor::spawn({
            let mediator_task =
                run_op_request_mediator(op_request_receiver, to_event_loop_tx, from_event_loop_rx);
            mediator_task.instrument(tracing::info_span!("op_request_mediator"))
        });

        let contract_handler = CH::build(ch_inbound, op_sender, op_manager.clone(), ch_builder)
            .await
            .map_err(|e| anyhow::anyhow!(e))?;

        // Create MessageProcessor - direct to SessionActor
        use crate::node::MessageProcessor;
        let message_processor = Arc::new(MessageProcessor::new(session_tx.clone()));

        let conn_manager = P2pConnManager::build(
            &config,
            op_manager.clone(),
            event_register,
            message_processor,
        )
        .await?;
        tracing::info!("P2P layer configured with MessageProcessor - network processing decoupled from client handling");

        let parent_span = tracing::Span::current();
        let contract_executor_task = GlobalExecutor::spawn({
            let task = async move {
                tracing::info!("Contract executor task starting");
                let result = contract::contract_handling(contract_handler).await;
                match &result {
                    Ok(_) => tracing::warn!("Contract executor task exiting normally (unexpected)"),
                    Err(e) => tracing::error!("Contract executor task exiting with error: {e}"),
                }
                result
            };
            task.instrument(tracing::info_span!(parent: parent_span.clone(), "contract_handling"))
        })
        .map(|r| match r {
            Ok(Err(e)) => anyhow::anyhow!("Error in contract handling task: {e}"),
            Ok(Ok(_)) => anyhow::anyhow!("Contract handling task exited unexpectedly"),
            Err(e) => anyhow::anyhow!(e),
        })
        .boxed();
        let clients = ClientEventsCombinator::new(clients);
        // Create node controller channel with capacity for shutdown signal
        // We clone the sender to return it for external shutdown triggering
        let (node_controller_tx, node_controller_rx) = tokio::sync::mpsc::channel(1);
        let shutdown_tx = node_controller_tx.clone();
        let client_events_task = GlobalExecutor::spawn({
            let op_manager_clone = op_manager.clone();
            let task = async move {
                tracing::info!("Client events task starting");
                let result = client_event_handling(
                    op_manager_clone,
                    clients,
                    client_responses,
                    node_controller_tx,
                )
                .await;
                tracing::warn!("Client events task exiting (unexpected)");
                result
            };
            task.instrument(tracing::info_span!(parent: parent_span, "client_event_handling"))
        })
        .map(|r| match r {
            Ok(_) => anyhow::anyhow!("Client event handling task exited unexpectedly"),
            Err(e) => anyhow::anyhow!(e),
        })
        .boxed();

        Ok((
            NodeP2P {
                conn_manager,
                notification_channel,
                client_wait_for_transaction: wait_for_event,
                op_manager,
                executor_listener,
                node_controller: node_controller_rx,
                should_try_connect: config.should_connect,
                peer_id: None, // PeerId removed - using PeerKeyLocation instead
                is_gateway: config.is_gateway,
                location: config.location,
                client_events_task,
                contract_executor_task,
                initial_join_task: None,
            },
            shutdown_tx,
        ))
    }
}
