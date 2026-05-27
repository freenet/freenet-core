use std::{convert::Infallible, net::SocketAddr, sync::Arc, time::Duration};

use futures::{FutureExt, future::BoxFuture};
use tokio::task::JoinHandle;
use tracing::Instrument;

use super::{
    NetEventRegister, PeerId,
    network_bridge::{
        EventLoopNotificationsReceiver, event_loop_notification_channel_with_capacity,
        p2p_protoc::P2pConnManager,
    },
};
use crate::{
    client_events::client_event_handling,
    ring::{ConnectionManager, Location},
};
use crate::{
    client_events::{BoxedClient, combinator::ClientEventsCombinator},
    config::GlobalExecutor,
    contract::{self, ContractHandler, ContractHandlerChannel, WaitingResolution},
    message::NodeEvent,
    node::NodeConfig,
    operations::connect,
};

use super::{OpManager, background_task_monitor::BackgroundTaskMonitor};

pub(crate) struct NodeP2P {
    pub(crate) op_manager: Arc<OpManager>,
    pub(super) conn_manager: P2pConnManager,
    pub(super) peer_id: Option<PeerId>,
    pub(super) is_gateway: bool,
    /// used for testing with deterministic location
    pub(super) location: Option<Location>,
    notification_channel: EventLoopNotificationsReceiver,
    client_wait_for_transaction: ContractHandlerChannel<WaitingResolution>,
    node_controller: tokio::sync::mpsc::Receiver<NodeEvent>,
    should_try_connect: bool,
    client_events_task: BoxFuture<'static, anyhow::Error>,
    contract_executor_task: BoxFuture<'static, anyhow::Error>,
    initial_join_task: Option<JoinHandle<()>>,
    session_actor_task: JoinHandle<()>,
    result_router_task: JoinHandle<()>,
    /// Monitor for background tasks spawned during node construction (Ring, OpManager, etc.)
    background_task_monitor: BackgroundTaskMonitor,
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

        // Initialize network status tracking for the connecting page diagnostics
        let gateway_addrs: std::collections::HashSet<std::net::SocketAddr> = self
            .conn_manager
            .gateways
            .iter()
            .filter_map(|g| g.socket_addr())
            .collect();
        super::network_status::init(
            self.conn_manager.listening_port(),
            gateway_addrs,
            crate::config::PCK_VERSION.to_string(),
        );
        // Wire the dashboard's subscription view directly to canonical
        // ring state so the panel doesn't drift the way the legacy
        // `record_subscription` mirror did.
        let ring = self.op_manager.ring.clone();
        super::network_status::set_subscription_provider(std::sync::Arc::new({
            let ring = ring.clone();
            move || ring.dashboard_subscription_snapshot()
        }));
        // Same pattern for the per-contract governance snapshot —
        // dashboard reads live state from the GovernanceManager
        // populated by the meter and HostingManager wiring.
        super::network_status::set_governance_provider(std::sync::Arc::new(move || {
            ring.dashboard_governance_snapshot()
        }));

        // Wire live ring stats for the dashboard: connection count +
        // hosted contracts + own public key, read on every homepage
        // request.
        let ring_stats = self.op_manager.ring.clone();
        super::network_status::set_ring_stats_provider(std::sync::Arc::new(move || {
            let (peer_id, own_pub_key) = {
                let pk = ring_stats.connection_manager.own_location();
                let key_bytes = pk.pub_key().as_bytes();
                let peer_id = pk.pub_key().to_string(); // 12-byte Display
                let own_pub_key = bs58::encode(key_bytes).into_string(); // full 32-byte
                (peer_id, own_pub_key)
            };
            super::network_status::RingStatsSnapshot {
                connection_count: ring_stats.connection_manager.connection_count() as u32,
                hosted_contracts: ring_stats.hosting_contracts_count() as u32,
                peer_id,
                own_pub_key,
            }
        }));

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
            self.node_controller,
        );

        // Monitor spawned infrastructure tasks (session actor, result router).
        // If any of these panics or exits unexpectedly, the node runs degraded with no
        // logs or detection. Combine into a single future that produces an error.
        // Keep AbortHandles for cleanup since the JoinHandles are moved into the future.
        let session_abort = self.session_actor_task.abort_handle();
        let router_abort = self.result_router_task.abort_handle();
        let infra_monitor = {
            let mut session_handle = self.session_actor_task;
            let mut router_handle = self.result_router_task;
            async move {
                fn join_result_to_error(
                    name: &str,
                    r: Result<(), tokio::task::JoinError>,
                ) -> anyhow::Error {
                    match r {
                        Err(e) if e.is_panic() => anyhow::anyhow!("{name} panicked: {e}"),
                        Err(e) => anyhow::anyhow!("{name} task failed: {e}"),
                        Ok(()) => anyhow::anyhow!("{name} exited unexpectedly"),
                    }
                }
                let e: anyhow::Error = tokio::select! {
                    biased;
                    r = &mut session_handle => join_result_to_error("Session actor", r),
                    r = &mut router_handle => join_result_to_error("Result router", r),
                };
                e
            }
        };

        // Monitor background tasks registered during node construction
        // (Ring maintenance, garbage cleanup, etc.)
        let background_monitor = self.background_task_monitor.wait_for_any_exit();

        let join_task = self.initial_join_task.take();
        let result = crate::deterministic_select! {
            r = f => {
               let Err(e) = r;
               eprintln!("CRITICAL: Network event listener exited: {e}");
               tracing::error!("Network event listener exited: {}", e);
               Err(e)
            },
            e = self.client_events_task => {
                eprintln!("CRITICAL: Client events task exited: {e}");
                tracing::error!("Client events task exited: {:?}", e);
                Err(e)
            },
            e = self.contract_executor_task => {
                eprintln!("CRITICAL: Contract executor task exited: {e}");
                tracing::error!("Contract executor task exited: {:?}", e);
                Err(e)
            },
            e = infra_monitor => {
                eprintln!("CRITICAL: Infrastructure task exited: {e}");
                tracing::error!("Infrastructure task exited: {:?}", e);
                Err(e)
            },
            e = background_monitor => {
                eprintln!("CRITICAL: Background task exited: {e}");
                tracing::error!("Background task exited: {:?}", e);
                Err(e)
            },
        };

        if let Some(handle) = join_task {
            handle.abort();
        }
        if let Some(handle) = aggressive_conn_task {
            handle.abort();
        }
        session_abort.abort();
        router_abort.abort();

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
        let channel_capacity = config.config.network_api.event_loop_channel_capacity;
        let (notification_channel, notification_tx) =
            event_loop_notification_channel_with_capacity(channel_capacity);
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
        let session_actor_task = GlobalExecutor::spawn(async move {
            tracing::info!("Session actor starting");
            session_actor.run().await;
            tracing::warn!("Session actor stopped");
        });

        // Spawn ResultRouter task
        use crate::client_events::result_router::ResultRouter;
        let router = ResultRouter::new(result_router_rx, session_tx.clone());
        let result_router_task = GlobalExecutor::spawn(async move {
            tracing::info!("Result router starting");
            router.run().await;
            tracing::warn!("Result router stopped");
        });

        tracing::info!("Actor-based client management infrastructure installed with result router");

        let background_task_monitor = BackgroundTaskMonitor::new();
        // Phase 1 / Phase 1.5 of the outer-loop rate-controller RFC
        // (#4074): start the cross-connection RTT shadow aggregator
        // and the out-of-band reference-path probe. Both are pure
        // observation — never read by the production data path.
        //
        // The local peer id is tagged onto every emitted event so the
        // collector can disaggregate samples by reporting node. We
        // construct it as `PeerId::new(pub_key, addr).to_string()` to
        // match the *format* used by other OTLP events (set elsewhere
        // from `KnownPeerKeyLocation::Display`).
        //
        // Caveat: for non-gateway nodes the external `own_addr` is
        // not known at build time, so we fall back to the listener
        // address (typically `0.0.0.0:<port>`). Other OTLP events
        // emitted later read the up-to-date `Ring::own_location()`,
        // so cross-correlation with the rest of the event stream by
        // peer_id alone works for gateways but is best-effort for
        // leaf nodes until they learn their external address. Phase
        // 1.5 accepts this; a refresh path is tracked in #4294.
        //
        // Gate on telemetry being enabled: when telemetry is opt-out
        // (`telemetry-enabled = false`) or in test environments
        // (detected by `--id` flag), `TelemetryReporter::new` returns
        // `None` and `send_standalone_event_with_peer_id` silently
        // drops events. The shadow aggregator is cheap to leave
        // running (no I/O), but reference-ping issues a real UDP DNS
        // query at 1Hz, so we must not fire that traffic when
        // telemetry is disabled — that would surprise opt-out
        // operators AND flood Cloudflare with redundant queries from
        // every CI simulation node.
        let telemetry_enabled =
            config.config.telemetry.enabled && !config.config.telemetry.is_test_environment;
        let listen_addr = config.own_addr.unwrap_or_else(|| {
            SocketAddr::new(config.network_listener_ip, config.network_listener_port)
        });
        let local_peer_id =
            crate::node::PeerId::new(config.key_pair.public().clone(), listen_addr).to_string();
        crate::transport::rolling_rtt_stats::spawn_aggregator(
            local_peer_id.clone(),
            &background_task_monitor,
        );
        if telemetry_enabled {
            crate::transport::reference_ping::spawn_reference_ping(
                local_peer_id,
                crate::transport::reference_ping::DEFAULT_REFERENCE_TARGET,
                &background_task_monitor,
            );
        }
        let connection_manager = ConnectionManager::new(&config);
        let op_manager = Arc::new(OpManager::new(
            notification_tx,
            ch_outbound,
            &config,
            event_register.clone(),
            connection_manager,
            result_router_tx,
            &background_task_monitor,
        )?);
        op_manager.ring.attach_op_manager(&op_manager);

        let contract_handler = CH::build(ch_inbound, op_manager.clone(), ch_builder)
            .await
            .map_err(|e| anyhow::anyhow!(e))?;

        let conn_manager =
            P2pConnManager::build(&config, op_manager.clone(), event_register).await?;

        let parent_span = tracing::Span::current();
        let contract_executor_task = GlobalExecutor::spawn({
            let task = async move {
                tracing::info!("Contract executor task starting");
                let result = contract::contract_handling(
                    contract_handler,
                    crate::contract::user_input::DashboardPrompter::new(
                        crate::contract::user_input::pending_prompts(),
                    ),
                )
                .await;
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
        // Slot 0 = HTTP client API, Slot 1 = WebSocket proxy (from serve_client_api).
        let clients = ClientEventsCombinator::new(clients).with_slot_names(&["http", "websocket"]);
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
                node_controller: node_controller_rx,
                should_try_connect: config.should_connect,
                peer_id: None, // PeerId removed - using PeerKeyLocation instead
                is_gateway: config.is_gateway,
                location: config.location,
                client_events_task,
                contract_executor_task,
                initial_join_task: None,
                session_actor_task,
                result_router_task,
                background_task_monitor,
            },
            shutdown_tx,
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Verify that a spawned task that panics is detected via JoinHandle.
    #[tokio::test]
    async fn test_join_handle_detects_panic() {
        let handle: JoinHandle<()> = tokio::spawn(async {
            panic!("intentional test panic");
        });
        let result = handle.await;
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(
            err.is_panic(),
            "JoinError should indicate a panic, got: {err}"
        );
    }

    /// Verify that a spawned task that returns cleanly produces Ok.
    #[tokio::test]
    async fn test_join_handle_detects_clean_exit() {
        let handle: JoinHandle<()> = tokio::spawn(async {
            // Clean return
        });
        let result = handle.await;
        assert!(result.is_ok(), "Clean task exit should produce Ok");
    }

    /// Three tasks: 2 sleeping, 1 panics after short delay. Verify tokio::select!
    /// triggers on the panicked task and returns a panic error.
    #[tokio::test]
    async fn test_select_catches_first_panicked_task() {
        let mut h1: JoinHandle<()> = tokio::spawn(async {
            tokio::time::sleep(Duration::from_secs(60)).await;
        });
        let mut h2: JoinHandle<()> = tokio::spawn(async {
            tokio::time::sleep(Duration::from_secs(60)).await;
        });
        let mut h3: JoinHandle<()> = tokio::spawn(async {
            tokio::time::sleep(Duration::from_millis(10)).await;
            panic!("task 3 panicked");
        });

        let result: anyhow::Result<()> = tokio::select! {
            biased;
            r = &mut h1 => match r {
                Err(e) if e.is_panic() => Err(anyhow::anyhow!("task 1 panicked: {e}")),
                Err(e) => Err(anyhow::anyhow!("task 1 failed: {e}")),
                Ok(()) => Err(anyhow::anyhow!("task 1 exited")),
            },
            r = &mut h2 => match r {
                Err(e) if e.is_panic() => Err(anyhow::anyhow!("task 2 panicked: {e}")),
                Err(e) => Err(anyhow::anyhow!("task 2 failed: {e}")),
                Ok(()) => Err(anyhow::anyhow!("task 2 exited")),
            },
            r = &mut h3 => match r {
                Err(e) if e.is_panic() => Err(anyhow::anyhow!("task 3 panicked: {e}")),
                Err(e) => Err(anyhow::anyhow!("task 3 failed: {e}")),
                Ok(()) => Err(anyhow::anyhow!("task 3 exited")),
            },
        };

        assert!(result.is_err());
        let err_msg = result.unwrap_err().to_string();
        assert!(
            err_msg.contains("task 3 panicked"),
            "Should catch the panicking task, got: {err_msg}"
        );

        // Clean up the sleeping tasks
        h1.abort();
        h2.abort();
    }
}
