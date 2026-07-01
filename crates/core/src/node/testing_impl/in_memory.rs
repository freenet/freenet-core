//! In-memory node builder for simulation testing.
//!
//! This module provides a `Builder` that uses the production event loop (`P2pConnManager`)
//! with `SimulationSocket` for testing without real network I/O.
//!
//! Each node must have the network context set before binding sockets. This is done
//! automatically by the `run_node` and `run_node_with_shared_storage` methods using
//! the `network_name` from the `Builder`.

use std::{collections::HashMap, sync::Arc};

use freenet_stdlib::prelude::*;
use tracing::Instrument;

use crate::{
    client_events::ClientEventsProxy,
    config::GlobalExecutor,
    contract::{
        self, ContractHandler, MemoryContractHandler, MockWasmContractHandler,
        MockWasmHandlerBuilder, SimulationContractHandler, SimulationHandlerBuilder,
    },
    node::{
        EventLoopExitReason, NetEventRegister,
        background_task_monitor::BackgroundTaskMonitor,
        network_bridge::{event_loop_notification_channel, p2p_protoc::P2pConnManager},
        op_state_manager::OpManager,
    },
    operations::connect,
    ring::{ConnectionManager, Location, PeerKeyLocation},
    transport::in_memory_socket::{SimulationSocket, register_address_network},
    wasm_runtime::MockStateStorage,
};

use super::Builder;

/// Converts the event loop result to a test-compatible result.
///
/// The event loop returns `Result<Infallible, anyhow::Error>` where:
/// - `Ok(Infallible)` is impossible (Infallible can't be constructed)
/// - `Err(EventLoopExitReason::GracefulShutdown)` means clean exit
/// - `Err(other)` means actual error
///
/// In testing, graceful shutdown is treated as success.
fn handle_event_loop_result(
    result: Result<std::convert::Infallible, anyhow::Error>,
) -> anyhow::Result<()> {
    match result {
        Ok(_infallible) => Ok(()),
        Err(e) => {
            // Use downcast_ref for type-safe error matching instead of string comparison
            if e.downcast_ref::<EventLoopExitReason>()
                .map(|r| matches!(r, EventLoopExitReason::GracefulShutdown))
                .unwrap_or(false)
            {
                tracing::info!("Node exited via graceful shutdown");
                Ok(())
            } else {
                Err(e)
            }
        }
    }
}

impl<ER> Builder<ER> {
    #[allow(dead_code)]
    pub async fn run_node<UsrEv>(
        self,
        user_events: UsrEv,
        parent_span: tracing::Span,
    ) -> anyhow::Result<()>
    where
        UsrEv: ClientEventsProxy + Send + 'static,
        ER: NetEventRegister + Clone,
    {
        let gateways = self.config.get_gateways()?;

        let (notification_channel, notification_tx) = event_loop_notification_channel();
        let (ops_ch_channel, ch_channel, wait_for_event) = contract::contract_handler_channel();

        let _guard = parent_span.enter();
        let connection_manager = ConnectionManager::new(&self.config);

        let (result_router_tx, _result_router_rx) = tokio::sync::mpsc::channel(100);

        // In-memory nodes use a monitor for API compatibility; tasks are cleaned
        // up when the monitor is dropped at the end of scope.
        let task_monitor = BackgroundTaskMonitor::new();
        let op_manager = Arc::new(OpManager::new(
            notification_tx,
            ops_ch_channel,
            &self.config,
            self.event_register.clone(),
            connection_manager.clone(),
            result_router_tx.clone(),
            &task_monitor,
        )?);
        op_manager.ring.attach_op_manager(&op_manager);

        // Publish the live Ring for governance sim tests to observe
        // ban-list state. Mirrors the `shared_cm` capture above.
        if let Some(out) = &self.shared_ring {
            *out.lock() = Some(op_manager.ring.clone());
        }

        std::mem::drop(_guard);

        let contract_handler = MemoryContractHandler::build(
            ch_channel,
            op_manager.clone(),
            self.contract_handler_name,
        )
        .await
        .map_err(|e| anyhow::anyhow!(e))?;

        GlobalExecutor::spawn(
            contract::contract_handling(
                contract_handler,
                crate::contract::user_input::AutoApprovePrompter,
            )
            .instrument(tracing::info_span!(parent: parent_span.clone(), "contract_handling")),
        );

        let conn_manager = P2pConnManager::build(
            &self.config,
            op_manager.clone(),
            self.event_register.clone(),
        )
        .await?;

        // Append contracts before starting
        append_contracts(&op_manager, self.contracts, self.contract_subscribers).await?;

        // Inject any pre-formed ring connections (topology preseed, #4233) before
        // the event loop starts, so a wide direct star exists at t=0 without the
        // organic CONNECT handshake. No-op (empty vec) for all existing tests.
        apply_preseeded_connections(&op_manager, self.preseed_connections).await;

        // Start join procedure for non-gateway nodes
        let join_task = if !gateways.is_empty() && !self.config.is_gateway {
            Some(connect::initial_join_procedure(op_manager.clone(), &gateways).await?)
        } else {
            None
        };

        // Spawn client event handling
        let (client_responses, _cli_response_sender) = contract::client_responses_channel();
        let (node_controller_tx, node_controller_rx) = tokio::sync::mpsc::channel(1);
        GlobalExecutor::spawn({
            let op_manager = op_manager.clone();
            crate::client_events::client_event_handling(
                op_manager,
                user_events,
                client_responses,
                node_controller_tx,
            )
            .instrument(tracing::info_span!(parent: parent_span.clone(), "client_event_handling"))
        });

        // Register this node's address with its network for InMemorySocket binding
        // This is thread-safe and must be done before the event loop binds the socket
        let local_addr = std::net::SocketAddr::new(
            self.config.network_listener_ip,
            self.config.network_listener_port,
        );
        register_address_network(local_addr, &self.network_name);

        // Run the production event loop with SimulationSocket
        let result = conn_manager
            .run_event_listener_with_socket::<SimulationSocket>(
                op_manager,
                wait_for_event,
                notification_channel,
                node_controller_rx,
            )
            .instrument(parent_span)
            .await;

        if let Some(handle) = join_task {
            handle.abort();
            let _join_result = handle.await;
        }

        handle_event_loop_result(result)
    }

    /// Runs a node with shared in-memory storage for deterministic simulation.
    ///
    /// Unlike `run_node`, this method uses `SimulationContractHandler` which stores
    /// all contract state in the provided `MockStateStorage`. The storage is Arc-backed,
    /// so state persists across node restarts when the same storage is reused.
    ///
    /// # Arguments
    /// * `user_events` - Client event proxy for handling user requests
    /// * `parent_span` - Tracing span for this node
    /// * `shared_storage` - Shared in-memory storage (clone to share across restarts)
    pub async fn run_node_with_shared_storage<UsrEv>(
        self,
        user_events: UsrEv,
        parent_span: tracing::Span,
        shared_storage: MockStateStorage,
    ) -> anyhow::Result<()>
    where
        UsrEv: ClientEventsProxy + Send + 'static,
        ER: NetEventRegister + Clone,
    {
        let gateways = self.config.get_gateways()?;

        let (notification_channel, notification_tx) = event_loop_notification_channel();
        let (ops_ch_channel, ch_channel, wait_for_event) = contract::contract_handler_channel();

        let _guard = parent_span.enter();
        let connection_manager = ConnectionManager::new(&self.config);

        if let Some(out) = &self.shared_cm {
            *out.lock() = Some(connection_manager.clone());
        }

        // Create result router channel
        let (result_router_tx, _result_router_rx) = tokio::sync::mpsc::channel(100);

        let task_monitor = BackgroundTaskMonitor::new();
        let op_manager = Arc::new(OpManager::new(
            notification_tx,
            ops_ch_channel,
            &self.config,
            self.event_register.clone(),
            connection_manager.clone(),
            result_router_tx.clone(),
            &task_monitor,
        )?);
        op_manager.ring.attach_op_manager(&op_manager);

        // Publish the live Ring for governance sim tests to observe
        // ban-list state. Mirrors the `shared_cm` capture above.
        if let Some(out) = &self.shared_ring {
            *out.lock() = Some(op_manager.ring.clone());
        }

        std::mem::drop(_guard);

        // Use SimulationContractHandler with shared in-memory storage
        let contract_handler = SimulationContractHandler::build(
            ch_channel,
            op_manager.clone(),
            SimulationHandlerBuilder {
                identifier: self.contract_handler_name,
                shared_storage,
            },
        )
        .await
        .map_err(|e| anyhow::anyhow!(e))?;

        GlobalExecutor::spawn(
            contract::contract_handling(
                contract_handler,
                crate::contract::user_input::AutoApprovePrompter,
            )
            .instrument(tracing::info_span!(parent: parent_span.clone(), "contract_handling")),
        );

        let conn_manager = P2pConnManager::build(
            &self.config,
            op_manager.clone(),
            self.event_register.clone(),
        )
        .await?;

        // Append contracts before starting
        append_contracts(&op_manager, self.contracts, self.contract_subscribers).await?;

        // Inject any pre-formed ring connections (topology preseed, #4233) before
        // the event loop starts, so a wide direct star exists at t=0 without the
        // organic CONNECT handshake. No-op (empty vec) for all existing tests.
        apply_preseeded_connections(&op_manager, self.preseed_connections).await;

        // Start join procedure for non-gateway nodes
        let join_task = if !gateways.is_empty() && !self.config.is_gateway {
            Some(connect::initial_join_procedure(op_manager.clone(), &gateways).await?)
        } else {
            None
        };

        // Spawn client event handling
        let (client_responses, _cli_response_sender) = contract::client_responses_channel();
        let (node_controller_tx, node_controller_rx) = tokio::sync::mpsc::channel(1);
        GlobalExecutor::spawn({
            let op_manager = op_manager.clone();
            crate::client_events::client_event_handling(
                op_manager,
                user_events,
                client_responses,
                node_controller_tx,
            )
            .instrument(tracing::info_span!(parent: parent_span.clone(), "client_event_handling"))
        });

        // Register this node's address with its network for InMemorySocket binding
        // This is thread-safe and must be done before the event loop binds the socket
        let local_addr = std::net::SocketAddr::new(
            self.config.network_listener_ip,
            self.config.network_listener_port,
        );
        register_address_network(local_addr, &self.network_name);

        // Run the production event loop with SimulationSocket
        let result = conn_manager
            .run_event_listener_with_socket::<SimulationSocket>(
                op_manager,
                wait_for_event,
                notification_channel,
                node_controller_rx,
            )
            .instrument(parent_span)
            .await;

        if let Some(handle) = join_task {
            handle.abort();
            let _join_result = handle.await;
        }

        handle_event_loop_result(result)
    }

    /// Runs a node using MockWasmRuntime — exercises the **production** ContractExecutor
    /// code path (init_tracker, validation, notification pipeline, corrupted state recovery)
    /// without requiring real WASM binaries.
    ///
    /// Same as `run_node_with_shared_storage` but uses `MockWasmContractHandler` instead of
    /// `SimulationContractHandler`.
    #[allow(dead_code)]
    pub async fn run_node_with_mock_wasm<UsrEv>(
        self,
        user_events: UsrEv,
        parent_span: tracing::Span,
        shared_storage: MockStateStorage,
        contract_store: Option<crate::wasm_runtime::InMemoryContractStore>,
    ) -> anyhow::Result<()>
    where
        UsrEv: ClientEventsProxy + Send + 'static,
        ER: NetEventRegister + Clone,
    {
        let gateways = self.config.get_gateways()?;

        let (notification_channel, notification_tx) = event_loop_notification_channel();
        let (ops_ch_channel, ch_channel, wait_for_event) = contract::contract_handler_channel();

        let _guard = parent_span.enter();
        let connection_manager = ConnectionManager::new(&self.config);

        if let Some(out) = &self.shared_cm {
            *out.lock() = Some(connection_manager.clone());
        }

        // Create result router channel
        let (result_router_tx, _result_router_rx) = tokio::sync::mpsc::channel(100);

        let task_monitor = BackgroundTaskMonitor::new();
        let op_manager = Arc::new(OpManager::new(
            notification_tx,
            ops_ch_channel,
            &self.config,
            self.event_register.clone(),
            connection_manager.clone(),
            result_router_tx.clone(),
            &task_monitor,
        )?);
        op_manager.ring.attach_op_manager(&op_manager);

        // Publish the live Ring for governance sim tests to observe
        // ban-list state. Mirrors the `shared_cm` capture above.
        if let Some(out) = &self.shared_ring {
            *out.lock() = Some(op_manager.ring.clone());
        }

        std::mem::drop(_guard);

        // Use MockWasmContractHandler — exercises the production ContractExecutor code path
        let contract_handler = MockWasmContractHandler::build(
            ch_channel,
            op_manager.clone(),
            MockWasmHandlerBuilder {
                identifier: self.contract_handler_name,
                shared_storage,
                contract_store,
            },
        )
        .await
        .map_err(|e| anyhow::anyhow!(e))?;

        GlobalExecutor::spawn(
            contract::contract_handling(
                contract_handler,
                crate::contract::user_input::AutoApprovePrompter,
            )
            .instrument(tracing::info_span!(parent: parent_span.clone(), "contract_handling")),
        );

        let conn_manager = P2pConnManager::build(
            &self.config,
            op_manager.clone(),
            self.event_register.clone(),
        )
        .await?;

        // Append contracts before starting
        append_contracts(&op_manager, self.contracts, self.contract_subscribers).await?;

        // Inject any pre-formed ring connections (topology preseed, #4233) before
        // the event loop starts, so a wide direct star exists at t=0 without the
        // organic CONNECT handshake. No-op (empty vec) for all existing tests.
        apply_preseeded_connections(&op_manager, self.preseed_connections).await;

        // Start join procedure for non-gateway nodes
        let join_task = if !gateways.is_empty() && !self.config.is_gateway {
            Some(connect::initial_join_procedure(op_manager.clone(), &gateways).await?)
        } else {
            None
        };

        // Spawn client event handling
        let (client_responses, _cli_response_sender) = contract::client_responses_channel();
        let (node_controller_tx, node_controller_rx) = tokio::sync::mpsc::channel(1);
        GlobalExecutor::spawn({
            let op_manager = op_manager.clone();
            crate::client_events::client_event_handling(
                op_manager,
                user_events,
                client_responses,
                node_controller_tx,
            )
            .instrument(tracing::info_span!(parent: parent_span.clone(), "client_event_handling"))
        });

        // Register this node's address with its network for InMemorySocket binding
        let local_addr = std::net::SocketAddr::new(
            self.config.network_listener_ip,
            self.config.network_listener_port,
        );
        register_address_network(local_addr, &self.network_name);

        // Run the production event loop with SimulationSocket
        let result = conn_manager
            .run_event_listener_with_socket::<SimulationSocket>(
                op_manager,
                wait_for_event,
                notification_channel,
                node_controller_rx,
            )
            .instrument(parent_span)
            .await;

        if let Some(handle) = join_task {
            handle.abort();
            let _join_result = handle.await;
        }

        handle_event_loop_result(result)
    }
}

/// Append contracts to the op_manager before starting the event loop.
async fn append_contracts(
    op_manager: &Arc<OpManager>,
    contracts: Vec<(ContractContainer, WrappedState, bool)>,
    _contract_subscribers: HashMap<ContractKey, Vec<PeerKeyLocation>>,
) -> anyhow::Result<()> {
    use crate::contract::ContractHandlerEvent;
    for (contract, state, subscription) in contracts {
        let key: ContractKey = contract.key();
        let state_size = state.size() as u64;
        op_manager
            .notify_contract_handler(ContractHandlerEvent::PutQuery {
                key,
                state,
                related_contracts: RelatedContracts::default(),
                contract: Some(contract),
            })
            .await?;
        tracing::debug!(
            "Appended contract {} to peer {}",
            key,
            op_manager.ring.connection_manager.get_own_addr().unwrap()
        );
        if subscription {
            op_manager
                .ring
                .host_contract(key, state_size, crate::ring::AccessType::Put);
            // In the new lease-based model, register an active subscription
            op_manager.ring.subscribe(key);
            // Register the contract in the neighbor-hosting advertised set so
            // the connection-established HostingStateResponse exchange
            // advertises it to neighbors. Without this, a startup-hosted
            // contract (SeedHostedContract) leaves `my_contracts` empty and
            // neighbors never learn this node hosts it — which the terminal
            // advertisement consult (and UPDATE proximity forwarding) rely on.
            // Mirrors the production announce that fires on first-time PUT/GET
            // hosting; the returned announcement is dropped because the node
            // has no ring connections yet at startup (the exchange runs later,
            // on connection-established).
            let _ = op_manager.neighbor_hosting.on_contract_hosted(&key);
        }
        // Note: contract_subscribers is ignored in the new model.
        // Neighbor hosting handles peer-to-peer awareness for update propagation.
    }
    Ok(())
}

/// Inject pre-formed ring connections at node startup, bypassing the organic
/// CONNECT/NAT-traversal handshake.
///
/// ## Why this exists
///
/// The Turmoil simulation harness cannot organically bootstrap a wide direct
/// star (one gateway with direct connections to 40-80 subscribers — the
/// production-incident topology, #4233): ring-routed CONNECT cannot form that
/// many direct connections within the virtual-time budget (subscribers hit "at
/// terminus, no uphill peers available — rejecting" in the tiny 1-gateway star).
/// N=8/16 form; N>=24 never finish, so the fan-out broadcast phase that actually
/// exercises the event-loop saturation surface is never reached.
///
/// This primitive is the connection-side analogue of the `SeedHostedContract`
/// contract preseed: it writes the ring `Connection`/`Location` state directly
/// into each node's [`ConnectionManager`] before the event loop starts, so the
/// gateway already believes it holds a direct connection to every subscriber
/// (and vice-versa) at t=0.
///
/// ## What it does — and what it deliberately leaves to the organic path
///
/// For each preseeded peer this calls [`Ring::add_connection`], which is pure
/// ring bookkeeping (it inserts into `location_for_peer` /
/// `connections_by_location` and enforces the `max_connections` cap; it does NOT
/// require a live transport connection). The underlying UDP/`SimulationSocket`
/// connection is then materialized lazily by the production event loop on the
/// first outbound message to that peer — the `OutboundMessageWithTarget` handler
/// resolves a ring-known peer via `get_peer_location_by_addr` and dials it
/// (`p2p_protoc.rs`). Both peers' addresses are already registered with the
/// network (`register_address_network`), so the in-memory socket delivers.
///
/// It deliberately does NOT pre-register the subscriber's interest/subscription
/// for any contract. Broadcast fan-out (`get_broadcast_targets_update`)
/// enumerates the interest manager + proximity cache, not the raw connection
/// set — and those are populated organically when each subscriber runs its real
/// `Subscribe` operation, which now routes directly to the gateway over the
/// injected connection (`subscribe.rs` calls `register_peer_interest` /
/// `add_downstream_subscriber` on the host as it processes the inbound
/// subscribe). So the scenario keeps using the genuine subscribe + interest +
/// broadcast machinery; the ONLY thing replaced is the CONNECT handshake.
///
/// `was_reserved: false` is correct because no reservation was ever made for
/// these synthetic connections. A peer that the cap rejects (over
/// `max_connections`) is logged and skipped rather than panicking, so a
/// mis-sized star surfaces as a connectivity shortfall in the test's own
/// assertions rather than an opaque startup panic.
async fn apply_preseeded_connections(
    op_manager: &Arc<OpManager>,
    connections: Vec<PeerKeyLocation>,
) {
    use crate::node::PeerId;

    for peer in connections {
        let Some(addr) = peer.socket_addr() else {
            tracing::warn!("preseed_connections: skipping peer with unknown address: {peer}");
            continue;
        };
        let loc = Location::from_address(&addr);
        let peer_id = PeerId::new(peer.pub_key().clone(), addr);
        // `add_connection` is idempotent-ish: the CM dedups by addr, so a
        // double-injection is harmless. The bool return signals "just crossed
        // the readiness threshold", which we don't need here.
        let _ = op_manager.ring.add_connection(loc, peer_id, false).await;
        tracing::debug!(
            "preseed_connections: injected direct connection {} -> {peer} (@ {loc})",
            op_manager
                .ring
                .connection_manager
                .get_own_addr()
                .map(|a| a.to_string())
                .unwrap_or_else(|| "<no addr>".to_string()),
        );
    }
}
