//! In-memory node builder for simulation testing.
//!
//! This module provides a `Builder` that uses the production event loop (`P2pConnManager`)
//! with `InMemorySocket` for testing without real network I/O.
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
        self, mediator_channels, op_request_channel, run_op_request_mediator, ContractHandler,
        MemoryContractHandler, SimulationContractHandler, SimulationHandlerBuilder,
    },
    node::{
        network_bridge::{event_loop_notification_channel, p2p_protoc::P2pConnManager},
        op_state_manager::OpManager,
        MessageProcessor, NetEventRegister,
    },
    operations::connect,
    ring::{ConnectionManager, PeerKeyLocation},
    transport::in_memory_socket::{register_address_network, InMemorySocket},
    wasm_runtime::MockStateStorage,
};

use super::Builder;

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

        // Create result router channel - needed for MessageProcessor
        let (result_router_tx, _result_router_rx) = tokio::sync::mpsc::channel(100);

        let op_manager = Arc::new(OpManager::new(
            notification_tx,
            ops_ch_channel,
            &self.config,
            self.event_register.clone(),
            connection_manager.clone(),
            result_router_tx.clone(),
        )?);
        op_manager.ring.attach_op_manager(&op_manager);
        std::mem::drop(_guard);

        // Create channels for the mediator pattern
        let (op_request_receiver, op_sender) = op_request_channel();
        let (executor_listener, to_event_loop_tx, from_event_loop_rx) =
            mediator_channels(op_manager.clone());

        // Spawn the mediator task
        GlobalExecutor::spawn({
            let mediator_task =
                run_op_request_mediator(op_request_receiver, to_event_loop_tx, from_event_loop_rx);
            mediator_task.instrument(tracing::info_span!("op_request_mediator"))
        });

        let contract_handler = MemoryContractHandler::build(
            ch_channel,
            op_sender,
            op_manager.clone(),
            self.contract_handler_name,
        )
        .await
        .map_err(|e| anyhow::anyhow!(e))?;

        GlobalExecutor::spawn(
            contract::contract_handling(contract_handler)
                .instrument(tracing::info_span!(parent: parent_span.clone(), "contract_handling")),
        );

        // Create MessageProcessor with dummy session channel for testing
        // In tests, results are not routed to clients through the session actor
        let (session_tx, _session_rx) = tokio::sync::mpsc::channel(100);
        let message_processor = Arc::new(MessageProcessor::new(session_tx));

        // Build P2pConnManager with test configuration
        let conn_manager = P2pConnManager::build(
            &self.config,
            op_manager.clone(),
            self.event_register.clone(),
            message_processor,
        )
        .await?;

        // Append contracts before starting
        append_contracts(&op_manager, self.contracts, self.contract_subscribers).await?;

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

        // Run the production event loop with InMemorySocket
        let result = conn_manager
            .run_event_listener_with_socket::<InMemorySocket>(
                op_manager,
                wait_for_event,
                notification_channel,
                executor_listener,
                node_controller_rx,
            )
            .instrument(parent_span)
            .await;

        if let Some(handle) = join_task {
            handle.abort();
            let _ = handle.await;
        }

        // Convert Infallible to () for test compatibility
        // Graceful shutdown is treated as success in testing
        match result {
            Ok(_infallible) => Ok(()),
            Err(e) if e.to_string() == "Graceful shutdown" => {
                tracing::info!("Node exited via graceful shutdown");
                Ok(())
            }
            Err(e) => Err(e),
        }
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

        // Create result router channel
        let (result_router_tx, _result_router_rx) = tokio::sync::mpsc::channel(100);

        let op_manager = Arc::new(OpManager::new(
            notification_tx,
            ops_ch_channel,
            &self.config,
            self.event_register.clone(),
            connection_manager.clone(),
            result_router_tx.clone(),
        )?);
        op_manager.ring.attach_op_manager(&op_manager);
        std::mem::drop(_guard);

        // Create channels for the mediator pattern
        let (op_request_receiver, op_sender) = op_request_channel();
        let (executor_listener, to_event_loop_tx, from_event_loop_rx) =
            mediator_channels(op_manager.clone());

        // Spawn the mediator task
        GlobalExecutor::spawn({
            let mediator_task =
                run_op_request_mediator(op_request_receiver, to_event_loop_tx, from_event_loop_rx);
            mediator_task.instrument(tracing::info_span!("op_request_mediator"))
        });

        // Use SimulationContractHandler with shared in-memory storage
        let contract_handler = SimulationContractHandler::build(
            ch_channel,
            op_sender,
            op_manager.clone(),
            SimulationHandlerBuilder {
                identifier: self.contract_handler_name,
                shared_storage,
            },
        )
        .await
        .map_err(|e| anyhow::anyhow!(e))?;

        GlobalExecutor::spawn(
            contract::contract_handling(contract_handler)
                .instrument(tracing::info_span!(parent: parent_span.clone(), "contract_handling")),
        );

        // Create MessageProcessor with dummy session channel for testing
        let (session_tx, _session_rx) = tokio::sync::mpsc::channel(100);
        let message_processor = Arc::new(MessageProcessor::new(session_tx));

        // Build P2pConnManager with test configuration
        let conn_manager = P2pConnManager::build(
            &self.config,
            op_manager.clone(),
            self.event_register.clone(),
            message_processor,
        )
        .await?;

        // Append contracts before starting
        append_contracts(&op_manager, self.contracts, self.contract_subscribers).await?;

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

        // Run the production event loop with InMemorySocket
        let result = conn_manager
            .run_event_listener_with_socket::<InMemorySocket>(
                op_manager,
                wait_for_event,
                notification_channel,
                executor_listener,
                node_controller_rx,
            )
            .instrument(parent_span)
            .await;

        if let Some(handle) = join_task {
            handle.abort();
            let _ = handle.await;
        }

        // Convert Infallible to () for test compatibility
        // Graceful shutdown is treated as success in testing
        match result {
            Ok(_infallible) => Ok(()),
            Err(e) if e.to_string() == "Graceful shutdown" => {
                tracing::info!("Node exited via graceful shutdown");
                Ok(())
            }
            Err(e) => Err(e),
        }
    }
}

/// Append contracts to the op_manager before starting the event loop.
async fn append_contracts(
    op_manager: &Arc<OpManager>,
    contracts: Vec<(ContractContainer, WrappedState, bool)>,
    contract_subscribers: HashMap<ContractKey, Vec<PeerKeyLocation>>,
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
            op_manager.ring.seed_contract(key, state_size);
        }
        if let Some(subscribers) = contract_subscribers.get(&key) {
            // add contract subscribers as downstream (test setup - no upstream_addr)
            for subscriber in subscribers {
                if op_manager
                    .ring
                    .add_downstream(&key, subscriber.clone(), None)
                    .is_err()
                {
                    tracing::warn!("Max subscribers for contract {} reached", key);
                    break;
                }
            }
        }
    }
    Ok(())
}
