use super::{ConnectionError, EventLoopNotificationsReceiver, NetworkBridge};
use crate::contract::{ContractHandlerEvent, WaitingTransaction};
use crate::message::{NetMessageV1, QueryResult};
use crate::node::subscribe::SubscribeMsg;
use crate::ring::Location;
use dashmap::DashSet;
use either::{Either, Left, Right};
use futures::future::BoxFuture;
use futures::stream::FuturesUnordered;
use futures::FutureExt;
use std::convert::Infallible;
use std::future::Future;
use std::net::{IpAddr, SocketAddr};
use std::pin::Pin;
use std::time::Duration;
use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
};
use tokio::net::UdpSocket;
use tokio::sync::mpsc::{self, Receiver, Sender};
use tokio::sync::oneshot::{self};
use tokio::time::timeout;
use tracing::Instrument;

use crate::node::network_bridge::handshake::{
    Event as HandshakeEvent, ForwardInfo, HandshakeError, HandshakeHandler, HanshakeHandlerMsg,
    OutboundMessage,
};
use crate::node::network_bridge::priority_select;
use crate::node::{MessageProcessor, PeerId};
use crate::operations::{connect::ConnectMsg, get::GetMsg, put::PutMsg, update::UpdateMsg};
use crate::transport::{
    create_connection_handler, PeerConnection, TransportError, TransportKeypair,
};
use crate::{
    client_events::ClientId,
    config::GlobalExecutor,
    contract::{
        ContractHandlerChannel, ExecutorToEventLoopChannel, NetworkEventListenerHalve,
        WaitingResolution,
    },
    message::{MessageStats, NetMessage, NodeEvent, Transaction},
    node::{handle_aborted_op, process_message_decoupled, NetEventRegister, NodeConfig, OpManager},
    ring::PeerKeyLocation,
    tracing::NetEventLog,
};

type P2pBridgeEvent = Either<(PeerId, Box<NetMessage>), NodeEvent>;

#[derive(Clone)]
pub(crate) struct P2pBridge {
    accepted_peers: Arc<DashSet<PeerId>>,
    ev_listener_tx: Sender<P2pBridgeEvent>,
    op_manager: Arc<OpManager>,
    log_register: Arc<dyn NetEventRegister>,
}

impl P2pBridge {
    fn new<EL>(
        sender: Sender<P2pBridgeEvent>,
        op_manager: Arc<OpManager>,
        event_register: EL,
    ) -> Self
    where
        EL: NetEventRegister,
    {
        Self {
            accepted_peers: Arc::new(DashSet::new()),
            ev_listener_tx: sender,
            op_manager,
            log_register: Arc::new(event_register),
        }
    }
}

impl NetworkBridge for P2pBridge {
    async fn drop_connection(&mut self, peer: &PeerId) -> super::ConnResult<()> {
        self.accepted_peers.remove(peer);
        self.ev_listener_tx
            .send(Right(NodeEvent::DropConnection(peer.clone())))
            .await
            .map_err(|_| ConnectionError::SendNotCompleted(peer.clone()))?;
        self.log_register
            .register_events(Either::Left(NetEventLog::disconnected(
                &self.op_manager.ring,
                peer,
            )))
            .await;
        Ok(())
    }

    async fn send(&self, target: &PeerId, msg: NetMessage) -> super::ConnResult<()> {
        self.log_register
            .register_events(NetEventLog::from_outbound_msg(&msg, &self.op_manager.ring))
            .await;
        self.op_manager.sending_transaction(target, &msg);
        self.ev_listener_tx
            .send(Left((target.clone(), Box::new(msg))))
            .await
            .map_err(|_| ConnectionError::SendNotCompleted(target.clone()))?;
        Ok(())
    }
}

type PeerConnChannelSender = Sender<Either<NetMessage, ConnEvent>>;
type PeerConnChannelRecv = Receiver<Either<NetMessage, ConnEvent>>;

pub(in crate::node) struct P2pConnManager {
    pub(in crate::node) gateways: Vec<PeerKeyLocation>,
    pub(in crate::node) bridge: P2pBridge,
    conn_bridge_rx: Receiver<P2pBridgeEvent>,
    event_listener: Box<dyn NetEventRegister>,
    connections: HashMap<PeerId, PeerConnChannelSender>,
    key_pair: TransportKeypair,
    listening_ip: IpAddr,
    listening_port: u16,
    is_gateway: bool,
    /// If set, will sent the location over network messages.
    ///
    /// It will also determine whether to trust the location of peers sent in network messages or derive them from IP.
    ///
    /// This is used for testing deterministically with given location. In production this should always be none
    /// and locations should be derived from IP addresses.
    this_location: Option<Location>,
    check_version: bool,
    bandwidth_limit: Option<usize>,
    blocked_addresses: Option<HashSet<SocketAddr>>,
    /// MessageProcessor for clean client handling separation
    message_processor: Arc<MessageProcessor>,
}

impl P2pConnManager {
    pub async fn build(
        config: &NodeConfig,
        op_manager: Arc<OpManager>,
        event_listener: impl NetEventRegister + Clone,
        message_processor: Arc<MessageProcessor>,
    ) -> anyhow::Result<Self> {
        let listen_port = config.network_listener_port;
        let listener_ip = config.network_listener_ip;

        let (tx_bridge_cmd, rx_bridge_cmd) = mpsc::channel(100);
        let bridge = P2pBridge::new(tx_bridge_cmd, op_manager, event_listener.clone());

        let gateways = config.get_gateways()?;
        let key_pair = config.key_pair.clone();
        Ok(P2pConnManager {
            gateways,
            bridge,
            conn_bridge_rx: rx_bridge_cmd,
            event_listener: Box::new(event_listener),
            connections: HashMap::new(),
            key_pair,
            listening_ip: listener_ip,
            listening_port: listen_port,
            is_gateway: config.is_gateway,
            this_location: config.location,
            check_version: !config.config.network_api.ignore_protocol_version,
            bandwidth_limit: config.config.network_api.bandwidth_limit,
            blocked_addresses: config.blocked_addresses.clone(),
            message_processor,
        })
    }

    #[allow(clippy::too_many_arguments)]
    #[tracing::instrument(name = "network_event_listener", fields(peer = %self.bridge.op_manager.ring.connection_manager.pub_key), skip_all)]
    pub async fn run_event_listener(
        self,
        op_manager: Arc<OpManager>,
        client_wait_for_transaction: ContractHandlerChannel<WaitingResolution>,
        notification_channel: EventLoopNotificationsReceiver,
        executor_listener: ExecutorToEventLoopChannel<NetworkEventListenerHalve>,
        node_controller: Receiver<NodeEvent>,
    ) -> anyhow::Result<Infallible> {
        // Destructure self to avoid partial move issues
        let P2pConnManager {
            gateways,
            bridge,
            conn_bridge_rx,
            event_listener,
            connections,
            key_pair,
            listening_ip,
            listening_port,
            is_gateway,
            this_location,
            check_version,
            bandwidth_limit,
            blocked_addresses,
            message_processor,
        } = self;

        tracing::info!(
            %listening_port,
            %listening_ip,
            %is_gateway,
            key = %key_pair.public(),
            "Opening network listener - will receive from channel"
        );

        let mut state = EventListenerState::new();

        // Separate peer_connections to allow independent borrowing by the stream
        let peer_connections: FuturesUnordered<
            BoxFuture<'static, Result<PeerConnectionInbound, TransportError>>,
        > = FuturesUnordered::new();

        let (outbound_conn_handler, inbound_conn_handler) = create_connection_handler::<UdpSocket>(
            key_pair.clone(),
            listening_ip,
            listening_port,
            is_gateway,
            bandwidth_limit,
        )
        .await?;

        // For non-gateway peers, pass the peer_ready flag so it can be set after first handshake
        // For gateways, pass None (they're always ready)
        let peer_ready = if !is_gateway {
            Some(bridge.op_manager.peer_ready.clone())
        } else {
            None
        };

        let (handshake_handler, handshake_handler_msg, outbound_message) = HandshakeHandler::new(
            inbound_conn_handler,
            outbound_conn_handler.clone(),
            bridge.op_manager.ring.connection_manager.clone(),
            bridge.op_manager.ring.router.clone(),
            this_location,
            is_gateway,
            peer_ready,
        );

        // Create priority select stream ONCE by moving ownership - it stays alive across iterations.
        // This fixes the lost wakeup race condition (issue #1932).
        let select_stream = priority_select::ProductionPrioritySelectStream::new(
            notification_channel.notifications_receiver,
            notification_channel.op_execution_receiver,
            conn_bridge_rx,
            handshake_handler,
            node_controller,
            client_wait_for_transaction,
            executor_listener,
            peer_connections,
        );

        // Pin the stream on the stack
        tokio::pin!(select_stream);

        // Reconstruct a P2pConnManager-like structure for use in the loop
        // We can't use the original self because we moved conn_bridge_rx
        let mut ctx = P2pConnManager {
            gateways,
            bridge,
            conn_bridge_rx: tokio::sync::mpsc::channel(1).1, // Dummy, won't be used
            event_listener,
            connections,
            key_pair,
            listening_ip,
            listening_port,
            is_gateway,
            this_location,
            check_version,
            bandwidth_limit,
            blocked_addresses,
            message_processor,
        };

        use futures::StreamExt;

        while let Some(result) = select_stream.as_mut().next().await {
            // Process the result using the existing handler
            let event = ctx
                .process_select_result(
                    result,
                    &mut state,
                    &mut select_stream,
                    &handshake_handler_msg,
                )
                .await?;

            match event {
                EventResult::Continue => continue,
                EventResult::Event(event) => {
                    match *event {
                        ConnEvent::InboundMessage(msg) => {
                            tracing::info!(
                                tx = %msg.id(),
                                msg_type = %msg,
                                peer = %ctx.bridge.op_manager.ring.connection_manager.get_peer_key().unwrap(),
                                "Received inbound message from peer - processing"
                            );
                            ctx.handle_inbound_message(
                                msg,
                                &outbound_message,
                                &op_manager,
                                &mut state,
                            )
                            .await?;
                        }
                        ConnEvent::OutboundMessage(NetMessage::V1(NetMessageV1::Aborted(tx))) => {
                            // TODO: handle aborted transaction as internal message
                            tracing::error!(%tx, "Aborted transaction");
                        }
                        ConnEvent::OutboundMessage(msg) => {
                            let Some(target_peer) = msg.target() else {
                                let id = *msg.id();
                                tracing::error!(%id, %msg, "Target peer not set, must be set for connection outbound message");
                                ctx.bridge.op_manager.completed(id);
                                continue;
                            };
                            tracing::info!(
                                tx = %msg.id(),
                                msg_type = %msg,
                                target_peer = %target_peer,
                                "Sending outbound message to peer"
                            );
                            match ctx.connections.get(&target_peer.peer) {
                                Some(peer_connection) => {
                                    if let Err(e) = peer_connection.send(Left(msg.clone())).await {
                                        tracing::error!(
                                            tx = %msg.id(),
                                            "Failed to send message to peer: {}", e
                                        );
                                    } else {
                                        tracing::info!(
                                            tx = %msg.id(),
                                            target_peer = %target_peer,
                                            "Message successfully sent to peer connection"
                                        );
                                    }
                                }
                                None => {
                                    tracing::warn!(
                                        id = %msg.id(),
                                        target = %target_peer.peer,
                                        "No existing outbound connection, establishing connection first"
                                    );

                                    // Queue the message for sending after connection is established
                                    let tx = *msg.id();
                                    let (callback, mut result) = tokio::sync::mpsc::channel(10);

                                    // Initiate connection to the peer
                                    ctx.bridge
                                        .ev_listener_tx
                                        .send(Right(NodeEvent::ConnectPeer {
                                            peer: target_peer.peer.clone(),
                                            tx,
                                            callback,
                                            is_gw: false,
                                        }))
                                        .await?;

                                    // Wait for connection to be established (with timeout)
                                    match timeout(Duration::from_secs(5), result.recv()).await {
                                        Ok(Some(Ok(_))) => {
                                            // Connection established, try sending again
                                            if let Some(peer_connection) =
                                                ctx.connections.get(&target_peer.peer)
                                            {
                                                if let Err(e) =
                                                    peer_connection.send(Left(msg)).await
                                                {
                                                    tracing::error!("Failed to send message to peer after establishing connection: {}", e);
                                                }
                                            }
                                        }
                                        Ok(Some(Err(e))) => {
                                            tracing::error!(
                                                "Failed to establish connection to {}: {:?}",
                                                target_peer.peer,
                                                e
                                            );
                                        }
                                        Ok(None) | Err(_) => {
                                            tracing::error!(
                                                "Timeout or error establishing connection to {}",
                                                target_peer.peer
                                            );
                                        }
                                    }
                                }
                            }
                        }
                        ConnEvent::ClosedChannel(reason) => {
                            match reason {
                                ChannelCloseReason::Handshake
                                | ChannelCloseReason::Bridge
                                | ChannelCloseReason::Controller
                                | ChannelCloseReason::Notification
                                | ChannelCloseReason::OpExecution => {
                                    // All ClosedChannel events are critical - the transport is unable to establish
                                    // more connections, rendering this peer useless. Perform cleanup and shutdown.
                                    tracing::error!(
                                        ?reason,
                                        is_gateway = ctx.bridge.op_manager.ring.is_gateway(),
                                        num_connections = ctx.connections.len(),
                                        "Critical channel closed - performing cleanup and shutting down"
                                    );

                                    // Clean up all active connections
                                    let peers_to_cleanup: Vec<_> =
                                        ctx.connections.keys().cloned().collect();
                                    for peer in peers_to_cleanup {
                                        tracing::debug!(%peer, "Cleaning up active connection due to critical channel closure");

                                        // Clean up ring state
                                        ctx.bridge
                                            .op_manager
                                            .ring
                                            .prune_connection(peer.clone())
                                            .await;

                                        // Remove from connection map
                                        ctx.connections.remove(&peer);

                                        // Notify handshake handler to clean up
                                        if let Err(e) = handshake_handler_msg
                                            .drop_connection(peer.clone())
                                            .await
                                        {
                                            tracing::warn!(%peer, error = ?e, "Failed to drop connection during cleanup");
                                        }
                                    }

                                    // Clean up reservations for in-progress connections
                                    // These are connections that started handshake but haven't completed yet
                                    // Notifying the callbacks will trigger the calling code to clean up reservations
                                    tracing::debug!(
                                        awaiting_count = state.awaiting_connection.len(),
                                        "Cleaning up in-progress connection reservations"
                                    );

                                    for (addr, mut callback) in state.awaiting_connection.drain() {
                                        tracing::debug!(%addr, "Notifying awaiting connection of shutdown");
                                        // Best effort notification - ignore errors since we're shutting down anyway
                                        // The callback sender will handle cleanup on their side
                                        let _ = callback
                                            .send_result(Err(HandshakeError::ChannelClosed))
                                            .await;
                                    }

                                    tracing::info!("Cleanup complete, exiting event loop");
                                    break;
                                }
                            }
                        }
                        ConnEvent::NodeAction(action) => match action {
                            NodeEvent::DropConnection(peer) => {
                                tracing::debug!(%peer, "Dropping connection");
                                if let Some(conn) = ctx.connections.remove(&peer) {
                                    // TODO: review: this could potentially leave garbage tasks in the background with peer listener
                                    timeout(
                                        Duration::from_secs(1),
                                        conn.send(Right(ConnEvent::NodeAction(
                                            NodeEvent::DropConnection(peer),
                                        ))),
                                    )
                                    .await
                                    .inspect_err(
                                        |error| {
                                            tracing::error!(
                                                "Failed to send drop connection message: {:?}",
                                                error
                                            );
                                        },
                                    )??;
                                }
                            }
                            NodeEvent::ConnectPeer {
                                peer,
                                tx,
                                callback,
                                is_gw,
                            } => {
                                ctx.handle_connect_peer(
                                    peer,
                                    Box::new(callback),
                                    tx,
                                    &handshake_handler_msg,
                                    &mut state,
                                    is_gw,
                                )
                                .await?;
                            }
                            NodeEvent::SendMessage { target, msg } => {
                                // Send the message to the target peer over the network
                                tracing::debug!(
                                    tx = %msg.id(),
                                    %target,
                                    "SendMessage event: sending message to peer via network bridge"
                                );
                                ctx.bridge.send(&target, *msg).await?;
                            }
                            NodeEvent::QueryConnections { callback } => {
                                let connections = ctx.connections.keys().cloned().collect();
                                timeout(
                                    Duration::from_secs(1),
                                    callback.send(QueryResult::Connections(connections)),
                                )
                                .await
                                .inspect_err(|error| {
                                    tracing::error!(
                                        "Failed to send connections query result: {:?}",
                                        error
                                    );
                                })??;
                            }
                            NodeEvent::QuerySubscriptions { callback } => {
                                // Get network subscriptions from OpManager
                                let network_subs = op_manager.get_network_subscriptions();

                                // Get application subscriptions from contract executor
                                // For now, we'll send a query to the contract handler
                                let (tx, mut rx) = tokio::sync::mpsc::channel(1);

                                op_manager
                                    .notify_contract_handler(
                                        ContractHandlerEvent::QuerySubscriptions { callback: tx },
                                    )
                                    .await?;

                                let app_subscriptions =
                                    match timeout(Duration::from_secs(1), rx.recv()).await {
                                        Ok(Some(QueryResult::NetworkDebug(info))) => {
                                            info.application_subscriptions
                                        }
                                        _ => Vec::new(),
                                    };

                                // Log network subscription details for debugging
                                for (contract_key, peers) in &network_subs {
                                    if !peers.is_empty() {
                                        tracing::debug!(
                                            %contract_key,
                                            peer_count = peers.len(),
                                            peers = ?peers,
                                            "Found network subscription"
                                        );
                                    }
                                }

                                let connections = ctx.connections.keys().cloned().collect();
                                let debug_info = crate::message::NetworkDebugInfo {
                                    application_subscriptions: app_subscriptions,
                                    network_subscriptions: network_subs,
                                    connected_peers: connections,
                                };

                                timeout(
                                    Duration::from_secs(1),
                                    callback.send(QueryResult::NetworkDebug(debug_info)),
                                )
                                .await
                                .inspect_err(|error| {
                                    tracing::error!(
                                        "Failed to send subscriptions query result: {:?}",
                                        error
                                    );
                                })??;
                            }
                            NodeEvent::QueryNodeDiagnostics { config, callback } => {
                                use freenet_stdlib::client_api::{
                                    ContractState, NetworkInfo, NodeDiagnosticsResponse, NodeInfo,
                                    SystemMetrics,
                                };
                                use std::collections::HashMap;

                                let mut response = NodeDiagnosticsResponse {
                                    node_info: None,
                                    network_info: None,
                                    subscriptions: Vec::new(),
                                    contract_states: HashMap::new(),
                                    system_metrics: None,
                                    connected_peers_detailed: Vec::new(),
                                };

                                // Collect node information
                                if config.include_node_info {
                                    // Calculate location and adress if is set
                                    let (addr, location) = if let Some(peer_id) =
                                        op_manager.ring.connection_manager.get_peer_key()
                                    {
                                        let location = Location::from_address(&peer_id.addr);
                                        (Some(peer_id.addr), Some(location))
                                    } else {
                                        (None, None)
                                    };

                                    // Always include basic node info, but only include address/location if available
                                    response.node_info = Some(NodeInfo {
                                        peer_id: ctx.key_pair.public().to_string(),
                                        is_gateway: self.is_gateway,
                                        location: location.map(|loc| format!("{:.6}", loc.0)),
                                        listening_address: addr
                                            .map(|peer_addr| peer_addr.to_string()),
                                        uptime_seconds: 0, // TODO: implement actual uptime tracking
                                    });
                                }

                                // Collect network information
                                if config.include_network_info {
                                    let connected_peers: Vec<_> = ctx
                                        .connections
                                        .keys()
                                        .map(|p| (p.to_string(), p.addr.to_string()))
                                        .collect();

                                    response.network_info = Some(NetworkInfo {
                                        connected_peers,
                                        active_connections: ctx.connections.len(),
                                    });
                                }

                                // Collect subscription information
                                if config.include_subscriptions {
                                    // Get network subscriptions from OpManager
                                    let _network_subs = op_manager.get_network_subscriptions();

                                    // Get application subscriptions from contract executor
                                    let (tx, mut rx) = tokio::sync::mpsc::channel(1);
                                    if op_manager
                                        .notify_contract_handler(
                                            ContractHandlerEvent::QuerySubscriptions {
                                                callback: tx,
                                            },
                                        )
                                        .await
                                        .is_ok()
                                    {
                                        let app_subscriptions = match timeout(
                                            Duration::from_secs(1),
                                            rx.recv(),
                                        )
                                        .await
                                        {
                                            Ok(Some(QueryResult::NetworkDebug(info))) => {
                                                info.application_subscriptions
                                            }
                                            _ => Vec::new(),
                                        };

                                        response.subscriptions = app_subscriptions
                                            .into_iter()
                                            .map(|sub| {
                                                freenet_stdlib::client_api::SubscriptionInfo {
                                                    contract_key: sub.contract_key,
                                                    client_id: sub.client_id.into(),
                                                }
                                            })
                                            .collect();
                                    }
                                }

                                // Collect contract states for specified contracts
                                if !config.contract_keys.is_empty() {
                                    for contract_key in &config.contract_keys {
                                        // Get actual subscriber information from OpManager
                                        let subscribers_info =
                                            op_manager.ring.subscribers_of(contract_key);
                                        let subscriber_count = subscribers_info
                                            .as_ref()
                                            .map(|s| s.value().len())
                                            .unwrap_or(0);
                                        let subscriber_peer_ids: Vec<String> =
                                            if config.include_subscriber_peer_ids {
                                                subscribers_info
                                                    .as_ref()
                                                    .map(|s| {
                                                        s.value()
                                                            .iter()
                                                            .map(|pk| pk.peer.to_string())
                                                            .collect()
                                                    })
                                                    .unwrap_or_default()
                                            } else {
                                                Vec::new()
                                            };

                                        response.contract_states.insert(
                                            *contract_key,
                                            ContractState {
                                                subscribers: subscriber_count as u32,
                                                subscriber_peer_ids,
                                            },
                                        );
                                    }
                                }

                                // Collect system metrics
                                if config.include_system_metrics {
                                    let seeding_contracts =
                                        op_manager.ring.all_network_subscriptions().len() as u32;
                                    response.system_metrics = Some(SystemMetrics {
                                        active_connections: ctx.connections.len() as u32,
                                        seeding_contracts,
                                    });
                                }

                                // Collect detailed peer information if requested
                                if config.include_detailed_peer_info {
                                    use freenet_stdlib::client_api::ConnectedPeerInfo;
                                    // Populate detailed peer information from actual connections
                                    for peer in ctx.connections.keys() {
                                        response.connected_peers_detailed.push(ConnectedPeerInfo {
                                            peer_id: peer.to_string(),
                                            address: peer.addr.to_string(),
                                        });
                                    }
                                }

                                timeout(
                                    Duration::from_secs(2),
                                    callback.send(QueryResult::NodeDiagnostics(response)),
                                )
                                .await
                                .inspect_err(|error| {
                                    tracing::error!(
                                        "Failed to send node diagnostics query result: {:?}",
                                        error
                                    );
                                })??;
                            }
                            NodeEvent::TransactionTimedOut(tx) => {
                                // Clean up client subscription to prevent memory leak
                                // Clients are not notified - transactions simply expire silently
                                if let Some(clients) = state.tx_to_client.remove(&tx) {
                                    tracing::debug!("Cleaned up {} client subscriptions for timed out transaction: {}", clients.len(), tx);
                                }
                            }
                            NodeEvent::TransactionCompleted(tx) => {
                                // Clean up client subscription after successful completion
                                state.tx_to_client.remove(&tx);
                            }
                            NodeEvent::LocalSubscribeComplete {
                                tx,
                                key,
                                subscribed,
                            } => {
                                tracing::info!("Received LocalSubscribeComplete event for transaction: {tx}, contract: {key}");

                                // Deliver SubscribeResponse directly to result router
                                tracing::info!("Sending SubscribeResponse to result router for transaction: {tx}");
                                use freenet_stdlib::client_api::{ContractResponse, HostResponse};
                                let response = Ok(HostResponse::ContractResponse(
                                    ContractResponse::SubscribeResponse { key, subscribed },
                                ));

                                match op_manager.result_router_tx.send((tx, response)).await {
                                    Ok(()) => {
                                        tracing::info!("Successfully sent SubscribeResponse to result router for transaction: {tx}");
                                        // Clean up client subscription after successful delivery
                                        state.tx_to_client.remove(&tx);
                                    }
                                    Err(e) => tracing::error!("Failed to send local subscribe response to result router: {}", e),
                                }
                            }
                            NodeEvent::Disconnect { cause } => {
                                tracing::info!(
                                    "Disconnecting from network{}",
                                    cause.map(|c| format!(": {c}")).unwrap_or_default()
                                );
                                break;
                            }
                        },
                    }
                }
            }
        }
        Err(anyhow::anyhow!("Network event stream ended unexpectedly"))
    }

    /// Process a SelectResult from the priority select stream
    async fn process_select_result(
        &mut self,
        result: priority_select::SelectResult,
        state: &mut EventListenerState,
        select_stream: &mut priority_select::ProductionPrioritySelectStream,
        handshake_handler_msg: &HanshakeHandlerMsg,
    ) -> anyhow::Result<EventResult> {
        let peer_id = &self.bridge.op_manager.ring.connection_manager.pub_key;

        use priority_select::SelectResult;
        match result {
            SelectResult::Notification(msg) => {
                tracing::debug!(
                    peer = %peer_id,
                    msg_present = msg.is_some(),
                    "PrioritySelect: notifications_receiver READY"
                );
                Ok(self.handle_notification_msg(msg))
            }
            SelectResult::OpExecution(msg) => {
                tracing::debug!(
                    peer = %peer_id,
                    "PrioritySelect: op_execution_receiver READY"
                );
                Ok(self.handle_op_execution(msg, state))
            }
            SelectResult::PeerConnection(msg) => {
                tracing::debug!(
                    peer = %peer_id,
                    "PrioritySelect: peer_connections READY"
                );
                self.handle_peer_connection_msg(msg, state, select_stream, handshake_handler_msg)
                    .await
            }
            SelectResult::ConnBridge(msg) => {
                tracing::debug!(
                    peer = %peer_id,
                    "PrioritySelect: conn_bridge_rx READY"
                );
                Ok(self.handle_bridge_msg(msg))
            }
            SelectResult::Handshake(result) => {
                tracing::debug!(
                    peer = %peer_id,
                    "PrioritySelect: handshake event READY"
                );
                match result {
                    Ok(event) => {
                        self.handle_handshake_action(
                            event,
                            state,
                            select_stream,
                            handshake_handler_msg,
                        )
                        .await?;
                        Ok(EventResult::Continue)
                    }
                    Err(handshake_error) => {
                        tracing::error!(?handshake_error, "Handshake handler error");
                        Ok(EventResult::Event(
                            ConnEvent::ClosedChannel(ChannelCloseReason::Handshake).into(),
                        ))
                    }
                }
            }
            SelectResult::NodeController(msg) => {
                tracing::debug!(
                    peer = %peer_id,
                    "PrioritySelect: node_controller READY"
                );
                Ok(self.handle_node_controller_msg(msg))
            }
            SelectResult::ClientTransaction(event_id) => {
                tracing::debug!(
                    peer = %peer_id,
                    "PrioritySelect: client_wait_for_transaction READY"
                );
                Ok(self.handle_client_transaction_subscription(event_id, state))
            }
            SelectResult::ExecutorTransaction(id) => {
                tracing::debug!(
                    peer = %peer_id,
                    "PrioritySelect: executor_listener READY"
                );
                Ok(self.handle_executor_transaction(id, state))
            }
        }
    }

    async fn handle_inbound_message(
        &self,
        msg: NetMessage,
        outbound_message: &OutboundMessage,
        op_manager: &Arc<OpManager>,
        state: &mut EventListenerState,
    ) -> anyhow::Result<()> {
        match msg {
            NetMessage::V1(NetMessageV1::Aborted(tx)) => {
                handle_aborted_op(tx, op_manager, &self.gateways).await?;
            }
            msg => {
                if let Some(addr) = state.transient_conn.get(msg.id()) {
                    // Forward message to transient joiner
                    outbound_message.send_to(*addr, msg).await?;
                } else {
                    self.process_message(msg, op_manager, None, state).await;
                }
            }
        }
        Ok(())
    }

    async fn process_message(
        &self,
        msg: NetMessage,
        op_manager: &Arc<OpManager>,
        executor_callback_opt: Option<ExecutorToEventLoopChannel<crate::contract::Callback>>,
        state: &mut EventListenerState,
    ) {
        tracing::info!(
            tx = %msg.id(),
            tx_type = ?msg.id().transaction_type(),
            msg_type = %msg,
            peer = %op_manager.ring.connection_manager.get_peer_key().unwrap(),
            "process_message called - processing network message"
        );

        // Only use the callback if this message was initiated by the executor
        let executor_callback_opt = if state.pending_from_executor.remove(msg.id()) {
            executor_callback_opt
        } else {
            None
        };

        let span = tracing::info_span!(
            "process_network_message",
            transaction = %msg.id(),
            tx_type = %msg.id().transaction_type()
        );

        let pending_op_result = state.pending_op_results.get(msg.id()).cloned();

        // Use MessageProcessor for clean client handling separation
        tracing::debug!(
            "Using PURE network processing - zero client types in network layer for transaction {}",
            msg.id()
        );
        GlobalExecutor::spawn(
            process_message_decoupled(
                msg,
                op_manager.clone(),
                self.bridge.clone(),
                self.event_listener.trait_clone(),
                executor_callback_opt,
                self.message_processor.clone(),
                pending_op_result,
            )
            .instrument(span),
        );
    }

    async fn handle_connect_peer(
        &mut self,
        peer: PeerId,
        mut callback: Box<dyn ConnectResultSender>,
        tx: Transaction,
        handshake_handler_msg: &HanshakeHandlerMsg,
        state: &mut EventListenerState,
        is_gw: bool,
    ) -> anyhow::Result<()> {
        tracing::info!(tx = %tx, remote = %peer, "Connecting to peer");
        if let Some(blocked_addrs) = &self.blocked_addresses {
            if blocked_addrs.contains(&peer.addr) {
                tracing::info!(tx = %tx, remote = %peer.addr, "Outgoing connection to peer blocked by local policy");
                // Don't propagate channel closed errors when notifying about blocked connections
                callback
                    .send_result(Err(HandshakeError::ConnectionError(
                        crate::node::network_bridge::ConnectionError::AddressBlocked(peer.addr),
                    )))
                    .await
                    .inspect_err(|e| {
                        tracing::debug!("Failed to send blocked connection notification: {:?}", e)
                    })
                    .ok();
                return Ok(());
            }
            tracing::debug!(tx = %tx, "Blocked addresses: {:?}, peer addr: {}", blocked_addrs, peer.addr);
        }
        state.awaiting_connection.insert(peer.addr, callback);
        let res = timeout(
            Duration::from_secs(10),
            handshake_handler_msg.establish_conn(peer.clone(), tx, is_gw),
        )
        .await
        .inspect_err(|error| {
            tracing::error!(tx = %tx, "Failed to establish connection: {:?}", error);
        })?;
        match res {
            Ok(()) => {
                tracing::debug!(tx = %tx,
                    "Successfully initiated connection process for peer: {:?}",
                    peer
                );
                Ok(())
            }
            Err(e) => Err(anyhow::Error::msg(e)),
        }
    }

    async fn handle_handshake_action(
        &mut self,
        event: HandshakeEvent,
        state: &mut EventListenerState,
        select_stream: &mut priority_select::ProductionPrioritySelectStream,
        _handshake_handler_msg: &HanshakeHandlerMsg, // Parameter added
    ) -> anyhow::Result<()> {
        match event {
            HandshakeEvent::InboundConnection {
                id,
                conn,
                joiner,
                op,
                forward_info,
                is_bootstrap,
            } => {
                if let Some(blocked_addrs) = &self.blocked_addresses {
                    if blocked_addrs.contains(&joiner.addr) {
                        tracing::info!(%id, remote = %joiner.addr, "Inbound connection from peer blocked by local policy");
                        // Not proceeding with adding connection or processing the operation.
                        // Don't call drop_connection_by_addr as it can cause channels to close abruptly
                        // Just ignore the connection and let it timeout naturally
                        return Ok(());
                    }
                }
                let (tx, rx) = mpsc::channel(1);
                self.connections.insert(joiner.clone(), tx);

                // IMPORTANT: Normally we do NOT add connection to ring here!
                // Connection should only be added after StartJoinReq is accepted
                // via CheckConnectivity. This prevents the "already connected" bug
                // where gateways reject valid join requests.
                //
                // EXCEPTION: Gateway bootstrap (is_bootstrap=true)
                // When a gateway accepts its very first connection (bootstrap case),
                // we must register it immediately so the gateway can respond to
                // FindOptimalPeer requests from subsequent joiners. Bootstrap connections
                // bypass the normal CheckConnectivity flow. See forward_conn() in
                // connect.rs and PR #1871 for full explanation.
                if is_bootstrap {
                    let location = Location::from_address(&joiner.addr);
                    tracing::info!(
                        %id,
                        %joiner,
                        %location,
                        "Bootstrap connection: immediately registering in ring"
                    );
                    self.bridge
                        .op_manager
                        .ring
                        .add_connection(location, joiner.clone(), true)
                        .await;
                }

                if let Some(op) = op {
                    self.bridge
                        .op_manager
                        .push(id, crate::operations::OpEnum::Connect(op))
                        .await?;
                }
                let task = peer_connection_listener(rx, conn).boxed();
                select_stream.push_peer_connection(task);

                if let Some(ForwardInfo {
                    target: forward_to,
                    msg,
                }) = forward_info.map(|b| *b)
                {
                    self.try_to_forward(&forward_to, msg).await?;
                }
            }
            HandshakeEvent::TransientForwardTransaction {
                target,
                tx,
                forward_to,
                msg,
            } => {
                if let Some(older_addr) = state.transient_conn.insert(tx, target) {
                    debug_assert_eq!(older_addr, target);
                    tracing::warn!(%target, %forward_to, "Transaction {} already exists as transient connections", tx);
                    if older_addr != target {
                        tracing::error!(
                            %tx,
                            "Not same target in new and old transient connections: {} != {}",
                            older_addr, target
                        );
                    }
                }
                self.try_to_forward(&forward_to, *msg).await?;
            }
            HandshakeEvent::OutboundConnectionSuccessful {
                peer_id,
                connection,
            } => {
                self.handle_successful_connection(peer_id, connection, state, select_stream, None)
                    .await?;
            }
            HandshakeEvent::OutboundGatewayConnectionSuccessful {
                peer_id,
                connection,
                remaining_checks,
            } => {
                self.handle_successful_connection(
                    peer_id,
                    connection,
                    state,
                    select_stream,
                    Some(remaining_checks),
                )
                .await?;
            }
            HandshakeEvent::OutboundConnectionFailed { peer_id, error } => {
                tracing::info!(%peer_id, "Connection failed: {:?}", error);
                if self.check_version {
                    if let HandshakeError::TransportError(
                        TransportError::ProtocolVersionMismatch { .. },
                    ) = &error
                    {
                        // The TransportError already has a user-friendly error message
                        // Just propagate it without additional logging to avoid duplication
                        return Err(error.into());
                    }
                }
                if let Some(mut r) = state.awaiting_connection.remove(&peer_id.addr) {
                    // Don't propagate channel closed errors - just log and continue
                    // The receiver may have timed out or been cancelled, which shouldn't crash the node
                    r.send_result(Err(error))
                        .await
                        .inspect_err(|e| {
                            tracing::warn!(%peer_id, "Failed to send connection error notification - receiver may have timed out: {:?}", e);
                        })
                        .ok();
                }
            }
            HandshakeEvent::RemoveTransaction(tx) => {
                state.transient_conn.remove(&tx);
            }
            HandshakeEvent::OutboundGatewayConnectionRejected { peer_id } => {
                tracing::info!(%peer_id, "Connection rejected by peer");
                if let Some(mut r) = state.awaiting_connection.remove(&peer_id.addr) {
                    // Don't propagate channel closed errors - just log and continue
                    if let Err(e) = r.send_result(Err(HandshakeError::ChannelClosed)).await {
                        tracing::debug!(%peer_id, "Failed to send rejection notification: {:?}", e);
                    }
                }
            }
            HandshakeEvent::InboundConnectionRejected { peer_id } => {
                tracing::debug!(%peer_id, "Inbound connection rejected");
            }
        }
        Ok(())
    }

    async fn try_to_forward(&mut self, forward_to: &PeerId, msg: NetMessage) -> anyhow::Result<()> {
        if let Some(peer) = self.connections.get(forward_to) {
            tracing::debug!(%forward_to, %msg, "Forwarding message to peer");
            // TODO: review: this could potentially leave garbage tasks in the background with peer listener
            timeout(Duration::from_secs(1), peer.send(Left(msg)))
                .await
                .inspect_err(|error| {
                    tracing::error!("Failed to forward message to peer: {:?}", error);
                })??;
        } else {
            tracing::warn!(%forward_to, "No connection to forward the message");
        }
        Ok(())
    }

    async fn handle_successful_connection(
        &mut self,
        peer_id: PeerId,
        connection: PeerConnection,
        state: &mut EventListenerState,
        select_stream: &mut priority_select::ProductionPrioritySelectStream,
        remaining_checks: Option<usize>,
    ) -> anyhow::Result<()> {
        if let Some(mut cb) = state.awaiting_connection.remove(&peer_id.addr) {
            let peer_id = if let Some(peer_id) = self
                .bridge
                .op_manager
                .ring
                .connection_manager
                .get_peer_key()
            {
                peer_id
            } else {
                let self_addr = connection
                    .my_address()
                    .ok_or_else(|| anyhow::anyhow!("self addr should be set"))?;
                let key = (*self.bridge.op_manager.ring.connection_manager.pub_key).clone();
                PeerId::new(self_addr, key)
            };
            timeout(
                Duration::from_secs(60),
                cb.send_result(Ok((peer_id, remaining_checks))),
            )
            .await
            .inspect_err(|error| {
                tracing::error!("Failed to send connection result: {:?}", error);
            })??;
        } else {
            tracing::warn!(%peer_id, "No callback for connection established");
        }
        let (tx, rx) = mpsc::channel(10);
        self.connections.insert(peer_id.clone(), tx);
        let task = peer_connection_listener(rx, connection).boxed();
        select_stream.push_peer_connection(task);
        Ok(())
    }

    async fn handle_peer_connection_msg(
        &mut self,
        msg: Option<Result<PeerConnectionInbound, TransportError>>,
        state: &mut EventListenerState,
        select_stream: &mut priority_select::ProductionPrioritySelectStream,
        handshake_handler_msg: &HanshakeHandlerMsg,
    ) -> anyhow::Result<EventResult> {
        match msg {
            Some(Ok(peer_conn)) => {
                // Get the remote address from the connection
                let remote_addr = peer_conn.conn.remote_addr();

                // Check if we need to establish a connection back to the sender
                let should_connect = !self.connections.keys().any(|peer| peer.addr == remote_addr)
                    && !state.awaiting_connection.contains_key(&remote_addr);

                if should_connect {
                    // Try to extract sender information from the message to establish connection
                    if let Some(sender_peer) = extract_sender_from_message(&peer_conn.msg) {
                        tracing::info!(
                            "Received message from unconnected peer {}, establishing connection proactively",
                            sender_peer.peer
                        );

                        let tx = Transaction::new::<crate::operations::connect::ConnectMsg>();
                        let (callback, _rx) = tokio::sync::mpsc::channel(10);

                        // Don't await - let it happen in the background
                        let _ = self
                            .handle_connect_peer(
                                sender_peer.peer.clone(),
                                Box::new(callback),
                                tx,
                                handshake_handler_msg,
                                state,
                                false, // not a gateway connection
                            )
                            .await;
                    }
                }

                let task = peer_connection_listener(peer_conn.rx, peer_conn.conn).boxed();
                select_stream.push_peer_connection(task);
                Ok(EventResult::Event(
                    ConnEvent::InboundMessage(peer_conn.msg).into(),
                ))
            }
            Some(Err(err)) => {
                if let TransportError::ConnectionClosed(socket_addr) = err {
                    if let Some(peer) = self
                        .connections
                        .keys()
                        .find_map(|k| (k.addr == socket_addr).then(|| k.clone()))
                    {
                        tracing::debug!(%peer, "Dropping connection");
                        self.bridge
                            .op_manager
                            .ring
                            .prune_connection(peer.clone())
                            .await;
                        self.connections.remove(&peer);
                        handshake_handler_msg.drop_connection(peer).await?;
                    }
                }
                Ok(EventResult::Continue)
            }
            None => {
                tracing::error!("All peer connections closed");
                Ok(EventResult::Continue)
            }
        }
    }

    fn handle_notification_msg(&self, msg: Option<Either<NetMessage, NodeEvent>>) -> EventResult {
        match msg {
            Some(Left(msg)) => {
                // Check if message has a target peer - if so, route as outbound, otherwise process locally
                if let Some(target) = msg.target() {
                    let self_peer = self
                        .bridge
                        .op_manager
                        .ring
                        .connection_manager
                        .get_peer_key()
                        .unwrap();
                    if target.peer != self_peer {
                        // Message targets another peer - send as outbound
                        tracing::info!(
                            tx = %msg.id(),
                            msg_type = %msg,
                            target_peer = %target,
                            "handle_notification_msg: Message has target peer, routing as OutboundMessage"
                        );
                        return EventResult::Event(ConnEvent::OutboundMessage(msg).into());
                    }
                }

                // Message targets self or has no target - process locally
                tracing::debug!(
                    tx = %msg.id(),
                    msg_type = %msg,
                    "handle_notification_msg: Received NetMessage notification, converting to InboundMessage"
                );
                EventResult::Event(ConnEvent::InboundMessage(msg).into())
            }
            Some(Right(action)) => {
                tracing::debug!("handle_notification_msg: Received NodeEvent notification");
                EventResult::Event(ConnEvent::NodeAction(action).into())
            }
            None => {
                EventResult::Event(ConnEvent::ClosedChannel(ChannelCloseReason::Notification).into())
            }
        }
    }

    fn handle_op_execution(
        &self,
        msg: Option<(Sender<NetMessage>, NetMessage)>,
        state: &mut EventListenerState,
    ) -> EventResult {
        match msg {
            Some((callback, msg)) => {
                state.pending_op_results.insert(*msg.id(), callback);
                EventResult::Event(ConnEvent::InboundMessage(msg).into())
            }
            None => {
                EventResult::Event(ConnEvent::ClosedChannel(ChannelCloseReason::OpExecution).into())
            }
        }
    }

    fn handle_bridge_msg(&self, msg: Option<P2pBridgeEvent>) -> EventResult {
        match msg {
            Some(Left((_, msg))) => EventResult::Event(ConnEvent::OutboundMessage(*msg).into()),
            Some(Right(action)) => EventResult::Event(ConnEvent::NodeAction(action).into()),
            None => EventResult::Event(ConnEvent::ClosedChannel(ChannelCloseReason::Bridge).into()),
        }
    }

    fn handle_node_controller_msg(&self, msg: Option<NodeEvent>) -> EventResult {
        match msg {
            Some(msg) => EventResult::Event(ConnEvent::NodeAction(msg).into()),
            None => {
                EventResult::Event(ConnEvent::ClosedChannel(ChannelCloseReason::Controller).into())
            }
        }
    }

    // Removed handle_handshake_msg as it's integrated into wait_for_event

    fn handle_client_transaction_subscription(
        &self,
        event_id: Result<(ClientId, WaitingTransaction), anyhow::Error>,
        state: &mut EventListenerState,
    ) -> EventResult {
        let Ok((client_id, transaction)) = event_id.inspect_err(|e| {
            tracing::error!("Error while receiving client transaction result: {:?}", e);
        }) else {
            return EventResult::Continue;
        };
        match transaction {
            WaitingTransaction::Transaction(tx) => {
                tracing::debug!(%tx, %client_id, "Subscribing client to transaction results");
                state.tx_to_client.entry(tx).or_default().insert(client_id);
            }
            WaitingTransaction::Subscription { contract_key } => {
                tracing::debug!(%client_id, %contract_key, "Client waiting for subscription");
                if let Some(clients) =
                    state
                        .client_waiting_transaction
                        .iter_mut()
                        .find_map(|(tx, clients)| {
                            if let WaitingTransaction::Subscription { contract_key: key } = tx {
                                return (key == &contract_key).then_some(clients);
                            }
                            None
                        })
                {
                    clients.insert(client_id);
                } else {
                    state.client_waiting_transaction.push((
                        WaitingTransaction::Subscription { contract_key },
                        HashSet::from_iter([client_id]),
                    ));
                }
            }
        }
        EventResult::Continue
    }

    fn handle_executor_transaction(
        &self,
        id: Result<Transaction, anyhow::Error>,
        state: &mut EventListenerState,
    ) -> EventResult {
        let Ok(id) = id.map_err(|err| {
            tracing::error!("Error while receiving transaction from executor: {:?}", err);
        }) else {
            return EventResult::Continue;
        };
        state.pending_from_executor.insert(id);
        EventResult::Continue
    }
}

trait ConnectResultSender {
    fn send_result(
        &mut self,
        result: Result<(PeerId, Option<usize>), HandshakeError>,
    ) -> Pin<Box<dyn Future<Output = Result<(), HandshakeError>> + Send + '_>>;
}

impl ConnectResultSender for Option<oneshot::Sender<Result<PeerId, HandshakeError>>> {
    fn send_result(
        &mut self,
        result: Result<(PeerId, Option<usize>), HandshakeError>,
    ) -> Pin<Box<dyn Future<Output = Result<(), HandshakeError>> + Send + '_>> {
        async move {
            self.take()
                .expect("always set")
                .send(result.map(|(id, _)| id))
                .map_err(|_| HandshakeError::ChannelClosed)?;
            Ok(())
        }
        .boxed()
    }
}

impl ConnectResultSender for mpsc::Sender<Result<(PeerId, Option<usize>), ()>> {
    fn send_result(
        &mut self,
        result: Result<(PeerId, Option<usize>), HandshakeError>,
    ) -> Pin<Box<dyn Future<Output = Result<(), HandshakeError>> + Send + '_>> {
        async move {
            self.send(result.map_err(|_| ()))
                .await
                .map_err(|_| HandshakeError::ChannelClosed)
        }
        .boxed()
    }
}

struct EventListenerState {
    // Note: peer_connections has been moved out to allow separate borrowing by the stream
    pending_from_executor: HashSet<Transaction>,
    // FIXME: we are potentially leaving trash here when transacrions are completed
    tx_to_client: HashMap<Transaction, HashSet<ClientId>>,
    client_waiting_transaction: Vec<(WaitingTransaction, HashSet<ClientId>)>,
    transient_conn: HashMap<Transaction, SocketAddr>,
    awaiting_connection: HashMap<SocketAddr, Box<dyn ConnectResultSender>>,
    pending_op_results: HashMap<Transaction, Sender<NetMessage>>,
}

impl EventListenerState {
    fn new() -> Self {
        Self {
            pending_from_executor: HashSet::new(),
            tx_to_client: HashMap::new(),
            client_waiting_transaction: Vec::new(),
            transient_conn: HashMap::new(),
            awaiting_connection: HashMap::new(),
            pending_op_results: HashMap::new(),
        }
    }
}

enum EventResult {
    Continue,
    Event(Box<ConnEvent>),
}

#[derive(Debug)]
pub(super) enum ConnEvent {
    InboundMessage(NetMessage),
    OutboundMessage(NetMessage),
    NodeAction(NodeEvent),
    ClosedChannel(ChannelCloseReason),
}

#[derive(Debug)]
pub(super) enum ChannelCloseReason {
    /// Handshake channel closed - potentially transient, continue operation
    Handshake,
    /// Internal bridge channel closed - critical, must shutdown gracefully
    Bridge,
    /// Node controller channel closed - critical, must shutdown gracefully
    Controller,
    /// Notification channel closed - critical, must shutdown gracefully
    Notification,
    /// Op execution channel closed - critical, must shutdown gracefully
    OpExecution,
}

#[allow(dead_code)]
enum ProtocolStatus {
    Unconfirmed,
    Confirmed,
    Reported,
    Failed,
}

#[derive(Debug)]
pub(super) struct PeerConnectionInbound {
    pub conn: PeerConnection,
    /// Receiver for inbound messages for the peer connection
    pub rx: Receiver<Either<NetMessage, ConnEvent>>,
    pub msg: NetMessage,
}

async fn peer_connection_listener(
    mut rx: PeerConnChannelRecv,
    mut conn: PeerConnection,
) -> Result<PeerConnectionInbound, TransportError> {
    loop {
        tokio::select! {
            msg = rx.recv() => {
                let Some(msg) = msg else { break Err(TransportError::ConnectionClosed(conn.remote_addr())); };
                match msg {
                    Left(msg) => {
                        tracing::debug!(to=%conn.remote_addr() ,"Sending message to peer. Msg: {msg}");
                        conn
                            .send(msg)
                            .await?;
                    }
                    Right(action) => {
                        tracing::debug!(to=%conn.remote_addr(), "Received action from channel");
                        match action {
                            ConnEvent::NodeAction(NodeEvent::DropConnection(_))
                            | ConnEvent::ClosedChannel(_) => {
                                break Err(TransportError::ConnectionClosed(conn.remote_addr()));
                            }
                            other => {
                                unreachable!("Unexpected action from peer_connection_listener channel: {:?}", other);
                            }
                        }
                    }
                }
            }
            msg = conn.recv() => {
                let Ok(msg) = msg
                    .inspect_err(|error| {
                        tracing::error!(from=%conn.remote_addr(), "Error while receiving message: {error}");
                    })
                else {
                    break Err(TransportError::ConnectionClosed(conn.remote_addr()));
                };
                let net_message = decode_msg(&msg).unwrap();
                tracing::debug!(from=%conn.remote_addr() ,"Received message from peer. Msg: {net_message}");
                break Ok(PeerConnectionInbound { conn, rx, msg: net_message });
            }
        }
    }
}

#[inline(always)]
fn decode_msg(data: &[u8]) -> Result<NetMessage, ConnectionError> {
    bincode::deserialize(data).map_err(|err| ConnectionError::Serialization(Some(err)))
}

/// Extract sender information from various message types
fn extract_sender_from_message(msg: &NetMessage) -> Option<PeerKeyLocation> {
    match msg {
        NetMessage::V1(msg_v1) => match msg_v1 {
            // Connect messages often have sender information
            NetMessageV1::Connect(connect_msg) => match connect_msg {
                ConnectMsg::Response { sender, .. } => Some(sender.clone()),
                ConnectMsg::Request { target, .. } => Some(target.clone()),
                _ => None,
            },
            // Get messages have sender in some variants
            NetMessageV1::Get(get_msg) => match get_msg {
                GetMsg::SeekNode { sender, .. } => Some(sender.clone()),
                GetMsg::ReturnGet { sender, .. } => Some(sender.clone()),
                _ => None,
            },
            // Put messages have sender in some variants
            NetMessageV1::Put(put_msg) => match put_msg {
                PutMsg::SeekNode { sender, .. } => Some(sender.clone()),
                PutMsg::SuccessfulPut { sender, .. } => Some(sender.clone()),
                PutMsg::PutForward { sender, .. } => Some(sender.clone()),
                _ => None,
            },
            // Update messages have sender in some variants
            NetMessageV1::Update(update_msg) => match update_msg {
                UpdateMsg::SeekNode { sender, .. } => Some(sender.clone()),
                UpdateMsg::SuccessfulUpdate { sender, .. } => Some(sender.clone()),
                UpdateMsg::Broadcasting { sender, .. } => Some(sender.clone()),
                UpdateMsg::BroadcastTo { sender, .. } => Some(sender.clone()),
                _ => None,
            },
            // Subscribe messages
            NetMessageV1::Subscribe(subscribe_msg) => match subscribe_msg {
                SubscribeMsg::SeekNode { subscriber, .. } => Some(subscriber.clone()),
                SubscribeMsg::ReturnSub { sender, .. } => Some(sender.clone()),
                _ => None,
            },
            // Other message types don't have sender info
            _ => None,
        },
    }
}

// TODO: add testing for the network loop, now it should be possible to do since we don't depend upon having real connections
